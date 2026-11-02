# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

> 本轮于 2026-11-02 测试 Bowl bc64975。Bowl、JuiceFS PostgreSQL、JuiceFS Redis 在同一对硅谷 ECS 上依次完成全部基础测试、8 项 fio 及单进程/四进程 mdtest；24 份 fio 错误码均为 0，完成字节数符合配置。

> 上轮 df5cb52 的 Bowl 随机写曾返回 EIO；本轮随机写与后续随机读、mdtest 均完成。Bowl 采用 ADR-0027：4 MiB Chunk、满 64 条 Slice 整体合并、后台串行整理与回收、PutBlock 直写目标路径、默认 RPC 超时从 5 秒调整为 30 秒；不能将改善归因于其中某一项。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘。实际完成范围与重跑环境见本轮执行说明；官方文档数值仅作历史参考：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `324.87 MiB/s` | `305.83 MiB/s` | `306.21 MiB/s` | **1.06x** | **1.06x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,660.03 MiB/s` | `455.43 MiB/s` | `449.03 MiB/s` | **3.64x** | **3.70x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `50.90 files/s` | `435.90 files/s` | `858.50 files/s` | **0.12x** | **0.06x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,369.20 files/s` | `2,229.90 files/s` | `3,142.50 files/s` | **0.61x** | **0.44x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `7,689.90 files/s` | `8,071.10 files/s` | `10,080.40 files/s` | **0.95x** | **0.76x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `272.03 MiB/s` | `297.52 MiB/s` | `306.47 MiB/s` | **0.91x** | **0.89x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `184.26 MiB/s` | `184.15 MiB/s` | `184.26 MiB/s` | **1.00x** | **1.00x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `196.41 MiB/s` | `198.03 MiB/s` | `199.29 MiB/s` | **0.99x** | **0.99x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `181.50 MiB/s` | `175.33 MiB/s` | `175.43 MiB/s` | **1.04x** | **1.03x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `476.72 ops/s` | `419.96 ops/s` | `1,423.06 ops/s` | **1.14x** | **0.33x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `420.87 ops/s` | `794.36 ops/s` | `1,388.68 ops/s` | **0.53x** | **0.30x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `2,731.52 ops/s` | `3,169.47 ops/s` | `10,149.10 ops/s` | **0.86x** | **0.27x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `521.82 ops/s` | `628.13 ops/s` | `1,286.74 ops/s` | **0.83x** | **0.41x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9cftu5s9p7shzwg5a6` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.211` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9cftu5s9p7shzwg5a7` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.210` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.071/0.094/0.241/0.038` ms | `ping -c 20 -i 0.05 172.16.1.211` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.211 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `181.21` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `179.32` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `bc64975` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **元数据引擎 (Server 节点)** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`bowl` 库) | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.211:9001/jfs-pg`) | rclone S3 (`http://172.16.1.211:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `324.87 MiB/s (12.61 s/file)` | `305.83 MiB/s (13.39 s/file)` | `306.21 MiB/s (13.38 s/file)` | `744.47 MiB/s` | `731.93 MiB/s` | **1.06x** | **1.06x** |
| **Read big file** | `1660.03 MiB/s (2.47 s/file)` | `455.43 MiB/s (8.99 s/file)` | `449.03 MiB/s (9.12 s/file)` | `895.88 MiB/s` | `892.99 MiB/s` | **3.64x** | **3.70x** |
| **Write small file** | `50.90 files/s (78.57 ms/file)` | `435.90 files/s (9.18 ms/file)` | `858.50 files/s (4.66 ms/file)` | `86.40 files/s` | `109.10 files/s` | **0.12x** | **0.06x** |
| **Read small file** | `1369.20 files/s (2.92 ms/file)` | `2229.90 files/s (1.79 ms/file)` | `3142.50 files/s (1.27 ms/file)` | `1857.90 files/s` | `937.30 files/s` | **0.61x** | **0.44x** |
| **Stat file** | `7689.90 files/s (0.52 ms/file)` | `8071.10 files/s (0.50 ms/file)` | `10080.40 files/s (0.40 ms/file)` | `7845.80 files/s` | `11989.50 files/s` | **0.95x** | **0.76x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `272.03 MiB/s (68.0 IOPS)` | `297.52 MiB/s (74.4 IOPS)` | `306.47 MiB/s (76.6 IOPS)` | **0.91x** | **0.89x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `184.26 MiB/s (46.1 IOPS)` | `184.15 MiB/s (46.0 IOPS)` | `184.26 MiB/s (46.1 IOPS)` | **1.00x** | **1.00x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `196.41 MiB/s (49.1 IOPS)` | `198.03 MiB/s (49.5 IOPS)` | `199.29 MiB/s (49.8 IOPS)` | **0.99x** | **0.99x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `181.50 MiB/s (45.4 IOPS)` | `175.33 MiB/s (43.8 IOPS)` | `175.43 MiB/s (43.9 IOPS)` | **1.04x** | **1.03x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `281.26 MiB/s (281.3 IOPS)` | `335.33 MiB/s (335.3 IOPS)` | `330.80 MiB/s (330.8 IOPS)` | **0.84x** | **0.85x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `183.86 MiB/s (183.9 IOPS)` | `184.47 MiB/s (184.5 IOPS)` | `184.30 MiB/s (184.3 IOPS)` | **1.00x** | **1.00x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `286.75 MiB/s (286.8 IOPS)` | `300.93 MiB/s (300.9 IOPS)` | `314.01 MiB/s (314.0 IOPS)` | **0.95x** | **0.91x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `184.47 MiB/s (184.5 IOPS)` | `121.72 MiB/s (121.7 IOPS)` | `119.64 MiB/s (119.6 IOPS)` | **1.52x** | **1.54x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `14.30 / 32.11 / 147.85 ms` | `11.30 / 44.30 / 83.36 ms` | `11.10 / 43.25 / 84.41 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.70 / 31.33 / 41.68 ms` | `21.72 / 115.87 / 166.72 ms` | `21.70 / 123.21 / 173.01 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `319.30 / 608.17 / 784.34 ms` | `308.94 / 354.42 / 446.69 ms` | `309.07 / 358.61 / 446.69 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `349.84 / 513.80 / 608.17 ms` | `354.23 / 1044.38 / 1400.90 ms` | `347.78 / 1044.38 / 1350.57 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.02 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比本轮实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `476.7` | `420.0` | `1,423.1` | `1,416.6` | `192.3` | `6.0` | **1.14x** | **0.33x** |
| **Directory stat** | `2,753.1` | `1,096.6` | `9,993.4` | `3,810.1` | `1,311.2` | `435.9` | **2.51x** | **0.28x** |
| **Directory removal** | `525.8` | `453.1` | `1,166.7` | `1,115.1` | `213.1` | `9.0` | **1.16x** | **0.45x** |
| **File creation** | `420.9` | `794.4` | `1,388.7` | `1,410.3` | `179.3` | `5.7` | **0.53x** | **0.30x** |
| **File stat** | `2,731.5` | `3,169.5` | `10,149.1` | `5,023.2` | `915.2` | `68.7` | **0.86x** | **0.27x** |
| **File read** | `554.3` | `1,664.5` | `2,863.4` | `3,487.9` | `371.0` | `33.9` | **0.33x** | **0.19x** |
| **File removal** | `521.8` | `628.1` | `1,286.7` | `1,163.4` | `217.5` | `23.7` | **0.83x** | **0.41x** |
| **Tree creation** | `540.8` | `672.3` | `1,474.9` | `1,503.0` | `187.9` | `6.0` | **0.80x** | **0.37x** |
| **Tree removal** | `527.3` | `510.9` | `1,191.1` | `1,119.8` | `218.4` | `9.9` | **1.03x** | **0.44x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `1,893.9` | `2,359.3` | `4,630.3` | **0.80x** | **0.41x** |
| **Directory stat** | `267,594.8` | `57,004.6` | `344,288.2` | **4.69x** | **0.78x** |
| **Directory removal** | `2,261.7` | `2,135.7` | `3,980.6` | **1.06x** | **0.57x** |
| **File creation** | `1,355.8` | `2,454.7` | `4,922.0` | **0.55x** | **0.28x** |
| **File stat** | `260,120.2` | `59,335.4` | `63,515.5` | **4.38x** | **4.10x** |
| **File read** | `2,643.0` | `11,211.3` | `13,850.2` | **0.24x** | **0.19x** |
| **File removal** | `1,915.0` | `2,426.0` | `5,167.1` | **0.79x** | **0.37x** |
| **Tree creation** | `230.2` | `205.4` | `224.5` | **1.12x** | **1.03x** |
| **Tree removal** | `362.3` | `461.4` | `818.0` | **0.79x** | **0.44x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`272.03 MiB/s`；同轮 JuiceFS(PG) 为 `297.52 MiB/s`。
- Bowl 单流顺序读：`184.26 MiB/s`；同轮 JuiceFS(PG) 为 `184.15 MiB/s`。
- Bowl 16 并发顺序写：`196.41 MiB/s`；同轮 JuiceFS(PG) 为 `198.03 MiB/s`。
- Bowl 16 并发顺序读：`181.50 MiB/s`；同轮 JuiceFS(PG) 为 `175.33 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

本轮 Bowl 使用 ADR-0026：块写入返回时已进入服务端页缓存，显式 `Fsync` 仅同步数据库 WAL，块数据由内核后台回写。本套件未测试整机断电恢复；吞吐结果不能作为断电后数据完整性的证明，也不能与旧版逐块刷盘结果视为相同持久化条件。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261102-060213`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9cftu5s9p7shzwg5a6`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9cftu5s9p7shzwg5a7`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T06:02:21Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T06:30:00Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
