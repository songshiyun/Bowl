# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘。实际完成范围与重跑环境见本轮执行说明；官方文档数值仅作历史参考：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `340.02 MiB/s` | `303.92 MiB/s` | `303.74 MiB/s` | **1.12x** | **1.12x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,782.86 MiB/s` | `454.98 MiB/s` | `459.94 MiB/s` | **3.92x** | **3.88x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `343.50 files/s` | `520.90 files/s` | `682.60 files/s` | **0.66x** | **0.50x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,873.50 files/s` | `2,705.10 files/s` | `3,188.80 files/s` | **0.69x** | **0.59x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,851.00 files/s` | `13,524.90 files/s` | `16,577.30 files/s` | **0.51x** | **0.41x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `313.61 MiB/s` | `300.65 MiB/s` | `304.11 MiB/s` | **1.04x** | **1.03x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `182.56 MiB/s` | `182.74 MiB/s` | `184.32 MiB/s` | **1.00x** | **0.99x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `196.28 MiB/s` | `198.58 MiB/s` | `198.98 MiB/s` | **0.99x** | **0.99x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `181.35 MiB/s` | `176.37 MiB/s` | `176.54 MiB/s` | **1.03x** | **1.03x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `512.72 ops/s` | `506.42 ops/s` | `2,418.55 ops/s` | **1.01x** | **0.21x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `686.04 ops/s` | `1,129.71 ops/s` | `2,252.60 ops/s` | **0.61x** | **0.30x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `2,863.76 ops/s` | `4,647.79 ops/s` | `16,351.86 ops/s` | **0.62x** | **0.18x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `589.16 ops/s` | `934.64 ops/s` | `2,101.46 ops/s` | **0.63x** | **0.28x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj93ymmufo05cjc13acy` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.223` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj93ymmufo05cjc13acz` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.222` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.035/0.064/0.269/0.051` ms | `ping -c 20 -i 0.05 172.16.1.223` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.223 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `164.09` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.29` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `0655a17` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **元数据引擎 (Server 节点)** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`bowl` 库) | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.223:9001/jfs-pg`) | rclone S3 (`http://172.16.1.223:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `340.02 MiB/s (12.05 s/file)` | `303.92 MiB/s (13.48 s/file)` | `303.74 MiB/s (13.49 s/file)` | `744.47 MiB/s` | `731.93 MiB/s` | **1.12x** | **1.12x** |
| **Read big file** | `1782.86 MiB/s (2.30 s/file)` | `454.98 MiB/s (9.00 s/file)` | `459.94 MiB/s (8.91 s/file)` | `895.88 MiB/s` | `892.99 MiB/s` | **3.92x** | **3.88x** |
| **Write small file** | `343.50 files/s (11.64 ms/file)` | `520.90 files/s (7.68 ms/file)` | `682.60 files/s (5.86 ms/file)` | `86.40 files/s` | `109.10 files/s` | **0.66x** | **0.50x** |
| **Read small file** | `1873.50 files/s (2.14 ms/file)` | `2705.10 files/s (1.48 ms/file)` | `3188.80 files/s (1.25 ms/file)` | `1857.90 files/s` | `937.30 files/s` | **0.69x** | **0.59x** |
| **Stat file** | `6851.00 files/s (0.58 ms/file)` | `13524.90 files/s (0.30 ms/file)` | `16577.30 files/s (0.24 ms/file)` | `7845.80 files/s` | `11989.50 files/s` | **0.51x** | **0.41x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `313.61 MiB/s (78.4 IOPS)` | `300.65 MiB/s (75.2 IOPS)` | `304.11 MiB/s (76.0 IOPS)` | **1.04x** | **1.03x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `182.56 MiB/s (45.6 IOPS)` | `182.74 MiB/s (45.7 IOPS)` | `184.32 MiB/s (46.1 IOPS)` | **1.00x** | **0.99x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `196.28 MiB/s (49.1 IOPS)` | `198.58 MiB/s (49.6 IOPS)` | `198.98 MiB/s (49.7 IOPS)` | **0.99x** | **0.99x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `181.35 MiB/s (45.3 IOPS)` | `176.37 MiB/s (44.1 IOPS)` | `176.54 MiB/s (44.1 IOPS)` | **1.03x** | **1.03x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `285.24 MiB/s (285.2 IOPS)` | `332.01 MiB/s (332.0 IOPS)` | `336.32 MiB/s (336.3 IOPS)` | **0.86x** | **0.85x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `183.71 MiB/s (183.7 IOPS)` | `184.75 MiB/s (184.8 IOPS)` | `184.63 MiB/s (184.6 IOPS)` | **0.99x** | **1.00x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `288.39 MiB/s (288.4 IOPS)` | `299.11 MiB/s (299.1 IOPS)` | `310.92 MiB/s (310.9 IOPS)` | **0.96x** | **0.93x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `184.61 MiB/s (184.6 IOPS)` | `120.94 MiB/s (120.9 IOPS)` | `120.34 MiB/s (120.3 IOPS)` | **1.53x** | **1.53x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `12.02 / 20.58 / 116.92 ms` | `11.32 / 43.25 / 63.70 ms` | `11.13 / 43.25 / 73.92 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.91 / 33.16 / 40.63 ms` | `21.89 / 114.82 / 173.01 ms` | `21.70 / 120.06 / 170.92 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `317.38 / 591.40 / 734.00 ms` | `309.30 / 358.61 / 446.69 ms` | `309.43 / 362.81 / 446.69 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `350.52 / 492.83 / 583.01 ms` | `348.95 / 1035.99 / 1317.01 ms` | `343.37 / 1044.38 / 1350.57 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比本轮实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `512.7` | `506.4` | `2,418.6` | `1,416.6` | `192.3` | `6.0` | **1.01x** | **0.21x** |
| **Directory stat** | `2,883.8` | `1,162.7` | `14,796.4` | `3,810.1` | `1,311.2` | `435.9` | **2.48x** | **0.19x** |
| **Directory removal** | `593.8` | `538.5` | `1,954.9` | `1,115.1` | `213.1` | `9.0` | **1.10x** | **0.30x** |
| **File creation** | `686.0` | `1,129.7` | `2,252.6` | `1,410.3` | `179.3` | `5.7` | **0.61x** | **0.30x** |
| **File stat** | `2,863.8` | `4,647.8` | `16,351.9` | `5,023.2` | `915.2` | `68.7` | **0.62x** | **0.18x** |
| **File read** | `870.4` | `2,444.7` | `4,842.7` | `3,487.9` | `371.0` | `33.9` | **0.36x** | **0.18x** |
| **File removal** | `589.2` | `934.6` | `2,101.5` | `1,163.4` | `217.5` | `23.7` | **0.63x** | **0.28x** |
| **Tree creation** | `595.6` | `913.4` | `2,431.8` | `1,503.0` | `187.9` | `6.0` | **0.65x** | **0.24x** |
| **Tree removal** | `613.3` | `778.4` | `1,972.3` | `1,119.8` | `218.4` | `9.9` | **0.79x** | **0.31x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `1,963.8` | `3,091.4` | `5,943.5` | **0.64x** | **0.33x** |
| **Directory stat** | `273,263.3` | `61,003.9` | `463,568.2` | **4.48x** | **0.59x** |
| **Directory removal** | `2,353.7` | `3,079.4` | `7,568.2` | **0.76x** | **0.31x** |
| **File creation** | `1,931.3` | `3,484.2` | `7,061.8` | **0.55x** | **0.27x** |
| **File stat** | `240,750.9` | `287,137.2` | `64,327.6` | **0.84x** | **3.74x** |
| **File read** | `5,017.8` | `14,019.0` | `18,965.2` | **0.36x** | **0.26x** |
| **File removal** | `2,316.7` | `3,207.7` | `8,079.2` | **0.72x** | **0.29x** |
| **Tree creation** | `267.2` | `454.2` | `369.2` | **0.59x** | **0.72x** |
| **Tree removal** | `334.2` | `603.4` | `594.5` | **0.55x** | **0.56x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`313.61 MiB/s`；同轮 JuiceFS(PG) 为 `300.65 MiB/s`。
- Bowl 单流顺序读：`182.56 MiB/s`；同轮 JuiceFS(PG) 为 `182.74 MiB/s`。
- Bowl 16 并发顺序写：`196.28 MiB/s`；同轮 JuiceFS(PG) 为 `198.58 MiB/s`。
- Bowl 16 并发顺序读：`181.35 MiB/s`；同轮 JuiceFS(PG) 为 `176.37 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

本轮 Bowl 使用 ADR-0026：块写入返回时已进入服务端页缓存，显式 `Fsync` 仅同步数据库 WAL，块数据由内核后台回写。本套件未测试整机断电恢复；吞吐结果不能作为断电后数据完整性的证明，也不能与旧版逐块刷盘结果视为相同持久化条件。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

本轮包含 ADR-0028：Create、Write、常规 Release 使用单语句原子 CTE 与内联异步 WAL 提交；重开文件时按版本保留内核页缓存。因此重复读取包含缓存命中收益，本轮数据不能单独归因于数据库往返减少。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261102-155330`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj93ymmufo05cjc13acy`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj93ymmufo05cjc13acz`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T15:53:38Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T16:19:51Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
