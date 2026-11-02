# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

> 本轮版本为 `df5cb52`，执行日期为 2026-11-02（北京时间）。**Bowl 测试未全部通过**：完成 `juicefs bench` 与前 6 项 fio；第 7 项 `direct_randwrite_4jobs` 返回 EIO（错误码 5），第 8 项随机读和两项 mdtest 未执行。失败与未执行项不计入成功指标，表中显示 `—`。

> 上轮失败的 4 GiB 单流顺序写与最终 `fsync` 本轮完成，fio 错误码为 0。本轮随机写失败时，服务端 `Write` 记录 `put block … context deadline exceeded`；根因未确认，不能宣称全套性能问题已解决。

> JuiceFS PG/Redis 对照组在新建同规格双机上完整补跑。第 2、7 节环境与资源审计属于对照组；Bowl 环境、磁盘与网络基线、实例清理记录见 [env_bowl.json](results/env_bowl.json)。跨实例比值仅供观察，不作为严格的优化收益证明。

> 失败证据：[fio 原始输出](results/fio_bowl_direct_randwrite_4jobs.json)、[服务端错误摘录](results/bowl-storage-errors.log)、[完整服务端日志](results/bowl-storage.log.gz)、[Bowl 运行日志](results/bowl-run.log)、[对照组运行日志](results/controls-run.log)。生产代码、超时与测试负载均保持本轮主分支原样。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘。实际完成范围与重跑环境见本轮执行说明；官方文档数值仅作历史参考：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `333.36 MiB/s` | `309.63 MiB/s` | `295.43 MiB/s` | **1.08x** | **1.13x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,555.58 MiB/s` | `438.34 MiB/s` | `452.24 MiB/s` | **3.55x** | **3.44x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `49.00 files/s` | `559.60 files/s` | `585.70 files/s` | **0.09x** | **0.08x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,385.80 files/s` | `2,553.70 files/s` | `2,781.80 files/s` | **0.54x** | **0.50x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,937.60 files/s` | `10,748.50 files/s` | `10,474.20 files/s` | **0.65x** | **0.66x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `279.88 MiB/s` | `306.61 MiB/s` | `292.59 MiB/s` | **0.91x** | **0.96x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `184.36 MiB/s` | `184.55 MiB/s` | `181.79 MiB/s` | **1.00x** | **1.01x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `196.43 MiB/s` | `194.46 MiB/s` | `194.89 MiB/s` | **1.01x** | **1.01x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `181.60 MiB/s` | `177.74 MiB/s` | `175.03 MiB/s` | **1.02x** | **1.04x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `—` | `628.87 ops/s` | `1,415.14 ops/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `—` | `802.87 ops/s` | `1,395.37 ops/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `—` | `3,124.82 ops/s` | `9,956.51 ops/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `—` | `652.13 ops/s` | `1,279.94 ops/s` | **—** | **—** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9br9hokza0u9k8y2qv` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.207` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9br9hokza0u9k8y2qw` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.206` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.071/0.102/0.313/0.053` ms | `ping -c 20 -i 0.05 172.16.1.207` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.207 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `178.97` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `174.45` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `df5cb52` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **元数据引擎 (Server 节点)** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`bowl` 库) | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.207:9001/jfs-pg`) | rclone S3 (`http://172.16.1.207:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `333.36 MiB/s (12.29 s/file)` | `309.63 MiB/s (13.23 s/file)` | `295.43 MiB/s (13.86 s/file)` | `744.47 MiB/s` | `731.93 MiB/s` | **1.08x** | **1.13x** |
| **Read big file** | `1555.58 MiB/s (2.63 s/file)` | `438.34 MiB/s (9.34 s/file)` | `452.24 MiB/s (9.06 s/file)` | `895.88 MiB/s` | `892.99 MiB/s` | **3.55x** | **3.44x** |
| **Write small file** | `49.00 files/s (81.62 ms/file)` | `559.60 files/s (7.15 ms/file)` | `585.70 files/s (6.83 ms/file)` | `86.40 files/s` | `109.10 files/s` | **0.09x** | **0.08x** |
| **Read small file** | `1385.80 files/s (2.89 ms/file)` | `2553.70 files/s (1.57 ms/file)` | `2781.80 files/s (1.44 ms/file)` | `1857.90 files/s` | `937.30 files/s` | **0.54x** | **0.50x** |
| **Stat file** | `6937.60 files/s (0.58 ms/file)` | `10748.50 files/s (0.37 ms/file)` | `10474.20 files/s (0.38 ms/file)` | `7845.80 files/s` | `11989.50 files/s` | **0.65x** | **0.66x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `279.88 MiB/s (70.0 IOPS)` | `306.61 MiB/s (76.7 IOPS)` | `292.59 MiB/s (73.2 IOPS)` | **0.91x** | **0.96x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `184.36 MiB/s (46.1 IOPS)` | `184.55 MiB/s (46.1 IOPS)` | `181.79 MiB/s (45.5 IOPS)` | **1.00x** | **1.01x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `196.43 MiB/s (49.1 IOPS)` | `194.46 MiB/s (48.6 IOPS)` | `194.89 MiB/s (48.7 IOPS)` | **1.01x** | **1.01x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `181.60 MiB/s (45.4 IOPS)` | `177.74 MiB/s (44.4 IOPS)` | `175.03 MiB/s (43.8 IOPS)` | **1.02x** | **1.04x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `281.45 MiB/s (281.4 IOPS)` | `326.87 MiB/s (326.9 IOPS)` | `320.90 MiB/s (320.9 IOPS)` | **0.86x** | **0.88x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `183.04 MiB/s (183.0 IOPS)` | `184.65 MiB/s (184.7 IOPS)` | `178.68 MiB/s (178.7 IOPS)` | **0.99x** | **1.02x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `—` | `312.48 MiB/s (312.5 IOPS)` | `305.35 MiB/s (305.4 IOPS)` | **—** | **—** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `—` | `122.73 MiB/s (122.7 IOPS)` | `121.04 MiB/s (121.0 IOPS)` | **—** | **—** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `13.81 / 33.82 / 139.46 ms` | `10.70 / 43.25 / 73.92 ms` | `11.71 / 43.25 / 93.85 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.69 / 30.80 / 34.34 ms` | `21.67 / 119.01 / 168.82 ms` | `22.00 / 125.31 / 164.63 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `317.22 / 599.78 / 775.95 ms` | `309.88 / 358.61 / 446.69 ms` | `319.20 / 442.50 / 541.07 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `350.62 / 513.80 / 616.56 ms` | `349.91 / 1052.77 / 1333.79 ms` | `351.94 / 1082.13 / 1367.34 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `—` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `—` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比本轮实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `—` | `628.9` | `1,415.1` | `1,416.6` | `192.3` | `6.0` | **—** | **—** |
| **Directory stat** | `—` | `3,065.0` | `10,043.5` | `3,810.1` | `1,311.2` | `435.9` | **—** | **—** |
| **Directory removal** | `—` | `568.8` | `1,165.2` | `1,115.1` | `213.1` | `9.0` | **—** | **—** |
| **File creation** | `—` | `802.9` | `1,395.4` | `1,410.3` | `179.3` | `5.7` | **—** | **—** |
| **File stat** | `—` | `3,124.8` | `9,956.5` | `5,023.2` | `915.2` | `68.7` | **—** | **—** |
| **File read** | `—` | `1,654.3` | `2,850.7` | `3,487.9` | `371.0` | `33.9` | **—** | **—** |
| **File removal** | `—` | `652.1` | `1,279.9` | `1,163.4` | `217.5` | `23.7` | **—** | **—** |
| **Tree creation** | `—` | `664.0` | `1,430.3` | `1,503.0` | `187.9` | `6.0` | **—** | **—** |
| **Tree removal** | `—` | `516.0` | `1,206.3` | `1,119.8` | `218.4` | `9.9` | **—** | **—** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `—` | `2,318.5` | `5,009.3` | **—** | **—** |
| **Directory stat** | `—` | `305,280.0` | `62,178.9` | **—** | **—** |
| **Directory removal** | `—` | `2,292.1` | `3,987.5` | **—** | **—** |
| **File creation** | `—` | `2,691.0` | `4,917.6` | **—** | **—** |
| **File stat** | `—` | `308,448.8` | `61,922.5` | **—** | **—** |
| **File read** | `—` | `11,610.0` | `14,051.4` | **—** | **—** |
| **File removal** | `—` | `2,658.6` | `5,008.0` | **—** | **—** |
| **Tree creation** | `—` | `228.9` | `195.4` | **—** | **—** |
| **Tree removal** | `—` | `491.3` | `338.4` | **—** | **—** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`279.88 MiB/s`；同轮 JuiceFS(PG) 为 `306.61 MiB/s`。
- Bowl 单流顺序读：`184.36 MiB/s`；同轮 JuiceFS(PG) 为 `184.55 MiB/s`。
- Bowl 16 并发顺序写：`196.43 MiB/s`；同轮 JuiceFS(PG) 为 `194.46 MiB/s`。
- Bowl 16 并发顺序读：`181.60 MiB/s`；同轮 JuiceFS(PG) 为 `177.74 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

本轮 Bowl 使用 ADR-0026：块写入返回时已进入服务端页缓存，显式 `Fsync` 仅同步数据库 WAL，块数据由内核后台回写。本套件未测试整机断电恢复；吞吐结果不能作为断电后数据完整性的证明，也不能与旧版逐块刷盘结果视为相同持久化条件。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261102-021647`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9br9hokza0u9k8y2qv`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9br9hokza0u9k8y2qw`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T02:16:51Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T02:34:27Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
