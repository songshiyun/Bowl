# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘，依次运行以下三组目标：

1. **Bowl（PostgreSQL 16 控制面 + Pebble）**：PostgreSQL 仅保存 `disks`、`commits`、`client_sessions`；每个分支通过常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 和 1 MiB 数据块，使用 Set 和 ValueMerger 处理整块与局部写入并按区间解码，以硬链接 Checkpoint 支持快照和 Fork；常规写批次以 `pebble.NoSync` 提交异步 WAL，并由后台 1 秒刷盘兜底。FileHandle、FileLock、O_TMPFILE 与 O(1) 索引的 NamespaceCache 由客户端维护。
   客户端默认开启 FUSE 内核 `CAP_WRITEBACK_CACHE` 与 `FOPEN_KEEP_CACHE`，常规写由内核页缓存聚合回写；Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `11.78 MiB/s` | `319.49 MiB/s` | `306.07 MiB/s` | **0.04x** | **0.04x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,533.01 MiB/s` | `420.89 MiB/s` | `448.53 MiB/s` | **3.64x** | **3.42x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `690.10 files/s` | `538.70 files/s` | `849.90 files/s` | **1.28x** | **0.81x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `2,856.00 files/s` | `1,156.80 files/s` | `3,426.40 files/s` | **2.47x** | **0.83x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `117,526.50 files/s` | `6,231.00 files/s` | `8,944.30 files/s` | **18.86x** | **13.14x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `59.53 MiB/s` | `308.16 MiB/s` | `303.41 MiB/s` | **0.19x** | **0.20x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `185.46 MiB/s` | `184.50 MiB/s` | `182.09 MiB/s` | **1.01x** | **1.02x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `11.46 MiB/s` | `198.96 MiB/s` | `198.97 MiB/s` | **0.06x** | **0.06x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `181.68 MiB/s` | `176.21 MiB/s` | `177.33 MiB/s` | **1.03x** | **1.02x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `1,086.12 ops/s` | `411.79 ops/s` | `1,400.24 ops/s` | **2.64x** | **0.78x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `851.36 ops/s` | `812.13 ops/s` | `1,390.16 ops/s` | **1.05x** | **0.61x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `3,171.19 ops/s` | `3,210.07 ops/s` | `10,129.27 ops/s` | **0.99x** | **0.31x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `583.47 ops/s` | `657.49 ops/s` | `1,287.51 ops/s` | **0.89x** | **0.45x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9ffoqaxu0flnrleyb0` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.201` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9ffoqaxu0flnrleyb1` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.200` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.067/0.098/0.314/0.054` ms | `ping -c 20 -i 0.05 172.16.1.201` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.201 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `172.81` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `179.16` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `626dd03` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | 每分支 Pebble (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.201:9001/jfs-pg`) | rclone S3 (`http://172.16.1.201:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **回写缓存与持久化路径** | 内核 `writeback_cache` + `FOPEN_KEEP_CACHE`；`pebble.NoSync` 异步 WAL + 后台 1 秒刷盘，Flush 不触发 Fsync RPC | 客户端回写缓存 + 对象存储上传 | 客户端回写缓存 + 对象存储上传 |
| **数据分片** | `1 MiB`（对齐 FUSE `MaxWrite`），整片 Set、局部 ValueMerger 补丁 | `4 MiB` block / `64 MiB` chunk (默认) | `4 MiB` block / `64 MiB` chunk (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `11.78 MiB/s (347.73 s/file)` | `319.49 MiB/s (12.82 s/file)` | `306.07 MiB/s (13.38 s/file)` | **0.04x** | **0.04x** |
| **Read big file** | `1533.01 MiB/s (2.67 s/file)` | `420.89 MiB/s (9.73 s/file)` | `448.53 MiB/s (9.13 s/file)` | **3.64x** | **3.42x** |
| **Write small file** | `690.10 files/s (5.80 ms/file)` | `538.70 files/s (7.42 ms/file)` | `849.90 files/s (4.71 ms/file)` | **1.28x** | **0.81x** |
| **Read small file** | `2856.00 files/s (1.40 ms/file)` | `1156.80 files/s (3.46 ms/file)` | `3426.40 files/s (1.17 ms/file)` | **2.47x** | **0.83x** |
| **Stat file** | `117526.50 files/s (0.03 ms/file)` | `6231.00 files/s (0.64 ms/file)` | `8944.30 files/s (0.45 ms/file)` | **18.86x** | **13.14x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `59.53 MiB/s (14.9 IOPS)` | `308.16 MiB/s (77.0 IOPS)` | `303.41 MiB/s (75.8 IOPS)` | **0.19x** | **0.20x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `185.46 MiB/s (46.4 IOPS)` | `184.50 MiB/s (46.1 IOPS)` | `182.09 MiB/s (45.5 IOPS)` | **1.01x** | **1.02x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `11.46 MiB/s (2.9 IOPS)` | `198.96 MiB/s (49.7 IOPS)` | `198.97 MiB/s (49.7 IOPS)` | **0.06x** | **0.06x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `181.68 MiB/s (45.4 IOPS)` | `176.21 MiB/s (44.0 IOPS)` | `177.33 MiB/s (44.3 IOPS)` | **1.03x** | **1.02x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `14.78 MiB/s (14.8 IOPS)` | `333.63 MiB/s (333.6 IOPS)` | `333.28 MiB/s (333.3 IOPS)` | **0.04x** | **0.04x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `186.67 MiB/s (186.7 IOPS)` | `184.65 MiB/s (184.7 IOPS)` | `184.63 MiB/s (184.6 IOPS)` | **1.01x** | **1.01x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `9.38 MiB/s (9.4 IOPS)` | `306.95 MiB/s (306.9 IOPS)` | `308.02 MiB/s (308.0 IOPS)` | **0.03x** | **0.03x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `186.14 MiB/s (186.1 IOPS)` | `119.93 MiB/s (119.9 IOPS)` | `122.04 MiB/s (122.0 IOPS)` | **1.55x** | **1.53x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `66.82 / 137.36 / 160.43 ms` | `10.67 / 42.73 / 73.92 ms` | `11.21 / 43.25 / 74.97 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.56 / 31.06 / 38.01 ms` | `21.68 / 114.82 / 168.82 ms` | `21.96 / 121.11 / 179.31 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `5580.90 / 7885.29 / 9193.91 ms` | `308.71 / 367.00 / 446.69 ms` | `308.85 / 358.61 / 446.69 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `348.80 / 480.25 / 549.45 ms` | `347.23 / 1044.38 / 1400.90 ms` | `347.48 / 1052.77 / 1300.23 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `1,086.1` | `411.8` | `1,400.2` | **2.64x** | **0.78x** |
| **Directory stat** | `3,294.4` | `768.8` | `10,114.4` | **4.29x** | **0.33x** |
| **Directory removal** | `580.3` | `526.3` | `1,177.8` | **1.10x** | **0.49x** |
| **File creation** | `851.4` | `812.1` | `1,390.2` | **1.05x** | **0.61x** |
| **File stat** | `3,171.2` | `3,210.1` | `10,129.3` | **0.99x** | **0.31x** |
| **File read** | `2,582.2` | `1,685.6` | `2,892.6` | **1.53x** | **0.89x** |
| **File removal** | `583.5` | `657.5` | `1,287.5` | **0.89x** | **0.45x** |
| **Tree creation** | `1,084.2` | `680.4` | `1,444.3` | **1.59x** | **0.75x** |
| **Tree removal** | `543.2` | `565.9` | `1,207.0` | **0.96x** | **0.45x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `3,081.0` | `2,290.7` | `4,601.2` | **1.34x** | **0.67x** |
| **Directory stat** | `215,312.2` | `307,104.5` | `60,667.4` | **0.70x** | **3.55x** |
| **Directory removal** | `1,315.6` | `2,340.2` | `4,053.0` | **0.56x** | **0.32x** |
| **File creation** | `2,946.2` | `2,689.5` | `4,495.6` | **1.10x** | **0.66x** |
| **File stat** | `109,601.3` | `302,763.0` | `60,756.5` | **0.36x** | **1.80x** |
| **File read** | `26,513.2` | `11,754.9` | `14,027.2` | **2.26x** | **1.89x** |
| **File removal** | `2,691.3` | `2,680.2` | `5,001.2` | **1.00x** | **0.54x** |
| **Tree creation** | `354.8` | `250.9` | `465.8` | **1.41x** | **0.76x** |
| **Tree removal** | `339.8` | `262.2` | `330.7` | **1.30x** | **1.03x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`59.53 MiB/s`；同轮 JuiceFS(PG) 为 `308.16 MiB/s`。
- Bowl 单流顺序读：`185.46 MiB/s`；同轮 JuiceFS(PG) 为 `184.50 MiB/s`。
- Bowl 16 并发顺序写：`11.46 MiB/s`；同轮 JuiceFS(PG) 为 `198.96 MiB/s`。
- Bowl 16 并发顺序读：`181.68 MiB/s`；同轮 JuiceFS(PG) 为 `176.21 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `11.78 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `59.53 MiB/s`。4 并发聚合吞吐低于单流，说明并发写路径存在额外开销，需单独剖析；不能把 `juicefs bench` 数值当作 Bowl 的单流写上限。

Bowl 的写入和同步路径使用 Pebble；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261104-091737`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9ffoqaxu0flnrleyb0`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9ffoqaxu0flnrleyb1`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-04T09:17:46Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T10:24:19Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
