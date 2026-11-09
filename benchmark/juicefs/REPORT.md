# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费本地 NVMe SSD 双机环境（`ecs.i5.xlarge` × 2，1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机（4 vCPU / 32 GiB 内存）、VPC 私网与 894 GiB `local_ssd_pro` 本地 NVMe SSD 数据盘，服务端按目标隔离运行以下三组存储栈：

1. **Bowl（PostgreSQL 16 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack）**：采用 ADR-0030 / ADR-0031 / ADR-0032 架构，PostgreSQL 仅保存单张 `disks` 表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`；文件数据落在本地 NVMe SSD 的 `packs/pack-<id>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与 `64 MiB` 分片热块缓存，支持活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。
   客户端采用无会话 `RawFS` 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块本地 NVMe SSD 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）作为元数据引擎 + Server 节点本地 NVMe SSD rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,076.63 MiB/s` | `690.66 MiB/s` | `679.95 MiB/s` | **1.56x** | **1.58x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,671.20 MiB/s` | `1,649.37 MiB/s` | `1,684.02 MiB/s` | **1.01x** | **0.99x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `2,028.10 files/s` | `740.70 files/s` | `1,128.90 files/s` | **2.74x** | **1.80x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `4,089.90 files/s` | `3,028.80 files/s` | `3,196.30 files/s` | **1.35x** | **1.28x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `103,368.30 files/s` | `13,203.80 files/s` | `46,341.80 files/s` | **7.83x** | **2.23x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `429.13 MiB/s` | `678.71 MiB/s` | `671.15 MiB/s` | **0.63x** | **0.64x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `738.42 MiB/s` | `1,384.72 MiB/s` | `1,383.32 MiB/s` | **0.53x** | **0.53x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `1,059.29 MiB/s` | `670.46 MiB/s` | `669.06 MiB/s` | **1.58x** | **1.58x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `1,400.58 MiB/s` | `1,606.43 MiB/s` | `1,495.71 MiB/s` | **0.87x** | **0.94x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `4,274.69 ops/s` | `548.63 ops/s` | `2,357.86 ops/s` | **7.79x** | **1.81x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `2,531.41 ops/s` | `1,302.61 ops/s` | `2,274.95 ops/s` | **1.94x** | **1.11x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `102,006.51 ops/s` | `4,870.85 ops/s` | `16,010.14 ops/s` | **20.94x** | **6.37x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `2,634.70 ops/s` | `1,064.88 ops/s` | `2,147.08 ops/s` | **2.47x** | **1.23x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9byi6it3dlf5vzetpn` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme1n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.193` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9byi6it3dlf5vzetpo` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.192` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.032/0.056/0.258/0.049` ms | `ping -c 20 -i 0.05 172.16.1.193` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.193 -t 5` |
| **Server 本地 NVMe SSD (894 GiB) 顺序写基线** | `1998.05` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 本地 NVMe SSD (894 GiB) 顺序读基线** | `3494.88` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69951` / `699877` IOPS | Server 本地 `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `51499` IOPS (P99 `61.8` µs) | Server 本地 `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `c6e884e` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / 单盘 `StoreActor` Pebble | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点本地 NVMe SSD)** | 单盘 Pebble + `packs/pack-<id>.pack` `64 MiB` 封包文件 (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.193:9001/jfs-pg`) | rclone S3 (`http://172.16.1.193:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **回写缓存与持久化路径** | 1 MiB Write-Through 直通写（`writeback_cache=false`）+ `FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV`；Group Commit `pebble.NoSync` + Fsync 双层并行落盘屏障 | 客户端回写缓存 + 对象存储上传 | 客户端回写缓存 + 对象存储上传 |
| **数据分片** | `64 MiB` `.pack` Append-Only 封包 + `256 KiB` 逻辑块网格 + `64 MiB` 分片热块缓存 + `ExtendTail` | `4 MiB` block / `64 MiB` chunk (默认) | `4 MiB` block / `64 MiB` chunk (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `1076.63 MiB/s (3.80 s/file)` | `690.66 MiB/s (5.93 s/file)` | `679.95 MiB/s (6.02 s/file)` | **1.56x** | **1.58x** |
| **Read big file** | `1671.20 MiB/s (2.45 s/file)` | `1649.37 MiB/s (2.48 s/file)` | `1684.02 MiB/s (2.43 s/file)` | **1.01x** | **0.99x** |
| **Write small file** | `2028.10 files/s (1.97 ms/file)` | `740.70 files/s (5.40 ms/file)` | `1128.90 files/s (3.54 ms/file)` | **2.74x** | **1.80x** |
| **Read small file** | `4089.90 files/s (0.98 ms/file)` | `3028.80 files/s (1.32 ms/file)` | `3196.30 files/s (1.25 ms/file)` | **1.35x** | **1.28x** |
| **Stat file** | `103368.30 files/s (0.04 ms/file)` | `13203.80 files/s (0.30 ms/file)` | `46341.80 files/s (0.09 ms/file)` | **7.83x** | **2.23x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `429.13 MiB/s (107.3 IOPS)` | `678.71 MiB/s (169.7 IOPS)` | `671.15 MiB/s (167.8 IOPS)` | **0.63x** | **0.64x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `738.42 MiB/s (184.6 IOPS)` | `1384.72 MiB/s (346.2 IOPS)` | `1383.32 MiB/s (345.8 IOPS)` | **0.53x** | **0.53x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `1059.29 MiB/s (264.8 IOPS)` | `670.46 MiB/s (167.6 IOPS)` | `669.06 MiB/s (167.3 IOPS)` | **1.58x** | **1.58x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `1400.58 MiB/s (350.1 IOPS)` | `1606.43 MiB/s (401.6 IOPS)` | `1495.71 MiB/s (373.9 IOPS)` | **0.87x** | **0.94x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `1154.78 MiB/s (1154.8 IOPS)` | `735.90 MiB/s (735.9 IOPS)` | `729.09 MiB/s (729.1 IOPS)` | **1.57x** | **1.58x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `1462.33 MiB/s (1462.3 IOPS)` | `1624.11 MiB/s (1624.1 IOPS)` | `1576.60 MiB/s (1576.6 IOPS)` | **0.90x** | **0.93x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `1189.66 MiB/s (1189.7 IOPS)` | `344.98 MiB/s (345.0 IOPS)` | `338.96 MiB/s (339.0 IOPS)` | **3.45x** | **3.51x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `1280.80 MiB/s (1280.8 IOPS)` | `481.94 MiB/s (481.9 IOPS)` | `499.70 MiB/s (499.7 IOPS)` | **2.66x** | **2.56x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `7.56 / 8.36 / 9.11 ms` | `5.06 / 18.74 / 28.18 ms` | `5.12 / 17.70 / 26.34 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `5.41 / 9.11 / 16.91 ms` | `2.88 / 4.23 / 6.65 ms` | `2.89 / 4.36 / 6.32 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `58.87 / 76.02 / 92.80 ms` | `91.82 / 183.50 / 227.54 ms` | `91.36 / 183.50 / 223.35 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `45.41 / 77.07 / 90.70 ms` | `38.90 / 109.58 / 154.14 ms` | `38.52 / 121.11 / 189.79 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.02 ms` | `0.00 / 0.01 / 0.03 ms` | `0.00 / 0.01 / 0.03 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.02 ms` | `0.01 / 0.01 / 0.02 ms` | `0.01 / 0.01 / 0.02 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `4,274.7` | `548.6` | `2,357.9` | **7.79x** | **1.81x** |
| **Directory stat** | `112,138.1` | `3,198.1` | `16,768.7` | **35.06x** | **6.69x** |
| **Directory removal** | `2,686.9` | `939.1` | `2,023.9` | **2.86x** | **1.33x** |
| **File creation** | `2,531.4` | `1,302.6` | `2,275.0` | **1.94x** | **1.11x** |
| **File stat** | `102,006.5` | `4,870.8` | `16,010.1` | **20.94x** | **6.37x** |
| **File read** | `39,111.4` | `2,527.1` | `4,855.1` | **15.48x** | **8.06x** |
| **File removal** | `2,634.7` | `1,064.9` | `2,147.1` | **2.47x** | **1.23x** |
| **Tree creation** | `4,158.4` | `1,022.8` | `2,528.3` | **4.07x** | **1.64x** |
| **Tree removal** | `2,667.9` | `808.3` | `2,199.5` | **3.30x** | **1.21x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.i5.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `8,947.4` | `2,605.0` | `7,560.7` | **3.43x** | **1.18x** |
| **Directory stat** | `1,257,230.0` | `349,260.8` | `455,356.8` | **3.60x** | **2.76x** |
| **Directory removal** | `10,281.2` | `3,290.3` | `6,745.6` | **3.12x** | **1.52x** |
| **File creation** | `9,422.8` | `3,628.6` | `6,816.5` | **2.60x** | **1.38x** |
| **File stat** | `1,387,122.5` | `296,376.2` | `63,005.8` | **4.68x** | **22.02x** |
| **File read** | `46,762.9` | `12,908.4` | `17,321.3` | **3.62x** | **2.70x** |
| **File removal** | `12,127.1` | `3,467.2` | `8,116.7` | **3.50x** | **1.49x** |
| **Tree creation** | `1,916.1` | `188.1` | `575.5` | **10.19x** | **3.33x** |
| **Tree removal** | `1,293.9` | `410.2` | `1,716.0` | **3.15x** | **0.75x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`429.13 MiB/s`；同轮 JuiceFS(PG) 为 `678.71 MiB/s`。
- Bowl 单流顺序读：`738.42 MiB/s`；同轮 JuiceFS(PG) 为 `1384.72 MiB/s`。
- Bowl 16 并发顺序写：`1059.29 MiB/s`；同轮 JuiceFS(PG) 为 `670.46 MiB/s`。
- Bowl 16 并发顺序读：`1400.58 MiB/s`；同轮 JuiceFS(PG) 为 `1606.43 MiB/s`。

服务端本地 NVMe SSD 顺序读写与 4K IOPS 物理基线、请求大小与并发数见第 2、4 节。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`（及清理 `/var/jfsCache`），且三组存储栈服务端进程按目标隔离启停。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1076.63 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `429.13 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

Bowl 的写入和同步路径使用单盘 `StoreActor` Pebble 与 `64 MiB` `.pack` 封包文件；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理客户端内核页缓存，但不清理 JuiceFS 客户端本地块缓存与服务端页缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的双端缓存清理不能改变 `juicefs bench` 单次运行内部这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0030 / ADR-0031 / ADR-0032 同时改变控制面协议、单盘 Actor 并发模型与 64 MiB Pack 数据层；单轮数据不能将变化归因于其中某一项。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261109-044716`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9byi6it3dlf5vzetpn`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9byi6it3dlf5vzetpo`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-09T04:47:25Z` |
| **实例销毁时间 (UTC)** | `2026-11-09T04:58:00Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
