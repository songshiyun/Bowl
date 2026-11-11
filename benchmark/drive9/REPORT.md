# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 16 仅保存单张 `disks` 控制面表（ADR-0032）；每个 `Disk` 由单个常驻 `StoreActor`（单写者串行执行器 + Pebble 引擎，ADR-0030/0031）承载，保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef`（`PackID + Offset + Length`）索引；文件数据追加写入 `packs/<pack_id_hex16>.pack` 的 64 MiB Append-Only Pack 文件中（`256 KiB` 逻辑块网格、`AppendBlocks` 整块零冗余拷贝、`WriteRequest` 池化零分配解码与尾块热缓存），常规写批次以 `pebble.NoSync` 提交异步 WAL，显式 `Fsync`/`Sync` 触发 Pack `fdatasync` + Pebble WAL `LogData(Sync)` 双层并行落盘屏障。客户端挂载时执行单次 `MountDisk` 握手获取 `WriterEpoch`（无后台心跳），禁用内核 `CAP_WRITEBACK_CACHE`，配备 `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`2 MiB` 同步探测 + `<64 KiB` 直通）+ 双端 `8M/32M` H2C 窗口 + `FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存 + 写后 `AttrTimeout=1.0s` + `MaxBackground=64`。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3（全部落于 Server 本地 NVMe SSD `/mnt/nvme`），客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 NVMe SSD Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地 NVMe 磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,699.27 MiB/s` | `1,319.70 MiB/s` | `675.41 MiB/s` | **1.29x** | **2.52x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,504.90 MiB/s` | `1,849.22 MiB/s` | `1,351.09 MiB/s` | **0.81x** | **1.11x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `1,392.00 files/s` | `1,877.60 files/s` | `90.70 files/s` | **0.74x** | **15.35x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `3,970.50 files/s` | `5,916.00 files/s` | `772.90 files/s` | **0.67x** | **5.14x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `115,646.00 files/s` | `14,741.00 files/s` | `18,735.90 files/s` | **7.85x** | **6.17x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `1,142.87 MiB/s` | `650.38 MiB/s` | `345.52 MiB/s` | **1.76x** | **3.31x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `653.52 MiB/s` | `575.15 MiB/s` | `53.54 MiB/s` | **1.14x** | **12.21x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `158.43 ops/s` | `2,120.29 ops/s` | `138.81 ops/s` | **0.07x** | **1.14x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `674.77 ops/s` | `231.63 ops/s` | `223.72 ops/s` | **2.91x** | **3.02x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `2,495.70 ops/s` | `91.15 ops/s` | `58.76 ops/s` | **27.38x** | **42.47x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `2,596.60 ops/s` | `99.28 ops/s` | `41.60 ops/s` | **26.15x** | **62.42x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `5.67 s` | `1.91 s` | `6.53 s` | **0.34x** | **1.15x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `666.79 rows/s` | `6,480.92 rows/s` | `7,723.18 rows/s` | **0.10x** | **0.09x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 + 本地数据盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj980079ks7kdbsi3ntz` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.194` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj980079ks7kdbsi3nu0` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.195` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.042/0.075/0.211/0.044 ms` | `ping -c 20 -i 0.05 172.16.1.194` |
| **内网单流 TCP 吞吐带宽** | `21.19` Gbps | `iperf3 -c 172.16.1.194 -t 5` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序写基线** | `1998.05` MiB/s | `/mnt/nvme` `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序读基线** | `3494.88` MiB/s | `/mnt/nvme` `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 节点本地 NVMe SSD 4K 随机写 / 随机读 IOPS** | `69953` / `699869` IOPS | `/mnt/nvme` `fio --rw=randwrite/randread --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 节点本地 NVMe SSD 4K 同步写 (`fsync=1`)** | `42099` IOPS (P99 `228.1` µs) | `/mnt/nvme` `fio --rw=write --bs=4k --iodepth=1 --direct=1 --fsync=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `9c60a34-dirty` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / 每盘单写者 `StoreActor` (Pebble) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | 每盘 `StoreActor` Pebble + `packs/<pack_id_hex16>.pack` 64 MiB Pack 文件 (`/var/lib/bowl/data` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`，单次 `MountDisk` 握手) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`writeback_cache=false`）+ 双端 `8M/32M` H2C 窗口 + `WriteRequest` 池化零分配解码；Group Commit `pebble.NoSync` + Fsync 双层并行落盘屏障 | 客户端本地 NVMe SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `1699.27 MiB/s (2.41 s/file)` | `1319.70 MiB/s (3.10 s/file)` | `675.41 MiB/s (6.06 s/file)` | **1.29x** | **2.52x** |
| **Read big file** | `1504.90 MiB/s (2.72 s/file)` | `1849.22 MiB/s (2.21 s/file)` | `1351.09 MiB/s (3.03 s/file)` | **0.81x** | **1.11x** |
| **Write small file** | `1392.00 files/s (2.87 ms/file)` | `1877.60 files/s (2.13 ms/file)` | `90.70 files/s (44.08 ms/file)` | **0.74x** | **15.35x** |
| **Read small file** | `3970.50 files/s (1.01 ms/file)` | `5916.00 files/s (0.68 ms/file)` | `772.90 files/s (5.18 ms/file)` | **0.67x** | **5.14x** |
| **Stat file** | `115646.00 files/s (0.03 ms/file)` | `14741.00 files/s (0.27 ms/file)` | `18735.90 files/s (0.21 ms/file)` | **7.85x** | **6.17x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `1,142.87 (1,173.36) MiB/s` | `650.38 (661.98) MiB/s` | `345.52 (348.14) MiB/s` | **1.76x** | **3.31x** |
| `sequential_read` | **顺序读 (256 MiB)** | `653.52 (676.77) MiB/s` | `575.15 (612.16) MiB/s` | `53.54 (53.55) MiB/s` | **1.14x** | **12.21x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `4.97 (5.29) MiB/s` | `2.17 (2.30) MiB/s` | `0.62 (0.67) MiB/s` | **2.29x** | **8.01x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `158.43 (160.75) ops/s` | `2,120.29 (2,187.08) ops/s` | `138.81 (140.41) ops/s` | **0.07x** | **1.14x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `674.77 (739.51) ops/s` | `231.63 (234.14) ops/s` | `223.72 (223.87) ops/s` | **2.91x** | **3.02x** |
| `rename` | **目录重命名 rename (500 个)** | `2,495.70 (2,626.58) ops/s` | `91.15 (91.64) ops/s` | `58.76 (58.98) ops/s` | **27.38x** | **42.47x** |
| `unlink` | **文件删除 unlink (500 个)** | `2,596.60 (2,638.21) ops/s` | `99.28 (99.70) ops/s` | `41.60 (41.95) ops/s` | **26.15x** | **62.42x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `28,514.56 (28,708.03) ops/s` | `10,846.35 (10,875.42) ops/s` | `24,925.35 (25,370.80) ops/s` | **2.63x** | **1.14x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `161,788.84 (176,802.19) ops/s` | `60,289.50 (63,733.78) ops/s` | `157,005.34 (160,515.72) ops/s` | **2.68x** | **1.03x** |
| `stat` | **文件属性查询 stat (500 个)** | `54,741.16 (58,167.48) ops/s` | `12,190.25 (12,614.18) ops/s` | `33,755.77 (35,229.76) ops/s` | **4.49x** | **1.62x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `5.668s (5.783s)` | `1.913s (1.930s)` | `6.535s (6.558s)` | **0.34x** | **1.15x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.080s (0.089s)` | `1.660s (1.752s)` | `0.761s (0.765s)` | **20.76x** | **9.52x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.030s (0.037s)` | `0.800s (0.857s)` | `0.131s (0.144s)` | **26.42x** | **4.34x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.047s (0.056s)` | `0.011s (0.011s)` | `0.048s (0.049s)` | **0.23x** | **1.01x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.429s (1.954s)` | `0.402s (0.417s)` | `0.381s (0.384s)` | **0.94x** | **0.89x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `1,120.72 (1,152.23) MiB/s` | `627.21 (644.85) MiB/s` | `346.91 (349.66) MiB/s` | **1.79x** | **3.23x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,827.43 (7,929.85) MiB/s` | `256.27 (1,750.76) MiB/s` | `606.01 (626.42) MiB/s` | **30.54x** | **12.92x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `540.25 (541.38) MiB/s` | `153.25 (161.32) MiB/s` | `2.72 (2.80) MiB/s` | **3.53x** | **198.54x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `151.86 (153.01) ops/s` | `2,170.70 (2,239.72) ops/s` | `137.98 (138.13) ops/s` | **0.07x** | **1.10x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `484.47 (698.53) ops/s` | `229.88 (230.67) ops/s` | `227.57 (230.04) ops/s` | **2.11x** | **2.13x** |
| `rename` | **目录重命名 rename (500 个)** | `2,526.40 (2,810.55) ops/s` | `89.25 (89.55) ops/s` | `56.39 (57.20) ops/s` | **28.31x** | **44.80x** |
| `unlink` | **文件删除 unlink (500 个)** | `2,726.98 (2,857.85) ops/s` | `96.04 (96.39) ops/s` | `44.80 (46.58) ops/s` | **28.39x** | **60.87x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `376,541.47 (421,375.82) ops/s` | `110,909.01 (133,049.56) ops/s` | `198,495.40 (212,583.47) ops/s` | **3.40x** | **1.90x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `1,063,198.65 (1,077,065.89) ops/s` | `80,094.33 (85,418.16) ops/s` | `215,641.89 (222,904.49) ops/s` | **13.27x** | **4.93x** |
| `stat` | **文件属性查询 stat (500 个)** | `740,831.84 (748,437.49) ops/s` | `587,325.29 (665,263.66) ops/s` | `504,475.71 (548,826.73) ops/s` | **1.26x** | **1.47x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `5.724s (5.985s)` | `1.966s (1.972s)` | `6.794s (6.848s)` | **0.34x** | **1.19x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.029s (0.031s)` | `0.219s (0.220s)` | `0.044s (0.065s)` | **7.51x** | **1.53x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.008s (0.010s)` | `0.013s (0.023s)` | `0.013s (0.013s)` | **1.71x** | **1.65x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.009s (0.010s)` | `0.004s (0.005s)` | `0.004s (0.005s)` | **0.48x** | **0.48x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.073s (0.080s)` | `0.118s (0.119s)` | `0.119s (0.159s)` | **1.62x** | **1.64x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `171.68 files/s` | `1,440.38 files/s` | `141.51 files/s` | **0.12x** | **1.21x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `461.99 files/s` | `54.51 files/s` | `64.07 files/s` | **8.47x** | **7.21x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `54.52 MiB/s` | `56.73 MiB/s` | `48.04 MiB/s` | **0.96x** | **1.13x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `1,243.36 MiB/s` | `684.33 MiB/s` | `49.19 MiB/s` | **1.82x** | **25.28x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,241.10 MiB/s` | `635.04 MiB/s` | `41.89 MiB/s` | **1.95x** | **29.63x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `1,342.21 rows/s` | `5,090.85 rows/s` | `3,757.02 rows/s` | **0.26x** | **0.36x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `126.71 rows/s` | `952.65 rows/s` | `809.20 rows/s` | **0.13x** | **0.16x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `599,801.32 rows/s` | `632,851.94 rows/s` | `56,530.94 rows/s` | **0.95x** | **10.61x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `666.79 rows/s` | `6,480.92 rows/s` | `7,723.18 rows/s` | **0.10x** | **0.09x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `274.44 rows/s` | `2,564.03 rows/s` | `2,734.14 rows/s` | **0.11x** | **0.10x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `984,910.86 rows/s` | `669,915.06 rows/s` | `751,747.22 rows/s` | **1.47x** | **1.31x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.1131 s` | `0.0088 s` | `0.0358 s` | **0.08x** | **0.32x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `1699.27 MiB/s`，Drive9 Sync `675.41 MiB/s`，吞吐比 `2.52x`。
- `Read big file`：Bowl `1504.90 MiB/s`，Drive9 Sync `1351.09 MiB/s`，吞吐比 `1.11x`。
- `Write small file`：Bowl `1392.00 files/s`，Drive9 Sync `90.70 files/s`，吞吐比 `15.35x`。
- `Read small file`：Bowl `3970.50 files/s`，Drive9 Sync `772.90 files/s`，吞吐比 `5.14x`。
- `Stat file`：Bowl `115646.00 files/s`，Drive9 Sync `18735.90 files/s`，吞吐比 `6.17x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1699.27 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `1142.87 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令同时清理 Client 与 Server 双端内核页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`），且各目标栈在 Server 端按进程隔离启停；但未在每轮内部重启 FUSE 进程或清空 Drive9 用户态内存缓存。因此本报告的 cold 表示双端 OS 页缓存清空下的冷状态。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0030/0031/0032 同时引入每盘单写者 `StoreActor`、64 MiB Pack 文件引擎与单次 `MountDisk` 握手；单轮数据反映当前 `labs/actor-architecture` 分支整体表现，本套件未测试整机断电恢复。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9bench-kp-20261110-184517`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj980079ks7kdbsi3ntz`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj980079ks7kdbsi3nu0`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-10T18:45:31Z` |
| **实例销毁时间 (UTC)** | `2026-11-10T19:10:52Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
