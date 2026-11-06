# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 16 仅保存 `disks`、`commits`、`client_sessions` 控制面表；每个 `(disk, branch)` 使用常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 与 12 字节 `BlobRef` 索引，文件数据落在 `blobs/<shard>/<blob_id>.blob` 的 1 MiB 不可变块文件中，整块直写、局部写 CoW，删除即时 `os.Remove` 回收，Checkpoint 与 Fork 以硬链接共享块；服务端分支锁降为拓扑生命周期 `RLock`，完整 1 MiB 块在 Inode 锁外并行落盘，单键点查免 Snapshot，读路径 atime 先登记内存再按 1 秒周期合并；常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`）。客户端采用 1 MiB Write-Through 直通写（禁用 `CAP_WRITEBACK_CACHE`）+ `FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存 + 写后 `AttrTimeout=1.0s` + `MaxBackground=64`；Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `289.14 MiB/s` | `202.97 MiB/s` | `61.65 MiB/s` | **1.42x** | **4.69x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,440.70 MiB/s` | `200.29 MiB/s` | `364.35 MiB/s` | **7.19x** | **3.95x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `562.00 files/s` | `2,304.20 files/s` | `99.50 files/s` | **0.24x** | **5.65x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `4,609.90 files/s` | `6,605.00 files/s` | `19.90 files/s` | **0.70x** | **231.65x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `128,315.00 files/s` | `19,287.10 files/s` | `33.30 files/s` | **6.65x** | **3853.30x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `184.91 MiB/s` | `258.23 MiB/s` | `199.76 MiB/s` | **0.72x** | **0.93x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `972.40 MiB/s` | `178.27 MiB/s` | `81.29 MiB/s` | **5.45x** | **11.96x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `571.11 ops/s` | `981.82 ops/s` | `144.86 ops/s` | **0.58x** | **3.94x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `3,772.70 ops/s` | `246.57 ops/s` | `247.00 ops/s` | **15.30x** | **15.27x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `3,080.29 ops/s` | `95.71 ops/s` | `60.84 ops/s` | **32.18x** | **50.63x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `2,920.27 ops/s` | `98.08 ops/s` | `51.59 ops/s` | **29.77x** | **56.60x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `0.77 s` | `1.91 s` | `6.13 s` | **2.47x** | **7.92x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `5,217.02 rows/s` | `7,580.42 rows/s` | `7,686.10 rows/s` | **0.69x** | **0.68x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj93j7f2ntjd6mgs48eq` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.251` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj93j7f2ntjd6mgs48er` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.250` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.063/0.088/0.178/0.027 ms` | `ping -c 20 -i 0.05 172.16.1.251` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.251 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `162.54` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `177.47` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `a253128` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | 每分支 Pebble 元数据 + `blobs/<shard>/<blob_id>.blob` 1 MiB 块文件 (`/var/lib/bowl/data`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | 1 MiB Write-Through 直通写（`writeback_cache=false`）+ `CAP_HANDLE_KILLPRIV` 负缓存；`pebble.NoSync` 异步 WAL（后台 1 秒刷盘）；仅 Fsync/Sync 写入持久化屏障 | 客户端本地 SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `289.14 MiB/s (14.17 s/file)` | `202.97 MiB/s (20.18 s/file)` | `61.65 MiB/s (66.44 s/file)` | **1.42x** | **4.69x** |
| **Read big file** | `1440.70 MiB/s (2.84 s/file)` | `200.29 MiB/s (20.45 s/file)` | `364.35 MiB/s (11.24 s/file)` | **7.19x** | **3.95x** |
| **Write small file** | `562.00 files/s (7.12 ms/file)` | `2304.20 files/s (1.74 ms/file)` | `99.50 files/s (40.19 ms/file)` | **0.24x** | **5.65x** |
| **Read small file** | `4609.90 files/s (0.87 ms/file)` | `6605.00 files/s (0.61 ms/file)` | `19.90 files/s (201.01 ms/file)` | **0.70x** | **231.65x** |
| **Stat file** | `128315.00 files/s (0.03 ms/file)` | `19287.10 files/s (0.21 ms/file)` | `33.30 files/s (120.18 ms/file)` | **6.65x** | **3853.30x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `184.91 (282.71) MiB/s` | `258.23 (424.43) MiB/s` | `199.76 (252.04) MiB/s` | **0.72x** | **0.93x** |
| `sequential_read` | **顺序读 (256 MiB)** | `972.40 (1,015.34) MiB/s` | `178.27 (184.18) MiB/s` | `81.29 (81.40) MiB/s` | **5.45x** | **11.96x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `8.39 (8.62) MiB/s` | `2.89 (6.02) MiB/s` | `5.06 (5.07) MiB/s` | **2.91x** | **1.66x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `571.11 (607.19) ops/s` | `981.82 (1,030.00) ops/s` | `144.86 (147.16) ops/s` | **0.58x** | **3.94x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `3,772.70 (3,953.63) ops/s` | `246.57 (251.74) ops/s` | `247.00 (247.49) ops/s` | **15.30x** | **15.27x** |
| `rename` | **目录重命名 rename (500 个)** | `3,080.29 (3,272.27) ops/s` | `95.71 (98.82) ops/s` | `60.84 (62.27) ops/s` | **32.18x** | **50.63x** |
| `unlink` | **文件删除 unlink (500 个)** | `2,920.27 (2,941.49) ops/s` | `98.08 (98.30) ops/s` | `51.59 (51.80) ops/s` | **29.77x** | **56.60x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `11,319.60 (37,984.81) ops/s` | `6,124.90 (6,587.69) ops/s` | `13,650.29 (14,159.62) ops/s` | **1.85x** | **0.83x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `176,172.96 (184,947.02) ops/s` | `42,323.43 (45,747.65) ops/s` | `73,663.59 (143,800.07) ops/s` | **4.16x** | **2.39x** |
| `stat` | **文件属性查询 stat (500 个)** | `66,981.87 (67,988.19) ops/s` | `19,756.71 (21,124.25) ops/s` | `38,945.79 (40,243.61) ops/s` | **3.39x** | **1.72x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.774s (0.794s)` | `1.910s (1.935s)` | `6.130s (6.145s)` | **2.47x** | **7.92x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.174s (0.178s)` | `1.959s (1.985s)` | `0.302s (0.310s)` | **11.23x** | **1.73x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.055s (0.151s)` | `0.780s (0.798s)` | `0.556s (0.564s)` | **14.19x** | **10.12x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.017s (0.028s)` | `0.030s (0.032s)` | `0.052s (0.054s)` | **1.78x** | **3.07x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.380s (1.690s)` | `0.371s (0.377s)` | `0.376s (0.448s)` | **0.98x** | **0.99x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `182.23 (273.89) MiB/s` | `269.82 (440.90) MiB/s` | `192.74 (261.69) MiB/s` | **0.68x** | **0.95x** |
| `sequential_read` | **顺序读 (256 MiB)** | `8,162.55 (8,269.28) MiB/s` | `2,283.92 (2,295.79) MiB/s` | `840.69 (860.94) MiB/s` | **3.57x** | **9.71x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `467.74 (476.69) MiB/s` | `178.64 (181.52) MiB/s` | `4.58 (4.59) MiB/s` | **2.62x** | **102.16x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `605.63 (625.72) ops/s` | `928.16 (975.08) ops/s` | `143.35 (146.46) ops/s` | **0.65x** | **4.22x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `3,291.52 (3,642.32) ops/s` | `250.78 (251.07) ops/s` | `238.65 (239.22) ops/s` | **13.13x** | **13.79x** |
| `rename` | **目录重命名 rename (500 个)** | `3,048.52 (3,127.58) ops/s` | `92.71 (93.70) ops/s` | `58.04 (59.25) ops/s` | **32.88x** | **52.53x** |
| `unlink` | **文件删除 unlink (500 个)** | `3,070.02 (3,087.57) ops/s` | `99.16 (99.44) ops/s` | `48.50 (51.66) ops/s` | **30.96x** | **63.31x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `481,602.77 (487,742.19) ops/s` | `139,697.97 (155,382.51) ops/s` | `183,837.05 (218,757.74) ops/s` | **3.45x** | **2.62x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `1,323,171.38 (1,368,655.60) ops/s` | `58,925.85 (59,906.05) ops/s` | `213,627.01 (260,188.57) ops/s` | **22.45x** | **6.19x** |
| `stat` | **文件属性查询 stat (500 个)** | `749,038.98 (804,654.53) ops/s` | `620,852.70 (642,356.06) ops/s` | `127,569.37 (500,493.61) ops/s` | **1.21x** | **5.87x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.739s (0.741s)` | `1.868s (1.918s)` | `6.278s (6.423s)` | **2.53x** | **8.49x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.029s (0.051s)` | `0.219s (0.219s)` | `0.052s (0.056s)` | **7.60x** | **1.82x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.024s (0.046s)` | `0.012s (0.012s)` | `0.013s (0.019s)` | **0.51x** | **0.56x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.003s (0.003s)` | `0.004s (0.005s)` | `0.004s (0.004s)` | **1.61x** | **1.34x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.037s (0.056s)` | `0.113s (0.116s)` | `0.148s (0.157s)` | **3.03x** | **3.98x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `638.29 files/s` | `814.97 files/s` | `156.65 files/s` | **0.78x** | **4.07x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `1,735.66 files/s` | `89.99 files/s` | `339.59 files/s` | **19.29x** | **5.11x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `53.85 MiB/s` | `58.18 MiB/s` | `49.14 MiB/s` | **0.93x** | **1.10x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `1,295.89 MiB/s` | `604.74 MiB/s` | `111.20 MiB/s` | **2.14x** | **11.65x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,309.15 MiB/s` | `881.85 MiB/s` | `61.51 MiB/s` | **1.48x** | **21.29x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `10,269.70 rows/s` | `6,606.84 rows/s` | `4,299.39 rows/s` | **1.55x** | **2.39x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `1,061.95 rows/s` | `1,122.77 rows/s` | `929.74 rows/s` | **0.95x** | **1.14x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `771,974.90 rows/s` | `640,073.61 rows/s` | `68,103.45 rows/s` | **1.21x** | **11.34x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `5,217.02 rows/s` | `7,580.42 rows/s` | `7,686.10 rows/s` | **0.69x** | **0.68x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `1,863.42 rows/s` | `2,852.45 rows/s` | `2,936.32 rows/s` | **0.65x** | **0.63x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `1,120,575.70 rows/s` | `1,181,040.61 rows/s` | `1,140,220.38 rows/s` | **0.95x** | **0.98x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0221 s` | `0.0086 s` | `0.0323 s` | **0.39x** | **1.46x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `289.14 MiB/s`，Drive9 Sync `61.65 MiB/s`，吞吐比 `4.69x`。
- `Read big file`：Bowl `1440.70 MiB/s`，Drive9 Sync `364.35 MiB/s`，吞吐比 `3.95x`。
- `Write small file`：Bowl `562.00 files/s`，Drive9 Sync `99.50 files/s`，吞吐比 `5.65x`。
- `Read small file`：Bowl `4609.90 files/s`，Drive9 Sync `19.90 files/s`，吞吐比 `231.65x`。
- `Stat file`：Bowl `128315.00 files/s`，Drive9 Sync `33.30 files/s`，吞吐比 `3853.30x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `289.14 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `184.91 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。本套件未测试整机断电恢复。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9bench-kp-20261106-135325`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj93j7f2ntjd6mgs48eq`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj93j7f2ntjd6mgs48er`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-06T13:53:52Z` |
| **实例销毁时间 (UTC)** | `2026-11-06T14:17:39Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
