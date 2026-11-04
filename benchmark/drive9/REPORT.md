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
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `295.12 MiB/s` | `203.01 MiB/s` | `61.44 MiB/s` | **1.45x** | **4.80x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `886.89 MiB/s` | `215.34 MiB/s` | `317.63 MiB/s` | **4.12x** | **2.79x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `377.00 files/s` | `2,028.80 files/s` | `98.80 files/s` | **0.19x** | **3.82x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,578.30 files/s` | `6,906.10 files/s` | `140.40 files/s` | **0.23x** | **11.24x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `30,642.20 files/s` | `40,279.40 files/s` | `481.90 files/s` | **0.76x** | **63.59x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `183.19 MiB/s` | `271.20 MiB/s` | `195.48 MiB/s` | **0.68x** | **0.94x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `592.70 MiB/s` | `183.46 MiB/s` | `58.71 MiB/s` | **3.23x** | **10.10x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `339.50 ops/s` | `1,020.82 ops/s` | `145.57 ops/s` | **0.33x** | **2.33x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `909.84 ops/s` | `252.57 ops/s` | `244.17 ops/s` | **3.60x** | **3.73x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `860.24 ops/s` | `95.68 ops/s` | `60.26 ops/s` | **8.99x** | **14.28x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `593.28 ops/s` | `80.97 ops/s` | `52.27 ops/s` | **7.33x** | **11.35x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `1.13 s` | `1.93 s` | `6.16 s` | **1.71x** | **5.47x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `3,612.23 rows/s` | `7,025.59 rows/s` | `8,296.27 rows/s` | **0.51x** | **0.44x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj9irkcbex24zbqogdao` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.209` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj9irkcbex24zbqogdap` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.208` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.074/0.097/0.192/0.031 ms` | `ping -c 20 -i 0.05 172.16.1.209` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.209 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `192.75` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.16` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `1f7cde9` | Drive9 `b529a36f` | Drive9 `b529a36f` |
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
| **Write big file** | `295.12 MiB/s (13.88 s/file)` | `203.01 MiB/s (20.18 s/file)` | `61.44 MiB/s (66.67 s/file)` | **1.45x** | **4.80x** |
| **Read big file** | `886.89 MiB/s (4.62 s/file)` | `215.34 MiB/s (19.02 s/file)` | `317.63 MiB/s (12.90 s/file)` | **4.12x** | **2.79x** |
| **Write small file** | `377.00 files/s (10.61 ms/file)` | `2028.80 files/s (1.97 ms/file)` | `98.80 files/s (40.49 ms/file)` | **0.19x** | **3.82x** |
| **Read small file** | `1578.30 files/s (2.53 ms/file)` | `6906.10 files/s (0.58 ms/file)` | `140.40 files/s (28.49 ms/file)` | **0.23x** | **11.24x** |
| **Stat file** | `30642.20 files/s (0.13 ms/file)` | `40279.40 files/s (0.10 ms/file)` | `481.90 files/s (8.30 ms/file)` | **0.76x** | **63.59x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `183.19 (281.13) MiB/s` | `271.20 (417.11) MiB/s` | `195.48 (250.96) MiB/s` | **0.68x** | **0.94x** |
| `sequential_read` | **顺序读 (256 MiB)** | `592.70 (776.47) MiB/s` | `183.46 (184.59) MiB/s` | `58.71 (59.14) MiB/s` | **3.23x** | **10.10x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `7.61 (7.84) MiB/s` | `2.79 (2.80) MiB/s` | `5.09 (5.09) MiB/s` | **2.72x** | **1.50x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `339.50 (341.75) ops/s` | `1,020.82 (1,024.44) ops/s` | `145.57 (146.42) ops/s` | **0.33x** | **2.33x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `909.84 (943.61) ops/s` | `252.57 (253.44) ops/s` | `244.17 (249.83) ops/s` | **3.60x** | **3.73x** |
| `rename` | **目录重命名 rename (500 个)** | `860.24 (898.20) ops/s` | `95.68 (98.27) ops/s` | `60.26 (61.72) ops/s` | **8.99x** | **14.28x** |
| `unlink` | **文件删除 unlink (500 个)** | `593.28 (721.27) ops/s` | `80.97 (85.14) ops/s` | `52.27 (52.44) ops/s` | **7.33x** | **11.35x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `21,466.16 (24,657.22) ops/s` | `6,809.00 (6,877.16) ops/s` | `12,716.92 (13,099.57) ops/s` | **3.15x** | **1.69x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `47,619.06 (77,090.43) ops/s` | `46,113.02 (47,209.16) ops/s` | `98,313.84 (126,522.61) ops/s` | **1.03x** | **0.48x** |
| `stat` | **文件属性查询 stat (500 个)** | `3,275.29 (3,402.85) ops/s` | `21,472.23 (23,066.50) ops/s` | `35,988.87 (36,256.73) ops/s` | **0.15x** | **0.09x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `1.127s (1.209s)` | `1.929s (1.934s)` | `6.159s (6.271s)` | **1.71x** | **5.47x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.192s (0.255s)` | `2.024s (2.129s)` | `0.295s (0.349s)` | **10.53x** | **1.54x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.049s (0.115s)` | `0.790s (0.791s)` | `0.036s (0.507s)` | **16.25x** | **0.73x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.025s (0.029s)` | `0.016s (0.017s)` | `0.043s (0.051s)` | **0.64x** | **1.69x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.271s (1.979s)` | `0.348s (0.358s)` | `0.320s (0.451s)` | **1.28x** | **1.18x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `177.28 (279.79) MiB/s` | `259.58 (430.53) MiB/s` | `198.27 (257.97) MiB/s` | **0.68x** | **0.89x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,437.16 (7,468.48) MiB/s` | `1,897.13 (1,902.05) MiB/s` | `611.93 (635.18) MiB/s` | **3.92x** | **12.15x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `445.79 (457.23) MiB/s` | `179.25 (179.35) MiB/s` | `4.46 (4.47) MiB/s` | **2.49x** | **99.98x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `334.35 (339.78) ops/s` | `1,025.13 (1,105.77) ops/s` | `142.04 (144.23) ops/s` | **0.33x** | **2.35x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `925.57 (947.00) ops/s` | `241.77 (242.57) ops/s` | `241.28 (241.89) ops/s` | **3.83x** | **3.84x** |
| `rename` | **目录重命名 rename (500 个)** | `1,166.91 (1,186.60) ops/s` | `93.74 (94.93) ops/s` | `58.95 (59.54) ops/s` | **12.45x** | **19.80x** |
| `unlink` | **文件删除 unlink (500 个)** | `309.62 (343.24) ops/s` | `99.47 (99.91) ops/s` | `52.58 (52.78) ops/s` | **3.11x** | **5.89x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `346,692.55 (417,509.51) ops/s` | `107,198.37 (146,984.37) ops/s` | `238,882.98 (248,697.21) ops/s` | **3.23x** | **1.45x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `224,756.98 (230,169.12) ops/s` | `71,473.16 (80,982.31) ops/s` | `272,152.39 (281,062.43) ops/s` | **3.14x** | **0.83x** |
| `stat` | **文件属性查询 stat (500 个)** | `787,057.00 (801,271.10) ops/s` | `619,615.54 (625,200.81) ops/s` | `18,381.27 (31,389.16) ops/s` | **1.27x** | **42.82x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `1.098s (1.154s)` | `1.860s (1.938s)` | `6.172s (6.178s)` | **1.69x** | **5.62x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.021s (0.028s)` | `0.146s (0.214s)` | `0.061s (0.064s)` | **6.93x** | **2.89x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.008s (0.009s)` | `0.014s (0.016s)` | `0.012s (0.014s)` | **1.75x** | **1.48x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.003s (0.003s)` | `0.004s (0.004s)` | `0.004s (0.004s)` | **1.29x** | **1.11x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.067s (0.068s)` | `0.071s (0.122s)` | `0.174s (0.176s)` | **1.06x** | **2.60x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `353.24 files/s` | `889.29 files/s` | `158.61 files/s` | **0.40x** | **2.23x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `1,530.28 files/s` | `92.37 files/s` | `343.60 files/s` | **16.57x** | **4.45x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `53.81 MiB/s` | `61.27 MiB/s` | `50.04 MiB/s` | **0.88x** | **1.08x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `236.00 MiB/s` | `661.17 MiB/s` | `81.33 MiB/s` | **0.36x** | **2.90x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,272.87 MiB/s` | `786.65 MiB/s` | `47.59 MiB/s` | **1.62x** | **26.74x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `7,260.24 rows/s` | `5,059.77 rows/s` | `4,094.41 rows/s` | **1.43x** | **1.77x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `760.82 rows/s` | `917.42 rows/s` | `911.36 rows/s` | **0.83x** | **0.83x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `75,127.84 rows/s` | `570,089.88 rows/s` | `71,530.98 rows/s` | **0.13x** | **1.05x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `3,612.23 rows/s` | `7,025.59 rows/s` | `8,296.27 rows/s` | **0.51x** | **0.44x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `1,082.97 rows/s` | `2,701.66 rows/s` | `2,626.23 rows/s` | **0.40x** | **0.41x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `947,341.15 rows/s` | `986,584.76 rows/s` | `994,251.98 rows/s` | **0.96x** | **0.95x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0269 s` | `0.0091 s` | `0.0318 s` | **0.34x** | **1.18x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `295.12 MiB/s`，Drive9 Sync `61.44 MiB/s`，吞吐比 `4.80x`。
- `Read big file`：Bowl `886.89 MiB/s`，Drive9 Sync `317.63 MiB/s`，吞吐比 `2.79x`。
- `Write small file`：Bowl `377.00 files/s`，Drive9 Sync `98.80 files/s`，吞吐比 `3.82x`。
- `Read small file`：Bowl `1578.30 files/s`，Drive9 Sync `140.40 files/s`，吞吐比 `11.24x`。
- `Stat file`：Bowl `30642.20 files/s`，Drive9 Sync `481.90 files/s`，吞吐比 `63.59x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `295.12 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `183.19 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。本套件未测试整机断电恢复。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261104222150`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9irkcbex24zbqogdao`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9irkcbex24zbqogdap`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-04T14:22:05Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T14:45:56Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
