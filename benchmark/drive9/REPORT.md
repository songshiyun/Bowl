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
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `285.06 MiB/s` | `199.83 MiB/s` | `60.79 MiB/s` | **1.43x** | **4.69x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,730.56 MiB/s` | `226.26 MiB/s` | `298.77 MiB/s` | **7.65x** | **5.79x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `476.80 files/s` | `2,185.90 files/s` | `100.40 files/s` | **0.22x** | **4.75x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `4,664.40 files/s` | `6,746.10 files/s` | `13.70 files/s` | **0.69x** | **340.47x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `87,335.80 files/s` | `13,757.70 files/s` | `35.60 files/s` | **6.35x** | **2453.25x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `251.80 MiB/s` | `259.61 MiB/s` | `186.55 MiB/s` | **0.97x** | **1.35x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `828.94 MiB/s` | `184.25 MiB/s` | `58.66 MiB/s` | **4.50x** | **14.13x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `678.55 ops/s` | `1,014.78 ops/s` | `143.53 ops/s` | **0.67x** | **4.73x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `3,395.24 ops/s` | `253.69 ops/s` | `242.76 ops/s` | **13.38x** | **13.99x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `3,328.14 ops/s` | `98.71 ops/s` | `58.32 ops/s` | **33.72x** | **57.07x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `2,976.25 ops/s` | `99.04 ops/s` | `52.76 ops/s` | **30.05x** | **56.41x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `0.77 s` | `1.84 s` | `6.07 s` | **2.39x** | **7.88x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `5,149.00 rows/s` | `7,182.68 rows/s` | `7,366.29 rows/s` | **0.72x** | **0.70x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj9idhcgv03k662udgfc` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.222` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj9idhcgv03k662udgfd` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.223` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.033/0.062/0.153/0.029 ms` | `ping -c 20 -i 0.05 172.16.1.222` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.222 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `188.9` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.16` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `e078024` (R2d) | Drive9 `b529a36f` | Drive9 `b529a36f` |
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
| **Write big file** | `289.44 MiB/s (14.15 s/file)` | `199.13 MiB/s (20.57 s/file)` | `61.80 MiB/s (66.28 s/file)` | **1.45x** | **4.68x** |
| **Read big file** | `1592.58 MiB/s (2.57 s/file)` | `203.54 MiB/s (20.12 s/file)` | `364.73 MiB/s (11.23 s/file)` | **7.82x** | **4.37x** |
| **Write small file** | `342.50 files/s (11.68 ms/file)` | `1846.90 files/s (2.17 ms/file)` | `105.30 files/s (37.97 ms/file)` | **0.19x** | **3.25x** |
| **Read small file** | `1799.30 files/s (2.22 ms/file)` | `6967.60 files/s (0.57 ms/file)` | `324.40 files/s (12.33 ms/file)` | **0.26x** | **5.55x** |
| **Stat file** | `128625.70 files/s (0.03 ms/file)` | `76170.90 files/s (0.05 ms/file)` | `599.80 files/s (6.67 ms/file)` | **1.69x** | **214.45x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `240.83 (284.69) MiB/s` | `288.87 (442.70) MiB/s` | `192.03 (258.38) MiB/s` | **0.83x** | **1.25x** |
| `sequential_read` | **顺序读 (256 MiB)** | `862.65 (878.16) MiB/s` | `183.95 (185.01) MiB/s` | `61.48 (61.73) MiB/s` | **4.69x** | **14.03x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `9.08 (9.15) MiB/s` | `2.77 (2.78) MiB/s` | `5.16 (5.21) MiB/s` | **3.27x** | **1.76x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `371.73 (377.89) ops/s` | `996.47 (1,030.11) ops/s` | `150.33 (153.18) ops/s` | **0.37x** | **2.47x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `977.48 (979.15) ops/s` | `262.03 (264.03) ops/s` | `258.75 (262.54) ops/s` | **3.73x** | **3.78x** |
| `rename` | **目录重命名 rename (500 个)** | `1,205.92 (1,289.49) ops/s` | `100.95 (102.97) ops/s` | `65.02 (65.18) ops/s` | **11.95x** | **18.55x** |
| `unlink` | **文件删除 unlink (500 个)** | `1,746.05 (1,757.31) ops/s` | `107.04 (107.63) ops/s` | `59.42 (62.07) ops/s` | **16.31x** | **29.38x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `25,466.87 (29,107.78) ops/s` | `7,245.08 (7,371.67) ops/s` | `14,320.64 (17,753.71) ops/s` | **3.52x** | **1.78x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `136,097.23 (161,855.19) ops/s` | `43,619.01 (48,657.57) ops/s` | `54,054.84 (120,638.05) ops/s` | **3.12x** | **2.52x** |
| `stat` | **文件属性查询 stat (500 个)** | `49,287.73 (57,454.07) ops/s` | `20,806.56 (21,180.68) ops/s` | `41,127.53 (47,552.68) ops/s` | **2.37x** | **1.20x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.967s (0.989s)` | `1.815s (1.880s)` | `5.723s (5.731s)` | **1.88x** | **5.92x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.099s (0.157s)` | `1.678s (1.726s)` | `0.236s (0.238s)` | **16.95x** | **2.38x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.088s (0.131s)` | `0.764s (0.770s)` | `0.036s (0.038s)` | **8.72x** | **0.41x** |
| `macro_find_files` | **目录树遍历 (find . -type f \| wc -l)** | `0.017s (0.019s)` | `0.016s (0.019s)` | `0.040s (0.049s)` | **0.98x** | **2.40x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.289s (1.814s)` | `0.355s (0.380s)` | `0.321s (0.336s)` | **1.23x** | **1.11x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `309.34 (498.10) MiB/s` | `259.38 (426.10) MiB/s` | `195.73 (250.72) MiB/s` | **1.19x** | **1.58x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,951.04 (8,134.68) MiB/s` | `2,225.37 (2,259.36) MiB/s` | `723.18 (724.63) MiB/s` | **3.57x** | **10.99x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `470.92 (473.57) MiB/s` | `177.57 (177.76) MiB/s` | `5.03 (5.06) MiB/s` | **2.65x** | **93.66x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `358.48 (362.77) ops/s` | `1,031.05 (1,046.36) ops/s` | `152.98 (153.01) ops/s` | **0.35x** | **2.34x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `996.72 (997.28) ops/s` | `264.44 (267.64) ops/s` | `259.12 (260.24) ops/s` | **3.77x** | **3.85x** |
| `rename` | **目录重命名 rename (500 个)** | `1,276.61 (1,279.44) ops/s` | `97.68 (98.75) ops/s` | `58.25 (62.00) ops/s` | **13.07x** | **21.92x** |
| `unlink` | **文件删除 unlink (500 个)** | `1,813.93 (1,936.68) ops/s` | `103.97 (104.33) ops/s` | `57.80 (62.15) ops/s` | **17.45x** | **31.38x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `466,886.10 (475,290.65) ops/s` | `123,676.66 (172,182.99) ops/s` | `194,861.50 (258,847.85) ops/s` | **3.78x** | **2.40x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `1,298,397.78 (1,303,732.41) ops/s` | `88,905.82 (92,453.34) ops/s` | `244,595.78 (255,283.61) ops/s` | **14.60x** | **5.31x** |
| `stat` | **文件属性查询 stat (500 个)** | `793,672.21 (806,071.50) ops/s` | `664,401.50 (689,168.06) ops/s` | `453,146.47 (473,365.48) ops/s` | **1.19x** | **1.75x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.902s (0.948s)` | `1.818s (1.854s)` | `5.819s (5.824s)` | **2.02x** | **6.45x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.031s (0.041s)` | `0.212s (0.228s)` | `0.051s (0.051s)` | **6.76x** | **1.61x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.025s (0.035s)` | `0.011s (0.012s)` | `0.018s (0.019s)` | **0.45x** | **0.71x** |
| `macro_find_files` | **目录树遍历 (find . -type f \| wc -l)** | `0.003s (0.003s)` | `0.004s (0.004s)` | `0.004s (0.004s)` | **1.63x** | **1.38x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.063s (0.073s)` | `0.108s (0.111s)` | `0.132s (0.155s)` | **1.72x** | **2.11x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `661.24 files/s` | `884.24 files/s` | `159.41 files/s` | **0.75x** | **4.15x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `1,810.40 files/s` | `99.96 files/s` | `346.91 files/s` | **18.11x** | **5.22x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `54.76 MiB/s` | `57.59 MiB/s` | `49.22 MiB/s` | **0.95x** | **1.11x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `1,216.60 MiB/s` | `612.94 MiB/s` | `78.41 MiB/s` | **1.98x** | **15.52x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,240.70 MiB/s` | `838.61 MiB/s` | `49.01 MiB/s` | **1.48x** | **25.32x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `9,531.95 rows/s` | `6,747.34 rows/s` | `4,517.69 rows/s` | **1.41x** | **2.11x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `1,072.36 rows/s` | `882.84 rows/s` | `952.24 rows/s` | **1.21x** | **1.13x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `190,880.39 rows/s` | `721,010.99 rows/s` | `68,750.19 rows/s` | **0.26x** | **2.78x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `5,072.03 rows/s` | `8,256.06 rows/s` | `7,672.94 rows/s` | **0.61x** | **0.66x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `1,691.32 rows/s` | `3,091.66 rows/s` | `2,804.26 rows/s` | **0.55x** | **0.60x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `1,213,868.45 rows/s` | `1,112,898.32 rows/s` | `956,387.25 rows/s` | **1.09x** | **1.27x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0184 s` | `0.0082 s` | `0.0291 s` | **0.44x** | **1.58x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `289.44 MiB/s`，Drive9 Sync `61.80 MiB/s`，吞吐比 `4.68x`。
- `Read big file`：Bowl `1592.58 MiB/s`，Drive9 Sync `364.73 MiB/s`，吞吐比 `4.37x`。
- `Write small file`：Bowl `342.50 files/s`，Drive9 Sync `105.30 files/s`，吞吐比 `3.25x`。
- `Read small file`：Bowl `1799.30 files/s`，Drive9 Sync `324.40 files/s`，吞吐比 `5.55x`。
- `Stat file`：Bowl `128625.70 files/s`，Drive9 Sync `599.80 files/s`，吞吐比 `214.45x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `289.44 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `240.83 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。本套件未测试整机断电恢复。

---

## 7. Bowl 本轮相对上一轮基线的提升（Round 2 `c7ff002` vs Baseline `2c02e71`）

本节只比较 Bowl 自身两个版本，数据来自同一套 Drive9 基准脚本。微观与宏观工作负载各 3 次样本取中位数；宏观为耗时，比值按 `Baseline 耗时 / Round 2 耗时` 折算，>1 表示加速。

### 7.1 Cold 微观与宏观

| 指标 | 单位 | Baseline `2c02e71` | Round 2 `c7ff002` | 提升 |
| :--- | :--- | ---: | ---: | ---: |
| `sequential_write` | MiB/s | 183.19 | 240.83 | **1.315x** |
| `sequential_read` | MiB/s | 592.70 | 862.65 | **1.455x** |
| `random_read_4k` | MiB/s | 7.61 | 9.08 | 1.193x |
| `small_write_4k_fsync` | ops/s | 339.50 | 371.73 | 1.095x |
| `mkdir` | ops/s | 909.84 | 977.48 | 1.074x |
| `rename` | ops/s | 860.24 | 1,205.92 | **1.402x** |
| `unlink` | ops/s | 593.28 | 1,746.05 | **2.943x** |
| `readdir_small` | ops/s | 21,466.16 | 25,466.87 | 1.186x |
| `readdir_large` | ops/s | 47,619.06 | 136,097.23 | **2.858x** |
| `stat` | ops/s | 3,275.29 | 49,287.73 | **15.048x** |
| `macro_git_clone` | s | 1.127 | 0.9674 | 1.165x |
| `macro_git_status_dirty` | s | 0.1920 | 0.0990 | **1.940x** |
| `macro_git_diff_dirty` | s | 0.0490 | 0.0876 | 0.559x |
| `macro_find_files` | s | 0.0250 | 0.0165 | 1.512x |
| `macro_go_build` | s | 0.2710 | 0.2893 | 0.937x |

### 7.2 Hot 微观与宏观

| 指标 | 单位 | Baseline `2c02e71` | Round 2 `c7ff002` | 提升 |
| :--- | :--- | ---: | ---: | ---: |
| `sequential_write` | MiB/s | 177.28 | 309.34 | **1.745x** |
| `sequential_read` | MiB/s | 7,437.16 | 7,951.04 | 1.069x |
| `random_read_4k` | MiB/s | 445.79 | 470.92 | 1.056x |
| `small_write_4k_fsync` | ops/s | 334.35 | 358.48 | 1.072x |
| `mkdir` | ops/s | 925.57 | 996.72 | 1.077x |
| `rename` | ops/s | 1,166.91 | 1,276.61 | 1.094x |
| `unlink` | ops/s | 309.62 | 1,813.93 | **5.859x** |
| `readdir_small` | ops/s | 346,692.55 | 466,886.10 | 1.347x |
| `readdir_large` | ops/s | 224,756.98 | 1,298,397.78 | **5.777x** |
| `stat` | ops/s | 787,057.00 | 793,672.21 | 1.008x |
| `macro_git_clone` | s | 1.098 | 0.9016 | 1.218x |
| `macro_git_status_dirty` | s | 0.0210 | 0.0314 | 0.669x |
| `macro_git_diff_dirty` | s | 0.0080 | 0.0254 | 0.316x |
| `macro_find_files` | s | 0.0030 | 0.0027 | 1.119x |
| `macro_go_build` | s | 0.0670 | 0.0625 | 1.071x |

### 7.3 完整性与 SQLite 事务基准

| 指标 | 单位 | Baseline `2c02e71` | Round 2 `c7ff002` | 提升 |
| :--- | :--- | ---: | ---: | ---: |
| 64×1KiB 小文件写 (`fsync` + dir `fsync`) | files/s | 353.24 | 467.88 | 1.325x |
| 64×1KiB 小文件 SHA-256 校验读 | files/s | 1,530.28 | 2,057.85 | 1.345x |
| 16 MiB 大文件写 (`fsync` + dir `fsync`) | MiB/s | 53.81 | 53.55 | 0.995x |
| 16 MiB 大文件读 (Pass 1) | MiB/s | 236.00 | 288.35 | **1.222x** |
| 16 MiB 大文件读 (Pass 2) | MiB/s | 1,272.87 | 1,279.19 | 1.005x |
| SQLite `DELETE` 插入 (256 rows) | rows/s | 7,260.24 | 9,531.95 | **1.313x** |
| SQLite `DELETE` 更新 (86 rows) | rows/s | 760.82 | 1,072.36 | **1.409x** |
| SQLite `DELETE` 全表读 (256 rows) | rows/s | 75,127.84 | 190,880.39 | **2.541x** |
| SQLite `WAL` 插入 (256 rows) | rows/s | 3,612.23 | 5,072.03 | **1.404x** |
| SQLite `WAL` 更新 (86 rows) | rows/s | 1,082.97 | 1,691.32 | **1.562x** |
| SQLite `WAL` 全表读 (256 rows) | rows/s | 947,341.15 | 1,213,868.45 | 1.281x |
| SQLite `WAL` checkpoint(TRUNCATE) | s | 0.0269 | 0.0184 | **1.462x** |

**分析**：Cold `sequential_read`（1.455x）、Cold `stat`（15.048x）、Cold `readdir_large`（2.858x）、Hot `sequential_write`（1.745x）、Hot `readdir_large`（5.777x）与 Hot `unlink`（5.859x）均显著提升；`16 MiB` 读 Pass 1 从 236.00 提升到 288.35 MiB/s（1.222x），SQLite WAL 插入/更新分别达到 5,072 / 1,691 rows/s。`macro_git_status_dirty` 与 `macro_git_diff_dirty` 仍低于基线（0.669x / 0.316x），但绝对差仅 10~17 ms，且 3 次样本区间与基线重叠，属亚 50 ms 计时方差，详见验证报告 §7.3。

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261105022559`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9idhcgv03k662udgfc`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9idhcgv03k662udgfd`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-04T18:26:16Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T18:47:35Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
