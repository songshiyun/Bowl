# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

由于 Drive9 默认采用客户端本地 Write-Back 缓存 + 本地 `.git`/构建目录 Overlay（`--profile=coding-agent --durability=interactive`），而 Bowl 采用无客户端本地盘缓存的跨机同步直通持久化契约，为兼顾「默认开箱体验对比」与「同步提交模式对比」，本次评测在同台机器上依次测试了三组目标：

1. **Bowl (`bowl`)**：PostgreSQL 16 + Server BlockStore，采用生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，客户端零本地盘缓存、每次写入直通服务端落盘）。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `178.45 MiB/s` | `201.88 MiB/s` | `65.10 MiB/s` | **0.88x** | **2.74x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,583.08 MiB/s` | `182.40 MiB/s` | `431.69 MiB/s` | **8.68x** | **3.67x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `392.60 files/s` | `536.40 files/s` | `100.10 files/s` | **0.73x** | **3.92x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,341.00 files/s` | `6,411.10 files/s` | `306.30 files/s` | **0.21x** | **4.38x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `7,094.00 files/s` | `65,244.20 files/s` | `488.00 files/s` | **0.11x** | **14.54x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `242.69 MiB/s` | `259.22 MiB/s` | `189.01 MiB/s` | **0.94x** | **1.28x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `861.36 MiB/s` | `177.68 MiB/s` | `83.41 MiB/s` | **4.85x** | **10.33x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `154.09 ops/s` | `1,023.80 ops/s` | `146.12 ops/s` | **0.15x** | **1.05x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `451.88 ops/s` | `250.22 ops/s` | `249.12 ops/s` | **1.81x** | **1.81x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `410.61 ops/s` | `95.39 ops/s` | `58.03 ops/s` | **4.30x** | **7.08x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `422.66 ops/s` | `99.54 ops/s` | `56.15 ops/s` | **4.25x** | **7.53x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `4.66 s` | `1.92 s` | `6.07 s` | **0.41x** | **1.30x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `1,034.24 rows/s` | `7,328.53 rows/s` | `8,111.27 rows/s` | **0.14x** | **0.13x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj9dcseepzra184oqk7y` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.195` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj9dcseepzra184oqk7z` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.194` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.075/0.101/0.185/0.029 ms` | `ping -c 20 -i 0.05 172.16.1.195` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.195 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `191.13` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `174.98` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `f2676ca+dirty` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **服务端元数据引擎** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`127.0.0.1:5432`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化契约** | 每次 `Write`/`Flush` 同步直通 Server 落盘 + Group Commit | 客户端本地 SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `178.45 MiB/s (22.95 s/file)` | `201.88 MiB/s (20.29 s/file)` | `65.10 MiB/s (62.92 s/file)` | **0.88x** | **2.74x** |
| **Read big file** | `1583.08 MiB/s (2.59 s/file)` | `182.40 MiB/s (22.46 s/file)` | `431.69 MiB/s (9.49 s/file)` | **8.68x** | **3.67x** |
| **Write small file** | `392.60 files/s (10.19 ms/file)` | `536.40 files/s (7.46 ms/file)` | `100.10 files/s (39.97 ms/file)` | **0.73x** | **3.92x** |
| **Read small file** | `1341.00 files/s (2.98 ms/file)` | `6411.10 files/s (0.62 ms/file)` | `306.30 files/s (13.06 ms/file)` | **0.21x** | **4.38x** |
| **Stat file** | `7094.00 files/s (0.56 ms/file)` | `65244.20 files/s (0.06 ms/file)` | `488.00 files/s (8.20 ms/file)` | **0.11x** | **14.54x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `242.69 (249.03) MiB/s` | `259.22 (430.81) MiB/s` | `189.01 (260.05) MiB/s` | **0.94x** | **1.28x** |
| `sequential_read` | **顺序读 (256 MiB)** | `861.36 (911.44) MiB/s` | `177.68 (180.64) MiB/s` | `83.41 (83.70) MiB/s` | **4.85x** | **10.33x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.61 (6.81) MiB/s` | `2.83 (3.03) MiB/s` | `5.09 (5.09) MiB/s` | **2.34x** | **1.30x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `154.09 (157.31) ops/s` | `1,023.80 (1,050.87) ops/s` | `146.12 (150.04) ops/s` | **0.15x** | **1.05x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `451.88 (452.04) ops/s` | `250.22 (251.32) ops/s` | `249.12 (250.73) ops/s` | **1.81x** | **1.81x** |
| `rename` | **目录重命名 rename (500 个)** | `410.61 (417.70) ops/s` | `95.39 (98.29) ops/s` | `58.03 (61.39) ops/s` | **4.30x** | **7.08x** |
| `unlink` | **文件删除 unlink (500 个)** | `422.66 (462.64) ops/s` | `99.54 (100.01) ops/s` | `56.15 (56.41) ops/s` | **4.25x** | **7.53x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `4,889.85 (5,486.87) ops/s` | `6,745.22 (7,056.20) ops/s` | `14,092.77 (14,812.10) ops/s` | **0.72x** | **0.35x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `12,771.12 (13,487.04) ops/s` | `42,657.30 (46,231.36) ops/s` | `127,356.25 (156,989.93) ops/s` | **0.30x** | **0.10x** |
| `stat` | **文件属性查询 stat (500 个)** | `2,848.20 (2,919.88) ops/s` | `21,478.90 (24,317.36) ops/s` | `37,380.86 (44,876.25) ops/s` | **0.13x** | **0.08x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `4.660s (4.665s)` | `1.925s (1.937s)` | `6.069s (6.136s)` | **0.41x** | **1.30x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.245s (0.251s)` | `1.794s (1.802s)` | `0.335s (0.352s)` | **7.32x** | **1.36x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.177s (0.181s)` | `0.774s (0.775s)` | `0.132s (0.520s)` | **4.37x** | **0.74x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.099s (0.100s)` | `0.015s (0.021s)` | `0.041s (0.057s)` | **0.16x** | **0.42x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.620s (2.165s)` | `0.365s (0.385s)` | `0.350s (0.384s)` | **0.59x** | **0.56x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `249.83 (256.30) MiB/s` | `246.31 (435.88) MiB/s` | `186.27 (276.95) MiB/s` | **1.01x** | **1.34x** |
| `sequential_read` | **顺序读 (256 MiB)** | `371.62 (427.74) MiB/s` | `2,360.32 (2,372.76) MiB/s` | `836.05 (871.26) MiB/s` | **0.16x** | **0.44x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.73 (6.74) MiB/s` | `182.77 (189.64) MiB/s` | `4.60 (4.66) MiB/s` | **0.04x** | **1.46x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `158.92 (163.38) ops/s` | `1,021.35 (1,052.05) ops/s` | `146.12 (146.86) ops/s` | **0.16x** | **1.09x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `534.09 (546.93) ops/s` | `249.73 (250.58) ops/s` | `239.97 (249.48) ops/s` | **2.14x** | **2.23x** |
| `rename` | **目录重命名 rename (500 个)** | `399.84 (414.67) ops/s` | `93.27 (94.93) ops/s` | `55.23 (55.96) ops/s` | **4.29x** | **7.24x** |
| `unlink` | **文件删除 unlink (500 个)** | `482.27 (506.02) ops/s` | `101.26 (101.97) ops/s` | `56.10 (56.21) ops/s` | **4.76x** | **8.60x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `5,922.15 (6,879.18) ops/s` | `121,028.74 (152,705.91) ops/s` | `247,255.46 (257,828.80) ops/s` | **0.05x** | **0.02x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `12,923.38 (13,111.49) ops/s` | `83,904.92 (87,302.95) ops/s` | `256,214.35 (263,180.84) ops/s` | **0.15x** | **0.05x** |
| `stat` | **文件属性查询 stat (500 个)** | `72,263.94 (73,518.96) ops/s` | `611,469.70 (612,057.20) ops/s` | `447,660.84 (462,234.44) ops/s` | **0.12x** | **0.16x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `4.630s (4.728s)` | `1.891s (1.906s)` | `6.175s (6.226s)` | **0.41x** | **1.33x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.168s (0.171s)` | `0.214s (0.218s)` | `0.050s (0.061s)` | **1.28x** | **0.30x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.053s (0.093s)` | `0.012s (0.014s)` | `0.018s (0.018s)` | **0.23x** | **0.34x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.071s (0.103s)` | `0.004s (0.004s)` | `0.004s (0.004s)` | **0.05x** | **0.06x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.291s (0.304s)` | `0.114s (0.123s)` | `0.168s (0.168s)` | **0.39x** | **0.58x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `155.38 files/s` | `927.68 files/s` | `160.93 files/s` | **0.17x** | **0.97x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `317.38 files/s` | `93.04 files/s` | `357.26 files/s` | **3.41x** | **0.89x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `46.29 MiB/s` | `58.98 MiB/s` | `48.26 MiB/s` | **0.78x** | **0.96x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `51.89 MiB/s` | `658.46 MiB/s` | `105.97 MiB/s` | **0.08x** | **0.49x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `58.47 MiB/s` | `731.63 MiB/s` | `62.40 MiB/s` | **0.08x** | **0.94x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `1,994.49 rows/s` | `6,234.48 rows/s` | `3,575.29 rows/s` | **0.32x** | **0.56x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `193.60 rows/s` | `1,143.85 rows/s` | `895.19 rows/s` | **0.17x** | **0.22x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `29,776.32 rows/s` | `596,938.36 rows/s` | `68,310.08 rows/s` | **0.05x** | **0.44x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `1,034.24 rows/s` | `7,328.53 rows/s` | `8,111.27 rows/s` | **0.14x** | **0.13x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `358.91 rows/s` | `2,559.06 rows/s` | `2,769.94 rows/s` | **0.14x** | **0.13x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `62,230.57 rows/s` | `999,773.49 rows/s` | `952,359.69 rows/s` | **0.06x** | **0.07x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.1034 s` | `0.0088 s` | `0.0328 s` | **0.08x** | **0.32x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `178.45 MiB/s`，Drive9 Sync `65.10 MiB/s`，吞吐比 `2.74x`。
- `Read big file`：Bowl `1583.08 MiB/s`，Drive9 Sync `431.69 MiB/s`，吞吐比 `3.67x`。
- `Write small file`：Bowl `392.60 files/s`，Drive9 Sync `100.10 files/s`，吞吐比 `3.92x`。
- `Read small file`：Bowl `1341.00 files/s`，Drive9 Sync `306.30 files/s`，吞吐比 `4.38x`。
- `Stat file`：Bowl `7094.00 files/s`，Drive9 Sync `488.00 files/s`，吞吐比 `14.54x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261101010738`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9dcseepzra184oqk7y`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9dcseepzra184oqk7z`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-10-31T17:07:53Z` |
| **实例销毁时间 (UTC)** | `2026-10-31T17:32:34Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
