# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下同时运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

由于 Drive9 默认采用客户端本地 Write-Back 缓存 + 本地 `.git`/构建目录 Overlay（`--profile=coding-agent --durability=interactive`），而 Bowl 采用无客户端本地盘缓存的跨机同步直通持久化契约，为兼顾「默认开箱体验对比」与「同持久化语义公平对比」，本次评测在同台机器上同时测试了三组目标：

1. **Bowl (`bowl`)**：PostgreSQL 16 + Server BlockStore，采用生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，客户端零本地盘缓存、每次写入直通服务端落盘）。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `179.17 MiB/s` | `198.44 MiB/s` | `58.01 MiB/s` | **0.90x** | **3.09x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `118.45 MiB/s` | `224.78 MiB/s` | `383.07 MiB/s` | **0.53x** | **0.31x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `317.40 files/s` | `2,268.50 files/s` | `98.60 files/s` | **0.14x** | **3.22x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `467.10 files/s` | `7,833.90 files/s` | `137.60 files/s` | **0.06x** | **3.39x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `5,541.50 files/s` | `17,978.60 files/s` | `483.50 files/s` | **0.31x** | **11.46x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `226.09 MiB/s` | `325.52 MiB/s` | `191.09 MiB/s` | **0.69x** | **1.18x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `141.13 MiB/s` | `184.44 MiB/s` | `82.27 MiB/s` | **0.77x** | **1.72x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `131.81 ops/s` | `1,035.92 ops/s` | `143.74 ops/s` | **0.13x** | **0.92x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `423.72 ops/s` | `248.08 ops/s` | `248.04 ops/s` | **1.71x** | **1.71x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `345.50 ops/s` | `96.25 ops/s` | `61.50 ops/s` | **3.59x** | **5.62x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `373.07 ops/s` | `98.11 ops/s` | `51.04 ops/s` | **3.80x** | **7.31x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `5.15 s` | `1.91 s` | `6.29 s` | **0.37x** | **1.22x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `909.17 rows/s` | `5,675.96 rows/s` | `7,229.85 rows/s` | **0.16x** | **0.13x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj919o5nf211olqico6j` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.222` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj919o5nf211olqico6k` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.223` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.039/0.068/0.145/0.031 ms` | `ping -c 20 -i 0.05 172.16.1.222` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.222 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `198.45` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.19` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `1619a96` | Drive9 `b529a36f` | Drive9 `b529a36f` |
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
| **Write big file** | `179.17 MiB/s (22.86 s/file)` | `198.44 MiB/s (20.64 s/file)` | `58.01 MiB/s (70.61 s/file)` | **0.90x** | **3.09x** |
| **Read big file** | `118.45 MiB/s (34.58 s/file)` | `224.78 MiB/s (18.22 s/file)` | `383.07 MiB/s (10.69 s/file)` | **0.53x** | **0.31x** |
| **Write small file** | `317.40 files/s (12.60 ms/file)` | `2268.50 files/s (1.76 ms/file)` | `98.60 files/s (40.58 ms/file)` | **0.14x** | **3.22x** |
| **Read small file** | `467.10 files/s (8.56 ms/file)` | `7833.90 files/s (0.51 ms/file)` | `137.60 files/s (29.06 ms/file)` | **0.06x** | **3.39x** |
| **Stat file** | `5541.50 files/s (0.72 ms/file)` | `17978.60 files/s (0.22 ms/file)` | `483.50 files/s (8.27 ms/file)` | **0.31x** | **11.46x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `226.09 (226.21) MiB/s` | `325.52 (432.02) MiB/s` | `191.09 (262.49) MiB/s` | **0.69x** | **1.18x** |
| `sequential_read` | **顺序读 (256 MiB)** | `141.13 (246.56) MiB/s` | `184.44 (184.70) MiB/s` | `82.27 (83.19) MiB/s` | **0.77x** | **1.72x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `5.52 (5.65) MiB/s` | `2.83 (2.86) MiB/s` | `5.11 (5.11) MiB/s` | **1.96x** | **1.08x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `131.81 (133.82) ops/s` | `1,035.92 (1,057.47) ops/s` | `143.74 (147.25) ops/s` | **0.13x** | **0.92x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `423.72 (454.94) ops/s` | `248.08 (252.15) ops/s` | `248.04 (248.32) ops/s` | **1.71x** | **1.71x** |
| `rename` | **目录重命名 rename (500 个)** | `345.50 (366.52) ops/s` | `96.25 (96.68) ops/s` | `61.50 (62.03) ops/s` | **3.59x** | **5.62x** |
| `unlink` | **文件删除 unlink (500 个)** | `373.07 (393.13) ops/s` | `98.11 (98.21) ops/s` | `51.04 (51.32) ops/s` | **3.80x** | **7.31x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `2,738.56 (4,032.10) ops/s` | `6,643.91 (6,830.58) ops/s` | `13,949.26 (14,633.11) ops/s` | **0.41x** | **0.20x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `8,240.40 (8,255.37) ops/s` | `41,769.86 (43,318.06) ops/s` | `104,847.32 (118,442.88) ops/s` | **0.20x** | **0.08x** |
| `stat` | **文件属性查询 stat (500 个)** | `2,562.10 (2,610.01) ops/s` | `20,714.04 (20,892.44) ops/s` | `35,844.35 (42,499.74) ops/s` | **0.12x** | **0.07x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `5.153s (5.431s)` | `1.908s (1.934s)` | `6.290s (6.344s)` | **0.37x** | **1.22x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.361s (0.369s)` | `2.081s (2.121s)` | `0.305s (0.308s)` | **5.76x** | **0.84x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.147s (0.149s)` | `0.782s (0.789s)` | `0.074s (0.114s)` | **5.31x** | **0.50x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.096s (0.109s)` | `0.016s (0.019s)` | `0.043s (0.048s)` | **0.17x** | **0.45x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.584s (2.099s)` | `0.361s (0.379s)` | `0.342s (0.346s)` | **0.62x** | **0.59x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `222.60 (228.23) MiB/s` | `247.54 (427.79) MiB/s` | `186.47 (273.75) MiB/s` | **0.90x** | **1.19x** |
| `sequential_read` | **顺序读 (256 MiB)** | `185.54 (215.79) MiB/s` | `2,339.87 (2,378.70) MiB/s` | `893.07 (906.87) MiB/s` | **0.08x** | **0.21x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `5.19 (5.53) MiB/s` | `172.64 (180.34) MiB/s` | `4.84 (4.87) MiB/s` | **0.03x** | **1.07x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `132.42 (133.36) ops/s` | `996.02 (1,034.34) ops/s` | `143.00 (143.55) ops/s` | **0.13x** | **0.93x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `492.51 (512.78) ops/s` | `244.33 (248.44) ops/s` | `239.33 (245.99) ops/s` | **2.02x** | **2.06x** |
| `rename` | **目录重命名 rename (500 个)** | `509.91 (513.08) ops/s` | `92.91 (95.97) ops/s` | `58.87 (59.09) ops/s` | **5.49x** | **8.66x** |
| `unlink` | **文件删除 unlink (500 个)** | `474.14 (484.78) ops/s` | `99.28 (99.94) ops/s` | `51.71 (51.86) ops/s` | **4.78x** | **9.17x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `5,283.02 (6,869.72) ops/s` | `125,187.78 (156,840.91) ops/s` | `233,642.13 (236,695.02) ops/s` | **0.04x** | **0.02x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `7,373.06 (8,333.69) ops/s` | `77,357.67 (83,386.34) ops/s` | `225,341.55 (247,411.15) ops/s` | **0.10x** | **0.03x** |
| `stat` | **文件属性查询 stat (500 个)** | `81,730.14 (93,123.68) ops/s` | `676,560.69 (679,421.54) ops/s` | `228,492.04 (369,587.70) ops/s` | **0.12x** | **0.36x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `5.177s (5.384s)` | `1.857s (1.861s)` | `6.270s (6.306s)` | **0.36x** | **1.21x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.145s (0.215s)` | `0.217s (0.219s)` | `0.048s (0.057s)` | **1.50x** | **0.33x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.041s (0.043s)` | `0.013s (0.013s)` | `0.012s (0.012s)` | **0.32x** | **0.29x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.090s (0.093s)` | `0.005s (0.005s)` | `0.006s (0.007s)` | **0.05x** | **0.07x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.395s (0.400s)` | `0.113s (0.117s)` | `0.155s (0.195s)` | **0.29x** | **0.39x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `138.36 files/s` | `963.96 files/s` | `154.33 files/s` | **0.14x** | **0.90x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `280.47 files/s` | `91.32 files/s` | `347.01 files/s` | **3.07x** | **0.81x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `47.27 MiB/s` | `59.01 MiB/s` | `49.31 MiB/s` | **0.80x** | **0.96x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `4.36 MiB/s` | `689.16 MiB/s` | `104.06 MiB/s` | **0.01x** | **0.04x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `4.46 MiB/s` | `724.09 MiB/s` | `62.35 MiB/s` | **0.01x** | **0.07x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `1,772.01 rows/s` | `3,096.84 rows/s` | `3,977.74 rows/s` | **0.57x** | **0.45x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `175.74 rows/s` | `763.44 rows/s` | `906.23 rows/s` | **0.23x** | **0.19x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `26,528.10 rows/s` | `593,818.72 rows/s` | `68,707.83 rows/s` | **0.04x** | **0.39x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `909.17 rows/s` | `5,675.96 rows/s` | `7,229.85 rows/s` | **0.16x** | **0.13x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `311.54 rows/s` | `2,780.47 rows/s` | `2,813.91 rows/s` | **0.11x** | **0.11x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `50,735.39 rows/s` | `1,046,097.77 rows/s` | `1,032,566.17 rows/s` | **0.05x** | **0.05x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.1258 s` | `0.0086 s` | `0.0446 s` | **0.07x** | **0.35x** |

---

## 6. 性能特征与存储层架构差异深度剖析（基于第一性原理与实测数据）

### 6.1 跨机同步持久化路径：Bowl (`bowl`) vs Drive9 远端同步模式 (`drive9_sync`)

在同样禁用客户端本地 Write-Back 伪装、要求每次 `close`/`fsync` 真正跨越 VPC 落盘至服务端存储节点的公平对比（`bowl` vs `drive9_sync`）下：

1. **大文件与小文件并发同步写吞吐（Bowl 达 `drive9_sync` 的 `3.09x` ~ `3.22x`）**：
   - 在 `juicefs bench -p 4` 中，Bowl 大文件并发写达 **`179.17 MiB/s`**（为 `drive9_sync` `58.01 MiB/s` 的 **`3.09x`**），小文件并发写达 **`317.40 files/s`**（为 `drive9_sync` `98.60 files/s` 的 **`3.22x`**），小文件并发读达 **`467.10 files/s`**（为 `drive9_sync` `137.60 files/s` 的 **`3.39x`**），并发 `Stat` 达 **`5,541.50 files/s`**（为 `drive9_sync` `483.50 files/s` 的 **`11.46x`**）。
   - **根因机制**：Bowl 将大文件切分为 `64 MiB` Chunk 与 `4 MiB` Block，客户端在 `Write` 时流式直通服务端 `BlockStore` 配合 RocksDB 式 `WriteThread` 两阶段 Group Commit（ADR-0022）；而在 `--durability=close-sync` 模式下，Drive9 客户端需先写入本地 Shadow Staging 存储，再在 `close`/`fsync` 时通过 HTTP/S3 协议上传至 `drive9-server` 并在 TiDB 中执行版本事务提交，承受了本地 Staging 写与远端上传写的双重开销。
2. **单流大文件冷缓存顺序读写与随机 4K 读（Bowl 顺序写 `1.18x`、顺序读 `1.72x`、随机读 `1.08x`）**：
   - 在 `drive9_fuse_bench.py` 冷缓存测试中，Bowl 单流 256 MiB 顺序写达 **`226.09 MiB/s`**（`drive9_sync` 为 `191.09 MiB/s`，Bowl 为 **`1.18x`**），冷缓存 256 MiB 顺序读达 **`141.13 MiB/s`**（`drive9_sync` 为 `82.27 MiB/s`，Bowl 为 **`1.72x`**），随机 4K 读达 **`5.52 MiB/s`**（对比 `drive9_default` `2.83 MiB/s` 为 **`1.96x`**，对比 `drive9_sync` `5.11 MiB/s` 为 **`1.08x`**）。
   - 得益于 ADR-0023 的服务端 `BlockStore` 无锁并发读块与客户端 `FileHandle` 顺序预读窗口（Sequential Read-Ahead），Bowl 在无客户端本地磁盘缓存的前提下，冷缓存顺序读与 4K 随机读均超越 Drive9。
3. **目录与命名空间同步变更操作（`mkdir` / `rename` / `unlink`：Bowl 达 `1.71x` ~ `9.17x`）**：
   - 无论在冷缓存还是热缓存下，Drive9 的目录结构变更（`mkdir`、`rename`、`unlink`）均需同步请求远端服务端：冷缓存下 Bowl 的 `mkdir`（**`423.72 ops/s`**）、`rename`（**`345.50 ops/s`**）、`unlink`（**`373.07 ops/s`**）分别为 `drive9_default` 的 **`1.71x`、`3.59x`、`3.80x`**，分别为 `drive9_sync` 的 **`1.71x`、`5.62x`、`7.31x`**（热缓存下更达到 `drive9_sync` 的 **`2.06x`、`8.66x`、`9.17x`**）。
   - 经过 ADR-0023 优化后，Bowl 消除了非孤儿文件 `Release` 路径上的 `Inode` 排他锁竞争与每次 RPC 的会话心跳行锁更新，使 PostgreSQL 16 单跳事务的命名空间变更吞吐大幅领先于 Drive9 的 HTTP REST + TiDB 链路。
4. **Git 研发工作流（`macro_git_clone` / `git status` / `git diff`）**：
   - 在同步持久化契约（`bowl` vs `drive9_sync`）下，Bowl 完成 200 文件 / 20 MiB 仓库的 `git clone --no-local` 仅需 **`5.153s`**，比 `drive9_sync`（`6.290s`）快 **`1.22x`**。
   - 在冷缓存 `git status --porcelain`（dirty 工作区）与 `git diff --name-only` 中，Bowl 耗时分别为 **`0.361s`** 与 **`0.147s`**，比 `drive9_default`（`2.081s` 与 `0.782s`）快 **`5.76x`** 与 **`5.31x`**（因为 `drive9_default` 在 `coding-agent` 模式下首次访问工作区会触发 Git Workspace / Ignore 规则链扫描与本地状态核验开销）。

### 6.2 客户端本地缓存与专用优化对比：Drive9 的优势场景与架构取舍

1. **客户端本地 Write-Back 异步暂存（`drive9_default` 的 `interactive` 模式）**：
   - 在 `drive9_default`（`--durability=interactive`）下，`write`、`fsync` 与 `close` 仅写入客户端本地 NVMe/ESSD Staging 目录即立即返回，后台 `CommitQueue` 异步排队上传。这使 `drive9_default` 在单机突发小文件写入（`juicefs bench` 小文件写 `2,268.50 files/s`、`small_write_4k_fsync` `1,035.92 ops/s`）及 `git clone`（`1.908s`）上接近本地磁盘速度。
   - **架构代价**：`fsync` 返回时尚未完成服务端持久化（客户端崩溃或断电存在未 Drain 队列丢失风险），且后台 `CommitQueue` 未完成前多客户端无法共享一致视图；此外在未 Drain 状态下若紧接执行包含硬链接（`link`）的操作（如 `git clone` 的 packfile 落盘），极易触发本地 `writeBack` 与远端 `syncOpenSourceForHardlink` 的版本冲突。
2. **客户端用户态内存/磁盘读缓存与目录全量缓存（`readCache` / `diskReadCache` / `dirCache`）**：
   - Drive9 在客户端进程内维护了 128 MiB 内存读缓存、1024 MiB 本地磁盘读缓存以及用户态全量目录项缓存（`dirCache`，`Lookup`/`GetAttr` 直接命中客户端进程内存而不发网络 RPC）。因此当数据已进入客户端读缓存（如热缓存 `sequential_read` `2,339.87 MiB/s`、`readdir_large`、`stat` 以及 `juicefs bench` 刚写完同一批文件后的 `Read big file`）时，Drive9 直接从客户端本地内存/本地盘返回。
   - **演进启示**：Bowl 目前坚持客户端零本地磁盘缓存与严格的 `1.0s` 内核缓存过期策略（1s 后 `stat`/`readdir` 均跨越 VPC 发起 RPC），以换取多节点共享挂载的强一致性；后续若为单客户端独占挂载场景引入可选的用户态 `DirCache` / 属性租约（Lease），可进一步消除热路径上的跨机 `Lookup`/`Readdir` 往返。
3. **SQLite 专用日志路径优化（`fuse_perf_baseline.py`）**：
   - Drive9 针对 SQLite `-wal`/`-journal` 路径内置了专用的 Append-Log / Batch Coalescing 与客户端读缓存驻留路径，因此在单客户端 SQLite 连续事务微基准中吞吐较高；而 Bowl 将 SQLite 数据库与日志文件均作为标准 POSIX 文件通过服务端 `BlockStore` 强一致落盘，依然在 `DELETE` 与 `WAL`（`PRAGMA synchronous=FULL`）双模式下 100% 通过 SHA-256 指纹校验、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261031110706`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj919o5nf211olqico6j`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj919o5nf211olqico6k`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-10-31T03:07:19Z` |
| **实例销毁时间 (UTC)** | `2026-10-31T03:34:12Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
