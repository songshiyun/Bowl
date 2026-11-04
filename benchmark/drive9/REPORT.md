# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 16 仅保存 `disks`、`commits`、`client_sessions` 控制面表；每个分支独立的 Pebble 保存 Inode、Entry、Xattr、Orphan 和 4 MiB 数据块，以 Set 和 ValueMerger 处理整块与局部写入，并以硬链接 Checkpoint 支持快照和 Fork。FileHandle、FileLock、O_TMPFILE 与按需加载的 NamespaceCache 由客户端维护。当前提交禁用内核 `writeback_cache`；Flush 和 Fsync 请求服务端同步，Release 负责释放本地句柄。该配置与 ADR-0029 的 writeback_cache 设计不同。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `26.51 MiB/s` | `199.70 MiB/s` | `60.15 MiB/s` | **0.13x** | **0.44x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `7.69 MiB/s` | `231.08 MiB/s` | `391.81 MiB/s` | **0.03x** | **0.02x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `12.90 files/s` | `2,212.20 files/s` | `101.60 files/s` | **0.01x** | **0.13x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `43.50 files/s` | `6,752.70 files/s` | `301.10 files/s` | **0.01x** | **0.14x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `110,803.90 files/s` | `14,078.90 files/s` | `530.70 files/s` | **7.87x** | **208.79x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `40.85 MiB/s` | `258.04 MiB/s` | `189.39 MiB/s` | **0.16x** | **0.22x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `16.94 MiB/s` | `184.15 MiB/s` | `58.08 MiB/s` | **0.09x** | **0.29x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `38.36 ops/s` | `950.08 ops/s` | `149.06 ops/s` | **0.04x** | **0.26x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `78.95 ops/s` | `257.63 ops/s` | `255.86 ops/s` | **0.31x** | **0.31x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `61.99 ops/s` | `99.49 ops/s` | `63.17 ops/s` | **0.62x** | **0.98x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `78.45 ops/s` | `100.54 ops/s` | `56.15 ops/s` | **0.78x** | **1.40x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `26.26 s` | `1.91 s` | `5.96 s` | **0.07x** | **0.23x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `191.64 rows/s` | `7,729.27 rows/s` | `8,273.81 rows/s` | **0.02x** | **0.02x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj94b8ccceaoxjysslfx` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.194` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj94b8ccceaoxjysslfy` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.195` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.061/0.083/0.174/0.030 ms` | `ping -c 20 -i 0.05 172.16.1.194` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.194 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `198.16` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.21` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `0ab26a7` | Drive9 `b529a36` | Drive9 `b529a36` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | 每分支 Pebble (`/var/lib/bowl/data`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | 禁用内核 `writeback_cache`；Flush/Fsync 请求服务端同步 Pebble | 客户端本地 SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `26.51 MiB/s (154.50 s/file)` | `199.70 MiB/s (20.51 s/file)` | `60.15 MiB/s (68.10 s/file)` | **0.13x** | **0.44x** |
| **Read big file** | `7.69 MiB/s (532.73 s/file)` | `231.08 MiB/s (17.73 s/file)` | `391.81 MiB/s (10.45 s/file)` | **0.03x** | **0.02x** |
| **Write small file** | `12.90 files/s (309.22 ms/file)` | `2212.20 files/s (1.81 ms/file)` | `101.60 files/s (39.36 ms/file)` | **0.01x** | **0.13x** |
| **Read small file** | `43.50 files/s (92.01 ms/file)` | `6752.70 files/s (0.59 ms/file)` | `301.10 files/s (13.28 ms/file)` | **0.01x** | **0.14x** |
| **Stat file** | `110803.90 files/s (0.04 ms/file)` | `14078.90 files/s (0.28 ms/file)` | `530.70 files/s (7.54 ms/file)` | **7.87x** | **208.79x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `40.85 (40.93) MiB/s` | `258.04 (434.69) MiB/s` | `189.39 (258.43) MiB/s` | **0.16x** | **0.22x** |
| `sequential_read` | **顺序读 (256 MiB)** | `16.94 (17.04) MiB/s` | `184.15 (184.55) MiB/s` | `58.08 (58.21) MiB/s` | **0.09x** | **0.29x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `0.49 (0.50) MiB/s` | `2.77 (2.77) MiB/s` | `5.09 (5.10) MiB/s` | **0.18x** | **0.10x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `38.36 (38.69) ops/s` | `950.08 (1,000.40) ops/s` | `149.06 (150.47) ops/s` | **0.04x** | **0.26x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `78.95 (79.31) ops/s` | `257.63 (258.29) ops/s` | `255.86 (256.52) ops/s` | **0.31x** | **0.31x** |
| `rename` | **目录重命名 rename (500 个)** | `61.99 (62.21) ops/s` | `99.49 (100.96) ops/s` | `63.17 (63.20) ops/s` | **0.62x** | **0.98x** |
| `unlink` | **文件删除 unlink (500 个)** | `78.45 (79.14) ops/s` | `100.54 (101.02) ops/s` | `56.15 (56.89) ops/s` | **0.78x** | **1.40x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `2,901.05 (3,056.44) ops/s` | `7,093.96 (7,501.04) ops/s` | `18,718.77 (18,792.48) ops/s` | **0.41x** | **0.15x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `34,627.90 (35,295.11) ops/s` | `42,409.02 (49,342.35) ops/s` | `120,109.96 (137,359.57) ops/s` | **0.82x** | **0.29x** |
| `stat` | **文件属性查询 stat (500 个)** | `295.29 (295.75) ops/s` | `22,554.74 (24,154.71) ops/s` | `35,286.56 (37,483.08) ops/s` | **0.01x** | **0.01x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `26.255s (26.301s)` | `1.913s (1.924s)` | `5.960s (5.973s)` | **0.07x** | **0.23x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.973s (0.996s)` | `2.005s (2.041s)` | `0.232s (0.282s)` | **2.06x** | **0.24x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.089s (0.092s)` | `0.773s (0.774s)` | `0.034s (0.036s)` | **8.71x** | **0.39x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.070s (0.071s)` | `0.028s (0.030s)` | `0.039s (0.040s)` | **0.41x** | **0.57x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `1.366s (2.837s)` | `0.341s (0.364s)` | `0.315s (0.389s)` | **0.25x** | **0.23x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `39.94 (40.32) MiB/s` | `258.34 (431.57) MiB/s` | `190.13 (260.78) MiB/s` | **0.15x** | **0.21x** |
| `sequential_read` | **顺序读 (256 MiB)** | `17.52 (17.60) MiB/s` | `2,042.99 (2,095.97) MiB/s` | `714.56 (720.12) MiB/s` | **0.01x** | **0.02x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `0.51 (0.52) MiB/s` | `183.42 (185.98) MiB/s` | `4.52 (4.57) MiB/s` | **0.00x** | **0.11x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `35.43 (35.77) ops/s` | `1,012.45 (1,019.81) ops/s` | `145.32 (146.87) ops/s` | **0.03x** | **0.24x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `72.72 (73.96) ops/s` | `255.37 (256.68) ops/s` | `248.57 (249.98) ops/s` | **0.28x** | **0.29x** |
| `rename` | **目录重命名 rename (500 个)** | `57.86 (58.55) ops/s` | `96.20 (97.45) ops/s` | `55.14 (55.71) ops/s` | **0.60x** | **1.05x** |
| `unlink` | **文件删除 unlink (500 个)** | `73.69 (74.15) ops/s` | `101.00 (102.00) ops/s` | `56.21 (56.58) ops/s` | **0.73x** | **1.31x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `285,384.06 (309,533.75) ops/s` | `100,404.63 (116,036.29) ops/s` | `218,686.79 (219,875.41) ops/s` | **2.84x** | **1.30x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `81,171.06 (108,095.84) ops/s` | `85,457.81 (93,109.04) ops/s` | `222,124.29 (241,252.46) ops/s` | **0.95x** | **0.37x** |
| `stat` | **文件属性查询 stat (500 个)** | `35,819.42 (46,645.43) ops/s` | `628,696.74 (630,229.82) ops/s` | `411,207.21 (444,750.52) ops/s` | **0.06x** | **0.09x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `27.689s (27.804s)` | `1.827s (1.845s)` | `6.033s (6.057s)` | **0.07x** | **0.22x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.144s (0.166s)` | `0.141s (0.147s)` | `0.041s (0.056s)` | **0.98x** | **0.29x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.029s (0.029s)` | `0.012s (0.016s)` | `0.018s (0.019s)` | **0.41x** | **0.62x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.008s (0.008s)` | `0.004s (0.007s)` | `0.003s (0.008s)` | **0.58x** | **0.43x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.381s (0.405s)` | `0.106s (0.113s)` | `0.171s (0.179s)` | **0.28x** | **0.45x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `34.42 files/s` | `863.54 files/s` | `162.89 files/s` | **0.04x** | **0.21x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `181.71 files/s` | `93.70 files/s` | `326.65 files/s` | **1.94x** | **0.56x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `22.21 MiB/s` | `58.42 MiB/s` | `48.52 MiB/s` | **0.38x** | **0.46x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `23.19 MiB/s` | `597.23 MiB/s` | `76.06 MiB/s` | **0.04x** | **0.30x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `24.57 MiB/s` | `953.56 MiB/s` | `49.23 MiB/s` | **0.03x** | **0.50x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `388.10 rows/s` | `6,451.29 rows/s` | `4,068.33 rows/s` | **0.06x** | **0.10x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `36.15 rows/s` | `1,063.78 rows/s` | `870.35 rows/s` | **0.03x** | **0.04x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `11,776.51 rows/s` | `652,937.97 rows/s` | `73,570.55 rows/s` | **0.02x** | **0.16x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `191.64 rows/s` | `7,729.27 rows/s` | `8,273.81 rows/s` | **0.02x** | **0.02x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `64.15 rows/s` | `2,830.32 rows/s` | `2,952.63 rows/s` | **0.02x** | **0.02x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `840,501.67 rows/s` | `1,104,795.94 rows/s` | `1,047,566.05 rows/s` | **0.76x** | **0.80x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.5616 s` | `0.0082 s` | `0.0317 s` | **0.01x** | **0.06x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `26.51 MiB/s`，Drive9 Sync `60.15 MiB/s`，吞吐比 `0.44x`。
- `Read big file`：Bowl `7.69 MiB/s`，Drive9 Sync `391.81 MiB/s`，吞吐比 `0.02x`。
- `Write small file`：Bowl `12.90 files/s`，Drive9 Sync `101.60 files/s`，吞吐比 `0.13x`。
- `Read small file`：Bowl `43.50 files/s`，Drive9 Sync `301.10 files/s`，吞吐比 `0.14x`。
- `Stat file`：Bowl `110803.90 files/s`，Drive9 Sync `530.70 files/s`，吞吐比 `208.79x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。本套件未测试整机断电恢复。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261104065947`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj94b8ccceaoxjysslfx`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj94b8ccceaoxjysslfy`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-03T23:00:03Z` |
| **实例销毁时间 (UTC)** | `2026-11-03T23:53:40Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
