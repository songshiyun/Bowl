# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 16 仅保存 `disks`、`commits`、`client_sessions` 控制面表；每个分支通过常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 和 1 MiB 数据块，以 Set 和 ValueMerger 处理整块与局部写入并按区间解码，以硬链接 Checkpoint 支持快照和 Fork；常规写批次以 `pebble.NoSync` 提交异步 WAL，并由后台 1 秒刷盘兜底。FileHandle、FileLock、O_TMPFILE 与 O(1) 索引的 NamespaceCache 由客户端维护。客户端默认开启内核 `CAP_WRITEBACK_CACHE` 与 `FOPEN_KEEP_CACHE`，常规写由内核页缓存聚合回写；Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `11.63 MiB/s` | `199.17 MiB/s` | `64.05 MiB/s` | **0.06x** | **0.18x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `982.76 MiB/s` | `240.35 MiB/s` | `344.80 MiB/s` | **4.09x** | **2.85x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `690.70 files/s` | `2,155.00 files/s` | `98.10 files/s` | **0.32x** | **7.04x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `2,751.10 files/s` | `7,082.60 files/s` | `12.80 files/s` | **0.39x** | **214.93x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `126,123.00 files/s` | `59,872.10 files/s` | `21.00 files/s` | **2.11x** | **6005.86x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `162.99 MiB/s` | `270.93 MiB/s` | `185.94 MiB/s` | **0.60x** | **0.88x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `489.79 MiB/s` | `183.64 MiB/s` | `58.16 MiB/s` | **2.67x** | **8.42x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `380.22 ops/s` | `981.92 ops/s` | `145.16 ops/s` | **0.39x** | **2.62x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `910.18 ops/s` | `251.58 ops/s` | `247.36 ops/s` | **3.62x** | **3.68x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `856.95 ops/s` | `97.63 ops/s` | `60.10 ops/s` | **8.78x** | **14.26x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `987.97 ops/s` | `96.60 ops/s` | `51.65 ops/s` | **10.23x** | **19.13x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `1.57 s` | `1.87 s` | `6.09 s` | **1.19x** | **3.88x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `3,653.96 rows/s` | `7,896.22 rows/s` | `8,736.18 rows/s` | **0.46x** | **0.42x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj901hvcvkdw6txxqwdu` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.203` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj901hvcvkdw6txxqwdv` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.202` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.061/0.086/0.172/0.030 ms` | `ping -c 20 -i 0.05 172.16.1.203` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.203 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `192.75` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `177.33` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `626dd03` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | 每分支 Pebble (`/var/lib/bowl/data`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | 内核 `writeback_cache` 聚合回写 + `pebble.NoSync` 异步 WAL（后台 1 秒刷盘）；仅 Fsync/Sync 写入持久化屏障 | 客户端本地 SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `11.63 MiB/s (352.06 s/file)` | `199.17 MiB/s (20.57 s/file)` | `64.05 MiB/s (63.95 s/file)` | **0.06x** | **0.18x** |
| **Read big file** | `982.76 MiB/s (4.17 s/file)` | `240.35 MiB/s (17.04 s/file)` | `344.80 MiB/s (11.88 s/file)` | **4.09x** | **2.85x** |
| **Write small file** | `690.70 files/s (5.79 ms/file)` | `2155.00 files/s (1.86 ms/file)` | `98.10 files/s (40.77 ms/file)` | **0.32x** | **7.04x** |
| **Read small file** | `2751.10 files/s (1.45 ms/file)` | `7082.60 files/s (0.56 ms/file)` | `12.80 files/s (311.46 ms/file)` | **0.39x** | **214.93x** |
| **Stat file** | `126123.00 files/s (0.03 ms/file)` | `59872.10 files/s (0.07 ms/file)` | `21.00 files/s (190.33 ms/file)` | **2.11x** | **6005.86x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `162.99 (234.01) MiB/s` | `270.93 (417.47) MiB/s` | `185.94 (265.07) MiB/s` | **0.60x** | **0.88x** |
| `sequential_read` | **顺序读 (256 MiB)** | `489.79 (510.05) MiB/s` | `183.64 (183.70) MiB/s` | `58.16 (59.24) MiB/s` | **2.67x** | **8.42x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `3.83 (3.86) MiB/s` | `2.77 (2.77) MiB/s` | `5.11 (5.12) MiB/s` | **1.38x** | **0.75x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `380.22 (393.41) ops/s` | `981.92 (989.00) ops/s` | `145.16 (145.31) ops/s` | **0.39x** | **2.62x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `910.18 (912.89) ops/s` | `251.58 (252.03) ops/s` | `247.36 (249.84) ops/s` | **3.62x** | **3.68x** |
| `rename` | **目录重命名 rename (500 个)** | `856.95 (866.11) ops/s` | `97.63 (98.07) ops/s` | `60.10 (61.98) ops/s` | **8.78x** | **14.26x** |
| `unlink` | **文件删除 unlink (500 个)** | `987.97 (1,005.02) ops/s` | `96.60 (98.64) ops/s` | `51.65 (51.90) ops/s` | **10.23x** | **19.13x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `19,430.81 (25,354.55) ops/s` | `7,676.66 (8,145.06) ops/s` | `12,448.42 (14,740.37) ops/s` | **2.53x** | **1.56x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `92,782.71 (97,165.39) ops/s` | `45,001.47 (52,402.88) ops/s` | `136,448.10 (139,410.90) ops/s` | **2.06x** | **0.68x** |
| `stat` | **文件属性查询 stat (500 个)** | `3,322.58 (3,808.03) ops/s` | `22,138.76 (25,178.57) ops/s` | `42,705.47 (46,302.29) ops/s` | **0.15x** | **0.08x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `1.571s (1.614s)` | `1.871s (1.959s)` | `6.087s (6.185s)` | **1.19x** | **3.88x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.138s (0.181s)` | `2.194s (2.208s)` | `0.227s (0.268s)` | **15.86x** | **1.64x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.079s (0.103s)` | `0.774s (0.792s)` | `0.035s (0.039s)` | **9.80x** | **0.45x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.018s (0.018s)` | `0.016s (0.018s)` | `0.044s (0.051s)` | **0.91x** | **2.47x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.316s (1.837s)` | `0.338s (0.355s)` | `0.354s (0.408s)` | **1.07x** | **1.12x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `209.42 (215.08) MiB/s` | `272.60 (432.06) MiB/s` | `187.92 (260.48) MiB/s` | **0.77x** | **1.11x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,077.75 (7,120.91) MiB/s` | `2,306.68 (2,307.76) MiB/s` | `648.77 (663.16) MiB/s` | **3.07x** | **10.91x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `249.38 (258.89) MiB/s` | `178.34 (179.16) MiB/s` | `4.37 (4.41) MiB/s` | **1.40x** | **57.10x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `485.07 (519.09) ops/s` | `1,010.80 (1,012.16) ops/s` | `142.89 (142.97) ops/s` | **0.48x** | **3.39x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `1,065.05 (1,074.34) ops/s` | `244.84 (247.29) ops/s` | `245.85 (246.38) ops/s` | **4.35x** | **4.33x** |
| `rename` | **目录重命名 rename (500 个)** | `1,110.36 (1,127.49) ops/s` | `91.73 (93.75) ops/s` | `58.42 (59.79) ops/s` | **12.10x** | **19.01x** |
| `unlink` | **文件删除 unlink (500 个)** | `1,378.99 (1,701.26) ops/s` | `99.19 (99.59) ops/s` | `51.82 (52.11) ops/s` | **13.90x** | **26.61x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `349,509.81 (354,303.76) ops/s` | `130,995.50 (165,645.80) ops/s` | `208,591.90 (220,104.27) ops/s` | **2.67x** | **1.68x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `224,005.51 (237,249.17) ops/s` | `59,119.10 (60,894.22) ops/s` | `267,390.69 (271,002.41) ops/s` | **3.79x** | **0.84x** |
| `stat` | **文件属性查询 stat (500 个)** | `117,494.62 (118,694.52) ops/s` | `627,054.39 (635,111.64) ops/s` | `51,605.40 (69,853.61) ops/s` | **0.19x** | **2.28x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `1.497s (1.528s)` | `1.879s (1.903s)` | `6.233s (6.248s)` | **1.26x** | **4.16x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.029s (0.034s)` | `0.216s (0.217s)` | `0.060s (0.065s)` | **7.56x** | **2.08x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.014s (0.023s)` | `0.012s (0.018s)` | `0.014s (0.020s)` | **0.86x** | **1.05x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.003s (0.004s)` | `0.004s (0.004s)` | `0.004s (0.004s)` | **1.29x** | **1.12x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.079s (0.088s)` | `0.105s (0.112s)` | `0.190s (0.195s)` | **1.33x** | **2.41x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `465.66 files/s` | `836.86 files/s` | `154.67 files/s` | **0.56x** | **3.01x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `2,488.93 files/s` | `96.03 files/s` | `318.97 files/s` | **25.92x** | **7.80x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `48.81 MiB/s` | `59.35 MiB/s` | `48.94 MiB/s` | **0.82x** | **1.00x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `140.13 MiB/s` | `661.21 MiB/s` | `75.44 MiB/s` | **0.21x** | **1.86x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,272.62 MiB/s` | `895.95 MiB/s` | `50.29 MiB/s` | **1.42x** | **25.31x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `5,941.23 rows/s` | `6,365.15 rows/s` | `4,234.31 rows/s` | **0.93x** | **1.40x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `630.68 rows/s` | `1,074.27 rows/s` | `903.09 rows/s` | **0.59x** | **0.70x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `67,245.93 rows/s` | `491,424.07 rows/s` | `67,605.31 rows/s` | **0.14x** | **0.99x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `3,653.96 rows/s` | `7,896.22 rows/s` | `8,736.18 rows/s` | **0.46x** | **0.42x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `1,292.73 rows/s` | `2,837.35 rows/s` | `2,797.40 rows/s` | **0.46x** | **0.46x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `1,270,093.27 rows/s` | `797,127.85 rows/s` | `1,043,900.93 rows/s` | **1.59x** | **1.22x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0500 s` | `0.0084 s` | `0.0354 s` | **0.17x** | **0.71x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `11.63 MiB/s`，Drive9 Sync `64.05 MiB/s`，吞吐比 `0.18x`。
- `Read big file`：Bowl `982.76 MiB/s`，Drive9 Sync `344.80 MiB/s`，吞吐比 `2.85x`。
- `Write small file`：Bowl `690.70 files/s`，Drive9 Sync `98.10 files/s`，吞吐比 `7.04x`。
- `Read small file`：Bowl `2751.10 files/s`，Drive9 Sync `12.80 files/s`，吞吐比 `214.93x`。
- `Stat file`：Bowl `126123.00 files/s`，Drive9 Sync `21.00 files/s`，吞吐比 `6005.86x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `11.63 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `162.99 MiB/s`。4 并发聚合吞吐低于单流，说明并发写路径存在额外开销，需单独剖析；不能把 `juicefs bench` 数值当作 Bowl 的单流写上限。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。本套件未测试整机断电恢复。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261104171737`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj901hvcvkdw6txxqwdu`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj901hvcvkdw6txxqwdv`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-04T09:17:55Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T09:47:24Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
