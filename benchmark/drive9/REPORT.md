# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

由于 Drive9 默认采用客户端本地 Write-Back 缓存 + 本地 `.git`/构建目录 Overlay（`--profile=coding-agent --durability=interactive`），而 Bowl 采用无客户端本地盘缓存的跨机写入服务端页缓存、显式 Fsync 仅同步数据库 WAL 的方式，为兼顾「默认开箱体验对比」与「同步提交模式对比」，本次评测在同台机器上依次测试了三组目标：

1. **Bowl (`bowl`)**：PostgreSQL 16 + Server BlockStore，采用生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，客户端零本地盘缓存，普通 Write 写入服务端页缓存，显式 Fsync 仅同步数据库 WAL，块数据由内核后台回写）。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `287.74 MiB/s` | `199.69 MiB/s` | `63.98 MiB/s` | **1.44x** | **4.50x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `914.55 MiB/s` | `187.77 MiB/s` | `346.32 MiB/s` | **4.87x** | **2.64x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `74.20 files/s` | `913.40 files/s` | `102.00 files/s` | **0.08x** | **0.73x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,307.00 files/s` | `7,300.30 files/s` | `304.60 files/s` | **0.18x** | **4.29x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `5,978.60 files/s` | `26,021.60 files/s` | `519.50 files/s` | **0.23x** | **11.51x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `354.56 MiB/s` | `288.65 MiB/s` | `191.38 MiB/s` | **1.23x** | **1.85x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `459.66 MiB/s` | `183.04 MiB/s` | `56.76 MiB/s` | **2.51x** | **8.10x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `198.64 ops/s` | `1,042.41 ops/s` | `149.55 ops/s` | **0.19x** | **1.33x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `477.34 ops/s` | `257.44 ops/s` | `253.70 ops/s` | **1.85x** | **1.88x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `356.40 ops/s` | `99.15 ops/s` | `62.65 ops/s` | **3.59x** | **5.69x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `429.45 ops/s` | `100.38 ops/s` | `56.21 ops/s` | **4.28x** | **7.64x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `3.32 s` | `1.83 s` | `5.94 s` | **0.55x** | **1.79x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `1,629.03 rows/s` | `7,062.84 rows/s` | `8,825.97 rows/s` | **0.23x** | **0.18x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj9f3lh7wxbe9ef3t03g` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.204` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj9f3lh7wxbe9ef3t03h` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.205` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.067/0.094/0.215/0.036 ms` | `ping -c 20 -i 0.05 172.16.1.204` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.204 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `187.12` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `178.93` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `df5cb52` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **服务端元数据引擎** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`127.0.0.1:5432`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化契约** | `Write` 写入 Server 页缓存；显式 `Fsync` 仅同步 PostgreSQL WAL，块数据由内核后台回写（ADR-0026） | 客户端本地 SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `287.74 MiB/s (14.24 s/file)` | `199.69 MiB/s (20.51 s/file)` | `63.98 MiB/s (64.02 s/file)` | **1.44x** | **4.50x** |
| **Read big file** | `914.55 MiB/s (4.48 s/file)` | `187.77 MiB/s (21.81 s/file)` | `346.32 MiB/s (11.83 s/file)` | **4.87x** | **2.64x** |
| **Write small file** | `74.20 files/s (53.89 ms/file)` | `913.40 files/s (4.38 ms/file)` | `102.00 files/s (39.22 ms/file)` | **0.08x** | **0.73x** |
| **Read small file** | `1307.00 files/s (3.06 ms/file)` | `7300.30 files/s (0.55 ms/file)` | `304.60 files/s (13.13 ms/file)` | **0.18x** | **4.29x** |
| **Stat file** | `5978.60 files/s (0.67 ms/file)` | `26021.60 files/s (0.15 ms/file)` | `519.50 files/s (7.70 ms/file)` | **0.23x** | **11.51x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `354.56 (370.21) MiB/s` | `288.65 (433.19) MiB/s` | `191.38 (262.67) MiB/s` | **1.23x** | **1.85x** |
| `sequential_read` | **顺序读 (256 MiB)** | `459.66 (637.17) MiB/s` | `183.04 (183.70) MiB/s` | `56.76 (56.97) MiB/s` | **2.51x** | **8.10x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.69 (6.79) MiB/s` | `2.77 (2.78) MiB/s` | `5.07 (5.09) MiB/s` | **2.41x** | **1.32x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `198.64 (205.28) ops/s` | `1,042.41 (1,042.54) ops/s` | `149.55 (150.52) ops/s` | **0.19x** | **1.33x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `477.34 (485.00) ops/s` | `257.44 (257.55) ops/s` | `253.70 (256.40) ops/s` | **1.85x** | **1.88x** |
| `rename` | **目录重命名 rename (500 个)** | `356.40 (358.33) ops/s` | `99.15 (100.16) ops/s` | `62.65 (63.07) ops/s` | **3.59x** | **5.69x** |
| `unlink` | **文件删除 unlink (500 个)** | `429.45 (435.07) ops/s` | `100.38 (102.41) ops/s` | `56.21 (56.25) ops/s` | **4.28x** | **7.64x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `4,784.30 (4,816.66) ops/s` | `7,533.09 (7,824.78) ops/s` | `17,121.69 (17,335.77) ops/s` | **0.64x** | **0.28x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `3,037.98 (11,558.61) ops/s` | `49,945.07 (53,322.35) ops/s` | `115,467.17 (122,151.02) ops/s` | **0.06x** | **0.03x** |
| `stat` | **文件属性查询 stat (500 个)** | `2,653.60 (2,770.49) ops/s` | `20,875.07 (21,078.40) ops/s` | `39,476.08 (40,946.86) ops/s` | **0.13x** | **0.07x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.321s (3.337s)` | `1.829s (1.866s)` | `5.944s (5.972s)` | **0.55x** | **1.79x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.288s (0.291s)` | `2.027s (2.062s)` | `0.267s (0.303s)` | **7.03x** | **0.92x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.188s (0.216s)` | `0.755s (0.755s)` | `0.029s (0.034s)` | **4.02x** | **0.16x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.076s (0.083s)` | `0.014s (0.027s)` | `0.039s (0.044s)` | **0.18x** | **0.52x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.561s (2.113s)` | `0.355s (0.362s)` | `0.315s (0.318s)` | **0.63x** | **0.56x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `352.12 (363.79) MiB/s` | `288.78 (415.09) MiB/s` | `190.27 (261.34) MiB/s` | **1.22x** | **1.85x** |
| `sequential_read` | **顺序读 (256 MiB)** | `567.30 (706.03) MiB/s` | `2,050.40 (2,096.59) MiB/s` | `650.17 (705.25) MiB/s` | **0.28x** | **0.87x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.83 (6.98) MiB/s` | `167.99 (168.23) MiB/s` | `4.54 (4.61) MiB/s` | **0.04x** | **1.50x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `195.95 (206.07) ops/s` | `1,046.53 (1,055.03) ops/s` | `147.45 (149.14) ops/s` | **0.19x** | **1.33x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `525.37 (528.07) ops/s` | `254.69 (254.75) ops/s` | `247.88 (251.04) ops/s` | **2.06x** | **2.12x** |
| `rename` | **目录重命名 rename (500 个)** | `393.68 (402.97) ops/s` | `96.04 (97.43) ops/s` | `55.77 (56.82) ops/s` | **4.10x** | **7.06x** |
| `unlink` | **文件删除 unlink (500 个)** | `539.03 (553.15) ops/s` | `106.64 (106.73) ops/s` | `56.46 (56.47) ops/s` | **5.05x** | **9.55x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `7,118.40 (7,727.69) ops/s` | `147,733.40 (159,168.40) ops/s` | `222,375.41 (226,134.23) ops/s` | **0.05x** | **0.03x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `13,269.62 (14,096.33) ops/s` | `87,414.80 (89,747.77) ops/s` | `289,600.39 (291,428.00) ops/s` | **0.15x** | **0.05x** |
| `stat` | **文件属性查询 stat (500 个)** | `39,664.72 (42,354.42) ops/s` | `649,911.09 (685,993.41) ops/s` | `475,846.51 (483,327.16) ops/s` | **0.06x** | **0.08x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.470s (3.661s)` | `1.821s (1.852s)` | `6.072s (6.087s)` | **0.52x** | **1.75x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.134s (0.209s)` | `0.210s (0.216s)` | `0.051s (0.054s)` | **1.57x** | **0.38x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.065s (0.117s)` | `0.012s (0.015s)` | `0.012s (0.018s)` | **0.19x** | **0.18x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.083s (0.086s)` | `0.004s (0.004s)` | `0.003s (0.004s)` | **0.05x** | **0.04x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.285s (0.320s)` | `0.107s (0.179s)` | `0.172s (0.187s)` | **0.38x** | **0.60x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `193.09 files/s` | `932.58 files/s` | `160.63 files/s` | **0.21x** | **1.20x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `312.03 files/s` | `98.51 files/s` | `351.66 files/s` | **3.17x** | **0.89x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `52.37 MiB/s` | `58.93 MiB/s` | `49.23 MiB/s` | **0.89x** | **1.06x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `184.44 MiB/s` | `532.10 MiB/s` | `81.66 MiB/s` | **0.35x** | **2.26x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `128.48 MiB/s` | `957.43 MiB/s` | `49.97 MiB/s` | **0.13x** | **2.57x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `2,877.99 rows/s` | `6,425.14 rows/s` | `4,079.80 rows/s` | **0.45x** | **0.71x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `310.35 rows/s` | `1,150.63 rows/s` | `934.82 rows/s` | **0.27x** | **0.33x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `43,957.98 rows/s` | `658,299.07 rows/s` | `78,308.22 rows/s` | **0.07x** | **0.56x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `1,629.03 rows/s` | `7,062.84 rows/s` | `8,825.97 rows/s` | **0.23x** | **0.18x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `585.72 rows/s` | `2,712.49 rows/s` | `2,873.50 rows/s` | **0.22x** | **0.20x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `87,228.01 rows/s` | `1,097,610.13 rows/s` | `834,822.53 rows/s` | **0.08x** | **0.10x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0692 s` | `0.0083 s` | `0.0309 s` | **0.12x** | **0.45x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `287.74 MiB/s`，Drive9 Sync `63.98 MiB/s`，吞吐比 `4.50x`。
- `Read big file`：Bowl `914.55 MiB/s`，Drive9 Sync `346.32 MiB/s`，吞吐比 `2.64x`。
- `Write small file`：Bowl `74.20 files/s`，Drive9 Sync `102.00 files/s`，吞吐比 `0.73x`。
- `Read small file`：Bowl `1307.00 files/s`，Drive9 Sync `304.60 files/s`，吞吐比 `4.29x`。
- `Stat file`：Bowl `5978.60 files/s`，Drive9 Sync `519.50 files/s`，吞吐比 `11.51x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261102100621`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9f3lh7wxbe9ef3t03g`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9f3lh7wxbe9ef3t03h`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T02:06:30Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T02:31:46Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
