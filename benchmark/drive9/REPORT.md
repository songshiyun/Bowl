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
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `284.83 MiB/s` | `199.10 MiB/s` | `60.73 MiB/s` | **1.43x** | **4.69x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,202.87 MiB/s` | `182.03 MiB/s` | `262.09 MiB/s` | **6.61x** | **4.59x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `326.80 files/s` | `2,184.70 files/s` | `100.00 files/s` | **0.15x** | **3.27x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,722.50 files/s` | `6,506.20 files/s` | `189.70 files/s` | **0.26x** | **9.08x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,956.60 files/s` | `64,870.30 files/s` | `508.30 files/s` | **0.11x** | **13.69x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `359.91 MiB/s` | `307.24 MiB/s` | `188.21 MiB/s` | **1.17x** | **1.91x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `262.01 MiB/s` | `184.12 MiB/s` | `57.38 MiB/s` | **1.42x** | **4.57x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `245.70 ops/s` | `922.34 ops/s` | `145.04 ops/s` | **0.27x** | **1.69x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `521.31 ops/s` | `242.79 ops/s` | `243.53 ops/s` | **2.15x** | **2.14x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `441.11 ops/s` | `93.63 ops/s` | `60.50 ops/s` | **4.71x** | **7.29x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `543.02 ops/s` | `98.48 ops/s` | `52.67 ops/s` | **5.51x** | **10.31x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `3.79 s` | `1.90 s` | `6.17 s` | **0.50x** | **1.63x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `1,129.46 rows/s` | `7,798.82 rows/s` | `6,748.69 rows/s` | **0.14x** | **0.17x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj9ip6zekn1tbk2yerd7` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.225` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj9ip6zekn1tbk2yerd8` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.224` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.073/0.106/0.216/0.037 ms` | `ping -c 20 -i 0.05 172.16.1.225` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.225 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `198.64` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.11` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `0655a17` | Drive9 `b529a36f` | Drive9 `b529a36f` |
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
| **Write big file** | `284.83 MiB/s (14.38 s/file)` | `199.10 MiB/s (20.57 s/file)` | `60.73 MiB/s (67.45 s/file)` | **1.43x** | **4.69x** |
| **Read big file** | `1202.87 MiB/s (3.41 s/file)` | `182.03 MiB/s (22.50 s/file)` | `262.09 MiB/s (15.63 s/file)` | **6.61x** | **4.59x** |
| **Write small file** | `326.80 files/s (12.24 ms/file)` | `2184.70 files/s (1.83 ms/file)` | `100.00 files/s (40.01 ms/file)` | **0.15x** | **3.27x** |
| **Read small file** | `1722.50 files/s (2.32 ms/file)` | `6506.20 files/s (0.61 ms/file)` | `189.70 files/s (21.09 ms/file)` | **0.26x** | **9.08x** |
| **Stat file** | `6956.60 files/s (0.57 ms/file)` | `64870.30 files/s (0.06 ms/file)` | `508.30 files/s (7.87 ms/file)` | **0.11x** | **13.69x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `359.91 (360.38) MiB/s` | `307.24 (434.67) MiB/s` | `188.21 (258.62) MiB/s` | **1.17x** | **1.91x** |
| `sequential_read` | **顺序读 (256 MiB)** | `262.01 (618.41) MiB/s` | `184.12 (184.99) MiB/s` | `57.38 (57.86) MiB/s` | **1.42x** | **4.57x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.90 (7.05) MiB/s` | `2.77 (2.78) MiB/s` | `5.05 (5.07) MiB/s` | **2.49x** | **1.37x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `245.70 (249.04) ops/s` | `922.34 (951.10) ops/s` | `145.04 (145.62) ops/s` | **0.27x** | **1.69x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `521.31 (527.71) ops/s` | `242.79 (248.64) ops/s` | `243.53 (249.36) ops/s` | **2.15x** | **2.14x** |
| `rename` | **目录重命名 rename (500 个)** | `441.11 (441.27) ops/s` | `93.63 (94.94) ops/s` | `60.50 (60.95) ops/s` | **4.71x** | **7.29x** |
| `unlink` | **文件删除 unlink (500 个)** | `543.02 (548.04) ops/s` | `98.48 (99.65) ops/s` | `52.67 (52.86) ops/s` | **5.51x** | **10.31x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `6,056.11 (6,200.38) ops/s` | `7,085.37 (7,520.88) ops/s` | `12,542.95 (13,874.42) ops/s` | **0.85x** | **0.48x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `4,675.37 (4,764.09) ops/s` | `45,417.96 (49,459.80) ops/s` | `120,155.71 (121,173.30) ops/s` | **0.10x** | **0.04x** |
| `stat` | **文件属性查询 stat (500 个)** | `2,939.48 (3,020.18) ops/s` | `20,549.61 (21,037.18) ops/s` | `36,908.02 (42,493.16) ops/s` | **0.14x** | **0.08x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.787s (3.842s)` | `1.899s (1.912s)` | `6.172s (6.225s)` | **0.50x** | **1.63x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.256s (0.298s)` | `1.813s (1.886s)` | `0.250s (0.286s)` | **7.09x** | **0.98x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.134s (0.165s)` | `0.783s (0.796s)` | `0.100s (0.513s)` | **5.86x** | **0.75x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.082s (0.091s)` | `0.017s (0.028s)` | `0.046s (0.073s)` | **0.20x** | **0.56x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.572s (2.052s)` | `0.374s (0.385s)` | `0.344s (0.440s)` | **0.65x** | **0.60x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `314.30 (357.40) MiB/s` | `258.78 (431.22) MiB/s` | `187.64 (263.15) MiB/s` | **1.21x** | **1.67x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,177.73 (7,283.37) MiB/s` | `1,996.33 (2,147.77) MiB/s` | `600.26 (616.72) MiB/s` | **3.60x** | **11.96x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `349.89 (363.97) MiB/s` | `180.67 (182.35) MiB/s` | `4.44 (4.45) MiB/s` | **1.94x** | **78.76x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `218.19 (222.55) ops/s` | `907.61 (922.00) ops/s` | `144.17 (145.19) ops/s` | **0.24x** | **1.51x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `506.13 (507.16) ops/s` | `248.17 (248.77) ops/s` | `240.96 (241.09) ops/s` | **2.04x** | **2.10x** |
| `rename` | **目录重命名 rename (500 个)** | `370.65 (392.18) ops/s` | `92.73 (93.25) ops/s` | `54.55 (54.80) ops/s` | **4.00x** | **6.79x** |
| `unlink` | **文件删除 unlink (500 个)** | `523.41 (573.72) ops/s` | `98.42 (100.66) ops/s` | `52.72 (52.82) ops/s` | **5.32x** | **9.93x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `9,230.97 (9,965.28) ops/s` | `136,854.13 (145,231.67) ops/s` | `225,227.76 (240,082.24) ops/s` | **0.07x** | **0.04x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `4,751.91 (4,778.84) ops/s` | `82,191.02 (90,247.60) ops/s` | `255,121.04 (272,719.08) ops/s` | **0.06x** | **0.02x** |
| `stat` | **文件属性查询 stat (500 个)** | `37,522.53 (43,277.60) ops/s` | `621,981.06 (637,607.41) ops/s` | `23,603.53 (23,701.76) ops/s` | **0.06x** | **1.59x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.783s (3.800s)` | `1.879s (1.926s)` | `6.312s (6.347s)` | **0.50x** | **1.67x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.105s (0.111s)` | `0.157s (0.217s)` | `0.063s (0.070s)` | **1.49x** | **0.60x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.033s (0.055s)` | `0.013s (0.014s)` | `0.013s (0.018s)` | **0.39x** | **0.39x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.066s (0.078s)` | `0.004s (0.005s)` | `0.004s (0.004s)` | **0.07x** | **0.06x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.243s (0.250s)` | `0.113s (0.128s)` | `0.174s (0.177s)` | **0.46x** | **0.72x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `186.85 files/s` | `828.20 files/s` | `158.56 files/s` | **0.23x** | **1.18x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `422.56 files/s` | `98.90 files/s` | `343.82 files/s` | **4.27x** | **1.23x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `49.38 MiB/s` | `58.91 MiB/s` | `48.84 MiB/s` | **0.84x** | **1.01x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `57.00 MiB/s` | `602.16 MiB/s` | `77.67 MiB/s` | **0.09x** | **0.73x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,152.52 MiB/s` | `675.49 MiB/s` | `45.67 MiB/s` | **1.71x** | **25.24x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `2,220.21 rows/s` | `6,208.85 rows/s` | `4,123.45 rows/s` | **0.36x** | **0.54x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `212.62 rows/s` | `1,053.70 rows/s` | `920.14 rows/s` | **0.20x** | **0.23x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `38,886.29 rows/s` | `549,845.89 rows/s` | `66,888.39 rows/s` | **0.07x** | **0.58x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `1,129.46 rows/s` | `7,798.82 rows/s` | `6,748.69 rows/s` | **0.14x** | **0.17x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `387.10 rows/s` | `2,669.09 rows/s` | `2,811.81 rows/s` | **0.15x** | **0.14x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `93,749.65 rows/s` | `904,187.87 rows/s` | `914,298.78 rows/s` | **0.10x** | **0.10x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0902 s` | `0.0088 s` | `0.0325 s` | **0.10x** | **0.36x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `284.83 MiB/s`，Drive9 Sync `60.73 MiB/s`，吞吐比 `4.69x`。
- `Read big file`：Bowl `1202.87 MiB/s`，Drive9 Sync `262.09 MiB/s`，吞吐比 `4.59x`。
- `Write small file`：Bowl `326.80 files/s`，Drive9 Sync `100.00 files/s`，吞吐比 `3.27x`。
- `Read small file`：Bowl `1722.50 files/s`，Drive9 Sync `189.70 files/s`，吞吐比 `9.08x`。
- `Stat file`：Bowl `6956.60 files/s`，Drive9 Sync `508.30 files/s`，吞吐比 `13.69x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

本轮包含 ADR-0028：Create、Write、常规 Release 使用单语句原子 CTE 与内联异步 WAL 提交；重开文件时按版本保留内核页缓存。因此重复读取包含缓存命中收益，本轮数据不能单独归因于数据库往返减少。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261102235331`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9ip6zekn1tbk2yerd7`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9ip6zekn1tbk2yerd8`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T15:53:46Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T16:18:23Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
