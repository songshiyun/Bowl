# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

> 本轮于 2026-11-02 测试 Bowl bc64975，Drive9 固定为 b529a36f；冷热各 45 项、每项 3 次采样，三组文件与 SQLite 完整性指纹一致。

> Bowl 采用 ADR-0027：4 MiB Chunk、满 64 条 Slice 整体合并、后台串行整理与回收、PutBlock 直写目标路径、默认 RPC 超时 30 秒。本轮同时包含这些变化，不能将结果变化归因于单项优化。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

由于 Drive9 默认采用客户端本地 Write-Back 缓存 + 本地 `.git`/构建目录 Overlay（`--profile=coding-agent --durability=interactive`），而 Bowl 采用无客户端本地盘缓存的跨机写入服务端页缓存、显式 Fsync 仅同步数据库 WAL 的方式，为兼顾「默认开箱体验对比」与「同步提交模式对比」，本次评测在同台机器上依次测试了三组目标：

1. **Bowl (`bowl`)**：PostgreSQL 16 + Server BlockStore，采用生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，客户端零本地盘缓存，普通 Write 写入服务端页缓存，显式 Fsync 仅同步数据库 WAL，块数据由内核后台回写）。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3，客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `288.07 MiB/s` | `202.99 MiB/s` | `62.84 MiB/s` | **1.42x** | **4.58x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `487.42 MiB/s` | `234.30 MiB/s` | `448.73 MiB/s` | **2.08x** | **1.09x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `157.80 files/s` | `1,913.70 files/s` | `103.00 files/s` | **0.08x** | **1.53x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,334.10 files/s` | `6,749.90 files/s` | `309.30 files/s` | **0.20x** | **4.31x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,544.00 files/s` | `12,130.20 files/s` | `499.00 files/s` | **0.54x** | **13.11x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `374.27 MiB/s` | `324.50 MiB/s` | `189.90 MiB/s` | **1.15x** | **1.97x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `531.05 MiB/s` | `184.12 MiB/s` | `56.21 MiB/s` | **2.88x** | **9.45x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `211.65 ops/s` | `1,010.42 ops/s` | `149.41 ops/s` | **0.21x** | **1.42x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `585.14 ops/s` | `260.19 ops/s` | `256.57 ops/s` | **2.25x** | **2.28x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `466.64 ops/s` | `99.93 ops/s` | `63.17 ops/s` | **4.67x** | **7.39x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `559.88 ops/s` | `101.08 ops/s` | `57.21 ops/s` | **5.54x** | **9.79x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `3.23 s` | `1.86 s` | `5.87 s` | **0.58x** | **1.82x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `1,581.65 rows/s` | `7,726.82 rows/s` | `7,080.74 rows/s` | **0.20x** | **0.22x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj912d5rbcy2stqm8dwi` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.212` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj912d5rbcy2stqm8dwj` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.213` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.043/0.071/0.170/0.033 ms` | `ping -c 20 -i 0.05 172.16.1.212` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.212 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `192.61` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.01` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `bc64975` | Drive9 `b529a36f` | Drive9 `b529a36f` |
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
| **Write big file** | `288.07 MiB/s (14.22 s/file)` | `202.99 MiB/s (20.18 s/file)` | `62.84 MiB/s (65.18 s/file)` | **1.42x** | **4.58x** |
| **Read big file** | `487.42 MiB/s (8.40 s/file)` | `234.30 MiB/s (17.48 s/file)` | `448.73 MiB/s (9.13 s/file)` | **2.08x** | **1.09x** |
| **Write small file** | `157.80 files/s (25.34 ms/file)` | `1913.70 files/s (2.09 ms/file)` | `103.00 files/s (38.85 ms/file)` | **0.08x** | **1.53x** |
| **Read small file** | `1334.10 files/s (3.00 ms/file)` | `6749.90 files/s (0.59 ms/file)` | `309.30 files/s (12.93 ms/file)` | **0.20x** | **4.31x** |
| **Stat file** | `6544.00 files/s (0.61 ms/file)` | `12130.20 files/s (0.33 ms/file)` | `499.00 files/s (8.02 ms/file)` | **0.54x** | **13.11x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `374.27 (381.96) MiB/s` | `324.50 (443.48) MiB/s` | `189.90 (256.66) MiB/s` | **1.15x** | **1.97x** |
| `sequential_read` | **顺序读 (256 MiB)** | `531.05 (533.13) MiB/s` | `184.12 (184.91) MiB/s` | `56.21 (56.60) MiB/s` | **2.88x** | **9.45x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.92 (7.40) MiB/s` | `2.78 (2.79) MiB/s` | `5.12 (5.16) MiB/s` | **2.49x** | **1.35x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `211.65 (224.11) ops/s` | `1,010.42 (1,025.96) ops/s` | `149.41 (150.29) ops/s` | **0.21x** | **1.42x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `585.14 (590.83) ops/s` | `260.19 (261.13) ops/s` | `256.57 (258.00) ops/s` | **2.25x** | **2.28x** |
| `rename` | **目录重命名 rename (500 个)** | `466.64 (467.92) ops/s` | `99.93 (100.52) ops/s` | `63.17 (64.06) ops/s` | **4.67x** | **7.39x** |
| `unlink` | **文件删除 unlink (500 个)** | `559.88 (584.79) ops/s` | `101.08 (101.11) ops/s` | `57.21 (57.58) ops/s` | **5.54x** | **9.79x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `6,701.54 (7,210.70) ops/s` | `6,552.93 (6,971.80) ops/s` | `15,299.19 (16,922.66) ops/s` | **1.02x** | **0.44x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `13,694.96 (14,858.16) ops/s` | `42,682.95 (43,429.06) ops/s` | `106,676.27 (127,407.78) ops/s` | **0.32x** | **0.13x** |
| `stat` | **文件属性查询 stat (500 个)** | `3,140.30 (3,144.09) ops/s` | `20,482.84 (21,826.50) ops/s` | `39,206.71 (44,409.20) ops/s` | **0.15x** | **0.08x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.234s (3.238s)` | `1.864s (1.864s)` | `5.871s (5.945s)` | **0.58x** | **1.82x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.305s (0.313s)` | `2.016s (2.121s)` | `0.267s (0.307s)` | **6.61x** | **0.87x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.122s (0.207s)` | `0.760s (0.764s)` | `0.034s (0.037s)` | **6.21x** | **0.28x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.081s (0.091s)` | `0.016s (0.027s)` | `0.041s (0.042s)` | **0.19x** | **0.50x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.629s (2.021s)` | `0.375s (0.377s)` | `0.332s (0.336s)` | **0.60x** | **0.53x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `381.03 (392.97) MiB/s` | `270.39 (439.10) MiB/s` | `186.61 (262.61) MiB/s` | **1.41x** | **2.04x** |
| `sequential_read` | **顺序读 (256 MiB)** | `397.11 (649.37) MiB/s` | `2,159.66 (2,237.60) MiB/s` | `655.84 (718.08) MiB/s` | **0.18x** | **0.61x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `7.19 (7.41) MiB/s` | `166.97 (178.39) MiB/s` | `4.91 (4.94) MiB/s` | **0.04x** | **1.46x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `196.20 (196.63) ops/s` | `1,077.22 (1,099.15) ops/s` | `149.30 (149.69) ops/s` | **0.18x** | **1.31x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `551.58 (571.17) ops/s` | `264.86 (265.06) ops/s` | `257.39 (260.76) ops/s` | **2.08x** | **2.14x** |
| `rename` | **目录重命名 rename (500 个)** | `573.89 (615.30) ops/s` | `100.14 (101.92) ops/s` | `56.17 (56.94) ops/s` | **5.73x** | **10.22x** |
| `unlink` | **文件删除 unlink (500 个)** | `585.69 (604.72) ops/s` | `105.22 (105.53) ops/s` | `58.80 (58.85) ops/s` | **5.57x** | **9.96x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `6,852.82 (8,694.99) ops/s` | `144,620.48 (158,027.90) ops/s` | `227,614.15 (248,983.01) ops/s` | **0.05x** | **0.03x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `15,137.14 (15,182.99) ops/s` | `85,787.90 (88,181.26) ops/s` | `255,947.06 (256,031.87) ops/s` | **0.18x** | **0.06x** |
| `stat` | **文件属性查询 stat (500 个)** | `46,697.24 (50,692.48) ops/s` | `626,913.65 (652,416.78) ops/s` | `434,725.15 (436,059.43) ops/s` | **0.07x** | **0.11x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.159s (3.243s)` | `1.824s (1.851s)` | `5.790s (5.794s)` | **0.58x** | **1.83x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.171s (0.208s)` | `0.217s (0.231s)` | `0.048s (0.068s)` | **1.27x** | **0.28x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.062s (0.085s)` | `0.013s (0.014s)` | `0.011s (0.012s)` | **0.21x** | **0.17x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.071s (0.076s)` | `0.004s (0.004s)` | `0.004s (0.004s)` | **0.06x** | **0.05x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.249s (0.301s)` | `0.116s (0.117s)` | `0.157s (0.169s)` | **0.47x** | **0.63x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `216.76 files/s` | `924.39 files/s` | `168.26 files/s` | **0.23x** | **1.29x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `337.76 files/s` | `100.91 files/s` | `371.57 files/s` | **3.35x** | **0.91x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `52.05 MiB/s` | `58.63 MiB/s` | `50.74 MiB/s` | **0.89x** | **1.03x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `86.09 MiB/s` | `617.02 MiB/s` | `83.19 MiB/s` | **0.14x** | **1.03x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `138.21 MiB/s` | `890.27 MiB/s` | `54.25 MiB/s` | **0.16x** | **2.55x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `3,094.95 rows/s` | `7,349.83 rows/s` | `4,450.11 rows/s` | **0.42x** | **0.70x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `333.00 rows/s` | `1,143.55 rows/s` | `996.22 rows/s` | **0.29x** | **0.33x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `42,295.77 rows/s` | `566,548.41 rows/s` | `72,251.81 rows/s` | **0.07x** | **0.59x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `1,581.65 rows/s` | `7,726.82 rows/s` | `7,080.74 rows/s` | **0.20x** | **0.22x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `629.93 rows/s` | `3,260.54 rows/s` | `3,100.91 rows/s` | **0.19x** | **0.20x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `92,427.55 rows/s` | `848,927.40 rows/s` | `969,282.09 rows/s` | **0.11x** | **0.10x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0727 s` | `0.0086 s` | `0.0294 s` | **0.12x** | **0.40x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `288.07 MiB/s`，Drive9 Sync `62.84 MiB/s`，吞吐比 `4.58x`。
- `Read big file`：Bowl `487.42 MiB/s`，Drive9 Sync `448.73 MiB/s`，吞吐比 `1.09x`。
- `Write small file`：Bowl `157.80 files/s`，Drive9 Sync `103.00 files/s`，吞吐比 `1.53x`。
- `Read small file`：Bowl `1334.10 files/s`，Drive9 Sync `309.30 files/s`，吞吐比 `4.31x`。
- `Stat file`：Bowl `6544.00 files/s`，Drive9 Sync `499.00 files/s`，吞吐比 `13.11x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261102140214`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj912d5rbcy2stqm8dwi`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj912d5rbcy2stqm8dwj`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T06:02:29Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T06:26:17Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
