# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 16 仅保存 `disks` 与 `commits` 控制面表（ADR-0032）；每个 `Disk` 由单个常驻 `StoreActor`（单写者串行执行器 + Pebble 引擎，ADR-0030/0031）承载，保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef`（`PackID + Offset + Length`）索引；文件数据追加写入 `blobs/<pack_id>.pack` 的 64 MiB 不可变 Pack 文件中（`1 MiB` 分块直写、局部写 CoW、Writev/Readv 聚散 I/O），常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`），显式 `Fsync`/`Sync` 触发 Pack `fdatasync` + Pebble WAL `LogData(Sync)` 持久化屏障。客户端挂载时执行单次 `MountDisk` 握手获取 `WriterEpoch`（无后台心跳），采用 1 MiB Write-Through 直通写（禁用 `CAP_WRITEBACK_CACHE`）+ `FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存 + 写后 `AttrTimeout=1.0s` + `MaxBackground=64`。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3（全部落于 Server 本地 NVMe SSD `/mnt/nvme`），客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 NVMe SSD Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地 NVMe 磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `805.15 MiB/s` | `1,313.91 MiB/s` | `656.10 MiB/s` | **0.61x** | **1.23x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,320.61 MiB/s` | `1,309.62 MiB/s` | `1,350.39 MiB/s` | **1.01x** | **0.98x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `791.80 files/s` | `2,043.70 files/s` | `82.90 files/s` | **0.39x** | **9.55x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,049.30 files/s` | `3,447.00 files/s` | `286.40 files/s` | **0.30x** | **3.66x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `93,674.30 files/s` | `13,835.40 files/s` | `448.70 files/s` | **6.77x** | **208.77x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `186.86 MiB/s` | `655.68 MiB/s` | `346.44 MiB/s` | **0.28x** | **0.54x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `487.53 MiB/s` | `61.75 MiB/s` | `53.28 MiB/s` | **7.89x** | **9.15x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `136.67 ops/s` | `2,150.00 ops/s` | `112.29 ops/s` | **0.06x** | **1.22x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `607.89 ops/s` | `215.22 ops/s` | `208.11 ops/s` | **2.82x** | **2.92x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `390.35 ops/s` | `85.24 ops/s` | `51.10 ops/s` | **4.58x** | **7.64x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `349.17 ops/s` | `89.75 ops/s` | `33.43 ops/s` | **3.89x** | **10.44x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `5.59 s` | `1.99 s` | `7.15 s` | **0.36x** | **1.28x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `524.91 rows/s` | `6,323.03 rows/s` | `6,889.05 rows/s` | **0.08x** | **0.08x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 + 本地数据盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj90x3rcwcyduv529x01` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.199` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj90x3rcwcyduv529x02` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.198` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.038/0.080/0.276/0.060 ms` | `ping -c 20 -i 0.05 172.16.1.199` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.199 -t 5` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序写基线** | `1998.05` MiB/s | `/mnt/nvme` `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序读基线** | `3494.88` MiB/s | `/mnt/nvme` `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 节点本地 NVMe SSD 4K 随机写 / 随机读 IOPS** | `69933` / `699362` IOPS | `/mnt/nvme` `fio --rw=randwrite/randread --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 节点本地 NVMe SSD 4K 同步写 (`fsync=1`)** | `50869` IOPS (P99 `63.6` µs) | `/mnt/nvme` `fio --rw=write --bs=4k --iodepth=1 --direct=1 --fsync=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `c6e884e` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / 每盘单写者 `StoreActor` (Pebble) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **服务端数据块引擎** | 每盘 `StoreActor` Pebble + `blobs/<pack_id>.pack` 64 MiB Pack 文件 (`/var/lib/bowl/data` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`，单次 `MountDisk` 握手) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | 1 MiB Write-Through 直通写（`writeback_cache=false`）+ `CAP_HANDLE_KILLPRIV` 负缓存；`pebble.NoSync` 异步 WAL（后台 1 秒刷盘）；显式 Fsync/Sync 触发 Pack `fdatasync` + WAL `LogData(Sync)` | 客户端本地 NVMe SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `805.15 MiB/s (5.09 s/file)` | `1313.91 MiB/s (3.12 s/file)` | `656.10 MiB/s (6.24 s/file)` | **0.61x** | **1.23x** |
| **Read big file** | `1320.61 MiB/s (3.10 s/file)` | `1309.62 MiB/s (3.13 s/file)` | `1350.39 MiB/s (3.03 s/file)` | **1.01x** | **0.98x** |
| **Write small file** | `791.80 files/s (5.05 ms/file)` | `2043.70 files/s (1.96 ms/file)` | `82.90 files/s (48.26 ms/file)` | **0.39x** | **9.55x** |
| **Read small file** | `1049.30 files/s (3.81 ms/file)` | `3447.00 files/s (1.16 ms/file)` | `286.40 files/s (13.97 ms/file)` | **0.30x** | **3.66x** |
| **Stat file** | `93674.30 files/s (0.04 ms/file)` | `13835.40 files/s (0.29 ms/file)` | `448.70 files/s (8.91 ms/file)` | **6.77x** | **208.77x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `186.86 (202.94) MiB/s` | `655.68 (665.73) MiB/s` | `346.44 (348.05) MiB/s` | **0.28x** | **0.54x** |
| `sequential_read` | **顺序读 (256 MiB)** | `487.53 (492.73) MiB/s` | `61.75 (539.20) MiB/s` | `53.28 (53.43) MiB/s` | **7.89x** | **9.15x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `1.30 (1.32) MiB/s` | `2.15 (2.27) MiB/s` | `0.61 (0.62) MiB/s` | **0.61x** | **2.13x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `136.67 (141.11) ops/s` | `2,150.00 (2,186.63) ops/s` | `112.29 (114.83) ops/s` | **0.06x** | **1.22x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `607.89 (635.92) ops/s` | `215.22 (215.74) ops/s` | `208.11 (209.16) ops/s` | **2.82x** | **2.92x** |
| `rename` | **目录重命名 rename (500 个)** | `390.35 (391.70) ops/s` | `85.24 (86.34) ops/s` | `51.10 (51.30) ops/s` | **4.58x** | **7.64x** |
| `unlink` | **文件删除 unlink (500 个)** | `349.17 (410.93) ops/s` | `89.75 (91.56) ops/s` | `33.43 (33.67) ops/s` | **3.89x** | **10.44x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `19,435.67 (23,881.76) ops/s` | `10,022.79 (10,713.23) ops/s` | `23,651.90 (23,763.78) ops/s` | **1.94x** | **0.82x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `169,272.05 (170,669.71) ops/s` | `44,764.16 (45,089.06) ops/s` | `21,683.79 (21,727.86) ops/s` | **3.78x** | **7.81x** |
| `stat` | **文件属性查询 stat (500 个)** | `55,818.40 (59,882.15) ops/s` | `12,214.03 (12,825.01) ops/s` | `72.68 (72.90) ops/s` | **4.57x** | **768.00x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `5.587s (5.630s)` | `1.994s (1.997s)` | `7.152s (7.153s)` | **0.36x** | **1.28x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.136s (0.140s)` | `1.722s (1.781s)` | `0.266s (0.359s)` | **12.68x** | **1.96x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.066s (0.076s)` | `0.860s (0.882s)` | `0.715s (0.722s)` | **13.06x** | **10.86x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.058s (0.062s)` | `0.011s (0.013s)` | `0.067s (0.089s)` | **0.18x** | **1.14x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.476s (1.927s)` | `0.414s (0.419s)` | `0.343s (0.448s)` | **0.87x** | **0.72x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `189.51 (197.83) MiB/s` | `666.51 (670.72) MiB/s` | `344.39 (349.88) MiB/s` | **0.28x** | **0.55x** |
| `sequential_read` | **顺序读 (256 MiB)** | `6,503.59 (6,505.19) MiB/s` | `444.84 (1,896.14) MiB/s` | `606.57 (617.91) MiB/s` | **14.62x** | **10.72x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `394.57 (402.86) MiB/s` | `167.52 (192.63) MiB/s` | `2.52 (2.56) MiB/s` | **2.36x** | **156.29x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `126.02 (142.85) ops/s` | `2,181.61 (2,187.80) ops/s` | `104.29 (106.83) ops/s` | **0.06x** | **1.21x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `434.45 (674.58) ops/s` | `212.17 (212.37) ops/s` | `205.60 (206.92) ops/s` | **2.05x** | **2.11x** |
| `rename` | **目录重命名 rename (500 个)** | `317.40 (368.07) ops/s` | `84.41 (85.22) ops/s` | `49.89 (50.45) ops/s` | **3.76x** | **6.36x** |
| `unlink` | **文件删除 unlink (500 个)** | `333.77 (423.74) ops/s` | `86.38 (86.77) ops/s` | `35.45 (35.48) ops/s` | **3.86x** | **9.42x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `423,307.30 (436,221.05) ops/s` | `106,125.00 (136,280.11) ops/s` | `217,462.22 (218,207.73) ops/s` | **3.99x** | **1.95x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `1,155,025.17 (1,158,352.05) ops/s` | `80,105.10 (82,089.60) ops/s` | `238,320.51 (239,415.47) ops/s` | **14.42x** | **4.85x** |
| `stat` | **文件属性查询 stat (500 个)** | `723,083.58 (734,005.92) ops/s` | `584,488.38 (591,083.99) ops/s` | `471,514.85 (526,424.01) ops/s` | **1.24x** | **1.53x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `5.543s (5.643s)` | `1.985s (2.011s)` | `7.162s (7.317s)` | **0.36x** | **1.29x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.030s (0.039s)` | `0.170s (0.231s)` | `0.050s (0.060s)` | **5.66x** | **1.66x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.005s (0.006s)` | `0.016s (0.025s)` | `0.013s (0.020s)` | **3.09x** | **2.52x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.005s (0.009s)` | `0.005s (0.005s)` | `0.004s (0.004s)` | **0.95x** | **0.72x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.086s (0.092s)` | `0.126s (0.127s)` | `0.111s (0.174s)` | **1.47x** | **1.30x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `114.03 files/s` | `1,791.82 files/s` | `126.13 files/s` | **0.06x** | **0.90x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `334.17 files/s` | `57.40 files/s` | `60.12 files/s` | **5.82x** | **5.56x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `47.14 MiB/s` | `61.68 MiB/s` | `49.03 MiB/s` | **0.76x** | **0.96x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `1,244.40 MiB/s` | `737.21 MiB/s` | `55.21 MiB/s` | **1.69x** | **22.54x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,249.40 MiB/s` | `596.28 MiB/s` | `49.38 MiB/s` | **2.10x** | **25.30x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `1,282.98 rows/s` | `4,915.64 rows/s` | `3,328.55 rows/s` | **0.26x** | **0.39x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `113.33 rows/s` | `899.89 rows/s` | `760.31 rows/s` | **0.13x** | **0.15x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `632,800.31 rows/s` | `555,785.93 rows/s` | `59,400.45 rows/s` | **1.14x** | **10.65x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `524.91 rows/s` | `6,323.03 rows/s` | `6,889.05 rows/s` | **0.08x** | **0.08x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `199.65 rows/s` | `2,233.18 rows/s` | `2,608.68 rows/s` | **0.09x** | **0.08x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `879,785.55 rows/s` | `1,038,299.46 rows/s` | `1,090,981.93 rows/s` | **0.85x** | **0.81x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.1890 s` | `0.0096 s` | `0.0360 s` | **0.05x** | **0.19x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `805.15 MiB/s`，Drive9 Sync `656.10 MiB/s`，吞吐比 `1.23x`。
- `Read big file`：Bowl `1320.61 MiB/s`，Drive9 Sync `1350.39 MiB/s`，吞吐比 `0.98x`。
- `Write small file`：Bowl `791.80 files/s`，Drive9 Sync `82.90 files/s`，吞吐比 `9.55x`。
- `Read small file`：Bowl `1049.30 files/s`，Drive9 Sync `286.40 files/s`，吞吐比 `3.66x`。
- `Stat file`：Bowl `93674.30 files/s`，Drive9 Sync `448.70 files/s`，吞吐比 `208.77x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `805.15 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `186.86 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令同时清理 Client 与 Server 双端内核页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`），且各目标栈在 Server 端按进程隔离启停；但未在每轮内部重启 FUSE 进程或清空 Drive9 用户态内存缓存。因此本报告的 cold 表示双端 OS 页缓存清空下的冷状态。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0030/0031/0032 同时引入每盘单写者 `StoreActor`、64 MiB Pack 文件引擎与单次 `MountDisk` 握手；单轮数据反映当前 `labs/actor-architecture` 分支整体表现，本套件未测试整机断电恢复。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9bench-kp-20261109-052701`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj90x3rcwcyduv529x01`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj90x3rcwcyduv529x02`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-09T05:27:18Z` |
| **实例销毁时间 (UTC)** | `2026-11-09T05:55:16Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
