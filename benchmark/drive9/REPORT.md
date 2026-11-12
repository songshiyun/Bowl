# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 18（deploy/prod 容器栈）仅保存单张 `disks` 控制面表（ADR-0032）；每个 `Disk` 由单个常驻 `StoreActor`（单写者串行执行器 + Pebble 引擎，ADR-0030/0031）承载，保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef`（`PackID + Offset + Length`）索引；文件数据追加写入 `packs/<pack_id_hex16>.pack` 的 64 MiB Append-Only Pack 文件中（`256 KiB` 逻辑块网格、`AppendBlocks` 整块零冗余拷贝、连续块合并单次 `pread`、`8 MiB` `SyncFileRange` 渐进回写、后台异步死包回收、`WriteRequest`/`ReadResponse` 池化零分配编解码与 `64 MiB` 分片热块缓存），常规写批次以 `pebble.NoSync` 提交异步 WAL，显式 `Fsync`/`Sync` 触发 Pack `fdatasync` + Pebble WAL `LogData(Sync)` 双层并行落盘屏障。客户端挂载时执行单次 `MountDisk` 握手获取 `WriterEpoch`（无后台心跳），禁用内核 `CAP_WRITEBACK_CACHE`，配备客户端 `1+8` H2C 多连接条带池（`32M / 8×64M`）与服务端 `128 MiB` H2C 窗口、`MarshalAppend` 零分配编码、`WriteBehindCoordinator`（`4 MiB × 4` 并发写流水线，`2 MiB` 同步探测 + `Create` 预热与小块连续写内存聚合）、`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4, ReadAheadMaxGlobalInflight=32`）、`completeDirs` 本地目录流合成、`FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存 + 写后 `AttrTimeout=1.0s` + `MaxBackground=64`。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3（全部落于 Server 本地 NVMe SSD `/mnt/nvme`），客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 NVMe SSD Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地 NVMe 磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,696.00 MiB/s` | `1,383.02 MiB/s` | `688.52 MiB/s` | **1.23x** | **2.46x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,648.56 MiB/s` | `2,133.34 MiB/s` | `1,422.55 MiB/s` | **0.77x** | **1.16x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `1,710.70 files/s` | `2,102.20 files/s` | `89.10 files/s` | **0.81x** | **19.20x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `4,031.60 files/s` | `5,118.60 files/s` | `739.70 files/s` | **0.79x** | **5.45x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `100,365.00 files/s` | `49,665.70 files/s` | `63,833.30 files/s` | **2.02x** | **1.57x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `1,363.60 MiB/s` | `674.54 MiB/s` | `365.28 MiB/s` | **2.02x** | **3.73x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `1,181.47 MiB/s` | `644.74 MiB/s` | `57.91 MiB/s` | **1.83x** | **20.40x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `761.72 ops/s` | `2,289.59 ops/s` | `132.46 ops/s` | **0.33x** | **5.75x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `3,490.50 ops/s` | `229.96 ops/s` | `219.55 ops/s` | **15.18x** | **15.90x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `3,062.26 ops/s` | `88.53 ops/s` | `54.36 ops/s` | **34.59x** | **56.34x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `3,312.72 ops/s` | `95.39 ops/s` | `38.72 ops/s` | **34.73x** | **85.55x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `0.40 s` | `1.91 s` | `6.51 s` | **4.80x** | **16.32x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `41,494.17 rows/s` | `6,975.40 rows/s` | `7,957.58 rows/s` | **5.95x** | **5.21x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 + 本地数据盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-<redacted-server>` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.196` |
| **Client 客户端** (`bowl-d9-client`) | `i-<redacted-client>` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.197` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-<redacted>` / `vsw-<redacted>` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.050/0.090/0.260/0.048 ms` | `ping -c 20 -i 0.05 172.16.1.196` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.196 -t 5` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序写基线** | `1998.05` MiB/s | `/mnt/nvme` `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序读基线** | `3494.88` MiB/s | `/mnt/nvme` `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 节点本地 NVMe SSD 4K 随机写 / 随机读 IOPS** | `69945` / `699819` IOPS | `/mnt/nvme` `fio --rw=randwrite/randread --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 节点本地 NVMe SSD 4K 同步写 (`fsync=1`)** | `51570` IOPS (P99 `62.6` µs) | `/mnt/nvme` `fio --rw=write --bs=4k --iodepth=1 --direct=1 --fsync=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `6399122` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **控制面 / 文件元数据** | postgres (PostgreSQL) 18.6 控制面 / 每盘单写者 `StoreActor` (Pebble) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **部署编排与可观测性栈** | `deploy/prod (docker-compose.yaml + storage.env)` · `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` | systemd (`tidb` + `drive9-server`) | systemd (`tidb` + `drive9-server`) |
| **服务端数据块引擎** | 每盘 `StoreActor` Pebble + `packs/<pack_id_hex16>.pack` 64 MiB Pack 文件 (`/var/lib/bowl/data` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`，单次 `MountDisk` 握手) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发写流水线 + 小块连续写聚合，`writeback_cache=false`）+ 客户端 `1+8` H2C 池 (`32M/8×64M`) / 服务端 `128M` 窗口 + 零分配编解码；Group Commit `pebble.NoSync` + `8 MiB` `SyncFileRange` 渐进回写 + Fsync 双层并行落盘屏障 | 客户端本地 NVMe SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4, ReadAheadMaxGlobalInflight=32`）+ `completeDirs` 本地目录流合成 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `1696.00 MiB/s (2.42 s/file)` | `1383.02 MiB/s (2.96 s/file)` | `688.52 MiB/s (5.95 s/file)` | **1.23x** | **2.46x** |
| **Read big file** | `1648.56 MiB/s (2.48 s/file)` | `2133.34 MiB/s (1.92 s/file)` | `1422.55 MiB/s (2.88 s/file)` | **0.77x** | **1.16x** |
| **Write small file** | `1710.70 files/s (2.34 ms/file)` | `2102.20 files/s (1.90 ms/file)` | `89.10 files/s (44.87 ms/file)` | **0.81x** | **19.20x** |
| **Read small file** | `4031.60 files/s (0.99 ms/file)` | `5118.60 files/s (0.78 ms/file)` | `739.70 files/s (5.41 ms/file)` | **0.79x** | **5.45x** |
| **Stat file** | `100365.00 files/s (0.04 ms/file)` | `49665.70 files/s (0.08 ms/file)` | `63833.30 files/s (0.06 ms/file)` | **2.02x** | **1.57x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `1,363.60 (1,365.22) MiB/s` | `674.54 (679.51) MiB/s` | `365.28 (368.92) MiB/s` | **2.02x** | **3.73x** |
| `sequential_read` | **顺序读 (256 MiB)** | `1,181.47 (1,325.61) MiB/s` | `644.74 (673.10) MiB/s` | `57.91 (57.92) MiB/s` | **1.83x** | **20.40x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `7.26 (7.43) MiB/s` | `5.02 (5.07) MiB/s` | `5.58 (5.66) MiB/s` | **1.45x** | **1.30x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `761.72 (789.75) ops/s` | `2,289.59 (2,290.61) ops/s` | `132.46 (135.30) ops/s` | **0.33x** | **5.75x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `3,490.50 (3,604.38) ops/s` | `229.96 (230.38) ops/s` | `219.55 (221.21) ops/s` | **15.18x** | **15.90x** |
| `rename` | **目录重命名 rename (500 个)** | `3,062.26 (3,474.53) ops/s` | `88.53 (91.68) ops/s` | `54.36 (54.75) ops/s` | **34.59x** | **56.34x** |
| `unlink` | **文件删除 unlink (500 个)** | `3,312.72 (3,447.96) ops/s` | `95.39 (102.33) ops/s` | `38.72 (38.87) ops/s` | **34.73x** | **85.55x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `65,134.91 (71,961.37) ops/s` | `10,537.89 (10,754.98) ops/s` | `28,970.10 (29,036.93) ops/s` | **6.18x** | **2.25x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `234,137.31 (241,518.88) ops/s` | `57,487.96 (61,697.66) ops/s` | `23,026.22 (155,449.01) ops/s` | **4.07x** | **10.17x** |
| `stat` | **文件属性查询 stat (500 个)** | `56,560.37 (60,615.91) ops/s` | `13,529.84 (15,088.75) ops/s` | `87.19 (87.23) ops/s` | **4.18x** | **648.72x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.399s (1.067s)` | `1.912s (1.981s)` | `6.508s (6.567s)` | **4.80x** | **16.32x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.199s (0.401s)` | `1.607s (1.705s)` | `0.785s (0.856s)` | **8.06x** | **3.94x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.163s (0.286s)` | `0.801s (0.807s)` | `0.132s (0.139s)` | **4.91x** | **0.81x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.008s (0.043s)` | `0.010s (0.010s)` | `0.049s (0.050s)` | **1.24x** | **5.95x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.590s (2.100s)` | `0.397s (0.400s)` | `0.388s (0.412s)` | **0.67x** | **0.66x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `1,312.59 (1,403.39) MiB/s` | `719.29 (722.65) MiB/s` | `358.64 (361.92) MiB/s` | **1.82x** | **3.66x** |
| `sequential_read` | **顺序读 (256 MiB)** | `8,316.35 (8,499.17) MiB/s` | `1,564.77 (2,115.20) MiB/s` | `597.06 (640.26) MiB/s` | **5.31x** | **13.93x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `497.09 (510.26) MiB/s` | `182.19 (184.72) MiB/s` | `5.69 (5.83) MiB/s` | **2.73x** | **87.36x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `759.71 (764.26) ops/s` | `2,483.47 (2,512.65) ops/s` | `132.32 (135.16) ops/s` | **0.31x** | **5.74x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `3,664.14 (3,793.36) ops/s` | `226.06 (226.68) ops/s` | `222.47 (224.53) ops/s` | **16.21x** | **16.47x** |
| `rename` | **目录重命名 rename (500 个)** | `3,583.44 (3,610.87) ops/s` | `86.73 (88.83) ops/s` | `52.55 (56.08) ops/s` | **41.31x** | **68.19x** |
| `unlink` | **文件删除 unlink (500 个)** | `3,871.52 (4,115.77) ops/s` | `93.01 (93.14) ops/s` | `43.46 (43.52) ops/s` | **41.63x** | **89.09x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `407,199.28 (534,763.95) ops/s` | `121,972.04 (129,925.06) ops/s` | `223,301.51 (231,907.85) ops/s` | **3.34x** | **1.82x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `1,094,599.68 (1,501,847.01) ops/s` | `56,770.53 (87,163.84) ops/s` | `248,752.75 (265,120.90) ops/s` | **19.28x** | **4.40x** |
| `stat` | **文件属性查询 stat (500 个)** | `834,705.03 (836,920.05) ops/s` | `620,651.56 (710,300.80) ops/s` | `564,441.11 (578,918.04) ops/s` | **1.34x** | **1.48x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.339s (0.341s)` | `1.918s (1.956s)` | `6.563s (6.681s)` | **5.66x** | **19.35x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.030s (0.048s)` | `0.219s (0.223s)` | `0.063s (0.072s)` | **7.25x** | **2.10x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.025s (0.043s)` | `0.013s (0.013s)` | `0.012s (0.013s)` | **0.50x** | **0.48x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.003s (0.003s)` | `0.004s (0.004s)` | `0.004s (0.007s)` | **1.45x** | **1.20x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.031s (0.057s)` | `0.113s (0.116s)` | `0.142s (0.157s)` | **3.61x** | **4.54x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `808.70 files/s` | `1,992.70 files/s` | `144.75 files/s` | **0.41x** | **5.59x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `1,845.54 files/s` | `92.12 files/s` | `326.19 files/s` | **20.03x** | **5.66x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `65.64 MiB/s` | `62.90 MiB/s` | `51.98 MiB/s` | **1.04x** | **1.26x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `1,326.73 MiB/s` | `696.86 MiB/s` | `76.72 MiB/s` | **1.90x** | **17.29x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,334.73 MiB/s` | `861.93 MiB/s` | `51.24 MiB/s` | **1.55x** | **26.05x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `27,183.83 rows/s` | `5,915.48 rows/s` | `4,142.70 rows/s` | **4.60x** | **6.56x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `7,683.89 rows/s` | `755.27 rows/s` | `847.33 rows/s` | **10.17x** | **9.07x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `835,296.50 rows/s` | `660,912.94 rows/s` | `66,599.01 rows/s` | **1.26x** | **12.54x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `41,494.17 rows/s` | `6,975.40 rows/s` | `7,957.58 rows/s` | **5.95x** | **5.21x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `19,946.16 rows/s` | `2,525.47 rows/s` | `2,583.91 rows/s` | **7.90x** | **7.72x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `1,271,455.82 rows/s` | `1,185,948.36 rows/s` | `1,089,997.15 rows/s` | **1.07x** | **1.17x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0057 s` | `0.0079 s` | `0.0333 s` | **1.38x** | **5.79x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `1696.00 MiB/s`，Drive9 Sync `688.52 MiB/s`，吞吐比 `2.46x`。
- `Read big file`：Bowl `1648.56 MiB/s`，Drive9 Sync `1422.55 MiB/s`，吞吐比 `1.16x`。
- `Write small file`：Bowl `1710.70 files/s`，Drive9 Sync `89.10 files/s`，吞吐比 `19.20x`。
- `Read small file`：Bowl `4031.60 files/s`，Drive9 Sync `739.70 files/s`，吞吐比 `5.45x`。
- `Stat file`：Bowl `100365.00 files/s`，Drive9 Sync `63833.30 files/s`，吞吐比 `1.57x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1696.00 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `1363.60 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令同时清理 Client 与 Server 双端内核页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`），且各目标栈在 Server 端按进程隔离启停；但未在每轮内部重启 FUSE 进程或清空 Drive9 用户态内存缓存。因此本报告的 cold 表示双端 OS 页缓存清空下的冷状态。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0030/0031/0032 同时引入每盘单写者 `StoreActor`、64 MiB Pack 文件引擎与单次 `MountDisk` 握手；单轮数据反映当前 `labs/actor-architecture` 分支整体表现，本套件未测试整机断电恢复。

---

## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `1276` 条（跨 Client+Server 全链路 `827` 条，异常链路 `104` 条）· Loki 业务日志 `15` 条（服务端 Warn/Error `0` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `d9_cold_io` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `d9_cold_seq_read`, `d9_cold_small_write_fsync`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `23.947 s` (`23947.0 ms`) vs FUSE 累计活跃 `3070.05 ms`（等效并发度 `0.13x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`3070.05 ms` / RPC `8271.91 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `97.54%` / Readlink `N/A` · WriteBehind 聚批 `2288.34 MiB` / 直通 `18.00 MiB` (Flush RPC `1176`) · ReadAhead 预读 `768.00 MiB` / 命中 `766.88 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 20,764 | 3070.05 | 0.148 | 0.016 | 0.997 | 100.0% | 错误返回 4174 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 9,114 | 1801.22 | 0.198 | 0.027 | 0.998 | 58.7% | 占 FUSE 58.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 609 | 601.94 | 0.988 | 0.787 | 10.923 | 19.6% | 占 FUSE 19.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 2,904 | 291.63 | 0.100 | 0.085 | 1.065 | 9.5% | 占 FUSE 9.5% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 73 | 44.50 | 0.610 | 0.575 | 3.175 | 1.4% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 6 | 10.71 | 1.785 | 1.750 | 2.485 | 0.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 19 | 114.82 | 6.043 | 6.750 | 24.050 | 3.7% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 6,045 | 8222.54 | 1.360 | 0.496 | 23.095 | 99.4% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 2.69x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 6,045 | 4936.10 | 0.817 | 0.318 | 21.458 | 59.7% | Write:2.257ms, Read:0.586ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 6,045 | 5825.09 | 0.964 | 0.329 | 19.573 | 70.4% | HTTP Body+Codec 差值均值 0.420 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 6,045 | 3286.45 | 0.544 | 0.178 | 9.513 | 39.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,194 | 2123.16 | 1.778 | 0.250 | 9.513 | 25.7% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 3,168 | 898.69 | 0.284 | 0.184 | 2.473 | 10.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,866 | 4.75 | 0.002 | 0.003 | 0.008 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3,179 | 848.41 | 0.267 | 0.182 | 2.453 | 10.3% | read(3168), lookup(11) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,045 | 29.24 | 0.005 | 0.003 | 0.043 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 921 | 14.02 | 0.015 | 0.018 | 0.044 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,866 | 38.03 | 0.013 | 0.011 | 0.068 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,866 | 11.93 | 0.004 | 0.003 | 0.021 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,257 | 25.22 | 0.011 | 0.008 | 0.033 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 3,213 | 769.83 | 0.240 | 0.180 | 2.370 | 9.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,194 | 1274.05 | 1.067 | 0.040 | 4.833 | 15.4% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 609 | 110.65 | 0.182 | 0.174 | 2.955 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 609 | 123.03 | 0.202 | 0.173 | 5.650 | 1.5% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 100 | 57.64 | 0.576 | 0.021 | 8.333 | 0.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 645 | 244.69 | 0.379 | 0.171 | 8.297 | 3.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 11 | 0.04 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 610 | 98.14 | 0.161 | 0.093 | 3.290 | 1.2% | — |

#### 工作负载 `d9_cold_meta_macro` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `33.778 s` (`33778.0 ms`) vs FUSE 累计活跃 `7346.96 ms`（等效并发度 `0.22x`）；占比基准：`FUSE 活跃总耗时` (`7346.96 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `99.26%` / Readlink `N/A` · WriteBehind 聚批 `64.16 MiB` / 直通 `0.00 MiB` (Flush RPC `5279`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `91.68%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 123,184 | 7346.96 | 0.060 | 0.002 | 0.501 | 100.0% | 错误返回 25008 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 6,623 | 1641.10 | 0.248 | 0.268 | 0.964 | 22.3% | 占 FUSE 22.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,307 | 1479.44 | 0.279 | 0.280 | 0.510 | 20.1% | 占 FUSE 20.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,056 | 1220.87 | 0.241 | 0.214 | 0.509 | 16.6% | 占 FUSE 16.6% |
| `2. 客户端等待` | `Client InodeLock 等待` | 3 | 0.54 | 0.179 | 0.075 | 0.492 | 0.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 26,305 | 6633.36 | 0.252 | 0.230 | 20.500 | 90.3% | 非 OK 状态 47 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 26,305 | 5255.77 | 0.200 | 0.181 | 20.401 | 71.5% | Write:0.220ms, Create:0.204ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 26,305 | 2233.52 | 0.085 | 0.080 | 2.365 | 30.4% | HTTP Body+Codec 差值均值 0.033 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 26,305 | 1377.60 | 0.052 | 0.050 | 2.365 | 18.8% | 非 OK 状态 47 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 5,315 | 400.74 | 0.075 | 0.072 | 0.242 | 5.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Create` | 5,307 | 290.55 | 0.055 | 0.053 | 0.148 | 4.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 23,456 | 26.27 | 0.001 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 2,852 | 55.70 | 0.020 | 0.019 | 0.063 | 0.8% | read(1118), readdirplus(1560), lookup(129) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 11,053 | 104.72 | 0.009 | 0.008 | 0.045 | 1.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,560 | 10.47 | 0.007 | 0.004 | 0.040 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 23,456 | 245.41 | 0.010 | 0.009 | 0.045 | 3.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 23,456 | 49.84 | 0.002 | 0.003 | 0.008 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 23,447 | 134.83 | 0.006 | 0.005 | 0.023 | 1.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 5,330 | 65.47 | 0.012 | 0.008 | 0.091 | 0.9% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 3 | 0.03 | 0.009 | 0.008 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 9 | 1.81 | 0.201 | 0.196 | 0.489 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 9 | 3.27 | 0.363 | 0.281 | 2.365 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 0.36 | 0.362 | 0.375 | 0.497 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 10 | 5.25 | 0.525 | 0.190 | 4.750 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.003 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 11 | 1.85 | 0.168 | 0.150 | 0.395 | 0.0% | — |

#### 工作负载 `d9_cold_seq_read` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.163 s` (`7163.0 ms`) vs FUSE 累计活跃 `453.88 ms`（等效并发度 `0.06x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`453.88 ms` / RPC `2818.44 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `81.82%` / Readlink `N/A` · WriteBehind 聚批 `762.00 MiB` / 直通 `6.00 MiB` (Flush RPC `192`) · ReadAhead 预读 `768.00 MiB` / 命中 `766.88 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 7,788 | 453.88 | 0.058 | 0.018 | 0.562 | 100.0% | 错误返回 787 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 6,144 | 312.89 | 0.051 | 0.018 | 0.479 | 68.9% | 占 FUSE 68.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 768 | 92.50 | 0.120 | 0.096 | 1.024 | 20.4% | 占 FUSE 20.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 3 | 37.29 | 12.431 | 13.653 | 16.384 | 8.2% | 占 FUSE 8.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 12 | 11.04 | 0.920 | 0.643 | 4.700 | 2.4% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 2 | 3.65 | 1.827 | 1.750 | 2.485 | 0.8% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 19 | 114.82 | 6.043 | 6.750 | 24.050 | 25.3% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 415 | 2809.41 | 6.770 | 7.195 | 23.437 | 99.7% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 6.21x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 415 | 1838.29 | 4.430 | 5.056 | 15.233 | 65.2% | Read:4.959ms, Write:4.295ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 415 | 2016.48 | 4.859 | 4.778 | 21.288 | 71.5% | HTTP Body+Codec 差值均值 2.519 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 415 | 971.11 | 2.340 | 2.139 | 10.150 | 34.5% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 198 | 599.70 | 3.029 | 2.595 | 10.150 | 21.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 198 | 353.75 | 1.787 | 1.781 | 8.020 | 12.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 215 | 0.58 | 0.003 | 0.003 | 0.010 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 200 | 347.25 | 1.736 | 1.747 | 8.020 | 12.3% | read(198), lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 192 | 3.63 | 0.019 | 0.019 | 0.049 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 207 | 1.14 | 0.005 | 0.006 | 0.015 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 215 | 4.04 | 0.019 | 0.016 | 0.207 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 215 | 2.12 | 0.010 | 0.008 | 0.049 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 212 | 3.38 | 0.016 | 0.016 | 0.094 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 235 | 336.96 | 1.434 | 1.573 | 7.650 | 12.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 198 | 402.83 | 2.035 | 1.881 | 4.928 | 14.3% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 3 | 8.79 | 2.930 | 2.125 | 4.925 | 0.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 3 | 14.87 | 4.955 | 6.250 | 9.925 | 0.5% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 12 | 38.09 | 3.175 | 3.750 | 9.800 | 1.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 15 | 47.12 | 3.141 | 2.350 | 9.850 | 1.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 3 | 0.01 | 0.003 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 8.66 | 2.887 | 2.250 | 4.297 | 0.3% | — |

#### 工作负载 `d9_cold_small_write_fsync` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `8.809 s` (`8809.0 ms`) vs FUSE 累计活跃 `815.80 ms`（等效并发度 `0.09x`）；占比基准：`FUSE 活跃总耗时` (`815.80 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.51%` / Readlink `N/A` · WriteBehind 聚批 `2.34 MiB` / 直通 `0.00 MiB` (Flush RPC `600`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 6,676 | 815.80 | 0.122 | 0.002 | 0.997 | 100.0% | 错误返回 1810 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 600 | 490.15 | 0.817 | 0.782 | 1.856 | 60.1% | 占 FUSE 60.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 600 | 199.85 | 0.333 | 0.370 | 0.832 | 24.5% | 占 FUSE 24.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 400 | 110.22 | 0.276 | 0.211 | 0.504 | 13.5% | 占 FUSE 13.5% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,211 | 769.57 | 0.348 | 0.341 | 0.991 | 94.3% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,211 | 496.81 | 0.225 | 0.217 | 0.732 | 60.9% | Create:0.241ms, Write:0.240ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,211 | 366.85 | 0.166 | 0.189 | 0.499 | 45.0% | HTTP Body+Codec 差值均值 0.043 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,211 | 272.76 | 0.123 | 0.124 | 0.495 | 33.4% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 600 | 139.14 | 0.232 | 0.201 | 0.495 | 17.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 600 | 68.08 | 0.113 | 0.151 | 0.249 | 8.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,209 | 2.78 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 2 | 0.01 | 0.006 | 0.008 | 0.010 | 0.0% | lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 606 | 14.60 | 0.024 | 0.021 | 0.050 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,209 | 22.48 | 0.010 | 0.008 | 0.043 | 2.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,209 | 5.18 | 0.002 | 0.003 | 0.009 | 0.6% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,609 | 11.61 | 0.007 | 0.007 | 0.023 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 600 | 8.16 | 0.014 | 0.017 | 0.025 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 600 | 75.23 | 0.125 | 0.173 | 0.250 | 9.2% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 600 | 75.68 | 0.126 | 0.172 | 0.249 | 9.3% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 4 | 0.58 | 0.145 | 0.025 | 0.490 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 600 | 72.32 | 0.121 | 0.166 | 0.249 | 8.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 600 | 62.93 | 0.105 | 0.091 | 0.220 | 7.7% | — |

#### 工作负载 `d9_hot` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `13.701 s` (`13701.0 ms`) vs FUSE 累计活跃 `8640.22 ms`（等效并发度 `0.63x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`8640.22 ms` / RPC `12231.66 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `98.84%` / Readlink `N/A` · WriteBehind 聚批 `2352.56 MiB` / 直通 `18.00 MiB` (Flush RPC `6458`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 126,631 | 8640.22 | 0.068 | 0.002 | 0.510 | 100.0% | 错误返回 29562 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,922 | 1735.50 | 0.293 | 0.333 | 0.511 | 20.1% | 占 FUSE 20.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 8,290 | 1703.04 | 0.205 | 0.245 | 0.915 | 19.7% | 占 FUSE 19.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,899 | 1398.44 | 0.237 | 0.207 | 0.505 | 16.2% | 占 FUSE 16.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 53 | 60.62 | 1.144 | 0.614 | 8.675 | 0.7% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 6 | 18.93 | 3.156 | 2.500 | 9.850 | 0.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 29,158 | 12063.64 | 0.414 | 0.261 | 10.843 | 98.6% | 非 OK 状态 74 次 · 异步并发流水线 (累计为 FUSE 活跃 1.42x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 29,158 | 8384.48 | 0.288 | 0.202 | 5.877 | 68.5% | Write:0.615ms, Create:0.212ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 29,158 | 6239.74 | 0.214 | 0.094 | 9.413 | 51.0% | HTTP Body+Codec 差值均值 0.088 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 29,158 | 3679.16 | 0.126 | 0.059 | 6.910 | 30.1% | 非 OK 状态 74 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 6,512 | 2357.94 | 0.362 | 0.082 | 4.966 | 19.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Create` | 5,922 | 357.95 | 0.060 | 0.063 | 0.192 | 2.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 26,797 | 38.94 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 1 | 1.92 | 1.919 | 1.750 | 2.485 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 2,367 | 36.51 | 0.015 | 0.015 | 0.050 | 0.3% | readdirplus(1594), read(569), lookup(143) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 10,654 | 121.31 | 0.011 | 0.012 | 0.047 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,594 | 14.89 | 0.009 | 0.008 | 0.045 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 26,797 | 307.69 | 0.011 | 0.010 | 0.046 | 2.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 26,797 | 65.00 | 0.002 | 0.003 | 0.009 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 26,179 | 185.34 | 0.007 | 0.007 | 0.024 | 1.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 6,563 | 1294.86 | 0.197 | 0.008 | 3.544 | 10.6% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 3 | 0.03 | 0.008 | 0.008 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 618 | 131.41 | 0.213 | 0.175 | 5.586 | 1.1% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 618 | 145.47 | 0.235 | 0.176 | 6.567 | 1.2% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 37 | 203.62 | 5.503 | 7.109 | 9.942 | 1.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 655 | 409.45 | 0.625 | 0.174 | 9.636 | 3.3% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 18 | 0.11 | 0.006 | 0.003 | 0.091 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 618 | 119.17 | 0.193 | 0.092 | 5.138 | 1.0% | — |

#### 工作负载 `fuse_perf_baseline` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `12.761 s` (`12761.0 ms`) vs FUSE 累计活跃 `156.82 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`156.82 ms` / RPC `171.87 ms`)
- **缓存与流水线命中**：NSCache Attr `99.91%` / Entry `94.84%` / Readlink `N/A` · WriteBehind 聚批 `15.13 MiB` / 直通 `2.03 MiB` (Flush RPC `90`) · ReadAhead 预读 `0.38 MiB` / 命中 `0.20 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 2,709 | 156.82 | 0.058 | 0.002 | 0.933 | 100.0% | 错误返回 791 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 83 | 71.72 | 0.864 | 0.790 | 8.192 | 45.7% | 占 FUSE 45.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 73 | 22.31 | 0.306 | 0.370 | 0.512 | 14.2% | 占 FUSE 14.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 119 | 18.28 | 0.154 | 0.158 | 0.512 | 11.7% | 占 FUSE 11.7% |
| `2. 客户端等待` | `Client InodeLock 等待` | 1 | 0.31 | 0.311 | 0.375 | 0.497 | 0.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 428 | 169.28 | 0.396 | 0.317 | 17.575 | 98.5% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 1.10x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 428 | 115.04 | 0.269 | 0.225 | 15.570 | 66.9% | Write:0.483ms, Fsync:0.200ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 428 | 89.21 | 0.208 | 0.146 | 7.525 | 51.9% | HTTP Body+Codec 差值均值 0.081 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 428 | 54.24 | 0.127 | 0.092 | 2.925 | 31.6% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 83 | 22.09 | 0.266 | 0.192 | 2.925 | 12.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 99 | 18.23 | 0.184 | 0.102 | 2.005 | 10.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 356 | 0.49 | 0.001 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 72 | 1.22 | 0.017 | 0.017 | 0.066 | 0.7% | read(68), lookup(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 243 | 2.27 | 0.009 | 0.004 | 0.048 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1 | 0.02 | 0.017 | 0.018 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 356 | 4.08 | 0.011 | 0.008 | 0.048 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 356 | 0.99 | 0.003 | 0.003 | 0.015 | 0.6% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 266 | 2.45 | 0.009 | 0.008 | 0.028 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 93 | 8.50 | 0.091 | 0.017 | 1.802 | 4.9% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 8 | 0.04 | 0.005 | 0.004 | 0.025 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 82 | 13.79 | 0.168 | 0.173 | 2.950 | 8.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 83 | 13.60 | 0.164 | 0.112 | 2.925 | 7.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 83 | 16.44 | 0.198 | 0.091 | 3.963 | 9.6% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 82 | 12.08 | 0.147 | 0.087 | 3.818 | 7.0% | — |

#### 工作负载 `jfs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `6.453 s` (`6453.0 ms`) vs FUSE 累计活跃 `11752.34 ms`（等效并发度 `1.82x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`11752.34 ms` / RPC `48890.47 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4138.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1424`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4094.50 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 48,325 | 11752.34 | 0.243 | 0.015 | 6.246 | 100.0% | 错误返回 4909 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,499 | 5732.53 | 1.274 | 0.108 | 15.746 | 48.8% | 占 FUSE 48.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 5334.83 | 0.161 | 0.015 | 3.253 | 45.4% | 占 FUSE 45.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 810 | 385.25 | 0.476 | 0.064 | 1.733 | 3.3% | 占 FUSE 3.3% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 823 | 5143.46 | 6.250 | 5.668 | 24.326 | 43.8% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 1.53 | 1.533 | 1.750 | 2.485 | 0.0% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 16 | 594.26 | 37.141 | 35.714 | 98.000 | 5.1% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,675 | 48809.22 | 13.281 | 15.811 | 49.766 | 99.8% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 4.16x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,675 | 18991.94 | 5.168 | 4.018 | 39.919 | 38.8% | Read:8.001ms, Write:5.132ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,675 | 38284.02 | 10.417 | 13.645 | 49.655 | 78.3% | HTTP Body+Codec 差值均值 2.303 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,675 | 29817.28 | 8.114 | 11.792 | 49.591 | 61.0% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 28961.63 | 20.225 | 29.566 | 49.591 | 59.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,432 | 800.28 | 0.559 | 0.664 | 2.257 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 28.86 | 0.013 | 0.003 | 0.023 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 2 | 9.97 | 4.986 | 2.500 | 9.900 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,434 | 759.11 | 0.529 | 0.659 | 2.181 | 1.6% | read(1432), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,237 | 17.43 | 0.008 | 0.006 | 0.044 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,024 | 16.35 | 0.016 | 0.017 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,237 | 34.51 | 0.015 | 0.013 | 0.072 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,237 | 19.11 | 0.009 | 0.005 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,237 | 53.18 | 0.024 | 0.010 | 0.048 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,961 | 688.95 | 0.351 | 0.386 | 0.969 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,432 | 2311.81 | 1.614 | 1.643 | 4.907 | 4.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 19.68 | 0.308 | 0.003 | 6.800 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 11.08 | 3.692 | 3.350 | 7.929 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `99.91%` / Entry `99.01%` / Readlink `N/A`（负缓存命中 `27,876`）；WriteBehind 聚批 `8858.19 MiB` / 直通 `46.04 MiB`（Flush RPC `14,427`，背压等待 `5248.57 ms`，屏障等待 `31.18 ms`）；ReadAhead 预读 `4864.38 MiB`（命中 `4861.57 MiB`，等待 `709.08 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `Read` | 44,105 | 7759.53 | 25.0% | 175.9 | 17.9 | 782.1 | 2851.4 |
| `Writev` | 29,154 | 6428.50 | 20.7% | 220.5 | 6.4 | 215.4 | 7701.6 |
| `Flush` | 16,477 | 3731.23 | 12.0% | 226.4 | 241.7 | 687.0 | 969.5 |
| `Create` | 12,315 | 3606.57 | 11.6% | 292.9 | 319.9 | 496.1 | 511.8 |
| `Unlink` | 12,272 | 2952.92 | 9.5% | 240.6 | 210.1 | 460.0 | 507.9 |
| `Rmdir` | 6,254 | 1634.02 | 5.3% | 261.3 | 229.8 | 487.9 | 733.0 |
| `Mkdir` | 6,254 | 1551.60 | 5.0% | 248.1 | 224.5 | 479.8 | 511.0 |
| `Fsync` | 1,319 | 1325.89 | 4.3% | 1005.2 | 787.8 | 1492.1 | 10601.4 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Write` | 14,552 | 42022.44 | 2.888 | 0.138 | 31.196 | 46.260 |
| `http_outer:Read` | 6,355 | 6767.41 | 1.065 | 0.299 | 4.849 | 9.366 |
| `http_outer:Create` | 12,315 | 1153.14 | 0.094 | 0.088 | 0.229 | 0.247 |
| `http_outer:Unlink` | 12,272 | 884.76 | 0.072 | 0.071 | 0.181 | 0.239 |
| `rpc:Write` | 14,552 (14552/0) | 33861.69 | 2.327 | 0.086 | 29.235 | 45.847 |
| `rpc:Read` | 6,355 (6355/0) | 1762.32 | 0.277 | 0.154 | 0.968 | 2.283 |
| `rpc:Create` | 12,315 (12315/0) | 731.66 | 0.059 | 0.061 | 0.098 | 0.194 |
| `rpc:Unlink` | 12,272 (12272/0) | 595.87 | 0.049 | 0.044 | 0.096 | 0.198 |
| `rpc:Fsync` | 1,319 (1319/0) | 425.23 | 0.322 | 0.202 | 0.471 | 6.121 |
| `rpc:Rmdir` | 6,254 (6254/0) | 308.57 | 0.049 | 0.046 | 0.096 | 0.177 |
| `actor_phase:phase_a_validate` | 56,171 | 636.75 | 0.011 | 0.010 | 0.025 | 0.046 |
| `actor_phase:wal_apply_nosync` | 54,844 | 403.97 | 0.007 | 0.006 | 0.020 | 0.024 |
| `actor_phase:pack_sync_barrier` | 1,319 | 285.38 | 0.216 | 0.172 | 0.246 | 5.878 |
| `actor_phase:dead_pack_remove` | 138 | 261.63 | 1.896 | 0.156 | 9.014 | 9.803 |
| `actor_phase:wal_apply_sync` | 1,319 | 257.82 | 0.195 | 0.175 | 0.246 | 3.836 |
| `actor_phase:phase_b_mutate` | 56,171 | 147.96 | 0.003 | 0.003 | 0.006 | 0.010 |
| `read_op:read` | 6,355 | 1648.90 | 0.259 | 0.152 | 0.959 | 2.251 |
| `read_op:readdirplus` | 3,169 | 48.42 | 0.015 | 0.014 | 0.038 | 0.065 |
| `read_op:lookup` | 302 | 3.84 | 0.013 | 0.011 | 0.034 | 0.048 |
| `read_op:getattr` | 99 | 0.50 | 0.005 | 0.004 | 0.015 | 0.025 |
| `kv_read:point_get` | 30,307 | 275.10 | 0.009 | 0.005 | 0.028 | 0.046 |
| `kv_read:prefix_scan` | 5,115 | 56.18 | 0.011 | 0.009 | 0.024 | 0.046 |
| `blob_pread` | 5,174 | 1458.78 | 0.282 | 0.233 | 0.805 | 2.222 |
| `blob_pwrite` | 14,612 | 4954.70 | 0.339 | 0.009 | 2.338 | 4.366 |

Pebble BlockCache 命中率 `99.56%` · ReadAmp `1.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `9b259425ed77021e` | 75.67 | `bowl-client:read_ahead.Prefetch` | 74.61 | 1.52 | 73.10 | `bowl-client:storage.v1.StorageService/Read` (74.61 ms) |
| `b7cb402ca6eea901` | 73.50 | `bowl-client:read_ahead.Prefetch` | 73.42 | 0.94 | 72.48 | `bowl-client:storage.v1.StorageService/Read` (73.42 ms) |
| `9e621cc28464a33c` | 54.83 | `bowl-client:write_behind.FlushChunk` | 54.79 | 1.85 | 52.94 | `bowl-client:storage.v1.StorageService/Write` (54.79 ms) |
| `48086ab8941327db` | 45.05 | `bowl-client:write_behind.FlushChunk` | 45.03 | 37.56 | 7.48 | `bowl-client:storage.v1.StorageService/Write` (45.03 ms) |
| `29170b2a6f4330b8` | 40.47 | `bowl-client:write_behind.FlushChunk` | 40.44 | 29.88 | 10.56 | `bowl-client:storage.v1.StorageService/Write` (40.44 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `macro_go_build_cpu.pb.gz` | `100ms 14.93% 14.93%      100ms 14.93%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      250ms 37.31%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_macro_go_build_cpu.txt`](results/telemetry/pprof_top/server_macro_go_build_cpu.txt) |
| `server` | `macro_go_build_heap.pb.gz` | `33.50MB 26.48% 26.48%    33.50MB 26.48%  bytes.growSlice` | `0     0%     0%    87.80MB 69.40%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_macro_go_build_heap.txt`](results/telemetry/pprof_top/server_macro_go_build_heap.txt) |
| `server` | `macro_go_build_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_macro_go_build_mutex.txt`](results/telemetry/pprof_top/server_macro_go_build_mutex.txt) |
| `server` | `seq_read_cpu.pb.gz` | `1.06s 65.84% 65.84%      1.06s 65.84%  internal/runtime/syscall/linux.Syscall6` | `1060ms 65.84% 65.84%     1060ms 65.84%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_seq_read_cpu.txt`](results/telemetry/pprof_top/server_seq_read_cpu.txt) |
| `server` | `seq_read_heap.pb.gz` | `72MB 42.53% 42.53%       72MB 42.53%  bytes.growSlice` | `0     0%     0%   141.16MB 83.38%  connectrpc.com/connect.(*Handler).ServeHTTP` | [`pprof_top/server_seq_read_heap.txt`](results/telemetry/pprof_top/server_seq_read_heap.txt) |
| `server` | `seq_read_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_seq_read_mutex.txt`](results/telemetry/pprof_top/server_seq_read_mutex.txt) |
| `server` | `small_write_fsync_cpu.pb.gz` | `110ms 15.94% 15.94%      110ms 15.94%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      300ms 43.48%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_small_write_fsync_cpu.txt`](results/telemetry/pprof_top/server_small_write_fsync_cpu.txt) |
| `server` | `small_write_fsync_heap.pb.gz` | `68.02MB 27.42% 27.42%    68.02MB 27.42%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%   215.87MB 87.01%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_small_write_fsync_heap.txt`](results/telemetry/pprof_top/server_small_write_fsync_heap.txt) |
| `server` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_small_write_fsync_mutex.txt`](results/telemetry/pprof_top/server_small_write_fsync_mutex.txt) |
| `server` | `sqlite_wal_cpu.pb.gz` | `10ms 50.00% 50.00%       10ms 50.00%  compress/flate.matchLen (inline)` | `0     0%     0%       10ms 50.00%  bufio.(*Writer).Flush` | [`pprof_top/server_sqlite_wal_cpu.txt`](results/telemetry/pprof_top/server_sqlite_wal_cpu.txt) |
| `server` | `sqlite_wal_heap.pb.gz` | `32.01MB 23.39% 23.39%    32.01MB 23.39%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    83.74MB 61.18%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_sqlite_wal_heap.txt`](results/telemetry/pprof_top/server_sqlite_wal_heap.txt) |
| `server` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_sqlite_wal_mutex.txt`](results/telemetry/pprof_top/server_sqlite_wal_mutex.txt) |
| `client` | `macro_go_build_cpu.pb.gz` | `170ms 26.98% 26.98%      170ms 26.98%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      400ms 63.49%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_macro_go_build_cpu.txt`](results/telemetry/pprof_top/client_macro_go_build_cpu.txt) |
| `client` | `macro_go_build_heap.pb.gz` | `64.02MB 49.42% 49.42%    64.02MB 49.42%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   102.35MB 79.00%  sync.(*Pool).Get` | [`pprof_top/client_macro_go_build_heap.txt`](results/telemetry/pprof_top/client_macro_go_build_heap.txt) |
| `client` | `macro_go_build_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_macro_go_build_mutex.txt`](results/telemetry/pprof_top/client_macro_go_build_mutex.txt) |
| `client` | `seq_read_cpu.pb.gz` | `0.62s 39.49% 39.49%      0.62s 39.49%  runtime.memmove` | `0     0%     0%      0.63s 40.13%  syscall.Syscall` | [`pprof_top/client_seq_read_cpu.txt`](results/telemetry/pprof_top/client_seq_read_cpu.txt) |
| `client` | `seq_read_heap.pb.gz` | `80.03MB 30.56% 30.56%    80.03MB 30.56%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   146.51MB 55.95%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_seq_read_heap.txt`](results/telemetry/pprof_top/client_seq_read_heap.txt) |
| `client` | `seq_read_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_seq_read_mutex.txt`](results/telemetry/pprof_top/client_seq_read_mutex.txt) |
| `client` | `small_write_fsync_cpu.pb.gz` | `20ms  8.00%  8.00%       20ms  8.00%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      120ms 48.00%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_small_write_fsync_cpu.txt`](results/telemetry/pprof_top/client_small_write_fsync_cpu.txt) |
| `client` | `small_write_fsync_heap.pb.gz` | `128MB 44.12% 44.12%      128MB 44.12%  bytes.growSlice` | `0     0%     0%   225.04MB 77.58%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_small_write_fsync_heap.txt`](results/telemetry/pprof_top/client_small_write_fsync_heap.txt) |
| `client` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_small_write_fsync_mutex.txt`](results/telemetry/pprof_top/client_small_write_fsync_mutex.txt) |
| `client` | `sqlite_wal_cpu.pb.gz` | `10ms 50.00% 50.00%       10ms 50.00%  compress/flate.matchLen (inline)` | `0     0%     0%       20ms   100%  bufio.(*Writer).Flush` | [`pprof_top/client_sqlite_wal_cpu.txt`](results/telemetry/pprof_top/client_sqlite_wal_cpu.txt) |
| `client` | `sqlite_wal_heap.pb.gz` | `64.02MB 49.25% 49.25%    64.02MB 49.25%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   101.85MB 78.35%  sync.(*Pool).Get` | [`pprof_top/client_sqlite_wal_heap.txt`](results/telemetry/pprof_top/client_sqlite_wal_heap.txt) |
| `client` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_sqlite_wal_mutex.txt`](results/telemetry/pprof_top/client_sqlite_wal_mutex.txt) |

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9bench-kp-20261112-050638`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-<redacted-server>`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-<redacted-client>`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-12T05:06:59Z` |
| **实例销毁时间 (UTC)** | `2026-11-12T05:32:21Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
