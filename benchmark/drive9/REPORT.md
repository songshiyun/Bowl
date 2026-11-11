# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 **`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 **Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。

三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：

1. **Bowl (`bowl`)**：PostgreSQL 18（deploy/prod 容器栈）仅保存单张 `disks` 控制面表（ADR-0032）；每个 `Disk` 由单个常驻 `StoreActor`（单写者串行执行器 + Pebble 引擎，ADR-0030/0031）承载，保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef`（`PackID + Offset + Length`）索引；文件数据追加写入 `packs/<pack_id_hex16>.pack` 的 64 MiB Append-Only Pack 文件中（`256 KiB` 逻辑块网格、`AppendBlocks` 整块零冗余拷贝、`WriteRequest` 池化零分配解码与尾块热缓存），常规写批次以 `pebble.NoSync` 提交异步 WAL，显式 `Fsync`/`Sync` 触发 Pack `fdatasync` + Pebble WAL `LogData(Sync)` 双层并行落盘屏障。客户端挂载时执行单次 `MountDisk` 握手获取 `WriterEpoch`（无后台心跳），禁用内核 `CAP_WRITEBACK_CACHE`，配备 `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`2 MiB` 同步探测 + `<64 KiB` 直通）+ 双端 `8M/32M` H2C 窗口 + `FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存 + 写后 `AttrTimeout=1.0s` + `MaxBackground=64`。
2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3（全部落于 Server 本地 NVMe SSD `/mnt/nvme`），客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 NVMe SSD Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地 NVMe 磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。
3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。

### 1.1 核心指标同机实测总览表

| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,649.21 MiB/s` | `1,332.24 MiB/s` | `606.09 MiB/s` | **1.24x** | **2.72x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,383.14 MiB/s` | `2,173.41 MiB/s` | `1,324.41 MiB/s` | **0.64x** | **1.04x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `1,738.90 files/s` | `2,309.40 files/s` | `62.60 files/s` | **0.75x** | **27.78x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `3,789.00 files/s` | `2,418.80 files/s` | `50.20 files/s` | **1.57x** | **75.48x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `35,967.10 files/s` | `59,838.00 files/s` | `131.80 files/s` | **0.60x** | **272.89x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `1,207.11 MiB/s` | `656.76 MiB/s` | `334.31 MiB/s` | **1.84x** | **3.61x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `1,073.56 MiB/s` | `535.32 MiB/s` | `51.43 MiB/s` | **2.01x** | **20.88x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `754.90 ops/s` | `2,042.15 ops/s` | `124.22 ops/s` | **0.37x** | **6.08x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `3,146.57 ops/s` | `213.54 ops/s` | `207.16 ops/s` | **14.74x** | **15.19x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `3,000.25 ops/s` | `84.21 ops/s` | `51.10 ops/s` | **35.63x** | **58.71x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `3,084.79 ops/s` | `92.17 ops/s` | `34.99 ops/s` | **33.47x** | **88.15x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `0.76 s` | `1.98 s` | `6.97 s` | **2.60x** | **9.16x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `4,880.32 rows/s` | `4,656.93 rows/s` | `7,899.33 rows/s` | **1.05x** | **0.62x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 + 本地数据盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj95goqmagl76j2rt5r4` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.195` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj95goqmagl76j2rt5r5` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30Gi | `cloud_essd` (40 GiB) + `local_ssd_pro (NVMe)` (894 GiB NVMe) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.194` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.042/0.084/0.289/0.058 ms` | `ping -c 20 -i 0.05 172.16.1.195` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.195 -t 5` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序写基线** | `1998.05` MiB/s | `/mnt/nvme` `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点本地 NVMe SSD (`/dev/nvme1n1`) 顺序读基线** | `3494.88` MiB/s | `/mnt/nvme` `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 节点本地 NVMe SSD 4K 随机写 / 随机读 IOPS** | `69959` / `699768` IOPS | `/mnt/nvme` `fio --rw=randwrite/randread --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 节点本地 NVMe SSD 4K 同步写 (`fsync=1`)** | `50472` IOPS (P99 `63.1` µs) | `/mnt/nvme` `fio --rw=write --bs=4k --iodepth=1 --direct=1 --fsync=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `8afc63c` | Drive9 `b529a36f` | Drive9 `b529a36f` |
| **控制面 / 文件元数据** | postgres (PostgreSQL) 18.6 控制面 / 每盘单写者 `StoreActor` (Pebble) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) | TiDB `v8.5.5` Unistore (`127.0.0.1:4000`) |
| **部署编排与可观测性栈** | `deploy/prod (docker-compose.yaml + storage.env)` · `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` | systemd (`tidb` + `drive9-server`) | systemd (`tidb` + `drive9-server`) |
| **服务端数据块引擎** | 每盘 `StoreActor` Pebble + `packs/<pack_id_hex16>.pack` 64 MiB Pack 文件 (`/var/lib/bowl/data` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) |
| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`，单次 `MountDisk` 握手) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |
| **写入持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`writeback_cache=false`）+ 双端 `8M/32M` H2C 窗口 + `WriteRequest` 池化零分配解码；Group Commit `pebble.NoSync` + Fsync 双层并行落盘屏障 | 客户端本地 NVMe SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |
| **客户端读缓存与预读** | 无本地磁盘缓存；文件级顺序预读窗口（Sequential Read-Ahead） | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 |
| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |

---

## 3. 套件一：基础并发测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `1649.21 MiB/s (2.48 s/file)` | `1332.24 MiB/s (3.07 s/file)` | `606.09 MiB/s (6.76 s/file)` | **1.24x** | **2.72x** |
| **Read big file** | `1383.14 MiB/s (2.96 s/file)` | `2173.41 MiB/s (1.88 s/file)` | `1324.41 MiB/s (3.09 s/file)` | **0.64x** | **1.04x** |
| **Write small file** | `1738.90 files/s (2.30 ms/file)` | `2309.40 files/s (1.73 ms/file)` | `62.60 files/s (63.94 ms/file)` | **0.75x** | **27.78x** |
| **Read small file** | `3789.00 files/s (1.06 ms/file)` | `2418.80 files/s (1.65 ms/file)` | `50.20 files/s (79.68 ms/file)` | **1.57x** | **75.48x** |
| **Stat file** | `35967.10 files/s (0.11 ms/file)` | `59838.00 files/s (0.07 ms/file)` | `131.80 files/s (30.36 ms/file)` | **0.60x** | **272.89x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `1,207.11 (1,226.85) MiB/s` | `656.76 (664.94) MiB/s` | `334.31 (335.94) MiB/s` | **1.84x** | **3.61x** |
| `sequential_read` | **顺序读 (256 MiB)** | `1,073.56 (1,113.04) MiB/s` | `535.32 (562.33) MiB/s` | `51.43 (51.60) MiB/s` | **2.01x** | **20.88x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.74 (6.85) MiB/s` | `4.69 (4.84) MiB/s` | `5.19 (5.22) MiB/s` | **1.44x** | **1.30x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `754.90 (770.71) ops/s` | `2,042.15 (2,115.40) ops/s` | `124.22 (127.14) ops/s` | **0.37x** | **6.08x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `3,146.57 (3,804.52) ops/s` | `213.54 (215.77) ops/s` | `207.16 (208.50) ops/s` | **14.74x** | **15.19x** |
| `rename` | **目录重命名 rename (500 个)** | `3,000.25 (3,194.01) ops/s` | `84.21 (85.93) ops/s` | `51.10 (51.61) ops/s` | **35.63x** | **58.71x** |
| `unlink` | **文件删除 unlink (500 个)** | `3,084.79 (3,197.46) ops/s` | `92.17 (93.33) ops/s` | `34.99 (35.02) ops/s` | **33.47x** | **88.15x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `18,188.00 (18,941.44) ops/s` | `10,587.16 (11,001.84) ops/s` | `27,488.46 (28,691.91) ops/s` | **1.72x** | **0.66x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `139,715.70 (148,477.07) ops/s` | `60,239.97 (61,292.15) ops/s` | `21,651.27 (21,808.99) ops/s` | **2.32x** | **6.45x** |
| `stat` | **文件属性查询 stat (500 个)** | `51,930.62 (52,140.93) ops/s` | `12,938.99 (14,234.23) ops/s` | `73.26 (73.62) ops/s` | **4.01x** | **708.90x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.760s (0.784s)` | `1.976s (1.993s)` | `6.967s (6.997s)` | **2.60x** | **9.16x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.112s (0.166s)` | `1.751s (1.817s)` | `0.298s (0.303s)` | **15.59x** | **2.65x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.100s (0.106s)` | `0.828s (0.848s)` | `0.717s (0.729s)` | **8.30x** | **7.19x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.014s (0.026s)` | `0.011s (0.024s)` | `0.070s (0.071s)` | **0.75x** | **4.89x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.332s (1.781s)` | `0.418s (0.429s)` | `0.308s (0.313s)` | **1.26x** | **0.93x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `1,089.68 (1,112.06) MiB/s` | `691.49 (696.82) MiB/s` | `336.88 (344.18) MiB/s` | **1.58x** | **3.23x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,001.87 (7,313.84) MiB/s` | `982.26 (1,823.57) MiB/s` | `597.58 (604.10) MiB/s` | **7.13x** | **11.72x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `443.14 (463.91) MiB/s` | `157.59 (168.06) MiB/s` | `5.30 (5.37) MiB/s` | **2.81x** | **83.57x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `756.19 (813.58) ops/s` | `2,030.99 (2,313.26) ops/s` | `126.76 (127.72) ops/s` | **0.37x** | **5.97x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `3,612.06 (3,686.85) ops/s` | `205.72 (206.44) ops/s` | `206.92 (209.61) ops/s` | **17.56x** | **17.46x** |
| `rename` | **目录重命名 rename (500 个)** | `3,554.26 (3,743.39) ops/s` | `81.92 (82.68) ops/s` | `49.73 (49.84) ops/s` | **43.39x** | **71.47x** |
| `unlink` | **文件删除 unlink (500 个)** | `3,961.39 (3,999.21) ops/s` | `87.12 (87.25) ops/s` | `35.56 (35.64) ops/s` | **45.47x** | **111.40x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `344,109.70 (462,421.91) ops/s` | `112,799.98 (139,537.20) ops/s` | `212,305.21 (222,437.76) ops/s` | **3.05x** | **1.62x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `1,063,485.85 (1,082,362.50) ops/s` | `74,870.10 (83,363.51) ops/s` | `240,325.69 (258,298.61) ops/s` | **14.20x** | **4.43x** |
| `stat` | **文件属性查询 stat (500 个)** | `807,608.32 (811,770.42) ops/s` | `632,160.00 (670,278.98) ops/s` | `525,362.95 (562,545.51) ops/s` | **1.28x** | **1.54x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `0.764s (0.828s)` | `1.980s (1.991s)` | `7.018s (7.053s)` | **2.59x** | **9.18x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.021s (0.042s)` | `0.224s (0.242s)` | `0.041s (0.064s)` | **10.86x** | **2.00x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.021s (0.037s)` | `0.013s (0.014s)` | `0.013s (0.013s)` | **0.61x** | **0.63x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.003s (0.003s)` | `0.004s (0.005s)` | `0.004s (0.017s)` | **1.26x** | **1.17x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.060s (0.076s)` | `0.117s (0.132s)` | `0.119s (0.123s)` | **1.95x** | **1.98x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `640.93 files/s` | `1,797.49 files/s` | `137.21 files/s` | **0.36x** | **4.67x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `1,506.55 files/s` | `88.04 files/s` | `305.34 files/s` | **17.11x** | **4.93x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `62.93 MiB/s` | `61.30 MiB/s` | `49.73 MiB/s` | **1.03x** | **1.27x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `1,240.23 MiB/s` | `630.44 MiB/s` | `70.54 MiB/s` | **1.97x** | **17.58x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,253.24 MiB/s` | `718.97 MiB/s` | `47.28 MiB/s` | **1.74x** | **26.51x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `9,332.29 rows/s` | `5,648.93 rows/s` | `3,810.42 rows/s` | **1.65x** | **2.45x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `924.95 rows/s` | `942.19 rows/s` | `809.45 rows/s` | **0.98x** | **1.14x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `581,277.86 rows/s` | `631,218.18 rows/s` | `66,388.30 rows/s` | **0.92x** | **8.76x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `4,880.32 rows/s` | `4,656.93 rows/s` | `7,899.33 rows/s` | **1.05x** | **0.62x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `1,757.27 rows/s` | `1,999.00 rows/s` | `2,479.33 rows/s` | **0.88x** | **0.71x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `1,044,706.07 rows/s` | `1,161,076.72 rows/s` | `1,042,099.18 rows/s` | **0.90x** | **1.00x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0202 s` | `0.0098 s` | `0.0381 s` | **0.49x** | **1.88x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `1649.21 MiB/s`，Drive9 Sync `606.09 MiB/s`，吞吐比 `2.72x`。
- `Read big file`：Bowl `1383.14 MiB/s`，Drive9 Sync `1324.41 MiB/s`，吞吐比 `1.04x`。
- `Write small file`：Bowl `1738.90 files/s`，Drive9 Sync `62.60 files/s`，吞吐比 `27.78x`。
- `Read small file`：Bowl `3789.00 files/s`，Drive9 Sync `50.20 files/s`，吞吐比 `75.48x`。
- `Stat file`：Bowl `35967.10 files/s`，Drive9 Sync `131.80 files/s`，吞吐比 `272.89x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1649.21 MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `1207.11 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

冷缓存准备命令同时清理 Client 与 Server 双端内核页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`），且各目标栈在 Server 端按进程隔离启停；但未在每轮内部重启 FUSE 进程或清空 Drive9 用户态内存缓存。因此本报告的 cold 表示双端 OS 页缓存清空下的冷状态。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

ADR-0030/0031/0032 同时引入每盘单写者 `StoreActor`、64 MiB Pack 文件引擎与单次 `MountDisk` 握手；单轮数据反映当前 `labs/actor-architecture` 分支整体表现，本套件未测试整机断电恢复。

### 6.1 `16 MiB` 大文件写（`fuse_perf_baseline.py`）负载生成器 CPU 封顶公平性说明

在套件三 [`fuse_perf_baseline.py`](fuse_perf_baseline.py) 的 `large_file_write`（`16 MiB + fsync + dir fsync`）中，三组被测系统吞吐高度收敛于同一区间：Bowl **`62.93 MiB/s`**（`0.254 s`）、Drive9 Default **`61.30 MiB/s`**（`0.261 s`）、Drive9 Sync **`49.73 MiB/s`**（`0.322 s`），远低于套件二 `drive9_fuse_bench.py` 同机测得的 `256 MiB + fsync` 预生成缓冲顺序写吞吐（Bowl **`1,207.11 MiB/s`**、Drive9 Default `656.76 MiB/s`、Drive9 Sync `334.31 MiB/s`）。

经源码与全链路遥测交叉核实，其物理瓶颈位于**测试脚本客户端单核 Python 负载生成逻辑**而非文件系统数据面：
1. `fuse_perf_baseline.py` 在计时闭包内以纯 Python 循环按 `32 B` SHA-256 摘要逐块拼接生成 `16 MiB` 伪随机字节流，并同步执行增量 SHA-256 计算，单核 Python 解释器仅内存生成与哈希计算即消耗约 `240–250 ms`（理论上限约 `64–66 MiB/s`）。
2. `phase_deltas` 中 `fuse_perf_baseline` 全阶段墙钟 `12,934.0 ms` 内，Bowl FUSE 入口全部 `3,398` 次系统调用累计活跃耗时仅 **`406.66 ms`（仅占墙钟 `3.1%`）**；其中 `16 MiB` 写入由 `WriteBehindCoordinator` 聚批 `14.00 MiB`（`4` 次并发 Flush RPC）+ 直通 `2.00 MiB` 落盘，服务端 `83` 次 `Fsync` 累计耗时仅 `32.74 ms`（P50 `0.253 ms`）。因此该项指标反映三者在单线程 CPU 受限流式生成写入下的端到端基线，纯粹文件系统大块顺序落盘能力应以套件一 `juicefs bench`（`1,649.21 MiB/s`）与套件二 `sequential_write`（`1,207.11 MiB/s`）为准。

---

## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `1399` 条（跨 Client+Server 全链路 `956` 条，异常链路 `104` 条）· Loki 业务日志 `50` 条（服务端 Warn/Error `42` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `d9_cold_io` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `d9_cold_seq_read`, `d9_cold_small_write_fsync`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `21.858 s` (`21858.0 ms`) vs FUSE 累计活跃 `3698.32 ms`（等效并发度 `0.17x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`3698.32 ms` / RPC `9243.53 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `97.54%` / Readlink `N/A` · WriteBehind 聚批 `2286.00 MiB` / 直通 `20.34 MiB` (Flush RPC `576`) · ReadAhead 预读 `840.00 MiB` / 命中 `764.62 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 23,665 | 3698.32 | 0.156 | 0.015 | 0.978 | 100.0% | 错误返回 7078 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 9,114 | 2099.61 | 0.230 | 0.028 | 1.011 | 56.8% | 占 FUSE 56.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 609 | 663.94 | 1.090 | 0.415 | 40.960 | 18.0% | 占 FUSE 17.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 2,904 | 553.83 | 0.191 | 0.115 | 1.555 | 15.0% | 占 FUSE 15.0% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 59 | 76.26 | 1.293 | 0.882 | 7.050 | 2.1% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 9 | 51.06 | 5.674 | 6.786 | 9.936 | 1.4% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 21 | 130.28 | 6.204 | 4.375 | 24.370 | 3.5% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 6,087 | 9243.53 | 1.519 | 0.504 | 33.083 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 6,086 | 9192.26 | 1.510 | 0.498 | 33.083 | 99.4% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 2.50x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 6,086 | 5920.41 | 0.973 | 0.310 | 15.902 | 64.0% | Write:2.529ms, Read:0.788ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 6,086 | 6279.73 | 1.032 | 0.325 | 33.083 | 67.9% | HTTP Body+Codec 差值均值 0.494 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 6,086 | 3271.85 | 0.538 | 0.187 | 33.083 | 35.4% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,194 | 1711.62 | 1.434 | 0.248 | 8.806 | 18.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 3,187 | 1018.45 | 0.320 | 0.197 | 2.969 | 11.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,866 | 5.53 | 0.002 | 0.003 | 0.009 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 111 | 45.73 | 0.412 | 0.160 | 3.613 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3,220 | 966.13 | 0.300 | 0.192 | 2.914 | 10.5% | read(3187), readdirplus(22), lookup(11) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,108 | 29.19 | 0.005 | 0.003 | 0.040 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 961 | 15.91 | 0.017 | 0.018 | 0.048 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,866 | 34.64 | 0.012 | 0.010 | 0.049 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,866 | 180.80 | 0.063 | 0.003 | 2.106 | 2.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,257 | 24.15 | 0.011 | 0.008 | 0.041 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 7,062 | 875.72 | 0.124 | 0.087 | 1.417 | 9.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,194 | 1134.14 | 0.950 | 0.042 | 4.448 | 12.3% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 609 | 118.25 | 0.194 | 0.176 | 1.955 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 609 | 326.26 | 0.536 | 0.176 | 33.083 | 3.5% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 10 | 66.43 | 6.643 | 6.667 | 24.500 | 0.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 645 | 1330.18 | 2.062 | 0.177 | 45.393 | 14.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 11 | 0.05 | 0.005 | 0.003 | 0.023 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 610 | 105.03 | 0.172 | 0.131 | 2.090 | 1.1% | — |

#### 工作负载 `d9_cold_meta_macro` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `27.292 s` (`27292.0 ms`) vs FUSE 累计活跃 `9428.29 ms`（等效并发度 `0.34x`）；占比基准：`FUSE 活跃总耗时` (`9428.29 ms`)
- **缓存与流水线命中**：NSCache Attr `96.67%` / Entry `99.25%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `64.17 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `95.47%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 132,174 | 9428.29 | 0.071 | 0.002 | 0.496 | 100.0% | 错误返回 34084 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 9,076 | 2716.47 | 0.299 | 0.349 | 0.511 | 28.8% | 占 FUSE 28.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,307 | 1487.80 | 0.280 | 0.278 | 0.510 | 15.8% | 占 FUSE 15.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,056 | 1352.61 | 0.268 | 0.228 | 0.568 | 14.3% | 占 FUSE 14.3% |
| `2. 客户端等待` | `Client InodeLock 等待` | 1 | 0.26 | 0.261 | 0.375 | 0.497 | 0.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 33,137 | 8560.65 | 0.258 | 0.245 | 2.365 | 90.8% | 非 OK 状态 47 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 33,137 | 6776.49 | 0.204 | 0.192 | 1.951 | 71.9% | Write:0.213ms, Create:0.207ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 33,137 | 2964.37 | 0.089 | 0.088 | 2.365 | 31.4% | HTTP Body+Codec 差值均值 0.035 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 33,137 | 1784.16 | 0.054 | 0.052 | 2.365 | 18.9% | 非 OK 状态 47 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 9,076 | 647.46 | 0.071 | 0.072 | 0.237 | 6.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Create` | 5,307 | 292.83 | 0.055 | 0.054 | 0.163 | 3.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 27,217 | 34.46 | 0.001 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 5,923 | 91.13 | 0.015 | 0.015 | 0.077 | 1.0% | readdirplus(3158), read(1088), getattr(1539) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 19,465 | 121.45 | 0.006 | 0.004 | 0.040 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 3,158 | 27.76 | 0.009 | 0.006 | 0.047 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 27,217 | 278.74 | 0.010 | 0.008 | 0.046 | 3.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 27,217 | 69.00 | 0.003 | 0.003 | 0.009 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 27,208 | 160.69 | 0.006 | 0.005 | 0.022 | 1.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 9,076 | 96.57 | 0.011 | 0.009 | 0.036 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 4 | 0.03 | 0.007 | 0.007 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 6 | 1.09 | 0.182 | 0.190 | 0.485 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 9 | 2.58 | 0.287 | 0.088 | 2.365 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 0.23 | 0.230 | 0.175 | 0.249 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 7 | 6.31 | 0.901 | 0.138 | 9.650 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 8 | 1.26 | 0.157 | 0.160 | 0.296 | 0.0% | — |

#### 工作负载 `d9_cold_seq_read` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `6.856 s` (`6856.0 ms`) vs FUSE 累计活跃 `777.76 ms`（等效并发度 `0.11x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`777.76 ms` / RPC `3775.17 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `81.82%` / Readlink `N/A` · WriteBehind 聚批 `762.00 MiB` / 直通 `6.00 MiB` (Flush RPC `192`) · ReadAhead 预读 `840.00 MiB` / 命中 `764.62 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 8,555 | 777.76 | 0.091 | 0.017 | 0.482 | 100.0% | 错误返回 1555 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 6,144 | 499.69 | 0.081 | 0.019 | 0.290 | 64.2% | 占 FUSE 64.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 3 | 148.62 | 49.540 | 54.613 | 65.536 | 19.1% | 占 FUSE 19.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 768 | 114.74 | 0.149 | 0.103 | 1.955 | 14.8% | 占 FUSE 14.8% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 25 | 29.15 | 1.166 | 0.875 | 4.792 | 3.7% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 3 | 18.14 | 6.047 | 7.500 | 9.950 | 2.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 21 | 130.28 | 6.204 | 4.375 | 24.370 | 16.8% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 439 | 3775.17 | 8.599 | 8.172 | 49.750 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 438 | 3764.88 | 8.596 | 8.162 | 49.750 | 99.7% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 4.85x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 438 | 2621.05 | 5.984 | 5.907 | 19.625 | 69.4% | Read:7.384ms, Write:5.109ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 438 | 2636.61 | 6.020 | 5.934 | 49.750 | 69.8% | HTTP Body+Codec 差值均值 3.409 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 438 | 1143.82 | 2.611 | 2.256 | 49.750 | 30.3% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 198 | 614.96 | 3.106 | 2.453 | 9.550 | 16.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 217 | 398.79 | 1.838 | 1.781 | 4.923 | 10.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 215 | 0.59 | 0.003 | 0.003 | 0.010 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 51 | 11.58 | 0.227 | 0.153 | 2.118 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 223 | 390.88 | 1.753 | 1.719 | 4.921 | 10.4% | read(217), readdirplus(4), lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 214 | 4.27 | 0.020 | 0.019 | 0.049 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 234 | 1.61 | 0.007 | 0.007 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 215 | 5.23 | 0.024 | 0.016 | 0.164 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 215 | 91.39 | 0.425 | 0.009 | 4.042 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 212 | 3.77 | 0.018 | 0.017 | 0.097 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 3,363 | 372.45 | 0.111 | 0.041 | 1.995 | 9.9% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 198 | 377.31 | 1.906 | 1.785 | 4.847 | 10.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 3 | 22.70 | 7.568 | 13.750 | 24.775 | 0.6% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 3 | 83.68 | 27.895 | 37.500 | 49.750 | 2.2% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 3 | 42.90 | 14.301 | 13.750 | 24.775 | 1.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 15 | 430.23 | 28.682 | 34.375 | 49.688 | 11.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 3 | 0.01 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 22.58 | 7.527 | 9.689 | 10.836 | 0.6% | — |

#### 工作负载 `d9_cold_small_write_fsync` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `8.865 s` (`8865.0 ms`) vs FUSE 累计活跃 `830.70 ms`（等效并发度 `0.09x`）；占比基准：`FUSE 活跃总耗时` (`830.70 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.51%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `2.34 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 7,274 | 830.70 | 0.114 | 0.003 | 0.789 | 100.0% | 错误返回 2410 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 600 | 292.65 | 0.488 | 0.413 | 1.024 | 35.2% | 占 FUSE 35.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 600 | 211.64 | 0.353 | 0.387 | 0.805 | 25.5% | 占 FUSE 25.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 600 | 206.52 | 0.344 | 0.383 | 0.896 | 24.9% | 占 FUSE 24.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,215 | 781.07 | 0.353 | 0.348 | 1.000 | 94.0% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,215 | 505.58 | 0.228 | 0.233 | 0.732 | 60.9% | Create:0.252ms, Write:0.243ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,215 | 376.66 | 0.170 | 0.204 | 0.990 | 45.3% | HTTP Body+Codec 差值均值 0.046 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,215 | 275.49 | 0.124 | 0.116 | 0.499 | 33.2% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 600 | 152.85 | 0.255 | 0.232 | 0.499 | 18.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 600 | 56.41 | 0.094 | 0.086 | 0.246 | 6.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,209 | 3.52 | 0.002 | 0.003 | 0.006 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 6 | 0.50 | 0.083 | 0.070 | 0.247 | 0.1% | readdirplus(4), lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 614 | 13.32 | 0.022 | 0.020 | 0.050 | 1.6% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4 | 0.43 | 0.108 | 0.050 | 0.247 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,209 | 22.14 | 0.010 | 0.008 | 0.043 | 2.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,209 | 5.87 | 0.003 | 0.003 | 0.010 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,609 | 11.59 | 0.007 | 0.007 | 0.024 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 600 | 8.64 | 0.014 | 0.017 | 0.033 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 600 | 79.81 | 0.133 | 0.175 | 0.249 | 9.6% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 600 | 84.36 | 0.141 | 0.175 | 0.249 | 10.2% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 0.48 | 0.482 | 0.375 | 0.497 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 600 | 77.94 | 0.130 | 0.171 | 0.249 | 9.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.007 | 0.005 | 0.025 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 600 | 66.79 | 0.111 | 0.130 | 0.200 | 8.0% | — |

#### 工作负载 `d9_hot` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `15.994 s` (`15994.0 ms`) vs FUSE 累计活跃 `10997.22 ms`（等效并发度 `0.69x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`10997.22 ms` / RPC `14709.05 ms`)
- **缓存与流水线命中**：NSCache Attr `96.79%` / Entry `98.83%` / Readlink `N/A` · WriteBehind 聚批 `2286.00 MiB` / 直通 `84.58 MiB` (Flush RPC `576`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.63%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 137,428 | 10997.22 | 0.080 | 0.003 | 0.499 | 100.0% | 错误返回 41556 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 11,988 | 3289.76 | 0.274 | 0.307 | 0.697 | 29.9% | 占 FUSE 29.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,922 | 1594.52 | 0.269 | 0.250 | 0.509 | 14.5% | 占 FUSE 14.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,898 | 1565.68 | 0.265 | 0.223 | 0.535 | 14.2% | 占 FUSE 14.2% |
| `2. 客户端等待` | `Client InodeLock 等待` | 1 | 0.00 | 0.001 | 0.013 | 0.025 | 0.0% | — |
| `2. 客户端等待` | `WriteBehind 背压等待` | 65 | 69.33 | 1.067 | 0.554 | 8.375 | 0.6% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 9 | 55.79 | 6.199 | 7.188 | 9.944 | 0.5% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 35,960 | 14530.99 | 0.404 | 0.250 | 55.857 | 98.8% | 非 OK 状态 75 次 · 异步并发流水线 (累计为 FUSE 活跃 1.34x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 35,960 | 10137.73 | 0.282 | 0.190 | 7.357 | 68.9% | Write:0.494ms, Create:0.195ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 35,960 | 7314.76 | 0.203 | 0.102 | 48.500 | 49.7% | HTTP Body+Codec 差值均值 0.081 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 35,960 | 4393.27 | 0.122 | 0.060 | 48.500 | 29.9% | 非 OK 状态 75 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 10,278 | 2482.43 | 0.242 | 0.078 | 4.503 | 16.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 618 | 631.88 | 1.022 | 0.208 | 48.500 | 4.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 30,563 | 41.17 | 0.001 | 0.003 | 0.006 | 0.3% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 142 | 61.04 | 0.430 | 0.174 | 2.470 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 5,403 | 82.35 | 0.015 | 0.014 | 0.091 | 0.6% | readdirplus(3220), read(473), getattr(1552) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 19,003 | 147.76 | 0.008 | 0.004 | 0.046 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 3,220 | 38.93 | 0.012 | 0.009 | 0.049 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 30,563 | 357.67 | 0.012 | 0.010 | 0.047 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 30,563 | 294.32 | 0.010 | 0.003 | 0.014 | 2.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 29,945 | 209.23 | 0.007 | 0.006 | 0.024 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 10,314 | 1216.62 | 0.118 | 0.010 | 2.333 | 8.3% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 4 | 0.03 | 0.007 | 0.007 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 615 | 114.13 | 0.186 | 0.176 | 1.855 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 618 | 356.50 | 0.577 | 0.176 | 32.833 | 2.4% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 10 | 210.58 | 21.058 | 16.667 | 24.833 | 1.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 652 | 1339.88 | 2.055 | 0.175 | 45.472 | 9.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 13 | 0.05 | 0.004 | 0.003 | 0.009 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 616 | 101.86 | 0.165 | 0.120 | 1.384 | 0.7% | — |

#### 工作负载 `fuse_perf_baseline` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `12.934 s` (`12934.0 ms`) vs FUSE 累计活跃 `406.66 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`406.66 ms`)
- **缓存与流水线命中**：NSCache Attr `99.91%` / Entry `94.87%` / Readlink `N/A` · WriteBehind 聚批 `14.00 MiB` / 直通 `3.16 MiB` (Flush RPC `4`) · ReadAhead 预读 `0.38 MiB` / 命中 `0.20 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,398 | 406.66 | 0.120 | 0.002 | 0.878 | 100.0% | 错误返回 1480 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 689 | 246.80 | 0.358 | 0.374 | 1.002 | 60.7% | 占 FUSE 60.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 83 | 58.61 | 0.706 | 0.563 | 16.384 | 14.4% | 占 FUSE 14.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 73 | 34.36 | 0.471 | 0.308 | 16.384 | 8.5% | 占 FUSE 8.4% |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 3.79 | 3.795 | 3.750 | 4.975 | 0.9% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,012 | 404.89 | 0.400 | 0.367 | 14.050 | 98.8% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,012 | 278.76 | 0.275 | 0.270 | 13.807 | 68.0% | Write:0.271ms, Unlink:0.386ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,012 | 195.04 | 0.193 | 0.172 | 3.963 | 47.6% | HTTP Body+Codec 差值均值 0.068 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,012 | 126.12 | 0.125 | 0.097 | 3.963 | 30.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 679 | 76.08 | 0.112 | 0.090 | 0.702 | 18.6% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 83 | 32.74 | 0.394 | 0.253 | 3.963 | 8.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 935 | 1.99 | 0.002 | 0.003 | 0.008 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 77 | 1.70 | 0.022 | 0.019 | 0.098 | 0.4% | read(68), readdirplus(5), lookup(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 832 | 6.14 | 0.007 | 0.003 | 0.047 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 6 | 0.17 | 0.028 | 0.025 | 0.097 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 935 | 8.62 | 0.009 | 0.007 | 0.047 | 2.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 935 | 3.81 | 0.004 | 0.003 | 0.014 | 0.9% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 845 | 6.84 | 0.008 | 0.008 | 0.025 | 1.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 672 | 15.81 | 0.024 | 0.009 | 0.285 | 3.9% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 51 | 0.53 | 0.010 | 0.010 | 0.037 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 82 | 15.06 | 0.184 | 0.180 | 1.270 | 3.7% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 83 | 18.60 | 0.224 | 0.140 | 3.963 | 4.5% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 83 | 21.34 | 0.257 | 0.099 | 4.308 | 5.2% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.01 | 0.005 | 0.008 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 82 | 12.99 | 0.158 | 0.143 | 1.718 | 3.2% | — |

#### 工作负载 `jfs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `6.755 s` (`6755.0 ms`) vs FUSE 累计活跃 `16059.59 ms`（等效并发度 `2.38x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`16059.59 ms` / RPC `63552.29 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `58.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4732.00 MiB` / 命中 `4076.25 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 52,853 | 16059.59 | 0.304 | 0.018 | 7.212 | 100.0% | 错误返回 9420 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 9768.94 | 0.294 | 0.022 | 4.007 | 60.8% | 占 FUSE 60.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,503 | 5793.13 | 1.287 | 0.126 | 22.784 | 36.1% | 占 FUSE 36.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 404 | 187.66 | 0.465 | 0.436 | 1.024 | 1.2% | 占 FUSE 1.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 657 | 4856.43 | 7.392 | 5.291 | 42.179 | 30.2% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 92.84 | 23.211 | 25.000 | 49.500 | 0.6% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 181 | 1926.63 | 10.644 | 6.761 | 77.375 | 12.0% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 3,871 | 63552.29 | 16.418 | 17.631 | 94.131 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,869 | 63468.17 | 16.404 | 17.612 | 94.131 | 99.9% | 非 OK 状态 34 次 · 异步并发流水线 (累计为 FUSE 活跃 3.96x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,866 | 52091.89 | 13.474 | 15.816 | 54.896 | 82.0% | Write:18.229ms, Read:15.857ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,867 | 35508.65 | 9.182 | 8.259 | 64.200 | 55.9% | HTTP Body+Codec 差值均值 6.239 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,866 | 11376.28 | 2.943 | 1.797 | 45.738 | 17.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 10279.65 | 7.179 | 4.041 | 45.738 | 16.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,622 | 1029.13 | 0.634 | 0.684 | 2.341 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 1264.98 | 0.564 | 0.003 | 13.525 | 2.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 298 | 1963.39 | 6.589 | 4.198 | 35.100 | 3.1% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,625 | 978.79 | 0.602 | 0.677 | 2.271 | 1.5% | read(1622), readdirplus(1), getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,215 | 19.60 | 0.016 | 0.018 | 0.047 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,429 | 17.89 | 0.007 | 0.007 | 0.029 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,072 | 27.84 | 0.013 | 0.011 | 0.074 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,072 | 820.91 | 0.396 | 0.004 | 8.822 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,072 | 52.24 | 0.025 | 0.009 | 0.049 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 19,428 | 876.57 | 0.045 | 0.042 | 0.100 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,433 | 2068.13 | 1.443 | 1.492 | 4.685 | 3.3% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2173.50 | 33.961 | 37.500 | 49.750 | 3.4% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 20.20 | 6.734 | 8.631 | 12.627 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `97.13%` / Entry `99.01%` / Readlink `N/A`（负缓存命中 `27,878`）；WriteBehind 聚批 `8674.00 MiB` / 直通 `230.25 MiB`（Flush RPC `2,180`，背压等待 `5002.02 ms`，屏障等待 `203.49 ms`）；ReadAhead 预读 `5572.38 MiB`（命中 `4841.07 MiB`，等待 `2056.90 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `Write` | 29,160 | 12599.99 | 30.9% | 432.1 | 296.7 | 511.9 | 6826.7 |
| `Read` | 43,979 | 12435.51 | 30.5% | 282.8 | 24.1 | 797.8 | 3494.1 |
| `Create` | 12,315 | 3508.77 | 8.6% | 284.9 | 280.3 | 495.5 | 726.9 |
| `Unlink` | 12,271 | 3298.88 | 8.1% | 268.8 | 225.9 | 482.8 | 615.3 |
| `ReadDirPlus` | 14,532 | 1779.72 | 4.4% | 122.5 | 6.2 | 452.9 | 508.6 |
| `Rmdir` | 6,254 | 1760.20 | 4.3% | 281.4 | 293.2 | 498.2 | 779.1 |
| `Fsync` | 1,319 | 1553.51 | 3.8% | 1177.8 | 412.4 | 950.9 | 40478.1 |
| `Mkdir` | 6,254 | 1543.89 | 3.8% | 246.9 | 224.0 | 477.3 | 508.5 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Write` | 22,659 | 27971.63 | 1.234 | 0.144 | 8.054 | 25.523 |
| `http_outer:Read` | 6,439 | 18979.09 | 2.948 | 0.342 | 19.590 | 24.971 |
| `http_outer:Create` | 12,315 | 1194.70 | 0.097 | 0.090 | 0.232 | 0.248 |
| `http_outer:Fsync` | 1,319 | 1192.11 | 0.904 | 0.363 | 0.494 | 35.021 |
| `rpc:Write` | 22,659 (22659/0) | 15197.23 | 0.671 | 0.079 | 3.356 | 17.251 |
| `rpc:Read` | 6,438 (6438/0) | 2113.07 | 0.328 | 0.167 | 0.992 | 2.377 |
| `rpc:Fsync` | 1,319 (1319/0) | 1131.09 | 0.858 | 0.221 | 0.485 | 35.021 |
| `rpc:Create` | 12,315 (12315/0) | 738.27 | 0.060 | 0.060 | 0.099 | 0.214 |
| `rpc:Unlink` | 12,271 (12271/0) | 653.79 | 0.053 | 0.046 | 0.121 | 0.229 |
| `rpc:Rmdir` | 6,254 (6254/0) | 377.36 | 0.060 | 0.065 | 0.099 | 0.209 |
| `actor_phase:phase_b_mutate` | 64,112 | 1370.27 | 0.021 | 0.003 | 0.007 | 0.021 |
| `actor_phase:phase_a_validate` | 64,112 | 716.00 | 0.011 | 0.009 | 0.030 | 0.047 |
| `actor_phase:pack_sync_barrier` | 1,319 | 703.95 | 0.534 | 0.175 | 0.246 | 31.681 |
| `actor_phase:wal_apply_nosync` | 62,785 | 456.34 | 0.007 | 0.006 | 0.018 | 0.024 |
| `actor_phase:dead_pack_remove` | 21 | 277.24 | 13.202 | 11.875 | 23.688 | 24.738 |
| `actor_phase:wal_apply_sync` | 1,313 | 248.71 | 0.189 | 0.176 | 0.246 | 1.812 |
| `read_op:read` | 6,438 | 1986.70 | 0.309 | 0.164 | 0.977 | 2.351 |
| `read_op:readdirplus` | 6,429 | 118.53 | 0.018 | 0.016 | 0.044 | 0.092 |
| `read_op:getattr` | 3,094 | 11.88 | 0.004 | 0.003 | 0.010 | 0.021 |
| `read_op:lookup` | 320 | 4.56 | 0.014 | 0.013 | 0.041 | 0.049 |
| `kv_read:point_get` | 47,935 | 322.71 | 0.007 | 0.004 | 0.024 | 0.043 |
| `kv_read:prefix_scan` | 8,583 | 103.40 | 0.012 | 0.010 | 0.024 | 0.048 |
| `blob_pread` | 26,490 | 1752.29 | 0.066 | 0.044 | 0.211 | 0.477 |
| `blob_pwrite` | 22,689 | 4531.27 | 0.200 | 0.011 | 1.778 | 2.468 |

Pebble BlockCache 命中率 `99.27%` · ReadAmp `1.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `375a8c353706c2ac` | 92.81 | `bowl-client:read_ahead.Prefetch` | 83.06 | 0.98 | 82.08 | `bowl-client:storage.v1.StorageService/Read` (83.06 ms) |
| `dfb5b6235019e7ce` | 79.56 | `bowl-client:write_behind.FlushChunk` | 79.51 | 30.70 | 48.81 | `bowl-client:storage.v1.StorageService/Write` (79.51 ms) |
| `9aded1d0acff00ae` | 77.96 | `bowl-client:write_behind.FlushChunk` | 77.90 | 35.26 | 42.65 | `bowl-client:storage.v1.StorageService/Write` (77.90 ms) |
| `84b3532889e074d4` | 74.10 | `bowl-client:write_behind.FlushChunk` | 74.07 | 11.54 | 62.54 | `bowl-client:storage.v1.StorageService/Write` (74.07 ms) |
| `eaf81b4a1b8444f0` | 73.16 | `bowl-client:write_behind.FlushChunk` | 73.09 | 22.22 | 50.87 | `bowl-client:storage.v1.StorageService/Write` (73.09 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `macro_go_build_cpu.pb.gz` | `0.23s 15.65% 15.65%      0.23s 15.65%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      570ms 38.78%  net/http.HandlerFunc.ServeHTTP` | [`pprof_top/server_macro_go_build_cpu.txt`](results/telemetry/pprof_top/server_macro_go_build_cpu.txt) |
| `server` | `macro_go_build_heap.pb.gz` | `33.30MB 25.91% 25.91%    33.30MB 25.91%  github.com/starsrc/bowl/services/storage/internal/store.(*BlobStore).extendHotShard` | `0     0%     0%    88.04MB 68.49%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_macro_go_build_heap.txt`](results/telemetry/pprof_top/server_macro_go_build_heap.txt) |
| `server` | `macro_go_build_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_macro_go_build_mutex.txt`](results/telemetry/pprof_top/server_macro_go_build_mutex.txt) |
| `server` | `seq_read_cpu.pb.gz` | `1.09s 61.24% 61.24%      1.09s 61.24%  internal/runtime/syscall/linux.Syscall6` | `1090ms 61.24% 61.24%     1090ms 61.24%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_seq_read_cpu.txt`](results/telemetry/pprof_top/server_seq_read_cpu.txt) |
| `server` | `seq_read_heap.pb.gz` | `96MB 51.64% 51.64%       96MB 51.64%  bytes.growSlice` | `0     0%     0%   165.05MB 88.79%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_seq_read_heap.txt`](results/telemetry/pprof_top/server_seq_read_heap.txt) |
| `server` | `seq_read_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_seq_read_mutex.txt`](results/telemetry/pprof_top/server_seq_read_mutex.txt) |
| `server` | `small_write_fsync_cpu.pb.gz` | `90ms 13.04% 13.04%       90ms 13.04%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      230ms 33.33%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_small_write_fsync_cpu.txt`](results/telemetry/pprof_top/server_small_write_fsync_cpu.txt) |
| `server` | `small_write_fsync_heap.pb.gz` | `72MB 28.37% 28.37%       72MB 28.37%  bytes.growSlice` | `0     0%     0%   233.65MB 92.07%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_small_write_fsync_heap.txt`](results/telemetry/pprof_top/server_small_write_fsync_heap.txt) |
| `server` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_small_write_fsync_mutex.txt`](results/telemetry/pprof_top/server_small_write_fsync_mutex.txt) |
| `server` | `sqlite_wal_cpu.pb.gz` | `10ms 33.33% 33.33%       10ms 33.33%  github.com/prometheus/client_model/go.(*Gauge).GetValue (inline)` | `0     0%     0%       10ms 33.33%  github.com/cockroachdb/pebble/v2.(*ConcurrencyLimitScheduler).periodicGranter` | [`pprof_top/server_sqlite_wal_cpu.txt`](results/telemetry/pprof_top/server_sqlite_wal_cpu.txt) |
| `server` | `sqlite_wal_heap.pb.gz` | `47.20MB 27.41% 27.41%    47.20MB 27.41%  github.com/cockroachdb/pebble/v2/internal/manual.New (inline)` | `0     0%     0%   115.71MB 67.19%  connectrpc.com/connect.(*Handler).ServeHTTP` | [`pprof_top/server_sqlite_wal_heap.txt`](results/telemetry/pprof_top/server_sqlite_wal_heap.txt) |
| `server` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_sqlite_wal_mutex.txt`](results/telemetry/pprof_top/server_sqlite_wal_mutex.txt) |
| `client` | `macro_go_build_cpu.pb.gz` | `190ms 20.00% 20.00%      190ms 20.00%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      490ms 51.58%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_macro_go_build_cpu.txt`](results/telemetry/pprof_top/client_macro_go_build_cpu.txt) |
| `client` | `macro_go_build_heap.pb.gz` | `64.02MB 54.90% 54.90%    64.02MB 54.90%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   103.01MB 88.34%  sync.(*Pool).Get` | [`pprof_top/client_macro_go_build_heap.txt`](results/telemetry/pprof_top/client_macro_go_build_heap.txt) |
| `client` | `macro_go_build_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_macro_go_build_mutex.txt`](results/telemetry/pprof_top/client_macro_go_build_mutex.txt) |
| `client` | `seq_read_cpu.pb.gz` | `0.73s 40.78% 40.78%      0.73s 40.78%  internal/runtime/syscall/linux.Syscall6` | `730ms 40.78% 40.78%      730ms 40.78%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/client_seq_read_cpu.txt`](results/telemetry/pprof_top/client_seq_read_cpu.txt) |
| `client` | `seq_read_heap.pb.gz` | `95.15MB 32.86% 32.86%    95.15MB 32.86%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   182.04MB 62.86%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_seq_read_heap.txt`](results/telemetry/pprof_top/client_seq_read_heap.txt) |
| `client` | `seq_read_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_seq_read_mutex.txt`](results/telemetry/pprof_top/client_seq_read_mutex.txt) |
| `client` | `small_write_fsync_cpu.pb.gz` | `60ms 20.69% 20.69%       60ms 20.69%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      190ms 65.52%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_small_write_fsync_cpu.txt`](results/telemetry/pprof_top/client_small_write_fsync_cpu.txt) |
| `client` | `small_write_fsync_heap.pb.gz` | `139.98MB 49.77% 49.77%   139.98MB 49.77%  bytes.growSlice` | `0     0%     0%   221.02MB 78.58%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_small_write_fsync_heap.txt`](results/telemetry/pprof_top/client_small_write_fsync_heap.txt) |
| `client` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_small_write_fsync_mutex.txt`](results/telemetry/pprof_top/client_small_write_fsync_mutex.txt) |
| `client` | `sqlite_wal_cpu.pb.gz` | `10ms 33.33% 33.33%       10ms 33.33%  aeshashbody` | `0     0%     0%       30ms   100%  github.com/prometheus/client_golang/prometheus/promhttp.HandlerForTransactional.func1` | [`pprof_top/client_sqlite_wal_cpu.txt`](results/telemetry/pprof_top/client_sqlite_wal_cpu.txt) |
| `client` | `sqlite_wal_heap.pb.gz` | `64.02MB 53.15% 53.15%    64.02MB 53.15%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   103.01MB 85.52%  sync.(*Pool).Get` | [`pprof_top/client_sqlite_wal_heap.txt`](results/telemetry/pprof_top/client_sqlite_wal_heap.txt) |
| `client` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_sqlite_wal_mutex.txt`](results/telemetry/pprof_top/client_sqlite_wal_mutex.txt) |

### R1 → R2 → R3 可观测性驱动根因定位与 H2C 双峰尾延迟闭环消除实证

本轮基准验证完整经历了三轮端到端实测闭环，验证了全链路遥测在定位跨层网络协议交互故障中的决定性作用：

1. **R1（`4e977c1`）暴露可观测性盲区与冷顺序读串行瓶颈**：
   - R1 审计发现 OTLP Endpoint 缺少 `http://` 协议头致 Jaeger 采样为空、Prometheus 首桶 `5 ms` 掩盖亚毫秒分布、缺少阶段级快照差值（`phase_deltas`）与服务端外层 HTTP Handler 计时（`bowl_storage_http_request_duration_seconds`），且单句柄顺序预读窗口深度偏浅使冷 `sequential_read` 停留在 `555.93 MiB/s`。
2. **R2（`8bf2b8b-dirty`）预读提速生效，新增跨层埋点精准捕获 `~10 ms` H2C 双峰尾延迟**：
   - 单句柄自适应预读（`W_read=4`）使冷 `sequential_read` 跃升 **1.81x** 至 `1,004.16 MiB/s`；但小包/元数据负载出现反常回退（冷 `small_write_4k_fsync` `92.10 ops/s`、冷 `mkdir` `155.08 ops/s`）。
   - R2 新增的 `http_outer` vs `rpc` 双层直方图与跨双端 Jaeger Trace 当场锁定唯一异常段：内层 `StoreActor` 执行正常（`rpc:Mkdir` P50 `0.076 ms`、`rpc:Fsync` P50 `0.353 ms`），但外层 `http_outer:Mkdir` P50 达 `6.157 ms`（P95 `9.813 ms`）、`http_outer:Fsync` P50 达 `6.878 ms`（P95 `9.969 ms`），Jaeger 跨端 `Client RPC -> Server RPC` `req_gap` 呈现在 `~0.2 ms` 与 `~10.1 ms` 之间的双峰分布——物理根因为 Go `x/net/http2` 将 `HEADERS` 帧与小包 `DATA` 帧分两次 `Write` 下发 Socket，触发 Linux 内核 `tcp_autocorking` 与对端 TCP Delayed ACK 互锁等待（`~9.5–10.2 ms`）。
3. **R3 终验（`8afc63c`，clean）合入 `h2CoalesceConn` + `tcp_autocorking=0`，慢桶 `100%` 归零并刷新全套最高性能**：
   - 双端启用 `h2CoalesceConn` 亚微秒级帧合并单次 `Write` 并在测试节点显式设置 `net.ipv4.tcp_autocorking=0` 后，全套件 **`51,465` 次**元数据与 `Fsync` 请求中：
     - **`51,417 / 51,465`（`99.91%`）`<= 1.0 ms`**，`51,439 / 51,465`（`99.95%`）`<= 2.5 ms`；
     - **`(5, 25] ms` 区间内 `bowl_storage_http_request` 与 `rpc_server_call` 的慢桶差值严格为 `0 / 51,465`（`0.000%`）**（其中 `d9_cold_small_write_fsync` 阶段 `2,215 / 2,215 = 100.00% <= 2.5 ms`、`99.86% <= 1.0 ms`；`d9_cold_meta_macro` 阶段 `22,973 / 22,973 = 100.00% <= 2.5 ms`、`99.98% <= 1.0 ms`）；
     - Jaeger 跨双端小包采样链路（`n = 385`）`Client RPC - Server RPC` 差值中位数降至 **`0.202 ms`**（P95 `0.334 ms`、P99 `0.458 ms`）；客户端 `bowl-mount.log` 为 `0 B`（`0` 报错），Pebble 事件日志告警 `0` 条（注：Loki 统计中服务端 `42` 条命中系 Pebble EventListener 输出的结构化生命周期 JSON 含空字段 `"err":""`，`errors_bowl-storage.json` 实际错误数为 `0`）。

#### 跨轮次服务端外层 HTTP vs 内层 RPC 小包延迟消除对照表

| 阶段 / 指标 | R2 (`8bf2b8b-dirty` · 受 autocork 影响) | **R3 (`8afc63c` · `h2CoalesceConn` 终验)** | 改善幅度 / 结论 |
| :--- | ---: | ---: | :--- |
| **全局 `http_outer:Mkdir` P50 / P95 / P99** (`n=6,254`) | `6.157 / 9.813 / — ms` | **`0.073 / 0.178 / 0.237 ms`** | **P50 快 `84.3x` · P95 快 `55.1x`**（内层 `rpc:Mkdir` `0.040 / 0.092 / 0.141 ms`） |
| **全局 `http_outer:Fsync` P50 / P95** (`n=1,319`) | `6.878 / 9.969 ms` | **`0.363 / 0.494 ms`** | **P50 快 `18.9x` · P95 快 `20.2x`**（内层 `rpc:Fsync` `0.221 / 0.485 ms`） |
| **全局 `http_outer:Create` Mean / P50 / P99** (`n=12,315`) | `~3.68 / — / — ms` | **`0.097 / 0.090 / 0.248 ms`** | **`12,315 / 12,315` (`100%`) `<= 2.5 ms`**（内层 `rpc:Create` `0.060 / 0.060 / 0.214 ms`） |
| **`d9_cold_small_write_fsync` `Create` HTTP+Codec 差值均值** (`n=600`) | `2.973 ms` (`srv_http 3.057` vs `rpc 0.084`) | **`0.049 ms`** (`srv_http 0.124` vs `rpc 0.075`) | **帧拆分停顿削减 `-98.4%` (`60.7x`)** |
| **`d9_cold_small_write_fsync` `Fsync` HTTP+Codec 差值均值** (`n=600`) | `6.652 ms` (`srv_http 6.939` vs `rpc 0.287`) | **`0.047 ms`** (`srv_http 0.302` vs `rpc 0.255`) | **帧拆分停顿削减 `-99.3%` (`141.5x`)**，`fuse.Fsync` P50 `10.240 -> 0.413 ms` |
| **`d9_cold_meta_macro` `Mkdir` HTTP+Codec 差值均值** (`n=3,098`) | `6.217 ms` (`srv_http 6.289` vs `rpc 0.072`) | **`0.030 ms`** (`srv_http 0.070` vs `rpc 0.040`) | **帧拆分停顿削减 `-99.5%` (`207.2x`)**，客户端 RPC Mean `6.511 -> 0.226 ms` |
| **全套件小包 + Fsync `(5, 25] ms` (`http_outer - rpc`) 慢桶数** | 数万次落入 `(5, 10] ms` 慢桶 | **`0 / 51,465` (`0.000%`)** | **`~10 ms` 双峰尾延迟彻底归零** |

#### 历史基线 vs R1 vs R2 vs R3 核心性能演进总表

| 套件与指标 | 上轮基线 (`9c60a34`) | R1 (`4e977c1`) | R2 (`8bf2b8b-dirty`) | **R3 终验 (`8afc63c`)** | R3 vs 基线 / 同机 Drive9 Default |
| :--- | ---: | ---: | ---: | ---: | :--- |
| `juicefs bench` **Write big / Read big** (MiB/s) | `1,699.27 / 1,504.90` | `1,637.21 / 1,341.55` | `1,563.57 / 1,337.41` | **`1,649.21 / 1,383.14`** | Write **`1.24x` D9 Default** (`1,332.24`) · **`2.72x` Sync** |
| `juicefs bench` **Write small / Read small** (files/s) | `1,392.0 / 3,970.5` | `1,714.3 / 3,401.2` | `1,633.8 / 1,143.0` | **`1,738.9 / 3,789.0`** | Write **`+24.9%` vs 基线** · Read **`1.57x` D9 Default** |
| Cold **`sequential_write`** (256 MiB + fsync) | `1,142.87 MiB/s` | `1,194.51 MiB/s` | `1,160.50 MiB/s` | **`1,207.11 MiB/s`** | **较基线 `+5.6%` · `1.84x` D9 Default · `3.61x` Sync** |
| Cold **`sequential_read`** (256 MiB) | `653.52 MiB/s` | `555.93 MiB/s` | `1,004.16 MiB/s` | **`1,073.56 MiB/s`** | **较基线 `+64.3%` (`1.93x` R1) · `2.01x` D9 Default** |
| Cold **`random_read_4k`** (1,000 reads) | `4.97 MiB/s` | `5.83 MiB/s` | `1.08 MiB/s` | **`6.74 MiB/s`** | **较基线 `+35.6%` (`6.24x` R2) · `1.44x` D9 Default** |
| Cold **`small_write_4k_fsync`** (200 files) | `158.43 ops/s` | `547.71 ops/s` | `92.10 ops/s` | **`754.90 ops/s`** | **较基线 `4.76x` (`8.20x` R2) · `6.08x` D9 Sync** |
| Cold **`mkdir` / `rename` / `unlink`** (ops/s) | `674.8 / 2,495.7 / 2,596.6` | `1,624.2 / 2,408.1 / 1,794.6` | `155.1 / 284.3 / 185.9` | **`3,146.6 / 3,000.3 / 3,084.8`** | **`mkdir` `4.66x` 基线 · `14.7x / 35.6x / 33.5x` D9 Default** |
| Cold **`macro_git_clone`** (200 files · 20 MiB) | `5.668 s` | `0.899 s` | `2.880 s` | **`0.760 s`** | **较基线快 `7.46x` · 较 D9 Default 快 `2.60x` (`9.16x` Sync)** |
| Hot **`sequential_read` / `mkdir` / `unlink`** | `7,827 MiB/s / 484 / 2,727` | `7,195 / 1,785 / 1,978` | `7,642 / 157 / 176` | **`7,001.9 MiB/s / 3,612.1 / 3,961.4`** | Hot `mkdir` **较基线 `7.46x`** · `17.6x / 45.5x` D9 Default |
| `fuse_perf_baseline` **64×1K Write / Read** (files/s) | `171.68 / 461.99` | `573.32 / 1,271.09` | `73.85 / 204.75` | **`640.93 / 1,506.55`** | **较基线 `3.73x / 3.26x`** · Read **`17.11x` D9 Default** |
| `SQLite DELETE` **Insert / Update** (rows/s) | `1,342.21 / 126.71` | `10,682.97 / 681.82` | `2,092.64 / 102.00` | **`9,332.29 / 924.95`** | **较基线 `6.95x / 7.30x`** · Insert **`1.65x` D9 Default** |
| `SQLite WAL` **Insert / Update / Checkpoint** | `666.8 / 274.4 / 0.113 s` | `4,722.5 / 1,581.4 / 0.026 s` | `711.4 / 404.1 / 0.070 s` | **`4,880.3 / 1,757.3 / 0.0202 s`** | **较基线 `7.32x / 6.40x` · Checkpoint 快 `5.59x`** |

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9bench-kp-20261111-191124`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj95goqmagl76j2rt5r4`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj95goqmagl76j2rt5r5`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-11T19:11:39Z` |
| **实例销毁时间 (UTC)** | `2026-11-11T19:38:04Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
