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
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `282.50 MiB/s` | `200.20 MiB/s` | `64.54 MiB/s` | **1.41x** | **4.38x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `951.07 MiB/s` | `180.51 MiB/s` | `346.40 MiB/s` | **5.27x** | **2.75x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `268.30 files/s` | `2,478.90 files/s` | `99.10 files/s` | **0.11x** | **2.71x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `661.00 files/s` | `7,723.00 files/s` | `12.80 files/s` | **0.09x** | **51.64x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,574.90 files/s` | `105,162.90 files/s` | `22.20 files/s` | **0.06x** | **296.17x** |
| `drive9_fuse_bench` (Cold) | **顺序写 (256 MiB + fsync)** | `346.78 MiB/s` | `260.35 MiB/s` | `189.59 MiB/s` | **1.33x** | **1.83x** |
| `drive9_fuse_bench` (Cold) | **顺序读 (256 MiB)** | `189.35 MiB/s` | `183.50 MiB/s` | `60.65 MiB/s` | **1.03x** | **3.12x** |
| `drive9_fuse_bench` (Cold) | **4K 小文件写 + fsync (200 个)** | `214.00 ops/s` | `1,011.60 ops/s` | `147.70 ops/s` | **0.21x** | **1.45x** |
| `drive9_fuse_bench` (Cold) | **目录创建 mkdir (500 个)** | `559.80 ops/s` | `251.27 ops/s` | `243.80 ops/s` | **2.23x** | **2.30x** |
| `drive9_fuse_bench` (Cold) | **目录重命名 rename (500 个)** | `444.59 ops/s` | `94.40 ops/s` | `57.46 ops/s` | **4.71x** | **7.74x** |
| `drive9_fuse_bench` (Cold) | **文件删除 unlink (500 个)** | `555.67 ops/s` | `98.65 ops/s` | `52.88 ops/s` | **5.63x** | **10.51x** |
| `drive9_fuse_bench` (Cold) | **Git Clone (200 文件 · 20 MiB, 耗时越低越好)** | `3.96 s` | `1.88 s` | `6.16 s` | **0.48x** | **1.56x** |
| SQLite Baseline (`WAL`) | **SQLite WAL 事务插入 (256 rows)** | `1,678.38 rows/s` | `7,715.88 rows/s` | `8,137.68 rows/s` | **0.22x** | **0.21x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-d9-server`) | `i-rj96chrxq3htln3b3rul` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.218` |
| **Client 客户端** (`bowl-d9-client`) | `i-rj96chrxq3htln3b3rum` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4Gi | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.219` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `rtt min/avg/max/mdev = 0.066/0.099/0.217/0.038 ms` | `ping -c 20 -i 0.05 172.16.1.218` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.218 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `192.61` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `184.19` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 三组被测文件系统架构与参数对照矩阵

| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |
| :--- | :--- | :--- | :--- |
| **代码版本 (Git SHA)** | Bowl `ee8d113` | Drive9 `b529a36f` | Drive9 `b529a36f` |
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
| **Write big file** | `282.50 MiB/s (14.50 s/file)` | `200.20 MiB/s (20.46 s/file)` | `64.54 MiB/s (63.46 s/file)` | **1.41x** | **4.38x** |
| **Read big file** | `951.07 MiB/s (4.31 s/file)` | `180.51 MiB/s (22.69 s/file)` | `346.40 MiB/s (11.82 s/file)` | **5.27x** | **2.75x** |
| **Write small file** | `268.30 files/s (14.91 ms/file)` | `2478.90 files/s (1.61 ms/file)` | `99.10 files/s (40.37 ms/file)` | **0.11x** | **2.71x** |
| **Read small file** | `661.00 files/s (6.05 ms/file)` | `7723.00 files/s (0.52 ms/file)` | `12.80 files/s (311.34 ms/file)` | **0.09x** | **51.64x** |
| **Stat file** | `6574.90 files/s (0.61 ms/file)` | `105162.90 files/s (0.04 ms/file)` | `22.20 files/s (180.37 ms/file)` | **0.06x** | **296.17x** |

---

## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试

### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `346.78 (359.70) MiB/s` | `260.35 (435.01) MiB/s` | `189.59 (260.49) MiB/s` | **1.33x** | **1.83x** |
| `sequential_read` | **顺序读 (256 MiB)** | `189.35 (318.66) MiB/s` | `183.50 (183.77) MiB/s` | `60.65 (60.92) MiB/s` | **1.03x** | **3.12x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `6.82 (6.94) MiB/s` | `2.78 (2.78) MiB/s` | `5.08 (5.10) MiB/s` | **2.45x** | **1.34x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `214.00 (218.73) ops/s` | `1,011.60 (1,033.55) ops/s` | `147.70 (148.10) ops/s` | **0.21x** | **1.45x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `559.80 (575.41) ops/s` | `251.27 (253.65) ops/s` | `243.80 (248.82) ops/s` | **2.23x** | **2.30x** |
| `rename` | **目录重命名 rename (500 个)** | `444.59 (462.28) ops/s` | `94.40 (95.80) ops/s` | `57.46 (61.38) ops/s` | **4.71x** | **7.74x** |
| `unlink` | **文件删除 unlink (500 个)** | `555.67 (572.94) ops/s` | `98.65 (100.05) ops/s` | `52.88 (52.95) ops/s` | **5.63x** | **10.51x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `3,491.68 (4,631.20) ops/s` | `7,024.47 (7,149.62) ops/s` | `13,529.25 (16,635.62) ops/s` | **0.50x** | **0.26x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `12,402.29 (12,499.54) ops/s` | `42,420.99 (49,278.85) ops/s` | `116,730.38 (125,871.02) ops/s` | **0.29x** | **0.11x** |
| `stat` | **文件属性查询 stat (500 个)** | `2,928.71 (2,972.73) ops/s` | `21,812.66 (22,785.68) ops/s` | `43,792.01 (49,563.91) ops/s` | **0.13x** | **0.07x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.958s (4.132s)` | `1.881s (1.932s)` | `6.158s (6.204s)` | **0.48x** | **1.56x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.321s (0.322s)` | `2.027s (2.034s)` | `0.239s (0.296s)` | **6.32x** | **0.75x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.129s (0.145s)` | `0.777s (0.785s)` | `0.066s (0.502s)` | **6.03x** | **0.51x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.098s (0.107s)` | `0.014s (0.015s)` | `0.050s (0.054s)` | **0.14x** | **0.51x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.515s (1.956s)` | `0.354s (0.357s)` | `0.323s (0.356s)` | **0.69x** | **0.63x** |

### 4.2 热缓存（Hot Cache，预热后直接测量）对比

#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `sequential_write` | **顺序写 (256 MiB + fsync)** | `300.70 (300.90) MiB/s` | `273.71 (437.25) MiB/s` | `188.93 (260.75) MiB/s` | **1.10x** | **1.59x** |
| `sequential_read` | **顺序读 (256 MiB)** | `7,225.67 (7,718.02) MiB/s` | `2,205.55 (2,235.35) MiB/s` | `708.39 (716.67) MiB/s` | **3.28x** | **10.20x** |
| `random_read_4k` | **随机 4K 读 (1,000 次)** | `321.86 (379.94) MiB/s` | `185.80 (190.12) MiB/s` | `4.52 (4.55) MiB/s` | **1.73x** | **71.15x** |
| `small_write_4k_fsync` | **4K 小文件写 + fsync (200 个)** | `176.25 (184.13) ops/s` | `1,056.96 (1,116.14) ops/s` | `145.44 (147.41) ops/s` | **0.17x** | **1.21x** |
| `mkdir` | **目录创建 mkdir (500 个)** | `496.44 (514.79) ops/s` | `249.24 (251.65) ops/s` | `248.20 (248.51) ops/s` | **1.99x** | **2.00x** |
| `rename` | **目录重命名 rename (500 个)** | `400.88 (414.98) ops/s` | `93.69 (93.99) ops/s` | `54.99 (57.94) ops/s` | **4.28x** | **7.29x** |
| `unlink` | **文件删除 unlink (500 个)** | `540.06 (576.89) ops/s` | `100.97 (101.40) ops/s` | `52.97 (53.08) ops/s` | **5.35x** | **10.19x** |
| `readdir_small` | **小目录列举 readdir (20 项)** | `6,981.68 (8,044.23) ops/s` | `127,292.05 (149,657.15) ops/s` | `211,153.11 (232,211.96) ops/s` | **0.05x** | **0.03x** |
| `readdir_large` | **大目录列举 readdir (500 项)** | `13,832.56 (14,196.39) ops/s` | `90,058.04 (97,328.02) ops/s` | `274,306.28 (293,574.47) ops/s` | **0.15x** | **0.05x** |
| `stat` | **文件属性查询 stat (500 个)** | `51,136.18 (62,392.71) ops/s` | `673,677.40 (691,973.89) ops/s` | `13,189.61 (31,655.76) ops/s` | **0.08x** | **3.88x** |

#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）

| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `macro_git_clone` | **Git Clone (--no-local · 200 文件 · 20 MiB)** | `3.738s (3.779s)` | `2.027s (2.061s)` | `6.293s (6.346s)` | **0.54x** | **1.68x** |
| `macro_git_status_dirty` | **Git Status (--porcelain · dirty 工作区)** | `0.154s (0.157s)` | `0.237s (0.240s)` | `0.038s (0.066s)` | **1.54x** | **0.25x** |
| `macro_git_diff_dirty` | **Git Diff (--name-only · dirty 工作区)** | `0.061s (0.092s)` | `0.015s (0.015s)` | `0.012s (0.024s)` | **0.24x** | **0.19x** |
| `macro_find_files` | **目录树遍历 (find . -type f | wc -l)** | `0.078s (0.081s)` | `0.004s (0.004s)` | `0.006s (0.015s)` | **0.05x** | **0.08x** |
| `macro_go_build` | **Go 编译构建 (go build ./...)** | `0.224s (0.251s)` | `0.117s (0.129s)` | `0.157s (0.175s)` | **0.52x** | **0.70x** |

---

## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准

运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。

| 测试工作负载 (`Workload`) | 指标单位 | Bowl (PG 16) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **64×1KiB 小文件写 (`fsync` + dir `fsync`)** | `files/s` | `178.03 files/s` | `872.52 files/s` | `159.48 files/s` | **0.20x** | **1.12x** |
| **64×1KiB 小文件 SHA-256 校验读** | `files/s` | `337.02 files/s` | `93.24 files/s` | `353.53 files/s` | **3.61x** | **0.95x** |
| **16 MiB 大文件写 (`fsync` + dir `fsync`)** | `MiB/s` | `52.59 MiB/s` | `60.25 MiB/s` | `50.23 MiB/s` | **0.87x** | **1.05x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 1)** | `MiB/s` | `185.99 MiB/s` | `694.17 MiB/s` | `81.72 MiB/s` | **0.27x** | **2.28x** |
| **16 MiB 大文件 SHA-256 校验读 (Pass 2)** | `MiB/s` | `1,180.74 MiB/s` | `737.46 MiB/s` | `51.82 MiB/s` | **1.60x** | **22.78x** |
| **SQLite `DELETE` 模式：事务插入 (256 rows)** | `rows/s` | `2,992.31 rows/s` | `6,573.81 rows/s` | `4,234.17 rows/s` | **0.46x** | **0.71x** |
| **SQLite `DELETE` 模式：事务更新 (86 rows)** | `rows/s` | `317.91 rows/s` | `1,117.53 rows/s` | `896.66 rows/s` | **0.28x** | **0.35x** |
| **SQLite `DELETE` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `41,419.50 rows/s` | `717,319.91 rows/s` | `69,839.66 rows/s` | **0.06x** | **0.59x** |
| **SQLite `WAL` 模式：事务插入 (256 rows)** | `rows/s` | `1,678.38 rows/s` | `7,715.88 rows/s` | `8,137.68 rows/s` | **0.22x** | **0.21x** |
| **SQLite `WAL` 模式：事务更新 (86 rows)** | `rows/s` | `545.50 rows/s` | `2,860.58 rows/s` | `2,739.99 rows/s` | **0.19x** | **0.20x** |
| **SQLite `WAL` 模式：全表聚合校验读 (256 rows)** | `rows/s` | `89,849.06 rows/s` | `1,231,550.79 rows/s` | `1,165,533.00 rows/s` | **0.07x** | **0.08x** |
| **SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时** | `s` | `0.0803 s` | `0.0087 s` | `0.0321 s` | **0.11x** | **0.40x** |

---

## 6. 本轮观测与比较边界

- `Write big file`：Bowl `282.50 MiB/s`，Drive9 Sync `64.54 MiB/s`，吞吐比 `4.38x`。
- `Read big file`：Bowl `951.07 MiB/s`，Drive9 Sync `346.40 MiB/s`，吞吐比 `2.75x`。
- `Write small file`：Bowl `268.30 files/s`，Drive9 Sync `99.10 files/s`，吞吐比 `2.71x`。
- `Read small file`：Bowl `661.00 files/s`，Drive9 Sync `12.80 files/s`，吞吐比 `51.64x`。
- `Stat file`：Bowl `6574.90 files/s`，Drive9 Sync `22.20 files/s`，吞吐比 `296.17x`。

Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。

冷缓存准备命令仅清理客户端内核页缓存；未重启 FUSE 进程、清理 Drive9 用户态或磁盘读缓存，也未清理服务端页缓存。因此本报告的 cold 表示该命令下的缓存状态，不能解释为完整远端冷读。

微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。

本轮包含 ADR-0028：Create、Write、常规 Release 使用单语句原子 CTE 与内联异步 WAL 提交；重开文件时按版本保留内核页缓存。因此重复读取包含缓存命中收益，本轮数据不能单独归因于数据库往返减少。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-d9-bench-kp-20261102184005`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj96chrxq3htln3b3rul`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj96chrxq3htln3b3rum`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T10:40:19Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T11:07:25Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
