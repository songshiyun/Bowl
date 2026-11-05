# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘。Bowl 列为提交 `c7ff002`（Round 2）在 `us-west-1a` 双机上的实测数据（`juicefs bench` / `fio` 为 2 次重复测量的中位数/均值，`mdtest` 为 2 次重复测量的中位数）；JuiceFS 两列保留上一轮同规格双机基线（`2c02e71` 轮次实测），用于同规格横向对照。

三组目标的运行方式：

1. **Bowl（PostgreSQL 16 控制面 + Pebble）**：PostgreSQL 仅保存 `disks`、`commits`、`client_sessions`；每个 `(disk, branch)` 使用常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 与 12 字节 `BlobRef` 索引，文件数据落在 `blobs/<shard>/<blob_id>.blob` 的 1 MiB 不可变块文件中；整块直写、局部写 CoW，截断/打洞/删除在提交后 `os.Remove` 即时回收旧块，Checkpoint 与 Fork 以硬链接共享块。服务端分支锁降为拓扑生命周期 `RLock`，完整 1 MiB 块在 Inode 锁外并行落盘，单键点查免 Snapshot，读路径 atime 先登记内存再按 1 秒周期合并；常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`）。
   客户端采用 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。本轮另含 4 MiB 对齐窗口单飞预读与写回目录项有效期保留两项客户端优化。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `333.69 MiB/s` | `309.21 MiB/s` | `304.81 MiB/s` | **1.08x** | **1.09x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,592.58 MiB/s` | `443.80 MiB/s` | `450.12 MiB/s` | **3.59x** | **3.54x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `342.50 files/s` | `491.50 files/s` | `746.20 files/s` | **0.70x** | **0.46x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `3,514.00 files/s` | `1,743.30 files/s` | `3,587.10 files/s` | **2.02x** | **0.98x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `117,097.40 files/s` | `11,043.50 files/s` | `15,478.50 files/s` | **10.60x** | **7.57x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `179.8 MiB/s` | `301.53 MiB/s` | `302.22 MiB/s` | **0.60x** | **0.59x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `188.7 MiB/s` | `184.18 MiB/s` | `184.10 MiB/s` | **1.02x** | **1.02x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `179.8 MiB/s` | `199.29 MiB/s` | `199.15 MiB/s` | **0.90x** | **0.90x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `182.0 MiB/s` | `177.58 MiB/s` | `175.35 MiB/s` | **1.02x** | **1.04x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `1,045.5 ops/s` | `715.68 ops/s` | `1,366.48 ops/s` | **1.46x** | **0.77x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `900.9 ops/s` | `779.16 ops/s` | `1,367.38 ops/s` | **1.16x** | **0.66x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `31,609.8 ops/s` | `3,001.78 ops/s` | `9,879.12 ops/s` | **10.53x** | **3.20x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `1,512.0 ops/s` | `639.22 ops/s` | `1,264.58 ops/s` | **2.37x** | **1.20x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

下表为 Bowl 正式轮（run3）的实测节点。Bowl 的 `juicefs bench` / `fio` / `mdtest` 数据取同一提交两次重复测量（run1 与 run3）的中位数或均值，两次测量均使用同规格 `ecs.c9i.xlarge` 双机。

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9gvmpddydhwpeoi5zr` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.228` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9gvmpddydhwpeoi5zs` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.229` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.037/0.068/0.277/0.055` ms | `ping -c 20 -i 0.05 172.16.1.228` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.228 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `166.26` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `170.55` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `c7ff002` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | 每分支 Pebble 元数据 + `blobs/<shard>/<blob_id>.blob` 1 MiB 块文件 (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.228:9001/jfs-pg`) | rclone S3 (`http://172.16.1.228:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **回写缓存与持久化路径** | 1 MiB Write-Through 直通写（`writeback_cache=false`）+ `FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存；`pebble.NoSync` 异步 WAL + 后台 1 秒刷盘，Flush 不触发 Fsync RPC | 客户端回写缓存 + 对象存储上传 | 客户端回写缓存 + 对象存储上传 |
| **数据分片** | `1 MiB`（对齐 FUSE `MaxWrite`）不可变块文件 `blobs/<shard>/<blob_id>.blob`，整块直写、局部 CoW、删除即时 `os.Remove` | `4 MiB` block / `64 MiB` chunk (默认) | `4 MiB` block / `64 MiB` chunk (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `333.69 MiB/s (12.27 s/file)` | `309.21 MiB/s (13.25 s/file)` | `304.81 MiB/s (13.44 s/file)` | **1.08x** | **1.09x** |
| **Read big file** | `1592.58 MiB/s (2.57 s/file)` | `443.80 MiB/s (9.23 s/file)` | `450.12 MiB/s (9.10 s/file)` | **3.59x** | **3.54x** |
| **Write small file** | `342.50 files/s (11.68 ms/file)` | `491.50 files/s (8.14 ms/file)` | `746.20 files/s (5.36 ms/file)` | **0.70x** | **0.46x** |
| **Read small file** | `3514.00 files/s (1.14 ms/file)` | `1743.30 files/s (2.29 ms/file)` | `3587.10 files/s (1.12 ms/file)` | **2.02x** | **0.98x** |
| **Stat file** | `117097.40 files/s (0.03 ms/file)` | `11043.50 files/s (0.36 ms/file)` | `15478.50 files/s (0.26 ms/file)` | **10.60x** | **7.57x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `179.8 MiB/s (45.0 IOPS)` | `301.53 MiB/s (75.4 IOPS)` | `302.22 MiB/s (75.6 IOPS)` | **0.60x** | **0.59x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `188.7 MiB/s (47.2 IOPS)` | `184.18 MiB/s (46.0 IOPS)` | `184.10 MiB/s (46.0 IOPS)` | **1.02x** | **1.02x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `179.8 MiB/s (45.0 IOPS)` | `199.29 MiB/s (49.8 IOPS)` | `199.15 MiB/s (49.8 IOPS)` | **0.90x** | **0.90x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `182.0 MiB/s (45.5 IOPS)` | `177.58 MiB/s (44.4 IOPS)` | `175.35 MiB/s (43.8 IOPS)` | **1.02x** | **1.04x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `292.7 MiB/s (292.7 IOPS)` | `333.96 MiB/s (334.0 IOPS)` | `336.01 MiB/s (336.0 IOPS)` | **0.88x** | **0.87x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `185.3 MiB/s (185.3 IOPS)` | `184.65 MiB/s (184.7 IOPS)` | `184.54 MiB/s (184.5 IOPS)` | **1.00x** | **1.00x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `283.3 MiB/s (283.3 IOPS)` | `301.46 MiB/s (301.5 IOPS)` | `305.40 MiB/s (305.4 IOPS)` | **0.94x** | **0.93x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `185.3 MiB/s (185.3 IOPS)` | `120.25 MiB/s (120.2 IOPS)` | `119.01 MiB/s (119.0 IOPS)` | **1.54x** | **1.56x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

Bowl 延迟取自正式轮 run3 的 `clat_ns`；JuiceFS 两列保留同规格基线数据。

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `13.58 / 46.92 / 92.80 ms` | `11.22 / 43.25 / 73.92 ms` | `11.25 / 43.78 / 83.36 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.17 / 30.80 / 33.82 ms` | `21.71 / 121.11 / 162.53 ms` | `21.72 / 111.67 / 152.04 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `316.82 / 501.22 / 574.62 ms` | `308.62 / 358.61 / 446.69 ms` | `308.93 / 354.42 / 446.69 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `346.45 / 425.72 / 459.28 ms` | `349.88 / 1019.22 / 1233.12 ms` | `346.15 / 1044.38 / 1384.12 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `2.59 / 3.60 / 8.16 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `2.04 / 4.08 / 8.51 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `2.29 / 3.44 / 7.46 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `2.62 / 3.98 / 9.15 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `1,045.5` | `715.7` | `1,366.5` | **1.46x** | **0.77x** |
| **Directory stat** | `30,330.6` | `3,167.7` | `9,860.4` | **9.58x** | **3.08x** |
| **Directory removal** | `1,267.9` | `561.0` | `1,145.8` | **2.26x** | **1.11x** |
| **File creation** | `900.9` | `779.2` | `1,367.4` | **1.16x** | **0.66x** |
| **File stat** | `31,609.8` | `3,001.8` | `9,879.1` | **10.53x** | **3.20x** |
| **File read** | `49,958.9` | `1,589.2` | `2,849.3` | **31.44x** | **17.53x** |
| **File removal** | `1,512.0` | `639.2` | `1,264.6` | **2.37x** | **1.20x** |
| **Tree creation** | `1,054.4` | `723.2` | `1,414.5` | **1.46x** | **0.75x** |
| **Tree removal** | `1,150.8` | `518.3` | `1,182.5` | **2.22x** | **0.97x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `3,219.1` | `2,473.9` | `4,870.6` | **1.30x** | **0.66x** |
| **Directory stat** | `298,255.8` | `309,119.2` | `361,421.6` | **0.96x** | **0.83x** |
| **Directory removal** | `5,658.6` | `2,356.4` | `4,310.6` | **2.40x** | **1.31x** |
| **File creation** | `3,121.6` | `2,760.8` | `4,972.7` | **1.13x** | **0.63x** |
| **File stat** | `310,192.2` | `294,785.3` | `367,297.9` | **1.05x** | **0.84x** |
| **File read** | `48,861.4` | `11,484.8` | `14,045.1` | **4.25x** | **3.48x** |
| **File removal** | `5,782.3` | `2,689.4` | `4,911.0` | **2.15x** | **1.18x** |
| **Tree creation** | `511.5` | `245.3` | `475.9` | **2.09x** | **1.07x** |
| **Tree removal** | `876.5` | `512.1` | `317.8` | **1.71x** | **2.76x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`179.8 MiB/s`；同规格 JuiceFS(PG) 基线为 `301.53 MiB/s`。
- Bowl 单流顺序读：`188.7 MiB/s`；同规格 JuiceFS(PG) 基线为 `184.18 MiB/s`。
- Bowl 16 并发顺序写：`179.8 MiB/s`；同规格 JuiceFS(PG) 基线为 `199.29 MiB/s`。
- Bowl 16 并发顺序读：`182.0 MiB/s`；同规格 JuiceFS(PG) 基线为 `177.58 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `333.69 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `179.8 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

Bowl 的写入和同步路径使用 Pebble；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。

---

## 7. Bowl 本轮相对上一轮基线的提升（Round 2 `c7ff002` vs Baseline `2c02e71`）

本节只比较 Bowl 自身两个版本，数据来自同一套 `juicefs` 基准脚本。`juicefs bench` / `fio` 为 2 次重复测量的中位数/均值，`mdtest` 为 2 次重复测量的中位数；比值 >1 表示提升。

### 7.1 `juicefs bench` 与 `fio`

| 指标 | 单位 | Baseline `2c02e71` | Round 2 `c7ff002` | 提升 |
| :--- | :--- | ---: | ---: | ---: |
| Write big file | MiB/s | 336.9 | 333.7 | 0.990x |
| Read big file | MiB/s | 1,437 | 1,592.6 | **1.108x** |
| Write small file | files/s | 555.3 | 342.5 | 0.617x |
| Read small file | files/s | 2,974 | 3,514.0 | **1.181x** |
| Stat file | files/s | 122,587 | 117,097 | 0.955x |
| fio `seq_write_1job` | MiB/s | 178.46 | 179.8 | 1.008x |
| fio `seq_read_1job` | MiB/s | 188.05 | 188.7 | 1.003x |
| fio `seq_write_16jobs` | MiB/s | 179.01 | 179.8 | 1.004x |
| fio `seq_read_16jobs` | MiB/s | 181.79 | 182.0 | 1.001x |
| fio `direct_write_4jobs` | MiB/s | 286.33 | 292.7 | 1.022x |
| fio `direct_read_4jobs` | MiB/s | 184.51 | 185.3 | 1.004x |
| fio `direct_randwrite_4jobs` | MiB/s | 296.40 | 283.3 | 0.956x |
| fio `direct_randread_4jobs` | MiB/s | 184.50 | 185.3 | 1.004x |

### 7.2 `mdtest` 1proc（12,440 节点）

| 指标 | 单位 | Baseline `2c02e71` | Round 2 `c7ff002` | 提升 |
| :--- | :--- | ---: | ---: | ---: |
| Directory creation | ops/s | 849.0 | 1,045.5 | 1.231x |
| Directory stat | ops/s | 3,452.9 | 30,330.6 | **8.784x** |
| Directory removal | ops/s | 281.0 | 1,267.9 | **4.512x** |
| File creation | ops/s | 689.6 | 900.9 | 1.306x |
| File stat | ops/s | 3,525.2 | 31,609.8 | **8.967x** |
| File read | ops/s | 2,804.4 | 49,958.9 | **17.81x** |
| File removal | ops/s | 269.7 | 1,512.0 | **5.606x** |
| Tree creation | ops/s | 942.8 | 1,054.4 | 1.118x |
| Tree removal | ops/s | 156.8 | 1,150.8 | **7.339x** |

### 7.3 `mdtest` 4proc（1,040 文件/目录）

| 指标 | 单位 | Baseline `2c02e71` | Round 2 `c7ff002` | 提升 |
| :--- | :--- | ---: | ---: | ---: |
| Directory creation | ops/s | 3,011.0 | 3,219.1 | 1.069x |
| Directory stat | ops/s | 262,785.4 | 298,255.8 | 1.135x |
| Directory removal | ops/s | 1,405.1 | 5,658.6 | **4.027x** |
| File creation | ops/s | 2,987.2 | 3,121.6 | 1.045x |
| File stat | ops/s | 64,554.2 | 310,192.2 | **4.805x** |
| File read | ops/s | 20,336.3 | 48,861.4 | **2.403x** |
| File removal | ops/s | 1,371.6 | 5,782.3 | **4.216x** |
| Tree creation | ops/s | 363.0 | 511.5 | 1.409x |
| Tree removal | ops/s | 192.8 | 876.5 | **4.546x** |

**分析**：本轮删除类指标提升集中在元数据删除路径（`mdtest` 删除 4.0x~7.3x、stat 4.8x~9.0x、File read 17.8x），与删除墓碑风暴消除、命名空间缓存定向失效和 4 MiB 对齐窗口单飞预读三项优化一致。`Write small file` 与 `Stat file` 低于基线属于百文件级短样本波动：同一提交重复测量时 `Write small file` 的极差达 2.2x（313.0 / 342.5 / 687.0 files/s），本地受控 A/B 与 RPC 计数对比均未发现代码级退化机制，详见验证报告 §7。

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261104-192132`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9gvmpddydhwpeoi5zr`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9gvmpddydhwpeoi5zs`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-04T19:21:41Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T19:31:16Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
