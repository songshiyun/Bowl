# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘，依次运行以下三组目标：

1. **Bowl（PostgreSQL 16 控制面 + Pebble）**：PostgreSQL 仅保存 `disks`、`commits`、`client_sessions`；每个 `(disk, branch)` 使用常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 与 12 字节 `BlobRef` 索引，文件数据落在 `blobs/<shard>/<blob_id>.blob` 的 1 MiB 不可变块文件中；整块直写、局部写 CoW，截断/打洞/删除在提交后 `os.Remove` 即时回收旧块，Checkpoint 与 Fork 以硬链接共享块。服务端分支锁降为拓扑生命周期 `RLock`，完整 1 MiB 块在 Inode 锁外并行落盘，单键点查免 Snapshot，读路径 atime 先登记内存再按 1 秒周期合并；常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`）。
   客户端采用 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `334.66 MiB/s` | `302.84 MiB/s` | `297.08 MiB/s` | **1.11x** | **1.13x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,701.99 MiB/s` | `454.92 MiB/s` | `474.90 MiB/s` | **3.74x** | **3.58x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `1,110.80 files/s` | `475.20 files/s` | `595.90 files/s` | **2.34x** | **1.86x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `4,570.30 files/s` | `1,364.30 files/s` | `1,598.50 files/s` | **3.35x** | **2.86x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `121,621.00 files/s` | `8,015.80 files/s` | `9,972.20 files/s` | **15.17x** | **12.20x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `176.43 MiB/s` | `292.51 MiB/s` | `300.73 MiB/s` | **0.60x** | **0.59x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `190.90 MiB/s` | `184.28 MiB/s` | `184.15 MiB/s` | **1.04x** | **1.04x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `179.81 MiB/s` | `199.01 MiB/s` | `198.83 MiB/s` | **0.90x** | **0.90x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `182.47 MiB/s` | `176.81 MiB/s` | `177.12 MiB/s` | **1.03x** | **1.03x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `3,533.74 ops/s` | `693.10 ops/s` | `1,420.64 ops/s` | **5.10x** | **2.49x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `2,755.48 ops/s` | `773.97 ops/s` | `1,389.96 ops/s` | **3.56x** | **1.98x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `103,070.36 ops/s` | `3,065.10 ops/s` | `10,037.58 ops/s` | **33.63x** | **10.27x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `3,069.28 ops/s` | `641.05 ops/s` | `1,276.39 ops/s` | **4.79x** | **2.40x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9ahyg55k5x08hpm54z` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.249` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9ahyg55k5x08hpm550` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.248` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.067/0.103/0.312/0.054` ms | `ping -c 20 -i 0.05 172.16.1.249` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.249 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `172.16` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `179.21` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `a253128` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | 每分支 Pebble 元数据 + `blobs/<shard>/<blob_id>.blob` 1 MiB 块文件 (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.249:9001/jfs-pg`) | rclone S3 (`http://172.16.1.249:9001/jfs-redis`) |
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
| **Write big file** | `334.66 MiB/s (12.24 s/file)` | `302.84 MiB/s (13.53 s/file)` | `297.08 MiB/s (13.79 s/file)` | **1.11x** | **1.13x** |
| **Read big file** | `1701.99 MiB/s (2.41 s/file)` | `454.92 MiB/s (9.00 s/file)` | `474.90 MiB/s (8.62 s/file)` | **3.74x** | **3.58x** |
| **Write small file** | `1110.80 files/s (3.60 ms/file)` | `475.20 files/s (8.42 ms/file)` | `595.90 files/s (6.71 ms/file)` | **2.34x** | **1.86x** |
| **Read small file** | `4570.30 files/s (0.88 ms/file)` | `1364.30 files/s (2.93 ms/file)` | `1598.50 files/s (2.50 ms/file)` | **3.35x** | **2.86x** |
| **Stat file** | `121621.00 files/s (0.03 ms/file)` | `8015.80 files/s (0.50 ms/file)` | `9972.20 files/s (0.40 ms/file)` | **15.17x** | **12.20x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `176.43 MiB/s (44.1 IOPS)` | `292.51 MiB/s (73.1 IOPS)` | `300.73 MiB/s (75.2 IOPS)` | **0.60x** | **0.59x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `190.90 MiB/s (47.7 IOPS)` | `184.28 MiB/s (46.1 IOPS)` | `184.15 MiB/s (46.0 IOPS)` | **1.04x** | **1.04x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `179.81 MiB/s (45.0 IOPS)` | `199.01 MiB/s (49.8 IOPS)` | `198.83 MiB/s (49.7 IOPS)` | **0.90x** | **0.90x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `182.47 MiB/s (45.6 IOPS)` | `176.81 MiB/s (44.2 IOPS)` | `177.12 MiB/s (44.3 IOPS)` | **1.03x** | **1.03x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `288.51 MiB/s (288.5 IOPS)` | `332.25 MiB/s (332.2 IOPS)` | `332.49 MiB/s (332.5 IOPS)` | **0.87x** | **0.87x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `187.45 MiB/s (187.4 IOPS)` | `184.24 MiB/s (184.2 IOPS)` | `184.45 MiB/s (184.4 IOPS)` | **1.02x** | **1.02x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `289.61 MiB/s (289.6 IOPS)` | `292.45 MiB/s (292.4 IOPS)` | `300.80 MiB/s (300.8 IOPS)` | **0.99x** | **0.96x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `187.39 MiB/s (187.4 IOPS)` | `122.39 MiB/s (122.4 IOPS)` | `117.90 MiB/s (117.9 IOPS)` | **1.53x** | **1.59x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `12.52 / 39.06 / 66.85 ms` | `11.44 / 43.78 / 73.92 ms` | `11.32 / 44.30 / 82.31 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `20.95 / 30.80 / 34.87 ms` | `21.70 / 114.82 / 160.43 ms` | `21.72 / 109.58 / 164.63 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `320.63 / 501.22 / 658.51 ms` | `308.35 / 354.42 / 442.50 ms` | `309.29 / 358.61 / 450.89 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `349.28 / 396.36 / 417.33 ms` | `345.82 / 1010.83 / 1266.68 ms` | `349.74 / 1010.83 / 1283.46 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.02 ms` | `0.00 / 0.01 / 0.02 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `3,533.7` | `693.1` | `1,420.6` | **5.10x** | **2.49x** |
| **Directory stat** | `111,033.1` | `3,051.7` | `10,229.7` | **36.38x** | **10.85x** |
| **Directory removal** | `3,052.4` | `557.8` | `1,170.0` | **5.47x** | **2.61x** |
| **File creation** | `2,755.5` | `774.0` | `1,390.0` | **3.56x** | **1.98x** |
| **File stat** | `103,070.4` | `3,065.1` | `10,037.6` | **33.63x** | **10.27x** |
| **File read** | `42,369.0` | `1,619.2` | `2,857.2` | **26.17x** | **14.83x** |
| **File removal** | `3,069.3` | `641.1` | `1,276.4` | **4.79x** | **2.40x** |
| **Tree creation** | `3,669.6` | `668.5` | `1,438.5` | **5.49x** | **2.55x** |
| **Tree removal** | `2,802.9` | `528.6` | `1,198.3` | **5.30x** | **2.34x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `8,885.8` | `2,318.6` | `4,601.1` | **3.83x** | **1.93x** |
| **Directory stat** | `1,288,587.5` | `52,953.7` | `33,044.3` | **24.33x** | **39.00x** |
| **Directory removal** | `9,719.0` | `2,239.2` | `4,388.9` | **4.34x** | **2.21x** |
| **File creation** | `8,142.7` | `2,618.9` | `5,159.9` | **3.11x** | **1.58x** |
| **File stat** | `1,466,449.7` | `295,226.0` | `63,873.1` | **4.97x** | **22.96x** |
| **File read** | `52,900.0` | `11,301.3` | `14,566.6` | **4.68x** | **3.63x** |
| **File removal** | `9,511.2` | `2,588.4` | `5,367.8` | **3.67x** | **1.77x** |
| **Tree creation** | `2,146.8` | `279.4` | `264.7` | **7.68x** | **8.11x** |
| **Tree removal** | `1,573.8` | `497.3` | `484.2` | **3.16x** | **3.25x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`176.43 MiB/s`；同轮 JuiceFS(PG) 为 `292.51 MiB/s`。
- Bowl 单流顺序读：`190.90 MiB/s`；同轮 JuiceFS(PG) 为 `184.28 MiB/s`。
- Bowl 16 并发顺序写：`179.81 MiB/s`；同轮 JuiceFS(PG) 为 `199.01 MiB/s`。
- Bowl 16 并发顺序读：`182.47 MiB/s`；同轮 JuiceFS(PG) 为 `176.81 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `334.66 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `176.43 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

Bowl 的写入和同步路径使用 Pebble；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261106-135313`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9ahyg55k5x08hpm54z`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9ahyg55k5x08hpm550`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-06T13:53:27Z` |
| **实例销毁时间 (UTC)** | `2026-11-06T14:19:14Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
