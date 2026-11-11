# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费本地 NVMe SSD 双机环境（`ecs.i5.xlarge` × 2，1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机（4 vCPU / 32 GiB 内存）、VPC 私网与 894 GiB `local_ssd_pro` 本地 NVMe SSD 数据盘，服务端按目标隔离运行以下三组存储栈：

1. **Bowl（PostgreSQL 16 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack）**：采用 ADR-0030 / ADR-0031 / ADR-0032 架构，PostgreSQL 仅保存单张 `disks` 表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在本地 NVMe SSD 的 `packs/<pack_id_hex16>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `WriteRequest` 池化零分配解码、活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。
   客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备对称的 `WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发 Write RPC 流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 顺序预读与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块本地 NVMe SSD 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）作为元数据引擎 + Server 节点本地 NVMe SSD rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,679.99 MiB/s` | `691.35 MiB/s` | `683.86 MiB/s` | **2.43x** | **2.46x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,403.37 MiB/s` | `1,633.01 MiB/s` | `1,645.17 MiB/s` | **0.86x** | **0.85x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `790.70 files/s` | `737.00 files/s` | `1,143.80 files/s` | **1.07x** | **0.69x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `967.20 files/s` | `3,042.50 files/s` | `4,798.60 files/s` | **0.32x** | **0.20x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `93,191.40 files/s` | `13,110.80 files/s` | `42,575.90 files/s` | **7.11x** | **2.19x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `1,339.44 MiB/s` | `694.71 MiB/s` | `684.03 MiB/s` | **1.93x** | **1.96x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `622.02 MiB/s` | `1,326.85 MiB/s` | `1,343.83 MiB/s` | **0.47x** | **0.46x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `1,305.81 MiB/s` | `678.20 MiB/s` | `677.44 MiB/s` | **1.93x** | **1.93x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `1,397.95 MiB/s` | `1,544.93 MiB/s` | `1,574.93 MiB/s` | **0.90x** | **0.89x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `389.62 ops/s` | `730.13 ops/s` | `2,303.83 ops/s` | **0.53x** | **0.17x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `294.99 ops/s` | `1,272.80 ops/s` | `2,250.97 ops/s` | **0.23x** | **0.13x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `78,492.38 ops/s` | `4,688.36 ops/s` | `16,888.08 ops/s` | **16.74x** | **4.65x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `296.88 ops/s` | `1,041.59 ops/s` | `2,207.59 ops/s` | **0.29x** | **0.13x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj919a36elzt54mhmlox` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme1n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.192` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj919a36elzt54mhmloy` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.193` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.029/0.061/0.256/0.054` ms | `ping -c 20 -i 0.05 172.16.1.192` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.192 -t 5` |
| **Server 本地 NVMe SSD (894 GiB) 顺序写基线** | `1998.05` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 本地 NVMe SSD (894 GiB) 顺序读基线** | `3494.88` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69954` / `695711` IOPS | Server 本地 `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `49885` IOPS (P99 `82.4` µs) | Server 本地 `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `9c60a34-dirty` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / 单盘 `StoreActor` Pebble | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点本地 NVMe SSD)** | 单盘 Pebble + `packs/<pack_id_hex16>.pack` `64 MiB` 封包文件 (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.192:9001/jfs-pg`) | rclone S3 (`http://172.16.1.192:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **回写缓存与持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`writeback_cache=false`）+ 双端 `8M/32M` H2C 窗口 + `WriteRequest` 池化零分配解码；Group Commit `pebble.NoSync` + Fsync 双层并行落盘屏障 | 客户端回写缓存 + 对象存储上传 | 客户端回写缓存 + 对象存储上传 |
| **数据分片** | `64 MiB` `.pack` Append-Only 封包 + `256 KiB` 逻辑块网格 + `AppendBlocks` 零冗余拷贝 + 尾块热缓存 + `ExtendTail` | `4 MiB` block / `64 MiB` chunk (默认) | `4 MiB` block / `64 MiB` chunk (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `1679.99 MiB/s (2.44 s/file)` | `691.35 MiB/s (5.92 s/file)` | `683.86 MiB/s (5.99 s/file)` | **2.43x** | **2.46x** |
| **Read big file** | `1403.37 MiB/s (2.92 s/file)` | `1633.01 MiB/s (2.51 s/file)` | `1645.17 MiB/s (2.49 s/file)` | **0.86x** | **0.85x** |
| **Write small file** | `790.70 files/s (5.06 ms/file)` | `737.00 files/s (5.43 ms/file)` | `1143.80 files/s (3.50 ms/file)` | **1.07x** | **0.69x** |
| **Read small file** | `967.20 files/s (4.14 ms/file)` | `3042.50 files/s (1.31 ms/file)` | `4798.60 files/s (0.83 ms/file)` | **0.32x** | **0.20x** |
| **Stat file** | `93191.40 files/s (0.04 ms/file)` | `13110.80 files/s (0.31 ms/file)` | `42575.90 files/s (0.09 ms/file)` | **7.11x** | **2.19x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `1339.44 MiB/s (334.9 IOPS)` | `694.71 MiB/s (173.7 IOPS)` | `684.03 MiB/s (171.0 IOPS)` | **1.93x** | **1.96x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `622.02 MiB/s (155.5 IOPS)` | `1326.85 MiB/s (331.7 IOPS)` | `1343.83 MiB/s (336.0 IOPS)` | **0.47x** | **0.46x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `1305.81 MiB/s (326.4 IOPS)` | `678.20 MiB/s (169.6 IOPS)` | `677.44 MiB/s (169.4 IOPS)` | **1.93x** | **1.93x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `1397.95 MiB/s (349.5 IOPS)` | `1544.93 MiB/s (386.2 IOPS)` | `1574.93 MiB/s (393.7 IOPS)` | **0.90x** | **0.89x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `1719.56 MiB/s (1719.6 IOPS)` | `741.76 MiB/s (741.8 IOPS)` | `737.89 MiB/s (737.9 IOPS)` | **2.32x** | **2.33x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `1370.36 MiB/s (1370.4 IOPS)` | `1572.36 MiB/s (1572.4 IOPS)` | `1605.02 MiB/s (1605.0 IOPS)` | **0.87x** | **0.85x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `748.13 MiB/s (748.1 IOPS)` | `340.43 MiB/s (340.4 IOPS)` | `340.37 MiB/s (340.4 IOPS)` | **2.20x** | **2.20x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `1004.41 MiB/s (1004.4 IOPS)` | `456.23 MiB/s (456.2 IOPS)` | `470.80 MiB/s (470.8 IOPS)` | **2.20x** | **2.13x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `2.44 / 2.64 / 3.16 ms` | `4.91 / 17.43 / 26.87 ms` | `5.01 / 15.53 / 29.75 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `6.43 / 11.99 / 15.40 ms` | `3.01 / 4.42 / 6.06 ms` | `2.97 / 4.23 / 5.47 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `45.44 / 99.09 / 137.36 ms` | `90.71 / 185.60 / 229.64 ms` | `91.07 / 181.40 / 221.25 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `45.41 / 77.07 / 95.94 ms` | `40.10 / 108.53 / 147.85 ms` | `39.88 / 102.24 / 139.46 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `2.30 / N/A / N/A ms` | `5.20 / N/A / N/A ms` | `5.22 / N/A / N/A ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `2.89 / N/A / N/A ms` | `2.48 / N/A / N/A ms` | `2.43 / N/A / N/A ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `5.30 / N/A / N/A ms` | `11.69 / N/A / N/A ms` | `11.56 / N/A / N/A ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `3.95 / N/A / N/A ms` | `8.74 / N/A / N/A ms` | `8.42 / N/A / N/A ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `389.6` | `730.1` | `2,303.8` | **0.53x** | **0.17x** |
| **Directory stat** | `79,609.4` | `4,798.9` | `15,799.2` | **16.59x** | **5.04x** |
| **Directory removal** | `296.8` | `908.0` | `1,982.8` | **0.33x** | **0.15x** |
| **File creation** | `295.0` | `1,272.8` | `2,251.0` | **0.23x** | **0.13x** |
| **File stat** | `78,492.4` | `4,688.4` | `16,888.1` | **16.74x** | **4.65x** |
| **File read** | `35,968.5` | `2,448.6` | `4,964.9` | **14.69x** | **7.24x** |
| **File removal** | `296.9` | `1,041.6` | `2,207.6` | **0.29x** | **0.13x** |
| **Tree creation** | `320.5` | `1,162.1` | `2,281.4` | **0.28x** | **0.14x** |
| **Tree removal** | `305.3` | `793.2` | `2,023.4` | **0.38x** | **0.15x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.i5.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `3,573.9` | `3,138.4` | `6,145.8` | **1.14x** | **0.58x** |
| **Directory stat** | `1,370,926.1` | `373,487.9` | `63,978.5` | **3.67x** | **21.43x** |
| **Directory removal** | `8,245.2` | `3,114.9` | `6,969.1` | **2.65x** | **1.18x** |
| **File creation** | `2,804.9` | `3,500.1` | `6,364.5` | **0.80x** | **0.44x** |
| **File stat** | `1,430,388.2` | `367,278.7` | `433,107.5` | **3.89x** | **3.30x** |
| **File read** | `34,130.6` | `12,095.9` | `12,599.5` | **2.82x** | **2.71x** |
| **File removal** | `5,127.2` | `3,383.9` | `7,548.4` | **1.52x** | **0.68x** |
| **Tree creation** | `519.4` | `617.3` | `354.5` | **0.84x** | **1.47x** |
| **Tree removal** | `422.1` | `705.0` | `594.5` | **0.60x** | **0.71x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`1339.44 MiB/s`；同轮 JuiceFS(PG) 为 `694.71 MiB/s`。
- Bowl 单流顺序读：`622.02 MiB/s`；同轮 JuiceFS(PG) 为 `1326.85 MiB/s`。
- Bowl 16 并发顺序写：`1305.81 MiB/s`；同轮 JuiceFS(PG) 为 `678.20 MiB/s`。
- Bowl 16 并发顺序读：`1397.95 MiB/s`；同轮 JuiceFS(PG) 为 `1544.93 MiB/s`。

服务端本地 NVMe SSD 顺序读写与 4K IOPS 物理基线、请求大小与并发数见第 2、4 节。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`（及清理 `/var/jfsCache`），且三组存储栈服务端进程按目标隔离启停。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1679.99 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `1339.44 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

Bowl 的写入和同步路径使用单盘 `StoreActor` Pebble 与 `64 MiB` `.pack` 封包文件；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理客户端内核页缓存，但不清理 JuiceFS 客户端本地块缓存与服务端页缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的双端缓存清理不能改变 `juicefs bench` 单次运行内部这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0030 / ADR-0031 / ADR-0032 同时改变控制面协议、单盘 Actor 并发模型与 64 MiB Pack 数据层；单轮数据不能将变化归因于其中某一项。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261110-184512`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj919a36elzt54mhmlox`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj919a36elzt54mhmloy`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-10T18:45:20Z` |
| **实例销毁时间 (UTC)** | `2026-11-10T18:59:51Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
