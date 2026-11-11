# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费本地 NVMe SSD 双机环境（`ecs.i5.xlarge` × 2，1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机（4 vCPU / 32 GiB 内存）、VPC 私网与 894 GiB `local_ssd_pro` 本地 NVMe SSD 数据盘，服务端按目标隔离运行以下三组存储栈：

1. **Bowl（deploy/prod 容器栈（PostgreSQL 18 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack + Jaeger/Prometheus/Loki 全栈可观测））**：采用 ADR-0030 / ADR-0031 / ADR-0032 架构，PostgreSQL 仅保存单张 `disks` 表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在本地 NVMe SSD 的 `packs/<pack_id_hex16>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `WriteRequest` 池化零分配解码、活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。
   客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备对称的 `WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发 Write RPC 流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 顺序预读与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块本地 NVMe SSD 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）作为元数据引擎 + Server 节点本地 NVMe SSD rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,566.96 MiB/s` | `674.21 MiB/s` | `673.26 MiB/s` | **2.32x** | **2.33x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,296.25 MiB/s` | `1,590.09 MiB/s` | `1,598.71 MiB/s` | **0.82x** | **0.81x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `1,609.40 files/s` | `675.20 files/s` | `1,116.20 files/s` | **2.38x** | **1.44x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `3,932.00 files/s` | `2,904.20 files/s` | `4,925.50 files/s` | **1.35x** | **0.80x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `84,770.40 files/s` | `12,325.60 files/s` | `16,760.90 files/s` | **6.88x** | **5.06x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `1,340.31 MiB/s` | `673.24 MiB/s` | `673.91 MiB/s` | **1.99x** | **1.99x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `1,057.31 MiB/s` | `1,276.41 MiB/s` | `1,302.80 MiB/s` | **0.83x** | **0.81x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `1,545.81 MiB/s` | `673.60 MiB/s` | `662.86 MiB/s` | **2.29x** | **2.33x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `916.79 MiB/s` | `1,517.60 MiB/s` | `1,600.94 MiB/s` | **0.60x** | **0.57x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `3,398.76 ops/s` | `485.94 ops/s` | `2,014.16 ops/s` | **6.99x** | **1.69x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `3,060.34 ops/s` | `1,142.84 ops/s` | `1,942.62 ops/s` | **2.68x** | **1.58x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `89,193.73 ops/s` | `4,197.05 ops/s` | `14,179.36 ops/s` | **21.25x** | **6.29x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `3,522.94 ops/s` | `919.21 ops/s` | `1,846.04 ops/s` | **3.83x** | **1.91x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj929hjdmkt42qgmghox` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme1n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.193` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj929hjdmkt42qgmghoy` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.192` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.045/0.083/0.214/0.049` ms | `ping -c 20 -i 0.05 172.16.1.193` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.193 -t 5` |
| **Server 本地 NVMe SSD (894 GiB) 顺序写基线** | `1998.05` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 本地 NVMe SSD (894 GiB) 顺序读基线** | `3494.88` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69939` / `699175` IOPS | Server 本地 `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `50250` IOPS (P99 `64.4` µs) | Server 本地 `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `8afc63c` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | postgres (PostgreSQL) 18.6 控制面 / 单盘 `StoreActor` Pebble | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **部署编排与可观测性栈** | `deploy/prod (docker-compose.yaml + storage.env)` · `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` | systemd (`postgresql` + `jfs-s3`) | systemd (`redis-server` + `jfs-s3`) |
| **数据块存储 (Server 节点本地 NVMe SSD)** | 单盘 Pebble + `packs/<pack_id_hex16>.pack` `64 MiB` 封包文件 (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.193:9001/jfs-pg`) | rclone S3 (`http://172.16.1.193:9001/jfs-redis`) |
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
| **Write big file** | `1566.96 MiB/s (2.61 s/file)` | `674.21 MiB/s (6.08 s/file)` | `673.26 MiB/s (6.08 s/file)` | **2.32x** | **2.33x** |
| **Read big file** | `1296.25 MiB/s (3.16 s/file)` | `1590.09 MiB/s (2.58 s/file)` | `1598.71 MiB/s (2.56 s/file)` | **0.82x** | **0.81x** |
| **Write small file** | `1609.40 files/s (2.49 ms/file)` | `675.20 files/s (5.92 ms/file)` | `1116.20 files/s (3.58 ms/file)` | **2.38x** | **1.44x** |
| **Read small file** | `3932.00 files/s (1.02 ms/file)` | `2904.20 files/s (1.38 ms/file)` | `4925.50 files/s (0.81 ms/file)` | **1.35x** | **0.80x** |
| **Stat file** | `84770.40 files/s (0.05 ms/file)` | `12325.60 files/s (0.32 ms/file)` | `16760.90 files/s (0.24 ms/file)` | **6.88x** | **5.06x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `1340.31 MiB/s (335.1 IOPS)` | `673.24 MiB/s (168.3 IOPS)` | `673.91 MiB/s (168.5 IOPS)` | **1.99x** | **1.99x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `1057.31 MiB/s (264.3 IOPS)` | `1276.41 MiB/s (319.1 IOPS)` | `1302.80 MiB/s (325.7 IOPS)` | **0.83x** | **0.81x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `1545.81 MiB/s (386.4 IOPS)` | `673.60 MiB/s (168.4 IOPS)` | `662.86 MiB/s (165.7 IOPS)` | **2.29x** | **2.33x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `916.79 MiB/s (229.2 IOPS)` | `1517.60 MiB/s (379.4 IOPS)` | `1600.94 MiB/s (400.2 IOPS)` | **0.60x** | **0.57x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `1599.38 MiB/s (1599.4 IOPS)` | `725.08 MiB/s (725.1 IOPS)` | `722.91 MiB/s (722.9 IOPS)` | **2.21x** | **2.21x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `1420.74 MiB/s (1420.7 IOPS)` | `1545.66 MiB/s (1545.7 IOPS)` | `1568.75 MiB/s (1568.8 IOPS)` | **0.92x** | **0.91x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `1120.35 MiB/s (1120.3 IOPS)` | `343.83 MiB/s (343.8 IOPS)` | `342.42 MiB/s (342.4 IOPS)` | **3.26x** | **3.27x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `1057.58 MiB/s (1057.6 IOPS)` | `465.93 MiB/s (465.9 IOPS)` | `494.03 MiB/s (494.0 IOPS)` | **2.27x** | **2.14x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `2.45 / 3.10 / 5.28 ms` | `5.11 / 16.45 / 31.06 ms` | `5.11 / 17.96 / 32.64 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `3.78 / 13.70 / 16.91 ms` | `3.13 / 4.82 / 8.09 ms` | `3.06 / 4.82 / 6.65 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `39.55 / 88.61 / 115.87 ms` | `90.38 / 181.40 / 227.54 ms` | `92.24 / 183.50 / 229.64 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `69.48 / 104.33 / 121.11 ms` | `39.45 / 117.97 / 179.31 ms` | `39.03 / 110.62 / 166.72 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `2.46 / 14.88 / 24.51 ms` | `5.31 / 32.11 / 53.74 ms` | `5.39 / 27.39 / 52.17 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `2.80 / 20.32 / 37.49 ms` | `2.56 / 11.21 / 52.17 ms` | `2.52 / 10.55 / 50.07 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `3.56 / 5.28 / 6.46 ms` | `11.37 / 67.63 / 85.46 ms` | `11.60 / 83.36 / 85.46 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `3.76 / 5.93 / 10.29 ms` | `8.35 / 30.28 / 120.06 ms` | `8.02 / 34.87 / 98.04 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `3,398.8` | `485.9` | `2,014.2` | **6.99x** | **1.69x** |
| **Directory stat** | `87,714.7` | `4,178.5` | `13,609.2` | **20.99x** | **6.45x** |
| **Directory removal** | `3,183.8` | `797.5` | `1,671.9` | **3.99x** | **1.90x** |
| **File creation** | `3,060.3` | `1,142.8` | `1,942.6` | **2.68x** | **1.58x** |
| **File stat** | `89,193.7` | `4,197.1` | `14,179.4` | **21.25x** | **6.29x** |
| **File read** | `36,817.8` | `2,232.3` | `4,091.4` | **16.49x** | **9.00x** |
| **File removal** | `3,522.9` | `919.2` | `1,846.0` | **3.83x** | **1.91x** |
| **Tree creation** | `3,261.5` | `897.7` | `2,123.2` | **3.63x** | **1.54x** |
| **Tree removal** | `3,008.5` | `709.9` | `1,712.7` | **4.24x** | **1.76x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.i5.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `7,736.2` | `2,829.0` | `5,216.2` | **2.73x** | **1.48x** |
| **Directory stat** | `1,142,510.2` | `330,806.9` | `420,772.5` | **3.45x** | **2.72x** |
| **Directory removal** | `8,942.2` | `2,976.4` | `5,003.1` | **3.00x** | **1.79x** |
| **File creation** | `8,450.9` | `3,321.2` | `5,836.8` | **2.54x** | **1.45x** |
| **File stat** | `1,420,999.6` | `280,465.1` | `426,939.4` | **5.07x** | **3.33x** |
| **File read** | `34,161.4` | `12,793.8` | `15,902.0` | **2.67x** | **2.15x** |
| **File removal** | `10,688.9` | `3,185.0` | `6,747.6` | **3.36x** | **1.58x** |
| **Tree creation** | `1,807.0` | `360.9` | `343.9` | **5.01x** | **5.25x** |
| **Tree removal** | `1,660.1` | `646.3` | `542.8` | **2.57x** | **3.06x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`1340.31 MiB/s`；同轮 JuiceFS(PG) 为 `673.24 MiB/s`。
- Bowl 单流顺序读：`1057.31 MiB/s`；同轮 JuiceFS(PG) 为 `1276.41 MiB/s`。
- Bowl 16 并发顺序写：`1545.81 MiB/s`；同轮 JuiceFS(PG) 为 `673.60 MiB/s`。
- Bowl 16 并发顺序读：`916.79 MiB/s`；同轮 JuiceFS(PG) 为 `1517.60 MiB/s`。

服务端本地 NVMe SSD 顺序读写与 4K IOPS 物理基线、请求大小与并发数见第 2、4 节。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`（及清理 `/var/jfsCache`），且三组存储栈服务端进程按目标隔离启停。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1566.96 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `1340.31 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

Bowl 的写入和同步路径使用单盘 `StoreActor` Pebble 与 `64 MiB` `.pack` 封包文件；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理客户端内核页缓存，但不清理 JuiceFS 客户端本地块缓存与服务端页缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的双端缓存清理不能改变 `juicefs bench` 单次运行内部这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0030 / ADR-0031 / ADR-0032 同时改变控制面协议、单盘 Actor 并发模型与 64 MiB Pack 数据层；单轮数据不能将变化归因于其中某一项。

---

## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `1363` 条（跨 Client+Server 全链路 `962` 条，异常链路 `71` 条）· Loki 业务日志 `310` 条（服务端 Warn/Error `297` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `fio_direct_randread` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `4.516 s` (`4516.0 ms`) vs FUSE 累计活跃 `14559.92 ms`（等效并发度 `3.22x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`14559.92 ms` / RPC `14853.67 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `452.00 MiB` / 命中 `44.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 4,118 | 14559.92 | 3.536 | 3.285 | 12.706 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 14559.53 | 3.555 | 3.293 | 12.873 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 4 | 0.16 | 0.040 | 0.024 | 0.128 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 9 | 0.11 | 0.012 | 0.008 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 16 | 286.74 | 17.921 | 17.000 | 24.840 | 2.0% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 4,342 | 14853.67 | 3.421 | 3.418 | 16.542 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,221 | 14789.89 | 3.504 | 3.459 | 16.777 | 99.6% | 非 OK 状态 120 次 · 异步并发流水线 (累计为 FUSE 活跃 1.02x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,207 | 11811.92 | 2.808 | 3.095 | 12.603 | 79.5% | Read:2.796ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,210 | 9722.59 | 2.309 | 2.003 | 15.133 | 65.5% | HTTP Body+Codec 差值均值 1.601 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,207 | 2977.97 | 0.708 | 0.364 | 4.174 | 20.0% | 非 OK 状态 31 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 4,207 | 2977.97 | 0.708 | 0.364 | 4.174 | 20.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,176 | 2844.89 | 0.681 | 0.286 | 4.132 | 19.2% | read(4176) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4,161 | 114.79 | 0.028 | 0.029 | 0.075 | 0.8% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 4,176 | 31.05 | 0.007 | 0.007 | 0.024 | 0.2% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 18,720 | 2571.94 | 0.137 | 0.040 | 2.073 | 17.3% | — |

#### 工作负载 `fio_direct_randwrite` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.783 s` (`10783.0 ms`) vs FUSE 累计活跃 `13678.61 ms`（等效并发度 `1.27x`）；占比基准：`FUSE 活跃总耗时` (`13678.61 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `13.00 MiB` / 直通 `4083.00 MiB` (Flush RPC `12`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 12,310 | 13678.61 | 1.111 | 0.005 | 7.584 | 100.0% | 错误返回 8192 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,096 | 13650.31 | 3.333 | 3.262 | 8.023 | 99.8% | 占 FUSE 99.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 4,096 | 26.61 | 0.006 | 0.005 | 0.028 | 0.2% | 占 FUSE 0.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 1.46 | 0.000 | 0.001 | 0.001 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 12 | 37.06 | 3.089 | 3.333 | 4.967 | 0.3% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,099 | 13490.33 | 3.291 | 3.460 | 9.001 | 98.6% | 非 OK 状态 4 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,099 | 8933.03 | 2.179 | 2.498 | 4.558 | 65.3% | Write:2.180ms, GetXattr:0.771ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,099 | 8219.70 | 2.005 | 1.965 | 4.937 | 60.1% | HTTP Body+Codec 差值均值 0.893 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,099 | 4557.30 | 1.112 | 0.962 | 4.443 | 33.3% | 非 OK 状态 4 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,095 | 4556.45 | 1.113 | 0.963 | 4.443 | 33.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:GetXattr` | 4 | 0.85 | 0.212 | 0.200 | 0.490 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,095 | 166.20 | 0.041 | 0.004 | 0.789 | 1.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4 | 0.27 | 0.067 | 0.075 | 0.100 | 0.0% | getxattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 8 | 0.25 | 0.032 | 0.025 | 0.099 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 3,865 | 93.97 | 0.024 | 0.019 | 0.073 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 3,865 | 461.33 | 0.119 | 0.008 | 1.518 | 3.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 3,865 | 76.56 | 0.020 | 0.018 | 0.050 | 0.6% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,095 | 2187.10 | 0.534 | 0.440 | 2.249 | 16.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2116.71 | 33.074 | 37.302 | 49.746 | 15.5% | — |

#### 工作负载 `fio_direct_seq` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.057 s` (`7057.0 ms`) vs FUSE 累计活跃 `18796.64 ms`（等效并发度 `2.66x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`18796.64 ms` / RPC `70835.65 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4087.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 16,473 | 18796.64 | 1.141 | 0.032 | 29.164 | 100.0% | 错误返回 8212 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 10385.01 | 2.535 | 0.109 | 49.744 | 55.2% | 占 FUSE 55.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,096 | 8288.13 | 2.023 | 0.117 | 29.810 | 44.1% | 占 FUSE 44.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 12 | 87.66 | 7.305 | 0.008 | 65.536 | 0.5% | 占 FUSE 0.5% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 808 | 7806.42 | 9.661 | 7.906 | 44.541 | 41.5% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 84.91 | 21.229 | 17.500 | 49.000 | 0.5% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 732 | 9789.11 | 13.373 | 8.684 | 49.019 | 52.1% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,079 | 70785.35 | 34.048 | 34.986 | 97.270 | 99.9% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 3.77x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,079 | 53272.96 | 25.624 | 28.034 | 48.239 | 75.2% | Read:27.553ms, Write:23.984ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,079 | 47878.20 | 23.029 | 22.711 | 84.364 | 67.6% | HTTP Body+Codec 差值均值 14.606 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,079 | 17512.39 | 8.423 | 6.953 | 49.031 | 24.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,032 | 14426.33 | 13.979 | 11.438 | 49.031 | 20.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,035 | 3085.17 | 2.981 | 2.560 | 9.928 | 4.4% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,040 | 1874.88 | 1.803 | 0.028 | 22.136 | 2.6% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 434 | 3483.48 | 8.026 | 6.667 | 36.437 | 4.9% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,039 | 3042.30 | 2.928 | 2.515 | 9.897 | 4.3% | read(1035), getattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,028 | 33.69 | 0.033 | 0.034 | 0.089 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,039 | 10.09 | 0.010 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 813 | 94.78 | 0.117 | 0.078 | 1.401 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 813 | 1270.99 | 1.563 | 0.017 | 16.289 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 813 | 18.86 | 0.023 | 0.018 | 0.077 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 16,400 | 2926.54 | 0.178 | 0.044 | 2.297 | 4.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,095 | 2213.28 | 2.021 | 1.810 | 4.913 | 3.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2154.56 | 33.665 | 37.302 | 49.746 | 3.0% | — |

#### 工作负载 `fio_seq_16jobs` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `31.755 s` (`31755.0 ms`) vs FUSE 累计活跃 `577306.14 ms`（等效并发度 `18.18x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`577306.14 ms` / RPC `1118809.11 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `16352.00 MiB` / 直通 `32.00 MiB` (Flush RPC `4096`) · ReadAhead 预读 `27680.00 MiB` / 命中 `15237.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 102,757 | 577306.14 | 5.618 | 0.038 | 114.338 | 100.0% | 错误返回 20560 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 65,536 | 436167.21 | 6.655 | 0.041 | 118.676 | 75.6% | 占 FUSE 75.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 16,384 | 137191.41 | 8.373 | 0.204 | 110.440 | 23.8% | 占 FUSE 23.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 16 | 3869.31 | 241.832 | 262.144 | 524.288 | 0.7% | 占 FUSE 0.7% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 3,643 | 134095.52 | 36.809 | 33.780 | 178.099 | 23.2% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 16 | 1484.56 | 92.785 | 100.000 | 247.000 | 0.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 3,744 | 171019.92 | 45.678 | 46.970 | 130.511 | 29.6% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 11,149 | 1118510.72 | 100.324 | 109.235 | 345.970 | 100.0% | 非 OK 状态 4 次 · 异步并发流水线 (累计为 FUSE 活跃 1.94x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 11,148 | 1039548.17 | 93.250 | 105.151 | 160.470 | 92.9% | Write:143.673ms, Read:64.151ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 11,148 | 457042.56 | 40.998 | 44.395 | 247.600 | 40.9% | HTTP Body+Codec 差值均值 33.915 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 11,148 | 78962.55 | 7.083 | 4.087 | 247.600 | 7.1% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,128 | 64183.15 | 15.548 | 8.935 | 185.500 | 5.7% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 6,956 | 13412.67 | 1.928 | 0.949 | 9.712 | 1.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,176 | 11789.15 | 2.823 | 0.051 | 41.325 | 1.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 435 | 10753.45 | 24.721 | 9.683 | 215.658 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 6,972 | 13131.10 | 1.883 | 0.929 | 9.694 | 1.2% | read(6956), getattr(16) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 6,921 | 201.11 | 0.029 | 0.029 | 0.087 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,988 | 66.60 | 0.010 | 0.008 | 0.027 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,625 | 304.74 | 0.116 | 0.082 | 0.918 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,625 | 4397.22 | 1.675 | 0.021 | 15.156 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,621 | 82.84 | 0.032 | 0.022 | 0.187 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 110,752 | 12466.51 | 0.113 | 0.042 | 1.954 | 1.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,381 | 8750.56 | 1.997 | 1.792 | 4.914 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 3 | 69.77 | 23.258 | 7.500 | 98.500 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 4 | 139.97 | 34.991 | 50.000 | 99.000 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 4.22 | 4.216 | 3.750 | 4.975 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 257 | 8716.53 | 33.916 | 37.500 | 49.846 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 4 | 0.02 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 17.82 | 5.940 | 8.297 | 9.279 | 0.0% | — |

#### 工作负载 `fio_seq_read_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.773 s` (`10773.0 ms`) vs FUSE 累计活跃 `3120.88 ms`（等效并发度 `0.29x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`3120.88 ms` / RPC `12657.49 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `4816.00 MiB` / 命中 `4050.25 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 16,391 | 3120.88 | 0.190 | 0.028 | 8.722 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 16,384 | 3120.70 | 0.190 | 0.028 | 8.722 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 3 | 0.09 | 0.029 | 0.032 | 0.064 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetAttr` | 1 | 0.04 | 0.035 | 0.064 | 0.064 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 193 | 579.41 | 3.002 | 1.891 | 23.070 | 18.6% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 1,211 | 12657.49 | 10.452 | 10.160 | 24.727 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,210 | 12627.55 | 10.436 | 10.074 | 24.726 | 99.8% | 非 OK 状态 3 次 · 异步并发流水线 (累计为 FUSE 活跃 4.06x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,210 | 10448.23 | 8.635 | 8.326 | 19.773 | 82.5% | Read:8.635ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,210 | 9204.56 | 7.607 | 7.634 | 24.127 | 72.7% | HTTP Body+Codec 差值均值 5.806 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,210 | 2179.32 | 1.801 | 1.748 | 4.953 | 17.2% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,210 | 2179.32 | 1.801 | 1.748 | 4.953 | 17.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,210 | 2129.34 | 1.760 | 1.735 | 4.927 | 16.8% | read(1210) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,207 | 27.66 | 0.023 | 0.020 | 0.076 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,211 | 9.61 | 0.008 | 0.008 | 0.024 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 19,313 | 2013.78 | 0.104 | 0.039 | 2.042 | 15.9% | — |

#### 工作负载 `fio_seq_write_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.764 s` (`10764.0 ms`) vs FUSE 累计活跃 `503.18 ms`（等效并发度 `0.05x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`503.18 ms` / RPC `8461.31 ms`)
- **缓存与流水线命中**：NSCache Attr `99.98%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4094.00 MiB` / 直通 `2.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 9,234 | 503.18 | 0.054 | 0.006 | 0.252 | 100.0% | 错误返回 5125 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,096 | 455.60 | 0.111 | 0.086 | 1.004 | 90.5% | 占 FUSE 90.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 1 | 36.02 | 36.024 | 65.536 | 65.536 | 7.2% | 占 FUSE 7.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 1,025 | 8.62 | 0.008 | 0.006 | 0.041 | 1.7% | 占 FUSE 1.7% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 89 | 117.73 | 1.323 | 0.837 | 18.325 | 23.4% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 6.12 | 6.119 | 7.500 | 9.950 | 1.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,030 | 8443.88 | 8.198 | 8.194 | 49.750 | 99.8% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 16.82x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,030 | 5011.90 | 4.866 | 5.680 | 14.349 | 59.2% | Write:4.883ms, Create:0.498ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,030 | 6315.37 | 6.131 | 6.014 | 49.750 | 74.6% | HTTP Body+Codec 差值均值 2.799 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,030 | 3431.97 | 3.332 | 2.515 | 49.750 | 40.6% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,026 | 3402.35 | 3.316 | 2.488 | 9.961 | 40.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 1 | 29.40 | 29.398 | 37.500 | 49.750 | 0.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,029 | 3.81 | 0.004 | 0.003 | 0.022 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 307 | 163.38 | 0.532 | 0.189 | 4.983 | 1.9% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1 | 0.01 | 0.006 | 0.008 | 0.010 | 0.0% | getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 1,029 | 63.65 | 0.062 | 0.061 | 0.229 | 0.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 1,029 | 521.15 | 0.506 | 0.012 | 4.143 | 6.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,028 | 31.12 | 0.030 | 0.017 | 0.046 | 0.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,026 | 2075.17 | 2.023 | 1.845 | 4.863 | 24.5% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 1 | 3.13 | 3.133 | 3.750 | 4.975 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 1 | 26.38 | 26.384 | 37.500 | 49.750 | 0.3% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 2.78 | 2.782 | 3.750 | 4.975 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 65 | 2158.22 | 33.203 | 37.305 | 49.746 | 25.5% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 2 | 17.42 | 8.710 | 3.100 | 14.767 | 0.2% | — |

#### 工作负载 `juicefs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.110 s` (`7110.0 ms`) vs FUSE 累计活跃 `18083.62 ms`（等效并发度 `2.54x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`18083.62 ms` / RPC `69579.71 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.94%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `58.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4864.00 MiB` / 命中 `4072.12 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 52,868 | 18083.62 | 0.342 | 0.015 | 10.756 | 100.0% | 错误返回 9420 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 10812.11 | 0.326 | 0.019 | 7.048 | 59.8% | 占 FUSE 59.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,503 | 6766.40 | 1.503 | 0.120 | 25.836 | 37.4% | 占 FUSE 37.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 404 | 213.65 | 0.529 | 0.447 | 2.926 | 1.2% | 占 FUSE 1.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 684 | 5838.10 | 8.535 | 7.132 | 38.600 | 32.3% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 54.12 | 13.530 | 8.333 | 49.000 | 0.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 207 | 2317.73 | 11.197 | 7.500 | 49.930 | 12.8% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 3,894 | 69579.71 | 17.868 | 18.900 | 95.837 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,893 | 69511.08 | 17.855 | 18.880 | 95.837 | 99.9% | 非 OK 状态 25 次 · 异步并发流水线 (累计为 FUSE 活跃 3.85x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,893 | 55590.43 | 14.280 | 16.691 | 48.379 | 79.9% | Read:17.953ms, Write:17.994ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,893 | 44038.92 | 11.312 | 10.281 | 76.133 | 63.3% | HTTP Body+Codec 差值均值 7.736 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,893 | 13920.66 | 3.576 | 2.189 | 47.458 | 20.0% | 非 OK 状态 2 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 12739.42 | 8.896 | 5.101 | 47.458 | 18.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,648 | 1103.49 | 0.670 | 0.704 | 2.399 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 1523.57 | 0.680 | 0.003 | 15.391 | 2.2% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 342 | 2599.29 | 7.600 | 6.477 | 24.941 | 3.7% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,651 | 1044.54 | 0.633 | 0.691 | 2.363 | 1.5% | read(1647), readdirplus(1), lookup(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,239 | 21.48 | 0.017 | 0.018 | 0.047 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,456 | 21.48 | 0.009 | 0.007 | 0.041 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,045 | 34.67 | 0.017 | 0.013 | 0.086 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,045 | 932.46 | 0.456 | 0.005 | 9.635 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,045 | 38.12 | 0.019 | 0.009 | 0.048 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 19,812 | 931.97 | 0.047 | 0.043 | 0.101 | 1.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,433 | 2207.02 | 1.540 | 1.527 | 4.810 | 3.2% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2184.95 | 34.140 | 37.500 | 49.750 | 3.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 7.48 | 2.493 | 3.150 | 4.097 | 0.0% | — |

#### 工作负载 `mdtest_1p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `17.709 s` (`17709.0 ms`) vs FUSE 累计活跃 `14178.06 ms`（等效并发度 `0.80x`）；占比基准：`FUSE 活跃总耗时` (`14178.06 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.41%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 264,902 | 14178.06 | 0.054 | 0.003 | 0.483 | 100.0% | 错误返回 26439 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 13,996 | 3808.35 | 0.272 | 0.267 | 0.509 | 26.9% | 占 FUSE 26.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 13,997 | 3540.03 | 0.253 | 0.231 | 0.507 | 25.0% | 占 FUSE 25.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 12,440 | 3294.56 | 0.265 | 0.255 | 0.509 | 23.2% | 占 FUSE 23.2% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 52,875 | 12721.27 | 0.241 | 0.209 | 0.995 | 89.7% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 52,875 | 9994.19 | 0.189 | 0.161 | 0.746 | 70.5% | Rmdir:0.194ms, Mkdir:0.187ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 52,875 | 4401.16 | 0.083 | 0.080 | 0.249 | 31.0% | HTTP Body+Codec 差值均值 0.031 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 52,875 | 2727.07 | 0.052 | 0.048 | 0.249 | 19.2% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 13,996 | 827.75 | 0.059 | 0.056 | 0.224 | 5.8% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 13,997 | 697.48 | 0.050 | 0.047 | 0.179 | 4.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 52,873 | 64.56 | 0.001 | 0.003 | 0.006 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.04 | 0.013 | 0.015 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.03 | 0.011 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 52,873 | 773.49 | 0.015 | 0.014 | 0.049 | 5.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 52,873 | 76.15 | 0.001 | 0.003 | 0.005 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 52,873 | 299.71 | 0.006 | 0.004 | 0.024 | 2.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.60 | 0.602 | 0.650 | 0.699 | 0.0% | — |

#### 工作负载 `mdtest_4p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.777 s` (`10777.0 ms`) vs FUSE 累计活跃 `1771.14 ms`（等效并发度 `0.16x`）；占比基准：`FUSE 活跃总耗时` (`1771.14 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.95%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `97.02%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 17,957 | 1771.14 | 0.099 | 0.003 | 0.801 | 100.0% | 错误返回 2136 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 1,094 | 469.03 | 0.429 | 0.393 | 1.229 | 26.5% | 占 FUSE 26.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 1,093 | 445.01 | 0.407 | 0.390 | 2.389 | 25.1% | 占 FUSE 25.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 1,040 | 405.64 | 0.390 | 0.393 | 0.981 | 22.9% | 占 FUSE 22.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,269 | 1567.89 | 0.367 | 0.369 | 1.816 | 88.5% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,269 | 1302.87 | 0.305 | 0.313 | 1.570 | 73.6% | Mkdir:0.341ms, Rmdir:0.309ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,269 | 496.73 | 0.116 | 0.114 | 0.456 | 28.0% | HTTP Body+Codec 差值均值 0.054 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,269 | 265.02 | 0.062 | 0.056 | 0.249 | 15.0% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 1,093 | 77.15 | 0.071 | 0.064 | 0.246 | 4.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 1,094 | 71.91 | 0.066 | 0.059 | 0.247 | 4.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,267 | 17.02 | 0.004 | 0.003 | 0.043 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.04 | 0.012 | 0.015 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.03 | 0.010 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,166 | 58.79 | 0.014 | 0.014 | 0.049 | 3.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,166 | 5.86 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,166 | 21.93 | 0.005 | 0.004 | 0.022 | 1.2% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.26 | 0.255 | 0.250 | 0.299 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `99.99%` / Entry `99.99%` / Readlink `N/A`（负缓存命中 `29,065`）；WriteBehind 聚批 `28635.00 MiB` / 直通 `4183.00 MiB`（Flush RPC `7,180`，背压等待 `147857.77 ms`，屏障等待 `1666.78 ms`）；ReadAhead 预读 `41908.00 MiB`（命中 `27490.38 MiB`，等待 `183992.91 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `Read` | 123,297 | 475044.56 | 71.8% | 3852.8 | 31.4 | 33173.8 | 107395.4 |
| `Write` | 33,175 | 166351.85 | 25.1% | 5014.4 | 186.5 | 37476.7 | 87785.9 |
| `Rmdir` | 15,093 | 4255.36 | 0.6% | 281.9 | 281.1 | 493.9 | 611.5 |
| `Mkdir` | 15,093 | 4012.58 | 0.6% | 265.9 | 238.7 | 486.8 | 511.6 |
| `Create` | 13,905 | 3921.80 | 0.6% | 282.0 | 278.8 | 497.6 | 802.5 |
| `Fsync` | 17 | 3905.33 | 0.6% | 229725.4 | 262144.0 | 524288.0 | 524288.0 |
| `Unlink` | 13,905 | 3519.58 | 0.5% | 253.1 | 218.7 | 473.5 | 509.1 |
| `Lookup` | 79,098 | 228.64 | 0.0% | 2.9 | 2.5 | 7.1 | 12.7 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Read` | 15,059 | 410047.15 | 27.229 | 22.378 | 91.105 | 98.288 |
| `http_outer:Write` | 11,713 | 170846.36 | 14.586 | 8.456 | 47.336 | 111.750 |
| `http_outer:Rmdir` | 15,093 | 1413.71 | 0.094 | 0.086 | 0.229 | 0.248 |
| `http_outer:Fsync` | 17 | 1399.42 | 82.319 | 122.500 | 237.250 | 247.450 |
| `rpc:Write` | 11,713 (11713/0) | 99307.69 | 8.478 | 3.161 | 34.835 | 92.232 |
| `rpc:Read` | 15,056 (15024/32) | 22758.62 | 1.512 | 0.896 | 5.065 | 9.294 |
| `rpc:Fsync` | 17 (17/0) | 1393.84 | 81.990 | 122.500 | 237.250 | 247.450 |
| `rpc:Rmdir` | 15,093 (15093/0) | 905.41 | 0.060 | 0.057 | 0.128 | 0.228 |
| `rpc:Mkdir` | 15,093 (15093/0) | 769.57 | 0.051 | 0.047 | 0.097 | 0.198 |
| `rpc:Create` | 13,905 (13905/0) | 765.14 | 0.055 | 0.050 | 0.099 | 0.221 |
| `actor_phase:phase_b_mutate` | 67,442 | 7683.25 | 0.114 | 0.003 | 0.019 | 3.997 |
| `actor_phase:phase_a_validate` | 67,442 | 1447.37 | 0.021 | 0.016 | 0.062 | 0.166 |
| `actor_phase:wal_apply_nosync` | 67,436 | 587.22 | 0.009 | 0.005 | 0.023 | 0.045 |
| `actor_phase:pack_sync_barrier` | 5 | 166.35 | 33.270 | 43.750 | 93.750 | 98.750 |
| `actor_phase:wal_apply_sync` | 5 | 73.08 | 14.616 | 3.750 | 87.500 | 97.500 |
| `actor_phase:dead_pack_remove` | 2 | 7.00 | 3.499 | 3.750 | 4.875 | 4.975 |
| `read_op:read` | 15,024 | 22191.78 | 1.477 | 0.877 | 4.988 | 9.263 |
| `read_op:readdirplus` | 6 | 0.91 | 0.152 | 0.150 | 0.425 | 0.485 |
| `read_op:getxattr` | 4 | 0.27 | 0.067 | 0.075 | 0.098 | 0.100 |
| `read_op:getattr` | 28 | 0.17 | 0.006 | 0.004 | 0.021 | 0.024 |
| `kv_read:prefix_scan` | 14,561 | 399.11 | 0.027 | 0.026 | 0.049 | 0.082 |
| `kv_read:point_get` | 15,901 | 139.37 | 0.009 | 0.008 | 0.022 | 0.028 |
| `blob_pread` | 184,997 | 20910.74 | 0.113 | 0.042 | 0.600 | 1.970 |
| `blob_pwrite` | 12,030 | 17433.12 | 1.449 | 1.310 | 3.479 | 4.783 |

Pebble BlockCache 命中率 `99.04%` · ReadAmp `1.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `66bdcb02f4c4cc45` | 374.20 | `bowl-client:fuse.Fsync` | 130.88 | 114.41 | 16.47 | `bowl-client:write_behind.BarrierWait` (134.34 ms) |
| `6d89f3e2db04874b` | 363.70 | `bowl-client:fuse.Fsync` | 129.78 | 116.06 | 13.72 | `bowl-client:write_behind.BackpressureWait` (138.86 ms) |
| `090cf5cbbd22ce4f` | 347.64 | `bowl-client:write_behind.FlushChunk` | 347.61 | 185.12 | 162.50 | `bowl-client:storage.v1.StorageService/Write` (347.61 ms) |
| `23dbdd566db32805` | 340.60 | `bowl-client:fuse.Fsync` | 136.67 | 124.57 | 12.10 | `bowl-client:storage.v1.StorageService/Fsync` (136.67 ms) |
| `d4c9e82b09bdfd8f` | 330.11 | `bowl-client:fuse.Fsync` | 76.17 | 64.08 | 12.08 | `bowl-client:write_behind.BarrierWait` (229.50 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `fio_direct_randwrite_cpu.pb.gz` | `4.43s 60.60% 60.60%      4.43s 60.60%  internal/runtime/syscall/linux.Syscall6` | `4.43s 60.60% 60.60%      4.43s 60.60%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_fio_direct_randwrite_cpu.txt`](results/telemetry/pprof_top/server_fio_direct_randwrite_cpu.txt) |
| `server` | `fio_direct_randwrite_heap.pb.gz` | `256.09MB 55.95% 55.95%   256.09MB 55.95%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%   424.49MB 92.74%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_fio_direct_randwrite_heap.txt`](results/telemetry/pprof_top/server_fio_direct_randwrite_heap.txt) |
| `server` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_fio_direct_randwrite_mutex.txt`](results/telemetry/pprof_top/server_fio_direct_randwrite_mutex.txt) |
| `server` | `fio_seq_read_1job_cpu.pb.gz` | `3.36s 56.00% 56.00%      3.36s 56.00%  internal/runtime/syscall/linux.Syscall6` | `3.36s 56.00% 56.00%      3.36s 56.00%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_fio_seq_read_1job_cpu.txt`](results/telemetry/pprof_top/server_fio_seq_read_1job_cpu.txt) |
| `server` | `fio_seq_read_1job_heap.pb.gz` | `96MB 52.33% 52.33%       96MB 52.33%  bytes.growSlice` | `0     0%     0%   165.54MB 90.23%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_fio_seq_read_1job_heap.txt`](results/telemetry/pprof_top/server_fio_seq_read_1job_heap.txt) |
| `server` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_fio_seq_read_1job_mutex.txt`](results/telemetry/pprof_top/server_fio_seq_read_1job_mutex.txt) |
| `server` | `fio_seq_write_1job_cpu.pb.gz` | `4.03s 66.17% 66.17%      4.03s 66.17%  internal/runtime/syscall/linux.Syscall6` | `4.03s 66.17% 66.17%      4.03s 66.17%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_fio_seq_write_1job_cpu.txt`](results/telemetry/pprof_top/server_fio_seq_write_1job_cpu.txt) |
| `server` | `fio_seq_write_1job_heap.pb.gz` | `96MB 52.33% 52.33%       96MB 52.33%  bytes.growSlice` | `0     0%     0%   165.54MB 90.23%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_fio_seq_write_1job_heap.txt`](results/telemetry/pprof_top/server_fio_seq_write_1job_heap.txt) |
| `server` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_fio_seq_write_1job_mutex.txt`](results/telemetry/pprof_top/server_fio_seq_write_1job_mutex.txt) |
| `server` | `mdtest_1p_cpu.pb.gz` | `0.78s 11.49% 11.49%      0.78s 11.49%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.31s 34.02%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_mdtest_1p_cpu.txt`](results/telemetry/pprof_top/server_mdtest_1p_cpu.txt) |
| `server` | `mdtest_1p_heap.pb.gz` | `50.63MB 39.62% 39.62%    50.63MB 39.62%  github.com/cockroachdb/pebble/v2/internal/manual.New (inline)` | `0     0%     0%    67.10MB 52.52%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_mdtest_1p_heap.txt`](results/telemetry/pprof_top/server_mdtest_1p_heap.txt) |
| `server` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_mdtest_1p_mutex.txt`](results/telemetry/pprof_top/server_mdtest_1p_mutex.txt) |
| `server` | `mdtest_4p_cpu.pb.gz` | `110ms 12.79% 12.79%      110ms 12.79%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      310ms 36.05%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_mdtest_4p_cpu.txt`](results/telemetry/pprof_top/server_mdtest_4p_cpu.txt) |
| `server` | `mdtest_4p_heap.pb.gz` | `50.63MB 39.59% 39.59%    50.63MB 39.59%  github.com/cockroachdb/pebble/v2/internal/manual.New (inline)` | `0     0%     0%    66.04MB 51.65%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_mdtest_4p_heap.txt`](results/telemetry/pprof_top/server_mdtest_4p_heap.txt) |
| `server` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_mdtest_4p_mutex.txt`](results/telemetry/pprof_top/server_mdtest_4p_mutex.txt) |
| `client` | `fio_direct_randwrite_cpu.pb.gz` | `1.36s 39.65% 39.65%      1.36s 39.65%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.82s 53.06%  net/http.(*http2clientStream).doRequest` | [`pprof_top/client_fio_direct_randwrite_cpu.txt`](results/telemetry/pprof_top/client_fio_direct_randwrite_cpu.txt) |
| `client` | `fio_direct_randwrite_heap.pb.gz` | `332.57MB 44.12% 44.12%   332.57MB 44.12%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   402.73MB 53.42%  sync.(*Pool).Get` | [`pprof_top/client_fio_direct_randwrite_heap.txt`](results/telemetry/pprof_top/client_fio_direct_randwrite_heap.txt) |
| `client` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_fio_direct_randwrite_mutex.txt`](results/telemetry/pprof_top/client_fio_direct_randwrite_mutex.txt) |
| `client` | `fio_seq_read_1job_cpu.pb.gz` | `2.25s 43.95% 43.95%      2.25s 43.95%  internal/runtime/syscall/linux.Syscall6` | `2.25s 43.95% 43.95%      2.25s 43.95%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/client_fio_seq_read_1job_cpu.txt`](results/telemetry/pprof_top/client_fio_seq_read_1job_cpu.txt) |
| `client` | `fio_seq_read_1job_heap.pb.gz` | `128MB 45.40% 45.40%      128MB 45.40%  bytes.growSlice` | `0     0%     0%   217.03MB 76.98%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_fio_seq_read_1job_heap.txt`](results/telemetry/pprof_top/client_fio_seq_read_1job_heap.txt) |
| `client` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_fio_seq_read_1job_mutex.txt`](results/telemetry/pprof_top/client_fio_seq_read_1job_mutex.txt) |
| `client` | `fio_seq_write_1job_cpu.pb.gz` | `1.53s 45.13% 45.13%      1.53s 45.13%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.55s 45.72%  syscall.Syscall` | [`pprof_top/client_fio_seq_write_1job_cpu.txt`](results/telemetry/pprof_top/client_fio_seq_write_1job_cpu.txt) |
| `client` | `fio_seq_write_1job_heap.pb.gz` | `95.15MB 34.38% 34.38%    95.15MB 34.38%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   159.74MB 57.71%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_fio_seq_write_1job_heap.txt`](results/telemetry/pprof_top/client_fio_seq_write_1job_heap.txt) |
| `client` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_fio_seq_write_1job_mutex.txt`](results/telemetry/pprof_top/client_fio_seq_write_1job_mutex.txt) |
| `client` | `mdtest_1p_cpu.pb.gz` | `0.73s 19.73% 19.73%      0.73s 19.73%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.05s 55.41%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_mdtest_1p_cpu.txt`](results/telemetry/pprof_top/client_mdtest_1p_cpu.txt) |
| `client` | `mdtest_1p_heap.pb.gz` | `335.54MB 29.84% 29.84%   335.54MB 29.84%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   733.09MB 65.19%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_mdtest_1p_heap.txt`](results/telemetry/pprof_top/client_mdtest_1p_heap.txt) |
| `client` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_mdtest_1p_mutex.txt`](results/telemetry/pprof_top/client_mdtest_1p_mutex.txt) |
| `client` | `mdtest_4p_cpu.pb.gz` | `120ms 21.82% 21.82%      120ms 21.82%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      330ms 60.00%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_mdtest_4p_cpu.txt`](results/telemetry/pprof_top/client_mdtest_4p_cpu.txt) |
| `client` | `mdtest_4p_heap.pb.gz` | `335.54MB 29.84% 29.84%   335.54MB 29.84%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   733.09MB 65.19%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_mdtest_4p_heap.txt`](results/telemetry/pprof_top/client_mdtest_4p_heap.txt) |
| `client` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_mdtest_4p_mutex.txt`](results/telemetry/pprof_top/client_mdtest_4p_mutex.txt) |

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261111-190848`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj929hjdmkt42qgmghox`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj929hjdmkt42qgmghoy`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-11T19:08:55Z` |
| **实例销毁时间 (UTC)** | `2026-11-11T19:24:03Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |

---

## 9. 基线 → R1 → R2 → R3 演进对比与全链路瓶颈归因实证

### 9.1 四轮核心指标演进总表（同规格 `us-west-1a` `ecs.i5.xlarge` × 2）

| 测试项 | 历史基线 (`latest_run`) | R1 (`4e977c1`) | R2 (`8bf2b8b`) | **R3 终验 (`8afc63c`)** | 同轮 `jfs_pg` / `jfs_redis` | 演进幅度与全链路遥测归因 |
| :--- | ---: | ---: | ---: | ---: | ---: | :--- |
| **`mdtest_1proc` Dir Create** | `389.6 ops/s` | `2,718.1 ops/s` | `2,521.0 ops/s` | **`3,398.8 ops/s`** | `485.9 / 2,014.2` | **较基线 `8.72x`、较 R2 `+34.8%`（`6.99x` PG）** |
| **`mdtest_1proc` File Create** | `295.0 ops/s` | `2,230.3 ops/s` | `1,968.3 ops/s` | **`3,060.3 ops/s`** | `1,142.8 / 1,942.6` | **较基线 `10.37x`、较 R2 `+55.5%`（`2.68x` PG）** |
| **`mdtest_1proc` File Removal** | `296.9 ops/s` | `2,256.6 ops/s` | `2,176.2 ops/s` | **`3,522.9 ops/s`** | `919.2 / 1,846.0` | **较基线 `11.87x`、较 R2 `+61.9%`（`3.83x` PG）** |
| **`mdtest_4proc` Create Dir / File / Rm** | `6,753 / 6,073 / 7,610` | `7,151 / 7,604 / 9,062` | `6,638 / 6,956 / 8,400` | **`7,736 / 8,451 / 10,689`** | Redis: `5,216 / 5,837 / 6,748` | **较 R2 再涨 `+16.5% ~ +27.3%`，全面领先 PG/Redis** |
| **JFS `Write big file` (1GiB×4)** | `1,679.99 MiB/s` | `1,751.10 MiB/s` | `1,654.93 MiB/s` | **`1,566.96 MiB/s`** | `674.21 / 673.26` | **`2.32x` PG / `2.33x` Redis**（`WriteBehind` `4 MiB × 4`） |
| **JFS `Read big file` (1GiB×4)** | `1,403.37 MiB/s` | `1,431.46 MiB/s` | `1,521.88 MiB/s` | **`1,296.25 MiB/s`** | `1,590.09 / 1,598.71` | `0.82x` PG（4 并发流触发全局 12 槽预读配额收敛） |
| **JFS `Write small file` (128KiB×400)** | `790.7 files/s` | `1,778.6 files/s` | `1,700.7 files/s` | **`1,609.4 files/s`** | `675.2 / 1,116.2` | **较基线 `2.04x`（`2.38x` PG / `1.44x` Redis）** |
| **JFS `Read small file` (128KiB×400)** | `967.2 files/s` | `3,653.9 files/s` | `3,568.4 files/s` | **`3,932.0 files/s`** | `2,904.2 / 4,925.5` | **较基线 `4.07x`、较 R2 `+10.2%`（`1.35x` PG）** |
| **JFS `Stat file` (400 files)** | `93,191.4 files/s` | `106,926.8 files/s` | `29,950.2 files/s` | **`84,770.4 files/s`** | `12,325.6 / 16,760.9` | **`6.88x` PG / `5.06x` Redis** |
| **fio `seq_write_1job` (4M·4GiB)** | `1,339.44 MiB/s` | `1,338.12 MiB/s` | `1,296.20 MiB/s` | **`1,340.31 MiB/s`** | `673.24 / 673.91` | **`1.99x` PG / `1.99x` Redis**，FUSE 活跃仅 `503.2 ms` |
| **fio `seq_read_1job` (4M·4GiB)** | `622.02 MiB/s` | `669.39 MiB/s` | `1,180.74 MiB/s` | **`1,057.31 MiB/s`** | `1,276.41 / 1,302.80` | **较基线 `+70.0%` (`1.70x`)、较 R1 `+58.0%`**；`ReadAhead W_read=4` 命中 `4,050.25 MiB`、`fuse.Read` P50 `28 µs` |
| **fio `seq_write_16jobs` (4M·16GiB)** | `1,305.81 MiB/s` | `1,662.51 MiB/s` | `1,582.08 MiB/s` | **`1,545.81 MiB/s`** | `673.60 / 662.86` | **较基线 `+18.4%`，`2.29x` PG / `2.33x` Redis** |
| **fio `seq_read_16jobs` (4M·16GiB)** | `1,397.95 MiB/s` | `1,463.90 MiB/s` | `1,198.36 MiB/s` | **`916.79 MiB/s`** | `1,517.60 / 1,600.94` | 见 §9.3 逐跳定位：Server HTTP Mean 降 `-49.1%`，但 `targetDepth=1` 下 `popPrefetch` 解锁等待与并发 `fetchAlignedWindow` 重复拉取 `27.7 GiB` |
| **fio `direct_write_4jobs` (1M·4GiB)** | `1,719.56 MiB/s` | `1,808.39 MiB/s` | `1,686.29 MiB/s` | **`1,599.38 MiB/s`** | `725.08 / 722.91` | **`2.21x` PG / `2.21x` Redis** |
| **fio `direct_read_4jobs` (1M·4GiB)** | `1,370.36 MiB/s` | `1,379.59 MiB/s` | `1,608.17 MiB/s` | **`1,420.74 MiB/s`** | `1,545.66 / 1,568.75` | **较基线 `+3.7%`**（`0.92x` PG） |
| **fio `direct_randwrite_4jobs` (1M·4GiB)** | `748.13 MiB/s` | `1,207.19 MiB/s` | `1,162.98 MiB/s` | **`1,120.35 MiB/s`** | `343.83 / 342.42` | **较基线 `+49.8%` (`1.50x`)，`3.26x` PG / `3.27x` Redis** |
| **fio `direct_randread_4jobs` (1M·4GiB)** | `1,004.41 MiB/s` | `1,063.34 MiB/s` | `1,074.78 MiB/s` | **`1,057.58 MiB/s`** | `465.93 / 494.03` | **较基线 `+5.3%`，`2.27x` PG / `2.14x` Redis** |

### 9.2 可观测性驱动根除 `h2c` 双写 `tcp_autocorking` + Delayed ACK 双峰延迟闭环实证

1. **问题暴露（基线 & R2b 慢实例对）**：在历史 `latest_run.json` 基线（`mdtest_1proc` Create `295 ops/s`、`Read small file` `967 files/s`）及 R2 跨实例对照中，服务端新增的 `bowl_storage_http_request_duration_seconds`（HTTP `HEADERS` 到达即计时）与内层 `rpc_server_call_duration_seconds`（请求 Body `END_STREAM` 读完后计时）精准定界出 Go `http2ClientConn` 将小包 Unary POST 拆分为两次 `write()`（`HEADERS` + `DATA`），在 Linux `6.8` `tcp_autocorking=1` 下与服务端 TCP Delayed ACK 形成 `~9.1–10.1 ms` 互锁。
2. **R3 根除实证（`h2CoalesceConn` 连接层帧合并 + `tcp_autocorking=0`）**：
   - 全套件 **`58,059` 次**元数据小包 RPC（`Mkdir 15,093`、`Rmdir 15,093`、`Create 13,905`、`Unlink 13,905`、`GetAttr 22`、`Fallocate 21` 等）中，`bowl_storage_http_request_duration_seconds` 与内层 `rpc_server_call_duration_seconds` 在 `(5, 25] ms` 慢桶的差值为 **`0 / 58,059` (`0.000%`)**：其中 `Mkdir` `15,093 / 15,093` (`100.00%`) `<= 1.0 ms`（`99.95% <= 0.5 ms`），`Create` 与 `Rmdir` `100.00% <= 2.5 ms`（`99.9% <= 0.5 ms`）。
   - `mdtest_1p`（52,875 次串行 RPC）逐跳对比：`Server HTTP Body+Codec` 差值均值从 R2 快态的 `0.177 ms` 进一步骤降 **`-82.5%` 至 `0.031 ms`**（`http_outer` Mean 从 `0.230 ms` 降至 **`0.083 ms`**，P99 仅 **`0.249 ms`**），客户端 `rpc_client_call` Mean 从 `0.389 ms` 降至 **`0.241 ms`**（P50 `0.209 ms`，P99 `0.995 ms`），直接推动 `mdtest_1proc` 文件创建/删除吞吐跃升至 **`3,060 / 3,523 ops/s`**（基线的 **`10.4x ~ 11.9x`**）。

### 9.3 数据面流水线归因与下一步精准优化锚点

1. **单流顺序读流水线（`ReadAhead W_read=4`）**：`fio_seq_read_1job` 稳定在 `1,057 ~ 1,181 MiB/s`（较基线 `622 MiB/s` 提升 `+70% ~ +90%`），`fuse.Read` 16,384 次调用 P50 仅 `28 µs`，客户端与服务端 CPU Top-1 均为内核 `Syscall6`（`44%` / `56%`）。
2. **高并发多流顺序读（`fio_seq_16jobs`）残余瓶颈定位**：R3 引入 `ReadAheadMaxGlobalInflight = 12` 后，`fio_seq_16jobs` 阶段服务端 `http_outer` Mean 如期从 R2 的 `80.61 ms` 降至 **`41.00 ms`（`-49.1%`）**、客户端 RPC Mean 从 `154.75 ms` 降至 **`100.32 ms`（`-35.2%`）**；但 16 流下 `targetDepth` 收敛为 `1` 时，`ReadAhead 预读等待` 从 R2 的 `912` 次增至 `3,744` 次，暴露出客户端 `popPrefetchForOffsetLocked` 弹出 `pf` 并解锁等待 `pf.done` 期间未将 `pf` 挂载至单飞 `hra.fetch`，使同一句柄紧随其后的内核并发 readahead 线程落入 `fetchAlignedWindow` 重复发起同偏移 `4 MiB` RPC（导致 `rpc_server:Read` 从 `5,131` 次增至 `6,956` 次、预读总量从 `19,740 MiB` 升至 `27,680 MiB`）。后续仅需将等待中的 `pf` 桥接至单飞 `hra.fetch` 即可消除重复拉取。
3. **双端 HTTP Body 读取缓冲扩容（`pprof` `-flat` 实证）**：`server_fio_seq_read_1job_heap`（`MaxBytesHandler -> bytes.growSlice` `96 MB` / `52.33%`）与 `client_fio_seq_read_1job_heap`（`connect.CallUnary -> bytes.growSlice` `128 MB` / `45.40%`）显示 ConnectRPC 在 `Codec.Unmarshal` 前的 `io.ReadAll` 切片扩容仍是 `4 MiB` 大块传输在用户态的主要内存分配热点。
