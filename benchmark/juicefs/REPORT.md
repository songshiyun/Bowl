# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费本地 NVMe SSD 双机环境（`ecs.i5.xlarge` × 2，1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机（4 vCPU / 32 GiB 内存）、VPC 私网与 894 GiB `local_ssd_pro` 本地 NVMe SSD 数据盘，服务端按目标隔离运行以下三组存储栈：

1. **Bowl（deploy/prod 容器栈（PostgreSQL 18 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack + Jaeger/Prometheus/Loki 全栈可观测））**：采用 ADR-0030 / ADR-0031 / ADR-0032 架构，PostgreSQL 仅保存单张 `disks` 表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在本地 NVMe SSD 的 `packs/<pack_id_hex16>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与 `64 MiB` 分片热块缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `128 MiB` H2C 连接窗口、`WriteRequest`/`ReadResponse` 池化零分配编解码、活跃包末尾同块 `ExtendTail` 顺延扩展、`8 MiB` `SyncFileRange` 渐进异步回写、相邻物理连续块合并单次 `pread` 无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及后台异步全死封口包回收。
   客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备客户端 `1+8` H2C 多连接条带池（`32 MiB` 元数据连接 + `8×64 MiB` 数据连接，单流 `8 MiB`）与 `MarshalAppend` 零分配编码、`WriteBehindCoordinator`（大块流式写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发 Write RPC 流水线，小块连续写经首笔探针或 `Create` 预热后内存聚合）、`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4`、`ReadAheadMaxGlobalInflight=32`）与 `completeDirs` 本地目录流合成，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块本地 NVMe SSD 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）作为元数据引擎 + Server 节点本地 NVMe SSD rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `1,705.25 MiB/s` | `712.64 MiB/s` | `700.49 MiB/s` | **2.39x** | **2.43x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,904.12 MiB/s` | `1,696.46 MiB/s` | `1,612.45 MiB/s` | **1.12x** | **1.18x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `2,001.50 files/s` | `682.70 files/s` | `1,139.40 files/s` | **2.93x** | **1.76x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `4,390.70 files/s` | `3,209.00 files/s` | `5,206.20 files/s` | **1.37x** | **0.84x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `29,482.90 files/s` | `14,168.70 files/s` | `17,310.90 files/s` | **2.08x** | **1.70x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `1,453.51 MiB/s` | `713.59 MiB/s` | `705.72 MiB/s` | **2.04x** | **2.06x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `1,662.34 MiB/s` | `1,476.04 MiB/s` | `1,416.81 MiB/s` | **1.13x** | **1.17x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `1,612.92 MiB/s` | `707.61 MiB/s` | `696.98 MiB/s` | **2.28x** | **2.31x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `1,972.31 MiB/s` | `1,705.42 MiB/s` | `1,710.59 MiB/s` | **1.16x** | **1.15x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `3,928.74 ops/s` | `643.41 ops/s` | `2,100.46 ops/s` | **6.11x** | **1.87x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `3,564.46 ops/s` | `1,191.07 ops/s` | `2,072.16 ops/s` | **2.99x** | **1.72x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `105,621.03 ops/s` | `4,462.47 ops/s` | `14,871.32 ops/s` | **23.67x** | **7.10x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `4,116.35 ops/s` | `968.54 ops/s` | `1,951.00 ops/s` | **4.25x** | **2.11x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-<redacted-server>` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme1n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.199` |
| **Client 客户端** (`bowl-bench-client`) | `i-<redacted-client>` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.198` |

### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-<redacted>` / `vsw-<redacted>` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.041/0.078/0.258/0.055` ms | `ping -c 20 -i 0.05 172.16.1.199` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.199 -t 5` |
| **Server 本地 NVMe SSD (894 GiB) 顺序写基线** | `1998.05` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 本地 NVMe SSD (894 GiB) 顺序读基线** | `3494.88` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69961` / `699143` IOPS | Server 本地 `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `50592` IOPS (P99 `62.8` µs) | Server 本地 `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `6399122` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | postgres (PostgreSQL) 18.6 控制面 / 单盘 `StoreActor` Pebble | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **部署编排与可观测性栈** | `deploy/prod (docker-compose.yaml + storage.env)` · `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` | systemd (`postgresql` + `jfs-s3`) | systemd (`redis-server` + `jfs-s3`) |
| **数据块存储 (Server 节点本地 NVMe SSD)** | 单盘 Pebble + `packs/<pack_id_hex16>.pack` `64 MiB` 封包文件 (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.199:9001/jfs-pg`) | rclone S3 (`http://172.16.1.199:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **回写缓存与持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发写流水线 + 小块连续写聚合，`writeback_cache=false`）+ 客户端 `1+8` H2C 池 (`32M/8×64M`) / 服务端 `128M` 窗口 + 零分配编解码；Group Commit `pebble.NoSync` + `8 MiB` `SyncFileRange` 渐进回写 + Fsync 双层并行落盘屏障 | 客户端回写缓存 + 对象存储上传 | 客户端回写缓存 + 对象存储上传 |
| **数据分片** | `64 MiB` `.pack` Append-Only 封包 + `256 KiB` 逻辑块网格 + 连续块合并单次 `pread` + `AppendBlocks` 零冗余拷贝 + `64 MiB` 热块缓存 + `ExtendTail` | `4 MiB` block / `64 MiB` chunk (默认) | `4 MiB` block / `64 MiB` chunk (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `1705.25 MiB/s (2.40 s/file)` | `712.64 MiB/s (5.75 s/file)` | `700.49 MiB/s (5.85 s/file)` | **2.39x** | **2.43x** |
| **Read big file** | `1904.12 MiB/s (2.15 s/file)` | `1696.46 MiB/s (2.41 s/file)` | `1612.45 MiB/s (2.54 s/file)` | **1.12x** | **1.18x** |
| **Write small file** | `2001.50 files/s (2.00 ms/file)` | `682.70 files/s (5.86 ms/file)` | `1139.40 files/s (3.51 ms/file)` | **2.93x** | **1.76x** |
| **Read small file** | `4390.70 files/s (0.91 ms/file)` | `3209.00 files/s (1.25 ms/file)` | `5206.20 files/s (0.77 ms/file)` | **1.37x** | **0.84x** |
| **Stat file** | `29482.90 files/s (0.14 ms/file)` | `14168.70 files/s (0.28 ms/file)` | `17310.90 files/s (0.23 ms/file)` | **2.08x** | **1.70x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `1453.51 MiB/s (363.4 IOPS)` | `713.59 MiB/s (178.4 IOPS)` | `705.72 MiB/s (176.4 IOPS)` | **2.04x** | **2.06x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `1662.34 MiB/s (415.6 IOPS)` | `1476.04 MiB/s (369.0 IOPS)` | `1416.81 MiB/s (354.2 IOPS)` | **1.13x** | **1.17x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `1612.92 MiB/s (403.2 IOPS)` | `707.61 MiB/s (176.9 IOPS)` | `696.98 MiB/s (174.2 IOPS)` | **2.28x** | **2.31x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `1972.31 MiB/s (493.1 IOPS)` | `1705.42 MiB/s (426.4 IOPS)` | `1710.59 MiB/s (427.6 IOPS)` | **1.16x** | **1.15x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `1760.21 MiB/s (1760.2 IOPS)` | `765.61 MiB/s (765.6 IOPS)` | `754.05 MiB/s (754.0 IOPS)` | **2.30x** | **2.33x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `2352.67 MiB/s (2352.7 IOPS)` | `1669.79 MiB/s (1669.8 IOPS)` | `1667.07 MiB/s (1667.1 IOPS)` | **1.41x** | **1.41x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `1361.25 MiB/s (1361.2 IOPS)` | `341.36 MiB/s (341.4 IOPS)` | `338.01 MiB/s (338.0 IOPS)` | **3.99x** | **4.03x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `1404.18 MiB/s (1404.2 IOPS)` | `500.61 MiB/s (500.6 IOPS)` | `501.47 MiB/s (501.5 IOPS)` | **2.80x** | **2.80x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `2.25 / 2.47 / 3.03 ms` | `4.80 / 16.45 / 25.82 ms` | `4.86 / 17.70 / 27.39 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `2.40 / 4.42 / 6.00 ms` | `2.71 / 3.95 / 4.82 ms` | `2.82 / 4.42 / 9.24 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `38.35 / 79.17 / 101.19 ms` | `86.15 / 175.11 / 212.86 ms` | `87.38 / 179.31 / 223.35 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `31.90 / 65.27 / 100.14 ms` | `36.17 / 106.43 / 170.92 ms` | `36.69 / 105.38 / 160.43 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `2.26 / 13.04 / 20.32 ms` | `5.09 / 27.92 / 49.02 ms` | `5.25 / 31.59 / 49.55 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `1.69 / 9.90 / 23.46 ms` | `2.33 / 9.37 / 49.02 ms` | `2.38 / 10.55 / 46.92 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `2.92 / 4.42 / 5.34 ms` | `11.59 / 83.36 / 84.41 ms` | `11.63 / 78.12 / 85.46 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `2.83 / 4.42 / 7.11 ms` | `7.90 / 31.33 / 109.58 ms` | `7.87 / 34.34 / 106.43 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `3,928.7` | `643.4` | `2,100.5` | **6.11x** | **1.87x** |
| **Directory stat** | `110,524.7` | `1,207.1` | `14,732.5` | **91.56x** | **7.50x** |
| **Directory removal** | `3,906.1` | `655.4` | `1,773.4` | **5.96x** | **2.20x** |
| **File creation** | `3,564.5` | `1,191.1` | `2,072.2` | **2.99x** | **1.72x** |
| **File stat** | `105,621.0` | `4,462.5` | `14,871.3` | **23.67x** | **7.10x** |
| **File read** | `40,423.4` | `2,311.0` | `4,338.7` | **17.49x** | **9.32x** |
| **File removal** | `4,116.3` | `968.5` | `1,951.0` | **4.25x** | **2.11x** |
| **Tree creation** | `3,645.4` | `1,051.8` | `2,193.7` | **3.47x** | **1.66x** |
| **Tree removal** | `3,557.8` | `766.8` | `1,785.9` | **4.64x** | **1.99x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.i5.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `9,015.7` | `3,186.7` | `5,869.4` | **2.83x** | **1.54x** |
| **Directory stat** | `1,465,150.1` | `367,064.2` | `62,346.8` | **3.99x** | **23.50x** |
| **Directory removal** | `11,966.7` | `3,100.6` | `6,457.0` | **3.86x** | **1.85x** |
| **File creation** | `10,282.5` | `3,463.7` | `6,168.3` | **2.97x** | **1.67x** |
| **File stat** | `1,315,254.2` | `348,977.0` | `449,390.3` | **3.77x** | **2.93x** |
| **File read** | `54,484.2` | `12,467.3` | `16,575.0` | **4.37x** | **3.29x** |
| **File removal** | `12,098.1` | `3,254.1` | `7,102.8` | **3.72x** | **1.70x** |
| **Tree creation** | `2,009.1` | `552.0` | `559.4` | **3.64x** | **3.59x** |
| **Tree removal** | `2,097.3` | `669.1` | `581.1` | **3.13x** | **3.61x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`1453.51 MiB/s`；同轮 JuiceFS(PG) 为 `713.59 MiB/s`。
- Bowl 单流顺序读：`1662.34 MiB/s`；同轮 JuiceFS(PG) 为 `1476.04 MiB/s`。
- Bowl 16 并发顺序写：`1612.92 MiB/s`；同轮 JuiceFS(PG) 为 `707.61 MiB/s`。
- Bowl 16 并发顺序读：`1972.31 MiB/s`；同轮 JuiceFS(PG) 为 `1705.42 MiB/s`。

服务端本地 NVMe SSD 顺序读写与 4K IOPS 物理基线、请求大小与并发数见第 2、4 节。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`（及清理 `/var/jfsCache`），且三组存储栈服务端进程按目标隔离启停。

Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `1705.25 MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `1453.51 MiB/s`。4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。

Bowl 的写入和同步路径使用单盘 `StoreActor` Pebble 与 `64 MiB` `.pack` 封包文件；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理客户端内核页缓存，但不清理 JuiceFS 客户端本地块缓存与服务端页缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的双端缓存清理不能改变 `juicefs bench` 单次运行内部这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0030 / ADR-0031 / ADR-0032 同时改变控制面协议、单盘 Actor 并发模型与 64 MiB Pack 数据层；单轮数据不能将变化归因于其中某一项。

---

## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `1330` 条（跨 Client+Server 全链路 `990` 条，异常链路 `42` 条）· Loki 业务日志 `4074` 条（服务端 Warn/Error `4043` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `fio_direct_randread` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.528 s` (`3528.0 ms`) vs FUSE 累计活跃 `10671.66 ms`（等效并发度 `3.02x`）；占比基准：`FUSE 活跃总耗时` (`10671.66 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `368.00 MiB` / 命中 `44.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 4,118 | 10671.66 | 2.591 | 2.709 | 7.863 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 10671.51 | 2.605 | 2.717 | 7.885 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 4 | 0.07 | 0.016 | 0.016 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Release` | 4 | 0.04 | 0.011 | 0.012 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 20 | 189.56 | 9.478 | 10.000 | 24.700 | 1.8% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 4,258 | 10726.89 | 2.519 | 2.202 | 9.971 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,142 | 10663.75 | 2.575 | 2.225 | 10.207 | 99.4% | 非 OK 状态 41 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,140 | 7925.65 | 1.914 | 1.642 | 7.724 | 73.9% | Read:1.914ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,141 | 6343.49 | 1.532 | 1.517 | 5.308 | 59.1% | HTTP Body+Codec 差值均值 0.871 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,140 | 2738.11 | 0.661 | 0.583 | 2.483 | 25.5% | 非 OK 状态 18 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 4,140 | 2738.11 | 0.661 | 0.583 | 2.483 | 25.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,124 | 2618.23 | 0.635 | 0.576 | 2.472 | 24.4% | read(4124) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4,124 | 98.73 | 0.024 | 0.021 | 0.063 | 0.9% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 4,124 | 30.08 | 0.007 | 0.008 | 0.024 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 4,464 | 2388.18 | 0.535 | 0.453 | 2.440 | 22.3% | — |

#### 工作负载 `fio_direct_randwrite` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.740 s` (`10740.0 ms`) vs FUSE 累计活跃 `11174.42 ms`（等效并发度 `1.04x`）；占比基准：`FUSE 活跃总耗时` (`11174.42 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `13.00 MiB` / 直通 `4083.00 MiB` (Flush RPC `12`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 8,214 | 11174.42 | 1.360 | 0.032 | 6.890 | 100.0% | 错误返回 4096 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 11152.80 | 2.723 | 2.885 | 7.557 | 99.8% | 占 FUSE 99.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 4,096 | 21.46 | 0.005 | 0.003 | 0.028 | 0.2% | 占 FUSE 0.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 4 | 0.07 | 0.018 | 0.021 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,099 | 11012.43 | 2.687 | 2.688 | 5.734 | 98.6% | 非 OK 状态 4 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,099 | 6483.22 | 1.582 | 1.716 | 1.610 | 58.0% | Write:1.582ms, GetXattr:0.730ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,099 | 7671.95 | 1.872 | 1.841 | 4.881 | 68.7% | HTTP Body+Codec 差值均值 0.767 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,099 | 4529.21 | 1.105 | 0.972 | 4.124 | 40.5% | 非 OK 状态 4 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,095 | 4528.54 | 1.106 | 0.973 | 4.124 | 40.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:GetXattr` | 4 | 0.67 | 0.168 | 0.175 | 0.249 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,095 | 22.09 | 0.005 | 0.003 | 0.032 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4 | 0.27 | 0.068 | 0.075 | 0.100 | 0.0% | getxattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 8 | 0.24 | 0.030 | 0.025 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,090 | 92.44 | 0.023 | 0.019 | 0.050 | 0.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,090 | 37.35 | 0.009 | 0.007 | 0.024 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,090 | 67.42 | 0.016 | 0.018 | 0.044 | 0.6% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,095 | 2236.39 | 0.546 | 0.432 | 2.295 | 20.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 283.86 | 4.435 | 4.310 | 9.867 | 2.5% | — |

#### 工作负载 `fio_direct_seq` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `5.616 s` (`5616.0 ms`) vs FUSE 累计活跃 `13804.08 ms`（等效并发度 `2.46x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`13804.08 ms` / RPC `58806.08 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4088.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 12,377 | 13804.08 | 1.115 | 0.080 | 22.406 | 100.0% | 错误返回 4116 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 8322.61 | 2.032 | 0.111 | 26.526 | 60.3% | 占 FUSE 60.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 5330.15 | 1.301 | 0.108 | 25.206 | 38.6% | 占 FUSE 38.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 12 | 131.34 | 10.945 | 0.011 | 65.536 | 1.0% | 占 FUSE 0.9% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 989 | 7904.76 | 7.993 | 7.217 | 24.597 | 57.3% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 7.89 | 1.974 | 2.000 | 4.900 | 0.1% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 518 | 4750.00 | 9.170 | 7.597 | 45.204 | 34.4% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,079 | 58761.34 | 28.264 | 28.471 | 49.794 | 99.9% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 4.26x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,079 | 24170.76 | 11.626 | 8.571 | 39.595 | 41.1% | Read:17.624ms, Write:5.743ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,079 | 46867.76 | 22.543 | 23.724 | 49.744 | 79.7% | HTTP Body+Codec 差值均值 5.905 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,079 | 34590.58 | 16.638 | 19.900 | 49.732 | 58.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,032 | 30923.70 | 29.965 | 36.618 | 49.732 | 52.6% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,035 | 3666.17 | 3.542 | 3.461 | 9.932 | 6.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,040 | 5.57 | 0.005 | 0.003 | 0.009 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,039 | 3625.17 | 3.489 | 3.426 | 9.931 | 6.2% | read(1035), getattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,028 | 33.16 | 0.032 | 0.030 | 0.201 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,039 | 9.79 | 0.009 | 0.008 | 0.046 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 1,039 | 113.12 | 0.109 | 0.075 | 1.661 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 1,039 | 12.56 | 0.012 | 0.009 | 0.033 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,039 | 19.35 | 0.019 | 0.017 | 0.070 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,898 | 3531.06 | 1.860 | 1.795 | 7.909 | 6.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,096 | 2315.32 | 2.113 | 1.951 | 4.949 | 3.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 33.01 | 0.516 | 0.003 | 15.400 | 0.1% | — |

#### 工作负载 `fio_seq_16jobs` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `21.627 s` (`21627.0 ms`) vs FUSE 累计活跃 `259856.09 ms`（等效并发度 `12.02x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`259856.09 ms` / RPC `757332.50 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `16352.00 MiB` / 直通 `32.00 MiB` (Flush RPC `4096`) · ReadAhead 预读 `16384.00 MiB` / 命中 `16372.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 86,373 | 259856.09 | 3.009 | 0.046 | 59.848 | 100.0% | 错误返回 4176 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 16,384 | 142445.74 | 8.694 | 0.145 | 107.799 | 54.8% | 占 FUSE 54.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 65,536 | 114520.54 | 1.747 | 0.037 | 28.101 | 44.1% | 占 FUSE 44.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 16 | 2845.59 | 177.850 | 187.246 | 262.144 | 1.1% | 占 FUSE 1.1% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 3,832 | 139769.23 | 36.474 | 34.921 | 99.879 | 53.8% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 13 | 68.49 | 5.268 | 1.250 | 24.350 | 0.0% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 925 | 20497.95 | 22.160 | 14.576 | 202.155 | 7.9% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 8,322 | 757332.50 | 91.004 | 98.891 | 248.497 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 8,321 | 757106.92 | 90.987 | 98.896 | 248.497 | 100.0% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 2.91x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 8,321 | 136073.63 | 16.353 | 11.926 | 114.275 | 18.0% | Read:26.439ms, Write:6.506ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 8,321 | 676967.14 | 81.356 | 90.569 | 248.450 | 89.4% | HTTP Body+Codec 差值均值 6.722 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 8,321 | 621033.29 | 74.634 | 86.970 | 248.446 | 82.0% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,128 | 607171.18 | 147.086 | 172.309 | 248.446 | 80.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 4,129 | 13696.52 | 3.317 | 2.995 | 14.284 | 1.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,176 | 997.29 | 0.239 | 0.003 | 0.397 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 35 | 1148.36 | 32.810 | 33.036 | 97.812 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,145 | 13309.61 | 3.211 | 2.951 | 11.862 | 1.8% | read(4129), getattr(16) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4,096 | 110.74 | 0.027 | 0.022 | 0.091 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 4,161 | 37.54 | 0.009 | 0.008 | 0.038 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,123 | 409.96 | 0.099 | 0.073 | 1.464 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,123 | 52.86 | 0.013 | 0.008 | 0.041 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,116 | 93.11 | 0.023 | 0.018 | 0.082 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 8,124 | 12961.74 | 1.595 | 1.447 | 7.951 | 1.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,381 | 10025.13 | 2.288 | 2.113 | 5.135 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 6 | 25.39 | 4.231 | 1.000 | 24.100 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 7 | 92.28 | 13.183 | 0.375 | 96.500 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 64 | 17.98 | 0.281 | 0.021 | 6.800 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 257 | 18.05 | 0.070 | 0.003 | 3.692 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 8 | 9.24 | 1.156 | 0.375 | 9.600 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 6 | 22.54 | 3.756 | 0.400 | 12.572 | 0.0% | — |

#### 工作负载 `fio_seq_read_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.747 s` (`10747.0 ms`) vs FUSE 累计活跃 `921.62 ms`（等效并发度 `0.09x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`921.62 ms` / RPC `6706.35 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4095.25 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 16,391 | 921.62 | 0.056 | 0.028 | 0.814 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 16,384 | 921.56 | 0.056 | 0.028 | 0.814 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 1 | 0.03 | 0.028 | 0.032 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 3 | 0.01 | 0.004 | 0.004 | 0.008 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 174 | 287.30 | 1.651 | 1.386 | 9.033 | 31.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,026 | 6677.95 | 6.509 | 7.159 | 17.671 | 99.6% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 7.28x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,026 | 4951.85 | 4.826 | 5.346 | 12.830 | 73.8% | Read:4.827ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,026 | 4420.40 | 4.308 | 4.058 | 9.796 | 65.9% | HTTP Body+Codec 差值均值 2.626 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,026 | 1726.10 | 1.682 | 1.813 | 4.841 | 25.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,026 | 1726.10 | 1.682 | 1.813 | 4.841 | 25.7% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,026 | 1689.96 | 1.647 | 1.794 | 4.812 | 25.2% | read(1026) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,024 | 20.85 | 0.020 | 0.019 | 0.049 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,027 | 6.75 | 0.007 | 0.007 | 0.023 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,164 | 1631.09 | 1.401 | 1.624 | 4.591 | 24.3% | — |

#### 工作负载 `fio_seq_write_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.756 s` (`10756.0 ms`) vs FUSE 累计活跃 `354.21 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`354.21 ms` / RPC `6869.81 ms`)
- **缓存与流水线命中**：NSCache Attr `99.98%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4094.00 MiB` / 直通 `2.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 5,138 | 354.21 | 0.069 | 0.074 | 0.227 | 100.0% | 错误返回 1029 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 334.34 | 0.082 | 0.086 | 0.232 | 94.4% | 占 FUSE 94.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 1 | 12.29 | 12.293 | 16.384 | 16.384 | 3.5% | 占 FUSE 3.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 1,025 | 5.92 | 0.006 | 0.005 | 0.029 | 1.7% | 占 FUSE 1.7% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 11 | 12.94 | 1.176 | 0.708 | 9.450 | 3.7% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 1.42 | 1.420 | 1.750 | 2.485 | 0.4% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,030 | 6852.48 | 6.653 | 7.415 | 20.841 | 99.7% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 19.39x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,030 | 4022.91 | 3.906 | 5.021 | 11.780 | 58.6% | Write:3.919ms, Create:0.506ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,030 | 5462.18 | 5.303 | 5.303 | 9.988 | 79.5% | HTTP Body+Codec 差值均值 2.556 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,030 | 2829.56 | 2.747 | 2.394 | 9.950 | 41.2% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,026 | 2822.55 | 2.751 | 2.396 | 9.061 | 41.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 1 | 6.73 | 6.732 | 7.500 | 9.950 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,029 | 3.09 | 0.003 | 0.003 | 0.012 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 3 | 4.17 | 1.390 | 1.375 | 2.477 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1 | 0.01 | 0.005 | 0.008 | 0.010 | 0.0% | getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1 | 0.00 | 0.003 | 0.003 | 0.005 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 1,029 | 66.29 | 0.064 | 0.053 | 0.428 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 1,029 | 9.52 | 0.009 | 0.008 | 0.025 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,028 | 23.35 | 0.023 | 0.017 | 0.043 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,026 | 2093.97 | 2.041 | 1.891 | 4.933 | 30.5% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 1 | 4.31 | 4.309 | 3.750 | 4.975 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 1 | 6.57 | 6.570 | 7.500 | 9.950 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 64 | 0.91 | 0.014 | 0.011 | 0.154 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 65 | 247.48 | 3.807 | 3.802 | 15.250 | 3.6% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 2 | 7.50 | 3.748 | 3.300 | 4.298 | 0.1% | — |

#### 工作负载 `juicefs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `5.796 s` (`5796.0 ms`) vs FUSE 累计活跃 `10782.13 ms`（等效并发度 `1.86x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`10782.13 ms` / RPC `47412.10 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.94%` / Readlink `N/A` · WriteBehind 聚批 `4138.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1424`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4094.50 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 48,323 | 10782.13 | 0.223 | 0.015 | 6.082 | 100.0% | 错误返回 4921 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,505 | 6521.55 | 1.448 | 0.105 | 20.681 | 60.5% | 占 FUSE 60.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 3557.43 | 0.107 | 0.016 | 1.872 | 33.0% | 占 FUSE 33.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 810 | 385.23 | 0.476 | 0.030 | 1.792 | 3.6% | 占 FUSE 3.6% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 858 | 5985.11 | 6.976 | 5.989 | 24.534 | 55.5% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 3 | 10.10 | 3.365 | 2.125 | 9.850 | 0.1% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 14 | 555.66 | 39.690 | 39.286 | 98.250 | 5.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,676 | 47342.94 | 12.879 | 15.420 | 52.267 | 99.9% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 4.40x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,676 | 16093.14 | 4.378 | 3.258 | 32.863 | 33.9% | Read:6.301ms, Write:4.807ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,676 | 39013.32 | 10.613 | 13.899 | 49.793 | 82.3% | HTTP Body+Codec 差值均值 2.112 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,676 | 31249.80 | 8.501 | 12.162 | 49.610 | 65.9% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 30348.49 | 21.193 | 30.522 | 49.610 | 64.0% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,432 | 842.74 | 0.589 | 0.664 | 2.359 | 1.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 57.85 | 0.026 | 0.003 | 0.021 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 3 | 5.03 | 1.677 | 0.175 | 4.925 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,435 | 798.76 | 0.557 | 0.660 | 2.311 | 1.7% | read(1432), lookup(1), getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,024 | 21.36 | 0.021 | 0.018 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,239 | 16.52 | 0.007 | 0.007 | 0.036 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,236 | 34.02 | 0.015 | 0.012 | 0.080 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,236 | 18.10 | 0.008 | 0.005 | 0.023 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,236 | 53.37 | 0.024 | 0.009 | 0.048 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,943 | 722.46 | 0.372 | 0.388 | 1.282 | 1.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,432 | 2316.49 | 1.618 | 1.633 | 4.920 | 4.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 15.68 | 0.245 | 0.003 | 6.800 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 11.91 | 3.970 | 4.750 | 7.929 | 0.0% | — |

#### 工作负载 `mdtest_1p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `15.176 s` (`15176.0 ms`) vs FUSE 累计活跃 `11897.75 ms`（等效并发度 `0.78x`）；占比基准：`FUSE 活跃总耗时` (`11897.75 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.41%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 262,143 | 11897.75 | 0.045 | 0.002 | 0.408 | 100.0% | 错误返回 26439 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 13,996 | 3120.18 | 0.223 | 0.203 | 0.496 | 26.2% | 占 FUSE 26.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 13,997 | 3064.48 | 0.219 | 0.199 | 0.489 | 25.8% | 占 FUSE 25.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 12,440 | 2820.96 | 0.227 | 0.205 | 0.499 | 23.7% | 占 FUSE 23.7% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 52,875 | 10765.16 | 0.204 | 0.182 | 0.995 | 90.5% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 52,875 | 8556.60 | 0.162 | 0.142 | 0.746 | 71.9% | Mkdir:0.165ms, Rmdir:0.159ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 52,875 | 3535.62 | 0.067 | 0.066 | 0.249 | 29.7% | HTTP Body+Codec 差值均值 0.025 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 52,875 | 2208.56 | 0.042 | 0.040 | 0.249 | 18.6% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 13,996 | 657.33 | 0.047 | 0.044 | 0.188 | 5.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 13,997 | 552.68 | 0.039 | 0.039 | 0.099 | 4.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 52,873 | 50.84 | 0.001 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.06 | 0.020 | 0.020 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.05 | 0.017 | 0.021 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 52,873 | 624.58 | 0.012 | 0.010 | 0.047 | 5.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 52,873 | 59.98 | 0.001 | 0.003 | 0.005 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 52,873 | 234.82 | 0.004 | 0.003 | 0.022 | 2.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.53 | 0.525 | 0.550 | 0.599 | 0.0% | — |

#### 工作负载 `mdtest_4p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.750 s` (`10750.0 ms`) vs FUSE 累计活跃 `1466.11 ms`（等效并发度 `0.14x`）；占比基准：`FUSE 活跃总耗时` (`1466.11 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.95%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `96.83%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 17,957 | 1466.11 | 0.082 | 0.002 | 0.507 | 100.0% | 错误返回 2136 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 1,094 | 397.84 | 0.364 | 0.362 | 0.881 | 27.1% | 占 FUSE 27.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 1,093 | 331.96 | 0.304 | 0.337 | 0.740 | 22.6% | 占 FUSE 22.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 1,040 | 330.87 | 0.318 | 0.358 | 0.801 | 22.6% | 占 FUSE 22.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,269 | 1274.74 | 0.299 | 0.316 | 0.995 | 86.9% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,269 | 1067.63 | 0.250 | 0.273 | 0.895 | 72.8% | Mkdir:0.293ms, Create:0.253ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,269 | 387.55 | 0.091 | 0.085 | 0.249 | 26.4% | HTTP Body+Codec 差值均值 0.042 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,269 | 207.11 | 0.049 | 0.043 | 0.228 | 14.1% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 1,093 | 57.34 | 0.052 | 0.046 | 0.223 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 1,094 | 55.72 | 0.051 | 0.044 | 0.228 | 3.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,267 | 10.82 | 0.003 | 0.003 | 0.025 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.04 | 0.012 | 0.016 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.03 | 0.009 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,197 | 45.92 | 0.011 | 0.010 | 0.041 | 3.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,197 | 4.75 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,197 | 17.36 | 0.004 | 0.003 | 0.017 | 1.2% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.18 | 0.178 | 0.150 | 0.199 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `99.99%` / Entry `99.99%` / Readlink `N/A`（负缓存命中 `29,065`）；WriteBehind 聚批 `28685.00 MiB` / 直通 `4133.00 MiB`（Flush RPC `7,580`，背压等待 `153672.04 ms`，屏障等待 `87.90 ms`）；ReadAhead 预读 `29040.00 MiB`（命中 `28693.75 MiB`，等待 `26280.47 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `Writev` | 33,177 | 168777.04 | 52.6% | 5087.2 | 126.1 | 40559.0 | 83811.8 |
| `Read` | 123,297 | 135001.19 | 42.1% | 1094.9 | 29.9 | 7435.8 | 22014.2 |
| `Mkdir` | 15,093 | 3465.00 | 1.1% | 229.6 | 203.5 | 431.6 | 500.4 |
| `Rmdir` | 15,093 | 3463.41 | 1.1% | 229.5 | 206.7 | 446.2 | 501.8 |
| `Create` | 13,905 | 3317.60 | 1.0% | 238.6 | 212.0 | 462.3 | 506.2 |
| `Unlink` | 13,905 | 3047.75 | 0.9% | 219.2 | 200.6 | 409.4 | 498.5 |
| `Fsync` | 17 | 2857.89 | 0.9% | 168111.0 | 187245.7 | 262144.0 | 262144.0 |
| `Flush` | 27,841 | 553.72 | 0.2% | 19.9 | 1.2 | 4.3 | 678.2 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Write` | 11,713 | 705709.90 | 60.250 | 27.795 | 228.005 | 245.601 |
| `http_outer:Read` | 11,763 | 80771.46 | 6.867 | 4.055 | 23.659 | 46.311 |
| `http_outer:Rmdir` | 15,093 | 1094.58 | 0.073 | 0.072 | 0.190 | 0.239 |
| `http_outer:Create` | 13,905 | 1054.00 | 0.076 | 0.075 | 0.196 | 0.240 |
| `rpc:Write` | 11,713 (11713/0) | 675794.45 | 57.696 | 26.099 | 227.955 | 245.591 |
| `rpc:Read` | 11,762 (11744/18) | 22669.64 | 1.927 | 1.348 | 6.586 | 9.657 |
| `rpc:Rmdir` | 15,093 (15093/0) | 715.33 | 0.047 | 0.044 | 0.096 | 0.194 |
| `rpc:Create` | 13,905 (13905/0) | 628.11 | 0.045 | 0.042 | 0.094 | 0.157 |
| `rpc:Mkdir` | 15,093 (15093/0) | 608.56 | 0.040 | 0.039 | 0.089 | 0.100 |
| `rpc:Unlink` | 13,905 (13905/0) | 577.53 | 0.042 | 0.035 | 0.087 | 0.119 |
| `actor_phase:phase_a_validate` | 69,613 | 1404.94 | 0.020 | 0.012 | 0.068 | 0.098 |
| `actor_phase:wal_apply_nosync` | 69,604 | 525.67 | 0.008 | 0.004 | 0.022 | 0.027 |
| `actor_phase:phase_b_mutate` | 69,613 | 212.10 | 0.003 | 0.003 | 0.009 | 0.019 |
| `actor_phase:pack_sync_barrier` | 8 | 98.85 | 12.356 | 0.500 | 80.000 | 96.000 |
| `actor_phase:wal_apply_sync` | 8 | 29.85 | 3.732 | 1.000 | 19.000 | 23.800 |
| `actor_phase:dead_pack_remove` | 128 | 18.88 | 0.148 | 0.014 | 0.467 | 4.650 |
| `read_op:read` | 11,746 | 22041.63 | 1.877 | 1.306 | 6.491 | 9.594 |
| `read_op:getxattr` | 4 | 0.27 | 0.068 | 0.075 | 0.098 | 0.100 |
| `read_op:getattr` | 27 | 0.15 | 0.006 | 0.005 | 0.020 | 0.024 |
| `read_op:lookup` | 4 | 0.11 | 0.027 | 0.025 | 0.048 | 0.049 |
| `kv_read:prefix_scan` | 11,296 | 284.84 | 0.025 | 0.021 | 0.047 | 0.081 |
| `kv_read:point_get` | 12,610 | 101.04 | 0.008 | 0.008 | 0.021 | 0.033 |
| `blob_pread` | 17,593 | 21234.53 | 1.207 | 0.904 | 4.177 | 6.234 |
| `blob_pwrite` | 12,030 | 18987.31 | 1.578 | 1.397 | 4.398 | 4.934 |

Pebble BlockCache 命中率 `99.04%` · ReadAmp `1.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `98d4f011a0a17780` | 237.24 | `bowl-client:fuse.Fsync` | 215.80 | 211.04 | 4.75 | `bowl-client:storage.v1.StorageService/Write` (215.80 ms) |
| `a0744ba7d441b1d9` | 232.40 | `bowl-client:fuse.Fsync` | 210.87 | 208.75 | 2.12 | `bowl-client:storage.v1.StorageService/Write` (210.87 ms) |
| `4abe4a1dc6179c88` | 225.95 | `bowl-client:fuse.Fsync` | 143.34 | 140.76 | 2.58 | `bowl-client:storage.v1.StorageService/Write` (143.34 ms) |
| `a025dfb0c9e4dd16` | 220.49 | `bowl-client:write_behind.FlushChunk` | 220.46 | 215.47 | 5.00 | `bowl-client:storage.v1.StorageService/Write` (220.46 ms) |
| `b65953061d1ebe30` | 220.03 | `bowl-client:fuse.Fsync` | 205.37 | 203.07 | 2.30 | `bowl-client:storage.v1.StorageService/Write` (205.37 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `fio_direct_randwrite_cpu.pb.gz` | `4.24s 62.54% 62.54%      4.24s 62.54%  internal/runtime/syscall/linux.Syscall6` | `4.24s 62.54% 62.54%      4.24s 62.54%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_fio_direct_randwrite_cpu.txt`](results/telemetry/pprof_top/server_fio_direct_randwrite_cpu.txt) |
| `server` | `fio_direct_randwrite_heap.pb.gz` | `100MB 30.38% 30.38%      100MB 30.38%  bytes.growSlice` | `0     0%     0%   278.06MB 84.48%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_fio_direct_randwrite_heap.txt`](results/telemetry/pprof_top/server_fio_direct_randwrite_heap.txt) |
| `server` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_fio_direct_randwrite_mutex.txt`](results/telemetry/pprof_top/server_fio_direct_randwrite_mutex.txt) |
| `server` | `fio_seq_read_1job_cpu.pb.gz` | `2.60s 57.65% 57.65%      2.60s 57.65%  internal/runtime/syscall/linux.Syscall6` | `2.60s 57.65% 57.65%      2.60s 57.65%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_fio_seq_read_1job_cpu.txt`](results/telemetry/pprof_top/server_fio_seq_read_1job_cpu.txt) |
| `server` | `fio_seq_read_1job_heap.pb.gz` | `99.20MB 59.10% 59.10%    99.20MB 59.10%  bytes.growSlice` | `0     0%     0%   143.71MB 85.62%  connectrpc.com/connect.(*Handler).ServeHTTP` | [`pprof_top/server_fio_seq_read_1job_heap.txt`](results/telemetry/pprof_top/server_fio_seq_read_1job_heap.txt) |
| `server` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_fio_seq_read_1job_mutex.txt`](results/telemetry/pprof_top/server_fio_seq_read_1job_mutex.txt) |
| `server` | `fio_seq_write_1job_cpu.pb.gz` | `4.21s 74.51% 74.51%      4.21s 74.51%  internal/runtime/syscall/linux.Syscall6` | `4.21s 74.51% 74.51%      4.21s 74.51%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_fio_seq_write_1job_cpu.txt`](results/telemetry/pprof_top/server_fio_seq_write_1job_cpu.txt) |
| `server` | `fio_seq_write_1job_heap.pb.gz` | `99.20MB 59.10% 59.10%    99.20MB 59.10%  bytes.growSlice` | `0     0%     0%   143.71MB 85.62%  connectrpc.com/connect.(*Handler).ServeHTTP` | [`pprof_top/server_fio_seq_write_1job_heap.txt`](results/telemetry/pprof_top/server_fio_seq_write_1job_heap.txt) |
| `server` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_fio_seq_write_1job_mutex.txt`](results/telemetry/pprof_top/server_fio_seq_write_1job_mutex.txt) |
| `server` | `mdtest_1p_cpu.pb.gz` | `0.71s 11.15% 11.15%      0.71s 11.15%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.44s 38.30%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_mdtest_1p_cpu.txt`](results/telemetry/pprof_top/server_mdtest_1p_cpu.txt) |
| `server` | `mdtest_1p_heap.pb.gz` | `51MB 28.86% 28.86%       51MB 28.86%  github.com/cockroachdb/pebble/v2/internal/manual.New` | `0     0%     0%   101.09MB 57.20%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_mdtest_1p_heap.txt`](results/telemetry/pprof_top/server_mdtest_1p_heap.txt) |
| `server` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_mdtest_1p_mutex.txt`](results/telemetry/pprof_top/server_mdtest_1p_mutex.txt) |
| `server` | `mdtest_4p_cpu.pb.gz` | `120ms 17.65% 17.65%      120ms 17.65%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      220ms 32.35%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_mdtest_4p_cpu.txt`](results/telemetry/pprof_top/server_mdtest_4p_cpu.txt) |
| `server` | `mdtest_4p_heap.pb.gz` | `51MB 36.96% 36.96%       51MB 36.96%  github.com/cockroachdb/pebble/v2/internal/manual.New` | `0     0%     0%    64.54MB 46.78%  sync.(*Pool).Get` | [`pprof_top/server_mdtest_4p_heap.txt`](results/telemetry/pprof_top/server_mdtest_4p_heap.txt) |
| `server` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_mdtest_4p_mutex.txt`](results/telemetry/pprof_top/server_mdtest_4p_mutex.txt) |
| `client` | `fio_direct_randwrite_cpu.pb.gz` | `1.51s 47.34% 47.34%      1.51s 47.34%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.92s 60.19%  net/http.(*http2clientStream).doRequest` | [`pprof_top/client_fio_direct_randwrite_cpu.txt`](results/telemetry/pprof_top/client_fio_direct_randwrite_cpu.txt) |
| `client` | `fio_direct_randwrite_heap.pb.gz` | `320.11MB 45.40% 45.40%   320.11MB 45.40%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   401.57MB 56.95%  sync.(*Pool).Get` | [`pprof_top/client_fio_direct_randwrite_heap.txt`](results/telemetry/pprof_top/client_fio_direct_randwrite_heap.txt) |
| `client` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_fio_direct_randwrite_mutex.txt`](results/telemetry/pprof_top/client_fio_direct_randwrite_mutex.txt) |
| `client` | `fio_seq_read_1job_cpu.pb.gz` | `2.12s 48.18% 48.18%      2.12s 48.18%  internal/runtime/syscall/linux.Syscall6` | `2.12s 48.18% 48.18%      2.12s 48.18%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/client_fio_seq_read_1job_cpu.txt`](results/telemetry/pprof_top/client_fio_seq_read_1job_cpu.txt) |
| `client` | `fio_seq_read_1job_heap.pb.gz` | `110.04MB 50.33% 50.33%   110.04MB 50.33%  bytes.growSlice` | `0     0%     0%   154.06MB 70.46%  connectrpc.com/connect.(*Client[go.shape.e80ba5ebc5f7385136069f97c3374d2892f8c315d57d93e4b627333ca6474eba,go.shape.struct { github.com/starsrc/bowl/gen/go/storage/v1.state google.golang.org/protobuf/internal/impl.MessageState "protogen:\"open.v1\""; Data []uint8 "protobuf:\"bytes,1,opt,name=data,proto3\" json:\"data,omitempty\""; github.com/starsrc/bowl/gen/go/storage/v1.unknownFields []uint8; github.com/starsrc/bowl/gen/go/storage/v1.sizeCache int32 }]).CallUnary` | [`pprof_top/client_fio_seq_read_1job_heap.txt`](results/telemetry/pprof_top/client_fio_seq_read_1job_heap.txt) |
| `client` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_fio_seq_read_1job_mutex.txt`](results/telemetry/pprof_top/client_fio_seq_read_1job_mutex.txt) |
| `client` | `fio_seq_write_1job_cpu.pb.gz` | `1.47s 46.37% 46.37%      1.47s 46.37%  runtime.memmove` | `1.47s 46.37% 46.37%      1.47s 46.37%  runtime.memmove` | [`pprof_top/client_fio_seq_write_1job_cpu.txt`](results/telemetry/pprof_top/client_fio_seq_write_1job_cpu.txt) |
| `client` | `fio_seq_write_1job_heap.pb.gz` | `110.04MB 50.33% 50.33%   110.04MB 50.33%  bytes.growSlice` | `0     0%     0%   154.06MB 70.46%  connectrpc.com/connect.(*Client[go.shape.e80ba5ebc5f7385136069f97c3374d2892f8c315d57d93e4b627333ca6474eba,go.shape.struct { github.com/starsrc/bowl/gen/go/storage/v1.state google.golang.org/protobuf/internal/impl.MessageState "protogen:\"open.v1\""; Data []uint8 "protobuf:\"bytes,1,opt,name=data,proto3\" json:\"data,omitempty\""; github.com/starsrc/bowl/gen/go/storage/v1.unknownFields []uint8; github.com/starsrc/bowl/gen/go/storage/v1.sizeCache int32 }]).CallUnary` | [`pprof_top/client_fio_seq_write_1job_heap.txt`](results/telemetry/pprof_top/client_fio_seq_write_1job_heap.txt) |
| `client` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_fio_seq_write_1job_mutex.txt`](results/telemetry/pprof_top/client_fio_seq_write_1job_mutex.txt) |
| `client` | `mdtest_1p_cpu.pb.gz` | `0.79s 20.31% 20.31%      0.79s 20.31%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.10s 53.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_mdtest_1p_cpu.txt`](results/telemetry/pprof_top/client_mdtest_1p_cpu.txt) |
| `client` | `mdtest_1p_heap.pb.gz` | `320.11MB 32.51% 32.51%   320.11MB 32.51%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   586.74MB 59.59%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_mdtest_1p_heap.txt`](results/telemetry/pprof_top/client_mdtest_1p_heap.txt) |
| `client` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_mdtest_1p_mutex.txt`](results/telemetry/pprof_top/client_mdtest_1p_mutex.txt) |
| `client` | `mdtest_4p_cpu.pb.gz` | `140ms 26.92% 26.92%      140ms 26.92%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      300ms 57.69%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_mdtest_4p_cpu.txt`](results/telemetry/pprof_top/client_mdtest_4p_cpu.txt) |
| `client` | `mdtest_4p_heap.pb.gz` | `320.11MB 32.51% 32.51%   320.11MB 32.51%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   586.74MB 59.59%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | [`pprof_top/client_mdtest_4p_heap.txt`](results/telemetry/pprof_top/client_mdtest_4p_heap.txt) |
| `client` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_mdtest_4p_mutex.txt`](results/telemetry/pprof_top/client_mdtest_4p_mutex.txt) |

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261112-050700`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-<redacted-server>`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-<redacted-client>`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-12T05:07:09Z` |
| **实例销毁时间 (UTC)** | `2026-11-12T05:21:00Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
