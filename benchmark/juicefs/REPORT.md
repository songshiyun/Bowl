# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘。实际完成范围与重跑环境见本轮执行说明；官方文档数值仅作历史参考：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `324.20 MiB/s` | `307.73 MiB/s` | `305.82 MiB/s` | **1.05x** | **1.06x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,598.78 MiB/s` | `451.21 MiB/s` | `453.25 MiB/s` | **3.54x** | **3.53x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `444.40 files/s` | `397.40 files/s` | `886.20 files/s` | **1.12x** | **0.50x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `125.80 files/s` | `3,143.50 files/s` | `5,092.50 files/s` | **0.04x** | **0.02x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,415.90 files/s` | `12,817.70 files/s` | `17,136.30 files/s` | **0.50x** | **0.37x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `276.38 MiB/s` | `308.04 MiB/s` | `299.33 MiB/s` | **0.90x** | **0.92x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `184.24 MiB/s` | `184.44 MiB/s` | `184.51 MiB/s` | **1.00x** | **1.00x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `196.85 MiB/s` | `199.27 MiB/s` | `198.99 MiB/s` | **0.99x** | **0.99x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `181.51 MiB/s` | `176.76 MiB/s` | `177.63 MiB/s` | **1.03x** | **1.02x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `490.86 ops/s` | `868.63 ops/s` | `2,077.49 ops/s` | **0.57x** | **0.24x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `545.54 ops/s` | `1,042.38 ops/s` | `1,995.63 ops/s` | **0.52x** | **0.27x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `2,841.55 ops/s` | `4,272.51 ops/s` | `14,241.68 ops/s` | **0.67x** | **0.20x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `532.03 ops/s` | `858.85 ops/s` | `1,868.75 ops/s` | **0.62x** | **0.28x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj98t4cevb40k5jnxppn` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.216` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj98t4cevb40k5jnxppo` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.217` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.043/0.072/0.313/0.059` ms | `ping -c 20 -i 0.05 172.16.1.216` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.216 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `173.53` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `174.37` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `ee8d113` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **元数据引擎 (Server 节点)** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`bowl` 库) | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.216:9001/jfs-pg`) | rclone S3 (`http://172.16.1.216:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `324.20 MiB/s (12.63 s/file)` | `307.73 MiB/s (13.31 s/file)` | `305.82 MiB/s (13.39 s/file)` | `744.47 MiB/s` | `731.93 MiB/s` | **1.05x** | **1.06x** |
| **Read big file** | `1598.78 MiB/s (2.56 s/file)` | `451.21 MiB/s (9.08 s/file)` | `453.25 MiB/s (9.04 s/file)` | `895.88 MiB/s` | `892.99 MiB/s` | **3.54x** | **3.53x** |
| **Write small file** | `444.40 files/s (9.00 ms/file)` | `397.40 files/s (10.07 ms/file)` | `886.20 files/s (4.51 ms/file)` | `86.40 files/s` | `109.10 files/s` | **1.12x** | **0.50x** |
| **Read small file** | `125.80 files/s (31.79 ms/file)` | `3143.50 files/s (1.27 ms/file)` | `5092.50 files/s (0.79 ms/file)` | `1857.90 files/s` | `937.30 files/s` | **0.04x** | **0.02x** |
| **Stat file** | `6415.90 files/s (0.62 ms/file)` | `12817.70 files/s (0.31 ms/file)` | `17136.30 files/s (0.23 ms/file)` | `7845.80 files/s` | `11989.50 files/s` | **0.50x** | **0.37x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `276.38 MiB/s (69.1 IOPS)` | `308.04 MiB/s (77.0 IOPS)` | `299.33 MiB/s (74.8 IOPS)` | **0.90x** | **0.92x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `184.24 MiB/s (46.1 IOPS)` | `184.44 MiB/s (46.1 IOPS)` | `184.51 MiB/s (46.1 IOPS)` | **1.00x** | **1.00x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `196.85 MiB/s (49.2 IOPS)` | `199.27 MiB/s (49.8 IOPS)` | `198.99 MiB/s (49.8 IOPS)` | **0.99x** | **0.99x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `181.51 MiB/s (45.4 IOPS)` | `176.76 MiB/s (44.2 IOPS)` | `177.63 MiB/s (44.4 IOPS)` | **1.03x** | **1.02x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `276.31 MiB/s (276.3 IOPS)` | `333.63 MiB/s (333.6 IOPS)` | `330.00 MiB/s (330.0 IOPS)` | **0.83x** | **0.84x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `184.10 MiB/s (184.1 IOPS)` | `184.62 MiB/s (184.6 IOPS)` | `184.55 MiB/s (184.6 IOPS)` | **1.00x** | **1.00x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `297.13 MiB/s (297.1 IOPS)` | `312.41 MiB/s (312.4 IOPS)` | `315.83 MiB/s (315.8 IOPS)` | **0.95x** | **0.94x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `184.52 MiB/s (184.5 IOPS)` | `122.24 MiB/s (122.2 IOPS)` | `116.65 MiB/s (116.7 IOPS)` | **1.51x** | **1.58x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `14.08 / 32.11 / 175.11 ms` | `10.92 / 43.25 / 73.92 ms` | `11.42 / 43.78 / 84.41 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.71 / 31.06 / 35.39 ms` | `21.68 / 104.33 / 158.34 ms` | `21.68 / 111.67 / 162.53 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `317.06 / 591.40 / 817.89 ms` | `308.13 / 442.50 / 450.89 ms` | `308.60 / 358.61 / 446.69 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `350.29 / 501.22 / 608.17 ms` | `344.93 / 1044.38 / 1317.01 ms` | `346.83 / 1019.22 / 1283.46 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比本轮实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `490.9` | `868.6` | `2,077.5` | `1,416.6` | `192.3` | `6.0` | **0.57x** | **0.24x** |
| **Directory stat** | `2,840.5` | `4,224.9` | `14,682.1` | `3,810.1` | `1,311.2` | `435.9` | **0.67x** | **0.19x** |
| **Directory removal** | `526.7` | `755.7` | `1,716.3` | `1,115.1` | `213.1` | `9.0` | **0.70x** | **0.31x** |
| **File creation** | `545.5` | `1,042.4` | `1,995.6` | `1,410.3` | `179.3` | `5.7` | **0.52x** | **0.27x** |
| **File stat** | `2,841.6` | `4,272.5` | `14,241.7` | `5,023.2` | `915.2` | `68.7` | **0.67x** | **0.20x** |
| **File read** | `493.5` | `2,219.0` | `4,188.2` | `3,487.9` | `371.0` | `33.9` | **0.22x** | **0.12x** |
| **File removal** | `532.0` | `858.8` | `1,868.8` | `1,163.4` | `217.5` | `23.7` | **0.62x** | **0.28x** |
| **Tree creation** | `551.7` | `911.3` | `2,093.6` | `1,503.0` | `187.9` | `6.0` | **0.61x** | **0.26x** |
| **Tree removal** | `519.6` | `701.8` | `1,732.9` | `1,119.8` | `218.4` | `9.9` | **0.74x** | **0.30x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `1,835.6` | `2,914.8` | `6,170.5` | **0.63x** | **0.30x** |
| **Directory stat** | `103,472.2` | `346,431.0` | `446,327.0` | **0.30x** | **0.23x** |
| **Directory removal** | `2,193.0` | `2,811.9` | `5,786.6` | **0.78x** | **0.38x** |
| **File creation** | `1,583.2` | `3,247.1` | `6,539.4` | **0.49x** | **0.24x** |
| **File stat** | `99,622.9` | `271,667.9` | `62,517.5` | **0.37x** | **1.59x** |
| **File read** | `2,791.8` | `13,062.6` | `17,849.9` | **0.21x** | **0.16x** |
| **File removal** | `1,996.1` | `3,184.8` | `7,336.2` | **0.63x** | **0.27x** |
| **Tree creation** | `265.2` | `559.4` | `252.8` | **0.47x** | **1.05x** |
| **Tree removal** | `329.2` | `270.0` | `1,468.5` | **1.22x** | **0.22x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`276.38 MiB/s`；同轮 JuiceFS(PG) 为 `308.04 MiB/s`。
- Bowl 单流顺序读：`184.24 MiB/s`；同轮 JuiceFS(PG) 为 `184.44 MiB/s`。
- Bowl 16 并发顺序写：`196.85 MiB/s`；同轮 JuiceFS(PG) 为 `199.27 MiB/s`。
- Bowl 16 并发顺序读：`181.51 MiB/s`；同轮 JuiceFS(PG) 为 `176.76 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

本轮 Bowl 使用 ADR-0026：块写入返回时已进入服务端页缓存，显式 `Fsync` 仅同步数据库 WAL，块数据由内核后台回写。本套件未测试整机断电恢复；吞吐结果不能作为断电后数据完整性的证明，也不能与旧版逐块刷盘结果视为相同持久化条件。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

本轮包含 ADR-0028：Create、Write、常规 Release 使用单语句原子 CTE 与内联异步 WAL 提交；重开文件时按版本保留内核页缓存。因此重复读取包含缓存命中收益，本轮数据不能单独归因于数据库往返减少。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261102-104004`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj98t4cevb40k5jnxppn`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj98t4cevb40k5jnxppo`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-02T10:40:10Z` |
| **实例销毁时间 (UTC)** | `2026-11-02T11:07:50Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
