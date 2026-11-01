# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下完整运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

为确保对比的严谨性与公平性，本次评测在**完全相同的双机硬件、VPC 私网与 120 GiB `cloud_essd` 云盘**上依次测试了三组存储架构，并同步对照 JuiceFS 官方文档基线：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：与 Bowl 使用同台 Server 上的同一个 PostgreSQL 16 实例作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标同机实测总览表

| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `178.59 MiB/s` | `302.28 MiB/s` | `302.49 MiB/s` | **0.59x** | **0.59x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,724.30 MiB/s` | `464.37 MiB/s` | `457.70 MiB/s` | **3.71x** | **3.77x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `423.30 files/s` | `539.60 files/s` | `934.40 files/s` | **0.78x** | **0.45x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,364.70 files/s` | `3,145.50 files/s` | `2,828.90 files/s` | **0.43x** | **0.48x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `6,486.00 files/s` | `9,212.90 files/s` | `17,847.70 files/s` | **0.70x** | **0.36x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `168.98 MiB/s` | `297.50 MiB/s` | `305.31 MiB/s` | **0.57x** | **0.55x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `187.84 MiB/s` | `184.26 MiB/s` | `184.90 MiB/s` | **1.02x** | **1.02x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `174.55 MiB/s` | `199.50 MiB/s` | `198.89 MiB/s` | **0.87x** | **0.88x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `181.80 MiB/s` | `176.95 MiB/s` | `176.70 MiB/s` | **1.03x** | **1.03x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `275.34 ops/s` | `826.35 ops/s` | `2,026.55 ops/s` | **0.33x** | **0.14x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `452.20 ops/s` | `1,028.72 ops/s` | `1,953.18 ops/s` | **0.44x** | **0.23x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `2,814.00 ops/s` | `4,073.68 ops/s` | `13,775.94 ops/s` | **0.69x** | **0.20x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `548.76 ops/s` | `847.69 ops/s` | `1,829.28 ops/s` | **0.65x** | **0.30x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj92vuoez02ylgvpl2b1` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.196` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj92vuoez02ylgvpl2b2` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.197` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.051/0.086/0.330/0.060` ms | `ping -c 20 -i 0.05 172.16.1.196` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.196 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `168.59` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `174.31` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `f2676ca+dirty` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **元数据引擎 (Server 节点)** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`bowl` 库) | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.196:9001/jfs-pg`) | rclone S3 (`http://172.16.1.196:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `178.59 MiB/s (22.93 s/file)` | `302.28 MiB/s (13.55 s/file)` | `302.49 MiB/s (13.54 s/file)` | `744.47 MiB/s` | `731.93 MiB/s` | **0.59x** | **0.59x** |
| **Read big file** | `1724.30 MiB/s (2.38 s/file)` | `464.37 MiB/s (8.82 s/file)` | `457.70 MiB/s (8.95 s/file)` | `895.88 MiB/s` | `892.99 MiB/s` | **3.71x** | **3.77x** |
| **Write small file** | `423.30 files/s (9.45 ms/file)` | `539.60 files/s (7.41 ms/file)` | `934.40 files/s (4.28 ms/file)` | `86.40 files/s` | `109.10 files/s` | **0.78x** | **0.45x** |
| **Read small file** | `1364.70 files/s (2.93 ms/file)` | `3145.50 files/s (1.27 ms/file)` | `2828.90 files/s (1.41 ms/file)` | `1857.90 files/s` | `937.30 files/s` | **0.43x** | **0.48x** |
| **Stat file** | `6486.00 files/s (0.62 ms/file)` | `9212.90 files/s (0.43 ms/file)` | `17847.70 files/s (0.22 ms/file)` | `7845.80 files/s` | `11989.50 files/s` | **0.70x** | **0.36x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `168.98 MiB/s (42.2 IOPS)` | `297.50 MiB/s (74.4 IOPS)` | `305.31 MiB/s (76.3 IOPS)` | **0.57x** | **0.55x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `187.84 MiB/s (47.0 IOPS)` | `184.26 MiB/s (46.1 IOPS)` | `184.90 MiB/s (46.2 IOPS)` | **1.02x** | **1.02x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `174.55 MiB/s (43.6 IOPS)` | `199.50 MiB/s (49.9 IOPS)` | `198.89 MiB/s (49.7 IOPS)` | **0.87x** | **0.88x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `181.80 MiB/s (45.5 IOPS)` | `176.95 MiB/s (44.2 IOPS)` | `176.70 MiB/s (44.2 IOPS)` | **1.03x** | **1.03x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `174.47 MiB/s (174.5 IOPS)` | `337.45 MiB/s (337.4 IOPS)` | `334.80 MiB/s (334.8 IOPS)` | **0.52x** | **0.52x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `182.91 MiB/s (182.9 IOPS)` | `184.89 MiB/s (184.9 IOPS)` | `184.93 MiB/s (184.9 IOPS)` | **0.99x** | **0.99x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `53.00 MiB/s (53.0 IOPS)` | `298.35 MiB/s (298.4 IOPS)` | `310.63 MiB/s (310.6 IOPS)` | **0.18x** | **0.17x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `176.04 MiB/s (176.0 IOPS)` | `121.03 MiB/s (121.0 IOPS)` | `122.53 MiB/s (122.5 IOPS)` | **1.45x** | **1.44x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `23.30 / 32.90 / 39.58 ms` | `11.46 / 43.25 / 73.92 ms` | `11.26 / 43.25 / 74.97 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `21.29 / 31.06 / 36.96 ms` | `21.70 / 107.48 / 164.63 ms` | `21.63 / 127.40 / 173.01 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `364.70 / 442.50 / 492.83 ms` | `308.43 / 354.42 / 446.69 ms` | `309.40 / 358.61 / 450.89 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `350.87 / 513.80 / 616.56 ms` | `343.99 / 1019.22 / 1266.68 ms` | `350.29 / 1035.99 / 1317.01 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比同机实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `275.3` | `826.3` | `2,026.6` | `1,416.6` | `192.3` | `6.0` | **0.33x** | **0.14x** |
| **Directory stat** | `584.8` | `4,135.0` | `13,831.5` | `3,810.1` | `1,311.2` | `435.9` | **0.14x** | **0.04x** |
| **Directory removal** | `507.4` | `749.2` | `1,672.6` | `1,115.1` | `213.1` | `9.0` | **0.68x** | **0.30x** |
| **File creation** | `452.2` | `1,028.7` | `1,953.2` | `1,410.3` | `179.3` | `5.7` | **0.44x** | **0.23x** |
| **File stat** | `2,814.0` | `4,073.7` | `13,775.9` | `5,023.2` | `915.2` | `68.7` | **0.69x** | **0.20x** |
| **File read** | `546.6` | `2,164.2` | `4,089.9` | `3,487.9` | `371.0` | `33.9` | **0.25x** | **0.13x** |
| **File removal** | `548.8` | `847.7` | `1,829.3` | `1,163.4` | `217.5` | `23.7` | **0.65x** | **0.30x** |
| **Tree creation** | `585.7` | `854.4` | `2,067.5` | `1,503.0` | `187.9` | `6.0` | **0.69x** | **0.28x** |
| **Tree removal** | `566.8` | `693.4` | `1,705.2` | `1,119.8` | `218.4` | `9.9` | **0.82x** | **0.33x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `1,907.7` | `2,923.7` | `6,312.4` | **0.65x** | **0.30x** |
| **Directory stat** | `261,809.8` | `339,483.7` | `62,922.0` | **0.77x** | **4.16x** |
| **Directory removal** | `2,233.1` | `2,757.7` | `5,368.8` | **0.81x** | **0.42x** |
| **File creation** | `1,367.2` | `3,202.2` | `6,377.6` | **0.43x** | **0.21x** |
| **File stat** | `250,309.0` | `266,708.5` | `62,477.3` | **0.94x** | **4.01x** |
| **File read** | `2,764.0` | `13,395.5` | `17,682.5` | **0.21x** | **0.16x** |
| **File removal** | `1,936.7` | `3,056.2` | `7,204.1` | **0.63x** | **0.27x** |
| **Tree creation** | `301.3` | `357.6` | `544.9` | **0.84x** | **0.55x** |
| **Tree removal** | `348.3` | `389.9` | `1,424.3` | **0.89x** | **0.24x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`168.98 MiB/s`；同轮 JuiceFS(PG) 为 `297.50 MiB/s`。
- Bowl 单流顺序读：`187.84 MiB/s`；同轮 JuiceFS(PG) 为 `184.26 MiB/s`。
- Bowl 16 并发顺序写：`174.55 MiB/s`；同轮 JuiceFS(PG) 为 `199.50 MiB/s`。
- Bowl 16 并发顺序读：`181.80 MiB/s`；同轮 JuiceFS(PG) 为 `176.95 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261031-173709`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj92vuoez02ylgvpl2b1`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj92vuoez02ylgvpl2b2`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-10-31T17:37:18Z` |
| **实例销毁时间 (UTC)** | `2026-10-31T18:05:25Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
