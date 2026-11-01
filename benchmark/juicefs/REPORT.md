# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

> 本轮版本为 `7fbc2fe`，执行日期为 2026-11-02（北京时间）。**Bowl 测试未全部通过**：两次独立双机运行均在 `fio seq_write_1job` 的最终 `fsync` 返回 EIO，服务端记录 `flush wal: context deadline exceeded`。

> Bowl 仅完成 `juicefs bench`；失败 fio 的吞吐不计入表格，其余 7 项 fio 和两项 mdtest 未执行，表中的 `—` 表示无有效结果。不能据此声称整体性能提升。

> JuiceFS PG/Redis 对照组在另建的同规格双机上补跑。第 2、7 节环境与资源审计属于该对照组；Bowl 复跑环境见 [env_bowl_retry.json](results/env_bowl_retry.json)，两次失败记录见 [首次运行](results/bowl-first-run.log) 与 [独立复跑](results/bowl-retry-run.log)。跨实例比值仅供观察，不作为优化收益证明。

> 失败证据：[fio 原始输出](results/fio_bowl_seq_write_1job.json)、[服务端错误](results/bowl-storage-errors.log)、[完整服务端日志](results/bowl-storage.log.gz)。生产代码保持最新主分支原样，未修改超时或测试负载来绕过失败。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘。实际完成范围与重跑环境见本轮执行说明；官方文档数值仅作历史参考：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `271.48 MiB/s` | `306.37 MiB/s` | `291.71 MiB/s` | **0.89x** | **0.93x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `1,464.34 MiB/s` | `450.08 MiB/s` | `483.98 MiB/s` | **3.25x** | **3.03x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `56.60 files/s` | `583.00 files/s` | `985.40 files/s` | **0.10x** | **0.06x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `1,415.20 files/s` | `1,112.60 files/s` | `2,511.70 files/s` | **1.27x** | **0.56x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `7,187.70 files/s` | `13,985.80 files/s` | `11,337.10 files/s` | **0.51x** | **0.63x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `—` | `305.90 MiB/s` | `295.48 MiB/s` | **—** | **—** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `—` | `184.16 MiB/s` | `184.03 MiB/s` | **—** | **—** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `—` | `197.52 MiB/s` | `198.50 MiB/s` | **—** | **—** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `—` | `175.65 MiB/s` | `176.67 MiB/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `—` | `987.58 ops/s` | `2,293.05 ops/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `—` | `1,125.06 ops/s` | `2,182.50 ops/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `—` | `4,565.17 ops/s` | `15,949.26 ops/s` | **—** | **—** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `—` | `938.45 ops/s` | `2,102.49 ops/s` | **—** | **—** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9a8rs2hdzun6vaaiu6` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.197` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9a8rs2hdzun6vaaiu7` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.196` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.034/0.065/0.297/0.059` ms | `ping -c 20 -i 0.05 172.16.1.197` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.197 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `181.42` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `177.89` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `7fbc2fe` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **元数据引擎 (Server 节点)** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`bowl` 库) | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.197:9001/jfs-pg`) | rclone S3 (`http://172.16.1.197:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `271.48 MiB/s (15.09 s/file)` | `306.37 MiB/s (13.37 s/file)` | `291.71 MiB/s (14.04 s/file)` | `744.47 MiB/s` | `731.93 MiB/s` | **0.89x** | **0.93x** |
| **Read big file** | `1464.34 MiB/s (2.80 s/file)` | `450.08 MiB/s (9.10 s/file)` | `483.98 MiB/s (8.46 s/file)` | `895.88 MiB/s` | `892.99 MiB/s` | **3.25x** | **3.03x** |
| **Write small file** | `56.60 files/s (70.71 ms/file)` | `583.00 files/s (6.86 ms/file)` | `985.40 files/s (4.06 ms/file)` | `86.40 files/s` | `109.10 files/s` | **0.10x** | **0.06x** |
| **Read small file** | `1415.20 files/s (2.83 ms/file)` | `1112.60 files/s (3.60 ms/file)` | `2511.70 files/s (1.59 ms/file)` | `1857.90 files/s` | `937.30 files/s` | **1.27x** | **0.56x** |
| **Stat file** | `7187.70 files/s (0.56 ms/file)` | `13985.80 files/s (0.29 ms/file)` | `11337.10 files/s (0.35 ms/file)` | `7845.80 files/s` | `11989.50 files/s` | **0.51x** | **0.63x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `—` | `305.90 MiB/s (76.5 IOPS)` | `295.48 MiB/s (73.9 IOPS)` | **—** | **—** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `—` | `184.16 MiB/s (46.0 IOPS)` | `184.03 MiB/s (46.0 IOPS)` | **—** | **—** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `—` | `197.52 MiB/s (49.4 IOPS)` | `198.50 MiB/s (49.6 IOPS)` | **—** | **—** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `—` | `175.65 MiB/s (43.9 IOPS)` | `176.67 MiB/s (44.2 IOPS)` | **—** | **—** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `—` | `329.53 MiB/s (329.5 IOPS)` | `325.93 MiB/s (325.9 IOPS)` | **—** | **—** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `—` | `184.63 MiB/s (184.6 IOPS)` | `180.00 MiB/s (180.0 IOPS)` | **—** | **—** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `—` | `289.51 MiB/s (289.5 IOPS)` | `290.50 MiB/s (290.5 IOPS)` | **—** | **—** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `—` | `119.51 MiB/s (119.5 IOPS)` | `116.73 MiB/s (116.7 IOPS)` | **—** | **—** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `—` | `10.69 / 42.73 / 83.36 ms` | `11.52 / 43.78 / 74.97 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `—` | `21.72 / 120.06 / 170.92 ms` | `21.73 / 123.21 / 166.72 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `—` | `309.22 / 442.50 / 450.89 ms` | `309.82 / 354.42 / 446.69 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `—` | `349.40 / 1069.55 / 1400.90 ms` | `351.24 / 1044.38 / 1317.01 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `—` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `—` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `—` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `—` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比本轮实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `—` | `987.6` | `2,293.0` | `1,416.6` | `192.3` | `6.0` | **—** | **—** |
| **Directory stat** | `—` | `4,648.6` | `10,315.8` | `3,810.1` | `1,311.2` | `435.9` | **—** | **—** |
| **Directory removal** | `—` | `821.0` | `1,816.6` | `1,115.1` | `213.1` | `9.0` | **—** | **—** |
| **File creation** | `—` | `1,125.1` | `2,182.5` | `1,410.3` | `179.3` | `5.7` | **—** | **—** |
| **File stat** | `—` | `4,565.2` | `15,949.3` | `5,023.2` | `915.2` | `68.7` | **—** | **—** |
| **File read** | `—` | `2,379.4` | `4,764.8` | `3,487.9` | `371.0` | `33.9` | **—** | **—** |
| **File removal** | `—` | `938.5` | `2,102.5` | `1,163.4` | `217.5` | `23.7` | **—** | **—** |
| **Tree creation** | `—` | `956.8` | `2,315.0` | `1,503.0` | `187.9` | `6.0` | **—** | **—** |
| **Tree removal** | `—` | `738.2` | `1,957.8` | `1,119.8` | `218.4` | `9.9` | **—** | **—** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `—` | `2,671.5` | `6,321.2` | **—** | **—** |
| **Directory stat** | `—` | `361,672.9` | `62,463.1` | **—** | **—** |
| **Directory removal** | `—` | `2,894.8` | `6,091.3` | **—** | **—** |
| **File creation** | `—` | `3,426.6` | `6,917.6` | **—** | **—** |
| **File stat** | `—` | `302,963.2` | `459,345.6` | **—** | **—** |
| **File read** | `—` | `14,326.5` | `18,624.6` | **—** | **—** |
| **File removal** | `—` | `3,306.4` | `7,908.1` | **—** | **—** |
| **Tree creation** | `—` | `316.1` | `583.9` | **—** | **—** |
| **Tree removal** | `—` | `649.6` | `1,565.1` | **—** | **—** |

---

## 6. 本轮观测与比较边界


服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261101-180757`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9a8rs2hdzun6vaaiu6`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9a8rs2hdzun6vaaiu7`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-01T18:08:00Z` |
| **实例销毁时间 (UTC)** | `2026-11-01T18:24:54Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
