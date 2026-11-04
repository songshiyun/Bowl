# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告

> 首次尝试的随机直接写因 PostgreSQL 连接中断而失败，原因未确定；本报告仅统计完整重跑结果。失败原始数据见 [首次运行记录](attempts/20261104-first/README.md)。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费双机环境（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 [JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。

本次评测采用同规格双机、VPC 私网与 120 GiB `cloud_essd` 云盘，依次运行以下三组目标：

1. **Bowl（PostgreSQL 16 控制面 + Pebble）**：PostgreSQL 仅保存 `disks`、`commits`、`client_sessions`；每个分支独立的 Pebble 保存 Inode、Entry、Xattr、Orphan 和 4 MiB 数据块，使用 Set 和 ValueMerger 处理整块与局部写入，并以硬链接 Checkpoint 支持快照和 Fork。FileHandle、FileLock、O_TMPFILE 与按需加载的 NamespaceCache 由客户端维护。
   当前提交禁用 FUSE 内核 `writeback_cache`；Flush 和 Fsync 请求服务端同步，Release 负责释放本地句柄。该配置与 ADR-0029 的 writeback_cache 设计不同。
2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。
3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 rclone S3 存储（`--trash-days 0`）。

### 1.1 核心指标本轮实测总览表

| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| `juicefs bench` (-p 4) | **大文件并发写 (1GiB×4)** | `27.20 MiB/s` | `305.90 MiB/s` | `305.51 MiB/s` | **0.09x** | **0.09x** |
| `juicefs bench` (-p 4) | **大文件并发读 (1GiB×4)** | `88.85 MiB/s` | `451.96 MiB/s` | `447.56 MiB/s` | **0.20x** | **0.20x** |
| `juicefs bench` (-p 4) | **小文件并发写 (128KiB×400)** | `13.10 files/s` | `473.00 files/s` | `928.80 files/s` | **0.03x** | **0.01x** |
| `juicefs bench` (-p 4) | **小文件并发读 (128KiB×400)** | `44.90 files/s` | `3,316.70 files/s` | `2,020.50 files/s` | **0.01x** | **0.02x** |
| `juicefs bench` (-p 4) | **小文件并发 Stat (400 files)** | `18,201.40 files/s` | `13,620.80 files/s` | `46,891.50 files/s` | **1.34x** | **0.39x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序写 (1 job · 4GiB)** | `37.39 MiB/s` | `290.46 MiB/s` | `302.89 MiB/s` | **0.13x** | **0.12x** |
| `fio` 吞吐量 (bs=4M) | **单流顺序读 (1 job · 4GiB)** | `18.46 MiB/s` | `184.57 MiB/s` | `184.48 MiB/s` | **0.10x** | **0.10x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序写 (16 jobs · 16GiB)** | `22.39 MiB/s` | `198.97 MiB/s` | `199.05 MiB/s` | **0.11x** | **0.11x** |
| `fio` 吞吐量 (bs=4M) | **16 并发顺序读 (16 jobs · 16GiB)** | `99.21 MiB/s` | `176.25 MiB/s` | `178.79 MiB/s` | **0.56x** | **0.55x** |
| `mdtest` (12,440 节点) | **目录创建 (Directory creation)** | `53.59 ops/s` | `503.88 ops/s` | `2,259.37 ops/s` | **0.11x** | **0.02x** |
| `mdtest` (12,440 节点) | **文件创建 (File creation)** | `58.22 ops/s` | `1,136.05 ops/s` | `2,273.73 ops/s` | **0.05x** | **0.03x** |
| `mdtest` (12,440 节点) | **文件 Stat (File stat)** | `227.04 ops/s` | `4,602.64 ops/s` | `16,592.10 ops/s` | **0.05x** | **0.01x** |
| `mdtest` (12,440 节点) | **文件删除 (File removal)** | `62.41 ops/s` | `954.72 ops/s` | `2,143.77 ops/s` | **0.07x** | **0.03x** |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj920xxtjyecqzjas9gm` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.196` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj920xxtjyecqzjas9gn` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (120 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.197` |

### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线

| 基线指标 | 实测数值 | 采集方式与说明 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.033/0.069/0.350/0.069` ms | `ping -c 20 -i 0.05 172.16.1.196` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.196 -t 5` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `169.41` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |
| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `170.45` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |

### 2.3 软件版本与三组存储栈配置矩阵

| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |
| :--- | :--- | :--- | :--- |
| **客户端版本** | Bowl `0ab26a7` (`fusermount3` fusermount3 version: 3.14.0) | `juicefs version 1.2.1+2024-08-30.cd871d1` | `juicefs version 1.2.1+2024-08-30.cd871d1` |
| **控制面 / 文件元数据** | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) 控制面 / Pebble 分支存储 | psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1) (`jfs_pg` 库) | Redis server v=7.0.15 (`AOF everysec`) |
| **数据块存储 (Server 节点)** | 每分支 Pebble (`/var/lib/bowl/data`) | rclone S3 (`http://172.16.1.196:9001/jfs-pg`) | rclone S3 (`http://172.16.1.196:9001/jfs-redis`) |
| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |
| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |
| **评测工具版本** | `juicefs bench` / `fio-3.36` / `mdtest-3.3.0` | 同左 | 同左 |

---

## 3. 套件一：基础测试（`juicefs bench -p 4`）

使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试：依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。

| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Write big file** | `27.20 MiB/s (150.60 s/file)` | `305.90 MiB/s (13.39 s/file)` | `305.51 MiB/s (13.41 s/file)` | **0.09x** | **0.09x** |
| **Read big file** | `88.85 MiB/s (46.10 s/file)` | `451.96 MiB/s (9.06 s/file)` | `447.56 MiB/s (9.15 s/file)` | **0.20x** | **0.20x** |
| **Write small file** | `13.10 files/s (304.81 ms/file)` | `473.00 files/s (8.46 ms/file)` | `928.80 files/s (4.31 ms/file)` | **0.03x** | **0.01x** |
| **Read small file** | `44.90 files/s (89.12 ms/file)` | `3316.70 files/s (1.21 ms/file)` | `2020.50 files/s (1.98 ms/file)` | **0.01x** | **0.02x** |
| **Stat file** | `18201.40 files/s (0.22 ms/file)` | `13620.80 files/s (0.29 ms/file)` | `46891.50 files/s (0.09 ms/file)` | **1.34x** | **0.39x** |

---

## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）

### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）

| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1` | `37.39 MiB/s (9.3 IOPS)` | `290.46 MiB/s (72.6 IOPS)` | `302.89 MiB/s (75.7 IOPS)` | **0.13x** | **0.12x** |
| **顺序读 (1 并发 · 4M · 4GiB)** | `fio.md: --rw=read --bs=4M --size=4G --numjobs=1` | `18.46 MiB/s (4.6 IOPS)` | `184.57 MiB/s (46.1 IOPS)` | `184.48 MiB/s (46.1 IOPS)` | **0.10x** | **0.10x** |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1` | `22.39 MiB/s (5.6 IOPS)` | `198.97 MiB/s (49.7 IOPS)` | `199.05 MiB/s (49.8 IOPS)` | **0.11x** | **0.11x** |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `fio.md: --rw=read --bs=4M --size=1G --numjobs=16` | `99.21 MiB/s (24.8 IOPS)` | `176.25 MiB/s (44.1 IOPS)` | `178.79 MiB/s (44.7 IOPS)` | **0.56x** | **0.55x** |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1` | `26.09 MiB/s (26.1 IOPS)` | `332.33 MiB/s (332.3 IOPS)` | `330.78 MiB/s (330.8 IOPS)` | **0.08x** | **0.08x** |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1` | `156.22 MiB/s (156.2 IOPS)` | `184.70 MiB/s (184.7 IOPS)` | `184.50 MiB/s (184.5 IOPS)` | **0.85x** | **0.85x** |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1` | `16.15 MiB/s (16.1 IOPS)` | `306.47 MiB/s (306.5 IOPS)` | `299.52 MiB/s (299.5 IOPS)` | **0.05x** | **0.05x** |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1` | `61.74 MiB/s (61.7 IOPS)` | `122.36 MiB/s (122.4 IOPS)` | `120.05 MiB/s (120.0 IOPS)` | **0.50x** | **0.51x** |

### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）

| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |
| :--- | ---: | ---: | ---: |
| **顺序写 (1 并发 · 4M · 4GiB)** | `106.60 / 110.62 / 115.87 ms` | `11.16 / 43.25 / 74.97 ms` | `11.12 / 43.25 / 73.92 ms` |
| **顺序读 (1 并发 · 4M · 4GiB)** | `216.68 / 263.19 / 270.53 ms` | `21.67 / 117.97 / 168.82 ms` | `21.68 / 112.72 / 158.34 ms` |
| **多并发顺序写 (16 并发 · 4M · 16GiB)** | `2853.80 / 3539.99 / 3707.76 ms` | `308.23 / 358.61 / 446.69 ms` | `308.55 / 354.42 / 450.89 ms` |
| **多并发顺序读 (16 并发 · 4M · 16GiB)** | `640.33 / 742.39 / 809.50 ms` | `347.38 / 1027.60 / 1350.57 ms` | `349.26 / 968.88 / 1166.02 ms` |
| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 顺序读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |
| **Direct 随机写 (4 并发 · 1M · 4GiB)** | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.00 / 0.01 ms` |
| **Direct 随机读 (4 并发 · 1M · 4GiB)** | `0.00 / 0.00 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` | `0.00 / 0.01 / 0.01 ms` |

---

## 5. 套件三：元数据 IOPS 测试（`mdtest`）

### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）

使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `53.6` | `503.9` | `2,259.4` | **0.11x** | **0.02x** |
| **Directory stat** | `210.8` | `1,182.6` | `16,471.8` | **0.18x** | **0.01x** |
| **Directory removal** | `60.9` | `534.6` | `1,958.4` | **0.11x** | **0.03x** |
| **File creation** | `58.2` | `1,136.1` | `2,273.7` | **0.05x** | **0.03x** |
| **File stat** | `227.0` | `4,602.6` | `16,592.1` | **0.05x** | **0.01x** |
| **File read** | `218.1` | `2,406.5` | `4,899.8` | **0.09x** | **0.04x** |
| **File removal** | `62.4` | `954.7` | `2,143.8` | **0.07x** | **0.03x** |
| **Tree creation** | `75.3` | `894.9` | `2,361.8` | **0.08x** | **0.03x** |
| **Tree removal** | `88.3` | `733.5` | `1,981.5` | **0.12x** | **0.04x** |

### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）

匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：

| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |
| :--- | ---: | ---: | ---: | ---: | ---: |
| **Directory creation** | `80.1` | `2,694.0` | `6,459.7` | **0.03x** | **0.01x** |
| **Directory stat** | `322.2` | `356,347.5` | `64,485.6` | **0.00x** | **0.00x** |
| **Directory removal** | `77.4` | `2,993.2` | `6,318.5` | **0.03x** | **0.01x** |
| **File creation** | `73.0` | `3,402.1` | `7,017.0` | **0.02x** | **0.01x** |
| **File stat** | `321.0` | `307,659.7` | `475,808.3` | **0.00x** | **0.00x** |
| **File read** | `14,693.3` | `14,085.4` | `18,793.2` | **1.04x** | **0.78x** |
| **File removal** | `80.9` | `3,256.9` | `8,002.7` | **0.02x** | **0.01x** |
| **Tree creation** | `21.3` | `374.7` | `363.3` | **0.06x** | **0.06x** |
| **Tree removal** | `22.1` | `573.1` | `1,612.6` | **0.04x** | **0.01x** |

---

## 6. 本轮观测与比较边界

- Bowl 单流顺序写：`37.39 MiB/s`；同轮 JuiceFS(PG) 为 `290.46 MiB/s`。
- Bowl 单流顺序读：`18.46 MiB/s`；同轮 JuiceFS(PG) 为 `184.57 MiB/s`。
- Bowl 16 并发顺序写：`22.39 MiB/s`；同轮 JuiceFS(PG) 为 `198.97 MiB/s`。
- Bowl 16 并发顺序读：`99.21 MiB/s`；同轮 JuiceFS(PG) 为 `176.25 MiB/s`。

服务端顺序读写物理基线、请求大小与并发数见第 2、4 节。不同请求大小与持久化路径的吞吐不能直接解释为云盘利用率。

Bowl 的写入和同步路径使用 Pebble；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。

`juicefs bench` 内部清理内核页缓存，但不清理 JuiceFS 客户端本地块缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的缓存清理不能改变这一边界。

`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。

ADR-0029 同时改变服务端存储和客户端状态管理；单轮数据不能将变化归因于其中某一项。

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-jfsbench-kp-20261103-233947`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj920xxtjyecqzjas9gm`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj920xxtjyecqzjas9gn`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-03T23:39:56Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T00:50:18Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
