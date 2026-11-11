# Bowl 分布式文件系统 — 阿里云硅谷双机内网 `pjdfstest` 基准测试报告

被测版本采用 ADR-0030 / ADR-0031 / ADR-0032 单盘独立 Actor 存储引擎与 `64 MiB` Append-Only Pack 数据层：PostgreSQL 18（deploy/prod 容器栈）仅保存单张 `disks` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在 `packs/<pack_id_hex16>.pack` 的 `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `WriteRequest` 池化零分配解码、活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。

客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备对称的 `WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 顺序预读与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。测试挂载保持 `strictatime` 与 0 秒属性/目录项超时，数据目录全程挂载于本地 NVMe SSD（`local_ssd_pro` 894 GiB）。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 在阿里云硅谷地域（`us-west-1`）真实开启 2 台按量计费本地 NVMe SSD 云服务器（`ecs.i5.xlarge` × 2，1 台 `storage-service` + PostgreSQL 服务端节点，1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行权威 POSIX 文件系统兼容性与基准测试集 **[pjdfstest](https://github.com/pjd/pjdfstest)** 的完整实测结果。

| 核心指标 | 实测结果 |
| :--- | :--- |
| **测试结论** | **100% PASS (全量通过)** |
| **测试系统调用目录数** | `17` 个系统调用族 |
| **测试脚本文件数 (`.t`)** | `238` 个测试脚本 |
| **总测试断言数 (Subtests)** | `8,798` 项 POSIX 断言 |
| **通过 / 失败断言数** | `8,798` 通过 / `0` 失败（通过率 **100.00%**） |
| **TAP TODO** | `28` 项，其中 `9` 项为预期失败；按 `prove` 规则不计入失败 |
| **全量执行总耗时** | `172.84 s`（约 `2.88 min`） |
| **内网综合执行吞吐率** | `50.90 tests/s`（平均单断言耗时 `19.65 ms`） |
| **云端地域与网络拓扑** | 阿里云 `us-west-1` (`us-west-1a`) 同可用区 VPC 私网直连 |
| **测试执行时间 (UTC)** | `2026-11-11T19:13:25Z` ~ `2026-11-11T19:16:27Z` |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj990ykeh6eh5p2guwye` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme1n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.190:9000` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj990ykeh6eh5p2guwyf` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.191` |

### 2.2 同可用区 VPC 内网与本地 NVMe SSD 性能基线

| 指标 | 实测数值 | 采集方式 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.047/0.083/0.227/0.048` ms | `ping -c 20 -i 0.05 172.16.1.190` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.190 -t 5` |
| **Server 本地 NVMe SSD 4 MiB 顺序写 / 读基线** | `1998.05` / `3494.88` MiB/s | `fio --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69933` / `699825` IOPS | `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `49872` IOPS (P99 `64.4` µs) | `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与挂载运行参数

| 组件 / 参数项 | 配置值 | 说明 |
| :--- | :--- | :--- |
| **Bowl Git Commit** | `8afc63c` | 仓库当前评测版本 |
| **部署编排与可观测性栈** | `deploy/prod (docker-compose.yaml + storage.env + strictatime/0s TTL override)` | `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` |
| **pjdfstest Git Commit** | `85a8aea` | 上游 `https://github.com/pjd/pjdfstest` |
| **PostgreSQL 版本** | `postgres (PostgreSQL) 18.6` | Server 节点控制面数据库（单张 `disks` 表） |
| **FUSE 用户态版本** | `fusermount3 version: 3.14.0` | Linux 内核 FUSE 协议 + `github.com/hanwen/go-fuse/v2` |
| **测试虚拟磁盘 (`DiskID` / `RootIno`)** | `01a1cc07-7e24-742e-8f07-e277e3ec8a70` (`root_ino=1`) | 配额 `2PiB` |
| **服务端 `STORAGE_ATIME_MODE`** | `strictatime` | 开启严格访问时间更新，验证 `atime` 刷新语义 |
| **FUSE 缓存超时配置** | `entry=0s, attr=0s, direntry=0s` | 内核属性与目录项缓存超时；客户端 NamespaceCache 仍可命中 |
| **内核回写缓存与数据分片** | `writeback_cache=False`, `keep_cache=True`, `max_background=64`, `block_grid=262144` 字节, `pack=67108864` 字节 | `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`<64 KiB` 直通）+ 服务端 64 MiB `.pack` 封包文件 |
| **服务端 WAL 与落盘策略** | `pebble.NoSync Group Commit + Fsync/FsyncDir 双层并行落盘屏障 (fdatasync + LogData)` | 常规写批次 Group Commit 异步提交，显式 Fsync/Sync 执行双层并行落盘屏障 |
| **客户端挂载命令** | `bowl mount <disk_id> /mnt/bowl --endpoint http://172.16.1.190:9000 --allow-other` | 启用 `default_permissions` 与 `--allow-other` 支持多 UID/GID 权限切换测试 |

---

## 3. 17 个系统调用目录兼容性与耗时汇总

| 系统调用目录 | 脚本数 (Files) | 断言数 (Tests) | PASS / ALL | 退出码 | 通过率 | 耗时 (s) | 吞吐率 (Tests/s) | 耗时占比 | 核心覆盖语义 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `chflags/` | 14 | 14 | 14/14 | 0 | 100.00% | 0.14s | 103.7 | 0.1% | 文件扩展标志位（FS_IOC_GETFLAGS / FS_IOC_SETFLAGS，验证 immutable / append 约束） |
| `chmod/` | 13 | 327 | 327/327 | 0 | 100.00% | 13.56s | 24.1 | 7.8% | 权限位变更、SUID/SGID 粘滞位保留与剥离、多身份权限检查 |
| `chown/` | 11 | 1,497 | 1497/1497 | 0 | 100.00% | 80.19s | 18.7 | 46.4% | 文件与符号链接属主/属组变更（chown/lchown/fchown）、非特权组约束与 SUID/SGID 清除 |
| `ftruncate/` | 15 | 89 | 89/89 | 0 | 100.00% | 3.03s | 29.4 | 1.8% | 基于已打开文件描述符的长度截断与扩展、只读句柄拒绝（EBADF/EINVAL） |
| `granular/` | 7 | 7 | 7/7 | 0 | 100.00% | 0.09s | 77.8 | 0.1% | 细粒度访问控制与基础文件操作边界校验 |
| `link/` | 18 | 359 | 359/359 | 0 | 100.00% | 11.65s | 30.8 | 6.7% | 硬链接创建、nlink 计数递增、跨目录硬链接与目录硬链接禁止（EPERM） |
| `mkdir/` | 13 | 118 | 118/118 | 0 | 100.00% | 2.04s | 57.7 | 1.2% | 目录创建、权限掩码、父目录写/执行权限校验、路径长度与存在性冲突（EEXIST） |
| `mkfifo/` | 13 | 120 | 120/120 | 0 | 100.00% | 2.05s | 58.6 | 1.2% | 命名管道（FIFO 特殊文件）创建与权限位验证 |
| `mknod/` | 12 | 186 | 186/186 | 0 | 100.00% | 4.19s | 44.4 | 2.4% | 字符设备、块设备、FIFO 与套接字特殊节点创建及权限验证 |
| `open/` | 27 | 337 | 337/337 | 0 | 100.00% | 4.60s | 73.3 | 2.7% | 文件打开标志组合（O_CREAT/O_EXCL/O_TRUNC/O_RDONLY/O_WRONLY/O_RDWR/O_NOFOLLOW）与权限矩阵 |
| `posix_fallocate/` | 1 | 1 | 1/1 | 0 | 100.00% | 0.05s | 20.0 | 0.0% | 物理空间预分配（fallocate Mode=0）、文件大小扩展与配额联动 |
| `rename/` | 25 | 4,857 | 4857/4857 | 0 | 100.00% | 23.86s | 203.6 | 13.8% | 同目录/跨目录原子重命名、覆盖非空目录拦截（ENOTEMPTY/EISDIR/ENOTDIR）及权限矩阵 |
| `rmdir/` | 16 | 145 | 145/145 | 0 | 100.00% | 2.15s | 67.3 | 1.2% | 空目录删除、非空目录拒绝（ENOTEMPTY）、当前目录/点目录拒绝与权限检查 |
| `symlink/` | 13 | 95 | 95/95 | 0 | 100.00% | 2.02s | 47.1 | 1.2% | 符号链接创建、目标路径长度边界与 readlink 解析 |
| `truncate/` | 15 | 84 | 84/84 | 0 | 100.00% | 3.00s | 28.0 | 1.7% | 基于路径的文件截断、稀疏扩展零填充、尺寸变更触发 ctime/mtime 更新 |
| `unlink/` | 15 | 440 | 440/440 | 0 | 100.00% | 18.80s | 23.4 | 10.9% | 文件与特殊节点删除、nlink 递减、Open-Unlink 孤儿句柄读写保活与延迟回收 |
| `utimensat/` | 10 | 122 | 122/122 | 0 | 100.00% | 1.44s | 85.0 | 0.8% | 纳秒级时间戳更新（UTIME_NOW / UTIME_OMIT / 显式时间戳）与权限校验 |
| **Total (合计)** | **238** | **8,798** | **8798/8798** | **全部为 0** | **100.00%** | **172.84s** | **50.9** | **100.0%** | **17 个系统调用目录** |

---

## 4. 内网性能特征与关键 POSIX 语义验证分析

### 4.1 跨机内网 RPC 延迟与吞吐分布剖析

- **内网通信底座**：Client 与 Server 位于阿里云硅谷 `us-west-1a` 同一交换机（`vsw-rj9rsvsgpihtsgxej6me5`），内网 ICMP RTT 延迟仅 `0.047/0.083/0.227/0.048` ms，内网带宽达 `21.22` Gbps。内核属性与目录项缓存超时设为 `0s`；客户端 NamespaceCache 仍会影响请求路径。
- **耗时集中度分析**：耗时排名前三的子集为 `chown/`（1,497 项断言，耗时 80.19s，占比 46.4%）、`rename/`（4,857 项断言，耗时 23.86s，占比 13.8%）、`unlink/`（440 项断言，耗时 18.80s，占比 10.9%）。

### 4.2 关键 POSIX 契约与边界语义验证结论

本轮磁盘根 Inode 为 `1`。用例覆盖标志位、多用户权限、打开后解链和空间预分配；各目录及脚本结果见第 3、5 节。

---

## 5. 全量 238 个测试脚本明细表（Case-by-Case Breakdown）

| 测试脚本 (`Case`) | 所属目录 | 断言数 (`Tests`) | 通过/总数 (`PASS/ALL`) | 耗时 (`ms`) | 状态 |
| :--- | :--- | ---: | ---: | ---: | :--- |
| `chflags/00.t` | `chflags/` | 1 | 1/1 | 6 ms | PASS |
| `chflags/01.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/02.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/03.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/04.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/05.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/06.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/07.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/08.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/09.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/10.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/11.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/12.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/13.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/00.t` | `chmod/` | 119 | 119/119 | 12,288 ms | PASS |
| `chmod/01.t` | `chmod/` | 17 | 17/17 | 46 ms | PASS |
| `chmod/02.t` | `chmod/` | 5 | 5/5 | 44 ms | PASS |
| `chmod/03.t` | `chmod/` | 5 | 5/5 | 582 ms | PASS |
| `chmod/04.t` | `chmod/` | 7 | 7/7 | 27 ms | PASS |
| `chmod/05.t` | `chmod/` | 14 | 14/14 | 45 ms | PASS |
| `chmod/06.t` | `chmod/` | 8 | 8/8 | 67 ms | PASS |
| `chmod/07.t` | `chmod/` | 25 | 25/25 | 70 ms | PASS |
| `chmod/08.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/09.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/10.t` | `chmod/` | 2 | 2/2 | 8 ms | PASS |
| `chmod/11.t` | `chmod/` | 109 | 109/109 | 252 ms | PASS |
| `chmod/12.t` | `chmod/` | 14 | 14/14 | 44 ms | PASS |
| `chown/00.t` | `chown/` | 1280 | 1280/1280 | 78,902 ms | PASS |
| `chown/01.t` | `chown/` | 22 | 22/22 | 56 ms | PASS |
| `chown/02.t` | `chown/` | 10 | 10/10 | 57 ms | PASS |
| `chown/03.t` | `chown/` | 10 | 10/10 | 602 ms | PASS |
| `chown/04.t` | `chown/` | 9 | 9/9 | 31 ms | PASS |
| `chown/05.t` | `chown/` | 18 | 18/18 | 51 ms | PASS |
| `chown/06.t` | `chown/` | 10 | 10/10 | 90 ms | PASS |
| `chown/07.t` | `chown/` | 132 | 132/132 | 303 ms | PASS |
| `chown/08.t` | `chown/` | 1 | 1/1 | 4 ms | PASS |
| `chown/09.t` | `chown/` | 1 | 1/1 | 4 ms | PASS |
| `chown/10.t` | `chown/` | 4 | 4/4 | 11 ms | PASS |
| `ftruncate/00.t` | `ftruncate/` | 26 | 26/26 | 2,075 ms | PASS |
| `ftruncate/01.t` | `ftruncate/` | 5 | 5/5 | 22 ms | PASS |
| `ftruncate/02.t` | `ftruncate/` | 5 | 5/5 | 47 ms | PASS |
| `ftruncate/03.t` | `ftruncate/` | 5 | 5/5 | 601 ms | PASS |
| `ftruncate/04.t` | `ftruncate/` | 4 | 4/4 | 18 ms | PASS |
| `ftruncate/05.t` | `ftruncate/` | 15 | 15/15 | 48 ms | PASS |
| `ftruncate/06.t` | `ftruncate/` | 8 | 8/8 | 28 ms | PASS |
| `ftruncate/07.t` | `ftruncate/` | 6 | 6/6 | 44 ms | PASS |
| `ftruncate/08.t` | `ftruncate/` | 1 | 1/1 | 5 ms | PASS |
| `ftruncate/09.t` | `ftruncate/` | 3 | 3/3 | 14 ms | PASS |
| `ftruncate/10.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/11.t` | `ftruncate/` | 1 | 1/1 | 5 ms | PASS |
| `ftruncate/12.t` | `ftruncate/` | 3 | 3/3 | 15 ms | PASS |
| `ftruncate/13.t` | `ftruncate/` | 4 | 4/4 | 15 ms | PASS |
| `ftruncate/14.t` | `ftruncate/` | 2 | 2/2 | 9 ms | PASS |
| `granular/00.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/01.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/02.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/03.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/04.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/05.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/06.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `link/00.t` | `link/` | 202 | 202/202 | 10,437 ms | PASS |
| `link/01.t` | `link/` | 32 | 32/32 | 82 ms | PASS |
| `link/02.t` | `link/` | 10 | 10/10 | 61 ms | PASS |
| `link/03.t` | `link/` | 13 | 13/13 | 612 ms | PASS |
| `link/04.t` | `link/` | 6 | 6/6 | 26 ms | PASS |
| `link/05.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/06.t` | `link/` | 18 | 18/18 | 59 ms | PASS |
| `link/07.t` | `link/` | 17 | 17/17 | 57 ms | PASS |
| `link/08.t` | `link/` | 10 | 10/10 | 72 ms | PASS |
| `link/09.t` | `link/` | 5 | 5/5 | 21 ms | PASS |
| `link/10.t` | `link/` | 23 | 23/23 | 59 ms | PASS |
| `link/11.t` | `link/` | 9 | 9/9 | 33 ms | PASS |
| `link/12.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/13.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/14.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/15.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/16.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/17.t` | `link/` | 8 | 8/8 | 23 ms | PASS |
| `mkdir/00.t` | `mkdir/` | 36 | 36/36 | 1,084 ms | PASS |
| `mkdir/01.t` | `mkdir/` | 17 | 17/17 | 47 ms | PASS |
| `mkdir/02.t` | `mkdir/` | 3 | 3/3 | 42 ms | PASS |
| `mkdir/03.t` | `mkdir/` | 3 | 3/3 | 585 ms | PASS |
| `mkdir/04.t` | `mkdir/` | 3 | 3/3 | 16 ms | PASS |
| `mkdir/05.t` | `mkdir/` | 12 | 12/12 | 40 ms | PASS |
| `mkdir/06.t` | `mkdir/` | 12 | 12/12 | 40 ms | PASS |
| `mkdir/07.t` | `mkdir/` | 6 | 6/6 | 44 ms | PASS |
| `mkdir/08.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/09.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/10.t` | `mkdir/` | 21 | 21/21 | 51 ms | PASS |
| `mkdir/11.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/12.t` | `mkdir/` | 2 | 2/2 | 8 ms | PASS |
| `mkfifo/00.t` | `mkfifo/` | 36 | 36/36 | 1,084 ms | PASS |
| `mkfifo/01.t` | `mkfifo/` | 17 | 17/17 | 48 ms | PASS |
| `mkfifo/02.t` | `mkfifo/` | 4 | 4/4 | 44 ms | PASS |
| `mkfifo/03.t` | `mkfifo/` | 4 | 4/4 | 587 ms | PASS |
| `mkfifo/04.t` | `mkfifo/` | 3 | 3/3 | 16 ms | PASS |
| `mkfifo/05.t` | `mkfifo/` | 12 | 12/12 | 41 ms | PASS |
| `mkfifo/06.t` | `mkfifo/` | 12 | 12/12 | 41 ms | PASS |
| `mkfifo/07.t` | `mkfifo/` | 6 | 6/6 | 41 ms | PASS |
| `mkfifo/08.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/09.t` | `mkfifo/` | 21 | 21/21 | 51 ms | PASS |
| `mkfifo/10.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/11.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/12.t` | `mkfifo/` | 2 | 2/2 | 8 ms | PASS |
| `mknod/00.t` | `mknod/` | 36 | 36/36 | 1,088 ms | PASS |
| `mknod/01.t` | `mknod/` | 27 | 27/27 | 66 ms | PASS |
| `mknod/02.t` | `mknod/` | 12 | 12/12 | 60 ms | PASS |
| `mknod/03.t` | `mknod/` | 12 | 12/12 | 606 ms | PASS |
| `mknod/04.t` | `mknod/` | 3 | 3/3 | 17 ms | PASS |
| `mknod/05.t` | `mknod/` | 12 | 12/12 | 40 ms | PASS |
| `mknod/06.t` | `mknod/` | 12 | 12/12 | 41 ms | PASS |
| `mknod/07.t` | `mknod/` | 6 | 6/6 | 41 ms | PASS |
| `mknod/08.t` | `mknod/` | 35 | 35/35 | 79 ms | PASS |
| `mknod/09.t` | `mknod/` | 1 | 1/1 | 4 ms | PASS |
| `mknod/10.t` | `mknod/` | 2 | 2/2 | 8 ms | PASS |
| `mknod/11.t` | `mknod/` | 28 | 28/28 | 2,061 ms | PASS |
| `open/00.t` | `open/` | 47 | 47/47 | 3,112 ms | PASS |
| `open/01.t` | `open/` | 22 | 22/22 | 57 ms | PASS |
| `open/02.t` | `open/` | 4 | 4/4 | 44 ms | PASS |
| `open/03.t` | `open/` | 4 | 4/4 | 587 ms | PASS |
| `open/04.t` | `open/` | 4 | 4/4 | 18 ms | PASS |
| `open/05.t` | `open/` | 12 | 12/12 | 39 ms | PASS |
| `open/06.t` | `open/` | 144 | 144/144 | 294 ms | PASS |
| `open/07.t` | `open/` | 25 | 25/25 | 63 ms | PASS |
| `open/08.t` | `open/` | 3 | 3/3 | 17 ms | PASS |
| `open/09.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/10.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/11.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/12.t` | `open/` | 6 | 6/6 | 42 ms | PASS |
| `open/13.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/14.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/15.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/16.t` | `open/` | 6 | 6/6 | 22 ms | PASS |
| `open/17.t` | `open/` | 3 | 3/3 | 13 ms | PASS |
| `open/18.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/19.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/20.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/21.t` | `open/` | 2 | 2/2 | 8 ms | PASS |
| `open/22.t` | `open/` | 21 | 21/21 | 50 ms | PASS |
| `open/23.t` | `open/` | 5 | 5/5 | 17 ms | PASS |
| `open/24.t` | `open/` | 5 | 5/5 | 19 ms | PASS |
| `open/25.t` | `open/` | 6 | 6/6 | 24 ms | PASS |
| `open/26.t` | `open/` | 9 | 9/9 | 26 ms | PASS |
| `posix_fallocate/00.t` | `posix_fallocate/` | 1 | 1/1 | 4 ms | PASS |
| `rename/00.t` | `rename/` | 122 | 122/122 | 7,297 ms | PASS |
| `rename/01.t` | `rename/` | 8 | 8/8 | 58 ms | PASS |
| `rename/02.t` | `rename/` | 6 | 6/6 | 601 ms | PASS |
| `rename/03.t` | `rename/` | 6 | 6/6 | 28 ms | PASS |
| `rename/04.t` | `rename/` | 18 | 18/18 | 60 ms | PASS |
| `rename/05.t` | `rename/` | 17 | 17/17 | 59 ms | PASS |
| `rename/06.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/07.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/08.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/09.t` | `rename/` | 2353 | 2353/2353 | 5,284 ms | PASS |
| `rename/10.t` | `rename/` | 2099 | 2099/2099 | 4,652 ms | PASS |
| `rename/11.t` | `rename/` | 10 | 10/10 | 74 ms | PASS |
| `rename/12.t` | `rename/` | 32 | 32/32 | 85 ms | PASS |
| `rename/13.t` | `rename/` | 32 | 32/32 | 76 ms | PASS |
| `rename/14.t` | `rename/` | 32 | 32/32 | 77 ms | PASS |
| `rename/15.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/16.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/17.t` | `rename/` | 8 | 8/8 | 22 ms | PASS |
| `rename/18.t` | `rename/` | 6 | 6/6 | 26 ms | PASS |
| `rename/19.t` | `rename/` | 6 | 6/6 | 25 ms | PASS |
| `rename/20.t` | `rename/` | 25 | 25/25 | 70 ms | PASS |
| `rename/21.t` | `rename/` | 16 | 16/16 | 52 ms | PASS |
| `rename/22.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/23.t` | `rename/` | 42 | 42/42 | 5,124 ms | PASS |
| `rename/24.t` | `rename/` | 13 | 13/13 | 48 ms | PASS |
| `rmdir/00.t` | `rmdir/` | 10 | 10/10 | 1,033 ms | PASS |
| `rmdir/01.t` | `rmdir/` | 14 | 14/14 | 42 ms | PASS |
| `rmdir/02.t` | `rmdir/` | 4 | 4/4 | 44 ms | PASS |
| `rmdir/03.t` | `rmdir/` | 5 | 5/5 | 595 ms | PASS |
| `rmdir/04.t` | `rmdir/` | 4 | 4/4 | 19 ms | PASS |
| `rmdir/05.t` | `rmdir/` | 6 | 6/6 | 41 ms | PASS |
| `rmdir/06.t` | `rmdir/` | 23 | 23/23 | 63 ms | PASS |
| `rmdir/07.t` | `rmdir/` | 10 | 10/10 | 35 ms | PASS |
| `rmdir/08.t` | `rmdir/` | 10 | 10/10 | 35 ms | PASS |
| `rmdir/09.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/10.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/11.t` | `rmdir/` | 47 | 47/47 | 113 ms | PASS |
| `rmdir/12.t` | `rmdir/` | 6 | 6/6 | 23 ms | PASS |
| `rmdir/13.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/14.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/15.t` | `rmdir/` | 2 | 2/2 | 8 ms | PASS |
| `symlink/00.t` | `symlink/` | 14 | 14/14 | 1,041 ms | PASS |
| `symlink/01.t` | `symlink/` | 5 | 5/5 | 22 ms | PASS |
| `symlink/02.t` | `symlink/` | 7 | 7/7 | 54 ms | PASS |
| `symlink/03.t` | `symlink/` | 6 | 6/6 | 600 ms | PASS |
| `symlink/04.t` | `symlink/` | 3 | 3/3 | 16 ms | PASS |
| `symlink/05.t` | `symlink/` | 12 | 12/12 | 40 ms | PASS |
| `symlink/06.t` | `symlink/` | 12 | 12/12 | 41 ms | PASS |
| `symlink/07.t` | `symlink/` | 6 | 6/6 | 42 ms | PASS |
| `symlink/08.t` | `symlink/` | 21 | 21/21 | 52 ms | PASS |
| `symlink/09.t` | `symlink/` | 1 | 1/1 | 5 ms | PASS |
| `symlink/10.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/11.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/12.t` | `symlink/` | 6 | 6/6 | 17 ms | PASS |
| `truncate/00.t` | `truncate/` | 21 | 21/21 | 2,062 ms | PASS |
| `truncate/01.t` | `truncate/` | 5 | 5/5 | 22 ms | PASS |
| `truncate/02.t` | `truncate/` | 5 | 5/5 | 46 ms | PASS |
| `truncate/03.t` | `truncate/` | 5 | 5/5 | 590 ms | PASS |
| `truncate/04.t` | `truncate/` | 4 | 4/4 | 19 ms | PASS |
| `truncate/05.t` | `truncate/` | 15 | 15/15 | 45 ms | PASS |
| `truncate/06.t` | `truncate/` | 8 | 8/8 | 27 ms | PASS |
| `truncate/07.t` | `truncate/` | 6 | 6/6 | 46 ms | PASS |
| `truncate/08.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/09.t` | `truncate/` | 3 | 3/3 | 14 ms | PASS |
| `truncate/10.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/11.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/12.t` | `truncate/` | 3 | 3/3 | 14 ms | PASS |
| `truncate/13.t` | `truncate/` | 4 | 4/4 | 16 ms | PASS |
| `truncate/14.t` | `truncate/` | 2 | 2/2 | 8 ms | PASS |
| `unlink/00.t` | `unlink/` | 112 | 112/112 | 17,279 ms | PASS |
| `unlink/01.t` | `unlink/` | 5 | 5/5 | 23 ms | PASS |
| `unlink/02.t` | `unlink/` | 4 | 4/4 | 44 ms | PASS |
| `unlink/03.t` | `unlink/` | 4 | 4/4 | 587 ms | PASS |
| `unlink/04.t` | `unlink/` | 4 | 4/4 | 19 ms | PASS |
| `unlink/05.t` | `unlink/` | 10 | 10/10 | 35 ms | PASS |
| `unlink/06.t` | `unlink/` | 10 | 10/10 | 35 ms | PASS |
| `unlink/07.t` | `unlink/` | 6 | 6/6 | 43 ms | PASS |
| `unlink/08.t` | `unlink/` | 3 | 3/3 | 13 ms | PASS |
| `unlink/09.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/10.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/11.t` | `unlink/` | 270 | 270/270 | 594 ms | PASS |
| `unlink/12.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/13.t` | `unlink/` | 2 | 2/2 | 8 ms | PASS |
| `unlink/14.t` | `unlink/` | 7 | 7/7 | 27 ms | PASS |
| `utimensat/00.t` | `utimensat/` | 32 | 32/32 | 79 ms | PASS |
| `utimensat/01.t` | `utimensat/` | 7 | 7/7 | 1,027 ms | PASS |
| `utimensat/02.t` | `utimensat/` | 10 | 10/10 | 33 ms | PASS |
| `utimensat/03.t` | `utimensat/` | 1 | 1/1 | 4 ms | PASS |
| `utimensat/04.t` | `utimensat/` | 10 | 10/10 | 31 ms | PASS |
| `utimensat/05.t` | `utimensat/` | 16 | 16/16 | 47 ms | PASS |
| `utimensat/06.t` | `utimensat/` | 13 | 13/13 | 41 ms | PASS |
| `utimensat/07.t` | `utimensat/` | 17 | 17/17 | 49 ms | PASS |
| `utimensat/08.t` | `utimensat/` | 9 | 9/9 | 27 ms | PASS |
| `utimensat/09.t` | `utimensat/` | 7 | 7/7 | 29 ms | PASS |
| **Total** | **17 dirs** | **8,798** | **8798/8798** | **172,845 ms** | **100% PASS (全量通过)** |

---

## 6. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `297` 条（跨 Client+Server 全链路 `88` 条，异常链路 `13` 条）· Loki 业务日志 `20` 条（服务端 Warn/Error `18` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `chmod` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `13.792 s` (`13792.0 ms`) vs FUSE 累计活跃 `206.18 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`206.18 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `96.76%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,323 | 206.18 | 0.062 | 0.003 | 0.693 | 100.0% | 错误返回 128 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 189 | 46.46 | 0.246 | 0.219 | 0.512 | 22.5% | 占 FUSE 22.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 72 | 31.31 | 0.435 | 0.397 | 1.024 | 15.2% | 占 FUSE 15.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 61 | 28.96 | 0.475 | 0.418 | 1.024 | 14.0% | 占 FUSE 14.1% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 549 | 176.25 | 0.321 | 0.291 | 2.455 | 85.5% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 549 | 149.98 | 0.273 | 0.246 | 1.963 | 72.7% | Readlink:0.205ms, SetAttr:0.346ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 549 | 49.91 | 0.091 | 0.084 | 2.065 | 24.2% | HTTP Body+Codec 差值均值 0.043 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 549 | 26.27 | 0.048 | 0.045 | 0.492 | 12.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 69 | 4.84 | 0.070 | 0.065 | 0.235 | 2.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 61 | 4.80 | 0.079 | 0.077 | 0.242 | 2.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 284 | 0.49 | 0.002 | 0.003 | 0.008 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 281 | 1.30 | 0.005 | 0.004 | 0.046 | 0.6% | readlink(189), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 357 | 0.63 | 0.002 | 0.003 | 0.012 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.16 | 0.005 | 0.004 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 284 | 3.04 | 0.011 | 0.009 | 0.046 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 284 | 0.71 | 0.003 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 284 | 1.73 | 0.006 | 0.005 | 0.024 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.03 | 0.011 | 0.018 | 0.025 | 0.0% | — |

#### 工作负载 `chown` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `80.849 s` (`80849.0 ms`) vs FUSE 累计活跃 `629.74 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`629.74 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `98.94%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 9,165 | 629.74 | 0.069 | 0.005 | 0.815 | 100.0% | 错误返回 599 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 559 | 190.89 | 0.341 | 0.315 | 0.983 | 30.3% | 占 FUSE 30.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 398 | 154.37 | 0.388 | 0.382 | 1.014 | 24.5% | 占 FUSE 24.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 164 | 79.28 | 0.483 | 0.416 | 1.536 | 12.6% | 占 FUSE 12.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,422 | 527.60 | 0.371 | 0.348 | 1.540 | 83.8% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,422 | 446.30 | 0.314 | 0.296 | 1.200 | 70.9% | Readlink:0.278ms, SetAttr:0.339ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,422 | 147.16 | 0.103 | 0.096 | 0.680 | 23.4% | HTTP Body+Codec 差值均值 0.046 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,422 | 81.31 | 0.057 | 0.052 | 0.340 | 12.9% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 342 | 24.84 | 0.073 | 0.068 | 0.245 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 559 | 18.67 | 0.033 | 0.026 | 0.216 | 3.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 782 | 1.48 | 0.002 | 0.003 | 0.008 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 654 | 3.96 | 0.006 | 0.005 | 0.049 | 0.6% | readlink(559), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 736 | 1.87 | 0.003 | 0.003 | 0.022 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.18 | 0.006 | 0.006 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 782 | 10.56 | 0.014 | 0.010 | 0.049 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 782 | 2.35 | 0.003 | 0.003 | 0.010 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 782 | 5.55 | 0.007 | 0.006 | 0.024 | 0.9% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.30 | 0.297 | 0.250 | 0.299 | 0.0% | — |

#### 工作负载 `ftruncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.263 s` (`3263.0 ms`) vs FUSE 累计活跃 `96.18 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`96.18 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.13%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 2,046 | 96.18 | 0.047 | 0.002 | 0.512 | 100.0% | 错误返回 75 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 17.58 | 0.220 | 0.204 | 1.024 | 18.3% | 占 FUSE 18.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 12.58 | 0.331 | 0.326 | 1.024 | 13.1% | 占 FUSE 13.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 10.88 | 0.286 | 0.234 | 1.024 | 11.3% | 占 FUSE 11.3% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 283 | 82.32 | 0.291 | 0.267 | 0.995 | 85.6% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 283 | 69.46 | 0.245 | 0.227 | 0.746 | 72.2% | Readlink:0.184ms, Mkdir:0.252ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 283 | 23.32 | 0.082 | 0.082 | 0.462 | 24.2% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 283 | 12.86 | 0.045 | 0.040 | 0.423 | 13.4% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 2.02 | 0.053 | 0.048 | 0.231 | 2.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.88 | 0.049 | 0.046 | 0.099 | 2.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 125 | 0.20 | 0.002 | 0.003 | 0.007 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 1.30 | 0.007 | 0.006 | 0.204 | 1.3% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.47 | 0.002 | 0.003 | 0.019 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.26 | 0.008 | 0.008 | 0.042 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 125 | 1.41 | 0.011 | 0.009 | 0.047 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 125 | 0.27 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 125 | 0.66 | 0.005 | 0.005 | 0.021 | 0.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.03 | 0.017 | 0.018 | 0.025 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

#### 工作负载 `link` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `11.890 s` (`11890.0 ms`) vs FUSE 累计活跃 `185.62 ms`（等效并发度 `0.02x`）；占比基准：`FUSE 活跃总耗时` (`185.62 ms`)
- **缓存与流水线命中**：NSCache Attr `99.19%` / Entry `97.01%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,594 | 185.62 | 0.052 | 0.003 | 0.589 | 100.0% | 错误返回 167 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 160 | 32.81 | 0.205 | 0.198 | 0.494 | 17.7% | 占 FUSE 17.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 65 | 30.63 | 0.471 | 0.412 | 1.024 | 16.5% | 占 FUSE 16.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 987 | 20.39 | 0.021 | 0.002 | 0.443 | 11.0% | 占 FUSE 11.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 507 | 156.51 | 0.309 | 0.286 | 0.982 | 84.3% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 507 | 133.10 | 0.263 | 0.243 | 0.849 | 71.7% | Readlink:0.172ms, Unlink:0.344ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 507 | 42.89 | 0.085 | 0.084 | 0.489 | 23.1% | HTTP Body+Codec 差值均值 0.039 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 507 | 23.41 | 0.046 | 0.042 | 0.243 | 12.6% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 65 | 5.21 | 0.080 | 0.074 | 0.241 | 2.8% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 160 | 2.96 | 0.019 | 0.014 | 0.048 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 244 | 0.43 | 0.002 | 0.003 | 0.009 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 284 | 1.40 | 0.005 | 0.005 | 0.042 | 0.8% | readlink(160), readdirplus(31), lookup(51) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 396 | 0.56 | 0.001 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.18 | 0.006 | 0.005 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 244 | 3.24 | 0.013 | 0.012 | 0.049 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 244 | 0.58 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 244 | 1.46 | 0.006 | 0.006 | 0.023 | 0.8% | — |

#### 工作负载 `open` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `4.834 s` (`4834.0 ms`) vs FUSE 累计活跃 `149.70 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`149.70 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `95.97%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,026 | 149.70 | 0.049 | 0.003 | 0.506 | 100.0% | 错误返回 97 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 60 | 26.52 | 0.442 | 0.398 | 1.024 | 17.7% | 占 FUSE 17.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 38 | 17.63 | 0.464 | 0.413 | 1.024 | 11.8% | 占 FUSE 11.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 16.42 | 0.205 | 0.198 | 0.512 | 11.0% | 占 FUSE 11.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 396 | 124.38 | 0.314 | 0.296 | 0.995 | 83.1% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 396 | 105.19 | 0.266 | 0.250 | 0.846 | 70.3% | SetAttr:0.339ms, Readlink:0.173ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 396 | 34.22 | 0.086 | 0.085 | 0.497 | 22.9% | HTTP Body+Codec 差值均值 0.038 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 396 | 19.18 | 0.048 | 0.046 | 0.497 | 12.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 60 | 3.74 | 0.062 | 0.064 | 0.220 | 2.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 38 | 3.12 | 0.082 | 0.081 | 0.242 | 2.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 223 | 0.35 | 0.002 | 0.003 | 0.006 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 203 | 1.29 | 0.006 | 0.007 | 0.249 | 0.9% | readdirplus(33), lookup(29), readlink(80) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 268 | 0.44 | 0.002 | 0.003 | 0.012 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 33 | 0.27 | 0.008 | 0.008 | 0.024 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 223 | 2.67 | 0.012 | 0.009 | 0.046 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 223 | 0.48 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 223 | 1.40 | 0.006 | 0.005 | 0.024 | 0.9% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.05 | 0.016 | 0.018 | 0.025 | 0.0% | — |

#### 工作负载 `rename` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `24.320 s` (`24320.0 ms`) vs FUSE 累计活跃 `1788.68 ms`（等效并发度 `0.07x`）；占比基准：`FUSE 活跃总耗时` (`1788.68 ms`)
- **缓存与流水线命中**：NSCache Attr `96.97%` / Entry `99.75%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 33,517 | 1788.68 | 0.053 | 0.004 | 0.508 | 100.0% | 错误返回 2653 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 11,907 | 404.35 | 0.034 | 0.003 | 0.486 | 22.6% | 占 FUSE 22.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rename` | 836 | 362.98 | 0.434 | 0.393 | 0.964 | 20.3% | 占 FUSE 20.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 713 | 296.56 | 0.416 | 0.389 | 0.891 | 16.6% | 占 FUSE 16.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,868 | 1467.30 | 0.379 | 0.368 | 0.989 | 82.0% | 非 OK 状态 105 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,868 | 1219.94 | 0.315 | 0.304 | 0.774 | 68.2% | Lookup:0.322ms, Rename:0.309ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,868 | 411.06 | 0.106 | 0.121 | 0.395 | 23.0% | HTTP Body+Codec 差值均值 0.042 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,868 | 247.36 | 0.064 | 0.064 | 0.243 | 13.8% | 非 OK 状态 105 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rename` | 836 | 68.89 | 0.082 | 0.079 | 0.242 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 713 | 44.26 | 0.062 | 0.069 | 0.116 | 2.5% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,682 | 4.31 | 0.002 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,214 | 8.75 | 0.007 | 0.007 | 0.042 | 0.5% | lookup(868), getxattr(98), readlink(161) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3,051 | 4.62 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.23 | 0.007 | 0.006 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,682 | 55.90 | 0.021 | 0.018 | 0.085 | 3.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,682 | 6.45 | 0.002 | 0.003 | 0.006 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,675 | 19.63 | 0.007 | 0.007 | 0.024 | 1.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.37 | 0.368 | 0.350 | 0.399 | 0.0% | — |

#### 工作负载 `suite` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `chmod`, `chown`, `ftruncate`, `link`, `open`, `rename`, `truncate`, `unlink`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `177.720 s` (`177720.0 ms`) vs FUSE 累计活跃 `3937.04 ms`（等效并发度 `0.02x`）；占比基准：`FUSE 活跃总耗时` (`3937.04 ms`)
- **缓存与流水线命中**：NSCache Attr `98.46%` / Entry `98.26%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.02 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 72,217 | 3937.04 | 0.055 | 0.003 | 0.561 | 100.0% | 错误返回 4444 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 1,479 | 612.34 | 0.414 | 0.389 | 0.967 | 15.6% | 占 FUSE 15.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 21,985 | 546.68 | 0.025 | 0.003 | 0.463 | 13.9% | 占 FUSE 13.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 1,792 | 456.74 | 0.255 | 0.218 | 0.869 | 11.6% | 占 FUSE 11.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 9,468 | 3281.45 | 0.347 | 0.326 | 2.365 | 83.3% | 非 OK 状态 126 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 9,468 | 2753.99 | 0.291 | 0.272 | 1.887 | 70.0% | SetAttr:0.326ms, Readlink:0.211ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 9,468 | 917.49 | 0.097 | 0.100 | 0.955 | 23.3% | HTTP Body+Codec 差值均值 0.041 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 9,468 | 527.46 | 0.056 | 0.053 | 0.495 | 13.4% | 非 OK 状态 126 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 1,420 | 93.13 | 0.066 | 0.068 | 0.226 | 2.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 857 | 69.53 | 0.081 | 0.078 | 0.242 | 1.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 5,629 | 9.30 | 0.002 | 0.003 | 0.007 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,116 | 25.27 | 0.006 | 0.006 | 0.247 | 0.6% | lookup(1244), readlink(1792), readdirplus(411) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,883 | 11.33 | 0.002 | 0.003 | 0.010 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 411 | 2.99 | 0.007 | 0.007 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 5,629 | 95.31 | 0.017 | 0.014 | 0.075 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 5,629 | 13.65 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 5,615 | 38.98 | 0.007 | 0.007 | 0.024 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 11 | 0.17 | 0.016 | 0.018 | 0.047 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 2 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 0.94 | 0.314 | 0.275 | 0.397 | 0.0% | — |

#### 工作负载 `truncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.239 s` (`3239.0 ms`) vs FUSE 累计活跃 `89.00 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`89.00 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.06%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 1,986 | 89.00 | 0.045 | 0.002 | 0.501 | 100.0% | 错误返回 71 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 17.82 | 0.223 | 0.204 | 0.512 | 20.0% | 占 FUSE 20.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 12.41 | 0.327 | 0.310 | 1.024 | 13.9% | 占 FUSE 13.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 10.70 | 0.282 | 0.250 | 1.024 | 12.0% | 占 FUSE 12.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 276 | 76.07 | 0.276 | 0.246 | 0.973 | 85.5% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 276 | 64.62 | 0.234 | 0.208 | 0.814 | 72.6% | Readlink:0.185ms, Mkdir:0.249ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 276 | 20.88 | 0.076 | 0.075 | 0.467 | 23.5% | HTTP Body+Codec 差值均值 0.035 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 276 | 11.45 | 0.041 | 0.039 | 0.249 | 12.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 1.99 | 0.052 | 0.048 | 0.231 | 2.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.98 | 0.052 | 0.050 | 0.221 | 2.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 118 | 0.19 | 0.002 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 0.96 | 0.005 | 0.005 | 0.046 | 1.1% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.41 | 0.002 | 0.003 | 0.010 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.26 | 0.008 | 0.008 | 0.025 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 118 | 1.51 | 0.013 | 0.012 | 0.049 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 118 | 0.24 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 118 | 0.72 | 0.006 | 0.006 | 0.024 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.04 | 0.021 | 0.025 | 0.049 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

#### 工作负载 `unlink` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `19.033 s` (`19033.0 ms`) vs FUSE 累计活跃 `218.70 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`218.70 ms`)
- **缓存与流水线命中**：NSCache Attr `99.75%` / Entry `97.20%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,780 | 218.70 | 0.058 | 0.003 | 0.739 | 100.0% | 错误返回 217 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 81 | 43.88 | 0.542 | 0.447 | 2.048 | 20.1% | 占 FUSE 20.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 92 | 38.79 | 0.422 | 0.390 | 1.024 | 17.7% | 占 FUSE 17.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mknod` | 44 | 21.56 | 0.490 | 0.427 | 2.048 | 9.9% | 占 FUSE 9.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 524 | 183.28 | 0.350 | 0.320 | 1.840 | 83.8% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 524 | 153.55 | 0.293 | 0.266 | 1.623 | 70.2% | Unlink:0.392ms, SetAttr:0.324ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 524 | 51.80 | 0.099 | 0.102 | 0.492 | 23.7% | HTTP Body+Codec 差值均值 0.042 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 524 | 29.73 | 0.057 | 0.053 | 0.245 | 13.6% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 81 | 8.04 | 0.099 | 0.085 | 0.245 | 3.7% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 92 | 5.35 | 0.058 | 0.061 | 0.112 | 2.4% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 351 | 0.57 | 0.002 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 191 | 1.13 | 0.006 | 0.006 | 0.050 | 0.5% | readdirplus(31), lookup(35), readlink(80) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 285 | 0.43 | 0.002 | 0.003 | 0.009 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.25 | 0.008 | 0.008 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 351 | 5.37 | 0.015 | 0.013 | 0.087 | 2.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 351 | 0.79 | 0.002 | 0.003 | 0.009 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 351 | 2.27 | 0.006 | 0.006 | 0.024 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1 | 0.01 | 0.015 | 0.018 | 0.025 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `98.46%` / Entry `98.26%` / Readlink `0.00%`（负缓存命中 `3,801`）；WriteBehind 聚批 `0.00 MiB` / 直通 `0.02 MiB`（Flush RPC `0`，背压等待 `0.00 ms`，屏障等待 `0.00 ms`）；ReadAhead 预读 `0.00 MiB`（命中 `0.00 MiB`，等待 `0.00 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `SetAttr` | 1,479 | 612.34 | 15.6% | 414.0 | 389.0 | 687.4 | 967.1 |
| `Lookup` | 21,988 | 547.43 | 13.9% | 24.9 | 3.1 | 191.3 | 463.0 |
| `Readlink` | 1,792 | 456.74 | 11.6% | 254.9 | 217.8 | 492.6 | 869.4 |
| `Unlink` | 857 | 404.07 | 10.3% | 471.5 | 410.2 | 888.7 | 1013.0 |
| `Rename` | 836 | 362.98 | 9.2% | 434.2 | 392.7 | 632.5 | 963.8 |
| `Mknod` | 682 | 319.45 | 8.1% | 468.4 | 408.6 | 875.7 | 1009.6 |
| `Mkdir` | 644 | 238.97 | 6.1% | 371.1 | 365.2 | 823.1 | 991.6 |
| `GetAttr` | 35,309 | 216.63 | 5.5% | 6.1 | 3.2 | 17.5 | 31.3 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:SetAttr` | 1,420 | 158.36 | 0.112 | 0.105 | 0.243 | 0.413 |
| `http_outer:Unlink` | 857 | 111.71 | 0.130 | 0.148 | 0.291 | 0.462 |
| `http_outer:Rename` | 836 | 103.16 | 0.123 | 0.155 | 0.242 | 0.250 |
| `http_outer:Readlink` | 1,792 | 96.24 | 0.054 | 0.045 | 0.133 | 0.235 |
| `rpc:SetAttr` | 1,420 (1420/0) | 93.13 | 0.066 | 0.068 | 0.116 | 0.226 |
| `rpc:Unlink` | 857 (857/0) | 69.53 | 0.081 | 0.078 | 0.205 | 0.242 |
| `rpc:Rename` | 836 (829/7) | 68.89 | 0.082 | 0.079 | 0.206 | 0.242 |
| `rpc:Mknod` | 682 (682/0) | 53.00 | 0.078 | 0.075 | 0.198 | 0.241 |
| `rpc:Lookup` | 1,245 (1244/1) | 44.89 | 0.036 | 0.036 | 0.081 | 0.097 |
| `rpc:Readlink` | 1,792 (1792/0) | 43.34 | 0.024 | 0.018 | 0.058 | 0.142 |
| `actor_phase:phase_a_validate` | 5,631 | 95.36 | 0.017 | 0.014 | 0.046 | 0.075 |
| `actor_phase:wal_apply_nosync` | 5,616 | 38.99 | 0.007 | 0.007 | 0.019 | 0.024 |
| `actor_phase:phase_b_mutate` | 5,631 | 13.66 | 0.002 | 0.003 | 0.005 | 0.009 |
| `actor_phase:wal_apply_sync` | 1 | 0.17 | 0.165 | 0.175 | 0.242 | 0.249 |
| `read_op:lookup` | 1,245 | 9.02 | 0.007 | 0.007 | 0.022 | 0.025 |
| `read_op:readlink` | 1,792 | 7.09 | 0.004 | 0.003 | 0.013 | 0.023 |
| `read_op:readdirplus` | 411 | 6.04 | 0.015 | 0.014 | 0.036 | 0.050 |
| `read_op:getattr` | 279 | 1.73 | 0.006 | 0.005 | 0.018 | 0.024 |
| `kv_read:point_get` | 6,886 | 11.34 | 0.002 | 0.003 | 0.005 | 0.010 |
| `kv_read:prefix_scan` | 411 | 2.99 | 0.007 | 0.007 | 0.022 | 0.024 |
| `blob_pwrite` | 11 | 0.17 | 0.016 | 0.018 | 0.036 | 0.047 |

Pebble BlockCache 命中率 `N/A (0 reads)` · ReadAmp `0.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `0e9ba3ed20830f31` | 0.98 | `bowl-client:fuse.StatFs` | 0.93 | 0.15 | 0.78 | `bowl-client:storage.v1.StorageService/StatFS` (0.93 ms) |
| `55cf7026f255aaa8` | 0.98 | `bowl-client:fuse.Symlink` | 0.90 | 0.17 | 0.73 | `bowl-client:storage.v1.StorageService/Symlink` (0.90 ms) |
| `6f0d59f959f65c45` | 0.67 | `bowl-client:fuse.Create` | 0.63 | 0.14 | 0.49 | `bowl-client:storage.v1.StorageService/Create` (0.63 ms) |
| `366fee47a4df479a` | 0.61 | `bowl-client:fuse.Mkdir` | 0.55 | 0.12 | 0.43 | `bowl-client:storage.v1.StorageService/Mkdir` (0.55 ms) |
| `bf5764c81053578c` | 0.59 | `bowl-client:fuse.Rmdir` | 0.55 | 0.12 | 0.42 | `bowl-client:storage.v1.StorageService/Rmdir` (0.55 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `chown_cpu.pb.gz` | `10ms  5.88%  5.88%       10ms  5.88%  bytes.NewBuffer` | `0     0%     0%      100ms 58.82%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_chown_cpu.txt`](results/telemetry/pprof_top/server_chown_cpu.txt) |
| `server` | `chown_heap.pb.gz` | `4097.37kB 22.80% 22.80%  4097.37kB 22.80%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%     8.74MB 49.81%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_chown_heap.txt`](results/telemetry/pprof_top/server_chown_heap.txt) |
| `server` | `chown_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_chown_mutex.txt`](results/telemetry/pprof_top/server_chown_mutex.txt) |
| `server` | `rename_cpu.pb.gz` | `20ms  9.09%  9.09%       20ms  9.09%  runtime.futex` | `0     0%     0%       80ms 36.36%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_rename_cpu.txt`](results/telemetry/pprof_top/server_rename_cpu.txt) |
| `server` | `rename_heap.pb.gz` | `4097.37kB 17.34% 17.34%  4097.37kB 17.34%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%     9.91MB 42.96%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_rename_heap.txt`](results/telemetry/pprof_top/server_rename_heap.txt) |
| `server` | `rename_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_rename_mutex.txt`](results/telemetry/pprof_top/server_rename_mutex.txt) |
| `client` | `chown_cpu.pb.gz` | `30ms 10.34% 10.34%       30ms 10.34%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      160ms 55.17%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_chown_cpu.txt`](results/telemetry/pprof_top/client_chown_cpu.txt) |
| `client` | `chown_heap.pb.gz` | `7143.82kB 47.54% 47.54%  7143.82kB 47.54%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%     6.98MB 47.54%  github.com/hanwen/go-fuse/v2/fuse.(*fuseFD).readRequest` | [`pprof_top/client_chown_heap.txt`](results/telemetry/pprof_top/client_chown_heap.txt) |
| `client` | `chown_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_chown_mutex.txt`](results/telemetry/pprof_top/client_chown_mutex.txt) |
| `client` | `rename_cpu.pb.gz` | `50ms 16.67% 16.67%       50ms 16.67%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      170ms 56.67%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_rename_cpu.txt`](results/telemetry/pprof_top/client_rename_cpu.txt) |
| `client` | `rename_heap.pb.gz` | `5953.18kB 36.39% 36.39%  5953.18kB 36.39%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%    10.82MB 67.69%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_rename_heap.txt`](results/telemetry/pprof_top/client_rename_heap.txt) |
| `client` | `rename_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_rename_mutex.txt`](results/telemetry/pprof_top/client_rename_mutex.txt) |

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-pjdfstest-kp-20261111-190843`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj990ykeh6eh5p2guwye`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj990ykeh6eh5p2guwyf`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-11T19:08:53Z` |
| **实例销毁时间 (UTC)** | `2026-11-11T19:17:09Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
