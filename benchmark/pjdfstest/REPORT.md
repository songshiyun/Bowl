# Bowl 分布式文件系统 — 阿里云硅谷双机内网 `pjdfstest` 基准测试报告

被测版本采用 ADR-0030 / ADR-0031 / ADR-0032 单盘独立 Actor 存储引擎与 `64 MiB` Append-Only Pack 数据层：PostgreSQL 18（deploy/prod 容器栈）仅保存单张 `disks` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在 `packs/<pack_id_hex16>.pack` 的 `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与 `64 MiB` 分片热块缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `128 MiB` H2C 连接窗口、`WriteRequest`/`ReadResponse` 池化零分配编解码、活跃包末尾同块 `ExtendTail` 顺延扩展、`8 MiB` `SyncFileRange` 渐进异步回写、相邻物理连续块合并单次 `pread` 无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及后台异步全死封口包回收。

客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备客户端 `1+8` H2C 多连接条带池（`32 MiB` 元数据连接 + `8×64 MiB` 数据连接，单流 `8 MiB`）与 `MarshalAppend` 零分配编码、`WriteBehindCoordinator`（大块流式写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发流水线，小块连续写经首笔探针或 `Create` 预热后内存聚合）、`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4`、`ReadAheadMaxGlobalInflight=32`）与 `completeDirs` 本地目录流合成，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。测试挂载保持 `strictatime` 与 0 秒属性/目录项超时，数据目录全程挂载于本地 NVMe SSD（`local_ssd_pro` 894 GiB）。

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
| **全量执行总耗时** | `170.78 s`（约 `2.85 min`） |
| **内网综合执行吞吐率** | `51.52 tests/s`（平均单断言耗时 `19.41 ms`） |
| **云端地域与网络拓扑** | 阿里云 `us-west-1` (`us-west-1a`) 同可用区 VPC 私网直连 |
| **测试执行时间 (UTC)** | `2026-11-12T05:12:44Z` ~ `2026-11-12T05:15:44Z` |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj947g04pwz6x8nu9fbk` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme0n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.200:9000` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj947g04pwz6x8nu9fbl` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.201` |

### 2.2 同可用区 VPC 内网与本地 NVMe SSD 性能基线

| 指标 | 实测数值 | 采集方式 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.040/0.078/0.252/0.054` ms | `ping -c 20 -i 0.05 172.16.1.200` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.200 -t 5` |
| **Server 本地 NVMe SSD 4 MiB 顺序写 / 读基线** | `1998.05` / `3494.88` MiB/s | `fio --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69943` / `698992` IOPS | `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `51216` IOPS (P99 `62.3` µs) | `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与挂载运行参数

| 组件 / 参数项 | 配置值 | 说明 |
| :--- | :--- | :--- |
| **Bowl Git Commit** | `6399122` | 仓库当前评测版本 |
| **部署编排与可观测性栈** | `deploy/prod (docker-compose.yaml + storage.env + strictatime/0s TTL override)` | `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` |
| **pjdfstest Git Commit** | `85a8aea` | 上游 `https://github.com/pjd/pjdfstest` |
| **PostgreSQL 版本** | `postgres (PostgreSQL) 18.6` | Server 节点控制面数据库（单张 `disks` 表） |
| **FUSE 用户态版本** | `fusermount3 version: 3.14.0` | Linux 内核 FUSE 协议 + `github.com/hanwen/go-fuse/v2` |
| **测试虚拟磁盘 (`DiskID` / `RootIno`)** | `01a1ce2c-2e79-7b77-8103-08dd7c6b9f74` (`root_ino=1`) | 配额 `2PiB` |
| **服务端 `STORAGE_ATIME_MODE`** | `strictatime` | 开启严格访问时间更新，验证 `atime` 刷新语义 |
| **FUSE 缓存超时配置** | `entry=0s, attr=0s, direntry=0s` | 内核属性与目录项缓存超时；客户端 NamespaceCache 仍可命中 |
| **内核回写缓存与数据分片** | `writeback_cache=False`, `keep_cache=True`, `max_background=64`, `block_grid=262144` 字节, `pack=67108864` 字节 | `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线 + 小块连续写内存聚合）+ `1+8` H2C 条带池 (`32M/8×64M`) + 服务端 `128M` 窗口与 64 MiB `.pack` 封包 |
| **服务端 WAL 与落盘策略** | `pebble.NoSync Group Commit + Fsync/FsyncDir 双层并行落盘屏障 (fdatasync + LogData)` | 常规写批次 Group Commit 异步提交，显式 Fsync/Sync 执行双层并行落盘屏障 |
| **客户端挂载命令** | `bowl mount <disk_id> /mnt/bowl --endpoint http://172.16.1.200:9000 --allow-other` | 启用 `default_permissions` 与 `--allow-other` 支持多 UID/GID 权限切换测试 |

---

## 3. 17 个系统调用目录兼容性与耗时汇总

| 系统调用目录 | 脚本数 (Files) | 断言数 (Tests) | PASS / ALL | 退出码 | 通过率 | 耗时 (s) | 吞吐率 (Tests/s) | 耗时占比 | 核心覆盖语义 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `chflags/` | 14 | 14 | 14/14 | 0 | 100.00% | 0.13s | 106.9 | 0.1% | 文件扩展标志位（FS_IOC_GETFLAGS / FS_IOC_SETFLAGS，验证 immutable / append 约束） |
| `chmod/` | 13 | 327 | 327/327 | 0 | 100.00% | 13.46s | 24.3 | 7.9% | 权限位变更、SUID/SGID 粘滞位保留与剥离、多身份权限检查 |
| `chown/` | 11 | 1,497 | 1497/1497 | 0 | 100.00% | 79.87s | 18.7 | 46.8% | 文件与符号链接属主/属组变更（chown/lchown/fchown）、非特权组约束与 SUID/SGID 清除 |
| `ftruncate/` | 15 | 89 | 89/89 | 0 | 100.00% | 2.95s | 30.2 | 1.7% | 基于已打开文件描述符的长度截断与扩展、只读句柄拒绝（EBADF/EINVAL） |
| `granular/` | 7 | 7 | 7/7 | 0 | 100.00% | 0.09s | 81.4 | 0.1% | 细粒度访问控制与基础文件操作边界校验 |
| `link/` | 18 | 359 | 359/359 | 0 | 100.00% | 11.54s | 31.1 | 6.8% | 硬链接创建、nlink 计数递增、跨目录硬链接与目录硬链接禁止（EPERM） |
| `mkdir/` | 13 | 118 | 118/118 | 0 | 100.00% | 1.99s | 59.4 | 1.2% | 目录创建、权限掩码、父目录写/执行权限校验、路径长度与存在性冲突（EEXIST） |
| `mkfifo/` | 13 | 120 | 120/120 | 0 | 100.00% | 1.99s | 60.3 | 1.2% | 命名管道（FIFO 特殊文件）创建与权限位验证 |
| `mknod/` | 12 | 186 | 186/186 | 0 | 100.00% | 4.11s | 45.3 | 2.4% | 字符设备、块设备、FIFO 与套接字特殊节点创建及权限验证 |
| `open/` | 27 | 337 | 337/337 | 0 | 100.00% | 4.51s | 74.8 | 2.6% | 文件打开标志组合（O_CREAT/O_EXCL/O_TRUNC/O_RDONLY/O_WRONLY/O_RDWR/O_NOFOLLOW）与权限矩阵 |
| `posix_fallocate/` | 1 | 1 | 1/1 | 0 | 100.00% | 0.05s | 20.8 | 0.0% | 物理空间预分配（fallocate Mode=0）、文件大小扩展与配额联动 |
| `rename/` | 25 | 4,857 | 4857/4857 | 0 | 100.00% | 23.06s | 210.6 | 13.5% | 同目录/跨目录原子重命名、覆盖非空目录拦截（ENOTEMPTY/EISDIR/ENOTDIR）及权限矩阵 |
| `rmdir/` | 16 | 145 | 145/145 | 0 | 100.00% | 2.07s | 69.9 | 1.2% | 空目录删除、非空目录拒绝（ENOTEMPTY）、当前目录/点目录拒绝与权限检查 |
| `symlink/` | 13 | 95 | 95/95 | 0 | 100.00% | 1.94s | 48.9 | 1.1% | 符号链接创建、目标路径长度边界与 readlink 解析 |
| `truncate/` | 15 | 84 | 84/84 | 0 | 100.00% | 2.94s | 28.6 | 1.7% | 基于路径的文件截断、稀疏扩展零填充、尺寸变更触发 ctime/mtime 更新 |
| `unlink/` | 15 | 440 | 440/440 | 0 | 100.00% | 18.67s | 23.6 | 10.9% | 文件与特殊节点删除、nlink 递减、Open-Unlink 孤儿句柄读写保活与延迟回收 |
| `utimensat/` | 10 | 122 | 122/122 | 0 | 100.00% | 1.41s | 86.3 | 0.8% | 纳秒级时间戳更新（UTIME_NOW / UTIME_OMIT / 显式时间戳）与权限校验 |
| **Total (合计)** | **238** | **8,798** | **8798/8798** | **全部为 0** | **100.00%** | **170.78s** | **51.5** | **100.0%** | **17 个系统调用目录** |

---

## 4. 内网性能特征与关键 POSIX 语义验证分析

### 4.1 跨机内网 RPC 延迟与吞吐分布剖析

- **内网通信底座**：Client 与 Server 位于阿里云硅谷 `us-west-1a` 同一交换机（`vsw-rj9rsvsgpihtsgxej6me5`），内网 ICMP RTT 延迟仅 `0.040/0.078/0.252/0.054` ms，内网带宽达 `21.22` Gbps。内核属性与目录项缓存超时设为 `0s`；客户端 NamespaceCache 仍会影响请求路径。
- **耗时集中度分析**：耗时排名前三的子集为 `chown/`（1,497 项断言，耗时 79.87s，占比 46.8%）、`rename/`（4,857 项断言，耗时 23.06s，占比 13.5%）、`unlink/`（440 项断言，耗时 18.67s，占比 10.9%）。

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
| `chmod/00.t` | `chmod/` | 119 | 119/119 | 12,269 ms | PASS |
| `chmod/01.t` | `chmod/` | 17 | 17/17 | 44 ms | PASS |
| `chmod/02.t` | `chmod/` | 5 | 5/5 | 43 ms | PASS |
| `chmod/03.t` | `chmod/` | 5 | 5/5 | 544 ms | PASS |
| `chmod/04.t` | `chmod/` | 7 | 7/7 | 26 ms | PASS |
| `chmod/05.t` | `chmod/` | 14 | 14/14 | 41 ms | PASS |
| `chmod/06.t` | `chmod/` | 8 | 8/8 | 61 ms | PASS |
| `chmod/07.t` | `chmod/` | 25 | 25/25 | 64 ms | PASS |
| `chmod/08.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/09.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/10.t` | `chmod/` | 2 | 2/2 | 7 ms | PASS |
| `chmod/11.t` | `chmod/` | 109 | 109/109 | 235 ms | PASS |
| `chmod/12.t` | `chmod/` | 14 | 14/14 | 44 ms | PASS |
| `chown/00.t` | `chown/` | 1280 | 1280/1280 | 78,674 ms | PASS |
| `chown/01.t` | `chown/` | 22 | 22/22 | 53 ms | PASS |
| `chown/02.t` | `chown/` | 10 | 10/10 | 53 ms | PASS |
| `chown/03.t` | `chown/` | 10 | 10/10 | 564 ms | PASS |
| `chown/04.t` | `chown/` | 9 | 9/9 | 31 ms | PASS |
| `chown/05.t` | `chown/` | 18 | 18/18 | 48 ms | PASS |
| `chown/06.t` | `chown/` | 10 | 10/10 | 81 ms | PASS |
| `chown/07.t` | `chown/` | 132 | 132/132 | 277 ms | PASS |
| `chown/08.t` | `chown/` | 1 | 1/1 | 4 ms | PASS |
| `chown/09.t` | `chown/` | 1 | 1/1 | 4 ms | PASS |
| `chown/10.t` | `chown/` | 4 | 4/4 | 10 ms | PASS |
| `ftruncate/00.t` | `ftruncate/` | 26 | 26/26 | 2,070 ms | PASS |
| `ftruncate/01.t` | `ftruncate/` | 5 | 5/5 | 22 ms | PASS |
| `ftruncate/02.t` | `ftruncate/` | 5 | 5/5 | 43 ms | PASS |
| `ftruncate/03.t` | `ftruncate/` | 5 | 5/5 | 554 ms | PASS |
| `ftruncate/04.t` | `ftruncate/` | 4 | 4/4 | 17 ms | PASS |
| `ftruncate/05.t` | `ftruncate/` | 15 | 15/15 | 42 ms | PASS |
| `ftruncate/06.t` | `ftruncate/` | 8 | 8/8 | 26 ms | PASS |
| `ftruncate/07.t` | `ftruncate/` | 6 | 6/6 | 40 ms | PASS |
| `ftruncate/08.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/09.t` | `ftruncate/` | 3 | 3/3 | 12 ms | PASS |
| `ftruncate/10.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/11.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/12.t` | `ftruncate/` | 3 | 3/3 | 14 ms | PASS |
| `ftruncate/13.t` | `ftruncate/` | 4 | 4/4 | 15 ms | PASS |
| `ftruncate/14.t` | `ftruncate/` | 2 | 2/2 | 7 ms | PASS |
| `granular/00.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/01.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/02.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/03.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/04.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/05.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/06.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `link/00.t` | `link/` | 202 | 202/202 | 10,395 ms | PASS |
| `link/01.t` | `link/` | 32 | 32/32 | 77 ms | PASS |
| `link/02.t` | `link/` | 10 | 10/10 | 59 ms | PASS |
| `link/03.t` | `link/` | 13 | 13/13 | 574 ms | PASS |
| `link/04.t` | `link/` | 6 | 6/6 | 23 ms | PASS |
| `link/05.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/06.t` | `link/` | 18 | 18/18 | 55 ms | PASS |
| `link/07.t` | `link/` | 17 | 17/17 | 54 ms | PASS |
| `link/08.t` | `link/` | 10 | 10/10 | 69 ms | PASS |
| `link/09.t` | `link/` | 5 | 5/5 | 20 ms | PASS |
| `link/10.t` | `link/` | 23 | 23/23 | 56 ms | PASS |
| `link/11.t` | `link/` | 9 | 9/9 | 31 ms | PASS |
| `link/12.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/13.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/14.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/15.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/16.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/17.t` | `link/` | 8 | 8/8 | 21 ms | PASS |
| `mkdir/00.t` | `mkdir/` | 36 | 36/36 | 1,079 ms | PASS |
| `mkdir/01.t` | `mkdir/` | 17 | 17/17 | 45 ms | PASS |
| `mkdir/02.t` | `mkdir/` | 3 | 3/3 | 39 ms | PASS |
| `mkdir/03.t` | `mkdir/` | 3 | 3/3 | 551 ms | PASS |
| `mkdir/04.t` | `mkdir/` | 3 | 3/3 | 16 ms | PASS |
| `mkdir/05.t` | `mkdir/` | 12 | 12/12 | 37 ms | PASS |
| `mkdir/06.t` | `mkdir/` | 12 | 12/12 | 37 ms | PASS |
| `mkdir/07.t` | `mkdir/` | 6 | 6/6 | 41 ms | PASS |
| `mkdir/08.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/09.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/10.t` | `mkdir/` | 21 | 21/21 | 49 ms | PASS |
| `mkdir/11.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/12.t` | `mkdir/` | 2 | 2/2 | 7 ms | PASS |
| `mkfifo/00.t` | `mkfifo/` | 36 | 36/36 | 1,081 ms | PASS |
| `mkfifo/01.t` | `mkfifo/` | 17 | 17/17 | 44 ms | PASS |
| `mkfifo/02.t` | `mkfifo/` | 4 | 4/4 | 42 ms | PASS |
| `mkfifo/03.t` | `mkfifo/` | 4 | 4/4 | 552 ms | PASS |
| `mkfifo/04.t` | `mkfifo/` | 3 | 3/3 | 17 ms | PASS |
| `mkfifo/05.t` | `mkfifo/` | 12 | 12/12 | 39 ms | PASS |
| `mkfifo/06.t` | `mkfifo/` | 12 | 12/12 | 37 ms | PASS |
| `mkfifo/07.t` | `mkfifo/` | 6 | 6/6 | 40 ms | PASS |
| `mkfifo/08.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/09.t` | `mkfifo/` | 21 | 21/21 | 48 ms | PASS |
| `mkfifo/10.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/11.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/12.t` | `mkfifo/` | 2 | 2/2 | 8 ms | PASS |
| `mknod/00.t` | `mknod/` | 36 | 36/36 | 1,077 ms | PASS |
| `mknod/01.t` | `mknod/` | 27 | 27/27 | 62 ms | PASS |
| `mknod/02.t` | `mknod/` | 12 | 12/12 | 56 ms | PASS |
| `mknod/03.t` | `mknod/` | 12 | 12/12 | 572 ms | PASS |
| `mknod/04.t` | `mknod/` | 3 | 3/3 | 15 ms | PASS |
| `mknod/05.t` | `mknod/` | 12 | 12/12 | 37 ms | PASS |
| `mknod/06.t` | `mknod/` | 12 | 12/12 | 38 ms | PASS |
| `mknod/07.t` | `mknod/` | 6 | 6/6 | 40 ms | PASS |
| `mknod/08.t` | `mknod/` | 35 | 35/35 | 72 ms | PASS |
| `mknod/09.t` | `mknod/` | 1 | 1/1 | 4 ms | PASS |
| `mknod/10.t` | `mknod/` | 2 | 2/2 | 7 ms | PASS |
| `mknod/11.t` | `mknod/` | 28 | 28/28 | 2,056 ms | PASS |
| `open/00.t` | `open/` | 47 | 47/47 | 3,101 ms | PASS |
| `open/01.t` | `open/` | 22 | 22/22 | 53 ms | PASS |
| `open/02.t` | `open/` | 4 | 4/4 | 41 ms | PASS |
| `open/03.t` | `open/` | 4 | 4/4 | 554 ms | PASS |
| `open/04.t` | `open/` | 4 | 4/4 | 17 ms | PASS |
| `open/05.t` | `open/` | 12 | 12/12 | 38 ms | PASS |
| `open/06.t` | `open/` | 144 | 144/144 | 277 ms | PASS |
| `open/07.t` | `open/` | 25 | 25/25 | 59 ms | PASS |
| `open/08.t` | `open/` | 3 | 3/3 | 16 ms | PASS |
| `open/09.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/10.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/11.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/12.t` | `open/` | 6 | 6/6 | 40 ms | PASS |
| `open/13.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/14.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/15.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/16.t` | `open/` | 6 | 6/6 | 20 ms | PASS |
| `open/17.t` | `open/` | 3 | 3/3 | 13 ms | PASS |
| `open/18.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/19.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/20.t` | `open/` | 1 | 1/1 | 5 ms | PASS |
| `open/21.t` | `open/` | 2 | 2/2 | 7 ms | PASS |
| `open/22.t` | `open/` | 21 | 21/21 | 48 ms | PASS |
| `open/23.t` | `open/` | 5 | 5/5 | 16 ms | PASS |
| `open/24.t` | `open/` | 5 | 5/5 | 17 ms | PASS |
| `open/25.t` | `open/` | 6 | 6/6 | 25 ms | PASS |
| `open/26.t` | `open/` | 9 | 9/9 | 24 ms | PASS |
| `posix_fallocate/00.t` | `posix_fallocate/` | 1 | 1/1 | 4 ms | PASS |
| `rename/00.t` | `rename/` | 122 | 122/122 | 7,277 ms | PASS |
| `rename/01.t` | `rename/` | 8 | 8/8 | 55 ms | PASS |
| `rename/02.t` | `rename/` | 6 | 6/6 | 560 ms | PASS |
| `rename/03.t` | `rename/` | 6 | 6/6 | 24 ms | PASS |
| `rename/04.t` | `rename/` | 18 | 18/18 | 57 ms | PASS |
| `rename/05.t` | `rename/` | 17 | 17/17 | 55 ms | PASS |
| `rename/06.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/07.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/08.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/09.t` | `rename/` | 2353 | 2353/2353 | 4,905 ms | PASS |
| `rename/10.t` | `rename/` | 2099 | 2099/2099 | 4,373 ms | PASS |
| `rename/11.t` | `rename/` | 10 | 10/10 | 64 ms | PASS |
| `rename/12.t` | `rename/` | 32 | 32/32 | 76 ms | PASS |
| `rename/13.t` | `rename/` | 32 | 32/32 | 69 ms | PASS |
| `rename/14.t` | `rename/` | 32 | 32/32 | 71 ms | PASS |
| `rename/15.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/16.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/17.t` | `rename/` | 8 | 8/8 | 20 ms | PASS |
| `rename/18.t` | `rename/` | 6 | 6/6 | 25 ms | PASS |
| `rename/19.t` | `rename/` | 6 | 6/6 | 24 ms | PASS |
| `rename/20.t` | `rename/` | 25 | 25/25 | 68 ms | PASS |
| `rename/21.t` | `rename/` | 16 | 16/16 | 48 ms | PASS |
| `rename/22.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/23.t` | `rename/` | 42 | 42/42 | 5,109 ms | PASS |
| `rename/24.t` | `rename/` | 13 | 13/13 | 43 ms | PASS |
| `rmdir/00.t` | `rmdir/` | 10 | 10/10 | 1,030 ms | PASS |
| `rmdir/01.t` | `rmdir/` | 14 | 14/14 | 38 ms | PASS |
| `rmdir/02.t` | `rmdir/` | 4 | 4/4 | 41 ms | PASS |
| `rmdir/03.t` | `rmdir/` | 5 | 5/5 | 556 ms | PASS |
| `rmdir/04.t` | `rmdir/` | 4 | 4/4 | 18 ms | PASS |
| `rmdir/05.t` | `rmdir/` | 6 | 6/6 | 36 ms | PASS |
| `rmdir/06.t` | `rmdir/` | 23 | 23/23 | 60 ms | PASS |
| `rmdir/07.t` | `rmdir/` | 10 | 10/10 | 33 ms | PASS |
| `rmdir/08.t` | `rmdir/` | 10 | 10/10 | 33 ms | PASS |
| `rmdir/09.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/10.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/11.t` | `rmdir/` | 47 | 47/47 | 106 ms | PASS |
| `rmdir/12.t` | `rmdir/` | 6 | 6/6 | 21 ms | PASS |
| `rmdir/13.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/14.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/15.t` | `rmdir/` | 2 | 2/2 | 7 ms | PASS |
| `symlink/00.t` | `symlink/` | 14 | 14/14 | 1,038 ms | PASS |
| `symlink/01.t` | `symlink/` | 5 | 5/5 | 21 ms | PASS |
| `symlink/02.t` | `symlink/` | 7 | 7/7 | 51 ms | PASS |
| `symlink/03.t` | `symlink/` | 6 | 6/6 | 557 ms | PASS |
| `symlink/04.t` | `symlink/` | 3 | 3/3 | 15 ms | PASS |
| `symlink/05.t` | `symlink/` | 12 | 12/12 | 38 ms | PASS |
| `symlink/06.t` | `symlink/` | 12 | 12/12 | 37 ms | PASS |
| `symlink/07.t` | `symlink/` | 6 | 6/6 | 38 ms | PASS |
| `symlink/08.t` | `symlink/` | 21 | 21/21 | 47 ms | PASS |
| `symlink/09.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/10.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/11.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/12.t` | `symlink/` | 6 | 6/6 | 17 ms | PASS |
| `truncate/00.t` | `truncate/` | 21 | 21/21 | 2,057 ms | PASS |
| `truncate/01.t` | `truncate/` | 5 | 5/5 | 21 ms | PASS |
| `truncate/02.t` | `truncate/` | 5 | 5/5 | 44 ms | PASS |
| `truncate/03.t` | `truncate/` | 5 | 5/5 | 560 ms | PASS |
| `truncate/04.t` | `truncate/` | 4 | 4/4 | 17 ms | PASS |
| `truncate/05.t` | `truncate/` | 15 | 15/15 | 43 ms | PASS |
| `truncate/06.t` | `truncate/` | 8 | 8/8 | 26 ms | PASS |
| `truncate/07.t` | `truncate/` | 6 | 6/6 | 37 ms | PASS |
| `truncate/08.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/09.t` | `truncate/` | 3 | 3/3 | 13 ms | PASS |
| `truncate/10.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/11.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/12.t` | `truncate/` | 3 | 3/3 | 14 ms | PASS |
| `truncate/13.t` | `truncate/` | 4 | 4/4 | 15 ms | PASS |
| `truncate/14.t` | `truncate/` | 2 | 2/2 | 7 ms | PASS |
| `unlink/00.t` | `unlink/` | 112 | 112/112 | 17,252 ms | PASS |
| `unlink/01.t` | `unlink/` | 5 | 5/5 | 21 ms | PASS |
| `unlink/02.t` | `unlink/` | 4 | 4/4 | 41 ms | PASS |
| `unlink/03.t` | `unlink/` | 4 | 4/4 | 553 ms | PASS |
| `unlink/04.t` | `unlink/` | 4 | 4/4 | 17 ms | PASS |
| `unlink/05.t` | `unlink/` | 10 | 10/10 | 34 ms | PASS |
| `unlink/06.t` | `unlink/` | 10 | 10/10 | 33 ms | PASS |
| `unlink/07.t` | `unlink/` | 6 | 6/6 | 39 ms | PASS |
| `unlink/08.t` | `unlink/` | 3 | 3/3 | 14 ms | PASS |
| `unlink/09.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/10.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/11.t` | `unlink/` | 270 | 270/270 | 547 ms | PASS |
| `unlink/12.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/13.t` | `unlink/` | 2 | 2/2 | 7 ms | PASS |
| `unlink/14.t` | `unlink/` | 7 | 7/7 | 27 ms | PASS |
| `utimensat/00.t` | `utimensat/` | 32 | 32/32 | 76 ms | PASS |
| `utimensat/01.t` | `utimensat/` | 7 | 7/7 | 1,024 ms | PASS |
| `utimensat/02.t` | `utimensat/` | 10 | 10/10 | 31 ms | PASS |
| `utimensat/03.t` | `utimensat/` | 1 | 1/1 | 4 ms | PASS |
| `utimensat/04.t` | `utimensat/` | 10 | 10/10 | 29 ms | PASS |
| `utimensat/05.t` | `utimensat/` | 16 | 16/16 | 45 ms | PASS |
| `utimensat/06.t` | `utimensat/` | 13 | 13/13 | 40 ms | PASS |
| `utimensat/07.t` | `utimensat/` | 17 | 17/17 | 48 ms | PASS |
| `utimensat/08.t` | `utimensat/` | 9 | 9/9 | 26 ms | PASS |
| `utimensat/09.t` | `utimensat/` | 7 | 7/7 | 27 ms | PASS |
| **Total** | **17 dirs** | **8,798** | **8798/8798** | **170,776 ms** | **100% PASS (全量通过)** |

---

## 6. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `319` 条（跨 Client+Server 全链路 `108` 条，异常链路 `8` 条）· Loki 业务日志 `20` 条（服务端 Warn/Error `14` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `chmod` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `13.679 s` (`13679.0 ms`) vs FUSE 累计活跃 `183.50 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`183.50 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `96.76%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,320 | 183.50 | 0.055 | 0.002 | 0.509 | 100.0% | 错误返回 125 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 189 | 41.91 | 0.222 | 0.207 | 0.505 | 22.8% | 占 FUSE 22.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 72 | 27.88 | 0.387 | 0.394 | 1.024 | 15.2% | 占 FUSE 15.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 61 | 26.35 | 0.432 | 0.412 | 1.024 | 14.4% | 占 FUSE 14.4% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 549 | 158.29 | 0.288 | 0.277 | 4.925 | 86.3% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 549 | 135.28 | 0.246 | 0.237 | 4.433 | 73.7% | Readlink:0.184ms, SetAttr:0.305ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 549 | 43.24 | 0.079 | 0.072 | 0.985 | 23.6% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 549 | 23.01 | 0.042 | 0.040 | 0.492 | 12.5% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 69 | 4.34 | 0.063 | 0.064 | 0.224 | 2.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 61 | 4.30 | 0.071 | 0.068 | 0.237 | 2.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 284 | 0.48 | 0.002 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 281 | 1.18 | 0.004 | 0.004 | 0.025 | 0.6% | readlink(189), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 357 | 0.55 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.14 | 0.004 | 0.003 | 0.020 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 284 | 2.78 | 0.010 | 0.008 | 0.043 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 284 | 0.61 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 284 | 1.43 | 0.005 | 0.004 | 0.021 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.03 | 0.009 | 0.009 | 0.025 | 0.0% | — |

#### 工作负载 `chown` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `80.526 s` (`80526.0 ms`) vs FUSE 累计活跃 `556.62 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`556.62 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `98.94%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 9,165 | 556.62 | 0.061 | 0.004 | 0.697 | 100.0% | 错误返回 599 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 559 | 173.92 | 0.311 | 0.300 | 0.964 | 31.2% | 占 FUSE 31.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 398 | 142.12 | 0.357 | 0.373 | 0.980 | 25.5% | 占 FUSE 25.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 164 | 68.87 | 0.420 | 0.406 | 1.003 | 12.4% | 占 FUSE 12.4% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,422 | 474.06 | 0.333 | 0.334 | 0.991 | 85.2% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,422 | 401.87 | 0.283 | 0.287 | 0.834 | 72.2% | Readlink:0.254ms, SetAttr:0.314ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,422 | 130.78 | 0.092 | 0.085 | 0.620 | 23.5% | HTTP Body+Codec 差值均值 0.041 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,422 | 72.19 | 0.051 | 0.047 | 0.620 | 13.0% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 342 | 22.56 | 0.066 | 0.064 | 0.234 | 4.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 559 | 16.87 | 0.030 | 0.024 | 0.110 | 3.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 782 | 1.33 | 0.002 | 0.003 | 0.007 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 654 | 3.58 | 0.005 | 0.004 | 0.048 | 0.6% | readlink(559), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 736 | 1.71 | 0.002 | 0.003 | 0.015 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.17 | 0.005 | 0.005 | 0.023 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 782 | 9.59 | 0.012 | 0.010 | 0.048 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 782 | 1.96 | 0.003 | 0.003 | 0.010 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 782 | 4.89 | 0.006 | 0.005 | 0.022 | 0.9% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.22 | 0.223 | 0.250 | 0.299 | 0.0% | — |

#### 工作负载 `ftruncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.176 s` (`3176.0 ms`) vs FUSE 累计活跃 `84.88 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`84.88 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.13%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 2,045 | 84.88 | 0.042 | 0.002 | 0.484 | 100.0% | 错误返回 74 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 16.40 | 0.205 | 0.196 | 0.512 | 19.3% | 占 FUSE 19.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 11.28 | 0.297 | 0.244 | 1.024 | 13.3% | 占 FUSE 13.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 9.86 | 0.259 | 0.222 | 1.024 | 11.6% | 占 FUSE 11.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 283 | 73.39 | 0.259 | 0.245 | 2.485 | 86.5% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 283 | 62.56 | 0.221 | 0.209 | 2.236 | 73.7% | Readlink:0.171ms, Mkdir:0.226ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 283 | 20.06 | 0.071 | 0.067 | 0.497 | 23.6% | HTTP Body+Codec 差值均值 0.033 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 283 | 10.83 | 0.038 | 0.037 | 0.249 | 12.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 1.80 | 0.047 | 0.040 | 0.231 | 2.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.77 | 0.047 | 0.044 | 0.099 | 2.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 125 | 0.18 | 0.001 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 0.88 | 0.005 | 0.005 | 0.042 | 1.0% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.38 | 0.001 | 0.003 | 0.010 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.19 | 0.006 | 0.005 | 0.024 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 125 | 1.27 | 0.010 | 0.009 | 0.042 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 125 | 0.24 | 0.002 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 125 | 0.61 | 0.005 | 0.004 | 0.016 | 0.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.03 | 0.016 | 0.018 | 0.025 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

#### 工作负载 `link` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `11.764 s` (`11764.0 ms`) vs FUSE 累计活跃 `172.14 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`172.14 ms`)
- **缓存与流水线命中**：NSCache Attr `99.19%` / Entry `97.01%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,594 | 172.14 | 0.048 | 0.002 | 0.540 | 100.0% | 错误返回 167 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 160 | 31.93 | 0.200 | 0.197 | 0.489 | 18.5% | 占 FUSE 18.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 65 | 29.53 | 0.454 | 0.425 | 2.048 | 17.2% | 占 FUSE 17.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 987 | 18.71 | 0.019 | 0.002 | 0.431 | 10.9% | 占 FUSE 10.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 507 | 147.77 | 0.291 | 0.272 | 1.525 | 85.8% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 507 | 125.50 | 0.248 | 0.231 | 1.283 | 72.9% | Readlink:0.167ms, Unlink:0.335ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 507 | 41.03 | 0.081 | 0.076 | 0.489 | 23.8% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 507 | 22.26 | 0.044 | 0.041 | 0.243 | 12.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 65 | 5.01 | 0.077 | 0.074 | 0.242 | 2.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 160 | 2.87 | 0.018 | 0.014 | 0.049 | 1.7% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 244 | 0.44 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 284 | 1.34 | 0.005 | 0.005 | 0.042 | 0.8% | readlink(160), lookup(51), readdirplus(31) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 396 | 0.57 | 0.001 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.18 | 0.006 | 0.005 | 0.023 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 244 | 3.07 | 0.013 | 0.011 | 0.048 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 244 | 0.51 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 244 | 1.44 | 0.006 | 0.006 | 0.023 | 0.8% | — |

#### 工作负载 `open` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `4.742 s` (`4742.0 ms`) vs FUSE 累计活跃 `141.83 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`141.83 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `95.97%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,023 | 141.83 | 0.047 | 0.002 | 0.501 | 100.0% | 错误返回 94 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 60 | 24.55 | 0.409 | 0.391 | 1.024 | 17.3% | 占 FUSE 17.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 38 | 16.23 | 0.427 | 0.395 | 1.024 | 11.4% | 占 FUSE 11.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 15.94 | 0.199 | 0.198 | 0.512 | 11.2% | 占 FUSE 11.2% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 396 | 121.14 | 0.306 | 0.295 | 4.975 | 85.4% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 396 | 100.31 | 0.253 | 0.244 | 2.490 | 70.7% | SetAttr:0.313ms, Readlink:0.167ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 396 | 35.88 | 0.091 | 0.089 | 2.485 | 25.3% | HTTP Body+Codec 差值均值 0.038 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 396 | 20.83 | 0.053 | 0.051 | 2.485 | 14.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 60 | 3.42 | 0.057 | 0.055 | 0.160 | 2.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 38 | 2.93 | 0.077 | 0.076 | 0.239 | 2.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 223 | 0.35 | 0.002 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 203 | 1.18 | 0.006 | 0.006 | 0.100 | 0.8% | readdirplus(33), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 268 | 0.49 | 0.002 | 0.003 | 0.020 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 33 | 0.27 | 0.008 | 0.008 | 0.024 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 223 | 2.69 | 0.012 | 0.010 | 0.045 | 1.9% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 223 | 0.47 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 223 | 1.19 | 0.005 | 0.005 | 0.021 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.04 | 0.015 | 0.018 | 0.025 | 0.0% | — |

#### 工作负载 `rename` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `23.520 s` (`23520.0 ms`) vs FUSE 累计活跃 `1583.43 ms`（等效并发度 `0.07x`）；占比基准：`FUSE 活跃总耗时` (`1583.43 ms`)
- **缓存与流水线命中**：NSCache Attr `96.97%` / Entry `99.75%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 33,517 | 1583.43 | 0.047 | 0.003 | 0.496 | 100.0% | 错误返回 2653 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 11,907 | 355.49 | 0.030 | 0.003 | 0.480 | 22.5% | 占 FUSE 22.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rename` | 836 | 325.96 | 0.390 | 0.388 | 0.913 | 20.6% | 占 FUSE 20.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 713 | 268.85 | 0.377 | 0.386 | 0.725 | 17.0% | 占 FUSE 17.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,868 | 1319.55 | 0.341 | 0.364 | 1.090 | 83.3% | 非 OK 状态 105 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,868 | 1085.79 | 0.281 | 0.302 | 0.856 | 68.6% | Lookup:0.285ms, Rename:0.272ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,868 | 382.42 | 0.099 | 0.108 | 0.377 | 24.2% | HTTP Body+Codec 差值均值 0.039 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,868 | 233.76 | 0.060 | 0.061 | 0.243 | 14.8% | 非 OK 状态 105 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rename` | 836 | 65.78 | 0.079 | 0.077 | 0.241 | 4.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 713 | 41.98 | 0.059 | 0.064 | 0.100 | 2.7% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,682 | 4.02 | 0.001 | 0.003 | 0.006 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,214 | 8.74 | 0.007 | 0.007 | 0.046 | 0.6% | lookup(868), getxattr(98), readlink(161) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3,051 | 4.74 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.22 | 0.007 | 0.007 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,682 | 54.92 | 0.020 | 0.018 | 0.077 | 3.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,682 | 5.81 | 0.002 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,675 | 16.62 | 0.006 | 0.006 | 0.019 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.40 | 0.402 | 0.450 | 0.499 | 0.0% | — |

#### 工作负载 `suite` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `chmod`, `chown`, `ftruncate`, `link`, `open`, `rename`, `truncate`, `unlink`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `175.546 s` (`175546.0 ms`) vs FUSE 累计活跃 `3513.55 ms`（等效并发度 `0.02x`）；占比基准：`FUSE 活跃总耗时` (`3513.55 ms`)
- **缓存与流水线命中**：NSCache Attr `98.46%` / Entry `98.26%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.02 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 72,208 | 3513.55 | 0.049 | 0.003 | 0.501 | 100.0% | 错误返回 4435 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 1,479 | 556.70 | 0.376 | 0.384 | 0.914 | 15.8% | 占 FUSE 15.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 21,985 | 475.55 | 0.022 | 0.003 | 0.453 | 13.5% | 占 FUSE 13.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 1,792 | 412.16 | 0.230 | 0.212 | 0.783 | 11.7% | 占 FUSE 11.7% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 9,468 | 2977.02 | 0.314 | 0.315 | 4.950 | 84.7% | 非 OK 状态 126 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 9,468 | 2480.73 | 0.262 | 0.265 | 4.297 | 70.6% | SetAttr:0.296ms, Readlink:0.191ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 9,468 | 853.54 | 0.090 | 0.091 | 2.470 | 24.3% | HTTP Body+Codec 差值均值 0.038 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 9,468 | 496.29 | 0.052 | 0.051 | 2.470 | 14.1% | 非 OK 状态 126 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 1,420 | 86.64 | 0.061 | 0.063 | 0.208 | 2.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rename` | 836 | 65.78 | 0.079 | 0.077 | 0.241 | 1.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 5,629 | 8.81 | 0.002 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,116 | 24.65 | 0.006 | 0.006 | 0.099 | 0.7% | lookup(1244), readlink(1792), readdirplus(411) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,883 | 11.30 | 0.002 | 0.003 | 0.010 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 411 | 3.11 | 0.008 | 0.007 | 0.033 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 5,629 | 92.03 | 0.016 | 0.014 | 0.062 | 2.6% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 5,629 | 12.16 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 5,615 | 34.25 | 0.006 | 0.006 | 0.021 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 11 | 0.16 | 0.015 | 0.017 | 0.047 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 2 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 0.92 | 0.308 | 0.275 | 0.497 | 0.0% | — |

#### 工作负载 `truncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.170 s` (`3170.0 ms`) vs FUSE 累计活跃 `80.97 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`80.97 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.06%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 1,985 | 80.97 | 0.041 | 0.002 | 0.501 | 100.0% | 错误返回 70 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 13.79 | 0.172 | 0.194 | 0.512 | 17.0% | 占 FUSE 17.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 12.91 | 0.340 | 0.274 | 1.024 | 15.9% | 占 FUSE 15.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 9.91 | 0.261 | 0.229 | 0.512 | 12.2% | 占 FUSE 12.2% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 276 | 71.14 | 0.258 | 0.241 | 0.982 | 87.9% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 276 | 59.09 | 0.214 | 0.201 | 0.753 | 73.0% | Readlink:0.146ms, Mkdir:0.251ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 276 | 21.80 | 0.079 | 0.075 | 0.810 | 26.9% | HTTP Body+Codec 差值均值 0.035 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 276 | 12.05 | 0.044 | 0.040 | 0.249 | 14.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 2.53 | 0.067 | 0.059 | 0.240 | 3.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.90 | 0.050 | 0.053 | 0.099 | 2.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 118 | 0.20 | 0.002 | 0.003 | 0.008 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 0.96 | 0.005 | 0.006 | 0.046 | 1.2% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.41 | 0.002 | 0.003 | 0.012 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.29 | 0.009 | 0.008 | 0.042 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 118 | 1.79 | 0.015 | 0.015 | 0.070 | 2.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 118 | 0.26 | 0.002 | 0.003 | 0.010 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 118 | 0.73 | 0.006 | 0.006 | 0.024 | 0.9% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.05 | 0.024 | 0.025 | 0.049 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

#### 工作负载 `unlink` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `19.117 s` (`19117.0 ms`) vs FUSE 累计活跃 `189.95 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`189.95 ms`)
- **缓存与流水线命中**：NSCache Attr `99.75%` / Entry `97.20%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,779 | 189.95 | 0.050 | 0.003 | 0.510 | 100.0% | 错误返回 216 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 81 | 39.76 | 0.491 | 0.431 | 2.048 | 20.9% | 占 FUSE 20.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 92 | 34.11 | 0.371 | 0.384 | 0.512 | 18.0% | 占 FUSE 18.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mknod` | 44 | 18.14 | 0.412 | 0.400 | 1.024 | 9.5% | 占 FUSE 9.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 524 | 161.90 | 0.309 | 0.307 | 1.285 | 85.2% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 524 | 134.08 | 0.256 | 0.258 | 1.041 | 70.6% | Unlink:0.357ms, SetAttr:0.282ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 524 | 47.29 | 0.090 | 0.091 | 0.484 | 24.9% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 524 | 27.82 | 0.053 | 0.049 | 0.244 | 14.6% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 81 | 7.47 | 0.092 | 0.082 | 0.244 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 92 | 4.91 | 0.053 | 0.049 | 0.112 | 2.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 351 | 0.57 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 191 | 1.10 | 0.006 | 0.006 | 0.050 | 0.6% | readdirplus(31), lookup(35), readlink(80) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 285 | 0.45 | 0.002 | 0.003 | 0.014 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.25 | 0.008 | 0.008 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 351 | 5.24 | 0.015 | 0.013 | 0.081 | 2.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 351 | 0.72 | 0.002 | 0.003 | 0.008 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 351 | 1.98 | 0.006 | 0.005 | 0.023 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1 | 0.01 | 0.011 | 0.018 | 0.025 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `98.46%` / Entry `98.26%` / Readlink `0.00%`（负缓存命中 `3,801`）；WriteBehind 聚批 `0.00 MiB` / 直通 `0.02 MiB`（Flush RPC `0`，背压等待 `0.00 ms`，屏障等待 `0.00 ms`）；ReadAhead 预读 `0.00 MiB`（命中 `0.00 MiB`，等待 `0.00 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `SetAttr` | 1,479 | 556.70 | 15.8% | 376.4 | 384.1 | 510.5 | 913.7 |
| `Lookup` | 21,988 | 476.44 | 13.6% | 21.7 | 2.5 | 182.8 | 453.7 |
| `Readlink` | 1,792 | 412.16 | 11.7% | 230.0 | 212.1 | 476.9 | 783.1 |
| `Unlink` | 857 | 366.90 | 10.4% | 428.1 | 400.5 | 811.6 | 996.8 |
| `Rename` | 836 | 325.96 | 9.3% | 389.9 | 388.0 | 507.2 | 912.7 |
| `Mknod` | 682 | 286.84 | 8.2% | 420.6 | 393.5 | 659.9 | 978.5 |
| `Mkdir` | 644 | 216.94 | 6.2% | 336.9 | 318.8 | 706.6 | 972.8 |
| `Rmdir` | 640 | 193.29 | 5.5% | 302.0 | 269.7 | 502.3 | 889.3 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:SetAttr` | 1,420 | 146.30 | 0.103 | 0.092 | 0.237 | 0.352 |
| `http_outer:Unlink` | 857 | 101.58 | 0.119 | 0.124 | 0.246 | 0.429 |
| `http_outer:Rename` | 836 | 96.83 | 0.116 | 0.136 | 0.240 | 0.249 |
| `http_outer:Readlink` | 1,792 | 88.36 | 0.049 | 0.043 | 0.110 | 0.223 |
| `rpc:SetAttr` | 1,420 (1420/0) | 86.64 | 0.061 | 0.063 | 0.099 | 0.208 |
| `rpc:Rename` | 836 (829/7) | 65.78 | 0.079 | 0.077 | 0.184 | 0.241 |
| `rpc:Unlink` | 857 (857/0) | 63.84 | 0.074 | 0.074 | 0.189 | 0.238 |
| `rpc:Mknod` | 682 (682/0) | 49.10 | 0.072 | 0.072 | 0.183 | 0.238 |
| `rpc:Lookup` | 1,245 (1244/1) | 42.43 | 0.034 | 0.035 | 0.077 | 0.096 |
| `rpc:Readlink` | 1,792 (1792/0) | 39.63 | 0.022 | 0.017 | 0.050 | 0.092 |
| `actor_phase:phase_a_validate` | 5,631 | 92.08 | 0.016 | 0.014 | 0.045 | 0.062 |
| `actor_phase:wal_apply_nosync` | 5,616 | 34.26 | 0.006 | 0.006 | 0.010 | 0.021 |
| `actor_phase:phase_b_mutate` | 5,631 | 12.17 | 0.002 | 0.003 | 0.005 | 0.008 |
| `actor_phase:wal_apply_sync` | 1 | 0.15 | 0.152 | 0.175 | 0.242 | 0.249 |
| `read_op:lookup` | 1,245 | 9.00 | 0.007 | 0.007 | 0.021 | 0.024 |
| `read_op:readlink` | 1,792 | 6.52 | 0.004 | 0.003 | 0.010 | 0.022 |
| `read_op:readdirplus` | 411 | 5.99 | 0.015 | 0.013 | 0.038 | 0.050 |
| `read_op:getattr` | 278 | 1.87 | 0.007 | 0.006 | 0.021 | 0.025 |
| `kv_read:point_get` | 6,885 | 11.31 | 0.002 | 0.003 | 0.005 | 0.010 |
| `kv_read:prefix_scan` | 411 | 3.11 | 0.008 | 0.007 | 0.022 | 0.033 |
| `blob_pwrite` | 11 | 0.16 | 0.015 | 0.017 | 0.036 | 0.047 |

Pebble BlockCache 命中率 `N/A (0 reads)` · ReadAmp `0.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `d01448d3047d1bac` | 1.01 | `bowl-client:fuse.Readlink` | 0.94 | 0.13 | 0.81 | `bowl-client:storage.v1.StorageService/Readlink` (0.94 ms) |
| `cd91c57e0a033b29` | 0.72 | `bowl-client:fuse.Unlink` | 0.67 | 0.22 | 0.45 | `bowl-client:storage.v1.StorageService/Unlink` (0.67 ms) |
| `e4ef0e45499339f0` | 0.67 | `bowl-client:fuse.StatFs` | 0.63 | 0.11 | 0.52 | `bowl-client:storage.v1.StorageService/StatFS` (0.63 ms) |
| `f0db8de01301fc3d` | 0.66 | `bowl-client:fuse.Mkdir` | 0.62 | 0.16 | 0.47 | `bowl-client:storage.v1.StorageService/Mkdir` (0.62 ms) |
| `6ea05ae633b21197` | 0.66 | `bowl-client:fuse.Mkdir` | 0.62 | 0.15 | 0.47 | `bowl-client:storage.v1.StorageService/Mkdir` (0.62 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `chown_cpu.pb.gz` | `10ms  5.88%  5.88%       10ms  5.88%  connectrpc.com/connect.checkServerStreamsCanFlush` | `0     0%     0%       80ms 47.06%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_chown_cpu.txt`](results/telemetry/pprof_top/server_chown_cpu.txt) |
| `server` | `chown_heap.pb.gz` | `4097.37kB 20.88% 20.88%  4097.37kB 20.88%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%     8.82MB 46.01%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_chown_heap.txt`](results/telemetry/pprof_top/server_chown_heap.txt) |
| `server` | `chown_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_chown_mutex.txt`](results/telemetry/pprof_top/server_chown_mutex.txt) |
| `server` | `rename_cpu.pb.gz` | `20ms  8.33%  8.33%       20ms  8.33%  runtime.memclrNoHeapPointers` | `0     0%     0%      100ms 41.67%  net/http.HandlerFunc.ServeHTTP` | [`pprof_top/server_rename_cpu.txt`](results/telemetry/pprof_top/server_rename_cpu.txt) |
| `server` | `rename_heap.pb.gz` | `4097.37kB 18.15% 18.15%  4097.37kB 18.15%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    10.55MB 47.88%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_rename_heap.txt`](results/telemetry/pprof_top/server_rename_heap.txt) |
| `server` | `rename_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_rename_mutex.txt`](results/telemetry/pprof_top/server_rename_mutex.txt) |
| `client` | `chown_cpu.pb.gz` | `30ms 10.71% 10.71%       30ms 10.71%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      120ms 42.86%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_chown_cpu.txt`](results/telemetry/pprof_top/client_chown_cpu.txt) |
| `client` | `chown_heap.pb.gz` | `4762.55kB 43.25% 43.25%  4762.55kB 43.25%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%  4762.55kB 43.25%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_chown_heap.txt`](results/telemetry/pprof_top/client_chown_heap.txt) |
| `client` | `chown_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_chown_mutex.txt`](results/telemetry/pprof_top/client_chown_mutex.txt) |
| `client` | `rename_cpu.pb.gz` | `20ms  6.25%  6.25%       50ms 15.62%  runtime.mallocgcSmallScanNoHeader` | `0     0%     0%      160ms 50.00%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_rename_cpu.txt`](results/telemetry/pprof_top/client_rename_cpu.txt) |
| `client` | `rename_heap.pb.gz` | `7143.82kB 67.32% 67.32%  7143.82kB 67.32%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%  8168.23kB 76.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_rename_heap.txt`](results/telemetry/pprof_top/client_rename_heap.txt) |
| `client` | `rename_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_rename_mutex.txt`](results/telemetry/pprof_top/client_rename_mutex.txt) |

---

## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-pjdfstest-kp-20261112-050719`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj947g04pwz6x8nu9fbk`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj947g04pwz6x8nu9fbl`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-12T05:07:27Z` |
| **实例销毁时间 (UTC)** | `2026-11-12T05:16:27Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
