# Bowl 分布式文件系统 — 阿里云硅谷双机内网 `pjdfstest` 基准测试报告

被测版本采用 ADR-0030 / ADR-0031 / ADR-0032 单盘独立 Actor 存储引擎与 `64 MiB` Append-Only Pack 数据层：PostgreSQL 16 仅保存单张 `disks` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在 `packs/<pack_id_hex16>.pack` 的 `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `WriteRequest` 池化零分配解码、活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。

客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备对称的 `WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 顺序预读与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。测试挂载保持 `strictatime` 与 0 秒属性/目录项超时，数据目录全程挂载于本地 NVMe SSD（`local_ssd_pro` 894 GiB）。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 在阿里云硅谷地域（`us-west-1`）真实开启 2 台按量计费本地 NVMe SSD 云服务器（`ecs.i5.xlarge` × 2，1 台 `storage-service` + PostgreSQL 服务端节点，1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行权威 POSIX 文件系统兼容性与基准测试集 **[pjdfstest](https://github.com/pjd/pjdfstest)** 的完整实测结果。

| 核心指标 | 实测结果 |
| :--- | :--- |
| **测试结论** | **PASS 8789/8798 (99.90%)；目录退出码见明细** |
| **测试系统调用目录数** | `17` 个系统调用族 |
| **测试脚本文件数 (`.t`)** | `238` 个测试脚本 |
| **总测试断言数 (Subtests)** | `8,798` 项 POSIX 断言 |
| **通过 / 失败断言数** | `8,789` 通过 / `9` 失败（通过率 **99.90%**） |
| **TAP TODO** | `28` 项，其中 `9` 项为预期失败；按 `prove` 规则不计入失败 |
| **全量执行总耗时** | `191.94 s`（约 `3.20 min`） |
| **内网综合执行吞吐率** | `45.84 tests/s`（平均单断言耗时 `21.82 ms`） |
| **云端地域与网络拓扑** | 阿里云 `us-west-1` (`us-west-1a`) 同可用区 VPC 私网直连 |
| **测试执行时间 (UTC)** | `2026-11-10T18:49:47Z` ~ `2026-11-10T18:53:01Z` |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj92ajzkmzal3sdknoyr` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB `/dev/nvme1n1`) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.191:9000` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj92ajzkmzal3sdknoys` | `ecs.i5.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 30.7 GiB | `local_ssd_pro (NVMe)` (894 GiB) | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.190` |

### 2.2 同可用区 VPC 内网与本地 NVMe SSD 性能基线

| 指标 | 实测数值 | 采集方式 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.030/0.061/0.318/0.062` ms | `ping -c 20 -i 0.05 172.16.1.191` |
| **内网单流 TCP 吞吐带宽** | `21.22` Gbps | `iperf3 -c 172.16.1.191 -t 5` |
| **Server 本地 NVMe SSD 4 MiB 顺序写 / 读基线** | `1998.05` / `3396.35` MiB/s | `fio --bs=4M --size=2G --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `69949` / `699795` IOPS | `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |
| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `50623` IOPS (P99 `63.9` µs) | `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |

### 2.3 软件版本与挂载运行参数

| 组件 / 参数项 | 配置值 | 说明 |
| :--- | :--- | :--- |
| **Bowl Git Commit** | `9c60a34-dirty` | 仓库当前评测版本 |
| **pjdfstest Git Commit** | `85a8aea` | 上游 `https://github.com/pjd/pjdfstest` |
| **PostgreSQL 版本** | `psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)` | Server 节点控制面数据库（单张 `disks` 表） |
| **FUSE 用户态版本** | `fusermount3 version: 3.14.0` | Linux 内核 FUSE 协议 + `github.com/hanwen/go-fuse/v2` |
| **测试虚拟磁盘 (`DiskID` / `RootIno`)** | `01a1c6cb-80a4-757d-b096-9ed7f1819e5f` (`root_ino=1`) | 配额 `2PiB` |
| **服务端 `STORAGE_ATIME_MODE`** | `strictatime` | 开启严格访问时间更新，验证 `atime` 刷新语义 |
| **FUSE 缓存超时配置** | `entry=0s, attr=0s, direntry=0s` | 内核属性与目录项缓存超时；客户端 NamespaceCache 仍可命中 |
| **内核回写缓存与数据分片** | `writeback_cache=False`, `keep_cache=True`, `max_background=64`, `block_grid=262144` 字节, `pack=67108864` 字节 | `WriteBehindCoordinator`（`4 MiB × 4` 并发顺序写流水线，`<64 KiB` 直通）+ 服务端 64 MiB `.pack` 封包文件 |
| **服务端 WAL 与落盘策略** | `pebble.NoSync Group Commit + Fsync/FsyncDir 双层并行落盘屏障 (fdatasync + LogData)` | 常规写批次 Group Commit 异步提交，显式 Fsync/Sync 执行双层并行落盘屏障 |
| **客户端挂载命令** | `bowl mount <disk_id> /mnt/bowl --endpoint http://172.16.1.191:9000 --allow-other` | 启用 `default_permissions` 与 `--allow-other` 支持多 UID/GID 权限切换测试 |

---

## 3. 17 个系统调用目录兼容性与耗时汇总

| 系统调用目录 | 脚本数 (Files) | 断言数 (Tests) | PASS / ALL | 退出码 | 通过率 | 耗时 (s) | 吞吐率 (Tests/s) | 耗时占比 | 核心覆盖语义 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `chflags/` | 14 | 14 | 14/14 | 0 | 100.00% | 0.15s | 94.6 | 0.1% | 文件扩展标志位（FS_IOC_GETFLAGS / FS_IOC_SETFLAGS，验证 immutable / append 约束） |
| `chmod/` | 13 | 327 | 326/327 | 1 | 99.69% | 15.03s | 21.8 | 7.8% | 权限位变更、SUID/SGID 粘滞位保留与剥离、多身份权限检查 |
| `chown/` | 11 | 1,497 | 1495/1497 | 1 | 99.87% | 83.15s | 18.0 | 43.3% | 文件与符号链接属主/属组变更（chown/lchown/fchown）、非特权组约束与 SUID/SGID 清除 |
| `ftruncate/` | 15 | 89 | 88/89 | 1 | 98.88% | 3.80s | 23.4 | 2.0% | 基于已打开文件描述符的长度截断与扩展、只读句柄拒绝（EBADF/EINVAL） |
| `granular/` | 7 | 7 | 7/7 | 0 | 100.00% | 0.10s | 73.7 | 0.0% | 细粒度访问控制与基础文件操作边界校验 |
| `link/` | 18 | 359 | 358/359 | 1 | 99.72% | 12.90s | 27.8 | 6.7% | 硬链接创建、nlink 计数递增、跨目录硬链接与目录硬链接禁止（EPERM） |
| `mkdir/` | 13 | 118 | 118/118 | 0 | 100.00% | 2.80s | 42.1 | 1.5% | 目录创建、权限掩码、父目录写/执行权限校验、路径长度与存在性冲突（EEXIST） |
| `mkfifo/` | 13 | 120 | 120/120 | 0 | 100.00% | 2.87s | 41.9 | 1.5% | 命名管道（FIFO 特殊文件）创建与权限位验证 |
| `mknod/` | 12 | 186 | 186/186 | 0 | 100.00% | 4.91s | 37.9 | 2.6% | 字符设备、块设备、FIFO 与套接字特殊节点创建及权限验证 |
| `open/` | 27 | 337 | 337/337 | 0 | 100.00% | 5.48s | 61.5 | 2.9% | 文件打开标志组合（O_CREAT/O_EXCL/O_TRUNC/O_RDONLY/O_WRONLY/O_RDWR/O_NOFOLLOW）与权限矩阵 |
| `posix_fallocate/` | 1 | 1 | 1/1 | 0 | 100.00% | 0.05s | 20.0 | 0.0% | 物理空间预分配（fallocate Mode=0）、文件大小扩展与配额联动 |
| `rename/` | 25 | 4,857 | 4856/4857 | 1 | 99.98% | 29.59s | 164.2 | 15.4% | 同目录/跨目录原子重命名、覆盖非空目录拦截（ENOTEMPTY/EISDIR/ENOTDIR）及权限矩阵 |
| `rmdir/` | 16 | 145 | 144/145 | 1 | 99.31% | 2.98s | 48.7 | 1.6% | 空目录删除、非空目录拒绝（ENOTEMPTY）、当前目录/点目录拒绝与权限检查 |
| `symlink/` | 13 | 95 | 95/95 | 0 | 100.00% | 2.90s | 32.8 | 1.5% | 符号链接创建、目标路径长度边界与 readlink 解析 |
| `truncate/` | 15 | 84 | 83/84 | 1 | 98.81% | 3.75s | 22.4 | 2.0% | 基于路径的文件截断、稀疏扩展零填充、尺寸变更触发 ctime/mtime 更新 |
| `unlink/` | 15 | 440 | 439/440 | 1 | 99.77% | 19.97s | 22.0 | 10.4% | 文件与特殊节点删除、nlink 递减、Open-Unlink 孤儿句柄读写保活与延迟回收 |
| `utimensat/` | 10 | 122 | 122/122 | 0 | 100.00% | 1.52s | 80.3 | 0.8% | 纳秒级时间戳更新（UTIME_NOW / UTIME_OMIT / 显式时间戳）与权限校验 |
| **Total (合计)** | **238** | **8,798** | **8789/8798** | **见各目录** | **99.90%** | **191.94s** | **45.8** | **100.0%** | **17 个系统调用目录** |

---

## 4. 内网性能特征与关键 POSIX 语义验证分析

### 4.1 跨机内网 RPC 延迟与吞吐分布剖析

- **内网通信底座**：Client 与 Server 位于阿里云硅谷 `us-west-1a` 同一交换机（`vsw-rj9rsvsgpihtsgxej6me5`），内网 ICMP RTT 延迟仅 `0.030/0.061/0.318/0.062` ms，内网带宽达 `21.22` Gbps。内核属性与目录项缓存超时设为 `0s`；客户端 NamespaceCache 仍会影响请求路径。
- **耗时集中度分析**：耗时排名前三的子集为 `chown/`（1,497 项断言，耗时 83.15s，占比 43.3%）、`rename/`（4,857 项断言，耗时 29.59s，占比 15.4%）、`unlink/`（440 项断言，耗时 19.97s，占比 10.4%）。

### 4.2 关键 POSIX 契约与边界语义验证结论

本轮磁盘根 Inode 为 `1`。用例覆盖标志位、多用户权限、打开后解链和空间预分配；各目录及脚本结果见第 3、5 节。

### 4.3 未通过子项

| 用例文件 | 总断言数 | 失败数 | 失败子测试序号 |
| :--- | ---: | ---: | :--- |
| `chmod/02.t` | 5 | 1 | 5 |
| `chown/02.t` | 10 | 2 | 5, 10 |
| `ftruncate/02.t` | 5 | 1 | 5 |
| `link/02.t` | 10 | 1 | 10 |
| `rename/01.t` | 8 | 1 | 8 |
| `rmdir/02.t` | 4 | 1 | 4 |
| `truncate/02.t` | 5 | 1 | 5 |
| `unlink/02.t` | 4 | 1 | 4 |

---

## 5. 全量 238 个测试脚本明细表（Case-by-Case Breakdown）

| 测试脚本 (`Case`) | 所属目录 | 断言数 (`Tests`) | 通过/总数 (`PASS/ALL`) | 耗时 (`ms`) | 状态 |
| :--- | :--- | ---: | ---: | ---: | :--- |
| `chflags/00.t` | `chflags/` | 1 | 1/1 | 6 ms | PASS |
| `chflags/01.t` | `chflags/` | 1 | 1/1 | 7 ms | PASS |
| `chflags/02.t` | `chflags/` | 1 | 1/1 | 7 ms | PASS |
| `chflags/03.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/04.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/05.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/06.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/07.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/08.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/09.t` | `chflags/` | 1 | 1/1 | 8 ms | PASS |
| `chflags/10.t` | `chflags/` | 1 | 1/1 | 8 ms | PASS |
| `chflags/11.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/12.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/13.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/00.t` | `chmod/` | 119 | 119/119 | 12,409 ms | PASS |
| `chmod/01.t` | `chmod/` | 17 | 17/17 | 67 ms | PASS |
| `chmod/02.t` | `chmod/` | 5 | 4/5 | - | FAIL |
| `chmod/03.t` | `chmod/` | 5 | 5/5 | 1,076 ms | PASS |
| `chmod/04.t` | `chmod/` | 7 | 7/7 | 26 ms | PASS |
| `chmod/05.t` | `chmod/` | 14 | 14/14 | 43 ms | PASS |
| `chmod/06.t` | `chmod/` | 8 | 8/8 | 686 ms | PASS |
| `chmod/07.t` | `chmod/` | 25 | 25/25 | 126 ms | PASS |
| `chmod/08.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/09.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/10.t` | `chmod/` | 2 | 2/2 | 8 ms | PASS |
| `chmod/11.t` | `chmod/` | 109 | 109/109 | 399 ms | PASS |
| `chmod/12.t` | `chmod/` | 14 | 14/14 | 57 ms | PASS |
| `chown/00.t` | `chown/` | 1280 | 1280/1280 | 80,313 ms | PASS |
| `chown/01.t` | `chown/` | 22 | 22/22 | 63 ms | PASS |
| `chown/02.t` | `chown/` | 10 | 8/10 | - | FAIL |
| `chown/03.t` | `chown/` | 10 | 10/10 | 1,020 ms | PASS |
| `chown/04.t` | `chown/` | 9 | 9/9 | 34 ms | PASS |
| `chown/05.t` | `chown/` | 18 | 18/18 | 71 ms | PASS |
| `chown/06.t` | `chown/` | 10 | 10/10 | 1,047 ms | PASS |
| `chown/07.t` | `chown/` | 132 | 132/132 | 426 ms | PASS |
| `chown/08.t` | `chown/` | 1 | 1/1 | 7 ms | PASS |
| `chown/09.t` | `chown/` | 1 | 1/1 | 4 ms | PASS |
| `chown/10.t` | `chown/` | 4 | 4/4 | 11 ms | PASS |
| `ftruncate/00.t` | `ftruncate/` | 26 | 26/26 | 2,114 ms | PASS |
| `ftruncate/01.t` | `ftruncate/` | 5 | 5/5 | 24 ms | PASS |
| `ftruncate/02.t` | `ftruncate/` | 5 | 4/5 | - | FAIL |
| `ftruncate/03.t` | `ftruncate/` | 5 | 5/5 | 970 ms | PASS |
| `ftruncate/04.t` | `ftruncate/` | 4 | 4/4 | 20 ms | PASS |
| `ftruncate/05.t` | `ftruncate/` | 15 | 15/15 | 68 ms | PASS |
| `ftruncate/06.t` | `ftruncate/` | 8 | 8/8 | 31 ms | PASS |
| `ftruncate/07.t` | `ftruncate/` | 6 | 6/6 | 371 ms | PASS |
| `ftruncate/08.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/09.t` | `ftruncate/` | 3 | 3/3 | 22 ms | PASS |
| `ftruncate/10.t` | `ftruncate/` | 1 | 1/1 | 5 ms | PASS |
| `ftruncate/11.t` | `ftruncate/` | 1 | 1/1 | 6 ms | PASS |
| `ftruncate/12.t` | `ftruncate/` | 3 | 3/3 | 14 ms | PASS |
| `ftruncate/13.t` | `ftruncate/` | 4 | 4/4 | 15 ms | PASS |
| `ftruncate/14.t` | `ftruncate/` | 2 | 2/2 | 8 ms | PASS |
| `granular/00.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/01.t` | `granular/` | 1 | 1/1 | 7 ms | PASS |
| `granular/02.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/03.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/04.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/05.t` | `granular/` | 1 | 1/1 | 8 ms | PASS |
| `granular/06.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `link/00.t` | `link/` | 202 | 202/202 | 10,501 ms | PASS |
| `link/01.t` | `link/` | 32 | 32/32 | 114 ms | PASS |
| `link/02.t` | `link/` | 10 | 9/10 | - | FAIL |
| `link/03.t` | `link/` | 13 | 13/13 | 1,025 ms | PASS |
| `link/04.t` | `link/` | 6 | 6/6 | 25 ms | PASS |
| `link/05.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/06.t` | `link/` | 18 | 18/18 | 94 ms | PASS |
| `link/07.t` | `link/` | 17 | 17/17 | 92 ms | PASS |
| `link/08.t` | `link/` | 10 | 10/10 | 694 ms | PASS |
| `link/09.t` | `link/` | 5 | 5/5 | 29 ms | PASS |
| `link/10.t` | `link/` | 23 | 23/23 | 79 ms | PASS |
| `link/11.t` | `link/` | 9 | 9/9 | 34 ms | PASS |
| `link/12.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/13.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/14.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/15.t` | `link/` | 1 | 1/1 | 5 ms | PASS |
| `link/16.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/17.t` | `link/` | 8 | 8/8 | 22 ms | PASS |
| `mkdir/00.t` | `mkdir/` | 36 | 36/36 | 1,099 ms | PASS |
| `mkdir/01.t` | `mkdir/` | 17 | 17/17 | 65 ms | PASS |
| `mkdir/02.t` | `mkdir/` | 3 | 3/3 | 52 ms | PASS |
| `mkdir/03.t` | `mkdir/` | 3 | 3/3 | 973 ms | PASS |
| `mkdir/04.t` | `mkdir/` | 3 | 3/3 | 18 ms | PASS |
| `mkdir/05.t` | `mkdir/` | 12 | 12/12 | 50 ms | PASS |
| `mkdir/06.t` | `mkdir/` | 12 | 12/12 | 60 ms | PASS |
| `mkdir/07.t` | `mkdir/` | 6 | 6/6 | 315 ms | PASS |
| `mkdir/08.t` | `mkdir/` | 1 | 1/1 | 6 ms | PASS |
| `mkdir/09.t` | `mkdir/` | 1 | 1/1 | 5 ms | PASS |
| `mkdir/10.t` | `mkdir/` | 21 | 21/21 | 70 ms | PASS |
| `mkdir/11.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/12.t` | `mkdir/` | 2 | 2/2 | 11 ms | PASS |
| `mkfifo/00.t` | `mkfifo/` | 36 | 36/36 | 1,115 ms | PASS |
| `mkfifo/01.t` | `mkfifo/` | 17 | 17/17 | 58 ms | PASS |
| `mkfifo/02.t` | `mkfifo/` | 4 | 4/4 | 48 ms | PASS |
| `mkfifo/03.t` | `mkfifo/` | 4 | 4/4 | 1,022 ms | PASS |
| `mkfifo/04.t` | `mkfifo/` | 3 | 3/3 | 19 ms | PASS |
| `mkfifo/05.t` | `mkfifo/` | 12 | 12/12 | 56 ms | PASS |
| `mkfifo/06.t` | `mkfifo/` | 12 | 12/12 | 56 ms | PASS |
| `mkfifo/07.t` | `mkfifo/` | 6 | 6/6 | 336 ms | PASS |
| `mkfifo/08.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/09.t` | `mkfifo/` | 21 | 21/21 | 61 ms | PASS |
| `mkfifo/10.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/11.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/12.t` | `mkfifo/` | 2 | 2/2 | 7 ms | PASS |
| `mknod/00.t` | `mknod/` | 36 | 36/36 | 1,111 ms | PASS |
| `mknod/01.t` | `mknod/` | 27 | 27/27 | 67 ms | PASS |
| `mknod/02.t` | `mknod/` | 12 | 12/12 | 77 ms | PASS |
| `mknod/03.t` | `mknod/` | 12 | 12/12 | 953 ms | PASS |
| `mknod/04.t` | `mknod/` | 3 | 3/3 | 18 ms | PASS |
| `mknod/05.t` | `mknod/` | 12 | 12/12 | 54 ms | PASS |
| `mknod/06.t` | `mknod/` | 12 | 12/12 | 56 ms | PASS |
| `mknod/07.t` | `mknod/` | 6 | 6/6 | 334 ms | PASS |
| `mknod/08.t` | `mknod/` | 35 | 35/35 | 93 ms | PASS |
| `mknod/09.t` | `mknod/` | 1 | 1/1 | 4 ms | PASS |
| `mknod/10.t` | `mknod/` | 2 | 2/2 | 11 ms | PASS |
| `mknod/11.t` | `mknod/` | 28 | 28/28 | 2,057 ms | PASS |
| `open/00.t` | `open/` | 47 | 47/47 | 3,152 ms | PASS |
| `open/01.t` | `open/` | 22 | 22/22 | 68 ms | PASS |
| `open/02.t` | `open/` | 4 | 4/4 | 65 ms | PASS |
| `open/03.t` | `open/` | 4 | 4/4 | 1,021 ms | PASS |
| `open/04.t` | `open/` | 4 | 4/4 | 18 ms | PASS |
| `open/05.t` | `open/` | 12 | 12/12 | 42 ms | PASS |
| `open/06.t` | `open/` | 144 | 144/144 | 322 ms | PASS |
| `open/07.t` | `open/` | 25 | 25/25 | 89 ms | PASS |
| `open/08.t` | `open/` | 3 | 3/3 | 22 ms | PASS |
| `open/09.t` | `open/` | 1 | 1/1 | 5 ms | PASS |
| `open/10.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/11.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/12.t` | `open/` | 6 | 6/6 | 335 ms | PASS |
| `open/13.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/14.t` | `open/` | 1 | 1/1 | 8 ms | PASS |
| `open/15.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/16.t` | `open/` | 6 | 6/6 | 23 ms | PASS |
| `open/17.t` | `open/` | 3 | 3/3 | 14 ms | PASS |
| `open/18.t` | `open/` | 1 | 1/1 | 5 ms | PASS |
| `open/19.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/20.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/21.t` | `open/` | 2 | 2/2 | 8 ms | PASS |
| `open/22.t` | `open/` | 21 | 21/21 | 63 ms | PASS |
| `open/23.t` | `open/` | 5 | 5/5 | 17 ms | PASS |
| `open/24.t` | `open/` | 5 | 5/5 | 18 ms | PASS |
| `open/25.t` | `open/` | 6 | 6/6 | 29 ms | PASS |
| `open/26.t` | `open/` | 9 | 9/9 | 29 ms | PASS |
| `posix_fallocate/00.t` | `posix_fallocate/` | 1 | 1/1 | 4 ms | PASS |
| `rename/00.t` | `rename/` | 122 | 122/122 | 7,375 ms | PASS |
| `rename/01.t` | `rename/` | 8 | 7/8 | - | FAIL |
| `rename/02.t` | `rename/` | 6 | 6/6 | 975 ms | PASS |
| `rename/03.t` | `rename/` | 6 | 6/6 | 27 ms | PASS |
| `rename/04.t` | `rename/` | 18 | 18/18 | 94 ms | PASS |
| `rename/05.t` | `rename/` | 17 | 17/17 | 87 ms | PASS |
| `rename/06.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/07.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/08.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/09.t` | `rename/` | 2353 | 2353/2353 | 7,578 ms | PASS |
| `rename/10.t` | `rename/` | 2099 | 2099/2099 | 6,777 ms | PASS |
| `rename/11.t` | `rename/` | 10 | 10/10 | 698 ms | PASS |
| `rename/12.t` | `rename/` | 32 | 32/32 | 94 ms | PASS |
| `rename/13.t` | `rename/` | 32 | 32/32 | 86 ms | PASS |
| `rename/14.t` | `rename/` | 32 | 32/32 | 76 ms | PASS |
| `rename/15.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/16.t` | `rename/` | 1 | 1/1 | 8 ms | PASS |
| `rename/17.t` | `rename/` | 8 | 8/8 | 21 ms | PASS |
| `rename/18.t` | `rename/` | 6 | 6/6 | 25 ms | PASS |
| `rename/19.t` | `rename/` | 6 | 6/6 | 30 ms | PASS |
| `rename/20.t` | `rename/` | 25 | 25/25 | 113 ms | PASS |
| `rename/21.t` | `rename/` | 16 | 16/16 | 82 ms | PASS |
| `rename/22.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/23.t` | `rename/` | 42 | 42/42 | 5,186 ms | PASS |
| `rename/24.t` | `rename/` | 13 | 13/13 | 50 ms | PASS |
| `rmdir/00.t` | `rmdir/` | 10 | 10/10 | 1,032 ms | PASS |
| `rmdir/01.t` | `rmdir/` | 14 | 14/14 | 60 ms | PASS |
| `rmdir/02.t` | `rmdir/` | 4 | 3/4 | - | FAIL |
| `rmdir/03.t` | `rmdir/` | 5 | 5/5 | 990 ms | PASS |
| `rmdir/04.t` | `rmdir/` | 4 | 4/4 | 24 ms | PASS |
| `rmdir/05.t` | `rmdir/` | 6 | 6/6 | 368 ms | PASS |
| `rmdir/06.t` | `rmdir/` | 23 | 23/23 | 94 ms | PASS |
| `rmdir/07.t` | `rmdir/` | 10 | 10/10 | 42 ms | PASS |
| `rmdir/08.t` | `rmdir/` | 10 | 10/10 | 47 ms | PASS |
| `rmdir/09.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/10.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/11.t` | `rmdir/` | 47 | 47/47 | 151 ms | PASS |
| `rmdir/12.t` | `rmdir/` | 6 | 6/6 | 25 ms | PASS |
| `rmdir/13.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/14.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/15.t` | `rmdir/` | 2 | 2/2 | 8 ms | PASS |
| `symlink/00.t` | `symlink/` | 14 | 14/14 | 1,050 ms | PASS |
| `symlink/01.t` | `symlink/` | 5 | 5/5 | 31 ms | PASS |
| `symlink/02.t` | `symlink/` | 7 | 7/7 | 74 ms | PASS |
| `symlink/03.t` | `symlink/` | 6 | 6/6 | 1,061 ms | PASS |
| `symlink/04.t` | `symlink/` | 3 | 3/3 | 15 ms | PASS |
| `symlink/05.t` | `symlink/` | 12 | 12/12 | 50 ms | PASS |
| `symlink/06.t` | `symlink/` | 12 | 12/12 | 63 ms | PASS |
| `symlink/07.t` | `symlink/` | 6 | 6/6 | 382 ms | PASS |
| `symlink/08.t` | `symlink/` | 21 | 21/21 | 71 ms | PASS |
| `symlink/09.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/10.t` | `symlink/` | 1 | 1/1 | 5 ms | PASS |
| `symlink/11.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/12.t` | `symlink/` | 6 | 6/6 | 18 ms | PASS |
| `truncate/00.t` | `truncate/` | 21 | 21/21 | 2,071 ms | PASS |
| `truncate/01.t` | `truncate/` | 5 | 5/5 | 21 ms | PASS |
| `truncate/02.t` | `truncate/` | 5 | 4/5 | - | FAIL |
| `truncate/03.t` | `truncate/` | 5 | 5/5 | 999 ms | PASS |
| `truncate/04.t` | `truncate/` | 4 | 4/4 | 17 ms | PASS |
| `truncate/05.t` | `truncate/` | 15 | 15/15 | 53 ms | PASS |
| `truncate/06.t` | `truncate/` | 8 | 8/8 | 30 ms | PASS |
| `truncate/07.t` | `truncate/` | 6 | 6/6 | 361 ms | PASS |
| `truncate/08.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/09.t` | `truncate/` | 3 | 3/3 | 17 ms | PASS |
| `truncate/10.t` | `truncate/` | 1 | 1/1 | 5 ms | PASS |
| `truncate/11.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/12.t` | `truncate/` | 3 | 3/3 | 17 ms | PASS |
| `truncate/13.t` | `truncate/` | 4 | 4/4 | 15 ms | PASS |
| `truncate/14.t` | `truncate/` | 2 | 2/2 | 8 ms | PASS |
| `unlink/00.t` | `unlink/` | 112 | 112/112 | 17,322 ms | PASS |
| `unlink/01.t` | `unlink/` | 5 | 5/5 | 29 ms | PASS |
| `unlink/02.t` | `unlink/` | 4 | 3/4 | - | FAIL |
| `unlink/03.t` | `unlink/` | 4 | 4/4 | 1,092 ms | PASS |
| `unlink/04.t` | `unlink/` | 4 | 4/4 | 20 ms | PASS |
| `unlink/05.t` | `unlink/` | 10 | 10/10 | 51 ms | PASS |
| `unlink/06.t` | `unlink/` | 10 | 10/10 | 46 ms | PASS |
| `unlink/07.t` | `unlink/` | 6 | 6/6 | 368 ms | PASS |
| `unlink/08.t` | `unlink/` | 3 | 3/3 | 13 ms | PASS |
| `unlink/09.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/10.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/11.t` | `unlink/` | 270 | 270/270 | 838 ms | PASS |
| `unlink/12.t` | `unlink/` | 1 | 1/1 | 5 ms | PASS |
| `unlink/13.t` | `unlink/` | 2 | 2/2 | 8 ms | PASS |
| `unlink/14.t` | `unlink/` | 7 | 7/7 | 42 ms | PASS |
| `utimensat/00.t` | `utimensat/` | 32 | 32/32 | 102 ms | PASS |
| `utimensat/01.t` | `utimensat/` | 7 | 7/7 | 1,030 ms | PASS |
| `utimensat/02.t` | `utimensat/` | 10 | 10/10 | 32 ms | PASS |
| `utimensat/03.t` | `utimensat/` | 1 | 1/1 | 4 ms | PASS |
| `utimensat/04.t` | `utimensat/` | 10 | 10/10 | 37 ms | PASS |
| `utimensat/05.t` | `utimensat/` | 16 | 16/16 | 58 ms | PASS |
| `utimensat/06.t` | `utimensat/` | 13 | 13/13 | 59 ms | PASS |
| `utimensat/07.t` | `utimensat/` | 17 | 17/17 | 49 ms | PASS |
| `utimensat/08.t` | `utimensat/` | 9 | 9/9 | 37 ms | PASS |
| `utimensat/09.t` | `utimensat/` | 7 | 7/7 | 43 ms | PASS |
| **Total** | **17 dirs** | **8,798** | **8789/8798** | **191,935 ms** | **PASS 8789/8798 (99.90%)；目录退出码见明细** |

---

## 6. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-pjdfstest-kp-20261110-184514`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj92ajzkmzal3sdknoyr`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj92ajzkmzal3sdknoys`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-10T18:45:20Z` |
| **实例销毁时间 (UTC)** | `2026-11-10T18:53:14Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
