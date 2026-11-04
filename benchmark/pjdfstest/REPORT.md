# Bowl 分布式文件系统 — 阿里云硅谷双机内网 `pjdfstest` 基准测试报告

被测版本采用 ADR-0029：PostgreSQL 仅保存 `disks`、`commits` 和 `client_sessions` 控制面表；每个分支通过常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 和 1 MiB 数据块，整块写入与局部补丁分别使用 Set 和 ValueMerger，读取按请求区间解码（DecodeBlobRange）；常规写批次以 `pebble.NoSync` 提交异步 WAL，并由后台 1 秒刷盘与 `WALBytesPerSync` 兜底。Checkpoint 和 Fork 通过硬链接共享文件。客户端在本地维护 FileHandle、FileLock、O_TMPFILE 和 O(1) 索引的 NamespaceCache。

客户端默认开启 FUSE 内核 `CAP_WRITEBACK_CACHE` 与 `FOPEN_KEEP_CACHE`，常规 `write()` 由内核页缓存聚合为最大 1 MiB 块回写；Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。测试挂载保持 `strictatime` 与 0 秒属性/目录项超时，以覆盖真实元数据路径。

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 在阿里云硅谷地域（`us-west-1`）真实开启 2 台按量计费云服务器（1 台 `storage-service` + PostgreSQL 服务端节点，1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行权威 POSIX 文件系统兼容性与基准测试集 **[pjdfstest](https://github.com/pjd/pjdfstest)** 的完整实测结果。

| 核心指标 | 实测结果 |
| :--- | :--- |
| **测试结论** | **100% PASS (全量通过)** |
| **测试系统调用目录数** | `17` 个系统调用族 |
| **测试脚本文件数 (`.t`)** | `238` 个测试脚本 |
| **总测试断言数 (Subtests)** | `8,798` 项 POSIX 断言 |
| **通过 / 失败断言数** | `8,798` 通过 / `0` 失败（通过率 **100.00%**） |
| **TAP TODO** | `28` 项，其中 `9` 项为预期失败；按 `prove` 规则不计入失败 |
| **全量执行总耗时** | `172.85 s`（约 `2.88 min`） |
| **内网综合执行吞吐率** | `50.90 tests/s`（平均单断言耗时 `19.65 ms`） |
| **云端地域与网络拓扑** | 阿里云 `us-west-1` (`us-west-1a`) 同可用区 VPC 私网直连 |
| **测试执行时间 (UTC)** | `2026-11-04T09:20:17Z` ~ `2026-11-04T09:23:12Z` |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj91n7p8m56c4bop2m6v` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.199:9000` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj91n7p8m56c4bop2m6w` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.198` |

### 2.2 同可用区 VPC 内网性能基线（Client <-> Server）

| 网络指标 | 实测数值 | 采集方式 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.062/0.090/0.289/0.050` ms | `ping -c 20 -i 0.05 172.16.1.199` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.199 -t 5` |

### 2.3 软件版本与挂载运行参数

| 组件 / 参数项 | 配置值 | 说明 |
| :--- | :--- | :--- |
| **Bowl Git Commit** | `626dd03` | 仓库当前评测版本 |
| **pjdfstest Git Commit** | `85a8aea` | 上游 `https://github.com/pjd/pjdfstest` |
| **PostgreSQL 版本** | `psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)` | Server 节点控制面数据库 |
| **FUSE 用户态版本** | `fusermount3 version: 3.14.0` | Linux 内核 FUSE 协议 + `github.com/hanwen/go-fuse/v2` |
| **测试虚拟磁盘 (`DiskID` / `RootIno`)** | `01a1a5db-f468-721b-9643-c1fc70852cf5` (`root_ino=1`) | 配额 `2PiB` |
| **服务端 `STORAGE_ATIME_MODE`** | `strictatime` | 开启严格访问时间更新，验证 `atime` 刷新语义 |
| **FUSE 缓存超时配置** | `entry=0s, attr=0s, direntry=0s` | 内核属性与目录项缓存超时；客户端 NamespaceCache 仍可命中 |
| **内核回写缓存与数据分片** | `writeback_cache=True`, `keep_cache=True`, `blob=1048576` 字节 | 内核页缓存聚合回写；Flush 不触发 Fsync RPC，服务端按 1 MiB 分片写入 |
| **服务端 WAL 同步策略** | `pebble.NoSync + 1s 后台刷盘` | 常规写批次异步提交，显式 Fsync/Sync 才写入持久化屏障 |
| **客户端挂载命令** | `bowl mount <disk_id> /mnt/bowl --endpoint http://172.16.1.199:9000 --allow-other` | 启用 `default_permissions` 与 `--allow-other` 支持多 UID/GID 权限切换测试 |

---

## 3. 17 个系统调用目录兼容性与耗时汇总

| 系统调用目录 | 脚本数 (Files) | 断言数 (Tests) | PASS / ALL | 退出码 | 通过率 | 耗时 (s) | 吞吐率 (Tests/s) | 耗时占比 | 核心覆盖语义 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `chflags/` | 14 | 14 | 14/14 | 0 | 100.00% | 0.14s | 97.9 | 0.1% | 文件扩展标志位（FS_IOC_GETFLAGS / FS_IOC_SETFLAGS，验证 immutable / append 约束） |
| `chmod/` | 13 | 327 | 327/327 | 0 | 100.00% | 13.60s | 24.0 | 7.9% | 权限位变更、SUID/SGID 粘滞位保留与剥离、多身份权限检查 |
| `chown/` | 11 | 1,497 | 1497/1497 | 0 | 100.00% | 80.15s | 18.7 | 46.4% | 文件与符号链接属主/属组变更（chown/lchown/fchown）、非特权组约束与 SUID/SGID 清除 |
| `ftruncate/` | 15 | 89 | 89/89 | 0 | 100.00% | 3.01s | 29.6 | 1.7% | 基于已打开文件描述符的长度截断与扩展、只读句柄拒绝（EBADF/EINVAL） |
| `granular/` | 7 | 7 | 7/7 | 0 | 100.00% | 0.09s | 78.7 | 0.1% | 细粒度访问控制与基础文件操作边界校验 |
| `link/` | 18 | 359 | 359/359 | 0 | 100.00% | 11.71s | 30.7 | 6.8% | 硬链接创建、nlink 计数递增、跨目录硬链接与目录硬链接禁止（EPERM） |
| `mkdir/` | 13 | 118 | 118/118 | 0 | 100.00% | 2.07s | 57.1 | 1.2% | 目录创建、权限掩码、父目录写/执行权限校验、路径长度与存在性冲突（EEXIST） |
| `mkfifo/` | 13 | 120 | 120/120 | 0 | 100.00% | 2.08s | 57.7 | 1.2% | 命名管道（FIFO 特殊文件）创建与权限位验证 |
| `mknod/` | 12 | 186 | 186/186 | 0 | 100.00% | 4.19s | 44.4 | 2.4% | 字符设备、块设备、FIFO 与套接字特殊节点创建及权限验证 |
| `open/` | 27 | 337 | 337/337 | 0 | 100.00% | 4.58s | 73.6 | 2.6% | 文件打开标志组合（O_CREAT/O_EXCL/O_TRUNC/O_RDONLY/O_WRONLY/O_RDWR/O_NOFOLLOW）与权限矩阵 |
| `posix_fallocate/` | 1 | 1 | 1/1 | 0 | 100.00% | 0.05s | 19.6 | 0.0% | 物理空间预分配（fallocate Mode=0）、文件大小扩展与配额联动 |
| `rename/` | 25 | 4,857 | 4857/4857 | 0 | 100.00% | 23.80s | 204.0 | 13.8% | 同目录/跨目录原子重命名、覆盖非空目录拦截（ENOTEMPTY/EISDIR/ENOTDIR）及权限矩阵 |
| `rmdir/` | 16 | 145 | 145/145 | 0 | 100.00% | 2.12s | 68.3 | 1.2% | 空目录删除、非空目录拒绝（ENOTEMPTY）、当前目录/点目录拒绝与权限检查 |
| `symlink/` | 13 | 95 | 95/95 | 0 | 100.00% | 2.00s | 47.4 | 1.2% | 符号链接创建、目标路径长度边界与 readlink 解析 |
| `truncate/` | 15 | 84 | 84/84 | 0 | 100.00% | 3.01s | 27.9 | 1.7% | 基于路径的文件截断、稀疏扩展零填充、尺寸变更触发 ctime/mtime 更新 |
| `unlink/` | 15 | 440 | 440/440 | 0 | 100.00% | 18.82s | 23.4 | 10.9% | 文件与特殊节点删除、nlink 递减、Open-Unlink 孤儿句柄读写保活与延迟回收 |
| `utimensat/` | 10 | 122 | 122/122 | 0 | 100.00% | 1.43s | 85.4 | 0.8% | 纳秒级时间戳更新（UTIME_NOW / UTIME_OMIT / 显式时间戳）与权限校验 |
| **Total (合计)** | **238** | **8,798** | **8798/8798** | **全部为 0** | **100.00%** | **172.85s** | **50.9** | **100.0%** | **17 个系统调用目录** |

---

## 4. 内网性能特征与关键 POSIX 语义验证分析

### 4.1 跨机内网 RPC 延迟与吞吐分布剖析

- **内网通信底座**：Client 与 Server 位于阿里云硅谷 `us-west-1a` 同一交换机（`vsw-rj9rsvsgpihtsgxej6me5`），内网 ICMP RTT 延迟仅 `0.062/0.090/0.289/0.050` ms，内网带宽达 `15.71` Gbps。内核属性与目录项缓存超时设为 `0s`；客户端 NamespaceCache 仍会影响请求路径。
- **耗时集中度分析**：耗时排名前三的子集为 `chown/`（1,497 项断言，耗时 80.15s，占比 46.4%）、`rename/`（4,857 项断言，耗时 23.80s，占比 13.8%）、`unlink/`（440 项断言，耗时 18.82s，占比 10.9%）。

### 4.2 关键 POSIX 契约与边界语义验证结论

本轮磁盘根 Inode 为 `1`。用例覆盖标志位、多用户权限、打开后解链和空间预分配；各目录及脚本结果见第 3、5 节。

---

## 5. 全量 238 个测试脚本明细表（Case-by-Case Breakdown）

| 测试脚本 (`Case`) | 所属目录 | 断言数 (`Tests`) | 通过/总数 (`PASS/ALL`) | 耗时 (`ms`) | 状态 |
| :--- | :--- | ---: | ---: | ---: | :--- |
| `chflags/00.t` | `chflags/` | 1 | 1/1 | 5 ms | PASS |
| `chflags/01.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/02.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/03.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/04.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/05.t` | `chflags/` | 1 | 1/1 | 5 ms | PASS |
| `chflags/06.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/07.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/08.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/09.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/10.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/11.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/12.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chflags/13.t` | `chflags/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/00.t` | `chmod/` | 119 | 119/119 | 12,292 ms | PASS |
| `chmod/01.t` | `chmod/` | 17 | 17/17 | 49 ms | PASS |
| `chmod/02.t` | `chmod/` | 5 | 5/5 | 43 ms | PASS |
| `chmod/03.t` | `chmod/` | 5 | 5/5 | 584 ms | PASS |
| `chmod/04.t` | `chmod/` | 7 | 7/7 | 28 ms | PASS |
| `chmod/05.t` | `chmod/` | 14 | 14/14 | 42 ms | PASS |
| `chmod/06.t` | `chmod/` | 8 | 8/8 | 99 ms | PASS |
| `chmod/07.t` | `chmod/` | 25 | 25/25 | 68 ms | PASS |
| `chmod/08.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/09.t` | `chmod/` | 1 | 1/1 | 4 ms | PASS |
| `chmod/10.t` | `chmod/` | 2 | 2/2 | 7 ms | PASS |
| `chmod/11.t` | `chmod/` | 109 | 109/109 | 258 ms | PASS |
| `chmod/12.t` | `chmod/` | 14 | 14/14 | 47 ms | PASS |
| `chown/00.t` | `chown/` | 1280 | 1280/1280 | 78,845 ms | PASS |
| `chown/01.t` | `chown/` | 22 | 22/22 | 57 ms | PASS |
| `chown/02.t` | `chown/` | 10 | 10/10 | 55 ms | PASS |
| `chown/03.t` | `chown/` | 10 | 10/10 | 592 ms | PASS |
| `chown/04.t` | `chown/` | 9 | 9/9 | 31 ms | PASS |
| `chown/05.t` | `chown/` | 18 | 18/18 | 49 ms | PASS |
| `chown/06.t` | `chown/` | 10 | 10/10 | 114 ms | PASS |
| `chown/07.t` | `chown/` | 132 | 132/132 | 316 ms | PASS |
| `chown/08.t` | `chown/` | 1 | 1/1 | 4 ms | PASS |
| `chown/09.t` | `chown/` | 1 | 1/1 | 5 ms | PASS |
| `chown/10.t` | `chown/` | 4 | 4/4 | 10 ms | PASS |
| `ftruncate/00.t` | `ftruncate/` | 26 | 26/26 | 2,074 ms | PASS |
| `ftruncate/01.t` | `ftruncate/` | 5 | 5/5 | 21 ms | PASS |
| `ftruncate/02.t` | `ftruncate/` | 5 | 5/5 | 45 ms | PASS |
| `ftruncate/03.t` | `ftruncate/` | 5 | 5/5 | 579 ms | PASS |
| `ftruncate/04.t` | `ftruncate/` | 4 | 4/4 | 19 ms | PASS |
| `ftruncate/05.t` | `ftruncate/` | 15 | 15/15 | 43 ms | PASS |
| `ftruncate/06.t` | `ftruncate/` | 8 | 8/8 | 27 ms | PASS |
| `ftruncate/07.t` | `ftruncate/` | 6 | 6/6 | 55 ms | PASS |
| `ftruncate/08.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/09.t` | `ftruncate/` | 3 | 3/3 | 13 ms | PASS |
| `ftruncate/10.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/11.t` | `ftruncate/` | 1 | 1/1 | 4 ms | PASS |
| `ftruncate/12.t` | `ftruncate/` | 3 | 3/3 | 15 ms | PASS |
| `ftruncate/13.t` | `ftruncate/` | 4 | 4/4 | 15 ms | PASS |
| `ftruncate/14.t` | `ftruncate/` | 2 | 2/2 | 7 ms | PASS |
| `granular/00.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/01.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/02.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/03.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/04.t` | `granular/` | 1 | 1/1 | 5 ms | PASS |
| `granular/05.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `granular/06.t` | `granular/` | 1 | 1/1 | 4 ms | PASS |
| `link/00.t` | `link/` | 202 | 202/202 | 10,435 ms | PASS |
| `link/01.t` | `link/` | 32 | 32/32 | 88 ms | PASS |
| `link/02.t` | `link/` | 10 | 10/10 | 60 ms | PASS |
| `link/03.t` | `link/` | 13 | 13/13 | 610 ms | PASS |
| `link/04.t` | `link/` | 6 | 6/6 | 26 ms | PASS |
| `link/05.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/06.t` | `link/` | 18 | 18/18 | 57 ms | PASS |
| `link/07.t` | `link/` | 17 | 17/17 | 56 ms | PASS |
| `link/08.t` | `link/` | 10 | 10/10 | 132 ms | PASS |
| `link/09.t` | `link/` | 5 | 5/5 | 22 ms | PASS |
| `link/10.t` | `link/` | 23 | 23/23 | 62 ms | PASS |
| `link/11.t` | `link/` | 9 | 9/9 | 33 ms | PASS |
| `link/12.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/13.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/14.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/15.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/16.t` | `link/` | 1 | 1/1 | 4 ms | PASS |
| `link/17.t` | `link/` | 8 | 8/8 | 21 ms | PASS |
| `mkdir/00.t` | `mkdir/` | 36 | 36/36 | 1,084 ms | PASS |
| `mkdir/01.t` | `mkdir/` | 17 | 17/17 | 48 ms | PASS |
| `mkdir/02.t` | `mkdir/` | 3 | 3/3 | 40 ms | PASS |
| `mkdir/03.t` | `mkdir/` | 3 | 3/3 | 585 ms | PASS |
| `mkdir/04.t` | `mkdir/` | 3 | 3/3 | 17 ms | PASS |
| `mkdir/05.t` | `mkdir/` | 12 | 12/12 | 40 ms | PASS |
| `mkdir/06.t` | `mkdir/` | 12 | 12/12 | 39 ms | PASS |
| `mkdir/07.t` | `mkdir/` | 6 | 6/6 | 68 ms | PASS |
| `mkdir/08.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/09.t` | `mkdir/` | 1 | 1/1 | 4 ms | PASS |
| `mkdir/10.t` | `mkdir/` | 21 | 21/21 | 51 ms | PASS |
| `mkdir/11.t` | `mkdir/` | 1 | 1/1 | 5 ms | PASS |
| `mkdir/12.t` | `mkdir/` | 2 | 2/2 | 7 ms | PASS |
| `mkfifo/00.t` | `mkfifo/` | 36 | 36/36 | 1,083 ms | PASS |
| `mkfifo/01.t` | `mkfifo/` | 17 | 17/17 | 49 ms | PASS |
| `mkfifo/02.t` | `mkfifo/` | 4 | 4/4 | 42 ms | PASS |
| `mkfifo/03.t` | `mkfifo/` | 4 | 4/4 | 591 ms | PASS |
| `mkfifo/04.t` | `mkfifo/` | 3 | 3/3 | 17 ms | PASS |
| `mkfifo/05.t` | `mkfifo/` | 12 | 12/12 | 39 ms | PASS |
| `mkfifo/06.t` | `mkfifo/` | 12 | 12/12 | 39 ms | PASS |
| `mkfifo/07.t` | `mkfifo/` | 6 | 6/6 | 73 ms | PASS |
| `mkfifo/08.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/09.t` | `mkfifo/` | 21 | 21/21 | 50 ms | PASS |
| `mkfifo/10.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/11.t` | `mkfifo/` | 1 | 1/1 | 4 ms | PASS |
| `mkfifo/12.t` | `mkfifo/` | 2 | 2/2 | 7 ms | PASS |
| `mknod/00.t` | `mknod/` | 36 | 36/36 | 1,082 ms | PASS |
| `mknod/01.t` | `mknod/` | 27 | 27/27 | 65 ms | PASS |
| `mknod/02.t` | `mknod/` | 12 | 12/12 | 60 ms | PASS |
| `mknod/03.t` | `mknod/` | 12 | 12/12 | 599 ms | PASS |
| `mknod/04.t` | `mknod/` | 3 | 3/3 | 16 ms | PASS |
| `mknod/05.t` | `mknod/` | 12 | 12/12 | 40 ms | PASS |
| `mknod/06.t` | `mknod/` | 12 | 12/12 | 40 ms | PASS |
| `mknod/07.t` | `mknod/` | 6 | 6/6 | 62 ms | PASS |
| `mknod/08.t` | `mknod/` | 35 | 35/35 | 73 ms | PASS |
| `mknod/09.t` | `mknod/` | 1 | 1/1 | 5 ms | PASS |
| `mknod/10.t` | `mknod/` | 2 | 2/2 | 7 ms | PASS |
| `mknod/11.t` | `mknod/` | 28 | 28/28 | 2,059 ms | PASS |
| `open/00.t` | `open/` | 47 | 47/47 | 3,110 ms | PASS |
| `open/01.t` | `open/` | 22 | 22/22 | 55 ms | PASS |
| `open/02.t` | `open/` | 4 | 4/4 | 42 ms | PASS |
| `open/03.t` | `open/` | 4 | 4/4 | 582 ms | PASS |
| `open/04.t` | `open/` | 4 | 4/4 | 19 ms | PASS |
| `open/05.t` | `open/` | 12 | 12/12 | 38 ms | PASS |
| `open/06.t` | `open/` | 144 | 144/144 | 268 ms | PASS |
| `open/07.t` | `open/` | 25 | 25/25 | 59 ms | PASS |
| `open/08.t` | `open/` | 3 | 3/3 | 17 ms | PASS |
| `open/09.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/10.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/11.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/12.t` | `open/` | 6 | 6/6 | 66 ms | PASS |
| `open/13.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/14.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/15.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/16.t` | `open/` | 6 | 6/6 | 21 ms | PASS |
| `open/17.t` | `open/` | 3 | 3/3 | 13 ms | PASS |
| `open/18.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/19.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/20.t` | `open/` | 1 | 1/1 | 4 ms | PASS |
| `open/21.t` | `open/` | 2 | 2/2 | 7 ms | PASS |
| `open/22.t` | `open/` | 21 | 21/21 | 52 ms | PASS |
| `open/23.t` | `open/` | 5 | 5/5 | 16 ms | PASS |
| `open/24.t` | `open/` | 5 | 5/5 | 17 ms | PASS |
| `open/25.t` | `open/` | 6 | 6/6 | 23 ms | PASS |
| `open/26.t` | `open/` | 9 | 9/9 | 25 ms | PASS |
| `posix_fallocate/00.t` | `posix_fallocate/` | 1 | 1/1 | 4 ms | PASS |
| `rename/00.t` | `rename/` | 122 | 122/122 | 7,290 ms | PASS |
| `rename/01.t` | `rename/` | 8 | 8/8 | 57 ms | PASS |
| `rename/02.t` | `rename/` | 6 | 6/6 | 586 ms | PASS |
| `rename/03.t` | `rename/` | 6 | 6/6 | 27 ms | PASS |
| `rename/04.t` | `rename/` | 18 | 18/18 | 59 ms | PASS |
| `rename/05.t` | `rename/` | 17 | 17/17 | 57 ms | PASS |
| `rename/06.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/07.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/08.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/09.t` | `rename/` | 2353 | 2353/2353 | 5,219 ms | PASS |
| `rename/10.t` | `rename/` | 2099 | 2099/2099 | 4,679 ms | PASS |
| `rename/11.t` | `rename/` | 10 | 10/10 | 93 ms | PASS |
| `rename/12.t` | `rename/` | 32 | 32/32 | 86 ms | PASS |
| `rename/13.t` | `rename/` | 32 | 32/32 | 74 ms | PASS |
| `rename/14.t` | `rename/` | 32 | 32/32 | 76 ms | PASS |
| `rename/15.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/16.t` | `rename/` | 1 | 1/1 | 4 ms | PASS |
| `rename/17.t` | `rename/` | 8 | 8/8 | 21 ms | PASS |
| `rename/18.t` | `rename/` | 6 | 6/6 | 26 ms | PASS |
| `rename/19.t` | `rename/` | 6 | 6/6 | 25 ms | PASS |
| `rename/20.t` | `rename/` | 25 | 25/25 | 72 ms | PASS |
| `rename/21.t` | `rename/` | 16 | 16/16 | 54 ms | PASS |
| `rename/22.t` | `rename/` | 1 | 1/1 | 5 ms | PASS |
| `rename/23.t` | `rename/` | 42 | 42/42 | 5,127 ms | PASS |
| `rename/24.t` | `rename/` | 13 | 13/13 | 46 ms | PASS |
| `rmdir/00.t` | `rmdir/` | 10 | 10/10 | 1,030 ms | PASS |
| `rmdir/01.t` | `rmdir/` | 14 | 14/14 | 42 ms | PASS |
| `rmdir/02.t` | `rmdir/` | 4 | 4/4 | 42 ms | PASS |
| `rmdir/03.t` | `rmdir/` | 5 | 5/5 | 579 ms | PASS |
| `rmdir/04.t` | `rmdir/` | 4 | 4/4 | 18 ms | PASS |
| `rmdir/05.t` | `rmdir/` | 6 | 6/6 | 41 ms | PASS |
| `rmdir/06.t` | `rmdir/` | 23 | 23/23 | 63 ms | PASS |
| `rmdir/07.t` | `rmdir/` | 10 | 10/10 | 35 ms | PASS |
| `rmdir/08.t` | `rmdir/` | 10 | 10/10 | 35 ms | PASS |
| `rmdir/09.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/10.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/11.t` | `rmdir/` | 47 | 47/47 | 109 ms | PASS |
| `rmdir/12.t` | `rmdir/` | 6 | 6/6 | 24 ms | PASS |
| `rmdir/13.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/14.t` | `rmdir/` | 1 | 1/1 | 4 ms | PASS |
| `rmdir/15.t` | `rmdir/` | 2 | 2/2 | 7 ms | PASS |
| `symlink/00.t` | `symlink/` | 14 | 14/14 | 1,041 ms | PASS |
| `symlink/01.t` | `symlink/` | 5 | 5/5 | 22 ms | PASS |
| `symlink/02.t` | `symlink/` | 7 | 7/7 | 52 ms | PASS |
| `symlink/03.t` | `symlink/` | 6 | 6/6 | 585 ms | PASS |
| `symlink/04.t` | `symlink/` | 3 | 3/3 | 17 ms | PASS |
| `symlink/05.t` | `symlink/` | 12 | 12/12 | 41 ms | PASS |
| `symlink/06.t` | `symlink/` | 12 | 12/12 | 41 ms | PASS |
| `symlink/07.t` | `symlink/` | 6 | 6/6 | 48 ms | PASS |
| `symlink/08.t` | `symlink/` | 21 | 21/21 | 52 ms | PASS |
| `symlink/09.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/10.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/11.t` | `symlink/` | 1 | 1/1 | 4 ms | PASS |
| `symlink/12.t` | `symlink/` | 6 | 6/6 | 16 ms | PASS |
| `truncate/00.t` | `truncate/` | 21 | 21/21 | 2,065 ms | PASS |
| `truncate/01.t` | `truncate/` | 5 | 5/5 | 22 ms | PASS |
| `truncate/02.t` | `truncate/` | 5 | 5/5 | 44 ms | PASS |
| `truncate/03.t` | `truncate/` | 5 | 5/5 | 580 ms | PASS |
| `truncate/04.t` | `truncate/` | 4 | 4/4 | 20 ms | PASS |
| `truncate/05.t` | `truncate/` | 15 | 15/15 | 45 ms | PASS |
| `truncate/06.t` | `truncate/` | 8 | 8/8 | 27 ms | PASS |
| `truncate/07.t` | `truncate/` | 6 | 6/6 | 64 ms | PASS |
| `truncate/08.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/09.t` | `truncate/` | 3 | 3/3 | 13 ms | PASS |
| `truncate/10.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/11.t` | `truncate/` | 1 | 1/1 | 4 ms | PASS |
| `truncate/12.t` | `truncate/` | 3 | 3/3 | 15 ms | PASS |
| `truncate/13.t` | `truncate/` | 4 | 4/4 | 15 ms | PASS |
| `truncate/14.t` | `truncate/` | 2 | 2/2 | 8 ms | PASS |
| `unlink/00.t` | `unlink/` | 112 | 112/112 | 17,293 ms | PASS |
| `unlink/01.t` | `unlink/` | 5 | 5/5 | 22 ms | PASS |
| `unlink/02.t` | `unlink/` | 4 | 4/4 | 42 ms | PASS |
| `unlink/03.t` | `unlink/` | 4 | 4/4 | 579 ms | PASS |
| `unlink/04.t` | `unlink/` | 4 | 4/4 | 19 ms | PASS |
| `unlink/05.t` | `unlink/` | 10 | 10/10 | 33 ms | PASS |
| `unlink/06.t` | `unlink/` | 10 | 10/10 | 37 ms | PASS |
| `unlink/07.t` | `unlink/` | 6 | 6/6 | 65 ms | PASS |
| `unlink/08.t` | `unlink/` | 3 | 3/3 | 13 ms | PASS |
| `unlink/09.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/10.t` | `unlink/` | 1 | 1/1 | 4 ms | PASS |
| `unlink/11.t` | `unlink/` | 270 | 270/270 | 581 ms | PASS |
| `unlink/12.t` | `unlink/` | 1 | 1/1 | 5 ms | PASS |
| `unlink/13.t` | `unlink/` | 2 | 2/2 | 7 ms | PASS |
| `unlink/14.t` | `unlink/` | 7 | 7/7 | 30 ms | PASS |
| `utimensat/00.t` | `utimensat/` | 32 | 32/32 | 75 ms | PASS |
| `utimensat/01.t` | `utimensat/` | 7 | 7/7 | 1,027 ms | PASS |
| `utimensat/02.t` | `utimensat/` | 10 | 10/10 | 32 ms | PASS |
| `utimensat/03.t` | `utimensat/` | 1 | 1/1 | 5 ms | PASS |
| `utimensat/04.t` | `utimensat/` | 10 | 10/10 | 30 ms | PASS |
| `utimensat/05.t` | `utimensat/` | 16 | 16/16 | 47 ms | PASS |
| `utimensat/06.t` | `utimensat/` | 13 | 13/13 | 41 ms | PASS |
| `utimensat/07.t` | `utimensat/` | 17 | 17/17 | 47 ms | PASS |
| `utimensat/08.t` | `utimensat/` | 9 | 9/9 | 27 ms | PASS |
| `utimensat/09.t` | `utimensat/` | 7 | 7/7 | 29 ms | PASS |
| **Total** | **17 dirs** | **8,798** | **8798/8798** | **172,854 ms** | **100% PASS (全量通过)** |

---

## 6. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-pjdfstest-kp-20261104-091738`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj91n7p8m56c4bop2m6v`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj91n7p8m56c4bop2m6w`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-04T09:17:45Z` |
| **实例销毁时间 (UTC)** | `2026-11-04T09:23:23Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
