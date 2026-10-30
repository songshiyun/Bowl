# Bowl 分布式文件系统 — 阿里云硅谷双机内网 `pjdfstest` 基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl** 在阿里云硅谷地域（`us-west-1`）真实开启 2 台按量计费云服务器（1 台 `storage-service` + PostgreSQL 服务端节点，1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行权威 POSIX 文件系统兼容性与基准测试集 **[pjdfstest](https://github.com/pjd/pjdfstest)** 的完整实测结果。

| 核心指标 | 实测结果 |
| :--- | :--- |
| **测试结论** | **100% PASS (全量通过)** |
| **测试系统调用目录数** | `17` 个系统调用族 |
| **测试脚本文件数 (`.t`)** | `238` 个测试脚本 |
| **总测试断言数 (Subtests)** | `8,798` 项 POSIX 断言 |
| **通过 / 失败断言数** | `8,798` 通过 / `0` 失败（通过率 **100.00%**） |
| **全量执行总耗时** | `204.69 s`（约 `3.41 min`） |
| **内网综合执行吞吐率** | `42.98 tests/s`（平均单断言耗时 `23.27 ms`） |
| **云端地域与网络拓扑** | 阿里云 `us-west-1` (`us-west-1a`) 同可用区 VPC 私网直连 |
| **测试执行时间 (UTC)** | `2026-10-30T07:49:22Z` ~ `2026-10-30T07:52:50Z` |

---

## 2. 阿里云硅谷双机测试拓扑与环境基线

### 2.1 双机云服务器配置（按量计费实测节点）

| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| **Server 服务端** (`bowl-bench-server`) | `i-rj9ca4qq1fzee0f4y0vn` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.198:9000` |
| **Client 客户端** (`bowl-bench-client`) | `i-rj9ca4qq1fzee0f4y0vo` | `ecs.c9i.xlarge` | Intel(R) Xeon(R) 6982P-C | 4 vCPU / 7.4 GiB | `cloud_essd` (40 GiB) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | `172.16.1.199` |

### 2.2 同可用区 VPC 内网性能基线（Client <-> Server）

| 网络指标 | 实测数值 | 采集方式 |
| :--- | :--- | :--- |
| **VPC ID / 交换机 ID** | `vpc-rj9gjwyzzh4v0cn576fhu` / `vsw-rj9rsvsgpihtsgxej6me5` | 阿里云 `us-west-1a` 私网网段 `172.16.1.0/24` |
| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `0.043/0.073/0.298/0.055` ms | `ping -c 20 -i 0.05 172.16.1.198` |
| **内网单流 TCP 吞吐带宽** | `15.71` Gbps | `iperf3 -c 172.16.1.198 -t 5` |

### 2.3 软件版本与挂载运行参数

| 组件 / 参数项 | 配置值 | 说明 |
| :--- | :--- | :--- |
| **Bowl Git Commit** | `77685a1` | 仓库当前评测版本 |
| **pjdfstest Git Commit** | `85a8aea` | 上游 `https://github.com/pjd/pjdfstest` |
| **PostgreSQL 版本** | `psql (PostgreSQL) 16.15 (Ubuntu 16.15-0ubuntu0.24.04.1)` | Server 节点本地持久化元数据库（已完成 13 个 Goose 迁移） |
| **FUSE 用户态版本** | `fusermount3 version: 3.14.0` | Linux 内核 FUSE 协议 + `github.com/hanwen/go-fuse/v2` |
| **测试虚拟磁盘 (`DiskID` / `RootIno`)** | `01a18bc8-ea3a-787a-95f9-686f7ada0c72` (`root_ino=2`) | 配额 `2PiB`，验证 `1 <-> root_ino` 非恒等根映射 |
| **服务端 `STORAGE_ATIME_MODE`** | `strictatime` | 开启严格访问时间更新，验证 `atime` 刷新语义 |
| **服务端 FUSE 缓存超时配置** | `entry=0s, attr=0s, direntry=0s` | 设为 `0s` 强制每次 `stat`/`lookup` 穿透内网 RPC 校验服务端真实状态 |
| **客户端挂载命令** | `bowl mount <disk_id> /mnt/bowl --endpoint http://172.16.1.198:9000 --allow-other` | 启用 `default_permissions` 与 `--allow-other` 支持多 UID/GID 权限切换测试 |

---

## 3. 17 个系统调用目录兼容性与耗时汇总

| 系统调用目录 | 脚本数 (Files) | 断言数 (Tests) | PASS / ALL | 通过率 | 耗时 (s) | 吞吐率 (Tests/s) | 耗时占比 | 核心覆盖语义 |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `chflags/` | 14 | 14 | 14/14 | 100.00% | 0.23s | 61.4 | 0.1% | 文件扩展标志位（FS_IOC_GETFLAGS / FS_IOC_SETFLAGS，验证 immutable / append 约束） |
| `chmod/` | 13 | 327 | 327/327 | 100.00% | 15.14s | 21.6 | 7.4% | 权限位变更、SUID/SGID 粘滞位保留与剥离、多身份权限检查 |
| `chown/` | 11 | 1,497 | 1497/1497 | 100.00% | 84.24s | 17.8 | 41.2% | 文件与符号链接属主/属组变更（chown/lchown/fchown）、非特权组约束与 SUID/SGID 清除 |
| `ftruncate/` | 15 | 89 | 89/89 | 100.00% | 3.95s | 22.5 | 1.9% | 基于已打开文件描述符的长度截断与扩展、只读句柄拒绝（EBADF/EINVAL） |
| `granular/` | 7 | 7 | 7/7 | 100.00% | 0.13s | 55.6 | 0.1% | 细粒度访问控制与基础文件操作边界校验 |
| `link/` | 18 | 359 | 359/359 | 100.00% | 13.19s | 27.2 | 6.4% | 硬链接创建、nlink 计数递增、跨目录硬链接与目录硬链接禁止（EPERM） |
| `mkdir/` | 13 | 118 | 118/118 | 100.00% | 2.87s | 41.2 | 1.4% | 目录创建、权限掩码、父目录写/执行权限校验、路径长度与存在性冲突（EEXIST） |
| `mkfifo/` | 13 | 120 | 120/120 | 100.00% | 2.94s | 40.9 | 1.4% | 命名管道（FIFO 特殊文件）创建与权限位验证 |
| `mknod/` | 12 | 186 | 186/186 | 100.00% | 5.35s | 34.8 | 2.6% | 字符设备、块设备、FIFO 与套接字特殊节点创建及权限验证 |
| `open/` | 27 | 337 | 337/337 | 100.00% | 5.91s | 57.1 | 2.9% | 文件打开标志组合（O_CREAT/O_EXCL/O_TRUNC/O_RDONLY/O_WRONLY/O_RDWR/O_NOFOLLOW）与权限矩阵 |
| `posix_fallocate/` | 1 | 1 | 1/1 | 100.00% | 0.06s | 15.9 | 0.0% | 物理空间预分配（fallocate Mode=0）、文件大小扩展与配额联动 |
| `rename/` | 25 | 4,857 | 4857/4857 | 100.00% | 38.62s | 125.8 | 18.9% | 同目录/跨目录原子重命名、覆盖非空目录拦截（ENOTEMPTY/EISDIR/ENOTDIR）及权限矩阵 |
| `rmdir/` | 16 | 145 | 145/145 | 100.00% | 3.14s | 46.1 | 1.5% | 空目录删除、非空目录拒绝（ENOTEMPTY）、当前目录/点目录拒绝与权限检查 |
| `symlink/` | 13 | 95 | 95/95 | 100.00% | 2.82s | 33.7 | 1.4% | 符号链接创建、目标路径长度边界与 readlink 解析 |
| `truncate/` | 15 | 84 | 84/84 | 100.00% | 3.87s | 21.7 | 1.9% | 基于路径的文件截断、稀疏扩展零填充、尺寸变更触发 ctime/mtime 更新 |
| `unlink/` | 15 | 440 | 440/440 | 100.00% | 20.44s | 21.5 | 10.0% | 文件与特殊节点删除、nlink 递减、Open-Unlink 孤儿句柄读写保活与延迟回收 |
| `utimensat/` | 10 | 122 | 122/122 | 100.00% | 1.80s | 67.7 | 0.9% | 纳秒级时间戳更新（UTIME_NOW / UTIME_OMIT / 显式时间戳）与权限校验 |
| **Total (合计)** | **238** | **8,798** | **8798/8798** | **100.00%** | **204.69s** | **43.0** | **100.0%** | **全量 17 个 POSIX 系统调用测试集** |

---

## 4. 内网性能特征与关键 POSIX 语义验证分析

### 4.1 跨机内网 RPC 延迟与吞吐分布剖析

- **内网通信底座**：Client 与 Server 位于阿里云硅谷 `us-west-1a` 同一交换机（`vsw-rj9rsvsgpihtsgxej6me5`），内网 ICMP RTT 延迟仅 `0.043/0.073/0.298/0.055` ms，内网带宽达 `15.71` Gbps。由于测试禁用了客户端内核属性/目录项缓存（`Timeout=0s`），每一次 `lookup`、`getattr`、`create`、`rename`、`unlink`、`setattr` 均真实触发跨机 ConnectRPC (HTTP/2) 请求及服务端 PostgreSQL 事务提交。
- **耗时集中度分析**：耗时排名前三的子集为 `chown/`（1,497 项断言，耗时 84.24s，占比 41.2%）、`rename/`（4,857 项断言，耗时 38.62s，占比 18.9%）、`unlink/`（440 项断言，耗时 20.44s，占比 10.0%）。
  - `rename/` 包含 4,857 项断言（占全量测试断言的 55.2%），密集覆盖文件、目录、符号链接、FIFO 等不同节点类型之间的同目录/跨目录原子替换与错误码（`EISDIR`/`ENOTDIR`/`ENOTEMPTY`/`EACCES`/`EINVAL`）矩阵，每次断言涉及多步父目录权限准备、文件创建、重命名与清理 RPC。
  - `chown/` 包含 1,497 项断言（占全量断言的 17.0%），重点测试特权/非特权用户切换（`setuid`/`setgid`/`setgroups`）下属主属组变更及 `S_ISUID`/`S_ISGID` 位清除逻辑。

### 4.2 关键 POSIX 契约与边界语义验证结论

1. **非恒等根 Inode 映射 (`1 <-> root_ino`)**：
   - 本次评测虚拟磁盘 `root_ino = 2`（大于 1），客户端 `ToServerIno` / `ToKernelIno` 在 FUSE 内核固定根号 `1` 与服务端持久化 `root_ino` 之间完成透明双向映射，根目录权限与 `..` 回溯全部通过验证。
2. **扩展属性标志位 (`chflags` / `FS_IOC_SETFLAGS`)**：
   - `chflags/00.t ~ 13.t` 完整测试了 Linux VFS `FS_IMMUTABLE_FL` 与 `FS_APPEND_FL` 标志位。客户端 `RawFS.Ioctl` 同时兼容 `FS_IOC_GETFLAGS`/`FS_IOC_SETFLAGS` 与内核 `FS_IOC_FSGETXATTR`/`FS_IOC_FSSETXATTR` (fileattr API)，服务端在 `SetAttr`、`Write`、`Unlink`、`Rename`、`Link` 中严格执行不可变（Immutable）与仅追加（Append-Only）保护。
3. **多身份权限裁决 (`--allow-other` + `default_permissions`)**：
   - `pjdfstest` 在测试过程中频繁切换进程有效 UID/GID（如 `65534`），通过 `--allow-other` 与内核 `default_permissions` 配合服务端传入的 Caller `uid`/`gid`/`umask`，精确通过了粘滞位（Sticky Bit `S_ISVTX`）目录删除/重命名限制、父目录写执行权限检查及新建节点属组继承（`S_ISGID` 目录继承）。
4. **Open-Unlink 孤儿文件生命周期与空间预分配**：
   - `unlink/14.t`、`posix_fallocate/00.t`、`truncate/`、`ftruncate/` 验证了打开状态下删除最后一条硬链接后句柄仍可正常读写与截断，以及 `fallocate` 物理空间预分配与配额扣减的一致性。

---

## 5. 全量 238 个测试脚本明细表（Case-by-Case Breakdown）

| 测试脚本 (`Case`) | 所属目录 | 断言数 (`Tests`) | 通过/总数 (`PASS/ALL`) | 耗时 (`ms`) | 状态 |
| :--- | :--- | ---: | ---: | ---: | :--- |
| `chflags/00.t` | `chflags/` | 1 | 1/1 | 12 ms | PASS |
| `chflags/01.t` | `chflags/` | 1 | 1/1 | 12 ms | PASS |
| `chflags/02.t` | `chflags/` | 1 | 1/1 | 10 ms | PASS |
| `chflags/03.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/04.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/05.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/06.t` | `chflags/` | 1 | 1/1 | 10 ms | PASS |
| `chflags/07.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/08.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/09.t` | `chflags/` | 1 | 1/1 | 10 ms | PASS |
| `chflags/10.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/11.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/12.t` | `chflags/` | 1 | 1/1 | 9 ms | PASS |
| `chflags/13.t` | `chflags/` | 1 | 1/1 | 8 ms | PASS |
| `chmod/00.t` | `chmod/` | 119 | 119/119 | 12,559 ms | PASS |
| `chmod/01.t` | `chmod/` | 17 | 17/17 | 98 ms | PASS |
| `chmod/02.t` | `chmod/` | 5 | 5/5 | 58 ms | PASS |
| `chmod/03.t` | `chmod/` | 5 | 5/5 | 1,195 ms | PASS |
| `chmod/04.t` | `chmod/` | 7 | 7/7 | 47 ms | PASS |
| `chmod/05.t` | `chmod/` | 14 | 14/14 | 81 ms | PASS |
| `chmod/06.t` | `chmod/` | 8 | 8/8 | 300 ms | PASS |
| `chmod/07.t` | `chmod/` | 25 | 25/25 | 146 ms | PASS |
| `chmod/08.t` | `chmod/` | 1 | 1/1 | 11 ms | PASS |
| `chmod/09.t` | `chmod/` | 1 | 1/1 | 10 ms | PASS |
| `chmod/10.t` | `chmod/` | 2 | 2/2 | 11 ms | PASS |
| `chmod/11.t` | `chmod/` | 109 | 109/109 | 441 ms | PASS |
| `chmod/12.t` | `chmod/` | 14 | 14/14 | 103 ms | PASS |
| `chown/00.t` | `chown/` | 1280 | 1280/1280 | 81,449 ms | PASS |
| `chown/01.t` | `chown/` | 22 | 22/22 | 106 ms | PASS |
| `chown/02.t` | `chown/` | 10 | 10/10 | 79 ms | PASS |
| `chown/03.t` | `chown/` | 10 | 10/10 | 1,267 ms | PASS |
| `chown/04.t` | `chown/` | 9 | 9/9 | 54 ms | PASS |
| `chown/05.t` | `chown/` | 18 | 18/18 | 100 ms | PASS |
| `chown/06.t` | `chown/` | 10 | 10/10 | 412 ms | PASS |
| `chown/07.t` | `chown/` | 132 | 132/132 | 663 ms | PASS |
| `chown/08.t` | `chown/` | 1 | 1/1 | 9 ms | PASS |
| `chown/09.t` | `chown/` | 1 | 1/1 | 8 ms | PASS |
| `chown/10.t` | `chown/` | 4 | 4/4 | 15 ms | PASS |
| `ftruncate/00.t` | `ftruncate/` | 26 | 26/26 | 2,165 ms | PASS |
| `ftruncate/01.t` | `ftruncate/` | 5 | 5/5 | 37 ms | PASS |
| `ftruncate/02.t` | `ftruncate/` | 5 | 5/5 | 59 ms | PASS |
| `ftruncate/03.t` | `ftruncate/` | 5 | 5/5 | 1,133 ms | PASS |
| `ftruncate/04.t` | `ftruncate/` | 4 | 4/4 | 31 ms | PASS |
| `ftruncate/05.t` | `ftruncate/` | 15 | 15/15 | 87 ms | PASS |
| `ftruncate/06.t` | `ftruncate/` | 8 | 8/8 | 53 ms | PASS |
| `ftruncate/07.t` | `ftruncate/` | 6 | 6/6 | 174 ms | PASS |
| `ftruncate/08.t` | `ftruncate/` | 1 | 1/1 | 9 ms | PASS |
| `ftruncate/09.t` | `ftruncate/` | 3 | 3/3 | 22 ms | PASS |
| `ftruncate/10.t` | `ftruncate/` | 1 | 1/1 | 11 ms | PASS |
| `ftruncate/11.t` | `ftruncate/` | 1 | 1/1 | 9 ms | PASS |
| `ftruncate/12.t` | `ftruncate/` | 3 | 3/3 | 27 ms | PASS |
| `ftruncate/13.t` | `ftruncate/` | 4 | 4/4 | 34 ms | PASS |
| `ftruncate/14.t` | `ftruncate/` | 2 | 2/2 | 15 ms | PASS |
| `granular/00.t` | `granular/` | 1 | 1/1 | 9 ms | PASS |
| `granular/01.t` | `granular/` | 1 | 1/1 | 9 ms | PASS |
| `granular/02.t` | `granular/` | 1 | 1/1 | 10 ms | PASS |
| `granular/03.t` | `granular/` | 1 | 1/1 | 9 ms | PASS |
| `granular/04.t` | `granular/` | 1 | 1/1 | 8 ms | PASS |
| `granular/05.t` | `granular/` | 1 | 1/1 | 9 ms | PASS |
| `granular/06.t` | `granular/` | 1 | 1/1 | 8 ms | PASS |
| `link/00.t` | `link/` | 202 | 202/202 | 10,766 ms | PASS |
| `link/01.t` | `link/` | 32 | 32/32 | 175 ms | PASS |
| `link/02.t` | `link/` | 10 | 10/10 | 85 ms | PASS |
| `link/03.t` | `link/` | 13 | 13/13 | 1,236 ms | PASS |
| `link/04.t` | `link/` | 6 | 6/6 | 42 ms | PASS |
| `link/05.t` | `link/` | 1 | 1/1 | 10 ms | PASS |
| `link/06.t` | `link/` | 18 | 18/18 | 100 ms | PASS |
| `link/07.t` | `link/` | 17 | 17/17 | 106 ms | PASS |
| `link/08.t` | `link/` | 10 | 10/10 | 309 ms | PASS |
| `link/09.t` | `link/` | 5 | 5/5 | 35 ms | PASS |
| `link/10.t` | `link/` | 23 | 23/23 | 101 ms | PASS |
| `link/11.t` | `link/` | 9 | 9/9 | 57 ms | PASS |
| `link/12.t` | `link/` | 1 | 1/1 | 8 ms | PASS |
| `link/13.t` | `link/` | 1 | 1/1 | 8 ms | PASS |
| `link/14.t` | `link/` | 1 | 1/1 | 9 ms | PASS |
| `link/15.t` | `link/` | 1 | 1/1 | 8 ms | PASS |
| `link/16.t` | `link/` | 1 | 1/1 | 8 ms | PASS |
| `link/17.t` | `link/` | 8 | 8/8 | 31 ms | PASS |
| `mkdir/00.t` | `mkdir/` | 36 | 36/36 | 1,151 ms | PASS |
| `mkdir/01.t` | `mkdir/` | 17 | 17/17 | 91 ms | PASS |
| `mkdir/02.t` | `mkdir/` | 3 | 3/3 | 49 ms | PASS |
| `mkdir/03.t` | `mkdir/` | 3 | 3/3 | 1,041 ms | PASS |
| `mkdir/04.t` | `mkdir/` | 3 | 3/3 | 26 ms | PASS |
| `mkdir/05.t` | `mkdir/` | 12 | 12/12 | 69 ms | PASS |
| `mkdir/06.t` | `mkdir/` | 12 | 12/12 | 72 ms | PASS |
| `mkdir/07.t` | `mkdir/` | 6 | 6/6 | 161 ms | PASS |
| `mkdir/08.t` | `mkdir/` | 1 | 1/1 | 9 ms | PASS |
| `mkdir/09.t` | `mkdir/` | 1 | 1/1 | 8 ms | PASS |
| `mkdir/10.t` | `mkdir/` | 21 | 21/21 | 83 ms | PASS |
| `mkdir/11.t` | `mkdir/` | 1 | 1/1 | 9 ms | PASS |
| `mkdir/12.t` | `mkdir/` | 2 | 2/2 | 12 ms | PASS |
| `mkfifo/00.t` | `mkfifo/` | 36 | 36/36 | 1,158 ms | PASS |
| `mkfifo/01.t` | `mkfifo/` | 17 | 17/17 | 97 ms | PASS |
| `mkfifo/02.t` | `mkfifo/` | 4 | 4/4 | 51 ms | PASS |
| `mkfifo/03.t` | `mkfifo/` | 4 | 4/4 | 1,084 ms | PASS |
| `mkfifo/04.t` | `mkfifo/` | 3 | 3/3 | 26 ms | PASS |
| `mkfifo/05.t` | `mkfifo/` | 12 | 12/12 | 73 ms | PASS |
| `mkfifo/06.t` | `mkfifo/` | 12 | 12/12 | 73 ms | PASS |
| `mkfifo/07.t` | `mkfifo/` | 6 | 6/6 | 168 ms | PASS |
| `mkfifo/08.t` | `mkfifo/` | 1 | 1/1 | 9 ms | PASS |
| `mkfifo/09.t` | `mkfifo/` | 21 | 21/21 | 87 ms | PASS |
| `mkfifo/10.t` | `mkfifo/` | 1 | 1/1 | 9 ms | PASS |
| `mkfifo/11.t` | `mkfifo/` | 1 | 1/1 | 9 ms | PASS |
| `mkfifo/12.t` | `mkfifo/` | 2 | 2/2 | 12 ms | PASS |
| `mknod/00.t` | `mknod/` | 36 | 36/36 | 1,160 ms | PASS |
| `mknod/01.t` | `mknod/` | 27 | 27/27 | 127 ms | PASS |
| `mknod/02.t` | `mknod/` | 12 | 12/12 | 79 ms | PASS |
| `mknod/03.t` | `mknod/` | 12 | 12/12 | 1,305 ms | PASS |
| `mknod/04.t` | `mknod/` | 3 | 3/3 | 27 ms | PASS |
| `mknod/05.t` | `mknod/` | 12 | 12/12 | 71 ms | PASS |
| `mknod/06.t` | `mknod/` | 12 | 12/12 | 76 ms | PASS |
| `mknod/07.t` | `mknod/` | 6 | 6/6 | 168 ms | PASS |
| `mknod/08.t` | `mknod/` | 35 | 35/35 | 122 ms | PASS |
| `mknod/09.t` | `mknod/` | 1 | 1/1 | 10 ms | PASS |
| `mknod/10.t` | `mknod/` | 2 | 2/2 | 11 ms | PASS |
| `mknod/11.t` | `mknod/` | 28 | 28/28 | 2,108 ms | PASS |
| `open/00.t` | `open/` | 47 | 47/47 | 3,242 ms | PASS |
| `open/01.t` | `open/` | 22 | 22/22 | 113 ms | PASS |
| `open/02.t` | `open/` | 4 | 4/4 | 52 ms | PASS |
| `open/03.t` | `open/` | 4 | 4/4 | 1,068 ms | PASS |
| `open/04.t` | `open/` | 4 | 4/4 | 29 ms | PASS |
| `open/05.t` | `open/` | 12 | 12/12 | 76 ms | PASS |
| `open/06.t` | `open/` | 144 | 144/144 | 506 ms | PASS |
| `open/07.t` | `open/` | 25 | 25/25 | 111 ms | PASS |
| `open/08.t` | `open/` | 3 | 3/3 | 29 ms | PASS |
| `open/09.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/10.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/11.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/12.t` | `open/` | 6 | 6/6 | 169 ms | PASS |
| `open/13.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/14.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/15.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/16.t` | `open/` | 6 | 6/6 | 32 ms | PASS |
| `open/17.t` | `open/` | 3 | 3/3 | 22 ms | PASS |
| `open/18.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/19.t` | `open/` | 1 | 1/1 | 9 ms | PASS |
| `open/20.t` | `open/` | 1 | 1/1 | 8 ms | PASS |
| `open/21.t` | `open/` | 2 | 2/2 | 12 ms | PASS |
| `open/22.t` | `open/` | 21 | 21/21 | 87 ms | PASS |
| `open/23.t` | `open/` | 5 | 5/5 | 35 ms | PASS |
| `open/24.t` | `open/` | 5 | 5/5 | 28 ms | PASS |
| `open/25.t` | `open/` | 6 | 6/6 | 50 ms | PASS |
| `open/26.t` | `open/` | 9 | 9/9 | 50 ms | PASS |
| `posix_fallocate/00.t` | `posix_fallocate/` | 1 | 1/1 | 9 ms | PASS |
| `rename/00.t` | `rename/` | 122 | 122/122 | 7,527 ms | PASS |
| `rename/01.t` | `rename/` | 8 | 8/8 | 78 ms | PASS |
| `rename/02.t` | `rename/` | 6 | 6/6 | 1,069 ms | PASS |
| `rename/03.t` | `rename/` | 6 | 6/6 | 42 ms | PASS |
| `rename/04.t` | `rename/` | 18 | 18/18 | 105 ms | PASS |
| `rename/05.t` | `rename/` | 17 | 17/17 | 105 ms | PASS |
| `rename/06.t` | `rename/` | 1 | 1/1 | 8 ms | PASS |
| `rename/07.t` | `rename/` | 1 | 1/1 | 8 ms | PASS |
| `rename/08.t` | `rename/` | 1 | 1/1 | 9 ms | PASS |
| `rename/09.t` | `rename/` | 2353 | 2353/2353 | 12,055 ms | PASS |
| `rename/10.t` | `rename/` | 2099 | 2099/2099 | 11,021 ms | PASS |
| `rename/11.t` | `rename/` | 10 | 10/10 | 313 ms | PASS |
| `rename/12.t` | `rename/` | 32 | 32/32 | 171 ms | PASS |
| `rename/13.t` | `rename/` | 32 | 32/32 | 138 ms | PASS |
| `rename/14.t` | `rename/` | 32 | 32/32 | 139 ms | PASS |
| `rename/15.t` | `rename/` | 1 | 1/1 | 9 ms | PASS |
| `rename/16.t` | `rename/` | 1 | 1/1 | 9 ms | PASS |
| `rename/17.t` | `rename/` | 8 | 8/8 | 31 ms | PASS |
| `rename/18.t` | `rename/` | 6 | 6/6 | 45 ms | PASS |
| `rename/19.t` | `rename/` | 6 | 6/6 | 43 ms | PASS |
| `rename/20.t` | `rename/` | 25 | 25/25 | 140 ms | PASS |
| `rename/21.t` | `rename/` | 16 | 16/16 | 111 ms | PASS |
| `rename/22.t` | `rename/` | 1 | 1/1 | 9 ms | PASS |
| `rename/23.t` | `rename/` | 42 | 42/42 | 5,228 ms | PASS |
| `rename/24.t` | `rename/` | 13 | 13/13 | 88 ms | PASS |
| `rmdir/00.t` | `rmdir/` | 10 | 10/10 | 1,059 ms | PASS |
| `rmdir/01.t` | `rmdir/` | 14 | 14/14 | 75 ms | PASS |
| `rmdir/02.t` | `rmdir/` | 4 | 4/4 | 53 ms | PASS |
| `rmdir/03.t` | `rmdir/` | 5 | 5/5 | 1,135 ms | PASS |
| `rmdir/04.t` | `rmdir/` | 4 | 4/4 | 28 ms | PASS |
| `rmdir/05.t` | `rmdir/` | 6 | 6/6 | 150 ms | PASS |
| `rmdir/06.t` | `rmdir/` | 23 | 23/23 | 120 ms | PASS |
| `rmdir/07.t` | `rmdir/` | 10 | 10/10 | 60 ms | PASS |
| `rmdir/08.t` | `rmdir/` | 10 | 10/10 | 65 ms | PASS |
| `rmdir/09.t` | `rmdir/` | 1 | 1/1 | 8 ms | PASS |
| `rmdir/10.t` | `rmdir/` | 1 | 1/1 | 8 ms | PASS |
| `rmdir/11.t` | `rmdir/` | 47 | 47/47 | 220 ms | PASS |
| `rmdir/12.t` | `rmdir/` | 6 | 6/6 | 42 ms | PASS |
| `rmdir/13.t` | `rmdir/` | 1 | 1/1 | 10 ms | PASS |
| `rmdir/14.t` | `rmdir/` | 1 | 1/1 | 9 ms | PASS |
| `rmdir/15.t` | `rmdir/` | 2 | 2/2 | 12 ms | PASS |
| `symlink/00.t` | `symlink/` | 14 | 14/14 | 1,078 ms | PASS |
| `symlink/01.t` | `symlink/` | 5 | 5/5 | 39 ms | PASS |
| `symlink/02.t` | `symlink/` | 7 | 7/7 | 68 ms | PASS |
| `symlink/03.t` | `symlink/` | 6 | 6/6 | 1,086 ms | PASS |
| `symlink/04.t` | `symlink/` | 3 | 3/3 | 26 ms | PASS |
| `symlink/05.t` | `symlink/` | 12 | 12/12 | 68 ms | PASS |
| `symlink/06.t` | `symlink/` | 12 | 12/12 | 71 ms | PASS |
| `symlink/07.t` | `symlink/` | 6 | 6/6 | 167 ms | PASS |
| `symlink/08.t` | `symlink/` | 21 | 21/21 | 90 ms | PASS |
| `symlink/09.t` | `symlink/` | 1 | 1/1 | 8 ms | PASS |
| `symlink/10.t` | `symlink/` | 1 | 1/1 | 10 ms | PASS |
| `symlink/11.t` | `symlink/` | 1 | 1/1 | 9 ms | PASS |
| `symlink/12.t` | `symlink/` | 6 | 6/6 | 20 ms | PASS |
| `truncate/00.t` | `truncate/` | 21 | 21/21 | 2,127 ms | PASS |
| `truncate/01.t` | `truncate/` | 5 | 5/5 | 39 ms | PASS |
| `truncate/02.t` | `truncate/` | 5 | 5/5 | 58 ms | PASS |
| `truncate/03.t` | `truncate/` | 5 | 5/5 | 1,123 ms | PASS |
| `truncate/04.t` | `truncate/` | 4 | 4/4 | 30 ms | PASS |
| `truncate/05.t` | `truncate/` | 15 | 15/15 | 85 ms | PASS |
| `truncate/06.t` | `truncate/` | 8 | 8/8 | 53 ms | PASS |
| `truncate/07.t` | `truncate/` | 6 | 6/6 | 165 ms | PASS |
| `truncate/08.t` | `truncate/` | 1 | 1/1 | 8 ms | PASS |
| `truncate/09.t` | `truncate/` | 3 | 3/3 | 21 ms | PASS |
| `truncate/10.t` | `truncate/` | 1 | 1/1 | 8 ms | PASS |
| `truncate/11.t` | `truncate/` | 1 | 1/1 | 9 ms | PASS |
| `truncate/12.t` | `truncate/` | 3 | 3/3 | 26 ms | PASS |
| `truncate/13.t` | `truncate/` | 4 | 4/4 | 25 ms | PASS |
| `truncate/14.t` | `truncate/` | 2 | 2/2 | 12 ms | PASS |
| `unlink/00.t` | `unlink/` | 112 | 112/112 | 17,530 ms | PASS |
| `unlink/01.t` | `unlink/` | 5 | 5/5 | 42 ms | PASS |
| `unlink/02.t` | `unlink/` | 4 | 4/4 | 52 ms | PASS |
| `unlink/03.t` | `unlink/` | 4 | 4/4 | 1,077 ms | PASS |
| `unlink/04.t` | `unlink/` | 4 | 4/4 | 30 ms | PASS |
| `unlink/05.t` | `unlink/` | 10 | 10/10 | 59 ms | PASS |
| `unlink/06.t` | `unlink/` | 10 | 10/10 | 63 ms | PASS |
| `unlink/07.t` | `unlink/` | 6 | 6/6 | 162 ms | PASS |
| `unlink/08.t` | `unlink/` | 3 | 3/3 | 22 ms | PASS |
| `unlink/09.t` | `unlink/` | 1 | 1/1 | 9 ms | PASS |
| `unlink/10.t` | `unlink/` | 1 | 1/1 | 9 ms | PASS |
| `unlink/11.t` | `unlink/` | 270 | 270/270 | 1,205 ms | PASS |
| `unlink/12.t` | `unlink/` | 1 | 1/1 | 9 ms | PASS |
| `unlink/13.t` | `unlink/` | 2 | 2/2 | 12 ms | PASS |
| `unlink/14.t` | `unlink/` | 7 | 7/7 | 73 ms | PASS |
| `utimensat/00.t` | `utimensat/` | 32 | 32/32 | 147 ms | PASS |
| `utimensat/01.t` | `utimensat/` | 7 | 7/7 | 1,055 ms | PASS |
| `utimensat/02.t` | `utimensat/` | 10 | 10/10 | 64 ms | PASS |
| `utimensat/03.t` | `utimensat/` | 1 | 1/1 | 9 ms | PASS |
| `utimensat/04.t` | `utimensat/` | 10 | 10/10 | 61 ms | PASS |
| `utimensat/05.t` | `utimensat/` | 16 | 16/16 | 87 ms | PASS |
| `utimensat/06.t` | `utimensat/` | 13 | 13/13 | 84 ms | PASS |
| `utimensat/07.t` | `utimensat/` | 17 | 17/17 | 100 ms | PASS |
| `utimensat/08.t` | `utimensat/` | 9 | 9/9 | 54 ms | PASS |
| `utimensat/09.t` | `utimensat/` | 7 | 7/7 | 64 ms | PASS |
| **Total** | **17 dirs** | **8,798** | **8798/8798** | **204,692 ms** | **100% PASS (全量通过)** |

---

## 6. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-pjdfstest-kp-20261030-074520`（已调用 `DeleteKeyPairs` 销毁） |
| **Server 节点实例 ID** | `i-rj9ca4qq1fzee0f4y0vn`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-rj9ca4qq1fzee0f4y0vo`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-10-30T07:45:49Z` |
| **实例销毁时间 (UTC)** | `2026-10-30T07:53:04Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
