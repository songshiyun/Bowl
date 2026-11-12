# 0020. Linux FUSE 客户端直接转发请求、会话过期自动重建与独立测试容器

## 状态
部分被 [ADR-0029](0029-pebble-per-branch-storage-swmr-and-checkpoint-commit.md)、[ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md) 与 [ADR-0033](0033-client-write-behind-multi-window-readahead-and-local-dir-synthesis.md) 替代 — `bowl mount`/`umount` 守护进程与 Linux 容器测试机制保留，无缓存直通与会话自动重建已演进为单次 `MountDisk` 静态 `Epoch` 握手、本地命名空间缓存与异步读写流水线

## 背景
在 `StorageService` 完成了 38 个 POSIX 元数据接口、磁盘配额管理以及基于 `Chunk-Slice-Block` 的数据读写功能（ADR-0001 至 ADR-0019）后，我们需要开发运行在 Linux 上的 `bowl` 命令行客户端，通过 FUSE 把指定的 `Disk` 挂载成本地目录（并且支持多台机器同时挂载同一块 `Disk`）。在设计客户端时，需要解决以下五个问题：
1. **客户端自己不切块，能否在本地做读写缓存**：像 JuiceFS 这类客户端之所以能在本地硬盘缓存数据块，是因为它的客户端直接拿到全局不变的 `sliceId` 去读写对象存储；而在我们的 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 中，`Slice` 和 `Block` 都由服务端 `StorageService` 内部管理，客户端只按文件的 `(ino, offset)` 调服务端的 `Read` 和 `Write`。在多台客户端共享挂载同一块 `Disk` 的情况下，如果客户端自己在本地缓存文件数据或者攒一批写入再异步发给服务端，不仅容易让其他机器读到旧数据，还会破坏服务端实时检查配额和落盘的机制。
2. **多用户文件权限（`rwx`）在哪里检查**：`StorageService` 虽然保存了每个文件的 `mode`、`uid` 和 `gid`，但像 `Lookup`、`Open`、`Unlink` 这些 RPC 请求并没有传当前操作进程的用户身份，服务端自己不做 Linux 的 `rwx` 权限检查。
3. **网络断连导致客户端会话（`ClientSession`）过期后怎么恢复**：当客户端因为网络中断没能按时续期，服务端会把过期的 `ClientSession` 以及它名下打开的所有文件句柄（`FileHandle`）和文件锁（`FileLock`）全部清掉。等网络恢复后，客户端既不能让整个挂载目录从此卡死不能用，也不能瞒着业务进程偷偷重新打开文件句柄（因为原来的文件锁已经丢了，偷偷重开会导致业务进程在以为自己还持有锁的情况下无锁写坏数据）。
4. **要不要为了支持 `FUSE_TMPFILE` 去改第三方 Go FUSE 库**：官方的 `github.com/hanwen/go-fuse/v2` 库里的 `RawFileSystem` 已经支持了 `Rename2`、`ReadDirPlus`、`FsyncDir`、`Fallocate`、`Xattr` 和 `GetLk/SetLk/SetLkw` 等几乎所有常用操作，唯独没有接 `FUSE_TMPFILE (51)` 回调。我们需要决定值不值得为了这一个低频操作去自己维护一份修改版的 `go-fuse` 库。
5. **如何在非 Linux 开发机（如 macOS）上运行需要内核特权的 FUSE 测试**：客户端只面向 Linux 发布，而且挂载 `/dev/fuse` 需要 `CAP_SYS_ADMIN` 权限。我们既不能要求开发者的 macOS 电脑直接跑 Linux FUSE，也不能给生产环境的 `storage-service` 容器加上危险的特权模式。

## 决策
1. **新建独立的 `client/` 模块，提供 `bowl mount` / `bowl umount` 命令，不用 PID 文件管理后台进程**：
   - 在仓库根目录下新建独立的 Go 模块 `client/`（`module github.com/starsrc/bowl/client`，只针对 Linux 编译、不依赖 CGO，通过本地 `replace` 直接引用根目录 `gen/go` 下的 ConnectRPC 接口代码）。
   - 提供顶层命令 `bowl mount <disk-id> <mountpoint>` 和 `bowl umount <mountpoint>`（同时支持 `unmount` 别名），把 `bowl disk ...` 留给磁盘管理命令使用。客户端通过 `--endpoint` 或环境变量 `BOWL_STORAGE_ENDPOINT`（自动识别 `h2c` 和 `HTTPS`）连接 `StorageService`，并支持用 `--token` 或 `BOWL_TOKEN` 传入认证令牌。
   - **默认转入后台运行，并等挂载成功后再退出前台命令**：执行 `bowl mount` 时，前台进程先检查挂载目录是否存在且没有被重复挂载，再调用 `GetDisk` 拿到根目录 `root_ino` 并向服务端注册初始 `ClientSession`；接着启动后台守护进程去挂载 `/dev/fuse`，前台父进程通过管道（Pipe）等待后台进程通知“挂载成功”（或者拿到具体的失败报错）后才退出（同时也支持加 `-f / --foreground` 在前台运行）。后台进程的日志默认写在 `/var/log/bowl/mount-<disk-id>.log`（如果没权限写 `/var/log`，就写到 `~/.local/state/bowl/logs/mount-<disk-id>.log`）。
   - **通过系统卸载命令通知守护进程退出**：不在本地留容易过期的 PID 文件；执行 `bowl umount`（支持 `-f / --force` 懒卸载）时，依次尝试调用系统的 `fusermount3 -u`、`fusermount -u` 或 `umount`。等 Linux 内核把已打开文件的 `Flush` 和 `Release` / `ReleaseDir` 全部发完并关闭 `/dev/fuse` 后，后台守护进程在收到 `OnUnmount` 事件时停止会话续期并自动退出。
2. **客户端直接转发读写请求，使用内核的 `default_permissions` 检查权限**：
   - **客户端不做本地写缓冲**：第一阶段客户端不在用户态内存或本地磁盘缓存文件数据，所有 FUSE 操作都直接发给 `StorageService`；挂载时关闭内核的异步写回缓存（`writeback_cache`），保证每次写入都同步发到服务端；单次最大写入设为 `MaxWrite = 1 MiB`，当读写请求超过 `4 MiB` 时按 `4 MiB` 切分后调用服务端接口。
   - **使用服务端返回的缓存超时，不强制开启 `Direct IO` 以保证 `mmap` 可用**：客户端把服务端返回的 `entry_timeout_seconds`、`attr_timeout_seconds` 和 `open_flags` 原样传给 FUSE 内核（服务端默认 `OpenKeepCache = false`，使得重新打开文件时会刷新内核页缓存），默认不强制加上 `FOPEN_DIRECT_IO`，从而保证挂载目录里的程序执行和 `mmap` 内存映射能正常工作（另外提供 `--direct-io` 可选参数）。
   - **根目录 `Inode 1` 与 `Disk.root_ino` 互相转换，并开启 `default_permissions`**：在客户端收发 FUSE 请求和返回目录项（包括 `.` 和 `..`）时，自动把内核要求的根目录编号 `1` 和服务端该磁盘真实的 `Disk.root_ino` 互相转换；挂载时默认加上 FUSE 内核的 `default_permissions` 参数，由客户端所在机器的 Linux 内核直接根据调用进程的 `uid / gid` 检查标准 POSIX `rwx` 权限和粘滞位（Sticky Bit），创建文件或目录时把调用方真实的 `uid / gid` 传给服务端，并提供 `--allow-other` 和 `--ro`（只读挂载）参数，同时通过 `Ioctl` 把 `FS_IOC_GETFLAGS` / `FS_IOC_SETFLAGS` 转发给 `GetAttr` / `SetAttr(flags)`。
3. **`ClientSession` 定时续期、过期后自动换新会话、网络闪断有限重试，以及关闭句柄失败时后台重试**：
   - **定时心跳续期与会话过期后换新 ID**：客户端启动时生成一个 63 位随机正整数作为 `session_id` 调用 `RenewSession`，并根据服务端返回的过期时间 `expires_at`，每隔剩余有效期的 `1/3` 时间自动发一次续期心跳。如果断网恢复后服务端返回 `session_recreated == true`（说明原来的会话已经过期被清理了），客户端立刻打印一条告警日志，并切换成一个新的随机 `session_id`：这样后续新打开的文件马上就能正常读写，而断网前打开的老文件描述符（它们在服务端的句柄和文件锁其实已经丢了）再去读写时会如实收到 `EBADF` 错误，绝不偷偷重开句柄掩盖丢锁问题。
   - **提取服务端 POSIX 错误码与最多 5 秒的网络重试**：客户端从 ConnectRPC 错误详情里读取 `google.rpc.ErrorInfo.Reason` 并转成 Linux 的 `syscall.Errno`；只有当请求因为网络闪断根本没发出去，或者是只读/幂等请求遇到连接断开时，才做总时长不超过 `5s` 的退避重试（如果等待期间收到内核的 `cancel` 中断信号则立刻返回 `EINTR`），超过 5 秒还连不上就返回 `EIO`，防止服务端停机时客户端机器上的进程永远卡死。
   - **关闭句柄（`Release` / `ReleaseDir`）失败时放入后台队列重试**：因为 FUSE 的 `Release` 和 `ReleaseDir` 不会把错误码返回给用户进程，如果关闭文件时刚好遇到网络闪断失败，服务端上的文件句柄、`flock` 锁或待回收的孤儿文件就会一直残留。因此客户端在本地删掉句柄记录的同时，如果发现发给服务端的 `Release` / `ReleaseDir` 因为网络断开、超时或中断失败了，就把它放进一个后台重试队列里定期重发（卸载退出时尽力把队列发完）；如果服务端返回 `EBADF` / `ENOENT` / `EINVAL` 说明句柄本来就已经不在了，就直接从队列移除；如果整个 `ClientSession` 都过期换新了，旧会话的所有待关闭句柄在服务端早已被一并清掉，客户端也直接清空旧队列。
4. **直接使用官方 `github.com/hanwen/go-fuse/v2` 库，不自行维护修改版**：
   - 直接使用上游官方的 `github.com/hanwen/go-fuse/v2`，不为了极少用到的匿名 `open(O_TMPFILE)` 去自己维护一份私有的 `go-fuse` 分支（当内核发来 `FUSE_TMPFILE` 时，直接按不支持该操作返回 `EOPNOTSUPP`，常见的临时文件写法如 `mkstemp` 完全不受影响）。
5. **用专门的 Linux 测试容器跑 FUSE 挂载测试**：
   - 保持 [`docker-compose.yaml`](../../docker-compose.yaml) 里的生产 `storage-service` 容器不加任何特权；单独为 `client/` 准备一个装有 Go 1.26 和 `fuse3` 的 Linux 测试容器镜像，用 `task test:client` 跑代码检查和单元测试，用 `task test:client:e2e`（只给这个测试容器加上 `/dev/fuse` 设备和 `CAP_SYS_ADMIN` 权限，并连入 `bowl-net` 网络）在容器里真正挂载目录，测试多挂载点并发读写和文件锁互斥。

## 结果
- **正面效果**：在不破坏 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 服务端统一管数据和配额的前提下，用简单清晰的客户端代码完成了 Linux FUSE 挂载功能；靠内核 `default_permissions` 解决了多用户权限检查；靠会话过期换新 ID 既让挂载点断网恢复后能继续用，又不会掩盖文件锁丢失；靠独立的测试容器让 macOS 开发者也能一键跑完真正的 FUSE 挂载测试。
- **代价与取舍**：由于第一阶段客户端不在本地缓存文件数据块（直到 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 才加上了顺序预读），跨机器重复打开大文件或者系统内存紧张回收了内核页缓存后再次读取时，都需要发请求去服务端读取。
