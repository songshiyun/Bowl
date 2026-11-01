# 0009. FUSE 文件打开句柄（FileHandle）数据库表、打开时截断文件与孤儿文件回收

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成目录树增删改查（`Lookup`、`GetAttr`、`SetAttr`、`Mknod`、`Mkdir`、`Symlink`、`Readlink`、`Link`、`Unlink`、`Rmdir`、`Rename`）后，需要实现文件打开到关闭的五个接口：`Create`、`Open`、`Flush`、`Fsync` 和 `Release`。在按照 POSIX 和 Linux FUSE 协议实现这些接口时，需要决定四个问题：
1. **打开文件的句柄（`FileHandle` / `fh`）存在哪里**：FUSE 协议在 `Open` 和 `Create` 时向内核返回一个 64 位无符号整数 `fh`，并在之后的 `Flush`、`Fsync`、`Release`（以及读写和加锁）请求里带上 `(ino, fh)`。如果服务端随便返回一个固定的数字或者只存在进程内存里，当多个进程打开同一个文件、或者服务端进程重启后，就没法准确知道一个文件还有多少个句柄正打开着，也没法拦住已经关闭或乱传的非法 `fh`（返回 `EBADF`）。
2. **文件被删除但还打开着（`unlink-whilst-open`）时，什么时候真正从数据库删掉**：ADR-0004 和 ADR-0008 规定，`Unlink` 和 `Rename` 覆盖删除文件时，如果它的 `nlink` 变成了 `0`，先保留 `inodes` 表里的记录，以便正在打开它的进程还能继续调用 `fstat` 和读写。现在我们需要在最后一个 `FileHandle` 调用 `Release` 关闭时，把这个 `nlink == 0` 的孤儿节点（`Orphan Inode`）真正从数据库里删掉；同时还要明确：一个 `nlink == 0` 但还没关完句柄的文件（比如通过 `/proc/self/fd` 访问），还能不能再次调用 `Open` 打开。
3. **不同操作系统的 `open(2)` 标志位数值不一样，且 `Open(O_TRUNC)` 需要同时清空文件**：像 Linux 和 macOS 标准库里的 `O_CREAT`、`O_TRUNC` 常量数值是不一样的，不能直接用服务端运行机器上的 `syscall.O_*`；另外 `Open` 带上 `O_TRUNC` 时需要在服务端直接把文件大小清零、清除特权位，并正确区分目录（返回 `EISDIR`）、符号链接（返回 `EINVAL`）和特殊设备文件的打开行为。
4. **`Flush` 和 `Fsync` 不要随便锁死 `Inode` 行**：同一个 `FileHandle` 因为进程调用 `dup(2)` 或 `fork(2)`，可能会触发好几次 `Flush`（每次关闭一个文件描述符都会调一次 `Flush`，但不会销毁句柄 `fh`）。如果在只做检查的 `Flush` 和 `Fsync` 里也去给 `inodes` 表加排他锁，就会白白卡住其他并发读取请求。

## 决策
1. **在数据库里新建 `file_handles` 表保存打开的句柄**：
   - 新建自增序列 `file_handle_id_seq`（从 `1` 开始，保证合法的 `fh > 0`）和 `file_handles` 表（包含主键 `fh BIGINT`、`ino BIGINT NOT NULL REFERENCES inodes(ino) ON DELETE CASCADE`、`flags INTEGER NOT NULL`、`created_at TIMESTAMPTZ`），并建好索引 `idx_file_handles_ino ON file_handles(ino)`。
   - 新增业务错误 `ErrBadFileHandle`（对应 gRPC `FailedPrecondition` / POSIX `EBADF`）：当目标 `Inode` 存在，但传入的 `fh` 在 `file_handles` 表里找不到，或者不属于这个 `ino` 时，统一返回 `ErrBadFileHandle`，跟文件本身不存在的 `ErrNotFound`（`ENOENT`）明确区分开。
2. **固定使用 Linux 的打开标志位数值，并在同一个事务里完成 `Create` 和 `Open`**：
   - 在 `meta` 包里明确定义固定的 Linux FUSE / POSIX 打开标志常量（`O_ACCMODE = 0o3`、`O_RDONLY = 0o0`、`O_WRONLY = 0o1`、`O_RDWR = 0o2`、`O_CREAT = 0o100`、`O_EXCL = 0o200`、`O_TRUNC = 0o1000`、`O_APPEND = 0o2000`），绝不使用宿主机操作系统的 `syscall.O_*`。
   - `Create`（创建并打开文件）：在写事务里对父目录 `parent_ino` 加 `FOR UPDATE` 行锁，检查父节点存在、是目录且 `nlink > 0`；检查 `mode & S_IFMT` 只能是 `0` 或 `S_IFREG`，结合 `Umask` 算出最终权限 `S_IFREG | ((mode & 0o7777) &^ (umask & 0o7777))`（如果父目录有 `S_ISGID` 位则继承父目录的 `gid`）；在同一个事务里依次执行 `CreateInode`（`parent_ino = 0`）、`CreateEntry`（重名或传了 `"."`/`".."` 返回 `ErrAlreadyExists`）、更新父目录时间戳 `TouchInodeTimestamps(parent_ino)` 以及插入句柄 `CreateFileHandle`，最后返回新建的 `Inode`、`fh`、`open_flags`（如果开启了 `WithOpenKeepCache(true)` 则加上 `FOPEN_KEEP_CACHE`）和缓存超时时间。
   - `Open`（打开已有文件）：在写事务里对目标 `ino` 加 `FOR UPDATE` 行锁；如果目标是目录（`S_IFDIR`）返回 `ErrIsDir`（`EISDIR`），如果是符号链接（`S_IFLNK`）返回 `ErrInvalidArgument`（`EINVAL`）；如果发现 `nlink == 0`，只有当 `HasFileHandlesByIno(ino)` 为 `true`（说明此时还有别的句柄正打开着它）时才允许再次打开，否则返回 `ErrNotFound`（`ENOENT`）；如果 `flags` 带有 `O_TRUNC` 清空标志，在只读模式（`O_RDONLY`）下直接报错返回 `ErrInvalidArgument`，在可写模式下且目标是常规文件（`S_IFREG`）时，把文件 `size` 设为 `0`、清除 `S_ISUID` / `S_ISGID` 特权位、保留原有的 `inodes.flags` 并更新 `mtime` 和 `ctime`（如果是设备或管道等特殊文件则忽略 `O_TRUNC`）；最后往 `file_handles` 表插入一条新句柄并返回 `fh` 和 `open_flags`。
3. **`Flush` 与 `Fsync` 先检查节点和句柄是否有效**：
   - `Flush` 和 `Fsync` 依次检查 `GetInode(ino)`（不存在返回 `ErrNotFound`）和 `GetFileHandle(fh, ino)`（句柄不存在返回 `ErrBadFileHandle`）。检查通过后返回成功，`Flush` 不删掉 `FileHandle`（注：后续在 [ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md) 中为带 `lock_owner` 的 `Flush` 加上了释放 POSIX 文件锁的逻辑，在 [ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md) 中为 `Fsync` 加上了等待硬盘刷盘完成的逻辑）。
4. **`Release` 删掉文件句柄，并在没有剩余句柄时彻底删掉 `nlink == 0` 的孤儿文件**：
   - `Release` 在写事务里删掉指定的句柄 `DeleteFileHandle(fh, ino)`（如果节点不存在返回 `ErrNotFound`，如果句柄不存在返回 `ErrBadFileHandle`）。
   - 如果目标文件的 `nlink == 0`，在同一个事务里检查 `HasFileHandlesByIno(ino)`：只要发现这个文件已经没有任何打开的句柄了，就调用 `DeleteInode(ino)` 把这条孤儿 `Inode` 记录从数据库里彻底删掉。

## 结果
- **正面效果**：用 `file_handles` 表把「打开文件 -> 删除目录项（变成 `nlink == 0` 的孤儿文件） -> 继续读写/查属性 -> 关闭最后一个句柄 -> 彻底从数据库删除」的整个过程串了起来，不会出现并发关闭时漏删或误删文件的问题，而且在 macOS 和 Linux 上跑测试时打开标志位的含义完全一致。
- **代价与取舍**：每次 `Open`、`Create` 和 `Release` 都要在 `file_handles` 表里插入或删除一行记录，不过都是按主键或索引操作，开销很小。
