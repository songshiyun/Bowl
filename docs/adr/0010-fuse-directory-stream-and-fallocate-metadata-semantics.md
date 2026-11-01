# 0010. FUSE 目录打开与分页读取（OpenDir/ReadDir/ReadDirPlus/ReleaseDir）及 Fallocate 预分配与打洞

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在完成文件打开与关闭（ADR-0009：`Create`、`Open`、`Flush`、`Fsync`、`Release`）后，需要继续实现目录的打开与遍历（`OpenDir`、`ReadDir`、`ReadDirPlus`、`ReleaseDir`）以及文件空间预分配与打洞（`Fallocate`）。在设计这五个接口时，需要确定四个具体问题：
1. **目录句柄 `fh` 是单独建表还是共用 `file_handles` 表**：FUSE 协议在 `FUSE_OPENDIR`、`FUSE_READDIR`、`FUSE_READDIRPLUS` 和 `FUSE_RELEASEDIR` 中同样使用 64 位无符号整数 `fh` 来代表一次打开会话。我们需要决定是给目录单独建一张表，还是直接共用 `file_handles` 表并在代码里按节点是不是目录（`S_IFDIR`）来区分。
2. **`ReadDir` / `ReadDirPlus` 如何生成 `.` 和 `..`、如何计算分页游标 `offset`**：数据库的 `entries` 表只存了真实的子文件和子目录，没有存 `"."` 和 `".."`。我们需要在不改 `entries` 表结构的前提下，给每次目录读取提供递增的 `offset` 分页游标，并且在 `ReadDirPlus` 里既能查出父目录属性，又符合 Linux 内核“不对 `.` 和 `..` 缓存目录项”的要求。
3. **目录被删除但还打开着（`rmdir-whilst-open`）时怎么处理**：ADR-0008 规定 `Rmdir` 删除空目录时会把它的 `nlink` 设为 `0` 并保留 `inodes` 记录。我们需要明确：当这个已被删的目录还被某个句柄打开着时，能不能再次调用 `OpenDir` 打开它、调用 `ReadDir` / `ReadDirPlus` 会返回什么，以及最后一个句柄调用 `ReleaseDir` 关闭时怎么把它彻底从数据库删掉。
4. **`Fallocate` 支持哪些模式标志（`mode`），什么时候改文件大小和时间戳**：Linux 的 `fallocate(2)` 有好几种模式（默认扩展文件大小、`FALLOC_FL_KEEP_SIZE` 保持大小不变、`FALLOC_FL_PUNCH_HOLE` 区间打洞等）。需要明确哪些模式组合合法、什么时候扩大 `size`、什么时候清除 `S_ISUID` / `S_ISGID` 特权位以及什么时候更新 `mtime` 和 `ctime`。

## 决策
1. **目录句柄与文件句柄共用 `file_handles` 表，按是不是目录（`S_IFDIR`）严格区分调用接口**：
   - `OpenDir` 和 `ReleaseDir` 直接共用 ADR-0009 建立的 `file_handles` 表和 `file_handle_id_seq` 序列。
   - 根据目标 `Inode` 的类型严格区分目录操作和文件操作，不准混用：
     - `OpenDir`：要求目标 `Inode` 必须是目录 `S_IFDIR`（不是目录返回 `ErrNotDir` / `ENOTDIR`），而且打开标志必须是只读模式（`flags & O_ACCMODE == O_RDONLY && flags & O_TRUNC == 0`，否则返回 `ErrInvalidArgument` / `EINVAL`）。
     - `ReadDir` / `ReadDirPlus` / `ReleaseDir`：如果目标 `Inode` 不是目录，统一返回 `ErrNotDir`（`ENOTDIR`）；如果是目录但 `(fh, ino)` 在表里找不到或对不上，返回 `ErrBadFileHandle`（`EBADF`）。反过来，文件接口（`Release` / `Fallocate`）如果收到目录节点，一律返回 `ErrIsDir`（`EISDIR`）。
2. **`ReadDir` / `ReadDirPlus` 按固定游标编号（`1` / `2` / `>=3`）返回 `.`、`..` 和真实子项**：
   - `ReadDir` 和 `ReadDirPlus` 全程在 `RepeatableRead` 只读事务中执行，遵循 `noatime` 策略不修改目录的 `atime`。
   - 游标 `offset` 的含义规定如下：
     - `offset = 1`：固定返回合成的 `"."`，指向当前目录自己的 `Inode`。
     - `offset = 2`：固定返回合成的 `".."`，通过 `GetParentInode(dir_ino)` 查询当前目录的父目录 `Inode`；如果查不到（比如已经是顶层根目录），就指向当前目录自己的 `Inode`。
     - `offset >= 3`：对应 `entries JOIN inodes` 按文件名升序（`ORDER BY e.name ASC`）查出来的真实子项，第 $k$ 个真实子项（从 $0$ 开始数）返回的下一个游标为 `next_offset = k + 3`（对应的 SQL 分页偏移量为 `OFFSET max(0, req.offset - 2)`）。
   - 在 `ReadDirPlus` 中，合成的 `"."` 和 `".."` 也会填上完整的 `Inode` 属性，但把它们的 `entry_timeout_seconds` 明确设为 `0`（因为 Linux FUSE 内核规定不对 `.` 和 `..` 缓存 dentry）；而真实子项则根据它是子目录还是文件，分别填上目录缓存时间（`dirEntryTimeout`）或文件缓存时间（`entryTimeout`）以及属性缓存时间（`attrTimeout`）。
3. **已被删除但仍打开的孤儿目录（`nlink == 0`）：允许继续打开、读取返回空列表、最后一个句柄关闭时彻底删除**：
   - `OpenDir`：在写事务里对目标目录调用 `GetInodeForUpdate(ino)` 加行锁；如果发现目录的 `nlink == 0`，只有当 `HasFileHandlesByIno(ino)` 为 `true`（说明还有别的未关闭句柄正打开着这个目录）时才允许分配新句柄，否则返回 `ErrNotFound`（`ENOENT`）；当开启了 `WithOpenDirCache(true)` 配置时，在返回结果的 `OpenFlags` 中加上 `FOPEN_CACHE_DIR`。
   - `ReadDir` / `ReadDirPlus`：检查完目录存在且 `(fh, ino)` 有效后，如果发现目录的 `nlink == 0`（已经被 `Rmdir` 删掉了），按照 POSIX `readdir(3)` 标准直接返回空列表（连 `"."` 和 `".."` 都不返回）。
   - `ReleaseDir`：在写事务里调用 `DeleteFileHandle(fh, ino)` 删掉当前句柄；如果目录的 `nlink == 0` 且删完后 `HasFileHandlesByIno(ino)` 变成了 `false`（没有任何打开句柄了），就在同一个事务里调用 `DeleteInode(ino)` 把这条孤儿目录记录彻底从数据库删掉。
4. **`Fallocate` 的支持模式、写权限检查与属性更新规则**：
   - 新增业务错误 `ErrNotSupported`（对应 gRPC `Unimplemented` / POSIX `EOPNOTSUPP`），并定义常量：`FALLOC_FL_KEEP_SIZE = 0x01`、`FALLOC_FL_PUNCH_HOLE = 0x02`。
   - 在写事务里调用 `GetInodeForUpdate(ino)` 锁住节点并检查 `GetFileHandle(fh, ino)`：如果句柄不存在，或者句柄是以只读模式打开的（`fh.Flags & O_ACCMODE == O_RDONLY`），都返回 `ErrBadFileHandle`（`EBADF`）；如果目标是目录返回 `ErrIsDir`（`EISDIR`），如果不是常规文件（非 `S_IFREG`）返回 `ErrNotSupported`（`EOPNOTSUPP`）。
   - 不同 `mode` 的处理规则：
     - `mode == 0`（默认预分配空间）：如果 `offset + length > current.Size`，就把文件的 `size` 扩大为 `offset + length`，并同时更新 `mtime` 和 `ctime`（按照 POSIX `posix_fallocate` 规范，预分配空间不清除 `S_ISUID` / `S_ISGID` 位）；如果 `offset + length <= current.Size`，保持 `size` 不变，只更新 `ctime`。
     - `mode == FALLOC_FL_KEEP_SIZE`：保持 `size` 不变，只更新 `ctime`。
     - `mode == FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE`（区间打洞清零）：保持 `size` 不变，清除常规文件的 `S_ISUID` 和 `S_ISGID` 特权位，并同时更新 `mtime` 和 `ctime`（在接入 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 数据层后，同时在该区间追加一条 `id = 0` 的空洞 `Slice` 记录）。
     - 如果只传了 `FALLOC_FL_PUNCH_HOLE` 却没带 `FALLOC_FL_KEEP_SIZE`，返回 `ErrInvalidArgument`（`EINVAL`）；传入任何其他不支持的标志位，返回 `ErrNotSupported`（`EOPNOTSUPP`）。

## 结果
- **正面效果**：不用新建额外的数据库表，就支持了目录打开、带属性预取的分页遍历、孤儿目录回收以及 `Fallocate` 预分配和打洞。
- **代价与取舍**：`ReadDir` / `ReadDirPlus` 用 `ORDER BY name ASC` 加 `OFFSET` 实现分页，当单个目录下有成千上万个文件且翻到很后面的页时会有扫描开销，但对绝大多数常规目录来说简单直接且完全够用。
