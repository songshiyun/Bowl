# 0013. 支持 Rename2（禁止覆盖与交换）、TmpFile 匿名临时文件、FsyncDir 与客户端掉线后孤儿文件自动回收

## 状态
已采纳 (accepted) — 演进 [ADR-0008](0008-fuse-directory-topology-locking-and-orphan-lifecycle.md)、[ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0010](0010-fuse-directory-stream-and-fallocate-metadata-semantics.md) 与 [ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)

## 背景
Storage 元数据引擎在完成目录增删改（ADR-0008）、文件打开关闭（ADR-0009）、目录遍历（ADR-0010）、扩展属性（ADR-0011）和文件锁及会话租约（ADR-0012）后，对照 Linux FUSE 协议，还需要补齐最后四项元数据功能：
1. **支持 `FUSE_RENAME2 (45)` 的禁止覆盖（`RENAME_NOREPLACE`）与双向交换（`RENAME_EXCHANGE`）**：之前的 `Rename` 只支持 `flags == 0` 的默认覆盖模式（ADR-0008），还不支持 Linux `renameat2(2)` 的 `RENAME_NOREPLACE`（目标已存在时报错不覆盖）和 `RENAME_EXCHANGE`（把两个路径指向的文件或目录互换）。而且这两种模式在“源和目标是同一个路径或同一个文件的不同硬链接”以及“目录能不能套进自己子目录”的判断上，跟普通 `Rename` 不一样。
2. **支持 `FUSE_TMPFILE (51)` 创建无名临时文件，并允许通过 `Link` 转成正式文件**：很多编译器、包管理器和数据库会用 `open(O_TMPFILE)` 先创建一个没有文件名的临时文件，等数据写完后再调用 `linkat`（触发 `FUSE_LINK`）一次性给它挂上正式文件名。如果用“先建一个假名字的文件再马上删掉（`Create + Unlink`）”来凑合，不仅要发两次请求，还会白白改动父目录的修改时间。同时，我们要允许没带 `O_EXCL` 的 `TmpFile` 通过 `Link` 挂上文件名，但绝不能让普通文件被 `Unlink` 删掉后还能通过 `Link` 重新活过来（演进 ADR-0008）。
3. **客户端崩溃掉线后，它打开着的孤儿节点（`Orphan Inode`）没人关闭怎么清理**：ADR-0012 已经用 `client_sessions` 会话表解决了客户端死机后文件锁的自动清理，但 ADR-0009 的 `file_handles` 表还没有关联 `session_id`。如果客户端删除了一个正在打开的文件（或者创建了 `TmpFile`），还没来得及发 `Release` / `ReleaseDir` 关闭句柄就断网死机了，这个孤儿节点就会永远留在数据库里删不掉；另外，在其他请求里顺带清理过期会话留下的孤儿节点时，还要避免因为锁多个 `Inode` 的顺序不一致引发数据库死锁。
4. **支持 `FUSE_FSYNCDIR (30)` 同步目录句柄**：程序在修改目录内容后经常会对目录描述符调用 `fsync(2)`，需要在目录句柄功能（ADR-0010）中补上跟文件 `Fsync` 对应的 `FsyncDir` 接口。

## 决策
1. **在 `Rename` 接口中增加 `flags` 参数，支持 `RENAME_NOREPLACE` 和 `RENAME_EXCHANGE`**：
   - **参数定义与检查**：不新建单独的 RPC 接口，直接在 `RenameRequest` 和 `meta.RenameParams` 中加上 `uint32 flags = 5`。定义常量：`RenameNoReplace = 0x1`（`1 << 0`）、`RenameExchange = 0x2`（`1 << 1`）、`RenameWhiteout = 0x4`（`1 << 2`）。如果同时传了 `RenameNoReplace | RenameExchange` 或者传了不认识的标志位，直接返回 `ErrInvalidArgument`（`EINVAL`）；如果传了 `RenameWhiteout (0x4)`，返回不支持 `ErrNotSupported`（`EOPNOTSUPP`）。
   - **当源和目标是同一个路径或同一个 `Inode` 的硬链接时怎么处理（演进 ADR-0008）**：
     - 当 `flags == RenameNoReplace`（禁止覆盖）时：只要源目录项和目标目录项都存在——**哪怕源和目标是同一个路径（`old_parent_ino == new_parent_ino && old_name == new_name`），或者指向同一个 `Inode`（`old_ino == new_ino`）**——都一律返回 `ErrAlreadyExists`（`EEXIST`），不能像普通 `Rename` 那样直接返回成功。
     - 当 `flags == RenameExchange`（双向交换）时：要求源目录项和目标目录项都必须存在，只要有一边不存在就返回 `ErrNotFound`（`ENOENT`）；如果两边都存在，且是同一个路径或指向同一个 `Inode`，则什么都不用改，直接返回成功。
   - **`RenameExchange` 交换两个不同节点的执行步骤**：
     - **按 `ino` 从小到大加锁**：先查出源和目标对应的节点 ID（如果任一不存在，锁住并检查新旧父目录有效后返回 `ErrNotFound`），把 `old_parent_ino`、`new_parent_ino`、`old_ino`、`new_ino` 去重后按 `ino` 从小到大依次加 `FOR UPDATE` 行锁，加锁后再核对一次目录项有没有被别的并发请求改掉。
     - **双向检查目录死循环**：当跨目录交换（`old_parent_ino != new_parent_ino`）时，如果 `old_ino` 是目录，要检查 `new_parent_ino` 不能是 `old_ino` 或者它的子孙目录；**同时如果 `new_ino` 也是目录，还要反过来检查 `old_parent_ino` 不能是 `new_ino` 或者它的子孙目录**，只要有一边会把目录套进自己的子目录里，就立刻拒绝并返回 `ErrInvalidArgument`（`EINVAL`）。
     - **互换 `entries` 记录并更新时间戳、父目录 `nlink` 和 `parent_ino`**：交换模式不限制两边的类型（可以把文件和目录互换，也不要求目录为空）；直接把 `(old_parent_ino, old_name)` 指向的节点改成 `new_ino`，把 `(new_parent_ino, new_name)` 指向的节点改成 `old_ino`；同时更新 `old_ino` 和 `new_ino` 的 `ctime`（如果被跨目录交换的是目录，同步更新它在 `inodes` 表里的 `parent_ino` 字段，两个节点自己的 `nlink` 不变）。如果在同一个父目录下交换，或者跨目录但两边都是目录/都不是目录，新旧父目录的 `nlink` 都不变，只更新父目录的 `mtime` 和 `ctime`；如果跨目录交换且一边是目录、另一边不是目录，那么迁入目录的那个父目录需要先检查 `nlink < MaxNlink`（超出返回 `ErrTooManyLinks`），然后迁出目录的父目录 `nlink - 1`、迁入目录的父目录 `nlink + 1`，并更新两个父目录的 `mtime` 和 `ctime`。

2. **新增 `TmpFile`（`FUSE_TMPFILE = 51`）接口直接创建 `nlink = 0` 的临时文件，并允许通过 `Link` 转正一次（演进 ADR-0008）**：
   - **独立 `TmpFile` 接口与参数要求**：新增 `TmpFile(TmpFileRequest) returns (TmpFileResponse)`（请求包含 `parent_ino`、`mode`、`uid`、`gid`、`flags`、`optional session_id`；返回结构与 `CreateResponse` 一样）。要求打开模式必须是可写的 `O_WRONLY` 或 `O_RDWR`（传只读 `O_RDONLY` 返回 `ErrInvalidArgument` / `EINVAL`）；文件类型只允许传 `0` 或 `S_IFREG`，统一存为 `S_IFREG | (mode & 0o7777)`。
   - **禁止在 `Create` 和 `Open` 中传入 `O_TMPFILE` 标志**：`Create` 和 `Open` 一旦看到 `flags` 里带有 `O_TMPFILE (0o20000000)` 标志，直接报错返回 `ErrInvalidArgument`（`EINVAL`）。这样能保证数据库 `file_handles.flags` 里的 `O_TMPFILE` 标志只可能来自真正的 `TmpFile` 接口，防止普通文件伪造这个标志在被删除后还能用 `Link` 恢复，也防止带 `O_EXCL` 的临时文件通过重新 `Open` 绕过不可链接的限制。
   - **在一个事务里直接创建 `nlink = 0` 的文件并分配句柄**：在读写事务中锁住父目录 `parent_ino`（检查父目录存在、是目录且 `nlink > 0`；如果父目录有 `S_ISGID` 位则继承父目录的 `gid`），直接在 `inodes` 表里插入一条 `nlink = 0` 的常规文件记录（**不往 `entries` 表写目录项，也不改动父目录的 `mtime` / `ctime`**），并在 `file_handles` 表分配一个打开句柄（存库时自动给 `flags` 加上 `O_TMPFILE = 0o20000000`）。
   - **允许通过 `Link` 给 `TmpFile` 挂上文件名，但只能转正一次**：当 `Link` 锁住源 `Inode` 发现它的 `nlink == 0` 时，去查这个 `Inode` 有没有未过期的打开句柄同时满足 `(flags & O_TMPFILE) != 0 && (flags & O_EXCL) == 0`：
     - 如果没有（比如它是被 `Unlink` 删掉的普通文件、创建时带了禁止链接的 `O_EXCL`、或者句柄已经关闭/会话已过期），就照常返回 `ErrNotFound`（`ENOENT`）；
     - 如果有，就允许在 `entries` 表插入新文件名、把源 `Inode` 的 `nlink` 加为 `1` 并更新 `ctime`，**同时在同一个事务里把这个文件所有打开句柄上的 `O_TMPFILE` 标志抹掉（`flags = flags & ~O_TMPFILE`）**。这样一旦它转成正式文件后又被 `Unlink` 删掉变回 `nlink == 0`，就不能再通过 `Link` 二次恢复了。

3. **`file_handles` 表绑定 `session_id`、提供 `RenewSession` 续期接口，并用 `FOR UPDATE SKIP LOCKED` 无死锁清理孤儿节点（演进 ADR-0009 / ADR-0012）**：
   - **数据库表加字段并保持兼容**：给 `file_handles` 表增加可空外键列 `session_id BIGINT REFERENCES client_sessions(session_id) ON DELETE CASCADE` 和索引 `idx_file_handles_session_id ON file_handles(session_id)`。在 `Create`、`TmpFile`、`Open`、`OpenDir`、`Fsync`、`FsyncDir`、`ReadDir`、`ReadDirPlus`、`ReleaseDir`、`Fallocate` 请求中加上 `optional uint64 session_id`（不传时存为 `NULL`，兼容老的测试调用）。
   - **打开句柄与调用时的 `session_id` 必须严格一致**：`GetFileHandle` 和 `DeleteFileHandle` 要求请求传入的 `req_session_id` 必须和句柄创建时保存的 `fh.session_id` 一致：传了 `session_id` 时，必须等于句柄的 `session_id` 且该会话还没过期（`client_sessions.expires_at > clock_timestamp()`）；没传 `session_id`（`NULL`）时，只允许操作同样没绑定会话（`fh.session_id IS NULL`）的句柄。任何跨会话乱用句柄或会话已过期的请求，一律返回 `ErrBadFileHandle`（`EBADF`）。
   - **先校验句柄没过期、再给会话续期，并提供专门的 `RenewSession` 接口**：
     - 新建句柄的接口（`Create`、`TmpFile`、`Open`、`OpenDir`）带了 `session_id` 时，先调用 `renewClientSession` 确保会话存在并续期，再插入 `file_handles`；
     - 使用已有句柄的写接口（`Flush`、`Release`、`ReleaseDir`、`Fallocate`、`SetLk`、`SetLkw`）带了 `session_id` 时，**必须先调 `GetFileHandle` / `DeleteFileHandle` 确认句柄存在且所属会话还没过期，检查通过后才能调 `renewClientSession` 续期**，防止已经过期的会话靠操作作废句柄把自己重新续活；
     - 只读接口（`Fsync`、`FsyncDir`、`ReadDir`、`ReadDirPlus`、`GetLk`）只检查句柄归属和会话未过期，不写数据库续期；
     - 新增专门的 `RenewSession(RenewSessionRequest) returns (RenewSessionResponse)` 接口，让长时间只读或空闲的客户端定时调用来延长会话有效期，并顺带清理过期会话和孤儿节点。
   - **用 `FOR UPDATE SKIP LOCKED` 跳过正被锁住的孤儿节点，避免死锁**：因为调用 `renewClientSession` 时当前事务可能已经锁住了某个 `Inode`，所以在删掉过期的 `client_sessions`（数据库外键会自动级联删掉它名下的 `file_handles`）后，执行 `SELECT i.ino FROM inodes i WHERE i.nlink = 0 AND NOT EXISTS (...) FOR UPDATE SKIP LOCKED` 来查找并删除所有已经没有有效句柄的孤儿 `Inode`。如果某个孤儿节点刚好被别的并发事务锁住，就先跳过留给下次清理，从而彻底避免多个事务互相等待对方手里的 `Inode` 锁造成数据库死锁（SQLSTATE `40P01`）。

4. **新增 `FUSE_FSYNCDIR (30)` 目录同步接口**：
   - 新增 `FsyncDir(FsyncDirRequest) returns (FsyncDirResponse)` 接口（包含 `uint64 ino`、`uint64 fh`、`bool datasync`、`optional uint64 session_id`）。
   - 先查 `GetInode(ino)`（不存在返回 `ErrNotFound` / `ENOENT`；不是目录返回 `ErrNotDir` / `ENOTDIR`），再查 `GetFileHandle(fh, ino)`（句柄不存在或会话已过期返回 `ErrBadFileHandle` / `EBADF`），检查通过后返回成功（注：后续在 [ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md) 中为 `FsyncDir` 加上了真正的硬盘刷盘等待）。

## 结果
- **正面效果**：在不破坏原有接口和测试用例的前提下，补齐了 `FUSE_RENAME2 (45)`、`FUSE_TMPFILE (51)`、`FUSE_FSYNCDIR (30)` 以及客户端死机后孤儿文件的自动清理；通过 `Link` 转正时抹掉 `O_TMPFILE` 标志和 `FOR UPDATE SKIP LOCKED` 跳过已锁节点，既保证了已被删的普通文件不会被非法恢复，又避免了并发清理死锁。
- **代价与取舍**：查询句柄时多了一次对 `client_sessions` 表的主键关联查询，续期时多了一次对 `nlink = 0` 孤儿节点的扫描；配合 `inodes(ino) WHERE nlink = 0` 的部分索引，扫描范围只限于极少数孤儿节点，开销很小。
