# 0006. FUSE Mknod 创建文件节点、父目录加锁检查与权限继承

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务在完成 `Lookup`、`GetAttr` 和 `SetAttr` 后，需要实现对接 Linux FUSE 内核 `FUSE_MKNOD` 协议和 POSIX `mknod(2)` 的 `Mknod` 接口，用来在指定父目录下创建非目录文件节点并绑定同名目录项（`Entry`）。在实现这个接口时，需要处理四个具体问题：
1. **允许创建哪些类型的文件，以及设备号 `rdev` 和 `umask` 怎么处理**：在 POSIX 和 FUSE 中，创建目录（`S_IFDIR`）和符号链接（`S_IFLNK`）有各自专门的 `Mkdir` 和 `Symlink` 接口，不能通过 `Mknod` 创建；而且设备号 `rdev` 只对字符设备（`S_IFCHR`）和块设备（`S_IFBLK`）有意义；另外 FUSE 请求传过来的 `umask` 掩码需要在计算最终权限位时扣掉。
2. **文件重名以及传入 `.` 和 `..` 时报什么错**：POSIX `mknod(2)` 规定，当要创建的名字已经存在（包括每个目录天生就有的 `.` 和 `..`）时，必须返回 `EEXIST`；而且这个判断必须在确认父节点存在且确实是个目录之后再做。
3. **并发写入时怎么防止父目录刚好被删掉，以及怎么继承父目录属组**：`Mknod` 要同时往 `inodes` 和 `entries` 两张表写数据。并发操作时，必须防止父目录在创建子文件的同一瞬间被别的请求删掉（`rmdir`），禁止往已经被删掉的孤儿目录（`nlink == 0`）里塞新文件，并且如果父目录带有 `S_ISGID` 位，新文件要自动继承父目录的属组 `gid`，同时更新父目录的 `mtime` 和 `ctime`。
4. **数据库插入语句怎么写才方便后面 `Mkdir`、`Symlink`、`Link` 共用**：后面的 `Mkdir`、`Symlink`、`Link` 同样需要做“插入节点、插入目录项、更新父目录时间”这三件事，把它们拆成三个独立的 SQL 函数比写死在一条大 SQL 里更好复用。

## 决策
1. **接口参数与 `umask` 权限计算**：
   - `MknodRequest` 接收 `mode`、`parent_ino`、`name`、`uid`、`gid`、`rdev`；业务层 `MknodParams`（以及后续的 `MkdirParams`、`CreateParams`、`TmpFileParams`）支持传入 `Umask`，按 `(mode & PermMask) &^ (umask & PermMask)` 计算扣除 `umask` 后的落库权限位；`MknodResponse` 与 `LookupResponse` 结构相同，返回新建的 `Inode` 以及 `entry_timeout_seconds`、`attr_timeout_seconds`。
   - 限制 `parent_ino` 在 `1..math.MaxInt64` 之间，`mode` 在 `1..0o177777`（`65535`）之间，`name` 长度为 `1..255` 字节且不能包含 `/` 和 `\x00`。
2. **只允许创建五种非目录非软链文件，非设备文件 `rdev` 归零，非目录节点 `parent_ino` 填 `0`**：
   - `Mknod` 只允许传入五种文件类型：常规文件 `S_IFREG (0o100000)`、字符设备 `S_IFCHR (0o020000)`、块设备 `S_IFBLK (0o060000)`、命名管道 `S_IFIFO (0o010000)`、套接字 `S_IFSOCK (0o140000)`；如果传了 `0`、目录 `S_IFDIR`、符号链接 `S_IFLNK` 或其他不认识的类型，一律返回 `ErrInvalidArgument`（`EINVAL`）。
   - 只有当类型是 `S_IFCHR` 或 `S_IFBLK` 时才保存请求传入的 `rdev`，其他类型一律把 `rdev` 设为 `0`。所有非目录节点（`Mknod`、`Create`、`TmpFile`、`Symlink`）在 `inodes` 表里的 `parent_ino` 字段固定填 `0`（因为非目录文件可以有多个硬链接挂在不同父目录下，父子关系由 `entries` 表记录；只有目录节点才在 `inodes.parent_ino` 里记录唯一父目录）。新建节点的初始 `size = 0`、`nlink = 1`、`generation = 0`，时间戳取当前时间 `clock_timestamp()`。
3. **在事务里先锁住父目录行、拦住已删除父目录，并处理 `S_ISGID` 属组继承**：
   - `meta.Manager.Mknod` 开启读写事务，先调用 `GetInodeForUpdate(parent_ino)` 锁住父目录行：如果父节点不存在，或者已经被删除（`nlink == 0` 的孤儿目录），返回 `ErrNotFound`（`ENOENT`）；如果父节点不是目录，返回 `ErrNotDir`（`ENOTDIR`）。
   - 父目录检查通过后，如果传入的 `name` 是 `"."` 或 `".."`，直接返回 `ErrAlreadyExists`（`EEXIST`）。
   - 如果父目录的 `mode` 带有 `S_ISGID (0o2000)` 位，新建节点的 `gid` 直接使用父目录的 `gid`。
4. **分三步执行 SQL，靠数据库唯一约束拦截重名冲突（SQLSTATE `23505`）**：
   - 在 `sqlc` 层定义三个独立的基础操作：`CreateInode`（插入节点）、`CreateEntry`（插入目录项）、`TouchInodeTimestamps`（更新父目录的 `mtime` 和 `ctime`）。
   - 正常创建时不提前去查一遍 `entries` 表有没有重名；直接调 `CreateEntry` 插入，如果触发了 PostgreSQL 主键唯一约束冲突（SQLSTATE `23505` `unique_violation`），代码捕获这个错误并转成 `ErrAlreadyExists`（`EEXIST`）同时回滚事务。

## 结果
- **正面效果**：完整实现了 POSIX / FUSE `mknod` 的文件类型限制、属组继承、已删父目录拦截和父目录时间更新；拆出来的 `CreateInode`、`CreateEntry`、`TouchInodeTimestamps` 三个 SQL 操作可以直接给后面的 `Mkdir`、`Symlink`、`Link` 共用。
- **代价与取舍**：如果并发创建同名文件撞上了唯一约束回滚，会白白用掉一个自增的 `inode_id_seq` 编号，但在正常情况下省去了一次每次创建前都要先查一遍 `entries` 表的额外查询。
