# 0005. FUSE SetAttr 在事务内加锁修改文件属性与 POSIX 属性规则检查

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务在支持 `Lookup` 和 `GetAttr` 后，需要实现对接 Linux FUSE 内核 `FUSE_SETATTR` 协议的 `SetAttr` 接口（对应 `chmod`、`chown`、`truncate`、`utimens` 等修改文件属性的系统调用）。在实现时需要处理三个关键问题：
1. **每次只改一部分属性，且合法性依赖于文件当前的类型和状态**：`SetAttr` 每次调用可能只改权限、只改属主、只改大小或只改时间。而且很多 POSIX 规则必须结合这个 `Inode` 当前是什么类型才能判断：比如对目录改 `size` 要报 `EISDIR`、对设备或符号链接改 `size` 要报 `EINVAL`、改权限 `mode` 时不能篡改文件类型位 `S_IFMT`、没传 `mode` 但改了常规文件的 `uid` / `gid` / `size` 时必须自动抹掉 `S_ISUID` / `S_ISGID` 特权位。如果不加锁先查出来、算好再写回去，两个并发的 `SetAttr` 就会互相覆盖掉对方的修改，也没法准确返回 `ENOENT`、`EISDIR` 和 `EINVAL`。
2. **`ctime` 由服务端统一更新，且截断文件时要同步更新 `mtime`**：按照标准 FUSE `FUSE_SETATTR` 规则，状态变更时间 `ctime` 必须由服务端更新为当前时间；而且当调用 `truncate` 修改了文件大小 `size`、又没有专门指定新的 `mtime` 时，必须顺带把 `mtime` 也更新为当前时间。
3. **Protobuf 如何表示“只改某几个字段”，以及防止 64 位整数转成数据库有符号数时溢出**：Protobuf 请求需要区分一个字段是“没传（不修改）”还是“传了 0”，需要支持 `ATIME_NOW` / `MTIME_NOW`（设为当前时间），并且要拦住什么都不改的空请求，以及防止 `uint64` 的 `ino` 或 `size` 超过 PostgreSQL `BIGINT` 的最大值 `math.MaxInt64`。

## 决策
1. **Protobuf 使用 `optional` 字段表示按需修改，并用 CEL 规则检查参数**：
   - 在 `SetAttrRequest` 中，将可修改的属性（`mode`、`uid`、`gid`、`size`、`atime`、`mtime`、`flags`、`fh`、`session_id`）定义为 `optional` 字段，并配上布尔标志（`atime_now`、`mtime_now`、`ctime_now`）。
   - 限制 `ino`（`> 0 && <= math.MaxInt64`）、`size`（`<= math.MaxInt64`）、`mode`（`<= 0o177777` 即 `65535`）和 `flags`（`<= math.MaxInt32`）的取值范围。
   - 用 CEL 规则强制要求：每次请求至少要指定修改一项属性；不能同时传具体的 `atime` 时间戳和 `atime_now = true`；不能同时传具体的 `mtime` 时间戳和 `mtime_now = true`。
2. **开启写事务，先用 `SELECT ... FOR UPDATE` 锁住 `Inode` 行，检查后再更新**：
   - 在 `sqlc` 查询层提供 `GetInodeForUpdate`（`SELECT ... FROM inodes WHERE ino = $1 FOR UPDATE`）和 `UpdateInodeAttr`（更新 `mode, size, uid, gid, atime, mtime, flags` 并把 `ctime` 设为当前时间 `clock_timestamp()`，返回更新后的完整记录）。
   - `meta.Manager.SetAttr` 开启读写事务：先用 `GetInodeForUpdate` 锁住并读出目标 `Inode`（不存在则返回 `ErrNotFound`）；如果请求带了文件句柄 `fh`，就调 `GetFileHandle` 检查句柄是否存在且属于该 `(ino, session_id)`（不对则返回 `ErrBadFileHandle` / `EBADF`），而且如果同时要改文件大小 `size`（即 `ftruncate`），句柄必须具有写权限（`O_WRONLY` 或 `O_RDWR`）；如果没传 `flags` 则保留原有的 `current.Flags`，最后调 `UpdateInodeAttr` 写入数据库并算出 `Blksize` 和 `Blocks`。
3. **严格执行 POSIX 属性修改规则与自动清除特权位**：
   - **不允许改变文件类型，也不允许修改符号链接的权限**：如果目标是符号链接（`S_IFLNK`）且请求修改 `mode`，直接返回 `ErrInvalidArgument`（POSIX 规定符号链接权限固定为 `0777`，不能用 `chmod` 修改）；如果传入的 `mode` 带有文件类型位（`mode & S_IFMT != 0`）且跟原来的文件类型不一致，返回 `ErrInvalidArgument`；最终存进数据库的 `mode` 始终保留原来的文件类型位 `(old_mode & S_IFMT) | (new_mode & 0o7777)`。
   - **只有常规文件才能修改 `size`**：当请求传入了 `size` 时，如果目标是目录（`S_IFDIR`）则返回 `ErrIsDir`（对应 `EISDIR`）；如果是其他非常规文件（`!= S_IFREG`）则返回 `ErrInvalidArgument`（对应 `EINVAL`）。
   - **改属主或改大小时自动清除 `S_ISUID` / `S_ISGID` 位**：如果请求没有传新的 `mode`，但对常规文件（`S_IFREG`）修改了 `uid`、`gid` 或 `size`，会自动把原来 `mode` 里的 `S_ISUID (0o4000)` 和 `S_ISGID (0o2000)` 特权位清掉。
   - **自动更新时间戳**：每次 `SetAttr` 更新都会把 `ctime` 设为当前时间；如果修改了 `size` 且没有专门传新的 `mtime` 或 `mtime_now`，也会自动把 `mtime` 设为当前时间。

## 结果
- **正面效果**：用数据库行级排他锁避免了并发修改属性时互相覆盖，在业务代码里完整实现了 POSIX / FUSE 的类型检查、特权位清除和错误码区分（`EINVAL`、`ENOENT`、`EISDIR`、`EBADF`）。
- **代价与取舍**：每次 `SetAttr` 在同一个事务里做一次主键加锁查询和一次主键更新，都是单行主键操作，耗时很短。
