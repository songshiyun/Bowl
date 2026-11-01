# 0007. FUSE 符号链接（Symlink）直接存在 inodes 表、Readlink 单条查询与损坏检查

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在实现创建符号链接（`Symlink`）和读取符号链接（`Readlink`）时，需要保存符号链接指向的目标路径（`target`）。在现有的 `inodes` 和 `entries` 两张表结构下，需要确定四个具体问题：
1. **符号链接的目标路径存在哪里**：因为符号链接的目标路径最长不超过 4096 字节，直接在 `inodes` 表里加一列可空的 `symlink_target` 字段最简单，不需要再单独建一张关联表；同时需要在代码和数据库 `CHECK` 约束里保证只有符号链接（`S_IFLNK`）才会填这个字段。
2. **符号链接的权限与大小规则**：按照 POSIX 标准，符号链接的权限固定为 `S_IFLNK | 0o777`（`0o120777`），它的文件大小 `size` 就是目标路径的字节长度，目标路径里可以包含 `'/'` 但绝对不能包含空字节 `'\x00'`。
3. **如果数据库里的符号链接路径意外为空，应该报什么错 (STARSRC-313)**：如果调 `Readlink` 时发现节点确实是个符号链接（`S_IFLNK`），但数据库里的 `symlink_target` 却是空的，这说明服务端底层数据坏了，必须返回内部存储错误（`EIO` / `codes.Internal`），不能当成客户端传错参数返回 `EINVAL`。
4. **读取符号链接时要不要更新访问时间 `atime` (STARSRC-256, STARSRC-339)**：默认使用 `noatime` 模式，读的时候不写数据库改时间；只有明确配置了 `relatime` 或 `strictatime` 时才按规则更新 `atime`。

## 决策
1. **在 `inodes` 表里增加 `symlink_target` 列并加上检查约束**：
   - 在 `inodes` 表添加可空列 `symlink_target`，加上数据库 `CHECK` 约束：当节点类型是符号链接 `(mode & S_IFMT) = S_IFLNK` 时，`symlink_target` 不能为空且长度在合法范围内；其他所有文件类型的 `symlink_target` 必须为 `NULL`（注：后续在 [ADR-0011](0011-fuse-xattr-byte-accurate-names-and-error-reason-contract.md) 中将该列从 `TEXT` 升级为 `BYTEA` 原始字节，并将长度上限固定为 `1..4095` 字节）。
2. **`Symlink` 创建符号链接并更新父目录时间**：
   - 检查目标路径长度在 `1..4095` 之间，且不包含空字节 `'\x00'`。
   - 在写事务里锁住父目录（检查父目录存在、是目录且未删除 `nlink > 0`）。
   - 插入新的符号链接 `Inode`：权限模式固定为 `S_IFLNK | 0o777`，`size` 设为 `len(target)`，`nlink = 1`；如果父目录带有 `S_ISGID` 位，则继承父目录的 `gid`；同时在 `entries` 表插入目录项并更新父目录的 `mtime` 和 `ctime`。
3. **`Readlink` 直接用一条 SQL 查询，并检查类型与数据是否损坏 (STARSRC-313)**：
   - 检查输入参数：`ino == 0 || ino > math.MaxInt64` 时返回 `ErrInvalidArgument`。
   - 直接调用 `GetSymlinkTarget(ctx, ino)` 查出一行记录（不开启显式事务）：如果找不到记录，返回 `ErrNotFound`（`ENOENT`）。
   - 检查是不是符号链接：如果 `(mode & S_IFMT) != S_IFLNK`，说明调错了文件类型，返回 `ErrInvalidArgument`（`EINVAL`）。
   - 检查数据是否损坏：如果节点确实是符号链接 `S_IFLNK`，但读出来的 `SymlinkTarget` 长度为 0，返回 `ErrIO`（对应 `codes.Internal` + `reason: "EIO"`），把服务端数据损坏和客户端参数错误明确区分开。
   - 按配置决定是否更新 `atime`：默认 `AtimeModeNoAtime` 不写数据库；如果开启了 `RelAtime` 或 `StrictAtime` 且满足更新时间条件，则调用 `TouchInodeAtime` 更新 `atime`。

## 结果
- **正面效果**：`Readlink` 只用一条主键查询就能拿到结果，速度很快；当底层数据异常损坏时能准确返回 `EIO`；同时支持了与 JuiceFS 一致的 `atime` 更新配置。
- **代价与取舍**：多了一步判断 `SymlinkTarget` 是否为空的检查，几乎没有额外开销。
