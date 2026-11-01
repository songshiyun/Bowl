# 0004. FUSE GetAttr 单条 SQL 直接查询与已删除文件的打开句柄检查

## 状态
已采纳 (accepted)

## 背景
Storage 元数据服务在实现对接 Linux FUSE 内核 `FUSE_GETATTR` 协议的 `GetAttr`（查询文件属性）接口时，需要考虑三个问题：
1. **查单个节点没必要开启显式数据库事务**：`GetAttr` 只是按主键 `ino` 查 `inodes` 表里的一行记录，直接执行一条 `SELECT` 语句就够了，不需要多花往返时间去执行 `BEGIN` 和 `COMMIT`。
2. **文件被删除后，只有还被打开着才能继续查属性 (STARSRC-279)**：POSIX 标准允许一个文件在被删除目录项（`unlink`，此时 `nlink == 0`）后，只要还有进程拿着打开它的文件句柄（`FileHandle`），就能继续对它调用 `fstat` 查属性。但如果一个 `nlink == 0` 的孤儿文件当前**没有任何打开的句柄**（比如等待后台清理的残留节点），就必须拦住并返回 `ENOENT`（文件不存在），绝不能让外部还能查到它的属性。
3. **返回结果要包含缓存超时时间和算好的块大小**：返回的 `Inode` 结构需要带上 `EntryTimeoutSeconds` 和 `AttrTimeoutSeconds`，并按统一规则填好 `Blksize = 65536` 和 `Blocks`。

## 决策
1. **直接用一条 `SELECT` 查询，不开启显式事务**：
   - `meta.Manager.GetAttr` 先检查输入参数：`ino == 0 || ino > math.MaxInt64` 时返回 `ErrInvalidArgument`。
   - 直接调用 `Store.GetInode(ctx, int64(ino))` 查询；如果查不到记录，返回 `ErrNotFound`（`ENOENT`）。
2. **发现 `nlink == 0` 时，检查是否还有未关闭的打开句柄 (STARSRC-279)**：
   - 当查出来的节点 `target.Nlink == 0` 时，调用 `Store.HasFileHandlesByIno(ctx, int64(ino))` 看看 `file_handles` 表里还有没有属于它的有效句柄。
   - 如果没有打开的句柄（`!hasHandles`），立刻返回 `ErrNotFound`（`ENOENT`）。
   - 只有确认还存在打开的句柄时，才正常返回 `st_nlink = 0` 的节点属性。
3. **统一计算块属性与填入缓存超时时间**：
   - 调用 `m.newInode(target)` 组装返回结构，填入全局配置的 `attrTimeout`，以及根据是不是目录填入 `dirEntryTimeout` 或 `entryTimeout`。
   - 统一设置 `Blksize = 65536` (`0x10000`)；目录固定 `Size = 4096`、`Blocks = 8`；特殊文件（`S_IFCHR`, `S_IFBLK`, `S_IFIFO`, `S_IFSOCK`）固定 `Size = 0`、`Blocks = 0`。
4. **错误码统一转换为 gRPC Status 并附带 POSIX `errno` 名称**：
   - 业务层的错误在 `ToStatus` 中统一转成对应的 gRPC 状态码，并在 `google.rpc.ErrorInfo` 中填上具体的 POSIX 错误码名称 `reason`（如 `ENOENT`, `EINVAL`, `ENOTDIR`, `EIO`, `EACCES`）和 `domain: bowl.storage`。

## 结果
- **正面效果**：
  - 既保证了“删掉文件但还没关闭句柄时调用 `fstat` 能正常工作”，又防止了没有句柄的残留孤儿节点被外部查出来。
  - 普通文件（`nlink > 0`）只需要一次主键查询就能返回属性。
- **代价与取舍**：只有遇到 `nlink == 0` 的孤儿文件时，才会多查一次 `file_handles` 表（`HasFileHandlesByIno`），对正常文件的读取没有任何额外开销。
