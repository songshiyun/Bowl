# 0003. POSIX Lookup 父子目录检查、`.` 与 `..` 特殊目录项解析及块大小计算

## 状态
已采纳 (accepted) — 部分被 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 演进（普通文件名查询合并为单条 `LEFT JOIN` SQL）

## 背景
在实现 Storage 元数据引擎的 `Lookup`（按父目录 ID 和文件名查找子节点）功能时，需要满足以下四个要求：
1. **严格区分“文件或父目录不存在（`ENOENT`）”和“父节点不是目录（`ENOTDIR`）”**：按照 POSIX `lookup(parent_ino, name)` 的规定，如果传入的 `parent_ino` 本身不存在，或者 `parent_ino` 下没有叫 `name` 的子项，应该返回 `ENOENT`；但如果 `parent_ino` 存在却是个普通文件而不是目录，必须返回 `ENOTDIR`。
2. **数据库只存基础字段，`blksize` 和 `blocks` 在读取时直接算出来**：数据库的 `inodes` 表只保存文件的核心状态（`mode`, `size`, `nlink`, `uid`, `gid`, `rdev`, 时间戳），不需要浪费空间去存固定不变的 `blksize`（推荐 I/O 块大小）和由文件大小算出来的 `blocks`（占用的 512 字节块数）。
3. **支持查找 `"."` 和 `".."`，并拦住已被删除的父目录**：需要支持查找当前目录 `"."` 和上级目录 `".."`（包括根目录的 `".."` 指向自己，以及当一个目录被删除但还被进程打开着时，在里面执行 `cd ..` 仍能通过 `inodes.parent_ino` 找到原来的上级目录）；同时，如果父目录已经被删除了（`parent.Nlink == 0`），不允许再在它下面查找普通子文件。
4. **对齐 JuiceFS 的空文件名处理和缓存超时设置**：当传入空文件名 `len(name) == 0` 时，先检查父目录存不存在、是不是目录，然后再返回 `ENOENT`；并且支持对目录（`dirEntryTimeout`）和普通文件（`entryTimeout`）分别设置不同的内核缓存有效期。

## 决策
1. **使用 `sqlc` + `pgx/v5` 访问数据库**：
   - 使用 `sqlc` 和 `pgx/v5` 生成类型安全的数据库查询代码 `Querier`，并配合 `github.com/starsrc/Aether/pkg/tx` 管理数据库事务。
2. **Lookup 的检查顺序与 `"."` / `".."` 解析逻辑**：
   - 先检查输入参数：`parent_ino == 0 || parent_ino > math.MaxInt64` 返回 `ErrInvalidArgument`；文件名超过 255 字节（`len(name) > MaxFileNameBytes`）返回 `ErrNameTooLong`；文件名包含 `'/'` 或 `'\x00'` 返回 `ErrInvalidArgument`。空文件名 `len(name) == 0` 不在这一步提前拦掉，留到后面检查完父目录再处理。
   - 按以下顺序检查父目录并查找目标项（注：早期版本开启只读事务分两步查，后续在 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 中对普通文件名优化为一条 `LEFT JOIN` SQL 一次性查出父目录与子项）：
     1. 检查父节点 `parent_ino`：不存在返回 `ErrNotFound`（`ENOENT`）；不是目录返回 `ErrNotDir`（`ENOTDIR`）。
     2. 优先处理 `".."`（必须在拦截 `parent.Nlink == 0` 之前处理，保证在被删掉的目录里还能 `cd ..` 退出来）：
        - 如果父目录本身就是根目录 `RootIno (1)`，返回根目录自己。
        - 如果 `parent.ParentIno > 0 && parent.ParentIno != parentIno`，直接按 `parent.ParentIno` 查出上级目录属性；如果查不到，再调 `GetParentInode` 去 `entries` 表反查，还没有就返回 `ErrNotFound`。
        - 如果 `parent.ParentIno <= 0`，调 `GetParentInode` 反查；如果查不到且 `parent.Nlink > 0` 就返回父目录自己，否则返回 `ErrNotFound`。
     3. 拦住已被删除的父目录：如果 `parent.Nlink == 0`，直接返回 `ErrNotFound`（`ENOENT`），不再往下查子项。
     4. 处理 `"."`：直接返回当前父目录自己的属性。
     5. 处理空文件名 `""`：前面确认父目录存在且确实是目录后，返回 `ErrNotFound`（`ENOENT`）。
     6. 查找普通子项：按 `(parent_ino, name)` 查找对应的子项 `Inode`，找不到返回 `ErrNotFound`（`ENOENT`）。
3. **根据文件类型计算 `Blksize`、`Blocks` 和 `Rdev`**：
   - 统一固定 `Blksize = 65536` (`0x10000`, 64 KiB)，与 JuiceFS 的 `attrToStat` 保持一致，避免应用按 4 KiB 小块读写。
   - 目录节点默认显示 `Size = 4096`，算得 `Blocks = 8`（每个块按 512 字节算）。
   - 常规文件与符号链接按 `(Size + 511) / 512` 计算 `Blocks`。
   - 特殊文件（字符设备 `S_IFCHR`、块设备 `S_IFBLK`、管道 `S_IFIFO`、套接字 `S_IFSOCK`）固定返回 `Size = 0` 和 `Blocks = 0`；只有字符设备和块设备才返回真实的设备号 `Rdev`，其他文件类型一律返回 `Rdev = 0`。
4. **目录与文件分别设置缓存超时时间**：
   - `meta.Manager` 分别维护三个超时配置：`dirEntryTimeout`（目录项缓存时间）、`entryTimeout`（非目录项缓存时间）和 `attrTimeout`（属性缓存时间），在 `Lookup` 和 `ReadDirPlus` 返回时一并填给客户端。

## 结果
- **正面效果**：
  - 完整处理了 `.`、`..`、空文件名 `""` 以及已被删除父目录的各种边界情况，解决了进程处在被删除目录中执行 `cd ..` 出错的问题。
  - 将 `Blksize` 设为 `65536`（64 KiB），让标准库使用大块读写。
- **代价与取舍**：多走几步条件判断，但都是内存判断和主键查询，非常快。
