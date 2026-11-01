# 0008. FUSE 目录增删改按 Inode 编号升序加锁、孤儿节点保留与防目录成环

## 状态
已采纳 (accepted)

## 背景
Storage 元数据引擎在实现 `Mkdir`、`Link`、`Unlink`、`Rmdir` 和 `Rename` 这五个修改目录结构的接口时，一次操作往往要同时读写多个父目录和子节点。为了保证并发安全和符合 POSIX 标准，需要解决四个核心问题：
1. **多个请求同时锁多个节点时容易发生数据库死锁**：比如一个请求执行跨目录重命名 `Rename(A/x -> B/y)`，同时另一个请求反向执行 `Rename(B/u -> A/v)`，或者并发执行 `Link` 和 `Unlink`。如果不同请求加数据库行锁的顺序不一致（比如都先锁源目录再锁目标目录），并发高的时候很容易互相等待对方手里的锁，触发 PostgreSQL 死锁报错（SQLSTATE `40P01`）。
2. **文件被删除时（`Unlink` / `Rmdir` / `Rename` 覆盖），什么时候留着 `inodes` 记录、能不能再用 `Link` 恢复**：ADR-0004 已经规定要支持“文件被删掉目录项但还打开着（`unlink-whilst-open`，`nlink == 0`）”时继续查属性。所以我们需要明确：`Unlink`、`Rmdir` 和 `Rename` 覆盖目标时只删 `entries` 目录项而不直接删 `inodes` 行（注：后续在 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 中优化为：如果没有打开的句柄就直接删掉 `inodes` 行，有打开句柄才保留为孤儿节点）；同时如果有人对已经变成 `nlink == 0` 的孤儿文件调用 `Link`，必须拦住不能让它“死而复生”。
3. **怎么判断一个目录是不是空目录，以及怎么维护父目录的 `nlink`**：在 POSIX 里，新建一个空目录它的 `nlink` 是 `2`（代表目录本身和里面的 `.`），每在里面建一个子目录，父目录的 `nlink` 就加 1（因为子目录里的 `..` 指向父目录）。但如果目录里只有普通文件而没有子目录，它的 `nlink` 依然是 `2`，所以不能光看 `nlink == 2` 就认为目录是空的。
4. **`Rename` 重命名时遇到同一个文件的硬链接，或者把父目录移进自己的子目录里怎么处理**：POSIX `rename(2)` 规定，如果源路径和目标路径指向的是同一个 `Inode`（无论是同一个路径，还是同一个文件的两个不同硬链接），必须什么都不改直接返回成功；另外跨目录移动一个目录时，必须检查不能把这个目录移动进它自己的子孙目录里面去（否则这棵子树就跟根目录断开变成死循环了，必须返回 `EINVAL`）。

## 决策
1. **所有修改目录的事务，一律按涉及到的所有 `Inode` 编号（`ino`）从小到大依次加锁**：
   - 因为一个以前创建的子目录，它的 `ino` 完全可能比后来新建的另一个父目录的 `ino` 更小，而且子目录随时可能在其他并发请求里当父目录用。所以规定：**不管节点在当前请求里是父目录还是子节点，一律把本次操作涉及到的所有 `ino` 收集起来去重，严格按数字从小到大执行 `SELECT ... FOR UPDATE` 加行锁**：
     - `Mkdir`：只锁 `parent_ino`。
     - `Link`：把 `new_parent_ino` 和 `ino` 去重后，先锁较小的那个，再锁较大的那个。
     - `Unlink` / `Rmdir`：先不加锁查一下目标 `Entry` 拿到它当前指向的 `child_ino`，然后把 `parent_ino` 和 `child_ino` 按从小到大加锁；拿到锁之后，再重新查一次确认父目录有效且该 `Entry` 指向的还是刚才那个 `child_ino`。如果在加锁前的一瞬间这个 `Entry` 被别的并发请求改掉了，就回滚当前事务重新来一遍。
     - `Rename`：同样先预读一下源 `Entry` 和目标 `Entry`（拿到 `old_ino` 和 `new_ino`），把 `old_parent_ino`、`new_parent_ino`、`old_ino`、`new_ino` 全部合在一起去重，严格按 `ino` 从小到大加锁；拿到锁后再核对一遍源和目标 `Entry` 有没有变，如果变了就回滚重试。
2. **删除目录项时把孤儿节点的 `nlink` 设为 `0`，并禁止用 `Link` 恢复已被删的孤儿节点**：
   - `Unlink`、`Rmdir` 和 `Rename` 覆盖目标时，从 `entries` 表删掉对应的目录项，并更新目标 `Inode` 的硬链接数 `nlink`（非目录节点 `nlink - 1`，目录节点直接把 `nlink` 设为 `0`）以及更新 `ctime`；当节点变成 `nlink == 0` 的孤儿节点（`Orphan Inode`）时保留其 `inodes` 记录以支持已打开句柄继续访问。
   - `Link` 在锁住源 `Inode` 后，如果发现它是目录就返回 `ErrPerm`（`EPERM`）；如果发现硬链接数已经达到上限 `MaxNlink (65000)` 就返回 `ErrTooManyLinks`（`EMLINK`）；如果发现它的 `nlink == 0`（已经被删掉的孤儿节点），就返回 `ErrNotFound`（`ENOENT`），不允许给已被删除的文件再挂新文件名（注：后续在 [ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md) 中，仅为带 `O_TMPFILE` 且未带 `O_EXCL` 的新建匿名临时文件开放了一次性 `Link` 转正）。
3. **维护目录的 `nlink` 和 `parent_ino`，用 `EXISTS` 查询判断目录是否为空**：
   - `Mkdir` 支持创建权限为 `0000` 的目录，结合 `Umask` 算出最终权限 `S_IFDIR | ((mode & 0o7777) &^ (umask & 0o7777))`；如果父目录的 `nlink >= MaxNlink (65000)` 则返回 `ErrTooManyLinks`（`EMLINK`）；如果父目录带有 `S_ISGID (0o2000)` 位，新目录同时继承父目录的 `gid` 和 `S_ISGID` 位。新目录初始 `nlink = 2`，在 `inodes.parent_ino` 记录父目录编号，并把父目录的 `nlink` 加 1。
   - `Rmdir` 和 `Rename`（覆盖目标目录时）统一用 `EXISTS (SELECT 1 FROM entries WHERE parent_ino = $1)` 检查目标目录下面还有没有子项，只要有子项就返回 `ErrNotEmpty`（`ENOTEMPTY`）。`Rmdir` 成功后把父目录的 `nlink` 减 1。
   - 针对空文件名 `""`、`"."` 和 `".."` 的特殊处理：在锁住并确认父目录有效后，`Unlink("")`、`Rmdir("")` 和 `Rename(old_name = "")` 返回 `ErrNotFound`（`ENOENT`）；创建类操作（`Mkdir` / `Link` / `Symlink`）传 `.` 或 `..` 返回 `ErrAlreadyExists`（`EEXIST`）；`Rmdir("..")` 返回 `ErrNotEmpty`（`ENOTEMPTY`）；其余对 `.` 或 `..` 的删除和重命名操作一律返回 `ErrInvalidArgument`（`EINVAL`）。
4. **`Rename` 遇到同一节点直接返回成功、检查覆盖类型，并用 `WITH RECURSIVE` 防止把目录移进自己的子目录**：
   - **源和目标是同一个路径或同一个 `Inode` 时直接返回**：在确认新旧父目录都有效且源 `Entry` 存在后，如果发现 `old_parent_ino == new_parent_ino && old_name == new_name`，或者目标 `Entry` 已经存在且 `old_ino == new_ino`（即同一个文件的两个硬链接），什么都不改，直接返回成功。
   - **覆盖已有目标时的类型检查与父目录更新**：当目标 `new_ino` 存在且跟源 `old_ino` 不是同一个节点时，如果源是目录而目标不是目录，返回 `ErrNotDir`；如果源不是目录而目标是目录，返回 `ErrIsDir`；如果两边都是目录，先检查目标目录必须为空（不为空返回 `ErrNotEmpty`），再把被覆盖的目标目录 `nlink` 设为 `0`。当跨父目录移动一个目录时，如果没有覆盖目标目录，要先检查目标父目录的 `nlink < MaxNlink`（否则返回 `ErrTooManyLinks`），然后把源父目录 `nlink - 1`、目标父目录 `nlink + 1`（如果刚好覆盖了目标父目录下的一个空目录，那目标父目录的 `nlink` 一减一加保持不变），并把被移动目录的 `inodes.parent_ino` 更新为 `new_parent_ino`。
   - **顺着父目录往上查，防止把目录移进自己的子孙目录**：当被移动的源节点是目录，而且换了父目录（`old_parent_ino != new_parent_ino`）时，用 PostgreSQL 的 `WITH RECURSIVE` 递归查询，从目标父目录 `new_parent_ino` 开始沿着 `entries` 一路往上查它所有的上级目录；只要在祖先链里看到了源目录 `old_ino`（说明正试图把目录塞进它自己的子目录里），就立刻拒绝重命名并返回 `ErrInvalidArgument`（`EINVAL`）。

## 结果
- **正面效果**：靠“所有涉及的 `ino` 一律从小到大加锁 + 发现目录项变化自动重试”，彻底根治了并发移动和删除目录时的数据库死锁问题；靠向上追溯全体祖先目录，防止了目录树被移动成死循环；同时把孤儿节点的状态变化和前面的 `Lookup`、`GetAttr`、`Mknod` 完整接上了。
- **代价与取舍**：`Unlink`、`Rmdir` 和 `Rename` 在加锁前需要先做一次按主键查 `Entry` 的预读，极端并发下如果刚好改了同一个目录项会触发一次重试；跨目录移动目录时的 `WITH RECURSIVE` 向上查询因为每个目录只有一个父目录，实际上就是顺着主键往上查几步，开销很小。
