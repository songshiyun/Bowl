# 0016. 用内存参考模型对比测试元数据与文件锁的随机操作序列

## 状态
已采纳 (accepted) — 配套验证 [ADR-0008](0008-fuse-directory-topology-locking-and-orphan-lifecycle.md)、[ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)、[ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md)、[ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md)

## 背景
Storage 元数据引擎已经实现了目录树增删改、硬链接计数（`nlink`）、匿名临时文件（`TmpFile`）转正式文件、孤儿节点（`Orphan Inode`）延迟清理、客户端会话过期回收、文件锁（POSIX 字节区间锁和 BSD `flock`）以及磁盘配额统计（`Disk`）等一整套功能。为了验证这些功能组合在一起时不会出错，我们面临三个实际问题：
1. **手写固定步骤的单元测试测不全复杂组合，而用 TLA+ 等外部工具又测不到真实的 SQL 代码**：靠人工写固定步骤的测试，很难覆盖像“跨目录 `Rename`（带 `RENAME_EXCHANGE` 交换）+ `TmpFile` 调 `Link` 转正 + `Open(O_TRUNC)` 清空 + 会话过期后由 `RenewSession` 清理孤儿文件”这样多步穿插的场景。如果去写独立的 TLA+ 或 Coq 建模公式，验证的只是纸面上的公式，根本测不到真正运行的 Go 事务和 PostgreSQL SQL 语句有没有漏写条件或留下脏数据。
2. **在不接入文件数据块的情况下先验证好元数据和配额**：对于目录树和配额统计来说，文件的实际内容其实只体现为常规文件 `Inode` 的 `size` 数字，完全可以在不涉及底层文件数据块读写的情况下，先把所有元数据和配额账目测扎实。
3. **36 个接口如何分组测试才最有效**：`StorageService` 一共有 36 个公开 RPC 接口。如果把“目录树和磁盘配额”跟毫不相干的“文件锁区间合并与等待队列”混在同一个随机测试里乱调一气，反而会稀释两边触发复杂边界的概率，报错时也不好排查。

## 决策
1. **写一个纯 Go 内存版参考实现（Reference Model），每走一步都和真实 PostgreSQL 数据库对比结果并检查底表数据**：
   - 用 Go 标准库 `math/rand/v2` 编写随机操作生成器，支持传入固定的 64 位随机种子（`Seed`）复现问题，并打印每一步的中文操作记录。
   - **既比对接口返回值，又直接查数据库表核对账目**：每一个随机生成的操作，都同时在内存参考实现和真实的 PostgreSQL `meta.Manager` 上执行一次；除了检查两边返回的结果和 POSIX 错误码是否完全一样，还直接按 `root_ino` 去查数据库里的 `disks`、`inodes`、`entries`、`file_handles`、`client_sessions`、`file_locks`、`file_lock_waiters` 表，逐条核对下面的数据一致性规则。

2. **把 36 个公开接口分成两组随机测试（目录树配额组 + 文件锁组）**：
   - **第一组：目录树、孤儿文件清理与磁盘配额测试**：
     - **写操作（19 个）**：`CreateDisk`、`UpdateDisk`、`DeleteDisk`、`Mkdir`、`Mknod`、`Symlink`、`Create`、`TmpFile`、`Link`、`Unlink`、`Rmdir`、`Rename`、`Open`、`OpenDir`、`SetAttr`、`Fallocate`、`Release`、`ReleaseDir`、`RenewSession`。
     - **读操作与检查（10 个）**：`Lookup`、`GetAttr`、`Readlink`、`ReadDir`、`ReadDirPlus`、`GetDisk`、`StatFS`、`Fsync`、`FsyncDir`，以及不传 `lock_owner` 和 `session_id` 的只读 `Flush`。
   - **第二组：文件锁区间拆分合并、双锁隔离与死锁检测测试**：
     - **加解锁操作（4 条路径，含 2 个专门接口和 2 个共用接口）**：专门的锁接口 `SetLk`、`SetLkw`，加上带 `lock_owner`（或 `session_id`）的 `Flush` 和带 `FuseReleaseFlockUnlock` 的 `Release`。
     - **查锁操作（1 个）**：`GetLk`。
   - **不放入随机状态机的接口（4 个）**：单个文件上的扩展属性接口（`SetXattr`、`GetXattr`、`ListXattr`、`RemoveXattr`）只读写单个文件自己的键值对，不影响目录树、磁盘配额和文件锁，继续由专门的单元测试覆盖。（合计：19 个主写接口 + 10 个主读接口 + 2 个锁写接口 + 1 个锁读接口 + 4 个扩展属性接口 = 36 个公开接口）。

3. **第一组测试每步必查的 6 条数据库底表规则**：
   在单线程随机测试的每一步执行完后，以及多线程并发测试全部跑完后，数据库表里的数据必须满足以下 6 条规则：
   - **规则 1（目录树不能成环，且父子记录两边对得上）**：
     - 根目录（`ino == root_ino`）自己的 `inodes.parent_ino == root_ino`，且在 `entries` 表里没有任何条目指向根目录；
     - 任何一个未被删除的普通子目录（`ino != root_ino && nlink > 0`），在 `entries` 表里都只有唯一一条父目录记录，且 `entries.parent_ino == inodes.parent_ino`；顺着 `entries` 往上找父目录，一定能在有限步内回到它所属 `Disk` 的 `root_ino`，绝不出现死循环。
   - **规则 2（硬链接数 `nlink` 必须和实际目录项数量一致）**：
     - 对于任何非目录节点：`inodes.nlink` 必须等于 `entries` 表里指向该 `ino` 的记录条数；
     - 对于任何目录节点：如果目录还没被删（包含 `root_ino`），`inodes.nlink` 必须等于 `2 + 该目录下子目录的个数`；如果是已经被 `Rmdir` 删掉但还打开着的孤儿目录，`inodes.nlink` 必须为 `0`，且它下面不能有任何子目录项。
   - **规则 3（`nlink == 0` 的孤儿文件必须有打开的句柄，句柄关完后必须被彻底删掉）**：
     - 任何 `inodes.nlink == 0` 的孤儿节点，在刚被 `Unlink` 或刚通过 `TmpFile` 创建出来的那一刻，`file_handles` 表里一定至少有一条属于它的有效句柄（没有绑定会话的句柄，或者绑定的会话还没过期）；
     - 当没有其他并发事务占着锁，并调用一次 `RenewSession` 清理过期会话后，数据库里绝不能残留任何既没有有效句柄、`nlink` 又等于 `0` 的孤儿节点。
   - **规则 4（磁盘已用空间和节点数必须和 `inodes` 表汇总结果完全一致）**：对于任何还未删除的 `Disk`（对应 `root_ino`）：
     - 该盘统计的 `used_bytes` 必须等于 `inodes` 表里同一 `root_ino` 下所有常规文件（`(mode & S_IFMT) == S_IFREG`，包含正常文件和还没物理删除的孤儿常规文件）的 `size` 总和（符号链接 `S_IFLNK` 的 `size` 不算进磁盘已用空间）；
     - 该盘统计的 `used_inodes` 必须等于 `inodes` 表里同一 `root_ino` 下的所有节点总数（包含根目录、正常节点和还没物理删除的孤儿节点）；
     - 始终满足 `0 <= used_bytes <= max_bytes` 且 `used_inodes >= 1`；调 `UpdateDisk` 缩容时如果新容量小于 `used_bytes` 必须报错返回 `ErrInvalidArgument`（`EINVAL`）；调 `DeleteDisk` 删除磁盘后，`inodes`、`entries`、`file_handles`、`file_locks` 表里属于该 `root_ino` 的记录必须全部清零。
   - **规则 5（不同磁盘之间绝不能互相硬链接或移动文件）**：`entries` 表里任何一对父子项 `(parent_ino, child_ino)`，父节点和子节点的 `root_ino` 必须相同；跨 `Disk` 调用 `Link` 或 `Rename`（包含 `RENAME_NOREPLACE` 和 `RENAME_EXCHANGE`）必须报错返回 `ErrCrossDevice`（`EXDEV`）。
   - **规则 6（`TmpFile` 匿名临时文件只能通过 `Link` 转成正式文件一次）**：只有 `TmpFile` 接口能在 `file_handles.flags` 里写 `O_TMPFILE` 标志（`Create` 和 `Open` 传入 `O_TMPFILE` 一律拒之门外）；一旦不带 `O_EXCL` 的 `TmpFile` 通过 `Link` 成功绑定了文件名（`nlink` 从 `0` 变成 `1`），它关联句柄上的 `O_TMPFILE` 标志必须在同一个事务里被抹掉；之后如果这个文件被 `Unlink` 删掉再次变成 `nlink == 0`，绝不允许再次调用 `Link` 把它恢复回来（必须返回 `ErrNotFound` / `ENOENT`）。

4. **第二组测试每步必查的 4 条文件锁规则**：
   - **锁规则 1（POSIX 字节区间锁不能重叠冲突，同拥有者的相邻同类型锁必须合并）**：在同一个文件的 POSIX 锁（`lk_flags = 0`）下，不同拥有者 `(session_id, lock_owner)` 持有的有效锁如果字节区间 `[start, end]` 有重叠，它们必须全都是读锁（`F_RDLCK`）；同一个拥有者 `(session_id, ino, lock_owner)` 名下的锁区间绝不能重叠，也不能出现首尾紧挨着（`prev.end + 1 == next.start`）且类型相同却没有合并成一条的碎片锁。
   - **锁规则 2（POSIX 锁和 BSD `flock` 互不干扰）**：只要会话没过期且文件没被删，`lk_flags == 0`（POSIX 锁）和 `lk_flags == FuseLkFlock`（BSD `flock`）各自独立加锁和解锁：带 `lock_owner` 的 `Flush` 只会删掉该进程在这个文件上的 POSIX 锁，不会动 `flock`；带 `FuseReleaseFlockUnlock` 的 `Release` 只会删掉这个句柄上的 `flock`，不会动其他句柄的 POSIX 锁。
   - **锁规则 3（同一个文件内互相等待成环时必须立即报死锁 `EDEADLK`）**：在同一个文件、同一种锁类型下，当阻塞加锁请求 `SetLkw` 遇到锁冲突需要排队等待时，如果顺着“谁在等谁的锁”画出的等待关系里出现了回到自己的环路（`hasDeadlockCycle`），必须立刻拒绝并返回 `ErrDeadlock`（`EDEADLK`），不能把这条会导致死锁的请求写进 `file_lock_waiters` 等待表。
   - **锁规则 4（取消等待或会话过期后，不能留下残留的等待记录或没人认领的锁）**：只要数据库连接正常，任何因为超时或取消（`ErrInterrupted` / `EINTR`）退出的 `SetLkw` 请求，在清理完成后，`file_lock_waiters` 表里不能留下它的 `PENDING` 记录，`file_locks` 表里也不能多出没人认领的锁；当会话过期并调用 `RenewSession` 清理后，该过期会话名下的 `file_locks` 和 `file_lock_waiters` 记录必须全部清零。

5. **用 20 毫秒短租约模拟客户端掉线，并分为日常快速测试与深度长测两档**：
   - **20ms 短租约模拟掉线**：在测试里把会话有效期设为极短的 `20ms`（`WithSessionLeaseTTL(20ms)`），在随机操作中间穿插短暂等待和 `RenewSession` 调用，反复测试“客户端掉线 -> 会话过期 -> `RenewSession` 自动删掉孤儿文件并退还磁盘配额”的完整过程。
   - **多线程并发乱序操作测试**：启动多个协程同时对同一棵目录树随机执行跨目录 `Rename`、`Link`、`Unlink`、`Open`/`Release`、`RenewSession` 和抢文件锁，验证全程不会因为数据库死锁而报错失败，并且等所有协程跑完调一次 `RenewSession` 后，数据库里的所有表依然满足上面全部规则。
   - **日常快跑与发版深测两档命令**：
     - **日常快速测试（`task test` 默认执行）**：在 2~3 秒内跑完多组固定步数的随机对比测试和多线程并发测试；
     - **深度长时测试（`task test:formal`）**：通过环境变量增加随机轮数和步数，用于发版前做长时间压力验证。

## 结果
- **正面效果**：不用专门去学和维护一套脱离代码的外部建模语言，直接对真实的 Go 事务代码和 PostgreSQL 数据库做随机对账和底表检查；把目录树配额测试和文件锁测试分开跑，既提高了测出边界问题的概率，又方便出错时快速定位。
- **代价与取舍**：在 `services/storage/internal/meta/` 下增加了一套内存参考实现和底表检查测试代码，平时跑 `task test` 会多花 2~3 秒时间。
