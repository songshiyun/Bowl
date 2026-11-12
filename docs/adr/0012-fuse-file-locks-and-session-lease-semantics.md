# 0012. FUSE 文件锁（GetLk/SetLk/SetLkw）、两种锁互不干扰、客户端会话租约与死锁检查

## 状态
部分被 [ADR-0029](0029-pebble-per-branch-storage-swmr-and-checkpoint-commit.md) 与 [ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md) 替代 (partially superseded by ADR-0029 and ADR-0032) — 服务端文件锁表与 `ClientSession` 会话租约已迁为客户端本地内存 `FileLock` 与无会话 `Epoch` 栅栏，POSIX 记录锁与 BSD `flock` 隔离语义保留

## 背景
Storage 元数据引擎在完成扩展属性（Xattr）和错误码规范（ADR-0011）后，需要实现 Linux FUSE 文件锁相关的三个接口：`FUSE_GETLK (31)`、`FUSE_SETLK (32)` 和 `FUSE_SETLKW (33)`，并在 `FUSE_FLUSH` 与 `FUSE_RELEASE` 关闭文件时清理对应的锁。在按照 POSIX `fcntl(2)`、BSD `flock(2)` 和 Linux 内核 FUSE 协议实现文件锁时，需要处理六个实际问题：
1. **POSIX 字节区间锁和 BSD `flock` 整文件锁的规则完全不同**：Linux FUSE 用同一个锁接口里的 `lk_flags`（`0` 代表 POSIX 锁，`FUSE_LK_FLOCK = 0x1` 代表 BSD `flock`）来区分这两种锁。在 Linux 内核里，POSIX 锁和 `flock` 各自管各自的，互不冲突；POSIX 锁要求检查打开句柄有没有对应的读/写权限（没权限返回 `EBADF`），并且读锁升级写锁遇到冲突失败时会保留原来的读锁；而 BSD `flock` 允许对只读句柄加排他锁（`LOCK_EX`），并且升级锁时是“先删掉旧锁、再申请新锁”（如果遇到冲突失败，原来的旧锁也没了）。另外，标准 Linux FUSE 协议没有专门区分 OFD（Open File Description）锁的通道，因此我们只按标准 POSIX 锁和 BSD `flock` 来实现。
2. **`Flush` 与 `Release` 分别该清理哪种锁**：POSIX 标准规定，只要同一个进程关闭了某个文件的**任何一个**文件描述符（触发 `FUSE_FLUSH` 并带上进程锁标识 `lock_owner`），就必须立刻释放这个进程（`lock_owner`）在该文件上持有的所有 POSIX 记录锁，哪怕这些锁当初是用别的句柄 `fh` 申请的；而 BSD `flock` 是绑在打开文件句柄（`fh`）上的，只有当这个句柄彻底关闭（触发 `FUSE_RELEASE` 且带有 `FUSE_RELEASE_FLOCK_UNLOCK = 0x2` 标志）时才释放。所以数据库里的 `file_locks` 表绝对不能在 `fh` 列上加级联删除外键（`REFERENCES file_handles(fh) ON DELETE CASCADE`），否则关掉一个没加锁的句柄或者合并了多个句柄的锁时就会把锁删错。
3. **客户端死机或断网后，留下的锁怎么自动清理**：文件锁只是程序运行期间的临时状态。不同客户端机器各自生成的 64 位 `lock_owner` 有可能撞号，而且一旦客户端机器断电或断网，没机会发 `Flush` 或 `Release`，数据库里留下的锁如果不自动过期清理，就会把其他正常机器永远卡死。
4. **加锁写事务不能用 `REPEATABLE READ` 隔离级别**：如果加锁事务用了 `REPEATABLE READ`（可重复读），当它在 `SELECT ... FOR UPDATE` 等待别人释放 `Inode` 行锁时，等拿到行锁往下查 `file_locks` 表时读到的还是等待之前的旧快照（或者直接报并发修改冲突），看不到刚刚提交的锁变化。
5. **`SetLkw` 阻塞等锁时被中途取消，如何避免留下没人要的锁，以及如何发现死锁（`EDEADLK`）**：当 `SetLkw` 正在等待锁，如果服务端事务刚把锁授给它，同一瞬间网络请求却超时或收到中断（`FUSE_INTERRUPT`）取消了，如果不做清理就会在数据库里留下一把没人认领的锁；另外，如果两个进程各自拿着同一个文件的读锁，同时调用 `SetLkw` 想升级成写锁，两边互相等待对方释放读锁，就会直接卡死。
6. **目录锁不需要服务端处理，且加解锁不能修改文件时间戳**：Linux 内核的 FUSE 驱动根本不会把目录上的 `flock(2)` 请求发给用户态服务端；同时加锁和解锁不能改动 `Inode` 的 `atime`、`mtime` 或 `ctime`。

## 决策
1. **提供 `GetLk`、`SetLk`、`SetLkw` 三个接口，并正确处理 64 位 `lock_owner`**：
   - 在 `StorageService` 和 `meta.Manager` 中提供 `GetLk`（`FUSE_GETLK = 31`）、`SetLk`（`FUSE_SETLK = 32`）、`SetLkw`（`FUSE_SETLKW = 33`）三个方法，共用 `FileLock` 结构（包含 `uint64 start`、`uint64 end`、`uint32 type`、`uint32 pid`）。
   - 定义与操作系统无关的常量：`F_RDLCK = 0`、`F_WRLCK = 1`、`F_UNLCK = 2`；锁标志 `FuseLkFlock = 0x1`；释放标志 `FuseReleaseFlush = 0x1` 和 `FuseReleaseFlockUnlock = 0x2`；最大字节偏移量 `OffsetMax = math.MaxInt64`（`0x7fffffffffffffff`）。
   - **锁区间与 `lock_owner` 存储**：锁区间为闭区间 `0 <= start <= end <= OffsetMax`（`end == OffsetMax` 表示一直锁到文件末尾，计算相邻区间合并时先判断 `< OffsetMax` 以防加 1 溢出）。因为 Linux 内核生成的 64 位无符号 `lock_owner` 最高位可能是 `1`，Protobuf 里允许完整的 `uint64` 范围，存入 PostgreSQL `BIGINT` 时直接转成 `int64(lock_owner)` 按原二进制位保存。
2. **POSIX 记录锁与 BSD `flock` 分开管理，各自检查权限和合并区间**：
   - **两种锁互不冲突**：`lk_flags == 0`（POSIX 记录锁）和 `lk_flags == FuseLkFlock (0x1)`（BSD `flock`）在检查冲突、查询和释放时各按各的 `lk_flags` 过滤，互不影响。
   - **打开句柄的读写权限检查**：
     - POSIX 记录锁（`lk_flags == 0`）：调 `SetLk` / `SetLkw` 申请读锁 `F_RDLCK` 时，要求句柄 `fh` 必须有读权限（`O_RDONLY` 或 `O_RDWR`）；申请写锁 `F_WRLCK` 时，要求句柄必须有写权限（`O_WRONLY` 或 `O_RDWR`），否则返回 `ErrBadFileHandle`（`EBADF`）；解锁 `F_UNLCK` 和查锁 `GetLk` 只检查 `fh` 存在且属于该文件，不限制读写模式。
     - BSD `flock`（`lk_flags == FuseLkFlock`）：要求锁区间必须是整文件 `start == 0 && end == OffsetMax`（否则返回 `ErrInvalidArgument` / `EINVAL`）；检查 `fh` 属于该文件，但**不检查 `fh` 的读写模式**（允许对只读句柄加排他锁 `F_WRLCK`）。
   - **锁区间拆分合并与锁升级规则**：
     - **POSIX 锁区间拆分与合并**：属于同一个 `(session_id, ino, lock_owner)` 的锁，如果跟别人的锁冲突就直接拒绝（`SetLk` 返回 `ErrAgain`），自己原有的锁保持不变；如果不跟别人冲突，就把自己名下与新区间重叠的旧锁覆盖或拆成左右两段（拆出来的残留段保留原来的 `pid`），并把重叠或首尾紧挨着（`prev.end < OffsetMax && prev.end + 1 == next.start`）且类型相同的锁合并成一条连续记录（合并后的 `pid` 更新为新请求的 `pid`）。
     - **BSD `flock` 先删旧锁再加新锁**：同一个句柄 `(session_id, ino, fh)` 上的 `flock` 在转换锁类型（例如从读锁升级为写锁）时，**先在事务里删掉这个 `(session_id, ino, fh)` 原来持有的旧 `flock` 记录**，再去检查跟其他句柄有没有冲突；如果有冲突，`SetLk` 依然会提交“删掉旧锁”的操作并返回 `ErrAgain`（`EAGAIN`），`SetLkw` 则提交删掉旧锁的操作并进入 `PENDING` 等待队列，与 Linux 内核 `flock_lock_inode` 的行为保持一致。
3. **用 `client_sessions` 表管理客户端会话有效期，加锁写事务统一使用 `READ COMMITTED`**：
   - **新建三张数据库表**：
     - `client_sessions` 表：`session_id BIGINT PRIMARY KEY`、`expires_at TIMESTAMPTZ NOT NULL`、`updated_at TIMESTAMPTZ NOT NULL`。每次锁操作或带 `session_id` 的请求自动延长会话过期时间（默认有效期 60 秒）。
     - `file_locks` 表：保存当前生效的锁，关联 `client_sessions(session_id) ON DELETE CASCADE` 和 `inodes(ino) ON DELETE CASCADE`；其中的 `fh` 字段只用来区分 `flock` 句柄，**不加 `file_handles(fh)` 外键**。
     - `file_lock_waiters` 表：保存 `SetLkw` 正在排队等待的锁请求，状态 `status` 分为 `1 = PENDING（等待中）`、`2 = GRANTED（已拿到锁）`、`3 = CANCELLED（已取消）`，主键为 `(session_id, request_id)`。
   - **加解锁写事务分三步执行（使用 `READ COMMITTED` 隔离级别）**：
     1. 调用 `GetInodeForUpdate(ino)` 锁住目标文件的 `Inode` 行（确认文件存在且不是目录），顺带清理已经过期的 `client_sessions`（数据库外键会自动删掉过期会话名下的锁和等待记录）并续期当前 `session_id`；
     2. 查询当前文件在对应 `lk_flags` 下的有效锁和 `PENDING` 等待队列（因为用了 `READ COMMITTED`，拿到 `Inode` 行锁后再查，一定能读到别人刚刚提交的最新锁状态）；
     3. 判断是否死锁、是否冲突，执行锁的增删改，事务提交后唤醒正在等待的请求。
   - **查锁 `GetLk` 用只读查询**：`GetLk` 不加行锁，只检查文件存在且不是目录、`fh` 有效，并过滤掉已过期会话的锁；如果传入的锁类型是 `F_UNLCK` 则返回 `ErrInvalidArgument`（`EINVAL`）；如果查到多个冲突的锁，按 `ORDER BY start_offset ASC, end_offset ASC, id ASC LIMIT 1` 返回排在最前面的一个冲突锁；没有冲突则返回 `type: F_UNLCK, pid: 0`。
4. **`Flush` 负责清理进程级 POSIX 锁，`Release` 负责销毁句柄并按需清理 `flock`**：
   - **`Flush`（清理 POSIX 锁）**：当 `Flush` 请求带有 `lock_owner` 时，在写事务里锁住 `Inode`、检查 `(ino, fh)` 有效后，按 `(session_id, ino, lock_owner)` 删掉该进程在这个文件上持有的所有 POSIX 记录锁（`lk_flags == 0`）并唤醒等待队列；`Flush` 不会删掉文件句柄 `fh`，也不会动 `flock` 锁。
   - **`Release`（销毁句柄并按标志清理 `flock`）**：在写事务里删掉 `file_handles(fh)`；只有当 `release_flags` 带有 `FuseReleaseFlockUnlock` 标志时，才去删掉这个 `(session_id, ino, fh)` 绑定的 `flock` 锁并唤醒等待队列，绝不误删其他句柄的 POSIX 锁；最后如果文件已被删除（`nlink == 0`）且没有其他打开的句柄了，就把这个孤儿文件彻底从数据库删掉。
5. **`SetLkw` 阻塞等待防残留、检查同文件死锁（`EDEADLK`）与防止写锁一直排不上队**：
   - **记录等待状态，防止请求取消后留下没人要的锁**：`SetLkw` 用 `(session_id, request_id)` 在 `file_lock_waiters` 表里记录状态（`PENDING / GRANTED / CANCELLED`）。每次在 `GetInodeForUpdate(ino)` 事务里准备授锁前，先检查请求有没有被取消（`ctx.Err()`）以及等待记录还在不在；如果已经取消了，就把状态改为 `CANCELLED`（删掉等待记录）并返回 `ErrInterrupted`（`EINTR`），绝不往 `file_locks` 写锁；如果可以授锁，就在同一个事务里同时写进 `file_locks` 并更新等待记录。如果在事务外面等待期间收到了取消信号 `ctx.Done()`，立刻开一个清理事务去把等待记录删掉；万一发现另一个并发事务刚好已经把锁授给它了（`GRANTED`），就把那把刚授的锁一并撤销删掉并唤醒别人，绝不留下没人认领的锁。
   - **检查同一个文件内的死锁（`EDEADLK`）**：当 `SetLkw` 遇到锁冲突需要排队等待时，根据这个文件当前已经持有的锁和正在排队的 `PENDING` 等待记录，检查“谁在等谁释放锁”的依赖链；如果把当前请求加进去之后会形成死循环（比如两个进程拿着读锁同时想升级成写锁，互相等待对方先放手），就立刻拒绝等待并返回 `ErrDeadlock`（`EDEADLK`）。（只检查同一个文件内的死锁，不做跨多个文件的全局死锁检查）。
   - **唤醒机制与防止写锁饿死**：登记 `PENDING` 的短事务提交后立刻释放 `Inode` 行锁，在事务外通过进程内通知结合退避轮询等待锁释放。当一个新的加锁请求进来时，如果这个请求者当前在该文件上**还没有持有任何锁**，而且它想要的锁跟已经在排队的 `PENDING` 等待者有冲突（比如已经有写锁在排队等待了，又新来了一个不相干的读锁），就不允许它插队（`SetLk` 直接返回 `ErrAgain`，`SetLkw` 老老实实排到队尾去），防止排队的写锁永远被源源不断的新读锁插队饿死；但如果请求者手里本来就已经拿着这个文件的锁（比如自己重入、降级或升级），就不拦它，以免造成额外的死锁。
6. **拒绝目录锁请求、不改文件时间戳并补充 3 个错误码映射**：
   - 如果服务端收到针对目录（`S_IFDIR`）的 `GetLk` / `SetLk` / `SetLkw`，统一返回 `ErrIsDir`（`EISDIR`）。已被删除但还打开着的孤儿文件（`nlink == 0`）照常支持所有文件锁操作。
   - `GetLk`、`SetLk`、`SetLkw` 一律不修改 `Inode` 的 `atime`、`mtime` 或 `ctime`。
   - 在 ADR-0011 的错误码列表上增加 3 个锁相关的错误映射：
     - `ErrAgain` $\rightarrow$ `ResourceExhausted` + `reason: "EAGAIN"`
     - `ErrDeadlock` $\rightarrow$ `Aborted` + `reason: "EDEADLK"`
     - `ErrInterrupted` $\rightarrow$ `Canceled` + `reason: "EINTR"`

## 结果
- **正面效果**：完整支持了 `FUSE_GETLK (31)`、`FUSE_SETLK (32)`、`FUSE_SETLKW (33)` 以及关闭文件时的锁清理，准确还原了 POSIX 字节区间锁拆分合并与 BSD `flock` 先删后加的规则；用 `client_sessions` 过期机制解决了客户端死机留下的死锁；用等待状态表和环路检查解决了请求中途取消留下残留锁以及双读锁升级互卡死锁的问题。
- **代价与取舍**：新增了 `client_sessions`、`file_locks` 和 `file_lock_waiters` 三张表，加锁和带锁关闭文件时需要在事务里锁住 `Inode` 行并计算锁区间；不过单个文件上同时存在的锁和等待者数量通常很少，且都有 `(ino, lk_flags)` 索引，查询和计算很快。
