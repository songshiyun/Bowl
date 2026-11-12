# 0028. 单语句原子 CTE、全数据面异步 WAL、Read 单查询合并与客户端无锁 Flush 短路

## 状态
部分被 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md)、[ADR-0031](0031-shardstore-append-only-pack-storage-and-256kib-block-grid.md) 与 [ADR-0033](0033-client-write-behind-multi-window-readahead-and-local-dir-synthesis.md) 替代 — PostgreSQL CTE 与同步直通写已被 `StoreActor` Pebble 引擎及客户端 `Write-Behind` 取代；客户端非截断 `Open`/`Create` 置位 `FOPEN_KEEP_CACHE` 与 `Flush` 免物理 `Fsync` 策略保留

## 背景
Bowl 的核心架构定位是**强一致「多写多读（MWMR）」无状态直通 + 服务端权威裁决**：客户端不在本地做破坏跨机一致性的脏写缓冲或伪造句柄，所有文件生命周期 RPC 实时直通服务端与 PostgreSQL。在同可用区 VPC 内网下，单次 RPC 的纯网络开销不足 `0.1 ms`。然而在演进前实现中，小文件读写全链路在服务端与客户端存在以下性能瓶颈：

1. **`Create`、`Open`、`OpenDir`、`Release`、`ReleaseDir` 採用多步显式写事务且阻塞等待云盘刷 WAL**：
   - `Create` 开启显式事务（`BEGIN`）$\rightarrow$ `FOR UPDATE` 锁死父目录 $\rightarrow$ 串行发送 5 条 SQL $\rightarrow$ `COMMIT` 默认 `synchronous_commit = on` 阻塞等待云盘物理刷 WAL 后才释放父目录行锁，耗费 8 次数据库往返。
   - `Open`（含只读 `O_RDONLY`）与 `OpenDir` 每次打开都开启 5 步显式写事务且默认同步刷 WAL；在前序异步写入产生大量待刷 WAL 与系统脏页期间，每次只读 `Open` 的 `COMMIT` 都被迫排队等待云盘完成物理 WAL `fsync`，将单次打开延迟拉高至数十毫秒。
   - `Release` 与 `ReleaseDir` 关闭未解链（`nlink > 0`）常规节点时同样开启 5 步显式写事务并等待同步 `COMMIT` 刷盘。
   - 其他数据面元数据写事务（`Mkdir`、`Unlink`、`Rename`、`Rmdir`、`SetAttr`、`Mknod`、`Symlink`、`Link`、`Xattr`、`FileLock` 及 `Open(O_TRUNC)` 等）走 `execWriteTxWithRetry` 时同样受默认 `synchronous_commit = on` 拖累。
2. **`Write` 耗费 8 次数据库往返，且 Go 层 `WriteThread` 排队与单独发送 `SET LOCAL` 得不偿失**：阶段 1 写完物理块后，阶段 2 开启显式事务 $\rightarrow$ 单独发送 `SET LOCAL synchronous_commit = off` $\rightarrow$ 串行执行锁 `Inode`、查句柄、追加 `chunks.slices`、插 `slice_refs`、更新 `Inode` 属性 $\rightarrow$ `COMMIT`。同时，[ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 引入的 Go 层 `WriteThread` 批次聚合队列在引入内存配额账本与异步 WAL 后已无存在价值，反而增加了队列调度复杂度和双路径维护包袱。
3. **`Read` 在服务端串行执行 2 次数据库查询并残留旧版事务死代码**：`readWithPool` 先执行 `GetInodeSizeAndChunksInRange` 查询节点属性与 Chunk 切片，返回后再串行执行 `GetFileHandle` 校验句柄权限与会话租约，白白增加一倍数据库往返；同时 `manager_read.go` 中还残留着仅供旧单测使用的 `execWriteTxWithRetry + LockValidSliceRefsKeyShare` 历史分支。
4. **未加锁文件在 `close()` 时仍发起跨机 `Flush` RPC，且重复 `open()` 强制作废内核缓存**：
   - 客户端 `RawFS.Flush` 未实装 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 的无锁本地短路，导致 99.99% 从未调用过 `SetLk` / `SetLkw` 的普通文件在 `close(fd)` 时都要多跑一趟跨机 `Flush` RPC + 服务端 4 表关联查询。
   - 默认固定关闭 `FOPEN_KEEP_CACHE` 与 `FOPEN_CACHE_DIR`，导致文件或目录未发生任何修改时，客户端每次 `open()` 也会无脑清空 Linux 内核 Page Cache。

## 决策

### 1. 句柄与读写热路径全面收敛为单语句 SQL（原子写 CTE + 合并只读查询），彻底删除历史慢路径死代码
利用 PostgreSQL「单条 SQL 语句天然具备 ACID 隐式事务与一致性快照」的内核特性，将高频写路径定义为单语句原子 `WITH (CTE)` 查询，将 `Read` 合并为单条只读查询，由生产与单测代码统一调用：
- **`Create` 单语句原子 CTE（8 次往返 $\rightarrow$ 1 次往返）**：
  - 单条 CTE 内联完成：`FOR UPDATE` 锁定父目录行 $\rightarrow$ 校验父目录存在、类型为目录（`S_IFDIR`）、非孤儿（`nlink > 0`）且目标名非 `.`/`..` $\rightarrow$ 校验 `client_sessions` 租约有效且归属目标磁盘 $\rightarrow$ 插入 `inodes`（自动处理父目录 `S_ISGID` 属组继承）$\rightarrow$ 插入 `entries` $\rightarrow$ 更新父目录 `mtime/ctime` $\rightarrow$ 插入 `file_handles` $\rightarrow$ 一次性返回诊断状态列、新节点与新句柄。
  - 同名冲突依然由 PostgreSQL `23505` 唯一约束捕获并映射为 `ErrAlreadyExists`（`EEXIST`）；其余校验失败通过诊断状态列在 1 次往返内精确还原 `ErrNotFound`、`ErrNotDir`、`ErrBadFileHandle`。
- **`OpenFileFast` 与 `OpenDirFast` 单语句原子 CTE（5 次往返 $\rightarrow$ 1 次往返）**：
  - 针对非截断 `Open`（不含 `O_TRUNC`）与 `OpenDir`，在单条 CTE 内读取目标 `inodes` 行（不加 `FOR SHARE` 锁，仅靠插入 `file_handles` 时外键自动获取最轻量的 `FOR KEY SHARE`），直接通过 `nlink > 0 OR EXISTS (SELECT 1 FROM file_handles fh WHERE fh.ino = ...)` 闭环覆盖正常节点与 Open-Unlink 孤儿节点存活校验，同时内联校验 `client_sessions` 租约有效性并插入 `file_handles`。
  - 彻底删除 `OpenDir` 与非截断 `Open` 的多步交互事务慢路径代码；仅当 `Open` 携带 `O_TRUNC` 需要截断数据块与扣减配额时才走写事务路径。
- **`Write` 单语句原子 CTE 与彻底退役 `WriteThread`（8 次往返 $\rightarrow$ 1 次往返）**：
  - **彻底移除 `WriteThread`**：删除 `manager.go` 与 `manager_write.go` 中的全部队列与批次聚合代码。
  - **阶段 1（保持不变）**：从内存号段分配器领取 `slice_id`，调用 `BlockStore.PutBlock` 将数据写入服务端 OS Page Cache。
  - **阶段 2（各协程并发直发单条原子 CTE，统一覆盖 `O_APPEND` 与跨 `4 MiB Chunk` 边界）**：
    - 调用前从内存配额账本读取目标磁盘当前剩余可用字节 `avail_bytes = max_bytes - used_bytes` 作为参数 `@quota_avail_bytes` 传入 CTE；
    - CTE 内部 `FOR UPDATE` 锁定目标 `inodes` 行，关联校验 `file_handles` 与 `client_sessions` 写权限，根据 `O_APPEND` 计算真实偏移 `actual_offset`、`new_size` 与扩容增量 `delta_bytes`；
    - 若 `delta_bytes > @quota_avail_bytes` 则返回 `quota_exceeded = true`，Go 侧映射为 `ErrNoSpace`（`ENOSPC`）并立即调用 `cleanupBlocks()` 删除阶段 1 物理块；
    - 若校验通过，在同条 CTE 内追加 1 行或跨 `4 MiB` 边界拆分追加 2 行 `chunks` 及 `slice_refs`，更新 `inodes` 的 `size`、剥离 `S_ISUID | S_ISGID` 并刷新 `mtime/ctime`，Go 侧将真实增量 `new_size - old_size` 累加至内存配额账本并按需触发异步 `Compaction`。
- **`ReleaseFileFast` 与 `ReleaseDirFast` 单语句原子 CTE（5 次往返 $\rightarrow$ 1 次往返）**：
  - 针对未置位 `FuseReleaseFlockUnlock` 且无 POSIX 锁活动的常规文件 `Release` 以及目录 `ReleaseDir`，直接执行 1 条原子 CTE：当节点类型匹配且 `nlink > 0`（非孤儿）时，内联校验会话有效性并删除 `file_handles`（文件释放同步清理关联 `file_lock_waiters`），同语句返回节点状态与最新 `nlink`。
  - **孤儿回收安全兜底**：若目标文件或目录在关闭前已被 `Unlink` / `Rmdir` / `Rename` 解链为孤儿（`nlink == 0`），CTE 保持句柄不删并返回孤儿信号，由服务端无缝转入 `GetInodeForUpdate` 孤儿节点与物理块回收事务。
- **`Read` 合并为单条只读 SQL（`GetInodeChunksAndHandleForRead`，2 次往返 $\rightarrow$ 1 次往返）**：
  - 将 `GetInodeSizeAndChunksInRange` 与 `GetFileHandle`（含 `client_sessions` 租约校验）合并为单条只读 SQL 查询 `GetInodeChunksAndHandleForRead`，通过左连接一次性返回 Inode 存在性、`mode`、`size`、时间戳、句柄有效性与标志位以及 `[start_indx, end_indx]` 区间的 Chunk 切片列表，在 Go 侧严格保持 `ENOENT` $\rightarrow$ `EISDIR` / `EINVAL` $\rightarrow$ `EBADF` 错误码优先级（并发 `Compaction` + `GC` 删块重试 `attempt > 0` 时继续复用仅查切片的 `GetInodeSizeAndChunksInRange`）。
  - 彻底删除 `manager_read.go` 中仅供旧单测使用的 `execWriteTxWithRetry + LockValidSliceRefsKeyShare` 历史死代码分支，让生产与单测统一走无锁单 SQL 路径。

### 2. 全数据面统一启用异步 WAL（`synchronous_commit = off`） + 控制面与 `Fsync` 物理刷盘屏障
- **数据面写操作全面开启异步 WAL 提交**：
  - 所有单语句写 CTE（`CreateFile`、`OpenFileFast`、`OpenDirFast`、`WriteFileAtomic`、`ReleaseFileFast`、`ReleaseDirFast` 等）均在 SQL 内联调用 `set_config('synchronous_commit', 'off', true)`；
  - 在数据面通用写事务入口 `execWriteTxWithRetry` 中统一设置事务级 `synchronous_commit = off`，使所有数据面多步写事务（含 `Open(O_TRUNC)`、孤儿回收慢路径、`Mkdir`、`Unlink`、`Rename`、`Rmdir`、`SetAttr`、`Mknod`、`Symlink`、`Link`、`Xattr`、`FileLock`、`RenewSession`、`CloseSession` 等）全部享受内存级 WAL 提交（写入 PostgreSQL Shared Buffers 与 WAL Buffer 即返回，跨客户端立即可见，由 PG 后台 `walwriter` 批量刷盘），彻底消除数据面非 `Fsync` 操作的云盘物理 `fsync` 阻塞。
- **控制面与 `Fsync` / `FsyncDir` 保持强持久化屏障**：
  - 低频控制面磁盘生命周期操作（`CreateDisk`、`UpdateDisk`、`DeleteDisk`）独立保持同步 WAL 提交（`synchronous_commit = on`）；
  - 当客户端显式调用 `Fsync` 或 `FsyncDir` 时，服务端通过 `SELECT pg_current_wal_flush_lsn()` 触发全局 WAL 物理落盘屏障（遵循 [ADR-0026](0026-page-cache-block-persistence-and-wal-only-fsync.md)）。

### 3. 客户端 `Flush` 无锁本地短路 + 基于版本比对智能启用内核缓存
- **客户端 `Flush` 落实无锁本地短路（遵循 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 决策 2）**：
  - 客户端 `RawFS` 按服务端 `ino` 维护 POSIX 记录锁（`lkFlags == 0`）活动集合：在 `SetLk` / `SetLkw` 通过本地句柄校验后、**发起 RPC 之前**立即将该服务端 `ino` 标记为存在 POSIX 锁活动（确保在途阻塞 `SetLkw` 或超时重试期间关闭其他描述符也不会漏发 `Flush`），并在会话过期重建清空本地状态时一并清空；
  - 当目标服务端 `ino` 未标记过 POSIX 记录锁活动时，`RawFS.Flush` 在完成本地 `ToServerIno` 与 `ResolveHandle` 校验后直接返回 `fuse.OK`，不再发起跨机 `Flush` RPC；一旦该 `ino` 调过 POSIX `SetLk` / `SetLkw`，后续任意关联句柄的 `Flush` 均严格直通服务端事务释放该 `lock_owner` 持有的 POSIX 锁与等待队列。
- **基于 `(mtime, size)` 与目录变更序列智能启用内核缓存（`FOPEN_KEEP_CACHE` 与 `FOPEN_CACHE_DIR`）**：
  - 不维护服务端内存句柄/会话镜像缓存，规避双状态同步失效复杂度；
  - 记录每个 `Inode` 最近一次已知的 `(mtime, size)` 版本：再次 `Open` 时若 `(mtime, size)` 未变（或显式开启 `STORAGE_OPEN_KEEP_CACHE`），则在 `OpenResult.OpenFlags` 中置位 `FOPEN_KEEP_CACHE` 保留客户端内核 Page Cache；一旦发现 `(mtime, size)` 变化则不置位以指示内核作废陈旧页缓存；`OpenDir` 同样支持返回 `FOPEN_CACHE_DIR` 加速重复目录遍历。

## 结果
- **正面效果**：
  - **小文件写全链路**：单文件 5 步写生命周期（`Lookup -> Create -> Write -> Flush -> Release`）从 **5 次 RPC + 23 次 PG 往返 + 2 次云盘 WAL `fsync`** 压缩至 **4 次 RPC（无锁 `Flush` 本地短路） + 4 次 PG 往返 + 0 物理刷盘等待**。
  - **小文件读全链路**：单文件读生命周期（`Open -> Read -> Flush -> Release`）从 **4 次 RPC + 9 次 PG 往返 + 1 次云盘 WAL `fsync` 阻塞** 压缩至 **3 次 RPC + 3 次 PG 往返 + 0 物理刷盘等待**。
  - **目录遍历与常规元数据操作**：`OpenDir -> ReadDir/ReadDirPlus -> ReleaseDir` 及 `Mkdir` / `Unlink` / `Rename` 等全部摆脱云盘 WAL 同步刷盘阻塞，同时彻底清除 `WriteThread` 与 `manager_read.go` 旧事务死代码。
- **代价与取舍**：
  - 核心校验与状态流转内聚到 PostgreSQL 单语句 CTE 与合并查询中，通过 `dbmock` 契约单测与真实 PostgreSQL 集成测试共同守护全部错误码优先级与并发边界。
