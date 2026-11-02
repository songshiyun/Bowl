# 0028. 单语句原子 CTE、内联异步 WAL 提交、退役 WriteThread 与智能内核缓存保留

## 状态
已采纳 (accepted) — 演进 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0014](0014-disk-service-and-quota-ledger.md)、[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)、[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)、[ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md)、[ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md)、[ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md)、[ADR-0026](0026-page-cache-block-persistence-and-wal-only-fsync.md) 与 [ADR-0027](0027-4mib-chunk-boundary-and-single-worker-compaction-gc.md)

## 背景
Bowl 的核心架构定位是**强一致「多写多读（MWMR）」无状态直通 + 服务端权威裁决**：客户端不在本地做破坏跨机一致性的脏写缓冲或伪造句柄，所有文件生命周期 RPC（`Lookup -> Create -> Write -> Flush -> Release`）实时直通服务端与 PostgreSQL。在同可用区 VPC 内网下，5 次 RPC 的纯网络开销不足 `0.5 ms`。然而在当前实现中，创建一个 `128 KiB` 小文件虽然只有 5 次客户端 RPC，却在服务端内部触发了 **23 次数据库网络往返 + 3 个独立显式写事务（含 2 次云盘物理 WAL `fsync` 阻塞）**：

1. **`Create` 耗费 8 次数据库往返且持锁等待云盘刷 WAL**：开启显式事务（`BEGIN`）$\rightarrow$ `FOR UPDATE` 锁死父目录 $\rightarrow$ 串行发送 5 条 SQL（查会话、插 `inodes`、插 `entries`、改父目录时间戳、插 `file_handles`）$\rightarrow$ `COMMIT` 默认 `synchronous_commit = on` 阻塞等待云盘物理刷 WAL 后才释放父目录行锁，导致同目录并发建文件在父目录行锁上严重排队。
2. **`Write` 耗费 8 次数据库往返，且 Go 层 `WriteThread` 排队与单独发送 `SET LOCAL` 得不偿失**：阶段 1 写完物理块后，阶段 2 开启显式事务 $\rightarrow$ 单独发送一条 `SET LOCAL synchronous_commit = off`（仅为了关闭同步提交就多耗费 1 次数据库往返）$\rightarrow$ 串行执行锁 `Inode`、查句柄、追加 `chunks.slices`、插 `slice_refs`、更新 `Inode` 属性 $\rightarrow$ `COMMIT`。同时，[ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 引入的 Go 层 `WriteThread` 批次聚合队列在引入内存配额账本与异步 WAL 后已无存在价值（PostgreSQL 自身的 `walwriter` 已在内存/WAL 层完成批量刷盘，不同文件写入无全局锁冲突，同文件写入由 PG 行锁天然保证串行安全），反而增加了队列调度复杂度和双路径维护包袱。
3. **`Release` 关闭常规文件耗费 5 次数据库往返且阻塞等待刷 WAL**：即便关闭未解链（`nlink > 0`）且未加锁的普通文件，也要开启显式写事务分步查 `Inode`、删句柄、复核 `nlink` 并等待同步 `COMMIT` 刷盘。
4. **小文件重复读与目录遍历每次 `open()` 强制作废内核缓存**：默认固定关闭 `FOPEN_KEEP_CACHE` 与 `FOPEN_CACHE_DIR`，导致哪怕文件或目录根本没有发生任何修改，客户端每次 `open()` 也会无脑清空 Linux 内核 Page Cache。

## 决策

### 1. 多步显式交互事务收敛为单语句隐式事务原子 CTE，彻底删除旧多步事务代码
利用 PostgreSQL「单条 SQL 语句天然具备 ACID 隐式事务」的内核特性，将 `Create`、`Write`、`Release` 定义为 `metadata.sql` / `chunks.sql` 中的单语句原子 `WITH (CTE)` 查询，通过 `sqlc` 与 `mockery` 生成至 `db.Querier` / `db.Store`，由生产代码直接无显式事务包装调用（彻底省去 `BEGIN` 与 `COMMIT` 往返，并删除原有的多步 `ExecTxWithOptions` 实现）：
- **`Create` 单语句原子 CTE（8 次往返 $\rightarrow$ 1 次往返）**：
  - 单条 CTE 内联完成：`FOR UPDATE` 锁定父目录行 $\rightarrow$ 校验父目录存在、类型为目录（`S_IFDIR`）、非孤儿（`nlink > 0`）且目标名非 `.`/`..` $\rightarrow$ 校验 `client_sessions` 租约有效且归属目标磁盘 $\rightarrow$ 插入 `inodes`（自动处理父目录 `S_ISGID` 属组继承）$\rightarrow$ 插入 `entries` $\rightarrow$ 更新父目录 `mtime/ctime` $\rightarrow$ 插入 `file_handles` $\rightarrow$ 一次性返回诊断状态列、新节点与新句柄。
  - 同名冲突依然由 PostgreSQL `23505` 唯一约束捕获并映射为 `ErrAlreadyExists`（`EEXIST`）；其余校验失败通过诊断状态列在 1 次往返内精确还原 `ErrNotFound`、`ErrNotDir`、`ErrBadFileHandle`，父目录锁持有时间从“6 次网络往返 + 云盘 I/O（数毫秒）”缩短为“PG 进程内单次内存执行（百微秒）”。
- **`Write` 单语句原子 CTE 与彻底退役 `WriteThread`（8 次往返 $\rightarrow$ 1 次往返）**：
  - **彻底移除 `WriteThread`**：删除 `manager.go` 与 `manager_write.go` 中的 `writePendingQueue`、`writeGroupMu`、`writeLeaderActive`、`writeCoalesceCh`、`writeInFlightStage1`、`WithWriteGroupMaxBatchSize` 及 `runWriteLeaderLoop` / `executeWriteBatch` / `executeBatchWriteTasks` 等全部队列与批次聚合代码。
  - **阶段 1（保持不变）**：从内存号段分配器领取 `slice_id`，调用 `BlockStore.PutBlock` 将数据写入服务端 OS Page Cache。
  - **阶段 2（各协程并发直发单条原子 CTE，统一覆盖 `O_APPEND` 与跨 `4 MiB Chunk` 边界）**：
    - 调用前从内存配额账本读取目标磁盘当前剩余可用字节 `avail_bytes = max_bytes - used_bytes`（无配额限制时传 `math.MaxInt64`）作为参数 `@quota_avail_bytes` 传入 CTE，**不搞「内存预占 `dataLen` 再退差额」的双轨分支**；
    - CTE 内部 `FOR UPDATE` 锁定目标 `inodes` 行，关联校验 `file_handles` 与 `client_sessions` 写权限；
    - 在 SQL 内直接根据 `O_APPEND` 标志确定真实写入偏移 `actual_offset = CASE WHEN (fh.flags & 1024) <> 0 THEN li.size ELSE @req_offset END`，计算 `new_size = GREATEST(li.size, actual_offset + @data_len)` 与真实扩容增量 `delta_bytes = GREATEST(0, new_size - li.size)`；
    - 直接在 SQL 内校验 `delta_bytes <= @quota_avail_bytes`：若超限则不执行任何数据修改并返回 `quota_exceeded = true`，Go 侧映射为 `ErrNoSpace`（`ENOSPC`）并立即调用 `cleanupBlocks()` 删除阶段 1 物理块；
    - 若校验通过，在同条 CTE 内根据 `actual_offset` 与 `data_len` 计算 `start_indx` 与 `end_indx`：不跨 `4 MiB` 边界时追加 1 行 `chunks` 且 `slice_refs.refs = 1`，跨 `4 MiB` 边界时拆分追加相邻 2 行 `chunks` 且 `slice_refs.refs = 2`，同时更新 `inodes` 的 `size`、剥离 `S_ISUID | S_ISGID` 并刷新 `mtime/ctime`，返回 `(old_size, new_size, root_ino, chunk_octet_lens)`；
    - Go 侧拿到成功结果后，直接将真实增量 `new_size - old_size` 累加至内存配额账本 `usedBytes`，并按返回的 `chunk_octet_lens` 触发异步 `Compaction`。
- **`Release` 常规文件单语句原子 CTE（5 次往返 $\rightarrow$ 1 次往返）**：
  - 针对未置位 `FuseReleaseFlockUnlock` 且无 POSIX 锁活动的常规文件，直接执行 1 条原子 CTE：读取目标 `Inode` 状态 $\rightarrow$ 当非目录且 `nlink > 0`（非孤儿）时，内联校验会话有效性并删除 `file_handles` 及关联 `file_lock_waiters` $\rightarrow$ 同语句返回节点状态与最新 `nlink`。
  - **孤儿回收安全兜底**：若目标文件在关闭前已被 `Unlink` / `Rename` 解链为孤儿（`nlink == 0`），CTE 保持句柄不删并立即返回 `nlink == 0` 信号，由服务端无缝转入 `GetInodeForUpdate` 孤儿节点与物理块回收事务，确保 Open-Unlink 语义与空间配额回收 100% 严密。

### 2. 全链路元数据内联异步 WAL 提交 + `Fsync` 统一持久化屏障
- **连接池保持默认 `synchronous_commit = on`**：保障 `CreateDisk`、`UpdateDisk`、`DeleteDisk` 等低频控制面操作的强持久化。
- **数据面高频写操作内联 `set_config('synchronous_commit', 'off', true)`**：废除在事务内单独发送 `SET LOCAL synchronous_commit = off` 语句的做法；在 `Create`、`Write`、`Release` 等高频写操作中，直接在单条 CTE 内部内联调用 `set_config('synchronous_commit', 'off', true)`，实现零额外网络往返的事务级异步 WAL 提交（写入 PostgreSQL Shared Buffers 与 WAL Buffer 即返回，跨客户端立即可见，由 PG 后台 `walwriter` 按周期批量刷盘）。
- **`Fsync` / `FsyncDir` 统一持久化屏障**：保持 [ADR-0026](0026-page-cache-block-persistence-and-wal-only-fsync.md) 契约，当客户端显式调用 `Fsync` 或 `FsyncDir` 时，服务端通过 `SELECT pg_current_wal_flush_lsn()` 触发全局 WAL 物理落盘屏障。

### 3. 基于版本比对智能启用内核缓存（`FOPEN_KEEP_CACHE` 与 `FOPEN_CACHE_DIR`），不引入服务端内存句柄缓存
- **不维护服务端内存句柄/会话镜像缓存**：既然 `Create`、`Write`、`Release` 已收敛为单语句 CTE，`Flush` 与 `Read` 也已是单语句查询，不在 Go 内存中额外维护 `file_handles` / `client_sessions` 快照缓存，彻底规避“数据库 + 内存双状态同步失效”的一致性复杂度。
- **按 `(mtime, size)` 与目录变更序列智能启用内核缓存**：
  - 记录每个 `Inode` 最近一次已知的 `(mtime, size)` 版本；当再次 `Open` 时，若该文件的 `(mtime, size)` 与上次已知版本一致（未发生外部修改），或显式开启了 `STORAGE_OPEN_KEEP_CACHE`，则在 `OpenResult.OpenFlags` 中置位 `FOPEN_KEEP_CACHE` 保留客户端内核 Page Cache；一旦发现 `(mtime, size)` 发生变化，则不置位 `FOPEN_KEEP_CACHE` 以指示内核自动作废陈旧页缓存，并更新已知版本。
  - 在 `OpenDir` 中，支持根据配置或目录变更状态返回 `FOPEN_CACHE_DIR`，加速重复目录遍历。

## 结果
- **正面效果**：
  - 单文件 5 步完整生命周期（`Lookup -> Create -> Write -> Flush -> Release`）的数据库交互从 **23 次往返压缩至 5 次往返**，消除非 `Fsync` 路径的物理云盘 WAL 刷盘等待；4 并发实测下，`Create` 吞吐提升 **6.29x**（`592.4 -> 3,724.5 files/s`），`Write` 阶段 2 提升 **6.26x**（`1,212.9 -> 7,588.6 ops/s`），`Release` 提升 **4.63x**（`1,932.7 -> 8,958.1 ops/s`），完整 5 步生命周期吞吐提升 **4.22x**（`383.5 -> 1,620.0 files/s`）。
  - 彻底清空 `WriteThread` 队列、多步显式事务旧代码与配额预占回退双轨制，代码体积与并发状态机复杂度大幅下降。
  - 智能启用 `FOPEN_KEEP_CACHE` 消除文件未修改时重复 `open()` 导致的内核页缓存无脑清空，同时 100% 守住跨客户端修改后的缓存作废强一致性。
- **代价与取舍**：
  - `Create`、`Write`、`Release` 的核心校验与状态流转内聚到 PostgreSQL CTE SQL 中，需要通过 `dbmock` 契约单测与真实 PostgreSQL 集成测试共同守护全部错误码分支与边界条件。
