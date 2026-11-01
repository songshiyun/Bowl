# 0026. 物理块服务端内核缓存级落盘（Page Cache）与 Fsync 仅刷数据库 WAL

## 状态
已采纳 (accepted) — 演进 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)、[ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 与 [ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md)

## 背景
在完成 [ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md) 后，普通 `Write` 请求虽然改为先将物理 `Block` 写入服务端操作系统的页缓存（Page Cache）并以异步提交（`SET LOCAL synchronous_commit = off`）更新 PostgreSQL 元数据，但在最新 Benchmark 压测中仍然暴露出三个由物理块显式刷盘引发的性能瓶颈：
1. **大文件写完后首次 `Fsync` 触发数 GiB 脏页集中刷盘，造成数秒阻塞与 RPC 超时**：[ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md) 在 `Fsync` / `FsyncDir` 中对整个数据目录调用 `unix.Syncfs(fd)`（或逐个调用文件与目录 `Sync`）。当客户端连续写入数 GiB 大文件并在关闭或同步时调用 `fsync(2)`，服务端 `syncfs` 会阻塞等待内核缓存中累积的数 GiB 脏块一次性全部刷入云盘（在单盘 `180 MiB/s` 上限下耗时长达十余秒），直接导致 `Fsync` 卡死甚至触发客户端 RPC 超时。
2. **全盘 `syncfs` 对并发小文件写入和 SQLite 事务造成严重的尾延迟连坐**：`syncfs` 作用于服务端块存储所在的文件系统全局。哪怕某个客户端只是对一个几十字节的小文件或 SQLite 数据库日志执行 `fsync`，只要同一时刻有其他并发流刚写入了大量数据块，该小文件的 `Fsync` 就会被迫连坐等待全盘脏页回写完成，并且全盘刷盘瞬间打满底层磁盘 I/O 队列，拖慢同机 PostgreSQL WAL 写入与并发请求响应。
3. **后台 `Compaction` 合并块与首次建目录仍残留同步 `fsync` 抖动**：`LocalFSBlockStore.PutBlock`（被后台 `Compaction` 调用）在写完新块后仍逐个执行 `syncBlockFile` 与 `SyncDirs`，且首次创建两级散列目录时也会对父目录执行 `syncDir`，在后台合并碎片期间持续向磁盘注入同步刷盘毛刺。

对比参考项目 JuiceFS + 对象存储服务端的持久化水位：客户端写入与 `fsync` 只需保证数据已经可靠送达远端存储服务并完成元数据持久化，远端存储节点自身的本地磁盘落盘完全交由操作系统内核后台调度，无需在客户端 `Fsync` 路径上对服务端底层块目录执行全盘物理 `syncfs`。

## 决策
1. **物理块写入统一为「服务端内核 Page Cache 进程级落盘」，移除块存储显式 `fsync` / `syncfs`（演进 ADR-0019 / ADR-0022 / ADR-0024）**：
   - **写入路径直通服务端内核缓存**：客户端 `Write` 直通服务端，服务端 `LocalFSBlockStore.PutBlock` 自动确保两级散列目录存在（`os.MkdirAll` / 按需创建并缓存已存在目录，不再对目录执行 `f.Sync()`），将数据写入同目录唯一临时文件 `.tmp_<slice_id>_<block_idx>_*`，执行 `Close()` 后通过 `os.Rename(tmpPath, finalPath)` 原子重命名到目标路径。
   - **进入内核 Page Cache 即视为存储层写入完成**：只要物理 `Block` 文件通过原子重命名进入服务端操作系统 Page Cache，即视为块存储层持久化完成；前台 `Write` 与后台 `Compaction` 统一调用 `PutBlock`，不再执行单文件 `fsync`、目录 `fsync` 或全盘 `syncfs`。后续向物理磁盘的脏页回写与限流（`balance_dirty_pages`）完全交由 Linux 内核在后台调度。
   - **清理冗余阶段化刷盘接口与状态**：移除 `StagedBlockStore` 接口（`StageBlock`、`SyncDirs`、`SyncPending`）、`block_store_syncfs_*.go` 系统调用封装以及 `LocalFSBlockStore` 中用于追踪未刷盘文件与目录的 `pendingFiles`、`pendingDirs`、`pendingSyncDirs` 状态。
   - **数据库元数据保持异步提交**：普通 `Write` 的 PostgreSQL 事务继续使用 `SET LOCAL synchronous_commit = off` 异步提交，其余低频目录树与节点元数据操作保持默认事务提交行为。
2. **同步路径（`Fsync` / `FsyncDir`）仅校验句柄并触发数据库元数据 WAL 刷盘（演进 ADR-0024）**：
   - 客户端调用 `Fsync` 或 `FsyncDir` 时，服务端先在只读事务中校验目标 `Inode` 类型与活跃 `FileHandle` 归属合法性。
   - 校验通过后，服务端不再对块存储目录执行任何物理刷盘（不调 `syncfs` 或文件 `fsync`），仅调用 `SELECT pg_current_wal_flush_lsn()` 触发 PostgreSQL WAL 刷盘，确保此前所有异步提交的元数据事务已在数据库层持久化落盘。

## 结果
- **收益**：
  - 彻底消除大文件写入后调用 `Fsync` 因集中刷盘数 GiB 脏页导致的数秒阻塞与 RPC 超时。
  - 彻底避免全盘 `syncfs` 对并发小文件写入、目录操作和 SQLite 事务造成的尾延迟连坐，同时消除后台 `Compaction` 的同步刷盘毛刺，对齐 JuiceFS + 对象存储服务端的持久化水位。
  - 大幅简化 `LocalFSBlockStore` 实现，去除 `StagedBlockStore`、跨平台 `syncfs` 分支与脏文件世代追踪锁。
- **边界与取舍**：
  - **完整的进程级容灾与跨客户端强一致可见性**：存储服务（`StorageService`）或数据库进程崩溃、OOM、重启均不丢数据；数据写入返回后，同机或跨机客户端立即可通过服务端内核 Page Cache 与 PostgreSQL 读到最新数据与属性。
  - **整机突发断电边界**：仅当服务端整机发生突发硬断电或宿主机内核 Panic 时，可能丢失服务端操作系统 Page Cache 中最后几秒尚未被 Linux 内核后台线程回写到物理磁盘的数据块。
