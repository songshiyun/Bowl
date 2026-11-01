# 0024. 服务端 Write 页缓存原子发布、有界世代异步刷盘与 Fsync 强持久化屏障设计

## 状态
已采纳 (accepted) — 演进 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md)、[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 与 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md)

## 背景
在完成 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 与 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 后，阿里云双机内网基准测试（[`benchmark/juicefs/REPORT.md`](../../benchmark/juicefs/REPORT.md)）显示 `juicefs bench (-p 4)` 的大文件并发写（`1 GiB × 4`，单次写入 `1 MiB`）中，JuiceFS 吞吐为 `302.28 ~ 302.49 MiB/s`，而 Bowl 为 `178.59 MiB/s`（约为 JuiceFS 的 `0.59x`）。

根因溯源表明：
1. **每次 `Write(1 MiB)` 强制穿透物理磁盘（等效 `O_SYNC`），卡死在单块云盘 `180 MiB/s` 物理同步写极限**：
   - 服务端挂载的单块 `120 GiB cloud_essd (PL1)` 硬件顺序同步写上限为 `180 MiB/s`（本地 `fio --direct=1 --end_fsync=1` 实测 `168.59 ~ 191.13 MiB/s`，磁盘利用率 `96.29%`）。
   - [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 与 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 要求每次 `Write` 在 `StageBlock` 与 `executeWriteBatch` 中同步阻塞执行 `tmpFile.Sync()`、`SyncDirs()` 以及 PostgreSQL WAL `fdatasync`（写 `4 GiB` 触发 4,096 次文件级 `fsync` 与 4,096 次目录级 `fsync`）。
   - 反观 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md) 与 [ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md) 中的 `Fsync` 与 `FsyncDir`，仅执行只读句柄校验，未承担物理介质落盘屏障职责。
2. **权衡备选方案**：
   - **方案 A（维持每次 `Write` 同步 `fsync`）**：吞吐量被永久锁死在单块云盘的同步写 IOPS 与带宽上限，违背标准 POSIX 将普通 `write(2)`（页缓存可见）与 `fsync(2)`（非易失介质持久化）解耦的契约设计。
   - **方案 B（在 Go 进程堆内存中缓存脏数据块）**：不仅造成 Go GC 压力与 OOM 风险、与 Linux 内核 OS Page Cache 双重缓存浪费内存，还会破坏跨进程/崩溃重启时的内核页缓存接管能力。
   - **方案 C（OS Page Cache 原子发布 + 有界后台世代刷盘 + `Fsync` / `FsyncDir` 强持久化屏障）**：Go 进程堆内存零块驻留，普通 `Write` 完成 `.tmp` 写入与 `os.Rename` 原子发布至内核 OS Page Cache 并提交元数据（配置 `SET LOCAL synchronous_commit = off`），后台按 `64 MiB / 200ms` 有界窗口异步刷盘，并将强持久化屏障收拢至 `Fsync` 与 `FsyncDir`。

## 决策
1. **标准 POSIX 持久化语义对齐与 `Write` 热路径免阻塞发布（演进 ADR-0019 / ADR-0022）**：
   - 在启用延迟同步（`WithDeferredSync`）的 `LocalFSBlockStore` 中，`StageBlock` 完成同目录唯一临时文件 `.tmp_<slice_id>_<block_idx>_*` 的 `Write(data)`、`Close()` 与 `os.Rename(tmpPath, finalPath)` 原子发布至服务端 OS Page Cache，跳过热路径阻塞式 `tmpFile.Sync()` 与阶段 2 的 `SyncDirs()`。
   - **POSIX 实时可见性 100% 守恒**：`Write` 返回 `OK` 时，目标 `Block` 文件已在服务端目录树中就绪且 PostgreSQL 事务已提交；同机或跨机客户端立即发起 `Read`、`GetAttr`、`Lookup` 均直接从服务端 OS Page Cache 与数据库读到最新数据及最新 `size`/`mtime`/`ctime`（含 `S_ISUID`/`S_ISGID` 剥离）。
   - **异常回滚零物理 I/O**：若批次内某任务因 `ErrBadFileHandle` 或 `ErrNoSpace` 被隔离剔除，其 `cleanupBlocks()` 直接从 OS Page Cache 删除目标文件并从待刷账本移除，不触发阻塞式目录 `fsync`。
2. **Go 进程零块驻留与有界世代异步刷盘器（Bounded Background Syncer）**：
   - `StorageService` Go 进程不在堆内存中驻留任何 `Block` 字节载荷，仅在 `LocalFSBlockStore` 中维护轻量级世代账本：`writeGen`（当前写入世代）、`flushedGen`（已落盘世代）、`unflushedBytes`（当前世代累计未刷盘字节数）以及当前世代的 `dirtyFiles` 与去重 `dirtyDirs`。
   - **双阈值有界触发**：每当累计未刷盘写入达到 `64 MiB`（`STORAGE_BLOCK_SYNC_THRESHOLD_BYTES`，默认 `67108864`）或每隔 `200 ms`（`STORAGE_BLOCK_SYNC_INTERVAL`，默认 `200ms`），后台刷盘协程自动切分当前世代并推進 `writeGen++`，在锁外优先通过 Linux `unix.Syncfs(fd)`（或在自定义钩子/非 Linux 环境下并发同步 `dirtyFiles` 与 `dirtyDirs`）完成物理落盘，随后更新 `flushedGen` 并广播唤醒等待队列。
3. **`Fsync` 与 `FsyncDir` 强持久化屏障（演进 ADR-0009 / ADR-0013）**：
   - `Fsync` 与 `FsyncDir` 在完成原有的 `Inode` 类型与活跃 `FileHandle` 归属校验后，调用 `StagedBlockStore.SyncPending(ctx)`：快照当前 `targetGen := writeGen`，若 `flushedGen < targetGen` 则立即触发并阻塞等待世代 `<= targetGen` 的全部脏块与目录项完成物理落盘（并发 `Fsync` 共享同一世代批次 Singleflight 合并等待）。
   - 若底层物理同步失败，向上层返回 `ErrIO`（映射为 `EIO`）。
4. **PostgreSQL 异步提交协同与「物理块先落盘、WAL 后落盘」顺序不变量**：
   - 为杜绝整机断电时「PostgreSQL 元数据已落盘但物理 `Block` 仍在内存」的悬空引用（Dangling Slice），当启用延迟同步时，`Write` 的数据库事务内执行 `SET LOCAL synchronous_commit = off`，使 WAL 写入缓冲区即返回而不阻塞等待 WAL `fdatasync`。
   - 在后台刷盘器完成物理块同步后，以及在 `Fsync` / `FsyncDir` 完成 `BlockStore.SyncPending(ctx)` 屏障后，触发 PostgreSQL WAL 刷盘屏障（`SELECT pg_current_wal_flush_lsn()`），严格保证「物理 `Block` 先于或同于元数据 WAL 落盘」的崩溃一致性顺序。

## 结果
- **积极影响**：
  - 彻底解除普通 `Write(1 MiB)` 每次穿透云盘物理 `fsync` 的 `180 MiB/s` 硬上限，在保持客户端 100% 同步直通、服务端 Go 堆内存零块驻留、`pjdfstest` 全量 POSIX 兼容性与配额/会话安全不变量的前提下，使多流大文件并发写充分利用 OS Page Cache 与批量刷盘带宽。
  - 将未刷盘脏页严格收敛在 `64 MiB / 200ms` 有界窗口内，并通过 `Fsync` / `FsyncDir` 强屏障与 PG `synchronous_commit = off` + WAL 刷盘屏障闭环崩溃一致性。
- **代价考量**：
  - 整机突发断电（非进程重启）时，未调用 `Fsync` 的最近 `< 200ms`（或 `< 64 MiB`）写入会与对应的异步提交 PG 事务一同回滚（符合标准 POSIX 未 `fsync` 写入的掉电语义），且不会产生损坏的悬空 `slice_id` 元数据。
