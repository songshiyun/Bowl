# 0023. 全路径会话续期内存节流、无锁 Flush/Release 去排他锁、Lookup 单语句直查与 Read 预读流水线设计

## 状态
已采纳 (accepted) — 演进 [ADR-0003](0003-posix-lookup-validation-and-derived-attributes.md)、[ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)、[ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md)、[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)、[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md) 与 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md)

## 背景
在完成 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 的服务端写路径 Group Commit 与无重叠前缀跳过 `Compaction` 优化后，大文件并发写吞吐已达云盘物理极限（`174.70 MiB/s` vs 物理上限 `172.74 MiB/s`）。然而对照同机 `JuiceFS + PostgreSQL 16` 基准测试（[`benchmark/juicefs/REPORT.md`](../../benchmark/juicefs/REPORT.md)），剔除评测环境中 `rclone serve s3` 未执行 `fsync`（Server PageCache 缓冲）及 `juicefs bench` 未清理客户端本地块缓存 `/var/jfsCache` 的表观偏差后，Bowl 在 **元数据高并发 IOPS（`mdtest` 创建、读取、属性查询）** 与 **大文件顺序读吞吐（`56.05 ~ 87.43 MiB/s`）** 上仍存在四项真实架构瓶颈与一项关键设计权衡：

1. **非 `Write` 热路径每次请求同步执行重型会话续期与全表孤儿扫描**：
   - [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 仅在 `Write` 中引入了内存时间窗口节流（`touchWriteClientSessionForHandle`）。而在 `Create`、`TmpFile`、`Open`、`OpenDir`（调用 `renewOptionalClientSession`）以及 `Read`、`Flush`、`Release`、`ReleaseDir`、`SetAttr`、`Fallocate`（调用 `touchActiveClientSessionForHandle`）中，每个请求仍在读写事务内同步执行 `DeleteExpiredClientSessions`、`UpsertClientSession` 与 `reclaimUnreferencedOrphans`（全表扫描 `nlink = 0` 孤儿节点）。
   - 当单客户端挂载实例运行多进程并发负载（如 4 并发 MPI `mdtest`）时，所有并发进程在同一个 `client_sessions(session_id)` 行级排他写锁上串行化排队，导致 `File creation` 从单进程 `271 ops/s` 到 4 进程仅升至 `429 ops/s`（JuiceFS 为 `3,414 ops/s`）。
2. **`Flush` 与 `Release` 无条件对 `Inode` 加排他写锁，将纯读生命周期（`Open -> Flush -> Release`）放大为 3 次写事务与 2 次 `Inode` 排他锁**：
   - 在 `mdtest File read`（打开空文件读取并关闭）中，Bowl 吞吐仅为 `1,092 ops/s`（JuiceFS 为 `24,625 ops/s`，相差 22 倍）。
   - 根因在于：客户端每次关闭文件描述符均无条件发送 `Flush` RPC，服务端 `Flush` 与随后的 `Release` 无论目标 `Inode` 是否持有 `FileLock`、无论是否为 `Orphan Inode`（`nlink == 0`），一上来均开启读写事务并调用 `GetInodeForUpdate(ino)` 对目标 `Inode` 加排他写锁。
3. **`FileHandle` 持久化策略权衡（为何选择「档位 A：保留数据库句柄表但消除冗余排他锁」而非「档位 B：纯内存句柄表」）**：
   - **档位 B（对齐 JuiceFS 纯内存分配 `fh` + 懒孤儿登记）**：虽能完全消除正常 `Open` / `Release` 的 `file_handles` 表写入，但在服务端进程重启瞬间，未发生 `Unlink` 的普通活跃 `FileHandle` 在服务端内存中会全部丢失，破坏 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md) 与 [ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md) 锁定的跨重启句柄持久化与严格 `EBADF` 校验契约。
   - **档位 A（保留 `file_handles` 表持久化，消除无锁 `Flush` 与非孤儿 `Release` 的 `Inode` 排他锁）**：在不牺牲跨重启句柄持久化与任何安全契约的前提下，省去 99.99% 无 POSIX 锁场景下的 `Flush` 写事务，并在 `Release` / `ReleaseDir` 关闭正常节点（`nlink > 0`）时免除 `inodes` 表 `FOR UPDATE` 排他锁，兼顾系统可靠性与高并发吞吐。
4. **`Lookup` 显式事务与双 SQL 往返开销，以及创建慢引发的 `stat` 内核缓存过期连锁反应**：
   - [ADR-0003](0003-posix-lookup-validation-and-derived-attributes.md) 为 `Lookup` 开启 `RepeatableRead` 只读事务并串行执行 `GetInode(parent_ino)` 与 `GetInodeByEntry(parent_ino, name)`，每次 `Lookup` 产生 **4 次 PostgreSQL 网络往返**（`BEGIN` + `SELECT parent` + `SELECT child` + `COMMIT`），导致冷 `Stat file` 延迟（`0.86 ms`）为 JuiceFS 单语句查询（`0.43 ms`）的 2 倍。
   - 同时，由于 `mdtest` 创建 1,040 个文件耗时 `2.42s` 超出 FUSE 内核 `1.0s` 缓存有效期（JuiceFS 仅耗时 `0.30s < 1.0s`），导致紧随其后的 `File stat` 阶段在 Bowl 中被迫发起跨机 `Lookup` RPC，吞吐跌至 `7,791 ops/s`（JuiceFS 100% 命中内核缓存达 `158,666 ops/s`）。
5. **数据面 `Read` 走写事务加 `FOR KEY SHARE` 锁、多 `Block` 串行读盘与客户端 `1 MiB` 停等**：
   - 服务端 `Read`（[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)）每次均开启读写事务、更新会话并调用 `LockValidSliceRefsKeyShare` 对 `slice_refs` 加数据库行锁，且在磁盘 I/O 期间长持数据库连接；当单次读取跨越多个 `Block` 时使用串行 `for` 循环调用 `GetBlock`。
   - 客户端 `Read`（[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)）按内核单次最大 `1 MiB` 串行停等请求，缺乏跨 `1 MiB` 请求的异步顺序预读（Read-Ahead），导致单流顺序读仅 `56.05 MiB/s`（不足云盘物理上限的三分之一）。

## 决策

1. **全路径 `ClientSession` 续期内存节流与过期/孤儿清理冷却（演进 ADR-0012 / ADR-0013 / ADR-0022）**：
   - 将 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 的内存时间窗口节流机制推广至全量携带 `session_id` 的元数据与数据面路径：`Create`、`TmpFile`、`Open`、`OpenDir`、`Read`、`Flush`、`Release`、`ReleaseDir`、`SetAttr`、`Fallocate`。
   - **节流窗口内的零写锁安全核验**：当同一 `session_id` 距上次数据库续期未满节流窗口（默认 `5s`，远小于 `30s` 租约 TTL）时，跳过 `UpsertClientSession`、`DeleteExpiredClientSessions` 与 `reclaimUnreferencedOrphans` 三条写 SQL；仅通过无锁只读查询核验 `expires_at > clock_timestamp()` 及句柄代际不变量 `handle.CreatedAt >= session.CreatedAt`。一旦发现会话已超期，立即清除内存节流状态、触发过期会话与孤儿清理并返回 `ErrBadFileHandle`（`EBADF`），100% 守恒防过期会话越权与防同名 `session_id` 重建后旧句柄复活的安全契约。
   - **显式保活与慢路径清理兜底**：`RenewSession`、`RenewSessionWithDisk`、`CloseSession` 以及跨越 `5s` 节流窗口首次触发数据库 `UpsertClientSession` 的慢路径，继续执行过期会话清理与 `FOR UPDATE SKIP LOCKED` 孤儿节点回收。

2. **保留 `file_handles` 表持久化（档位 A），实行无锁 `Flush` 免写事务与 `Release` / `ReleaseDir` 非孤儿免锁 `Inode`（演进 ADR-0009 / ADR-0012）**：
   - **无 POSIX 锁时 `Flush` 免除 `Inode` 排他锁与删锁事务**：跟踪活跃 POSIX 记录锁状态（客户端与服务端追踪 `Inode` 是否调用过 `SetLk` / `SetLkw`）。当目标 `Inode` 未持有任何 POSIX 记录锁时，客户端在确认本地会话与句柄绑定有效后可直接短路返回，或由服务端在不调用 `GetInodeForUpdate` 排他锁的前提下仅做只读存在性与会话核验后直接返回，消除无锁文件关闭时的空删锁写事务。
   - **`Release` 与 `ReleaseDir` 按 `nlink` 分流加锁**：
     - 关闭句柄时，不再无条件前置调用 `GetInodeForUpdate` 排他锁死 `Inode`。
     - 对于常规未解链节点（`inode.Nlink > 0`）且无需清理 BSD `flock`（未置位 `FuseReleaseFlockUnlock` 或未加过 `flock`）的场景，校验会话后直接按 `(fh, ino, session_id)` 删除 `file_handles` 行即提交事务，**全程 0 次 `inodes` 表排他锁**。
     - 仅当检测到目标节点处于 `Orphan Inode` 状态（`nlink == 0`，含删除句柄瞬间并发变为 `0` 的重检）或需清理 `flock` 时，才在同一事务内调用 `GetInodeForUpdate(ino)` 加排他锁并执行 `reclaimOrphanIfNoHandles` 物理回收与配额退还。

3. **`Lookup` 常规目录项单语句无事务外框直查（演进 ADR-0003）**：
   - 对特殊目录项（`"."`、`".."`）及空名称（`""`）维持 [ADR-0003](0003-posix-lookup-validation-and-derived-attributes.md) 的原有解析分支不变。
   - 对常规目录项（`len(name) > 0` 且非 `"."`/`".."`），对齐 [ADR-0004](0004-fuse-getattr-direct-query-and-unlinked-inode-support.md) `GetAttr` 的无事务外框直查模式，利用 PostgreSQL 单条 SQL 语句天然具备的语句级原子快照，通过单条 `LEFT JOIN` SQL（`LookupEntryWithParent`）一次性返回父节点属性（`parent_mode`、`parent_nlink`）与目标子项 `Inode` 完整属性，在应用层严格按原序判定父节点不存在（`ErrNotFound`）、父节点非目录（`ErrNotDir`）、孤儿父目录或子项不存在（`ErrNotFound`），将单次 `Lookup` 的数据库网络往返从 **4 次降至 1 次**。

4. **服务端 `Read` 免数据库行锁只读快路径、多 `Block` 并发读取与客户端顺序预读（演进 ADR-0019 / ADR-0020）**：
   - **服务端 `Read` 免写事务与进程内/乐观保护**：当无需刷新 `atime`（如默认 `noatime` 或未达 `relatime`/`strictatime` 刷新阈值）且会话处于节流窗口内时，`Read` 不再开启读写事务及调用 `LockValidSliceRefsKeyShare`（消除数据库 `FOR KEY SHARE` 页锁修改与读盘期间的长连接占用）；在微秒级只读查询获取 `Inode`、`FileHandle` 与 `chunks` 切片视图后立即归还数据库连接。在物理块读取阶段，通过进程内活跃读引用保护结合乐观重试兜底（若并发 `Compaction` 刚好替换并清理了旧物理块导致 `GetBlock` 返回 `ErrBlockNotFound`，自动重新拉取最新合并后的 `chunks` 视图重试读取），确保 0 数据库锁且 100% 并发读取安全。
   - **服务端多 `Block` 有界并发读取**：当单次 `Read` 裁剪出的 `readSegment` 跨越多个物理 `Block` 时，使用有界并发协程池同时调用 `BlockStore.GetBlock` 填充目标缓冲区，消除串行磁盘 I/O 停等。
   - **客户端顺序流式预读（Read-Ahead）与写操作即时失效**：在客户端 `RawFS` 中按句柄检测连续顺序读取模式；命中顺序流时按 `4 MiB` 物理 `Block` 粒度对齐拉取并异步预读下一窗口，使后续 `1 MiB` 内核 `FUSE_READ` 直接命中内存缓冲。一旦该 `Inode` 发生本地 `Write`、`SetAttr`（含截断）、`Fallocate` 或句柄 `Release`，立即清空对应预读缓冲，严守 POSIX 读写立即可见性契约。

## 结果
- **积极影响**：
  - 在 100% 保持 Protobuf 契约、数据库 Schema、跨重启句柄持久化与全量 POSIX 安全测试（`pjdfstest` 及会话/孤儿/死锁边界测试）守恒的前提下，彻底消除了多进程并发时的 `client_sessions` 单行锁排队。
  - 将常规文件 `Open -> Flush -> Release` 的 `Inode` 排他写锁从 **2 次降至 0 次**，将 `Lookup` 数据库往返从 **4 次降至 1 次**，大幅缩短 `mdtest` 文件创建与读取耗时，并使紧随其后的 `stat` 阶段重新落入内核缓存有效期内。
  - 服务端 `Read` 实现纯只读快速归还连接与多块并发读盘，配合客户端顺序预读流水线，使单流与并发大文件读取吞吐逼近云盘物理带宽上限。
- **代价考量**：
  - 服务端需维护轻量级的活跃 POSIX 锁状态标记与进程内读块引用/乐观重试逻辑；客户端需维护有界顺序预读缓冲区并在写/截断/关闭路径严格执行同步失效。
