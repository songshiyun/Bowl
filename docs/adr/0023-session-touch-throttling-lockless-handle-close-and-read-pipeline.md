# 0023. 减少会话续期写库频率、关闭普通文件免加行锁、Lookup 合并为单条查询与 Read 顺序预读

## 状态
已被 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md)、[ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md) 与 [ADR-0033](0033-client-write-behind-multi-window-readahead-and-local-dir-synthesis.md) 替代 (superseded by ADR-0030, ADR-0032, and ADR-0033)

## 背景
在完成 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 的服务端批量写入提交和 `Compaction` 跳过已规整大块优化后，大文件并发写速度已经达到云盘物理上限（`174.70 MiB/s` vs 物理上限 `172.74 MiB/s`）。但对比同机器的 `JuiceFS + PostgreSQL 16` 测试结果（[`benchmark/juicefs/REPORT.md`](../../benchmark/juicefs/REPORT.md)），排除掉测试环境里 `rclone serve s3` 未开启 `fsync` 以及 `juicefs bench` 未清理本地缓存 `/var/jfsCache` 的外部差异后，Bowl 在 **高并发小文件操作（`mdtest` 创建、读取、查属性）** 与 **大文件顺序读速度（`56.05 ~ 87.43 MiB/s`）** 上仍然存在四个明显的性能瓶颈：

1. **除 `Write` 外的其他接口每次请求都要写数据库续期会话并扫描全表孤儿节点**：
   - [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 只在 `Write` 接口里加了按时间间隔续期会话的限制。而在 `Create`、`TmpFile`、`Open`、`OpenDir`（调用 `renewOptionalClientSession`）以及 `Read`、`Flush`、`Release`、`ReleaseDir`、`SetAttr`、`Fallocate`（调用 `touchActiveClientSessionForHandle`）中，每次请求依然会在数据库事务里执行 `DeleteExpiredClientSessions`、`UpsertClientSession` 和 `reclaimUnreferencedOrphans`（扫描所有 `nlink = 0` 的孤儿节点）。
   - 当同一个客户端挂载点上有多个进程同时操作文件（例如 4 进程并发运行 `mdtest`）时，所有请求都会去抢同一条 `client_sessions(session_id)` 记录的数据库排他锁，导致文件创建速度从单进程的 `271 ops/s` 到 4 进程时只涨到 `429 ops/s`（JuiceFS 为 `3,414 ops/s`）。
2. **`Flush` 与 `Release` 每次都无条件锁住 `Inode` 行，导致单纯打开读一下文件并关闭（`Open -> Flush -> Release`）也要触发 3 次写事务和 2 次 `Inode` 排他锁**：
   - 在 `mdtest File read`（打开空文件读取并关闭）测试中，Bowl 只有 `1,092 ops/s`（JuiceFS 为 `24,625 ops/s`，慢了 22 倍）。
   - 原因是：客户端每次关闭文件描述符都会发送 `Flush` RPC，而服务端的 `Flush` 和随后的 `Release` 不管这个文件有没有加过 `FileLock`、也不管它是不是已被删除的 `Orphan Inode`（`nlink == 0`），一上来全都开启数据库写事务并调用 `GetInodeForUpdate(ino)` 锁住 `Inode` 行。
3. **文件句柄（`FileHandle`）存放在数据库还是只放内存的取舍**：
   - **如果学 JuiceFS 只在内存里分配 `fh`**，虽然平时 `Open` / `Release` 不用写 `file_handles` 表，但一旦服务端进程重启，所有正在打开的普通文件句柄都会丢失，客户端再拿着原来的 `fh` 来读写就会全部报错，破坏了 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md) 和 [ADR-0013](0013-fuse-rename2-tmpfile-fsyncdir-and-session-orphan-reclamation.md) 支持服务端重启后继续使用已打开句柄的设计。
   - **因此我们选择保留数据库里的 `file_handles` 表，只去掉 `Flush` 和 `Release` 中不必要的写事务与 `Inode` 行锁**：这样既保证了服务端重启后文件句柄不丢，又能省掉 99.99% 未加锁场景下的 `Flush` 写事务，并且在关闭正常文件（`nlink > 0`）时不再锁 `inodes` 表。
4. **`Lookup` 开启事务分两条 SQL 查询，且创建文件太慢导致紧接着的 `stat` 缓存全部过期**：
   - [ADR-0003](0003-posix-lookup-validation-and-derived-attributes.md) 的 `Lookup` 每次都会开启一个只读事务，先后执行 `GetInode(parent_ino)` 和 `GetInodeByEntry(parent_ino, name)`，算上 `BEGIN` 和 `COMMIT` 一共要跟 PostgreSQL 交互 **4 次**，导致单次 `Stat file` 耗时（`0.86 ms`）比 JuiceFS 的单条 SQL 查询（`0.43 ms`）慢一倍。
   - 同时，因为 `mdtest` 创建 1,040 个文件花了 `2.42s`，超过了 FUSE 内核 `1.0s` 的目录项缓存有效期（JuiceFS 创建只花了 `0.30s`），等到下一阶段执行 `File stat` 时，内核缓存早已过期，所有请求都不得不重新发网络 RPC 去调 `Lookup`，导致吞吐量掉到 `7,791 ops/s`（而 JuiceFS 全部命中内核缓存，达到 `158,666 ops/s`）。
5. **服务端 `Read` 走写事务加数据库锁、多个 `Block` 挨个串行读盘，且客户端每次只读 `1 MiB` 不做预读**：
   - 服务端 `Read`（[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)）每次都开启读写事务、更新会话并对 `slice_refs` 表加 `FOR KEY SHARE` 行锁，而且在读硬盘文件期间一直占着数据库连接不放；遇到跨多个 `Block` 的读取时，也是用 `for` 循环一个接一个串行读文件。
   - 客户端 `Read`（[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)）每次只按内核请求的 `1 MiB` 大小发一次请求、等结果回来再发下一次，没有提前预读后续数据块，导致单流顺序读只有 `56.05 MiB/s`（不到云盘硬件速度的三分之一）。

## 决策

1. **所有带 `session_id` 的接口统一在内存里控制续期频率，每 `5s` 才写库续期一次（演进 ADR-0012 / ADR-0013 / ADR-0022）**：
   - 把 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 在内存里记录上次续期时间的做法，推广到所有带 `session_id` 的接口：`Create`、`TmpFile`、`Open`、`OpenDir`、`Read`、`Flush`、`Release`、`ReleaseDir`、`SetAttr`、`Fallocate`。
   - **5 秒内免写库、只做只读检查**：只要同一个 `session_id` 距离上次写数据库续期还不到 `5s`（远小于 `30s` 的会话过期时间），就跳过 `UpsertClientSession`、`DeleteExpiredClientSessions` 和 `reclaimUnreferencedOrphans` 这三条写 SQL，只用无锁只读查询确认会话还没过期（`expires_at > clock_timestamp()`）且句柄不是旧会话留下的（`handle.CreatedAt >= session.CreatedAt`）。如果检查发现会话其实已经过期了，就立刻清掉内存记录、触发过期清理并返回 `ErrBadFileHandle`（`EBADF`），防止过期会话继续操作或同名 `session_id` 重建后旧句柄被误用。
   - **专门的续期接口和超过 5 秒的请求照常清理**：`RenewSession`、`RenewSessionWithDisk`、`CloseSession`，以及距离上次续期超过 `5s` 的请求，仍然照常更新数据库里的会话过期时间并顺带清理孤儿节点。

2. **保留 `file_handles` 数据库表，没加过锁的文件 `Flush` 不写库，关闭正常文件 `Release` 不锁 `Inode`（演进 ADR-0009 / ADR-0012）**：
   - **没加过 POSIX 文件锁时，`Flush` 不再开写事务去删锁**：客户端和服务端各自记录哪些 `Inode` 调用过 `SetLk` / `SetLkw` 加锁。如果目标文件从来没加过 POSIX 记录锁，客户端检查完本地会话和句柄有效后可以直接返回成功，或者由服务端只做一次只读查询确认节点和句柄有效就返回，不再调用 `GetInodeForUpdate` 锁表和执行空的删锁事务。
   - **`Release` 与 `ReleaseDir` 只在节点已被删除（`nlink == 0`）或需要释放 `flock` 时才锁 `Inode`**：
     - 关闭文件或目录句柄时，不再一上来就调用 `GetInodeForUpdate` 锁住 `Inode` 行。
     - 如果文件没有被删除（`inode.Nlink > 0`），也不需要释放 BSD `flock` 锁（没有带 `FuseReleaseFlockUnlock` 标志或没加过 `flock`），检查完会话有效后直接按 `(fh, ino, session_id)` 删掉 `file_handles` 表里对应的那一行就提交事务，**全程不对 `inodes` 表加排他锁**。
     - 只有发现该节点已经被删除（`nlink == 0`，包括删掉句柄那一刻刚好变成 `0` 的情况）或者需要清理 `flock` 时，才在同一个事务里调用 `GetInodeForUpdate(ino)` 加排他锁，去执行孤儿节点回收和退还配额。

3. **普通文件名的 `Lookup` 合并成一条 `LEFT JOIN` SQL 直接查询，不再开启显式事务（演进 ADR-0003）**：
   - 查找特殊目录项（`"."`、`".."`）和空名称（`""`）时，保持 [ADR-0003](0003-posix-lookup-validation-and-derived-attributes.md) 原有的处理逻辑不变。
   - 查找普通文件名（`len(name) > 0` 且不是 `"."`/`".."`）时，不再开启 `RepeatableRead` 事务分两步查，而是直接执行一条 `LEFT JOIN` SQL（`LookupEntryWithParent`），一次性查出父目录属性（`parent_mode`、`parent_nlink`）和目标子项的完整 `Inode` 属性。由于 PostgreSQL 单条 SQL 本身就读取同一个时间点的快照，代码拿到结果后按原顺序判断父节点不存在（`ErrNotFound`）、父节点不是目录（`ErrNotDir`）、父目录已被删除或子项不存在（`ErrNotFound`）即可，把单次 `Lookup` 的数据库网络交互从 **4 次减少到 1 次**。

4. **服务端 `Read` 查完元数据立即释放数据库连接、多块并发读盘，配合客户端顺序预读（演进 ADR-0019 / ADR-0020）**：
   - **服务端 `Read` 不加数据库行锁、读硬盘前先还回数据库连接**：在不需要更新 `atime`（例如默认的 `noatime` 模式，或还没到 `relatime` 更新间隔）且会话还在 5 秒免续期窗口内时，`Read` 不再开启读写事务，也不再调用 `LockValidSliceRefsKeyShare` 给数据库加锁；只做一次快速的只读查询拿到 `Inode`、`FileHandle` 和 `chunks` 切片列表，随后**立刻把数据库连接归还给连接池**。接着去读硬盘上的 `Block` 文件时，用进程内的引用计数保护正在读的块；万一遇到后台 `Compaction` 刚好把旧块合并删掉导致返回 `ErrBlockNotFound`，就重新查一次最新的 `chunks` 切片列表再读一遍。这样既不用给数据库加锁，又不会在读盘时占着数据库连接。
   - **服务端并发读取多个 `Block`**：当一次 `Read` 请求需要跨越多个物理 `Block` 文件时，使用限制了并发数的协程池同时调用 `BlockStore.GetBlock` 读取各个块，不再排队串行读。
   - **客户端顺序预读（Read-Ahead）与写入时立即清空缓存**：客户端 `RawFS` 记录每个打开句柄的读取位置；一旦发现是在连续顺序往下读，就按 `4 MiB`（一个完整 `Block` 的大小）对齐拉取数据，并在后台提前异步拉取下一个 `4 MiB` 窗口放到内存里，让后续内核发来的 `1 MiB` `FUSE_READ` 请求直接从内存拿到数据。一旦这个文件发生了本地 `Write`、`SetAttr`（如截断）、`Fallocate` 或关闭句柄 `Release`，立刻清空该文件的预读缓存，保证不会读到旧数据。

## 结果
- **正面效果**：
  - 在不改动 Protobuf 接口、数据库表结构、服务端重启后句柄依然有效以及全部 POSIX 测试用例的前提下，彻底消除了多进程并发时在 `client_sessions` 表同一行上的加锁排队。
  - 普通文件走完 `Open -> Flush -> Release` 全流程对 `Inode` 加的排他锁从 **2 次降到 0 次**，`Lookup` 的数据库往返从 **4 次降到 1 次**，大幅加快了 `mdtest` 创建和读取文件的速度，也让创建完紧接着执行的 `stat` 操作能赶在 1 秒缓存过期前直接命中内核缓存。
  - 服务端 `Read` 查完切片立即释放数据库连接并并发读取多个数据块，配合客户端的顺序预读，让大文件顺序读速度大幅提升并接近云盘硬件上限。
- **代价与取舍**：
  - 服务端需要额外在内存里记录哪些文件加过 POSIX 锁，以及处理读盘时遇到后台合并删掉旧块的重试；客户端需要维护预读缓存，并在发生写入、截断或关闭文件时及时清空缓存。
