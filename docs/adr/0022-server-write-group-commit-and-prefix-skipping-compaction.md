# 0022. 服务端写入批量合并提交（Group Commit）、Compaction 跳过已规整大块与客户端保持同步直通

## 状态
已采纳 (accepted) — 演进 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 与 [ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)

## 背景
在阿里云双机内网测试（[`benchmark/juicefs/REPORT.md`](../../benchmark/juicefs/REPORT.md)）中，面对硬件顺序写上限为 `174.36 MiB/s` 的 `cloud_essd` 云盘，Bowl 在单流顺序写、16 并发顺序写及 Direct I/O 写入下的速度都被卡在 `85 ~ 88.8 MiB/s`（只有云盘硬件上限的一半），而且 16 并发写入完全没有比单流更快。排查代码后定位到三个具体瓶颈和一个关于客户端缓存的取舍问题：
1. **每次 `1 MiB` 顺序写写满一个 `64 MiB` `Chunk` 后，100% 触发后台 `Compaction` 把刚写好的数据全部重读重写一遍（写放大 2 倍）**：Linux FUSE 内核限制单次 `FUSE_WRITE` 最大为 256 页（`1 MiB`），客户端每次同步发送 `1 MiB` 的 `Write` 请求；服务端一个 `Chunk` 是 `64 MiB`，触发合并的阈值 `CompactSliceThreshold` 设为 `64`。于是顺序写满一个 `64 MiB` `Chunk` 刚好产生 64 个互不重叠的 `1 MiB` `Slice`，立刻触发 `CompactChunk` 把刚写进硬盘的 `64 MiB` 数据重新读出来，再写成 16 个 `4 MiB` 的新 `Block` 文件（如果是按 `128 KiB` 顺序写，更会因为不会跳过前面已经合并好的大块而触发 8 次重复重写，写入量放大到 5.5 倍）。
2. **`Write` 事务里抢同一行数据库锁，且每个请求单独刷盘**：每次 `Write` 都要先单独查一次数据库拿 `NextSliceID`，接着在 PostgreSQL 事务里调用 `touchActiveClientSessionForHandle`（清理过期会话、加排他锁更新 `client_sessions` 行、扫描孤儿节点）和 `GetDiskByRootInoForUpdate`（加排他锁更新 `disks` 表里的已用配额）。这导致同一个客户端或同一个 `Disk` 下的 16 个并发写线程全部在数据库同一行锁上排队，而且每个请求都要各自执行一次父目录 `syncDir` 和 PostgreSQL WAL `fdatasync`。
3. **为什么不在客户端内存里攒够 `4 MiB` 再异步发给服务端（Write-Behind Buffer）**：
   - **省下的网络耗时微乎其微**：同可用区内网往返延迟（RTT）只有 `0.071 ms`（带宽 `15.71 Gbps`），发 4 次 `1 MiB` 请求比合并发 1 次 `4 MiB` 请求只多出 3 次网络往返，耗时 `0.213 ms`，还不到写 `4 MiB` 云盘耗时（`22.94 ms`）的 `1%`。
   - **会破坏 6 项基本文件系统行为**：如果客户端把不到 `4 MiB` 的数据先放进自己内存就告诉内核写入成功，会导致：
     1. 磁盘配额满（`ENOSPC`）或句柄失效（`EBADF`）时，`write(2)` 当场报错被吞掉；
     2. 刚调完 `write(2)` 立刻调 `stat(2)` 时，读到的还是没清除 `S_ISUID` / `S_ISGID` 特权位的旧属性和旧文件大小，无法通过 `pjdfstest` 测试；
     3. 跨客户端使用文件锁（`SetLk` / `Flock`）时，写端解锁后读端还读不到刚才留在写端内存里的数据；
     4. 多客户端同时用 `O_APPEND` 追加写时顺序错乱；
     5. 一旦网络闪断导致会话过期重建（`session_recreated`），旧句柄作废，之前已经告诉用户写入成功但还留在客户端内存里的数据就会**悄无声息地彻底丢失**。

## 决策
1. **客户端继续保持每次写入直接发往服务端，不在客户端内存攒脏数据（维持 ADR-0020）**：
   - 客户端不設异步写缓冲；每次 `FUSE_WRITE` 都同步调用 `StorageService.Write`，必须等服务端处理完写入并提交数据库事务后，才向内核返回成功。
2. **`CompactChunk` 跳过开头已经规整且无重叠的大块（`skipSome`），只合并后面的零碎切片**：
   - 参考 JuiceFS 的 `skipSome` 做法，在 [`compact.go`](../../services/storage/internal/meta/compact.go) 中从 `Chunk` 开头往后检查 `Slice` 列表：只要开头的 `Slice` 长度不小于 `1 MiB`，并且跟后面所有的 `Slice` 都没有字节区间重叠，就认为它已经是规整好的数据块，直接跳过不动（保留原有的 `slice_id` 和硬盘上的 `Block` 文件）。
   - 只有跳过开头这些规整大块后，剩下的尾部零碎或有重叠的 `Slice` 数量仍然达到 `CompactSliceThreshold (64)` 时，才把尾部这部分读出来重新合并，并用 CAS 把 `chunks.slices` 更新为「跳过的前缀 + 合并后的尾部」。这样一来，按 `1 MiB` 顺序写文件时**不会触发任何后台重写（写放大从 2.0 倍降到 1.0 倍）**，按更小粒度顺序写时也只合并新写入的尾部切片。
3. **优化 `BlockStore` 目录同步锁，合并同批次的目录刷盘**：
   - 保持 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 先写 `.tmp` 临时文件再 `os.Rename` 为正式文件的做法不变。
   - 修复 `ensureDirSynced` 的锁竞争：当两级散列子目录已经在 `syncedDirs` 缓存里记录过时直接返回，不再在全局互斥锁 `dirMu` 里重复调用 `os.Stat`；并发批量写入时，对同一批次涉及的父目录先去重再统一执行目录同步（`SyncDirs`）。
4. **服务端写路径减少数据库锁竞争，并把并发写入分两阶段批量提交（Group Commit）**：
   - **去掉每次写入时的全表扫描，并在内存里批量预取 `slice_id`**：因为 `GetFileHandle` 已经顺带检查了会话是否过期（`client_sessions.expires_at > clock_timestamp()`），所以把清理过期会话和孤儿节点的工作交给后台 `RenewSession` 定期执行，在 `Write` 事务里不再做全表扫描，且同一个 `session_id` 每隔固定时间窗口才写库更新一次过期时间，消除多线程并发写抢同一行 `client_sessions` 锁的问题；同时服务端每次从数据库序列里一次性预取一批 `slice_id` 放在内存里按需分配，省去每次 `Write` 单独查库拿 ID 的网络往返。
   - **两阶段写入与单事务批量提交（参考 RocksDB `WriteThread`）**：
     - **阶段 1（并发写块文件）**：同时到达的多个 `Write` 请求各自从内存领取 `slice_id`，并发把数据写成块文件。
     - **阶段 2（合并到一个数据库事务里一次性提交）**：写完块文件的请求进入 `WriteThread` 队列，由排在最前面的请求（Leader）把当前队列里攒下的一批请求收集起来：先统一同步涉及到的文件目录，再开启**一个 PostgreSQL 事务**，按 `ino` 从小到大锁住涉及的 `Inode` 行，在内存里按请求顺序算好每个文件的写入位置（包括 `O_APPEND` 追加写）并拼好 `24B` 的 `Slice` 记录，按磁盘 `RootIno` 汇总检查并更新配额，批量插入 `slice_refs`、更新 `Inode` 属性，最后**整批请求共用 1 次数据库 `COMMIT`**，提交完成后一次性唤醒这批请求里的所有调用方。
     - **单条报错自动隔离**：如果同一批请求里有个别请求因为句柄失效（`EBADF`）、不是普通文件或配额超限（`ENOSPC`）报错，Leader 会自动把这条报错的请求挑出来单独返回错误并删掉它刚才写的块文件，其余正常的请求继续合并提交，互不影响。

## 结果
- **正面效果**：在不改动 Protobuf 接口和数据库表结构、也不牺牲客户端同步写入安全性的前提下，彻底解决了大文件顺序写被后台重写一遍的问题，并消除了多线程并发写入抢同一行数据库锁和重复刷盘的开销，让单流和并发写入速度都能跑满云盘硬件上限。
- **代价与取舍**：服务端 `meta.Manager` 需要在内存里维护一个 `slice_id` 批量分配器，以及在阶段 2 收集批量请求和挑出报错请求回滚的处理逻辑。
