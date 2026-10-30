# 0022. 服务端 RocksDB 式两阶段流水线 Group Commit、无重叠前缀跳过 Compaction 与客户端同步直通守恒

## 状态
已采纳 (accepted) — 演进 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 与 [ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)

## 背景
在阿里云双机内网基准测试（[`benchmark/juicefs/REPORT.md`](../../benchmark/juicefs/REPORT.md)）中，面对物理顺序写上限为 `174.36 MiB/s` 的 `cloud_essd` 云盘，Bowl 在单流顺序写、16 并发顺序写及 Direct I/O 写入负载下的吞吐量均被锁定在 `85 ~ 88.8 MiB/s`（仅为云盘物理上限的 50.8%），且 16 并发写入未呈现并发加速。代码级实证溯源揭示了三项核心瓶颈与一项关键架构权衡：
1. **单块 `1 MiB` 顺序写 100% 触发后台全量 `Compaction` 造成 2.0x 物理写放大**：Linux FUSE 内核协议硬编码单次 `FUSE_WRITE` 上限为 `FUSE_MAX_MAX_PAGES = 256` 页（`1 MiB`），客户端每次同步发送 `1 MiB` `Write` RPC；而服务端 `Chunk` 跨度为 `64 MiB`、`CompactSliceThreshold = 64`。顺序写满一个 `64 MiB` `Chunk` 恰好生成 64 个无重叠的 `1 MiB` `Slice`，100% 触发 `CompactChunk` 将刚落盘的 `64 MiB` 数据重新读出并重写为 16 个 `4 MiB` 新 `Block` 文件（若以 `128 KiB` 顺序写，更会因缺少已对齐前缀跳过机制而触发 8 次级联重写，写放大达 5.5x）。
2. **`Write` 事务内跨文件全局行锁串行化与独立刷盘放大**：每次 `Write` 均在事务外单独发起 `NextSliceID` SQL 查询，并在 PostgreSQL 事务内调用 `touchActiveClientSessionForHandle`（扫描过期会话、排他锁更新 `client_sessions` 行、扫描孤儿节点）及 `GetDiskByRootInoForUpdate`（排他锁整块 `Disk` 行扣减配额），导致同一客户端或同一 `Disk` 下的 16 路并发写线程在单行锁上完全退化为串行排队，且每个请求独立触发父目录 `syncDir` 与 PostgreSQL WAL `fdatasync`。
3. **为何否决「客户端用户态 4 MiB 异步写合并缓冲（Write-Behind Buffer）」**：
   - **定量算账无必要**：同可用区 VPC 内网 RTT 仅 `0.071 ms`（带宽 `15.71 Gbps`），`4 × 1 MiB` 同步 RPC 相比 `1 × 4 MiB` 合并 RPC 多出的 3 次网络固定往返仅 `0.213 ms`，占 `4 MiB` 云盘物理写入耗时（`22.94 ms`）的 `0.93%`。
   - **破坏 6 项核心契约**：若客户端将 `< 4 MiB` 写入拷入内存即向内核返回 `OK`，会直接吞没 `write(2)` 当场的 `ENOSPC` / `EBADF` 同步错误码（破坏配额拦截与短写契约）、导致 `write(2)` 后立即 `stat(2)` 读到未清除的 `S_ISUID` / `S_ISGID` 特权位与旧 `mtime` / `size`（破坏 `pjdfstest` POSIX 兼容性）、导致跨客户端 `SetLk` / `Flock` 解锁后读端读到旧数据、扰乱多客户端 `O_APPEND` 原子追加序，并在网络闪断触发 `ClientSession` 超期轮换（`session_recreated`）时因旧 `FileHandle` 销毁而造成已承诺数据的**静默丢失（Silent Data Loss）**。

## 决策
1. **坚守客户端 100% 零缓存同步直通契约（维持 ADR-0020）**：
   - 客户端不引入任何驻留未落盘脏数据的异步写合并缓冲（Write-Behind Buffer）；每次 `FUSE_WRITE` 保持同步直通 `StorageService.Write`，严格以服务端物理 `Block` 落盘且 PostgreSQL 元数据事务提交作为向内核返回 `OK` 的唯一边界。
2. **`CompactChunk` 引入无重叠大块前缀跳过（`skipSome`）与增量合并**：
   - 借鉴 JuiceFS `skipSome` 算法，在 [`compact.go`](../../services/storage/internal/meta/compact.go) 中从 `Chunk` 头部顺序扫描 `Slice` 序列：凡是长度 `>= 1 MiB` 且与后续所有 `Slice` 均无字节区间重叠的前缀 `Slice`，一律视为已规整块直接跳过（保留原 `slice_id` 与物理 `Block` 文件不动）。
   - 仅当跳过整洁前缀后，剩余尾部零碎或存在覆盖重叠的 `Slice` 数量仍达到 `CompactSliceThreshold (64)` 时，才对尾部子序列执行读取重整，并通过 CAS 将 `chunks.slices` 原子替换为 `skippedPrefix || compactedSuffix`。由此确保 `1 MiB` 顺序写产生 **0 次 `Compaction` 重写（物理写放大从 2.0x 降至 1.0x）**，且小粒度顺序写仅做 $O(1)$ 增量合并。
3. **`BlockStore` 完整保留 `.tmp -> fdatasync -> rename -> syncDir` 原子发布并消除冗余目录锁**：
   - 严格守恒 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md) 的物理块原子发布不变量：绝不省去临时文件写入、`tmpFile.Sync()`、`os.Rename` 与父目录 `syncDir`。
   - 修复 `ensureDirSynced` 热路径：当散列子目录已存在于 `syncedDirs` 缓存时直接返回，不在全局互斥锁 `dirMu` 内重复执行 `os.Stat`；在并发批次写入中支持按去重后的父目录集合合并执行 `syncDir`（Group Directory Sync）。
4. **服务端写路径去锁减负与 RocksDB 式 `WriteThread` 两阶段流水线 Group Commit**：
   - **剥离热路径冗余会话更新与号段预取**：`GetFileHandle` 已内联校验 `client_sessions.expires_at > clock_timestamp()`，将过期会话清理与孤儿回收完全交由后台 `RenewSession` 周期任务，在 `Write` 事务中移除全表清理/孤儿扫描并对同一 `session_id` 的租约续期做内存时间窗口节流，消除单客户端多线程并发写的 `client_sessions` 行锁争用；同时在服务端内存按步长批量预取 `slice_id` 号段，消除每次 `Write` 的独立 `NextSliceID` SQL 往返。
   - **两阶段流水线与单事务 Group Commit（对齐 RocksDB `WriteThread`）**：
     - **阶段 1（并行数据落盘）**：并发到达的 `Write` 请求各自从内存号段领取 `slice_id`，并行完成 `.tmp` 块文件写入、`Sync()` 与 `Rename`（打满多核与云盘多队列并发）。
     - **阶段 2（Leader-Follower 批量目录同步与单次 PG 事务提交）**：完成阶段 1 的请求链入无锁 `WriteThread` 队列，由 `BatchGroup Leader` 收集当前就绪批次：先对批次内涉及的父目录去重执行 `syncDir`，再在**单个 PostgreSQL 事务**内按 `ino` 升序加锁 `FOR UPDATE`、在内存中按到达顺序推演同 `Inode` 的写入偏移（含 `O_APPEND`）并拼接同 `(ino, indx)` 的 24B `Slice` 序列、按 `RootIno` 汇总执行 **1 次 `Disk` 配额校验与扣减**、批量插入 `slice_refs`、更新 `Inode` 属性并**全组共享 1 次 PG WAL `COMMIT`**，完成后批量唤醒全部 `Follower`。
     - **异常隔离与回退安全**：若批次内个别请求存在句柄失效（`EBADF`）、非文件类型或配额超限（`ENOSPC`），Leader 自动剔除异常请求单独返回对应错误并清理其未提交物理块，其余合法请求正常合并提交，100% 保持单请求错误语义与块回收不变量。

## 结果
- **积极影响**：在不修改任何 Protobuf 契约与数据库 Schema、不牺牲任何 POSIX 实时语义与跨客户端锁/配额/会话安全性的前提下，消除了大文件顺序写的 2.0x `Compaction` 写放大以及多并发写入时的 `client_sessions` / `disks` 单行锁排队与重复 `fsync` 开销，使单流与多并发写入均可逼近底层云盘物理极限。
- **代价考量**：服务端 `meta.Manager` 需维护轻量级的 `slice_id` 号段分配器与阶段 2 `WriteThread` 批次聚合及单条异常隔离回退逻辑。
