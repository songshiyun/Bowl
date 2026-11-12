# 0033. 客户端 Write-Behind 顺序写流水线、单飞多窗口预读与新目录完整流本地合成

## 状态
已采纳 (accepted) — 替代 [ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 与 [ADR-0028](0028-single-statement-atomic-cte-async-wal-and-smart-keep-cache.md) 中「客户端保持纯同步 Write-Through 直通」的约束，演进 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 与 [ADR-0029](0029-pebble-per-branch-storage-swmr-and-checkpoint-commit.md) 的客户端读写缓存模型。

## 背景
在单盘 `StoreActor` 与 `64 MiB` `.pack` 存储引擎（[ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md)~[ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md)）就绪后，客户端仍受限于三处往返与并发瓶颈：
1. **同步单块写限制吞吐且小块写放大 RPC 次数**：Linux FUSE 单次 `Write` 上限为 `1 MiB`（小块流式写更低至 `4 KiB~64 KiB`），同步逐笔等待 RTT 无法跑满单文件大块顺序写带宽；若直接盲目改为异步写，又会破坏 `Epoch` 过期拦截（`EBUSY`）、`S_ISUID/S_ISGID` 清理、`immutable/append` 权限校验、小配额 `ENOSPC` 以及多并发 `Write` RPC 在服务端 `256 KiB` 逻辑块上的锁竞争与属性乱序回写。
2. **内核并发预读跨窗口乱序导致预取击穿与单飞盲区**：Linux 内核 readahead 工作线程与用户线程并发下发读请求时，跨 `4 MiB` 窗口边界的晚到读请求会被单游标判定误识为随机跳读而清空预取流水线；同时在弹出在途预取槽位等待期间，后续并发读不可见该在途请求，触发重复 `4 MiB` `Read` RPC 与高额预取丢弃。
3. **新建目录紧邻遍历产生冗余网络往返**：诸如 `git clone`、`mdtest` 等负载在 `Mkdir` 与批量 `Create` 后立即执行 `ReadDir`/`Lookup`，而写端客户端本就握有新建空目录自诞生以来的完整子项集合。

## 决策

1. **受控激活、对齐分片的 `WriteBehindCoordinator`（`B* = 4 MiB`，`W* = 4`，句柄级 `errseq` 隔离）**
   - **前置同步探针与 `Create` 预热**：非新建打开句柄的大块流式写（`> 256 KiB`）须先累计完成 `2 MiB`（小块写完成首笔）同步直通探针，当场校验 `Epoch` 栅栏、`KILLPRIV`、`immutable/append` 标志与初始配额；对当前客户端刚通过 `Create` 创建的普通文件（非 `DirectIO`、无 `SUID/SGID`、无 `Immutable/Append`、非 `O_SYNC/O_DSYNC/O_APPEND`）直接调用 `SeedCreatedInode` 免探针进入内存 `4 MiB` 聚合槽（`stageBuf`）。
   - **`256 KiB` 物理块网格对齐与有界并发**：仅当聚合槽起始偏移对齐 `256 KiB`（`store.BlobSize`）且分片长度为 `4 MiB` 整倍数时才允许最多 `W* = 4`（`16 MiB`）个异步 `Write` RPC 并发在途，从数学上保证任意两个在途 RPC 覆盖互不相交的服务端 `256 KiB` 逻辑块，零 `inodeLocks` 局部块 CoW 冲突。
   - **乱序属性保护、观测屏障与句柄级 `errseq`**：异步提交回包仅在满足同流世代（`streamGen`）且 `Size`/`Ctime`/`Mtime` 单调不退（`isNotOlderCommittedInode`）时刷新本地属性缓存；`Read`、`GetAttr`、`SetAttr`、`Fallocate`、`ReadDir` 等观测/变更入口自动等待目标（或全局）缓冲排空；异步写失败通过递增文件级 `errSeq` 记录，在各打开 `FileHandle` 调用 `Flush`（`close(fd)`）或 `Fsync` 时按各自游标精准兑现一次错误并重置流状态。

2. **驻留槽位单飞与保留尾窗的 `ReadAheadCoordinator`（`W_read = 4`，全局在途上限 `32`）**
   - **随机小读零放大门控**：句柄首笔读请求及尺寸 `< 128 KiB`（`readAheadMinWant`）的非窗口内请求一律按原始尺寸直通，严禁将 `4 KiB` 随机读放大为 `4 MiB` 拉取。
   - **单调高水位 `streamEnd`、上一窗口 `prevWinBuf` 与原地 `windowSlot` 单飞**：以单调高水位 `streamEnd` 识别顺序流，窗口推进时保留刚滑出的上一 `4 MiB` 窗口 `prevWinBuf` 供内核乱序晚到读直接命中内存；所有前台按需窗口拉取与后台异步预取统一驻留在按偏移有序的 `slots` 切片中直至完成晋升或废弃，消除等待期间的单飞盲区。单流最多维持 `W_read = 4` 个预取窗口，跨句柄全局在途预取上限 `ReadAheadMaxGlobalInflight = 32`（`128 MiB`）并按活跃流水线流数公平分配槽位。

3. **`NamespaceCache` 新建目录完整流本地合成（`completeDirs`）与负缓存**
   - 客户端 `Mkdir` 创建的新目录在 `NamespaceCache` 中标记为完整目录（`completeDirs`），后续本地 `Create`、`Mknod`、`Mkdir`、`Symlink`、`Link`、`Unlink`、`Rmdir`、`Rename` 原地同步增删目录项缓存并维护父目录属性；在缓存未过期且未被容量淘汰前，`ReadDir`/`ReadDirPlus` 直接本地合成返回，未命中子项的 `Lookup`（先完成 `> 255B` `ENAMETOOLONG` 边界校验）直接短路返回 `ENOENT`（`0` 次服务端 RPC），同时对 `Readlink` 目标路径启用受 TTL 管控的本地缓存。
