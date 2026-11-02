# 0027. 逻辑分块（Chunk）收敛为 4 MiB 边界、满 64 条切片全量合并与后台单线程 Compaction/GC

## 状态
已采纳 (accepted) — 演进 [ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)、[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)、[ADR-0022](0022-server-write-group-commit-and-prefix-skipping-compaction.md) 与 [ADR-0026](0026-page-cache-block-persistence-and-wal-only-fsync.md)

## 背景
Bowl 的整体存储架构以**公网对象存储（S3）**保障数据最终持久化安全，同时在存储服务端将**本地大容量 NVMe SSD 作为超级冷热缓存池**：热数据常驻本地 SSD 直接响应读写，后台异步与公网 S3 协同同步。

在阿里云双机 Benchmark（Commit `df5cb52`）中，`fio` 4 并发随机覆写测试（`direct_randwrite_4jobs`，在已写满的 4×1 GiB 文件上随机覆写 1 MiB 数据块）中途触发客户端 `EIO` 超时失败。排查确认原 `64 MiB Chunk` 与后台并发整理机制在随机覆盖写场景下存在三个连环缺陷：
1. **跨区间合并为 `64 MiB` 大 `Slice` 导致最高 64 倍读写放大与公网重传浪费**：原方案将 `ChunkSize` 设为 `64 MiB`，后台 `CompactChunk` 会将 `Chunk` 内相邻的有效数据合成为最大 `64 MiB` 的超大 `Slice`（底下挂载 16 个 `4 MiB` 的 `Block` 文件）。当随机写修改了某 `1 MiB` 数据时，从该位置往后的几十兆未修改数据被连带从本地盘读出并重写；未来对接公网 S3 时更会导致「改 1 MiB 却需重传 64 MiB」的流量浪费。
2. **`skipSome` 防抖条件存在漏洞**：原 `CompactChunk` 仅在开头存在可跳过切片（`skipCount > 0`）时才检查剩余切片数量是否达到 `CompactSliceThreshold (64)`；一旦合成了从偏移 0 开始的 `64 MiB` 大 `Slice`，后续任意覆盖该大 `Slice` 的随机修改都会使 `skipCount == 0`，导致防抖失效，演变为每轮把整页 `64 MiB` 重抄一遍。
3. **后台整理协程无并发上限**：`4 GiB` 文件共 64 个 `Chunk` 在随机写时同时触发数十个后台整理协程争抢磁盘与内存，触发 Linux 内核脏页限流（`balance_dirty_pages`），导致前台写入阻塞超过客户端 `5s` 超时线。

## 决策
1. **将 `ChunkSize` 直接从 `64 MiB` 收敛为 `4 MiB`（与 `BlockSize = 4 MiB` 1:1 对齐，演进 ADR-0019）**：
   - 将 `ChunkSize` 从 `64 MiB`（`1 << 26`）调整为 `4 MiB`（`1 << 22 = 4,194,304` 字节），与物理块大小 `BlockSize (4 MiB)` 完全一致；PostgreSQL `chunks(ino, indx, slices)` 表结构无需改动（`indx = offset / 4 MiB`）。
   - **天然隔离 `4 MiB` 边界**：写入、截断（`SetAttr`）与打洞（`Fallocate`）原本即按 `ChunkSize` 切分，改为 `4 MiB` 后，任何 `Slice`（含空洞 `Slice`）长度均不超过 `4 MiB` 且绝不跨越 `4 MiB` 边界；每个实心 `Slice` 在底层最多只对应 1 个物理 `Block` 文件（`block_idx = 0`）或 1 个 S3 对象。
   - **保持 Append-Only 纯增量追加**：前台 `Write` 无论是写 `1 KiB`、`4 KiB` 还是 `1 MiB`，只将该次修改的数据写入独立物理块文件，并向对应 `4 MiB Chunk` 的 `chunks.slices` 末尾追加 24 字节 `Slice` 记录（若单次写入跨越 `4 MiB Chunk` 边界，沿用现有跨 `Chunk` 逻辑将描述符拆入相邻两个 `Chunk` 并记 `slice_refs.refs = 2`），不在前台写事务内读取或清理旧切片。
2. **移除 `skipSome`，仅当单个 `4 MiB Chunk` 累积满 `64` 条 `Slice` 时一次性合并（演进 ADR-0022）**：
   - **顺序写天然不触发后台合并**：Linux FUSE 单次最大写入 `1 MiB`，顺序写满一个 `4 MiB Chunk` 仅产生 `4` 条 `Slice`（即使按 `128 KiB` 顺序写也仅 `32` 条），远低于 `CompactSliceThreshold (64)`，无需 `skipSome` 即可保证顺序写零后台重整。
   - **小修改攒满 64 条才一次性压平为 `<= 4 MiB` 新块**：平时零星小修改仅追加 `Slice` 记录，读取时通过内存区间树（`BuildSlice`）直接叠加最新视图返回，不启动任何后台合并；只有当同一个 `4 MiB Chunk` 内累积满 `64` 条 `Slice` 时，才触发 `CompactChunk` 将这 64 条小 `Slice` 一次性合并为紧致的新 `Slice`（无空洞时直接变成 1 条最大 `4 MiB` 的 `Slice`），并将被替换的 64 条旧 `Slice` 引用计数减 1。
   - **删除 `skipSome` 根除防抖漏洞**：移除 `skipSome` 前缀跳过逻辑，阈值防抖条件统一简化为 `enforceThreshold && len(rawSlices) < m.compactSliceThreshold (64)` 时直接短路返回，杜绝 `skipCount == 0` 引发的重复重写。
3. **设立全局唯一的单线程后台 Worker，串行执行 `Compaction` 与废弃块回收（`GC`）**：
   - 移除 `maybeTriggerAsyncCompact` 中按 `Chunk` 直接 `go func()` 派生无上限后台协程的做法，改为向全局唯一的单线程后台任务队列非阻塞去重投递 `(ino, indx)`。
   - 单线程后台 Worker 每次从队列取出一个 `(ino, indx)` 完成 64 条 `Slice` 的合并替换并扣减旧 `slice_refs` 后，**立即在同一线程内清理 `refs <= 0` 的废弃物理块文件**，同时由该单线程按定时周期（`gcInterval`）兜底执行过期会话清理、孤儿 `Inode` 回收与未登记孤儿块对账（`CollectGarbage`）。后台重整与物理删块全程严格单线程排队推进，永不与前台读写争抢本地 SSD 带宽。
4. **`PutBlock` 直接写入目标块路径并放宽客户端默认 RPC 超时（演进 ADR-0020 / ADR-0026）**：
   - 由于每个 `slice_id` 全局唯一且只追加不原地修改，阶段 1 写完物理块后才进入阶段 2 提交数据库事务，`LocalFSBlockStore.PutBlock` 直接创建并写入目标块文件 `<slice_id>_<block_idx>_<block_size>`（若写入或关闭失败则 `os.Remove` 清理），省去每次先建 `.tmp_*` 临时文件再 `os.Rename` 的双倍本地目录操作开销。
   - 将客户端 `DefaultRPCTimeout` 从 `5s` 调整为 `30s`，消除极端云盘脏页回写抖动下的误报 `EIO`。

## 结果
- **正面效果**：
  - 将 `ChunkSize` 与 `BlockSize` 统一收敛为 `4 MiB`，既无需在 `64 MiB Chunk` 内再套一层槽位概念，又彻底消除了跨 `4 MiB` 区间连带重写导致的 30~64 倍写放大与未来 S3 重传流量浪费。
  - 每次读取、追加与合并操作涉及的 `chunks.slices` 元数据规模缩小至原来的 `1/16`，顺序写永不触发后台合并，高频小修改攒满 64 条才做一次最大 `4 MiB` 的合并。
  - 全局单线程后台 Worker 串行执行合并与删块，结合 `PutBlock` 直写目标路径与客户端 `30s` 超时，彻底消除后台争抢磁盘带宽引发的前台超时报错。
- **代价与取舍**：
  - 相同文件大小在 PostgreSQL `chunks` 表中的行数变为原来的 16 倍（例如 `1 GiB` 写满文件从 16 行变为 256 行），但每行的 `slices` 二进制列体积缩小为原来的 `1/16`，单次 `Read` / `Write` 最多只访问 1~2 行，PostgreSQL MVCC 行更新写放大反而更低。
