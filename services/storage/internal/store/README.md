# Store (Labs 实验性新一代存储引擎)

## 定位与目标

`store` 是 `services/storage` 下的新一代实验性存储引擎（Labs 第一步），完整架构决策见 [ADR-0030](../../../../docs/adr/0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 与 [ADR-0031](../../../../docs/adr/0031-shardstore-append-only-pack-storage-and-256kib-block-grid.md)。

本模块采用**完全重写（Clean-slate）**的方式设计，不迁就现有 `meta`、`pebblekv` 或 `server` 的历史实现包袱，唯一目标是追求**极致的性能与清晰的架构边界**。

## 核心架构：Meta + Data 融合的 Actor 模型与 64 MiB Append-Only Pack 数据层

`store` 将 **文件元数据层（Meta Layer）** 与 **真实数据存储层（Data Layer）** 内聚封装为统一的 **`StoreActor` 架构模型**：

1. **Actor 隔离与双阶段 Group Commit（Share-Nothing per Actor）**
   - 每个 `StoreActor` 独占管理单个分支（或只读快照）的文件元数据状态、`sys:pack` 存活字节记账与底层数据读写通道。
   - 写请求通过双缓冲 MPSC 队列进入单协程主循环，按 **Phase A（纯内存只读预校验）$\rightarrow$ Phase B（零拷贝 `SetDeferred`/`DeleteDeferred` 批次编码）$\rightarrow$ 单次 `IndexedBatch` 提交** 批量收敛；读请求 100% 在队列外无锁直读。
2. **文件元数据层（Meta Layer）**
   - 采用 8 类大端序定长/前缀键（`sys:stat`、`sys:pack:<packID>`、`inode:<ino>`、`entry:<parent>:<name>`、`xattr:<ino>:<name>`、`dirty:<ino>`、`orphan:<ino>`、`data:<ino>:<blockIdx>`）配合 Prefix Bloom Filter，消除高频操作下的多余比较与内存分配。
3. **`256 KiB` 逻辑分块网格与 `64 MiB` Append-Only `.pack` 封包容器（ADR-0031）**
   - **逻辑分块（`BlobSize = 256 KiB`）**：对齐 AWS EBS SSD 最大 I/O 切片粒度，每个逻辑块在 Pebble 中严格对应 1 条 `DataKey(ino, blockIdx) -> 16B BlobRef{PackID(8B), Offset(4B), Length(4B)}`（绝不变长区间分裂），按实际有效字节长度 `Length`（`1..256 KiB`）紧凑追加。
   - **扁平 `.pack` 容器与常驻句柄池**：数据存储在 `<branch_dir>/packs/<pack_id_hex16>.pack`（单包封口上限 `PackMaxSize = 64 MiB`），维护 1 个常驻活跃可写句柄与已封口只读句柄表 `packFiles`，配合 `64 MiB` 分片环形热块缓存（`hotShard`），实现读写路径 `0` 次 `open/close/unlink` 系统调用。
   - **严格 Append-Only + 活跃包尾部顺延（`ExtendTail`）**：历史已写偏移永不原地覆写或 `Truncate`；当目标块恰好位于当前活跃 `.pack` 最末尾时直接原地顺延写指针（流式追加写 `0` 写放大），其余局部修改合并后追加写入活跃 `.pack` 尾部。
4. **`Commit` 封口硬链接与 ShardStore 式 CAS 搬迁回收（`CompactPacks`）**
   - **封口硬链接（`SealActivePack` + `LinkAllTo`）**：快照或子分支派生前先将活跃 `.pack` 封口刷盘，再将扁平 `packs/*.pack` 硬链接至目标目录。
   - **实时存活记账与全死包秒删**：在 Pebble 中通过 `sys:pack:<packID>`（17B `PackMeta{ver, LiveBytes, TotalBytes}`）实时维护各包存活字节；已封口包 `LiveBytes == 0` 时在 WAL 持久化屏障完成后自动关闭句柄并物理删除 `.pack` 文件。
   - **低水位 CAS 搬迁回收（`CompactPacks`）**：针对 `LiveBytes > 0 && LiveBytes * 2 <= TotalBytes` 的已封口包，扫描活块追加至新包并通过 `opCompactRelocate` 在 Phase A 严格执行 `curRef == oldRef` 原子 CAS 校验（防御并发覆写幽灵回滚与死文件复活），在完成新包 `fdatasync` 与 Pebble WAL `Sync` 双重持久化屏障后删除旧 `.pack` 文件。
