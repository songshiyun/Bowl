# 0031. 基于 ShardStore 的 64 MiB Append-Only Pack 封包存储、256 KiB 逻辑分块与 CAS 搬迁回收

## 状态
已采纳 (accepted) — 替代 [ADR-0029](0029-pebble-per-branch-storage-swmr-and-checkpoint-commit.md) 与 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 的独立 `.blob` 数据层；§4.1 跨分支封口硬链接已随 [ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md) 单盘化裁撤，封包渐进回写、后台死包回收与连续冷块合并读由 [ADR-0034](0034-h2c-striped-transport-zero-alloc-codec-and-pack-io-coalescing.md) 演进

## 背景
在 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 的 `StoreActor` 架构下，元数据层已实现单协程 `IndexedBatch` Group Commit 与 100% 锁外并发直读，但底层数据层（Data Layer）沿用了「1 个逻辑块对应宿主机 1 个独立 `blobs/<00..ff>/<id>.blob` 文件」的模型，暴露出以下四个根源问题：
1. **宿主机小文件爆炸与高频 VFS 元数据系统调用**：每个块独立建文件导致 `10 GiB` 数据产生数万至十万个宿主机小文件，无法常驻打开文件描述符（`fd`）；每次未命中热缓存的 `Read` 与每次新块 `Write` 都必须执行 `open + pread/pwrite + close`，且覆写与删除必须在阶段 4 同步执行 `os.Remove`（触发宿主机 `ext4/xfs` 目录项日志更新）。
2. **`PatchLog` 原地修改与并发无锁 `Read` 竞态**：为规避小块写频繁新建 `.blob` 文件，[ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 引入了复杂的 `WindowLocal` 判定与 `PatchLog`（阶段 3 原地 `WriteAt + Truncate`），并在阶段 4 同步删除被替换旧块，迫使无锁 `Read` 编写 `ENOENT` 重试循环。
3. **`1 MiB` 大粒度块的随机覆写放大**：此前为压住宿主机 `.blob` 文件总数，被迫将逻辑块定为 `1 MiB`，导致已固化块发生 `4 KiB` 随机覆写时需读改写整整 `1 MiB`（256 倍写放大）。
4. **`Commit` / `Fork` 遍历 256 个子目录逐块硬链接开销**：生成快照或派生子分支时需遍历 `00..ff` 共 256 个子目录对数万个 `.blob` 逐个调用 `os.Link`。

借鉴 AWS S3/EBS 单机存储引擎 **ShardStore (SOSP '21)** 的「无值 LSM（Value-less LSM）+ 紧凑 `Locator` 指针 + 数十 MiB 只追加 Extent（Append-Only Pack）+ 软更新（Soft Updates）崩溃一致性 + 原子 CAS 搬迁回收」架构，以及 AWS EBS 在线 SSD 卷（`gp2/gp3/io1/io2`）固定 **`256 KiB`** 最大 I/O 切片的设计，我们对 `services/storage/internal/store` 的数据存储层进行重构。

---

## 决策

### 1. 逻辑分块网格（`256 KiB`）与 `64 MiB` 单分支 `.pack` 物理容器

1. **逻辑分块大小（`BlobSize = 256 KiB`）**：
   - 将常规文件的固定逻辑分块网格从 `1 MiB` 调整为 **`256 KiB`（`256 << 10 = 262,144` Bytes）**，直接对齐 AWS EBS SSD 在线卷的 `256 KiB` 最大 I/O 切片粒度。
   - 文件内字节偏移 `offset` 映射到逻辑块序号 `blockIdx = offset / 256 KiB`、块内偏移 `inBlock = offset % 256 KiB`。
   - 一次 Linux FUSE `MaxWrite (1 MiB)` 顺序写最多跨越 4 个连续 `256 KiB` 逻辑块（在同一个 Pebble `IndexedBatch` 中原子提交），同时将已封口块的 `4 KiB` 随机覆写 CoW 写放大从 256 倍压降至 **64 倍**（缩小 4 倍）。
   - **严禁变长区间分裂（杜绝 Ceph BlueStore `ExtentMap` 雪崩）**：每个 `256 KiB` 逻辑块网格在 Pebble 中永远只对应 **1 条** `DataKey(ino, blockIdx)`，局部覆写绝不在 LSM 树中拆裂为前、中、后多条子区间记录。
2. **物理封包容器（`<branch_dir>/packs/<pack_id>.pack`，上限 `64 MiB`）**：
   - 废除二级分片目录 `blobs/<00..ff>/<id>.blob`，改为每个分支/快照目录下扁平的 **`packs/<pack_id_hex16>.pack`**。
   - 单个 `.pack` 文件的封口容量上限锁定为 **`PackMaxSize = 64 MiB`（`64 << 20` Bytes）**：
     - 相比独立块文件，宿主机文件数量锐减 **250~1000 倍**（`10 GiB` 数据仅约 `160` 个 `.pack` 文件），所有 `.pack` 的只读 `*os.File` 句柄均可常驻内存池，彻底消除读写路径上的 `open` / `close` 系统调用；
     - 相比 `1 GiB ~ 4 GiB` 超大文件，`64 MiB` 上限彻底规避了冷热数据混杂导致的「1% 活数据绑架整包」与后台 GC 搬迁卡死前台 I/O 的写停顿（Write Stall）。
   - 每个逻辑块按其**实际有效字节长度 `Length`（`1 .. 256 KiB`）**在活跃 `.pack` 尾部紧凑追加，不足 `256 KiB` 的小文件或尾块不预留空洞。

---

### 2. Pebble 索引编码：16 字节 `BlobRef` (`Locator`) 与 `.pack` 存活字节记账

1. **数据块定位指针（`DataKey` $\rightarrow$ `16 B BlobRef`）**：
   - 键：`"data:" + uint64(ino) + ":" + uint64(blockIdx)`（22 B，Prefix Bloom 切分前 14 B `"data:<8B_ino>:"`）。
   - 值：从 12 B 扩展为 **`16 B` 定长 `BlobRef`（即 ShardStore `Locator`）**：
     ```
     [ PackID (8B BigEndian) | Offset (4B BigEndian) | Length (4B BigEndian) ]
     ```
     其中 `PackID > 0` 为所属 `.pack` 文件编号，`Offset` 为块在 `.pack` 文件内的起始字节偏移，`Length`（`1 .. 262144`）为块的有效字节长度，需满足 `uint64(Offset) + uint64(Length) <= PackMaxSize`。
2. **`.pack` 存活字节元数据键（`PackStatKey`）**：
   - 键：`"sys:pack:" + uint64(packID)`（17 B，全键参与 Bloom 过滤）。
   - 值：**`17 B` 定长 `PackMeta`**：
     ```
     [ ver(1B) | LiveBytes(8B BigEndian) | TotalBytes(8B BigEndian) ]
     ```
   - `StoreActor` 在内存中维护 `packStats map[uint64]PackMeta`（启动时通过扫描 `"sys:pack:"` 前缀恢复）：
     - 在 Phase B 每当新增、覆盖或删除 `DataKey` 时，同步在内存中增减对应新旧 `PackID` 的 `LiveBytes`（及活跃包的 `TotalBytes`），并在 Step 3 收敛批次时将本批发生变动的 `PackID` 通过 `SetDeferred` / `DeleteDeferred` 写入 `s.batch`，与 `DataKey` 处于同一个 Pebble 原子批次。

---

### 3. 严格 Append-Only 写入流水线（废除 `PatchLog` 原地覆写与前台 `os.Remove`）

1. **常驻活跃 `.pack` 写句柄与只读句柄池**：
   - 每个可写 `StoreActor` 的 `BlobStore` 维护最多 **1 个** 当前活跃可追加封包（`activePack`），持有常驻打开的读写 `*os.File` 句柄与当前写指针 `writeOffset`；
   - 当 `writeOffset + len(chunk) > PackMaxSize`（`64 MiB`）时，将当前活跃 `.pack` 封口转为只读（移入只读句柄缓存表 `openFiles map[uint64]*os.File`），并分配新 `packID` 创建下一个活跃 `.pack`。
2. **纯追加写 + 活跃包最末尾同块顺延扩展**：
   - **砍掉 `PatchLog`、砍掉 Stage 4 同步 `os.Remove`、砍掉 `Read` 的 `ENOENT` 重试**：
     - 已写入 `.pack` 的历史偏移区间一旦被后续写入跨过，**永不原地覆写、永不调用 `Truncate`、不在前台同步删除物理文件**；
     - 无锁并发 `Read` 通过 `BlobRef{PackID, Offset, Length}` 先查 `hotShard`（以 `(PackID, Offset)` 或块引用定位），未命中则直接对常驻 `*os.File` 调用 `ReadAt(dst, int64(ref.Offset))`（**0 `open/close`、0 撕裂读、永不触发 `ENOENT`**）。
   - **唯一允许的原地尾部顺延（Tail Extension）**：
     - 在 `inodeLocks[ino & 0xfff]` 保护下，当且仅当目标逻辑块的旧 `BlobRef` 恰好位于当前 `activePack` 的**最末尾**（即 `oldRef.PackID == activePackID && oldRef.Offset + oldRef.Length == activeWriteOffset`）且扩展后不超过 `PackMaxSize` 时，允许直接从 `oldRef.Offset + patchOffset` 处写入新增/修改字节并将 `activeWriteOffset` 顺延推进至 `oldRef.Offset + newLength`（连续流式小块 `append` **0 写放大**）；
     - 其余所有整块写与局部写（从 `hotShard` 或常驻 `fd` 读取 `256 KiB` 旧块合并后）一律在 `activePack` 的 `activeWriteOffset` 处原子预留新区间并执行 1 次 `WriteAt` 追加写入，生成新的 `BlobRef{PackID, NewOffset, NewLength}`。
3. **Soft Updates 崩溃一致性与 `Fsync` 屏障**：
   - **先写 `.pack` 数据、后提交 Pebble 指针**：Stage 1 在入队前完成向 `activePack` 的 `WriteAt`（数据进入 OS Page Cache）；若 Phase A 校验失败，仅在 `activePack` 中留下一段无任何 `DataKey` 指向的未引用字节（`LiveBytes` 不增加），**绝不污染任何已有块的数据**。
   - **精准 `Fsync` 与目录屏障最小化**：
     - `Fsync(ino)` 时，`ApplyNoSyncWait(s.batch, pebble.Sync)` 与数据层 `SyncInode(ino)` 并行执行；
     - 数据层仅需对包含该 `ino` 未刷盘写入的 `.pack` 常驻 `*os.File` 调用 `fdatasync`（通常仅 1 个 `activePack` 句柄，省去原方案对数千个 `.blob` 并发 `open + fdatasync + close` 的开销），且仅在新建 `.pack` 文件首次落盘时对 `packs/` 目录执行 1 次目录 `fsync`。

---

### 4. `Commit` / `Fork` 封口硬链接与 ShardStore 式 CAS 搬迁回收（GC）

1. **`Commit` / `Fork` 封口与扁平硬链接共享**：
   - 对分支执行 `Commit` 或 `Checkpoint` 前，先调用 `SealActivePack()` 将当前 `activePack` 封口并刷盘，随后将 `<branch_dir>/packs/*.pack` 硬链接（`os.Link`）至目标快照/子分支的 `packs/` 目录（`10 GiB` 仅约 `160` 次 `os.Link`，耗时 `< 2 ms`）。
   - 封口后的 `.pack` 绝不追加或修改，确保跨分支与历史 `Commit` 共享同一物理 Inode 时的绝对不可变性。
2. **基于 `StoreActor` Phase A CAS 栅栏的 `.pack` 垃圾回收（`CompactPacks`）**：
   - **水位线触发与全死包秒删**：
     - 当某个已封口 `.pack`（`packID != activePackID`）的 `LiveBytes == 0` 时，无需搬迁任何数据块，只需等待当前 Pebble WAL 持久化屏障完成后，直接关闭其只读 `*os.File` 句柄并调用 `os.Remove` 删除该 `.pack` 文件（若历史 `Commit` 仍通过硬链接持有该文件，宿主文件系统自动保留底层 Inode 直至快照删除，**0 跨分支引用计数开销**）。
     - 当已封口 `.pack` 的 `LiveBytes > 0` 且存活率低于阈值（默认 `LiveBytes * 2 <= TotalBytes` 且 `TotalBytes >= 4 MiB`，或由显式 `CompactPacks` 触发）时，执行 **ShardStore 四步安全搬迁**：
       1. **定位候选活块**：扫描当前分支 Pebble `DataKey`（或按搬迁计划读取指向源 `srcPackID` 的 `(ino, blockIdx, oldRef)`）；
       2. **读取并追加至新包**：在队列外从 `srcPackID` 读取活块字节，追加写入当前 `activePack` 获得 `newRef`；
       3. **Actor Phase A 原子 CAS 校验（防 ShardStore Bug #1：并发覆写幽灵回滚）**：将搬迁命令投递至 `StoreActor`，在 Phase A 中通过 `s.batch.Get(DataKey(ino, blockIdx))` 严格校验当前 `curRef == oldRef`：
          - 若 `DataKey` 已被前台并发删除或已指向别的 `BlobRef`，直接跳过该块的指针更新，并将刚预写到 `activePack` 的 `newRef` 记为死空间（**100% 杜绝后台 GC 覆盖用户新写或复活已删除文件**）；
          - 若 `curRef == oldRef`，在 Phase B 中将 `DataKey` 原地更新为 `newRef`，并同步扣减 `srcPackID` 的 `LiveBytes`、累加 `newRef.PackID` 的 `LiveBytes`。
       4. **持久化屏障后删旧包（防 ShardStore Bug #2：未 Flush 即删旧包导致断电悬空指针）**：当 `srcPackID` 的 `LiveBytes` 归零后，**必须先完成目标新 `.pack` 的 `fdatasync` 与 Pebble `LogData(nil, pebble.Sync)`（确保所有新 `BlobRef` 与 `sys:pack` 删除均已落盘）**，随后才关闭并 `os.Remove` 源 `.pack` 文件。

---

## 结果
- **正面效果**：
  - **宿主机文件数锐减 250~1000 倍，读写路径 0 `open/close/unlink`**：`64 MiB` `.pack` 封包配合常驻 `*os.File` 句柄池，使未命中 `hotShard` 的读写直接退化为单次 `pread` / `pwrite` 系统调用；`Fsync` 通常只需对 1 个活跃 `.pack` fd 执行 `fdatasync`。
  - **随机覆写放大降低 4 倍，代码大幅简化**：逻辑块对齐 EBS SSD 的 `256 KiB` 粒度，同时通过严格 Append-Only + 尾部顺延扩展，彻底消除了 `PatchLog` 原地 `Truncate`、Stage 4 同步删文件以及并发 `Read` 的 `ENOENT` 重试。
  - **形式化验证级 GC 与崩溃安全**：完整落地 ShardStore (SOSP '21) 的 Soft Updates 定序与 Actor Phase A CAS 搬迁校验，结合单分支硬链接封口共享，在不引入跨分支分布式引用计数的前提下实现安全的空间回收。
- **代价与取舍**：
  - 覆写与删除不再于当前 RPC 结束时立即缩小宿主机文件体积，而是待对应 `.pack` 内全部块死亡（`LiveBytes == 0`）或触发低水位 `CompactPacks` 搬迁后按 `.pack` 粒度整包回收。
