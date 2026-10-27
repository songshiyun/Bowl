# 五大存储系统数据层全景对比与 Bowl 数据层（NVMe + S3 + 版本控制/克隆）架构调研总报告

> **关联分项报告**：
> - [JuiceFS 数据层调研报告](file:///Users/fanxing/code/starsrc/bowl/docs/research/data-layer-juicefs.md)
> - [Ceph (CephFS + BlueStore/SeaStore) 数据层调研报告](file:///Users/fanxing/code/starsrc/bowl/docs/research/data-layer-ceph.md)
> - [Cloudflare Computer (DOFS) 数据层调研报告](file:///Users/fanxing/code/starsrc/bowl/docs/research/data-layer-computer.md)
> - [Drive9 数据层调研报告](file:///Users/fanxing/code/starsrc/bowl/docs/research/data-layer-drive9.md)
> - [LayerFS 数据层调研报告](file:///Users/fanxing/code/starsrc/bowl/docs/research/data-layer-layerfs.md)

---

## 一、五大系统数据层核心机制横向对比矩阵

| 架构维度 | JuiceFS | Ceph (CephFS + BlueStore) | Cloudflare Computer (DOFS) | Drive9 | LayerFS |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **底层目标介质** | 对象存储（S3/OSS）+ 本地 SSD 缓存 (`disk_cache.go`) | 裸 NVMe SSD (`KernelDevice` / `io_uring` / SPDK) | Cloudflare DO 内嵌 SQLite BLOB（规划冷存 R2） | 小文件内联 TiDB，大文件 S3 + 本地 SSD `ShadowStore`/WAL | 全局单文件 SQLite (`pack` BLOB) + 工作区私有 `overlay.sqlite` |
| **分块粒度与层级** | **三层**：`Chunk`(64MiB) $\to$ `Slice`(变长追加) $\to$ `Block`(4MiB) | **两层**：CephFS `Object`(默认 4MiB) $\to$ BlueStore `lextent`/`Blob`/`pextent`(最小 4KiB) | **单层定长**：固定 `CHUNK_SIZE = 512 KiB` | **混合分层**：$\le 50\text{KB}$ 内联；8MiB 分段上传；或 64MiB Chunk + 24B Slice | **混合 CDC**：$<128\text{KiB}$ 整存；$\ge 128\text{KiB}$ 采用 GEAR FastCDC (8~32KiB) + 8KiB B+ 树 |
| **寻址方式** | `(ino, indx)` 查 DB 得 24B Slice 序列，拼接 `<slice_id>_<idx>_<size>` | **确定性数学条带化**：`Striper` 直接算 `<ino_hex>.<obj_no:08x>`，无集中块表 | `(inode, idx)` 查 `vfs_chunks` 得 SHA-256 `hash`（CAS 内容寻址） | 路径/Inode 查 `file_nodes`，Extent 模式同 JuiceFS `(inode, indx)` | `BLAKE3` 内容寻址（CAS）+ 不可变 Extent Tree 映射至 `(pack_id, group, record)` |
| **随机覆盖写模型** | **Append-only Slice 叠加**；读路径通过 `buildSlice` 二叉树动态裁剪 | SSD 上 **`prefer_deferred_size_ssd = 0`**，直接按 4KiB 原地写空洞或 COW 分配新块 | **块级 RMW**：读出 512KiB 旧块修改后重算 SHA-256 并替换映射行 | FUSE 侧写本地 `ShadowStore` + WAL；Extent 模式走 Slice 叠加与二叉树裁剪 | 热写路径写本地 `overlay.sqlite` 的 **4KiB Cell + 512B 有效性位图**，Commit 时才切块 |
| **本地 SSD 与 S3 协同** | `rawstaging/` 写回暂存 $\to$ `os.Link` 硬链接晋升至 `raw/` 读缓存 $\to$ 异步上传 S3 | 无原生 S3 主存路径（通过 RGW 对外提供 S3，或 Cache Tiering） | 热块留本地 SQLite，规划 `location='r2'` 将 $>4\text{MiB}$ 或冷 Blob 异步下沉至 R2 | 写先落本地 `<hash>.shadow` + `Journal` WAL 立即返回，后台 `commitQueue` 异步推 S3 | 本地 `overlay.sqlite` 承载可变工作区，Commit 时打包成不可变 `Packfile` |
| **版本控制与克隆** | 元数据层复制 `chunk` 表映射行并递增 `chunk_ref(slice_id)` 引用计数（零数据拷贝克隆） | 基于 `SnapContext` 与 BlueStore `Blob` 共享引用计数实现快照与 CoW | 基于 `rev` 修订号、`manifest_hash` 与全局 SHA-256 CAS 块共享实现快照同步 | 每个文件维护递增 `Revision` 与历史版本表，Extent 模式通过 `jfs_chunk_ref` 共享 Slice | 原生 Git 式树状历史（`Commit` / `Branch`），跨版本/跨分支通过 CAS + Zstd 前缀差分共享 |
| **稀疏文件与空洞** | 整块未写无 DB 行；局部空洞追加 `id=0` 的虚拟 Slice，读时内存填零 | `ExtentMap` 无记录即为空洞；开启零块检测时全零写入自动转为打洞 | **伪稀疏**：强制要求 `idx` 连续（否则报 `EIO`），空洞生成全零块靠 SHA-256 去重 | Extent 模式用 `id=0` Slice 表示空洞；本地 Shadow 文件利用 OS 原生稀疏文件 | 构建流使用 `FileRun::Zero`，对齐 32KiB 静态零块哈希子树，免内存分配 |
| **碎片整理与 GC** | 切片数超阈值触发 **Compaction** 重写上传；离线 GC 设 **1 小时 `mtime` 安全窗**防误删在途块 | **无 Slice Compaction**；MDS 将删除推入持久化 **`PurgeQueue`** 异步并发删对象 | Unlink 仅删 `vfs_chunks`；后台 `gc()` 扫描 `NOT EXISTS` 且超过 **1 小时安全窗**的孤儿 Blob | Slice 引用归零入队 `block_gc_tasks`（设 **60s 宽限期**）；Unlink 记 `jfs_delfile` 分批清理 | 全局 `pack` 表触发器禁删（永不删除，仅离线 `seal`）；局部 Overlay 按代退役异步清理 |

---

## 二、第一性原理重推演：当加入「先写本地 NVMe 再写 S3」+「版本控制与克隆」后，底层约束发生了什么质变？

如果仅仅做单机本地 POSIX 存储，按 `(ino, chunk_index)` 原地覆写（In-place Overwrite）是最简单的。但一旦引入 **1. 优先写本地 NVMe 再异步写 S3** 与 **2. 版本控制与克隆（Versioning & Clone）** 两大核心前提，从第一性原理出发，会产生三条铁律：

### 铁律 1：逻辑寻址 `(ino, chunk_index)` 与物理数据块 ID 必须解耦（Indirection & Immutability）
- **为什么不能原地覆写 `<ino>_<chunk_index>`？**
  1. **克隆与快照的零拷贝要求**：当克隆一个文件或克隆整个 `Disk`（或保留某个历史版本）时，源节点与目标节点必须共享相同的底层物理数据块。如果直接原地修改物理块，会同时篡改历史版本和克隆副本的数据（破坏快照隔离性）。
  2. **S3 对象的不可变要求**：S3 不支持局部 `pwrite`。如果本地 NVMe 上的物理块是可变且不断原地覆写的，后台向 S3 同步时将面临严重的同步撕裂（Torn Read during Upload）与整块重复上传的写放大。
- **必然推论**：所有落入持久化层（NVMe 缓存层 + S3 归档层）的物理数据块，一旦封盘（Seal/Freeze）就必须是**不可变的（Immutable）**，由全局唯一的物理 ID（递增 `slice_id` / `blob_id` 或内容哈希 `hash`）标识，并通过数据库映射表与引用计数（RefCount）管理生命周期。

### 铁律 2：高频随机小写必须在本地 NVMe 上吸收合并，决不能每次 4KB 写都直接生成 S3 对象
- 如果每次 `write(4KB)` 都直接变成一个不可变 S3 对象，不仅会在 S3 上产生海量极小对象（PUT 请求费用爆炸、列表与 GC 极慢），还会导致读路径需要拼接成百上千个碎片。
- 在我们调研的 5 个系统中，演化出了**三条吸收随机写并对接不可变后端（S3 / CAS）的经典路径**：

---

## 三、满足「NVMe 先写后上 S3 + 版本控制/克隆」的三大架构路线对比

### 路线 A：JuiceFS / Drive9 (Extent) 路线 —— 「Chunk(64MB) + 不可变 Slice/Block(4MB) + NVMe `rawstaging` Writeback + 引用计数克隆」
- **核心工作流**：
  1. **写路径（Write-back）**：写入在内存中按连续区间聚合为 `Slice`，切分为最大 `4 MiB` 的不可变 `Block`。开启 Writeback 时，Block 直接写入本地 NVMe 的 `rawstaging/` 目录（并通过 `os.Link` 硬链接到 `raw/` 读缓存目录），同时向 PostgreSQL 的 `chunk(ino, indx)` 表追加 24 字节 Slice 描述符，**立即向客户端返回成功**。
  2. **异步写 S3**：后台上传协程扫描 `rawstaging/`，将不可变 Block 异步上传至 S3，上传成功后仅删除 `rawstaging/` 下的路径，数据继续留在 `raw/` 目录作为本地 NVMe 高速读缓存（空间不足时按 `2-random` / LRU 淘汰已上云的冷块）。
  3. **克隆与版本控制**：克隆文件或克隆整个 `Disk` 时，直接在 PostgreSQL 事务内复制 `chunk` 表映射行，并对引用的所有 `slice_id` 在 `chunk_ref` 表执行 `refs = refs + 1`（纯元数据操作，零物理数据拷贝）。克隆后发生覆盖写时，只追加新 Slice，原版本与新分支互不干扰。
- **优势**：
  - 全链路无锁追加写，无需在写时读取旧块（免 RMW），也无需计算全文加密哈希；
  - 本地 NVMe 与 S3 采用完全相同的 `<slice_id>_<block_idx>_<size>` 不可变块格式，硬链接零拷贝晋升读缓存极其优雅。
- **代价**：
  - 高频随机覆盖写会产生多层重叠 Slice，读路径需执行 [`buildSlice`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go#L134) 二叉树区间裁剪，且需要后台 **Compaction** 定期合并碎片（注意：在有克隆/历史快照引用旧 Slice 时，Compaction 只会降低旧 Slice 的 `refs` 为 `refs - 1`，当 `refs == 0` 时才由 GC 物理删除 NVMe 与 S3 上的块）。

### 路线 B：Drive9 (`ShadowStore`) + DOFS 混合路线 —— 「本地 NVMe 原地写影子缓存（Write-back Shadow）+ 定长 Chunk (1~4MiB) CoW 快照上云」
- **核心工作流**：
  1. **活跃热写期（NVMe In-place Shadow）**：当文件处于打开活跃写入期（Working state），直接在本地 NVMe 的 `shadow/` 目录下按定长 Chunk（如 `4 MiB`）原地 `pwrite` 覆写，并写入本地轻量 WAL / PostgreSQL 脏标记。此时无论随机覆写多少次 4KB，都在本地 NVMe 原地吸收，**零写放大、零 Slice 碎片、无需 Compaction**。
  2. **封盘与异步上云（Seal & Upload to S3）**：当文件句柄 `Flush`/`Release`、或者经过短暂冷却期（Cooldown）、或者用户显式触发**版本提交/快照/克隆**时，将发生过修改的脏 Chunk 分配新的不可变 `blob_id`（或计算 `BLAKE3` 哈希），冻结为只读块并推入后台队列异步上传至 S3，同时更新 PostgreSQL 中的 `chunk_mappings(ino, chunk_index) -> blob_id` 并维护 `blob_refs(blob_id, refs)`。
  3. **克隆后再修改（Copy-on-Write）**：若某 Chunk 的 `refs > 1`（被历史版本或克隆盘共享）或已上传 S3 冻结，当再次发生局部覆盖写时，先从本地 NVMe 读缓存（未命中则从 S3 拉取）拷贝一份到新的可变 Shadow Chunk 中修改（塊级 CoW），原 `blob_id` 引用计数减 1。
- **优势**：
  - **读路径永远是 $O(1)$ 单层直读**（每个 `(ino, chunk_index)` 永远只对应唯一一个最新的 `blob_id`），彻底消灭了 JuiceFS 的 `buildSlice` 二叉树裁剪和复杂的后台 Compaction！
  - 热文件高频随机覆写（如 SQLite、编译产物、日志高频刷写）不会产生海量微型 S3 对象碎片，只有在 Seal/Flush 后才以整块上传 S3。
- **代价**：
  - 对已冻结的共享大块做局部小修改时，需要在本地 NVMe 上做一次块级 Copy-on-Write（因此 Chunk 大小宜选 **1 MiB ~ 4 MiB**，在 NVMe 上拷贝 1~4 MiB 仅需 0.2~1 毫秒）。

### 路线 C：LayerFS 路线 —— 「本地 NVMe Overlay (4KB Cell) + 显式 Commit 触发 FastCDC 内容定义分块 + Packfile」
- **核心工作流**：
  - 日常 POSIX 读写全部落在工作区私有的 `overlay`（4KB Cell + 位图），仅在显式调用 `Commit` 时才对变更文件跑 FastCDC 滚动哈希（8~32KB 变长块）、计算 BLAKE3、做 Zstd 前缀差分并打包成 Packfile 上传。
- **优势**：
  - 跨文件、跨版本、甚至文本中间插入几个字节后的去重率达到极致。
- **代价**：
  - 读路径需要穿透多层（Overlay Cell $\to$ Piece $\to$ Extent B+ 树 $\to$ Packfile $\to$ Zstd 前缀链解压）；且 Packfile 将多个块绑在一起导致单个块极难做在线 GC 物理删除（LayerFS 直接放弃了全局 GC）。

---

## 四、Bowl 数据层最终敲定架构决策（已沉淀至 [ADR-0019](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0019-chunk-slice-block-nvme-data-layer.md)）

经过多轮架构对齐，Bowl 数据层正式确立采用**「路线 A：`Chunk (64MiB) -> Slice (24B 元数据描述符) -> Block (<=4MiB 不可变碎片文件)` + PostgreSQL 元数据层合并 + 本地 NVMe SSD 落盘（预埋 S3 与克隆引用计数底座）」**，核心决策详见 [ADR-0019](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0019-chunk-slice-block-nvme-data-layer.md)：

1. **服务边界与协议契约**：由 `StorageService`（[`proto/storage/v1/storage.proto`](file:///Users/fanxing/code/starsrc/bowl/proto/storage/v1/storage.proto)）统一内聚提供 `Read` 与 `Write` RPC，单次请求最大载荷设定为 `4 MiB`；支持服务端 `O_APPEND` 原子追加定址、自动刷新 `mtime`/`ctime` 并剥离 `S_ISUID`/`S_ISGID`。
2. **三层不可变切片与元数据合并**：
   - `chunks(ino, indx, slices)` 表按 `64 MiB` 划分逻辑 `Chunk`，每次写入追加 24 字节定长 `Slice` 记录 `[pos:4B][id:8B][size:4B][off:4B][len:4B]`；
   - 底层通过抽象 `BlockStore` 接口将 `Slice` 载荷按最大 `4 MiB` 切分为不可变物理 `Block` 文件，以两级 Hex 目录散列落盘至本地 NVMe SSD；
   - 读路径在内存通过区间裁剪树（`buildSlice`）合并重叠 `Slice`，空洞（`id == 0`）直接内存填零。
3. **分阶段演进与 `slice_refs` 引用计数底座**：第一阶段完整闭环本地 NVMe SSD 数据读写、元数据合并、后台 `Compaction` 与异步物理 GC，同时一步到位建立 `slice_refs(slice_id, size, refs)` 表与 `BlockStore` 抽象，为第二阶段「NVMe Write-back 异步上传 S3」及「零拷贝克隆/版本控制」提供零返工底座。
4. **配额与存量操作单事务联动**：继续遵循 [ADR-0015](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0015-unify-disk-and-storage-service-with-transactional-quota.md) 按文件逻辑 `size` 核算 `Disk.used_bytes`；`SetAttr`（截断）、`Open(O_TRUNC)`、`Fallocate`（打洞）、`Unlink`、`Rename`、`Release`、`RenewSession` 与 `DeleteDisk` 在同一 PostgreSQL 事务内更新 `chunks` 与 `slice_refs`，由后台 GC 协程异步清理 `refs <= 0` 的 NVMe 物理文件。
