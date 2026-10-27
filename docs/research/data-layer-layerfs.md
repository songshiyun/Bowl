# LayerFS 数据层深度调研报告：内容寻址、分块引擎、工作区缓存与物理落盘机制

> **文档标识**：BOWL-RESEARCH-LAYERFS-DATA-LAYER  
> **调研基准**：LayerFS 核心源码（`core/crates/`）与设计规范（`docs/general/`、`core/docs/`）  
> **编写规范**：ASD-STE100（简明技术英文规范转化之简体中文工程规范），术语一致、逻辑严密、结论均附有一手源码行号与符号证据。  
> **目标系统参考**：Bowl 存储系统（PostgreSQL 元数据引擎 + 本地 NVMe SSD 分块落盘）。

---

## 1. 架构总览与分层职责边界

LayerFS 的核心数据架构划分为两大工作流与边界正交的六大核心 Crate（参见 [`docs/general/cluster_one_handbook.md:65-98`](file:///Users/fanxing/code/other/filesystem/layerfs/docs/general/cluster_one_handbook.md#L65-L98) 与 [`core/Cargo.toml`](file:///Users/fanxing/code/other/filesystem/layerfs/core/Cargo.toml)）：

```text
                        ┌────────────────────────────────────────────────────────┐
                        │          上层应用 / FUSE 客户端 / Agent 沙箱            │
                        └──────────────────────────┬─────────────────────────────┘
                                                   │ POSIX 系统调用 / FUSE 请求
                                                   ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 集群二（Cluster Two）：工作区状态机与本地可变缓冲层                                               │
│                                                                                                 │
│   ┌──────────────────────────┐                   ┌──────────────────────────────────────────┐   │
│   │       layerfs-fuse       │                   │             layerfs-workspace            │   │
│   │ FUSE 协议分发与会话生命周期 │ ──── 委派调用 ───► │ 工作区快照视图管理与提交驱动（Driver）      │   │
│   └──────────────────────────┘                   └────────────────────┬─────────────────────┘   │
│                                                                       │                         │
│                                                                       ▼                         │
│                                                  ┌──────────────────────────────────────────┐   │
│                                                  │             layerfs-overlay              │   │
│                                                  │ 4 KiB Cell 写入缓冲、掩码合并与分代退役    │   │
│                                                  │ （本地独立 SQLite：MEMORY / WAL=OFF）    │   │
│                                                  └──────────────────────────────────────────┘   │
└───────────────────────────────────────────────────────────────────────┬─────────────────────────┘
                                                                        │ 批量 Commit / 捕获流
                                                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────────────────────┐
│ 集群一（Cluster One）：全局不可变存储与历史目录层                                                 │
│                                                                                                 │
│   ┌──────────────────────────┐                   ┌──────────────────────────────────────────┐   │
│   │     layerfs-content      │                   │             layerfs-storage              │   │
│   │ FastCDC 分块扫描、         │ ──── 规整对象 ───► │ CAS 准入排重、Zstd 前缀差分、Pack 聚合   │   │
│   │ Extent Tree 编解码与逻辑读取│   (LFSO 封包)     │ （单写并发通道、无重试设计）              │   │
│   └──────────────────────────┘                   └────────────────────┬─────────────────────┘   │
│                                                                       │ 物理持久化端口           │
│                                                                       ▼ (PackPersistence)       │
│                                                  ┌──────────────────────────────────────────┐   │
│                                                  │           layerfs-persistence            │   │
│                                                  │ 全局 Store 单文件 SQLite 持久化引擎       │   │
│                                                  │ （/layerfs-store/global/store.sqlite）   │   │
│                                                  └──────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## 2. 维度一：分块与寻址模型

### 2.1 内容寻址（CAS）与不可变对象标识

LayerFS 将内容寻址置于规范化对象（Canonical Object）层级，而非对物理存储块或裸文件字节做简单散列。

1. **对象标识生成规则**：
   - 规范对象 ID（`ObjectId`）固定为 32 字节哈希值，算法使用 BLAKE3。
   - 命名空间包含硬编码的冷冻前缀隔离域：`OBJECT_DOMAIN = b"layerfs/object/v2\0"`。
   - 计算公式：
     $$\text{ObjectId} = \text{BLAKE3}\Big(b\text{"layerfs/object/v2\textbackslash 0"} \;\|\; \text{canonical\_object\_bytes}\Big)$$
   - **源码依据**：[`core/crates/layerfs-content/src/object/id.rs:12-28`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-content/src/object/id.rs#L12-L28)。

2. **规范对象信封结构（Canonical Envelope）**：
   - 所有对象（无论是小文件、数据块、还是目录树节点）必须封装为 `LFSO` 格式，总包头长 9 字节：
     - `OBJECT_MAGIC`：`[u8; 4] = *b"LFSO"`；
     - `BYTES_KIND`：`u8 = 1`；
     - `payload_len`：大端序 32 位无符号整数，大小为 $4 + \text{value\_len}$；
     - `value_len`：大端序 32 位无符号整数，表示实际有效载荷长度；
     - `value`：具体角色（ObjectRole）的真实二进制。
   - 单对象物理最大尺寸约束：`MAX_CANONICAL_OBJECT_BYTES = 16 MiB`，有效载荷上限为 $16\,\text{MiB} - 9\,\text{B}$。
   - **源码依据**：[`core/crates/layerfs-content/src/object/codec.rs:14-38`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-content/src/object/codec.rs#L14-L38)。

### 2.2 内容定义分块（FastCDC / GEAR CDC）

对大文件的分块并非采用简单的定长切分，而是基于两字节滚动 GEAR 算法的内容定义分块（Content-Defined Chunking, CDC），以阻断局部插入或删除引发的字节偏移雪崩。

1. **分块阈值与切分策略**：
   - **小文件直通阈值（Cutoff）**：
     - 逻辑长度为 0：特殊空对象表示；
     - $0 < \text{size} < 128\,\text{KiB}$：直接作为整文件对象（Whole-file Object），不进入分块，直接生成单个根对象；
     - $\ge 128\,\text{KiB}$：触发 CDC 流式扫描与切分（阈值支持 $128\,\text{KiB} \sim 1\,\text{MiB}$，默认为 $128\,\text{KiB}$）。
     - **源码依据**：[`docs/general/cas_cdc_deltaencoding_handbook.md:141-155`](file:///Users/fanxing/code/other/filesystem/layerfs/docs/general/cas_cdc_deltaencoding_handbook.md#L141-L155)。
   - **GEAR FastCDC 核心参数**：
     - 最小分块边界（`MINIMUM_CHUNK_BYTES`）：`8,192` 字节（8 KiB）；
     - 目标分块期望值（`TARGET_CHUNK_BYTES`）：`16,384` 字节（16 KiB）；
     - 最大分块上限（`MAXIMUM_CHUNK_BYTES`）：`32,768` 字节（32 KiB，到达时硬性截断切分）；
     - 归一化位移（`NORMALIZATION_SHIFT`）：`2`；
     - 散列掩码：`SMALL_MASK = 0x0000_d903_0353_7000`，`LARGE_MASK = 0x0000_d901_0353_0000`；
     - 滚动哈希种子（`PROFILE_SEED`）：`0`。
   - **冷冻配置签名（Profile ID）**：
     - 系统在启动时通过 BLAKE3 将尺寸、掩码、位移以及 256 项 GEAR 查找表哈希为固定 Profile ID，防止任意节点采用不一致的分块参数破坏 CAS 引用一致性。
     - **源码依据**：[`core/crates/layerfs-content/src/file/cdc/gear.rs:13-52`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-content/src/file/cdc/gear.rs#L13-L52)。

### 2.3 文件分块树结构（Extent Tree / Manifest Recipe）

当文件被切分为多个 CDC Chunk 后，LayerFS 通过一棵不可变的 B+ 树（Extent Tree）维护逻辑偏移量到物理 Chunk 的映射，其角色类型分为叶子页与分支页：

1. **叶子切片单元（`ExtentSlice`）**：
   - 记录单个数据切片三元组：`payload_object_id`（目标 Chunk 的 32 字节 ObjectId）、`source_offset`（在目标 Chunk 中的起始字节偏移）、`logical_length`（切片贡献的逻辑字节长度）。
   - 硬性校验约束：`logical_length > 0` 且 $\text{source\_offset} + \text{logical\_length} \le \text{MAXIMUM\_CHUNK\_BYTES}\,(32\,\text{KiB})$。
   - **源码依据**：[`core/crates/layerfs-content/src/file/mapping/types.rs:24-52`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-content/src/file/mapping/types.rs#L24-L52)。

2. **树节点页面组织（`ExtentNode`）**：
   - **叶子页（`ExtentNode::Leaf`）**：
     - 聚合一组按逻辑顺序排列的 `ExtentSlice` 列表；
     - 维护子树累计逻辑字节量 `subtree_logical_bytes`。
   - **分支页（`ExtentNode::Branch`）**：
     - 包含层级 `level`（`1` 到 `31`）、子树累计逻辑字节量 `subtree_logical_bytes`、累计切片数 `subtree_extent_count`；
     - 包含子项描述符列表 `children: Vec<ChildDescriptor>`，每个描述符记录 `(cumulative_logical_end, cumulative_extent_end, child_object_id)`。
   - **页面规格约束**：
     - 单个树节点页面最大物理尺寸 `MAX_NODE_OBJECT_BYTES = 8,192` 字节（8 KiB）；
     - 非根页面最小项数 `MIN_ENTRIES = 64`，根分支节点最小子项数 `MINIMUM_ROOT_ENTRIES = 2`。
   - **源码依据**：[`core/crates/layerfs-content/src/file/mapping/types.rs:16-22, 70-106`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-content/src/file/mapping/types.rs#L16-L22)。

### 2.4 Delta 差分编码与 Zstandard 前缀机制

针对相近文件版本或小幅度编辑后的数据块，LayerFS 并未采用通用文本 Diff 或字节补丁（XOR / VCDIFF），而是采用了基于 **Zstandard 前缀字典（Zstd Raw Prefix Compression）** 的定向差分机制。

1. **前缀差分物理原理**：
   - 编码器复用 Zstd C API 的 `ZSTD_CCtx_refPrefix` 与 `ZSTD_DCtx_refPrefix`。
   - 将先前的基线对象（Base Canonical Object）原始二进制作为压缩字典的前缀注入压缩上下文，直接对新对象进行单次压缩。
   - **解码依赖**：新记录仅存储 `(base_object_id, compressed_frame)`，解码时必须先根据 `base_object_id` 获取基准数据并置入解压前缀。
   - **源码依据**：[`core/crates/layerfs-storage/src/encoding/codec.rs:1-49`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-storage/src/encoding/codec.rs#L1-L49)。

2. **物理编码记录格式（`ParsedRecord`）**：
   - `FULL_TAG = 0`：无基线的独立压缩帧；
   - `PREFIX_TAG = 1`：基于单一直接基准（`Option<ObjectId>`，32 字节）的前缀差分帧；
   - `STORED_TAG = 2`：原样直存（Verbatim），无压缩帧、无基准（适用于探针检测出不可压缩的数据，避免膨胀与解压开销）。
   - **物理编码开销计算**：
     - WholeFile 记录头：$1\,\text{B (Tag)} + 8\,\text{B (Raw Length)} + (32\,\text{B if Prefix}) + \text{Frame Length}$；
     - Chunk 记录头：$1\,\text{B (Tag)} + 4\,\text{B (Raw Length)} + (32\,\text{B if Prefix}) + \text{Frame Length}$。
   - **源码依据**：[`core/crates/layerfs-storage/src/encoding/delta/record.rs:18-59`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-storage/src/encoding/delta/record.rs#L18-L59)。

3. **差分选型门禁与深度限制（Single-Trial Selection）**：
   - 严格遵循**单次试探原则（One-Trial）**：先生成独立的 FULL/STORED 方案作为基线；若存在候选基准，执行一次 PREFIX 压缩；仅当 `PREFIX 编码总字节 < FULL 编码总字节` 时采纳，出现平局或膨胀时坚决回退独立帧。
   - **差分链深度硬限制**：
     - 整文件差分最大深度（`whole_file_delta_max_depth`）：16；
     - Chunk 块差分最大深度（`chunk_delta_max_depth`）：8；
     - 元数据 Inode 差分最大深度（`metadata_delta_max_depth`）：8。
   - 超出深度链或超过解压链预算时，强制退化为独立 FULL 编码。
   - **源码依据**：[`docs/general/cas_cdc_deltaencoding_handbook.md:310-388`](file:///Users/fanxing/code/other/filesystem/layerfs/docs/general/cas_cdc_deltaencoding_handbook.md#L310-L388) 与 [`core/docs/architecture/84-global-sqlite-store.md:90-101`](file:///Users/fanxing/code/other/filesystem/layerfs/core/docs/architecture/84-global-sqlite-store.md#L90-L101)。

---

## 3. 维度二：本地 NVMe SSD / 物理落盘布局

### 3.1 物理落盘终态架构：单 SQLite 文件（Global SQLite Store）

在 LayerFS 演进中，早期的外部松散文件（Loose Files）、目录 Hex 分片与 S3 方案已被彻底废除（2026-10 规范）。**系统最终确定所有已提交数据、元数据与索引全部收敛至单一宿主机本地 SQLite 数据库文件**（参见 [`core/docs/architecture/84-global-sqlite-store.md:1-40`](file:///Users/fanxing/code/other/filesystem/layerfs/core/docs/architecture/84-global-sqlite-store.md#L1-L40)）：
- 物理路径：`/layerfs-store/global/store.sqlite`（及其附属的 `-wal` 与 `-shm`）。
- 任何通过两级十六进制目录（如 `objects/ab/cd/`）在磁盘存储独立 Chunk 文件的设计在 LayerFS HEAD 版本均不存在，底层所有数据块均聚合为 Packfile 并以内联 BLOB 形式存在于 SQLite 的 `pack` 表中。

### 3.2 Packfile 组织结构与内部索引

为了消除大量小对象对 SQLite B-Tree 的性能损耗，LayerFS 采用了仿 Git 的 Packfile 聚合设计。

1. **Pack 文件头规范（Header）**：
   - 魔数标识：`PACK_MAGIC: [u8; 8] = *b"LFPACK\0\0"`；
   - 控制头总长度：`HEADER_LEN = 24` 字节；
   - 字段排布：
     $$\big[\text{magic: 8B}\big]\big[\text{version: 4B}\big]\big[\text{group\_count: 4B}\big]\big[\text{assembled\_length: 4B}\big]\big[\text{reserved: 4B}\big]$$
   - 长度限制：普通 Pack 最大长度 `PACK_LIMIT = 262,144` 字节（256 KiB）；超大对象单例 Pack 限制 `SINGLETON_PACK_LIMIT = 16,777,216` 字节（16 MiB）。
   - **源码依据**：[`core/crates/layerfs-storage/src/pack/layout.rs:44-70`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-storage/src/pack/layout.rs#L44-L70)。

2. **目录预留区与写入放大消除（Sized / Reserved Directory Region）**：
   - Pack 头部之后紧随目录索引区（Directory Region），每个目录项记录 Group 的物理偏移与长度（普通/Chunk 格式项宽 16 字节，整文件格式项宽 4 字节）。
   - **关键机制**：早期版本目录向后增长导致后续 Group 发生位移，造成高达 7.59 倍的写入放大（Issue `#219`）。当前实现（v9+ 及 Sized v23-v25）在创建 Pack 时预留固定的目录槽位，Group 仅向尾部追加，Pack 封盘后按实际项数收口。
   - **源码依据**：[`core/crates/layerfs-storage/src/pack/layout.rs:13-36`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-storage/src/pack/layout.rs#L13-L36)。

### 3.3 SQLite 表结构、索引与不可变约束

Pack 与对象索引在数据库中的物理 Schema 定义在 [`core/crates/layerfs-persistence/sql/sqlite/`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/sql/sqlite/)：

1. **`pack` 数据表与物理约束**（[`objects.sql:1-12`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/sql/sqlite/objects.sql#L1-L12)）：
   ```sql
   CREATE TABLE pack (
     pack_id INTEGER PRIMARY KEY CHECK(pack_id > 0),
     domain INTEGER NOT NULL CHECK(domain IN (0, 1)),
     digest BLOB NOT NULL CHECK(length(digest) = 32),
     length INTEGER NOT NULL CHECK(length BETWEEN 32 AND 16781312),
     body BLOB NOT NULL CHECK(length(body) = length),
     CHECK(domain = 1 OR length <= 262144)
   ) STRICT;

   -- 强行禁止对已写入 Pack 执行任何修改与删除
   CREATE TRIGGER pack_immutable_update BEFORE UPDATE ON pack BEGIN
     SELECT RAISE(ABORT, 'published pack immutable');
   END;
   CREATE TRIGGER pack_immutable_delete BEFORE DELETE ON pack BEGIN
     SELECT RAISE(ABORT, 'published pack immutable');
   END;
   ```

2. **`object_location` 对象寻址表**（[`metadata.sql:12-19`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/sql/sqlite/metadata.sql#L12-L19)）：
   ```sql
   CREATE TABLE object_location (
     object_id BLOB PRIMARY KEY CHECK(length(object_id) = 32),
     role INTEGER NOT NULL CHECK(role BETWEEN 1 AND 13),
     canonical_length INTEGER NOT NULL CHECK(canonical_length BETWEEN 1 AND 16777216),
     pack_id INTEGER NOT NULL REFERENCES pack(pack_id),
     group_number INTEGER NOT NULL CHECK(group_number BETWEEN 0 AND 255),
     record_number INTEGER NOT NULL CHECK(record_number BETWEEN 0 AND 8190)
   ) STRICT, WITHOUT ROWID;
   ```
   通过 `WITHOUT ROWID` 将 `object_id` 直接作为聚簇索引键，寻址物理数据时仅需一次 B-Tree 查找即可获得 `(pack_id, group_number, record_number)`。

### 3.4 SQLite PRAGMA 配置与 I/O 策略

根据官方指令（2026-10-07 所有者决议），LayerFS **永久禁用 Durable 模式，仅允许运行 Disposable 模式**（参见 [`core/crates/layerfs-persistence/src/backend/sqlite/profile.rs:11-74`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/src/backend/sqlite/profile.rs#L11-L74)）：

| PRAGMA 参数 | 设定值 | 架构设计考量 |
| :--- | :--- | :--- |
| `journal_mode` | `WAL` | 写入者不阻塞并发读取者，读写分离 |
| `synchronous` | `OFF (0)` | **Disposable 核心**：禁用任何 `fsync` / `fdatasync`，极致榨取 NVMe 吞吐，崩溃不保证物理恢复 |
| `fullfsync` | `0` | 彻底屏蔽 macOS / Linux 深度落盘调用 |
| `page_size` | `4096` | 4 KiB 物理页，匹配主流 NVMe SSD 的扇区大小与 Linux 内存页 |
| `auto_vacuum` | `NONE` 或 `INCREMENTAL` | 减少写路径上的空闲页重整开销 |
| `wal_autocheckpoint` | `1000` | 每 1000 页触发一次被动折叠 checkpoint |
| `journal_size_limit` | `4,194,304` (4 MiB) | 严格限制 WAL 文件上限，防止磁盘空间失控 |
| `cache_size` | `-2048` (2 MiB) | 严格限制每连接内存页缓存，杜绝内存泄漏 |
| `mmap_size` | `0` | 显式禁用 mmap，所有读写均走确定性有界用户缓冲区 |
| `temp_store` | `MEMORY (2)` | 临时表与排序全部在内存，严禁临时文件刷盘 |
| `busy_timeout` | `0` | **无重试原则（No-Retry）**：遇锁直接返回错误，由上层调度管道保证单写原子性 |

---

## 4. 维度三：写路径、随机覆盖写与读路径

### 4.1 FUSE 工作区写入缓冲与 4 KiB Cell 结构

LayerFS 在运行态将文件系统的修改与全局不可变存储彻底解耦：
- **热写路径绝对不切块**：在 FUSE `WRITE` 阶段，绝对不计算 FastCDC 滚动哈希，不生成 BLAKE3，不构造 Pack，不发起全局 SQLite 写入。
- **本地 Overlay SQLite 库**：每个挂载的 Workspace 独占一个本地私有的 SQLite 文件（`<overlay_dir>/overlay.sqlite`），配置为 `EXCLUSIVE` 排他锁、`MEMORY` 日志与 `synchronous=OFF`（参见 [`core/docs/issues/303/02-base-overlay.md:73-98`](file:///Users/fanxing/code/other/filesystem/layerfs/core/docs/issues/303/02-base-overlay.md#L73-L98)）。

#### 写入核心模型：4 KiB Cell 与有效性位图（Validity Mask）
1. **尺寸常量**（[`core/crates/layerfs-overlay/src/contract/types.rs:18-21`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/contract/types.rs#L18-L21)）：
   - `CELL_BYTES = 4,096`（4 KiB）；
   - `RUN_BYTES = 32,768`（32 KiB，对应单槽槽位大小）；
   - `MASK_BYTES = CELL_BYTES / 8 = 512` 字节（每个 Cell 对应 512 字节位图）。
2. **Cell 的物理与内存表示**（[`core/crates/layerfs-overlay/src/payload/cells.rs:7-30`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/payload/cells.rs#L7-L30)）：
   ```rust
   pub(crate) struct Stored {
       pub epoch: i64,
       pub data: Vec<u8>,
       pub validity: Option<Vec<u8>>, // 为 None 表示整个 Cell 全部字节均有效
   }
   pub(crate) struct Window {
       pub data: Box<[u8; CELL_BYTES]>,
       pub mask: Box<[u8; MASK_BYTES]>, // 小端位图，标记哪些字节是写入的
   }
   ```
3. **写入覆盖算法（`write_cells`）**（[`core/crates/layerfs-overlay/src/payload/stream.rs:19-79`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/payload/stream.rs#L19-L79)）：
   - 当写入范围涵盖整个 4 KiB 对齐 Cell 时（`whole`）：直接覆盖或插入整块数据，`validity` 置为 `None`（Dense Row，无位图开销）；
   - 当写入跨越 Cell 局部（`partial write`，如随机写入 100 字节）：执行点查读取既有 Cell，通过位操作将写入区间标记入 `Window.mask`，执行差量 upsert，仅更新变更字节与位图。
   - **完全消除写放大**：针对随机覆盖写，无需将整个不可变 Chunk 读出修改再重算哈希，仅在本地以 4 KiB Cell + 位图增量记录。

### 4.2 读路径分层组合模型（View Composition）

当 FUSE 触发 `READ` 操作时，系统通过 `compose_layers` 实施多层动态合成（参见 [`core/crates/layerfs-overlay/src/payload/stream.rs:248-385`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/payload/stream.rs#L248-L385)）：

```text
       ┌────────────────────────────────────────────────────────┐
       │                 FUSE READ(offset, length)              │
       └───────────────────────────┬────────────────────────────┘
                                   │
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │   1. 本地活动代（Active Generation Overlay）            │
       │      读取 4 KiB Cell 及有效性掩码 (Validity Mask)       │
       └───────────────────────────┬────────────────────────────┘
                                   │ 未决未填充区间 (open > 0)
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │   2. 捕获代 / 引用切片（Captured / Pieces / Reference） │
       │      读取暂存的引用数据与未决边界                        │
       └───────────────────────────┬────────────────────────────┘
                                   │ 仍未决区间 (read.inherited != 0)
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │   3. 底层不可变 Base 快照（Committed Root）              │
       │      查找 Extent Tree ➔ 定位 CAS Pack ➔ Zstd 解压 Chunk │
       └───────────────────────────┬────────────────────────────┘
                                   │ 仍超出文件原始大小的部分
                                   ▼
       ┌────────────────────────────────────────────────────────┐
       │   4. 逻辑空洞填充（Zero Fill）                           │
       │      自动补零（POSIX 语义保证）                          │
       └────────────────────────────────────────────────────────┘
```

- **合成执行机制**：
  1. 初始化输出缓冲区与 `decided` 决策标记数组（每个字节初态为 0：未决）；
  2. 优先从 Active Overlay 的 `fs_cell` 表查询；若命中 Dense Cell，直接内存复制并标记对应区间为 1（已决）；若命中带有 `validity` 掩码的 Partial Cell，仅复制位为 1 的字节；
  3. 检查截断阈值：凡处于 `inherited_cutoff` 之外的字节强制置为已决（阻断下穿）；
  4. 若仍有字节处于未决状态（`open != 0`），生成 `read.inherited` 掩码位图返回给 FUSE 工作区驱动；
  5. 工作区驱动将 `read.inherited` 中标记为 1 的空隙，路由至底层的 `BaseView::content`，通过不可变 Extent Tree 并行读取 CAS 数据块补全。

### 4.3 何时切块落盘（Commit 驱动机制）

可变文件切块写入 CAS 存储层的**唯一触发时机是执行明确的 Commit 操作**（参见 [`core/crates/layerfs-workspace/src/construction/driver.rs:48-95`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-workspace/src/construction/driver.rs#L48-L95) 与 [`core/crates/layerfs-workspace/src/construction/captured/owner.rs:1-80`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-workspace/src/construction/captured/owner.rs#L1-L80)）：
1. **冻结与捕获（Capture）**：调用 `Overlay.capture()`，将活跃代标记为捕获代（`Active -> Captured`），后续 FUSE 写入切换至新代号，捕获代数据变为只读快照；
2. **提取变更流（Runs Normalization）**：遍历捕获代中的 Cell 与修改记录，将其整理为 `FileRun::Data(bytes)`、`FileRun::Zero(length)` 与 `FileRun::End` 流式迭代器；
3. **分块与持久化（`construct_runs`）**：
   - 管道化送入 `layerfs-content::construct_runs`，启动 FastCDC 滚动哈希引擎；
   - 超过 128 KiB 的文件切分为 8~32 KiB 的 Chunk 并封装为 `LFSO` 对象；
   - 生成对应的 Extent Tree 叶子页与分支页；
   - 规范对象依次送入 `layerfs-storage::Save::sink`，经 CAS 去重后打包为 Packfile，并批量执行单次物理写入事务写入全局 SQLite；
4. **历史提交（History Commit）**：调用 `HistoryCatalog::commit_branch`，以 CAS 生成的目录树根哈希推进分支，完成原子发布。

---

## 5. 维度四：稀疏文件、Truncate、Fallocate 与空间核算

### 5.1 文件截断（Truncate / Shrink）机制

在基于快照和层叠缓冲的文件系统中，将数吉字节（GB）的大文件截断为小文件如果采用同步扫描删除，会导致严重的写暂停。LayerFS 采用了**逻辑截断屏障 + 异步阶梯回收**机制：

1. **逻辑阶梯截断（$O(1)$ Cutoff）**：
   - 当调用 `setattr(size)` 执行缩容时，系统直接更新 `fs_inode` 表中的 `size` 与 `inherited_cutoff` 字段（[`core/crates/layerfs-overlay/src/database/statements.rs:105-110`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/database/statements.rs#L105-L110)）。
   - **截断屏障**：在后续的读路径（`compose_layers`）中，凡偏移量大于等于 `inherited_cutoff` 的读取请求，在合成时直接被 `settle(&mut decided, &mut open, layer.cutoff, end)` 强行截断，底层旧数据与旧 Cell 逻辑瞬间不可见。
2. **阶梯记录与异步清理（Staircase Reclamation）**：
   - 将截断点插入 `fs_truncate` 表：`INSERT INTO fs_truncate VALUES(inode, generation, depth, cell_offset, epoch)`；
   - 由后台维护任务（`maintenance/orphan.rs` 与 `maintenance/garbage.rs`）以最多 64 行的批量游标，分批从 `fs_cell` 中异步物理删除超出 `cell_offset` 的废弃 Cell（[`core/crates/layerfs-overlay/src/maintenance/orphan.rs:328-340`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/maintenance/orphan.rs#L328-L340)）。

### 5.2 稀疏文件与 Fallocate 打洞（Punch Hole）

1. **空洞运行表示（`FileRun::Zero`）**：
   - 在构建与流式处理中，文件空洞通过 `FileRun::Zero(u64)` 显式表示，不占用任何内存与载荷空间（[`core/crates/layerfs-content/src/file/construction/runs.rs:16-24`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-content/src/file/construction/runs.rs#L16-L24)）。
2. **32 KiB 零块周期与子树对齐复用**：
   - 针对大型全零空洞，FastCDC 的全零块周期固定为 32 KiB（`MAXIMUM_CHUNK_BYTES`）；
   - `construct_runs` 遇到大型 `Zero` 跨度时，对齐的完整零块与上层 Mapping 子树直接复用预先固化的静态哈希对象，**完全跳过内存物理清零与哈希计算**，实现对吉字节空洞的对数级（Logarithmic）极速构建（参见 [`docs/general/cas_cdc_deltaencoding_handbook.md:18-27`](file:///Users/fanxing/code/other/filesystem/layerfs/docs/general/cas_cdc_deltaencoding_handbook.md#L18-L27)）。
3. **Fallocate 打洞在 Overlay 中的落地**：
   - 在本地工作区中，打洞区间所涵盖的既有 Cell 直接被从 `fs_cell` 中移除或打上无效掩码，同时若涉及跨层穿透，则通过 `Piece` 切断引用；读取打洞区间时，由于未决位落入空值，自动补零返回。

### 5.3 四维容量与空间核算机制（Space Accounting）

LayerFS 在全局存储中建立了四个维度的精细化容量核算，严格区分去重前、封装后与实际落盘大小（[`core/crates/layerfs-persistence/sql/sqlite/metadata.sql:31-45`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/sql/sqlite/metadata.sql#L31-L45)）：

```sql
CREATE TABLE store_totals (
  id INTEGER PRIMARY KEY CHECK(id = 1),
  logical   INTEGER NOT NULL DEFAULT 0 CHECK(logical >= 0),
  canonical INTEGER NOT NULL DEFAULT 0 CHECK(canonical >= 0),
  stored    INTEGER NOT NULL DEFAULT 0 CHECK(stored >= 0),
  metadata  INTEGER NOT NULL DEFAULT 0 CHECK(metadata >= 0),
  -- Init 统计快照...
) STRICT;
```

1. **`logical`（逻辑容量）**：用户视角所见所有文件的原始逻辑字节尺寸累加和（未去重、未压缩）。
2. **`canonical`（规范容量）**：所有独立对象经 `LFSO` 信封封装后的标准规范字节总和（反映去重后的有效载荷边界）。
3. **`stored`（物理存储容量）**：Packfile 内部经 Zstandard 压缩（FULL/PREFIX/STORED）并加上 Pack 控制头后，实际写入磁盘 `pack` 表的物理 BLOB 字节总数。
4. **`metadata`（元数据容量）**：`object_location`、`inode_value`、`history`、B-Tree 索引等元数据占用的物理容量估算。
5. **空间核算触发器**：通过在 `commit` 与 `layer` 相关表上挂接 SQLite 触发器（如 `commit_logical`），在每次提交时增量更新 `store_totals`，保持 $O(1)$ 的全局容量快照。

---

## 6. 维度五：垃圾回收（GC）与引用完整性

### 6.1 全局不可变存储的“永不删除”设计哲学

在 Cluster One 全局存储引擎中，**当前实现刻意没有实现全局不可变 Chunk 的垃圾回收器（No Garbage Collection）**：
1. **触发器物理刚性拦截**：
   - 如前文第 3.3 节所示，`pack` 表上配置了 `pack_immutable_delete` 触发器，在 SQLite 引擎层直接阻止任何 `DELETE FROM pack` 操作（[`core/crates/layerfs-persistence/sql/sqlite/objects.sql:11-12`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/sql/sqlite/objects.sql#L11-L12)）。
2. **架构权衡根因**：
   - 官方设计明确指出：“No garbage collection, content deletion, Branch deletion or serial refund. History is the product.”（[`core/docs/architecture/16-history.md:596`](file:///Users/fanxing/code/other/filesystem/layerfs/core/docs/architecture/16-history.md#L596)）。
   - **Pack 打包带来的碎裂困境**：多个 Chunk（可能属于不同文件或不同 Commit）被打包在同一个 256 KiB 的 Pack BLOB 内。若要物理删除某一个 Chunk，必须对 Pack 执行解包、重写并更新所有相关对象的 `object_location`，造成极大的写放大和并发阻塞；因此只要历史分支存在，全局块永久追加不删。

### 6.2 异常中断与孤儿块（Orphan Chunks）状态处理

当客户端在 `Save` 流程中生成并持久化了一批 Packfile，但后续在生成根目录树或调用 `commit_branch` 时因崩溃、网络中断或并发竞争（`HeadMoved`）而失败时：
1. **孤儿块的空间债务（Space Debt）**：
   - 这些已经持久化在 `pack` 和 `object_location` 表中的数据块变成了无法从任何有效 `Commit` 可达的“未引用块”（Unreferenced Garbage）。
   - **安全性保证**：由于 CAS 的强内容寻址特性，未引用块的存在**完全不影响逻辑文件系统的一致性与安全性**；若未来的提交恰好写入相同哈希的数据，CAS 排重机制会直接复用该孤儿块，使其“复活”为合法引用。
2. **离线封盘与紧缩（Sealing & Compaction）**：
   - 仅在由宿主明确调用的 `Handles::seal()` 阶段（如导出只读镜像或归档时），才通过单次排他事务执行 Checkpoint 与未引用空间回收（[`core/crates/layerfs-persistence/src/store/seal.rs:1-50`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-persistence/src/store/seal.rs#L1-L50)）。

### 6.3 工作区本地 Overlay 的精细化回收

与全局 CAS 的永不删除相反，工作区本地私有 SQLite 具有极其严密的局部垃圾清理机制：

1. **分代退役清理（Generation Retirement）**（[`core/crates/layerfs-overlay/src/maintenance/garbage.rs:13-75`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/maintenance/garbage.rs#L13-L75)）：
   - 当 Commit 成功被安装（Install: base root 推进）后，先前代（Captured Generation）的局部修改已被固化到底层不可变存储中，该代号被标记为 retired；
   - 后台任务以每次 64 行的批次，扫描并物理删除已退役代的 `fs_cell`、`fs_data_ref` 与 `fs_entry` 行，释放 SQLite 页面供后续写入复用。
2. **孤儿句柄全生命周期闭环（Orphan Inode Reclamation）**（[`core/crates/layerfs-overlay/src/maintenance/orphan.rs:40-160`](file:///Users/fanxing/code/other/filesystem/layerfs/core/crates/layerfs-overlay/src/maintenance/orphan.rs#L40-L160)）：
   - 当打开的文件被执行 `unlink` 操作（`open-unlinked` 状态，`nlink == 0`），其数据行被转移到特殊的孤儿空间；
   - 只要对应的 FUSE 文件句柄未关闭，读取依然能透视其内容；
   - 当最后一个持有该 Inode 的进程触发 `RELEASE` 或会话终止时，后台任务当场分批物理删除其在 overlay 数据库中的全部数据。
3. **卸载即销毁（Terminal Unmount）**：
   - 工作区生命周期终结时，整个本地 `overlay.sqlite` 数据库文件直接被宿主 OS `unlink` 删除，瞬间完成 100% 局部物理空间回收。

---

## 7. 维度六：对 Bowl 的架构启发、深度对比与取舍建议

结合 Bowl 的现状（PostgreSQL 承载强一致性 POSIX 元数据、商业化 Disk 配额账本、单事务 Inode/FileHandle 生命周期）与未来的“本地 NVMe SSD Chunk 数据层”目标，对比 LayerFS 的得失，提炼关键工程决策。

### 7.1 分块模型全方位权衡：『CAS + CDC 快照型』 vs 『POSIX 原地偏移量定长分块』

| 架构维度 | 方案 A：LayerFS 路线（CAS + FastCDC + Delta + Extent Tree） | 方案 B：POSIX 定长分块路线（Fixed-size Chunk，如 4 MiB 块） |
| :--- | :--- | :--- |
| **核心适用场景** | **版本控制、代码构建、AI Agent 工作区**（读密集、Git/npm 批量写、高频分支快照） | **通用 POSIX 文件系统、数据库存储、高吞吐随机读写**（低延迟、原地随机覆盖写） |
| **写入与随机覆盖性能** | **极差（无原生覆盖写）**：必须依赖本地 SQLite Overlay 缓冲层（4KB Cell + 掩码），在 Commit 时消耗巨额 CPU 跑 FastCDC 滚动哈希与 Zstd 压缩。 | **极高**：直接根据 $\text{offset} / \text{chunk\_size}$ 定位 Chunk，直接 `pwrite`/`io_uring` 写入对应物理块偏移，无额外哈希与差分计算。 |
| **读取吞吐与延迟** | **较差（多层合成开销）**：<br>Active Cell $\to$ Piece $\to$ Extent Tree $\to$ Packfile $\to$ Zstd 前缀解压，尾延迟高，内存拷贝多。 | **极高（$O(1)$ 直通）**：<br>无 Manifest 解析开销，单次系统调用直通 NVMe 物理扇区，极易发挥 Linux Page Cache 与 Direct I/O。 |
| **去重与快照能力** | **极致**：跨文件、跨版本天然字节级去重；快照仅为一个 32 字节 Root ID，克隆/分支秒级且零空间开销。 | **一般**：无字节内容去重（1 字节插入即导致整块哈希失配）；快照需在元数据表维护 Chunk 指针引用计数或 CoW 映射。 |
| **垃圾回收复杂度** | **极高 / 近乎无法在线执行**：<br>Pack 打包导致单个 Chunk 无法独立释放；需全局图遍历与重打包（Repack/Compaction），写放大剧烈。 | **极低 / 极致轻量**：<br>Chunk 独立落盘，文件删除或截断时，直接根据 `(ino, chunk_index)` 执行物理文件 `unlink` 或 `fallocate punch_hole`，无级联依赖。 |
| **系统架构复杂度** | **极高**：需维护两套存储引擎（全局 SQLite + 工作区局部 Overlay SQLite）与复杂的世代退役合并状态机。 | **简洁清晰**：PostgreSQL 保持元数据权威，本地 NVMe SSD 纯粹作为扁平分块存储池，职责清晰，无深层次状态裂缝。 |

### 7.2 对 Bowl 数据层落地的明确取舍与推荐方案

> ➡️ **核心决策建议**：  
> **Bowl 应坚决采纳『POSIX 原地/偏移量定长分块（推荐 4 MiB）』作为主力数据路径，拒绝将 LayerFS 的 FastCDC + Packfile + Zstd Prefix 复杂度引入通用读写路径。**  
> LayerFS 的双层设计（Overlay + CAS）是特定面向“代码树多次修改然后一次性 Commit”的冷热分离妥协，若将其放在通用 POSIX 随机写场景下，将导致不可接受的 CPU 消耗、写放大与难以收敛的 GC 复杂度。

### 7.3 本地 NVMe SSD 物理文件组织最佳实践（提炼自工业界与调研实证）

针对 Bowl 即将设计的 NVMe SSD Chunk 落盘层，推荐采纳以下物理布局与 I/O 规约：

1. **两级十六进制目录分片（Two-Level Hex Fanout）**：
   - 物理路径模板：`/data/bowl/chunks/{disk_id}/{hash[0:2]}/{hash[2:4]}/{chunk_id}.chunk`；
   - **设计机理**：单目录文件数超过数万时，ext4/xfs 的目录 Inode 树锁会引发严重的 `lookup` 争用；两级分片将单个叶子目录的文件数控制在合理数量（例如 256 分片），保证目录元数据操作的高并发性。
2. **原子写入与零破坏（Atomic Write & Rename）**：
   - 严禁直接原地覆盖写未初始化的 Chunk 文件；
   - **规范路径**：先写入同目录下的临时文件（如 `.tmp_{chunk_id}_{uuid}`），完成数据刷盘（`fdatasync`）后，通过 Linux 原语 `rename(2)`（或 `renameat2(RENAME_NOREPLACE)`）原子覆盖至目标 Chunk 文件；
   - 彻底阻断写中途宕机留下损坏块的问题。
3. **块头自包含校验（Self-Contained Header & Checksum）**：
   - 借鉴 LayerFS 的 `ObjectId` 强校验思想，在每个 Chunk 文件头保留 64 字节魔数与元信息区：
     - `Magic`（8 字节，如 `BOWLCHNK`）；
     - `Version`（4 字节）；
     - `DataLength`（4 字节）；
     - `Checksum`（32 字节，采用 BLAKE3 或 xxHash64）；
     - `Reserved`（16 字节预留）；
   - 读取时快速校验校验和，防范 NVMe SSD 静默位翻转（Bit Rot）与半写损坏。
4. **直通 POSIX 预分配与打洞（Fallocate Integration）**：
   - 紧密配合已采纳的 [ADR-0010](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0010-fuse-directory-stream-and-fallocate-metadata-semantics.md)：
   - 当调用 `Fallocate(mode=0)` 扩展空间时，底层 Chunk 文件直接调用 `fallocate(fd, 0, offset, length)` 直通 NVMe 预分配物理 Extent；
   - 当调用 `Fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` 打洞时，底层调用系统 `fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, length)` 释放物理块，真正向 NVMe 盘释放空间，并保持与 PostgreSQL 配额账本（[ADR-0015](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0015-unify-disk-and-storage-service-with-transactional-quota.md)）的物理一致。
5. **并发 I/O 驱动选型**：
   - 放弃传统带有全局 Mutex 的阻塞读写，数据面采用 Go `sys_unix` / Linux `io_uring`，以多队列无锁并发直接打满现代 NVMe SSD 的数十万 IOPS。

---

## 8. 调研结论摘要

1. **寻址模型**：LayerFS 实现了规范对象级的 BLAKE3 CAS 寻址，配合 8~32 KiB 的 FastCDC 滚动哈希与 Extent Tree 映射树，构建了高度紧凑但计算密集的快照存储结构。
2. **落盘布局**：LayerFS 最终放弃了外部松散文件与目录分片，走向单一 SQLite 数据库（`store.sqlite`）方案，所有数据块以 Packfile BLOB 形式持久化，配以 `synchronous=OFF` 的极端 Disposable 策略。
3. **读写解耦**：通过在 FUSE 工作区引入独立的 `overlay.sqlite`，以 4 KiB Cell 和位图掩码承载高频随机写，直到显式 Commit 时才执行批处理 CDC 切块，有效解决了 CAS 无法直接处理 POSIX 随机写的问题。
4. **空间与生命周期**：全局存储采用只追加不删除策略（硬性触发器拦截），工作区局部存储则通过分代退役与孤儿句柄机制实施严格的细粒度空间回收。
5. **Bowl 取舍落地**：Bowl 的定位是通用高性能 POSIX 存储，应汲取 LayerFS 在元数据自校验、两级目录分片、原子 Rename 与空洞打洞等硬件级最佳实践，但在数据分块上应坚持『4 MiB 原地偏移定长分块』，以确保极致的读写吞吐与极简的单块垃圾回收机制。
