# drive9 数据层一手源码深度调研报告

## 摘要与系统全景

本报告基于 `github.com/mem9-ai/drive9` 仓库的一手源码，对其底层数据存储模型、本地写日志、缓存体系、读写路径、区间剪裁算法、垃圾回收以及崩溃恢复机制进行深度调研，并提炼其对 Bowl 项目（PostgreSQL 元数据 + 本地 NVMe SSD Chunk 落盘）的架构启发与工程取舍。

drive9 定位为“面向 AI 智能体的原生文件系统”，在架构上采用**元数据与数据存储解耦**的分布式存储设计：
- **元数据控制面与核心数据面**：以 TiDB / MySQL（自建或 TiDB Cloud Serverless）作为底层关系数据库存储，使用逻辑分表（`file_nodes`、`inodes`、`contents` 等）组织树状命名空间；
- **分层数据平面**：小文件直接作为二进制 BLOB 内联在数据库行内；普通中大文件经由分块（8 MiB）多部分上传写入 S3 / 对象存储；高性能随机覆写与 POSIX 严苛负载文件则通过 `content_layout = extent` 路由至基于 JuiceFS 协议模型封装的 Extent 引擎；
- **FUSE 客户端本地加速层**：客户端通过本地磁盘维护写回影子文件（`ShadowStore`）、写前日志（`Journal` WAL）、分段内存写缓冲（`WriteBuffer`）以及多级读缓存（LRU 内存缓存 + 磁盘 `.drc` 缓存），在保证崩溃恢复能力的同时削减对象存储的高昂往返延迟。

---

## 一、分块与寻址模型（Chunking & Addressing Model）

### 1.1 大小文件分层分流原则

drive9 在存储层面设计了三种物理内容表征（`ContentLayout`），定义于 [`pkg/datastore/layout.go:3-10`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/layout.go#L3-L10)：
- `single`（`ContentLayoutSingle`）：整对象单文件存储。
- `append_log`（`ContentLayoutAppendLog`）：面向日志追加优化的流式追加布局。
- `extent`（`ContentLayoutExtent`）：切片分块存储，专为高频随机覆写、POSIX 数据库文件（如 SQLite）及大容量文件设计。

物理存储后端类型 `StorageType` 定义于 [`pkg/datastore/store.go:45-50`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/store.go#L45-L50)：
- `StorageDB9 = "db9"`：内联存储。文件数据以二进制存入数据库列（`files.content_blob`）。
- `StorageS3 = "s3"`：外部对象存储。`contents.storage_ref` 存储对象存储的 S3 Key。
- `StorageExtent = "extent"`：由 Extent 引擎接管的块存储，见 [`pkg/datastore/layout.go:44`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/layout.go#L44)。

分流阈值由 `smallFileThreshold` 决定：
- 客户端默认回退阈值为 50,000 字节（约 50KB），见 [`pkg/client/transfer.go:17`](/Users/fanxing/code/other/filesystem/drive9/pkg/client/transfer.go#L17) 与 [`pkg/fuse/read.go:21`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/read.go#L21)；
- 服务端通过 `/v1/status` 接口协商下发 `inline_threshold`（见 [`pkg/client/client.go:322-328`](/Users/fanxing/code/other/filesystem/drive9/pkg/client/client.go#L322-L328)）；
- 当文件大小 $\le \text{smallFileThreshold}$ 时，写入走内联快速路径，直接存入数据库；
- 当文件大小超过阈值时，客户端切换为 S3 分块传输（`UploadPart`）或 Extent 切片分配。

### 1.2 Extent 数据模型与 24 字节二进制 Slice 编码

当文件启用 `content_layout = extent` 时，drive9 采用与 JuiceFS 兼容的切片化逻辑组织：
- **物理常量定义**（[`pkg/datastore/layout.go:12-21`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/layout.go#L12-L21)）：
  - `ExtentChunkSize = 1 << 26`（64 MiB）：大文件按 64 MiB 划分逻辑块（Chunk）。
  - `ExtentBlockSize = 4 << 20`（4 MiB）：对象存储上的物理数据块（Block）最大上限。
  - `extentSliceBytes = 24`：切片元数据的定长二进制字节数。
  - `extentCompactSlices = 2500`：当单个 Chunk 内切片碎片累积达到 2500 个时，触发压缩合并（Compaction）。

每个 Chunk 内的数据由若干有序的切片（Slice）按时间覆写覆盖而成。每个 Slice 在数据库持久化或网络传输时采用大端序 24 字节编码，定义在 [`pkg/extent/slices.go:9-39`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go#L9-L39)：
```
+---------------+---------------+---------------+---------------+---------------+
| pos (uint32)  |  id (uint64)  | size (uint32) | off (uint32)  |  len (uint32) |
|   [0:4] 4B    |   [4:12] 8B   |  [12:16] 4B   |  [16:20] 4B   |  [20:24] 4B   |
+---------------+---------------+---------------+---------------+---------------+
```
字段物理语义：
1. `pos`（4 字节，`uint32`）：该切片在当前 64 MiB Chunk 内的**逻辑起始偏移量**（$0 \le \text{pos} < 64\text{ MiB}$）。
2. `id`（8 字节，`uint64`）：全局唯一的物理对象 Slice ID。若 `id == 0`，则表示该区间为一个**空洞（Hole / Sparse Gap）**。
3. `size`（4 字节，`uint32`）：生成该 Slice 对应底层物理对象（Blob）时的完整原始容量。
4. `off`（4 字节，`uint32`）：该切片在对应物理对象 Blob 内部的**起始偏移量**。
5. `len`（4 字节，`uint32`）：该切片有效逻辑数据的长度（字节数）。

### 1.3 逻辑偏移映射、重叠切片树与区间覆盖剪裁

当发生随机覆盖写时，新的 Slice 会叠加到 Chunk 已有的切片历史中。drive9 使用一棵带二叉平衡特性的动态区间剪裁树（`rawSlice` 树）解决重叠区间的覆盖裁剪计算，核心逻辑见 [`pkg/extent/slices.go:48-105`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go#L48-L105)：

```go
// rawSlice 节点结构 (pkg/extent/slices.go:11-18)
type rawSlice struct {
    pos         uint32
    id          uint64
    size        uint32
    off         uint32
    len         uint32
    left, right *rawSlice
}
```

- **区间剪裁算法（`cut`）**（[`pkg/extent/slices.go:48-71`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go#L48-L71)）：
  以某个分割点 `pos` 将现有 Slice 递归分裂为左右两部分：若 `pos` 落在 Slice 内部（`s.pos < pos < s.pos + s.len`），则右侧新切片的物理偏移增加 `l = pos - s.pos`（`off: s.off + l`），有效长度缩减为 `len: s.len - l`，左侧原切片长度截断为 `l`。
- **视图构建（`buildOverlay`）**（[`pkg/extent/slices.go:83-105`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go#L83-L105)）：
  按时间序列逐个将新 Slice 插入树中，新写入的 Slice 会调用 `cut` 强行剪除（覆盖）下层旧 Slice 的重叠部分。中序遍历（`visit`）整棵树即可得到从逻辑位置 0 到 Chunk 末尾无任何重叠、按顺序排列的有效 Slice 列表。若相邻两片之间存在间隔（`s.pos > pos`），系统自动填充 `Id: 0` 的空洞切片（[`pkg/extent/slices.go:97-100`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go#L97-L100)）。

### 1.4 元数据表结构关联

在数据库层面，Extent 模式通过在标准表上添加列与引入专有扩展表实现。定义于 [`pkg/tenant/schema/extent.go:14-150`](/Users/fanxing/code/other/filesystem/drive9/pkg/tenant/schema/extent.go#L14-L150)：

```sql
-- 1. 挂载到主文件树 file_nodes 与 contents (pkg/tenant/schema/extent.go:16-19)
ALTER TABLE file_nodes ADD COLUMN content_layout VARCHAR(32) NOT NULL DEFAULT '';
ALTER TABLE file_nodes ADD COLUMN extent_ino BIGINT UNSIGNED NULL;
CREATE INDEX idx_file_nodes_extent_ino ON file_nodes(extent_ino);
ALTER TABLE contents ADD COLUMN content_layout VARCHAR(32) NOT NULL DEFAULT '';

-- 2. Extent 专有分块表 (pkg/tenant/schema/extent.go:63-69)
CREATE TABLE IF NOT EXISTS jfs_chunk (
    id     BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
    inode  BIGINT UNSIGNED NOT NULL,
    indx   INT UNSIGNED NOT NULL,      -- Chunk 序号：offset / (64 MiB)
    slices LONGBLOB NOT NULL,          -- 24 字节 Slice 的二进制列表
    UNIQUE KEY uk_jfs_chunk (inode, indx)
);

-- 3. 物理切片引用计数表 (pkg/tenant/schema/extent.go:70-75)
CREATE TABLE IF NOT EXISTS jfs_chunk_ref (
    chunkid BIGINT UNSIGNED NOT NULL PRIMARY KEY,  -- 对应 Slice ID
    size    INT UNSIGNED NOT NULL,
    refs    INT NOT NULL,                           -- 引用计数
    KEY idx_jfs_chunk_ref_refs (refs)
);

-- 4. 异步切片合并任务队列 (pkg/tenant/schema/extent.go:119-136)
CREATE TABLE IF NOT EXISTS slice_compact_tasks (
    task_id       VARCHAR(64) PRIMARY KEY,
    extent_ino    BIGINT UNSIGNED NOT NULL,
    chunk         INT UNSIGNED NOT NULL,
    status        VARCHAR(20) NOT NULL,
    attempt_count INT NOT NULL DEFAULT 0,
    receipt       VARCHAR(128) NULL,
    lease_until   DATETIME(3) NULL,
    ...
    UNIQUE INDEX uk_slice_compact_ino_chunk (extent_ino, chunk)
);
```

**关联映射路径**：
当访问文件时，首先根据路径哈希在 `file_nodes` 定位节点并获得 `extent_ino`（[`pkg/datastore/store.go:125-147`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/store.go#L125-L147)）。如果 `content_layout == 'extent'`，读取偏移量 `Offset` 换算为 `indx = Offset / 64MB`，在 `jfs_chunk` 表中查询 `(inode, indx)` 对应的 `slices` BLOB。解析出切片数组后，通过 `buildOverlay` 计算出当前访问字节所对应的底层物理块 ID（`id`）与块内偏置（`off`）。

---

## 二、本地磁盘缓存与持久化布局（Disk Cache & Local Journaling）

在分布式网络文件系统中，直接对远程 S3 或 TiDB 实施 POSIX 细粒度同步会导致性能严重下降。drive9 在客户端本地磁盘开辟了专门的缓存与日志落盘体系。

### 2.1 本地存储在数据面的角色划分

本地磁盘在 drive9 中扮演三种正交角色：
1. **写回影子文件（`ShadowStore` / Write-back Staging）**：作为未提交脏数据的临时落盘载体，拦截频繁写入，使得客户端无需将全部脏数据保存在易失性 RAM 中。
2. **写前日志（`Journal` WAL）**：记录原子元数据与数据提交序列号（Group Commit），在主机崩溃断电后恢复未提交事务。
3. **分块读缓存（`DiskReadCache`）**：对已从远程下载的数据块以 `.drc` 文件在本地落盘，提供断网与二次读取的超高速访问。

### 2.2 本地 Shadow 影子文件管理（`ShadowStore`）

源码位于 [`pkg/fuse/shadow.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go)。
- **存储路径与命名**：影子文件存放于 `<cache_dir>/<path_hash>.shadow`（[`pkg/fuse/shadow.go:85-88`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go#L85-L88)）。
- **配额与保护参数**（[`pkg/fuse/shadow.go:16-30`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go#L16-L30)）：
  - `smallFileShadowThreshold = 64 << 20`（64 MiB）：缓存模式下单个文件最大阈值。
  - `defaultWriteCacheFreeRatio = 0.10`：缓存分区必须保留至少 10% 空闲空间，否则写操作立即返回 `ENOSPC` 失败。
  - `defaultWriteCacheSizeMB = 1024`（1 GiB）：写回影子缓存默认最大总配额。
- **安全并发机制（Generation Pinning 与 Retirement）**（[`pkg/fuse/shadow.go:71-102`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go#L71-L102)）：
  在文件由后台异步上传线程提交远端并清理本地影子文件时，可能存在活跃读句柄正在读取该影子文件。为此，drive9 设计了代际标记（Generation Token）与退休映射（`retired map[uint64]*retiredShadow`）：
  - 读操作通过 `Pin` 获取当前 active generation；
  - 清理操作调用 `Remove` 时，并不直接销毁物理 fd，而是将其移动至 `retired` 映射；
  - 只有当最后一个活跃读句柄执行 `Unpin` 使引用计数归零时，才关闭文件句柄并物理删除磁盘上的 `.shadow` 文件。
- **并发覆写防护（`writeGen`）**（[`pkg/fuse/shadow.go:104-110`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go#L104-L110)）：
  每次修改影子文件，`writeGen` 单调自增。上传完成后的清理动作必须传入启动时的 generation，执行原子比对 `RemoveIfGeneration`。如果期间有新写入改变了 generation，清理逻辑自动放弃删除，避免覆盖更新产生数据丢失。

### 2.3 WAL 写前日志协议与持久化格式

源码位于 [`pkg/fuse/journal.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go)。
WAL 日志专为快速持久化和崩溃一致性设计，操作类型 `JournalOp` 定义于 [`pkg/fuse/journal.go:16-29`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L16-L29)：
```go
const (
    JournalWrite       JournalOp = iota // 写数据
    JournalTruncate                     // 截断文件
    JournalRename                       // 重命名
    JournalUnlink                       // 物理删除
    JournalMkdir                        // 创建目录
    JournalRmdir                        // 删除目录
    JournalFsync                        // 本地持久化锚点标记
    JournalCommit                       // 远端已提交确认标记
    JournalPendingMeta                  // 待刷盘元数据索引记录
)
```

**二进制持久化帧格式（Wire Frame Format）**（[`pkg/fuse/journal.go:58-63`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L58-L63)）：
```
+------------------------+-------------------------+-------------------------+
| Length (uint32, 4B LE) |   JSON Payload (N 字节)  |  CRC32 (uint32, 4B LE)  |
+------------------------+-------------------------+-------------------------+
```
- `Length`：4 字节小端序整数，记录后续 JSON 载荷字节长度。
- `Payload`：序列化后的 `JournalEntry`（包含 `Seq`、`Op`、`Path`、`Length`、`BaseRev`、`ShadowSpill` 等字段，见 [`pkg/fuse/journal.go:32-51`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L32-L51)）。
- `CRC32`：载荷内容的 IEEE CRC32 校验和。

### 2.4 Group Commit 与并发 `FsyncShared` 聚合机制

频繁调用系统 `fsync(2)` 会导致磁头寻道与 SSD 刷盘堵塞。drive9 实现了高效的组提交（Group Commit）机制，见 [`pkg/fuse/journal.go:169-208`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L169-L208)：
1. 每个追加操作持有 `j.mu` 单调递增分配全局序列号 `j.seq`（[`pkg/fuse/journal.go:142`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L142)）。
2. 当线程调用 `FsyncShared()` 时，先检查当前已完成持久化的序列号 `doneGen`；若 `doneGen >= target`，说明其他先发线程的刷盘已经顺带覆盖了本地目标，直接无锁返回（[`pkg/fuse/journal.go:178-181`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L178-L181)）。
3. 否则竞争 `syncMu`。竞得锁的 Leader 线程快照最新的 `snapshot = j.seq.Load()`，执行单次底层物理文件 `fd.Sync()`（[`pkg/fuse/journal.go:199`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L199)）。
4. 物理刷盘成功后将 `doneGen` 推进至 `snapshot`，所有排队等待的并发请求一次性全部被确认。

### 2.5 磁盘读缓存组织（`DiskReadCache`）

源码位于 [`pkg/fuse/disk_read_cache.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/disk_read_cache.go)。
- **文件格式与命名**：文件后缀为 `.drc`，文件名通过 SHA-256 哈希计算得到：`SHA256(FileID \0 Revision \0 Offset \0 Length)`（[`pkg/fuse/disk_read_cache.go:50-53`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/disk_read_cache.go#L50-L53)）。
- **文件内部布局**：JSON Header（记录 `FileID`、`Revision`、`Offset`、`Length`、`Size`、`CRC32`、`CreatedAt`）+ 二进制 Data + 数据 CRC32 校验和。
- **容量与淘汰策略**：基于 `container/list` 维护内存 LRU 队列，默认最大容量 `defaultDiskReadCacheMaxSize = 1 GiB`（[`pkg/fuse/disk_read_cache.go:25`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/disk_read_cache.go#L25)）。当容量超过上限或所在磁盘空闲比率低于 10% 时，按最久未访问优先剔除磁盘上的 `.drc` 文件。

---

## 三、写路径、随机覆盖写与读路径（I/O Paths & Consistency）

### 3.1 内存写缓冲抽象与小文件快路径

在 FUSE 句柄层，每个文件句柄持有一个非线程安全的 `WriteBuffer`（[`pkg/fuse/write.go:34-90`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L34-L90)），受 `FileHandle.mu` 保护：
- **小文件单缓冲区（`smallFileData []byte`）**：当写入未超出 `smallFileMax`（默认 50KB）且没有开启分段懒加载时，直接采用一块连续的切片内存写入（[`pkg/fuse/write.go:89`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L89) 与 [`pkg/fuse/write.go:252-254`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L252-L254)），消除哈希查找与切片分片开销。
- **大文件稀疏分段映射（`parts map[int][]byte`）**：一旦大小突破阈值，平滑晋升为分段稀疏表。分段大小 `DefaultPartSize = 8 MiB`（[`pkg/fuse/write.go:11`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L11)）。每个分段拥有独立的脏标记（`dirtyParts map[int]bool`）。

### 3.2 顺序写、流式上传与随机覆盖写

在 `WriteBuffer.Write(offset, data)`（[`pkg/fuse/write.go:235-306`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L235-L306)）中：
1. **跨分段写入**：将 `[offset, offset+len)` 换算为对应的 `partIdx = pos / partSize` 与内偏置 `partOff = pos % partSize`。
2. **读-改-写（RMW）与懒加载**：如果目标 Part 尚未在内存中初始化，调用 `ensurePart(partIdx)`（[`pkg/fuse/write.go:270`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L270)）。若该 Part 属于原文件已存在的区间（`startOffset < remoteSize`），则触发 `LoadPart` 从远端 S3 或本地缓存将旧数据读入内存，随后执行内存局部覆盖，并标记 `dirtyParts[partIdx] = true`。
3. **空洞填充（Gap zero-fill）**：若发生跳跃写入（`offset > totalSize`），中间缺失的 Part 会由 `ensurePart` 自动分配并用零字节填充（[`pkg/fuse/write.go:308-310`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L308-L310)）。
4. **顺序流式触发**：通过 `appendCursor` 探测顺序追加写。若当前写入填满了某个 8 MiB Part，立即通过 `OnPartFull` 回调触发后台流式分块上传（`StreamUploader`，[`pkg/fuse/write.go:79-82`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L79-L82)），无需等到 `close` 时阻塞串行上传全部数据。

### 3.3 读路径的分级降水式检索链

在 `Dat9FS.Read`（[`pkg/fuse/dat9fs.go:13259-13550`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13259-L13550)）中，读取请求按照严谨的数据可见性优先级降水式执行：
1. **Extent 模式分流**：若句柄标记为 `fh.isExtent()`，直接调用 `fs.extentRead` 委派给 JuiceFS VFS 切片读取引擎（[`pkg/fuse/dat9fs.go:13292-13300`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13292-L13300)）。
2. **已删除句柄（Unlink-while-open）快照**：若文件已被 unlink，但当前 fd 仍保持开启，直接由 `fh.UnlinkedData` 或专用 `UnlinkedShadowGen` 影子文件提供持久读取保障（[`pkg/fuse/dat9fs.go:13371-13414`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13371-L13414)），符合严格 POSIX 规范。
3. **句柄内部未提交脏数据**：优先从 `fh.Dirty`（`WriteBuffer`）中获取覆盖内容（[`pkg/fuse/dat9fs.go:13369`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13369)）。
4. **活跃并发追加（Active Append）区间可见性**：通过 `readActiveAppendVisibleRange` 跨句柄读取同一路径正在分段上传或已进入 Pending 状态的更新（[`pkg/fuse/dat9fs.go:13494`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13494)）。
5. **本地影子文件（ShadowStore）**：若句柄关联本地缓存文件，通过 `shadowStore.ReadAtGen` 读取（[`pkg/fuse/dat9fs.go:13403`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13403)）。
6. **用户态内存读缓存（`ReadCache`）**：针对小于 4 MiB 的文件，查询 LRU 内存切片（[`pkg/fuse/read.go:98-128`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/read.go#L98-L128)）。
7. **本地磁盘块读缓存（`DiskReadCache`）与远程 S3 GET**：未命中时，触发向对象存储的并发 Range Read 预取，并将下载块异步写入 `.drc` 文件（[`pkg/fuse/disk_read_cache.go:139`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/disk_read_cache.go#L139)）。

### 3.4 Flush 与 Fsync 的双模（Dual-mode）一致性机制

在分布式文件系统中，POSIX `fsync` 的延迟对上层应用影响巨大。drive9 支持两种同步工作模式（定义于 [`pkg/fuse/handle_fsync.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/handle_fsync.go) 与 [`pkg/fuse/handle_flush.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/handle_flush.go)）：

#### 交互式极速模式（`SyncInteractive`，默认）
专为开发工具、Git 编译与交互式命令设计（[`pkg/fuse/handle_fsync.go:173-270`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/handle_fsync.go#L173-L270)）：
1. **本地强持久化**：将内存脏数据全部写入本地磁盘影子文件 `ShadowStore`；
2. **写 WAL 日志**：向 `Journal` WAL 追加 `JournalFsync` 条目（记录 Inode、路径、长度和当前 BaseRev）；
3. **本地落盘确认**：调用 `fs.journal.FsyncShared()`，由 Group Commit 确保 WAL 日志物理落盘到本地磁盘；
4. **提前返回内核**：本地持久化完成后，`Fsync` 系统调用**立即返回 `OK`**（耗时通常在 1~5ms 之间）；
5. **异步远端提交**：后台通过 `commitQueue` 将脏数据分块推送到远端 S3 / TiDB。若此时主机掉电，重启后依靠本地 WAL 与 ShadowStore 重建待提交事务。

#### 强一致模式（`SyncStrict`）
面向严苛持久化场景（[`pkg/fuse/handle_fsync.go:298-350`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/handle_fsync.go#L298-L350)）：
- `Fsync` 不仅在本地落盘，而且必须同步等待远程 S3 多部分上传或 HTTP Commit 彻底完成，拿到 TiDB 的成功响应后才向内核返回。

#### SQLite 专有判定通道
源码显式规约：若目标文件为 SQLite 事务日志（`isSQLitePersistentJournalPath`），即使处于 `SyncInteractive` 模式，也强制升级执行远端同步，防止数据库损坏（[`pkg/fuse/handle_fsync.go:172`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/handle_fsync.go#L172)）。

### 3.5 远端 Commit 与元数据 CAS 版本控制

在将本地缓存数据推向 TiDB 元数据时，多挂载点并发可能引发写入冲突。drive9 在元数据层采用了乐观版本并发控制（CAS，基于 `revision`）：
- 每个 `File` 实体具备单调递增的 `Revision`（[`pkg/datastore/store.go:115`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/store.go#L115)）。
- 在提交覆盖写或多部分上传时，SQL 语句强制带上基准版本断言：`WHERE inode_id = ? AND revision = ?`（[`pkg/datastore/file_tx.go:180-220`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/file_tx.go#L180-L220)）。
- 若受影响行数等于 0，则说明期间存在其他客户端覆写，返回 `ErrRevisionConflict`，客户端必须重新拉取最新元数据并决定合并或抛错。

---

## 四、稀疏文件、Truncate、Fallocate 与配额核算（Sparse, Truncate & Quotas）

### 4.1 空洞（Hole）在 Extent 与 Shadow 中的表达

- **Extent 模型中的空洞**：如前所述，在 24 字节 Slice 结构中，`id == 0` 的 Slice 专门代表空洞。当用户执行 `lseek(SEEK_SET, 10GB)` 并写入少量数据时，前序庞大的未写入区间在 `jfs_chunk` 表中仅表现为 `id=0, len=10GB` 的元数据片段，**在底层物理对象存储上完全不分配任何对象，物理占用为 0**。
- **本地 Shadow 影子文件中的空洞**：在本地 Linux / macOS 磁盘上，`ShadowFile` 调用系统级 `ftruncate` 扩容文件，依赖本地文件系统（ext4/APFS）自身的 Sparse File 特性保留洞，且通过 `written shadowWrittenRanges`（[`pkg/fuse/shadow.go:39`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go#L39)）精确追踪当前进程实际写入的字节区间，配额扣减仅按实际覆盖计算。

### 4.2 文件截断（Truncate）的状态机保护

文件截断（`ftruncate`）是网络文件系统中极易破坏并发一致性的危险操作。drive9 为此构建了极其周密的状态机机制，分散在 `pkg/fuse/ftruncate_*.go` 中：
1. **别名栅栏（Alias Fence）**：当同一 Inode 在本地打开了多个文件描述符（跨 fd 或多进程打开），截断操作会通过 `ftruncateFence` 锁定所有别名句柄（[`pkg/fuse/ftruncate_reset.go:33-39`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/ftruncate_reset.go#L33-L39)）。
2. **截断可见性传递（`visibleTruncate`）**：若截断发生在一处句柄，通过标记 `WriteBuffer.visibleTruncate = true`，使其他并发句柄能立刻感知文件尺寸截短，不会读出已被截断的残留内存数据（[`pkg/fuse/write.go:53-55`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L53-L55) 与 [`pkg/fuse/dat9fs.go:13437`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13437)）。
3. **退役读取视图（Retired Read Image）**：对于已经发起的截断，正在运行中的并发读句柄会绑定至快照镜像（`retiredReadImage`，[`pkg/fuse/write.go:40`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L40)），保证原子快照读取隔离，直到全部读取完毕后才退役该镜像。

### 4.3 Fallocate 在元数据与数据面的语义落地

在 `dat9fs.go` 中，drive9 未暴露独立的 `Fallocate` FUSE opcode，而是将预分配与截断收敛到 `SetAttr(FATTR_SIZE)`（[`pkg/fuse/extent_ops.go:365-371`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/extent_ops.go#L365-L371) 与 [`pkg/fuse/extent_ops.go:520-548`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/extent_ops.go#L520-L548)）。
当触发大小变更时：
- Extent 文件直接路由至 `vfs.SetAttr(size)`，在 TiDB 中更新 `jfs_node.length`；
- 普通文件在服务端由事务统一处理尺寸扩展。

### 4.4 配额核算体系（双态记账与热行保护）

源码位于 [`pkg/backend/quota.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota.go) 与 [`pkg/backend/quota_store.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota_store.go)。

#### 双态记账模型（Two-phase Quota Accounting）
中心配额账本将使用量严格划分为两种状态（[`pkg/backend/quota_store.go:116-117`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota_store.go#L116-L117)）：
1. `ReservedBytes`（预留字节）：针对分块多部分上传，在上传开始前调用 `AtomicReserveAndInsertUpload`（[`pkg/backend/quota_store.go:34`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota_store.go#L34)）预先冻结配额；
2. `StorageBytes`（已确认存储）：上传完成时调用 `TransferReservedToConfirmed`（[`pkg/backend/quota_store.go:33`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota_store.go#L33)），将预留空间转为已确认空间。如果上传中止或超时，调用 `AbortActiveReservationTx` 退还预留字节。

#### 覆写 Delta 差额校验
在小文件覆写或 Patch 写入时，系统首先查询当前已确认文件的物理大小 `ConfirmedFileSizeByPathTx`，计算容量差额（[`pkg/backend/quota.go:51-58`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota.go#L51-L58)）：
$$\Delta_{\text{bytes}} = \text{newSize} - \text{currentSize}$$
若 $\Delta_{\text{bytes}} \le 0$（文件缩容或等长覆盖），无需额外配额直接放行；若 $\Delta_{\text{bytes}} > 0$，仅对增量部分执行配额准入检查，彻底避免全额双重计费。

#### 数据库热点行保护（Hot Row Last）
在更新单个租户的配额计数器行 `tenant_quota_usage` 时，如果每个操作都在事务开头更新该行，会导致高并发下整表行锁争用极其严重。drive9 采用 **"hot row last"** 原则（[`pkg/backend/quota_store.go:41-45`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota_store.go#L41-L45)）：在事务内优先处理文件节点、元数据与任务插入，将唯一的 `IncrQuotaUsageCountersTx` 挪到事务提交前的最后一行执行，将锁持有时间压缩至微秒级。

---

## 五、垃圾回收与崩溃恢复（GC & Crash Recovery）

### 5.1 物理数据块 GC：引用计数与 60s 宽限期

在 Extent 引擎中，旧数据块的回收涉及两层队列与安全机制，定义于 [`pkg/datastore/jfs_gc.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/jfs_gc.go)：
1. **切片引用计数（`jfs_chunk_ref`）**：
   每个物理 Slice ID 记录当前的引用数 `refs`。
2. **切片死亡与入队（`jfsEnqueueDeadSliceGCTx`）**（[`pkg/datastore/jfs_gc.go:35-119`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/jfs_gc.go#L35-L119)）：
   当覆盖写或 Compaction 导致某个 Slice 不再被任何逻辑 Chunk 引用（`refs <= 0`）时，系统并不立即物理删除对象。而是将其拆分为 4 MiB 的 Block 键名（格式为 `chunks/<id/1000000>/<id/1000>/<id>_<blockIndex>_<blockLen>`），批量写入 `block_gc_tasks` 表。
3. **60 秒安全宽限期（`extentBlockGCGraceSQL`）**（[`pkg/datastore/layout.go:22-31`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/layout.go#L22-L31)）：
   ```go
   extentBlockGCGraceSQL = "DATE_ADD(CURRENT_TIMESTAMP(3), INTERVAL 60 SECOND)"
   ```
   **设计原因**：当 Compaction 提交成功瞬间，旧 Slice 从元数据中消失；但集群中其他并发挂载点可能仍持有旧的元数据缓存并在读取旧 Slice。若立即物理删除 S3 对象，会导致并发读请求遭遇 `404 NoSuchKey` 致命穿孔！因此物理块入队后强制设定 60 秒的 `available_at`，由 TiDB 服务端时间评估，彻底规避客户端时钟漂移风险。

### 5.2 文件删除与孤儿清理：异步批量 Drain

在执行文件删除（`unlink`）时，若直接在事务中同步遍历并删除成百上千个 Chunk 的 Slice 引用，会导致巨大的事务锁延迟。drive9 的实现为（[`pkg/datastore/jfs_gc.go:136-150`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/jfs_gc.go#L136-L150)）：
- 事务内仅向 `jfs_delfile (inode, length, expire)` 插入一条极简记录，耗时极短；
- 后台 Worker 以 `jfsDeleteChunkBatch = 64` 为批次分段扫描 `jfs_chunk`，递减引用计数并逐步推进删除；
- 即便服务中途重启，未完成删除的文件依然常驻 `jfs_delfile`，开机后自动续跑，永不泄漏。

### 5.3 压缩合并（Compaction）与竞争丢失补偿

当 Chunk 内切片碎片增多时，后台定期触发 `ExecuteCompact`（[`pkg/extent/compact.go:13-85`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/compact.go#L13-L85)）：
1. 客户端通过 `readChunkRaw` 读取当前全量 Slice 列表（`origin`）；
2. 调用 `compactOverlay` 合并重叠切片，生成精简视图；
3. 从元数据服务申请新的 Slice ID，调用 `vfs.Compact` 将多切片拼接上传为全新的合并对象；
4. 执行 **CAS 提交**：将原 `origin` 字节流作为前置期望传给元数据服务提交更新。
5. **CAS 丢失补偿（`enqueueSliceGC`）**（[`pkg/extent/compact.go:68-80`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/compact.go#L68-L80)）：
   若两个客户端并发压缩同一 Chunk，后提交者会遭遇 `syscall.EINVAL`（CAS 冲突）。此时后提交者刚上传到对象存储的新物理块彻底失去元数据引用。系统通过捕获该状态，主动调用 `enqueueSliceGC(rt, id, size)` 将其直接送入物理 GC 队列，防止孤儿对象永久泄漏在对象存储中。

### 5.4 崩溃恢复：本地 Journal Replay 与待发索引重建

当系统意外宕机或崩溃后重启时，客户端通过 `replayJournalIntoPending`（[`pkg/fuse/journal.go:378-450`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L378-L450)）重构数据状态：
1. **重放 WAL 帧**：全量线性扫描本地 WAL 磁盘文件，按时间顺序扫描全部 `JournalEntry`；
2. **过滤已落盘路径**：记录每个路径最新的 `JournalCommit` / `JournalUnlink` 序列号 `latestDone`；若该路径最新的 `JournalFsync` 或 `JournalPendingMeta` 序列号大于 `latestDone`，说明存在未提交给远端的本地数据；
3. **尺寸完备性防御（Anti-torn-write Guard）**（[`pkg/fuse/journal.go:436-441`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L436-L441)）：
   检查本地 `ShadowStore` 中的影子文件。若磁盘影子文件的大小小于元数据声明的大小（`size < meta.Size`），说明断电瞬间发生残缺写入（Torn Write），系统**坚决拒绝恢复该帧**并丢弃损坏的影子文件，回滚到前序安全的快照版本，防止将残缺损坏数据上传覆盖远端健康的修订版。
4. **重建待发索引（`PendingIndex`）**：校验完整的有效条目被重新注册到待提交队列，唤起异步上传线程推向远端。

---

## 六、对 Bowl 系统的架构启发与设计取舍

### 6.1 存储介质与拓扑假设的本质差异

| 架构维度 | drive9 设计假设 | Bowl 系统设计定位 |
| :--- | :--- | :--- |
| **底层数据存储** | 远端对象存储（S3 / GCS / MinIO），单次 PUT 延迟 50~200ms | **本地或专用网络 NVMe SSD**（块设备/本地 Chunk 文件），I/O 延迟在微秒级（< 100µs） |
| **元数据存储** | TiDB / MySQL（网络分布式数据库，支持多挂载点并发访问） | **PostgreSQL 单实例 / 强一致主备**（极高事务吞吐与纯粹关系型约束） |
| **写路径妥协** | 必须采用复杂的本地 `ShadowStore` + 本地 WAL + 异步上传（Write-back） | **可采用 Direct I/O 同步落盘或简短 RingBuffer**，无需建立庞大的全套异步对象上传器 |
| **客户端复杂度** | 客户端（FUSE）极重，维护切片合并、WAL 压缩、S3 分段上传与多级缓存 | **服务端重、客户端薄**：客户端仅负责向服务端传输 Chunk 读写，复杂逻辑内聚在服务端 |

### 6.2 元数据引擎与事务边界的划分

- **drive9 的两难与代价**：
  drive9 试图在同一套文件系统中融合“经典单对象模式”与“JuiceFS Extent 模式”，同时在应用层通过独立租户数据库与中心元数据数据库分别管理文件树与商业配额。导致系统不得不实现复杂的**最终一致性突变重放机制（`mutation_replay.go`）**与复杂的补偿 GC 逻辑。
- **对 Bowl 的建议**：
  Bowl 在 [ADR-0015](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0015-unify-disk-and-storage-service-with-transactional-quota.md) 中已果断采纳“合并 Disk 与 Storage 服务”的决策，将 Inode 树与磁盘配额强收敛到 PostgreSQL 单事务内。
  **第一性原理结论**：在设计 Chunk 数据层元数据表（如 `chunk_mappings`）时，**绝不要**像 drive9 一样脱离核心事务另外开辟一套独立的队列系统；必须将 Chunk 物理分配记录与 Inode 元数据变更（`SetAttr`、`Write`、`Truncate`）置于同一个 PostgreSQL 事务中，以 ACID 事务直接保证“元数据修改即物理块映射落地”。

### 6.3 Extent 区间切片管理对 Bowl Chunk 映射的借鉴与简化

1. **切片树的工程精妙性**：
   drive9 的 `rawSlice` 剪裁树（[`pkg/extent/slices.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go)）使用 24 字节结构完美表达了重叠覆写与空洞，算法纯粹、无内存动态扩容惩罚。
2. **Bowl 的简化取舍（定长 Chunk vs 变长 Slice）**：
   - drive9 之所以需要动态切片与后续极其复杂的 Compaction，是因为底层是**不可追加写、不可部分覆写的 S3 对象存储**。
   - 而 Bowl 的数据面直接落盘在 **NVMe SSD** 上，可以直接利用底层文件系统的随机 `pwrite(2)` 或块设备的按块覆写能力！
   - **取舍结论**：Bowl 应当采用**固定大小的 Chunk（如 64 MiB）**，并在 Chunk 内部按固定 Block（如 4 KiB）对齐寻址；随机覆盖写直接定位到 NVMe 物理偏移就地覆写（In-place Write）或写入 Copy-on-Write 块，**完全不需要引入代价昂贵的对象拼接 Compaction 后台任务**。

### 6.4 本地写日志（Write Journal）在 NVMe 架构下的取舍

- drive9 的本地 WAL（[`pkg/fuse/journal.go`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go)）核心价值在于为异步 S3 上传提供“暂存断电保护”。
- 对于 Bowl：
  - 如果数据最终是持久化在服务端的 NVMe SSD 上，客户端与服务端之间走 gRPC / ConnectRPC 协议。
  - 当客户端发起写请求时，服务端利用内核 Page Cache 或 Direct I/O 写入 SSD 物理文件，并在 `Fsync` 时触发底层 NVMe 设备的刷新。
  - **建议**：Bowl 的 FUSE 客户端仅需保留精简的内存读写缓冲（RingBuffer / PageCache），无需在客户端本地磁盘构建复杂的 WAL 日志系统；崩溃一致性通过服务端的 WAL（或 PostgreSQL 事务与 Chunk 落盘协议）统一由服务端底座承载。

### 6.5 容量配额账本：单事务闭环与物理块统计

- drive9 的 `ensureStorageQuota` 需要在事务中计算 `deltaBytes`，并通过 “hot row last” 延迟更新租户配额行（[`pkg/backend/quota.go:51`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota.go#L51)）。
- Bowl 在 [ADR-0015](file:///Users/fanxing/code/starsrc/bowl/docs/adr/0015-unify-disk-and-storage-service-with-transactional-quota.md) 中已确立 `used_bytes` 的单事务严格原子更新。
- **drive9 带来的补充警示**：
  在引入 Chunk 物理落盘后，文件的逻辑大小（`size`）与物理块占用（`allocated_bytes`）可能因稀疏空洞或块对齐（4KB 对齐）而产生差异。Bowl 应当在配额模型中明确区分：
  - 商业计费配额：是按逻辑文件大小 `size` 核算，还是按底层 NVMe 物理占用的 Chunk 块数核算。
  - 建议继续遵循 ADR-0015 的逻辑字节判定原则，但底层 Chunk 分配器必须严密保证空洞区间不向物理介质申请任何物理存储块。

---

## 七、总结与参考源码索引表

| 核心组件 | 关键职责 | 核心源码路径 |
| :--- | :--- | :--- |
| **Extent 切片模型** | 24 字节 Slice 结构定义、动态重叠剪裁树与空洞表达 | [`pkg/extent/slices.go:9-105`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/slices.go#L9-L105) |
| **Extent 压缩合并** | 切片合并、Blob 上传、CAS 原子提交与补偿 GC | [`pkg/extent/compact.go:13-115`](/Users/fanxing/code/other/filesystem/drive9/pkg/extent/compact.go#L13-L115) |
| **Extent 数据库表结构** | `jfs_chunk`、`jfs_chunk_ref`、`slice_compact_tasks` | [`pkg/tenant/schema/extent.go:63-136`](/Users/fanxing/code/other/filesystem/drive9/pkg/tenant/schema/extent.go#L63-L136) |
| **FUSE 本地 WAL 日志** | 二进制帧封装、CRC32 校验、组提交 `FsyncShared` | [`pkg/fuse/journal.go:58-208`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L58-L208) |
| **本地写回影子存储** | 影子文件生命周期、代际代币（Pin/Retire）与配额防护 | [`pkg/fuse/shadow.go:32-150`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/shadow.go#L32-L150) |
| **磁盘分块读缓存** | `.drc` 结构、LRU 队列与磁盘空闲水位控制 | [`pkg/fuse/disk_read_cache.go:38-150`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/disk_read_cache.go#L38-L150) |
| **内存写缓冲区** | 小文件单切片与大文件 8MB 分段稀疏映射转换 | [`pkg/fuse/write.go:34-112`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/write.go#L34-L112) |
| **FUSE 读分流** | 7 级降水式检索链与未提交数据优先可见性 | [`pkg/fuse/dat9fs.go:13285-13550`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/dat9fs.go#L13285-L13550) |
| **FUSE Fsync 同步机制** | `SyncInteractive` 本地持久化与 `SyncStrict` 远端提交 | [`pkg/fuse/handle_fsync.go:170-300`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/handle_fsync.go#L170-L300) |
| **配额核算与热行优化** | 双态记账（预留/确认）、覆写 Delta 差额与 Hot-row-last | [`pkg/backend/quota.go:44-70`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota.go#L44-L70), [`pkg/backend/quota_store.go:41-45`](/Users/fanxing/code/other/filesystem/drive9/pkg/backend/quota_store.go#L41-L45) |
| **切片块垃圾回收** | 4MB 块分解、60s 读安全宽限期与引用计数归零 | [`pkg/datastore/jfs_gc.go:20-119`](/Users/fanxing/code/other/filesystem/drive9/pkg/datastore/jfs_gc.go#L20-L119) |
| **崩溃恢复重放** | WAL 重放、残缺写入（Torn-write）检测与待发索引恢复 | [`pkg/fuse/journal.go:378-450`](/Users/fanxing/code/other/filesystem/drive9/pkg/fuse/journal.go#L378-L450) |
