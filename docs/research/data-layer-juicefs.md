# JuiceFS 数据层（Data Layer）一手源码深度调研报告

## 1. 调研背景与架构全景

本报告基于 JuiceFS 开源仓库（`github.com/juicedata/juicefs`）一手源码，针对其数据层（Data Layer）的架构抽象、存储模型、落盘布局、读写控制、垃圾回收及一致性保障机制进行深度调研。

JuiceFS 采用“元数据引擎（Metadata Engine）+ 对象存储（Object Storage）”松耦合架构。数据层将文件逻辑数据切分为固定尺寸的逻辑块，通过追加切片写入对象存储，并在元数据引擎中维护切片区间映射关系。

```
+-----------------------------------------------------------------------------------+
|                                 VFS Layer (POSIX)                                 |
|      pkg/vfs/writer.go (fileWriter / chunkWriter / sliceWriter / commitThread)    |
|      pkg/vfs/reader.go (dataReader / sliceReader / readManySlices)                |
+-----------------------------------------------------------------------------------+
                                         |
     +-----------------------------------+-----------------------------------+
     | (元数据变更)                                                          | (数据块 I/O)
     v                                                                       v
+------------------------------------+                  +------------------------------------+
|       Metadata Layer               |                  |         Chunk Store Layer          |
|  pkg/meta/base.go (baseMeta)       |                  |  pkg/chunk/cached_store.go         |
|  pkg/meta/slice.go (buildSlice)    |                  |  pkg/chunk/page.go (Pool)          |
|  pkg/meta/sql.go (dbMeta)          |                  |  pkg/chunk/disk_cache.go           |
|  - Table: chunk (Slices blob)      |                  +------------------------------------+
|  - Table: chunk_ref (引用计数)     |                                     |
|  - Table: delslices (延迟删除)     |                  +------------------+-----------------+
+------------------------------------+                  | (本地缓存 / Staging)| (后端持久化)
                                                        v                  v
                                                +---------------+  +-------------------------+
                                                | NVMe SSD 缓存 |  | Object Storage Backend  |
                                                | raw / staging |  | (S3 / OSS / file.go)    |
                                                +---------------+  +-------------------------+
```

---

## 2. 分块与寻址模型

JuiceFS 将任意大小的 POSIX 文件按逻辑维度与物理维度划分为三层抽象：**Chunk**、**Slice**、**Block**。

### 2.1 三层切分设计

1. **Chunk（逻辑分块，64 MiB）**：
   - 符号与定义：[`pkg/meta/interface.go#L38-L41`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/interface.go#L38-L41)
     ```go
     const (
         ChunkBits = 26
         ChunkSize = 1 << ChunkBits // 64M (67,108,864 字节)
     )
     ```
   - 作用：文件的逻辑切分单元。任意文件逻辑偏移 `offset` 对应的 Chunk 索引为 `indx = uint32(offset / ChunkSize)`，内部偏移量为 `pos = uint32(offset % ChunkSize)`。
   - 文件最大支持尺寸由 Chunk 数量决定（[`pkg/vfs/vfs.go#L46`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/vfs.go#L46)：`maxFileSize = meta.ChunkSize << 31`，即 128 PiB）。

2. **Slice（逻辑写入切片，动态长度 <= 64 MiB）**：
   - 符号与定义：[`pkg/meta/interface.go#L337-L344`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/interface.go#L337-L344) 与 [`pkg/meta/slice.go#L21-L29`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go#L21-L29)
     ```go
     type Slice struct {
         Id   uint64 // 全局唯一 Slice ID
         Size uint32 // 该 Slice 对应底层存储对象的总字节数
         Off  uint32 // 当前数据在该 Slice 对象中的起始读取偏移
         Len  uint32 // 当前数据在该 Slice 对象中的有效连续长度
     }
     ```
   - 作用：客户端单次连续追加写的逻辑载体。同一个 Chunk 内发生随机写、覆盖写或并发写时，JuiceFS 不覆写已存在的 Slice，而是分配新 Slice 追加写入。
   - Slice ID 分配机制：客户端通过元数据引擎 RPC `NewSlice` 获取全局递增 ID（[`pkg/meta/base.go#L2200-L2214`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/base.go#L2200-L2214)）。为降低元数据引擎压力，客户端每次批量向元数据引擎申请 `sliceIdBatch = 10000` 个 ID 并在本地无锁发号。

3. **Block（物理存储块，默认 4 MiB）**：
   - 符号与定义：[`pkg/meta/config.go#L87`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/config.go#L87) 与 [`pkg/chunk/cached_store.go#L65-L83`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/cached_store.go#L65-L83)
   - 默认配置：`Format.BlockSize = 4096` KiB（即 4 MiB）。
   - 作用：后端对象存储（以及本地缓存系统）的物理读写与存储单元。一个长度为 $L$ 的 Slice 会被均匀切分为 $\lceil L / \text{BlockSize} \rceil$ 个 Block，每个 Block 独立上传或下载。

### 2.2 二进制编码与数据库存储结构

元数据层使用 `(inode, indx)` 二元组唯一定位一个 Chunk。

#### 2.2.1 数据库物理表结构（以 SQL / PostgreSQL 为例）
在 [`pkg/meta/sql.go#L168-L185`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L168-L185) 中定义了数据存储表：
```go
type chunk struct {
    Id     int64  `xorm:"pk bigserial"`
    Inode  Ino    `xorm:"unique(chunk) notnull"`
    Indx   uint32 `xorm:"unique(chunk) notnull"`
    Slices []byte `xorm:"blob notnull"`
}

type sliceRef struct {
    Id   uint64 `xorm:"pk chunkid"`
    Size uint32 `xorm:"notnull"`
    Refs int    `xorm:"index notnull"`
}

type delslices struct {
    Id      uint64 `xorm:"pk chunkid"`
    Deleted int64  `xorm:"notnull"`
    Slices  []byte `xorm:"blob notnull"`
}
```

#### 2.2.2 Slice 的 24 字节定长二进制编码
在 `chunk` 表的 `slices` 字段中，并非使用 JSON 或 Protobuf，而是直接采用严格对齐的 **24 字节定长二进制序列**（[`pkg/meta/slice.go#L91-L100`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go#L91-L100)）：
```go
const sliceBytes = 24

func marshalSlice(pos uint32, id uint64, size, off, len uint32) []byte {
    w := utils.NewBuffer(sliceBytes)
    w.Put32(pos)  // [0..3]   Slice 在当前 Chunk 内的起始逻辑偏移 [0, 64MB)
    w.Put64(id)   // [4..11]  全局 Slice ID (uint64, 大端序)
    w.Put32(size) // [12..15] Slice 底层对象总长度
    w.Put32(off)  // [16..19] 本段有效数据在 Slice 内的偏移行
    w.Put32(len)  // [20..23] 本段有效数据的连续长度
    return w.Bytes()
}
```
当一个 Chunk 经历多次覆盖写时，`slices` 二进制字段的长度严格为 $24 \times N$ 字节（[`pkg/meta/slice.go#L118-L132`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go#L118-L132) 的 `readSliceBuf` 会校验 `len(buf) % sliceBytes == 0`）。

---

## 3. 本地磁盘 / NVMe SSD 物理落盘布局

JuiceFS 支持将数据缓存在本地 NVMe SSD（`pkg/chunk/disk_cache.go`），且支持以本地单机/共享文件系统作为持久化后端（`pkg/object/file.go`）。

### 3.1 对象存储键与路径命名规则

Block 对象的全局命名键规则实现于 [`pkg/chunk/cached_store.go#L74-L79`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/cached_store.go#L74-L79)：
```go
func (s *rSlice) key(indx int) string {
    if s.store.conf.HashPrefix {
        return fmt.Sprintf("chunks/%02X/%v/%v_%v_%v", s.id%256, s.id/1000/1000, s.id, indx, s.blockSize(indx))
    }
    return fmt.Sprintf("chunks/%v/%v/%v_%v_%v", s.id/1000/1000, s.id/1000, s.id, indx, s.blockSize(indx))
}
```
- **分层目录规则**：按 `s.id` 计算千万级与百万级目录；若启用 `HashPrefix`（散列前缀，避免对象存储或单目录 inode 热点），第一级为 `s.id % 256` 的两字符十六进制大写目录（如 `chunks/3A/0/...`）。
- **文件命名格式**：`<slice_id>_<block_index>_<block_size>`。例如 `1024001_0_4194304` 代表 Slice ID 为 1024001、第 0 个 Block、大小为 4 MiB。

### 3.2 disk_cache.go 本地 SSD 缓存布局与 Staging 机制

本地磁盘缓存包含两级目录（[`pkg/chunk/disk_cache.go#L43-L44`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go#L43-L44)）：
- `cacheDir = "raw"`：只读读取缓存目录，路径为 `<cache-dir>/raw/<key>`。
- `stagingDir = "rawstaging"`：写回模式（writeback）下数据暂存目录，路径为 `<cache-dir>/rawstaging/<key>`。

#### 3.2.1 写入与硬链接晋升流程
当启用 writeback 或写缓存时（[`pkg/chunk/disk_cache.go#L774-L800`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go#L774-L800) 的 `stage` 方法）：
1. 写入临时 staging 文件：`<cache-dir>/rawstaging/<key>.tmp`，写入完成后重命名为 staging 目标路径。
2. 建立硬链接（Zero-copy Promotion）：若同时启用了读缓存，通过 `os.Link(stagingPath, cachePath)` 在 `raw/` 目录下建立同名硬链接，无需数据复制。
3. 负值容量占位标记：调用 `cache.add(key, -int32(len(data)), ...)`。**尺寸传入负数**代表该数据块正在 staging 阶段，常规缓存淘汰算法会跳过负数尺寸的块，防止未完成上传的数据被提前淘汰（[`pkg/chunk/cache_eviction.go#L162-L164`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/cache_eviction.go#L162-L164)）。
4. 上传完成后转正：后台上传线程将数据推送到对象存储后，调用 `cache.uploaded(key, size)`（[`pkg/chunk/disk_cache.go#L802-L804`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go#L802-L804)），将尺寸改为正数，该文件正式受淘汰策略管辖。

### 3.3 Checksum 数据校验机制

在本地缓存或 Stage 文件落盘时，JuiceFS 支持追加校验码与元数据尾部（Footer），实现在 [`pkg/chunk/disk_cache_file.go#L29-L152`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache_file.go#L29-L152)：
1. **分段校验结构**：
   - 常量定义：`csBlock = 32 << 10`（每 32 KiB 数据计算一个校验和）。
   - 算法：`crc32.Castagnoli`（硬件加速 CRC32C）。
   - 存储布局：4 MiB 的 Block 划分为 128 个 32 KiB 子块，生成 $128 \times 4 = 512$ 字节的 Checksum，直接追加在文件数据末尾：`[Data: 4MiB] [Checksum: 512B] [StageFooter: MsgPack]`。
2. **四种校验级别（csLevel）**（[`pkg/chunk/disk_cache_file.go#L30-L34, L191-L245`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache_file.go#L30-L34)）：
   - `CsNone ("none")`：不执行校验。
   - `CsFull ("full")`：仅在完整读取整个 Block（`off == 0 && len(b) == cf.length`）时才进行校验。
   - `CsShrink ("shrink")`：仅对读请求完全覆盖的 32 KiB 整块计算校验。
   - `CsExtend ("extend")`：若读取范围未对齐 32 KiB，向前和向后扩展读取窗口至 32 KiB 边界（`NewOffPage` 分配对齐临时缓冲），校验成功后再截取用户请求的字节切片返回。

### 3.4 SSD 缓存淘汰策略与配额

缓存容量与配额实现在 [`pkg/chunk/disk_cache.go#L101-L185, L820-L870`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go#L101-L185) 与 [`pkg/chunk/cache_eviction.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/cache_eviction.go)：
1. **配额管理**：
   - 限制指标包括总字节数 `capacity` 与 inode 数 `maxItems`。
   - 动态保留水位：通过 `freeRatio`（默认保留 10% 磁盘空间与 inode）调用 `curFreeRatio()` 查询底层挂载点 `Statfs`，动态调低可用配额（`setLimitByFreeRatio`）。
2. **三种淘汰策略（`KeyIndex`）**：
   - `none`：不淘汰，写满即报 `errStageFull`。
   - `2-random`（默认策略）：[`pkg/chunk/cache_eviction.go#L136-L185`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/cache_eviction.go#L136-L185)，基于 Go map 随机迭代器的 2 选 1 蓄水池抽样。连续抽样两个条目，对比其访问时间 `atime`，优先淘汰访问时间更早的一个。具有 $O(1)$ 内存开销且几乎无锁竞争。
   - `lru`：[`pkg/chunk/cache_eviction.go#L200-L330`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/cache_eviction.go#L200-L330)，基于标准最小堆（Min-Heap）维护精确 `atime`。
3. **写入冷却防护（Cooldown）**：
   - 为防止突发大写入流冲垮热点读取缓存，刚写入 Staging 的块在记录缓存时间戳时被显式扣减冷却期：`atime = time.Now().Add(-cache.stagedBlockCooldown)`（[`pkg/chunk/disk_cache.go#L793`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go#L793)）。

### 3.5 I/O 方式与内存池模型

1. **直接 I/O 与 Page Cache 管理**：
   - JuiceFS 未使用 `O_DIRECT` 打开本地缓存文件。
   - 避免操作系统双重缓存占用（OS PageCache + 用户态 Cache）：在 `flushPage` 写完文件后，若 `dropCache=true`，会显式调用 `dropOSCache(f)`（[`pkg/chunk/disk_cache.go#L560`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go#L560)）。在 Linux 下通过 `unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)` 主动让内核驱逐该文件的 Page Cache（[`pkg/chunk/utils_linux.go#L34-L40`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/utils_linux.go#L34-L40)）。
2. **堆外内存池与 Page 模型**：
   - 引用计数 Page：[`pkg/chunk/page.go#L33-L97`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/page.go#L33-L97) 定义 `Page` 结构体，持有 `refs int32` 引用计数与 `dep *Page` 依赖链。切片派生时通过 `Acquire()` 共享底层数组，`Release()` 归零时自动释放。
   - 2 的幂次分级内存池：[`pkg/utils/alloc.go#L65-L87`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/utils/alloc.go#L65-L87) 初始化了 34 个 `sync.Pool`（覆盖 $1 \sim 8\text{ GiB}$，每个池负责 $2^k$ 字节缓冲）。`Alloc0(size)` 自动寻找最近的 $2^{\lceil \log_2(\text{size}) \rceil}$ 字节缓冲切片，大幅降低大块 I/O 下的 Go GC 压力。

### 3.6 object/file.go 本地存储后端实现

在 [`pkg/object/file.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/object/file.go) 中，JuiceFS 将本地目录模拟为对象存储后端：
- 路径拼接：`path(key)` 映射为 `<root>/<key>`（[`pkg/object/file.go#L70-L75`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/object/file.go#L70-L75)）。
- 原子落盘：`Put` 操作默认先写入临时文件 `TmpFilePath(p, name)`，数据写毕并 `f.Close()` 后，通过 `os.Rename(tmp, p)` 实现 POSIX 原子重命名发布（[`pkg/object/file.go#L155-L199`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/object/file.go#L155-L199)）。
- 区间读取：`Get` 操作直接通过 `io.NewSectionReader(f, off, limit)` 实现只读切片（[`pkg/object/file.go#L120-L145`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/object/file.go#L120-L145)）。

---

## 4. 写路径、随机覆盖写与读路径

### 4.1 写缓冲切分与 Block 上传

客户端写入的核心实现在 `pkg/vfs/writer.go`：
1. **层级控制模型**：
   - `fileWriter`（文件级锁与缓冲统计）：管理当前打开文件的全量写入状态。
   - `chunkWriter`（64 MiB Chunk 级写状态）：管理分配至该 Chunk 的 `slices []*sliceWriter`。
   - `sliceWriter`（Slice 级写状态）：持有底层 `chunk.Writer`，负责缓冲数据并向对象存储写入 Block。
2. **切分与追加逻辑（`findWritableSlice`）**（[`pkg/vfs/writer.go#L184-L203`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/writer.go#L184-L203)）：
   ```go
   func (c *chunkWriter) findWritableSlice(pos uint32, size uint32) *sliceWriter {
       blockSize := uint32(c.file.w.blockSize)
       for i := range c.slices {
           s := c.slices[len(c.slices)-1-i]
           if !s.freezed {
               flushoff := s.slen / blockSize * blockSize
               if pos >= s.off+flushoff && pos <= s.off+s.slen {
                   return s
               } else if i > 3 {
                   s.freezed = true
                   go s.flushData()
               }
           }
           if pos < s.off+s.slen && s.off < pos+size {
               return nil // 发生重叠覆盖，禁止复用当前 Slice
           }
       }
       return nil
   }
   ```
   - 若写入位置 `pos` 严格落在未冻结 Slice 的末尾未冲刷窗口内，复用该 `sliceWriter`。
   - 累积满 4 MiB Block 时，触发 `writer.FlushTo(int(s.slen))` 异步上传。
   - 若 Slice 达到 64 MiB（`meta.ChunkSize`），标记 `freezed = true` 并启动后台 `flushData()`（[`pkg/vfs/writer.go#L162-L164`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/writer.go#L162-L164)）。

### 4.2 CommitWrite 元数据提交流程

当一个 Slice 冻结且所有包含的 Block 全部上传成功后，由后台专有协程 `commitThread()` 提交元数据（[`pkg/vfs/writer.go#L206-L253`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/writer.go#L206-L253)）：
1. **依赖保序（Growing Dependency）**：若属于跨 Chunk 的连续增长写，后一个 Chunk 的首个 Slice 会设置 `s.dep = lastSlice`，必须等待前序 Slice 提交完成后方可提交，确保文件长度单调递增（[`pkg/vfs/writer.go#L220-L222`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/writer.go#L220-L222)）。
2. **提交元数据**：调用 `f.w.m.Write(ctx, inode, c.indx, s.off, Slice{Id: s.id, Size: s.length, Off: s.soff, Len: s.slen}, ...)`。
3. **数据库追加写入**（[`pkg/meta/sql.go#L1660-L1683, L3544-L3551`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L1660-L1683)）：
   - 执行 `upsertSlice`：在 PostgreSQL 中使用原子 SQL 拼接：
     ```sql
     INSERT INTO chunk (inode, indx, slices)
     VALUES ($1, $2, $3)
     ON CONFLICT (inode, indx)
     DO UPDATE SET slices = chunk.slices || EXCLUDED.slices;
     ```
   - 同步插入引用计数表：`INSERT INTO chunk_ref (chunkid, size, refs) VALUES (slice.id, slice.size, 1)`。
4. **客户端读缓存失效**：调用 `f.w.reader.Invalidate(...)` 使得旧的读缓存失效（[`pkg/vfs/writer.go#L229`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/writer.go#L229)）。

### 4.3 随机覆盖写：为何采用 Append-only Slice 叠加

在对象存储中，对象是不可变（Immutable）的，无法就地修改某几个字节。若采用原地覆写，必须对 4 MiB 的 Block 执行“读-修改-写”（Read-Modify-Write），这在跨公网高延迟下会导致严重的性能退化与并发写丢失冲突。

因此，JuiceFS 针对随机写采用 **LSM-tree 式的 Append-only 叠加模型**：
- 任何覆盖写均生成一个独立的局部 Slice，直接上传新 Block，并在数据库 `chunk.slices` 尾部追加 24 字节记录。
- 物理开销被降至最低：写路径无需等待旧数据读取，只有单纯的网络顺序上传与元数据极小追加。

### 4.4 读路径：区间覆写裁剪（Interval Cutting 算法）

既然底层存储存在多层重叠的 Slice，读路径必须负责**将多层历史 Slice 合并裁切为单层无重叠的真实有效数据区间**。

#### 4.4.1 buildSlice 区间二叉树裁切算法
核心算法位于 [`pkg/meta/slice.go#L55-L155`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go#L55-L155)：
```go
func buildSlice(ss []*slice) []Slice {
    var root *slice
    for i := range ss {
        s := new(slice)
        *s = *ss[i]
        var right *slice
        s.left, right = root.cut(s.pos)       // 左剪切点：s.pos
        _, s.right = right.cut(s.pos + s.len) // 右剪切点：s.pos + s.len
        root = s                              // 新 Slice 成为当前子树根节点，覆盖历史重叠区域
    }
    var pos uint32
    var chunk []Slice
    root.visit(func(s *slice) {               // 中序遍历还原升序有序区间
        if s.pos > pos {
            chunk = append(chunk, Slice{Size: s.pos - pos, Len: s.pos - pos}) // 自动补齐空洞洞
            pos = s.pos
        }
        chunk = append(chunk, Slice{Id: s.id, Size: s.size, Off: s.off, Len: s.len})
        pos += s.len
    })
    return chunk
}
```
- **工作机制**：`cut(pos)` 递归分裂历史区间。因为 `ss` 是按时间先后顺序排列的，后发生的写操作（数组靠后的 Slice）作为“剪刀”切入已有的二叉树，将重叠区域的旧 Slice 截短、分裂或丢弃，然后将新 Slice 插入作为子树根。
- **空洞填补**：中序遍历（`visit`）时，若相邻两段 Slice 存在间隙（`s.pos > pos`），说明存在未写入区域（Hole），自动插入 `Id == 0` 的空洞切片。

#### 4.4.2 dataReader 读取并发与空洞处理
在 [`pkg/vfs/reader.go#L813-L879`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/reader.go#L813-L879) 中：
1. **空洞快速返回**：当 `s.Id == 0` 时，直接在用户内存缓冲区中执行 `memset(0)`，不发生任何底层网络或磁盘 I/O（[`pkg/vfs/reader.go#L816-L821`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/reader.go#L816-L821)）。
2. **多 Slice 并发聚合读取**：当一个读请求跨越多个裁切后的 Slice 时，`dataReader.Read` 会并发向 `ChunkStore` 发起子区间请求，各协程使用 `page.Slice(...)` 共享切片写入不同内存段，并在 `waits` 归零后统一向内核返回数据。

---

## 5. 稀疏文件、Truncate、Fallocate 与配额核算

### 5.1 稀疏文件（Sparse File）表示

在 JuiceFS 中，稀疏空洞有两种表现形式：
1. **整块 Chunk 空洞**：若一个 64 MiB Chunk 从未被写入，数据库 `chunk` 表中**完全不存在**该 `(inode, indx)` 记录。`doRead` 返回空列表，上层直接返回全零数据，元数据存储开销为 0。
2. **Chunk 内部空洞**：若 Chunk 内部存在部分空洞，在切片列表中通过 `Id == 0` 且 `pos, len` 明确的虚拟切片表示。

### 5.2 Truncate 截断机制

在 [`pkg/meta/sql.go#L1685-L1754`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L1685-L1754) 的 `doTruncate` 中：
1. **扩大文件（Truncate Extend）**：
   - 纯元数据变更：只更新 `node.length = length`，不向底层写入任何零字节数据。后续读取超过原长度的区域自然落入“无 Chunk 记录”规则，返回全零。
2. **缩小截断（Truncate Shrink）**：
   - 零物理重写：不删除底层对象存储中的已有 Block，而是向受影响的 Chunk 尾部追加一个“空洞切片”（`id = 0`，`size = 0`，`off = 0`，`len = 被截断长度`）：
     ```go
     m.appendSlice(s, inode, uint32(left/ChunkSize), marshalSlice(uint32(left%ChunkSize), 0, 0, 0, l))
     ```
   - 中间完整 Chunk 覆盖：对处于截断区间内的整块 Chunk，追加全长 64 MiB 的空洞切片（`marshalSlice(0, 0, 0, 0, ChunkSize)`）。
   - 该机制使得缩小截断成为 $O(1)$ 的瞬时数据库追加操作，被截断的数据在读路径中被 `buildSlice` 裁切覆盖遮蔽，后续由异步 Compaction 与 GC 回收。

### 5.3 Fallocate（含 PUNCH_HOLE 与 ZERO_RANGE）

在 [`pkg/meta/sql.go#L1757-L1825`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L1757-L1825) 的 `doFallocate` 中：
- 当模式包含 `FALLOC_FL_PUNCH_HOLE` 或 `FALLOC_FL_ZERO_RANGE` 时（`mode & (fallocZeroRange | fallocPunchHole) != 0`）：
  - 遍历目标范围涉及的全部 Chunk，针对每个 Chunk 计算局部偏移 `coff` 与长度 `l`。
  - 直接调用 `m.appendSlice(s, inode, indx, marshalSlice(uint32(coff), 0, 0, 0, uint32(l)))`，将 `id=0` 的空洞切片追加到对应 Chunk 的 `slices` 二进制末尾。
  - **实现零数据重写打洞**：无需重写或裁剪已有对象存储 Block，毫秒级完成 GB 甚至 TB 级别的打洞。

### 5.4 容量配额核算机制

JuiceFS 的存储容量核算与 POSIX 对齐，实现在 [`pkg/meta/utils.go#L184-L189`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/utils.go#L184-L189) 的 `align4K`：
```go
func align4K(length uint64) int64 {
    if length == 0 {
        return 1 << 12 // 4096 字节
    }
    return int64((((length - 1) >> 12) + 1) << 12)
}
```
- **逻辑长度核算**：JuiceFS 的目录树配额统计（`checkQuota`、`updateStats`、`dirStat`）以文件逻辑长度向上对齐 4 KiB 为基准：`delta.space = align4K(newLength) - align4K(oldLength)`（[`pkg/meta/sql.go#L1709, L3533`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L1709)）。
- **打洞对配额的影响**：由于 `FALLOC_FL_PUNCH_HOLE` 强制携带 `FALLOC_FL_KEEP_SIZE`，文件逻辑长度未变，因此在 JuiceFS 当前实现中，PUNCH_HOLE 不会减少文件系统统计中的已用空间配额；只有实际 `truncate` 改变文件长度或删除文件时，配额计数才会被调减。

---

## 6. 碎片整理（Compaction）与垃圾回收（GC）

### 6.1 Compaction 碎片整理机制

由于追加写与覆盖写会使 `chunk.slices` 二进制列表持续膨胀，切片碎片过多会导致读放大与元数据膨胀。

#### 6.1.1 触发时机
1. **读触发（异步）**：[`pkg/meta/base.go#L2190-L2196`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/base.go#L2190-L2196)，当客户端执行 `Read` 时，若原始切片数 `len(ss) >= 5` 或裁切后切片数 `len(*slices) >= 5`，触发后台协程 `go m.compactChunk(inode, indx, false, false, tierID)`。
2. **写触发**：在关闭文件或提交写时若超过碎片阈值（如 `maxSlices = 10`）也会触发。
3. **管理命令触发**：通过 `juicefs gc --compact` 主动批量执行（[`cmd/gc.go#L175-L195`](file:///Users/fanxing/code/other/filesystem/juicefs/cmd/gc.go#L175-L195)）。

#### 6.1.2 整理与提交流程
整理流程实现于 [`pkg/meta/base.go#L2873-L2980`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/base.go#L2873-L2980) 与 [`pkg/vfs/compact.go#L54-L110`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/compact.go#L54-L110)：
1. **局部跳过优化（`skipSome`）**：若头部前几个切片已经足够大且没有被后续覆写覆盖，保持其不动，仅整理后续碎片区间（[`pkg/meta/slice.go#L183-L209`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go#L183-L209)）。
2. **数据实体合并（`vfs.Compact`）**：客户端向元数据申请新的 Slice ID，顺序读取旧切片的有效字节，重新组装为完整的 4 MiB Block，并上传作为全新的 Slice 对象。
3. **乐观并发 CAS 替换（`doCompactChunk`）**：
   在 [`pkg/meta/sql.go#L4043-L4081`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L4043-L4081) 中：
   ```go
   _, err := s.ForUpdate().MustCols("indx").Get(&c2)
   if len(c2.Slices) < len(origin) || !bytes.Equal(origin, c2.Slices[:len(origin)]) {
       return syscall.EINVAL // 在合并期间该 Chunk 发生非前缀写或被截断，合并作废
   }
   // 关键：保留在合并执行期间并发写入的新 Slice（c2.Slices[len(origin):]）
   c2.Slices = append(append(c2.Slices[:skipped*sliceBytes], marshalSlice(pos, id, size, 0, size)...), c2.Slices[len(origin):]...)
   ```
   - 若在客户端执行 `Compact` 期间，有其他客户端向该 Chunk 尾部追加了数据，`c2.Slices[:len(origin)]` 依然严格匹配 `origin`！
   - 系统安全地将新合并的 Slice 替换掉旧片段，同时**保留并发写入的新切片**。
   - 若 CAS 失败返回 `EINVAL`，客户端将放弃提交并立即删除刚上传的合并 Slice 对象（[`pkg/meta/base.go#L2964-L2967`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/base.go#L2964-L2967)）。

### 6.2 垃圾回收（GC）与防误删机制

分布式数据层中，对象存储的物理文件删除必须防范两类风险：并发写入中的悬空对象被误删、以及回收已被复用的对象。

#### 6.2.1 引用计数与延迟回收表（delslices）
- **引用计数表（`chunk_ref`）**：跟踪每个 Slice 的被引用次数（支持克隆、硬链接等）。当 Compaction 替换或文件被删除时，执行 `update chunk_ref set refs = refs - 1 where chunkid = ?`（[`pkg/meta/sql.go#L4074`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L4074)）。
- **回收站（Trash）延迟删除**：若启用了回收站功能（`TrashDays > 0`），旧切片不直接删除，而是序列化插入 `delslices` 表（[`pkg/meta/sql.go#L4065`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L4065)），等待超时后由后台任务清理。

#### 6.2.2 离线 GC 的三态比对与时间隔离窗（Grace Period）
在独立 GC 扫描时（[`cmd/gc.go#L238-L340`](file:///Users/fanxing/code/other/filesystem/juicefs/cmd/gc.go#L238-L340)）：
1. **三态切片扫描（`ScanSlices`）**：
   - `vkeys`：当前存活在 `chunk` 表中的有效 Slice。
   - `pkeys`：悬挂待清理切片（`refs <= 0` 的 `sliceRef`）。
   - `ckeys`：延迟删除切片（`delslices` 表中的切片）。
2. **物理对象比对与时间隔离窗（`maxMtime`）**：
   - 遍历对象存储上的所有 Block。
   - **时间保护窗机制**（[`cmd/gc.go#L122, L308`](file:///Users/fanxing/code/other/filesystem/juicefs/cmd/gc.go#L122)）：`maxMtime = time.Now().Add(-time.Hour)`。
   - 若发现一个对象既不在 `vkeys` 也不在 `pkeys` / `ckeys` 中，但在对象存储上的修改时间（`mtime`）距今不足 1 小时，**必须跳过，严禁删除**！
   - **原因**：客户端正在执行大文件的分块上传。客户端可能已经向对象存储写入了前 10 个 Block，但整个 Slice 尚未完成上传，尚未调用 `CommitWrite` 将 Slice 写入元数据引擎。如果 GC 盲目清理元数据中未登记的对象，就会直接破坏并发写入中的文件数据。

---

## 7. 对 Bowl 的架构启发与取舍

Bowl 采用与 JuiceFS 不同的物理基础设施：**PostgreSQL 承载强一致性 POSIX 元数据，Chunk 直落本地 NVMe SSD（单机 / 统一阵列）**。

下表对比了 JuiceFS 针对高延迟对象存储的设计与 Bowl 针对本地 NVMe SSD 的架构权衡：

| 机制维度 | JuiceFS 的设计（适配高延迟对象存储 S3） | Bowl 的物理现实（直落本地 NVMe SSD） | Bowl 的架构决策与取舍 |
| :--- | :--- | :--- | :--- |
| **分块粒度** | Chunk(64M) $\to$ Slice(动态) $\to$ Block(4M) | NVMe 原生支持 4KB 扇区对齐、微秒级延迟 | **精简为两层**：保留 64M 逻辑 Chunk，舍弃 4M Block 与 Slice 分裂 |
| **覆盖写模型** | Append-only Slice 叠加（解决 S3 不可覆写问题） | 本地文件系统支持原地 `pwrite` 与预分配 | **采用原地覆写 / Extent 映射**，消灭复杂的区间二叉树裁切 |
| **读路径** | `buildSlice` 二叉树动态合并 + 多协程 Range 聚合 | NVMe 随机读取延迟极低（10~20 $\mu s$） | **直接读取本地文件或零拷贝直接 I/O** |
| **稀疏打洞** | 往元数据追加 `id=0` 的空洞切片，不改物理对象 | Linux 支持原生 `fallocate(FALLOC_FL_PUNCH_HOLE)` | **元数据与本地文件系统同时原子 Punch Hole**，物理归还空间 |
| **碎片整理** | 复杂的异步/同步 Compaction 网络合并流 | 无多切片碎片堆积问题 | **完全废除 Compaction 模块**，消除读写放大与后台 CPU 开销 |
| **内存缓冲** | 多级分权缓冲 + Staging 磁盘回写目录 | NVMe 带宽与内存极为接近 | **精简数据路径**，依靠内核 PageCache 或小尺寸直接 I/O 环形缓冲 |
| **GC 回收** | 需扫描全量 S3 对象比对元数据，需 1 小时安全窗口 | 本地文件系统文件删除为原子 POSIX 操作 | **随 Inode/Chunk 解链即时物理 unlink**，无需长时间离线扫描 |

### 7.1 值得 Bowl 直接借鉴的机制

1. **64 MiB Chunk 宏观分块设计**：
   - 将大文件以 64 MiB 切片作为元数据与物理调度的基本单元。对于单机多盘或未来分布式磁盘池，以 Chunk 为粒度在不同 NVMe 驱动器之间执行条带化（Striping）或均衡负载，不仅避免单盘单文件 inode 性能瓶颈，还能实现无锁并行 I/O。
2. **两级目录散列分片（Directory Sharding）**：
   - 本地 NVMe 文件系统（如 ext4 / xfs）若在一个目录下存放数百万个 Chunk 文件，会导致目录项锁定竞争与遍历性能断崖式下降。JuiceFS 的两级分片策略（如 `<disk_uuid>/chunks/%02X/%04X/<chunk_id>`）可直接沿用，保证单目录下文件数不超过数千个。
3. **分级内存池设计（Power-of-2 Allocator）**：
   - [`pkg/utils/alloc.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/utils/alloc.go) 中的 34 级 `sync.Pool` 堆外/堆内混合分配器设计非常成熟。在 Go 编写的高吞吐存储服务中，针对 I/O 缓冲区使用分级内存池可彻底避免缓冲区频繁触发 Go runtime GC STW。
4. **定长二进制元数据打包（Binary Packing）**：
   - 在 PostgreSQL 中存储 Chunk 相关的块分配信息时，避免为每 4KB 扇区存储一行数据库记录，借鉴定长二进制（如 `bytea`）进行紧凑位图或 Extent 存储，能大幅压缩元数据索引体积并提升 `lookup` / `read` 效率。

### 7.2 在 Bowl 中应当舍弃或大幅简化的设计

1. **废弃 Slice 概念与 `buildSlice` 裁切算法**：
   - JuiceFS 引入 Slice 与区间二叉树剪切树（[`pkg/meta/slice.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go)）的**唯一根因是对象存储（S3）不支持 In-place Overwrite**。
   - 在 NVMe SSD 上，文件系统原生支持字节粒度修改。Bowl 可以直接将每个 64 MiB Chunk 映射为一个本地稀疏文件（Sparse File）或预分配文件，覆盖写直接定位到 `offset % ChunkSize` 执行 `pwrite`。无需多版本切片堆叠，彻底消除了写放大与读取合并开销。
2. **废弃 Compaction（碎片整理）机制**：
   - 去除了多 Slice 追加写后，数据自然就地更新，Chunk 在物理上始终是单层连续的（或由底层文件系统 Extent 自动管理），因此无需任何形式的后台 Compaction 协程（[`pkg/vfs/compact.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/compact.go)），避免了读取旧数据、重写新数据造成的 SSD 寿命磨损与写入放大（WAF）。
3. **重构 Punch Hole 与物理容量核算**：
   - JuiceFS 的配额按逻辑长度算，且打洞不减配额（[`pkg/meta/sql.go#L1791`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go#L1791)）。
   - 在商业化本地磁盘管理（Bowl）中，用户需要真实的物理打洞释放空间。Bowl 应在 PostgreSQL 事务内记录实际 Block 释放，并直接在底层 NVMe 文件上执行系统调用 `fallocate(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, off, len)`，使底层 SSD 控制器接收 TRIM/UNMAP 指令，真正释放物理闪存块并调减磁盘配额账本。
4. **废弃基于时间的复杂离线 GC 扫描**：
   - 本地 NVMe SSD 的 Chunk 文件由单机文件系统或存储引擎直接管理。当元数据中的 Inode 解链且句柄全部释放（Orphan Inode 回收）时，可直接在事务提交后由本地 Worker 调用 `unlink` 物理删除对应 Chunk 文件，无需启动耗时数小时的对象比对与 1 小时保守隔离等待。

---

## 8. 调研结论与关键文件索引

| 模块职责 | 核心源文件路径 | 关键符号 / 函数 | 核心机制与特征 |
| :--- | :--- | :--- | :--- |
| **切分模型** | [`pkg/meta/interface.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/interface.go) | `ChunkBits`, `ChunkSize`, `Slice` | 64MB Chunk，动态 Slice，4MB Block |
| **切片计算** | [`pkg/meta/slice.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/slice.go) | `marshalSlice`, `buildSlice`, `compactChunk` | 24 字节定长二进制编码，二叉树区间剪切算法 |
| **SQL 存储** | [`pkg/meta/sql.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/sql.go) | `chunk`, `upsertSlice`, `doTruncate`, `doFallocate` | PostgreSQL `ON CONFLICT DO UPDATE SET slices = chunk.slices \|\| ?` |
| **写路径** | [`pkg/vfs/writer.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/writer.go) | `fileWriter`, `chunkWriter`, `findWritableSlice`, `commitThread` | 顺序追加复用 Slice，随机写生成新 Slice，后台异步批量提交 |
| **读路径** | [`pkg/vfs/reader.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/reader.go) | `dataReader.Read`, `readSlice`, `readManySlices` | 空洞直接内存清零，多 Slice 并发读取与用户缓冲填充 |
| **本地缓存** | [`pkg/chunk/disk_cache.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache.go) | `diskCache`, `stage`, `flushPage`, `cleanupFull` | `raw` 与 `rawstaging`，硬链接晋升，负数尺寸标记，冷却时间戳 |
| **数据校验** | [`pkg/chunk/disk_cache_file.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/disk_cache_file.go) | `checksum`, `openCacheFile`, `stageFooter` | 32KB 分段 CRC32C，4MB 附带 512B 校验尾，MsgPack Footer |
| **内存池** | [`pkg/chunk/page.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/chunk/page.go), [`pkg/utils/alloc.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/utils/alloc.go) | `Page`, `NewOffPage`, `Alloc0`, `Free0`, `pools` | 引用计数 Page，34 个 2 的幂次分级 `sync.Pool` |
| **碎片整理** | [`pkg/vfs/compact.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/vfs/compact.go), [`pkg/meta/base.go`](file:///Users/fanxing/code/other/filesystem/juicefs/pkg/meta/base.go) | `Compact`, `baseMeta.compactChunk`, `doCompactChunk` | 读/写碎片触发，新 Slice 上传，数据库乐观前缀 CAS 提交 |
| **离线回收** | [`cmd/gc.go`](file:///Users/fanxing/code/other/filesystem/juicefs/cmd/gc.go) | `gc`, `filterGcObject`, `ScanSlices` | 三态切片全量比对，1 小时 `mtime` 安全隔离窗，防并发误删 |
