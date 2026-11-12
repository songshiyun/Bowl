# 0019. 文件数据分三层（Chunk-Slice-Block）存储、数据库合并切片与本地 NVMe 落盘设计

## 状态
已被 [ADR-0031](0031-shardstore-append-only-pack-storage-and-256kib-block-grid.md) 替代 (superseded by ADR-0031) — 三层 `Chunk-Slice-Block` 与 PostgreSQL 切片合并已被 `256 KiB` 逻辑 `Blob` 网格与 `64 MiB` Append-Only `.pack` 封包容器取代

## 背景
在完成 `StorageService` 的 36 个 POSIX 元数据接口与 `Disk` 容量配额管理（ADR-0001 至 ADR-0018）后，系统需要实现真正的文件数据读写功能（`Read` 与 `Write`）。在设计数据存储结构时，需要兼顾以下四个要求：
1. **既要利用本地 NVMe SSD 快速读写，又要方便以后把冷数据同步到对象存储（S3）**：数据需要先写入本地 NVMe SSD 以保证低延迟，以后还要支持在后台把数据异步上传到 S3，并把本地 NVMe 当作热数据缓存。如果直接在本地 NVMe 上用一个大文件原地覆盖修改（In-place Overwrite），以后同步到不支持原地修改的 S3 时，每次改几个字节都要把整个大块重新上传一遍，而且并发读写时还容易读到写了一半的数据。
2. **为以后的快照和克隆（Snapshot & Clone）打好基础**：系统后续计划支持文件和整块 `Disk` 的快照及零拷贝克隆。这就要求文件的逻辑位置 `(ino, chunk_index)` 和底层硬盘上的物理数据块分开：每次写入都生成新的不可变数据块（Immutable Block），已经写好的物理块不再原地修改，而是通过引用计数在多个快照或克隆文件之间共享。
3. **数据切块逻辑放在服务端还是客户端**：需要决定是让客户端自己去切分数据块并直接写底层存储，还是让客户端只管发读写请求，把切块、写盘、合并切片和垃圾回收全部放在 `StorageService` 服务端内部完成。
4. **追加写切片时如何统计磁盘配额（`Disk.used_bytes`）并与截断、删除等操作配合**：每次修改文件都追加新切片（`Slice`）的话，在后台 `Compaction` 把被覆盖的旧切片清理掉之前，硬盘上会暂时多占一些物理空间；同时，像 `SetAttr`（截断文件）、`Open(O_TRUNC)`、`Fallocate`（打洞）、`Unlink`、`Rename`、`Release`、`RenewSession` 和 `DeleteDisk` 这些操作，都需要同步清理或标记对应的数据切片，并且不能因为在数据库事务里直接删硬盘文件而卡住其他请求。

## 决策
1. **由 `StorageService` 统一提供 `Read` / `Write` 接口，单次读写最大 `4 MiB`**：
   - 在 `proto/storage/v1/storage.proto` 的 `StorageService` 中新增 `Read` 和 `Write` 接口（对应 Linux FUSE 的 `FUSE_READ` 和 `FUSE_WRITE`），单次请求最多读写 `4 MiB`（`4,194,304` 字节，刚好对应一个物理 `Block` 的最大长度）。
   - FUSE 客户端只负责把 `(ino, fh, offset, size/data, session_id)` 转发给服务端；数据怎么切块、怎么写到本地 NVMe 硬盘、怎么在 PostgreSQL 里记录切片、怎么在后台合并碎片（`Compaction`）和删除无用块文件（GC），全部由 `StorageService` 内部负责。
   - **读写权限检查与属性更新**：`Read` 要求目标是常规文件且打开句柄不是只写模式 `O_WRONLY`（否则返回 `EBADF`）；`Write` 要求目标是常规文件且打开句柄是 `O_WRONLY` 或 `O_RDWR`（否则返回 `EBADF`）。如果打开文件时带了 `O_APPEND` 追加标志，服务端在数据库事务里锁住 `Inode` 后，强制从当前最新的文件末尾 `inode.Size` 开始写入；只要写入字节数大于 0，就自动更新文件的 `mtime` 和 `ctime`，并清除常规文件的 `S_ISUID` 和 `S_ISGID` 特权位。
2. **文件数据按 `Chunk (64 MiB) -> Slice (24 字节记录) -> Block (<= 4 MiB 文件)` 三层管理**：
   - **Chunk（每 64 MiB 一个逻辑分块）**：在 PostgreSQL 中新建 `chunks(ino, indx, slices)` 表（主键为 `(ino, indx)`，其中 `indx = offset / 64MiB` 使用 64 位 `BIGINT`，支持最大 `MaxInt64` 即 8 EiB 的稀疏文件偏移量）。
   - **Slice（每次写入生成一条 24 字节的切片记录）**：每次连续写入都分配一个全局递增的 `slice_id`（`slice_id = 0` 专门用来表示全部为 0 的文件空洞 Hole）。在 `chunks.slices`（`BYTEA` 二进制列）末尾按写入先后顺序追加一条 24 字节的大端序定长记录：`[pos:4B][id:8B][size:4B][off:4B][len:4B]`。
   - **Block（最大 4 MiB 的物理数据块文件）**：每次写入的 `Slice` 数据按最大 `4 MiB` 切分成一个或多个不可修改的 `Block` 文件，通过 `BlockStore` 接口保存在本地 NVMe SSD 的两级十六进制目录（`chunks/%02X/%v/<slice_id>_<block_idx>_<block_size>`）下，先写成同目录临时文件，再通过 `rename` 原子替换成正式文件名。
   - **读取时按先后顺序合并切片（`buildSlice`）**：执行 `Read` 时，从 PostgreSQL 读出目标 `(ino, indx)` 的 `slices` 二进制序列，在内存里按“后写的覆盖先写的”规则把重叠区间裁剪合并成一段段互不重叠的有效数据区间；遇到空洞区间（`id == 0` 或从未写过的空白区域）直接在内存里填 `0` 返回，遇到有数据的区间则从 `BlockStore` 读取对应的 `Block` 文件内容拼装返回。
3. **建立 `slice_refs` 引用计数表，方便后续支持 S3 分层与文件克隆**：
   - 在 PostgreSQL 中建立 `slice_refs(slice_id, size, refs)` 引用计数表。
   - 每次新写入一个 `Slice`，都在数据库事务里往 `slice_refs` 插入一条 `refs = 1` 的记录；以后要做零拷贝文件克隆时只需要把对应 `Slice` 的 `refs` 加 1，要做 S3 冷热分层时也只需要替换或扩展 `BlockStore` 的实现，不需要推翻现有的数据库表结构。
4. **磁盘已用配额（`Disk.used_bytes`）按文件的逻辑大小 `size` 统计**：
   - 继续按照 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 的规则，只按常规文件的**逻辑大小 `size`** 计算磁盘的已用空间 `Disk.used_bytes`。当 `Write` 使文件变大（`new_size = max(old_size, write_offset + len) > old_size`）时，在同一个事务里检查并增加 `delta_bytes = new_size - old_size` 的配额占用；反复覆盖写同一个位置产生的旧 `Slice` 碎片由后台 `Compaction` 自动回收，不会多扣用户的磁盘配额。
5. **截断、打洞、删除文件与后台碎片合并（Compaction）及垃圾回收（GC）的配合**：
   - **截断文件与打洞**：当 `SetAttr` 把文件改小，或者 `Open(O_TRUNC)` 把文件清空时，在同一个数据库事务里直接删掉超出新文件大小的整块 `Chunk` 行（同时把里面包含的非空洞 `slice_id` 在 `slice_refs` 表里的引用计数 `refs` 减 1），并对截断处切到一半的那个 `Chunk` 或者 `Fallocate(FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE)` 打洞的区间，直接追加一条 `id = 0` 的空洞 `Slice` 记录。
   - **删除文件与删除磁盘**：`Unlink`、`Rename`（覆盖目标文件）、`Release`（回收孤儿文件）、`RenewSession`（回收过期会话的孤儿文件）和 `DeleteDisk`，在数据库事务里删除 `Inode` 时，一并删掉它关联的 `chunks` 记录，并把涉及到的所有 `slice_refs.refs` 减 1。
   - **后台合并碎片（Compaction）与异步删除物理文件（GC）**：当某个 `Chunk` 里累积的 `Slice` 记录超过阈值（默认 64 条）时，后台 `Compaction` 协程会把当前有效的数据读出来写成连续的新 `Slice`，用 CAS 把已合并的旧切片序列替换成新切片，并把旧 `Slice` 的引用计数 `refs` 减 1；后台 GC 协程定期扫描 `slice_refs` 表里 `refs <= 0` 的记录，调用 `BlockStore` 删掉硬盘上的 `Block` 文件并删掉对应的 `slice_refs` 行，从而避免在前台数据库事务里直接删硬盘文件造成卡顿。此外，后台 GC 还会低频扫描硬盘上的块目录：如果发现某个块文件的修改时间已经超过安全宽限期（默认 1 小时，可通过 `WithGCOrphanGracePeriod` 配置），且在 `slice_refs` 表里根本没有登记（例如写完块文件后数据库事务失败回滚留下的残留文件），就自动把它从硬盘上删掉。

## 结果
- **正面效果**：用「64 MiB 分块 + 24 字节切片追加记录 + 最大 4 MiB 不可变块文件」这一套结构，同时解决了本地 NVMe 高速读写、随机覆盖写、无需重写硬盘即可截断/打洞、以及未来对接 S3 和文件克隆的需求；前台事务只改数据库里的切片记录和引用计数，删除大文件时也能瞬间完成。
- **代价与取舍**：如果对同一个位置频繁做小块随机覆盖写，一个 `Chunk` 里会攒下多层 `Slice` 记录，需要靠 `StorageService` 后台的 `Compaction` 和 GC 协程定期合并碎片并删掉废弃的 `Block` 文件。
