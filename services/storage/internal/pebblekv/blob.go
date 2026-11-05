package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// BlobSize 是文件数据块的固定上限，与 Linux FUSE MaxWrite (1 MiB) 1:1 对齐。
const BlobSize = 1 << 20

// blobShardCount 与 Path 的分片规则 (id & 0xff) 对齐。
const blobShardCount = 256

// 热块缓存参数：16 分片消除全局互斥，每分片 64 槽位 + 4 MiB 字节预算。
// 总预算 64 MiB 足以驻留 400 × 128 KiB = 50 MiB 的小文件块与 16 × 1 MiB 的大文件块，
// 同时避免 1024 × 1 MiB = 1 GiB 的最坏内存占用。
const (
	// hotBlobShards 是热块缓存分片数，按 blob_id % hotBlobShards 选择分片。
	hotBlobShards = 16
	// hotBlobSlotsPerShard 是每分片槽位数（16 × 64 = 1024 个块上限）。
	hotBlobSlotsPerShard = 64
	// hotBlobByteBudget 是单个 BlobStore 的热块内存上限。
	hotBlobByteBudget = 64 << 20
	// hotBlobShardBudget 是每分片字节上限，16 × 4 MiB = 64 MiB。
	hotBlobShardBudget = hotBlobByteBudget / hotBlobShards
)

// hotBlob 是热块缓存条目；data 一经写入不再原地修改，仅整体替换或剔除。
type hotBlob struct {
	id   uint64
	data []byte
}

// hotShard 是热块缓存的单个分片，独立加锁与独立字节预算。
// slots 作为环形队列使用：head 指向最旧槽位，count 是已占用槽位数。
// 用 head+count 而不是单个 next 游标，避免队列尚未回绕时把空槽误判为最旧项。
type hotShard struct {
	mu    sync.Mutex
	slots [hotBlobSlotsPerShard]hotBlob
	head  int
	count int
	bytes int
}

// BlobRef 是 Pebble DataKey 中保存的定长数据块引用。
type BlobRef struct {
	ID     uint64
	Length uint32
}

// EncodeBlobRef 编码 12 字节 BlobRef：[blob_id(8B BigEndian) | length(4B BigEndian)]。
func EncodeBlobRef(ref BlobRef) []byte {
	b := make([]byte, 12)
	binary.BigEndian.PutUint64(b[:8], ref.ID)
	binary.BigEndian.PutUint32(b[8:], ref.Length)
	return b
}

// DecodeBlobRef 解码并校验 12 字节 BlobRef。
func DecodeBlobRef(b []byte) (BlobRef, error) {
	if len(b) != 12 {
		return BlobRef{}, ErrInvalidValue
	}
	ref := BlobRef{ID: binary.BigEndian.Uint64(b[:8]), Length: binary.BigEndian.Uint32(b[8:])}
	if ref.ID == 0 || ref.Length == 0 || ref.Length > BlobSize {
		return BlobRef{}, ErrInvalidValue
	}
	return ref, nil
}

// BlobStore 管理一个分支或快照目录下不可变的 blobs/<shard>/<blob_id>.blob 文件。
type BlobStore struct {
	dir string
	// link 是硬链接实现，测试可注入 EMLINK 等失败以验证自动回退拷贝。
	link func(string, string) error

	mu        sync.Mutex
	dirtyFile map[uint64]struct{}
	dirtyDir  map[string]struct{}
	// dirtyNew 记录由 write() 新建、尚未完成目录屏障的块文件。AppendTail/Patch 原地覆写
	// 不创建目录项，因此目录屏障只从 dirtyNew 与显式 dirtyDir 推导。
	dirtyNew map[uint64]struct{}
	// dirtyIno 记录窗口内脏块所属的 Inode；inoBlobs 是 ino -> 脏块集合的反向索引，
	// 供 Fsync(ino) 只刷该文件的块，避免把其它并发写入文件的脏块一起拖进屏障。
	dirtyIno map[uint64]uint64
	inoBlobs map[uint64]map[uint64]struct{}
	// shardReady 记录已创建过的分片目录，避免每次写入都发起 MkdirAll。
	shardReady [blobShardCount]atomic.Bool
	// hot 按 id % hotBlobShards 分片，命中时省去 open+pread+close，且分片间互不争锁。
	hot [hotBlobShards]hotShard
	// readOps 累计块文件读取次数，用于验证 Diff / Merge 的 BlobRef 短路。
	readOps atomic.Int64
	// fileSyncs / dirSyncs 累计 Sync 实际发出的块文件与分片目录 fsync 次数，
	// 用于回归测试锁定“过渡块不产生目录屏障”的契约。
	fileSyncs atomic.Int64
	dirSyncs  atomic.Int64
	// writeHook 仅用于测试观测块写入阶段是否已脱离 Inode 锁。
	writeHook func()
}

// NewBlobStore 返回以 base 目录（分支或快照目录）为根的块文件存储。
func NewBlobStore(base string) *BlobStore {
	return newBlobStore(base, os.Link)
}

func newBlobStore(base string, link func(string, string) error) *BlobStore {
	if link == nil {
		link = os.Link
	}
	return &BlobStore{
		dir:       filepath.Join(base, "blobs"),
		link:      link,
		dirtyFile: make(map[uint64]struct{}),
		dirtyDir:  make(map[string]struct{}),
		dirtyNew:  make(map[uint64]struct{}),
		dirtyIno:  make(map[uint64]uint64),
		inoBlobs:  make(map[uint64]map[uint64]struct{}),
	}
}

// ReadOps 返回累计块文件读取次数。
func (s *BlobStore) ReadOps() int64 { return s.readOps.Load() }

// SetWriteHook 设置块写入阶段的测试观测钩子，必须在并发写入前调用。
func (s *BlobStore) SetWriteHook(fn func()) { s.writeHook = fn }

// Root 返回 blobs 目录路径。
func (s *BlobStore) Root() string { return s.dir }

// Path 返回指定编号块文件的绝对路径。
func (s *BlobStore) Path(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%02x", id&0xff), fmt.Sprintf("%016x.blob", id))
}

// Write 将不可变块写入 shard 目录。块编号全局单调递增，在 Pebble 批次提交前
// 没有任何引用指向该文件，因此直接覆盖写入即可，无需临时文件加重命名。
func (s *BlobStore) Write(id uint64, data []byte) error {
	return s.write(id, 0, data)
}

// WriteForInode 写入块并登记其所属 Inode，使 Fsync(ino) 只同步该文件的脏块。
func (s *BlobStore) WriteForInode(ino, id uint64, data []byte) error {
	return s.write(id, ino, data)
}

func (s *BlobStore) write(id, ino uint64, data []byte) error {
	if id == 0 || len(data) == 0 || len(data) > BlobSize {
		return ErrInvalidValue
	}
	if s.writeHook != nil {
		s.writeHook()
	}
	path := s.Path(id)
	dir := filepath.Dir(path)
	if !s.shardReady[id&0xff].Load() {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		s.shardReady[id&0xff].Store(true)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.Write(data)
	closeErr := file.Close()
	if err := errors.Join(writeErr, closeErr); err != nil {
		_ = os.Remove(path)
		return err
	}
	if ino != 0 {
		s.markFileInode(id, ino)
	} else {
		s.markFile(id)
	}
	// 新建 .blob 文件需要父目录屏障保证目录项持久化；AppendTail/Patch 原地覆写已有文件
	// 不创建目录项，绝不能把 shardDir 标脏（每次 fsync 会多一次 ext4 目录日志提交）。
	s.markNewFile(id)
	// 直接收养调用方缓冲区，不做整块拷贝：块内容一经写入即不可变，
	// 而所有调用方在 Write 之后都不再改写该切片（整块写是请求体子切片，局部写是刚构造的新块）。
	// 这样 16 MiB 顺序写不再额外产生 16 MiB 堆垃圾，4 KiB 热读仍 100% 命中内存。
	s.cacheHotOwned(id, data)
	return nil
}

// Read 读取完整块内容。
func (s *BlobStore) Read(id uint64, length int) ([]byte, error) {
	if length <= 0 || length > BlobSize {
		return nil, ErrInvalidValue
	}
	data := make([]byte, length)
	if err := s.ReadAt(id, 0, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ReadAt 按块内偏移读取指定区间。
func (s *BlobStore) ReadAt(id uint64, offset int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	// 先记账再查热缓存，保持 ReadOps() 的观测契约。
	s.readOps.Add(1)
	if data, ok := s.hotBlock(id); ok && offset >= 0 && offset+int64(len(dst)) <= int64(len(data)) {
		copy(dst, data[offset:offset+int64(len(dst))])
		return nil
	}
	file, err := os.Open(s.Path(id))
	if err != nil {
		return err
	}
	defer file.Close()
	n, err := file.ReadAt(dst, offset)
	if errors.Is(err, io.EOF) {
		if n == len(dst) {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

// Exists 报告块文件是否存在。
func (s *BlobStore) Exists(id uint64) bool {
	_, err := os.Stat(s.Path(id))
	return err == nil
}

// Remove 立即删除块文件；不存在视为已回收。
func (s *BlobStore) Remove(id uint64) error {
	path := s.Path(id)
	err := os.Remove(path)
	// 无论文件是否已不存在，都必须剔除热缓存与脏标记：
	// 块已被逻辑删除，不能继续读到旧内容，Sync 也不应再对已删除块发起 open。
	s.evictHot(id)
	s.mu.Lock()
	_, transient := s.dirtyFile[id]
	delete(s.dirtyFile, id)
	delete(s.dirtyNew, id)
	if ino, ok := s.dirtyIno[id]; ok {
		delete(s.dirtyIno, id)
		if set := s.inoBlobs[ino]; set != nil {
			delete(set, id)
			if len(set) == 0 {
				delete(s.inoBlobs, ino)
			}
		}
	}
	s.mu.Unlock()
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	// 窗口内新建、尚未被任何 Sync 持久化的过渡块，其目录项从未落盘，
	// 删除它不需要目录屏障；只有删除“已持久化”的块才必须把 unlink 也刷下去。
	if !transient {
		s.markDir(filepath.Dir(path))
	}
	return nil
}

// WindowLocal 报告该块是否仍在本次 Sync 窗口内新建、尚未被任何持久化提交引用。
// 窗口内的块可以原地追加：它既没有落盘屏障，也没有被 Commit / Fork 固化成共享副本。
func (s *BlobStore) WindowLocal(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dirtyFile[id]
	return ok
}

// AppendTail 在窗口内块按逻辑偏移 offset 覆写 chunk，并把热缓存替换为写入后的完整内容。
// 使用 WriteAt + Truncate 而非 O_APPEND：上一次未提交/失败批次可能在块尾留下残留字节，
// 按逻辑偏移覆写并把文件截断到完整逻辑长度才能保证块内容精确、不错位。
// full 必须是写入后的完整块（长度与块文件最终大小一致），供后续局部写 CoW 与热读直接命中内存。
//
// 截断目标取 len(full) 而不是 offset+len(chunk)：尾部追加时两者相同（覆盖残留字节），
// 块内原地覆写时 len(full) 保留完整的逻辑块长度，不会把后续字节截掉。
func (s *BlobStore) AppendTail(id uint64, offset int64, chunk, full []byte) error {
	targetSize := int64(len(full))
	if targetSize == 0 {
		targetSize = offset + int64(len(chunk))
	}
	file, err := os.OpenFile(s.Path(id), os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	_, writeErr := file.WriteAt(chunk, offset)
	var truncErr error
	if writeErr == nil {
		truncErr = file.Truncate(targetSize)
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, truncErr, closeErr); err != nil {
		return err
	}
	// 后台 Sync 可能在本块构建与提交之间清掉脏标记，原地覆写后必须重新登记，
	// 否则新写入的字节可能一直等不到下一次屏障。
	s.markFile(id)
	s.cacheHotOwned(id, full)
	return nil
}

// Patch 在窗口内块按逻辑偏移覆写 chunk，并把文件截断到 targetSize（完整逻辑长度）。
// 与 AppendTail 的差别：调用方没有构造完整块缓冲区，因此不更新热缓存，而是剔除
// 已失效的旧缓存项；后续读直接命中块文件（OS Page Cache）。
func (s *BlobStore) Patch(id uint64, offset int64, chunk []byte, targetSize int64) error {
	file, err := os.OpenFile(s.Path(id), os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	var writeErr error
	if len(chunk) > 0 {
		_, writeErr = file.WriteAt(chunk, offset)
	}
	var truncErr error
	if writeErr == nil {
		truncErr = file.Truncate(targetSize)
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, truncErr, closeErr); err != nil {
		return err
	}
	// 后台 Sync 可能在本块构建与提交之间清掉脏标记，原地覆写后必须重新登记，
	// 否则新写入的字节可能一直等不到下一次屏障。
	s.markFile(id)
	// 热缓存里的旧内容已被本次覆写废弃：继续命中会把旧字节读回来。
	s.evictHot(id)
	return nil
}

// FileSyncOps 返回累计块文件 fsync 次数，用于验证 Sync 只刷仍然存活的脏块。
func (s *BlobStore) FileSyncOps() int64 { return s.fileSyncs.Load() }

// DirSyncOps 返回累计分片目录 fsync 次数，用于验证过渡块不残留目录屏障。
func (s *BlobStore) DirSyncOps() int64 { return s.dirSyncs.Load() }

func (s *BlobStore) hotShardFor(id uint64) *hotShard {
	return &s.hot[id%hotBlobShards]
}

// hotBlock 返回指定块的内存副本。写入后的块内容不可变，返回的切片可安全并发读取。
func (s *BlobStore) hotBlock(id uint64) ([]byte, bool) {
	sh := s.hotShardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].id == id && sh.slots[idx].data != nil {
			return sh.slots[idx].data, true
		}
	}
	return nil, false
}

// cacheHotOwned 收养最近写入块的缓冲区（不拷贝），供局部写 CoW 与热读直接命中内存。
// 调用方必须在 Write / AppendTail 之后放弃对 data 的写所有权：块内容一经写入即不可变。
func (s *BlobStore) cacheHotOwned(id uint64, data []byte) {
	if len(data) > hotBlobShardBudget {
		// 单块超过分片预算时不缓存，避免刚插入就被 trimLocked 立即淘汰。
		return
	}
	sh := s.hotShardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].id == id {
			sh.bytes += len(data) - len(sh.slots[idx].data)
			sh.slots[idx].data = data
			sh.trimLocked()
			return
		}
	}
	if sh.count == len(sh.slots) {
		sh.evictOldestLocked()
	}
	idx := (sh.head + sh.count) % len(sh.slots)
	sh.slots[idx] = hotBlob{id: id, data: data}
	sh.count++
	sh.bytes += len(data)
	sh.trimLocked()
}

// trimLocked 从最旧槽位开始淘汰，直到分片字节数回到预算内。
func (sh *hotShard) trimLocked() {
	for sh.bytes > hotBlobShardBudget && sh.count > 0 {
		sh.evictOldestLocked()
	}
	if sh.count == 0 {
		sh.bytes = 0
	}
}

// evictOldestLocked 淘汰环形队列头部的块。
func (sh *hotShard) evictOldestLocked() {
	slot := &sh.slots[sh.head]
	sh.bytes -= len(slot.data)
	*slot = hotBlob{}
	sh.head = (sh.head + 1) % len(sh.slots)
	sh.count--
}

// evictHot 在块被删除后剔除热缓存，避免读到已删除块的旧内容。
func (s *BlobStore) evictHot(id uint64) {
	sh := s.hotShardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.evictIDLocked(id)
}

// evictIDLocked 精确剔除指定块，并把后续槽位前移以维持环形队列连续。
func (sh *hotShard) evictIDLocked(id uint64) {
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].id != id {
			continue
		}
		sh.bytes -= len(sh.slots[idx].data)
		for j := i; j < sh.count-1; j++ {
			from := (sh.head + j + 1) % len(sh.slots)
			to := (sh.head + j) % len(sh.slots)
			sh.slots[to] = sh.slots[from]
		}
		tail := (sh.head + sh.count - 1) % len(sh.slots)
		sh.slots[tail] = hotBlob{}
		sh.count--
		return
	}
}

// markShard 记录分片目录已存在，避免后续写入重复 MkdirAll。
func (s *BlobStore) markShard(name string) {
	if index, err := strconv.ParseUint(name, 16, 8); err == nil {
		s.shardReady[index].Store(true)
	}
}

// markFile 登记一个未关联 Inode 的脏块；同步失败重试与硬链接复制走这条路径。
func (s *BlobStore) markFile(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyFile[id] = struct{}{}
	if ino, ok := s.dirtyIno[id]; ok {
		delete(s.dirtyIno, id)
		if set := s.inoBlobs[ino]; set != nil {
			delete(set, id)
			if len(set) == 0 {
				delete(s.inoBlobs, ino)
			}
		}
	}
}

// markFileInode 登记脏块及其所属 Inode。
func (s *BlobStore) markFileInode(id, ino uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyFile[id] = struct{}{}
	s.dirtyIno[id] = ino
	set := s.inoBlobs[ino]
	if set == nil {
		set = make(map[uint64]struct{})
		s.inoBlobs[ino] = set
	}
	set[id] = struct{}{}
}

// markNewFile 登记新建块，供 Sync 推导目录屏障。
func (s *BlobStore) markNewFile(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyNew[id] = struct{}{}
}

func (s *BlobStore) markDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyDir[dir] = struct{}{}
}

// HasDirty 报告是否仍有待同步的块文件或分片目录。
func (s *BlobStore) HasDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dirtyFile) > 0 || len(s.dirtyDir) > 0
}

// syncFile 对单个块文件执行 open + fsync + close；文件已被回收时视为已同步。
func (s *BlobStore) syncFile(id uint64) error {
	file, err := os.Open(s.Path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		s.markFile(id)
		return err
	}
	s.fileSyncs.Add(1)
	if err := errors.Join(syncFileData(file), file.Close()); err != nil {
		s.markFile(id)
		return err
	}
	return nil
}

// syncDir 对单个分片目录执行 open + fsync + close。
func (s *BlobStore) syncDir(dir string) error {
	handle, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		s.markDir(dir)
		return err
	}
	s.dirSyncs.Add(1)
	if err := errors.Join(handle.Sync(), handle.Close()); err != nil {
		s.markDir(dir)
		return err
	}
	return nil
}

// syncBatch 同步一批块文件与分片目录。
// 单个对象直接在调用方协程执行（小文件 fsync 不付协程开销），
// 多个对象用有界 worker 并行，把数千个 open+fsync+close 的串行排队压成并发屏障。
func (s *BlobStore) syncBatch(files []uint64, dirs []string) error {
	if len(files) == 0 && len(dirs) == 0 {
		return nil
	}
	var errs []error
	switch {
	case len(files) == 1:
		if err := s.syncFile(files[0]); err != nil {
			errs = append(errs, err)
		}
	case len(files) > 1:
		workers := min(len(files), 16)
		errs = append(errs, runParallel(len(files), workers, func(i int) error { return s.syncFile(files[i]) })...)
	}
	switch {
	case len(dirs) == 1:
		if err := s.syncDir(dirs[0]); err != nil {
			errs = append(errs, err)
		}
	case len(dirs) > 1:
		workers := min(len(dirs), 8)
		errs = append(errs, runParallel(len(dirs), workers, func(i int) error { return s.syncDir(dirs[i]) })...)
	}
	return errors.Join(errs...)
}

// runParallel 用固定数量 worker 处理 [0,n) 下标区间，返回全部错误。
func runParallel(n, workers int, fn func(int) error) []error {
	var idx atomic.Int64
	errCh := make(chan error, n)
	var wg sync.WaitGroup
	wg.Add(workers)
	for range workers {
		go func() {
			defer wg.Done()
			for {
				i := int(idx.Add(1)) - 1
				if i >= n {
					return
				}
				if err := fn(i); err != nil {
					errCh <- err
				}
			}
		}()
	}
	wg.Wait()
	close(errCh)
	errs := make([]error, 0, n)
	for err := range errCh {
		errs = append(errs, err)
	}
	return errs
}

// Sync 将近期写入或删除的块文件与 shard 目录刷入磁盘。
func (s *BlobStore) Sync() error {
	s.mu.Lock()
	if len(s.dirtyFile) == 0 && len(s.dirtyDir) == 0 {
		s.mu.Unlock()
		return nil
	}
	files := make([]uint64, 0, len(s.dirtyFile))
	for id := range s.dirtyFile {
		files = append(files, id)
	}
	// 只为“确实存在存活脏文件”的分片目录发目录屏障：一次事务里被 CoW 反复替换的
	// 中间过渡块已经消失，它们的目录项从未落盘，fsync 它们纯属浪费。
	// 单次 SQLite WAL 事务的分片目录 fsync 因此从 41 次收敛到 1 次。
	// 目录屏障只来自“新建 / 删除块文件”的显式登记：AppendTail/Patch 原地覆写不产生
	// 目录项变更，若按脏文件反推父目录会为每次文件 fsync 多付一次目录 fsync。
	dirSet := make(map[string]struct{}, len(s.dirtyDir))
	for dir := range s.dirtyDir {
		dirSet[dir] = struct{}{}
	}
	for _, id := range files {
		if _, ok := s.dirtyNew[id]; ok {
			dirSet[filepath.Dir(s.Path(id))] = struct{}{}
			delete(s.dirtyNew, id)
		}
	}
	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	clear(s.dirtyFile)
	clear(s.dirtyDir)
	clear(s.dirtyIno)
	clear(s.inoBlobs)
	s.mu.Unlock()

	return s.syncBatch(files, dirs)
}

// SyncInode 只同步指定 Inode 的脏块；其它文件的脏块保留给各自 fsync 或后台兜底刷盘。
// 未登记 Inode 的脏块（旧调用方写入或上次同步失败重试）会跟随本次屏障一起落盘，保证不饿死。
func (s *BlobStore) SyncInode(ino uint64) error {
	s.mu.Lock()
	var files []uint64
	if ids := s.inoBlobs[ino]; len(ids) > 0 {
		files = make([]uint64, 0, len(ids))
		for id := range ids {
			files = append(files, id)
		}
	}
	if len(s.dirtyFile) > len(s.dirtyIno) {
		for id := range s.dirtyFile {
			if _, ok := s.dirtyIno[id]; !ok {
				files = append(files, id)
			}
		}
	}
	if len(files) == 0 && len(s.dirtyDir) == 0 {
		s.mu.Unlock()
		return nil
	}
	dirSet := make(map[string]struct{}, len(s.dirtyDir))
	for dir := range s.dirtyDir {
		dirSet[dir] = struct{}{}
	}
	for _, id := range files {
		if _, ok := s.dirtyNew[id]; ok {
			dirSet[filepath.Dir(s.Path(id))] = struct{}{}
			delete(s.dirtyNew, id)
		}
		delete(s.dirtyFile, id)
		if owner, ok := s.dirtyIno[id]; ok {
			delete(s.dirtyIno, id)
			if set := s.inoBlobs[owner]; set != nil {
				delete(set, id)
				if len(set) == 0 {
					delete(s.inoBlobs, owner)
				}
			}
		}
	}
	delete(s.inoBlobs, ino)
	clear(s.dirtyDir)
	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	s.mu.Unlock()

	return s.syncBatch(files, dirs)
}

// LinkAllTo 通过硬链接把本目录下所有块共享给目标快照或分支目录，遇硬链接上限时回退复制。
func (s *BlobStore) LinkAllTo(dst *BlobStore) error {
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		shard := filepath.Join(s.dir, entry.Name())
		files, err := os.ReadDir(shard)
		if err != nil {
			return err
		}
		targetDir := filepath.Join(dst.dir, entry.Name())
		if err := os.MkdirAll(targetDir, 0o755); err != nil {
			return err
		}
		dst.markShard(entry.Name())
		for _, file := range files {
			if file.IsDir() || !strings.HasSuffix(file.Name(), ".blob") {
				continue
			}
			from := filepath.Join(shard, file.Name())
			to := filepath.Join(targetDir, file.Name())
			if _, err := os.Stat(to); err == nil {
				continue
			} else if !errors.Is(err, os.ErrNotExist) {
				return err
			}
			if err := s.link(from, to); err != nil {
				// 达到宿主文件系统硬链接上限（EMLINK）等情况回退为流式拷贝，
				// 生成新的物理 Inode 以重新获得独立硬链接配额。
				if copyErr := copyBlobFile(from, to); copyErr != nil {
					return errors.Join(err, copyErr)
				}
				if id, parseErr := parseBlobID(file.Name()); parseErr == nil {
					dst.markFile(id)
				}
			}
			dst.markDir(targetDir)
		}
	}
	return dst.Sync()
}

func copyBlobFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	return errors.Join(copyErr, out.Close())
}

// parseBlobID 从 <blob_id>.blob 文件名解析块编号。
func parseBlobID(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".blob"), 16, 64)
}
