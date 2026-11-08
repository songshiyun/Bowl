package store

import (
	"errors"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// BlobStore 规格与分片热块缓存参数（ADR-0031 §1.2 & §3.1）。
const (
	BlobIDSegment      uint64 = 4096
	hotBlobShards             = 16
	hotBlobSlots              = 64
	hotBlobTotalBudget        = 64 << 20                           // 64 MiB
	hotBlobShardBudget        = hotBlobTotalBudget / hotBlobShards // 4 MiB per shard
)

// BlobReserveFunc 向盘级分配器持久化预留一段连续 PackID 编号，返回闭区间 [first, last]。
type BlobReserveFunc func(count uint64) (first uint64, last uint64, err error)

type hotBlob struct {
	packID uint64
	offset uint32
	data   []byte
}

// hotShard 是热块缓存的单个分片，独立加锁与独立 4 MiB 字节预算。
type hotShard struct {
	mu    sync.Mutex
	slots [hotBlobSlots]hotBlob
	head  int
	count int
	bytes int
}

// BlobStore 管理单个分支或快照目录下扁平的 packs/<pack_id_hex16>.pack 封包文件（单包上限 64 MiB）、
// 常驻活跃写句柄与已封口只读句柄池、64 MiB 分片环形热块缓存及脏包屏障追踪表（ADR-0031 §1.2 & §3）。
type BlobStore struct {
	dir         string
	dirReady    atomic.Bool
	maxPackSize atomic.Uint32

	// 1. 无锁 PackID 分配游标（步长 4096 向分配器预留）
	nextPackID     atomic.Uint64
	reservedPackID atomic.Uint64
	allocMu        sync.Mutex
	reservePackID  BlobReserveFunc

	// 2. 分片环形热块缓存（16 分片 × 64 槽位，总预算 64 MiB，按 (PackID, Offset) 定位）
	hot [hotBlobShards]hotShard

	// 3. Per-Inode 轻量分片定序锁（4096 分片 Mutex）
	inodeLocks [4096]sync.Mutex

	// 4. 活跃 .pack 写句柄与脏包追踪表
	mu           sync.Mutex
	closed       bool
	activePackID uint64
	activeFile   *os.File
	activeOffset uint32
	dirtyPacks   map[uint64]struct{}            // 未完成 fdatasync 的脏 packID 集合
	unownedDirty map[uint64]struct{}            // 未绑定特定 ino (ino=0) 的脏 packID 集合
	inoPacks     map[uint64]map[uint64]struct{} // ino -> 未同步脏 packID 集合
	dirtyNew     map[uint64]struct{}            // 新建尚未执行 packs/ 目录 fsync 的 packID 集合
	dirDirty     bool                           // packs/ 目录项发生变更（如 RemovePack）需执行目录 fsync

	// 5. 常驻 .pack 文件句柄表（涵盖活跃包与已封口只读包，0 open/close 并发 ReadAt）
	filesMu   sync.RWMutex
	packFiles map[uint64]*os.File

	syncFileCount atomic.Uint64
	syncDirCount  atomic.Uint64
}

func init() {
	openInitHooks = append(openInitHooks, func(s *StoreActor) error {
		bs := NewBlobStore(s.dir)
		s.blobs = bs
		s.syncInodeFn = bs.SyncInode
		return nil
	})
}

// Blobs 返回当前 StoreActor 绑定的 *BlobStore 实例。
func (s *StoreActor) Blobs() *BlobStore {
	if bs, ok := s.blobs.(*BlobStore); ok {
		return bs
	}
	return nil
}

// SetBlobReserveFunc 为当前 StoreActor 的 BlobStore 注入自定义 PackID 号段预留器。
func (s *StoreActor) SetBlobReserveFunc(fn BlobReserveFunc) {
	if bs := s.Blobs(); bs != nil {
		bs.SetReserveFunc(fn)
	}
}

// NewBlobStore 创建以 base（分支或快照根目录）为根的 BlobStore，并在未注入外部分配器时扫描已有最大 PackID 初始化默认分配器。
func NewBlobStore(base string) *BlobStore {
	return NewBlobStoreWithAllocator(base, nil)
}

// NewBlobStoreWithAllocator 创建指定号段预留分配器的 BlobStore。
func NewBlobStoreWithAllocator(base string, reserve BlobReserveFunc) *BlobStore {
	bs := &BlobStore{
		dir:           filepath.Join(base, "packs"),
		reservePackID: reserve,
		dirtyPacks:    make(map[uint64]struct{}),
		unownedDirty:  make(map[uint64]struct{}),
		inoPacks:      make(map[uint64]map[uint64]struct{}),
		dirtyNew:      make(map[uint64]struct{}),
		packFiles:     make(map[uint64]*os.File),
	}
	bs.maxPackSize.Store(PackMaxSize)
	maxID := bs.scanExistingPacks()
	if bs.reservePackID == nil {
		bs.setupDefaultAllocator(maxID)
	}
	return bs
}

// SetReserveFunc 动态注入外部 PackID 号段预留函数并重置本地预留游标。
func (s *BlobStore) SetReserveFunc(fn BlobReserveFunc) {
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	s.reservePackID = fn
	s.nextPackID.Store(0)
	s.reservedPackID.Store(0)
}

// AdvanceMinPackID 确保后续分配的 PackID 严格大于 minPackID（供 StoreActor 启动恢复 sys:pack 时对齐游标）。
func (s *BlobStore) AdvanceMinPackID(minPackID uint64) {
	if minPackID == 0 {
		return
	}
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	if next := s.nextPackID.Load(); next != 0 && next <= minPackID {
		s.nextPackID.Store(minPackID + 1)
	} else if next == 0 {
		s.setupDefaultAllocator(minPackID)
	}
}

// MaxPackSize 返回单个 .pack 封包文件的封口容量上限（默认 PackMaxSize = 64 MiB）。
func (s *BlobStore) MaxPackSize() uint32 {
	return s.maxPackSize.Load()
}

// SetMaxPackSize 设置单个 .pack 封包文件的封口容量上限（供测试验证跨包自动切包）。
func (s *BlobStore) SetMaxPackSize(size uint32) {
	if size == 0 || size > PackMaxSize {
		size = PackMaxSize
	}
	s.maxPackSize.Store(size)
}

// ActivePackID 返回当前活跃可写 .pack 的编号（0 表示尚无活跃包或已封口）。
func (s *BlobStore) ActivePackID() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activePackID
}

// ActiveOffset 返回当前活跃可写 .pack 的尾部写偏移量。
func (s *BlobStore) ActiveOffset() uint32 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.activeOffset
}

func (s *BlobStore) scanExistingPacks() uint64 {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0
	}
	s.dirReady.Store(true)
	var maxID uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		if id, err := parsePackID(entry.Name()); err == nil && id > maxID {
			maxID = id
		}
	}
	return maxID
}

func (s *BlobStore) setupDefaultAllocator(initialLast uint64) {
	last := initialLast
	var mu sync.Mutex
	s.reservePackID = func(count uint64) (uint64, uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		if count == 0 {
			count = BlobIDSegment
		}
		if last == math.MaxUint64 {
			return 0, 0, ErrNoSpace
		}
		first := last + 1
		var reserved uint64
		if math.MaxUint64-last < count {
			reserved = math.MaxUint64
		} else {
			reserved = last + count
		}
		last = reserved
		return first, reserved, nil
	}
}

// NextPackID 无锁分配下一个唯一 PackID；当本地号段耗尽时加 allocMu 按 4096 步长预留新号段。
func (s *BlobStore) NextPackID() (uint64, error) {
	for {
		next := s.nextPackID.Load()
		reserved := s.reservedPackID.Load()
		if next != 0 && next <= reserved {
			if s.nextPackID.CompareAndSwap(next, next+1) {
				return next, nil
			}
			continue
		}
		s.allocMu.Lock()
		next = s.nextPackID.Load()
		reserved = s.reservedPackID.Load()
		if next != 0 && next <= reserved {
			s.allocMu.Unlock()
			continue
		}
		if s.reservePackID == nil {
			s.setupDefaultAllocator(0)
		}
		first, last, err := s.reservePackID(BlobIDSegment)
		if err != nil {
			s.allocMu.Unlock()
			return 0, err
		}
		if first == 0 || first > last {
			s.allocMu.Unlock()
			return 0, ErrNoSpace
		}
		id := first
		s.reservedPackID.Store(last)
		s.nextPackID.Store(first + 1)
		s.allocMu.Unlock()
		return id, nil
	}
}

// Root 返回 packs 根目录路径（<branch_dir>/packs）。
func (s *BlobStore) Root() string { return s.dir }

// InodeLock 返回指定 Inode 对应的分片互斥锁指针（按 ino & 0xfff 索引）。
func (s *BlobStore) InodeLock(ino uint64) *sync.Mutex {
	return &s.inodeLocks[ino&0xfff]
}

// Path 返回指定编号封包文件的物理路径 <branch_dir>/packs/<pack_id_hex16>.pack。
func (s *BlobStore) Path(packID uint64) string {
	var buf [22]byte // "/0000000000000000.pack"
	const hexDigits = "0123456789abcdef"
	buf[0] = filepath.Separator
	for i := 16; i >= 1; i-- {
		buf[i] = hexDigits[packID&0x0f]
		packID >>= 4
	}
	copy(buf[17:], ".pack")
	return s.dir + string(buf[:])
}

// Append 将有效块数据（1..256 KiB）追加写入当前活跃 .pack：
//   - 若当前活跃包剩余空间不足 MaxPackSize（64 MiB），自动将当前活跃包封口并分配新 PackID 创建下一个活跃 .pack；
//   - 在 activeOffset 处执行 WriteAt，推进 activeOffset，登记 ino 脏包并写入 hotShard 热缓存。
func (s *BlobStore) Append(ino uint64, data []byte) (BlobRef, error) {
	if len(data) == 0 || len(data) > BlobSize {
		return BlobRef{}, ErrInvalidValue
	}
	maxCap := s.maxPackSize.Load()
	if uint32(len(data)) > maxCap {
		return BlobRef{}, ErrInvalidValue
	}

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return BlobRef{}, ErrClosed
	}

	if s.activeFile == nil || uint64(s.activeOffset)+uint64(len(data)) > uint64(maxCap) {
		if err := s.openNextActivePackLocked(); err != nil {
			s.mu.Unlock()
			return BlobRef{}, err
		}
	}

	packID := s.activePackID
	offset := s.activeOffset
	file := s.activeFile

	if _, err := file.WriteAt(data, int64(offset)); err != nil {
		s.mu.Unlock()
		return BlobRef{}, err
	}

	s.activeOffset = offset + uint32(len(data))
	s.markPackDirtyLocked(packID, ino)
	s.mu.Unlock()

	ref := BlobRef{
		PackID: packID,
		Offset: offset,
		Length: uint32(len(data)),
	}
	cached := make([]byte, len(data))
	copy(cached, data)
	s.cacheHotOwned(ref.PackID, ref.Offset, cached)
	return ref, nil
}

// ExtendTail 尝试对位于当前活跃 .pack 最末尾的逻辑块执行原地顺延扩展（ADR-0031 §3.2）：
//   - 仅当 oldRef.PackID == activePackID && oldRef.Offset + oldRef.Length == activeOffset 且扩展后不超过 MaxPackSize 时生效；
//   - 若不满足尾部顺延条件，返回 (BlobRef{}, false, nil)，调用方应回退为读取旧块合并后调用 Append。
func (s *BlobStore) ExtendTail(ino uint64, oldRef BlobRef, patchOffset uint32, patchData []byte, newLength uint32) (BlobRef, bool, error) {
	if oldRef.PackID == 0 || oldRef.Length == 0 || newLength < oldRef.Length || newLength == 0 || newLength > BlobSize {
		return BlobRef{}, false, ErrInvalidValue
	}
	if uint64(patchOffset)+uint64(len(patchData)) > uint64(newLength) {
		return BlobRef{}, false, ErrInvalidValue
	}
	maxCap := s.maxPackSize.Load()

	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return BlobRef{}, false, ErrClosed
	}
	if s.activeFile == nil ||
		oldRef.PackID != s.activePackID ||
		oldRef.Offset+oldRef.Length != s.activeOffset ||
		uint64(oldRef.Offset)+uint64(newLength) > uint64(maxCap) {
		s.mu.Unlock()
		return BlobRef{}, false, nil
	}

	file := s.activeFile
	packID := s.activePackID

	// 处理跨越旧块末尾空洞或尾部新增字节写入
	writeStart := patchOffset
	writePayload := patchData
	if patchOffset > oldRef.Length {
		holeLen := patchOffset - oldRef.Length
		writeStart = oldRef.Length
		writePayload = make([]byte, int(holeLen)+len(patchData))
		copy(writePayload[holeLen:], patchData)
	}
	if uint64(writeStart)+uint64(len(writePayload)) < uint64(newLength) && newLength > oldRef.Length {
		tailZeroStart := max(oldRef.Length, writeStart+uint32(len(writePayload)))
		if len(writePayload) == 0 {
			writeStart = tailZeroStart
			writePayload = make([]byte, newLength-tailZeroStart)
		} else {
			ext := make([]byte, newLength-writeStart)
			copy(ext, writePayload)
			writePayload = ext
		}
	}

	if len(writePayload) > 0 {
		if _, err := file.WriteAt(writePayload, int64(oldRef.Offset)+int64(writeStart)); err != nil {
			s.mu.Unlock()
			return BlobRef{}, false, err
		}
	}

	s.activeOffset = oldRef.Offset + newLength
	s.markPackDirtyLocked(packID, ino)
	s.mu.Unlock()

	newRef := BlobRef{
		PackID: oldRef.PackID,
		Offset: oldRef.Offset,
		Length: newLength,
	}
	s.extendHotShard(oldRef, patchOffset, patchData, newLength)
	return newRef, true, nil
}

func (s *BlobStore) openNextActivePackLocked() error {
	if !s.dirReady.Load() {
		if err := os.MkdirAll(s.dir, 0o755); err != nil {
			return err
		}
		s.dirReady.Store(true)
	}
	packID, err := s.NextPackID()
	if err != nil {
		return err
	}
	path := s.Path(packID)
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}

	s.filesMu.Lock()
	s.packFiles[packID] = file
	s.filesMu.Unlock()

	s.activePackID = packID
	s.activeFile = file
	s.activeOffset = 0
	s.dirtyNew[packID] = struct{}{}
	return nil
}

func (s *BlobStore) markPackDirtyLocked(packID, ino uint64) {
	s.dirtyPacks[packID] = struct{}{}
	if ino == 0 {
		s.unownedDirty[packID] = struct{}{}
		return
	}
	set := s.inoPacks[ino]
	if set == nil {
		set = make(map[uint64]struct{})
		s.inoPacks[ino] = set
	}
	set[packID] = struct{}{}
}

// Read 读取指定 BlobRef 的完整内容。
func (s *BlobStore) Read(ref BlobRef) ([]byte, error) {
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > BlobSize || uint64(ref.Offset)+uint64(ref.Length) > PackMaxSize {
		return nil, ErrInvalidValue
	}
	data := make([]byte, ref.Length)
	if err := s.ReadAt(ref, 0, data); err != nil {
		return nil, err
	}
	return data, nil
}

// ReadAt 按块内相对偏移 offset 读取指定区间 [offset, offset+len(dst))：
//   - 优先从 hotShard 分片锁内直接拷贝（0 系统调用）；
//   - 未命中则直接对常驻 .pack 文件句柄执行单次 ReadAt(dst, int64(ref.Offset)+offset)。
func (s *BlobStore) ReadAt(ref BlobRef, offset int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	if ref.PackID == 0 || ref.Length == 0 || ref.Length > BlobSize || uint64(ref.Offset)+uint64(ref.Length) > PackMaxSize {
		return ErrInvalidValue
	}
	if offset < 0 || uint64(offset)+uint64(len(dst)) > uint64(ref.Length) {
		return io.ErrUnexpectedEOF
	}
	if s.hotReadAt(ref.PackID, ref.Offset, offset, dst) {
		return nil
	}

	file, err := s.getOrOpenPackFile(ref.PackID)
	if err != nil {
		return err
	}
	n, err := file.ReadAt(dst, int64(ref.Offset)+offset)
	if errors.Is(err, io.EOF) {
		if n == len(dst) {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

func (s *BlobStore) getOrOpenPackFile(packID uint64) (*os.File, error) {
	s.filesMu.RLock()
	if f, ok := s.packFiles[packID]; ok && f != nil {
		s.filesMu.RUnlock()
		return f, nil
	}
	s.filesMu.RUnlock()

	s.filesMu.Lock()
	defer s.filesMu.Unlock()
	if f, ok := s.packFiles[packID]; ok && f != nil {
		return f, nil
	}
	f, err := os.Open(s.Path(packID))
	if err != nil {
		return nil, err
	}
	s.packFiles[packID] = f
	return f, nil
}

// hotReadAt 在对应分片锁保护下直接将内存缓存切片的 [offset, offset+len(dst)) 拷贝至 dst。
func (s *BlobStore) hotReadAt(packID uint64, blockOffset uint32, offset int64, dst []byte) bool {
	if offset < 0 {
		return false
	}
	sh := s.hotShardFor(packID, blockOffset)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		slot := &sh.slots[idx]
		if slot.packID == packID && slot.offset == blockOffset && slot.data != nil {
			end := offset + int64(len(dst))
			if end <= int64(len(slot.data)) {
				copy(dst, slot.data[offset:end])
				return true
			}
			return false
		}
	}
	return false
}

// EvictHot 从分片环形热块缓存中剔除指定 BlobRef 对应的块。
func (s *BlobStore) EvictHot(ref BlobRef) {
	sh := s.hotShardFor(ref.PackID, ref.Offset)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.evictBlockLocked(ref.PackID, ref.Offset)
}

func (s *BlobStore) hotShardFor(packID uint64, offset uint32) *hotShard {
	shardIdx := (packID + uint64(offset/BlobSize)) % hotBlobShards
	return &s.hot[shardIdx]
}

func (s *BlobStore) cacheHotOwned(packID uint64, offset uint32, data []byte) {
	if len(data) > hotBlobShardBudget {
		return
	}
	sh := s.hotShardFor(packID, offset)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].packID == packID && sh.slots[idx].offset == offset {
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
	sh.slots[idx] = hotBlob{packID: packID, offset: offset, data: data}
	sh.count++
	sh.bytes += len(data)
	sh.trimLocked()
}

func (s *BlobStore) extendHotShard(oldRef BlobRef, patchOffset uint32, patchData []byte, newLength uint32) {
	sh := s.hotShardFor(oldRef.PackID, oldRef.Offset)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		slot := &sh.slots[idx]
		if slot.packID != oldRef.PackID || slot.offset != oldRef.Offset || slot.data == nil {
			continue
		}
		oldData := slot.data
		newLen := int(newLength)
		if newLen > hotBlobShardBudget {
			sh.evictBlockLocked(oldRef.PackID, oldRef.Offset)
			return
		}
		var newData []byte
		if newLen <= cap(oldData) {
			newData = oldData[:newLen]
			if newLen > len(oldData) {
				clear(newData[len(oldData):newLen])
			}
		} else {
			newData = make([]byte, newLen)
			copy(newData, oldData)
		}
		if len(patchData) > 0 && int(patchOffset)+len(patchData) <= newLen {
			copy(newData[patchOffset:], patchData)
		}
		sh.bytes += newLen - len(oldData)
		slot.data = newData
		sh.trimLocked()
		return
	}
}

func (sh *hotShard) trimLocked() {
	for sh.bytes > hotBlobShardBudget && sh.count > 0 {
		sh.evictOldestLocked()
	}
	if sh.count == 0 {
		sh.bytes = 0
	}
}

func (sh *hotShard) evictOldestLocked() {
	slot := &sh.slots[sh.head]
	sh.bytes -= len(slot.data)
	*slot = hotBlob{}
	sh.head = (sh.head + 1) % len(sh.slots)
	sh.count--
}

func (sh *hotShard) evictBlockLocked(packID uint64, offset uint32) {
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].packID != packID || sh.slots[idx].offset != offset {
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

func (s *BlobStore) evictHotPack(packID uint64) {
	for i := range s.hot {
		sh := &s.hot[i]
		sh.mu.Lock()
		for j := 0; j < sh.count; {
			idx := (sh.head + j) % len(sh.slots)
			if sh.slots[idx].packID != packID {
				j++
				continue
			}
			sh.bytes -= len(sh.slots[idx].data)
			for k := j; k < sh.count-1; k++ {
				from := (sh.head + k + 1) % len(sh.slots)
				to := (sh.head + k) % len(sh.slots)
				sh.slots[to] = sh.slots[from]
			}
			tail := (sh.head + sh.count - 1) % len(sh.slots)
			sh.slots[tail] = hotBlob{}
			sh.count--
		}
		if sh.count == 0 {
			sh.bytes = 0
		}
		sh.mu.Unlock()
	}
}

// Exists 报告指定编号的 .pack 文件在磁盘上是否存在。
func (s *BlobStore) Exists(packID uint64) bool {
	_, err := os.Stat(s.Path(packID))
	return err == nil
}

// HasDirty 报告是否仍有待同步的脏 .pack 文件或 packs/ 目录。
func (s *BlobStore) HasDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dirtyPacks) > 0 || s.dirDirty
}

// SyncFileCount 返回已成功执行 fdatasync 的 .pack 文件累计次数（用于测试观测）。
func (s *BlobStore) SyncFileCount() uint64 { return s.syncFileCount.Load() }

// SyncDirCount 返回已成功执行 fsync 的 packs/ 目录累计次数（用于测试观测）。
func (s *BlobStore) SyncDirCount() uint64 { return s.syncDirCount.Load() }

// Sync 将所有未刷盘的脏 .pack 常驻句柄与待刷 packs/ 目录持久化落盘。
func (s *BlobStore) Sync() error {
	s.mu.Lock()
	if len(s.dirtyPacks) == 0 && !s.dirDirty {
		s.mu.Unlock()
		return nil
	}
	packs := make([]uint64, 0, len(s.dirtyPacks))
	needSyncDir := s.dirDirty
	for packID := range s.dirtyPacks {
		packs = append(packs, packID)
		if _, isNew := s.dirtyNew[packID]; isNew {
			needSyncDir = true
			delete(s.dirtyNew, packID)
		}
	}
	clear(s.dirtyPacks)
	clear(s.unownedDirty)
	clear(s.inoPacks)
	s.dirDirty = false
	s.mu.Unlock()

	return s.syncBatch(packs, needSyncDir)
}

// SyncInode 仅对包含指定 Inode 未刷盘写入的脏 .pack 常驻句柄执行 fdatasync，
// 且仅在涉及新建 .pack 首次落盘或目录变更时对 packs/ 目录执行 1 次目录 fsync（ADR-0031 §3.3）。
func (s *BlobStore) SyncInode(ino uint64) error {
	s.mu.Lock()
	var packs []uint64
	if set := s.inoPacks[ino]; len(set) > 0 {
		packs = make([]uint64, 0, len(set))
		for packID := range set {
			packs = append(packs, packID)
		}
	}
	for packID := range s.unownedDirty {
		packs = append(packs, packID)
	}
	if len(packs) == 0 && !s.dirDirty {
		s.mu.Unlock()
		return nil
	}

	needSyncDir := s.dirDirty
	s.dirDirty = false
	for _, packID := range packs {
		if _, isNew := s.dirtyNew[packID]; isNew {
			needSyncDir = true
			delete(s.dirtyNew, packID)
		}
		delete(s.dirtyPacks, packID)
		delete(s.unownedDirty, packID)
		for owner, set := range s.inoPacks {
			delete(set, packID)
			if len(set) == 0 {
				delete(s.inoPacks, owner)
			}
		}
	}
	delete(s.inoPacks, ino)
	s.mu.Unlock()

	return s.syncBatch(packs, needSyncDir)
}

func (s *BlobStore) syncPack(packID uint64) error {
	file, err := s.getOrOpenPackFile(packID)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		s.reMarkPackDirty(packID)
		return err
	}
	if err := syncFileData(file); err != nil {
		s.reMarkPackDirty(packID)
		return err
	}
	s.syncFileCount.Add(1)
	return nil
}

func (s *BlobStore) reMarkPackDirty(packID uint64) {
	s.mu.Lock()
	s.dirtyPacks[packID] = struct{}{}
	s.unownedDirty[packID] = struct{}{}
	s.mu.Unlock()
}

func (s *BlobStore) syncPacksDir() error {
	handle, err := os.Open(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		s.mu.Lock()
		s.dirDirty = true
		s.mu.Unlock()
		return err
	}
	if err := errors.Join(handle.Sync(), handle.Close()); err != nil {
		s.mu.Lock()
		s.dirDirty = true
		s.mu.Unlock()
		return err
	}
	s.syncDirCount.Add(1)
	return nil
}

func (s *BlobStore) syncBatch(packs []uint64, needSyncDir bool) error {
	var errs []error
	for _, packID := range packs {
		if err := s.syncPack(packID); err != nil {
			errs = append(errs, err)
		}
	}
	if needSyncDir {
		if err := s.syncPacksDir(); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// SealActivePack 将当前活跃 .pack 封口（不再接受任何后续 Append 或 ExtendTail 追加）并同步刷盘（ADR-0031 §4.1）。
func (s *BlobStore) SealActivePack() error {
	s.mu.Lock()
	s.activePackID = 0
	s.activeFile = nil
	s.activeOffset = 0
	s.mu.Unlock()
	return s.Sync()
}

// LinkAllTo 先将当前活跃 .pack 封口并刷盘，再将 packs/*.pack 扁平硬链接至目标快照或子分支目录（遇 EMLINK 回退流式复制）。
func (s *BlobStore) LinkAllTo(dst *BlobStore) error {
	if dst == nil {
		return ErrInvalidValue
	}
	if err := s.SealActivePack(); err != nil {
		return err
	}
	entries, err := os.ReadDir(s.dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if err := os.MkdirAll(dst.dir, 0o755); err != nil {
		return err
	}
	dst.dirReady.Store(true)

	var maxLinkedID uint64
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			continue
		}
		packID, parseErr := parsePackID(entry.Name())
		if parseErr == nil && packID > maxLinkedID {
			maxLinkedID = packID
		}
		from := filepath.Join(s.dir, entry.Name())
		to := filepath.Join(dst.dir, entry.Name())
		if _, statErr := os.Stat(to); statErr == nil {
			continue
		} else if !errors.Is(statErr, os.ErrNotExist) {
			return statErr
		}
		if linkErr := os.Link(from, to); linkErr != nil {
			if copyErr := copyPackFile(from, to); copyErr != nil {
				return errors.Join(linkErr, copyErr)
			}
			if parseErr == nil {
				dst.reMarkPackDirty(packID)
			}
		}
		dst.mu.Lock()
		dst.dirDirty = true
		dst.mu.Unlock()
	}
	if maxLinkedID > 0 {
		dst.AdvanceMinPackID(maxLinkedID)
	}
	return dst.Sync()
}

func copyPackFile(from, to string) error {
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()
	out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o644)
	if err != nil {
		return err
	}
	_, copyErr := io.Copy(out, in)
	return errors.Join(copyErr, out.Close())
}

// RemovePack 关闭指定 .pack 的常驻文件句柄、剔除热块缓存并从宿主机删除物理 .pack 文件（ADR-0031 §4.2）。
func (s *BlobStore) RemovePack(packID uint64) error {
	if packID == 0 {
		return nil
	}
	s.evictHotPack(packID)

	s.mu.Lock()
	if s.activePackID == packID {
		s.activePackID = 0
		s.activeFile = nil
		s.activeOffset = 0
	}
	delete(s.dirtyPacks, packID)
	delete(s.unownedDirty, packID)
	delete(s.dirtyNew, packID)
	for owner, set := range s.inoPacks {
		delete(set, packID)
		if len(set) == 0 {
			delete(s.inoPacks, owner)
		}
	}
	s.dirDirty = true
	s.mu.Unlock()

	s.filesMu.Lock()
	if f, ok := s.packFiles[packID]; ok {
		delete(s.packFiles, packID)
		if f != nil {
			_ = f.Close()
		}
	}
	s.filesMu.Unlock()

	err := os.Remove(s.Path(packID))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

// Close 安全关闭所有常驻 .pack 文件句柄（含活跃包与已封口只读包）。
func (s *BlobStore) Close() error {
	s.mu.Lock()
	s.closed = true
	s.activePackID = 0
	s.activeFile = nil
	s.activeOffset = 0
	s.mu.Unlock()

	s.filesMu.Lock()
	defer s.filesMu.Unlock()
	var errs []error
	for id, f := range s.packFiles {
		delete(s.packFiles, id)
		if f != nil {
			if err := f.Close(); err != nil && !errors.Is(err, os.ErrClosed) {
				errs = append(errs, err)
			}
		}
	}
	return errors.Join(errs...)
}

func parsePackID(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".pack"), 16, 64)
}
