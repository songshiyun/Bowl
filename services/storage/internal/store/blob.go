package store

import (
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
)

// BlobStore 规格与分片热块缓存参数（ADR-0030 §3.1 & §3.2）。
const (
	BlobIDSegment      uint64 = 4096
	blobShardCount            = 256
	hotBlobShards             = 16
	hotBlobSlots              = 64
	hotBlobTotalBudget        = 64 << 20                           // 64 MiB
	hotBlobShardBudget        = hotBlobTotalBudget / hotBlobShards // 4 MiB per shard
)

// BlobReserveFunc 向盘级分配器持久化预留一段连续 BlobID 编号，返回闭区间 [first, last]。
type BlobReserveFunc func(count uint64) (first uint64, last uint64, err error)

type hotBlob struct {
	id   uint64
	data []byte
}

// hotShard 是热块缓存的单个分片，独立加锁与独立 4 MiB 字节预算。
type hotShard struct {
	mu    sync.Mutex
	slots [hotBlobSlots]hotBlob
	head  int
	count int
	bytes int
}

// BlobStore 管理单个分支或快照目录下不可变的 blobs/<00..ff>/<blob_id_hex16>.blob 文件、
// 64 MiB 分片环形热块缓存、4096 步长无锁 BlobID 分配游标及脏块屏障追踪表（ADR-0030 §3.2）。
type BlobStore struct {
	dir        string
	shardReady [blobShardCount]atomic.Bool

	// 1. 无锁 BlobID 分配游标（步长 4096 向盘级分配器持久化预留）
	nextBlobID     atomic.Uint64
	reservedBlobID atomic.Uint64
	allocMu        sync.Mutex
	reserveBlobID  BlobReserveFunc

	// 2. 分片环形热块缓存（16 分片 × 64 槽位，总预算 64 MiB，分片独立加锁）
	hot [hotBlobShards]hotShard

	// 3. Per-Inode 轻量分片定序锁（4096 分片 Mutex）
	inodeLocks [4096]sync.Mutex

	// 4. 脏块与分片目录屏障追踪表（供 Fsync(ino) 与 Commit 精确刷盘）
	mu        sync.Mutex
	dirtyFile map[uint64]uint64              // blobID -> 所属 ino（未完成 fdatasync 的窗口内脏块；0 表示未关联特定 ino）
	inoBlobs  map[uint64]map[uint64]struct{} // ino -> dirty blobID 集合
	dirtyNew  map[uint64]struct{}            // 本窗口内新创建的 blobID（需在 Sync 时连带 fsync 父分片目录）
	dirtyDir  map[string]struct{}            // 删除过已持久化旧块的分片目录（需在 Sync 时 fsync 目录）
}

func init() {
	openInitHooks = append(openInitHooks, func(s *StoreActor) error {
		bs := NewBlobStore(s.dir)
		s.blobs = bs
		s.applyPatchesFn = bs.ApplyPatches
		s.syncInodeFn = bs.SyncInode
		s.detachMetaFn = bs.DetachMeta
		s.removeFilesFn = bs.RemoveFiles
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

// SetBlobReserveFunc 为当前 StoreActor 的 BlobStore 注入自定义盘级 BlobID 号段预留器。
func (s *StoreActor) SetBlobReserveFunc(fn BlobReserveFunc) {
	if bs := s.Blobs(); bs != nil {
		bs.SetReserveFunc(fn)
	}
}

// NewBlobStore 创建以 base（分支或快照根目录）为根的 BlobStore，并在未注入外部分配器时扫描已有最大 BlobID 初始化默认号段分配器。
func NewBlobStore(base string) *BlobStore {
	return NewBlobStoreWithAllocator(base, nil)
}

// NewBlobStoreWithAllocator 创建指定号段预留分配器的 BlobStore。
func NewBlobStoreWithAllocator(base string, reserve BlobReserveFunc) *BlobStore {
	bs := &BlobStore{
		dir:           filepath.Join(base, "blobs"),
		reserveBlobID: reserve,
		dirtyFile:     make(map[uint64]uint64),
		inoBlobs:      make(map[uint64]map[uint64]struct{}),
		dirtyNew:      make(map[uint64]struct{}),
		dirtyDir:      make(map[string]struct{}),
	}
	maxID := bs.scanExistingBlobs()
	if bs.reserveBlobID == nil {
		bs.setupDefaultAllocator(maxID)
	}
	return bs
}

// SetReserveFunc 动态注入外部 BlobID 号段预留函数并重置本地预留游标。
func (s *BlobStore) SetReserveFunc(fn BlobReserveFunc) {
	s.allocMu.Lock()
	defer s.allocMu.Unlock()
	s.reserveBlobID = fn
	s.nextBlobID.Store(0)
	s.reservedBlobID.Store(0)
}

func (s *BlobStore) scanExistingBlobs() uint64 {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return 0
	}
	var maxID uint64
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		s.markShard(entry.Name())
		shardPath := filepath.Join(s.dir, entry.Name())
		files, err := os.ReadDir(shardPath)
		if err != nil {
			continue
		}
		for _, f := range files {
			if f.IsDir() || !strings.HasSuffix(f.Name(), ".blob") {
				continue
			}
			if id, err := parseBlobID(f.Name()); err == nil && id > maxID {
				maxID = id
			}
		}
	}
	return maxID
}

func (s *BlobStore) setupDefaultAllocator(initialLast uint64) {
	last := initialLast
	var mu sync.Mutex
	s.reserveBlobID = func(count uint64) (uint64, uint64, error) {
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

// NextBlobID 无锁分配下一个唯一 BlobID；当本地号段耗尽时加 allocMu 按 4096 步长预留新号段。
func (s *BlobStore) NextBlobID() (uint64, error) {
	for {
		next := s.nextBlobID.Load()
		reserved := s.reservedBlobID.Load()
		if next != 0 && next <= reserved {
			if s.nextBlobID.CompareAndSwap(next, next+1) {
				return next, nil
			}
			continue
		}
		s.allocMu.Lock()
		next = s.nextBlobID.Load()
		reserved = s.reservedBlobID.Load()
		if next != 0 && next <= reserved {
			s.allocMu.Unlock()
			continue
		}
		if s.reserveBlobID == nil {
			s.setupDefaultAllocator(0)
		}
		first, last, err := s.reserveBlobID(BlobIDSegment)
		if err != nil {
			s.allocMu.Unlock()
			return 0, err
		}
		if first == 0 || first > last {
			s.allocMu.Unlock()
			return 0, ErrNoSpace
		}
		id := first
		s.reservedBlobID.Store(last)
		s.nextBlobID.Store(first + 1)
		s.allocMu.Unlock()
		return id, nil
	}
}

// Root 返回 blobs 根目录路径。
func (s *BlobStore) Root() string { return s.dir }

// InodeLock 返回指定 Inode 对应的分片互斥锁指针（按 ino & 0xfff 索引）。
func (s *BlobStore) InodeLock(ino uint64) *sync.Mutex {
	return &s.inodeLocks[ino&0xfff]
}

// Path 返回指定编号块文件的物理路径 <branch_dir>/blobs/<00..ff>/<blob_id_hex16>.blob。
func (s *BlobStore) Path(id uint64) string {
	var buf [29]byte // "/00/0000000000000000.blob"
	const hexDigits = "0123456789abcdef"
	shard := byte(id & 0xff)
	buf[0] = filepath.Separator
	buf[1] = hexDigits[shard>>4]
	buf[2] = hexDigits[shard&0x0f]
	buf[3] = filepath.Separator
	for i := 19; i >= 4; i-- {
		buf[i] = hexDigits[id&0x0f]
		id >>= 4
	}
	copy(buf[20:], ".blob")
	return s.dir + string(buf[:25])
}

// Write 将不可变块写入分片目录（不绑定特定 Inode）。
func (s *BlobStore) Write(id uint64, data []byte) error {
	return s.write(0, id, data)
}

// WriteForInode 将块写入分片目录、登记其所属 Inode 與 dirtyNew，并零拷贝收养内存切片至 hotShard。
func (s *BlobStore) WriteForInode(ino, id uint64, data []byte) error {
	return s.write(ino, id, data)
}

func (s *BlobStore) write(ino, id uint64, data []byte) error {
	if id == 0 || len(data) == 0 || len(data) > BlobSize {
		return ErrInvalidValue
	}
	path := s.Path(id)
	shardIdx := id & 0xff
	if !s.shardReady[shardIdx].Load() {
		dir := filepath.Dir(path)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
		s.shardReady[shardIdx].Store(true)
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

	s.mu.Lock()
	s.dirtyFile[id] = ino
	if ino != 0 {
		set := s.inoBlobs[ino]
		if set == nil {
			set = make(map[uint64]struct{})
			s.inoBlobs[ino] = set
		}
		set[id] = struct{}{}
	}
	s.dirtyNew[id] = struct{}{}
	s.mu.Unlock()

	s.cacheHotOwned(id, data)
	return nil
}

// Read 读取指定块的完整内容。
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

// ReadAt 按块内偏移读取指定区间：优先从 hotShard 分片锁内直接拷贝（0 系统调用），未命中则走 os.Open + file.ReadAt。
func (s *BlobStore) ReadAt(id uint64, offset int64, dst []byte) error {
	if len(dst) == 0 {
		return nil
	}
	if s.hotReadAt(id, offset, dst) {
		return nil
	}
	file, err := os.Open(s.Path(id))
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	n, err := file.ReadAt(dst, offset)
	if errors.Is(err, io.EOF) {
		if n == len(dst) {
			return nil
		}
		return io.ErrUnexpectedEOF
	}
	return err
}

// hotReadAt 在对应分片锁保护下直接将内存缓存切片的 [offset, offset+len(dst)) 拷贝至 dst。
func (s *BlobStore) hotReadAt(id uint64, offset int64, dst []byte) bool {
	if offset < 0 {
		return false
	}
	sh := s.hotShardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].id == id && sh.slots[idx].data != nil {
			data := sh.slots[idx].data
			end := offset + int64(len(dst))
			if end <= int64(len(data)) {
				copy(dst, data[offset:end])
				return true
			}
			return false
		}
	}
	return false
}

// EvictHot 从分片环形热块缓存中剔除指定块。
func (s *BlobStore) EvictHot(id uint64) {
	s.evictHot(id)
}

func (s *BlobStore) hotShardFor(id uint64) *hotShard {
	return &s.hot[id%hotBlobShards]
}

func (s *BlobStore) cacheHotOwned(id uint64, data []byte) {
	if len(data) > hotBlobShardBudget {
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

func (s *BlobStore) evictHot(id uint64) {
	sh := s.hotShardFor(id)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	sh.evictIDLocked(id)
}

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

// Exists 报告块文件在磁盘上是否存在。
func (s *BlobStore) Exists(id uint64) bool {
	_, err := os.Stat(s.Path(id))
	return err == nil
}

// WindowLocal 报告指定块是否为当前 Sync 窗口内新建且尚未持久化的脏块。
func (s *BlobStore) WindowLocal(id uint64) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.dirtyFile[id]
	return ok
}

// HasDirty 报告是否仍有待同步的脏块或分片目录。
func (s *BlobStore) HasDirty() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.dirtyFile) > 0 || len(s.dirtyDir) > 0
}

// DetachMeta 在 Actor Step 4 内执行纯内存状态摘除（0 磁盘 I/O）：
// 从 hotShard、dirtyFile、dirtyNew、inoBlobs 中剔除指定块；
// 若被删除块不是本窗口内新建块（即不在 dirtyNew 中），则将其父分片目录登记进 dirtyDir。
func (s *BlobStore) DetachMeta(ids []uint64) {
	if len(ids) == 0 {
		return
	}
	for _, id := range ids {
		if id != 0 {
			s.evictHot(id)
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, id := range ids {
		if id == 0 {
			continue
		}
		_, isNew := s.dirtyNew[id]
		delete(s.dirtyNew, id)
		if ino, ok := s.dirtyFile[id]; ok {
			delete(s.dirtyFile, id)
			if ino != 0 {
				if set := s.inoBlobs[ino]; set != nil {
					delete(set, id)
					if len(set) == 0 {
						delete(s.inoBlobs, ino)
					}
				}
			}
		}
		if !isNew {
			s.dirtyDir[s.shardDir(id)] = struct{}{}
		}
	}
}

func (s *BlobStore) shardDir(id uint64) string {
	return filepath.Join(s.dir, fmt.Sprintf("%02x", id&0xff))
}

// RemoveFiles 在 Actor 队列外由调用方协程删除物理 .blob 文件（单块直接 os.Remove，多块并发删除，不存在视为已删除）。
func (s *BlobStore) RemoveFiles(ids []uint64) error {
	if len(ids) == 0 {
		return nil
	}
	removeOne := func(id uint64) error {
		if id == 0 {
			return nil
		}
		err := os.Remove(s.Path(id))
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	if len(ids) == 1 {
		return removeOne(ids[0])
	}
	workers := min(len(ids), 16)
	errs := runParallel(len(ids), workers, func(i int) error {
		return removeOne(ids[i])
	})
	return errors.Join(errs...)
}

// Remove 摘除指定块的内存状态并立即删除其物理文件。
func (s *BlobStore) Remove(id uint64) error {
	if id == 0 {
		return nil
	}
	s.DetachMeta([]uint64{id})
	return s.RemoveFiles([]uint64{id})
}

// ApplyPatches 在 Actor Step 3 按 blobID 批量原地合并窗口内局部写增量（WriteAt + Truncate + 同步更新 hotShard）。
func (s *BlobStore) ApplyPatches(patches []PatchLog) error {
	if len(patches) == 0 {
		return nil
	}
	for i := 0; i < len(patches); {
		blobID := patches[i].BlobID
		if blobID == 0 {
			return ErrInvalidValue
		}
		j := i + 1
		for j < len(patches) && patches[j].BlobID == blobID {
			j++
		}
		if err := s.applyBlobPatches(patches[i:j]); err != nil {
			return err
		}
		i = j
	}
	return nil
}

func (s *BlobStore) applyBlobPatches(group []PatchLog) error {
	last := group[len(group)-1]
	if last.BlobID == 0 || last.NewLen == 0 || last.NewLen > BlobSize {
		return ErrInvalidValue
	}
	file, err := os.OpenFile(s.Path(last.BlobID), os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	var writeErr error
	var truncErr error
	for _, p := range group {
		if p.NewLen == 0 || p.NewLen > BlobSize {
			writeErr = ErrInvalidValue
			break
		}
		if len(p.Data) > 0 {
			if _, wErr := file.WriteAt(p.Data, int64(p.Offset)); wErr != nil {
				writeErr = wErr
				break
			}
		}
		if tErr := file.Truncate(int64(p.NewLen)); tErr != nil {
			truncErr = tErr
			break
		}
	}
	closeErr := file.Close()
	if err := errors.Join(writeErr, truncErr, closeErr); err != nil {
		return err
	}

	s.markFileInode(last.BlobID, last.Ino)
	for _, p := range group {
		s.patchHotShard(p)
	}
	return nil
}

func (s *BlobStore) patchHotShard(p PatchLog) {
	sh := s.hotShardFor(p.BlobID)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for i := 0; i < sh.count; i++ {
		idx := (sh.head + i) % len(sh.slots)
		if sh.slots[idx].id != p.BlobID || sh.slots[idx].data == nil {
			continue
		}
		oldData := sh.slots[idx].data
		newLen := int(p.NewLen)
		if newLen > hotBlobShardBudget {
			sh.evictIDLocked(p.BlobID)
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
		if len(p.Data) > 0 && int(p.Offset)+len(p.Data) <= newLen {
			copy(newData[p.Offset:], p.Data)
		}
		sh.bytes += newLen - len(oldData)
		sh.slots[idx].data = newData
		sh.trimLocked()
		return
	}
}

func (s *BlobStore) markShard(name string) {
	if index, err := strconv.ParseUint(name, 16, 8); err == nil {
		s.shardReady[index].Store(true)
	}
}

func (s *BlobStore) markFile(id uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.dirtyFile[id]; !exists {
		s.dirtyFile[id] = 0
	}
}

func (s *BlobStore) markFileInode(id, ino uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ino == 0 {
		if _, exists := s.dirtyFile[id]; !exists {
			s.dirtyFile[id] = 0
		}
		return
	}
	s.dirtyFile[id] = ino
	set := s.inoBlobs[ino]
	if set == nil {
		set = make(map[uint64]struct{})
		s.inoBlobs[ino] = set
	}
	set[id] = struct{}{}
}

func (s *BlobStore) markDir(dir string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtyDir[dir] = struct{}{}
}

// Sync 将所有窗口内脏块与待刷分片目录持久化落盘。
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
	dirSet := make(map[string]struct{}, len(s.dirtyDir))
	for dir := range s.dirtyDir {
		dirSet[dir] = struct{}{}
	}
	for _, id := range files {
		if _, ok := s.dirtyNew[id]; ok {
			dirSet[s.shardDir(id)] = struct{}{}
			delete(s.dirtyNew, id)
		}
	}
	dirs := make([]string, 0, len(dirSet))
	for dir := range dirSet {
		dirs = append(dirs, dir)
	}
	clear(s.dirtyFile)
	clear(s.dirtyDir)
	clear(s.inoBlobs)
	s.mu.Unlock()

	return s.syncBatch(files, dirs)
}

// SyncInode 仅同步指定 Inode 关联的脏块文件与对应新建/删块分片目录。
func (s *BlobStore) SyncInode(ino uint64) error {
	s.mu.Lock()
	var files []uint64
	if ids := s.inoBlobs[ino]; len(ids) > 0 {
		files = make([]uint64, 0, len(ids))
		for id := range ids {
			files = append(files, id)
		}
	}
	for id, owner := range s.dirtyFile {
		if owner == 0 {
			files = append(files, id)
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
			dirSet[s.shardDir(id)] = struct{}{}
			delete(s.dirtyNew, id)
		}
		if owner, ok := s.dirtyFile[id]; ok {
			delete(s.dirtyFile, id)
			if owner != 0 {
				if set := s.inoBlobs[owner]; set != nil {
					delete(set, id)
					if len(set) == 0 {
						delete(s.inoBlobs, owner)
					}
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

func (s *BlobStore) syncFile(id uint64) error {
	file, err := os.Open(s.Path(id))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		s.markFile(id)
		return err
	}
	if err := errors.Join(syncFileData(file), file.Close()); err != nil {
		s.markFile(id)
		return err
	}
	return nil
}

func (s *BlobStore) syncDir(dir string) error {
	handle, err := os.Open(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		s.markDir(dir)
		return err
	}
	if err := errors.Join(handle.Sync(), handle.Close()); err != nil {
		s.markDir(dir)
		return err
	}
	return nil
}

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

func parseBlobID(name string) (uint64, error) {
	return strconv.ParseUint(strings.TrimSuffix(name, ".blob"), 16, 64)
}
