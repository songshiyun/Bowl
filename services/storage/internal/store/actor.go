package store

import (
	"bytes"
	"errors"
	"io"
	"math"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"
)

// Pebble 初始化与批次重置阈值常量（ADR-0030 §2.2 & §2.5）。
const (
	DefaultMemTableSize    = 16 << 20 // 16 MiB
	DefaultCacheSize       = 64 << 20 // 64 MiB
	DefaultMaxOpenFiles    = 128
	DefaultWALBytesPerSync = 4 << 20 // 4 MiB
	DefaultBloomBitsPerKey = 10
	PebbleLevelCount       = 7

	BatchResetMaxReprBytes = 1 << 20 // 1 MiB
	BatchResetMaxCount     = 8192
)

// pebbleReader 抽象 *pebble.DB 与 *pebble.Batch 的共享只读接口。
type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error)
}

// opKind 标识投递到 StoreActor 双缓冲队列的操作类型。
type opKind uint8

const (
	opCustom opKind = iota
	opMknod
	opMkdir
	opCreate
	opSymlink
	opLink
	opUnlink
	opRmdir
	opRename
	opSetAttr
	opSetXattr
	opRemoveXattr
	opWrite
	opFallocate
	opFsync
	opFsyncDir
	opPurgeOrphan
	opCleanOrphans
	opCompactRelocate
	opMax
)

type opHandler struct {
	validate func(s *StoreActor, cmd *writeCmd) error
	mutate   func(s *StoreActor, cmd *writeCmd)
}

var (
	opHandlers    [opMax]opHandler
	openInitHooks []func(s *StoreActor) error
)

// registerOp 注册指定 opKind 的 Phase A 校验与 Phase B 变更处理器（供同包扩展文件在 init 中挂载）。
func registerOp(op opKind, validate func(*StoreActor, *writeCmd) error, mutate func(*StoreActor, *writeCmd)) {
	opHandlers[op] = opHandler{validate: validate, mutate: mutate}
}

// writeCmd 是投递至 StoreActor 双缓冲 MPSC 队列的命令载体（由 sync.Pool 复用）。
type writeCmd struct {
	op   opKind
	req  any
	resp any
	err  error
	done chan struct{} // 容量为 1 的完成信号

	validate    func(s *StoreActor, cmd *writeCmd) error
	mutate      func(s *StoreActor, cmd *writeCmd)
	syncBarrier bool
	syncIno     uint64
	pinnedPacks []uint64
}

// InodeReserveFunc 向盘级分配器持久化预留一段连续 Inode 编号，返回闭区间 [first, last]。
type InodeReserveFunc func(count uint64) (first uint64, last uint64, err error)

// Options 配置 StoreActor 实例打开参数。
type Options struct {
	Dir          string
	DiskID       string
	BranchID     string
	ReadOnly     bool
	MaxBytes     uint64
	MaxInodes    uint64
	AtimeMode    AtimeMode
	Cache        *pebble.Cache
	ReserveInode InodeReserveFunc
	Now          func() time.Time
}

// NewPebbleOptions 构建符合 ADR-0030 §2.2 规范的 *pebble.Options：
//   - 自定义 Comparer.Split（entry:/xattr: -> 15，data: -> 14，其余 -> len(key)）；
//   - 全部 7 层启用 bloom.FilterPolicy(10)；
//   - MemTableSize=16MiB, MaxOpenFiles=128, WALBytesPerSync=4MiB, FormatMajorVersion=pebble.FormatNewest。
func NewPebbleOptions(cache *pebble.Cache, readOnly bool) *pebble.Options {
	if cache == nil {
		cache = pebble.NewCache(DefaultCacheSize)
	}
	cmp := *pebble.DefaultComparer
	cmp.Name = "bowl.store.comparer"
	cmp.Split = SplitKey

	var levels [PebbleLevelCount]pebble.LevelOptions
	policy := bloom.FilterPolicy(DefaultBloomBitsPerKey)
	for i := range levels {
		levels[i].FilterPolicy = policy
	}

	return &pebble.Options{
		Comparer:           &cmp,
		Cache:              cache,
		MemTableSize:       DefaultMemTableSize,
		MaxOpenFiles:       DefaultMaxOpenFiles,
		WALBytesPerSync:    DefaultWALBytesPerSync,
		FormatMajorVersion: pebble.FormatNewest,
		Levels:             levels,
		ReadOnly:           readOnly,
		ErrorIfNotExists:   readOnly,
	}
}

// StoreActor 是单分支（或只读快照）存储引擎实例（ADR-0030 §2.3）。
type StoreActor struct {
	dir        string
	diskID     string
	branchID   string
	readOnly   bool
	atimeMode  AtimeMode
	now        func() time.Time
	db         *pebble.DB
	ownedCache *pebble.Cache

	maxBytes  atomic.Uint64
	maxInodes atomic.Uint64

	// 1. 双缓冲 MPSC 写命令队列（RPC 协程向 active 追加，Actor 协程交换到 draining 批量处理）
	mb struct {
		mu       sync.Mutex
		active   []*writeCmd
		draining []*writeCmd
		notify   chan struct{}
		closed   bool
	}
	cmdPool sync.Pool

	// 2. Actor 写协程私有状态（仅在单协程 Loop 内无锁读写）
	batch          *pebble.Batch
	stat           BranchStat
	packStats      map[uint64]PackMeta
	dirtyPackStats map[uint64]struct{}
	nextIno        uint64
	reservedIno    uint64
	reserveInode   InodeReserveFunc
	walDirty       atomic.Bool
	tickCount      atomic.Uint64
	batchResetCnt  atomic.Uint64

	// 3. 并发读路径无锁镜像（每次 Batch 提交后刷新）
	atomicLogicalBytes atomic.Uint64
	atomicInodes       atomic.Uint64

	// 4. 延迟 Atime 内存脏表（读路径登记/叠加，写循环或关闭时合并刷盘）
	atimeMu    sync.Mutex
	dirtyAtime map[uint64]int64 // ino -> unixNano

	// 5. 生命周期守卫（保护 s.db 不在在途直读或写阶段 1 预读期间被 Close 关闭）
	lifecycleMu    sync.RWMutex
	closing        atomic.Bool
	discardOnClose atomic.Bool
	loopDone       chan struct{}
	closeOnce      sync.Once
	closeErr       error

	// 6. Data Layer 协同锁与钩子
	inodeLocks                [4096]sync.Mutex
	blobs                     any
	syncInodeFn               func(ino uint64) error
	beforeCompactRelocateHook func()
	inFlightMu                sync.Mutex
	inFlightPacks             map[uint64]int
	deadPacksMu               sync.Mutex
	pendingDeadPacks          []uint64
}

// Open 打开或初始化一个 StoreActor 实例。
// 空库首次可写打开时通过单次 Batch 初始化根目录 Inode(1)（S_IFDIR | 0755, Nlink = 2）与 "sys:stat"（Inodes = 1, LogicalBytes = 0）。
func Open(opts Options) (*StoreActor, error) {
	if opts.Dir == "" {
		return nil, ErrInvalid
	}
	nowFn := opts.Now
	if nowFn == nil {
		nowFn = time.Now
	}

	var ownedCache *pebble.Cache
	cache := opts.Cache
	if cache == nil {
		ownedCache = pebble.NewCache(DefaultCacheSize)
		cache = ownedCache
	}

	if !opts.ReadOnly {
		if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
			if ownedCache != nil {
				ownedCache.Unref()
			}
			return nil, err
		}
	}

	pebbleOpts := NewPebbleOptions(cache, opts.ReadOnly)
	db, err := pebble.Open(opts.Dir, pebbleOpts)
	if err != nil {
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}

	s := &StoreActor{
		dir:            opts.Dir,
		diskID:         opts.DiskID,
		branchID:       opts.BranchID,
		readOnly:       opts.ReadOnly,
		atimeMode:      opts.AtimeMode,
		now:            nowFn,
		db:             db,
		ownedCache:     ownedCache,
		packStats:      make(map[uint64]PackMeta),
		dirtyPackStats: make(map[uint64]struct{}),
		reserveInode:   opts.ReserveInode,
		dirtyAtime:     make(map[uint64]int64),
		loopDone:       make(chan struct{}),
		inFlightPacks:  make(map[uint64]int),
	}
	s.maxBytes.Store(opts.MaxBytes)
	s.maxInodes.Store(opts.MaxInodes)
	s.mb.notify = make(chan struct{}, 1)
	s.cmdPool.New = func() any {
		return &writeCmd{
			done: make(chan struct{}, 1),
		}
	}

	if err := s.initOrLoadState(); err != nil {
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}

	for _, hook := range openInitHooks {
		if err := hook(s); err != nil {
			if bs := s.Blobs(); bs != nil {
				_ = bs.Close()
			}
			_ = db.Close()
			if ownedCache != nil {
				ownedCache.Unref()
			}
			return nil, err
		}
	}

	if err := s.syncPackStatsWithBlobs(); err != nil {
		if bs := s.Blobs(); bs != nil {
			_ = bs.Close()
		}
		_ = db.Close()
		if ownedCache != nil {
			ownedCache.Unref()
		}
		return nil, err
	}

	if s.readOnly {
		close(s.loopDone)
	} else {
		s.batch = s.db.NewIndexedBatch()
		go s.runLoop()
	}

	return s, nil
}

func (s *StoreActor) initOrLoadState() error {
	val, closer, err := s.db.Get(StatKey())
	if errors.Is(err, pebble.ErrNotFound) {
		if s.readOnly {
			return ErrNotFound
		}
		nowNano := s.now().UnixNano()
		root := Inode{
			Ino:        RootIno,
			ParentIno:  RootIno,
			Mode:       S_IFDIR | 0o755,
			Nlink:      2,
			Size:       0,
			Atime:      nowNano,
			Mtime:      nowNano,
			Ctime:      nowNano,
			Generation: 1,
			WriteSeq:   1,
		}
		stat := BranchStat{
			LogicalBytes: 0,
			Inodes:       1,
			NextWriteSeq: 1,
		}
		initBatch := s.db.NewBatch()
		defer func() { _ = initBatch.Close() }()
		if err := batchSetInodeDeferred(initBatch, root); err != nil {
			return err
		}
		batchSetStatDeferred(initBatch, stat)
		if err := s.db.Apply(initBatch, pebble.Sync); err != nil {
			return err
		}
		s.stat = stat
		s.atomicLogicalBytes.Store(stat.LogicalBytes)
		s.atomicInodes.Store(stat.Inodes)
		s.setupDefaultInodeAllocator(RootIno)
		return nil
	}
	if err != nil {
		return err
	}
	stat, decErr := DecodeBranchStat(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return errors.Join(decErr, closeErr)
	}
	s.stat = stat
	s.atomicLogicalBytes.Store(stat.LogicalBytes)
	s.atomicInodes.Store(stat.Inodes)

	if !s.readOnly && s.reserveInode == nil {
		maxIno, err := s.scanMaxInode()
		if err != nil {
			return err
		}
		s.setupDefaultInodeAllocator(maxIno)
	}
	return s.loadPackStats()
}

func (s *StoreActor) loadPackStats() error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	prefix := PackStatPrefix()
	for valid := iter.SeekGE(prefix); valid && bytes.HasPrefix(iter.Key(), prefix); valid = iter.Next() {
		packID, decErr := DecodePackStatKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		meta, decErr := DecodePackMeta(iter.Value())
		if decErr != nil {
			return decErr
		}
		s.packStats[packID] = meta
	}
	return iter.Error()
}

func (s *StoreActor) syncPackStatsWithBlobs() error {
	bs := s.Blobs()
	if bs == nil {
		return nil
	}
	var maxPackID uint64
	var zeroLiveMetaPacks []uint64
	for packID, meta := range s.packStats {
		if packID > maxPackID {
			maxPackID = packID
		}
		if meta.LiveBytes == 0 {
			zeroLiveMetaPacks = append(zeroLiveMetaPacks, packID)
		}
	}

	var deadDiskPacks []uint64
	for _, packID := range bs.existingPackIDs() {
		if packID > maxPackID {
			maxPackID = packID
		}
		if meta, ok := s.packStats[packID]; !ok || meta.LiveBytes == 0 {
			deadDiskPacks = append(deadDiskPacks, packID)
		}
	}

	if maxPackID > 0 {
		bs.AdvanceMinPackID(maxPackID)
	}
	if s.readOnly {
		return nil
	}

	if len(zeroLiveMetaPacks) > 0 {
		cleanBatch := s.db.NewBatch()
		defer func() { _ = cleanBatch.Close() }()
		for _, packID := range zeroLiveMetaPacks {
			delete(s.packStats, packID)
			batchDeletePackMetaDeferred(cleanBatch, packID)
			if !slices.Contains(deadDiskPacks, packID) {
				deadDiskPacks = append(deadDiskPacks, packID)
			}
		}
		if err := s.db.Apply(cleanBatch, pebble.Sync); err != nil {
			return err
		}
	} else if len(deadDiskPacks) > 0 {
		if err := s.db.LogData(nil, pebble.Sync); err != nil {
			return err
		}
	}
	for _, packID := range deadDiskPacks {
		_ = bs.RemovePack(packID)
	}
	return nil
}

func (s *StoreActor) scanMaxInode() (uint64, error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	// "inode;" 紧邻 "inode:" 之后（':' = 0x3a, ';' = 0x3b），SeekLT 直接定位最大 InodeKey。
	if iter.SeekLT([]byte("inode;")) && bytes.HasPrefix(iter.Key(), inodePrefixBytes) {
		ino, err := DecodeInodeKey(iter.Key())
		if err != nil {
			return 0, err
		}
		return ino, iter.Error()
	}
	return RootIno, iter.Error()
}

func (s *StoreActor) setupDefaultInodeAllocator(initialLast uint64) {
	if s.reserveInode != nil {
		return
	}
	last := max(initialLast, RootIno)
	var mu sync.Mutex
	s.reserveInode = func(count uint64) (uint64, uint64, error) {
		mu.Lock()
		defer mu.Unlock()
		if count == 0 {
			count = InodeSegment
		}
		if last >= uint64(math.MaxInt64) {
			return 0, 0, ErrInodeExhausted
		}
		first := last + 1
		var reserved uint64
		if uint64(math.MaxInt64)-last < count {
			reserved = uint64(math.MaxInt64)
		} else {
			reserved = last + count
		}
		last = reserved
		return first, reserved, nil
	}
}

// enter 是所有直读路径与写阶段 1 预读的统一生命周期读守卫（ADR-0030 §2.6）。
func (s *StoreActor) enter() (func(), error) {
	if s.closing.Load() {
		return nil, ErrClosed
	}
	s.lifecycleMu.RLock()
	if s.closing.Load() {
		s.lifecycleMu.RUnlock()
		return nil, ErrClosed
	}
	return s.lifecycleMu.RUnlock, nil
}

// DB 返回底层 *pebble.DB 实例。
func (s *StoreActor) DB() *pebble.DB { return s.db }

// Dir 返回当前分支工作区目录路径。
func (s *StoreActor) Dir() string { return s.dir }

// ReadOnly 报告实例是否为只读模式。
func (s *StoreActor) ReadOnly() bool { return s.readOnly }

// SetMaxBytes 动态更新磁盘逻辑容量配额上限。
func (s *StoreActor) SetMaxBytes(maxBytes uint64) { s.maxBytes.Store(maxBytes) }

// MaxBytes 返回当前逻辑容量配额上限。
func (s *StoreActor) MaxBytes() uint64 { return s.maxBytes.Load() }

// SetMaxInodes 动态更新节点数量配额上限。
func (s *StoreActor) SetMaxInodes(maxInodes uint64) { s.maxInodes.Store(maxInodes) }

// MaxInodes 返回当前节点数量配额上限。
func (s *StoreActor) MaxInodes() uint64 { return s.maxInodes.Load() }

// TickCount 返回 Actor 后台已执行的写批次轮数（用于并发合并测试观测）。
func (s *StoreActor) TickCount() uint64 { return s.tickCount.Load() }

// BatchReallocCount 返回 IndexedBatch 触发防膨胀重建的次数（用于测试观测）。
func (s *StoreActor) BatchReallocCount() uint64 { return s.batchResetCnt.Load() }

// WALDirty 报告是否存在尚未通过 pebble.Sync 刷盘的 NoSync WAL 写入（用于测试观测）。
func (s *StoreActor) WALDirty() bool { return s.walDirty.Load() }

// StatFS 直接从内存原子镜像读取逻辑已用字节数与节点数（0 锁、0 I/O，ADR-0030 §2.4）。
func (s *StoreActor) StatFS() (StatFSResult, error) {
	if s.closing.Load() {
		return StatFSResult{}, ErrClosed
	}
	usedBytes := s.atomicLogicalBytes.Load()
	usedInodes := s.atomicInodes.Load()
	totalBytes := s.maxBytes.Load()

	var availBytes uint64
	if totalBytes > usedBytes {
		availBytes = totalBytes - usedBytes
	}

	const blockSize = uint64(DefaultStatFSBlockSize)
	blocks := totalBytes / blockSize
	if blocks == 0 && totalBytes > 0 {
		blocks = 1
	}
	bavail := availBytes / blockSize

	return StatFSResult{
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		AvailBytes: availBytes,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bavail,
		Bavail:     bavail,
		Files:      usedInodes,
		Ffree:      0,
		Namelen:    MaxFileNameBytes,
	}, nil
}

// GetAttr 在 lifecycleMu.RLock() 守卫下直接调用 db.Get(InodeKey) 原地解码并叠加 dirtyAtime（严禁创建 Snapshot）。
func (s *StoreActor) GetAttr(ino uint64) (Attribute, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return Attribute{}, ErrInvalid
	}
	unlock, err := s.enter()
	if err != nil {
		return Attribute{}, err
	}
	defer unlock()

	inode, err := s.readInode(s.db, ino)
	if err != nil {
		return Attribute{}, err
	}
	if err := s.checkInodeLive(s.db, inode); err != nil {
		return Attribute{}, err
	}
	s.overlayAtime(&inode)
	return inode.ToAttribute(), nil
}

func (s *StoreActor) readInode(reader pebbleReader, ino uint64) (Inode, error) {
	var keyBuf [InodeKeyLen]byte
	putInodeKey(keyBuf[:], ino)
	val, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return Inode{}, ErrNotFound
	}
	if err != nil {
		return Inode{}, err
	}
	inode, decErr := DecodeInode(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return Inode{}, errors.Join(decErr, closeErr)
	}
	if inode.Ino != ino {
		return Inode{}, ErrInvalidValue
	}
	return inode, nil
}

func (s *StoreActor) checkInodeLive(reader pebbleReader, inode Inode) error {
	if inode.Nlink != 0 {
		return nil
	}
	if s.readOnly {
		return ErrNotFound
	}
	var keyBuf [OrphanKeyLen]byte
	putOrphanKey(keyBuf[:], inode.Ino)
	_, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return ErrNotFound
	}
	if err != nil {
		return err
	}
	return closer.Close()
}

// MarkAtimeDirty 非阻塞登记节点最新访问时间至内存 dirtyAtime 表。
func (s *StoreActor) MarkAtimeDirty(ino uint64, atime time.Time) {
	s.markAtimeDirty(ino, atime.UnixNano())
}

func (s *StoreActor) markAtimeDirty(ino uint64, unixNano int64) {
	if ino == 0 || s.readOnly || s.closing.Load() {
		return
	}
	s.atimeMu.Lock()
	if prev, ok := s.dirtyAtime[ino]; !ok || unixNano > prev {
		s.dirtyAtime[ino] = unixNano
	}
	s.atimeMu.Unlock()
}

func (s *StoreActor) overlayAtime(inode *Inode) {
	if inode == nil {
		return
	}
	s.atimeMu.Lock()
	if ns, ok := s.dirtyAtime[inode.Ino]; ok && ns > inode.Atime {
		inode.Atime = ns
	}
	s.atimeMu.Unlock()
}

func (s *StoreActor) discardDirtyAtime(ino uint64) {
	if ino == 0 {
		return
	}
	s.atimeMu.Lock()
	delete(s.dirtyAtime, ino)
	s.atimeMu.Unlock()
}

// FlushWAL 在存在未落盘 NoSync 写入时执行一次同步 WAL 刷盘（db.LogData(nil, pebble.Sync)），
// 并在 WAL 落盘确认后物理清理挂起的全死封口 .pack 文件（ADR-0031 §4.2）。
func (s *StoreActor) FlushWAL() error {
	if s.readOnly {
		return nil
	}
	unlock, err := s.enter()
	if err != nil {
		return err
	}
	defer unlock()

	toReclaim := s.takePendingDeadPacks()
	wasDirty := s.walDirty.Swap(false)
	if !wasDirty && len(toReclaim) == 0 {
		return nil
	}
	if err := s.db.LogData(nil, pebble.Sync); err != nil {
		s.restorePendingDeadPacks(toReclaim)
		s.walDirty.Store(true)
		return err
	}
	s.removePacks(toReclaim)
	return nil
}

// ReadView 为 Actor Step 2 Phase A 提供纯内存只读预校验视图（底层读取 s.batch 以支持同批写后读，绝不暴露写方法）。
type ReadView struct {
	s *StoreActor
}

// Get 在当前 IndexedBatch 视图上执行点查（包含同批前序命令已写入但尚未 Apply 的数据）。
func (v ReadView) Get(key []byte) ([]byte, io.Closer, error) {
	return v.s.batch.Get(key)
}

// NewIter 在当前 IndexedBatch 视图上创建前缀/范围扫描迭代器。
func (v ReadView) NewIter(opts *pebble.IterOptions) (*pebble.Iterator, error) {
	return v.s.batch.NewIter(opts)
}

// Stat 返回当前批次实时分支统计副本。
func (v ReadView) Stat() BranchStat { return v.s.stat }

// PackStat 返回指定 packID 在当前 Actor 内存中的 PackMeta 统计。
func (v ReadView) PackStat(packID uint64) (PackMeta, bool) {
	meta, ok := v.s.packStats[packID]
	return meta, ok
}

// Now 返回当前时间戳。
func (v ReadView) Now() time.Time { return v.s.now() }

// PrepareIno 检查并按 4096 步长预留 Inode 号段，返回下一个可用 Inode 编号（Phase A 只读预览，不推进游标）。
func (v ReadView) PrepareIno() (uint64, error) { return v.s.prepareIno() }

// CheckQuota 校验新增 bytesDelta 与 inodesDelta 是否超出配额上限。
func (v ReadView) CheckQuota(bytesDelta, inodesDelta uint64) error {
	return v.s.checkQuota(bytesDelta, inodesDelta)
}

// MutateView 为 Actor Step 2 Phase B 提供内存记账与零拷贝批次写入视图。
type MutateView struct {
	s   *StoreActor
	cmd *writeCmd
}

// Batch 返回当前共享 *pebble.IndexedBatch 以供调用 SetDeferred / DeleteDeferred。
func (m MutateView) Batch() *pebble.Batch { return m.s.batch }

// Stat 返回当前 Actor 权威分支统计指针。
func (m MutateView) Stat() *BranchStat { return &m.s.stat }

// NextWriteSeq 递增并返回下一条写序列号。
func (m MutateView) NextWriteSeq() uint64 {
	m.s.stat.NextWriteSeq++
	return m.s.stat.NextWriteSeq
}

// ConsumeIno 消费并推进 Phase A 已预留的本地 Inode 游标（s.nextIno++）。
func (m MutateView) ConsumeIno() uint64 { return m.s.consumeIno() }

// AddInodes 增减分支节点计数。
func (m MutateView) AddInodes(delta int64) {
	if delta >= 0 {
		m.s.stat.Inodes += uint64(delta)
	} else if uint64(-delta) <= m.s.stat.Inodes {
		m.s.stat.Inodes -= uint64(-delta)
	} else {
		m.s.stat.Inodes = 0
	}
}

// AddLogicalBytes 增减分支逻辑字节数。
func (m MutateView) AddLogicalBytes(delta int64) {
	if delta >= 0 {
		m.s.stat.LogicalBytes += uint64(delta)
	} else if uint64(-delta) <= m.s.stat.LogicalBytes {
		m.s.stat.LogicalBytes -= uint64(-delta)
	} else {
		m.s.stat.LogicalBytes = 0
	}
}

// AddBlobRef 在 Phase B 登记新增或更新后的 DataKey 引用块，累加目标 .pack 的 LiveBytes 并更新 TotalBytes。
func (m MutateView) AddBlobRef(ref BlobRef) {
	m.s.addBlobRef(ref)
}

// RemoveBlobRef 在 Phase B 登记被覆盖或删除的旧 DataKey 引用块，扣减目标 .pack 的 LiveBytes。
func (m MutateView) RemoveBlobRef(ref BlobRef) {
	m.s.removeBlobRef(ref)
}

// WriteOp 定义投递至 StoreActor 双阶段 Group Commit 循环的写操作契约。
type WriteOp struct {
	Validate    func(v ReadView) error
	Mutate      func(m MutateView)
	SyncBarrier bool   // 是否作为 Fsync / FsyncDir 持久化屏障批次提交
	SyncIno     uint64 // Fsync 目标文件 Inode（0 表示仅刷 WAL）
}

// Execute 将一条写操作投递至双缓冲 MPSC 队列并等待所属 Group Commit 批次完成。
func (s *StoreActor) Execute(op WriteOp) error {
	if s.readOnly {
		return ErrReadOnly
	}
	unlock, err := s.enter()
	if err != nil {
		return err
	}
	defer unlock()

	cmd := s.acquireCmd()
	cmd.op = opCustom
	cmd.syncBarrier = op.SyncBarrier
	cmd.syncIno = op.SyncIno
	if op.Validate != nil {
		cmd.validate = func(actor *StoreActor, _ *writeCmd) error {
			return op.Validate(ReadView{s: actor})
		}
	}
	if op.Mutate != nil {
		cmd.mutate = func(actor *StoreActor, c *writeCmd) {
			op.Mutate(MutateView{s: actor, cmd: c})
		}
	}
	return s.enqueueAndWait(cmd)
}

func (s *StoreActor) acquireCmd() *writeCmd {
	return s.cmdPool.Get().(*writeCmd)
}

func (s *StoreActor) releaseCmd(cmd *writeCmd) {
	cmd.op = 0
	cmd.req = nil
	cmd.resp = nil
	cmd.err = nil
	cmd.validate = nil
	cmd.mutate = nil
	cmd.syncBarrier = false
	cmd.syncIno = 0
	cmd.pinnedPacks = cmd.pinnedPacks[:0]
	s.cmdPool.Put(cmd)
}

func (s *StoreActor) submit(cmd *writeCmd) error {
	if s.readOnly {
		s.unpinPacks(cmd.pinnedPacks)
		s.releaseCmd(cmd)
		return ErrReadOnly
	}
	unlock, err := s.enter()
	if err != nil {
		s.unpinPacks(cmd.pinnedPacks)
		s.releaseCmd(cmd)
		return err
	}
	defer unlock()
	return s.enqueueAndWait(cmd)
}

func (s *StoreActor) enqueueAndWait(cmd *writeCmd) error {
	s.mb.mu.Lock()
	if s.mb.closed {
		s.mb.mu.Unlock()
		s.unpinPacks(cmd.pinnedPacks)
		s.releaseCmd(cmd)
		return ErrClosed
	}
	s.mb.active = append(s.mb.active, cmd)
	select {
	case s.mb.notify <- struct{}{}:
	default:
	}
	s.mb.mu.Unlock()

	<-cmd.done
	cmdErr := cmd.err
	s.releaseCmd(cmd)
	return cmdErr
}

// prepareIno 在 Phase A 检查本地 Inode 号段余量；若已耗尽则按 4096 步长向分配器预留新号段。
func (s *StoreActor) prepareIno() (uint64, error) {
	if s.nextIno == 0 || s.nextIno > s.reservedIno {
		if err := s.reserveInoSegment(); err != nil {
			return 0, err
		}
	}
	if s.nextIno > uint64(math.MaxInt64) {
		return 0, ErrInodeExhausted
	}
	return s.nextIno, nil
}

func (s *StoreActor) reserveInoSegment() error {
	if s.reserveInode == nil {
		s.setupDefaultInodeAllocator(RootIno)
	}
	first, last, err := s.reserveInode(InodeSegment)
	if err != nil {
		return err
	}
	if first == 0 || first > last || first > uint64(math.MaxInt64) {
		return ErrInodeExhausted
	}
	s.nextIno = first
	s.reservedIno = last
	return nil
}

// consumeIno 在 Phase B 消费当前已预留的 Inode 编号并推进游标。
func (s *StoreActor) consumeIno() uint64 {
	ino := s.nextIno
	s.nextIno++
	return ino
}

func (s *StoreActor) checkQuota(bytesDelta, inodesDelta uint64) error {
	if maxB := s.maxBytes.Load(); maxB > 0 && bytesDelta > 0 {
		if s.stat.LogicalBytes > maxB || bytesDelta > maxB-s.stat.LogicalBytes {
			return ErrNoSpace
		}
	}
	if maxI := s.maxInodes.Load(); maxI > 0 && inodesDelta > 0 {
		if s.stat.Inodes > maxI || inodesDelta > maxI-s.stat.Inodes {
			return ErrNoSpace
		}
	}
	return nil
}

// runLoop 是 StoreActor 单协程写循环。
func (s *StoreActor) runLoop() {
	defer close(s.loopDone)
	for {
		<-s.mb.notify
		for {
			// Step 1：单锁指针交换排空队列（Drain）
			s.mb.mu.Lock()
			if len(s.mb.active) == 0 {
				closed := s.mb.closed
				s.mb.mu.Unlock()
				if closed {
					if !s.discardOnClose.Load() {
						s.closeErr = s.flushOnClose()
					}
					return
				}
				break
			}
			s.mb.active, s.mb.draining = s.mb.draining[:0], s.mb.active
			s.mb.mu.Unlock()

			s.processTick()
		}
	}
}

// processTick 严格执行 ADR-0030 §2.5 与 ADR-0031 §3 的 Step 2 ~ Step 4。
func (s *StoreActor) processTick() {
	cmds := s.mb.draining

	var (
		hasSyncBarrier  bool
		fsyncFileInodes []uint64
		mutated         bool
	)

	// Step 2：逐条两阶段串行演进（Phase A Validate -> Phase B Mutate）
	for _, cmd := range cmds {
		// Phase A — 纯内存只读预校验（绝不触碰 s.batch 写接口）
		if err := s.validateCmd(cmd); err != nil {
			for _, id := range cmd.pinnedPacks {
				if id != 0 {
					if _, ok := s.packStats[id]; !ok {
						s.packStats[id] = PackMeta{}
						s.dirtyPackStats[id] = struct{}{}
					}
				}
			}
			s.unpinPacks(cmd.pinnedPacks)
			cmd.pinnedPacks = cmd.pinnedPacks[:0]
			cmd.err = err
			continue
		}

		// Phase B — 内存记账与原地写入（必定成功）
		prevCount := s.batch.Count()
		prevStat := s.stat
		s.mutateCmd(cmd)
		s.unpinPacks(cmd.pinnedPacks)
		cmd.pinnedPacks = cmd.pinnedPacks[:0]
		if s.batch.Count() > prevCount || s.stat != prevStat || len(s.dirtyPackStats) > 0 {
			mutated = true
		}
		if cmd.syncBarrier || cmd.op == opFsync || cmd.op == opFsyncDir {
			hasSyncBarrier = true
			if cmd.syncIno != 0 && !slices.Contains(fsyncFileInodes, cmd.syncIno) {
				fsyncFileInodes = append(fsyncFileInodes, cmd.syncIno)
			}
		}
	}

	// Step 3：批次收敛与合并提交（Seal & Commit）
	var (
		commitErr error
		deadPacks []uint64
	)
	if mutated || s.batch.Count() > 0 || hasSyncBarrier || len(s.dirtyPackStats) > 0 {
		// 顺带将内存 dirtyAtime 与 dirtyPackStats 合入当前批次
		s.flushDirtyAtimeToBatch(s.batch)
		var wrotePacks bool
		deadPacks, wrotePacks = s.flushDirtyPackStatsToBatch(s.batch)
		if wrotePacks {
			mutated = true
		}
	}
	if mutated {
		// 批次末尾仅写 1 次 "sys:stat"
		batchSetStatDeferred(s.batch, s.stat)
	}
	if s.batch.Count() > 0 {
		if hasSyncBarrier {
			commitErr = s.commitWithSyncBarrier(fsyncFileInodes, deadPacks)
		} else {
			commitErr = s.db.Apply(s.batch, pebble.NoSync)
			if commitErr == nil {
				s.addPendingDeadPacks(deadPacks)
				s.walDirty.Store(true)
			}
		}
		if commitErr == nil {
			s.atomicLogicalBytes.Store(s.stat.LogicalBytes)
			s.atomicInodes.Store(s.stat.Inodes)
		}
	} else if hasSyncBarrier {
		commitErr = s.syncBarrierWithoutBatch(fsyncFileInodes)
	}

	s.tickCount.Add(1)

	// Step 4：防膨胀重置与唤醒调用方（Reset & Wake）
	if len(s.batch.Repr()) > BatchResetMaxReprBytes || s.batch.Count() > BatchResetMaxCount {
		s.batch = s.db.NewIndexedBatch()
		s.batchResetCnt.Add(1)
	} else {
		s.batch.Reset()
	}

	for i, cmd := range cmds {
		if cmd.err == nil && commitErr != nil {
			cmd.err = commitErr
		}
		cmds[i] = nil
		cmd.done <- struct{}{}
	}
}

func (s *StoreActor) validateCmd(cmd *writeCmd) error {
	if cmd.validate != nil {
		return cmd.validate(s, cmd)
	}
	if int(cmd.op) < len(opHandlers) && opHandlers[cmd.op].validate != nil {
		return opHandlers[cmd.op].validate(s, cmd)
	}
	return nil
}

func (s *StoreActor) mutateCmd(cmd *writeCmd) {
	if cmd.mutate != nil {
		cmd.mutate(s, cmd)
		return
	}
	if int(cmd.op) < len(opHandlers) && opHandlers[cmd.op].mutate != nil {
		opHandlers[cmd.op].mutate(s, cmd)
	}
}

func (s *StoreActor) commitWithSyncBarrier(fsyncFileInodes []uint64, deadPacks []uint64) error {
	toReclaim := s.takePendingDeadPacks()
	if err := s.db.ApplyNoSyncWait(s.batch, pebble.Sync); err != nil {
		s.restorePendingDeadPacks(toReclaim)
		return err
	}
	toReclaim = append(toReclaim, deadPacks...)
	var dataErr error
	if s.syncInodeFn != nil {
		for _, ino := range fsyncFileInodes {
			dataErr = errors.Join(dataErr, s.syncInodeFn(ino))
		}
	}
	walErr := s.batch.SyncWait()
	if walErr != nil {
		s.restorePendingDeadPacks(toReclaim)
		s.walDirty.Store(true)
	} else {
		if dataErr == nil {
			s.walDirty.Store(false)
		}
		s.removePacks(toReclaim)
	}
	return errors.Join(dataErr, walErr)
}

func (s *StoreActor) syncBarrierWithoutBatch(fsyncFileInodes []uint64) error {
	var dataErr error
	if s.syncInodeFn != nil {
		for _, ino := range fsyncFileInodes {
			dataErr = errors.Join(dataErr, s.syncInodeFn(ino))
		}
	}
	toReclaim := s.takePendingDeadPacks()
	var walErr error
	if s.walDirty.Swap(false) || len(toReclaim) > 0 {
		walErr = s.db.LogData(nil, pebble.Sync)
		if walErr != nil {
			s.restorePendingDeadPacks(toReclaim)
			s.walDirty.Store(true)
		} else {
			s.removePacks(toReclaim)
		}
	}
	return errors.Join(dataErr, walErr)
}

func (s *StoreActor) addBlobRef(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta := s.packStats[ref.PackID]
	meta.LiveBytes += uint64(ref.Length)
	endOffset := uint64(ref.Offset) + uint64(ref.Length)
	if endOffset > meta.TotalBytes {
		meta.TotalBytes = endOffset
	}
	if meta.LiveBytes > meta.TotalBytes {
		meta.TotalBytes = meta.LiveBytes
	}
	s.packStats[ref.PackID] = meta
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) removeBlobRef(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta, ok := s.packStats[ref.PackID]
	if !ok {
		return
	}
	dec := uint64(ref.Length)
	if dec >= meta.LiveBytes {
		meta.LiveBytes = 0
	} else {
		meta.LiveBytes -= dec
	}
	s.packStats[ref.PackID] = meta
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) activePackID() uint64 {
	if bs := s.Blobs(); bs != nil {
		return bs.ActivePackID()
	}
	return 0
}

func (s *StoreActor) appendBlob(blobs *BlobStore, ino uint64, data []byte, pinned *[]uint64) (BlobRef, error) {
	ref, err := blobs.Append(ino, data)
	if err != nil {
		return BlobRef{}, err
	}
	s.pinPack(ref.PackID, pinned)
	return ref, nil
}

func (s *StoreActor) pinPack(packID uint64, pinned *[]uint64) {
	if packID == 0 {
		return
	}
	s.inFlightMu.Lock()
	s.inFlightPacks[packID]++
	s.inFlightMu.Unlock()
	if pinned != nil {
		*pinned = append(*pinned, packID)
	}
}

func (s *StoreActor) unpinPacks(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, id := range packIDs {
		if c := s.inFlightPacks[id]; c <= 1 {
			delete(s.inFlightPacks, id)
		} else {
			s.inFlightPacks[id] = c - 1
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) isPackPinned(packID uint64) bool {
	s.inFlightMu.Lock()
	pinned := s.inFlightPacks[packID] > 0
	s.inFlightMu.Unlock()
	return pinned
}

func (s *StoreActor) flushDirtyPackStatsToBatch(batch *pebble.Batch) (deadPacks []uint64, wrote bool) {
	activeID := s.activePackID()
	for packID := range s.dirtyPackStats {
		meta, ok := s.packStats[packID]
		if !ok {
			continue
		}
		if meta.LiveBytes == 0 && packID != activeID && !s.isPackPinned(packID) {
			delete(s.packStats, packID)
			batchDeletePackMetaDeferred(batch, packID)
			deadPacks = append(deadPacks, packID)
			wrote = true
			continue
		}
		batchSetPackMetaDeferred(batch, packID, meta)
		wrote = true
	}
	clear(s.dirtyPackStats)

	for packID, meta := range s.packStats {
		if meta.LiveBytes == 0 && packID != activeID && !s.isPackPinned(packID) {
			delete(s.packStats, packID)
			batchDeletePackMetaDeferred(batch, packID)
			deadPacks = append(deadPacks, packID)
			wrote = true
		}
	}
	return deadPacks, wrote
}

func (s *StoreActor) addPendingDeadPacks(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.deadPacksMu.Lock()
	for _, id := range packIDs {
		if id != 0 && !slices.Contains(s.pendingDeadPacks, id) {
			s.pendingDeadPacks = append(s.pendingDeadPacks, id)
		}
	}
	s.deadPacksMu.Unlock()
}

func (s *StoreActor) takePendingDeadPacks() []uint64 {
	s.deadPacksMu.Lock()
	defer s.deadPacksMu.Unlock()
	if len(s.pendingDeadPacks) == 0 {
		return nil
	}
	out := s.pendingDeadPacks
	s.pendingDeadPacks = nil
	return out
}

func (s *StoreActor) restorePendingDeadPacks(packIDs []uint64) {
	s.addPendingDeadPacks(packIDs)
}

func (s *StoreActor) removePacks(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	bs := s.Blobs()
	if bs == nil {
		return
	}
	for _, packID := range packIDs {
		_ = bs.RemovePack(packID)
	}
}

func (s *StoreActor) takeDirtyAtime() map[uint64]int64 {
	s.atimeMu.Lock()
	defer s.atimeMu.Unlock()
	if len(s.dirtyAtime) == 0 {
		return nil
	}
	pending := s.dirtyAtime
	s.dirtyAtime = make(map[uint64]int64)
	return pending
}

func (s *StoreActor) flushDirtyAtimeToBatch(batch *pebble.Batch) bool {
	pending := s.takeDirtyAtime()
	if len(pending) == 0 {
		return false
	}
	wrote := false
	for ino, ns := range pending {
		inode, err := s.readInode(batch, ino)
		if err != nil {
			continue
		}
		if ns <= inode.Atime {
			continue
		}
		inode.Atime = ns
		if err := batchSetInodeDeferred(batch, inode); err != nil {
			continue
		}
		batchSetDirtyDeferred(batch, ino)
		wrote = true
	}
	return wrote
}

func (s *StoreActor) flushOnClose() error {
	var applyErr error
	wroteAtime := s.flushDirtyAtimeToBatch(s.batch)
	deadPacks, wrotePacks := s.flushDirtyPackStatsToBatch(s.batch)
	if wroteAtime || wrotePacks {
		applyErr = s.db.Apply(s.batch, pebble.NoSync)
		s.batch.Reset()
		if applyErr == nil {
			s.addPendingDeadPacks(deadPacks)
			s.walDirty.Store(true)
		}
	}
	toReclaim := s.takePendingDeadPacks()
	var syncErr error
	if s.walDirty.Swap(false) || len(toReclaim) > 0 {
		syncErr = s.db.LogData(nil, pebble.Sync)
	}
	if applyErr == nil && syncErr == nil {
		s.removePacks(toReclaim)
	}
	return errors.Join(applyErr, syncErr)
}

// Close 执行三步无死锁优雅关闭：
//   - Step 1：置 closing=true 拒新，封闭 mb 队列并唤醒 Actor 协程；
//   - Step 2：等待 <-loopDone 排空已入队写批次，刷入残留 dirtyAtime 与 WAL Sync；
//   - Step 3：获取 lifecycleMu.Lock() 排空在途直读后安全关闭底层 *pebble.DB。
func (s *StoreActor) Close() error {
	return s.closeInternal(false)
}

// CloseDiscard 执行销毁型三步关闭（用于 Rollback / DeleteBranch / DeleteDisk）：
// 排空已入队批次后直接跳过尾部 dirtyAtime 与 WAL Sync 刷盘，省去无意义磁盘 fsync。
func (s *StoreActor) CloseDiscard() error {
	return s.closeInternal(true)
}

func (s *StoreActor) closeInternal(discard bool) error {
	s.closeOnce.Do(func() {
		// Step 1 — 拉闸拒新并封闭写队列
		s.discardOnClose.Store(discard)
		s.closing.Store(true)

		s.mb.mu.Lock()
		s.mb.closed = true
		select {
		case s.mb.notify <- struct{}{}:
		default:
		}
		s.mb.mu.Unlock()

		// Step 2 — 排空已入队写批次并等待 Actor 协程退出
		<-s.loopDone

		// Step 3 — 排空在途直读并安全关闭 Pebble 与常驻 .pack 文件句柄
		s.lifecycleMu.Lock()
		if s.batch != nil {
			_ = s.batch.Close()
			s.batch = nil
		}
		dbErr := s.db.Close()
		if s.ownedCache != nil {
			s.ownedCache.Unref()
			s.ownedCache = nil
		}
		var blobErr error
		if bs := s.Blobs(); bs != nil {
			blobErr = bs.Close()
		}
		s.lifecycleMu.Unlock()

		s.closeErr = errors.Join(s.closeErr, dbErr, blobErr)
	})
	return s.closeErr
}
