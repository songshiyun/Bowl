package pebblekv

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

var (
	// ErrBusy 表示仍有句柄占用实例。
	ErrBusy = errors.New("pebblekv: 实例正在使用")
	// ErrClosed 表示管理器或句柄已经关闭。
	ErrClosed = errors.New("pebblekv: 已关闭")
	// ErrInvalidID 表示标识符不能作为单个目录名。
	ErrInvalidID = errors.New("pebblekv: 无效标识符")
	// ErrInodeExhausted 表示该 Disk 的 Inode 编号已用尽。
	ErrInodeExhausted = errors.New("pebblekv: Inode 编号已用尽")
)

const (
	// inodeSegment 是一次持久化预留的 Inode 号段长度，段内发号不再产生 I/O。
	inodeSegment = 4096
	// inodeKey 保存已持久化的 Inode 分配上界。
	inodeKey = "last-inode"
	// blobIDKey 保存已持久化的 Blob 编号分配上界。
	blobIDKey = "last-blob-id"
	// inodeAllocatorKind 与 blobAllocatorKind 是盘级分配器数据库目录名。
	inodeAllocatorKind = "inode-allocator"
	blobAllocatorKind  = "blob-allocator"

	// 常驻实例的内存表与文件句柄上限，兼顾多分支并发与进程内存占用。
	// Pebble 仅承载纯元数据与 12 字节 BlobRef 索引，1 MiB 数据块写入独立 .blob 文件。
	memTableSize = 16 << 20
	maxOpenFiles = 128
	// maxIdleHandles 是句柄池保留的空闲实例上限，超出后按 LRU 关闭。
	maxIdleHandles = 32
	// sharedCacheSize 是进程内所有实例共享的块缓存容量。
	sharedCacheSize = 64 << 20
	// allocatorMemTableSize 是编号分配器小型数据库的内存表上限。
	allocatorMemTableSize = 256 << 10
	// walBytesPerSync 控制单个 WAL 累计写入多少字节后强制同步一次。
	walBytesPerSync = 4 << 20
	// defaultWALFlushInterval 是后台兜底刷盘周期，决定断电时的最大丢失窗口。
	defaultWALFlushInterval = time.Second
)

// 暂存目录使用不入池的短命句柄，关闭句柄即释放底层实例。
var stagingPrefixes = []string{".fork-", ".merge-", ".restore-"}

type instanceID struct{ disk, id string }

type allocatorID struct{ disk, kind string }

// pooledDB 是句柄池中的一个常驻实例。
type pooledDB struct {
	db *pebble.DB
	// blobs 管理该分支或快照目录下的不可变块文件。
	blobs    *BlobStore
	refs     int
	lastUsed uint64
	// mu 串行化底层实例的关闭与后台 WAL 刷盘，避免对已关闭实例发起写入。
	mu     sync.Mutex
	closed bool
	// dirty 表示该分支自上次兜底刷盘后仍有未同步的写入。
	dirty bool
}

// Stats 是句柄池的累计打开与复用次数，用于诊断与测试。
type Stats struct {
	BranchOpens      int64
	BranchReuses     int64
	CommitOpens      int64
	CommitReuses     int64
	ResidentBranches int
	ResidentCommits  int
	// WALFlushes 是后台兜底刷盘的累计次数。
	WALFlushes int64
}

// Manager 管理本进程内的分支和快照实例。同一存储根目录必须共用一个 Manager。
// 分支与快照实例按引用计数常驻句柄池，调用方应在结束使用后关闭所有句柄。
type Manager struct {
	root       string
	cache      *pebble.Cache
	mu         sync.Mutex
	closed     bool
	clock      uint64
	stats      Stats
	branches   map[instanceID]*pooledDB
	commits    map[instanceID]*pooledDB
	allocators map[allocatorID]*inodeAllocator

	flushInterval time.Duration
	stopFlush     chan struct{}
	flushDone     chan struct{}
	stopOnce      sync.Once
}

// Option 配置句柄池管理器。
type Option func(*Manager)

// WithWALFlushInterval 覆盖后台兜底刷盘周期，主要用于测试缩短等待时间。
func WithWALFlushInterval(interval time.Duration) Option {
	return func(m *Manager) {
		if interval > 0 {
			m.flushInterval = interval
		}
	}
}

// NewManager 创建存储实例管理器，并启动后台异步 WAL 兜底刷盘协程。
func NewManager(root string, opts ...Option) *Manager {
	m := &Manager{
		root: root, cache: pebble.NewCache(sharedCacheSize),
		branches: make(map[instanceID]*pooledDB), commits: make(map[instanceID]*pooledDB),
		allocators:    make(map[allocatorID]*inodeAllocator),
		flushInterval: defaultWALFlushInterval, stopFlush: make(chan struct{}), flushDone: make(chan struct{}),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	go m.walFlushLoop()
	return m
}

// walFlushLoop 周期把有脏写的分支 WAL 同步到磁盘，收敛机器断电的丢失窗口。
func (m *Manager) walFlushLoop() {
	defer close(m.flushDone)
	ticker := time.NewTicker(m.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-m.stopFlush:
			return
		case <-ticker.C:
			m.FlushWAL()
		}
	}
}

// FlushWAL 对池内有新写入的可写分支执行一次同步 WAL 刷盘。
// 显式持久化屏障（Fsync / FsyncDir / Commit）不依赖本方法。
func (m *Manager) FlushWAL() {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return
	}
	var targets []*pooledDB
	for _, entry := range m.branches {
		if !entry.dirty {
			continue
		}
		// 先清除标记：刷盘期间的新写入会重新置脏，下一周期继续兜底。
		entry.dirty = false
		targets = append(targets, entry)
	}
	m.mu.Unlock()
	for _, entry := range targets {
		entry.mu.Lock()
		if entry.closed {
			entry.mu.Unlock()
			continue
		}
		// 先确认块文件落盘，再同步指向它们的元数据 WAL。
		blobErr := entry.blobs.Sync()
		var err error
		if blobErr == nil {
			err = entry.db.LogData(nil, pebble.Sync)
		}
		entry.mu.Unlock()
		if err := errors.Join(blobErr, err); err != nil {
			if !errors.Is(err, ErrClosed) {
				slog.Error("后台同步分支 WAL 失败", "error", err)
			}
			m.mu.Lock()
			entry.dirty = true
			m.mu.Unlock()
			continue
		}
		m.mu.Lock()
		m.stats.WALFlushes++
		m.mu.Unlock()
	}
}

// MarkBranchDirty 标记常驻分支自上次兜底刷盘后仍有未同步写入。
func (m *Manager) MarkBranchDirty(disk, branch string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry := m.branches[instanceID{disk, branch}]; entry != nil {
		entry.dirty = true
	}
}

// MarkBranchClean 在显式持久化屏障完成后清除脏写标记，避免重复同步。
func (m *Manager) MarkBranchClean(disk, branch string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if entry := m.branches[instanceID{disk, branch}]; entry != nil {
		entry.dirty = false
	}
}

func (m *Manager) options(readOnly bool) *pebble.Options {
	return &pebble.Options{
		ReadOnly: readOnly, ErrorIfNotExists: readOnly,
		FormatMajorVersion: pebble.FormatNewest,
		Cache:              m.cache, MemTableSize: memTableSize, MaxOpenFiles: maxOpenFiles,
		WALBytesPerSync: walBytesPerSync,
	}
}

func isStaging(id string) bool {
	for _, prefix := range stagingPrefixes {
		if strings.HasPrefix(id, prefix) {
			return true
		}
	}
	return false
}

func validID(id string) bool {
	if id == "" || id == "." || id == ".." || strings.ContainsRune(id, '\x00') {
		return false
	}
	for i := range len(id) {
		if os.IsPathSeparator(id[i]) {
			return false
		}
	}
	return true
}
func (m *Manager) path(disk, kind, id string) (string, error) {
	if !validID(disk) || !validID(id) {
		return "", ErrInvalidID
	}
	return filepath.Join(m.root, "disks", disk, kind, id), nil
}

func (m *Manager) tick() uint64 {
	m.clock++
	return m.clock
}

// Stats 返回句柄池的累计打开与复用次数。
func (m *Manager) Stats() Stats {
	m.mu.Lock()
	defer m.mu.Unlock()
	stats := m.stats
	stats.ResidentBranches, stats.ResidentCommits = len(m.branches), len(m.commits)
	return stats
}

// close 关闭底层实例并与后台 WAL 刷盘互斥，重复关闭是幂等的。
func (entry *pooledDB) close() error {
	entry.mu.Lock()
	defer entry.mu.Unlock()
	if entry.closed {
		return nil
	}
	entry.closed = true
	return entry.db.Close()
}

// busyLocked 报告是否仍有未释放的句柄引用。
func (m *Manager) busyLocked() error {
	for _, entry := range m.branches {
		if entry.refs != 0 {
			return ErrBusy
		}
	}
	for _, entry := range m.commits {
		if entry.refs != 0 {
			return ErrBusy
		}
	}
	return nil
}

// evictIdleLocked 按最近使用时间关闭超出上限的空闲实例。
func (m *Manager) evictIdleLocked() {
	for {
		idle, oldest, key, branch := 0, (*pooledDB)(nil), instanceID{}, false
		consider := func(candidate instanceID, entry *pooledDB, isBranch bool) {
			// 脏写分支等待后台兜底刷盘后再驱逐，避免关闭实例时丢失未同步的 WAL。
			if entry.refs != 0 || entry.dirty {
				return
			}
			idle++
			if oldest == nil || entry.lastUsed < oldest.lastUsed {
				oldest, key, branch = entry, candidate, isBranch
			}
		}
		for key, entry := range m.branches {
			consider(key, entry, true)
		}
		for key, entry := range m.commits {
			consider(key, entry, false)
		}
		if idle <= maxIdleHandles || oldest == nil {
			return
		}
		_ = oldest.close()
		if branch {
			delete(m.branches, key)
		} else {
			delete(m.commits, key)
		}
	}
}

// evictDiskLocked 关闭指定 Disk 下所有空闲实例，仍有引用时返回 ErrBusy。
func (m *Manager) evictDiskLocked(disk string) error {
	for key, entry := range m.branches {
		if key.disk == disk && entry.refs != 0 {
			return ErrBusy
		}
	}
	for key, entry := range m.commits {
		if key.disk == disk && entry.refs != 0 {
			return ErrBusy
		}
	}
	var errs []error
	for key, entry := range m.branches {
		if key.disk == disk {
			errs = append(errs, entry.close())
			delete(m.branches, key)
		}
	}
	for key, entry := range m.commits {
		if key.disk == disk {
			errs = append(errs, entry.close())
			delete(m.commits, key)
		}
	}
	return errors.Join(errs...)
}

// Branch 是可写分支的独占实例句柄。
type Branch struct {
	manager   *Manager
	key       instanceID
	entry     *pooledDB
	db        *pebble.DB
	blobs     *BlobStore
	ephemeral bool
	closed    bool
}

// DB 返回分支的 Pebble 实例。调用方不得直接关闭实例，也不得在句柄关闭后使用它。
func (b *Branch) DB() *pebble.DB { return b.db }

// Blobs 返回分支目录下的不可变块文件存储。
func (b *Branch) Blobs() *BlobStore { return b.blobs }

// OpenBranch 打开或创建分支；真实分支复用常驻实例，暂存目录使用短命句柄。
func (m *Manager) OpenBranch(disk, branch string) (*Branch, error) {
	path, err := m.path(disk, "branches", branch)
	if err != nil {
		return nil, err
	}
	key := instanceID{disk, branch}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if isStaging(branch) {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, err
		}
		db, err := pebble.Open(path, m.options(false))
		if err != nil {
			return nil, err
		}
		return &Branch{manager: m, key: key, db: db, blobs: NewBlobStore(path), ephemeral: true}, nil
	}
	if entry := m.branches[key]; entry != nil {
		entry.refs++
		entry.lastUsed = m.tick()
		m.stats.BranchReuses++
		return &Branch{manager: m, key: key, entry: entry, db: entry.db, blobs: entry.blobs}, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := pebble.Open(path, m.options(false))
	if err != nil {
		return nil, err
	}
	entry := &pooledDB{db: db, blobs: NewBlobStore(path), refs: 1, lastUsed: m.tick()}
	m.branches[key] = entry
	m.stats.BranchOpens++
	m.evictIdleLocked()
	return &Branch{manager: m, key: key, entry: entry, db: db, blobs: entry.blobs}, nil
}

// Checkpoint 刷新分支并以硬链接生成不可覆盖的 Commit 快照。
func (b *Branch) Checkpoint(commit string) error {
	m := b.manager
	path, err := m.path(b.key.disk, "commits", commit)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed || b.closed {
		return ErrClosed
	}
	if err := b.blobs.Sync(); err != nil {
		return err
	}
	if err := b.db.Flush(); err != nil {
		return err
	}
	if err := b.db.Checkpoint(path, pebble.WithFlushedWAL()); err != nil {
		return err
	}
	// 快照目录与分支工作区共享同一批不可变块文件，未修改块零拷贝。
	return b.blobs.LinkAllTo(NewBlobStore(path))
}

// Close 释放分支句柄；常驻实例只递减引用计数。
func (b *Branch) Close() error {
	if b.closed {
		return nil
	}
	b.closed = true
	if b.ephemeral {
		return b.db.Close()
	}
	m := b.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if b.entry.refs > 0 {
		b.entry.refs--
	}
	b.entry.lastUsed = m.tick()
	m.evictIdleLocked()
	return nil
}

// Commit 是共享只读快照的引用。
type Commit struct {
	manager *Manager
	key     instanceID
	entry   *pooledDB
	blobs   *BlobStore
	closed  bool
}

// DB 返回只读 Pebble 实例。调用方不得直接关闭实例，也不得在句柄关闭后使用它。
func (c *Commit) DB() *pebble.DB { return c.entry.db }

// Blobs 返回快照目录下的不可变块文件存储。
func (c *Commit) Blobs() *BlobStore { return c.blobs }

// OpenCommit 共享同一 Disk 和 Commit 的只读实例。
func (m *Manager) OpenCommit(disk, commit string) (*Commit, error) {
	path, err := m.path(disk, "commits", commit)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	key := instanceID{disk, commit}
	entry := m.commits[key]
	if entry == nil {
		db, err := pebble.Open(path, m.options(true))
		if err != nil {
			return nil, err
		}
		entry = &pooledDB{db: db, blobs: NewBlobStore(path)}
		m.commits[key] = entry
		m.stats.CommitOpens++
	} else {
		m.stats.CommitReuses++
	}
	entry.refs++
	entry.lastUsed = m.tick()
	m.evictIdleLocked()
	return &Commit{manager: m, key: key, entry: entry, blobs: entry.blobs}, nil
}

// Close 释放快照引用；常驻实例只递减引用计数。
func (c *Commit) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	m := c.manager
	m.mu.Lock()
	defer m.mu.Unlock()
	if c.entry.refs > 0 {
		c.entry.refs--
	}
	c.entry.lastUsed = m.tick()
	m.evictIdleLocked()
	return nil
}

// EvictBranch 在持有分支独占写锁且没有活动引用时关闭常驻分支实例。
func (m *Manager) EvictBranch(disk, branch string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := instanceID{disk, branch}
	entry := m.branches[key]
	if entry == nil {
		return nil
	}
	if entry.refs != 0 {
		return ErrBusy
	}
	delete(m.branches, key)
	return entry.close()
}

// EvictCommit 在没有活动引用时关闭常驻快照实例。
func (m *Manager) EvictCommit(disk, commit string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	key := instanceID{disk, commit}
	entry := m.commits[key]
	if entry == nil {
		return nil
	}
	if entry.refs != 0 {
		return ErrBusy
	}
	delete(m.commits, key)
	return entry.close()
}

// EvictDisk 关闭指定 Disk 的全部常驻实例并清理内存分配器状态。
func (m *Manager) EvictDisk(disk string) error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return ErrClosed
	}
	if err := m.evictDiskLocked(disk); err != nil {
		m.mu.Unlock()
		return err
	}
	var allocators []*inodeAllocator
	for id, allocator := range m.allocators {
		if id.disk == disk {
			allocators = append(allocators, allocator)
			delete(m.allocators, id)
		}
	}
	m.mu.Unlock()
	var errs []error
	for _, allocator := range allocators {
		errs = append(errs, allocator.close())
	}
	return errors.Join(errs...)
}

// Fork 从不可变快照生成新的分支工作区。
func (m *Manager) Fork(disk, commit, branch string) (err error) {
	path, err := m.path(disk, "branches", branch)
	if err != nil {
		return err
	}
	snapshot, err := m.OpenCommit(disk, commit)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, snapshot.Close()) }()
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.branches[instanceID{disk, branch}]; exists {
		return ErrBusy
	}
	if _, statErr := os.Stat(path); statErr == nil {
		return ErrBusy
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return statErr
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	staging, err := os.MkdirTemp(filepath.Dir(path), ".fork-")
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, os.RemoveAll(staging)) }()
	source, _ := m.path(disk, "commits", commit)
	if err := stageCheckpoint(source, staging); err != nil {
		return err
	}
	opts := m.options(false)
	opts.ErrorIfNotExists = true
	db, err := pebble.Open(staging, opts)
	if err != nil {
		return err
	}
	if err := db.Flush(); err != nil {
		return errors.Join(err, db.Close())
	}
	checkpointErr := db.Checkpoint(path, pebble.WithFlushedWAL())
	if err := errors.Join(checkpointErr, db.Close()); err != nil {
		return err
	}
	// 快照内未修改的块通过硬链接共享到新分支，保持零数据复制。
	return NewBlobStore(source).LinkAllTo(NewBlobStore(path))
}

// 只读 Pebble 实例无法调用 Checkpoint；先复制控制文件并硬链接不可变表。
// blobs 目录由 Fork 在检查点完成后单独硬链接，避免与 Pebble 表混在一起复制。
func stageCheckpoint(source, destination string) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "LOCK" || entry.Name() == "blobs" {
			continue
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("快照包含非普通文件 %s", entry.Name())
		}
		from, to := filepath.Join(source, entry.Name()), filepath.Join(destination, entry.Name())
		if strings.HasSuffix(entry.Name(), ".sst") {
			if err := os.Link(from, to); err != nil {
				return err
			}
			continue
		}
		in, err := os.Open(from)
		if err != nil {
			return err
		}
		out, err := os.OpenFile(to, os.O_WRONLY|os.O_CREATE|os.O_EXCL, info.Mode().Perm())
		if err != nil {
			_ = in.Close()
			return err
		}
		_, copyErr := io.Copy(out, in)
		if err := errors.Join(copyErr, in.Close(), out.Close()); err != nil {
			return err
		}
	}
	return nil
}

// inodeAllocator 是按 Disk 独立的编号号段分配器，段内发号不产生 I/O。
type inodeAllocator struct {
	mu       sync.Mutex
	db       *pebble.DB
	key      string
	initial  uint64
	next     uint64
	reserved uint64
	closed   bool
}

func (a *inodeAllocator) close() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil
	}
	a.closed = true
	return a.db.Close()
}

// readLastLocked 返回已持久化的分配上界。
func (a *inodeAllocator) readLastLocked() (uint64, bool, error) {
	value, closer, err := a.db.Get([]byte(a.key))
	if errors.Is(err, pebble.ErrNotFound) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	if len(value) != 8 {
		return 0, false, errors.Join(ErrInvalidValue, closer.Close())
	}
	last := binary.BigEndian.Uint64(value)
	return last, true, closer.Close()
}

// reserveLocked 持久化新的号段上界，随后段内发号完全在内存完成。
func (a *inodeAllocator) reserveLocked() error {
	last, exists, err := a.readLastLocked()
	if err != nil {
		return err
	}
	if !exists {
		last = a.initial
	}
	if last == math.MaxUint64 {
		return ErrInodeExhausted
	}
	reserved := last + inodeSegment
	if reserved < last {
		reserved = math.MaxUint64
	}
	if err := a.db.Set([]byte(a.key), binary.BigEndian.AppendUint64(nil, reserved), pebble.Sync); err != nil {
		return err
	}
	a.next, a.reserved = last+1, reserved
	return nil
}

func (a *inodeAllocator) allocate() (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return 0, ErrClosed
	}
	if a.next == 0 || a.next > a.reserved {
		if err := a.reserveLocked(); err != nil {
			return 0, err
		}
	}
	ino := a.next
	a.next++
	return ino, nil
}

func (a *inodeAllocator) init() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return ErrClosed
	}
	last, exists, err := a.readLastLocked()
	if err != nil {
		return err
	}
	if !exists {
		return a.db.Set([]byte(a.key), binary.BigEndian.AppendUint64(nil, a.initial), pebble.Sync)
	}
	if last < a.initial {
		return ErrInvalidValue
	}
	return nil
}

// allocator 返回 Disk 指定用途的常驻号段分配器，首次使用时打开其小型数据库。
func (m *Manager) allocator(disk, kind string, initial uint64) (*inodeAllocator, error) {
	path, err := m.path(disk, "", kind)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil, ErrClosed
	}
	if allocator := m.allocators[allocatorID{disk, kind}]; allocator != nil {
		return allocator, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	db, err := pebble.Open(path, &pebble.Options{Cache: m.cache, MemTableSize: allocatorMemTableSize, MaxOpenFiles: 16})
	if err != nil {
		return nil, err
	}
	key := inodeKey
	if kind == blobAllocatorKind {
		key = blobIDKey
	}
	allocator := &inodeAllocator{db: db, key: key, initial: initial}
	m.allocators[allocatorID{disk, kind}] = allocator
	return allocator, nil
}

// AllocateInode 在 Disk 级数据库按号段持久化上界，段内发号不随分支和快照回退。
func (m *Manager) AllocateInode(disk string) (uint64, error) {
	allocator, err := m.allocator(disk, inodeAllocatorKind, 1)
	if err != nil {
		return 0, err
	}
	return allocator.allocate()
}

// AllocateBlobID 在 Disk 级数据库按号段持久化上界，保证块编号在整棵树内单调唯一。
func (m *Manager) AllocateBlobID(disk string) (uint64, error) {
	allocator, err := m.allocator(disk, blobAllocatorKind, 0)
	if err != nil {
		return 0, err
	}
	return allocator.allocate()
}

// InitInodeAllocator 持久化根节点编号，已有分配器不会被重置。
func (m *Manager) InitInodeAllocator(disk string) error {
	allocator, err := m.allocator(disk, inodeAllocatorKind, 1)
	if err != nil {
		return err
	}
	return allocator.init()
}

// RemoveDisk 关闭指定 Disk 的常驻实例与编号分配器，然后删除磁盘目录。
func (m *Manager) RemoveDisk(disk string) error {
	path, err := m.path(disk, "", "inode-allocator")
	if err != nil {
		return err
	}
	if err := m.EvictDisk(disk); err != nil {
		return err
	}
	return os.RemoveAll(filepath.Dir(path))
}

// Close 在所有分支及快照句柄关闭后释放管理器。
func (m *Manager) Close() error {
	m.mu.Lock()
	if m.closed {
		m.mu.Unlock()
		return nil
	}
	if err := m.busyLocked(); err != nil {
		m.mu.Unlock()
		return err
	}
	m.mu.Unlock()
	m.stopOnce.Do(func() { close(m.stopFlush) })
	<-m.flushDone
	// 停机前同步所有脏写分支，避免关闭实例时丢失未刷盘的 WAL。
	m.FlushWAL()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return nil
	}
	if err := m.busyLocked(); err != nil {
		return err
	}
	m.closed = true
	var errs []error
	for key, entry := range m.branches {
		errs = append(errs, entry.close())
		delete(m.branches, key)
	}
	for key, entry := range m.commits {
		errs = append(errs, entry.close())
		delete(m.commits, key)
	}
	for id, allocator := range m.allocators {
		if err := allocator.close(); err != nil {
			errs = append(errs, fmt.Errorf("关闭磁盘 %s 的编号分配器: %w", id.disk, err))
		}
		delete(m.allocators, id)
	}
	m.cache.Unref()
	return errors.Join(errs...)
}
