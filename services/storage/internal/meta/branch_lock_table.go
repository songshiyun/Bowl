package meta

import (
	"sort"
	"sync"
)

type branchLockKey struct{ disk, branch string }

type inodeLockKey struct {
	disk, branch string
	ino          uint64
}

// inodeLockEntry 是带引用计数的 Inode 锁，refs 归零后从锁表移除，避免海量 Inode 场景无界增长。
type inodeLockEntry struct {
	mu   sync.RWMutex
	refs int
}

// BranchLockTable 统一管理分支拓扑生命周期读写锁、分支元数据批次互斥锁及带引用计数的 Inode 读写锁。
type BranchLockTable struct {
	branchLocks     sync.Map
	branchMetaLocks sync.Map
	inodeLocksMu    sync.Mutex
	inodeLocks      map[inodeLockKey]*inodeLockEntry
}

// NewBranchLockTable 创建分支与节点锁表。
func NewBranchLockTable() *BranchLockTable {
	return &BranchLockTable{
		inodeLocks: make(map[inodeLockKey]*inodeLockEntry),
	}
}

func (t *BranchLockTable) branchLock(disk, branch string) *sync.RWMutex {
	value, _ := t.branchLocks.LoadOrStore(branchLockKey{disk, branch}, &sync.RWMutex{})
	return value.(*sync.RWMutex)
}

// LockBranchRead 取得分支共享读锁，允许同分支无限并发读取。
func (t *BranchLockTable) LockBranchRead(disk, branch string) func() {
	mu := t.branchLock(disk, branch)
	mu.RLock()
	return mu.RUnlock
}

// LockBranchWrite 取得分支拓扑生命周期独占锁，仅用于替换或冻结整分支目录。
func (t *BranchLockTable) LockBranchWrite(disk, branch string) func() {
	mu := t.branchLock(disk, branch)
	mu.Lock()
	return mu.Unlock
}

// LockBranchMeta 串行化同分支的元数据批次，避免共享 BranchStat 丢失更新。
func (t *BranchLockTable) LockBranchMeta(disk, branch string) func() {
	value, _ := t.branchMetaLocks.LoadOrStore(branchLockKey{disk, branch}, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

// acquireInodeLock 取得锁条目并增加引用计数；调用方解锁后必须调用 releaseInodeLock。
func (t *BranchLockTable) acquireInodeLock(disk, branch string, ino uint64) (inodeLockKey, *inodeLockEntry) {
	key := inodeLockKey{disk, branch, ino}
	t.inodeLocksMu.Lock()
	if t.inodeLocks == nil {
		t.inodeLocks = make(map[inodeLockKey]*inodeLockEntry)
	}
	entry := t.inodeLocks[key]
	if entry == nil {
		entry = &inodeLockEntry{}
		t.inodeLocks[key] = entry
	}
	entry.refs++
	t.inodeLocksMu.Unlock()
	return key, entry
}

// releaseInodeLock 减少引用计数，归零时回收锁条目；此时已无人持有底层互斥量。
func (t *BranchLockTable) releaseInodeLock(key inodeLockKey, entry *inodeLockEntry) {
	t.inodeLocksMu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(t.inodeLocks, key)
	}
	t.inodeLocksMu.Unlock()
}

// LockInodeRead 取得单个 Inode 的共享读锁，防止并发覆写删除正在读取的块文件。
func (t *BranchLockTable) LockInodeRead(disk, branch string, ino uint64) func() {
	key, entry := t.acquireInodeLock(disk, branch, ino)
	entry.mu.RLock()
	return func() {
		entry.mu.RUnlock()
		t.releaseInodeLock(key, entry)
	}
}

// LockInodeWrite 取得单个 Inode 的独占写锁。
func (t *BranchLockTable) LockInodeWrite(disk, branch string, ino uint64) func() {
	key, entry := t.acquireInodeLock(disk, branch, ino)
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		t.releaseInodeLock(key, entry)
	}
}

// LockInodesWrite 按固定 ino 升序对多个 Inode 加独占锁，忽略 0 与重复编号，避免目录操作死锁。
func (t *BranchLockTable) LockInodesWrite(disk, branch string, inos ...uint64) func() {
	ordered := make([]uint64, 0, len(inos))
	seen := make(map[uint64]struct{}, len(inos))
	for _, ino := range inos {
		if ino == 0 {
			continue
		}
		if _, ok := seen[ino]; ok {
			continue
		}
		seen[ino] = struct{}{}
		ordered = append(ordered, ino)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	type heldLock struct {
		key   inodeLockKey
		entry *inodeLockEntry
	}
	locks := make([]heldLock, 0, len(ordered))
	for _, ino := range ordered {
		key, entry := t.acquireInodeLock(disk, branch, ino)
		entry.mu.Lock()
		locks = append(locks, heldLock{key: key, entry: entry})
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].entry.mu.Unlock()
			t.releaseInodeLock(locks[i].key, locks[i].entry)
		}
	}
}

// ActiveInodeLocks 返回当前持有引用计数的 Inode 锁条目数。
func (t *BranchLockTable) ActiveInodeLocks() int {
	t.inodeLocksMu.Lock()
	defer t.inodeLocksMu.Unlock()
	return len(t.inodeLocks)
}

// HasBranchLocks 报告指定分支在拓扑锁表或元数据锁表中是否仍存留条目。
func (t *BranchLockTable) HasBranchLocks(disk, branch string) bool {
	key := branchLockKey{disk, branch}
	if _, ok := t.branchLocks.Load(key); ok {
		return true
	}
	if _, ok := t.branchMetaLocks.Load(key); ok {
		return true
	}
	return false
}

// EvictBranch 清理已删除分支的拓扑锁与元数据锁条目。
func (t *BranchLockTable) EvictBranch(disk, branch string) {
	key := branchLockKey{disk, branch}
	t.branchLocks.Delete(key)
	t.branchMetaLocks.Delete(key)
}

// EvictDisk 清理已删除磁盘下所有分支的拓扑锁与元数据锁条目。
func (t *BranchLockTable) EvictDisk(disk string) {
	t.branchLocks.Range(func(k, _ any) bool {
		if key, ok := k.(branchLockKey); ok && key.disk == disk {
			t.branchLocks.Delete(key)
		}
		return true
	})
	t.branchMetaLocks.Range(func(k, _ any) bool {
		if key, ok := k.(branchLockKey); ok && key.disk == disk {
			t.branchMetaLocks.Delete(key)
		}
		return true
	})
}
