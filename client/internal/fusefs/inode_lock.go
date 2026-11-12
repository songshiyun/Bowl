package fusefs

import (
	"sync"
	"sync/atomic"
	"time"
)

// inodeLocks 提供按 Inode 粒度的轻量互斥锁，避免用全局锁串行化无关节点的跨机 RPC。
// 仅在并发 O_APPEND 定序与 Unlink/Release 孤儿标记等需要同一节点串行的场景使用。
type inodeLocks struct {
	mu      sync.Mutex
	entries map[uint64]*inodeLockEntry
	metrics atomic.Pointer[Metrics]
}

type inodeLockEntry struct {
	mu   sync.Mutex
	refs int
}

func (l *inodeLocks) bindMetrics(m *Metrics) {
	l.metrics.Store(m)
}

func (l *inodeLocks) activeEntries() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.entries)
}

// lock 获取指定 Inode 的互斥锁，返回幂等的释放函数。
func (l *inodeLocks) lock(ino uint64) func() {
	l.mu.Lock()
	if l.entries == nil {
		l.entries = make(map[uint64]*inodeLockEntry)
	}
	entry := l.entries[ino]
	if entry == nil {
		entry = &inodeLockEntry{}
		l.entries[ino] = entry
	}
	entry.refs++
	l.mu.Unlock()

	if entry.mu.TryLock() {
		l.metrics.Load().incInodeLockFast()
	} else {
		waitStart := time.Now()
		entry.mu.Lock()
		l.metrics.Load().observeInodeLockContended(time.Since(waitStart))
	}
	return func() {
		entry.mu.Unlock()
		l.mu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(l.entries, ino)
		}
		l.mu.Unlock()
	}
}
