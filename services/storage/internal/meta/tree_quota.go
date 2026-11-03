package meta

import (
	"strings"
	"sync"
)

// TreeQuotaUsage 是整棵分支树去重后的逻辑用量。
type TreeQuotaUsage struct {
	Bytes  uint64
	Inodes uint64
}

// TreeQuota 串行协调一块磁盘的各分支用量。
type TreeQuota struct {
	mu              sync.Mutex
	maxBytes        uint64
	branches        map[string]map[uint64]uint64
	inodes          map[uint64]map[string]uint64
	usage           TreeQuotaUsage
	committed       TreeQuotaUsage
	nextReservation uint64
	closed          bool
}

// NewTreeQuota 创建指定容量的配额协调器。
func NewTreeQuota(maxBytes uint64) *TreeQuota {
	return &TreeQuota{maxBytes: maxBytes, branches: make(map[string]map[uint64]uint64), inodes: make(map[uint64]map[string]uint64)}
}

// Usage 返回已提交的去重用量。
func (q *TreeQuota) Usage() TreeQuotaUsage {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.committed
}

// Apply 在配额锁内执行持久化；失败时保持原有用量。回调不能重入此协调器。
func (q *TreeQuota) Apply(branch string, ino, size uint64, live bool, persist func() error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.applyLocked(branch, ino, size, live, persist, true)
}

func (q *TreeQuota) applyLocked(branch string, ino, size uint64, live bool, persist func() error, enforce bool) error {
	if q.closed {
		return ErrNotFound
	}
	values := q.inodes[ino]
	var oldMax, newMax uint64
	var oldCommittedMax, newCommittedMax uint64
	var oldCommitted, newCommitted bool
	for name, value := range values {
		oldMax = max(oldMax, value)
		if name != branch {
			newMax = max(newMax, value)
		}
		if !strings.HasPrefix(name, "\x00reservation:") {
			oldCommitted = true
			oldCommittedMax = max(oldCommittedMax, value)
			if name != branch {
				newCommitted = true
				newCommittedMax = max(newCommittedMax, value)
			}
		}
	}
	if live {
		newMax = max(newMax, size)
		if !strings.HasPrefix(branch, "\x00reservation:") {
			newCommitted = true
			newCommittedMax = max(newCommittedMax, size)
		}
	}
	base := q.usage.Bytes - oldMax
	if newMax > ^uint64(0)-base {
		return ErrNoSpace
	}
	newUsed := base + newMax
	if enforce && newUsed > q.maxBytes && newUsed > q.usage.Bytes {
		return ErrNoSpace
	}
	if persist != nil {
		if err := persist(); err != nil {
			return err
		}
	}
	if live {
		if q.branches[branch] == nil {
			q.branches[branch] = make(map[uint64]uint64)
		}
		if values == nil {
			values = make(map[string]uint64)
			q.inodes[ino] = values
			q.usage.Inodes++
		}
		q.branches[branch][ino] = size
		values[branch] = size
	} else if values != nil {
		delete(q.branches[branch], ino)
		delete(values, branch)
		if len(values) == 0 {
			delete(q.inodes, ino)
			q.usage.Inodes--
		}
	}
	q.usage.Bytes = newUsed
	q.committed.Bytes = q.committed.Bytes - oldCommittedMax + newCommittedMax
	if oldCommitted != newCommitted {
		if newCommitted {
			q.committed.Inodes++
		} else {
			q.committed.Inodes--
		}
	}
	return nil
}

func (q *TreeQuota) ReplaceBranch(branch string, sizes map[uint64]uint64) error {
	return q.replaceBranch(branch, sizes, true)
}

func (q *TreeQuota) replaceBranch(branch string, sizes map[uint64]uint64, enforce bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.replaceBranchLocked(branch, sizes, enforce)
}

func (q *TreeQuota) replaceBranchLocked(branch string, sizes map[uint64]uint64, enforce bool) error {
	if q.closed {
		return ErrNotFound
	}

	combined := make(map[uint64]uint64)
	committed := make(map[uint64]uint64)
	for name, nodes := range q.branches {
		if name == branch {
			continue
		}
		for ino, size := range nodes {
			combined[ino] = max(combined[ino], size)
			if !strings.HasPrefix(name, "\x00reservation:") {
				committed[ino] = max(committed[ino], size)
			}
		}
	}
	for ino, size := range sizes {
		combined[ino] = max(combined[ino], size)
		if !strings.HasPrefix(branch, "\x00reservation:") {
			committed[ino] = max(committed[ino], size)
		}
	}
	var used uint64
	for _, size := range combined {
		if size > ^uint64(0)-used || (enforce && (size > q.maxBytes || used > q.maxBytes-size)) {
			return ErrNoSpace
		}
		used += size
	}
	for ino := range q.branches[branch] {
		delete(q.inodes[ino], branch)
		if len(q.inodes[ino]) == 0 {
			delete(q.inodes, ino)
		}
	}
	if sizes == nil {
		delete(q.branches, branch)
	} else {
		q.branches[branch] = make(map[uint64]uint64, len(sizes))
		for ino, size := range sizes {
			q.branches[branch][ino] = size
			if q.inodes[ino] == nil {
				q.inodes[ino] = make(map[string]uint64)
			}
			q.inodes[ino][branch] = size
		}
	}
	q.usage = TreeQuotaUsage{Bytes: used, Inodes: uint64(len(combined))}
	var committedBytes uint64
	for _, size := range committed {
		committedBytes += size
	}
	q.committed = TreeQuotaUsage{Bytes: committedBytes, Inodes: uint64(len(committed))}
	return nil
}

// SetMaxBytes 修改容量上限，不能低于当前用量。
func (q *TreeQuota) SetMaxBytes(limit uint64) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrNotFound
	}
	if limit < q.usage.Bytes {
		return ErrInvalidShrink
	}
	q.maxBytes = limit
	return nil
}
