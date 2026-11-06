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

// Snapshot 原子返回容量上限与已提交用量，供 StatFS 免查控制面。
func (q *TreeQuota) Snapshot() (uint64, TreeQuotaUsage) {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.maxBytes, q.committed
}

// Apply 在配额锁内执行持久化；失败时保持原有用量。回调不能重入此协调器。
func (q *TreeQuota) Apply(branch string, ino, size uint64, live bool, persist func() error) error {
	return q.ApplyChange(branch, ino, size, live, true, persist)
}

// ApplyChange 在配额锁内更新单个节点的用量或存活状态，并在校验通过后执行持久化回调。
func (q *TreeQuota) ApplyChange(branch string, ino, size uint64, live, enforce bool, persist func() error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.applyLocked(branch, ino, size, live, persist, enforce)
}

// Reclaim 在配额锁内回收已无引用的节点，不受当前配额上限阻断。
func (q *TreeQuota) Reclaim(branch string, ino uint64, persist func() error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.applyLocked(branch, ino, 0, false, persist, false)
}

// Create 在配额锁内严格校验剩余容量（usage.Bytes < maxBytes 且可容纳 size），依次执行 prepare 与 persist 并记入新节点。
func (q *TreeQuota) Create(branch string, ino, size uint64, prepare, persist func() error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrNotFound
	}
	if q.usage.Bytes >= q.maxBytes || size > q.maxBytes-q.usage.Bytes {
		return ErrNoSpace
	}
	if prepare != nil {
		if err := prepare(); err != nil {
			return err
		}
	}
	return q.applyLocked(branch, ino, size, true, persist, true)
}

// CommitWithQuota 在配额锁内执行不改变单节点逻辑字节数的持久化操作（如硬链接、孤儿标记或重命名）。
// enforce 为 true 时要求当前用量严格小于容量上限（usage.Bytes < maxBytes）。
func (q *TreeQuota) CommitWithQuota(enforce bool, persist func() error) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrNotFound
	}
	if enforce && q.usage.Bytes >= q.maxBytes {
		return ErrNoSpace
	}
	if persist != nil {
		return persist()
	}
	return nil
}

// InitMaxBytes 无条件设置容量上限（用于从控制面恢复初始配额，不校验当前用量）。
func (q *TreeQuota) InitMaxBytes(limit uint64) {
	q.mu.Lock()
	defer q.mu.Unlock()
	q.maxBytes = limit
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

// BranchQuotaStage 持有配额锁并记录分支替换前的用量快照，支持两阶段提交或回滚。
type BranchQuotaStage struct {
	quota     *TreeQuota
	branch    string
	oldSizes  map[uint64]uint64
	active    bool
	committed bool
}

// StageBranchReplace 在配额锁内暂存分支全量替换；失败时立即释放配额锁。
func (q *TreeQuota) StageBranchReplace(branch string, sizes map[uint64]uint64, enforce bool) (*BranchQuotaStage, error) {
	q.mu.Lock()
	oldSizes := q.branches[branch]
	if err := q.replaceBranchLocked(branch, sizes, enforce); err != nil {
		q.mu.Unlock()
		return nil, err
	}
	return &BranchQuotaStage{quota: q, branch: branch, oldSizes: oldSizes, active: true}, nil
}

// StageBranchPatch 在配额锁内基于脏节点集合与变更节点表暂存分支增量替换；失败时立即释放配额锁。
func (q *TreeQuota) StageBranchPatch(branch string, dirty map[uint64]bool, changed map[uint64]uint64, enforce bool) (*BranchQuotaStage, error) {
	q.mu.Lock()
	oldSizes := q.branches[branch]
	sizes := make(map[uint64]uint64, len(oldSizes)+len(changed))
	for ino, size := range oldSizes {
		if !dirty[ino] {
			sizes[ino] = size
		}
	}
	for ino, size := range changed {
		sizes[ino] = size
	}
	if err := q.replaceBranchLocked(branch, sizes, enforce); err != nil {
		q.mu.Unlock()
		return nil, err
	}
	return &BranchQuotaStage{quota: q, branch: branch, oldSizes: oldSizes, active: true}, nil
}

// Commit 确认暂存的分支配额变更并释放配额锁。
func (s *BranchQuotaStage) Commit() {
	if s == nil || !s.active {
		return
	}
	s.committed = true
	s.active = false
	s.quota.mu.Unlock()
}

// Rollback 在未提交时恢复分支原有配额并释放配额锁。
func (s *BranchQuotaStage) Rollback() error {
	if s == nil || !s.active {
		return nil
	}
	var err error
	if !s.committed {
		err = s.quota.replaceBranchLocked(s.branch, s.oldSizes, false)
	}
	s.active = false
	s.quota.mu.Unlock()
	return err
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

// WithShrinkCheck 在配额锁内校验缩容水位线并执行更新回调（内部可通过 CheckLimit 校验），成功后更新容量上限。
func (q *TreeQuota) WithShrinkCheck(fn func() (uint64, error)) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return ErrNotFound
	}
	limit, err := fn()
	if err != nil {
		return err
	}
	if limit < q.usage.Bytes {
		return ErrInvalidShrink
	}
	q.maxBytes = limit
	return nil
}

// CheckLimit 在配额锁已持有时校验新容量上限是否低于当前用量。
func (q *TreeQuota) CheckLimit(limit uint64) error {
	if q.closed {
		return ErrNotFound
	}
	if limit < q.usage.Bytes {
		return ErrInvalidShrink
	}
	return nil
}

// BeginClose 标记配额协调器已关闭以拒绝后续操作，并返回在删除失败时恢复开启状态的回滚函数。
func (q *TreeQuota) BeginClose() (func(), error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil, ErrNotFound
	}
	q.closed = true
	return func() {
		q.mu.Lock()
		q.closed = false
		q.mu.Unlock()
	}, nil
}
