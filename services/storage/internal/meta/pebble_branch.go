package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type branchLockKey struct{ disk, branch string }

func (m *manager) branchLock(disk, branch string) *sync.RWMutex {
	value, _ := m.branchLocks.LoadOrStore(branchLockKey{disk, branch}, &sync.RWMutex{})
	return value.(*sync.RWMutex)
}

// lockBranchRead 取得分支共享读锁，允许同分支无限并发读取。
func (m *manager) lockBranchRead(disk, branch string) func() {
	mu := m.branchLock(disk, branch)
	mu.RLock()
	return mu.RUnlock
}

// lockBranchWrite 取得分支拓扑生命周期独占锁，仅用于替换或冻结整分支目录。
func (m *manager) lockBranchWrite(disk, branch string) func() {
	mu := m.branchLock(disk, branch)
	mu.Lock()
	return mu.Unlock
}

// branchMetaLock 串行化同分支的元数据批次，避免共享 BranchStat 丢失更新。
func (m *manager) branchMetaLock(disk, branch string) func() {
	value, _ := m.branchMetaLocks.LoadOrStore(branchLockKey{disk, branch}, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
}

type inodeLockKey struct {
	disk, branch string
	ino          uint64
}

// inodeLockEntry 是带引用计数的 Inode 锁，refs 归零后从锁表移除，避免海量 Inode 场景无界增长。
type inodeLockEntry struct {
	mu   sync.RWMutex
	refs int
}

// acquireInodeLock 取得锁条目并增加引用计数；调用方解锁后必须调用 releaseInodeLock。
func (m *manager) acquireInodeLock(disk, branch string, ino uint64) (inodeLockKey, *inodeLockEntry) {
	key := inodeLockKey{disk, branch, ino}
	m.inodeLocksMu.Lock()
	entry := m.inodeLocks[key]
	if entry == nil {
		entry = &inodeLockEntry{}
		m.inodeLocks[key] = entry
	}
	entry.refs++
	m.inodeLocksMu.Unlock()
	return key, entry
}

// releaseInodeLock 减少引用计数，归零时回收锁条目；此时已无人持有底层互斥量。
func (m *manager) releaseInodeLock(key inodeLockKey, entry *inodeLockEntry) {
	m.inodeLocksMu.Lock()
	entry.refs--
	if entry.refs == 0 {
		delete(m.inodeLocks, key)
	}
	m.inodeLocksMu.Unlock()
}

// lockInodeRead 取得单个 Inode 的共享读锁，防止并发覆写删除正在读取的块文件。
func (m *manager) lockInodeRead(disk, branch string, ino uint64) func() {
	key, entry := m.acquireInodeLock(disk, branch, ino)
	entry.mu.RLock()
	return func() {
		entry.mu.RUnlock()
		m.releaseInodeLock(key, entry)
	}
}

// lockInodeWrite 取得单个 Inode 的独占写锁。
func (m *manager) lockInodeWrite(disk, branch string, ino uint64) func() {
	key, entry := m.acquireInodeLock(disk, branch, ino)
	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		m.releaseInodeLock(key, entry)
	}
}

// lockInodesWrite 按固定 ino 升序对多个 Inode 加独占锁，避免目录操作死锁。
func (m *manager) lockInodesWrite(disk, branch string, inos ...uint64) func() {
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
		key, entry := m.acquireInodeLock(disk, branch, ino)
		entry.mu.Lock()
		locks = append(locks, heldLock{key: key, entry: entry})
	}
	return func() {
		for i := len(locks) - 1; i >= 0; i-- {
			locks[i].entry.mu.Unlock()
			m.releaseInodeLock(locks[i].key, locks[i].entry)
		}
	}
}

// sweepBranchOrphans 必须在数据库事务中持有该分支写端咨询锁，并确认没有活跃写端。
func (m *manager) sweepBranchOrphans(ctx context.Context, q db.Querier, diskID pgtype.UUID, branchName string) (err error) {
	disk := diskID.String()
	path := filepath.Join(m.storageRoot, "disks", disk, "branches", branchName)
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		return nil
	} else if err != nil {
		return err
	}
	quota, ok := m.lookupTreeQuota(disk)
	if !ok {
		row, err := q.GetDisk(ctx, diskID)
		if err != nil {
			return err
		}
		control, err := q.GetDiskControlPlane(ctx, diskID)
		if err != nil {
			return err
		}
		if quota, err = m.loadTreeQuota(ctx, row, control); err != nil {
			return err
		}
	}
	unlock := m.lockBranchWrite(disk, branchName)
	defer unlock()
	branch, err := m.pebble.OpenBranch(disk, branchName)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	lower, upper := pebblekv.OrphanRange()
	iter, err := branch.DB().NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	for iter.First(); iter.Valid(); iter.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		ino, err := pebblekv.DecodeOrphanKey(iter.Key())
		if err != nil {
			return err
		}
		if err := m.withBranchReclaimTx(disk, branchName, branch.DB(), branch.Blobs(), quota, func(tx *BranchMutationTx) error {
			if err := tx.ReclaimOrphanInode(ino); err != nil {
				return err
			}
			return tx.CommitReclaim(ino)
		}); err != nil {
			return fmt.Errorf("回收分支 %s 的孤儿节点 %d: %w", branchName, ino, err)
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	m.pebble.MarkBranchDirty(disk, branchName)
	return nil
}

func (m *manager) closeBranchSession(ctx context.Context, sessionID uint64) (bool, error) {
	controlStore := newBranchControlStore(m.store)
	managed := false
	needsSweep := false
	err := m.store.ExecTx(ctx, func(q db.Querier) error {
		var err error
		managed, needsSweep, err = controlStore.DeactivateOrDeleteManagedSession(ctx, q, sessionID)
		return err
	})
	if err == nil && needsSweep {
		err = m.sweepExpiredBranchSession(ctx, sessionID)
	}
	return managed, err
}

// sweepExpiredBranchSessions 保留写端租约记录，直到该分支的清扫成功。
func (m *manager) sweepExpiredBranchSessions(ctx context.Context) error {
	controlStore := newBranchControlStore(m.store)
	sessions, err := controlStore.ListExpiredManagedSessionIDs(ctx)
	if err != nil {
		return err
	}
	for _, id := range sessions {
		if err := m.sweepExpiredBranchSession(ctx, id); err != nil {
			return err
		}
	}
	return nil
}

func (m *manager) sweepExpiredBranchSession(ctx context.Context, sessionID uint64) error {
	controlStore := newBranchControlStore(m.store)
	deleted := false
	err := m.store.ExecTx(ctx, func(q db.Querier) error {
		expired, err := controlStore.DeleteExpiredManagedSession(ctx, q, sessionID)
		if err != nil || !expired.deleted {
			return err
		}
		deleted = true
		if expired.readOnly {
			return nil
		}
		hasWriter, err := controlStore.HasActiveBranchWriter(ctx, q, expired.diskID, expired.branch)
		if err != nil || hasWriter {
			return err
		}
		return m.sweepBranchOrphans(ctx, q, expired.diskID, expired.branch)
	})
	if err == nil && deleted {
		m.invalidatePebbleSession(sessionID)
		m.clearSessionCursors(sessionID)
	}
	return err
}
