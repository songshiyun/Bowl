package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

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
	unlock := m.locks.LockBranchWrite(disk, branchName)
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
