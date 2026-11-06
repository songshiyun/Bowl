package meta

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func validateBranchName(name string) error {
	if len(name) == 0 || len(name) > 255 || name == "." || name == ".." {
		return ErrInvalidArgument
	}
	for i := range len(name) {
		if name[i] == '/' || name[i] < 0x20 || name[i] == 0x7f || os.IsPathSeparator(name[i]) {
			return ErrInvalidArgument
		}
	}
	return nil
}

func clearBranchDirty(store *pebble.DB) (err error) {
	batch := store.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	lower, upper := pebblekv.DirtyRange()
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	return batch.Commit(pebble.Sync)
}

func branchHasDirty(store *pebble.DB) (dirty bool, err error) {
	lower, upper := pebblekv.DirtyRange()
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	dirty = iter.First()
	return dirty, iter.Error()
}

// branchHasDirtyOrAtime 把内存中待合并的访问时间视为脏写，确保 Fork/Merge 生成快照。
func (m *manager) branchHasDirtyOrAtime(disk, branch string, store *pebble.DB) (bool, error) {
	if m.hasPendingAtime(disk, branch) {
		return true, nil
	}
	return branchHasDirty(store)
}

// 新工作区没有旧写端的打开句柄，快照中的孤儿不能继承到新工作区。
func prepareBranch(store *pebble.DB, blobs *pebblekv.BlobStore, name string) (err error) {
	lower, upper := pebblekv.OrphanRange()
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	quota := NewTreeQuota(math.MaxUint64)
	for iter.First(); iter.Valid(); iter.Next() {
		ino, err := pebblekv.DecodeOrphanKey(iter.Key())
		if err != nil {
			return err
		}
		if err := reclaimBranchOrphan(store, blobs, quota, name, ino); err != nil {
			return err
		}
	}
	if err := iter.Error(); err != nil {
		return err
	}
	return clearBranchDirty(store)
}

func (m *manager) versionQuota(ctx context.Context, q db.Querier, diskID pgtype.UUID, control db.GetDiskControlPlaneForUpdateRow) (*TreeQuota, error) {
	row, err := q.GetDisk(ctx, diskID)
	if err != nil {
		return nil, err
	}
	return m.loadTreeQuota(ctx, row, db.GetDiskControlPlaneRow{DiskID: control.DiskID, StorageDir: control.StorageDir, Branches: control.Branches})
}

// createVersion 要求调用方持有分支锁，并在数据库提交后清理变更标记。
func (m *manager) createVersion(ctx context.Context, q db.Querier, diskID pgtype.UUID, b *storedBranch, branch *pebblekv.Branch, id uuid.UUID, message string) (*CommitInfo, error) {
	var mergeParent pgtype.UUID
	var mergeParentID *string
	if b.State == "MERGING" {
		var state storedMergeState
		if len(b.MergeState) == 0 || json.Unmarshal(b.MergeState, &state) != nil || b.HeadCommitID == nil || *b.HeadCommitID != state.TargetCommitID {
			return nil, ErrBusy
		}
		var err error
		mergeParent, err = parseDiskID(state.SourceCommitID)
		if err != nil {
			return nil, ErrBusy
		}
		for _, ino := range state.SidecarInodes {
			node, err := getMergeInode(branch.DB(), ino)
			if err != nil {
				return nil, err
			}
			if node.exists {
				return nil, ErrBusy
			}
		}
		for _, path := range state.SidecarPaths {
			exists, err := mergePathExists(branch.DB(), path)
			if err != nil {
				return nil, err
			}
			if exists {
				return nil, ErrBusy
			}
		}
		mergeParentID = &state.SourceCommitID
	}
	var parent pgtype.UUID
	if b.HeadCommitID != nil {
		var err error
		parent, err = parseDiskID(*b.HeadCommitID)
		if err != nil {
			return nil, err
		}
	}
	if err := m.flushPebbleAtimeEntriesLocked(diskID.String(), b.Name, branch.DB()); err != nil {
		return nil, err
	}
	if err := branch.Checkpoint(id.String()); err != nil {
		return nil, err
	}
	row, err := q.CreateCommit(ctx, db.CreateCommitParams{
		CommitID: pgtype.UUID{Bytes: id, Valid: true}, DiskID: diskID, BranchName: b.Name,
		ParentCommitID: parent, MergeParentCommitID: mergeParent, Message: message,
		CheckpointDir: filepath.Join(m.storageRoot, "disks", diskID.String(), "commits", id.String()),
	})
	if err != nil {
		return nil, err
	}
	idString := id.String()
	info := &CommitInfo{CommitID: idString, DiskID: diskID.String(), BranchName: b.Name, ParentCommitID: b.HeadCommitID, MergeParentCommitID: mergeParentID, Message: message, CreatedAt: row.CreatedAt.Time}
	b.HeadCommitID = &idString
	b.State, b.MergeState = "NORMAL", nil
	return info, nil
}

func mergePathExists(store *pebble.DB, path string) (bool, error) {
	node, err := mergePathInode(store, path)
	return node.exists, err
}

// Commit 将分支当前状态定格为快照。
func (m *manager) Commit(ctx context.Context, params CommitParams) (info *CommitInfo, err error) {
	return m.commit(ctx, params, false)
}

// ResolveMerge 完成已物化且旁路副本已清理的合并。
func (m *manager) ResolveMerge(ctx context.Context, params ResolveMergeParams) (*CommitInfo, error) {
	return m.commit(ctx, CommitParams{DiskID: params.DiskID, BranchName: params.BranchName, Message: params.Message}, true)
}

func (m *manager) commit(ctx context.Context, params CommitParams, requireMerge bool) (info *CommitInfo, err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if err := validateBranchName(params.BranchName); err != nil {
		return nil, err
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	id := uuid.Must(uuid.NewV7())
	var branch *pebblekv.Branch
	var unlock func()
	created := false
	defer func() {
		if branch != nil {
			err = errors.Join(err, branch.Close())
		}
		if unlock != nil {
			unlock()
		}
	}()
	controlStore := newBranchControlStore(m.store)
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := controlStore.LockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		target, _ := controlStore.FindBranch(branches, params.BranchName)
		if target == nil {
			return ErrNotFound
		}
		if requireMerge && (target.State != "MERGING" || len(target.MergeState) == 0) {
			return ErrInvalidArgument
		}
		unlock = m.locks.LockBranchWrite(diskID.String(), params.BranchName)
		branch, err = m.pebble.OpenBranch(diskID.String(), params.BranchName)
		if err != nil {
			return err
		}
		created = true
		info, err = m.createVersion(ctx, q, diskID, target, branch, id, params.Message)
		if err != nil {
			return err
		}
		return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
	})
	if err != nil {
		if created {
			err = errors.Join(err, os.RemoveAll(filepath.Join(m.storageRoot, "disks", diskID.String(), "commits", id.String())))
		}
		return nil, err
	}
	if err = clearBranchDirty(branch.DB()); err != nil {
		return nil, err
	}
	return info, nil
}

// CreateBranch 从源分支或历史快照派生工作区。
func (m *manager) CreateBranch(ctx context.Context, params CreateBranchParams) (result *Branch, err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if err := validateBranchName(params.Name); err != nil {
		return nil, err
	}
	if params.SourceBranch != nil {
		if err := validateBranchName(*params.SourceBranch); err != nil {
			return nil, err
		}
	}
	var sourceID pgtype.UUID
	if params.SourceCommitID != nil {
		sourceID, err = parseDiskID(*params.SourceCommitID)
		if err != nil {
			return nil, err
		}
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	disk := diskID.String()
	swapper := newWorkspaceSwapper(m)
	var source *pebblekv.Branch
	var unlockSource func()
	var swap *WorkspaceSwap
	var checkpointCreated bool
	var checkpointID string
	defer func() {
		swap.ReleaseUnlock()
		if source != nil {
			err = errors.Join(err, source.Close())
		}
		if unlockSource != nil {
			unlockSource()
		}
	}()
	controlStore := newBranchControlStore(m.store)
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := controlStore.LockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		if _, i := controlStore.FindBranch(branches, params.Name); i >= 0 {
			return ErrAlreadyExists
		}
		quota, err := m.versionQuota(ctx, q, diskID, control)
		if err != nil {
			return err
		}
		var parent, fork string
		if params.SourceCommitID != nil {
			row, err := q.GetCommit(ctx, sourceID)
			if errors.Is(err, pgx.ErrNoRows) || err == nil && row.DiskID != diskID {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
			parent, fork = row.BranchName, sourceID.String()
			if params.SourceBranch != nil {
				parent = *params.SourceBranch
				if _, i := controlStore.FindBranch(branches, parent); i < 0 {
					return ErrNotFound
				}
			}
		} else {
			parent = "main"
			if params.SourceBranch != nil {
				parent = *params.SourceBranch
			}
			sourceBranch, _ := controlStore.FindBranch(branches, parent)
			if sourceBranch == nil {
				return ErrNotFound
			}
			if sourceBranch.State == "MERGING" {
				return ErrBusy
			}
			unlockSource = m.locks.LockBranchWrite(disk, parent)
			source, err = m.pebble.OpenBranch(disk, parent)
			if err != nil {
				return err
			}
			dirty, err := m.branchHasDirtyOrAtime(disk, parent, source.DB())
			if err != nil {
				return err
			}
			if dirty || sourceBranch.HeadCommitID == nil {
				id := uuid.Must(uuid.NewV7())
				checkpointID, checkpointCreated = id.String(), true
				if _, err := m.createVersion(ctx, q, diskID, sourceBranch, source, id, ""); err != nil {
					return err
				}
			}
			fork = *sourceBranch.HeadCommitID
		}
		var sizes map[uint64]uint64
		swap, sizes, err = swapper.StageFork(disk, fork, params.Name)
		if err != nil {
			return err
		}
		if err := swap.StageReplace(quota, sizes, true); err != nil {
			return err
		}
		now := time.Now().UTC()
		id := uuid.Must(uuid.NewV7()).String()
		branches = append(branches, storedBranch{BranchID: id, Name: params.Name, ParentBranch: &parent, ForkCommitID: &fork, HeadCommitID: &fork, State: "NORMAL", CreatedAt: &now})
		if err := controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches); err != nil {
			return err
		}
		result = &Branch{BranchID: id, DiskID: disk, Name: params.Name, ParentBranch: &parent, ForkCommitID: &fork, HeadCommitID: &fork, State: "NORMAL", CreatedAt: now}
		return nil
	})
	err = swap.Finalize(err)
	if err != nil {
		if checkpointCreated {
			err = errors.Join(err, os.RemoveAll(filepath.Join(m.storageRoot, "disks", disk, "commits", checkpointID)))
		}
		return nil, err
	}
	if checkpointCreated {
		if err = clearBranchDirty(source.DB()); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// ListBranches 列出磁盘的逻辑分支。
func (m *manager) ListBranches(ctx context.Context, diskID string) ([]Branch, error) {
	disk, err := m.GetDisk(ctx, diskID)
	if err != nil {
		return nil, err
	}
	return disk.Branches, nil
}

// DeleteBranch 删除没有活跃会话的非根分支。
func (m *manager) DeleteBranch(ctx context.Context, params DeleteBranchParams) (err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	if err := validateBranchName(params.Name); err != nil {
		return err
	}
	if params.Name == "main" {
		return ErrInvalidArgument
	}
	if err := m.sweepExpiredBranchSessions(ctx); err != nil {
		return err
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	disk := diskID.String()
	swapper := newWorkspaceSwapper(m)
	var swap *WorkspaceSwap
	controlStore := newBranchControlStore(m.store)
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := controlStore.LockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		if err := controlStore.LockBranchWriter(ctx, q, diskID, params.Name); err != nil {
			return err
		}
		_, i := controlStore.FindBranch(branches, params.Name)
		if i < 0 {
			return ErrNotFound
		}
		active, err := controlStore.HasActiveBranchSessions(ctx, q, diskID, params.Name)
		if err != nil {
			return err
		}
		if active {
			return ErrBusy
		}
		quota, err := m.versionQuota(ctx, q, diskID, control)
		if err != nil {
			return err
		}
		swap, err = swapper.StageDelete(disk, params.Name)
		if err != nil {
			return err
		}
		if err := swap.StageReplace(quota, nil, false); err != nil {
			return err
		}
		branches = append(branches[:i], branches[i+1:]...)
		return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
	})
	return swap.Finalize(err)
}

// Rollback 将无活跃写端的分支恢复到指定快照。
func (m *manager) Rollback(ctx context.Context, params RollbackParams) (err error) {
	return m.rollback(ctx, params, false)
}

// AbortMerge 在无活跃写端时恢复合并前的快照。
func (m *manager) AbortMerge(ctx context.Context, params AbortMergeParams) error {
	return m.rollback(ctx, RollbackParams{DiskID: params.DiskID, BranchName: params.BranchName}, true)
}

func (m *manager) rollback(ctx context.Context, params RollbackParams, abort bool) (err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	if err := validateBranchName(params.BranchName); err != nil {
		return err
	}
	var commitID pgtype.UUID
	if !abort {
		commitID, err = parseDiskID(params.CommitID)
		if err != nil {
			return err
		}
	}
	if err := m.sweepExpiredBranchSessions(ctx); err != nil {
		return err
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	disk := diskID.String()
	swapper := newWorkspaceSwapper(m)
	var swap *WorkspaceSwap
	controlStore := newBranchControlStore(m.store)
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := controlStore.LockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		if err := controlStore.LockBranchWriter(ctx, q, diskID, params.BranchName); err != nil {
			return err
		}
		target, _ := controlStore.FindBranch(branches, params.BranchName)
		if target == nil {
			return ErrNotFound
		}
		hasWriter, err := controlStore.HasActiveBranchWriter(ctx, q, diskID, params.BranchName)
		if err != nil {
			return err
		}
		if hasWriter {
			return ErrBusy
		}
		if abort {
			if target.State != "MERGING" || target.HeadCommitID == nil || len(target.MergeState) == 0 {
				return ErrInvalidArgument
			}
			var state storedMergeState
			if err := json.Unmarshal(target.MergeState, &state); err != nil || state.TargetCommitID != *target.HeadCommitID {
				return ErrInvalidArgument
			}
			commitID, err = parseDiskID(state.TargetCommitID)
			if err != nil {
				return err
			}
		}
		row, err := q.GetCommit(ctx, commitID)
		if errors.Is(err, pgx.ErrNoRows) || err == nil && row.DiskID != diskID {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		quota, err := m.versionQuota(ctx, q, diskID, control)
		if err != nil {
			return err
		}
		var sizes map[uint64]uint64
		swap, sizes, err = swapper.StageRestore(disk, params.BranchName, commitID.String())
		if err != nil {
			return err
		}
		if err := swap.StageReplace(quota, sizes, false); err != nil {
			return err
		}
		id := commitID.String()
		target.HeadCommitID = &id
		target.State = "NORMAL"
		target.MergeState = nil
		return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
	})
	return swap.Finalize(err)
}
