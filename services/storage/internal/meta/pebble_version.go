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
	if name == "" || len(name) > 255 || name == "." || name == ".." {
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
	if delErr := batch.DeleteRange(lower, upper, nil); delErr != nil {
		return delErr
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
		ino, decodeErr := pebblekv.DecodeOrphanKey(iter.Key())
		if decodeErr != nil {
			return decodeErr
		}
		if reclaimErr := reclaimBranchOrphan(store, blobs, quota, name, ino); reclaimErr != nil {
			return reclaimErr
		}
	}
	if iterErr := iter.Error(); iterErr != nil {
		return iterErr
	}
	return clearBranchDirty(store)
}

func (m *manager) versionQuota(ctx context.Context, q db.Querier, diskID pgtype.UUID, control db.GetDiskControlPlaneForUpdateRow) (*TreeQuota, error) {
	row, err := q.GetDisk(ctx, diskID)
	if err != nil {
		return nil, err
	}
	return m.loadTreeQuota(ctx, &row, db.GetDiskControlPlaneRow(control))
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
	if err := m.checkpointBranchLocked(diskID.String(), b.Name, branch, id.String()); err != nil {
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
	return m.commit(ctx, CommitParams(params), true)
}

func (m *manager) commit(ctx context.Context, params CommitParams, requireMerge bool) (info *CommitInfo, err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if valErr := validateBranchName(params.BranchName); valErr != nil {
		return nil, valErr
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
		control, branches, lockErr := controlStore.LockedBranches(ctx, q, diskID)
		if lockErr != nil {
			return lockErr
		}
		target, _ := controlStore.FindBranch(branches, params.BranchName)
		if target == nil {
			return ErrNotFound
		}
		if requireMerge && (target.State != "MERGING" || len(target.MergeState) == 0) {
			return ErrInvalidArgument
		}
		unlock = m.locks.LockBranchWrite(diskID.String(), params.BranchName)
		var openErr error
		branch, openErr = m.pebble.OpenBranch(diskID.String(), params.BranchName)
		if openErr != nil {
			return openErr
		}
		created = true
		info, openErr = m.createVersion(ctx, q, diskID, target, branch, id, params.Message)
		if openErr != nil {
			return openErr
		}
		return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
	})
	if err != nil {
		if created {
			err = errors.Join(err, os.RemoveAll(filepath.Join(m.storageRoot, "disks", diskID.String(), "commits", id.String())))
		}
		return nil, err
	}
	err = clearBranchDirty(branch.DB())
	if err != nil {
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
	if valErr := validateBranchName(params.Name); valErr != nil {
		return nil, valErr
	}
	if params.SourceBranch != nil {
		if valErr := validateBranchName(*params.SourceBranch); valErr != nil {
			return nil, valErr
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
		control, branches, lockErr := controlStore.LockedBranches(ctx, q, diskID)
		if lockErr != nil {
			return lockErr
		}
		if _, i := controlStore.FindBranch(branches, params.Name); i >= 0 {
			return ErrAlreadyExists
		}
		quota, quotaErr := m.versionQuota(ctx, q, diskID, control)
		if quotaErr != nil {
			return quotaErr
		}
		var parent, fork string
		if params.SourceCommitID != nil {
			row, getErr := q.GetCommit(ctx, sourceID)
			if errors.Is(getErr, pgx.ErrNoRows) || getErr == nil && row.DiskID != diskID {
				return ErrNotFound
			}
			if getErr != nil {
				return getErr
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
			var openErr error
			source, openErr = m.pebble.OpenBranch(disk, parent)
			if openErr != nil {
				return openErr
			}
			dirty, dirtyErr := m.flushAndCheckBranchDirtyLocked(disk, parent, source.DB())
			if dirtyErr != nil {
				return dirtyErr
			}
			if dirty || sourceBranch.HeadCommitID == nil {
				id := uuid.Must(uuid.NewV7())
				checkpointID, checkpointCreated = id.String(), true
				if _, createErr := m.createVersion(ctx, q, diskID, sourceBranch, source, id, ""); createErr != nil {
					return createErr
				}
			}
			fork = *sourceBranch.HeadCommitID
		}
		var sizes map[uint64]uint64
		var stageErr error
		swap, sizes, stageErr = swapper.StageFork(disk, fork, params.Name)
		if stageErr != nil {
			return stageErr
		}
		if replaceErr := swap.StageReplace(quota, sizes, true); replaceErr != nil {
			return replaceErr
		}
		now := time.Now().UTC()
		id := uuid.Must(uuid.NewV7()).String()
		branches = append(branches, storedBranch{BranchID: id, Name: params.Name, ParentBranch: &parent, ForkCommitID: &fork, HeadCommitID: &fork, State: "NORMAL", CreatedAt: &now})
		if saveErr := controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches); saveErr != nil {
			return saveErr
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
		err = clearBranchDirty(source.DB())
		if err != nil {
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
	if valErr := validateBranchName(params.Name); valErr != nil {
		return valErr
	}
	if params.Name == "main" {
		return ErrInvalidArgument
	}
	if sweepErr := m.sweepExpiredBranchSessions(ctx); sweepErr != nil {
		return sweepErr
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	disk := diskID.String()
	swapper := newWorkspaceSwapper(m)
	var swap *WorkspaceSwap
	controlStore := newBranchControlStore(m.store)
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, lockErr := controlStore.LockedBranches(ctx, q, diskID)
		if lockErr != nil {
			return lockErr
		}
		if writerLockErr := controlStore.LockBranchWriter(ctx, q, diskID, params.Name); writerLockErr != nil {
			return writerLockErr
		}
		_, i := controlStore.FindBranch(branches, params.Name)
		if i < 0 {
			return ErrNotFound
		}
		active, activeErr := controlStore.HasActiveBranchSessions(ctx, q, diskID, params.Name)
		if activeErr != nil {
			return activeErr
		}
		if active {
			return ErrBusy
		}
		quota, quotaErr := m.versionQuota(ctx, q, diskID, control)
		if quotaErr != nil {
			return quotaErr
		}
		var stageErr error
		swap, stageErr = swapper.StageDelete(disk, params.Name)
		if stageErr != nil {
			return stageErr
		}
		if replaceErr := swap.StageReplace(quota, nil, false); replaceErr != nil {
			return replaceErr
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
	if valErr := validateBranchName(params.BranchName); valErr != nil {
		return valErr
	}
	var commitID pgtype.UUID
	if !abort {
		commitID, err = parseDiskID(params.CommitID)
		if err != nil {
			return err
		}
	}
	if sweepErr := m.sweepExpiredBranchSessions(ctx); sweepErr != nil {
		return sweepErr
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	disk := diskID.String()
	swapper := newWorkspaceSwapper(m)
	var swap *WorkspaceSwap
	controlStore := newBranchControlStore(m.store)
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, lockErr := controlStore.LockedBranches(ctx, q, diskID)
		if lockErr != nil {
			return lockErr
		}
		if writerLockErr := controlStore.LockBranchWriter(ctx, q, diskID, params.BranchName); writerLockErr != nil {
			return writerLockErr
		}
		target, _ := controlStore.FindBranch(branches, params.BranchName)
		if target == nil {
			return ErrNotFound
		}
		hasWriter, writerErr := controlStore.HasActiveBranchWriter(ctx, q, diskID, params.BranchName)
		if writerErr != nil {
			return writerErr
		}
		if hasWriter {
			return ErrBusy
		}
		if abort {
			if target.State != "MERGING" || target.HeadCommitID == nil || len(target.MergeState) == 0 {
				return ErrInvalidArgument
			}
			var state storedMergeState
			if unmarshalErr := json.Unmarshal(target.MergeState, &state); unmarshalErr != nil || state.TargetCommitID != *target.HeadCommitID {
				return ErrInvalidArgument
			}
			var parseErr error
			commitID, parseErr = parseDiskID(state.TargetCommitID)
			if parseErr != nil {
				return parseErr
			}
		}
		row, getErr := q.GetCommit(ctx, commitID)
		if errors.Is(getErr, pgx.ErrNoRows) || getErr == nil && row.DiskID != diskID {
			return ErrNotFound
		}
		if getErr != nil {
			return getErr
		}
		quota, quotaErr := m.versionQuota(ctx, q, diskID, control)
		if quotaErr != nil {
			return quotaErr
		}
		var sizes map[uint64]uint64
		var stageErr error
		swap, sizes, stageErr = swapper.StageRestore(disk, params.BranchName, commitID.String())
		if stageErr != nil {
			return stageErr
		}
		if replaceErr := swap.StageReplace(quota, sizes, false); replaceErr != nil {
			return replaceErr
		}
		id := commitID.String()
		target.HeadCommitID = &id
		target.State = "NORMAL"
		target.MergeState = nil
		return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
	})
	return swap.Finalize(err)
}
