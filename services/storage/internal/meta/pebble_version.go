package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

func branchIndex(branches []storedBranch, name string) int {
	for i := range branches {
		if branches[i].Name == name {
			return i
		}
	}
	return -1
}

func lockedBranches(ctx context.Context, q db.Querier, diskID pgtype.UUID) (db.GetDiskControlPlaneForUpdateRow, []storedBranch, error) {
	control, err := q.GetDiskControlPlaneForUpdate(ctx, diskID)
	if errors.Is(err, pgx.ErrNoRows) {
		return control, nil, ErrNotFound
	}
	if err != nil {
		return control, nil, err
	}
	var branches []storedBranch
	if err := json.Unmarshal(control.Branches, &branches); err != nil {
		return control, nil, fmt.Errorf("读取分支树: %w", err)
	}
	return control, branches, nil
}

func saveBranches(ctx context.Context, q db.Querier, diskID pgtype.UUID, old []byte, branches []storedBranch) error {
	raw, err := json.Marshal(branches)
	if err != nil {
		return err
	}
	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{DiskID: diskID, ExpectedBranches: old, Branches: raw})
	return err
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

// 新工作区没有旧写端的打开句柄，快照中的孤儿不能继承到新工作区。
func prepareBranch(store *pebble.DB, name string) (err error) {
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
		if err := reclaimBranchOrphan(store, quota, name, ino); err != nil {
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
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := lockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		i := branchIndex(branches, params.BranchName)
		if i < 0 {
			return ErrNotFound
		}
		if requireMerge && (branches[i].State != "MERGING" || len(branches[i].MergeState) == 0) {
			return ErrInvalidArgument
		}
		unlock = m.lockBranchWrite(diskID.String(), params.BranchName)
		branch, err = m.pebble.OpenBranch(diskID.String(), params.BranchName)
		if err != nil {
			return err
		}
		created = true
		info, err = m.createVersion(ctx, q, diskID, &branches[i], branch, id, params.Message)
		if err != nil {
			return err
		}
		return saveBranches(ctx, q, diskID, control.Branches, branches)
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
	var source *pebblekv.Branch
	var unlockSource, unlockTarget func()
	var quota *TreeQuota
	var quotaLocked, quotaChanged, forkCreated, checkpointCreated, committed bool
	var checkpointID string
	defer func() {
		if quotaLocked {
			if !committed && quotaChanged {
				err = errors.Join(err, quota.replaceBranchLocked(params.Name, nil, false))
			}
			quota.mu.Unlock()
		}
		if source != nil {
			err = errors.Join(err, source.Close())
		}
		if unlockTarget != nil {
			unlockTarget()
		}
		if unlockSource != nil {
			unlockSource()
		}
	}()
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := lockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		if branchIndex(branches, params.Name) >= 0 {
			return ErrAlreadyExists
		}
		quota, err = m.versionQuota(ctx, q, diskID, control)
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
				if branchIndex(branches, parent) < 0 {
					return ErrNotFound
				}
			}
		} else {
			parent = "main"
			if params.SourceBranch != nil {
				parent = *params.SourceBranch
			}
			i := branchIndex(branches, parent)
			if i < 0 {
				return ErrNotFound
			}
			if branches[i].State == "MERGING" {
				return ErrBusy
			}
			unlockSource = m.lockBranchWrite(disk, parent)
			source, err = m.pebble.OpenBranch(disk, parent)
			if err != nil {
				return err
			}
			dirty, err := branchHasDirty(source.DB())
			if err != nil {
				return err
			}
			if dirty || branches[i].HeadCommitID == nil {
				id := uuid.Must(uuid.NewV7())
				checkpointID, checkpointCreated = id.String(), true
				if _, err := m.createVersion(ctx, q, diskID, &branches[i], source, id, ""); err != nil {
					return err
				}
			}
			fork = *branches[i].HeadCommitID
		}
		unlockTarget = m.lockBranchWrite(disk, params.Name)
		targetPath := filepath.Join(m.storageRoot, "disks", disk, "branches", params.Name)
		if _, err := os.Stat(targetPath); err == nil {
			return ErrAlreadyExists
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		}
		forkCreated = true
		if err := m.pebble.Fork(disk, fork, params.Name); err != nil {
			return err
		}
		workspace, err := m.pebble.OpenBranch(disk, params.Name)
		if err != nil {
			return err
		}
		err = prepareBranch(workspace.DB(), params.Name)
		var sizes map[uint64]uint64
		if err == nil {
			sizes, err = readBranchSizesFromDB(workspace.DB())
		}
		err = errors.Join(err, workspace.Close())
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		id := uuid.Must(uuid.NewV7()).String()
		branches = append(branches, storedBranch{BranchID: id, Name: params.Name, ParentBranch: &parent, ForkCommitID: &fork, HeadCommitID: &fork, State: "NORMAL", CreatedAt: &now})
		if err := saveBranches(ctx, q, diskID, control.Branches, branches); err != nil {
			return err
		}
		quota.mu.Lock()
		quotaLocked = true
		if err := quota.replaceBranchLocked(params.Name, sizes, true); err != nil {
			return err
		}
		quotaChanged = true
		result = &Branch{BranchID: id, DiskID: disk, Name: params.Name, ParentBranch: &parent, ForkCommitID: &fork, HeadCommitID: &fork, State: "NORMAL", CreatedAt: now}
		return nil
	})
	committed = err == nil
	if err != nil {
		if forkCreated {
			// 删除工作区前先驱逐刚打开的常驻句柄，避免残留指向已删目录的实例。
			err = errors.Join(err, m.pebble.EvictBranch(disk, params.Name),
				os.RemoveAll(filepath.Join(m.storageRoot, "disks", disk, "branches", params.Name)))
		}
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
	path := filepath.Join(m.storageRoot, "disks", disk, "branches", params.Name)
	backup := filepath.Join(filepath.Dir(path), ".delete-"+uuid.Must(uuid.NewV7()).String())
	var unlock func()
	var quota *TreeQuota
	var oldSizes map[uint64]uint64
	var moved, quotaLocked, quotaChanged, committed bool
	defer func() {
		if quotaLocked {
			if !committed && quotaChanged {
				err = errors.Join(err, quota.replaceBranchLocked(params.Name, oldSizes, false))
			}
			quota.mu.Unlock()
		}
		if unlock != nil {
			unlock()
		}
	}()
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := lockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		raw := q.(interface{ RawDBTX() db.DBTX }).RawDBTX()
		if err := lockBranchWriter(ctx, raw, diskID, params.Name); err != nil {
			return err
		}
		i := branchIndex(branches, params.Name)
		if i < 0 {
			return ErrNotFound
		}
		var active bool
		if err := raw.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM client_sessions WHERE lease_managed AND disk_id = $1 AND branch_name = $2 AND expires_at > clock_timestamp())`, diskID, params.Name).Scan(&active); err != nil {
			return err
		}
		if active {
			return ErrBusy
		}
		quota, err = m.versionQuota(ctx, q, diskID, control)
		if err != nil {
			return err
		}
		unlock = m.lockBranchWrite(disk, params.Name)
		if err := m.pebble.EvictBranch(disk, params.Name); err != nil {
			return err
		}
		if err := os.Rename(path, backup); err != nil {
			return err
		}
		moved = true
		branches = append(branches[:i], branches[i+1:]...)
		if err := saveBranches(ctx, q, diskID, control.Branches, branches); err != nil {
			return err
		}
		quota.mu.Lock()
		quotaLocked = true
		oldSizes = quota.branches[params.Name]
		if err := quota.replaceBranchLocked(params.Name, nil, false); err != nil {
			return err
		}
		quotaChanged = true
		return nil
	})
	committed = err == nil
	if moved {
		if err != nil {
			err = errors.Join(err, os.Rename(backup, path))
		} else {
			err = os.RemoveAll(backup)
		}
	}
	if err == nil {
		m.invalidatePebbleBranchSessions(disk, params.Name)
	}
	return err
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
	path := filepath.Join(m.storageRoot, "disks", disk, "branches", params.BranchName)
	staging := ".restore-" + uuid.Must(uuid.NewV7()).String()
	stagePath := filepath.Join(filepath.Dir(path), staging)
	backup := filepath.Join(filepath.Dir(path), ".backup-"+uuid.Must(uuid.NewV7()).String())
	var unlock func()
	var quota *TreeQuota
	var oldSizes map[uint64]uint64
	var moved, installed, quotaLocked, quotaChanged, committed bool
	defer func() {
		if quotaLocked {
			if !committed && quotaChanged {
				err = errors.Join(err, quota.replaceBranchLocked(params.BranchName, oldSizes, false))
			}
			quota.mu.Unlock()
		}
		if unlock != nil {
			unlock()
		}
	}()
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		control, branches, err := lockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		raw := q.(interface{ RawDBTX() db.DBTX }).RawDBTX()
		if err := lockBranchWriter(ctx, raw, diskID, params.BranchName); err != nil {
			return err
		}
		i := branchIndex(branches, params.BranchName)
		if i < 0 {
			return ErrNotFound
		}
		_, err = q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: params.BranchName})
		if err == nil {
			return ErrBusy
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if abort {
			if branches[i].State != "MERGING" || branches[i].HeadCommitID == nil || len(branches[i].MergeState) == 0 {
				return ErrInvalidArgument
			}
			var state storedMergeState
			if err := json.Unmarshal(branches[i].MergeState, &state); err != nil || state.TargetCommitID != *branches[i].HeadCommitID {
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
		quota, err = m.versionQuota(ctx, q, diskID, control)
		if err != nil {
			return err
		}
		unlock = m.lockBranchWrite(disk, params.BranchName)
		if err := m.pebble.EvictBranch(disk, params.BranchName); err != nil {
			return err
		}
		if err := m.pebble.Fork(disk, commitID.String(), staging); err != nil {
			return err
		}
		workspace, err := m.pebble.OpenBranch(disk, staging)
		if err != nil {
			return err
		}
		err = prepareBranch(workspace.DB(), staging)
		var sizes map[uint64]uint64
		if err == nil {
			sizes, err = readBranchSizesFromDB(workspace.DB())
		}
		err = errors.Join(err, workspace.Close())
		if err != nil {
			return err
		}
		if err := os.Rename(path, backup); err != nil {
			return err
		}
		moved = true
		if err := os.Rename(stagePath, path); err != nil {
			return err
		}
		installed = true
		id := commitID.String()
		branches[i].HeadCommitID = &id
		branches[i].State = "NORMAL"
		branches[i].MergeState = nil
		if err := saveBranches(ctx, q, diskID, control.Branches, branches); err != nil {
			return err
		}
		quota.mu.Lock()
		quotaLocked = true
		oldSizes = quota.branches[params.BranchName]
		if err := quota.replaceBranchLocked(params.BranchName, sizes, false); err != nil {
			return err
		}
		quotaChanged = true
		return nil
	})
	committed = err == nil
	if err != nil && moved {
		if installed {
			err = errors.Join(err, os.RemoveAll(path))
		}
		err = errors.Join(err, os.Rename(backup, path))
	} else if err == nil && moved {
		err = os.RemoveAll(backup)
	}
	err = errors.Join(err, os.RemoveAll(stagePath))
	return err
}
