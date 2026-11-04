package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

var errMergeConflicts = errors.New("合并存在冲突")

// Merge 将源分支的无冲突变更合入没有写端的目标分支。
func (m *manager) Merge(ctx context.Context, params MergeParams) (result *MergeResult, err error) {
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if err := validateBranchName(params.SourceBranch); err != nil {
		return nil, err
	}
	if err := validateBranchName(params.TargetBranch); err != nil {
		return nil, err
	}
	if params.SourceBranch == params.TargetBranch || params.Strategy < ConflictStrategyUnspecified || params.Strategy > ConflictStrategyMaterialize {
		return nil, ErrInvalidArgument
	}
	if err := m.sweepExpiredBranchSessions(ctx); err != nil {
		return nil, err
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	disk := diskID.String()
	branchDir := filepath.Join(m.storageRoot, "disks", disk, "branches")
	targetPath := filepath.Join(branchDir, params.TargetBranch)
	staging := ".merge-" + uuid.Must(uuid.NewV7()).String()
	stagePath := filepath.Join(branchDir, staging)
	backupPath := filepath.Join(branchDir, ".backup-"+uuid.Must(uuid.NewV7()).String())
	var source, target *pebblekv.Branch
	var unlockSource, unlockTarget func()
	var created []string
	var quota *TreeQuota
	var oldSizes map[uint64]uint64
	var quotaLocked, quotaChanged, moved, installed bool
	var sourcePinned, targetPinned bool
	var workspaceCloseErr error
	defer func() {
		if source != nil {
			err = errors.Join(err, source.Close())
		}
		if target != nil {
			err = errors.Join(err, target.Close())
		}
		if quotaLocked {
			quota.mu.Unlock()
		}
		if unlockTarget != nil {
			unlockTarget()
		}
		if unlockSource != nil {
			unlockSource()
		}
	}()
	err = m.store.ExecTx(ctx, func(q db.Querier) (txErr error) {
		control, branches, err := lockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		si, ti := branchIndex(branches, params.SourceBranch), branchIndex(branches, params.TargetBranch)
		if si < 0 || ti < 0 {
			return ErrNotFound
		}
		if branches[si].State == "MERGING" || branches[ti].State == "MERGING" {
			return ErrBusy
		}
		raw := q.(interface{ RawDBTX() db.DBTX }).RawDBTX()
		if err := lockBranchWriter(ctx, raw, diskID, params.TargetBranch); err != nil {
			return err
		}
		_, err = q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: params.TargetBranch})
		if err == nil {
			return ErrBusy
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		_, err = q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: params.SourceBranch})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		quota, err = m.versionQuota(ctx, q, diskID, control)
		if err != nil {
			return err
		}
		unlockSource = m.lockBranchWrite(disk, params.SourceBranch)
		unlockTarget = m.lockBranchWrite(disk, params.TargetBranch)
		source, err = m.pebble.OpenBranch(disk, params.SourceBranch)
		if err != nil {
			return err
		}
		target, err = m.pebble.OpenBranch(disk, params.TargetBranch)
		if err != nil {
			return err
		}
		pin := func(branch *pebblekv.Branch, stored *storedBranch) (bool, error) {
			dirty, err := branchHasDirty(branch.DB())
			if err != nil {
				return false, err
			}
			if !dirty && stored.HeadCommitID != nil {
				return false, nil
			}
			id := uuid.Must(uuid.NewV7())
			created = append(created, id.String())
			_, err = m.createVersion(ctx, q, diskID, stored, branch, id, "")
			return err == nil, err
		}
		sourcePinned, err = pin(source, &branches[si])
		if err != nil {
			return err
		}
		targetPinned, err = pin(target, &branches[ti])
		if err != nil {
			return err
		}
		sourceID, err := parseDiskID(*branches[si].HeadCommitID)
		if err != nil {
			return err
		}
		targetID, err := parseDiskID(*branches[ti].HeadCommitID)
		if err != nil {
			return err
		}
		graph, sourceAncestors, targetAncestors, err := loadMergeGraph(ctx, q, diskID, sourceID, targetID)
		if err != nil {
			return err
		}
		if targetAncestors[sourceID] {
			id := targetID.String()
			result = &MergeResult{Result: MergeResultMerged, MergeCommitID: &id}
			return saveBranches(ctx, q, diskID, control.Branches, branches)
		}
		baseID, err := graph.lowestMergeBase(sourceID, targetID)
		if err != nil {
			return err
		}
		dirty, err := m.mergeDirtyInodes(ctx, disk, graph, baseID, sourceAncestors, targetAncestors)
		if err != nil {
			return err
		}
		if err := m.pebble.Fork(disk, targetID.String(), staging); err != nil {
			return err
		}
		workspace, err := m.pebble.OpenBranch(disk, staging)
		if err != nil {
			return err
		}
		defer func() {
			workspaceCloseErr = workspace.Close()
			if workspaceCloseErr != nil {
				txErr = errors.Join(txErr, workspaceCloseErr)
			}
		}()
		if err := prepareBranch(workspace.DB(), staging); err != nil {
			return err
		}
		var snapshots [3]*pebblekv.Commit
		for i, id := range []pgtype.UUID{baseID, targetID, sourceID} {
			snapshots[i], err = m.pebble.OpenCommit(disk, id.String())
			if err != nil {
				for j := 0; j < i; j++ {
					err = errors.Join(err, snapshots[j].Close())
				}
				return err
			}
		}
		stores := [3]*pebble.DB{snapshots[0].DB(), snapshots[1].DB(), snapshots[2].DB()}
		changedSizes, conflicts, mergeErr := m.mergeWorkspace(workspace.DB(), stores, dirty, params.Strategy)
		var sidecarPaths []string
		var sidecarInodes []uint64
		if mergeErr == nil && len(conflicts) != 0 && params.Strategy == ConflictStrategyMaterialize {
			sidecarPaths, sidecarInodes, mergeErr = m.materializeMergeConflicts(disk, workspace.DB(), stores, conflicts)
		}
		for _, snapshot := range snapshots {
			mergeErr = errors.Join(mergeErr, snapshot.Close())
		}
		if mergeErr != nil {
			return mergeErr
		}
		if len(conflicts) != 0 && (params.Strategy == ConflictStrategyUnspecified || params.Strategy == ConflictStrategyAbortOnConflict) {
			result = &MergeResult{Result: MergeResultConflicts, Conflicts: conflicts}
			return errMergeConflicts
		}
		materialized := len(conflicts) != 0 && params.Strategy == ConflictStrategyMaterialize
		if materialized {
			state := storedMergeState{BaseCommitID: baseID.String(), SourceBranch: params.SourceBranch, SourceCommitID: sourceID.String(), TargetCommitID: targetID.String(), SidecarPaths: sidecarPaths, SidecarInodes: sidecarInodes, Conflicts: conflicts}
			branches[ti].MergeState, err = json.Marshal(state)
			if err != nil {
				return err
			}
			branches[ti].State = "MERGING"
			result = &MergeResult{Result: MergeResultMaterialized, Conflicts: conflicts}
		} else {
			mergeID := uuid.Must(uuid.NewV7())
			created = append(created, mergeID.String())
			if err := workspace.Checkpoint(mergeID.String()); err != nil {
				return err
			}
			_, err = q.CreateCommit(ctx, db.CreateCommitParams{
				CommitID: pgtype.UUID{Bytes: mergeID, Valid: true}, DiskID: diskID, BranchName: params.TargetBranch,
				ParentCommitID: targetID, MergeParentCommitID: sourceID,
				CheckpointDir: filepath.Join(m.storageRoot, "disks", disk, "commits", mergeID.String()),
			})
			if err != nil {
				return err
			}
			if err := clearBranchDirty(workspace.DB()); err != nil {
				return err
			}
			id := mergeID.String()
			branches[ti].HeadCommitID = &id
			branches[ti].State, branches[ti].MergeState = "NORMAL", nil
			result = &MergeResult{Result: MergeResultMerged, MergeCommitID: &id}
		}
		for _, ino := range sidecarInodes {
			node, err := readCreateInode(workspace.DB(), ino)
			if err != nil {
				return err
			}
			changedSizes[ino] = node.Size
		}
		if err := workspace.Close(); err != nil {
			return err
		}
		if err := target.Close(); err != nil {
			return err
		}
		target = nil
		if err := m.pebble.EvictBranch(disk, params.TargetBranch); err != nil {
			return err
		}
		if err := os.Rename(targetPath, backupPath); err != nil {
			return err
		}
		moved = true
		if err := os.Rename(stagePath, targetPath); err != nil {
			return err
		}
		installed = true
		if err := saveBranches(ctx, q, diskID, control.Branches, branches); err != nil {
			return err
		}
		quota.mu.Lock()
		quotaLocked = true
		oldSizes = quota.branches[params.TargetBranch]
		sizes := make(map[uint64]uint64, len(oldSizes)+len(changedSizes))
		for ino, size := range oldSizes {
			if !dirty[ino] {
				sizes[ino] = size
			}
		}
		for ino, size := range changedSizes {
			sizes[ino] = size
		}
		if err := quota.replaceBranchLocked(params.TargetBranch, sizes, materialized); err != nil {
			return err
		}
		quotaChanged = true
		return nil
	})
	conflicted := errors.Is(err, errMergeConflicts) && workspaceCloseErr == nil
	var cleanupErr error
	if err != nil {
		if quotaLocked && quotaChanged {
			cleanupErr = errors.Join(cleanupErr, quota.replaceBranchLocked(params.TargetBranch, oldSizes, false))
		}
		if moved {
			if installed {
				cleanupErr = errors.Join(cleanupErr, os.RemoveAll(targetPath))
			}
			cleanupErr = errors.Join(cleanupErr, os.Rename(backupPath, targetPath))
		}
		for _, id := range created {
			cleanupErr = errors.Join(cleanupErr, os.RemoveAll(filepath.Join(m.storageRoot, "disks", disk, "commits", id)))
		}
	} else {
		if moved {
			err = errors.Join(err, os.RemoveAll(backupPath))
		}
		if sourcePinned {
			err = errors.Join(err, clearBranchDirty(source.DB()))
		}
		if targetPinned && target != nil {
			err = errors.Join(err, clearBranchDirty(target.DB()))
		}
	}
	cleanupErr = errors.Join(cleanupErr, os.RemoveAll(stagePath))
	if conflicted && cleanupErr == nil {
		return result, nil
	}
	err = errors.Join(err, cleanupErr)
	if err != nil {
		return nil, fmt.Errorf("合并分支: %w", err)
	}
	return result, nil
}
