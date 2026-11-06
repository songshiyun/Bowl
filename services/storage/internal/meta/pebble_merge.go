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
	swapper := newWorkspaceSwapper(m)
	controlStore := newBranchControlStore(m.store)
	var source, target *pebblekv.Branch
	var unlockSource, unlockTarget func()
	var swap *WorkspaceSwap
	var created []string
	var sourcePinned, targetPinned bool
	var workspaceCloseErr error
	defer func() {
		if source != nil {
			err = errors.Join(err, source.Close())
		}
		if target != nil {
			err = errors.Join(err, target.Close())
		}
		swap.ReleaseUnlock()
		if unlockTarget != nil {
			unlockTarget()
		}
		if unlockSource != nil {
			unlockSource()
		}
	}()
	err = m.store.ExecTx(ctx, func(q db.Querier) (txErr error) {
		control, branches, err := controlStore.LockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		sourceBranch, _ := controlStore.FindBranch(branches, params.SourceBranch)
		targetBranch, _ := controlStore.FindBranch(branches, params.TargetBranch)
		if sourceBranch == nil || targetBranch == nil {
			return ErrNotFound
		}
		if sourceBranch.State == "MERGING" || targetBranch.State == "MERGING" {
			return ErrBusy
		}
		if err := controlStore.LockBranchWriter(ctx, q, diskID, params.TargetBranch); err != nil {
			return err
		}
		hasTargetWriter, err := controlStore.HasActiveBranchWriter(ctx, q, diskID, params.TargetBranch)
		if err != nil {
			return err
		}
		if hasTargetWriter {
			return ErrBusy
		}
		if _, err := controlStore.HasActiveBranchWriter(ctx, q, diskID, params.SourceBranch); err != nil {
			return err
		}
		quota, err := m.versionQuota(ctx, q, diskID, control)
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
			dirty, err := m.branchHasDirtyOrAtime(disk, stored.Name, branch.DB())
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
		sourcePinned, err = pin(source, sourceBranch)
		if err != nil {
			return err
		}
		targetPinned, err = pin(target, targetBranch)
		if err != nil {
			return err
		}
		sourceID, err := parseDiskID(*sourceBranch.HeadCommitID)
		if err != nil {
			return err
		}
		targetID, err := parseDiskID(*targetBranch.HeadCommitID)
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
			return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
		}
		baseID, err := graph.lowestMergeBase(sourceID, targetID)
		if err != nil {
			return err
		}
		dirty, err := m.mergeDirtyInodes(ctx, disk, graph, baseID, sourceAncestors, targetAncestors)
		if err != nil {
			return err
		}
		var workspace *pebblekv.Branch
		swap, workspace, err = swapper.PrepareStaging(disk, params.TargetBranch, targetID.String(), ".merge-", unlockTarget)
		unlockTarget = nil
		if err != nil {
			return err
		}
		defer func() {
			workspaceCloseErr = workspace.Close()
			if workspaceCloseErr != nil {
				txErr = errors.Join(txErr, workspaceCloseErr)
			}
		}()
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
		// 合并结果会直接复制源侧 DataKey；先把基准与源快照的块硬链接进暂存目录。
		for i, snapshot := range snapshots {
			if i == 1 {
				continue
			}
			if err := snapshot.Blobs().LinkAllTo(workspace.Blobs()); err != nil {
				for _, opened := range snapshots {
					err = errors.Join(err, opened.Close())
				}
				return err
			}
		}
		stores := [3]*pebble.DB{snapshots[0].DB(), snapshots[1].DB(), snapshots[2].DB()}
		blobStores := [3]*pebblekv.BlobStore{snapshots[0].Blobs(), snapshots[1].Blobs(), snapshots[2].Blobs()}
		changedSizes, conflicts, mergeErr := m.mergeWorkspace(workspace.DB(), workspace.Blobs(), stores, blobStores, dirty, params.Strategy)
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
			targetBranch.MergeState, err = json.Marshal(state)
			if err != nil {
				return err
			}
			targetBranch.State = "MERGING"
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
			targetBranch.HeadCommitID = &id
			targetBranch.State, targetBranch.MergeState = "NORMAL", nil
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
		if err := swap.InstallStaged(); err != nil {
			return err
		}
		if err := controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches); err != nil {
			return err
		}
		return swap.StagePatch(quota, dirty, changedSizes, materialized)
	})
	conflicted := errors.Is(err, errMergeConflicts) && workspaceCloseErr == nil
	if conflicted {
		cleanupErr := swap.Rollback()
		for _, id := range created {
			cleanupErr = errors.Join(cleanupErr, os.RemoveAll(filepath.Join(m.storageRoot, "disks", disk, "commits", id)))
		}
		if cleanupErr == nil {
			return result, nil
		}
		err = errors.Join(err, cleanupErr)
	} else {
		err = swap.Finalize(err)
		if err != nil {
			for _, id := range created {
				err = errors.Join(err, os.RemoveAll(filepath.Join(m.storageRoot, "disks", disk, "commits", id)))
			}
		}
	}
	if err == nil {
		if sourcePinned {
			err = errors.Join(err, clearBranchDirty(source.DB()))
		}
		if targetPinned && target != nil {
			err = errors.Join(err, clearBranchDirty(target.DB()))
		}
	}
	if err != nil {
		return nil, fmt.Errorf("合并分支: %w", err)
	}
	return result, nil
}
