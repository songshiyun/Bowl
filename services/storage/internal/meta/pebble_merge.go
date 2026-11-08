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
	if valErr := validateBranchName(params.SourceBranch); valErr != nil {
		return nil, valErr
	}
	if valErr := validateBranchName(params.TargetBranch); valErr != nil {
		return nil, valErr
	}
	if params.SourceBranch == params.TargetBranch || params.Strategy < ConflictStrategyUnspecified || params.Strategy > ConflictStrategyMaterialize {
		return nil, ErrInvalidArgument
	}
	if sweepErr := m.sweepExpiredBranchSessions(ctx); sweepErr != nil {
		return nil, sweepErr
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
		control, branches, lockErr := controlStore.LockedBranches(ctx, q, diskID)
		if lockErr != nil {
			return lockErr
		}
		sourceBranch, _ := controlStore.FindBranch(branches, params.SourceBranch)
		targetBranch, _ := controlStore.FindBranch(branches, params.TargetBranch)
		if sourceBranch == nil || targetBranch == nil {
			return ErrNotFound
		}
		if sourceBranch.State == "MERGING" || targetBranch.State == "MERGING" {
			return ErrBusy
		}
		if lockWriterErr := controlStore.LockBranchWriter(ctx, q, diskID, params.TargetBranch); lockWriterErr != nil {
			return lockWriterErr
		}
		hasTargetWriter, writerErr := controlStore.HasActiveBranchWriter(ctx, q, diskID, params.TargetBranch)
		if writerErr != nil {
			return writerErr
		}
		if hasTargetWriter {
			return ErrBusy
		}
		if _, srcWriterErr := controlStore.HasActiveBranchWriter(ctx, q, diskID, params.SourceBranch); srcWriterErr != nil {
			return srcWriterErr
		}
		quota, quotaErr := m.versionQuota(ctx, q, diskID, control)
		if quotaErr != nil {
			return quotaErr
		}
		unlockSource = m.locks.LockBranchWrite(disk, params.SourceBranch)
		unlockTarget = m.locks.LockBranchWrite(disk, params.TargetBranch)
		var openErr error
		source, openErr = m.pebble.OpenBranch(disk, params.SourceBranch)
		if openErr != nil {
			return openErr
		}
		target, openErr = m.pebble.OpenBranch(disk, params.TargetBranch)
		if openErr != nil {
			return openErr
		}
		pin := func(branch *pebblekv.Branch, stored *storedBranch) (bool, error) {
			dirty, dirtyErr := m.flushAndCheckBranchDirtyLocked(disk, stored.Name, branch.DB())
			if dirtyErr != nil {
				return false, dirtyErr
			}
			if !dirty && stored.HeadCommitID != nil {
				return false, nil
			}
			id := uuid.Must(uuid.NewV7())
			created = append(created, id.String())
			_, createErr := m.createVersion(ctx, q, diskID, stored, branch, id, "")
			return createErr == nil, createErr
		}
		sourcePinned, openErr = pin(source, sourceBranch)
		if openErr != nil {
			return openErr
		}
		targetPinned, openErr = pin(target, targetBranch)
		if openErr != nil {
			return openErr
		}
		sourceID, parseErr := parseDiskID(*sourceBranch.HeadCommitID)
		if parseErr != nil {
			return parseErr
		}
		targetID, parseErr := parseDiskID(*targetBranch.HeadCommitID)
		if parseErr != nil {
			return parseErr
		}
		graph, sourceAncestors, targetAncestors, graphErr := loadMergeGraph(ctx, q, diskID, sourceID, targetID)
		if graphErr != nil {
			return graphErr
		}
		if targetAncestors[sourceID] {
			id := targetID.String()
			result = &MergeResult{Result: MergeResultMerged, MergeCommitID: &id}
			return controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches)
		}
		baseID, baseErr := graph.lowestMergeBase(sourceID, targetID)
		if baseErr != nil {
			return baseErr
		}
		dirty, dirtyErr := m.mergeDirtyInodes(ctx, disk, graph, baseID, sourceAncestors, targetAncestors)
		if dirtyErr != nil {
			return dirtyErr
		}
		var workspace *pebblekv.Branch
		var stageErr error
		swap, workspace, stageErr = swapper.PrepareStaging(disk, params.TargetBranch, targetID.String(), ".merge-", unlockTarget)
		unlockTarget = nil
		if stageErr != nil {
			return stageErr
		}
		defer func() {
			workspaceCloseErr = workspace.Close()
			if workspaceCloseErr != nil {
				txErr = errors.Join(txErr, workspaceCloseErr)
			}
		}()
		var snapshots [3]*pebblekv.Commit
		for i, id := range []pgtype.UUID{baseID, targetID, sourceID} {
			var snapErr error
			snapshots[i], snapErr = m.pebble.OpenCommit(disk, id.String())
			if snapErr != nil {
				for j := range i {
					snapErr = errors.Join(snapErr, snapshots[j].Close())
				}
				return snapErr
			}
		}
		// 合并结果会直接复制源侧 DataKey；先把基准与源快照的块硬链接进暂存目录。
		for i, snapshot := range snapshots {
			if i == 1 {
				continue
			}
			if linkErr := snapshot.Blobs().LinkAllTo(workspace.Blobs()); linkErr != nil {
				for _, opened := range snapshots {
					linkErr = errors.Join(linkErr, opened.Close())
				}
				return linkErr
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
			var marshalErr error
			targetBranch.MergeState, marshalErr = json.Marshal(state)
			if marshalErr != nil {
				return marshalErr
			}
			targetBranch.State = "MERGING"
			result = &MergeResult{Result: MergeResultMaterialized, Conflicts: conflicts}
		} else {
			mergeID := uuid.Must(uuid.NewV7())
			created = append(created, mergeID.String())
			if ckptErr := workspace.Checkpoint(mergeID.String()); ckptErr != nil {
				return ckptErr
			}
			_, createCommitErr := q.CreateCommit(ctx, db.CreateCommitParams{
				CommitID: pgtype.UUID{Bytes: mergeID, Valid: true}, DiskID: diskID, BranchName: params.TargetBranch,
				ParentCommitID: targetID, MergeParentCommitID: sourceID,
				CheckpointDir: filepath.Join(m.storageRoot, "disks", disk, "commits", mergeID.String()),
			})
			if createCommitErr != nil {
				return createCommitErr
			}
			if clearErr := clearBranchDirty(workspace.DB()); clearErr != nil {
				return clearErr
			}
			id := mergeID.String()
			targetBranch.HeadCommitID = &id
			targetBranch.State, targetBranch.MergeState = "NORMAL", nil
			result = &MergeResult{Result: MergeResultMerged, MergeCommitID: &id}
		}
		for _, ino := range sidecarInodes {
			node, readErr := readCreateInode(workspace.DB(), ino)
			if readErr != nil {
				return readErr
			}
			changedSizes[ino] = node.Size
		}
		if closeErr := workspace.Close(); closeErr != nil {
			return closeErr
		}
		if closeErr := target.Close(); closeErr != nil {
			return closeErr
		}
		target = nil
		if installErr := swap.InstallStaged(); installErr != nil {
			return installErr
		}
		if saveErr := controlStore.SaveBranches(ctx, q, diskID, control.Branches, branches); saveErr != nil {
			return saveErr
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
