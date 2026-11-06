package meta

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// RenewBranchSession 注册或续期分支会话。
func (m *manager) RenewBranchSession(ctx context.Context, params RenewBranchSessionParams) (RenewSessionResult, error) {
	if params.SessionID == 0 || params.SessionID > math.MaxInt64 {
		return RenewSessionResult{}, ErrInvalidArgument
	}
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return RenewSessionResult{}, err
	}
	branchName := "main"
	if params.BranchName != nil {
		branchName = *params.BranchName
	}
	if validateBranchName(branchName) != nil {
		return RenewSessionResult{}, ErrInvalidArgument
	}
	if !params.ReadOnly && params.CommitID != nil && *params.CommitID != "" {
		return RenewSessionResult{}, ErrInvalidArgument
	}
	controlStore := newBranchControlStore(m.store)
	var result RenewSessionResult
	var checkpointDir string
	var cachedCommitID pgtype.UUID
	var cachedCreatedAt time.Time
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		if err := controlStore.LockSession(ctx, q, params.SessionID); err != nil {
			return err
		}
		_, branches, err := controlStore.LockedBranches(ctx, q, diskID)
		if err != nil {
			return err
		}
		target, _ := controlStore.FindBranch(branches, branchName)
		if target == nil {
			return ErrNotFound
		}
		if !params.ReadOnly {
			if err := controlStore.LockBranchWriter(ctx, q, diskID, branchName); err != nil {
				return err
			}
		}
		previous, exists, err := controlStore.InspectSession(ctx, q, params.SessionID)
		if err != nil {
			return err
		}
		if exists && (!previous.managed || previous.diskID != diskID || previous.branch != branchName || previous.readOnly != params.ReadOnly) {
			return ErrInvalidArgument
		}
		var commitID pgtype.UUID
		if params.ReadOnly {
			commit := params.CommitID
			if commit == nil && exists && previous.active {
				id := previous.commitID.String()
				commit = &id
			} else if commit == nil {
				var id pgtype.UUID
				id, checkpointDir, err = m.snapshotBranchSession(ctx, q, diskID, target)
				if err != nil {
					return err
				}
				value := id.String()
				commit = &value
			}
			if commit == nil {
				return ErrNotFound
			}
			commitID, err = parseDiskID(*commit)
			if err != nil {
				return err
			}
			row, err := q.GetCommit(ctx, commitID)
			if errors.Is(err, pgx.ErrNoRows) || err == nil && row.DiskID != diskID {
				return ErrNotFound
			}
			if err != nil {
				return err
			}
		}
		if exists && previous.commitID != commitID {
			return ErrInvalidArgument
		}
		swept := false
		if !params.ReadOnly && !previous.active {
			hasWriter, err := controlStore.HasActiveBranchWriter(ctx, q, diskID, branchName)
			if err != nil {
				return err
			}
			if !hasWriter {
				if err := m.sweepBranchOrphans(ctx, q, diskID, branchName); err != nil {
					return err
				}
				swept = true
			}
		}
		ttl := m.sessionLeaseTTL
		if ttl <= 0 {
			ttl = DefaultSessionLeaseTTL
		}
		renew := db.RenewControlPlaneSessionParams{
			SessionID: int64(params.SessionID), DiskID: diskID, BranchName: branchName,
			ReadOnly: params.ReadOnly, CommitID: commitID, LeaseMicros: ttl.Microseconds(),
		}
		session, err := controlStore.RenewSession(ctx, q, renew)
		if errors.Is(err, pgx.ErrNoRows) {
			expired, found, checkErr := controlStore.IsSessionExpired(ctx, q, params.SessionID)
			if checkErr != nil {
				return checkErr
			}
			if !found || !expired {
				return ErrInvalidArgument
			}
			if !params.ReadOnly && !swept {
				hasWriter, checkErr := controlStore.HasActiveBranchWriter(ctx, q, diskID, branchName)
				if checkErr != nil {
					return checkErr
				}
				if hasWriter {
					return ErrBusy
				}
				if checkErr = m.sweepBranchOrphans(ctx, q, diskID, branchName); checkErr != nil {
					return checkErr
				}
				swept = true
			}
			session, err = controlStore.RenewSession(ctx, q, renew)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrInvalidArgument
			}
		}
		if err != nil {
			return err
		}
		recreated := !exists || !previous.createdAt.Equal(session.CreatedAt.Time)
		if recreated && !params.ReadOnly && !swept {
			// 新租约尚未提交，旧租约已过期，分支锁仍由本事务持有。
			if err := m.sweepBranchOrphans(ctx, q, diskID, branchName); err != nil {
				return err
			}
		}
		result = RenewSessionResult{ExpiresAt: session.ExpiresAt.Time, SessionRecreated: recreated}
		cachedCommitID, cachedCreatedAt = commitID, session.CreatedAt.Time
		return nil
	})
	if err != nil {
		if checkpointDir != "" {
			err = errors.Join(err, os.RemoveAll(checkpointDir))
		}
		return RenewSessionResult{}, err
	}
	m.cachePebbleSession(pebbleReadSession{
		id: params.SessionID, diskID: diskID, branch: branchName, commitID: cachedCommitID,
		readOnly: params.ReadOnly, managed: true, active: true, createdAt: cachedCreatedAt,
	})
	return result, nil
}

// snapshotBranchSession 为新只读会话固定工作区，且不推进分支头。
func (m *manager) snapshotBranchSession(ctx context.Context, q db.Querier, diskID pgtype.UUID, target *storedBranch) (id pgtype.UUID, checkpointDir string, err error) {
	unlock := m.locks.LockBranchWrite(diskID.String(), target.Name)
	defer unlock()
	branch, err := m.pebble.OpenBranch(diskID.String(), target.Name)
	if err != nil {
		return id, "", err
	}
	defer func() { err = errors.Join(err, branch.Close()) }()
	if err := m.flushPebbleAtimeEntriesLocked(diskID.String(), target.Name, branch.DB()); err != nil {
		return id, "", err
	}
	dirty, err := branchHasDirty(branch.DB())
	if err != nil {
		return id, "", err
	}
	var parent pgtype.UUID
	if target.HeadCommitID != nil {
		parent, err = parseDiskID(*target.HeadCommitID)
		if err != nil {
			return id, "", err
		}
		if !dirty {
			return parent, "", nil
		}
	}
	newID, err := uuid.NewV7()
	if err != nil {
		return id, "", err
	}
	id = pgtype.UUID{Bytes: newID, Valid: true}
	path := filepath.Join(m.storageRoot, "disks", diskID.String(), "commits", newID.String())
	if _, err := os.Stat(path); err == nil {
		return id, "", ErrAlreadyExists
	} else if !errors.Is(err, os.ErrNotExist) {
		return id, "", err
	}
	checkpointDir = path
	if err := branch.Checkpoint(newID.String()); err != nil {
		return id, checkpointDir, err
	}
	_, err = q.CreateCommit(ctx, db.CreateCommitParams{
		CommitID: id, DiskID: diskID, BranchName: target.Name,
		ParentCommitID: parent, CheckpointDir: checkpointDir,
	})
	return id, checkpointDir, err
}
