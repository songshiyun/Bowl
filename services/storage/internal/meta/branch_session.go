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

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
	var result RenewSessionResult
	var checkpointDir string
	var cachedCommitID pgtype.UUID
	var cachedCreatedAt time.Time
	err = m.store.ExecTx(ctx, func(q db.Querier) error {
		raw, ok := q.(interface{ RawDBTX() db.DBTX })
		if !ok || raw.RawDBTX() == nil {
			return fmt.Errorf("会话事务无法访问数据库")
		}
		conn := raw.RawDBTX()
		if err := lockSession(ctx, conn, params.SessionID); err != nil {
			return err
		}
		control, err := q.GetDiskControlPlaneForUpdate(ctx, diskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		var branches []storedBranch
		if err := json.Unmarshal(control.Branches, &branches); err != nil {
			return fmt.Errorf("读取分支树: %w", err)
		}
		var target *storedBranch
		for i := range branches {
			if branches[i].Name == branchName {
				target = &branches[i]
				break
			}
		}
		if target == nil {
			return ErrNotFound
		}
		if !params.ReadOnly {
			if err := lockBranchWriter(ctx, conn, diskID, branchName); err != nil {
				return err
			}
		}
		var previousDisk, previousCommit pgtype.UUID
		var previousCreated pgtype.Timestamptz
		var previousBranch string
		var previousReadOnly, managed, active bool
		err = conn.QueryRow(ctx, `SELECT disk_id, branch_name, read_only, commit_id, lease_managed, expires_at > clock_timestamp(), created_at
			FROM client_sessions WHERE session_id = $1`, int64(params.SessionID)).Scan(
			&previousDisk, &previousBranch, &previousReadOnly, &previousCommit, &managed, &active, &previousCreated)
		exists := err == nil
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		if exists && (!managed || previousDisk != diskID || previousBranch != branchName || previousReadOnly != params.ReadOnly) {
			return ErrInvalidArgument
		}
		var commitID pgtype.UUID
		if params.ReadOnly {
			commit := params.CommitID
			if commit == nil && exists && active {
				id := previousCommit.String()
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
		if exists && previousCommit != commitID {
			return ErrInvalidArgument
		}
		swept := false
		if !params.ReadOnly && !active {
			_, err := q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: branchName})
			if errors.Is(err, pgx.ErrNoRows) {
				if err := m.sweepBranchOrphans(ctx, q, diskID, branchName); err != nil {
					return err
				}
				swept = true
			} else if err != nil {
				return err
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
		session, err := q.RenewControlPlaneSession(ctx, renew)
		if errors.Is(err, pgx.ErrNoRows) {
			var expired bool
			checkErr := conn.QueryRow(ctx, `SELECT expires_at <= clock_timestamp() FROM client_sessions WHERE session_id = $1`, int64(params.SessionID)).Scan(&expired)
			if errors.Is(checkErr, pgx.ErrNoRows) || checkErr == nil && !expired {
				return ErrInvalidArgument
			}
			if checkErr != nil {
				return checkErr
			}
			if !params.ReadOnly && !swept {
				_, checkErr = q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: branchName})
				if checkErr == nil {
					return ErrBusy
				}
				if !errors.Is(checkErr, pgx.ErrNoRows) {
					return checkErr
				}
				if checkErr = m.sweepBranchOrphans(ctx, q, diskID, branchName); checkErr != nil {
					return checkErr
				}
				swept = true
			}
			session, err = q.RenewControlPlaneSession(ctx, renew)
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrInvalidArgument
			}
		}
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) {
				switch pgErr.Code {
				case "23505":
					if pgErr.ConstraintName == "uq_client_sessions_managed_writer" {
						return ErrBusy
					}
				case "23503":
					return ErrNotFound
				case "23514":
					return ErrInvalidArgument
				}
			}
			return err
		}
		recreated := !exists || !previousCreated.Time.Equal(session.CreatedAt.Time)
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
	unlock := m.lockBranchWrite(diskID.String(), target.Name)
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
