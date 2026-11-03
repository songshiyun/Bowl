package meta

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type sessionIDContextKey struct{}

type openInClientContextKey struct{}

// WithOpenInClient 将客户端本地打开状态放入请求上下文。
func WithOpenInClient(ctx context.Context, open bool) context.Context {
	return context.WithValue(ctx, openInClientContextKey{}, open)
}

// OpenInClientFromContext 返回客户端是否设置了本地打开状态。
func OpenInClientFromContext(ctx context.Context) (bool, bool) {
	open, ok := ctx.Value(openInClientContextKey{}).(bool)
	return open, ok
}

// WithSessionID 将客户端会话标识放入请求上下文。
func WithSessionID(ctx context.Context, sessionID *uint64) context.Context {
	return context.WithValue(ctx, sessionIDContextKey{}, sessionID)
}

// SessionIDFromContext 返回请求上下文中设置的客户端会话标识。
func SessionIDFromContext(ctx context.Context) (*uint64, bool) {
	id, ok := ctx.Value(sessionIDContextKey{}).(*uint64)
	return id, ok
}

type branchLockKey struct{ disk, branch string }

func (m *manager) lockBranch(disk, branch string) func() {
	value, _ := m.branchLocks.LoadOrStore(branchLockKey{disk, branch}, &sync.Mutex{})
	mu := value.(*sync.Mutex)
	mu.Lock()
	return mu.Unlock
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
	row, err := q.GetDisk(ctx, diskID)
	if err != nil {
		return err
	}
	control, err := q.GetDiskControlPlane(ctx, diskID)
	if err != nil {
		return err
	}
	quota, err := m.loadTreeQuota(ctx, row, control)
	if err != nil {
		return err
	}
	unlock := m.lockBranch(disk, branchName)
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
		if err := reclaimBranchOrphan(branch.DB(), quota, branchName, ino); err != nil {
			return fmt.Errorf("回收分支 %s 的孤儿节点 %d: %w", branchName, ino, err)
		}
	}
	return iter.Error()
}

func reclaimBranchOrphan(store *pebble.DB, quota *TreeQuota, branch string, ino uint64) (err error) {
	var size uint64
	value, closer, err := store.Get(pebblekv.InodeKey(ino))
	exists := err == nil
	if exists {
		inode, decodeErr := pebblekv.DecodeInode(value)
		closeErr := closer.Close()
		if err := errors.Join(decodeErr, closeErr); err != nil {
			return err
		}
		if inode.Ino != ino {
			return fmt.Errorf("Inode 编号与键不一致")
		}
		size = inode.Size
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}
	batch := store.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	if err := batch.Delete(pebblekv.InodeKey(ino), nil); err != nil {
		return err
	}
	lower, upper := pebblekv.XattrRange(ino)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	lower, upper = pebblekv.DataRange(ino, 0)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	if err := batch.Delete(pebblekv.OrphanKey(ino), nil); err != nil {
		return err
	}
	if exists {
		value, closer, err := store.Get(pebblekv.StatKey())
		if err != nil {
			return err
		}
		stat, decodeErr := pebblekv.DecodeBranchStat(value)
		closeErr := closer.Close()
		if err := errors.Join(decodeErr, closeErr); err != nil {
			return err
		}
		stat.Inodes -= min(stat.Inodes, 1)
		stat.LogicalBytes -= min(stat.LogicalBytes, size)
		if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); err != nil {
			return err
		}
	}
	return quota.Apply(branch, ino, 0, false, func() error { return batch.Commit(pebble.Sync) })
}

func lockSession(ctx context.Context, conn db.DBTX, sessionID uint64) error {
	_, err := conn.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('bowl:session:' || $1::bigint::text, 0))`, int64(sessionID))
	return err
}

func lockBranchWriter(ctx context.Context, conn db.DBTX, diskID pgtype.UUID, branch string) error {
	_, err := conn.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('bowl:writer:' || $1::uuid::text || ':' || $2::text, 0))`, diskID, branch)
	return err
}

func (m *manager) closeBranchSession(ctx context.Context, sessionID uint64) (bool, error) {
	managed := false
	needsSweep := false
	err := m.store.ExecTx(ctx, func(q db.Querier) error {
		raw := q.(interface{ RawDBTX() db.DBTX }).RawDBTX()
		if err := lockSession(ctx, raw, sessionID); err != nil {
			return err
		}
		var diskID pgtype.UUID
		var branch string
		var readOnly bool
		err := raw.QueryRow(ctx, `SELECT disk_id, branch_name, read_only FROM client_sessions WHERE session_id = $1 AND lease_managed`, int64(sessionID)).Scan(&diskID, &branch, &readOnly)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		managed = true
		if !readOnly {
			if err := lockBranchWriter(ctx, raw, diskID, branch); err != nil {
				return err
			}
			// 先持久化写端失效，再清扫 Pebble；后续事务失败时旧写端不会复活。
			_, err := raw.Exec(ctx, `UPDATE client_sessions SET expires_at = LEAST(expires_at, clock_timestamp()) WHERE session_id = $1 AND lease_managed`, int64(sessionID))
			needsSweep = err == nil
			return err
		}
		if _, err := raw.Exec(ctx, `DELETE FROM client_sessions WHERE session_id = $1 AND lease_managed`, int64(sessionID)); err != nil {
			return err
		}
		return nil
	})
	if err == nil && needsSweep {
		err = m.sweepExpiredBranchSession(ctx, sessionID)
	}
	return managed, err
}

// sweepExpiredBranchSessions 保留写端租约记录，直到该分支的清扫成功。
func (m *manager) sweepExpiredBranchSessions(ctx context.Context) error {
	pp, ok := m.store.(poolProvider)
	if !ok || pp == nil || pp.Pool() == nil {
		return nil
	}
	rows, err := pp.Pool().Query(ctx, `SELECT session_id FROM client_sessions
		WHERE lease_managed AND expires_at <= clock_timestamp() ORDER BY session_id`)
	if err != nil {
		return err
	}
	var sessions []uint64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		sessions = append(sessions, uint64(id))
	}
	err = rows.Err()
	rows.Close()
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
	deleted := false
	err := m.store.ExecTx(ctx, func(q db.Querier) error {
		raw := q.(interface{ RawDBTX() db.DBTX }).RawDBTX()
		if err := lockSession(ctx, raw, sessionID); err != nil {
			return err
		}
		var diskID pgtype.UUID
		var branch string
		var readOnly bool
		err := raw.QueryRow(ctx, `SELECT disk_id, branch_name, read_only FROM client_sessions
			WHERE session_id = $1 AND lease_managed AND expires_at <= clock_timestamp()`, int64(sessionID)).Scan(&diskID, &branch, &readOnly)
		if errors.Is(err, pgx.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		if !readOnly {
			if err := lockBranchWriter(ctx, raw, diskID, branch); err != nil {
				return err
			}
		}
		tag, err := raw.Exec(ctx, `DELETE FROM client_sessions WHERE session_id = $1
			AND lease_managed AND expires_at <= clock_timestamp()`, int64(sessionID))
		if err != nil || tag.RowsAffected() == 0 {
			return err
		}
		deleted = true
		if readOnly {
			return nil
		}
		_, err = q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: branch})
		if err == nil {
			return nil
		}
		if !errors.Is(err, pgx.ErrNoRows) {
			return err
		}
		return m.sweepBranchOrphans(ctx, q, diskID, branch)
	})
	if err == nil && deleted {
		m.clearSessionCursors(sessionID)
	}
	return err
}
