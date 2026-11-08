// Package meta 实现虚拟磁盘、分支控制面与文件系统元数据管理。
package meta

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type rawDBTXProvider interface {
	RawDBTX() db.DBTX
}

// BranchControlStore 统一封装分支拓扑 JSONB、事务咨询锁与客户端会话租约的控制面数据库操作。
type BranchControlStore struct {
	store db.Store
}

func newBranchControlStore(store db.Store) BranchControlStore {
	return BranchControlStore{store: store}
}

func rawConn(q db.Querier) (db.DBTX, error) {
	raw, ok := q.(rawDBTXProvider)
	if !ok || raw.RawDBTX() == nil {
		return nil, errors.New("会话事务无法访问数据库")
	}
	return raw.RawDBTX(), nil
}

// LockSession 获取单会话事务级咨询锁。
func (s BranchControlStore) LockSession(ctx context.Context, q db.Querier, sessionID uint64) error {
	conn, err := rawConn(q)
	if err != nil {
		return err
	}
	return lockSession(ctx, conn, sessionID)
}

// LockBranchWriter 获取单分支写端事务级咨询锁。
func (s BranchControlStore) LockBranchWriter(ctx context.Context, q db.Querier, diskID pgtype.UUID, branch string) error {
	conn, err := rawConn(q)
	if err != nil {
		return err
	}
	return lockBranchWriter(ctx, conn, diskID, branch)
}

// LockedBranches 锁定磁盘控制面行并解码分支拓扑。
//
//nolint:revive // unexported-return: 仅在 meta 包内部共享分支拓扑记录。
func (s BranchControlStore) LockedBranches(ctx context.Context, q db.Querier, diskID pgtype.UUID) (db.GetDiskControlPlaneForUpdateRow, []storedBranch, error) {
	return lockedBranches(ctx, q, diskID)
}

// FindBranch 在已解码的分支切片中按名称查找分支指针与下标。
//
//nolint:revive // unexported-return: 仅在 meta 包内部共享分支拓扑记录。
func (s BranchControlStore) FindBranch(branches []storedBranch, name string) (branch *storedBranch, index int) {
	i := branchIndex(branches, name)
	if i < 0 {
		return nil, -1
	}
	return &branches[i], i
}

// SaveBranches 编码并通过 CAS 保存分支拓扑。
func (s BranchControlStore) SaveBranches(ctx context.Context, q db.Querier, diskID pgtype.UUID, old []byte, branches []storedBranch) error {
	return saveBranches(ctx, q, diskID, old, branches)
}

type controlPlaneSessionRecord struct {
	diskID    pgtype.UUID
	branch    string
	readOnly  bool
	commitID  pgtype.UUID
	managed   bool
	active    bool
	createdAt time.Time
}

// InspectSession 在事务内查询会话的当前绑定属性与存活状态。
//
//nolint:revive // unexported-return: 仅在 meta 包内部共享会话控制面记录。
func (s BranchControlStore) InspectSession(ctx context.Context, q db.Querier, sessionID uint64) (controlPlaneSessionRecord, bool, error) {
	conn, err := rawConn(q)
	if err != nil {
		return controlPlaneSessionRecord{}, false, err
	}
	var rec controlPlaneSessionRecord
	var created pgtype.Timestamptz
	err = conn.QueryRow(ctx, `SELECT disk_id, branch_name, read_only, commit_id, lease_managed, expires_at > clock_timestamp(), created_at
		FROM client_sessions WHERE session_id = $1`, int64(sessionID)).Scan(
		&rec.diskID, &rec.branch, &rec.readOnly, &rec.commitID, &rec.managed, &rec.active, &created)
	if errors.Is(err, pgx.ErrNoRows) {
		return controlPlaneSessionRecord{}, false, nil
	}
	if err != nil {
		return controlPlaneSessionRecord{}, false, err
	}
	rec.createdAt = created.Time
	return rec, true, nil
}

// IsSessionExpired 检查指定会话记录是否已达过期时间。
func (s BranchControlStore) IsSessionExpired(ctx context.Context, q db.Querier, sessionID uint64) (expired, exists bool, err error) {
	conn, err := rawConn(q)
	if err != nil {
		return false, false, err
	}
	err = conn.QueryRow(ctx, `SELECT expires_at <= clock_timestamp() FROM client_sessions WHERE session_id = $1`, int64(sessionID)).Scan(&expired)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	return expired, true, nil
}

// HasActiveBranchSessions 检查指定分支是否存在未过期的受管会话（读或写）。
func (s BranchControlStore) HasActiveBranchSessions(ctx context.Context, q db.Querier, diskID pgtype.UUID, branch string) (bool, error) {
	conn, err := rawConn(q)
	if err != nil {
		return false, err
	}
	var active bool
	err = conn.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM client_sessions WHERE lease_managed AND disk_id = $1 AND branch_name = $2 AND expires_at > clock_timestamp())`, diskID, branch).Scan(&active)
	return active, err
}

// HasActiveBranchWriter 检查指定分支是否存在未过期的活跃写端会话。
func (s BranchControlStore) HasActiveBranchWriter(ctx context.Context, q db.Querier, diskID pgtype.UUID, branch string) (bool, error) {
	_, err := q.GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: branch})
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// RenewSession 调用控制面续期函数并将 PostgreSQL 约束错误映射为领域哨兵错误。
func (s BranchControlStore) RenewSession(ctx context.Context, q db.Querier, params *db.RenewControlPlaneSessionParams) (db.ClientSession, error) {
	session, err := q.RenewControlPlaneSession(ctx, *params)
	if err != nil {
		return db.ClientSession{}, mapControlPlaneSessionError(err)
	}
	return session, nil
}

func mapControlPlaneSessionError(err error) error {
	if pgErr, ok := errors.AsType[*pgconn.PgError](err); ok {
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

// DeactivateOrDeleteManagedSession 注销受管会话：可写会话先在当前事务内强制过期以待孤儿清扫，只读会话直接删除。
func (s BranchControlStore) DeactivateOrDeleteManagedSession(ctx context.Context, q db.Querier, sessionID uint64) (managed, needsSweep bool, err error) {
	conn, err := rawConn(q)
	if err != nil {
		return false, false, err
	}
	err = lockSession(ctx, conn, sessionID)
	if err != nil {
		return false, false, err
	}
	var diskID pgtype.UUID
	var branch string
	var readOnly bool
	err = conn.QueryRow(ctx, `SELECT disk_id, branch_name, read_only FROM client_sessions WHERE session_id = $1 AND lease_managed`, int64(sessionID)).Scan(&diskID, &branch, &readOnly)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, err
	}
	if !readOnly {
		err = lockBranchWriter(ctx, conn, diskID, branch)
		if err != nil {
			return true, false, err
		}
		// 先持久化写端失效，再清扫 Pebble；后续事务失败时旧写端不会复活。
		_, err = conn.Exec(ctx, `UPDATE client_sessions SET expires_at = LEAST(expires_at, clock_timestamp()) WHERE session_id = $1 AND lease_managed`, int64(sessionID))
		return true, err == nil, err
	}
	_, err = conn.Exec(ctx, `DELETE FROM client_sessions WHERE session_id = $1 AND lease_managed`, int64(sessionID))
	return true, false, err
}

// ListExpiredManagedSessionIDs 列出所有已过期的受管会话编号。
func (s BranchControlStore) ListExpiredManagedSessionIDs(ctx context.Context) ([]uint64, error) {
	pp, ok := s.store.(poolProvider)
	if !ok || pp == nil || pp.Pool() == nil {
		return nil, nil
	}
	rows, err := pp.Pool().Query(ctx, `SELECT session_id FROM client_sessions
		WHERE lease_managed AND expires_at <= clock_timestamp() ORDER BY session_id`)
	if err != nil {
		return nil, err
	}
	var sessions []uint64
	for rows.Next() {
		var id int64
		if scanErr := rows.Scan(&id); scanErr != nil {
			rows.Close()
			return nil, scanErr
		}
		sessions = append(sessions, uint64(id))
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return nil, err
	}
	return sessions, nil
}

type deletedExpiredSession struct {
	diskID   pgtype.UUID
	branch   string
	readOnly bool
	deleted  bool
}

// DeleteExpiredManagedSession 在持有咨询锁的事务内删除已过期的受管会话记录。
//
//nolint:revive // unexported-return: 仅在 meta 包内部共享过期会话删除结果。
func (s BranchControlStore) DeleteExpiredManagedSession(ctx context.Context, q db.Querier, sessionID uint64) (deletedExpiredSession, error) {
	conn, err := rawConn(q)
	if err != nil {
		return deletedExpiredSession{}, err
	}
	err = lockSession(ctx, conn, sessionID)
	if err != nil {
		return deletedExpiredSession{}, err
	}
	var out deletedExpiredSession
	err = conn.QueryRow(ctx, `SELECT disk_id, branch_name, read_only FROM client_sessions
		WHERE session_id = $1 AND lease_managed AND expires_at <= clock_timestamp()`, int64(sessionID)).Scan(&out.diskID, &out.branch, &out.readOnly)
	if errors.Is(err, pgx.ErrNoRows) {
		return deletedExpiredSession{}, nil
	}
	if err != nil {
		return deletedExpiredSession{}, err
	}
	if !out.readOnly {
		err = lockBranchWriter(ctx, conn, out.diskID, out.branch)
		if err != nil {
			return deletedExpiredSession{}, err
		}
	}
	tag, err := conn.Exec(ctx, `DELETE FROM client_sessions WHERE session_id = $1
		AND lease_managed AND expires_at <= clock_timestamp()`, int64(sessionID))
	if err != nil || tag.RowsAffected() == 0 {
		return deletedExpiredSession{}, err
	}
	out.deleted = true
	return out, nil
}

// FindSession 从连接池读取会话权威状态（不开启事务）。
//
//nolint:revive // unexported-return: 仅在 meta 包内部共享只读会话快照。
func (s BranchControlStore) FindSession(ctx context.Context, sessionID uint64) (pebbleReadSession, error) {
	pp, ok := s.store.(poolProvider)
	if !ok || pp == nil || pp.Pool() == nil {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	var session pebbleReadSession
	session.id = sessionID
	err := pp.Pool().QueryRow(ctx, `SELECT disk_id, branch_name, read_only, commit_id, lease_managed,
		expires_at > clock_timestamp(), created_at FROM client_sessions WHERE session_id = $1`, int64(sessionID)).Scan(
		&session.diskID, &session.branch, &session.readOnly, &session.commitID,
		&session.managed, &session.active, &session.createdAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return pebbleReadSession{}, ErrBadFileHandle
	}
	return session, err
}

func lockSession(ctx context.Context, conn db.DBTX, sessionID uint64) error {
	_, err := conn.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('bowl:session:' || $1::bigint::text, 0))`, int64(sessionID))
	return err
}

func lockBranchWriter(ctx context.Context, conn db.DBTX, diskID pgtype.UUID, branch string) error {
	_, err := conn.Exec(ctx, `SELECT pg_advisory_xact_lock(hashtextextended('bowl:writer:' || $1::uuid::text || ':' || $2::text, 0))`, diskID, branch)
	return err
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
