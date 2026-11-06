package meta_test

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"slices"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type controlPlaneExecer interface {
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
}

func controlPlaneUUID(t *testing.T, value string) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	require.NoError(t, id.Scan(value))
	return id
}

func requireCommitAncestors(t *testing.T, ancestors []db.Commit, base, left, right, merge pgtype.UUID) {
	t.Helper()
	require.Len(t, ancestors, 4)
	require.True(t, slices.ContainsFunc(ancestors, func(commit db.Commit) bool { return commit.CommitID == base }))
	require.True(t, slices.ContainsFunc(ancestors, func(commit db.Commit) bool { return commit.CommitID == left }))
	require.True(t, slices.ContainsFunc(ancestors, func(commit db.Commit) bool { return commit.CommitID == right }))
	require.True(t, slices.ContainsFunc(ancestors, func(commit db.Commit) bool { return commit.CommitID == merge }))
}

func controlPlaneBranchVariants(t *testing.T, raw []byte) [][]byte {
	t.Helper()
	var branches []map[string]any
	require.NoError(t, json.Unmarshal(raw, &branches))
	one, err := json.Marshal(append(append([]map[string]any{}, branches...), map[string]any{
		"branch_id": "10000000-0000-0000-0000-000000000001", "name": "one", "parent_branch": "main", "state": "NORMAL",
	}))
	require.NoError(t, err)
	two, err := json.Marshal(append(append([]map[string]any{}, branches...), map[string]any{
		"branch_id": "10000000-0000-0000-0000-000000000002", "name": "two", "parent_branch": "main", "state": "NORMAL",
	}))
	require.NoError(t, err)
	return [][]byte{one, two}
}

func casRaceHasSingleWinner(first, second error) bool {
	firstWon := first == nil && errors.Is(second, pgx.ErrNoRows)
	secondWon := second == nil && errors.Is(first, pgx.ErrNoRows)
	return firstWon || secondWon
}

func renewSessionsConcurrently(t *testing.T, q *db.Queries, ctx context.Context, diskID pgtype.UUID) (error, error) {
	t.Helper()
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, err := q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: 1, DiskID: diskID, BranchName: "main",
			LeaseMicros: int64(time.Minute / time.Microsecond),
		})
		errs <- err
	}()
	go func() {
		<-start
		_, err := q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: 2, DiskID: diskID, BranchName: "main",
			LeaseMicros: int64(time.Minute / time.Microsecond),
		})
		errs <- err
	}()
	close(start)
	return <-errs, <-errs
}

func requireSingleWriter(t *testing.T, first, second error) {
	t.Helper()
	require.NotEqual(t, first == nil, second == nil, "并发续约必须恰好一个成功")
	conflict := cmp.Or(first, second)
	var pgErr *pgconn.PgError
	require.ErrorAs(t, conflict, &pgErr)
	require.Equal(t, "23505", pgErr.Code)
	require.Equal(t, "uq_client_sessions_managed_writer", pgErr.ConstraintName)
}

func expireControlPlaneSession(t *testing.T, ctx context.Context, execer controlPlaneExecer, sessionID int64) {
	t.Helper()
	_, err := execer.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, sessionID)
	require.NoError(t, err)
}

// awaitBlockedLease 等待竞争写端真正阻塞在 PostgreSQL 锁上，而不是用固定时长猜测。
func awaitBlockedLease(t *testing.T, pool *pgxpool.Pool, ctx context.Context) {
	t.Helper()
	require.Eventually(t, func() bool {
		var blocked int
		return pool.QueryRow(ctx, `SELECT count(*) FROM pg_locks
			WHERE NOT granted AND database = (SELECT oid FROM pg_database WHERE datname = current_database())`).Scan(&blocked) == nil && blocked > 0
	}, 5*time.Second, 10*time.Millisecond, "竞争写端必须在数据库锁上等待")
}

func TestControlPlaneBranchConcurrentUpdates(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_branch_race")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	q := db.New(pool)
	disk, err := q.GetDiskControlPlane(ctx, diskID)
	require.NoError(t, err)
	variants := controlPlaneBranchVariants(t, disk.Branches)
	start := make(chan struct{})
	errs := make(chan error, 2)
	go func() {
		<-start
		_, casErr := q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
			DiskID: diskID, ExpectedBranches: disk.Branches, Branches: variants[0],
		})
		errs <- casErr
	}()
	go func() {
		<-start
		_, casErr := q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
			DiskID: diskID, ExpectedBranches: disk.Branches, Branches: variants[1],
		})
		errs <- casErr
	}()
	close(start)
	first, second := <-errs, <-errs
	require.True(t, casRaceHasSingleWinner(first, second), "并发 CAS 必须恰好一个成功、另一个返回 ErrNoRows")
	disk, err = q.GetDiskControlPlane(ctx, diskID)
	require.NoError(t, err)
	var stored []map[string]any
	require.NoError(t, json.Unmarshal(disk.Branches, &stored))
	require.Len(t, stored, 2)
	require.Contains(t, []string{"one", "two"}, stored[1]["name"])
	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: diskID, ExpectedBranches: disk.Branches, Branches: []byte(`[]`),
	})
	var constraint *pgconn.PgError
	require.ErrorAs(t, err, &constraint)
	require.Equal(t, "23514", constraint.Code)
}

func TestControlPlaneCommitAncestry(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_commit")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	otherDiskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000002")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024), ($2, 200, 1024)`, diskID, otherDiskID)
	require.NoError(t, err)
	q := db.New(pool)
	base := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000001")
	left := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000002")
	right := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000003")
	merge := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000004")
	createdBase, err := q.CreateCommit(ctx, db.CreateCommitParams{CommitID: base, DiskID: diskID, BranchName: "main", Message: "base", CheckpointDir: "/base"})
	require.NoError(t, err)
	require.True(t, createdBase.CreatedAt.Valid)
	createdLeft, err := q.CreateCommit(ctx, db.CreateCommitParams{CommitID: left, DiskID: diskID, BranchName: "main", ParentCommitID: base, Message: "left", CheckpointDir: "/left"})
	require.NoError(t, err)
	require.True(t, createdLeft.CreatedAt.Valid)
	createdRight, err := q.CreateCommit(ctx, db.CreateCommitParams{CommitID: right, DiskID: diskID, BranchName: "feature", ParentCommitID: base, Message: "right", CheckpointDir: "/right"})
	require.NoError(t, err)
	require.True(t, createdRight.CreatedAt.Valid)
	createdMerge, err := q.CreateCommit(ctx, db.CreateCommitParams{CommitID: merge, DiskID: diskID, BranchName: "main", ParentCommitID: left, MergeParentCommitID: right, Message: "merge", CheckpointDir: "/merge"})
	require.NoError(t, err)
	require.True(t, createdMerge.CreatedAt.Valid)

	got, err := q.GetCommit(ctx, merge)
	require.NoError(t, err)
	require.Equal(t, left, got.ParentCommitID)
	require.Equal(t, right, got.MergeParentCommitID)
	require.Equal(t, "/merge", got.CheckpointDir)
	branch, err := q.ListCommitsByBranch(ctx, db.ListCommitsByBranchParams{DiskID: diskID, BranchName: "feature"})
	require.NoError(t, err)
	require.Len(t, branch, 1)
	require.Equal(t, right, branch[0].CommitID)
	ancestors, err := q.ListCommitAncestors(ctx, db.ListCommitAncestorsParams{DiskID: diskID, CommitID: merge})
	require.NoError(t, err)
	requireCommitAncestors(t, ancestors, base, left, right, merge)
	_, err = q.CreateCommit(ctx, db.CreateCommitParams{CommitID: controlPlaneUUID(t, "20000000-0000-0000-0000-000000000001"), DiskID: otherDiskID, BranchName: "main", ParentCommitID: base, CheckpointDir: "/invalid"})
	var foreignKey *pgconn.PgError
	require.ErrorAs(t, err, &foreignKey)
	require.Equal(t, "23503", foreignKey.Code)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 1, DiskID: diskID, BranchName: "main", ReadOnly: true, CommitID: merge,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	deleted, err := q.DeleteDisk(ctx, diskID)
	require.NoError(t, err)
	require.EqualValues(t, 1, deleted)
	var commitCount, sessionCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM commits WHERE disk_id = $1`, diskID).Scan(&commitCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE disk_id = $1`, diskID).Scan(&sessionCount))
	require.Zero(t, commitCount)
	require.Zero(t, sessionCount)
}

func TestControlPlaneSessionLeases(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_lease")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	q := db.New(pool)
	commitID := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000001")
	_, err = q.CreateCommit(ctx, db.CreateCommitParams{CommitID: commitID, DiskID: diskID, BranchName: "main", CheckpointDir: "/base"})
	require.NoError(t, err)

	firstRenew, secondRenew := renewSessionsConcurrently(t, q, ctx, diskID)
	requireSingleWriter(t, firstRenew, secondRenew)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 100, DiskID: diskID, BranchName: "feature", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 101, DiskID: diskID, BranchName: "main", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 102, DiskID: diskID, BranchName: "main", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	readers, err := q.ListActiveCommitSessions(ctx, db.ListActiveCommitSessionsParams{DiskID: diskID, CommitID: commitID})
	require.NoError(t, err)
	require.Len(t, readers, 2)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 200, DiskID: diskID, BranchName: "short", LeaseMicros: int64(30 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	expireControlPlaneSession(t, ctx, pool, 200)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 201, DiskID: diskID, BranchName: "short", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 200, DiskID: diskID, BranchName: "short", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	var conflict *pgconn.PgError
	require.ErrorAs(t, err, &conflict)
	require.Equal(t, "uq_client_sessions_managed_writer", conflict.ConstraintName)

	renewed, err := q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 201, DiskID: diskID, BranchName: "short", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	require.Equal(t, int64(201), renewed.SessionID)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 300, DiskID: diskID, BranchName: "reader", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(30 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	expireControlPlaneSession(t, ctx, pool, 300)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 300, DiskID: diskID, BranchName: "reader", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 400, DiskID: diskID, BranchName: "same-writer", LeaseMicros: int64(30 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	expireControlPlaneSession(t, ctx, pool, 400)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 400, DiskID: diskID, BranchName: "same-writer", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 401, DiskID: diskID, BranchName: "main", ReadOnly: true,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	var constraint *pgconn.PgError
	require.ErrorAs(t, err, &constraint)
	require.Equal(t, "23514", constraint.Code)

	otherDiskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000002")
	_, err = pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 200, 1024)`, otherDiskID)
	require.NoError(t, err)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 402, DiskID: otherDiskID, BranchName: "main", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.ErrorAs(t, err, &constraint)
	require.Equal(t, "23503", constraint.Code)
}

func TestControlPlaneLeaseLocksArePerBranch(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_locks")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = db.New(tx).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 1, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	otherCtx, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	_, err = db.New(pool).RenewControlPlaneSession(otherCtx, db.RenewControlPlaneSessionParams{
		SessionID: 2, DiskID: diskID, BranchName: "feature", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
}

func TestControlPlaneLeaseStartsAfterConflictingTransaction(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_lease_wait")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = db.New(tx).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 1, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	type result struct {
		session db.ClientSession
		err     error
	}
	results := make(chan result, 1)
	go func() {
		session, renewErr := db.New(pool).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: 2, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Second / time.Microsecond),
		})
		results <- result{session: session, err: renewErr}
	}()
	awaitBlockedLease(t, pool, ctx)
	require.NoError(t, tx.Rollback(ctx))
	r := <-results
	require.NoError(t, r.err)
	var leaseStartsAfterWait bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT expires_at > clock_timestamp() + interval '700 milliseconds'
		FROM client_sessions WHERE session_id = 2`).Scan(&leaseStartsAfterWait))
	require.True(t, leaseStartsAfterWait, "租约必须从冲突事务结束后开始计时")
}

func TestControlPlaneLeaseTakesOverWriterExpiredWhileWaiting(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_expired_wait")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer func() { _ = tx.Rollback(context.Background()) }()
	_, err = db.New(tx).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 1, DiskID: diskID, BranchName: "main", LeaseMicros: int64(100 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	errs := make(chan error, 1)
	go func() {
		_, renewErr := db.New(pool).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: 2, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Minute / time.Microsecond),
		})
		errs <- renewErr
	}()
	awaitBlockedLease(t, pool, ctx)
	expireControlPlaneSession(t, ctx, tx, 1)
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-errs)
	writer, err := db.New(pool).GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: "main"})
	require.NoError(t, err)
	require.EqualValues(t, 2, writer.SessionID)
}

func TestControlPlaneSchemaMigrationDownUpRoundtrip(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_migration")
	sqlDB := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = sqlDB.Close() })

	require.NoError(t, goose.DownToContext(ctx, sqlDB, ".", 20261101000000), "回滚控制面迁移必须成功")
	version, err := goose.GetDBVersionContext(ctx, sqlDB)
	require.NoError(t, err)
	require.Equal(t, int64(20261101000000), version)

	var legacyTables, controlTables int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('inodes', 'entries', 'chunks')`).Scan(&legacyTables))
	require.Equal(t, 3, legacyTables, "回滚后必须恢复旧 POSIX 表")
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name = 'commits'`).Scan(&controlTables))
	require.Zero(t, controlTables, "回滚后控制面 commits 表必须消失")

	require.NoError(t, goose.UpContext(ctx, sqlDB, "."), "重新应用控制面迁移必须成功")
	version, err = goose.GetDBVersionContext(ctx, sqlDB)
	require.NoError(t, err)
	require.Equal(t, int64(20261104010000), version)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('inodes', 'entries', 'chunks')`).Scan(&legacyTables))
	require.Zero(t, legacyTables, "重新升级后旧 POSIX 表必须再次删除")

	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	otherDiskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000002")
	_, err = pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024), ($2, 200, 1024)`, diskID, otherDiskID)
	require.NoError(t, err)

	base := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000001")
	_, err = pool.Exec(ctx, `INSERT INTO commits (commit_id, disk_id, branch_name, checkpoint_dir)
		VALUES ($1, $2, 'main', '/base')`, base, diskID)
	require.NoError(t, err)

	foreign := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000002")
	_, err = pool.Exec(ctx, `INSERT INTO commits (commit_id, disk_id, branch_name, parent_commit_id, checkpoint_dir)
		VALUES ($1, $2, 'main', $3, '/foreign')`, foreign, otherDiskID, base)
	var foreignKey *pgconn.PgError
	require.ErrorAs(t, err, &foreignKey)
	require.Equal(t, "23503", foreignKey.Code)

	self := controlPlaneUUID(t, "10000000-0000-0000-0000-000000000003")
	_, err = pool.Exec(ctx, `INSERT INTO commits (commit_id, disk_id, branch_name, parent_commit_id, checkpoint_dir)
		VALUES ($1, $2, 'main', $1, '/self')`, self, diskID)
	var selfParent *pgconn.PgError
	require.ErrorAs(t, err, &selfParent)
	require.Equal(t, "23514", selfParent.Code)

	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, expires_at, disk_id, branch_name, lease_managed)
		VALUES (1, clock_timestamp() + interval '1 minute', $1, 'main', true)`, diskID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, expires_at, disk_id, branch_name, lease_managed)
		VALUES (2, clock_timestamp() + interval '1 minute', $1, 'main', true)`, diskID)
	var uniqueWriter *pgconn.PgError
	require.ErrorAs(t, err, &uniqueWriter)
	require.Equal(t, "23505", uniqueWriter.Code)
	require.Equal(t, "uq_client_sessions_managed_writer", uniqueWriter.ConstraintName)

	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, expires_at, disk_id, branch_name, read_only, lease_managed)
		VALUES (3, clock_timestamp() + interval '1 minute', $1, 'main', true, true)`, diskID)
	var sessionCheck *pgconn.PgError
	require.ErrorAs(t, err, &sessionCheck)
	require.Equal(t, "23514", sessionCheck.Code)

	_, err = pool.Exec(ctx, `UPDATE disks SET branches = '[]'::jsonb WHERE disk_id = $1`, diskID)
	var branchCheck *pgconn.PgError
	require.ErrorAs(t, err, &branchCheck)
	require.Equal(t, "23514", branchCheck.Code)

	var renewed int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM renew_control_plane_session(1, $1, 'main', false, NULL::uuid, 60000000)`, diskID).Scan(&renewed))
	require.Equal(t, 1, renewed, "迁移往返后 renew_control_plane_session 必须可用")

	_, err = pool.Exec(ctx, `DELETE FROM disks WHERE disk_id = $1`, diskID)
	require.NoError(t, err)
	var commitCount, sessionCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM commits WHERE disk_id = $1`, diskID).Scan(&commitCount))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE disk_id = $1`, diskID).Scan(&sessionCount))
	require.Zero(t, commitCount)
	require.Zero(t, sessionCount)
}
