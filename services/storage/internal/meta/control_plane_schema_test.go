package meta_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

func TestControlPlaneDiskDefaultBranch(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_default")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, name, root_ino, max_bytes)
		VALUES ('00000000-0000-0000-0000-000000000001', 'disk', 100, 1024)`)
	require.NoError(t, err)

	var storageDir string
	var rawBranches []byte
	err = pool.QueryRow(ctx, `SELECT storage_dir, branches FROM disks WHERE root_ino = 100`).Scan(&storageDir, &rawBranches)
	require.NoError(t, err)
	require.Empty(t, storageDir)

	var branches []struct {
		BranchID     uuid.UUID `json:"branch_id"`
		Name         string    `json:"name"`
		ParentBranch *string   `json:"parent_branch"`
		ForkCommitID *string   `json:"fork_commit_id"`
		HeadCommitID *string   `json:"head_commit_id"`
		State        string    `json:"state"`
		MergeState   any       `json:"merge_state"`
	}
	require.NoError(t, json.Unmarshal(rawBranches, &branches))
	require.Len(t, branches, 1)
	require.NotEqual(t, uuid.Nil, branches[0].BranchID)
	require.Equal(t, "main", branches[0].Name)
	require.Nil(t, branches[0].ParentBranch)
	require.Nil(t, branches[0].ForkCommitID)
	require.Nil(t, branches[0].HeadCommitID)
	require.Equal(t, "NORMAL", branches[0].State)
	require.Nil(t, branches[0].MergeState)
}

func TestControlPlaneBranchUpdates(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_branch")
	var diskID pgtype.UUID
	require.NoError(t, diskID.Scan("00000000-0000-0000-0000-000000000001"))
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)

	q := db.New(pool)
	disk, err := q.GetDiskControlPlane(ctx, diskID)
	require.NoError(t, err)
	var branches []map[string]any
	require.NoError(t, json.Unmarshal(disk.Branches, &branches))
	branches = append(branches, map[string]any{
		"branch_id": "00000000-0000-0000-0000-000000000002", "name": "feature",
		"parent_branch": "main", "fork_commit_id": nil, "head_commit_id": nil,
		"state": "NORMAL", "merge_state": nil,
	})
	added, err := json.Marshal(branches)
	require.NoError(t, err)
	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: diskID, ExpectedBranches: disk.Branches, Branches: added,
	})
	require.NoError(t, err)

	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: diskID, ExpectedBranches: disk.Branches, Branches: disk.Branches,
	})
	require.ErrorIs(t, err, pgx.ErrNoRows)

	branches[1]["state"] = "MERGING"
	branches[1]["merge_state"] = map[string]any{"conflicts": []string{"file.txt"}}
	merging, err := json.Marshal(branches)
	require.NoError(t, err)
	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: diskID, ExpectedBranches: added, Branches: merging,
	})
	require.NoError(t, err)
	disk, err = q.GetDiskControlPlane(ctx, diskID)
	require.NoError(t, err)
	require.JSONEq(t, string(merging), string(disk.Branches))

	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: diskID, ExpectedBranches: merging, Branches: diskRootBranch(t, branches),
	})
	require.NoError(t, err)
	_, err = q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
		DiskID: diskID, ExpectedBranches: merging, Branches: merging,
	})
	require.True(t, errors.Is(err, pgx.ErrNoRows))
}

func TestControlPlaneBranchConcurrentUpdates(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_branch_race")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	q := db.New(pool)
	disk, err := q.GetDiskControlPlane(ctx, diskID)
	require.NoError(t, err)
	var branches []map[string]any
	require.NoError(t, json.Unmarshal(disk.Branches, &branches))
	variants := make([][]byte, 2)
	for i, name := range []string{"one", "two"} {
		variant := append(append([]map[string]any{}, branches...), map[string]any{
			"branch_id": []string{"10000000-0000-0000-0000-000000000001", "10000000-0000-0000-0000-000000000002"}[i],
			"name":      name, "parent_branch": "main", "state": "NORMAL",
		})
		variants[i], err = json.Marshal(variant)
		require.NoError(t, err)
	}
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, variant := range variants {
		go func(variant []byte) {
			<-start
			_, err := q.CompareAndSetDiskBranches(ctx, db.CompareAndSetDiskBranchesParams{
				DiskID: diskID, ExpectedBranches: disk.Branches, Branches: variant,
			})
			errs <- err
		}(variant)
	}
	close(start)
	first, second := <-errs, <-errs
	require.True(t, (first == nil && errors.Is(second, pgx.ErrNoRows)) || (second == nil && errors.Is(first, pgx.ErrNoRows)))
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

func TestControlPlaneMigrationRoundTrip(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_migration")
	raw, err := os.ReadFile("../../db/migrations/20261104000000_add_adr0029_control_plane_schema.sql")
	require.NoError(t, err)
	parts := strings.SplitN(string(raw), "-- +goose Down", 2)
	require.Len(t, parts, 2)
	_, err = pool.Exec(ctx, parts[1])
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES
		('00000000-0000-0000-0000-000000000001', 100, 1024),
		('00000000-0000-0000-0000-000000000002', 200, 1024)`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, disk_id, expires_at) VALUES
		(1, '00000000-0000-0000-0000-000000000001', clock_timestamp() + interval '1 minute'),
		(2, '00000000-0000-0000-0000-000000000001', clock_timestamp() + interval '1 minute')`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, parts[0])
	require.NoError(t, err)
	var rootCount, rootIDs, legacyCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT branches->0->>'branch_id')
		FROM disks WHERE branches->0->>'name' = 'main' AND storage_dir = ''`).Scan(&rootCount, &rootIDs))
	require.Equal(t, 2, rootCount)
	require.Equal(t, 2, rootIDs)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions
		WHERE branch_name = 'main' AND NOT read_only AND commit_id IS NULL AND NOT lease_managed`).Scan(&legacyCount))
	require.Equal(t, 2, legacyCount)
	_, err = pool.Exec(ctx, parts[1])
	require.NoError(t, err)
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions`).Scan(&legacyCount))
	require.Equal(t, 2, legacyCount)
	_, err = pool.Exec(ctx, parts[0])
	require.NoError(t, err)
}

func diskRootBranch(t *testing.T, branches []map[string]any) []byte {
	t.Helper()
	root, err := json.Marshal(branches[:1])
	require.NoError(t, err)
	return root
}

func controlPlaneUUID(t *testing.T, value string) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	require.NoError(t, id.Scan(value))
	return id
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
	for _, c := range []db.CreateCommitParams{
		{CommitID: base, DiskID: diskID, BranchName: "main", Message: "base", CheckpointDir: "/base"},
		{CommitID: left, DiskID: diskID, BranchName: "main", ParentCommitID: base, Message: "left", CheckpointDir: "/left"},
		{CommitID: right, DiskID: diskID, BranchName: "feature", ParentCommitID: base, Message: "right", CheckpointDir: "/right"},
		{CommitID: merge, DiskID: diskID, BranchName: "main", ParentCommitID: left, MergeParentCommitID: right, Message: "merge", CheckpointDir: "/merge"},
	} {
		created, err := q.CreateCommit(ctx, c)
		require.NoError(t, err)
		require.True(t, created.CreatedAt.Valid)
	}

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
	var ids []pgtype.UUID
	for _, ancestor := range ancestors {
		ids = append(ids, ancestor.CommitID)
	}
	require.ElementsMatch(t, []pgtype.UUID{base, left, right, merge}, ids)
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

	start := make(chan struct{})
	errs := make(chan error, 8)
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			<-start
			_, err := q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
				SessionID: id, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Minute / time.Microsecond),
			})
			errs <- err
		}(int64(i + 1))
	}
	close(start)
	wg.Wait()
	close(errs)
	winners := 0
	for err := range errs {
		if err == nil {
			winners++
			continue
		}
		var pgErr *pgconn.PgError
		require.ErrorAs(t, err, &pgErr)
		require.Equal(t, "23505", pgErr.Code)
		require.Equal(t, "uq_client_sessions_managed_writer", pgErr.ConstraintName)
	}
	require.Equal(t, 1, winners)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 100, DiskID: diskID, BranchName: "feature", LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)
	for _, id := range []int64{101, 102} {
		_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: id, DiskID: diskID, BranchName: "main", ReadOnly: true, CommitID: commitID,
			LeaseMicros: int64(time.Minute / time.Microsecond),
		})
		require.NoError(t, err)
	}
	readers, err := q.ListActiveCommitSessions(ctx, db.ListActiveCommitSessionsParams{DiskID: diskID, CommitID: commitID})
	require.NoError(t, err)
	require.Len(t, readers, 2)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 200, DiskID: diskID, BranchName: "short", LeaseMicros: int64(30 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	time.Sleep(60 * time.Millisecond)
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
	legacyArgs := db.UpsertClientSessionParams{
		SessionID: 201, DiskID: diskID, ExpiresAt: pgtype.Timestamptz{Time: time.Now().Add(time.Minute), Valid: true},
	}
	_, err = q.UpsertClientSession(ctx, legacyArgs)
	require.ErrorIs(t, err, pgx.ErrNoRows)
	_, err = db.NewStore(pool).UpsertClientSession(ctx, legacyArgs)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 300, DiskID: diskID, BranchName: "reader", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(30 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	time.Sleep(60 * time.Millisecond)
	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 300, DiskID: diskID, BranchName: "reader", ReadOnly: true, CommitID: commitID,
		LeaseMicros: int64(time.Minute / time.Microsecond),
	})
	require.NoError(t, err)

	_, err = q.RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 400, DiskID: diskID, BranchName: "same-writer", LeaseMicros: int64(30 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	time.Sleep(60 * time.Millisecond)
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
	defer tx.Rollback(context.Background())
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
	defer tx.Rollback(context.Background())
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
		session, err := db.New(pool).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: 2, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Second / time.Microsecond),
		})
		results <- result{session: session, err: err}
	}()
	select {
	case r := <-results:
		t.Fatalf("冲突事务尚未结束，写端不应获得租约：%v", r.err)
	case <-time.After(1200 * time.Millisecond):
	}
	require.NoError(t, tx.Rollback(ctx))
	r := <-results
	require.NoError(t, r.err)
	require.Greater(t, time.Until(r.session.ExpiresAt.Time), 700*time.Millisecond)
}

func TestControlPlaneLeaseTakesOverWriterExpiredWhileWaiting(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "control_plane_expired_wait")
	diskID := controlPlaneUUID(t, "00000000-0000-0000-0000-000000000001")
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 100, 1024)`, diskID)
	require.NoError(t, err)
	tx, err := pool.Begin(ctx)
	require.NoError(t, err)
	defer tx.Rollback(context.Background())
	_, err = db.New(tx).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
		SessionID: 1, DiskID: diskID, BranchName: "main", LeaseMicros: int64(100 * time.Millisecond / time.Microsecond),
	})
	require.NoError(t, err)
	errs := make(chan error, 1)
	go func() {
		_, err := db.New(pool).RenewControlPlaneSession(ctx, db.RenewControlPlaneSessionParams{
			SessionID: 2, DiskID: diskID, BranchName: "main", LeaseMicros: int64(time.Minute / time.Microsecond),
		})
		errs <- err
	}()
	select {
	case err := <-errs:
		t.Fatalf("冲突事务尚未结束，写端不应获得租约：%v", err)
	case <-time.After(200 * time.Millisecond):
	}
	require.NoError(t, tx.Commit(ctx))
	require.NoError(t, <-errs)
	writer, err := db.New(pool).GetActiveBranchWriter(ctx, db.GetActiveBranchWriterParams{DiskID: diskID, BranchName: "main"})
	require.NoError(t, err)
	require.EqualValues(t, 2, writer.SessionID)
}
