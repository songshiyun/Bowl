package db_test

import (
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

func mustUUID(t *testing.T, raw string) pgtype.UUID {
	t.Helper()
	var id pgtype.UUID
	require.NoError(t, id.Scan(raw))
	return id
}

func TestSingleDiskControlPlaneMigrationUpDownRoundtrip(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "db_migration_roundtrip")
	sqlDB := stdlib.OpenDBFromPool(pool)
	t.Cleanup(func() { _ = sqlDB.Close() })

	version, err := goose.GetDBVersionContext(ctx, sqlDB)
	require.NoError(t, err)
	require.Equal(t, int64(20261108000000), version)

	// 1. 验证 +goose Up 后仅保留 4 列 disks 表，已删除 commits / client_sessions 表及续期函数
	var droppedTables int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('commits', 'client_sessions')`).Scan(&droppedTables))
	require.Zero(t, droppedTables)

	var droppedFuncs int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.routines
		WHERE routine_schema = 'public' AND routine_name = 'renew_control_plane_session'`).Scan(&droppedFuncs))
	require.Zero(t, droppedFuncs)

	rows, err := pool.Query(ctx, `SELECT column_name FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = 'disks' ORDER BY ordinal_position`)
	require.NoError(t, err)
	var cols []string
	for rows.Next() {
		var col string
		require.NoError(t, rows.Scan(&col))
		cols = append(cols, col)
	}
	require.NoError(t, rows.Err())
	rows.Close()
	require.Equal(t, []string{"disk_id", "name", "created_at", "updated_at"}, cols)

	var idxCount int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_indexes
		WHERE schemaname = 'public' AND tablename = 'disks' AND indexname = 'idx_disks_created_at_id'`).Scan(&idxCount))
	require.Equal(t, 1, idxCount)

	// 插入一条磁盘记录，验证回滚与重升时存量行兼容
	diskID := mustUUID(t, "018f0000-0000-7000-8000-000000000001")
	store := db.NewStore(pool)
	created, err := store.CreateDisk(ctx, db.CreateDiskParams{
		DiskID: diskID,
		Name:   "alpha",
	})
	require.NoError(t, err)
	require.Equal(t, diskID, created.DiskID)
	require.Equal(t, "alpha", created.Name)

	// 2. 验证 +goose Down 完整还原被删列、表与函数
	require.NoError(t, goose.DownToContext(ctx, sqlDB, ".", 20261104010000))
	version, err = goose.GetDBVersionContext(ctx, sqlDB)
	require.NoError(t, err)
	require.Equal(t, int64(20261104010000), version)

	var restoredTables int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM information_schema.tables
		WHERE table_schema = 'public' AND table_name IN ('commits', 'client_sessions')`).Scan(&restoredTables))
	require.Equal(t, 2, restoredTables)

	var renewedCount int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM renew_control_plane_session(1, $1, 'main', false, NULL::uuid, 60000000)`,
		diskID,
	).Scan(&renewedCount))
	require.Equal(t, 1, renewedCount)

	// 3. 再次 +goose Up 回到极简 4 列结构
	require.NoError(t, goose.UpContext(ctx, sqlDB, "."))
	version, err = goose.GetDBVersionContext(ctx, sqlDB)
	require.NoError(t, err)
	require.Equal(t, int64(20261108000000), version)

	got, err := store.GetDisk(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, "alpha", got.Name)
}

func TestStoreDiskCRUDAndPagination(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "db_disk_crud")
	store := db.NewStore(pool)

	id1, err := uuid.NewV7()
	require.NoError(t, err)
	id2, err := uuid.NewV7()
	require.NoError(t, err)
	pgID1 := pgtype.UUID{Bytes: id1, Valid: true}
	pgID2 := pgtype.UUID{Bytes: id2, Valid: true}

	d1, err := store.CreateDisk(ctx, db.CreateDiskParams{DiskID: pgID1, Name: "disk-1"})
	require.NoError(t, err)
	require.Equal(t, pgID1, d1.DiskID)
	require.Equal(t, "disk-1", d1.Name)
	require.True(t, d1.CreatedAt.Valid)
	require.True(t, d1.UpdatedAt.Valid)

	d2, err := store.CreateDisk(ctx, db.CreateDiskParams{DiskID: pgID2, Name: "disk-2"})
	require.NoError(t, err)
	require.Equal(t, pgID2, d2.DiskID)

	// GetDisk & GetDiskForUpdate & UpdateDisk in transaction
	err = store.ExecTx(ctx, func(q db.Querier) error {
		locked, lockErr := q.GetDiskForUpdate(ctx, pgID1)
		if lockErr != nil {
			return lockErr
		}
		require.Equal(t, "disk-1", locked.Name)

		updated, upErr := q.UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID: pgID1,
			Name:   "disk-1-renamed",
		})
		if upErr != nil {
			return upErr
		}
		require.Equal(t, "disk-1-renamed", updated.Name)
		require.False(t, updated.UpdatedAt.Time.Before(locked.UpdatedAt.Time))
		return nil
	})
	require.NoError(t, err)

	got1, err := store.GetDisk(ctx, pgID1)
	require.NoError(t, err)
	require.Equal(t, "disk-1-renamed", got1.Name)

	// ListDisks cursor pagination
	page1, err := store.ListDisks(ctx, db.ListDisksParams{
		HasCursor: false,
		PageLimit: 1,
	})
	require.NoError(t, err)
	require.Len(t, page1, 1)
	require.Equal(t, pgID2, page1[0].DiskID)

	page2, err := store.ListDisks(ctx, db.ListDisksParams{
		HasCursor:       true,
		CursorCreatedAt: page1[0].CreatedAt,
		CursorDiskID:    page1[0].DiskID,
		PageLimit:       2,
	})
	require.NoError(t, err)
	require.Len(t, page2, 1)
	require.Equal(t, pgID1, page2[0].DiskID)

	// DeleteDisk
	affected, err := store.DeleteDisk(ctx, pgID1)
	require.NoError(t, err)
	require.EqualValues(t, 1, affected)

	affectedAgain, err := store.DeleteDisk(ctx, pgID1)
	require.NoError(t, err)
	require.Zero(t, affectedAgain)

	_, err = store.GetDisk(ctx, pgID1)
	require.ErrorIs(t, err, pgx.ErrNoRows)
}
