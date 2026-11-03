package db_test

import (
	"context"
	"net/url"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestContractMigration 验证真实 Goose 迁移、控制面记录保留及结构回滚。
func TestContractMigration(t *testing.T) {
	pool, dbName := createUniqueTempDB(t, "bowl_contract_migration")
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dsn := pool.Config().ConnString()
	if strings.Contains(dsn, "://") {
		u, err := url.Parse(dsn)
		require.NoError(t, err)
		u.Path = "/" + dbName
		dsn = u.String()
	} else {
		dsn += " dbname=" + dbName
	}
	runGoose := func(args ...string) {
		t.Helper()
		argv := append([]string{"-dir", "../../db/migrations", "postgres", dsn}, args...)
		out, err := exec.CommandContext(ctx, "goose", argv...).CombinedOutput()
		require.NoError(t, err, "迁移执行失败: %s", out)
	}
	runGoose("up-to", "20261104000000")
	const diskID = "11111111-1111-7111-8111-111111111111"
	const commitID = "22222222-2222-7222-8222-222222222222"
	_, err := pool.Exec(ctx, `INSERT INTO disks (disk_id, root_ino, max_bytes) VALUES ($1, 42, 4096)`, diskID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO commits (commit_id, disk_id, branch_name, checkpoint_dir) VALUES ($1, $2, 'main', '/checkpoint')`, commitID, diskID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, disk_id, expires_at, lease_managed)
		VALUES (1, $1, clock_timestamp() + interval '1 hour', true), (2, $1, clock_timestamp() + interval '1 hour', false)`, diskID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE TABLE _rollback_entries_name_backup (id int);
		CREATE TABLE _rollback_symlink_target_backup (id int);
		CREATE FUNCTION alloc_ino(UUID) RETURNS bigint LANGUAGE sql AS 'SELECT 1::bigint'`)
	require.NoError(t, err)
	runGoose("up")

	assertControlTables := func() {
		t.Helper()
		rows, err := pool.Query(ctx, `SELECT tablename FROM pg_tables WHERE schemaname = 'public' ORDER BY tablename`)
		require.NoError(t, err)
		var tables []string
		for rows.Next() {
			var name string
			require.NoError(t, rows.Scan(&name))
			tables = append(tables, name)
		}
		require.NoError(t, rows.Err())
		rows.Close()
		expected := []string{"client_sessions", "commits", "disks", "goose_db_version"}
		sort.Strings(expected)
		require.Equal(t, expected, tables)
		var count int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_sequences WHERE schemaname = 'public' AND sequencename IN ('inode_id_seq', 'file_handle_id_seq', 'slice_id_seq', 'file_locks_id_seq')`).Scan(&count))
		require.Zero(t, count)
		var functionExists bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT to_regprocedure('public.alloc_ino(uuid)') IS NOT NULL`).Scan(&functionExists))
		require.False(t, functionExists)
	}
	assertControlTables()
	var disks, commits, managed, legacy int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM disks WHERE disk_id = $1`, diskID).Scan(&disks))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM commits WHERE commit_id = $1`, commitID).Scan(&commits))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id = 1`).Scan(&managed))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id = 2`).Scan(&legacy))
	var root uint64
	require.NoError(t, pool.QueryRow(ctx, `SELECT root_ino FROM disks WHERE disk_id = $1`, diskID).Scan(&root))
	require.Equal(t, uint64(1), root)
	require.Equal(t, 1, disks)
	require.Equal(t, 1, commits)
	require.Equal(t, 1, managed)
	require.Zero(t, legacy)

	// 新磁盘均使用根节点 1，回滚仍须重建唯一约束和完整旧表结构。
	_, err = pool.Exec(ctx, `INSERT INTO disks (disk_id, max_bytes) VALUES
		('33333333-3333-7333-8333-333333333333', 4096),
		('44444444-4444-7444-8444-444444444444', 4096)`)
	require.NoError(t, err)
	runGoose("down")
	var roots int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM inodes WHERE ino IN (SELECT root_ino FROM disks)`).Scan(&roots))
	require.Equal(t, 3, roots)
	for _, name := range []string{"inodes", "entries", "inode_xattrs", "chunks", "slice_refs", "file_handles", "file_locks", "file_lock_waiters"} {
		var exists bool
		require.NoError(t, pool.QueryRow(ctx, `SELECT to_regclass($1) IS NOT NULL`, "public."+name).Scan(&exists))
		require.True(t, exists, "%s 未恢复", name)
	}
	runGoose("up")
	assertControlTables()
}
