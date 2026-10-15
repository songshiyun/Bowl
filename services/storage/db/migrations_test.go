package db_test

import (
	"context"
	"database/sql"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"
)

func getTestDSN() string {
	if dsn := os.Getenv("STORAGE_DB_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://postgres:postgres@localhost:5432/bowl?sslmode=disable"
}

func getMigrationsDir(t *testing.T) string {
	wd, err := os.Getwd()
	require.NoError(t, err)

	candidates := []string{
		filepath.Join(wd, "migrations"),
		filepath.Join(wd, "db", "migrations"),
	}
	for _, c := range candidates {
		if info, err := os.Stat(c); err == nil && info.IsDir() {
			return c
		}
	}
	return filepath.Join(wd, "migrations")
}

const (
	modeDir  = 0o040755 // POSIX 目录类型 (S_IFDIR) 配合 0755 权限
	modeFile = 0o100644 // POSIX 常规文件类型 (S_IFREG) 配合 0644 权限
)

func TestStorageMetadataMigrations(t *testing.T) {
	if _, err := exec.LookPath("goose"); err != nil {
		t.Skipf("未在系统 PATH 中找到 goose 命令，跳过迁移执行测试: %v", err)
		return
	}

	dsn := getTestDSN()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		t.Skipf("PostgreSQL 不可用，跳过集成迁移测试: %v", err)
		return
	}

	migrationsDir := getMigrationsDir(t)
	require.DirExists(t, migrationsDir, "迁移目录必须存在")

	// 1. 执行 goose up
	cmdUp := exec.CommandContext(ctx, "goose", "-dir", migrationsDir, "postgres", dsn, "up")
	outputUp, err := cmdUp.CombinedOutput()
	require.NoError(t, err, "goose up 执行失败: %s", string(outputUp))

	// 2. 验证序列定义与初始值 (START WITH 2)
	var seqStartValue int64
	err = db.QueryRowContext(ctx, `
		SELECT start_value 
		FROM pg_sequences 
		WHERE sequencename = 'inode_id_seq'
	`).Scan(&seqStartValue)
	require.NoError(t, err, "查询 inode_id_seq 序列元数据失败")
	require.Equal(t, int64(2), seqStartValue, "inode_id_seq 起始值必须为 2")

	// 验证从序列获取下一个值
	var nextVal int64
	err = db.QueryRowContext(ctx, `SELECT nextval('inode_id_seq')`).Scan(&nextVal)
	require.NoError(t, err)
	require.GreaterOrEqual(t, nextVal, int64(2), "序列生成的 Inode 编号必须 >= 2")

	// 3. 验证 inodes 表结构
	var hasBlksize, hasBlocks bool
	err = db.QueryRowContext(ctx, `
		SELECT 
			EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'inodes' AND column_name = 'blksize'),
			EXISTS(SELECT 1 FROM information_schema.columns WHERE table_schema = current_schema() AND table_name = 'inodes' AND column_name = 'blocks')
	`).Scan(&hasBlksize, &hasBlocks)
	require.NoError(t, err)
	require.False(t, hasBlksize, "inodes 表不应包含 blksize 字段")
	require.False(t, hasBlocks, "inodes 表不应包含 blocks 字段")

	// 验证 inodes 必须字段存在
	var columnCount int
	err = db.QueryRowContext(ctx, `
		SELECT count(*) 
		FROM information_schema.columns 
		WHERE table_schema = current_schema()
		  AND table_name = 'inodes' 
		  AND column_name IN ('ino', 'generation', 'mode', 'size', 'nlink', 'uid', 'gid', 'rdev', 'atime', 'mtime', 'ctime')
	`).Scan(&columnCount)
	require.NoError(t, err)
	require.Equal(t, 11, columnCount, "inodes 表必须包含 11 个核心 POSIX 属性字段")

	// 4. 验证 entries 复合主键与外键约束
	var pkColumns []string
	rows, err := db.QueryContext(ctx, `
		SELECT kcu.column_name
		FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu
		  ON tc.constraint_name = kcu.constraint_name
		WHERE tc.table_schema = current_schema()
		  AND tc.table_name = 'entries' 
		  AND tc.constraint_type = 'PRIMARY KEY'
		ORDER BY kcu.ordinal_position
	`)
	require.NoError(t, err)
	defer rows.Close()

	for rows.Next() {
		var col string
		require.NoError(t, rows.Scan(&col))
		pkColumns = append(pkColumns, col)
	}
	require.Equal(t, []string{"parent_ino", "name"}, pkColumns, "entries 表主键必须为 (parent_ino, name)")

	// 验证辅助索引 idx_entries_ino 存在
	var hasIdxEntriesIno bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_indexes 
			WHERE tablename = 'entries' AND indexname = 'idx_entries_ino'
		)
	`).Scan(&hasIdxEntriesIno)
	require.NoError(t, err)
	require.True(t, hasIdxEntriesIno, "idx_entries_ino 辅助索引必须存在")

	// 5. 验证外键级联与限制行为 (CASCADE / RESTRICT)
	// 5.1 插入根节点 (ino = 1) 与子节点 (ino = 100)
	_, err = db.ExecContext(ctx, `
		INSERT INTO inodes (ino, generation, mode, size, nlink, uid, gid, rdev)
		VALUES (1, 0, $1, 4096, 2, 0, 0, 0),
		       (100, 0, $2, 1024, 1, 0, 0, 0)
		ON CONFLICT (ino) DO NOTHING
	`, modeDir, modeFile)
	require.NoError(t, err)

	// 5.2 插入目录项 entry
	_, err = db.ExecContext(ctx, `
		INSERT INTO entries (parent_ino, name, ino)
		VALUES (1, 'hello.txt', 100)
	`)
	require.NoError(t, err)

	// 5.3 尝试删除受保护的目标子节点 ino=100，应触发 ON DELETE RESTRICT 报错
	_, err = db.ExecContext(ctx, `DELETE FROM inodes WHERE ino = 100`)
	require.Error(t, err, "存在 entry 引用时删除目标 inode 必须被 RESTRICT 阻止")

	// 5.4 删除父节点 ino=1，应触发 ON DELETE CASCADE 级联删除 entry
	_, err = db.ExecContext(ctx, `DELETE FROM inodes WHERE ino = 1`)
	require.NoError(t, err, "删除父节点应级联成功")

	var entryExists bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (SELECT 1 FROM entries WHERE parent_ino = 1 AND name = 'hello.txt')
	`).Scan(&entryExists)
	require.NoError(t, err)
	require.False(t, entryExists, "父节点删除后对应 entry 应被级联删除")

	// 此时目标子节点已无引用，可正常删除
	_, err = db.ExecContext(ctx, `DELETE FROM inodes WHERE ino = 100`)
	require.NoError(t, err)

	// 6. 执行 goose down 回滚
	cmdDown := exec.CommandContext(ctx, "goose", "-dir", migrationsDir, "postgres", dsn, "down")
	outputDown, err := cmdDown.CombinedOutput()
	require.NoError(t, err, "goose down 回滚执行失败: %s", string(outputDown))

	// 验证表和序列已被清除
	var tablesExist bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM information_schema.tables 
			WHERE table_schema = current_schema() AND table_name IN ('inodes', 'entries')
		)
	`).Scan(&tablesExist)
	require.NoError(t, err)
	require.False(t, tablesExist, "回滚后 inodes 与 entries 表不应存在")

	var seqExists bool
	err = db.QueryRowContext(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_sequences WHERE schemaname = current_schema() AND sequencename = 'inode_id_seq'
		)
	`).Scan(&seqExists)
	require.NoError(t, err)
	require.False(t, seqExists, "回滚后 inode_id_seq 序列不应存在")

	// 7. 重新执行 goose up，保证测试结束后环境保持最新迁移就绪状态
	cmdRestore := exec.CommandContext(ctx, "goose", "-dir", migrationsDir, "postgres", dsn, "up")
	outputRestore, err := cmdRestore.CombinedOutput()
	require.NoError(t, err, "恢复 goose up 执行失败: %s", string(outputRestore))
}
