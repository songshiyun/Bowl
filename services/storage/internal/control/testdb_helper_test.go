package control_test

import (
	"cmp"
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"
)

const defaultTestDSN = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"

// setupTestDB 建立当前测试用例专用的 PostgreSQL 管理连接池，并在测试结束时自动回收。
func setupTestDB(t *testing.T) (*pgxpool.Pool, context.Context) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	dsn := cmp.Or(
		os.Getenv("STORAGE_TEST_DSN"),
		os.Getenv("STORAGE_DB_DSN"),
		os.Getenv("DATABASE_URL"),
		os.Getenv("BOWL_TEST_POSTGRES_DSN"),
		os.Getenv("TEST_DB_DSN"),
		defaultTestDSN,
	)
	poolCfg, err := pgxpool.ParseConfig(dsn)
	require.NoError(t, err)
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	require.NoError(t, err)
	require.NoError(t, pool.Ping(ctx), "连接测试数据库失败: %s", dsn)
	t.Cleanup(pool.Close)
	return pool, ctx
}

// setupIsolatedTestDB 为测试用例创建独立的临时数据库，
// 通过 goose 应用全部迁移，并在测试结束时按 LIFO 顺序先关闭测试连接池、再强制销毁临时数据库。
func setupIsolatedTestDB(t *testing.T, prefix string) (*pgxpool.Pool, context.Context) {
	t.Helper()
	adminPool, ctx := setupTestDB(t)

	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	_, err := adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, dropErr := adminPool.Exec(dropCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		require.NoError(t, dropErr)
	})

	cfg := adminPool.Config().Copy()
	cfg.ConnConfig.Database = dbName
	isoPool, err := pgxpool.NewWithConfig(ctx, cfg)
	require.NoError(t, err)
	t.Cleanup(isoPool.Close)

	goose.SetBaseFS(os.DirFS("../../db/migrations"))
	require.NoError(t, goose.SetDialect("postgres"))
	sqlDB := stdlib.OpenDBFromPool(isoPool)
	t.Cleanup(func() { _ = sqlDB.Close() })
	require.NoError(t, goose.UpContext(ctx, sqlDB, "."))
	return isoPool, ctx
}
