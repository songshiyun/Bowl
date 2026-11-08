package server_test

import (
	"cmp"
	"context"
	"fmt"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

const defaultTestDSN = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"

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

func newTestServerAndClient(t *testing.T, prefix string, opts ...control.Option) (storagev1connect.StorageServiceClient, control.Manager, context.Context) {
	t.Helper()
	pool, ctx := setupIsolatedTestDB(t, prefix)
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	ctrlOpts := []control.Option{
		control.WithStorageRoot(root),
		control.WithEntryTimeout(1.5),
		control.WithAttrTimeout(2.5),
		control.WithDirEntryTimeout(3.5),
		control.WithOpenKeepCache(true),
	}
	ctrlOpts = append(ctrlOpts, opts...)
	mgr := control.New(dbStore, ctrlOpts...)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	var srvCfg config.ServerConfig
	srvCfg.ApplyDefaultPort(0)
	srvCfg.ServerReadMaxBytes = config.DefaultServerReadMaxBytes

	srv, err := server.New(server.Params{
		Config:  srvCfg,
		Control: mgr,
	})
	require.NoError(t, err)

	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)

	client := storagev1connect.NewStorageServiceClient(httpSrv.Client(), httpSrv.URL)
	return client, mgr, ctx
}
