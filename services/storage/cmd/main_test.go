package main

import (
	"cmp"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

const defaultCmdTestDSN = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"

func freePort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := tcpAddr.Port
	require.NoError(t, ln.Close())
	return port
}

func setupCmdTestDB(t *testing.T) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)

	baseDSN := cmp.Or(
		os.Getenv("STORAGE_TEST_DSN"),
		os.Getenv("STORAGE_DB_DSN"),
		os.Getenv("DATABASE_URL"),
		os.Getenv("BOWL_TEST_POSTGRES_DSN"),
		os.Getenv("TEST_DB_DSN"),
		defaultCmdTestDSN,
	)
	adminPool, err := db.OpenPool(config.DBPoolConfig{DSN: baseDSN})
	require.NoError(t, err)
	require.NoError(t, adminPool.Ping(ctx))
	t.Cleanup(adminPool.Close)

	dbName := fmt.Sprintf("cmd_run_%d", time.Now().UnixNano())
	_, err = adminPool.Exec(ctx, fmt.Sprintf("CREATE DATABASE %s;", dbName))
	require.NoError(t, err)
	t.Cleanup(func() {
		dropCtx, dropCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer dropCancel()
		_, dropErr := adminPool.Exec(dropCtx, fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName))
		require.NoError(t, dropErr)
	})

	connCfg := adminPool.Config().ConnConfig
	isoDSN := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s?sslmode=disable",
		connCfg.User,
		connCfg.Password,
		connCfg.Host,
		connCfg.Port,
		dbName,
	)
	isoPool, err := db.OpenPool(config.DBPoolConfig{DSN: isoDSN})
	require.NoError(t, err)
	defer isoPool.Close()

	goose.SetBaseFS(os.DirFS("../db/migrations"))
	require.NoError(t, goose.SetDialect("postgres"))
	sqlDB := stdlib.OpenDBFromPool(isoPool)
	defer func() { _ = sqlDB.Close() }()
	require.NoError(t, goose.UpContext(ctx, sqlDB, "."))

	return isoDSN
}

func TestRunDualPortLifecycle(t *testing.T) {
	dsn := setupCmdTestDB(t)
	pubPort := freePort(t)
	admPort := freePort(t)
	if admPort == pubPort {
		admPort = freePort(t)
	}

	t.Setenv("DB_DSN", dsn)
	t.Setenv("SERVER_HOST", "127.0.0.1")
	t.Setenv("SERVER_PORT", strconv.Itoa(pubPort))
	t.Setenv("INTERNAL_SERVER_HOST", "127.0.0.1")
	t.Setenv("INTERNAL_SERVER_PORT", strconv.Itoa(admPort))
	t.Setenv("STORAGE_DATA_DIR", t.TempDir())
	t.Setenv("OTEL_ENABLED", "false")
	t.Setenv("METRICS_ENABLED", "true")
	t.Setenv("PPROF_ENABLED", "true")

	runCtx, cancelRun := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(runCtx)
	}()

	httpClient := &http.Client{Timeout: 500 * time.Millisecond}
	adminReadyURL := fmt.Sprintf("http://127.0.0.1:%d/readyz", admPort)
	deadline := time.Now().Add(5 * time.Second)
	ready := false
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, adminReadyURL, http.NoBody)
		require.NoError(t, err)
		resp, err := httpClient.Do(req)
		if err == nil {
			_ = resp.Body.Close()
			if resp.StatusCode == http.StatusOK {
				ready = true
				break
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	require.True(t, ready, "等待 cmd.run 双端口管理面 /readyz 就绪超时")

	// 验证公网 RPC 可用且公网 /metrics 返回 404
	pubReq, err := http.NewRequestWithContext(context.Background(), http.MethodGet, fmt.Sprintf("http://127.0.0.1:%d/metrics", pubPort), http.NoBody)
	require.NoError(t, err)
	pubMetricsResp, err := httpClient.Do(pubReq)
	require.NoError(t, err)
	_ = pubMetricsResp.Body.Close()
	require.Equal(t, http.StatusNotFound, pubMetricsResp.StatusCode)

	rpcClient := storagev1connect.NewStorageServiceClient(httpClient, fmt.Sprintf("http://127.0.0.1:%d", pubPort))
	listResp, err := rpcClient.ListDisks(context.Background(), connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 10}))
	require.NoError(t, err)
	require.Empty(t, listResp.Msg.GetDisks())

	cancelRun()
	select {
	case runErr := <-errCh:
		require.NoError(t, runErr)
	case <-time.After(5 * time.Second):
		t.Fatal("等待 cmd.run 优雅停机超时")
	}
}
