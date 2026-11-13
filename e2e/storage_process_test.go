//go:build e2e

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	defaultStorageTestDSN   = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"
	storageBuildTimeout     = 5 * time.Minute
	storageLifecycleTimeout = 90 * time.Second
)

// syncBuffer 提供并发安全的进程标准输出与错误日志采集缓冲。
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// storageTestDSN 返回 E2E 使用的 PostgreSQL DSN。
func storageTestDSN() string {
	return cmp.Or(os.Getenv("BOWL_TEST_POSTGRES_DSN"), defaultStorageTestDSN)
}

// buildStorageBinary 编译当前平台的 storage-service 二进制，供真实进程生命周期测试使用。
func buildStorageBinary(t *testing.T) string {
	t.Helper()
	binary := filepath.Join(t.TempDir(), "storage-service")
	ctx, cancel := context.WithTimeout(context.Background(), storageBuildTimeout)
	defer cancel()
	build := exec.CommandContext(ctx, "go", "-C", "../services/storage", "build", "-o", binary, "./cmd")
	build.Env = append(os.Environ(), "GOWORK=off")
	output, err := build.CombinedOutput()
	require.NoError(t, ctx.Err(), "编译 storage-service 超时: %s", output)
	require.NoError(t, err, "编译 storage-service 失败: %s", output)
	return binary
}

// freeTCPPort 申请一个当前可用的本机端口。
func freeTCPPort(t *testing.T) int {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := tcpAddr.Port
	require.NoError(t, listener.Close())
	return port
}

// freeTCPPortPair 同时锁定并返回两个互不冲突的本机空闲端口。
func freeTCPPortPair(t *testing.T) (pubPort, adminPort int) {
	t.Helper()
	pubListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	adminListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	pubAddr, pubOK := pubListener.Addr().(*net.TCPAddr)
	require.True(t, pubOK)
	adminAddr, adminOK := adminListener.Addr().(*net.TCPAddr)
	require.True(t, adminOK)
	pubPort, adminPort = pubAddr.Port, adminAddr.Port
	require.NoError(t, pubListener.Close())
	require.NoError(t, adminListener.Close())
	return pubPort, adminPort
}

// storageProcessEnv 构造 storage-service 的完整测试环境变量。
func storageProcessEnv(publicPort, internalPort int, dataDir, dsn string, extra ...string) []string {
	base := []string{
		"SERVER_HOST=127.0.0.1",
		"SERVER_PORT=" + strconv.Itoa(publicPort),
		"INTERNAL_SERVER_HOST=127.0.0.1",
		"INTERNAL_SERVER_PORT=" + strconv.Itoa(internalPort),
		"STORAGE_DATA_DIR=" + dataDir,
		"DB_DSN=" + dsn,
	}
	return append(base, extra...)
}

// runStorageProcessExpectFailure 启动 storage-service 并断言其在超时内以非零退出码失败。
func runStorageProcessExpectFailure(t *testing.T, binary string, env []string) (exitCode int, out string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(append(os.Environ(), "OTEL_ENABLED=false"), env...)
	output, err := cmd.CombinedOutput()
	require.NoError(t, ctx.Err(), "storage-service 未在超时内退出: %s", output)
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr, "storage-service 必须以非零退出码失败: %s", output)
	return exitErr.ExitCode(), string(output)
}

// startStorageDaemon 启动真实双端口 storage-service 子进程并等待管理面 /readyz 就绪。
func startStorageDaemon(t *testing.T, binary, dataDir string) (pubURL, adminURL string, logs *syncBuffer, stopAndWait func() int) {
	t.Helper()
	pubPort, adminPort := freeTCPPortPair(t)
	ctx, cancel := context.WithTimeout(context.Background(), storageLifecycleTimeout)
	t.Cleanup(cancel)

	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(
		append(os.Environ(), "OTEL_ENABLED=false"),
		storageProcessEnv(
			pubPort,
			adminPort,
			dataDir,
			storageTestDSN(),
			"METRICS_ENABLED=true",
			"PPROF_ENABLED=true",
			"LOG_FORMAT=json",
			"LOG_LEVEL=info",
			"LOG_CONNECT_MESSAGE=handled rpc",
		)...,
	)
	logs = &syncBuffer{}
	cmd.Stdout = logs
	cmd.Stderr = logs
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	pubURL = "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(pubPort))
	adminURL = "http://" + net.JoinHostPort("127.0.0.1", strconv.Itoa(adminPort))
	readyBody := waitHTTPStatus(t, adminURL+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`, "管理面 /readyz 必须报告 PostgreSQL 就绪")

	stopAndWait = func() int {
		require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
		require.NoError(t, cmd.Wait(), "SIGTERM 必须触发零退出码优雅停机: %s", logs.String())
		return cmd.ProcessState.ExitCode()
	}
	return pubURL, adminURL, logs, stopAndWait
}

// waitHTTPStatus 轮询等待目标 HTTP 端点返回预期状态码并返回响应体。
func waitHTTPStatus(t *testing.T, rawURL string, wantStatus int) string {
	t.Helper()
	httpClient := &http.Client{Timeout: time.Second}
	var bodyText string
	require.EventuallyWithT(t, func(c *assert.CollectT) {
		req, reqErr := http.NewRequestWithContext(context.Background(), http.MethodGet, rawURL, http.NoBody)
		require.NoError(c, reqErr)
		resp, doErr := httpClient.Do(req)
		require.NoError(c, doErr)
		defer func() { _ = resp.Body.Close() }()
		raw, readErr := io.ReadAll(resp.Body)
		require.NoError(c, readErr)
		require.Equal(c, wantStatus, resp.StatusCode)
		bodyText = string(raw)
	}, 30*time.Second, 25*time.Millisecond, "等待 %s 返回状态码 %d 超时", rawURL, wantStatus)
	return bodyText
}

// getHTTPStatusAndBody 执行单次同步 HTTP GET 并校验状态码。
func getHTTPStatusAndBody(t *testing.T, rawURL string, wantStatus int) (status int, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, reqErr := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	require.NoError(t, reqErr)
	resp, doErr := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	require.NoError(t, doErr)
	defer func() { _ = resp.Body.Close() }()
	raw, readErr := io.ReadAll(resp.Body)
	require.NoError(t, readErr)
	require.Equal(t, wantStatus, resp.StatusCode, "GET %s 状态码不匹配: %s", rawURL, string(raw))
	return resp.StatusCode, string(raw)
}

func TestStorageProcessRejectsInvalidConfig(t *testing.T) {
	binary := buildStorageBinary(t)

	t.Run("missing DSN", func(t *testing.T) {
		pubPort, adminPort := freeTCPPortPair(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(pubPort, adminPort, t.TempDir(), ""))
		require.NotZero(t, exitCode, "缺少 DB_DSN 必须非零退出: %s", output)
		require.Contains(t, output, "DB_DSN")
		recordArtifact(t, map[string]string{"missing_dsn_exit": strconv.Itoa(exitCode)})
	})

	t.Run("malformed DSN URL", func(t *testing.T) {
		pubPort, adminPort := freeTCPPortPair(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(pubPort, adminPort, t.TempDir(), "://bad"))
		require.NotZero(t, exitCode, "非法 DSN URL 必须非零退出: %s", output)
		require.Contains(t, output, "parse pool config", "非法 DSN URL 必须在连接池解析阶段失败: %s", output)
		recordArtifact(t, map[string]string{"malformed_dsn_exit": strconv.Itoa(exitCode)})
	})

	t.Run("invalid port", func(t *testing.T) {
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(70000, freeTCPPort(t), t.TempDir(), storageTestDSN()))
		require.NotZero(t, exitCode, "非法端口必须非零退出: %s", output)
		require.Contains(t, output, "invalid SERVER_PORT")
		recordArtifact(t, map[string]string{"invalid_port_exit": strconv.Itoa(exitCode)})
	})

	t.Run("port already in use", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		t.Cleanup(func() { _ = listener.Close() })
		tcpAddr, ok := listener.Addr().(*net.TCPAddr)
		require.True(t, ok)
		adminPort := freeTCPPort(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(tcpAddr.Port, adminPort, t.TempDir(), storageTestDSN()))
		require.NotZero(t, exitCode, "端口冲突必须非零退出: %s", output)
		require.Contains(t, output, "address already in use")
		recordArtifact(t, map[string]string{"port_conflict_exit": strconv.Itoa(exitCode)})
	})

	t.Run("dual port conflict", func(t *testing.T) {
		samePort := freeTCPPort(t)
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(samePort, samePort, t.TempDir(), storageTestDSN()))
		require.NotZero(t, exitCode, "公网端口与内网管理端口相同必须非零退出: %s", output)
		require.Contains(t, output, "conflict")
		recordArtifact(t, map[string]string{"dual_port_conflict_exit": strconv.Itoa(exitCode)})
	})

	recordArtifact(t, map[string]string{"invalid_config_cases": "5"})
}

// TestStorageProcessDualPortAdminAndRecovery 验证真实子进程双端口管理面隔离、
// X-Request-Id 传播、公开指标与高基数防线、SIGTERM 优雅重启持久化恢复及 DeleteDisk 物理目录回收。
func TestStorageProcessDualPortAdminAndRecovery(t *testing.T) {
	binary := buildStorageBinary(t)
	dataDir := t.TempDir()
	ctx, cancel := context.WithTimeout(context.Background(), storageLifecycleTimeout)
	defer cancel()

	// 1. 启动首个真实双端口进程，验证内网管理面公开端点与公网端口物理隔离。
	pubURL1, adminURL1, logs1, stop1 := startStorageDaemon(t, binary, dataDir)

	healthStatus, healthBody := getHTTPStatusAndBody(t, adminURL1+"/healthz", http.StatusOK)
	require.Contains(t, healthBody, `"status":"ok"`)
	readyStatus, readyBody := getHTTPStatusAndBody(t, adminURL1+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`)
	pprofStatus, _ := getHTTPStatusAndBody(t, adminURL1+"/debug/pprof/cmdline", http.StatusOK)

	pubHealth404, _ := getHTTPStatusAndBody(t, pubURL1+"/healthz", http.StatusNotFound)
	pubReady404, _ := getHTTPStatusAndBody(t, pubURL1+"/readyz", http.StatusNotFound)
	pubMetrics404, _ := getHTTPStatusAndBody(t, pubURL1+"/metrics", http.StatusNotFound)
	pubPprof404, _ := getHTTPStatusAndBody(t, pubURL1+"/debug/pprof/cmdline", http.StatusNotFound)
	require.Equal(t, http.StatusNotFound, pubHealth404)
	require.Equal(t, http.StatusNotFound, pubReady404)
	require.Equal(t, http.StatusNotFound, pubMetrics404)
	require.Equal(t, http.StatusNotFound, pubPprof404)

	// 2. 创建磁盘，验证 X-Request-Id 传播至结构化日志及 EBUSY 公开指标原因标签。
	client1 := testutil.NewStorageClientAt(pubURL1)
	created, err := client1.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "process-recovery-disk",
		MaxBytes: 4 * 1024 * 1024,
	}))
	require.NoError(t, err)
	diskID := created.Msg.GetDisk().GetDiskId()
	t.Cleanup(func() {
		cleanupCtx, stopCleanup := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCleanup()
		_, _ = testutil.NewStorageClient().DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})

	diskDir := filepath.Join(dataDir, "disks", diskID)
	dirInfo, statErr := os.Stat(diskDir)
	require.NoError(t, statErr)
	require.True(t, dirInfo.IsDir())

	const tracedReqID = "e2e-req-trace-42"
	tracedMount := connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	})
	tracedMount.Header().Set("X-Request-Id", tracedReqID)
	mount1, err := client1.MountDisk(ctx, tracedMount)
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch1)

	require.EventuallyWithT(t, func(c *assert.CollectT) {
		assert.Contains(c, logs1.String(), `"request_id":"`+tracedReqID+`"`)
	}, 5*time.Second, 15*time.Millisecond, "进程标准输出结构化日志必须携带入站 X-Request-Id")

	fileResp, err := client1.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("persisted.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	persistedPayload := []byte("persisted-across-sigterm")
	_, err = client1.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    fileIno,
		Data:   persistedPayload,
	}))
	require.NoError(t, err)

	// 推进世代号至 2，再用过期世代号 1 触发 EBUSY 错误原因指标。
	mount2, err := client1.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(2), mount2.Msg.GetEpoch())

	_, err = client1.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    fileIno,
		Data:   []byte("stale"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	metricsStatus, metricsBody := getHTTPStatusAndBody(t, adminURL1+"/metrics", http.StatusOK)
	require.Contains(t, metricsBody, "bowl_storage_disks_loaded")
	require.Contains(t, metricsBody, "bowl_storage_logical_bytes")
	require.Contains(t, metricsBody, "bowl_storage_inodes")
	require.Contains(t, metricsBody, "rpc_server_call_duration")
	require.Regexp(t, `(?m)^rpc_server_call_duration[^\n]*rpc_error_reason="EBUSY"`, metricsBody)
	require.NotContains(t, metricsBody, diskID, "高基数 disk_id 严禁泄露至公开 Prometheus 指标")
	require.NotContains(t, metricsBody, "disk_id=")
	require.NotContains(t, metricsBody, "bowl_disk_id=")
	require.NotContains(t, metricsBody, "bowl_ino=")

	exit1 := stop1()
	require.Zero(t, exit1)

	// 3. 在同一 STORAGE_DATA_DIR 重启新进程，验证懒加载持久化恢复与 DeleteDisk 彻底回收物理目录。
	pubURL2, _, _, stop2 := startStorageDaemon(t, binary, dataDir)
	client2 := testutil.NewStorageClientAt(pubURL2)

	gotAfterRestart, err := client2.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, uint64(len(persistedPayload)), gotAfterRestart.Msg.GetDisk().GetUsedBytes())

	readAfterRestart, err := client2.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Size:   uint32(len(persistedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, persistedPayload, readAfterRestart.Msg.GetData())

	mount3, err := client2.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	restartedEpoch := mount3.Msg.GetEpoch()
	require.Equal(t, uint64(3), restartedEpoch, "重启后 WriterEpoch 必须在持久化世代 2 基础上递增至 3")

	_, err = client2.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	_, deletedStatErr := os.Stat(diskDir)
	require.ErrorIs(t, deletedStatErr, os.ErrNotExist, "DeleteDisk 必须彻底删除本地磁盘物理目录")

	_, err = client2.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	exit2 := stop2()
	require.Zero(t, exit2)

	highCardinalityGuarded := !strings.Contains(metricsBody, diskID) && !strings.Contains(metricsBody, "disk_id=")
	recordArtifact(t, map[string]string{
		"admin_healthz_status":     strconv.Itoa(healthStatus),
		"admin_readyz_status":      strconv.Itoa(readyStatus),
		"admin_pprof_status":       strconv.Itoa(pprofStatus),
		"admin_metrics_status":     strconv.Itoa(metricsStatus),
		"public_admin_status":      strconv.Itoa(pubMetrics404),
		"observed_request_id":      tracedReqID,
		"ebusy_metric_labeled":     "true",
		"high_cardinality_guarded": strconv.FormatBool(highCardinalityGuarded),
		"sigterm_exit_1":           strconv.Itoa(exit1),
		"sigterm_exit_2":           strconv.Itoa(exit2),
		"restarted_epoch":          strconv.FormatUint(restartedEpoch, 10),
		"persisted_sha256":         fileSHA256(readAfterRestart.Msg.GetData()),
		"physical_dir_removed":     strconv.FormatBool(os.IsNotExist(deletedStatErr)),
	})
}
