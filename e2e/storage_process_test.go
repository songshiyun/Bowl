//go:build e2e

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

const (
	defaultStorageTestDSN   = "postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable"
	storageBuildTimeout     = 5 * time.Minute
	storageLifecycleTimeout = time.Minute
)

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
	port := listener.Addr().(*net.TCPAddr).Port
	require.NoError(t, listener.Close())
	return port
}

// storageProcessEnv 构造 storage-service 的完整测试环境变量。
func storageProcessEnv(publicPort, internalPort int, dataDir, dsn string) []string {
	return []string{
		"SERVER_HOST=127.0.0.1",
		"SERVER_PORT=" + strconv.Itoa(publicPort),
		"INTERNAL_SERVER_HOST=127.0.0.1",
		"INTERNAL_SERVER_PORT=" + strconv.Itoa(internalPort),
		"STORAGE_DATA_DIR=" + dataDir,
		"DB_DSN=" + dsn,
		"STORAGE_SESSION_SWEEP_INTERVAL=1m",
	}
}

// runStorageProcessExpectFailure 启动 storage-service 并断言其在超时内以非零退出码失败。
func runStorageProcessExpectFailure(t *testing.T, binary string, env []string) (int, string) {
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

func TestStorageProcessRejectsInvalidConfig(t *testing.T) {
	binary := buildStorageBinary(t)

	t.Run("missing DSN", func(t *testing.T) {
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(freeTCPPort(t), freeTCPPort(t), t.TempDir(), ""))
		require.NotZero(t, exitCode, "缺少 DB_DSN 必须非零退出: %s", output)
		require.Contains(t, output, "DB_DSN")
		recordArtifact(t, map[string]string{"missing_dsn_exit": strconv.Itoa(exitCode)})
	})

	t.Run("malformed DSN URL", func(t *testing.T) {
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(freeTCPPort(t), freeTCPPort(t), t.TempDir(), "://bad"))
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
		port := listener.Addr().(*net.TCPAddr).Port
		exitCode, output := runStorageProcessExpectFailure(t, binary, storageProcessEnv(port, freeTCPPort(t), t.TempDir(), storageTestDSN()))
		require.NotZero(t, exitCode, "端口冲突必须非零退出: %s", output)
		require.Contains(t, output, "address already in use")
		recordArtifact(t, map[string]string{"port_conflict_exit": strconv.Itoa(exitCode)})
	})
}

func TestStorageProcessGracefulSIGTERM(t *testing.T) {
	binary := buildStorageBinary(t)
	port := freeTCPPort(t)
	ctx, cancel := context.WithTimeout(context.Background(), storageLifecycleTimeout)
	t.Cleanup(cancel)
	cmd := exec.CommandContext(ctx, binary)
	cmd.Env = append(append(os.Environ(), "OTEL_ENABLED=false"), storageProcessEnv(port, freeTCPPort(t), t.TempDir(), storageTestDSN())...)
	var output bytes.Buffer
	cmd.Stdout = &output
	cmd.Stderr = &output
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	})

	address := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
	require.Eventually(t, func() bool {
		conn, err := net.DialTimeout("tcp", address, time.Second)
		return err == nil && conn.Close() == nil
	}, 30*time.Second, 20*time.Millisecond, "storage-service 未在超时内就绪")

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, cmd.Wait(), "SIGTERM 必须触发 0 退出: %s", output.String())
	recordArtifact(t, map[string]string{
		"sigterm_exit": strconv.Itoa(cmd.ProcessState.ExitCode()),
	})
}
