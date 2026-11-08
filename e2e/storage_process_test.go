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

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
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
	tcpAddr, ok := listener.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := tcpAddr.Port
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
	}
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
		tcpAddr, ok := listener.Addr().(*net.TCPAddr)
		require.True(t, ok)
		port := tcpAddr.Port
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

// TestSingleDiskStorageLifecycle 从公开 RPC 验证单盘 MountDisk 世代号抢占、数据读写与孤儿节点回收闭环。
func TestSingleDiskStorageLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "single-disk-lifecycle",
		MaxBytes: 8 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := created.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	require.Equal(t, uint64(1), rootIno)
	diskID := disk.GetDiskId()
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, deleteErr := client.DeleteDisk(cleanup, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		require.NoError(t, deleteErr)
	})

	// 1. 首次可写挂载分配 Epoch 1，只读挂载返回 Epoch 0 且不推进世代号。
	mount1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch1)

	mountRO, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, mountRO.Msg.GetEpoch())

	// 2. 使用 Epoch 1 写入文件与扩展属性，并校验磁盘实时用量。
	file, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("shared"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	ino := file.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    ino,
		Data:   []byte("hello"),
	}))
	require.NoError(t, err)
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    ino,
		Name:   []byte("user.test"),
		Value:  []byte("value"),
	}))
	require.NoError(t, err)
	xattr, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		DiskId: diskID,
		Ino:    ino,
		Name:   []byte("user.test"),
	}))
	require.NoError(t, err)
	xattrValue := string(xattr.Msg.GetValue())
	require.Equal(t, "value", xattrValue)

	gotDisk, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	usedBytes := gotDisk.Msg.GetDisk().GetUsedBytes()
	require.Equal(t, uint64(5), usedBytes)

	// 3. 验证打开状态下解链（Orphan Inode）仍可读取，调用 ReclaimOrphan 后返回 ENOENT。
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:       diskID,
		Epoch:        epoch1,
		ParentIno:    1,
		Name:         []byte("shared"),
		OpenInClient: new(true),
	}))
	require.NoError(t, err)
	read, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    ino,
		Size:   5,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), read.Msg.GetData())

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    ino,
	}))
	require.NoError(t, err)
	_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    ino,
		Size:   5,
	}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	orphanAfterReclaimReason := testutil.ExtractErrorInfo(err).GetReason()

	// 4. 新可写挂载递增 Epoch = 2 零等待抢占写权限，旧 Epoch = 1 写入立即被拦截返回 EBUSY。
	mount2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch2 := mount2.Msg.GetEpoch()
	require.Equal(t, uint64(2), epoch2)

	_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("stale"),
		Mode:      0o644,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")
	staleEpochReason := testutil.ExtractErrorInfo(err).GetReason()

	// 5. 单次 4 MiB Write/Read RPC 往返：覆盖服务端最大块写入路径。
	bigPayload := bytes.Repeat([]byte{0x4d}, 4*1024*1024)
	bigFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch2,
		ParentIno: 1,
		Name:      []byte("big.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	bigIno := bigFile.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch2,
		Ino:    bigIno,
		Data:   bigPayload,
	}))
	require.NoError(t, err)
	bigRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    bigIno,
		Size:   uint32(len(bigPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, bigPayload, bigRead.Msg.GetData(), "4 MiB 单次 Write/Read 往返必须逐字节一致")

	recordArtifact(t, map[string]string{
		"epoch_1":              strconv.FormatUint(epoch1, 10),
		"epoch_2":              strconv.FormatUint(epoch2, 10),
		"stale_epoch_write":    staleEpochReason,
		"root_ino":             strconv.FormatUint(rootIno, 10),
		"used_bytes":           strconv.FormatUint(usedBytes, 10),
		"xattr_value":          xattrValue,
		"orphan_after_reclaim": orphanAfterReclaimReason,
		"big_write_bytes":      strconv.Itoa(len(bigPayload)),
		"big_write_sha256":     fileSHA256(bigPayload),
	})
}
