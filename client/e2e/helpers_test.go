//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"cmp"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	// helperProcessTimeout 为一次性辅助进程（CLI、pgrep、以指定 uid 运行）设定统一上界。
	helperProcessTimeout = 30 * time.Second
	// mountProcessTimeout 为前台 mount 进程的完整生命周期设定上界，避免 SIGTERM 失效时永久阻塞。
	mountProcessTimeout = 2 * time.Minute
)

// getStorageEndpoint 获取测试环境的 StorageService 接入地址。
func getStorageEndpoint() string {
	return cmp.Or(os.Getenv("BOWL_STORAGE_ENDPOINT"), "http://storage-service:9000")
}

// getBowlBin 获取 bowl CLI 二进制文件路径。
func getBowlBin() string {
	return cmp.Or(os.Getenv("BOWL_BIN"), "/tmp/bowl")
}

// newTestStorageClient 创建直连 StorageService 的 RPC 客户端。
func newTestStorageClient(t *testing.T) storagev1connect.StorageServiceClient {
	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint: getStorageEndpoint(),
	})
	require.NoError(t, err, "连接 StorageService 失败")
	return client
}

// createTestDisk 创建独立测试磁盘。
func createTestDisk(t *testing.T, client storagev1connect.StorageServiceClient) *storagev1.Disk {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name: fmt.Sprintf("e2e-disk-%d", time.Now().UnixNano()), MaxBytes: 10 * 1024 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := resp.Msg.GetDisk()
	require.NotNil(t, disk)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
		require.NoError(t, err, "清理测试磁盘失败")
		_, err = client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: disk.GetDiskId()}))
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
	})
	return disk
}

// prepareDiskFiles 通过可写挂载准备初始文件并优雅卸载，供后续只读挂载读取。
func prepareDiskFiles(t *testing.T, diskID string, prepare func(string)) {
	t.Helper()
	mp := t.TempDir()
	cmd, _, stderr := startBowlMount(t, getStorageEndpoint(), diskID, mp)
	waitForMount(t, mp, "准备数据时挂载失败: %s", stderr.String())
	prepare(mp)
	stopBowlMountGracefully(t, cmd, mp)
}

// isMounted 通过读取 /proc/self/mountinfo 确认目标路径是否为已生效的挂载点。
func isMounted(mountpoint string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	target := filepath.Clean(mountpoint)
	return err == nil && strings.Contains(string(data), " "+target+" ")
}

// waitForMount 等待挂载点出现在 /proc/self/mountinfo 中。
func waitForMount(t *testing.T, mountpoint string, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, func() bool { return isMounted(mountpoint) }, 10*time.Second, 10*time.Millisecond, msgAndArgs...)
}

// waitForUnmount 等待挂载点从 /proc/self/mountinfo 中消失。
func waitForUnmount(t *testing.T, mountpoint string, msgAndArgs ...any) {
	t.Helper()
	require.Eventually(t, func() bool { return !isMounted(mountpoint) }, 5*time.Second, 10*time.Millisecond, msgAndArgs...)
}

// findBowlDaemonPID 通过 pgrep 查找与指定挂载点绑定的后台 bowl 守护进程 PID。
func findBowlDaemonPID(t *testing.T, mountpoint string) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "pgrep", "-f", regexp.QuoteMeta(mountpoint)).Output()
	require.NoError(t, err, "未找到挂载点 %s 的后台守护进程", mountpoint)
	fields := strings.Fields(string(out))
	require.NotEmpty(t, fields)
	pid, err := strconv.Atoi(fields[0])
	require.NoError(t, err)
	return pid
}

// waitForProcessExit 等待指定 PID 的进程退出或进入僵尸状态（容器内无 init 回收）。
func waitForProcessExit(t *testing.T, pid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		stat, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
		return err != nil || strings.Contains(string(stat), ") Z ")
	}, 5*time.Second, 10*time.Millisecond, "进程 PID %d 在超时内未退出", pid)
}

// runAs 以指定 uid/gid 运行可执行文件并返回退出码与输出。
func runAs(t *testing.T, uid, gid uint32, name string, args ...string) (exitCode int, stdoutStr, stderrStr string) {
	t.Helper()
	return runAsWithStdin(t, uid, gid, "", name, args...)
}

// runAsWithStdin 以指定 uid/gid 运行可执行文件并写入标准输入。
func runAsWithStdin(t *testing.T, uid, gid uint32, stdin, name string, args ...string) (exitCode int, stdoutStr, stderrStr string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: uid, Gid: gid}}
	cmd.Stdin = strings.NewReader(stdin)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()
	require.NoError(t, ctx.Err(), "以 uid=%d 运行 %s 超时: %v", uid, name, err)
	require.NotNil(t, cmd.ProcessState, "以 uid=%d 启动 %s 失败: %v", uid, name, err)
	return cmd.ProcessState.ExitCode(), stdout.String(), stderr.String()
}

// startBowlMount 启动 bowl mount -f 前台子进程。
func startBowlMount(t *testing.T, endpoint, diskID, mountpoint string, extraFlags ...string) (cmd *exec.Cmd, stdout, stderr *bytes.Buffer) {
	args := []string{"mount", "-f", "--endpoint", endpoint, diskID, mountpoint}
	args = append(args, extraFlags...)

	ctx, cancel := context.WithTimeout(context.Background(), mountProcessTimeout)
	t.Cleanup(cancel)
	cmd = exec.CommandContext(ctx, getBowlBin(), args...)
	var outBuf, errBuf bytes.Buffer
	cmd.Stdout = &outBuf
	cmd.Stderr = &errBuf

	err := cmd.Start()
	require.NoError(t, err, "启动 bowl mount 进程失败")

	return cmd, &outBuf, &errBuf
}

// mountWithCleanup 挂载磁盘并注册幂等的优雅卸载，返回挂载点与停止函数。
func mountWithCleanup(t *testing.T, endpoint, diskID string, extraFlags ...string) (mountpoint string, stop func()) {
	t.Helper()
	mountpoint = t.TempDir()
	cmd, _, stderr := startBowlMount(t, endpoint, diskID, mountpoint, extraFlags...)
	var once sync.Once
	stop = func() { once.Do(func() { stopBowlMountGracefully(t, cmd, mountpoint) }) }
	t.Cleanup(stop)
	waitForMount(t, mountpoint, "挂载点就绪失败: %s", stderr.String())
	return mountpoint, stop
}

// stopBowlMountGracefully 向 bowl mount 前台子进程发送 SIGTERM 并验证优雅退出与无残留卸载。
func stopBowlMountGracefully(t *testing.T, cmd *exec.Cmd, mountpoint string) {
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM), "发送 SIGTERM 失败")
	require.NoError(t, cmd.Wait(), "bowl mount 进程应以退出码 0 干净退出")
	waitForUnmount(t, mountpoint, "卸载后 mountinfo 不应残留该挂载点")
}
