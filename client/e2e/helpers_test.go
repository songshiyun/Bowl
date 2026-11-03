//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// getStorageEndpoint 获取测试环境的 StorageService 接入地址。
func getStorageEndpoint() string {
	ep := os.Getenv("BOWL_STORAGE_ENDPOINT")
	if ep != "" {
		return ep
	}
	return "http://storage-service:9000"
}

// getBowlBin 获取 bowl CLI 二进制文件路径。
func getBowlBin() string {
	bin := os.Getenv("BOWL_BIN")
	if bin != "" {
		return bin
	}
	return "/tmp/bowl"
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

// commitMain 创建当前 main 分支的不可变快照。
func commitMain(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: diskID, BranchName: "main"}))
	require.NoError(t, err, "创建分支提交失败")
	require.NotNil(t, resp.Msg.GetCommit())
	id := resp.Msg.GetCommit().GetCommitId()
	require.NotEmpty(t, id)
	return id
}

// prepareSnapshot 通过可写挂载准备文件并提交，供只读挂载读取。
func prepareSnapshot(t *testing.T, client storagev1connect.StorageServiceClient, diskID string, prepare func(string)) string {
	t.Helper()
	mp := t.TempDir()
	cmd, _, stderr := startBowlMount(t, getStorageEndpoint(), diskID, mp)
	if err := waitForMount(mp, 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("准备数据时挂载失败: %v: %s", err, stderr.String())
	}
	defer stopBowlMountGracefully(t, cmd, mp, 5*time.Second)
	prepare(mp)
	return commitMain(t, client, diskID)
}

// isMounted 通过读取 /proc/self/mountinfo 确认目标路径是否为已生效的挂载点。
func isMounted(mountpoint string) bool {
	data, err := os.ReadFile("/proc/self/mountinfo")
	if err != nil {
		return false
	}
	target := filepath.Clean(mountpoint)
	lines := strings.Split(string(data), "\n")
	for _, line := range lines {
		fields := strings.Fields(line)
		if len(fields) >= 5 {
			if filepath.Clean(fields[4]) == target {
				return true
			}
		}
	}
	return false
}

// waitForMount 轮询等待挂载点出现在 /proc/self/mountinfo 中。
func waitForMount(mountpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if isMounted(mountpoint) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("挂载点 %s 在 %v 内未完成就绪", mountpoint, timeout)
}

// waitForUnmount 轮询等待挂载点从 /proc/self/mountinfo 中消失。
func waitForUnmount(mountpoint string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !isMounted(mountpoint) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("挂载点 %s 在 %v 后仍残留在 mountinfo 中", mountpoint, timeout)
}

// startBowlMount 启动 bowl mount -f 前台子进程。
func startBowlMount(t *testing.T, endpoint, diskID, mountpoint string, extraFlags ...string) (*exec.Cmd, *bytes.Buffer, *bytes.Buffer) {
	args := []string{"mount", "-f", "--endpoint", endpoint, diskID, mountpoint}
	args = append(args, extraFlags...)

	cmd := exec.Command(getBowlBin(), args...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Start()
	require.NoError(t, err, "启动 bowl mount 进程失败")

	return cmd, &stdout, &stderr
}

// stopBowlMountGracefully 向 bowl mount 前台子进程发送 SIGTERM 并验证优雅退出与无残留卸载。
func stopBowlMountGracefully(t *testing.T, cmd *exec.Cmd, mountpoint string, timeout time.Duration) {
	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM), "发送 SIGTERM 失败")

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		require.NoError(t, err, "bowl mount 进程应以退出码 0 干净退出")
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		t.Fatalf("bowl mount 进程在 %v 内未响应 SIGTERM 退出", timeout)
	}

	require.NoError(t, waitForUnmount(mountpoint, 5*time.Second), "卸载后 mountinfo 不应残留该挂载点")
}
