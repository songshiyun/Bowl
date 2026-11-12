//go:build e2e && linux

package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestE2E_CLI_DaemonMount_ZeroDelayAndDuplicateRejection 验证默认后台模式零延迟挂载、Statfs 配额、系统默认日志、重复挂载拦截及优雅卸载。
func TestE2E_CLI_DaemonMount_ZeroDelayAndDuplicateRejection(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint, "--debug")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "后台挂载执行失败: %s", string(out))

	var stat unix.Stat_t
	err = unix.Stat(mountpoint, &stat)
	require.NoError(t, err, "挂载完成后必须立即可访问挂载点")
	require.Equal(t, uint64(1), stat.Ino, "根目录 Inode 必须重写为 1")
	require.Equal(t, uint32(syscall.S_IFDIR), stat.Mode&syscall.S_IFMT)

	_, err = os.Stat(filepath.Join(mountpoint, "nonexistent-entry.tmp"))
	require.ErrorIs(t, err, os.ErrNotExist)

	var stfs unix.Statfs_t
	err = unix.Statfs(mountpoint, &stfs)
	require.NoError(t, err, "挂载完成后必须立即可读取 statfs")
	require.Equal(t, int64(4096), stfs.Bsize)
	expectedBlocks := disk.GetMaxBytes() / 4096
	require.Equal(t, expectedBlocks, stfs.Blocks)
	require.Positive(t, stfs.Bavail)
	require.GreaterOrEqual(t, stfs.Files, uint64(1))

	dupCmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
	dupOut, dupErr := dupCmd.CombinedOutput()
	require.Error(t, dupErr, "重复挂载已挂载目录必须失败")
	var exitErr *exec.ExitError
	require.ErrorAs(t, dupErr, &exitErr)
	require.NotEqual(t, 0, exitErr.ExitCode())
	require.Contains(t, string(dupOut), "已挂载")

	daemonPID := findBowlDaemonPID(t, mountpoint)
	require.Positive(t, daemonPID)

	umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
	umountOut, umountErr := umountCmd.CombinedOutput()
	require.NoError(t, umountErr, "卸载失败: %s", string(umountOut))

	waitForUnmount(t, mountpoint)
	waitForProcessExit(t, daemonPID)

	expectedLog := fmt.Sprintf("/var/log/bowl/mount-%s.log", disk.GetDiskId())
	logFi, err := os.Stat(expectedLog)
	require.NoError(t, err, "默认系统日志文件必须存在: %s", expectedLog)
	require.Positive(t, logFi.Size(), "默认系统日志文件必须包含实际写入内容")

	recordArtifact(t, map[string]string{
		"mount_ready_zero_delay":  "true",
		"root_ino":                "1",
		"block_size":              "4096",
		"blocks":                  strconv.FormatUint(expectedBlocks, 10),
		"duplicate_mount_blocked": "true",
		"system_log_written":      "true",
		"daemon_exited":           "true",
	})
}

// TestE2E_CLI_LogFiles 验证显式自定义日志落盘及系统日志目录不可写时的非特权用户目录回退。
func TestE2E_CLI_LogFiles(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("显式指定日志文件路径创建并写入", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()
		customLog := filepath.Join(t.TempDir(), "logs", "custom-mount.log")

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, "--log-file", customLog, "--debug", disk.GetDiskId(), mountpoint)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "挂载失败: %s", string(out))
		_, err = os.Stat(mountpoint)
		require.NoError(t, err, "触发一次 FUSE 请求以产生调试日志")

		daemonPID := findBowlDaemonPID(t, mountpoint)

		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		waitForProcessExit(t, daemonPID)

		fi, err := os.Stat(customLog)
		require.NoError(t, err, "自定义日志文件必须存在")
		require.Positive(t, fi.Size(), "自定义日志文件必须包含实际写入内容")
	})

	t.Run("系统日志目录不可写时回退至XDG_STATE_HOME", func(t *testing.T) {
		disk := createTestDisk(t, client)
		require.NoError(t, os.MkdirAll("/var/log/bowl", 0o755))
		xdgState, err := os.MkdirTemp("", "bowl-xdg-")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(xdgState) })
		require.NoError(t, os.Chmod(xdgState, 0o777))
		mountpoint, err := os.MkdirTemp("", "bowl-mount-")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(mountpoint) })
		require.NoError(t, os.Chmod(mountpoint, 0o777))
		t.Setenv("XDG_STATE_HOME", xdgState)

		exitCode, stdout, stderr := runAs(t, 65534, 65534, getBowlBin(),
			"mount", "--endpoint", endpoint, "--debug", disk.GetDiskId(), mountpoint)
		require.Zero(t, exitCode, "非 root 挂载失败: stdout=%s stderr=%s", stdout, stderr)
		t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", mountpoint).Run() })
		probeCode, probeOut, probeErr := runAs(t, 65534, 65534, "ls", mountpoint)
		require.Zero(t, probeCode, "非 root 访问挂载点失败: stdout=%s stderr=%s", probeOut, probeErr)

		daemonPID := findBowlDaemonPID(t, mountpoint)
		waitForMount(t, mountpoint)

		fallbackLog := filepath.Join(xdgState, "bowl", "logs", fmt.Sprintf("mount-%s.log", disk.GetDiskId()))
		fi, err := os.Stat(fallbackLog)
		require.NoError(t, err, "系统日志目录不可写时必须回退到 XDG_STATE_HOME: %s", fallbackLog)
		require.Positive(t, fi.Size(), "回退日志文件必须包含实际写入内容")

		_, err = os.Stat(fmt.Sprintf("/var/log/bowl/mount-%s.log", disk.GetDiskId()))
		require.ErrorIs(t, err, os.ErrNotExist, "回退后不得在 /var/log/bowl 留下日志")

		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		waitForProcessExit(t, daemonPID)
	})

	t.Run("XDG_STATE_HOME为空时回退至HOME本地状态目录", func(t *testing.T) {
		disk := createTestDisk(t, client)
		require.NoError(t, os.MkdirAll("/var/log/bowl", 0o755))
		home, err := os.MkdirTemp("", "bowl-home-")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(home) })
		require.NoError(t, os.Chmod(home, 0o777))
		mountpoint, err := os.MkdirTemp("", "bowl-mount-")
		require.NoError(t, err)
		t.Cleanup(func() { _ = os.RemoveAll(mountpoint) })
		require.NoError(t, os.Chmod(mountpoint, 0o777))
		t.Setenv("XDG_STATE_HOME", "")
		t.Setenv("HOME", home)

		exitCode, stdout, stderr := runAs(t, 65534, 65534, getBowlBin(),
			"mount", "--endpoint", endpoint, "--debug", disk.GetDiskId(), mountpoint)
		require.Zero(t, exitCode, "非 root 挂载失败: stdout=%s stderr=%s", stdout, stderr)
		t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", mountpoint).Run() })
		probeCode, probeOut, probeErr := runAs(t, 65534, 65534, "ls", mountpoint)
		require.Zero(t, probeCode, "非 root 访问挂载点失败: stdout=%s stderr=%s", probeOut, probeErr)

		daemonPID := findBowlDaemonPID(t, mountpoint)
		waitForMount(t, mountpoint)

		fallbackLog := filepath.Join(home, ".local", "state", "bowl", "logs", fmt.Sprintf("mount-%s.log", disk.GetDiskId()))
		fi, err := os.Stat(fallbackLog)
		require.NoError(t, err, "XDG_STATE_HOME 为空时必须回退到 HOME/.local/state: %s", fallbackLog)
		require.Positive(t, fi.Size(), "回退日志文件必须包含实际写入内容")

		_, err = os.Stat(fmt.Sprintf("/var/log/bowl/mount-%s.log", disk.GetDiskId()))
		require.ErrorIs(t, err, os.ErrNotExist, "回退后不得在 /var/log/bowl 留下日志")

		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		waitForProcessExit(t, daemonPID)
	})

	recordArtifact(t, map[string]string{
		"explicit_log_written":  "true",
		"xdg_fallback_written":  "true",
		"home_fallback_written": "true",
	})
}

// TestE2E_CLI_Umount_AliasAndForceLazy 验证忙碌目录下常规卸载拒绝及 bowl unmount -f 懒卸载释放后零残留退出。
func TestE2E_CLI_Umount_AliasAndForceLazy(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	disk := createTestDisk(t, client)
	mountpoint := t.TempDir()

	cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
	require.NoError(t, cmd.Run())

	daemonPID := findBowlDaemonPID(t, mountpoint)

	occupier := exec.Command("cat")
	occupier.Dir = mountpoint
	occupierStdin, err := occupier.StdinPipe()
	require.NoError(t, err, "创建目录占用进程 stdin 失败")
	require.NoError(t, occupier.Start(), "启动目录占用进程失败")
	defer func() { _ = occupierStdin.Close() }()

	normalUmount := exec.Command(getBowlBin(), "umount", mountpoint)
	normalOut, normalErr := normalUmount.CombinedOutput()
	require.Error(t, normalErr, "忙碌目录下常规卸载应失败: %s", string(normalOut))

	forceUmount := exec.Command(getBowlBin(), "unmount", "-f", mountpoint)
	forceOut, forceErr := forceUmount.CombinedOutput()
	require.NoError(t, forceErr, "unmount -f 懒卸载必须成功: %s", string(forceOut))

	waitForUnmount(t, mountpoint)

	_ = occupier.Process.Signal(syscall.SIGKILL)
	_ = occupier.Wait()

	waitForProcessExit(t, daemonPID)

	recordArtifact(t, map[string]string{
		"busy_normal_umount":  "rejected",
		"unmount_alias":       "unmount",
		"force_lazy_unmount":  "detached",
		"daemon_exited_clean": "true",
	})
}
