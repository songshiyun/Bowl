//go:build e2e && linux

package e2e_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestE2E_CLI_DaemonMount_ZeroDelayAndDuplicateRejection 验证默认后台模式挂载零延迟访问、重复挂载拦截与 umount 卸载及守护进程退出。
func TestE2E_CLI_DaemonMount_ZeroDelayAndDuplicateRejection(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()

	// 1. 默认后台模式挂载 (未加 -f)
	cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint, "--debug")
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "后台挂载执行失败: %s", string(out))

	// 2. 0 延迟断言: 父进程退出 0 后下一微秒挂载点即处于完全就绪状态 (零竞态验证)
	var stat unix.Stat_t
	err = unix.Stat(mountpoint, &stat)
	require.NoError(t, err, "挂载完成后必须立即可访问挂载点 (0 延迟就绪)")
	require.Equal(t, uint64(1), stat.Ino, "根目录 Inode 必须重写为 1")

	var stfs unix.Statfs_t
	err = unix.Statfs(mountpoint, &stfs)
	require.NoError(t, err, "挂载完成后必须立即可读取 statfs")
	require.Equal(t, int64(4096), stfs.Bsize)

	// 3. 重复挂载校验: 再次对同一挂载点执行挂载必须被立即拦截并返回非零退出码
	dupCmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
	dupOut, dupErr := dupCmd.CombinedOutput()
	require.Error(t, dupErr, "重复挂载已挂载目录必须失败")
	var exitErr *exec.ExitError
	require.ErrorAs(t, dupErr, &exitErr)
	require.NotEqual(t, 0, exitErr.ExitCode())
	require.Contains(t, string(dupOut), "已挂载")

	// 4. 定位后台守护进程 PID 并确保其正常活跃
	daemonPID := findBowlDaemonPID(t, mountpoint)
	require.Greater(t, daemonPID, 0)
	t.Logf("已检测到活跃守护进程 PID: %d", daemonPID)

	// 5. 执行 bowl umount <mountpoint> 卸载
	umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
	umountOut, umountErr := umountCmd.CombinedOutput()
	require.NoError(t, umountErr, "卸载失败: %s", string(umountOut))

	// 6. 验证 mountinfo 挂载表已清理，且后台守护进程退出零残留
	waitForUnmount(t, mountpoint)
	waitForProcessExit(t, daemonPID)
	recordArtifact(t, map[string]string{
		"mount_ready_zero_delay":  "true",
		"duplicate_mount_blocked": "true",
		"umount_alias":            "umount",
		"daemon_exited":           "true",
	})
}

// TestE2E_CLI_LogFiles 验证显式 --log-file 落盘、系统日志落盘与 /var/log 不可写时的用户目录回退。
func TestE2E_CLI_LogFiles(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("explicit --log-file is created and written", func(t *testing.T) {
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
		require.Greater(t, fi.Size(), int64(0), "自定义日志文件必须包含实际写入内容")
	})

	t.Run("default system log /var/log/bowl/mount-<disk-id>.log is written", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, "--debug", disk.GetDiskId(), mountpoint)
		out, err := cmd.CombinedOutput()
		require.NoError(t, err, "挂载失败: %s", string(out))
		_, err = os.Stat(mountpoint)
		require.NoError(t, err, "触发一次 FUSE 请求以产生调试日志")

		daemonPID := findBowlDaemonPID(t, mountpoint)

		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		waitForProcessExit(t, daemonPID)

		expectedLog := filepath.Join("/var/log/bowl", fmt.Sprintf("mount-%s.log", disk.GetDiskId()))
		fi, err := os.Stat(expectedLog)
		require.NoError(t, err, "默认系统日志文件必须存在: %s", expectedLog)
		require.Greater(t, fi.Size(), int64(0), "默认系统日志文件必须包含实际写入内容")
	})

	t.Run("falls back to XDG_STATE_HOME when /var/log/bowl is not writable", func(t *testing.T) {
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
		require.Greater(t, fi.Size(), int64(0), "回退日志文件必须包含实际写入内容")

		_, err = os.Stat(filepath.Join("/var/log/bowl", fmt.Sprintf("mount-%s.log", disk.GetDiskId())))
		require.ErrorIs(t, err, os.ErrNotExist, "回退后不得在 /var/log/bowl 留下日志")

		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		waitForProcessExit(t, daemonPID)
	})

	t.Run("falls back to HOME/.local/state when XDG_STATE_HOME is empty", func(t *testing.T) {
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
		require.Greater(t, fi.Size(), int64(0), "回退日志文件必须包含实际写入内容")

		_, err = os.Stat(filepath.Join("/var/log/bowl", fmt.Sprintf("mount-%s.log", disk.GetDiskId())))
		require.ErrorIs(t, err, os.ErrNotExist, "回退后不得在 /var/log/bowl 留下日志")

		umountCmd := exec.Command(getBowlBin(), "umount", mountpoint)
		require.NoError(t, umountCmd.Run())
		waitForProcessExit(t, daemonPID)
	})

	recordArtifact(t, map[string]string{
		"explicit_log_written":  "true",
		"system_log_written":    "true",
		"xdg_fallback_written":  "true",
		"home_fallback_written": "true",
	})
}

// TestE2E_CLI_Umount_AliasAndForceLazy 验证 bowl unmount 别名以及 -f/--force 懒卸载释放占用后零孤儿残留。
func TestE2E_CLI_Umount_AliasAndForceLazy(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("unmount alias works identically to umount", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
		require.NoError(t, cmd.Run())

		daemonPID := findBowlDaemonPID(t, mountpoint)

		unmountCmd := exec.Command(getBowlBin(), "unmount", mountpoint)
		out, err := unmountCmd.CombinedOutput()
		require.NoError(t, err, "unmount 别名执行失败: %s", string(out))

		waitForUnmount(t, mountpoint)
		waitForProcessExit(t, daemonPID)
	})

	t.Run("force lazy unmount detaches busy directory and daemon exits cleanly", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd := exec.Command(getBowlBin(), "mount", "--endpoint", endpoint, disk.GetDiskId(), mountpoint)
		require.NoError(t, cmd.Run())

		daemonPID := findBowlDaemonPID(t, mountpoint)

		// 启动一个保持占用 mountpoint 目录的子进程（阻塞读取 stdin，不依赖固定时长）。
		occupier := exec.Command("cat")
		occupier.Dir = mountpoint
		occupierStdin, err := occupier.StdinPipe()
		require.NoError(t, err, "创建目录占用进程 stdin 失败")
		require.NoError(t, occupier.Start(), "启动目录占用进程失败")
		defer func() { _ = occupierStdin.Close() }()

		// 常规卸载在忙碌目录下应失败
		normalUmount := exec.Command(getBowlBin(), "umount", mountpoint)
		normalOut, normalErr := normalUmount.CombinedOutput()
		require.Error(t, normalErr, "忙碌目录下常规卸载应失败: %s", string(normalOut))

		// 强制懒卸载 (bowl umount -f <mountpoint>)
		forceUmount := exec.Command(getBowlBin(), "umount", "-f", mountpoint)
		forceOut, forceErr := forceUmount.CombinedOutput()
		require.NoError(t, forceErr, "懒卸载必须成功: %s", string(forceOut))

		// 挂载点在 mountinfo 中被立刻摘除
		waitForUnmount(t, mountpoint)

		// 杀死占用目录的进程，内核释放最后的活跃引用
		_ = occupier.Process.Signal(syscall.SIGKILL)
		_ = occupier.Wait()

		// 释放后后台守护进程必须安全退出，无孤儿进程残留
		waitForProcessExit(t, daemonPID)
	})

	recordArtifact(t, map[string]string{
		"unmount_alias":       "unmount",
		"force_lazy_unmount":  "detached",
		"daemon_exited_clean": "true",
	})
}
