package cli

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func withUmountToolStub(t *testing.T, availableTool, toolPath string, out []byte, err error) (calledName *string, calledArgs *[]string) {
	t.Helper()
	prevLook := lookPath
	prevRun := runCommand

	var name string
	var args []string
	lookPath = func(file string) (string, error) {
		if file == availableTool {
			return toolPath, nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(bin string, a ...string) ([]byte, error) {
		name = bin
		args = append([]string(nil), a...)
		return out, err
	}
	t.Cleanup(func() {
		lookPath = prevLook
		runCommand = prevRun
	})
	return &name, &args
}

func TestUmount_GivenHelpAndInvalidArgs_ReturnsExpectedExitCodes(t *testing.T) {
	// Given / When / Then
	code, out, errOut := runCLI("umount", "-h")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "bowl umount <mountpoint>")

	code, out, errOut = runCLI("unmount", "--help")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "bowl umount <mountpoint>")

	code, out, errOut = runCLI("umount")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "需要且仅需要 1 个位置参数")

	code, out, errOut = runCLI("umount", "/mnt/a", "/mnt/b")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "需要且仅需要 1 个位置参数")

	code, out, errOut = runCLI("umount", "--bogus", "/mnt/a")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, `未知标志 "--bogus"`)
}

func TestUmount_GivenUnmountedOrUnreadableMountInfo_ReturnsExit1(t *testing.T) {
	// Given: 未挂载目录
	withMountInfoContent(t, "100 30 0:50 / /mnt/other rw - fuse.bowl bowl:"+testDiskID1+" rw\n")

	// When
	code, out, errOut := runCLI("umount", "/mnt/not-mounted")

	// Then
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "不是已挂载的文件系统")

	// Given: mountinfo 文件不可读
	withUnreadableMountInfo(t)
	code, out, errOut = runCLI("umount", "/mnt/not-mounted")
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "检查挂载点状态失败")
}

func TestUmount_GivenFusermount3Available_ExecutesNormalAndForceArgs(t *testing.T) {
	// Given
	mp := "/mnt/bowl-a"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	binName, binArgs := withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", nil, nil)

	// When: 常规卸载
	code, out, errOut := runCLI("umount", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, "/usr/bin/fusermount3", *binName)
	require.Equal(t, []string{"-u", mp}, *binArgs)

	// When: unmount 别名 + -f 强制卸载
	code, out, errOut = runCLI("unmount", "-f", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-uz", mp}, *binArgs)
}

func TestUmount_GivenFusermountAndUmountFallbacks_SelectsExpectedToolAndFlags(t *testing.T) {
	// Given: 仅 fusermount 可用
	mp := "/mnt/bowl-b"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	binName, binArgs := withUmountToolStub(t, "fusermount", "/bin/fusermount", nil, nil)

	// When
	code, _, errOut := runCLI("umount", "--force", mp)

	// Then
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "/bin/fusermount", *binName)
	require.Equal(t, []string{"-uz", mp}, *binArgs)

	// Given: 仅系统 umount 可用
	binName, binArgs = withUmountToolStub(t, "umount", "/bin/umount", nil, nil)

	// When: 普通与 -f 模式
	code, _, errOut = runCLI("umount", mp)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "/bin/umount", *binName)
	require.Equal(t, []string{mp}, *binArgs)

	code, _, errOut = runCLI("umount", "-f", mp)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, []string{"-l", mp}, *binArgs)
}

func TestUmount_GivenMissingToolOrToolFailure_ReturnsExit1WithErrorMessage(t *testing.T) {
	// Given: 无可用卸载工具
	mp := "/mnt/bowl-c"
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	withUmountToolStub(t, "none", "", nil, nil)

	// When
	code, _, errOut := runCLI("umount", mp)

	// Then
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "未找到系统卸载工具")

	// Given: 卸载工具执行报错
	withUmountToolStub(t, "fusermount3", "/usr/bin/fusermount3", []byte("device busy"), errors.New("exit status 1"))

	// When
	code, _, errOut = runCLI("umount", mp)

	// Then
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "卸载执行失败")
	require.Contains(t, errOut, "device busy")
}

func withUmountDrainStub(t *testing.T, timeout, poll time.Duration) {
	t.Helper()
	prevTimeout := umountDrainTimeout
	prevPoll := umountDrainPollInterval
	umountDrainTimeout = timeout
	umountDrainPollInterval = poll
	t.Cleanup(func() {
		umountDrainTimeout = prevTimeout
		umountDrainPollInterval = prevPoll
	})
}

func TestUmount_GivenActiveMountMeta_WaitsForDrainOnNormalAndTimesOutWhenStuck(t *testing.T) {
	// Given: 常规卸载前存在 mountMeta，卸载命令返回 0 后异步清理 mountMeta
	mp := "/mnt/bowl-drain"
	metaDir := t.TempDir()
	withTempMountMetaDirs(t, metaDir)
	withMountInfoContent(t, "100 30 0:50 / "+mp+" rw - fuse.bowl bowl:"+testDiskID1+" rw\n")
	withUmountDrainStub(t, 500*time.Millisecond, 5*time.Millisecond)

	metaPath, err := writeMountMeta(&mountMeta{
		Mountpoint: mp,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)

	prevLook := lookPath
	prevRun := runCommand
	lookPath = func(file string) (string, error) {
		if file == "fusermount3" {
			return "/usr/bin/fusermount3", nil
		}
		return "", errors.New("not found")
	}
	runCommand = func(_ string, _ ...string) ([]byte, error) {
		go func() {
			time.Sleep(30 * time.Millisecond)
			_ = os.Remove(metaPath)
		}()
		return nil, nil
	}
	t.Cleanup(func() {
		lookPath = prevLook
		runCommand = prevRun
	})

	// When
	code, out, errOut := runCLI("umount", mp)

	// Then: 等待 mountMeta 消失后才返回 0
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
	_, statErr := os.Stat(metaPath)
	require.True(t, os.IsNotExist(statErr), "bowl umount 返回 0 时 mountMeta 必须已消失")

	// Given: 常规卸载等待超时（mountMeta 始终未消失且 PID 存活）
	metaPath2, err := writeMountMeta(&mountMeta{
		Mountpoint: mp,
		PID:        os.Getpid(),
		DiskID:     testDiskID1,
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	})
	require.NoError(t, err)
	withUmountDrainStub(t, 30*time.Millisecond, 5*time.Millisecond)
	runCommand = func(_ string, _ ...string) ([]byte, error) {
		return nil, nil
	}

	// When
	code, _, errOut = runCLI("umount", mp)

	// Then
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "超时")

	// Given: -f 强制卸载即使 mountMeta 仍存在也不等待，直接返回 0
	_, statErr2 := os.Stat(metaPath2)
	require.NoError(t, statErr2)
	code, out, errOut = runCLI("umount", "-f", mp)
	require.Equal(t, 0, code)
	require.Empty(t, out)
	require.Empty(t, errOut)
}
