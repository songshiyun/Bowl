//go:build e2e && linux

package e2e_test

import (
	"os"
	"path/filepath"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	fsImmutableFl = 0x00000010
	fsAppendFl    = 0x00000020
)

func getIoctlFlags(fd int) (int, error) { return unix.IoctlGetInt(fd, unix.FS_IOC_GETFLAGS) }
func setIoctlFlags(fd, flags int) error {
	return unix.IoctlSetPointerInt(fd, unix.FS_IOC_SETFLAGS, flags)
}

func requireWriteDenied(t *testing.T, path string) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY, 0)
	require.NoError(t, err)
	_, err = f.WriteString("denied")
	require.ErrorIs(t, err, syscall.EPERM)
	require.NoError(t, f.Close())
}

func requireTruncateDenied(t *testing.T, path string) {
	t.Helper()
	require.ErrorIs(t, os.Truncate(path, 0), syscall.EPERM)
}

func requireRenameDenied(t *testing.T, path string) {
	t.Helper()
	require.ErrorIs(t, os.Rename(path, path+".renamed"), syscall.EPERM)
}

func requireUnlinkDenied(t *testing.T, path string) {
	t.Helper()
	require.ErrorIs(t, os.Remove(path), syscall.EPERM)
}

func verifyImmutable(t *testing.T, path string) {
	t.Helper()
	requireWriteDenied(t, path)
	requireTruncateDenied(t, path)
	requireRenameDenied(t, path)
	requireUnlinkDenied(t, path)
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, "line 1\n", string(data))
}

func clearFlagsOnPath(t *testing.T, path string) {
	t.Helper()
	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()
	require.NoError(t, setIoctlFlags(int(f.Fd()), 0))
}

// TestE2E_Ioctl 通过真实挂载点验证 FS_IOC_SETFLAGS/GETFLAGS 的不可变、仅追加、组合标志及非法命令边界。
func TestE2E_Ioctl(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mp, _ := mountWithCleanup(t, getStorageEndpoint(), disk.GetDiskId(), "--debug")

	t.Run("不可变标志拦截全部修改与删除", func(t *testing.T) {
		path := filepath.Join(mp, "immutable.txt")
		require.NoError(t, os.WriteFile(path, []byte("line 1\n"), 0o644))
		f, err := os.Open(path)
		require.NoError(t, err)
		require.NoError(t, setIoctlFlags(int(f.Fd()), fsImmutableFl))
		got, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsImmutableFl, got)
		require.NoError(t, f.Close())
		t.Cleanup(func() { clearFlagsOnPath(t, path) })

		verifyImmutable(t, path)
	})

	t.Run("仅追加标志放行O_APPEND并拦截覆写截断", func(t *testing.T) {
		path := filepath.Join(mp, "append.txt")
		require.NoError(t, os.WriteFile(path, []byte("line 1\n"), 0o644))
		f, err := os.Open(path)
		require.NoError(t, err)
		require.NoError(t, setIoctlFlags(int(f.Fd()), fsAppendFl))
		got, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsAppendFl, got)
		require.NoError(t, f.Close())
		t.Cleanup(func() { clearFlagsOnPath(t, path) })

		af, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
		require.NoError(t, err)
		_, err = af.WriteString("line 2\n")
		require.NoError(t, err)
		require.NoError(t, af.Close())

		data, err := os.ReadFile(path)
		require.NoError(t, err)
		require.Equal(t, "line 1\nline 2\n", string(data))

		requireWriteDenied(t, path)
		requireTruncateDenied(t, path)
	})

	t.Run("组合标志与未知标志及不支持命令", func(t *testing.T) {
		path := filepath.Join(mp, "both.txt")
		require.NoError(t, os.WriteFile(path, []byte("line 1\n"), 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()

		require.NoError(t, setIoctlFlags(int(f.Fd()), fsImmutableFl|fsAppendFl))
		got, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Equal(t, fsImmutableFl|fsAppendFl, got)

		verifyImmutable(t, path)

		require.NoError(t, setIoctlFlags(int(f.Fd()), 0))
		cleared, err := getIoctlFlags(int(f.Fd()))
		require.NoError(t, err)
		require.Zero(t, cleared)

		require.ErrorIs(t, setIoctlFlags(int(f.Fd()), 0x40), syscall.EOPNOTSUPP)
		_, err = unix.IoctlGetInt(int(f.Fd()), unix.TIOCGWINSZ)
		require.ErrorIs(t, err, syscall.ENOTTY)
	})

	recordArtifact(t, map[string]string{
		"immutable_getflags": "0x10",
		"append_getflags":    "0x20",
		"both_getflags":      "0x30",
		"append_write":       "line 1\\nline 2\\n",
		"immutable_write":    "EPERM",
		"immutable_truncate": "EPERM",
		"immutable_rename":   "EPERM",
		"immutable_unlink":   "EPERM",
		"append_overwrite":   "EPERM",
		"append_truncate":    "EPERM",
		"clear_flags":        "ok",
		"unknown_flag":       "EOPNOTSUPP",
		"unsupported_ioctl":  "ENOTTY",
	})
}
