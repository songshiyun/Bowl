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

// TestE2E_Topology 验证目录树增删与父目录 nlink 守恒、SGID 属组与特权位继承及 FIFO 特殊节点创建解链。
func TestE2E_Topology(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint, _ := mountWithCleanup(t, getStorageEndpoint(), disk.GetDiskId(), "--debug")

	t.Run("嵌套目录创建删除与根目录硬链接数守恒", func(t *testing.T) {
		var rootStatBefore unix.Stat_t
		require.NoError(t, unix.Stat(mountpoint, &rootStatBefore))
		initNlink := rootStatBefore.Nlink

		dir1 := filepath.Join(mountpoint, "dir1")
		require.NoError(t, os.Mkdir(dir1, 0o755))

		var rootStatAfter unix.Stat_t
		require.NoError(t, unix.Stat(mountpoint, &rootStatAfter))
		require.Equal(t, initNlink+1, rootStatAfter.Nlink)

		require.ErrorIs(t, os.Mkdir(dir1, 0o755), syscall.EEXIST)

		dir2 := filepath.Join(dir1, "dir2")
		dir3 := filepath.Join(dir2, "dir3")
		require.NoError(t, os.MkdirAll(dir3, 0o755))
		require.ErrorIs(t, syscall.Rmdir(dir1), syscall.ENOTEMPTY)

		require.NoError(t, syscall.Rmdir(dir3))
		require.NoError(t, syscall.Rmdir(dir2))
		require.NoError(t, syscall.Rmdir(dir1))

		var rootStatFinal unix.Stat_t
		require.NoError(t, unix.Stat(mountpoint, &rootStatFinal))
		require.Equal(t, initNlink, rootStatFinal.Nlink)
		require.ErrorIs(t, syscall.Rmdir(dir1), syscall.ENOENT)
	})

	t.Run("父目录SGID属组与特权位自动继承", func(t *testing.T) {
		parentDir := filepath.Join(mountpoint, "sgid_parent")
		require.NoError(t, os.Mkdir(parentDir, 0o755))
		require.NoError(t, os.Chown(parentDir, 1000, 2000))
		require.NoError(t, os.Chmod(parentDir, 0o755|os.ModeSetgid))

		childDir := filepath.Join(parentDir, "child")
		require.NoError(t, os.Mkdir(childDir, 0o755))

		var childStat unix.Stat_t
		require.NoError(t, unix.Stat(childDir, &childStat))
		require.Equal(t, uint32(2000), childStat.Gid)
		require.NotZero(t, childStat.Mode&unix.S_ISGID)

		require.NoError(t, syscall.Rmdir(childDir))
		require.NoError(t, syscall.Rmdir(parentDir))
	})

	t.Run("命名管道FIFO节点创建类型边界与解链", func(t *testing.T) {
		fifoPath := filepath.Join(mountpoint, "test_fifo")
		require.NoError(t, unix.Mkfifo(fifoPath, 0o640))

		var stat unix.Stat_t
		require.NoError(t, unix.Stat(fifoPath, &stat))
		require.Equal(t, uint32(syscall.S_IFIFO), stat.Mode&syscall.S_IFMT)
		require.Equal(t, uint32(0o640), stat.Mode&0o777)

		require.ErrorIs(t, unix.Mkfifo(fifoPath, 0o640), syscall.EEXIST)
		require.ErrorIs(t, syscall.Rmdir(fifoPath), syscall.ENOTDIR)

		subDir := filepath.Join(mountpoint, "test_dir")
		require.NoError(t, os.Mkdir(subDir, 0o755))
		require.ErrorIs(t, syscall.Unlink(subDir), syscall.EISDIR)
		require.NoError(t, syscall.Rmdir(subDir))

		require.NoError(t, syscall.Unlink(fifoPath))
		_, err := os.Stat(fifoPath)
		require.ErrorIs(t, err, os.ErrNotExist)
		require.ErrorIs(t, syscall.Unlink(fifoPath), syscall.ENOENT)
	})

	recordArtifact(t, map[string]string{
		"mkdir_rmdir":        "ok",
		"root_nlink_restore": "true",
		"missing_rmdir":      "ENOENT",
		"sgid_inherited":     "true",
		"gid_inherited":      "2000",
		"mknod_fifo":         "ok",
		"unlink_fifo":        "ok",
		"stat_after_unlink":  "ENOENT",
		"unlink_missing":     "ENOENT",
	})
}
