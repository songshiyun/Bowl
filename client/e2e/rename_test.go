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

func createRegularFile(t *testing.T, path string) {
	t.Helper()
	err := unix.Mknod(path, syscall.S_IFREG|0o644, 0)
	require.NoError(t, err, "创建常规文件失败: %s", path)
}

// TestE2E_Rename 验证同目录/跨目录重命名、覆盖边界、NOREPLACE、EXCHANGE、祖先环检测与 WHITEOUT 拒绝。
func TestE2E_Rename(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint, _ := mountWithCleanup(t, getStorageEndpoint(), disk.GetDiskId(), "--debug")

	t.Run("同目录跨目录移动与非法覆盖边界", func(t *testing.T) {
		fileA := filepath.Join(mountpoint, "file_a.txt")
		fileB := filepath.Join(mountpoint, "file_b.txt")
		createRegularFile(t, fileA)

		require.NoError(t, os.Rename(fileA, fileB))
		_, err := os.Stat(fileA)
		require.True(t, os.IsNotExist(err))
		_, err = os.Stat(fileB)
		require.NoError(t, err)

		subDir1 := filepath.Join(mountpoint, "subdir1")
		require.NoError(t, os.Mkdir(subDir1, 0o755))
		fileInSub1 := filepath.Join(subDir1, "file_in_sub1.txt")
		require.NoError(t, os.Rename(fileB, fileInSub1))

		subDir2 := filepath.Join(mountpoint, "subdir2")
		require.NoError(t, os.Mkdir(subDir2, 0o755))
		fileInSub2 := filepath.Join(subDir2, "file_in_sub2.txt")
		require.NoError(t, os.Rename(fileInSub1, fileInSub2))

		createRegularFile(t, fileA)
		require.NoError(t, os.Rename(fileA, fileInSub2))

		nonEmptyDir := filepath.Join(mountpoint, "non_empty_dst")
		require.NoError(t, os.Mkdir(nonEmptyDir, 0o755))
		createRegularFile(t, filepath.Join(nonEmptyDir, "child.txt"))

		createRegularFile(t, fileA)
		require.ErrorIs(t, os.Rename(fileA, nonEmptyDir), syscall.EEXIST)

		srcDir := filepath.Join(mountpoint, "src_dir")
		require.NoError(t, os.Mkdir(srcDir, 0o755))
		require.ErrorIs(t, os.Rename(srcDir, fileA), syscall.ENOTDIR)
		require.ErrorIs(t, os.Rename(srcDir, nonEmptyDir), syscall.EEXIST)
	})

	t.Run("RENAME_NOREPLACE冲突与缺失拦截", func(t *testing.T) {
		fileA := filepath.Join(mountpoint, "noreplace_a.txt")
		fileB := filepath.Join(mountpoint, "noreplace_b.txt")
		createRegularFile(t, fileA)

		require.NoError(t, unix.Renameat2(unix.AT_FDCWD, fileA, unix.AT_FDCWD, fileB, unix.RENAME_NOREPLACE))

		createRegularFile(t, fileA)
		require.ErrorIs(t, unix.Renameat2(unix.AT_FDCWD, fileA, unix.AT_FDCWD, fileB, unix.RENAME_NOREPLACE), syscall.EEXIST)

		_, err := os.Stat(fileA)
		require.NoError(t, err)
		_, err = os.Stat(fileB)
		require.NoError(t, err)

		missingFile := filepath.Join(mountpoint, "missing_noreplace.txt")
		dstFile := filepath.Join(mountpoint, "dst_noreplace.txt")
		require.ErrorIs(t, unix.Renameat2(unix.AT_FDCWD, missingFile, unix.AT_FDCWD, dstFile, unix.RENAME_NOREPLACE), syscall.ENOENT)
	})

	t.Run("RENAME_EXCHANGE常规文件与非空目录双向原子互换", func(t *testing.T) {
		filePath := filepath.Join(mountpoint, "file_swap")
		createRegularFile(t, filePath)

		dirPath := filepath.Join(mountpoint, "dir_swap")
		require.NoError(t, os.Mkdir(dirPath, 0o755))
		createRegularFile(t, filepath.Join(dirPath, "child.txt"))

		require.NoError(t, unix.Renameat2(unix.AT_FDCWD, filePath, unix.AT_FDCWD, dirPath, unix.RENAME_EXCHANGE))

		infoAfterAtFilePath, err := os.Stat(filePath)
		require.NoError(t, err)
		require.True(t, infoAfterAtFilePath.IsDir())

		infoAfterAtDirPath, err := os.Stat(dirPath)
		require.NoError(t, err)
		require.False(t, infoAfterAtDirPath.IsDir())

		_, err = os.Stat(filepath.Join(filePath, "child.txt"))
		require.NoError(t, err)

		require.NoError(t, unix.Renameat2(unix.AT_FDCWD, filePath, unix.AT_FDCWD, dirPath, unix.RENAME_EXCHANGE))

		missingPath := filepath.Join(mountpoint, "exchange_missing_target")
		require.ErrorIs(t, unix.Renameat2(unix.AT_FDCWD, filePath, unix.AT_FDCWD, missingPath, unix.RENAME_EXCHANGE), syscall.ENOENT)
		require.ErrorIs(t, unix.Renameat2(unix.AT_FDCWD, missingPath, unix.AT_FDCWD, filePath, unix.RENAME_EXCHANGE), syscall.ENOENT)
	})

	t.Run("祖先环EINVAL与WHITEOUT拒绝", func(t *testing.T) {
		parent := filepath.Join(mountpoint, "ancestor_parent")
		child := filepath.Join(parent, "child")
		grandchild := filepath.Join(child, "grandchild")
		require.NoError(t, os.MkdirAll(grandchild, 0o755))

		targetLoop := filepath.Join(grandchild, "parent_cycle")
		require.ErrorIs(t, os.Rename(parent, targetLoop), syscall.EINVAL)

		_, err := os.Stat(grandchild)
		require.NoError(t, err)
		_, err = os.Stat(targetLoop)
		require.True(t, os.IsNotExist(err))

		srcWhiteout := filepath.Join(mountpoint, "whiteout_src.txt")
		dstWhiteout := filepath.Join(mountpoint, "whiteout_dst.txt")
		createRegularFile(t, srcWhiteout)
		require.ErrorIs(t, unix.Renameat2(unix.AT_FDCWD, srcWhiteout, unix.AT_FDCWD, dstWhiteout, unix.RENAME_WHITEOUT), syscall.EOPNOTSUPP)
	})

	recordArtifact(t, map[string]string{
		"basic_rename":              "ok",
		"cross_dir_rename":          "ok",
		"dir_over_file":             "ENOTDIR",
		"dir_over_nonempty":         "EEXIST",
		"noreplace_existing_target": "EEXIST",
		"noreplace_missing_source":  "ENOENT",
		"exchange_file_dir":         "ok",
		"exchange_missing":          "ENOENT",
		"ancestor_cycle":            "EINVAL",
		"whiteout":                  "EOPNOTSUPP",
	})
}
