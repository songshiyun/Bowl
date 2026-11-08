//go:build e2e && linux

package e2e_test

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestE2E_FileHandle(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("create_open_sync_and_excl_conflict", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		filePath := filepath.Join(mountpoint, "file-test.txt")

		// 1. O_CREAT 创建文件并成功打开
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		require.NoError(t, err, "O_CREATE|O_EXCL 创建新文件必须成功")
		t.Cleanup(func() { _ = f.Close() })

		// 2. O_CREAT|O_EXCL 冲突断言 EEXIST
		_, errConflict := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		require.Error(t, errConflict, "排他创建已存在文件必须报错")
		require.True(t, errors.Is(errConflict, os.ErrExist) || errors.Is(errConflict, syscall.EEXIST), "排他创建冲突必须返回 EEXIST")

		// 3. f.Sync() 与 fdatasync
		require.NoError(t, f.Sync(), "f.Sync 必须成功")
		require.NoError(t, unix.Fdatasync(int(f.Fd())), "unix.Fdatasync 必须成功")

		// 4. 关闭文件句柄
		require.NoError(t, f.Close(), "关闭常规文件句柄必须成功")

		// 5. 对目录调用非目录写打开错误边界 (EISDIR)
		_, errDir := os.OpenFile(mountpoint, os.O_WRONLY, 0)
		require.Error(t, errDir, "写模式打开目录必须报错")
		require.ErrorIs(t, errDir, syscall.EISDIR, "错误必须为 EISDIR")
	})

	t.Run("readonly_mount_blocks_write_allows_read", func(t *testing.T) {
		disk := createTestDisk(t, client)

		prepareSnapshot(t, client, disk.GetDiskId(), func(mp string) {
			require.NoError(t, os.WriteFile(filepath.Join(mp, "ro-target.txt"), []byte("只读"), 0o644))
		})

		mountpoint := t.TempDir()

		// 2. 以 --ro 只读模式挂载
		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--ro", "--debug")
		waitForMount(t, mountpoint, "只读挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		existingFile := filepath.Join(mountpoint, "ro-target.txt")
		newFile := filepath.Join(mountpoint, "new-file.txt")

		// 3. 禁止创建新文件 (EROFS)
		_, errCreate := os.OpenFile(newFile, os.O_CREATE|os.O_WRONLY, 0o644)
		require.Error(t, errCreate, "只读挂载禁止创建新文件")
		require.ErrorIs(t, errCreate, syscall.EROFS, "错误必须为 EROFS")

		// 4. 禁止写模式打开既有文件 (EROFS)
		_, errWrite := os.OpenFile(existingFile, os.O_WRONLY, 0)
		require.Error(t, errWrite, "只读挂载禁止 O_WRONLY 打开")
		require.ErrorIs(t, errWrite, syscall.EROFS, "错误必须为 EROFS")

		_, errRW := os.OpenFile(existingFile, os.O_RDWR, 0)
		require.Error(t, errRW, "只读挂载禁止 O_RDWR 打开")
		require.ErrorIs(t, errRW, syscall.EROFS, "错误必须为 EROFS")

		// 5. 允许只读模式打开既有文件
		roF, errRO := os.OpenFile(existingFile, os.O_RDONLY, 0)
		require.NoError(t, errRO, "只读挂载允许 O_RDONLY 打开")
		require.NoError(t, roF.Close(), "关闭只读文件句柄")
	})

	t.Run("tmpfile_linkat_persists_across_remount", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

		fd, err := unix.Open(mountpoint, unix.O_TMPFILE|unix.O_RDWR, 0o600)
		require.NoError(t, err, "O_TMPFILE 打开匿名文件必须成功")
		t.Cleanup(func() { _ = unix.Close(fd) })

		payload := []byte("anonymous-tmpfile-payload")
		written, err := unix.Write(fd, payload)
		require.NoError(t, err)
		require.Equal(t, len(payload), written)

		buf := make([]byte, len(payload))
		read, err := unix.Pread(fd, buf, 0)
		require.NoError(t, err)
		require.Equal(t, len(payload), read)
		require.Equal(t, payload, buf)

		namedPath := filepath.Join(mountpoint, "tmpfile-named.txt")
		require.NoError(t, unix.Linkat(
			unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", fd),
			unix.AT_FDCWD, namedPath, unix.AT_SYMLINK_FOLLOW,
		), "O_TMPFILE 匿名文件必须能通过 /proc/self/fd 具名化")
		require.NoError(t, unix.Close(fd))

		immediate, err := os.ReadFile(namedPath)
		require.NoError(t, err)
		require.Equal(t, payload, immediate)
		namedDigest := fileSHA256(t, namedPath)
		stop()

		remountpoint, stopRemount := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")
		remountedPath := filepath.Join(remountpoint, "tmpfile-named.txt")
		persisted, err := os.ReadFile(remountedPath)
		require.NoError(t, err, "卸载重挂后具名化的 O_TMPFILE 必须仍可读取")
		require.Equal(t, payload, persisted)
		persistedDigest := fileSHA256(t, remountedPath)
		stopRemount()

		recordArtifact(t, map[string]string{
			"tmpfile_bytes":             strconv.Itoa(len(payload)),
			"tmpfile_named_sha256":      namedDigest,
			"tmpfile_persisted_sha256":  persistedDigest,
			"tmpfile_remount_persisted": "true",
		})
	})

	t.Run("unlink_whilst_open_orphan_lifecycle", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		targetName := "orphan-unlink.txt"
		filePath := filepath.Join(mountpoint, targetName)

		// 1. 通过挂载点打开并创建常规文件
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		require.NoError(t, err, "挂载点打开创建常规文件必须成功")
		t.Cleanup(func() { _ = f.Close() })

		// 2. 通过同一挂载点解链，客户端保留已打开的孤儿句柄。
		require.NoError(t, os.Remove(filePath))
		_, err = os.Stat(filePath)
		require.ErrorIs(t, err, os.ErrNotExist)

		// 4. 断言已打开孤儿句柄 f.Stat() 与 f.Sync() 依然成功，且内核感知的 Nlink 最终变为 0
		require.NoError(t, f.Sync(), "孤儿句柄 f.Sync() 必须成功")
		fi, err := f.Stat()
		require.NoError(t, err, "已解链已打开句柄 f.Stat() 必须依然成功")
		require.Equal(t, targetName, fi.Name())

		require.Eventually(t, func() bool {
			var stat unix.Stat_t
			return unix.Fstat(int(f.Fd()), &stat) == nil && stat.Nlink == 0
		}, 3*time.Second, 100*time.Millisecond, "孤儿句柄的 Nlink 必须变为 0")

		// 5. 孤儿句柄仍可读写，关闭后完成本地句柄释放。
		_, err = f.WriteString("孤儿数据")
		require.NoError(t, err)
		require.NoError(t, f.Close())

	})

	recordArtifact(t, map[string]string{
		"excl_conflict":       "EEXIST",
		"directory_write":     "EISDIR",
		"readonly_create":     "EROFS",
		"readonly_open_write": "EROFS",
		"orphan_nlink":        "0",
		"orphan_readwrite":    "ok",
	})
}
