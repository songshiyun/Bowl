//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestE2E_FileHandle(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("常规句柄创建排他冲突与孤儿解链生命周期", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, _ := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

		filePath := filepath.Join(mountpoint, "file-test.txt")

		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		require.NoError(t, err, "O_CREATE|O_EXCL 创建新文件必须成功")
		t.Cleanup(func() { _ = f.Close() })

		_, errConflict := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		require.Error(t, errConflict, "排他创建已存在文件必须报错")
		require.True(t, errors.Is(errConflict, os.ErrExist) || errors.Is(errConflict, syscall.EEXIST))

		require.NoError(t, f.Sync(), "f.Sync 必须成功")
		require.NoError(t, unix.Fdatasync(int(f.Fd())), "unix.Fdatasync 必须成功")
		require.NoError(t, f.Close(), "关闭常规文件句柄必须成功")

		_, errDir := os.OpenFile(mountpoint, os.O_WRONLY, 0)
		require.ErrorIs(t, errDir, syscall.EISDIR, "写模式打开目录必须返回 EISDIR")

		targetName := "orphan-unlink.txt"
		orphanPath := filepath.Join(mountpoint, targetName)
		orphanF, err := os.OpenFile(orphanPath, os.O_CREATE|os.O_RDWR, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = orphanF.Close() })

		require.NoError(t, os.Remove(orphanPath))
		_, err = os.Stat(orphanPath)
		require.ErrorIs(t, err, os.ErrNotExist)

		require.NoError(t, orphanF.Sync(), "已解链孤儿句柄 Sync 必须成功")
		fi, err := orphanF.Stat()
		require.NoError(t, err, "已解链孤儿句柄 Stat 必须依然成功")
		require.Equal(t, targetName, fi.Name())

		require.Eventually(t, func() bool {
			var stat unix.Stat_t
			return unix.Fstat(int(orphanF.Fd()), &stat) == nil && stat.Nlink == 0
		}, 3*time.Second, 50*time.Millisecond, "孤儿句柄 Nlink 必须变为 0")

		_, err = orphanF.WriteString("孤儿数据")
		require.NoError(t, err)
		require.NoError(t, orphanF.Close())
	})

	var sparseSize int
	var namedDigest, persistedDigest string
	var unlinkedReclaimed bool

	t.Run("匿名文件跨分片稀疏打洞具名化持久化与未具名落盘孤儿回收", func(t *testing.T) {
		disk := createTestDisk(t, client)
		diskID := disk.GetDiskId()
		mountpoint, stop := mountWithCleanup(t, endpoint, diskID, "--debug")

		// 1. 验证未具名 O_TMPFILE 显式 Fsync 上云后关闭句柄触发服务端孤儿空间自动回收
		unlinkedFD, err := unix.Open(mountpoint, unix.O_TMPFILE|unix.O_RDWR, 0o600)
		require.NoError(t, err, "打开未具名 O_TMPFILE 必须成功")
		unlinkedClosed := false
		t.Cleanup(func() {
			map[bool]func(){true: func() {}, false: func() { _ = unix.Close(unlinkedFD) }}[unlinkedClosed]()
		})

		unlinkedPayload := bytes.Repeat([]byte{'U'}, 512*1024)
		nUnlinked, err := unix.Write(unlinkedFD, unlinkedPayload)
		require.NoError(t, err)
		require.Equal(t, len(unlinkedPayload), nUnlinked)
		require.NoError(t, unix.Fsync(unlinkedFD), "未具名 O_TMPFILE Fsync 必须成功上云")

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		diskDuringSync, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
		require.NoError(t, err)
		require.GreaterOrEqual(t, diskDuringSync.Msg.GetDisk().GetUsedBytes(), uint64(len(unlinkedPayload)), "Fsync 后服务端必须统计孤儿文件占用容量")

		require.NoError(t, unix.Close(unlinkedFD))
		unlinkedClosed = true

		require.Eventually(t, func() bool {
			rCtx, rCancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer rCancel()
			resp, gErr := client.GetDisk(rCtx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
			return gErr == nil && resp.Msg.GetDisk().GetUsedBytes() == 0
		}, 5*time.Second, 50*time.Millisecond, "关闭未具名已上云 O_TMPFILE 后服务端孤儿容量必须自动回收归零")
		unlinkedReclaimed = true

		// 2. 验证跨 >1MiB 聚合块连续写入 + 天然稀疏空洞 + PUNCH_HOLE 打洞后 Linkat 具名化与重挂持久化
		fd, err := unix.Open(mountpoint, unix.O_TMPFILE|unix.O_RDWR, 0o600)
		require.NoError(t, err, "O_TMPFILE 打开匿名文件必须成功")
		fdClosed := false
		t.Cleanup(func() {
			map[bool]func(){true: func() {}, false: func() { _ = unix.Close(fd) }}[fdClosed]()
		})

		const headLen = 1536 * 1024 // 1.5 MiB 跨越 1 MiB 上传分片边界
		const tailOff = 3 * 1024 * 1024
		const tailLen = 64 * 1024
		const holeOff = 512 * 1024
		const holeLen = 64 * 1024
		const totalLen = tailOff + tailLen

		headData := bytes.Repeat([]byte("TMPHEAD-"), headLen/8)
		tailData := bytes.Repeat([]byte("TMPTAIL!"), tailLen/8)
		expected := make([]byte, totalLen)
		copy(expected[0:headLen], headData)
		copy(expected[tailOff:totalLen], tailData)
		clear(expected[holeOff : holeOff+holeLen])

		writtenHead, err := unix.Pwrite(fd, headData, 0)
		require.NoError(t, err)
		require.Equal(t, headLen, writtenHead)

		writtenTail, err := unix.Pwrite(fd, tailData, tailOff)
		require.NoError(t, err)
		require.Equal(t, tailLen, writtenTail)

		require.NoError(t, unix.Fallocate(fd, unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, holeOff, holeLen), "本地匿名文件 PUNCH_HOLE 打洞必须成功")

		localBuf := make([]byte, totalLen)
		readLocal, err := unix.Pread(fd, localBuf, 0)
		require.NoError(t, err)
		require.Equal(t, totalLen, readLocal)
		require.Equal(t, expected, localBuf, "具名化前本地稀疏缓冲读取必须与预期模型一致")

		namedPath := filepath.Join(mountpoint, "tmpfile-sparse-named.bin")
		require.NoError(t, unix.Linkat(
			unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", fd),
			unix.AT_FDCWD, namedPath, unix.AT_SYMLINK_FOLLOW,
		), "跨分片稀疏 O_TMPFILE 必须能通过 /proc/self/fd 具名化")
		require.NoError(t, unix.Close(fd))
		fdClosed = true

		expectedSum := sha256.Sum256(expected)
		expectedHex := hex.EncodeToString(expectedSum[:])
		namedDigest = fileSHA256(t, namedPath)
		require.Equal(t, expectedHex, namedDigest)
		sparseSize = totalLen
		stop()

		remountpoint, stopRemount := mountWithCleanup(t, endpoint, diskID, "--debug")
		remountedPath := filepath.Join(remountpoint, "tmpfile-sparse-named.bin")
		persisted, err := os.ReadFile(remountedPath)
		require.NoError(t, err, "卸载重挂后具名化的稀疏 O_TMPFILE 必须仍可完整读取")
		require.Equal(t, expected, persisted)
		persistedDigest = fileSHA256(t, remountedPath)
		require.Equal(t, expectedHex, persistedDigest)
		stopRemount()
	})

	recordArtifact(t, map[string]string{
		"excl_conflict":             "EEXIST",
		"directory_write":           "EISDIR",
		"orphan_nlink":              "0",
		"orphan_readwrite":          "ok",
		"tmpfile_unlinked_reclaim":  strconv.FormatBool(unlinkedReclaimed),
		"tmpfile_sparse_bytes":      strconv.Itoa(sparseSize),
		"tmpfile_named_sha256":      namedDigest,
		"tmpfile_persisted_sha256":  persistedDigest,
		"tmpfile_remount_persisted": strconv.FormatBool(namedDigest == persistedDigest && namedDigest != ""),
	})
}
