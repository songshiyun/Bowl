//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func overwriteAt(t *testing.T, f *os.File, model []byte, offset int64, data []byte) {
	t.Helper()
	_, err := f.WriteAt(data, offset)
	require.NoError(t, err)
	copy(model[offset:int(offset)+len(data)], data)
}

func TestE2E_ReadWrite(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	var largeFileDigest string
	var largeFileSize int64
	var appendRecords int
	var quotaWrite, mmapSync, elfExec string

	t.Run("large_file_span_blocks_and_chunks_sha256", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		filePath := filepath.Join(mountpoint, "large-66m.bin")
		const totalSize = 66 * 1024 * 1024
		fullBuf := make([]byte, totalSize)
		prng := rand.NewChaCha8([32]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})
		_, err := prng.Read(fullBuf)
		require.NoError(t, err)

		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		written, err := f.Write(fullBuf)
		require.NoError(t, err)
		require.Equal(t, totalSize, written)
		require.NoError(t, f.Close())

		f, err = os.OpenFile(filePath, os.O_WRONLY, 0o644)
		require.NoError(t, err)
		overwriteAt(t, f, fullBuf, 2*1024*1024, []byte("OVERWRITE_AT_2MIB_BLOCK"))
		overwriteAt(t, f, fullBuf, 4*1024*1024-10, []byte("STRADDLE_FIRST_4MIB_CHUNK_BOUNDARY_123456"))
		overwriteAt(t, f, fullBuf, 63*1024*1024+900*1024, bytes.Repeat([]byte("CHUNK0_EDGE_"), 1000))
		overwriteAt(t, f, fullBuf, 64*1024*1024-10, []byte("STRADDLE_CHUNK_BOUNDARY_1234567890"))
		overwriteAt(t, f, fullBuf, 64*1024*1024+500*1024, bytes.Repeat([]byte("CHUNK1_BODY_"), 1000))
		overwriteAt(t, f, fullBuf, totalSize-128, []byte("TAIL_OVERWRITE_AT_66MIB_END_PADDING_BUFFER"))
		require.NoError(t, f.Sync())
		require.NoError(t, f.Close())

		expectedSum := sha256.Sum256(fullBuf)
		readF, err := os.Open(filePath)
		require.NoError(t, err)
		defer readF.Close()
		hasher := sha256.New()
		copied, err := io.Copy(hasher, readF)
		require.NoError(t, err)
		largeFileSize = copied
		require.Equal(t, int64(totalSize), copied)
		require.Equal(t, expectedSum[:], hasher.Sum(nil), "66 MiB 跨块跨分片文件读写 SHA-256 必须 100% 一致")
		largeFileDigest = hex.EncodeToString(expectedSum[:])
	})

	t.Run("concurrent_append_o_append", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		filePath := filepath.Join(mountpoint, "append-test.log")
		initF, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
		require.NoError(t, err)
		require.NoError(t, initF.Close())

		// 2 个并发 O_APPEND 写入者各写一条定长记录：每条记录必须出现且仅出现一次。
		const firstLine = "W00-S0000-0000000000000000000000000000000000000000"
		const secondLine = "W01-S0001-0000000000000000000000000000000000000000"
		first, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0o644)
		require.NoError(t, err)
		second, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0o644)
		require.NoError(t, err)
		errs := make(chan error, 2)
		go func() { _, writeErr := first.WriteString(firstLine + "\n"); errs <- writeErr }()
		go func() { _, writeErr := second.WriteString(secondLine + "\n"); errs <- writeErr }()
		require.NoError(t, errors.Join(<-errs, <-errs))
		require.NoError(t, first.Close())
		require.NoError(t, second.Close())
		content, err := os.ReadFile(filePath)
		require.NoError(t, err)
		records := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
		appendRecords = len(records)
		require.ElementsMatch(t, []string{firstLine, secondLine}, records, "每条记录必须出现且仅出现一次")
	})

	t.Run("quota_enforcement_enospc", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("e2e-disk-quota-%d", time.Now().UnixNano()),
			MaxBytes: 1 * 1024 * 1024,
		}))
		require.NoError(t, err)
		disk := resp.Msg.GetDisk()
		diskID := disk.GetDiskId()
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanCancel()
			_, delErr := client.DeleteDisk(cleanCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
			require.NoError(t, delErr, "清理测试磁盘失败: %s", diskID)
			_, getErr := client.GetDisk(cleanCtx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
			require.Error(t, getErr, "删除磁盘后 GetDisk 必须返回错误: %s", diskID)
			require.Equal(t, connect.CodeNotFound, connect.CodeOf(getErr), "删除磁盘后 GetDisk 应返回 CodeNotFound")
		})

		mountpoint := t.TempDir()
		cmd, _, _ := startBowlMount(t, endpoint, diskID, mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		filePath := filepath.Join(mountpoint, "quota-file.bin")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY, 0o644)
		require.NoError(t, err)
		defer f.Close()

		halfMB := make([]byte, 512*1024)
		n, err := f.Write(halfMB)
		require.NoError(t, err)
		require.Equal(t, len(halfMB), n)

		_, writeErr := f.Write(make([]byte, 1024*1024))
		require.Error(t, writeErr, "超出配额写入必须报错")
		require.ErrorIs(t, writeErr, syscall.ENOSPC, "错误必须为 syscall.ENOSPC，实际: %v", writeErr)
		quotaWrite = errors.Unwrap(writeErr).Error()
	})

	t.Run("mmap_shared_sync_and_persist", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		filePath := filepath.Join(mountpoint, "mmap-test.bin")
		const mmapSize = 8192
		require.NoError(t, os.WriteFile(filePath, bytes.Repeat([]byte{'A'}, mmapSize), 0o644))

		f, err := os.OpenFile(filePath, os.O_RDWR, 0o644)
		require.NoError(t, err)
		mmapData, err := unix.Mmap(int(f.Fd()), 0, mmapSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		require.NoError(t, err)
		copy(mmapData[100:111], "MMAP_MUTATE")
		copy(mmapData[5000:5011], "MMAP_PAGE2_")
		require.NoError(t, unix.Msync(mmapData, unix.MS_SYNC))
		require.NoError(t, unix.Munmap(mmapData))
		require.NoError(t, f.Close())

		persisted, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Len(t, persisted, mmapSize)
		require.Equal(t, []byte("MMAP_MUTATE"), persisted[100:111])
		require.Equal(t, []byte("MMAP_PAGE2_"), persisted[5000:5011])
		mmapSync = string(persisted[100:111]) + "|" + string(persisted[5000:5011])
	})

	t.Run("exec_linux_elf_binary_direct_run", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		binPath := getBowlBin()
		binData, err := os.ReadFile(binPath)
		require.NoError(t, err, "读取 host bowl 二进制失败")

		execPath := filepath.Join(mountpoint, "bowl-exec")
		require.NoError(t, os.WriteFile(execPath, binData, 0o755), "写入挂载点可执行文件失败")
		out, err := exec.Command(execPath, "version").CombinedOutput()
		require.NoError(t, err, "在挂载点执行二进制失败: %s", string(out))
		require.Contains(t, string(out), "bowl version", "输出必须包含 bowl version")
		elfExec = strings.TrimSpace(string(out))
	})

	recordArtifact(t, map[string]string{
		"large_file_size":   strconv.FormatInt(largeFileSize, 10),
		"large_file_sha256": largeFileDigest,
		"append_records":    strconv.Itoa(appendRecords),
		"quota_write":       quotaWrite,
		"mmap_sync":         mmapSync,
		"elf_exec":          elfExec,
	})
}
