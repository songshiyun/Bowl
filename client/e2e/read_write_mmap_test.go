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
	var mmapSync, elfExec string
	var wbBarrierStatSize, wbBarrierDirSize int64
	var wbImmutableErr, raInvalidated string
	var quotaSyncWrite, quotaAsyncFlush, quotaRecover string

	t.Run("大文件跨块覆写并发追加内存映射与可执行加载", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, _ := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

		// 1. 66 MiB 跨块跨分片覆写与 SHA-256 校验
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

		// 2. 并发 O_APPEND 原子追加
		appendPath := filepath.Join(mountpoint, "append-test.log")
		initF, err := os.OpenFile(appendPath, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0o644)
		require.NoError(t, err)
		require.NoError(t, initF.Close())

		const firstLine = "W00-S0000-0000000000000000000000000000000000000000"
		const secondLine = "W01-S0001-0000000000000000000000000000000000000000"
		first, err := os.OpenFile(appendPath, os.O_WRONLY|os.O_APPEND, 0o644)
		require.NoError(t, err)
		second, err := os.OpenFile(appendPath, os.O_WRONLY|os.O_APPEND, 0o644)
		require.NoError(t, err)
		errs := make(chan error, 2)
		go func() { _, writeErr := first.WriteString(firstLine + "\n"); errs <- writeErr }()
		go func() { _, writeErr := second.WriteString(secondLine + "\n"); errs <- writeErr }()
		require.NoError(t, errors.Join(<-errs, <-errs))
		require.NoError(t, first.Close())
		require.NoError(t, second.Close())
		content, err := os.ReadFile(appendPath)
		require.NoError(t, err)
		records := strings.Split(strings.TrimRight(string(content), "\n"), "\n")
		appendRecords = len(records)
		require.ElementsMatch(t, []string{firstLine, secondLine}, records)

		// 3. mmap MAP_SHARED 共享页修改与落盘
		mmapPath := filepath.Join(mountpoint, "mmap-test.bin")
		const mmapSize = 8192
		require.NoError(t, os.WriteFile(mmapPath, bytes.Repeat([]byte{'A'}, mmapSize), 0o644))

		mf, err := os.OpenFile(mmapPath, os.O_RDWR, 0o644)
		require.NoError(t, err)
		mmapData, err := unix.Mmap(int(mf.Fd()), 0, mmapSize, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
		require.NoError(t, err)
		copy(mmapData[100:111], "MMAP_MUTATE")
		copy(mmapData[5000:5011], "MMAP_PAGE2_")
		require.NoError(t, unix.Msync(mmapData, unix.MS_SYNC))
		require.NoError(t, unix.Munmap(mmapData))
		require.NoError(t, mf.Close())

		persisted, err := os.ReadFile(mmapPath)
		require.NoError(t, err)
		require.Len(t, persisted, mmapSize)
		require.Equal(t, []byte("MMAP_MUTATE"), persisted[100:111])
		require.Equal(t, []byte("MMAP_PAGE2_"), persisted[5000:5011])
		mmapSync = string(persisted[100:111]) + "|" + string(persisted[5000:5011])

		// 4. 直接加载执行挂载点上的 Linux ELF 二进制
		binData, err := os.ReadFile(getBowlBin())
		require.NoError(t, err)
		execPath := filepath.Join(mountpoint, "bowl-exec")
		require.NoError(t, os.WriteFile(execPath, binData, 0o755))
		out, err := exec.Command(execPath, "version").CombinedOutput()
		require.NoError(t, err, "在挂载点执行二进制失败: %s", string(out))
		require.Contains(t, string(out), "bowl version")
		elfExec = strings.TrimSpace(string(out))
	})

	t.Run("异步写回可见性屏障不可变标志切换与顺序预读变更即时失效", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, _ := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--direct-io")

		const oneMiB = 1024 * 1024
		chunk1 := bytes.Repeat([]byte{'1'}, oneMiB)
		chunk2 := bytes.Repeat([]byte{'2'}, oneMiB)
		chunk3 := bytes.Repeat([]byte{'3'}, oneMiB)
		chunk4 := bytes.Repeat([]byte{'4'}, oneMiB)
		chunk5 := bytes.Repeat([]byte{'5'}, oneMiB)

		// 1. WriteBehind 未满 4MiB 聚合槽在未调用 Sync/Close 前对 Stat、ReadDirPlus 与新句柄 Read 保持同步可见
		wbDir := filepath.Join(mountpoint, "wb-barrier-dir")
		require.NoError(t, os.Mkdir(wbDir, 0o755))
		wbPath := filepath.Join(wbDir, "staged.bin")
		wf, err := os.OpenFile(wbPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wf.Close() })

		_, err = wf.Write(chunk1)
		require.NoError(t, err)
		_, err = wf.Write(chunk2)
		require.NoError(t, err)
		_, err = wf.Write(chunk3)
		require.NoError(t, err)

		stAfterStage3, err := os.Stat(wbPath)
		require.NoError(t, err)
		wbBarrierStatSize = stAfterStage3.Size()
		require.Equal(t, int64(3*oneMiB), wbBarrierStatSize, "GetAttr 必须冲刷未满聚合槽并返回最新 3MiB 大小")

		_, err = wf.Write(chunk4)
		require.NoError(t, err)
		dirEntries, err := os.ReadDir(wbDir)
		require.NoError(t, err)
		require.Len(t, dirEntries, 1)
		entryInfo, err := dirEntries[0].Info()
		require.NoError(t, err)
		wbBarrierDirSize = entryInfo.Size()
		require.Equal(t, int64(4*oneMiB), wbBarrierDirSize, "ReadDirPlus 必须触发写回屏障返回最新 4MiB 大小")

		_, err = wf.Write(chunk5)
		require.NoError(t, err)
		verifyReader, err := os.Open(wbPath)
		require.NoError(t, err)
		readStage5 := make([]byte, oneMiB)
		nRead5, err := verifyReader.ReadAt(readStage5, 4*oneMiB)
		require.NoError(t, err)
		require.Equal(t, oneMiB, nRead5)
		require.Equal(t, chunk5, readStage5, "新句柄读取未显式 Sync 的聚合槽区间必须返回完整最新数据")
		require.NoError(t, verifyReader.Close())
		require.NoError(t, wf.Close())

		// 2. 激活异步顺序写流后通过 ioctl 置位 FS_IMMUTABLE_FL，后续写入必须同步返回 EPERM
		immPath := filepath.Join(mountpoint, "wb-immutable.bin")
		immFile, err := os.OpenFile(immPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = setIoctlFlags(int(immFile.Fd()), 0)
			_ = immFile.Close()
		})
		_, err = immFile.Write(chunk1)
		require.NoError(t, err)
		_, err = immFile.Write(chunk2)
		require.NoError(t, err)
		require.NoError(t, setIoctlFlags(int(immFile.Fd()), fsImmutableFl))
		_, immWriteErr := immFile.Write(chunk3)
		require.ErrorIs(t, immWriteErr, syscall.EPERM, "激活写回流后置位不可变标志必须迫使后续写入同步返回 EPERM")
		wbImmutableErr = "EPERM"
		require.NoError(t, setIoctlFlags(int(immFile.Fd()), 0))
		require.NoError(t, immFile.Close())

		// 3. ReadAhead 8MiB 顺序预读窗口建立后，并发覆写、打洞与截断必须即时失效预读缓冲
		raPath := filepath.Join(mountpoint, "readahead-inval.bin")
		raInit := bytes.Repeat([]byte("RA_INIT_"), oneMiB) // 8 MiB
		require.NoError(t, os.WriteFile(raPath, raInit, 0o644))

		rf, err := os.Open(raPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = rf.Close() })

		firstProbe := make([]byte, 64*1024)
		_, err = rf.ReadAt(firstProbe, 0)
		require.NoError(t, err)
		require.Equal(t, raInit[:64*1024], firstProbe)

		secondSeq := make([]byte, 128*1024)
		_, err = rf.ReadAt(secondSeq, 64*1024)
		require.NoError(t, err)
		require.Equal(t, raInit[64*1024:192*1024], secondSeq)

		thirdHit := make([]byte, 128*1024)
		_, err = rf.ReadAt(thirdHit, 192*1024)
		require.NoError(t, err)
		require.Equal(t, raInit[192*1024:320*1024], thirdHit)

		mutator, err := os.OpenFile(raPath, os.O_RDWR, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = mutator.Close() })

		patch := []byte("FRESH_OVERWRITE_PATCH_512K")
		_, err = mutator.WriteAt(patch, 512*1024)
		require.NoError(t, err)

		gotPatch := make([]byte, len(patch))
		_, err = rf.ReadAt(gotPatch, 512*1024)
		require.NoError(t, err)
		require.Equal(t, patch, gotPatch, "覆写窗口内偏移后既有只读句柄必须立即读到最新字节而非陈旧预读缓存")

		require.NoError(t, unix.Fallocate(int(mutator.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, oneMiB, 64*1024))
		gotHole := make([]byte, 64*1024)
		_, err = rf.ReadAt(gotHole, oneMiB)
		require.NoError(t, err)
		require.Equal(t, make([]byte, 64*1024), gotHole, "Fallocate 打洞后既有只读句柄必须立即读到全零空洞")
		require.NoError(t, mutator.Close())

		require.NoError(t, os.Truncate(raPath, 768*1024))
		eofProbe := make([]byte, 4096)
		nEOF, eofErr := rf.ReadAt(eofProbe, 768*1024)
		require.Zero(t, nEOF)
		require.ErrorIs(t, eofErr, io.EOF, "Truncate 缩容后既有只读句柄读取新 EOF 处必须立即返回 io.EOF")
		require.NoError(t, rf.Close())
		raInvalidated = "write_punch_truncate_ok"
	})

	t.Run("配额限制直通拦截与异步写回句柄级errseq隔离及截断恢复", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     fmt.Sprintf("e2e-disk-quota-%d", time.Now().UnixNano()),
			MaxBytes: 3 * 1024 * 1024,
		}))
		require.NoError(t, err)
		diskID := resp.Msg.GetDisk().GetDiskId()
		t.Cleanup(func() {
			cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cleanCancel()
			_, delErr := client.DeleteDisk(cleanCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
			require.NoError(t, delErr, "清理配额测试磁盘失败: %s", diskID)
		})

		mountpoint, _ := mountWithCleanup(t, endpoint, diskID, "--direct-io")

		const oneMiB = 1024 * 1024
		block := bytes.Repeat([]byte{'Q'}, oneMiB)
		asyncPath := filepath.Join(mountpoint, "quota-async.bin")

		wFile, err := os.OpenFile(asyncPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = wFile.Close() })

		rFile, err := os.Open(asyncPath)
		require.NoError(t, err)
		t.Cleanup(func() { _ = rFile.Close() })

		// 前 2 MiB 同步探针写入成功并激活流（剩余 1 MiB）；后 4 MiB 填满聚合槽触发异步后台 RPC 超出 3 MiB 配额报错
		_, err = wFile.Write(block)
		require.NoError(t, err)
		_, err = wFile.Write(block)
		require.NoError(t, err)
		_, err = wFile.Write(block)
		require.NoError(t, err)
		_, err = wFile.Write(block)
		require.NoError(t, err)
		_, err = wFile.Write(block)
		require.NoError(t, err)
		_, err = wFile.Write(block)
		require.NoError(t, err)

		require.NoError(t, rFile.Close(), "只读句柄关闭仅等待在途写回屏障，不得消费或报错粘性 ENOSPC")

		syncErr := wFile.Sync()
		require.ErrorIs(t, syncErr, syscall.ENOSPC, "可写句柄 Sync 必须按 errseq 精确兑现后台异步写回 ENOSPC")
		quotaAsyncFlush = "ENOSPC"
		_ = wFile.Close()

		// 盘上剩余 1 MiB，新建文件处于 2 MiB 同步探针期内：第 1 个 1 MiB 写满配额，第 2 个 1 MiB 在 Write 同步返回 ENOSPC
		syncQuotaPath := filepath.Join(mountpoint, "quota-sync.bin")
		sf, err := os.OpenFile(syncQuotaPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = sf.Close() })
		_, err = sf.Write(block)
		require.NoError(t, err)
		_, syncWriteErr := sf.Write(block)
		require.ErrorIs(t, syncWriteErr, syscall.ENOSPC, "同步直通超出配额必须在 Write 立即返回 ENOSPC")
		quotaSyncWrite = errors.Unwrap(syncWriteErr).Error()
		require.NoError(t, sf.Close())

		// 以 O_TRUNC 重新打开释放原 2 MiB 占用后，新句柄写入与 Sync 正常恢复，不被历史 errseq 污染
		recoverF, err := os.OpenFile(asyncPath, os.O_WRONLY|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		_, err = recoverF.Write(block)
		require.NoError(t, err)
		require.NoError(t, recoverF.Sync(), "O_TRUNC 释放空间后新句柄写入与 Sync 必须完全恢复正常")
		require.NoError(t, recoverF.Close())
		quotaRecover = "ok"
	})

	recordArtifact(t, map[string]string{
		"large_file_size":      strconv.FormatInt(largeFileSize, 10),
		"large_file_sha256":    largeFileDigest,
		"append_records":       strconv.Itoa(appendRecords),
		"mmap_sync":            mmapSync,
		"elf_exec":             elfExec,
		"wb_barrier_stat_size": strconv.FormatInt(wbBarrierStatSize, 10),
		"wb_barrier_dir_size":  strconv.FormatInt(wbBarrierDirSize, 10),
		"wb_immutable_switch":  wbImmutableErr,
		"readahead_invalidate": raInvalidated,
		"quota_sync_write":     quotaSyncWrite,
		"quota_async_flush":    quotaAsyncFlush,
		"quota_trunc_recover":  quotaRecover,
	})
}
