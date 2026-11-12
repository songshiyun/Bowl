//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type forkDiskJSON struct {
	DiskID       string `json:"disk_id"`
	ParentDiskID string `json:"parent_disk_id"`
	Name         string `json:"name"`
	MaxBytes     uint64 `json:"max_bytes"`
	UsedBytes    uint64 `json:"used_bytes"`
}

type writeBehindStatsJSON struct {
	StagedCalls      uint64 `json:"staged_calls"`
	StagedBytes      uint64 `json:"staged_bytes"`
	PassthroughCalls uint64 `json:"passthrough_calls"`
	PassthroughBytes uint64 `json:"passthrough_bytes"`
	FlushRPCs        uint64 `json:"flush_rpcs"`
	FlushErrors      uint64 `json:"flush_errors"`
	Inflight         int64  `json:"inflight"`
	PendingSlots     int64  `json:"pending_slots"`
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func unixOpenEINTR(path string, mode int, perm uint32) (int, error) {
	for {
		fd, err := unix.Open(path, mode, perm)
		if !errors.Is(err, syscall.EINTR) {
			return fd, err
		}
	}
}

func unixWriteFullEINTR(fd int, data []byte) error {
	for len(data) > 0 {
		n, err := unix.Write(fd, data)
		if n > 0 {
			data = data[n:]
		}
		if err != nil && !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
	return nil
}

func unixMsyncEINTR(b []byte, flags int) error {
	for {
		err := unix.Msync(b, flags)
		if !errors.Is(err, syscall.EINTR) {
			return err
		}
	}
}

// deterministicPayload 生成指定标签与长度的非全零确定性字节序列。
func deterministicPayload(tag string, size int) []byte {
	out := make([]byte, size)
	var blockIdx uint64
	seedPrefix := []byte(tag + ":")
	for off := 0; off < size; off += sha256.Size {
		var buf [128]byte
		n := copy(buf[:], seedPrefix)
		binary.BigEndian.PutUint64(buf[n:n+8], blockIdx)
		sum := sha256.Sum256(buf[:n+8])
		copy(out[off:], sum[:min(sha256.Size, size-off)])
		blockIdx++
	}
	return out
}

func registerDiskCleanup(t *testing.T, client storagev1connect.StorageServiceClient, diskID string) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			require.NoError(t, err, "清理测试磁盘失败: %s", diskID)
		}
	})
}

func createForkParentDisk(t *testing.T, client storagev1connect.StorageServiceClient, name, sizeStr string) string {
	t.Helper()
	out := runBowlSuccess(t, "disk", "create", "--endpoint", getStorageEndpoint(), "--size", sizeStr, "--name", name, "--json")
	var created forkDiskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &created), "解析 disk create 输出失败: %s", out)
	require.NotEmpty(t, created.DiskID)
	registerDiskCleanup(t, client, created.DiskID)
	return created.DiskID
}

func forkOnlineDisk(t *testing.T, client storagev1connect.StorageServiceClient, sourceDiskID, childName string) forkDiskJSON {
	t.Helper()
	out := runBowlSuccess(t, "disk", "fork", sourceDiskID, "--endpoint", getStorageEndpoint(), "--name", childName, "--json")
	var forked forkDiskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &forked), "解析 disk fork 输出失败: %s", out)
	require.NotEmpty(t, forked.DiskID)
	require.Equal(t, sourceDiskID, forked.ParentDiskID)
	require.Equal(t, childName, forked.Name)
	registerDiskCleanup(t, client, forked.DiskID)
	return forked
}

func resolveAdminAddrForMount(t *testing.T, mountpoint string) string {
	t.Helper()
	metaPath := mountMetaPath(t, mountpoint)
	var adminAddr string
	require.Eventually(t, func() bool {
		raw, err := os.ReadFile(metaPath)
		if err != nil {
			return false
		}
		var meta struct {
			AdminAddr string `json:"admin_addr"`
		}
		if json.Unmarshal(raw, &meta) != nil {
			return false
		}
		adminAddr = meta.AdminAddr
		return adminAddr != "" && adminAddr != "off"
	}, 5*time.Second, 10*time.Millisecond, "读取挂载管理面地址失败: %s", metaPath)
	return adminAddr
}

// readWriteBehindStats 通过本地 HTTP 管理面拉取 WriteBehind 统计，零 FUSE 请求干扰待测聚合槽。
func readWriteBehindStats(t *testing.T, adminAddr string) writeBehindStatsJSON {
	t.Helper()
	code, body := httpGetStatusAndBody(t, "http://"+adminAddr+"/debug/stats")
	require.Equal(t, 200, code)
	var payload struct {
		WriteBehind writeBehindStatsJSON `json:"write_behind"`
	}
	require.NoError(t, json.Unmarshal([]byte(body), &payload), "解析 debug/stats 失败: %s", body)
	return payload.WriteBehind
}

// startIsolatedForkTrigger 在打开待测未刷句柄之前预先派生独立子进程，使其文件描述符表不继承后续打开的 FUSE 句柄，
// 避免当前测试进程直接 fork+exec 时触发 Linux 内核 do_close_on_exec -> filp_close -> FUSE_FLUSH 提前冲刷聚合槽。
func startIsolatedForkTrigger(t *testing.T, client storagev1connect.StorageServiceClient) func(sourceDiskID, childName string) forkDiskJSON {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), mountProcessTimeout)
	t.Cleanup(cancel)

	const pyDriver = `import json, subprocess, sys
for line in sys.stdin:
    req = json.loads(line)
    p = subprocess.run(req["argv"], capture_output=True, text=True)
    sys.stdout.write(json.dumps({"code": p.returncode, "stdout": p.stdout, "stderr": p.stderr}) + "\n")
    sys.stdout.flush()
`
	cmd := exec.CommandContext(ctx, "python3", "-u", "-c", pyDriver)
	stdinPipe, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdoutPipe, err := cmd.StdoutPipe()
	require.NoError(t, err)
	var stderrBuf bytes.Buffer
	cmd.Stderr = &stderrBuf
	require.NoError(t, cmd.Start())
	t.Cleanup(func() {
		_ = stdinPipe.Close()
		_ = cmd.Wait()
	})
	reader := bufio.NewReader(stdoutPipe)

	return func(sourceDiskID, childName string) forkDiskJSON {
		t.Helper()
		argv := []string{
			getBowlBin(), "disk", "fork", sourceDiskID,
			"--endpoint", getStorageEndpoint(),
			"--name", childName,
			"--json",
		}
		reqBytes, mErr := json.Marshal(map[string]any{"argv": argv})
		require.NoError(t, mErr)
		_, wErr := stdinPipe.Write(append(reqBytes, '\n'))
		require.NoError(t, wErr)

		respLine, rErr := reader.ReadBytes('\n')
		require.NoError(t, rErr, "隔离进程执行 bowl disk fork 通信失败: %s", stderrBuf.String())
		var res struct {
			Code   int    `json:"code"`
			Stdout string `json:"stdout"`
			Stderr string `json:"stderr"`
		}
		require.NoError(t, json.Unmarshal(respLine, &res))
		require.Zero(t, res.Code, "bowl disk fork 失败: stdout=%s stderr=%s", res.Stdout, res.Stderr)

		var forked forkDiskJSON
		require.NoError(t, json.Unmarshal([]byte(res.Stdout), &forked), "解析 disk fork JSON 失败: %s", res.Stdout)
		require.NotEmpty(t, forked.DiskID)
		require.Equal(t, sourceDiskID, forked.ParentDiskID)
		require.Equal(t, childName, forked.Name)
		registerDiskCleanup(t, client, forked.DiskID)
		return forked
	}
}

// TestE2E_ForkWorkload_FileModificationRetentionBoundary 在单次在线 bowl disk fork 中并排验证 7 类文件修改状态
// 的精确保留与丢弃边界（fsync、close Flush、O_APPEND 直通、<4MiB 内存聚合槽、>4MiB 自动派发切分、孤儿/O_TMPFILE、mmap+msync）。
func TestE2E_ForkWorkload_FileModificationRetentionBoundary(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	parentID := createForkParentDisk(t, client, "e2e-fork-retention-parent", "2GiB")
	parentMnt, _ := mountWithCleanup(t, endpoint, parentID, "--stats-file")
	adminAddr := resolveAdminAddrForMount(t, parentMnt)

	// 在打开任何挂载点文件前预先启动外部 CLI 触发进程，杜绝后续 fork+exec 经 do_close_on_exec -> FUSE_FLUSH 提前冲刷任何打开句柄，并避免跨 execve 继承 FUSE fd。
	triggerIsolatedFork := startIsolatedForkTrigger(t, client)

	const (
		sz128KiB = 128 * 1024
		sz64KiB  = 64 * 1024
		sz32KiB  = 32 * 1024
		sz256KiB = 256 * 1024
		sz512KiB = 512 * 1024
		sz1MiB   = 1024 * 1024
		sz4MiB   = 4 * 1024 * 1024
	)

	// 1. durable_fsync.bin: O_CREATE|O_RDWR 写入 128 KiB + Sync()，保持 fd 打开跨越 Fork。
	durableData := deterministicPayload("durable-fsync", sz128KiB)
	durableSHA := sha256Hex(durableData)
	durableF, err := os.OpenFile(filepath.Join(parentMnt, "durable_fsync.bin"), os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = durableF.Close() })
	n, err := durableF.Write(durableData)
	require.NoError(t, err)
	require.Equal(t, sz128KiB, n)
	require.NoError(t, durableF.Sync())

	// 2. closed_flush.bin: O_CREATE|O_RDWR 写入 128 KiB，不调 Sync() 直接 Close()（FUSE Flush 排空 WriteBehind）。
	closedData := deterministicPayload("closed-flush", sz128KiB)
	closedSHA := sha256Hex(closedData)
	closedF, err := os.OpenFile(filepath.Join(parentMnt, "closed_flush.bin"), os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	n, err = closedF.Write(closedData)
	require.NoError(t, err)
	require.Equal(t, sz128KiB, n)
	require.NoError(t, closedF.Close())

	// 3. append_direct.log: O_CREATE|O_WRONLY|O_APPEND 逐行写入 64 行（共 64 KiB），不调 Sync() 且保持 fd 打开。
	const appendLineCount = 64
	const appendLineBytes = 1024
	appendF, err := os.OpenFile(filepath.Join(parentMnt, "append_direct.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = appendF.Close() })
	var expectedAppendBuf bytes.Buffer
	expectedAppendLines := make([]string, 0, appendLineCount)
	for lineIdx := range appendLineCount {
		fill := hex.EncodeToString(deterministicPayload("append-line-"+strconv.Itoa(lineIdx), 600))
		prefix := fmt.Sprintf("LINE-%04d-", lineIdx)
		body := (prefix + fill)[:appendLineBytes-1]
		line := body + "\n"
		expectedAppendLines = append(expectedAppendLines, body)
		expectedAppendBuf.WriteString(line)
		written, writeErr := appendF.WriteString(line)
		require.NoError(t, writeErr)
		require.Equal(t, appendLineBytes, written)
	}
	appendSHA := sha256Hex(expectedAppendBuf.Bytes())

	// 6. orphan_and_tmpfile: 保持匿名 O_TMPFILE (256 KiB) 与 open+write(1 MiB)+unlink 孤儿句柄打开。
	// 先于第 4、5 步执行 os.Remove，避免目录解链操作与后续未刷盘内存聚合槽产生交叉干扰。
	tmpFD, err := unixOpenEINTR(parentMnt, unix.O_TMPFILE|unix.O_RDWR|unix.O_CLOEXEC, 0o600)
	require.NoError(t, err)
	tmpClosed := false
	t.Cleanup(func() {
		if !tmpClosed {
			_ = unix.Close(tmpFD)
		}
	})
	tmpData := deterministicPayload("anon-tmpfile", sz256KiB)
	require.NoError(t, unixWriteFullEINTR(tmpFD, tmpData))

	orphanPath := filepath.Join(parentMnt, "orphan_unlinked.bin")
	orphanF, err := os.OpenFile(orphanPath, os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = orphanF.Close() })
	orphanData := deterministicPayload("open-unlinked-orphan", sz1MiB)
	n, err = orphanF.Write(orphanData)
	require.NoError(t, err)
	require.Equal(t, sz1MiB, n)
	require.NoError(t, os.Remove(orphanPath))

	// 7. mmap_msync.bin: 预建 64 KiB v1 文件，MAP_SHARED 覆写为 v2 并调用 Msync(MS_SYNC)，保持映射与 fd 打开。
	mmapPath := filepath.Join(parentMnt, "mmap_msync.bin")
	mmapV1 := deterministicPayload("mmap-v1", sz64KiB)
	mmapV2 := deterministicPayload("mmap-v2", sz64KiB)
	mmapV1SHA := sha256Hex(mmapV1)
	mmapV2SHA := sha256Hex(mmapV2)
	require.NotEqual(t, mmapV1SHA, mmapV2SHA)
	require.NoError(t, os.WriteFile(mmapPath, mmapV1, 0o644))

	mmapF, err := os.OpenFile(mmapPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mmapF.Close() })
	mmapSlice, err := unix.Mmap(int(mmapF.Fd()), 0, sz64KiB, unix.PROT_READ|unix.PROT_WRITE, unix.MAP_SHARED)
	require.NoError(t, err)
	mmapUnmapped := false
	t.Cleanup(func() {
		if !mmapUnmapped {
			_ = unix.Munmap(mmapSlice)
		}
	})
	copy(mmapSlice, mmapV2)
	require.NoError(t, unixMsyncEINTR(mmapSlice, unix.MS_SYNC))

	// 确认此刻父盘服务端已用容量包含 1 MiB 打开态孤儿（128KiB + 128KiB + 64KiB + 1MiB + 64KiB = 1,441,792 B）。
	getCtx, getCancel := context.WithTimeout(context.Background(), 5*time.Second)
	parentBeforeStage, err := client.GetDisk(getCtx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: parentID}))
	getCancel()
	require.NoError(t, err)
	require.Equal(t, uint64(sz128KiB+sz128KiB+sz64KiB+sz1MiB+sz64KiB), parentBeforeStage.Msg.GetDisk().GetUsedBytes())

	wbBefore := readWriteBehindStats(t, adminAddr)
	require.Zero(t, wbBefore.Inflight)
	require.Zero(t, wbBefore.PendingSlots)

	// 4. staged_unflushed.bin: O_CREATE|O_RDWR 写入 8 x 32 KiB = 256 KiB (< 4 MiB)，不调 Sync()/Close()，滞留客户端 stageBuf。
	stagedData := deterministicPayload("staged-unflushed", sz256KiB)
	stagedSHA := sha256Hex(stagedData)
	stagedF, err := os.OpenFile(filepath.Join(parentMnt, "staged_unflushed.bin"), os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	stagedClosed := false
	t.Cleanup(func() {
		if !stagedClosed {
			_ = stagedF.Close()
		}
	})
	for off := 0; off < sz256KiB; off += sz32KiB {
		written, writeErr := stagedF.Write(stagedData[off : off+sz32KiB])
		require.NoError(t, writeErr)
		require.Equal(t, sz32KiB, written)
	}

	// 5. auto_flushed_4mib.bin: O_CREATE|O_RDWR 写入 144 x 32 KiB = 4 MiB + 512 KiB，不调 Sync()/Close()。
	// 前 4 MiB 填满槽位自动异步派发至服务端，尾部 512 KiB 滞留新的客户端 stageBuf。
	const autoTotalBytes = sz4MiB + sz512KiB
	autoData := deterministicPayload("auto-flushed-4m512k", autoTotalBytes)
	autoPrefix4MiBSHA := sha256Hex(autoData[:sz4MiB])
	autoFullSHA := sha256Hex(autoData)
	autoF, err := os.OpenFile(filepath.Join(parentMnt, "auto_flushed_4mib.bin"), os.O_CREATE|os.O_RDWR, 0o644)
	require.NoError(t, err)
	autoClosed := false
	t.Cleanup(func() {
		if !autoClosed {
			_ = autoF.Close()
		}
	})
	for off := 0; off < autoTotalBytes; off += sz32KiB {
		written, writeErr := autoF.Write(autoData[off : off+sz32KiB])
		require.NoError(t, writeErr)
		require.Equal(t, sz32KiB, written)
	}

	// 通过本地管理面与隐藏虚拟 .stats 双重确认：4 MiB 自动派发 RPC 已落达 StoreActor（inflight == 0），
	// 同时两个未满聚合槽（staged_unflushed 256 KiB + auto_flushed 尾部 512 KiB）仍驻留内存（pending_slots == 2）。
	require.Eventually(t, func() bool {
		wb := readWriteBehindStats(t, adminAddr)
		return wb.FlushRPCs > wbBefore.FlushRPCs && wb.Inflight == 0 && wb.PendingSlots == 2
	}, 5*time.Second, 5*time.Millisecond, "等待 4 MiB 自动派发完成且尾部缓冲驻留超时")

	virtualStatsRaw, err := os.ReadFile(filepath.Join(parentMnt, ".stats"))
	require.NoError(t, err)
	require.Contains(t, string(virtualStatsRaw), "bowl_client_write_behind_inflight 0\n")
	require.Contains(t, string(virtualStatsRaw), "bowl_client_write_behind_pending_slots 2\n")

	// 在所有句柄保持打开、未刷缓冲驻留内存的瞬间，由外部独立进程触发在线 bowl disk fork。
	childDisk := triggerIsolatedFork(parentID, "e2e-fork-retention-child")
	wbAfterFork := readWriteBehindStats(t, adminAddr)
	require.Equal(t, int64(2), wbAfterFork.PendingSlots, "在线 Fork 全程不得冲刷父客户端未满 stageBuf")
	const expectedChildUsedBytes = uint64(sz128KiB + sz128KiB + sz64KiB + 0 + sz4MiB + sz64KiB)
	require.Equal(t, expectedChildUsedBytes, childDisk.UsedBytes,
		"子盘 used_bytes 必须精确等于已落服务端存活文件总和（排除 1 MiB 孤儿与客户端内存缓冲）")

	// 以 --direct-io 挂载子盘，逐项核验 7 类状态的保留与丢弃结果。
	childMnt, _ := mountWithCleanup(t, endpoint, childDisk.DiskID, "--direct-io")

	dirEntries, err := os.ReadDir(childMnt)
	require.NoError(t, err)
	gotNames := make([]string, 0, len(dirEntries))
	for _, entry := range dirEntries {
		gotNames = append(gotNames, entry.Name())
	}
	slices.Sort(gotNames)
	expectedNames := []string{
		"append_direct.log",
		"auto_flushed_4mib.bin",
		"closed_flush.bin",
		"durable_fsync.bin",
		"mmap_msync.bin",
		"staged_unflushed.bin",
	}
	require.Equal(t, expectedNames, gotNames, "子盘目录不得残留孤儿文件或匿名临时文件")

	// 核验 1: durable_fsync.bin 完整保留 128 KiB。
	gotDurable, err := os.ReadFile(filepath.Join(childMnt, "durable_fsync.bin"))
	require.NoError(t, err)
	require.Len(t, gotDurable, sz128KiB)
	require.Equal(t, durableSHA, sha256Hex(gotDurable))

	// 核验 2: closed_flush.bin 完整保留 128 KiB。
	gotClosed, err := os.ReadFile(filepath.Join(childMnt, "closed_flush.bin"))
	require.NoError(t, err)
	require.Len(t, gotClosed, sz128KiB)
	require.Equal(t, closedSHA, sha256Hex(gotClosed))

	// 核验 3: append_direct.log 完整保留全部 64 行（64 KiB）。
	gotAppend, err := os.ReadFile(filepath.Join(childMnt, "append_direct.log"))
	require.NoError(t, err)
	require.Len(t, gotAppend, sz64KiB)
	require.Equal(t, appendSHA, sha256Hex(gotAppend))
	gotLines := strings.Split(strings.TrimSuffix(string(gotAppend), "\n"), "\n")
	require.Equal(t, expectedAppendLines, gotLines)

	// 核验 4: staged_unflushed.bin 文件存在但大小为精确 0 B。
	stagedChildInfo, err := os.Stat(filepath.Join(childMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Zero(t, stagedChildInfo.Size(), "未满 4 MiB 且未 Flush/Sync 的新建文件在子盘必须为 0 B")
	gotStagedChild, err := os.ReadFile(filepath.Join(childMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Empty(t, gotStagedChild)

	// 核验 5: auto_flushed_4mib.bin 精确保留前 4 MiB，尾部 512 KiB 为 0 B。
	gotAutoChild, err := os.ReadFile(filepath.Join(childMnt, "auto_flushed_4mib.bin"))
	require.NoError(t, err)
	require.Len(t, gotAutoChild, sz4MiB, "超过 4 MiB 未 Sync 文件在子盘必须仅保留已自动派发的 4 MiB 整块")
	require.Equal(t, autoPrefix4MiBSHA, sha256Hex(gotAutoChild))

	// 核验 6: orphan_unlinked.bin 在子盘不存在。
	_, err = os.Stat(filepath.Join(childMnt, "orphan_unlinked.bin"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// 核验 7: mmap_msync.bin 精确保留 v2 覆写字节。
	gotMmapChild, err := os.ReadFile(filepath.Join(childMnt, "mmap_msync.bin"))
	require.NoError(t, err)
	require.Len(t, gotMmapChild, sz64KiB)
	require.Equal(t, mmapV2SHA, sha256Hex(gotMmapChild))

	// Fork 完成后关闭父盘未刷句柄，验证父盘冲刷出完整 256 KiB 与 4.5 MiB，而子盘依然保持 0 B 与 4 MiB。
	require.NoError(t, stagedF.Close())
	stagedClosed = true
	require.NoError(t, autoF.Close())
	autoClosed = true

	gotStagedParent, err := os.ReadFile(filepath.Join(parentMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Len(t, gotStagedParent, sz256KiB)
	require.Equal(t, stagedSHA, sha256Hex(gotStagedParent))

	gotAutoParent, err := os.ReadFile(filepath.Join(parentMnt, "auto_flushed_4mib.bin"))
	require.NoError(t, err)
	require.Len(t, gotAutoParent, autoTotalBytes)
	require.Equal(t, autoFullSHA, sha256Hex(gotAutoParent))

	gotStagedChildAfter, err := os.ReadFile(filepath.Join(childMnt, "staged_unflushed.bin"))
	require.NoError(t, err)
	require.Empty(t, gotStagedChildAfter, "父盘 Close 冲刷聚合槽不得穿透篡改已派生子盘")

	gotAutoChildAfter, err := os.ReadFile(filepath.Join(childMnt, "auto_flushed_4mib.bin"))
	require.NoError(t, err)
	require.Len(t, gotAutoChildAfter, sz4MiB)
	require.Equal(t, autoPrefix4MiBSHA, sha256Hex(gotAutoChildAfter))

	require.NoError(t, unix.Munmap(mmapSlice))
	mmapUnmapped = true
	require.NoError(t, unix.Close(tmpFD))
	tmpClosed = true

	recordArtifact(t, map[string]string{
		"durable_fsync_child_bytes":       strconv.Itoa(len(gotDurable)),
		"closed_flush_child_bytes":        strconv.Itoa(len(gotClosed)),
		"append_direct_child_lines":       strconv.Itoa(len(gotLines)),
		"append_direct_child_bytes":       strconv.Itoa(len(gotAppend)),
		"staged_unflushed_child_bytes":    strconv.Itoa(len(gotStagedChildAfter)),
		"staged_unflushed_parent_bytes":   strconv.Itoa(len(gotStagedParent)),
		"auto_flushed_child_bytes":        strconv.Itoa(len(gotAutoChildAfter)),
		"auto_flushed_parent_bytes":       strconv.Itoa(len(gotAutoParent)),
		"orphan_and_tmpfile_child_leaked": "false",
		"child_used_bytes":                strconv.FormatUint(childDisk.UsedBytes, 10),
		"mmap_msync_child_sha256":         mmapV2SHA,
	})
}

const (
	slotMagicStr  = "BOWLSLT1"
	slotCount     = 64
	slotSizeBytes = 64 * 1024
)

func encodeSlotBlock(slotID, version uint64) []byte {
	buf := make([]byte, slotSizeBytes)
	copy(buf[0:8], slotMagicStr)
	binary.BigEndian.PutUint64(buf[8:16], slotID)
	binary.BigEndian.PutUint64(buf[16:24], version)

	var seed [24]byte
	copy(seed[0:8], "SLOTPAYL")
	binary.BigEndian.PutUint64(seed[8:16], slotID)
	binary.BigEndian.PutUint64(seed[16:24], version)
	mask := sha256.Sum256(seed[:])

	payload := buf[24 : slotSizeBytes-sha256.Size]
	for i := range payload {
		payload[i] = mask[i%sha256.Size] ^ byte((i*31+int(slotID)*7+int(version))&0xff)
	}
	digest := sha256.Sum256(buf[:slotSizeBytes-sha256.Size])
	copy(buf[slotSizeBytes-sha256.Size:], digest[:])
	return buf
}

func verifyAndDecodeSlotBlock(t *testing.T, raw []byte, expectedSlotID uint64) uint64 {
	t.Helper()
	require.Len(t, raw, slotSizeBytes)
	require.Equal(t, slotMagicStr, string(raw[0:8]), "槽位 %d 魔数损坏", expectedSlotID)
	gotSlotID := binary.BigEndian.Uint64(raw[8:16])
	require.Equal(t, expectedSlotID, gotSlotID, "槽位编号不匹配")
	version := binary.BigEndian.Uint64(raw[16:24])

	wantDigest := sha256.Sum256(raw[:slotSizeBytes-sha256.Size])
	require.Equal(t, wantDigest[:], raw[slotSizeBytes-sha256.Size:],
		"槽位 %d (version=%d) 内部 SHA-256 撕裂校验失败", expectedSlotID, version)
	require.Equal(t, encodeSlotBlock(expectedSlotID, version), raw,
		"槽位 %d (version=%d) 载荷与确定性模板不一致", expectedSlotID, version)
	return version
}

type atomicConfigDoc struct {
	Seq     uint64 `json:"seq"`
	Padding string `json:"padding"`
	SHA256  string `json:"sha256"`
}

func buildAtomicConfigBytes(seq uint64) ([]byte, error) {
	pad := hex.EncodeToString(deterministicPayload("cfg-seq-"+strconv.FormatUint(seq, 10), 16*1024))
	digest := sha256Hex([]byte(strconv.FormatUint(seq, 10) + ":" + pad))
	doc := atomicConfigDoc{
		Seq:     seq,
		Padding: pad,
		SHA256:  digest,
	}
	raw, err := json.Marshal(doc)
	if err != nil {
		return nil, err
	}
	return append(raw, '\n'), nil
}

func parseAndVerifyAtomicConfig(t *testing.T, raw []byte) uint64 {
	t.Helper()
	var doc atomicConfigDoc
	require.NoError(t, json.Unmarshal(raw, &doc), "config.json 解析失败（出现半写或损坏）")
	require.Len(t, doc.Padding, 32*1024)
	wantDigest := sha256Hex([]byte(strconv.FormatUint(doc.Seq, 10) + ":" + doc.Padding))
	require.Equal(t, wantDigest, doc.SHA256, "config.json 内部摘要不匹配 (seq=%d)", doc.Seq)
	return doc.Seq
}

// TestE2E_ForkWorkload_ConcurrentInPlaceMutationAndAtomicRename 在高频跨 16 个 256KiB 块的原地 WriteAt 突变
// 及 write(.tmp)+fsync+rename 原子替换风暴持续进行期间，连续触发 3 次在线 bowl disk fork，验证零撕裂、持久水位单调与写时复制隔离。
func TestE2E_ForkWorkload_ConcurrentInPlaceMutationAndAtomicRename(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	parentID := createForkParentDisk(t, client, "e2e-fork-mutator-parent", "2GiB")
	parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
	// 在启动后台突变协程与打开挂载点文件前预先启动外部 CLI 触发进程，防止同进程 exec 触发 do_close_on_exec -> FUSE_FLUSH 提前冲刷 slots.bin / config.json.tmp。
	triggerIsolatedFork := startIsolatedForkTrigger(t, client)

	slotsPath := filepath.Join(parentMnt, "slots.bin")
	cfgPath := filepath.Join(parentMnt, "config.json")

	// 初始化 4 MiB (64 x 64 KiB) 槽位基准文件与 seq=0 的 config.json。
	seedF, err := os.OpenFile(slotsPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	for i := range uint64(slotCount) {
		_, writeErr := seedF.WriteAt(encodeSlotBlock(i, 0), int64(i)*slotSizeBytes)
		require.NoError(t, writeErr)
	}
	require.NoError(t, seedF.Sync())
	require.NoError(t, seedF.Close())

	initCfgBytes, err := buildAtomicConfigBytes(0)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(cfgPath, initCfgBytes, 0o644))

	var (
		snapMu           sync.Mutex
		durableVersions  [slotCount]uint64
		syncRounds       atomic.Uint64
		durableConfigSeq atomic.Uint64
		stopCh           = make(chan struct{})
		mutatorErrCh     = make(chan error, 1)
		renameErrCh      = make(chan error, 1)
	)

	// 后台协程 1：跨 16 个 256KiB BlobStore 块持续原地覆写 64KiB 槽位并周期性 Sync。
	go func() {
		f, openErr := os.OpenFile(slotsPath, os.O_RDWR, 0o644)
		if openErr != nil {
			mutatorErrCh <- openErr
			return
		}
		defer func() { _ = f.Close() }()

		var stagedVersions [slotCount]uint64
		var step uint64
		for {
			select {
			case <-stopCh:
				if syncErr := f.Sync(); syncErr != nil {
					mutatorErrCh <- syncErr
					return
				}
				snapMu.Lock()
				durableVersions = stagedVersions
				snapMu.Unlock()
				mutatorErrCh <- nil
				return
			default:
			}

			slotIdx := (step*17 + step/slotCount) % slotCount
			stagedVersions[slotIdx]++
			ver := stagedVersions[slotIdx]
			block := encodeSlotBlock(slotIdx, ver)
			if _, wErr := f.WriteAt(block, int64(slotIdx)*slotSizeBytes); wErr != nil {
				mutatorErrCh <- wErr
				return
			}
			step++
			if step%16 == 0 {
				if syncErr := f.Sync(); syncErr != nil {
					mutatorErrCh <- syncErr
					return
				}
				snapMu.Lock()
				durableVersions = stagedVersions
				snapMu.Unlock()
				syncRounds.Add(1)
			}
		}
	}()

	// 后台协程 2：持续执行 config.json.tmp -> Write -> Sync -> Close -> Rename 原子更新风暴。
	go func() {
		var seq uint64
		tmpPath := cfgPath + ".tmp"
		for {
			select {
			case <-stopCh:
				renameErrCh <- nil
				return
			default:
			}
			seq++
			payload, mErr := buildAtomicConfigBytes(seq)
			if mErr != nil {
				renameErrCh <- mErr
				return
			}
			tf, openErr := os.OpenFile(tmpPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
			if openErr != nil {
				renameErrCh <- openErr
				return
			}
			if _, wErr := tf.Write(payload); wErr != nil {
				_ = tf.Close()
				renameErrCh <- wErr
				return
			}
			if sErr := tf.Sync(); sErr != nil {
				_ = tf.Close()
				renameErrCh <- sErr
				return
			}
			if cErr := tf.Close(); cErr != nil {
				renameErrCh <- cErr
				return
			}
			if rErr := os.Rename(tmpPath, cfgPath); rErr != nil {
				renameErrCh <- rErr
				return
			}
			durableConfigSeq.Store(seq)
		}
	}()

	const forkIterations = 3
	type forkSnapshot struct {
		child      forkDiskJSON
		minSlotVer [slotCount]uint64
		minCfgSeq  uint64
	}
	snapshots := make([]forkSnapshot, 0, forkIterations)

	var prevRound, prevCfgSeq uint64
	for i := range forkIterations {
		require.Eventually(t, func() bool {
			return syncRounds.Load() >= prevRound+3 && durableConfigSeq.Load() >= prevCfgSeq+4
		}, 15*time.Second, 10*time.Millisecond, "等待第 %d 次 Fork 前后台突变推进超时", i+1)

		var minVers [slotCount]uint64
		snapMu.Lock()
		minVers = durableVersions
		snapMu.Unlock()
		minSeq := durableConfigSeq.Load()

		child := triggerIsolatedFork(parentID, fmt.Sprintf("e2e-fork-mutator-child-%d", i+1))
		snapshots = append(snapshots, forkSnapshot{
			child:      child,
			minSlotVer: minVers,
			minCfgSeq:  minSeq,
		})
		prevRound = syncRounds.Load()
		prevCfgSeq = durableConfigSeq.Load()
	}

	close(stopCh)
	require.NoError(t, <-mutatorErrCh, "后台槽位突变协程报错")
	require.NoError(t, <-renameErrCh, "后台原子重命名协程报错")

	var finalParentVers [slotCount]uint64
	snapMu.Lock()
	finalParentVers = durableVersions
	snapMu.Unlock()
	finalParentCfgSeq := durableConfigSeq.Load()

	// 逐一挂载 3 个在线派生的子盘，核验 64 个槽位零撕裂、水位不回退、config.json 完整以及子盘覆写与父盘隔离。
	for idx, snap := range snapshots {
		childMnt, stopChild := mountWithCleanup(t, endpoint, snap.child.DiskID, "--direct-io")

		childSlotsRaw, readErr := os.ReadFile(filepath.Join(childMnt, "slots.bin"))
		require.NoError(t, readErr)
		require.Len(t, childSlotsRaw, slotCount*slotSizeBytes)

		var childVers [slotCount]uint64
		for slotIdx := range uint64(slotCount) {
			off := slotIdx * slotSizeBytes
			ver := verifyAndDecodeSlotBlock(t, childSlotsRaw[off:off+slotSizeBytes], slotIdx)
			require.GreaterOrEqual(t, ver, snap.minSlotVer[slotIdx],
				"子盘 #%d 槽位 %d 版本号不得低于 Fork 前已 Sync 水位", idx+1, slotIdx)
			childVers[slotIdx] = ver
		}

		childCfgRaw, readErr := os.ReadFile(filepath.Join(childMnt, "config.json"))
		require.NoError(t, readErr)
		gotCfgSeq := parseAndVerifyAtomicConfig(t, childCfgRaw)
		require.GreaterOrEqual(t, gotCfgSeq, snap.minCfgSeq,
			"子盘 #%d config.json 序号不得低于 Fork 前已 Rename 水位", idx+1)
		t.Logf("子盘 #%d 校验通过: slot[0]_ver=%d (min=%d), config_seq=%d (min=%d)",
			idx+1, childVers[0], snap.minSlotVer[0], gotCfgSeq, snap.minCfgSeq)

		// 在子盘上原地覆写部分槽位并 Sync，验证同一 256 KiB 物理块内相邻槽位完好且不污染父盘。
		childF, openErr := os.OpenFile(filepath.Join(childMnt, "slots.bin"), os.O_RDWR, 0o644)
		require.NoError(t, openErr)
		mutatedTargets := []uint64{0, 15, 32, 63}
		for _, targetSlot := range mutatedTargets {
			newVer := childVers[targetSlot] + 1_000_000 + uint64(idx)
			_, wErr := childF.WriteAt(encodeSlotBlock(targetSlot, newVer), int64(targetSlot)*slotSizeBytes)
			require.NoError(t, wErr)
			childVers[targetSlot] = newVer
		}
		require.NoError(t, childF.Sync())
		require.NoError(t, childF.Close())

		afterChildMutRaw, readErr := os.ReadFile(filepath.Join(childMnt, "slots.bin"))
		require.NoError(t, readErr)
		for slotIdx := range uint64(slotCount) {
			off := slotIdx * slotSizeBytes
			gotVer := verifyAndDecodeSlotBlock(t, afterChildMutRaw[off:off+slotSizeBytes], slotIdx)
			require.Equal(t, childVers[slotIdx], gotVer)
		}
		stopChild()
	}

	// 核验父盘在经历 3 次在线 Fork 与所有子盘覆写后，全部 64 个槽位与 config.json 仍精确匹配父盘最终提交态。
	parentSlotsRaw, err := os.ReadFile(slotsPath)
	require.NoError(t, err)
	require.Len(t, parentSlotsRaw, slotCount*slotSizeBytes)
	for slotIdx := range uint64(slotCount) {
		off := slotIdx * slotSizeBytes
		gotVer := verifyAndDecodeSlotBlock(t, parentSlotsRaw[off:off+slotSizeBytes], slotIdx)
		require.Equal(t, finalParentVers[slotIdx], gotVer)
	}
	parentCfgRaw, err := os.ReadFile(cfgPath)
	require.NoError(t, err)
	require.Equal(t, finalParentCfgSeq, parseAndVerifyAtomicConfig(t, parentCfgRaw))

	recordArtifact(t, map[string]string{
		"slot_count":                strconv.Itoa(slotCount),
		"fork_children_verified":    strconv.Itoa(forkIterations),
		"zero_torn_slots":           "true",
		"atomic_rename_intact":      "true",
		"parent_cow_block_isolated": "true",
	})
}

const sqliteWorkloadHelperPy = `import hashlib
import json
import os
import select
import sqlite3
import sys


def row_payload(tag: str, seq: int, width: int) -> tuple[str, str]:
    base = f"{tag}|seq={seq:08d}|"
    seed = hashlib.sha256(base.encode("utf-8")).hexdigest()
    repeat = (width // len(seed)) + 2
    body = (base + (seed * repeat))[:width]
    digest = hashlib.sha256(body.encode("utf-8")).hexdigest()
    return body, digest


def mode_wal_loop(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA page_size = 4096;")
    conn.execute("PRAGMA journal_mode = WAL;")
    conn.execute("PRAGMA synchronous = FULL;")
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.execute("CREATE TABLE groups (gid INTEGER PRIMARY KEY, label TEXT NOT NULL);")
    conn.execute("INSERT INTO groups (gid, label) VALUES (1, 'core');")
    conn.execute(
        "CREATE TABLE events ("
        "seq INTEGER PRIMARY KEY, "
        "gid INTEGER NOT NULL REFERENCES groups(gid), "
        "payload TEXT NOT NULL, "
        "sha256 TEXT NOT NULL);"
    )
    conn.execute("CREATE INDEX idx_events_sha256 ON events(sha256);")
    conn.execute("CREATE TABLE counters (id INTEGER PRIMARY KEY CHECK (id = 1), total_events INTEGER NOT NULL);")
    conn.execute("INSERT INTO counters (id, total_events) VALUES (1, 0);")

    seq = 0
    while True:
        rlist, _, _ = select.select([sys.stdin], [], [], 0.0)
        if rlist:
            line = sys.stdin.readline()
            if not line or line.strip() == "STOP":
                break
        seq += 1
        payload, digest = row_payload("wal-acid", seq, 512)
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute(
            "INSERT INTO events (seq, gid, payload, sha256) VALUES (?, 1, ?, ?);",
            (seq, payload, digest),
        )
        conn.execute("UPDATE counters SET total_events = ? WHERE id = 1;", (seq,))
        conn.execute("COMMIT;")
        sys.stdout.write(f"ACK {seq}\n")
        sys.stdout.flush()

    conn.close()
    sys.stdout.write(f"FINAL {seq}\n")
    sys.stdout.flush()


def mode_verify_wal_child(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA foreign_keys = ON;")
    ic = [r[0] for r in conn.execute("PRAGMA integrity_check;").fetchall()]
    fk = conn.execute("PRAGMA foreign_key_check;").fetchall()
    cnt, min_seq, max_seq = conn.execute(
        "SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM events;"
    ).fetchone()
    total_events = conn.execute("SELECT total_events FROM counters WHERE id = 1;").fetchone()[0]

    bad_rows = 0
    prev = 0
    for seq, payload, got_sha in conn.execute("SELECT seq, payload, sha256 FROM events ORDER BY seq ASC;"):
        if seq != prev + 1:
            bad_rows += 1
        prev = seq
        want_sha = hashlib.sha256(payload.encode("utf-8")).hexdigest()
        if got_sha != want_sha:
            bad_rows += 1

    next_seq = max_seq + 1
    c_payload, c_sha = row_payload("child-wal-write", next_seq, 512)
    conn.execute("BEGIN IMMEDIATE;")
    conn.execute("INSERT INTO events (seq, gid, payload, sha256) VALUES (?, 1, ?, ?);", (next_seq, c_payload, c_sha))
    conn.execute("UPDATE counters SET total_events = ? WHERE id = 1;", (next_seq,))
    conn.execute("COMMIT;")
    ckpt = conn.execute("PRAGMA wal_checkpoint(TRUNCATE);").fetchone()
    after_cnt = conn.execute("SELECT COUNT(*) FROM events;").fetchone()[0]
    ic_after = [r[0] for r in conn.execute("PRAGMA integrity_check;").fetchall()]
    conn.close()

    print(json.dumps({
        "integrity_check": ic,
        "fk_violations": len(fk),
        "count": cnt,
        "min_seq": min_seq,
        "max_seq": max_seq,
        "counter_total": total_events,
        "bad_rows": bad_rows,
        "after_insert_count": after_cnt,
        "checkpoint_busy": ckpt[0],
        "integrity_after": ic_after,
    }))


def mode_delete_hot_journal(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA page_size = 4096;")
    jm = conn.execute("PRAGMA journal_mode = DELETE;").fetchone()[0]
    assert jm.lower() == "delete", f"unexpected journal_mode={jm}"
    conn.execute("PRAGMA synchronous = FULL;")
    conn.execute("CREATE TABLE records (seq INTEGER PRIMARY KEY, body TEXT NOT NULL, sha256 TEXT NOT NULL);")
    conn.execute("CREATE INDEX idx_records_sha256 ON records(sha256);")

    conn.execute("BEGIN IMMEDIATE;")
    for seq in range(1, 101):
        body, digest = row_payload("committed-delete", seq, 1024)
        conn.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, body, digest))
    conn.execute("COMMIT;")

    # 缩小页缓存至 64 KiB，迫使未提交大事务将原页刷入 db-journal (fsync) 并将脏页覆写进主库文件。
    conn.execute("PRAGMA cache_size = -64;")
    conn.execute("BEGIN IMMEDIATE;")
    for seq in range(1, 101):
        dirty_body, dirty_sha = row_payload("UNCOMMITTED_DIRTY_OVERWRITE", seq, 1024)
        conn.execute("UPDATE records SET body = ?, sha256 = ? WHERE seq = ?;", (dirty_body, dirty_sha, seq))
    for seq in range(101, 2601):
        dirty_body, dirty_sha = row_payload("UNCOMMITTED_DIRTY_INSERT", seq, 1024)
        conn.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, dirty_body, dirty_sha))

    journal_path = db_path + "-journal"
    j_size = os.path.getsize(journal_path) if os.path.exists(journal_path) else 0
    db_size = os.path.getsize(db_path)
    sys.stdout.write(f"READY_MID_DELETE_TX {j_size} {db_size}\n")
    sys.stdout.flush()

    cmd = sys.stdin.readline().strip()
    assert cmd == "COMMIT_NOW", f"unexpected cmd={cmd}"
    conn.execute("COMMIT;")
    final_cnt = conn.execute("SELECT COUNT(*) FROM records;").fetchone()[0]
    conn.close()
    sys.stdout.write(f"PARENT_COMMITTED {final_cnt}\n")
    sys.stdout.flush()


def mode_wal_uncommitted_spill(db_path: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn.execute("PRAGMA page_size = 4096;")
    jm = conn.execute("PRAGMA journal_mode = WAL;").fetchone()[0]
    assert jm.lower() == "wal", f"unexpected journal_mode={jm}"
    conn.execute("PRAGMA synchronous = FULL;")
    conn.execute("CREATE TABLE records (seq INTEGER PRIMARY KEY, body TEXT NOT NULL, sha256 TEXT NOT NULL);")
    conn.execute("CREATE INDEX idx_records_sha256 ON records(sha256);")

    # 前 50 条落入主库文件，51..100 作为已提交帧保留在 WAL 头部。
    conn.execute("BEGIN IMMEDIATE;")
    for seq in range(1, 51):
        body, digest = row_payload("committed-wal", seq, 1024)
        conn.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, body, digest))
    conn.execute("COMMIT;")
    conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")

    conn.execute("BEGIN IMMEDIATE;")
    for seq in range(51, 101):
        body, digest = row_payload("committed-wal", seq, 1024)
        conn.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, body, digest))
    conn.execute("COMMIT;")

    # 缩小缓存至 64 KiB 并写入 2500 条 (~5 MiB) 未提交记录，迫使 >4 MiB 未提交 WAL 帧自动派发至服务端。
    conn.execute("PRAGMA cache_size = -64;")
    conn.execute("BEGIN IMMEDIATE;")
    for seq in range(101, 2601):
        dirty_body, dirty_sha = row_payload("UNCOMMITTED_WAL_SPILL", seq, 2048)
        conn.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, dirty_body, dirty_sha))

    wal_path = db_path + "-wal"
    w_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
    sys.stdout.write(f"READY_MID_WAL_TX {w_size}\n")
    sys.stdout.flush()

    cmd = sys.stdin.readline().strip()
    assert cmd == "COMMIT_NOW", f"unexpected cmd={cmd}"
    conn.execute("COMMIT;")
    final_cnt = conn.execute("SELECT COUNT(*) FROM records;").fetchone()[0]
    conn.close()
    sys.stdout.write(f"PARENT_COMMITTED {final_cnt}\n")
    sys.stdout.flush()


def mode_wal_sync_off(db_path: str) -> None:
    conn1 = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn1.execute("PRAGMA page_size = 4096;")
    conn1.execute("PRAGMA journal_mode = WAL;")
    conn1.execute("PRAGMA synchronous = FULL;")
    conn1.execute("CREATE TABLE records (seq INTEGER PRIMARY KEY, body TEXT NOT NULL, sha256 TEXT NOT NULL);")
    conn1.execute("BEGIN IMMEDIATE;")
    for seq in range(1, 51):
        body, digest = row_payload("sync-full-base", seq, 1024)
        conn1.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, body, digest))
    conn1.execute("COMMIT;")
    conn1.execute("PRAGMA wal_checkpoint(TRUNCATE);")
    conn1.close()

    # 重新打开连接并切换 PRAGMA synchronous = OFF，连续提交 51..80 (~60 KiB < 4 MiB stageBuf)，保持句柄不关闭。
    conn2 = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    conn2.execute("PRAGMA synchronous = OFF;")
    for seq in range(51, 81):
        body, digest = row_payload("sync-off-tail", seq, 1024)
        conn2.execute("BEGIN IMMEDIATE;")
        conn2.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (seq, body, digest))
        conn2.execute("COMMIT;")

    wal_path = db_path + "-wal"
    w_size = os.path.getsize(wal_path) if os.path.exists(wal_path) else 0
    sys.stdout.write(f"READY_SYNC_OFF {w_size}\n")
    sys.stdout.flush()

    cmd = sys.stdin.readline().strip()
    assert cmd == "CLOSE_NOW", f"unexpected cmd={cmd}"
    conn2.close()
    sys.stdout.write("CLOSED_OK\n")
    sys.stdout.flush()


def mode_verify_records(db_path: str, expected_tag: str, insert_branch_tag: str) -> None:
    conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
    ic = [r[0] for r in conn.execute("PRAGMA integrity_check;").fetchall()]
    cnt, min_seq, max_seq = conn.execute(
        "SELECT COUNT(*), COALESCE(MIN(seq), 0), COALESCE(MAX(seq), 0) FROM records;"
    ).fetchone()
    bad_rows = 0
    dirty_leaked = 0
    for seq, body, got_sha in conn.execute("SELECT seq, body, sha256 FROM records ORDER BY seq ASC;"):
        if "UNCOMMITTED_" in body:
            dirty_leaked += 1
        if expected_tag:
            want_body, want_sha = row_payload(expected_tag, seq, 1024)
            if body != want_body or got_sha != want_sha:
                bad_rows += 1
        else:
            if hashlib.sha256(body.encode("utf-8")).hexdigest() != got_sha:
                bad_rows += 1

    after_cnt = cnt
    if insert_branch_tag:
        next_seq = max_seq + 1
        b_body, b_sha = row_payload(insert_branch_tag, next_seq, 1024)
        conn.execute("BEGIN IMMEDIATE;")
        conn.execute("INSERT INTO records (seq, body, sha256) VALUES (?, ?, ?);", (next_seq, b_body, b_sha))
        conn.execute("COMMIT;")
        if conn.execute("PRAGMA journal_mode;").fetchone()[0].lower() == "wal":
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE);")
        after_cnt = conn.execute("SELECT COUNT(*) FROM records;").fetchone()[0]

    ic_after = [r[0] for r in conn.execute("PRAGMA integrity_check;").fetchall()]
    conn.close()
    print(json.dumps({
        "integrity_check": ic,
        "count": cnt,
        "min_seq": min_seq,
        "max_seq": max_seq,
        "bad_rows": bad_rows,
        "dirty_leaked": dirty_leaked,
        "after_insert_count": after_cnt,
        "integrity_after": ic_after,
    }))


if __name__ == "__main__":
    mode = sys.argv[1]
    if mode == "wal_loop":
        mode_wal_loop(sys.argv[2])
    elif mode == "verify_wal_child":
        mode_verify_wal_child(sys.argv[2])
    elif mode == "delete_hot_journal":
        mode_delete_hot_journal(sys.argv[2])
    elif mode == "wal_uncommitted_spill":
        mode_wal_uncommitted_spill(sys.argv[2])
    elif mode == "wal_sync_off":
        mode_wal_sync_off(sys.argv[2])
    elif mode == "verify_records":
        tag = sys.argv[3] if len(sys.argv) > 3 else ""
        branch_tag = sys.argv[4] if len(sys.argv) > 4 else ""
        mode_verify_records(sys.argv[2], tag, branch_tag)
    else:
        raise SystemExit(f"unknown mode: {mode}")
`

type sqliteVerifyRecordsResult struct {
	IntegrityCheck   []string `json:"integrity_check"`
	Count            int      `json:"count"`
	MinSeq           int      `json:"min_seq"`
	MaxSeq           int      `json:"max_seq"`
	BadRows          int      `json:"bad_rows"`
	DirtyLeaked      int      `json:"dirty_leaked"`
	AfterInsertCount int      `json:"after_insert_count"`
	IntegrityAfter   []string `json:"integrity_after"`
}

func runPythonVerifyRecords(t *testing.T, scriptPath, dbPath, expectedTag, insertBranchTag string) sqliteVerifyRecordsResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, "python3", scriptPath, "verify_records", dbPath, expectedTag, insertBranchTag).CombinedOutput()
	require.NoError(t, err, "执行 SQLite 校验脚本失败: %s", string(out))
	var res sqliteVerifyRecordsResult
	require.NoError(t, json.Unmarshal(out, &res), "解析 SQLite 校验输出失败: %s", string(out))
	return res
}

// TestE2E_ForkWorkload_SQLiteTransactionsAndCrashRecovery 使用真实 C libsqlite3（经 Python3 内置 sqlite3 驱动）
// 在 /dev/fuse 挂载点上验证在线分叉下的 WAL 连续提交前缀守恒、DELETE 热日志自动回滚、WAL >4MiB 未提交溢出丢弃与 synchronous=OFF 缓冲对比。
func TestE2E_ForkWorkload_SQLiteTransactionsAndCrashRecovery(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	scriptPath := filepath.Join(t.TempDir(), "sqlite_workload_helper.py")
	require.NoError(t, os.WriteFile(scriptPath, []byte(sqliteWorkloadHelperPy), 0o644))

	t.Run("WAL模式高频连续事务提交与多次在线分叉", func(t *testing.T) {
		parentID := createForkParentDisk(t, client, "e2e-sqlite-wal-loop-parent", "2GiB")
		parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
		dbPath := filepath.Join(parentMnt, "events.db")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "python3", "-u", scriptPath, "wal_loop", dbPath)
		stdinPipe, err := cmd.StdinPipe()
		require.NoError(t, err)
		stdoutPipe, err := cmd.StdoutPipe()
		require.NoError(t, err)
		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf
		require.NoError(t, cmd.Start())

		var lastAckedSeq atomic.Int64
		var finalParentSeq atomic.Int64
		readerDone := make(chan error, 1)
		go func() {
			scanner := bufio.NewScanner(stdoutPipe)
			for scanner.Scan() {
				line := strings.TrimSpace(scanner.Text())
				if rest, ok := strings.CutPrefix(line, "ACK "); ok {
					v, parseErr := strconv.ParseInt(rest, 10, 64)
					if parseErr != nil {
						readerDone <- parseErr
						return
					}
					lastAckedSeq.Store(v)
				} else if rest, ok := strings.CutPrefix(line, "FINAL "); ok {
					v, parseErr := strconv.ParseInt(rest, 10, 64)
					if parseErr != nil {
						readerDone <- parseErr
						return
					}
					finalParentSeq.Store(v)
				}
			}
			readerDone <- scanner.Err()
		}()

		const forkRounds = 3
		type walForkCase struct {
			child          forkDiskJSON
			minAckedBefore int64
		}
		cases := make([]walForkCase, 0, forkRounds)
		var prevAcked int64
		for i := range forkRounds {
			require.Eventually(t, func() bool {
				return lastAckedSeq.Load() >= prevAcked+8
			}, 20*time.Second, 10*time.Millisecond, "等待 SQLite WAL 第 %d 次 Fork 前提交推进超时: %s", i+1, stderrBuf.String())

			acked := lastAckedSeq.Load()
			child := forkOnlineDisk(t, client, parentID, fmt.Sprintf("e2e-sqlite-wal-child-%d", i+1))
			cases = append(cases, walForkCase{child: child, minAckedBefore: acked})
			prevAcked = lastAckedSeq.Load()
		}

		_, err = io.WriteString(stdinPipe, "STOP\n")
		require.NoError(t, err)
		require.NoError(t, stdinPipe.Close())
		require.NoError(t, <-readerDone)
		require.NoError(t, cmd.Wait(), "SQLite WAL 写入进程异常退出: %s", stderrBuf.String())
		require.Positive(t, finalParentSeq.Load())

		for idx, fc := range cases {
			childMnt, stopChild := mountWithCleanup(t, endpoint, fc.child.DiskID)
			childDB := filepath.Join(childMnt, "events.db")

			vCtx, vCancel := context.WithTimeout(context.Background(), helperProcessTimeout)
			out, vErr := exec.CommandContext(vCtx, "python3", scriptPath, "verify_wal_child", childDB).CombinedOutput()
			vCancel()
			require.NoError(t, vErr, "子盘 #%d SQLite WAL 校验失败: %s", idx+1, string(out))

			var res struct {
				IntegrityCheck   []string `json:"integrity_check"`
				FKViolations     int      `json:"fk_violations"`
				Count            int64    `json:"count"`
				MinSeq           int64    `json:"min_seq"`
				MaxSeq           int64    `json:"max_seq"`
				CounterTotal     int64    `json:"counter_total"`
				BadRows          int      `json:"bad_rows"`
				AfterInsertCount int64    `json:"after_insert_count"`
				CheckpointBusy   int      `json:"checkpoint_busy"`
				IntegrityAfter   []string `json:"integrity_after"`
			}
			require.NoError(t, json.Unmarshal(out, &res))
			require.Equal(t, []string{"ok"}, res.IntegrityCheck)
			require.Zero(t, res.FKViolations)
			require.Equal(t, int64(1), res.MinSeq)
			require.Equal(t, res.MaxSeq, res.Count, "events 表必须构成严格无空洞前缀 [1..K_cut]")
			require.GreaterOrEqual(t, res.MaxSeq, fc.minAckedBefore, "K_cut 必须不低于 Fork 前已确认提交序号")
			require.Equal(t, res.MaxSeq, res.CounterTotal, "跨表事务计数器必须与 events 最大序号严格一致")
			require.Zero(t, res.BadRows)
			require.Equal(t, res.MaxSeq+1, res.AfterInsertCount)
			require.Zero(t, res.CheckpointBusy)
			require.Equal(t, []string{"ok"}, res.IntegrityAfter)
			t.Logf("SQLite WAL 子盘 #%d 校验通过: K_cut=%d (minAcked=%d)", idx+1, res.MaxSeq, fc.minAckedBefore)
			stopChild()
		}

		// 确认父盘数据库未被任何子盘写入或 checkpoint 污染。
		pCtx, pCancel := context.WithTimeout(context.Background(), helperProcessTimeout)
		parentOut, pErr := exec.CommandContext(pCtx, "sqlite3", dbPath, "SELECT COUNT(*), MAX(seq) FROM events;").CombinedOutput()
		pCancel()
		require.NoError(t, pErr, "sqlite3 CLI 查询父盘失败: %s", string(parentOut))
		expectedPair := fmt.Sprintf("%d|%d", finalParentSeq.Load(), finalParentSeq.Load())
		require.Equal(t, expectedPair, strings.TrimSpace(string(parentOut)))
	})

	t.Run("DELETE日志模式事务中途缓存溢出热日志回滚", func(t *testing.T) {
		parentID := createForkParentDisk(t, client, "e2e-sqlite-del-parent", "2GiB")
		parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
		dbPath := filepath.Join(parentMnt, "delete_mode.db")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "python3", "-u", scriptPath, "delete_hot_journal", dbPath)
		stdinPipe, err := cmd.StdinPipe()
		require.NoError(t, err)
		stdoutPipe, err := cmd.StdoutPipe()
		require.NoError(t, err)
		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf
		require.NoError(t, cmd.Start())

		stdoutReader := bufio.NewReader(stdoutPipe)
		readyLine, err := stdoutReader.ReadString('\n')
		require.NoError(t, err, "等待 READY_MID_DELETE_TX 失败: %s", stderrBuf.String())
		readyFields := strings.Fields(strings.TrimSpace(readyLine))
		require.Len(t, readyFields, 3)
		require.Equal(t, "READY_MID_DELETE_TX", readyFields[0])

		parentJournalInfo, err := os.Stat(dbPath + "-journal")
		require.NoError(t, err, "父盘事务中途必须存在非空回滚热日志 db-journal")
		require.Positive(t, parentJournalInfo.Size())

		// 在未提交缓存溢出状态触发在线 bowl disk fork。
		childDisk := forkOnlineDisk(t, client, parentID, "e2e-sqlite-del-child")

		// 放行父盘完成 COMMIT（父盘最终落定 2600 行）。
		_, err = io.WriteString(stdinPipe, "COMMIT_NOW\n")
		require.NoError(t, err)
		require.NoError(t, stdinPipe.Close())
		commitLine, err := stdoutReader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "PARENT_COMMITTED 2600", strings.TrimSpace(commitLine))
		require.NoError(t, cmd.Wait(), "父盘 DELETE 模式进程异常: %s", stderrBuf.String())

		parentVerify := runPythonVerifyRecords(t, scriptPath, dbPath, "", "")
		require.Equal(t, []string{"ok"}, parentVerify.IntegrityCheck)
		require.Equal(t, 2600, parentVerify.Count)

		// 挂载子盘：在打开 SQLite 前先验证热日志 db-journal 与主库未提交脏页均被真实捕获！
		childMnt, _ := mountWithCleanup(t, endpoint, childDisk.DiskID)
		childDB := filepath.Join(childMnt, "delete_mode.db")
		childJournalPath := childDB + "-journal"

		childJournalInfo, err := os.Stat(childJournalPath)
		require.NoError(t, err, "子盘打开 SQLite 前必须捕获热日志 delete_mode.db-journal")
		require.Positive(t, childJournalInfo.Size())

		rawChildDBBeforeRollback, err := os.ReadFile(childDB)
		require.NoError(t, err)
		require.True(t, bytes.Contains(rawChildDBBeforeRollback, []byte("UNCOMMITTED_DIRTY_")),
			"子盘主库文件在回滚前必须实证包含缓存溢出的未提交脏页")

		// 用 SQLite 打开子盘：自动执行热日志回滚并删除 -journal，恢复至精确 100 条已提交记录，并可立即写入第 101 条。
		childVerify := runPythonVerifyRecords(t, scriptPath, childDB, "committed-delete", "child-after-rollback")
		require.Equal(t, []string{"ok"}, childVerify.IntegrityCheck)
		require.Equal(t, 100, childVerify.Count)
		require.Equal(t, 1, childVerify.MinSeq)
		require.Equal(t, 100, childVerify.MaxSeq)
		require.Zero(t, childVerify.BadRows, "回滚后原有 1..100 条记录的内容与 SHA-256 必须完好恢复")
		require.Zero(t, childVerify.DirtyLeaked, "严禁泄漏任何未提交溢出记录")
		require.Equal(t, 101, childVerify.AfterInsertCount)
		require.Equal(t, []string{"ok"}, childVerify.IntegrityAfter)

		_, err = os.Stat(childJournalPath)
		require.ErrorIs(t, err, os.ErrNotExist, "热日志自动回滚完成后 db-journal 必须被清理")
	})

	t.Run("WAL模式事务中途超4MiB未提交帧溢出与共享内存恢复", func(t *testing.T) {
		parentID := createForkParentDisk(t, client, "e2e-sqlite-walspill-parent", "2GiB")
		parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
		adminAddr := resolveAdminAddrForMount(t, parentMnt)
		dbPath := filepath.Join(parentMnt, "wal_spill.db")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "python3", "-u", scriptPath, "wal_uncommitted_spill", dbPath)
		stdinPipe, err := cmd.StdinPipe()
		require.NoError(t, err)
		stdoutPipe, err := cmd.StdoutPipe()
		require.NoError(t, err)
		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf
		require.NoError(t, cmd.Start())

		stdoutReader := bufio.NewReader(stdoutPipe)
		readyLine, err := stdoutReader.ReadString('\n')
		require.NoError(t, err, "等待 READY_MID_WAL_TX 失败: %s", stderrBuf.String())
		readyFields := strings.Fields(strings.TrimSpace(readyLine))
		require.Len(t, readyFields, 2)
		require.Equal(t, "READY_MID_WAL_TX", readyFields[0])
		walBytesOnParent, err := strconv.ParseInt(readyFields[1], 10, 64)
		require.NoError(t, err)
		require.GreaterOrEqual(t, walBytesOnParent, int64(4*1024*1024), "未提交 WAL 溢出规模必须超过 4 MiB")

		// 等待客户端自动派发的 4 MiB 未提交 WAL 块在服务端落盘完成（inflight == 0）。
		require.Eventually(t, func() bool {
			wb := readWriteBehindStats(t, adminAddr)
			return wb.FlushRPCs >= 1 && wb.Inflight == 0
		}, 10*time.Second, 10*time.Millisecond, "等待 >4 MiB 未提交 WAL 自动派发落定超时")

		childDisk := forkOnlineDisk(t, client, parentID, "e2e-sqlite-walspill-child")

		// 放行父盘完成 COMMIT（父盘达 2600 行）。
		_, err = io.WriteString(stdinPipe, "COMMIT_NOW\n")
		require.NoError(t, err)
		require.NoError(t, stdinPipe.Close())
		commitLine, err := stdoutReader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "PARENT_COMMITTED 2600", strings.TrimSpace(commitLine))
		require.NoError(t, cmd.Wait(), "父盘 WAL 溢出测试进程异常: %s", stderrBuf.String())

		// 挂载子盘：打开 SQLite 前先验证 >= 4 MiB 的未提交 WAL 帧确已落入子盘！
		childMnt, _ := mountWithCleanup(t, endpoint, childDisk.DiskID)
		childDB := filepath.Join(childMnt, "wal_spill.db")
		childWALInfo, err := os.Stat(childDB + "-wal")
		require.NoError(t, err)
		require.GreaterOrEqual(t, childWALInfo.Size(), int64(4*1024*1024),
			"子盘打开前 wal_spill.db-wal 必须实证包含已自动刷入服务端的 >=4 MiB 未提交帧")

		// 打开 SQLite：自动重建 -shm，精确保留已提交的 1..100 行并丢弃全部未提交 WAL 帧。
		childVerify := runPythonVerifyRecords(t, scriptPath, childDB, "committed-wal", "child-wal-recovered")
		require.Equal(t, []string{"ok"}, childVerify.IntegrityCheck)
		require.Equal(t, 100, childVerify.Count)
		require.Equal(t, 1, childVerify.MinSeq)
		require.Equal(t, 100, childVerify.MaxSeq)
		require.Zero(t, childVerify.BadRows)
		require.Zero(t, childVerify.DirtyLeaked)
		require.Equal(t, 101, childVerify.AfterInsertCount)
		require.Equal(t, []string{"ok"}, childVerify.IntegrityAfter)
	})

	t.Run("WAL模式synchronous_OFF客户端缓冲未刷盘对比", func(t *testing.T) {
		parentID := createForkParentDisk(t, client, "e2e-sqlite-syncoff-parent", "2GiB")
		parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
		adminAddr := resolveAdminAddrForMount(t, parentMnt)
		dbPath := filepath.Join(parentMnt, "wal_sync_off.db")

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
		defer cancel()
		cmd := exec.CommandContext(ctx, "python3", "-u", scriptPath, "wal_sync_off", dbPath)
		stdinPipe, err := cmd.StdinPipe()
		require.NoError(t, err)
		stdoutPipe, err := cmd.StdoutPipe()
		require.NoError(t, err)
		var stderrBuf bytes.Buffer
		cmd.Stderr = &stderrBuf
		require.NoError(t, cmd.Start())

		stdoutReader := bufio.NewReader(stdoutPipe)
		readyLine, err := stdoutReader.ReadString('\n')
		require.NoError(t, err, "等待 READY_SYNC_OFF 失败: %s", stderrBuf.String())
		parentWALBytesStr, ok := strings.CutPrefix(strings.TrimSpace(readyLine), "READY_SYNC_OFF ")
		require.True(t, ok, "预期收到 READY_SYNC_OFF，实际: %q", readyLine)
		parentWALBytes, err := strconv.ParseInt(parentWALBytesStr, 10, 64)
		require.NoError(t, err)
		require.GreaterOrEqual(t, parentWALBytes, int64(60*1024), "父盘本地视图应观测到 >=60 KiB 的已缓冲 WAL 帧")

		wb := readWriteBehindStats(t, adminAddr)
		require.GreaterOrEqual(t, wb.PendingSlots, int64(1), "synchronous=OFF 未关闭连接时 WAL 帧必须滞留客户端 WriteBehind stageBuf")

		childDisk := forkOnlineDisk(t, client, parentID, "e2e-sqlite-syncoff-child")

		// 子盘打开 SQLite 前先核验 wal_sync_off.db-wal 无任何 WAL 数据帧落盘：
		// SQLite 新建 -wal 时调用 fchown 触发 SetAttr -> ResetInodeStream，首笔 32B WAL 文件头走同步探针，
		// 后续全部 51..80 事务帧（>=4120B/帧，总计 >60 KiB）均滞留客户端 stageBuf，故子盘 db-wal 尺寸 <= 32B（0 个帧）。
		childMnt, _ := mountWithCleanup(t, endpoint, childDisk.DiskID)
		childDB := filepath.Join(childMnt, "wal_sync_off.db")
		if walInfo, statErr := os.Stat(childDB + "-wal"); statErr == nil {
			require.LessOrEqual(t, walInfo.Size(), int64(32),
				"synchronous=OFF 下子盘 db-wal 最多仅含 32B WAL 头同步探针（0 个事务数据帧）")
		} else {
			require.ErrorIs(t, statErr, os.ErrNotExist)
		}
		childVerify := runPythonVerifyRecords(t, scriptPath, childDB, "sync-full-base", "child-syncoff-branch")
		require.Equal(t, []string{"ok"}, childVerify.IntegrityCheck)
		require.Equal(t, 50, childVerify.Count, "synchronous=OFF 下滞留客户端缓冲的 51..80 不得出现在子盘")
		require.Equal(t, 1, childVerify.MinSeq)
		require.Equal(t, 50, childVerify.MaxSeq)
		require.Zero(t, childVerify.BadRows)
		require.Equal(t, 51, childVerify.AfterInsertCount)

		// 关闭父盘 SQLite 连接（触发 Flush 冲刷客户端缓冲），父盘必须完整拥有全部 80 行。
		_, err = io.WriteString(stdinPipe, "CLOSE_NOW\n")
		require.NoError(t, err)
		require.NoError(t, stdinPipe.Close())
		closedLine, err := stdoutReader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "CLOSED_OK", strings.TrimSpace(closedLine))
		require.NoError(t, cmd.Wait(), "父盘 synchronous=OFF 进程异常: %s", stderrBuf.String())

		parentVerify := runPythonVerifyRecords(t, scriptPath, dbPath, "", "")
		require.Equal(t, []string{"ok"}, parentVerify.IntegrityCheck)
		require.Equal(t, 80, parentVerify.Count)
		require.Equal(t, 1, parentVerify.MinSeq)
		require.Equal(t, 80, parentVerify.MaxSeq)
		require.Zero(t, parentVerify.BadRows)
	})

	recordArtifact(t, map[string]string{
		"wal_continuous_multifork":    "gapless_acid_verified",
		"delete_hot_journal_rollback": "100_kept_2500_rolled_back",
		"wal_4mib_uncommitted_spill":  "100_kept_2500_discarded",
		"wal_synchronous_off_child":   "50",
		"wal_synchronous_off_parent":  "80",
	})
}

func runGitCmd(t *testing.T, repoDir string, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = repoDir
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "执行 git %v 失败 (dir=%s): %s", args, repoDir, string(out))
	return string(out)
}

func expectedGitStepContent(commitNum int) string {
	payload := hex.EncodeToString(deterministicPayload("git-step-"+strconv.Itoa(commitNum), 512))
	return fmt.Sprintf("commit=%d\nsha256=%s\npayload=%s\n", commitNum, sha256Hex([]byte(payload)), payload)
}

func expectedGitTrackedContent(slot, commitNum int) string {
	payload := hex.EncodeToString(deterministicPayload(fmt.Sprintf("git-tracked-%d-%d", slot, commitNum), 512))
	return fmt.Sprintf("slot=%d\nupdated_at_commit=%d\npayload=%s\n", slot, commitNum, payload)
}

// TestE2E_ForkWorkload_RealGitCommitLoop 在真实 git add + git commit 连续提交循环运行期间
// 触发 3 次在线 bowl disk fork，验证子盘清理中断锁文件后对象图零损坏、提交计数单调不退且可继续提交。
func TestE2E_ForkWorkload_RealGitCommitLoop(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	parentID := createForkParentDisk(t, client, "e2e-git-fork-parent", "2GiB")
	parentMnt, _ := mountWithCleanup(t, endpoint, parentID)
	repoDir := filepath.Join(parentMnt, "repo")
	require.NoError(t, os.MkdirAll(filepath.Join(repoDir, "tracked"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(repoDir, "history"), 0o755))

	runGitCmd(t, repoDir, "init", "-b", "main")
	runGitCmd(t, repoDir, "config", "user.name", "Bowl E2E")
	runGitCmd(t, repoDir, "config", "user.email", "e2e@bowl.local")

	const trackedSlots = 5
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "README.md"), []byte("# Bowl Online Fork Git E2E\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(repoDir, "state.json"), []byte("{\"commit\":0}\n"), 0o644))
	for s := range trackedSlots {
		require.NoError(t, os.WriteFile(
			filepath.Join(repoDir, "tracked", fmt.Sprintf("mod_%d.txt", s)),
			[]byte(expectedGitTrackedContent(s, 0)),
			0o644,
		))
	}
	runGitCmd(t, repoDir, "add", "-A")
	runGitCmd(t, repoDir, "commit", "-m", "commit-0")

	const totalCommits = 25
	var lastCommittedN atomic.Int64
	workerErrCh := make(chan error, 1)

	go func() {
		for n := 1; n <= totalCommits; n++ {
			slot := n % trackedSlots
			if wErr := os.WriteFile(
				filepath.Join(repoDir, "tracked", fmt.Sprintf("mod_%d.txt", slot)),
				[]byte(expectedGitTrackedContent(slot, n)),
				0o644,
			); wErr != nil {
				workerErrCh <- wErr
				return
			}
			if wErr := os.WriteFile(
				filepath.Join(repoDir, "history", fmt.Sprintf("step_%02d.txt", n)),
				[]byte(expectedGitStepContent(n)),
				0o644,
			); wErr != nil {
				workerErrCh <- wErr
				return
			}
			stateRaw := fmt.Sprintf("{\"commit\":%d}\n", n)
			if wErr := os.WriteFile(filepath.Join(repoDir, "state.json"), []byte(stateRaw), 0o644); wErr != nil {
				workerErrCh <- wErr
				return
			}

			addCtx, addCancel := context.WithTimeout(context.Background(), helperProcessTimeout)
			addOut, addErr := exec.CommandContext(addCtx, "git", "-C", repoDir, "add", "-A").CombinedOutput()
			addCancel()
			if addErr != nil {
				workerErrCh <- fmt.Errorf("git add commit-%d 失败: %w (%s)", n, addErr, string(addOut))
				return
			}

			cCtx, cCancel := context.WithTimeout(context.Background(), helperProcessTimeout)
			cOut, cErr := exec.CommandContext(cCtx, "git", "-C", repoDir, "commit", "-m", fmt.Sprintf("commit-%d", n)).CombinedOutput()
			cCancel()
			if cErr != nil {
				workerErrCh <- fmt.Errorf("git commit-%d 失败: %w (%s)", n, cErr, string(cOut))
				return
			}
			lastCommittedN.Store(int64(n))
		}
		workerErrCh <- nil
	}()

	type gitForkSnap struct {
		child           forkDiskJSON
		committedBefore int64
	}
	const forkCount = 3
	thresholds := [forkCount]int64{3, 10, 17}
	gitSnaps := make([]gitForkSnap, 0, forkCount)

	for i, minCommit := range thresholds {
		require.Eventually(t, func() bool {
			return lastCommittedN.Load() >= minCommit
		}, 30*time.Second, 10*time.Millisecond, "等待 Git 提交推进至 commit-%d 超时", minCommit)

		before := lastCommittedN.Load()
		child := forkOnlineDisk(t, client, parentID, fmt.Sprintf("e2e-git-fork-child-%d", i+1))
		gitSnaps = append(gitSnaps, gitForkSnap{
			child:           child,
			committedBefore: before,
		})
	}

	require.NoError(t, <-workerErrCh, "父盘 Git 连续提交循环失败")
	require.Equal(t, int64(totalCommits), lastCommittedN.Load())

	for idx, snap := range gitSnaps {
		childMnt, stopChild := mountWithCleanup(t, endpoint, snap.child.DiskID)
		childRepo := filepath.Join(childMnt, "repo")

		// 若在线切面恰好捕获在 git add / git commit 持锁中途，模拟掉电恢复清理残留 *.lock 并对齐索引与工作树至 HEAD。
		var removedLocks []string
		walkErr := filepath.WalkDir(filepath.Join(childRepo, ".git"), func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !d.IsDir() && strings.HasSuffix(d.Name(), ".lock") {
				removedLocks = append(removedLocks, p)
				return os.Remove(p)
			}
			return nil
		})
		require.NoError(t, walkErr)

		// 在执行 reset/clean 前先直接读取原始 HEAD 提交树，证明提交对象图绝无半提交文件泄漏。
		headSubjectBeforeReset := strings.TrimSpace(runGitCmd(t, childRepo, "log", "-1", "--format=%s", "HEAD"))
		rawHeadNStr, ok := strings.CutPrefix(headSubjectBeforeReset, "commit-")
		require.True(t, ok, "未预期的 HEAD 提交标题: %s", headSubjectBeforeReset)
		rawHeadN, parseErr := strconv.Atoi(rawHeadNStr)
		require.NoError(t, parseErr)
		treeFilesOut := runGitCmd(t, childRepo, "ls-tree", "-r", "--name-only", "HEAD")
		treeFiles := strings.Split(strings.TrimSpace(treeFilesOut), "\n")
		if rawHeadN >= 1 {
			require.Contains(t, treeFiles, fmt.Sprintf("history/step_%02d.txt", rawHeadN))
		}
		require.NotContains(t, treeFiles, fmt.Sprintf("history/step_%02d.txt", rawHeadN+1),
			"原始 HEAD 提交树绝不得包含尚未完成 commit 的下一轮文件")

		runGitCmd(t, childRepo, "reset", "--hard", "HEAD")
		runGitCmd(t, childRepo, "clean", "-fd")

		// 校验 Git 对象库完整性（零损坏、零缺失、零断链）。
		fsckOut := runGitCmd(t, childRepo, "fsck", "--full")
		lowerFsck := strings.ToLower(fsckOut)
		require.NotContains(t, lowerFsck, "error:")
		require.NotContains(t, lowerFsck, "fatal:")
		require.NotContains(t, lowerFsck, "corrupt")
		require.NotContains(t, lowerFsck, "missing")
		require.NotContains(t, lowerFsck, "broken")

		revCountStr := strings.TrimSpace(runGitCmd(t, childRepo, "rev-list", "--count", "HEAD"))
		revCount, parseErr := strconv.ParseInt(revCountStr, 10, 64)
		require.NoError(t, parseErr)
		require.GreaterOrEqual(t, revCount, snap.committedBefore+1,
			"子盘 #%d 提交计数必须不低于 Fork 前已完成的提交数 (+1 含 commit-0)", idx+1)

		headSubject := strings.TrimSpace(runGitCmd(t, childRepo, "log", "-1", "--format=%s", "HEAD"))
		headNStr, ok := strings.CutPrefix(headSubject, "commit-")
		require.True(t, ok, "未预期的 HEAD 提交标题: %s", headSubject)
		headN, parseErr := strconv.Atoi(headNStr)
		require.NoError(t, parseErr)
		require.Equal(t, int64(headN+1), revCount)
		require.GreaterOrEqual(t, int64(headN), snap.committedBefore)

		// 逐个校验 HEAD 提交对应的工作区文件确定性内容。
		gotState, readErr := os.ReadFile(filepath.Join(childRepo, "state.json"))
		require.NoError(t, readErr)
		require.Equal(t, fmt.Sprintf("{\"commit\":%d}\n", headN), string(gotState))

		for step := 1; step <= headN; step++ {
			stepRaw, sErr := os.ReadFile(filepath.Join(childRepo, "history", fmt.Sprintf("step_%02d.txt", step)))
			require.NoError(t, sErr)
			require.Equal(t, expectedGitStepContent(step), string(stepRaw))
		}
		_, nextStepErr := os.Stat(filepath.Join(childRepo, "history", fmt.Sprintf("step_%02d.txt", headN+1)))
		require.ErrorIs(t, nextStepErr, os.ErrNotExist)

		for s := range trackedSlots {
			lastModCommit := 0
			for c := headN; c >= 1; c-- {
				if c%trackedSlots == s {
					lastModCommit = c
					break
				}
			}
			trackedRaw, tErr := os.ReadFile(filepath.Join(childRepo, "tracked", fmt.Sprintf("mod_%d.txt", s)))
			require.NoError(t, tErr)
			require.Equal(t, expectedGitTrackedContent(s, lastModCommit), string(trackedRaw))
		}

		// 在派生子盘上执行全新 Git 提交并再次运行 git fsck --full。
		childBranchFile := filepath.Join(childRepo, fmt.Sprintf("child_fork_%d.txt", idx+1))
		require.NoError(t, os.WriteFile(childBranchFile, []byte("child-diverged-commit\n"), 0o644))
		runGitCmd(t, childRepo, "add", filepath.Base(childBranchFile))
		runGitCmd(t, childRepo, "commit", "-m", fmt.Sprintf("child-%d-commit", idx+1))
		afterRevCount := strings.TrimSpace(runGitCmd(t, childRepo, "rev-list", "--count", "HEAD"))
		require.Equal(t, strconv.Itoa(headN+2), afterRevCount)
		afterFsck := strings.ToLower(runGitCmd(t, childRepo, "fsck", "--full"))
		require.NotContains(t, afterFsck, "error:")
		require.NotContains(t, afterFsck, "corrupt")

		t.Logf("Git 子盘 #%d 校验通过: HEAD=commit-%d (minBefore=%d, 清理残留锁=%d)",
			idx+1, headN, snap.committedBefore, len(removedLocks))
		stopChild()
	}

	// 核验父盘仓库完整停留在 commit-25（共 26 个提交）且不含任何子盘分支文件。
	parentFsck := strings.ToLower(runGitCmd(t, repoDir, "fsck", "--full"))
	require.NotContains(t, parentFsck, "error:")
	require.NotContains(t, parentFsck, "corrupt")
	parentRevCount := strings.TrimSpace(runGitCmd(t, repoDir, "rev-list", "--count", "HEAD"))
	require.Equal(t, strconv.Itoa(totalCommits+1), parentRevCount)
	_, err := os.Stat(filepath.Join(repoDir, "child_fork_1.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	recordArtifact(t, map[string]string{
		"parent_total_commits": strconv.Itoa(totalCommits + 1),
		"fork_children_count":  strconv.Itoa(forkCount),
		"git_fsck_clean":       "true",
		"child_new_commit_ok":  "true",
		"parent_isolated":      "true",
	})
}
