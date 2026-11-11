//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
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

func requireFileContent(t *testing.T, path, want string) {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	require.Equal(t, want, string(data))
}

// permissionErrorSummary 提取命令 stderr 末尾的权限错误描述，作为可复现的工件字段。
func permissionErrorSummary(stderr string) string {
	parts := strings.Split(stderr, ": ")
	return strings.TrimSpace(parts[len(parts)-1])
}

// TestE2E_MultiClient_ComprehensiveAcceptance 验证单盘无会话 Epoch 世代号抢占、已打开句柄围栏、孤儿清扫、只读并发挂载与多用户权限。
func TestE2E_MultiClient_ComprehensiveAcceptance(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()
	endpoint := getStorageEndpoint()

	mpA := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(mpA), 0o755), "测试临时目录父级必须可被普通用户穿透")
	require.NoError(t, os.Chmod(mpA, 0o755), "挂载点必须可被普通用户穿透")
	cmdA, _, stderrA := startBowlMount(t, endpoint, diskID, mpA, "--allow-other")
	waitForMount(t, mpA, "挂载盘 A 失败: %s", stderrA.String())
	t.Cleanup(func() { stopBowlMountGracefully(t, cmdA, mpA) })

	pathA := filepath.Join(mpA, "snapshot.txt")
	require.NoError(t, os.WriteFile(pathA, []byte("版本一"), 0o644))
	require.NoError(t, unix.Setxattr(pathA, "user.ro_meta", []byte("ro_xattr_val"), 0))
	require.NoError(t, os.Symlink("snapshot.txt", filepath.Join(mpA, "ro_link")))
	require.NoError(t, os.Mkdir(filepath.Join(mpA, "ro_existing_dir"), 0o755))

	t.Run("只读无状态挂载返回Epoch0且统一拦截各类写操作", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		roHandshake, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
			DiskId:   diskID,
			ReadOnly: true,
		}))
		require.NoError(t, err)
		require.Zero(t, roHandshake.Msg.GetEpoch(), "MountDisk(read_only=true) 必须返回 epoch = 0")

		mpRO := t.TempDir()
		cmdRO, _, stderrRO := startBowlMount(t, endpoint, diskID, mpRO, "--ro", "--direct-io")
		waitForMount(t, mpRO, "只读挂载失败: %s", stderrRO.String())
		t.Cleanup(func() { stopBowlMountGracefully(t, cmdRO, mpRO) })

		roSnap := filepath.Join(mpRO, "snapshot.txt")
		roSym := filepath.Join(mpRO, "ro_link")
		roDir := filepath.Join(mpRO, "ro_existing_dir")

		requireFileContent(t, roSnap, "版本一")
		symTarget, err := os.Readlink(roSym)
		require.NoError(t, err)
		require.Equal(t, "snapshot.txt", symTarget)

		xattrBuf := make([]byte, 32)
		xattrLen, err := unix.Getxattr(roSnap, "user.ro_meta", xattrBuf)
		require.NoError(t, err)
		require.Equal(t, "ro_xattr_val", string(xattrBuf[:xattrLen]))
		listLen, err := unix.Listxattr(roSnap, nil)
		require.NoError(t, err)
		require.Positive(t, listLen)

		roFD, err := os.Open(roSnap)
		require.NoError(t, err)
		defer roFD.Close()
		roFlags, err := getIoctlFlags(int(roFD.Fd()))
		require.NoError(t, err)
		require.Zero(t, roFlags)

		require.ErrorIs(t, os.WriteFile(filepath.Join(mpRO, "new.txt"), nil, 0o644), syscall.EROFS)
		_, openWriteErr := os.OpenFile(roSnap, os.O_WRONLY, 0)
		require.ErrorIs(t, openWriteErr, syscall.EROFS)
		require.ErrorIs(t, os.Mkdir(filepath.Join(mpRO, "new_dir"), 0o755), syscall.EROFS)
		require.ErrorIs(t, syscall.Rmdir(roDir), syscall.EROFS)
		require.ErrorIs(t, syscall.Unlink(roSnap), syscall.EROFS)
		require.ErrorIs(t, os.Rename(roSnap, filepath.Join(mpRO, "renamed.txt")), syscall.EROFS)
		require.ErrorIs(t, os.Link(roSnap, filepath.Join(mpRO, "hardlink.txt")), syscall.EROFS)
		require.ErrorIs(t, os.Symlink("snapshot.txt", filepath.Join(mpRO, "new_sym")), syscall.EROFS)
		require.ErrorIs(t, os.Chmod(roSnap, 0o600), syscall.EROFS)
		require.ErrorIs(t, os.Truncate(roSnap, 4), syscall.EROFS)
		require.ErrorIs(t, unix.Setxattr(roSnap, "user.denied", []byte("v"), 0), syscall.EROFS)
		require.ErrorIs(t, unix.Removexattr(roSnap, "user.ro_meta"), syscall.EROFS)
		require.ErrorIs(t, setIoctlFlags(int(roFD.Fd()), fsImmutableFl), syscall.EROFS)
		require.ErrorIs(t, unix.Fallocate(int(roFD.Fd()), 0, 0, 4096), syscall.EBADF)

		liveFileA := filepath.Join(mpA, "live-while-ro.txt")
		require.NoError(t, os.WriteFile(liveFileA, []byte("实时内容"), 0o644), "只读挂载不得干扰正在运行的活跃写端")
		requireFileContent(t, filepath.Join(mpRO, "live-while-ro.txt"), "实时内容")
	})

	mpB := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(mpB), 0o755), "测试临时目录父级必须可被普通用户穿透")
	require.NoError(t, os.Chmod(mpB, 0o755), "挂载点必须可被普通用户穿透")
	var orphanReclaimed bool

	t.Run("新可写挂载递增Epoch抢占旧写端并拦截旧已打开句柄与清扫孤儿节点", func(t *testing.T) {
		preOpenPath := filepath.Join(mpA, "preopened-writer.txt")
		preOpenFile, err := os.OpenFile(preOpenPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = preOpenFile.Close() })
		_, err = preOpenFile.WriteString("接管前初始数据")
		require.NoError(t, err)
		require.NoError(t, preOpenFile.Sync())

		tmpFD, err := unix.Open(mpA, unix.O_TMPFILE|unix.O_RDWR, 0o600)
		require.NoError(t, err)
		preTmpFile := os.NewFile(uintptr(tmpFD), "preopened-tmpfile")
		t.Cleanup(func() { _ = preTmpFile.Close() })
		_, err = preTmpFile.WriteString("接管前匿名缓冲")
		require.NoError(t, err)

		orphanPath := filepath.Join(mpA, "orphan-unlinked.bin")
		orphanFile, err := os.OpenFile(orphanPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = orphanFile.Close() })

		orphanPayload := bytes.Repeat([]byte("O"), 65536)
		_, err = orphanFile.Write(orphanPayload)
		require.NoError(t, err)
		require.NoError(t, orphanFile.Sync())

		var orphanStat unix.Stat_t
		require.NoError(t, unix.Fstat(int(orphanFile.Fd()), &orphanStat))
		orphanIno := orphanStat.Ino

		require.NoError(t, os.Remove(orphanPath))

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			DiskId: diskID,
			Ino:    orphanIno,
		}))
		require.NoError(t, err, "接管前已解链但仍打开的孤儿节点必须存在于服务端")

		diskBefore, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
		require.NoError(t, err)
		usedBeforeTakeover := diskBefore.Msg.GetDisk().GetUsedBytes()
		require.GreaterOrEqual(t, usedBeforeTakeover, uint64(len(orphanPayload)))

		takeoverStart := time.Now()
		cmdB, _, stderrB := startBowlMount(t, endpoint, diskID, mpB, "--allow-other")
		waitForMount(t, mpB, "新可写客户端零等待接管挂载失败: %s", stderrB.String())
		t.Cleanup(func() { stopBowlMountGracefully(t, cmdB, mpB) })
		require.Less(t, time.Since(takeoverStart), 3*time.Second, "新可写挂载必须零等待立即接管")

		require.NoError(t, os.WriteFile(filepath.Join(mpB, "taken-over.txt"), []byte("新写端接管"), 0o644))
		requireFileContent(t, filepath.Join(mpB, "taken-over.txt"), "新写端接管")

		_, orphanAttrErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			DiskId: diskID,
			Ino:    orphanIno,
		}))
		require.Error(t, orphanAttrErr, "新可写挂载握手必须自动清扫前任残留的孤儿节点")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(orphanAttrErr))

		diskAfter, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
		require.NoError(t, err)
		require.Less(t, diskAfter.Msg.GetDisk().GetUsedBytes(), usedBeforeTakeover, "清扫孤儿节点后必须释放其占用的逻辑容量")
		orphanReclaimed = true

		require.ErrorIs(t, os.Mkdir(filepath.Join(mpA, "fenced-dir"), 0o755), syscall.EBUSY)
		require.ErrorIs(t, os.WriteFile(filepath.Join(mpA, "fenced-file.txt"), []byte("stale"), 0o644), syscall.EBUSY)

		require.ErrorIs(t, preOpenFile.Truncate(0), syscall.EBUSY, "旧挂载已打开常规句柄 Truncate 必须被 Epoch 围栏拦截返回 EBUSY")
		_, staleHandleWriteErr := preOpenFile.WriteAt([]byte("stale-handle-write"), 0)
		require.ErrorIs(t, staleHandleWriteErr, syscall.EBUSY, "旧挂载已打开常规句柄 Write 必须被 Epoch 围栏拦截返回 EBUSY")
		require.ErrorIs(t, preOpenFile.Sync(), syscall.EBUSY, "旧挂载已打开常规句柄 Fsync 必须被 Epoch 围栏拦截返回 EBUSY")

		staleLinkErr := unix.Linkat(
			unix.AT_FDCWD, fmt.Sprintf("/proc/self/fd/%d", preTmpFile.Fd()),
			unix.AT_FDCWD, filepath.Join(mpA, "stale-tmp-named.txt"), unix.AT_SYMLINK_FOLLOW,
		)
		require.ErrorIs(t, staleLinkErr, syscall.EBUSY, "旧挂载已打开匿名句柄 Linkat 必须被 Epoch 围栏拦截返回 EBUSY")
		require.ErrorIs(t, preTmpFile.Sync(), syscall.EBUSY, "旧挂载已打开匿名句柄 Fsync 必须被 Epoch 围栏拦截返回 EBUSY")
		require.ErrorIs(t, preTmpFile.Close(), syscall.EBUSY, "旧挂载已打开匿名句柄 Close 排空上云必须被 Epoch 围栏拦截返回 EBUSY")

		require.ErrorIs(t, os.Mkdir(filepath.Join(mpA, "fenced-dir-retry"), 0o755), syscall.EBUSY, "旧写端不得自动重连抢回 Epoch")
		require.NoError(t, os.WriteFile(filepath.Join(mpB, "still-leader.txt"), []byte("持续持有写权限"), 0o644))
		requireFileContent(t, filepath.Join(mpB, "preopened-writer.txt"), "接管前初始数据")
	})

	var remove1001Summary, rename1001Summary string
	t.Run("多用户权限与粘滞位隔离", func(t *testing.T) {
		secretPath := filepath.Join(mpB, "secret.txt")
		require.NoError(t, os.WriteFile(secretPath, []byte("ROOT_TOP_SECRET_DATA"), 0o600))

		roDirPath := filepath.Join(mpB, "readonly-dir")
		require.NoError(t, os.Mkdir(roDirPath, 0o755))

		sharedTmpPath := filepath.Join(mpB, "shared-tmp")
		require.NoError(t, os.Mkdir(sharedTmpPath, 0o777|os.ModeSticky))
		require.NoError(t, unix.Chmod(sharedTmpPath, 0o1777), "设置 Sticky Bit (01777) 失败")

		codeRead, _, errRead := runAs(t, 1000, 1000, "cat", secretPath)
		require.Equal(t, 1, codeRead, "普通用户读取 root 0600 必须被拒绝: %s", errRead)
		require.Contains(t, errRead, "Permission denied")

		codeWrite, _, errWrite := runAsWithStdin(t, 1000, 1000, "unauthorized append\n", "tee", "-a", secretPath)
		require.Equal(t, 1, codeWrite, "普通用户写入 root 0600 必须被拒绝: %s", errWrite)
		require.Contains(t, errWrite, "Permission denied")

		forbiddenFile := filepath.Join(roDirPath, "forbidden.txt")
		codeCreateRO, _, errCreateRO := runAs(t, 1000, 1000, "touch", forbiddenFile)
		require.Equal(t, 1, codeCreateRO, "普通用户在 0755 目录下创建文件必须被拒绝: %s", errCreateRO)
		require.Contains(t, errCreateRO, "Permission denied")

		u1000File := filepath.Join(sharedTmpPath, "u1000.txt")
		codeCreateTmp, _, errCreateTmp := runAs(t, 1000, 1000, "touch", u1000File)
		require.Equal(t, 0, codeCreateTmp, "uid=1000 在 01777 共享目录下创建文件必须成功: %s", errCreateTmp)

		st1000, err := os.Stat(u1000File)
		require.NoError(t, err)
		sysStat, ok := st1000.Sys().(*syscall.Stat_t)
		require.True(t, ok)
		require.Equal(t, uint32(1000), sysStat.Uid, "文件属主 UID 必须为 1000")
		require.Equal(t, uint32(1000), sysStat.Gid, "文件属组 GID 必须为 1000")

		codeRemove1001, _, errRemove1001 := runAs(t, 1001, 1001, "rm", u1000File)
		require.Equal(t, 1, codeRemove1001, "非属主删除 Sticky Bit 目录下文件必须被拦截: %s", errRemove1001)
		remove1001Summary = permissionErrorSummary(errRemove1001)
		require.Equal(t, "Operation not permitted", remove1001Summary, "非属主删除必须返回 EPERM: %s", errRemove1001)
		require.FileExists(t, u1000File, "被拦截后文件必须仍然存在")

		u1001Renamed := filepath.Join(sharedTmpPath, "u1001_renamed.txt")
		codeRename1001, _, errRename1001 := runAs(t, 1001, 1001, "mv", u1000File, u1001Renamed)
		require.Equal(t, 1, codeRename1001, "非属主重命名 Sticky Bit 目录下文件必须被拦截: %s", errRename1001)
		rename1001Summary = permissionErrorSummary(errRename1001)
		require.Equal(t, "Operation not permitted", rename1001Summary, "非属主重命名必须返回 EPERM: %s", errRename1001)
		require.NoFileExists(t, u1001Renamed, "被拦截后不得出现重命名目标")
		require.FileExists(t, u1000File, "被拦截后源文件必须仍然存在")

		codeRemove1000, _, errRemove1000 := runAs(t, 1000, 1000, "rm", u1000File)
		require.Equal(t, 0, codeRemove1000, "属主删除自己创建的文件必须成功: %s", errRemove1000)
		_, err = os.Stat(u1000File)
		require.True(t, os.IsNotExist(err), "删除后文件必须不再存在")
	})

	recordArtifact(t, map[string]string{
		"readonly_epoch":         "0",
		"readonly_mutations":     "EROFS",
		"readonly_fd_fallocate":  "EBADF",
		"readonly_live_read":     "实时内容",
		"writer_takeover":        "新写端接管",
		"orphan_reclaimed":       strconv.FormatBool(orphanReclaimed),
		"preempted_writer":       "EBUSY",
		"preopened_handle_fence": "EBUSY",
		"preopened_tmp_fence":    "EBUSY",
		"no_reconnect_pingpong":  "true",
		"uid1000_read_0600":      "EACCES",
		"uid1000_write_0600":     "EACCES",
		"uid1000_create_0755":    "EACCES",
		"uid1000_create_sticky":  "ok",
		"uid1001_remove_sticky":  remove1001Summary,
		"uid1001_rename_sticky":  rename1001Summary,
		"uid1000_remove_own":     "ok",
	})
}
