//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
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

// TestE2E_MultiClient_ComprehensiveAcceptance 验证单盘无会话 Epoch 世代号抢占、孤儿自动清扫、只读无状态并发挂载与多用户权限。
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
	defer stopBowlMountGracefully(t, cmdA, mpA)

	pathA := filepath.Join(mpA, "snapshot.txt")
	require.NoError(t, os.WriteFile(pathA, []byte("版本一"), 0o644))

	t.Run("只读无状态挂载返回Epoch0且不干扰活跃写端", func(t *testing.T) {
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
		defer stopBowlMountGracefully(t, cmdRO, mpRO)

		requireFileContent(t, filepath.Join(mpRO, "snapshot.txt"), "版本一")
		require.ErrorIs(t, os.WriteFile(filepath.Join(mpRO, "new.txt"), nil, 0o644), syscall.EROFS, "只读挂载本地写操作必须直接返回 EROFS")

		// 验证活跃写端 mpA 未被只读挂载踢掉，仍可正常创建并写入新文件，且只读客户端可无状态实时直读
		liveFileA := filepath.Join(mpA, "live-while-ro.txt")
		require.NoError(t, os.WriteFile(liveFileA, []byte("实时内容"), 0o644), "只读挂载不得干扰正在运行的活跃写端")
		requireFileContent(t, filepath.Join(mpRO, "live-while-ro.txt"), "实时内容")
	})

	mpB := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(mpB), 0o755), "测试临时目录父级必须可被普通用户穿透")
	require.NoError(t, os.Chmod(mpB, 0o755), "挂载点必须可被普通用户穿透")
	var orphanReclaimed bool

	t.Run("新可写挂载零等待递增Epoch抢占旧写端并自动清扫孤儿节点", func(t *testing.T) {
		// 1. 在旧写端 mpA 创建 64 KiB 文件并在保持打开状态下解链，构造残留 Orphan Inode
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
		require.NoError(t, err, "接管前已解链但仍打开的 Orphan Inode 必须存在于服务端")

		diskBefore, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
		require.NoError(t, err)
		usedBeforeTakeover := diskBefore.Msg.GetDisk().GetUsedBytes()
		require.GreaterOrEqual(t, usedBeforeTakeover, uint64(len(orphanPayload)))

		// 2. 在旧可写客户端 mpA 未卸载时，新可写客户端 mpB 直接通过 bowl mount 零等待接管写权限
		takeoverStart := time.Now()
		cmdB, _, stderrB := startBowlMount(t, endpoint, diskID, mpB, "--allow-other")
		waitForMount(t, mpB, "新可写客户端零等待接管挂载失败: %s", stderrB.String())
		t.Cleanup(func() { stopBowlMountGracefully(t, cmdB, mpB) })
		require.Less(t, time.Since(takeoverStart), 3*time.Second, "新可写挂载必须零 TTL 等待立即接管")

		// 新写端 mpB 正常写入成功
		require.NoError(t, os.WriteFile(filepath.Join(mpB, "taken-over.txt"), []byte("新写端接管"), 0o644))
		requireFileContent(t, filepath.Join(mpB, "taken-over.txt"), "新写端接管")

		// 3. 验证前任残留的 Orphan Inode 已被新写端 MountDisk(read_only=false) 自动清扫
		_, orphanAttrErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
			DiskId: diskID,
			Ino:    orphanIno,
		}))
		require.Error(t, orphanAttrErr, "新可写挂载握手必须自动清扫前任残留的 Orphan Inode")
		require.Equal(t, connect.CodeNotFound, connect.CodeOf(orphanAttrErr))

		diskAfter, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
		require.NoError(t, err)
		require.Less(t, diskAfter.Msg.GetDisk().GetUsedBytes(), usedBeforeTakeover, "清扫孤儿节点后必须释放其占用的逻辑容量")
		orphanReclaimed = true

		// 4. 被抢占的旧可写客户端 mpA 后续发起写操作立即收到 POSIX EBUSY，且不会自动重连抢回 Epoch
		errStaleMkdir := os.Mkdir(filepath.Join(mpA, "fenced-dir"), 0o755)
		require.ErrorIs(t, errStaleMkdir, syscall.EBUSY, "被抢占的旧写端发起目录创建必须立即返回 EBUSY")

		errStaleWrite := os.WriteFile(filepath.Join(mpA, "fenced-file.txt"), []byte("stale"), 0o644)
		require.ErrorIs(t, errStaleWrite, syscall.EBUSY, "被抢占的旧写端发起文件写入必须立即返回 EBUSY")

		// 再次重试旧写端依然返回 EBUSY，且新写端 mpB 写权限稳固未受影响（证明旧写端未重连抢回 Epoch）
		errStaleRetry := os.Mkdir(filepath.Join(mpA, "fenced-dir-retry"), 0o755)
		require.ErrorIs(t, errStaleRetry, syscall.EBUSY, "旧写端不得自动重连抢回 Epoch")
		require.NoError(t, os.WriteFile(filepath.Join(mpB, "still-leader.txt"), []byte("持续持有写权限"), 0o644))
	})

	var remove1001Summary, rename1001Summary string
	t.Run("多用户权限", func(t *testing.T) {
		// 准备 1: root 属主 0600 文件 secret.txt
		secretPath := filepath.Join(mpB, "secret.txt")
		require.NoError(t, os.WriteFile(secretPath, []byte("ROOT_TOP_SECRET_DATA"), 0o600))

		// 准备 2: root 属主 0755 目录 readonly-dir
		roDirPath := filepath.Join(mpB, "readonly-dir")
		require.NoError(t, os.Mkdir(roDirPath, 0o755))

		// 准备 3: 01777 带 Sticky Bit 的共享目录 shared-tmp
		sharedTmpPath := filepath.Join(mpB, "shared-tmp")
		require.NoError(t, os.Mkdir(sharedTmpPath, 0o777|os.ModeSticky))
		require.NoError(t, unix.Chmod(sharedTmpPath, 0o1777), "设置 Sticky Bit (01777) 失败")

		// 3.1 uid=1000 读取或写入 secret.txt (0600) 均被拒绝并返回 EACCES
		codeRead, _, errRead := runAs(t, 1000, 1000, "cat", secretPath)
		require.Equal(t, 1, codeRead, "普通用户读取 root 0600 必须被拒绝: %s", errRead)
		require.Contains(t, errRead, "Permission denied")

		codeWrite, _, errWrite := runAsWithStdin(t, 1000, 1000, "unauthorized append\n", "tee", "-a", secretPath)
		require.Equal(t, 1, codeWrite, "普通用户写入 root 0600 必须被拒绝: %s", errWrite)
		require.Contains(t, errWrite, "Permission denied")

		// 3.2 uid=1000 在 readonly-dir (0755) 下创建文件被拒绝并返回 EACCES
		forbiddenFile := filepath.Join(roDirPath, "forbidden.txt")
		codeCreateRO, _, errCreateRO := runAs(t, 1000, 1000, "touch", forbiddenFile)
		require.Equal(t, 1, codeCreateRO, "普通用户在 0755 目录下创建文件必须被拒绝: %s", errCreateRO)
		require.Contains(t, errCreateRO, "Permission denied")

		// 3.3 uid=1000 在 shared-tmp (01777) 下成功创建文件 u1000.txt
		u1000File := filepath.Join(sharedTmpPath, "u1000.txt")
		codeCreateTmp, _, errCreateTmp := runAs(t, 1000, 1000, "touch", u1000File)
		require.Equal(t, 0, codeCreateTmp, "uid=1000 在 01777 共享目录下创建文件必须成功: %s", errCreateTmp)

		// 校验属主确为 uid=1000, gid=1000
		st1000, err := os.Stat(u1000File)
		require.NoError(t, err)
		sysStat, ok := st1000.Sys().(*syscall.Stat_t)
		require.True(t, ok)
		require.Equal(t, uint32(1000), sysStat.Uid, "文件属主 UID 必须为 1000")
		require.Equal(t, uint32(1000), sysStat.Gid, "文件属组 GID 必须为 1000")

		// 切换 uid=1001 尝试 os.Remove u1000.txt 被内核 Sticky Bit 规则拦截返回 EPERM
		codeRemove1001, _, errRemove1001 := runAs(t, 1001, 1001, "rm", u1000File)
		require.Equal(t, 1, codeRemove1001, "非属主删除 Sticky Bit 目录下文件必须被拦截: %s", errRemove1001)
		remove1001Summary = permissionErrorSummary(errRemove1001)
		require.Equal(t, "Operation not permitted", remove1001Summary, "非属主删除必须返回 EPERM: %s", errRemove1001)
		require.FileExists(t, u1000File, "被拦截后文件必须仍然存在")

		// 切换 uid=1001 尝试 os.Rename u1000.txt 被内核 Sticky Bit 规则拦截返回 EPERM
		u1001Renamed := filepath.Join(sharedTmpPath, "u1001_renamed.txt")
		codeRename1001, _, errRename1001 := runAs(t, 1001, 1001, "mv", u1000File, u1001Renamed)
		require.Equal(t, 1, codeRename1001, "非属主重命名 Sticky Bit 目录下文件必须被拦截: %s", errRename1001)
		rename1001Summary = permissionErrorSummary(errRename1001)
		require.Equal(t, "Operation not permitted", rename1001Summary, "非属主重命名必须返回 EPERM: %s", errRename1001)
		require.NoFileExists(t, u1001Renamed, "被拦截后不得出现重命名目标")
		require.FileExists(t, u1000File, "被拦截后源文件必须仍然存在")

		// uid=1000 自己可以正常删除 u1000.txt
		codeRemove1000, _, errRemove1000 := runAs(t, 1000, 1000, "rm", u1000File)
		require.Equal(t, 0, codeRemove1000, "属主删除自己创建的文件必须成功: %s", errRemove1000)
		_, err = os.Stat(u1000File)
		require.True(t, os.IsNotExist(err), "删除后文件必须不再存在")
	})

	recordArtifact(t, map[string]string{
		"readonly_epoch":        "0",
		"readonly_write":        "EROFS",
		"readonly_live_read":    "实时内容",
		"writer_takeover":       "新写端接管",
		"orphan_reclaimed":      strconv.FormatBool(orphanReclaimed),
		"preempted_writer":      "EBUSY",
		"no_reconnect_pingpong": "true",
		"uid1000_read_0600":     "EACCES",
		"uid1000_write_0600":    "EACCES",
		"uid1000_create_0755":   "EACCES",
		"uid1000_create_sticky": "ok",
		"uid1001_remove_sticky": remove1001Summary,
		"uid1001_rename_sticky": rename1001Summary,
		"uid1000_remove_own":    "ok",
	})
}
