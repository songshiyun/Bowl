//go:build e2e && linux

package e2e_test

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
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

// TestE2E_MultiClient_ComprehensiveAcceptance 验证分支单写多读与快照隔离。
func TestE2E_MultiClient_ComprehensiveAcceptance(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()
	endpoint := getStorageEndpoint()
	mpA := t.TempDir()
	require.NoError(t, os.Chmod(filepath.Dir(mpA), 0o755), "测试临时目录父级必须可被普通用户穿透")
	require.NoError(t, os.Chmod(mpA, 0o755), "挂载点必须可被普通用户穿透")
	cmdA, _, stderrA := startBowlMount(t, endpoint, diskID, mpA, "--allow-other", "--branch", "main")
	waitForMount(t, mpA, "挂载 main 分支失败: %s", stderrA.String())
	defer stopBowlMountGracefully(t, cmdA, mpA)
	require.NotEmpty(t, disk.GetBranches())
	pathA := filepath.Join(mpA, "snapshot.txt")
	require.NoError(t, os.WriteFile(pathA, []byte("版本一"), 0o644))

	t.Run("同一分支拒绝第二个写会话", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		mp := t.TempDir()
		cmd := exec.CommandContext(ctx, getBowlBin(), "mount", "-f", "--endpoint", endpoint, diskID, mp, "--branch", "main")
		out, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr, string(out))
		require.Equal(t, 1, exitErr.ExitCode(), string(out))
		require.Contains(t, string(out), "busy")
		require.False(t, isMounted(mp))
	})

	t.Run("无只读标志的提交挂载被拒绝", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, getBowlBin(), "mount", "-f", "--endpoint", endpoint, diskID, t.TempDir(), "--commit", disk.GetBranches()[0].GetHeadCommitId())
		out, err := cmd.CombinedOutput()
		var exitErr *exec.ExitError
		require.ErrorAs(t, err, &exitErr, string(out))
		require.Equal(t, 2, exitErr.ExitCode(), string(out))
	})

	t.Run("只读分支固定挂载时的快照", func(t *testing.T) {
		mp := t.TempDir()
		cmd, _, stderr := startBowlMount(t, endpoint, diskID, mp, "--ro", "--branch", "main")
		waitForMount(t, mp, "只读挂载失败: %s", stderr.String())
		defer stopBowlMountGracefully(t, cmd, mp)
		readonlyPath := filepath.Join(mp, "snapshot.txt")
		data, err := os.ReadFile(readonlyPath)
		require.NoError(t, err)
		require.Equal(t, "版本一", string(data))
		require.NoError(t, os.WriteFile(pathA, []byte("版本二"), 0o644))
		data, err = os.ReadFile(readonlyPath)
		require.NoError(t, err)
		require.Equal(t, "版本一", string(data))
		require.NoError(t, os.WriteFile(pathA, []byte("版本一"), 0o644))
	})

	t.Run("只读快照和独立可写分支", func(t *testing.T) {
		commitID := commitMain(t, client, diskID)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: diskID, Name: "feature-x", SourceCommitId: &commitID}))
		require.NoError(t, err)

		mpRO := t.TempDir()
		cmdRO, _, _ := startBowlMount(t, endpoint, diskID, mpRO, "--ro", "--branch", "main")
		waitForMount(t, mpRO)
		defer stopBowlMountGracefully(t, cmdRO, mpRO)
		data, err := os.ReadFile(filepath.Join(mpRO, "snapshot.txt"))
		require.NoError(t, err)
		require.Equal(t, "版本一", string(data))
		require.ErrorIs(t, os.WriteFile(filepath.Join(mpRO, "new.txt"), nil, 0o644), syscall.EROFS)

		mpFeature := t.TempDir()
		cmdFeature, _, _ := startBowlMount(t, endpoint, diskID, mpFeature, "--branch", "feature-x")
		waitForMount(t, mpFeature)
		defer stopBowlMountGracefully(t, cmdFeature, mpFeature)
		require.NoError(t, os.WriteFile(filepath.Join(mpFeature, "feature.txt"), []byte("分支"), 0o644))
		_, err = os.Stat(filepath.Join(mpA, "feature.txt"))
		require.ErrorIs(t, err, os.ErrNotExist)

		mpCommit := t.TempDir()
		cmdCommit, _, _ := startBowlMount(t, endpoint, diskID, mpCommit, "--ro", "--commit", commitID)
		waitForMount(t, mpCommit)
		defer stopBowlMountGracefully(t, cmdCommit, mpCommit)
		require.NoError(t, os.WriteFile(pathA, []byte("版本二"), 0o644))
		require.NotEqual(t, commitID, commitMain(t, client, diskID))
		requireFileContent(t, filepath.Join(mpRO, "snapshot.txt"), "版本一")
		requireFileContent(t, filepath.Join(mpCommit, "snapshot.txt"), "版本一")
	})

	var remove1001Summary, rename1001Summary string
	t.Run("多用户权限", func(t *testing.T) {
		// 准备 1: root 属主 0600 文件 secret.txt
		secretPath := filepath.Join(mpA, "secret.txt")
		require.NoError(t, os.WriteFile(secretPath, []byte("ROOT_TOP_SECRET_DATA"), 0o600))

		// 准备 2: root 属主 0755 目录 readonly-dir
		roDirPath := filepath.Join(mpA, "readonly-dir")
		require.NoError(t, os.Mkdir(roDirPath, 0o755))

		// 准备 3: 01777 带 Sticky Bit 的共享目录 shared-tmp
		sharedTmpPath := filepath.Join(mpA, "shared-tmp")
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
		"second_writer":          "EBUSY",
		"commit_without_ro":      "exit 2",
		"snapshot_isolation":     "版本一",
		"readonly_write":         "EROFS",
		"feature_branch_visible": "false",
		"uid1000_read_0600":      "EACCES",
		"uid1000_write_0600":     "EACCES",
		"uid1000_create_0755":    "EACCES",
		"uid1000_create_sticky":  "ok",
		"uid1001_remove_sticky":  remove1001Summary,
		"uid1001_rename_sticky":  rename1001Summary,
		"uid1000_remove_own":     "ok",
	})
}
