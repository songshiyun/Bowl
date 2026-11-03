//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const multiUserHelperEnv = "BOWL_TEST_MULTI_USER_HELPER"

// TestMultiUserHelperProcess 作为非 root 普通用户凭据（uid/gid）派生的测试辅助进程。
func TestMultiUserHelperProcess(t *testing.T) {
	if os.Getenv(multiUserHelperEnv) != "1" {
		return
	}

	args := os.Args
	var subArgs []string
	for i, arg := range args {
		if arg == "--" {
			subArgs = args[i+1:]
			break
		}
	}

	if len(subArgs) < 2 {
		fmt.Fprintf(os.Stderr, "缺少参数: %v\n", subArgs)
		os.Exit(99)
	}

	op := subArgs[0]
	target := subArgs[1]

	handleErr := func(err error) {
		if err == nil {
			fmt.Println("SUCCESS")
			os.Exit(0)
		}
		if errors.Is(err, syscall.EACCES) {
			fmt.Fprintf(os.Stderr, "ERR_EACCES: %v\n", err)
			os.Exit(10)
		}
		if errors.Is(err, syscall.EPERM) {
			fmt.Fprintf(os.Stderr, "ERR_EPERM: %v\n", err)
			os.Exit(11)
		}
		fmt.Fprintf(os.Stderr, "ERR_OTHER: %v\n", err)
		os.Exit(12)
	}

	switch op {
	case "read":
		_, err := os.ReadFile(target)
		handleErr(err)
	case "write":
		f, err := os.OpenFile(target, os.O_WRONLY|os.O_APPEND, 0644)
		if err == nil {
			_, err = f.Write([]byte("unauthorized append\n"))
			_ = f.Close()
		}
		handleErr(err)
	case "create":
		f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err == nil {
			_, err = f.Write([]byte("created-by-helper\n"))
			_ = f.Close()
		}
		handleErr(err)
	case "remove":
		err := os.Remove(target)
		handleErr(err)
	case "rename":
		if len(subArgs) < 3 {
			fmt.Fprintln(os.Stderr, "rename 需要新目标路径")
			os.Exit(99)
		}
		newTarget := subArgs[2]
		err := os.Rename(target, newTarget)
		handleErr(err)
	default:
		fmt.Fprintf(os.Stderr, "未知操作: %s\n", op)
		os.Exit(99)
	}
	os.Exit(0)
}

// grantAncestorTraverseForTest 确保位于 os.TempDir() 之下的测试专属临时目录层级的权限可被普通用户穿透，
// 绝不触碰 os.TempDir()（如 /tmp）及其上层系统目录；若修改权限则保留特殊位并通过 t.Cleanup 精确还原原始 mode。
func grantAncestorTraverseForTest(t *testing.T, targetPath string) {
	t.Helper()

	tempRoot := filepath.Clean(os.TempDir())
	for p := filepath.Clean(targetPath); p != tempRoot && p != "/" && p != "."; p = filepath.Dir(p) {
		info, err := os.Stat(p)
		if err != nil {
			continue
		}
		origMode := info.Mode()
		if origMode.Perm()&0o111 != 0o111 {
			dirPath := p
			require.NoError(t, os.Chmod(dirPath, origMode|0o111), "赋予测试临时目录穿透权限失败: %s", dirPath)
			t.Cleanup(func() {
				if _, statErr := os.Stat(dirPath); statErr == nil {
					_ = os.Chmod(dirPath, origMode)
				}
			})
		}
	}
}

// runMultiUserHelperCmd 执行带有真实非 root 普通用户凭据的子进程操作。
func runMultiUserHelperCmd(t *testing.T, uid, gid uint32, op string, args ...string) (int, string, string) {
	t.Helper()

	// 确保测试二进制及其位于 os.TempDir() 之下的父目录可被普通用户读取与执行
	grantAncestorTraverseForTest(t, os.Args[0])

	cmdArgs := []string{"-test.run=^TestMultiUserHelperProcess$", "--", op}
	cmdArgs = append(cmdArgs, args...)

	cmd := exec.Command(os.Args[0], cmdArgs...)
	cmd.Env = append(os.Environ(), multiUserHelperEnv+"=1")
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid: uid,
			Gid: gid,
		},
	}

	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	exitCode := 0
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			exitCode = exitErr.ExitCode()
		} else {
			t.Fatalf("运行多用户辅助进程失败: %v", err)
		}
	}
	return exitCode, stdout.String(), stderr.String()
}

// TestE2E_MultiClient_ComprehensiveAcceptance 验证分支单写多读与快照隔离。
func TestE2E_MultiClient_ComprehensiveAcceptance(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	diskID := disk.GetDiskId()
	endpoint := getStorageEndpoint()
	mpA := t.TempDir()
	grantAncestorTraverseForTest(t, mpA)
	cmdA, _, stderrA := startBowlMount(t, endpoint, diskID, mpA, "--allow-other", "--branch", "main")
	if err := waitForMount(mpA, 10*time.Second); err != nil {
		_ = cmdA.Process.Kill()
		_ = cmdA.Wait()
		t.Fatalf("挂载 main 分支失败: %v: %s", err, stderrA.String())
	}
	defer stopBowlMountGracefully(t, cmdA, mpA, 5*time.Second)
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

	t.Run("只读分支使用创建时的初始快照", func(t *testing.T) {
		mp := t.TempDir()
		cmd, _, stderr := startBowlMount(t, endpoint, diskID, mp, "--ro", "--branch", "main")
		if err := waitForMount(mp, 10*time.Second); err != nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			t.Fatalf("只读挂载失败: %v: %s", err, stderr.String())
		}
		defer stopBowlMountGracefully(t, cmd, mp, 5*time.Second)
		_, err := os.Stat(filepath.Join(mp, "snapshot.txt"))
		require.ErrorIs(t, err, os.ErrNotExist)
	})

	t.Run("只读快照和独立可写分支", func(t *testing.T) {
		commitID := commitMain(t, client, diskID)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: diskID, Name: "feature-x", SourceCommitId: &commitID}))
		require.NoError(t, err)

		mpRO := t.TempDir()
		cmdRO, _, _ := startBowlMount(t, endpoint, diskID, mpRO, "--ro", "--branch", "main")
		require.NoError(t, waitForMount(mpRO, 10*time.Second))
		defer stopBowlMountGracefully(t, cmdRO, mpRO, 5*time.Second)
		data, err := os.ReadFile(filepath.Join(mpRO, "snapshot.txt"))
		require.NoError(t, err)
		require.Equal(t, "版本一", string(data))
		require.ErrorIs(t, os.WriteFile(filepath.Join(mpRO, "new.txt"), nil, 0o644), syscall.EROFS)

		mpFeature := t.TempDir()
		cmdFeature, _, _ := startBowlMount(t, endpoint, diskID, mpFeature, "--branch", "feature-x")
		require.NoError(t, waitForMount(mpFeature, 10*time.Second))
		defer stopBowlMountGracefully(t, cmdFeature, mpFeature, 5*time.Second)
		require.NoError(t, os.WriteFile(filepath.Join(mpFeature, "feature.txt"), []byte("分支"), 0o644))
		_, err = os.Stat(filepath.Join(mpA, "feature.txt"))
		require.ErrorIs(t, err, os.ErrNotExist)

		mpCommit := t.TempDir()
		cmdCommit, _, _ := startBowlMount(t, endpoint, diskID, mpCommit, "--ro", "--commit", commitID)
		require.NoError(t, waitForMount(mpCommit, 10*time.Second))
		defer stopBowlMountGracefully(t, cmdCommit, mpCommit, 5*time.Second)
		require.NoError(t, os.WriteFile(pathA, []byte("版本二"), 0o644))
		require.NotEqual(t, commitID, commitMain(t, client, diskID))
		for _, mp := range []string{mpRO, mpCommit} {
			data, err = os.ReadFile(filepath.Join(mp, "snapshot.txt"))
			require.NoError(t, err)
			require.Equal(t, "版本一", string(data))
		}
	})

	t.Run("多用户权限", func(t *testing.T) {
		// 准备 1: root 属主 0600 文件 secret.txt
		secretPath := filepath.Join(mpA, "secret.txt")
		require.NoError(t, os.WriteFile(secretPath, []byte("ROOT_TOP_SECRET_DATA"), 0600))

		// 准备 2: root 属主 0755 目录 readonly-dir
		roDirPath := filepath.Join(mpA, "readonly-dir")
		require.NoError(t, os.Mkdir(roDirPath, 0755))

		// 准备 3: 01777 带 Sticky Bit 的共享目录 shared-tmp
		sharedTmpPath := filepath.Join(mpA, "shared-tmp")
		require.NoError(t, os.Mkdir(sharedTmpPath, 0777|os.ModeSticky))
		require.NoError(t, unix.Chmod(sharedTmpPath, 01777), "设置 Sticky Bit (01777) 失败")

		// 3.1 uid=1000 读取或写入 secret.txt (0600) 均被拒绝并返回 EACCES
		codeRead, _, errRead := runMultiUserHelperCmd(t, 1000, 1000, "read", secretPath)
		require.Equal(t, 10, codeRead, "普通用户读取 root 0600 必须被拒绝: %s", errRead)
		require.Contains(t, errRead, "ERR_EACCES")

		codeWrite, _, errWrite := runMultiUserHelperCmd(t, 1000, 1000, "write", secretPath)
		require.Equal(t, 10, codeWrite, "普通用户写入 root 0600 必须被拒绝: %s", errWrite)
		require.Contains(t, errWrite, "ERR_EACCES")

		// 3.2 uid=1000 在 readonly-dir (0755) 下创建文件被拒绝并返回 EACCES
		forbiddenFile := filepath.Join(roDirPath, "forbidden.txt")
		codeCreateRO, _, errCreateRO := runMultiUserHelperCmd(t, 1000, 1000, "create", forbiddenFile)
		require.Equal(t, 10, codeCreateRO, "普通用户在 0755 目录下创建文件必须被拒绝: %s", errCreateRO)
		require.Contains(t, errCreateRO, "ERR_EACCES")

		// 3.3 uid=1000 在 shared-tmp (01777) 下成功创建文件 u1000.txt
		u1000File := filepath.Join(sharedTmpPath, "u1000.txt")
		codeCreateTmp, _, errCreateTmp := runMultiUserHelperCmd(t, 1000, 1000, "create", u1000File)
		require.Equal(t, 0, codeCreateTmp, "uid=1000 在 01777 共享目录下创建文件必须成功: %s", errCreateTmp)

		// 校验属主确为 uid=1000, gid=1000
		st1000, err := os.Stat(u1000File)
		require.NoError(t, err)
		sysStat := st1000.Sys().(*syscall.Stat_t)
		require.Equal(t, uint32(1000), sysStat.Uid, "文件属主 UID 必须为 1000")
		require.Equal(t, uint32(1000), sysStat.Gid, "文件属组 GID 必须为 1000")

		// 切换 uid=1001 尝试 os.Remove u1000.txt 被内核 Sticky Bit 规则拦截返回 EPERM (或 EACCES)
		codeRemove1001, _, errRemove1001 := runMultiUserHelperCmd(t, 1001, 1001, "remove", u1000File)
		require.True(t, codeRemove1001 == 11 || codeRemove1001 == 10, "非属主删除 Sticky Bit 目录下文件必须被拦截: %s (code: %d)", errRemove1001, codeRemove1001)

		// 切换 uid=1001 尝试 os.Rename u1000.txt 被内核 Sticky Bit 规则拦截返回 EPERM (或 EACCES)
		u1001Renamed := filepath.Join(sharedTmpPath, "u1001_renamed.txt")
		codeRename1001, _, errRename1001 := runMultiUserHelperCmd(t, 1001, 1001, "rename", u1000File, u1001Renamed)
		require.True(t, codeRename1001 == 11 || codeRename1001 == 10, "非属主重命名 Sticky Bit 目录下文件必须被拦截: %s (code: %d)", errRename1001, codeRename1001)

		// uid=1000 自己可以正常删除 u1000.txt
		codeRemove1000, _, errRemove1000 := runMultiUserHelperCmd(t, 1000, 1000, "remove", u1000File)
		require.Equal(t, 0, codeRemove1000, "属主删除自己创建的文件必须成功: %s", errRemove1000)
		_, err = os.Stat(u1000File)
		require.True(t, os.IsNotExist(err), "删除后文件必须不再存在")
	})
}
