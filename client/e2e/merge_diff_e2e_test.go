//go:build e2e && linux

package e2e_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestClientE2E_ForkDiffPatchMergeAndFUSEMountVerification(t *testing.T) {
	// Given: 通过 FUSE 挂载主盘写入基线文件 -> 卸载并 Fork 子盘 -> 双边修改制造冲突并各自新增文件
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	mainDisk := createTestDisk(t, client)
	mainID := mainDisk.GetDiskId()

	mpMain1, stopMain1 := mountWithCleanup(t, endpoint, mainID)
	require.NoError(t, os.WriteFile(filepath.Join(mpMain1, "shared.txt"), []byte("line1\nbase\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpMain1, "base.txt"), []byte("keep\n"), 0o644))
	stopMain1()

	featOut := runBowlSuccess(t, "disk", "fork", mainID, "--endpoint", endpoint, "--name", "e2e-feat-branch", "-q")
	featID := featOut[:len(featOut)-1]
	registerDiskCleanup(t, client, featID)

	mpMain2, stopMain2 := mountWithCleanup(t, endpoint, mainID)
	require.NoError(t, os.WriteFile(filepath.Join(mpMain2, "shared.txt"), []byte("line1\nmain\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpMain2, "main-only.txt"), []byte("m\n"), 0o644))
	stopMain2()

	mpFeat, stopFeat := mountWithCleanup(t, endpoint, featID)
	require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "shared.txt"), []byte("line1\nfeat\n"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mpFeat, "feat-only.txt"), []byte("f\n"), 0o644))
	stopFeat()

	// When 1: `bowl disk diff --target --patch` 检出差异、冲突标记与统一补丁输出
	diffOut := runBowlSuccess(t, "disk", "diff", featID, "--endpoint", endpoint, "--target", mainID, "--patch")
	require.Contains(t, diffOut, "shared.txt")
	require.Contains(t, diffOut, "CONTENT")
	require.Contains(t, diffOut, "+feat")

	// When 2: `bowl disk merge` 默认 Fail 策略触发 ECONFLICT 并返回退出码 1
	conflictOut := runBowlExpectExit(t, 1, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID)
	require.Contains(t, conflictOut, "ECONFLICT")
	require.Contains(t, conflictOut, "shared.txt")

	// When 3: `bowl disk merge --dry-run` 预演通过
	dryOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID, "--strategy=ours", "--dry-run")
	require.Contains(t, dryOut, "Dry Run")

	// When 4: 使用 `--resolve /shared.txt=theirs` + `--strategy=ours` 正式合并
	mergeOut := runBowlSuccess(t, "disk", "merge", featID, "--endpoint", endpoint, "--into", mainID, "--strategy=ours", "--resolve", "/shared.txt=theirs")
	require.Contains(t, mergeOut, mainID)

	// Then: 重新挂载主盘，POSIX 文件读取确认保留 main-only.txt、新增 feat-only.txt 且 shared.txt 采用 theirs
	mpMain3, stopMain3 := mountWithCleanup(t, endpoint, mainID)
	defer stopMain3()

	gotShared, err := os.ReadFile(filepath.Join(mpMain3, "shared.txt"))
	require.NoError(t, err)
	require.Equal(t, "line1\nfeat\n", string(gotShared))

	gotFeat, err := os.ReadFile(filepath.Join(mpMain3, "feat-only.txt"))
	require.NoError(t, err)
	require.Equal(t, "f\n", string(gotFeat))

	gotMainOnly, err := os.ReadFile(filepath.Join(mpMain3, "main-only.txt"))
	require.NoError(t, err)
	require.Equal(t, "m\n", string(gotMainOnly))
}
