//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/cli"
)

func evolutionCLI(args ...string) (exitCode int, stdoutStr, stderrStr string) {
	args = append(args, "--endpoint", getStorageEndpoint())
	var stdout, stderr bytes.Buffer
	code := cli.Execute(args, &stdout, &stderr)
	return code, stdout.String(), stderr.String()
}

func evolutionSuccess(t *testing.T, args ...string) string {
	t.Helper()
	code, out, errOut := evolutionCLI(args...)
	require.Equal(t, 0, code, "%v: %s\n%s", args, errOut, out)
	return out
}

func evolutionMount(t *testing.T, diskID, branch string) (mountpoint string, stop func()) {
	t.Helper()
	path := t.TempDir()
	cmd, _, stderr := startBowlMount(t, getStorageEndpoint(), diskID, path, "--branch", branch)
	var once sync.Once
	stop = func() { once.Do(func() { stopBowlMountGracefully(t, cmd, path) }) }
	t.Cleanup(stop)
	waitForMount(t, path, "挂载分支 %s 失败: %s", branch, stderr.String())
	return path, stop
}

func evolutionRead(t *testing.T, mountpoint, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(mountpoint, name))
	require.NoError(t, err)
	return string(data)
}

func evolutionRequireAbsent(t *testing.T, mountpoint, name string) {
	t.Helper()
	_, err := os.Stat(filepath.Join(mountpoint, name))
	require.ErrorIs(t, err, os.ErrNotExist)
}

// requireSameHardlink 断言两个路径指向同一 Inode，且链接数达到期望值。
func requireSameHardlink(t *testing.T, first, second string, wantLinks uint32) {
	t.Helper()
	var firstStat, secondStat unix.Stat_t
	require.NoError(t, unix.Stat(first, &firstStat))
	require.NoError(t, unix.Stat(second, &secondStat))
	require.Equal(t, firstStat.Ino, secondStat.Ino, "%s 与 %s 必须指向同一 Inode", first, second)
	require.Equal(t, wantLinks, firstStat.Nlink)
	require.Equal(t, wantLinks, secondStat.Nlink)
}

type branchWrite struct {
	path   string
	file   string
	data   string
	shared string
}

func writeBranch(item branchWrite) error {
	return errors.Join(
		os.WriteFile(filepath.Join(item.path, item.file), []byte(item.data), 0o644),
		os.WriteFile(filepath.Join(item.path, "shared.txt"), []byte(item.shared), 0o644),
	)
}

// writeBranchesConcurrently 并发写入两个分支的独立文件与共享文件。
func writeBranchesConcurrently(t *testing.T, first, second branchWrite) {
	t.Helper()
	errs := make(chan error, 2)
	go func() { errs <- writeBranch(first) }()
	go func() { errs <- writeBranch(second) }()
	require.NoError(t, <-errs)
	require.NoError(t, <-errs)
}

type branchSummary struct {
	Name   string `json:"name"`
	Parent string `json:"parent_branch"`
}

func branchParent(t *testing.T, branches []branchSummary, name string) string {
	t.Helper()
	index := slices.IndexFunc(branches, func(branch branchSummary) bool { return branch.Name == name })
	require.NotEqual(t, -1, index, "分支 %s 不存在", name)
	return branches[index].Parent
}

func hasBranch(branches []branchSummary, name string) bool {
	return slices.ContainsFunc(branches, func(branch branchSummary) bool { return branch.Name == name })
}

type diskListEntry struct {
	DiskID    string            `json:"disk_id"`
	UsedBytes uint64            `json:"used_bytes"`
	Branches  []json.RawMessage `json:"branches"`
}

type diskListPage struct {
	Disks         []diskListEntry `json:"disks"`
	NextPageToken string          `json:"next_page_token"`
}

// listDiskPage 执行一页 disk list 并解析磁盘与下一页游标。
func listDiskPage(t *testing.T, limit, pageToken string) diskListPage {
	t.Helper()
	var page diskListPage
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "disk", "list", "--json", "--limit", limit, "--page-token", pageToken)), &page))
	return page
}

// requireDiskListed 断言 disk list 包含目标磁盘的用量与分支数。
func requireDiskListed(t *testing.T, diskID string, wantUsed uint64, wantBranches int) {
	t.Helper()
	var listed diskListPage
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "disk", "list", "--json", "--limit", "1000")), &listed))
	index := slices.IndexFunc(listed.Disks, func(entry diskListEntry) bool { return entry.DiskID == diskID })
	require.NotEqual(t, -1, index, "磁盘列表必须包含新建磁盘")
	require.Equal(t, wantUsed, listed.Disks[index].UsedBytes)
	require.Len(t, listed.Disks[index].Branches, wantBranches)
}

type branchListPage struct {
	Branches []branchSummary `json:"branches"`
}

type diffOutput struct {
	Entries []struct {
		Path string `json:"path"`
	} `json:"entries"`
}

func requireDiffContains(t *testing.T, diskID, from, to, wantPath string) int {
	t.Helper()
	var diff diffOutput
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "diff", diskID, "--from", from, "--to", to, "--json")), &diff))
	require.True(t, slices.ContainsFunc(diff.Entries, func(entry struct {
		Path string `json:"path"`
	}) bool {
		return entry.Path == wantPath
	}), "diff 必须包含 %s", wantPath)
	return len(diff.Entries)
}

func TestE2E_BranchTreeEvolution(t *testing.T) {
	created := evolutionSuccess(t, "disk", "create", "--size", "1GiB", "--name", "branch-evolution", "--json")
	var disk struct {
		DiskID string `json:"disk_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(created), &disk))
	require.NotEmpty(t, disk.DiskID)
	diskID := disk.DiskID
	t.Cleanup(func() {
		code, out, errOut := evolutionCLI("disk", "delete", diskID, "--yes")
		require.Equal(t, 0, code, "%s\n%s", errOut, out)
	})

	mainPath, stopMain := evolutionMount(t, diskID, "main")
	require.NoError(t, os.WriteFile(filepath.Join(mainPath, "shared.txt"), []byte("origin"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mainPath, "baseline.txt"), []byte("baseline"), 0o644))
	commitJSON := evolutionSuccess(t, "commit", diskID, "--branch", "main", "-m", "init", "--json")
	var initial struct {
		CommitID string `json:"commit_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(commitJSON), &initial))
	require.NotEmpty(t, initial.CommitID)
	stopMain()

	evolutionSuccess(t, "branch", "create", diskID, "--name", "B", "--from", "main")
	evolutionSuccess(t, "branch", "create", diskID, "--name", "C", "--from", "B")
	evolutionSuccess(t, "branch", "create", diskID, "--name", "D", "--from", "B")
	evolutionSuccess(t, "branch", "create", diskID, "--name", "E", "--from", "B")

	var listedBranches branchListPage
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "branch", "ls", diskID, "--json")), &listedBranches))
	require.Len(t, listedBranches.Branches, 5)
	require.True(t, hasBranch(listedBranches.Branches, "main"))
	require.True(t, hasBranch(listedBranches.Branches, "B"))
	require.True(t, hasBranch(listedBranches.Branches, "C"))
	require.True(t, hasBranch(listedBranches.Branches, "D"))
	require.True(t, hasBranch(listedBranches.Branches, "E"))
	require.Equal(t, "main", branchParent(t, listedBranches.Branches, "B"))
	require.Equal(t, "B", branchParent(t, listedBranches.Branches, "C"))
	require.Equal(t, "B", branchParent(t, listedBranches.Branches, "D"))
	evolutionSuccess(t, "branch", "rm", diskID, "--name", "E", "--json")
	var afterRemoval branchListPage
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "branch", "ls", diskID, "--json")), &afterRemoval))
	require.False(t, hasBranch(afterRemoval.Branches, "E"), "branch rm 后 E 必须消失")

	var before struct {
		UsedBytes     uint64 `json:"used_bytes"`
		DefaultBranch string `json:"default_branch"`
	}
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "disk", "get", diskID, "--json")), &before))
	require.Equal(t, uint64(len("origin")+len("baseline")), before.UsedBytes)
	require.Equal(t, "main", before.DefaultBranch)

	cPath, stopC := evolutionMount(t, diskID, "C")
	dPath, stopD := evolutionMount(t, diskID, "D")
	writeBranchesConcurrently(t,
		branchWrite{path: cPath, file: "c.txt", data: "only C", shared: "from C"},
		branchWrite{path: dPath, file: "d.txt", data: "only D", shared: "from D"},
	)
	require.Equal(t, "from C", evolutionRead(t, cPath, "shared.txt"))
	require.Equal(t, "from D", evolutionRead(t, dPath, "shared.txt"))
	stopC()
	stopD()
	cCommit := evolutionSuccess(t, "commit", diskID, "--branch", "C", "--json")
	var cCommitInfo struct {
		CommitID string `json:"commit_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(cCommit), &cCommitInfo))
	dCommit := evolutionSuccess(t, "commit", diskID, "--branch", "D", "--json")
	var dCommitInfo struct {
		CommitID string `json:"commit_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(dCommit), &dCommitInfo))
	diffEntries := requireDiffContains(t, diskID, initial.CommitID, cCommitInfo.CommitID, "c.txt")
	require.Equal(t, 2, diffEntries, "C 分支相对初始快照必须包含 c.txt 与 shared.txt 两项差异")
	diffText := evolutionSuccess(t, "diff", diskID, "--from", initial.CommitID, "--to", cCommitInfo.CommitID)
	require.Contains(t, diffText, "+only C", "diff 必须展示 c.txt 的新增行内容")
	require.Contains(t, diffText, "-origin", "diff 必须展示 shared.txt 被替换的旧行内容")
	require.Contains(t, diffText, "+from C", "diff 必须展示 shared.txt 的新行内容")

	var after struct {
		UsedBytes uint64 `json:"used_bytes"`
		Branches  []branchSummary
	}
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "disk", "get", diskID, "--json")), &after))
	require.Equal(t, before.UsedBytes+uint64(len("only C")+len("only D")), after.UsedBytes)
	require.Len(t, after.Branches, 4)
	require.Equal(t, "main", branchParent(t, after.Branches, "B"))
	require.Equal(t, "B", branchParent(t, after.Branches, "C"))
	require.Equal(t, "B", branchParent(t, after.Branches, "D"))
	requireDiskListed(t, diskID, after.UsedBytes, 4)

	evolutionSuccess(t, "merge", diskID, "--from", "C", "--into", "B")
	code, conflict, errOut := evolutionCLI("merge", diskID, "--from", "D", "--into", "B")
	require.Equal(t, 1, code, errOut)
	require.Contains(t, conflict, "shared.txt")
	conflictPath, stopConflict := evolutionMount(t, diskID, "B")
	require.Equal(t, "from C", evolutionRead(t, conflictPath, "shared.txt"))
	require.Equal(t, "only C", evolutionRead(t, conflictPath, "c.txt"))
	evolutionRequireAbsent(t, conflictPath, "d.txt")
	stopConflict()
	evolutionSuccess(t, "merge", diskID, "--from", "D", "--into", "B", "--strategy", "theirs")
	evolutionSuccess(t, "merge", diskID, "--from", "B", "--into", "main")

	bPath, stopB := evolutionMount(t, diskID, "B")
	require.Equal(t, "from D", evolutionRead(t, bPath, "shared.txt"))
	require.Equal(t, "only C", evolutionRead(t, bPath, "c.txt"))
	require.Equal(t, "only D", evolutionRead(t, bPath, "d.txt"))
	code, _, errOut = evolutionCLI("rollback", diskID, "--branch", "B", "--to", initial.CommitID)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "EBUSY")
	stopB()
	evolutionSuccess(t, "rollback", diskID, "--branch", "B", "--to", initial.CommitID)
	bPath, stopB = evolutionMount(t, diskID, "B")
	bAfterRevert := evolutionRead(t, bPath, "shared.txt")
	require.Equal(t, "origin", bAfterRevert)
	require.Equal(t, "baseline", evolutionRead(t, bPath, "baseline.txt"))
	evolutionRequireAbsent(t, bPath, "c.txt")
	evolutionRequireAbsent(t, bPath, "d.txt")
	stopB()
	mainPath, stopMain = evolutionMount(t, diskID, "main")
	mainShared := evolutionRead(t, mainPath, "shared.txt")
	require.Equal(t, "from D", mainShared)
	mainC := evolutionRead(t, mainPath, "c.txt")
	require.Equal(t, "only C", mainC)
	mainD := evolutionRead(t, mainPath, "d.txt")
	require.Equal(t, "only D", mainD)
	sharedDigest := fileSHA256(t, filepath.Join(mainPath, "shared.txt"))
	stopMain()

	branchesJSON, err := json.Marshal(after.Branches)
	require.NoError(t, err)
	recordArtifact(t, map[string]string{
		"used_bytes":            strconv.FormatUint(after.UsedBytes, 10),
		"branches":              string(branchesJSON),
		"diff_entries":          strconv.Itoa(diffEntries),
		"diff_added_line":       "+only C",
		"diff_removed_line":     "-origin",
		"merge_conflict":        strconv.FormatBool(strings.Contains(conflict, "shared.txt")),
		"rollback_blocked":      strings.TrimSpace(errOut),
		"main_shared_sha256":    sharedDigest,
		"main_shared_content":   mainShared,
		"main_c_file_present":   strconv.FormatBool(mainC == "only C"),
		"main_d_file_present":   strconv.FormatBool(mainD == "only D"),
		"branch_b_after_revert": bAfterRevert,
	})
}

func TestE2E_BranchMergeContinueAbortAndInvalidArgs(t *testing.T) {
	created := evolutionSuccess(t, "disk", "create", "--size", "1GiB", "--name", "merge-recovery", "--json")
	var disk struct {
		DiskID string `json:"disk_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(created), &disk))
	diskID := disk.DiskID
	t.Cleanup(func() {
		code, out, errOut := evolutionCLI("disk", "delete", diskID, "--yes")
		require.Equal(t, 0, code, "%s\n%s", errOut, out)
	})

	mainPath, stopMain := evolutionMount(t, diskID, "main")
	require.NoError(t, os.WriteFile(filepath.Join(mainPath, "conflict.txt"), []byte("base"), 0o644))
	evolutionSuccess(t, "commit", diskID, "--branch", "main", "-m", "base")
	stopMain()
	evolutionSuccess(t, "branch", "create", diskID, "--name", "B", "--from", "main")
	evolutionSuccess(t, "branch", "create", diskID, "--name", "C", "--from", "B")

	cPath, stopC := evolutionMount(t, diskID, "C")
	require.NoError(t, os.WriteFile(filepath.Join(cPath, "conflict.txt"), []byte("theirs"), 0o644))
	stopC()
	evolutionSuccess(t, "commit", diskID, "--branch", "C", "-m", "theirs")
	bPath, stopB := evolutionMount(t, diskID, "B")
	require.NoError(t, os.WriteFile(filepath.Join(bPath, "conflict.txt"), []byte("ours"), 0o644))
	stopB()
	evolutionSuccess(t, "commit", diskID, "--branch", "B", "-m", "ours")

	code, out, errOut := evolutionCLI("merge", diskID, "--from", "C", "--into", "B", "--strategy", "manual")
	require.Equal(t, 0, code, "%s\n%s", errOut, out)
	require.Contains(t, out, "conflict.txt")
	evolutionSuccess(t, "merge", diskID, "--into", "B", "--abort")
	bPath, stopB = evolutionMount(t, diskID, "B")
	abortRestored := evolutionRead(t, bPath, "conflict.txt")
	require.Equal(t, "ours", abortRestored)
	evolutionRequireAbsent(t, bPath, "conflict.txt.theirs")
	evolutionRequireAbsent(t, bPath, "conflict.txt.base")
	stopB()

	evolutionSuccess(t, "merge", diskID, "--from", "C", "--into", "B", "--strategy", "manual")
	bPath, stopB = evolutionMount(t, diskID, "B")
	require.NoError(t, os.WriteFile(filepath.Join(bPath, "conflict.txt"), []byte("resolved"), 0o644))
	require.NoError(t, os.Remove(filepath.Join(bPath, "conflict.txt.theirs")))
	require.NoError(t, os.Remove(filepath.Join(bPath, "conflict.txt.base")))
	stopB()
	continueJSON := evolutionSuccess(t, "merge", diskID, "--into", "B", "--continue", "-m", "resolved", "--json")
	var continued struct {
		CommitID string `json:"commit_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(continueJSON), &continued))
	require.NotEmpty(t, continued.CommitID)
	bPath, stopB = evolutionMount(t, diskID, "B")
	resolved := evolutionRead(t, bPath, "conflict.txt")
	require.Equal(t, "resolved", resolved)
	stopB()

	code, _, _ = evolutionCLI("merge", diskID, "--into", "B", "--continue", "--from", "C")
	require.Equal(t, 2, code, "--continue 与 --from 不能同时使用")
	code, _, _ = evolutionCLI("merge", diskID, "--into", "B", "--message", "x")
	require.Equal(t, 2, code, "--message 只能与 --continue 一起使用")
	code, _, _ = evolutionCLI("branch", "create", diskID, "--name", "bad/name")
	require.Equal(t, 2, code, "非法分支名必须被 CLI 拒绝")
	code, _, _ = evolutionCLI("branch", "ls", "not-a-uuid")
	require.Equal(t, 2, code, "非法 disk-id 必须被 CLI 拒绝")
	code, _, _ = evolutionCLI("diff", diskID, "--from", "", "--to", "main")
	require.Equal(t, 2, code, "空 diff 版本引用必须被 CLI 拒绝")

	recordArtifact(t, map[string]string{
		"abort_restored":           abortRestored,
		"continued_commit_created": strconv.FormatBool(continued.CommitID != ""),
		"resolved_content":         resolved,
	})
}

func TestE2E_BranchMergeDirectoryRenamePreservesHardlink(t *testing.T) {
	created := evolutionSuccess(t, "disk", "create", "--size", "1GiB", "--name", "merge-hardlink", "--json")
	var disk struct {
		DiskID string `json:"disk_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(created), &disk))
	diskID := disk.DiskID
	require.NotEmpty(t, diskID)
	t.Cleanup(func() {
		code, out, errOut := evolutionCLI("disk", "delete", diskID, "--yes")
		require.Equal(t, 0, code, "%s\n%s", errOut, out)
	})

	mainPath, stopMain := evolutionMount(t, diskID, "main")
	require.NoError(t, os.Mkdir(filepath.Join(mainPath, "dir"), 0o755))
	sourcePath := filepath.Join(mainPath, "dir", "file.txt")
	linkPath := filepath.Join(mainPath, "dir", "file-link.txt")
	require.NoError(t, os.WriteFile(sourcePath, []byte("hardlink-payload"), 0o644))
	require.NoError(t, os.Link(sourcePath, linkPath))
	requireSameHardlink(t, sourcePath, linkPath, 2)
	evolutionSuccess(t, "commit", diskID, "--branch", "main", "-m", "base")
	stopMain()

	evolutionSuccess(t, "branch", "create", diskID, "--name", "B", "--from", "main")
	bPath, stopB := evolutionMount(t, diskID, "B")
	require.NoError(t, os.Rename(filepath.Join(bPath, "dir"), filepath.Join(bPath, "moved")))
	require.Equal(t, "hardlink-payload", evolutionRead(t, bPath, "moved/file.txt"))
	evolutionSuccess(t, "commit", diskID, "--branch", "B", "-m", "rename")
	stopB()

	evolutionSuccess(t, "merge", diskID, "--from", "B", "--into", "main")

	mainPath, stopMain = evolutionMount(t, diskID, "main")
	movedFile := filepath.Join(mainPath, "moved", "file.txt")
	movedLink := filepath.Join(mainPath, "moved", "file-link.txt")
	require.Equal(t, "hardlink-payload", evolutionRead(t, mainPath, "moved/file.txt"))
	require.Equal(t, "hardlink-payload", evolutionRead(t, mainPath, "moved/file-link.txt"))
	requireSameHardlink(t, movedFile, movedLink, 2)
	evolutionRequireAbsent(t, mainPath, "dir")
	digest := fileSHA256(t, movedFile)
	stopMain()

	recordArtifact(t, map[string]string{
		"directory_renamed": "dir->moved",
		"hardlink_nlink":    "2",
		"hardlink_sha256":   digest,
	})
}

func TestE2E_BranchMergeDirectoryMoveConflictRecovery(t *testing.T) {
	created := evolutionSuccess(t, "disk", "create", "--size", "1GiB", "--name", "merge-move-conflict", "--json")
	var disk struct {
		DiskID string `json:"disk_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(created), &disk))
	diskID := disk.DiskID
	require.NotEmpty(t, diskID)
	t.Cleanup(func() {
		code, out, errOut := evolutionCLI("disk", "delete", diskID, "--yes")
		require.Equal(t, 0, code, "%s\n%s", errOut, out)
	})

	mainPath, stopMain := evolutionMount(t, diskID, "main")
	require.NoError(t, os.Mkdir(filepath.Join(mainPath, "dir"), 0o755))
	conflictPath := filepath.Join(mainPath, "dir", "conflict.txt")
	linkPath := filepath.Join(mainPath, "dir", "link.txt")
	require.NoError(t, os.WriteFile(conflictPath, []byte("base"), 0o644))
	require.NoError(t, os.Link(conflictPath, linkPath))
	requireSameHardlink(t, conflictPath, linkPath, 2)
	evolutionSuccess(t, "commit", diskID, "--branch", "main", "-m", "base")
	stopMain()

	evolutionSuccess(t, "branch", "create", diskID, "--name", "B", "--from", "main")
	evolutionSuccess(t, "branch", "create", diskID, "--name", "C", "--from", "main")

	bPath, stopB := evolutionMount(t, diskID, "B")
	require.NoError(t, os.Rename(filepath.Join(bPath, "dir"), filepath.Join(bPath, "moved")))
	require.NoError(t, os.WriteFile(filepath.Join(bPath, "moved", "conflict.txt"), []byte("theirs"), 0o644))
	evolutionSuccess(t, "commit", diskID, "--branch", "B", "-m", "rename-and-edit")
	stopB()

	cPath, stopC := evolutionMount(t, diskID, "C")
	require.NoError(t, os.WriteFile(filepath.Join(cPath, "dir", "conflict.txt"), []byte("ours"), 0o644))
	evolutionSuccess(t, "commit", diskID, "--branch", "C", "-m", "edit")
	stopC()

	code, out, errOut := evolutionCLI("merge", diskID, "--from", "B", "--into", "C", "--strategy", "manual")
	require.Equal(t, 0, code, "%s\n%s", errOut, out)
	require.Contains(t, out, "moved/conflict.txt")

	cPath, stopC = evolutionMount(t, diskID, "C")
	require.Equal(t, "ours", evolutionRead(t, cPath, "moved/conflict.txt"))
	require.Equal(t, "ours", evolutionRead(t, cPath, "moved/link.txt"))
	requireSameHardlink(t, filepath.Join(cPath, "moved", "conflict.txt"), filepath.Join(cPath, "moved", "link.txt"), 2)
	require.Equal(t, "theirs", evolutionRead(t, cPath, "moved/conflict.txt.theirs"))
	require.Equal(t, "base", evolutionRead(t, cPath, "moved/conflict.txt.base"))
	evolutionRequireAbsent(t, cPath, "dir")
	stopC()

	evolutionSuccess(t, "merge", diskID, "--into", "C", "--abort")

	cPath, stopC = evolutionMount(t, diskID, "C")
	require.Equal(t, "ours", evolutionRead(t, cPath, "dir/conflict.txt"))
	require.Equal(t, "ours", evolutionRead(t, cPath, "dir/link.txt"))
	requireSameHardlink(t, filepath.Join(cPath, "dir", "conflict.txt"), filepath.Join(cPath, "dir", "link.txt"), 2)
	evolutionRequireAbsent(t, cPath, "moved")
	stopC()

	evolutionSuccess(t, "merge", diskID, "--from", "B", "--into", "C", "--strategy", "manual")
	cPath, stopC = evolutionMount(t, diskID, "C")
	require.NoError(t, os.WriteFile(filepath.Join(cPath, "moved", "conflict.txt"), []byte("resolved"), 0o644))
	require.NoError(t, os.Remove(filepath.Join(cPath, "moved", "conflict.txt.theirs")))
	require.NoError(t, os.Remove(filepath.Join(cPath, "moved", "conflict.txt.base")))
	require.Equal(t, "resolved", evolutionRead(t, cPath, "moved/link.txt"))
	stopC()

	continueJSON := evolutionSuccess(t, "merge", diskID, "--into", "C", "--continue", "-m", "resolved", "--json")
	var continued struct {
		CommitID string `json:"commit_id"`
	}
	require.NoError(t, json.Unmarshal([]byte(continueJSON), &continued))
	require.NotEmpty(t, continued.CommitID)

	cPath, stopC = evolutionMount(t, diskID, "C")
	require.Equal(t, "resolved", evolutionRead(t, cPath, "moved/conflict.txt"))
	require.Equal(t, "resolved", evolutionRead(t, cPath, "moved/link.txt"))
	requireSameHardlink(t, filepath.Join(cPath, "moved", "conflict.txt"), filepath.Join(cPath, "moved", "link.txt"), 2)
	evolutionRequireAbsent(t, cPath, "dir")
	digest := fileSHA256(t, filepath.Join(cPath, "moved", "conflict.txt"))
	stopC()

	recordArtifact(t, map[string]string{
		"materialized_conflict":  "moved/conflict.txt",
		"abort_restored_dir":     "ours",
		"continued_commit_ready": strconv.FormatBool(continued.CommitID != ""),
		"resolved_content":       "resolved",
		"hardlink_nlink":         "2",
		"resolved_sha256":        digest,
	})
}
