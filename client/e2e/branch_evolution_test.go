//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/cli"
)

func evolutionCLI(args ...string) (int, string, string) {
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

func evolutionMount(t *testing.T, diskID, branch string) (string, func()) {
	t.Helper()
	path := t.TempDir()
	cmd, _, stderr := startBowlMount(t, getStorageEndpoint(), diskID, path, "--branch", branch)
	if err := waitForMount(path, 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("挂载分支 %s 失败: %v: %s", branch, err, stderr.String())
	}
	var once sync.Once
	stop := func() { once.Do(func() { stopBowlMountGracefully(t, cmd, path, 5*time.Second) }) }
	t.Cleanup(stop)
	return path, stop
}

func evolutionRead(t *testing.T, mountpoint, name, expected string) {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(mountpoint, name))
	require.NoError(t, err)
	require.Equal(t, expected, string(data))
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

	for _, branch := range []struct{ name, from string }{{"B", "main"}, {"C", "B"}, {"D", "B"}} {
		evolutionSuccess(t, "branch", "create", diskID, "--name", branch.name, "--from", branch.from)
	}
	var before struct {
		UsedBytes     uint64 `json:"used_bytes"`
		DefaultBranch string `json:"default_branch"`
	}
	getJSON := evolutionSuccess(t, "disk", "get", diskID, "--json")
	require.NoError(t, json.Unmarshal([]byte(getJSON), &before))
	require.Equal(t, uint64(len("origin")+len("baseline")), before.UsedBytes)
	require.Equal(t, "main", before.DefaultBranch)

	cPath, stopC := evolutionMount(t, diskID, "C")
	dPath, stopD := evolutionMount(t, diskID, "D")
	writeErrors := make(chan error, 2)
	for _, item := range []struct{ path, file, data, shared string }{
		{cPath, "c.txt", "only C", "from C"},
		{dPath, "d.txt", "only D", "from D"},
	} {
		go func() {
			if err := os.WriteFile(filepath.Join(item.path, item.file), []byte(item.data), 0o644); err != nil {
				writeErrors <- err
				return
			}
			writeErrors <- os.WriteFile(filepath.Join(item.path, "shared.txt"), []byte(item.shared), 0o644)
		}()
	}
	timer := time.NewTimer(30 * time.Second)
	defer timer.Stop()
	var writeResults [2]error
	for i := range writeResults {
		select {
		case writeResults[i] = <-writeErrors:
		case <-timer.C:
			t.Fatal("并发写入未在 30 秒内完成")
		}
	}
	for _, err := range writeResults {
		require.NoError(t, err)
	}
	evolutionRead(t, cPath, "shared.txt", "from C")
	evolutionRead(t, dPath, "shared.txt", "from D")
	stopC()
	stopD()

	var after struct {
		UsedBytes uint64 `json:"used_bytes"`
		Branches  []struct {
			Name   string `json:"name"`
			Parent string `json:"parent_branch"`
		} `json:"branches"`
	}
	require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, "disk", "get", diskID, "--json")), &after))
	require.Equal(t, before.UsedBytes+uint64(len("only C")+len("only D")), after.UsedBytes)
	require.Len(t, after.Branches, 4)
	parents := make(map[string]string)
	for _, branch := range after.Branches {
		parents[branch.Name] = branch.Parent
	}
	require.Equal(t, "main", parents["B"])
	require.Equal(t, "B", parents["C"])
	require.Equal(t, "B", parents["D"])
	found := false
	pageToken := ""
	for {
		args := []string{"disk", "list", "--json", "--limit", "1000"}
		if pageToken != "" {
			args = append(args, "--page-token", pageToken)
		}
		var listed struct {
			Disks []struct {
				DiskID    string            `json:"disk_id"`
				UsedBytes uint64            `json:"used_bytes"`
				Branches  []json.RawMessage `json:"branches"`
			} `json:"disks"`
			NextPageToken string `json:"next_page_token"`
		}
		require.NoError(t, json.Unmarshal([]byte(evolutionSuccess(t, args...)), &listed))
		for _, item := range listed.Disks {
			if item.DiskID == diskID {
				found = true
				require.Equal(t, after.UsedBytes, item.UsedBytes)
				require.Len(t, item.Branches, 4)
			}
		}
		if found || listed.NextPageToken == "" {
			break
		}
		require.NotEqual(t, pageToken, listed.NextPageToken, "分页游标必须前进")
		pageToken = listed.NextPageToken
	}
	require.True(t, found, "磁盘列表必须包含新建磁盘")

	evolutionSuccess(t, "merge", diskID, "--from", "C", "--into", "B")
	code, conflict, errOut := evolutionCLI("merge", diskID, "--from", "D", "--into", "B")
	require.Equal(t, 1, code, errOut)
	require.Contains(t, conflict, "shared.txt")
	conflictPath, stopConflict := evolutionMount(t, diskID, "B")
	evolutionRead(t, conflictPath, "shared.txt", "from C")
	evolutionRead(t, conflictPath, "c.txt", "only C")
	_, err := os.Stat(filepath.Join(conflictPath, "d.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
	stopConflict()
	evolutionSuccess(t, "merge", diskID, "--from", "D", "--into", "B", "--strategy", "theirs")
	evolutionSuccess(t, "merge", diskID, "--from", "B", "--into", "main")

	bPath, stopB := evolutionMount(t, diskID, "B")
	evolutionRead(t, bPath, "shared.txt", "from D")
	evolutionRead(t, bPath, "c.txt", "only C")
	evolutionRead(t, bPath, "d.txt", "only D")
	code, _, errOut = evolutionCLI("rollback", diskID, "--branch", "B", "--to", initial.CommitID)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "EBUSY")
	stopB()
	evolutionSuccess(t, "rollback", diskID, "--branch", "B", "--to", initial.CommitID)
	bPath, stopB = evolutionMount(t, diskID, "B")
	evolutionRead(t, bPath, "shared.txt", "origin")
	evolutionRead(t, bPath, "baseline.txt", "baseline")
	_, err = os.Stat(filepath.Join(bPath, "c.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(bPath, "d.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)
	stopB()
	mainPath, stopMain = evolutionMount(t, diskID, "main")
	evolutionRead(t, mainPath, "shared.txt", "from D")
	evolutionRead(t, mainPath, "c.txt", "only C")
	evolutionRead(t, mainPath, "d.txt", "only D")
	stopMain()
}
