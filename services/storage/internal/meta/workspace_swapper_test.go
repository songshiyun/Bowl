package meta_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func assertNoTemporaryBranchDirs(t *testing.T, root, diskID string) {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(root, "disks", diskID, "branches"))
	require.NoError(t, err)
	for _, entry := range entries {
		name := entry.Name()
		require.False(t, strings.HasPrefix(name, ".backup-") || strings.HasPrefix(name, ".delete-") || strings.HasPrefix(name, ".restore-") || strings.HasPrefix(name, ".fork-") || strings.HasPrefix(name, ".merge-"),
			"不得残留临时分支目录 %s", name)
	}
}

// CreateBranch 因控制面分支树约束失败时，必须原子回滚新分支目录与 TreeQuota 用量，不泄露句柄或临时目录。
func TestWorkspaceSwapperCreateBranchRollbackConservation(t *testing.T) {
	e := setupPebbleCreate(t, 4096)
	file, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("seed"), Mode: 0o644, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: bytes.Repeat([]byte("a"), 1024), SessionID: &e.sid})
	require.NoError(t, err)
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	// 注入一个触发 CompareAndSetDiskBranches 检查约束（23514）的触发器，使持久化分支拓扑失败。
	_, err = e.pool.Exec(e.ctx, `CREATE OR REPLACE FUNCTION reject_branch_update() RETURNS trigger AS $$
	BEGIN
		IF jsonb_array_length(NEW.branches) > 1 THEN
			RAISE EXCEPTION 'simulated control plane failure' USING ERRCODE = '23514';
		END IF;
		RETURN NEW;
	END;
	$$ LANGUAGE plpgsql;
	CREATE TRIGGER trg_reject_branch_update BEFORE UPDATE OF branches ON disks FOR EACH ROW EXECUTE FUNCTION reject_branch_update();`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = e.pool.Exec(e.ctx, `DROP TRIGGER IF EXISTS trg_reject_branch_update ON disks; DROP FUNCTION IF EXISTS reject_branch_update();`)
	})

	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "feature", SourceCommitID: &commit.CommitID})
	require.Error(t, err)

	// 1. 新分支目录已被彻底清理，无残留临时目录。
	_, statErr := os.Stat(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "feature"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)

	// 2. TreeQuota 用量守恒：仅保留 main 分支的 1024 字节与 2 个节点（根 + seed）。
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 1024, Inodes: 2}, quota.Usage())

	// 3. 移除触发器后可再次正常创建同名分支（常驻句柄已驱逐且锁已释放）。
	_, err = e.pool.Exec(e.ctx, `DROP TRIGGER trg_reject_branch_update ON disks`)
	require.NoError(t, err)
	branch, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "feature", SourceCommitID: &commit.CommitID})
	require.NoError(t, err)
	require.Equal(t, "feature", branch.Name)
}

// CreateBranch 派生历史快照超出缩容后的磁盘配额上限时，必须返回 ErrNoSpace 并完整清理新分支目录与配额。
func TestWorkspaceSwapperCreateBranchQuotaExceededRollback(t *testing.T) {
	e := setupPebbleCreate(t, 4096)
	file, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("historical"), Mode: 0o644, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: bytes.Repeat([]byte("h"), 2048), SessionID: &e.sid})
	require.NoError(t, err)
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	// 在 main 上删除文件释放配额，并将磁盘缩容至 512 字节。
	require.NoError(t, e.mgr.Unlink(e.ctx, &e.sid, 1, []byte("historical"), false))
	size := uint64(512)
	_, err = e.mgr.UpdateDisk(e.ctx, meta.UpdateDiskParams{DiskID: e.disk.DiskID, MaxBytes: &size})
	require.NoError(t, err)

	// 从包含 2048 字节文件的历史快照派生分支，因超出 512 字节上限必须返回 ErrNoSpace。
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "over-quota", SourceCommitID: &commit.CommitID})
	require.ErrorIs(t, err, meta.ErrNoSpace)

	_, statErr := os.Stat(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "over-quota"))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)

	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 0, Inodes: 1}, quota.Usage())
	branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 1)
}

// DeleteBranch、Rollback 与 AbortMerge 在控制面持久化失败时，必须原子恢复分支目录与 TreeQuota 用量；
// 成功提交时必须清理 .backup-* / .delete-* / .restore-* 临时目录。
func TestWorkspaceSwapperDeleteRollbackAndAbortMergeConservation(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	baseFile, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("conflict.txt"), Mode: 0o644, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: baseFile.Inode.Ino, Data: []byte("base"), SessionID: &e.sid})
	require.NoError(t, err)
	baseCommit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	// 创建 feature 分支并写入独立修改。
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "feature", SourceCommitID: &baseCommit.CommitID})
	require.NoError(t, err)
	featSID := uint64(301)
	branchName := "feature"
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featSID, DiskID: e.disk.DiskID, BranchName: &branchName})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: baseFile.Inode.Ino, Data: []byte("feature-change!"), SessionID: &featSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, featSID))

	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	beforeUsage := quota.Usage()
	require.Equal(t, meta.TreeQuotaUsage{Bytes: uint64(len("feature-change!")), Inodes: 2}, beforeUsage)

	// 注入控制面触发器拦截 disks.branches 更新。
	_, err = e.pool.Exec(e.ctx, `CREATE OR REPLACE FUNCTION reject_any_branch_update() RETURNS trigger AS $$
	BEGIN
		RAISE EXCEPTION 'simulated branch save failure' USING ERRCODE = '23514';
	END;
	$$ LANGUAGE plpgsql;
	CREATE TRIGGER trg_reject_any_branch_update BEFORE UPDATE OF branches ON disks FOR EACH ROW EXECUTE FUNCTION reject_any_branch_update();`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = e.pool.Exec(e.ctx, `DROP TRIGGER IF EXISTS trg_reject_any_branch_update ON disks; DROP FUNCTION IF EXISTS reject_any_branch_update();`)
	})

	// 1. DeleteBranch 失败时，feature 目录与配额完好恢复。
	require.Error(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "feature"}))
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)
	require.Equal(t, beforeUsage, quota.Usage())

	// 2. Rollback 失败时，feature 目录的修改（"feature-change!"）与配额完好恢复。
	require.Error(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "feature", CommitID: baseCommit.CommitID}))
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)
	require.Equal(t, beforeUsage, quota.Usage())

	// 验证 feature 分支的数据确实未丢。
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featSID, DiskID: e.disk.DiskID, BranchName: &branchName})
	require.NoError(t, err)
	read, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: baseFile.Inode.Ino, Size: 32, SessionID: &featSID})
	require.NoError(t, err)
	require.Equal(t, []byte("feature-change!"), read.Data)
	require.NoError(t, e.mgr.CloseSession(e.ctx, featSID))

	// 移除触发器，构造 MERGING 状态以验证 AbortMerge 的回滚守恒与成功提交。
	_, err = e.pool.Exec(e.ctx, `DROP TRIGGER trg_reject_any_branch_update ON disks`)
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: baseFile.Inode.Ino, Data: []byte("main-conflict"), SessionID: &e.sid})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))

	mergeRes, err := e.mgr.Merge(e.ctx, meta.MergeParams{
		DiskID: e.disk.DiskID, SourceBranch: "feature", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize,
	})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultMaterialized, mergeRes.Result)
	mergingUsage := quota.Usage()

	// 3. 重新挂载触发器，验证 AbortMerge 失败时恢复 MERGING 目录与配额。
	_, err = e.pool.Exec(e.ctx, `CREATE TRIGGER trg_reject_any_branch_update BEFORE UPDATE OF branches ON disks FOR EACH ROW EXECUTE FUNCTION reject_any_branch_update();`)
	require.NoError(t, err)
	require.Error(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "main"}))
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)
	require.Equal(t, mergingUsage, quota.Usage())

	// 4. 移除触发器后，AbortMerge 与 DeleteBranch 正常提交且无残留临时目录。
	_, err = e.pool.Exec(e.ctx, `DROP TRIGGER trg_reject_any_branch_update ON disks`)
	require.NoError(t, err)
	require.NoError(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "main"}))
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)
	require.NoError(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "feature"}))
	assertNoTemporaryBranchDirs(t, e.root, e.disk.DiskID)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: uint64(len("main-conflict")), Inodes: 2}, quota.Usage())
}

// CreateDisk 在控制面持久化失败时，必须通过 WorkspaceSwapper 完整清理新建磁盘目录。
func TestWorkspaceSwapperCreateDiskRollbackCleansStorage(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	_, err := e.pool.Exec(e.ctx, `CREATE OR REPLACE FUNCTION reject_disk_insert() RETURNS trigger AS $$
	BEGIN
		RAISE EXCEPTION 'simulated disk insert failure' USING ERRCODE = '23514';
	END;
	$$ LANGUAGE plpgsql;
	CREATE TRIGGER trg_reject_disk_insert BEFORE INSERT ON disks FOR EACH ROW EXECUTE FUNCTION reject_disk_insert();`)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = e.pool.Exec(e.ctx, `DROP TRIGGER IF EXISTS trg_reject_disk_insert ON disks; DROP FUNCTION IF EXISTS reject_disk_insert();`)
	})

	_, err = e.mgr.CreateDisk(e.ctx, meta.CreateDiskParams{Name: "failed-disk", MaxBytes: 4096})
	require.Error(t, err)

	entries, err := os.ReadDir(filepath.Join(e.root, "disks"))
	require.NoError(t, err)
	require.Len(t, entries, 1, "失败的 CreateDisk 不得残留第二个磁盘目录")
	require.Equal(t, e.disk.DiskID, entries[0].Name())
}
