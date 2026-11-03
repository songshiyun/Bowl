package meta_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func mergeCall(t *testing.T, e *pebbleCreateEnv, source, target string) *meta.MergeResult {
	t.Helper()
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: source, TargetBranch: target})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultMerged, result.Result)
	require.NotNil(t, result.MergeCommitID)
	return result
}

func mergeCreate(t *testing.T, e *pebbleCreateEnv, sid, parent uint64, name string, data []byte) uint64 {
	t.Helper()
	created, err := e.mgr.Create(meta.WithSessionID(e.ctx, &sid), meta.CreateParams{ParentIno: parent, Name: []byte(name), Mode: 0o644})
	require.NoError(t, err)
	if len(data) > 0 {
		_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: created.Inode.Ino, Data: data, SessionID: &sid})
		require.NoError(t, err)
	}
	return created.Inode.Ino
}

func mergeLookup(t *testing.T, e *pebbleCreateEnv, sid, parent uint64, name string) *meta.Inode {
	t.Helper()
	inode, err := e.mgr.Lookup(meta.WithSessionID(e.ctx, &sid), parent, []byte(name))
	require.NoError(t, err)
	return inode
}

func mergeWrite(t *testing.T, e *pebbleCreateEnv, sid, ino uint64, data []byte) {
	t.Helper()
	for offset := 0; offset < len(data); offset += pebblekv.BlobSize {
		end := min(offset+pebblekv.BlobSize, len(data))
		_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: uint64(offset), Data: data[offset:end], SessionID: &sid})
		require.NoError(t, err)
	}
}

func mergeRead(t *testing.T, e *pebbleCreateEnv, sid, ino uint64, expected []byte) {
	t.Helper()
	for offset := 0; offset < len(expected); offset += pebblekv.BlobSize {
		end := min(offset+pebblekv.BlobSize, len(expected))
		response, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: uint64(offset), Size: uint32(end - offset), SessionID: &sid})
		require.NoError(t, err)
		require.Equal(t, expected[offset:end], response.Data)
	}
}

func mergeBranch(t *testing.T, e *pebbleCreateEnv, name, source string) *meta.Branch {
	t.Helper()
	branch, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: name, SourceBranch: &source})
	require.NoError(t, err)
	return branch
}

func TestMergeWriterGateAndReadOnlySnapshot(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	ino := mergeCreate(t, e, 102, 1, "新文件", []byte("源数据"))
	params := meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"}
	_, err := e.mgr.Merge(e.ctx, params)
	require.ErrorIs(t, err, meta.ErrBusy)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	oldCommit := *e.disk.Branches[0].HeadCommitID
	versionSession(t, e, "main", 103, true, &oldCommit)
	result := mergeCall(t, e, "dev", "main")
	versionSession(t, e, "main", 104, true, result.MergeCommitID)
	require.Equal(t, ino, mergeLookup(t, e, 104, 1, "新文件").Ino)
	_, err = e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(103)), 1, []byte("新文件"))
	require.ErrorIs(t, err, meta.ErrNotFound)
	versionRead(t, e, 104, ino, []byte("源数据"))
	for _, bad := range []meta.MergeParams{
		{DiskID: "bad", SourceBranch: "dev", TargetBranch: "main"},
		{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "dev"},
		{DiskID: e.disk.DiskID, SourceBranch: "../x", TargetBranch: "main"},
	} {
		_, err := e.mgr.Merge(e.ctx, bad)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
}

func TestMergeMultiLevelBranches(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	b := mergeBranch(t, e, "B", "main")
	c := mergeBranch(t, e, "C", "B")
	d := mergeBranch(t, e, "D", "B")
	require.Equal(t, b.HeadCommitID, c.ForkCommitID)
	require.Equal(t, b.HeadCommitID, d.ForkCommitID)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	versionSession(t, e, "C", 102, false, nil)
	ci := mergeCreate(t, e, 102, 1, "c", []byte("C"))
	first := mergeCall(t, e, "C", "B")
	versionSession(t, e, "D", 103, false, nil)
	di := mergeCreate(t, e, 103, 1, "d", []byte("DD"))
	second := mergeCall(t, e, "D", "B")
	require.NotEqual(t, *first.MergeCommitID, *second.MergeCommitID)
	final := mergeCall(t, e, "B", "main")
	versionSession(t, e, "main", 104, true, final.MergeCommitID)
	require.Equal(t, ci, mergeLookup(t, e, 104, 1, "c").Ino)
	require.Equal(t, di, mergeLookup(t, e, 104, 1, "d").Ino)
	versionRead(t, e, 104, ci, []byte("C"))
	versionRead(t, e, 104, di, []byte("DD"))
	var parent, mergeParent string
	err := e.pool.QueryRow(e.ctx, `SELECT parent_commit_id::text, merge_parent_commit_id::text FROM commits WHERE commit_id=$1`, *second.MergeCommitID).Scan(&parent, &mergeParent)
	require.NoError(t, err)
	require.Equal(t, *first.MergeCommitID, parent)
	require.NotEmpty(t, mergeParent)
	upToDate := mergeCall(t, e, "D", "B")
	require.Equal(t, second.MergeCommitID, upToDate.MergeCommitID)
}

func TestMergeNonConflictingChangesAndQuota(t *testing.T) {
	e := setupPebbleCreate(t, 32<<20)
	a := mergeCreate(t, e, e.sid, 1, "a", nil)
	b := mergeCreate(t, e, e.sid, 1, "b", nil)
	mergeCreate(t, e, e.sid, 1, "删除", []byte("旧值"))
	dir, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("旧目录"), Mode: 0o755})
	require.NoError(t, err)
	child := mergeCreate(t, e, e.sid, dir.Ino, "子文件", []byte("子"))
	require.NoError(t, e.mgr.SetXattr(e.branchContext(), meta.SetXattrParams{Ino: b, Name: []byte("user.old"), Value: []byte("旧属性")}))
	base, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	oursData := bytes.Repeat([]byte("A"), pebblekv.BlobSize+11)
	theirsData := bytes.Repeat([]byte("B"), pebblekv.BlobSize+17)
	mergeWrite(t, e, e.sid, a, oursData)
	oursDir, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("目标目录"), Mode: 0o755})
	require.NoError(t, err)
	oursFile := mergeCreate(t, e, e.sid, oursDir.Ino, "目标文件", []byte("目标"))
	oursSame := mergeCreate(t, e, e.sid, 1, "相同", []byte("相同内容"))
	mergeWrite(t, e, 102, b, theirsData)
	devCtx := meta.WithSessionID(e.ctx, ptrVersionSID(102))
	require.NoError(t, e.mgr.SetXattr(devCtx, meta.SetXattrParams{Ino: b, Name: []byte("user.test"), Value: []byte("属性")}))
	require.NoError(t, e.mgr.RemoveXattr(devCtx, b, []byte("user.old")))
	require.NoError(t, e.mgr.Unlink(devCtx, 1, []byte("删除")))
	require.NoError(t, e.mgr.Rename(devCtx, meta.RenameParams{OldParentIno: 1, OldName: []byte("旧目录"), NewParentIno: 1, NewName: []byte("新目录")}))
	devDir, err := e.mgr.Mkdir(devCtx, meta.MkdirParams{ParentIno: 1, Name: []byte("源目录"), Mode: 0o755})
	require.NoError(t, err)
	devFile := mergeCreate(t, e, 102, devDir.Ino, "源文件", []byte("源"))
	devSame := mergeCreate(t, e, 102, 1, "相同", []byte("相同内容"))
	require.NotEqual(t, oursSame, devSame)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result := mergeCall(t, e, "dev", "main")
	var parent, mergeParent string
	err = e.pool.QueryRow(e.ctx, `SELECT parent_commit_id::text, merge_parent_commit_id::text FROM commits WHERE commit_id=$1`, *result.MergeCommitID).Scan(&parent, &mergeParent)
	require.NoError(t, err)
	require.NotEqual(t, base.CommitID, parent)
	require.NotEqual(t, parent, mergeParent)
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	mergeRead(t, e, 103, a, oursData)
	mergeRead(t, e, 103, b, theirsData)
	versionRead(t, e, 103, oursFile, []byte("目标"))
	versionRead(t, e, 103, devFile, []byte("源"))
	require.Equal(t, devSame, mergeLookup(t, e, 103, 1, "相同").Ino)
	require.Equal(t, dir.Ino, mergeLookup(t, e, 103, 1, "新目录").Ino)
	require.Equal(t, child, mergeLookup(t, e, 103, dir.Ino, "子文件").Ino)
	_, err = e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(103)), 1, []byte("删除"))
	require.ErrorIs(t, err, meta.ErrNotFound)
	attr, err := e.mgr.GetXattr(meta.WithSessionID(e.ctx, ptrVersionSID(103)), b, []byte("user.test"))
	require.NoError(t, err)
	require.Equal(t, []byte("属性"), attr)
	_, err = e.mgr.GetXattr(meta.WithSessionID(e.ctx, ptrVersionSID(103)), b, []byte("user.old"))
	require.ErrorIs(t, err, meta.ErrNoData)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	wantBytes := uint64(len(oursData) + len(theirsData) + len("子") + len("目标") + len("源") + len("相同内容"))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: wantBytes, Inodes: 10}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		require.Equal(t, uint64(10), stat.Inodes)
		require.Equal(t, wantBytes, stat.LogicalBytes)
		assertPebbleMissing(t, store, pebblekv.DirtyKey(b))
	})
	storage := pebblekv.NewManager(e.root)
	snapshot, err := storage.OpenCommit(e.disk.DiskID, *result.MergeCommitID)
	require.NoError(t, err)
	getPebbleValue(t, snapshot.DB(), pebblekv.DirtyKey(b))
	require.NoError(t, snapshot.Close())
	require.NoError(t, storage.Close())
}

func TestMergeConflictAndTransactionFailure(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("初始"))
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("目标"), SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("源端"), SessionID: ptrVersionSID(102)})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
	require.Len(t, result.Conflicts, 1)
	require.Equal(t, "/文件", result.Conflicts[0].Path)
	versionSession(t, e, "main", 103, false, nil)
	versionRead(t, e, 103, ino, []byte("目标"))
	require.NoError(t, e.mgr.CloseSession(e.ctx, 103))

	// 回退到共同祖先，让合并可成功进入工作区交换，再强制数据库事务回滚。
	branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	base := *branches[1].ForkCommitID
	require.NoError(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "main", CommitID: base}))
	failing := meta.New(&rollbackVersionStore{Store: db.NewStore(e.pool), pool: e.pool}, meta.WithStorageRoot(e.root))
	commitDir := filepath.Join(e.root, "disks", e.disk.DiskID, "commits")
	before, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	_, err = failing.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.ErrorIs(t, err, errVersionTransaction)
	after, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	require.Equal(t, len(before), len(after))
	versionSession(t, e, "main", 104, false, nil)
	versionRead(t, e, 104, ino, []byte("初始"))
}

func TestMergeDirectoryDeletionWithConcurrentCreate(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	dir, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("目录"), Mode: 0o755})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	ino := mergeCreate(t, e, e.sid, dir.Ino, "新文件", []byte("保留"))
	require.NoError(t, e.mgr.Rmdir(meta.WithSessionID(e.ctx, ptrVersionSID(102)), 1, []byte("目录")))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
	versionSession(t, e, "main", 103, false, nil)
	require.Equal(t, ino, mergeLookup(t, e, 103, dir.Ino, "新文件").Ino)
	versionRead(t, e, 103, ino, []byte("保留"))
}

func TestMergeSourceTimestampChange(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("内容"))
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	modified := time.Unix(123456, 0).UTC()
	_, err := e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mtime: &modified, SessionID: ptrVersionSID(102)})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result := mergeCall(t, e, "dev", "main")
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	got, err := e.mgr.GetAttr(meta.WithSessionID(e.ctx, ptrVersionSID(103)), ino)
	require.NoError(t, err)
	require.Equal(t, modified, got.Attr.Mtime)
}

func TestMergeIdenticalCreationThenSourceModification(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	mergeCreate(t, e, e.sid, 1, "文件", []byte("内容"))
	sourceIno := mergeCreate(t, e, 102, 1, "文件", []byte("内容"))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	mergeCall(t, e, "dev", "main")
	mergeWrite(t, e, 102, sourceIno, []byte("新值"))
	result := mergeCall(t, e, "dev", "main")
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	file := mergeLookup(t, e, 103, 1, "文件")
	versionRead(t, e, 103, file.Ino, []byte("新值"))
}

func TestMergeMoveDirectoryAcrossParents(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	a, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("a"), Mode: 0o755})
	require.NoError(t, err)
	b, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("b"), Mode: 0o755})
	require.NoError(t, err)
	child, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: a.Ino, Name: []byte("子目录"), Mode: 0o755})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	require.NoError(t, e.mgr.Rename(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.RenameParams{
		OldParentIno: a.Ino, OldName: []byte("子目录"), NewParentIno: b.Ino, NewName: []byte("子目录"),
	}))
	mergeCreate(t, e, e.sid, 1, "目标文件", nil)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result := mergeCall(t, e, "dev", "main")
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	moved := mergeLookup(t, e, 103, b.Ino, "子目录")
	require.Equal(t, child.Ino, moved.Ino)
	require.Equal(t, b.Ino, moved.Attr.ParentIno)
	parentA, err := e.mgr.GetAttr(meta.WithSessionID(e.ctx, ptrVersionSID(103)), a.Ino)
	require.NoError(t, err)
	parentB, err := e.mgr.GetAttr(meta.WithSessionID(e.ctx, ptrVersionSID(103)), b.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(2), parentA.Attr.Nlink)
	require.Equal(t, uint32(3), parentB.Attr.Nlink)
}

func TestMergeHardLinkChanges(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("内容"))
	_, err := e.mgr.Link(e.branchContext(), meta.LinkParams{Ino: ino, NewParentIno: 1, NewName: []byte("旧链接")})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	_, err = e.mgr.Link(e.branchContext(), meta.LinkParams{Ino: ino, NewParentIno: 1, NewName: []byte("新链接")})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Unlink(meta.WithSessionID(e.ctx, ptrVersionSID(102)), 1, []byte("旧链接")))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result := mergeCall(t, e, "dev", "main")
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	file := mergeLookup(t, e, 103, 1, "新链接")
	require.Equal(t, ino, file.Ino)
	require.Equal(t, uint32(2), file.Attr.Nlink)
	_, err = e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(103)), 1, []byte("旧链接"))
	require.ErrorIs(t, err, meta.ErrNotFound)
	versionRead(t, e, 103, ino, []byte("内容"))
}

func TestMergeDirectoryMovedToDifferentNames(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	_, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("目录"), Mode: 0o755})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	require.NoError(t, e.mgr.Rename(e.branchContext(), meta.RenameParams{OldParentIno: 1, OldName: []byte("目录"), NewParentIno: 1, NewName: []byte("目标名称")}))
	require.NoError(t, e.mgr.Rename(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.RenameParams{OldParentIno: 1, OldName: []byte("目录"), NewParentIno: 1, NewName: []byte("源名称")}))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
}

func TestMergeDirectoryMovesCannotCreateCycle(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	a, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("a"), Mode: 0o755})
	require.NoError(t, err)
	b, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("b"), Mode: 0o755})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	require.NoError(t, e.mgr.Rename(e.branchContext(), meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: b.Ino, NewName: []byte("a")}))
	require.NoError(t, e.mgr.Rename(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.RenameParams{OldParentIno: 1, OldName: []byte("b"), NewParentIno: a.Ino, NewName: []byte("b")}))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
}

func TestMergeConflictingCreatedFiles(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	ours := mergeCreate(t, e, e.sid, 1, "文件", []byte("目标"))
	theirs := mergeCreate(t, e, 102, 1, "文件", []byte("来源"))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
	require.Len(t, result.Conflicts, 1)
	require.Equal(t, "/文件", result.Conflicts[0].Path)
	require.Nil(t, result.Conflicts[0].BaseAttr)
	require.Equal(t, ours, result.Conflicts[0].OursAttr.Ino)
	require.Equal(t, theirs, result.Conflicts[0].TheirsAttr.Ino)
}

func TestMergeIdenticalBytesWithDifferentLinks(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	mergeCreate(t, e, e.sid, 1, "a", []byte("内容"))
	mergeCreate(t, e, e.sid, 1, "b", []byte("内容"))
	source := mergeCreate(t, e, 102, 1, "a", []byte("内容"))
	_, err := e.mgr.Link(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.LinkParams{Ino: source, NewParentIno: 1, NewName: []byte("b")})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
}

func TestMergeIdenticalHardLinks(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	ours := mergeCreate(t, e, e.sid, 1, "a", []byte("内容"))
	_, err := e.mgr.Link(e.branchContext(), meta.LinkParams{Ino: ours, NewParentIno: 1, NewName: []byte("b")})
	require.NoError(t, err)
	theirs := mergeCreate(t, e, 102, 1, "a", []byte("内容"))
	_, err = e.mgr.Link(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.LinkParams{Ino: theirs, NewParentIno: 1, NewName: []byte("b")})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result := mergeCall(t, e, "dev", "main")
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	first := mergeLookup(t, e, 103, 1, "a")
	second := mergeLookup(t, e, 103, 1, "b")
	require.Equal(t, theirs, first.Ino)
	require.Equal(t, first.Ino, second.Ino)
	require.Equal(t, uint32(2), first.Attr.Nlink)
}

func TestMergeDataAndTimestampConflict(t *testing.T) {
	for _, timestampOnSource := range []bool{false, true} {
		t.Run(fmt.Sprintf("时间戳在源分支=%t", timestampOnSource), func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("内容"))
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			timeSID, dataSID := e.sid, uint64(102)
			if timestampOnSource {
				timeSID, dataSID = dataSID, timeSID
			}
			modified := time.Unix(123456, 0).UTC()
			_, err := e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mtime: &modified, SessionID: &timeSID})
			require.NoError(t, err)
			mergeWrite(t, e, dataSID, ino, []byte("修改"))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
			require.NoError(t, err)
			require.Equal(t, meta.MergeResultConflicts, result.Result)
		})
	}
}

func TestMergeFileRenamedDifferently(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	mergeCreate(t, e, e.sid, 1, "文件", []byte("内容"))
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	require.NoError(t, e.mgr.Rename(e.branchContext(), meta.RenameParams{OldParentIno: 1, OldName: []byte("文件"), NewParentIno: 1, NewName: []byte("目标名称")}))
	require.NoError(t, e.mgr.Rename(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.RenameParams{OldParentIno: 1, OldName: []byte("文件"), NewParentIno: 1, NewName: []byte("源名称")}))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
}

func TestMergeConflictStrategiesPreserveWholeFiles(t *testing.T) {
	for _, tc := range []struct {
		name     string
		strategy meta.ConflictStrategy
	}{
		{"默认", meta.ConflictStrategyUnspecified},
		{"目标", meta.ConflictStrategyOurs},
		{"来源", meta.ConflictStrategyTheirs},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := setupPebbleCreate(t, 32<<20)
			base := bytes.Repeat([]byte("b"), 2*pebblekv.BlobSize)
			ino := mergeCreate(t, e, e.sid, 1, "文件", nil)
			mergeWrite(t, e, e.sid, ino, base)
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			ours, theirs := bytes.Clone(base), bytes.Clone(base)
			ours[0], theirs[pebblekv.BlobSize] = 'o', 't'
			_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("o"), SessionID: &e.sid})
			require.NoError(t, err)
			_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: pebblekv.BlobSize, Data: []byte("t"), SessionID: ptrVersionSID(102)})
			require.NoError(t, err)
			extra := mergeCreate(t, e, 102, 1, "无冲突", []byte("来源"))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			before, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: tc.strategy})
			require.NoError(t, err)
			if tc.strategy == meta.ConflictStrategyUnspecified {
				require.Equal(t, meta.MergeResultConflicts, result.Result)
				require.Len(t, result.Conflicts, 1)
				require.Equal(t, "/文件", result.Conflicts[0].Path)
				require.Equal(t, ino, result.Conflicts[0].BaseAttr.Ino)
				require.Equal(t, ino, result.Conflicts[0].OursAttr.Ino)
				require.Equal(t, ino, result.Conflicts[0].TheirsAttr.Ino)
				after, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
				require.NoError(t, err)
				require.Equal(t, before, after)
			} else {
				require.Equal(t, meta.MergeResultMerged, result.Result)
				require.NotNil(t, result.MergeCommitID)
				var parent, mergeParent string
				require.NoError(t, e.pool.QueryRow(e.ctx, `SELECT parent_commit_id::text, merge_parent_commit_id::text FROM commits WHERE commit_id=$1`, *result.MergeCommitID).Scan(&parent, &mergeParent))
				require.NotEmpty(t, parent)
				require.NotEqual(t, parent, mergeParent)
			}
			versionSession(t, e, "main", 103, false, nil)
			if tc.strategy == meta.ConflictStrategyTheirs {
				mergeRead(t, e, 103, ino, theirs)
			} else {
				mergeRead(t, e, 103, ino, ours)
			}
			if tc.strategy == meta.ConflictStrategyUnspecified {
				_, err := e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(103)), 1, []byte("无冲突"))
				require.ErrorIs(t, err, meta.ErrNotFound)
			} else {
				require.Equal(t, extra, mergeLookup(t, e, 103, 1, "无冲突").Ino)
				versionRead(t, e, 103, extra, []byte("来源"))
			}
		})
	}
}

func TestMergeMaterializeResolveAndAbort(t *testing.T) {
	for _, action := range []string{"ResolveMerge", "Commit", "AbortMerge"} {
		t.Run(action, func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
			require.NoError(t, e.mgr.SetXattr(e.branchContext(), meta.SetXattrParams{Ino: ino, Name: []byte("user.test"), Value: []byte("base")}))
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			mergeWrite(t, e, e.sid, ino, []byte("ours"))
			mergeWrite(t, e, 102, ino, []byte("them"))
			require.NoError(t, e.mgr.SetXattr(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.SetXattrParams{Ino: ino, Name: []byte("user.test"), Value: []byte("them")}))
			extra := mergeCreate(t, e, 102, 1, "无冲突", []byte("extra"))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			before := quota.Usage()
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize})
			require.NoError(t, err)
			require.Equal(t, meta.MergeResultMaterialized, result.Result)
			require.Nil(t, result.MergeCommitID)
			require.Len(t, result.Conflicts, 1)
			branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			require.Equal(t, "MERGING", branches[0].State)
			targetID, sourceID := *branches[0].HeadCommitID, *branches[1].HeadCommitID
			require.Equal(t, meta.TreeQuotaUsage{Bytes: before.Bytes + 8, Inodes: before.Inodes + 2}, quota.Usage())
			_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
			require.ErrorIs(t, err, meta.ErrBusy)
			_, err = e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
			require.ErrorIs(t, err, meta.ErrBusy)
			versionSession(t, e, "main", 103, false, nil)
			versionRead(t, e, 103, ino, []byte("ours"))
			theirs := mergeLookup(t, e, 103, 1, "文件.theirs")
			base := mergeLookup(t, e, 103, 1, "文件.base")
			versionRead(t, e, 103, theirs.Ino, []byte("them"))
			versionRead(t, e, 103, base.Ino, []byte("base"))
			xattr, err := e.mgr.GetXattr(meta.WithSessionID(e.ctx, ptrVersionSID(103)), theirs.Ino, []byte("user.test"))
			require.NoError(t, err)
			require.Equal(t, []byte("them"), xattr)
			require.Equal(t, extra, mergeLookup(t, e, 103, 1, "无冲突").Ino)
			e.withBranch(t, "main", func(store *pebble.DB) {
				stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
				require.NoError(t, err)
				require.Equal(t, uint64(17), stat.LogicalBytes)
				require.Equal(t, uint64(5), stat.Inodes)
				getPebbleValue(t, store, pebblekv.DirtyKey(theirs.Ino))
				getPebbleValue(t, store, pebblekv.DirtyKey(base.Ino))
				getPebbleValue(t, store, pebblekv.DirtyKey(1))
			})
			if action == "AbortMerge" {
				require.ErrorIs(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "main"}), meta.ErrBusy)
				require.NoError(t, e.mgr.CloseSession(e.ctx, 103))
				require.NoError(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "main"}))
				require.Equal(t, before, quota.Usage())
				versionSession(t, e, "main", 104, false, nil)
				versionRead(t, e, 104, ino, []byte("ours"))
				for _, name := range []string{"文件.theirs", "文件.base", "无冲突"} {
					_, err := e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(104)), 1, []byte(name))
					require.ErrorIs(t, err, meta.ErrNotFound)
				}
			} else {
				ctx := meta.WithSessionID(e.ctx, ptrVersionSID(103))
				mergeWrite(t, e, 103, ino, []byte("done"))
				require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("文件.theirs")))
				_, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
				require.ErrorIs(t, err, meta.ErrBusy)
				require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("文件.base")))
				var info *meta.CommitInfo
				if action == "ResolveMerge" {
					info, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main", Message: "完成"})
				} else {
					info, err = e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main", Message: "完成"})
				}
				require.NoError(t, err)
				require.Equal(t, &targetID, info.ParentCommitID)
				require.Equal(t, &sourceID, info.MergeParentCommitID)
				require.Equal(t, "完成", info.Message)
				versionRead(t, e, 103, ino, []byte("done"))
				var parent, mergeParent string
				require.NoError(t, e.pool.QueryRow(e.ctx, `SELECT parent_commit_id::text, merge_parent_commit_id::text FROM commits WHERE commit_id=$1`, info.CommitID).Scan(&parent, &mergeParent))
				require.Equal(t, targetID, parent)
				require.Equal(t, sourceID, mergeParent)
				require.Equal(t, before, quota.Usage())
			}
			branches, err = e.mgr.ListBranches(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			require.Equal(t, "NORMAL", branches[0].State)
		})
	}
}

func TestMergeConflictTopologyStrategies(t *testing.T) {
	for _, scenario := range []string{"目标删除", "来源删除", "同名创建", "不同位置移动", "目录删除", "目录内修改", "深层文件修改", "同名目录"} {
		for _, strategy := range []meta.ConflictStrategy{meta.ConflictStrategyOurs, meta.ConflictStrategyTheirs} {
			t.Run(fmt.Sprintf("%s/%d", scenario, strategy), func(t *testing.T) {
				e := setupPebbleCreate(t, 1<<20)
				var ino uint64
				if scenario == "目录删除" || scenario == "目录内修改" || scenario == "深层文件修改" {
					dir, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("目录"), Mode: 0o755})
					require.NoError(t, err)
					ino = dir.Ino
					if scenario == "目录内修改" || scenario == "深层文件修改" {
						parent := ino
						if scenario == "深层文件修改" {
							sub, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: ino, Name: []byte("子目录"), Mode: 0o755})
							require.NoError(t, err)
							parent = sub.Ino
						}
						mergeCreate(t, e, e.sid, parent, "子文件", []byte("base"))
					}
				} else if scenario != "同名创建" && scenario != "同名目录" {
					ino = mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
				}
				mergeBranch(t, e, "dev", "main")
				versionSession(t, e, "dev", 102, false, nil)
				oursCtx, theirsCtx := e.branchContext(), meta.WithSessionID(e.ctx, ptrVersionSID(102))
				switch scenario {
				case "目标删除":
					require.NoError(t, e.mgr.Unlink(oursCtx, 1, []byte("文件")))
					mergeWrite(t, e, 102, ino, []byte("them"))
				case "来源删除":
					mergeWrite(t, e, e.sid, ino, []byte("ours"))
					require.NoError(t, e.mgr.Unlink(theirsCtx, 1, []byte("文件")))
				case "同名创建":
					mergeCreate(t, e, e.sid, 1, "文件", []byte("ours"))
					mergeCreate(t, e, 102, 1, "文件", []byte("them"))
				case "不同位置移动":
					require.NoError(t, e.mgr.Rename(oursCtx, meta.RenameParams{OldParentIno: 1, OldName: []byte("文件"), NewParentIno: 1, NewName: []byte("目标")}))
					require.NoError(t, e.mgr.Rename(theirsCtx, meta.RenameParams{OldParentIno: 1, OldName: []byte("文件"), NewParentIno: 1, NewName: []byte("来源")}))
				case "目录删除":
					mergeCreate(t, e, e.sid, ino, "子文件", []byte("ours"))
					require.NoError(t, e.mgr.Rmdir(theirsCtx, 1, []byte("目录")))
				case "目录内修改", "深层文件修改":
					parent := ino
					if scenario == "深层文件修改" {
						parent = mergeLookup(t, e, e.sid, ino, "子目录").Ino
					}
					child := mergeLookup(t, e, e.sid, parent, "子文件")
					mergeWrite(t, e, e.sid, child.Ino, []byte("ours"))
					require.NoError(t, e.mgr.Unlink(theirsCtx, parent, []byte("子文件")))
					if scenario == "深层文件修改" {
						require.NoError(t, e.mgr.Rmdir(theirsCtx, ino, []byte("子目录")))
					}
					require.NoError(t, e.mgr.Rmdir(theirsCtx, 1, []byte("目录")))
				case "同名目录":
					ours, err := e.mgr.Mkdir(oursCtx, meta.MkdirParams{ParentIno: 1, Name: []byte("目录"), Mode: 0o755})
					require.NoError(t, err)
					theirs, err := e.mgr.Mkdir(theirsCtx, meta.MkdirParams{ParentIno: 1, Name: []byte("目录"), Mode: 0o755})
					require.NoError(t, err)
					mergeCreate(t, e, e.sid, ours.Ino, "目标文件", []byte("ours"))
					mergeCreate(t, e, 102, theirs.Ino, "来源文件", []byte("them"))
				}
				mergeCreate(t, e, 102, 1, "无冲突", []byte("extra"))
				require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
				result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: strategy})
				require.NoError(t, err)
				require.Equal(t, meta.MergeResultMerged, result.Result)
				versionSession(t, e, "main", 103, false, nil)
				ctx := meta.WithSessionID(e.ctx, ptrVersionSID(103))
				mergeLookup(t, e, 103, 1, "无冲突")
				switch scenario {
				case "目标删除", "来源删除":
					present := scenario == "目标删除" && strategy == meta.ConflictStrategyTheirs || scenario == "来源删除" && strategy == meta.ConflictStrategyOurs
					file, err := e.mgr.Lookup(ctx, 1, []byte("文件"))
					if !present {
						require.ErrorIs(t, err, meta.ErrNotFound)
					} else {
						require.NoError(t, err)
						data := []byte("ours")
						if strategy == meta.ConflictStrategyTheirs {
							data = []byte("them")
						}
						versionRead(t, e, 103, file.Ino, data)
					}
				case "同名创建":
					file := mergeLookup(t, e, 103, 1, "文件")
					data := []byte("ours")
					if strategy == meta.ConflictStrategyTheirs {
						data = []byte("them")
					}
					versionRead(t, e, 103, file.Ino, data)
				case "不同位置移动":
					kept, removed := "目标", "来源"
					if strategy == meta.ConflictStrategyTheirs {
						kept, removed = removed, kept
					}
					require.Equal(t, ino, mergeLookup(t, e, 103, 1, kept).Ino)
					_, err := e.mgr.Lookup(ctx, 1, []byte(removed))
					require.ErrorIs(t, err, meta.ErrNotFound)
				case "目录删除", "目录内修改", "深层文件修改":
					dir, err := e.mgr.Lookup(ctx, 1, []byte("目录"))
					if strategy == meta.ConflictStrategyTheirs {
						require.ErrorIs(t, err, meta.ErrNotFound)
					} else {
						require.NoError(t, err)
						parent := dir.Ino
						if scenario == "深层文件修改" {
							parent = mergeLookup(t, e, 103, parent, "子目录").Ino
						}
						child := mergeLookup(t, e, 103, parent, "子文件")
						versionRead(t, e, 103, child.Ino, []byte("ours"))
					}
				case "同名目录":
					dir := mergeLookup(t, e, 103, 1, "目录")
					kept, removed := "目标文件", "来源文件"
					if strategy == meta.ConflictStrategyTheirs {
						kept, removed = removed, kept
					}
					mergeLookup(t, e, 103, dir.Ino, kept)
					_, err := e.mgr.Lookup(ctx, dir.Ino, []byte(removed))
					require.ErrorIs(t, err, meta.ErrNotFound)
					e.withBranch(t, "main", func(store *pebble.DB) {
						stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
						require.NoError(t, err)
						require.Equal(t, uint64(4), stat.Inodes)
						require.Equal(t, uint64(9), stat.LogicalBytes)
					})
				}
			})
		}
	}
}

func TestMergeDirectoryCycleStrategies(t *testing.T) {
	for _, strategy := range []meta.ConflictStrategy{meta.ConflictStrategyOurs, meta.ConflictStrategyTheirs} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			a, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("a"), Mode: 0o755})
			require.NoError(t, err)
			b, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("b"), Mode: 0o755})
			require.NoError(t, err)
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			require.NoError(t, e.mgr.Rename(e.branchContext(), meta.RenameParams{OldParentIno: 1, OldName: []byte("a"), NewParentIno: b.Ino, NewName: []byte("a")}))
			require.NoError(t, e.mgr.Rename(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.RenameParams{OldParentIno: 1, OldName: []byte("b"), NewParentIno: a.Ino, NewName: []byte("b")}))
			extra := mergeCreate(t, e, 102, 1, "无冲突", []byte("extra"))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: strategy})
			require.NoError(t, err)
			require.Equal(t, meta.MergeResultMerged, result.Result)
			versionSession(t, e, "main", 103, false, nil)
			require.Equal(t, extra, mergeLookup(t, e, 103, 1, "无冲突").Ino)
			if strategy == meta.ConflictStrategyOurs {
				require.Equal(t, b.Ino, mergeLookup(t, e, 103, 1, "b").Ino)
				require.Equal(t, a.Ino, mergeLookup(t, e, 103, b.Ino, "a").Ino)
			} else {
				require.Equal(t, a.Ino, mergeLookup(t, e, 103, 1, "a").Ino)
				require.Equal(t, b.Ino, mergeLookup(t, e, 103, a.Ino, "b").Ino)
			}
		})
	}
}

func TestMergeMaterializeRejectsCollisionAndQuotaOverflow(t *testing.T) {
	for _, scenario := range []string{"旁路名称已存在", "配额不足"} {
		t.Run(scenario, func(t *testing.T) {
			limit := uint64(1 << 20)
			if scenario == "配额不足" {
				limit = 11
			}
			e := setupPebbleCreate(t, limit)
			ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			mergeWrite(t, e, e.sid, ino, []byte("ours"))
			mergeWrite(t, e, 102, ino, []byte("them"))
			if scenario == "旁路名称已存在" {
				mergeCreate(t, e, e.sid, 1, "文件.base", []byte("保留"))
			}
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			before, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			usage := quota.Usage()
			_, err = e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize})
			if scenario == "旁路名称已存在" {
				require.ErrorIs(t, err, meta.ErrAlreadyExists)
			} else {
				require.ErrorIs(t, err, meta.ErrNoSpace)
			}
			after, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
			require.NoError(t, err)
			require.Equal(t, before, after)
			require.Equal(t, usage, quota.Usage())
			versionSession(t, e, "main", 103, false, nil)
			versionRead(t, e, 103, ino, []byte("ours"))
			_, err = e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(103)), 1, []byte("文件.theirs"))
			require.ErrorIs(t, err, meta.ErrNotFound)
			if scenario == "旁路名称已存在" {
				file := mergeLookup(t, e, 103, 1, "文件.base")
				versionRead(t, e, 103, file.Ino, []byte("保留"))
			}
		})
	}
}

func TestResolveMergeRejectsRenamedSidecar(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	mergeWrite(t, e, e.sid, ino, []byte("ours"))
	mergeWrite(t, e, 102, ino, []byte("them"))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultMaterialized, result.Result)
	versionSession(t, e, "main", 103, false, nil)
	ctx := meta.WithSessionID(e.ctx, ptrVersionSID(103))
	require.NoError(t, e.mgr.Rename(ctx, meta.RenameParams{OldParentIno: 1, OldName: []byte("文件.theirs"), NewParentIno: 1, NewName: []byte("改名")}))
	require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("文件.base")))
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.ErrorIs(t, err, meta.ErrBusy)
	require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("改名")))
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
}

func TestMergeMaterializeFileVariants(t *testing.T) {
	for _, scenario := range []string{"目标删除", "来源删除", "同名创建", "符号链接"} {
		t.Run(scenario, func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			var ino uint64
			if scenario == "符号链接" {
				link, err := e.mgr.Symlink(e.branchContext(), meta.SymlinkParams{ParentIno: 1, Name: []byte("文件"), Target: []byte("base")})
				require.NoError(t, err)
				ino = link.Ino
			} else if scenario != "同名创建" {
				ino = mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
			}
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			oursCtx, theirsCtx := e.branchContext(), meta.WithSessionID(e.ctx, ptrVersionSID(102))
			switch scenario {
			case "目标删除":
				require.NoError(t, e.mgr.Unlink(oursCtx, 1, []byte("文件")))
				mergeWrite(t, e, 102, ino, []byte("them"))
			case "来源删除":
				mergeWrite(t, e, e.sid, ino, []byte("ours"))
				require.NoError(t, e.mgr.Unlink(theirsCtx, 1, []byte("文件")))
			case "同名创建":
				mergeCreate(t, e, e.sid, 1, "文件", []byte("ours"))
				mergeCreate(t, e, 102, 1, "文件", []byte("them"))
			case "符号链接":
				require.NoError(t, e.mgr.Unlink(oursCtx, 1, []byte("文件")))
				require.NoError(t, e.mgr.Unlink(theirsCtx, 1, []byte("文件")))
				_, err := e.mgr.Symlink(oursCtx, meta.SymlinkParams{ParentIno: 1, Name: []byte("文件"), Target: []byte("ours")})
				require.NoError(t, err)
				_, err = e.mgr.Symlink(theirsCtx, meta.SymlinkParams{ParentIno: 1, Name: []byte("文件"), Target: []byte("them")})
				require.NoError(t, err)
			}
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize})
			require.NoError(t, err)
			require.Equal(t, meta.MergeResultMaterialized, result.Result)
			versionSession(t, e, "main", 103, false, nil)
			ctx := meta.WithSessionID(e.ctx, ptrVersionSID(103))
			for _, side := range []struct {
				name    string
				present bool
				data    string
			}{
				{"文件", scenario != "目标删除", "ours"},
				{"文件.theirs", scenario != "来源删除", "them"},
				{"文件.base", scenario != "同名创建", "base"},
			} {
				file, err := e.mgr.Lookup(ctx, 1, []byte(side.name))
				if !side.present {
					require.ErrorIs(t, err, meta.ErrNotFound)
					continue
				}
				require.NoError(t, err)
				if scenario == "符号链接" {
					target, err := e.mgr.Readlink(ctx, file.Ino)
					require.NoError(t, err)
					require.Equal(t, []byte(side.data), target)
				} else {
					versionRead(t, e, 103, file.Ino, []byte(side.data))
				}
				if side.name != "文件" {
					require.NoError(t, e.mgr.Unlink(ctx, 1, []byte(side.name)))
				}
			}
			_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
			require.NoError(t, err)
		})
	}
}

func TestMergeReplacedFileConflictAttributes(t *testing.T) {
	for _, strategy := range []meta.ConflictStrategy{meta.ConflictStrategyUnspecified, meta.ConflictStrategyOurs, meta.ConflictStrategyTheirs, meta.ConflictStrategyMaterialize} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			original := mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			require.NoError(t, e.mgr.Unlink(e.branchContext(), 1, []byte("文件")))
			replacement := mergeCreate(t, e, e.sid, 1, "文件", []byte("ours"))
			mergeWrite(t, e, 102, original, []byte("them"))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: strategy})
			require.NoError(t, err)
			if strategy == meta.ConflictStrategyUnspecified || strategy == meta.ConflictStrategyMaterialize {
				require.Len(t, result.Conflicts, 1)
				require.Equal(t, "/文件", result.Conflicts[0].Path)
				require.Equal(t, original, result.Conflicts[0].BaseAttr.Ino)
				require.Equal(t, replacement, result.Conflicts[0].OursAttr.Ino)
				require.Equal(t, original, result.Conflicts[0].TheirsAttr.Ino)
			}
			versionSession(t, e, "main", 103, false, nil)
			file := mergeLookup(t, e, 103, 1, "文件")
			if strategy == meta.ConflictStrategyTheirs {
				require.Equal(t, original, file.Ino)
				versionRead(t, e, 103, file.Ino, []byte("them"))
			} else {
				require.Equal(t, replacement, file.Ino)
				versionRead(t, e, 103, file.Ino, []byte("ours"))
			}
			if strategy == meta.ConflictStrategyMaterialize {
				theirs := mergeLookup(t, e, 103, 1, "文件.theirs")
				base := mergeLookup(t, e, 103, 1, "文件.base")
				versionRead(t, e, 103, theirs.Ino, []byte("them"))
				versionRead(t, e, 103, base.Ino, []byte("base"))
			}
		})
	}
}

func TestMergeReportsFileAndEntryConflictsTogether(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	ino := mergeCreate(t, e, e.sid, 1, "原文件", []byte("base"))
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	mergeWrite(t, e, e.sid, ino, []byte("ours"))
	mergeWrite(t, e, 102, ino, []byte("them"))
	ours := mergeCreate(t, e, e.sid, 1, "新文件", []byte("ours"))
	theirs := mergeCreate(t, e, 102, 1, "新文件", []byte("them"))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main"})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultConflicts, result.Result)
	require.Len(t, result.Conflicts, 2)
	conflicts := make(map[string]meta.MergeConflict)
	for _, conflict := range result.Conflicts {
		conflicts[conflict.Path] = conflict
	}
	require.Contains(t, conflicts, "/原文件")
	require.Contains(t, conflicts, "/新文件")
	require.Equal(t, ino, conflicts["/原文件"].BaseAttr.Ino)
	require.Equal(t, ino, conflicts["/原文件"].OursAttr.Ino)
	require.Equal(t, ino, conflicts["/原文件"].TheirsAttr.Ino)
	require.Nil(t, conflicts["/新文件"].BaseAttr)
	require.Equal(t, ours, conflicts["/新文件"].OursAttr.Ino)
	require.Equal(t, theirs, conflicts["/新文件"].TheirsAttr.Ino)
}

func TestMergeMaterializeFileInMovedDirectory(t *testing.T) {
	for _, moveOnSource := range []bool{false, true} {
		t.Run(fmt.Sprint(moveOnSource), func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			dir, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("旧目录"), Mode: 0o755})
			require.NoError(t, err)
			ino := mergeCreate(t, e, e.sid, dir.Ino, "文件", []byte("base"))
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			mergeWrite(t, e, e.sid, ino, []byte("ours"))
			mergeWrite(t, e, 102, ino, []byte("them"))
			ctx := e.branchContext()
			if moveOnSource {
				ctx = meta.WithSessionID(e.ctx, ptrVersionSID(102))
			}
			require.NoError(t, e.mgr.Rename(ctx, meta.RenameParams{OldParentIno: 1, OldName: []byte("旧目录"), NewParentIno: 1, NewName: []byte("新目录")}))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize})
			require.NoError(t, err)
			require.Equal(t, meta.MergeResultMaterialized, result.Result)
			require.Len(t, result.Conflicts, 1)
			require.Equal(t, "/新目录/文件", result.Conflicts[0].Path)
			versionSession(t, e, "main", 103, false, nil)
			parent := mergeLookup(t, e, 103, 1, "新目录")
			require.Equal(t, dir.Ino, parent.Ino)
			ours := mergeLookup(t, e, 103, parent.Ino, "文件")
			theirs := mergeLookup(t, e, 103, parent.Ino, "文件.theirs")
			base := mergeLookup(t, e, 103, parent.Ino, "文件.base")
			versionRead(t, e, 103, ours.Ino, []byte("ours"))
			versionRead(t, e, 103, theirs.Ino, []byte("them"))
			versionRead(t, e, 103, base.Ino, []byte("base"))
			ctx = meta.WithSessionID(e.ctx, ptrVersionSID(103))
			require.NoError(t, e.mgr.Unlink(ctx, parent.Ino, []byte("文件.theirs")))
			require.NoError(t, e.mgr.Unlink(ctx, parent.Ino, []byte("文件.base")))
			_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
			require.NoError(t, err)
		})
	}
}

type switchableVersionStore struct {
	*rollbackVersionStore
	fail bool
}

func TestMergeResolutionValidation(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	_, err := e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: "bad", BranchName: "main"})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "../bad"})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "不存在"})
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: "bad", BranchName: "main"}), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "不存在"}), meta.ErrNotFound)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	require.ErrorIs(t, e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "main"}), meta.ErrInvalidArgument)
}

func TestMergeConflictStrategyPreservesSelectedAttributes(t *testing.T) {
	for _, strategy := range []meta.ConflictStrategy{meta.ConflictStrategyOurs, meta.ConflictStrategyTheirs} {
		t.Run(fmt.Sprint(strategy), func(t *testing.T) {
			e := setupPebbleCreate(t, 1<<20)
			ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
			mergeBranch(t, e, "dev", "main")
			versionSession(t, e, "dev", 102, false, nil)
			oursCtx, theirsCtx := e.branchContext(), meta.WithSessionID(e.ctx, ptrVersionSID(102))
			oursMode, theirsMode := uint32(0o600), uint32(0o640)
			_, err := e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &oursMode, SessionID: &e.sid})
			require.NoError(t, err)
			_, err = e.mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Mode: &theirsMode, SessionID: ptrVersionSID(102)})
			require.NoError(t, err)
			require.NoError(t, e.mgr.SetXattr(oursCtx, meta.SetXattrParams{Ino: ino, Name: []byte("user.value"), Value: []byte("ours")}))
			require.NoError(t, e.mgr.SetXattr(theirsCtx, meta.SetXattrParams{Ino: ino, Name: []byte("user.value"), Value: []byte("them")}))
			require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
			result, err := e.mgr.Merge(e.ctx, meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: strategy})
			require.NoError(t, err)
			require.Equal(t, meta.MergeResultMerged, result.Result)
			versionSession(t, e, "main", 103, false, nil)
			ctx := meta.WithSessionID(e.ctx, ptrVersionSID(103))
			file, err := e.mgr.GetAttr(ctx, ino)
			require.NoError(t, err)
			value, err := e.mgr.GetXattr(ctx, ino, []byte("user.value"))
			require.NoError(t, err)
			mode, expected := oursMode, []byte("ours")
			if strategy == meta.ConflictStrategyTheirs {
				mode, expected = theirsMode, []byte("them")
			}
			require.Equal(t, mode, file.Attr.Mode&0o777)
			require.Equal(t, expected, value)
		})
	}
}

func (s *switchableVersionStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	if s.fail {
		return s.rollbackVersionStore.ExecTx(ctx, fn)
	}
	return s.Store.ExecTx(ctx, fn)
}

func TestMaterializedMergeTransactionFailureRestoresWorkspace(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	store := &switchableVersionStore{rollbackVersionStore: &rollbackVersionStore{Store: db.NewStore(e.pool), pool: e.pool}}
	e.mgr = meta.New(store, meta.WithStorageRoot(e.root), meta.WithSessionLeaseTTL(time.Minute))
	ino := mergeCreate(t, e, e.sid, 1, "文件", []byte("base"))
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	mergeWrite(t, e, e.sid, ino, []byte("ours"))
	mergeWrite(t, e, 102, ino, []byte("them"))
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	params := meta.MergeParams{DiskID: e.disk.DiskID, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyMaterialize}
	before, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	usage := quota.Usage()
	commitDir := filepath.Join(e.root, "disks", e.disk.DiskID, "commits")
	commitsBefore, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	store.fail = true
	_, err = e.mgr.Merge(e.ctx, params)
	store.fail = false
	require.ErrorIs(t, err, errVersionTransaction)
	after, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, before, after)
	require.Equal(t, usage, quota.Usage())
	commitsAfter, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	require.Len(t, commitsAfter, len(commitsBefore))
	result, err := e.mgr.Merge(e.ctx, params)
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultMaterialized, result.Result)
	store.fail = true
	err = e.mgr.AbortMerge(e.ctx, meta.AbortMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
	store.fail = false
	require.ErrorIs(t, err, errVersionTransaction)
	branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "MERGING", branches[0].State)
	versionSession(t, e, "main", 103, false, nil)
	ctx := meta.WithSessionID(e.ctx, ptrVersionSID(103))
	versionRead(t, e, 103, ino, []byte("ours"))
	theirs := mergeLookup(t, e, 103, 1, "文件.theirs")
	base := mergeLookup(t, e, 103, 1, "文件.base")
	versionRead(t, e, 103, theirs.Ino, []byte("them"))
	versionRead(t, e, 103, base.Ino, []byte("base"))
	require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("文件.theirs")))
	require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("文件.base")))
	commitsBefore, err = os.ReadDir(commitDir)
	require.NoError(t, err)
	store.fail = true
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
	store.fail = false
	require.ErrorIs(t, err, errVersionTransaction)
	commitsAfter, err = os.ReadDir(commitDir)
	require.NoError(t, err)
	require.Len(t, commitsAfter, len(commitsBefore))
	branches, err = e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "MERGING", branches[0].State)
	e.withBranch(t, "main", func(store *pebble.DB) { getPebbleValue(t, store, pebblekv.DirtyKey(1)) })
	_, err = e.mgr.ResolveMerge(e.ctx, meta.ResolveMergeParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
}
