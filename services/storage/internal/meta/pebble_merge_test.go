package meta_test

import (
	"bytes"
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
