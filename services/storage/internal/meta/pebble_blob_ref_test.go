package meta_test

import (
	"bytes"
	"os"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// 仅修改时间戳的未变更块必须靠 12B BlobRef 短路，不读取任何 .blob 文件。
func TestPebbleDiffUnchangedBlockSkipsBlobReads(t *testing.T) {
	e := setupPebbleCreate(t, 32<<20)
	ctx := e.branchContext()
	same, err := e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("same"), Mode: 0o644})
	require.NoError(t, err)
	other, err := e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("other"), Mode: 0o644})
	require.NoError(t, err)
	sameData := bytes.Repeat([]byte{0x31}, pebblekv.BlobSize)
	otherData := bytes.Repeat([]byte{0x32}, pebblekv.BlobSize)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: same.Inode.Ino, Data: sameData, SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: other.Inode.Ino, Data: otherData, SessionID: &e.sid})
	require.NoError(t, err)
	base, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	devSID := uint64(102)
	versionSession(t, e, "dev", devSID, false, nil)
	devCtx := meta.WithSessionID(e.ctx, &devSID)

	// 只修改 same 的 mtime，数据块引用保持不变。
	_, err = e.mgr.SetAttr(devCtx, same.Inode.Ino, meta.SetAttrInput{MtimeNow: true})
	require.NoError(t, err)

	readOps := func() (int64, int64) {
		t.Helper()
		branch, err := e.storage.OpenBranch(e.disk.DiskID, "dev")
		require.NoError(t, err)
		branchOps := branch.Blobs().ReadOps()
		require.NoError(t, branch.Close())
		commit, err := e.storage.OpenCommit(e.disk.DiskID, base.CommitID)
		require.NoError(t, err)
		commitOps := commit.Blobs().ReadOps()
		require.NoError(t, commit.Close())
		return branchOps, commitOps
	}
	branchBefore, commitBefore := readOps()
	entries, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: base.CommitID, ToRef: "dev"})
	require.NoError(t, err)
	require.Len(t, entries, 1)
	require.Equal(t, "same", entries[0].Path)
	require.Empty(t, entries[0].ModifiedBlockIndices)
	branchAfter, commitAfter := readOps()
	require.Equal(t, branchBefore, branchAfter, "未变更块不得读取分支 .blob")
	require.Equal(t, commitBefore, commitAfter, "未变更块不得读取快照 .blob")

	// 真正修改 other 的数据后，Diff 必须读取两侧块内容才能判定差异。
	changed := bytes.Repeat([]byte{0x33}, pebblekv.BlobSize)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: other.Inode.Ino, Data: changed, SessionID: &devSID})
	require.NoError(t, err)
	branchBefore, commitBefore = readOps()
	entries, err = e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: base.CommitID, ToRef: "dev"})
	require.NoError(t, err)
	require.Len(t, entries, 2)
	branchAfter, commitAfter = readOps()
	require.Greater(t, branchAfter, branchBefore)
	require.Greater(t, commitAfter, commitBefore)
}

// 三路 Merge 必须直接复用获胜方 BlobRef 并硬链接块文件，零数据重编码。
func TestPebbleMergeHardlinksWinningBlob(t *testing.T) {
	e := setupPebbleCreate(t, 32<<20)
	ino := mergeCreate(t, e, e.sid, 1, "blob", nil)
	original := bytes.Repeat([]byte{0x41}, pebblekv.BlobSize)
	mergeWrite(t, e, e.sid, ino, original)
	_, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	mergeBranch(t, e, "dev", "main")
	versionSession(t, e, "dev", 102, false, nil)
	winning := bytes.Repeat([]byte{0x42}, pebblekv.BlobSize)
	mergeWrite(t, e, 102, ino, winning)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	result := mergeCall(t, e, "dev", "main")
	require.NotNil(t, result.MergeCommitID)
	versionSession(t, e, "main", 103, true, result.MergeCommitID)
	mergeRead(t, e, 103, ino, winning)

	mainBranch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	devBranch, err := e.storage.OpenBranch(e.disk.DiskID, "dev")
	require.NoError(t, err)
	readRef := func(store *pebble.DB) pebblekv.BlobRef {
		t.Helper()
		value, closer, err := store.Get(pebblekv.DataKey(ino, 0))
		require.NoError(t, err)
		ref, err := pebblekv.DecodeBlobRef(value)
		require.NoError(t, err)
		require.NoError(t, closer.Close())
		return ref
	}
	mainRef := readRef(mainBranch.DB())
	devRef := readRef(devBranch.DB())
	require.Equal(t, devRef, mainRef, "合并结果必须直接复用获胜方 12B BlobRef")
	require.NoError(t, mainBranch.Close())
	require.NoError(t, devBranch.Close())

	mainPath := metaBlobPath(e.root, e.disk.DiskID, "branches", "main", mainRef.ID)
	devPath := metaBlobPath(e.root, e.disk.DiskID, "branches", "dev", devRef.ID)
	mainInfo, err := os.Stat(mainPath)
	require.NoError(t, err)
	devInfo, err := os.Stat(devPath)
	require.NoError(t, err)
	require.True(t, os.SameFile(mainInfo, devInfo), "合并必须硬链接获胜方块文件而非重写数据")
}
