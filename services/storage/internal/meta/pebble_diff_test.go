package meta_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func TestDiffBranchAndCommit(t *testing.T) {
	e := setupPebbleCreate(t, 32<<20)
	ctx := e.branchContext()
	dir, err := e.mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: 1, Name: []byte("dir"), Mode: 0o755})
	require.NoError(t, err)
	create := func(parent uint64, name string, data []byte) *meta.Inode {
		t.Helper()
		file, err := e.mgr.Create(ctx, meta.CreateParams{ParentIno: parent, Name: []byte(name), Mode: 0o644})
		require.NoError(t, err)
		if len(data) != 0 {
			_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: data, SessionID: &e.sid})
			require.NoError(t, err)
		}
		return file.Inode
	}
	changed := create(dir.Ino, "changed", []byte("before"))
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: changed.Ino, Offset: 2 * pebblekv.BlobSize, Data: []byte("tail"), SessionID: &e.sid})
	require.NoError(t, err)
	create(dir.Ino, "old", []byte("move"))
	create(1, "deleted", []byte("remove"))
	attrs := create(1, "attrs", nil)
	unchanged := create(1, "unchanged", []byte("stable"))
	base, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	devSID := uint64(102)
	versionSession(t, e, "dev", devSID, false, nil)
	devCtx := meta.WithSessionID(e.ctx, &devSID)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: changed.Ino, Data: []byte("after!"), SessionID: &devSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Rename(devCtx, meta.RenameParams{OldParentIno: dir.Ino, OldName: []byte("old"), NewParentIno: dir.Ino, NewName: []byte("new")}))
	require.NoError(t, e.mgr.Unlink(devCtx, 1, []byte("deleted")))
	added, err := e.mgr.Create(devCtx, meta.CreateParams{ParentIno: 1, Name: []byte("added"), Mode: 0o600})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: added.Inode.Ino, Offset: pebblekv.BlobSize, Data: []byte("new"), SessionID: &devSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.SetXattr(devCtx, meta.SetXattrParams{Ino: attrs.Ino, Name: []byte("user.test"), Value: []byte("value")}))
	// 损坏未变更文件的数据，验证比较不会访问它的数据值。
	e.withBranch(t, "dev", func(store *pebble.DB) {
		require.NoError(t, store.Set(pebblekv.DataKey(unchanged.Ino, 0), []byte("无效数据"), pebble.Sync))
	})
	result, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: base.CommitID, ToRef: "dev"})
	require.NoError(t, err)
	require.Len(t, result, 5)
	require.Equal(t, []string{"added", "attrs", "deleted", "dir/changed", "dir/new"}, []string{result[0].Path, result[1].Path, result[2].Path, result[3].Path, result[4].Path})
	require.Equal(t, meta.DiffChangeTypeAdded, result[0].ChangeType)
	require.Equal(t, []uint64{1}, result[0].ModifiedBlockIndices)
	require.Equal(t, meta.DiffChangeTypeModified, result[1].ChangeType)
	require.Empty(t, result[1].ModifiedBlockIndices)
	require.Equal(t, meta.DiffChangeTypeDeleted, result[2].ChangeType)
	require.Nil(t, result[2].NewAttr)
	require.Equal(t, meta.DiffChangeTypeModified, result[3].ChangeType)
	require.Equal(t, []uint64{0}, result[3].ModifiedBlockIndices)
	require.Equal(t, meta.DiffChangeTypeRenamed, result[4].ChangeType)
	require.Equal(t, "dir/old", result[4].OldPath)
	branchResult, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: "main", ToRef: "dev"})
	require.NoError(t, err)
	require.Equal(t, result, branchResult)
	same, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: "dev", ToRef: "dev"})
	require.NoError(t, err)
	require.Empty(t, same)
	mode := uint32(0o700)
	_, err = e.mgr.SetAttr(devCtx, 1, meta.SetAttrInput{Mode: &mode})
	require.NoError(t, err)
	rootChanged, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: base.CommitID, ToRef: "dev"})
	require.NoError(t, err)
	require.Equal(t, ".", rootChanged[0].Path)
	require.Equal(t, meta.DiffChangeTypeModified, rootChanged[0].ChangeType)
}

func TestDiffValidationAndReferencePrecedence(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	base := *e.disk.Branches[0].HeadCommitID
	for _, params := range []meta.DiffParams{
		{DiskID: "bad", FromRef: "main", ToRef: "main"},
		{DiskID: uuid.Nil.String(), FromRef: "main", ToRef: "main"},
		{DiskID: e.disk.DiskID, FromRef: "../bad", ToRef: "main"},
		{DiskID: e.disk.DiskID, FromRef: "main", ToRef: ""},
	} {
		_, err := e.mgr.Diff(e.ctx, params)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
	_, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: "missing", ToRef: "main"})
	require.ErrorIs(t, err, meta.ErrNotFound)
	other, err := e.mgr.CreateDisk(e.ctx, meta.CreateDiskParams{Name: "其他", MaxBytes: 1 << 20})
	require.NoError(t, err)
	_, err = e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: *other.Branches[0].HeadCommitID, ToRef: "main"})
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: base, SourceCommitID: &base})
	require.NoError(t, err)
	versionSession(t, e, base, 102, false, nil)
	_, err = e.mgr.Create(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.CreateParams{ParentIno: 1, Name: []byte("branch")})
	require.NoError(t, err)
	result, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: "main", ToRef: base})
	require.NoError(t, err)
	require.Len(t, result, 1)
	require.Equal(t, "branch", result[0].Path)
}

func TestDiffDirectoryRenameHardLinksAndOrphan(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	dir, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: 1, Name: []byte("old"), Mode: 0o755})
	require.NoError(t, err)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: dir.Ino, Name: []byte("file"), Mode: 0o644})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("value"), SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Link(e.branchContext(), meta.LinkParams{Ino: file.Inode.Ino, NewParentIno: 1, NewName: []byte("link")})
	require.NoError(t, err)
	base, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	devSID := uint64(102)
	versionSession(t, e, "dev", devSID, false, nil)
	devCtx := meta.WithSessionID(e.ctx, &devSID)
	require.NoError(t, e.mgr.Rename(devCtx, meta.RenameParams{OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: []byte("new")}))
	orphan, err := e.mgr.Create(devCtx, meta.CreateParams{ParentIno: 1, Name: []byte("orphan")})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: orphan.Inode.Ino, Data: []byte("hidden"), SessionID: &devSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Unlink(meta.WithOpenInClient(devCtx, true), 1, []byte("orphan")))
	result, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: base.CommitID, ToRef: "dev"})
	require.NoError(t, err)
	require.Len(t, result, 2)
	require.Equal(t, "new", result[0].Path)
	require.Equal(t, "old", result[0].OldPath)
	require.Equal(t, meta.DiffChangeTypeRenamed, result[0].ChangeType)
	require.Equal(t, "new/file", result[1].Path)
	require.Equal(t, "old/file", result[1].OldPath)
	require.Equal(t, meta.DiffChangeTypeRenamed, result[1].ChangeType)
}
func TestDiffExistingStorageConstraints(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("f"), Mode: 0o644})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("base"), SessionID: &e.sid})
	require.NoError(t, err)
	base, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	devSID := uint64(102)
	versionSession(t, e, "dev", devSID, false, nil)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("left"), SessionID: &e.sid})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("rght"), SessionID: &devSID})
	require.NoError(t, err)
	var main, dev pebblekv.Inode
	e.withBranch(t, "main", func(store *pebble.DB) { main = readPebbleCreateInode(t, store, file.Inode.Ino) })
	e.withBranch(t, "dev", func(store *pebble.DB) { dev = readPebbleCreateInode(t, store, file.Inode.Ino) })
	require.Equal(t, main.WriteSeq, dev.WriteSeq)
	require.Equal(t, main.Size, dev.Size)
	require.Equal(t, main.Ino, dev.Ino)
	require.Equal(t, main.Mode, dev.Mode)
	require.Equal(t, main.UID, dev.UID)
	require.Equal(t, main.GID, dev.GID)
	require.Equal(t, main.Rdev, dev.Rdev)
	require.Equal(t, main.Flags, dev.Flags)
	require.Equal(t, main.SymlinkTarget, dev.SymlinkTarget)
	// 让两边的时间也相同，单独验证变更标记能识别不同内容。
	e.withBranch(t, "dev", func(store *pebble.DB) {
		dev.MtimeSec, dev.MtimeNsec = main.MtimeSec, main.MtimeNsec
		dev.CtimeSec, dev.CtimeNsec = main.CtimeSec, main.CtimeNsec
		encoded, err := pebblekv.EncodeInode(dev)
		require.NoError(t, err)
		require.NoError(t, store.Set(pebblekv.InodeKey(dev.Ino), encoded, pebble.Sync))
	})
	versionRead(t, e, e.sid, file.Inode.Ino, []byte("left"))
	versionRead(t, e, devSID, file.Inode.Ino, []byte("rght"))
	versionSession(t, e, "main", 103, true, nil)
	versionRead(t, e, 103, file.Inode.Ino, []byte("left"))
	versionSession(t, e, "main", 104, true, &base.CommitID)
	versionRead(t, e, 104, file.Inode.Ino, []byte("base"))
	changes, err := e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: "main", ToRef: "dev"})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, "f", changes[0].Path)
	require.Equal(t, meta.DiffChangeTypeModified, changes[0].ChangeType)
	require.Equal(t, []uint64{0}, changes[0].ModifiedBlockIndices)
	left, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	right, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "dev"})
	require.NoError(t, err)
	changes, err = e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: left.CommitID, ToRef: right.CommitID})
	require.NoError(t, err)
	require.Len(t, changes, 1)
	require.Equal(t, "f", changes[0].Path)
	require.Equal(t, []uint64{0}, changes[0].ModifiedBlockIndices)
	_, err = e.mgr.Create(meta.WithSessionID(e.ctx, &devSID), meta.CreateParams{ParentIno: 1, Name: []byte("other")})
	require.NoError(t, err)
	later, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "dev"})
	require.NoError(t, err)
	changes, err = e.mgr.Diff(e.ctx, meta.DiffParams{DiskID: e.disk.DiskID, FromRef: left.CommitID, ToRef: later.CommitID})
	require.NoError(t, err)
	require.Len(t, changes, 2)
	require.Equal(t, "f", changes[0].Path)
	require.Equal(t, []uint64{0}, changes[0].ModifiedBlockIndices)
	require.Equal(t, "other", changes[1].Path)
}

func TestDiffReadOnlyBranchSnapshot(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("f")})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("base"), SessionID: &e.sid})
	require.NoError(t, err)
	base, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("left"), SessionID: &e.sid})
	require.NoError(t, err)
	versionSession(t, e, "main", 103, true, nil)
	versionRead(t, e, 103, file.Inode.Ino, []byte("left"))
	var snapshotID, parentID string
	err = e.pool.QueryRow(e.ctx, `SELECT c.commit_id::text, c.parent_commit_id::text FROM client_sessions s
		JOIN commits c ON c.commit_id = s.commit_id WHERE s.session_id = $1`, 103).Scan(&snapshotID, &parentID)
	require.NoError(t, err)
	require.NotEqual(t, base.CommitID, snapshotID)
	require.Equal(t, base.CommitID, parentID)
	branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, &base.CommitID, branches[0].HeadCommitID)
	e.withBranch(t, "main", func(store *pebble.DB) { getPebbleValue(t, store, pebblekv.DirtyKey(file.Inode.Ino)) })
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("next"), SessionID: &e.sid})
	require.NoError(t, err)
	versionSession(t, e, "main", 103, true, nil)
	versionRead(t, e, 103, file.Inode.Ino, []byte("left"))
	versionSession(t, e, "main", 104, true, nil)
	versionRead(t, e, 104, file.Inode.Ino, []byte("next"))
	versionSession(t, e, "main", 105, true, &base.CommitID)
	versionRead(t, e, 105, file.Inode.Ino, []byte("base"))
	newHead, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	versionSession(t, e, "main", 106, true, nil)
	var cleanID string
	err = e.pool.QueryRow(e.ctx, `SELECT commit_id::text FROM client_sessions WHERE session_id = $1`, 106).Scan(&cleanID)
	require.NoError(t, err)
	require.Equal(t, newHead.CommitID, cleanID)
}

func TestDiffReadOnlyBranchSnapshotTransactionFailure(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("f")})
	require.NoError(t, err)
	commitDir := filepath.Join(e.root, "disks", e.disk.DiskID, "commits")
	before, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	failing := meta.New(&rollbackVersionStore{Store: db.NewStore(e.pool), pool: e.pool}, meta.WithStorageRoot(e.root))
	_, err = failing.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: 103, DiskID: e.disk.DiskID, ReadOnly: true})
	require.ErrorIs(t, err, errVersionTransaction)
	after, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	require.Equal(t, before, after)
	var count int
	require.NoError(t, e.pool.QueryRow(e.ctx, `SELECT count(*) FROM client_sessions WHERE session_id = $1`, 103).Scan(&count))
	require.Zero(t, count)
	e.withBranch(t, "main", func(store *pebble.DB) { getPebbleValue(t, store, pebblekv.DirtyKey(file.Inode.Ino)) })
}
