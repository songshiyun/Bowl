package meta_test

import (
	"context"
	"math"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func topologySymlink(t *testing.T, e *pebbleCreateEnv, parent uint64, name, target string) *meta.Inode {
	t.Helper()
	inode, err := e.mgr.Symlink(e.branchContext(), meta.SymlinkParams{ParentIno: parent, Name: []byte(name), Target: []byte(target)})
	require.NoError(t, err)
	return inode
}

func topologyDir(t *testing.T, e *pebbleCreateEnv, parent uint64, name string) *meta.Inode {
	t.Helper()
	inode, err := e.mgr.Mkdir(e.branchContext(), meta.MkdirParams{ParentIno: parent, Name: []byte(name), Mode: 0o755})
	require.NoError(t, err)
	return inode
}

func topologyRename(oldParent uint64, oldName string, newParent uint64, newName string, flags uint32) meta.RenameParams {
	return meta.RenameParams{OldParentIno: oldParent, OldName: []byte(oldName), NewParentIno: newParent, NewName: []byte(newName), Flags: flags}
}

func assertPebbleMissing(t *testing.T, store *pebble.DB, key []byte) {
	t.Helper()
	_, closer, err := store.Get(key)
	if closer != nil {
		require.NoError(t, closer.Close())
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func topologyStat(t *testing.T, store *pebble.DB) pebblekv.BranchStat {
	t.Helper()
	stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
	require.NoError(t, err)
	return stat
}

func TestPebbleUnlinkAndReclaim(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	inode := topologySymlink(t, e, 1, "file", "123456")
	_, err := e.mgr.Link(ctx, meta.LinkParams{Ino: inode.Ino, NewParentIno: 1, NewName: []byte("alias")})
	require.NoError(t, err)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	xattr := pebblekv.XattrKey(inode.Ino, []byte("user.test"))
	data := pebblekv.DataKey(inode.Ino, 0)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.NoError(t, store.Set(xattr, []byte("attr"), pebble.Sync))
		require.NoError(t, store.Set(data, []byte("data"), pebble.Sync))
	})
	require.NoError(t, e.mgr.Unlink(ctx, 1, []byte("alias")))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.EntryKey(1, []byte("alias")))
		child := readPebbleCreateInode(t, store, inode.Ino)
		stat := topologyStat(t, store)
		require.Equal(t, uint32(1), child.Nlink)
		require.Equal(t, uint64(2), stat.Inodes)
		require.Equal(t, uint64(6), stat.LogicalBytes)
		require.Equal(t, stat.NextWriteSeq-1, child.WriteSeq)
		require.Equal(t, stat.NextWriteSeq-1, readPebbleCreateInode(t, store, 1).WriteSeq)
		getPebbleValue(t, store, pebblekv.DirtyKey(inode.Ino))
		getPebbleValue(t, store, pebblekv.DirtyKey(1))
	})
	openCtx := meta.WithOpenInClient(ctx, true)
	require.NoError(t, e.mgr.Unlink(openCtx, 1, []byte("file")))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.EntryKey(1, []byte("file")))
		require.Zero(t, readPebbleCreateInode(t, store, inode.Ino).Nlink)
		require.Equal(t, []byte{1}, getPebbleValue(t, store, pebblekv.OrphanKey(inode.Ino)))
		require.Equal(t, []byte("attr"), getPebbleValue(t, store, xattr))
		require.Equal(t, []byte("data"), getPebbleValue(t, store, data))
		require.Equal(t, uint64(6), topologyStat(t, store).LogicalBytes)
	})
	got, err := e.mgr.GetAttr(ctx, inode.Ino)
	require.NoError(t, err)
	require.Zero(t, got.Attr.Nlink)
	params := meta.ReclaimOrphanParams{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: inode.Ino}
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, params))
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, params))
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		for _, key := range [][]byte{pebblekv.InodeKey(inode.Ino), pebblekv.OrphanKey(inode.Ino), xattr, data} {
			assertPebbleMissing(t, store, key)
		}
		stat := topologyStat(t, store)
		require.Equal(t, uint64(1), stat.Inodes)
		require.Zero(t, stat.LogicalBytes)
		getPebbleValue(t, store, pebblekv.DirtyKey(inode.Ino))
	})
	immediate := topologySymlink(t, e, 1, "immediate", "abc")
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.NoError(t, store.Set(pebblekv.XattrKey(immediate.Ino, []byte("x")), []byte("v"), pebble.Sync))
		require.NoError(t, store.Set(pebblekv.DataKey(immediate.Ino, 3), []byte("v"), pebble.Sync))
	})
	require.NoError(t, e.mgr.Unlink(meta.WithOpenInClient(ctx, false), 1, []byte("immediate")))
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		for _, key := range [][]byte{pebblekv.InodeKey(immediate.Ino), pebblekv.OrphanKey(immediate.Ino), pebblekv.XattrKey(immediate.Ino, []byte("x")), pebblekv.DataKey(immediate.Ino, 3)} {
			assertPebbleMissing(t, store, key)
		}
	})
}

func TestPebbleRmdir(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	dir := topologyDir(t, e, 1, "dir")
	file := topologySymlink(t, e, dir.Ino, "child", "x")
	require.ErrorIs(t, e.mgr.Rmdir(ctx, 1, []byte("dir")), meta.ErrNotEmpty)
	require.ErrorIs(t, e.mgr.Unlink(ctx, 1, []byte("dir")), meta.ErrIsDir)
	require.ErrorIs(t, e.mgr.Rmdir(ctx, dir.Ino, []byte("child")), meta.ErrNotDir)
	require.ErrorIs(t, e.mgr.Rmdir(ctx, 1, []byte(".")), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rmdir(ctx, 1, []byte("..")), meta.ErrNotEmpty)
	require.ErrorIs(t, e.mgr.Rmdir(ctx, file.Ino, nil), meta.ErrNotDir)
	require.NoError(t, e.mgr.Unlink(ctx, dir.Ino, []byte("child")))
	open := meta.WithOpenInClient(ctx, true)
	require.NoError(t, e.mgr.Rmdir(open, 1, []byte("dir")))
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.EntryKey(1, []byte("dir")))
		require.Equal(t, uint32(2), readPebbleCreateInode(t, store, 1).Nlink)
		require.Zero(t, readPebbleCreateInode(t, store, dir.Ino).Nlink)
		getPebbleValue(t, store, pebblekv.OrphanKey(dir.Ino))
	})
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, meta.ReclaimOrphanParams{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: dir.Ino}))
	other := topologyDir(t, e, 1, "other")
	require.NoError(t, e.mgr.Rmdir(ctx, 1, []byte("other")))
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(dir.Ino))
		assertPebbleMissing(t, store, pebblekv.InodeKey(other.Ino))
		require.Equal(t, uint64(1), topologyStat(t, store).Inodes)
	})
}

func TestPebbleRenameFiles(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	source := topologySymlink(t, e, 1, "source", "source")
	target := topologySymlink(t, e, 1, "target", "target!")
	_, err := e.mgr.Link(ctx, meta.LinkParams{Ino: source.Ino, NewParentIno: 1, NewName: []byte("alias")})
	require.NoError(t, err)
	var before pebblekv.BranchStat
	e.withBranch(t, "main", func(store *pebble.DB) { before = topologyStat(t, store) })
	for _, flags := range []uint32{0, meta.RenameExchange} {
		require.NoError(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "alias", flags)))
	}
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, before, topologyStat(t, store))
		for _, name := range []string{"source", "alias"} {
			entry, err := pebblekv.DecodeEntry(getPebbleValue(t, store, pebblekv.EntryKey(1, []byte(name))))
			require.NoError(t, err)
			require.Equal(t, source.Ino, entry.Ino)
		}
	})
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "alias", meta.RenameNoReplace)), meta.ErrAlreadyExists)
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "source", 0)))
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "source", meta.RenameNoReplace)), meta.ErrAlreadyExists)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "missing", meta.RenameExchange)), meta.ErrNotFound)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "target", meta.RenameNoReplace)), meta.ErrAlreadyExists)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.NoError(t, store.Set(pebblekv.XattrKey(target.Ino, []byte("x")), []byte("v"), pebble.Sync))
		require.NoError(t, store.Set(pebblekv.DataKey(target.Ino, 0), []byte("v"), pebble.Sync))
	})
	require.NoError(t, e.mgr.Rename(meta.WithOpenInClient(ctx, true), topologyRename(1, "source", 1, "target", 0)))
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 13, Inodes: 3}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.EntryKey(1, []byte("source")))
		entry, err := pebblekv.DecodeEntry(getPebbleValue(t, store, pebblekv.EntryKey(1, []byte("target"))))
		require.NoError(t, err)
		require.Equal(t, source.Ino, entry.Ino)
		require.Zero(t, readPebbleCreateInode(t, store, target.Ino).Nlink)
		getPebbleValue(t, store, pebblekv.OrphanKey(target.Ino))
		getPebbleValue(t, store, pebblekv.XattrKey(target.Ino, []byte("x")))
		getPebbleValue(t, store, pebblekv.DataKey(target.Ino, 0))
	})
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, meta.ReclaimOrphanParams{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: target.Ino}))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	second := topologySymlink(t, e, 1, "second", "1234")
	e.withBranch(t, "main", func(store *pebble.DB) {
		for _, ino := range []uint64{source.Ino, second.Ino} {
			require.NoError(t, store.Set(pebblekv.XattrKey(ino, []byte("x")), []byte("v"), pebble.Sync))
			require.NoError(t, store.Set(pebblekv.DataKey(ino, 3), []byte("v"), pebble.Sync))
		}
	})
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(1, "target", 1, "second", 0)))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(second.Ino))
		assertPebbleMissing(t, store, pebblekv.XattrKey(second.Ino, []byte("x")))
		assertPebbleMissing(t, store, pebblekv.DataKey(second.Ino, 3))
		getPebbleValue(t, store, pebblekv.XattrKey(source.Ino, []byte("x")))
		getPebbleValue(t, store, pebblekv.DataKey(source.Ino, 3))
		require.Equal(t, uint32(2), readPebbleCreateInode(t, store, source.Ino).Nlink)
	})
}

func TestPebbleRenameDirectoriesAndExchange(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	a := topologyDir(t, e, 1, "a")
	b := topologyDir(t, e, 1, "b")
	child := topologyDir(t, e, a.Ino, "child")
	grand := topologyDir(t, e, child.Ino, "grand")
	topologySymlink(t, e, grand.Ino, "nested", "n")
	file := topologySymlink(t, e, b.Ino, "file", "data")
	other := topologyDir(t, e, b.Ino, "other")
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "a", grand.Ino, "loop", 0)), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "a", child.Ino, "grand", meta.RenameExchange)), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(grand.Ino, "nested", 1, "a", meta.RenameExchange)), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(a.Ino, "child", b.Ino, "file", 0)), meta.ErrNotDir)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(b.Ino, "file", a.Ino, "child", 0)), meta.ErrIsDir)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(b.Ino, "other", a.Ino, "child", 0)), meta.ErrNotEmpty)
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(a.Ino, "child", b.Ino, "file", meta.RenameExchange)))
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, uint32(2), readPebbleCreateInode(t, store, a.Ino).Nlink)
		require.Equal(t, uint32(4), readPebbleCreateInode(t, store, b.Ino).Nlink)
		require.Equal(t, b.Ino, readPebbleCreateInode(t, store, child.Ino).ParentIno)
		entry, err := pebblekv.DecodeEntry(getPebbleValue(t, store, pebblekv.EntryKey(a.Ino, []byte("child"))))
		require.NoError(t, err)
		require.Equal(t, file.Ino, entry.Ino)
	})
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(b.Ino, "file", a.Ino, "moved", 0)))
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, uint32(3), readPebbleCreateInode(t, store, a.Ino).Nlink)
		require.Equal(t, uint32(3), readPebbleCreateInode(t, store, b.Ino).Nlink)
		require.Equal(t, a.Ino, readPebbleCreateInode(t, store, child.Ino).ParentIno)
	})
	empty := topologyDir(t, e, a.Ino, "empty")
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(b.Ino, "other", a.Ino, "empty", 0)))
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(empty.Ino))
		require.Equal(t, uint32(4), readPebbleCreateInode(t, store, a.Ino).Nlink)
		require.Equal(t, uint32(2), readPebbleCreateInode(t, store, b.Ino).Nlink)
		require.Equal(t, a.Ino, readPebbleCreateInode(t, store, other.Ino).ParentIno)
	})
	victim := topologyDir(t, e, a.Ino, "victim")
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(a.Ino, "empty", a.Ino, "victim", 0)))
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(victim.Ino))
		require.Equal(t, uint32(4), readPebbleCreateInode(t, store, a.Ino).Nlink)
	})
}

func TestPebbleTopologyValidationAndReclaimSessions(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	file := topologySymlink(t, e, 1, "file", "x")
	dir := topologyDir(t, e, 1, "dir")
	for _, op := range []func(context.Context, uint64, []byte) error{e.mgr.Unlink, e.mgr.Rmdir} {
		require.ErrorIs(t, op(ctx, 0, []byte("x")), meta.ErrInvalidArgument)
		require.ErrorIs(t, op(ctx, math.MaxUint64, []byte("x")), meta.ErrInvalidArgument)
		require.ErrorIs(t, op(ctx, 1, []byte("x/y")), meta.ErrInvalidArgument)
		require.ErrorIs(t, op(ctx, 1, []byte(strings.Repeat("x", 256))), meta.ErrNameTooLong)
		require.ErrorIs(t, op(ctx, 9999, []byte("x")), meta.ErrNotFound)
		require.ErrorIs(t, op(ctx, file.Ino, nil), meta.ErrNotDir)
		require.ErrorIs(t, op(ctx, 1, nil), meta.ErrNotFound)
		require.ErrorIs(t, op(meta.WithOpenInClient(e.ctx, false), 1, []byte("x")), meta.ErrInvalidArgument)
	}
	require.ErrorIs(t, e.mgr.Unlink(ctx, 1, []byte(".")), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Unlink(ctx, 1, []byte("..")), meta.ErrInvalidArgument)
	for _, tc := range []struct {
		params meta.RenameParams
		want   error
	}{
		{topologyRename(0, "x", 1, "y", 0), meta.ErrInvalidArgument},
		{topologyRename(1, "x", 1, "y", meta.RenameWhiteout), meta.ErrNotSupported},
		{topologyRename(1, "x", 1, "y", meta.RenameNoReplace|meta.RenameExchange), meta.ErrInvalidArgument},
		{topologyRename(1, "x", 1, "y", 8), meta.ErrInvalidArgument},
		{topologyRename(1, "", 1, "y", 0), meta.ErrNotFound},
		{topologyRename(1, ".", 1, "y", 0), meta.ErrInvalidArgument},
		{topologyRename(1, "x", 1, "..", 0), meta.ErrInvalidArgument},
		{topologyRename(1, "x", 1, "", 0), meta.ErrInvalidArgument},
		{topologyRename(file.Ino, "x", 1, "y", 0), meta.ErrNotDir},
		{topologyRename(1, "file", 9999, "y", 0), meta.ErrNotFound},
	} {
		require.ErrorIs(t, e.mgr.Rename(ctx, tc.params), tc.want)
	}
	require.ErrorIs(t, e.mgr.Rename(meta.WithOpenInClient(e.ctx, true), topologyRename(1, "file", 1, "z", 0)), meta.ErrInvalidArgument)
	params := meta.ReclaimOrphanParams{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: file.Ino}
	require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, params), meta.ErrInvalidArgument)
	for _, bad := range []meta.ReclaimOrphanParams{
		{DiskID: e.disk.DiskID, Ino: file.Ino},
		{SessionID: math.MaxUint64, DiskID: e.disk.DiskID, Ino: file.Ino},
		{SessionID: e.sid, DiskID: "bad", Ino: file.Ino},
		{SessionID: e.sid, DiskID: "00000000-0000-0000-0000-000000000000", Ino: file.Ino},
		{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: 0},
		{SessionID: e.sid, DiskID: e.disk.DiskID, Ino: math.MaxUint64},
	} {
		require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, bad), meta.ErrInvalidArgument)
	}
	for _, branch := range []string{"", ".", "..", "bad/name", "bad\\name", "bad\nname", strings.Repeat("x", 256), "other"} {
		params.BranchName = &branch
		require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, params), meta.ErrInvalidArgument)
	}
	params.BranchName = nil
	params.DiskID = "11111111-1111-7111-8111-111111111111"
	require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, params), meta.ErrInvalidArgument)
	params.DiskID = e.disk.DiskID
	params.SessionID = 9999
	require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, params), meta.ErrBadFileHandle)
	reader := uint64(104)
	_, err := e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: reader, DiskID: e.disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	params.SessionID = reader
	require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, params), meta.ErrPerm)
	params.SessionID = e.sid
	params.Ino = 9999
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.NoError(t, store.Set(pebblekv.OrphanKey(params.Ino), []byte{1}, pebble.Sync))
	})
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, params))
	e.withBranch(t, "main", func(store *pebble.DB) { assertPebbleMissing(t, store, pebblekv.OrphanKey(params.Ino)) })
	changePebbleCreateInode(t, e, dir.Ino, func(inode *pebblekv.Inode) { inode.Nlink = 0 })
	require.ErrorIs(t, e.mgr.Rmdir(ctx, dir.Ino, nil), meta.ErrNotFound)
	_, err = e.pool.Exec(e.ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() WHERE session_id = $1`, int64(e.sid))
	require.NoError(t, err)
	awaitSessionRejected(t, e.mgr, e.ctx, e.sid)
	require.ErrorIs(t, e.mgr.ReclaimOrphan(e.ctx, params), meta.ErrBadFileHandle)
	require.ErrorIs(t, e.mgr.Unlink(ctx, 1, []byte("file")), meta.ErrBadFileHandle)
}

func TestPebbleTopologyConcurrentUnlink(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	inode := topologySymlink(t, e, 1, "file", "123")
	_, err := e.mgr.Link(ctx, meta.LinkParams{Ino: inode.Ino, NewParentIno: 1, NewName: []byte("alias")})
	require.NoError(t, err)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, name := range []string{"file", "alias"} {
		wg.Add(1)
		go func(name string) {
			defer wg.Done()
			errs <- e.mgr.Unlink(ctx, 1, []byte(name))
		}(name)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(inode.Ino))
		require.Equal(t, uint64(1), topologyStat(t, store).Inodes)
	})
}

func TestPebbleTopologySharedQuotaAcrossBranches(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	inode := topologySymlink(t, e, 1, "shared", "123456")
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	storage := e.storage
	branch, err := storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	commit := uuid.NewString()
	require.NoError(t, branch.Checkpoint(commit))
	require.NoError(t, branch.Close())
	require.NoError(t, storage.Fork(e.disk.DiskID, commit, "feature"))
	_, err = e.pool.Exec(e.ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, e.disk.DiskID)
	require.NoError(t, err)
	require.NoError(t, quota.ReplaceBranch("feature", map[uint64]uint64{1: 0, inode.Ino: 6}))
	feature := "feature"
	featureSID := uint64(202)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featureSID, DiskID: e.disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	featureCtx := meta.WithSessionID(e.ctx, &featureSID)
	require.NoError(t, e.mgr.Unlink(e.branchContext(), 1, []byte("shared")))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	got, err := e.mgr.Lookup(featureCtx, 1, []byte("shared"))
	require.NoError(t, err)
	require.Equal(t, inode.Ino, got.Ino)
	e.withBranch(t, "main", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(inode.Ino))
		require.Equal(t, uint64(1), topologyStat(t, store).Inodes)
	})
	require.NoError(t, e.mgr.Unlink(meta.WithOpenInClient(featureCtx, true), 1, []byte("shared")))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	require.NoError(t, e.mgr.ReclaimOrphan(e.ctx, meta.ReclaimOrphanParams{SessionID: featureSID, DiskID: e.disk.DiskID, BranchName: &feature, Ino: inode.Ino}))
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
	e.withBranch(t, "feature", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.InodeKey(inode.Ino))
		require.Equal(t, uint64(1), topologyStat(t, store).Inodes)
	})
}

func TestPebbleRenameHardlinkOverwriteAndLimits(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	source := topologySymlink(t, e, 1, "source", "123")
	target := topologySymlink(t, e, 1, "target", "4567")
	_, err := e.mgr.Link(ctx, meta.LinkParams{Ino: target.Ino, NewParentIno: 1, NewName: []byte("remaining")})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Rename(ctx, topologyRename(1, "source", 1, "target", 0)))
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 7, Inodes: 3}, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		stat := topologyStat(t, store)
		require.Equal(t, uint32(1), readPebbleCreateInode(t, store, target.Ino).Nlink)
		require.Equal(t, stat.NextWriteSeq-1, readPebbleCreateInode(t, store, target.Ino).WriteSeq)
		require.Equal(t, stat.NextWriteSeq-1, readPebbleCreateInode(t, store, source.Ino).WriteSeq)
		assertPebbleMissing(t, store, pebblekv.OrphanKey(target.Ino))
	})
	a := topologyDir(t, e, 1, "a")
	b := topologyDir(t, e, 1, "b")
	dir := topologyDir(t, e, a.Ino, "dir")
	file := topologySymlink(t, e, b.Ino, "file", "x")
	changePebbleCreateInode(t, e, b.Ino, func(inode *pebblekv.Inode) { inode.Nlink = meta.MaxNlink })
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(a.Ino, "dir", b.Ino, "new", 0)), meta.ErrTooManyLinks)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(a.Ino, "dir", b.Ino, "file", meta.RenameExchange)), meta.ErrTooManyLinks)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(b.Ino, "file", a.Ino, "dir", meta.RenameExchange)), meta.ErrTooManyLinks)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, a.Ino, readPebbleCreateInode(t, store, dir.Ino).ParentIno)
		require.Equal(t, uint32(1), readPebbleCreateInode(t, store, file.Ino).Nlink)
		getPebbleValue(t, store, pebblekv.EntryKey(a.Ino, []byte("dir")))
		getPebbleValue(t, store, pebblekv.EntryKey(b.Ino, []byte("file")))
	})
}

func TestPebbleRenameRejectsBrokenParentChain(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	source := topologyDir(t, e, 1, "source")
	a := topologyDir(t, e, 1, "a")
	destination := topologyDir(t, e, a.Ino, "destination")
	changePebbleCreateInode(t, e, a.Ino, func(inode *pebblekv.Inode) { inode.ParentIno = a.Ino })
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "source", destination.Ino, "moved", 0)), meta.ErrInvalidArgument)
	changePebbleCreateInode(t, e, a.Ino, func(inode *pebblekv.Inode) { inode.ParentIno = 9999 })
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "source", destination.Ino, "moved", 0)), meta.ErrNotFound)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, uint64(1), readPebbleCreateInode(t, store, source.Ino).ParentIno)
		getPebbleValue(t, store, pebblekv.EntryKey(1, []byte("source")))
		assertPebbleMissing(t, store, pebblekv.EntryKey(destination.Ino, []byte("moved")))
	})
}

func TestPebbleTopologySequenceExhaustionKeepsState(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	ctx := e.branchContext()
	file := topologySymlink(t, e, 1, "file", "abc")
	dir := topologyDir(t, e, 1, "dir")
	var before pebblekv.BranchStat
	e.withBranch(t, "main", func(store *pebble.DB) {
		before = topologyStat(t, store)
		before.NextWriteSeq = math.MaxUint64
		require.NoError(t, store.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(before), pebble.Sync))
	})
	require.ErrorIs(t, e.mgr.Unlink(ctx, 1, []byte("file")), meta.ErrNoSpace)
	require.ErrorIs(t, e.mgr.Rmdir(ctx, 1, []byte("dir")), meta.ErrNoSpace)
	require.ErrorIs(t, e.mgr.Rename(ctx, topologyRename(1, "file", 1, "new", 0)), meta.ErrNoSpace)
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, before, topologyStat(t, store))
		require.Equal(t, uint32(1), readPebbleCreateInode(t, store, file.Ino).Nlink)
		require.Equal(t, uint32(2), readPebbleCreateInode(t, store, dir.Ino).Nlink)
		getPebbleValue(t, store, pebblekv.EntryKey(1, []byte("file")))
		getPebbleValue(t, store, pebblekv.EntryKey(1, []byte("dir")))
		assertPebbleMissing(t, store, pebblekv.EntryKey(1, []byte("new")))
	})
}
