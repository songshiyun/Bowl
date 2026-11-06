package meta_test

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// 两个并发写不同文件与并发 GetAttr/Read 必须全部成功且数据隔离正确。
func TestPebbleConcurrentWritesAndReadsShareBranchReadLock(t *testing.T) {
	e := setupPebbleCreate(t, 8*pebblekv.BlobSize)
	first, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("f00"), Mode: 0o644, SessionID: &e.sid})
	require.NoError(t, err)
	second, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("f01"), Mode: 0o644, SessionID: &e.sid})
	require.NoError(t, err)

	errs := make(chan error, 4)
	go func() {
		_, writeErr := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: first.Inode.Ino, Data: bytes.Repeat([]byte{1}, pebblekv.BlobSize), SessionID: &e.sid})
		errs <- writeErr
	}()
	go func() {
		_, writeErr := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: second.Inode.Ino, Data: bytes.Repeat([]byte{2}, pebblekv.BlobSize), SessionID: &e.sid})
		errs <- writeErr
	}()
	go func() {
		_, attrErr := e.mgr.GetAttr(e.ctx, &e.sid, first.Inode.Ino)
		errs <- attrErr
	}()
	go func() {
		_, attrErr := e.mgr.GetAttr(e.ctx, &e.sid, second.Inode.Ino)
		errs <- attrErr
	}()
	require.NoError(t, errors.Join(<-errs, <-errs, <-errs, <-errs))

	firstData, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: first.Inode.Ino, Size: pebblekv.BlobSize, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{1}, pebblekv.BlobSize), firstData.Data)
	secondData, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: second.Inode.Ino, Size: pebblekv.BlobSize, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{2}, pebblekv.BlobSize), secondData.Data)
}

// Read 热路径只登记内存脏 atime，GetAttr 立即可见，Fsync 屏障才真正刷入 Pebble。
func TestPebbleReadAtimeDeferredUntilBarrier(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	mgr := meta.New(db.NewStore(e.pool), meta.WithStorageRoot(e.root), meta.WithPebbleManager(e.storage),
		meta.WithAtimeMode(meta.AtimeModeStrictAtime))
	sid := uint64(777)
	_, err := mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
	require.NoError(t, err)
	file, err := mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("atime"), Mode: 0o644, SessionID: &sid})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("data"), SessionID: &sid})
	require.NoError(t, err)
	old := time.Unix(1, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Atime: &old, SessionID: &sid})
	require.NoError(t, err)

	_, err = mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 4, SessionID: &sid})
	require.NoError(t, err)
	attr, err := mgr.GetAttr(e.ctx, &sid, ino)
	require.NoError(t, err)
	require.True(t, attr.Attr.Atime.After(old), "GetAttr 必须立即观测到内存中登记的 atime")

	// 直接读取 Pebble 证明 Read 热路径尚未同步提交批次。
	e.withBranch(t, "main", func(store *pebble.DB) {
		inode := readPebbleCreateInode(t, store, ino)
		require.Equal(t, old.Unix(), inode.AtimeSec)
	})

	require.NoError(t, mgr.Fsync(e.ctx, meta.FsyncParams{Ino: ino, SessionID: &sid}))
	e.withBranch(t, "main", func(store *pebble.DB) {
		inode := readPebbleCreateInode(t, store, ino)
		require.Greater(t, inode.AtimeSec, old.Unix(), "Fsync 屏障必须把内存脏 atime 刷入 Pebble")
	})
}

// Lookup、GetAttr、Readlink、StatFS 与 Xattr 系列入口必须要求显式 SessionID 并校验静态边界。
func TestPebbleReadAndXattrRequireExplicitSessionAndValidateBounds(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	symlink, err := e.mgr.Symlink(e.ctx, meta.SymlinkParams{
		SessionID: &e.sid, ParentIno: 1, Name: []byte("ln"), Target: []byte("target"),
	})
	require.NoError(t, err)
	file, err := e.mgr.Create(e.ctx, meta.CreateParams{
		ParentIno: 1, Name: []byte("reg"), Mode: 0o644, SessionID: &e.sid,
	})
	require.NoError(t, err)

	// 显式 sessionID 为 nil 时必须拒绝。
	_, err = e.mgr.Lookup(e.ctx, nil, 1, []byte("reg"))
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.GetAttr(e.ctx, nil, file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Readlink(e.ctx, nil, symlink.Ino)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.StatFS(e.ctx, nil, 1)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.SetXattr(e.ctx, meta.SetXattrParams{
		Ino: file.Inode.Ino, Name: []byte("user.k"), Value: []byte("v"),
	}), meta.ErrInvalidArgument)
	_, err = e.mgr.GetXattr(e.ctx, nil, file.Inode.Ino, []byte("user.k"))
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.ListXattr(e.ctx, nil, file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.RemoveXattr(e.ctx, nil, file.Inode.Ino, []byte("user.k")), meta.ErrInvalidArgument)

	// 节点创建与目录拓扑操作在未提供显式 SessionID 时也必须拒绝。
	_, err = e.mgr.Mknod(e.ctx, meta.MknodParams{ParentIno: 1, Name: []byte("nod"), Mode: meta.S_IFREG | 0o644})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Mkdir(e.ctx, meta.MkdirParams{ParentIno: 1, Name: []byte("dir"), Mode: 0o755})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Symlink(e.ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("sym"), Target: []byte("target")})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Link(e.ctx, meta.LinkParams{Ino: file.Inode.Ino, NewParentIno: 1, NewName: []byte("hard")})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Unlink(e.ctx, nil, 1, []byte("reg"), false), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rmdir(e.ctx, nil, 1, []byte("dir")), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rename(e.ctx, meta.RenameParams{OldParentIno: 1, OldName: []byte("reg"), NewParentIno: 1, NewName: []byte("reg2")}), meta.ErrInvalidArgument)

	// 静态入参边界校验。
	_, err = e.mgr.Lookup(e.ctx, &e.sid, 0, []byte("reg"))
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Lookup(e.ctx, &e.sid, 1, bytes.Repeat([]byte("a"), meta.MaxFileNameBytes+1))
	require.ErrorIs(t, err, meta.ErrNameTooLong)
	_, err = e.mgr.Lookup(e.ctx, &e.sid, 1, []byte("a/b"))
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.GetAttr(e.ctx, &e.sid, 0)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Readlink(e.ctx, &e.sid, 0)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.StatFS(e.ctx, &e.sid, 0)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.SetXattr(e.ctx, meta.SetXattrParams{
		SessionID: &e.sid, Ino: 0, Name: []byte("user.k"),
	}), meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.SetXattr(e.ctx, meta.SetXattrParams{
		SessionID: &e.sid, Ino: file.Inode.Ino, Name: bytes.Repeat([]byte("k"), meta.MaxXattrNameBytes+1),
	}), meta.ErrRange)
	require.ErrorIs(t, e.mgr.SetXattr(e.ctx, meta.SetXattrParams{
		SessionID: &e.sid, Ino: file.Inode.Ino, Name: []byte("user.k"), Value: make([]byte, meta.MaxXattrValueBytes+1),
	}), meta.ErrRange)
	_, err = e.mgr.GetXattr(e.ctx, &e.sid, file.Inode.Ino, nil)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.ListXattr(e.ctx, &e.sid, 0)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.RemoveXattr(e.ctx, &e.sid, file.Inode.Ino, []byte{'a', 0}), meta.ErrInvalidArgument)

	// 显式传入 SessionID 时正常完成读写。
	lookedUp, err := e.mgr.Lookup(e.ctx, &e.sid, 1, []byte("reg"))
	require.NoError(t, err)
	require.Equal(t, file.Inode.Ino, lookedUp.Ino)
	target, err := e.mgr.Readlink(e.ctx, &e.sid, symlink.Ino)
	require.NoError(t, err)
	require.Equal(t, []byte("target"), target)
	statfs, err := e.mgr.StatFS(e.ctx, &e.sid, 1)
	require.NoError(t, err)
	require.Equal(t, uint64(1<<20), statfs.TotalBytes)
	require.NoError(t, e.mgr.SetXattr(e.ctx, meta.SetXattrParams{
		SessionID: &e.sid, Ino: file.Inode.Ino, Name: []byte("user.k"), Value: []byte("val"),
	}))
	val, err := e.mgr.GetXattr(e.ctx, &e.sid, file.Inode.Ino, []byte("user.k"))
	require.NoError(t, err)
	require.Equal(t, []byte("val"), val)
	names, err := e.mgr.ListXattr(e.ctx, &e.sid, file.Inode.Ino)
	require.NoError(t, err)
	require.Equal(t, [][]byte{[]byte("user.k")}, names)
	require.NoError(t, e.mgr.RemoveXattr(e.ctx, &e.sid, file.Inode.Ino, []byte("user.k")))
}
