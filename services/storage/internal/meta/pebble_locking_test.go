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
	first, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("f00"), Mode: 0o644})
	require.NoError(t, err)
	second, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("f01"), Mode: 0o644})
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
		_, attrErr := e.mgr.GetAttr(e.branchContext(), first.Inode.Ino)
		errs <- attrErr
	}()
	go func() {
		_, attrErr := e.mgr.GetAttr(e.branchContext(), second.Inode.Ino)
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
	ctx := meta.WithSessionID(e.ctx, &sid)
	file, err := mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("atime"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("data"), SessionID: &sid})
	require.NoError(t, err)
	old := time.Unix(1, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, ino, meta.SetAttrInput{Atime: &old, SessionID: &sid})
	require.NoError(t, err)

	_, err = mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 4, SessionID: &sid})
	require.NoError(t, err)
	attr, err := mgr.GetAttr(ctx, ino)
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
