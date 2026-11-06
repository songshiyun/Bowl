package meta_test

import (
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 并发 RPC 必须在同一个常驻分支实例上保持数据一致，不得因重复打开或竞争而丢失写入。
func TestPebbleConcurrentRPCsKeepBranchConsistent(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("shared"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: bytes.Repeat([]byte{0x5a}, 4096), SessionID: &e.sid})
	require.NoError(t, err)

	errs := make(chan error, 4)
	go func() {
		_, readErr := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: 0, Size: 1024, SessionID: &e.sid})
		errs <- readErr
	}()
	go func() {
		_, writeErr := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 16, Data: []byte{0x11}, SessionID: &e.sid})
		errs <- writeErr
	}()
	go func() {
		_, lookupErr := e.mgr.Lookup(e.branchContext(), 1, []byte("shared"))
		errs <- lookupErr
	}()
	go func() {
		_, attrErr := e.mgr.GetAttr(e.branchContext(), ino)
		errs <- attrErr
	}()
	require.NoError(t, errors.Join(<-errs, <-errs, <-errs, <-errs))

	// 独立参考模型：初始 4 KiB 全为 0x5a，并发写入仅覆写偏移 16 的一个字节。
	want := bytes.Repeat([]byte{0x5a}, 4096)
	want[16] = 0x11
	read, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 4096, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, want, read.Data)
}

// 回档替换分支目录前必须驱逐常驻句柄，回档后读取必须返回快照内容且后续写入落在新目录。
func TestPebbleRollbackEvictsResidentBranchInstance(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("versioned"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("before"), SessionID: &e.sid})
	require.NoError(t, err)
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("after!"), SessionID: &e.sid})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	require.NoError(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "main", CommitID: commit.CommitID}))

	sid := uint64(202)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
	require.NoError(t, err)
	read, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 16, SessionID: &sid})
	require.NoError(t, err)
	require.Equal(t, []byte("before"), read.Data, "回档后不得读到旧目录的残留数据")

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 6, Data: []byte("!!"), SessionID: &sid})
	require.NoError(t, err)
	read, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 16, SessionID: &sid})
	require.NoError(t, err)
	require.Equal(t, []byte("before!!"), read.Data)
}
