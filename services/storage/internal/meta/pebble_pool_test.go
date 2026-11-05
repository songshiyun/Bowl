package meta_test

import (
	"bytes"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// 并发 RPC 必须复用同一个常驻分支实例，不能每次请求重新 pebble.Open。
func TestPebbleConcurrentRPCsReuseResidentBranchInstance(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("shared"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: bytes.Repeat([]byte{0x5a}, 4096), SessionID: &e.sid})
	require.NoError(t, err)
	opens := e.storage.Stats().BranchOpens
	require.Equal(t, int64(1), opens)

	const workers = 32
	var wg sync.WaitGroup
	errs := make(chan error, workers)
	for i := range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var err error
			switch i % 4 {
			case 0:
				_, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: uint64(i) * 16, Size: 1024, SessionID: &e.sid})
			case 1:
				_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: uint64(i) * 16, Data: []byte{byte(i)}, SessionID: &e.sid})
			case 2:
				_, err = e.mgr.Lookup(e.branchContext(), 1, []byte("shared"))
			default:
				_, err = e.mgr.GetAttr(e.branchContext(), ino)
			}
			errs <- err
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	stats := e.storage.Stats()
	require.Equal(t, opens, stats.BranchOpens, "并发 RPC 不得重复 pebble.Open")
	require.Greater(t, stats.BranchReuses, int64(workers))
	require.Equal(t, 1, stats.ResidentBranches)
}

// 回档替换分支目录前必须驱逐常驻句柄，替换后重新打开且不残留旧目录实例。
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
	require.Equal(t, []byte("before"), read.Data)
	require.Equal(t, 1, e.storage.Stats().ResidentBranches, "回档后不得残留旧目录的常驻句柄")

	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: 6, Data: []byte("!!"), SessionID: &sid})
	require.NoError(t, err)
	read, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 16, SessionID: &sid})
	require.NoError(t, err)
	require.Equal(t, []byte("before!!"), read.Data)
}
