package meta_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func metaBlobPath(root, disk, kind, name string, id uint64) string {
	return filepath.Join(root, "disks", disk, kind, name, "blobs", fmt.Sprintf("%02x", id&0xff), fmt.Sprintf("%016x.blob", id))
}

// 同一文件不同块的 1 MiB 写入必须并行进入 .blob 落盘阶段，而不是排队等待 Inode 锁。
func TestPebbleWriteFullBlocksRunOutsideInodeLock(t *testing.T) {
	e := setupPebbleCreate(t, 16*pebblekv.BlobSize)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("parallel"), Mode: 0o644})
	require.NoError(t, err)
	ino := file.Inode.Ino
	first := bytes.Repeat([]byte{0x11}, pebblekv.BlobSize)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: first, SessionID: &e.sid})
	require.NoError(t, err)

	branch, err := e.storage.OpenBranch(e.disk.DiskID, "main")
	require.NoError(t, err)
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	branch.Blobs().SetWriteHook(func() {
		entered <- struct{}{}
		select {
		case <-release:
		case <-time.After(10 * time.Second):
		}
	})
	defer func() {
		branch.Blobs().SetWriteHook(nil)
		require.NoError(t, branch.Close())
	}()

	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, offset := range []uint64{0, pebblekv.BlobSize} {
		data := bytes.Repeat([]byte{byte(0x20 + offset/pebblekv.BlobSize)}, pebblekv.BlobSize)
		go func(offset uint64, data []byte) {
			<-start
			_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Offset: offset, Data: data, SessionID: &e.sid})
			errs <- err
		}(offset, data)
	}
	close(start)
	for range 2 {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("两个整块写入未能并行进入 .blob 落盘阶段，分支锁或 Inode 锁仍在阻塞")
		}
	}
	close(release)
	for range 2 {
		require.NoError(t, <-errs)
	}
	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Offset: pebblekv.BlobSize, Size: pebblekv.BlobSize, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x21}, pebblekv.BlobSize), got.Data)
}

// 16 线程并发写不同文件与并发 GetAttr/Read 必须全部成功且数据隔离正确。
func TestPebbleConcurrentWritesAndReadsShareBranchReadLock(t *testing.T) {
	e := setupPebbleCreate(t, 64*pebblekv.BlobSize)
	const workers = 16
	inos := make([]uint64, workers)
	for i := range workers {
		file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte(fmt.Sprintf("f%02d", i)), Mode: 0o644})
		require.NoError(t, err)
		inos[i] = file.Inode.Ino
	}
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, workers*2)
	for i := range workers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			data := bytes.Repeat([]byte{byte(i + 1)}, pebblekv.BlobSize)
			_, err := e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: inos[i], Data: data, SessionID: &e.sid})
			errs <- err
			_, err = e.mgr.GetAttr(e.branchContext(), inos[i])
			errs <- err
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	for i := range workers {
		got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: inos[i], Size: pebblekv.BlobSize, SessionID: &e.sid})
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, pebblekv.BlobSize), got.Data)
	}
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

// 模拟宿主硬链接上限 EMLINK，Commit 与 Fork 都必须回退为流式拷贝并保持数据可用。
func TestPebbleCommitAndForkEMLINKFallback(t *testing.T) {
	pool, _ := setupIsolatedMetaTestDB(t, "pebble_emlink")
	root := t.TempDir()
	storage := pebblekv.NewManager(root, pebblekv.WithLinkFunc(func(string, string) error {
		return syscall.EMLINK
	}))
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(e2eContext(t), meta.CreateDiskParams{Name: "硬链接回退", MaxBytes: 8 << 20})
	require.NoError(t, err)
	ctx := e2eContext(t)
	sid := uint64(901)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: disk.DiskID})
	require.NoError(t, err)
	branchCtx := meta.WithSessionID(ctx, &sid)
	file, err := mgr.Create(branchCtx, meta.CreateParams{ParentIno: 1, Name: []byte("f"), Mode: 0o644})
	require.NoError(t, err)
	data := bytes.Repeat([]byte{0x6d}, pebblekv.BlobSize)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: data, SessionID: &sid})
	require.NoError(t, err)
	commit, err := mgr.Commit(ctx, meta.CommitParams{DiskID: disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	var ref pebblekv.BlobRef
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	value, closer, err := branch.DB().Get(pebblekv.DataKey(file.Inode.Ino, 0))
	require.NoError(t, err)
	ref, err = pebblekv.DecodeBlobRef(value)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.NoError(t, branch.Close())
	branchPath := metaBlobPath(root, disk.DiskID, "branches", "main", ref.ID)
	commitPath := metaBlobPath(root, disk.DiskID, "commits", commit.CommitID, ref.ID)
	branchInfo, err := os.Stat(branchPath)
	require.NoError(t, err)
	commitInfo, err := os.Stat(commitPath)
	require.NoError(t, err)
	require.False(t, os.SameFile(branchInfo, commitInfo), "Commit 在 EMLINK 时必须复制块文件")

	_, err = mgr.CreateBranch(ctx, meta.CreateBranchParams{DiskID: disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	forkPath := metaBlobPath(root, disk.DiskID, "branches", "dev", ref.ID)
	forkInfo, err := os.Stat(forkPath)
	require.NoError(t, err)
	require.False(t, os.SameFile(commitInfo, forkInfo), "Fork 在 EMLINK 时必须复制块文件")
	devSID := uint64(902)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: devSID, DiskID: disk.DiskID, BranchName: ptrString("dev")})
	require.NoError(t, err)
	got, err := mgr.Read(ctx, &meta.ReadRequest{Ino: file.Inode.Ino, Size: pebblekv.BlobSize, SessionID: &devSID})
	require.NoError(t, err)
	require.Equal(t, data, got.Data)
}

func ptrString(value string) *string { return &value }

func e2eContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}
