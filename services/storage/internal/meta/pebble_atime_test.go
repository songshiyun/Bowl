package meta_test

import (
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func newStrictAtimeManager(t *testing.T, e *pebbleCreateEnv) meta.Manager {
	t.Helper()
	return meta.New(
		db.NewStore(e.pool),
		meta.WithStorageRoot(e.root),
		meta.WithSessionLeaseTTL(time.Minute),
		meta.WithPebbleManager(e.storage),
		meta.WithAtimeMode(meta.AtimeModeStrictAtime),
	)
}

func assertSnapshotInodeAtimeAfter(t *testing.T, storage *pebblekv.Manager, diskID, commitID string, ino uint64, old time.Time) {
	t.Helper()
	snap, err := storage.OpenCommit(diskID, commitID)
	require.NoError(t, err)
	defer func() { require.NoError(t, snap.Close()) }()
	inode := readPebbleCreateInode(t, snap.DB(), ino)
	got := time.Unix(inode.AtimeSec, int64(inode.AtimeNsec)).UTC()
	require.True(t, got.After(old), "快照 %s 中节点 %d 的 atime (%v) 必须晚于旧时间 (%v)", commitID, ino, got, old)
}

// TestPebbleAtimeOverlayAndSnapshotBarriers 验证 Read / Readlink 产生的内存脏 atime：
// 1. 在 GetAttr 与 Lookup 中立即可见（此时尚未写入底层 Pebble）；
// 2. 在 Commit、CreateBranch（Fork）、Merge 及只读会话绑定（RenewBranchSession ReadOnly）时 100% 自动持久化至快照 Pebble。
func TestPebbleAtimeOverlayAndSnapshotBarriers(t *testing.T) {
	e := setupPebbleCreate(t, 4<<20)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	mgr := newStrictAtimeManager(t, e)

	sid := uint64(801)
	_, err := mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
	require.NoError(t, err)

	reg, err := mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("reg.txt"), Mode: 0o644, SessionID: &sid})
	require.NoError(t, err)
	regIno := reg.Inode.Ino
	_, err = mgr.Write(e.ctx, &meta.WriteRequest{Ino: regIno, Data: []byte("payload"), SessionID: &sid})
	require.NoError(t, err)

	sym, err := mgr.Symlink(e.ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("sym.lnk"), Target: []byte("reg.txt"), SessionID: &sid})
	require.NoError(t, err)
	symIno := sym.Ino

	t1 := time.Unix(100, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, regIno, meta.SetAttrInput{Atime: &t1, SessionID: &sid})
	require.NoError(t, err)
	_, err = mgr.SetAttr(e.ctx, symIno, meta.SetAttrInput{Atime: &t1, SessionID: &sid})
	require.NoError(t, err)

	// 先提交基准版本，清理初始创建的 dirty 标记。
	baseCommit, err := mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main", Message: "base"})
	require.NoError(t, err)

	// 1. Read 与 Readlink 产生内存脏 atime，GetAttr 与 Lookup 必须立即可见，且此时 Pebble 尚未落盘。
	_, err = mgr.Read(e.ctx, &meta.ReadRequest{Ino: regIno, Size: 7, SessionID: &sid})
	require.NoError(t, err)
	target, err := mgr.Readlink(e.ctx, &sid, symIno)
	require.NoError(t, err)
	require.Equal(t, []byte("reg.txt"), target)

	regAttr, err := mgr.GetAttr(e.ctx, &sid, regIno)
	require.NoError(t, err)
	require.True(t, regAttr.Attr.Atime.After(t1), "GetAttr 必须立即观测到 Read 登记的内存脏 atime")
	symAttr, err := mgr.GetAttr(e.ctx, &sid, symIno)
	require.NoError(t, err)
	require.True(t, symAttr.Attr.Atime.After(t1), "GetAttr 必须立即观测到 Readlink 登记的内存脏 atime")

	regLookup, err := mgr.Lookup(e.ctx, &sid, 1, []byte("reg.txt"))
	require.NoError(t, err)
	require.Equal(t, regAttr.Attr.Atime, regLookup.Attr.Atime, "Lookup 必须立即叠加内存脏 atime")
	symLookup, err := mgr.Lookup(e.ctx, &sid, 1, []byte("sym.lnk"))
	require.NoError(t, err)
	require.Equal(t, symAttr.Attr.Atime, symLookup.Attr.Atime, "Lookup 必须立即叠加 Readlink 的内存脏 atime")

	// 直接读取分支 Pebble 验证此时尚未同步刷盘。
	e.withBranch(t, "main", func(store *pebble.DB) {
		require.Equal(t, t1.Unix(), readPebbleCreateInode(t, store, regIno).AtimeSec)
		require.Equal(t, t1.Unix(), readPebbleCreateInode(t, store, symIno).AtimeSec)
	})

	// 2. Commit 屏障：必须自动将内存脏 atime 刷入 Pebble 并持久化至新 Commit 快照。
	commit1, err := mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main", Message: "atime-commit"})
	require.NoError(t, err)
	require.NotEqual(t, baseCommit.CommitID, commit1.CommitID)
	assertSnapshotInodeAtimeAfter(t, e.storage, e.disk.DiskID, commit1.CommitID, regIno, t1)
	assertSnapshotInodeAtimeAfter(t, e.storage, e.disk.DiskID, commit1.CommitID, symIno, t1)

	// 3. CreateBranch（Fork）屏障：当源分支仅有内存脏 atime 时，CreateBranch 必须判定分支为脏并生成包含新 atime 的 Fork 快照。
	t2 := time.Unix(200, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, regIno, meta.SetAttrInput{Atime: &t2, SessionID: &sid})
	require.NoError(t, err)
	commitBeforeFork, err := mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	_, err = mgr.Read(e.ctx, &meta.ReadRequest{Ino: regIno, Size: 7, SessionID: &sid})
	require.NoError(t, err)
	forkBranch, err := mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "forked"})
	require.NoError(t, err)
	require.NotNil(t, forkBranch.ForkCommitID)
	require.NotEqual(t, commitBeforeFork.CommitID, *forkBranch.ForkCommitID, "源分支存在内存脏 atime 时 CreateBranch 必须生成新快照")
	assertSnapshotInodeAtimeAfter(t, e.storage, e.disk.DiskID, *forkBranch.ForkCommitID, regIno, t2)

	// 4. 只读会话快照绑定屏障：当分支仅有内存脏 atime 时，绑定不指定 CommitID 的只读会话必须生成新快照并持久化该 atime。
	t3 := time.Unix(300, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, symIno, meta.SetAttrInput{Atime: &t3, SessionID: &sid})
	require.NoError(t, err)
	_, err = mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	_, err = mgr.Readlink(e.ctx, &sid, symIno)
	require.NoError(t, err)
	roSID := uint64(802)
	_, err = mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{
		SessionID: roSID, DiskID: e.disk.DiskID, ReadOnly: true,
	})
	require.NoError(t, err)
	roSymAttr, err := mgr.GetAttr(e.ctx, &roSID, symIno)
	require.NoError(t, err)
	require.True(t, roSymAttr.Attr.Atime.After(t3), "只读会话快照必须包含绑定前 Readlink 产生的脏 atime")
	require.NoError(t, mgr.CloseSession(e.ctx, roSID))

	// 5. Merge 屏障：源分支或目标分支上的内存脏 atime 在 Merge 固定（pin）快照与三路合并后必须持久化至合并提交快照。
	t4 := time.Unix(400, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, regIno, meta.SetAttrInput{Atime: &t4, SessionID: &sid})
	require.NoError(t, err)
	_, err = mgr.SetAttr(e.ctx, symIno, meta.SetAttrInput{Atime: &t4, SessionID: &sid})
	require.NoError(t, err)
	mergeBase, err := mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)

	_, err = mgr.CreateBranch(e.ctx, meta.CreateBranchParams{
		DiskID: e.disk.DiskID, Name: "src-branch", SourceCommitID: &mergeBase.CommitID,
	})
	require.NoError(t, err)
	srcSID := uint64(803)
	srcBranchName := "src-branch"
	_, err = mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{
		SessionID: srcSID, DiskID: e.disk.DiskID, BranchName: &srcBranchName,
	})
	require.NoError(t, err)
	// 在源分支仅执行 Read（产生 regIno 内存脏 atime，无其他写入），在目标分支 main 仅执行 Readlink（产生 symIno 内存脏 atime）。
	_, err = mgr.Read(e.ctx, &meta.ReadRequest{Ino: regIno, Size: 7, SessionID: &srcSID})
	require.NoError(t, err)
	require.NoError(t, mgr.CloseSession(e.ctx, srcSID))

	_, err = mgr.Readlink(e.ctx, &sid, symIno)
	require.NoError(t, err)
	require.NoError(t, mgr.CloseSession(e.ctx, sid))

	mergeRes, err := mgr.Merge(e.ctx, meta.MergeParams{
		DiskID: e.disk.DiskID, SourceBranch: "src-branch", TargetBranch: "main", Strategy: meta.ConflictStrategyAbortOnConflict,
	})
	require.NoError(t, err)
	require.Equal(t, meta.MergeResultMerged, mergeRes.Result)
	require.NotNil(t, mergeRes.MergeCommitID)
	assertSnapshotInodeAtimeAfter(t, e.storage, e.disk.DiskID, *mergeRes.MergeCommitID, regIno, t4)
	assertSnapshotInodeAtimeAfter(t, e.storage, e.disk.DiskID, *mergeRes.MergeCommitID, symIno, t4)
}

// TestPebbleAtimeTrackerPreservesEntriesOnFlushFailure 验证：
// 1. 当底层 Pebble 批次提交失败时，内存中的脏 atime 条目不会丢失，后续重试仍能正确刷盘；
// 2. 节点被物理删除（Unlink / Reclaim）或分支/磁盘被删除时，自动清理对应内存脏 atime 条目。
func TestPebbleAtimeTrackerPreservesEntriesOnFlushFailure(t *testing.T) {
	root := t.TempDir()
	storage := newTestStorage(t, root)
	locks := meta.NewBranchLockTable()
	tracker := meta.NewPebbleAtimeTracker(meta.AtimeModeStrictAtime, storage, locks, root)

	branch, err := storage.OpenBranch("disk-1", "main")
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()

	old := time.Unix(50, 0).UTC()
	inode := pebblekv.Inode{
		Ino: 10, Mode: meta.S_IFREG | 0o644, Nlink: 1, Size: 8,
		AtimeSec: old.Unix(), MtimeSec: old.Unix(), CtimeSec: old.Unix(),
	}
	encoded, err := pebblekv.EncodeInode(inode)
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set(pebblekv.InodeKey(10), encoded, pebble.Sync))

	now := time.Unix(500, 123000000).UTC()
	tracker.MarkDirty("disk-1", "main", inode, now)
	require.True(t, tracker.HasPending("disk-1", "main"))

	// 叠加校验：刷盘前 Overlay 立即反映最新内存脏 atime。
	overlayInode := inode
	tracker.Overlay("disk-1", "main", &overlayInode)
	require.Equal(t, now.Unix(), overlayInode.AtimeSec)
	require.Equal(t, uint32(now.Nanosecond()), overlayInode.AtimeNsec)

	// 构造只读 Pebble DB 模拟底层批次提交（batch.Commit）失败。
	failDir := t.TempDir()
	rwDB, err := pebble.Open(failDir, &pebble.Options{})
	require.NoError(t, err)
	require.NoError(t, rwDB.Set(pebblekv.InodeKey(10), encoded, pebble.Sync))
	require.NoError(t, rwDB.Close())
	roDB, err := pebble.Open(failDir, &pebble.Options{ReadOnly: true})
	require.NoError(t, err)
	defer func() { require.NoError(t, roDB.Close()) }()

	// 1. 单节点刷盘失败：条目必须回填保留。
	err = tracker.FlushInodeLocked("disk-1", "main", roDB, 10)
	require.Error(t, err)
	require.True(t, tracker.HasPending("disk-1", "main"), "单节点刷盘失败后内存脏 atime 条目不得丢失")

	// 2. 分支批量刷盘失败：条目必须回填保留。
	err = tracker.FlushBranchLocked("disk-1", "main", roDB)
	require.Error(t, err)
	require.True(t, tracker.HasPending("disk-1", "main"), "分支批量刷盘失败后内存脏 atime 条目不得丢失")

	// 3. 后续在正常的分支 Pebble 上重试刷盘：必须 100% 成功落盘并清空内存挂起项。
	require.NoError(t, tracker.FlushBranchLocked("disk-1", "main", branch.DB()))
	require.False(t, tracker.HasPending("disk-1", "main"))
	flushed := readPebbleCreateInode(t, branch.DB(), 10)
	require.Equal(t, now.Unix(), flushed.AtimeSec)
	require.Equal(t, uint32(now.Nanosecond()), flushed.AtimeNsec)
	require.Equal(t, []byte{1}, getPebbleValue(t, branch.DB(), pebblekv.DirtyKey(10)))

	// 4. 删除清理：DiscardInode / DiscardBranch / DiscardDisk 精准移除内存脏项。
	tLater := time.Unix(900, 0).UTC()
	tracker.MarkDirty("disk-1", "main", flushed, tLater)
	tracker.MarkDirty("disk-1", "feature", flushed, tLater)
	tracker.MarkDirty("disk-2", "main", flushed, tLater)
	require.True(t, tracker.HasPending("disk-1", "main"))
	require.True(t, tracker.HasPending("disk-1", "feature"))
	require.True(t, tracker.HasPending("disk-2", "main"))

	tracker.DiscardInode("disk-1", "main", 10)
	require.False(t, tracker.HasPending("disk-1", "main"), "DiscardInode 后该节点的脏 atime 必须被清理")
	require.True(t, tracker.HasPending("disk-1", "feature"))

	tracker.DiscardBranch("disk-1", "feature")
	require.False(t, tracker.HasPending("disk-1", "feature"), "DiscardBranch 后该分支的脏 atime 必须被清理")
	require.True(t, tracker.HasPending("disk-2", "main"))

	tracker.DiscardDisk("disk-2")
	require.False(t, tracker.HasPending("disk-2", "main"), "DiscardDisk 后该磁盘的脏 atime 必须被清理")

	// 5. Manager 端到端删除联动：Unlink 文件后再次创建快照，不应因已删除节点的残留脏 atime 报错。
	e := setupPebbleCreate(t, 1<<20)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))
	mgr := newStrictAtimeManager(t, e)
	sid := uint64(809)
	_, err = mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
	require.NoError(t, err)
	tempFile, err := mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("temp.txt"), Mode: 0o644, SessionID: &sid})
	require.NoError(t, err)
	_, err = mgr.Write(e.ctx, &meta.WriteRequest{Ino: tempFile.Inode.Ino, Data: []byte("x"), SessionID: &sid})
	require.NoError(t, err)
	oldTime := time.Unix(10, 0).UTC()
	_, err = mgr.SetAttr(e.ctx, tempFile.Inode.Ino, meta.SetAttrInput{Atime: &oldTime, SessionID: &sid})
	require.NoError(t, err)
	_, err = mgr.Read(e.ctx, &meta.ReadRequest{Ino: tempFile.Inode.Ino, Size: 1, SessionID: &sid})
	require.NoError(t, err)
	require.NoError(t, mgr.Unlink(e.ctx, &sid, 1, []byte("temp.txt"), false))
	_, err = mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
}
