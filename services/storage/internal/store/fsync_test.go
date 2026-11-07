package store_test

import (
	"bytes"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestFsyncAndFsyncDirValidationAndOrphanLiveness(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-fsync-val",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)

	// 1. 非法 Inode 编号校验
	require.ErrorIs(t, actor.Fsync(0), store.ErrInvalid)
	require.ErrorIs(t, actor.Fsync(uint64(math.MaxInt64)+1), store.ErrInvalid)
	require.ErrorIs(t, actor.FsyncDir(0), store.ErrInvalid)
	require.ErrorIs(t, actor.FsyncDir(uint64(math.MaxInt64)+1), store.ErrInvalid)

	// 2. 不存在的节点返回 ErrNotFound
	require.ErrorIs(t, actor.Fsync(99999), store.ErrNotFound)
	require.ErrorIs(t, actor.FsyncDir(99999), store.ErrNotFound)

	// 3. 创建目录、常规文件与符号链接
	subDir, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("sub"),
		Mode:      store.S_IFDIR | 0o755,
	})
	require.NoError(t, err)

	regFile, err := actor.Create(store.CreateReq{
		ParentIno: subDir.Ino,
		Name:      []byte("file.txt"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	sym, err := actor.Symlink(store.SymlinkReq{
		ParentIno: subDir.Ino,
		Name:      []byte("link.txt"),
		Target:    []byte("file.txt"),
	})
	require.NoError(t, err)

	// 4. 类型匹配校验：
	// - Fsync 对目录返回 ErrIsDir，对常规文件和符号链接成功
	// - FsyncDir 对目录成功，对常规文件和符号链接返回 ErrNotDir
	require.ErrorIs(t, actor.Fsync(store.RootIno), store.ErrIsDir)
	require.ErrorIs(t, actor.Fsync(subDir.Ino), store.ErrIsDir)
	require.NoError(t, actor.Fsync(regFile.Ino))
	require.NoError(t, actor.Fsync(sym.Ino))

	require.NoError(t, actor.FsyncDir(store.RootIno))
	require.NoError(t, actor.FsyncDir(subDir.Ino))
	require.ErrorIs(t, actor.FsyncDir(regFile.Ino), store.ErrNotDir)
	require.ErrorIs(t, actor.FsyncDir(sym.Ino), store.ErrNotDir)

	// 5. 孤儿节点（Nlink == 0 但 openHandle == true）仍存活可执行 Fsync，Purge 后返回 ErrNotFound
	require.NoError(t, actor.Unlink(subDir.Ino, []byte("file.txt"), true))
	require.NoError(t, actor.Fsync(regFile.Ino))
	require.NoError(t, actor.PurgeOrphanInode(regFile.Ino))
	require.ErrorIs(t, actor.Fsync(regFile.Ino), store.ErrNotFound)

	// 6. 无句柄直接 Unlink 后返回 ErrNotFound
	require.NoError(t, actor.Unlink(subDir.Ino, []byte("link.txt"), false))
	require.ErrorIs(t, actor.Fsync(sym.Ino), store.ErrNotFound)

	require.NoError(t, actor.Close())

	// 7. 已关闭实例返回 ErrClosed
	require.ErrorIs(t, actor.Fsync(store.RootIno), store.ErrClosed)
	require.ErrorIs(t, actor.FsyncDir(store.RootIno), store.ErrClosed)

	// 8. 只读实例返回 ErrReadOnly
	roActor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-fsync-val",
		BranchID: "main",
		ReadOnly: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roActor.Close()) }()

	require.ErrorIs(t, roActor.Fsync(subDir.Ino), store.ErrReadOnly)
	require.ErrorIs(t, roActor.FsyncDir(subDir.Ino), store.ErrReadOnly)
}

func TestFsyncFlushesPendingDirtyAtimeAndClearsWALDirty(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:       dir,
		DiskID:    "disk-fsync-atime",
		BranchID:  "main",
		AtimeMode: store.AtimeModeStrictAtime,
		MaxBytes:  64 << 20,
	})
	require.NoError(t, err)

	reg, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("atime.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(reg.Ino, 0, []byte("hello-fsync"))
	require.NoError(t, err)

	// 1. 普通写入采用 pebble.NoSync，此时 walDirty 应为 true；调用 FsyncDir(RootIno) 在 s.batch.Count()==0 时通过 LogData(nil, pebble.Sync) 清零 walDirty
	require.True(t, actor.WALDirty())
	require.NoError(t, actor.FsyncDir(store.RootIno))
	require.False(t, actor.WALDirty())

	// 2. 登记内存 dirtyAtime（尚未刷入 Pebble），此时单独调用 Fsync(reg.Ino)：
	// Step 3 必须先将挂起的内存 dirtyAtime 编码进 s.batch（使 s.batch.Count() > 0），
	// 再通过 ApplyNoSyncWait(s.batch, pebble.Sync) + SyncInode + SyncWait 持久化到 Pebble。
	futureAtime := time.Unix(0, reg.Atime).Add(2 * time.Hour)
	actor.MarkAtimeDirty(reg.Ino, futureAtime)

	require.NoError(t, actor.Fsync(reg.Ino))
	require.False(t, actor.WALDirty())

	// 使用 CloseDiscard 关闭（跳过关闭时的 flushOnClose），再以只读模式重开验证 Fsync 已经把 dirtyAtime 真正落盘到 Pebble
	require.NoError(t, actor.CloseDiscard())

	roActor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-fsync-atime",
		BranchID: "main",
		ReadOnly: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roActor.Close()) }()

	roAttr, err := roActor.GetAttr(reg.Ino)
	require.NoError(t, err)
	require.Equal(t, futureAtime.UnixNano(), roAttr.Atime, "Fsync 必须在 Step 3 将挂起的 dirtyAtime 编码进 s.batch 并持久化落盘")
}

func TestFsyncPerInodeIsolationDirectorySyncAndWindowSolidification(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-fsync-iso",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	blobs := actor.Blobs()
	require.NotNil(t, blobs)

	fileA, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("a.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	fileB, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("b.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// fileA 写入 2 个块（blobID 1, 2），fileB 写入 2 个块（blobID 3, 4）
	_, _, err = actor.Write(fileA.Ino, 0, bytes.Repeat([]byte{0x11}, 4096))
	require.NoError(t, err)
	_, _, err = actor.Write(fileA.Ino, store.BlobSize, bytes.Repeat([]byte{0x22}, 4096))
	require.NoError(t, err)
	_, _, err = actor.Write(fileB.Ino, 0, bytes.Repeat([]byte{0x33}, 4096))
	require.NoError(t, err)
	_, _, err = actor.Write(fileB.Ino, store.BlobSize, bytes.Repeat([]byte{0x44}, 4096))
	require.NoError(t, err)

	for id := uint64(1); id <= 4; id++ {
		require.True(t, blobs.WindowLocal(id), "blob %d 初始应处于 WindowLocal 窗口内", id)
	}

	// 1. FsyncDir 仅同步 Pebble WAL，不触发任何 blobs.SyncInode
	require.NoError(t, actor.FsyncDir(store.RootIno))
	require.False(t, actor.WALDirty())
	require.Equal(t, uint64(0), blobs.SyncFileCount(), "FsyncDir 不得触发 .blob 文件刷盘")
	require.Equal(t, uint64(0), blobs.SyncDirCount(), "FsyncDir 不得触发 blobs 分片目录刷盘")
	for id := uint64(1); id <= 4; id++ {
		require.True(t, blobs.WindowLocal(id))
	}

	// 2. 在 Fsync(fileA) 前对 fileA 的块 0 (blobID=1) 连续执行局部写（走 PatchLog 原地合并）：
	// 不分配新 BlobID、不产生新目录项
	_, _, err = actor.Write(fileA.Ino, 4096, bytes.Repeat([]byte{0x12}, 4096))
	require.NoError(t, err)
	require.True(t, blobs.Exists(1))
	require.False(t, blobs.Exists(5))

	// 3. 调用 Fsync(fileA.Ino)：
	//    - 只刷盘归属于 fileA 的脏块 (blobID 1, 2)，绝不将 fileB 的脏块 (blobID 3, 4) 拖入屏障；
	//    - 对新建块 1, 2 所在的 2 个分片目录执行 fsync；
	//    - 块 1, 2 的 WindowLocal 翻转为 false，块 3, 4 保持 WindowLocal == true。
	require.NoError(t, actor.Fsync(fileA.Ino))
	require.Equal(t, uint64(2), blobs.SyncFileCount(), "Fsync(fileA) 只能刷 fileA 的 2 个脏块")
	require.Equal(t, uint64(2), blobs.SyncDirCount(), "Fsync(fileA) 只应刷块 1, 2 对应的 2 个新建分片目录")
	require.False(t, blobs.WindowLocal(1))
	require.False(t, blobs.WindowLocal(2))
	require.True(t, blobs.WindowLocal(3), "fileB 的脏块 3 不得被 Fsync(fileA) 固化")
	require.True(t, blobs.WindowLocal(4), "fileB 的脏块 4 不得被 Fsync(fileA) 固化")

	// 4. 验证纯 PatchLog 原地修改（无 dirtyNew、无 dirtyDir）时 SyncInode 0 目录 fsync：
	prevFileSyncs := blobs.SyncFileCount()
	prevDirSyncs := blobs.SyncDirCount()
	require.NoError(t, blobs.ApplyPatches([]store.PatchLog{
		{
			BlobID: 2,
			Ino:    fileA.Ino,
			Offset: 0,
			Data:   bytes.Repeat([]byte{0x29}, 1024),
			NewLen: 4096,
		},
	}))
	require.True(t, blobs.WindowLocal(2))
	require.NoError(t, actor.Fsync(fileA.Ino))
	require.Equal(t, prevFileSyncs+1, blobs.SyncFileCount(), "原地 PatchLog 后 Fsync 应刷盘目标块文件")
	require.Equal(t, prevDirSyncs, blobs.SyncDirCount(), "原地 PatchLog 不产生目录项变更，必须 0 目录 fsync")
	require.False(t, blobs.WindowLocal(2))

	// 5. 窗口固化后局部写自动切换为标准 CoW：
	//    - 对 fileA 已固化块 1 局部写 -> 分配新 blobID=5 并将旧固化块 1 删除（登记 shardDir(1) 至 dirtyDir）；
	//    - 对 fileB 仍处于 WindowLocal 的块 3 局部写 -> 仍走 PatchLog 原地合并（不分配新 BlobID）。
	_, _, err = actor.Write(fileA.Ino, 0, bytes.Repeat([]byte{0x99}, 2048))
	require.NoError(t, err)
	require.False(t, blobs.Exists(1), "旧块 1 在 CoW 后应被删除")
	require.True(t, blobs.Exists(5), "fileA 已固化块局部覆写必须自动转 CoW 分配新块 5")
	require.True(t, blobs.WindowLocal(5))

	_, _, err = actor.Write(fileB.Ino, 4096, bytes.Repeat([]byte{0x88}, 2048))
	require.NoError(t, err)
	require.True(t, blobs.Exists(3), "fileB 未固化块 3 局部写仍应原地合并")
	require.False(t, blobs.Exists(6), "fileB 未固化块局部写不得分配新块")

	// 6. 再次 Fsync(fileA.Ino)：
	//    - 刷盘新块 5（1 次文件 fdatasync）；
	//    - 刷盘新块 5 的分片目录（dirtyNew）与已删除旧固化块 1 的分片目录（dirtyDir），共 2 次目录 fsync。
	prevFileSyncs = blobs.SyncFileCount()
	prevDirSyncs = blobs.SyncDirCount()
	require.NoError(t, actor.Fsync(fileA.Ino))
	require.Equal(t, prevFileSyncs+1, blobs.SyncFileCount())
	require.Equal(t, prevDirSyncs+2, blobs.SyncDirCount())
	require.False(t, blobs.WindowLocal(5))
}

func TestGroupCommitFsyncAndFsyncDirDualLayerBarrier(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-fsync-batch",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	fileA, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("batch-a.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	fileB, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("batch-b.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 先占住当前 Tick，使后续 Write(fileA)、Write(fileB)、Fsync(fileA)、FsyncDir(RootIno) 在同一轮 Tick (s.batch.Count() > 0) 中通过 ApplyNoSyncWait + SyncInode + SyncWait 合并提交
	blockCh := make(chan struct{})
	enteredCh := make(chan struct{})
	holdDone := make(chan error, 1)
	go func() {
		holdDone <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				close(enteredCh)
				<-blockCh
				return nil
			},
			Mutate: func(m store.MutateView) {},
		})
	}()
	<-enteredCh

	errCh := make(chan error, 4)
	go func() {
		_, _, wErr := actor.Write(fileA.Ino, 0, bytes.Repeat([]byte{0xa1}, 8192))
		errCh <- wErr
	}()
	go func() {
		_, _, wErr := actor.Write(fileB.Ino, 0, bytes.Repeat([]byte{0xb2}, 8192))
		errCh <- wErr
	}()
	time.Sleep(10 * time.Millisecond)
	go func() {
		errCh <- actor.Fsync(fileA.Ino)
	}()
	go func() {
		errCh <- actor.FsyncDir(store.RootIno)
	}()
	time.Sleep(10 * time.Millisecond)

	ticksBeforeRelease := actor.TickCount()
	close(blockCh)
	require.NoError(t, <-holdDone)
	for range 4 {
		require.NoError(t, <-errCh)
	}

	// 验证 4 条命令在阻塞释放后的单轮 Tick 中合并完成，且 WALDirty 清零、仅 fileA 的块被 SyncInode 固化
	require.LessOrEqual(t, actor.TickCount()-ticksBeforeRelease, uint64(2))
	require.False(t, actor.WALDirty())
	require.Equal(t, uint64(1), actor.Blobs().SyncFileCount(), "同批次中仅 Fsync(fileA) 关联的 1 个脏块应被 SyncInode 刷盘")
	require.Equal(t, uint64(1), actor.Blobs().SyncDirCount())

	valA, closerA, err := actor.DB().Get(store.DataKey(fileA.Ino, 0))
	require.NoError(t, err)
	refA, err := store.DecodeBlobRef(valA)
	require.NoError(t, closerA.Close())
	require.NoError(t, err)
	require.False(t, actor.Blobs().WindowLocal(refA.ID), "fileA 的块在同批 Fsync 后必须固化 (WindowLocal=false)")

	valB, closerB, err := actor.DB().Get(store.DataKey(fileB.Ino, 0))
	require.NoError(t, err)
	refB, err := store.DecodeBlobRef(valB)
	require.NoError(t, closerB.Close())
	require.NoError(t, err)
	require.True(t, actor.Blobs().WindowLocal(refB.ID), "同批次未调用 Fsync 的 fileB 块必须保持 WindowLocal=true")

	gotA, err := actor.Read(fileA.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xa1}, 8192), gotA)

	gotB, err := actor.Read(fileB.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xb2}, 8192), gotB)
}
