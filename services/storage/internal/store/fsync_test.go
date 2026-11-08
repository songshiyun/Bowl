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

	require.True(t, blobs.HasDirty())

	// 1. FsyncDir 仅同步 Pebble WAL，不触发任何 blobs.SyncInode
	require.NoError(t, actor.FsyncDir(store.RootIno))
	require.False(t, actor.WALDirty())
	require.Equal(t, uint64(0), blobs.SyncFileCount(), "FsyncDir 不得触发 .blob 文件刷盘")
	require.Equal(t, uint64(0), blobs.SyncDirCount(), "FsyncDir 不得触发 blobs 分片目录刷盘")
	require.True(t, blobs.HasDirty())

	// 2. 在 Fsync(fileA) 前对 fileA 的块 0 连续执行局部写
	_, _, err = actor.Write(fileA.Ino, 4096, bytes.Repeat([]byte{0x12}, 4096))
	require.NoError(t, err)
	require.True(t, blobs.Exists(1))
	require.False(t, blobs.Exists(5))

	// 3. 调用 Fsync(fileA.Ino)
	require.NoError(t, actor.Fsync(fileA.Ino))
	require.GreaterOrEqual(t, blobs.SyncFileCount(), uint64(1))
	require.GreaterOrEqual(t, blobs.SyncDirCount(), uint64(1))

	// 4. 局部覆写后再 Fsync(fileA.Ino)
	_, _, err = actor.Write(fileA.Ino, 0, bytes.Repeat([]byte{0x99}, 2048))
	require.NoError(t, err)
	_, _, err = actor.Write(fileB.Ino, 4096, bytes.Repeat([]byte{0x88}, 2048))
	require.NoError(t, err)

	prevFileSyncs := blobs.SyncFileCount()
	prevDirSyncs := blobs.SyncDirCount()
	require.NoError(t, actor.Fsync(fileA.Ino))
	require.GreaterOrEqual(t, blobs.SyncFileCount(), prevFileSyncs+1)
	require.Equal(t, prevDirSyncs, blobs.SyncDirCount(), "已有活跃 .pack 后续追加落盘无需重复执行 packs/ 目录 fsync")
}

func TestGroupCommitFsyncAndFsyncDirDualLayerBarrier(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-fsync-batch",
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
	release := store.HoldTickForTest(actor)

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
	release()
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
	require.NotZero(t, refA.PackID)

	valB, closerB, err := actor.DB().Get(store.DataKey(fileB.Ino, 0))
	require.NoError(t, err)
	refB, err := store.DecodeBlobRef(valB)
	require.NoError(t, closerB.Close())
	require.NoError(t, err)
	require.NotZero(t, refB.PackID)

	gotA, err := actor.Read(fileA.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xa1}, 8192), gotA)

	gotB, err := actor.Read(fileB.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xb2}, 8192), gotB)
}
