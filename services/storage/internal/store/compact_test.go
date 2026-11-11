package store_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requirePackMeta(t *testing.T, actor *store.StoreActor, packID uint64) store.PackMeta {
	t.Helper()
	val, closer, err := actor.DB().Get(store.PackStatKey(packID))
	require.NoError(t, err)
	meta, decErr := store.DecodePackMeta(val)
	require.NoError(t, closer.Close())
	require.NoError(t, decErr)
	return meta
}

func requireNoPackMeta(t *testing.T, actor *store.StoreActor, packID uint64) {
	t.Helper()
	_, _, err := actor.DB().Get(store.PackStatKey(packID))
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func requireDataRef(t *testing.T, actor *store.StoreActor, ino, blockIdx uint64) store.BlobRef {
	t.Helper()
	val, closer, err := actor.DB().Get(store.DataKey(ino, blockIdx))
	require.NoError(t, err)
	ref, decErr := store.DecodeBlobRef(val)
	require.NoError(t, closer.Close())
	require.NoError(t, decErr)
	return ref
}

func requireKeyDeleted(t *testing.T, actor *store.StoreActor, key []byte) {
	t.Helper()
	_, _, err := actor.DB().Get(key)
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func createAndWriteFile(t *testing.T, actor *store.StoreActor, name string, fill byte, size int) store.Attribute {
	t.Helper()
	ctx := context.Background()
	attr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte(name),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, attr.Ino, 0, bytes.Repeat([]byte{fill}, size))
	require.NoError(t, err)
	return attr
}

func TestCompactPacksLowWatermarkRelocationAndDualPersistenceBarrier(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor1, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-basic",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	actor1.Blobs().SetMaxPackSize(2 * store.BlobSize)

	f1, err := actor1.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	f2, err := actor1.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	f3, err := actor1.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f3.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// 1. Pack 1 (填满 512 KiB)：f1 block 0 (256 KiB) + f2 block 0 (256 KiB)
	f1Block0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	f2Block0 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	_, _, err = actor1.Write(ctx, f1.Ino, 0, f1Block0)
	require.NoError(t, err)
	_, _, err = actor1.Write(ctx, f2.Ino, 0, f2Block0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), actor1.Blobs().ActivePackID())

	// 2. Pack 2 (填满 512 KiB)：f3 block 0 (256 KiB) + f3 block 1 (128 KiB) + f2 block 1 (128 KiB)
	f3Block0 := bytes.Repeat([]byte{0x33}, store.BlobSize)
	f3Block1 := bytes.Repeat([]byte{0x44}, store.BlobSize/2)
	f2Block1 := bytes.Repeat([]byte{0x55}, store.BlobSize/2)
	_, _, err = actor1.Write(ctx, f3.Ino, 0, f3Block0)
	require.NoError(t, err)
	_, _, err = actor1.Write(ctx, f3.Ino, store.BlobSize, f3Block1)
	require.NoError(t, err)
	_, _, err = actor1.Write(ctx, f2.Ino, store.BlobSize, f2Block1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), actor1.Blobs().ActivePackID())
	require.NoError(t, actor1.Close())

	// 3. 重新打开使 Pack 1 与 Pack 2 均封口，随后删除 f2.bin：
	//    - Pack 1: LiveBytes = 256 KiB, TotalBytes = 512 KiB (50% <= 50%, 命中搬迁)
	//    - Pack 2: LiveBytes = 384 KiB, TotalBytes = 512 KiB (75% > 50%, 不搬迁)
	actor2, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-basic",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor2.Close()) }()
	actor2.Blobs().SetMaxPackSize(2 * store.BlobSize)
	require.NoError(t, actor2.Unlink(ctx, store.RootIno, []byte("f2.bin"), false))

	meta1Before := requirePackMeta(t, actor2, 1)
	require.Equal(t, store.PackMeta{LiveBytes: store.BlobSize, TotalBytes: 2 * store.BlobSize}, meta1Before)
	meta2Before := requirePackMeta(t, actor2, 2)
	require.Equal(t, store.PackMeta{LiveBytes: store.BlobSize + store.BlobSize/2, TotalBytes: 2 * store.BlobSize}, meta2Before)

	syncFilesBefore := actor2.Blobs().SyncFileCount()

	// 4. 执行 CompactPacks：仅搬迁 Pack 1 至新包 Pack 3，并在新包 fdatasync + Pebble WAL Sync 后删除旧包
	require.NoError(t, actor2.CompactPacks(ctx))

	require.False(t, actor2.Blobs().Exists(1), "低水位源封口包 Pack 1 必须在搬迁与双重持久化屏障后被物理删除")
	requireNoPackMeta(t, actor2, 1)

	require.True(t, actor2.Blobs().Exists(2), "高水位封口包 Pack 2 (75% > 50%) 不应被搬迁删除")
	require.Equal(t, meta2Before, requirePackMeta(t, actor2, 2))

	require.True(t, actor2.Blobs().Exists(3), "搬迁目标新包 Pack 3 必须存在")
	require.Equal(t, store.PackMeta{LiveBytes: store.BlobSize, TotalBytes: store.BlobSize}, requirePackMeta(t, actor2, 3))
	require.Greater(t, actor2.Blobs().SyncFileCount(), syncFilesBefore, "删除旧包前必须先对目标新 .pack 执行 fdatasync")
	require.False(t, actor2.WALDirty(), "CompactPacks 完成后 Pebble WAL 不得残留未 Sync 状态")

	// 穿透热缓存直读磁盘 .pack 验证搬迁前后全文件字节一致
	actor2.Blobs().EvictHot(store.BlobRef{PackID: 3, Offset: 0, Length: store.BlobSize})
	gotF1, err := actor2.Read(ctx, f1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, f1Block0, gotF1)

	gotF3, err := actor2.Read(ctx, f3.Ino, 0, store.BlobSize+store.BlobSize/2)
	require.NoError(t, err)
	require.Equal(t, bytes.Join([][]byte{f3Block0, f3Block1}, nil), gotF3)
}

func TestCompactPacksPhaseACASFenceAgainstConcurrentOverwriteTruncateAndUnlink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor1, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)

	_ = createAndWriteFile(t, actor1, "dead.bin", 0xAA, store.BlobSize)
	fKeep := createAndWriteFile(t, actor1, "keep.bin", 0xBB, 32768)
	fOverwrite := createAndWriteFile(t, actor1, "overwrite.bin", 0xCC, 32768)
	fTruncate := createAndWriteFile(t, actor1, "truncate.bin", 0xDD, 32768)
	fUnlink := createAndWriteFile(t, actor1, "unlink.bin", 0xEE, 32768)
	require.Equal(t, uint64(1), actor1.Blobs().ActivePackID())
	require.NoError(t, actor1.Close())

	actor2, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor2.Close()) }()

	// 删除 dead.bin，使已封口 Pack 1 存活率降至 128 KiB / 384 KiB <= 50%
	require.NoError(t, actor2.Unlink(ctx, store.RootIno, []byte("dead.bin"), false))

	newOverwritePayload := bytes.Repeat([]byte{0xFE}, 32768)
	var hookTriggered bool
	actor2.SetBeforeCompactRelocateHookForTest(func() {
		hookTriggered = true
		_, _, hErr := actor2.Write(ctx, fOverwrite.Ino, 0, newOverwritePayload)
		require.NoError(t, hErr)

		_, hErr = actor2.SetAttr(ctx, fTruncate.Ino, store.SetAttrReq{Size: new(uint64(0))})
		require.NoError(t, hErr)

		hErr = actor2.Unlink(ctx, store.RootIno, []byte("unlink.bin"), false)
		require.NoError(t, hErr)
	})
	defer actor2.SetBeforeCompactRelocateHookForTest(nil)

	require.NoError(t, actor2.CompactPacks(ctx))
	require.True(t, hookTriggered, "必须在搬迁读取追加后、提交 opCompactRelocate 前触发并发写钩子")

	// 1. 源封口包 Pack 1 物理文件与 sys:pack:1 已彻底回收
	require.False(t, actor2.Blobs().Exists(1))
	requireNoPackMeta(t, actor2, 1)

	// 2. fKeep 成功搬迁至 Pack 2；fOverwrite 保持前台新写的 0xFE 而非旧 GC 块 0xCC
	gotKeep, err := actor2.Read(ctx, fKeep.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xBB}, 32768), gotKeep)

	gotOverwrite, err := actor2.Read(ctx, fOverwrite.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, newOverwritePayload, gotOverwrite)

	// 3. fTruncate 与 fUnlink 的 DataKey 不得被后台 GC 恢复或复活
	truncAttr, err := actor2.GetAttr(ctx, fTruncate.Ino)
	require.NoError(t, err)
	require.Zero(t, truncAttr.Size)
	requireKeyDeleted(t, actor2, store.DataKey(fTruncate.Ino, 0))

	_, err = actor2.GetAttr(ctx, fUnlink.Ino)
	require.ErrorIs(t, err, store.ErrNotFound)
	requireKeyDeleted(t, actor2, store.DataKey(fUnlink.Ino, 0))

	// 4. 新包 Pack 2 的 LiveBytes 仅计入 fKeep + fOverwrite (64 KiB)，TotalBytes 包含 4 个预写块 + 1 个前台新写块 (160 KiB)
	refKeep := requireDataRef(t, actor2, fKeep.Ino, 0)
	refOverwrite := requireDataRef(t, actor2, fOverwrite.Ino, 0)
	meta2 := requirePackMeta(t, actor2, 2)
	require.Equal(t, uint64(refKeep.Length+refOverwrite.Length), meta2.LiveBytes)
	require.Equal(t, uint64(65536), meta2.LiveBytes)
	require.Equal(t, uint64(5*32768), meta2.TotalBytes)
}

func TestStageOnePrewriteFailureAndZeroLiveSealedPackCompaction(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor1, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-phase-a-dead-bytes",
		MaxBytes: 64 * 1024,
	})
	require.NoError(t, err)

	// 无低水位候选包时 CompactPacks 安全返回 nil
	require.NoError(t, actor1.CompactPacks(ctx))

	livePayload := bytes.Repeat([]byte{0x42}, 32*1024)
	f1 := createAndWriteFile(t, actor1, "quota.bin", 0x42, 32*1024)

	// Stage 1 预写追加 256 KiB 至 Pack 1，但 Stage 2 Phase A 因超过 64 KiB 配额返回 ErrNoSpace：
	// sys:pack:1 的 LiveBytes 保持 32 KiB，TotalBytes 推进至 288 KiB 记录预写死空间
	_, _, err = actor1.Write(ctx, f1.Ino, store.BlobSize, bytes.Repeat([]byte{0xEE}, store.BlobSize))
	require.ErrorIs(t, err, store.ErrNoSpace)
	require.Equal(t, store.PackMeta{LiveBytes: 32 * 1024, TotalBytes: 32*1024 + store.BlobSize}, requirePackMeta(t, actor1, 1))
	require.NoError(t, actor1.Close())

	// 已关闭实例与只读实例防御校验
	require.ErrorIs(t, actor1.CompactPacks(ctx), store.ErrClosed)
	roActor, err := store.Open(store.Options{Dir: dir, DiskID: "disk-phase-a-dead-bytes", ReadOnly: true})
	require.NoError(t, err)
	require.ErrorIs(t, roActor.CompactPacks(ctx), store.ErrReadOnly)
	require.NoError(t, roActor.Close())

	// 重启后 Pack 1 处于封口态且 LiveBytes(32 KiB)*2 <= TotalBytes(288 KiB)，CompactPacks 自动搬迁至 Pack 2 并回收 Pack 1
	actor2, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-phase-a-dead-bytes",
		MaxBytes: 64 * 1024,
	})
	require.NoError(t, err)
	require.NoError(t, actor2.CompactPacks(ctx))
	require.False(t, actor2.Blobs().Exists(1))
	requireNoPackMeta(t, actor2, 1)

	got, err := actor2.Read(ctx, f1.Ino, 0, 32*1024)
	require.NoError(t, err)
	require.Equal(t, livePayload, got)

	// 删除 quota.bin 使活跃包 Pack 2 的 LiveBytes 归零后重启，验证 0 活字节残留封口包被彻底清理
	require.NoError(t, actor2.Unlink(ctx, store.RootIno, []byte("quota.bin"), false))
	require.NoError(t, actor2.Close())

	actor3, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-phase-a-dead-bytes",
		MaxBytes: 64 * 1024,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor3.Close()) }()
	require.NoError(t, actor3.CompactPacks(ctx))
	requireNoPackMeta(t, actor3, 2)
	require.False(t, actor3.Blobs().Exists(2))
}

func TestDeadSealedPackSoftUpdatesCrashRecoveryAndZeroBlockElision(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor1, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-soft-updates",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	actor1.Blobs().SetMaxPackSize(store.BlobSize)

	// 1. 256 KiB 块网格全零残块省略不变量：
	// 写入 64 KiB 非零块（Pack 1），对前半 32 KiB 打洞后截断至 16 KiB 使残块全零 -> 直接删除 DataKey 且不向活跃包追加全零废块
	fZero := createAndWriteFile(t, actor1, "zero-elide.bin", 0x55, 64*1024)
	require.NoError(t, actor1.Fallocate(ctx, fZero.Ino, 0, 32*1024, store.FallocPunchHole|store.FallocKeepSize))
	offsetBeforeZeroTrunc := actor1.Blobs().ActiveOffset()
	_, err = actor1.SetAttr(ctx, fZero.Ino, store.SetAttrReq{Size: new(uint64(16 * 1024))})
	require.NoError(t, err)
	requireKeyDeleted(t, actor1, store.DataKey(fZero.Ino, 0))
	require.Equal(t, offsetBeforeZeroTrunc, actor1.Blobs().ActiveOffset(), "全零边界残块截断不得向活跃 .pack 追加写入")

	// 活跃包保护：此时 Pack 1 的 LiveBytes == 0，但作为当前活跃包绝不提前删除物理文件
	require.NoError(t, actor1.FlushWAL())
	require.True(t, actor1.Blobs().Exists(1), "当前活跃 .pack 即使 LiveBytes == 0 也不得提前删除")

	// 2. Soft Updates 死包回收屏障顺序：
	// 写入 256 KiB 满块触发滚动至 Pack 2：已封口的全死包 Pack 1 同批删除 sys:pack:1，但 WAL Sync 前物理文件仍保留在磁盘上
	f1 := createAndWriteFile(t, actor1, "f1.bin", 0xA1, store.BlobSize)
	require.Equal(t, uint64(2), actor1.Blobs().ActivePackID())
	requireNoPackMeta(t, actor1, 1)
	require.True(t, actor1.Blobs().Exists(1), "NoSync WAL 落盘前不得提前删除死包物理文件")
	require.NoError(t, actor1.FlushWAL())
	require.False(t, actor1.Blobs().Exists(1), "FlushWAL 落盘确认后死包 000001.pack 必须被物理删除")

	// 3. 模拟崩溃退出后的残留死包与孤儿节点恢复：
	// 写入 fOrphan (Pack 3) 并转为孤儿节点 (openHandle=true)，随后将 f1 (Pack 2) 整块打洞归零，不刷盘直接 CloseDiscard 模拟崩溃
	fOrphan := createAndWriteFile(t, actor1, "orphan.bin", 0xB2, store.BlobSize)
	require.Equal(t, uint64(3), actor1.Blobs().ActivePackID())
	require.NoError(t, actor1.Unlink(ctx, store.RootIno, []byte("orphan.bin"), true))
	require.NoError(t, actor1.Fallocate(ctx, f1.Ino, 0, store.BlobSize, store.FallocPunchHole|store.FallocKeepSize))
	requireNoPackMeta(t, actor1, 2)
	require.True(t, actor1.Blobs().Exists(2), "崩溃前未刷 WAL，死包 000002.pack 仍残留在磁盘上")
	require.NoError(t, actor1.CloseDiscard())

	// 重新 Open：启动期 syncPackStatsWithBlobs 自动清理无存活引用的残留死包 Pack 2，保留孤儿引用的 Pack 3
	actor2, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-soft-updates",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor2.Close()) }()

	require.False(t, actor2.Blobs().Exists(2), "Open 恢复时必须自动物理清理无 sys:pack 存活引用的残留死包")
	require.True(t, actor2.Blobs().Exists(3), "仍被孤儿节点引用的封口包必须保留")

	// 执行 CleanOrphans 批量回收残留孤儿节点：清理 OrphanKey/InodeKey/DataKey，扣减 Pack 3 至 0 并在 FlushWAL 后物理回收 Pack 3
	require.NoError(t, actor2.CleanOrphans(ctx))
	requireKeyDeleted(t, actor2, store.OrphanKey(fOrphan.Ino))
	requireKeyDeleted(t, actor2, store.InodeKey(fOrphan.Ino))
	requireKeyDeleted(t, actor2, store.DataKey(fOrphan.Ino, 0))
	requireNoPackMeta(t, actor2, 3)
	require.NoError(t, actor2.FlushWAL())
	require.False(t, actor2.Blobs().Exists(3))
}
