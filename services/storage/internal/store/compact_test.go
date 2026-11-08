package store_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestCompactPacksLowWatermarkRelocationAndDualPersistenceBarrier(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-basic",
		BranchID: "main",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 单个 .pack 容量上限设为 2 个 256 KiB 块（512 KiB）
	actor.Blobs().SetMaxPackSize(2 * store.BlobSize)

	f1, err := actor.Create(store.CreateReq{ParentIno: store.RootIno, Name: []byte("f1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	f2, err := actor.Create(store.CreateReq{ParentIno: store.RootIno, Name: []byte("f2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	f3, err := actor.Create(store.CreateReq{ParentIno: store.RootIno, Name: []byte("f3.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)

	// 1. Pack 1 (已封口, 50% 存活率 -> 命中 LiveBytes * 2 <= TotalBytes)：
	//    写入 f1 block 0 (256 KiB) + f2 block 0 (256 KiB) 填满 Pack 1
	f1Block0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	f2Block0 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	_, _, err = actor.Write(f1.Ino, 0, f1Block0)
	require.NoError(t, err)
	_, _, err = actor.Write(f2.Ino, 0, f2Block0)
	require.NoError(t, err)
	require.Equal(t, uint64(1), actor.Blobs().ActivePackID())

	// 2. Pack 2 (已封口, 75% 存活率 -> 高于水位线，不应被搬迁)：
	//    写入 f3 block 0 (256 KiB) + f3 block 1 (128 KiB) + f2 block 1 (128 KiB) 填满 Pack 2
	f3Block0 := bytes.Repeat([]byte{0x33}, store.BlobSize)
	f3Block1 := bytes.Repeat([]byte{0x44}, store.BlobSize/2)
	f2Block1 := bytes.Repeat([]byte{0x55}, store.BlobSize/2)
	_, _, err = actor.Write(f3.Ino, 0, f3Block0)
	require.NoError(t, err)
	_, _, err = actor.Write(f3.Ino, store.BlobSize, f3Block1)
	require.NoError(t, err)
	_, _, err = actor.Write(f2.Ino, store.BlobSize, f2Block1)
	require.NoError(t, err)
	require.Equal(t, uint64(2), actor.Blobs().ActivePackID())

	// 3. 封口 Pack 2，随后 Unlink(f2.bin)：
	//    - Pack 1: LiveBytes = 256 KiB, TotalBytes = 512 KiB (50% <= 50%, 命中搬迁)
	//    - Pack 2: LiveBytes = 384 KiB, TotalBytes = 512 KiB (75% > 50%, 不搬迁)
	require.NoError(t, actor.Blobs().SealActivePack())
	require.NoError(t, actor.Unlink(store.RootIno, []byte("f2.bin"), false))

	meta1Before, ok := readPackMetaFromDB(t, actor, 1)
	require.True(t, ok)
	require.Equal(t, store.PackMeta{LiveBytes: store.BlobSize, TotalBytes: 2 * store.BlobSize}, meta1Before)

	meta2Before, ok := readPackMetaFromDB(t, actor, 2)
	require.True(t, ok)
	require.Equal(t, store.PackMeta{LiveBytes: store.BlobSize + store.BlobSize/2, TotalBytes: 2 * store.BlobSize}, meta2Before)

	syncFilesBefore := actor.Blobs().SyncFileCount()

	// 4. 执行 CompactPacks：
	//    - 仅 Pack 1 被筛选搬迁至新活跃包 Pack 3
	//    - 强制完成新包 fdatasync 与 Pebble WAL Sync 后物理删除 0000000000000001.pack
	require.NoError(t, actor.CompactPacks())

	require.False(t, actor.Blobs().Exists(1), "低水位源封口包 Pack 1 必须在搬迁与双重持久化屏障后被物理删除")
	_, ok = readPackMetaFromDB(t, actor, 1)
	require.False(t, ok, "源封口包 Pack 1 的 sys:pack:1 必须已从 Pebble 删除")

	require.True(t, actor.Blobs().Exists(2), "高水位封口包 Pack 2 (75% > 50%) 不应被搬迁删除")
	meta2After, ok := readPackMetaFromDB(t, actor, 2)
	require.True(t, ok)
	require.Equal(t, meta2Before, meta2After)

	require.True(t, actor.Blobs().Exists(3), "搬迁目标新包 Pack 3 必须存在")
	meta3After, ok := readPackMetaFromDB(t, actor, 3)
	require.True(t, ok)
	require.Equal(t, store.PackMeta{LiveBytes: store.BlobSize, TotalBytes: store.BlobSize}, meta3After)

	// 验证双重持久化屏障已执行：目标新包已完成 fdatasync 且 Pebble WAL 已 Sync
	require.Greater(t, actor.Blobs().SyncFileCount(), syncFilesBefore, "删除旧包前必须先对目标新 .pack 执行 fdatasync")
	require.False(t, actor.WALDirty(), "CompactPacks 完成后 Pebble WAL 不得残留未 Sync 状态")

	// 验证搬迁前后全文件读取字节 100% 一致（穿透热缓存直读磁盘 .pack）
	actor.Blobs().EvictHot(store.BlobRef{PackID: 3, Offset: 0, Length: store.BlobSize})
	gotF1, err := actor.Read(f1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, f1Block0, gotF1)

	gotF3, err := actor.Read(f3.Ino, 0, store.BlobSize+store.BlobSize/2)
	require.NoError(t, err)
	require.Equal(t, append(append([]byte(nil), f3Block0...), f3Block1...), gotF3)
}

func TestCompactPacksPhaseACASFenceAgainstConcurrentOverwriteTruncateAndUnlink(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		BranchID: "main",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 在 Pack 1 中写入 5 个文件：
	// - fDead: 256 KiB (将在封口后先删除，使 Pack 1 存活率降至 <= 50% 触发搬迁)
	// - fKeep: 32 KiB (搬迁期间无并发改动，应成功搬迁至新包)
	// - fOverwrite: 32 KiB (搬迁读取后、提交 opCompactRelocate 前被前台 Write 覆写为新内容，CAS 必须拒绝旧数据回滚)
	// - fTruncate: 32 KiB (搬迁读取后、提交 opCompactRelocate 前被前台 SetAttr 截断为 0，CAS 必须拒绝旧块恢复)
	// - fUnlink: 32 KiB (搬迁读取后、提交 opCompactRelocate 前被前台 Unlink 删除，CAS 必须拒绝死文件复活)
	createAndWrite := func(name string, fill byte, size int) store.Attribute {
		attr, cErr := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte(name),
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, cErr)
		_, _, cErr = actor.Write(attr.Ino, 0, bytes.Repeat([]byte{fill}, size))
		require.NoError(t, cErr)
		return attr
	}

	_ = createAndWrite("dead.bin", 0xAA, store.BlobSize)
	fKeep := createAndWrite("keep.bin", 0xBB, 32768)
	fOverwrite := createAndWrite("overwrite.bin", 0xCC, 32768)
	fTruncate := createAndWrite("truncate.bin", 0xDD, 32768)
	fUnlink := createAndWrite("unlink.bin", 0xEE, 32768)

	require.Equal(t, uint64(1), actor.Blobs().ActivePackID())
	require.NoError(t, actor.Blobs().SealActivePack())

	// 删除 dead.bin，使 Pack 1 的 LiveBytes = 4 * 32 KiB = 128 KiB, TotalBytes = 384 KiB (LiveBytes * 2 <= TotalBytes)
	require.NoError(t, actor.Unlink(store.RootIno, []byte("dead.bin"), false))

	newOverwritePayload := bytes.Repeat([]byte{0xFE}, 32768)
	var hookTriggered bool
	actor.SetBeforeCompactRelocateHookForTest(func() {
		hookTriggered = true
		// 1. 前台并发覆写 fOverwrite
		_, _, hErr := actor.Write(fOverwrite.Ino, 0, newOverwritePayload)
		require.NoError(t, hErr)

		// 2. 前台并发截断 fTruncate 至 0
		zero := uint64(0)
		_, hErr = actor.SetAttr(fTruncate.Ino, store.SetAttrReq{Size: &zero})
		require.NoError(t, hErr)

		// 3. 前台并发删除 fUnlink
		hErr = actor.Unlink(store.RootIno, []byte("unlink.bin"), false)
		require.NoError(t, hErr)
	})
	defer actor.SetBeforeCompactRelocateHookForTest(nil)

	require.NoError(t, actor.CompactPacks())
	require.True(t, hookTriggered, "必须在搬迁读取追加后、提交 opCompactRelocate 前触发并发写钩子")

	// 1. 源封口包 Pack 1 的所有块（1 个成功搬迁 + 3 个被前台覆写/截断/删除）均已不再引用 Pack 1，Pack 1 必须被彻底回收
	require.False(t, actor.Blobs().Exists(1), "Pack 1 在所有活块完成搬迁或前台更新后必须被物理回收")
	_, ok := readPackMetaFromDB(t, actor, 1)
	require.False(t, ok, "sys:pack:1 必须已从 Pebble 删除")

	// 2. fKeep 数据完好搬迁至新包
	gotKeep, err := actor.Read(fKeep.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xBB}, 32768), gotKeep)

	// 3. fOverwrite 必须保持前台新写入的 0xFE，绝不能被后台 GC 旧数据 0xCC 覆盖回滚（防 ShardStore Bug #1）
	gotOverwrite, err := actor.Read(fOverwrite.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, newOverwritePayload, gotOverwrite)

	// 4. fTruncate 必须保持 Size=0 且无残留 DataKey
	truncAttr, err := actor.GetAttr(fTruncate.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(0), truncAttr.Size)
	gotTrunc, err := actor.Read(fTruncate.Ino, 0, 32768)
	require.NoError(t, err)
	require.Empty(t, gotTrunc)
	_, _, err = actor.DB().Get(store.DataKey(fTruncate.Ino, 0))
	require.Error(t, err, "已被前台截断的块不得被后台 GC 重新写入 DataKey")

	// 5. fUnlink 必须保持已删除状态，Pebble 中不得复活 DataKey
	_, err = actor.GetAttr(fUnlink.Ino)
	require.ErrorIs(t, err, store.ErrNotFound)
	_, _, err = actor.DB().Get(store.DataKey(fUnlink.Ino, 0))
	require.Error(t, err, "已被前台删除的文件不得被后台 GC 复活 DataKey")

	// 6. 校验新包 Pack 2 的 sys:pack:2 记账：
	//    预写了 4 个 32 KiB 搬迁块（其中 3 个 CAS 跳过不计入 LiveBytes）+ 1 个前台覆写 32 KiB 块
	//    因此 LiveBytes 必须严格等于 fKeep(32KiB) + fOverwrite(32KiB) = 65536，TotalBytes = 5 * 32KiB = 163840
	meta2, ok := readPackMetaFromDB(t, actor, 2)
	require.True(t, ok)
	require.Equal(t, uint64(65536), meta2.LiveBytes, "CAS 跳过的预写块绝不能计入 LiveBytes")
	require.Equal(t, uint64(5*32768), meta2.TotalBytes, "TotalBytes 必须包含预写塊与前台新写块推进的物理尾部偏移")
	require.Equal(t, sumActualLiveBytesByPack(t, actor)[2], meta2.LiveBytes)

	// 7. 全量磁盘 .pack 与 Pebble DataKey/sys:pack 一致性审计
	auditDiskBlobsMatchPebbleDataKeys(t, actor)
}

func TestCompactPacksEdgeCasesReadOnlyAndClosed(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-edge",
		BranchID: "main",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)

	// 无候选低水位包时 CompactPacks 安全返回 nil 并同步 WAL
	require.NoError(t, actor.CompactPacks())
	require.NoError(t, actor.Close())

	// 关闭后调用返回 ErrClosed
	require.ErrorIs(t, actor.CompactPacks(), store.ErrClosed)

	// 只读实例调用返回 ErrReadOnly
	roActor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-edge",
		BranchID: "main",
		ReadOnly: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roActor.Close()) }()
	require.ErrorIs(t, roActor.CompactPacks(), store.ErrReadOnly)
}
