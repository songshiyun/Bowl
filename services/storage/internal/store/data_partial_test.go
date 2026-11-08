package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func countLiveBlobFiles(t *testing.T, blobs *store.BlobStore) int {
	t.Helper()
	entries, err := os.ReadDir(blobs.Root())
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	var count int
	for _, f := range entries {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".pack" {
			count++
		}
	}
	return count
}

func readDataRefFromDB(t *testing.T, actor *store.StoreActor, ino, blockIdx uint64) (store.BlobRef, bool) {
	t.Helper()
	val, closer, err := actor.DB().Get(store.DataKey(ino, blockIdx))
	if err != nil {
		return store.BlobRef{}, false
	}
	defer func() { _ = closer.Close() }()
	ref, err := store.DecodeBlobRef(val)
	require.NoError(t, err)
	return ref, true
}

func TestPartialWriteTailExtendAppendAndHotRead(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-partial-append",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("append.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 连续 8 次 4 KiB 小块追加写：
	// 第 1 次写出 1.pack 首段 [0, 4 KiB)；
	// 第 2..8 次命中活跃 .pack 最末尾同块，直接走 ExtendTail 原地顺延推进写指针，零死空间。
	const chunkSize = 4096
	const rounds = 8
	expected := make([]byte, 0, chunkSize*rounds)

	for i := range rounds {
		chunk := bytes.Repeat([]byte{byte(0x21 + i)}, chunkSize)
		offset := uint64(i * chunkSize)
		n, attr, wErr := actor.Write(f.Ino, offset, chunk)
		require.NoError(t, wErr)
		require.Equal(t, uint32(chunkSize), n)
		expected = append(expected, chunk...)
		require.Equal(t, uint64(len(expected)), attr.Size)

		// 每次写入后立即无锁 Read（命中 hotShard），必须读到最新完整字节
		got, rErr := actor.Read(f.Ino, 0, uint32(len(expected)))
		require.NoError(t, rErr)
		require.Equal(t, expected, got)
	}

	// 验证磁盘上仅存在 1 个物理 .pack 文件（PackID = 1）且写指针恰好等于 32 KiB
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))
	require.True(t, actor.Blobs().Exists(1))
	require.False(t, actor.Blobs().Exists(2))
	require.Equal(t, uint32(chunkSize*rounds), actor.Blobs().ActiveOffset())

	// 进一步验证块内局部覆写（读取旧块合并后追加写入活跃 .pack 新偏移）与跨越块内空洞写（自动从 oldRef.Length 补零顺延）
	overwrite := bytes.Repeat([]byte{0x99}, 1024)
	n, attr, err := actor.Write(f.Ino, 2048, overwrite)
	require.NoError(t, err)
	require.Equal(t, uint32(1024), n)
	require.Equal(t, uint64(len(expected)), attr.Size)
	copy(expected[2048:2048+1024], overwrite)

	// 跨越旧块尾部 (32 KiB) 到 40 KiB 处写入 2 KiB，中间 [32 KiB, 40 KiB) 自动补零顺延
	holeTail := bytes.Repeat([]byte{0x77}, 2048)
	n, attr, err = actor.Write(f.Ino, 40*1024, holeTail)
	require.NoError(t, err)
	require.Equal(t, uint32(2048), n)
	require.Equal(t, uint64(42*1024), attr.Size)

	expectedWithHole := make([]byte, 42*1024)
	copy(expectedWithHole, expected)
	copy(expectedWithHole[40*1024:], holeTail)

	// 1. 命中 hotShard 读取
	gotHot, err := actor.Read(f.Ino, 0, uint32(len(expectedWithHole)))
	require.NoError(t, err)
	require.Equal(t, expectedWithHole, gotHot)

	// 2. 驱逐 hotShard 后回退磁盘 .pack 文件读取，验证落盘字节完全一致
	curRef, ok := readDataRefFromDB(t, actor, f.Ino, 0)
	require.True(t, ok)
	actor.Blobs().EvictHot(curRef)
	gotDisk, err := actor.Read(f.Ino, 0, uint32(len(expectedWithHole)))
	require.NoError(t, err)
	require.Equal(t, expectedWithHole, gotDisk)
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))
}

func TestPartialWriteSyncedBlockCoWAndPhaseAFailureIsolation(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-partial-cow",
		MaxBytes: 16 * 1024, // 配额上限 16 KiB
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("cow.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. 首次写入 8 KiB
	initial := bytes.Repeat([]byte{0x41}, 8192)
	_, _, err = actor.Write(f.Ino, 0, initial)
	require.NoError(t, err)
	require.True(t, actor.Blobs().Exists(1))
	require.True(t, actor.Blobs().HasDirty())

	badPatch := bytes.Repeat([]byte{0xff}, 8192)
	_, _, err = actor.Write(f.Ino, 12*1024, badPatch)
	require.ErrorIs(t, err, store.ErrNoSpace)

	// 验证 hotShard 未被污染
	gotHotAfterFail, err := actor.Read(f.Ino, 0, 16*1024)
	require.NoError(t, err)
	require.Equal(t, initial, gotHotAfterFail)

	// 验证 1.pack 中原块区间 [0, 8192) 字节未被篡改
	rawDisk, err := os.ReadFile(actor.Blobs().Path(1))
	require.NoError(t, err)
	require.Equal(t, initial, rawDisk[:len(initial)])
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	// 3. 调用 SyncInode 将 1.pack 持久化落盘
	require.NoError(t, actor.Blobs().SyncInode(f.Ino))
	require.False(t, actor.Blobs().HasDirty())

	// 4. 对块 0 执行局部覆写（offset=4096, len=4096）：追加至 1.pack 尾部新偏移并切换 DataKey
	cowPatch := bytes.Repeat([]byte{0x42}, 4096)
	_, attr, err := actor.Write(f.Ino, 4096, cowPatch)
	require.NoError(t, err)
	require.Equal(t, uint64(8192), attr.Size)
	require.True(t, actor.Blobs().Exists(1))
	require.True(t, actor.Blobs().HasDirty())
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	expectedCoW := append(bytes.Repeat([]byte{0x41}, 4096), cowPatch...)
	gotCoW, err := actor.Read(f.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, expectedCoW, gotCoW)

	// 5. 再次测试 Phase A 配额超限失败时，已提交块数据完好无损
	require.NoError(t, actor.Blobs().SyncInode(f.Ino))
	require.False(t, actor.Blobs().HasDirty())
	_, _, err = actor.Write(f.Ino, 12*1024, badPatch)
	require.ErrorIs(t, err, store.ErrNoSpace)
	gotAfterFail2, err := actor.Read(f.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, expectedCoW, gotAfterFail2)
}

func TestSetAttrSizeTruncateAndExpandAcrossBlocks(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-truncate",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("trunc.bin"),
		Mode:      store.S_IFREG | store.S_ISUID | store.S_ISGID | 0o644,
	})
	require.NoError(t, err)

	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, store.BlobSize)
	_, _, err = actor.Write(f.Ino, 0, b0)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, store.BlobSize, b1)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, 2*store.BlobSize, b2)
	require.NoError(t, err)

	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))
	ref0Init, ok := readDataRefFromDB(t, actor, f.Ino, 0)
	require.True(t, ok)
	ref1Init, ok := readDataRefFromDB(t, actor, f.Ino, 1)
	require.True(t, ok)
	_, ok = readDataRefFromDB(t, actor, f.Ino, 2)
	require.True(t, ok)
	offsetBeforeTrunc := actor.Blobs().ActiveOffset()
	require.Equal(t, uint32(3*store.BlobSize), offsetBeforeTrunc)

	// 1. 截断至 1.5 个 256 KiB 逻辑块（384 KiB）：
	//    - 整块 2 完全落入截断区间，直接从 Pebble 删除 DataKey(ino, 2)；
	//    - 边界块 1 读取前 128 KiB 有效前缀追加写入活跃 .pack，并更新 DataKey(ino, 1) -> newRef；
	//    - 块 0 保持原 BlobRef 不变，且 S_ISUID|S_ISGID 被自动清除。
	size15Blocks := uint64(store.BlobSize + store.BlobSize/2)
	attr, err := actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size15Blocks})
	require.NoError(t, err)
	require.Equal(t, size15Blocks, attr.Size)
	require.Zero(t, attr.Mode&(store.S_ISUID|store.S_ISGID))

	_, ok = readDataRefFromDB(t, actor, f.Ino, 2)
	require.False(t, ok, "被整块截断移除的 block 2 DataKey 必须从 Pebble 删除")

	ref0After1, ok := readDataRefFromDB(t, actor, f.Ino, 0)
	require.True(t, ok)
	require.Equal(t, ref0Init, ref0After1)

	ref1After1, ok := readDataRefFromDB(t, actor, f.Ino, 1)
	require.True(t, ok)
	require.Equal(t, uint64(1), ref1After1.PackID)
	require.Equal(t, offsetBeforeTrunc, ref1After1.Offset, "截断后的边界残块应追加至活跃 .pack 尾部新偏移")
	require.Equal(t, uint32(store.BlobSize/2), ref1After1.Length)

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, size15Blocks, st.UsedBytes)

	// 2. 扩容至 2 个 256 KiB 逻辑块（512 KiB）：
	//    随后读取块 1 [256 KiB, 512 KiB)，前半 128 KiB 为 0x22，后半 128 KiB 为 0。
	size2Blocks := uint64(2 * store.BlobSize)
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size2Blocks})
	require.NoError(t, err)
	require.Equal(t, size2Blocks, attr.Size)

	gotBlock1, err := actor.Read(f.Ino, store.BlobSize, store.BlobSize)
	require.NoError(t, err)
	expectedBlock1 := append(bytes.Repeat([]byte{0x22}, store.BlobSize/2), make([]byte, store.BlobSize/2)...)
	require.Equal(t, expectedBlock1, gotBlock1)

	// 驱逐 hotShard 后从磁盘 .pack 读取块 1，验证物理追加字节一致
	actor.Blobs().EvictHot(ref1After1)
	gotBlock1Disk, err := actor.Read(f.Ino, store.BlobSize, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, expectedBlock1, gotBlock1Disk)
	require.NotEqual(t, ref1Init.Offset, ref1After1.Offset)

	// 3. 块内截断至 64 KiB：
	//    - 块 1 整块删除 DataKey(ino, 1)；
	//    - 块 0 读取前 64 KiB 追加写入活跃 .pack 并更新 DataKey(ino, 0)。
	size64K := uint64(64 * 1024)
	offsetBeforeTrunc2 := actor.Blobs().ActiveOffset()
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size64K})
	require.NoError(t, err)
	require.Equal(t, size64K, attr.Size)

	_, ok = readDataRefFromDB(t, actor, f.Ino, 1)
	require.False(t, ok, "整块落入截断区间的 block 1 DataKey 必须被删除")
	ref0After2, ok := readDataRefFromDB(t, actor, f.Ino, 0)
	require.True(t, ok)
	require.Equal(t, offsetBeforeTrunc2, ref0After2.Offset)
	require.Equal(t, uint32(size64K), ref0After2.Length)

	got64K, err := actor.Read(f.Ino, 0, uint32(size64K))
	require.NoError(t, err)
	require.Equal(t, b0[:size64K], got64K)

	// 4. 若边界残块截断后的有效前缀全为零，直接删除该 DataKey 而不向 .pack 追加零字节：
	//    先在块 0 的 [32 KiB, 64 KiB) 保留非零、将 [0, 32 KiB) 清零，随后截断至 16 KiB。
	require.NoError(t, actor.Fallocate(f.Ino, 0, 32*1024, store.FallocPunchHole|store.FallocKeepSize))
	offsetBeforeZeroTrunc := actor.Blobs().ActiveOffset()
	size16K := uint64(16 * 1024)
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size16K})
	require.NoError(t, err)
	require.Equal(t, size16K, attr.Size)
	_, ok = readDataRefFromDB(t, actor, f.Ino, 0)
	require.False(t, ok, "截断后全零的边界残块必须直接删除 DataKey")
	require.Equal(t, offsetBeforeZeroTrunc, actor.Blobs().ActiveOffset(), "全零残块截断不应向 .pack 追加写入")

	got16K, err := actor.Read(f.Ino, 0, uint32(size16K))
	require.NoError(t, err)
	require.Equal(t, make([]byte, size16K), got16K)

	// 5. Truncate(0)：删除全部剩余 DataKey 并归零逻辑字节统计
	_, _, err = actor.Write(f.Ino, 0, bytes.Repeat([]byte{0x55}, 4096))
	require.NoError(t, err)
	sizeZero := uint64(0)
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &sizeZero})
	require.NoError(t, err)
	require.Equal(t, uint64(0), attr.Size)
	_, ok = readDataRefFromDB(t, actor, f.Ino, 0)
	require.False(t, ok)

	stZero, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(0), stZero.UsedBytes)
}

func TestFallocateExtendKeepSizeAndPunchHole(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-falloc",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("falloc.bin"),
		Mode:      store.S_IFREG | store.S_ISUID | store.S_ISGID | 0o644,
	})
	require.NoError(t, err)

	// 1. mode = 0 默认预留扩展：Size 从 0 扩展到 256 KiB，更新 LogicalBytes，不分配全零物理块
	require.NoError(t, actor.Fallocate(f.Ino, 0, store.BlobSize, 0))
	attr, err := actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), attr.Size)
	require.Equal(t, 0, countLiveBlobFiles(t, actor.Blobs()))

	// 2. mode = FallocKeepSize：超出当前 Size 预留但不改变文件 Size
	require.NoError(t, actor.Fallocate(f.Ino, store.BlobSize, store.BlobSize, store.FallocKeepSize))
	attr, err = actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), attr.Size)

	// 3. 写入 3 个 256 KiB 逻辑块（block 0, 1, 2 追加写入 1.pack）
	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, store.BlobSize)
	_, _, err = actor.Write(f.Ino, 0, b0)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, store.BlobSize, b1)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, 2*store.BlobSize, b2)
	require.NoError(t, err)
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	ref0Before, ok := readDataRefFromDB(t, actor, f.Ino, 0)
	require.True(t, ok)
	ref2Before, ok := readDataRefFromDB(t, actor, f.Ino, 2)
	require.True(t, ok)
	offsetBeforeHole := actor.Blobs().ActiveOffset()

	// 4. FallocPunchHole | FallocKeepSize 跨 256 KiB 块打洞：[128 KiB, 640 KiB)
	//    - 块 0：后半 [128 KiB, 256 KiB) 清零后追加写入活跃 .pack，更新 DataKey(ino, 0)；
	//    - 块 1：整块落入打洞区间，直接在 Phase B 删除 DataKey(ino, 1)；
	//    - 块 2：前半 [0, 128 KiB) 清零后追加写入活跃 .pack，更新 DataKey(ino, 2)；
	//    - Size 保持 768 KiB 不变，且 S_ISUID|S_ISGID 被清除。
	holeStart := uint64(store.BlobSize / 2)
	holeLen := uint64(2 * store.BlobSize)
	require.NoError(t, actor.Fallocate(f.Ino, holeStart, holeLen, store.FallocPunchHole|store.FallocKeepSize))

	attrAfterHole, err := actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(3*store.BlobSize), attrAfterHole.Size)
	require.Zero(t, attrAfterHole.Mode&(store.S_ISUID|store.S_ISGID))

	_, ok = readDataRefFromDB(t, actor, f.Ino, 1)
	require.False(t, ok, "整块落入打洞区间的 block 1 DataKey 必须从 Pebble 删除")

	ref0After, ok := readDataRefFromDB(t, actor, f.Ino, 0)
	require.True(t, ok)
	require.Equal(t, offsetBeforeHole, ref0After.Offset)
	require.NotEqual(t, ref0Before.Offset, ref0After.Offset)

	ref2After, ok := readDataRefFromDB(t, actor, f.Ino, 2)
	require.True(t, ok)
	require.Equal(t, offsetBeforeHole+store.BlobSize, ref2After.Offset)
	require.NotEqual(t, ref2Before.Offset, ref2After.Offset)

	gotAll, err := actor.Read(f.Ino, 0, 3*store.BlobSize)
	require.NoError(t, err)
	expectedAfterHole := make([]byte, 3*store.BlobSize)
	copy(expectedAfterHole[:store.BlobSize/2], b0[:store.BlobSize/2])
	copy(expectedAfterHole[2*store.BlobSize+store.BlobSize/2:], b2[store.BlobSize/2:])
	require.Equal(t, expectedAfterHole, gotAll)

	// 5. 对块 0 的 [0, 128 KiB) 局部打洞使整块变全零 -> 直接删除 DataKey(ino, 0) 且不向 .pack 追加零字节；
	//    对块 2 的 [128 KiB, 192 KiB) 局部打洞 -> 追加更新后的非零块至活跃 .pack。
	offsetBeforeFullZeroPunch := actor.Blobs().ActiveOffset()
	require.NoError(t, actor.Fallocate(f.Ino, 0, store.BlobSize/2, store.FallocPunchHole|store.FallocKeepSize))
	_, ok = readDataRefFromDB(t, actor, f.Ino, 0)
	require.False(t, ok, "打洞后全为零的块 0 必须直接删除 DataKey")
	require.Equal(t, offsetBeforeFullZeroPunch, actor.Blobs().ActiveOffset(), "全零残块不应向活跃 .pack 追加写入")

	offsetBeforeBlock2Punch := actor.Blobs().ActiveOffset()
	require.NoError(t, actor.Fallocate(f.Ino, 2*store.BlobSize+store.BlobSize/2, store.BlobSize/4, store.FallocPunchHole|store.FallocKeepSize))
	ref2After2, ok := readDataRefFromDB(t, actor, f.Ino, 2)
	require.True(t, ok)
	require.Equal(t, offsetBeforeBlock2Punch, ref2After2.Offset)

	gotBlock2, err := actor.Read(f.Ino, 2*store.BlobSize, store.BlobSize)
	require.NoError(t, err)
	expectedBlock2 := make([]byte, store.BlobSize)
	copy(expectedBlock2[store.BlobSize*3/4:], b2[store.BlobSize*3/4:])
	require.Equal(t, expectedBlock2, gotBlock2)

	// 6. FallocZeroRange 置零并自动扩展文件 Size（不带 FallocKeepSize）：
	require.NoError(t, actor.Fallocate(f.Ino, 2*store.BlobSize+store.BlobSize*3/4, store.BlobSize/2, store.FallocZeroRange))
	attrAfterZeroExpand, err := actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(3*store.BlobSize+store.BlobSize/4), attrAfterZeroExpand.Size)
	_, ok = readDataRefFromDB(t, actor, f.Ino, 2)
	require.False(t, ok, "被 FallocZeroRange 清零剩余非零区间后，block 2 DataKey 应被删除")

	// 7. 非法 mode 校验：单独 FallocPunchHole 不带 FallocKeepSize 返回 ErrInvalid
	require.ErrorIs(t, actor.Fallocate(f.Ino, 0, 4096, store.FallocPunchHole), store.ErrInvalid)
}

func TestDataKeyCleanupOnUnlinkRenamePurgeOrphanAndCleanOrphans(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-reclaim-e2e",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. Unlink(openHandle=false) 直接删除含多个跨块（整块 + 局部块）的文件 -> 对应 DataKey 全部从 Pebble 删除
	f1, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(f1.Ino, 0, bytes.Repeat([]byte{0x11}, store.BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(f1.Ino, store.BlobSize, bytes.Repeat([]byte{0x12}, 8192))
	require.NoError(t, err)

	require.NoError(t, actor.Unlink(store.RootIno, []byte("f1"), false))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(f1.Ino))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f1.Ino, 0))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f1.Ino, 1))

	// 2. Rename 覆盖删除目标文件 (TargetOpenHandle=false) -> 被覆盖目标的 DataKey 全部删除
	src, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("src"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	dst, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("dst"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(src.Ino, 0, bytes.Repeat([]byte{0x21}, 4096))
	require.NoError(t, err)
	_, _, err = actor.Write(dst.Ino, 0, bytes.Repeat([]byte{0x22}, store.BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(dst.Ino, store.BlobSize, bytes.Repeat([]byte{0x23}, 16384))
	require.NoError(t, err)

	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno:     store.RootIno,
		OldName:          []byte("src"),
		NewParentIno:     store.RootIno,
		NewName:          []byte("dst"),
		TargetOpenHandle: false,
	}))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(dst.Ino))
	assertKeyNotFound(t, actor.DB(), store.DataKey(dst.Ino, 0))
	assertKeyNotFound(t, actor.DB(), store.DataKey(dst.Ino, 1))

	// 3. Unlink(openHandle=true) 转为孤儿节点时保留数据块可读，PurgeOrphanInode 后清理 DataKey
	require.NoError(t, actor.Unlink(store.RootIno, []byte("dst"), true))
	gotOrphan, err := actor.Read(src.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x21}, 4096), gotOrphan)

	require.NoError(t, actor.PurgeOrphanInode(src.Ino))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(src.Ino))
	assertKeyNotFound(t, actor.DB(), store.OrphanKey(src.Ino))
	assertKeyNotFound(t, actor.DB(), store.DataKey(src.Ino, 0))

	// 4. CleanOrphans 批量清理多个含数据块的孤儿节点
	var orphanInos []uint64
	for i := range 3 {
		fo, cErr := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte{byte('o'), byte('0' + i)},
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, cErr)
		orphanInos = append(orphanInos, fo.Ino)
		_, _, wErr := actor.Write(fo.Ino, 0, bytes.Repeat([]byte{byte(0x30 + i)}, 8192))
		require.NoError(t, wErr)
		require.NoError(t, actor.Unlink(store.RootIno, []byte{byte('o'), byte('0' + i)}, true))
	}

	require.NoError(t, actor.CleanOrphans())
	for _, ino := range orphanInos {
		assertKeyNotFound(t, actor.DB(), store.InodeKey(ino))
		assertKeyNotFound(t, actor.DB(), store.OrphanKey(ino))
		assertKeyNotFound(t, actor.DB(), store.DataKey(ino, 0))
	}

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(0), st.UsedBytes)
	require.Equal(t, uint64(1), st.Files) // 仅剩 RootIno
}

func TestCrossBlockPartialWriteAndHotShardBudgetEviction(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-cross-partial",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("cross.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. 跨 256 KiB 块边界的非对齐写入：从 offset = 256 KiB - 4 KiB 写入 256 KiB + 8 KiB（跨越 block 0 尾部、block 1 整块、block 2 头部）
	startOffset := uint64(store.BlobSize - 4096)
	crossPayload := make([]byte, 4096+store.BlobSize+4096)
	for i := range crossPayload {
		crossPayload[i] = byte((i*31 + 7) & 0xff)
	}
	n, attr, err := actor.Write(f.Ino, startOffset, crossPayload)
	require.NoError(t, err)
	require.Equal(t, uint32(len(crossPayload)), n)
	require.Equal(t, startOffset+uint64(len(crossPayload)), attr.Size)
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	// 再次在跨块边界覆写（追加合并块至活跃 .pack）
	crossPatch := bytes.Repeat([]byte{0xab}, 8192)
	_, _, err = actor.Write(f.Ino, uint64(store.BlobSize-4096), crossPatch)
	require.NoError(t, err)

	gotCross, err := actor.Read(f.Ino, uint64(store.BlobSize-4096), 8192)
	require.NoError(t, err)
	require.Equal(t, crossPatch, gotCross)

	// 2. 验证 ExtendTail 顺延扩容内存切片超出单分片预算（4 MiB）时自动触发 trimLocked 淘汰最旧热块：
	for i := range 20 {
		fi, cErr := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte{'s', byte('a' + i)},
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, cErr)
		// 先写 4 KiB
		_, _, wErr := actor.Write(fi.Ino, 0, bytes.Repeat([]byte{byte(i + 1)}, 4096))
		require.NoError(t, wErr)
		// 再通过 ExtendTail 顺延扩充到 256 KiB 满块
		_, _, wErr = actor.Write(fi.Ino, 4096, bytes.Repeat([]byte{byte(i + 1)}, store.BlobSize-4096))
		require.NoError(t, wErr)

		got, rErr := actor.Read(fi.Ino, 0, 8192)
		require.NoError(t, rErr)
		require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, 8192), got)
	}
}
