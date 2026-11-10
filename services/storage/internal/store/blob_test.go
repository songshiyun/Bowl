package store_test

import (
	"bytes"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestBlobStorePackPathAllocationAndReopenScan(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	require.Equal(t, filepath.Join(base, "packs"), blobs.Root())
	require.Equal(t, filepath.Join(base, "packs", "00000000000001ab.pack"), blobs.Path(0x1ab))

	var firstID, lastInFirstSeg uint64
	for i := range int(store.BlobIDSegment) {
		id, err := blobs.NextPackID()
		require.NoError(t, err)
		if i == 0 {
			firstID = id
		}
		if i == int(store.BlobIDSegment)-1 {
			lastInFirstSeg = id
		}
	}
	require.Equal(t, uint64(1), firstID)
	require.Equal(t, uint64(4096), lastInFirstSeg)

	nextID, err := blobs.NextPackID()
	require.NoError(t, err)
	require.Equal(t, uint64(4097), nextID)

	// 验证重启扫描已有 .pack 文件恢复最大 packID
	base2 := t.TempDir()
	packDir := filepath.Join(base2, "packs")
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(packDir, "0000000000000250.pack"), []byte("pack-data"), 0o644))

	reopened := store.NewBlobStore(base2)
	defer func() { require.NoError(t, reopened.Close()) }()
	ref, err := reopened.Append(10, []byte("hello-pack"))
	require.NoError(t, err)
	require.Greater(t, ref.PackID, uint64(0x0250))
}

func TestBlobStoreAppendAutoRolloverAtPackMaxSize(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	require.Equal(t, uint32(store.PackMaxSize), blobs.MaxPackSize())

	// 将封口阈值设为 3 * BlobSize (768 KiB) 以验证跨包自动封口切包
	blobs.SetMaxPackSize(3 * store.BlobSize)
	require.Equal(t, uint32(3*store.BlobSize), blobs.MaxPackSize())

	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, 128*1024) // 128 KiB
	b3 := bytes.Repeat([]byte{0x44}, store.BlobSize)

	ref0, err := blobs.Append(2, b0)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: store.BlobSize}, ref0)

	ref1, err := blobs.Append(2, b1)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: store.BlobSize, Length: store.BlobSize}, ref1)

	ref2, err := blobs.Append(2, b2)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 2 * store.BlobSize, Length: 128 * 1024}, ref2)

	// 此时 Pack 1 已用 640 KiB，剩余 128 KiB < 256 KiB (b3)，追加 b3 必须自动将 Pack 1 封口并切入 Pack 2 (Offset=0)
	ref3, err := blobs.Append(2, b3)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 2, Offset: 0, Length: store.BlobSize}, ref3)
	require.Equal(t, uint64(2), blobs.ActivePackID())
	require.Equal(t, uint32(store.BlobSize), blobs.ActiveOffset())

	// 验证 Pack 1 物理文件大小恰好为 640 KiB，Pack 2 物理文件大小恰好为 256 KiB
	info1, err := os.Stat(blobs.Path(1))
	require.NoError(t, err)
	require.Equal(t, int64(2*store.BlobSize+128*1024), info1.Size())

	info2, err := os.Stat(blobs.Path(2))
	require.NoError(t, err)
	require.Equal(t, int64(store.BlobSize), info2.Size())

	// 验证所有块通过 ReadAt 无论命中热缓存还是驱逐后走常驻只读句柄均能精确读取
	for _, tc := range []struct {
		ref  store.BlobRef
		want []byte
	}{
		{ref0, b0},
		{ref1, b1},
		{ref2, b2},
		{ref3, b3},
	} {
		blobs.EvictHot(tc.ref)
		got := make([]byte, tc.ref.Length)
		require.NoError(t, blobs.ReadAt(tc.ref, 0, got))
		require.Equal(t, tc.want, got)
	}
}

func TestBlobStoreExtendTailAndNonTailRejection(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	// 1. 首次追加 4 KiB 块（位于 Pack 1, Offset 0, Length 4096，处于活跃包最末尾）
	chunk0 := bytes.Repeat([]byte{0xaa}, 4096)
	ref, err := blobs.Append(5, chunk0)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: 4096}, ref)

	// 2. 活跃包最末尾顺延追加 4 KiB（patchOffset = 4096, newLength = 8192）
	chunk1 := bytes.Repeat([]byte{0xbb}, 4096)
	extRef1, ok, err := blobs.ExtendTail(5, ref, 4096, chunk1, 8192)
	require.NoError(t, err)
	require.True(t, ok, "位于活跃包最末尾的块必须允许原地顺延扩展")
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: 8192}, extRef1)
	require.Equal(t, uint32(8192), blobs.ActiveOffset())

	// 3. 活跃包最末尾块内局部覆写并顺延（patchOffset = 2048, len = 8192 -> newLength = 10240）
	chunk2 := bytes.Repeat([]byte{0xcc}, 8192)
	extRef2, ok, err := blobs.ExtendTail(5, extRef1, 2048, chunk2, 10240)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: 10240}, extRef2)

	// 4. 跨越尾块末尾空洞顺延扩展（从 10240 跨越到 12288 写入 2048 字节 -> [10240, 12288) 自动填零，newLength = 14336）
	chunk3 := bytes.Repeat([]byte{0xdd}, 2048)
	extRef3, ok, err := blobs.ExtendTail(5, extRef2, 12288, chunk3, 14336)
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: 14336}, extRef3)

	expected := make([]byte, 14336)
	copy(expected[0:4096], chunk0)
	copy(expected[4096:8192], chunk1)
	copy(expected[2048:10240], chunk2)
	copy(expected[12288:14336], chunk3)

	// 验证热缓存命中读取与驱逐后磁盘读取完全一致
	gotHot := make([]byte, len(expected))
	require.NoError(t, blobs.ReadAt(extRef3, 0, gotHot))
	require.Equal(t, expected, gotHot)

	blobs.EvictHot(extRef3)
	gotDisk := make([]byte, len(expected))
	require.NoError(t, blobs.ReadAt(extRef3, 0, gotDisk))
	require.Equal(t, expected, gotDisk)

	// 5. 当后续追加了另一个块后，extRef3 不再位于活跃包最末尾，ExtendTail 必须返回 ok=false 且不篡改历史偏移数据
	otherRef, err := blobs.Append(6, []byte("tail-breaker"))
	require.NoError(t, err)
	require.Equal(t, uint32(14336), otherRef.Offset)

	_, ok, err = blobs.ExtendTail(5, extRef3, 14336, []byte("no-op"), 14341)
	require.NoError(t, err)
	require.False(t, ok, "非活跃包最末尾的块必须拒绝原地顺延")

	// 6. 滚动封口后原末尾块 otherRef 也不再允许 ExtendTail
	blobs.SetMaxPackSize(otherRef.Offset + otherRef.Length)
	nextPackRef, err := blobs.Append(6, []byte("next-pack"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), nextPackRef.PackID)
	_, ok, err = blobs.ExtendTail(6, otherRef, otherRef.Length, []byte("more"), otherRef.Length+4)
	require.NoError(t, err)
	require.False(t, ok, "已封口包中的块必须拒绝 ExtendTail")
}

func TestBlobStoreHotShardCacheEvictionAndReadAt(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	// 单个 hotShard 预算为 4 MiB（16 个 256 KiB 块），总预算 64 MiB（256 个 256 KiB 块）。
	// 连续写入 17 个落入同一分片 (每隔 16 个 256 KiB 块落入同一分片) 的块，使第 1 个块从该分片热缓存淘汰。
	var sameShardRefs []store.BlobRef
	var sameShardPayloads [][]byte

	for i := range 17 * 16 {
		fill := byte((i*13 + 7) & 0xff)
		data := bytes.Repeat([]byte{fill}, store.BlobSize)
		ref, err := blobs.Append(10, data)
		require.NoError(t, err)
		if i%16 == 0 {
			sameShardRefs = append(sameShardRefs, ref)
			sameShardPayloads = append(sameShardPayloads, data)
		}
	}
	require.Len(t, sameShardRefs, 17)

	// 验证最新的块 sameShardRefs[16] 仍在 hotShard 中：即使将底层 .pack 文件截断为 0，ReadAt 仍命中内存返回正确字节
	lastRef := sameShardRefs[16]
	origLastPackBytes, err := os.ReadFile(blobs.Path(lastRef.PackID))
	require.NoError(t, err)
	require.NoError(t, os.Truncate(blobs.Path(lastRef.PackID), 0))

	buf := make([]byte, 4096)
	require.NoError(t, blobs.ReadAt(lastRef, 128, buf))
	require.Equal(t, sameShardPayloads[16][128:128+4096], buf)

	// 验证最旧的块 sameShardRefs[0] 已从 hotShard 淘汰：底层 .pack 被截断为 0 时走常驻句柄 ReadAt 返回 EOF 错误
	firstRef := sameShardRefs[0]
	origFirstPackBytes, err := os.ReadFile(blobs.Path(firstRef.PackID))
	require.NoError(t, err)
	require.NoError(t, os.Truncate(blobs.Path(firstRef.PackID), 0))
	require.Error(t, blobs.ReadAt(firstRef, 0, buf))

	// 恢复底层 .pack 文件后，常驻句柄 ReadAt 正常读取成功
	require.NoError(t, os.WriteFile(blobs.Path(lastRef.PackID), origLastPackBytes, 0o644))
	require.NoError(t, os.WriteFile(blobs.Path(firstRef.PackID), origFirstPackBytes, 0o644))
	require.NoError(t, blobs.ReadAt(firstRef, 512, buf))
	require.Equal(t, sameShardPayloads[0][512:512+4096], buf)
}

func TestBlobStoreSyncInodeAndRemovePack(t *testing.T) {
	srcBase := t.TempDir()

	src := store.NewBlobStore(srcBase)
	defer func() { require.NoError(t, src.Close()) }()

	// 1. 首次向 Pack 1 追加写入 fileA (ino=10) 与 fileB (ino=20)
	dataA := bytes.Repeat([]byte{0x31}, 8192)
	dataB := bytes.Repeat([]byte{0x32}, 8192)
	refA, err := src.Append(10, dataA)
	require.NoError(t, err)
	refB, err := src.Append(20, dataB)
	require.NoError(t, err)
	require.Equal(t, refA.PackID, refB.PackID)
	require.True(t, src.HasDirty())

	// 2. SyncInode(10)：对新建 Pack 1 执行 1 次 fdatasync，并对 packs/ 目录执行 1 次 fsync
	require.NoError(t, src.SyncInode(10))
	require.Equal(t, uint64(1), src.SyncFileCount())
	require.Equal(t, uint64(1), src.SyncDirCount())

	// 3. 继续向同一个活跃 Pack 1 追加写入并再次 SyncInode(10)：
	//    仅执行 1 次文件 fdatasync，不得重复执行 packs/ 目录 fsync（SyncDirCount 保持不变）
	refA2, err := src.Append(10, bytes.Repeat([]byte{0x33}, 4096))
	require.NoError(t, err)
	require.Equal(t, refA.PackID, refA2.PackID)
	require.NoError(t, src.SyncInode(10))
	require.Equal(t, uint64(2), src.SyncFileCount())
	require.Equal(t, uint64(1), src.SyncDirCount(), "已同步过的 .pack 继续追加写不得重复触发目录 fsync")

	// 4. 触发滚动切包至 Pack 2 后，调用 RemovePack(packID) 删除已封口的 Pack 1 并关闭源句柄
	src.SetMaxPackSize(src.ActiveOffset())
	refPack2, err := src.Append(10, []byte("rollover"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), refPack2.PackID)

	require.NoError(t, src.RemovePack(refA.PackID))
	require.False(t, src.Exists(refA.PackID))
	require.True(t, src.Exists(refPack2.PackID))
}

func TestBlobStoreCloseAndValidationErrorPaths(t *testing.T) {
	blobs := store.NewBlobStore(t.TempDir())

	// 1. Append 边界与非法长度校验
	_, err := blobs.Append(1, nil)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = blobs.Append(1, make([]byte, store.BlobSize+1))
	require.ErrorIs(t, err, store.ErrInvalidValue)

	ref, err := blobs.Append(1, []byte("hello-pack"))
	require.NoError(t, err)

	// 2. ExtendTail 非法参数校验
	_, _, err = blobs.ExtendTail(1, store.BlobRef{}, 0, []byte("x"), 1)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, _, err = blobs.ExtendTail(1, ref, 0, []byte("x"), ref.Length-1)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, _, err = blobs.ExtendTail(1, ref, 0, []byte("x"), store.BlobSize+1)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, _, err = blobs.ExtendTail(1, ref, ref.Length, []byte("overflow"), ref.Length+2)
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 3. Read / ReadAt 非法 BlobRef 与越界偏移校验
	buf := make([]byte, 4)
	require.NoError(t, blobs.ReadAt(ref, 0, nil))
	require.ErrorIs(t, blobs.ReadAt(store.BlobRef{}, 0, buf), store.ErrInvalidValue)
	require.ErrorIs(t, blobs.ReadAt(ref, -1, buf), io.ErrUnexpectedEOF)
	require.ErrorIs(t, blobs.ReadAt(ref, int64(ref.Length), buf), io.ErrUnexpectedEOF)
	_, err = blobs.Read(store.BlobRef{})
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 4. Close 后幂等且拒绝后续 Append / AppendBlocks / ExtendTail
	require.NoError(t, blobs.Close())
	require.NoError(t, blobs.Close())

	_, err = blobs.Append(1, []byte("after-close"))
	require.ErrorIs(t, err, store.ErrClosed)
	_, err = blobs.AppendBlocks(1, make([]byte, store.BlobSize))
	require.ErrorIs(t, err, store.ErrClosed)
	_, _, err = blobs.ExtendTail(1, ref, ref.Length, []byte("!"), ref.Length+1)
	require.ErrorIs(t, err, store.ErrClosed)
}

func TestBlobStoreAppendBlocksCoalescingAndCrossPackRollover(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	// 非法长度校验：0 字节、非 256 KiB 倍数、或 MaxPackSize < 256 KiB
	_, err := blobs.AppendBlocks(1, nil)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = blobs.AppendBlocks(1, make([]byte, store.BlobSize+1024))
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 将封口容量设为 3 个 256 KiB 块（768 KiB）
	blobs.SetMaxPackSize(3 * store.BlobSize)

	// 先写入 128 KiB 局部块，使 Pack 1 偏移处于 128 KiB，剩余容量 640 KiB（仅能再容纳 2 个 256 KiB 满块）
	prefix := bytes.Repeat([]byte{0x05}, 128*1024)
	refPrefix, err := blobs.Append(10, prefix)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: 128 * 1024}, refPrefix)

	// 一次性批量追加 4 个 256 KiB 满块（共 1 MiB）：
	// - 前 2 块（512 KiB）合并单次 WriteAt 写入 Pack 1 [128 KiB, 640 KiB)
	// - 后 2 块（512 KiB）自动封口 Pack 1 并合并单次 WriteAt 写入 Pack 2 [0, 512 KiB)
	payload := make([]byte, 4*store.BlobSize)
	blocks := make([][]byte, 4)
	for i := range 4 {
		fill := byte(0x10 + i)
		blocks[i] = bytes.Repeat([]byte{fill}, store.BlobSize)
		copy(payload[i*store.BlobSize:(i+1)*store.BlobSize], blocks[i])
	}

	refs, err := blobs.AppendBlocks(10, payload)
	require.NoError(t, err)
	require.Len(t, refs, 4)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 128 * 1024, Length: store.BlobSize}, refs[0])
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 128*1024 + store.BlobSize, Length: store.BlobSize}, refs[1])
	require.Equal(t, store.BlobRef{PackID: 2, Offset: 0, Length: store.BlobSize}, refs[2])
	require.Equal(t, store.BlobRef{PackID: 2, Offset: store.BlobSize, Length: store.BlobSize}, refs[3])
	require.Equal(t, uint64(2), blobs.ActivePackID())
	require.Equal(t, uint32(2*store.BlobSize), blobs.ActiveOffset())

	// 验证所有块通过热缓存与驱逐后的磁盘读取完全一致
	for i, ref := range refs {
		gotHot := make([]byte, store.BlobSize)
		require.NoError(t, blobs.ReadAt(ref, 0, gotHot))
		require.Equal(t, blocks[i], gotHot)

		blobs.EvictHot(ref)
		gotDisk := make([]byte, store.BlobSize)
		require.NoError(t, blobs.ReadAt(ref, 0, gotDisk))
		require.Equal(t, blocks[i], gotDisk)
	}
}

func TestBlobStoreHotShardSlotBufferReuseZeroAllocAfterWarmup(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	block := bytes.Repeat([]byte{0x5a}, store.BlobSize)

	// 预热：写入 256 个 256 KiB 满块（16 分片 × 16 槽位 = 64 MiB 热缓存预算填满）
	for range 256 {
		_, err := blobs.Append(1, block)
		require.NoError(t, err)
	}

	// 预热填满后，继续追加 256 KiB 满块应原地复用被淘汰的最旧槽位底层切片，不发生 256 KiB 堆分配
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const MeasureBlocks = 64
	for i := range MeasureBlocks {
		block[0] = byte(i)
		_, err := blobs.Append(1, block)
		require.NoError(t, err)
	}
	runtime.ReadMemStats(&after)

	totalAllocDelta := after.TotalAlloc - before.TotalAlloc
	// 若未复用底层切片，64 次 256 KiB 分配至少产生 16 MiB (16,777,216 B) 堆分配；复用后仅产生极小文件 I/O 内部开销 (< 512 KiB)
	require.Less(t, totalAllocDelta, uint64(512*1024), "热缓存填满预热后继续满块写入必须原地复用环形槽位底层切片（避免每次分配 256 KiB）")

	// 验证通过 AppendBlocks 批量追加同样复用底层切片且热缓存读取内容准确
	batchPayload := bytes.Repeat([]byte{0x6b}, 4*store.BlobSize)
	runtime.ReadMemStats(&before)
	refs, err := blobs.AppendBlocks(1, batchPayload)
	require.NoError(t, err)
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64*1024))
	for _, ref := range refs {
		got := make([]byte, store.BlobSize)
		require.NoError(t, blobs.ReadAt(ref, 0, got))
		require.Equal(t, batchPayload[:store.BlobSize], got)
	}
}

func TestBlobStoreSealedPackBackgroundPreSyncAndDirBarrierInheritance(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	blobs.SetMaxPackSize(2 * store.BlobSize)

	// 1. ino=10 写满 Pack 1（2 个 256 KiB 块）
	payload1 := bytes.Repeat([]byte{0x11}, 2*store.BlobSize)
	refs1, err := blobs.AppendBlocks(10, payload1)
	require.NoError(t, err)
	require.Len(t, refs1, 2)
	require.Equal(t, uint64(1), refs1[0].PackID)
	require.Equal(t, uint64(1), refs1[1].PackID)

	// 2. ino=20 追加 1 个块触发 Pack 1 封口并切入新活跃包 Pack 2
	ref2, err := blobs.Append(20, bytes.Repeat([]byte{0x22}, store.BlobSize))
	require.NoError(t, err)
	require.Equal(t, uint64(2), ref2.PackID)

	// 3. 后台刷盘协程自动对已封口的 Pack 1 执行异步 fdatasync
	require.Eventually(t, func() bool {
		return blobs.SyncFileCount() == 1
	}, 2*time.Second, 5*time.Millisecond, "已封口 Pack 1 必须由后台协程自动完成预刷盘")
	require.Equal(t, uint64(0), blobs.SyncDirCount(), "后台预刷盘仅刷数据文件，不提前执行目录 fsync")
	require.True(t, blobs.HasDirty())

	// 4. 首次对仅关联已封口新建包 Pack 1 的 ino=10 调用 SyncInode(10)：
	//    - Pack 1 已被后台刷完消记，SyncFileCount 不得重复递增（保持为 1）
	//    - 因 Pack 1 属于 dirtyNew 新建包，消记时继承 dirDirty=true，SyncInode(10) 必须可靠执行 1 次 packs/ 目录 fsync
	require.NoError(t, blobs.SyncInode(10))
	require.Equal(t, uint64(1), blobs.SyncFileCount(), "已被后台预刷盘的封口包不得在前台 SyncInode 时重复 fdatasync")
	require.Equal(t, uint64(1), blobs.SyncDirCount(), "新建封口包被后台提前刷完后，首次 SyncInode 必须执行 1 次 packs/ 目录 fsync")

	// 5. 再次调用 SyncInode(10) 应为无操作（SyncFileCount 与 SyncDirCount 均不变）
	require.NoError(t, blobs.SyncInode(10))
	require.Equal(t, uint64(1), blobs.SyncFileCount())
	require.Equal(t, uint64(1), blobs.SyncDirCount())

	// 6. 对活跃包 Pack 2 关联的 ino=20 调用 SyncInode(20)
	require.NoError(t, blobs.SyncInode(20))
	require.Equal(t, uint64(2), blobs.SyncFileCount())
	require.Equal(t, uint64(2), blobs.SyncDirCount())
	require.False(t, blobs.HasDirty())
}

func TestBlobStoreForegroundBackgroundSyncCoordinationAndSkipAlreadySynced(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	blobs.SetMaxPackSize(store.BlobSize)

	enteredPack1 := make(chan struct{})
	releasePack1 := make(chan struct{})
	enteredPack2 := make(chan struct{})
	releasePack2 := make(chan struct{})

	var pack1HookOnce, pack2HookOnce sync.Once
	blobs.SetSyncFileHookForTest(func(packID uint64) {
		switch packID {
		case 1:
			pack1HookOnce.Do(func() {
				close(enteredPack1)
				<-releasePack1
			})
		case 2:
			pack2HookOnce.Do(func() {
				close(enteredPack2)
				<-releasePack2
			})
		}
	})

	// 1. 写入 Pack 1 (ino=10) 并通过写入 Pack 2 (ino=20) 触发 Pack 1 封口进入后台刷盘
	_, err := blobs.Append(10, bytes.Repeat([]byte{0x11}, store.BlobSize))
	require.NoError(t, err)
	_, err = blobs.Append(20, bytes.Repeat([]byte{0x22}, store.BlobSize))
	require.NoError(t, err)

	// 等待后台协程进入 Pack 1 的 syncPack（尚未执行底层 fdatasync）
	<-enteredPack1

	// 验证「先刷盘、后消记」：后台刷盘完成前 dirtyPacks 与 inoPacks[10] 仍保留，
	// 此时前台调用 SyncInode(10) 必须等待 Pack 1 落盘完成后才能返回，绝不提前返回。
	fgDone := make(chan error, 1)
	go func() {
		fgDone <- blobs.SyncInode(10)
	}()

	select {
	case fgErr := <-fgDone:
		t.Fatalf("后台正在刷盘 Pack 1 时，前台 SyncInode(10) 不得在 fdatasync 完成前提前返回 (got %v)", fgErr)
	case <-time.After(25 * time.Millisecond):
	}

	// 放行 Pack 1 后台刷盘：前后台协同单次完成 Pack 1 的 fdatasync，且前台 SyncInode(10) 完成目录 fsync
	close(releasePack1)
	require.NoError(t, <-fgDone)
	require.Equal(t, uint64(1), blobs.SyncFileCount(), "同一封口包前后台并发刷盘必须去重协同，仅执行 1 次 fdatasync")
	require.Equal(t, uint64(1), blobs.SyncDirCount(), "前台 SyncInode(10) 等待后台封口包完成后必须执行 1 次目录 fsync")

	// 2. 继续追加写入 Pack 3 (ino=30)、Pack 4 (ino=40)、Pack 5 (ino=50)：
	//    此时 Pack 2 封口并卡在后台刷盘钩子 enteredPack2，Pack 3 与 Pack 4 已封口排在后台队列中。
	_, err = blobs.Append(30, bytes.Repeat([]byte{0x33}, store.BlobSize))
	require.NoError(t, err)
	<-enteredPack2

	_, err = blobs.Append(40, bytes.Repeat([]byte{0x44}, store.BlobSize))
	require.NoError(t, err)
	_, err = blobs.Append(50, bytes.Repeat([]byte{0x55}, store.BlobSize))
	require.NoError(t, err)

	// 在后台协程仍卡在 Pack 2 期间：
	// - 前台提前刷完已封口的 Pack 3 (SyncInode(30))
	// - 前台直接删除已封口的 Pack 4 (RemovePack(4))
	require.NoError(t, blobs.SyncInode(30))
	require.Equal(t, uint64(2), blobs.SyncFileCount(), "前台 SyncInode(30) 率先完成 Pack 3 的 fdatasync")
	require.NoError(t, blobs.RemovePack(4))

	// 放行 Pack 2，等待后台队列排空（Pack 2 完成 fdatasync，Pack 3 已由前台刷完跳过，Pack 4 已被删除跳过）
	close(releasePack2)
	require.Eventually(t, func() bool {
		return blobs.SyncFileCount() == 3
	}, 2*time.Second, 5*time.Millisecond)

	// 再同步最后一个活跃包 Pack 5 (ino=50)，总 fdatasync 次数精确为 4（Pack 1, 2, 3, 5），无后台滞后重复刷盘
	require.NoError(t, blobs.SyncInode(50))
	require.Equal(t, uint64(4), blobs.SyncFileCount())
	require.False(t, blobs.HasDirty())
}

func TestBlobStoreSyncBatchBoundedParallelismAndConcurrentRemoveClose(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	blobs.SetMaxPackSize(store.BlobSize)

	// 1. 先卡住后台协程对 Pack 1 的预刷盘，以便积累 12 个脏 .pack 后测试 syncBatch 多包有界并发（上限 8）
	enteredPack1 := make(chan struct{})
	releasePack1 := make(chan struct{})
	var pack1Once sync.Once

	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	blobs.SetSyncFileHookForTest(func(packID uint64) {
		if packID == 1 {
			pack1Once.Do(func() {
				close(enteredPack1)
				<-releasePack1
			})
			return
		}
		cur := inFlight.Add(1)
		for {
			prev := maxInFlight.Load()
			if cur <= prev || maxInFlight.CompareAndSwap(prev, cur) {
				break
			}
		}
		time.Sleep(15 * time.Millisecond)
		inFlight.Add(-1)
	})

	const totalPacks = 12
	for i := range totalPacks {
		_, err := blobs.Append(uint64(100+i), bytes.Repeat([]byte{byte(i + 1)}, store.BlobSize))
		require.NoError(t, err)
	}
	<-enteredPack1

	// 放行 Pack 1 并立即在前台调用 Sync() 同步全部 12 个脏包
	close(releasePack1)
	require.NoError(t, blobs.Sync())
	require.Equal(t, uint64(totalPacks), blobs.SyncFileCount())
	require.Equal(t, uint64(1), blobs.SyncDirCount())
	require.False(t, blobs.HasDirty())
	require.GreaterOrEqual(t, maxInFlight.Load(), int32(2), "多脏包 syncBatch 必须并发执行 syncPack")
	require.LessOrEqual(t, maxInFlight.Load(), int32(9), "syncBatch 并发度（至多 8 协程 + 1 后台协程）必须受有界上限约束")

	// 2. 验证高频切包后台预刷盘与并发 SyncInode / RemovePack / Close 无竞态或句柄泄漏
	blobs.SetSyncFileHookForTest(nil)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := range 24 {
			_, _ = blobs.Append(200, bytes.Repeat([]byte{byte(0xa0 + i)}, store.BlobSize))
		}
	}()
	go func() {
		defer wg.Done()
		for p := uint64(1); p <= 16; p++ {
			_ = blobs.SyncInode(200)
			_ = blobs.RemovePack(p)
			time.Sleep(time.Millisecond)
		}
	}()
	wg.Wait()
	require.NoError(t, blobs.Close())
}
