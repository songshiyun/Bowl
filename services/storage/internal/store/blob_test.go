package store_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requireHotAndColdRead(t *testing.T, blobs *store.BlobStore, ref store.BlobRef, want []byte) {
	t.Helper()
	gotHot := make([]byte, len(want))
	require.NoError(t, blobs.ReadAt(ref, 0, gotHot))
	require.Equal(t, want, gotHot)

	blobs.EvictHot(ref)
	gotCold := make([]byte, len(want))
	require.NoError(t, blobs.ReadAt(ref, 0, gotCold))
	require.Equal(t, want, gotCold)
}

func TestBlobStorePackPathAllocationAndReopenScan(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	require.Equal(t, filepath.Join(base, "packs"), blobs.Root())
	require.Equal(t, filepath.Join(base, "packs", "00000000000001ab.pack"), blobs.Path(0x1ab))

	firstID, err := blobs.NextPackID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), firstID)
	secondID, err := blobs.NextPackID(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), secondID)

	// 验证重启扫描已有高编号 .pack 文件后自动推进分配水位，防止新写入覆盖历史封包
	base2 := t.TempDir()
	packDir := filepath.Join(base2, "packs")
	require.NoError(t, os.MkdirAll(packDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(packDir, "0000000000000250.pack"), []byte("pack-data"), 0o644))

	reopened := store.NewBlobStore(base2)
	defer func() { require.NoError(t, reopened.Close()) }()
	ref, err := reopened.Append(ctx, 10, []byte("hello-pack"))
	require.NoError(t, err)
	require.Greater(t, ref.PackID, uint64(0x0250))
}

func TestBlobStoreAppendAutoRolloverAndAppendBlocksCrossPack(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	require.Equal(t, uint32(store.PackMaxSize), blobs.MaxPackSize())

	// 将封口阈值设为 3 * BlobSize (768 KiB) 以验证单块与多块跨包自动封口切包
	blobs.SetMaxPackSize(3 * store.BlobSize)
	require.Equal(t, uint32(3*store.BlobSize), blobs.MaxPackSize())

	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, 128*1024)
	b3 := bytes.Repeat([]byte{0x44}, 128*1024)

	ref0, err := blobs.Append(ctx, 2, b0)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 0, Length: store.BlobSize}, ref0)

	ref1, err := blobs.Append(ctx, 2, b1)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: store.BlobSize, Length: store.BlobSize}, ref1)

	ref2, err := blobs.Append(ctx, 2, b2)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 1, Offset: 2 * store.BlobSize, Length: 128 * 1024}, ref2)

	// Pack 1 已用 640 KiB，剩余 128 KiB 容纳不下 256 KiB 新块；先验证追加 256 KiB 自动封口 Pack 1 并切入 Pack 2
	fullInPack2 := bytes.Repeat([]byte{0x55}, store.BlobSize)
	ref3, err := blobs.Append(ctx, 2, fullInPack2)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 2, Offset: 0, Length: store.BlobSize}, ref3)
	require.Equal(t, uint64(2), blobs.ActivePackID())

	// 在 Pack 2 再追加 128 KiB (b3)，此时 Pack 2 已用 384 KiB，剩余 384 KiB（仅够再容纳 1 个 256 KiB 满块）
	ref4, err := blobs.Append(ctx, 2, b3)
	require.NoError(t, err)
	require.Equal(t, store.BlobRef{PackID: 2, Offset: store.BlobSize, Length: 128 * 1024}, ref4)

	// 一次性批量追加 3 个 256 KiB 满块（共 768 KiB）：
	// - 第 0 块写入 Pack 2 尾部 [384 KiB, 640 KiB)
	// - 第 1、2 块自动封口 Pack 2 并合并写入 Pack 3 [0, 512 KiB)
	m0 := bytes.Repeat([]byte{0x61}, store.BlobSize)
	m1 := bytes.Repeat([]byte{0x62}, store.BlobSize)
	m2 := bytes.Repeat([]byte{0x63}, store.BlobSize)
	batchPayload := bytes.Join([][]byte{m0, m1, m2}, nil)

	refs, err := blobs.AppendBlocks(ctx, 2, batchPayload)
	require.NoError(t, err)
	require.Len(t, refs, 3)
	require.Equal(t, store.BlobRef{PackID: 2, Offset: store.BlobSize + 128*1024, Length: store.BlobSize}, refs[0])
	require.Equal(t, store.BlobRef{PackID: 3, Offset: 0, Length: store.BlobSize}, refs[1])
	require.Equal(t, store.BlobRef{PackID: 3, Offset: store.BlobSize, Length: store.BlobSize}, refs[2])
	require.Equal(t, uint64(3), blobs.ActivePackID())
	require.Equal(t, uint32(2*store.BlobSize), blobs.ActiveOffset())

	info1, err := os.Stat(blobs.Path(1))
	require.NoError(t, err)
	require.Equal(t, int64(2*store.BlobSize+128*1024), info1.Size())

	info2, err := os.Stat(blobs.Path(2))
	require.NoError(t, err)
	require.Equal(t, int64(2*store.BlobSize+128*1024), info2.Size())

	requireHotAndColdRead(t, blobs, ref0, b0)
	requireHotAndColdRead(t, blobs, ref1, b1)
	requireHotAndColdRead(t, blobs, ref2, b2)
	requireHotAndColdRead(t, blobs, ref3, fullInPack2)
	requireHotAndColdRead(t, blobs, ref4, b3)
	requireHotAndColdRead(t, blobs, refs[0], m0)
	requireHotAndColdRead(t, blobs, refs[1], m1)
	requireHotAndColdRead(t, blobs, refs[2], m2)
}

func TestBlobStoreExtendTailAndNonTailRejection(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	// 1. 首次追加 4 KiB 块（位于 Pack 1, Offset 0, Length 4096，处于活跃包最末尾）
	chunk0 := bytes.Repeat([]byte{0xaa}, 4096)
	ref, err := blobs.Append(ctx, 5, chunk0)
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

	// 4. 跨越尾块末尾空洞顺延扩展（[10240, 12288) 自动填零，newLength = 14336）
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

	requireHotAndColdRead(t, blobs, extRef3, expected)

	// 5. 当后续追加了另一个块后，extRef3 不再位于活跃包最末尾，ExtendTail 必须返回 ok=false 且不篡改历史偏移数据
	otherRef, err := blobs.Append(ctx, 6, []byte("tail-breaker"))
	require.NoError(t, err)
	require.Equal(t, uint32(14336), otherRef.Offset)

	_, ok, err = blobs.ExtendTail(5, extRef3, 14336, []byte("no-op"), 14341)
	require.NoError(t, err)
	require.False(t, ok, "非活跃包最末尾的块必须拒绝原地顺延")

	// 6. 滚动封口后原末尾块 otherRef 也不再允许 ExtendTail
	blobs.SetMaxPackSize(otherRef.Offset + otherRef.Length)
	nextPackRef, err := blobs.Append(ctx, 6, []byte("next-pack"))
	require.NoError(t, err)
	require.Equal(t, uint64(2), nextPackRef.PackID)
	_, ok, err = blobs.ExtendTail(6, otherRef, otherRef.Length, []byte("more"), otherRef.Length+4)
	require.NoError(t, err)
	require.False(t, ok, "已封口包中的块必须拒绝 ExtendTail")
}

func TestBlobStoreSealedPackBackgroundPreSyncAndDirBarrierInheritance(t *testing.T) {
	ctx := context.Background()
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	defer func() { require.NoError(t, blobs.Close()) }()

	blobs.SetMaxPackSize(2 * store.BlobSize)

	// 1. ino=10 写满 Pack 1（2 个 256 KiB 块）
	payload1 := bytes.Repeat([]byte{0x11}, 2*store.BlobSize)
	refs1, err := blobs.AppendBlocks(ctx, 10, payload1)
	require.NoError(t, err)
	require.Len(t, refs1, 2)
	require.Equal(t, uint64(1), refs1[0].PackID)

	// 2. ino=20 追加 1 个块触发 Pack 1 封口并切入新活跃包 Pack 2
	ref2, err := blobs.Append(ctx, 20, bytes.Repeat([]byte{0x22}, store.BlobSize))
	require.NoError(t, err)
	require.Equal(t, uint64(2), ref2.PackID)

	// 3. 后台刷盘协程自动对已封口的 Pack 1 执行异步 fdatasync
	require.Eventually(t, func() bool {
		return blobs.SyncFileCount() == 1
	}, 2*time.Second, 5*time.Millisecond, "已封口 Pack 1 必须由后台协程自动完成预刷盘")
	require.Zero(t, blobs.SyncDirCount(), "后台预刷盘仅刷数据文件，不提前执行目录 fsync")
	require.True(t, blobs.HasDirty())

	// 4. 首次对仅关联已封口新建包 Pack 1 的 ino=10 调用 SyncInode(10)：
	//    - Pack 1 已被后台刷完消记，SyncFileCount 不得重复递增（保持为 1）
	//    - 因 Pack 1 属于新建包，消记时继承 dirDirty=true，SyncInode(10) 必须执行 1 次 packs/ 目录 fsync
	require.NoError(t, blobs.SyncInode(10))
	require.Equal(t, uint64(1), blobs.SyncFileCount(), "已被后台预刷盘的封口包不得在前台 SyncInode 时重复 fdatasync")
	require.Equal(t, uint64(1), blobs.SyncDirCount(), "新建封口包被后台提前刷完后，首次 SyncInode 必须执行 1 次 packs/ 目录 fsync")

	// 5. 对活跃包 Pack 2 关联的 ino=20 调用 SyncInode(20)，并在同包继续追加后验证不重复执行目录 fsync
	require.NoError(t, blobs.SyncInode(20))
	require.Equal(t, uint64(2), blobs.SyncFileCount())
	require.Equal(t, uint64(2), blobs.SyncDirCount())
	require.False(t, blobs.HasDirty())

	_, err = blobs.Append(ctx, 20, bytes.Repeat([]byte{0x33}, 4096))
	require.NoError(t, err)
	require.NoError(t, blobs.SyncInode(20))
	require.Equal(t, uint64(3), blobs.SyncFileCount())
	require.Equal(t, uint64(2), blobs.SyncDirCount(), "已同步过的活跃 .pack 继续追加不得重复触发目录 fsync")

	require.NoError(t, blobs.RemovePack(1))
	require.False(t, blobs.Exists(1))
	require.True(t, blobs.Exists(2))
}

func TestBlobStoreCloseAndValidationErrorPaths(t *testing.T) {
	ctx := context.Background()
	blobs := store.NewBlobStore(t.TempDir())

	// 1. Append / AppendBlocks 边界与非法长度校验
	_, err := blobs.Append(ctx, 1, nil)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = blobs.Append(ctx, 1, make([]byte, store.BlobSize+1))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = blobs.AppendBlocks(ctx, 1, nil)
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = blobs.AppendBlocks(ctx, 1, make([]byte, store.BlobSize+1024))
	require.ErrorIs(t, err, store.ErrInvalidValue)

	ref, err := blobs.Append(ctx, 1, []byte("hello-pack"))
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

	_, err = blobs.Append(ctx, 1, []byte("after-close"))
	require.ErrorIs(t, err, store.ErrClosed)
	_, err = blobs.AppendBlocks(ctx, 1, make([]byte, store.BlobSize))
	require.ErrorIs(t, err, store.ErrClosed)
	_, _, err = blobs.ExtendTail(1, ref, ref.Length, []byte("!"), ref.Length+1)
	require.ErrorIs(t, err, store.ErrClosed)
}
