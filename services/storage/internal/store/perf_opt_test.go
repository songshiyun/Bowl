package store_test

import (
	"bytes"
	"context"
	"os"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestReadMultiBlocksCoalescesContiguousColdPreads(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := store.NewMetrics()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-coalesce-pread",
		MaxBytes: 128 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("seq4m.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 构造 4 MiB (16 × 256 KiB) 确定性非对称载荷并一次性通过批量追加写入同一 .pack
	const totalSize = 16 * store.BlobSize
	want := make([]byte, totalSize)
	for blk := range 16 {
		fill := bytes.Repeat([]byte{byte(0x10 + blk), byte(0xA0 + blk)}, store.BlobSize/2)
		copy(want[blk*store.BlobSize:(blk+1)*store.BlobSize], fill)
	}
	_, _, err = actor.Write(ctx, f.Ino, 0, want)
	require.NoError(t, err)

	// 1. 4 MiB 对齐满块冷读：16 个逻辑块物理紧邻，底层仅触发 1 次 pread
	beforePreads := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	got, err := actor.Read(ctx, f.Ino, 0, totalSize)
	require.NoError(t, err)
	require.Equal(t, want, got)
	afterPreads := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	require.Equal(t, uint64(1), afterPreads-beforePreads, "16 个连续冷块应合并为单次 pread")

	// 2. 非对齐子区间跨 4 块冷读 [100 KiB, 900 KiB)：底层同样仅触发 1 次合并 pread
	const subOff = 100 * 1024
	const subLen = 800 * 1024
	beforeSub := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	gotSub, err := actor.Read(ctx, f.Ino, subOff, subLen)
	require.NoError(t, err)
	require.Equal(t, want[subOff:subOff+subLen], gotSub)
	afterSub := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	require.Equal(t, uint64(1), afterSub-beforeSub, "非对齐起止的连续满块子区间读应合并为单次 pread")
}

func TestReadMultiBlocksHotShardPriorityMixedWithColdRuns(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	metrics := store.NewMetrics()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-hot-cold-mix",
		MaxBytes: 128 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("mixed.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 先写入 8 个连续冷满块（Block 0..7，共 2 MiB；批量整块写跳过 hotShard，直接落盘到 Pack 1 [0..8*BlobSize)）
	const totalBlocks = 8
	const totalSize = totalBlocks * store.BlobSize
	want := make([]byte, totalSize)
	for blk := range totalBlocks {
		for i := range store.BlobSize {
			want[blk*store.BlobSize+i] = byte((blk+1)*17 + (i % 251))
		}
	}
	_, _, err = actor.Write(ctx, f.Ino, 0, want)
	require.NoError(t, err)

	// 对中间 Block 3 执行 4 KiB 局部覆写：RMW 产生的新块追加至 Pack 1 偏移 8*BlobSize 并驻留 hotShard
	patchOff := uint64(3*store.BlobSize + 4096)
	patch := bytes.Repeat([]byte{0xEE, 0x77}, 2048)
	_, _, err = actor.Write(ctx, f.Ino, patchOff, patch)
	require.NoError(t, err)
	copy(want[patchOff:int(patchOff)+len(patch)], patch)

	// 反事实污染：将 Block 3 新块对应的磁盘物理字节（Pack 1 偏移 8*BlobSize+4096）改写为 0x00，
	// 证明读取必须优先命中 hotShard 内存最新数据而绝不误读磁盘字节。
	packPath := packFilePathForTest(dir, 1)
	st, err := os.Stat(packPath)
	require.NoError(t, err)
	require.Equal(t, int64(9*store.BlobSize), st.Size())
	packFile, err := os.OpenFile(packPath, os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = packFile.WriteAt(make([]byte, len(patch)), int64(8*store.BlobSize+4096))
	require.NoError(t, err)
	require.NoError(t, packFile.Close())

	beforePreads := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	beforeHot := gatherCounterMetricValue(t, reg, "bowl_storage_blob_read_source_total", map[string]string{"source": "hot_cache"})
	got, err := actor.Read(ctx, f.Ino, 0, totalSize)
	require.NoError(t, err)
	require.Equal(t, want, got, "跨 hotShard 热块与冷磁盘块混合读取必须优先返回 hotShard 最新数据")
	afterPreads := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	afterHot := gatherCounterMetricValue(t, reg, "bowl_storage_blob_read_source_total", map[string]string{"source": "hot_cache"})
	// Block 0..2 连续冷段合并为 1 次 pread，Block 3 命中 hotShard (0 pread)，Block 4..7 连续冷段合并为 1 次 pread
	require.Equal(t, uint64(1), afterHot-beforeHot, "Block 3 应命中 1 次 hot_cache")
	require.Equal(t, uint64(2), afterPreads-beforePreads, "热块两侧的连续冷区间应各自合并为 1 次 pread（总计 2 次）")
}

func TestReadMultiBlocksSparseHolesAndContextCancellation(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	reg := prometheus.NewRegistry()
	metrics := store.NewMetrics()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-holes-cancel",
		MaxBytes: 128 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("holes.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 布局（6 × 256 KiB = 1.5 MiB）：
	// Block 0: 未分配空洞
	// Block 1..2: 两个连续冷满块（512 KiB）
	// Block 3: 仅前 64 KiB 有效数据 + 后 192 KiB 块内尾部空洞（通过 Close+Reopen 清空热缓存走冷读）
	// Block 4: 未分配空洞
	// Block 5: 满块冷数据
	const totalSize = 6 * store.BlobSize
	want := make([]byte, totalSize)

	seg12 := bytes.Repeat([]byte{0xB1, 0xB2}, store.BlobSize)
	_, _, err = actor.Write(ctx, f.Ino, 1*store.BlobSize, seg12)
	require.NoError(t, err)
	copy(want[1*store.BlobSize:3*store.BlobSize], seg12)

	seg3 := bytes.Repeat([]byte{0xC3}, 64*1024)
	_, _, err = actor.Write(ctx, f.Ino, 3*store.BlobSize, seg3)
	require.NoError(t, err)
	copy(want[3*store.BlobSize:3*store.BlobSize+len(seg3)], seg3)

	seg5 := bytes.Repeat([]byte{0xD5}, store.BlobSize)
	_, _, err = actor.Write(ctx, f.Ino, 5*store.BlobSize, seg5)
	require.NoError(t, err)
	copy(want[5*store.BlobSize:6*store.BlobSize], seg5)

	require.NoError(t, actor.Close())

	reopened, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-holes-cancel",
		MaxBytes: 128 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	dirtyDst := bytes.Repeat([]byte{0xFF}, totalSize)
	beforePreads := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	got, err := reopened.ReadInto(ctx, f.Ino, 0, totalSize, dirtyDst[:0])
	require.NoError(t, err)
	require.Equal(t, want, got, "夹杂整块空洞与块尾空洞的合并冷读必须精确清零空洞且数据无损")
	afterPreads := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	// Block 1..2 + Block 3 前 64K 若物理紧邻可合并为 1 次 pread，Block 5 为 1 次 pread（共 <= 3 次，远少于 4 次逐块读）
	require.LessOrEqual(t, afterPreads-beforePreads, uint64(3))

	// 验证 Context 取消在单块读与多块读中均立即传播且不触发任何底层磁盘 pread
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	preadsBeforeCancel := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	_, err = reopened.Read(cancelCtx, f.Ino, 1*store.BlobSize, store.BlobSize)
	require.ErrorIs(t, err, context.Canceled)
	_, err = reopened.Read(cancelCtx, f.Ino, 0, totalSize)
	require.ErrorIs(t, err, context.Canceled)
	preadsAfterCancel := gatherHistogramCount(t, reg, "bowl_storage_blob_pread_duration_seconds", nil)
	require.Equal(t, preadsBeforeCancel, preadsAfterCancel, "已取消 Context 的 Read 不得发起底层磁盘 pread")
}

func TestStoreExtendTailWriteModeAndZeroAllocReadInto(t *testing.T) {
	ctx := context.Background()
	reg := prometheus.NewRegistry()
	metrics := store.NewMetrics()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-extend-tail-readinto",
		MaxBytes: 128 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("stream.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 连续 64 次 4 KiB 顺序追加写满 1 个 256 KiB 块，应全部走 extend_tail（0 次 rmw）。
	chunk := bytes.Repeat([]byte{0x5C}, 4096)
	for step := range 64 {
		_, _, writeErr := actor.Write(ctx, f.Ino, uint64(step*4096), chunk)
		require.NoError(t, writeErr)
	}
	extendCount := gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "extend_tail"})
	rmwCount := gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"})
	require.Equal(t, uint64(64), extendCount, "连续 64 次 4 KiB 顺序写必须全部命中 extend_tail")
	require.Zero(t, rmwCount, "连续顺序追加写不得触发任何 rmw 回读")

	// 验证复用调用方缓冲区的 ReadInto（单块热读路径）零载荷拷贝且堆分配保持 O(1) 有界小常数。
	dst := make([]byte, store.BlobSize)
	var got []byte
	allocs := testing.AllocsPerRun(50, func() {
		var readErr error
		got, readErr = actor.ReadInto(ctx, f.Ino, 0, store.BlobSize, dst[:0])
		if readErr != nil || len(got) != store.BlobSize {
			t.Fatalf("ReadInto failed: len=%d err=%v", len(got), readErr)
		}
	})
	require.LessOrEqual(t, allocs, float64(4), "复用 dst 缓冲区的单块 ReadInto 必须保持 O(1) 有界分配（无 256 KiB 载荷重分配）")
	require.Same(t, &dst[0], &got[0], "ReadInto 必须原地复用调用方 dst 底层存储")
}
