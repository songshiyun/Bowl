package store_test

import (
	"bytes"
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestReadMultiBlocksCoalescesContiguousColdPreads(t *testing.T) {
	ctx := context.Background()
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-coalesce-pread",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("seq4m.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 构造 4 MiB (16 × 256 KiB) 确定性非对称载荷并一次性通过 AppendBlocks 连续写入同一 .pack
	const totalSize = 16 * store.BlobSize
	want := make([]byte, totalSize)
	for blk := range 16 {
		fill := bytes.Repeat([]byte{byte(0x10 + blk), byte(0xA0 + blk)}, store.BlobSize/2)
		copy(want[blk*store.BlobSize:(blk+1)*store.BlobSize], fill)
	}
	_, _, err = actor.Write(ctx, f.Ino, 0, want)
	require.NoError(t, err)

	// 1. 4 MiB 对齐满块冷读：16 个逻辑块物理紧邻，底层仅触发 1 次 pread
	beforePreads := actor.Blobs().PreadCount()
	got, err := actor.Read(ctx, f.Ino, 0, totalSize)
	require.NoError(t, err)
	require.Equal(t, want, got)
	require.Equal(t, uint64(1), actor.Blobs().PreadCount()-beforePreads, "16 个连续冷块应合并为单次 pread")

	// 2. 非对齐子区间跨 4 块冷读 [100 KiB, 900 KiB)：底层同样仅触发 1 次合并 pread
	const subOff = 100 * 1024
	const subLen = 800 * 1024
	beforeSub := actor.Blobs().PreadCount()
	gotSub, err := actor.Read(ctx, f.Ino, subOff, subLen)
	require.NoError(t, err)
	require.Equal(t, want[subOff:subOff+subLen], gotSub)
	require.Equal(t, uint64(1), actor.Blobs().PreadCount()-beforeSub, "非对齐起止的连续满块子区间读应合并为单次 pread")
}

func TestReadMultiBlocksHotShardPriorityMixedWithColdRuns(t *testing.T) {
	ctx := context.Background()
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-hot-cold-mix",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("mixed.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 先写入 8 个连续冷满块（Block 0..7，共 2 MiB；AppendBlocks 跳过 hotShard，直接落盘）
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

	// 对中间 Block 3 执行 4 KiB 局部覆写：新块通过 Append 写入并驻留 hotShard
	patchOff := uint64(3*store.BlobSize + 4096)
	patch := bytes.Repeat([]byte{0xEE, 0x77}, 2048)
	_, _, err = actor.Write(ctx, f.Ino, patchOff, patch)
	require.NoError(t, err)
	copy(want[patchOff:int(patchOff)+len(patch)], patch)

	// 白盒反事实污染：将 Block 3 新 BlobRef 对应的磁盘物理字节改写为陈旧毒化值 0x00，
	// 证明读取必须优先命中 hotShard 内存最新数据而绝不误读磁盘字节。
	hotRef3 := requireDataRef(t, actor, f.Ino, 3)
	packFile, err := os.OpenFile(actor.Blobs().Path(hotRef3.PackID), os.O_RDWR, 0o644)
	require.NoError(t, err)
	_, err = packFile.WriteAt(make([]byte, len(patch)), int64(hotRef3.Offset)+4096)
	require.NoError(t, err)
	require.NoError(t, packFile.Close())

	beforePreads := actor.Blobs().PreadCount()
	got, err := actor.Read(ctx, f.Ino, 0, totalSize)
	require.NoError(t, err)
	require.Equal(t, want, got, "跨 hotShard 热块与冷磁盘块混合读取必须优先返回 hotShard 最新数据")
	// Block 0..2 连续冷段合并为 1 次 pread，Block 3 命中 hotShard (0 pread)，Block 4..7 连续冷段合并为 1 次 pread
	require.Equal(t, uint64(2), actor.Blobs().PreadCount()-beforePreads, "热块两侧的连续冷区间应各自合并为 1 次 pread（总计 2 次）")
}

func TestReadMultiBlocksSparseHolesAndContextCancellation(t *testing.T) {
	ctx := context.Background()
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-holes-cancel",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("holes.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 布局（6 × 256 KiB = 1.5 MiB）：
	// Block 0: 未分配空洞
	// Block 1..2: 两个连续冷满块（512 KiB）
	// Block 3: 仅前 64 KiB 有效数据 + 后 192 KiB 块内尾部空洞（逐出热缓存以走冷读）
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
	actor.Blobs().EvictHot(requireDataRef(t, actor, f.Ino, 3))

	seg5 := bytes.Repeat([]byte{0xD5}, store.BlobSize)
	_, _, err = actor.Write(ctx, f.Ino, 5*store.BlobSize, seg5)
	require.NoError(t, err)
	copy(want[5*store.BlobSize:6*store.BlobSize], seg5)

	dirtyDst := bytes.Repeat([]byte{0xFF}, totalSize)
	beforePreads := actor.Blobs().PreadCount()
	got, err := actor.ReadInto(ctx, f.Ino, 0, totalSize, dirtyDst[:0])
	require.NoError(t, err)
	require.Equal(t, want, got, "夹杂整块空洞与块尾空洞的合并冷读必须精确清零空洞且数据无损")
	// Block 1..2 + Block 3 前 64K 若物理紧邻可合并为 1 次 pread，Block 5 为 1 次 pread（共 <= 3 次，远少于 4 次逐块读）
	require.LessOrEqual(t, actor.Blobs().PreadCount()-beforePreads, uint64(3))

	// 验证 Context 取消在单块读与多块读中均立即传播且不触发任何底层磁盘 pread
	cancelCtx, cancel := context.WithCancel(ctx)
	cancel()

	preadsBeforeCancel := actor.Blobs().PreadCount()
	_, err = actor.Read(cancelCtx, f.Ino, 1*store.BlobSize, store.BlobSize)
	require.ErrorIs(t, err, context.Canceled)
	_, err = actor.Read(cancelCtx, f.Ino, 0, totalSize)
	require.ErrorIs(t, err, context.Canceled)
	require.Equal(t, preadsBeforeCancel, actor.Blobs().PreadCount(), "已取消 Context 的 Read 不得发起底层磁盘 pread")
}

func TestAsyncDeadPackReclaimNonBlockingFsyncAndDrainOnClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-async-reclaim",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	// 将封包上限缩至 512 KiB，便于快速制造已封口死包
	actor.Blobs().SetMaxPackSize(2 * store.BlobSize)

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("reclaim.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// Pack 1: 写入 512 KiB 填满封口；随后覆写同区间切入 Pack 2，此时 Pack 1 LiveBytes 归零成为待回收死包
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x11}, 2*store.BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x22}, 2*store.BlobSize))
	require.NoError(t, err)
	// 再追加写入切入 Pack 3，使 Pack 2 也封口，供后续验证 Close 排空
	_, _, err = actor.Write(ctx, f.Ino, 2*store.BlobSize, bytes.Repeat([]byte{0x33}, store.BlobSize))
	require.NoError(t, err)
	require.True(t, actor.Blobs().Exists(1))

	removeEntered := make(chan uint64, 4)
	releaseRemove := make(chan struct{})
	var releaseOnce sync.Once
	actor.Blobs().SetRemovePackHookForTest(func(packID uint64) {
		removeEntered <- packID
		<-releaseRemove
	})

	// 1. 前台 Fsync 在 Pebble WAL Sync 完成后必须立即返回，绝不被慢速死包物理删除阻塞
	fsyncDone := make(chan error, 1)
	go func() {
		fsyncDone <- actor.Fsync(ctx, f.Ino)
	}()

	select {
	case fsyncErr := <-fsyncDone:
		require.NoError(t, fsyncErr)
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(releaseRemove) })
		t.Fatal("前台 Fsync 被后台死包删除同步阻塞")
	}

	// 确认后台回收协程已收到 Pack 1 并在钩子处挂起，放行后 WaitPendingReclaims 确保物理文件被彻底移除
	select {
	case id := <-removeEntered:
		require.Equal(t, uint64(1), id)
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(releaseRemove) })
		t.Fatal("后台死包回收协程未收到待删死包")
	}
	releaseOnce.Do(func() { close(releaseRemove) })
	actor.Blobs().WaitPendingReclaims()
	require.False(t, actor.Blobs().Exists(1), "WaitPendingReclaims 返回后死包 1 必须已物理删除")

	// 2. 再使已封口的 Pack 2 变为死包，挂载短暂延迟钩子验证 Close() 必须排空后台回收队列后再返回
	actor.Blobs().SetRemovePackHookForTest(func(_ uint64) {
		time.Sleep(25 * time.Millisecond)
	})
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x44}, 2*store.BlobSize))
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(ctx, f.Ino))
	require.NoError(t, actor.Close())
	require.False(t, actor.Blobs().Exists(2), "Close 返回前必须排空后台死包回收队列并删净 Pack 2")
}

func TestActivePackIDLockFreeWritebackHintsAndExtendHotDoubling(t *testing.T) {
	ctx := context.Background()
	blobs := store.NewBlobStore(t.TempDir())
	defer func() { require.NoError(t, blobs.Close()) }()

	require.Zero(t, blobs.ActivePackID())

	// 1. 验证连续 63 次 4 KiB ExtendTail (4 KiB -> 256 KiB) 采用倍增扩容，堆分配次数远低于线性 63 次
	chunk := bytes.Repeat([]byte{0x5C}, 4096)
	var ref store.BlobRef
	allocs := testing.AllocsPerRun(5, func() {
		cur, appErr := blobs.Append(ctx, 10, chunk)
		if appErr != nil {
			t.Fatalf("Append failed: %v", appErr)
		}
		for step := 2; step <= 64; step++ {
			newLen := uint32(step * 4096)
			nextRef, ok, extErr := blobs.ExtendTail(10, cur, cur.Length, chunk, newLen)
			if extErr != nil || !ok {
				t.Fatalf("ExtendTail failed at step %d: ok=%v err=%v", step, ok, extErr)
			}
			cur = nextRef
		}
		ref = cur
	})
	require.Equal(t, uint64(1), blobs.ActivePackID())
	require.Equal(t, uint32(store.BlobSize), ref.Length)
	require.LessOrEqual(t, allocs, float64(12), "1 次 Append + 63 次 4 KiB ExtendTail 倍增扩容总分配次数应 <= 12（消除 O(N^2) 重分配）")

	gotFull, err := blobs.Read(ref)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat(chunk, 64), gotFull)

	// 2. 验证并发读取 ActivePackID 与跨 8 MiB 边界写入提示（写入 16 MiB 跨过 8 MiB 与 16 MiB 两个窗口）
	stopReader := make(chan struct{})
	errCh := make(chan uint64, 1)
	var readerWG sync.WaitGroup
	readerWG.Go(func() {
		for {
			select {
			case <-stopReader:
				return
			default:
				if id := blobs.ActivePackID(); id < 1 {
					select {
					case errCh <- id:
					default:
					}
					return
				}
			}
		}
	})

	buf4M := bytes.Repeat([]byte{0xA7}, 4<<20)
	for range 4 {
		_, err := blobs.AppendBlocks(ctx, 10, buf4M)
		require.NoError(t, err)
	}
	close(stopReader)
	readerWG.Wait()
	select {
	case badID := <-errCh:
		t.Fatalf("并发读取 ActivePackID 返回非法值: %d", badID)
	default:
	}

	require.GreaterOrEqual(t, blobs.WritebackHintCount(), uint64(2), "顺序写入跨过 8 MiB 与 16 MiB 边界应触发至少 2 次异步回写提示")
}
