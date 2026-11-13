package store

import (
	"bytes"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func createAndWriteInternal(t *testing.T, actor *StoreActor, name string, fill byte, size int) Attribute {
	t.Helper()
	ctx := context.Background()
	attr, err := actor.Create(ctx, CreateReq{
		ParentIno: RootIno,
		Name:      []byte(name),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, attr.Ino, 0, bytes.Repeat([]byte{fill}, size))
	require.NoError(t, err)
	return attr
}

func gatherHistogramMapInternal(t *testing.T, reg *prometheus.Registry) map[string][]*dto.Metric {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	out := make(map[string][]*dto.Metric, len(fams))
	for _, fam := range fams {
		out[fam.GetName()] = fam.GetMetric()
	}
	return out
}

func findMetricByLabelInternal(metrics []*dto.Metric, labelName, labelVal string) *dto.Metric {
	for _, m := range metrics {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == labelName && lp.GetValue() == labelVal {
				return m
			}
		}
	}
	return nil
}

func TestCompactPacksPhaseACASFenceAgainstConcurrentOverwriteTruncateAndUnlink(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor1, err := Open(Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)

	_ = createAndWriteInternal(t, actor1, "dead.bin", 0xAA, BlobSize)
	fKeep := createAndWriteInternal(t, actor1, "keep.bin", 0xBB, 32768)
	fOverwrite := createAndWriteInternal(t, actor1, "overwrite.bin", 0xCC, 32768)
	fTruncate := createAndWriteInternal(t, actor1, "truncate.bin", 0xDD, 32768)
	fUnlink := createAndWriteInternal(t, actor1, "unlink.bin", 0xEE, 32768)
	require.Equal(t, uint64(1), actor1.blobs.activePackID())
	require.NoError(t, actor1.Close())

	newOverwritePayload := bytes.Repeat([]byte{0xFE}, 32768)
	var hookTriggered bool
	var actor2 *StoreActor
	actor2, err = Open(Options{
		Dir:      dir,
		DiskID:   "disk-compact-cas",
		MaxBytes: 128 << 20,
		beforeCompactRelocateHook: func() {
			hookTriggered = true
			_, _, hErr := actor2.Write(ctx, fOverwrite.Ino, 0, newOverwritePayload)
			require.NoError(t, hErr)

			_, hErr = actor2.SetAttr(ctx, fTruncate.Ino, SetAttrReq{Size: new(uint64(0))})
			require.NoError(t, hErr)

			hErr = actor2.Unlink(ctx, RootIno, []byte("unlink.bin"), false)
			require.NoError(t, hErr)
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor2.Close()) }()

	// 删除 dead.bin，使已封口 Pack 1 存活率降至 128 KiB / 384 KiB <= 50%
	require.NoError(t, actor2.Unlink(ctx, RootIno, []byte("dead.bin"), false))

	require.NoError(t, actor2.CompactPacks(ctx))
	require.True(t, hookTriggered, "必须在搬迁读取追加后、提交 opCompactRelocate 前触发并发写钩子")

	// 1. 源封口包 Pack 1 物理文件已彻底回收
	_, statErr := os.Stat(packFilePath(actor2.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)

	// 2. fKeep 成功搬迁至 Pack 2；fOverwrite 保持前台新写的 0xFE 而非旧 GC 块 0xCC
	gotKeep, err := actor2.Read(ctx, fKeep.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0xBB}, 32768), gotKeep)

	gotOverwrite, err := actor2.Read(ctx, fOverwrite.Ino, 0, 32768)
	require.NoError(t, err)
	require.Equal(t, newOverwritePayload, gotOverwrite)

	// 3. fTruncate 与 fUnlink 不得被后台 GC 恢复或复活
	truncAttr, err := actor2.GetAttr(ctx, fTruncate.Ino)
	require.NoError(t, err)
	require.Zero(t, truncAttr.Size)
	gotTrunc, err := actor2.Read(ctx, fTruncate.Ino, 0, 32768)
	require.NoError(t, err)
	require.Empty(t, gotTrunc)

	_, err = actor2.GetAttr(ctx, fUnlink.Ino)
	require.ErrorIs(t, err, ErrNotFound)

	// 4. 新包 Pack 2 的 LiveBytes 仅计入 fKeep + fOverwrite (64 KiB)，TotalBytes 包含 4 个预写块 + 1 个前台新写块 (160 KiB)
	snap := actor2.MetricsSnapshot()
	require.Equal(t, uint64(65536), snap.PackLiveBytes)
	require.Equal(t, uint64(5*32768), snap.PackPhysicalBytes)
}

func TestAsyncDeadPackReclaimNonBlockingFsyncAndDrainOnClose(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	removeEntered := make(chan uint64, 4)
	releaseRemove := make(chan struct{})
	var releaseOnce sync.Once

	actor, err := Open(Options{
		Dir:         dir,
		DiskID:      "disk-async-reclaim",
		MaxBytes:    64 << 20,
		MaxPackSize: 2 * BlobSize, // 将封包上限缩至 512 KiB，便于快速制造已封口死包
		removePackHook: func(packID uint64) {
			if packID == 1 {
				removeEntered <- packID
				<-releaseRemove
				return
			}
			time.Sleep(25 * time.Millisecond)
		},
	})
	require.NoError(t, err)

	f, err := actor.Create(ctx, CreateReq{
		ParentIno: RootIno,
		Name:      []byte("reclaim.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// Pack 1: 写入 512 KiB 填满封口；随后覆写同区间切入 Pack 2，此时 Pack 1 LiveBytes 归零成为待回收死包
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x11}, 2*BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x22}, 2*BlobSize))
	require.NoError(t, err)
	// 再追加写入切入 Pack 3，使 Pack 2 也封口，供后续验证 Close 排空
	_, _, err = actor.Write(ctx, f.Ino, 2*BlobSize, bytes.Repeat([]byte{0x33}, BlobSize))
	require.NoError(t, err)
	_, statErr := os.Stat(packFilePath(actor.blobs.dir, 1))
	require.NoError(t, statErr)

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

	// 确认后台回收协程已收到 Pack 1 并在钩子处挂起，放行后 waitPendingReclaims 确保物理文件被彻底移除
	select {
	case id := <-removeEntered:
		require.Equal(t, uint64(1), id)
	case <-time.After(2 * time.Second):
		releaseOnce.Do(func() { close(releaseRemove) })
		t.Fatal("后台死包回收协程未收到待删死包")
	}
	releaseOnce.Do(func() { close(releaseRemove) })
	actor.blobs.waitPendingReclaims()
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist, "waitPendingReclaims 返回后死包 1 必须已物理删除")

	// 2. 再使已封口的 Pack 2 变为死包，验证 Close() 必须排空后台回收队列后再返回
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x44}, 2*BlobSize))
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(ctx, f.Ino))
	require.NoError(t, actor.Close())
	_, statErr = os.Stat(packFilePath(actor.blobs.dir, 2))
	require.ErrorIs(t, statErr, os.ErrNotExist, "Close 返回前必须排空后台死包回收队列并删净 Pack 2")
}

func TestSyncBarrierParallelismWALTimingAndReadHistograms(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var (
		syncHookEnabled atomic.Bool
		syncHookCalls   atomic.Int32
		tickHookArmed   atomic.Bool
		tickOnce        sync.Once
		tickEntered     = make(chan struct{})
		releaseTick     = make(chan struct{})
	)

	actor, err := Open(Options{
		Dir:      dir,
		DiskID:   "disk-metrics-sync",
		MaxBytes: 64 << 20,
		Metrics:  metrics,
		syncFileHook: func(_ uint64) {
			if !syncHookEnabled.Load() {
				return
			}
			syncHookCalls.Add(1)
			time.Sleep(35 * time.Millisecond)
		},
		beforeTickHook: func() {
			if !tickHookArmed.Load() {
				return
			}
			tickOnce.Do(func() {
				close(tickEntered)
				<-releaseTick
			})
		},
	})
	require.NoError(t, err)

	dirAttr, err := actor.Mkdir(ctx, MkdirReq{
		ParentIno: RootIno,
		Name:      []byte("sub"),
		Mode:      S_IFDIR | 0o755,
	})
	require.NoError(t, err)
	f, err := actor.Create(ctx, CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("data.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)
	sym, err := actor.Symlink(ctx, SymlinkReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("ln"),
		Target:    []byte("data.bin"),
	})
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.k"), []byte("v"), 0))

	// 1. 验证纯 Fsync (syncBarrierWithoutBatch) 中 .pack fdatasync 在独立协程并发执行且正确采集耗时
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x31}, BlobSize))
	require.NoError(t, err)

	syncHookEnabled.Store(true)
	require.NoError(t, actor.Fsync(ctx, f.Ino))
	require.Equal(t, int32(1), syncHookCalls.Load())

	// 2. 验证合并批次 commitWithSyncBarrier 中 wal_apply_sync 记录完整墙钟耗时（不被并发 35ms .pack sync 扣减为零）
	f2, err := actor.Create(ctx, CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("merged.bin"),
		Mode:      S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f2.Ino, 0, bytes.Repeat([]byte{0x42}, BlobSize))
	require.NoError(t, err)

	var (
		wg   sync.WaitGroup
		mErr error
		cErr error
		fErr error
	)
	tickHookArmed.Store(true)

	wg.Go(func() {
		_, mErr = actor.Mkdir(ctx, MkdirReq{
			ParentIno: dirAttr.Ino,
			Name:      []byte("co-batch-dir"),
			Mode:      S_IFDIR | 0o755,
		})
	})
	<-tickEntered
	wg.Go(func() {
		// 与下一轮或排空合并：再投递一条带 batch 变更的写命令和一条 Fsync 使它们在同一 draining 批内合并
		_, cErr = actor.Mkdir(ctx, MkdirReq{
			ParentIno: dirAttr.Ino,
			Name:      []byte("co-batch-dir-2"),
			Mode:      S_IFDIR | 0o755,
		})
	})
	wg.Go(func() {
		fErr = actor.Fsync(ctx, f2.Ino)
	})
	time.Sleep(10 * time.Millisecond)
	close(releaseTick)
	wg.Wait()
	require.NoError(t, mErr)
	require.NoError(t, cErr)
	require.NoError(t, fErr)
	tickHookArmed.Store(false)
	syncHookEnabled.Store(false)

	// 3. 写入跨块数据后关闭重开以清空热缓存，触发全部 9 类只读操作、KVScan 与磁盘 .pack pread
	_, _, err = actor.Write(ctx, f.Ino, BlobSize, bytes.Repeat([]byte{0x32}, 4096))
	require.NoError(t, err)
	require.NoError(t, actor.Close())

	reopened, err := Open(Options{
		Dir:      dir,
		DiskID:   "disk-metrics-sync",
		MaxBytes: 64 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	_, _, err = reopened.Lookup(ctx, dirAttr.Ino, []byte("data.bin"))
	require.NoError(t, err)
	_, err = reopened.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	_, err = reopened.ReadDir(ctx, dirAttr.Ino, 0, 16)
	require.NoError(t, err)
	_, err = reopened.ReadDirPlus(ctx, dirAttr.Ino, 0, 16)
	require.NoError(t, err)
	_, err = reopened.Readlink(ctx, sym.Ino)
	require.NoError(t, err)
	_, err = reopened.GetXattr(ctx, f.Ino, []byte("user.k"))
	require.NoError(t, err)
	_, err = reopened.ListXattr(ctx, f.Ino)
	require.NoError(t, err)
	_, err = reopened.StatFS(ctx)
	require.NoError(t, err)

	_, err = reopened.Read(ctx, f.Ino, 0, BlobSize+4096)
	require.NoError(t, err)

	gathered := gatherHistogramMapInternal(t, reg)

	// 断言阶段拆分与 WAL Sync 完整墙钟计时
	phaseMetrics := gathered["bowl_storage_actor_tick_phase_duration_seconds"]
	require.NotNil(t, findMetricByLabelInternal(phaseMetrics, "phase", "phase_a_validate"))
	require.NotNil(t, findMetricByLabelInternal(phaseMetrics, "phase", "phase_b_mutate"))
	require.Nil(t, findMetricByLabelInternal(phaseMetrics, "phase", "validate_mutate"), "旧的合并标签 validate_mutate 必须彻底移除")

	walSyncMetric := findMetricByLabelInternal(phaseMetrics, "phase", "wal_apply_sync")
	require.NotNil(t, walSyncMetric)
	require.GreaterOrEqual(t, walSyncMetric.GetHistogram().GetSampleSum(), 0.030, "commitWithSyncBarrier 中 wal_apply_sync 必须记录从 ApplyNoSyncWait 到 SyncWait 的完整墙钟")

	// 断言全部 9 类只读操作直方图非零
	readOpMetrics := gathered["bowl_storage_read_op_duration_seconds"]
	for _, op := range []string{"read", "lookup", "getattr", "readdir", "readdirplus", "readlink", "getxattr", "listxattr", "statfs"} {
		m := findMetricByLabelInternal(readOpMetrics, "op", op)
		require.NotNil(t, m, "缺少 read_op 直方图标签 %s", op)
		require.GreaterOrEqual(t, m.GetHistogram().GetSampleCount(), uint64(1), "read_op=%s 计数必须 >= 1", op)
	}

	// 断言 KV 点查/扫描与 BlobPread 直方图非零
	kvMetrics := gathered["bowl_storage_kv_read_duration_seconds"]
	require.GreaterOrEqual(t, findMetricByLabelInternal(kvMetrics, "op", "point_get").GetHistogram().GetSampleCount(), uint64(1))
	require.GreaterOrEqual(t, findMetricByLabelInternal(kvMetrics, "op", "prefix_scan").GetHistogram().GetSampleCount(), uint64(1))

	preadMetrics := gathered["bowl_storage_blob_pread_duration_seconds"]
	require.Len(t, preadMetrics, 1)
	require.GreaterOrEqual(t, preadMetrics[0].GetHistogram().GetSampleCount(), uint64(1))
}

func TestBlobStoreExtendHotDoublingAndWritebackHints(t *testing.T) {
	ctx := context.Background()
	blobs := newBlobStore(t.TempDir(), PackMaxSize, nil, nil, nil, nil, nil)
	defer func() { require.NoError(t, blobs.close()) }()

	require.Zero(t, blobs.activePackID())

	// 1. 验证连续 63 次 4 KiB extendTail (4 KiB -> 256 KiB) 采用倍增扩容，堆分配次数远低于线性 63 次
	chunk := bytes.Repeat([]byte{0x5C}, 4096)
	var ref BlobRef
	allocs := testing.AllocsPerRun(5, func() {
		cur, appErr := blobs.append(ctx, 10, chunk)
		if appErr != nil {
			t.Fatalf("append failed: %v", appErr)
		}
		for step := 2; step <= 64; step++ {
			newLen := uint32(step * 4096)
			nextRef, ok, extErr := blobs.extendTail(10, cur, cur.Length, chunk, newLen)
			if extErr != nil || !ok {
				t.Fatalf("extendTail failed at step %d: ok=%v err=%v", step, ok, extErr)
			}
			cur = nextRef
		}
		ref = cur
	})
	require.Equal(t, uint64(1), blobs.activePackID())
	require.Equal(t, uint32(BlobSize), ref.Length)
	require.LessOrEqual(t, allocs, float64(12), "1 次 append + 63 次 4 KiB extendTail 倍增扩容总分配次数应 <= 12（消除 O(N^2) 重分配）")

	gotFull := make([]byte, ref.Length)
	require.NoError(t, blobs.readAtContext(ctx, ref, 0, gotFull))
	require.Equal(t, bytes.Repeat(chunk, 64), gotFull)

	// 2. 验证并发读取 activePackID 与跨 8 MiB 边界写入提示（写入 16 MiB 跨过 8 MiB 与 16 MiB 两个窗口）
	stopReader := make(chan struct{})
	errCh := make(chan uint64, 1)
	var readerWG sync.WaitGroup
	readerWG.Go(func() {
		for {
			select {
			case <-stopReader:
				return
			default:
				if id := blobs.activePackID(); id < 1 {
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
		_, err := blobs.appendBlocks(ctx, 10, buf4M)
		require.NoError(t, err)
	}
	close(stopReader)
	readerWG.Wait()
	select {
	case badID := <-errCh:
		t.Fatalf("并发读取 activePackID 返回非法值: %d", badID)
	default:
	}

	require.GreaterOrEqual(t, blobs.writebackHintCount.Load(), uint64(2), "顺序写入跨过 8 MiB 与 16 MiB 边界应触发至少 2 次异步回写提示")
}
