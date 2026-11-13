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
		Dir:         dir,
		DiskID:      "disk-compact-basic",
		MaxBytes:    128 << 20,
		MaxPackSize: 2 * store.BlobSize,
	})
	require.NoError(t, err)

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
	_, statErr := os.Stat(packFilePathForTest(dir, 1))
	require.NoError(t, statErr)
	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.NoError(t, statErr)
	require.NoError(t, actor1.Close())

	// 3. 重新打开使 Pack 1 与 Pack 2 均封口，随后删除 f2.bin：
	//    - Pack 1: LiveBytes = 256 KiB, TotalBytes = 512 KiB (50% <= 50%, 命中搬迁)
	//    - Pack 2: LiveBytes = 384 KiB, TotalBytes = 512 KiB (75% > 50%, 不搬迁)
	metrics := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	actor2, err := store.Open(store.Options{
		Dir:         dir,
		DiskID:      "disk-compact-basic",
		MaxBytes:    128 << 20,
		MaxPackSize: 2 * store.BlobSize,
		Metrics:     metrics,
	})
	require.NoError(t, err)
	require.NoError(t, actor2.Unlink(ctx, store.RootIno, []byte("f2.bin"), false))

	snapBefore := actor2.MetricsSnapshot()
	require.Equal(t, 1, snapBefore.LowWatermarkPacks)
	require.Equal(t, uint64(2*store.BlobSize+store.BlobSize/2), snapBefore.PackLiveBytes)
	require.Equal(t, uint64(4*store.BlobSize), snapBefore.PackPhysicalBytes)

	syncFilesBefore := gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "flush_wal",
	})

	// 4. 执行 CompactPacks：仅搬迁 Pack 1 至新包 Pack 3，并在新包 fdatasync + Pebble WAL Sync 后删除旧包
	require.NoError(t, actor2.CompactPacks(ctx))

	_, statErr = os.Stat(packFilePathForTest(dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist, "低水位源封口包 Pack 1 必须在搬迁与双重持久化屏障后被物理删除")

	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.NoError(t, statErr, "高水位封口包 Pack 2 (75% > 50%) 不应被搬迁删除")

	info3, statErr := os.Stat(packFilePathForTest(dir, 3))
	require.NoError(t, statErr, "搬迁目标新包 Pack 3 必须存在")
	require.Equal(t, int64(store.BlobSize), info3.Size())

	syncFilesAfter := gatherHistogramCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "flush_wal",
	})
	require.Greater(t, syncFilesAfter, syncFilesBefore, "删除旧包前必须先对目标新 .pack 执行 fdatasync")
	require.False(t, actor2.WALDirty(), "CompactPacks 完成后 Pebble WAL 不得残留未 Sync 状态")

	snapAfter := actor2.MetricsSnapshot()
	require.Zero(t, snapAfter.LowWatermarkPacks)
	require.Equal(t, uint64(2*store.BlobSize+store.BlobSize/2), snapAfter.PackLiveBytes)
	require.Equal(t, uint64(3*store.BlobSize), snapAfter.PackPhysicalBytes)
	require.NoError(t, actor2.Close())

	// 关闭重开清空热缓存，穿透直读磁盘 .pack 验证搬迁前后全文件字节一致
	actor3, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-compact-basic",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor3.Close()) }()

	gotF1, err := actor3.Read(ctx, f1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, f1Block0, gotF1)

	gotF3, err := actor3.Read(ctx, f3.Ino, 0, store.BlobSize+store.BlobSize/2)
	require.NoError(t, err)
	require.Equal(t, bytes.Join([][]byte{f3Block0, f3Block1}, nil), gotF3)
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
	// Pack 1 的 LiveBytes 保持 32 KiB，TotalBytes 推进至 288 KiB 记录预写死空间
	_, _, err = actor1.Write(ctx, f1.Ino, store.BlobSize, bytes.Repeat([]byte{0xEE}, store.BlobSize))
	require.ErrorIs(t, err, store.ErrNoSpace)
	snap1 := actor1.MetricsSnapshot()
	require.Equal(t, uint64(32*1024), snap1.PackLiveBytes)
	require.Equal(t, uint64(32*1024+store.BlobSize), snap1.PackPhysicalBytes)
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
	_, statErr := os.Stat(packFilePathForTest(dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.NoError(t, statErr)

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
	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.ErrorIs(t, statErr, os.ErrNotExist)
	snap3 := actor3.MetricsSnapshot()
	require.Zero(t, snap3.PackLiveBytes)
	require.Zero(t, snap3.PackPhysicalBytes)
}

func TestDeadSealedPackSoftUpdatesCrashRecoveryAndZeroBlockElision(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	actor1, err := store.Open(store.Options{
		Dir:         dir,
		DiskID:      "disk-soft-updates",
		MaxBytes:    128 << 20,
		MaxPackSize: store.BlobSize,
		Metrics:     metrics,
	})
	require.NoError(t, err)

	// 1. 256 KiB 块网格全零残块省略不变量：
	// 写入 64 KiB 非零块（Pack 1），对前半 32 KiB 打洞后截断至 16 KiB 使残块全零 -> 直接删除 DataKey 且不向活跃包追加全零废块
	fZero := createAndWriteFile(t, actor1, "zero-elide.bin", 0x55, 64*1024)
	require.NoError(t, actor1.Fallocate(ctx, fZero.Ino, 0, 32*1024, store.FallocPunchHole|store.FallocKeepSize))
	infoBeforeTrunc, statErr := os.Stat(packFilePathForTest(dir, 1))
	require.NoError(t, statErr)
	zeroElideBefore := gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "zero_elide"})

	_, err = actor1.SetAttr(ctx, fZero.Ino, store.SetAttrReq{Size: new(uint64(16 * 1024))})
	require.NoError(t, err)
	zeroElideAfter := gatherCounterMetricValue(t, reg, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "zero_elide"})
	require.Equal(t, zeroElideBefore+1, zeroElideAfter)

	infoAfterTrunc, statErr := os.Stat(packFilePathForTest(dir, 1))
	require.NoError(t, statErr)
	require.Equal(t, infoBeforeTrunc.Size(), infoAfterTrunc.Size(), "全零边界残块截断不得向活跃 .pack 追加写入")

	gotZero, err := actor1.Read(ctx, fZero.Ino, 0, 16*1024)
	require.NoError(t, err)
	require.Equal(t, make([]byte, 16*1024), gotZero)

	// 活跃包保护：此时 Pack 1 的 LiveBytes == 0，但作为当前活跃包绝不提前删除物理文件
	require.NoError(t, actor1.FlushWAL())
	_, statErr = os.Stat(packFilePathForTest(dir, 1))
	require.NoError(t, statErr, "当前活跃 .pack 即使 LiveBytes == 0 也不得提前删除")

	// 2. Soft Updates 死包回收屏障顺序：
	// 写入 256 KiB 满块触发滚动至 Pack 2：已封口的全死包 Pack 1 同批删除 sys:pack:1，但 WAL Sync 前物理文件仍保留在磁盘上
	f1 := createAndWriteFile(t, actor1, "f1.bin", 0xA1, store.BlobSize)
	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.NoError(t, statErr)
	_, statErr = os.Stat(packFilePathForTest(dir, 1))
	require.NoError(t, statErr, "NoSync WAL 落盘前不得提前删除死包物理文件")
	require.NoError(t, actor1.FlushWAL())
	_, statErr = os.Stat(packFilePathForTest(dir, 1))
	require.ErrorIs(t, statErr, os.ErrNotExist, "FlushWAL 落盘确认后死包 000001.pack 必须被物理删除")

	// 3. 模拟崩溃退出后的残留死包与孤儿节点恢复：
	// 写入 fOrphan (Pack 3) 并转为孤儿节点 (openHandle=true)，随后将 f1 (Pack 2) 整块打洞归零，不刷盘直接 CloseDiscard 模拟崩溃
	fOrphan := createAndWriteFile(t, actor1, "orphan.bin", 0xB2, store.BlobSize)
	_, statErr = os.Stat(packFilePathForTest(dir, 3))
	require.NoError(t, statErr)
	require.NoError(t, actor1.Unlink(ctx, store.RootIno, []byte("orphan.bin"), true))
	require.NoError(t, actor1.Fallocate(ctx, f1.Ino, 0, store.BlobSize, store.FallocPunchHole|store.FallocKeepSize))
	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.NoError(t, statErr, "崩溃前未刷 WAL，死包 000002.pack 仍残留在磁盘上")
	require.NoError(t, actor1.CloseDiscard())

	// 重新 Open：启动期 syncPackStatsWithBlobs 自动清理无存活引用的残留死包 Pack 2，保留孤儿引用的 Pack 3
	actor2, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-soft-updates",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor2.Close()) }()

	_, statErr = os.Stat(packFilePathForTest(dir, 2))
	require.ErrorIs(t, statErr, os.ErrNotExist, "Open 恢复时必须自动物理清理无 sys:pack 存活引用的残留死包")
	_, statErr = os.Stat(packFilePathForTest(dir, 3))
	require.NoError(t, statErr, "仍被孤儿节点引用的封口包必须保留")

	// 执行 CleanOrphans 批量回收残留孤儿节点：清理 Orphan/Inode/Data，扣减 Pack 3 至 0 并在 FlushWAL 后物理回收 Pack 3
	require.NoError(t, actor2.CleanOrphans(ctx))
	_, err = actor2.GetAttr(ctx, fOrphan.Ino)
	require.ErrorIs(t, err, store.ErrNotFound)
	st, err := actor2.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(3), st.Files)

	require.NoError(t, actor2.FlushWAL())
	_, statErr = os.Stat(packFilePathForTest(dir, 3))
	require.ErrorIs(t, statErr, os.ErrNotExist)
}
