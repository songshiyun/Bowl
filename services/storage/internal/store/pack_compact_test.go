package store_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requirePackFileCount(t *testing.T, dir string, want int) []string {
	t.Helper()
	files := findPackFiles(t, dir)
	require.Lenf(t, files, want, "packs 目录 .pack 文件数量不符: want=%d got=%d files=%v", want, len(files), files)
	return files
}

func TestGivenMaxPackSizeOptionsWhenOpenedThenBoundsEnforced(t *testing.T) {
	// Given: 非法（小于 BlobSize 或大于 PackMaxSize）与合法阈值 MaxPackSize
	// When & Then: 超出允许范围返回 ErrInvalid，精确上下限可正常打开并关闭
	_, err := store.Open(store.Options{Dir: t.TempDir(), DiskID: "min-minus-1", MaxBytes: 64 << 20, MaxPackSize: store.BlobSize - 1})
	require.ErrorIsf(t, err, store.ErrInvalid, "MaxPackSize 小于 BlobSize 应返回 ErrInvalid: got=%v", err)

	_, err = store.Open(store.Options{Dir: t.TempDir(), DiskID: "max-plus-1", MaxBytes: 64 << 20, MaxPackSize: store.PackMaxSize + 1})
	require.ErrorIsf(t, err, store.ErrInvalid, "MaxPackSize 大于 PackMaxSize 应返回 ErrInvalid: got=%v", err)

	minActor := openTestActor(t, &store.Options{DiskID: "min-valid", MaxPackSize: store.BlobSize})
	require.NoError(t, minActor.Close())

	maxActor := openTestActor(t, &store.Options{DiskID: "max-valid", MaxPackSize: store.PackMaxSize})
	require.NoError(t, maxActor.Close())
}

func TestGivenPreexistingHighPackIDWhenOpenedThenNextPackAllocatesHigherID(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 先初始化实例并关闭，随后在 packs 目录放置编号 250 的已有 pack 文件
	initActor := openTestActor(t, &store.Options{Dir: dir, DiskID: "pack-id-recovery", MaxPackSize: 2 * store.BlobSize})
	require.NoError(t, initActor.Close())

	require.NoError(t, os.MkdirAll(filepath.Join(dir, "packs"), 0o755))
	preexisting := filepath.Join(dir, "packs", "0000000000000250.pack")
	require.NoError(t, os.WriteFile(preexisting, make([]byte, store.BlobSize), 0o600))

	// When: 重新打开并写入一个整块触发分配新活跃 pack
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "pack-id-recovery", MaxPackSize: 2 * store.BlobSize})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("f.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(store.BlobSize, 0x19))
	require.NoError(t, err)

	// Then: 新创建的 pack 编号必须严格高于 250（即 0000000000000251.pack）
	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000251.pack"))
	require.NoErrorf(t, err, "恢复后新建 pack 应从 251 起步: err=%v", err)
}

func TestGivenWritesRollingAcrossThreePacksWhenClosedAndReopenedThenSyncTargetsAndColdReadMatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 每个 pack 上限 2 块（512 KiB），写入 5 块跨越 3 个 pack
	actor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "rollover-3", MaxPackSize: 2 * store.BlobSize})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("roll.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	payload := makePatternBytes(5*store.BlobSize, 0x33)
	_, _, err = actor.Write(ctx, f.Ino, 0, payload)
	require.NoError(t, err)

	// When: 直接 Close 排空 2 个已封口 pack 的 bg_rotate 刷盘，随后重开追加 1 块并 Fsync 触发 fsync_barrier
	require.NoError(t, actor.Close())

	// Then: 存在 3 个 pack 文件，bg_rotate 至少同步 2 个封口 pack
	requirePackFileCount(t, dir, 3)
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{"target": "pack_data", "trigger": "bg_rotate"}), uint64(2), "2 个已封口 pack 应在 Close 排空前完成 bg_rotate fdatasync")

	reopened, reg2 := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "rollover-3", MaxPackSize: 2 * store.BlobSize})
	got, err := reopened.Read(ctx, f.Ino, 0, 5*store.BlobSize)
	require.NoError(t, err)
	require.Equalf(t, payload, got, "跨 3 个 pack 冷重开回读数据不符")

	fBarrier, err := reopened.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("barrier.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = reopened.Write(ctx, fBarrier.Ino, 0, makePatternBytes(store.BlobSize, 0x34))
	require.NoError(t, err)
	require.NoError(t, reopened.Fsync(ctx, fBarrier.Ino))
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg2, "bowl_storage_blob_sync_duration_seconds", map[string]string{"target": "packs_dir", "trigger": "fsync_barrier"}), uint64(1), "新活跃 pack Fsync 应触发 packs_dir fsync_barrier")
}

func TestGivenLowAndHighWatermarkPacksWhenCompactPacksThenLowWatermarkRelocatedAndHighKept(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 每个 pack 上限 4 块（1 MiB）；pack1 留 1/4（25%<=50% 低水位），pack2 留 3/4（75%>50% 高水位），pack3 活跃
	actor, reg := newMeteredActor(t, &store.Options{Dir: dir, DiskID: "compact-wm", MaxPackSize: 4 * store.BlobSize})

	fKeep1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("keep1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	keep1Data := makePatternBytes(store.BlobSize, 0x11)
	_, _, err = actor.Write(ctx, fKeep1.Ino, 0, keep1Data)
	require.NoError(t, err)

	fDrop1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("drop1.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, fDrop1.Ino, 0, makePatternBytes(3*store.BlobSize, 0x22))
	require.NoError(t, err)

	fKeep2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("keep2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	keep2Data := makePatternBytes(3*store.BlobSize, 0x33)
	_, _, err = actor.Write(ctx, fKeep2.Ino, 0, keep2Data)
	require.NoError(t, err)

	fDrop2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("drop2.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, fDrop2.Ino, 0, makePatternBytes(store.BlobSize, 0x44))
	require.NoError(t, err)

	// 再写入 1 块封口 pack2（开启 pack3）
	fAnchor, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("anchor.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, fAnchor.Ino, 0, makePatternBytes(store.BlobSize, 0x55))
	require.NoError(t, err)

	// 删除 drop1（pack1 降为 25% 活数据）与 drop2（pack2 降为 75% 活数据）
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("drop1.bin"), false))
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("drop2.bin"), false))
	require.NoError(t, actor.FlushWAL())

	beforeSnap := actor.MetricsSnapshot()
	require.Equalf(t, 1, beforeSnap.LowWatermarkPacks, "应仅有 pack1 处于低水位: got=%d", beforeSnap.LowWatermarkPacks)

	// When: 执行 CompactPacks
	require.NoError(t, actor.CompactPacks(ctx))

	// Then: 低水位 pack1 被搬迁并物理删除（0000000000000001.pack 消失），高水位 pack2 保留，WALDirty 清零，触发 flush_wal 同步
	afterSnap := actor.MetricsSnapshot()
	require.Equalf(t, 0, afterSnap.LowWatermarkPacks, "CompactPacks 后低水位 pack 数应归零")
	require.Lessf(t, afterSnap.PackPhysicalBytes, beforeSnap.PackPhysicalBytes, "CompactPacks 后 PackPhysicalBytes 应下降")
	require.Falsef(t, actor.WALDirty(), "CompactPacks 结束后 WALDirty 应为 false")
	require.GreaterOrEqualf(t, histogramSampleCount(t, reg, "bowl_storage_blob_sync_duration_seconds", map[string]string{"trigger": "flush_wal"}), uint64(1), "CompactPacks 回收旧 pack 前应执行 flush_wal 同步")

	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000001.pack"))
	require.ErrorIsf(t, err, os.ErrNotExist, "低水位 pack1 应已被删除: err=%v", err)
	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000002.pack"))
	require.NoErrorf(t, err, "高水位 pack2 不得被 CompactPacks 误删: err=%v", err)

	// 再次 CompactPacks 应为空操作，存活文件数据完全吻合
	require.NoError(t, actor.CompactPacks(ctx))
	got1, err := actor.Read(ctx, fKeep1.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, keep1Data, got1)
	got2, err := actor.Read(ctx, fKeep2.Ino, 0, 3*store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, keep2Data, got2)
}

func TestGivenActiveDeadPackAndCrashRecoveryWhenFlushedAndCleanedThenDeadPacksReclaimedSafely(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 创建文件填满 pack1（2 块=512 KiB）随后删除，此时 pack1 为唯一活跃 pack（LiveBytes==0 但不得被提前删）
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "dead-pack-lifecycle", MaxPackSize: 2 * store.BlobSize})
	f1, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("ephemeral.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f1.Ino, 0, makePatternBytes(2*store.BlobSize, 0x0A))
	require.NoError(t, err)
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("ephemeral.bin"), false))
	require.NoError(t, actor.FlushWAL())

	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000001.pack"))
	require.NoErrorf(t, err, "活跃 pack 即使 LiveBytes==0 也不得在封口前被删除: err=%v", err)

	// When: 写入 2 块封口 pack1 并滚动到 pack2，随后 FlushWAL
	f2, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("live.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f2.Ino, 0, makePatternBytes(2*store.BlobSize, 0x0B))
	require.NoError(t, err)
	require.NoError(t, actor.FlushWAL())

	// Then: 已封口的死包 pack1 在 FlushWAL 后被物理清理
	_, err = os.Stat(filepath.Join(dir, "packs", "0000000000000001.pack"))
	require.ErrorIsf(t, err, os.ErrNotExist, "封口死包 pack1 应在 FlushWAL 后被回收: err=%v", err)

	// When: 制造 openHandle=true 孤儿与未同步写操作后以 CloseDiscard 模拟掉电，再重开并执行 CleanOrphans + CompactPacks
	orphanFile, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("orphan.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, orphanFile.Ino, 0, makePatternBytes(2*store.BlobSize, 0x0C))
	require.NoError(t, err)
	require.NoError(t, actor.FlushWAL())
	require.NoError(t, actor.Unlink(ctx, store.RootIno, []byte("orphan.bin"), true))

	require.NoError(t, actor.CloseDiscard())

	recovered := openTestActor(t, &store.Options{Dir: dir, DiskID: "dead-pack-lifecycle", MaxPackSize: 2 * store.BlobSize})
	require.NoError(t, recovered.CleanOrphans(ctx))
	require.NoError(t, recovered.CompactPacks(ctx))

	// Then: live.bin 完整可读且无孤儿残留
	hasOrphans, err := recovered.HasOrphans()
	require.NoError(t, err)
	require.Falsef(t, hasOrphans, "恢复并 CleanOrphans 后不应残留孤儿")
	gotLive, err := recovered.Read(ctx, f2.Ino, 0, 2*store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, makePatternBytes(2*store.BlobSize, 0x0B), gotLive)
}

func TestGivenQuotaLimitsWhenPrewriteFailsOrMaxBytesUpdatedThenSpaceRecoveredAndPersisted(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()

	// Given: 4 块配额（1 MiB），pack 上限 2 块，写入 3 块后余量不足 2 块
	actor := openTestActor(t, &store.Options{Dir: dir, DiskID: "quota-pack", MaxBytes: 4 * store.BlobSize, MaxPackSize: 2 * store.BlobSize})
	f, err := actor.Create(ctx, store.CreateReq{ParentIno: store.RootIno, Name: []byte("q.bin"), Mode: store.S_IFREG | 0o644})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, makePatternBytes(3*store.BlobSize, 0x61))
	require.NoError(t, err)

	// When: 尝试写入 2 块超出配额，返回 ErrNoSpace 且 LogicalBytes 维持 3 块
	_, _, err = actor.Write(ctx, f.Ino, 3*store.BlobSize, makePatternBytes(2*store.BlobSize, 0x62))
	require.ErrorIsf(t, err, store.ErrNoSpace, "超额写入应返回 ErrNoSpace: got=%v", err)
	require.Equalf(t, uint64(3*store.BlobSize), actor.MetricsSnapshot().LogicalBytes, "失败写入不得泄露 LogicalBytes")

	// When: 尝试将 MaxBytes 缩容至低于已用 3 块返回 ErrQuotaShrinkBelowUsed；合法扩容至 8 块后可继续写入并持久化
	require.ErrorIsf(t, actor.UpdateMaxBytes(ctx, 2*store.BlobSize), store.ErrQuotaShrinkBelowUsed, "缩容低于已用容量应返回 ErrQuotaShrinkBelowUsed")
	require.NoError(t, actor.UpdateMaxBytes(ctx, 8*store.BlobSize))
	require.Equalf(t, uint64(8*store.BlobSize), actor.MaxBytes(), "UpdateMaxBytes 后 MaxBytes() 未更新")

	_, _, err = actor.Write(ctx, f.Ino, 3*store.BlobSize, makePatternBytes(2*store.BlobSize, 0x63))
	require.NoError(t, err)
	require.NoError(t, actor.Close())

	// Then: 重开携带 ResetEpoch:true 时允许合法缩容 MaxBytes（例如 6 块），同时校验持久化不丢失
	reopened := openTestActor(t, &store.Options{Dir: dir, DiskID: "quota-pack", MaxBytes: 6 * store.BlobSize, MaxPackSize: 2 * store.BlobSize, ResetEpoch: true})
	require.Equalf(t, uint64(6*store.BlobSize), reopened.MaxBytes(), "ResetEpoch 重开后有效缩容 MaxBytes 应生效")
}
