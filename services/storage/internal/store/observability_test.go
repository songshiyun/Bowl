package store_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Lines() []map[string]any {
	b.mu.Lock()
	raw := b.buf.String()
	b.mu.Unlock()

	var out []map[string]any
	for line := range strings.SplitSeq(strings.TrimSpace(raw), "\n") {
		if line == "" {
			continue
		}
		var m map[string]any
		if err := json.Unmarshal([]byte(line), &m); err == nil {
			out = append(out, m)
		}
	}
	return out
}

func jsonIntField(t *testing.T, entry map[string]any, key string) int64 {
	t.Helper()
	v, ok := entry[key].(float64)
	require.True(t, ok, "expected numeric JSON field %q, got %T", key, entry[key])
	return int64(v)
}

func newTestJSONLogger(level slog.Level) (*slog.Logger, *syncBuffer) {
	buf := &syncBuffer{}
	h := slog.NewJSONHandler(buf, &slog.HandlerOptions{Level: level})
	return slog.New(h), buf
}

func gatherMap(t *testing.T, g prometheus.Gatherer) map[string]*dto.MetricFamily {
	t.Helper()
	fams, err := g.Gather()
	require.NoError(t, err)
	out := make(map[string]*dto.MetricFamily, len(fams))
	for _, f := range fams {
		out[f.GetName()] = f
	}
	return out
}

func matchLabels(m *dto.Metric, want map[string]string) bool {
	if len(m.GetLabel()) != len(want) {
		return false
	}
	for _, lp := range m.GetLabel() {
		if v, ok := want[lp.GetName()]; !ok || v != lp.GetValue() {
			return false
		}
	}
	return true
}

func counterVal(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string) uint64 {
	t.Helper()
	fam, ok := fams[name]
	require.True(t, ok, "missing metric family %s", name)
	for _, m := range fam.GetMetric() {
		if matchLabels(m, labels) {
			require.NotNil(t, m.GetCounter())
			return uint64(m.GetCounter().GetValue())
		}
	}
	t.Fatalf("metric %s with labels %v not found", name, labels)
	return 0
}

func histCountAndSum(t *testing.T, fams map[string]*dto.MetricFamily, name string, labels map[string]string) (count uint64, sum float64) {
	t.Helper()
	fam, ok := fams[name]
	require.True(t, ok, "missing metric family %s", name)
	for _, m := range fam.GetMetric() {
		if matchLabels(m, labels) {
			require.NotNil(t, m.GetHistogram())
			return m.GetHistogram().GetSampleCount(), m.GetHistogram().GetSampleSum()
		}
	}
	t.Fatalf("histogram %s with labels %v not found", name, labels)
	return 0, 0
}

func TestMetricsNilSafetyPedanticRegistryAndZeroDiskIDLabels(t *testing.T) {
	// 1. nil *Metrics 与 nil *StoreActor 接收者安全性验证
	var nilMetrics *store.Metrics
	descCh := make(chan *prometheus.Desc, 16)
	metricCh := make(chan prometheus.Metric, 16)
	nilMetrics.Describe(descCh)
	nilMetrics.Collect(metricCh)
	require.Empty(t, descCh)
	require.Empty(t, metricCh)

	var nilActor *store.StoreActor
	require.Equal(t, store.ActorMetricsSnapshot{}, nilActor.MetricsSnapshot())

	// 2. PedanticRegistry 严格校验全部 17 项指标名称、桶定义与零 disk_id 标签污染
	m := store.NewMetrics()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(m))

	// 校验未污染全局 DefaultGatherer
	defaultFams, err := prometheus.DefaultGatherer.Gather()
	require.NoError(t, err)
	for _, fam := range defaultFams {
		require.False(t, strings.HasPrefix(fam.GetName(), "bowl_storage_"), "unexpected global metric %s", fam.GetName())
	}

	fams := gatherMap(t, reg)
	expectedNames := []string{
		"bowl_storage_actor_queue_wait_seconds",
		"bowl_storage_actor_tick_batch_size",
		"bowl_storage_actor_tick_phase_duration_seconds",
		"bowl_storage_inode_lock_wait_seconds",
		"bowl_storage_blob_write_blocks_total",
		"bowl_storage_blob_write_bytes_total",
		"bowl_storage_blob_rmw_read_duration_seconds",
		"bowl_storage_blob_mu_wait_seconds",
		"bowl_storage_blob_pwrite_duration_seconds",
		"bowl_storage_blob_sync_duration_seconds",
		"bowl_storage_blob_read_source_total",
		"bowl_storage_hot_cache_evictions_total",
		"bowl_storage_epoch_fenced_total",
		"bowl_storage_pebble_stall_events_total",
		"bowl_storage_pebble_disk_slow_total",
		"bowl_storage_bg_sync_errors_total",
		"bowl_storage_pack_remove_errors_total",
	}
	gotNames := make([]string, 0, len(fams))
	for name, fam := range fams {
		gotNames = append(gotNames, name)
		for _, metric := range fam.GetMetric() {
			for _, lp := range metric.GetLabel() {
				require.NotEqual(t, "disk_id", lp.GetName(), "high-frequency metric %s must not carry disk_id label", name)
			}
		}
	}
	slices.Sort(expectedNames)
	slices.Sort(gotNames)
	require.Equal(t, expectedNames, gotNames)

	// 校验 tick_batch_size 精确桶边界: 1,2,4,8,16,32,64,128,256
	batchFam := fams["bowl_storage_actor_tick_batch_size"]
	require.Len(t, batchFam.GetMetric(), 1)
	var gotBounds []float64
	for _, b := range batchFam.GetMetric()[0].GetHistogram().GetBucket() {
		gotBounds = append(gotBounds, b.GetUpperBound())
	}
	require.Equal(t, []float64{1, 2, 4, 8, 16, 32, 64, 128, 256}, gotBounds)
}

func TestObservabilityEndToEndPipelineAndSnapshot(t *testing.T) {
	m := store.NewMetrics()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(m))

	logger, logBuf := newTestJSONLogger(slog.LevelInfo)
	const (
		diskID    = "disk-obs-e2e"
		maxBytes  = uint64(64 << 20)
		maxInodes = uint64(2048)
	)

	actor, err := store.Open(store.Options{
		Dir:       t.TempDir(),
		DiskID:    diskID,
		MaxBytes:  maxBytes,
		MaxInodes: maxInodes,
		Logger:    logger,
		Metrics:   m,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// Step 1: AdvanceEpoch 抢占日志与限频 ErrStaleEpoch 计数校验
	epoch, _, err := actor.AdvanceEpoch()
	require.NoError(t, err)
	require.Equal(t, uint64(1), epoch)

	f1, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("full.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	})
	require.NoError(t, err)

	f2, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("partial.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	})
	require.NoError(t, err)

	// 连续触发 5 次旧 Epoch=0 拦截：计数精确为 5，日志因 1s 限频仅输出 1 条
	for range 5 {
		_, _, staleErr := actor.Write(f1.Ino, 0, []byte("stale"), 0)
		require.ErrorIs(t, staleErr, store.ErrStaleEpoch)
	}
	fams := gatherMap(t, reg)
	require.Equal(t, uint64(5), counterVal(t, fams, "bowl_storage_epoch_fenced_total", nil))

	var epochAdvLogs, fencedLogs int
	for _, entry := range logBuf.Lines() {
		switch entry["msg"] {
		case "writer epoch advanced":
			epochAdvLogs++
			require.Equal(t, diskID, entry["disk_id"])
			require.Equal(t, int64(0), jsonIntField(t, entry, "old_epoch"))
			require.Equal(t, int64(1), jsonIntField(t, entry, "new_epoch"))
		case "stale writer epoch fenced":
			fencedLogs++
			require.Equal(t, diskID, entry["disk_id"])
			require.Equal(t, int64(0), jsonIntField(t, entry, "req_epoch"))
			require.Equal(t, int64(1), jsonIntField(t, entry, "current_epoch"))
		}
	}
	require.Equal(t, 1, epochAdvLogs)
	require.Equal(t, 1, fencedLogs, "rate-limited stale epoch warning should emit once within 1s burst")

	// Step 2: 整块写 (full_block) / 尾顺延写 (extend_tail) / 覆写合并 (rmw) / 稀疏全零跳过 (zero_elide) / Fallocate 置零 (falloc_zero)
	fullData := bytes.Repeat([]byte{0xAB}, store.BlobSize)
	n, _, err := actor.Write(f1.Ino, 0, fullData, epoch)
	require.NoError(t, err)
	require.Equal(t, uint32(store.BlobSize), n)

	partA := bytes.Repeat([]byte{0x11}, 4096)
	partB := bytes.Repeat([]byte{0x22}, 4096)
	patchC := bytes.Repeat([]byte{0x33}, 1024)

	// 首次新块写入 4KiB + 紧邻尾部顺延扩展 4KiB（全程 0 RMW）
	_, _, err = actor.Write(f2.Ino, 0, partA, epoch)
	require.NoError(t, err)
	_, _, err = actor.Write(f2.Ino, 4096, partB, epoch)
	require.NoError(t, err)

	fams = gatherMap(t, reg)
	require.Equal(t, uint64(1), counterVal(t, fams, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "full_block"}))
	require.Equal(t, uint64(store.BlobSize), counterVal(t, fams, "bowl_storage_blob_write_bytes_total", map[string]string{"mode": "full_block"}))
	require.Equal(t, uint64(2), counterVal(t, fams, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "extend_tail"}))
	require.Equal(t, uint64(8192), counterVal(t, fams, "bowl_storage_blob_write_bytes_total", map[string]string{"mode": "extend_tail"}))
	require.Equal(t, uint64(0), counterVal(t, fams, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}))

	// 回写 offset=0 触发真实 RMW（预读 8192B 合并后追加 8192B）
	_, _, err = actor.Write(f2.Ino, 0, patchC, epoch)
	require.NoError(t, err)

	// 写入全零新块触发 zero_elide
	_, _, err = actor.Write(f2.Ino, store.BlobSize, make([]byte, 4096), epoch)
	require.NoError(t, err)

	// 对 f1 满块执行边界打洞触发 falloc_zero
	require.NoError(t, actor.Fallocate(f1.Ino, 0, 4096, store.FallocPunchHole|store.FallocKeepSize, epoch))

	fams = gatherMap(t, reg)
	require.Equal(t, uint64(1), counterVal(t, fams, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "rmw"}))
	require.Equal(t, uint64(8192), counterVal(t, fams, "bowl_storage_blob_write_bytes_total", map[string]string{"mode": "rmw"}))
	require.Equal(t, uint64(1), counterVal(t, fams, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "zero_elide"}))
	require.Equal(t, uint64(4096), counterVal(t, fams, "bowl_storage_blob_write_bytes_total", map[string]string{"mode": "zero_elide"}))
	require.Equal(t, uint64(1), counterVal(t, fams, "bowl_storage_blob_write_blocks_total", map[string]string{"mode": "falloc_zero"}))
	require.Equal(t, uint64(store.BlobSize), counterVal(t, fams, "bowl_storage_blob_write_bytes_total", map[string]string{"mode": "falloc_zero"}))

	rmwReadCnt, _ := histCountAndSum(t, fams, "bowl_storage_blob_rmw_read_duration_seconds", nil)
	require.Equal(t, uint64(2), rmwReadCnt, "1 from Write RMW + 1 from Fallocate partial punch")
	muWaitCnt, _ := histCountAndSum(t, fams, "bowl_storage_blob_mu_wait_seconds", nil)
	require.GreaterOrEqual(t, muWaitCnt, uint64(5))
	pwriteCnt, _ := histCountAndSum(t, fams, "bowl_storage_blob_pwrite_duration_seconds", nil)
	require.GreaterOrEqual(t, pwriteCnt, uint64(5))

	// Step 3: Read 热缓存命中 (hot_cache) vs 穿透磁盘 (disk_pack) 与环形容量淘汰 (hot_cache_evictions_total)
	wantF2 := make([]byte, 8192)
	copy(wantF2, partA)
	copy(wantF2[4096:], partB)
	copy(wantF2, patchC)

	beforeHot := counterVal(t, fams, "bowl_storage_blob_read_source_total", map[string]string{"source": "hot_cache"})
	beforeDisk := counterVal(t, fams, "bowl_storage_blob_read_source_total", map[string]string{"source": "disk_pack"})

	gotHot, err := actor.Read(f2.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, wantF2, gotHot)

	fams = gatherMap(t, reg)
	require.Equal(t, beforeHot+1, counterVal(t, fams, "bowl_storage_blob_read_source_total", map[string]string{"source": "hot_cache"}))
	require.Equal(t, beforeDisk, counterVal(t, fams, "bowl_storage_blob_read_source_total", map[string]string{"source": "disk_pack"}))

	// 驱逐当前活跃包中所有热块后再次读取，验证穿透至 disk_pack
	activePack := actor.Blobs().ActivePackID()
	for off := uint32(0); off < actor.Blobs().ActiveOffset(); off += 1024 {
		actor.Blobs().EvictHot(store.BlobRef{PackID: activePack, Offset: off, Length: 1024})
	}
	gotDisk, err := actor.Read(f2.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, wantF2, gotDisk)

	fams = gatherMap(t, reg)
	require.Equal(t, beforeDisk+1, counterVal(t, fams, "bowl_storage_blob_read_source_total", map[string]string{"source": "disk_pack"}))

	// 连续向同一 hotShard（offset < 256KiB，offset/BlobSize 恒同）写入 65 个独立短块触发槽位上限淘汰
	fEvict, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("evict.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	})
	require.NoError(t, err)
	smallPayload := bytes.Repeat([]byte{0x7E}, 512)
	for range 65 {
		// 每次覆写 offset=0 产生新的包内 offset 追加，65 次均落在同一 256KiB 区间分片
		_, _, err = actor.Write(fEvict.Ino, 0, smallPayload, epoch)
		require.NoError(t, err)
	}
	fams = gatherMap(t, reg)
	require.GreaterOrEqual(t, counterVal(t, fams, "bowl_storage_hot_cache_evictions_total", nil), uint64(1))

	// Step 4: Fsync 屏障 + 慢 Tick (>20ms) 日志 + 切包 bg_rotate + CompactPacks (flush_wal & dead_pack_remove)
	var slowHookOnce sync.Once
	actor.Blobs().SetSyncFileHookForTest(func(_ uint64) {
		slowHookOnce.Do(func() {
			time.Sleep(25 * time.Millisecond)
		})
	})
	require.NoError(t, actor.Fsync(f2.Ino, epoch))
	actor.Blobs().SetSyncFileHookForTest(nil)

	fams = gatherMap(t, reg)
	fsyncDataCnt, _ := histCountAndSum(t, fams, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "fsync_barrier",
	})
	fsyncDirCnt, _ := histCountAndSum(t, fams, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "packs_dir",
		"trigger": "fsync_barrier",
	})
	require.GreaterOrEqual(t, fsyncDataCnt, uint64(1))
	require.GreaterOrEqual(t, fsyncDirCnt, uint64(1))

	packPhaseCnt, _ := histCountAndSum(t, fams, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "pack_sync_barrier"})
	walSyncPhaseCnt, _ := histCountAndSum(t, fams, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "wal_apply_sync"})
	walNoSyncPhaseCnt, _ := histCountAndSum(t, fams, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "wal_apply_nosync"})
	valMutPhaseCnt, _ := histCountAndSum(t, fams, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{"phase": "validate_mutate"})
	require.GreaterOrEqual(t, packPhaseCnt, uint64(1))
	require.GreaterOrEqual(t, walSyncPhaseCnt, uint64(1))
	require.GreaterOrEqual(t, walNoSyncPhaseCnt, uint64(1))
	require.GreaterOrEqual(t, valMutPhaseCnt, uint64(1))

	// 验证慢 processTick (>20ms) 告警日志已输出
	var foundSlowTick bool
	for _, entry := range logBuf.Lines() {
		if entry["msg"] == "slow actor tick" {
			foundSlowTick = true
			require.Equal(t, diskID, entry["disk_id"])
			require.GreaterOrEqual(t, jsonIntField(t, entry, "total_ms"), int64(20))
		}
	}
	require.True(t, foundSlowTick, "expected slow actor tick warning log")

	// Step 5: 并发 Inode 锁等待与 MPSC 批次合并验证
	releaseTick := store.HoldTickForTest(actor)
	var wg sync.WaitGroup
	concErrCh := make(chan error, 8)
	for i := range 4 {
		idx := i
		wg.Go(func() {
			payload := fmt.Appendf(nil, "concurrent-%d", idx)
			if _, _, wErr := actor.Write(f2.Ino, 0, payload, epoch); wErr != nil {
				concErrCh <- wErr
			}
		})
	}
	for i := range 3 {
		idx := i
		wg.Go(func() {
			if _, mErr := actor.Mkdir(store.MkdirReq{
				ParentIno: store.RootIno,
				Name:      fmt.Appendf(nil, "dir-%d", idx),
				Mode:      store.S_IFDIR | 0o755,
				Epoch:     epoch,
			}); mErr != nil {
				concErrCh <- mErr
			}
		})
	}
	time.Sleep(15 * time.Millisecond)
	releaseTick()
	wg.Wait()
	close(concErrCh)
	for cErr := range concErrCh {
		require.NoError(t, cErr)
	}

	fams = gatherMap(t, reg)
	inoLockCnt, _ := histCountAndSum(t, fams, "bowl_storage_inode_lock_wait_seconds", nil)
	require.GreaterOrEqual(t, inoLockCnt, uint64(1))
	qWaitCnt, qWaitSum := histCountAndSum(t, fams, "bowl_storage_actor_queue_wait_seconds", nil)
	require.Positive(t, qWaitCnt)
	require.Positive(t, qWaitSum)

	// Step 6: 切包 bg_rotate、低水位快照 LowWatermarkPacks、死包回收与 MetricsSnapshot 精确核查
	actor.Blobs().SetMaxPackSize(2 * store.BlobSize) // 512 KiB 封口上限
	fRot, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("rotate.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	})
	require.NoError(t, err)

	// 写入 2 个满块填满一个 512KiB 封口包，再写 1 个满块切入新包触发后台 bgSyncLoop
	_, _, err = actor.Write(fRot.Ino, 0, bytes.Repeat([]byte{0x41}, 2*store.BlobSize), epoch)
	require.NoError(t, err)
	_, _, err = actor.Write(fRot.Ino, 2*store.BlobSize, bytes.Repeat([]byte{0x42}, store.BlobSize), epoch)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		cur := gatherMap(t, reg)
		cnt, _ := histCountAndSum(t, cur, "bowl_storage_blob_sync_duration_seconds", map[string]string{
			"target":  "pack_data",
			"trigger": "bg_rotate",
		})
		return cnt >= 1
	}, 2*time.Second, 10*time.Millisecond)

	// 覆写 fRot 第 0 块，使刚才 512KiB 已封口包的 LiveBytes 降为 256KiB (LiveBytes*2 == TotalBytes -> 低水位!)
	_, _, err = actor.Write(fRot.Ino, 0, bytes.Repeat([]byte{0x43}, store.BlobSize), epoch)
	require.NoError(t, err)

	snapBeforeCompact := actor.MetricsSnapshot()
	statFS, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, diskID, snapBeforeCompact.DiskID)
	require.False(t, snapBeforeCompact.ReadOnly)
	require.Equal(t, statFS.UsedBytes, snapBeforeCompact.LogicalBytes)
	require.Equal(t, statFS.Files, snapBeforeCompact.Inodes)
	require.Equal(t, maxBytes, snapBeforeCompact.MaxBytes)
	require.Equal(t, maxInodes, snapBeforeCompact.MaxInodes)
	require.Greater(t, snapBeforeCompact.PackPhysicalBytes, snapBeforeCompact.PackLiveBytes)
	require.Positive(t, snapBeforeCompact.PackLiveBytes)
	require.GreaterOrEqual(t, snapBeforeCompact.LowWatermarkPacks, 1)
	require.GreaterOrEqual(t, snapBeforeCompact.OpenPackFDs, 2)
	require.Positive(t, snapBeforeCompact.HotCacheBytes)
	require.Positive(t, snapBeforeCompact.HotCacheBlocks)
	require.False(t, snapBeforeCompact.PebbleStallActive)
	require.NotNil(t, snapBeforeCompact.Pebble)

	// 执行 CompactPacks 搬迁回收低水位包，触发 flush_wal 同步；并整文件 Unlink+Fsync 检验 Tick 内 dead_pack_remove
	require.NoError(t, actor.CompactPacks())
	snapAfterCompact := actor.MetricsSnapshot()
	require.Equal(t, 0, snapAfterCompact.LowWatermarkPacks)

	// 再写满一个独立封口包并立刻覆写使其在 Tick 内归零，随后通过 Fsync 触发 Tick 内 dead_pack_remove
	actor.Blobs().SetMaxPackSize(store.BlobSize)
	_, _, err = actor.Write(fRot.Ino, 0, bytes.Repeat([]byte{0x51}, store.BlobSize), epoch)
	require.NoError(t, err)
	_, _, err = actor.Write(fRot.Ino, 0, bytes.Repeat([]byte{0x52}, store.BlobSize), epoch)
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(fRot.Ino, epoch))

	fams = gatherMap(t, reg)
	flushWALSyncCnt, _ := histCountAndSum(t, fams, "bowl_storage_blob_sync_duration_seconds", map[string]string{
		"target":  "pack_data",
		"trigger": "flush_wal",
	})
	require.GreaterOrEqual(t, flushWALSyncCnt, uint64(1))
	deadRemCnt, _ := histCountAndSum(t, fams, "bowl_storage_actor_tick_phase_duration_seconds", map[string]string{
		"phase": "dead_pack_remove",
	})
	require.GreaterOrEqual(t, deadRemCnt, uint64(1))
}

func TestPebbleObservabilityAndSilentErrorFixes(t *testing.T) {
	// 1. 验证 slogPebbleLogger 分级映射（INFO 级别下 Infof 降噪为 0 条，DEBUG 级别下输出 DEBUG）
	infoLogger, infoBuf := newTestJSONLogger(slog.LevelInfo)
	exitCode := store.ExerciseSlogPebbleLoggerForTest(infoLogger, "disk-log-info")
	require.Equal(t, 1, exitCode)
	infoLines := infoBuf.Lines()
	require.Len(t, infoLines, 3, "Infof must be suppressed when logger level is INFO")
	require.Equal(t, "WARN", infoLines[0]["level"])
	require.Equal(t, "ERROR", infoLines[1]["level"])
	require.Equal(t, "ERROR", infoLines[2]["level"])
	require.Equal(t, true, infoLines[2]["fatal"])

	dbgLogger, dbgBuf := newTestJSONLogger(slog.LevelDebug)
	_ = store.ExerciseSlogPebbleLoggerForTest(dbgLogger, "disk-log-dbg")
	dbgLines := dbgBuf.Lines()
	require.Len(t, dbgLines, 4)
	require.Equal(t, "DEBUG", dbgLines[0]["level"])

	// 2. 端到端验证 vfs.WithDiskHealthChecks 激活 DiskSlow 回调 + Pebble WriteStall 回调
	m := store.NewMetrics()
	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(m))
	logger, logBuf := newTestJSONLogger(slog.LevelInfo)

	dir := t.TempDir()
	// 在启动对账前预置孤儿死包文件并将 packs 目录暂设为只读 0o555，使启动期 RemovePack 真实触发 EACCES 告警
	packsDir := filepath.Join(dir, "packs")
	require.NoError(t, os.MkdirAll(packsDir, 0o755))
	bogusStartupPack := filepath.Join(packsDir, "0000000000000099.pack")
	require.NoError(t, os.WriteFile(bogusStartupPack, []byte("orphan"), 0o644))
	require.NoError(t, os.Chmod(packsDir, 0o555))
	defer func() { _ = os.Chmod(packsDir, 0o755) }()

	actor, armSlowSync, err := store.OpenWithSlowPebbleSyncForTest(&store.Options{
		Dir:     dir,
		DiskID:  "disk-slow-err",
		Logger:  logger,
		Metrics: m,
	}, 10*time.Millisecond)
	require.NoError(t, os.Chmod(packsDir, 0o755))
	require.NoError(t, err)
	require.NoError(t, os.Remove(bogusStartupPack))
	defer func() {
		require.NoError(t, actor.Close())
	}()

	// 挂载单次 WAL Sync 阻塞栅栏：等待 vfs.WithDiskHealthChecks 后台协程采样到 in-flight 慢 I/O 后立即放行
	releaseSlowSync := armSlowSync()
	advErrCh := make(chan error, 1)
	go func() {
		_, _, advErr := actor.AdvanceEpoch()
		advErrCh <- advErr
	}()

	require.Eventually(t, func() bool {
		fams := gatherMap(t, reg)
		return counterVal(t, fams, "bowl_storage_pebble_disk_slow_total", nil) >= 1
	}, 3500*time.Millisecond, 20*time.Millisecond)
	releaseSlowSync()
	require.NoError(t, <-advErrCh)

	// 验证 WriteStallBegin / WriteStallEnd 更新快照状态与计数器
	actor.SimulatePebbleStallForTest(true, "L0 file count threshold exceeded")
	require.True(t, actor.MetricsSnapshot().PebbleStallActive)
	actor.SimulatePebbleStallForTest(false, "")
	require.False(t, actor.MetricsSnapshot().PebbleStallActive)

	// 3. 验证 bgSyncLoop syncPack 失败与运行期 removePacks 失败均不再静默吞错
	actor.Blobs().SetMaxPackSize(store.BlobSize)
	fAttr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("err.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     1,
	})
	require.NoError(t, err)

	// 在后台刷盘钩子中关闭被轮转封口包的缓存 FD，使 syncFileData 真实报错
	var injectBgErrOnce sync.Once
	actor.Blobs().SetSyncFileHookForTest(func(packID uint64) {
		injectBgErrOnce.Do(func() {
			actor.Blobs().CloseCachedPackFDForTest(packID)
		})
	})

	_, _, err = actor.Write(fAttr.Ino, 0, bytes.Repeat([]byte{0x61}, store.BlobSize), 1)
	require.NoError(t, err)
	firstPackID := actor.Blobs().ActivePackID()
	_, _, err = actor.Write(fAttr.Ino, store.BlobSize, bytes.Repeat([]byte{0x62}, store.BlobSize), 1)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		fams := gatherMap(t, reg)
		return counterVal(t, fams, "bowl_storage_bg_sync_errors_total", nil) >= 1
	}, 2*time.Second, 10*time.Millisecond)
	actor.Blobs().SetSyncFileHookForTest(nil)

	// 将 firstPackID 对应的物理文件替换为非空目录后覆写第 0 块 + FlushWAL，触发运行期 removePacks 报错
	firstPackPath := actor.Blobs().Path(firstPackID)
	actor.Blobs().CloseCachedPackFDForTest(firstPackID)
	require.NoError(t, os.Remove(firstPackPath))
	require.NoError(t, os.MkdirAll(firstPackPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(firstPackPath, "keep"), []byte("x"), 0o644))
	defer func() { _ = os.RemoveAll(firstPackPath) }()

	_, _, err = actor.Write(fAttr.Ino, 0, bytes.Repeat([]byte{0x63}, store.BlobSize), 1)
	require.NoError(t, err)
	require.NoError(t, actor.FlushWAL())

	fams := gatherMap(t, reg)
	require.GreaterOrEqual(t, counterVal(t, fams, "bowl_storage_pebble_stall_events_total", nil), uint64(1))
	require.GreaterOrEqual(t, counterVal(t, fams, "bowl_storage_bg_sync_errors_total", nil), uint64(1))
	require.GreaterOrEqual(t, counterVal(t, fams, "bowl_storage_pack_remove_errors_total", nil), uint64(2), "1 startup + 1 runtime remove error")

	var sawDiskSlow, sawBgSyncErr, sawStartupRemErr, sawRuntimeRemErr bool
	for _, entry := range logBuf.Lines() {
		switch entry["msg"] {
		case "pebble disk slow":
			sawDiskSlow = true
		case "background pack sync failed":
			sawBgSyncErr = true
		case "startup dead pack remove failed":
			sawStartupRemErr = true
		case "dead pack remove failed":
			sawRuntimeRemErr = true
		}
	}
	require.True(t, sawDiskSlow, "expected pebble disk slow log")
	require.True(t, sawBgSyncErr, "expected background pack sync failed log")
	require.True(t, sawStartupRemErr, "expected startup dead pack remove failed log")
	require.True(t, sawRuntimeRemErr, "expected runtime dead pack remove failed log")
}

func TestMetricsHotPathZeroAllocs(t *testing.T) {
	m := store.NewMetrics()
	allocs := testing.AllocsPerRun(200, func() {
		store.ExerciseMetricsZeroAllocForTest(m)
	})
	require.Zero(t, allocs, "Metrics hot-path instrumentation must incur 0 heap allocations")
}

func TestConcurrentOpenSnapshotAndCloseNoRaceOrGoroutineLeak(t *testing.T) {
	// 预热全局运行时背景协程后采样基线协程数
	runtime.GC()
	time.Sleep(30 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()

	m := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(m))
	logger := slog.New(slog.NewTextHandler(bytes.NewBuffer(nil), &slog.HandlerOptions{Level: slog.LevelError}))

	// 校验 Open 失败路径（只读打开不存在目录）同样正确关闭 fsCloser 不泄漏健康检测协程
	for i := range 5 {
		_, openErr := store.Open(store.Options{
			Dir:      filepath.Join(t.TempDir(), fmt.Sprintf("missing-%d", i)),
			DiskID:   fmt.Sprintf("missing-%d", i),
			ReadOnly: true,
			Logger:   logger,
			Metrics:  m,
		})
		require.Error(t, openErr)
	}

	const rounds = 6
	for r := range rounds {
		actor, err := store.Open(store.Options{
			Dir:      t.TempDir(),
			DiskID:   fmt.Sprintf("disk-race-%d", r),
			MaxBytes: 32 << 20,
			Logger:   logger,
			Metrics:  m,
		})
		require.NoError(t, err)

		fAttr, err := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte("f.dat"),
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, err)

		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		var wg sync.WaitGroup
		workerErrs := make(chan error, 64)

		// 并发写与 Fsync
		wg.Go(func() {
			payload := bytes.Repeat([]byte{0x5A}, 4096)
			for i := range 16 {
				if ctx.Err() != nil {
					return
				}
				_, _, wErr := actor.Write(fAttr.Ino, uint64(i)*4096, payload)
				if wErr != nil {
					if !errors.Is(wErr, store.ErrClosed) {
						workerErrs <- wErr
					}
					return
				}
			}
			if fsErr := actor.Fsync(fAttr.Ino); fsErr != nil && !errors.Is(fsErr, store.ErrClosed) {
				workerErrs <- fsErr
			}
		})

		// 并发拉取 MetricsSnapshot 与 Prometheus Gather
		for range 3 {
			wg.Go(func() {
				for range 20 {
					_ = actor.MetricsSnapshot()
					if _, gErr := reg.Gather(); gErr != nil {
						workerErrs <- gErr
						return
					}
				}
			})
		}

		// 在并发访问中途执行 Close，检验与 MetricsSnapshot 的关闭竞态守卫
		time.Sleep(5 * time.Millisecond)
		require.NoError(t, actor.Close())
		wg.Wait()
		cancel()
		close(workerErrs)
		for wErr := range workerErrs {
			require.NoError(t, wErr)
		}

		// 关闭后再次调用 MetricsSnapshot 必须安全且 Pebble 指针为 nil
		closedSnap := actor.MetricsSnapshot()
		require.Equal(t, fmt.Sprintf("disk-race-%d", r), closedSnap.DiskID)
		require.Nil(t, closedSnap.Pebble)
	}

	require.Eventually(t, func() bool {
		runtime.GC()
		return runtime.NumGoroutine() <= baseGoroutines+2
	}, 3*time.Second, 20*time.Millisecond, "detected goroutine leak after concurrent Open/Close cycles (before=%d, after=%d)", baseGoroutines, runtime.NumGoroutine())
}
