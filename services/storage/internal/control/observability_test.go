package control_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability"
	"github.com/starsrc/Aether/pkg/observability/obsfx"
	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type syncLogBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncLogBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncLogBuffer) Entries(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := b.buf.String()
	b.mu.Unlock()

	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var entries []map[string]any
	for line := range strings.SplitSeq(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		dec := json.NewDecoder(strings.NewReader(line))
		dec.UseNumber()
		var entry map[string]any
		require.NoError(t, dec.Decode(&entry), "解析 JSON 日志失败: %s", line)
		entries = append(entries, entry)
	}
	return entries
}

func gatherMap(t *testing.T, g prometheus.Gatherer) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	out := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				require.NotEqual(t, "disk_id", lp.GetName(),
					"指标 %s 严禁携带高基数 disk_id 标签", mf.GetName())
			}
		}
		out[mf.GetName()] = mf
	}
	return out
}

func metricGaugeValue(t *testing.T, mfs map[string]*dto.MetricFamily, name string, labels ...string) float64 {
	t.Helper()
	mf, ok := mfs[name]
	require.True(t, ok, "未找到 Gauge 指标 %s", name)
	m := findMetricWithLabels(t, mf, labels...)
	require.NotNil(t, m.GetGauge(), "指标 %s 不是 Gauge", name)
	return m.GetGauge().GetValue()
}

func metricCounterValue(t *testing.T, mfs map[string]*dto.MetricFamily, name string, labels ...string) float64 {
	t.Helper()
	mf, ok := mfs[name]
	require.True(t, ok, "未找到 Counter 指标 %s", name)
	m := findMetricWithLabels(t, mf, labels...)
	require.NotNil(t, m.GetCounter(), "指标 %s 不是 Counter", name)
	return m.GetCounter().GetValue()
}

func metricHistogram(t *testing.T, mfs map[string]*dto.MetricFamily, name string, labels ...string) *dto.Histogram {
	t.Helper()
	mf, ok := mfs[name]
	require.True(t, ok, "未找到 Histogram 指标 %s", name)
	m := findMetricWithLabels(t, mf, labels...)
	require.NotNil(t, m.GetHistogram(), "指标 %s 不是 Histogram", name)
	return m.GetHistogram()
}

func findMetricWithLabels(t *testing.T, mf *dto.MetricFamily, kvs ...string) *dto.Metric {
	t.Helper()
	require.Zero(t, len(kvs)%2, "标签键值必须成对传入")
	for _, m := range mf.GetMetric() {
		if len(kvs) == 0 && len(m.GetLabel()) == 0 {
			return m
		}
		if len(m.GetLabel()) != len(kvs)/2 {
			continue
		}
		matched := true
		for i := 0; i < len(kvs); i += 2 {
			key, val := kvs[i], kvs[i+1]
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == key && lp.GetValue() == val {
					found = true
					break
				}
			}
			if !found {
				matched = false
				break
			}
		}
		if matched {
			return m
		}
	}
	t.Fatalf("指标 %s 未找到标签匹配项 %v", mf.GetName(), kvs)
	return nil
}

func TestObservability_PedanticRegistryAndZeroDiskIDLabel(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "ctrl_obs_pedantic")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	storeMetrics := store.NewMetrics()
	mgr := control.New(
		dbStore,
		control.WithStorageRoot(root),
		control.WithStoreMetrics(storeMetrics),
	)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, mgr.Close())
		}
	})

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(storeMetrics, mgr.Collector())

	// 1. 零活跃盘空态抓取校验
	emptyMFs := gatherMap(t, reg)
	require.Zero(t, metricGaugeValue(t, emptyMFs, "bowl_storage_disks_loaded", "mode", "rw"))
	require.Zero(t, metricGaugeValue(t, emptyMFs, "bowl_storage_disks_loaded", "mode", "ro"))
	require.Zero(t, metricGaugeValue(t, emptyMFs, "bowl_storage_logical_bytes"))

	// 2. 创建并写入磁盘后全量指标家族与描述符一致性校验
	disk, err := mgr.CreateDisk(ctx, control.CreateDiskParams{
		Name:     "pedantic-disk",
		MaxBytes: 16 << 20,
	})
	require.NoError(t, err)
	mountRes, err := mgr.MountDisk(ctx, control.MountDiskParams{
		DiskID:   disk.DiskID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)

	fAttr, err := actor.Create(store.CreateReq{
		ParentIno: control.RootIno,
		Name:      []byte("sample.bin"),
		Mode:      0o644,
		Epoch:     mountRes.Epoch,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(fAttr.Ino, 0, []byte("hello-pedantic"), mountRes.Epoch)
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(fAttr.Ino, mountRes.Epoch))

	activeMFs := gatherMap(t, reg)

	expectedEngineMetrics := []string{
		"bowl_storage_disks_loaded",
		"bowl_storage_logical_bytes",
		"bowl_storage_inodes",
		"bowl_storage_pack_physical_bytes",
		"bowl_storage_pack_live_bytes",
		"bowl_storage_pack_low_watermark_files",
		"bowl_storage_pack_pending_dead_files",
		"bowl_storage_pack_open_fds",
		"bowl_storage_hot_cache_bytes",
		"bowl_storage_hot_cache_blocks",
		"bowl_storage_pebble_stall_active",
		"bowl_storage_pebble_wal_bytes_in_total",
		"bowl_storage_pebble_wal_bytes_written_total",
		"bowl_storage_pebble_wal_fsync_duration_seconds",
		"bowl_storage_pebble_l0_sublevels",
		"bowl_storage_pebble_l0_tables",
		"bowl_storage_pebble_read_amp",
		"bowl_storage_pebble_compaction_debt_bytes",
		"bowl_storage_pebble_compactions_in_progress",
		"bowl_storage_pebble_flushes_in_progress",
		"bowl_storage_pebble_filter_hits_total",
		"bowl_storage_pebble_filter_misses_total",
		"bowl_storage_pebble_open_table_iters",
		"bowl_storage_pebble_block_cache_bytes",
		"bowl_storage_pebble_block_cache_count",
		"bowl_storage_pebble_block_cache_hits_total",
		"bowl_storage_pebble_block_cache_misses_total",
		"bowl_storage_control_actor_loads_total",
		"bowl_storage_control_actor_evictions_total",
		"bowl_storage_control_cleanup_errors_total",
	}
	for _, name := range expectedEngineMetrics {
		_, ok := activeMFs[name]
		require.True(t, ok, "PedanticRegistry 缺少引擎/控制面指标 %s", name)
	}

	// 3. Manager 关闭后再次抓取不得 panic 或产生描述符冲突
	require.NoError(t, mgr.Close())
	closed = true

	closedMFs := gatherMap(t, reg)
	require.Zero(t, metricGaugeValue(t, closedMFs, "bowl_storage_disks_loaded", "mode", "rw"))
	require.Zero(t, metricGaugeValue(t, closedMFs, "bowl_storage_pebble_block_cache_bytes"))
}

func TestObservability_MultiDiskSharedCacheNoDoubleCountAndSnapshotAggregation(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "ctrl_obs_shared_cache")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	storeMetrics := store.NewMetrics()
	mgr := control.New(
		dbStore,
		control.WithStorageRoot(root),
		control.WithStoreMetrics(storeMetrics),
	)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(storeMetrics, mgr.Collector())

	// 创建两块独立虚拟磁盘并挂载可写世代号
	diskA, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "disk-a", MaxBytes: 64 << 20})
	require.NoError(t, err)
	diskB, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "disk-b", MaxBytes: 64 << 20})
	require.NoError(t, err)

	mountA, err := mgr.MountDisk(ctx, control.MountDiskParams{DiskID: diskA.DiskID, ReadOnly: false})
	require.NoError(t, err)
	mountB, err := mgr.MountDisk(ctx, control.MountDiskParams{DiskID: diskB.DiskID, ReadOnly: false})
	require.NoError(t, err)

	actorA, err := mgr.Actor(ctx, diskA.DiskID)
	require.NoError(t, err)
	actorB, err := mgr.Actor(ctx, diskB.DiskID)
	require.NoError(t, err)

	// 盘 A：创建 2 个文件，分别写入 4096 B 与 8192 B（逻辑字节 12288 B，根 + 2 文件 = 3 inodes）
	fA1, err := actorA.Create(store.CreateReq{ParentIno: control.RootIno, Name: []byte("a1.dat"), Mode: 0o644, Epoch: mountA.Epoch})
	require.NoError(t, err)
	fA2, err := actorA.Create(store.CreateReq{ParentIno: control.RootIno, Name: []byte("a2.dat"), Mode: 0o644, Epoch: mountA.Epoch})
	require.NoError(t, err)

	payloadA1 := bytes.Repeat([]byte("A"), 4096)
	payloadA2 := bytes.Repeat([]byte("B"), 8192)
	_, _, err = actorA.Write(fA1.Ino, 0, payloadA1, mountA.Epoch)
	require.NoError(t, err)
	_, _, err = actorA.Write(fA2.Ino, 0, payloadA2, mountA.Epoch)
	require.NoError(t, err)
	require.NoError(t, actorA.Fsync(fA1.Ino, mountA.Epoch))

	// 盘 B：创建 1 个文件，写入 256 KiB 整块 + 1024 B 尾扩展（逻辑字节 263168 B，根 + 1 文件 = 2 inodes）
	fB1, err := actorB.Create(store.CreateReq{ParentIno: control.RootIno, Name: []byte("b1.dat"), Mode: 0o644, Epoch: mountB.Epoch})
	require.NoError(t, err)
	payloadBFull := bytes.Repeat([]byte("C"), 256<<10)
	payloadBTail := bytes.Repeat([]byte("D"), 1024)
	_, _, err = actorB.Write(fB1.Ino, 0, payloadBFull, mountB.Epoch)
	require.NoError(t, err)
	_, _, err = actorB.Write(fB1.Ino, uint64(len(payloadBFull)), payloadBTail, mountB.Epoch)
	require.NoError(t, err)
	require.NoError(t, actorB.Fsync(fB1.Ino, mountB.Epoch))

	// 将两盘 MemTable 刷入 L0 SSTable 并执行读取，使 SSTable 块真实驻留共享 64 MiB pebble.Cache（杜绝 0 == 0 假绿）
	require.NoError(t, actorA.DB().Flush())
	require.NoError(t, actorB.DB().Flush())

	readA1, err := actorA.Read(fA1.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, payloadA1, readA1)
	_, _, err = actorA.Lookup(control.RootIno, []byte("a2.dat"))
	require.NoError(t, err)

	readB1, err := actorB.Read(fB1.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, payloadBFull[:4096], readB1)
	_, _, err = actorB.Lookup(control.RootIno, []byte("b1.dat"))
	require.NoError(t, err)

	cacheMetrics := control.SharedCacheMetrics(mgr)
	require.Positive(t, cacheMetrics.Size, "SSTable Flush + 读取后共享 BlockCache 驻留字节必须 > 0")
	require.Positive(t, cacheMetrics.Count, "SSTable Flush + 读取后共享 BlockCache 块数必须 > 0")

	// 验证底层每盘 db.Metrics().BlockCache 均返回全局共享值（若跨盘误求和将翻 2 倍）
	require.Equal(t, cacheMetrics.Size, actorA.DB().Metrics().BlockCache.Size)
	require.Equal(t, cacheMetrics.Size, actorB.DB().Metrics().BlockCache.Size)

	mfs := gatherMap(t, reg)

	// 1. 断言共享 BlockCache 指标精确等于单次 m.cache.Metrics()（绝非 2 倍）
	require.InDelta(t, float64(cacheMetrics.Size), metricGaugeValue(t, mfs, "bowl_storage_pebble_block_cache_bytes"), 1e-9)
	require.InDelta(t, float64(cacheMetrics.Count), metricGaugeValue(t, mfs, "bowl_storage_pebble_block_cache_count"), 1e-9)
	require.InDelta(t, float64(cacheMetrics.Hits), metricCounterValue(t, mfs, "bowl_storage_pebble_block_cache_hits_total"), 1e-9)
	require.InDelta(t, float64(cacheMetrics.Misses), metricCounterValue(t, mfs, "bowl_storage_pebble_block_cache_misses_total"), 1e-9)

	// 2. 断言独立参考模型计算的跨盘快照聚合值
	const expectedLogicalBytes = float64(4096 + 8192 + (256 << 10) + 1024)
	const expectedInodes = float64(3 + 2)
	require.InDelta(t, 2.0, metricGaugeValue(t, mfs, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	require.Zero(t, metricGaugeValue(t, mfs, "bowl_storage_disks_loaded", "mode", "ro"))
	require.InDelta(t, expectedLogicalBytes, metricGaugeValue(t, mfs, "bowl_storage_logical_bytes"), 1e-9)
	require.InDelta(t, expectedInodes, metricGaugeValue(t, mfs, "bowl_storage_inodes"), 1e-9)
	require.GreaterOrEqual(t, metricGaugeValue(t, mfs, "bowl_storage_pack_physical_bytes"), expectedLogicalBytes)
	require.InDelta(t, expectedLogicalBytes, metricGaugeValue(t, mfs, "bowl_storage_pack_live_bytes"), 1e-9)
	require.GreaterOrEqual(t, metricGaugeValue(t, mfs, "bowl_storage_pack_open_fds"), 2.0)
	require.Positive(t, metricGaugeValue(t, mfs, "bowl_storage_hot_cache_bytes"))
	require.Positive(t, metricGaugeValue(t, mfs, "bowl_storage_hot_cache_blocks"))

	// 3. 断言 Pebble WAL 指标与 FsyncLatency 直方图（累积桶 + 秒级 sum + count）
	require.Positive(t, metricCounterValue(t, mfs, "bowl_storage_pebble_wal_bytes_in_total"))
	require.Positive(t, metricCounterValue(t, mfs, "bowl_storage_pebble_wal_bytes_written_total"))
	require.GreaterOrEqual(t, metricGaugeValue(t, mfs, "bowl_storage_pebble_l0_tables"), 2.0)
	require.GreaterOrEqual(t, metricGaugeValue(t, mfs, "bowl_storage_pebble_l0_sublevels"), 1.0)
	require.GreaterOrEqual(t, metricGaugeValue(t, mfs, "bowl_storage_pebble_read_amp"), 1.0)

	fsyncHist := metricHistogram(t, mfs, "bowl_storage_pebble_wal_fsync_duration_seconds")
	require.GreaterOrEqual(t, fsyncHist.GetSampleCount(), uint64(2), "两盘各自执行 Fsync + AdvanceEpoch 后刷盘次数必须 >= 2")
	require.Positive(t, fsyncHist.GetSampleSum(), "WAL fsync 秒级累积耗时必须 > 0")
	require.Less(t, fsyncHist.GetSampleSum(), 30.0, "WAL fsync sum 必须已换算为秒而非纳秒")
	var prevCum uint64
	for _, b := range fsyncHist.GetBucket() {
		require.GreaterOrEqual(t, b.GetCumulativeCount(), prevCum, "直方图累积桶必须单调非递减")
		prevCum = b.GetCumulativeCount()
	}
	require.LessOrEqual(t, prevCum, fsyncHist.GetSampleCount())

	// 4. 断言共享 store.Metrics 热路径计数器与直方图准确累加两块盘的事件
	writeFullBlocks := metricCounterValue(t, mfs, "bowl_storage_blob_write_blocks_total", "mode", "full_block")
	writeExtendTail := metricCounterValue(t, mfs, "bowl_storage_blob_write_blocks_total", "mode", "extend_tail")
	require.InDelta(t, 1.0, writeFullBlocks, 1e-9)
	require.GreaterOrEqual(t, writeExtendTail, 1.0)
	totalWrittenBytes := metricCounterValue(t, mfs, "bowl_storage_blob_write_bytes_total", "mode", "full_block") +
		metricCounterValue(t, mfs, "bowl_storage_blob_write_bytes_total", "mode", "extend_tail") +
		metricCounterValue(t, mfs, "bowl_storage_blob_write_bytes_total", "mode", "rmw")
	require.InDelta(t, expectedLogicalBytes, totalWrittenBytes, 1e-9)

	tickHist := metricHistogram(t, mfs, "bowl_storage_actor_tick_batch_size")
	require.GreaterOrEqual(t, tickHist.GetSampleCount(), uint64(6))
	require.InDelta(t, 2.0, metricCounterValue(t, mfs, "bowl_storage_control_actor_loads_total", "mode", "rw"), 1e-9)

	// 5. 驱逐盘 A 后以只读模式重新注入，验证 rw=1 / ro=1 模式维度统计
	evicted, err := control.EvictIdleActorForTest(ctx, mgr, diskA.DiskID)
	require.NoError(t, err)
	require.True(t, evicted)
	_, err = control.InjectReadOnlyActorForTest(ctx, mgr, diskA.DiskID)
	require.NoError(t, err)

	mfsAfterRO := gatherMap(t, reg)
	require.InDelta(t, 1.0, metricGaugeValue(t, mfsAfterRO, "bowl_storage_disks_loaded", "mode", "rw"), 1e-9)
	require.InDelta(t, 1.0, metricGaugeValue(t, mfsAfterRO, "bowl_storage_disks_loaded", "mode", "ro"), 1e-9)
	require.InDelta(t, 1.0, metricCounterValue(t, mfsAfterRO, "bowl_storage_control_actor_loads_total", "mode", "ro"), 1e-9)
	require.InDelta(t, 1.0, metricCounterValue(t, mfsAfterRO, "bowl_storage_control_actor_evictions_total"), 1e-9)
}

func TestObservability_FxModuleWiringExportsStoreAndEngineMetrics(t *testing.T) {
	origTP := otel.GetTracerProvider()
	origMP := otel.GetMeterProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetMeterProvider(origMP)
		otel.SetTextMapPropagator(origProp)
	})

	pool, ctx := setupIsolatedTestDB(t, "ctrl_obs_fx_wiring")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	obsCfg := observability.DefaultConfig()
	obsCfg.OtelEnabled = true
	obsCfg.OtelServiceName = "bowl-storage-control-test"
	obsCfg.OtelTracesExporter = telemetry.ExporterNone
	obsCfg.MetricsEnabled = true

	metaCfg := config.MetaConfig{
		DataDirVal:   root,
		AtimeModeVal: "relatime",
	}

	var (
		rt  *observability.Runtime
		mgr control.Manager
	)
	app := fx.New(
		fx.NopLogger,
		fx.Supply(
			fx.Annotate(obsCfg, fx.As(new(observability.Config))),
			fx.Annotate(dbStore, fx.As(new(db.Store))),
			metaCfg,
		),
		fx.Provide(
			obsfx.AsOption(func() observability.Option {
				return observability.WithSetDefaultLogger(false)
			}),
		),
		obsfx.Module,
		control.Module,
		fx.Populate(&rt, &mgr),
	)
	require.NoError(t, app.Start(ctx))
	t.Cleanup(func() {
		require.NoError(t, app.Stop(context.Background()))
	})

	disk, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "fx-wired", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mountRes, err := mgr.MountDisk(ctx, control.MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)

	fAttr, err := actor.Create(store.CreateReq{ParentIno: control.RootIno, Name: []byte("fx.txt"), Mode: 0o644, Epoch: mountRes.Epoch})
	require.NoError(t, err)
	_, _, err = actor.Write(fAttr.Ino, 0, []byte("fx-metrics-payload"), mountRes.Epoch)
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(fAttr.Ino, mountRes.Epoch))

	mux := http.NewServeMux()
	rt.MountAdminRoutes(mux)

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()
	require.Contains(t, body, "bowl_storage_actor_queue_wait_seconds_bucket")
	require.Contains(t, body, "bowl_storage_actor_tick_phase_duration_seconds_bucket")
	require.Contains(t, body, "bowl_storage_blob_write_blocks_total")
	require.Contains(t, body, "bowl_storage_blob_pwrite_duration_seconds_bucket")
	require.Contains(t, body, "bowl_storage_pebble_wal_fsync_duration_seconds_bucket")
	require.Contains(t, body, "bowl_storage_pebble_block_cache_bytes")
	require.Contains(t, body, "bowl_storage_control_actor_loads_total")
	require.Contains(t, body, "bowl_storage_control_actor_evictions_total")
	require.Contains(t, body, "bowl_storage_control_cleanup_errors_total")
	require.NotContains(t, body, "disk_id=")
}

func TestObservability_LifecycleLogsEvictionAndCleanupErrorAccounting(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "ctrl_obs_logs_cleanup")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	var logBuf syncLogBuffer
	logCfg := slogx.EnvConfig{
		LogLevel:  "debug",
		LogFormat: "json",
	}
	logger := slogx.New(logCfg, slogx.WithWriter(&logBuf), slogx.WithSetDefault(false))

	tp := sdktrace.NewTracerProvider()
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
	})
	tracedCtx, span := tp.Tracer("control-test").Start(ctx, "lifecycle-op")
	defer span.End()
	expectedTraceID := span.SpanContext().TraceID().String()

	mgr := control.New(
		dbStore,
		control.WithStorageRoot(root),
		control.WithLogger(logger),
	)
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	reg := prometheus.NewPedanticRegistry()
	reg.MustRegister(mgr.Collector())

	// 1. CreateDisk -> MountDisk(ro) -> MountDisk(rw fence) -> Evict -> Lazy Actor Load -> DeleteDisk
	disk, err := mgr.CreateDisk(tracedCtx, control.CreateDiskParams{Name: "log-disk", MaxBytes: 8 << 20})
	require.NoError(t, err)

	_, err = mgr.MountDisk(tracedCtx, control.MountDiskParams{DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)

	rwMount, err := mgr.MountDisk(tracedCtx, control.MountDiskParams{DiskID: disk.DiskID, ReadOnly: false})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rwMount.Epoch)

	evicted, err := control.EvictIdleActorForTest(tracedCtx, mgr, disk.DiskID)
	require.NoError(t, err)
	require.True(t, evicted)

	_, err = mgr.Actor(tracedCtx, disk.DiskID)
	require.NoError(t, err)

	require.NoError(t, mgr.DeleteDisk(tracedCtx, disk.DiskID))

	// 2. 验证回滚与删盘路径目录清理失败不静默吞错：输出 WarnContext 且累加 cleanup_errors_total
	failMgr := control.New(
		&failingCreateDiskStore{Store: dbStore},
		control.WithStorageRoot(t.TempDir()),
		control.WithLogger(logger),
	)
	t.Cleanup(func() {
		require.NoError(t, failMgr.Close())
	})
	control.SetRemoveAllForTest(failMgr, func(string) error {
		return errors.New("simulated directory remove EACCES")
	})

	failReg := prometheus.NewPedanticRegistry()
	failReg.MustRegister(failMgr.Collector())

	_, err = failMgr.CreateDisk(tracedCtx, control.CreateDiskParams{Name: "rollback-err", MaxBytes: 4 << 20})
	require.Error(t, err)

	failMFs := gatherMap(t, failReg)
	require.InDelta(t, 1.0, metricCounterValue(t, failMFs, "bowl_storage_control_cleanup_errors_total"), 1e-9)

	// 同时验证 DeleteDisk 清理目录失败累加 cleanup_errors_total
	disk2, err := mgr.CreateDisk(tracedCtx, control.CreateDiskParams{Name: "del-err-disk", MaxBytes: 4 << 20})
	require.NoError(t, err)
	realDir := ""
	control.SetRemoveAllForTest(mgr, func(target string) error {
		realDir = target
		return errors.New("simulated delete dir remove EIO")
	})
	t.Cleanup(func() {
		if realDir != "" {
			_ = os.RemoveAll(realDir)
		}
	})
	err = mgr.DeleteDisk(tracedCtx, disk2.DiskID)
	require.Error(t, err)

	mfs := gatherMap(t, reg)
	require.InDelta(t, 1.0, metricCounterValue(t, mfs, "bowl_storage_control_cleanup_errors_total"), 1e-9)
	require.InDelta(t, 1.0, metricCounterValue(t, mfs, "bowl_storage_control_actor_evictions_total"), 1e-9)
	require.InDelta(t, 3.0, metricCounterValue(t, mfs, "bowl_storage_control_actor_loads_total", "mode", "rw"), 1e-9)

	// 3. 核查结构化日志条目（含 disk_id、epoch、mode、component="control" 与 trace_id）
	entries := logBuf.Entries(t)
	expectedMessages := map[string]bool{
		"disk created":                           false,
		"disk mounted":                           false,
		"disk epoch fenced":                      false,
		"actor evicted":                          false,
		"actor loaded":                           false,
		"disk deleted":                           false,
		"create disk rollback remove dir failed": false,
		"delete disk directory cleanup failed":   false,
	}
	for _, entry := range entries {
		if entry["component"] != "control" {
			continue
		}
		msg, _ := entry["msg"].(string)
		if _, tracked := expectedMessages[msg]; !tracked {
			continue
		}
		expectedMessages[msg] = true
		require.Equal(t, expectedTraceID, entry["trace_id"], "日志 %q 必须继承上下文 trace_id", msg)
		require.NotEmpty(t, entry["disk_id"], "日志 %q 必须携带 disk_id", msg)

		switch msg {
		case "disk created", "disk mounted", "disk epoch fenced", "actor evicted", "actor loaded", "disk deleted":
			require.Contains(t, entry, "epoch", "生命周期日志 %q 必须包含 epoch", msg)
			require.Contains(t, entry, "mode", "生命周期日志 %q 必须包含 mode", msg)
		case "create disk rollback remove dir failed", "delete disk directory cleanup failed":
			require.Equal(t, "WARN", entry["level"], "清理失败日志 %q 必须为 WARN 级别", msg)
		}
	}
	for msg, seen := range expectedMessages {
		require.True(t, seen, "缺失预期的控制面结构化日志: %s", msg)
	}
}

func TestObservability_ConcurrentAcquireDeleteAndScrapeRace(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "ctrl_obs_race")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	storeMetrics := store.NewMetrics()
	mgr := control.New(
		dbStore,
		control.WithStorageRoot(root),
		control.WithStoreMetrics(storeMetrics),
	)
	closed := false
	t.Cleanup(func() {
		if !closed {
			require.NoError(t, mgr.Close())
		}
	})

	reg := prometheus.NewRegistry()
	reg.MustRegister(storeMetrics, mgr.Collector())

	stopScrape := make(chan struct{})
	errCh := make(chan error, 128)
	reportErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
	}

	var scrapeCount atomic.Uint64
	var scrapeWG sync.WaitGroup
	for range 3 {
		scrapeWG.Go(func() {
			for {
				select {
				case <-stopScrape:
					return
				default:
					mfs, gatherErr := reg.Gather()
					if gatherErr != nil {
						reportErr(fmt.Errorf("gather failed: %w", gatherErr))
						return
					}
					if len(mfs) == 0 {
						reportErr(errors.New("gather returned empty metric families"))
						return
					}
					scrapeCount.Add(1)
				}
			}
		})
	}

	const workers = 4
	const rounds = 5
	var workerWG sync.WaitGroup
	for w := range workers {
		workerID := w
		workerWG.Go(func() {
			for r := range rounds {
				disk, createErr := mgr.CreateDisk(ctx, control.CreateDiskParams{
					Name:     fmt.Sprintf("race-w%d-r%d", workerID, r),
					MaxBytes: 8 << 20,
				})
				if createErr != nil {
					reportErr(fmt.Errorf("create disk failed: %w", createErr))
					return
				}

				mountRes, mountErr := mgr.MountDisk(ctx, control.MountDiskParams{
					DiskID:   disk.DiskID,
					ReadOnly: false,
				})
				if mountErr != nil {
					reportErr(fmt.Errorf("mount disk failed: %w", mountErr))
					return
				}

				// 先写入样本数据，确保并发抓取覆盖非空快照与 WAL 指标
				actorInit, initErr := mgr.Actor(ctx, disk.DiskID)
				if initErr != nil {
					reportErr(fmt.Errorf("initial actor lookup failed: %w", initErr))
					return
				}
				fAttr, mkErr := actorInit.Create(store.CreateReq{
					ParentIno: control.RootIno,
					Name:      []byte("race.bin"),
					Mode:      0o644,
					Epoch:     mountRes.Epoch,
				})
				if mkErr != nil {
					reportErr(fmt.Errorf("create file failed: %w", mkErr))
					return
				}
				if _, _, wrErr := actorInit.Write(fAttr.Ino, 0, []byte("race-data"), mountRes.Epoch); wrErr != nil {
					reportErr(fmt.Errorf("write file failed: %w", wrErr))
					return
				}

				// 并发 Acquire (Actor) + Evict/DeleteDisk 竞争
				var innerWG sync.WaitGroup
				innerWG.Go(func() {
					for range 4 {
						actor, acqErr := mgr.Actor(ctx, disk.DiskID)
						if acqErr != nil {
							if !errors.Is(acqErr, control.ErrNotFound) && !errors.Is(acqErr, control.ErrClosed) {
								reportErr(fmt.Errorf("unexpected actor acquire error: %w", acqErr))
							}
							return
						}
						if _, statErr := actor.StatFS(); statErr != nil {
							if !errors.Is(statErr, store.ErrClosed) {
								reportErr(fmt.Errorf("unexpected StatFS error: %w", statErr))
							}
							return
						}
					}
				})
				round := r
				innerWG.Go(func() {
					if round%2 == 0 {
						if _, evictErr := control.EvictIdleActorForTest(ctx, mgr, disk.DiskID); evictErr != nil {
							if !errors.Is(evictErr, control.ErrClosed) {
								reportErr(fmt.Errorf("unexpected evict error: %w", evictErr))
							}
						}
					}
					if delErr := mgr.DeleteDisk(ctx, disk.DiskID); delErr != nil {
						reportErr(fmt.Errorf("delete disk failed: %w", delErr))
					}
				})
				innerWG.Wait()
			}
		})
	}

	workerWG.Wait()

	// 在高频 Collect 持续运行时执行 mgr.Close()，检验共享 Cache Ref/Unref 与池关闭零竞态
	time.Sleep(10 * time.Millisecond)
	require.NoError(t, mgr.Close())
	closed = true

	close(stopScrape)
	scrapeWG.Wait()
	close(errCh)
	for asyncErr := range errCh {
		require.NoError(t, asyncErr)
	}
	require.Positive(t, scrapeCount.Load())
}
