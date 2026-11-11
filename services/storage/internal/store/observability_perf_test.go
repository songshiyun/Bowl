package store_test

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestPebbleEventsLogPersistedUnderWarnLogLevel(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	warnLogger := slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelWarn}))

	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-pebble-events",
		MaxBytes: 64 << 20,
		Logger:   warnLogger,
	})
	require.NoError(t, err)

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("events.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x7A}, 4096))
	require.NoError(t, err)
	require.NoError(t, actor.DB().Flush())
	require.NoError(t, actor.Close())

	logPath := filepath.Join(dir, store.PebbleEventsLogFileName)
	raw, err := os.ReadFile(logPath)
	require.NoError(t, err)
	require.NotEmpty(t, raw, "LOG_LEVEL=warn 下 pebble_events.log 必须真实落盘非空")

	seenEvents := make(map[string]int)
	scanner := bufio.NewScanner(bytes.NewReader(raw))
	for scanner.Scan() {
		line := bytes.TrimSpace(scanner.Bytes())
		if len(line) == 0 {
			continue
		}
		var rec map[string]any
		require.NoError(t, json.Unmarshal(line, &rec))
		require.Equal(t, "disk-pebble-events", rec["disk_id"])
		ev, ok := rec["event"].(string)
		require.True(t, ok && ev != "")
		seenEvents[ev]++
	}
	require.NoError(t, scanner.Err())

	for _, expected := range []string{"open_begin", "wal_created", "table_created", "flush_begin", "flush_end", "close"} {
		require.GreaterOrEqual(t, seenEvents[expected], 1, "pebble_events.log 缺少关键事件 %s", expected)
	}
}

func TestReadIntoDirtyBufferZeroesOnlySparseHoles(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-sparse-readinto",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("sparse.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 构造 1 MiB (4 × 256 KiB) 混合稀疏布局：
	// - Block 0: [0, 100 KiB) = 0xAA, [100 KiB, 256 KiB) = 块尾空洞
	// - Block 1: [256 KiB, 512 KiB) = 整块未分配空洞
	// - Block 2: [512 KiB, 768 KiB) = 0xBB 满块
	// - Block 3: [768 KiB, 1024 KiB) = 末尾扩展空洞
	const part0Len = 100 * 1024
	const totalLen = 4 * store.BlobSize
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0xAA}, part0Len))
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f.Ino, 2*store.BlobSize, bytes.Repeat([]byte{0xBB}, store.BlobSize))
	require.NoError(t, err)
	size1M := uint64(totalLen)
	_, err = actor.SetAttr(ctx, f.Ino, store.SetAttrReq{Size: &size1M})
	require.NoError(t, err)

	// 1. 跨多块读取至填满 0xFE 脏数据的复用缓冲区
	dirtyBuf := bytes.Repeat([]byte{0xFE}, totalLen)
	got, err := actor.ReadInto(ctx, f.Ino, 0, totalLen, dirtyBuf[:0])
	require.NoError(t, err)
	require.Len(t, got, totalLen)
	require.Same(t, &dirtyBuf[0], &got[0], "cap(dst) 充足时 ReadInto 必须零分配复用底层数组")

	expected := make([]byte, totalLen)
	copy(expected[0:part0Len], bytes.Repeat([]byte{0xAA}, part0Len))
	copy(expected[2*store.BlobSize:3*store.BlobSize], bytes.Repeat([]byte{0xBB}, store.BlobSize))
	require.Equal(t, expected, got, "混合稀疏空洞与真实块读取不得残留任何 0xFE 脏字节")

	// 2. 单块纯空洞读取（Block 1）与块内尾部稀疏读取（Block 0 后半段）
	for i := range dirtyBuf[:store.BlobSize] {
		dirtyBuf[i] = 0xFE
	}
	gotHole, err := actor.ReadInto(ctx, f.Ino, store.BlobSize, store.BlobSize, dirtyBuf[:0])
	require.NoError(t, err)
	require.Equal(t, make([]byte, store.BlobSize), gotHole)

	for i := range dirtyBuf[:64*1024] {
		dirtyBuf[i] = 0xFE
	}
	gotTailHole, err := actor.ReadInto(ctx, f.Ino, part0Len-4096, 64*1024, dirtyBuf[:0])
	require.NoError(t, err)
	expectedTail := make([]byte, 64*1024)
	copy(expectedTail[:4096], bytes.Repeat([]byte{0xAA}, 4096))
	require.Equal(t, expectedTail, gotTailHole)
}

func gatherHistogramMap(t *testing.T, reg *prometheus.Registry) map[string][]*dto.Metric {
	t.Helper()
	fams, err := reg.Gather()
	require.NoError(t, err)
	out := make(map[string][]*dto.Metric, len(fams))
	for _, fam := range fams {
		out[fam.GetName()] = fam.GetMetric()
	}
	return out
}

func findMetricByLabel(metrics []*dto.Metric, labelName, labelVal string) *dto.Metric {
	for _, m := range metrics {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == labelName && lp.GetValue() == labelVal {
				return m
			}
		}
	}
	return nil
}

func TestSyncBarrierParallelismWALTimingAndReadHistograms(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	metrics := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-metrics-sync",
		MaxBytes: 64 << 20,
		Metrics:  metrics,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	dirAttr, err := actor.Mkdir(ctx, store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("sub"),
		Mode:      store.S_IFDIR | 0o755,
	})
	require.NoError(t, err)
	f, err := actor.Create(ctx, store.CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("data.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	sym, err := actor.Symlink(ctx, store.SymlinkReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("ln"),
		Target:    []byte("data.bin"),
	})
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(ctx, f.Ino, []byte("user.k"), []byte("v"), 0))

	// 1. 验证纯 Fsync (syncBarrierWithoutBatch) 中 .pack fdatasync 在独立协程并发执行且正确采集耗时
	_, _, err = actor.Write(ctx, f.Ino, 0, bytes.Repeat([]byte{0x31}, store.BlobSize))
	require.NoError(t, err)

	var syncHookCalls int32
	var syncHookMu sync.Mutex
	actor.Blobs().SetSyncFileHookForTest(func(_ uint64) {
		syncHookMu.Lock()
		syncHookCalls++
		syncHookMu.Unlock()
		time.Sleep(35 * time.Millisecond)
	})
	require.NoError(t, actor.Fsync(ctx, f.Ino))
	syncHookMu.Lock()
	require.Equal(t, int32(1), syncHookCalls)
	syncHookMu.Unlock()

	// 2. 验证合并批次 commitWithSyncBarrier 中 wal_apply_sync 记录完整墙钟耗时（不被并发 35ms .pack sync 扣减为零）
	f2, err := actor.Create(ctx, store.CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("merged.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(ctx, f2.Ino, 0, bytes.Repeat([]byte{0x42}, store.BlobSize))
	require.NoError(t, err)

	var (
		wg          sync.WaitGroup
		once        sync.Once
		tickEntered = make(chan struct{})
		releaseTick = make(chan struct{})
		mErr        error
		cErr        error
		fErr        error
	)
	actor.SetBeforeTickHookForTest(func() {
		once.Do(func() {
			close(tickEntered)
			<-releaseTick
		})
	})

	wg.Go(func() {
		_, mErr = actor.Mkdir(ctx, store.MkdirReq{
			ParentIno: dirAttr.Ino,
			Name:      []byte("co-batch-dir"),
			Mode:      store.S_IFDIR | 0o755,
		})
	})
	<-tickEntered
	wg.Go(func() {
		// 与下一轮或排空合并：再投递一条带 batch 变更的写命令和一条 Fsync 使它们在同一 draining 批内合并
		_, cErr = actor.Mkdir(ctx, store.MkdirReq{
			ParentIno: dirAttr.Ino,
			Name:      []byte("co-batch-dir-2"),
			Mode:      store.S_IFDIR | 0o755,
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
	actor.SetBeforeTickHookForTest(nil)
	actor.Blobs().SetSyncFileHookForTest(nil)

	// 3. 触发全部 9 类只读操作与冷读 pread
	_, _, err = actor.Lookup(ctx, dirAttr.Ino, []byte("data.bin"))
	require.NoError(t, err)
	_, err = actor.GetAttr(ctx, f.Ino)
	require.NoError(t, err)
	_, err = actor.ReadDir(ctx, dirAttr.Ino, 0, 16)
	require.NoError(t, err)
	_, err = actor.ReadDirPlus(ctx, dirAttr.Ino, 0, 16)
	require.NoError(t, err)
	_, err = actor.Readlink(ctx, sym.Ino)
	require.NoError(t, err)
	_, err = actor.GetXattr(ctx, f.Ino, []byte("user.k"))
	require.NoError(t, err)
	_, err = actor.ListXattr(ctx, f.Ino)
	require.NoError(t, err)
	_, err = actor.StatFS(ctx)
	require.NoError(t, err)

	// 写入跨块数据并清退热缓存，触发 KVScan (data_blocks) 与磁盘 .pack pread
	_, _, err = actor.Write(ctx, f.Ino, store.BlobSize, bytes.Repeat([]byte{0x32}, 4096))
	require.NoError(t, err)
	ref1 := requireDataRef(t, actor, f.Ino, 1)
	actor.Blobs().EvictHot(ref1)
	_, err = actor.Read(ctx, f.Ino, 0, store.BlobSize+4096)
	require.NoError(t, err)

	gathered := gatherHistogramMap(t, reg)

	// 断言阶段拆分与 WAL Sync 完整墙钟计时
	phaseMetrics := gathered["bowl_storage_actor_tick_phase_duration_seconds"]
	require.NotNil(t, findMetricByLabel(phaseMetrics, "phase", "phase_a_validate"))
	require.NotNil(t, findMetricByLabel(phaseMetrics, "phase", "phase_b_mutate"))
	require.Nil(t, findMetricByLabel(phaseMetrics, "phase", "validate_mutate"), "旧的合并标签 validate_mutate 必须彻底移除")

	walSyncMetric := findMetricByLabel(phaseMetrics, "phase", "wal_apply_sync")
	require.NotNil(t, walSyncMetric)
	require.GreaterOrEqual(t, walSyncMetric.GetHistogram().GetSampleSum(), 0.030, "commitWithSyncBarrier 中 wal_apply_sync 必须记录从 ApplyNoSyncWait 到 SyncWait 的完整墙钟")

	// 断言全部 9 类只读操作直方图非零
	readOpMetrics := gathered["bowl_storage_read_op_duration_seconds"]
	for _, op := range []string{"read", "lookup", "getattr", "readdir", "readdirplus", "readlink", "getxattr", "listxattr", "statfs"} {
		m := findMetricByLabel(readOpMetrics, "op", op)
		require.NotNil(t, m, "缺少 read_op 直方图标签 %s", op)
		require.GreaterOrEqual(t, m.GetHistogram().GetSampleCount(), uint64(1), "read_op=%s 计数必须 >= 1", op)
	}

	// 断言 KV 点查/扫描与 BlobPread 直方图非零
	kvMetrics := gathered["bowl_storage_kv_read_duration_seconds"]
	require.GreaterOrEqual(t, findMetricByLabel(kvMetrics, "op", "point_get").GetHistogram().GetSampleCount(), uint64(1))
	require.GreaterOrEqual(t, findMetricByLabel(kvMetrics, "op", "prefix_scan").GetHistogram().GetSampleCount(), uint64(1))

	preadMetrics := gathered["bowl_storage_blob_pread_duration_seconds"]
	require.Len(t, preadMetrics, 1)
	require.GreaterOrEqual(t, preadMetrics[0].GetHistogram().GetSampleCount(), uint64(1))
}

func TestStoreOTelSpansWhenSampledAndZeroAllocsWhenUnsampled(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
	})

	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-otel-spans",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 未采样 context.Background() 热路径零分配验证
	unsampledCtx := context.Background()
	allocs := testing.AllocsPerRun(100, func() {
		_, sErr := actor.StatFS(unsampledCtx)
		if sErr != nil {
			t.Fatal(sErr)
		}
	})
	require.Zero(t, allocs, "未采样请求热路径 StatFS 必须保持 0 allocs/op")
	require.Empty(t, exporter.GetSpans(), "未携带父录制 Span 时 store 层绝不产生孤立子 Span")

	// 2. 采样请求全链路子 Span 与 Tick SpanLink 验证
	tracer := tp.Tracer("test-rpc")
	sampledCtx, rootSpan := tracer.Start(context.Background(), "rpc.WriteAndRead")
	rootSpanID := rootSpan.SpanContext().SpanID()

	f, err := actor.Create(sampledCtx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("traced.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(sampledCtx, f.Ino, 0, bytes.Repeat([]byte{0x5A}, store.BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(sampledCtx, f.Ino, store.BlobSize, bytes.Repeat([]byte{0x5B}, 4096))
	require.NoError(t, err)
	require.NoError(t, actor.Fsync(sampledCtx, f.Ino))

	ref1 := requireDataRef(t, actor, f.Ino, 1)
	actor.Blobs().EvictHot(ref1)
	_, err = actor.Read(sampledCtx, f.Ino, 0, store.BlobSize+4096)
	require.NoError(t, err)
	rootSpan.End()

	spans := exporter.GetSpans()
	spanNames := make(map[string]int)
	var foundTickWithLink bool
	for _, sp := range spans {
		spanNames[sp.Name]++
		if sp.Name == "store.Tick" && len(sp.Links) > 0 {
			for _, lnk := range sp.Links {
				if lnk.SpanContext.SpanID() == rootSpanID {
					foundTickWithLink = true
				}
			}
		}
	}

	for _, expected := range []string{
		"store.BlobWrite",
		"store.QueueWait",
		"store.Tick",
		"store.PhaseA",
		"store.PhaseB",
		"store.SyncBarrier",
		"store.KVGet",
		"store.KVScan",
		"store.BlobPread",
	} {
		require.GreaterOrEqual(t, spanNames[expected], 1, "采样请求缺少预期子 Span %s", expected)
	}
	require.True(t, foundTickWithLink, "store.Tick 必须通过 SpanLink 关联批内采样请求的 SpanContext")
}
