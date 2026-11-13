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
	"go.opentelemetry.io/otel/trace"

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
	_, err = actor.Checkpoint(ctx, filepath.Join(t.TempDir(), "ck-pebble-events"))
	require.NoError(t, err)
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
	require.NoError(t, actor.Close())

	reopened, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-otel-spans",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopened.Close()) }()

	_, err = reopened.Read(sampledCtx, f.Ino, 0, store.BlobSize+4096)
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

func TestForkObservabilityCheckpointPhaseSpansAndAllocatorTrace(t *testing.T) {
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

	metrics := store.NewMetrics()
	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(metrics))

	var logMu sync.Mutex
	var logBuf bytes.Buffer
	warnLogger := slog.New(slog.NewJSONHandler(&syncWriter{mu: &logMu, buf: &logBuf}, &slog.HandlerOptions{Level: slog.LevelWarn}))

	var inoTraceIDs, packTraceIDs []string
	var allocMu sync.Mutex

	rootDir := t.TempDir()
	srcDir := filepath.Join(rootDir, "src")
	actor, err := store.Open(store.Options{
		Dir:               srcDir,
		DiskID:            "disk-fork-obs",
		MaxBytes:          64 << 20,
		Metrics:           metrics,
		Logger:            warnLogger,
		SlowTickThreshold: time.Nanosecond,
		InoAllocator: func(allocCtx context.Context, minNext uint64) (uint64, uint64, error) {
			allocMu.Lock()
			inoTraceIDs = append(inoTraceIDs, mustTraceIDFromCtx(allocCtx))
			allocMu.Unlock()
			return minNext, minNext + store.InodeSegment - 1, nil
		},
		PackAllocator: func(allocCtx context.Context, minNext uint64) (uint64, uint64, error) {
			allocMu.Lock()
			packTraceIDs = append(packTraceIDs, mustTraceIDFromCtx(allocCtx))
			allocMu.Unlock()
			return minNext, minNext + store.PackSegment - 1, nil
		},
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	tracer := tp.Tracer("test-fork-rpc")
	sampledCtx, rootSpan := tracer.Start(context.Background(), "rpc.ForkSource")
	wantTraceID := rootSpan.SpanContext().TraceID().String()

	f, err := actor.Create(sampledCtx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("seed.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(sampledCtx, f.Ino, 0, bytes.Repeat([]byte{0x3C}, 4096))
	require.NoError(t, err)

	// 写入后删除留一个已挂载孤儿，使 SanitizeCheckpoint 产生实盘日志。
	orphan, err := actor.Create(sampledCtx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("orphan.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(sampledCtx, orphan.Ino, 0, bytes.Repeat([]byte{0x4D}, 4096))
	require.NoError(t, err)
	require.NoError(t, actor.Unlink(sampledCtx, store.RootIno, []byte("orphan.bin"), true))

	commitID := "01900000-0000-7000-8000-0000000000ab"
	commitDir := filepath.Join(rootDir, "commits", commitID)
	ckStats, err := actor.Checkpoint(sampledCtx, commitDir)
	require.NoError(t, err)
	require.True(t, ckStats.HasOrphans)
	require.GreaterOrEqual(t, ckStats.LivePacks, 1)

	_, err = store.SanitizeCheckpoint(sampledCtx, store.SanitizeOptions{
		Dir:      commitDir,
		CommitID: commitID,
		Logger:   warnLogger,
		Metrics:  metrics,
	})
	require.NoError(t, err)

	childDir := filepath.Join(rootDir, "disks", "child")
	require.NoError(t, store.CloneCheckpointDir(sampledCtx, commitDir, childDir))
	rootSpan.End()

	allocMu.Lock()
	require.Contains(t, inoTraceIDs, wantTraceID, "InoAllocator 必须继承采样请求的 TraceID")
	require.Contains(t, packTraceIDs, wantTraceID, "PackAllocator 必须继承采样请求的 TraceID")
	allocMu.Unlock()

	spans := exporter.GetSpans()
	var tickSpan, ckSpan *tracetest.SpanStub
	spanCounts := make(map[string]int)
	for i := range spans {
		sp := &spans[i]
		spanCounts[sp.Name]++
		if sp.Name == "store.Checkpoint" {
			ckSpan = sp
		}
		if sp.Name == "store.Tick" && ckSpan != nil && sp.SpanContext.SpanID() == ckSpan.Parent.SpanID() {
			tickSpan = sp
		}
	}
	if tickSpan == nil && ckSpan != nil {
		for i := range spans {
			if spans[i].SpanContext.SpanID() == ckSpan.Parent.SpanID() {
				tickSpan = &spans[i]
				break
			}
		}
	}
	require.NotNil(t, ckSpan, "必须采集到 store.Checkpoint span")
	require.NotNil(t, tickSpan, "store.Checkpoint 的父 Span 必须是 store.Tick")
	require.Equal(t, "store.Tick", tickSpan.Name)
	require.False(t, tickSpan.EndTime.Before(ckSpan.EndTime), "store.Tick 必须包裹完整的 store.Checkpoint 阶段")
	require.GreaterOrEqual(t, spanCounts["store.SanitizeCheckpoint"], 1)
	require.GreaterOrEqual(t, spanCounts["store.CloneCheckpointDir"], 1)

	gathered := gatherHistogramMap(t, reg)
	ckMetric := findMetricByLabel(gathered["bowl_storage_actor_tick_phase_duration_seconds"], "phase", "checkpoint")
	require.NotNil(t, ckMetric, "必须注册并观测 phase=checkpoint")
	require.GreaterOrEqual(t, ckMetric.GetHistogram().GetSampleCount(), uint64(1))
	var sawExemplar bool
	for _, b := range ckMetric.GetHistogram().GetBucket() {
		if ex := b.GetExemplar(); ex != nil {
			for _, lp := range ex.GetLabel() {
				if lp.GetName() == "trace_id" && lp.GetValue() == wantTraceID {
					sawExemplar = true
				}
			}
		}
	}
	require.True(t, sawExemplar, "采样态 phase=checkpoint 直方图必须附带匹配的 trace_id Exemplar")

	rawCommitLog, err := os.ReadFile(filepath.Join(commitDir, store.PebbleEventsLogFileName))
	require.NoError(t, err)
	require.Contains(t, string(rawCommitLog), `"disk_id":"commit:`+commitID+`"`, "SanitizeCheckpoint 的 pebble_events.log 必须带有 commit:<id> 前缀标识")

	logMu.Lock()
	logText := logBuf.String()
	logMu.Unlock()
	require.Contains(t, logText, `"has_checkpoint":true`)
	require.Contains(t, logText, `"checkpoint_ms"`)
}

type syncWriter struct {
	mu  *sync.Mutex
	buf *bytes.Buffer
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func mustTraceIDFromCtx(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	sc := trace.SpanContextFromContext(ctx)
	if !sc.IsValid() {
		return ""
	}
	return sc.TraceID().String()
}
