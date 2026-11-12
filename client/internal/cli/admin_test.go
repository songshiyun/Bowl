package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func newTestSamplerProvider(exporter sdktrace.SpanExporter, ratio float64) *sdktrace.TracerProvider {
	return sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(newClientTraceSampler(ratio)),
	)
}

func TestNormalizeOTLPEndpointAndBareEndpointStartup(t *testing.T) {
	require.Empty(t, normalizeOTLPEndpoint("   "))
	require.Equal(t, "http://172.16.1.205:4317", normalizeOTLPEndpoint("172.16.1.205:4317"))
	require.Equal(t, "http://172.16.1.205:4317", normalizeOTLPEndpoint("  172.16.1.205:4317  "))
	require.Equal(t, "http://127.0.0.1:4317", normalizeOTLPEndpoint("http://127.0.0.1:4317"))
	require.Equal(t, "https://collector.example.com:4317", normalizeOTLPEndpoint("https://collector.example.com:4317"))

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "172.16.1.205:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	var logBuf bytes.Buffer
	opts := &mountOptions{
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 0.001,
	}
	obs, err := setupObservability(context.Background(), opts, &logBuf)
	require.NoError(t, err, "裸 host:port OTLP endpoint 应自动补齐 http:// 并成功构建 OTLP 导出器")
	require.Equal(t, "http://172.16.1.205:4317", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, obs.Shutdown(shutdownCtx))
}

func TestPriorityOpSampler_FsyncAndLinkedSpanBoost(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := newTestSamplerProvider(exporter, 1e-12)
	defer func() { _ = tp.Shutdown(context.Background()) }()

	tracer := tp.Tracer("bowl/client/test")

	// 1. 低频持久化与生命周期屏障（Fsync / FsyncDir / OnUnmount）在极低采样率下必须 100% 采样。
	for _, opName := range []string{"fuse.Fsync", "fuse.FsyncDir", "fuse.OnUnmount"} {
		_, sp := tracer.Start(context.Background(), opName)
		require.True(t, sp.SpanContext().IsSampled(), "%s 应确定性采样", opName)
		sp.End()
	}

	// 2. 携带已采样 SpanLink 的后台异步根 Span（write_behind.FlushChunk / read_ahead.Prefetch）必须跟随采样。
	sampledCtx := trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16},
		SpanID:     [8]byte{1, 2, 3, 4, 5, 6, 7, 8},
		TraceFlags: trace.FlagsSampled,
	}))
	_, linkedRoot := tracer.Start(context.Background(), "write_behind.FlushChunk",
		trace.WithNewRoot(),
		trace.WithLinks(trace.LinkFromContext(sampledCtx)),
	)
	require.True(t, linkedRoot.SpanContext().IsSampled(), "关联已采样写入者的异步刷盘根 Span 必须保留采样")
	linkedRoot.End()

	// 3. 显式配置采样率为 0 时完全关闭采样，不违规强开。
	zeroTP := newTestSamplerProvider(exporter, 0)
	defer func() { _ = zeroTP.Shutdown(context.Background()) }()
	_, zeroSpan := zeroTP.Tracer("bowl/client/test").Start(context.Background(), "fuse.Fsync")
	require.False(t, zeroSpan.SpanContext().IsSampled())
	zeroSpan.End()
}

func TestSetupObservability_SubMillisecondRPCBucketsAndAdminEndpoints(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	var logBuf bytes.Buffer
	opts := &mountOptions{
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: 1.0,
	}
	obs, err := setupObservability(context.Background(), opts, &logBuf)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = obs.Shutdown(ctx)
	}()

	hist, err := obs.meterProvider.Meter("connectrpc").Float64Histogram("rpc.client.duration")
	require.NoError(t, err)
	hist.Record(context.Background(), 0.00003) // 30µs，落在 (25µs, 50µs] 桶

	instFS := fusefs.NewInstrumentedRawFS(fusefs.NewRawFS(nil, fusefs.Config{
		DiskID:  "test-disk",
		RootIno: 1,
	}))
	require.NoError(t, obs.RegisterCollector(instFS.Collector()))

	admin, err := startAdminServer("127.0.0.1:0", obs, instFS)
	require.NoError(t, err)
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		admin.shutdown(ctx)
	}()

	metricsResp, err := http.Get("http://" + admin.addr() + "/metrics")
	require.NoError(t, err)
	metricsBody, err := io.ReadAll(metricsResp.Body)
	_ = metricsResp.Body.Close()
	require.NoError(t, err)
	metricsText := string(metricsBody)
	require.Contains(t, metricsText, `le="2.5e-05"} 0`)
	require.Contains(t, metricsText, `le="5e-05"} 1`)
	require.Contains(t, metricsText, "bowl_client_write_behind_writes_total")
	require.Contains(t, metricsText, "bowl_client_fuse_connection_waiting")

	statsResp, err := http.Get("http://" + admin.addr() + "/debug/stats")
	require.NoError(t, err)
	statsBody, err := io.ReadAll(statsResp.Body)
	_ = statsResp.Body.Close()
	require.NoError(t, err)

	var snap fusefs.Snapshot
	require.NoError(t, json.Unmarshal(statsBody, &snap))
	require.Len(t, snap.FUSE.Ops, fusefs.OpCount)
}
