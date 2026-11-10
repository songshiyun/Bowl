package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/starsrc/Aether/pkg/observability"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	v1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

var validW3CTraceparentRe = regexp.MustCompile(`^00-([0-9a-f]{32})-([0-9a-f]{16})-(00|01)$`)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) Lines(t *testing.T) []map[string]any {
	t.Helper()
	b.mu.Lock()
	raw := b.buf.String()
	b.mu.Unlock()

	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return nil
	}
	parts := strings.Split(trimmed, "\n")
	out := make([]map[string]any, 0, len(parts))
	for _, line := range parts {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		var entry map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &entry), "invalid JSON log line: %s", line)
		out = append(out, entry)
	}
	return out
}

func (b *syncBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
}

func saveAndRestoreGlobalOTel(t *testing.T) {
	t.Helper()
	origTP := otel.GetTracerProvider()
	origMP := otel.GetMeterProvider()
	origProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(origTP)
		otel.SetMeterProvider(origMP)
		otel.SetTextMapPropagator(origProp)
	})
}

func newH2CTestServer(t *testing.T, handler http.Handler) *httptest.Server {
	t.Helper()
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := httptest.NewUnstartedServer(handler)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return srv
}

func newConnectErrorWithReason(t *testing.T, code connect.Code, domain, reason string) *connect.Error {
	t.Helper()
	cErr := connect.NewError(code, errors.New(reason))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: domain,
		Reason: reason,
	})
	require.NoError(t, err)
	cErr.AddDetail(detail)
	return cErr
}

func gatherMetricFamilies(t *testing.T, g prometheus.Gatherer) map[string]*dto.MetricFamily {
	t.Helper()
	mfs, err := g.Gather()
	require.NoError(t, err)
	res := make(map[string]*dto.MetricFamily, len(mfs))
	for _, mf := range mfs {
		res[mf.GetName()] = mf
	}
	return res
}

func counterValueByLabels(mf *dto.MetricFamily, wantLabels map[string]string) (float64, bool) {
	if mf == nil {
		return 0, false
	}
	for _, m := range mf.GetMetric() {
		matched := true
		for k, v := range wantLabels {
			gotVal := ""
			found := false
			for _, lp := range m.GetLabel() {
				if lp.GetName() == k {
					gotVal = lp.GetValue()
					found = true
					break
				}
			}
			if !found || gotVal != v {
				matched = false
				break
			}
		}
		if matched && m.GetCounter() != nil {
			return m.GetCounter().GetValue(), true
		}
	}
	return 0, false
}

// okStorageHandler 为覆盖全部 31 个 Unary RPC 的桩实现，用于核验 31/31 出站请求链路头透传。
type okStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
}

func (okStorageHandler) Lookup(context.Context, *connect.Request[v1.LookupRequest]) (*connect.Response[v1.LookupResponse], error) {
	return connect.NewResponse(&v1.LookupResponse{}), nil
}
func (okStorageHandler) GetAttr(context.Context, *connect.Request[v1.GetAttrRequest]) (*connect.Response[v1.GetAttrResponse], error) {
	return connect.NewResponse(&v1.GetAttrResponse{}), nil
}
func (okStorageHandler) SetAttr(context.Context, *connect.Request[v1.SetAttrRequest]) (*connect.Response[v1.SetAttrResponse], error) {
	return connect.NewResponse(&v1.SetAttrResponse{}), nil
}
func (okStorageHandler) Mknod(context.Context, *connect.Request[v1.MknodRequest]) (*connect.Response[v1.MknodResponse], error) {
	return connect.NewResponse(&v1.MknodResponse{}), nil
}
func (okStorageHandler) Mkdir(context.Context, *connect.Request[v1.MkdirRequest]) (*connect.Response[v1.MkdirResponse], error) {
	return connect.NewResponse(&v1.MkdirResponse{}), nil
}
func (okStorageHandler) Symlink(context.Context, *connect.Request[v1.SymlinkRequest]) (*connect.Response[v1.SymlinkResponse], error) {
	return connect.NewResponse(&v1.SymlinkResponse{}), nil
}
func (okStorageHandler) Readlink(context.Context, *connect.Request[v1.ReadlinkRequest]) (*connect.Response[v1.ReadlinkResponse], error) {
	return connect.NewResponse(&v1.ReadlinkResponse{}), nil
}
func (okStorageHandler) Link(context.Context, *connect.Request[v1.LinkRequest]) (*connect.Response[v1.LinkResponse], error) {
	return connect.NewResponse(&v1.LinkResponse{}), nil
}
func (okStorageHandler) Unlink(context.Context, *connect.Request[v1.UnlinkRequest]) (*connect.Response[v1.UnlinkResponse], error) {
	return connect.NewResponse(&v1.UnlinkResponse{}), nil
}
func (okStorageHandler) Rmdir(context.Context, *connect.Request[v1.RmdirRequest]) (*connect.Response[v1.RmdirResponse], error) {
	return connect.NewResponse(&v1.RmdirResponse{}), nil
}
func (okStorageHandler) Rename(context.Context, *connect.Request[v1.RenameRequest]) (*connect.Response[v1.RenameResponse], error) {
	return connect.NewResponse(&v1.RenameResponse{}), nil
}
func (okStorageHandler) Create(context.Context, *connect.Request[v1.CreateRequest]) (*connect.Response[v1.CreateResponse], error) {
	return connect.NewResponse(&v1.CreateResponse{}), nil
}
func (okStorageHandler) Read(context.Context, *connect.Request[v1.ReadRequest]) (*connect.Response[v1.ReadResponse], error) {
	return connect.NewResponse(&v1.ReadResponse{}), nil
}
func (okStorageHandler) Write(context.Context, *connect.Request[v1.WriteRequest]) (*connect.Response[v1.WriteResponse], error) {
	return connect.NewResponse(&v1.WriteResponse{}), nil
}
func (okStorageHandler) Fsync(context.Context, *connect.Request[v1.FsyncRequest]) (*connect.Response[v1.FsyncResponse], error) {
	return connect.NewResponse(&v1.FsyncResponse{}), nil
}
func (okStorageHandler) FsyncDir(context.Context, *connect.Request[v1.FsyncDirRequest]) (*connect.Response[v1.FsyncDirResponse], error) {
	return connect.NewResponse(&v1.FsyncDirResponse{}), nil
}
func (okStorageHandler) ReadDir(context.Context, *connect.Request[v1.ReadDirRequest]) (*connect.Response[v1.ReadDirResponse], error) {
	return connect.NewResponse(&v1.ReadDirResponse{}), nil
}
func (okStorageHandler) ReadDirPlus(context.Context, *connect.Request[v1.ReadDirPlusRequest]) (*connect.Response[v1.ReadDirPlusResponse], error) {
	return connect.NewResponse(&v1.ReadDirPlusResponse{}), nil
}
func (okStorageHandler) Fallocate(context.Context, *connect.Request[v1.FallocateRequest]) (*connect.Response[v1.FallocateResponse], error) {
	return connect.NewResponse(&v1.FallocateResponse{}), nil
}
func (okStorageHandler) SetXattr(context.Context, *connect.Request[v1.SetXattrRequest]) (*connect.Response[v1.SetXattrResponse], error) {
	return connect.NewResponse(&v1.SetXattrResponse{}), nil
}
func (okStorageHandler) GetXattr(context.Context, *connect.Request[v1.GetXattrRequest]) (*connect.Response[v1.GetXattrResponse], error) {
	return connect.NewResponse(&v1.GetXattrResponse{}), nil
}
func (okStorageHandler) ListXattr(context.Context, *connect.Request[v1.ListXattrRequest]) (*connect.Response[v1.ListXattrResponse], error) {
	return connect.NewResponse(&v1.ListXattrResponse{}), nil
}
func (okStorageHandler) RemoveXattr(context.Context, *connect.Request[v1.RemoveXattrRequest]) (*connect.Response[v1.RemoveXattrResponse], error) {
	return connect.NewResponse(&v1.RemoveXattrResponse{}), nil
}
func (okStorageHandler) MountDisk(context.Context, *connect.Request[v1.MountDiskRequest]) (*connect.Response[v1.MountDiskResponse], error) {
	return connect.NewResponse(&v1.MountDiskResponse{}), nil
}
func (okStorageHandler) CreateDisk(context.Context, *connect.Request[v1.CreateDiskRequest]) (*connect.Response[v1.CreateDiskResponse], error) {
	return connect.NewResponse(&v1.CreateDiskResponse{}), nil
}
func (okStorageHandler) GetDisk(context.Context, *connect.Request[v1.GetDiskRequest]) (*connect.Response[v1.GetDiskResponse], error) {
	return connect.NewResponse(&v1.GetDiskResponse{}), nil
}
func (okStorageHandler) ListDisks(context.Context, *connect.Request[v1.ListDisksRequest]) (*connect.Response[v1.ListDisksResponse], error) {
	return connect.NewResponse(&v1.ListDisksResponse{}), nil
}
func (okStorageHandler) UpdateDisk(context.Context, *connect.Request[v1.UpdateDiskRequest]) (*connect.Response[v1.UpdateDiskResponse], error) {
	return connect.NewResponse(&v1.UpdateDiskResponse{}), nil
}
func (okStorageHandler) DeleteDisk(context.Context, *connect.Request[v1.DeleteDiskRequest]) (*connect.Response[v1.DeleteDiskResponse], error) {
	return connect.NewResponse(&v1.DeleteDiskResponse{}), nil
}
func (okStorageHandler) StatFS(context.Context, *connect.Request[v1.StatFSRequest]) (*connect.Response[v1.StatFSResponse], error) {
	return connect.NewResponse(&v1.StatFSResponse{}), nil
}
func (okStorageHandler) ReclaimOrphan(context.Context, *connect.Request[v1.ReclaimOrphanRequest]) (*connect.Response[v1.ReclaimOrphanResponse], error) {
	return connect.NewResponse(&v1.ReclaimOrphanResponse{}), nil
}

func TestRequestIDContextHelpers(t *testing.T) {
	id, ok := RequestIDFromContext(nil)
	require.False(t, ok)
	require.Zero(t, id)

	id, ok = RequestIDFromContext(context.Background())
	require.False(t, ok)
	require.Zero(t, id)

	ctxZero := WithRequestID(context.Background(), 0)
	id, ok = RequestIDFromContext(ctxZero)
	require.False(t, ok)
	require.Zero(t, id)

	ctxNilParent := WithRequestID(nil, 99) //nolint:staticcheck // 故意验证 nil parent 安全性
	id, ok = RequestIDFromContext(ctxNilParent)
	require.True(t, ok)
	require.Equal(t, uint64(99), id)
}

func TestMetrics_NilReceiverSafetyAndInstanceIsolation(t *testing.T) {
	var nilMetrics *Metrics
	descCh := make(chan *prometheus.Desc, 8)
	metricCh := make(chan prometheus.Metric, 8)
	require.NotPanics(t, func() {
		nilMetrics.Describe(descCh)
		nilMetrics.Collect(metricCh)
		nilMetrics.incRetry("/storage.v1.StorageService/Read")
		nilMetrics.incRetryExhausted("/storage.v1.StorageService/Read")
		nilMetrics.incStaleEpoch()
	})
	require.Empty(t, descCh)
	require.Empty(t, metricCh)

	m1 := NewMetrics()
	m2 := NewMetrics()
	reg1 := prometheus.NewRegistry()
	reg2 := prometheus.NewRegistry()
	require.NoError(t, reg1.Register(m1))
	require.NoError(t, reg2.Register(m2))

	m1.incRetry(storagev1connect.StorageServiceReadProcedure)
	m1.incRetryExhausted(storagev1connect.StorageServiceReadProcedure)
	m1.incStaleEpoch()

	mfs1 := gatherMetricFamilies(t, reg1)
	mfs2 := gatherMetricFamilies(t, reg2)

	v1Retries, ok := counterValueByLabels(mfs1["bowl_client_rpc_retries_total"], map[string]string{
		"procedure": storagev1connect.StorageServiceReadProcedure,
	})
	require.True(t, ok)
	require.InDelta(t, 1.0, v1Retries, 1e-9)

	v1Exhausted, ok := counterValueByLabels(mfs1["bowl_client_rpc_retry_exhausted_total"], map[string]string{
		"procedure": storagev1connect.StorageServiceReadProcedure,
	})
	require.True(t, ok)
	require.InDelta(t, 1.0, v1Exhausted, 1e-9)

	v1Stale, ok := counterValueByLabels(mfs1["bowl_client_stale_epoch_total"], nil)
	require.True(t, ok)
	require.InDelta(t, 1.0, v1Stale, 1e-9)

	// m2 实例与 reg2 必须完全隔离为 0
	_, hasRetries2 := mfs2["bowl_client_rpc_retries_total"]
	require.False(t, hasRetries2)
	v2Stale, ok := counterValueByLabels(mfs2["bowl_client_stale_epoch_total"], nil)
	require.True(t, ok)
	require.InDelta(t, 0.0, v2Stale, 1e-9)
}

func TestObservabilitySetup_PropagatesHeadersOnAll31RPCsAndRecordsDurationErrorReason(t *testing.T) {
	saveAndRestoreGlobalOTel(t)

	var logBuf syncBuffer
	rpcMetrics := NewMetrics()

	cfg := observability.DefaultConfig()
	cfg.OtelEnabled = true
	cfg.OtelServiceName = "bowl-client-test"
	cfg.OtelTracesExporter = telemetry.ExporterNone
	cfg.OtelSampleRatio = 1.0
	cfg.MetricsEnabled = true
	cfg.PprofEnabled = false
	cfg.AdminAddr = "off"

	rt, err := observability.Setup(
		context.Background(),
		cfg,
		observability.WithLogWriter(&logBuf),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(rpcMetrics),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })

	// 额外挂载内存 SpanRecorder 以验证客户端 Span 属性写入了 rpc.error_reason="EBUSY"
	spanRecorder := tracetest.NewSpanRecorder()
	rt.TracerProvider().RegisterSpanProcessor(spanRecorder)

	type capturedReq struct {
		path           string
		traceparent    string
		requestID      string
		authorization  string
		acceptEncoding string
	}
	var (
		captureMu sync.Mutex
		captured  []capturedReq
		failEBUSY atomic.Bool
	)

	pathPrefix, connectHandler := storagev1connect.NewStorageServiceHandler(okStorageHandler{})
	mux := http.NewServeMux()
	mux.Handle(pathPrefix, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		captureMu.Lock()
		captured = append(captured, capturedReq{
			path:           r.URL.Path,
			traceparent:    r.Header.Get("Traceparent"),
			requestID:      r.Header.Get(HeaderRequestID),
			authorization:  r.Header.Get("Authorization"),
			acceptEncoding: r.Header.Get("Accept-Encoding"),
		})
		captureMu.Unlock()

		if failEBUSY.Load() && r.URL.Path == storagev1connect.StorageServiceWriteProcedure {
			ebusyHandler := connect.NewUnaryHandler(
				storagev1connect.StorageServiceWriteProcedure,
				func(context.Context, *connect.Request[v1.WriteRequest]) (*connect.Response[v1.WriteResponse], error) {
					return nil, newConnectErrorWithReason(t, connect.CodeFailedPrecondition, "bowl.storage", "EBUSY")
				},
			)
			ebusyHandler.ServeHTTP(w, r)
			return
		}
		connectHandler.ServeHTTP(w, r)
	}))

	srv := newH2CTestServer(t, mux)

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint: srv.URL,
		Token:    "secret-token-xyz",
		Logger:   rt.Logger(),
		Metrics:  rpcMetrics,
	})
	require.NoError(t, err)

	// 1. 验证全部 31 个 Unary RPC 100% 注入合法 traceparent、X-Request-Id、Authorization 与 Accept-Encoding
	// 前 15 个调用显式通过 WithRequestID 注入，后 16 个调用不注入（验证自动原子递增生成）
	allCalls := []struct {
		procedure string
		invoke    func(ctx context.Context) error
	}{
		{storagev1connect.StorageServiceLookupProcedure, func(ctx context.Context) error {
			_, err := client.Lookup(ctx, connect.NewRequest(&v1.LookupRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceGetAttrProcedure, func(ctx context.Context) error {
			_, err := client.GetAttr(ctx, connect.NewRequest(&v1.GetAttrRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceSetAttrProcedure, func(ctx context.Context) error {
			_, err := client.SetAttr(ctx, connect.NewRequest(&v1.SetAttrRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceMknodProcedure, func(ctx context.Context) error {
			_, err := client.Mknod(ctx, connect.NewRequest(&v1.MknodRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceMkdirProcedure, func(ctx context.Context) error {
			_, err := client.Mkdir(ctx, connect.NewRequest(&v1.MkdirRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceSymlinkProcedure, func(ctx context.Context) error {
			_, err := client.Symlink(ctx, connect.NewRequest(&v1.SymlinkRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceReadlinkProcedure, func(ctx context.Context) error {
			_, err := client.Readlink(ctx, connect.NewRequest(&v1.ReadlinkRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceLinkProcedure, func(ctx context.Context) error {
			_, err := client.Link(ctx, connect.NewRequest(&v1.LinkRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceUnlinkProcedure, func(ctx context.Context) error {
			_, err := client.Unlink(ctx, connect.NewRequest(&v1.UnlinkRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceRmdirProcedure, func(ctx context.Context) error {
			_, err := client.Rmdir(ctx, connect.NewRequest(&v1.RmdirRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceRenameProcedure, func(ctx context.Context) error {
			_, err := client.Rename(ctx, connect.NewRequest(&v1.RenameRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceCreateProcedure, func(ctx context.Context) error {
			_, err := client.Create(ctx, connect.NewRequest(&v1.CreateRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceReadProcedure, func(ctx context.Context) error {
			_, err := client.Read(ctx, connect.NewRequest(&v1.ReadRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceWriteProcedure, func(ctx context.Context) error {
			_, err := client.Write(ctx, connect.NewRequest(&v1.WriteRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceFsyncProcedure, func(ctx context.Context) error {
			_, err := client.Fsync(ctx, connect.NewRequest(&v1.FsyncRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceFsyncDirProcedure, func(ctx context.Context) error {
			_, err := client.FsyncDir(ctx, connect.NewRequest(&v1.FsyncDirRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceReadDirProcedure, func(ctx context.Context) error {
			_, err := client.ReadDir(ctx, connect.NewRequest(&v1.ReadDirRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceReadDirPlusProcedure, func(ctx context.Context) error {
			_, err := client.ReadDirPlus(ctx, connect.NewRequest(&v1.ReadDirPlusRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceFallocateProcedure, func(ctx context.Context) error {
			_, err := client.Fallocate(ctx, connect.NewRequest(&v1.FallocateRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceSetXattrProcedure, func(ctx context.Context) error {
			_, err := client.SetXattr(ctx, connect.NewRequest(&v1.SetXattrRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceGetXattrProcedure, func(ctx context.Context) error {
			_, err := client.GetXattr(ctx, connect.NewRequest(&v1.GetXattrRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceListXattrProcedure, func(ctx context.Context) error {
			_, err := client.ListXattr(ctx, connect.NewRequest(&v1.ListXattrRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceRemoveXattrProcedure, func(ctx context.Context) error {
			_, err := client.RemoveXattr(ctx, connect.NewRequest(&v1.RemoveXattrRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceMountDiskProcedure, func(ctx context.Context) error {
			_, err := client.MountDisk(ctx, connect.NewRequest(&v1.MountDiskRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceCreateDiskProcedure, func(ctx context.Context) error {
			_, err := client.CreateDisk(ctx, connect.NewRequest(&v1.CreateDiskRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceGetDiskProcedure, func(ctx context.Context) error {
			_, err := client.GetDisk(ctx, connect.NewRequest(&v1.GetDiskRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceListDisksProcedure, func(ctx context.Context) error {
			_, err := client.ListDisks(ctx, connect.NewRequest(&v1.ListDisksRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceUpdateDiskProcedure, func(ctx context.Context) error {
			_, err := client.UpdateDisk(ctx, connect.NewRequest(&v1.UpdateDiskRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceDeleteDiskProcedure, func(ctx context.Context) error {
			_, err := client.DeleteDisk(ctx, connect.NewRequest(&v1.DeleteDiskRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceStatFSProcedure, func(ctx context.Context) error {
			_, err := client.StatFS(ctx, connect.NewRequest(&v1.StatFSRequest{}))
			return err
		}},
		{storagev1connect.StorageServiceReclaimOrphanProcedure, func(ctx context.Context) error {
			_, err := client.ReclaimOrphan(ctx, connect.NewRequest(&v1.ReclaimOrphanRequest{}))
			return err
		}},
	}
	require.Len(t, allCalls, 31)

	parentCtx, parentSpan := otel.Tracer("fuse-parent").Start(context.Background(), "fuse.op")
	expectedParentTraceID := parentSpan.SpanContext().TraceID().String()
	defer parentSpan.End()

	for idx, tc := range allCalls {
		callCtx := context.Background()
		if idx == 0 {
			callCtx = parentCtx
		}
		if idx < 15 {
			callCtx = WithRequestID(callCtx, uint64(1000+idx))
		}
		require.NoError(t, tc.invoke(callCtx), "procedure %s failed", tc.procedure)
	}

	captureMu.Lock()
	require.Len(t, captured, 31)
	var prevAutoID uint64
	for idx, got := range captured {
		require.Equal(t, allCalls[idx].procedure, got.path)
		require.Equal(t, "Bearer secret-token-xyz", got.authorization)
		require.Equal(t, "identity", got.acceptEncoding)

		matches := validW3CTraceparentRe.FindStringSubmatch(got.traceparent)
		require.Len(t, matches, 4, "procedure %s missing valid W3C traceparent, got %q", got.path, got.traceparent)
		require.NotEqual(t, strings.Repeat("0", 32), matches[1], "trace_id must be non-zero")
		require.NotEqual(t, strings.Repeat("0", 16), matches[2], "span_id must be non-zero")
		require.Equal(t, "01", matches[3], "sample ratio 1.0 must set sampled flag 01")
		if idx == 0 {
			require.Equal(t, expectedParentTraceID, matches[1], "child RPC must inherit parent span trace_id")
		}

		parsedReqID, parseErr := strconv.ParseUint(got.requestID, 10, 64)
		require.NoError(t, parseErr, "X-Request-Id %q must be valid uint64", got.requestID)
		require.Greater(t, parsedReqID, uint64(0))
		if idx < 15 {
			require.Equal(t, uint64(1000+idx), parsedReqID)
		} else {
			require.Greater(t, parsedReqID, prevAutoID, "auto-generated X-Request-Id must monotonically increase")
			prevAutoID = parsedReqID
		}
	}
	captureMu.Unlock()

	// 2. 触发 Write 返回 ErrorInfo{Reason: "EBUSY", Domain: "bowl.storage"}，验证客户端 Registry 抓取到
	// rpc_client_call_duration（含 rpc_error_reason="EBUSY" 标签）及客户端 Span 含 rpc.error_reason="EBUSY"
	failEBUSY.Store(true)
	writeCtx := WithRequestID(context.Background(), 7777)
	_, writeErr := client.Write(writeCtx, connect.NewRequest(&v1.WriteRequest{
		DiskId: "disk-epoch-test",
		Epoch:  12,
		Ino:    2,
	}))
	require.Error(t, writeErr)
	require.Equal(t, syscall.EBUSY, MapError(writeErr))

	mfs := gatherMetricFamilies(t, rt.Registry())
	var (
		foundClientDurationFamily bool
		foundEBUSYLabel           bool
	)
	for name, mf := range mfs {
		if strings.HasPrefix(name, "rpc_client_call_duration") {
			foundClientDurationFamily = true
			for _, m := range mf.GetMetric() {
				for _, lp := range m.GetLabel() {
					if lp.GetName() == "rpc_error_reason" && lp.GetValue() == "EBUSY" {
						foundEBUSYLabel = true
					}
				}
			}
		}
	}
	require.True(t, foundClientDurationFamily, "client Registry must expose rpc_client_call_duration metric family")
	require.True(t, foundEBUSYLabel, "rpc_client_call_duration must carry rpc_error_reason=\"EBUSY\" label")

	var foundSpanEBUSY bool
	for _, sp := range spanRecorder.Ended() {
		for _, attr := range sp.Attributes() {
			if string(attr.Key) == "rpc.error_reason" && attr.Value.AsString() == "EBUSY" {
				foundSpanEBUSY = true
			}
		}
	}
	require.True(t, foundSpanEBUSY, "client OTel Span must carry rpc.error_reason=\"EBUSY\" attribute")
}

func TestRetryAndExhaustion_MetricsSpansAndStructuredLogs(t *testing.T) {
	saveAndRestoreGlobalOTel(t)

	var logBuf syncBuffer
	rpcMetrics := NewMetrics()

	cfg := observability.DefaultConfig()
	cfg.OtelEnabled = true
	cfg.OtelServiceName = "bowl-client-retry-test"
	cfg.OtelTracesExporter = telemetry.ExporterNone
	cfg.OtelSampleRatio = 1.0
	cfg.MetricsEnabled = true
	cfg.PprofEnabled = false
	cfg.AdminAddr = "off"

	rt, err := observability.Setup(
		context.Background(),
		cfg,
		observability.WithLogWriter(&logBuf),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(rpcMetrics),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })

	spanRecorder := tracetest.NewSpanRecorder()
	rt.TracerProvider().RegisterSpanProcessor(spanRecorder)

	var (
		mode        atomic.Int32 // 0: 前2次失败第3次成功；1: 永久返回 CodeUnavailable
		readAttempt atomic.Int32
	)

	mux := http.NewServeMux()
	mux.Handle(storagev1connect.StorageServiceReadProcedure, connect.NewUnaryHandler(
		storagev1connect.StorageServiceReadProcedure,
		func(context.Context, *connect.Request[v1.ReadRequest]) (*connect.Response[v1.ReadResponse], error) {
			n := readAttempt.Add(1)
			if mode.Load() == 1 || n <= 2 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("upstream transient reset"))
			}
			return connect.NewResponse(&v1.ReadResponse{Data: []byte("ok-payload")}), nil
		},
	))
	srv := newH2CTestServer(t, mux)

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint: srv.URL,
		Logger:   rt.Logger(),
		Metrics:  rpcMetrics,
		RetryConfig: &RetryConfig{
			MaxElapsedTime:  45 * time.Millisecond,
			InitialInterval: 8 * time.Millisecond,
			Multiplier:      1.0,
			MaxInterval:     8 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	})
	require.NoError(t, err)

	// 阶段 1：瞬断重试恢复（前 2 次失败，第 3 次成功 -> 触发 2 次退避重试，3 个独立尝试客户端 Span）
	mode.Store(0)
	readAttempt.Store(0)
	resp, err := client.Read(WithRequestID(context.Background(), 501), connect.NewRequest(&v1.ReadRequest{
		DiskId: "disk-1",
		Ino:    10,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("ok-payload"), resp.Msg.GetData())
	require.Equal(t, int32(3), readAttempt.Load())

	// 验证 RetryInterceptor 包裹在 otelconnect 外层：3 次网络尝试生成了 3 个独立的客户端 Span
	var clientReadSpans []sdktrace.ReadOnlySpan
	for _, sp := range spanRecorder.Ended() {
		if sp.Name() == "storage.v1.StorageService/Read" {
			clientReadSpans = append(clientReadSpans, sp)
		}
	}
	require.Len(t, clientReadSpans, 3, "each retry attempt must record an independent client span")

	mfs := gatherMetricFamilies(t, rt.Registry())
	retriesVal, ok := counterValueByLabels(mfs["bowl_client_rpc_retries_total"], map[string]string{
		"procedure": storagev1connect.StorageServiceReadProcedure,
	})
	require.True(t, ok)
	require.InDelta(t, 2.0, retriesVal, 1e-9)

	_, hasExhausted := mfs["bowl_client_rpc_retry_exhausted_total"]
	require.False(t, hasExhausted)

	// 验证首次重试打 WARN 日志 rpc_retry_triggered（同一次调用的第 2 次退避不重复打印）
	entries := logBuf.Lines(t)
	require.Len(t, entries, 1)
	assert.Equal(t, "WARN", entries[0]["level"])
	assert.Equal(t, "rpc_retry_triggered", entries[0]["msg"])
	assert.Equal(t, storagev1connect.StorageServiceReadProcedure, entries[0]["procedure"])
	assert.InDelta(t, 1.0, entries[0]["attempt"], 1e-9)
	assert.InDelta(t, 501.0, entries[0]["req_id"], 1e-9)
	assert.Len(t, entries[0]["trace_id"], 32)
	assert.Contains(t, entries[0]["err"], "upstream transient reset")

	// 阶段 2：连续 2 次独立调用均耗尽重试预算转为 EIO
	// 验证 retry_exhausted_total == 2，且 rpc_retry_exhausted_eio 不限频必打 2 条 ERROR 日志（1s 内 rpc_retry_triggered 被限频抑制）
	logBuf.Reset()
	mode.Store(1)

	_, err1 := client.Read(WithRequestID(context.Background(), 601), connect.NewRequest(&v1.ReadRequest{DiskId: "disk-1", Ino: 10}))
	require.Error(t, err1)
	require.Equal(t, syscall.EIO, MapError(err1))

	_, err2 := client.Read(WithRequestID(context.Background(), 602), connect.NewRequest(&v1.ReadRequest{DiskId: "disk-1", Ino: 10}))
	require.Error(t, err2)
	require.Equal(t, syscall.EIO, MapError(err2))

	mfsAfter := gatherMetricFamilies(t, rt.Registry())
	exhaustedVal, ok := counterValueByLabels(mfsAfter["bowl_client_rpc_retry_exhausted_total"], map[string]string{
		"procedure": storagev1connect.StorageServiceReadProcedure,
	})
	require.True(t, ok)
	require.InDelta(t, 2.0, exhaustedVal, 1e-9)

	retriesAfter, ok := counterValueByLabels(mfsAfter["bowl_client_rpc_retries_total"], map[string]string{
		"procedure": storagev1connect.StorageServiceReadProcedure,
	})
	require.True(t, ok)
	require.Greater(t, retriesAfter, 2.0)

	exhaustedLogs := logBuf.Lines(t)
	var (
		gotExhausted []map[string]any
		gotTriggered []map[string]any
	)
	for _, entry := range exhaustedLogs {
		switch entry["msg"] {
		case "rpc_retry_exhausted_eio":
			gotExhausted = append(gotExhausted, entry)
		case "rpc_retry_triggered":
			gotTriggered = append(gotTriggered, entry)
		}
	}
	assert.Empty(t, gotTriggered, "rpc_retry_triggered within 1s window must be rate-limited")
	require.Len(t, gotExhausted, 2, "rpc_retry_exhausted_eio must never be rate-limited")

	expectedReqIDs := []float64{601, 602}
	for i, entry := range gotExhausted {
		assert.Equal(t, "ERROR", entry["level"])
		assert.Equal(t, storagev1connect.StorageServiceReadProcedure, entry["procedure"])
		assert.GreaterOrEqual(t, entry["attempts"].(float64), 1.0)
		assert.GreaterOrEqual(t, entry["elapsed_ms"].(float64), 0.0)
		assert.InDelta(t, expectedReqIDs[i], entry["req_id"], 1e-9)
		assert.Len(t, entry["trace_id"], 32)
		assert.Contains(t, entry["err"], "upstream transient reset")
	}
}

func TestStaleEpochAndSlowRPC_MetricsRateLimitAndStructuredLogs(t *testing.T) {
	saveAndRestoreGlobalOTel(t)

	var logBuf syncBuffer
	rpcMetrics := NewMetrics()

	cfg := observability.DefaultConfig()
	cfg.OtelEnabled = true
	cfg.OtelServiceName = "bowl-client-epoch-slow-test"
	cfg.OtelTracesExporter = telemetry.ExporterNone
	cfg.OtelSampleRatio = 1.0
	cfg.MetricsEnabled = true
	cfg.PprofEnabled = false
	cfg.AdminAddr = "off"

	rt, err := observability.Setup(
		context.Background(),
		cfg,
		observability.WithLogWriter(&logBuf),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(rpcMetrics),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = rt.Shutdown(context.Background()) })

	var writeCalls atomic.Int32
	mux := http.NewServeMux()
	mux.Handle(storagev1connect.StorageServiceWriteProcedure, connect.NewUnaryHandler(
		storagev1connect.StorageServiceWriteProcedure,
		func(context.Context, *connect.Request[v1.WriteRequest]) (*connect.Response[v1.WriteResponse], error) {
			writeCalls.Add(1)
			return nil, newConnectErrorWithReason(t, connect.CodeFailedPrecondition, "bowl.storage", "EBUSY")
		},
	))
	mux.Handle(storagev1connect.StorageServiceLookupProcedure, connect.NewUnaryHandler(
		storagev1connect.StorageServiceLookupProcedure,
		func(context.Context, *connect.Request[v1.LookupRequest]) (*connect.Response[v1.LookupResponse], error) {
			return nil, newConnectErrorWithReason(t, connect.CodeNotFound, "bowl.storage", "ENOENT")
		},
	))
	mux.Handle(storagev1connect.StorageServiceFsyncProcedure, connect.NewUnaryHandler(
		storagev1connect.StorageServiceFsyncProcedure,
		func(ctx context.Context, _ *connect.Request[v1.FsyncRequest]) (*connect.Response[v1.FsyncResponse], error) {
			timer := time.NewTimer(1030 * time.Millisecond)
			defer timer.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-timer.C:
				return connect.NewResponse(&v1.FsyncResponse{}), nil
			}
		},
	))
	srv := newH2CTestServer(t, mux)

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint: srv.URL,
		Logger:   rt.Logger(),
		Metrics:  rpcMetrics,
	})
	require.NoError(t, err)

	// 1. 连续触发 3 次旧 Epoch 围栏拦截 EBUSY + 1 次普通 ENOENT
	for i := range 3 {
		_, wErr := client.Write(
			WithRequestID(context.Background(), uint64(800+i)),
			connect.NewRequest(&v1.WriteRequest{DiskId: "disk-fenced", Epoch: 9, Ino: 2}),
		)
		require.Error(t, wErr)
		require.Equal(t, syscall.EBUSY, MapError(wErr))
	}
	_, lErr := client.Lookup(WithRequestID(context.Background(), 899), connect.NewRequest(&v1.LookupRequest{DiskId: "disk-fenced", Name: []byte("missing")}))
	require.Error(t, lErr)
	require.Equal(t, syscall.ENOENT, MapError(lErr))

	// 携带 ErrorInfo 的 EBUSY 严禁重试（3 次调用对应服务端恰好 3 次收到）
	require.Equal(t, int32(3), writeCalls.Load())

	mfs := gatherMetricFamilies(t, rt.Registry())
	staleCount, ok := counterValueByLabels(mfs["bowl_client_stale_epoch_total"], nil)
	require.True(t, ok)
	require.InDelta(t, 3.0, staleCount, 1e-9, "every EBUSY fence must increment stale_epoch_total even when logs are rate-limited")

	_, hasRetries := mfs["bowl_client_rpc_retries_total"]
	require.False(t, hasRetries)

	entries := logBuf.Lines(t)
	require.Len(t, entries, 1, "writer_epoch_fenced logs within 1s window must be rate-limited to 1 entry")
	assert.Equal(t, "ERROR", entries[0]["level"])
	assert.Equal(t, "writer_epoch_fenced", entries[0]["msg"])
	assert.Equal(t, storagev1connect.StorageServiceWriteProcedure, entries[0]["procedure"])
	assert.InDelta(t, 9.0, entries[0]["local_epoch"], 1e-9)
	assert.InDelta(t, 800.0, entries[0]["req_id"], 1e-9)
	assert.Len(t, entries[0]["trace_id"], 32)
	assert.Contains(t, entries[0]["err"], "EBUSY")

	// 2. 验证单次耗时 > 1s 真实触发 slow_rpc WARN 结构化日志
	logBuf.Reset()
	ctxSlow, cancelSlow := context.WithTimeout(WithRequestID(context.Background(), 901), 5*time.Second)
	defer cancelSlow()
	_, fsyncErr := client.Fsync(ctxSlow, connect.NewRequest(&v1.FsyncRequest{DiskId: "disk-fenced", Epoch: 10, Ino: 2}))
	require.NoError(t, fsyncErr)

	slowEntries := logBuf.Lines(t)
	require.Len(t, slowEntries, 1)
	assert.Equal(t, "WARN", slowEntries[0]["level"])
	assert.Equal(t, "slow_rpc", slowEntries[0]["msg"])
	assert.Equal(t, storagev1connect.StorageServiceFsyncProcedure, slowEntries[0]["procedure"])
	assert.GreaterOrEqual(t, slowEntries[0]["duration_ms"].(float64), 1000.0)
	assert.Equal(t, "ok", slowEntries[0]["code"])
	assert.InDelta(t, 901.0, slowEntries[0]["req_id"], 1e-9)
	assert.Len(t, slowEntries[0]["trace_id"], 32)
}
