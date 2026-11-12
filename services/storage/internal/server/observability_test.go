package server

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/obsfx"
	"github.com/starsrc/pkg/serverx"
	"github.com/starsrc/pkg/tx"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type memDiskDB struct {
	mu    sync.Mutex
	disks map[[16]byte]db.Disk
}

func newMemDiskDB() *memDiskDB {
	return &memDiskDB{disks: make(map[[16]byte]db.Disk)}
}

func (m *memDiskDB) ExecTx(_ context.Context, fn func(db.Querier) error) error {
	return fn(m)
}

func (m *memDiskDB) ExecTxWithOptions(_ context.Context, fn func(db.Querier) error, _ ...tx.Option) error {
	return fn(m)
}

func (m *memDiskDB) CreateDisk(_ context.Context, arg db.CreateDiskParams) (db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	row := db.Disk{
		DiskID:    arg.DiskID,
		Name:      arg.Name,
		CreatedAt: now,
		UpdatedAt: now,
	}
	m.disks[arg.DiskID.Bytes] = row
	return row, nil
}

func (m *memDiskDB) DeleteDisk(_ context.Context, diskID pgtype.UUID) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.disks[diskID.Bytes]; !ok {
		return 0, nil
	}
	delete(m.disks, diskID.Bytes)
	return 1, nil
}

func (m *memDiskDB) GetDisk(_ context.Context, diskID pgtype.UUID) (db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.disks[diskID.Bytes]
	if !ok {
		return db.Disk{}, pgx.ErrNoRows
	}
	return row, nil
}

func (m *memDiskDB) GetDiskForUpdate(ctx context.Context, diskID pgtype.UUID) (db.Disk, error) {
	return m.GetDisk(ctx, diskID)
}

func (m *memDiskDB) ListDisks(_ context.Context, _ db.ListDisksParams) ([]db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]db.Disk, 0, len(m.disks))
	for _, d := range m.disks {
		out = append(out, d)
	}
	return out, nil
}

func (m *memDiskDB) UpdateDisk(_ context.Context, arg db.UpdateDiskParams) (db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.disks[arg.DiskID.Bytes]
	if !ok {
		return db.Disk{}, pgx.ErrNoRows
	}
	row.Name = arg.Name
	row.UpdatedAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	m.disks[arg.DiskID.Bytes] = row
	return row, nil
}

func TestEndToEndFineBucketMetricsHTTPMiddlewareAndFullChainSpans(t *testing.T) {
	ctx := context.Background()
	prevMP := otel.GetMeterProvider()
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()

	obsCfg := observability.DefaultConfig()
	obsCfg.AdminAddr = "off"
	obsCfg.LogLevel = "error"

	httpMetrics := NewHTTPMetrics()
	storeMetrics := store.NewMetrics()
	mgr := control.New(
		newMemDiskDB(),
		control.WithStorageRoot(t.TempDir()),
		control.WithStoreMetrics(storeMetrics),
	)
	t.Cleanup(func() { _ = mgr.Close() })

	rt, err := observability.Setup(
		ctx,
		config.NewObservabilityConfig(&obsCfg),
		observability.WithLogWriter(io.Discard),
		observability.WithSetDefaultLogger(false),
		observability.WithCollectors(httpMetrics, storeMetrics, mgr.Collector()),
	)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, rt.Shutdown(context.Background()))
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
		if otel.GetTracerProvider() != prevTP {
			otel.SetTracerProvider(prevTP)
		}
		otel.SetTextMapPropagator(prevProp)
	})

	ready, err := ConfigureFineBucketMeterProvider(FineBucketParams{
		Runtime:   rt,
		Telemetry: obsCfg,
		AdminCfg:  obsCfg,
	})
	require.NoError(t, err)
	fineMP, ok := otel.GetMeterProvider().(*sdkmetric.MeterProvider)
	require.True(t, ok)
	var mpShutdownOnce sync.Once
	shutdownFineMP := func(c context.Context) error {
		var sErr error
		mpShutdownOnce.Do(func() {
			sErr = fineMP.Shutdown(c)
		})
		return sErr
	}
	t.Cleanup(func() {
		require.NoError(t, shutdownFineMP(context.Background()))
	})

	// 挂载内存 Span 导出器以便断言 Server -> Control -> StoreActor 端到端 Span 链
	spanExporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(spanExporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	srv, err := New(Params{
		Config: config.ServerConfig{
			ServerPort:         0,
			InternalServerPort: 9090,
			ServerReadMaxBytes: 8 << 20,
		},
		Control:       mgr,
		Observability: obsCfg,
		Telemetry:     obsCfg,
		AdminCfg:      obsCfg,
		Gatherer:      rt.Gatherer(),
		FineBuckets:   ready,
		HTTPMetrics:   httpMetrics,
		Logger:        rt.Logger(),
	})
	require.NoError(t, err)
	require.NotNil(t, srv.AdminHandler())

	httpSrv := httptest.NewServer(srv.Handler())
	defer httpSrv.Close()

	client := storagev1connect.NewStorageServiceClient(httpSrv.Client(), httpSrv.URL)

	// 1. 全链路 RPC：CreateDisk -> MountDisk -> Create -> Write (512 KiB) -> Fsync -> Read (512 KiB)
	createDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "obs-e2e-disk",
		MaxBytes: 64 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: diskID,
	}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()

	createFileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch,
		ParentIno: store.RootIno,
		Name:      []byte("payload.bin"),
		Mode:      store.S_IFREG | 0o644,
	}))
	require.NoError(t, err)
	fileIno := createFileResp.Msg.GetInode().GetIno()

	const ioSize = 512 * 1024
	payload := make([]byte, ioSize)
	state := uint64(0x9e3779b97f4a7c15)
	for i := range payload {
		state ^= state << 13
		state ^= state >> 7
		state ^= state << 17
		payload[i] = byte(state)
	}
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 0,
		Data:   payload,
	}))
	require.NoError(t, err)

	_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
	}))
	require.NoError(t, err)

	readResp, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Offset: 0,
		Size:   ioSize,
	}))
	require.NoError(t, err)
	require.Equal(t, payload, readResp.Msg.GetData())

	// 读写池缓冲必须在 RPC 往返结束后 100% 归还零残留
	require.Zero(t, leasedWritePayloadCount())
	require.Zero(t, leasedReadPayloadCount())

	// 发送一条未知 URL 探测，验证 procedure 归一到 "other" 且不引发高基数膨胀
	probeResp, err := httpSrv.Client().Post(httpSrv.URL+"/malicious/random-path-12345", "application/proto", bytes.NewReader([]byte("x")))
	require.NoError(t, err)
	_ = probeResp.Body.Close()

	// 2. Gather 校验：无重复 target_info，rpc_server_call_duration_seconds 与 HTTP 外层直方图均为 18 级精细桶
	fams, err := rt.Gatherer().Gather()
	require.NoError(t, err)

	byName := make(map[string]*dto.MetricFamily, len(fams))
	for _, fam := range fams {
		byName[fam.GetName()] = fam
	}

	if targetFam := byName["target_info"]; targetFam != nil {
		require.Len(t, targetFam.GetMetric(), 1, "升级 MeterProvider 后 target_info 不得重复")
	}

	rpcDurFam := byName["rpc_server_call_duration_seconds"]
	require.NotNil(t, rpcDurFam, "必须导出 otelconnect rpc_server_call_duration_seconds 直方图")
	require.NotEmpty(t, rpcDurFam.GetMetric())
	rpcBuckets := rpcDurFam.GetMetric()[0].GetHistogram().GetBucket()
	// OTel Prometheus exporter 的 GetBucket() 可能不含 +Inf 或含 18 个显式上界桶，校验首桶必为 25µs (0.000025)
	require.GreaterOrEqual(t, len(rpcBuckets), len(SubMillisecondBuckets))
	require.InDelta(t, 0.000025, rpcBuckets[0].GetUpperBound(), 1e-12, "首桶边界必须消除 5ms 失真并下探至 25µs")
	require.InDelta(t, 10.0, rpcBuckets[len(SubMillisecondBuckets)-1].GetUpperBound(), 1e-9)

	httpDurFam := byName["bowl_storage_http_request_duration_seconds"]
	require.NotNil(t, httpDurFam)
	var sawWrite2xx, sawRead2xx, sawOther4xx bool
	for _, m := range httpDurFam.GetMetric() {
		var proc, sc string
		for _, lp := range m.GetLabel() {
			switch lp.GetName() {
			case "procedure":
				proc = lp.GetValue()
			case "status_class":
				sc = lp.GetValue()
			}
		}
		require.Len(t, m.GetHistogram().GetBucket(), len(SubMillisecondBuckets))
		require.InDelta(t, 0.000025, m.GetHistogram().GetBucket()[0].GetUpperBound(), 1e-12)
		if proc == storagev1connect.StorageServiceWriteProcedure && sc == "2xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawWrite2xx = true
		}
		if proc == storagev1connect.StorageServiceReadProcedure && sc == "2xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawRead2xx = true
		}
		if proc == "other" && sc == "4xx" && m.GetHistogram().GetSampleCount() == 1 {
			sawOther4xx = true
		}
	}
	require.True(t, sawWrite2xx, "外层 HTTP 直方图必须记录 Write 2xx")
	require.True(t, sawRead2xx, "外层 HTTP 直方图必须记录 Read 2xx")
	require.True(t, sawOther4xx, "未知路径必须收敛至 procedure=other, status_class=4xx")

	reqBytesFam := byName["bowl_storage_http_request_bytes_total"]
	require.NotNil(t, reqBytesFam)
	var writeReqBytes float64
	for _, m := range reqBytesFam.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "procedure" && lp.GetValue() == storagev1connect.StorageServiceWriteProcedure {
				writeReqBytes = m.GetCounter().GetValue()
			}
		}
	}
	require.GreaterOrEqual(t, writeReqBytes, float64(ioSize))

	respBytesFam := byName["bowl_storage_http_response_bytes_total"]
	require.NotNil(t, respBytesFam)
	var readRespBytes float64
	for _, m := range respBytesFam.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "procedure" && lp.GetValue() == storagev1connect.StorageServiceReadProcedure {
				readRespBytes = m.GetCounter().GetValue()
			}
		}
	}
	require.GreaterOrEqual(t, readRespBytes, float64(ioSize))

	// 3. 断言 Server -> Control -> StoreActor 全链路 OTel Span 已真实打通
	spans := spanExporter.GetSpans()
	spanNames := make(map[string]int)
	for _, sp := range spans {
		spanNames[sp.Name]++
	}
	for _, expected := range []string{
		"control.OpenDiskActor",
		"store.BlobWrite",
		"store.QueueWait",
		"store.Tick",
		"store.PhaseA",
		"store.PhaseB",
		"store.SyncBarrier",
		"store.KVGet",
		"store.KVScan",
	} {
		require.GreaterOrEqual(t, spanNames[expected], 1, "端到端 RPC 链路缺少子 Span %s", expected)
	}

	// 4. 真实请求管理面 /metrics，校验 Go runtime / process 标准指标与 bowl_storage_* / rpc_server_* 精细桶直方图完整共存，且优雅关闭零报错
	adminReq := httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody)
	adminRec := httptest.NewRecorder()
	srv.AdminHandler().ServeHTTP(adminRec, adminReq)
	require.Equal(t, http.StatusOK, adminRec.Code)
	metricsText := adminRec.Body.String()
	for _, wantMetric := range []string{
		"go_goroutines",
		"go_memstats_alloc_bytes",
		"go_gc_duration_seconds",
		"go_build_info",
		"target_info",
		"bowl_storage_disks_loaded",
		"bowl_storage_logical_bytes",
		"bowl_storage_inodes",
		"bowl_storage_http_request_duration_seconds_bucket",
		"bowl_storage_blob_sync_duration_seconds_bucket",
		"rpc_server_call_duration_seconds_bucket",
	} {
		require.Contains(t, metricsText, wantMetric, "/metrics 必须导出指标 %s", wantMetric)
	}
	if runtime.GOOS == "linux" {
		require.Contains(t, metricsText, "process_cpu_seconds_total")
		require.Contains(t, metricsText, "process_resident_memory_bytes")
	}

	require.NoError(t, shutdownFineMP(ctx))
	require.NoError(t, rt.Shutdown(ctx))
}

func TestFxModuleMetricsEndpointAndGracefulShutdown(t *testing.T) {
	ctx := context.Background()
	prevMP := otel.GetMeterProvider()
	t.Cleanup(func() {
		if otel.GetMeterProvider() != prevMP {
			otel.SetMeterProvider(prevMP)
		}
	})

	cfg := &config.Config{
		ServerConfig: serverx.ServerConfig{
			ServerHost:         "127.0.0.1",
			ServerPort:         0,
			InternalServerPort: 0,
			ServerReadMaxBytes: 8 << 20,
		},
		EnvConfig: observability.DefaultConfig(),
		Meta: config.MetaConfig{
			StorageDataDirVal: t.TempDir(),
		},
	}
	cfg.LogLevel = "error"
	cfg.AdminAddr = "off"

	var (
		srv *Server
		rt  *observability.Runtime
	)
	app := fx.New(
		fx.Supply(cfg),
		config.Module,
		obsfx.Module,
		fx.Provide(
			obsfx.AsOption(func() observability.Option { return observability.WithLogWriter(io.Discard) }),
			obsfx.AsOption(func() observability.Option { return observability.WithSetDefaultLogger(false) }),
			func() db.Store { return newMemDiskDB() },
		),
		control.Module,
		Module,
		fx.Populate(&srv, &rt),
		fx.NopLogger,
	)

	require.NoError(t, app.Start(ctx))
	require.Nil(t, rt.MeterProvider(), "obsfx Runtime 默认 MeterProvider 必须被抑制以避免二次 Shutdown")

	pubSrv := httptest.NewServer(srv.Handler())
	defer pubSrv.Close()
	client := storagev1connect.NewStorageServiceClient(pubSrv.Client(), pubSrv.URL)
	_, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{}))
	require.NoError(t, err)

	adminMux := http.NewServeMux()
	observability.RegisterAdminRoutes(adminMux, rt.Gatherer(), cfg)
	rec := httptest.NewRecorder()
	adminMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", http.NoBody))
	require.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	require.Contains(t, body, "go_goroutines")
	require.Contains(t, body, "bowl_storage_http_request_duration_seconds_bucket")
	require.Contains(t, body, "rpc_server_call_duration_seconds_bucket")

	require.NoError(t, app.Stop(ctx), "Fx 应用优雅停机必须零错误退出（不得触发 reader is shutdown）")
}
