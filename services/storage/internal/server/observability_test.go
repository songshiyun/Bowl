package server_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime/pprof"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability"
	"github.com/starsrc/Aether/pkg/observability/obsfx"
	"github.com/starsrc/Aether/pkg/observability/slogx"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/server"
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

func (b *syncLogBuffer) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.buf.Reset()
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

type memSpanExporter struct {
	mu    sync.Mutex
	spans []sdktrace.ReadOnlySpan
}

func (e *memSpanExporter) ExportSpans(_ context.Context, spans []sdktrace.ReadOnlySpan) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = append(e.spans, spans...)
	return nil
}

func (e *memSpanExporter) Shutdown(context.Context) error { return nil }

func (e *memSpanExporter) Reset() {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.spans = nil
}

func (e *memSpanExporter) Spans() []sdktrace.ReadOnlySpan {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]sdktrace.ReadOnlySpan, len(e.spans))
	copy(out, e.spans)
	return out
}

func freeLoopbackPort(t *testing.T) int {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	tcpAddr, ok := ln.Addr().(*net.TCPAddr)
	require.True(t, ok)
	port := tcpAddr.Port
	require.NoError(t, ln.Close())
	return port
}

func jsonUint64(v uint64) json.Number {
	return json.Number(strconv.FormatUint(v, 10))
}

func waitForHTTPStatus(t *testing.T, url string, expectedStatus int) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	var lastErr error
	var lastStatus int
	for time.Now().Before(deadline) {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, http.NoBody)
		require.NoError(t, err)
		resp, err := client.Do(req)
		if err == nil {
			body, readErr := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if readErr == nil && resp.StatusCode == expectedStatus {
				return string(body)
			}
			lastStatus = resp.StatusCode
			lastErr = readErr
		} else {
			lastErr = err
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("等待 %s 返回状态码 %d 超时 (lastStatus=%d, lastErr=%v)", url, expectedStatus, lastStatus, lastErr)
	return ""
}

func TestObservability_SingleLogAndNotFoundClassification(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "srv_obs_log")
	dbStore := db.NewStore(pool)
	mgr := control.New(dbStore, control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	startServerWithLevel := func(t *testing.T, level string) (storagev1connect.StorageServiceClient, *syncLogBuffer) {
		t.Helper()
		logBuf := &syncLogBuffer{}
		cfg := &config.Config{
			EnvConfig: observability.DefaultConfig(),
		}
		cfg.ServerHost = "127.0.0.1"
		cfg.ServerPort = freeLoopbackPort(t)
		cfg.InternalServerHost = "127.0.0.1"
		cfg.InternalServerPort = freeLoopbackPort(t)
		cfg.ServerReadMaxBytes = config.DefaultServerReadMaxBytes
		cfg.LogLevel = level
		cfg.LogFormat = "json"
		cfg.LogConnectMessage = "handled rpc"
		cfg.ApplyDefaultServiceName("bowl-storage-test")
		cfg.ApplyDefaultTrustRemote(true)

		var srv *server.Server
		app := fx.New(
			fx.Supply(cfg),
			fx.Provide(func() control.Manager { return mgr }),
			fx.Supply(pool),
			config.Module,
			obsfx.Module,
			fx.Provide(obsfx.AsOption(func() observability.Option {
				return observability.WithLogWriter(logBuf)
			})),
			fx.Provide(obsfx.AsOption(func() observability.Option {
				return observability.WithSetDefaultLogger(false)
			})),
			server.Module,
			fx.Populate(&srv),
			fx.NopLogger,
		)
		startCtx, cancelStart := context.WithTimeout(ctx, 5*time.Second)
		defer cancelStart()
		require.NoError(t, app.Start(startCtx))
		t.Cleanup(func() {
			stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancelStop()
			require.NoError(t, app.Stop(stopCtx))
		})

		waitForHTTPStatus(t, "http://"+srv.AdminAddr()+"/readyz", http.StatusOK)
		client := storagev1connect.NewStorageServiceClient(&http.Client{Timeout: 5 * time.Second}, "http://"+srv.Addr())
		return client, logBuf
	}

	// 1. INFO 级别下验证单次成功日志唯一性、业务字段提取与 Lookup/GetXattr NotFound 静默降级
	infoClient, infoBuf := startServerWithLevel(t, "info")

	createDiskResp, err := infoClient.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "obs-log-disk",
		MaxBytes: 4 << 20,
	}))
	require.NoError(t, err)
	diskID := createDiskResp.Msg.GetDisk().GetDiskId()

	mountResp, err := infoClient.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch)

	createFileResp, err := infoClient.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1,
		Name:      []byte("probe.txt"),
		Mode:      0o644,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := createFileResp.Msg.GetInode().GetIno()

	// ① 实证：单次成功只读 RPC (GetAttr) 严格只输出 1 条 "handled rpc" 日志，含 disk_id/ino 且不含零值 epoch
	infoBuf.Reset()
	_, err = infoClient.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    fileIno,
	}))
	require.NoError(t, err)

	entries := infoBuf.Entries(t)
	require.Len(t, entries, 1, "单次成功 RPC 必须严格只输出 1 条日志")
	require.Equal(t, "handled rpc", entries[0]["msg"])
	require.Equal(t, "INFO", entries[0]["level"])
	require.Equal(t, "ok", entries[0]["code"])
	require.Equal(t, storagev1connect.StorageServiceGetAttrProcedure, entries[0]["procedure"])
	require.Equal(t, diskID, entries[0]["disk_id"])
	require.Equal(t, jsonUint64(fileIno), entries[0]["ino"])
	_, hasEpoch := entries[0]["epoch"]
	require.False(t, hasEpoch, "epoch 为 0 的只读 RPC 不应附加零值 epoch")

	// 单次成功写 RPC (Write) 严格输出 1 条日志，且同时包含 disk_id、epoch、ino 与入站 X-Request-Id
	infoBuf.Reset()
	writeReq := connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Data:   []byte("abc"),
	})
	writeReq.Header().Set("X-Request-Id", "42")
	_, err = infoClient.Write(ctx, writeReq)
	require.NoError(t, err)

	writeEntries := infoBuf.Entries(t)
	require.Len(t, writeEntries, 1)
	require.Equal(t, "handled rpc", writeEntries[0]["msg"])
	require.Equal(t, diskID, writeEntries[0]["disk_id"])
	require.Equal(t, jsonUint64(epoch), writeEntries[0]["epoch"])
	require.Equal(t, jsonUint64(fileIno), writeEntries[0]["ino"])
	require.Equal(t, "42", writeEntries[0]["request_id"])

	// ② 实证：Lookup 与 GetXattr 返回 CodeNotFound 在 INFO 级别下输出 0 条 WARN/INFO 日志
	infoBuf.Reset()
	_, err = infoClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte("non-existent-entry"),
	}))
	requireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	_, err = infoClient.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Name:   []byte("user.missing"),
	}))
	requireConnectError(t, err, connect.CodeNotFound, "ENODATA")
	require.Empty(t, infoBuf.Entries(t), "INFO 级别下 Lookup/GetXattr 未命中必须输出 0 条日志")

	// 反向守门：非豁免方法（如 GetAttr 查询不存在的 Inode）返回 CodeNotFound 在 INFO 下仍输出 1 条 WARN
	_, err = infoClient.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    999999,
	}))
	requireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	warnEntries := infoBuf.Entries(t)
	require.Len(t, warnEntries, 1)
	require.Equal(t, "WARN", warnEntries[0]["level"])
	require.Equal(t, "ENOENT", warnEntries[0]["error_reason"])

	// 2. 开启 DEBUG 级别后，Lookup 与 GetXattr 返回 CodeNotFound 各输出 1 条 DEBUG 日志
	debugClient, debugBuf := startServerWithLevel(t, "debug")

	debugBuf.Reset()
	_, err = debugClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte("non-existent-entry"),
	}))
	requireConnectError(t, err, connect.CodeNotFound, "ENOENT")

	debugLookupEntries := debugBuf.Entries(t)
	require.Len(t, debugLookupEntries, 1)
	require.Equal(t, "DEBUG", debugLookupEntries[0]["level"])
	require.Equal(t, "handled rpc", debugLookupEntries[0]["msg"])
	require.Equal(t, "ENOENT", debugLookupEntries[0]["error_reason"])
	require.Equal(t, diskID, debugLookupEntries[0]["disk_id"])

	debugBuf.Reset()
	_, err = debugClient.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Name:   []byte("user.missing"),
	}))
	requireConnectError(t, err, connect.CodeNotFound, "ENODATA")

	debugXattrEntries := debugBuf.Entries(t)
	require.Len(t, debugXattrEntries, 1)
	require.Equal(t, "DEBUG", debugXattrEntries[0]["level"])
	require.Equal(t, "ENODATA", debugXattrEntries[0]["error_reason"])
	require.Equal(t, diskID, debugXattrEntries[0]["disk_id"])
	require.Equal(t, jsonUint64(fileIno), debugXattrEntries[0]["ino"])
}

func TestObservability_ErrorReasonMetricsAndDualPortAdminIsolation(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "srv_obs_admin")
	dbStore := db.NewStore(pool)
	mgr := control.New(dbStore, control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	// 兼容性前置验证：裸 ServerConfig (InternalServerPort == 0) 不构造管理面服务
	var singleCfg config.ServerConfig
	singleCfg.ApplyDefaultPort(0)
	singleSrv, err := server.New(server.Params{
		Config:  singleCfg,
		Control: mgr,
	})
	require.NoError(t, err)
	require.Nil(t, singleSrv.AdminHTTPServer())
	require.Empty(t, singleSrv.AdminAddr())

	// 双端口完整装配（含 obsfx.Module 隔离 Registry、MeterProvider 与 Postgres ReadyCheck）
	logBuf := &syncLogBuffer{}
	cfg := &config.Config{
		EnvConfig: observability.DefaultConfig(),
	}
	cfg.ServerHost = "127.0.0.1"
	cfg.ServerPort = freeLoopbackPort(t)
	cfg.InternalServerHost = "127.0.0.1"
	cfg.InternalServerPort = freeLoopbackPort(t)
	if cfg.InternalServerPort == cfg.ServerPort {
		cfg.InternalServerPort = freeLoopbackPort(t)
	}
	cfg.ServerReadMaxBytes = config.DefaultServerReadMaxBytes
	cfg.EnvConfig.MetricsEnabled = true
	cfg.PprofEnabled = true
	cfg.ApplyDefaultServiceName("bowl-storage-metrics")
	cfg.ApplyDefaultTrustRemote(true)

	var srv *server.Server
	app := fx.New(
		fx.Supply(cfg),
		fx.Provide(func() control.Manager { return mgr }),
		fx.Supply(pool),
		config.Module,
		obsfx.Module,
		fx.Provide(obsfx.AsOption(func() observability.Option {
			return observability.WithLogWriter(logBuf)
		})),
		fx.Provide(obsfx.AsOption(func() observability.Option {
			return observability.WithSetDefaultLogger(false)
		})),
		server.Module,
		fx.Populate(&srv),
		fx.NopLogger,
	)
	startCtx, cancelStart := context.WithTimeout(ctx, 5*time.Second)
	defer cancelStart()
	require.NoError(t, app.Start(startCtx))
	t.Cleanup(func() {
		stopCtx, cancelStop := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancelStop()
		require.NoError(t, app.Stop(stopCtx))
	})

	require.NotNil(t, srv.AdminHTTPServer())
	publicBase := "http://" + srv.Addr()
	adminBase := "http://" + srv.AdminAddr()

	// ④ 实证：管理面 /metrics、/debug/pprof/cmdline、/healthz、/readyz 全部返回 HTTP 200
	readyBody := waitForHTTPStatus(t, adminBase+"/readyz", http.StatusOK)
	require.Contains(t, readyBody, `"postgres":"ok"`)
	healthBody := waitForHTTPStatus(t, adminBase+"/healthz", http.StatusOK)
	require.Contains(t, healthBody, `"status":"ok"`)
	_ = waitForHTTPStatus(t, adminBase+"/debug/pprof/cmdline", http.StatusOK)
	_ = waitForHTTPStatus(t, adminBase+"/metrics", http.StatusOK)

	// 公网端口请求管理端点必须全部返回 HTTP 404（物理隔离）
	for _, adminPath := range []string{"/metrics", "/debug/pprof/cmdline", "/healthz", "/readyz"} {
		_ = waitForHTTPStatus(t, publicBase+adminPath, http.StatusNotFound)
	}

	// ③ 实证：触发 EBUSY 错误时，日志含 error_reason="EBUSY"，且 /metrics 的 rpc_server_call_duration 含 rpc_error_reason="EBUSY"
	client := storagev1connect.NewStorageServiceClient(&http.Client{Timeout: 5 * time.Second}, publicBase)
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "ebusy-metrics-disk",
		MaxBytes: 4 << 20,
	}))
	require.NoError(t, err)
	diskID := diskResp.Msg.GetDisk().GetDiskId()

	mount1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	staleEpoch := mount1.Msg.GetEpoch()

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1,
		Name:      []byte("fenced.bin"),
		Mode:      0o644,
		DiskId:    diskID,
		Epoch:     staleEpoch,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	// 第二次可写挂载使 staleEpoch=1 过期
	_, err = client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)

	logBuf.Reset()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  staleEpoch,
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Data:   []byte("stale-write"),
	}))
	requireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	ebusyLogs := logBuf.Entries(t)
	require.Len(t, ebusyLogs, 1)
	require.Equal(t, "WARN", ebusyLogs[0]["level"])
	require.Equal(t, "EBUSY", ebusyLogs[0]["error_reason"])
	require.Equal(t, diskID, ebusyLogs[0]["disk_id"])
	require.Equal(t, jsonUint64(staleEpoch), ebusyLogs[0]["epoch"])
	require.Equal(t, jsonUint64(fileIno), ebusyLogs[0]["ino"])

	metricsText := waitForHTTPStatus(t, adminBase+"/metrics", http.StatusOK)
	var matchedEBUSYDuration bool
	for line := range strings.SplitSeq(metricsText, "\n") {
		if strings.HasPrefix(line, "rpc_server_call_duration") && strings.Contains(line, `rpc_error_reason="EBUSY"`) {
			matchedEBUSYDuration = true
			break
		}
	}
	require.True(t, matchedEBUSYDuration, "/metrics 的 rpc_server_call_duration 必须包含 rpc_error_reason=\"EBUSY\" 标签:\n%s", metricsText)

	// 基数爆炸防线实证：disk_id 与 ino 绝不能泄露进 Prometheus 标签集
	require.NotContains(t, metricsText, diskID, "高基数 disk_id 严禁注入指标标签")
	require.NotContains(t, metricsText, "disk_id=")
	require.NotContains(t, metricsText, "bowl_disk_id=")
	require.NotContains(t, metricsText, "bowl_ino=")
}

func TestObservability_SpanAttrsPprofSlowLogAndContextShortCircuit(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "srv_obs_trace")
	dbStore := db.NewStore(pool)
	mgr := control.New(dbStore, control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	// 1. Span 属性录制验证（bowl.disk_id / bowl.epoch / bowl.ino）与未采样跳过验证
	exporter := &memSpanExporter{}
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.AlwaysSample()))
	prevTP := otel.GetTracerProvider()
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		_ = tp.Shutdown(context.Background())
	})

	logBuf := &syncLogBuffer{}
	logger := slogx.New(slogx.LogConfig{
		LogLevel:          "info",
		LogFormat:         "json",
		LogConnectMessage: "handled rpc",
	}, slogx.WithWriter(logBuf), slogx.WithSetDefault(false))

	var srvCfg config.ServerConfig
	srvCfg.ApplyDefaultPort(0)
	srvCfg.ServerReadMaxBytes = config.DefaultServerReadMaxBytes

	srv, err := server.New(server.Params{
		Config:  srvCfg,
		Control: mgr,
		Logger:  logger,
	})
	require.NoError(t, err)

	httpSrv := httptest.NewServer(srv.Handler())
	t.Cleanup(httpSrv.Close)
	client := storagev1connect.NewStorageServiceClient(httpSrv.Client(), httpSrv.URL)

	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "span-disk",
		MaxBytes: 4 << 20,
	}))
	require.NoError(t, err)
	diskID := diskResp.Msg.GetDisk().GetDiskId()

	mountResp, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch := mountResp.Msg.GetEpoch()

	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: 1,
		Name:      []byte("traced.txt"),
		Mode:      0o644,
		DiskId:    diskID,
		Epoch:     epoch,
	}))
	require.NoError(t, err)
	fileIno := createResp.Msg.GetInode().GetIno()

	exporter.Reset()
	tracedWriteReq := connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Data:   []byte("span-payload"),
	})
	tracedWriteReq.Header().Set("X-Request-Id", "1001")
	_, err = client.Write(ctx, tracedWriteReq)
	require.NoError(t, err)

	spans := exporter.Spans()
	require.NotEmpty(t, spans)
	attrMap := make(map[attribute.Key]attribute.Value)
	for _, sp := range spans {
		for _, kv := range sp.Attributes() {
			attrMap[kv.Key] = kv.Value
		}
	}
	require.Equal(t, diskID, attrMap["bowl.disk_id"].AsString())
	require.Equal(t, int64(epoch), attrMap["bowl.epoch"].AsInt64())
	require.Equal(t, int64(fileIno), attrMap["bowl.ino"].AsInt64())
	require.Equal(t, "1001", attrMap["bowl.request_id"].AsString())

	// 未采样时验证 0 Span 导出（otelconnect 在 server.New 构造拦截器时捕获 TracerProvider）
	neverTP := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter), sdktrace.WithSampler(sdktrace.NeverSample()))
	otel.SetTracerProvider(neverTP)
	t.Cleanup(func() { _ = neverTP.Shutdown(context.Background()) })

	neverSrv, err := server.New(server.Params{
		Config:  srvCfg,
		Control: mgr,
		Logger:  logger,
	})
	require.NoError(t, err)
	neverHTTPSrv := httptest.NewServer(neverSrv.Handler())
	t.Cleanup(neverHTTPSrv.Close)
	neverClient := storagev1connect.NewStorageServiceClient(neverHTTPSrv.Client(), neverHTTPSrv.URL)

	exporter.Reset()
	_, err = neverClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    diskID,
		ParentIno: 1,
		Name:      []byte("traced.txt"),
	}))
	require.NoError(t, err)
	require.Empty(t, exporter.Spans())

	// 2. pprof.Do("rpc", procedure) 标签与 >50ms 慢操作日志（数据面告警 + 控制面豁免）
	obsInterceptor := server.NewStorageObservabilityInterceptor(logger)

	var observedRPC string
	slowDataNext := obsInterceptor(func(pprofCtx context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
		lbl, ok := pprof.Label(pprofCtx, "rpc")
		if ok {
			observedRPC = lbl
		}
		time.Sleep(60 * time.Millisecond)
		return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
	})

	logBuf.Reset()
	readReq := connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Size:   4,
	})
	// 通过真实 HTTP Handler 验证 Spec.Procedure 填充与慢日志触发
	slowMux := http.NewServeMux()
	slowMux.Handle(storagev1connect.NewStorageServiceHandler(
		&slowStubHandler{
			Server:       srv,
			sleepDur:     60 * time.Millisecond,
			onPprofLabel: func(v string) { observedRPC = v },
		},
		connect.WithInterceptors(obsInterceptor),
	))
	slowHTTPSrv := httptest.NewServer(slowMux)
	t.Cleanup(slowHTTPSrv.Close)
	slowClient := storagev1connect.NewStorageServiceClient(slowHTTPSrv.Client(), slowHTTPSrv.URL)

	logBuf.Reset()
	_, err = slowClient.Read(ctx, readReq)
	require.NoError(t, err)
	require.Equal(t, storagev1connect.StorageServiceReadProcedure, observedRPC)

	slowLogs := logBuf.Entries(t)
	require.Len(t, slowLogs, 1)
	require.Equal(t, "WARN", slowLogs[0]["level"])
	require.Equal(t, "slow storage rpc", slowLogs[0]["msg"])
	require.Equal(t, storagev1connect.StorageServiceReadProcedure, slowLogs[0]["procedure"])
	require.Equal(t, diskID, slowLogs[0]["disk_id"])
	require.Equal(t, jsonUint64(fileIno), slowLogs[0]["ino"])

	// 控制面 CreateDisk > 50ms 不应触发 "slow storage rpc"
	logBuf.Reset()
	_, err = slowClient.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "slow-ctrl-exempt",
		MaxBytes: 1 << 20,
	}))
	require.NoError(t, err)
	require.Empty(t, logBuf.Entries(t), "控制面 RPC 不应误报 slow storage rpc")
	_ = slowDataNext

	// 3. 已取消 / 已超时 ctx 在进入 actor.* 前快速短路验证（且保持底层状态零变更）
	canceledCtx, cancelNow := context.WithCancel(ctx)
	cancelNow()

	_, err = srv.Write(canceledCtx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Data:   []byte("must-not-be-written"),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeCanceled, connect.CodeOf(err))
	require.ErrorIs(t, err, context.Canceled)

	// 独立模型不变量校验：此前写入的是 "span-payload"，取消的写入绝不能污染文件内容
	readBack, err := srv.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    fileIno,
		Fh:     1,
		Offset: 0,
		Size:   64,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("span-payload"), readBack.Msg.GetData())

	expiredCtx, cancelExpired := context.WithDeadline(ctx, time.Now().Add(-time.Second))
	defer cancelExpired()
	newMode := uint32(0o600)
	_, err = srv.SetAttr(expiredCtx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: diskID,
		Epoch:  epoch,
		Ino:    fileIno,
		Mode:   &newMode,
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	require.ErrorIs(t, err, context.DeadlineExceeded)
}

type slowStubHandler struct {
	*server.Server
	sleepDur     time.Duration
	onPprofLabel func(string)
}

func (h *slowStubHandler) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	if lbl, ok := pprof.Label(ctx, "rpc"); ok && h.onPprofLabel != nil {
		h.onPprofLabel(lbl)
	}
	time.Sleep(h.sleepDur)
	return h.Server.Read(ctx, req)
}

func (h *slowStubHandler) CreateDisk(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	time.Sleep(h.sleepDur)
	return h.Server.CreateDisk(ctx, req)
}

func TestReadyzFailsWhenPostgresPoolClosed(t *testing.T) {
	pool, _ := setupIsolatedTestDB(t, "srv_readyz_fail")
	dbStore := db.NewStore(pool)
	mgr := control.New(dbStore, control.WithStorageRoot(t.TempDir()))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})

	// 单独获取一个专供探针关闭测试的独立连接池，避免影响主连接池 cleanup
	probePool, _ := setupIsolatedTestDB(t, "srv_readyz_probe")

	var srvCfg config.ServerConfig
	srvCfg.ServerHost = "127.0.0.1"
	srvCfg.ServerPort = 0
	srvCfg.InternalServerHost = "127.0.0.1"
	srvCfg.InternalServerPort = freeLoopbackPort(t)

	srv, err := server.New(server.Params{
		Config:   srvCfg,
		Control:  mgr,
		AdminCfg: observability.AdminConfig{MetricsEnabled: true, PprofEnabled: true},
		Pool:     probePool,
	})
	require.NoError(t, err)

	adminTestSrv := httptest.NewServer(srv.AdminHandler())
	t.Cleanup(adminTestSrv.Close)

	_ = waitForHTTPStatus(t, adminTestSrv.URL+"/readyz", http.StatusOK)
	probePool.Close()
	failBody := waitForHTTPStatus(t, adminTestSrv.URL+"/readyz", http.StatusServiceUnavailable)
	require.Contains(t, failBody, `"status":"unavailable"`)
}
