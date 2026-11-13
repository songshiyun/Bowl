package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/observability/slogx"
	"github.com/starsrc/pkg/observability/telemetry"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/version"
)

const clientServiceName = "bowl-client"

// normalizeOTLPEndpoint 对缺少 URL scheme（http:// 或 https://）的裸 host:port OTLP 地址自动补齐 http://，
// 修复 OTel Go SDK url.Parse("172.16.1.x:4317") 报 "first path segment in URL cannot contain colon" 的缺陷。
func normalizeOTLPEndpoint(raw string) string {
	trimmed := strings.TrimSpace(raw)
	if trimmed == "" {
		return ""
	}
	if !strings.Contains(trimmed, "://") {
		return "http://" + trimmed
	}
	return trimmed
}

// normalizeOTLPEnv 原地归一化进程环境变量中的 OTLP Endpoint（同时覆盖守护子进程继承与 OTel SDK 内部环境读取）。
func normalizeOTLPEnv() {
	for _, key := range []string{"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"} {
		raw := strings.TrimSpace(os.Getenv(key))
		if raw == "" {
			continue
		}
		if normalized := normalizeOTLPEndpoint(raw); normalized != raw {
			_ = os.Setenv(key, normalized)
		}
	}
}

// priorityOpSampler 为低频高价值持久化与生命周期屏障（Fsync / FsyncDir / OnUnmount）提供确定性采样提权，
// 防止默认 0.001 比率将其全部漏采，其余高频操作仍严格遵循基础采样率。
type priorityOpSampler struct {
	base sdktrace.Sampler
}

//nolint:gocritic // sdktrace.Sampler 接口锁定按值传参签名
func (s priorityOpSampler) ShouldSample(p sdktrace.SamplingParameters) sdktrace.SamplingResult {
	switch p.Name {
	case "fuse.Fsync", "fuse.FsyncDir", "fuse.OnUnmount":
		return sdktrace.SamplingResult{
			Decision:   sdktrace.RecordAndSample,
			Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
		}
	}
	for _, link := range p.Links {
		if link.SpanContext.IsSampled() {
			return sdktrace.SamplingResult{
				Decision:   sdktrace.RecordAndSample,
				Tracestate: trace.SpanContextFromContext(p.ParentContext).TraceState(),
			}
		}
	}
	return s.base.ShouldSample(p)
}

func (s priorityOpSampler) Description() string {
	return "PriorityOpSampler{" + s.base.Description() + "}"
}

func newClientTraceSampler(ratio float64) sdktrace.Sampler {
	if ratio <= 0 {
		return sdktrace.ParentBased(sdktrace.NeverSample())
	}
	return sdktrace.ParentBased(priorityOpSampler{
		base: sdktrace.TraceIDRatioBased(ratio),
	})
}

func newClientMetricView() sdkmetric.View {
	return sdkmetric.NewView(
		sdkmetric.Instrument{
			Name: "rpc.client.*",
			Kind: sdkmetric.InstrumentKindHistogram,
		},
		sdkmetric.Stream{
			Aggregation: sdkmetric.AggregationExplicitBucketHistogram{
				Boundaries: rpc.SubMillisecondDurationBuckets,
			},
		},
	)
}

// buildObsConfig 生成客户端可观测性配置：始终启用 Tracing 以生成并透传 W3C traceparent；
// 仅当设置 OTEL_EXPORTER_OTLP_ENDPOINT 时启用 OTLP 导出，否则使用零网络开销的 "none" 导出器。
// 管理面由 startAdminServer 自行监听（需挂载 /debug/stats），故 AdminAddr 置空。
func buildObsConfig(opts *mountOptions) *observability.EnvConfig {
	normalizeOTLPEnv()

	cfg := observability.DefaultConfig()
	cfg.LogConfig = slogx.LogConfig{
		LogLevel:          opts.logLevel,
		LogFormat:         opts.logFormat,
		LogConnectMessage: slogx.DefaultConnectMessage,
	}
	serviceName := strings.TrimSpace(os.Getenv("OTEL_SERVICE_NAME"))
	if serviceName == "" {
		serviceName = clientServiceName
	}
	cfg.OtelEnabled = true
	cfg.OtelServiceName = serviceName
	cfg.OtelServiceVersion = version.Version
	cfg.OtelSampleRatio = opts.traceSampleRatio
	cfg.OtelTracesExporter = telemetry.ExporterNone

	endpoint := firstNonEmptyEnv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint != "" {
		normalized := normalizeOTLPEndpoint(endpoint)
		cfg.OtelTracesExporter = telemetry.ExporterOTLP
		cfg.OtelEndpoint = normalized
		cfg.OtelInsecure = strings.HasPrefix(strings.ToLower(normalized), "http://")
		if rawInsecure := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")); rawInsecure != "" {
			if parsed, err := strconv.ParseBool(rawInsecure); err == nil {
				cfg.OtelInsecure = parsed
			}
		}
	}
	cfg.AdminAddr = ""
	return &cfg
}

// clientRuntime 承载客户端定制的可观测性运行时（含亚毫秒指标桶 View 与持久化屏障提权 Sampler）。
type clientRuntime struct {
	logger         *slog.Logger
	registry       *prometheus.Registry
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	shutdownOnce   sync.Once
	shutdownErr    error
}

func (r *clientRuntime) Logger() *slog.Logger {
	return r.logger
}

func (r *clientRuntime) TracerProvider() *sdktrace.TracerProvider {
	return r.tracerProvider
}

func (r *clientRuntime) RegisterCollector(cs ...prometheus.Collector) error {
	for _, c := range cs {
		if c == nil {
			continue
		}
		if err := r.registry.Register(c); err != nil {
			return err
		}
	}
	return nil
}

func (r *clientRuntime) MountAdminRoutes(mux *http.ServeMux) {
	observability.RegisterAdminRoutes(mux, r.registry, observability.AdminConfig{
		MetricsEnabled: true,
		PprofEnabled:   true,
	})
}

func (r *clientRuntime) Shutdown(ctx context.Context) error {
	r.shutdownOnce.Do(func() {
		var errs []error
		if r.tracerProvider != nil {
			if err := r.tracerProvider.Shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		if r.meterProvider != nil {
			if err := r.meterProvider.Shutdown(ctx); err != nil {
				errs = append(errs, err)
			}
		}
		r.shutdownErr = errors.Join(errs...)
	})
	return r.shutdownErr
}

func newClientTracerProvider(ctx context.Context, cfg *observability.EnvConfig, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	sampler := newClientTraceSampler(cfg.SampleRatio())
	switch cfg.TracesExporter() {
	case telemetry.ExporterNone:
		return sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	case telemetry.ExporterOTLP:
		endpoint := normalizeOTLPEndpoint(cfg.Endpoint())
		otlpOpts := []otlptracegrpc.Option{
			otlptracegrpc.WithEndpointURL(endpoint),
		}
		if cfg.Insecure() && strings.HasPrefix(strings.ToLower(endpoint), "http://") {
			otlpOpts = append(otlpOpts, otlptracegrpc.WithInsecure())
		}
		exporter, err := otlptracegrpc.New(ctx, otlpOpts...)
		if err != nil {
			return nil, fmt.Errorf("创建 OTLP Trace 导出器失败: %w", err)
		}
		return sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(exporter),
			sdktrace.WithResource(res),
			sdktrace.WithSampler(sampler),
		), nil
	default:
		return nil, fmt.Errorf("不支持的链路导出器类型 %q", cfg.TracesExporter())
	}
}

// setupObservability 初始化日志、隔离 Registry、带亚毫秒桶 View 的 MeterProvider 与带屏障提权采样的 TracerProvider。
func setupObservability(ctx context.Context, opts *mountOptions, logWriter io.Writer) (*clientRuntime, error) {
	cfg := buildObsConfig(opts)
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("初始化可观测性失败: %w", err)
	}

	var slogOpts []slogx.Option
	if logWriter != nil {
		slogOpts = append(slogOpts, slogx.WithWriter(logWriter))
	}
	slogOpts = append(slogOpts, slogx.WithSetDefault(true))
	logger := slogx.New(cfg, slogOpts...)

	reg := telemetry.NewRegistry()
	if err := telemetry.RegisterRuntimeCollectors(reg); err != nil {
		return nil, fmt.Errorf("初始化可观测性失败: %w", err)
	}

	res, err := telemetry.NewResource(cfg)
	if err != nil {
		return nil, fmt.Errorf("初始化可观测性失败: %w", err)
	}

	promExporter, err := otelprom.New(
		otelprom.WithRegisterer(reg),
		otelprom.WithoutScopeInfo(),
	)
	if err != nil {
		return nil, fmt.Errorf("初始化可观测性失败: %w", err)
	}

	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExporter),
		sdkmetric.WithView(newClientMetricView()),
	)
	otel.SetMeterProvider(mp)

	tp, err := newClientTracerProvider(ctx, cfg, res)
	if err != nil {
		_ = mp.Shutdown(ctx)
		return nil, fmt.Errorf("初始化可观测性失败: %w", err)
	}
	telemetry.RegisterGlobalTracer(tp)

	return &clientRuntime{
		logger:         logger,
		registry:       reg,
		meterProvider:  mp,
		tracerProvider: tp,
	}, nil
}

// validateAdminAddr 校验管理面地址："off" 表示关闭，其余必须为 host:port。
func validateAdminAddr(addr string) error {
	if strings.EqualFold(addr, adminAddrOff) {
		return nil
	}
	if _, _, err := net.SplitHostPort(addr); err != nil {
		return fmt.Errorf("无效的 --admin-addr \"%s\": %w", addr, err)
	}
	return nil
}

// adminServer 是挂载进程的 loopback 管理面 HTTP 服务；nil 值表示已关闭。
type adminServer struct {
	ln  net.Listener
	srv *http.Server
}

// startAdminServer 在 addr 上启动管理面：/metrics、/debug/pprof/*、/healthz、/readyz 与 /debug/stats。
// addr 为 "off" 时返回 nil（方法对 nil 安全）。
func startAdminServer(addr string, obs *clientRuntime, fs *fusefs.RawFS) (*adminServer, error) {
	if strings.EqualFold(addr, adminAddrOff) {
		return nil, nil
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("监听管理面地址 \"%s\" 失败: %w", addr, err)
	}
	mux := http.NewServeMux()
	obs.MountAdminRoutes(mux)
	mux.Handle("GET /debug/stats", statsHandler(fs))

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		// WriteTimeout 保持 0：/debug/pprof/profile?seconds=N 为长时流式响应。
	}
	go func() {
		if serveErr := srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			obs.Logger().Warn("admin_server_stopped", "error", serveErr.Error())
		}
	}()
	return &adminServer{ln: ln, srv: srv}, nil
}

// addr 返回实际监听地址；关闭时返回 "off"。
func (a *adminServer) addr() string {
	if a == nil {
		return adminAddrOff
	}
	return a.ln.Addr().String()
}

func (a *adminServer) shutdown(ctx context.Context) {
	if a == nil {
		return
	}
	if err := a.srv.Shutdown(ctx); err != nil {
		_ = a.srv.Close()
	}
}

// statsHandler 渲染 FUSE 统计快照：默认 JSON，?format=text 为人类可读文本，?format=stats 为 .stats 键值格式。
func statsHandler(fs *fusefs.RawFS) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		snap := fs.Snapshot()
		switch format := r.URL.Query().Get("format"); format {
		case "", "json":
			body, err := snap.FormatJSON()
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json; charset=utf-8")
			_, _ = w.Write(body)
		case "text":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, snap.FormatText())
		case "stats":
			w.Header().Set("Content-Type", "text/plain; charset=utf-8")
			_, _ = io.WriteString(w, snap.FormatStatsFile())
		default:
			http.Error(w, fmt.Sprintf("unsupported format %q (json|text|stats)", format), http.StatusBadRequest)
		}
	})
}
