package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/starsrc/Aether/pkg/observability"
	"github.com/starsrc/Aether/pkg/observability/slogx"
	"github.com/starsrc/Aether/pkg/observability/telemetry"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/version"
)

const clientServiceName = "bowl-client"

// buildObsConfig 生成客户端可观测性配置：始终启用 Tracing 以生成并透传 W3C traceparent；
// 仅当设置 OTEL_EXPORTER_OTLP_ENDPOINT 时启用 OTLP 导出，否则使用零网络开销的 "none" 导出器。
// 管理面由 startAdminServer 自行监听（需挂载 /debug/stats），故 AdminAddr 置空。
func buildObsConfig(opts *mountOptions) *observability.EnvConfig {
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
	if endpoint := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")); endpoint != "" {
		cfg.OtelTracesExporter = telemetry.ExporterOTLP
		cfg.OtelEndpoint = endpoint
		if rawInsecure := strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")); rawInsecure != "" {
			if parsed, err := strconv.ParseBool(rawInsecure); err == nil {
				cfg.OtelInsecure = parsed
			}
		}
	}
	cfg.AdminAddr = ""
	return &cfg
}

// setupObservability 初始化日志、隔离 Registry、MeterProvider 与 TracerProvider。
func setupObservability(ctx context.Context, opts *mountOptions, logWriter io.Writer) (*observability.Runtime, error) {
	obs, err := observability.Setup(ctx, buildObsConfig(opts), observability.WithLogWriter(logWriter))
	if err != nil {
		return nil, fmt.Errorf("初始化可观测性失败: %w", err)
	}
	return obs, nil
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
func startAdminServer(addr string, obs *observability.Runtime, fs *fusefs.InstrumentedRawFS) (*adminServer, error) {
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
func statsHandler(fs *fusefs.InstrumentedRawFS) http.Handler {
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
