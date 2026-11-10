package server

import (
	"fmt"
	"log/slog"
	"net/http"

	"connectrpc.com/connect"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/starsrc/Aether/pkg/serverx"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/validator"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
)

var _ storagev1connect.StorageServiceHandler = (*Server)(nil)

const (
	// HTTP2MaxReadFrameSize 为服务端 HTTP/2 接收帧大小上限 (1 MiB)。
	HTTP2MaxReadFrameSize = 1 << 20

	// HTTP2MaxReceiveBufferPerStream 为服务端单流 HTTP/2 接收流控窗口上限 (< 4 MiB)。
	HTTP2MaxReceiveBufferPerStream = (4 << 20) - 1

	// HTTP2MaxReceiveBufferPerConnection 为服务端单连接 HTTP/2 接收流控窗口上限 (< 4 MiB)。
	HTTP2MaxReceiveBufferPerConnection = (4 << 20) - 1
)

// Server 封装并管理底层 HTTP 服务与 ConnectRPC 路由处理器。
type Server struct {
	storagev1connect.UnimplementedStorageServiceHandler
	control    control.Manager
	config     config.ServerConfig
	logger     *slog.Logger
	httpServer *http.Server
}

// Handler 返回 HTTP 处理器实例，便于测试与中间件挂载。
func (s *Server) Handler() http.Handler {
	if s.httpServer != nil {
		return s.httpServer.Handler
	}
	return nil
}

// HTTPServer 返回底层 HTTP 服务器实例。
func (s *Server) HTTPServer() *http.Server {
	return s.httpServer
}

// Control 返回持有的单盘控制面与 StoreActor 实例池管理器。
func (s *Server) Control() control.Manager {
	return s.control
}

// Addr 返回服务器监听的网络地址。
func (s *Server) Addr() string {
	if s.httpServer != nil {
		return s.httpServer.Addr
	}
	return s.config.Addr()
}

// Params 定义 Server 初始化所需的依赖注入参数。
type Params struct {
	fx.In

	Config        config.ServerConfig
	Control       control.Manager
	Observability serverx.ObservabilityConfig `optional:"true"`
	Telemetry     telemetry.Config            `optional:"true"`
	ObsReady      observability.Ready         `optional:"true"`
	Logger        *slog.Logger                `optional:"true"`
	Lifecycle     fx.Lifecycle                `optional:"true"`
	Shutdowner    fx.Shutdowner               `optional:"true"`
}

// New 构造 Server 实例，组装洋葱拦截器链与 ConnectRPC 处理器，并按需在 Fx 生命周期中注册启停钩子。
//
//nolint:gocritic // hugeParam: Uber Fx fx.In 参数结构要求按值传入构造器。
func New(p Params) (*Server, error) {
	if p.Config.Port() < 0 || p.Config.Port() > 65535 {
		return nil, fmt.Errorf("invalid server port %d: must be between 0 and 65535", p.Config.Port())
	}

	obsCfg := p.Observability
	if obsCfg == nil && p.Telemetry != nil {
		obsCfg = toObservabilityConfig(p.Telemetry)
	}

	val, err := validator.NewStorageValidator()
	if err != nil {
		return nil, fmt.Errorf("create storage validator: %w", err)
	}
	valInterceptor := NewValidationInterceptor(val)

	interceptors, err := serverx.NewInterceptorsWithValidator(
		quietExpectedNotFound(p.Logger),
		obsCfg,
		valInterceptor,
	)
	if err != nil {
		return nil, fmt.Errorf("build interceptors: %w", err)
	}

	srv := &Server{
		control: p.Control,
		config:  p.Config,
		logger:  quietExpectedNotFound(p.Logger),
	}

	mux := http.NewServeMux()
	var handlerOpts []connect.HandlerOption
	handlerOpts = append(handlerOpts, connect.WithInterceptors(interceptors...))
	if p.Config.ReadMaxBytes() > 0 {
		handlerOpts = append(handlerOpts, connect.WithReadMaxBytes(int(p.Config.ReadMaxBytes())))
	}

	path, handler := storagev1connect.NewStorageServiceHandler(
		srv,
		handlerOpts...,
	)
	mux.Handle(path, handler)

	httpServer := serverx.NewHTTPServer(p.Config.Addr(), mux, p.Config)
	httpServer.HTTP2 = &http.HTTP2Config{
		MaxReadFrameSize:              HTTP2MaxReadFrameSize,
		MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
		MaxReceiveBufferPerConnection: HTTP2MaxReceiveBufferPerConnection,
	}
	srv.httpServer = httpServer

	if p.Lifecycle != nil && p.Shutdowner != nil {
		serverx.RegisterSingleLifecycle(p.Lifecycle, p.Shutdowner, p.Config, "server", httpServer)
	}

	return srv, nil
}
