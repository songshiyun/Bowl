package server

import (
	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/observability/obsfx"
)

func provideHTTPMetricsCollector(m *HTTPMetrics) prometheus.Collector {
	return m
}

// Module 提供 Server 构造、外层 HTTP 指标采集器与精细桶 OTel MeterProvider 装配。
var Module = fx.Module("server",
	fx.Provide(
		NewHTTPMetrics,
		obsfx.AsCollector(provideHTTPMetricsCollector),
		ConfigureFineBucketMeterProvider,
		New,
	),
	fx.Invoke(func(*Server) {}),
)
