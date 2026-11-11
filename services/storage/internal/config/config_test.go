package config_test

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"

	"github.com/starsrc/Aether/pkg/observability/telemetry"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

func TestNormalizeOTLPEndpointAndTracerProviderAcceptsBareHostPort(t *testing.T) {
	require.Empty(t, config.NormalizeOTLPEndpoint("   "))
	require.Equal(t, "http://jaeger:4317", config.NormalizeOTLPEndpoint("jaeger:4317"))
	require.Equal(t, "http://172.16.1.205:4317", config.NormalizeOTLPEndpoint(" 172.16.1.205:4317 "))
	require.Equal(t, "http://alloy:4317", config.NormalizeOTLPEndpoint("http://alloy:4317"))
	require.Equal(t, "https://collector.example.com:4317", config.NormalizeOTLPEndpoint("https://collector.example.com:4317"))

	envFile := filepath.Join(t.TempDir(), "storage.env")
	require.NoError(t, os.WriteFile(envFile, []byte("DB_DSN=postgres://bowl:bowl@127.0.0.1:5432/bowl?sslmode=disable\nOTEL_EXPORTER_OTLP_METRICS_ENDPOINT=alloy:4317\n"), 0o600))
	t.Cleanup(func() { _ = os.Unsetenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") })

	t.Setenv("OTEL_ENABLED", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "otlp")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "jaeger:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "127.0.0.1:4317")

	cfg, err := config.Load(envFile)
	require.NoError(t, err)
	require.Equal(t, "http://jaeger:4317", cfg.OtelEndpoint)
	require.Equal(t, "http://127.0.0.1:4317", cfg.OtelTracesEndpoint)
	require.Equal(t, "http://127.0.0.1:4317", cfg.Endpoint())
	require.Equal(t, "http://jaeger:4317", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	require.Equal(t, "http://127.0.0.1:4317", os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"))
	require.Equal(t, "http://alloy:4317", os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"))

	var (
		errMu    sync.Mutex
		otelErrs []error
	)
	prevHandler := otel.GetErrorHandler()
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(hErr error) {
		errMu.Lock()
		otelErrs = append(otelErrs, hErr)
		errMu.Unlock()
	}))
	defer otel.SetErrorHandler(prevHandler)

	tp, err := telemetry.NewTracerProvider(context.Background(), cfg)
	require.NoError(t, err)
	require.NoError(t, tp.Shutdown(context.Background()))

	errMu.Lock()
	defer errMu.Unlock()
	require.Empty(t, otelErrs, "OTel SDK 初始化裸 host:port 环境变量时不得产生 URL 解析错误")
}
