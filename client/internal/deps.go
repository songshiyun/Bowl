// Package internal 锁定 client 子模块的核心第三方依赖。
package internal

import (
	// 锁定 client 子模块的核心依赖项。
	_ "connectrpc.com/connect"
	_ "connectrpc.com/otelconnect"
	_ "github.com/google/uuid"
	_ "github.com/hanwen/go-fuse/v2/fuse"
	_ "github.com/prometheus/client_golang/prometheus"
	_ "github.com/prometheus/client_model/go"
	_ "github.com/starsrc/Aether/pkg/observability"
	_ "github.com/starsrc/Aether/pkg/observability/slogx"
	_ "github.com/starsrc/Aether/pkg/observability/telemetry"
	_ "github.com/starsrc/Aether/pkg/serverx"
	_ "go.opentelemetry.io/otel"
	_ "go.opentelemetry.io/otel/attribute"
	_ "go.opentelemetry.io/otel/codes"
	_ "go.opentelemetry.io/otel/metric"
	_ "go.opentelemetry.io/otel/trace"
	_ "golang.org/x/net/http2"
	_ "golang.org/x/sys/unix"

	// 锁定生成的 Protobuf 与 Connect 契约包依赖。
	_ "github.com/starsrc/bowl/gen/go/storage/v1"
	_ "github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)
