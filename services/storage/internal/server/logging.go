package server

import (
	"context"
	"log/slog"

	"connectrpc.com/connect"

	"github.com/starsrc/Aether/pkg/observability/slogx"
)

// expectedNotFoundHandler 把预期内的 POSIX 未命中访问日志降级为 DEBUG。
//
// Lookup 负向探测（ENOENT）与 GetXattr 查询不存在的扩展属性（ENODATA）都是
// 正常业务语义，Aether 默认分级会把 connect.CodeNotFound 记为 WARN。高并发下
// 同步写日志会阻塞 RPC 线程，因此这里仅在日志属性 code == "not_found" 时降级，
// 其余非预期错误仍保持 WARN/ERROR。
type expectedNotFoundHandler struct {
	slog.Handler
}

// quietExpectedNotFound 用预期未命中降级处理器包装 logger。
func quietExpectedNotFound(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.Default()
	}
	return slog.New(expectedNotFoundHandler{Handler: logger.Handler()})
}

//nolint:gocritic // hugeParam: 标准库 slog.Handler 接口契约要求按值接收 slog.Record。
func (h expectedNotFoundHandler) Handle(ctx context.Context, record slog.Record) error {
	if record.Level >= slog.LevelWarn && recordCode(&record) == connect.CodeNotFound.String() {
		if !h.Enabled(ctx, slog.LevelDebug) {
			return nil
		}
		record.Level = slog.LevelDebug
	}
	return h.Handler.Handle(ctx, record)
}

func (h expectedNotFoundHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return expectedNotFoundHandler{Handler: h.Handler.WithAttrs(attrs)}
}

func (h expectedNotFoundHandler) WithGroup(name string) slog.Handler {
	return expectedNotFoundHandler{Handler: h.Handler.WithGroup(name)}
}

// recordCode 提取访问日志中的 Connect 状态码属性。
func recordCode(record *slog.Record) string {
	code := ""
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == slogx.DefaultCodeKey {
			code = attr.Value.String()
			return false
		}
		return true
	})
	return code
}
