package store

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

const (
	defaultPebbleDiskSlowThreshold = 500 * time.Millisecond
	defaultPebbleSlowJobThreshold  = 500 * time.Millisecond
)

var _ pebble.Logger = (*slogPebbleLogger)(nil)

// slogPebbleLogger 将 Pebble 内部日志桥接至结构化 slog.Logger：
//   - Infof 降噪映射至 Debug，未开启 Debug 时跳过格式化（0 alloc）；
//   - Warnf / Errorf 映射至 Warn / Error；
//   - Fatalf 输出 Error 日志后执行 os.Exit(1) 保持 Pebble 数据损坏 fail-fast 契约。
type slogPebbleLogger struct {
	logger *slog.Logger
	exitFn func(int)
}

func newSlogPebbleLogger(base *slog.Logger, diskID string) *slogPebbleLogger {
	if base == nil {
		base = slog.Default()
	}
	return &slogPebbleLogger{
		logger: base.With("component", "pebble", "disk_id", diskID),
		exitFn: os.Exit,
	}
}

func formatPebbleMsg(format string, args ...any) string {
	return strings.TrimRight(fmt.Sprintf(format, args...), "\r\n")
}

func (l *slogPebbleLogger) Infof(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	if !l.logger.Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	l.logger.Debug(formatPebbleMsg(format, args...))
}

func (l *slogPebbleLogger) Warnf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Warn(formatPebbleMsg(format, args...))
}

func (l *slogPebbleLogger) Errorf(format string, args ...any) {
	if l == nil || l.logger == nil {
		return
	}
	l.logger.Error(formatPebbleMsg(format, args...))
}

func (l *slogPebbleLogger) Fatalf(format string, args ...any) {
	if l != nil && l.logger != nil {
		l.logger.Error(formatPebbleMsg(format, args...), "fatal", true)
	}
	if l != nil && l.exitFn != nil {
		l.exitFn(1)
		return
	}
	os.Exit(1)
}

// configurePebbleObservability 为 Pebble 实例挂载结构化日志器、事件监听器与 500ms 慢盘健康检测包装。
// 调用方必须在 StoreActor 关闭（或 Open 中途出错回滚）时显式关闭返回的 io.Closer 以停止后台健康检测协程。
func (s *StoreActor) configurePebbleObservability(pebbleOpts *pebble.Options, baseLogger *slog.Logger, diskSlowThreshold time.Duration) io.Closer {
	pLogger := newSlogPebbleLogger(baseLogger, s.diskID)
	pebbleOpts.Logger = pLogger

	if diskSlowThreshold <= 0 {
		diskSlowThreshold = defaultPebbleDiskSlowThreshold
	}

	onDiskSlow := func(info vfs.DiskSlowInfo) {
		// DiskSlow 在监控协程中同步回调，严禁获取任何可能等待 I/O 的锁。
		s.metrics.incPebbleDiskSlow()
		pLogger.logger.Warn("pebble disk slow",
			"op", info.OpType.String(),
			"file", filepath.Base(info.Path),
			"write_bytes", info.WriteSize,
			"duration_ms", info.Duration.Milliseconds(),
		)
	}

	listener := pebble.EventListener{
		WriteStallBegin: func(info pebble.WriteStallBeginInfo) {
			s.pebbleStallCount.Add(1)
			s.metrics.incPebbleStallEvents()
			pLogger.logger.Warn("pebble write stall begin", "reason", info.Reason)
		},
		WriteStallEnd: func() {
			for {
				cur := s.pebbleStallCount.Load()
				if cur <= 0 {
					break
				}
				if s.pebbleStallCount.CompareAndSwap(cur, cur-1) {
					break
				}
			}
			pLogger.logger.Warn("pebble write stall end")
		},
		DiskSlow: onDiskSlow,
		BackgroundError: func(err error) {
			pLogger.logger.Error("pebble background error", "err", err)
		},
		DataCorruption: func(info pebble.DataCorruptionInfo) {
			pLogger.logger.Error("pebble data corruption",
				"path", info.Path,
				"is_remote", info.IsRemote,
				"err", info.Details,
			)
			pLogger.Fatalf("data corruption: %s", info)
		},
		FlushEnd: func(info pebble.FlushInfo) {
			if (info.Err != nil && !errors.Is(info.Err, pebble.ErrCancelledCompaction)) || info.TotalDuration > defaultPebbleSlowJobThreshold {
				pLogger.logger.Warn("pebble slow or failed flush",
					"job_id", info.JobID,
					"reason", info.Reason,
					"input_bytes", info.InputBytes,
					"duration_ms", info.TotalDuration.Milliseconds(),
					"err", info.Err,
				)
			}
		},
		CompactionEnd: func(info pebble.CompactionInfo) {
			if (info.Err != nil && !errors.Is(info.Err, pebble.ErrCancelledCompaction)) || info.TotalDuration > defaultPebbleSlowJobThreshold {
				pLogger.logger.Warn("pebble slow or failed compaction",
					"job_id", info.JobID,
					"reason", info.Reason,
					"duration_ms", info.TotalDuration.Milliseconds(),
					"err", info.Err,
				)
			}
		},
	}

	pebbleOpts.EventListener = &listener
	s.pebbleListener = &listener
	baseFS := pebbleOpts.FS
	if baseFS == nil {
		baseFS = vfs.Default
	}
	checkedFS, fsCloser := vfs.WithDiskHealthChecks(baseFS, diskSlowThreshold, nil, onDiskSlow)
	pebbleOpts.FS = checkedFS
	return fsCloser
}
