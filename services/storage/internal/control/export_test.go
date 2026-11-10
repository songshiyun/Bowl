package control

import (
	"context"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

// SharedCacheMetrics 导出 Manager 持有的进程级共享 Pebble BlockCache 快照指标（仅用于实证测试独立对账）。
func SharedCacheMetrics(mgr Manager) pebble.CacheMetrics {
	m, ok := mgr.(*manager)
	if !ok || m == nil {
		return pebble.CacheMetrics{}
	}
	m.poolMu.RLock()
	cache := m.cache
	if cache != nil {
		cache.Ref()
	}
	m.poolMu.RUnlock()
	if cache == nil {
		return pebble.CacheMetrics{}
	}
	defer cache.Unref()
	return cache.Metrics()
}

// EvictIdleActorForTest 从控制面实例池驱逐指定磁盘的 StoreActor（优雅刷盘关闭但保留磁盘目录与 PG 记录），
// 用于实证测试空闲驱逐结构化日志、bowl_storage_control_actor_evictions_total 与二次懒加载计数。
func EvictIdleActorForTest(ctx context.Context, mgr Manager, diskID string) (bool, error) {
	m, ok := mgr.(*manager)
	if !ok || m == nil {
		return false, ErrInvalidArgument
	}
	return m.evictIdleActor(ctx, diskID)
}

// InjectReadOnlyActorForTest 将只读打开的 StoreActor 注入实例池（仅用于实证测试 ro 模式指标统计）。
func InjectReadOnlyActorForTest(ctx context.Context, mgr Manager, diskID string) (*store.StoreActor, error) {
	m, ok := mgr.(*manager)
	if !ok || m == nil {
		return nil, ErrInvalidArgument
	}
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	cache := m.cache
	m.poolMu.RUnlock()

	roActor, err := store.Open(store.Options{
		Dir:       m.diskDir(diskID),
		DiskID:    diskID,
		ReadOnly:  true,
		AtimeMode: m.atimeMode,
		Cache:     cache,
		Logger:    m.baseLogger,
		Metrics:   m.storeMetrics,
	})
	if err != nil {
		return nil, err
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		_ = roActor.Close()
		return nil, ErrClosed
	}
	m.actors[diskID] = roActor
	m.poolMu.Unlock()

	m.collector.incActorLoad(true)
	m.logger.InfoContext(ctx, "actor loaded",
		"disk_id", diskID,
		"epoch", roActor.WriterEpoch(),
		"mode", actorModeRO,
	)
	return roActor, nil
}

// SetRemoveAllForTest 替换目录清理函数（仅用于实证测试回滚/删盘目录清理失败时的日志告警与错误指标累加）。
func SetRemoveAllForTest(mgr Manager, fn func(string) error) {
	m, ok := mgr.(*manager)
	if !ok || m == nil || fn == nil {
		return
	}
	m.removeAll = fn
}
