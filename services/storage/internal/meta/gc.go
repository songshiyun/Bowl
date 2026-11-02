package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

const (
	// DefaultGCBatchLimit 定义单批次物理垃圾回收扫描的最大切片记录数。
	DefaultGCBatchLimit int32 = 100
	// DefaultGCInterval 定义后台 GC Worker 扫描轮询周期的默认时间间隔。
	DefaultGCInterval time.Duration = time.Minute
	// DefaultGCOrphanGracePeriod 定义后台 GC 对账回收未登记孤儿物理块文件的默认安全宽限期。
	DefaultGCOrphanGracePeriod time.Duration = time.Hour
)

// reclaimGarbageSliceRefsInTx 在当前数据库写事务内扫描并物理删除最多 limit 条 refs <= 0 的废弃物理块及其 slice_refs 行。
// 受在途读取保护（isSliceReadInFlight）的切片将被跳过；
// 若某个切片的物理块删除发生错误，跳过清除该切片的 slice_refs 记录以保留至下次重试，且不使事务回滚已成功回收的其他切片。
func (m *manager) reclaimGarbageSliceRefsInTx(ctx context.Context, q db.Querier, bs BlockStore, limit int32) (int, []error, error) {
	rows, err := q.ListGarbageSliceRefs(ctx, limit)
	if err != nil {
		return 0, nil, err
	}
	if len(rows) == 0 {
		return 0, nil, nil
	}

	var (
		reclaimed         int
		capturedBlockErrs []error
	)

	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return reclaimed, capturedBlockErrs, err
		}
		if m.isSliceReadInFlight(row.SliceID) {
			continue
		}

		// 非法 ID 或零大小异常切片，直接从数据库元数据中移除
		if row.SliceID <= 0 || row.Size <= 0 {
			if err := q.DeleteSliceRef(ctx, row.SliceID); err != nil {
				return reclaimed, capturedBlockErrs, err
			}
			reclaimed++
			continue
		}

		blocks := SliceBlocks(uint32(row.Size))
		var deleteFailed bool
		for _, b := range blocks {
			if err := bs.DeleteBlock(ctx, uint64(row.SliceID), b.BlockIdx, b.BlockSize); err != nil {
				capturedBlockErrs = append(capturedBlockErrs, err)
				deleteFailed = true
				break
			}
		}

		if deleteFailed {
			// 某个 Block 文件物理删除遇到错误：跳过删除该 slice_refs 记录，
			// 不回滚已成功删除的其他切片，保留在数据库中供下次 GC 重试。
			continue
		}

		if err := q.DeleteSliceRef(ctx, row.SliceID); err != nil {
			return reclaimed, capturedBlockErrs, err
		}
		reclaimed++
	}
	return reclaimed, capturedBlockErrs, nil
}

// reclaimObsoleteSliceRefs 独立推进 refs <= 0 的废弃 Block 物理删除与 slice_refs 清理（供合并完成后同一 Worker 内立即推进）。
func (m *manager) reclaimObsoleteSliceRefs(ctx context.Context, batchLimit int32) (int, error) {
	if batchLimit <= 0 {
		batchLimit = DefaultGCBatchLimit
	}
	bs := m.getBlockStore()
	var (
		reclaimed         int
		capturedBlockErrs []error
	)
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		var txErr error
		reclaimed, capturedBlockErrs, txErr = m.reclaimGarbageSliceRefsInTx(ctx, q, bs, batchLimit)
		return txErr
	})
	if err != nil {
		return 0, err
	}
	if len(capturedBlockErrs) > 0 {
		return reclaimed, errors.Join(capturedBlockErrs...)
	}
	return reclaimed, nil
}

// reclaimAllObsoleteSliceRefs 在同一 Worker 内持续按批次回收 refs <= 0 的废弃 Block，直至无更多可立即回收项或上下文取消。
func (m *manager) reclaimAllObsoleteSliceRefs(ctx context.Context, batchLimit int32) {
	if batchLimit <= 0 {
		batchLimit = DefaultGCBatchLimit
	}
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := m.reclaimObsoleteSliceRefs(ctx, batchLimit)
		if err != nil || n < int(batchLimit) {
			return
		}
	}
}

// CollectGarbage 执行单批次物理垃圾回收。
//  1. 清理已超期的客户端会话租约（级联释放句柄、文件锁与等待队列），并非阻塞物理回收无有效句柄引用的 Orphan Inode。
//  2. 扫描 slice_refs 表中引用计数已归零 (refs <= 0) 的物理切片，调用 BlockStore.DeleteBlock 物理删除本地 NVMe 文件，
//     在该切片的所有关联 Block 文件均物理删除成功（或文件本就不存在）后，从数据库中清除 slice_refs 行。
//  3. 若底层 BlockStore 实现了 BlockLister，进一步扫描修改时间超过安全宽限期（默认 1 小时）的物理块文件并与 slice_refs 对账，
//     仅对在 slice_refs 中无记录（pgx.ErrNoRows）的未登记孤儿物理块文件执行物理回收。
//
// 若 limit <= 0，默认采用 DefaultGCBatchLimit (100)。
// 若某切片的物理块删除发生错误，跳过清除该切片的 slice_refs 记录以保留至下次 GC 重试，并不使事务整体回滚已成功回收的其他切片。
func (m *manager) CollectGarbage(ctx context.Context, limit int32) (int, error) {
	if limit <= 0 {
		limit = DefaultGCBatchLimit
	}

	var (
		totalReclaimed    int
		capturedBlockErrs []error
		expiredCount      int64
		reclaimedInos     []int64
	)

	bs := m.getBlockStore()

	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		totalReclaimed = 0
		capturedBlockErrs = nil
		expiredCount = 0
		reclaimedInos = nil

		var txErr error
		expiredCount, txErr = q.DeleteExpiredClientSessions(ctx)
		if txErr != nil {
			return fmt.Errorf("delete expired client sessions: %w", txErr)
		}
		reclaimedInos, txErr = m.reclaimUnreferencedOrphansInTx(ctx, q)
		if txErr != nil {
			return txErr
		}

		totalReclaimed, capturedBlockErrs, txErr = m.reclaimGarbageSliceRefsInTx(ctx, q, bs, limit)
		return txErr
	})
	if err != nil {
		return 0, err
	}

	m.finalizeReclaimedOrphans(reclaimedInos)
	if expiredCount > 0 {
		m.cleanExpiredDirCursors(ctx)
		m.notifyAllLockWaiters()
	}

	if lister, ok := bs.(BlockLister); ok && m.store != nil {
		gracePeriod := m.gcOrphanGracePeriod
		if gracePeriod <= 0 {
			gracePeriod = DefaultGCOrphanGracePeriod
		}
		cutoff := time.Now().Add(-gracePeriod)
		candidates, listErr := lister.ListBlocksBefore(ctx, cutoff, int(limit))
		if listErr != nil {
			capturedBlockErrs = append(capturedBlockErrs, listErr)
		} else {
			for _, cand := range candidates {
				if err := ctx.Err(); err != nil {
					capturedBlockErrs = append(capturedBlockErrs, err)
					break
				}
				if cand.SliceID == 0 || cand.SliceID > math.MaxInt64 {
					continue
				}
				_, getErr := m.store.GetSliceRef(ctx, int64(cand.SliceID))
				if getErr == nil {
					// 在 slice_refs 中存在记录（无论 refs > 0 还是 refs <= 0），绝不越权删除
					continue
				}
				if !errors.Is(getErr, pgx.ErrNoRows) {
					capturedBlockErrs = append(capturedBlockErrs, getErr)
					continue
				}
				if delErr := bs.DeleteBlock(ctx, cand.SliceID, cand.BlockIdx, cand.BlockSize); delErr != nil {
					capturedBlockErrs = append(capturedBlockErrs, delErr)
					continue
				}
				totalReclaimed++
			}
		}
	}

	if len(capturedBlockErrs) > 0 {
		return totalReclaimed, errors.Join(capturedBlockErrs...)
	}

	return totalReclaimed, nil
}

// startGCWorkerWithDone 启动或接管 Storage 唯一的后台单线程 Worker（串行执行自动 Compaction、废弃 Block 立即回收与周期性 GC），并返回 doneCh 通道。
// 协程完全退出排空后关闭 doneCh。
// 若 interval <= 0 则采用 DefaultGCInterval；若 batchLimit <= 0 则采用 DefaultGCBatchLimit。
func (m *manager) startGCWorkerWithDone(ctx context.Context, interval time.Duration, batchLimit int32) <-chan struct{} {
	if interval <= 0 {
		interval = DefaultGCInterval
	}
	if batchLimit <= 0 {
		batchLimit = DefaultGCBatchLimit
	}

	m.workerMu.Lock()
	prevCancel := m.workerCancel
	prevDone := m.workerDoneCh
	m.workerMu.Unlock()

	if prevCancel != nil {
		prevCancel()
	}
	if prevDone != nil {
		<-prevDone
	}

	m.workerMu.Lock()
	m.workerStopped = false
	doneCh := m.startBackgroundWorkerLocked(ctx, interval, batchLimit, true)
	m.workerMu.Unlock()
	return doneCh
}

// startBackgroundWorkerLocked 在持有 m.workerMu 的前提下启动唯一后台 Worker 协程。
func (m *manager) startBackgroundWorkerLocked(ctx context.Context, interval time.Duration, batchLimit int32, periodicGC bool) <-chan struct{} {
	if interval <= 0 {
		interval = DefaultGCInterval
	}
	if batchLimit <= 0 {
		batchLimit = DefaultGCBatchLimit
	}
	if m.compactState == nil {
		m.compactState = make(map[chunkKey]compactTaskState)
	}
	if m.compactWakeCh == nil {
		m.compactWakeCh = make(chan struct{}, 1)
	}

	workerCtx, workerCancel := context.WithCancel(ctx)
	doneCh := make(chan struct{})
	m.workerCancel = workerCancel
	m.workerDoneCh = doneCh

	go func() {
		defer func() {
			workerCancel()
			m.workerMu.Lock()
			if m.workerDoneCh == doneCh {
				m.workerCancel = nil
				m.workerDoneCh = nil
				if ctx.Err() != nil {
					m.workerStopped = true
				}
			}
			m.workerMu.Unlock()
			close(doneCh)
		}()

		m.runBackgroundWorkerLoop(workerCtx, interval, batchLimit, periodicGC)
	}()

	return doneCh
}

func (m *manager) hasQueuedCompactTasks() bool {
	m.workerMu.Lock()
	n := len(m.compactQueue)
	m.workerMu.Unlock()
	return n > 0
}

func (m *manager) popNextCompactTask() (chunkKey, bool) {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if len(m.compactQueue) == 0 {
		return chunkKey{}, false
	}
	key := m.compactQueue[0]
	m.compactQueue = m.compactQueue[1:]
	if len(m.compactQueue) == 0 {
		m.compactQueue = nil
	}
	m.compactState[key] = compactStateRunning
	return key, true
}

func (m *manager) runPeriodicGCCycle(ctx context.Context, batchLimit int32) {
	for {
		if ctx.Err() != nil {
			return
		}
		n, err := m.CollectGarbage(ctx, batchLimit)
		if err != nil || n < int(batchLimit) {
			return
		}
		// 若有待处理的合并任务，单次推进一轮完整 CollectGarbage（涵盖超期会话、孤儿 Inode、一批废弃块及一批未登记孤儿块）后让出给合并任务，实现穿插推进
		if m.hasQueuedCompactTasks() {
			return
		}
	}
}

func (m *manager) runBackgroundWorkerLoop(ctx context.Context, interval time.Duration, batchLimit int32, periodicGC bool) {
	var (
		ticker *time.Ticker
		tickCh <-chan time.Time
		nextGC time.Time
	)
	if periodicGC {
		ticker = time.NewTicker(interval)
		defer ticker.Stop()
		tickCh = ticker.C
		nextGC = time.Now().Add(interval)
	}

	maybeRunDuePeriodicGC := func() bool {
		if !periodicGC || ctx.Err() != nil {
			return false
		}
		due := !time.Now().Before(nextGC)
		select {
		case <-tickCh:
			due = true
		default:
		}
		if !due {
			return false
		}
		select {
		case <-tickCh:
		default:
		}
		m.runPeriodicGCCycle(ctx, batchLimit)
		nextGC = time.Now().Add(interval)
		return true
	}

	for {
		if ctx.Err() != nil {
			return
		}

		maybeRunDuePeriodicGC()
		if ctx.Err() != nil {
			return
		}

		if key, ok := m.popNextCompactTask(); ok {
			m.executeCompactTaskInWorker(ctx, key, batchLimit, maybeRunDuePeriodicGC)
			continue
		}

		select {
		case <-ctx.Done():
			return
		case <-m.compactWakeCh:
		case <-tickCh:
			if ctx.Err() != nil {
				return
			}
			m.runPeriodicGCCycle(ctx, batchLimit)
			nextGC = time.Now().Add(interval)
		}
	}
}

func (m *manager) executeCompactTaskInWorker(ctx context.Context, key chunkKey, batchLimit int32, maybeRunDuePeriodicGC func() bool) {
	clearRunningState := func() {
		m.workerMu.Lock()
		if st := m.compactState[key]; st == compactStateRunning || st == compactStateRerun {
			delete(m.compactState, key)
		}
		m.workerMu.Unlock()
	}

	const maxRounds = 16
	for round := 0; round < maxRounds; round++ {
		if ctx.Err() != nil {
			clearRunningState()
			return
		}
		if round > 0 {
			maybeRunDuePeriodicGC()
			if ctx.Err() != nil {
				clearRunningState()
				return
			}
		}

		compactCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		finalCount, casAborted, compacted, err := m.compactChunkWithCount(compactCtx, key.ino, key.indx, true)
		cancel()

		if compacted && ctx.Err() == nil {
			reclaimCtx, rCancel := context.WithTimeout(ctx, 30*time.Second)
			m.reclaimAllObsoleteSliceRefs(reclaimCtx, batchLimit)
			rCancel()
		}

		if err != nil {
			clearRunningState()
			return
		}

		m.workerMu.Lock()
		pending := m.compactState[key] == compactStateRerun
		if !casAborted && finalCount < m.compactSliceThreshold && !pending {
			delete(m.compactState, key)
			m.workerMu.Unlock()
			return
		}
		m.compactState[key] = compactStateRunning
		m.workerMu.Unlock()

		if !casAborted && finalCount >= m.compactSliceThreshold {
			// 若还有其他 Chunk 在队列中等待，则将当前仍需继续合并的 Chunk 重新排入队尾以保证多 Chunk 公平推进
			if m.requeueIfOtherChunksWaiting(key) {
				return
			}
			continue
		}

		queryCtx, qCancel := context.WithTimeout(ctx, 5*time.Second)
		chunk, qErr := m.store.GetChunk(queryCtx, db.GetChunkParams{
			Ino:  int64(key.ino),
			Indx: key.indx,
		})
		qCancel()
		if qErr != nil {
			clearRunningState()
			return
		}
		rawLatest, decErr := DecodeSlices(chunk.Slices)
		if decErr != nil {
			clearRunningState()
			return
		}
		latestCount := len(rawLatest) - skipSome(rawLatest)
		if latestCount < m.compactSliceThreshold {
			m.workerMu.Lock()
			if m.compactState[key] != compactStateRerun {
				delete(m.compactState, key)
				m.workerMu.Unlock()
				return
			}
			m.compactState[key] = compactStateRunning
			m.workerMu.Unlock()
		}
		if m.requeueIfOtherChunksWaiting(key) {
			return
		}
	}

	clearRunningState()
}

func (m *manager) requeueIfOtherChunksWaiting(key chunkKey) bool {
	m.workerMu.Lock()
	defer m.workerMu.Unlock()
	if len(m.compactQueue) == 0 {
		return false
	}
	m.compactState[key] = compactStateQueued
	m.compactQueue = append(m.compactQueue, key)
	return true
}

// StartGCWorker 在后台启动周期性物理垃圾回收协程。
// 当 ctx 取消时自动平滑退出。
// 若 interval <= 0 则采用 DefaultGCInterval；若 batchLimit <= 0 则采用 DefaultGCBatchLimit。
func (m *manager) StartGCWorker(ctx context.Context, interval time.Duration, batchLimit int32) {
	_ = m.startGCWorkerWithDone(ctx, interval, batchLimit)
}
