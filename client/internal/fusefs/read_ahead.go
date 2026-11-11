package fusefs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

// MaxRPCChunkSize 定义客户端单次读写 RPC 的最大载荷切片大小 (4 MiB = 4,194,304 字节)。
const MaxRPCChunkSize = 4 * 1024 * 1024

const defaultPrefetchTimeout = 30 * time.Second

// readAheadMinWant 是触发对齐窗口拉取的最小请求尺寸。
// 内核 FUSE 的 readahead 请求按 128 KiB 起步（MaxReadAhead=1 MiB），
// 随机小读通常为 4~64 KiB；用尺寸阈值把“预读流量”和“随机小读”分开，
// 保持既有契约：非连续随机读严格按请求尺寸直通，绝不放大成 4 MiB。
const readAheadMinWant = 128 * 1024

const (
	pfStateRunning uint32 = iota
	pfStateReady
	pfStatePromoted
	pfStateDiscarded
)

type prefetchFuture struct {
	offset       uint64
	done         chan struct{}
	data         []byte
	err          error
	cancel       context.CancelFunc
	state        atomic.Uint32
	fetchedBytes atomic.Uint64
}

// windowFetch 是一次单飞的对齐窗口拉取；同窗口内的并发读共享同一个 future。
type windowFetch struct {
	offset uint64
	done   chan struct{}
	data   []byte
	err    error
}

type handleReadAhead struct {
	mu          sync.Mutex
	ino         uint64
	gen         uint64
	lastReadEnd uint64
	hasLastRead bool
	seqCount    int
	// streamEnd 是该句柄已观测到的顺序读高水位（单调递增）。
	// 内核 readahead 线程与用户线程会并发下发读请求，到达顺序与偏移顺序无关，
	// 单靠 lastReadEnd 无法判定“是否同一股顺序流”，必须用单调高水位。
	streamEnd uint64
	// reads 是该句柄已收到的读请求数；首个请求一律直通，避免一次性大读被放大。
	reads       int
	winOffset   uint64
	winData     []byte
	winValid    bool
	winEOF      bool
	winConsumed int
	prefetch    *prefetchFuture
	// fetch 是当前在途的 4 MiB 对齐窗口拉取，用于把并发读合并成一次 RPC。
	fetch *windowFetch
}

// requestInWindowLocked 报告请求区间是否与当前活跃窗口、在途窗口拉取或异步预取窗口相交。
// 调用方必须持有 hra.mu。
func (hra *handleReadAhead) requestInWindowLocked(offset uint64, want int) bool {
	end := offset + uint64(want)
	if hra.winValid && offset < hra.winOffset+uint64(len(hra.winData)) && end > hra.winOffset {
		return true
	}
	if hra.fetch != nil && offset < hra.fetch.offset+MaxRPCChunkSize && end > hra.fetch.offset {
		return true
	}
	if hra.prefetch != nil && offset < hra.prefetch.offset+MaxRPCChunkSize && end > hra.prefetch.offset {
		return true
	}
	return false
}

// ChunkFetcher 定义在指定字节偏移与期望长度下向底层存储拉取数据块的窄端口适配器。
type ChunkFetcher func(ctx context.Context, offset uint64, length uint32) ([]byte, error)

// ReadAheadCoordinator 协调单个客户端实例中所有打开句柄的 4 MiB 对齐预读窗口、
// 异步下一窗口预取、跨窗口边界拼接、单飞（Singleflight）窗口合并及并发写代数（gen）失效。
type ReadAheadCoordinator struct {
	mu      sync.Mutex
	handles map[uint64]*handleReadAhead
	metrics atomic.Pointer[Metrics]
}

// NewReadAheadCoordinator 创建并初始化 ReadAheadCoordinator。
func NewReadAheadCoordinator() *ReadAheadCoordinator {
	return &ReadAheadCoordinator{
		handles: make(map[uint64]*handleReadAhead),
	}
}

func (c *ReadAheadCoordinator) bindMetrics(m *Metrics) {
	c.metrics.Store(m)
}

func (c *ReadAheadCoordinator) discardPrefetch(pf *prefetchFuture) {
	if pf == nil {
		return
	}
	if pf.cancel != nil {
		pf.cancel()
	}
	for {
		st := pf.state.Load()
		if st == pfStatePromoted || st == pfStateDiscarded {
			return
		}
		if pf.state.CompareAndSwap(st, pfStateDiscarded) {
			if st == pfStateReady {
				c.metrics.Load().addReadAheadDiscardedBytes(pf.fetchedBytes.Load())
			}
			return
		}
	}
}

func (c *ReadAheadCoordinator) promotePrefetch(pf *prefetchFuture) bool {
	return pf != nil && pf.state.CompareAndSwap(pfStateReady, pfStatePromoted)
}

func (c *ReadAheadCoordinator) discardWindowLocked(hra *handleReadAhead) {
	if hra.winValid && len(hra.winData) > hra.winConsumed {
		c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(hra.winData) - hra.winConsumed))
	}
	hra.winOffset = 0
	hra.winData = nil
	hra.winValid = false
	hra.winEOF = false
	hra.winConsumed = 0
}

func (c *ReadAheadCoordinator) installWindowLocked(hra *handleReadAhead, offset uint64, data []byte, eof bool, consumed int) {
	c.discardWindowLocked(hra)
	hra.winOffset = offset
	hra.winData = data
	hra.winValid = len(data) > 0
	hra.winEOF = eof
	hra.winConsumed = min(len(data), max(0, consumed))
}

func (c *ReadAheadCoordinator) resetHandleLocked(hra *handleReadAhead) {
	hra.gen++
	hra.lastReadEnd = 0
	hra.hasLastRead = false
	hra.seqCount = 0
	hra.streamEnd = 0
	hra.fetch = nil
	c.discardWindowLocked(hra)
	if hra.prefetch != nil {
		pf := hra.prefetch
		hra.prefetch = nil
		c.discardPrefetch(pf)
	}
}

func (c *ReadAheadCoordinator) clearWindowAndPrefetchLocked(hra *handleReadAhead) {
	hra.gen++
	hra.fetch = nil
	c.discardWindowLocked(hra)
	if hra.prefetch != nil {
		pf := hra.prefetch
		hra.prefetch = nil
		c.discardPrefetch(pf)
	}
}

func (c *ReadAheadCoordinator) getOrCreateHandle(fh, ino uint64) *handleReadAhead {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.handles == nil {
		c.handles = make(map[uint64]*handleReadAhead)
	}
	hra, ok := c.handles[fh]
	if !ok {
		hra = &handleReadAhead{
			ino: ino,
		}
		c.handles[fh] = hra
		return hra
	}
	hra.mu.Lock()
	if hra.ino != ino {
		c.resetHandleLocked(hra)
		hra.ino = ino
	}
	hra.mu.Unlock()
	return hra
}

// fetchAlignedWindow 单飞拉取覆盖 offset 的 4 MiB 对齐窗口，调用期间会临时释放 hra.mu，
// 返回时重新持有。同窗口内并发的读请求共享同一次 ChunkFetcher 调用。
func (c *ReadAheadCoordinator) fetchAlignedWindow(ctx context.Context, hra *handleReadAhead, offset uint64, fetch ChunkFetcher) ([]byte, error) {
	aligned := offset &^ uint64(MaxRPCChunkSize-1)
	if hra.fetch != nil && hra.fetch.offset == aligned {
		f := hra.fetch
		hra.mu.Unlock()
		select {
		case <-f.done:
		case <-ctx.Done():
			hra.mu.Lock()
			return nil, ctx.Err()
		}
		hra.mu.Lock()
		return f.data, f.err
	}
	f := &windowFetch{offset: aligned, done: make(chan struct{})}
	hra.fetch = f
	hra.mu.Unlock()
	data, err := fetch(ctx, aligned, MaxRPCChunkSize)
	if err != nil {
		f.err = err
	} else {
		f.data = data
		c.metrics.Load().addReadAheadPrefetchedBytes(uint64(len(data)))
	}
	close(f.done)
	hra.mu.Lock()
	if hra.fetch == f {
		hra.fetch = nil
	}
	return f.data, f.err
}

func (c *ReadAheadCoordinator) startPrefetchLocked(triggerCtx context.Context, hra *handleReadAhead, nextOffset uint64, fetch ChunkFetcher) {
	if fetch == nil {
		return
	}
	if hra.prefetch != nil {
		if hra.prefetch.offset == nextOffset {
			return
		}
		pf := hra.prefetch
		hra.prefetch = nil
		c.discardPrefetch(pf)
	}

	baseCtx := context.Background()
	if triggerCtx != nil {
		baseCtx = context.WithoutCancel(triggerCtx)
	}
	pfCtx, pfCancel := context.WithTimeout(baseCtx, defaultPrefetchTimeout)
	pf := &prefetchFuture{
		offset: nextOffset,
		done:   make(chan struct{}),
		cancel: pfCancel,
	}
	hra.prefetch = pf
	m := c.metrics.Load()
	m.incReadAheadTrigger()
	m.addReadAheadInflight(1)

	go func() {
		defer close(pf.done)
		defer pfCancel()
		defer c.metrics.Load().addReadAheadInflight(-1)
		data, err := fetch(pfCtx, nextOffset, MaxRPCChunkSize)
		if err != nil {
			pf.err = err
			pf.state.Store(pfStateDiscarded)
			return
		}
		pf.data = data
		n := uint64(len(data))
		pf.fetchedBytes.Store(n)
		m := c.metrics.Load()
		m.addReadAheadPrefetchedBytes(n)
		if !pf.state.CompareAndSwap(pfStateRunning, pfStateReady) {
			m.addReadAheadDiscardedBytes(n)
		}
	}()
}

func (c *ReadAheadCoordinator) fetchReadWindow(ctx context.Context, hra *handleReadAhead, offset uint64, fetch ChunkFetcher) (data []byte, hit bool, err error) {
	var pf *prefetchFuture
	if hra.prefetch != nil && hra.prefetch.offset == offset {
		pf = hra.prefetch
		hra.prefetch = nil
	} else if hra.prefetch != nil {
		old := hra.prefetch
		hra.prefetch = nil
		c.discardPrefetch(old)
	}

	startGen := hra.gen
	if pf != nil {
		hra.mu.Unlock()
		select {
		case <-pf.done:
		case <-ctx.Done():
			hra.mu.Lock()
			c.discardPrefetch(pf)
			return nil, false, ctx.Err()
		}
		hra.mu.Lock()
		if pf.err == nil && hra.gen == startGen && c.promotePrefetch(pf) {
			return pf.data, true, nil
		}
		c.discardPrefetch(pf)
	}

	hra.mu.Unlock()
	data, err = fetch(ctx, offset, MaxRPCChunkSize)
	hra.mu.Lock()
	if err != nil {
		return nil, false, err
	}
	c.metrics.Load().addReadAheadPrefetchedBytes(uint64(len(data)))
	return data, false, nil
}

// Read 在指定文件句柄上执行带预读缓存与单飞窗口协调的读取，将结果写入 dest 并返回实际读取字节数。
func (c *ReadAheadCoordinator) Read(ctx context.Context, fh, ino, offset uint64, dest []byte, fetch ChunkFetcher) (int, error) {
	want := len(dest)
	if want == 0 {
		return 0, nil
	}

	hra := c.getOrCreateHandle(fh, ino)

	if want <= MaxRPCChunkSize {
		hra.mu.Lock()
		hra.reads++

		// 1. 若当前请求起始偏移位于异步预取窗口内，等待预取完成并提升为活跃窗口
		if (!hra.winValid || offset < hra.winOffset || offset >= hra.winOffset+uint64(len(hra.winData))) &&
			hra.prefetch != nil &&
			offset >= hra.prefetch.offset &&
			offset < hra.prefetch.offset+MaxRPCChunkSize {
			pf := hra.prefetch
			startGen := hra.gen
			hra.mu.Unlock()
			select {
			case <-pf.done:
			case <-ctx.Done():
				return 0, ctx.Err()
			}
			hra.mu.Lock()
			if hra.gen == startGen && pf.err == nil && c.promotePrefetch(pf) {
				if hra.prefetch == pf {
					hra.prefetch = nil
				}
				c.installWindowLocked(hra, pf.offset, pf.data, len(pf.data) < MaxRPCChunkSize, 0)
				if !hra.winEOF {
					c.startPrefetchLocked(ctx, hra, hra.winOffset+MaxRPCChunkSize, fetch)
				}
			} else {
				if hra.prefetch == pf {
					hra.prefetch = nil
				}
				c.discardPrefetch(pf)
			}
		}

		// 2. 命中活跃预读窗口
		if hra.winValid && offset >= hra.winOffset && offset < hra.winOffset+uint64(len(hra.winData)) {
			relOff := int(offset - hra.winOffset)
			avail := len(hra.winData) - relOff
			if avail >= want || hra.winEOF {
				n := min(want, avail)
				copy(dest[:n], hra.winData[relOff:relOff+n])
				hra.winConsumed = min(len(hra.winData), hra.winConsumed+n)
				hra.lastReadEnd = offset + uint64(n)
				hra.hasLastRead = true
				hra.seqCount++
				hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
				hra.mu.Unlock()
				c.metrics.Load().addReadAheadHit(uint64(n))
				return n, nil
			}

			// 跨窗口边界读取：前缀取自当前窗口尾部，后缀从下一 4 MiB 窗口补齐
			prefix := make([]byte, avail)
			copy(prefix, hra.winData[relOff:])
			hra.winConsumed = min(len(hra.winData), hra.winConsumed+avail)
			nextWinOffset := hra.winOffset + uint64(len(hra.winData))
			startGen := hra.gen

			nextData, fromPrefetch, err := c.fetchReadWindow(ctx, hra, nextWinOffset, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}

			rem := min(want-avail, len(nextData))
			if hra.gen == startGen {
				c.installWindowLocked(hra, nextWinOffset, nextData, len(nextData) < MaxRPCChunkSize, rem)
				if !hra.winEOF {
					c.startPrefetchLocked(ctx, hra, nextWinOffset+MaxRPCChunkSize, fetch)
				}
			} else if len(nextData) > rem {
				c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(nextData) - rem))
			}

			copy(dest[:avail], prefix)
			copy(dest[avail:avail+rem], nextData[:rem])
			total := avail + rem
			hra.lastReadEnd = offset + uint64(total)
			hra.hasLastRead = true
			hra.seqCount++
			hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
			hra.mu.Unlock()
			hitBytes := uint64(avail)
			if fromPrefetch {
				hitBytes += uint64(rem)
			}
			c.metrics.Load().addReadAheadHit(hitBytes)
			return total, nil
		}

		// 3. 顺序读与内核预读流量统一走 4 MiB 对齐窗口单飞拉取。
		sequential := hra.hasLastRead && offset == hra.lastReadEnd
		readahead := want >= readAheadMinWant && hra.reads > 1 && !hra.winEOF &&
			offset+uint64(want) > hra.streamEnd &&
			offset < hra.streamEnd+MaxRPCChunkSize
		if sequential || readahead {
			startGen := hra.gen
			data, err := c.fetchAlignedWindow(ctx, hra, offset, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}
			winOffset := offset &^ uint64(MaxRPCChunkSize-1)
			rel := offset - winOffset
			winEOF := len(data) < MaxRPCChunkSize
			covers := rel <= uint64(len(data)) && (len(data)-int(rel) >= want || winEOF)
			if hra.gen != startGen {
				if covers {
					avail := len(data) - int(rel)
					n := min(want, avail)
					copy(dest[:n], data[rel:rel+uint64(n)])
					if len(data) > n {
						c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(data) - n))
					}
					hra.mu.Unlock()
					return n, nil
				}
				c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(data)))
			} else {
				if covers {
					avail := len(data) - int(rel)
					n := min(want, avail)
					c.installWindowLocked(hra, winOffset, data, winEOF, n)
					copy(dest[:n], data[rel:rel+uint64(n)])
					hra.lastReadEnd = offset + uint64(n)
					hra.hasLastRead = true
					hra.seqCount++
					hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
					if !hra.winEOF {
						c.startPrefetchLocked(ctx, hra, winOffset+MaxRPCChunkSize, fetch)
					}
					hra.mu.Unlock()
					return n, nil
				}
				if avail := len(data) - int(rel); avail > 0 {
					c.installWindowLocked(hra, winOffset, data, winEOF, avail)
					prefix := make([]byte, avail)
					copy(prefix, data[rel:])
					nextWinOffset := winOffset + uint64(len(data))
					nextData, fromPrefetch, err := c.fetchReadWindow(ctx, hra, nextWinOffset, fetch)
					if err != nil {
						hra.mu.Unlock()
						return 0, err
					}
					rem := min(want-avail, len(nextData))
					if hra.gen == startGen {
						c.installWindowLocked(hra, nextWinOffset, nextData, len(nextData) < MaxRPCChunkSize, rem)
						if !hra.winEOF {
							c.startPrefetchLocked(ctx, hra, nextWinOffset+MaxRPCChunkSize, fetch)
						}
					} else if len(nextData) > rem {
						c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(nextData) - rem))
					}
					copy(dest[:avail], prefix)
					copy(dest[avail:avail+rem], nextData[:rem])
					total := avail + rem
					hra.lastReadEnd = offset + uint64(total)
					hra.hasLastRead = true
					hra.seqCount++
					hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
					hra.mu.Unlock()
					if fromPrefetch && rem > 0 {
						c.metrics.Load().addReadAheadHit(uint64(rem))
					}
					return total, nil
				}
				c.installWindowLocked(hra, winOffset, data, winEOF, 0)
			}
		}

		// 4. 首次读取或非连续随机读取：按请求尺寸直通，不产生多余读放大。
		if !hra.requestInWindowLocked(offset, want) {
			c.clearWindowAndPrefetchLocked(hra)
		}
		startGen := hra.gen
		hra.mu.Unlock()

		data, err := fetch(ctx, offset, uint32(want))
		if err != nil {
			return 0, err
		}

		n := min(want, len(data))
		copy(dest[:n], data[:n])
		hra.mu.Lock()
		if hra.gen == startGen {
			hra.lastReadEnd = offset + uint64(n)
			hra.hasLastRead = true
			hra.seqCount = 1
			hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
		}
		hra.mu.Unlock()

		return n, nil
	}

	hra.mu.Lock()
	c.clearWindowAndPrefetchLocked(hra)
	startGen := hra.gen
	hra.mu.Unlock()

	var totalRead int
	for totalRead < want {
		chunkReqSize := min(want-totalRead, MaxRPCChunkSize)
		curOffset := offset + uint64(totalRead)

		chunkData, err := fetch(ctx, curOffset, uint32(chunkReqSize))
		if err != nil {
			if totalRead > 0 {
				break
			}
			return 0, err
		}

		if len(chunkData) == 0 {
			break
		}
		n := min(chunkReqSize, len(chunkData))
		copy(dest[totalRead:totalRead+n], chunkData[:n])
		totalRead += n

		if n < chunkReqSize {
			break
		}
	}

	hra.mu.Lock()
	if hra.gen == startGen {
		hra.lastReadEnd = offset + uint64(totalRead)
		hra.hasLastRead = true
		hra.seqCount = 1
	}
	hra.mu.Unlock()

	return totalRead, nil
}

// InvalidateInode 递增目标 Inode 下所有打开句柄的代数（gen），取消在途预取并清空缓存窗口。
func (c *ReadAheadCoordinator) InvalidateInode(ino uint64) {
	c.mu.Lock()
	var targets []*handleReadAhead
	for _, hra := range c.handles {
		targets = append(targets, hra)
	}
	c.mu.Unlock()

	for _, hra := range targets {
		hra.mu.Lock()
		if hra.ino == ino {
			c.resetHandleLocked(hra)
		}
		hra.mu.Unlock()
	}
}

// RemoveHandle 在本地句柄释放时取消在途预取并移除其预读状态。
func (c *ReadAheadCoordinator) RemoveHandle(fh uint64) {
	c.mu.Lock()
	hra, ok := c.handles[fh]
	if ok {
		delete(c.handles, fh)
	}
	c.mu.Unlock()

	if ok && hra != nil {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}

// Clear 取消所有句柄的在途预取并清空全部预读状态。
func (c *ReadAheadCoordinator) Clear() {
	c.mu.Lock()
	handles := c.handles
	c.handles = make(map[uint64]*handleReadAhead)
	c.mu.Unlock()

	for _, hra := range handles {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}
