package fusefs

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"

	"github.com/starsrc/bowl/client/internal/rpc"
)

// MaxRPCChunkSize 定义客户端单次读写 RPC 的最大载荷切片大小 (4 MiB = 4,194,304 字节)。
const MaxRPCChunkSize = 4 * 1024 * 1024

// ReadAheadMaxPrefetch 定义确认连续顺序流后单句柄最多并发维持的 4 MiB 异步预读窗口数（W_read = 4，对齐 WriteBehind W* = 4）。
const ReadAheadMaxPrefetch = 4

// ReadAheadMaxGlobalInflight 定义单个客户端实例跨所有活跃句柄允许并发在途的最大异步预读 RPC 数（12 * 4 MiB = 48 MiB）。
// 单流（1~3 并发）仍可享满 W_read = 4 流水线深度；16 路高并发顺序读（如 fio seq_read_16jobs）自动按活跃流数公平收敛至单流深度 1 且全局不超过 12 路，
// 避免 16 * 4 = 64 路（256 MiB）并发流冲刷 32 MiB HTTP/2 连接窗口与缓冲池。
const ReadAheadMaxGlobalInflight = 12

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

// sharedReadBuf 为池化 rpc.ReadPayload 提供原子引用计数包装，
// 确保活跃窗口、上一窗口、单飞等待者与前台拷贝均释放引用后才归还 4 MiB 缓冲池。
type sharedReadBuf struct {
	payload rpc.ReadPayload
	refs    atomic.Int32
}

func newSharedReadBuf(p rpc.ReadPayload) *sharedReadBuf {
	b := &sharedReadBuf{payload: p}
	b.refs.Store(1)
	return b
}

func (b *sharedReadBuf) bytes() []byte {
	if b == nil {
		return nil
	}
	return b.payload.Data
}

func (b *sharedReadBuf) retain() *sharedReadBuf {
	if b == nil {
		return nil
	}
	for {
		cur := b.refs.Load()
		if cur <= 0 {
			return nil
		}
		if b.refs.CompareAndSwap(cur, cur+1) {
			return b
		}
	}
}

func (b *sharedReadBuf) release() {
	if b == nil {
		return
	}
	if b.refs.Add(-1) == 0 {
		b.payload.Release()
	}
}

type prefetchFuture struct {
	offset       uint64
	done         chan struct{}
	buf          *sharedReadBuf
	err          error
	cancel       context.CancelFunc
	spanCtx      trace.SpanContext
	state        atomic.Uint32
	fetchedBytes atomic.Uint64
}

// windowFetch 是一次单飞的对齐窗口拉取；同窗口内的并发读共享同一个 future。
type windowFetch struct {
	offset    uint64
	done      chan struct{}
	buf       *sharedReadBuf
	err       error
	spanCtx   trace.SpanContext
	waiters   int
	completed bool
}

type handleReadAhead struct {
	mu          sync.Mutex
	ino         uint64
	gen         uint64
	lastReadEnd uint64
	hasLastRead bool
	seqCount    int
	pipelined   bool
	// streamEnd 是该句柄已观测到的顺序读高水位（单调递增）。
	// 内核 readahead 线程与用户线程会并发下发读请求，到达顺序与偏移顺序无关，
	// 单靠 lastReadEnd 无法判定“是否同一股顺序流”，必须用单调高水位。
	streamEnd uint64
	// reads 是该句柄已收到的读请求数；首个请求一律直通，避免一次性大读被放大。
	reads       int
	winOffset   uint64
	winBuf      *sharedReadBuf
	winValid    bool
	winEOF      bool
	winConsumed int

	// prevWinBuf 保留顺序推进刚滑出的上一 4 MiB 窗口，供乱序晚到的内核并发 readahead 请求直接命中内存，
	// 避免跨 4 MiB 边界乱序请求误判为随机跳读而清空整条流水线。
	prevWinOffset   uint64
	prevWinBuf      *sharedReadBuf
	prevWinValid    bool
	prevWinConsumed int

	prefetches []*prefetchFuture
	// fetch 是当前在途的 4 MiB 对齐窗口拉取，用于把并发读合并成一次 RPC。
	fetch *windowFetch
}

func (hra *handleReadAhead) winData() []byte {
	return hra.winBuf.bytes()
}

func (hra *handleReadAhead) prevWinData() []byte {
	return hra.prevWinBuf.bytes()
}

// requestInWindowLocked 报告请求区间是否与当前活跃窗口、上一窗口、在途窗口拉取或异步预取流水线相交。
// 调用方必须持有 hra.mu。
func (hra *handleReadAhead) requestInWindowLocked(offset uint64, want int) bool {
	end := offset + uint64(want)
	if hra.winValid && offset < hra.winOffset+uint64(len(hra.winData())) && end > hra.winOffset {
		return true
	}
	if hra.prevWinValid && offset < hra.prevWinOffset+uint64(len(hra.prevWinData())) && end > hra.prevWinOffset {
		return true
	}
	if hra.fetch != nil && offset < hra.fetch.offset+MaxRPCChunkSize && end > hra.fetch.offset {
		return true
	}
	for _, pf := range hra.prefetches {
		if offset < pf.offset+MaxRPCChunkSize && end > pf.offset {
			return true
		}
	}
	return false
}

// ChunkFetcher 定义在指定字节偏移与期望长度下向底层存储拉取数据块的窄端口适配器。
type ChunkFetcher func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error)

// ReadAheadCoordinator 协调单个客户端实例中所有打开句柄的 4 MiB 对齐预读窗口、
// W_read=4 并发流水线预取、跨窗口边界拼接、单飞（Singleflight）窗口合并及并发写代数（gen）失效。
type ReadAheadCoordinator struct {
	mu                 sync.Mutex
	handles            map[uint64]*handleReadAhead
	byInode            map[uint64]map[uint64]*handleReadAhead
	inflightPrefetches atomic.Int32
	activePipelined    atomic.Int32
	metrics            atomic.Pointer[Metrics]
	tracerProvider     atomic.Pointer[trace.TracerProvider]
}

// NewReadAheadCoordinator 创建并初始化 ReadAheadCoordinator。
func NewReadAheadCoordinator() *ReadAheadCoordinator {
	return &ReadAheadCoordinator{
		handles: make(map[uint64]*handleReadAhead),
		byInode: make(map[uint64]map[uint64]*handleReadAhead),
	}
}

func (c *ReadAheadCoordinator) bindInodeLocked(fh, ino uint64, hra *handleReadAhead) {
	if c.byInode == nil {
		c.byInode = make(map[uint64]map[uint64]*handleReadAhead)
	}
	set := c.byInode[ino]
	if set == nil {
		set = make(map[uint64]*handleReadAhead)
		c.byInode[ino] = set
	}
	set[fh] = hra
}

func (c *ReadAheadCoordinator) unbindInodeLocked(fh, ino uint64) {
	if set := c.byInode[ino]; set != nil {
		delete(set, fh)
		if len(set) == 0 {
			delete(c.byInode, ino)
		}
	}
}

func (c *ReadAheadCoordinator) bindMetrics(m *Metrics) {
	c.metrics.Store(m)
}

func (c *ReadAheadCoordinator) bindTracerProvider(tp trace.TracerProvider) {
	if tp == nil {
		c.tracerProvider.Store(nil)
		return
	}
	c.tracerProvider.Store(&tp)
}

func (c *ReadAheadCoordinator) activeTracer() (trace.Tracer, bool) {
	ptr := c.tracerProvider.Load()
	if ptr == nil || *ptr == nil {
		return nil, false
	}
	tp := *ptr
	if _, isNoop := tp.(noop.TracerProvider); isNoop {
		return nil, false
	}
	return tp.Tracer("bowl/client/fusefs"), true
}

func (c *ReadAheadCoordinator) waitDone(ctx context.Context, done <-chan struct{}, linkSC trace.SpanContext) error {
	select {
	case <-done:
		return nil
	default:
	}
	waitStart := time.Now()
	var waitSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		var opts []trace.SpanStartOption
		if linkSC.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: linkSC}))
		}
		_, waitSpan = tracer.Start(ctx, "read_ahead.Wait", opts...)
	}
	var waitErr error
	select {
	case <-done:
	case <-ctx.Done():
		waitErr = ctx.Err()
		waitSpan.RecordError(waitErr)
		waitSpan.SetStatus(codes.Error, waitErr.Error())
	}
	waitSpan.End()
	c.metrics.Load().observeReadAheadWait(time.Since(waitStart))
	return waitErr
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
				buf := pf.buf
				pf.buf = nil
				buf.release()
				c.metrics.Load().addReadAheadDiscardedBytes(pf.fetchedBytes.Load())
			}
			return
		}
	}
}

func (c *ReadAheadCoordinator) promotePrefetch(pf *prefetchFuture) (*sharedReadBuf, bool) {
	if pf == nil || !pf.state.CompareAndSwap(pfStateReady, pfStatePromoted) {
		return nil, false
	}
	buf := pf.buf
	pf.buf = nil
	return buf, true
}

func (c *ReadAheadCoordinator) discardPrevWindowLocked(hra *handleReadAhead) {
	if hra.prevWinValid {
		dataLen := len(hra.prevWinData())
		if dataLen > hra.prevWinConsumed {
			c.metrics.Load().addReadAheadDiscardedBytes(uint64(dataLen - hra.prevWinConsumed))
		}
	}
	if hra.prevWinBuf != nil {
		hra.prevWinBuf.release()
		hra.prevWinBuf = nil
	}
	hra.prevWinOffset = 0
	hra.prevWinValid = false
	hra.prevWinConsumed = 0
}

func (c *ReadAheadCoordinator) discardAllWindowsLocked(hra *handleReadAhead) {
	c.discardPrevWindowLocked(hra)
	if hra.winValid {
		dataLen := len(hra.winData())
		if dataLen > hra.winConsumed {
			c.metrics.Load().addReadAheadDiscardedBytes(uint64(dataLen - hra.winConsumed))
		}
	}
	if hra.winBuf != nil {
		hra.winBuf.release()
		hra.winBuf = nil
	}
	hra.winOffset = 0
	hra.winValid = false
	hra.winEOF = false
	hra.winConsumed = 0
}

func (c *ReadAheadCoordinator) installWindowLocked(hra *handleReadAhead, offset uint64, buf *sharedReadBuf, eof bool, consumed int) {
	dataLen := len(buf.bytes())
	// 若旧活跃窗口刚好紧邻新窗口左侧，将其平滑降级为 prevWinBuf 以承接乱序并发 readahead 请求。
	if hra.winValid && hra.winBuf != nil && dataLen > 0 && hra.winOffset+uint64(len(hra.winData())) == offset {
		c.discardPrevWindowLocked(hra)
		hra.prevWinOffset = hra.winOffset
		hra.prevWinBuf = hra.winBuf
		hra.prevWinValid = true
		hra.prevWinConsumed = hra.winConsumed
		hra.winBuf = nil
		hra.winValid = false
		hra.winConsumed = 0
	} else {
		c.discardAllWindowsLocked(hra)
	}
	hra.winOffset = offset
	if dataLen > 0 {
		hra.winBuf = buf.retain()
		hra.winValid = true
	} else {
		hra.winBuf = nil
		hra.winValid = false
	}
	hra.winEOF = eof
	hra.winConsumed = min(dataLen, max(0, consumed))
}

func (c *ReadAheadCoordinator) discardAllPrefetchesLocked(hra *handleReadAhead) {
	for _, pf := range hra.prefetches {
		c.discardPrefetch(pf)
	}
	clear(hra.prefetches)
	hra.prefetches = hra.prefetches[:0]
}

func (c *ReadAheadCoordinator) setPipelinedLocked(hra *handleReadAhead, v bool) {
	if hra.pipelined == v {
		return
	}
	hra.pipelined = v
	if v {
		c.activePipelined.Add(1)
	} else {
		c.activePipelined.Add(-1)
	}
}

func (c *ReadAheadCoordinator) tryAcquirePrefetchSlot() bool {
	for {
		cur := c.inflightPrefetches.Load()
		if cur >= ReadAheadMaxGlobalInflight {
			return false
		}
		if c.inflightPrefetches.CompareAndSwap(cur, cur+1) {
			return true
		}
	}
}

func (c *ReadAheadCoordinator) resetHandleLocked(hra *handleReadAhead) {
	hra.gen++
	hra.lastReadEnd = 0
	hra.hasLastRead = false
	hra.seqCount = 0
	c.setPipelinedLocked(hra, false)
	hra.streamEnd = 0
	hra.fetch = nil
	c.discardAllWindowsLocked(hra)
	c.discardAllPrefetchesLocked(hra)
}

func (c *ReadAheadCoordinator) clearWindowAndPrefetchLocked(hra *handleReadAhead) {
	hra.gen++
	c.setPipelinedLocked(hra, false)
	hra.fetch = nil
	c.discardAllWindowsLocked(hra)
	c.discardAllPrefetchesLocked(hra)
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
		c.bindInodeLocked(fh, ino, hra)
		return hra
	}
	hra.mu.Lock()
	if hra.ino != ino {
		oldIno := hra.ino
		c.resetHandleLocked(hra)
		hra.ino = ino
		c.unbindInodeLocked(fh, oldIno)
		c.bindInodeLocked(fh, ino, hra)
	}
	hra.mu.Unlock()
	return hra
}

// fetchAlignedWindow 单飞拉取覆盖 offset 的 4 MiB 对齐窗口，调用期间会临时释放 hra.mu，
// 返回时重新持有；返回的 *sharedReadBuf 已为当前调用方独立增加 1 个引用计数，调用方负责 release()。
func (c *ReadAheadCoordinator) fetchAlignedWindow(ctx context.Context, hra *handleReadAhead, offset uint64, fetch ChunkFetcher) (*sharedReadBuf, error) {
	aligned := offset &^ uint64(MaxRPCChunkSize-1)
	if hra.fetch != nil && hra.fetch.offset == aligned {
		f := hra.fetch
		f.waiters++
		hra.mu.Unlock()
		waitErr := c.waitDone(ctx, f.done, f.spanCtx)
		hra.mu.Lock()
		var callerBuf *sharedReadBuf
		if waitErr == nil && f.err == nil && f.buf != nil {
			callerBuf = f.buf.retain()
		}
		f.waiters--
		if f.waiters == 0 && f.completed && f.buf != nil {
			f.buf.release()
			f.buf = nil
		}
		if waitErr != nil {
			return nil, waitErr
		}
		return callerBuf, f.err
	}

	f := &windowFetch{
		offset:  aligned,
		done:    make(chan struct{}),
		spanCtx: trace.SpanContextFromContext(ctx),
		waiters: 1,
	}
	hra.fetch = f
	hra.mu.Unlock()

	payload, err := fetch(ctx, aligned, MaxRPCChunkSize)
	if err != nil {
		f.err = err
	} else {
		f.buf = newSharedReadBuf(payload)
		c.metrics.Load().addReadAheadPrefetchedBytes(uint64(len(f.buf.bytes())))
	}
	close(f.done)

	hra.mu.Lock()
	f.completed = true
	if hra.fetch == f {
		hra.fetch = nil
	}
	var callerBuf *sharedReadBuf
	if f.err == nil && f.buf != nil {
		callerBuf = f.buf.retain()
	}
	f.waiters--
	if f.waiters == 0 && f.buf != nil {
		f.buf.release()
		f.buf = nil
	}
	return callerBuf, f.err
}

func (c *ReadAheadCoordinator) launchPrefetchLocked(triggerCtx context.Context, hra *handleReadAhead, nextOffset uint64, fetch ChunkFetcher) *prefetchFuture {
	triggerSC := trace.SpanContextFromContext(triggerCtx)
	pfBaseCtx := context.Background()
	var pfSpan trace.Span = noopSpanInstance
	if tracer, ok := c.activeTracer(); ok {
		opts := make([]trace.SpanStartOption, 0, 3)
		opts = append(opts,
			trace.WithNewRoot(),
			trace.WithAttributes(
				attribute.Int64("read_ahead.ino", int64(hra.ino)),
				attribute.Int64("read_ahead.offset", int64(nextOffset)),
				attribute.Int("read_ahead.length", MaxRPCChunkSize),
			),
		)
		if triggerSC.IsValid() {
			opts = append(opts, trace.WithLinks(trace.Link{SpanContext: triggerSC}))
		}
		pfBaseCtx, pfSpan = tracer.Start(pfBaseCtx, "read_ahead.Prefetch", opts...)
	}

	pfCtx, pfCancel := context.WithTimeout(pfBaseCtx, defaultPrefetchTimeout)
	pf := &prefetchFuture{
		offset:  nextOffset,
		done:    make(chan struct{}),
		cancel:  pfCancel,
		spanCtx: pfSpan.SpanContext(),
	}
	m := c.metrics.Load()
	m.incReadAheadTrigger()
	m.addReadAheadInflight(1)

	go func() {
		defer close(pf.done)
		defer pfSpan.End()
		defer pfCancel()
		defer c.inflightPrefetches.Add(-1)
		defer c.metrics.Load().addReadAheadInflight(-1)

		payload, err := fetch(pfCtx, nextOffset, MaxRPCChunkSize)
		if err != nil {
			pf.err = err
			pf.state.Store(pfStateDiscarded)
			pfSpan.RecordError(err)
			pfSpan.SetStatus(codes.Error, err.Error())
			return
		}
		buf := newSharedReadBuf(payload)
		n := uint64(len(buf.bytes()))
		pf.fetchedBytes.Store(n)
		pf.buf = buf
		pfSpan.SetAttributes(attribute.Int64("read_ahead.fetched_bytes", int64(n)))

		m := c.metrics.Load()
		m.addReadAheadPrefetchedBytes(n)
		if !pf.state.CompareAndSwap(pfStateRunning, pfStateReady) {
			pf.buf = nil
			buf.release()
			m.addReadAheadDiscardedBytes(n)
		}
	}()
	return pf
}

// ensurePrefetchPipelineLocked 根据当前顺序流成熟度补齐异步预取流水线：
//   - 首窗口刚建立时预取 1 个 4 MiB 窗口；
//   - 一旦确认连续流（累计 >= 3 次顺序读、窗口内已消费 >= 1 MiB 或已有预取晋升），立即并发拉满 W_read = 4 个 4 MiB 预读窗口。
func (c *ReadAheadCoordinator) ensurePrefetchPipelineLocked(triggerCtx context.Context, hra *handleReadAhead, fetch ChunkFetcher) {
	if fetch == nil || !hra.winValid || hra.winEOF {
		c.discardAllPrefetchesLocked(hra)
		return
	}

	targetDepth := 1
	if hra.pipelined || hra.seqCount >= 3 || hra.winConsumed >= 1<<20 {
		c.setPipelinedLocked(hra, true)
		streams := max(1, int(c.activePipelined.Load()))
		perStreamCap := max(1, ReadAheadMaxGlobalInflight/streams)
		targetDepth = min(ReadAheadMaxPrefetch, perStreamCap)
	}

	keep := 0
	stopMore := false
	for _, pf := range hra.prefetches {
		expected := hra.winOffset + uint64(keep+1)*MaxRPCChunkSize
		st := pf.state.Load()
		if pf.offset != expected || st == pfStateDiscarded || st == pfStatePromoted || stopMore {
			c.discardPrefetch(pf)
			continue
		}
		hra.prefetches[keep] = pf
		keep++
		if st == pfStateReady && pf.fetchedBytes.Load() < MaxRPCChunkSize {
			stopMore = true
		}
	}
	clear(hra.prefetches[keep:])
	hra.prefetches = hra.prefetches[:keep]
	if stopMore {
		return
	}

	for len(hra.prefetches) < targetDepth {
		if !c.tryAcquirePrefetchSlot() {
			break
		}
		nextOff := hra.winOffset + uint64(len(hra.prefetches)+1)*MaxRPCChunkSize
		pf := c.launchPrefetchLocked(triggerCtx, hra, nextOff, fetch)
		hra.prefetches = append(hra.prefetches, pf)
	}
}

func (c *ReadAheadCoordinator) popPrefetchForOffsetLocked(hra *handleReadAhead, alignedOffset uint64) *prefetchFuture {
	idx := -1
	for i, pf := range hra.prefetches {
		if pf.offset == alignedOffset {
			idx = i
			break
		}
	}
	if idx < 0 {
		return nil
	}
	for i := 0; i < idx; i++ {
		c.discardPrefetch(hra.prefetches[i])
		hra.prefetches[i] = nil
	}
	pf := hra.prefetches[idx]
	hra.prefetches[idx] = nil
	rem := copy(hra.prefetches, hra.prefetches[idx+1:])
	clear(hra.prefetches[rem:])
	hra.prefetches = hra.prefetches[:rem]
	return pf
}

func (c *ReadAheadCoordinator) fetchReadWindow(ctx context.Context, hra *handleReadAhead, offset uint64, fetch ChunkFetcher) (buf *sharedReadBuf, hit bool, err error) {
	pf := c.popPrefetchForOffsetLocked(hra, offset)
	if pf == nil && len(hra.prefetches) > 0 {
		c.discardAllPrefetchesLocked(hra)
	}

	startGen := hra.gen
	if pf != nil {
		hra.mu.Unlock()
		waitErr := c.waitDone(ctx, pf.done, pf.spanCtx)
		hra.mu.Lock()
		if waitErr != nil {
			c.discardPrefetch(pf)
			return nil, false, waitErr
		}
		if pf.err == nil && hra.gen == startGen {
			if promotedBuf, ok := c.promotePrefetch(pf); ok {
				c.setPipelinedLocked(hra, true)
				return promotedBuf, true, nil
			}
		}
		c.discardPrefetch(pf)
	}

	hra.mu.Unlock()
	payload, fetchErr := fetch(ctx, offset, MaxRPCChunkSize)
	hra.mu.Lock()
	if fetchErr != nil {
		return nil, false, fetchErr
	}
	newBuf := newSharedReadBuf(payload)
	c.metrics.Load().addReadAheadPrefetchedBytes(uint64(len(newBuf.bytes())))
	return newBuf, false, nil
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

		// 0. 若请求完全落在刚滑出的上一 4 MiB 窗口内（内核并发 readahead 跨边界乱序到达），直接从内存返回。
		if hra.prevWinValid && offset >= hra.prevWinOffset && offset+uint64(want) <= hra.prevWinOffset+uint64(len(hra.prevWinData())) {
			prevData := hra.prevWinData()
			relOff := int(offset - hra.prevWinOffset)
			copy(dest[:want], prevData[relOff:relOff+want])
			hra.prevWinConsumed = min(len(prevData), hra.prevWinConsumed+want)
			hra.seqCount++
			hra.streamEnd = max(hra.streamEnd, offset+uint64(want))
			hra.mu.Unlock()
			c.metrics.Load().addReadAheadHit(uint64(want))
			return want, nil
		}

		// 1. 若当前请求起始偏移位于异步预取流水线中，等待该预取完成并提升为活跃窗口，同时立即补齐后续流水线窗口。
		alignedReq := offset &^ uint64(MaxRPCChunkSize-1)
		if (!hra.winValid || offset < hra.winOffset || offset >= hra.winOffset+uint64(len(hra.winData()))) && len(hra.prefetches) > 0 {
			if pf := c.popPrefetchForOffsetLocked(hra, alignedReq); pf != nil {
				startGen := hra.gen
				hra.mu.Unlock()
				waitErr := c.waitDone(ctx, pf.done, pf.spanCtx)
				if waitErr != nil {
					c.discardPrefetch(pf)
					return 0, waitErr
				}
				hra.mu.Lock()
				if hra.gen == startGen && pf.err == nil {
					if pfBuf, ok := c.promotePrefetch(pf); ok {
						c.setPipelinedLocked(hra, true)
						pfData := pfBuf.bytes()
						if len(pfData) == 0 && offset >= pf.offset {
							// 预取已精确命中对齐边界上的 EOF，释放缓冲并清空更高偏移的无用预取，免发重复 RPC。
							pfBuf.release()
							hra.winEOF = true
							c.discardAllPrefetchesLocked(hra)
							hra.mu.Unlock()
							c.metrics.Load().addReadAheadHit(0)
							return 0, nil
						}
						c.installWindowLocked(hra, pf.offset, pfBuf, len(pfData) < MaxRPCChunkSize, 0)
						pfBuf.release()
						c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
					} else {
						c.discardPrefetch(pf)
					}
				} else {
					c.discardPrefetch(pf)
				}
			}
		}

		// 2. 命中活跃预读窗口
		if hra.winValid && offset >= hra.winOffset && offset < hra.winOffset+uint64(len(hra.winData())) {
			winData := hra.winData()
			relOff := int(offset - hra.winOffset)
			avail := len(winData) - relOff
			if avail >= want || hra.winEOF {
				n := min(want, avail)
				copy(dest[:n], winData[relOff:relOff+n])
				hra.winConsumed = min(len(winData), hra.winConsumed+n)
				hra.lastReadEnd = offset + uint64(n)
				hra.hasLastRead = true
				hra.seqCount++
				hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
				if !hra.winEOF {
					c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
				}
				hra.mu.Unlock()
				c.metrics.Load().addReadAheadHit(uint64(n))
				return n, nil
			}

			// 跨窗口边界读取：前缀取自当前窗口尾部，后缀从下一 4 MiB 窗口补齐
			prefix := make([]byte, avail)
			copy(prefix, winData[relOff:])
			hra.winConsumed = min(len(winData), hra.winConsumed+avail)
			nextWinOffset := hra.winOffset + uint64(len(winData))
			startGen := hra.gen

			nextBuf, fromPrefetch, err := c.fetchReadWindow(ctx, hra, nextWinOffset, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}
			nextData := nextBuf.bytes()
			rem := min(want-avail, len(nextData))
			if hra.gen == startGen {
				c.installWindowLocked(hra, nextWinOffset, nextBuf, len(nextData) < MaxRPCChunkSize, rem)
				c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
			} else if len(nextData) > rem {
				c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(nextData) - rem))
			}

			copy(dest[:avail], prefix)
			copy(dest[avail:avail+rem], nextData[:rem])
			nextBuf.release()

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
			winBuf, err := c.fetchAlignedWindow(ctx, hra, offset, fetch)
			if err != nil {
				hra.mu.Unlock()
				return 0, err
			}
			data := winBuf.bytes()
			winOffset := offset &^ uint64(MaxRPCChunkSize-1)
			rel := offset - winOffset
			winEOF := len(data) < MaxRPCChunkSize
			covers := rel <= uint64(len(data)) && (len(data)-int(rel) >= want || winEOF)
			if hra.gen != startGen {
				if covers {
					avail := len(data) - int(rel)
					n := min(want, avail)
					copy(dest[:n], data[rel:rel+uint64(n)])
					winBuf.release()
					if len(data) > n {
						c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(data) - n))
					}
					hra.mu.Unlock()
					return n, nil
				}
				c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(data)))
				winBuf.release()
			} else {
				if covers {
					avail := len(data) - int(rel)
					n := min(want, avail)
					c.installWindowLocked(hra, winOffset, winBuf, winEOF, n)
					copy(dest[:n], data[rel:rel+uint64(n)])
					winBuf.release()
					hra.lastReadEnd = offset + uint64(n)
					hra.hasLastRead = true
					hra.seqCount++
					hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
					if !hra.winEOF {
						c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
					}
					hra.mu.Unlock()
					return n, nil
				}
				if avail := len(data) - int(rel); avail > 0 {
					c.installWindowLocked(hra, winOffset, winBuf, winEOF, avail)
					prefix := make([]byte, avail)
					copy(prefix, data[rel:])
					nextWinOffset := winOffset + uint64(len(data))
					winBuf.release()

					nextBuf, fromPrefetch, err := c.fetchReadWindow(ctx, hra, nextWinOffset, fetch)
					if err != nil {
						hra.mu.Unlock()
						return 0, err
					}
					nextData := nextBuf.bytes()
					rem := min(want-avail, len(nextData))
					if hra.gen == startGen {
						c.installWindowLocked(hra, nextWinOffset, nextBuf, len(nextData) < MaxRPCChunkSize, rem)
						c.ensurePrefetchPipelineLocked(ctx, hra, fetch)
					} else if len(nextData) > rem {
						c.metrics.Load().addReadAheadDiscardedBytes(uint64(len(nextData) - rem))
					}
					copy(dest[:avail], prefix)
					copy(dest[avail:avail+rem], nextData[:rem])
					nextBuf.release()

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
				c.installWindowLocked(hra, winOffset, winBuf, winEOF, 0)
				winBuf.release()
			}
		}

		// 4. 首次读取或非连续随机读取：按请求尺寸直通，不产生多余读放大。
		if !hra.requestInWindowLocked(offset, want) {
			c.clearWindowAndPrefetchLocked(hra)
		}
		startGen := hra.gen
		hra.mu.Unlock()

		payload, err := fetch(ctx, offset, uint32(want))
		if err != nil {
			return 0, err
		}
		data := payload.Data
		n := min(want, len(data))
		copy(dest[:n], data[:n])
		payload.Release()

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

		payload, err := fetch(ctx, curOffset, uint32(chunkReqSize))
		if err != nil {
			if totalRead > 0 {
				break
			}
			return 0, err
		}
		chunkData := payload.Data
		if len(chunkData) == 0 {
			payload.Release()
			break
		}
		n := min(chunkReqSize, len(chunkData))
		copy(dest[totalRead:totalRead+n], chunkData[:n])
		payload.Release()
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
	set := c.byInode[ino]
	if len(set) == 0 {
		c.mu.Unlock()
		return
	}
	targets := make([]*handleReadAhead, 0, len(set))
	for _, hra := range set {
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
		if hra != nil {
			c.unbindInodeLocked(fh, hra.ino)
		}
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
	c.byInode = make(map[uint64]map[uint64]*handleReadAhead)
	c.mu.Unlock()

	for _, hra := range handles {
		hra.mu.Lock()
		c.resetHandleLocked(hra)
		hra.mu.Unlock()
	}
}
