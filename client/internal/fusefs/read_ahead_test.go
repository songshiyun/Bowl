package fusefs

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
)

func buildDeterministicFile(size int, seed uint64) []byte {
	out := make([]byte, size)
	x := seed ^ 0x9e3779b97f4a7c15
	for off := 0; off+8 <= size; off += 8 {
		x ^= x >> 12
		x ^= x << 25
		x ^= x >> 27
		binary.LittleEndian.PutUint64(out[off:off+8], x*0x2545f4914f6cdd1d)
	}
	for off := size &^ 7; off < size; off++ {
		out[off] = byte((seed + uint64(off)*31) & 0xff)
	}
	return out
}

func TestReadAheadCoordinator_GlobalInflightBudgetUnder16Streams(t *testing.T) {
	coord := NewReadAheadCoordinator()
	const numStreams = 16
	const fileSize = 32 * MaxRPCChunkSize

	var activePrefetches atomic.Int32
	var maxObservedPrefetches atomic.Int32
	allStreamsPrefetching := make(chan struct{})
	var allStreamsOnce sync.Once
	prefetchGate := make(chan struct{})
	var closeOnce sync.Once
	defer closeOnce.Do(func() { close(prefetchGate) })

	makeFetcher := func(fgAllowed *atomic.Bool) ChunkFetcher {
		return func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			if offset >= fileSize {
				return rpc.ReadPayload{}, nil
			}
			isPrefetch := !fgAllowed.Load()
			if isPrefetch {
				cur := activePrefetches.Add(1)
				for {
					prev := maxObservedPrefetches.Load()
					if cur <= prev || maxObservedPrefetches.CompareAndSwap(prev, cur) {
						break
					}
				}
				if cur == ReadAheadMaxGlobalInflight {
					allStreamsOnce.Do(func() { close(allStreamsPrefetching) })
				}
				defer activePrefetches.Add(-1)
				select {
				case <-prefetchGate:
				case <-ctx.Done():
					return rpc.ReadPayload{}, ctx.Err()
				}
			}
			rem := min(uint64(length), fileSize-offset)
			buf := bytes.Repeat([]byte{byte((offset >> 20) & 0xff)}, int(rem))
			return rpc.ReadPayload{Data: buf}, nil
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 16 路并发顺序流同时推进首窗口并激活预读流水线（复现 fio seq_read_16jobs 拓扑）
	var wg sync.WaitGroup
	for i := range numStreams {
		fh := uint64(1000 + i)
		ino := uint64(2000 + i)
		wg.Go(func() {
			var fgAllowed atomic.Bool
			fetcher := makeFetcher(&fgAllowed)
			dest := make([]byte, 1<<20)

			// 第 1 次读取直通，第 2 次读取建立 4 MiB 对齐窗口并激活预读流水线
			for step := range 3 {
				fgAllowed.Store(step < 2)
				n, err := coord.Read(ctx, fh, ino, uint64(step)*(1<<20), dest, fetcher)
				require.NoError(t, err)
				require.Equal(t, 1<<20, n)
			}
		})
	}
	wg.Wait()

	select {
	case <-allStreamsPrefetching:
	case <-time.After(2 * time.Second):
		t.Fatalf("16 路并发顺序流未全部获得专属异步预读槽位，当前最大并发=%d", maxObservedPrefetches.Load())
	}

	observed := maxObservedPrefetches.Load()
	require.Equal(t, int32(ReadAheadMaxGlobalInflight), observed,
		"16 路并发顺序流全局在途后台预读数必须精确收敛于 ReadAheadMaxGlobalInflight=%d（16 流稳态每流 2 槽双缓冲，且拒绝膨胀为 16*4=64）",
		ReadAheadMaxGlobalInflight,
	)
	require.Equal(t, int32(numStreams), coord.activePipelined.Load())

	// 放行闸门并移除全部句柄后，全局在途计数与活跃流计数必须严格归零
	closeOnce.Do(func() { close(prefetchGate) })
	for i := range numStreams {
		coord.RemoveHandle(uint64(1000 + i))
	}
	require.Equal(t, int32(0), coord.activePipelined.Load())
	require.Eventually(t, func() bool {
		return coord.inflightPrefetches.Load() == 0
	}, 2*time.Second, 10*time.Millisecond)
}

func TestReadAheadCoordinator_ConcurrentPrefetchWaitSingleflightZeroDuplicateRPC(t *testing.T) {
	coord := NewReadAheadCoordinator()
	metrics := NewMetrics()
	coord.bindMetrics(metrics)

	const (
		fh       = uint64(10)
		ino      = uint64(20)
		fileSize = 8 * MaxRPCChunkSize // 32 MiB
		subChunk = 256 * 1024
	)
	expected := buildDeterministicFile(fileSize, 0x10203040)

	var win1FetchCalls atomic.Int32
	win1PrefetchStarted := make(chan struct{})
	var startedOnce sync.Once
	win1Gate := make(chan struct{})
	var gateOnce sync.Once
	defer gateOnce.Do(func() { close(win1Gate) })

	fetcher := func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
		if offset >= fileSize {
			return rpc.ReadPayload{}, nil
		}
		if offset == MaxRPCChunkSize && length == MaxRPCChunkSize {
			win1FetchCalls.Add(1)
			startedOnce.Do(func() { close(win1PrefetchStarted) })
			select {
			case <-win1Gate:
			case <-ctx.Done():
				return rpc.ReadPayload{}, ctx.Err()
			}
		}
		end := min(int(offset)+int(length), fileSize)
		cp := append([]byte(nil), expected[int(offset):end]...)
		return rpc.ReadPayload{Data: cp}, nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 先顺序消费窗口 0 [0, 4 MiB - 128 KiB)，确保窗口 1 [4 MiB, 8 MiB) 的后台预取处于在途阻塞态。
	for off := 0; off < MaxRPCChunkSize-subChunk; off += subChunk {
		buf := make([]byte, subChunk)
		n, err := coord.Read(ctx, fh, ino, uint64(off), buf, fetcher)
		require.NoError(t, err)
		require.Equal(t, subChunk, n)
		require.Equal(t, expected[off:off+subChunk], buf)
	}

	select {
	case <-win1PrefetchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("窗口 1 [4 MiB, 8 MiB) 后台预取未在超时内启动")
	}

	// 模拟内核 FUSE MaxBackground 并发下发：1 个跨 4 MiB 边界请求 + 16 个窗口 1 内的 256 KiB 并发读同时到达并等待同一在途预取槽位。
	const win1SubChunks = MaxRPCChunkSize / subChunk // 16
	ready := make(chan struct{}, win1SubChunks+1)
	var wg sync.WaitGroup

	// 跨 [4 MiB - 128 KiB, 4 MiB + 128 KiB) 边界读
	wg.Go(func() {
		crossOff := MaxRPCChunkSize - subChunk/2
		buf := make([]byte, subChunk)
		ready <- struct{}{}
		n, err := coord.Read(ctx, fh, ino, uint64(crossOff), buf, fetcher)
		require.NoError(t, err)
		require.Equal(t, subChunk, n)
		require.Equal(t, expected[crossOff:crossOff+subChunk], buf)
	})

	for idx := range win1SubChunks {
		wg.Go(func() {
			off := MaxRPCChunkSize + idx*subChunk
			buf := make([]byte, subChunk)
			ready <- struct{}{}
			n, err := coord.Read(ctx, fh, ino, uint64(off), buf, fetcher)
			require.NoError(t, err)
			require.Equal(t, subChunk, n)
			require.Equal(t, expected[off:off+subChunk], buf)
		})
	}

	for range win1SubChunks + 1 {
		<-ready
	}
	// 留出调度窗口让全部 17 个并发读进入 ReadAheadCoordinator 等待队列，验证绝不逃逸出重复前台 RPC。
	time.Sleep(25 * time.Millisecond)
	require.Equal(t, int32(1), win1FetchCalls.Load(), "等待在途预取期间并发到达的同窗口读严禁重复发起 4 MiB Read RPC")

	gateOnce.Do(func() { close(win1Gate) })
	wg.Wait()

	require.Equal(t, int32(1), win1FetchCalls.Load(), "窗口 1 [4 MiB, 8 MiB) 全程只能发起唯一 1 次 RPC")
	coord.RemoveHandle(fh)
}

func TestReadAheadCoordinator_16StreamsConcurrentKernelReadaheadZeroAmplification(t *testing.T) {
	coord := NewReadAheadCoordinator()
	metrics := NewMetrics()
	coord.bindMetrics(metrics)

	const (
		numStreams       = 16
		workersPerStream = 4
		windowsPerStream = 4
		fileSize         = windowsPerStream * MaxRPCChunkSize // 16 MiB per stream
		subChunk         = 256 * 1024                         // 256 KiB Linux FUSE Read 粒度
		chunksPerStream  = fileSize / subChunk                // 64
	)

	refFiles := make([][]byte, numStreams)
	for s := range numStreams {
		refFiles[s] = buildDeterministicFile(fileSize, uint64(0xabc000+s))
	}

	// 记录每个 (streamIdx, alignedWindowIdx) 被发起 4 MiB RPC 的次数，验证零重复拉取与零窗口回滚。
	var windowFetches [numStreams][windowsPerStream]atomic.Int32
	var initPassThroughFetches [numStreams]atomic.Int32
	var stragglerPassThroughFetches [numStreams]atomic.Int32

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var streamWG sync.WaitGroup
	for s := range numStreams {
		streamIdx := s
		fh := uint64(5000 + streamIdx)
		ino := uint64(6000 + streamIdx)
		expected := refFiles[streamIdx]

		fetcher := func(fetchCtx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			select {
			case <-fetchCtx.Done():
				return rpc.ReadPayload{}, fetchCtx.Err()
			default:
			}
			// 注入交错延迟（奇偶窗口不同耗时），刻意制造跨协程唤醒竞争与快慢包交错。
			if (offset/MaxRPCChunkSize)%2 == 0 {
				time.Sleep(800 * time.Microsecond)
			} else {
				time.Sleep(200 * time.Microsecond)
			}
			if offset >= fileSize {
				return rpc.ReadPayload{}, nil
			}
			switch {
			case length == MaxRPCChunkSize:
				winIdx := offset / MaxRPCChunkSize
				windowFetches[streamIdx][winIdx].Add(1)
			case offset < 4*subChunk:
				initPassThroughFetches[streamIdx].Add(1)
			default:
				stragglerPassThroughFetches[streamIdx].Add(1)
			}
			end := min(int(offset)+int(length), fileSize)
			cp := append([]byte(nil), expected[int(offset):end]...)
			return rpc.ReadPayload{Data: cp}, nil
		}

		streamWG.Go(func() {
			// 对齐 Linux FUSE 内核 readahead 并发模型（bdi ra_pages 滑动窗口上限 <= 2 MiB = 8 x 256 KiB）：
			// 4 个后台工作线程在同一 fh 上乱序并发拉取，但最快与最慢请求间距受内核预读窗口约束（不会出现单线程倒退 12 MiB 的伪随机跳读）。
			const maxKernelRaWindowChunks = 8
			chunkCh := make(chan uint64, maxKernelRaWindowChunks)
			var doneChunks [chunksPerStream]atomic.Bool
			var producerWG sync.WaitGroup
			producerWG.Go(func() {
				defer close(chunkCh)
				low := uint64(0)
				for idx := range uint64(chunksPerStream) {
					for idx >= low+maxKernelRaWindowChunks {
						for low < uint64(chunksPerStream) && doneChunks[low].Load() {
							low++
						}
						if idx < low+maxKernelRaWindowChunks {
							break
						}
						time.Sleep(50 * time.Microsecond)
					}
					chunkCh <- idx
				}
			})

			var workerWG sync.WaitGroup
			for range workersPerStream {
				workerWG.Go(func() {
					buf := make([]byte, subChunk)
					for idx := range chunkCh {
						off := idx * subChunk
						n, err := coord.Read(ctx, fh, ino, off, buf, fetcher)
						require.NoError(t, err)
						require.Equal(t, subChunk, n)
						require.Equal(t, expected[off:off+subChunk], buf)
						doneChunks[idx].Store(true)
					}
				})
			}
			workerWG.Wait()
			producerWG.Wait()
			// EOF 探测读取必须返回 0 字节
			eofBuf := make([]byte, subChunk)
			nEOF, err := coord.Read(ctx, fh, ino, fileSize, eofBuf, fetcher)
			require.NoError(t, err)
			require.Zero(t, nEOF)
		})
	}
	streamWG.Wait()

	for s := range numStreams {
		require.Equal(t, int32(1), initPassThroughFetches[s].Load(), "流 %d 首窗口启动仅允许首请求 256 KiB 直通 1 次", s)
		require.Zero(t, stragglerPassThroughFetches[s].Load(), "流 %d 在内核 readahead 窗口约束下不得产生任何跨双窗口滞后直通", s)
		for w := range windowsPerStream {
			require.Equal(t, int32(1), windowFetches[s][w].Load(),
				"流 %d 窗口 %d (offset=%d MiB) 必须严格只拉取 1 次，严禁单飞逃逸或旧窗口回滚重拉",
				s, w, w*4,
			)
		}
		coord.RemoveHandle(uint64(5000 + s))
	}

	require.Equal(t, int32(0), coord.activePipelined.Load())
	require.Eventually(t, func() bool {
		return coord.inflightPrefetches.Load() == 0
	}, 2*time.Second, 10*time.Millisecond)

	// 16 个流 × 16 MiB 全程唯一未二次消费的仅限每个流首个 256 KiB 直通区间在首 4 MiB 窗口内的重叠部分（严格 <= 16 * 256 KiB = 4 MiB）。
	snap := metrics.Snapshot()
	require.LessOrEqual(t, snap.ReadAhead.DiscardedBytes, uint64(numStreams*subChunk),
		"16 路并发顺序读全程不得丢弃任何完整 4 MiB 预读窗口（历史故障曾丢弃 14.8 GiB）",
	)
}

func TestReadAheadCoordinator_LateWaiterAndConcurrentInvalidateSafety(t *testing.T) {
	t.Run("并发预读完成推进到高窗口后慢速同代读取绝不倒退winOffset或重置在途预取", func(t *testing.T) {
		coord := NewReadAheadCoordinator()
		const (
			fh       = uint64(88)
			ino      = uint64(99)
			fileSize = 6 * MaxRPCChunkSize // 24 MiB
			subChunk = 256 * 1024
		)
		expected := buildDeterministicFile(fileSize, 0x55aa1234)

		var winFetches [6]atomic.Int32
		win3PrefetchGate := make(chan struct{})
		win3Started := make(chan struct{})
		var win3Once sync.Once
		var releaseWin3 sync.Once
		defer releaseWin3.Do(func() { close(win3PrefetchGate) })

		fetcher := func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			if offset >= fileSize {
				return rpc.ReadPayload{}, nil
			}
			if length == MaxRPCChunkSize {
				idx := offset / MaxRPCChunkSize
				winFetches[idx].Add(1)
				if idx == 3 { // [12 MiB, 16 MiB)
					win3Once.Do(func() { close(win3Started) })
					select {
					case <-win3PrefetchGate:
					case <-ctx.Done():
						return rpc.ReadPayload{}, ctx.Err()
					}
				}
			}
			end := min(int(offset)+int(length), fileSize)
			cp := append([]byte(nil), expected[int(offset):end]...)
			return rpc.ReadPayload{Data: cp}, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 顺序推进至窗口 2 [8 MiB, 12 MiB)，使 currWin=8 MiB, prevWin=4 MiB，且窗口 3 (12 MiB) 处于在途预取态。
		for off := 0; off < 2*MaxRPCChunkSize+2*subChunk; off += subChunk {
			// 刻意跳过 4 MiB + 512 KiB 留给乱序慢读取验证
			if off == MaxRPCChunkSize+2*subChunk {
				continue
			}
			buf := make([]byte, subChunk)
			n, err := coord.Read(ctx, fh, ino, uint64(off), buf, fetcher)
			require.NoError(t, err)
			require.Equal(t, subChunk, n)
			require.Equal(t, expected[off:off+subChunk], buf)
		}

		select {
		case <-win3Started:
		case <-time.After(2 * time.Second):
			t.Fatal("窗口 3 [12 MiB, 16 MiB) 预取未启动")
		}

		// 下发乱序慢请求（跨 prevWin [4 MiB, 8 MiB) 与 currWin [8 MiB, 12 MiB) 边界：[8 MiB - 128 KiB, 8 MiB + 128 KiB)），
		// 验证直接从 prevWin + currWin 内存拼接返回，绝不倒退 winOffset 或取消在途的窗口 3 (12 MiB) 预取。
		straddleOff := 2*MaxRPCChunkSize - subChunk/2
		straddleBuf := make([]byte, subChunk)
		n, err := coord.Read(ctx, fh, ino, uint64(straddleOff), straddleBuf, fetcher)
		require.NoError(t, err)
		require.Equal(t, subChunk, n)
		require.Equal(t, expected[straddleOff:straddleOff+subChunk], straddleBuf)

		releaseWin3.Do(func() { close(win3PrefetchGate) })

		// 继续读完窗口 3 [12 MiB, 16 MiB)，验证在途预取未被取消且全窗口仅拉取 1 次。
		for off := 2*MaxRPCChunkSize + 2*subChunk; off < 4*MaxRPCChunkSize; off += subChunk {
			buf := make([]byte, subChunk)
			n, err := coord.Read(ctx, fh, ino, uint64(off), buf, fetcher)
			require.NoError(t, err)
			require.Equal(t, subChunk, n)
			require.Equal(t, expected[off:off+subChunk], buf)
		}

		for w := range 4 {
			require.Equal(t, int32(1), winFetches[w].Load(), "窗口 %d 必须严格只拉取 1 次", w)
		}
		coord.RemoveHandle(fh)
	})

	t.Run("前台等待在途预取期间触发InvalidateInode不得向活跃调用方泄露context.Canceled且必须读到覆写后新数据", func(t *testing.T) {
		coord := NewReadAheadCoordinator()
		const (
			fh       = uint64(188)
			ino      = uint64(199)
			fileSize = 3 * MaxRPCChunkSize // 12 MiB
			subChunk = 256 * 1024
		)

		var version atomic.Int32
		v1Data := bytes.Repeat([]byte{0x11}, fileSize)
		v2Data := bytes.Repeat([]byte{0x22}, fileSize)

		win1PrefetchEntered := make(chan struct{})
		var enteredOnce sync.Once

		fetcher := func(ctx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			if offset == MaxRPCChunkSize && length == MaxRPCChunkSize && version.Load() == 0 {
				enteredOnce.Do(func() { close(win1PrefetchEntered) })
				<-ctx.Done()
				return rpc.ReadPayload{}, ctx.Err()
			}
			if offset >= fileSize {
				return rpc.ReadPayload{}, nil
			}
			src := v1Data
			if version.Load() > 0 {
				src = v2Data
			}
			end := min(int(offset)+int(length), fileSize)
			cp := append([]byte(nil), src[int(offset):end]...)
			return rpc.ReadPayload{Data: cp}, nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for off := 0; off < MaxRPCChunkSize; off += subChunk {
			buf := make([]byte, subChunk)
			n, err := coord.Read(ctx, fh, ino, uint64(off), buf, fetcher)
			require.NoError(t, err)
			require.Equal(t, subChunk, n)
			require.Equal(t, v1Data[off:off+subChunk], buf)
		}

		select {
		case <-win1PrefetchEntered:
		case <-time.After(2 * time.Second):
			t.Fatal("旧版本窗口 1 预取未启动")
		}

		errCh := make(chan error, 1)
		gotBuf := make([]byte, subChunk)
		go func() {
			n, err := coord.Read(ctx, fh, ino, MaxRPCChunkSize, gotBuf, fetcher)
			if err == nil && n != subChunk {
				err = context.DeadlineExceeded
			}
			errCh <- err
		}()

		// 等待前台 Read 挂载到旧版本预取槽位后触发覆写失效：旧预取被取消，前台 Read 自动无缝重拉最新 v2 数据。
		time.Sleep(20 * time.Millisecond)
		version.Store(1)
		coord.InvalidateInode(ino)

		require.NoError(t, <-errCh)
		require.Equal(t, v2Data[MaxRPCChunkSize:MaxRPCChunkSize+subChunk], gotBuf)
		coord.RemoveHandle(fh)
	})
}

func TestReadAheadCoordinator_Random1MiBAcross1GiBZeroPrefetchAndZeroCancel(t *testing.T) {
	coord := NewReadAheadCoordinator()
	metrics := NewMetrics()
	coord.bindMetrics(metrics)

	const (
		numJobs    = 4
		readsPerFh = 256
		blockSize  = 1 << 20        // 1 MiB (fio direct_randread bs=1M)
		fileSize   = 1 << 30        // 1 GiB
		numBlocks  = fileSize >> 20 // 1024 blocks
	)

	var (
		amplifiedFetches atomic.Int64
		canceledFetches  atomic.Int64
		totalFetches     atomic.Int64
	)

	expectedBlockByte := func(ino, off uint64) byte {
		return byte(((ino * 131) ^ (off >> 20) ^ 0x5d) & 0xff)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for j := range numJobs {
		jobIdx := j
		fh := uint64(9000 + jobIdx)
		ino := uint64(9100 + jobIdx)

		fetcher := func(fetchCtx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
			totalFetches.Add(1)
			if length > blockSize {
				amplifiedFetches.Add(1)
			}
			select {
			case <-fetchCtx.Done():
				canceledFetches.Add(1)
				return rpc.ReadPayload{}, fetchCtx.Err()
			default:
			}
			rem := min(uint64(length), uint64(fileSize)-offset)
			out := make([]byte, rem)
			for pos := uint64(0); pos < rem; pos += blockSize {
				chunk := min(uint64(blockSize), rem-pos)
				b := expectedBlockByte(ino, offset+pos)
				for k := range chunk {
					out[pos+k] = b
				}
			}
			return rpc.ReadPayload{Data: out}, nil
		}

		wg.Go(func() {
			buf := make([]byte, blockSize)
			// 先注入一组典型的“相距 < 4 MiB 但不连续”跳跃偏移（旧实现必误触发 4 MiB 放大与后台取消），
			// 再以互质步长遍历 1 GiB 全空间非连续 1 MiB 偏移（步长 37 与 1024 互质，且非 +1 连续）。
			nearJumpBlocks := []uint64{100, 106, 108, 111, 500, 502, 505, 507}
			for step := range readsPerFh {
				var blk uint64
				if step < len(nearJumpBlocks) {
					blk = (nearJumpBlocks[step] + uint64(jobIdx)*64) % numBlocks
				} else {
					blk = (uint64(jobIdx)*251 + uint64(step)*37) % numBlocks
				}
				off := blk * blockSize
				n, err := coord.Read(ctx, fh, ino, off, buf, fetcher)
				require.NoError(t, err)
				require.Equal(t, blockSize, n)
				wantByte := expectedBlockByte(ino, off)
				require.Equal(t, wantByte, buf[0])
				require.Equal(t, wantByte, buf[blockSize/2])
				require.Equal(t, wantByte, buf[blockSize-1])
			}
			coord.RemoveHandle(fh)
		})
	}
	wg.Wait()

	require.Equal(t, int64(numJobs*readsPerFh), totalFetches.Load(), "每次随机 1 MiB 读应精确对应 1 次直通 RPC")
	require.Zero(t, amplifiedFetches.Load(), "1 GiB 随机 1 MiB 读严禁放大拉取 4 MiB 窗口")
	require.Zero(t, canceledFetches.Load(), "1 GiB 随机 1 MiB 读严禁触发后台预读取消（历史曾出现 120 次 CANCELED）")

	snap := metrics.Snapshot()
	require.Zero(t, snap.ReadAhead.Triggers, "随机 1 MiB 跳读不得触发任何后台 Prefetch")
	require.Zero(t, snap.ReadAhead.PrefetchedBytes, "随机 1 MiB 跳读不得产生任何预读字节")
	require.Zero(t, snap.ReadAhead.DiscardedBytes, "随机 1 MiB 跳读丢弃字节必须为 0")
}
