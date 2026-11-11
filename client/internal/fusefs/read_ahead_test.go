package fusefs

import (
	"bytes"
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/rpc"
)

func TestReadAheadCoordinator_GlobalInflightBudgetUnder16Streams(t *testing.T) {
	coord := NewReadAheadCoordinator()
	const numStreams = 16
	const fileSize = 32 * MaxRPCChunkSize

	var activePrefetches atomic.Int32
	var maxObservedPrefetches atomic.Int32
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

	ctx := context.Background()
	dest := make([]byte, 1<<20)

	// 16 路并发顺序流同时推进首窗口并激活预读流水线（复现 fio seq_read_16jobs 拓扑）
	var wg sync.WaitGroup
	for i := range numStreams {
		fh := uint64(1000 + i)
		ino := uint64(2000 + i)
		wg.Go(func() {
			var fgAllowed atomic.Bool
			fetcher := makeFetcher(&fgAllowed)

			// 第 1 次读取直通，第 2 次读取建立 4 MiB 对齐窗口，第 3 次命中窗口超过 1 MiB 升级为 pipelined
			for step := range 3 {
				fgAllowed.Store(step < 2)
				n, err := coord.Read(ctx, fh, ino, uint64(step)*(1<<20), dest, fetcher)
				require.NoError(t, err)
				require.Equal(t, 1<<20, n)
			}
		})
	}
	wg.Wait()

	// 给后台预读 goroutine 进入 fetcher 闸门留出调度窗口
	time.Sleep(30 * time.Millisecond)

	observed := maxObservedPrefetches.Load()
	require.Positive(t, observed, "应正常启动后台异步预读")
	require.LessOrEqual(t, observed, int32(ReadAheadMaxGlobalInflight),
		"16 路并发顺序流全局在途后台预读数必须严格受控于 ReadAheadMaxGlobalInflight=%d（拒绝膨胀为 16*4=64）",
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
