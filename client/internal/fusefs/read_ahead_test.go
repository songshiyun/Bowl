package fusefs_test

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func makeDeterministicFile(size int) []byte {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte((i*31 + i/4096*17 + 7) & 0xff)
	}
	return buf
}

func TestReadAheadCoordinator顺序读对齐窗口命中与异步预取提升(t *testing.T) {
	const (
		chunkSize = fusefs.MaxRPCChunkSize
		totalSize = 3 * chunkSize
		fh        = uint64(10)
		ino       = uint64(100)
	)
	fileData := makeDeterministicFile(totalSize)
	coord := fusefs.NewReadAheadCoordinator()

	type callRecord struct {
		offset uint64
		length uint32
	}
	var callsMu sync.Mutex
	var calls []callRecord
	var prefetch1Done = make(chan struct{})
	var prefetch2Done = make(chan struct{})
	var callCount atomic.Int32

	fetcher := func(_ context.Context, offset uint64, length uint32) ([]byte, error) {
		idx := callCount.Add(1)
		callsMu.Lock()
		calls = append(calls, callRecord{offset: offset, length: length})
		callsMu.Unlock()
		end := min(int(offset)+int(length), len(fileData))
		var out []byte
		if int(offset) < end {
			out = append([]byte(nil), fileData[offset:end]...)
		}
		if idx == 3 {
			// 第 3 次调用是窗口 0 发布后自动触发的窗口 1 ([4 MiB, 8 MiB)) 异步预取
			defer close(prefetch1Done)
		}
		if idx == 4 {
			// 第 4 次调用是窗口 1 提升为活跃窗口后自动触发的窗口 2 ([8 MiB, 12 MiB)) 异步预取
			defer close(prefetch2Done)
		}
		return out, nil
	}

	ctx := context.Background()

	// 1. 首个读请求（[0, 64 KiB)）直通，不放大为 4 MiB
	buf1 := make([]byte, 64*1024)
	n, err := coord.Read(ctx, fh, ino, 0, buf1, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf1), n)
	require.True(t, bytes.Equal(fileData[:len(buf1)], buf1))
	require.Equal(t, int32(1), callCount.Load())
	callsMu.Lock()
	require.Equal(t, callRecord{offset: 0, length: 64 * 1024}, calls[0])
	callsMu.Unlock()

	// 2. 紧接上一次末尾的顺序读（[64 KiB, 128 KiB)）：触发窗口 0 ([0, 4 MiB)) 对齐拉取，并启动窗口 1 ([4 MiB, 8 MiB)) 异步预取
	buf2 := make([]byte, 64*1024)
	n, err = coord.Read(ctx, fh, ino, 64*1024, buf2, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf2), n)
	require.True(t, bytes.Equal(fileData[64*1024:128*1024], buf2))

	<-prefetch1Done
	require.Equal(t, int32(3), callCount.Load())
	callsMu.Lock()
	require.Equal(t, callRecord{offset: 0, length: chunkSize}, calls[1])
	require.Equal(t, callRecord{offset: chunkSize, length: chunkSize}, calls[2])
	callsMu.Unlock()

	// 3. 同一 4 MiB 窗口内的后续读取（[128 KiB, 4 MiB)）直接命中内存活跃窗口，不再调用 fetcher
	buf3 := make([]byte, chunkSize-128*1024)
	n, err = coord.Read(ctx, fh, ino, 128*1024, buf3, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf3), n)
	require.True(t, bytes.Equal(fileData[128*1024:chunkSize], buf3))
	require.Equal(t, int32(3), callCount.Load())

	// 4. 读取进入窗口 1（[4 MiB, 4 MiB + 128 KiB)）：直接将已预取的窗口 1 提升（Promote）为活跃窗口，并异步预取窗口 2 ([8 MiB, 12 MiB))
	buf4 := make([]byte, 128*1024)
	n, err = coord.Read(ctx, fh, ino, chunkSize, buf4, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf4), n)
	require.True(t, bytes.Equal(fileData[chunkSize:chunkSize+128*1024], buf4))

	<-prefetch2Done
	require.Equal(t, int32(4), callCount.Load())
	callsMu.Lock()
	require.Equal(t, callRecord{offset: 2 * chunkSize, length: chunkSize}, calls[3])
	callsMu.Unlock()
}

func TestReadAheadCoordinator跨窗口边界拼接与后半窗口短读EOF(t *testing.T) {
	const (
		chunkSize = fusefs.MaxRPCChunkSize
		// 2 个完整 4 MiB 窗口 + 第 3 个窗口仅 100 KiB（EOF 短读）
		tailBytes = 100 * 1024
		totalSize = 2*chunkSize + tailBytes
		fh        = uint64(20)
		ino       = uint64(200)
	)
	fileData := makeDeterministicFile(totalSize)
	coord := fusefs.NewReadAheadCoordinator()

	fetcher := func(_ context.Context, offset uint64, length uint32) ([]byte, error) {
		if int(offset) >= len(fileData) {
			return nil, nil
		}
		end := min(int(offset)+int(length), len(fileData))
		return append([]byte(nil), fileData[offset:end]...), nil
	}

	ctx := context.Background()

	// 1. 首次读取 [0, 64 KiB) 直通
	buf1 := make([]byte, 64*1024)
	n, err := coord.Read(ctx, fh, ino, 0, buf1, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf1), n)
	require.True(t, bytes.Equal(fileData[:len(buf1)], buf1))

	// 2. 第二次顺序读取 [64 KiB, 128 KiB) 建立活跃窗口 0 ([0, 4 MiB))
	buf2 := make([]byte, 64*1024)
	n, err = coord.Read(ctx, fh, ino, 64*1024, buf2, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf2), n)
	require.True(t, bytes.Equal(fileData[64*1024:128*1024], buf2))

	// 3. 在活跃窗口 0 命中时跨 4 MiB 边界读取 [4 MiB - 128 KiB, 4 MiB + 192 KiB)（总长 320 KiB）
	crossStart1 := uint64(chunkSize - 128*1024)
	crossWant1 := 320 * 1024
	bufCross1 := make([]byte, crossWant1)
	n, err = coord.Read(ctx, fh, ino, crossStart1, bufCross1, fetcher)
	require.NoError(t, err)
	require.Equal(t, crossWant1, n)
	require.True(t, bytes.Equal(fileData[crossStart1:int(crossStart1)+crossWant1], bufCross1))

	// 4. 此时活跃窗口已推进至窗口 1 ([4 MiB, 8 MiB))，在窗口 1 尾部跨 8 MiB 边界读取 [8 MiB - 64 KiB, 8 MiB + 256 KiB)
	//    由于窗口 2 ([8 MiB, 8 MiB + 100 KiB)) 在 100 KiB 处到达 EOF，拼接结果长度应为 64 KiB + 100 KiB = 164 KiB
	crossStart2 := uint64(2*chunkSize - 64*1024)
	crossWant2 := 64*1024 + 256*1024
	expectedRead2 := 64*1024 + tailBytes
	bufCross2 := make([]byte, crossWant2)
	n, err = coord.Read(ctx, fh, ino, crossStart2, bufCross2, fetcher)
	require.NoError(t, err)
	require.Equal(t, expectedRead2, n)
	require.True(t, bytes.Equal(fileData[crossStart2:int(crossStart2)+expectedRead2], bufCross2[:n]))

	// 5. 另开新句柄测试未命中活跃窗口时直接发起跨 4 MiB 边界顺序读：
	//    先在 [4 MiB - 128 KiB, 4 MiB - 64 KiB) 首读直通，再顺序读 [4 MiB - 64 KiB, 4 MiB + 64 KiB)
	const fh2 = uint64(21)
	initBuf := make([]byte, 64*1024)
	n, err = coord.Read(ctx, fh2, ino, uint64(chunkSize-128*1024), initBuf, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(initBuf), n)

	straddleStart := uint64(chunkSize - 64*1024)
	straddleBuf := make([]byte, 128*1024)
	n, err = coord.Read(ctx, fh2, ino, straddleStart, straddleBuf, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(straddleBuf), n)
	require.True(t, bytes.Equal(fileData[straddleStart:int(straddleStart)+len(straddleBuf)], straddleBuf))
}

func TestReadAheadCoordinator并发读取同一窗口Singleflight单飞合并(t *testing.T) {
	const (
		chunkSize = fusefs.MaxRPCChunkSize
		fh        = uint64(30)
		ino       = uint64(300)
		readers   = 8
		readSize  = 256 * 1024 // >= 128 KiB (readAheadMinWant)
	)
	fileData := makeDeterministicFile(chunkSize) // 恰好 1 个窗口（EOF，不会触发窗口 1 异步预取）
	coord := fusefs.NewReadAheadCoordinator()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var window0Fetches atomic.Int32
	fetchEntered := make(chan struct{})
	releaseFetch := make(chan struct{})

	fetcher := func(ctx context.Context, offset uint64, length uint32) ([]byte, error) {
		if offset == 0 && length == chunkSize {
			if window0Fetches.Add(1) == 1 {
				close(fetchEntered)
			}
			select {
			case <-releaseFetch:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		end := min(int(offset)+int(length), len(fileData))
		return append([]byte(nil), fileData[offset:end]...), nil
	}

	// 1. 首次读取 [0, 128 KiB) 直通，推进 streamEnd 至 128 KiB
	firstBuf := make([]byte, 128*1024)
	n, err := coord.Read(ctx, fh, ino, 0, firstBuf, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(firstBuf), n)
	require.True(t, bytes.Equal(fileData[:len(firstBuf)], firstBuf))

	// 2. 启动第 1 个并发读（偏移 128 KiB），使它进入对齐窗口 0 ([0, 4 MiB)) 单飞拉取并阻塞在 releaseFetch
	results := make([][]byte, readers)
	errs := make([]error, readers)
	var wg sync.WaitGroup
	wg.Add(readers)

	go func() {
		defer wg.Done()
		buf := make([]byte, readSize)
		off := uint64(128 * 1024)
		got, readErr := coord.Read(ctx, fh, ino, off, buf, fetcher)
		if readErr == nil {
			results[0] = buf[:got]
		}
		errs[0] = readErr
	}()

	select {
	case <-fetchEntered:
	case <-ctx.Done():
		t.Fatal("等待首个窗口拉取进入超时")
	}

	// 3. 并发启动其余 7 个 Goroutine 读取同一 4 MiB 窗口内的不同子区间（乱序到达，模拟内核 MaxBackground 并发预读）
	ready := make(chan struct{})
	for i := 1; i < readers; i++ {
		idx := i
		go func() {
			defer wg.Done()
			<-ready
			buf := make([]byte, readSize)
			off := uint64(idx * readSize)
			got, readErr := coord.Read(ctx, fh, ino, off, buf, fetcher)
			if readErr == nil {
				results[idx] = buf[:got]
			}
			errs[idx] = readErr
		}()
	}
	close(ready)
	// 给并发读 Goroutine 进入 fetchAlignedWindow 单飞等待留出调度窗口
	time.Sleep(10 * time.Millisecond)
	close(releaseFetch)
	wg.Wait()

	// 断言底层 4 MiB 对齐窗口 ChunkFetcher 仅被调用 1 次，且所有并发读均拿到各自的精确数据区间
	require.Equal(t, int32(1), window0Fetches.Load(), "同一 4 MiB 对齐窗口的并发读必须通过 Singleflight 合并为 1 次拉取")
	for i := range readers {
		require.NoError(t, errs[i])
		var expectedOff int
		if i == 0 {
			expectedOff = 128 * 1024
		} else {
			expectedOff = i * readSize
		}
		require.True(t, bytes.Equal(fileData[expectedOff:expectedOff+readSize], results[i]), "并发读 #%d 数据不匹配", i)
	}
}

func TestReadAheadCoordinator在途拉取并发InvalidateInode递增Gen拒绝旧窗口污染缓存(t *testing.T) {
	const (
		chunkSize = fusefs.MaxRPCChunkSize
		fh        = uint64(40)
		ino       = uint64(400)
	)
	oldData := bytes.Repeat([]byte{0xAA}, chunkSize)
	newData := bytes.Repeat([]byte{0xBB}, chunkSize)
	coord := fusefs.NewReadAheadCoordinator()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var useNewData atomic.Bool
	var fetchCalls atomic.Int32
	inFlightStarted := make(chan struct{})
	releaseInFlight := make(chan struct{})

	fetcher := func(ctx context.Context, offset uint64, length uint32) ([]byte, error) {
		callIdx := fetchCalls.Add(1)
		if callIdx == 2 {
			// 第 2 次调用是顺序读触发的 4 MiB 对齐窗口在途拉取
			close(inFlightStarted)
			select {
			case <-releaseInFlight:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			// 模拟在途拉取返回写发生前的旧窗口数据
			return append([]byte(nil), oldData[offset:int(offset)+int(length)]...), nil
		}
		src := oldData
		if useNewData.Load() {
			src = newData
		}
		end := min(int(offset)+int(length), len(src))
		return append([]byte(nil), src[offset:end]...), nil
	}

	// 1. 首次读取 [0, 64 KiB) 直通
	buf1 := make([]byte, 64*1024)
	n, err := coord.Read(ctx, fh, ino, 0, buf1, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf1), n)
	require.True(t, bytes.Equal(oldData[:len(buf1)], buf1))

	// 2. 启动第 2 次顺序读 [64 KiB, 128 KiB)，使其进入 4 MiB 对齐窗口在途拉取并暂停
	readDone := make(chan error, 1)
	buf2 := make([]byte, 64*1024)
	go func() {
		got, readErr := coord.Read(ctx, fh, ino, 64*1024, buf2, fetcher)
		if readErr == nil && got != len(buf2) {
			readErr = context.Canceled
		}
		readDone <- readErr
	}()

	select {
	case <-inFlightStarted:
	case <-ctx.Done():
		t.Fatal("等待在途窗口拉取启动超时")
	}

	// 3. 在拉取尚未返回期间，模拟并发 Write / SetAttr / Fallocate 更新文件数据并调用 InvalidateInode(ino)
	useNewData.Store(true)
	coord.InvalidateInode(ino)

	// 4. 放行在途旧拉取完成返回
	close(releaseInFlight)
	require.NoError(t, <-readDone)
	require.True(t, bytes.Equal(oldData[64*1024:128*1024], buf2))

	// 5. 断言在途返回的旧窗口绝未发布为活跃缓存：紧接着读取 [128 KiB, 192 KiB) 必须穿透底层 fetcher 拿到新数据 0xBB
	buf3 := make([]byte, 64*1024)
	n, err = coord.Read(ctx, fh, ino, 128*1024, buf3, fetcher)
	require.NoError(t, err)
	require.Equal(t, len(buf3), n)
	require.True(t, bytes.Equal(newData[128*1024:192*1024], buf3), "并发 InvalidateInode 后的在途旧窗口不得污染缓存")
	require.Equal(t, int32(3), fetchCalls.Load(), "旧窗口被丢弃后，后续读必须重新穿透底层 ChunkFetcher")
}

func TestReadAheadCoordinator大读分片循环与随机小读直通(t *testing.T) {
	const (
		chunkSize = fusefs.MaxRPCChunkSize
		totalSize = 2*chunkSize + 512*1024
		fh        = uint64(50)
		ino       = uint64(500)
	)
	fileData := makeDeterministicFile(totalSize)
	coord := fusefs.NewReadAheadCoordinator()

	type callRecord struct {
		offset uint64
		length uint32
	}
	var calls []callRecord

	fetcher := func(_ context.Context, offset uint64, length uint32) ([]byte, error) {
		calls = append(calls, callRecord{offset: offset, length: length})
		if int(offset) >= len(fileData) {
			return nil, nil
		}
		end := min(int(offset)+int(length), len(fileData))
		return append([]byte(nil), fileData[offset:end]...), nil
	}

	ctx := context.Background()

	// 1. 非连续随机小读（< 128 KiB）：每次均按请求尺寸直通，不放大为 4 MiB
	randomOffsets := []uint64{0, 512 * 1024, 128 * 1024, 3*1024*1024 + 4096}
	for i, off := range randomOffsets {
		smallBuf := make([]byte, 16*1024)
		n, err := coord.Read(ctx, fh, ino, off, smallBuf, fetcher)
		require.NoError(t, err)
		require.Equal(t, len(smallBuf), n)
		require.True(t, bytes.Equal(fileData[off:off+uint64(len(smallBuf))], smallBuf))
		require.Equal(t, callRecord{offset: off, length: 16 * 1024}, calls[i], "随机小读 #%d 不得产生读放大", i)
	}

	// 2. > 4 MiB 大读（请求 10 MiB，文件实际剩余 8.5 MiB）：按 4 MiB 顺序分片循环并在第 3 片短读 EOF 处停止
	calls = nil
	largeBuf := make([]byte, 10*1024*1024)
	n, err := coord.Read(ctx, fh, ino, 0, largeBuf, fetcher)
	require.NoError(t, err)
	require.Equal(t, totalSize, n)
	require.True(t, bytes.Equal(fileData, largeBuf[:n]))
	require.Equal(t, []callRecord{
		{offset: 0, length: chunkSize},
		{offset: chunkSize, length: chunkSize},
		{offset: 2 * chunkSize, length: 2 * 1024 * 1024},
	}, calls)

	// 3. > 4 MiB 大读中途分片报错：已读出前片数据时按 POSIX 短读返回已读字节与 nil error；首片报错则返回 error
	sentinelErr := errors.New("simulated network failure on second chunk")
	partialBuf := make([]byte, 2*chunkSize)
	var chunkIdx int
	n, err = coord.Read(ctx, fh, ino, 0, partialBuf, func(_ context.Context, offset uint64, length uint32) ([]byte, error) {
		chunkIdx++
		if chunkIdx == 2 {
			return nil, sentinelErr
		}
		return append([]byte(nil), fileData[offset:int(offset)+int(length)]...), nil
	})
	require.NoError(t, err)
	require.Equal(t, chunkSize, n)
	require.True(t, bytes.Equal(fileData[:chunkSize], partialBuf[:n]))

	_, err = coord.Read(ctx, fh, ino, 0, partialBuf, func(_ context.Context, _ uint64, _ uint32) ([]byte, error) {
		return nil, sentinelErr
	})
	require.ErrorIs(t, err, sentinelErr)
}

func TestReadAheadCoordinator生命周期清理即时取消在途预取(t *testing.T) {
	const chunkSize = fusefs.MaxRPCChunkSize

	// 反射守卫：ReadAheadCoordinator 不得再保留 InvalidateSession 方法
	coordType := reflect.TypeFor[*fusefs.ReadAheadCoordinator]()
	_, hasInvalidateSession := coordType.MethodByName("InvalidateSession")
	require.False(t, hasInvalidateSession, "ReadAheadCoordinator 不得保留 InvalidateSession 方法")

	triggerPrefetchOnHandle := func(t *testing.T, coord *fusefs.ReadAheadCoordinator, fh, ino uint64) <-chan error {
		t.Helper()
		prefetchStarted := make(chan struct{})
		prefetchCanceled := make(chan error, 1)

		fetcher := func(ctx context.Context, offset uint64, length uint32) ([]byte, error) {
			if offset == chunkSize && length == chunkSize {
				close(prefetchStarted)
				<-ctx.Done()
				prefetchCanceled <- ctx.Err()
				return nil, ctx.Err()
			}
			return bytes.Repeat([]byte{byte(fh)}, int(length)), nil
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// 首次读直通 + 第二次顺序读触发窗口 0 拉取并启动窗口 1 ([4 MiB, 8 MiB)) 异步预取
		buf := make([]byte, 64*1024)
		_, err := coord.Read(ctx, fh, ino, 0, buf, fetcher)
		require.NoError(t, err)
		_, err = coord.Read(ctx, fh, ino, 64*1024, buf, fetcher)
		require.NoError(t, err)

		select {
		case <-prefetchStarted:
		case <-ctx.Done():
			t.Fatalf("等待 fh=%d 异步预取启动超时", fh)
		}
		return prefetchCanceled
	}

	t.Run("RemoveHandle取消对应句柄在途预取并清理状态", func(t *testing.T) {
		coord := fusefs.NewReadAheadCoordinator()
		canceledCh := triggerPrefetchOnHandle(t, coord, 61, 601)

		coord.RemoveHandle(61)

		select {
		case err := <-canceledCh:
			require.ErrorIs(t, err, context.Canceled)
		case <-time.After(2 * time.Second):
			t.Fatal("RemoveHandle 未及时取消在途预取 Context")
		}

		// 移除句柄后再次以同一 fh 读取，必须作为全新句柄首读直通（不命中旧窗口 0）
		var gotLengths []uint32
		buf := make([]byte, 32*1024)
		n, err := coord.Read(context.Background(), 61, 601, 128*1024, buf, func(_ context.Context, _ uint64, length uint32) ([]byte, error) {
			gotLengths = append(gotLengths, length)
			return bytes.Repeat([]byte{0x5A}, int(length)), nil
		})
		require.NoError(t, err)
		require.Equal(t, len(buf), n)
		require.Equal(t, []uint32{32 * 1024}, gotLengths)
	})

	t.Run("Clear取消所有句柄在途预取并清空全部状态", func(t *testing.T) {
		coord := fusefs.NewReadAheadCoordinator()
		canceled1 := triggerPrefetchOnHandle(t, coord, 81, 801)
		canceled2 := triggerPrefetchOnHandle(t, coord, 82, 802)

		coord.Clear()

		for i, ch := range []<-chan error{canceled1, canceled2} {
			select {
			case err := <-ch:
				require.ErrorIs(t, err, context.Canceled, "句柄 #%d 预取未取消", i)
			case <-time.After(2 * time.Second):
				t.Fatalf("Clear 未及时取消句柄 #%d 的在途预取", i)
			}
		}
	})
}

type fakeStorageClientForReadAhead struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mu          sync.Mutex
	fileData    []byte
	readCalls   int
	readOffsets []uint64
	readSizes   []uint32
	onRead4MiB  func(ctx context.Context, offset uint64)
}

func (f *fakeStorageClientForReadAhead) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	offset := req.Msg.GetOffset()
	size := req.Msg.GetSize()
	f.mu.Lock()
	f.readCalls++
	f.readOffsets = append(f.readOffsets, offset)
	f.readSizes = append(f.readSizes, size)
	hook := f.onRead4MiB
	f.mu.Unlock()

	if size == fusefs.MaxRPCChunkSize && hook != nil {
		hook(ctx, offset)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if int(offset) >= len(f.fileData) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	end := min(int(offset)+int(size), len(f.fileData))
	out := append([]byte(nil), f.fileData[offset:end]...)
	return connect.NewResponse(&storagev1.ReadResponse{Data: out}), nil
}

func (f *fakeStorageClientForReadAhead) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off := int(req.Msg.GetOffset())
	data := req.Msg.GetData()
	if off+len(data) > len(f.fileData) {
		grown := make([]byte, off+len(data))
		copy(grown, f.fileData)
		f.fileData = grown
	}
	copy(f.fileData[off:off+len(data)], data)
	return connect.NewResponse(&storagev1.WriteResponse{
		Size: uint32(len(data)),
		Inode: &storagev1.Inode{
			Ino:  req.Msg.GetIno(),
			Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644, Size: uint64(len(f.fileData)), Nlink: 1},
		},
	}), nil
}

func (f *fakeStorageClientForReadAhead) SetAttr(_ context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.Msg.Size != nil {
		newSize := int(req.Msg.GetSize())
		if newSize < len(f.fileData) {
			f.fileData = f.fileData[:newSize]
		} else if newSize > len(f.fileData) {
			grown := make([]byte, newSize)
			copy(grown, f.fileData)
			f.fileData = grown
		}
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode: &storagev1.Inode{
			Ino:  req.Msg.GetIno(),
			Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644, Size: uint64(len(f.fileData)), Nlink: 1},
		},
	}), nil
}

func (f *fakeStorageClientForReadAhead) Fallocate(_ context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off := int(req.Msg.GetOffset())
	length := int(req.Msg.GetLength())
	for i := off; i < min(off+length, len(f.fileData)); i++ {
		f.fileData[i] = 0
	}
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (f *fakeStorageClientForReadAhead) Fsync(_ context.Context, _ *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func TestRawFS预读协调器与读写截断及生命周期联动(t *testing.T) {
	const (
		chunkSize = fusefs.MaxRPCChunkSize
		nodeID    = uint64(100)
		fh        = uint64(11)
	)

	// 结构体反射守卫：RawFS 不得再暴露旧版 readAheadMu、readAheadHandles、sessions 或 orphanSessions 字段
	rawFSType := reflect.TypeFor[fusefs.RawFS]()
	for _, forbidden := range []string{"readAheadMu", "readAheadHandles", "sessions", "orphanSessions"} {
		_, hasOld := rawFSType.FieldByName(forbidden)
		require.False(t, hasOld, "RawFS 不得保留旧字段 %s", forbidden)
	}
	orphanField, hasOrphanInodes := rawFSType.FieldByName("orphanInodes")
	require.True(t, hasOrphanInodes, "RawFS 应持有无会话 orphanInodes 集合")
	require.Equal(t, reflect.TypeFor[map[uint64]struct{}](), orphanField.Type)

	client := &fakeStorageClientForReadAhead{
		fileData: bytes.Repeat([]byte{0x11}, 2*chunkSize),
	}

	fs := fusefs.NewRawFS(client, fusefs.Config{DiskID: "disk-1", Epoch: 1, RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, nodeID, fh, syscall.O_RDWR)

	readAt := func(offset uint64, size uint32) ([]byte, fuse.Status) {
		buf := make([]byte, size)
		res, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   offset,
			Size:     size,
		}, buf)
		if st != fuse.OK {
			return nil, st
		}
		out, st := res.Bytes(buf)
		return append([]byte(nil), out...), st
	}

	// 1. 首读 [0, 64 KiB) 直通 + 第二次顺序读 [64 KiB, 128 KiB) 建立 4 MiB 活跃窗口
	data, st := readAt(0, 64*1024)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, bytes.Repeat([]byte{0x11}, 64*1024), data)

	data, st = readAt(64*1024, 64*1024)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, bytes.Repeat([]byte{0x11}, 64*1024), data)

	// 2. Write 失效预读缓存：随后读取 [128 KiB, 192 KiB) 必须穿透拿到新写入的 0x22
	patchWrite := bytes.Repeat([]byte{0x22}, 64*1024)
	written, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
		Offset:   128 * 1024,
		Size:     uint32(len(patchWrite)),
	}, patchWrite)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(len(patchWrite)), written)

	data, st = readAt(128*1024, 64*1024)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, patchWrite, data, "Write 后读取必须命中最新数据而非旧预读窗口")

	// 再次顺序读建立窗口后，验证 Fallocate 打洞失效预读窗口
	_, st = readAt(192*1024, 64*1024)
	require.Equal(t, fuse.OK, st)
	st = fs.Fallocate(nil, &fuse.FallocateIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
		Offset:   256 * 1024,
		Length:   64 * 1024,
		Mode:     unix.FALLOC_FL_PUNCH_HOLE | unix.FALLOC_FL_KEEP_SIZE,
	})
	require.Equal(t, fuse.OK, st)
	data, st = readAt(256*1024, 64*1024)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, make([]byte, 64*1024), data, "Fallocate 打洞后读取必须返回清零数据而非旧预读窗口")

	// 再次顺序读建立窗口后，验证 SetAttr 截断失效预读窗口
	_, st = readAt(320*1024, 64*1024)
	require.Equal(t, fuse.OK, st)
	var attrOut fuse.AttrOut
	st = fs.SetAttr(nil, &fuse.SetAttrIn{
		SetAttrInCommon: fuse.SetAttrInCommon{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Valid:    fuse.FATTR_SIZE,
			Size:     384 * 1024,
		},
	}, &attrOut)
	require.Equal(t, fuse.OK, st)
	data, st = readAt(384*1024, 64*1024)
	require.Equal(t, fuse.OK, st)
	require.Empty(t, data, "SetAttr 截断后越界读取必须返回 EOF 空数据而非旧预读窗口")

	// 3. 验证 Release 立即取消该句柄在途预取
	const fh1 = uint64(110)
	fs.PrepareTestFileHandle(t, nodeID, fh1, syscall.O_RDONLY)

	readAtFh := func(targetFh, offset uint64, size uint32) ([]byte, fuse.Status) {
		buf := make([]byte, size)
		res, readSt := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       targetFh,
			Offset:   offset,
			Size:     size,
		}, buf)
		if readSt != fuse.OK {
			return nil, readSt
		}
		out, readSt := res.Bytes(buf)
		return append([]byte(nil), out...), readSt
	}

	client.mu.Lock()
	client.fileData = bytes.Repeat([]byte{0x33}, 2*chunkSize)
	prefetchStarted := make(chan struct{})
	prefetchCanceled := make(chan error, 1)
	var once1 sync.Once
	client.onRead4MiB = func(ctx context.Context, offset uint64) {
		if offset == chunkSize {
			once1.Do(func() {
				close(prefetchStarted)
				<-ctx.Done()
				prefetchCanceled <- ctx.Err()
			})
		}
	}
	client.mu.Unlock()

	_, st = readAtFh(fh1, 0, 64*1024)
	require.Equal(t, fuse.OK, st)
	_, st = readAtFh(fh1, 64*1024, 64*1024)
	require.Equal(t, fuse.OK, st)

	select {
	case <-prefetchStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("等待 RawFS.Read 触发异步预取超时")
	}

	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: nodeID}, Fh: fh1})
	select {
	case cancelErr := <-prefetchCanceled:
		require.ErrorIs(t, cancelErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("RawFS.Release 未及时取消句柄的在途预取")
	}

	// 4. 验证 OnUnmount -> ReadAheadCoordinator.Clear 取消在途预取
	const fh3 = uint64(13)
	fs.PrepareTestFileHandle(t, nodeID, fh3, syscall.O_RDONLY)

	prefetchStarted3 := make(chan struct{})
	prefetchCanceled3 := make(chan error, 1)
	var once3 sync.Once
	client.mu.Lock()
	client.onRead4MiB = func(ctx context.Context, offset uint64) {
		if offset == chunkSize {
			once3.Do(func() {
				close(prefetchStarted3)
				<-ctx.Done()
				prefetchCanceled3 <- ctx.Err()
			})
		}
	}
	client.mu.Unlock()

	_, st = readAtFh(fh3, 0, 64*1024)
	require.Equal(t, fuse.OK, st)
	_, st = readAtFh(fh3, 64*1024, 64*1024)
	require.Equal(t, fuse.OK, st)

	select {
	case <-prefetchStarted3:
	case <-time.After(2 * time.Second):
		t.Fatal("等待 fh3 触发异步预取超时")
	}

	fs.OnUnmount()
	select {
	case cancelErr := <-prefetchCanceled3:
		require.ErrorIs(t, cancelErr, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("RawFS.OnUnmount 未及时通过 Clear 取消在途预取")
	}
}
