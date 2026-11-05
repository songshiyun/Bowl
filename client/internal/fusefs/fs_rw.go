package fusefs

import (
	"context"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// MaxRPCChunkSize 定义客户端单次读写 RPC 的最大载荷切片大小 (4 MiB = 4,194,304 字节)。
const MaxRPCChunkSize = 4 * 1024 * 1024

const defaultPrefetchTimeout = 30 * time.Second

type prefetchFuture struct {
	offset uint64
	done   chan struct{}
	data   []byte
	err    error
	cancel context.CancelFunc
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
	sessionID   uint64
	gen         uint64
	lastReadEnd uint64
	hasLastRead bool
	seqCount    int
	// streamEnd 是该句柄已观测到的顺序读高水位（单调递增）。
	// 内核 readahead 线程与用户线程会并发下发读请求，到达顺序与偏移顺序无关，
	// 单靠 lastReadEnd 无法判定“是否同一股顺序流”，必须用单调高水位。
	streamEnd uint64
	// reads 是该句柄已收到的读请求数；首个请求一律直通，避免一次性大读被放大。
	reads     int
	winOffset uint64
	winData   []byte
	winValid  bool
	winEOF    bool
	prefetch  *prefetchFuture
	// fetch 是当前在途的 4 MiB 对齐窗口拉取，用于把并发读合并成一次 RPC。
	fetch *windowFetch
}

// readAheadMinWant 是触发对齐窗口拉取的最小请求尺寸。
// 内核 FUSE 的 readahead 请求按 128 KiB 起步（MaxReadAhead=1 MiB），
// 随机小读通常为 4~64 KiB；用尺寸阈值把“预读流量”和“随机小读”分开，
// 保持既有契约：非连续随机读严格按请求尺寸直通，绝不放大成 4 MiB。
const readAheadMinWant = 128 * 1024

func (hra *handleReadAhead) resetLocked() {
	hra.gen++
	hra.lastReadEnd = 0
	hra.hasLastRead = false
	hra.seqCount = 0
	hra.streamEnd = 0
	hra.fetch = nil
	hra.winOffset = 0
	hra.winData = nil
	hra.winValid = false
	hra.winEOF = false
	if hra.prefetch != nil {
		if hra.prefetch.cancel != nil {
			hra.prefetch.cancel()
		}
		hra.prefetch = nil
	}
}

func (hra *handleReadAhead) clearWindowAndPrefetchLocked() {
	hra.gen++
	hra.fetch = nil
	hra.winOffset = 0
	hra.winData = nil
	hra.winValid = false
	hra.winEOF = false
	if hra.prefetch != nil {
		if hra.prefetch.cancel != nil {
			hra.prefetch.cancel()
		}
		hra.prefetch = nil
	}
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

func (fs *RawFS) getOrCreateHandleReadAhead(fh, ino, sessionID uint64) *handleReadAhead {
	fs.readAheadMu.Lock()
	defer fs.readAheadMu.Unlock()
	if fs.readAheadHandles == nil {
		fs.readAheadHandles = make(map[uint64]*handleReadAhead)
	}
	hra, ok := fs.readAheadHandles[fh]
	if !ok {
		hra = &handleReadAhead{
			ino:       ino,
			sessionID: sessionID,
		}
		fs.readAheadHandles[fh] = hra
		return hra
	}
	hra.mu.Lock()
	if hra.ino != ino || hra.sessionID != sessionID {
		hra.resetLocked()
		hra.ino = ino
		hra.sessionID = sessionID
	}
	hra.mu.Unlock()
	return hra
}

func (fs *RawFS) invalidateReadAheadByIno(ino uint64) {
	fs.readAheadMu.Lock()
	var targets []*handleReadAhead
	for _, hra := range fs.readAheadHandles {
		targets = append(targets, hra)
	}
	fs.readAheadMu.Unlock()

	for _, hra := range targets {
		hra.mu.Lock()
		if hra.ino == ino {
			hra.resetLocked()
		}
		hra.mu.Unlock()
	}
}

func (fs *RawFS) removeReadAheadByFh(fh uint64) {
	fs.readAheadMu.Lock()
	hra, ok := fs.readAheadHandles[fh]
	if ok {
		delete(fs.readAheadHandles, fh)
	}
	fs.readAheadMu.Unlock()

	if ok && hra != nil {
		hra.mu.Lock()
		hra.resetLocked()
		hra.mu.Unlock()
	}
}

func (fs *RawFS) clearAllReadAhead() {
	fs.readAheadMu.Lock()
	handles := fs.readAheadHandles
	fs.readAheadHandles = make(map[uint64]*handleReadAhead)
	fs.readAheadMu.Unlock()

	for _, hra := range handles {
		hra.mu.Lock()
		hra.resetLocked()
		hra.mu.Unlock()
	}
}

// fetchAlignedWindow 单飞拉取覆盖 offset 的 4 MiB 对齐窗口，调用期间会临时释放 hra.mu，
// 返回时重新持有。同窗口内并发的读请求共享同一次 RPC，避免内核并发预读退化成 N 次小 RPC。
func (fs *RawFS) fetchAlignedWindow(ctx context.Context, hra *handleReadAhead, ino, sid, offset uint64) ([]byte, error) {
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
	resp, err := fs.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		Ino:       ino,
		Fh:        0,
		Offset:    aligned,
		Size:      MaxRPCChunkSize,
		SessionId: optionalSession(sid),
	}))
	if err != nil {
		if isServerEBADF(err) && fs.sessions != nil {
			fs.sessions.TriggerRenew()
		}
		f.err = err
	} else {
		f.data = resp.Msg.GetData()
	}
	close(f.done)
	hra.mu.Lock()
	if hra.fetch == f {
		hra.fetch = nil
	}
	return f.data, f.err
}

func (fs *RawFS) startPrefetchLocked(hra *handleReadAhead, fh uint64, nextOffset uint64) {
	if fs.client == nil {
		return
	}
	if hra.prefetch != nil {
		if hra.prefetch.offset == nextOffset {
			return
		}
		if hra.prefetch.cancel != nil {
			hra.prefetch.cancel()
		}
		hra.prefetch = nil
	}

	ino := hra.ino
	sid := hra.sessionID
	pfCtx, pfCancel := context.WithTimeout(context.Background(), defaultPrefetchTimeout)
	pf := &prefetchFuture{
		offset: nextOffset,
		done:   make(chan struct{}),
		cancel: pfCancel,
	}
	hra.prefetch = pf

	go func() {
		defer close(pf.done)
		defer pfCancel()
		resp, err := fs.client.Read(pfCtx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       ino,
			Fh:        0,
			Offset:    nextOffset,
			Size:      MaxRPCChunkSize,
			SessionId: optionalSession(sid),
		}))
		if err != nil {
			if isServerEBADF(err) && fs.sessions != nil {
				fs.sessions.TriggerRenew()
			}
			pf.err = err
			return
		}
		pf.data = resp.Msg.GetData()
	}()
}

func makeReadResult(data []byte, buf []byte) fuse.ReadResult {
	if len(buf) >= len(data) {
		copy(buf, data)
		return fuse.ReadResultData(buf[:len(data)])
	}
	return fuse.ReadResultData(data)
}

func (fs *RawFS) fetchReadWindow(ctx context.Context, hra *handleReadAhead, ino, fh, sid, offset uint64) ([]byte, error) {
	var pf *prefetchFuture
	if hra.prefetch != nil && hra.prefetch.offset == offset {
		pf = hra.prefetch
		hra.prefetch = nil
	} else if hra.prefetch != nil {
		if hra.prefetch.cancel != nil {
			hra.prefetch.cancel()
		}
		hra.prefetch = nil
	}

	startGen := hra.gen
	if pf != nil {
		hra.mu.Unlock()
		select {
		case <-pf.done:
		case <-ctx.Done():
			hra.mu.Lock()
			return nil, ctx.Err()
		}
		hra.mu.Lock()
		if pf.err == nil && hra.gen == startGen {
			return pf.data, nil
		}
	}

	hra.mu.Unlock()
	resp, err := fs.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		Ino:       ino,
		Fh:        0,
		Offset:    offset,
		Size:      MaxRPCChunkSize,
		SessionId: optionalSession(sid),
	}))
	hra.mu.Lock()
	if err != nil {
		if isServerEBADF(err) && fs.sessions != nil {
			fs.sessions.TriggerRenew()
		}
		return nil, err
	}
	return resp.Msg.GetData(), nil
}

// Read 实现常规文件数据读取，对应 Linux FUSE FUSE_READ 协议。
// 校验本地句柄与访问模式，并在服务端请求中省略旧版句柄：
//  1. 映射内核 Inode 为服务端 Inode，并校验句柄所属活跃会话；
//  2. 目标读取长度以 input.Size 与传入非空 buf 长度二者较小值为准；长度为 0 时立即返回空数据；
//  3. 在同一文件句柄上维护顺序读检测与 4 MiB 对齐预读缓冲：顺序读与内核并发预读流量统一按 4 MiB 对齐窗口
//     向服务端单飞拉取，并在窗口发布后异步预取下一窗口，使后续落入窗口内的读取直接命中客户端内存；
//     非连续随机读保持按请求尺寸直通读取，不产生额外放大，且不清空落在窗口内的在途拉取；
//     窗口未到文件末尾且请求跨窗口边界时，用下一 4 MiB 对齐窗口补齐；窗口已到 EOF 时直接返回剩余字节；
//  4. 当 want > MaxRPCChunkSize 时按 4 MiB 顺序循环分片读取；
//  5. 遇到 EOF 或短读立即停止分片循环并返回已读数据；中途分片报错但已有数据读取成功时按短读返回 fuse.OK，首片失败返回对应 errno。
func (fs *RawFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return nil, status
	}

	handle, sid, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return nil, status
	}
	if !handle.CanRead() {
		return nil, fuse.EBADF
	}

	want := int(input.Size)
	if len(buf) > 0 && len(buf) < want {
		want = len(buf)
	}
	if want == 0 {
		return fuse.ReadResultData(nil), fuse.OK
	}
	if result, local := fs.readTmp(input.NodeId, input.Offset, want, buf); local {
		return result, fuse.OK
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	hra := fs.getOrCreateHandleReadAhead(input.Fh, ino, sid)

	if want <= MaxRPCChunkSize {
		hra.mu.Lock()
		hra.reads++

		// 1. 若当前请求起始偏移位于异步预取窗口内，等待预取完成并提升为活跃窗口
		if (!hra.winValid || input.Offset < hra.winOffset || input.Offset >= hra.winOffset+uint64(len(hra.winData))) &&
			hra.prefetch != nil &&
			input.Offset >= hra.prefetch.offset &&
			input.Offset < hra.prefetch.offset+MaxRPCChunkSize {
			pf := hra.prefetch
			startGen := hra.gen
			hra.mu.Unlock()
			select {
			case <-pf.done:
			case <-ctx.Done():
				return nil, fs.ToStatus(ctx.Err(), cancel)
			}
			hra.mu.Lock()
			if hra.gen == startGen && pf.err == nil {
				if hra.prefetch == pf {
					hra.prefetch = nil
				}
				hra.winOffset = pf.offset
				hra.winData = pf.data
				hra.winValid = len(pf.data) > 0
				hra.winEOF = len(pf.data) < MaxRPCChunkSize
				if !hra.winEOF {
					fs.startPrefetchLocked(hra, input.Fh, hra.winOffset+MaxRPCChunkSize)
				}
			} else if hra.prefetch == pf {
				hra.prefetch = nil
			}
		}

		// 2. 命中活跃预读窗口
		if hra.winValid && input.Offset >= hra.winOffset && input.Offset < hra.winOffset+uint64(len(hra.winData)) {
			relOff := int(input.Offset - hra.winOffset)
			avail := len(hra.winData) - relOff
			if avail >= want || hra.winEOF {
				n := min(want, avail)
				chunk := hra.winData[relOff : relOff+n]
				hra.lastReadEnd = input.Offset + uint64(n)
				hra.hasLastRead = true
				hra.seqCount++
				hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
				hra.mu.Unlock()
				return makeReadResult(chunk, buf), fuse.OK
			}

			// 跨窗口边界读取：前缀取自当前窗口尾部，后缀从下一 4 MiB 窗口补齐
			prefix := make([]byte, avail)
			copy(prefix, hra.winData[relOff:])
			nextWinOffset := hra.winOffset + uint64(len(hra.winData))
			startGen := hra.gen

			nextData, err := fs.fetchReadWindow(ctx, hra, ino, input.Fh, sid, nextWinOffset)
			if err != nil {
				// 绝不能把“前缀 + 拉取失败”当作短读返回：内核会把短读视为 EOF，
				// 把 i_size 截断到前缀末尾，随后所有越过该边界的读取都直接返回 0。
				// 预读请求的上下文可能被内核主动取消，返回错误让内核重试即可。
				hra.mu.Unlock()
				return nil, fs.ToStatusWithSession(err, cancel)
			}

			if hra.gen == startGen {
				hra.winOffset = nextWinOffset
				hra.winData = nextData
				hra.winValid = len(nextData) > 0
				hra.winEOF = len(nextData) < MaxRPCChunkSize
				if !hra.winEOF {
					fs.startPrefetchLocked(hra, input.Fh, nextWinOffset+MaxRPCChunkSize)
				}
			}

			rem := min(want-avail, len(nextData))
			combined := make([]byte, avail+rem)
			copy(combined, prefix)
			copy(combined[avail:], nextData[:rem])
			hra.lastReadEnd = input.Offset + uint64(len(combined))
			hra.hasLastRead = true
			hra.seqCount++
			hra.mu.Unlock()
			return makeReadResult(combined, buf), fuse.OK
		}

		// 3. 顺序读与内核预读流量统一走 4 MiB 对齐窗口单飞拉取。
		//    - 顺序读：当前请求紧接上一次读取末尾（用户态顺序读、dd/readahead 推进）；
		//    - 预读流量：内核在 MaxBackground=64 下会并发下发同一 fh 的多个读请求，
		//      到达顺序与偏移顺序无关，lastReadEnd 尚未推进时“连续读”判定必然失败，
		//      因此用单调高水位 streamEnd 判定“是否仍在同一股流附近”。
		//    两者共用 fetchAlignedWindow：同一 4 MiB 对齐窗口内并发请求共享一次 RPC，
		//    窗口发布后由 startPrefetchLocked 预取下一窗口。
		//    随机小读（< readAheadMinWant 且非顺序）仍走下面的直通分支，不引入读放大。
		sequential := hra.hasLastRead && input.Offset == hra.lastReadEnd
		readahead := want >= readAheadMinWant && hra.reads > 1 && !hra.winEOF &&
			input.Offset+uint64(want) > hra.streamEnd &&
			input.Offset < hra.streamEnd+MaxRPCChunkSize
		if sequential || readahead {
			startGen := hra.gen
			data, err := fs.fetchAlignedWindow(ctx, hra, ino, sid, input.Offset)
			if err != nil {
				hra.mu.Unlock()
				return nil, fs.ToStatusWithSession(err, cancel)
			}
			winOffset := input.Offset &^ uint64(MaxRPCChunkSize-1)
			rel := input.Offset - winOffset
			// 窗口不足一个完整 RPC 尺寸即说明已到真实文件末尾。
			winEOF := len(data) < MaxRPCChunkSize
			// 只有对齐窗口完整覆盖本次请求、或窗口已到文件末尾时才直接切片返回。
			// 请求跨越 4 MiB 窗口边界且窗口未到 EOF 时窗口尾部不足 want，若在这里返回短读，
			// 内核会把短读当成 EOF 并把 i_size 截断到窗口末尾，后续越过该边界的
			// 读取将直接返回 0。此时要么用下一窗口补齐，要么落到直通分支按请求尺寸重新读取。
			covers := rel <= uint64(len(data)) && (len(data)-int(rel) >= want || winEOF)
			if hra.gen != startGen {
				// 拉取期间该句柄窗口已被并发写或随机读失效：拉到的数据可能早于失效点，
				// 绝不能发布为活跃缓存或触发预读，只用它为本次请求切出所需区间返回。
				if covers {
					avail := len(data) - int(rel)
					n := min(want, avail)
					chunk := data[rel : rel+uint64(n)]
					hra.mu.Unlock()
					return makeReadResult(chunk, buf), fuse.OK
				}
			} else {
				hra.winOffset = winOffset
				hra.winData = data
				hra.winValid = len(data) > 0
				hra.winEOF = winEOF
				if covers {
					avail := len(data) - int(rel)
					n := min(want, avail)
					chunk := data[rel : rel+uint64(n)]
					hra.lastReadEnd = input.Offset + uint64(n)
					hra.hasLastRead = true
					hra.seqCount++
					hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
					if !hra.winEOF {
						fs.startPrefetchLocked(hra, input.Fh, winOffset+MaxRPCChunkSize)
					}
					hra.mu.Unlock()
					return makeReadResult(chunk, buf), fuse.OK
				}
				// 请求跨 4 MiB 对齐窗口边界且当前窗口未到 EOF：前缀取自当前窗口尾部，
				// 后缀从下一 4 MiB 对齐窗口补齐，避免落入直通分支再发一次小 RPC。
				if avail := len(data) - int(rel); avail > 0 {
					prefix := make([]byte, avail)
					copy(prefix, data[rel:])
					nextWinOffset := winOffset + uint64(len(data))
					nextData, err := fs.fetchReadWindow(ctx, hra, ino, input.Fh, sid, nextWinOffset)
					if err != nil {
						// 与第 2 分支一致：绝不能把“前缀 + 拉取失败”当作短读返回。
						hra.mu.Unlock()
						return nil, fs.ToStatusWithSession(err, cancel)
					}
					if hra.gen == startGen {
						hra.winOffset = nextWinOffset
						hra.winData = nextData
						hra.winValid = len(nextData) > 0
						hra.winEOF = len(nextData) < MaxRPCChunkSize
						if !hra.winEOF {
							fs.startPrefetchLocked(hra, input.Fh, nextWinOffset+MaxRPCChunkSize)
						}
					}
					rem := min(want-avail, len(nextData))
					combined := make([]byte, avail+rem)
					copy(combined, prefix)
					copy(combined[avail:], nextData[:rem])
					hra.lastReadEnd = input.Offset + uint64(len(combined))
					hra.hasLastRead = true
					hra.seqCount++
					hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
					hra.mu.Unlock()
					return makeReadResult(combined, buf), fuse.OK
				}
			}
		}

		// 4. 首次读取或非连续随机读取：按请求尺寸直通，不产生多余读放大。
		//    只有请求完全不落在“活跃窗口 / 在途窗口拉取 / 异步预取窗口”内时才清窗，
		//    避免内核乱序到达的小请求把在途 4 MiB 拉取判废。
		if !hra.requestInWindowLocked(input.Offset, want) {
			hra.clearWindowAndPrefetchLocked()
		}
		startGen := hra.gen
		hra.mu.Unlock()

		resp, err := fs.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       ino,
			Fh:        0,
			Offset:    input.Offset,
			Size:      uint32(want),
			SessionId: optionalSession(sid),
		}))
		if err != nil {
			return nil, fs.ToStatusWithSession(err, cancel)
		}

		data := resp.Msg.GetData()
		hra.mu.Lock()
		if hra.gen == startGen {
			hra.lastReadEnd = input.Offset + uint64(len(data))
			hra.hasLastRead = true
			hra.seqCount = 1
			// 首次读也可能是“从文件中部开始的顺序流”（tail / SQLite / dd skip），
			// 必须推进高水位，否则后续请求永远无法进入预读分支。
			hra.streamEnd = max(hra.streamEnd, hra.lastReadEnd)
		}
		hra.mu.Unlock()

		return makeReadResult(data, buf), fuse.OK
	}

	hra.mu.Lock()
	hra.clearWindowAndPrefetchLocked()
	startGen := hra.gen
	hra.mu.Unlock()

	var out []byte
	if len(buf) >= want {
		out = buf[:0]
	} else {
		out = make([]byte, 0, want)
	}

	var totalRead int
	for totalRead < want {
		chunkReqSize := want - totalRead
		if chunkReqSize > MaxRPCChunkSize {
			chunkReqSize = MaxRPCChunkSize
		}
		curOffset := input.Offset + uint64(totalRead)

		resp, err := fs.client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       ino,
			Fh:        0,
			Offset:    curOffset,
			Size:      uint32(chunkReqSize),
			SessionId: optionalSession(sid),
		}))
		if err != nil {
			if isServerEBADF(err) && fs.sessions != nil {
				fs.sessions.TriggerRenew()
			}
			if totalRead > 0 {
				break
			}
			return nil, fs.ToStatusWithSession(err, cancel)
		}

		chunkData := resp.Msg.GetData()
		if len(chunkData) == 0 {
			break
		}
		out = append(out, chunkData...)
		totalRead += len(chunkData)

		if len(chunkData) < chunkReqSize {
			break
		}
	}

	hra.mu.Lock()
	if hra.gen == startGen {
		hra.lastReadEnd = input.Offset + uint64(totalRead)
		hra.hasLastRead = true
		hra.seqCount = 1
	}
	hra.mu.Unlock()

	return fuse.ReadResultData(out[:totalRead]), fuse.OK
}

// Write 实现常规文件数据写入，对应 Linux FUSE FUSE_WRITE 协议。
// 常规文件写入直接提交服务端；匿名文件在具名化前保存在本地：
// 1. --ro 只读挂载下直接拦截返回 EROFS；
// 2. 映射 Inode 并校验句柄所属会话 session_id，立即同步失效目标 Inode 的预读缓冲；
// 3. 当 len(data) <= MaxRPCChunkSize（包含 0 字节写入探针）时发起单次 Write RPC 直通落盘并校验模式；
// 4. 当 len(data) > MaxRPCChunkSize 时按 4 MiB 顺序分片调用，并累加实际写入字节数；
// 5. 遵循 POSIX 短写语义：若中途分片报错（如 ENOSPC）且已有数据写入成功，返回已写入字节数与 fuse.OK；首片即失败返回对应 errno。
func (fs *RawFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	epoch := fs.cache.version()
	if status := fs.CheckWritable(); status != fuse.OK {
		return 0, status
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	handle, sid, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return 0, status
	}
	if !handle.CanWrite() {
		return 0, fuse.EBADF
	}

	if cancel != nil {
		select {
		case <-cancel:
			return 0, fuse.EINTR
		default:
		}
	}

	appendMode := handle.Flags&syscall.O_APPEND != 0
	if n, st, local := fs.writeTmp(input.NodeId, input.Offset, data, appendMode); local {
		return n, st
	}
	if appendMode {
		// 同一 Inode 的 O_APPEND 必须在“读取当前大小 -> 提交 Write”之间保持定序，
		// 否则并发追加会读到相同旧大小而互相覆盖。
		unlock := fs.inodeLocks.lock(input.NodeId)
		defer unlock()
		cached, st := fs.getInode(cancel, input.NodeId)
		if st != fuse.OK {
			return 0, st
		}
		copyInput := *input
		copyInput.Offset = cached.inode.GetAttr().GetSize()
		input = &copyInput
	}
	fs.invalidateReadAheadByIno(ino)
	defer fs.invalidateReadAheadByIno(ino)

	// Write 为非幂等变更（尤其在 O_APPEND 下），一旦在途发出 RPC 则忽略中途 FUSE_INTERRUPT（如 Go SIGURG 抢占信号），
	// 避免服务端已提交追加但客户端因中断返回 EINTR 触发调用方重试造成重复追加。
	ctx, cancelFunc := fs.RPCContext(nil)
	defer cancelFunc()

	dataLen := len(data)
	if dataLen <= MaxRPCChunkSize {
		resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:       ino,
			Fh:        0,
			Offset:    input.Offset,
			Data:      data,
			SessionId: optionalSession(sid),
		}))
		if err != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.ToStatusWithSession(err, nil)
		}
		fs.markDirty(ino)
		fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), DefaultWriteAttrTimeoutSeconds)
		fs.caps.markAbsent(input.NodeId)
		return resp.Msg.GetSize(), fuse.OK
	}

	var totalWritten uint32
	for totalWritten < uint32(dataLen) {
		if cancel != nil {
			select {
			case <-cancel:
				if totalWritten > 0 {
					return totalWritten, fuse.OK
				}
				return 0, fuse.EINTR
			default:
			}
		}

		chunkSize := uint32(dataLen) - totalWritten
		if chunkSize > MaxRPCChunkSize {
			chunkSize = MaxRPCChunkSize
		}
		chunk := data[totalWritten : totalWritten+chunkSize]
		curOffset := input.Offset + uint64(totalWritten)

		resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:       ino,
			Fh:        0,
			Offset:    curOffset,
			Data:      chunk,
			SessionId: optionalSession(sid),
		}))
		if err != nil {
			fs.cache.invalidateInode(input.NodeId)
			if isServerEBADF(err) && fs.sessions != nil {
				fs.sessions.TriggerRenew()
			}
			if totalWritten > 0 {
				return totalWritten, fuse.OK
			}
			return 0, fs.ToStatusWithSession(err, nil)
		}

		fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), DefaultWriteAttrTimeoutSeconds)
		fs.caps.markAbsent(input.NodeId)
		n := resp.Msg.GetSize()
		fs.markDirty(ino)
		totalWritten += n
		if n < chunkSize {
			break
		}
	}

	return totalWritten, fuse.OK
}
