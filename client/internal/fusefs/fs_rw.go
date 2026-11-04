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

type handleReadAhead struct {
	mu          sync.Mutex
	ino         uint64
	sessionID   uint64
	gen         uint64
	lastReadEnd uint64
	hasLastRead bool
	seqCount    int
	winOffset   uint64
	winData     []byte
	winValid    bool
	winEOF      bool
	prefetch    *prefetchFuture
}

func (hra *handleReadAhead) resetLocked() {
	hra.gen++
	hra.lastReadEnd = 0
	hra.hasLastRead = false
	hra.seqCount = 0
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
//  3. 在同一文件句柄上维护顺序读检测与 4 MiB 对齐预读缓冲：当检测到连续顺序读取时，按 4 MiB 窗口向服务端拉取并异步预取下一 4 MiB 窗口，
//     使后续落入窗口内的读取直接命中客户端内存；非连续随机读保持按请求尺寸直通读取，不产生额外放大；
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
				hra.mu.Unlock()
				return makeReadResult(prefix, buf), fuse.OK
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

		// 3. 连续顺序读检测：当前读取紧接上一次读取末尾且 want < MaxRPCChunkSize 时，按 4 MiB 窗口拉取并启动异步预读
		if hra.hasLastRead && input.Offset == hra.lastReadEnd && want < MaxRPCChunkSize {
			startGen := hra.gen
			winData, err := fs.fetchReadWindow(ctx, hra, ino, input.Fh, sid, input.Offset)
			if err != nil {
				hra.mu.Unlock()
				return nil, fs.ToStatusWithSession(err, cancel)
			}

			n := min(want, len(winData))
			if hra.gen == startGen {
				hra.winOffset = input.Offset
				hra.winData = winData
				hra.winValid = len(winData) > 0
				hra.winEOF = len(winData) < MaxRPCChunkSize
				hra.lastReadEnd = input.Offset + uint64(n)
				hra.hasLastRead = true
				hra.seqCount++
				if !hra.winEOF {
					fs.startPrefetchLocked(hra, input.Fh, input.Offset+MaxRPCChunkSize)
				}
			}
			hra.mu.Unlock()
			return makeReadResult(winData[:n], buf), fuse.OK
		}

		// 4. 首次读取或非连续随机读取：清空旧窗口并按请求尺寸直通读取，不产生多余读放大
		hra.clearWindowAndPrefetchLocked()
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
		fs.cache.invalidateInode(input.NodeId)
		fs.cache.putInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), 0, DefaultWriteAttrTimeoutSeconds)
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

		fs.cache.invalidateInode(input.NodeId)
		fs.cache.putInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), 0, DefaultWriteAttrTimeoutSeconds)
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
