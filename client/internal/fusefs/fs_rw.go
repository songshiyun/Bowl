package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Read 实现常规文件数据读取，对应 Linux FUSE FUSE_READ 协议。
// 校验本地句柄与访问模式，并在服务端请求中省略旧版句柄：
//  1. 映射内核 Inode 为服务端 Inode，并校验本地文件句柄；
//  2. 目标读取长度以 input.Size 与传入非空 buf 长度二者较小值为准；长度为 0 时立即返回空数据；
//  3. 在同一文件句柄上维护顺序读检测与 4 MiB 对齐预读缓冲：顺序读与内核并发预读流量统一按 4 MiB 对齐窗口
//     向服务端单飞拉取，并在确认连续顺序流后并发维持 W_read=4 个 4 MiB 预取窗口；
//     非连续随机读保持按请求尺寸直通读取，不产生额外放大，且不清空落在窗口内的在途拉取；
//     窗口未到文件末尾且请求跨窗口边界时，用下一 4 MiB 对齐窗口补齐；窗口已到 EOF 时直接返回剩余字节；
//  4. 当 want > MaxRPCChunkSize 时按 4 MiB 顺序循环分片读取；
//  5. 遇到 EOF 或短读立即停止分片循环并返回已读数据；中途分片报错但已有数据读取成功时按短读返回 fuse.OK，首片失败返回对应 errno。
func (fs *RawFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (res fuse.ReadResult, status fuse.Status) {
	opCtx, endSpan := fs.startFuseOp(OpRead)
	defer func() { endSpan(status) }()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if fs.isVirtualStatsIno(input.NodeId) {
		return fs.readVirtualStats(input, buf)
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return nil, status
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
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

	dest := buf
	if len(dest) < want {
		dest = make([]byte, want)
	} else {
		dest = dest[:want]
	}

	if n, local, st := fs.tmpMgr.Read(input.NodeId, input.Offset, dest); local {
		return fuse.ReadResultData(dest[:n]), st
	}

	fs.writeBehind.BarrierInode(opCtx, ino)

	baseCtx, cancelTimeout := context.WithTimeout(opCtx, DefaultRPCTimeout)
	ctx, cancelChan := rpc.ContextWithCancelChan(baseCtx, cancel)
	defer func() {
		cancelChan()
		cancelTimeout()
	}()

	n, err := fs.readAhead.Read(ctx, input.Fh, ino, input.Offset, dest, func(fetchCtx context.Context, offset uint64, length uint32) (rpc.ReadPayload, error) {
		reqCtx := rpc.WithRequestID(fetchCtx, fs.NextRequestID())
		resp, rpcErr := fs.client.Read(reqCtx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:    ino,
			Fh:     0,
			Offset: offset,
			Size:   length,
			DiskId: fs.cfg.DiskID,
		}))
		if rpcErr != nil {
			return rpc.ReadPayload{}, rpcErr
		}
		return rpc.TakeReadPayload(resp.Msg), nil
	})
	if err != nil {
		return nil, fs.ToStatus(err, cancel)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
}

// Write 实现常规文件数据写入，对应 Linux FUSE FUSE_WRITE 协议。
// 常规文件写入直接提交服务端；匿名文件在具名化前保存在本地：
// 1. --ro 只读挂载下直接拦截返回 EROFS；
// 2. 映射 Inode 并校验本地文件句柄，立即同步失效目标 Inode 的预读缓冲；
// 3. 当 len(data) <= MaxRPCChunkSize（包含 0 字节写入探针）时发起单次 Write RPC 直通落盘并校验模式；
// 4. 当 len(data) > MaxRPCChunkSize 时按 4 MiB 顺序分片调用，并累加实际写入字节数；
// 5. 遵循 POSIX 短写语义：若中途分片报错（如 ENOSPC）且已有数据写入成功，返回已写入字节数与 fuse.OK；首片即失败返回对应 errno。
func (fs *RawFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, status fuse.Status) {
	opCtx, endSpan := fs.startFuseOp(OpWrite)
	defer func() { endSpan(status) }()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	epoch := fs.cache.version()
	if status = fs.CheckWritable(); status != fuse.OK {
		return 0, status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return 0, fuse.EACCES
	}

	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return 0, status
	}

	handle, status := fs.checkedHandle(input.NodeId, input.Fh, false)
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
	if n, local, st := fs.tmpMgr.Write(input.NodeId, input.Offset, data, appendMode); local {
		return n, st
	}

	dataLen := len(data)
	syncMode := handle.Flags&(syscall.O_SYNC|syscall.O_DSYNC) != 0
	// 属性未缓存（例如刚经 Ioctl/错误失效）或携带 Immutable/Append/SUID/SGID 时必须走同步直通校验。
	specialInode := true
	if cached, ok := fs.cache.inode(input.NodeId); ok && cached.inode != nil && cached.inode.GetAttr() != nil {
		attr := cached.inode.GetAttr()
		specialInode = attr.GetFlags()&(ProtoFlagImmutable|ProtoFlagAppend) != 0 ||
			attr.GetMode()&(syscall.S_ISUID|syscall.S_ISGID) != 0
	}

	switch {
	case appendMode:
		// 同一 Inode 的 O_APPEND 必须在“排空异步写 -> 读取当前大小 -> 提交 Write”之间保持定序，
		// 否则并发追加会读到相同旧大小而互相覆盖。
		unlock := fs.inodeLocks.lock(input.NodeId)
		defer unlock()
		if flushErr := fs.writeBehind.FlushAndResetInodeForHandle(opCtx, input.Fh, ino); flushErr != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.ToStatus(flushErr, nil)
		}
		cached, st := fs.getInodeWithContext(opCtx, cancel, input.NodeId)
		if st != fuse.OK {
			return 0, st
		}
		copyInput := *input
		copyInput.Offset = cached.inode.GetAttr().GetSize()
		input = &copyInput
	case syncMode || specialInode || !fs.caps.knownAbsent(input.NodeId) || dataLen == 0 || dataLen > MaxRPCChunkSize:
		if flushErr := fs.writeBehind.FlushAndResetInodeForHandle(opCtx, input.Fh, ino); flushErr != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.ToStatus(flushErr, nil)
		}
	default:
		fs.readAhead.InvalidateInode(ino)
		if wbWritten, handled, wbErr := fs.writeBehind.WriteForHandle(opCtx, input.Fh, ino, input.NodeId, input.Offset, data, epoch); handled {
			if wbErr != nil {
				fs.cache.invalidateInode(input.NodeId)
				return 0, fs.ToStatus(wbErr, nil)
			}
			fs.readAhead.InvalidateInode(ino)
			return wbWritten, fuse.OK
		}
	}

	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

	// Write 为非幂等变更（尤其在 O_APPEND 下），一旦在途发出 RPC 则忽略中途 FUSE_INTERRUPT（如 Go SIGURG 抢占信号），
	// 避免服务端已提交追加但客户端因中断返回 EINTR 触发调用方重试造成重复追加。
	// 使用 context.WithoutCancel(opCtx) 保留调用链 SpanContext，同时屏蔽上游取消对非幂等落盘的半途中断。
	detachedCtx := context.WithoutCancel(opCtx)

	if dataLen <= MaxRPCChunkSize {
		ctx, cancelFunc := fs.RPCContextFrom(detachedCtx, nil)
		defer cancelFunc()
		resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    ino,
			Fh:     0,
			Offset: input.Offset,
			Data:   data,
			DiskId: fs.cfg.DiskID,
			Epoch:  fs.cfg.Epoch,
		}))
		if err != nil {
			fs.cache.invalidateInode(input.NodeId)
			return 0, fs.ToStatus(err, nil)
		}
		fs.markDirty(ino)
		fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), DefaultWriteAttrTimeoutSeconds)
		fs.caps.markAbsent(input.NodeId)
		written = resp.Msg.GetSize()
		fs.metrics.addWriteBehindPassthrough(uint64(written))
		if !appendMode && !syncMode && dataLen > 0 {
			fs.writeBehind.ObserveSyncWrite(ino, input.Offset, written)
		}
		return written, fuse.OK
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

		chunkSize := min(uint32(dataLen)-totalWritten, MaxRPCChunkSize)
		chunk := data[totalWritten : totalWritten+chunkSize]
		curOffset := input.Offset + uint64(totalWritten)

		chunkCtx, chunkCancel := fs.RPCContextFrom(detachedCtx, nil)
		resp, err := fs.client.Write(chunkCtx, connect.NewRequest(&storagev1.WriteRequest{
			Ino:    ino,
			Fh:     0,
			Offset: curOffset,
			Data:   chunk,
			DiskId: fs.cfg.DiskID,
			Epoch:  fs.cfg.Epoch,
		}))
		chunkCancel()
		if err != nil {
			fs.cache.invalidateInode(input.NodeId)
			if totalWritten > 0 {
				return totalWritten, fuse.OK
			}
			return 0, fs.ToStatus(err, nil)
		}

		fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), DefaultWriteAttrTimeoutSeconds)
		fs.caps.markAbsent(input.NodeId)
		n := resp.Msg.GetSize()
		fs.markDirty(ino)
		fs.metrics.addWriteBehindPassthrough(uint64(n))
		totalWritten += n
		if n < chunkSize {
			break
		}
	}

	return totalWritten, fuse.OK
}
