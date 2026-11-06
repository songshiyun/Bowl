package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

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

	dest := buf
	if len(dest) < want {
		dest = make([]byte, want)
	} else {
		dest = dest[:want]
	}

	if n, local, st := fs.tmpMgr.Read(input.NodeId, input.Offset, dest); local {
		return fuse.ReadResultData(dest[:n]), st
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	n, err := fs.readAhead.Read(ctx, input.Fh, ino, sid, input.Offset, dest, func(fetchCtx context.Context, offset uint64, length uint32) ([]byte, error) {
		resp, rpcErr := fs.client.Read(fetchCtx, connect.NewRequest(&storagev1.ReadRequest{
			Ino:       ino,
			Fh:        0,
			Offset:    offset,
			Size:      length,
			SessionId: optionalSession(sid),
		}))
		if rpcErr != nil {
			if isServerEBADF(rpcErr) && fs.sessions != nil {
				fs.sessions.TriggerRenew()
			}
			return nil, rpcErr
		}
		return resp.Msg.GetData(), nil
	})
	if err != nil {
		return nil, fs.ToStatusWithSession(err, cancel)
	}
	return fuse.ReadResultData(dest[:n]), fuse.OK
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
	if n, local, st := fs.tmpMgr.Write(input.NodeId, input.Offset, data, appendMode); local {
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
	fs.readAhead.InvalidateInode(ino)
	defer fs.readAhead.InvalidateInode(ino)

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
