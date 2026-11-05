package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// ToProtoOpenFlags 将宿主机操作系统特定的打开标志位转换为平台无关的服务端协议标志位。
// 仅保留访问模式（O_RDONLY / O_WRONLY / O_RDWR）及 O_CREAT、O_EXCL、O_TRUNC、O_APPEND 控制位，
// 彻底剥离 Linux O_LARGEFILE、O_DIRECT、O_NOFOLLOW、O_CLOEXEC 等内核高位，严防碰撞服务端 O_TMPFILE (0o20000000) 禁区。
func ToProtoOpenFlags(sysFlags uint32) uint32 {
	var protoFlags uint32

	switch sysFlags & uint32(syscall.O_ACCMODE) {
	case uint32(syscall.O_RDONLY):
		protoFlags |= 0o0
	case uint32(syscall.O_WRONLY):
		protoFlags |= 0o1
	case uint32(syscall.O_RDWR):
		protoFlags |= 0o2
	}

	if (sysFlags & uint32(syscall.O_CREAT)) != 0 {
		protoFlags |= 0o100
	}
	if (sysFlags & uint32(syscall.O_EXCL)) != 0 {
		protoFlags |= 0o200
	}
	if (sysFlags & uint32(syscall.O_TRUNC)) != 0 {
		protoFlags |= 0o1000
	}
	if (sysFlags & uint32(syscall.O_APPEND)) != 0 {
		protoFlags |= 0o2000
	}

	return protoFlags
}

// Create 创建服务端目录项并分配本地句柄。
func (fs *RawFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if input.Flags&uint32(syscall.O_ACCMODE) == syscall.O_ACCMODE {
		return fuse.EINVAL
	}
	parentIno, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	epoch := fs.cache.version()
	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()
	var sid uint64
	if fs.sessions != nil {
		sid = fs.sessions.CurrentSessionID()
	}
	resp, err := fs.client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: parentIno, Name: []byte(name), Mode: input.Mode & 0xffff,
		Uid: input.Uid, Gid: input.Gid, Flags: ToProtoOpenFlags(input.Flags), SessionId: optionalSession(sid),
	}))
	if err != nil {
		return fs.ToStatusWithSession(err, cancel)
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return fuse.EIO
	}
	ino := fs.ToKernelIno(inode.GetIno())
	fh, status := fs.openHandle(ino, input.Flags, false)
	if status != fuse.OK {
		return status
	}
	fs.cache.invalidateDirListingTouchParentAt(epoch, input.NodeId, inode, 0, resp.Msg.GetAttrTimeoutSeconds())
	fs.cache.putEntryAt(epoch, input.NodeId, name, ino)
	fs.cache.putInodeAt(epoch, ino, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	fs.caps.markAbsent(ino)
	fs.FillEntryOut(&out.EntryOut, inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds())
	fs.addTmpLookup(ino)
	out.Fh = fh
	out.OpenFlags = resp.Msg.GetOpenFlags()
	if fs.cfg.DirectIO {
		out.OpenFlags &^= fuse.FOPEN_KEEP_CACHE
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	} else if input.Flags&uint32(syscall.O_TRUNC) != 0 {
		out.OpenFlags &^= fuse.FOPEN_KEEP_CACHE
	} else {
		// 非截断且非 DirectIO 时保留内核 Page Cache，避免每次 open 全量清空。
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	}
	return fuse.OK
}

// Open 读取缓存中的节点类型并在本地分配句柄。
func (fs *RawFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	mode := input.Flags & uint32(syscall.O_ACCMODE)
	if mode == syscall.O_ACCMODE {
		return fuse.EINVAL
	}
	if fs.cfg.ReadOnly && (mode != syscall.O_RDONLY || input.Flags&syscall.O_TRUNC != 0) {
		return fuse.Status(syscall.EROFS)
	}
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	epoch := fs.cache.version()
	cached, status := fs.getInode(cancel, input.NodeId)
	if status != fuse.OK {
		return status
	}
	if fs.cache.version() != epoch {
		return fuse.Status(syscall.ESTALE)
	}
	if cached.inode.GetAttr().GetMode()&syscall.S_IFMT == syscall.S_IFDIR {
		return fuse.Status(syscall.EISDIR)
	}
	if input.Flags&syscall.O_TRUNC != 0 {
		set := &fuse.SetAttrIn{}
		set.NodeId = input.NodeId
		set.Valid = fuse.FATTR_SIZE
		if status, local := fs.setTmpAttr(set, &fuse.AttrOut{}); local {
			if status != fuse.OK {
				return status
			}
		} else {
			ino, st := fs.ToServerIno(input.NodeId)
			if st != fuse.OK {
				return st
			}
			ctx, done := fs.RPCContext(cancel)
			zero := uint64(0)
			resp, err := fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{Ino: ino, Size: &zero, SessionId: fs.currentSession()}))
			done()
			fs.invalidateReadAheadByIno(ino)
			if err != nil {
				fs.cache.invalidateInode(input.NodeId)
				return fs.ToStatus(err, cancel)
			}
			fs.cache.refreshInodeAt(epoch, input.NodeId, resp.Msg.GetInode(), resp.Msg.GetAttrTimeoutSeconds())
			fs.caps.markAbsent(input.NodeId)
		}
	}
	fh, status := fs.openHandle(input.NodeId, input.Flags, false)
	if status != fuse.OK {
		return status
	}
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags |= fuse.FOPEN_DIRECT_IO
	} else if input.Flags&uint32(syscall.O_TRUNC) == 0 {
		// 非截断且非 DirectIO 时保留内核 Page Cache，避免每次 open 全量清空。
		out.OpenFlags |= fuse.FOPEN_KEEP_CACHE
	}
	return fuse.OK
}

// Flush 在 close(fd) 时排空内核页缓存写入并释放本地 POSIX 记录锁。
// 常规文件不再发起跨机物理 Fsync RPC；显式 Fsync、FsyncDir 与 OnUnmount 才确认物理落盘。
func (fs *RawFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if _, _, status := fs.checkedHandle(input.NodeId, input.Fh, false); status != fuse.OK {
		return status
	}
	fs.locks.ReleasePOSIXByOwner(input.NodeId, input.LockOwner)
	// 匿名文件尚未具名，关闭时必须先上云并解链；该路径只写服务端 WAL，不触发物理 fsync。
	return fs.syncTmpFile(cancel, input.NodeId)
}

// Fsync 等待服务端完成文件持久化。
func (fs *RawFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	_, sid, status := fs.checkedHandle(input.NodeId, input.Fh, false)
	if status != fuse.OK {
		return status
	}
	if status := fs.syncTmpFile(cancel, input.NodeId); status != fuse.OK {
		return status
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()
	_, err := fs.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		Ino: ino, Fh: 0, Datasync: input.FsyncFlags&1 != 0, SessionId: optionalSession(sid),
	}))
	if err == nil {
		fs.clearDirty(ino)
	}
	return fs.ToStatusWithSession(err, cancel)
}

// Release 在本地释放句柄与 flock；最后一个孤儿句柄触发回收。
func (fs *RawFS) Release(_ <-chan struct{}, input *fuse.ReleaseIn) {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	h, ok := fs.handles.Get(input.Fh)
	if !ok || h.Ino != input.NodeId || h.IsDir {
		return
	}
	// 与在途 Unlink/Rename 的孤儿标记按 Inode 串行，避免最后句柄关闭与解链互相错过。
	unlock := fs.inodeLocks.lock(h.Ino)
	defer unlock()
	if _, ok := fs.handles.Get(input.Fh); !ok {
		return
	}
	fs.locks.ReleaseFlockByFH(input.NodeId, input.Fh, input.LockOwner)
	fs.locks.ForgetHandle(input.Fh)
	fs.removeReadAheadByFh(input.Fh)
	orphan := fs.handles.IsOrphan(input.NodeId)
	fs.handles.Release(input.Fh)
	if fs.sessions != nil {
		fs.sessions.UnbindHandle(input.Fh)
	}
	fs.dropTmpIfUnused(input.NodeId, orphan)
}
