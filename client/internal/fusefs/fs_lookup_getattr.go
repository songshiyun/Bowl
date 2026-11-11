package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func (fs *RawFS) lookupInodeWithContext(parentCtx context.Context, cancel <-chan struct{}, parent uint64, name string) (cachedInode, fuse.Status) {
	parentIno, status := fs.ToServerIno(parent)
	if status != fuse.OK {
		return cachedInode{}, status
	}
	if len(name) > MaxFilenameBytes {
		return cachedInode{}, fuse.Status(syscall.ENAMETOOLONG)
	}
	if ino, ok := fs.cache.entry(parent, name); ok {
		if ino == 0 {
			return cachedInode{}, fuse.ENOENT
		}
		if serverIno, st := fs.ToServerIno(ino); st == fuse.OK && fs.writeBehind.HasPending(serverIno) {
			fs.writeBehind.BarrierInode(parentCtx, serverIno)
		}
		if cached, ok := fs.cache.inode(ino); ok {
			return cached, fuse.OK
		}
	}
	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContextFrom(parentCtx, cancel)
	defer cancelFunc()
	resp, err := fs.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		DiskId:    fs.cfg.DiskID,
	}))
	if fs.cache.version() != epoch {
		return cachedInode{}, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		status := fs.ToStatus(err, cancel)
		if status == fuse.ENOENT {
			fs.metrics.incNSCacheMiss(nsCacheNegativeEntry)
			fs.cache.putEntryAtIf(generation, epoch, parent, name, 0)
		}
		return cachedInode{}, status
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return cachedInode{}, fuse.EIO
	}
	ino := fs.ToKernelIno(inode.GetIno())
	if fs.writeBehind.HasPending(inode.GetIno()) {
		fs.writeBehind.BarrierInode(parentCtx, inode.GetIno())
		if fresh, ok := fs.cache.inode(ino); ok {
			fs.cache.putEntryAtIf(generation, epoch, parent, name, ino)
			return fresh, fuse.OK
		}
	}
	cached := cachedInode{inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds()}
	fs.cache.putEntryAtIf(generation, epoch, parent, name, ino)
	fs.cache.putInodeAtIf(generation, epoch, ino, inode, cached.entryTimeout, cached.attrTimeout)
	return cached, fuse.OK
}

func (fs *RawFS) getInodeWithContext(parentCtx context.Context, cancel <-chan struct{}, ino uint64) (cachedInode, fuse.Status) {
	if inode, ok := fs.tmpMgr.LocalInode(ino); ok {
		return cachedInode{inode: inode}, fuse.OK
	}
	serverIno, status := fs.ToServerIno(ino)
	if status != fuse.OK {
		return cachedInode{}, status
	}
	if fs.writeBehind.HasPending(serverIno) {
		fs.writeBehind.BarrierInode(parentCtx, serverIno)
	}
	if cached, ok := fs.cache.inode(ino); ok {
		return cached, fuse.OK
	}
	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContextFrom(parentCtx, cancel)
	defer cancelFunc()
	resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		Ino:    serverIno,
		DiskId: fs.cfg.DiskID,
	}))
	if fs.cache.version() != epoch {
		return cachedInode{}, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		return cachedInode{}, fs.ToStatus(err, cancel)
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return cachedInode{}, fuse.EIO
	}
	cached := cachedInode{inode: inode, attrTimeout: resp.Msg.GetAttrTimeoutSeconds()}
	fs.cache.refreshInodeAtIf(generation, epoch, ino, inode, cached.attrTimeout)
	return cached, fuse.OK
}

// Lookup 在缓存命中时直接返回目录项。
func (fs *RawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (status fuse.Status) {
	opCtx, endSpan := fs.startFuseOp(OpLookup)
	defer func() { endSpan(status) }()

	if fs.isVirtualStatsName(header.NodeId, name) {
		fs.fillVirtualStatsEntryOut(out)
		return fuse.OK
	}
	if _, st := fs.ToServerIno(header.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	cached, status := fs.lookupInodeWithContext(opCtx, cancel, header.NodeId, name)
	if status == fuse.OK {
		fs.FillEntryOut(out, cached.inode, cached.entryTimeout, cached.attrTimeout)
		out.NodeId = fs.tmpMgr.ObserveKernelLookup(cached.inode.GetIno())
		out.Ino = out.NodeId
	}
	return status
}

// GetAttr 在缓存命中时直接返回节点属性。
func (fs *RawFS) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) (status fuse.Status) {
	opCtx, endSpan := fs.startFuseOp(OpGetAttr)
	defer func() { endSpan(status) }()

	if fs.isVirtualStatsIno(in.NodeId) {
		out.SetTimeout(0)
		fs.fillVirtualStatsAttr(&out.Attr)
		return fuse.OK
	}
	if _, st := fs.ToServerIno(in.NodeId); st != fuse.OK {
		return st
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if inode, removed := fs.handles.RemovedDir(in.NodeId); removed {
		fs.FillAttrOut(out, inode, 0)
		return fuse.OK
	}
	if handled, st := fs.tmpMgr.GetAttr(in.NodeId, &out.Attr); handled {
		out.SetTimeout(0)
		return st
	}
	cached, status := fs.getInodeWithContext(opCtx, cancel, in.NodeId)
	if status == fuse.OK {
		fs.FillAttrOut(out, cached.inode, cached.attrTimeout)
	}
	return status
}

// StatFs 实现文件系统容量与 Inode 配额统计查询，对应 Linux FUSE FUSE_STATFS 协议。
func (fs *RawFS) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) (status fuse.Status) {
	opCtx, endSpan := fs.startFuseOp(OpStatFs)
	defer func() { endSpan(status) }()

	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if fs.isVirtualStatsIno(in.NodeId) {
		inCopy := *in
		inCopy.NodeId = fuse.FUSE_ROOT_ID
		in = &inCopy
	}
	ino, status := fs.ToServerIno(in.NodeId)
	if status != fuse.OK {
		return status
	}
	fs.writeBehind.BarrierAll(opCtx)

	ctx, cancelFunc := fs.RPCContextFrom(opCtx, cancel)
	defer cancelFunc()

	resp, err := fs.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino:    ino,
		DiskId: fs.cfg.DiskID,
	}))
	if err != nil {
		return fs.ToStatus(err, cancel)
	}

	msg := resp.Msg
	out.Blocks = msg.GetBlocks()
	out.Bfree = msg.GetBfree()
	out.Bavail = msg.GetBavail()
	out.Files = msg.GetFiles()
	out.Ffree = msg.GetFfree()
	out.Bsize = msg.GetBsize()
	out.Frsize = msg.GetFrsize()
	out.NameLen = msg.GetNamelen()

	return fuse.OK
}
