package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// lookupInode 查找目录项；跨机 RPC 在锁外并发执行。
func (fs *RawFS) lookupInode(cancel <-chan struct{}, parent uint64, name string) (cachedInode, fuse.Status) {
	if ino, ok := fs.cache.entry(parent, name); ok {
		if ino == 0 {
			return cachedInode{}, fuse.ENOENT
		}
		if cached, ok := fs.cache.inode(ino); ok {
			return cached, fuse.OK
		}
	}
	parentIno, status := fs.ToServerIno(parent)
	if status != fuse.OK {
		return cachedInode{}, status
	}
	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()
	resp, err := fs.client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: parentIno, Name: []byte(name), SessionId: fs.currentSession()}))
	if fs.cache.version() != epoch {
		return cachedInode{}, fuse.Status(syscall.ESTALE)
	}
	if err != nil {
		status := fs.ToStatus(err, cancel)
		if status == fuse.ENOENT {
			fs.cache.putEntryAtIf(generation, epoch, parent, name, 0)
		}
		return cachedInode{}, status
	}
	inode := resp.Msg.GetInode()
	if inode == nil || inode.GetAttr() == nil {
		return cachedInode{}, fuse.EIO
	}
	ino := fs.ToKernelIno(inode.GetIno())
	cached := cachedInode{inode, resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds()}
	fs.cache.putEntryAtIf(generation, epoch, parent, name, ino)
	fs.cache.putInodeAtIf(generation, epoch, ino, inode, cached.entryTimeout, cached.attrTimeout)
	return cached, fuse.OK
}

func (fs *RawFS) getInode(cancel <-chan struct{}, ino uint64) (cachedInode, fuse.Status) {
	if inode, ok := fs.tmpMgr.LocalInode(ino); ok {
		return cachedInode{inode: inode}, fuse.OK
	}
	if cached, ok := fs.cache.inode(ino); ok {
		return cached, fuse.OK
	}
	serverIno, status := fs.ToServerIno(ino)
	if status != fuse.OK {
		return cachedInode{}, status
	}
	epoch, generation := fs.cache.snapshot()
	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()
	resp, err := fs.client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: serverIno, SessionId: fs.currentSession()}))
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
func (fs *RawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	if _, status := fs.ToServerIno(header.NodeId); status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	cached, status := fs.lookupInode(cancel, header.NodeId, name)
	if status == fuse.OK {
		fs.FillEntryOut(out, cached.inode, cached.entryTimeout, cached.attrTimeout)
		out.NodeId = fs.tmpMgr.ObserveKernelLookup(cached.inode.GetIno())
		out.Ino = out.NodeId
	}
	return status
}

// GetAttr 在缓存命中时直接返回节点属性。
func (fs *RawFS) GetAttr(cancel <-chan struct{}, in *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	if _, status := fs.ToServerIno(in.NodeId); status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	if inode, removed := fs.handles.RemovedDir(in.NodeId); removed {
		fs.FillAttrOut(out, inode, 0)
		return fuse.OK
	}
	if handled, status := fs.tmpMgr.GetAttr(in.NodeId, &out.Attr); handled {
		out.SetTimeout(0)
		return status
	}
	cached, status := fs.getInode(cancel, in.NodeId)
	if status == fuse.OK {
		fs.FillAttrOut(out, cached.inode, cached.attrTimeout)
	}
	return status
}

// StatFs 实现文件系统容量与 Inode 配额统计查询，对应 Linux FUSE FUSE_STATFS 协议。
func (fs *RawFS) StatFs(cancel <-chan struct{}, in *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	ino, status := fs.ToServerIno(in.NodeId)
	if status != fuse.OK {
		return status
	}

	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()

	resp, err := fs.client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		Ino: ino, SessionId: fs.currentSession(),
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
