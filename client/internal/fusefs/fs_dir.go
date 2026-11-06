package fusefs

import (
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// OpenDir 在本地检查节点类型并分配目录句柄。
func (fs *RawFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return status
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	cached, status := fs.getInode(cancel, input.NodeId)
	if status != fuse.OK {
		return status
	}
	if cached.inode.GetAttr().GetMode()&syscall.S_IFMT != syscall.S_IFDIR {
		return fuse.Status(syscall.ENOTDIR)
	}
	fh, status := fs.openHandle(input.NodeId, input.Flags, true)
	if status == fuse.OK {
		out.Fh = fh
	}
	return status
}

// readDirEntries 获取完整目录流；超出容量的目录按页读取且不进入缓存。
func (fs *RawFS) readDirEntries(cancel <-chan struct{}, input *fuse.ReadIn) ([]*storagev1.DirEntryPlus, fuse.Status) {
	_, sid, status := fs.checkedHandle(input.NodeId, input.Fh, true)
	if status != fuse.OK {
		return nil, status
	}
	if entries, ok := fs.cache.dir(input.NodeId); ok {
		return entries, fuse.OK
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return nil, status
	}
	epoch, generation := fs.cache.snapshot()
	ctx, done := fs.RPCContext(cancel)
	defer done()
	offset := input.Offset
	var entries []*storagev1.DirEntryPlus
	for {
		resp, err := fs.client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{
			Ino: ino, Offset: offset, Limit: 512, SessionId: optionalSession(sid),
		}))
		if fs.cache.version() != epoch {
			return nil, fuse.Status(syscall.ESTALE)
		}
		if err != nil {
			return nil, fs.ToStatusWithSession(err, cancel)
		}
		page := resp.Msg.GetEntries()
		for _, e := range page {
			if e.GetNextOffset() <= offset {
				return nil, fuse.EIO
			}
			offset = e.GetNextOffset()
			e.Ino = fs.ToKernelIno(e.GetIno())
		}
		entries = append(entries, page...)
		if input.Offset != 0 || len(entries)+1 > fs.cache.capacity {
			return entries, fuse.OK
		}
		if len(page) < 512 {
			fs.cache.putDirAtIf(generation, epoch, input.NodeId, entries)
			return entries, fuse.OK
		}
	}
}

// ReadDir 返回目录项及其续读位置。
func (fs *RawFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	entries, status := fs.readDirEntries(cancel, input)
	if status != fuse.OK {
		return status
	}
	for _, entry := range entries {
		if entry.GetNextOffset() <= input.Offset {
			continue
		}
		mode := uint32(syscall.S_IFDIR)
		if entry.GetInode() != nil && entry.GetInode().GetAttr() != nil {
			mode = entry.GetInode().GetAttr().GetMode()
		}
		if !out.AddDirEntry(fuse.DirEntry{Ino: entry.GetIno(), Name: string(entry.GetName()), Mode: mode, Off: entry.GetNextOffset()}) {
			break
		}
	}
	return fuse.OK
}

// ReadDirPlus 返回目录项和属性；点目录项不增加内核引用计数。
func (fs *RawFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	entries, status := fs.readDirEntries(cancel, input)
	if status != fuse.OK {
		return status
	}
	for _, entry := range entries {
		if entry.GetNextOffset() <= input.Offset {
			continue
		}
		mode := uint32(syscall.S_IFDIR)
		if entry.GetInode() != nil && entry.GetInode().GetAttr() != nil {
			mode = entry.GetInode().GetAttr().GetMode()
		}
		name := string(entry.GetName())
		eo := out.AddDirLookupEntry(fuse.DirEntry{Ino: entry.GetIno(), Name: name, Mode: mode, Off: entry.GetNextOffset()})
		if eo == nil {
			break
		}
		if name == "." || name == ".." {
			*eo = fuse.EntryOut{}
		} else {
			fs.FillEntryOut(eo, entry.GetInode(), entry.GetEntryTimeoutSeconds(), entry.GetAttrTimeoutSeconds())
			eo.NodeId = fs.tmpMgr.ObserveKernelLookup(entry.GetIno())
			eo.Attr.Ino = eo.NodeId
		}
	}
	return fuse.OK
}

// FsyncDir 等待服务端完成目录持久化。
func (fs *RawFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	_, sid, status := fs.checkedHandle(input.NodeId, input.Fh, true)
	if status != fuse.OK {
		return status
	}
	if _, removed := fs.handles.RemovedDir(input.NodeId); removed {
		// Rmdir 已同步持久化，已删除目录没有待同步的服务端状态。
		return fuse.OK
	}
	ino, status := fs.ToServerIno(input.NodeId)
	if status != fuse.OK {
		return status
	}
	ctx, cancelFunc := fs.RPCContext(cancel)
	defer cancelFunc()
	_, err := fs.client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		Ino: ino, Fh: 0, Datasync: input.FsyncFlags&1 != 0, SessionId: optionalSession(sid),
	}))
	return fs.ToStatusWithSession(err, cancel)
}

// ReleaseDir 只释放本地目录句柄。
func (fs *RawFS) ReleaseDir(input *fuse.ReleaseIn) {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	h, ok := fs.handles.Get(input.Fh)
	if !ok || !h.IsDir || h.Ino != input.NodeId {
		return
	}
	fs.locks.ForgetHandle(input.Fh)
	fs.handles.Release(input.Fh)
	if fs.sessions != nil {
		fs.sessions.UnbindHandle(input.Fh)
	}
}
