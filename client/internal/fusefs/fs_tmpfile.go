package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type rawFSTmpMaterializer struct {
	fs     *RawFS
	cancel <-chan struct{}
	sid    *uint64
}

func (fs *RawFS) newTmpMaterializer(cancel <-chan struct{}) *rawFSTmpMaterializer {
	return &rawFSTmpMaterializer{
		fs:     fs,
		cancel: cancel,
		sid:    fs.currentSession(),
	}
}

func (m *rawFSTmpMaterializer) CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (*storagev1.Inode, float64, float64, fuse.Status) {
	resp, err := m.fs.client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
		SessionId: m.sid,
	}))
	if err != nil {
		return nil, 0, 0, m.fs.ToStatus(err, m.cancel)
	}
	return resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (m *rawFSTmpMaterializer) WriteChunk(ctx context.Context, serverIno, offset uint64, data []byte) fuse.Status {
	written, err := m.fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		Ino:       serverIno,
		Offset:    offset,
		Data:      data,
		SessionId: m.sid,
	}))
	if err != nil {
		return m.fs.ToStatusWithSession(err, m.cancel)
	}
	if written.Msg.GetSize() != uint32(len(data)) {
		return fuse.EIO
	}
	return fuse.OK
}

func (m *rawFSTmpMaterializer) CommitTmpAttr(ctx context.Context, serverIno, size uint64, atime, mtime *timestamppb.Timestamp) (*storagev1.Inode, fuse.Status) {
	updated, err := m.fs.client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:       serverIno,
		Size:      &size,
		Atime:     atime,
		Mtime:     mtime,
		SessionId: m.sid,
	}))
	if err != nil {
		return nil, m.fs.ToStatus(err, m.cancel)
	}
	return updated.Msg.GetInode(), fuse.OK
}

func (m *rawFSTmpMaterializer) RollbackTmpInode(_ context.Context, parentIno uint64, name string) {
	ctx, done := m.fs.RPCContext(nil)
	defer done()
	open := false
	_, _ = m.fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		SessionId:    m.fs.currentSession(),
	}))
}

func (m *rawFSTmpMaterializer) UnlinkOpenTmpInode(ctx context.Context, parentIno uint64, name string) fuse.Status {
	open := true
	_, err := m.fs.client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		SessionId:    m.sid,
	}))
	if err != nil {
		return m.fs.ToStatus(err, m.cancel)
	}
	return fuse.OK
}

func (m *rawFSTmpMaterializer) LinkInode(ctx context.Context, serverIno, newParentIno uint64, name string) (*storagev1.Inode, float64, float64, fuse.Status) {
	resp, err := m.fs.client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          serverIno,
		NewParentIno: newParentIno,
		NewName:      []byte(name),
		SessionId:    m.sid,
	}))
	if err != nil {
		return nil, 0, 0, m.fs.ToStatus(err, m.cancel)
	}
	return resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

// Tmpfile 创建只存在于客户端内存的匿名文件。
func (fs *RawFS) Tmpfile(_ <-chan struct{}, input *fuse.CreateIn, out *fuse.CreateOut) fuse.Status {
	if status := fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return status
	}
	mode := input.Flags & uint32(syscall.O_ACCMODE)
	if mode != syscall.O_WRONLY && mode != syscall.O_RDWR {
		return fuse.EINVAL
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	ino, inode := fs.tmpMgr.Create(input.NodeId, input.Uid, input.Gid, input.Mode, input.Flags)
	fh, status := fs.openHandle(ino, input.Flags, false)
	if status != fuse.OK {
		fs.tmpMgr.OnForget(ino, 1, fs.handles.IsOpen)
		return status
	}
	fs.caps.markAbsent(ino)
	fs.FillEntryOut(&out.EntryOut, inode, 0, 0)
	out.Fh = fh
	if fs.cfg.DirectIO {
		out.OpenFlags = fuse.FOPEN_DIRECT_IO
	}
	return fuse.OK
}

// Forget 释放内核对具名匿名节点的引用。
func (fs *RawFS) Forget(nodeID, nlookup uint64) {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	fs.caps.forget(nodeID)
	if fs.tmpMgr.OnForget(nodeID, nlookup, fs.handles.IsOpen) {
		fs.cache.invalidateInode(nodeID)
	}
}

// linkTmp 将匿名文件具名化，已具名化的匿名节点不能再次通过此路径 Link。
func (fs *RawFS) linkTmp(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (fuse.Status, bool) {
	ctx, done := fs.RPCContext(cancel)
	defer done()
	epoch := fs.cache.version()
	mat := fs.newTmpMaterializer(cancel)
	serverIno, firstUpload, inode, entryTimeout, attrTimeout, handled, status := fs.tmpMgr.Link(
		ctx, input.Oldnodeid, input.NodeId, filename, fs.handles.IsOpen(input.Oldnodeid), mat, out,
	)
	if !handled {
		return fuse.OK, false
	}
	if status != fuse.OK {
		return status, true
	}
	if firstUpload {
		fs.markDirty(serverIno)
	} else {
		fs.handles.ClearOrphan(input.Oldnodeid)
		fs.stateMu.Lock()
		delete(fs.orphanSessions, input.Oldnodeid)
		fs.stateMu.Unlock()
	}
	fs.cache.invalidateDirListing(input.NodeId)
	fs.cache.putEntryAt(epoch, input.NodeId, filename, input.Oldnodeid)
	fs.cache.putInodeAt(epoch, input.Oldnodeid, inode, entryTimeout, attrTimeout)
	return fuse.OK, true
}

// syncTmpFile 将匿名文件上云并解链，供 Flush / Fsync 完成持久化。
func (fs *RawFS) syncTmpFile(cancel <-chan struct{}, ino uint64) fuse.Status {
	ctx, done := fs.RPCContext(cancel)
	defer done()
	mat := fs.newTmpMaterializer(cancel)
	parentKernelIno, tempName, serverIno, uploaded, status := fs.tmpMgr.Sync(ctx, ino, mat)
	if tempName != "" {
		fs.cache.invalidateDirListing(parentKernelIno)
		fs.cache.removeEntry(parentKernelIno, tempName)
	}
	if status != fuse.OK {
		return status
	}
	if uploaded {
		fs.markDirty(serverIno)
		fs.markOrphan(ino, mat.sid)
	}
	return fuse.OK
}
