package fusefs

import (
	"context"
	"syscall"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

type rawFSTmpMaterializer struct {
	fs     *RawFS
	cancel <-chan struct{}
}

func (fs *RawFS) newTmpMaterializer(cancel <-chan struct{}) *rawFSTmpMaterializer {
	return &rawFSTmpMaterializer{
		fs:     fs,
		cancel: cancel,
	}
}

func (m *rawFSTmpMaterializer) CreateTmpInode(ctx context.Context, parentIno uint64, name string, mode, uid, gid uint32) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status) {
	reqCtx := rpc.WithRequestID(ctx, m.fs.NextRequestID())
	resp, err := m.fs.client.Mknod(reqCtx, connect.NewRequest(&storagev1.MknodRequest{
		ParentIno: parentIno,
		Name:      []byte(name),
		Mode:      mode,
		Uid:       uid,
		Gid:       gid,
		DiskId:    m.fs.cfg.DiskID,
		Epoch:     m.fs.cfg.Epoch,
	}))
	if err != nil {
		return nil, 0, 0, m.fs.ToStatus(err, m.cancel)
	}
	return resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

func (m *rawFSTmpMaterializer) WriteChunk(ctx context.Context, serverIno, offset uint64, data []byte) fuse.Status {
	reqCtx := rpc.WithRequestID(ctx, m.fs.NextRequestID())
	written, err := m.fs.client.Write(reqCtx, connect.NewRequest(&storagev1.WriteRequest{
		Ino:    serverIno,
		Offset: offset,
		Data:   data,
		DiskId: m.fs.cfg.DiskID,
		Epoch:  m.fs.cfg.Epoch,
	}))
	if err != nil {
		return m.fs.ToStatus(err, m.cancel)
	}
	if written.Msg.GetSize() != uint32(len(data)) {
		return fuse.EIO
	}
	return fuse.OK
}

func (m *rawFSTmpMaterializer) CommitTmpAttr(ctx context.Context, serverIno, size uint64, atime, mtime *timestamppb.Timestamp) (*storagev1.Inode, fuse.Status) {
	reqCtx := rpc.WithRequestID(ctx, m.fs.NextRequestID())
	updated, err := m.fs.client.SetAttr(reqCtx, connect.NewRequest(&storagev1.SetAttrRequest{
		Ino:    serverIno,
		Size:   &size,
		Atime:  atime,
		Mtime:  mtime,
		DiskId: m.fs.cfg.DiskID,
		Epoch:  m.fs.cfg.Epoch,
	}))
	if err != nil {
		return nil, m.fs.ToStatus(err, m.cancel)
	}
	return updated.Msg.GetInode(), fuse.OK
}

func (m *rawFSTmpMaterializer) RollbackTmpInode(ctx context.Context, parentIno uint64, name string) {
	m.fs.metrics.incTmpRollback()
	baseCtx := context.Background()
	if ctx != nil {
		baseCtx = context.WithoutCancel(ctx)
	}
	rollbackCtx, done := m.fs.RPCContextFrom(baseCtx, nil)
	defer done()
	open := false
	_, err := m.fs.client.Unlink(rollbackCtx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		DiskId:       m.fs.cfg.DiskID,
		Epoch:        m.fs.cfg.Epoch,
	}))
	if err != nil {
		m.fs.metrics.incTmpRollbackError()
		m.fs.logger().WarnContext(rollbackCtx, "tmpfile_rollback_failed",
			"parent_ino", parentIno,
			"name", name,
			"error", err,
		)
	}
}

func (m *rawFSTmpMaterializer) UnlinkOpenTmpInode(ctx context.Context, parentIno uint64, name string) fuse.Status {
	reqCtx := rpc.WithRequestID(ctx, m.fs.NextRequestID())
	open := true
	_, err := m.fs.client.Unlink(reqCtx, connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno:    parentIno,
		Name:         []byte(name),
		OpenInClient: &open,
		DiskId:       m.fs.cfg.DiskID,
		Epoch:        m.fs.cfg.Epoch,
	}))
	if err != nil {
		return m.fs.ToStatus(err, m.cancel)
	}
	return fuse.OK
}

func (m *rawFSTmpMaterializer) LinkInode(ctx context.Context, serverIno, newParentIno uint64, name string) (inode *storagev1.Inode, entryTimeout, attrTimeout float64, status fuse.Status) {
	reqCtx := rpc.WithRequestID(ctx, m.fs.NextRequestID())
	resp, err := m.fs.client.Link(reqCtx, connect.NewRequest(&storagev1.LinkRequest{
		Ino:          serverIno,
		NewParentIno: newParentIno,
		NewName:      []byte(name),
		DiskId:       m.fs.cfg.DiskID,
		Epoch:        m.fs.cfg.Epoch,
	}))
	if err != nil {
		return nil, 0, 0, m.fs.ToStatus(err, m.cancel)
	}
	return resp.Msg.GetInode(), resp.Msg.GetEntryTimeoutSeconds(), resp.Msg.GetAttrTimeoutSeconds(), fuse.OK
}

// Tmpfile 创建只存在于客户端内存的匿名文件。
func (fs *RawFS) Tmpfile(_ <-chan struct{}, input *fuse.CreateIn, out *fuse.CreateOut) (status fuse.Status) {
	start := fs.metrics.beginOp(OpTmpfile)
	_, endSpan := fs.startFuseOp(OpTmpfile)
	defer func() {
		endSpan(status)
		fs.metrics.endOp(OpTmpfile, start, status, 0)
	}()

	if status = fs.CheckWritable(); status != fuse.OK {
		return status
	}
	if fs.isVirtualStatsIno(input.NodeId) {
		return fuse.Status(syscall.ENOTDIR)
	}
	if _, st := fs.ToServerIno(input.NodeId); st != fuse.OK {
		return st
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
	start := fs.metrics.beginOp(OpForget)
	defer fs.metrics.endOp(OpForget, start, fuse.OK, 0)

	if nodeID == VirtualStatsIno {
		return
	}
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	fs.caps.forget(nodeID)
	if fs.tmpMgr.OnForget(nodeID, nlookup, fs.handles.IsOpen) {
		fs.cache.invalidateInode(nodeID)
	}
}

// linkTmp 将匿名文件具名化，已具名化的匿名节点不能再次通过此路径 Link。
func (fs *RawFS) linkTmp(parentCtx context.Context, cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) (fuse.Status, bool) {
	if fs.tmpMgr.getFile(input.Oldnodeid) == nil {
		return fuse.OK, false
	}
	ctx, done := fs.RPCContextFrom(parentCtx, cancel)
	defer done()
	epoch := fs.cache.version()
	mat := fs.newTmpMaterializer(cancel)
	res, handled, status := fs.tmpMgr.Link(
		ctx, input.Oldnodeid, input.NodeId, filename, fs.handles.IsOpen(input.Oldnodeid), mat, out,
	)
	if !handled {
		return fuse.OK, false
	}
	if status != fuse.OK {
		return status, true
	}
	if res.FirstUpload {
		fs.markDirty(res.ServerIno)
	} else {
		fs.handles.ClearOrphan(input.Oldnodeid)
		fs.stateMu.Lock()
		delete(fs.orphanInodes, input.Oldnodeid)
		delete(fs.orphanSpans, input.Oldnodeid)
		fs.stateMu.Unlock()
	}
	fs.cache.invalidateDirListing(input.NodeId)
	fs.cache.putEntryAt(epoch, input.NodeId, filename, input.Oldnodeid)
	fs.cache.putInodeAt(epoch, input.Oldnodeid, res.Inode, res.EntryTimeout, res.AttrTimeout)
	return fuse.OK, true
}

// syncTmpFileWithContext 将匿名文件上云并解链，供 Flush / Fsync 完成持久化。
func (fs *RawFS) syncTmpFileWithContext(parentCtx context.Context, cancel <-chan struct{}, ino uint64) fuse.Status {
	if !fs.tmpMgr.HasPendingSync(ino) {
		return fuse.OK
	}
	ctx, done := fs.RPCContextFrom(parentCtx, cancel)
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
		fs.markOrphanWithContext(parentCtx, ino)
	}
	return fuse.OK
}
