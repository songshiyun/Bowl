package server

import (
	"context"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// Create 在指定父目录下原子创建常规文件并分配初始文件句柄 (FileHandle)。
func (s *Server) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	msg := req.Msg
	params := meta.CreateParams{
		ParentIno: msg.ParentIno,
		Name:      msg.Name,
		Mode:      msg.Mode,
		UID:       msg.Uid,
		GID:       msg.Gid,
		Flags:     msg.Flags,
		SessionID: msg.SessionId,
	}
	res, err := s.metaManager.Create(ctx, params)
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               toProtoInode(res.Inode),
		Fh:                  res.Fh,
		OpenFlags:           res.OpenFlags,
		EntryTimeoutSeconds: res.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  res.AttrTimeoutSeconds,
	}), nil
}

// Fsync 同步并持久化文件节点与句柄关联的状态。
func (s *Server) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	msg := req.Msg
	err := s.metaManager.Fsync(ctx, meta.FsyncParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Datasync:  msg.Datasync,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

// FsyncDir 同步并持久化目录节点与目录句柄关联的状态。
func (s *Server) FsyncDir(ctx context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	msg := req.Msg
	err := s.metaManager.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Datasync:  msg.Datasync,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

// ReadDir 读取指定目录的子项流（基本条目）。
func (s *Server) ReadDir(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.ReadDir(ctx, meta.ReadDirParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Limit:     msg.Limit,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DirEntry, len(res))
	for i, e := range res {
		entries[i] = toProtoDirEntry(e)
	}

	return connect.NewResponse(&storagev1.ReadDirResponse{
		Entries: entries,
	}), nil
}

// ReadDirPlus 读取指定目录的子项流并附带完整 Inode 属性与缓存时效（扩展条目）。
func (s *Server) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	msg := req.Msg
	res, err := s.metaManager.ReadDirPlus(ctx, meta.ReadDirPlusParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Limit:     msg.Limit,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	entries := make([]*storagev1.DirEntryPlus, len(res))
	for i, e := range res {
		entries[i] = toProtoDirEntryPlus(e)
	}

	return connect.NewResponse(&storagev1.ReadDirPlusResponse{
		Entries: entries,
	}), nil
}

// Fallocate 为常规文件扩展逻辑空间或清零指定区间。
func (s *Server) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	msg := req.Msg
	err := s.metaManager.Fallocate(ctx, meta.FallocateParams{
		Ino:       msg.Ino,
		Fh:        msg.Fh,
		Offset:    msg.Offset,
		Length:    msg.Length,
		Mode:      msg.Mode,
		SessionID: msg.SessionId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

// toProtoDirEntry 将 meta.DirEntry 转换为 proto storagev1.DirEntry。
func toProtoDirEntry(e meta.DirEntry) *storagev1.DirEntry {
	return &storagev1.DirEntry{
		Ino:        e.Ino,
		Name:       e.Name,
		Mode:       e.Mode,
		NextOffset: e.NextOffset,
	}
}

// toProtoDirEntryPlus 将 meta.DirEntryPlus 转换为 proto storagev1.DirEntryPlus。
func toProtoDirEntryPlus(e meta.DirEntryPlus) *storagev1.DirEntryPlus {
	return &storagev1.DirEntryPlus{
		Ino:                 e.Ino,
		Name:                e.Name,
		NextOffset:          e.NextOffset,
		Inode:               toProtoInode(e.Inode),
		EntryTimeoutSeconds: e.EntryTimeoutSeconds,
		AttrTimeoutSeconds:  e.AttrTimeoutSeconds,
	}
}
