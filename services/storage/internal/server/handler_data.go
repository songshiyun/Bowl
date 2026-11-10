package server

import (
	"context"
	"math"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const maxReadWriteSize = 4 << 20 // 4 MiB 单次读写上限

// Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。
func (s *Server) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	msg := req.Msg
	if msg.Fh > math.MaxInt64 || msg.Size > maxReadWriteSize {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	data, err := actor.Read(msg.Ino, msg.Offset, msg.Size)
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.ReadResponse{
		Data: data,
	}), nil
}

// Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。
func (s *Server) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	msg := req.Msg
	if err := validateWriteEpoch(msg.Epoch); err != nil {
		return nil, toConnectError(err)
	}
	if msg.Fh > math.MaxInt64 || len(msg.Data) > maxReadWriteSize {
		return nil, toConnectError(store.ErrInvalid)
	}
	actor, err := s.control.Actor(ctx, msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	if ctxErr := ctx.Err(); ctxErr != nil {
		return nil, toConnectError(ctxErr)
	}
	written, attr, err := actor.Write(msg.Ino, msg.Offset, msg.Data, msg.Epoch)
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  written,
		Inode: toProtoInode(&attr),
	}), nil
}
