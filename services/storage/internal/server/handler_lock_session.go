package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// RenewSession 显式延长客户端会话租约有效期并顺带回收孤儿节点。
func (s *Server) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if req.Msg.DiskId == nil {
		return nil, toConnectError(meta.ErrInvalidArgument)
	}
	res, err := s.metaManager.RenewBranchSession(ctx, meta.RenewBranchSessionParams{
		SessionID: req.Msg.SessionId, DiskID: *req.Msg.DiskId,
		BranchName: req.Msg.BranchName, ReadOnly: req.Msg.ReadOnly, CommitID: req.Msg.CommitId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt:        timestamppb.New(res.ExpiresAt),
		SessionRecreated: res.SessionRecreated,
	}), nil
}

// CloseSession 在客户端优雅卸载时即时注销指定会话租约。
func (s *Server) CloseSession(ctx context.Context, req *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error) {
	if err := s.metaManager.CloseSession(ctx, req.Msg.GetSessionId()); err != nil {
		return nil, toConnectError(err)
	}

	return connect.NewResponse(&storagev1.CloseSessionResponse{}), nil
}

// ReclaimOrphan 回收最后一个本地句柄关闭的孤儿节点。
func (s *Server) ReclaimOrphan(ctx context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	err := s.metaManager.ReclaimOrphan(ctx, meta.ReclaimOrphanParams{
		SessionID: req.Msg.GetSessionId(), DiskID: req.Msg.GetDiskId(),
		BranchName: req.Msg.BranchName, Ino: req.Msg.GetIno(),
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}
