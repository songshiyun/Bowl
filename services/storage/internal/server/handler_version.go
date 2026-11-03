package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func toProtoBranch(branch meta.Branch) *storagev1.Branch {
	state := storagev1.BranchState_BRANCH_STATE_NORMAL
	if branch.State == "MERGING" {
		state = storagev1.BranchState_BRANCH_STATE_MERGING
	}
	return &storagev1.Branch{
		BranchId: branch.BranchID, DiskId: branch.DiskID, Name: branch.Name,
		ParentBranch: branch.ParentBranch, ForkCommitId: branch.ForkCommitID,
		HeadCommitId: branch.HeadCommitID, State: state, CreatedAt: timestamppb.New(branch.CreatedAt),
	}
}

// CreateBranch 派生逻辑分支。
func (s *Server) CreateBranch(ctx context.Context, req *connect.Request[storagev1.CreateBranchRequest]) (*connect.Response[storagev1.CreateBranchResponse], error) {
	branch, err := s.metaManager.CreateBranch(ctx, meta.CreateBranchParams{
		DiskID: req.Msg.DiskId, Name: req.Msg.Name, SourceBranch: req.Msg.SourceBranch, SourceCommitID: req.Msg.SourceCommitId,
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CreateBranchResponse{Branch: toProtoBranch(*branch)}), nil
}

// ListBranches 列出逻辑分支。
func (s *Server) ListBranches(ctx context.Context, req *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
	branches, err := s.metaManager.ListBranches(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	out := make([]*storagev1.Branch, 0, len(branches))
	for _, branch := range branches {
		out = append(out, toProtoBranch(branch))
	}
	return connect.NewResponse(&storagev1.ListBranchesResponse{Branches: out}), nil
}

// DeleteBranch 删除逻辑分支。
func (s *Server) DeleteBranch(ctx context.Context, req *connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error) {
	if err := s.metaManager.DeleteBranch(ctx, meta.DeleteBranchParams{DiskID: req.Msg.DiskId, Name: req.Msg.Name}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.DeleteBranchResponse{}), nil
}

// Commit 定格版本快照。
func (s *Server) Commit(ctx context.Context, req *connect.Request[storagev1.CommitRequest]) (*connect.Response[storagev1.CommitResponse], error) {
	info, err := s.metaManager.Commit(ctx, meta.CommitParams{DiskID: req.Msg.DiskId, BranchName: req.Msg.BranchName, Message: req.Msg.Message})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.CommitResponse{Commit: &storagev1.CommitInfo{
		CommitId: info.CommitID, DiskId: info.DiskID, BranchName: info.BranchName,
		ParentCommitId: info.ParentCommitID, MergeParentCommitId: info.MergeParentCommitID,
		Message: info.Message, CreatedAt: timestamppb.New(info.CreatedAt),
	}}), nil
}

// Rollback 在无活跃写端时恢复版本快照。
func (s *Server) Rollback(ctx context.Context, req *connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error) {
	if err := s.metaManager.Rollback(ctx, meta.RollbackParams{DiskID: req.Msg.DiskId, BranchName: req.Msg.BranchName, CommitID: req.Msg.CommitId}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.RollbackResponse{}), nil
}
