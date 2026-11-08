package server

import (
	"context"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func toProtoBranch(branch *meta.Branch) *storagev1.Branch {
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
	return connect.NewResponse(&storagev1.CreateBranchResponse{Branch: toProtoBranch(branch)}), nil
}

// ListBranches 列出逻辑分支。
func (s *Server) ListBranches(ctx context.Context, req *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
	branches, err := s.metaManager.ListBranches(ctx, req.Msg.DiskId)
	if err != nil {
		return nil, toConnectError(err)
	}
	out := make([]*storagev1.Branch, 0, len(branches))
	for i := range branches {
		out = append(out, toProtoBranch(&branches[i]))
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
	return connect.NewResponse(&storagev1.CommitResponse{Commit: toProtoCommit(info)}), nil
}

func toProtoCommit(info *meta.CommitInfo) *storagev1.CommitInfo {
	return &storagev1.CommitInfo{
		CommitId: info.CommitID, DiskId: info.DiskID, BranchName: info.BranchName,
		ParentCommitId: info.ParentCommitID, MergeParentCommitId: info.MergeParentCommitID,
		Message: info.Message, CreatedAt: timestamppb.New(info.CreatedAt),
	}
}

// ResolveMerge 在清理冲突副本后完成合并。
func (s *Server) ResolveMerge(ctx context.Context, req *connect.Request[storagev1.ResolveMergeRequest]) (*connect.Response[storagev1.ResolveMergeResponse], error) {
	info, err := s.metaManager.ResolveMerge(ctx, meta.ResolveMergeParams{DiskID: req.Msg.DiskId, BranchName: req.Msg.BranchName, Message: req.Msg.Message})
	if err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.ResolveMergeResponse{Commit: toProtoCommit(info)}), nil
}

// AbortMerge 恢复合并前的工作区。
func (s *Server) AbortMerge(ctx context.Context, req *connect.Request[storagev1.AbortMergeRequest]) (*connect.Response[storagev1.AbortMergeResponse], error) {
	if err := s.metaManager.AbortMerge(ctx, meta.AbortMergeParams{DiskID: req.Msg.DiskId, BranchName: req.Msg.BranchName}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.AbortMergeResponse{}), nil
}

// Merge 将源分支的变更合入目标分支。
func (s *Server) Merge(ctx context.Context, req *connect.Request[storagev1.MergeRequest]) (*connect.Response[storagev1.MergeResponse], error) {
	result, err := s.metaManager.Merge(ctx, meta.MergeParams{
		DiskID: req.Msg.DiskId, SourceBranch: req.Msg.SourceBranch, TargetBranch: req.Msg.TargetBranch,
		Strategy: meta.ConflictStrategy(req.Msg.Strategy),
	})
	if err != nil {
		return nil, toConnectError(err)
	}
	response := &storagev1.MergeResponse{Result: storagev1.MergeResult(result.Result), MergeCommitId: result.MergeCommitID}
	for _, conflict := range result.Conflicts {
		item := &storagev1.MergeConflict{Path: conflict.Path, Reason: conflict.Reason}
		if conflict.BaseAttr != nil {
			item.BaseAttr = toProtoAttribute(&conflict.BaseAttr.Attr)
		}
		if conflict.OursAttr != nil {
			item.OursAttr = toProtoAttribute(&conflict.OursAttr.Attr)
		}
		if conflict.TheirsAttr != nil {
			item.TheirsAttr = toProtoAttribute(&conflict.TheirsAttr.Attr)
		}
		response.Conflicts = append(response.Conflicts, item)
	}
	return connect.NewResponse(response), nil
}

// Rollback 在无活跃写端时恢复版本快照。
func (s *Server) Rollback(ctx context.Context, req *connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error) {
	if err := s.metaManager.Rollback(ctx, meta.RollbackParams{DiskID: req.Msg.DiskId, BranchName: req.Msg.BranchName, CommitID: req.Msg.CommitId}); err != nil {
		return nil, toConnectError(err)
	}
	return connect.NewResponse(&storagev1.RollbackResponse{}), nil
}
