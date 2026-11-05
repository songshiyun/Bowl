package cli_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestCommitAndRollback(t *testing.T) {
	diskID, commitID := uuid.NewString(), uuid.NewString()
	var commitReq *storagev1.CommitRequest
	var rollbackReq *storagev1.RollbackRequest
	h := &versionHandler{
		commit: func(req *connect.Request[storagev1.CommitRequest]) (*connect.Response[storagev1.CommitResponse], error) {
			commitReq = req.Msg
			return connect.NewResponse(&storagev1.CommitResponse{Commit: &storagev1.CommitInfo{CommitId: commitID, DiskId: req.Msg.DiskId, BranchName: req.Msg.BranchName, Message: req.Msg.Message}}), nil
		},
		rollback: func(req *connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error) {
			rollbackReq = req.Msg
			return connect.NewResponse(&storagev1.RollbackResponse{}), nil
		},
	}
	endpoint := setupTestStorageServer(t, h)
	code, out, errOut := runVersionCLI("commit", diskID, "--endpoint", endpoint, "-m", "初始版本", "--json")
	require.Equal(t, 0, code, errOut)
	require.Equal(t, diskID, commitReq.DiskId)
	require.Equal(t, "main", commitReq.BranchName)
	require.Equal(t, "初始版本", commitReq.Message)
	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	require.Equal(t, commitID, result["commit_id"])
	code, out, errOut = runVersionCLI("commit", "--branch=B", "--message=版本二", "--endpoint", endpoint, diskID)
	require.Equal(t, 0, code, errOut)
	require.Equal(t, "B", commitReq.BranchName)
	require.Equal(t, "版本二", commitReq.Message)
	require.Contains(t, out, commitID)
	code, out, errOut = runVersionCLI("rollback", diskID, "--to", commitID, "--endpoint", endpoint, "--json")
	require.Equal(t, 0, code, errOut)
	require.Equal(t, diskID, rollbackReq.DiskId)
	require.Equal(t, "main", rollbackReq.BranchName)
	require.Equal(t, commitID, rollbackReq.CommitId)
	require.NoError(t, json.Unmarshal([]byte(out), &result))
	require.Equal(t, commitID, result["commit_id"])
	code, out, errOut = runVersionCLI("rollback", diskID, "--branch", "B", "--to", commitID, "--endpoint", endpoint)
	require.Equal(t, 0, code, errOut)
	require.Equal(t, "B", rollbackReq.BranchName)
	require.Contains(t, out, commitID)
	code, _, errOut = runVersionCLI("rollback", diskID, "--to", strings.ReplaceAll(commitID, "-", ""), "--endpoint", endpoint)
	require.Equal(t, 0, code, errOut)
	require.Equal(t, commitID, rollbackReq.CommitId)
	h.rollback = func(_ *connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("busy"))
	}
	code, _, errOut = runVersionCLI("rollback", diskID, "--to", commitID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "EBUSY")
	require.Contains(t, errOut, "卸载可写挂载")
}

func TestCommitRollbackValidationAndErrors(t *testing.T) {
	id := uuid.NewString()
	for _, args := range [][]string{
		{"commit"}, {"commit", "bad"}, {"commit", uuid.Nil.String()},
		{"commit", id, "--branch="}, {"commit", id, "--branch", ".."}, {"commit", id, "-m"},
		{"rollback", id}, {"rollback", id, "--to", uuid.Nil.String()}, {"rollback", id, "--to", "bad"},
		{"rollback", id, "--to", id, "--branch", "bad/name"},
	} {
		code, _, _ := runVersionCLI(args...)
		require.Equal(t, 2, code, "%v", args)
	}
	h := &versionHandler{
		commit: func(_ *connect.Request[storagev1.CommitRequest]) (*connect.Response[storagev1.CommitResponse], error) {
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("服务不可用"))
		},
		rollback: func(_ *connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("快照不存在"))
		},
	}
	endpoint := setupTestStorageServer(t, h)
	code, _, errOut := runVersionCLI("commit", id, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "创建版本快照失败")
	code, _, errOut = runVersionCLI("rollback", id, "--to", id, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "回档分支失败")
}
