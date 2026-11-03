package cli_test

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestMergeStrategiesAndResults(t *testing.T) {
	diskID, commitID := uuid.NewString(), uuid.NewString()
	var captured *storagev1.MergeRequest
	result := &storagev1.MergeResponse{Result: storagev1.MergeResult_MERGE_RESULT_MERGED, MergeCommitId: &commitID}
	h := &versionHandler{merge: func(req *connect.Request[storagev1.MergeRequest]) (*connect.Response[storagev1.MergeResponse], error) {
		captured = req.Msg
		return connect.NewResponse(result), nil
	}}
	endpoint := setupTestStorageServer(t, h)
	for _, tc := range []struct {
		flag     string
		expected storagev1.ConflictStrategy
	}{
		{"", storagev1.ConflictStrategy_CONFLICT_STRATEGY_ABORT_ON_CONFLICT},
		{"--strategy=", storagev1.ConflictStrategy_CONFLICT_STRATEGY_ABORT_ON_CONFLICT},
		{"--strategy=abort", storagev1.ConflictStrategy_CONFLICT_STRATEGY_ABORT_ON_CONFLICT},
		{"--strategy=ours", storagev1.ConflictStrategy_CONFLICT_STRATEGY_OURS},
		{"--strategy=theirs", storagev1.ConflictStrategy_CONFLICT_STRATEGY_THEIRS},
		{"--strategy=manual", storagev1.ConflictStrategy_CONFLICT_STRATEGY_MATERIALIZE},
		{"--strategy=materialize", storagev1.ConflictStrategy_CONFLICT_STRATEGY_MATERIALIZE},
	} {
		args := []string{"merge", diskID, "--from", "C", "--into", "B", "--endpoint", endpoint}
		if tc.flag != "" {
			args = append(args, tc.flag)
		}
		code, out, errOut := runVersionCLI(args...)
		require.Equal(t, 0, code, errOut)
		require.Contains(t, out, commitID)
		require.Equal(t, diskID, captured.DiskId)
		require.Equal(t, "C", captured.SourceBranch)
		require.Equal(t, "B", captured.TargetBranch)
		require.Equal(t, tc.expected, captured.Strategy)
	}
	result = &storagev1.MergeResponse{
		Result:    storagev1.MergeResult_MERGE_RESULT_CONFLICTS,
		Conflicts: []*storagev1.MergeConflict{{Path: "data.txt", Reason: "内容冲突", BaseAttr: &storagev1.InodeAttribute{Mode: 0o100644, Size: 2}, OursAttr: &storagev1.InodeAttribute{Mode: 0o100600, Size: 3}, TheirsAttr: &storagev1.InodeAttribute{Mode: 0o100644, Size: 4}}},
	}
	args := []string{"merge", diskID, "--from", "C", "--into", "B", "--endpoint", endpoint}
	code, out, errOut := runVersionCLI(args...)
	require.Equal(t, 1, code, errOut)
	for _, s := range []string{"data.txt", "内容冲突", "Base:", "Ours:", "Theirs:"} {
		require.Contains(t, out, s)
	}
	sizes := make(map[string]string)
	for _, line := range strings.Split(out, "\n") {
		label, value, ok := strings.Cut(line, ": ")
		if !ok || label != "Base" && label != "Ours" && label != "Theirs" {
			continue
		}
		var attr struct {
			Size string `json:"size"`
		}
		require.NoError(t, json.Unmarshal([]byte(value), &attr))
		sizes[label] = attr.Size
	}
	require.Equal(t, map[string]string{"Base": "2", "Ours": "3", "Theirs": "4"}, sizes)
	code, out, errOut = runVersionCLI(append(args, "--json")...)
	require.Equal(t, 1, code, errOut)
	var decoded storagev1.MergeResponse
	require.NoError(t, protojson.Unmarshal([]byte(out), &decoded))
	require.Equal(t, storagev1.MergeResult_MERGE_RESULT_CONFLICTS, decoded.Result)
	require.Len(t, decoded.Conflicts, 1)
	require.Equal(t, uint64(4), decoded.Conflicts[0].GetTheirsAttr().GetSize())
	result.Result = storagev1.MergeResult_MERGE_RESULT_MATERIALIZED
	code, out, errOut = runVersionCLI(append(args, "--strategy", "manual")...)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "data.txt")
	require.Contains(t, out, "--continue")
	require.Contains(t, out, "--abort")
	code, out, errOut = runVersionCLI(append(args, "--strategy", "manual", "--json")...)
	require.Equal(t, 0, code, errOut)
	require.NoError(t, protojson.Unmarshal([]byte(out), &decoded))
	require.Equal(t, storagev1.MergeResult_MERGE_RESULT_MATERIALIZED, decoded.Result)
	result.Result = storagev1.MergeResult_MERGE_RESULT_UNSPECIFIED
	code, _, errOut = runVersionCLI(args...)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "未知合并结果")
}

func TestMergeContinueAndAbort(t *testing.T) {
	diskID, commitID := uuid.NewString(), uuid.NewString()
	var resolveReq *storagev1.ResolveMergeRequest
	var abortReq *storagev1.AbortMergeRequest
	h := &versionHandler{
		resolve: func(req *connect.Request[storagev1.ResolveMergeRequest]) (*connect.Response[storagev1.ResolveMergeResponse], error) {
			resolveReq = req.Msg
			return connect.NewResponse(&storagev1.ResolveMergeResponse{Commit: &storagev1.CommitInfo{CommitId: commitID}}), nil
		},
		abort: func(req *connect.Request[storagev1.AbortMergeRequest]) (*connect.Response[storagev1.AbortMergeResponse], error) {
			abortReq = req.Msg
			return connect.NewResponse(&storagev1.AbortMergeResponse{}), nil
		},
	}
	endpoint := setupTestStorageServer(t, h)
	code, out, errOut := runVersionCLI("merge", diskID, "--into", "B", "--continue", "-m", "解决冲突", "--endpoint", endpoint)
	require.Equal(t, 0, code, errOut)
	require.Equal(t, diskID, resolveReq.DiskId)
	require.Equal(t, "B", resolveReq.BranchName)
	require.Equal(t, "解决冲突", resolveReq.Message)
	require.Contains(t, out, commitID)
	code, out, errOut = runVersionCLI("merge", diskID, "--into", "B", "--continue", "--message=版本", "--endpoint", endpoint, "--json")
	require.Equal(t, 0, code, errOut)
	var commit storagev1.CommitInfo
	require.NoError(t, protojson.Unmarshal([]byte(out), &commit))
	require.Equal(t, commitID, commit.CommitId)
	require.Equal(t, "版本", resolveReq.Message)
	code, out, errOut = runVersionCLI("merge", diskID, "--into", "B", "--abort", "--endpoint", endpoint, "--json")
	require.Equal(t, 0, code, errOut)
	require.Equal(t, diskID, abortReq.DiskId)
	require.Equal(t, "B", abortReq.BranchName)
	require.Contains(t, out, `"aborted": true`)
	h.abort = func(_ *connect.Request[storagev1.AbortMergeRequest]) (*connect.Response[storagev1.AbortMergeResponse], error) {
		return nil, connect.NewError(connect.CodeUnavailable, errors.New("服务不可用"))
	}
	code, _, errOut = runVersionCLI("merge", diskID, "--into", "B", "--abort", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "中止合并失败")
}

func TestMergeValidation(t *testing.T) {
	id := uuid.NewString()
	for _, args := range [][]string{
		{"merge"}, {"merge", "bad", "--from", "C", "--into", "B"},
		{"merge", id, "--from", "C"}, {"merge", id, "--into", "B"},
		{"merge", id, "--from", "C", "--into", "B", "--strategy", "bad"},
		{"merge", id, "--from", "C", "--into", "B", "--continue"},
		{"merge", id, "--into", "B", "--abort", "--continue"},
		{"merge", id, "--into", "B", "--continue", "--strategy="},
		{"merge", id, "--into", "B", "--abort", "--from", "C"},
		{"merge", id, "--into", "B", "--abort", "-m", "消息"},
		{"merge", id, "--into", "..", "--continue"},
	} {
		code, _, _ := runVersionCLI(args...)
		require.Equal(t, 2, code, "%v", args)
	}
}
