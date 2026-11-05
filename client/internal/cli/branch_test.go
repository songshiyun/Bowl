package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/cli"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type versionHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	create   func(*connect.Request[storagev1.CreateBranchRequest]) (*connect.Response[storagev1.CreateBranchResponse], error)
	list     func(*connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error)
	delete   func(*connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error)
	commit   func(*connect.Request[storagev1.CommitRequest]) (*connect.Response[storagev1.CommitResponse], error)
	rollback func(*connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error)
	merge    func(*connect.Request[storagev1.MergeRequest]) (*connect.Response[storagev1.MergeResponse], error)
	resolve  func(*connect.Request[storagev1.ResolveMergeRequest]) (*connect.Response[storagev1.ResolveMergeResponse], error)
	abort    func(*connect.Request[storagev1.AbortMergeRequest]) (*connect.Response[storagev1.AbortMergeResponse], error)
}

func (h *versionHandler) CreateBranch(_ context.Context, r *connect.Request[storagev1.CreateBranchRequest]) (*connect.Response[storagev1.CreateBranchResponse], error) {
	return h.create(r)
}
func (h *versionHandler) ListBranches(_ context.Context, r *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
	return h.list(r)
}
func (h *versionHandler) DeleteBranch(_ context.Context, r *connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error) {
	return h.delete(r)
}
func (h *versionHandler) Commit(_ context.Context, r *connect.Request[storagev1.CommitRequest]) (*connect.Response[storagev1.CommitResponse], error) {
	return h.commit(r)
}
func (h *versionHandler) Rollback(_ context.Context, r *connect.Request[storagev1.RollbackRequest]) (*connect.Response[storagev1.RollbackResponse], error) {
	return h.rollback(r)
}
func (h *versionHandler) Merge(_ context.Context, r *connect.Request[storagev1.MergeRequest]) (*connect.Response[storagev1.MergeResponse], error) {
	return h.merge(r)
}
func (h *versionHandler) ResolveMerge(_ context.Context, r *connect.Request[storagev1.ResolveMergeRequest]) (*connect.Response[storagev1.ResolveMergeResponse], error) {
	return h.resolve(r)
}
func (h *versionHandler) AbortMerge(_ context.Context, r *connect.Request[storagev1.AbortMergeRequest]) (*connect.Response[storagev1.AbortMergeResponse], error) {
	return h.abort(r)
}

func runVersionCLI(args ...string) (int, string, string) {
	var out, errOut bytes.Buffer
	code := cli.Execute(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestBranchCreateSources(t *testing.T) {
	diskID, commitID, branchID := uuid.NewString(), uuid.NewString(), uuid.NewString()
	var captured *storagev1.CreateBranchRequest
	var auth string
	h := &versionHandler{create: func(req *connect.Request[storagev1.CreateBranchRequest]) (*connect.Response[storagev1.CreateBranchResponse], error) {
		captured, auth = req.Msg, req.Header().Get("Authorization")
		return connect.NewResponse(&storagev1.CreateBranchResponse{Branch: &storagev1.Branch{
			BranchId: branchID, DiskId: diskID, Name: req.Msg.Name, ParentBranch: req.Msg.SourceBranch,
			ForkCommitId: req.Msg.SourceCommitId, HeadCommitId: &commitID, State: storagev1.BranchState_BRANCH_STATE_NORMAL,
			CreatedAt: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)),
		}}), nil
	}}
	endpoint := setupTestStorageServer(t, h)
	tests := []struct {
		name, branch, commit string
		flags                []string
	}{
		{name: "默认来源"},
		{name: "指定分支", flags: []string{"--from", "main"}, branch: "main"},
		{name: "UUID 分支名", flags: []string{"--from", commitID}, branch: commitID},
		{name: "分支与快照", flags: []string{"--from=main@" + commitID}, branch: "main", commit: commitID},
		{name: "仅快照", flags: []string{"--from", "@" + commitID}, commit: commitID},
		{name: "独立快照选项", flags: []string{"--from", "main", "--commit", commitID}, branch: "main", commit: commitID},
		{name: "仅独立快照选项", flags: []string{"--commit", commitID}, commit: commitID},
		{name: "非标准 UUID 输入", flags: []string{"--commit", strings.ReplaceAll(commitID, "-", "")}, commit: commitID},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			args := []string{"branch", "create", diskID, "--name", "B", "--endpoint", endpoint, "--token", "secret", "--json"}
			args = append(args, tc.flags...)
			code, out, errOut := runVersionCLI(args...)
			require.Equal(t, 0, code, errOut)
			require.Equal(t, "Bearer secret", auth)
			require.Equal(t, diskID, captured.DiskId)
			require.Equal(t, "B", captured.Name)
			if tc.branch == "" {
				require.Nil(t, captured.SourceBranch)
			} else {
				require.Equal(t, tc.branch, captured.GetSourceBranch())
			}
			if tc.commit == "" {
				require.Nil(t, captured.SourceCommitId)
			} else {
				require.Equal(t, tc.commit, captured.GetSourceCommitId())
			}
			var result map[string]any
			require.NoError(t, json.Unmarshal([]byte(out), &result))
			require.Equal(t, branchID, result["branch_id"])
			require.Equal(t, "NORMAL", result["state"])
			require.Equal(t, commitID, result["head_commit_id"])
		})
	}
	code, out, errOut := runVersionCLI("branch", "create", diskID, "--name", "B", "--from", "main@"+commitID, "--endpoint", endpoint)
	require.Equal(t, 0, code, errOut)
	for _, s := range []string{"Branch ID:", "Name:", "Parent Branch:", "Fork Commit:", "Head Commit:", "State:", branchID, commitID} {
		require.Contains(t, out, s)
	}
	code, _, errOut = runVersionCLI("branch", "create", diskID, "--name", "-feature", "--endpoint", endpoint)
	require.Equal(t, 0, code, errOut)
	require.Equal(t, "-feature", captured.Name)
	code, _, errOut = runVersionCLI("branch", "create", diskID, "--name", strings.Repeat("a", 255), "--endpoint", endpoint)
	require.Equal(t, 0, code, errOut)
	require.Len(t, captured.Name, 255)
}

func TestBranchListAndDelete(t *testing.T) {
	diskID, head := uuid.NewString(), uuid.NewString()
	parent := "main"
	h := &versionHandler{}
	h.list = func(req *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
		require.Equal(t, diskID, req.Msg.DiskId)
		return connect.NewResponse(&storagev1.ListBranchesResponse{Branches: []*storagev1.Branch{{Name: "B", ParentBranch: &parent, HeadCommitId: &head, State: storagev1.BranchState_BRANCH_STATE_MERGING, CreatedAt: timestamppb.New(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))}}}), nil
	}
	h.delete = func(req *connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error) {
		require.Equal(t, diskID, req.Msg.DiskId)
		require.Equal(t, "B", req.Msg.Name)
		return connect.NewResponse(&storagev1.DeleteBranchResponse{}), nil
	}
	endpoint := setupTestStorageServer(t, h)
	for _, alias := range []string{"ls", "list"} {
		code, out, errOut := runVersionCLI("branch", alias, diskID, "--endpoint", endpoint)
		require.Equal(t, 0, code, errOut)
		for _, s := range []string{"NAME", "PARENT", "HEAD COMMIT", "STATE", "CREATED AT", "main", head, "MERGING", "2026-01-01T00:00:00Z"} {
			require.Contains(t, out, s)
		}
		code, out, errOut = runVersionCLI("branch", alias, diskID, "--endpoint", endpoint, "--json")
		require.Equal(t, 0, code, errOut)
		var result struct {
			Branches []struct {
				Name   string `json:"name"`
				Parent string `json:"parent_branch"`
			} `json:"branches"`
		}
		require.NoError(t, json.Unmarshal([]byte(out), &result))
		require.Len(t, result.Branches, 1)
		require.Equal(t, "B", result.Branches[0].Name)
		require.Equal(t, "main", result.Branches[0].Parent)
	}
	for _, alias := range []string{"rm", "delete"} {
		code, out, errOut := runVersionCLI("branch", alias, diskID, "--name", "B", "--endpoint", endpoint, "--json")
		require.Equal(t, 0, code, errOut)
		require.Contains(t, out, `"deleted": true`)
	}
	h.delete = func(_ *connect.Request[storagev1.DeleteBranchRequest]) (*connect.Response[storagev1.DeleteBranchResponse], error) {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("busy"))
	}
	code, _, errOut := runVersionCLI("branch", "rm", diskID, "--name", "B", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "EBUSY")
	require.Contains(t, errOut, "活跃会话")
	h.list = func(_ *connect.Request[storagev1.ListBranchesRequest]) (*connect.Response[storagev1.ListBranchesResponse], error) {
		return connect.NewResponse(&storagev1.ListBranchesResponse{}), nil
	}
	code, out, errOut := runVersionCLI("branch", "ls", diskID, "--endpoint", endpoint, "--json")
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, `"branches": []`)
}

func TestBranchValidation(t *testing.T) {
	id := uuid.NewString()
	badNames := []string{"", ".", "..", "a/b", "a\x00b", "a\nb", "a\x7fb", "a\u0085b", strings.Repeat("a", 256)}
	for _, name := range badNames {
		for _, sub := range []string{"create", "rm"} {
			code, _, _ := runVersionCLI("branch", sub, id, "--name="+name)
			require.Equal(t, 2, code, "%s %q", sub, name)
		}
	}
	for _, args := range [][]string{
		{"branch"}, {"branch", "unknown"}, {"branch", "ls"}, {"branch", "ls", uuid.Nil.String()},
		{"branch", "create", "bad", "--name", "B"}, {"branch", "create", id},
		{"branch", "create", id, "--name", "B", "--from="},
		{"branch", "create", id, "--name", "B", "--from=@" + uuid.Nil.String()},
		{"branch", "create", id, "--name", "B", "--from=..@" + id},
		{"branch", "create", id, "--name", "B", "--from=main@" + id + "@" + id},
		{"branch", "create", id, "--name", "B", "--from=main@" + id, "--commit", id},
		{"branch", "create", id, "--name", "B", "--commit", "invalid"},
		{"branch", "create", id, "--name", "B", "--from", "--json"},
		{"branch", "ls", id, "--json=true"},
	} {
		code, _, _ := runVersionCLI(args...)
		require.Equal(t, 2, code, "%v", args)
	}
}
