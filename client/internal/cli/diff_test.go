package cli_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/cli"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type diffHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mu         sync.Mutex
	entries    []*storagev1.DiffEntry
	contents   map[string][]byte
	attrs      map[string]*storagev1.InodeAttribute
	sessions   map[uint64]string
	reads      []*storagev1.ReadRequest
	closed     []uint64
	request    *storagev1.DiffRequest
	renewals   []*storagev1.RenewSessionRequest
	commitOnly string
}

func (h *diffHandler) Diff(_ context.Context, req *connect.Request[storagev1.DiffRequest]) (*connect.Response[storagev1.DiffResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.request = req.Msg
	return connect.NewResponse(&storagev1.DiffResponse{Entries: h.entries}), nil
}

func (h *diffHandler) RenewSession(_ context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.renewals = append(h.renewals, req.Msg)
	ref := req.Msg.GetBranchName()
	if req.Msg.CommitId == nil && ref == h.commitOnly {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("分支不存在"))
	}
	if req.Msg.CommitId != nil {
		ref = req.Msg.GetCommitId()
	}
	if _, ok := h.contents[ref]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("版本不存在"))
	}
	if !req.Msg.ReadOnly || req.Msg.GetDiskId() == "" || req.Msg.SessionId == 0 {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("会话参数错误"))
	}
	if h.sessions == nil {
		h.sessions = make(map[uint64]string)
	}
	h.sessions[req.Msg.SessionId] = ref
	return connect.NewResponse(&storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}), nil
}

func (h *diffHandler) Lookup(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	ref := h.sessions[req.Msg.GetSessionId()]
	if ref == "" || string(req.Msg.Name) != "file" {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("文件不存在"))
	}
	return connect.NewResponse(&storagev1.LookupResponse{Inode: &storagev1.Inode{Ino: 2, Attr: h.attrs[ref]}}), nil
}

func (h *diffHandler) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.reads = append(h.reads, req.Msg)
	content := h.contents[h.sessions[req.Msg.GetSessionId()]]
	start := min(req.Msg.Offset, uint64(len(content)))
	end := min(start+uint64(req.Msg.Size), uint64(len(content)))
	return connect.NewResponse(&storagev1.ReadResponse{Data: content[start:end]}), nil
}

func (h *diffHandler) CloseSession(_ context.Context, req *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.closed = append(h.closed, req.Msg.SessionId)
	return connect.NewResponse(&storagev1.CloseSessionResponse{}), nil
}

func diffTestHandler(old, newData []byte) *diffHandler {
	oldAttr := &storagev1.InodeAttribute{Mode: 0o100644, Size: uint64(len(old))}
	newAttr := &storagev1.InodeAttribute{Mode: 0o100644, Size: uint64(len(newData))}
	return &diffHandler{
		entries:  []*storagev1.DiffEntry{{ChangeType: storagev1.DiffChangeType_DIFF_CHANGE_TYPE_MODIFIED, Path: "file", OldAttr: oldAttr, NewAttr: newAttr, ModifiedBlockIndices: []uint64{0}}},
		contents: map[string][]byte{"old": old, "new": newData},
		attrs:    map[string]*storagev1.InodeAttribute{"old": oldAttr, "new": newAttr},
	}
}

func runDiff(t *testing.T, h *diffHandler, flags ...string) (int, string, string) {
	t.Helper()
	endpoint := setupTestStorageServer(t, h)
	args := []string{"diff", uuid.NewString(), "--from", "old", "--to", "new", "--endpoint", endpoint}
	args = append(args, flags...)
	var out, errOut bytes.Buffer
	code := cli.Execute(args, &out, &errOut)
	return code, out.String(), errOut.String()
}

func TestDiffTextAndSessions(t *testing.T) {
	h := diffTestHandler([]byte("one\nold\nthree\n"), []byte("one\nnew\nthree\n"))
	code, out, errOut := runDiff(t, h)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "--- a/file\n+++ b/file\n@@ -1,3 +1,3 @@\n one\n-old\n+new\n three\n")
	require.Len(t, h.reads, 2)
	require.Len(t, h.closed, 2)
	require.Equal(t, "old", h.request.FromRef)
	require.Equal(t, "new", h.request.ToRef)
}

func TestDiffPatchApplies(t *testing.T) {
	for _, tc := range []struct{ old, new string }{
		{"a\nb\nc\n", "a\nx\nc\n"},
		{"a\n1\n2\n3\n4\n5\n6\n7\n8\n9\nz\n", "b\n1\n2\n3\n4\n5\n6\n7\n8\n9\ny\n"},
		{"first\nlast", "first\nchanged"},
		{"one\ntwo\nthree\n", "three\none\nfour\n"},
	} {
		t.Run(tc.old+" → "+tc.new, func(t *testing.T) {
			h := diffTestHandler([]byte(tc.old), []byte(tc.new))
			code, out, errOut := runDiff(t, h)
			require.Equal(t, 0, code, errOut)
			start := strings.Index(out, "--- a/file\n")
			require.NotEqual(t, -1, start)
			dir := t.TempDir()
			file := filepath.Join(dir, "file")
			require.NoError(t, os.WriteFile(file, []byte(tc.old), 0o644))
			cmd := exec.Command("git", "apply", "-")
			cmd.Dir = dir
			cmd.Stdin = strings.NewReader(out[start:])
			output, err := cmd.CombinedOutput()
			require.NoError(t, err, string(output))
			applied, err := os.ReadFile(file)
			require.NoError(t, err)
			require.Equal(t, tc.new, string(applied))
		})
	}
}

func TestDiffBinaryOnlySniffs(t *testing.T) {
	old := append([]byte("bin\x00"), bytes.Repeat([]byte("a"), 9000)...)
	newData := bytes.Repeat([]byte("b"), 9000)
	h := diffTestHandler(old, newData)
	code, out, errOut := runDiff(t, h)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "Binary files a/file and b/file differ")
	for _, read := range h.reads {
		require.Zero(t, read.Offset)
		require.Equal(t, uint32(8192), read.Size)
	}
	require.Len(t, h.reads, 2)
}

func TestDiffBinaryAfterSniff(t *testing.T) {
	old := append(bytes.Repeat([]byte("a"), 8192), 0)
	newData := append(bytes.Repeat([]byte("b"), 8192), 0)
	h := diffTestHandler(old, newData)
	code, out, errOut := runDiff(t, h)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "Binary files a/file and b/file differ")
	require.NotContains(t, out, "--- a/file")
}

func TestDiffTextReadsRemainder(t *testing.T) {
	old := append(bytes.Repeat([]byte("a"), 9000), '\n')
	newData := append(bytes.Repeat([]byte("b"), 9000), '\n')
	h := diffTestHandler(old, newData)
	code, out, errOut := runDiff(t, h)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "@@ -1,1 +1,1 @@")
	require.Len(t, h.reads, 4)
	require.Equal(t, uint32(8192), h.reads[0].Size)
	require.Equal(t, uint32(8192), h.reads[1].Size)
	require.Equal(t, uint64(8192), h.reads[2].Offset)
	require.Equal(t, uint64(8192), h.reads[3].Offset)
}

func TestDiffReportsStaleSnapshot(t *testing.T) {
	h := diffTestHandler([]byte("old"), []byte("new"))
	h.attrs["new"] = &storagev1.InodeAttribute{Mode: 0o100644, Size: 8}
	code, out, errOut := runDiff(t, h)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "只读快照与分支工作区不一致")
	require.NotContains(t, out, "+++ b/file")
	require.Len(t, h.reads, 1)
	require.Len(t, h.closed, 2)
}

func TestDiffUUIDCommitFallback(t *testing.T) {
	h := diffTestHandler([]byte("old\n"), []byte("new\n"))
	branch := uuid.NewString()
	h.contents[branch] = h.contents["old"]
	h.attrs[branch] = h.attrs["old"]
	h.commitOnly = branch
	code, out, errOut := runDiff(t, h, "--from", branch)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "-old\n+new")
	require.GreaterOrEqual(t, len(h.renewals), 3)
	require.Equal(t, branch, h.renewals[0].GetBranchName())
	require.Equal(t, branch, h.renewals[1].GetCommitId())
}

func TestDiffUUIDBranchTakesPrecedence(t *testing.T) {
	h := diffTestHandler([]byte("old\n"), []byte("new\n"))
	branch := uuid.NewString()
	h.contents[branch] = h.contents["old"]
	h.attrs[branch] = h.attrs["old"]
	code, out, errOut := runDiff(t, h, "--from", branch)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "-old\n+new")
	require.Equal(t, branch, h.renewals[0].GetBranchName())
	require.Nil(t, h.renewals[0].CommitId)
}

func TestDiffSummaryAndJSONDoNotRead(t *testing.T) {
	h := diffTestHandler([]byte("old"), []byte("new"))
	h.entries[0].NewAttr.Size = 1<<20 + 1
	code, out, errOut := runDiff(t, h)
	require.Equal(t, 0, code, errOut)
	require.Contains(t, out, "MODIFIED: size 3 -> 1048577")
	require.Contains(t, out, "modified_blocks: [0]")
	require.Empty(t, h.reads)
	require.Empty(t, h.sessions)
	h = diffTestHandler([]byte("old"), []byte("new"))
	code, out, errOut = runDiff(t, h, "--json")
	require.Equal(t, 0, code, errOut)
	var decoded storagev1.DiffResponse
	require.NoError(t, protojson.Unmarshal([]byte(out), &decoded))
	require.Len(t, decoded.Entries, 1)
	require.Equal(t, storagev1.DiffChangeType_DIFF_CHANGE_TYPE_MODIFIED, decoded.Entries[0].ChangeType)
	require.Equal(t, "file", decoded.Entries[0].Path)
	require.Equal(t, []uint64{0}, decoded.Entries[0].ModifiedBlockIndices)
	require.Empty(t, h.sessions)
}

func TestDiffAddedDeletedAndMissingNewline(t *testing.T) {
	for _, added := range []bool{true, false} {
		old, newData := []byte("line"), []byte(nil)
		if added {
			old, newData = nil, []byte("line")
		}
		h := diffTestHandler(old, newData)
		if added {
			h.entries[0].ChangeType = storagev1.DiffChangeType_DIFF_CHANGE_TYPE_ADDED
			h.entries[0].OldAttr = nil
		} else {
			h.entries[0].ChangeType = storagev1.DiffChangeType_DIFF_CHANGE_TYPE_DELETED
			h.entries[0].NewAttr = nil
		}
		code, out, errOut := runDiff(t, h)
		require.Equal(t, 0, code, errOut)
		require.Contains(t, out, "\\ No newline at end of file")
		if added {
			require.Contains(t, out, "--- /dev/null\n+++ b/file\n@@ -0,0 +1,1 @@\n+line")
		} else {
			require.Contains(t, out, "--- a/file\n+++ /dev/null\n@@ -1,1 +0,0 @@\n-line")
		}
		require.Len(t, h.closed, 1)
	}
}

func TestDiffRejectsInvalidArgs(t *testing.T) {
	for _, args := range [][]string{{"diff"}, {"diff", "bad"}, {"diff", uuid.Nil.String()}, {"diff", uuid.NewString(), "--from"}, {"diff", uuid.NewString(), "--from", ".."}, {"diff", uuid.NewString(), "--unknown"}} {
		var out, errOut bytes.Buffer
		require.Equal(t, 2, cli.Execute(args, &out, &errOut), strings.Join(args, " "))
	}
}
