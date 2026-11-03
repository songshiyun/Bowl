package cli_test

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/starsrc/bowl/client/internal/cli"
	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type cliMockStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	getDiskFn      func(context.Context, *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func TestMount_BranchAndCommit(t *testing.T) {
	diskID := uuid.NewString()
	commitID := uuid.NewString()
	var getDiskCalls atomic.Int32
	server := &cliMockStorageHandler{getDiskFn: func(_ context.Context, _ *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
		getDiskCalls.Add(1)
		return connect.NewResponse(&storagev1.GetDiskResponse{Disk: &storagev1.Disk{
			DiskId: diskID, RootIno: 1001, Branches: []*storagev1.Branch{{Name: "main"}},
		}}), nil
	}}
	endpoint := setupTestStorageServer(t, server)
	mountpoint := t.TempDir()
	tests := []struct {
		name           string
		flags          []string
		code           int
		branch, commit string
		ro             bool
		message        string
	}{
		{name: "默认分支", branch: "main"},
		{name: "指定分支", flags: []string{"--branch=feature-x"}, branch: "feature-x"},
		{name: "只读分支", flags: []string{"--ro", "--branch", "main"}, branch: "main", ro: true},
		{name: "提交快照", flags: []string{"--ro", "--commit", commitID}, commit: commitID, ro: true},
		{name: "分支与提交互斥", flags: []string{"--branch", "main", "--commit", commitID, "--ro"}, code: 2, message: "mutually exclusive"},
		{name: "提交需要只读", flags: []string{"--commit", commitID}, code: 2, message: "requires --ro"},
		{name: "无效提交", flags: []string{"--ro", "--commit", "invalid"}, code: 2, message: "UUID"},
		{name: "空提交 UUID", flags: []string{"--ro", "--commit", uuid.Nil.String()}, code: 2, message: "UUID"},
		{name: "单点分支", flags: []string{"--branch", "."}, code: 2},
		{name: "双点分支", flags: []string{"--branch", ".."}, code: 2},
		{name: "斜杠分支", flags: []string{"--branch", "a/b"}, code: 2},
		{name: "空字节分支", flags: []string{"--branch", "a\x00b"}, code: 2},
		{name: "换行分支", flags: []string{"--branch", "a\nb"}, code: 2},
		{name: "控制字符分支", flags: []string{"--branch", "a\x7f"}, code: 2},
		{name: "过长分支", flags: []string{"--branch", strings.Repeat("x", 256)}, code: 2},
		{name: "最大长度分支", flags: []string{"--branch", strings.Repeat("x", 255)}, branch: strings.Repeat("x", 255)},
		{name: "缺少分支值", flags: []string{"--branch"}, code: 2},
		{name: "缺少提交值", flags: []string{"--commit"}, code: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			cli.SetMountRunnerForTest(t, func(_ context.Context, _ storagev1connect.StorageServiceClient, cfg fusefs.Config, _ string, _, _ io.Writer) error {
				called = true
				require.Equal(t, uint64(1), cfg.RootIno)
				require.Equal(t, tc.branch, cfg.BranchName)
				require.Equal(t, tc.commit, cfg.CommitID)
				require.Equal(t, tc.ro, cfg.ReadOnly)
				return nil
			})
			before := getDiskCalls.Load()
			args := []string{"mount", "-f", "--endpoint", endpoint, diskID, mountpoint}
			args = append(args, tc.flags...)
			var stdout, stderr bytes.Buffer
			require.Equal(t, tc.code, cli.Execute(args, &stdout, &stderr), stderr.String())
			if tc.code == 2 {
				require.Equal(t, before, getDiskCalls.Load())
				require.False(t, called)
				require.Contains(t, stderr.String(), tc.message)
			} else {
				require.True(t, called)
			}
		})
	}
}

func (h *cliMockStorageHandler) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	if h.getDiskFn != nil {
		return h.getDiskFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
}

func (h *cliMockStorageHandler) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if h.renewSessionFn != nil {
		return h.renewSessionFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnavailable, os.ErrNotExist)
}

func setupTestStorageServer(t *testing.T, handler storagev1connect.StorageServiceHandler) string {
	mux := http.NewServeMux()
	path, h := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, h)

	h2cServer := &http2.Server{}
	ts := httptest.NewServer(h2c.NewHandler(mux, h2cServer))
	t.Cleanup(ts.Close)
	return ts.URL
}

func TestMount_ArgValidation(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	t.Run("missing positional arguments returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount"}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")

		stderr.Reset()
		code = cli.Execute([]string{"mount", validUUID}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "用法:")
	})

	t.Run("invalid or nil uuid returns exit code 2", func(t *testing.T) {
		testCases := []string{
			"not-a-uuid",
			"12345",
			"00000000-0000-0000-0000-000000000000",
		}
		for _, invalidID := range testCases {
			var stdout, stderr bytes.Buffer
			code := cli.Execute([]string{"mount", invalidID, tmpDir}, &stdout, &stderr)
			require.Equal(t, 2, code)
			require.Contains(t, stderr.String(), "UUID")
		}
	})

	t.Run("nonexistent mountpoint returns exit code 2", func(t *testing.T) {
		var stdout, stderr bytes.Buffer
		nonExistent := filepath.Join(tmpDir, "does-not-exist")
		code := cli.Execute([]string{"mount", validUUID, nonExistent}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "挂载点")
	})

	t.Run("mountpoint is a file not dir returns exit code 2", func(t *testing.T) {
		testFile := filepath.Join(tmpDir, "somefile.txt")
		require.NoError(t, os.WriteFile(testFile, []byte("hello"), 0o644))

		var stdout, stderr bytes.Buffer
		code := cli.Execute([]string{"mount", validUUID, testFile}, &stdout, &stderr)
		require.Equal(t, 2, code)
		require.Contains(t, stderr.String(), "目录")
	})
}

func TestMount_FlagPositioning(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	// 模拟远程不存在的 Disk
	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, os.ErrNotExist)
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	// 测试 flags 出现在位置参数之前和之后均能被正确解析到 endpoint
	cases := [][]string{
		{"mount", "-f", "--endpoint", endpoint, validUUID, tmpDir},
		{"mount", validUUID, tmpDir, "-f", "--endpoint", endpoint},
		{"mount", validUUID, "-f", tmpDir, "--endpoint", endpoint},
	}

	for _, args := range cases {
		var stdout, stderr bytes.Buffer
		code := cli.Execute(args, &stdout, &stderr)
		// 因为到达了 GetDisk 且 Disk 不存在，所以返回退出码 1（说明 flag 解析成功并连接到了 endpoint）
		require.Equal(t, 1, code, "args: %v", args)
		require.Contains(t, stderr.String(), "磁盘不存在")
	}
}

func TestMount_DefaultDaemonDelegation(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:   validUUID,
					RootIno:  1001,
					MaxBytes: 1024 * 1024,
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var launcherCalled bool
	cli.SetProcessLauncherForTest(t, func(cmd *exec.Cmd) error {
		launcherCalled = true
		require.Contains(t, cmd.Args, "mount")
		require.Contains(t, cmd.Args, validUUID)
		w := cmd.ExtraFiles[0]
		_, _ = w.WriteString("OK\n")
		return nil
	})

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"mount", "--endpoint", endpoint, validUUID, tmpDir}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.True(t, launcherCalled)
}

func TestMount_ForegroundRunnerSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:   validUUID,
					RootIno:  1001,
					MaxBytes: 1024 * 1024,
				},
			}), nil
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var runnerCalled bool
	cli.SetMountRunnerForTest(t, func(ctx context.Context, client storagev1connect.StorageServiceClient, cfg fusefs.Config, mountpoint string, stdout, stderr io.Writer) error {
		runnerCalled = true
		require.Equal(t, validUUID, cfg.DiskID)
		require.Equal(t, uint64(1001), cfg.RootIno)
		require.Equal(t, tmpDir, mountpoint)
		return nil
	})

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"mount", "-f", "--endpoint", endpoint, validUUID, tmpDir}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.True(t, runnerCalled)
}

func TestMount_ForegroundRunner_PropagatesDiskIDToRenewSession(t *testing.T) {
	tmpDir := t.TempDir()
	validUUID := uuid.New().String()

	var capturedDiskID string
	mockSrv := &cliMockStorageHandler{
		getDiskFn: func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:   validUUID,
					RootIno:  1001,
					MaxBytes: 1024 * 1024,
				},
			}), nil
		},
		renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			capturedDiskID = req.Msg.GetDiskId()
			return nil, connect.NewError(connect.CodeUnavailable, os.ErrPermission)
		},
	}
	endpoint := setupTestStorageServer(t, mockSrv)

	var stdout, stderr bytes.Buffer
	code := cli.Execute([]string{"mount", "-f", "--endpoint", endpoint, validUUID, tmpDir}, &stdout, &stderr)
	require.Equal(t, 1, code)
	require.Equal(t, validUUID, capturedDiskID, "runForegroundMount 调用 session.New 时必须透传 cfg.DiskID")
}

func TestMount_前台会话传递分支和提交(t *testing.T) {
	diskID, commitID := uuid.NewString(), uuid.NewString()
	for _, tc := range []struct {
		name, branch, commit string
		ro                   bool
	}{
		{name: "默认分支", branch: "main"},
		{name: "只读分支", branch: "feature-x", ro: true},
		{name: "提交快照", commit: commitID, ro: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var captured *storagev1.RenewSessionRequest
			server := &cliMockStorageHandler{
				getDiskFn: func(context.Context, *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
					return connect.NewResponse(&storagev1.GetDiskResponse{Disk: &storagev1.Disk{DiskId: diskID, RootIno: 42, Branches: []*storagev1.Branch{{Name: "main"}}}}), nil
				},
				renewSessionFn: func(_ context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
					captured = req.Msg
					return nil, connect.NewError(connect.CodePermissionDenied, os.ErrPermission)
				},
			}
			args := []string{"mount", "-f", "--endpoint", setupTestStorageServer(t, server), diskID, t.TempDir()}
			if tc.branch != "" && tc.branch != "main" {
				args = append(args, "--branch", tc.branch)
			}
			if tc.commit != "" {
				args = append(args, "--commit", tc.commit)
			}
			if tc.ro {
				args = append(args, "--ro")
			}
			var stdout, stderr bytes.Buffer
			require.Equal(t, 1, cli.Execute(args, &stdout, &stderr))
			require.NotNil(t, captured)
			require.Equal(t, diskID, captured.GetDiskId())
			require.Equal(t, tc.branch, captured.GetBranchName())
			require.Equal(t, tc.commit, captured.GetCommitId())
			require.Equal(t, tc.ro, captured.GetReadOnly())
		})
	}
}
