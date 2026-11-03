package server_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

func TestRenewSessionWithBranch(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	deadline := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	branch := "dev"
	disk := "11111111-1111-7111-8111-111111111111"
	mgr.EXPECT().RenewBranchSession(mock.Anything, meta.RenewBranchSessionParams{
		SessionID: 1, DiskID: disk, BranchName: &branch,
	}).Return(meta.RenewSessionResult{
		ExpiresAt: deadline, SessionRecreated: true,
	}, nil).Once()
	resp, err := client.RenewSession(context.Background(), connect.NewRequest(&storagev1.RenewSessionRequest{
		SessionId: 1, DiskId: &disk, BranchName: &branch,
	}))
	require.NoError(t, err)
	require.True(t, resp.Msg.SessionRecreated)
	require.Equal(t, deadline, resp.Msg.ExpiresAt.AsTime())
}

func TestRenewSessionSnapshotAndBusy(t *testing.T) {
	disk := "11111111-1111-7111-8111-111111111111"
	commit := "22222222-2222-7222-8222-222222222222"
	deadline := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	for _, explicitCommit := range []bool{false, true} {
		t.Run(fmt.Sprintf("指定快照=%t", explicitCommit), func(t *testing.T) {
			mgr := metamock.NewMockManager(t)
			client, cleanup := setupTestServer(t, mgr)
			defer cleanup()
			var commitID *string
			if explicitCommit {
				commitID = &commit
			}
			mgr.EXPECT().RenewBranchSession(mock.Anything, meta.RenewBranchSessionParams{
				SessionID: 2, DiskID: disk, ReadOnly: true, CommitID: commitID,
			}).Return(meta.RenewSessionResult{ExpiresAt: deadline}, nil).Once()
			response, err := client.RenewSession(context.Background(), connect.NewRequest(&storagev1.RenewSessionRequest{
				SessionId: 2, DiskId: &disk, ReadOnly: true, CommitId: commitID,
			}))
			require.NoError(t, err)
			require.Equal(t, deadline, response.Msg.ExpiresAt.AsTime())
		})
	}
	t.Run("写端冲突", func(t *testing.T) {
		mgr := metamock.NewMockManager(t)
		client, cleanup := setupTestServer(t, mgr)
		defer cleanup()
		branch := "main"
		mgr.EXPECT().RenewBranchSession(mock.Anything, meta.RenewBranchSessionParams{
			SessionID: 4, DiskID: disk, BranchName: &branch,
		}).Return(meta.RenewSessionResult{}, meta.ErrBusy).Once()
		_, err := client.RenewSession(context.Background(), connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: 4, DiskId: &disk, BranchName: &branch,
		}))
		require.Error(t, err)
		require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
		info := server.ExtractErrorInfo(err)
		require.NotNil(t, info)
		require.Equal(t, "EBUSY", info.GetReason())
	})
	_, client, cleanup := setupTopologyXattrTestServer(t)
	defer cleanup()
	_, err := client.RenewSession(context.Background(), connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: 3, ReadOnly: true}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestLocalOpenStateIsNotIgnored(t *testing.T) {
	mockMgr, client, cleanup := setupTopologyXattrTestServer(t)
	defer cleanup()
	mockMgr.EXPECT().Unlink(mock.Anything, uint64(1), []byte("file")).Return(nil).Maybe()
	_, err := client.Unlink(context.Background(), connect.NewRequest(&storagev1.UnlinkRequest{
		ParentIno: 1, Name: []byte("file"), OpenInClient: proto.Bool(true),
	}))
	require.Error(t, err)
	require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
	mockMgr.AssertNotCalled(t, "Unlink", mock.Anything, uint64(1), []byte("file"))
}

func TestNewVersionRPCsAreValidatedAndUnimplemented(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mockMgr)
	defer cleanup()
	ctx := context.Background()
	const disk = "11111111-1111-7111-8111-111111111111"
	const commit = "22222222-2222-7222-8222-222222222222"
	calls := map[string]func() error{
		"CreateBranch": func() error {
			_, err := client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: disk, Name: "dev"}))
			return err
		},
		"ListBranches": func() error {
			_, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: disk}))
			return err
		},
		"DeleteBranch": func() error {
			_, err := client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{DiskId: disk, Name: "dev"}))
			return err
		},
		"Commit": func() error {
			_, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: disk, BranchName: "main"}))
			return err
		},
		"Rollback": func() error {
			_, err := client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: disk, BranchName: "main", CommitId: commit}))
			return err
		},
		"Merge": func() error {
			_, err := client.Merge(ctx, connect.NewRequest(&storagev1.MergeRequest{DiskId: disk, SourceBranch: "dev", TargetBranch: "main"}))
			return err
		},
		"ResolveMerge": func() error {
			_, err := client.ResolveMerge(ctx, connect.NewRequest(&storagev1.ResolveMergeRequest{DiskId: disk, BranchName: "main"}))
			return err
		},
		"AbortMerge": func() error {
			_, err := client.AbortMerge(ctx, connect.NewRequest(&storagev1.AbortMergeRequest{DiskId: disk, BranchName: "main"}))
			return err
		},
		"Diff": func() error {
			_, err := client.Diff(ctx, connect.NewRequest(&storagev1.DiffRequest{DiskId: disk, FromRef: "main", ToRef: "dev"}))
			return err
		},
		"ReclaimOrphan": func() error {
			_, err := client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{SessionId: 1, DiskId: disk, Ino: 1}))
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.Error(t, err)
			require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
		})
	}
	_, err := client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: "bad", Name: "dev"}))
	require.Error(t, err)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestEvolvedRequestsDoNotUseLegacySemantics(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mockMgr)
	defer cleanup()
	ctx := context.Background()
	calls := map[string]func() error{
		"按会话创建节点": func() error {
			_, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{ParentIno: 1, Name: []byte("x"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话创建目录": func() error {
			_, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{ParentIno: 1, Name: []byte("x"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话创建软链接": func() error {
			_, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{ParentIno: 1, Name: []byte("x"), Target: []byte("y"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话创建硬链接": func() error {
			_, err := client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{Ino: 1, NewParentIno: 1, NewName: []byte("x"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话删除目录": func() error {
			_, err := client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{ParentIno: 1, Name: []byte("x"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话重命名": func() error {
			_, err := client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: []byte("new"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话设置扩展属性": func() error {
			_, err := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{Ino: 1, Name: []byte("user.test"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话读取扩展属性": func() error {
			_, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{Ino: 1, Name: []byte("user.test"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话列出扩展属性": func() error {
			_, err := client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{Ino: 1, SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话删除扩展属性": func() error {
			_, err := client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{Ino: 1, Name: []byte("user.test"), SessionId: proto.Uint64(1)}))
			return err
		},
		"按会话删除": func() error {
			_, err := client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: 1, Name: []byte("file"), SessionId: proto.Uint64(1)}))
			return err
		},
		"本地文件未打开": func() error {
			_, err := client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: 1, Name: []byte("file"), OpenInClient: proto.Bool(false)}))
			return err
		},
		"目标文件已打开": func() error {
			_, err := client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: []byte("new"), TargetOpenInClient: proto.Bool(true)}))
			return err
		},
		"本地句柄读取": func() error {
			_, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1}))
			return err
		},
		"本地句柄写入": func() error {
			_, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: 1, Data: []byte("x")}))
			return err
		},
		"本地句柄文件同步": func() error {
			_, err := client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{Ino: 1}))
			return err
		},
		"本地句柄目录同步": func() error {
			_, err := client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{Ino: 1}))
			return err
		},
		"本地句柄预分配": func() error {
			_, err := client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{Ino: 1, Length: 1}))
			return err
		},
	}
	for name, call := range calls {
		t.Run(name, func(t *testing.T) {
			err := call()
			require.Error(t, err)
			require.Equal(t, connect.CodeUnimplemented, connect.CodeOf(err))
		})
	}
}

func TestSessionReadAndReadDirHandlers(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	ctx := context.Background()
	sid := uint64(1)
	withSession := mock.MatchedBy(func(ctx context.Context) bool {
		value, ok := meta.SessionIDFromContext(ctx)
		return ok && value != nil && *value == sid
	})
	inode := &meta.Inode{Ino: 1}
	mgr.EXPECT().Lookup(withSession, uint64(1), []byte("x")).Return(inode, nil).Once()
	mgr.EXPECT().GetAttr(withSession, uint64(1)).Return(inode, nil).Once()
	mgr.EXPECT().Readlink(withSession, uint64(1)).Return([]byte("目标"), nil).Once()
	mgr.EXPECT().StatFS(withSession, uint64(1)).Return(&meta.StatFSResult{}, nil).Once()
	mgr.EXPECT().ReadDir(withSession, meta.ReadDirParams{Ino: 1, Limit: 1, SessionID: &sid}).Return([]meta.DirEntry{}, nil).Once()
	mgr.EXPECT().ReadDirPlus(withSession, meta.ReadDirPlusParams{Ino: 1, Limit: 1, SessionID: &sid}).Return([]meta.DirEntryPlus{}, nil).Once()

	_, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{ParentIno: 1, Name: []byte("x"), SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{Ino: 1, SessionId: &sid}))
	require.NoError(t, err)
	response, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{Ino: 1, SessionId: &sid}))
	require.NoError(t, err)
	require.Equal(t, []byte("目标"), response.Msg.Target)
	_, err = client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: 1, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{Ino: 1, Limit: 1, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.ReadDirPlus(ctx, connect.NewRequest(&storagev1.ReadDirPlusRequest{Ino: 1, Limit: 1, SessionId: &sid}))
	require.NoError(t, err)
}
