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
	for _, open := range []bool{false, true} {
		withOpen := mock.MatchedBy(func(ctx context.Context) bool {
			value, set := meta.OpenInClientFromContext(ctx)
			return set && value == open
		})
		mockMgr.EXPECT().Unlink(withOpen, uint64(1), []byte("file")).Return(nil).Once()
		_, err := client.Unlink(context.Background(), connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: 1, Name: []byte("file"), OpenInClient: proto.Bool(open),
		}))
		require.NoError(t, err)
	}
}

func TestNewVersionRPCsAreValidatedAndUnimplemented(t *testing.T) {
	mockMgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mockMgr)
	defer cleanup()
	ctx := context.Background()
	const disk = "11111111-1111-7111-8111-111111111111"
	calls := map[string]func() error{
		"ResolveMerge": func() error {
			_, err := client.ResolveMerge(ctx, connect.NewRequest(&storagev1.ResolveMergeRequest{DiskId: disk, BranchName: "main"}))
			return err
		},
		"AbortMerge": func() error {
			_, err := client.AbortMerge(ctx, connect.NewRequest(&storagev1.AbortMergeRequest{DiskId: disk, BranchName: "main"}))
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

func TestDiffHandler(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	const disk = "11111111-1111-7111-8111-111111111111"
	old := sampleTestInode(2, meta.S_IFREG|0o600)
	newAttr := sampleTestInode(2, meta.S_IFREG|0o644)
	old.Attr.Size, newAttr.Attr.Size = 3, 7
	mgr.EXPECT().Diff(mock.Anything, meta.DiffParams{DiskID: disk, FromRef: "main", ToRef: "dev"}).Return([]meta.DiffEntry{{
		ChangeType: meta.DiffChangeTypeRenamed, Path: "dir/new", OldPath: "old", OldAttr: old, NewAttr: newAttr, ModifiedBlockIndices: []uint64{0, 3},
	}}, nil).Once()
	response, err := client.Diff(context.Background(), connect.NewRequest(&storagev1.DiffRequest{DiskId: disk, FromRef: "main", ToRef: "dev"}))
	require.NoError(t, err)
	require.Len(t, response.Msg.Entries, 1)
	entry := response.Msg.Entries[0]
	require.Equal(t, storagev1.DiffChangeType_DIFF_CHANGE_TYPE_RENAMED, entry.ChangeType)
	require.Equal(t, "dir/new", entry.Path)
	require.Equal(t, "old", entry.OldPath)
	require.Equal(t, uint64(3), entry.OldAttr.Size)
	require.Equal(t, uint64(7), entry.NewAttr.Size)
	require.Equal(t, []uint64{0, 3}, entry.ModifiedBlockIndices)
	_, err = client.Diff(context.Background(), connect.NewRequest(&storagev1.DiffRequest{DiskId: "bad", FromRef: "main", ToRef: "dev"}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
	mgr.EXPECT().Diff(mock.Anything, meta.DiffParams{DiskID: disk, FromRef: "main", ToRef: "missing"}).Return(nil, meta.ErrNotFound).Once()
	_, err = client.Diff(context.Background(), connect.NewRequest(&storagev1.DiffRequest{DiskId: disk, FromRef: "main", ToRef: "missing"}))
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(err))
}

func TestSessionTopologyAndReclaimHandlers(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	sid := uint64(41)
	branch := "main"
	disk := "11111111-1111-7111-8111-111111111111"
	withSession := mock.MatchedBy(func(ctx context.Context) bool {
		id, ok := meta.SessionIDFromContext(ctx)
		return ok && id != nil && *id == sid
	})
	withSessionAndOpen := mock.MatchedBy(func(ctx context.Context) bool {
		id, ok := meta.SessionIDFromContext(ctx)
		open, set := meta.OpenInClientFromContext(ctx)
		return ok && id != nil && *id == sid && set && open
	})
	mgr.EXPECT().Unlink(withSessionAndOpen, uint64(1), []byte("file")).Return(nil).Once()
	mgr.EXPECT().Rmdir(withSession, uint64(1), []byte("dir")).Return(nil).Once()
	mgr.EXPECT().Rename(withSessionAndOpen, meta.RenameParams{OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: []byte("new")}).Return(nil).Once()
	mgr.EXPECT().ReclaimOrphan(mock.Anything, meta.ReclaimOrphanParams{SessionID: sid, DiskID: disk, BranchName: &branch, Ino: 2}).Return(nil).Once()
	ctx := context.Background()
	_, err := client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: 1, Name: []byte("file"), SessionId: &sid, OpenInClient: proto.Bool(true)}))
	require.NoError(t, err)
	_, err = client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{ParentIno: 1, Name: []byte("dir"), SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{OldParentIno: 1, OldName: []byte("old"), NewParentIno: 1, NewName: []byte("new"), SessionId: &sid, TargetOpenInClient: proto.Bool(true)}))
	require.NoError(t, err)
	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{SessionId: sid, DiskId: disk, BranchName: &branch, Ino: 2}))
	require.NoError(t, err)
	mgr.EXPECT().ReclaimOrphan(mock.Anything, meta.ReclaimOrphanParams{SessionID: sid, DiskID: disk, Ino: 3}).Return(meta.ErrBadFileHandle).Once()
	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{SessionId: sid, DiskId: disk, Ino: 3}))
	require.Error(t, err)
	require.Equal(t, "EBADF", server.ExtractErrorInfo(err).GetReason())
	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{SessionId: sid, DiskId: "bad", Ino: 3}))
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(err))
}

func TestSessionXattrHandlers(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	ctx := context.Background()
	sid := uint64(41)
	name := []byte("user.test")
	value := []byte("值")
	withSession := mock.MatchedBy(func(ctx context.Context) bool {
		id, ok := meta.SessionIDFromContext(ctx)
		return ok && id != nil && *id == sid
	})
	mgr.EXPECT().SetXattr(withSession, meta.SetXattrParams{Ino: 1, Name: name, Value: value, Flags: meta.XattrCreate}).Return(nil).Once()
	mgr.EXPECT().GetXattr(withSession, uint64(1), name).Return(value, nil).Once()
	mgr.EXPECT().ListXattr(withSession, uint64(1)).Return([][]byte{name}, nil).Once()
	mgr.EXPECT().RemoveXattr(withSession, uint64(1), name).Return(nil).Once()

	_, err := client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{Ino: 1, Name: name, Value: value, Flags: meta.XattrCreate, SessionId: &sid}))
	require.NoError(t, err)
	got, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{Ino: 1, Name: name, SessionId: &sid}))
	require.NoError(t, err)
	require.Equal(t, value, got.Msg.Value)
	list, err := client.ListXattr(ctx, connect.NewRequest(&storagev1.ListXattrRequest{Ino: 1, SessionId: &sid}))
	require.NoError(t, err)
	require.Equal(t, [][]byte{name}, list.Msg.Names)
	_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{Ino: 1, Name: name, SessionId: &sid}))
	require.NoError(t, err)
}

func TestLocalHandleDataAndSyncHandlers(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	ctx := context.Background()
	sid := uint64(41)
	inode := sampleTestInode(2, meta.S_IFREG|0o644)
	mgr.EXPECT().Read(mock.Anything, &meta.ReadRequest{Ino: 2, Offset: 7, Size: 3, SessionID: &sid}).Return(&meta.ReadResponse{Data: []byte("abc")}, nil).Once()
	mgr.EXPECT().Write(mock.Anything, &meta.WriteRequest{Ino: 2, Offset: 7, Data: []byte("abc"), SessionID: &sid}).Return(&meta.WriteResponse{Size: 3, Inode: inode}, nil).Once()
	mgr.EXPECT().Fsync(mock.Anything, meta.FsyncParams{Ino: 2, Datasync: true, SessionID: &sid}).Return(nil).Once()
	mgr.EXPECT().FsyncDir(mock.Anything, meta.FsyncDirParams{Ino: 1, SessionID: &sid}).Return(nil).Once()
	mgr.EXPECT().Fallocate(mock.Anything, meta.FallocateParams{Ino: 2, Offset: 7, Length: 3, Mode: meta.FallocFlKeepSize, SessionID: &sid}).Return(nil).Once()

	read, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: 2, Offset: 7, Size: 3, SessionId: &sid}))
	require.NoError(t, err)
	require.Equal(t, []byte("abc"), read.Msg.Data)
	write, err := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: 2, Offset: 7, Data: []byte("abc"), SessionId: &sid}))
	require.NoError(t, err)
	require.Equal(t, uint32(3), write.Msg.Size)
	_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{Ino: 2, Datasync: true, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{Ino: 1, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{Ino: 2, Offset: 7, Length: 3, Mode: meta.FallocFlKeepSize, SessionId: &sid}))
	require.NoError(t, err)
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

func TestSessionCreateAndLinkHandlers(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	ctx := context.Background()
	sid := uint64(41)
	withSession := mock.MatchedBy(func(ctx context.Context) bool {
		id, ok := meta.SessionIDFromContext(ctx)
		return ok && id != nil && *id == sid
	})
	inode := sampleTestInode(2, meta.S_IFREG|0o644)
	mgr.EXPECT().Create(withSession, mock.Anything).Return(&meta.CreateResult{Inode: inode, Fh: 1}, nil).Once()
	mgr.EXPECT().Mknod(withSession, mock.Anything).Return(inode, nil).Once()
	mgr.EXPECT().Mkdir(withSession, mock.Anything).Return(inode, nil).Once()
	mgr.EXPECT().Symlink(withSession, mock.Anything).Return(inode, nil).Once()
	mgr.EXPECT().Link(withSession, mock.Anything).Return(inode, nil).Once()

	_, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("create"), Mode: 0o644, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{ParentIno: 1, Name: []byte("node"), Mode: meta.S_IFREG | 0o644, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{ParentIno: 1, Name: []byte("dir"), Mode: 0o755, SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{ParentIno: 1, Name: []byte("sym"), Target: []byte("target"), SessionId: &sid}))
	require.NoError(t, err)
	_, err = client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{Ino: 2, NewParentIno: 1, NewName: []byte("link"), SessionId: &sid}))
	require.NoError(t, err)
}

func TestVersionHandlers(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	ctx := context.Background()
	const disk = "11111111-1111-7111-8111-111111111111"
	const commit = "22222222-2222-7222-8222-222222222222"
	const branchID = "33333333-3333-7333-8333-333333333333"
	parent := "main"
	created := time.Date(2026, 11, 4, 0, 0, 0, 0, time.UTC)
	branch := meta.Branch{BranchID: branchID, DiskID: disk, Name: "dev", ParentBranch: &parent, ForkCommitID: proto.String(commit), HeadCommitID: proto.String(commit), State: "NORMAL", CreatedAt: created}
	mgr.EXPECT().CreateBranch(mock.Anything, meta.CreateBranchParams{DiskID: disk, Name: "dev", SourceBranch: &parent, SourceCommitID: proto.String(commit)}).Return(&branch, nil).Once()
	mgr.EXPECT().ListBranches(mock.Anything, disk).Return([]meta.Branch{branch}, nil).Once()
	mgr.EXPECT().Commit(mock.Anything, meta.CommitParams{DiskID: disk, BranchName: "dev", Message: "保存"}).Return(&meta.CommitInfo{CommitID: commit, DiskID: disk, BranchName: "dev", ParentCommitID: proto.String(commit), Message: "保存", CreatedAt: created}, nil).Once()
	mgr.EXPECT().Rollback(mock.Anything, meta.RollbackParams{DiskID: disk, BranchName: "dev", CommitID: commit}).Return(meta.ErrBusy).Once()
	mgr.EXPECT().DeleteBranch(mock.Anything, meta.DeleteBranchParams{DiskID: disk, Name: "dev"}).Return(nil).Once()

	createdBranch, err := client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: disk, Name: "dev", SourceBranch: &parent, SourceCommitId: proto.String(commit)}))
	require.NoError(t, err)
	require.Equal(t, branchID, createdBranch.Msg.Branch.BranchId)
	require.Equal(t, &parent, createdBranch.Msg.Branch.ParentBranch)
	require.Equal(t, proto.String(commit), createdBranch.Msg.Branch.HeadCommitId)
	require.Equal(t, storagev1.BranchState_BRANCH_STATE_NORMAL, createdBranch.Msg.Branch.State)
	require.Equal(t, created, createdBranch.Msg.Branch.CreatedAt.AsTime())
	list, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: disk}))
	require.NoError(t, err)
	require.Len(t, list.Msg.Branches, 1)
	require.Equal(t, createdBranch.Msg.Branch, list.Msg.Branches[0])
	version, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: disk, BranchName: "dev", Message: "保存"}))
	require.NoError(t, err)
	require.Equal(t, commit, version.Msg.Commit.CommitId)
	require.Equal(t, proto.String(commit), version.Msg.Commit.ParentCommitId)
	require.Equal(t, "保存", version.Msg.Commit.Message)
	require.Equal(t, created, version.Msg.Commit.CreatedAt.AsTime())
	_, err = client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: disk, BranchName: "dev", CommitId: commit}))
	require.Equal(t, connect.CodeFailedPrecondition, connect.CodeOf(err))
	require.Equal(t, "EBUSY", server.ExtractErrorInfo(err).GetReason())
	_, err = client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{DiskId: disk, Name: "dev"}))
	require.NoError(t, err)
}

func TestMergeHandler(t *testing.T) {
	mgr := metamock.NewMockManager(t)
	client, cleanup := setupTestServer(t, mgr)
	defer cleanup()
	const disk = "11111111-1111-7111-8111-111111111111"
	const commit = "22222222-2222-7222-8222-222222222222"
	params := meta.MergeParams{DiskID: disk, SourceBranch: "dev", TargetBranch: "main", Strategy: meta.ConflictStrategyTheirs}
	mgr.EXPECT().Merge(mock.Anything, params).Return(&meta.MergeResult{Result: meta.MergeResultMerged, MergeCommitID: proto.String(commit)}, nil).Once()
	response, err := client.Merge(context.Background(), connect.NewRequest(&storagev1.MergeRequest{
		DiskId: disk, SourceBranch: "dev", TargetBranch: "main", Strategy: storagev1.ConflictStrategy_CONFLICT_STRATEGY_THEIRS,
	}))
	require.NoError(t, err)
	require.Equal(t, storagev1.MergeResult_MERGE_RESULT_MERGED, response.Msg.Result)
	require.Equal(t, proto.String(commit), response.Msg.MergeCommitId)
	inode := sampleTestInode(2, meta.S_IFREG|0o644)
	params.Strategy = meta.ConflictStrategyUnspecified
	mgr.EXPECT().Merge(mock.Anything, params).Return(&meta.MergeResult{Result: meta.MergeResultConflicts, Conflicts: []meta.MergeConflict{{Path: "/文件", OursAttr: inode, Reason: "冲突"}}}, nil).Once()
	response, err = client.Merge(context.Background(), connect.NewRequest(&storagev1.MergeRequest{DiskId: disk, SourceBranch: "dev", TargetBranch: "main"}))
	require.NoError(t, err)
	require.Equal(t, storagev1.MergeResult_MERGE_RESULT_CONFLICTS, response.Msg.Result)
	require.Len(t, response.Msg.Conflicts, 1)
	require.Equal(t, "/文件", response.Msg.Conflicts[0].Path)
	require.Equal(t, inode.Attr.Mode, response.Msg.Conflicts[0].OursAttr.Mode)
	require.Nil(t, response.Msg.Conflicts[0].BaseAttr)
	mgr.EXPECT().Merge(mock.Anything, params).Return(nil, meta.ErrBusy).Once()
	_, err = client.Merge(context.Background(), connect.NewRequest(&storagev1.MergeRequest{DiskId: disk, SourceBranch: "dev", TargetBranch: "main"}))
	require.Equal(t, "EBUSY", server.ExtractErrorInfo(err).GetReason())
}
