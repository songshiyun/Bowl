//go:build e2e

package e2e_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestBranchStorageLifecycle 从公开 RPC 验证迁移后的分支、数据和孤儿节点闭环。
func TestBranchStorageLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()
	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "contract-cleanup", MaxBytes: 4096}))
	require.NoError(t, err)
	disk := created.Msg.GetDisk()
	require.Equal(t, uint64(1), disk.GetRootIno())
	diskID := disk.GetDiskId()
	mainID := uint64(time.Now().UnixNano())
	featureID := mainID + 1
	readerID := mainID + 2
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		for _, id := range []uint64{mainID, featureID, readerID} {
			_, closeErr := client.CloseSession(cleanup, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: id}))
			require.NoError(t, closeErr)
		}
		_, deleteErr := client.DeleteDisk(cleanup, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		require.NoError(t, deleteErr)
	})

	heartbeatCtx, stopHeartbeats := context.WithCancel(ctx)
	var heartbeatWG sync.WaitGroup
	heartbeatErrors := make(chan error, 3)
	t.Cleanup(func() { stopHeartbeats(); heartbeatWG.Wait() })
	keepAlive := func(request *storagev1.RenewSessionRequest) {
		heartbeatWG.Add(1)
		go func() {
			defer heartbeatWG.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-heartbeatCtx.Done():
					return
				case <-ticker.C:
					_, err := client.RenewSession(heartbeatCtx, connect.NewRequest(request))
					if err != nil && heartbeatCtx.Err() == nil {
						heartbeatErrors <- err
						return
					}
				}
			}
		}()
	}

	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: mainID, DiskId: &diskID}))
	require.NoError(t, err)
	keepAlive(&storagev1.RenewSessionRequest{SessionId: mainID, DiskId: &diskID})
	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: featureID, DiskId: &diskID}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")
	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: readerID}))
	testutil.RequireConnectError(t, err, connect.CodeInvalidArgument, "EINVAL")

	file, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("shared"), Mode: 0o644, SessionId: &mainID}))
	require.NoError(t, err)
	ino := file.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: ino, Data: []byte("hello"), SessionId: &mainID}))
	require.NoError(t, err)
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{Ino: ino, Name: []byte("user.test"), Value: []byte("value"), SessionId: &mainID}))
	require.NoError(t, err)
	xattr, err := client.GetXattr(ctx, connect.NewRequest(&storagev1.GetXattrRequest{Ino: ino, Name: []byte("user.test"), SessionId: &mainID}))
	require.NoError(t, err)
	require.Equal(t, []byte("value"), xattr.Msg.Value)

	base, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: diskID, BranchName: "main", Message: "基础"}))
	require.NoError(t, err)
	baseID := base.Msg.GetCommit().GetCommitId()
	_, err = client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: diskID, Name: "feature", SourceCommitId: &baseID}))
	require.NoError(t, err)
	branch := "feature"
	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: featureID, DiskId: &diskID, BranchName: &branch}))
	require.NoError(t, err)
	keepAlive(&storagev1.RenewSessionRequest{SessionId: featureID, DiskId: &diskID, BranchName: &branch})
	featureFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("feature"), Mode: 0o644, SessionId: &featureID}))
	require.NoError(t, err)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: featureFile.Msg.GetInode().GetIno(), Data: []byte("world"), SessionId: &featureID}))
	require.NoError(t, err)
	featureCommit, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: diskID, BranchName: branch, Message: "分支"}))
	require.NoError(t, err)
	diff, err := client.Diff(ctx, connect.NewRequest(&storagev1.DiffRequest{DiskId: diskID, FromRef: baseID, ToRef: featureCommit.Msg.GetCommit().GetCommitId()}))
	require.NoError(t, err)
	require.NotEmpty(t, diff.Msg.Entries)
	gotDisk, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	require.Equal(t, uint64(10), gotDisk.Msg.GetDisk().GetUsedBytes())

	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: readerID, DiskId: &diskID, ReadOnly: true, CommitId: &baseID}))
	require.NoError(t, err)
	keepAlive(&storagev1.RenewSessionRequest{SessionId: readerID, DiskId: &diskID, ReadOnly: true, CommitId: &baseID})
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: ino, Data: []byte("x"), SessionId: &readerID}))
	testutil.RequireConnectError(t, err, connect.CodePermissionDenied, "EPERM")
	_, err = client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: diskID, BranchName: "main", CommitId: baseID}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: 1, Name: []byte("shared"), SessionId: &mainID, OpenInClient: proto.Bool(true)}))
	require.NoError(t, err)
	read, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &mainID}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), read.Msg.Data)
	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{SessionId: mainID, DiskId: diskID, Ino: ino}))
	require.NoError(t, err)
	_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &mainID}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	read, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &readerID}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), read.Msg.Data)

	stopHeartbeats()
	heartbeatWG.Wait()
	close(heartbeatErrors)
	for heartbeatErr := range heartbeatErrors {
		require.NoError(t, heartbeatErr)
	}
	for _, id := range []uint64{mainID, featureID, readerID} {
		_, err = client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: id}))
		require.NoError(t, err)
	}
	merged, err := client.Merge(ctx, connect.NewRequest(&storagev1.MergeRequest{DiskId: diskID, SourceBranch: branch, TargetBranch: "main"}))
	require.NoError(t, err)
	require.NotEmpty(t, merged.Msg.GetMergeCommitId())
	_, err = client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: diskID, BranchName: "main", CommitId: baseID}))
	require.NoError(t, err)
	_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: mainID, DiskId: &diskID}))
	require.NoError(t, err)
	read, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &mainID}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), read.Msg.Data)
}
