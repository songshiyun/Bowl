//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"strconv"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestBranchStorageLifecycle 从公开 RPC 验证迁移后的分支、数据和孤儿节点闭环。
func TestBranchStorageLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()
	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{Name: "contract-cleanup", MaxBytes: 8 * 1024 * 1024}))
	require.NoError(t, err)
	disk := created.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	require.Equal(t, uint64(1), rootIno)
	diskID := disk.GetDiskId()
	mainID := uint64(time.Now().UnixNano())
	featureID := mainID + 1
	readerID := mainID + 2
	t.Cleanup(func() {
		cleanup, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, closeErr := client.CloseSession(cleanup, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: mainID}))
		require.NoError(t, closeErr)
		_, closeErr = client.CloseSession(cleanup, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: featureID}))
		require.NoError(t, closeErr)
		_, closeErr = client.CloseSession(cleanup, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: readerID}))
		require.NoError(t, closeErr)
		_, deleteErr := client.DeleteDisk(cleanup, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		require.NoError(t, deleteErr)
	})

	// 会话租约默认 2s，测试在每个数据面阶段前显式续约，替代后台心跳协程。
	renew := func(request *storagev1.RenewSessionRequest) *storagev1.RenewSessionResponse {
		resp, renewErr := client.RenewSession(ctx, connect.NewRequest(request))
		require.NoError(t, renewErr)
		return resp.Msg
	}
	branch := "feature"
	mainSession := &storagev1.RenewSessionRequest{SessionId: mainID, DiskId: &diskID}
	featureSession := &storagev1.RenewSessionRequest{SessionId: featureID, DiskId: &diskID, BranchName: &branch}
	readerSession := &storagev1.RenewSessionRequest{SessionId: readerID, DiskId: &diskID, ReadOnly: true}
	firstRenewal := renew(mainSession)
	require.NotNil(t, firstRenewal.GetExpiresAt(), "会话续期必须返回租约到期时间")
	require.True(t, firstRenewal.GetSessionRecreated(), "首次续期必须创建会话")

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
	xattrValue := string(xattr.Msg.Value)
	require.Equal(t, "value", xattrValue)

	base, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: diskID, BranchName: "main", Message: "基础"}))
	require.NoError(t, err)
	baseID := base.Msg.GetCommit().GetCommitId()
	_, err = client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{DiskId: diskID, Name: branch, SourceCommitId: &baseID}))
	require.NoError(t, err)
	renew(featureSession)
	featureFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("feature"), Mode: 0o644, SessionId: &featureID}))
	require.NoError(t, err)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: featureFile.Msg.GetInode().GetIno(), Data: []byte("world"), SessionId: &featureID}))
	require.NoError(t, err)
	featureCommit, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: diskID, BranchName: branch, Message: "分支"}))
	require.NoError(t, err)
	diff, err := client.Diff(ctx, connect.NewRequest(&storagev1.DiffRequest{DiskId: diskID, FromRef: baseID, ToRef: featureCommit.Msg.GetCommit().GetCommitId()}))
	require.NoError(t, err)
	diffEntries := len(diff.Msg.Entries)
	require.Equal(t, 1, diffEntries)
	gotDisk, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	usedBytes := gotDisk.Msg.GetDisk().GetUsedBytes()
	require.Equal(t, uint64(10), usedBytes)

	commitID := baseID
	readerSession.CommitId = &commitID
	renew(readerSession)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: ino, Data: []byte("x"), SessionId: &readerID}))
	testutil.RequireConnectError(t, err, connect.CodePermissionDenied, "EPERM")
	readerWriteReason := testutil.ExtractErrorInfo(err).GetReason()
	renew(mainSession)
	_, err = client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: diskID, BranchName: "main", CommitId: baseID}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")
	rollbackWhileOpenReason := testutil.ExtractErrorInfo(err).GetReason()

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{ParentIno: 1, Name: []byte("shared"), SessionId: &mainID, OpenInClient: new(true)}))
	require.NoError(t, err)
	read, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &mainID}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), read.Msg.Data)
	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{SessionId: mainID, DiskId: diskID, Ino: ino}))
	require.NoError(t, err)
	_, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &mainID}))
	testutil.RequireConnectError(t, err, connect.CodeNotFound, "ENOENT")
	orphanAfterReclaimReason := testutil.ExtractErrorInfo(err).GetReason()
	renew(readerSession)
	read, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &readerID}))
	require.NoError(t, err)
	readerStillReads := string(read.Msg.Data)
	require.Equal(t, "hello", readerStillReads)

	_, err = client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: mainID}))
	require.NoError(t, err)
	_, err = client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: featureID}))
	require.NoError(t, err)
	_, err = client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: readerID}))
	require.NoError(t, err)
	merged, err := client.Merge(ctx, connect.NewRequest(&storagev1.MergeRequest{DiskId: diskID, SourceBranch: branch, TargetBranch: "main"}))
	require.NoError(t, err)
	mergeCommitPresent := merged.Msg.GetMergeCommitId() != ""
	require.True(t, mergeCommitPresent)
	_, err = client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: diskID, BranchName: "main", CommitId: baseID}))
	require.NoError(t, err)
	renew(mainSession)
	read, err = client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: ino, Size: 5, SessionId: &mainID}))
	require.NoError(t, err)
	rollbackRestored := string(read.Msg.Data)
	require.Equal(t, "hello", rollbackRestored)

	// 单次 4 MiB Write/Read RPC 往返：覆盖服务端 8 MiB 读上限内的最大块写入路径。
	bigPayload := bytes.Repeat([]byte{0x4d}, 4*1024*1024)
	bigFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{ParentIno: 1, Name: []byte("big.bin"), Mode: 0o644, SessionId: &mainID}))
	require.NoError(t, err)
	bigIno := bigFile.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: bigIno, Data: bigPayload, SessionId: &mainID}))
	require.NoError(t, err)
	bigRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{Ino: bigIno, Size: uint32(len(bigPayload)), SessionId: &mainID}))
	require.NoError(t, err)
	require.Equal(t, bigPayload, bigRead.Msg.Data, "4 MiB 单次 Write/Read 往返必须逐字节一致")

	_, err = client.CloseSession(ctx, connect.NewRequest(&storagev1.CloseSessionRequest{SessionId: mainID}))
	require.NoError(t, err)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{Ino: bigIno, Data: []byte("stale"), SessionId: &mainID}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBADF")
	closedSessionWrite := testutil.ExtractErrorInfo(err).GetReason()

	recordArtifact(t, map[string]string{
		"session_renewed":      strconv.FormatBool(firstRenewal.GetExpiresAt() != nil),
		"closed_session_write": closedSessionWrite,
		"root_ino":             strconv.FormatUint(rootIno, 10),
		"used_bytes":           strconv.FormatUint(usedBytes, 10),
		"xattr_value":          xattrValue,
		"diff_entries":         strconv.Itoa(diffEntries),
		"reader_write":         readerWriteReason,
		"rollback_while_open":  rollbackWhileOpenReason,
		"orphan_after_reclaim": orphanAfterReclaimReason,
		"reader_still_reads":   readerStillReads,
		"merge_commit":         strconv.FormatBool(mergeCommitPresent),
		"rollback_restored":    rollbackRestored,
		"big_write_bytes":      strconv.Itoa(len(bigPayload)),
		"big_write_sha256":     fileSHA256(bigPayload),
	})
}
