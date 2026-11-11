package control

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requireStatusMapping(t *testing.T, err error, wantCode codes.Code, wantReason string) {
	t.Helper()
	st := ToStatus(err)
	require.NotNil(t, st)
	require.Equal(t, wantCode, st.Code())
	require.Equal(t, err.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, ErrorDomain, info.GetDomain())
	require.Equal(t, wantReason, info.GetReason())
}

func TestToStatusMapsStoreAndControlSentinels(t *testing.T) {
	requireStatusMapping(t, store.ErrQuotaShrinkBelowUsed, codes.FailedPrecondition, "EINVAL")
	requireStatusMapping(t, store.ErrStaleEpoch, codes.FailedPrecondition, "EBUSY")
	requireStatusMapping(t, ErrInvalidArgument, codes.InvalidArgument, "EINVAL")
	requireStatusMapping(t, store.ErrInvalidKey, codes.InvalidArgument, "EINVAL")
	requireStatusMapping(t, store.ErrInvalidValue, codes.InvalidArgument, "EINVAL")
	requireStatusMapping(t, ErrNameTooLong, codes.InvalidArgument, "ENAMETOOLONG")
	requireStatusMapping(t, ErrRange, codes.InvalidArgument, "ERANGE")
	requireStatusMapping(t, ErrNotFound, codes.NotFound, "ENOENT")
	requireStatusMapping(t, ErrNoData, codes.NotFound, "ENODATA")
	requireStatusMapping(t, ErrAlreadyExists, codes.AlreadyExists, "EEXIST")
	requireStatusMapping(t, ErrPerm, codes.PermissionDenied, "EPERM")
	requireStatusMapping(t, store.ErrReadOnly, codes.PermissionDenied, "EPERM")
	requireStatusMapping(t, ErrNotDir, codes.FailedPrecondition, "ENOTDIR")
	requireStatusMapping(t, ErrIsDir, codes.FailedPrecondition, "EISDIR")
	requireStatusMapping(t, ErrNotEmpty, codes.FailedPrecondition, "ENOTEMPTY")
	requireStatusMapping(t, ErrNoSpace, codes.ResourceExhausted, "ENOSPC")
	requireStatusMapping(t, store.ErrInodeExhausted, codes.ResourceExhausted, "ENOSPC")
	requireStatusMapping(t, ErrNotSupported, codes.Unimplemented, "EOPNOTSUPP")
	requireStatusMapping(t, ErrTooManyLinks, codes.ResourceExhausted, "EMLINK")
	requireStatusMapping(t, ErrFileTooLarge, codes.InvalidArgument, "EFBIG")
	requireStatusMapping(t, ErrIO, codes.Internal, "EIO")
	requireStatusMapping(t, ErrClosed, codes.Internal, "EIO")

	wrappedShrink := fmt.Errorf("shrink rejected: %w", store.ErrQuotaShrinkBelowUsed)
	requireStatusMapping(t, wrappedShrink, codes.FailedPrecondition, "EINVAL")

	passthroughBase, err := status.New(codes.InvalidArgument, "validator_erange").WithDetails(&errdetails.ErrorInfo{
		Reason: "ERANGE",
		Domain: ErrorDomain,
	})
	require.NoError(t, err)
	passthrough := ToStatus(passthroughBase.Err())
	require.NotNil(t, passthrough)
	require.Equal(t, codes.InvalidArgument, passthrough.Code())
	require.Len(t, passthrough.Details(), 1)
	passthroughInfo, ok := passthrough.Details()[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, "ERANGE", passthroughInfo.GetReason())

	require.Nil(t, ToStatus(nil))
	require.NoError(t, ToGRPCError(nil))

	grpcErr := ToGRPCError(store.ErrStaleEpoch)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())

	stCanceled := ToStatus(fmt.Errorf("aborted: %w", context.Canceled))
	require.Equal(t, codes.Canceled, stCanceled.Code())
	require.Empty(t, stCanceled.Details())

	stDeadline := ToStatus(fmt.Errorf("timed out: %w", context.DeadlineExceeded))
	require.Equal(t, codes.DeadlineExceeded, stDeadline.Code())
	require.Empty(t, stDeadline.Details())

	stUnknown := ToStatus(errors.New("unexpected internal failure"))
	require.Equal(t, codes.Unknown, stUnknown.Code())
	require.Empty(t, stUnknown.Details())
}

// TestEvictIdleActorAndClosedPoolFencing 验证黑盒 RPC 未暴露的实例池空闲驱逐刷盘与池关闭后 ErrClosed 栅栏。
func TestEvictIdleActorAndClosedPoolFencing(t *testing.T) {
	const diskID = "018f0000-0000-7000-8000-000000000001"
	root := t.TempDir()

	seedActor, err := store.Open(store.Options{
		Dir:      filepath.Join(root, "disks", diskID),
		DiskID:   diskID,
		MaxBytes: 4 << 20,
	})
	require.NoError(t, err)
	epoch, _, err := seedActor.AdvanceEpoch()
	require.NoError(t, err)
	require.Equal(t, uint64(1), epoch)
	require.NoError(t, seedActor.Close())

	mgr := newManager(nil, WithStorageRoot(root))
	ctx := context.Background()

	_, err = mgr.evictIdleActor(ctx, "not-a-uuid")
	require.ErrorIs(t, err, ErrInvalidArgument)

	evictedBeforeLoad, err := mgr.evictIdleActor(ctx, diskID)
	require.NoError(t, err)
	require.False(t, evictedBeforeLoad)

	loaded, err := mgr.getOrOpenActorLocked(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), loaded.WriterEpoch())

	evictedAfterLoad, err := mgr.evictIdleActor(ctx, diskID)
	require.NoError(t, err)
	require.True(t, evictedAfterLoad)
	_, err = loaded.StatFS()
	require.ErrorIs(t, err, store.ErrClosed)

	reloaded, err := mgr.getOrOpenActorLocked(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, uint64(1), reloaded.WriterEpoch())

	require.NoError(t, mgr.Close())
	require.NoError(t, mgr.Close())

	_, err = mgr.evictIdleActor(ctx, diskID)
	require.ErrorIs(t, err, ErrClosed)
	_, err = mgr.Actor(ctx, diskID)
	require.ErrorIs(t, err, ErrClosed)
	_, err = mgr.CreateDisk(ctx, CreateDiskParams{Name: "after-close", MaxBytes: 4096})
	require.ErrorIs(t, err, ErrClosed)
}
