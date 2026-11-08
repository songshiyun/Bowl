package control_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func requireStatusMapping(t *testing.T, err error, wantCode codes.Code, wantReason string) {
	t.Helper()
	st := control.ToStatus(err)
	require.NotNil(t, st)
	require.Equal(t, wantCode, st.Code())
	require.Equal(t, err.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	require.Equal(t, control.ErrorDomain, info.GetDomain())
	require.Equal(t, "bowl.storage", info.GetDomain())
	require.Equal(t, wantReason, info.GetReason())
}

func TestToStatusMapsStoreAndControlSentinels(t *testing.T) {
	requireStatusMapping(t, store.ErrQuotaShrinkBelowUsed, codes.FailedPrecondition, "EINVAL")
	requireStatusMapping(t, control.ErrInvalidShrink, codes.FailedPrecondition, "EINVAL")
	requireStatusMapping(t, store.ErrStaleEpoch, codes.FailedPrecondition, "EBUSY")
	requireStatusMapping(t, control.ErrInvalidArgument, codes.InvalidArgument, "EINVAL")
	requireStatusMapping(t, store.ErrInvalidKey, codes.InvalidArgument, "EINVAL")
	requireStatusMapping(t, store.ErrInvalidValue, codes.InvalidArgument, "EINVAL")
	requireStatusMapping(t, control.ErrNameTooLong, codes.InvalidArgument, "ENAMETOOLONG")
	requireStatusMapping(t, control.ErrRange, codes.InvalidArgument, "ERANGE")
	requireStatusMapping(t, control.ErrNotFound, codes.NotFound, "ENOENT")
	requireStatusMapping(t, control.ErrNoData, codes.NotFound, "ENODATA")
	requireStatusMapping(t, control.ErrAlreadyExists, codes.AlreadyExists, "EEXIST")
	requireStatusMapping(t, control.ErrPerm, codes.PermissionDenied, "EPERM")
	requireStatusMapping(t, store.ErrReadOnly, codes.PermissionDenied, "EPERM")
	requireStatusMapping(t, control.ErrNotDir, codes.FailedPrecondition, "ENOTDIR")
	requireStatusMapping(t, control.ErrIsDir, codes.FailedPrecondition, "EISDIR")
	requireStatusMapping(t, control.ErrNotEmpty, codes.FailedPrecondition, "ENOTEMPTY")
	requireStatusMapping(t, control.ErrNoSpace, codes.ResourceExhausted, "ENOSPC")
	requireStatusMapping(t, store.ErrInodeExhausted, codes.ResourceExhausted, "ENOSPC")
	requireStatusMapping(t, control.ErrNotSupported, codes.Unimplemented, "EOPNOTSUPP")
	requireStatusMapping(t, control.ErrTooManyLinks, codes.ResourceExhausted, "EMLINK")
	requireStatusMapping(t, control.ErrFileTooLarge, codes.InvalidArgument, "EFBIG")
	requireStatusMapping(t, control.ErrIO, codes.Internal, "EIO")
	requireStatusMapping(t, control.ErrClosed, codes.Internal, "EIO")

	wrappedShrink := fmt.Errorf("shrink rejected: %w", store.ErrQuotaShrinkBelowUsed)
	requireStatusMapping(t, wrappedShrink, codes.FailedPrecondition, "EINVAL")

	require.Nil(t, control.ToStatus(nil))
	require.NoError(t, control.ToGRPCError(nil))

	grpcErr := control.ToGRPCError(store.ErrStaleEpoch)
	st, ok := status.FromError(grpcErr)
	require.True(t, ok)
	require.Equal(t, codes.FailedPrecondition, st.Code())

	stCanceled := control.ToStatus(context.Canceled)
	require.Equal(t, codes.Canceled, stCanceled.Code())
	require.Empty(t, stCanceled.Details())

	stDeadline := control.ToStatus(context.DeadlineExceeded)
	require.Equal(t, codes.DeadlineExceeded, stDeadline.Code())
	require.Empty(t, stDeadline.Details())

	stUnknown := control.ToStatus(errors.New("unexpected error"))
	require.Equal(t, codes.Unknown, stUnknown.Code())
	require.Empty(t, stUnknown.Details())
}
