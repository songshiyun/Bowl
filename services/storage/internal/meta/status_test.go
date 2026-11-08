package meta_test

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func requireStatusMapping(t *testing.T, err error, wantCode codes.Code, wantMessage, wantReason string) {
	t.Helper()
	st := meta.ToStatus(err)
	require.NotNil(t, st)
	require.Equal(t, wantCode, st.Code())
	require.Equal(t, wantMessage, st.Message())

	details := st.Details()
	require.Len(t, details, 1, "必须恰好包含一个 Detail (ErrorInfo)")
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok, "Detail 必须为 *errdetails.ErrorInfo 类型")
	require.Equal(t, meta.ErrorDomain, info.GetDomain())
	require.Equal(t, "bowl.storage", info.GetDomain())
	require.Equal(t, wantReason, info.GetReason())
}

func requireSentinelMapping(t *testing.T, err error, wantCode codes.Code, wantReason string) {
	t.Helper()
	requireStatusMapping(t, err, wantCode, err.Error(), wantReason)
}

func TestToStatus_AllSentinelErrors(t *testing.T) {
	requireSentinelMapping(t, meta.ErrInvalidShrink, codes.FailedPrecondition, "EINVAL")
	requireSentinelMapping(t, meta.ErrInvalidArgument, codes.InvalidArgument, "EINVAL")
	requireSentinelMapping(t, meta.ErrNameTooLong, codes.InvalidArgument, "ENAMETOOLONG")
	requireSentinelMapping(t, meta.ErrRange, codes.InvalidArgument, "ERANGE")
	requireSentinelMapping(t, meta.ErrTooBig, codes.InvalidArgument, "E2BIG")
	requireSentinelMapping(t, meta.ErrNotFound, codes.NotFound, "ENOENT")
	requireSentinelMapping(t, meta.ErrNoData, codes.NotFound, "ENODATA")
	requireSentinelMapping(t, meta.ErrAlreadyExists, codes.AlreadyExists, "EEXIST")
	requireSentinelMapping(t, meta.ErrPerm, codes.PermissionDenied, "EPERM")
	requireSentinelMapping(t, meta.ErrNotDir, codes.FailedPrecondition, "ENOTDIR")
	requireSentinelMapping(t, meta.ErrIsDir, codes.FailedPrecondition, "EISDIR")
	requireSentinelMapping(t, meta.ErrNotEmpty, codes.FailedPrecondition, "ENOTEMPTY")
	requireSentinelMapping(t, meta.ErrBadFileHandle, codes.FailedPrecondition, "EBADF")
	requireSentinelMapping(t, meta.ErrNoSpace, codes.ResourceExhausted, "ENOSPC")
	requireSentinelMapping(t, meta.ErrAgain, codes.ResourceExhausted, "EAGAIN")
	requireSentinelMapping(t, meta.ErrDeadlock, codes.Aborted, "EDEADLK")
	requireSentinelMapping(t, meta.ErrInterrupted, codes.Canceled, "EINTR")
	requireSentinelMapping(t, meta.ErrNotSupported, codes.Unimplemented, "EOPNOTSUPP")
	requireSentinelMapping(t, meta.ErrTooManyLinks, codes.ResourceExhausted, "EMLINK")
	requireSentinelMapping(t, meta.ErrFileTooLarge, codes.InvalidArgument, "EFBIG")
	requireSentinelMapping(t, meta.ErrIllegalSeek, codes.InvalidArgument, "ESPIPE")
	requireSentinelMapping(t, meta.ErrIO, codes.Internal, "EIO")
	requireSentinelMapping(t, meta.ErrAccess, codes.PermissionDenied, "EACCES")
	requireSentinelMapping(t, meta.ErrCrossDevice, codes.FailedPrecondition, "EXDEV")
	requireSentinelMapping(t, meta.ErrBusy, codes.FailedPrecondition, "EBUSY")
}

func TestErrInvalidShrink_HierarchyAndStatus(t *testing.T) {
	err := meta.ErrInvalidShrink

	// 1. 同时满足对 ErrInvalidShrink 与 ErrInvalidArgument 的匹配
	require.ErrorIs(t, err, meta.ErrInvalidShrink, "必须满足 errors.Is(err, ErrInvalidShrink)")
	require.ErrorIs(t, err, meta.ErrInvalidArgument, "必须满足 errors.Is(err, ErrInvalidArgument)")

	// 2. ToStatus 优先匹配 ErrInvalidShrink，映射为 FailedPrecondition 与 EINVAL
	st := meta.ToStatus(err)
	require.NotNil(t, st)
	assert.Equal(t, codes.FailedPrecondition, st.Code())
	assert.Equal(t, err.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, "bowl.storage", info.GetDomain())
	assert.Equal(t, "EINVAL", info.GetReason())

	// 3. 包装后的错误依然映射为 FailedPrecondition 与 EINVAL
	wrapped := fmt.Errorf("disk shrink check failed: %w", err)
	stWrapped := meta.ToStatus(wrapped)
	require.NotNil(t, stWrapped)
	assert.Equal(t, codes.FailedPrecondition, stWrapped.Code())
	assert.Equal(t, wrapped.Error(), stWrapped.Message())
	detailsWrapped := stWrapped.Details()
	require.Len(t, detailsWrapped, 1)
	infoWrapped, ok := detailsWrapped[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, "bowl.storage", infoWrapped.GetDomain())
	assert.Equal(t, "EINVAL", infoWrapped.GetReason())
}

func TestToStatus_WrappedError(t *testing.T) {
	wrapped := fmt.Errorf("上层操作包装: %w", meta.ErrNotFound)
	st := meta.ToStatus(wrapped)

	require.NotNil(t, st)
	assert.Equal(t, codes.NotFound, st.Code())
	assert.Equal(t, wrapped.Error(), st.Message())

	details := st.Details()
	require.Len(t, details, 1)

	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, meta.ErrorDomain, info.GetDomain())
	assert.Equal(t, "ENOENT", info.GetReason())
}

func TestToStatus_NilError(t *testing.T) {
	assert.Nil(t, meta.ToStatus(nil))
	assert.NoError(t, meta.ToGRPCError(nil))
}

func TestToStatus_ContextErrors(t *testing.T) {
	t.Run("纯 context.Canceled 映射为 codes.Canceled 且不携带 ErrorInfo", func(t *testing.T) {
		st := meta.ToStatus(context.Canceled)
		require.NotNil(t, st)
		assert.Equal(t, codes.Canceled, st.Code())
		assert.Equal(t, context.Canceled.Error(), st.Message())
		assert.Empty(t, st.Details(), "纯 context.Canceled 不得携带 ErrorInfo 详情")
	})

	t.Run("包装的 context.Canceled 映射为 codes.Canceled 且不携带 ErrorInfo", func(t *testing.T) {
		wrapped := fmt.Errorf("operation aborted by client: %w", context.Canceled)
		st := meta.ToStatus(wrapped)
		require.NotNil(t, st)
		assert.Equal(t, codes.Canceled, st.Code())
		assert.Equal(t, wrapped.Error(), st.Message())
		assert.Empty(t, st.Details())
	})

	t.Run("纯 context.DeadlineExceeded 映射为 codes.DeadlineExceeded 且不携带 ErrorInfo", func(t *testing.T) {
		st := meta.ToStatus(context.DeadlineExceeded)
		require.NotNil(t, st)
		assert.Equal(t, codes.DeadlineExceeded, st.Code())
		assert.Equal(t, context.DeadlineExceeded.Error(), st.Message())
		assert.Empty(t, st.Details(), "纯 context.DeadlineExceeded 不得携带 ErrorInfo 详情")
	})

	t.Run("包装的 context.DeadlineExceeded 映射为 codes.DeadlineExceeded 且不携带 ErrorInfo", func(t *testing.T) {
		wrapped := fmt.Errorf("timeout waiting for lock: %w", context.DeadlineExceeded)
		st := meta.ToStatus(wrapped)
		require.NotNil(t, st)
		assert.Equal(t, codes.DeadlineExceeded, st.Code())
		assert.Equal(t, wrapped.Error(), st.Message())
		assert.Empty(t, st.Details())
	})

	t.Run("对比 ErrInterrupted 必须映射为 codes.Canceled 且携带 EINTR ErrorInfo", func(t *testing.T) {
		st := meta.ToStatus(meta.ErrInterrupted)
		require.NotNil(t, st)
		assert.Equal(t, codes.Canceled, st.Code())
		details := st.Details()
		require.Len(t, details, 1)
		info, ok := details[0].(*errdetails.ErrorInfo)
		require.True(t, ok)
		assert.Equal(t, "EINTR", info.GetReason())
		assert.Equal(t, meta.ErrorDomain, info.GetDomain())
	})
}

func TestToStatus_UnknownError(t *testing.T) {
	err := errors.New("底层数据库未预期的网络断开")
	st := meta.ToStatus(err)

	require.NotNil(t, st)
	assert.Equal(t, codes.Unknown, st.Code())
	assert.Equal(t, err.Error(), st.Message())
	assert.Empty(t, st.Details(), "未知错误不携带 ErrorInfo 领域信息")
}

func TestToGRPCError_Conversion(t *testing.T) {
	err := meta.ToGRPCError(meta.ErrNoSpace)
	require.Error(t, err)

	st, ok := status.FromError(err)
	require.True(t, ok)
	assert.Equal(t, codes.ResourceExhausted, st.Code())

	details := st.Details()
	require.Len(t, details, 1)
	info, ok := details[0].(*errdetails.ErrorInfo)
	require.True(t, ok)
	assert.Equal(t, meta.ErrorDomain, info.GetDomain())
	assert.Equal(t, "ENOSPC", info.GetReason())
}

func validatorStatusError(t *testing.T, reason string) error {
	t.Helper()
	message := "validation failure: " + reason
	st, err := status.New(codes.InvalidArgument, message).WithDetails(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: meta.ErrorDomain,
	})
	require.NoError(t, err)
	return st.Err()
}

func TestToStatus_PreservesValidatorGRPCStatus(t *testing.T) {
	requireStatusMapping(t, validatorStatusError(t, "ERANGE"), codes.InvalidArgument, "validation failure: ERANGE", "ERANGE")
	requireStatusMapping(t, validatorStatusError(t, "E2BIG"), codes.InvalidArgument, "validation failure: E2BIG", "E2BIG")
	requireStatusMapping(t, validatorStatusError(t, "ENAMETOOLONG"), codes.InvalidArgument, "validation failure: ENAMETOOLONG", "ENAMETOOLONG")
	requireStatusMapping(t, validatorStatusError(t, "EINVAL"), codes.InvalidArgument, "validation failure: EINVAL", "EINVAL")
}
