package rpc

import (
	"context"
	"errors"
	"io"
	"syscall"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
)

func requireReasonMapping(t *testing.T, reason string, expected syscall.Errno) {
	t.Helper()
	connErr := connect.NewError(connect.CodeInternal, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: reason, Domain: "bowl.storage"})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.Equal(t, expected, MapError(connErr), "Reason %s 必须映射为 %v", reason, expected)
}

func requireCodeMapping(t *testing.T, code connect.Code, expected syscall.Errno) {
	t.Helper()
	require.Equal(t, expected, MapError(connect.NewError(code, nil)), "状态码 %s 必须映射为 %v", code, expected)
}

func TestMapError_All25ErrorInfos(t *testing.T) {
	requireReasonMapping(t, "EINVAL", syscall.EINVAL)
	requireReasonMapping(t, "ENAMETOOLONG", syscall.ENAMETOOLONG)
	requireReasonMapping(t, "ERANGE", syscall.ERANGE)
	requireReasonMapping(t, "E2BIG", syscall.E2BIG)
	requireReasonMapping(t, "ENOENT", syscall.ENOENT)
	requireReasonMapping(t, "ENODATA", syscall.ENODATA)
	requireReasonMapping(t, "EEXIST", syscall.EEXIST)
	requireReasonMapping(t, "EPERM", syscall.EPERM)
	requireReasonMapping(t, "EACCES", syscall.EACCES)
	requireReasonMapping(t, "ENOTDIR", syscall.ENOTDIR)
	requireReasonMapping(t, "EISDIR", syscall.EISDIR)
	requireReasonMapping(t, "ENOTEMPTY", syscall.ENOTEMPTY)
	requireReasonMapping(t, "EBADF", syscall.EBADF)
	requireReasonMapping(t, "ENOSPC", syscall.ENOSPC)
	requireReasonMapping(t, "EAGAIN", syscall.EAGAIN)
	requireReasonMapping(t, "EDEADLK", syscall.EDEADLK)
	requireReasonMapping(t, "EINTR", syscall.EINTR)
	requireReasonMapping(t, "EOPNOTSUPP", syscall.EOPNOTSUPP)
	requireReasonMapping(t, "EMLINK", syscall.EMLINK)
	requireReasonMapping(t, "EFBIG", syscall.EFBIG)
	requireReasonMapping(t, "ESPIPE", syscall.ESPIPE)
	requireReasonMapping(t, "EXDEV", syscall.EXDEV)
	requireReasonMapping(t, "EIO", syscall.EIO)
	requireReasonMapping(t, "EROFS", syscall.EROFS)
	requireReasonMapping(t, "EBUSY", syscall.EBUSY)
}

func TestMapError_ErrorInfoDomainAndReason(t *testing.T) {
	t.Run("空Reason降级按Connect状态码兜底", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeNotFound, nil)
		detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "", Domain: "bowl.storage"})
		require.NoError(t, err)
		connErr.AddDetail(detail)
		require.Equal(t, syscall.ENOENT, MapError(connErr))
	})

	t.Run("自定义Domain携带合法Reason仍正常映射", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeInvalidArgument, nil)
		detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EEXIST", Domain: "custom.domain"})
		require.NoError(t, err)
		connErr.AddDetail(detail)
		require.Equal(t, syscall.EEXIST, MapError(connErr))
	})
}

func TestMapError_ConnectCodeFallback(t *testing.T) {
	requireCodeMapping(t, connect.CodeNotFound, syscall.ENOENT)
	requireCodeMapping(t, connect.CodeAlreadyExists, syscall.EEXIST)
	requireCodeMapping(t, connect.CodePermissionDenied, syscall.EACCES)
	requireCodeMapping(t, connect.CodeInvalidArgument, syscall.EINVAL)
	requireCodeMapping(t, connect.CodeUnimplemented, syscall.EOPNOTSUPP)
	requireCodeMapping(t, connect.CodeResourceExhausted, syscall.ENOSPC)
	requireCodeMapping(t, connect.CodeCanceled, syscall.EINTR)
	requireCodeMapping(t, connect.CodeUnavailable, syscall.EIO)
	requireCodeMapping(t, connect.CodeInternal, syscall.EIO)
	requireCodeMapping(t, connect.CodeDeadlineExceeded, syscall.EIO)
	requireCodeMapping(t, connect.CodeUnknown, syscall.EIO)
	requireCodeMapping(t, connect.CodeAborted, syscall.EIO)
	requireCodeMapping(t, connect.CodeFailedPrecondition, syscall.EBUSY)
	requireCodeMapping(t, connect.CodeOutOfRange, syscall.EIO)
	requireCodeMapping(t, connect.CodeDataLoss, syscall.EIO)
	requireCodeMapping(t, connect.CodeUnauthenticated, syscall.EIO)
}

func TestMapError_TransportAndNil(t *testing.T) {
	require.Equal(t, syscall.Errno(0), MapError(nil))
	require.Equal(t, syscall.EIO, MapError(io.EOF))
	require.Equal(t, syscall.EIO, MapError(errors.New("connection reset by peer")))
	require.Equal(t, syscall.EIO, MapError(connect.NewError(connect.CodeCanceled, syscall.ECONNRESET)))
	require.Equal(t, syscall.EIO, MapError(connect.NewError(connect.CodeCanceled, syscall.EPIPE)))
}

func TestContextWithCancelChanAndPriority(t *testing.T) {
	closedCancel := make(chan struct{})
	close(closedCancel)
	interruptedCtx, cancelInterrupted := ContextWithCancelChan(context.Background(), closedCancel)
	defer cancelInterrupted()
	require.ErrorIs(t, interruptedCtx.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(interruptedCtx), ErrKernelInterrupted)
	require.Equal(t, syscall.EINTR, MapError(context.Cause(interruptedCtx)))

	openCancel := make(chan struct{})
	normalCtx, cancelNormal := ContextWithCancelChan(context.Background(), openCancel)
	cancelNormal()
	<-normalCtx.Done()
	require.ErrorIs(t, normalCtx.Err(), context.Canceled)
	require.NotErrorIs(t, context.Cause(normalCtx), ErrKernelInterrupted)

	nilChanCtx, cancelNil := ContextWithCancelChan(context.Background(), nil)
	require.NoError(t, nilChanCtx.Err())
	cancelNil()
	<-nilChanCtx.Done()
	require.ErrorIs(t, nilChanCtx.Err(), context.Canceled)

	require.Equal(t, syscall.EINTR, MapErrorWithCancel(connect.NewError(connect.CodeInternal, errors.New("boom")), closedCancel))
	require.Equal(t, syscall.ENOENT, MapErrorWithCancel(connect.NewError(connect.CodeNotFound, nil), openCancel))
	require.Equal(t, syscall.EACCES, MapErrorWithCancel(connect.NewError(connect.CodePermissionDenied, nil), nil))
}
