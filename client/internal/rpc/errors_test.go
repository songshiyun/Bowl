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
	require.Equal(t, expected, MapError(connErr), "Reason %s must map to %v", reason, expected)
}

func requireCodeMapping(t *testing.T, code connect.Code, expected syscall.Errno) {
	t.Helper()
	require.Equal(t, expected, MapError(connect.NewError(code, nil)), "code %s must map to %v", code, expected)
}

func TestMapError_All24ErrorInfos(t *testing.T) {
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
}

func TestMapError_ErrorInfoDomainAndReason(t *testing.T) {
	t.Run("empty reason in ErrorInfo falls through", func(t *testing.T) {
		connErr := connect.NewError(connect.CodeNotFound, nil)
		detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "", Domain: "bowl.storage"})
		require.NoError(t, err)
		connErr.AddDetail(detail)
		require.Equal(t, syscall.ENOENT, MapError(connErr))
	})

	t.Run("non-bowl domain with valid reason", func(t *testing.T) {
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
	requireCodeMapping(t, connect.CodeFailedPrecondition, syscall.EIO)
	requireCodeMapping(t, connect.CodeOutOfRange, syscall.EIO)
	requireCodeMapping(t, connect.CodeDataLoss, syscall.EIO)
	requireCodeMapping(t, connect.CodeUnauthenticated, syscall.EIO)
}

func TestMapError_TransportAndNil(t *testing.T) {
	t.Run("nil error returns 0", func(t *testing.T) {
		require.Equal(t, syscall.Errno(0), MapError(nil))
	})

	t.Run("io.EOF maps to EIO", func(t *testing.T) {
		require.Equal(t, syscall.EIO, MapError(io.EOF))
	})

	t.Run("arbitrary network error maps to EIO", func(t *testing.T) {
		require.Equal(t, syscall.EIO, MapError(errors.New("connection reset by peer")))
	})

	t.Run("CodeCanceled wrapping ECONNRESET maps to EIO", func(t *testing.T) {
		require.Equal(t, syscall.EIO, MapError(connect.NewError(connect.CodeCanceled, syscall.ECONNRESET)))
	})

	t.Run("CodeCanceled wrapping EPIPE maps to EIO", func(t *testing.T) {
		require.Equal(t, syscall.EIO, MapError(connect.NewError(connect.CodeCanceled, syscall.EPIPE)))
	})
}

func TestContextWithCancelChan_CancelTriggered(t *testing.T) {
	cancel := make(chan struct{})
	ctx, cancelFn := ContextWithCancelChan(context.Background(), cancel)
	defer cancelFn()

	require.NoError(t, ctx.Err())
	close(cancel)
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.ErrorIs(t, context.Cause(ctx), ErrKernelInterrupted)
	require.Equal(t, syscall.EINTR, MapError(context.Cause(ctx)))
}

func TestContextWithCancelChan_NormalCancel(t *testing.T) {
	cancel := make(chan struct{})
	ctx, cancelFn := ContextWithCancelChan(context.Background(), cancel)

	cancelFn()
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
	require.NotErrorIs(t, context.Cause(ctx), ErrKernelInterrupted)
}

func TestContextWithCancelChan_NilChan(t *testing.T) {
	ctx, cancelFn := ContextWithCancelChan(context.Background(), nil)
	require.NoError(t, ctx.Err())
	cancelFn()
	<-ctx.Done()
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

func TestMapErrorWithCancel(t *testing.T) {
	t.Run("prioritize EINTR when cancel chan is closed", func(t *testing.T) {
		cancel := make(chan struct{})
		close(cancel)
		connErr := connect.NewError(connect.CodeInternal, errors.New("internal server failure"))
		require.Equal(t, syscall.EINTR, MapErrorWithCancel(connErr, cancel))
	})

	t.Run("fallback to regular mapping when cancel chan is open", func(t *testing.T) {
		cancel := make(chan struct{})
		require.Equal(t, syscall.ENOENT, MapErrorWithCancel(connect.NewError(connect.CodeNotFound, nil), cancel))
	})

	t.Run("handle nil cancel chan normally", func(t *testing.T) {
		require.Equal(t, syscall.EACCES, MapErrorWithCancel(connect.NewError(connect.CodePermissionDenied, nil), nil))
	})
}
