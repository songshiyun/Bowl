package rpc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func TestShouldRetry_RejectsNilAndBusinessErrorInfo(t *testing.T) {
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, nil))

	bowlErr := connect.NewError(connect.CodeUnavailable, nil)
	bowlDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "ENOENT", Domain: "bowl.storage"})
	require.NoError(t, err)
	bowlErr.AddDetail(bowlDetail)
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, bowlErr))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, bowlErr))

	customDomainErr := connect.NewError(connect.CodeUnavailable, nil)
	customDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EEXIST", Domain: "custom.domain"})
	require.NoError(t, err)
	customDomainErr.AddDetail(customDetail)
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, customDomainErr))

	emptyReasonErr := connect.NewError(connect.CodeUnavailable, nil)
	emptyDetail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "", Domain: "bowl.storage"})
	require.NoError(t, err)
	emptyReasonErr.AddDetail(emptyDetail)
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, emptyReasonErr))
}

func TestShouldRetry_RejectsTerminalConnectCodes(t *testing.T) {
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeInvalidArgument, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeNotFound, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connect.NewError(connect.CodeAlreadyExists, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connect.NewError(connect.CodePermissionDenied, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceWriteProcedure, connect.NewError(connect.CodeFailedPrecondition, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceWriteProcedure, connect.NewError(connect.CodeResourceExhausted, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceFsyncProcedure, connect.NewError(connect.CodeAborted, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeUnimplemented, nil)))
}

func TestShouldRetry_DialFailuresVsMidFlightTransportFailures(t *testing.T) {
	dialOpErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	connectSysErr := &os.SyscallError{Syscall: "connect", Err: errors.New("refused")}
	wrappedRefused := fmt.Errorf("dial storage: %w", syscall.ECONNREFUSED)

	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceCreateDiskProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceForkDiskProcedure))

	require.True(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, dialOpErr))
	require.True(t, shouldRetry(storagev1connect.StorageServiceCreateDiskProcedure, connectSysErr))
	require.True(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, dialOpErr))
	require.True(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, connectSysErr))
	require.True(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, wrappedRefused))
	require.True(t, shouldRetry(storagev1connect.StorageServiceWriteProcedure, wrappedRefused))
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, dialOpErr))

	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeUnavailable, nil)))
	require.True(t, shouldRetry(storagev1connect.StorageServiceGetDiskProcedure, io.EOF))
	require.True(t, shouldRetry(storagev1connect.StorageServiceFsyncProcedure, io.ErrUnexpectedEOF))
	require.True(t, shouldRetry(storagev1connect.StorageServiceLookupProcedure, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
	require.True(t, shouldRetry(storagev1connect.StorageServiceStatFSProcedure, syscall.EPIPE))

	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connect.NewError(connect.CodeUnavailable, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceWriteProcedure, io.ErrUnexpectedEOF))
	require.False(t, shouldRetry(storagev1connect.StorageServiceUnlinkProcedure, syscall.ECONNRESET))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateDiskProcedure, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
	require.False(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, connect.NewError(connect.CodeUnavailable, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, io.EOF))
	require.False(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, io.ErrUnexpectedEOF))
	require.False(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, syscall.ECONNRESET))
	require.False(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
	require.False(t, shouldRetry(storagev1connect.StorageServiceForkDiskProcedure, syscall.EPIPE))

	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, errors.New("unknown failure")))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeInternal, nil)))
}

func TestComputeJitteredIntervalAndBudgetExhaustionMapping(t *testing.T) {
	require.Equal(t, 500*time.Millisecond, computeJitteredInterval(time.Second, func() float64 { return 0 }))
	require.Equal(t, 750*time.Millisecond, computeJitteredInterval(time.Second, func() float64 { return 0.5 }))
	require.Equal(t, time.Second, computeJitteredInterval(time.Second, func() float64 { return 1 }))
	require.Equal(t, time.Duration(0), computeJitteredInterval(0, func() float64 { return 0 }))
	require.Equal(t, -time.Second, computeJitteredInterval(-time.Second, func() float64 { return 0.5 }))

	require.Equal(t, syscall.EIO, MapError(wrapBudgetExceededError(nil)))
	require.Equal(t, syscall.EIO, MapError(wrapBudgetExceededError(errors.New("boom"))))
	require.Equal(t, syscall.EIO, MapError(wrapBudgetExceededError(io.ErrUnexpectedEOF)))

	deadlineErr := connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
	require.Same(t, deadlineErr, wrapBudgetExceededError(deadlineErr))

	businessErr := connect.NewError(connect.CodeUnavailable, errors.New("boom"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "ENOENT", Domain: "bowl.storage"})
	require.NoError(t, err)
	businessErr.AddDetail(detail)
	require.Same(t, businessErr, wrapBudgetExceededError(businessErr))
	require.Equal(t, syscall.ENOENT, MapError(wrapBudgetExceededError(businessErr)))
}
