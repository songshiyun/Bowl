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

func TestRetryConfigWithDefaults(t *testing.T) {
	got := RetryConfig{}.withDefaults()
	require.Equal(t, DefaultMaxElapsedTime, got.MaxElapsedTime)
	require.Equal(t, DefaultInitialInterval, got.InitialInterval)
	require.InDelta(t, DefaultMultiplier, got.Multiplier, 1e-9)
	require.Equal(t, DefaultMaxInterval, got.MaxInterval)
	require.False(t, got.Disable)
	require.Nil(t, got.RandFloat64)
}

func TestRetryConfigWithDefaultsPreservesExplicitValues(t *testing.T) {
	cfg := RetryConfig{
		Disable:         true,
		MaxElapsedTime:  time.Minute,
		InitialInterval: time.Millisecond,
		Multiplier:      1.5,
		MaxInterval:     250 * time.Millisecond,
	}
	require.Equal(t, cfg, cfg.withDefaults())
}

func TestRetryConfigWithDefaultsReplacesNonPositiveValues(t *testing.T) {
	cfg := RetryConfig{
		MaxElapsedTime:  -time.Second,
		InitialInterval: -time.Millisecond,
		Multiplier:      -1,
		MaxInterval:     -time.Millisecond,
	}
	got := cfg.withDefaults()
	require.Equal(t, DefaultMaxElapsedTime, got.MaxElapsedTime)
	require.Equal(t, DefaultInitialInterval, got.InitialInterval)
	require.InDelta(t, DefaultMultiplier, got.Multiplier, 1e-9)
	require.Equal(t, DefaultMaxInterval, got.MaxInterval)
}

func TestHasErrorInfoDetectsBusinessReason(t *testing.T) {
	connErr := connect.NewError(connect.CodeUnavailable, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "ENOENT", Domain: "bowl.storage"})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.True(t, hasErrorInfo(connErr))
}

func TestHasErrorInfoDetectsReasonOutsideBowlDomain(t *testing.T) {
	connErr := connect.NewError(connect.CodeUnavailable, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EEXIST", Domain: "custom.domain"})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.True(t, hasErrorInfo(connErr))
}

func TestHasErrorInfoIgnoresEmptyReason(t *testing.T) {
	connErr := connect.NewError(connect.CodeUnavailable, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "", Domain: "bowl.storage"})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.False(t, hasErrorInfo(connErr))
}

func TestHasErrorInfoIgnoresPlainAndNilErrors(t *testing.T) {
	require.False(t, hasErrorInfo(nil))
	require.False(t, hasErrorInfo(errors.New("boom")))
	require.False(t, hasErrorInfo(connect.NewError(connect.CodeInternal, nil)))
}

func TestIsDialErrorRecognizesConnectionRefused(t *testing.T) {
	require.True(t, isDialError(syscall.ECONNREFUSED))
	require.True(t, isDialError(fmt.Errorf("dial storage: %w", syscall.ECONNREFUSED)))
}

func TestIsDialErrorRecognizesDialOpError(t *testing.T) {
	require.True(t, isDialError(&net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}))
}

func TestIsDialErrorRecognizesConnectSyscallError(t *testing.T) {
	require.True(t, isDialError(&os.SyscallError{Syscall: "connect", Err: errors.New("boom")}))
}

func TestIsDialErrorRejectsOtherErrors(t *testing.T) {
	require.False(t, isDialError(nil))
	require.False(t, isDialError(errors.New("boom")))
	require.False(t, isDialError(&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
	require.False(t, isDialError(&os.SyscallError{Syscall: "read", Err: syscall.ECONNRESET}))
}

func TestIsTransportErrorRecognizesConnectionFailures(t *testing.T) {
	require.True(t, isTransportError(syscall.ECONNRESET))
	require.True(t, isTransportError(io.EOF))
	require.True(t, isTransportError(io.ErrUnexpectedEOF))
	require.True(t, isTransportError(syscall.EPIPE))
	require.True(t, isTransportError(&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
}

func TestIsTransportErrorRejectsOtherErrors(t *testing.T) {
	require.False(t, isTransportError(nil))
	require.False(t, isTransportError(errors.New("boom")))
	require.False(t, isTransportError(syscall.ECONNREFUSED))
	require.False(t, isTransportError(&os.SyscallError{Syscall: "connect", Err: syscall.ECONNREFUSED}))
}

func TestComputeJitteredIntervalUsesRandomFactor(t *testing.T) {
	require.Equal(t, 500*time.Millisecond, computeJitteredInterval(time.Second, func() float64 { return 0 }))
	require.Equal(t, 750*time.Millisecond, computeJitteredInterval(time.Second, func() float64 { return 0.5 }))
	require.Equal(t, time.Second, computeJitteredInterval(time.Second, func() float64 { return 1 }))
}

func TestComputeJitteredIntervalFallsBackForNonPositiveResult(t *testing.T) {
	require.Equal(t, time.Duration(0), computeJitteredInterval(0, func() float64 { return 0 }))
	require.Equal(t, -time.Second, computeJitteredInterval(-time.Second, func() float64 { return 0.5 }))
}

func TestIsNeverRetryCodeRejectsTerminalCodes(t *testing.T) {
	require.True(t, isNeverRetryCode(connect.CodeInvalidArgument))
	require.True(t, isNeverRetryCode(connect.CodeNotFound))
	require.True(t, isNeverRetryCode(connect.CodeAlreadyExists))
	require.True(t, isNeverRetryCode(connect.CodePermissionDenied))
	require.True(t, isNeverRetryCode(connect.CodeFailedPrecondition))
	require.True(t, isNeverRetryCode(connect.CodeResourceExhausted))
	require.True(t, isNeverRetryCode(connect.CodeAborted))
	require.True(t, isNeverRetryCode(connect.CodeUnimplemented))
}

func TestIsNeverRetryCodeAllowsTransientCodes(t *testing.T) {
	require.False(t, isNeverRetryCode(connect.CodeUnavailable))
	require.False(t, isNeverRetryCode(connect.CodeInternal))
	require.False(t, isNeverRetryCode(connect.CodeDeadlineExceeded))
	require.False(t, isNeverRetryCode(connect.CodeCanceled))
	require.False(t, isNeverRetryCode(connect.CodeUnknown))
	require.False(t, isNeverRetryCode(connect.CodeDataLoss))
}

func TestShouldRetryRejectsNilError(t *testing.T) {
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, nil))
}

func TestShouldRetryRejectsBusinessErrorInfo(t *testing.T) {
	connErr := connect.NewError(connect.CodeUnavailable, nil)
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "ENOENT", Domain: "bowl.storage"})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connErr))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connErr))
}

func TestShouldRetryRejectsNeverRetryCodes(t *testing.T) {
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeNotFound, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeInvalidArgument, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connect.NewError(connect.CodeAlreadyExists, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connect.NewError(connect.CodePermissionDenied, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceWriteProcedure, connect.NewError(connect.CodeFailedPrecondition, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceFsyncProcedure, connect.NewError(connect.CodeFailedPrecondition, nil)))
}

func TestShouldRetryRetriesDialErrorsForAllProcedures(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
	require.True(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, dialErr))
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, dialErr))
	require.True(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, syscall.ECONNREFUSED))
}

func TestShouldRetryRetriesIdempotentUnavailable(t *testing.T) {
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeUnavailable, nil)))
	require.True(t, shouldRetry(storagev1connect.StorageServiceGetDiskProcedure, connect.NewError(connect.CodeUnavailable, errors.New("temporary"))))
}

func TestShouldRetryRetriesIdempotentTransportErrors(t *testing.T) {
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, io.EOF))
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, io.ErrUnexpectedEOF))
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
	require.True(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, syscall.EPIPE))
}

func TestShouldRetryRejectsNonIdempotentFailuresAfterSend(t *testing.T) {
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, connect.NewError(connect.CodeUnavailable, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, io.ErrUnexpectedEOF))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateProcedure, syscall.ECONNRESET))
	require.False(t, shouldRetry(storagev1connect.StorageServiceCreateDiskProcedure, &net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET}))
}

func TestShouldRetryRejectsUnknownIdempotentFailures(t *testing.T) {
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, errors.New("boom")))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeInternal, nil)))
}

func TestWrapBudgetExceededErrorFromNil(t *testing.T) {
	wrapped := wrapBudgetExceededError(nil)
	var connErr *connect.Error
	require.ErrorAs(t, wrapped, &connErr)
	require.Equal(t, connect.CodeDeadlineExceeded, connErr.Code())
	require.ErrorIs(t, wrapped, context.DeadlineExceeded)
}

func TestWrapBudgetExceededErrorPreservesBusinessErrorInfo(t *testing.T) {
	connErr := connect.NewError(connect.CodeUnavailable, errors.New("boom"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "ENOENT", Domain: "bowl.storage"})
	require.NoError(t, err)
	connErr.AddDetail(detail)
	require.Same(t, connErr, wrapBudgetExceededError(connErr))
}

func TestWrapBudgetExceededErrorPreservesDeadlineExceeded(t *testing.T) {
	connErr := connect.NewError(connect.CodeDeadlineExceeded, context.DeadlineExceeded)
	require.Same(t, connErr, wrapBudgetExceededError(connErr))
}

func TestWrapBudgetExceededErrorWrapsOtherErrors(t *testing.T) {
	wrapped := wrapBudgetExceededError(errors.New("boom"))
	var connErr *connect.Error
	require.ErrorAs(t, wrapped, &connErr)
	require.Equal(t, connect.CodeDeadlineExceeded, connErr.Code())
}

func TestBudgetExhaustionMapsToEIO(t *testing.T) {
	require.Equal(t, syscall.EIO, MapError(wrapBudgetExceededError(nil)))
	require.Equal(t, syscall.EIO, MapError(wrapBudgetExceededError(errors.New("boom"))))
	require.Equal(t, syscall.EIO, MapError(wrapBudgetExceededError(io.ErrUnexpectedEOF)))
}

func TestCancellationTakesPrecedenceOverRetryMapping(t *testing.T) {
	cancel := make(chan struct{})
	close(cancel)
	require.Equal(t, syscall.EINTR, MapErrorWithCancel(connect.NewError(connect.CodeUnavailable, nil), cancel))
}
