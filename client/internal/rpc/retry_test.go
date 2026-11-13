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

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func newTestErrorInfo(t *testing.T, domain, reason string) *connect.Error {
	t.Helper()
	err := connect.NewError(connect.CodeUnavailable, errors.New("storage failure"))
	detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Domain: domain,
		Reason: reason,
	})
	require.NoError(t, detailErr)
	err.AddDetail(detail)
	return err
}

func TestIsIdempotentProcedure_GivenWhitelistAndMutatingProcedures_ReturnsExpectedClassification(t *testing.T) {
	// Given / When / Then: 13 个只读与幂等接口判定为 true
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceGetDiskProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceListDisksProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceLookupProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceGetAttrProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadlinkProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceFsyncProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceFsyncDirProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadDirProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceReadDirPlusProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceGetXattrProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceListXattrProcedure))
	require.True(t, IsIdempotentProcedure(storagev1connect.StorageServiceStatFSProcedure))

	// 非幂等写接口判定为 false
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceCreateDiskProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceForkDiskProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceCreateProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceWriteProcedure))
	require.False(t, IsIdempotentProcedure(storagev1connect.StorageServiceUnlinkProcedure))
}

func TestRetry_GivenDisabledConfig_WhenUnavailableOccurs_DoesNotRetry(t *testing.T) {
	// Given
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("unreachable")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			Disable: true,
		},
	}, clk)
	require.NoError(t, err)

	// When
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	// Then
	require.Error(t, err)
	require.Equal(t, connect.CodeUnavailable, connect.CodeOf(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenIdempotentRead_WhenTransientFailuresRecover_RetriesWithBoundedBackoffAndSucceeds(t *testing.T) {
	// Given: 前 3 次返回可重试的 CodeUnavailable，第 4 次成功；固定 RandFloat64=1.0 以便精确验证指数步长
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-1"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-2"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient-3"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("recovered")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     250 * time.Millisecond,
			MaxElapsedTime:  5 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	// When
	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 9}))

	// Then
	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("recovered"), payload.Data)
	payload.Release()
	require.Equal(t, 4, handler.calls)
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		250 * time.Millisecond,
	}, clk.Sleeps())
}

func TestRetry_GivenJitterRandZero_WhenRetrying_AppliesHalfIntervalFloor(t *testing.T) {
	// Given: RandFloat64=0.0 时退避因子为 0.5
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient"))
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     1 * time.Second,
			MaxElapsedTime:  2 * time.Second,
			RandFloat64:     func() float64 { return 0.0 },
		},
	}, clk)
	require.NoError(t, err)

	// When
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	// Then
	require.NoError(t, err)
	require.Equal(t, []time.Duration{50 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenIdempotentRead_WhenBudgetExhausted_WrapsDeadlineExceededAndMapsEIO(t *testing.T) {
	// Given: 总预算 350ms，步长 100ms -> 200ms -> 250ms(截断为剩余 50ms)
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("still down"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     250 * time.Millisecond,
			MaxElapsedTime:  350 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	// When
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	// Then
	require.Error(t, err)
	require.Equal(t, connect.CodeDeadlineExceeded, connect.CodeOf(err))
	require.Equal(t, syscall.EIO, MapError(err))
	require.Equal(t, 3, handler.calls)
	require.Equal(t, []time.Duration{
		100 * time.Millisecond,
		200 * time.Millisecond,
		50 * time.Millisecond,
	}, clk.Sleeps())
}

func TestRetry_GivenBusinessErrorInfo_WhenUnavailableCodeSet_NeverRetries(t *testing.T) {
	// Given
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, newTestErrorInfo(t, "bowl.storage", "ENOENT")
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	// When
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 4}))

	// Then
	require.Error(t, err)
	require.Equal(t, syscall.ENOENT, MapError(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenEmptyReasonErrorInfo_WhenUnavailable_FallsBackToRetry(t *testing.T) {
	// Given
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, newTestErrorInfo(t, "bowl.storage", "")
			},
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			RandFloat64: func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	// When
	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	// Then
	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("ok"), payload.Data)
	payload.Release()
	require.Equal(t, 2, handler.calls)
}

func TestRetry_GivenTerminalConnectCode_WhenIdempotentRead_NeverRetries(t *testing.T) {
	// Given
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodePermissionDenied, errors.New("denied"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	// When
	_, err = client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	// Then
	require.Error(t, err)
	require.Equal(t, syscall.EACCES, MapError(err))
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenNonIdempotentWrite_WhenUnavailableOrTransportEOF_DoesNotRetry(t *testing.T) {
	// Given
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("mid-flight drop"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
	}, clk)
	require.NoError(t, err)

	// When
	_, err = client.Write(context.Background(), connect.NewRequest(&storagev1.WriteRequest{Ino: 1, Data: []byte("x")}))

	// Then
	require.Error(t, err)
	require.Equal(t, 1, handler.calls)
	require.Empty(t, clk.Sleeps())
}

func TestRetry_GivenNonIdempotentWrite_WhenDialConnectionRefused_RetriesAndSucceeds(t *testing.T) {
	// Given: 通过 HTTPClient 注入建连阶段 ECONNREFUSED 故障，第 2 次由内存适配器接通
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{}
	innerClient := newInMemoryHTTPClient(handler)
	var attempt int
	dialErrClient := &httpClientWithFirstErr{
		firstErr: &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED},
		next:     innerClient.Transport,
		attempts: &attempt,
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: dialErrClient.HTTPClient(),
		RetryConfig: &RetryConfig{
			InitialInterval: 80 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	// When
	resp, err := client.Write(context.Background(), connect.NewRequest(&storagev1.WriteRequest{Ino: 1, Data: []byte("abc")}))

	// Then
	require.NoError(t, err)
	require.Equal(t, uint32(3), resp.Msg.GetSize())
	require.Equal(t, 2, attempt)
	require.Equal(t, []time.Duration{80 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenIdempotentRead_WhenTransportEOFAndReset_RetriesAndSucceeds(t *testing.T) {
	// Given: 第 1 次 io.ErrUnexpectedEOF，第 2 次 net.OpError(ECONNRESET)，第 3 次成功
	clk := newAutoAdvanceClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{}
	innerClient := newInMemoryHTTPClient(handler)
	transport := &scriptedTransport{
		errs: []error{
			io.ErrUnexpectedEOF,
			&net.OpError{Op: "read", Net: "tcp", Err: syscall.ECONNRESET},
		},
		next: innerClient.Transport,
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: transport.HTTPClient(),
		RetryConfig: &RetryConfig{
			InitialInterval: 50 * time.Millisecond,
			Multiplier:      2.0,
			MaxInterval:     500 * time.Millisecond,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	// When
	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))

	// Then
	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("ok"), payload.Data)
	payload.Release()
	require.Equal(t, 3, transport.calls)
	require.Equal(t, []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}, clk.Sleeps())
}

func TestRetry_GivenParentContextCanceledDuringBackoff_StopsImmediatelyWithCanceled(t *testing.T) {
	// Given: 手动虚拟时钟在首次退避定时器挂起后触发父上下文取消
	clk := newManualClock(time.Date(2026, 11, 13, 10, 0, 0, 0, time.UTC))
	handler := &scriptedReadHandler{
		steps: []func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error){
			func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("unavailable"))
			},
		},
	}
	client, err := newClientWithClock(ClientConfig{
		Endpoint:   "http://127.0.0.1:9000",
		HTTPClient: newInMemoryHTTPClient(handler),
		RetryConfig: &RetryConfig{
			InitialInterval: 100 * time.Millisecond,
			MaxElapsedTime:  10 * time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	}, clk)
	require.NoError(t, err)

	parentCtx, cancelParent := context.WithCancel(context.Background())
	errCh := make(chan error, 1)

	// When
	go func() {
		_, callErr := client.Read(parentCtx, connect.NewRequest(&storagev1.ReadRequest{Ino: 1, Size: 2}))
		errCh <- callErr
	}()

	armedDuration := <-clk.TimerArmed()
	require.Equal(t, 100*time.Millisecond, armedDuration)
	cancelParent()
	gotErr := <-errCh

	// Then
	require.ErrorIs(t, gotErr, context.Canceled)
	require.Equal(t, syscall.EINTR, MapError(gotErr))
	require.Equal(t, 1, handler.calls)
}

func TestRetry_GivenSyscallConnectError_ShouldRetryEvenForNonIdempotentProcedure(t *testing.T) {
	// Given / When / Then: 覆盖 os.SyscallError{Syscall:"connect"} 建连识别与绝不重试状态码
	connectErr := fmt.Errorf("connect failed: %w", &os.SyscallError{Syscall: "connect", Err: errors.New("refused")})
	require.True(t, shouldRetry(storagev1connect.StorageServiceCreateDiskProcedure, connectErr))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeInvalidArgument, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeNotFound, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeAlreadyExists, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeFailedPrecondition, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeResourceExhausted, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeAborted, nil)))
	require.False(t, shouldRetry(storagev1connect.StorageServiceReadProcedure, connect.NewError(connect.CodeUnimplemented, nil)))
}
