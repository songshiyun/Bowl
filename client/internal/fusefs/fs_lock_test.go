package fusefs_test

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type lockMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *lockMockStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupLockTestRPCServer(t *testing.T, srv *lockMockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestLockTypeConversion(t *testing.T) {
	for _, tc := range []struct{ host, protocol uint32 }{
		{uint32(syscall.F_RDLCK), fusefs.ProtoLockTypeRead},
		{uint32(syscall.F_WRLCK), fusefs.ProtoLockTypeWrite},
		{uint32(syscall.F_UNLCK), fusefs.ProtoLockTypeUnlock},
	} {
		gotProtocol, status := fusefs.ToProtoLockType(tc.host)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, tc.protocol, gotProtocol)
		gotHost, status := fusefs.ToHostLockType(tc.protocol)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, tc.host, gotHost)
	}
	_, status := fusefs.ToProtoLockType(999)
	require.Equal(t, fuse.EINVAL, status)
	_, status = fusefs.ToHostLockType(999)
	require.Equal(t, fuse.EINVAL, status)
}

func TestNormalizeLockRange(t *testing.T) {
	start, end, status := fusefs.NormalizeLockRange(fuse.FUSE_LK_FLOCK, 100, 200)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(0), start)
	require.Equal(t, uint64(math.MaxUint64), end)
	start, end, status = fusefs.NormalizeLockRange(0, uint64(math.MaxInt64)+1, math.MaxUint64)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(math.MaxInt64)+1, start)
	require.Equal(t, uint64(math.MaxUint64), end)
	_, _, status = fusefs.NormalizeLockRange(0, 20, 10)
	require.Equal(t, fuse.EINVAL, status)
}
