package fusefs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func Test本地句柄会话过期取消预读(t *testing.T) {
	const step = 64 * 1024
	started := make(chan struct{})
	canceled := make(chan struct{})
	var once sync.Once
	server := &rwMockStorageServer{
		renewSessionFn: func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			return connect.NewResponse(&storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(120 * time.Millisecond))}), nil
		},
		readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
			// 统一预读状态机后，窗口与预取都按 4 MiB 对齐：第二次顺序读拉取 [0,4MiB)，
			// 随后的异步预取固定从 4 MiB 开始。
			if req.Msg.GetOffset() == fusefs.MaxRPCChunkSize {
				once.Do(func() { close(started) })
				<-ctx.Done()
				close(canceled)
				return nil, ctx.Err()
			}
			return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, req.Msg.GetSize())}), nil
		},
	}
	client := setupRWTestRPCServer(t, server)
	mgr, err := session.New(context.Background(), client, session.Config{MinInterval: 10 * time.Second})
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
	t.Cleanup(fs.OnUnmount)
	const fh = 301
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2001, fh, syscall.O_RDONLY, false))
	for _, offset := range []uint64{0, step} {
		_, status := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 2001}, Fh: fh, Offset: offset, Size: step}, nil)
		require.Equal(t, fuse.OK, status)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("预读未启动")
	}
	require.Eventually(t, func() bool {
		_, status := fs.ResolveHandle(fh)
		return status == fuse.EBADF
	}, time.Second, 10*time.Millisecond)
	select {
	case <-canceled:
	case <-time.After(time.Second):
		t.Fatal("会话过期后预读未取消")
	}
}

func Test本地句柄服务端会话失效触发续期(t *testing.T) {
	var renews atomic.Int32
	server := &handleMockStorageServer{
		renewSessionFn: func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			n := renews.Add(1)
			return connect.NewResponse(&storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Minute)), SessionRecreated: n >= 2}), nil
		},
		fsyncFn: func(context.Context, *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
			err := connect.NewError(connect.CodeFailedPrecondition, errors.New("会话已过期"))
			detail, detailErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EBADF", Domain: "bowl.storage"})
			require.NoError(t, detailErr)
			err.AddDetail(detail)
			return nil, err
		},
	}
	client := setupHandleTestRPCServer(t, server)
	mgr, err := session.New(context.Background(), client, session.Config{MinInterval: 10 * time.Second, MaxInterval: 20 * time.Second})
	require.NoError(t, err)
	require.NoError(t, mgr.Start(context.Background()))
	t.Cleanup(func() { _ = mgr.Close() })
	oldSID := mgr.CurrentSessionID()
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
	t.Cleanup(fs.OnUnmount)
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2001, 401, syscall.O_RDWR, false))
	require.Equal(t, fuse.EBADF, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 2001}, Fh: 401}))
	require.Eventually(t, func() bool {
		return renews.Load() >= 3 && mgr.CurrentSessionID() != 0 && mgr.CurrentSessionID() != oldSID
	}, time.Second, 10*time.Millisecond)
}
