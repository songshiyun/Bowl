package fusefs_test

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
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
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type handleMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	createFn       func(context.Context, *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error)
	openFn         func(context.Context, *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error)
	flushFn        func(context.Context, *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error)
	fsyncFn        func(context.Context, *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error)
	releaseFn      func(context.Context, *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error)
	setLkFn        func(context.Context, *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error)
	setLkwFn       func(context.Context, *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *handleMockStorageServer) SetLk(ctx context.Context, req *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
	if m.setLkFn != nil {
		return m.setLkFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.SetLkResponse{}), nil
}

func (m *handleMockStorageServer) SetLkw(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
	if m.setLkwFn != nil {
		return m.setLkwFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.SetLkwResponse{}), nil
}

func (m *handleMockStorageServer) Create(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	if m.createFn != nil {
		return m.createFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Open(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
	if m.openFn != nil {
		return m.openFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Flush(ctx context.Context, req *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
	if m.flushFn != nil {
		return m.flushFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Fsync(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	if m.fsyncFn != nil {
		return m.fsyncFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) Release(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
	if m.releaseFn != nil {
		return m.releaseFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *handleMockStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupHandleTestRPCServer(t *testing.T, srv *handleMockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func setupTestSessionManager(t *testing.T, client storagev1connect.StorageServiceClient) *session.Manager {
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.Close()
	})
	return mgr
}

func TestToProtoOpenFlags_Sanitization(t *testing.T) {
	// 1. 基础读写模式映射
	require.Equal(t, uint32(0o0), fusefs.ToProtoOpenFlags(uint32(syscall.O_RDONLY)))
	require.Equal(t, uint32(0o1), fusefs.ToProtoOpenFlags(uint32(syscall.O_WRONLY)))
	require.Equal(t, uint32(0o2), fusefs.ToProtoOpenFlags(uint32(syscall.O_RDWR)))

	// 2. 控制位单独映射
	require.Equal(t, uint32(0o100), fusefs.ToProtoOpenFlags(uint32(syscall.O_CREAT)))
	require.Equal(t, uint32(0o200), fusefs.ToProtoOpenFlags(uint32(syscall.O_EXCL)))
	require.Equal(t, uint32(0o1000), fusefs.ToProtoOpenFlags(uint32(syscall.O_TRUNC)))
	require.Equal(t, uint32(0o2000), fusefs.ToProtoOpenFlags(uint32(syscall.O_APPEND)))

	// 3. 常见组合映射
	flags1 := uint32(syscall.O_WRONLY | syscall.O_CREAT | syscall.O_TRUNC)
	require.Equal(t, uint32(0o1|0o100|0o1000), fusefs.ToProtoOpenFlags(flags1))

	flags2 := uint32(syscall.O_RDWR | syscall.O_CREAT | syscall.O_EXCL)
	require.Equal(t, uint32(0o2|0o100|0o200), fusefs.ToProtoOpenFlags(flags2))

	flags3 := uint32(syscall.O_WRONLY | syscall.O_APPEND)
	require.Equal(t, uint32(0o1|0o2000), fusefs.ToProtoOpenFlags(flags3))

	// 4. 跨架构宿主机高位标志剥离（防碰撞服务端 0o20000000 O_TMPFILE）
	var highBits uint32 = 0x8000 | 0x4000 | 0x20000 | 0x80000 | 0x800 | 0x101000 | 0o20000000
	cleaned := fusefs.ToProtoOpenFlags(flags1 | highBits)
	require.Equal(t, uint32(0o1|0o100|0o1000), cleaned, "高位必须被彻底剥离")
	require.Zero(t, cleaned&0o20000000, "严禁碰撞服务端 O_TMPFILE (0o20000000)")
}

func TestRawFS_Create(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)

	t.Run("read-only mode blocks create with EROFS", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, mgr, fusefs.Config{ReadOnly: true, RootIno: 1001})
		var out fuse.CreateOut
		status := roFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
			Mode:     0o644,
		}, "file.txt", &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("anti-aliasing invalid parent ino returns ENOENT", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		var out fuse.CreateOut
		status := fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1001}, // 非 1 且等于 RootIno
		}, "file.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("successful create with direct-io and handle binding", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			RootIno:  1001,
			DirectIO: true,
		})

		var calledParentIno uint64
		var calledName string
		var calledMode uint32
		var calledUid, calledGid uint32
		var calledFlags uint32
		var calledSID *uint64

		mockSrv.createFn = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			calledParentIno = req.Msg.GetParentIno()
			calledName = string(req.Msg.GetName())
			calledMode = req.Msg.GetMode()
			calledUid = req.Msg.GetUid()
			calledGid = req.Msg.GetGid()
			calledFlags = req.Msg.GetFlags()
			calledSID = req.Msg.SessionId

			return connect.NewResponse(&storagev1.CreateResponse{
				Inode: &storagev1.Inode{
					Ino:        2002,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode:  0o644 | syscall.S_IFREG,
						Size:  0,
						Nlink: 1,
						Uid:   1000,
						Gid:   1000,
					},
				},
				Fh:                  42,
				OpenFlags:           0,
				EntryTimeoutSeconds: 1.5,
				AttrTimeoutSeconds:  2.5,
			}), nil
		}

		var out fuse.CreateOut
		status := fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{
				NodeId: 1,
				Caller: fuse.Caller{
					Owner: fuse.Owner{Uid: 1000, Gid: 1000},
				},
			},
			Flags: uint32(syscall.O_CREAT | syscall.O_WRONLY | 0x8000), // 携带高位
			Mode:  0o100644,
		}, "test.txt", &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(1001), calledParentIno)
		require.Equal(t, "test.txt", calledName)
		require.Equal(t, uint32(0o100644&0xffff), calledMode)
		require.Equal(t, uint32(1000), calledUid)
		require.Equal(t, uint32(1000), calledGid)
		require.Equal(t, uint32(0o1|0o100), calledFlags, "Flags 必须经 ToProtoOpenFlags 清洗")
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)

		// 验证 out 填充
		require.Equal(t, uint64(2002), out.EntryOut.NodeId)
		require.Equal(t, uint64(42), out.OpenOut.Fh)
		require.True(t, (out.OpenOut.OpenFlags&fuse.FOPEN_DIRECT_IO) != 0, "DirectIO 必须附加 FOPEN_DIRECT_IO")

		// 验证句柄会话血缘已绑定
		sid, err := mgr.ResolveHandleSession(42)
		require.NoError(t, err)
		require.Equal(t, mgr.CurrentSessionID(), sid)
	})

	t.Run("server error mapped to fuse.Status", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
		mockSrv.createFn = func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			return nil, connect.NewError(connect.CodeAlreadyExists, errors.New("file exists"))
		}

		var out fuse.CreateOut
		status := fs.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
			Mode:     0o644,
		}, "exists.txt", &out)
		require.Equal(t, fuse.Status(syscall.EEXIST), status)
	})
}

func TestRawFS_Open(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)

	t.Run("read-only mode blocks write open but allows read open", func(t *testing.T) {
		roFS := fusefs.NewRawFS(client, mgr, fusefs.Config{ReadOnly: true, RootIno: 1001})

		// 1. O_WRONLY -> EROFS
		var out fuse.OpenOut
		status := roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_WRONLY),
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)

		// 2. O_RDWR -> EROFS
		status = roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDWR),
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)

		// 3. O_RDONLY | O_TRUNC -> EROFS
		status = roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDONLY | syscall.O_TRUNC),
		}, &out)
		require.Equal(t, fuse.Status(syscall.EROFS), status)

		// 4. O_RDONLY -> 成功放行
		mockSrv.openFn = func(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			return connect.NewResponse(&storagev1.OpenResponse{
				Fh:        100,
				OpenFlags: 0,
			}), nil
		}
		status = roFS.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDONLY),
		}, &out)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(100), out.Fh)
	})

	t.Run("open passes cleaned flags and session_id", func(t *testing.T) {
		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			RootIno:  1001,
			DirectIO: true,
		})

		var calledIno uint64
		var calledFlags uint32
		var calledSID *uint64

		mockSrv.openFn = func(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			calledIno = req.Msg.GetIno()
			calledFlags = req.Msg.GetFlags()
			calledSID = req.Msg.SessionId

			return connect.NewResponse(&storagev1.OpenResponse{
				Fh:        88,
				OpenFlags: 0,
			}), nil
		}

		var out fuse.OpenOut
		status := fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2002},
			Flags:    uint32(syscall.O_RDWR | syscall.O_APPEND | 0x8000), // 携带高位
		}, &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2002), calledIno)
		require.Equal(t, uint32(0o2|0o2000), calledFlags)
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)
		require.Equal(t, uint64(88), out.Fh)
		require.True(t, (out.OpenFlags&fuse.FOPEN_DIRECT_IO) != 0)

		// 检查 handle session
		sid, err := mgr.ResolveHandleSession(88)
		require.NoError(t, err)
		require.Equal(t, mgr.CurrentSessionID(), sid)
	})
}

func TestRawFS_Flush(t *testing.T) {
	var flushCalls atomic.Int32
	mockSrv := &handleMockStorageServer{
		flushFn: func(_ context.Context, _ *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
			flushCalls.Add(1)
			return connect.NewResponse(&storagev1.FlushResponse{}), nil
		},
	}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
	t.Cleanup(fs.OnUnmount)
	require.Equal(t, fuse.ENOENT, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 1001}, Fh: 55}))
	require.Equal(t, fuse.EBADF, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 2001}, Fh: 999}))
	mgr.BindHandle(55, mgr.CurrentSessionID())
	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 2001}, Fh: 55, LockOwner: 777}))
	require.Zero(t, flushCalls.Load())
}

func TestRawFS_Fsync(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

	t.Run("unbound handle returns EBADF", func(t *testing.T) {
		status := fs.Fsync(nil, &fuse.FsyncIn{
			InHeader:   fuse.InHeader{NodeId: 2001},
			Fh:         999,
			FsyncFlags: 0,
		})
		require.Equal(t, fuse.Status(syscall.EBADF), status)
	})

	t.Run("datasync flag differentiated correctly", func(t *testing.T) {
		mgr.BindHandle(66, mgr.CurrentSessionID())

		var calledDatasync bool
		mockSrv.fsyncFn = func(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
			calledDatasync = req.Msg.GetDatasync()
			return connect.NewResponse(&storagev1.FsyncResponse{}), nil
		}

		// 1. FsyncFlags = 0 -> datasync = false
		status := fs.Fsync(nil, &fuse.FsyncIn{
			InHeader:   fuse.InHeader{NodeId: 2001},
			Fh:         66,
			FsyncFlags: 0,
		})
		require.Equal(t, fuse.OK, status)
		require.False(t, calledDatasync)

		// 2. FsyncFlags = 1 -> datasync = true
		status = fs.Fsync(nil, &fuse.FsyncIn{
			InHeader:   fuse.InHeader{NodeId: 2001},
			Fh:         66,
			FsyncFlags: 1,
		})
		require.Equal(t, fuse.OK, status)
		require.True(t, calledDatasync)
	})
}

func TestRawFS_Release(t *testing.T) {
	mockSrv := &handleMockStorageServer{}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})

	t.Run("release unbinds handle and passes sanitized flags", func(t *testing.T) {
		fh := uint64(77)
		mgr.BindHandle(fh, mgr.CurrentSessionID())
		_, ok := mgr.LookupHandleSession(fh)
		require.True(t, ok, "句柄必须先已绑定")

		var calledIno, calledFh uint64
		var calledLockOwner *uint64
		var calledReleaseFlags uint32
		var calledSID *uint64

		mockSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			calledIno = req.Msg.GetIno()
			calledFh = req.Msg.GetFh()
			calledLockOwner = req.Msg.LockOwner
			calledReleaseFlags = req.Msg.GetReleaseFlags()
			calledSID = req.Msg.SessionId
			return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
		}

		fs.Release(nil, &fuse.ReleaseIn{
			InHeader:     fuse.InHeader{NodeId: 2001},
			Fh:           fh,
			LockOwner:    999,
			ReleaseFlags: 0x1f, // 高位包含杂乱标志，期望 & 0x3 保留 0x3
		})

		// 验证参数透传与清洗
		require.Equal(t, uint64(2001), calledIno)
		require.Equal(t, fh, calledFh)
		require.NotNil(t, calledLockOwner)
		require.Equal(t, uint64(999), *calledLockOwner)
		require.Equal(t, uint32(0x3), calledReleaseFlags)
		require.NotNil(t, calledSID)
		require.Equal(t, mgr.CurrentSessionID(), *calledSID)

		// 验证 handle 必定已被解绑
		_, ok = mgr.LookupHandleSession(fh)
		require.False(t, ok, "Release 后句柄血缘必须已被清理")
	})

	t.Run("transient network error enqueues async retry until server succeeds or returns EBADF", func(t *testing.T) {
		retryFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
			RootIno:              1001,
			ReleaseRetryInterval: 15 * time.Millisecond,
		})
		t.Cleanup(retryFS.OnUnmount)

		// 1. 初次超时/不可用失败，后续重试成功
		fh1 := uint64(177)
		sid := mgr.CurrentSessionID()
		mgr.BindHandle(fh1, sid)

		var attempts1 atomic.Int32
		mockSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			if req.Msg.GetFh() != fh1 {
				return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
			}
			n := attempts1.Add(1)
			if n < 3 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporary network failure"))
			}
			return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
		}

		retryFS.Release(nil, &fuse.ReleaseIn{
			InHeader:     fuse.InHeader{NodeId: 2001},
			Fh:           fh1,
			LockOwner:    888,
			ReleaseFlags: 0x1,
		})

		// 本地句柄立即解绑
		_, ok := mgr.LookupHandleSession(fh1)
		require.False(t, ok, "初次 Release 失败后本地句柄仍须立即解绑")

		require.Eventually(t, func() bool {
			return attempts1.Load() == 3
		}, 150*time.Millisecond, 5*time.Millisecond, "后台重试队列应自动重发直至服务端返回成功")

		// 成功后不应再继续重试
		time.Sleep(45 * time.Millisecond)
		require.Equal(t, int32(3), attempts1.Load())

		// 2. 初次失败入队，重试期间服务端返回 EBADF（视为已完成销毁并出队）
		fh2 := uint64(178)
		mgr.BindHandle(fh2, sid)

		var attempts2 atomic.Int32
		mockSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			if req.Msg.GetFh() != fh2 {
				return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
			}
			n := attempts2.Add(1)
			if n == 1 {
				return nil, connect.NewError(connect.CodeDeadlineExceeded, errors.New("timeout"))
			}
			cerr := connect.NewError(connect.CodeFailedPrecondition, errors.New("bad file descriptor"))
			detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
				Reason: "EBADF",
				Domain: "bowl.storage",
			})
			require.NoError(t, err)
			cerr.AddDetail(detail)
			return nil, cerr
		}

		retryFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       fh2,
		})

		require.Eventually(t, func() bool {
			return attempts2.Load() == 2
		}, 150*time.Millisecond, 5*time.Millisecond, "重试收到 EBADF 后应停止重试")

		time.Sleep(45 * time.Millisecond)
		require.Equal(t, int32(2), attempts2.Load(), "EBADF 视为已完成销毁，出队后不再重发")
	})

	t.Run("session rotation drops stale session pending release items", func(t *testing.T) {
		var rotateFlag atomic.Bool
		localSrv := &handleMockStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				recreated := rotateFlag.Swap(false)
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Millisecond)),
					SessionRecreated: recreated,
				}), nil
			},
		}
		localClient := setupHandleTestRPCServer(t, localSrv)
		localMgr, err := session.New(context.Background(), localClient, session.Config{
			MinInterval: 15 * time.Millisecond,
			MaxInterval: 30 * time.Millisecond,
		})
		require.NoError(t, err)
		require.NoError(t, localMgr.Start(context.Background()))
		t.Cleanup(func() { _ = localMgr.Close() })

		oldSID := localMgr.CurrentSessionID()
		retryFS := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{
			RootIno:              1001,
			ReleaseRetryInterval: 80 * time.Millisecond,
		})
		t.Cleanup(retryFS.OnUnmount)

		fh := uint64(179)
		localMgr.BindHandle(fh, oldSID)

		var releaseCalls atomic.Int32
		localSrv.releaseFn = func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			releaseCalls.Add(1)
			return nil, connect.NewError(connect.CodeUnavailable, errors.New("network down"))
		}

		// 初次 Release 失败入队（仅触发 1 次同步调用）
		retryFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       fh,
		})
		require.Equal(t, int32(1), releaseCalls.Load())

		// 触发会话超期轮换
		rotateFlag.Store(true)
		require.Eventually(t, func() bool {
			cur := localMgr.CurrentSessionID()
			return cur != 0 && cur != oldSID
		}, 500*time.Millisecond, 10*time.Millisecond, "会话应完成轮换")

		// 等待超过一个 ReleaseRetryInterval 周期，验证旧会话项被直接丢弃，不再发起额外 Release RPC
		time.Sleep(120 * time.Millisecond)
		require.Equal(t, int32(1), releaseCalls.Load(), "会话轮换后队列应自动丢弃旧会话待释放项，不再发送 RPC")
	})

	t.Run("session expiry callback immediately cancels in-flight readAhead prefetch and purges releaseQueue", func(t *testing.T) {
		const step = 64 * 1024
		prefetchStarted := make(chan struct{})
		prefetchCanceled := make(chan struct{})
		var prefetchOnce sync.Once
		var releaseCalls atomic.Int32

		localSrv := &rwMockStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				// 赋予 80ms 初始短租约，以便通过 ResolveHandleSession 本地超期立即触发 OnSessionExpired
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(80 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			},
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				// 异步预取下一 4 MiB 窗口（Offset == step + 4 MiB）时模拟长耗时请求，等待被会话过期取消
				if req.Msg.GetOffset() == step+fusefs.MaxRPCChunkSize {
					prefetchOnce.Do(func() { close(prefetchStarted) })
					<-ctx.Done()
					close(prefetchCanceled)
					return nil, ctx.Err()
				}
				return connect.NewResponse(&storagev1.ReadResponse{
					Data: make([]byte, int(req.Msg.GetSize())),
				}), nil
			},
			releaseFn: func(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
				releaseCalls.Add(1)
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("network down"))
			},
		}
		localClient := setupRWTestRPCServer(t, localSrv)
		localMgr, err := session.New(context.Background(), localClient, session.Config{
			MinInterval: 10 * time.Second,
			MaxInterval: 20 * time.Second,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = localMgr.Close() })

		oldSID := localMgr.CurrentSessionID()
		rawFS := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{
			RootIno:              1001,
			ReleaseRetryInterval: 200 * time.Millisecond,
		})
		t.Cleanup(rawFS.OnUnmount)

		const readFH = uint64(301)
		const relFH = uint64(302)
		const lateRelFH = uint64(303)
		localMgr.BindHandle(readFH, oldSID)
		localMgr.BindHandle(relFH, oldSID)
		localMgr.BindHandle(lateRelFH, oldSID)

		// 1. 连续两次顺序读取触发 4 MiB 窗口拉取及下一 4 MiB 窗口异步预取（prefetchFuture）
		_, st1 := rawFS.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       readFH,
			Offset:   0,
			Size:     step,
		}, nil)
		require.Equal(t, fuse.OK, st1)

		_, st2 := rawFS.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       readFH,
			Offset:   step,
			Size:     step,
		}, nil)
		require.Equal(t, fuse.OK, st2)

		select {
		case <-prefetchStarted:
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for async prefetch to start")
		}

		// 2. 在 80ms 租约尚未到期前，先调用 Release(relFH) 使其因网络错误进入 releaseQueue（此时 ReleaseRetryInterval=200ms 尚未触发）
		rawFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       relFH,
		})
		require.Equal(t, int32(1), releaseCalls.Load())

		// 等待 90ms 让 80ms 租约在本地过期，调用 ResolveHandle 触发 OnSessionExpired 即时清理
		time.Sleep(90 * time.Millisecond)
		_, stExpired := rawFS.ResolveHandle(readFH)
		require.Equal(t, fuse.EBADF, stExpired)
		require.Equal(t, uint64(0), localMgr.CurrentSessionID())

		// 对已过期会话的 lateRelFH 调用 Release 必须直接清理本地状态并返回，绝不向服务端发起额外 Release RPC
		rawFS.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Fh:       lateRelFH,
		})
		require.Equal(t, int32(1), releaseCalls.Load(), "会话过期后对旧句柄调用 Release 绝不能再向服务端发起 Release RPC")

		// 3. 断言在途的 prefetchFuture 被立即取消（无需等待 30s defaultPrefetchTimeout 或 OnUnmount）
		select {
		case <-prefetchCanceled:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("expected in-flight prefetchFuture to be cancelled immediately on session expiry")
		}

		// 4. 等待超过 ReleaseRetryInterval (200ms)，断言 releaseQueue 中属于 oldSID 的 relFH 已被 onSessionExpired 立即移除，不再发起后台重试 RPC
		time.Sleep(160 * time.Millisecond)
		require.Equal(t, int32(1), releaseCalls.Load(), "onSessionExpired 必须立即清空 releaseQueue 中属于已过期会话的待重试项")
	})

	t.Run("server EBADF on session-bound RPC triggers immediate renewLoop wakeup via TriggerRenew", func(t *testing.T) {
		var renewCalls atomic.Int32
		localSrv := &handleMockStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				n := renewCalls.Add(1)
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Second)),
					SessionRecreated: n >= 2,
				}), nil
			},
			fsyncFn: func(ctx context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
				cerr := connect.NewError(connect.CodeFailedPrecondition, errors.New("session expired on server"))
				detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
					Reason: "EBADF",
					Domain: "bowl.storage",
				})
				require.NoError(t, err)
				cerr.AddDetail(detail)
				return nil, cerr
			},
		}
		localClient := setupHandleTestRPCServer(t, localSrv)
		localMgr, err := session.New(context.Background(), localClient, session.Config{
			MinInterval: 10 * time.Second,
			MaxInterval: 20 * time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, localMgr.Start(context.Background()))
		t.Cleanup(func() { _ = localMgr.Close() })

		oldSID := localMgr.CurrentSessionID()
		const fh = uint64(401)
		localMgr.BindHandle(fh, oldSID)

		rawFS := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{RootIno: 1001})
		t.Cleanup(rawFS.OnUnmount)

		require.Equal(t, int32(1), renewCalls.Load())

		// Fsync 收到服务端 EBADF 时立即唤醒续期循环。
		st := rawFS.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 2001}, Fh: fh})
		require.Equal(t, fuse.EBADF, st)

		require.Eventually(t, func() bool {
			cur := localMgr.CurrentSessionID()
			return renewCalls.Load() >= 3 && cur != 0 && cur != oldSID
		}, 1*time.Second, 10*time.Millisecond, "服务端返回 EBADF 后必须立即唤醒 renewLoop 并完成会话轮换")
	})

	t.Run("Create, Open, and OpenDir wait on AcquireSessionID when session is inactive, share new session, and respect RPCTimeout (EIO) and cancel (EINTR)", func(t *testing.T) {
		var mu sync.Mutex
		var renewSIDs []uint64
		allowNewSession := make(chan struct{})

		localSrv := &mockDirStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				sid := req.Msg.GetSessionId()
				renewSIDs = append(renewSIDs, sid)
				callNum := len(renewSIDs)
				mu.Unlock()

				if callNum == 1 {
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(40 * time.Millisecond)),
						SessionRecreated: false,
					}), nil
				}
				select {
				case <-allowNewSession:
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Second)),
						SessionRecreated: false,
					}), nil
				default:
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("network outage"))
				}
			},
			openDirFn: func(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
				return connect.NewResponse(&storagev1.OpenDirResponse{
					Fh: 5003,
				}), nil
			},
		}

		// 同时在同一个测试服务上挂载 Create 与 Open 处理
		mux := http.NewServeMux()
		var createSID, openSID, openDirSID uint64
		combinedSrv := &struct {
			*mockDirStorageServer
		}{mockDirStorageServer: localSrv}
		_ = combinedSrv

		handleSrv := &handleMockStorageServer{
			renewSessionFn: localSrv.renewSessionFn,
			createFn: func(ctx context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
				mu.Lock()
				createSID = req.Msg.GetSessionId()
				mu.Unlock()
				return connect.NewResponse(&storagev1.CreateResponse{
					Inode: &storagev1.Inode{Ino: 2001, Attr: &storagev1.InodeAttribute{Mode: 0o644 | syscall.S_IFREG}},
					Fh:    5001,
				}), nil
			},
			openFn: func(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
				mu.Lock()
				openSID = req.Msg.GetSessionId()
				mu.Unlock()
				return connect.NewResponse(&storagev1.OpenResponse{
					Fh: 5002,
				}), nil
			},
		}

		// 组合支持 Create/Open/OpenDir/RenewSession 的 mock handler
		fullSrv := &fullSessionTestServer{
			handleMockStorageServer: handleSrv,
			openDirFn: func(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
				mu.Lock()
				openDirSID = req.Msg.GetSessionId()
				mu.Unlock()
				return connect.NewResponse(&storagev1.OpenDirResponse{
					Fh: 5003,
				}), nil
			},
		}
		path, handler := storagev1connect.NewStorageServiceHandler(fullSrv)
		mux.Handle(path, handler)
		ts := httptest.NewServer(mux)
		t.Cleanup(ts.Close)
		localClient := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)

		localMgr, err := session.New(context.Background(), localClient, session.Config{
			MinInterval:         15 * time.Millisecond,
			MaxInterval:         25 * time.Millisecond,
			InitialRetryBackoff: 5 * time.Second,
			MaxRetryBackoff:     5 * time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, localMgr.Start(context.Background()))
		t.Cleanup(func() { _ = localMgr.Close() })

		oldSID := localMgr.CurrentSessionID()
		// 等待初始会话过期并使 CurrentSessionID 变为 0
		require.Eventually(t, func() bool {
			return localMgr.CurrentSessionID() == 0
		}, 1*time.Second, 5*time.Millisecond)

		rawFS := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{
			RootIno:    1001,
			RPCTimeout: 50 * time.Millisecond,
		})
		t.Cleanup(rawFS.OnUnmount)

		// 1. 断网持续超过 RPCTimeout (50ms) 时，Create / Open / OpenDir 等待 AcquireSessionID 超时返回 EIO
		var createOut fuse.CreateOut
		stCreateTimeout := rawFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
			Mode:     0o644,
		}, "timeout.txt", &createOut)
		require.Equal(t, fuse.Status(syscall.EIO), stCreateTimeout)

		// 2. FUSE cancel 通道关闭时，Open 等待 AcquireSessionID 立即中断返回 EINTR
		cancelCh := make(chan struct{})
		close(cancelCh)
		var openOut fuse.OpenOut
		stOpenCancel := rawFS.Open(cancelCh, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: 2001},
			Flags:    uint32(syscall.O_RDONLY),
		}, &openOut)
		require.Equal(t, fuse.Status(syscall.EINTR), stOpenCancel)

		// 3. 恢复网络后，并发发起 Create / Open / OpenDir：
		// 立即唤醒处于 5s 退避中的 renewLoop，并在新会话就绪后共享同一个新 session_id 成功完成调用
		rawFSWithNormalTimeout := fusefs.NewRawFS(localClient, localMgr, fusefs.Config{
			RootIno:    1001,
			RPCTimeout: 2 * time.Second,
		})
		t.Cleanup(rawFSWithNormalTimeout.OnUnmount)

		close(allowNewSession)

		var wg sync.WaitGroup
		wg.Add(3)
		var stC, stO, stOD fuse.Status
		go func() {
			defer wg.Done()
			var out fuse.CreateOut
			stC = rawFSWithNormalTimeout.Create(nil, &fuse.CreateIn{
				InHeader: fuse.InHeader{NodeId: 1},
				Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
				Mode:     0o644,
			}, "new.txt", &out)
		}()
		go func() {
			defer wg.Done()
			var out fuse.OpenOut
			stO = rawFSWithNormalTimeout.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: 2001},
				Flags:    uint32(syscall.O_RDONLY),
			}, &out)
		}()
		go func() {
			defer wg.Done()
			var out fuse.OpenOut
			stOD = rawFSWithNormalTimeout.OpenDir(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: 1},
			}, &out)
		}()
		wg.Wait()

		require.Equal(t, fuse.OK, stC)
		require.Equal(t, fuse.OK, stO)
		require.Equal(t, fuse.OK, stOD)

		newSID := localMgr.CurrentSessionID()
		require.NotZero(t, newSID)
		require.NotEqual(t, oldSID, newSID)

		mu.Lock()
		require.Equal(t, newSID, createSID)
		require.Equal(t, newSID, openSID)
		require.Equal(t, newSID, openDirSID)
		mu.Unlock()
	})
}

type fullSessionTestServer struct {
	*handleMockStorageServer
	openDirFn func(context.Context, *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error)
}

func (f *fullSessionTestServer) OpenDir(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
	if f.openDirFn != nil {
		return f.openDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

type adr0028LifecycleMockServer struct {
	*handleMockStorageServer
	lookupFn      func(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
	writeFn       func(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
	readFn        func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	openDirFn     func(context.Context, *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error)
	readDirFn     func(context.Context, *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error)
	readDirPlusFn func(context.Context, *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error)
	releaseDirFn  func(context.Context, *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error)
}

func (s *adr0028LifecycleMockServer) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	if s.lookupFn != nil {
		return s.lookupFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (s *adr0028LifecycleMockServer) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	if s.writeFn != nil {
		return s.writeFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (s *adr0028LifecycleMockServer) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	if s.readFn != nil {
		return s.readFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (s *adr0028LifecycleMockServer) OpenDir(ctx context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
	if s.openDirFn != nil {
		return s.openDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (s *adr0028LifecycleMockServer) ReadDir(ctx context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	if s.readDirFn != nil {
		return s.readDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (s *adr0028LifecycleMockServer) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	if s.readDirPlusFn != nil {
		return s.readDirPlusFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (s *adr0028LifecycleMockServer) ReleaseDir(ctx context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
	if s.releaseDirFn != nil {
		return s.releaseDirFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

// TestRawFS_ADR0028_WriteReadDirLifecycle_RPCCounts 验证：
//  1. 无 POSIX 记录锁活动时：
//     - 写全链路 Lookup(ENOENT) -> Create -> Write -> Flush -> Release：客户端 Flush 经 ToServerIno + ResolveHandle 校验后本地短路（Flush RPC 计数为 0），全链路向服务端精确发起 4 次 RPC（Lookup、Create、Write、Release）；
//     - 读全链路 Open -> Read -> Flush -> Release：客户端 Flush 本地短路（Flush RPC 计数为 0），全链路向服务端精确发起 3 次 RPC（Open、Read、Release），且正确透传 FOPEN_KEEP_CACHE；
//     - 目录全链路 OpenDir -> ReadDir / ReadDirPlus -> ReleaseDir：正确透传 FOPEN_CACHE_DIR，遍历目录项并关闭目录句柄；
//  2. POSIX 锁活动后 Flush 和文件锁仍全部在客户端本地完成。
func TestRawFS_ADR0028_WriteReadDirLifecycle_RPCCounts(t *testing.T) {
	var (
		lookupCount      atomic.Int64
		createCount      atomic.Int64
		writeCount       atomic.Int64
		openCount        atomic.Int64
		readCount        atomic.Int64
		flushCount       atomic.Int64
		releaseCount     atomic.Int64
		openDirCount     atomic.Int64
		readDirCount     atomic.Int64
		readDirPlusCount atomic.Int64
		releaseDirCount  atomic.Int64
		setLkCount       atomic.Int64
	)

	resetCounts := func() {
		lookupCount.Store(0)
		createCount.Store(0)
		writeCount.Store(0)
		openCount.Store(0)
		readCount.Store(0)
		flushCount.Store(0)
		releaseCount.Store(0)
		openDirCount.Store(0)
		readDirCount.Store(0)
		readDirPlusCount.Store(0)
		releaseDirCount.Store(0)
		setLkCount.Store(0)
	}

	payload4K := bytes.Repeat([]byte("ADR0028_CLIENT_4K_"), 256)[:4096]
	now := timestamppb.Now()

	baseSrv := &handleMockStorageServer{
		createFn: func(_ context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			createCount.Add(1)
			return connect.NewResponse(&storagev1.CreateResponse{
				Inode: &storagev1.Inode{
					Ino: 3001,
					Attr: &storagev1.InodeAttribute{
						Mode:  uint32(syscall.S_IFREG | 0o644),
						Nlink: 1,
						Atime: now,
						Mtime: now,
						Ctime: now,
					},
				},
				Fh:                  7001,
				OpenFlags:           fuse.FOPEN_KEEP_CACHE,
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  1.0,
			}), nil
		},
		openFn: func(_ context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			openCount.Add(1)
			return connect.NewResponse(&storagev1.OpenResponse{
				Fh:        7002,
				OpenFlags: fuse.FOPEN_KEEP_CACHE,
			}), nil
		},
		flushFn: func(_ context.Context, req *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
			flushCount.Add(1)
			return connect.NewResponse(&storagev1.FlushResponse{}), nil
		},
		releaseFn: func(_ context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			releaseCount.Add(1)
			return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
		},
		setLkFn: func(_ context.Context, req *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
			setLkCount.Add(1)
			return connect.NewResponse(&storagev1.SetLkResponse{}), nil
		},
	}

	mockSrv := &adr0028LifecycleMockServer{
		handleMockStorageServer: baseSrv,
		lookupFn: func(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			lookupCount.Add(1)
			return nil, connect.NewError(connect.CodeNotFound, errors.New("entry not found"))
		},
		writeFn: func(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
			writeCount.Add(1)
			return connect.NewResponse(&storagev1.WriteResponse{
				Size: uint32(len(req.Msg.GetData())),
			}), nil
		},
		readFn: func(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
			readCount.Add(1)
			return connect.NewResponse(&storagev1.ReadResponse{
				Data: payload4K,
			}), nil
		},
		openDirFn: func(_ context.Context, req *connect.Request[storagev1.OpenDirRequest]) (*connect.Response[storagev1.OpenDirResponse], error) {
			openDirCount.Add(1)
			return connect.NewResponse(&storagev1.OpenDirResponse{
				Fh:        8001,
				OpenFlags: fuse.FOPEN_CACHE_DIR,
			}), nil
		},
		readDirFn: func(_ context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
			readDirCount.Add(1)
			return connect.NewResponse(&storagev1.ReadDirResponse{
				Entries: []*storagev1.DirEntry{
					{Ino: 1001, Name: []byte("."), Mode: uint32(syscall.S_IFDIR | 0o755), NextOffset: 1},
					{Ino: 1001, Name: []byte(".."), Mode: uint32(syscall.S_IFDIR | 0o755), NextOffset: 2},
					{Ino: 3001, Name: []byte("file.bin"), Mode: uint32(syscall.S_IFREG | 0o644), NextOffset: 3},
				},
			}), nil
		},
		readDirPlusFn: func(_ context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
			readDirPlusCount.Add(1)
			return connect.NewResponse(&storagev1.ReadDirPlusResponse{
				Entries: []*storagev1.DirEntryPlus{
					{Ino: 1001, Name: []byte("."), NextOffset: 1},
					{Ino: 1001, Name: []byte(".."), NextOffset: 2},
					{
						Ino:        3001,
						Name:       []byte("file.bin"),
						NextOffset: 3,
						Inode: &storagev1.Inode{
							Ino: 3001,
							Attr: &storagev1.InodeAttribute{
								Mode:  uint32(syscall.S_IFREG | 0o644),
								Size:  4096,
								Nlink: 1,
								Atime: now,
								Mtime: now,
								Ctime: now,
							},
						},
						EntryTimeoutSeconds: 1.0,
						AttrTimeoutSeconds:  1.0,
					},
				},
			}), nil
		},
		releaseDirFn: func(_ context.Context, req *connect.Request[storagev1.ReleaseDirRequest]) (*connect.Response[storagev1.ReleaseDirResponse], error) {
			releaseDirCount.Add(1)
			return connect.NewResponse(&storagev1.ReleaseDirResponse{}), nil
		},
	}

	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(mockSrv)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
	mgr := setupTestSessionManager(t, client)
	rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
	t.Cleanup(rawFS.OnUnmount)

	// 1.1 无 POSIX 锁活动时的写全链路：Lookup(ENOENT) -> Create -> Write -> Flush -> Release（4 次服务端 RPC，Flush 本地短路）
	resetCounts()
	var lookupOut fuse.EntryOut
	stLookup := rawFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file.bin", &lookupOut)
	require.Equal(t, fuse.Status(syscall.ENOENT), stLookup)

	var createOut fuse.CreateOut
	stCreate := rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_CREAT | syscall.O_RDWR),
		Mode:     0o644,
	}, "file.bin", &createOut)
	require.Equal(t, fuse.OK, stCreate)
	require.Equal(t, uint64(3001), createOut.NodeId)
	require.Equal(t, uint64(7001), createOut.Fh)
	require.Equal(t, uint32(fuse.FOPEN_KEEP_CACHE), createOut.OpenFlags)

	written, stWrite := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
		Size:     uint32(len(payload4K)),
	}, payload4K)
	require.Equal(t, fuse.OK, stWrite)
	require.Equal(t, uint32(4096), written)

	stFlushWrite := rawFS.Flush(nil, &fuse.FlushIn{
		InHeader:  fuse.InHeader{NodeId: createOut.NodeId},
		Fh:        createOut.Fh,
		LockOwner: 0xAA01,
	})
	require.Equal(t, fuse.OK, stFlushWrite)
	require.Equal(t, int64(0), flushCount.Load(), "无 POSIX 锁活动时客户端 Flush 必须本地短路，不发起服务端 Flush RPC")

	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	totalWriteRPCs := lookupCount.Load() + createCount.Load() + writeCount.Load() + flushCount.Load() + releaseCount.Load()
	require.Equal(t, int64(1), lookupCount.Load())
	require.Equal(t, int64(1), createCount.Load())
	require.Equal(t, int64(1), writeCount.Load())
	require.Equal(t, int64(0), flushCount.Load())
	require.Equal(t, int64(1), releaseCount.Load())
	require.Equal(t, int64(4), totalWriteRPCs, "无 POSIX 锁活动时写全链路必须精确向服务端发起 4 次 RPC（Lookup、Create、Write、Release）")

	// 1.2 无 POSIX 锁活动时的读全链路：Open -> Read -> Flush -> Release（3 次服务端 RPC，Flush 本地短路且透传 FOPEN_KEEP_CACHE）
	resetCounts()
	var openOut fuse.OpenOut
	stOpen := rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut)
	require.Equal(t, fuse.OK, stOpen)
	require.Equal(t, uint64(7002), openOut.Fh)
	require.Equal(t, uint32(fuse.FOPEN_KEEP_CACHE), openOut.OpenFlags, "Open 必须正确透传 FOPEN_KEEP_CACHE")

	readBuf := make([]byte, 4096)
	readRes, stRead := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     4096,
	}, readBuf)
	require.Equal(t, fuse.OK, stRead)
	resBytes, stBytes := readRes.Bytes(readBuf)
	require.Equal(t, fuse.OK, stBytes)
	require.Equal(t, payload4K, resBytes)

	stFlushRead := rawFS.Flush(nil, &fuse.FlushIn{
		InHeader:  fuse.InHeader{NodeId: 3001},
		Fh:        openOut.Fh,
		LockOwner: 0xAA02,
	})
	require.Equal(t, fuse.OK, stFlushRead)
	require.Equal(t, int64(0), flushCount.Load(), "无 POSIX 锁活动时读生命周期客户端 Flush 必须本地短路")

	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Fh:       openOut.Fh,
	})

	totalReadRPCs := openCount.Load() + readCount.Load() + flushCount.Load() + releaseCount.Load()
	require.Equal(t, int64(1), openCount.Load())
	require.Equal(t, int64(1), readCount.Load())
	require.Equal(t, int64(0), flushCount.Load())
	require.Equal(t, int64(1), releaseCount.Load())
	require.Equal(t, int64(3), totalReadRPCs, "无 POSIX 锁活动时读全链路必须精确向服务端发起 3 次 RPC（Open、Read、Release）")

	// 1.3 目录全链路：OpenDir -> ReadDir / ReadDirPlus -> ReleaseDir（透传 FOPEN_CACHE_DIR，遍历目录项并关闭目录句柄）
	resetCounts()
	var openDirOut fuse.OpenOut
	stOpenDir := rawFS.OpenDir(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 1},
	}, &openDirOut)
	require.Equal(t, fuse.OK, stOpenDir)
	require.Equal(t, uint64(8001), openDirOut.Fh)
	require.Equal(t, uint32(fuse.FOPEN_CACHE_DIR), openDirOut.OpenFlags, "OpenDir 必须正确透传 FOPEN_CACHE_DIR")

	dirBuf := make([]byte, 4096)
	dirList := fuse.NewDirEntryList(dirBuf, 0)
	stReadDir := rawFS.ReadDir(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       openDirOut.Fh,
		Offset:   0,
		Size:     4096,
	}, dirList)
	require.Equal(t, fuse.OK, stReadDir)

	dirPlusBuf := make([]byte, 4096)
	dirPlusList := fuse.NewDirEntryList(dirPlusBuf, 0)
	stReadDirPlus := rawFS.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       openDirOut.Fh,
		Offset:   0,
		Size:     4096,
	}, dirPlusList)
	require.Equal(t, fuse.OK, stReadDirPlus)

	rawFS.ReleaseDir(&fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       openDirOut.Fh,
	})

	require.Equal(t, int64(1), openDirCount.Load())
	require.Equal(t, int64(1), readDirCount.Load())
	require.Equal(t, int64(1), readDirPlusCount.Load())
	require.Equal(t, int64(1), releaseDirCount.Load())

	// 2. POSIX 锁活动不增加文件操作的 RPC。
	// 2.1 在 Inode 3001 上开启句柄并调用一次 POSIX SetLk
	var lkOpenOut fuse.OpenOut
	require.Equal(t, fuse.OK, rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Flags:    uint32(syscall.O_RDWR),
	}, &lkOpenOut))
	require.Equal(t, fuse.OK, rawFS.SetLk(nil, &fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Fh:       lkOpenOut.Fh,
		Owner:    0xBB01,
		Lk: fuse.FileLock{
			Start: 0,
			End:   127,
			Typ:   uint32(syscall.F_WRLCK),
			Pid:   1001,
		},
		LkFlags: 0, // POSIX 记录锁
	}))
	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Fh:       lkOpenOut.Fh,
	})

	// 2.2 触发 POSIX 锁后，写全链路仍为 4 次服务端 RPC。
	resetCounts()
	require.Equal(t, fuse.Status(syscall.ENOENT), rawFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file.bin", &lookupOut))
	require.Equal(t, fuse.OK, rawFS.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    uint32(syscall.O_CREAT | syscall.O_RDWR),
		Mode:     0o644,
	}, "file.bin", &createOut))
	_, stWriteAfterLk := rawFS.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
		Offset:   0,
		Size:     uint32(len(payload4K)),
	}, payload4K)
	require.Equal(t, fuse.OK, stWriteAfterLk)
	require.Equal(t, fuse.OK, rawFS.Flush(nil, &fuse.FlushIn{
		InHeader:  fuse.InHeader{NodeId: createOut.NodeId},
		Fh:        createOut.Fh,
		LockOwner: 0xBB01,
	}))
	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Fh:       createOut.Fh,
	})

	totalWriteRPCsAfterLk := lookupCount.Load() + createCount.Load() + writeCount.Load() + flushCount.Load() + releaseCount.Load()
	require.Zero(t, flushCount.Load(), "发生 POSIX 锁活动后 Flush 仍在本地完成")
	require.Equal(t, int64(4), totalWriteRPCsAfterLk)

	// 2.3 触发 POSIX 锁后，读全链路仍为 3 次服务端 RPC。
	resetCounts()
	require.Equal(t, fuse.OK, rawFS.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Flags:    uint32(syscall.O_RDONLY),
	}, &openOut))
	_, stReadAfterLk := rawFS.Read(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     4096,
	}, readBuf)
	require.Equal(t, fuse.OK, stReadAfterLk)
	require.Equal(t, fuse.OK, rawFS.Flush(nil, &fuse.FlushIn{
		InHeader:  fuse.InHeader{NodeId: 3001},
		Fh:        openOut.Fh,
		LockOwner: 0xBB02,
	}))
	rawFS.Release(nil, &fuse.ReleaseIn{
		InHeader: fuse.InHeader{NodeId: 3001},
		Fh:       openOut.Fh,
	})

	totalReadRPCsAfterLk := openCount.Load() + readCount.Load() + flushCount.Load() + releaseCount.Load()
	require.Zero(t, flushCount.Load(), "发生 POSIX 锁活动后 Flush 仍在本地完成")
	require.Equal(t, int64(3), totalReadRPCsAfterLk)
}
