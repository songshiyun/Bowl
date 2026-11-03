package fusefs_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type handleMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	getAttrFn      func(context.Context, *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error)
	createFn       func(context.Context, *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error)
	openFn         func(context.Context, *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error)
	flushFn        func(context.Context, *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error)
	fsyncFn        func(context.Context, *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error)
	releaseFn      func(context.Context, *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error)
	setLkFn        func(context.Context, *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error)
	setLkwFn       func(context.Context, *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *handleMockStorageServer) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	if m.getAttrFn != nil {
		return m.getAttrFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{Inode: &storagev1.Inode{Ino: req.Msg.GetIno(), Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644}}}), nil
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
		require.NotZero(t, out.OpenOut.Fh)
		require.NotEqual(t, uint64(42), out.OpenOut.Fh)
		require.True(t, (out.OpenOut.OpenFlags&fuse.FOPEN_DIRECT_IO) != 0, "DirectIO 必须附加 FOPEN_DIRECT_IO")

		// 验证句柄会话血缘已绑定
		sid, err := mgr.ResolveHandleSession(out.Fh)
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

func TestRawFS_本地句柄打开关闭与同步(t *testing.T) {
	var getAttrs, opens, releases, flushes atomic.Int32
	var syncRequests []*storagev1.FsyncRequest
	mockSrv := &handleMockStorageServer{
		getAttrFn: func(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
			getAttrs.Add(1)
			return connect.NewResponse(&storagev1.GetAttrResponse{Inode: &storagev1.Inode{Ino: req.Msg.GetIno(), Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644}}}), nil
		},
		openFn: func(_ context.Context, _ *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			opens.Add(1)
			return connect.NewResponse(&storagev1.OpenResponse{}), nil
		},
		releaseFn: func(_ context.Context, _ *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
			releases.Add(1)
			return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
		},
		flushFn: func(_ context.Context, _ *connect.Request[storagev1.FlushRequest]) (*connect.Response[storagev1.FlushResponse], error) {
			flushes.Add(1)
			return connect.NewResponse(&storagev1.FlushResponse{}), nil
		},
		fsyncFn: func(_ context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
			syncRequests = append(syncRequests, req.Msg)
			return connect.NewResponse(&storagev1.FsyncResponse{}), nil
		},
	}
	client := setupHandleTestRPCServer(t, mockSrv)
	mgr := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001, DirectIO: true})
	t.Cleanup(fs.OnUnmount)
	input := &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 2002}, Flags: syscall.O_RDWR | syscall.O_APPEND}
	var first, second fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, input, &first))
	require.Equal(t, fuse.OK, fs.Open(nil, input, &second))
	require.NotEqual(t, first.Fh, second.Fh)
	require.Equal(t, int32(1), getAttrs.Load())
	require.Zero(t, opens.Load())
	require.NotZero(t, first.OpenFlags&fuse.FOPEN_DIRECT_IO)
	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 2002}, Fh: first.Fh, LockOwner: 777}))
	require.Zero(t, flushes.Load())
	require.Equal(t, fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 2002}, Fh: first.Fh}))
	require.Equal(t, fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 2002}, Fh: first.Fh, FsyncFlags: 1}))
	require.Len(t, syncRequests, 2)
	require.Zero(t, syncRequests[0].GetFh())
	require.False(t, syncRequests[0].GetDatasync())
	require.True(t, syncRequests[1].GetDatasync())
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 2002}, Fh: first.Fh})
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 2002}, Fh: second.Fh})
	require.Zero(t, releases.Load())
	_, status := fs.ResolveHandle(first.Fh)
	require.Equal(t, fuse.EBADF, status)

	readOnly := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001, ReadOnly: true})
	t.Cleanup(readOnly.OnUnmount)
	require.Equal(t, fuse.Status(syscall.EROFS), readOnly.Open(nil, input, &fuse.OpenOut{}))
	require.Equal(t, fuse.OK, readOnly.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 2002}, Flags: syscall.O_RDONLY}, &fuse.OpenOut{}))
}
