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
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type rwMockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	readFn         func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	writeFn        func(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
	setAttrFn      func(context.Context, *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error)
	fallocateFn    func(context.Context, *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error)
	openFn         func(context.Context, *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error)
	releaseFn      func(context.Context, *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error)
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
}

func (m *rwMockStorageServer) GetAttr(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	return connect.NewResponse(&storagev1.GetAttrResponse{Inode: &storagev1.Inode{Ino: req.Msg.GetIno(), Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644}}}), nil
}

func (m *rwMockStorageServer) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	if m.readFn != nil {
		return m.readFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *rwMockStorageServer) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	if m.writeFn != nil {
		return m.writeFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *rwMockStorageServer) SetAttr(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	if m.setAttrFn != nil {
		return m.setAttrFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode: &storagev1.Inode{Ino: req.Msg.GetIno(), Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644}},
	}), nil
}

func (m *rwMockStorageServer) Fallocate(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	if m.fallocateFn != nil {
		return m.fallocateFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (m *rwMockStorageServer) Open(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
	if m.openFn != nil {
		return m.openFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.OpenResponse{Fh: 77777}), nil
}

func (m *rwMockStorageServer) Release(ctx context.Context, req *connect.Request[storagev1.ReleaseRequest]) (*connect.Response[storagev1.ReleaseResponse], error) {
	if m.releaseFn != nil {
		return m.releaseFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.ReleaseResponse{}), nil
}

func (m *rwMockStorageServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	if m.renewSessionFn != nil {
		return m.renewSessionFn(ctx, req)
	}
	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt: timestamppb.New(time.Now().Add(60 * time.Second)),
	}), nil
}

func setupRWTestRPCServer(t *testing.T, srv *rwMockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_Read_ChunkingAndBoundaries(t *testing.T) {
	const (
		diskID  = "00000000-0000-0000-0000-000000000001"
		rootIno = 1001
		fileIno = 2002
		testFH  = 12345
	)

	t.Run("9 MiB 大块读取精确 3 次分片 (4 MiB + 4 MiB + 1 MiB)", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024 // 9 MiB
		pattern := make([]byte, totalSize)
		for i := range pattern {
			pattern[i] = byte(i % 251)
		}

		type callRecord struct {
			ino    uint64
			fh     uint64
			offset uint64
			size   uint32
			sid    uint64
		}
		var calls []callRecord

		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				msg := req.Msg
				var sid uint64
				if msg.SessionId != nil {
					sid = *msg.SessionId
				}
				calls = append(calls, callRecord{
					ino:    msg.GetIno(),
					fh:     msg.GetFh(),
					offset: msg.GetOffset(),
					size:   msg.GetSize(),
					sid:    sid,
				})

				start := int(msg.GetOffset())
				end := start + int(msg.GetSize())
				if end > len(pattern) {
					end = len(pattern)
				}
				return connect.NewResponse(&storagev1.ReadResponse{
					Data: pattern[start:end],
				}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		curSID := mgr.CurrentSessionID()
		mgr.BindHandle(testFH, curSID)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:  diskID,
			RootIno: rootIno,
		})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     totalSize,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, res)
		require.Equal(t, totalSize, res.Size())

		buf, code := res.Bytes(nil)
		require.Equal(t, fuse.OK, code)
		require.True(t, bytes.Equal(pattern, buf))

		// 验证精确 3 次 RPC 调用
		require.Len(t, calls, 3)
		require.Equal(t, callRecord{ino: fileIno, fh: 0, offset: 0, size: 4 * 1024 * 1024, sid: curSID}, calls[0])
		require.Equal(t, callRecord{ino: fileIno, fh: 0, offset: 4 * 1024 * 1024, size: 4 * 1024 * 1024, sid: curSID}, calls[1])
		require.Equal(t, callRecord{ino: fileIno, fh: 0, offset: 8 * 1024 * 1024, size: 1 * 1024 * 1024, sid: curSID}, calls[2])
	})

	t.Run("<= 4 MiB 单次直通读取并复用传入 buf", func(t *testing.T) {
		const readSize = 64 * 1024
		data := bytes.Repeat([]byte("BOWL_DATA_"), readSize/10)

		var calls int
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				calls++
				require.Equal(t, uint64(fileIno), req.Msg.GetIno())
				require.Zero(t, req.Msg.GetFh())
				require.Equal(t, uint64(1024), req.Msg.GetOffset())
				require.Equal(t, uint32(readSize), req.Msg.GetSize())
				return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		buf := make([]byte, readSize)
		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   1024,
			Size:     readSize,
		}

		res, status := fs.Read(nil, input, buf)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, 1, calls)
		require.Equal(t, len(data), res.Size())

		resBytes, code := res.Bytes(nil)
		require.Equal(t, fuse.OK, code)
		require.Equal(t, data, resBytes)
	})

	t.Run("want == 0 直接返回空数据不触发 RPC", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				t.Fatal("want == 0 不应触发 RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     0,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.NotNil(t, res)
		require.Equal(t, 0, res.Size())
	})

	t.Run("len(buf) > 0 且小于 input.Size 时以 len(buf) 为准截断", func(t *testing.T) {
		var reqSize uint32
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				reqSize = req.Msg.GetSize()
				return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, reqSize)}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		buf := make([]byte, 512)
		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     4096, // input.Size 较大，但 buf 仅 512
		}

		res, status := fs.Read(nil, input, buf)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(512), reqSize)
		require.Equal(t, 512, res.Size())
	})

	t.Run("中途短读与遇到 EOF 立即停止分片循环", func(t *testing.T) {
		// 请求 9 MiB，第 1 片返回 4 MiB，第 2 片返回 2 MiB (< 4 MiB 短读/EOF)
		var calls int
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				calls++
				if calls == 1 {
					return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, 4*1024*1024)}), nil
				}
				if calls == 2 {
					return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, 2*1024*1024)}), nil
				}
				t.Fatal("第 2 片短读后不应再触发第 3 次 RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     9 * 1024 * 1024,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, 2, calls)
		require.Equal(t, 6*1024*1024, res.Size())
	})

	t.Run("中途分片报错但 totalRead > 0 返回已读数据与 fuse.OK", func(t *testing.T) {
		var calls int
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				calls++
				if calls == 1 {
					return connect.NewResponse(&storagev1.ReadResponse{Data: make([]byte, 4*1024*1024)}), nil
				}
				return nil, makeStorageError(connect.CodeInternal, "EIO", "i/o error")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     9 * 1024 * 1024,
		}

		res, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, 2, calls)
		require.Equal(t, 4*1024*1024, res.Size())
	})

	t.Run("首片读取失败返回对应 errno", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, makeStorageError(connect.CodeInternal, "EIO", "disk read failure")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     1024,
		}

		_, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.Status(syscall.EIO), status)
	})

	t.Run("无效句柄与未绑定句柄返回 EBADF", func(t *testing.T) {
		client := setupRWTestRPCServer(t, &rwMockStorageServer{})
		mgr := setupTestSessionManager(t, client)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       999999, // 未绑定
			Offset:   0,
			Size:     1024,
		}

		_, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("只写句柄在本地拒绝读取", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				return nil, makeStorageError(connect.CodeFailedPrecondition, "EBADF", "bad file descriptor")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_WRONLY, false))

		input := &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     1024,
		}

		_, status := fs.Read(nil, input, nil)
		require.Equal(t, fuse.EBADF, status)
	})
}

func TestRawFS_Write_ChunkingAndBoundaries(t *testing.T) {
	const (
		diskID  = "00000000-0000-0000-0000-000000000001"
		rootIno = 1001
		fileIno = 2002
		testFH  = 12345
	)

	t.Run("只读模式直接返回 EROFS", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				t.Fatal("只读模式不应调用 Write RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:   diskID,
			RootIno:  rootIno,
			ReadOnly: true, // 只读挂载
		})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDONLY, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, []byte("hello"))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.Status(syscall.EROFS), status)
	})

	t.Run("无效句柄返回 EBADF", func(t *testing.T) {
		client := setupRWTestRPCServer(t, &rwMockStorageServer{})
		mgr := setupTestSessionManager(t, client)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       999999, // 未绑定
			Offset:   0,
		}

		written, status := fs.Write(nil, input, []byte("hello"))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("0 字节写入透传服务端返回 0 与 fuse.OK", func(t *testing.T) {
		var called bool
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				called = true
				require.Equal(t, uint64(fileIno), req.Msg.GetIno())
				require.Zero(t, req.Msg.GetFh())
				require.Equal(t, 0, len(req.Msg.GetData()))
				return connect.NewResponse(&storagev1.WriteResponse{Size: 0}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   100,
		}

		written, status := fs.Write(nil, input, []byte{})
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(0), written)
		require.True(t, called)
	})

	t.Run("<= 4 MiB 单次写入直接返回", func(t *testing.T) {
		data := []byte("Hello Bowl Direct IO")
		var called bool
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				called = true
				require.Equal(t, uint64(fileIno), req.Msg.GetIno())
				require.Zero(t, req.Msg.GetFh())
				require.Equal(t, uint64(50), req.Msg.GetOffset())
				require.Equal(t, data, req.Msg.GetData())
				return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(data))}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   50,
		}

		written, status := fs.Write(nil, input, data)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(len(data)), written)
		require.True(t, called)
	})

	t.Run("9 MiB 大块写入精确 3 次分片 (4 MiB + 4 MiB + 1 MiB)", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024 // 9 MiB
		pattern := make([]byte, totalSize)
		for i := range pattern {
			pattern[i] = byte(i % 253)
		}

		type callRecord struct {
			ino     uint64
			fh      uint64
			offset  uint64
			dataLen int
			sid     uint64
		}
		var calls []callRecord

		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				msg := req.Msg
				var sid uint64
				if msg.SessionId != nil {
					sid = *msg.SessionId
				}
				calls = append(calls, callRecord{
					ino:     msg.GetIno(),
					fh:      msg.GetFh(),
					offset:  msg.GetOffset(),
					dataLen: len(msg.GetData()),
					sid:     sid,
				})
				return connect.NewResponse(&storagev1.WriteResponse{
					Size: uint32(len(msg.GetData())),
				}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		curSID := mgr.CurrentSessionID()
		mgr.BindHandle(testFH, curSID)

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   100,
		}

		written, status := fs.Write(nil, input, pattern)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(totalSize), written)

		// 验证精确 3 次分片与偏移计算
		require.Len(t, calls, 3)
		require.Equal(t, callRecord{ino: fileIno, fh: 0, offset: 100, dataLen: 4 * 1024 * 1024, sid: curSID}, calls[0])
		require.Equal(t, callRecord{ino: fileIno, fh: 0, offset: 100 + 4*1024*1024, dataLen: 4 * 1024 * 1024, sid: curSID}, calls[1])
		require.Equal(t, callRecord{ino: fileIno, fh: 0, offset: 100 + 8*1024*1024, dataLen: 1 * 1024 * 1024, sid: curSID}, calls[2])
	})

	t.Run("第 3 片写入返回 ENOSPC 时返回前 8 MiB 短写与 fuse.OK", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024
		data := make([]byte, totalSize)

		var calls int
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				calls++
				if calls == 1 || calls == 2 {
					return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(req.Msg.GetData()))}), nil
				}
				// 第 3 片报错 ENOSPC
				return nil, makeStorageError(connect.CodeResourceExhausted, "ENOSPC", "disk quota exceeded")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, data)
		require.Equal(t, fuse.OK, status, "POSIX 短写在已有数据写入成功时必须返回 fuse.OK")
		require.Equal(t, uint32(8*1024*1024), written, "应返回前 2 片累计成功写入字节数 (8 MiB)")
		require.Equal(t, 3, calls)
	})

	t.Run("首片写入失败返回对应 errno 与 0 字节", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				return nil, makeStorageError(connect.CodeResourceExhausted, "ENOSPC", "disk quota exceeded")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, make([]byte, 1024))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.Status(syscall.ENOSPC), status)
	})

	t.Run("某片返回 written < len(chunk) 时立即停止分片循环", func(t *testing.T) {
		const totalSize = 9 * 1024 * 1024
		data := make([]byte, totalSize)

		var calls int
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				calls++
				if calls == 1 {
					// 仅写入 2 MiB (< 4 MiB)
					return connect.NewResponse(&storagev1.WriteResponse{Size: 2 * 1024 * 1024}), nil
				}
				t.Fatal("第 1 片短写后不应再触发后续 RPC")
				return nil, nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, data)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(2*1024*1024), written)
		require.Equal(t, 1, calls)
	})

	t.Run("只读句柄在本地拒绝写入", func(t *testing.T) {
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				return nil, makeStorageError(connect.CodeFailedPrecondition, "EBADF", "bad file descriptor")
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDONLY, false))

		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		written, status := fs.Write(nil, input, []byte("data"))
		require.Equal(t, uint32(0), written)
		require.Equal(t, fuse.EBADF, status)
	})

	t.Run("Write 发起前 cancel 已关闭返回 EINTR，在途发出后忽略中途 cancel 正常完成避免重复追加", func(t *testing.T) {
		var rpcCalls int
		inFlightCancel := make(chan struct{})
		mockSrv := &rwMockStorageServer{
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				rpcCalls++
				close(inFlightCancel)
				if err := ctx.Err(); err != nil {
					return nil, err
				}
				return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(req.Msg.GetData()))}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))
		input := &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}

		preCanceled := make(chan struct{})
		close(preCanceled)
		writtenPre, stPre := fs.Write(preCanceled, input, []byte("hello"))
		require.Equal(t, uint32(0), writtenPre)
		require.Equal(t, fuse.Status(syscall.EINTR), stPre)
		require.Equal(t, 0, rpcCalls, "发起前已取消不得调用服务端 Write RPC")

		writtenInFlight, stInFlight := fs.Write(inFlightCancel, input, []byte("hello"))
		require.Equal(t, fuse.OK, stInFlight)
		require.Equal(t, uint32(5), writtenInFlight)
		require.Equal(t, 1, rpcCalls)
	})
}

func TestRawFS_Read_SequentialReadAheadAndInvalidation(t *testing.T) {
	const (
		diskID  = "00000000-0000-0000-0000-000000000001"
		rootIno = 1001
		fileIno = 2002
		testFH  = 12345
	)

	t.Run("连续顺序读取 16 MiB (1 MiB 步长) 触发 4 MiB 对齐拉取与异步预读且命中内存缓存", func(t *testing.T) {
		const totalSize = 16 * 1024 * 1024 // 16 MiB
		const stepSize = 1 * 1024 * 1024   // 1 MiB
		fileData := make([]byte, totalSize)
		for i := range fileData {
			fileData[i] = byte((i*13 + 7) % 251)
		}

		var rpcCalls atomic.Int32
		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				rpcCalls.Add(1)
				off := int(req.Msg.GetOffset())
				sz := int(req.Msg.GetSize())
				if off >= len(fileData) {
					return connect.NewResponse(&storagev1.ReadResponse{Data: nil}), nil
				}
				end := min(off+sz, len(fileData))
				return connect.NewResponse(&storagev1.ReadResponse{
					Data: fileData[off:end],
				}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))
		t.Cleanup(fs.OnUnmount)

		got := make([]byte, 0, totalSize)
		for off := uint64(0); off < totalSize; off += stepSize {
			res, status := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileIno},
				Fh:       testFH,
				Offset:   off,
				Size:     stepSize,
			}, nil)
			require.Equal(t, fuse.OK, status)
			chunk, code := res.Bytes(nil)
			require.Equal(t, fuse.OK, code)
			require.Len(t, chunk, stepSize)
			got = append(got, chunk...)
		}

		require.True(t, bytes.Equal(fileData, got), "顺序预读拼装出的 16 MiB 字节流必须与源文件完全一致")
		// 16 次 1 MiB 顺序读：第 1 次 1 MiB 直通 + 后续按 4 MiB 窗口拉取/异步预取，总 RPC 次数远少于 16 次（<= 6 次）
		require.LessOrEqual(t, int(rpcCalls.Load()), 6, "4 MiB 预读窗口应大幅减少跨机 Read RPC 次数")
	})

	t.Run("非连续随机读保持按请求尺寸直通读取不产生多余读放大", func(t *testing.T) {
		const reqSize = 64 * 1024
		var mu sync.Mutex
		var observedSizes []uint32

		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				mu.Lock()
				observedSizes = append(observedSizes, req.Msg.GetSize())
				mu.Unlock()
				return connect.NewResponse(&storagev1.ReadResponse{
					Data: bytes.Repeat([]byte{0xAB}, int(req.Msg.GetSize())),
				}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))
		t.Cleanup(fs.OnUnmount)

		randomOffsets := []uint64{0, 1024 * 1024, 512 * 1024, 8 * 1024 * 1024, 256 * 1024}
		for _, off := range randomOffsets {
			res, status := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileIno},
				Fh:       testFH,
				Offset:   off,
				Size:     reqSize,
			}, nil)
			require.Equal(t, fuse.OK, status)
			require.Equal(t, reqSize, res.Size())
		}

		mu.Lock()
		defer mu.Unlock()
		require.Len(t, observedSizes, len(randomOffsets))
		for i, sz := range observedSizes {
			require.Equal(t, uint32(reqSize), sz, "随机读取 #%d 必须严格按请求尺寸直通而不放大为 4 MiB", i)
		}
	})

	t.Run("Write / SetAttr / Fallocate / Open(O_TRUNC) / Release 立即同步失效预读缓冲", func(t *testing.T) {
		const fileSize = 8 * 1024 * 1024
		const readStep = 512 * 1024

		var version atomic.Int32
		version.Store(1)
		var rpcCalls atomic.Int32

		mockSrv := &rwMockStorageServer{
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				rpcCalls.Add(1)
				v := byte(version.Load())
				sz := int(req.Msg.GetSize())
				return connect.NewResponse(&storagev1.ReadResponse{
					Data: bytes.Repeat([]byte{v}, sz),
				}), nil
			},
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				version.Add(1)
				return connect.NewResponse(&storagev1.WriteResponse{
					Size: uint32(len(req.Msg.GetData())),
				}), nil
			},
			setAttrFn: func(ctx context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
				version.Add(1)
				return connect.NewResponse(&storagev1.SetAttrResponse{
					Inode: &storagev1.Inode{Ino: req.Msg.GetIno(), Attr: &storagev1.InodeAttribute{Size: fileSize, Mode: syscall.S_IFREG | 0o644}},
				}), nil
			},
			fallocateFn: func(ctx context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
				version.Add(1)
				return connect.NewResponse(&storagev1.FallocateResponse{}), nil
			},
			openFn: func(ctx context.Context, req *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
				version.Add(1)
				return connect.NewResponse(&storagev1.OpenResponse{Fh: testFH + 1}), nil
			},
		}

		client := setupRWTestRPCServer(t, mockSrv)
		mgr := setupTestSessionManager(t, client)
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		fs := fusefs.NewRawFS(client, mgr, fusefs.Config{DiskID: diskID, RootIno: rootIno})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))
		t.Cleanup(fs.OnUnmount)

		// 辅助函数：通过两次连续顺序读填充 4 MiB 预读缓冲，并验证第 3 次顺序读命中缓存（版本号仍为 expectedVer）
		populateAndVerifyCacheHit := func(expectedVer byte) {
			res1, st1 := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileIno},
				Fh:       testFH,
				Offset:   0,
				Size:     readStep,
			}, nil)
			require.Equal(t, fuse.OK, st1)
			b1, _ := res1.Bytes(nil)
			require.Equal(t, expectedVer, b1[0])

			res2, st2 := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileIno},
				Fh:       testFH,
				Offset:   readStep,
				Size:     readStep,
			}, nil)
			require.Equal(t, fuse.OK, st2)
			b2, _ := res2.Bytes(nil)
			require.Equal(t, expectedVer, b2[0])

			callsBefore := rpcCalls.Load()
			res3, st3 := fs.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: fileIno},
				Fh:       testFH,
				Offset:   2 * readStep,
				Size:     readStep,
			}, nil)
			require.Equal(t, fuse.OK, st3)
			b3, _ := res3.Bytes(nil)
			require.Equal(t, expectedVer, b3[0])
			require.Equal(t, callsBefore, rpcCalls.Load(), "第 3 次连续读取必须直接命中客户端预读缓冲而不发起同步 RPC")
		}

		// 1. 填充预读缓冲（version = 1），然后执行 Write 触发失效
		populateAndVerifyCacheHit(1)
		_, wSt := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}, []byte("update"))
		require.Equal(t, fuse.OK, wSt)

		// Write 后继续读取 3 * readStep：必须重新向服务端拉取最新数据（version = 2），绝不能返回旧缓存 version = 1
		resAfterWrite, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   3 * readStep,
			Size:     readStep,
		}, nil)
		require.Equal(t, fuse.OK, st)
		bAfterWrite, _ := resAfterWrite.Bytes(nil)
		require.Equal(t, byte(2), bAfterWrite[0], "Write 后必须同步失效预读缓存并读到最新数据")

		// 2. 重新填充预读缓冲（version = 2），然后执行 SetAttr (FATTR_SIZE) 触发失效
		populateAndVerifyCacheHit(2)
		var attrOut fuse.AttrOut
		saSt := fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: fileIno},
				Valid:    fuse.FATTR_SIZE,
				Size:     fileSize,
			},
		}, &attrOut)
		require.Equal(t, fuse.OK, saSt)

		resAfterSetAttr, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   3 * readStep,
			Size:     readStep,
		}, nil)
		require.Equal(t, fuse.OK, st)
		bAfterSetAttr, _ := resAfterSetAttr.Bytes(nil)
		require.Equal(t, byte(3), bAfterSetAttr[0], "SetAttr 后必须同步失效预读缓存并读到最新数据")

		// 3. 重新填充预读缓冲（version = 3），然后执行 Fallocate 触发失效
		populateAndVerifyCacheHit(3)
		faSt := fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Length:   4096,
			Mode:     0,
		})
		require.Equal(t, fuse.OK, faSt)

		resAfterFalloc, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   3 * readStep,
			Size:     readStep,
		}, nil)
		require.Equal(t, fuse.OK, st)
		bAfterFalloc, _ := resAfterFalloc.Bytes(nil)
		require.Equal(t, byte(4), bAfterFalloc[0], "Fallocate 后必须同步失效预读缓存并读到最新数据")

		// 4. 重新填充预读缓冲（version = 4），然后执行 Open(O_TRUNC) 触发失效
		populateAndVerifyCacheHit(4)
		var openOut fuse.OpenOut
		opSt := fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Flags:    uint32(syscall.O_RDWR | syscall.O_TRUNC),
		}, &openOut)
		require.Equal(t, fuse.OK, opSt)

		resAfterOpenTrunc, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   3 * readStep,
			Size:     readStep,
		}, nil)
		require.Equal(t, fuse.OK, st)
		bAfterOpenTrunc, _ := resAfterOpenTrunc.Bytes(nil)
		require.Equal(t, byte(5), bAfterOpenTrunc[0], "Open(O_TRUNC) 后必须同步失效预读缓存并读到最新数据")

		// 5. Release 句柄时清除该句柄关联的预读缓冲
		populateAndVerifyCacheHit(5)
		fs.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
		})
		require.Equal(t, fuse.OK, fs.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))
		version.Store(6)

		resAfterRelease, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   3 * readStep,
			Size:     readStep,
		}, nil)
		require.Equal(t, fuse.OK, st)
		bAfterRelease, _ := resAfterRelease.Bytes(nil)
		require.Equal(t, byte(6), bAfterRelease[0], "Release 后重绑句柄读取必须不命中旧预读缓存")
	})
}
