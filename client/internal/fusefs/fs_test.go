package fusefs_test

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func TestRawFS_BasicsAndInoMapping(t *testing.T) {
	cfg := fusefs.Config{
		DiskID:     "00000000-0000-0000-0000-000000000001",
		RootIno:    1001,
		ReadOnly:   true,
		DirectIO:   true,
		AllowOther: true,
		Debug:      true,
	}

	rawFS := fusefs.NewRawFS(nil, nil, cfg)
	require.NotNil(t, rawFS)

	// 1. 默认 RPCTimeout 常量必须为 30 秒
	require.Equal(t, 30*time.Second, fusefs.DefaultRPCTimeout)

	// 2. Inode 映射测试
	// 内核根目录 1 应映射为服务端 1001
	serverIno, status := rawFS.ToServerIno(1)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(1001), serverIno)

	// 非 1 且等于 RootIno 的内核访问必须被拦截返回 ENOENT（防混叠）
	_, status = rawFS.ToServerIno(1001)
	require.Equal(t, fuse.ENOENT, status)

	// 常规非根 Inode 原样返回
	serverIno, status = rawFS.ToServerIno(2002)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(2002), serverIno)

	// 服务端 Inode 转内核 Inode
	require.Equal(t, uint64(1), rawFS.ToKernelIno(1001))
	require.Equal(t, uint64(2002), rawFS.ToKernelIno(2002))

	// 当 RootIno 本身就是 1 时的特殊情况
	cfgRoot1 := fusefs.Config{RootIno: 1}
	fsRoot1 := fusefs.NewRawFS(nil, nil, cfgRoot1)
	serverIno, status = fsRoot1.ToServerIno(1)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint64(1), serverIno)

	// 2. 只读校验
	require.Equal(t, fuse.Status(syscall.EROFS), rawFS.CheckWritable())
	writableFS := fusefs.NewRawFS(nil, nil, fusefs.Config{ReadOnly: false})
	require.Equal(t, fuse.OK, writableFS.CheckWritable())

	// 3. NextRequestID 严格在 [1, math.MaxInt64] 范围内单调递增
	id1 := rawFS.NextRequestID()
	id2 := rawFS.NextRequestID()
	require.True(t, id1 >= 1 && id1 <= math.MaxInt64)
	require.True(t, id2 >= 1 && id2 <= math.MaxInt64)
	require.True(t, id2 > id1)

	// 4. OnUnmount 回调触发
	unmounted := false
	cbFS := fusefs.NewRawFS(nil, nil, fusefs.Config{
		OnUnmount: func() {
			unmounted = true
		},
	})
	cbFS.OnUnmount()
	require.True(t, unmounted)
}

func TestRawFS_BuildMountOptions(t *testing.T) {
	cfg := fusefs.Config{
		DiskID:     "test-disk-123",
		RootIno:    42,
		ReadOnly:   true,
		AllowOther: true,
		Debug:      true,
	}

	opts := fusefs.BuildMountOptions(cfg)
	require.NotNil(t, opts)
	require.Equal(t, "bowl:test-disk-123", opts.FsName)
	require.Equal(t, "bowl", opts.Name)
	require.Contains(t, opts.Options, "default_permissions")
	require.Contains(t, opts.Options, "ro")
	require.True(t, opts.AllowOther)
	require.True(t, opts.EnableLocks)
	require.False(t, opts.DisableXAttrs)
	require.Equal(t, 1<<20, opts.MaxWrite)
	require.Equal(t, 1<<20, opts.MaxReadAhead)
	require.True(t, opts.DirectMount)
	require.True(t, opts.Debug)

	// 非只读时 Options 不应包含 ro
	optsRW := fusefs.BuildMountOptions(fusefs.Config{DiskID: "rw", ReadOnly: false})
	require.NotContains(t, optsRW.Options, "ro")
}

func TestRawFS_FillAttrAndEntry(t *testing.T) {
	rawFS := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 100})

	now := time.Now()
	ino := &storagev1.Inode{
		Ino:        100, // 映射到 1
		Generation: 5,
		Attr: &storagev1.InodeAttribute{
			Mode:    0o755 | syscall.S_IFDIR,
			Size:    4096,
			Nlink:   2,
			Uid:     1000,
			Gid:     1000,
			Rdev:    0,
			Blksize: 65536,
			Blocks:  8,
			Atime:   timestamppb.New(now),
			Mtime:   timestamppb.New(now.Add(time.Second)),
			Ctime:   timestamppb.New(now.Add(2 * time.Second)),
			Flags:   16,
		},
	}

	var attr fuse.Attr
	rawFS.FillAttr(&attr, ino)
	require.Equal(t, uint64(1), attr.Ino)
	require.Equal(t, uint64(4096), attr.Size)
	require.Equal(t, uint32(0o755|syscall.S_IFDIR), attr.Mode)
	require.Equal(t, uint32(2), attr.Nlink)
	require.Equal(t, uint32(1000), attr.Uid)
	require.Equal(t, uint32(1000), attr.Gid)
	require.Equal(t, uint32(65536), attr.Blksize)
	require.Equal(t, uint64(8), attr.Blocks)
	require.Equal(t, uint64(now.Unix()), attr.Atime)
	require.Equal(t, uint32(now.Nanosecond()), attr.Atimensec)

	var entryOut fuse.EntryOut
	rawFS.FillEntryOut(&entryOut, ino, 1.5, 2.5)
	require.Equal(t, uint64(1), entryOut.NodeId)
	require.Equal(t, uint64(5), entryOut.Generation)
	require.Equal(t, 1500*time.Millisecond, entryOut.EntryTimeout())
	require.Equal(t, 2500*time.Millisecond, entryOut.AttrTimeout())
	require.Equal(t, uint64(1), entryOut.Attr.Ino)

	var attrOut fuse.AttrOut
	rawFS.FillAttrOut(&attrOut, ino, 3.0)
	require.Equal(t, 3*time.Second, attrOut.Timeout())
	require.Equal(t, uint64(1), attrOut.Attr.Ino)
}

type mockStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	lookupFn  func(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
	getAttrFn func(context.Context, *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error)
	statFSFn  func(context.Context, *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error)
}

func (m *mockStorageServer) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	if m.lookupFn != nil {
		return m.lookupFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockStorageServer) GetAttr(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	if m.getAttrFn != nil {
		return m.getAttrFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockStorageServer) StatFS(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	if m.statFSFn != nil {
		return m.statFSFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func setupTestRPCServer(t *testing.T, srv *mockStorageServer) storagev1connect.StorageServiceClient {
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)

	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_Lookup(t *testing.T) {
	mockSrv := &mockStorageServer{}
	client := setupTestRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("lookup in root succeeds with server ino translation", func(t *testing.T) {
		mockSrv.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetParentIno(), "内核 NodeId 1 应转换为服务端的 1001")
			require.Equal(t, "file.txt", string(req.Msg.GetName()))

			return connect.NewResponse(&storagev1.LookupResponse{
				Inode: &storagev1.Inode{
					Ino:        2001,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode:    0o644 | syscall.S_IFREG,
						Size:    1024,
						Nlink:   1,
						Blksize: 4096,
						Blocks:  2,
					},
				},
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  2.0,
			}), nil
		}

		var out fuse.EntryOut
		header := &fuse.InHeader{NodeId: 1} // FUSE_ROOT_ID
		status := rawFS.Lookup(nil, header, "file.txt", &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2001), out.NodeId)
		require.Equal(t, uint64(1024), out.Attr.Size)
		require.Equal(t, 1*time.Second, out.EntryTimeout())
		require.Equal(t, 2*time.Second, out.AttrTimeout())
	})

	t.Run("lookup not found returns ENOENT", func(t *testing.T) {
		mockSrv.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("entry not found"))
		}

		var out fuse.EntryOut
		header := &fuse.InHeader{NodeId: 1}
		status := rawFS.Lookup(nil, header, "notexist.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
	})

	t.Run("lookup anti-aliasing prevents direct access to root_ino", func(t *testing.T) {
		called := false
		mockSrv.lookupFn = func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
			called = true
			return nil, nil
		}

		var out fuse.EntryOut
		header := &fuse.InHeader{NodeId: 1001} // 内核越过映射直接访问 1001
		status := rawFS.Lookup(nil, header, "file.txt", &out)
		require.Equal(t, fuse.ENOENT, status)
		require.False(t, called, "防混叠拦截后不应向远端发送 RPC")
	})
}

func TestRawFS_GetAttr(t *testing.T) {
	mockSrv := &mockStorageServer{}
	client := setupTestRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("getattr root translates 1001 back to 1", func(t *testing.T) {
		mockSrv.getAttrFn = func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno())

			return connect.NewResponse(&storagev1.GetAttrResponse{
				Inode: &storagev1.Inode{
					Ino:        1001,
					Generation: 1,
					Attr: &storagev1.InodeAttribute{
						Mode:    0o755 | syscall.S_IFDIR,
						Size:    4096,
						Nlink:   2,
						Blksize: 4096,
						Blocks:  8,
					},
				},
				AttrTimeoutSeconds: 1.5,
			}), nil
		}

		var out fuse.AttrOut
		in := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 1}}
		status := rawFS.GetAttr(nil, in, &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(1), out.Attr.Ino, "出站 Ino 应转为 1 (ToKernelIno)")
		require.Equal(t, uint64(4096), out.Attr.Size)
		require.Equal(t, 1500*time.Millisecond, out.Timeout())
	})

	t.Run("getattr not found returns ENOENT", func(t *testing.T) {
		mockSrv.getAttrFn = func(ctx context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
			return nil, connect.NewError(connect.CodeNotFound, errors.New("inode not found"))
		}

		var out fuse.AttrOut
		in := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 9999}}
		status := rawFS.GetAttr(nil, in, &out)
		require.Equal(t, fuse.ENOENT, status)
	})
}

func TestRawFS_StatFs(t *testing.T) {
	mockSrv := &mockStorageServer{}
	client := setupTestRPCServer(t, mockSrv)

	cfg := fusefs.Config{
		DiskID:  "00000000-0000-0000-0000-000000000001",
		RootIno: 1001,
	}
	rawFS := fusefs.NewRawFS(client, nil, cfg)

	t.Run("statfs fills all fields accurately", func(t *testing.T) {
		mockSrv.statFSFn = func(ctx context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
			require.Equal(t, uint64(1001), req.Msg.GetIno())

			return connect.NewResponse(&storagev1.StatFSResponse{
				TotalBytes: 10 * 1024 * 1024 * 1024,
				UsedBytes:  2 * 1024 * 1024 * 1024,
				AvailBytes: 8 * 1024 * 1024 * 1024,
				Bsize:      4096,
				Frsize:     4096,
				Blocks:     2621440,
				Bfree:      2097152,
				Bavail:     2097152,
				Files:      128,
				Ffree:      0,
				Namelen:    255,
			}), nil
		}

		var out fuse.StatfsOut
		in := &fuse.InHeader{NodeId: 1}
		status := rawFS.StatFs(nil, in, &out)

		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint64(2621440), out.Blocks)
		require.Equal(t, uint64(2097152), out.Bfree)
		require.Equal(t, uint64(2097152), out.Bavail)
		require.Equal(t, uint64(128), out.Files)
		require.Equal(t, uint64(0), out.Ffree)
		require.Equal(t, uint32(4096), out.Bsize)
		require.Equal(t, uint32(4096), out.Frsize)
		require.Equal(t, uint32(255), out.NameLen)
	})
}

func TestRawFS_EndToEndDefault30sBudget_And_ChainContracts(t *testing.T) {
	const (
		diskID  = "00000000-0000-0000-0000-000000000001"
		rootIno = 1001
		fileIno = 2002
		testFH  = 88888
	)

	t.Run("default RawFS + NewClientFromConfig + healthy ClientSession succeeds over 5s and carries ~30s deadline", func(t *testing.T) {
		var (
			mu                sync.Mutex
			writeAttempts     int
			renewAttempts     int
			observedDeadlines []time.Duration
			renewDeadlines    []time.Duration
		)

		srv := &rwMockStorageServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				dl, ok := ctx.Deadline()
				var rem time.Duration
				if ok {
					rem = time.Until(dl)
				}
				mu.Lock()
				renewAttempts++
				renewDeadlines = append(renewDeadlines, rem)
				mu.Unlock()
				// 保持默认 15s 租约，使 renewLoop 在在途请求期间持续健康续期
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt: timestamppb.New(time.Now().Add(15 * time.Second)),
				}), nil
			},
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				dl, ok := ctx.Deadline()
				require.True(t, ok, "默认链路传输到服务端的请求上下文必须携带有效截止时间")
				rem := time.Until(dl)

				mu.Lock()
				writeAttempts++
				observedDeadlines = append(observedDeadlines, rem)
				mu.Unlock()

				// 模拟正常在途请求耗时超过旧 5s 限制（5.2s）但小于 30s 默认预算
				select {
				case <-time.After(5200 * time.Millisecond):
					return connect.NewResponse(&storagev1.WriteResponse{
						Size: uint32(len(req.Msg.GetData())),
					}), nil
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			},
		}

		mux := http.NewServeMux()
		path, handler := storagev1connect.NewStorageServiceHandler(srv)
		mux.Handle(path, handler)
		ts := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
		t.Cleanup(ts.Close)

		// 使用真实默认组装的 NewClientFromConfig（不传 RetryConfig，启用默认重试拦截器与默认 30s 预算）
		client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
			Endpoint: ts.URL,
		})
		require.NoError(t, err)

		// 启动健康且持续续期的 ClientSession（使用默认 3s RenewRPCTimeout，短 MinInterval 确保在 5.2s 期间触发多次续期）
		mgr, err := session.New(context.Background(), client, session.Config{
			DiskID:      diskID,
			MinInterval: 1 * time.Second,
			MaxInterval: 1 * time.Second,
		})
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Close() })
		require.NoError(t, mgr.Start(context.Background()))

		initialSID := mgr.CurrentSessionID()
		require.NotZero(t, initialSID)
		mgr.BindHandle(testFH, initialSID)

		// 使用默认 Config（不指定 RPCTimeout，自动采用 DefaultRPCTimeout = 30s）
		rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:  diskID,
			RootIno: rootIno,
		})
		t.Cleanup(rawFS.OnUnmount)
		require.Equal(t, fuse.OK, rawFS.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		payload := []byte("adr-0027-30s-budget-verification-payload")
		start := time.Now()
		written, status := rawFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}, payload)
		elapsed := time.Since(start)

		require.Equal(t, fuse.OK, status, "超过 5s 但未超过 30s 的正常在途请求必须成功，不能被旧 5s 预算截断为 EIO")
		require.Equal(t, uint32(len(payload)), written)
		require.GreaterOrEqual(t, elapsed, 5200*time.Millisecond)
		require.Less(t, elapsed, 10*time.Second)

		mu.Lock()
		gotWriteAttempts := writeAttempts
		gotWriteDLs := append([]time.Duration(nil), observedDeadlines...)
		gotRenewAttempts := renewAttempts
		gotRenewDLs := append([]time.Duration(nil), renewDeadlines...)
		mu.Unlock()

		require.Equal(t, 1, gotWriteAttempts, "Write 请求应恰好执行 1 次")
		require.Len(t, gotWriteDLs, 1)
		require.Greater(t, gotWriteDLs[0], 25*time.Second, "实际传输截止时间必须接近 30s 默认预算，而非 5s")
		require.LessOrEqual(t, gotWriteDLs[0], 30*time.Second)

		// 确认在 5.2s 请求期间，独立心跳持续续期且未发生会话轮换，且心跳单次超时仍保持 <= 3s
		require.Equal(t, initialSID, mgr.CurrentSessionID(), "健康续期的 ClientSession 在 5.2s 请求期间不得失效或轮换")
		require.GreaterOrEqual(t, gotRenewAttempts, 3, "5.2s 请求期间后台 renewLoop 应持续完成多次续期")
		for _, rdl := range gotRenewDLs {
			require.Greater(t, rdl, time.Duration(0))
			require.LessOrEqual(t, rdl, 3*time.Second, "独立心跳单次超时必须保持 <= 3s，不随常规 RPC 放宽为 30s")
		}
	})

	t.Run("explicit shorter RPCTimeout, FUSE cancel to EINTR, budget exhaustion to EIO, non-idempotent Write no-retry, shared retry deadline, and SetLkw exemption", func(t *testing.T) {
		var (
			mu                  sync.Mutex
			writeAttempts       int
			clientReadDeadlines []time.Time
			serverReadRemDLs    []time.Duration
		)

		srv := &fullChainMockServer{
			renewSessionFn: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt: timestamppb.New(time.Now().Add(15 * time.Second)),
				}), nil
			},
			lookupFn: func(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
				switch string(req.Msg.GetName()) {
				case "hang.txt":
					<-ctx.Done()
					return nil, ctx.Err()
				case "enoent.txt":
					return nil, newChainStorageError(connect.CodeNotFound, "ENOENT", "no such file")
				default:
					return connect.NewResponse(&storagev1.LookupResponse{
						Inode: &storagev1.Inode{Ino: fileIno, Attr: &storagev1.InodeAttribute{Mode: syscall.S_IFREG | 0o644}},
					}), nil
				}
			},
			writeFn: func(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
				mu.Lock()
				writeAttempts++
				mu.Unlock()
				// 非幂等 Write 送达服务端后返回 CodeUnavailable，即使默认 30s 预算充足也严禁重试
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("transient failure after write received"))
			},
			readFn: func(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
				dl, ok := ctx.Deadline()
				if ok {
					rem := time.Until(dl)
					mu.Lock()
					serverReadRemDLs = append(serverReadRemDLs, rem)
					callIdx := len(serverReadRemDLs)
					mu.Unlock()
					if callIdx == 1 {
						time.Sleep(40 * time.Millisecond)
						return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporary read failure"))
					}
				}
				return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("ok")}), nil
			},
		}

		mux := http.NewServeMux()
		path, handler := storagev1connect.NewStorageServiceHandler(srv)
		mux.Handle(path, handler)
		ts := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
		t.Cleanup(ts.Close)

		// 在重试拦截器内层捕获每次实际发起 HTTP 传输时的 context.Deadline()
		deadlineProbe := connect.WithInterceptors(connect.UnaryInterceptorFunc(func(next connect.UnaryFunc) connect.UnaryFunc {
			return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
				if req.Spec().Procedure == storagev1connect.StorageServiceReadProcedure {
					if dl, ok := ctx.Deadline(); ok {
						mu.Lock()
						clientReadDeadlines = append(clientReadDeadlines, dl)
						mu.Unlock()
					}
				}
				return next(ctx, req)
			}
		}))

		client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
			Endpoint: ts.URL,
			RetryConfig: &rpc.RetryConfig{
				// 保持默认 MaxElapsedTime (0 -> DefaultMaxElapsedTime = 30s)，缩短退避间隔以加速测试
				InitialInterval: 10 * time.Millisecond,
				MaxInterval:     20 * time.Millisecond,
			},
		}, deadlineProbe)
		require.NoError(t, err)

		mgr, err := session.New(context.Background(), client, session.Config{DiskID: diskID})
		require.NoError(t, err)
		t.Cleanup(func() { _ = mgr.Close() })
		require.NoError(t, mgr.Start(context.Background()))
		mgr.BindHandle(testFH, mgr.CurrentSessionID())

		// 1. 显式配置的更短 RPCTimeout (80ms) 优先于 30s 默认重试预算生效，耗尽时返回 EIO
		shortFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:     diskID,
			RootIno:    rootIno,
			RPCTimeout: 80 * time.Millisecond,
		})
		t.Cleanup(shortFS.OnUnmount)
		require.Equal(t, fuse.OK, shortFS.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		var entryOut fuse.EntryOut
		start := time.Now()
		stTimeout := shortFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "hang.txt", &entryOut)
		elapsedTimeout := time.Since(start)
		require.Equal(t, fuse.Status(syscall.EIO), stTimeout, "显式更短 RPCTimeout 耗尽必须返回 EIO")
		require.GreaterOrEqual(t, elapsedTimeout, 70*time.Millisecond)
		require.Less(t, elapsedTimeout, 500*time.Millisecond, "显式 80ms 超时必须生效，不能等待 30s")

		// 2. 默认 30s 预算下 FUSE 主动取消立即返回 EINTR
		defaultFS := fusefs.NewRawFS(client, mgr, fusefs.Config{
			DiskID:  diskID,
			RootIno: rootIno,
		})
		t.Cleanup(defaultFS.OnUnmount)
		require.Equal(t, fuse.OK, defaultFS.PrepareTestHandle(fileIno, testFH, syscall.O_RDWR, false))

		cancelCh := make(chan struct{})
		go func() {
			time.Sleep(40 * time.Millisecond)
			close(cancelCh)
		}()
		startCancel := time.Now()
		stCancel := defaultFS.Lookup(cancelCh, &fuse.InHeader{NodeId: 1}, "hang.txt", &entryOut)
		require.Equal(t, fuse.Status(syscall.EINTR), stCancel, "FUSE 主动取消必须返回 EINTR")
		require.Less(t, time.Since(startCancel), 500*time.Millisecond)

		// 3. 原有业务错误准确传播且不触发重试
		stNotFound := defaultFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "enoent.txt", &entryOut)
		require.Equal(t, fuse.ENOENT, stNotFound)

		// 4. 延长 30s 预算绝不导致非幂等 Write 在送达服务端后被重复提交
		written, stWrite := defaultFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
		}, []byte("once-only"))
		require.Equal(t, fuse.Status(syscall.EIO), stWrite)
		require.Equal(t, uint32(0), written)
		mu.Lock()
		require.Equal(t, 1, writeAttempts, "非幂等 Write 送达服务端失败后严禁重复提交")
		mu.Unlock()

		// 5. 多次允许的幂等重试共享同一个总截止时间，不按每次尝试重置 30s 预算
		res, stRead := defaultFS.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: fileIno},
			Fh:       testFH,
			Offset:   0,
			Size:     2,
		}, nil)
		require.Equal(t, fuse.OK, stRead)
		require.Equal(t, 2, res.Size())
		mu.Lock()
		require.Len(t, clientReadDeadlines, 2, "Read 应重试 1 次共调用 2 次")
		require.True(t, clientReadDeadlines[0].Equal(clientReadDeadlines[1]), "多次重试必须共享同一个总截止时间")
		require.Len(t, serverReadRemDLs, 2)
		require.Less(t, serverReadRemDLs[1], serverReadRemDLs[0], "服务端第 2 次尝试收到的剩余预算必须随已消耗时间递减，不得重置为 30s")
		mu.Unlock()

		// 6. 本地 SetLkw 不受 RPC 超时约束，并响应内核中断。
		request := func(owner uint64, typ uint32) *fuse.LkIn {
			return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: fileIno}, Fh: testFH, Owner: owner,
				Lk: fuse.FileLock{Start: 0, End: 10, Typ: typ}}
		}
		require.Equal(t, fuse.OK, shortFS.SetLk(nil, request(100, uint32(syscall.F_WRLCK))))
		cancelLock := make(chan struct{})
		defer close(cancelLock)
		lockDone := make(chan fuse.Status, 1)
		go func() { lockDone <- shortFS.SetLkw(cancelLock, request(101, uint32(syscall.F_WRLCK))) }()
		select {
		case status := <-lockDone:
			t.Fatalf("等待锁提前返回：%v", status)
		case <-time.After(150 * time.Millisecond):
		}
		require.Equal(t, fuse.OK, shortFS.SetLk(nil, request(100, uint32(syscall.F_UNLCK))))
		select {
		case status := <-lockDone:
			require.Equal(t, fuse.OK, status)
		case <-time.After(time.Second):
			t.Fatal("释放锁后等待者未被唤醒")
		}
		lkwCancel := make(chan struct{})
		go func() { time.Sleep(40 * time.Millisecond); close(lkwCancel) }()
		require.Equal(t, fuse.EINTR, shortFS.SetLkw(lkwCancel, request(102, uint32(syscall.F_WRLCK))))
		// 7. 上游更早截止时间（caller context deadline < 30s DefaultMaxElapsedTime）在 NewClientFromConfig 默认拦截器下优先触发
		upstreamCtx, upstreamCancel := context.WithTimeout(context.Background(), 70*time.Millisecond)
		defer upstreamCancel()
		startUpstream := time.Now()
		_, errUpstream := client.Lookup(upstreamCtx, connect.NewRequest(&storagev1.LookupRequest{
			ParentIno: rootIno,
			Name:      []byte("hang.txt"),
		}))
		elapsedUpstream := time.Since(startUpstream)
		require.Error(t, errUpstream)
		require.Equal(t, syscall.EIO, rpc.MapError(errUpstream), "上游更早截止时间耗尽应映射为 EIO")
		require.GreaterOrEqual(t, elapsedUpstream, 60*time.Millisecond)
		require.Less(t, elapsedUpstream, 400*time.Millisecond, "上游更早截止时间必须生效，不被 30s 默认重试预算覆盖")
	})
}

type fullChainMockServer struct {
	storagev1connect.UnimplementedStorageServiceHandler
	renewSessionFn func(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
	lookupFn       func(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error)
	writeFn        func(context.Context, *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error)
	readFn         func(context.Context, *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error)
	setLkwFn       func(context.Context, *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error)
}

func (m *fullChainMockServer) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	return m.renewSessionFn(ctx, req)
}

func (m *fullChainMockServer) Lookup(ctx context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	return m.lookupFn(ctx, req)
}

func (m *fullChainMockServer) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	return m.writeFn(ctx, req)
}

func (m *fullChainMockServer) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	return m.readFn(ctx, req)
}

func (m *fullChainMockServer) SetLkw(ctx context.Context, req *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
	return m.setLkwFn(ctx, req)
}

func newChainStorageError(code connect.Code, reason, msg string) error {
	connErr := connect.NewError(code, errors.New(msg))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: reason,
		Domain: "bowl.storage",
	})
	if err == nil {
		connErr.AddDetail(detail)
	}
	return connErr
}
