package fusefs

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"connectrpc.com/otelconnect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type obsTestServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu          sync.Mutex
	reqIDs      []uint64
	methods     []string
	failWrite   atomic.Bool
	failUnlink  atomic.Bool
	failFsync   atomic.Bool
	failReclaim atomic.Int32
	reclaimInos []uint64
	prefetchHit chan struct{}
	fileSize    uint64
	readPayload []byte
}

func (s *obsTestServer) recordReq(header http.Header, method string) {
	raw := header.Get(rpc.HeaderRequestID)
	id, _ := strconv.ParseUint(raw, 10, 64)
	s.mu.Lock()
	s.reqIDs = append(s.reqIDs, id)
	s.methods = append(s.methods, method)
	s.mu.Unlock()
}

func (s *obsTestServer) makeInode(ino, size uint64, mode, nlink uint32) *storagev1.Inode {
	now := timestamppb.Now()
	return &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:    mode,
			Size:    size,
			Nlink:   nlink,
			Blksize: 4096,
			Atime:   now,
			Mtime:   now,
			Ctime:   now,
		},
	}
}

func (s *obsTestServer) Lookup(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	s.recordReq(req.Header(), "Lookup")
	name := string(req.Msg.GetName())
	if name == "exists.txt" {
		return connect.NewResponse(&storagev1.LookupResponse{
			Inode:               s.makeInode(10, 128, syscall.S_IFREG|0o644, 1),
			EntryTimeoutSeconds: 1,
			AttrTimeoutSeconds:  1,
		}), nil
	}
	return nil, connect.NewError(connect.CodeNotFound, nil)
}

func (s *obsTestServer) GetAttr(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	s.recordReq(req.Header(), "GetAttr")
	s.mu.Lock()
	sz := s.fileSize
	s.mu.Unlock()
	mode := uint32(syscall.S_IFREG | 0o644)
	if req.Msg.GetIno() == 1 || req.Msg.GetIno() == 20 {
		mode = syscall.S_IFDIR | 0o755
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              s.makeInode(req.Msg.GetIno(), sz, mode, 1),
		AttrTimeoutSeconds: 1,
	}), nil
}

func (s *obsTestServer) StatFS(_ context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	s.recordReq(req.Header(), "StatFS")
	return connect.NewResponse(&storagev1.StatFSResponse{
		Blocks:  1024,
		Bfree:   512,
		Bavail:  512,
		Files:   1000,
		Ffree:   900,
		Bsize:   4096,
		Frsize:  4096,
		Namelen: 255,
	}), nil
}

func (s *obsTestServer) Mkdir(_ context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	s.recordReq(req.Header(), "Mkdir")
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               s.makeInode(20, 0, syscall.S_IFDIR|0o755, 2),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (s *obsTestServer) Mknod(_ context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	s.recordReq(req.Header(), "Mknod")
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               s.makeInode(21, 0, syscall.S_IFREG|0o644, 1),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (s *obsTestServer) Rmdir(_ context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	s.recordReq(req.Header(), "Rmdir")
	return connect.NewResponse(&storagev1.RmdirResponse{}), nil
}

func (s *obsTestServer) Unlink(_ context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	s.recordReq(req.Header(), "Unlink")
	if s.failUnlink.Load() {
		return nil, connect.NewError(connect.CodeInternal, nil)
	}
	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

func (s *obsTestServer) Rename(_ context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	s.recordReq(req.Header(), "Rename")
	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

func (s *obsTestServer) Symlink(_ context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	s.recordReq(req.Header(), "Symlink")
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               s.makeInode(22, 6, syscall.S_IFLNK|0o777, 1),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (s *obsTestServer) Readlink(_ context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	s.recordReq(req.Header(), "Readlink")
	return connect.NewResponse(&storagev1.ReadlinkResponse{Target: []byte("target")}), nil
}

func (s *obsTestServer) Link(_ context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	s.recordReq(req.Header(), "Link")
	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               s.makeInode(req.Msg.GetIno(), 128, syscall.S_IFREG|0o644, 2),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (s *obsTestServer) Create(_ context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	s.recordReq(req.Header(), "Create")
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               s.makeInode(23, 0, syscall.S_IFREG|0o644, 1),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (s *obsTestServer) Fsync(_ context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	s.recordReq(req.Header(), "Fsync")
	if s.failFsync.Load() {
		return nil, connect.NewError(connect.CodeInternal, nil)
	}
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (s *obsTestServer) FsyncDir(_ context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	s.recordReq(req.Header(), "FsyncDir")
	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

func (s *obsTestServer) ReadDirPlus(_ context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	s.recordReq(req.Header(), "ReadDirPlus")
	return connect.NewResponse(&storagev1.ReadDirPlusResponse{
		Entries: []*storagev1.DirEntryPlus{
			{
				Ino:                 10,
				Name:                []byte("exists.txt"),
				NextOffset:          1,
				Inode:               s.makeInode(10, 128, syscall.S_IFREG|0o644, 1),
				EntryTimeoutSeconds: 1,
				AttrTimeoutSeconds:  1,
			},
		},
	}), nil
}

func (s *obsTestServer) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	s.recordReq(req.Header(), "Read")
	off := req.Msg.GetOffset()
	sz := uint64(req.Msg.GetSize())
	s.mu.Lock()
	payload := s.readPayload
	ch := s.prefetchHit
	if off == MaxRPCChunkSize && ch != nil {
		s.prefetchHit = nil
		close(ch)
	}
	s.mu.Unlock()
	if len(payload) == 0 {
		data := bytes.Repeat([]byte{'R'}, int(min(sz, 64)))
		return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
	}
	if off >= uint64(len(payload)) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	end := min(off+sz, uint64(len(payload)))
	return connect.NewResponse(&storagev1.ReadResponse{Data: payload[off:end]}), nil
}

func (s *obsTestServer) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	s.recordReq(req.Header(), "Write")
	if s.failWrite.Load() {
		return nil, connect.NewError(connect.CodeResourceExhausted, nil)
	}
	n := uint32(len(req.Msg.GetData()))
	newEnd := req.Msg.GetOffset() + uint64(n)
	s.mu.Lock()
	if newEnd > s.fileSize {
		s.fileSize = newEnd
	}
	sz := s.fileSize
	s.mu.Unlock()
	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  n,
		Inode: s.makeInode(req.Msg.GetIno(), sz, syscall.S_IFREG|0o644, 1),
	}), nil
}

func (s *obsTestServer) SetAttr(_ context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	s.recordReq(req.Header(), "SetAttr")
	sz := uint64(0)
	if req.Msg.Size != nil {
		sz = req.Msg.GetSize()
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              s.makeInode(req.Msg.GetIno(), sz, syscall.S_IFREG|0o644, 1),
		AttrTimeoutSeconds: 1,
	}), nil
}

func (s *obsTestServer) Fallocate(_ context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	s.recordReq(req.Header(), "Fallocate")
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (s *obsTestServer) SetXattr(_ context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	s.recordReq(req.Header(), "SetXattr")
	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

func (s *obsTestServer) GetXattr(_ context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	s.recordReq(req.Header(), "GetXattr")
	if string(req.Msg.GetName()) == securityCapabilityXattr {
		cerr := connect.NewError(connect.CodeNotFound, nil)
		if detail, derr := connect.NewErrorDetail(&errdetails.ErrorInfo{Domain: "bowl.storage", Reason: "ENODATA"}); derr == nil {
			cerr.AddDetail(detail)
		}
		return nil, cerr
	}
	return connect.NewResponse(&storagev1.GetXattrResponse{Value: []byte("val")}), nil
}

func (s *obsTestServer) ListXattr(_ context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	s.recordReq(req.Header(), "ListXattr")
	return connect.NewResponse(&storagev1.ListXattrResponse{Names: [][]byte{[]byte("user.k")}}), nil
}

func (s *obsTestServer) RemoveXattr(_ context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	s.recordReq(req.Header(), "RemoveXattr")
	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}

func (s *obsTestServer) ReclaimOrphan(_ context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	s.recordReq(req.Header(), "ReclaimOrphan")
	if rem := s.failReclaim.Add(-1); rem >= 0 {
		return nil, connect.NewError(connect.CodeUnavailable, nil)
	}
	s.mu.Lock()
	s.reclaimInos = append(s.reclaimInos, req.Msg.GetIno())
	s.mu.Unlock()
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}

//nolint:gocritic // 测试辅助按值复制 Config 以便填充默认值
func startObsTestEnv(t *testing.T, svc *obsTestServer, cfg Config) (*InstrumentedRawFS, *tracetest.SpanRecorder, *syncBuffer) {
	t.Helper()

	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))

	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
		_ = tp.Shutdown(context.Background())
	})

	serverOtel, err := otelconnect.NewInterceptor(
		otelconnect.WithTracerProvider(tp),
		otelconnect.WithPropagator(propagation.TraceContext{}),
		otelconnect.WithTrustRemote(),
	)
	require.NoError(t, err)

	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(svc, connect.WithInterceptors(serverOtel))
	mux.Handle(path, handler)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)

	srv := httptest.NewUnstartedServer(mux)
	srv.EnableHTTP2 = true
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)

	var logBuf syncBuffer
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	}
	if cfg.DiskID == "" {
		cfg.DiskID = "disk-obs"
	}
	if cfg.Epoch == 0 {
		cfg.Epoch = 1
	}
	if cfg.RootIno == 0 {
		cfg.RootIno = 1
	}
	if cfg.TracerProvider == nil {
		cfg.TracerProvider = tp
	}

	rpcClient, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint:    srv.URL,
		RetryConfig: &rpc.RetryConfig{Disable: true},
	})
	require.NoError(t, err)

	raw := NewRawFS(rpcClient, cfg)
	ins := NewInstrumentedRawFS(raw)
	return ins, recorder, &logBuf
}

func TestMetrics_PedanticRegistryAndCardinalityGuard(t *testing.T) {
	svc := &obsTestServer{}
	ins, _, _ := startObsTestEnv(t, svc, Config{EnableStatsFile: true})
	t.Cleanup(ins.OnUnmount)

	// 触发成功路径与多种 POSIX errno 错误路径
	var entryOut fuse.EntryOut
	require.Equal(t, fuse.OK, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".stats", &entryOut))
	require.Equal(t, fuse.ENOENT, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "missing.txt", &entryOut))
	var openOut fuse.OpenOut
	require.Equal(t, fuse.EACCES, ins.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: VirtualStatsIno},
		Flags:    syscall.O_WRONLY,
	}, &openOut))

	reg := prometheus.NewPedanticRegistry()
	require.NoError(t, reg.Register(ins.Collector()))

	families, err := reg.Gather()
	require.NoError(t, err)
	require.NotEmpty(t, families)

	forbiddenLabels := map[string]struct{}{
		"ino":    {},
		"inode":  {},
		"fh":     {},
		"handle": {},
		"path":   {},
		"name":   {},
	}
	seenMetricNames := make(map[string]bool, len(families))
	for _, mf := range families {
		seenMetricNames[mf.GetName()] = true
		for _, metric := range mf.GetMetric() {
			for _, lp := range metric.GetLabel() {
				_, forbidden := forbiddenLabels[lp.GetName()]
				require.False(t, forbidden, "指标 %s 严禁包含高基数或敏感标签 %q", mf.GetName(), lp.GetName())
			}
		}
	}

	for _, expected := range []string{
		"bowl_client_fuse_op_duration_seconds",
		"bowl_client_fuse_ops_total",
		"bowl_client_fuse_inflight_ops",
		"bowl_client_fuse_op_errors_total",
		"bowl_client_fuse_io_bytes_total",
		"bowl_client_nscache_hits_total",
		"bowl_client_nscache_misses_total",
		"bowl_client_capcache_hits_total",
		"bowl_client_readahead_triggers_total",
		"bowl_client_readahead_discarded_bytes_total",
		"bowl_client_tmpfile_resident_bytes",
		"bowl_client_tmpfile_rollback_errors_total",
		"bowl_client_open_handles",
		"bowl_client_locks_active",
		"bowl_client_inodelock_acquisitions_total",
		"bowl_client_queue_depth",
		"bowl_client_queue_ops_total",
	} {
		require.True(t, seenMetricNames[expected], "PedanticRegistry 缺少预期指标 %s", expected)
	}
}

func TestInstrumentedRawFS_All36OperationsAndZeroAllocs(t *testing.T) {
	svc := &obsTestServer{}
	ins, _, _ := startObsTestEnv(t, svc, Config{EnableStatsFile: true})

	// 1. 覆盖全部 36 个 FUSE 方法
	var entryOut fuse.EntryOut
	require.Equal(t, fuse.OK, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "exists.txt", &entryOut))

	var attrOut fuse.AttrOut
	require.Equal(t, fuse.OK, ins.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 10}}, &attrOut))

	var statfsOut fuse.StatfsOut
	require.Equal(t, fuse.OK, ins.StatFs(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, &statfsOut))

	require.Equal(t, fuse.OK, ins.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: 0o755}, "sub", &entryOut))
	require.Equal(t, fuse.OK, ins.Mknod(nil, &fuse.MknodIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Mode: syscall.S_IFREG | 0o644}, "nod", &entryOut))
	require.Equal(t, fuse.OK, ins.Rmdir(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "sub"))
	require.Equal(t, fuse.OK, ins.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "nod"))
	require.Equal(t, fuse.OK, ins.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Newdir: fuse.FUSE_ROOT_ID}, "exists.txt", "renamed.txt"))
	require.Equal(t, fuse.OK, ins.Symlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "target", "sym", &entryOut))

	target, st := ins.Readlink(nil, &fuse.InHeader{NodeId: 22})
	require.Equal(t, fuse.OK, st)
	require.Equal(t, []byte("target"), target)

	require.Equal(t, fuse.OK, ins.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Oldnodeid: 10}, "hardlink.txt", &entryOut))

	var createOut fuse.CreateOut
	require.Equal(t, fuse.OK, ins.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: syscall.O_RDWR, Mode: 0o644}, "created.txt", &createOut))

	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, ins.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 10}, Flags: syscall.O_RDWR}, &openOut))

	readBuf := make([]byte, 32)
	readRes, st := ins.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Size: 32}, readBuf)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, 32, readRes.Size())

	written, st := ins.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0}, []byte("hello-world"))
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(11), written)

	require.Equal(t, fuse.OK, ins.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: 10}, Valid: fuse.FATTR_MODE, Mode: 0o600}}, &attrOut))
	require.Equal(t, fuse.OK, ins.Fallocate(nil, &fuse.FallocateIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Length: 4096}))
	require.Equal(t, fuse.OK, ins.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 10}}, "user.k", []byte("v")))

	xattrBuf := make([]byte, 16)
	sz, st := ins.GetXAttr(nil, &fuse.InHeader{NodeId: 10}, "user.k", xattrBuf)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(3), sz)

	sz, st = ins.ListXAttr(nil, &fuse.InHeader{NodeId: 10}, xattrBuf)
	require.Equal(t, fuse.OK, st)
	require.Positive(t, sz)

	require.Equal(t, fuse.OK, ins.RemoveXAttr(nil, &fuse.InHeader{NodeId: 10}, "user.k"))

	var ioctlOut fuse.IoctlOut
	ioctlBuf := make([]byte, 8)
	require.Equal(t, fuse.OK, ins.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: 10}, Cmd: FS_IOC_GETFLAGS}, nil, &ioctlOut, ioctlBuf))

	var lkOut fuse.LkOut
	lkIn := &fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 10},
		Fh:       openOut.Fh,
		Owner:    101,
		Lk:       fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_WRLCK)},
	}
	require.Equal(t, fuse.OK, ins.GetLk(nil, lkIn, &lkOut))
	require.Equal(t, fuse.OK, ins.SetLk(nil, lkIn))
	lkUnlock := *lkIn
	lkUnlock.Lk.Typ = uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, ins.SetLkw(nil, &lkUnlock))

	require.Equal(t, fuse.OK, ins.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, LockOwner: 101}))
	require.Equal(t, fuse.OK, ins.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh}))
	ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh})
	ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: createOut.NodeId}, Fh: createOut.Fh})

	var tmpOut fuse.CreateOut
	require.Equal(t, fuse.OK, ins.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: syscall.O_RDWR, Mode: 0o600}, &tmpOut))
	ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})
	ins.Forget(tmpOut.NodeId, 1)

	var dirOpenOut fuse.OpenOut
	require.Equal(t, fuse.OK, ins.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: syscall.O_RDONLY}, &dirOpenOut))
	dirBuf := make([]byte, 4096)
	dirList := fuse.NewDirEntryList(dirBuf, 0)
	require.Equal(t, fuse.OK, ins.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpenOut.Fh}, dirList))
	dirPlusList := fuse.NewDirEntryList(dirBuf, 0)
	require.Equal(t, fuse.OK, ins.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpenOut.Fh}, dirPlusList))
	require.Equal(t, fuse.OK, ins.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpenOut.Fh}))
	ins.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpenOut.Fh})

	// 校验 8 个非统计透传方法不破坏契约
	_ = ins.String()
	ins.SetDebug(false)
	_ = ins.Access(nil, &fuse.AccessIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}})
	_ = ins.Lseek(nil, &fuse.LseekIn{InHeader: fuse.InHeader{NodeId: 10}}, &fuse.LseekOut{})
	_, _ = ins.Writev(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 10}}, nil)
	_, _ = ins.CopyFileRange(nil, &fuse.CopyFileRangeIn{InHeader: fuse.InHeader{NodeId: 10}})
	_ = ins.Statx(nil, &fuse.StatxIn{InHeader: fuse.InHeader{NodeId: 10}}, &fuse.StatxOut{})

	ins.OnUnmount()

	snap := ins.Snapshot()
	require.Len(t, snap.FUSE.Ops, OpCount)
	for op := range FuseOp(OpCount) {
		opSnap := snap.FUSE.Ops[op]
		require.GreaterOrEqual(t, opSnap.Calls, uint64(1), "FUSE 操作 %s 未被记录", op.String())
	}
	require.Equal(t, uint64(32), snap.FUSE.ReadBytes)
	require.Equal(t, uint64(11), snap.FUSE.WriteBytes)

	// 2. 热路径 0 B/op、0 allocs/op 实证测试（同时覆盖 fuse.OK 与错误返回分支）
	rawZero := NewRawFS(nil, Config{
		DiskID:          "disk-zero",
		Epoch:           1,
		RootIno:         1,
		EnableStatsFile: true,
		ReadOnly:        true,
	})
	t.Cleanup(rawZero.OnUnmount)
	insZero := NewInstrumentedRawFS(rawZero)

	rawZero.cache.putEntry(fuse.FUSE_ROOT_ID, "neg-cached", 0)
	rawZero.caps.markAbsent(100)

	lookupHdr := &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}
	getAttrIn := &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: VirtualStatsIno}}
	xattrHdr := &fuse.InHeader{NodeId: 100}
	writeIn := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 100}}
	writePayload := []byte("abcd")

	allocsOKLookup := testing.AllocsPerRun(1000, func() {
		st := insZero.Lookup(nil, lookupHdr, ".stats", &entryOut)
		if st != fuse.OK {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, allocsOKLookup, "InstrumentedRawFS.Lookup 热路径必须 0 分配")

	allocsOKGetAttr := testing.AllocsPerRun(1000, func() {
		st := insZero.GetAttr(nil, getAttrIn, &attrOut)
		if st != fuse.OK {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, allocsOKGetAttr, "InstrumentedRawFS.GetAttr 热路径必须 0 分配")

	allocsErrLookup := testing.AllocsPerRun(1000, func() {
		st := insZero.Lookup(nil, lookupHdr, "neg-cached", &entryOut)
		if st != fuse.ENOENT {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, allocsErrLookup, "InstrumentedRawFS.Lookup 负缓存命中错误路径必须 0 分配")

	allocsCapShortCircuit := testing.AllocsPerRun(1000, func() {
		_, st := insZero.GetXAttr(nil, xattrHdr, securityCapabilityXattr, nil)
		if st != fuse.Status(syscall.ENODATA) {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, allocsCapShortCircuit, "InstrumentedRawFS.GetXAttr capability 短路路径必须 0 分配")

	allocsForget := testing.AllocsPerRun(1000, func() {
		insZero.Forget(999, 1)
	})
	require.Zero(t, allocsForget, "InstrumentedRawFS.Forget 热路径必须 0 分配")

	allocsWriteRO := testing.AllocsPerRun(1000, func() {
		_, st := insZero.Write(nil, writeIn, writePayload)
		if st != fuse.Status(syscall.EROFS) {
			t.Fatalf("unexpected status: %v", st)
		}
	})
	require.Zero(t, allocsWriteRO, "InstrumentedRawFS.Write 错误路径必须 0 分配")
}

func TestSubsystems_All8MetricsAndHealthSignals(t *testing.T) {
	t.Run("NamespaceCache与CapabilityCache指标", func(t *testing.T) {
		svc := &obsTestServer{}
		ins, _, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		// 容量淘汰实证：独立小容量缓存绑定同一 Metrics
		smallCache := NewNamespaceCache(2)
		smallCache.bindMetrics(ins.Metrics())
		smallCache.putEntry(1, "a", 101)
		smallCache.putEntry(1, "b", 102)
		smallCache.putEntry(1, "c", 103) // 触发 1 次容量淘汰

		var entryOut fuse.EntryOut
		// 首次 Lookup 存在项 -> entry miss + attr 回填
		require.Equal(t, fuse.OK, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "exists.txt", &entryOut))
		// 二次 Lookup 存在项 -> entry hit + attr hit
		require.Equal(t, fuse.OK, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "exists.txt", &entryOut))
		// 首次 Lookup 不存在项 -> entry miss + negative_entry miss -> 回填负缓存
		require.Equal(t, fuse.ENOENT, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "no-such.txt", &entryOut))
		// 二次 Lookup 不存在项 -> negative_entry hit
		require.Equal(t, fuse.ENOENT, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "no-such.txt", &entryOut))

		// ReadDir miss -> 回填 -> ReadDir hit
		var dirOpen fuse.OpenOut
		require.Equal(t, fuse.OK, ins.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: syscall.O_RDONLY}, &dirOpen))
		buf := make([]byte, 4096)
		require.Equal(t, fuse.OK, ins.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpen.Fh}, fuse.NewDirEntryList(buf, 0)))
		require.Equal(t, fuse.OK, ins.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpen.Fh}, fuse.NewDirEntryList(buf, 0)))

		// capabilityCache: miss -> ENODATA markAbsent -> hit -> reset
		_, st := ins.GetXAttr(nil, &fuse.InHeader{NodeId: 10}, securityCapabilityXattr, nil)
		require.Equal(t, fuse.Status(syscall.ENODATA), st)
		_, st = ins.GetXAttr(nil, &fuse.InHeader{NodeId: 10}, securityCapabilityXattr, nil)
		require.Equal(t, fuse.Status(syscall.ENODATA), st)

		snap := ins.Snapshot()
		require.GreaterOrEqual(t, snap.NamespaceCache.EntryHits, uint64(1))
		require.GreaterOrEqual(t, snap.NamespaceCache.EntryMisses, uint64(2))
		require.GreaterOrEqual(t, snap.NamespaceCache.NegativeEntryHits, uint64(1))
		require.Equal(t, uint64(1), snap.NamespaceCache.NegativeEntryMisses)
		require.GreaterOrEqual(t, snap.NamespaceCache.AttrHits, uint64(1))
		require.Equal(t, uint64(1), snap.NamespaceCache.ReadDirHits)
		require.Equal(t, uint64(1), snap.NamespaceCache.ReadDirMisses)
		require.Equal(t, uint64(1), snap.NamespaceCache.Evictions)
		require.Positive(t, snap.NamespaceCache.Entries)
		require.Equal(t, uint64(1), snap.CapabilityCache.Misses)
		require.Equal(t, uint64(1), snap.CapabilityCache.Hits)
		require.GreaterOrEqual(t, snap.CapabilityCache.Grants, uint64(1))
		require.Equal(t, 1, snap.CapabilityCache.Entries)
		require.Equal(t, 1, snap.Handles.OpenDirs)

		ins.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpen.Fh})
		ins.Raw().caps.reset()
		require.Equal(t, uint64(1), ins.Snapshot().CapabilityCache.EpochInvalidations)
	})

	t.Run("ReadAheadCoordinator预读命中与窗口关闭丢弃字节数", func(t *testing.T) {
		prefetchDone := make(chan struct{})
		svc := &obsTestServer{
			readPayload: bytes.Repeat([]byte{'A'}, 3*MaxRPCChunkSize),
			prefetchHit: prefetchDone,
		}
		ins, _, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		ins.Raw().PrepareTestFileHandle(t, 10, 1, syscall.O_RDONLY)

		// 第 1 次读取 64 KiB 直通（建立顺序读流起点）
		buf64K := make([]byte, 64*1024)
		res, st := ins.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1, Offset: 0, Size: uint32(len(buf64K))}, buf64K)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, 64*1024, res.Size())

		// 第 2 次顺序读 [64 KiB, 192 KiB) 触发窗口 0 (4 MiB) 拉取 + 窗口 1 (4 MiB) 后台预取
		buf128K := make([]byte, 128*1024)
		res, st = ins.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1, Offset: 64 * 1024, Size: uint32(len(buf128K))}, buf128K)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, 128*1024, res.Size())

		// 第 3 次顺序读 [192 KiB, 320 KiB) 直接命中窗口 0 内存
		res, st = ins.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1, Offset: 192 * 1024, Size: uint32(len(buf128K))}, buf128K)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, 128*1024, res.Size())

		// 等待窗口 1 ([4 MiB, 8 MiB)) 后台预取在服务端响应并落入 prefetchFuture.ready
		select {
		case <-prefetchDone:
		case <-time.After(3 * time.Second):
			t.Fatal("等待后台预取完成超时")
		}
		require.Eventually(t, func() bool {
			s := ins.Snapshot().ReadAhead
			return s.Inflight == 0 && s.PrefetchedBytes == 2*MaxRPCChunkSize
		}, 2*time.Second, 10*time.Millisecond)

		snapBeforeClose := ins.Snapshot().ReadAhead
		require.Equal(t, uint64(1), snapBeforeClose.Triggers)
		require.Equal(t, uint64(2*MaxRPCChunkSize), snapBeforeClose.PrefetchedBytes)
		require.Equal(t, uint64(1), snapBeforeClose.Hits)
		require.Equal(t, uint64(128*1024), snapBeforeClose.HitBytes)
		require.Equal(t, uint64(0), snapBeforeClose.DiscardedBytes)

		// 关闭句柄：窗口 0 仅消费了 256 KiB（第 2、3 次各 128 KiB），剩余 (4 MiB - 256 KiB) 未消费，
		// 加上已就绪但未提升的窗口 1 整块 4 MiB，总丢弃字节数必须精确等于 (8 MiB - 256 KiB)。
		ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1})

		snapAfterClose := ins.Snapshot().ReadAhead
		expectedDiscarded := uint64(2*MaxRPCChunkSize - 256*1024)
		require.Equal(t, expectedDiscarded, snapAfterClose.DiscardedBytes)
	})

	t.Run("TmpFileManager驻留字节与回滚失败告警及转正", func(t *testing.T) {
		svc := &obsTestServer{}
		ins, _, logBuf := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		var tmpOut fuse.CreateOut
		require.Equal(t, fuse.OK, ins.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    syscall.O_RDWR,
			Mode:     0o600,
		}, &tmpOut))
		require.Equal(t, 1, ins.Snapshot().TmpFile.ActiveFiles)

		// 写入 6 KiB -> 分配 2 个 4 KiB 稀疏页 = 8192 B
		payload := bytes.Repeat([]byte{'T'}, 6*1024)
		n, st := ins.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh, Offset: 0}, payload)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, uint32(len(payload)), n)
		require.Equal(t, uint64(2*tmpSparsePageSize), ins.Snapshot().TmpFile.ResidentBytes)

		// 截断至 2 KiB -> 释放第 2 页 -> 驻留降为 4096 B
		var attrOut fuse.AttrOut
		require.Equal(t, fuse.OK, ins.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: tmpOut.NodeId},
				Valid:    fuse.FATTR_SIZE,
				Size:     2048,
			},
		}, &attrOut))
		require.Equal(t, uint64(tmpSparsePageSize), ins.Snapshot().TmpFile.ResidentBytes)

		// 注入上云 Write 失败 + 回滚 Unlink 失败：必须记录回滚错误与结构化告警，且保留本地页数据
		svc.failWrite.Store(true)
		svc.failUnlink.Store(true)
		var linkOut fuse.EntryOut
		st = ins.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "promoted.txt", &linkOut)
		require.NotEqual(t, fuse.OK, st)

		tmpSnap := ins.Snapshot().TmpFile
		require.Equal(t, uint64(1), tmpSnap.Rollbacks)
		require.Equal(t, uint64(1), tmpSnap.RollbackErrors)
		require.Equal(t, uint64(tmpSparsePageSize), tmpSnap.ResidentBytes)
		require.Equal(t, 1, tmpSnap.ActiveFiles)
		require.Contains(t, logBuf.String(), "tmpfile_rollback_failed")

		// 恢复服务端后再次 Link 转正 -> promotes=1, resident_bytes=0, active_files=0
		svc.failWrite.Store(false)
		svc.failUnlink.Store(false)
		require.Equal(t, fuse.OK, ins.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "promoted.txt", &linkOut))

		tmpSnapAfter := ins.Snapshot().TmpFile
		require.Equal(t, uint64(1), tmpSnapAfter.Promotes)
		require.Equal(t, uint64(0), tmpSnapAfter.ResidentBytes)
		require.Equal(t, 0, tmpSnapAfter.ActiveFiles)
	})

	t.Run("LockTable与inodeLocks竞争指标", func(t *testing.T) {
		svc := &obsTestServer{}
		ins, _, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		ins.Raw().PrepareTestFileHandle(t, 10, 1, syscall.O_RDWR)
		ins.Raw().PrepareTestFileHandle(t, 10, 2, syscall.O_RDWR)
		require.Equal(t, 2, ins.Snapshot().Handles.OpenFiles)

		// 1. LockTable 冲突与阻塞等待
		require.Equal(t, fuse.OK, ins.SetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 10},
			Fh:       1,
			Owner:    1,
			Lk:       fuse.FileLock{Start: 0, End: 100, Typ: uint32(syscall.F_WRLCK)},
		}))
		require.Equal(t, 1, ins.Snapshot().Locks.ActiveLocks)

		// 非阻塞冲突 -> EAGAIN
		require.Equal(t, fuse.EAGAIN, ins.SetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 10},
			Fh:       2,
			Owner:    2,
			Lk:       fuse.FileLock{Start: 0, End: 100, Typ: uint32(syscall.F_WRLCK)},
		}))

		// 阻塞等待 SetLkw
		waitDone := make(chan fuse.Status, 1)
		go func() {
			waitDone <- ins.SetLkw(nil, &fuse.LkIn{
				InHeader: fuse.InHeader{NodeId: 10},
				Fh:       2,
				Owner:    2,
				Lk:       fuse.FileLock{Start: 0, End: 100, Typ: uint32(syscall.F_WRLCK)},
			})
		}()
		require.Eventually(t, func() bool {
			return ins.Snapshot().Locks.Conflicts >= 2
		}, 2*time.Second, 5*time.Millisecond)
		time.Sleep(15 * time.Millisecond)

		require.Equal(t, fuse.OK, ins.SetLk(nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 10},
			Fh:       1,
			Owner:    1,
			Lk:       fuse.FileLock{Start: 0, End: 100, Typ: uint32(syscall.F_UNLCK)},
		}))
		select {
		case st := <-waitDone:
			require.Equal(t, fuse.OK, st)
		case <-time.After(2 * time.Second):
			t.Fatal("SetLkw 未在解锁后唤醒")
		}

		lockSnap := ins.Snapshot().Locks
		require.Equal(t, uint64(2), lockSnap.Conflicts)
		require.Equal(t, uint64(1), lockSnap.Waits)
		require.Greater(t, lockSnap.WaitSeconds, 0.0)

		// 2. inodeLocks 快路径 vs 竞争路径
		unlockFirst := ins.Raw().inodeLocks.lock(777)
		contendedDone := make(chan struct{})
		go func() {
			u := ins.Raw().inodeLocks.lock(777)
			u()
			close(contendedDone)
		}()
		time.Sleep(15 * time.Millisecond)
		unlockFirst()
		select {
		case <-contendedDone:
		case <-time.After(2 * time.Second):
			t.Fatal("inodeLocks 竞争协程未完成")
		}

		ilSnap := ins.Snapshot().InodeLocks
		require.GreaterOrEqual(t, ilSnap.FastAcquisitions, uint64(1))
		require.GreaterOrEqual(t, ilSnap.ContendedAcquisitions, uint64(1))
		require.Greater(t, ilSnap.WaitSeconds, 0.0)
		require.Equal(t, 0, ilSnap.ActiveEntries)
	})

	t.Run("异步队列指标与孤儿回收积压1s限频告警及Snapshot多格式导出", func(t *testing.T) {
		svc := &obsTestServer{}
		svc.failReclaim.Store(3)
		ins, _, logBuf := startObsTestEnv(t, svc, Config{OrphanBacklogThreshold: 1})
		t.Cleanup(ins.OnUnmount)

		ins.Raw().PrepareTestFileHandle(t, 10, 1, syscall.O_RDWR)

		// 写入标记 dirty -> Fsync 失败 -> Fsync 成功
		_, st := ins.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1, Offset: 0}, []byte("dirty"))
		require.Equal(t, fuse.OK, st)
		require.Equal(t, 1, ins.Snapshot().Queues.DirtyInodes)

		svc.failFsync.Store(true)
		require.NotEqual(t, fuse.OK, ins.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1}))
		require.Equal(t, uint64(1), ins.Snapshot().Queues.DirtyFlushErrors)

		svc.failFsync.Store(false)
		require.Equal(t, fuse.OK, ins.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1}))
		require.Equal(t, uint64(1), ins.Snapshot().Queues.DirtyFlushOK)
		require.Equal(t, 0, ins.Snapshot().Queues.DirtyInodes)

		// 打开状态下 Unlink 标记孤儿 -> Release 触发连续 3 次失败后自动重试恢复
		require.Equal(t, fuse.OK, ins.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "exists.txt"))
		require.Equal(t, 1, ins.Snapshot().Queues.OrphanInodes)

		ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1})

		require.Eventually(t, func() bool {
			q := ins.Snapshot().Queues
			return q.ReleaseQueueDepth == 0 && q.OrphanReclaimOK == 1 && q.AsyncReleaseOK == 1
		}, 3*time.Second, 20*time.Millisecond)

		qSnap := ins.Snapshot().Queues
		require.Equal(t, uint64(3), qSnap.OrphanReclaimErrors)
		require.Equal(t, uint64(2), qSnap.AsyncReleaseErrors)

		// 3 次失败均发生在 <1s 窗口内，1s 限频器必须将 orphan_reclaim_backlogged 压缩为恰好 1 条告警
		logs := logBuf.String()
		require.Equal(t, 1, strings.Count(logs, "orphan_reclaim_backlogged"), "1s 窗口内孤儿积压告警必须严格限频为 1 条: %s", logs)

		// 校验 Snapshot 三种输出格式契约（FormatJSON / FormatText / FormatStatsFile）
		snap := ins.Snapshot()
		rawJSON, err := snap.FormatJSON()
		require.NoError(t, err)
		var decoded Snapshot
		require.NoError(t, json.Unmarshal(rawJSON, &decoded))
		require.Equal(t, snap.FUSE.TotalCalls, decoded.FUSE.TotalCalls)

		txt := snap.FormatText()
		for _, section := range []string{"FUSE:", "NamespaceCache:", "CapabilityCache:", "ReadAhead:", "TmpFile:", "Handles:", "Locks:", "InodeLocks:", "Queues:"} {
			require.Contains(t, txt, section)
		}

		statsText := snap.FormatStatsFile()
		parsedStats := make(map[string]float64)
		for line := range strings.SplitSeq(strings.TrimSpace(statsText), "\n") {
			fields := strings.Fields(line)
			require.Len(t, fields, 2, "juicefs bench 解析契约要求每行严格两列: %q", line)
			val, parseErr := strconv.ParseFloat(fields[1], 64)
			require.NoError(t, parseErr, "第二列必须为合法浮点数: %q", line)
			parsedStats[fields[0]] = val
		}
		for _, jfsKey := range []string{
			"juicefs_uptime",
			"juicefs_cpu_usage",
			"juicefs_memory",
			"juicefs_fuse_ops_durations_histogram_seconds_total",
			"juicefs_fuse_ops_durations_histogram_seconds_sum",
			"juicefs_transaction_durations_histogram_seconds_total",
			"juicefs_transaction_durations_histogram_seconds_sum",
			"juicefs_object_request_durations_histogram_seconds_PUT_total",
			"juicefs_object_request_durations_histogram_seconds_PUT_sum",
			"juicefs_object_request_durations_histogram_seconds_GET_total",
			"juicefs_object_request_durations_histogram_seconds_GET_sum",
			"juicefs_object_request_durations_histogram_seconds_DELETE_total",
			"juicefs_object_request_durations_histogram_seconds_DELETE_sum",
			"juicefs_blockcache_write_hist_seconds_total",
			"juicefs_blockcache_write_hist_seconds_sum",
			"juicefs_blockcache_read_hist_seconds_total",
			"juicefs_blockcache_read_hist_seconds_sum",
		} {
			_, ok := parsedStats[jfsKey]
			require.True(t, ok, ".stats 快照缺少 juicefs bench 必需键 %s", jfsKey)
		}
	})
}

func TestVirtualStatsFile_OptInRootOnlyDirectIOReadOnlyHiddenFromReadDir(t *testing.T) {
	t.Run("默认关闭时不拦截虚拟文件", func(t *testing.T) {
		svc := &obsTestServer{}
		ins, _, _ := startObsTestEnv(t, svc, Config{EnableStatsFile: false})
		t.Cleanup(ins.OnUnmount)

		var out fuse.EntryOut
		require.Equal(t, fuse.ENOENT, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".stats", &out))
		require.Equal(t, fuse.ENOENT, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, ".jfs.stats", &out))
	})

	t.Run("开启后仅根目录Lookup可见且DirectIO动态只读且ReadDir隐藏", func(t *testing.T) {
		svc := &obsTestServer{}
		ins, _, _ := startObsTestEnv(t, svc, Config{EnableStatsFile: true})
		t.Cleanup(ins.OnUnmount)

		// 1. 根目录 Lookup .stats 与 .jfs.stats 返回 VirtualStatsIno，AttrValid=0，非零 Size（兼容内核 sendfile），零 RPC
		for _, name := range []string{".stats", ".jfs.stats"} {
			var out fuse.EntryOut
			require.Equal(t, fuse.OK, ins.Lookup(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, name, &out))
			require.Equal(t, VirtualStatsIno, out.NodeId)
			require.Equal(t, VirtualStatsIno, out.Ino)
			require.Positive(t, out.Size)
			require.Equal(t, uint32(syscall.S_IFREG|0o444), out.Mode)
			require.Equal(t, uint64(0), out.AttrValid)
			require.Equal(t, uint32(0), out.AttrValidNsec)
			require.Equal(t, uint64(0), out.EntryValid)
			require.Equal(t, uint32(0), out.EntryValidNsec)
		}
		svc.mu.Lock()
		require.Empty(t, svc.methods, "根目录虚拟统计文件 Lookup 不得触发任何服务端 RPC")
		svc.mu.Unlock()

		// 2. 子目录同名文件不拦截
		var subOut fuse.EntryOut
		require.Equal(t, fuse.ENOENT, ins.Lookup(nil, &fuse.InHeader{NodeId: 20}, ".stats", &subOut))

		// 3. GetAttr 返回 AttrValid=0、非零 Size 与 0444 权限
		var attrOut fuse.AttrOut
		require.Equal(t, fuse.OK, ins.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: VirtualStatsIno}}, &attrOut))
		require.Equal(t, uint64(0), attrOut.AttrValid)
		require.Positive(t, attrOut.Size)
		require.Equal(t, uint32(syscall.S_IFREG|0o444), attrOut.Mode)

		// 4. Open 拒绝任何写标志并返回 EACCES
		for _, badFlags := range []uint32{
			syscall.O_WRONLY,
			syscall.O_RDWR,
			syscall.O_RDONLY | syscall.O_TRUNC,
			syscall.O_RDONLY | syscall.O_APPEND,
		} {
			var openOut fuse.OpenOut
			st := ins.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: VirtualStatsIno},
				Flags:    badFlags,
			}, &openOut)
			require.Equal(t, fuse.EACCES, st, "写标志 %#x 打开虚拟统计文件必须返回 EACCES", badFlags)
		}

		// 5. 只读 Open 成功返回 FOPEN_DIRECT_IO，分段 Read 无撕裂且重读偏移 0 动态刷新
		var openOut fuse.OpenOut
		require.Equal(t, fuse.OK, ins.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: VirtualStatsIno},
			Flags:    syscall.O_RDONLY,
		}, &openOut))
		require.Equal(t, uint32(fuse.FOPEN_DIRECT_IO), openOut.OpenFlags)

		readFull := func() map[string]float64 {
			var full []byte
			chunk := make([]byte, 256)
			for offset := uint64(0); ; {
				res, st := ins.Read(nil, &fuse.ReadIn{
					InHeader: fuse.InHeader{NodeId: VirtualStatsIno},
					Fh:       openOut.Fh,
					Offset:   offset,
					Size:     uint32(len(chunk)),
				}, chunk)
				require.Equal(t, fuse.OK, st)
				if res.Size() == 0 {
					break
				}
				b, st := res.Bytes(chunk)
				require.Equal(t, fuse.OK, st)
				full = append(full, b...)
				offset += uint64(len(b))
			}
			// 越界偏移安全返回空切片
			eofRes, st := ins.Read(nil, &fuse.ReadIn{
				InHeader: fuse.InHeader{NodeId: VirtualStatsIno},
				Fh:       openOut.Fh,
				Offset:   1 << 20,
				Size:     256,
			}, chunk)
			require.Equal(t, fuse.OK, st)
			require.Equal(t, 0, eofRes.Size())

			out := make(map[string]float64)
			for line := range strings.SplitSeq(strings.TrimSpace(string(full)), "\n") {
				fields := strings.Fields(line)
				require.Len(t, fields, 2)
				v, err := strconv.ParseFloat(fields[1], 64)
				require.NoError(t, err)
				out[fields[0]] = v
			}
			return out
		}

		firstSnap := readFull()
		var stOut fuse.StatfsOut
		for range 5 {
			require.Equal(t, fuse.OK, ins.StatFs(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, &stOut))
		}
		secondSnap := readFull()
		require.Greater(t,
			secondSnap["juicefs_fuse_ops_durations_histogram_seconds_total"],
			firstSnap["juicefs_fuse_ops_durations_histogram_seconds_total"],
			"从偏移 0 重新读取 .stats 必须动态反映新增 FUSE 操作计数",
		)

		ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: VirtualStatsIno}, Fh: openOut.Fh})

		// 6. ReadDir 与 ReadDirPlus 严禁暴露 .stats 或 .jfs.stats
		var dirOpen fuse.OpenOut
		require.Equal(t, fuse.OK, ins.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Flags: syscall.O_RDONLY}, &dirOpen))
		defer ins.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpen.Fh})

		dirBuf := make([]byte, 4096)
		require.Equal(t, fuse.OK, ins.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpen.Fh}, fuse.NewDirEntryList(dirBuf, 0)))
		require.NotContains(t, string(dirBuf), ".stats")
		require.NotContains(t, string(dirBuf), ".jfs.stats")

		clear(dirBuf)
		require.Equal(t, fuse.OK, ins.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, Fh: dirOpen.Fh}, fuse.NewDirEntryList(dirBuf, 0)))
		require.NotContains(t, string(dirBuf), ".stats")
		require.NotContains(t, string(dirBuf), ".jfs.stats")
	})
}

func findSpansByNameSubstr(spans []sdktrace.ReadOnlySpan, sub string) []sdktrace.ReadOnlySpan {
	var matched []sdktrace.ReadOnlySpan
	for _, sp := range spans {
		if strings.Contains(sp.Name(), sub) {
			matched = append(matched, sp)
		}
	}
	return matched
}

func TestObservability_RequestIDAndFourTraceContinuityPaths(t *testing.T) {
	t.Run("路径1_Write含O_APPEND多阶段RPC与RequestID单调注入", func(t *testing.T) {
		svc := &obsTestServer{fileSize: 512}
		ins, recorder, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		ins.Raw().PrepareTestFileHandle(t, 10, 1, syscall.O_WRONLY|syscall.O_APPEND)
		// 清空缓存迫使 O_APPEND Write 在同一操作内先后触发 GetAttr RPC 与多片 Write RPC
		ins.Raw().cache.clear()
		recorder.Reset()
		svc.mu.Lock()
		svc.reqIDs = nil
		svc.mu.Unlock()

		bigPayload := bytes.Repeat([]byte{'W'}, MaxRPCChunkSize+1024)
		written, st := ins.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: 10},
			Fh:       1,
			Offset:   0,
		}, bigPayload)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, uint32(len(bigPayload)), written)

		spans := recorder.Ended()
		rootSpans := findSpansByNameSubstr(spans, "fuse.Write")
		require.Len(t, rootSpans, 1)
		wantTraceID := rootSpans[0].SpanContext().TraceID()
		require.True(t, wantTraceID.IsValid())

		getAttrSpans := findSpansByNameSubstr(spans, "StorageService/GetAttr")
		writeSpans := findSpansByNameSubstr(spans, "StorageService/Write")
		require.NotEmpty(t, getAttrSpans)
		require.GreaterOrEqual(t, len(writeSpans), 2)

		for _, sp := range getAttrSpans {
			require.Equal(t, wantTraceID, sp.SpanContext().TraceID(), "O_APPEND GetAttr 必须与 fuse.Write 共享同一 TraceID")
		}
		for _, sp := range writeSpans {
			require.Equal(t, wantTraceID, sp.SpanContext().TraceID(), "分片 Write RPC 必须与 fuse.Write 共享同一 TraceID")
		}

		// 验证所有出站 RPC 携带严格单调递增非零 X-Request-Id
		svc.mu.Lock()
		reqIDs := append([]uint64(nil), svc.reqIDs...)
		svc.mu.Unlock()
		require.Len(t, reqIDs, 3)
		for i, id := range reqIDs {
			require.Positive(t, id)
			if i > 0 {
				require.Greater(t, id, reqIDs[i-1], "X-Request-Id 必须单调递增")
			}
		}
	})

	t.Run("路径2_RollbackTmpInode在父Context取消后仍保留TraceID完成回滚", func(t *testing.T) {
		svc := &obsTestServer{}
		ins, recorder, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		// 场景 A: 通过 RawFS.Link 上云中途失败触发自动回滚，Mknod/Write/回滚 Unlink 共享同一 TraceID
		var tmpOut fuse.CreateOut
		require.Equal(t, fuse.OK, ins.Tmpfile(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Flags:    syscall.O_RDWR,
			Mode:     0o600,
		}, &tmpOut))
		_, st := ins.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh, Offset: 0}, []byte("tmp-data"))
		require.Equal(t, fuse.OK, st)

		recorder.Reset()
		svc.failWrite.Store(true)
		var linkOut fuse.EntryOut
		require.NotEqual(t, fuse.OK, ins.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Oldnodeid: tmpOut.NodeId,
		}, "fail-promote.txt", &linkOut))

		spans := recorder.Ended()
		linkRoots := findSpansByNameSubstr(spans, "fuse.Link")
		require.Len(t, linkRoots, 1)
		linkTraceID := linkRoots[0].SpanContext().TraceID()

		unlinkSpans := findSpansByNameSubstr(spans, "StorageService/Unlink")
		require.NotEmpty(t, unlinkSpans)
		for _, sp := range unlinkSpans {
			require.Equal(t, linkTraceID, sp.SpanContext().TraceID(), "回滚 Unlink RPC 必须继承 fuse.Link 的 TraceID")
		}

		// 场景 B: 父 Context 已被取消时直接调用 RollbackTmpInode，context.WithoutCancel 必须保证回滚不被中断且保留父 TraceID
		recorder.Reset()
		parentCtx, parentSpan := ins.Raw().startOpSpan(context.Background(), "parent.CanceledUpload")
		wantCanceledTraceID := trace.SpanContextFromContext(parentCtx).TraceID()
		canceledCtx, cancelNow := context.WithCancel(parentCtx)
		cancelNow()
		require.ErrorIs(t, canceledCtx.Err(), context.Canceled)

		mat := ins.Raw().newTmpMaterializer(nil)
		mat.RollbackTmpInode(canceledCtx, 1, "orphan-tmp")
		parentSpan.End()

		canceledSpans := recorder.Ended()
		rollbackUnlinks := findSpansByNameSubstr(canceledSpans, "StorageService/Unlink")
		require.NotEmpty(t, rollbackUnlinks)
		for _, sp := range rollbackUnlinks {
			require.Equal(t, wantCanceledTraceID, sp.SpanContext().TraceID(), "父 Context 取消后 RollbackTmpInode 仍必须保持同一 TraceID")
		}
	})

	t.Run("路径3_ReadAheadCoordinator后台异步预读继承触发Read的TraceID", func(t *testing.T) {
		prefetchDone := make(chan struct{})
		svc := &obsTestServer{
			readPayload: bytes.Repeat([]byte{'P'}, 3*MaxRPCChunkSize),
			prefetchHit: prefetchDone,
		}
		ins, recorder, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		ins.Raw().PrepareTestFileHandle(t, 10, 1, syscall.O_RDONLY)

		buf64K := make([]byte, 64*1024)
		_, st := ins.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1, Offset: 0, Size: uint32(len(buf64K))}, buf64K)
		require.Equal(t, fuse.OK, st)

		recorder.Reset()
		svc.mu.Lock()
		svc.reqIDs = nil
		svc.mu.Unlock()

		// 此次前台 Read 返回后其 RPCContext 立即被 defer cancelFunc() 取消，后台预读协程异步拉取 [4 MiB, 8 MiB)
		buf128K := make([]byte, 128*1024)
		_, st = ins.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1, Offset: 64 * 1024, Size: uint32(len(buf128K))}, buf128K)
		require.Equal(t, fuse.OK, st)

		select {
		case <-prefetchDone:
		case <-time.After(3 * time.Second):
			t.Fatal("等待后台异步预取 RPC 超时")
		}
		require.Eventually(t, func() bool {
			return len(findSpansByNameSubstr(recorder.Ended(), "StorageService/Read")) >= 4
		}, 2*time.Second, 10*time.Millisecond)

		spans := recorder.Ended()
		readRoot := findSpansByNameSubstr(spans, "fuse.Read")
		require.Len(t, readRoot, 1)
		wantTraceID := readRoot[0].SpanContext().TraceID()

		rpcReadSpans := findSpansByNameSubstr(spans, "StorageService/Read")
		require.GreaterOrEqual(t, len(rpcReadSpans), 2)
		for _, sp := range rpcReadSpans {
			require.Equal(t, wantTraceID, sp.SpanContext().TraceID(), "前台对齐读与后台异步预读 RPC 必须共享触发 fuse.Read 的 TraceID")
		}

		svc.mu.Lock()
		reqIDs := append([]uint64(nil), svc.reqIDs...)
		svc.mu.Unlock()
		require.Len(t, reqIDs, 2)
		require.NotEqual(t, reqIDs[0], reqIDs[1], "前台读与后台预读应各自具备唯一 RequestID")
	})

	t.Run("路径4_孤儿节点异步Release重试队列保持TraceID不断链", func(t *testing.T) {
		svc := &obsTestServer{}
		// 首次同步 ReclaimOrphan 失败进入异步 releaseQueue，后台 ticker 第二次重试成功
		svc.failReclaim.Store(1)
		ins, recorder, _ := startObsTestEnv(t, svc, Config{})
		t.Cleanup(ins.OnUnmount)

		ins.Raw().PrepareTestFileHandle(t, 10, 1, syscall.O_RDWR)
		require.Equal(t, fuse.OK, ins.Unlink(nil, &fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID}, "exists.txt"))

		recorder.Reset()
		ins.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: 1})

		require.Eventually(t, func() bool {
			svc.mu.Lock()
			defer svc.mu.Unlock()
			return len(svc.reclaimInos) == 1
		}, 3*time.Second, 20*time.Millisecond)

		require.Eventually(t, func() bool {
			// 客户端 + 服务端 otelconnect 各记录 2 次尝试（共 4 个 ReclaimOrphan Span）
			return len(findSpansByNameSubstr(recorder.Ended(), "StorageService/ReclaimOrphan")) >= 4
		}, 2*time.Second, 10*time.Millisecond)

		spans := recorder.Ended()
		releaseRoots := findSpansByNameSubstr(spans, "fuse.Release")
		require.Len(t, releaseRoots, 1)
		wantTraceID := releaseRoots[0].SpanContext().TraceID()
		require.True(t, wantTraceID.IsValid())

		reclaimSpans := findSpansByNameSubstr(spans, "StorageService/ReclaimOrphan")
		require.GreaterOrEqual(t, len(reclaimSpans), 4)
		for _, sp := range reclaimSpans {
			require.Equal(t, wantTraceID, sp.SpanContext().TraceID(), "首次与异步队列重试的 ReclaimOrphan RPC 必须保持同一 TraceID")
		}
	})
}
