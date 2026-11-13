package fusefs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
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
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type memFile struct {
	inode *storagev1.Inode
	data  []byte
	link  []byte
}

// testStorageServer 是挂载在真实 ConnectRPC HTTP 传输上的有状态服务端替身，
// 用于端到端验证客户端 FUSE 根 Span 透传、WriteBehind/ReadAhead 流水线、池化编解码与元数据缓存。
type testStorageServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu            sync.Mutex
	nextIno       uint64
	attrTTL       float64
	entryTTL      float64
	files         map[uint64]*memFile
	entries       map[uint64]map[string]uint64
	readDirHook   func(ctx context.Context, req *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error)
	readHook      func(ctx context.Context, req *storagev1.ReadRequest) ([]byte, error)
	writeHook     func(ctx context.Context, req *storagev1.WriteRequest) error
	lookupCalls   atomic.Int64
	readlinkCalls atomic.Int64
	fsyncCalls    atomic.Int64
	reclaimCalls  atomic.Int64
	readCalls     atomic.Int64
	writeCalls    atomic.Int64
}

func newTestStorageServer(entryTTL, attrTTL float64) *testStorageServer {
	s := &testStorageServer{
		nextIno:  100,
		attrTTL:  attrTTL,
		entryTTL: entryTTL,
		files:    make(map[uint64]*memFile),
		entries:  make(map[uint64]map[string]uint64),
	}
	now := timestamppb.Now()
	s.files[1] = &memFile{
		inode: &storagev1.Inode{
			Ino:        1,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:  syscall.S_IFDIR | 0o755,
				Nlink: 2,
				Mtime: now,
				Ctime: now,
			},
		},
	}
	s.entries[1] = make(map[string]uint64)
	return s
}

func cloneProtoInode(src *storagev1.Inode) *storagev1.Inode {
	return cloneInode(src)
}

func (s *testStorageServer) Lookup(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	s.lookupCalls.Add(1)
	name := string(req.Msg.GetName())
	if len(name) > MaxFilenameBytes {
		return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("filename too long"))
	}
	if name == "forbidden" {
		return nil, connect.NewError(connect.CodePermissionDenied, errors.New("permission denied"))
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.entries[req.Msg.GetParentIno()]
	ino := dir[name]
	f := s.files[ino]
	if ino == 0 || f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               cloneProtoInode(f.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

func (s *testStorageServer) GetAttr(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[req.Msg.GetIno()]
	if f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              cloneProtoInode(f.inode),
		AttrTimeoutSeconds: s.attrTTL,
	}), nil
}

func (s *testStorageServer) Mkdir(_ context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIno++
	ino := s.nextIno
	now := timestamppb.Now()
	node := &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFDIR | (req.Msg.GetMode() & 0o7777),
			Nlink: 2,
			Mtime: now,
			Ctime: now,
		},
	}
	s.files[ino] = &memFile{inode: node}
	s.entries[ino] = make(map[string]uint64)
	if s.entries[req.Msg.GetParentIno()] == nil {
		s.entries[req.Msg.GetParentIno()] = make(map[string]uint64)
	}
	s.entries[req.Msg.GetParentIno()][string(req.Msg.GetName())] = ino
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               cloneProtoInode(node),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

func (s *testStorageServer) Create(_ context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIno++
	ino := s.nextIno
	now := timestamppb.Now()
	node := &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | (req.Msg.GetMode() & 0o7777),
			Nlink: 1,
			Mtime: now,
			Ctime: now,
		},
	}
	s.files[ino] = &memFile{inode: node}
	if s.entries[req.Msg.GetParentIno()] == nil {
		s.entries[req.Msg.GetParentIno()] = make(map[string]uint64)
	}
	s.entries[req.Msg.GetParentIno()][string(req.Msg.GetName())] = ino
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               cloneProtoInode(node),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

func (s *testStorageServer) Symlink(_ context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextIno++
	ino := s.nextIno
	now := timestamppb.Now()
	target := append([]byte(nil), req.Msg.GetTarget()...)
	node := &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFLNK | 0o777,
			Nlink: 1,
			Size:  uint64(len(target)),
			Mtime: now,
			Ctime: now,
		},
	}
	s.files[ino] = &memFile{inode: node, link: target}
	if s.entries[req.Msg.GetParentIno()] == nil {
		s.entries[req.Msg.GetParentIno()] = make(map[string]uint64)
	}
	s.entries[req.Msg.GetParentIno()][string(req.Msg.GetName())] = ino
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               cloneProtoInode(node),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

func (s *testStorageServer) Readlink(_ context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	s.readlinkCalls.Add(1)
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[req.Msg.GetIno()]
	if f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	return connect.NewResponse(&storagev1.ReadlinkResponse{
		Target: append([]byte(nil), f.link...),
	}), nil
}

func (s *testStorageServer) Link(_ context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[req.Msg.GetIno()]
	if f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	f.inode.Attr.Nlink++
	f.inode.Attr.Ctime = timestamppb.Now()
	if s.entries[req.Msg.GetNewParentIno()] == nil {
		s.entries[req.Msg.GetNewParentIno()] = make(map[string]uint64)
	}
	s.entries[req.Msg.GetNewParentIno()][string(req.Msg.GetNewName())] = req.Msg.GetIno()
	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               cloneProtoInode(f.inode),
		EntryTimeoutSeconds: s.entryTTL,
		AttrTimeoutSeconds:  s.attrTTL,
	}), nil
}

func (s *testStorageServer) Unlink(_ context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dir := s.entries[req.Msg.GetParentIno()]
	name := string(req.Msg.GetName())
	ino := dir[name]
	if ino == 0 {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	delete(dir, name)
	if f := s.files[ino]; f != nil {
		if f.inode.Attr.Nlink > 0 {
			f.inode.Attr.Nlink--
		}
		if f.inode.Attr.Nlink == 0 && !req.Msg.GetOpenInClient() {
			delete(s.files, ino)
		}
	}
	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

func (s *testStorageServer) Rename(_ context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	oldDir := s.entries[req.Msg.GetOldParentIno()]
	oldName := string(req.Msg.GetOldName())
	srcIno := oldDir[oldName]
	if srcIno == 0 {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("src not found"))
	}
	if s.entries[req.Msg.GetNewParentIno()] == nil {
		s.entries[req.Msg.GetNewParentIno()] = make(map[string]uint64)
	}
	newDir := s.entries[req.Msg.GetNewParentIno()]
	newName := string(req.Msg.GetNewName())
	if dstIno := newDir[newName]; dstIno != 0 && dstIno != srcIno {
		if dstFile := s.files[dstIno]; dstFile != nil {
			if dstFile.inode.Attr.Nlink > 0 {
				dstFile.inode.Attr.Nlink--
			}
			if dstFile.inode.Attr.Nlink == 0 && !req.Msg.GetTargetOpenInClient() {
				delete(s.files, dstIno)
			}
		}
	}
	delete(oldDir, oldName)
	newDir[newName] = srcIno
	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

func (s *testStorageServer) ReclaimOrphan(_ context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	s.reclaimCalls.Add(1)
	s.mu.Lock()
	delete(s.files, req.Msg.GetIno())
	s.mu.Unlock()
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}

func (s *testStorageServer) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	s.writeCalls.Add(1)
	if s.writeHook != nil {
		if err := s.writeHook(ctx, req.Msg); err != nil {
			return nil, err
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[req.Msg.GetIno()]
	if f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	end := int(req.Msg.GetOffset()) + len(req.Msg.GetData())
	if end > len(f.data) {
		grown := make([]byte, end)
		copy(grown, f.data)
		f.data = grown
	}
	copy(f.data[req.Msg.GetOffset():end], req.Msg.GetData())
	f.inode.Attr.Size = uint64(len(f.data))
	f.inode.Attr.Mtime = timestamppb.Now()
	f.inode.Attr.Ctime = f.inode.Attr.Mtime
	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(len(req.Msg.GetData())),
		Inode: cloneProtoInode(f.inode),
	}), nil
}

func (s *testStorageServer) Read(ctx context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	s.readCalls.Add(1)
	if s.readHook != nil {
		data, err := s.readHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&storagev1.ReadResponse{Data: data}), nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	f := s.files[req.Msg.GetIno()]
	if f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	off := int(req.Msg.GetOffset())
	if off >= len(f.data) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	end := min(off+int(req.Msg.GetSize()), len(f.data))
	out := append([]byte(nil), f.data[off:end]...)
	return connect.NewResponse(&storagev1.ReadResponse{Data: out}), nil
}

func (s *testStorageServer) Fsync(_ context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	s.fsyncCalls.Add(1)
	s.mu.Lock()
	f := s.files[req.Msg.GetIno()]
	s.mu.Unlock()
	if f == nil {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("not found"))
	}
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (s *testStorageServer) ReadDirPlus(ctx context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	if s.readDirHook != nil {
		entries, err := s.readDirHook(ctx, req.Msg)
		if err != nil {
			return nil, err
		}
		return connect.NewResponse(&storagev1.ReadDirPlusResponse{Entries: entries}), nil
	}
	return connect.NewResponse(&storagev1.ReadDirPlusResponse{}), nil
}

func setupTestClientFS(t *testing.T, srv *testStorageServer, tp trace.TracerProvider, entryTTL, attrTTL float64) *RawFS {
	t.Helper()
	var handlerOpts []connect.HandlerOption
	if tp != nil {
		prevProp := otel.GetTextMapPropagator()
		otel.SetTextMapPropagator(propagation.TraceContext{})
		prevTP := otel.GetTracerProvider()
		otel.SetTracerProvider(tp)
		t.Cleanup(func() {
			otel.SetTracerProvider(prevTP)
			otel.SetTextMapPropagator(prevProp)
		})
		otelInt, err := otelconnect.NewInterceptor(
			otelconnect.WithTracerProvider(tp),
			otelconnect.WithTrustRemote(),
		)
		require.NoError(t, err)
		handlerOpts = append(handlerOpts, connect.WithInterceptors(otelInt))
	}

	mux := http.NewServeMux()
	path, h := storagev1connect.NewStorageServiceHandler(srv, handlerOpts...)
	mux.Handle(path, h)
	httpSrv := httptest.NewUnstartedServer(mux)
	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	httpSrv.Config.Protocols = &protocols
	httpSrv.Start()
	t.Cleanup(httpSrv.Close)

	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint: httpSrv.URL,
	})
	require.NoError(t, err)

	return NewRawFS(client, Config{
		DiskID:              "disk-test",
		Epoch:               1,
		RootIno:             1,
		RootInode:           cloneProtoInode(srv.files[1].inode),
		EntryTimeoutSeconds: entryTTL,
		AttrTimeoutSeconds:  attrTTL,
		TracerProvider:      tp,
	})
}

func TestMetrics_26Buckets41OpsAndSysfsGauges(t *testing.T) {
	var nilFS *RawFS
	if nilFS.Collector() != nil {
		t.Fatal("nil RawFS 的 Collector() 必须返回无类型 nil")
	}
	var emptyFS RawFS
	if emptyFS.Collector() != nil {
		t.Fatal("未绑定 metrics 的 RawFS 的 Collector() 必须返回无类型 nil")
	}

	srv := newTestStorageServer(1.0, 1.0)
	fs := setupTestClientFS(t, srv, nil, 1.0, 1.0)
	m := fs.Metrics()

	// 1. 验证 26 桶覆盖至 ~33.55s：模拟 2.5s 慢操作，落入桶 22 (<= 4.19s)，绝不被旧 524ms 截断。
	m.beginOp(OpFsync)
	m.endOp(OpFsync, time.Now().Add(-2500*time.Millisecond), fuse.OK, 0)

	// 2. 验证新增补齐的 5 个 FUSE 操作埋点（Access / Lseek / Writev / CopyFileRange / Statx），并确保 Writev 不双计 OpWrite。
	var cOut fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDWR,
		Mode:     0o644,
	}, "metrics-writev.bin", &cOut))
	_ = fs.Access(nil, &fuse.AccessIn{})
	_ = fs.Lseek(nil, &fuse.LseekIn{}, &fuse.LseekOut{})
	written, st := fs.Writev(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: cOut.NodeId},
		Fh:       cOut.Fh,
		Size:     3,
	}, [][]byte{[]byte("abc")})
	require.Equal(t, fuse.OK, st)
	require.Equal(t, uint32(3), written)
	_, _ = fs.CopyFileRange(nil, &fuse.CopyFileRangeIn{})
	_ = fs.Statx(nil, &fuse.StatxIn{}, &fuse.StatxOut{})

	// 3. 验证 Linux /sys/fs/fuse/connections/<dev> waiting 与 congestion_threshold 采集。
	fakeSysfs := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(fakeSysfs, "waiting"), []byte("7\n"), 0o600))
	require.NoError(t, os.WriteFile(filepath.Join(fakeSysfs, "congestion_threshold"), []byte("48\n"), 0o600))
	fs.setFuseSysfsDir(fakeSysfs)

	snap := fs.Snapshot()
	require.Len(t, snap.FUSE.Ops, 41)
	require.Len(t, snap.FUSE.Ops[OpFsync].Buckets, 26)
	var highBucketSum uint64
	for i := 20; i < 26; i++ {
		highBucketSum += snap.FUSE.Ops[OpFsync].Buckets[i]
	}
	require.Equal(t, uint64(1), highBucketSum, ">524ms 的 2.5s 延迟必须落入 [20..25] 高位桶而不得截断丢失")

	for _, op := range []FuseOp{OpAccess, OpLseek, OpWritev, OpCopyFileRange, OpStatx} {
		require.Equal(t, uint64(1), snap.FUSE.Ops[op].Calls, "%s 调用计数应被精确记录", op.String())
	}
	require.Equal(t, uint64(3), snap.FUSE.Ops[OpWritev].Bytes, "Writev 字节数应精确累加至 OpWritev")
	require.Zero(t, snap.FUSE.Ops[OpWrite].Calls, "Writev 不得重复触发 OpWrite 计数")
	require.Equal(t, 7, snap.Queues.FuseWaiting)
	require.Equal(t, 48, snap.Queues.FuseCongestionThreshold)

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(fs.Collector()))
	require.InDelta(t, float64(7), testutil.ToFloat64(collectSingleGauge(t, reg, "bowl_client_fuse_connection_waiting")), 1e-9)
	require.InDelta(t, float64(48), testutil.ToFloat64(collectSingleGauge(t, reg, "bowl_client_fuse_connection_congestion_threshold")), 1e-9)

	jsonBytes, err := snap.FormatJSON()
	require.NoError(t, err)
	require.Contains(t, string(jsonBytes), `"write_behind"`)
	var decoded Snapshot
	require.NoError(t, json.Unmarshal(jsonBytes, &decoded))
	require.Equal(t, 7, decoded.Queues.FuseWaiting)
	require.Equal(t, 48, decoded.Queues.FuseCongestionThreshold)
	require.Contains(t, snap.FormatText(), "WriteBehind")
	require.Contains(t, snap.FormatStatsFile(), "bowl_client_write_behind_staged_calls_total")
	require.Contains(t, snap.FormatStatsFile(), "bowl_client_fuse_connection_waiting 7")
}

func collectSingleGauge(t *testing.T, reg *prometheus.Registry, metricName string) prometheus.Collector {
	t.Helper()
	families, err := reg.Gather()
	require.NoError(t, err)
	for _, mf := range families {
		if mf.GetName() == metricName && len(mf.GetMetric()) == 1 {
			val := mf.GetMetric()[0].GetGauge().GetValue()
			g := prometheus.NewGauge(prometheus.GaugeOpts{Name: metricName, Help: "test"})
			g.Set(val)
			return g
		}
	}
	t.Fatalf("未找到指标 %s", metricName)
	return nil
}

func TestFuseRootSpans_ErrorAttributesAndReadDirPlusPagination(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	srv := newTestStorageServer(1.0, 1.0)
	var pageTraceIDs []trace.TraceID
	var pageMu sync.Mutex
	srv.readDirHook = func(ctx context.Context, req *storagev1.ReadDirPlusRequest) ([]*storagev1.DirEntryPlus, error) {
		pageMu.Lock()
		pageTraceIDs = append(pageTraceIDs, trace.SpanFromContext(ctx).SpanContext().TraceID())
		pageMu.Unlock()

		var count int
		startOff := req.GetOffset()
		if startOff == 0 {
			count = 512
		} else {
			count = 88
		}
		out := make([]*storagev1.DirEntryPlus, count)
		for i := range count {
			idx := startOff + uint64(i) + 1
			out[i] = &storagev1.DirEntryPlus{
				Ino:        1000 + idx,
				Name:       []byte("f-" + strings.Repeat("x", 4)),
				NextOffset: idx,
				Inode:      testInode(1000+idx, syscall.S_IFREG|0o644, 1),
			}
		}
		return out, nil
	}

	inst := setupTestClientFS(t, srv, tp, 1.0, 1.0)

	// 1. 非零错误状态（EACCES）必须记录 fuse.errno / fuse.status_code 并标红 Span；ENOENT 记录属性但不标红。
	var entryOut fuse.EntryOut
	require.Equal(t, fuse.EACCES, inst.Lookup(nil, &fuse.InHeader{NodeId: 1}, "forbidden", &entryOut))
	require.Equal(t, fuse.ENOENT, inst.Lookup(nil, &fuse.InHeader{NodeId: 1}, "missing-file", &entryOut))

	// 2. 多页 ReadDirPlus（600 条目 -> 512 + 88 两页 RPC）必须挂载在同一 fuse.ReadDirPlus 根 Span 下，严禁分页裂变。
	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, inst.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &openOut))
	dirBuf := make([]byte, 64<<10)
	dirList := fuse.NewDirEntryList(dirBuf, 0)
	require.Equal(t, fuse.OK, inst.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       openOut.Fh,
		Offset:   0,
		Size:     uint32(len(dirBuf)),
	}, dirList))
	inst.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: openOut.Fh})

	pageMu.Lock()
	require.Len(t, pageTraceIDs, 2, "600 项大目录应触发 2 页 ReadDirPlus RPC")
	require.True(t, pageTraceIDs[0].IsValid())
	require.Equal(t, pageTraceIDs[0], pageTraceIDs[1], "同一 ReadDirPlus 分页请求必须共享同一根 TraceID")
	readDirTraceID := pageTraceIDs[0]
	pageMu.Unlock()

	spans := exporter.GetSpans()
	var readDirRootSpanID trace.SpanID
	var foundEACCES, foundENOENT bool
	for _, sp := range spans {
		if sp.Name == "fuse.ReadDirPlus" && sp.SpanContext.TraceID() == readDirTraceID && !sp.Parent.IsValid() {
			readDirRootSpanID = sp.SpanContext.SpanID()
		}
		if sp.Name == "fuse.Lookup" {
			attrs := make(map[string]any, len(sp.Attributes))
			for _, kv := range sp.Attributes {
				attrs[string(kv.Key)] = kv.Value.AsInterface()
			}
			switch attrs["fuse.errno"] {
			case "EACCES":
				foundEACCES = true
				require.Equal(t, codes.Error, sp.Status.Code)
				require.NotZero(t, attrs["fuse.status_code"])
			case "ENOENT":
				foundENOENT = true
				require.NotEqual(t, codes.Error, sp.Status.Code)
			}
		}
	}
	require.True(t, readDirRootSpanID.IsValid(), "必须生成顶层根 Span fuse.ReadDirPlus")
	var clientPageSpans int
	for _, sp := range spans {
		if sp.Name == "storage.v1.StorageService/ReadDirPlus" && sp.Parent.SpanID() == readDirRootSpanID {
			clientPageSpans++
		}
	}
	require.Equal(t, 2, clientPageSpans, "两页客户端 ReadDirPlus RPC Span 均必须挂载在同一 fuse.ReadDirPlus 父 Span 下")
	require.True(t, foundEACCES, "EACCES Lookup 必须记录 fuse.errno=EACCES 并标为 Error")
	require.True(t, foundENOENT, "ENOENT Lookup 必须记录 fuse.errno=ENOENT 且不误标 Error")
}

func TestWriteBehind_ObservabilityBackpressureBarrierAndSpanLinks(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	srv := newTestStorageServer(1.0, 1.0)
	blockFirstFour := make(chan struct{})
	blockFifth := make(chan struct{})
	startedInflight := make(chan struct{}, 16)
	var asyncChunkSeq atomic.Int32
	srv.writeHook = func(_ context.Context, req *storagev1.WriteRequest) error {
		if len(req.GetData()) == WriteBehindChunkSize {
			seq := asyncChunkSeq.Add(1)
			startedInflight <- struct{}{}
			if seq <= WriteBehindMaxInFlight {
				<-blockFirstFour
			} else {
				<-blockFifth
			}
		}
		return nil
	}

	inst := setupTestClientFS(t, srv, tp, 1.0, 1.0)

	var createOut fuse.CreateOut
	require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDWR,
		Mode:     0o644,
	}, "wb.bin", &createOut))

	// 前 2 次 1 MiB 为同步探针直通；随后 20 次 1 MiB 聚合为 5 个 4 MiB 异步块（填满 W*=4 并在第 5 块触发背压等待）。
	chunk1M := bytes.Repeat([]byte("w"), 1<<20)
	writerErrCh := make(chan fuse.Status, 1)
	go func() {
		for i := range 22 {
			w, st := inst.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: createOut.NodeId},
				Fh:       createOut.Fh,
				Offset:   uint64(i) * (1 << 20),
				Size:     uint32(len(chunk1M)),
			}, chunk1M)
			if st != fuse.OK {
				writerErrCh <- st
				return
			}
			if w != uint32(len(chunk1M)) {
				writerErrCh <- fuse.EIO
				return
			}
		}
		writerErrCh <- fuse.OK
	}()

	for range WriteBehindMaxInFlight {
		<-startedInflight
	}
	// 让第 5 个 4 MiB 块进入 waitBackpressureLocked 阻塞。
	time.Sleep(15 * time.Millisecond)
	close(blockFirstFour)
	// 等待全部 22 次写入返回且第 5 个异步块启动并卡在 blockFifth。
	require.Equal(t, fuse.OK, <-writerErrCh)
	<-startedInflight

	// 发起 Flush 使其确定性进入 BarrierWait，15ms 后放行第 5 个异步块。
	flushDone := make(chan fuse.Status, 1)
	go func() {
		flushDone <- inst.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
		})
	}()
	time.Sleep(15 * time.Millisecond)
	close(blockFifth)
	require.Equal(t, fuse.OK, <-flushDone)

	// 再发一次 O_SYNC 句柄直通写验证 passthrough 统计累加。
	var syncOpen fuse.OpenOut
	require.Equal(t, fuse.OK, inst.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Flags:    syscall.O_WRONLY | syscall.O_SYNC,
	}, &syncOpen))
	_, st := inst.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: createOut.NodeId},
		Fh:       syncOpen.Fh,
		Offset:   22 << 20,
		Size:     4096,
	}, bytes.Repeat([]byte("s"), 4096))
	require.Equal(t, fuse.OK, st)

	snap := inst.Snapshot()
	require.Equal(t, uint64(20), snap.WriteBehind.StagedCalls)
	require.Equal(t, uint64(20<<20), snap.WriteBehind.StagedBytes)
	require.Equal(t, uint64(3), snap.WriteBehind.PassthroughCalls)
	require.Equal(t, uint64((2<<20)+4096), snap.WriteBehind.PassthroughBytes)
	require.Equal(t, uint64(5), snap.WriteBehind.FlushRPCs)
	require.Zero(t, snap.WriteBehind.FlushErrors)
	require.GreaterOrEqual(t, snap.WriteBehind.BackpressureWaits, uint64(1))
	require.Greater(t, snap.WriteBehind.BackpressureWaitSeconds, 0.0)
	require.GreaterOrEqual(t, snap.WriteBehind.BarrierWaits, uint64(1))
	require.Greater(t, snap.WriteBehind.BarrierWaitSeconds, 0.0)

	// 验证异步 write_behind.FlushChunk 为独立根 Span 且携带指向前台 fuse.Write 的 SpanLink。
	spans := exporter.GetSpans()
	fuseWriteSpans := make(map[trace.SpanID]bool)
	for _, sp := range spans {
		if sp.Name == "fuse.Write" {
			fuseWriteSpans[sp.SpanContext.SpanID()] = true
		}
	}
	var flushChunkCount, linkedFlushChunks, backpressureSpans, barrierSpans int
	for _, sp := range spans {
		switch sp.Name {
		case "write_behind.FlushChunk":
			flushChunkCount++
			require.False(t, sp.Parent.IsValid(), "write_behind.FlushChunk 必须为独立根 Span")
			for _, link := range sp.Links {
				if fuseWriteSpans[link.SpanContext.SpanID()] {
					linkedFlushChunks++
					break
				}
			}
		case "write_behind.BackpressureWait":
			backpressureSpans++
		case "write_behind.BarrierWait":
			barrierSpans++
		}
	}
	require.Equal(t, 5, flushChunkCount)
	require.Equal(t, 5, linkedFlushChunks, "每个异步刷盘根 Span 必须通过 SpanLink 关联前台写入者")
	require.GreaterOrEqual(t, backpressureSpans, 1)
	require.GreaterOrEqual(t, barrierSpans, 1)
}

func TestReadAhead_MultiWindowPipelinePooledDecodeAndSpans(t *testing.T) {
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSyncer(exporter))
	defer func() { _ = tp.Shutdown(context.Background()) }()

	srv := newTestStorageServer(1.0, 1.0)
	const fileSize = 32 << 20 // 8 * 4 MiB
	expected := make([]byte, fileSize)
	for i := range expected {
		expected[i] = byte((i*31 + 7) & 0xff)
	}

	var activeReads atomic.Int32
	var maxConcurrent atomic.Int32
	releasePrefetchGate := make(chan struct{})
	var gateOnce sync.Once
	allFourInflight := make(chan struct{})
	var fourOnce sync.Once

	srv.readHook = func(_ context.Context, req *storagev1.ReadRequest) ([]byte, error) {
		off := int(req.GetOffset())
		sz := int(req.GetSize())
		// 窗口 1..4（偏移 4 MiB .. 16 MiB）并发预取闸门：实证同时在途达到 W_read = 4。
		if off >= MaxRPCChunkSize && off <= 4*MaxRPCChunkSize && sz == MaxRPCChunkSize {
			cur := activeReads.Add(1)
			for {
				prev := maxConcurrent.Load()
				if cur <= prev || maxConcurrent.CompareAndSwap(prev, cur) {
					break
				}
			}
			if cur >= ReadAheadMaxPrefetch {
				fourOnce.Do(func() { close(allFourInflight) })
			}
			<-releasePrefetchGate
			activeReads.Add(-1)
		}
		if off >= len(expected) {
			return nil, nil
		}
		end := min(off+sz, len(expected))
		return expected[off:end], nil
	}

	inst := setupTestClientFS(t, srv, tp, 1.0, 1.0)
	var createOut fuse.CreateOut
	require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDONLY,
		Mode:     0o644,
	}, "seq.bin", &createOut))

	readAtRaw := func(off uint64, size int) ([]byte, fuse.Status) {
		buf := make([]byte, size)
		res, st := inst.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   off,
			Size:     uint32(size),
		}, buf)
		if st != fuse.OK {
			return nil, st
		}
		data, st := res.Bytes(buf)
		if st != fuse.OK {
			return nil, st
		}
		return append([]byte(nil), data...), fuse.OK
	}
	readAt := func(off uint64, size int) []byte {
		data, st := readAtRaw(off, size)
		require.Equal(t, fuse.OK, st)
		return data
	}

	// 第 1 次顺序读 [0, 1 MiB)：直通；第 2 次顺序读 [1 MiB, 2 MiB)：激活 4 MiB 窗口 0 并并发拉起窗口 1..4 共 4 个预取！
	got0 := readAt(0, 1<<20)
	require.Equal(t, expected[:1<<20], got0)
	got1 := readAt(1<<20, 1<<20)
	require.Equal(t, expected[1<<20:2<<20], got1)

	select {
	case <-allFourInflight:
	case <-time.After(3 * time.Second):
		t.Fatalf("顺序流激活后未拉起 W_read=%d 个并发预取，当前最大并发=%d", ReadAheadMaxPrefetch, maxConcurrent.Load())
	}
	require.Equal(t, int32(ReadAheadMaxPrefetch), maxConcurrent.Load(), "ReadAhead 流水线并发度必须达到 W_read=4")

	// 消费完窗口 0 后读取窗口 1 [4 MiB, 5 MiB)，使其进入 read_ahead.Wait 等待，再放行闸门。
	type readOutcome struct {
		data []byte
		st   fuse.Status
	}
	readWin1Done := make(chan readOutcome, 2)
	go func() {
		d1, s1 := readAtRaw(2<<20, 2<<20)
		readWin1Done <- readOutcome{data: d1, st: s1}
		d2, s2 := readAtRaw(4<<20, 1<<20)
		readWin1Done <- readOutcome{data: d2, st: s2}
	}()
	out1 := <-readWin1Done
	require.Equal(t, fuse.OK, out1.st)
	require.Equal(t, expected[2<<20:4<<20], out1.data)
	time.Sleep(15 * time.Millisecond)
	gateOnce.Do(func() { close(releasePrefetchGate) })
	out2 := <-readWin1Done
	require.Equal(t, fuse.OK, out2.st)
	require.Equal(t, expected[4<<20:5<<20], out2.data)

	// 验证跨 4 MiB 窗口切窗后慢一步到达的上一窗口乱序尾巴读命中 prevWinBuf（不触发额外 RPC）。
	callsBeforeStraggler := srv.readCalls.Load()
	straggler := readAt(3<<20, 1<<20)
	require.Equal(t, expected[3<<20:4<<20], straggler)
	// 把剩余 [5 MiB, 32 MiB) 全部顺序读完并逐字节校验零拷贝池化解码无任何串包或 UAF。
	for off := 5 << 20; off < fileSize; off += 1 << 20 {
		chunk := readAt(uint64(off), 1<<20)
		require.Equal(t, expected[off:off+(1<<20)], chunk)
	}
	_ = callsBeforeStraggler

	snap := inst.Snapshot()
	require.GreaterOrEqual(t, snap.ReadAhead.Triggers, uint64(4))
	require.GreaterOrEqual(t, snap.ReadAhead.Waits, uint64(1))
	require.Greater(t, snap.ReadAhead.WaitSeconds, 0.0)

	// 验证 read_ahead.Prefetch 为独立根 Span（不寄生在前序 fuse.Read 下虚增耗时），且挂载 SpanLink；read_ahead.Wait 为子 Span。
	spans := exporter.GetSpans()
	fuseReadSpans := make(map[trace.SpanID]bool)
	for _, sp := range spans {
		if sp.Name == "fuse.Read" {
			fuseReadSpans[sp.SpanContext.SpanID()] = true
		}
	}
	var prefetchRoots, linkedPrefetches, waitSpans int
	for _, sp := range spans {
		switch sp.Name {
		case "read_ahead.Prefetch":
			require.False(t, sp.Parent.IsValid(), "read_ahead.Prefetch 必须是独立根 Span，严禁挂在前序 fuse.Read 下")
			prefetchRoots++
			for _, link := range sp.Links {
				if fuseReadSpans[link.SpanContext.SpanID()] {
					linkedPrefetches++
					break
				}
			}
		case "read_ahead.Wait":
			require.True(t, sp.Parent.IsValid(), "read_ahead.Wait 必须挂在阻塞的 fuse.Read 下")
			waitSpans++
		}
	}
	require.GreaterOrEqual(t, prefetchRoots, 4)
	require.Equal(t, prefetchRoots, linkedPrefetches, "每个 read_ahead.Prefetch 必须通过 SpanLink 指向触发它的 fuse.Read")
	require.GreaterOrEqual(t, waitSpans, 1)

	inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: createOut.NodeId}, Fh: createOut.Fh})
}

func TestContendedLocks_InodeLockAndLockTableWaitMetrics(t *testing.T) {
	m := NewMetrics()
	var il inodeLocks
	il.bindMetrics(m)

	unlockFirst := il.lock(42)
	lockedSecond := make(chan struct{})
	go func() {
		u := il.lock(42)
		u()
		close(lockedSecond)
	}()
	time.Sleep(10 * time.Millisecond)
	unlockFirst()
	<-lockedSecond

	lt := NewLockTable()
	lt.bindMetrics(m)
	lt.RegisterHandle(1, syscall.O_RDWR)
	lt.RegisterHandle(2, syscall.O_RDWR)

	require.Equal(t, fuse.OK, lt.SetLk(&fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 99},
		Fh:       1,
		Owner:    1001,
		Lk:       fuse.FileLock{Start: 0, End: 100, Typ: syscall.F_WRLCK},
	}))

	waitDone := make(chan fuse.Status, 1)
	go func() {
		waitDone <- lt.SetLkw(context.Background(), nil, &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 99},
			Fh:       2,
			Owner:    1002,
			Lk:       fuse.FileLock{Start: 0, End: 100, Typ: syscall.F_WRLCK},
		})
	}()
	time.Sleep(10 * time.Millisecond)
	require.Equal(t, fuse.OK, lt.SetLk(&fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 99},
		Fh:       1,
		Owner:    1001,
		Lk:       fuse.FileLock{Start: 0, End: 100, Typ: syscall.F_UNLCK},
	}))
	require.Equal(t, fuse.OK, <-waitDone)

	snap := m.Snapshot()
	require.Equal(t, uint64(1), snap.InodeLocks.ContendedAcquisitions)
	require.Greater(t, snap.InodeLocks.WaitSeconds, 0.0)
	require.Equal(t, uint64(1), snap.Locks.Waits)
	require.Greater(t, snap.Locks.WaitSeconds, 0.0)

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(m))
	families, err := reg.Gather()
	require.NoError(t, err)
	found := make(map[string]bool)
	for _, mf := range families {
		found[mf.GetName()] = true
		switch mf.GetName() {
		case "bowl_client_lock_wait_duration_seconds", "bowl_client_inodelock_wait_duration_seconds":
			require.Equal(t, uint64(1), mf.GetMetric()[0].GetHistogram().GetSampleCount())
		case "bowl_client_lock_wait_seconds_total":
			require.Greater(t, mf.GetMetric()[0].GetCounter().GetValue(), 0.0)
		}
	}
	require.True(t, found["bowl_client_lock_wait_seconds_total"])
	require.True(t, found["bowl_client_lock_wait_duration_seconds"])
	require.True(t, found["bowl_client_inodelock_wait_duration_seconds"])
}

func TestUnlinkAndRename_DirtyInodesCleanupPreservingHardlinks(t *testing.T) {
	t.Run("单硬链接关闭后Unlink与打开态孤儿Release均彻底清除dirtyInodes", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)
		payload := []byte("dirty-data")

		// 文件 A：Create -> Write (O_SYNC 直通置脏) -> Release -> Unlink
		var cA fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR | syscall.O_SYNC,
			Mode:     0o644,
		}, "a.txt", &cA))
		_, st := inst.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: cA.NodeId},
			Fh:       cA.Fh,
			Size:     uint32(len(payload)),
		}, payload)
		require.Equal(t, fuse.OK, st)
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cA.NodeId}, Fh: cA.Fh})
		require.Equal(t, fuse.OK, inst.Unlink(nil, &fuse.InHeader{NodeId: 1}, "a.txt"))

		// 文件 B：Create -> Write -> 打开态 Unlink（成孤儿）-> Release（触发 ReclaimOrphan + clearDirty）
		var cB fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR | syscall.O_SYNC,
			Mode:     0o644,
		}, "b.txt", &cB))
		_, st = inst.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: cB.NodeId},
			Fh:       cB.Fh,
			Size:     uint32(len(payload)),
		}, payload)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, fuse.OK, inst.Unlink(nil, &fuse.InHeader{NodeId: 1}, "b.txt"))
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cB.NodeId}, Fh: cB.Fh})
		require.Equal(t, int64(1), srv.reclaimCalls.Load())

		// 文件 C 被文件 D Rename 覆盖：C 的脏位被清理，仅存活的 D 保留脏位！
		var cC, cD fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR | syscall.O_SYNC,
			Mode:     0o644,
		}, "c.txt", &cC))
		_, _ = inst.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: cC.NodeId}, Fh: cC.Fh, Size: uint32(len(payload))}, payload)
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cC.NodeId}, Fh: cC.Fh})

		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR | syscall.O_SYNC,
			Mode:     0o644,
		}, "d.txt", &cD))
		_, _ = inst.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: cD.NodeId}, Fh: cD.Fh, Size: uint32(len(payload))}, payload)
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: cD.NodeId}, Fh: cD.Fh})

		require.Equal(t, fuse.OK, inst.Rename(nil, &fuse.RenameIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Newdir:   1,
		}, "d.txt", "c.txt"))

		inst.OnUnmount()
		require.Equal(t, int64(1), srv.fsyncCalls.Load(), "OnUnmount 只应对唯一存活的脏节点发起 1 次 Fsync，严禁对已删节点触发 not_found Fsync 风暴")
		require.Zero(t, inst.Snapshot().Queues.DirtyFlushErrors)
	})

	t.Run("多硬链接nlink大于1时Unlink一个名字保留脏位直至最后一个链接删除", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)
		payload := []byte("hardlink-dirty")

		var c1 fuse.CreateOut
		require.Equal(t, fuse.OK, inst.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    syscall.O_RDWR | syscall.O_SYNC,
			Mode:     0o644,
		}, "h1.txt", &c1))
		_, st := inst.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: c1.NodeId},
			Fh:       c1.Fh,
			Size:     uint32(len(payload)),
		}, payload)
		require.Equal(t, fuse.OK, st)
		inst.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: c1.NodeId}, Fh: c1.Fh})

		var linkOut fuse.EntryOut
		require.Equal(t, fuse.OK, inst.Link(nil, &fuse.LinkIn{
			InHeader:  fuse.InHeader{NodeId: 1},
			Oldnodeid: c1.NodeId,
		}, "h2.txt", &linkOut))

		// 删除第 1 个硬链接（此时解链前 nlink == 2，h2.txt 仍存活）：不得误清脏标记！
		require.Equal(t, fuse.OK, inst.Unlink(nil, &fuse.InHeader{NodeId: 1}, "h1.txt"))
		require.Equal(t, 1, inst.Snapshot().Queues.DirtyInodes, "剩余硬链接存活时必须保留 dirtyInodes")

		// 再删除最后 1 个硬链接 h2.txt（此时解链前 nlink == 1）：必须清除脏标记！
		require.Equal(t, fuse.OK, inst.Unlink(nil, &fuse.InHeader{NodeId: 1}, "h2.txt"))
		require.Zero(t, inst.Snapshot().Queues.DirtyInodes, "最后一个硬链接删除后必须清除 dirtyInodes")

		inst.OnUnmount()
		require.Zero(t, srv.fsyncCalls.Load())
	})
}

func TestCompleteDirsOverlongNameAndReadlinkTTLCache(t *testing.T) {
	t.Run("completeDirs命中完整空目录时256字节超长名优先返回ENAMETOOLONG", func(t *testing.T) {
		srv := newTestStorageServer(1.0, 1.0)
		inst := setupTestClientFS(t, srv, nil, 1.0, 1.0)

		var dirOut fuse.EntryOut
		require.Equal(t, fuse.OK, inst.Mkdir(nil, &fuse.MkdirIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Mode:     0o755,
		}, "empty-dir", &dirOut))

		var lookupOut fuse.EntryOut
		longName := strings.Repeat("a", 256)
		require.Equal(t, fuse.Status(syscall.ENAMETOOLONG), inst.Lookup(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, longName, &lookupOut))

		// 合法 255 字节缺失名仍直接走 completeDirs 本地负缓存返回 ENOENT，0 次 RPC。
		validMissing := strings.Repeat("b", 255)
		before := srv.lookupCalls.Load()
		require.Equal(t, fuse.ENOENT, inst.Lookup(nil, &fuse.InHeader{NodeId: dirOut.NodeId}, validMissing, &lookupOut))
		require.Equal(t, before, srv.lookupCalls.Load())
	})

	t.Run("Readlink在strict_0s模式不缓存_在1s生产TTL下命中缓存并随Unlink失效", func(t *testing.T) {
		// 1) strict 0s 模式（pjdfstest）：绝不缓存 Readlink
		strictSrv := newTestStorageServer(0, 0)
		strictFS := setupTestClientFS(t, strictSrv, nil, 0, 0)

		var symStrict fuse.EntryOut
		require.Equal(t, fuse.OK, strictFS.Symlink(nil, &fuse.InHeader{NodeId: 1}, "/target/strict", "s0", &symStrict))
		for range 3 {
			got, st := strictFS.Readlink(nil, &fuse.InHeader{NodeId: symStrict.NodeId})
			require.Equal(t, fuse.OK, st)
			require.Equal(t, "/target/strict", string(got))
		}
		require.Equal(t, int64(3), strictSrv.readlinkCalls.Load(), "AttrTimeout=0 时每次 Readlink 必须穿透服务端")
		require.Zero(t, strictFS.Snapshot().NamespaceCache.ReadlinkHits)

		// 2) 1.0s TTL 生产模式：Symlink 与 Readlink 命中缓存，Unlink 同步淘汰
		prodSrv := newTestStorageServer(1.0, 1.0)
		prodFS := setupTestClientFS(t, prodSrv, nil, 1.0, 1.0)

		var symProd fuse.EntryOut
		require.Equal(t, fuse.OK, prodFS.Symlink(nil, &fuse.InHeader{NodeId: 1}, "/target/prod", "s1", &symProd))
		for range 5 {
			got, st := prodFS.Readlink(nil, &fuse.InHeader{NodeId: symProd.NodeId})
			require.Equal(t, fuse.OK, st)
			require.Equal(t, "/target/prod", string(got))
		}
		require.Zero(t, prodSrv.readlinkCalls.Load(), "Symlink 预热后 TTL 窗口内 Readlink 应 100% 本地命中")
		require.Equal(t, uint64(5), prodFS.Snapshot().NamespaceCache.ReadlinkHits)

		// Unlink 同步淘汰 Readlink 缓存
		require.Equal(t, fuse.OK, prodFS.Unlink(nil, &fuse.InHeader{NodeId: 1}, "s1"))
		_, st := prodFS.Readlink(nil, &fuse.InHeader{NodeId: symProd.NodeId})
		require.Equal(t, fuse.ENOENT, st)
		require.Equal(t, int64(1), prodSrv.readlinkCalls.Load())
	})
}
