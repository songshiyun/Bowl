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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	capTestRootIno = 1001
	capTestFileIno = 2002
	capTestFH      = 777
)

func capTestInode(ino uint64, mode uint32, nlink uint32) *storagev1.Inode {
	return &storagev1.Inode{
		Ino:  ino,
		Attr: &storagev1.InodeAttribute{Mode: mode, Nlink: nlink},
	}
}

func capabilityNoDataError() *connect.ErrorDetail {
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: "ENODATA",
		Domain: "bowl.storage",
	})
	if err != nil {
		panic(err)
	}
	return detail
}

type capabilityMockServer struct {
	storagev1connect.UnimplementedStorageServiceHandler

	getAttrCalls   atomic.Int64
	getXattrCalls  atomic.Int64
	writeCalls     atomic.Int64
	createCalls    atomic.Int64
	mknodCalls     atomic.Int64
	setAttrCalls   atomic.Int64
	fallocateCalls atomic.Int64
	present        atomic.Bool
}

func (m *capabilityMockServer) GetAttr(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	m.getAttrCalls.Add(1)
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              capTestInode(req.Msg.GetIno(), syscall.S_IFREG|0o644, 1),
		AttrTimeoutSeconds: 1,
	}), nil
}

func (m *capabilityMockServer) GetXattr(_ context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	m.getXattrCalls.Add(1)
	if string(req.Msg.GetName()) == "security.capability" && m.present.Load() {
		return connect.NewResponse(&storagev1.GetXattrResponse{Value: []byte{0x01, 0x00, 0x00, 0x02}}), nil
	}
	err := connect.NewError(connect.CodeNotFound, errors.New("ENODATA"))
	err.AddDetail(capabilityNoDataError())
	return nil, err
}

func (m *capabilityMockServer) SetXattr(_ context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	if string(req.Msg.GetName()) == "security.capability" {
		m.present.Store(true)
	}
	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

func (m *capabilityMockServer) RemoveXattr(_ context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	if string(req.Msg.GetName()) == "security.capability" {
		m.present.Store(false)
	}
	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}

func (m *capabilityMockServer) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	m.writeCalls.Add(1)
	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(len(req.Msg.GetData())),
		Inode: capTestInode(req.Msg.GetIno(), syscall.S_IFREG|0o644, 1),
	}), nil
}

func (m *capabilityMockServer) SetAttr(_ context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	m.setAttrCalls.Add(1)
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              capTestInode(req.Msg.GetIno(), syscall.S_IFREG|0o644, 1),
		AttrTimeoutSeconds: 1,
	}), nil
}

func (m *capabilityMockServer) Fallocate(_ context.Context, _ *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	m.fallocateCalls.Add(1)
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (m *capabilityMockServer) Create(_ context.Context, _ *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	m.createCalls.Add(1)
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               capTestInode(capTestFileIno, syscall.S_IFREG|0o644, 1),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (m *capabilityMockServer) Mknod(_ context.Context, _ *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	m.mknodCalls.Add(1)
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               capTestInode(capTestFileIno, syscall.S_IFIFO|0o644, 1),
		EntryTimeoutSeconds: 1,
		AttrTimeoutSeconds:  1,
	}), nil
}

func (m *capabilityMockServer) RenewSession(_ context.Context, _ *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	return connect.NewResponse(&storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}), nil
}

func setupCapabilityRPCServer(t *testing.T, srv *capabilityMockServer) storagev1connect.StorageServiceClient {
	t.Helper()
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	return storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)
}

func TestRawFS_SecurityCapabilityNegativeCache(t *testing.T) {
	srv := &capabilityMockServer{}
	client := setupCapabilityRPCServer(t, srv)
	rawFS := fusefs.NewRawFS(client, nil, fusefs.Config{DiskID: "cap-cache", RootIno: capTestRootIno})
	require.Equal(t, fuse.OK, rawFS.PrepareTestHandle(capTestFileIno, capTestFH, syscall.O_RDWR, false))

	header := &fuse.InHeader{NodeId: capTestFileIno}

	// 1. 未知节点首次查询命中服务端 ENODATA，随后本地负缓存直接返回且不再发 RPC。
	_, status := rawFS.GetXAttr(nil, header, "security.capability", nil)
	require.Equal(t, fuse.Status(syscall.ENODATA), status)
	require.EqualValues(t, 1, srv.getXattrCalls.Load())
	_, status = rawFS.GetXAttr(nil, header, "security.capability", nil)
	require.Equal(t, fuse.Status(syscall.ENODATA), status)
	require.EqualValues(t, 1, srv.getXattrCalls.Load(), "负缓存命中后不得再次发起 GetXattr RPC")

	// 2. 显式 SetXattr 后标记为存在，查询必须回源读取真实值。
	require.Equal(t, fuse.OK, rawFS.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: *header}, "security.capability", []byte{0x01}))
	buf := make([]byte, 4)
	n, status := rawFS.GetXAttr(nil, header, "security.capability", buf)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint32(4), n)
	require.EqualValues(t, 2, srv.getXattrCalls.Load())

	// 3. Write 成功后在本地重置为无，后续查询直接返回 ENODATA 且不发 RPC。
	data := make([]byte, 1<<20)
	written, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: *header, Fh: capTestFH, Offset: 0}, data)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint32(len(data)), written)
	require.EqualValues(t, 1, srv.writeCalls.Load())
	before := srv.getXattrCalls.Load()
	_, status = rawFS.GetXAttr(nil, header, "security.capability", nil)
	require.Equal(t, fuse.Status(syscall.ENODATA), status)
	require.EqualValues(t, before, srv.getXattrCalls.Load(), "Write 后负缓存必须直接命中")

	// 4. SetAttr 大小变更、Fallocate、RemoveXattr 同样重置负缓存。
	require.Equal(t, fuse.OK, rawFS.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: *header}, "security.capability", []byte{0x01}))
	set := &fuse.SetAttrIn{}
	set.NodeId = capTestFileIno
	set.Valid = fuse.FATTR_SIZE
	set.Size = 0
	require.Equal(t, fuse.OK, rawFS.SetAttr(nil, set, &fuse.AttrOut{}))
	_, status = rawFS.GetXAttr(nil, header, "security.capability", nil)
	require.Equal(t, fuse.Status(syscall.ENODATA), status)

	require.Equal(t, fuse.OK, rawFS.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: *header}, "security.capability", []byte{0x01}))
	require.Equal(t, fuse.OK, rawFS.Fallocate(nil, &fuse.FallocateIn{InHeader: *header, Fh: capTestFH, Offset: 0, Length: 1}))
	_, status = rawFS.GetXAttr(nil, header, "security.capability", nil)
	require.Equal(t, fuse.Status(syscall.ENODATA), status)

	require.Equal(t, fuse.OK, rawFS.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: *header}, "security.capability", []byte{0x01}))
	require.Equal(t, fuse.OK, rawFS.RemoveXAttr(nil, header, "security.capability"))
	_, status = rawFS.GetXAttr(nil, header, "security.capability", nil)
	require.Equal(t, fuse.Status(syscall.ENODATA), status)
}

func TestRawFS_SecurityCapabilityNegativeCacheOnCreateAndMknod(t *testing.T) {
	srv := &capabilityMockServer{}
	client := setupCapabilityRPCServer(t, srv)

	t.Run("Create 新建文件默认无 security.capability", func(t *testing.T) {
		rawFS := fusefs.NewRawFS(client, nil, fusefs.Config{DiskID: "cap-create", RootIno: capTestRootIno})
		var out fuse.CreateOut
		status := rawFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     0o644,
			Flags:    uint32(syscall.O_RDWR | syscall.O_CREAT),
		}, "newfile", &out)
		require.Equal(t, fuse.OK, status)
		require.EqualValues(t, 1, srv.createCalls.Load())

		before := srv.getXattrCalls.Load()
		_, status = rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: out.NodeId}, "security.capability", nil)
		require.Equal(t, fuse.Status(syscall.ENODATA), status)
		require.EqualValues(t, before, srv.getXattrCalls.Load(), "新建文件不得为 security.capability 发起 RPC")
	})

	t.Run("Mknod 新建节点默认无 security.capability", func(t *testing.T) {
		rawFS := fusefs.NewRawFS(client, nil, fusefs.Config{DiskID: "cap-mknod", RootIno: capTestRootIno})
		var out fuse.EntryOut
		status := rawFS.Mknod(nil, &fuse.MknodIn{
			InHeader: fuse.InHeader{NodeId: fuse.FUSE_ROOT_ID},
			Mode:     syscall.S_IFIFO | 0o644,
		}, "fifo", &out)
		require.Equal(t, fuse.OK, status)
		require.EqualValues(t, 1, srv.mknodCalls.Load())

		before := srv.getXattrCalls.Load()
		_, status = rawFS.GetXAttr(nil, &fuse.InHeader{NodeId: out.NodeId}, "security.capability", nil)
		require.Equal(t, fuse.Status(syscall.ENODATA), status)
		require.EqualValues(t, before, srv.getXattrCalls.Load(), "Mknod 新建节点不得为 security.capability 发起 RPC")
	})
}

func TestRawFS_WriteRetainsAttrCacheTimeout(t *testing.T) {
	srv := &capabilityMockServer{}
	client := setupCapabilityRPCServer(t, srv)
	rawFS := fusefs.NewRawFS(client, nil, fusefs.Config{DiskID: "attr-timeout", RootIno: capTestRootIno})
	require.Equal(t, fuse.OK, rawFS.PrepareTestHandle(capTestFileIno, capTestFH, syscall.O_RDWR, false))

	data := make([]byte, 1<<20)
	_, status := rawFS.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: capTestFileIno}, Fh: capTestFH}, data)
	require.Equal(t, fuse.OK, status)
	require.EqualValues(t, 1, srv.writeCalls.Load())

	var out fuse.AttrOut
	status = rawFS.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: capTestFileIno}}, &out)
	require.Equal(t, fuse.OK, status)
	require.Zero(t, srv.getAttrCalls.Load(), "写后属性缓存有效期内不得触发 GetAttr RPC")
	require.Equal(t, time.Second, out.Timeout(), "写后必须保留 1s 属性缓存，禁止置 0")
}
