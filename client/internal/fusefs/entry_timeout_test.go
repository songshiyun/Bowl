package fusefs_test

import (
	"context"
	"fmt"
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
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// entryTimeoutMockServer 复刻真实服务端的缓存时长回填：Create / Lookup 返回 1 秒 entry 超时，
// Write 响应只携带属性、不携带 entry timeout。
type entryTimeoutMockServer struct {
	storagev1connect.UnimplementedStorageServiceHandler
	lookupCalls atomic.Int64
	fileIno     uint64
}

func (m *entryTimeoutMockServer) inode(size uint64) *storagev1.Inode {
	return &storagev1.Inode{
		Ino: m.fileIno,
		Attr: &storagev1.InodeAttribute{
			Mode: syscall.S_IFREG | 0o644, Size: size, Nlink: 1, Blksize: 1 << 20,
			Atime: timestamppb.Now(), Mtime: timestamppb.Now(), Ctime: timestamppb.Now(),
		},
	}
}

func (m *entryTimeoutMockServer) RenewSession(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	return connect.NewResponse(&storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Hour))}), nil
}

func (m *entryTimeoutMockServer) Create(context.Context, *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode: m.inode(0), EntryTimeoutSeconds: 1.0, AttrTimeoutSeconds: 1.0,
	}), nil
}

func (m *entryTimeoutMockServer) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	return connect.NewResponse(&storagev1.WriteResponse{
		Size: uint32(len(req.Msg.GetData())), Inode: m.inode(uint64(len(req.Msg.GetData()))),
	}), nil
}

func (m *entryTimeoutMockServer) Lookup(context.Context, *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	m.lookupCalls.Add(1)
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode: m.inode(1), EntryTimeoutSeconds: 1.0, AttrTimeoutSeconds: 1.0,
	}), nil
}

func (m *entryTimeoutMockServer) GetAttr(context.Context, *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	return connect.NewResponse(&storagev1.GetAttrResponse{Inode: m.inode(1), AttrTimeoutSeconds: 1.0}), nil
}

// TestEntryTimeoutSurvivesWriteRefresh 锁定“写路径刷新属性不得清零目录项有效期”的契约。
// Write / SetAttr / Open(O_TRUNC) / GetAttr 的响应不携带 entry timeout；
// 若把 0 回填进客户端缓存，后续 Lookup 命中缓存时会把 EntryValid=0 交给内核，
// 内核据此拒绝缓存 dentry，该文件的每次 stat 都会重新穿透 FUSE Lookup（Hot stat 暴跌）。
func TestEntryTimeoutSurvivesWriteRefresh(t *testing.T) {
	srv := &entryTimeoutMockServer{fileIno: 2002}
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(srv)
	mux.Handle(path, handler)
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)
	client := storagev1connect.NewStorageServiceClient(ts.Client(), ts.URL)

	mgr := setupTestSessionManager(t, client)
	rawFS := fusefs.NewRawFS(client, mgr, fusefs.Config{RootIno: 1001})
	t.Cleanup(rawFS.OnUnmount)

	// 复刻 drive9 bench_stat 的 prepare_dir：逐个 create + write。
	for i := 0; i < 3; i++ {
		var createOut fuse.CreateOut
		st := rawFS.Create(nil, &fuse.CreateIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Flags:    uint32(syscall.O_CREAT | syscall.O_WRONLY),
			Mode:     0o644,
		}, fmt.Sprintf("entry%05d.txt", i), &createOut)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, uint64(1), createOut.EntryValid, "Create 必须回填服务端返回的 1 秒目录项有效期")

		n, st := rawFS.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: createOut.NodeId},
			Fh:       createOut.Fh,
			Offset:   0,
		}, []byte("x"))
		require.Equal(t, fuse.OK, st)
		require.Equal(t, uint32(1), n)
	}

	var lookupOut fuse.EntryOut
	require.Equal(t, fuse.OK, rawFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "entry00000.txt", &lookupOut))
	require.Zero(t, srv.lookupCalls.Load(), "Write 刷新属性后 Lookup 必须命中客户端缓存")
	require.Equal(t, uint64(1), lookupOut.EntryValid, "写路径刷新属性必须保留既有目录项有效期")
}
