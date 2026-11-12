package rpc

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type recordingRoundTripper struct {
	calls      atomic.Int64
	idleClosed atomic.Int64
	lastPathMu sync.Mutex
	seenPaths  []string
}

func (r *recordingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls.Add(1)
	r.lastPathMu.Lock()
	r.seenPaths = append(r.seenPaths, req.URL.Path)
	r.lastPathMu.Unlock()
	return &http.Response{
		StatusCode: http.StatusOK,
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(nil)),
		Request:    req,
	}, nil
}

func (r *recordingRoundTripper) CloseIdleConnections() {
	r.idleClosed.Add(1)
}

func TestShardedRoundTripper_UnitRoutingAndCloseIdle(t *testing.T) {
	metaRT := &recordingRoundTripper{}
	var dataRTs [HTTP2DataConnectionShards]*recordingRoundTripper
	rt := &shardedRoundTripper{
		metaTransport: metaRT,
	}
	for i := range HTTP2DataConnectionShards {
		dataRTs[i] = &recordingRoundTripper{}
		rt.dataTransports[i] = dataRTs[i]
	}

	// 1. Read 与 Write（含带前缀路径）按 8 路严格轮询分发，不触碰 metaTransport
	const rounds = 3
	for r := range rounds {
		for s := range HTTP2DataConnectionShards {
			proc := storagev1connect.StorageServiceReadProcedure
			if (r+s)%2 == 1 {
				proc = "/api/v1" + storagev1connect.StorageServiceWriteProcedure
			}
			req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1:9000"+proc, http.NoBody)
			require.NoError(t, err)
			resp, err := rt.RoundTrip(req)
			require.NoError(t, err)
			_ = resp.Body.Close()
		}
	}
	require.Zero(t, metaRT.calls.Load(), "大块数据面 Read/Write 不得占用元数据专用连接")
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, int64(rounds), dataRTs[i].calls.Load(), "数据条带 %d 必须均匀承接轮询流量", i)
	}

	// 2. 元数据、屏障与控制面 RPC 全部路由至 metaTransport，不触碰任何 dataTransports
	metaProcedures := []string{
		storagev1connect.StorageServiceLookupProcedure,
		storagev1connect.StorageServiceGetAttrProcedure,
		storagev1connect.StorageServiceCreateProcedure,
		storagev1connect.StorageServiceUnlinkProcedure,
		storagev1connect.StorageServiceFsyncProcedure,
		storagev1connect.StorageServiceReadDirPlusProcedure,
		storagev1connect.StorageServiceMountDiskProcedure,
		storagev1connect.StorageServiceStatFSProcedure,
	}
	for _, proc := range metaProcedures {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, "http://127.0.0.1:9000"+proc, http.NoBody)
		require.NoError(t, err)
		resp, err := rt.RoundTrip(req)
		require.NoError(t, err)
		_ = resp.Body.Close()
	}
	require.Equal(t, int64(len(metaProcedures)), metaRT.calls.Load())
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, int64(rounds), dataRTs[i].calls.Load(), "元数据流量不得污染数据条带 %d 计数", i)
	}

	// 3. CloseIdleConnections 转发至 metaTransport 与全部 8 个 dataTransports
	httpCli := &http.Client{Transport: rt}
	httpCli.CloseIdleConnections()
	require.Equal(t, int64(1), metaRT.idleClosed.Load())
	for i := range HTTP2DataConnectionShards {
		require.Equal(t, int64(1), dataRTs[i].idleClosed.Load())
	}
}

type shardedNetProbeHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	payload []byte
}

func (h *shardedNetProbeHandler) Read(_ context.Context, _ *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	return connect.NewResponse(&storagev1.ReadResponse{Data: h.payload}), nil
}

func (h *shardedNetProbeHandler) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(len(req.Msg.GetData()))}), nil
}

func (h *shardedNetProbeHandler) Lookup(_ context.Context, _ *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode: &storagev1.Inode{Ino: 2},
	}), nil
}

func (h *shardedNetProbeHandler) Fsync(_ context.Context, _ *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func TestNewClient_EstablishesIsolatedMetaAnd8DataH2CConnections(t *testing.T) {
	probe := &shardedNetProbeHandler{
		payload: bytes.Repeat([]byte{0x7E}, 64<<10),
	}
	path, connectHandler := storagev1connect.NewStorageServiceHandler(
		probe,
		connect.WithCodec(newProtoBinaryCodec()),
	)

	var (
		mu        sync.Mutex
		dataPeers = make(map[string]int)
		metaPeers = make(map[string]int)
	)
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		if isBulkDataProcedure(r.URL.Path) {
			dataPeers[r.RemoteAddr]++
		} else {
			metaPeers[r.RemoteAddr]++
		}
		mu.Unlock()
		connectHandler.ServeHTTP(w, r)
	}))

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:   mux,
		Protocols: &protocols,
		HTTP2: &http.HTTP2Config{
			MaxReadFrameSize:              HTTP2MaxReadFrameSize,
			MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
			MaxReceiveBufferPerConnection: HTTP2MaxReceiveBufferPerConnection,
		},
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })

	cli, err := NewClient("http://"+ln.Addr().String(), "")
	require.NoError(t, err)

	ctx := context.Background()
	writeBuf := bytes.Repeat([]byte{0x42}, 32<<10)
	for i := range HTTP2DataConnectionShards {
		rResp, rErr := cli.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
			DiskId: "11111111-1111-4111-8111-111111111111",
			Ino:    uint64(10 + i),
			Offset: 0,
			Size:   uint32(len(probe.payload)),
		}))
		require.NoError(t, rErr)
		p := TakeReadPayload(rResp.Msg)
		require.Equal(t, probe.payload, p.Data)
		p.Release()

		wResp, wErr := cli.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: "11111111-1111-4111-8111-111111111111",
			Epoch:  1,
			Ino:    uint64(10 + i),
			Offset: 0,
			Data:   writeBuf,
		}))
		require.NoError(t, wErr)
		require.Equal(t, uint32(len(writeBuf)), wResp.Msg.GetSize())
	}

	for range 4 {
		_, lErr := cli.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
			DiskId:    "11111111-1111-4111-8111-111111111111",
			ParentIno: 1,
			Name:      []byte("f"),
		}))
		require.NoError(t, lErr)
		_, fErr := cli.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
			DiskId: "11111111-1111-4111-8111-111111111111",
			Epoch:  1,
			Ino:    2,
		}))
		require.NoError(t, fErr)
	}

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, dataPeers, HTTP2DataConnectionShards, "8 路数据条带必须建立 8 条独立 H2C TCP 连接")
	require.Len(t, metaPeers, 1, "元数据/屏障流量必须收敛在 1 条专用 H2C TCP 连接")
	for metaAddr := range metaPeers {
		_, shared := dataPeers[metaAddr]
		require.False(t, shared, "元数据专用连接 %s 严禁与大块数据连接共享 TCP 通道", metaAddr)
	}
}
