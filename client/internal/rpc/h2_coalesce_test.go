package rpc

import (
	"bytes"
	"context"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// autocorkDelayedACKConn 精确模拟 Linux net/ipv4/tcp.c tcp_should_autocork (TSQ_THROTTLED)
// 与虚拟网卡 (virtio-net) 单包 TX 回收延迟 + 服务端 TCP Delayed ACK (10ms) 的交互状态机：
//   - 当客户端发出第 1 个小分片（如 HTTP/2 HEADERS ~60B）且尚未收到服务端响应或 Delayed ACK 时，inflight=true；
//   - 若紧随其后的第 2 次 Write 长度 < sizeGoal (2*MSS = 2896B)，触发 tcp_should_autocork 挂起该包，
//     直至服务端 10ms Delayed ACK 到期或服务端下行响应捎带 ACK 解除 TSQ_THROTTLED；
//   - 若第 2 次 Write 长度 >= sizeGoal（如 4 KiB Write RPC），skb->len < size_goal 为假，立即直通发送。
type autocorkDelayedACKConn struct {
	net.Conn

	sizeGoal   int
	delackWait time.Duration
	writes     *atomic.Int64

	mu       sync.Mutex
	inflight bool
	corked   []byte
	ackTimer *time.Timer
	closed   bool
}

func newAutocorkDelayedACKConn(c net.Conn, sizeGoal int, delackWait time.Duration, writes *atomic.Int64) net.Conn {
	return &autocorkDelayedACKConn{
		Conn:       c,
		sizeGoal:   sizeGoal,
		delackWait: delackWait,
		writes:     writes,
	}
}

func (c *autocorkDelayedACKConn) releaseCorkLocked() {
	if c.ackTimer != nil {
		c.ackTimer.Stop()
	}
	c.inflight = false
	if len(c.corked) > 0 && !c.closed {
		buf := append([]byte(nil), c.corked...)
		c.corked = c.corked[:0]
		_, _ = writeFull(c.Conn, buf)
	}
}

func (c *autocorkDelayedACKConn) onDelayedACK() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.releaseCorkLocked()
}

func (c *autocorkDelayedACKConn) Write(p []byte) (int, error) {
	if c.writes != nil {
		c.writes.Add(1)
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return c.Conn.Write(p)
	}

	// 若已有未确认包在途且本次写小于 size_goal，命中 tcp_should_autocork 暂存等待 ACK
	if c.inflight && len(c.corked)+len(p) < c.sizeGoal {
		c.corked = append(c.corked, p...)
		c.mu.Unlock()
		return len(p), nil
	}

	// 若累积字节或本次写达到 size_goal（如 4 KiB Write），立即突破 autocork 一并下发
	var toSend []byte
	if len(c.corked) > 0 {
		toSend = make([]byte, 0, len(c.corked)+len(p))
		toSend = append(toSend, c.corked...)
		toSend = append(toSend, p...)
		c.corked = c.corked[:0]
	} else {
		toSend = p
	}

	c.inflight = true
	if c.ackTimer == nil {
		c.ackTimer = time.AfterFunc(c.delackWait, c.onDelayedACK)
	} else {
		c.ackTimer.Reset(c.delackWait)
	}
	c.mu.Unlock()

	n, err := writeFull(c.Conn, toSend)
	if err != nil {
		return 0, err
	}
	if n < len(p) {
		return n, nil
	}
	return len(p), nil
}

func (c *autocorkDelayedACKConn) Read(b []byte) (int, error) {
	n, err := c.Conn.Read(b)
	if n > 0 {
		// 服务端下行数据包到达客户端，捎带 TCP ACK 立即解除 TSQ_THROTTLED 并冲刷挂起分片
		c.mu.Lock()
		c.releaseCorkLocked()
		c.mu.Unlock()
	}
	return n, err
}

func (c *autocorkDelayedACKConn) Close() error {
	c.mu.Lock()
	c.closed = true
	if c.ackTimer != nil {
		c.ackTimer.Stop()
	}
	c.corked = nil
	c.mu.Unlock()
	return c.Conn.Close()
}

type coalesceBenchHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu              sync.Mutex
	innerDurByProc  map[string]time.Duration
	innerCallsByPrc map[string]int
	lastWriteLen    atomic.Int64
}

func newCoalesceBenchHandler() *coalesceBenchHandler {
	return &coalesceBenchHandler{
		innerDurByProc:  make(map[string]time.Duration),
		innerCallsByPrc: make(map[string]int),
	}
}

func (h *coalesceBenchHandler) recordInner(proc string, d time.Duration) {
	h.mu.Lock()
	h.innerDurByProc[proc] += d
	h.innerCallsByPrc[proc]++
	h.mu.Unlock()
}

func (h *coalesceBenchHandler) Mkdir(_ context.Context, _ *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	start := time.Now()
	defer func() { h.recordInner(storagev1connect.StorageServiceMkdirProcedure, time.Since(start)) }()
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode: &storagev1.Inode{Ino: 2},
	}), nil
}

func (h *coalesceBenchHandler) Fsync(_ context.Context, _ *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	start := time.Now()
	defer func() { h.recordInner(storagev1connect.StorageServiceFsyncProcedure, time.Since(start)) }()
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (h *coalesceBenchHandler) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	start := time.Now()
	defer func() { h.recordInner(storagev1connect.StorageServiceWriteProcedure, time.Since(start)) }()
	n := len(req.Msg.GetData())
	h.lastWriteLen.Store(int64(n))
	return connect.NewResponse(&storagev1.WriteResponse{Size: uint32(n)}), nil
}

func startTestH2CServer(t *testing.T, handler storagev1connect.StorageServiceHandler, outerDur map[string]*atomic.Int64) string {
	t.Helper()
	path, connectHandler := storagev1connect.NewStorageServiceHandler(
		handler,
		connect.WithCodec(newProtoBinaryCodec()),
	)
	mux := http.NewServeMux()
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		connectHandler.ServeHTTP(w, r)
		if counter, ok := outerDur[r.URL.Path]; ok {
			counter.Add(time.Since(start).Nanoseconds())
		}
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
	return "http://" + ln.Addr().String()
}

func TestH2CoalesceConn_EliminatesTwoWriteAutocorkDelayedACKDeadlock(t *testing.T) {
	const (
		iters      = 8
		sizeGoal   = 2896 // 2 * TCP MSS (1448B)
		delackWait = 10 * time.Millisecond
	)
	payload4K := bytes.Repeat([]byte{0xAB}, 4096)

	runSuite := func(t *testing.T, enableCoalesce bool) (writesPerSmallOp float64, outerWriteMean, outerFsyncMean, outerMkdirMean, innerFsyncMean time.Duration) {
		t.Helper()
		svc := newCoalesceBenchHandler()
		outerDur := map[string]*atomic.Int64{
			storagev1connect.StorageServiceWriteProcedure: {},
			storagev1connect.StorageServiceFsyncProcedure: {},
			storagev1connect.StorageServiceMkdirProcedure: {},
		}
		endpoint := startTestH2CServer(t, svc, outerDur)

		var rawWrites atomic.Int64
		var protocols http.Protocols
		protocols.SetUnencryptedHTTP2(true)
		tr := &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				var d net.Dialer
				c, err := d.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				sim := newAutocorkDelayedACKConn(c, sizeGoal, delackWait, &rawWrites)
				if enableCoalesce {
					return newH2CoalesceConn(sim), nil
				}
				return sim, nil
			},
			Protocols: &protocols,
			HTTP2: &http.HTTP2Config{
				MaxReadFrameSize:              HTTP2MaxReadFrameSize,
				MaxReceiveBufferPerStream:     HTTP2MaxReceiveBufferPerStream,
				MaxReceiveBufferPerConnection: HTTP2MaxReceiveBufferPerConnection,
			},
		}
		t.Cleanup(tr.CloseIdleConnections)

		cli, err := NewClientFromConfig(ClientConfig{
			Endpoint:   endpoint,
			HTTPClient: &http.Client{Transport: tr},
		})
		require.NoError(t, err)

		ctx := context.Background()
		// 预热建连并排空 HTTP/2 握手帧计数
		_, err = cli.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: "d1", Epoch: 1, Ino: 10, Offset: 0, Data: payload4K,
		}))
		require.NoError(t, err)
		for _, v := range outerDur {
			v.Store(0)
		}
		svc.mu.Lock()
		clear(svc.innerDurByProc)
		clear(svc.innerCallsByPrc)
		svc.mu.Unlock()
		beforeWrites := rawWrites.Load()

		for range iters {
			_, err = cli.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
				DiskId: "d1", Epoch: 1, Ino: 10, Offset: 0, Data: payload4K,
			}))
			require.NoError(t, err)
			_, err = cli.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
				DiskId: "d1", Epoch: 1, Ino: 10,
			}))
			require.NoError(t, err)
			_, err = cli.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				DiskId: "d1", Epoch: 1, ParentIno: 1, Name: []byte("dir"), Mode: 0o755,
			}))
			require.NoError(t, err)
		}

		totalOps := float64(iters * 3)
		writesPerSmallOp = float64(rawWrites.Load()-beforeWrites) / totalOps
		outerWriteMean = time.Duration(outerDur[storagev1connect.StorageServiceWriteProcedure].Load() / iters)
		outerFsyncMean = time.Duration(outerDur[storagev1connect.StorageServiceFsyncProcedure].Load() / iters)
		outerMkdirMean = time.Duration(outerDur[storagev1connect.StorageServiceMkdirProcedure].Load() / iters)

		svc.mu.Lock()
		innerFsyncMean = svc.innerDurByProc[storagev1connect.StorageServiceFsyncProcedure] / iters
		svc.mu.Unlock()
		return
	}

	// 1. 反事实基线（无合并）：精确复现 #41 的全部实证特征：
	//    - 每次 Unary RPC 触发 >= 2 次底层 Write (HEADERS + DATA)；
	//    - 4 KiB Write (>= size_goal) 不受影响 (< 3ms)；
	//    - Fsync / Mkdir (< size_goal) 在 Server HTTP 外层阻塞 ~10ms 等待 DATA 帧，而内层 ConnectRPC Handler < 1ms！
	uncoalWrites, uncoalWriteDur, uncoalFsyncDur, uncoalMkdirDur, uncoalInnerFsync := runSuite(t, false)
	require.GreaterOrEqual(t, uncoalWrites, 2.0, "未合并时 Go http2ClientConn 每次 Unary 请求至少触发 2 次底层 Write")
	require.Less(t, uncoalWriteDur, 3*time.Millisecond, "4 KiB Write 超出 size_goal 应直接穿透 autocork")
	require.GreaterOrEqual(t, uncoalFsyncDur, 9*time.Millisecond, "未合并时小包 Fsync 应稳定复现 ~10ms Delayed ACK 死锁")
	require.GreaterOrEqual(t, uncoalMkdirDur, 9*time.Millisecond, "未合并时小包 Mkdir 应稳定复现 ~10ms Delayed ACK 死锁")
	require.Less(t, uncoalInnerFsync, time.Millisecond, "内层 ConnectRPC Handler 耗时仍为亚毫秒级")

	// 2. 修复后（启用 h2CoalesceConn，并与 newHTTPTransport 生产路径一致）：
	//    - HEADERS + DATA 合并为单次底层 Write (~1.0 次/op)；
	//    - Fsync / Mkdir / Write 全部亚毫秒完成，彻底消除 ~10ms 死锁！
	coalWrites, coalWriteDur, coalFsyncDur, coalMkdirDur, coalInnerFsync := runSuite(t, true)
	require.LessOrEqual(t, coalWrites, 1.25, "合并后每次小负载 Unary 请求应收敛为单次底层 Write (含偶发 WINDOW_UPDATE)")
	require.Less(t, coalWriteDur, 3*time.Millisecond)
	require.Less(t, coalFsyncDur, 3*time.Millisecond, "合并 HEADERS+DATA 后 Fsync 应彻底消除 10ms 死锁")
	require.Less(t, coalMkdirDur, 3*time.Millisecond, "合并 HEADERS+DATA 后 Mkdir 应彻底消除 10ms 死锁")
	require.Less(t, coalInnerFsync, time.Millisecond)
}

func TestH2CoalesceConn_LargeConcurrentWritesAndFlowControlFallback(t *testing.T) {
	svc := newCoalesceBenchHandler()
	endpoint := startTestH2CServer(t, svc, nil)

	// 直接走生产路径 NewClient (内部启用 newHTTPTransport + newH2CoalesceConn)
	cli, err := NewClient(endpoint, "")
	require.NoError(t, err)

	// 16 并发 * 4 MiB = 64 MiB > 32 MiB 连接接收流控窗口，必现触发流控等待与 150µs 兜底冲刷分支，验证零死锁与数据完整性
	chunk4M := bytes.Repeat([]byte{0x5C}, 4<<20)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	errCh := make(chan error, 16)
	var wg sync.WaitGroup
	for i := range 16 {
		ino := uint64(100 + i)
		wg.Go(func() {
			resp, wErr := cli.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
				DiskId: "d1",
				Epoch:  1,
				Ino:    ino,
				Offset: 0,
				Data:   chunk4M,
			}))
			if wErr != nil {
				errCh <- wErr
				return
			}
			if resp.Msg.GetSize() != uint32(len(chunk4M)) {
				errCh <- connect.NewError(connect.CodeInternal, nil)
				return
			}
			if _, mErr := cli.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				DiskId: "d1", Epoch: 1, ParentIno: 1, Name: []byte("sub"), Mode: 0o755,
			})); mErr != nil {
				errCh <- mErr
			}
		})
	}
	wg.Wait()
	close(errCh)
	for e := range errCh {
		require.NoError(t, e)
	}
}
