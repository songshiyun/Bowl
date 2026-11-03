package session_test

import (
	"context"
	"crypto/tls"
	"errors"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/session"
)

type mockStorageHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu                sync.Mutex
	renewSessionCalls []uint64
	renewSessionReqs  []*storagev1.RenewSessionRequest
	closeSessionCalls []uint64
	renewSessionFunc  func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
	closeSessionFunc  func(ctx context.Context, req *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error)
}

func (m *mockStorageHandler) RenewSession(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	m.mu.Lock()
	m.renewSessionCalls = append(m.renewSessionCalls, req.Msg.GetSessionId())
	m.renewSessionReqs = append(m.renewSessionReqs, req.Msg)
	fn := m.renewSessionFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}

	return connect.NewResponse(&storagev1.RenewSessionResponse{
		ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Second)),
		SessionRecreated: false,
	}), nil
}

func (m *mockStorageHandler) CloseSession(ctx context.Context, req *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error) {
	m.mu.Lock()
	m.closeSessionCalls = append(m.closeSessionCalls, req.Msg.GetSessionId())
	fn := m.closeSessionFunc
	m.mu.Unlock()

	if fn != nil {
		return fn(ctx, req)
	}

	return connect.NewResponse(&storagev1.CloseSessionResponse{}), nil
}

func (m *mockStorageHandler) getCalls() []uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	res := make([]uint64, len(m.renewSessionCalls))
	copy(res, m.renewSessionCalls)
	return res
}

func setupTestServer(t *testing.T, handler storagev1connect.StorageServiceHandler) (storagev1connect.StorageServiceClient, func()) {
	t.Helper()
	mux := http.NewServeMux()
	path, srvHandler := storagev1connect.NewStorageServiceHandler(handler)
	mux.Handle(path, srvHandler)

	server := httptest.NewServer(h2c.NewHandler(mux, &http2.Server{}))
	h2cClient := &http.Client{
		Transport: &http2.Transport{
			AllowHTTP: true,
			DialTLSContext: func(ctx context.Context, network, addr string, _ *tls.Config) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, addr)
			},
		},
	}
	client := storagev1connect.NewStorageServiceClient(h2cClient, server.URL)

	return client, func() {
		server.Close()
	}
}

func TestManager_InitialActivation(t *testing.T) {
	t.Run("fails when initial RenewSession RPC returns error", func(t *testing.T) {
		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("storage service unavailable"))
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		mgr, err := session.New(context.Background(), client, session.Config{})
		require.Error(t, err)
		require.Nil(t, mgr)
		require.Contains(t, err.Error(), "activate initial session")
	})

	t.Run("fails when client is nil", func(t *testing.T) {
		mgr, err := session.New(context.Background(), nil, session.Config{})
		require.Error(t, err)
		require.Nil(t, mgr)
		require.Contains(t, err.Error(), "storage client is nil")
	})

	t.Run("succeeds with 63-bit session ID and registers session on server", func(t *testing.T) {
		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				sid := req.Msg.GetSessionId()
				require.GreaterOrEqual(t, sid, uint64(1))
				require.LessOrEqual(t, sid, uint64(math.MaxInt64))
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(30 * time.Second)),
					SessionRecreated: true, // 初始首次创建，服务端返回 true 属正常现象
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		mgr, err := session.New(context.Background(), client, session.Config{})
		require.NoError(t, err)
		require.NotNil(t, mgr)
		defer func() { _ = mgr.Close() }()

		curID := mgr.CurrentSessionID()
		require.GreaterOrEqual(t, curID, uint64(1))
		require.LessOrEqual(t, curID, uint64(math.MaxInt64))

		calls := handler.getCalls()
		require.Len(t, calls, 1)
		require.Equal(t, curID, calls[0])
	})
}

func TestManager_HandleSessionBinding(t *testing.T) {
	handler := &mockStorageHandler{}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	curSID := mgr.CurrentSessionID()

	t.Run("lookup and resolve unbound handle returns EBADF", func(t *testing.T) {
		sid, ok := mgr.LookupHandleSession(9999)
		require.False(t, ok)
		require.Equal(t, uint64(0), sid)

		resSID, err := mgr.ResolveHandleSession(9999)
		require.ErrorIs(t, err, session.ErrBadFileHandle)
		require.Equal(t, uint64(0), resSID)
	})

	t.Run("bind, lookup, resolve, and unbind lifecycle", func(t *testing.T) {
		fh := uint64(1001)
		mgr.BindHandle(fh, curSID)

		sid, ok := mgr.LookupHandleSession(fh)
		require.True(t, ok)
		require.Equal(t, curSID, sid)

		resSID, err := mgr.ResolveHandleSession(fh)
		require.NoError(t, err)
		require.Equal(t, curSID, resSID)

		// 释放句柄
		mgr.UnbindHandle(fh)

		sidAfter, okAfter := mgr.LookupHandleSession(fh)
		require.False(t, okAfter)
		require.Equal(t, uint64(0), sidAfter)

		resAfter, errAfter := mgr.ResolveHandleSession(fh)
		require.ErrorIs(t, errAfter, session.ErrBadFileHandle)
		require.Equal(t, uint64(0), resAfter)
	})

	t.Run("concurrent handle operations are thread-safe", func(t *testing.T) {
		const goroutines = 20
		const opsPerGoroutine = 50

		var wg sync.WaitGroup
		wg.Add(goroutines)

		for i := 0; i < goroutines; i++ {
			go func(baseFH uint64) {
				defer wg.Done()
				for j := uint64(0); j < opsPerGoroutine; j++ {
					fh := baseFH*1000 + j
					mgr.BindHandle(fh, curSID)
					sid, ok := mgr.LookupHandleSession(fh)
					if ok && sid != curSID {
						t.Errorf("expected sid %d, got %d", curSID, sid)
					}
					_, _ = mgr.ResolveHandleSession(fh)
					mgr.UnbindHandle(fh)
				}
			}(uint64(i + 1))
		}

		wg.Wait()
	})
}

func TestManager_AdaptiveRenewLoop(t *testing.T) {
	t.Run("periodically renews session at 1/3 lease interval and terminates cleanly on Close", func(t *testing.T) {
		renewCount := 0
		var mu sync.Mutex

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				renewCount++
				mu.Unlock()
				// 每次返回 120ms 后过期，1/3 即 40ms 续期一次
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(120 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		cfg := session.Config{
			MinInterval:         20 * time.Millisecond,
			MaxInterval:         100 * time.Millisecond,
			InitialRetryBackoff: 20 * time.Millisecond,
			MaxRetryBackoff:     50 * time.Millisecond,
		}

		mgr, err := session.New(context.Background(), client, cfg)
		require.NoError(t, err)
		defer func() { _ = mgr.Close() }()

		err = mgr.Start(context.Background())
		require.NoError(t, err)

		// 重复 Start 返回错误
		err = mgr.Start(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "already started")

		// 等待 150ms，应该触发至少 2 次以上续期（初始 1 次 + 后台续期多次）
		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return renewCount >= 3
		}, 5*time.Second, 10*time.Millisecond)

		// 验证 Close 优雅退出
		err = mgr.Close()
		require.NoError(t, err)

		mu.Lock()
		countAtClose := renewCount
		mu.Unlock()

		// 等待 50ms 确认后台协程已停止，续期次数不再增加
		time.Sleep(50 * time.Millisecond)
		mu.Lock()
		countAfterWait := renewCount
		mu.Unlock()
		require.Equal(t, countAtClose, countAfterWait, "renewLoop must stop after Close()")

		// 幂等 Close
		require.NoError(t, mgr.Close())
	})

	t.Run("retries with backoff on transient network error without crashing", func(t *testing.T) {
		var mu sync.Mutex
		attempts := 0

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				attempts++
				curAttempt := attempts
				mu.Unlock()

				// 第 1 次是初始激活（成功）
				if curAttempt == 1 {
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(90 * time.Millisecond)),
						SessionRecreated: false,
					}), nil
				}
				// 第 2 次模拟网络瞬断返回 Unavailable
				if curAttempt == 2 {
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("network glitch"))
				}
				// 第 3 次重试成功
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(300 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		cfg := session.Config{
			MinInterval:         20 * time.Millisecond,
			MaxInterval:         100 * time.Millisecond,
			InitialRetryBackoff: 20 * time.Millisecond,
			MaxRetryBackoff:     50 * time.Millisecond,
		}

		mgr, err := session.New(context.Background(), client, cfg)
		require.NoError(t, err)

		err = mgr.Start(context.Background())
		require.NoError(t, err)

		// 重复 Start 返回错误
		err = mgr.Start(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "already started")
		defer func() { _ = mgr.Close() }()

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()
			return attempts >= 3
		}, 5*time.Second, 10*time.Millisecond)
	})
}

// logCaptureHandler 记录所有 slog 输出以便单测断言日志告警
type logCaptureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func (h *logCaptureHandler) Enabled(_ context.Context, _ slog.Level) bool {
	return true
}

func (h *logCaptureHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *logCaptureHandler) WithAttrs(_ []slog.Attr) slog.Handler {
	return h
}

func (h *logCaptureHandler) WithGroup(_ string) slog.Handler {
	return h
}

func (h *logCaptureHandler) hasWarnLog(msgSubstr string) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, r := range h.records {
		if r.Level >= slog.LevelWarn && strings.Contains(r.Message, msgSubstr) {
			return true
		}
	}
	return false
}

func TestManager_SessionRecreated_RotationAndHandleIsolation(t *testing.T) {
	var mu sync.Mutex
	calls := make([]uint64, 0)
	recreatedReturned := false

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			defer mu.Unlock()
			sid := req.Msg.GetSessionId()
			calls = append(calls, sid)

			// 初始创建
			if len(calls) == 1 {
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}

			// 第 2 次续期模拟服务端会话超期被 sweep 清理后重新 upsert（SessionRecreated == true）
			if !recreatedReturned {
				recreatedReturned = true
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: true,
				}), nil
			}

			// 轮换新 ID 后的正常续期
			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(200 * time.Millisecond)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	logCapture := &logCaptureHandler{}
	logger := slog.New(logCapture)

	cfg := session.Config{
		MinInterval:         20 * time.Millisecond,
		MaxInterval:         100 * time.Millisecond,
		InitialRetryBackoff: 20 * time.Millisecond,
		MaxRetryBackoff:     50 * time.Millisecond,
		Logger:              logger,
	}

	mgr, err := session.New(context.Background(), client, cfg)
	require.NoError(t, err)

	oldSID := mgr.CurrentSessionID()

	// 断网前打开一个旧句柄
	oldFH := uint64(501)
	mgr.BindHandle(oldFH, oldSID)

	// 此时旧句柄有效
	resSID, err := mgr.ResolveHandleSession(oldFH)
	require.NoError(t, err)
	require.Equal(t, oldSID, resSID)

	// 启动后台续期
	err = mgr.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	// 等待发生会话轮换（活跃 sessionID 发生变化且非 0、不等于 oldSID）
	require.Eventually(t, func() bool {
		cur := mgr.CurrentSessionID()
		return cur != 0 && cur != oldSID
	}, 5*time.Second, 10*time.Millisecond)

	newSID := mgr.CurrentSessionID()
	require.GreaterOrEqual(t, newSID, uint64(1))
	require.LessOrEqual(t, newSID, uint64(math.MaxInt64))
	require.NotEqual(t, oldSID, newSID)

	// 1. 验证输出明确的 WARN 告警日志
	require.True(t, logCapture.hasWarnLog("client session expired on server; rotating session_id"), "必须输出会话超期轮换警告日志")

	// 2. 验证断网前打开的老句柄已被清除，确定性返回 EBADF，绝不借用新会话续命
	oldResSID, err := mgr.ResolveHandleSession(oldFH)
	require.ErrorIs(t, err, session.ErrBadFileHandle, "老句柄解析必须返回 EBADF")
	require.Equal(t, uint64(0), oldResSID, "老句柄在旧会话退役时已从 handleSessions 清理，应返回 0")

	// 验证 LookupHandleSession 已被立即清理
	lookupSID, ok := mgr.LookupHandleSession(oldFH)
	require.False(t, ok, "旧会话退役后 LookupHandleSession 应返回 false")
	require.Equal(t, uint64(0), lookupSID)

	// 3. 验证新句柄在轮换后绑定新 session_id 并自愈可用
	newFH := uint64(502)
	mgr.BindHandle(newFH, newSID)
	activeResSID, err := mgr.ResolveHandleSession(newFH)
	require.NoError(t, err)
	require.Equal(t, newSID, activeResSID)

	// 4. 验证 UnbindHandle 幂等安全
	mgr.UnbindHandle(oldFH)
	_, okAfterUnbind := mgr.LookupHandleSession(oldFH)
	require.False(t, okAfterUnbind)
}

func TestManager_SessionRecreated_RetryOnRotationFailure(t *testing.T) {
	TestManager_SessionExpired_RotationRetryOnError(t)
}

func TestManager_SessionExpired_RotationRetryOnError(t *testing.T) {
	var mu sync.Mutex
	calls := make([]uint64, 0)
	rotationAttempts := 0
	firstRotationEntered := make(chan struct{})
	releaseFirstRotation := make(chan struct{})
	var firstRotationOnce sync.Once

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			sid := req.Msg.GetSessionId()
			calls = append(calls, sid)
			callNum := len(calls)

			// 初始激活
			if callNum == 1 {
				mu.Unlock()
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}

			// 第 2 次续期返回 session_recreated = true 触发轮换
			if callNum == 2 {
				mu.Unlock()
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(100 * time.Millisecond)),
					SessionRecreated: true,
				}), nil
			}

			// 轮换激活新 ID 阶段：第一次阻塞在屏障以便观测旧句柄退役窗口，放行后失败（模拟瞬态网络故障），第二次成功
			rotationAttempts++
			curAttempt := rotationAttempts
			mu.Unlock()

			if curAttempt == 1 {
				firstRotationOnce.Do(func() { close(firstRotationEntered) })
				select {
				case <-releaseFirstRotation:
				case <-ctx.Done():
					return nil, ctx.Err()
				}
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("network glitch during rotation"))
			}

			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(200 * time.Millisecond)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	cfg := session.Config{
		MinInterval:         20 * time.Millisecond,
		MaxInterval:         100 * time.Millisecond,
		InitialRetryBackoff: 20 * time.Millisecond,
		MaxRetryBackoff:     50 * time.Millisecond,
	}

	mgr, err := session.New(context.Background(), client, cfg)
	require.NoError(t, err)

	initialSID := mgr.CurrentSessionID()
	const oldFH = uint64(777)
	mgr.BindHandle(oldFH, initialSID)

	resolvedSID, err := mgr.ResolveHandleSession(oldFH)
	require.NoError(t, err)
	require.Equal(t, initialSID, resolvedSID)

	err = mgr.Start(context.Background())
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	// 等待进入首次轮换 RenewSession(newSID) 尝试并阻塞在屏障上
	select {
	case <-firstRotationEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for first rotation RenewSession attempt")
	}

	// 此时旧会话已被立即置为未激活态（CurrentSessionID() == 0），且旧句柄必须已从 handleSessions 清理并立即返回 EBADF
	require.Equal(t, uint64(0), mgr.CurrentSessionID(), "旧会话过期清理后、新会话激活完成前 CurrentSessionID 必须为 0")
	midSID, err := mgr.ResolveHandleSession(oldFH)
	require.ErrorIs(t, err, session.ErrBadFileHandle, "轮换激活受阻窗口内旧句柄必须立即返回 EBADF")
	require.Equal(t, uint64(0), midSID)
	_, okMid := mgr.LookupHandleSession(oldFH)
	require.False(t, okMid, "轮换激活受阻窗口内旧句柄必须已从 handleSessions 清理")

	// 放行首次轮换尝试（使其返回 Unavailable 触发重试）
	close(releaseFirstRotation)

	// 等待最终成功轮换为非 0 且不等于 initialSID 的新会话
	require.Eventually(t, func() bool {
		cur := mgr.CurrentSessionID()
		return cur != 0 && cur != initialSID
	}, 5*time.Second, 10*time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, rotationAttempts, 2, "轮换激活应发生至少一次重试后成功")
}

func TestManager_DiskIDPropagation_And_CloseSession(t *testing.T) {
	const testDiskID = "11111111-1111-7111-8111-111111111111"

	t.Run("New, renewLoop, and handleSessionExpired all propagate DiskId and Close calls CloseSession", func(t *testing.T) {
		var mu sync.Mutex
		callIdx := 0

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				defer mu.Unlock()
				callIdx++
				// 第 2 次调用返回 SessionRecreated = true 触发轮换
				if callIdx == 2 {
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Millisecond)),
						SessionRecreated: true,
					}), nil
				}
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		mgr, err := session.New(context.Background(), client, session.Config{
			DiskID:              testDiskID,
			BranchName:          "feature-x",
			ReadOnly:            true,
			CommitID:            "22222222-2222-4222-8222-222222222222",
			MinInterval:         15 * time.Millisecond,
			MaxInterval:         50 * time.Millisecond,
			InitialRetryBackoff: 15 * time.Millisecond,
			MaxRetryBackoff:     50 * time.Millisecond,
		})
		require.NoError(t, err)

		initialSID := mgr.CurrentSessionID()
		require.NoError(t, mgr.Start(context.Background()))

		// 等待发生轮换并至少完成 3 次 RenewSession（New -> renewLoop 触发 recreated -> handleSessionExpired 激活新会话）
		require.Eventually(t, func() bool {
			cur := mgr.CurrentSessionID()
			return cur != 0 && cur != initialSID
		}, 3*time.Second, 10*time.Millisecond)

		rotatedSID := mgr.CurrentSessionID()

		// 调用 Close() 必须停止 renewLoop 并对最新活跃会话发送 CloseSession
		require.NoError(t, mgr.Close())
		// 重复调用 Close() 幂等，不会重复发送 CloseSession
		require.NoError(t, mgr.Close())

		handler.mu.Lock()
		reqs := append([]*storagev1.RenewSessionRequest(nil), handler.renewSessionReqs...)
		closeCalls := append([]uint64(nil), handler.closeSessionCalls...)
		handler.mu.Unlock()

		require.GreaterOrEqual(t, len(reqs), 3)
		for i, r := range reqs {
			require.NotNil(t, r.DiskId, "第 %d 次 RenewSession 请求必须携带 DiskId", i+1)
			require.Equal(t, testDiskID, r.GetDiskId(), "第 %d 次 RenewSession 请求的 DiskId 必须等于配置的 DiskID", i+1)
			require.Equal(t, "feature-x", r.GetBranchName())
			require.True(t, r.GetReadOnly())
			require.Equal(t, "22222222-2222-4222-8222-222222222222", r.GetCommitId())
		}

		require.Equal(t, []uint64{rotatedSID}, closeCalls, "Close() 必须恰好调用一次 CloseSession 且携带当前最新活跃的 SessionId")
	})

	t.Run("Close logs warn and returns nil when CloseSession RPC fails", func(t *testing.T) {
		handler := &mockStorageHandler{
			closeSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error) {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("server down on unmount"))
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		logCapture := &logCaptureHandler{}
		mgr, err := session.New(context.Background(), client, session.Config{
			Logger: slog.New(logCapture),
		})
		require.NoError(t, err)

		require.NoError(t, mgr.Close())
		require.True(t, logCapture.hasWarnLog("close session failed on shutdown"), "CloseSession 失败时应记录 Warn 日志而不阻塞卸载")
	})
}

func TestManager_PerCallRenewRPCTimeout(t *testing.T) {
	var mu sync.Mutex
	attempts := 0

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			attempts++
			cur := attempts
			mu.Unlock()

			// 第 1 次初始激活成功
			if cur == 1 {
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(300 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}
			// 第 2 次模拟黑洞/假死连接，阻塞直到单次 RPC 超时取消
			if cur == 2 {
				<-ctx.Done()
				return nil, ctx.Err()
			}
			// 第 3 次恢复正常
			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(300 * time.Millisecond)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	mgr, err := session.New(context.Background(), client, session.Config{
		MinInterval:         20 * time.Millisecond,
		MaxInterval:         40 * time.Millisecond,
		InitialRetryBackoff: 15 * time.Millisecond,
		MaxRetryBackoff:     30 * time.Millisecond,
		RenewRPCTimeout:     40 * time.Millisecond,
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	require.NoError(t, mgr.Start(context.Background()))

	// 验证第 2 次挂起的 RenewSession 被 RenewRPCTimeout 取消后不会卡死 renewLoop，能够推进到第 3 次成功续约
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts >= 3
	}, 2*time.Second, 10*time.Millisecond, "挂起的 RenewSession 必须被 RenewRPCTimeout 超时打断并继续后续重试")
}

func TestManager_GracePeriodExpiryImmediateCleanup_OnRenewFailure(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	expiredCallbacks := make([]uint64, 0)
	registeredCallbacks := make([]uint64, 0)
	serverPartitioned := true

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			attempts++
			cur := attempts
			partitioned := serverPartitioned
			mu.Unlock()

			if cur == 1 {
				// 初始租约 60ms
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}
			if partitioned {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("network partition"))
			}
			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(300 * time.Millisecond)),
				SessionRecreated: true,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	mgr, err := session.New(context.Background(), client, session.Config{
		MinInterval:         15 * time.Millisecond,
		MaxInterval:         25 * time.Millisecond,
		InitialRetryBackoff: 15 * time.Millisecond,
		MaxRetryBackoff:     25 * time.Millisecond,
		RenewRPCTimeout:     50 * time.Millisecond,
		OnSessionExpired: func(expiredSID uint64) {
			mu.Lock()
			expiredCallbacks = append(expiredCallbacks, expiredSID)
			mu.Unlock()
		},
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	mgr.RegisterOnSessionExpired(func(expiredSID uint64) {
		mu.Lock()
		registeredCallbacks = append(registeredCallbacks, expiredSID)
		mu.Unlock()
	})

	oldSID := mgr.CurrentSessionID()
	const oldFH = uint64(801)
	mgr.BindHandle(oldFH, oldSID)

	require.NoError(t, mgr.Start(context.Background()))

	// 在网络仍然断开（serverPartitioned == true）的情况下，等待 60ms 租约宽限期耗尽：
	// 客户端必须立即标记旧会话退役、清空 handleSessions 并触发 OnSessionExpired（无需等待网络恢复）
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(expiredCallbacks) > 0 && len(registeredCallbacks) > 0
	}, 2*time.Second, 10*time.Millisecond, "宽限期耗尽时无需等待网络恢复即应立即触发 OnSessionExpired")

	mu.Lock()
	require.Equal(t, []uint64{oldSID}, expiredCallbacks, "Config.OnSessionExpired 对同一旧会话必须恰好触发一次")
	require.Equal(t, []uint64{oldSID}, registeredCallbacks, "RegisterOnSessionExpired 对同一旧会话必须恰好触发一次")
	mu.Unlock()

	require.Equal(t, uint64(0), mgr.CurrentSessionID(), "宽限期耗尽后当前会话必须立即置为 0")
	_, ok := mgr.LookupHandleSession(oldFH)
	require.False(t, ok, "宽限期耗尽后本地 handleSessions 必须已被立即清空")
	_, err = mgr.ResolveHandleSession(oldFH)
	require.ErrorIs(t, err, session.ErrBadFileHandle)

	// 恢复网络连通后，自动完成新会话激活且不再重复触发旧会话的过期回调
	mu.Lock()
	serverPartitioned = false
	mu.Unlock()

	require.Eventually(t, func() bool {
		cur := mgr.CurrentSessionID()
		return cur != 0 && cur != oldSID
	}, 2*time.Second, 10*time.Millisecond)

	mu.Lock()
	require.Equal(t, []uint64{oldSID}, expiredCallbacks, "网络恢复完成轮换后不得重复触发同一 oldSID 的回调")
	require.Equal(t, []uint64{oldSID}, registeredCallbacks)
	mu.Unlock()
}

func TestManager_ResolveHandleSession_LocalExpiryBeforeRenewLoopTick(t *testing.T) {
	var mu sync.Mutex
	expiredCallbacks := make([]uint64, 0)

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			// 返回 40ms 后过期的短租约
			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(40 * time.Millisecond)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	// 不启动 renewLoop（或模拟 renewLoop 尚未 tick），直接验证 ResolveHandleSession 的本地超期即时拦截
	mgr, err := session.New(context.Background(), client, session.Config{
		MinInterval: 10 * time.Second,
		MaxInterval: 20 * time.Second,
		OnSessionExpired: func(expiredSID uint64) {
			mu.Lock()
			expiredCallbacks = append(expiredCallbacks, expiredSID)
			mu.Unlock()
		},
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	oldSID := mgr.CurrentSessionID()
	const fh = uint64(901)
	mgr.BindHandle(fh, oldSID)

	// 等待超过 40ms 使本地 expiresAt 到期
	time.Sleep(60 * time.Millisecond)

	// 调用 ResolveHandleSession 必须立即检测到本地租约超期，返回 ErrBadFileHandle，清空 handleSessions 并触发回调
	gotSID, err := mgr.ResolveHandleSession(fh)
	require.ErrorIs(t, err, session.ErrBadFileHandle)
	require.Equal(t, oldSID, gotSID)

	_, ok := mgr.LookupHandleSession(fh)
	require.False(t, ok, "ResolveHandleSession 检测到本地超期后必须立即清空该会话的 handleSessions")

	mu.Lock()
	require.Equal(t, []uint64{oldSID}, expiredCallbacks, "ResolveHandleSession 检测到本地超期后必须立即触发一次 OnSessionExpired")
	mu.Unlock()

	// 再次调用不会重复触发回调
	_, err = mgr.ResolveHandleSession(fh)
	require.ErrorIs(t, err, session.ErrBadFileHandle)
	mu.Lock()
	require.Equal(t, []uint64{oldSID}, expiredCallbacks)
	mu.Unlock()
}

func TestManager_TriggerRenew_ImmediateWakeup(t *testing.T) {
	var mu sync.Mutex
	calls := 0

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			calls++
			mu.Unlock()
			// 返回 60s 租约，使得默认自适应睡眠间隔达到 MaxInterval (10s)
			return connect.NewResponse(&storagev1.RenewSessionResponse{
				ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Second)),
				SessionRecreated: false,
			}), nil
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	mgr, err := session.New(context.Background(), client, session.Config{
		MinInterval: 5 * time.Second,
		MaxInterval: 10 * time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	require.NoError(t, mgr.Start(context.Background()))

	// 初始仅有 1 次调用（New 阶段）
	time.Sleep(30 * time.Millisecond)
	mu.Lock()
	require.Equal(t, 1, calls)
	mu.Unlock()

	// 调用 TriggerRenew() 应立即唤醒处于 10s 长睡眠中的 renewLoop 发起 RenewSession
	mgr.TriggerRenew()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls >= 2
	}, 500*time.Millisecond, 10*time.Millisecond, "TriggerRenew 必须立即唤醒 renewLoop 发起续约 RPC")
}

func TestManager_PerCallTimeoutClamped_And_BackoffClampedToRemainingGracePeriod(t *testing.T) {
	require.Equal(t, 3*time.Second, session.DefaultRenewRPCTimeout)

	t.Run("per-call timeout is clamped to min(RenewRPCTimeout, time.Until(expiresAt)) before expiry and uses RenewRPCTimeout after expiry", func(t *testing.T) {
		var mu sync.Mutex
		var observedRemainingDeadlines []time.Duration
		attempts := 0

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				dl, hasDL := ctx.Deadline()
				remDL := time.Duration(0)
				if hasDL {
					remDL = time.Until(dl)
				}

				mu.Lock()
				attempts++
				cur := attempts
				observedRemainingDeadlines = append(observedRemainingDeadlines, remDL)
				mu.Unlock()

				if cur == 1 {
					// 初始租约 90ms（显著小于默认 3s RenewRPCTimeout）
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(90 * time.Millisecond)),
						SessionRecreated: false,
					}), nil
				}
				if cur == 2 {
					// 续期时剩余租约 < 90ms，超时必须被钳位在 time.Until(expiresAt) (<= 90ms)，远小于 3s
					// 挂起等待该钳位超时触发，从而直接跨越 expiresAt 进入新会话重建
					<-ctx.Done()
					return nil, ctx.Err()
				}
				// cur == 3 为过期后重建新会话，超时应恢复为完整 RenewRPCTimeout (3s)
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(500 * time.Millisecond)),
					SessionRecreated: true,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		mgr, err := session.New(context.Background(), client, session.Config{
			MinInterval:         20 * time.Millisecond,
			MaxInterval:         30 * time.Millisecond,
			InitialRetryBackoff: 15 * time.Millisecond,
			MaxRetryBackoff:     30 * time.Millisecond,
			// 不显式设置 RenewRPCTimeout，使用默认 3s
		})
		require.NoError(t, err)
		defer func() { _ = mgr.Close() }()

		initialSID := mgr.CurrentSessionID()
		require.NoError(t, mgr.Start(context.Background()))

		require.Eventually(t, func() bool {
			cur := mgr.CurrentSessionID()
			return cur != 0 && cur != initialSID
		}, 2*time.Second, 10*time.Millisecond)

		mu.Lock()
		dls := append([]time.Duration(nil), observedRemainingDeadlines...)
		reqs := append([]uint64(nil), handler.renewSessionCalls...)
		mu.Unlock()

		require.GreaterOrEqual(t, len(dls), 3)
		// 第 1 次初始注册使用完整 3s 超时
		require.Greater(t, dls[0], 1500*time.Millisecond, "初始注册应使用 3s RenewRPCTimeout")
		// 第 2 次到期前续期必须钳位在剩余租约时间 (<= 100ms)
		require.LessOrEqual(t, dls[1], 100*time.Millisecond, "到期前续期单次 RPC 超时必须钳位至 time.Until(expiresAt)")
		// 第 3 次为跨越 expiresAt 后的新会话激活，不得再对已过期的 initialSID 发送续期，且超时恢复为 3s
		require.NotEqual(t, initialSID, reqs[2], "跨越 expiresAt 后绝不能再对已过期的 oldSID 发送 RenewSession")
		require.Greater(t, dls[2], 1500*time.Millisecond, "新会话注册应使用完整 3s RenewRPCTimeout")
	})

	t.Run("backoff sleep is clamped to remaining grace period and expires immediately without extra oldSID RPC", func(t *testing.T) {
		var mu sync.Mutex
		var callSIDs []uint64
		expiredAt := time.Time{}

		handler := &mockStorageHandler{
			renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
				mu.Lock()
				sid := req.Msg.GetSessionId()
				callSIDs = append(callSIDs, sid)
				callNum := len(callSIDs)
				mu.Unlock()

				if callNum == 1 {
					// 初始租约 80ms
					return connect.NewResponse(&storagev1.RenewSessionResponse{
						ExpiresAt:        timestamppb.New(time.Now().Add(80 * time.Millisecond)),
						SessionRecreated: false,
					}), nil
				}
				if callNum == 2 {
					// 约 26ms 时首次续期失败；虽然 InitialRetryBackoff 设为 2s，但必须被钳位到剩余 ~54ms 租约内唤醒并立即触发过期清理
					return nil, connect.NewError(connect.CodeUnavailable, errors.New("temporary failure"))
				}
				// callNum == 3 必须是新会话 ID 的激活请求
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(500 * time.Millisecond)),
					SessionRecreated: true,
				}), nil
			},
		}
		client, cleanup := setupTestServer(t, handler)
		defer cleanup()

		start := time.Now()
		mgr, err := session.New(context.Background(), client, session.Config{
			MinInterval:         20 * time.Millisecond,
			MaxInterval:         30 * time.Millisecond,
			InitialRetryBackoff: 2 * time.Second, // 故意设置远大于剩余租约的退避时间
			MaxRetryBackoff:     2 * time.Second,
			OnSessionExpired: func(expiredSID uint64) {
				mu.Lock()
				expiredAt = time.Now()
				mu.Unlock()
			},
		})
		require.NoError(t, err)
		defer func() { _ = mgr.Close() }()

		oldSID := mgr.CurrentSessionID()
		require.NoError(t, mgr.Start(context.Background()))

		require.Eventually(t, func() bool {
			cur := mgr.CurrentSessionID()
			return cur != 0 && cur != oldSID
		}, 500*time.Millisecond, 5*time.Millisecond, "退避睡眠必须被钳位在剩余宽限期内，不得睡眠满 2s")

		mu.Lock()
		expElapsed := expiredAt.Sub(start)
		gotSIDs := append([]uint64(nil), callSIDs...)
		mu.Unlock()

		require.Less(t, expElapsed, 300*time.Millisecond, "必须在 80ms 宽限期到期当下立即触发过期清理，绝不能被 2s 退避阻塞")
		require.GreaterOrEqual(t, len(gotSIDs), 3)
		require.Equal(t, oldSID, gotSIDs[0])
		require.Equal(t, oldSID, gotSIDs[1])
		require.NotEqual(t, oldSID, gotSIDs[2], "退避定时器在 expiresAt 到期唤醒后，绝不能再对已过期的 oldSID 发送 RenewSession")
	})
}

func TestManager_AcquireSessionID_ConcurrentWakeupAndSingleFlight(t *testing.T) {
	var mu sync.Mutex
	var callSIDs []uint64
	allowNewSession := make(chan struct{})

	handler := &mockStorageHandler{
		renewSessionFunc: func(ctx context.Context, req *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
			mu.Lock()
			sid := req.Msg.GetSessionId()
			callSIDs = append(callSIDs, sid)
			callNum := len(callSIDs)
			mu.Unlock()

			if callNum == 1 {
				// 初始短租约 40ms
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(40 * time.Millisecond)),
					SessionRecreated: false,
				}), nil
			}
			// 直到 allowNewSession 关闭前模拟断网，放行后激活新会话
			select {
			case <-allowNewSession:
				return connect.NewResponse(&storagev1.RenewSessionResponse{
					ExpiresAt:        timestamppb.New(time.Now().Add(60 * time.Second)),
					SessionRecreated: true,
				}), nil
			default:
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("network down"))
			}
		},
	}
	client, cleanup := setupTestServer(t, handler)
	defer cleanup()

	mgr, err := session.New(context.Background(), client, session.Config{
		MinInterval:         15 * time.Millisecond,
		MaxInterval:         25 * time.Millisecond,
		InitialRetryBackoff: 5 * time.Second, // 长退避，验证 AcquireSessionID 会通过 TriggerRenew 立即唤醒跳过退避睡眠
		MaxRetryBackoff:     5 * time.Second,
	})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()

	oldSID := mgr.CurrentSessionID()
	require.NoError(t, mgr.Start(context.Background()))

	// 等待初始会话过期并进入 handleSessionExpired 的 5s 退避等待（此时 CurrentSessionID == 0）
	require.Eventually(t, func() bool {
		mu.Lock()
		n := len(callSIDs)
		mu.Unlock()
		return mgr.CurrentSessionID() == 0 && n >= 2
	}, 1*time.Second, 5*time.Millisecond)

	// 1. 当网络仍未恢复时，AcquireSessionID 受 caller ctx 超时控制返回 DeadlineExceeded
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 35*time.Millisecond)
	defer shortCancel()
	_, err = mgr.AcquireSessionID(shortCtx)
	require.ErrorIs(t, err, context.DeadlineExceeded)

	// 2. 恢复网络，并发启动 10 个协程调用 AcquireSessionID：
	// 必须立即唤醒处于 5s 退避睡眠中的 renewLoop，且只由 renewLoop 激活同一个新会话并广播给所有等待者
	close(allowNewSession)

	const waiters = 10
	var wg sync.WaitGroup
	wg.Add(waiters)
	results := make([]uint64, waiters)
	errs := make([]error, waiters)

	startWait := time.Now()
	for i := 0; i < waiters; i++ {
		go func(idx int) {
			defer wg.Done()
			cCtx, cCancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cCancel()
			results[idx], errs[idx] = mgr.AcquireSessionID(cCtx)
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(startWait)

	require.Less(t, elapsed, 300*time.Millisecond, "AcquireSessionID 必须立即唤醒 renewLoop 跳过 5s 退避睡眠")
	newSID := mgr.CurrentSessionID()
	require.NotZero(t, newSID)
	require.NotEqual(t, oldSID, newSID)

	for i := 0; i < waiters; i++ {
		require.NoError(t, errs[i])
		require.Equal(t, newSID, results[i], "所有并发等待的 AcquireSessionID 必须共享同一个新激活的会话 ID")
	}
}
