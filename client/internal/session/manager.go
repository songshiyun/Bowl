package session

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// ErrBadFileHandle 表示文件句柄无效、未绑定或其所属会话已淘汰失效（对齐 POSIX EBADF 语义）。
var ErrBadFileHandle = syscall.EBADF

const (
	DefaultMinInterval         = 200 * time.Millisecond
	DefaultMaxInterval         = 20 * time.Second
	DefaultInitialRetryBackoff = 200 * time.Millisecond
	DefaultMaxRetryBackoff     = 2 * time.Second
	DefaultRenewRPCTimeout     = 3 * time.Second
)

// StorageClient 抽象 StorageService 的 RenewSession 与 CloseSession RPC 契约。
type StorageClient interface {
	RenewSession(context.Context, *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error)
	CloseSession(context.Context, *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error)
}

// Config 定义 ClientSession 生命周期的参数配置。
type Config struct {
	DiskID              string
	BranchName          string
	ReadOnly            bool
	CommitID            string
	MinInterval         time.Duration
	MaxInterval         time.Duration
	InitialRetryBackoff time.Duration
	MaxRetryBackoff     time.Duration
	RenewRPCTimeout     time.Duration
	OnSessionExpired    func(expiredSID uint64)
	Logger              *slog.Logger
}

// Manager 负责 ClientSession 自适应保活、超期自愈轮换及句柄血缘绑定。
type Manager struct {
	client StorageClient
	cfg    Config
	logger *slog.Logger

	// 当前活跃会话 ID（0 表示已过期未激活）
	currentSessionID atomic.Uint64

	// 句柄血缘映射与新会话就绪广播通道
	handleMu           sync.RWMutex
	handleSessions     map[uint64]uint64
	readyCh            chan struct{}
	onSessionExpiredFn []func(expiredSID uint64)

	// 后台续期协程生命周期控制
	loopCtx     context.Context
	cancelLoop  context.CancelFunc
	loopDone    chan struct{}
	renewWakeCh chan struct{}
	startOnce   sync.Once
	closeOnce   sync.Once

	// 最新租约截止时间
	expiresAtMu sync.RWMutex
	expiresAt   time.Time
}

// New 生成 63 位随机正整数并调用 RenewSession 激活初始 ClientSession。
// 若初始激活失败则返回错误，阻止挂载进入就绪态。
func New(ctx context.Context, client StorageClient, cfg Config) (*Manager, error) {
	if client == nil {
		return nil, errors.New("storage client is nil")
	}

	normalizeConfig(&cfg)

	initialSID, err := generateSessionID(0)
	if err != nil {
		return nil, fmt.Errorf("generate initial session id: %w", err)
	}

	callStart := time.Now()
	callCtx, cancel := context.WithTimeout(ctx, cfg.RenewRPCTimeout)
	resp, err := client.RenewSession(callCtx, newRenewSessionRequest(cfg, initialSID))
	cancel()
	if err != nil {
		return nil, fmt.Errorf("activate initial session: %w", err)
	}

	var expiresAt time.Time
	if expTs := resp.Msg.GetExpiresAt(); expTs != nil {
		expiresAt = expTs.AsTime()
	} else {
		expiresAt = callStart.Add(15 * time.Second)
	}

	loopCtx, cancelLoop := context.WithCancel(context.Background())

	readyCh := make(chan struct{})
	close(readyCh)

	m := &Manager{
		client:         client,
		cfg:            cfg,
		logger:         cfg.Logger,
		handleSessions: make(map[uint64]uint64),
		readyCh:        readyCh,
		loopCtx:        loopCtx,
		cancelLoop:     cancelLoop,
		loopDone:       make(chan struct{}),
		renewWakeCh:    make(chan struct{}, 1),
		expiresAt:      expiresAt,
	}
	m.currentSessionID.Store(initialSID)

	return m, nil
}

// RegisterOnSessionExpired 线程安全地注册会话过期淘汰回调。
// 当某个会话过期淘汰时，每个过期 sessionID 保证至多触发一次回调。
func (m *Manager) RegisterOnSessionExpired(fn func(expiredSID uint64)) {
	if m == nil || fn == nil {
		return
	}
	m.handleMu.Lock()
	m.onSessionExpiredFn = append(m.onSessionExpiredFn, fn)
	m.handleMu.Unlock()
}

// TriggerRenew 按需唤醒后台 renewLoop 立即发起一次续约探测（例如业务 RPC 遇到服务端返回 EBADF 时）。
func (m *Manager) TriggerRenew() {
	if m == nil {
		return
	}
	select {
	case m.renewWakeCh <- struct{}{}:
	default:
	}
}

func (m *Manager) invalidateExpiredSession(oldSID uint64) {
	if m == nil || oldSID == 0 {
		return
	}
	m.handleMu.Lock()
	if m.currentSessionID.Load() != oldSID {
		m.handleMu.Unlock()
		return
	}
	m.currentSessionID.Store(0)
	m.readyCh = make(chan struct{})
	clear(m.handleSessions)

	var callbacks []func(expiredSID uint64)
	if m.cfg.OnSessionExpired != nil {
		callbacks = append(callbacks, m.cfg.OnSessionExpired)
	}
	callbacks = append(callbacks, m.onSessionExpiredFn...)
	m.handleMu.Unlock()

	for _, cb := range callbacks {
		if cb != nil {
			cb(oldSID)
		}
	}
}

// Start 启动后台自适应租约续期保活协程。
// 协程会按 (expiresAt - now) / 3 动态心跳保活；收到 Close 信号时干净终止。
func (m *Manager) Start(ctx context.Context) error {
	var alreadyStarted bool
	m.startOnce.Do(func() {
		alreadyStarted = true
		go func() {
			defer close(m.loopDone)
			m.renewLoop(m.loopCtx)
		}()
	})
	if !alreadyStarted {
		return errors.New("session manager renew loop already started")
	}
	return nil
}

func (m *Manager) renewLoop(ctx context.Context) {
	currentBackoff := m.cfg.InitialRetryBackoff

	for {
		sid := m.currentSessionID.Load()
		if sid == 0 {
			m.handleSessionExpired(ctx, sid)
			currentBackoff = m.cfg.InitialRetryBackoff
			continue
		}

		m.expiresAtMu.RLock()
		exp := m.expiresAt
		m.expiresAtMu.RUnlock()

		now := time.Now()
		if !now.Before(exp) {
			m.handleSessionExpired(ctx, sid)
			currentBackoff = m.cfg.InitialRetryBackoff
			continue
		}

		rem := exp.Sub(now)
		interval := rem / 3
		if interval < m.cfg.MinInterval {
			interval = m.cfg.MinInterval
		} else if interval > m.cfg.MaxInterval {
			interval = m.cfg.MaxInterval
		}
		if interval > rem {
			interval = rem
		}

		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		case <-m.renewWakeCh:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		}

		sid = m.currentSessionID.Load()
		if sid == 0 {
			m.handleSessionExpired(ctx, sid)
			currentBackoff = m.cfg.InitialRetryBackoff
			continue
		}

		m.expiresAtMu.RLock()
		exp = m.expiresAt
		m.expiresAtMu.RUnlock()

		if !time.Now().Before(exp) {
			m.handleSessionExpired(ctx, sid)
			currentBackoff = m.cfg.InitialRetryBackoff
			continue
		}

		rpcTimeout := min(m.cfg.RenewRPCTimeout, time.Until(exp))
		callCtx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := m.client.RenewSession(callCtx, newRenewSessionRequest(m.cfg, sid))
		cancel()
		if err != nil {
			if ctx.Err() != nil {
				return
			}

			m.expiresAtMu.RLock()
			expNow := m.expiresAt
			m.expiresAtMu.RUnlock()

			remNow := time.Until(expNow)
			if remNow <= 0 || m.currentSessionID.Load() != sid {
				m.handleSessionExpired(ctx, sid)
				currentBackoff = m.cfg.InitialRetryBackoff
				continue
			}

			sleepDur := min(currentBackoff, remNow)
			m.logger.Warn("renew session failed, retrying with backoff",
				"session_id", sid,
				"error", err,
				"backoff", sleepDur,
			)

			backoffTimer := time.NewTimer(sleepDur)
			select {
			case <-ctx.Done():
				backoffTimer.Stop()
				return
			case <-backoffTimer.C:
			case <-m.renewWakeCh:
				if !backoffTimer.Stop() {
					select {
					case <-backoffTimer.C:
					default:
					}
				}
			}

			if !time.Now().Before(expNow) || m.currentSessionID.Load() != sid {
				m.handleSessionExpired(ctx, sid)
				currentBackoff = m.cfg.InitialRetryBackoff
				continue
			}

			currentBackoff = min(currentBackoff*2, m.cfg.MaxRetryBackoff)
			continue
		}

		currentBackoff = m.cfg.InitialRetryBackoff

		if resp.Msg.GetSessionRecreated() || m.currentSessionID.Load() != sid {
			m.handleSessionExpired(ctx, sid)
			continue
		}

		if expTs := resp.Msg.GetExpiresAt(); expTs != nil {
			m.expiresAtMu.Lock()
			m.expiresAt = expTs.AsTime()
			m.expiresAtMu.Unlock()
		}
	}
}

func (m *Manager) handleSessionExpired(ctx context.Context, oldSID uint64) {
	// 立即淘汰旧会话、清空本地句柄血缘映射并触发 OnSessionExpired 清理回调
	m.invalidateExpiredSession(oldSID)

	newSID, err := generateSessionID(oldSID)
	if err != nil {
		m.logger.Error("failed to generate new session id on session expiry", "error", err)
		return
	}

	m.logger.Warn("client session expired on server; rotating session_id",
		"old_session_id", oldSID,
		"new_session_id", newSID,
	)

	// 激活新会话，遇到临时网络错误时以指数退避重试保活
	backoff := m.cfg.InitialRetryBackoff
	for {
		callStart := time.Now()
		callCtx, cancel := context.WithTimeout(ctx, m.cfg.RenewRPCTimeout)
		resp, err := m.client.RenewSession(callCtx, newRenewSessionRequest(m.cfg, newSID))
		cancel()
		if err == nil {
			var newExp time.Time
			if expTs := resp.Msg.GetExpiresAt(); expTs != nil {
				newExp = expTs.AsTime()
			} else {
				newExp = callStart.Add(15 * time.Second)
			}
			m.expiresAtMu.Lock()
			m.expiresAt = newExp
			m.expiresAtMu.Unlock()

			// 新会话激活成功，原子切换当前活跃会话 ID 并关闭 readyCh 广播唤醒所有等待的 AcquireSessionID 调用方
			m.handleMu.Lock()
			m.currentSessionID.Store(newSID)
			select {
			case <-m.readyCh:
			default:
				close(m.readyCh)
			}
			m.handleMu.Unlock()

			// 排空在激活重试期间积压的唤醒信号，使返回 renewLoop 后正常按 1/3 租约睡眠
			select {
			case <-m.renewWakeCh:
			default:
			}
			return
		}

		if ctx.Err() != nil {
			return
		}

		m.logger.Warn("activate rotated session failed, retrying with backoff",
			"new_session_id", newSID,
			"error", err,
			"backoff", backoff,
		)

		backoffTimer := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			backoffTimer.Stop()
			return
		case <-backoffTimer.C:
		case <-m.renewWakeCh:
			if !backoffTimer.Stop() {
				select {
				case <-backoffTimer.C:
				default:
				}
			}
		}

		backoff = min(backoff*2, m.cfg.MaxRetryBackoff)
	}
}

// CurrentSessionID 返回当前活跃会话 ID（若已过期未激活则返回 0）。
func (m *Manager) CurrentSessionID() uint64 {
	return m.currentSessionID.Load()
}

// AcquireSessionID 获取当前有效的活跃会话 ID。
// 若当前会话未激活（currentSessionID == 0）或本地已超过宽限期，则立即唤醒后台 renewLoop 跳过退避睡眠重试注册，
// 并阻塞等待新会话激活就绪（绝不在业务协程自行发起 RenewSession RPC）。
func (m *Manager) AcquireSessionID(ctx context.Context) (uint64, error) {
	if sid := m.currentSessionID.Load(); sid != 0 {
		m.expiresAtMu.RLock()
		exp := m.expiresAt
		m.expiresAtMu.RUnlock()
		if time.Now().Before(exp) {
			return sid, nil
		}
		m.invalidateExpiredSession(sid)
		m.TriggerRenew()
	}

	m.handleMu.RLock()
	if sid := m.currentSessionID.Load(); sid != 0 {
		m.handleMu.RUnlock()
		return sid, nil
	}
	readyCh := m.readyCh
	m.handleMu.RUnlock()

	m.TriggerRenew()

	select {
	case <-readyCh:
		return m.currentSessionID.Load(), nil
	case <-m.loopCtx.Done():
		return 0, m.loopCtx.Err()
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

// BindHandle 在 Open/Create/OpenDir 成功时登记句柄与打开时刻使用的 sessionID。
// 仅记录 fh -> session_id 整数映射，绝不缓存文件属性或数据。
func (m *Manager) BindHandle(fh uint64, sessionID uint64) {
	m.handleMu.Lock()
	m.handleSessions[fh] = sessionID
	m.handleMu.Unlock()
}

// LookupHandleSession 查询指定 fh 打开时登记的有效 sessionID。
// 若句柄未注册、会话已轮换或本地租约已超期，立即清理并返回 (0, false) 或 (sid, false)。
func (m *Manager) LookupHandleSession(fh uint64) (uint64, bool) {
	m.handleMu.RLock()
	sid, ok := m.handleSessions[fh]
	m.handleMu.RUnlock()
	if !ok {
		return 0, false
	}

	if sid != m.currentSessionID.Load() {
		return sid, false
	}

	m.expiresAtMu.RLock()
	exp := m.expiresAt
	m.expiresAtMu.RUnlock()

	if !time.Now().Before(exp) {
		m.invalidateExpiredSession(sid)
		m.TriggerRenew()
		return sid, false
	}

	return sid, true
}

// ResolveHandleSession 校验并获取指定 fh 绑定的有效 sessionID。
// 若句柄不存在、其绑定的会话已被淘汰或本地租约已超期，立即清理并返回 ErrBadFileHandle (syscall.EBADF)。
// 严格杜绝借用新 CurrentSessionID 伪造续命老句柄。
func (m *Manager) ResolveHandleSession(fh uint64) (uint64, error) {
	m.handleMu.RLock()
	sid, ok := m.handleSessions[fh]
	m.handleMu.RUnlock()
	if !ok {
		return 0, ErrBadFileHandle
	}

	if sid != m.currentSessionID.Load() {
		return sid, ErrBadFileHandle
	}

	m.expiresAtMu.RLock()
	exp := m.expiresAt
	m.expiresAtMu.RUnlock()

	if !time.Now().Before(exp) {
		m.invalidateExpiredSession(sid)
		m.TriggerRenew()
		return sid, ErrBadFileHandle
	}

	return sid, nil
}

// UnbindHandle 在 Release/ReleaseDir 完成时清理句柄映射条目，避免内存泄漏。
func (m *Manager) UnbindHandle(fh uint64) {
	m.handleMu.Lock()
	delete(m.handleSessions, fh)
	m.handleMu.Unlock()
}

// Close 优雅停止后台续期协程并尽力注销当前活跃会话。
func (m *Manager) Close() error {
	m.closeOnce.Do(func() {
		m.cancelLoop()
		// 若后台协程未启动过，直接关闭 loopDone 避免死锁
		m.startOnce.Do(func() {
			close(m.loopDone)
		})
		<-m.loopDone

		if sid := m.currentSessionID.Load(); sid > 0 {
			closeCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			if _, err := m.client.CloseSession(closeCtx, connect.NewRequest(&storagev1.CloseSessionRequest{
				SessionId: sid,
			})); err != nil {
				m.logger.Warn("close session failed on shutdown",
					"session_id", sid,
					"error", err,
				)
			}
		}
	})
	return nil
}

func newRenewSessionRequest(cfg Config, sessionID uint64) *connect.Request[storagev1.RenewSessionRequest] {
	req := &storagev1.RenewSessionRequest{
		SessionId: sessionID,
	}
	if cfg.DiskID != "" {
		req.DiskId = &cfg.DiskID
	}
	if cfg.BranchName != "" {
		req.BranchName = &cfg.BranchName
	}
	if cfg.CommitID != "" {
		req.CommitId = &cfg.CommitID
	}
	req.ReadOnly = cfg.ReadOnly
	return connect.NewRequest(req)
}

func normalizeConfig(cfg *Config) {
	if cfg.MinInterval <= 0 {
		cfg.MinInterval = DefaultMinInterval
	}
	if cfg.MaxInterval <= 0 {
		cfg.MaxInterval = DefaultMaxInterval
	}
	if cfg.MinInterval > cfg.MaxInterval {
		cfg.MinInterval, cfg.MaxInterval = cfg.MaxInterval, cfg.MinInterval
	}
	if cfg.InitialRetryBackoff <= 0 {
		cfg.InitialRetryBackoff = DefaultInitialRetryBackoff
	}
	if cfg.MaxRetryBackoff <= 0 {
		cfg.MaxRetryBackoff = DefaultMaxRetryBackoff
	}
	if cfg.InitialRetryBackoff > cfg.MaxRetryBackoff {
		cfg.InitialRetryBackoff, cfg.MaxRetryBackoff = cfg.MaxRetryBackoff, cfg.InitialRetryBackoff
	}
	if cfg.RenewRPCTimeout <= 0 {
		cfg.RenewRPCTimeout = DefaultRenewRPCTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
}

// generateSessionID 生成在 [1, math.MaxInt64] 范围内的加密安全随机 63 位正整数。
func generateSessionID(exclude uint64) (uint64, error) {
	for {
		var buf [8]byte
		if _, err := rand.Read(buf[:]); err != nil {
			return 0, fmt.Errorf("read crypto random: %w", err)
		}
		// 掩码 0x7FFFFFFFFFFFFFFF 确保结果落在 [0, math.MaxInt64] 范围内
		sid := binary.BigEndian.Uint64(buf[:]) & 0x7FFFFFFFFFFFFFFF
		if sid > 0 && sid != exclude {
			return sid, nil
		}
	}
}
