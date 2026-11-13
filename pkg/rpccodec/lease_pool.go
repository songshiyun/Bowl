// Package rpccodec 提供跨端共享的 4 MiB noscan 缓冲租约池与可扩展 ConnectRPC 二进制 Protobuf 编解码器。
package rpccodec

import (
	"runtime"
	"sync"
	"sync/atomic"
	"weak"
)

const (
	// MaxPayloadSize 为池化零分配编解码支持的单次读写最大载荷上限（4 MiB）。
	MaxPayloadSize = 4 << 20
)

// OrphanPolicy 控制已注册对象在未被显式 Release 或 Claim 即被 GC 回收时的行为。
type OrphanPolicy uint8

const (
	// OrphanDropEntryOnly 仅注销弱引用表项与活跃计数，不回收到期缓冲（防外部仍持有切片引用时发生 UAF）。
	OrphanDropEntryOnly OrphanPolicy = iota
	// OrphanRecycleBuffer 注销弱引用表项并立即将缓冲归还缓冲池（用于无外部切片逃逸的服务端消息）。
	OrphanRecycleBuffer
)

type leaseReleaser interface {
	release(stopGuardCleanup bool)
}

// Guard 是从 PayloadLeasePool 接管租约所有权的幂等释放句柄。
// 零值与 nil 安全；调用 Release 后不得再读写对应载荷切片。
type Guard struct {
	lease leaseReleaser
}

// Release 幂等地停止兜底清理并将 4 MiB 缓冲归还缓冲池。
func (g *Guard) Release() {
	if g == nil || g.lease == nil {
		return
	}
	g.lease.release(true)
}

type payloadLease[T any] struct {
	pool         *PayloadLeasePool[T]
	key          weak.Pointer[T]
	buf          *[MaxPayloadSize]byte
	msgCleanup   runtime.Cleanup
	guardCleanup runtime.Cleanup
	claimed      atomic.Bool
	released     atomic.Bool
}

func (l *payloadLease[T]) release(stopGuardCleanup bool) {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	if stopGuardCleanup {
		l.guardCleanup.Stop()
	}
	buf := l.buf
	l.buf = nil
	l.pool.RecycleBuf(buf)
}

func cleanupUnclaimedLease[T any](l *payloadLease[T]) {
	p := l.pool
	p.mu.Lock()
	if p.leases[l.key] == l {
		delete(p.leases, l.key)
		p.active.Add(-1)
	}
	p.mu.Unlock()

	if p.policy == OrphanRecycleBuffer && l.claimed.CompareAndSwap(false, true) {
		l.release(false)
	}
}

func cleanupOrphanedGuard[T any](l *payloadLease[T]) {
	l.release(false)
}

// PayloadLeasePool 管理一组独立的 4 MiB noscan 缓冲与 weak.Pointer[T] 租约追踪表。
// 必须通过 NewPayloadLeasePool 实例化；不同实例间的缓冲通道、sync.Pool 与租约表完全隔离。
type PayloadLeasePool[T any] struct {
	freeCh chan *[MaxPayloadSize]byte
	pool   sync.Pool
	policy OrphanPolicy

	mu     sync.Mutex
	leases map[weak.Pointer[T]]*payloadLease[T]
	active atomic.Int64
}

// NewPayloadLeasePool 创建一个独立的 4 MiB 缓冲租约池。
// retainedCap 控制常驻通道免受 GC 驱逐的缓冲槽位数，小于 0 时钳位为 0。
func NewPayloadLeasePool[T any](retainedCap int, policy OrphanPolicy) *PayloadLeasePool[T] {
	if retainedCap < 0 {
		retainedCap = 0
	}
	return &PayloadLeasePool[T]{
		freeCh: make(chan *[MaxPayloadSize]byte, retainedCap),
		pool: sync.Pool{
			New: func() any {
				return new([MaxPayloadSize]byte)
			},
		},
		policy: policy,
		leases: make(map[weak.Pointer[T]]*payloadLease[T]),
	}
}

// AcquireBuf 从常驻通道或 sync.Pool 获取一个 4 MiB noscan 缓冲。
func (p *PayloadLeasePool[T]) AcquireBuf() *[MaxPayloadSize]byte {
	select {
	case buf := <-p.freeCh:
		return buf
	default:
	}
	buf, _ := p.pool.Get().(*[MaxPayloadSize]byte)
	if buf == nil {
		return new([MaxPayloadSize]byte)
	}
	return buf
}

// RecycleBuf 将 4 MiB 缓冲归还常驻通道或 sync.Pool；buf 为 nil 时安全空操作。
func (p *PayloadLeasePool[T]) RecycleBuf(buf *[MaxPayloadSize]byte) {
	if buf == nil {
		return
	}
	select {
	case p.freeCh <- buf:
	default:
		p.pool.Put(buf)
	}
}

// Register 为消息对象绑定一块 4 MiB 缓冲租约，并挂载未接管对象被 GC 时的清理策略。
// 若同一消息已存在旧租约，将先回收旧租约再写入新租约。
func (p *PayloadLeasePool[T]) Register(msg *T, buf *[MaxPayloadSize]byte) {
	if p == nil {
		return
	}
	if msg == nil {
		p.RecycleBuf(buf)
		return
	}
	if buf == nil {
		return
	}

	key := weak.Make(msg)
	lease := &payloadLease[T]{
		pool: p,
		key:  key,
		buf:  buf,
	}
	lease.msgCleanup = runtime.AddCleanup(msg, cleanupUnclaimedLease[T], lease)

	var prev *payloadLease[T]
	p.mu.Lock()
	old, existed := p.leases[key]
	if existed && old != nil && old != lease {
		prev = old
	}
	p.leases[key] = lease
	if !existed {
		p.active.Add(1)
	}
	p.mu.Unlock()

	if prev != nil && prev.claimed.CompareAndSwap(false, true) {
		prev.msgCleanup.Stop()
		prev.release(false)
	}
}

// Release 显式归还 msg 关联的 4 MiB 缓冲；在回收缓冲前调用 clearData 清空消息上的切片引用。
// 当无活跃租约时走无锁、零分配快路径直接返回 false。
func (p *PayloadLeasePool[T]) Release(msg *T, clearData func(*T)) bool {
	if p == nil || msg == nil || p.active.Load() == 0 {
		return false
	}

	key := weak.Make(msg)
	p.mu.Lock()
	lease, existed := p.leases[key]
	if existed {
		delete(p.leases, key)
		p.active.Add(-1)
	}
	p.mu.Unlock()

	if lease == nil || !lease.claimed.CompareAndSwap(false, true) {
		return false
	}
	lease.msgCleanup.Stop()
	if clearData != nil {
		clearData(msg)
	}
	lease.release(false)
	return true
}

// Claim 从 msg 接管租约所有权并返回带 GC 兜底回收能力的 Guard；
// 若 msg 无对应租约或无活跃租约，返回 nil。
func (p *PayloadLeasePool[T]) Claim(msg *T) *Guard {
	if p == nil || msg == nil || p.active.Load() == 0 {
		return nil
	}

	key := weak.Make(msg)
	p.mu.Lock()
	lease, existed := p.leases[key]
	if existed {
		delete(p.leases, key)
		p.active.Add(-1)
	}
	p.mu.Unlock()

	if lease == nil || !lease.claimed.CompareAndSwap(false, true) {
		return nil
	}
	lease.msgCleanup.Stop()

	guard := &Guard{lease: lease}
	lease.guardCleanup = runtime.AddCleanup(guard, cleanupOrphanedGuard[T], lease)
	return guard
}

// LeasedCount 返回当前尚未被接管、释放或清理的租约数量（仅供单测校验零泄漏）。
func (p *PayloadLeasePool[T]) LeasedCount() int {
	if p == nil {
		return 0
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.leases)
}
