// Package readbuf 管理单次 RPC 读取结果在序列化完成前的缓冲所有权。
package readbuf

import (
	"context"
	"sync"
)

const (
	minPooledSize = 256 << 10
	maxPooledSize = 4 << 20
	idleLimit     = 8
)

// Pool 最多保留八个不大于 4 MiB 的闲置缓冲；不限制或合并正在执行的读取。
type Pool struct{ idle chan []byte }

func NewPool() *Pool { return &Pool{idle: make(chan []byte, idleLimit)} }

type scopeKey struct{}

type scope struct {
	mu      sync.Mutex
	pool    *Pool
	buffers []*Buffer
	closed  bool
}

// WithScope 绑定 HTTP 请求的缓冲所有权。调用方必须在处理器完成序列化和写入后释放，
// 不能在领域 Read 返回时释放。结束后的上下文再次申请缓冲会退回普通分配。
func (p *Pool) WithScope(ctx context.Context) (context.Context, func()) {
	s := &scope{pool: p}
	return context.WithValue(ctx, scopeKey{}, s), s.close
}

func (s *scope) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	for _, b := range s.buffers {
		s.pool.put(b.data)
	}
	s.buffers = nil
}

// Buffer 由一次 Read 独占；同一次读取的重试可以替换内容，不可并发调用 Bytes。
type Buffer struct {
	pool *Pool
	data []byte
}

// NewBuffer 为一次领域读取创建独立所有权，未绑定请求范围时保留普通分配语义。
func NewBuffer(ctx context.Context) *Buffer {
	b := &Buffer{}
	if s, ok := ctx.Value(scopeKey{}).(*scope); ok {
		s.mu.Lock()
		defer s.mu.Unlock()
		if !s.closed {
			b.pool = s.pool
			s.buffers = append(s.buffers, b)
		}
	}
	return b
}

// Bytes 返回与 make([]byte, size) 一样已清零的区间。下一次调用会使旧区间失效；
// 最后一个区间保留到请求处理器结束，跨请求和重试都不会泄露旧文件或部分失败数据。
func (b *Buffer) Bytes(size uint32) []byte {
	if b.pool == nil {
		return make([]byte, size)
	}
	if cap(b.data) >= int(size) {
		b.data = b.data[:size]
		clear(b.data)
		return b.data
	}
	b.pool.put(b.data)
	b.data = b.pool.get(int(size))
	return b.data
}

func (p *Pool) get(size int) []byte {
	if size >= minPooledSize && size <= maxPooledSize {
		for range idleLimit {
			select {
			case data := <-p.idle:
				if cap(data) >= size {
					data = data[:size]
					clear(data)
					return data
				}
				// 丢弃容量不足的缓冲，给大读返回的缓冲留出槽位。
			default:
				return make([]byte, size)
			}
		}
	}
	return make([]byte, size)
}

func (p *Pool) put(data []byte) {
	if cap(data) < minPooledSize || cap(data) > maxPooledSize {
		return
	}
	select {
	case p.idle <- data[:0]:
	default:
	}
}
