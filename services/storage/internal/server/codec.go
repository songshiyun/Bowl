package server

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"unicode/utf8"
	"weak"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	codecNameProto          = "proto"
	writePayloadRetainedCap = 8 // 常驻 8 x 4 MiB = 32 MiB noscan 写缓冲（免受 GC 周期驱逐与清零）
	readPayloadRetainedCap  = 8 // 常驻 8 x 4 MiB = 32 MiB noscan 读缓冲（免受 GC 周期驱逐与清零）
)

type writePayloadLease struct {
	key      weak.Pointer[storagev1.WriteRequest]
	buf      *[maxReadWriteSize]byte
	cleanup  runtime.Cleanup
	released atomic.Bool
}

func (l *writePayloadLease) release(stopCleanup bool) {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	if stopCleanup {
		l.cleanup.Stop()
	}
	leasedPayloadsMu.Lock()
	if leasedPayloads[l.key] == l {
		delete(leasedPayloads, l.key)
	}
	leasedPayloadsMu.Unlock()
	recycleWritePayloadBuf(l.buf)
	l.buf = nil
}

func cleanupOrphanedWritePayloadLease(l *writePayloadLease) {
	l.release(false)
}

var (
	writePayloadFreeCh = make(chan *[maxReadWriteSize]byte, writePayloadRetainedCap)
	writePayloadPool   = sync.Pool{
		New: func() any {
			return new([maxReadWriteSize]byte)
		},
	}

	leasedPayloadsMu sync.Mutex
	leasedPayloads   = make(map[weak.Pointer[storagev1.WriteRequest]]*writePayloadLease)
)

func acquireWritePayloadBuf() *[maxReadWriteSize]byte {
	select {
	case buf := <-writePayloadFreeCh:
		return buf
	default:
	}
	buf, _ := writePayloadPool.Get().(*[maxReadWriteSize]byte)
	if buf == nil {
		return new([maxReadWriteSize]byte)
	}
	return buf
}

func recycleWritePayloadBuf(buf *[maxReadWriteSize]byte) {
	if buf == nil {
		return
	}
	select {
	case writePayloadFreeCh <- buf:
	default:
		writePayloadPool.Put(buf)
	}
}

func registerLeasedWritePayload(msg *storagev1.WriteRequest, buf *[maxReadWriteSize]byte) {
	key := weak.Make(msg)
	lease := &writePayloadLease{
		key: key,
		buf: buf,
	}
	lease.cleanup = runtime.AddCleanup(msg, cleanupOrphanedWritePayloadLease, lease)

	var prev *writePayloadLease
	leasedPayloadsMu.Lock()
	if old := leasedPayloads[key]; old != nil && old != lease {
		prev = old
	}
	leasedPayloads[key] = lease
	leasedPayloadsMu.Unlock()

	if prev != nil {
		prev.release(true)
	}
}

func releaseWritePayload(msg *storagev1.WriteRequest) {
	if msg == nil {
		return
	}
	key := weak.Make(msg)
	leasedPayloadsMu.Lock()
	lease := leasedPayloads[key]
	if lease != nil {
		delete(leasedPayloads, key)
	}
	leasedPayloadsMu.Unlock()

	if lease != nil {
		msg.Data = nil
		lease.release(true)
	}
}

// leasedWritePayloadCount 返回当前尚未归还池的 WriteRequest 缓冲区数量（仅供单测校验零泄漏）。
func leasedWritePayloadCount() int {
	leasedPayloadsMu.Lock()
	defer leasedPayloadsMu.Unlock()
	return len(leasedPayloads)
}

type readPayloadLease struct {
	key      weak.Pointer[storagev1.ReadResponse]
	buf      *[maxReadWriteSize]byte
	cleanup  runtime.Cleanup
	released atomic.Bool
}

func (l *readPayloadLease) release(stopCleanup bool) {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	if stopCleanup {
		l.cleanup.Stop()
	}
	leasedReadPayloadsMu.Lock()
	if leasedReadPayloads[l.key] == l {
		delete(leasedReadPayloads, l.key)
	}
	leasedReadPayloadsMu.Unlock()
	recycleReadPayloadBuf(l.buf)
	l.buf = nil
}

func cleanupOrphanedReadPayloadLease(l *readPayloadLease) {
	l.release(false)
}

var (
	readPayloadFreeCh = make(chan *[maxReadWriteSize]byte, readPayloadRetainedCap)
	readPayloadPool   = sync.Pool{
		New: func() any {
			return new([maxReadWriteSize]byte)
		},
	}

	leasedReadPayloadsMu sync.Mutex
	leasedReadPayloads   = make(map[weak.Pointer[storagev1.ReadResponse]]*readPayloadLease)
)

func acquireReadPayloadBuf() *[maxReadWriteSize]byte {
	select {
	case buf := <-readPayloadFreeCh:
		return buf
	default:
	}
	buf, _ := readPayloadPool.Get().(*[maxReadWriteSize]byte)
	if buf == nil {
		return new([maxReadWriteSize]byte)
	}
	return buf
}

func recycleReadPayloadBuf(buf *[maxReadWriteSize]byte) {
	if buf == nil {
		return
	}
	select {
	case readPayloadFreeCh <- buf:
	default:
		readPayloadPool.Put(buf)
	}
}

func registerLeasedReadPayload(msg *storagev1.ReadResponse, buf *[maxReadWriteSize]byte) {
	key := weak.Make(msg)
	lease := &readPayloadLease{
		key: key,
		buf: buf,
	}
	lease.cleanup = runtime.AddCleanup(msg, cleanupOrphanedReadPayloadLease, lease)

	var prev *readPayloadLease
	leasedReadPayloadsMu.Lock()
	if old := leasedReadPayloads[key]; old != nil && old != lease {
		prev = old
	}
	leasedReadPayloads[key] = lease
	leasedReadPayloadsMu.Unlock()

	if prev != nil {
		prev.release(true)
	}
}

func releaseReadPayload(msg *storagev1.ReadResponse) {
	if msg == nil {
		return
	}
	key := weak.Make(msg)
	leasedReadPayloadsMu.Lock()
	lease := leasedReadPayloads[key]
	if lease != nil {
		delete(leasedReadPayloads, key)
	}
	leasedReadPayloadsMu.Unlock()

	if lease != nil {
		msg.Data = nil
		lease.release(true)
	}
}

// leasedReadPayloadCount 返回当前尚未归还池的 ReadResponse 缓冲区数量（仅供单测校验零泄漏）。
func leasedReadPayloadCount() int {
	leasedReadPayloadsMu.Lock()
	defer leasedReadPayloadsMu.Unlock()
	return len(leasedReadPayloads)
}

// newWritePayloadReleaseInterceptor 构造挂载在拦截器链最外层的生命周期回收器，
// 确保无论请求在校验拦截器被拒、正常处理完成还是发生异常，借用的 4 MiB 写缓冲均确定性归还 sync.Pool。
func newWritePayloadReleaseInterceptor() connect.UnaryInterceptorFunc {
	return func(next connect.UnaryFunc) connect.UnaryFunc {
		return func(ctx context.Context, req connect.AnyRequest) (connect.AnyResponse, error) {
			if req != nil {
				if msg, ok := req.Any().(*storagev1.WriteRequest); ok {
					defer releaseWritePayload(msg)
				}
			}
			return next(ctx, req)
		}
	}
}

// protoBinaryCodec 实现 connect.Codec 及其内部的 marshalAppender / stableCodec 扩展接口，
// 对高频大载荷 WriteRequest 采用池化 4 MiB 缓冲直解，消除每次 4 MiB Write RPC 的堆分配、清零与 GC 扫描开销。
type protoBinaryCodec struct{}

func newProtoBinaryCodec() connect.Codec {
	return &protoBinaryCodec{}
}

func (c *protoBinaryCodec) Name() string {
	return codecNameProto
}

func (c *protoBinaryCodec) Marshal(message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if readResp, ok := message.(*storagev1.ReadResponse); ok && readResp != nil {
		defer releaseReadPayload(readResp)
	}
	return proto.Marshal(protoMsg)
}

func (c *protoBinaryCodec) MarshalAppend(dst []byte, message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if readResp, ok := message.(*storagev1.ReadResponse); ok && readResp != nil {
		defer releaseReadPayload(readResp)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, protoMsg)
}

func (c *protoBinaryCodec) MarshalStable(message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if readResp, ok := message.(*storagev1.ReadResponse); ok && readResp != nil {
		defer releaseReadPayload(readResp)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(protoMsg)
}

func (c *protoBinaryCodec) IsBinary() bool {
	return true
}

func (c *protoBinaryCodec) Unmarshal(data []byte, message any) error {
	if writeReq, ok := message.(*storagev1.WriteRequest); ok {
		return unmarshalWriteRequest(data, writeReq)
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return fmt.Errorf("%T does not implement proto.Message", message)
	}
	if err := proto.Unmarshal(data, protoMsg); err != nil {
		return fmt.Errorf("unmarshal into %T: %w", message, err)
	}
	return nil
}

func unmarshalWriteRequest(raw []byte, msg *storagev1.WriteRequest) error {
	if msg == nil {
		return fmt.Errorf("unmarshal into %T: nil message", msg)
	}
	releaseWritePayload(msg)
	proto.Reset(msg)
	var leased *[maxReadWriteSize]byte
	fallback := func() error {
		if leased != nil {
			recycleWritePayloadBuf(leased)
			leased = nil
		}
		proto.Reset(msg)
		if err := proto.Unmarshal(raw, msg); err != nil {
			return fmt.Errorf("unmarshal into %T: %w", msg, err)
		}
		return nil
	}

	b := raw
	for len(b) > 0 {
		num, typ, tagLen := protowire.ConsumeTag(b)
		if tagLen < 0 {
			return fallback()
		}
		b = b[tagLen:]

		switch num {
		case 1: // uint64 ino
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Ino = v
			b = b[n:]
		case 2: // uint64 fh
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Fh = v
			b = b[n:]
		case 3: // uint64 offset
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Offset = v
			b = b[n:]
		case 4: // bytes data
			if typ != protowire.BytesType {
				return fallback()
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 {
				return fallback()
			}
			switch {
			case len(v) == 0:
				if leased != nil {
					recycleWritePayloadBuf(leased)
					leased = nil
				}
				msg.Data = []byte{}
			case len(v) <= maxReadWriteSize:
				if leased == nil {
					leased = acquireWritePayloadBuf()
				}
				copy(leased[:len(v)], v)
				msg.Data = leased[:len(v):len(v)]
			default:
				if leased != nil {
					recycleWritePayloadBuf(leased)
					leased = nil
				}
				msg.Data = append([]byte(nil), v...)
			}
			b = b[n:]
		case 5: // string disk_id
			if typ != protowire.BytesType {
				return fallback()
			}
			v, n := protowire.ConsumeBytes(b)
			if n < 0 || !utf8.Valid(v) {
				return fallback()
			}
			msg.DiskId = string(v)
			b = b[n:]
		case 6: // uint64 epoch
			if typ != protowire.VarintType {
				return fallback()
			}
			v, n := protowire.ConsumeVarint(b)
			if n < 0 {
				return fallback()
			}
			msg.Epoch = v
			b = b[n:]
		default:
			return fallback()
		}
	}

	if leased != nil {
		registerLeasedWritePayload(msg, leased)
	}
	return nil
}
