package rpc

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"weak"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	codecNameProto = "proto"

	// MaxReadPayloadSize 为池化零分配解码支持的最大单次 ReadResponse 载荷上限（4 MiB）。
	MaxReadPayloadSize = 4 << 20

	// readPayloadRetainedCap 常驻保留 16 x 4 MiB = 64 MiB noscan 读缓冲，
	// 支撑 W_read=4 多窗口并发流水线预读与直读在 GC 周期间零分配复用。
	readPayloadRetainedCap = 16
)

type readPayloadLease struct {
	key          weak.Pointer[storagev1.ReadResponse]
	buf          *[MaxReadPayloadSize]byte
	msgCleanup   runtime.Cleanup
	guardCleanup runtime.Cleanup
	claimed      atomic.Bool
	released     atomic.Bool
}

func (l *readPayloadLease) release(stopGuardCleanup bool) {
	if l == nil || !l.released.CompareAndSwap(false, true) {
		return
	}
	if stopGuardCleanup {
		l.guardCleanup.Stop()
	}
	recycleReadPayloadBuf(l.buf)
	l.buf = nil
}

type readPayloadGuard struct {
	lease *readPayloadLease
}

// ReadPayload 封装从 ReadResponse 接管所有权的底层切片及其幂等池归还句柄。
// 零值安全；调用 Release 后不得再读写 Data。
type ReadPayload struct {
	Data  []byte
	guard *readPayloadGuard
}

// Release 幂等地将底层 4 MiB 池化缓冲归还缓冲池，并清空 Data 引用。
func (p *ReadPayload) Release() {
	if p == nil {
		return
	}
	if p.guard != nil {
		if p.guard.lease != nil {
			p.guard.lease.release(true)
		}
		p.guard = nil
	}
	p.Data = nil
}

func cleanupUnclaimedReadPayloadEntry(key weak.Pointer[storagev1.ReadResponse]) {
	leasedReadPayloadsMu.Lock()
	delete(leasedReadPayloads, key)
	leasedReadPayloadsMu.Unlock()
}

func cleanupOrphanedReadPayloadLease(l *readPayloadLease) {
	l.release(false)
}

var (
	readPayloadFreeCh = make(chan *[MaxReadPayloadSize]byte, readPayloadRetainedCap)
	readPayloadPool   = sync.Pool{
		New: func() any {
			return new([MaxReadPayloadSize]byte)
		},
	}

	leasedReadPayloadsMu sync.Mutex
	leasedReadPayloads   = make(map[weak.Pointer[storagev1.ReadResponse]]*readPayloadLease)
)

func acquireReadPayloadBuf() *[MaxReadPayloadSize]byte {
	select {
	case buf := <-readPayloadFreeCh:
		return buf
	default:
	}
	buf, _ := readPayloadPool.Get().(*[MaxReadPayloadSize]byte)
	if buf == nil {
		return new([MaxReadPayloadSize]byte)
	}
	return buf
}

func recycleReadPayloadBuf(buf *[MaxReadPayloadSize]byte) {
	if buf == nil {
		return
	}
	select {
	case readPayloadFreeCh <- buf:
	default:
		readPayloadPool.Put(buf)
	}
}

func registerLeasedReadPayload(msg *storagev1.ReadResponse, buf *[MaxReadPayloadSize]byte) {
	key := weak.Make(msg)
	lease := &readPayloadLease{
		key: key,
		buf: buf,
	}
	// 若调用方未通过 TakeReadPayload 显式接管所有权，仅在 msg 被 GC 时移除弱引用表项，
	// 绝不背着外部可能持有的 msg.GetData() 切片擅自重用底层数组（零 UAF 风险）。
	lease.msgCleanup = runtime.AddCleanup(msg, cleanupUnclaimedReadPayloadEntry, key)

	var prev *readPayloadLease
	leasedReadPayloadsMu.Lock()
	if old := leasedReadPayloads[key]; old != nil && old != lease {
		prev = old
	}
	leasedReadPayloads[key] = lease
	leasedReadPayloadsMu.Unlock()

	if prev != nil && prev.claimed.CompareAndSwap(false, true) {
		prev.msgCleanup.Stop()
		prev.release(false)
	}
}

func discardUnclaimedReadPayloadOnReset(msg *storagev1.ReadResponse) {
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

	if lease != nil && lease.claimed.CompareAndSwap(false, true) {
		lease.msgCleanup.Stop()
		msg.Data = nil
		lease.release(false)
	}
}

// TakeReadPayload 从 ReadResponse 接管数据载荷所有权并返回带池归还能力的 ReadPayload。
// 若底层切片来自 4 MiB 缓冲池，调用返回的 ReadPayload.Release() 将确定性归还缓冲；
// 若遗漏调用 Release()，runtime.AddCleanup 会在 ReadPayload 不可达时兜底回收缓冲。
// 若消息非池化分配（如空切片、>4 MiB 载荷或单测手造消息），Release() 为安全空操作。
func TakeReadPayload(msg *storagev1.ReadResponse) ReadPayload {
	if msg == nil {
		return ReadPayload{}
	}
	data := msg.Data
	msg.Data = nil
	if len(data) == 0 {
		return ReadPayload{Data: data}
	}

	key := weak.Make(msg)
	leasedReadPayloadsMu.Lock()
	lease := leasedReadPayloads[key]
	if lease != nil {
		delete(leasedReadPayloads, key)
	}
	leasedReadPayloadsMu.Unlock()

	if lease == nil || !lease.claimed.CompareAndSwap(false, true) {
		return ReadPayload{Data: data}
	}
	lease.msgCleanup.Stop()

	guard := &readPayloadGuard{lease: lease}
	lease.guardCleanup = runtime.AddCleanup(guard, cleanupOrphanedReadPayloadLease, lease)
	return ReadPayload{
		Data:  data,
		guard: guard,
	}
}

// leasedReadPayloadCount 返回当前尚未接管或清理的 ReadResponse 映射项数（仅供单测校验零泄漏）。
func leasedReadPayloadCount() int {
	leasedReadPayloadsMu.Lock()
	defer leasedReadPayloadsMu.Unlock()
	return len(leasedReadPayloads)
}

// protoBinaryCodec 实现 connect.Codec 及其内部 marshalAppender / stableCodec 扩展接口，
// 对高频大载荷 ReadResponse 采用池化 4 MiB 缓冲直解，消除每次 4 MiB Read RPC 的堆分配与清零开销。
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
	return proto.Marshal(protoMsg)
}

func (c *protoBinaryCodec) MarshalAppend(dst []byte, message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, protoMsg)
}

func (c *protoBinaryCodec) MarshalStable(message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(protoMsg)
}

func (c *protoBinaryCodec) IsBinary() bool {
	return true
}

func (c *protoBinaryCodec) Unmarshal(data []byte, message any) error {
	if readResp, ok := message.(*storagev1.ReadResponse); ok {
		return unmarshalReadResponse(data, readResp)
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

func unmarshalReadResponse(raw []byte, msg *storagev1.ReadResponse) error {
	if msg == nil {
		return fmt.Errorf("unmarshal into %T: nil message", msg)
	}
	discardUnclaimedReadPayloadOnReset(msg)
	proto.Reset(msg)

	var leased *[MaxReadPayloadSize]byte
	fallback := func() error {
		if leased != nil {
			recycleReadPayloadBuf(leased)
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
		case 1: // bytes data
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
					recycleReadPayloadBuf(leased)
					leased = nil
				}
				msg.Data = []byte{}
			case len(v) <= MaxReadPayloadSize:
				if leased == nil {
					leased = acquireReadPayloadBuf()
				}
				copy(leased[:len(v)], v)
				msg.Data = leased[:len(v):len(v)]
			default:
				if leased != nil {
					recycleReadPayloadBuf(leased)
					leased = nil
				}
				msg.Data = append([]byte(nil), v...)
			}
			b = b[n:]
		default:
			return fallback()
		}
	}

	if leased != nil {
		registerLeasedReadPayload(msg, leased)
	}
	return nil
}
