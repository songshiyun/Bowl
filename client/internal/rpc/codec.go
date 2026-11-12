package rpc

import (
	"bytes"
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
	codecNameProto = "proto"

	// MaxReadPayloadSize 为池化零分配解码支持的最大单次 ReadResponse 载荷上限（4 MiB）。
	MaxReadPayloadSize = 4 << 20

	// SmallReadPayloadCutoff 为小读响应免 4 MiB 缓冲池租约的字节上限（256 KiB）。
	// <= 256 KiB 的载荷（如 128 KiB 小文件读与 4 KiB 随机读）直接克隆为紧凑切片返回，
	// 避免占用 4 MiB 大缓冲槽位、全局租约锁竞争与 runtime.AddCleanup 注册开销。
	SmallReadPayloadCutoff = 256 << 10

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
	if _, ok := leasedReadPayloads[key]; ok {
		delete(leasedReadPayloads, key)
		leasedReadPayloadActive.Add(-1)
	}
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

	leasedReadPayloadsMu    sync.Mutex
	leasedReadPayloads      = make(map[weak.Pointer[storagev1.ReadResponse]]*readPayloadLease)
	leasedReadPayloadActive atomic.Int64
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
	old, existed := leasedReadPayloads[key]
	if existed && old != nil && old != lease {
		prev = old
	}
	leasedReadPayloads[key] = lease
	if !existed {
		leasedReadPayloadActive.Add(1)
	}
	leasedReadPayloadsMu.Unlock()

	if prev != nil && prev.claimed.CompareAndSwap(false, true) {
		prev.msgCleanup.Stop()
		prev.release(false)
	}
}

func discardUnclaimedReadPayloadOnReset(msg *storagev1.ReadResponse) {
	if msg == nil || leasedReadPayloadActive.Load() == 0 {
		return
	}
	key := weak.Make(msg)
	leasedReadPayloadsMu.Lock()
	lease, existed := leasedReadPayloads[key]
	if existed {
		delete(leasedReadPayloads, key)
		leasedReadPayloadActive.Add(-1)
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
// 若消息非池化分配（如空切片、<=256 KiB 小载荷、>4 MiB 载荷或单测手造消息），Release() 为安全空操作。
func TakeReadPayload(msg *storagev1.ReadResponse) ReadPayload {
	if msg == nil {
		return ReadPayload{}
	}
	data := msg.Data
	msg.Data = nil
	if len(data) <= SmallReadPayloadCutoff || len(data) > MaxReadPayloadSize || leasedReadPayloadActive.Load() == 0 {
		return ReadPayload{Data: data}
	}

	key := weak.Make(msg)
	leasedReadPayloadsMu.Lock()
	lease, existed := leasedReadPayloads[key]
	if existed {
		delete(leasedReadPayloads, key)
		leasedReadPayloadActive.Add(-1)
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
	if writeReq, ok := message.(*storagev1.WriteRequest); ok {
		return marshalAppendWriteRequest(nil, writeReq)
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	return proto.Marshal(protoMsg)
}

func (c *protoBinaryCodec) MarshalAppend(dst []byte, message any) ([]byte, error) {
	if writeReq, ok := message.(*storagev1.WriteRequest); ok {
		return marshalAppendWriteRequest(dst, writeReq)
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, protoMsg)
}

func writeRequestWireSize(msg *storagev1.WriteRequest) int {
	var n int
	if msg.Ino != 0 {
		n += 1 + protowire.SizeVarint(msg.Ino)
	}
	if msg.Fh != 0 {
		n += 1 + protowire.SizeVarint(msg.Fh)
	}
	if msg.Offset != 0 {
		n += 1 + protowire.SizeVarint(msg.Offset)
	}
	if len(msg.Data) > 0 {
		n += 1 + protowire.SizeBytes(len(msg.Data))
	}
	if msg.DiskId != "" {
		n += 1 + protowire.SizeBytes(len(msg.DiskId))
	}
	if msg.Epoch != 0 {
		n += 1 + protowire.SizeVarint(msg.Epoch)
	}
	return n
}

func marshalAppendWriteRequest(dst []byte, msg *storagev1.WriteRequest) ([]byte, error) {
	if msg == nil {
		if dst == nil {
			return []byte{}, nil
		}
		return dst, nil
	}
	if !utf8.ValidString(msg.DiskId) {
		return proto.MarshalOptions{}.MarshalAppend(dst, msg)
	}

	wireSize := writeRequestWireSize(msg)
	if wireSize == 0 {
		if dst == nil {
			return []byte{}, nil
		}
		return dst, nil
	}
	if cap(dst)-len(dst) < wireSize {
		grown := make([]byte, len(dst), len(dst)+wireSize)
		copy(grown, dst)
		dst = grown
	}

	if msg.Ino != 0 {
		dst = protowire.AppendTag(dst, 1, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Ino)
	}
	if msg.Fh != 0 {
		dst = protowire.AppendTag(dst, 2, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Fh)
	}
	if msg.Offset != 0 {
		dst = protowire.AppendTag(dst, 3, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Offset)
	}
	if len(msg.Data) > 0 {
		dst = protowire.AppendTag(dst, 4, protowire.BytesType)
		dst = protowire.AppendBytes(dst, msg.Data)
	}
	if msg.DiskId != "" {
		dst = protowire.AppendTag(dst, 5, protowire.BytesType)
		dst = protowire.AppendString(dst, msg.DiskId)
	}
	if msg.Epoch != 0 {
		dst = protowire.AppendTag(dst, 6, protowire.VarintType)
		dst = protowire.AppendVarint(dst, msg.Epoch)
	}
	return dst, nil
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
			case len(v) <= SmallReadPayloadCutoff:
				if leased != nil {
					recycleReadPayloadBuf(leased)
					leased = nil
				}
				msg.Data = bytes.Clone(v)
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
				msg.Data = bytes.Clone(v)
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
