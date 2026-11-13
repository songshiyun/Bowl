package rpc

import (
	"bytes"
	"fmt"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
)

const (
	// MaxReadPayloadSize 为池化零分配解码支持的最大单次 ReadResponse 载荷上限（4 MiB）。
	MaxReadPayloadSize = rpccodec.MaxPayloadSize

	// SmallReadPayloadCutoff 为小读响应免 4 MiB 缓冲池租约的字节上限（256 KiB）。
	// <= 256 KiB 的载荷（如 128 KiB 小文件读与 4 KiB 随机读）直接克隆为紧凑切片返回，
	// 避免占用 4 MiB 大缓冲槽位、全局租约锁竞争与 runtime.AddCleanup 注册开销。
	SmallReadPayloadCutoff = 256 << 10

	// readPayloadRetainedCap 常驻保留 16 x 4 MiB = 64 MiB noscan 读缓冲，
	// 支撑 W_read=4 多窗口并发流水线预读与直读在 GC 周期间零分配复用。
	readPayloadRetainedCap = 16
)

var readPayloadLeases = rpccodec.NewPayloadLeasePool[storagev1.ReadResponse](
	readPayloadRetainedCap,
	rpccodec.OrphanDropEntryOnly,
)

// ReadPayload 封装从 ReadResponse 接管所有权的底层切片及其幂等池归还句柄。
// 零值安全；调用 Release 后不得再读写 Data。
type ReadPayload struct {
	Data  []byte
	guard *rpccodec.Guard
}

// Release 幂等地将底层 4 MiB 池化缓冲归还缓冲池，并清空 Data 引用。
func (p *ReadPayload) Release() {
	if p == nil {
		return
	}
	if p.guard != nil {
		p.guard.Release()
		p.guard = nil
	}
	p.Data = nil
}

func clearReadResponseData(msg *storagev1.ReadResponse) {
	msg.Data = nil
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
	if len(data) <= SmallReadPayloadCutoff || len(data) > MaxReadPayloadSize {
		return ReadPayload{Data: data}
	}
	return ReadPayload{
		Data:  data,
		guard: readPayloadLeases.Claim(msg),
	}
}

// leasedReadPayloadCount 返回当前尚未接管或清理的 ReadResponse 映射项数（仅供单测校验零泄漏）。
func leasedReadPayloadCount() int {
	return readPayloadLeases.LeasedCount()
}

// newProtoBinaryCodec 构造客户端专用 ConnectRPC 二进制 Protobuf 编解码器。
func newProtoBinaryCodec() connect.Codec {
	return rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{
		MarshalAppend: marshalWriteRequestHook,
		Unmarshal:     unmarshalReadResponseHook,
	})
}

func marshalWriteRequestHook(dst []byte, message any) (out []byte, handled bool, err error) {
	writeReq, ok := message.(*storagev1.WriteRequest)
	if !ok {
		return nil, false, nil
	}
	out, err = marshalAppendWriteRequest(dst, writeReq)
	return out, true, err
}

func unmarshalReadResponseHook(data []byte, message any) (bool, error) {
	readResp, ok := message.(*storagev1.ReadResponse)
	if !ok {
		return false, nil
	}
	return true, unmarshalReadResponse(data, readResp)
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

func unmarshalReadResponse(raw []byte, msg *storagev1.ReadResponse) error {
	if msg == nil {
		return fmt.Errorf("unmarshal into %T: nil message", msg)
	}
	readPayloadLeases.Release(msg, clearReadResponseData)
	proto.Reset(msg)

	var leased *[MaxReadPayloadSize]byte
	fallback := func() error {
		if leased != nil {
			readPayloadLeases.RecycleBuf(leased)
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
					readPayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = []byte{}
			case len(v) <= SmallReadPayloadCutoff:
				if leased != nil {
					readPayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = bytes.Clone(v)
			case len(v) <= MaxReadPayloadSize:
				if leased == nil {
					leased = readPayloadLeases.AcquireBuf()
				}
				copy(leased[:len(v)], v)
				msg.Data = leased[:len(v):len(v)]
			default:
				if leased != nil {
					readPayloadLeases.RecycleBuf(leased)
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
		readPayloadLeases.Register(msg, leased)
	}
	return nil
}
