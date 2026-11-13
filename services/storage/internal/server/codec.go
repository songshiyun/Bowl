package server

import (
	"context"
	"fmt"
	"unicode/utf8"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/rpccodec"
)

const (
	writePayloadRetainedCap = 8 // 常驻 8 x 4 MiB = 32 MiB noscan 写缓冲（免受 GC 周期驱逐与清零）
	readPayloadRetainedCap  = 8 // 常驻 8 x 4 MiB = 32 MiB noscan 读缓冲（免受 GC 周期驱逐与清零）
)

var (
	writePayloadLeases = rpccodec.NewPayloadLeasePool[storagev1.WriteRequest](
		writePayloadRetainedCap,
		rpccodec.OrphanRecycleBuffer,
	)
	readPayloadLeases = rpccodec.NewPayloadLeasePool[storagev1.ReadResponse](
		readPayloadRetainedCap,
		rpccodec.OrphanRecycleBuffer,
	)
)

func clearWriteRequestData(msg *storagev1.WriteRequest) {
	msg.Data = nil
}

func clearReadResponseData(msg *storagev1.ReadResponse) {
	msg.Data = nil
}

func releaseWritePayload(msg *storagev1.WriteRequest) {
	writePayloadLeases.Release(msg, clearWriteRequestData)
}

// leasedWritePayloadCount 返回当前尚未归还池的 WriteRequest 缓冲区数量（仅供单测校验零泄漏）。
func leasedWritePayloadCount() int {
	return writePayloadLeases.LeasedCount()
}

func acquireReadPayloadBuf() *[maxReadWriteSize]byte {
	return readPayloadLeases.AcquireBuf()
}

func recycleReadPayloadBuf(buf *[maxReadWriteSize]byte) {
	readPayloadLeases.RecycleBuf(buf)
}

func registerLeasedReadPayload(msg *storagev1.ReadResponse, buf *[maxReadWriteSize]byte) {
	readPayloadLeases.Register(msg, buf)
}

func releaseReadPayload(msg *storagev1.ReadResponse) {
	readPayloadLeases.Release(msg, clearReadResponseData)
}

// leasedReadPayloadCount 返回当前尚未归还池的 ReadResponse 缓冲区数量（仅供单测校验零泄漏）。
func leasedReadPayloadCount() int {
	return readPayloadLeases.LeasedCount()
}

// newWritePayloadReleaseInterceptor 构造挂载在拦截器链最外层的生命周期回收器，
// 确保无论请求在校验拦截器被拒、正常处理完成还是发生异常，借用的 4 MiB 写缓冲均确定性归还缓冲池。
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

// newProtoBinaryCodec 构造存储端专用 ConnectRPC 二进制 Protobuf 编解码器。
func newProtoBinaryCodec() *rpccodec.ProtoBinaryCodec {
	return rpccodec.NewProtoBinaryCodec(rpccodec.CodecHooks{
		MarshalAppend: marshalReadResponseHook,
		AfterMarshal:  afterMarshalReadResponseHook,
		Unmarshal:     unmarshalWriteRequestHook,
	})
}

func afterMarshalReadResponseHook(message any) {
	if readResp, ok := message.(*storagev1.ReadResponse); ok && readResp != nil {
		releaseReadPayload(readResp)
	}
}

func marshalReadResponseHook(dst []byte, message any) (out []byte, handled bool, err error) {
	readResp, ok := message.(*storagev1.ReadResponse)
	if !ok {
		return nil, false, nil
	}
	if readResp == nil {
		return dst, true, nil
	}
	data := readResp.Data
	if len(data) == 0 {
		releaseReadPayload(readResp)
		return dst, true, nil
	}
	// ReadResponse 仅含字段 `bytes data = 1`（wire tag = (1<<3)|2 = 0x0a）。
	need := 1 + protowire.SizeVarint(uint64(len(data))) + len(data)
	if cap(dst)-len(dst) < need {
		grown := make([]byte, len(dst), len(dst)+need)
		copy(grown, dst)
		dst = grown
	}
	dst = append(dst, 0x0a)
	dst = protowire.AppendVarint(dst, uint64(len(data)))
	dst = append(dst, data...)
	releaseReadPayload(readResp)
	return dst, true, nil
}

func unmarshalWriteRequestHook(data []byte, message any) (bool, error) {
	writeReq, ok := message.(*storagev1.WriteRequest)
	if !ok {
		return false, nil
	}
	return true, unmarshalWriteRequest(data, writeReq)
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
			writePayloadLeases.RecycleBuf(leased)
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
					writePayloadLeases.RecycleBuf(leased)
					leased = nil
				}
				msg.Data = []byte{}
			case len(v) <= maxReadWriteSize:
				if leased == nil {
					leased = writePayloadLeases.AcquireBuf()
				}
				copy(leased[:len(v)], v)
				msg.Data = leased[:len(v):len(v)]
			default:
				if leased != nil {
					writePayloadLeases.RecycleBuf(leased)
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
		writePayloadLeases.Register(msg, leased)
	}
	return nil
}
