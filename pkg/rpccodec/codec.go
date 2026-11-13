package rpccodec

import (
	"fmt"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/proto"
)

const (
	// CodecNameProto 为 ConnectRPC 二进制 Protobuf 编解码器名称。
	CodecNameProto = "proto"
)

// CodecHooks 为热路径消息提供零分配编解码钩子；未命中消息自动回退标准 proto 实现。
type CodecHooks struct {
	MarshalAppend func(dst []byte, msg any) (out []byte, handled bool, err error)
	AfterMarshal  func(msg any)
	Unmarshal     func(data []byte, msg any) (handled bool, err error)
}

// ProtoBinaryCodec 实现 connect.Codec 及 MarshalAppend / MarshalStable / IsBinary 扩展接口。
type ProtoBinaryCodec struct {
	hooks CodecHooks
}

var _ connect.Codec = (*ProtoBinaryCodec)(nil)

// NewProtoBinaryCodec 创建带可选热路径钩子的 ProtoBinaryCodec 实例。
func NewProtoBinaryCodec(hooks CodecHooks) *ProtoBinaryCodec {
	return &ProtoBinaryCodec{hooks: hooks}
}

// Name 恒返回 "proto"。
func (c *ProtoBinaryCodec) Name() string {
	return CodecNameProto
}

// IsBinary 恒返回 true。
func (c *ProtoBinaryCodec) IsBinary() bool {
	return true
}

// Marshal 将消息编码为 Protobuf 二进制字节；命中 MarshalAppend 钩子时优先走零分配快路径。
func (c *ProtoBinaryCodec) Marshal(message any) ([]byte, error) {
	if c.hooks.MarshalAppend != nil {
		if out, handled, err := c.hooks.MarshalAppend(nil, message); handled {
			return out, err
		}
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if c.hooks.AfterMarshal != nil {
		defer c.hooks.AfterMarshal(message)
	}
	return proto.Marshal(protoMsg)
}

// MarshalAppend 将消息编码并追加至 dst；命中 MarshalAppend 钩子时优先走零分配快路径。
func (c *ProtoBinaryCodec) MarshalAppend(dst []byte, message any) ([]byte, error) {
	if c.hooks.MarshalAppend != nil {
		if out, handled, err := c.hooks.MarshalAppend(dst, message); handled {
			return out, err
		}
	}
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if c.hooks.AfterMarshal != nil {
		defer c.hooks.AfterMarshal(message)
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, protoMsg)
}

// MarshalStable 按确定性线格式编码消息，并在编码完成后触发 AfterMarshal 钩子。
func (c *ProtoBinaryCodec) MarshalStable(message any) ([]byte, error) {
	protoMsg, ok := message.(proto.Message)
	if !ok {
		return nil, fmt.Errorf("%T does not implement proto.Message", message)
	}
	if c.hooks.AfterMarshal != nil {
		defer c.hooks.AfterMarshal(message)
	}
	return proto.MarshalOptions{Deterministic: true}.Marshal(protoMsg)
}

// Unmarshal 从二进制字节解码消息；命中 Unmarshal 钩子时优先走池化快路径。
func (c *ProtoBinaryCodec) Unmarshal(data []byte, message any) error {
	if c.hooks.Unmarshal != nil {
		if handled, err := c.hooks.Unmarshal(data, message); handled {
			return err
		}
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
