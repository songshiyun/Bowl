package server

import (
	"bytes"
	"context"
	"runtime"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/pkg/validator"
)

func unmarshalAndReleaseWritePayload(t *testing.T, codec connect.Codec, raw []byte) {
	t.Helper()
	var m storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(raw, &m))
	releaseWritePayload(&m)
}

func unmarshalAbandonedWriteRequest(t *testing.T, codec connect.Codec, raw []byte) {
	t.Helper()
	msg := &storagev1.WriteRequest{}
	require.NoError(t, codec.Unmarshal(raw, msg))
	require.Equal(t, 1, leasedWritePayloadCount())
	runtime.KeepAlive(msg)
}

func TestProtoBinaryCodecWriteRequestPoolAndMalformedWireFallback(t *testing.T) {
	codec := newProtoBinaryCodec()
	require.Equal(t, "proto", codec.Name())

	payload := bytes.Repeat([]byte{0x11, 0x22, 0x33, 0x44}, maxReadWriteSize/4)
	orig := &storagev1.WriteRequest{
		Ino:    42,
		Fh:     7,
		Offset: 8 << 20,
		Data:   payload,
		DiskId: "123e4567-e89b-12d3-a456-426614174000",
		Epoch:  3,
	}
	raw, err := codec.Marshal(orig)
	require.NoError(t, err)

	// 1. 正常 4 MiB 解包与池归还：篡改底层输入切片不影响已解码的 msg.Data
	rawMut := append([]byte(nil), raw...)
	var decoded storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(rawMut, &decoded))
	clear(rawMut)
	require.Equal(t, orig.Ino, decoded.Ino)
	require.Equal(t, orig.Fh, decoded.Fh)
	require.Equal(t, orig.Offset, decoded.Offset)
	require.Equal(t, orig.DiskId, decoded.DiskId)
	require.Equal(t, orig.Epoch, decoded.Epoch)
	require.Equal(t, payload, decoded.Data)
	require.Equal(t, maxReadWriteSize, cap(decoded.Data))
	require.Equal(t, 1, leasedWritePayloadCount())

	releaseWritePayload(&decoded)
	require.Nil(t, decoded.Data)
	require.Zero(t, leasedWritePayloadCount())

	// 2. 预热池后连续 8 次解包 4 MiB 请求（无池化需分配 32 MiB），总堆分配增量必须严格 < 64 KiB
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	unmarshalAndReleaseWritePayload(t, codec, raw)
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10))
	require.Zero(t, leasedWritePayloadCount())

	// 3. 标准客户端无法构造的 Protobuf 线格式边界：空载荷、同包重复字段覆盖、超上限载荷、未知字段及异构 wire type 回退
	emptyRaw, err := proto.Marshal(&storagev1.WriteRequest{
		Ino:    9,
		DiskId: "123e4567-e89b-12d3-a456-426614174000",
		Epoch:  1,
	})
	require.NoError(t, err)

	var mEmpty storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(emptyRaw, &mEmpty))
	require.Zero(t, leasedWritePayloadCount())

	// 同包重复 field 4：先借用 4 MiB 池缓冲，后被同包空 bytes 字段覆盖时必须立即归还池缓冲
	dupZeroData := protowire.AppendTag(append([]byte(nil), raw...), 4, protowire.BytesType)
	dupZeroData = protowire.AppendBytes(dupZeroData, nil)
	var mDupZero storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(dupZeroData, &mDupZero))
	require.Empty(t, mDupZero.Data)
	require.Zero(t, leasedWritePayloadCount())

	// 同包重复 field 4：先借用池缓冲，后被 > 4 MiB 超上限字段覆盖时必须归还池缓冲并独立分配拷贝
	oversized := bytes.Repeat([]byte{0x7e}, maxReadWriteSize+1)
	dupOversized := protowire.AppendTag(append([]byte(nil), raw...), 4, protowire.BytesType)
	dupOversized = protowire.AppendBytes(dupOversized, oversized)
	var mOversized storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(dupOversized, &mOversized))
	require.Equal(t, oversized, mOversized.Data)
	require.Zero(t, leasedWritePayloadCount())

	// 末尾携带未知字段号（field 99）触发 fallback 回退标准解码且不残留池租约
	unknownTail := protowire.AppendTag(append([]byte(nil), raw...), 99, protowire.VarintType)
	unknownTail = protowire.AppendVarint(unknownTail, 12345)
	var mUnknown storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(unknownTail, &mUnknown))
	require.Equal(t, payload, mUnknown.Data)
	require.Zero(t, leasedWritePayloadCount())

	// 标量字段携带异构 wire type（field 1 附带 Fixed64）触发 fallback 归还池租约并按标准未知字段兼容解码
	mismatchWire := protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.Fixed64Type)
	mismatchWire = protowire.AppendFixed64(mismatchWire, 99)
	var mMismatch storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(mismatchWire, &mMismatch))
	require.Equal(t, payload, mMismatch.Data)
	require.Zero(t, leasedWritePayloadCount())

	// 4. 畸形二进制报文（借用池缓冲后遇截断 tag、非法 wire type 6、非法 UTF-8 disk_id、nil 指针）必须报错且零泄漏
	truncatedAfterData := append(append([]byte(nil), raw...), 0xff, 0xff, 0xff)
	var mTruncated storagev1.WriteRequest
	require.Error(t, codec.Unmarshal(truncatedAfterData, &mTruncated))
	require.Zero(t, leasedWritePayloadCount())

	illegalWireType := protowire.AppendTag(append([]byte(nil), raw...), 1, protowire.Type(6))
	var mBadWire storagev1.WriteRequest
	require.Error(t, codec.Unmarshal(illegalWireType, &mBadWire))
	require.Zero(t, leasedWritePayloadCount())

	badUTF8DiskID := protowire.AppendTag(append([]byte(nil), raw...), 5, protowire.BytesType)
	badUTF8DiskID = protowire.AppendBytes(badUTF8DiskID, []byte{0xff, 0xfe})
	var mBadUTF8 storagev1.WriteRequest
	require.Error(t, codec.Unmarshal(badUTF8DiskID, &mBadUTF8))
	require.Zero(t, leasedWritePayloadCount())

	var nilWriteReq *storagev1.WriteRequest
	require.Error(t, codec.Unmarshal(raw, nilWriteReq))
	require.Zero(t, leasedWritePayloadCount())

	// 5. 同一 WriteRequest 实例重复 Unmarshal 到空载荷或未知字段分支时立即释放旧租约
	var mReused storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(raw, &mReused))
	require.Equal(t, 1, leasedWritePayloadCount())
	require.NoError(t, codec.Unmarshal(emptyRaw, &mReused))
	require.Zero(t, leasedWritePayloadCount())

	require.NoError(t, codec.Unmarshal(raw, &mReused))
	require.Equal(t, 1, leasedWritePayloadCount())
	require.NoError(t, codec.Unmarshal(unknownTail, &mReused))
	require.Zero(t, leasedWritePayloadCount())
}

func TestWritePayloadReleaseOnValidationRejectAndOrphanedGC(t *testing.T) {
	codec := newProtoBinaryCodec()
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	chain := newWritePayloadReleaseInterceptor()(
		NewValidationInterceptor(v)(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			t.Fatal("非法请求必须在校验拦截器短路返回")
			return nil, nil
		}),
	)

	// 1. Epoch=0 的 1 MiB WriteRequest 被校验拦截时仍 100% 归还池缓冲
	rawInvalid, err := codec.Marshal(&storagev1.WriteRequest{
		Ino:    10,
		Data:   bytes.Repeat([]byte{0xab}, 1<<20),
		DiskId: "123e4567-e89b-12d3-a456-426614174000",
		Epoch:  0,
	})
	require.NoError(t, err)

	req := connect.NewRequest(&storagev1.WriteRequest{})
	require.NoError(t, codec.Unmarshal(rawInvalid, req.Msg))
	require.Equal(t, 1, leasedWritePayloadCount())

	_, callErr := chain(context.Background(), req)
	require.Error(t, callErr)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(callErr))
	require.Zero(t, leasedWritePayloadCount())
	require.Nil(t, req.Msg.Data)

	// 2. typed-nil WriteRequest / ReadRequest 进入热路径快检分支不得空指针崩溃
	_, nilWriteErr := chain(context.Background(), connect.NewRequest[storagev1.WriteRequest](nil))
	require.Error(t, nilWriteErr)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(nilWriteErr))

	_, nilReadErr := chain(context.Background(), connect.NewRequest[storagev1.ReadRequest](nil))
	require.Error(t, nilReadErr)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(nilReadErr))

	// 3. 进入拦截器前即被丢弃的孤儿 WriteRequest 通过 weak.Pointer + runtime.AddCleanup 自动回收池缓冲
	orphanRaw, err := codec.Marshal(&storagev1.WriteRequest{
		Ino:    10,
		Data:   bytes.Repeat([]byte{0x5a}, 64<<10),
		DiskId: "123e4567-e89b-12d3-a456-426614174000",
		Epoch:  1,
	})
	require.NoError(t, err)
	unmarshalAbandonedWriteRequest(t, codec, orphanRaw)

	require.Eventually(t, func() bool {
		runtime.GC()
		return leasedWritePayloadCount() == 0
	}, 2*time.Second, 10*time.Millisecond, "孤儿 WriteRequest 租约必须在 GC 后自动归零")
}

func registerAbandonedReadResponse(t *testing.T) {
	t.Helper()
	buf := acquireReadPayloadBuf()
	buf[0] = 0x99
	resp := &storagev1.ReadResponse{
		Data: buf[:4096:4096],
	}
	registerLeasedReadPayload(resp, buf)
	require.Equal(t, 1, leasedReadPayloadCount())
	runtime.KeepAlive(resp)
}

func marshalAndReleaseReadPayload(t *testing.T, codec *protoBinaryCodec, dst []byte) []byte {
	t.Helper()
	buf := acquireReadPayloadBuf()
	resp := &storagev1.ReadResponse{
		Data: buf[:maxReadWriteSize:maxReadWriteSize],
	}
	registerLeasedReadPayload(resp, buf)
	out, err := codec.MarshalAppend(dst[:0], resp)
	require.NoError(t, err)
	require.Nil(t, resp.Data)
	require.Zero(t, leasedReadPayloadCount())
	return out
}

func TestProtoBinaryCodecReadResponsePoolAndOrphanedGC(t *testing.T) {
	codec := &protoBinaryCodec{}
	payload := bytes.Repeat([]byte{0x41, 0x42, 0x43, 0x44}, maxReadWriteSize/4)

	// 1. Marshal 与 MarshalStable 编码完成后立即释放 4 MiB 池租约，且编码出的字节完整保真
	buf1 := acquireReadPayloadBuf()
	copy(buf1[:], payload)
	resp1 := &storagev1.ReadResponse{Data: buf1[:maxReadWriteSize:maxReadWriteSize]}
	registerLeasedReadPayload(resp1, buf1)
	require.Equal(t, 1, leasedReadPayloadCount())

	wireBytes, err := codec.Marshal(resp1)
	require.NoError(t, err)
	require.Nil(t, resp1.Data)
	require.Zero(t, leasedReadPayloadCount())

	var checkResp storagev1.ReadResponse
	require.NoError(t, proto.Unmarshal(wireBytes, &checkResp))
	require.Equal(t, payload, checkResp.Data)

	buf2 := acquireReadPayloadBuf()
	copy(buf2[:4096], payload[:4096])
	resp2 := &storagev1.ReadResponse{Data: buf2[:4096:4096]}
	registerLeasedReadPayload(resp2, buf2)
	_, err = codec.MarshalStable(resp2)
	require.NoError(t, err)
	require.Zero(t, leasedReadPayloadCount())

	// 2. 预热池与目标序列化帧缓冲后，连续 8 次 4 MiB ReadResponse 借池 + MarshalAppend 堆分配增量严格 < 64 KiB
	frameDst := make([]byte, 0, maxReadWriteSize+64)
	frameDst = marshalAndReleaseReadPayload(t, codec, frameDst)

	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	for range 8 {
		frameDst = marshalAndReleaseReadPayload(t, codec, frameDst)
	}
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10))
	require.Zero(t, leasedReadPayloadCount())

	// 3. 未进入 Marshal 即被丢弃的孤儿 ReadResponse 在 GC 后通过 runtime.AddCleanup 自动回收
	registerAbandonedReadResponse(t)
	require.Eventually(t, func() bool {
		runtime.GC()
		return leasedReadPayloadCount() == 0
	}, 2*time.Second, 10*time.Millisecond, "孤儿 ReadResponse 租约必须在 GC 后自动归零")
}
