package server

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
	"github.com/starsrc/bowl/pkg/validator"
)

func TestProtoBinaryCodecWriteRequestPoolReuseAndFallback(t *testing.T) {
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

	// 1. 正常 4 MiB 解包与池归还，覆盖底层网络缓冲被立即篡改也不影响解出的 msg.Data（非零拷贝悬空引用）
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

	// 2. 预热池后重复解包 16 次 4 MiB 请求（无池化时需分配 64 MiB）：
	// 常驻有界 noscan 缓冲池免受 GC 驱逐与 -race 随机丢弃，总堆分配增量必须严格 < 64 KiB。
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	const measureRounds = 16
	for range measureRounds {
		var m storagev1.WriteRequest
		require.NoError(t, codec.Unmarshal(raw, &m))
		releaseWritePayload(&m)
	}
	runtime.ReadMemStats(&after)
	require.Less(t, after.TotalAlloc-before.TotalAlloc, uint64(64<<10))
	require.Zero(t, leasedWritePayloadCount())

	// 3. 空 Data 与未知字段回退不得残留池租约
	emptyRaw, err := proto.Marshal(&storagev1.WriteRequest{
		Ino:    9,
		DiskId: "123e4567-e89b-12d3-a456-426614174000",
		Epoch:  1,
	})
	require.NoError(t, err)
	unknownTail := protowire.AppendTag(append([]byte(nil), raw...), 99, protowire.VarintType)
	unknownTail = protowire.AppendVarint(unknownTail, 12345)

	var mEmpty, mUnknown storagev1.WriteRequest
	require.NoError(t, codec.Unmarshal(emptyRaw, &mEmpty))
	require.Zero(t, leasedWritePayloadCount())
	require.NoError(t, codec.Unmarshal(unknownTail, &mUnknown))
	require.Equal(t, payload, mUnknown.Data)
	require.Zero(t, leasedWritePayloadCount())

	// 4. 损坏的二进制报文必须报错且不泄漏租约
	var mBad storagev1.WriteRequest
	require.Error(t, codec.Unmarshal([]byte{0xff, 0xff, 0xff}, &mBad))
	require.Zero(t, leasedWritePayloadCount())

	// 5. 同一 WriteRequest 实例重复 Unmarshal 到空载荷或回退分支时必须立即归还旧租约
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

func TestWritePayloadReleaseInterceptorReleasesOnValidationRejectAndPanic(t *testing.T) {
	codec := newProtoBinaryCodec()
	v, err := validator.NewStorageValidator()
	require.NoError(t, err)

	chain := newWritePayloadReleaseInterceptor()(
		NewValidationInterceptor(v)(func(_ context.Context, _ connect.AnyRequest) (connect.AnyResponse, error) {
			return nil, errors.New("should not reach handler on invalid epoch")
		}),
	)

	// Epoch=0 的非法 1 MiB WriteRequest：在校验拦截器即被拦截，仍必须 100% 归还池缓冲
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

	// typed-nil WriteRequest / ReadRequest 进入快速校验分支不得触发空指针 panic
	_, nilWriteErr := chain(context.Background(), connect.NewRequest[storagev1.WriteRequest](nil))
	require.Error(t, nilWriteErr)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(nilWriteErr))

	_, nilReadErr := chain(context.Background(), connect.NewRequest[storagev1.ReadRequest](nil))
	require.Error(t, nilReadErr)
	require.Equal(t, connect.CodeInvalidArgument, connect.CodeOf(nilReadErr))
}

func TestWritePayloadNoPermanentLeakWhenAbandonedBeforeInterceptor(t *testing.T) {
	_, handler := storagev1connect.NewStorageServiceHandler(
		storagev1connect.UnimplementedStorageServiceHandler{},
		connect.WithCodec(newProtoBinaryCodec()),
		connect.WithInterceptors(newWritePayloadReleaseInterceptor()),
	)
	ts := httptest.NewServer(handler)
	t.Cleanup(ts.Close)

	msgBytes, err := proto.Marshal(&storagev1.WriteRequest{
		Ino:    10,
		Data:   bytes.Repeat([]byte{0x5a}, 64<<10),
		DiskId: "123e4567-e89b-12d3-a456-426614174000",
		Epoch:  1,
	})
	require.NoError(t, err)

	// 构造包含 2 个消息帧的非法 gRPC Unary 请求体，触发 ConnectRPC receiveUnaryMessage
	// 在进入 UnaryInterceptor 之前直接以 cardinality violation 中止返回。
	frame := make([]byte, 5+len(msgBytes))
	frame[0] = 0
	binary.BigEndian.PutUint32(frame[1:5], uint32(len(msgBytes)))
	copy(frame[5:], msgBytes)
	twoFrames := append(append([]byte(nil), frame...), frame...)

	httpReq, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		ts.URL+storagev1connect.StorageServiceWriteProcedure,
		bytes.NewReader(twoFrames),
	)
	require.NoError(t, err)
	httpReq.Header.Set("Content-Type", "application/grpc+proto")

	resp, err := ts.Client().Do(httpReq)
	require.NoError(t, err)
	_ = resp.Body.Close()

	require.Eventually(t, func() bool {
		runtime.GC()
		return leasedWritePayloadCount() == 0
	}, 2*time.Second, 10*time.Millisecond, "进入拦截器前中止的请求不得在全局租约表中产生永久内存泄漏")
}
