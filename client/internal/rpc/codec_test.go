package rpc

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"
	"time"
	"unsafe"

	"connectrpc.com/connect"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	otelprom "go.opentelemetry.io/otel/exporters/prometheus"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

func TestProtoBinaryCodec_ReadResponsePooledReuseAndZeroUAF(t *testing.T) {
	codec := newProtoBinaryCodec()
	src := bytes.Repeat([]byte{0x5A, 0xA5, 0x3C, 0xC3}, MaxReadPayloadSize/4)
	raw, err := proto.Marshal(&storagev1.ReadResponse{Data: src})
	require.NoError(t, err)

	// 1. 显式接管并归还后，下一次 4 MiB 解码直接复用同一底层数组
	msg1 := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(raw, msg1))
	require.Equal(t, 1, leasedReadPayloadCount())

	p1 := TakeReadPayload(msg1)
	require.Equal(t, 0, leasedReadPayloadCount())
	require.Nil(t, msg1.Data)
	require.True(t, bytes.Equal(src, p1.Data))
	firstPtr := unsafe.SliceData(p1.Data)
	p1.Release()
	p1.Release() // 幂等

	msg2 := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(raw, msg2))
	p2 := TakeReadPayload(msg2)
	secondPtr := unsafe.SliceData(p2.Data)
	require.Equal(t, firstPtr, secondPtr, "释放后的 4 MiB 池化缓冲应被确定性复用")
	p2.Release()

	// 2. 未调用 TakeReadPayload 但外部保留 msg.GetData() 时（>256 KiB 池化路径），GC 后不得发生 UAF 污染且弱引用表项应自动清理
	pooledRaw, err := proto.Marshal(&storagev1.ReadResponse{Data: src[:SmallReadPayloadCutoff+1024]})
	require.NoError(t, err)
	holdUnclaimedSlice := func() []byte {
		unclaimed := &storagev1.ReadResponse{}
		require.NoError(t, codec.Unmarshal(pooledRaw, unclaimed))
		require.Equal(t, 1, leasedReadPayloadCount())
		return unclaimed.GetData()
	}
	retained := holdUnclaimedSlice()
	expectedRetained := append([]byte(nil), retained...)
	for range 4 {
		runtime.GC()
		time.Sleep(5 * time.Millisecond)
	}
	require.Eventually(t, func() bool {
		runtime.GC()
		return leasedReadPayloadCount() == 0
	}, 2*time.Second, 10*time.Millisecond)
	require.Equal(t, expectedRetained, retained, "未接管的切片在 msg GC 后不得被池重用覆盖")

	// 3. <= 256 KiB 小载荷（覆盖 4 KiB 随机小读与 128 KiB 小文件读）直接克隆返回，不进 4 MiB 租约池，TakeReadPayload/Release 安全无副作用
	for _, sz := range []int{0, 4 << 10, 128 << 10, SmallReadPayloadCutoff} {
		smallRaw, mErr := proto.Marshal(&storagev1.ReadResponse{Data: src[:sz]})
		require.NoError(t, mErr)
		smallMsg := &storagev1.ReadResponse{}
		require.NoError(t, codec.Unmarshal(smallRaw, smallMsg))
		require.Equal(t, 0, leasedReadPayloadCount(), "小载荷 (%d B) 不得注册 4 MiB 租约", sz)

		pSmall := TakeReadPayload(smallMsg)
		require.Nil(t, smallMsg.Data)
		require.True(t, bytes.Equal(src[:sz], pSmall.Data))
		if sz > 0 {
			require.LessOrEqual(t, cap(pSmall.Data), SmallReadPayloadCutoff+64, "小载荷底层切片不得占用 4 MiB 大数组")
		}
		pSmall.Release()
		pSmall.Release()
		require.Nil(t, pSmall.Data)
	}

	// 4. 未知字段回退到标准 proto.Unmarshal 不报错且保留数据
	var rawWithUnknown []byte
	rawWithUnknown = protowire.AppendTag(rawWithUnknown, 1, protowire.BytesType)
	rawWithUnknown = protowire.AppendBytes(rawWithUnknown, []byte("hello-bowl"))
	rawWithUnknown = protowire.AppendTag(rawWithUnknown, 99, protowire.VarintType)
	rawWithUnknown = protowire.AppendVarint(rawWithUnknown, 42)

	msgFallback := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(rawWithUnknown, msgFallback))
	require.Equal(t, []byte("hello-bowl"), msgFallback.GetData())
}

func TestProtoBinaryCodec_WriteRequestMarshalAppendEquivalenceAndZeroAlloc(t *testing.T) {
	codec := newProtoBinaryCodec()
	appender, ok := codec.(interface {
		MarshalAppend(dst []byte, msg any) ([]byte, error)
	})
	require.True(t, ok, "protoBinaryCodec 必须实现 MarshalAppend 以复用 Connect bufferPool")

	payload4M := bytes.Repeat([]byte{0x19, 0x84, 0xAB, 0xCD}, MaxReadPayloadSize/4)
	cases := []*storagev1.WriteRequest{
		{},
		{
			Ino:    1,
			Fh:     0,
			Offset: 0,
			Data:   []byte("small-write"),
			DiskId: "11111111-1111-4111-8111-111111111111",
			Epoch:  1,
		},
		{
			Ino:    9876543210,
			Fh:     42,
			Offset: 1099511627776,
			Data:   payload4M,
			DiskId: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
			Epoch:  777,
		},
	}

	for i, tc := range cases {
		stdWire, err := proto.Marshal(tc)
		require.NoError(t, err)

		gotMarshal, err := codec.Marshal(tc)
		require.NoError(t, err)
		require.True(t, bytes.Equal(stdWire, gotMarshal), "case %d Marshal 编码必须与标准 proto.Marshal 逐字节一致", i)

		prefix := []byte("prefix-hdr:")
		gotAppend, err := appender.MarshalAppend(append([]byte(nil), prefix...), tc)
		require.NoError(t, err)
		require.Equal(t, prefix, gotAppend[:len(prefix)])
		require.True(t, bytes.Equal(stdWire, gotAppend[len(prefix):]), "case %d MarshalAppend 追加结果必须与标准 proto.Marshal 逐字节一致", i)

		roundTrip := &storagev1.WriteRequest{}
		require.NoError(t, proto.Unmarshal(gotMarshal, roundTrip))
		require.True(t, proto.Equal(tc, roundTrip), "case %d 反序列化后全字段（Ino/Fh/Offset/Data/DiskId/Epoch）必须完全相等", i)
	}

	// 非法 UTF-8 DiskId 回退至标准库返回错误
	_, invalidErr := appender.MarshalAppend(nil, &storagev1.WriteRequest{
		Ino:    1,
		DiskId: string([]byte{0xff, 0xfe}),
		Epoch:  1,
	})
	require.Error(t, invalidErr)

	// 预热池容量充足时，小载荷与 4 MiB 大载荷 WriteRequest 以及常规 Protobuf 请求 MarshalAppend 均达成 0 allocs/op
	smallReq := cases[1]
	bigReq := cases[2]
	readReq := &storagev1.ReadRequest{
		Ino:    42,
		Fh:     7,
		Offset: 4096,
		Size:   128 << 10,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}
	reusableBuf := make([]byte, 0, MaxReadPayloadSize+256)
	require.Zero(t, testing.AllocsPerRun(20, func() {
		out, mErr := appender.MarshalAppend(reusableBuf[:0], smallReq)
		if mErr != nil || len(out) == 0 {
			t.Fatalf("unexpected small marshal result: len=%d err=%v", len(out), mErr)
		}
	}), "容量充足时小载荷 WriteRequest MarshalAppend 必须达成 0 allocs/op")
	require.Zero(t, testing.AllocsPerRun(20, func() {
		out, mErr := appender.MarshalAppend(reusableBuf[:0], bigReq)
		if mErr != nil || len(out) <= MaxReadPayloadSize {
			t.Fatalf("unexpected big marshal result: len=%d err=%v", len(out), mErr)
		}
	}), "容量充足时 4 MiB WriteRequest MarshalAppend 必须达成 0 allocs/op")
	require.Zero(t, testing.AllocsPerRun(20, func() {
		out, mErr := appender.MarshalAppend(reusableBuf[:0], readReq)
		if mErr != nil || len(out) == 0 {
			t.Fatalf("unexpected readReq marshal result: len=%d err=%v", len(out), mErr)
		}
	}), "容量充足时常规 Protobuf 请求 MarshalAppend 必须达成 0 allocs/op")
}

type readTestHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler
	failsLeft int
	payload   []byte
}

func (h *readTestHandler) Read(_ context.Context, _ *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	if h.failsLeft > 0 {
		h.failsLeft--
		return nil, connect.NewError(connect.CodeUnavailable, context.DeadlineExceeded)
	}
	return connect.NewResponse(&storagev1.ReadResponse{Data: h.payload}), nil
}

func TestClientSubMillisecondBucketsAndLogicalDuration(t *testing.T) {
	reg := prometheus.NewRegistry()
	promExp, err := otelprom.New(otelprom.WithRegisterer(reg), otelprom.WithoutScopeInfo())
	require.NoError(t, err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(promExp))
	prevMP := otel.GetMeterProvider()
	otel.SetMeterProvider(mp)
	t.Cleanup(func() {
		otel.SetMeterProvider(prevMP)
		_ = mp.Shutdown(context.Background())
	})

	handler := &readTestHandler{
		failsLeft: 1,
		payload:   []byte("sub-ms-check"),
	}
	_, mux := storagev1connect.NewStorageServiceHandler(handler)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	rpcMetrics := NewMetrics()
	require.NoError(t, reg.Register(rpcMetrics))

	client, err := NewClientFromConfig(ClientConfig{
		Endpoint:   srv.URL,
		HTTPClient: &http.Client{},
		Metrics:    rpcMetrics,
		RetryConfig: &RetryConfig{
			InitialInterval: 5 * time.Millisecond,
			MaxInterval:     10 * time.Millisecond,
			MaxElapsedTime:  time.Second,
			RandFloat64:     func() float64 { return 1.0 },
		},
	})
	require.NoError(t, err)

	resp, err := client.Read(context.Background(), connect.NewRequest(&storagev1.ReadRequest{
		Ino:    1,
		Offset: 0,
		Size:   12,
		DiskId: "11111111-1111-4111-8111-111111111111",
	}))
	require.NoError(t, err)
	payload := TakeReadPayload(resp.Msg)
	require.Equal(t, []byte("sub-ms-check"), payload.Data)
	payload.Release()

	mfs, err := reg.Gather()
	require.NoError(t, err)

	var (
		foundLogical  *dto.MetricFamily
		foundCallHist *dto.MetricFamily
	)
	for _, mf := range mfs {
		switch mf.GetName() {
		case "bowl_client_rpc_logical_duration_seconds":
			foundLogical = mf
		case "rpc_client_call_duration_seconds":
			foundCallHist = mf
		}
	}

	require.NotNil(t, foundLogical, "必须导出 bowl_client_rpc_logical_duration_seconds")
	require.Len(t, foundLogical.GetMetric(), 1)
	logicalHist := foundLogical.GetMetric()[0].GetHistogram()
	require.Equal(t, uint64(1), logicalHist.GetSampleCount())
	require.Len(t, logicalHist.GetBucket(), len(SubMillisecondDurationBuckets))
	require.InDelta(t, 0.000025, logicalHist.GetBucket()[0].GetUpperBound(), 1e-9)

	require.NotNil(t, foundCallHist, "必须导出 rpc_client_call_duration_seconds")
	var totalAttempts uint64
	for _, m := range foundCallHist.GetMetric() {
		h := m.GetHistogram()
		totalAttempts += h.GetSampleCount()
		require.Len(t, h.GetBucket(), len(SubMillisecondDurationBuckets))
		require.InDelta(t, 0.000025, h.GetBucket()[0].GetUpperBound(), 1e-9)
	}
	require.Equal(t, uint64(2), totalAttempts, "otelconnect 应记录 2 次网络尝试，逻辑直方图记录 1 次总调用")
}
