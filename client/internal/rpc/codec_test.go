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

	// 2. 未调用 TakeReadPayload 但外部保留 msg.GetData() 时，GC 后不得发生 UAF 污染且弱引用表项应自动清理
	smallRaw, err := proto.Marshal(&storagev1.ReadResponse{Data: src[:1024]})
	require.NoError(t, err)
	holdUnclaimedSlice := func() []byte {
		unclaimed := &storagev1.ReadResponse{}
		require.NoError(t, codec.Unmarshal(smallRaw, unclaimed))
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

	// 3. 未知字段回退到标准 proto.Unmarshal 不报错且保留数据
	var rawWithUnknown []byte
	rawWithUnknown = protowire.AppendTag(rawWithUnknown, 1, protowire.BytesType)
	rawWithUnknown = protowire.AppendBytes(rawWithUnknown, []byte("hello-bowl"))
	rawWithUnknown = protowire.AppendTag(rawWithUnknown, 99, protowire.VarintType)
	rawWithUnknown = protowire.AppendVarint(rawWithUnknown, 42)

	msgFallback := &storagev1.ReadResponse{}
	require.NoError(t, codec.Unmarshal(rawWithUnknown, msgFallback))
	require.Equal(t, []byte("hello-bowl"), msgFallback.GetData())
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
