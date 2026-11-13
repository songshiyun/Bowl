package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	testSourceDiskID = "01950000-0000-7000-8000-000000000001"
	testChildDiskID  = "01950000-0000-7000-8000-000000000002"
	testNilDiskID    = "00000000-0000-0000-0000-000000000000"
)

type mockDiskForkHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu              sync.Mutex
	lastReq         *storagev1.ForkDiskRequest
	lastAuth        string
	lastTraceparent string
	lastRequestID   string
	forkFunc        func(ctx context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error)
	getDiskFn       func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error)
}

func (m *mockDiskForkHandler) ForkDisk(ctx context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	m.mu.Lock()
	m.lastReq = req.Msg
	m.lastAuth = req.Header().Get("Authorization")
	m.lastTraceparent = req.Header().Get("Traceparent")
	m.lastRequestID = req.Header().Get("X-Request-Id")
	fn := m.forkFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	parentID := req.Msg.GetSourceDiskId()
	maxBytes := uint64(1 << 30)
	if req.Msg.MaxBytes != nil {
		maxBytes = req.Msg.GetMaxBytes()
	}
	return connect.NewResponse(&storagev1.ForkDiskResponse{
		Disk: &storagev1.Disk{
			DiskId:       testChildDiskID,
			Name:         req.Msg.GetName(),
			RootIno:      1,
			MaxBytes:     maxBytes,
			UsedBytes:    4096,
			UsedInodes:   2,
			CreatedAt:    now,
			UpdatedAt:    now,
			ParentDiskId: &parentID,
		},
	}), nil
}

func (m *mockDiskForkHandler) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	if m.getDiskFn != nil {
		return m.getDiskFn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func startMockDiskH2CServer(t *testing.T, h storagev1connect.StorageServiceHandler) string {
	t.Helper()
	path, connectHandler := storagev1connect.NewStorageServiceHandler(h)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)

	var protocols http.Protocols
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler:   mux,
		Protocols: &protocols,
	}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	return "http://" + ln.Addr().String()
}

func runCLI(args ...string) (exitCode int, stdout, stderr string) {
	var outBuf, errBuf bytes.Buffer
	code := Execute(args, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

func TestDiskForkHelpAndRouting(t *testing.T) {
	code, out, errOut := runCLI("disk", "--help")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "fork")

	for _, flag := range []string{"-h", "--help"} {
		code, out, errOut = runCLI("disk", "fork", flag)
		require.Equal(t, 0, code)
		require.Empty(t, errOut)
		require.Contains(t, out, "bowl disk fork <source-disk-id>")
		require.Contains(t, out, "--size")
		require.Contains(t, out, "--name")
		require.Contains(t, out, "--quiet")
		require.Contains(t, out, "--json")
	}
}

func TestDiskForkValidationErrors(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		errSubstr string
	}{
		{
			name:      "缺少位置参数",
			args:      []string{"disk", "fork"},
			errSubstr: "需要且仅需要 1 个位置参数",
		},
		{
			name:      "多余位置参数",
			args:      []string{"disk", "fork", testSourceDiskID, "extra"},
			errSubstr: "需要且仅需要 1 个位置参数",
		},
		{
			name:      "非法UUID",
			args:      []string{"disk", "fork", "not-a-uuid"},
			errSubstr: "无效的 source-disk-id",
		},
		{
			name:      "全零NilUUID",
			args:      []string{"disk", "fork", testNilDiskID},
			errSubstr: "无效的 source-disk-id",
		},
		{
			name:      "未知标志",
			args:      []string{"disk", "fork", testSourceDiskID, "--unknown"},
			errSubstr: "未知标志",
		},
		{
			name:      "size缺少参数值",
			args:      []string{"disk", "fork", testSourceDiskID, "--size"},
			errSubstr: "--size 需要指定参数值",
		},
		{
			name:      "name缺少参数值",
			args:      []string{"disk", "fork", testSourceDiskID, "--name"},
			errSubstr: "--name 需要指定参数值",
		},
		{
			name:      "endpoint缺少参数值",
			args:      []string{"disk", "fork", testSourceDiskID, "--endpoint"},
			errSubstr: "--endpoint 需要指定参数值",
		},
		{
			name:      "token缺少参数值",
			args:      []string{"disk", "fork", testSourceDiskID, "--token"},
			errSubstr: "--token 需要指定参数值",
		},
		{
			name:      "quiet与json互斥",
			args:      []string{"disk", "fork", testSourceDiskID, "-q", "--json"},
			errSubstr: "不能同时使用",
		},
		{
			name:      "size为空字符串",
			args:      []string{"disk", "fork", testSourceDiskID, "--size="},
			errSubstr: "无效的 --size 参数",
		},
		{
			name:      "size为零",
			args:      []string{"disk", "fork", testSourceDiskID, "--size", "0"},
			errSubstr: "无效的 --size 参数",
		},
		{
			name:      "name超长256字节",
			args:      []string{"disk", "fork", testSourceDiskID, "--name", strings.Repeat("n", 256)},
			errSubstr: "不能超过 255 字节",
		},
		{
			name:      "name包含NUL",
			args:      []string{"disk", "fork", testSourceDiskID, "--name", "bad\x00name"},
			errSubstr: "不能包含空字节",
		},
		{
			name:      "无效endpoint协议",
			args:      []string{"disk", "fork", testSourceDiskID, "--endpoint", "ftp://127.0.0.1:9000"},
			errSubstr: "无效的 endpoint",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, errOut := runCLI(tc.args...)
			require.Equal(t, 2, code)
			require.Empty(t, out)
			require.Contains(t, errOut, tc.errSubstr)
		})
	}
}

func TestDiskForkOutputModesAndParentDiskDisplay(t *testing.T) {
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	handler := &mockDiskForkHandler{
		getDiskFn: func(_ context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:     req.Msg.GetDiskId(),
					Name:       "root-disk",
					RootIno:    1,
					MaxBytes:   1 << 30,
					UsedBytes:  0,
					UsedInodes: 1,
					CreatedAt:  now,
					UpdatedAt:  now,
				},
			}), nil
		},
	}
	endpoint := startMockDiskH2CServer(t, handler)

	// 1. 默认详情输出（省略 --size，验证 MaxBytes 为 nil 且紧接 Disk ID 打印 Parent Disk）
	code, out, errOut := runCLI("disk", "fork", testSourceDiskID, "--name", "branch-exp", "--endpoint", endpoint, "--token", "sec-tok")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotNil(t, handler.lastReq)
	require.Equal(t, testSourceDiskID, handler.lastReq.GetSourceDiskId())
	require.Equal(t, "branch-exp", handler.lastReq.GetName())
	require.Nil(t, handler.lastReq.MaxBytes)
	require.Equal(t, "Bearer sec-tok", handler.lastAuth)

	expectedHeader := "Disk ID:      " + testChildDiskID + "\nParent Disk:  " + testSourceDiskID + "\nName:         branch-exp\n"
	require.True(t, strings.HasPrefix(out, expectedHeader), "实际输出:\n%s", out)

	// 2. --json 输出模式（显式传入 --size=2GiB）
	code, out, errOut = runCLI("disk", "fork", "--name=json-branch", "--size=2GiB", "--json", "--endpoint="+endpoint, "--token=tok2", "--", testSourceDiskID)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotNil(t, handler.lastReq.MaxBytes)
	require.Equal(t, uint64(2<<30), handler.lastReq.GetMaxBytes())

	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &dj))
	require.Equal(t, testChildDiskID, dj.DiskID)
	require.Equal(t, testSourceDiskID, dj.ParentDiskID)
	require.Equal(t, "json-branch", dj.Name)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)

	// 3. -q / --quiet 静默模式
	code, out, errOut = runCLI("disk", "fork", testSourceDiskID, "-q", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, testChildDiskID+"\n", out)

	// 4. 根磁盘（无 parent_disk_id）在详情与 --json 下均不展示父盘字段
	code, out, errOut = runCLI("disk", "get", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotContains(t, out, "Parent Disk:")

	code, out, errOut = runCLI("disk", "get", testSourceDiskID, "--json", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotContains(t, out, "parent_disk_id")
}

func TestDiskForkServerErrors(t *testing.T) {
	var mode string
	var mu sync.Mutex
	handler := &mockDiskForkHandler{
		forkFunc: func(_ context.Context, _ *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
			mu.Lock()
			m := mode
			mu.Unlock()
			switch m {
			case "not_found":
				return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
			case "shrink":
				return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("quota shrink below used"))
			case "empty_disk":
				return connect.NewResponse(&storagev1.ForkDiskResponse{}), nil
			default:
				return nil, connect.NewError(connect.CodeInternal, errors.New("internal boom"))
			}
		},
	}
	endpoint := startMockDiskH2CServer(t, handler)

	setMode := func(m string) {
		mu.Lock()
		mode = m
		mu.Unlock()
	}

	setMode("not_found")
	code, out, errOut := runCLI("disk", "fork", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "磁盘不存在: "+testSourceDiskID)

	setMode("shrink")
	code, out, errOut = runCLI("disk", "fork", testSourceDiskID, "--size", "1MiB", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "目标容量小于源磁盘已用容量")

	setMode("empty_disk")
	code, out, errOut = runCLI("disk", "fork", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "空的虚拟磁盘元数据")

	setMode("internal")
	code, out, errOut = runCLI("disk", "fork", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "派生虚拟磁盘失败")
}

func TestDiskForkCLISpanAndW3CTraceparentPropagation(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	prevProp := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(prevTP)
		otel.SetTextMapPropagator(prevProp)
	})

	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { _ = tp.Shutdown(context.Background()) })

	var failNext bool
	handler := &mockDiskForkHandler{
		forkFunc: func(_ context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
			if failNext {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("source not found"))
			}
			now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
			parentID := req.Msg.GetSourceDiskId()
			return connect.NewResponse(&storagev1.ForkDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:       testChildDiskID,
					Name:         req.Msg.GetName(),
					RootIno:      1,
					MaxBytes:     1 << 30,
					CreatedAt:    now,
					UpdatedAt:    now,
					ParentDiskId: &parentID,
				},
			}), nil
		},
	}
	endpoint := startMockDiskH2CServer(t, handler)

	// 1. 成功路径：cli.disk.fork 根 Span 与出站 Traceparent 共享 TraceID，且 X-Request-Id 非 "1"。
	code, _, errOut := runCLI("disk", "fork", testSourceDiskID, "--name", "traced-fork", "-q", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)

	handler.mu.Lock()
	okTraceparent := handler.lastTraceparent
	okReqID := handler.lastRequestID
	handler.mu.Unlock()
	require.NotEmpty(t, okReqID)
	require.NotEqual(t, "1", okReqID)
	require.Contains(t, okTraceparent, "00-")

	spans := exporter.GetSpans()
	var cliForkSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "cli.disk.fork" {
			cliForkSpan = &spans[i]
		}
	}
	require.NotNil(t, cliForkSpan, "必须产生客户端根 Span cli.disk.fork")
	require.Contains(t, okTraceparent, cliForkSpan.SpanContext.TraceID().String(), "请求头 Traceparent 必须携带 cli.disk.fork TraceID")

	attrMap := make(map[string]string)
	for _, a := range cliForkSpan.Attributes {
		attrMap[string(a.Key)] = a.Value.AsString()
	}
	require.Equal(t, testSourceDiskID, attrMap["bowl.source_disk_id"])
	require.Equal(t, testChildDiskID, attrMap["bowl.disk_id"])

	// 2. 失败路径：cli.disk.fork 标记 Error 状态。
	exporter.Reset()
	failNext = true
	code, _, errOut = runCLI("disk", "fork", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在")

	spans = exporter.GetSpans()
	var errSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "cli.disk.fork" {
			errSpan = &spans[i]
		}
	}
	require.NotNil(t, errSpan)
	require.Equal(t, codes.Error, errSpan.Status.Code)
}
