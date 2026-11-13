package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
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
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	testSourceDiskID = "01950000-0000-7000-8000-000000000001"
	testChildDiskID  = "01950000-0000-7000-8000-000000000002"
	testNilDiskID    = "00000000-0000-0000-0000-000000000000"
)

type mockDiskHandler struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu              sync.Mutex
	lastCreateReq   *storagev1.CreateDiskRequest
	lastForkReq     *storagev1.ForkDiskRequest
	lastListReq     *storagev1.ListDisksRequest
	lastUpdateReq   *storagev1.UpdateDiskRequest
	lastDeleteReq   *storagev1.DeleteDiskRequest
	lastAuth        string
	lastTraceparent string
	lastRequestID   string
	deleteCalls     int

	createFn func(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error)
	forkFn   func(ctx context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error)
	getFn    func(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error)
	listFn   func(ctx context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error)
	updateFn func(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error)
	deleteFn func(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error)
}

func (m *mockDiskHandler) recordHeaders(header http.Header) {
	m.lastAuth = header.Get("Authorization")
	m.lastTraceparent = header.Get("Traceparent")
	m.lastRequestID = header.Get("X-Request-Id")
}

func (m *mockDiskHandler) CreateDisk(ctx context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	m.mu.Lock()
	m.lastCreateReq = req.Msg
	m.recordHeaders(req.Header())
	fn := m.createFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	return connect.NewResponse(&storagev1.CreateDiskResponse{
		Disk: &storagev1.Disk{
			DiskId:     testSourceDiskID,
			Name:       req.Msg.GetName(),
			RootIno:    1,
			MaxBytes:   req.Msg.GetMaxBytes(),
			UsedBytes:  0,
			UsedInodes: 1,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}), nil
}

func (m *mockDiskHandler) ForkDisk(ctx context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	m.mu.Lock()
	m.lastForkReq = req.Msg
	m.recordHeaders(req.Header())
	fn := m.forkFn
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

func (m *mockDiskHandler) GetDisk(ctx context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	m.mu.Lock()
	m.recordHeaders(req.Header())
	fn := m.getFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("unimplemented"))
}

func (m *mockDiskHandler) ListDisks(ctx context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	m.mu.Lock()
	m.lastListReq = req.Msg
	m.recordHeaders(req.Header())
	fn := m.listFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	return connect.NewResponse(&storagev1.ListDisksResponse{
		Disks: []*storagev1.Disk{
			{
				DiskId:     testSourceDiskID,
				Name:       "root-disk",
				RootIno:    1,
				MaxBytes:   1 << 30,
				UsedBytes:  512 << 20,
				UsedInodes: 10,
				CreatedAt:  now,
				UpdatedAt:  now,
			},
		},
	}), nil
}

func (m *mockDiskHandler) UpdateDisk(ctx context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	m.mu.Lock()
	m.lastUpdateReq = req.Msg
	m.recordHeaders(req.Header())
	fn := m.updateFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	name := "updated-disk"
	if req.Msg.Name != nil {
		name = req.Msg.GetName()
	}
	maxBytes := uint64(2 << 30)
	if req.Msg.MaxBytes != nil {
		maxBytes = req.Msg.GetMaxBytes()
	}
	return connect.NewResponse(&storagev1.UpdateDiskResponse{
		Disk: &storagev1.Disk{
			DiskId:     req.Msg.GetDiskId(),
			Name:       name,
			RootIno:    1,
			MaxBytes:   maxBytes,
			UsedBytes:  4096,
			UsedInodes: 2,
			CreatedAt:  now,
			UpdatedAt:  now,
		},
	}), nil
}

func (m *mockDiskHandler) DeleteDisk(ctx context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	m.mu.Lock()
	m.lastDeleteReq = req.Msg
	m.deleteCalls++
	m.recordHeaders(req.Header())
	fn := m.deleteFn
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, req)
	}
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
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

func runDiskWithPorts(ports diskPorts, args ...string) (exitCode int, stdout, stderr string) {
	var outBuf, errBuf bytes.Buffer
	code := executeDiskWithPorts(args, &outBuf, &errBuf, ports)
	return code, outBuf.String(), errBuf.String()
}

func TestDiskHelpAndRouting(t *testing.T) {
	code, out, errOut := runCLI("disk")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl disk <subcommand>")

	code, out, errOut = runCLI("disk", "unknown")
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "未知 disk 子命令 \"unknown\"")

	for _, helpArg := range []string{"help", "-h", "--help"} {
		code, out, errOut = runCLI("disk", helpArg)
		require.Equal(t, 0, code)
		require.Empty(t, errOut)
		for _, sub := range []string{"create", "fork", "list", "get", "update", "resize", "delete"} {
			require.Contains(t, out, sub)
		}
	}

	subHelpChecks := []struct {
		subcmd   string
		contains []string
	}{
		{"create", []string{"bowl disk create --size <size>", "--quiet", "--json"}},
		{"fork", []string{"bowl disk fork <source-disk-id>", "--size", "--name", "--quiet", "--json"}},
		{"list", []string{"bowl disk list [flags]", "--limit", "--page-token", "--json"}},
		{"get", []string{"bowl disk get <disk-id>", "--json"}},
		{"update", []string{"bowl disk update <disk-id>", "bowl disk resize <disk-id>", "--size", "--name"}},
		{"resize", []string{"bowl disk update <disk-id>", "bowl disk resize <disk-id>", "--size", "--name"}},
		{"delete", []string{"bowl disk delete <disk-id>", "--yes", "--json"}},
	}

	for _, sc := range subHelpChecks {
		for _, flag := range []string{"-h", "--help"} {
			code, out, errOut = runCLI("disk", sc.subcmd, flag)
			require.Equal(t, 0, code)
			require.Empty(t, errOut)
			for _, substr := range sc.contains {
				require.Contains(t, out, substr)
			}
		}
	}
}

func TestDiskValidationErrors(t *testing.T) {
	cases := []struct {
		name      string
		args      []string
		errSubstr string
	}{
		// fork
		{"fork缺少位置参数", []string{"disk", "fork"}, "需要且仅需要 1 个位置参数"},
		{"fork多余位置参数", []string{"disk", "fork", testSourceDiskID, "extra"}, "需要且仅需要 1 个位置参数"},
		{"fork非法UUID", []string{"disk", "fork", "not-a-uuid"}, "无效的 source-disk-id"},
		{"fork全零NilUUID", []string{"disk", "fork", testNilDiskID}, "无效的 source-disk-id"},
		{"fork未知标志", []string{"disk", "fork", testSourceDiskID, "--unknown"}, "未知标志"},
		{"fork_size缺少参数值", []string{"disk", "fork", testSourceDiskID, "--size"}, "--size 需要指定参数值"},
		{"fork_name缺少参数值", []string{"disk", "fork", testSourceDiskID, "--name"}, "--name 需要指定参数值"},
		{"fork_endpoint缺少参数值", []string{"disk", "fork", testSourceDiskID, "--endpoint"}, "--endpoint 需要指定参数值"},
		{"fork_token缺少参数值", []string{"disk", "fork", testSourceDiskID, "--token"}, "--token 需要指定参数值"},
		{"fork_quiet与json互斥", []string{"disk", "fork", testSourceDiskID, "-q", "--json"}, "不能同时使用"},
		{"fork_size为空字符串", []string{"disk", "fork", testSourceDiskID, "--size="}, "无效的 --size 参数"},
		{"fork_size为零", []string{"disk", "fork", testSourceDiskID, "--size", "0"}, "无效的 --size 参数"},
		{"fork_name超长256字节", []string{"disk", "fork", testSourceDiskID, "--name", strings.Repeat("n", 256)}, "不能超过 255 字节"},
		{"fork_name包含NUL", []string{"disk", "fork", testSourceDiskID, "--name", "bad\x00name"}, "不能包含空字节"},
		{"fork无效endpoint协议", []string{"disk", "fork", testSourceDiskID, "--endpoint", "ftp://127.0.0.1:9000"}, "无效的 endpoint"},

		// create
		{"create多余位置参数", []string{"disk", "create", "--size", "1GiB", "extra"}, "create 命令不接受位置参数"},
		{"create缺少size", []string{"disk", "create"}, "必须指定 --size 参数"},
		{"create_quiet与json互斥", []string{"disk", "create", "--size", "1GiB", "-q", "--json"}, "不能同时使用"},
		{"create_size无效", []string{"disk", "create", "--size", "bad"}, "无效的 --size 参数"},
		{"create_name超长256字节", []string{"disk", "create", "--size", "1GiB", "--name", strings.Repeat("a", 256)}, "不能超过 255 字节"},
		{"create_name包含NUL", []string{"disk", "create", "--size", "1GiB", "--name", "bad\x00"}, "不能包含空字节"},

		// list
		{"list多余位置参数", []string{"disk", "list", "extra"}, "list 命令不接受位置参数"},
		{"list禁止quiet", []string{"disk", "list", "-q"}, "未知标志"},
		{"list_limit缺少参数值", []string{"disk", "list", "--limit"}, "--limit 需要指定参数值"},
		{"list_page-token缺少参数值", []string{"disk", "list", "--page-token"}, "--page-token 需要指定参数值"},
		{"list_limit为零", []string{"disk", "list", "--limit", "0"}, "必须为 [1, 1000] 范围内的整数"},
		{"list_limit超上限", []string{"disk", "list", "--limit", "1001"}, "必须为 [1, 1000] 范围内的整数"},
		{"list_limit非数字", []string{"disk", "list", "--limit", "abc"}, "必须为 [1, 1000] 范围内的整数"},

		// get
		{"get缺少位置参数", []string{"disk", "get"}, "需要且仅需要 1 个位置参数"},
		{"get非法UUID", []string{"disk", "get", "bad"}, "无效的 disk-id"},
		{"get禁止quiet", []string{"disk", "get", testSourceDiskID, "-q"}, "未知标志"},

		// update / resize
		{"update缺少位置参数", []string{"disk", "update"}, "需要且仅需要 1 个位置参数"},
		{"update非法UUID", []string{"disk", "update", testNilDiskID, "--size", "1GiB"}, "无效的 disk-id"},
		{"update禁止quiet", []string{"disk", "update", testSourceDiskID, "--size", "1GiB", "--quiet"}, "未知标志"},
		{"update未指定size或name", []string{"disk", "update", testSourceDiskID}, "必须至少指定 --size 或 --name 之一"},
		{"resize未指定size或name", []string{"disk", "resize", testSourceDiskID}, "必须至少指定 --size 或 --name 之一"},
		{"update_name超长", []string{"disk", "update", testSourceDiskID, "--name", strings.Repeat("b", 256)}, "不能超过 255 字节"},
		{"update_size无效", []string{"disk", "update", testSourceDiskID, "--size", "0"}, "无效的 --size 参数"},

		// delete
		{"delete缺少位置参数", []string{"disk", "delete"}, "需要且仅需要 1 个位置参数"},
		{"delete非法UUID", []string{"disk", "delete", "bad"}, "无效的 disk-id"},
		{"delete禁止quiet", []string{"disk", "delete", testSourceDiskID, "-q"}, "未知标志"},
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

func TestDiskOutputModesAndParentDiskDisplay(t *testing.T) {
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	handler := &mockDiskHandler{
		getFn: func(_ context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:     req.Msg.GetDiskId(),
					Name:       "root-disk",
					RootIno:    1,
					MaxBytes:   1 << 30,
					UsedBytes:  4096,
					UsedInodes: 1,
					CreatedAt:  now,
					UpdatedAt:  now,
				},
			}), nil
		},
	}
	endpoint := startMockDiskH2CServer(t, handler)

	// 1. fork 默认详情输出（省略 --size，验证 MaxBytes 为 nil 且紧接 Disk ID 打印 Parent Disk）
	code, out, errOut := runCLI("disk", "fork", testSourceDiskID, "--name", "branch-exp", "--endpoint", endpoint, "--token", "sec-tok")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotNil(t, handler.lastForkReq)
	require.Equal(t, testSourceDiskID, handler.lastForkReq.GetSourceDiskId())
	require.Equal(t, "branch-exp", handler.lastForkReq.GetName())
	require.Nil(t, handler.lastForkReq.MaxBytes)
	require.Equal(t, "Bearer sec-tok", handler.lastAuth)

	expectedHeader := "Disk ID:      " + testChildDiskID + "\nParent Disk:  " + testSourceDiskID + "\nName:         branch-exp\n"
	require.True(t, strings.HasPrefix(out, expectedHeader), "实际输出:\n%s", out)

	// 2. fork --json 输出模式（显式传入 --size=2GiB）
	code, out, errOut = runCLI("disk", "fork", "--name=json-branch", "--size=2GiB", "--json", "--endpoint="+endpoint, "--token=tok2", "--", testSourceDiskID)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotNil(t, handler.lastForkReq.MaxBytes)
	require.Equal(t, uint64(2<<30), handler.lastForkReq.GetMaxBytes())

	var dj diskJSON
	require.NoError(t, json.Unmarshal([]byte(out), &dj))
	require.Equal(t, testChildDiskID, dj.DiskID)
	require.Equal(t, testSourceDiskID, dj.ParentDiskID)
	require.Equal(t, "json-branch", dj.Name)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)

	// 3. fork -q / --quiet 静默模式
	code, out, errOut = runCLI("disk", "fork", testSourceDiskID, "-q", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, testChildDiskID+"\n", out)

	// 4. get 根磁盘（无 parent_disk_id）在详情与 --json 下均不展示父盘字段
	code, out, errOut = runCLI("disk", "get", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotContains(t, out, "Parent Disk:")

	code, out, errOut = runCLI("disk", "get", testSourceDiskID, "--json", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotContains(t, out, "parent_disk_id")

	// 5. create (-q / --json / 默认详情)
	code, out, errOut = runCLI("disk", "create", "--size", "1GiB", "--name", "c1", "-q", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, testSourceDiskID+"\n", out)
	require.Equal(t, uint64(1<<30), handler.lastCreateReq.GetMaxBytes())

	code, out, errOut = runCLI("disk", "create", "--size=2GiB", "--json", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NoError(t, json.Unmarshal([]byte(out), &dj))
	require.Equal(t, testSourceDiskID, dj.DiskID)
	require.Equal(t, uint64(2<<30), dj.MaxBytes)

	// 6. list (tabwriter 表格与 Next Page Token / --json)
	handler.listFn = func(_ context.Context, _ *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
		return connect.NewResponse(&storagev1.ListDisksResponse{
			Disks: []*storagev1.Disk{
				{
					DiskId:     testSourceDiskID,
					Name:       "alpha",
					RootIno:    1,
					MaxBytes:   2 << 30,
					UsedBytes:  1 << 30,
					UsedInodes: 7,
					CreatedAt:  now,
					UpdatedAt:  now,
				},
			},
			NextPageToken: "cursor-next-1",
		}), nil
	}
	code, out, errOut = runCLI("disk", "list", "--limit", "10", "--page-token", "tok-0", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, uint32(10), handler.lastListReq.GetPageSize())
	require.Equal(t, "tok-0", handler.lastListReq.GetPageToken())
	require.Contains(t, out, "DISK ID")
	require.Contains(t, out, "50.00%")
	require.Contains(t, out, "Next Page Token: cursor-next-1")

	code, out, errOut = runCLI("disk", "list", "--json", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var listRes diskListJSON
	require.NoError(t, json.Unmarshal([]byte(out), &listRes))
	require.Len(t, listRes.Disks, 1)
	require.Equal(t, "cursor-next-1", listRes.NextPageToken)

	// 7. update 与 resize 别名
	code, out, errOut = runCLI("disk", "resize", testSourceDiskID, "--size", "4GiB", "--name", "", "--json", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.NotNil(t, handler.lastUpdateReq.Name)
	require.Empty(t, handler.lastUpdateReq.GetName())
	require.NotNil(t, handler.lastUpdateReq.MaxBytes)
	require.Equal(t, uint64(4<<30), handler.lastUpdateReq.GetMaxBytes())
	require.NoError(t, json.Unmarshal([]byte(out), &dj))
	require.Equal(t, uint64(4<<30), dj.MaxBytes)

	// 8. delete (-y 文本与 --json)
	code, out, errOut = runDiskWithPorts(diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "", false, nil },
	}, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, "已删除虚拟磁盘 "+testSourceDiskID+"\n", out)

	code, out, errOut = runDiskWithPorts(diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "", false, nil },
	}, "delete", testSourceDiskID, "--yes", "--json", "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	var delRes diskDeleteJSON
	require.NoError(t, json.Unmarshal([]byte(out), &delRes))
	require.Equal(t, testSourceDiskID, delRes.DiskID)
	require.True(t, delRes.Deleted)
}

func TestDiskServerErrors(t *testing.T) {
	var mode string
	var mu sync.Mutex
	setMode := func(m string) {
		mu.Lock()
		mode = m
		mu.Unlock()
	}
	getMode := func() string {
		mu.Lock()
		defer mu.Unlock()
		return mode
	}

	handler := &mockDiskHandler{
		createFn: func(_ context.Context, _ *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
			if getMode() == "empty_disk" {
				return connect.NewResponse(&storagev1.CreateDiskResponse{}), nil
			}
			return nil, connect.NewError(connect.CodeInternal, errors.New("create boom"))
		},
		forkFn: func(_ context.Context, _ *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
			switch getMode() {
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
		getFn: func(_ context.Context, _ *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			switch getMode() {
			case "not_found":
				return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
			case "empty_disk":
				return connect.NewResponse(&storagev1.GetDiskResponse{}), nil
			case "used_10mib":
				return connect.NewResponse(&storagev1.GetDiskResponse{
					Disk: &storagev1.Disk{
						DiskId:    testSourceDiskID,
						MaxBytes:  100 << 20,
						UsedBytes: 10 << 20,
					},
				}), nil
			default:
				return nil, connect.NewError(connect.CodeInternal, errors.New("get boom"))
			}
		},
		listFn: func(_ context.Context, _ *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
			return nil, connect.NewError(connect.CodeInternal, errors.New("list boom"))
		},
		updateFn: func(_ context.Context, _ *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
			switch getMode() {
			case "update_not_found":
				return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
			case "update_shrink":
				return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("shrink conflict"))
			case "update_empty":
				return connect.NewResponse(&storagev1.UpdateDiskResponse{}), nil
			default:
				return nil, connect.NewError(connect.CodeInternal, errors.New("update boom"))
			}
		},
		deleteFn: func(_ context.Context, _ *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
			switch getMode() {
			case "not_found":
				return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
			case "failed_precondition":
				return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("active mount"))
			case "invalid_argument":
				return nil, connect.NewError(connect.CodeInvalidArgument, errors.New("bad disk id"))
			case "ebusy_detail":
				cErr := connect.NewError(connect.CodeUnavailable, errors.New("busy"))
				detail, dErr := connect.NewErrorDetail(&errdetails.ErrorInfo{Reason: "EBUSY"})
				if dErr == nil {
					cErr.AddDetail(detail)
				}
				return nil, cErr
			default:
				return nil, connect.NewError(connect.CodeInternal, errors.New("delete boom"))
			}
		},
	}
	endpoint := startMockDiskH2CServer(t, handler)

	// fork 错误矩阵
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

	// create / get / list 错误
	setMode("empty_disk")
	code, _, errOut = runCLI("disk", "create", "--size", "1GiB", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "空的虚拟磁盘元数据")

	setMode("not_found")
	code, _, errOut = runCLI("disk", "get", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testSourceDiskID)

	code, _, errOut = runCLI("disk", "list", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "列出虚拟磁盘失败")

	// update 缩容预检与 UpdateDisk 错误
	setMode("used_10mib")
	code, _, errOut = runCLI("disk", "update", testSourceDiskID, "--size", "1MiB", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "不允许缩容至已用水位以下")

	setMode("update_shrink")
	code, _, errOut = runCLI("disk", "update", testSourceDiskID, "--name", "only-name", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "目标容量小于磁盘当前已用容量")

	setMode("update_not_found")
	code, _, errOut = runCLI("disk", "update", testSourceDiskID, "--name", "only-name", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testSourceDiskID)

	setMode("update_empty")
	code, _, errOut = runCLI("disk", "update", testSourceDiskID, "--name", "only-name", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "空的虚拟磁盘元数据")

	// delete 错误（含 CodeInvalidArgument -> 2 与 ErrorInfo{Reason:"EBUSY"} 解包）
	delPorts := diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "", false, nil },
	}
	setMode("not_found")
	code, _, errOut = runDiskWithPorts(delPorts, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在: "+testSourceDiskID)

	setMode("failed_precondition")
	code, _, errOut = runDiskWithPorts(delPorts, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "仍处于挂载状态")

	setMode("invalid_argument")
	code, _, errOut = runDiskWithPorts(delPorts, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 2, code)
	require.Contains(t, errOut, "无效的请求参数")

	setMode("ebusy_detail")
	code, _, errOut = runDiskWithPorts(delPorts, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "仍处于挂载状态")

	setMode("internal")
	code, _, errOut = runDiskWithPorts(delPorts, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "删除虚拟磁盘失败")
}

func TestDiskDeleteLocalGuards(t *testing.T) {
	handler := &mockDiskHandler{}
	endpoint := startMockDiskH2CServer(t, handler)

	// 1. 本机已挂载拦截（不触发 RPC）
	code, out, errOut := runDiskWithPorts(diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "/mnt/bowl-test", true, nil },
		stdin:          strings.NewReader("y\n"),
	}, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl umount /mnt/bowl-test")
	require.Equal(t, 0, handler.deleteCalls)

	// 2. 挂载表检查报错（不触发 RPC）
	code, out, errOut = runDiskWithPorts(diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "", false, errors.New("proc io err") },
		stdin:          strings.NewReader("y\n"),
	}, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "检查本机挂载表失败")
	require.Equal(t, 0, handler.deleteCalls)

	// 3. stdin 输入 n 与 EOF 取消删除（无 "错误: " 前缀，不触发 RPC）
	for _, input := range []string{"n\n", ""} {
		code, out, errOut = runDiskWithPorts(diskPorts{
			findMountpoint: func(string) (string, bool, error) { return "", false, nil },
			stdin:          strings.NewReader(input),
		}, "delete", testSourceDiskID, "--endpoint", endpoint)
		require.Equal(t, 1, code)
		require.Empty(t, out)
		require.Contains(t, errOut, "操作已取消")
		require.NotContains(t, errOut, "错误:")
		require.Equal(t, 0, handler.deleteCalls)
	}

	// 4. stdin 读取异常
	code, out, errOut = runDiskWithPorts(diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "", false, nil },
		stdin:          errReader{err: errors.New("broken pipe")},
	}, "delete", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "读取终端确认输入失败")
	require.Equal(t, 0, handler.deleteCalls)

	// 5. stdin 输入 YES 确认删除
	code, out, errOut = runDiskWithPorts(diskPorts{
		findMountpoint: func(string) (string, bool, error) { return "", false, nil },
		stdin:          strings.NewReader("YES\n"),
	}, "delete", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 0, code)
	require.Contains(t, errOut, "确认删除虚拟磁盘")
	require.Contains(t, out, "已删除虚拟磁盘 "+testSourceDiskID)
	require.Equal(t, 1, handler.deleteCalls)
}

type errReader struct{ err error }

func (r errReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

func TestDiskCLISpanAndW3CTraceparentPropagation(t *testing.T) {
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

	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	var failFork bool
	handler := &mockDiskHandler{
		forkFn: func(_ context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
			if failFork {
				return nil, connect.NewError(connect.CodeNotFound, errors.New("source not found"))
			}
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
		getFn: func(_ context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
			return connect.NewResponse(&storagev1.GetDiskResponse{
				Disk: &storagev1.Disk{
					DiskId:    req.Msg.GetDiskId(),
					Name:      "root",
					RootIno:   1,
					MaxBytes:  1 << 30,
					CreatedAt: now,
					UpdatedAt: now,
				},
			}), nil
		},
	}
	endpoint := startMockDiskH2CServer(t, handler)

	// 1. 全 6 个子命令（含 resize 别名）Span 名、W3C Traceparent 与 X-Request-Id 传播
	spanRuns := []struct {
		spanName string
		run      func() (int, string, string)
	}{
		{"cli.disk.create", func() (int, string, string) {
			return runCLI("disk", "create", "--size", "1GiB", "-q", "--endpoint", endpoint)
		}},
		{"cli.disk.fork", func() (int, string, string) {
			return runCLI("disk", "fork", testSourceDiskID, "--name", "traced-fork", "-q", "--endpoint", endpoint)
		}},
		{"cli.disk.list", func() (int, string, string) {
			return runCLI("disk", "list", "--endpoint", endpoint)
		}},
		{"cli.disk.get", func() (int, string, string) {
			return runCLI("disk", "get", testSourceDiskID, "--endpoint", endpoint)
		}},
		{"cli.disk.update", func() (int, string, string) {
			return runCLI("disk", "resize", testSourceDiskID, "--name", "renamed", "--endpoint", endpoint)
		}},
		{"cli.disk.delete", func() (int, string, string) {
			return runDiskWithPorts(diskPorts{
				findMountpoint: func(string) (string, bool, error) { return "", false, nil },
			}, "delete", testSourceDiskID, "-y", "--endpoint", endpoint)
		}},
	}

	for _, sr := range spanRuns {
		exporter.Reset()
		code, _, errOut := sr.run()
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
		var rootSpan *tracetest.SpanStub
		for i := range spans {
			if spans[i].Name == sr.spanName {
				rootSpan = &spans[i]
			}
		}
		require.NotNil(t, rootSpan, "必须产生客户端根 Span %s", sr.spanName)
		require.Contains(t, okTraceparent, rootSpan.SpanContext.TraceID().String(), "请求头 Traceparent 必须携带 %s TraceID", sr.spanName)

		if sr.spanName == "cli.disk.fork" {
			attrMap := make(map[string]string)
			for _, a := range rootSpan.Attributes {
				attrMap[string(a.Key)] = a.Value.AsString()
			}
			require.Equal(t, testSourceDiskID, attrMap["bowl.source_disk_id"])
			require.Equal(t, testChildDiskID, attrMap["bowl.disk_id"])
		}
	}

	// 2. 失败路径：cli.disk.fork 标记 Error 状态。
	exporter.Reset()
	failFork = true
	code, _, errOut := runCLI("disk", "fork", testSourceDiskID, "--endpoint", endpoint)
	require.Equal(t, 1, code)
	require.Contains(t, errOut, "磁盘不存在")

	spans := exporter.GetSpans()
	var errSpan *tracetest.SpanStub
	for i := range spans {
		if spans[i].Name == "cli.disk.fork" {
			errSpan = &spans[i]
		}
	}
	require.NotNil(t, errSpan)
	require.Equal(t, codes.Error, errSpan.Status.Code)
}

var _ io.Reader = errReader{}
