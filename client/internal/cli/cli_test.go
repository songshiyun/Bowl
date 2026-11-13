package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/version"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const (
	testDiskID1 = "01950000-0000-7000-8000-000000000001"
	testDiskID2 = "01950000-0000-7000-8000-000000000002"
	testNilUUID = "00000000-0000-0000-0000-000000000000"
)

type roundTripperFunc func(*http.Request) (*http.Response, error)

func (fn roundTripperFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func runCLI(args ...string) (exitCode int, stdout, stderr string) {
	var outBuf, errBuf bytes.Buffer
	code := Execute(args, &outBuf, &errBuf)
	return code, outBuf.String(), errBuf.String()
}

func withMountInfoContent(t *testing.T, content string) {
	t.Helper()
	prev := mountInfoPath
	path := filepath.Join(t.TempDir(), "mountinfo")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o600))
	mountInfoPath = path
	t.Cleanup(func() { mountInfoPath = prev })
}

func withMissingMountInfo(t *testing.T) {
	t.Helper()
	prev := mountInfoPath
	mountInfoPath = filepath.Join(t.TempDir(), "nonexistent-mountinfo")
	t.Cleanup(func() { mountInfoPath = prev })
}

func withUnreadableMountInfo(t *testing.T) {
	t.Helper()
	prev := mountInfoPath
	mountInfoPath = t.TempDir()
	t.Cleanup(func() { mountInfoPath = prev })
}

func withDiskStdin(t *testing.T, r io.Reader) {
	t.Helper()
	prev := diskStdin
	diskStdin = r
	t.Cleanup(func() { diskStdin = prev })
}

func withTempMountMetaDirs(t *testing.T, dirs ...string) {
	t.Helper()
	prev := mountMetaDirs
	mountMetaDirs = func() []string { return dirs }
	t.Cleanup(func() { mountMetaDirs = prev })
}

type errReader struct{ err error }

func (r errReader) Read(_ []byte) (int, error) {
	return 0, r.err
}

// fakeStorage 提供内存状态化存储适配器，同时支撑 disk 与 mount 测试。
type fakeStorage struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu              sync.Mutex
	disks           map[string]*storagev1.Disk
	mounted         map[string]bool
	nextSeq         int
	epochSeq        uint64
	lastAuth        string
	lastTraceparent string
	lastRequestID   string
	deleteCalls     int
	updateCalls     int
	forcedErr       map[string]error
	returnNilDisk   map[string]bool
	mountZeroRoot   bool
	mountZeroEpoch  bool
}

func newFakeStorage() *fakeStorage {
	return &fakeStorage{
		disks:         make(map[string]*storagev1.Disk),
		mounted:       make(map[string]bool),
		forcedErr:     make(map[string]error),
		returnNilDisk: make(map[string]bool),
	}
}

func (f *fakeStorage) seedDisk(id, name string, maxBytes, usedBytes uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	f.disks[id] = &storagev1.Disk{
		DiskId:     id,
		Name:       name,
		RootIno:    1,
		MaxBytes:   maxBytes,
		UsedBytes:  usedBytes,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
}

func (f *fakeStorage) recordHeaders(header http.Header) {
	f.lastAuth = header.Get("Authorization")
	f.lastTraceparent = header.Get("Traceparent")
	f.lastRequestID = header.Get("X-Request-Id")
}

func (f *fakeStorage) CreateDisk(_ context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["CreateDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["CreateDisk"] {
		return connect.NewResponse(&storagev1.CreateDiskResponse{}), nil
	}
	f.nextSeq++
	id := fmt.Sprintf("01950000-0000-7000-8000-%012d", f.nextSeq)
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	d := &storagev1.Disk{
		DiskId:     id,
		Name:       req.Msg.GetName(),
		RootIno:    1,
		MaxBytes:   req.Msg.GetMaxBytes(),
		UsedBytes:  0,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	f.disks[id] = d
	return connect.NewResponse(&storagev1.CreateDiskResponse{Disk: d}), nil
}

func (f *fakeStorage) ForkDisk(_ context.Context, req *connect.Request[storagev1.ForkDiskRequest]) (*connect.Response[storagev1.ForkDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ForkDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["ForkDisk"] {
		return connect.NewResponse(&storagev1.ForkDiskResponse{}), nil
	}
	src, ok := f.disks[req.Msg.GetSourceDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("source disk not found"))
	}
	maxBytes := src.GetMaxBytes()
	if req.Msg.MaxBytes != nil {
		maxBytes = req.Msg.GetMaxBytes()
	}
	if maxBytes < src.GetUsedBytes() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("quota shrink below used"))
	}
	f.nextSeq++
	id := fmt.Sprintf("01950000-0000-7000-8000-%012d", f.nextSeq)
	parentID := src.GetDiskId()
	now := timestamppb.New(time.Date(2026, 11, 12, 12, 0, 0, 0, time.UTC))
	child := &storagev1.Disk{
		DiskId:       id,
		ParentDiskId: &parentID,
		Name:         req.Msg.GetName(),
		RootIno:      1,
		MaxBytes:     maxBytes,
		UsedBytes:    src.GetUsedBytes(),
		UsedInodes:   src.GetUsedInodes(),
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	f.disks[id] = child
	return connect.NewResponse(&storagev1.ForkDiskResponse{Disk: child}), nil
}

func (f *fakeStorage) GetDisk(_ context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["GetDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["GetDisk"] {
		return connect.NewResponse(&storagev1.GetDiskResponse{}), nil
	}
	d, ok := f.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{Disk: d}), nil
}

func (f *fakeStorage) ListDisks(_ context.Context, req *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["ListDisks"]; err != nil {
		return nil, err
	}
	keys := make([]string, 0, len(f.disks))
	for k := range f.disks {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	start := 0
	if req.Msg.GetPageToken() != "" {
		for idx, k := range keys {
			if k == req.Msg.GetPageToken() {
				start = idx
				break
			}
		}
	}
	limit := int(req.Msg.GetPageSize())
	if limit <= 0 {
		limit = 50
	}
	end := min(start+limit, len(keys))
	out := make([]*storagev1.Disk, 0, end-start)
	for _, k := range keys[start:end] {
		out = append(out, f.disks[k])
	}
	var nextToken string
	if end < len(keys) {
		nextToken = keys[end]
	}
	return connect.NewResponse(&storagev1.ListDisksResponse{
		Disks:         out,
		NextPageToken: nextToken,
	}), nil
}

func (f *fakeStorage) UpdateDisk(_ context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.updateCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["UpdateDisk"]; err != nil {
		return nil, err
	}
	if f.returnNilDisk["UpdateDisk"] {
		return connect.NewResponse(&storagev1.UpdateDiskResponse{}), nil
	}
	d, ok := f.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if req.Msg.MaxBytes != nil && req.Msg.GetMaxBytes() < d.GetUsedBytes() {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("shrink below used"))
	}
	updated := &storagev1.Disk{
		DiskId:       d.GetDiskId(),
		ParentDiskId: d.ParentDiskId,
		Name:         d.GetName(),
		RootIno:      d.GetRootIno(),
		MaxBytes:     d.GetMaxBytes(),
		UsedBytes:    d.GetUsedBytes(),
		UsedInodes:   d.GetUsedInodes(),
		CreatedAt:    d.GetCreatedAt(),
		UpdatedAt:    d.GetUpdatedAt(),
	}
	if req.Msg.Name != nil {
		updated.Name = req.Msg.GetName()
	}
	if req.Msg.MaxBytes != nil {
		updated.MaxBytes = req.Msg.GetMaxBytes()
	}
	f.disks[d.GetDiskId()] = updated
	return connect.NewResponse(&storagev1.UpdateDiskResponse{Disk: updated}), nil
}

func (f *fakeStorage) DeleteDisk(_ context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCalls++
	f.recordHeaders(req.Header())
	if err := f.forcedErr["DeleteDisk"]; err != nil {
		return nil, err
	}
	id := req.Msg.GetDiskId()
	if f.mounted[id] {
		return nil, connect.NewError(connect.CodeFailedPrecondition, errors.New("active mount session"))
	}
	if _, ok := f.disks[id]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	delete(f.disks, id)
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

func (f *fakeStorage) MountDisk(_ context.Context, req *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recordHeaders(req.Header())
	if err := f.forcedErr["MountDisk"]; err != nil {
		return nil, err
	}
	d, ok := f.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if f.mountZeroRoot {
		return connect.NewResponse(&storagev1.MountDiskResponse{}), nil
	}
	var epoch uint64
	if !f.mountZeroEpoch && !req.Msg.GetReadOnly() {
		f.epochSeq++
		epoch = f.epochSeq
	}
	return connect.NewResponse(&storagev1.MountDiskResponse{
		Epoch: epoch,
		RootInode: &storagev1.Inode{
			Ino:  d.GetRootIno(),
			Attr: &storagev1.InodeAttribute{Mode: 0o040755},
		},
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func newFakeStorageHTTPClient(f *fakeStorage) *http.Client {
	path, connectHandler := storagev1connect.NewStorageServiceHandler(f)
	mux := http.NewServeMux()
	mux.Handle(path, connectHandler)
	return &http.Client{
		Transport: roundTripperFunc(func(req *http.Request) (*http.Response, error) {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)
			resp := rec.Result()
			resp.Request = req
			return resp, nil
		}),
	}
}

func withDiskStorageFake(t *testing.T, f *fakeStorage) {
	t.Helper()
	prev := diskHTTPClient
	diskHTTPClient = newFakeStorageHTTPClient(f)
	t.Cleanup(func() { diskHTTPClient = prev })
}

func withMountStorageFake(t *testing.T, f *fakeStorage) {
	t.Helper()
	prev := mountHTTPClient
	mountHTTPClient = newFakeStorageHTTPClient(f)
	t.Cleanup(func() { mountHTTPClient = prev })
}

func requireSpanByName(t *testing.T, spans tracetest.SpanStubs, name string) *tracetest.SpanStub {
	t.Helper()
	for i := range spans {
		if spans[i].Name == name {
			return &spans[i]
		}
	}
	t.Fatalf("未采集到期望 Span %q", name)
	return nil
}

func requireSpanStringAttr(t *testing.T, sp *tracetest.SpanStub, key, want string) {
	t.Helper()
	for _, attr := range sp.Attributes {
		if string(attr.Key) == key {
			require.Equal(t, want, attr.Value.AsString())
			return
		}
	}
	t.Fatalf("Span %s 缺少属性 %q", sp.Name, key)
}

func TestExecute_GivenNoArgs_PrintsUsageToStderrAndReturns2(t *testing.T) {
	// Given / When
	code, out, errOut := runCLI()

	// Then
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, "bowl <command>")
}

func TestExecute_GivenUnknownCommand_PrintsErrorAndReturns2(t *testing.T) {
	// Given / When
	code, out, errOut := runCLI("unknown-cmd")

	// Then
	require.Equal(t, 2, code)
	require.Empty(t, out)
	require.Contains(t, errOut, `错误: 未知命令 "unknown-cmd"`)
	require.Contains(t, errOut, "可用命令:")
}

func TestExecute_GivenVersionFlags_PrintsVersionAndReturns0(t *testing.T) {
	// Given / When / Then
	want := "bowl version " + version.Version + "\n"

	code, out, errOut := runCLI("version")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, want, out)

	code, out, errOut = runCLI("--version")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, want, out)

	code, out, errOut = runCLI("-v")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Equal(t, want, out)
}

func TestExecute_GivenHelpFlags_PrintsUsageAndReturns0(t *testing.T) {
	// Given / When / Then
	code, out, errOut := runCLI("help")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "可用命令:")

	code, out, errOut = runCLI("--help")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "可用命令:")

	code, out, errOut = runCLI("-h")
	require.Equal(t, 0, code)
	require.Empty(t, errOut)
	require.Contains(t, out, "可用命令:")
}
