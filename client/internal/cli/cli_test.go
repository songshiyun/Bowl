package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type fakeStorageService struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mu sync.Mutex

	mountRequests  []*storagev1.MountDiskRequest
	getDiskCalls   int
	deleteCalls    []string
	mkdirRequests  []*storagev1.MkdirRequest
	lookupRequests []*storagev1.LookupRequest

	disks map[string]*storagev1.Disk
	epoch uint64
}

func newFakeStorageServer(t *testing.T) (svc *fakeStorageService, endpoint string) {
	t.Helper()
	svc = &fakeStorageService{
		disks: make(map[string]*storagev1.Disk),
	}
	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(svc)
	mux.Handle(path, handler)

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)
	return svc, srv.URL
}

func (s *fakeStorageService) MountDisk(_ context.Context, req *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mountRequests = append(s.mountRequests, req.Msg)

	disk, ok := s.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}

	var epoch uint64
	if !req.Msg.GetReadOnly() {
		s.epoch++
		epoch = s.epoch
	}
	now := timestamppb.New(time.Unix(1700000000, 0).UTC())
	return connect.NewResponse(&storagev1.MountDiskResponse{
		Epoch: epoch,
		RootInode: &storagev1.Inode{
			Ino:        disk.GetRootIno(),
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:    syscall.S_IFDIR | 0o755,
				Nlink:   2,
				Size:    4096,
				Blksize: 65536,
				Atime:   now,
				Mtime:   now,
				Ctime:   now,
			},
		},
		EntryTimeoutSeconds: 1.5,
		AttrTimeoutSeconds:  2.5,
	}), nil
}

func (s *fakeStorageService) CreateDisk(_ context.Context, req *connect.Request[storagev1.CreateDiskRequest]) (*connect.Response[storagev1.CreateDiskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := timestamppb.New(time.Unix(1700000000, 0).UTC())
	id := uuid.NewString()
	disk := &storagev1.Disk{
		DiskId:     id,
		Name:       req.Msg.GetName(),
		RootIno:    1,
		MaxBytes:   req.Msg.GetMaxBytes(),
		UsedBytes:  4096,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}
	s.disks[id] = disk
	return connect.NewResponse(&storagev1.CreateDiskResponse{Disk: disk}), nil
}

func (s *fakeStorageService) GetDisk(_ context.Context, req *connect.Request[storagev1.GetDiskRequest]) (*connect.Response[storagev1.GetDiskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getDiskCalls++
	disk, ok := s.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	return connect.NewResponse(&storagev1.GetDiskResponse{Disk: disk}), nil
}

func (s *fakeStorageService) ListDisks(_ context.Context, _ *connect.Request[storagev1.ListDisksRequest]) (*connect.Response[storagev1.ListDisksResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*storagev1.Disk, 0, len(s.disks))
	for _, d := range s.disks {
		out = append(out, d)
	}
	return connect.NewResponse(&storagev1.ListDisksResponse{Disks: out}), nil
}

func (s *fakeStorageService) UpdateDisk(_ context.Context, req *connect.Request[storagev1.UpdateDiskRequest]) (*connect.Response[storagev1.UpdateDiskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	disk, ok := s.disks[req.Msg.GetDiskId()]
	if !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	if req.Msg.Name != nil {
		disk.Name = req.Msg.GetName()
	}
	if req.Msg.MaxBytes != nil {
		disk.MaxBytes = req.Msg.GetMaxBytes()
	}
	return connect.NewResponse(&storagev1.UpdateDiskResponse{Disk: disk}), nil
}

func (s *fakeStorageService) DeleteDisk(_ context.Context, req *connect.Request[storagev1.DeleteDiskRequest]) (*connect.Response[storagev1.DeleteDiskResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deleteCalls = append(s.deleteCalls, req.Msg.GetDiskId())
	if _, ok := s.disks[req.Msg.GetDiskId()]; !ok {
		return nil, connect.NewError(connect.CodeNotFound, errors.New("disk not found"))
	}
	delete(s.disks, req.Msg.GetDiskId())
	return connect.NewResponse(&storagev1.DeleteDiskResponse{}), nil
}

func (s *fakeStorageService) Mkdir(_ context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.mkdirRequests = append(s.mkdirRequests, req.Msg)
	now := timestamppb.New(time.Unix(1700000000, 0).UTC())
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode: &storagev1.Inode{
			Ino:        2,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:  syscall.S_IFDIR | 0o755,
				Nlink: 2,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
		},
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (s *fakeStorageService) Lookup(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.lookupRequests = append(s.lookupRequests, req.Msg)
	now := timestamppb.New(time.Unix(1700000000, 0).UTC())
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode: &storagev1.Inode{
			Ino:        3,
			Generation: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:  syscall.S_IFREG | 0o644,
				Nlink: 1,
				Atime: now,
				Mtime: now,
				Ctime: now,
			},
		},
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func TestCLI_RemovedBranchAndVersionCommandsAndMountFlags(t *testing.T) {
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"--help"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	helpOut := stdout.String()
	for _, removedCmd := range []string{"branch", "commit", "merge", "rollback", "diff"} {
		require.NotContains(t, helpOut, removedCmd, "根帮助信息不得再包含已裁撤的 %s 命令", removedCmd)

		stdout.Reset()
		stderr.Reset()
		exitCode := Execute([]string{removedCmd}, &stdout, &stderr)
		require.Equal(t, 2, exitCode, "调用已移除的子命令 %s 必须返回退出码 2", removedCmd)
		require.Contains(t, stderr.String(), "未知命令")
	}

	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"mount", "--help"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	mountHelp := stdout.String()
	require.NotContains(t, mountHelp, "--branch")
	require.NotContains(t, mountHelp, "--commit")

	diskID := uuid.NewString()
	mp := t.TempDir()
	for _, flagArgs := range [][]string{
		{"mount", "--branch", "main", diskID, mp},
		{"mount", "--branch=main", diskID, mp},
		{"mount", "--commit", uuid.NewString(), "--ro", diskID, mp},
		{"mount", "--commit=" + uuid.NewString(), "--ro", diskID, mp},
	} {
		stdout.Reset()
		stderr.Reset()
		exitCode := Execute(flagArgs, &stdout, &stderr)
		require.Equal(t, 2, exitCode, "传入已移除的标志 %v 必须返回退出码 2", flagArgs)
		require.Contains(t, stderr.String(), "未知标志")
	}
}

func TestCLI_MountSingleHandshakeRWAndRO(t *testing.T) {
	svc, endpoint := newFakeStorageServer(t)
	diskID := uuid.NewString()
	now := timestamppb.New(time.Unix(1700000000, 0).UTC())
	svc.disks[diskID] = &storagev1.Disk{
		DiskId:     diskID,
		Name:       "mount-test-disk",
		RootIno:    1,
		MaxBytes:   1 << 30,
		UsedBytes:  4096,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	origServe := serveFUSE
	t.Cleanup(func() { serveFUSE = origServe })

	// 1. 可写挂载 (read_only = false)：执行单次 MountDisk 握手，注入 Epoch > 0 并透传到数据面写 RPC
	var capturedRWConfig fusefs.Config
	serveFUSE = func(_ context.Context, rawFS *fusefs.RawFS, cfg fusefs.Config, _ string) error {
		defer rawFS.OnUnmount()
		capturedRWConfig = rawFS.Config()
		require.Equal(t, cfg.Epoch, capturedRWConfig.Epoch)

		var entryOut fuse.EntryOut
		st := rawFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0o755}, "subdir", &entryOut)
		require.Equal(t, fuse.OK, st)

		st = rawFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file.txt", &entryOut)
		require.Equal(t, fuse.OK, st)
		return nil
	}

	mp := t.TempDir()
	var stdout, stderr bytes.Buffer
	exitCode := Execute([]string{"mount", "-f", "--endpoint", endpoint, "--direct-io", "--allow-other", diskID, mp}, &stdout, &stderr)
	require.Equal(t, 0, exitCode, "可写挂载失败: %s", stderr.String())

	svc.mu.Lock()
	require.Len(t, svc.mountRequests, 1, "bowl mount 必须仅发起单次 MountDisk 握手")
	require.Equal(t, diskID, svc.mountRequests[0].GetDiskId())
	require.False(t, svc.mountRequests[0].GetReadOnly())
	require.Zero(t, svc.getDiskCalls, "bowl mount 不得额外调用 GetDisk")
	require.Len(t, svc.mkdirRequests, 1)
	require.Equal(t, diskID, svc.mkdirRequests[0].GetDiskId())
	require.Equal(t, uint64(1), svc.mkdirRequests[0].GetEpoch())
	require.Len(t, svc.lookupRequests, 1)
	require.Equal(t, diskID, svc.lookupRequests[0].GetDiskId())
	svc.mu.Unlock()

	require.Equal(t, diskID, capturedRWConfig.DiskID)
	require.Equal(t, uint64(1), capturedRWConfig.Epoch)
	require.Equal(t, uint64(1), capturedRWConfig.RootIno)
	require.NotNil(t, capturedRWConfig.RootInode)
	require.InDelta(t, 1.5, capturedRWConfig.EntryTimeoutSeconds, 1e-6)
	require.InDelta(t, 2.5, capturedRWConfig.AttrTimeoutSeconds, 1e-6)
	require.False(t, capturedRWConfig.ReadOnly)
	require.True(t, capturedRWConfig.DirectIO)
	require.True(t, capturedRWConfig.AllowOther)

	// 2. 只读挂载 (--ro, read_only = true)：执行单次 MountDisk 握手，注入 Epoch = 0 且本地拦截写操作
	var capturedROConfig fusefs.Config
	serveFUSE = func(_ context.Context, rawFS *fusefs.RawFS, _ fusefs.Config, _ string) error {
		defer rawFS.OnUnmount()
		capturedROConfig = rawFS.Config()
		require.Equal(t, fuse.Status(syscall.EROFS), rawFS.CheckWritable())
		return nil
	}

	stdout.Reset()
	stderr.Reset()
	exitCode = Execute([]string{"mount", "-f", "--ro", "--endpoint", endpoint, diskID, mp}, &stdout, &stderr)
	require.Equal(t, 0, exitCode, "只读挂载失败: %s", stderr.String())

	svc.mu.Lock()
	require.Len(t, svc.mountRequests, 2)
	require.Equal(t, diskID, svc.mountRequests[1].GetDiskId())
	require.True(t, svc.mountRequests[1].GetReadOnly())
	svc.mu.Unlock()

	require.Equal(t, diskID, capturedROConfig.DiskID)
	require.Zero(t, capturedROConfig.Epoch)
	require.Equal(t, uint64(1), capturedROConfig.RootIno)
	require.True(t, capturedROConfig.ReadOnly)

	// 3. 挂载不存在的磁盘：返回退出码 1 并提示磁盘不存在
	stdout.Reset()
	stderr.Reset()
	missingDiskID := uuid.NewString()
	exitCode = Execute([]string{"mount", "-f", "--endpoint", endpoint, missingDiskID, mp}, &stdout, &stderr)
	require.Equal(t, 1, exitCode)
	require.Contains(t, stderr.String(), "磁盘不存在")
}

func TestCLI_UmountLocalOnly(t *testing.T) {
	mp := filepath.Clean(t.TempDir())
	fakeMountInfo := filepath.Join(t.TempDir(), "mountinfo")
	content := fmt.Sprintf("100 30 0:50 / %s rw,nosuid,nodev - fuse.bowl bowl:%s rw\n", mp, uuid.NewString())
	require.NoError(t, os.WriteFile(fakeMountInfo, []byte(content), 0o644))

	origMountInfoPath := mountInfoPath
	origLookPath := lookPath
	origRunCommand := runCommand
	t.Cleanup(func() {
		mountInfoPath = origMountInfoPath
		lookPath = origLookPath
		runCommand = origRunCommand
	})

	mountInfoPath = fakeMountInfo
	lookPath = func(file string) (string, error) {
		if file == "fusermount3" {
			return "/bin/fusermount3", nil
		}
		return "", errors.New("not found")
	}

	var invoked [][]string
	runCommand = func(name string, args ...string) ([]byte, error) {
		invoked = append(invoked, append([]string{name}, args...))
		return nil, nil
	}

	var stdout, stderr bytes.Buffer
	code := Execute([]string{"umount", mp}, &stdout, &stderr)
	require.Equal(t, 0, code, "umount 失败: %s", stderr.String())
	require.Equal(t, [][]string{{"/bin/fusermount3", "-u", mp}}, invoked)

	invoked = nil
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"unmount", "-f", mp}, &stdout, &stderr)
	require.Equal(t, 0, code, "unmount -f 失败: %s", stderr.String())
	require.Equal(t, [][]string{{"/bin/fusermount3", "-uz", mp}}, invoked)
}

func TestCLI_DiskOutputAndDeleteMountinfoGuard(t *testing.T) {
	svc, endpoint := newFakeStorageServer(t)

	// 1. disk create (JSON & 文本输出均不包含 default_branch / branches)
	var stdout, stderr bytes.Buffer
	code := Execute([]string{"disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "alpha", "--json"}, &stdout, &stderr)
	require.Equal(t, 0, code, "disk create --json 失败: %s", stderr.String())

	var createdMap map[string]any
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &createdMap))
	require.NotContains(t, createdMap, "default_branch")
	require.NotContains(t, createdMap, "branches")
	diskID, ok := createdMap["disk_id"].(string)
	require.True(t, ok)
	require.NotEmpty(t, diskID)

	// 2. disk get (文本与 JSON)
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "get", "--endpoint", endpoint, diskID}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Contains(t, stdout.String(), diskID)
	require.NotContains(t, stdout.String(), "Default Branch")
	require.NotContains(t, stdout.String(), "Branches")

	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "get", "--endpoint", endpoint, diskID, "--json"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	var gotMap map[string]any
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &gotMap))
	require.NotContains(t, gotMap, "default_branch")
	require.NotContains(t, gotMap, "branches")

	// 3. disk list (文本表头不含 BRANCHES，JSON 不含分支字段)
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "list", "--endpoint", endpoint}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.Contains(t, stdout.String(), "DISK ID")
	require.NotContains(t, stdout.String(), "BRANCHES")

	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "list", "--endpoint", endpoint, "--json"}, &stdout, &stderr)
	require.Equal(t, 0, code)
	require.NotContains(t, stdout.String(), "default_branch")
	require.NotContains(t, stdout.String(), "branches")

	// 4. disk update
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "update", "--endpoint", endpoint, diskID, "--size", "2GiB", "--name", "beta", "--json"}, &stdout, &stderr)
	require.Equal(t, 0, code, "disk update 失败: %s", stderr.String())
	var updatedMap map[string]any
	require.NoError(t, json.Unmarshal(stdout.Bytes(), &updatedMap))
	require.Equal(t, "beta", updatedMap["name"])
	require.NotContains(t, updatedMap, "default_branch")
	require.NotContains(t, updatedMap, "branches")

	// 5. disk delete：当本机 /proc/self/mountinfo 存在活跃挂载时必须拦截且不调用服务端 DeleteDisk
	fakeMountInfo := filepath.Join(t.TempDir(), "mountinfo")
	mountedLine := fmt.Sprintf("100 30 0:50 / /mnt/active-bowl rw,nosuid,nodev - fuse.bowl bowl:%s rw\n", diskID)
	require.NoError(t, os.WriteFile(fakeMountInfo, []byte(mountedLine), 0o644))

	origMountInfoPath := mountInfoPath
	t.Cleanup(func() { mountInfoPath = origMountInfoPath })
	mountInfoPath = fakeMountInfo

	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "delete", "--endpoint", endpoint, diskID, "-y"}, &stdout, &stderr)
	require.Equal(t, 1, code, "本机挂载状态下 disk delete 必须被拦截")
	require.Contains(t, stderr.String(), "bowl umount /mnt/active-bowl")
	svc.mu.Lock()
	require.Empty(t, svc.deleteCalls, "本机挂载快检拦截后不得向服务端发送 DeleteDisk RPC")
	svc.mu.Unlock()

	// 6. 卸载后（mountinfo 无该盘记录），disk delete 正常调用 DeleteDisk RPC 并返回 0
	require.NoError(t, os.WriteFile(fakeMountInfo, []byte("23 61 0:21 / /sys rw - sysfs sysfs rw\n"), 0o644))
	stdout.Reset()
	stderr.Reset()
	code = Execute([]string{"disk", "delete", "--endpoint", endpoint, diskID, "-y", "--json"}, &stdout, &stderr)
	require.Equal(t, 0, code, "卸载后 disk delete 失败: %s", stderr.String())
	svc.mu.Lock()
	require.Equal(t, []string{diskID}, svc.deleteCalls)
	svc.mu.Unlock()
}
