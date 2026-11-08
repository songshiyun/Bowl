package fusefs_test

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"syscall"
	"testing"
	"unsafe"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type recordedCall struct {
	procedure string
	diskID    string
	epoch     uint64
}

type epochAuditClient struct {
	storagev1connect.UnimplementedStorageServiceHandler
	mu         sync.Mutex
	calls      []recordedCall
	writeErr   error
	mountCalls int
}

func (c *epochAuditClient) record(proc, diskID string, epoch uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.calls = append(c.calls, recordedCall{
		procedure: proc,
		diskID:    diskID,
		epoch:     epoch,
	})
}

func (c *epochAuditClient) takeCalls() []recordedCall {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := append([]recordedCall(nil), c.calls...)
	c.calls = nil
	return out
}

func (c *epochAuditClient) MountDisk(_ context.Context, _ *connect.Request[storagev1.MountDiskRequest]) (*connect.Response[storagev1.MountDiskResponse], error) {
	c.mu.Lock()
	c.mountCalls++
	c.mu.Unlock()
	return nil, connect.NewError(connect.CodeUnimplemented, errors.New("MountDisk must never be called by FUSE data plane"))
}

func sampleInode(ino uint64, mode uint32) *storagev1.Inode {
	return &storagev1.Inode{
		Ino:        ino,
		Generation: 1,
		Attr: &storagev1.InodeAttribute{
			Mode:  mode,
			Nlink: 1,
			Size:  64,
		},
	}
}

// --- 9 Read-Only RPCs ---

func (c *epochAuditClient) Lookup(_ context.Context, req *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	c.record("Lookup", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.LookupResponse{
		Inode:               sampleInode(10, syscall.S_IFREG|0o644),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *epochAuditClient) GetAttr(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	c.record("GetAttr", req.Msg.GetDiskId(), 0)
	mode := uint32(syscall.S_IFREG | 0o644)
	if req.Msg.GetIno() == 1 {
		mode = syscall.S_IFDIR | 0o755
	}
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              sampleInode(req.Msg.GetIno(), mode),
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (c *epochAuditClient) Readlink(_ context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	c.record("Readlink", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.ReadlinkResponse{Target: []byte("/target")}), nil
}

func (c *epochAuditClient) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	c.record("Read", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.ReadResponse{Data: []byte("hello")}), nil
}

func (c *epochAuditClient) ReadDir(_ context.Context, req *connect.Request[storagev1.ReadDirRequest]) (*connect.Response[storagev1.ReadDirResponse], error) {
	c.record("ReadDir", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.ReadDirResponse{}), nil
}

func (c *epochAuditClient) ReadDirPlus(_ context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	c.record("ReadDirPlus", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.ReadDirPlusResponse{}), nil
}

func (c *epochAuditClient) GetXattr(_ context.Context, req *connect.Request[storagev1.GetXattrRequest]) (*connect.Response[storagev1.GetXattrResponse], error) {
	c.record("GetXattr", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.GetXattrResponse{Value: []byte("val")}), nil
}

func (c *epochAuditClient) ListXattr(_ context.Context, req *connect.Request[storagev1.ListXattrRequest]) (*connect.Response[storagev1.ListXattrResponse], error) {
	c.record("ListXattr", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.ListXattrResponse{Names: [][]byte{[]byte("user.k")}}), nil
}

func (c *epochAuditClient) StatFS(_ context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	c.record("StatFS", req.Msg.GetDiskId(), 0)
	return connect.NewResponse(&storagev1.StatFSResponse{Blocks: 1000, Bsize: 4096, Frsize: 4096, Namelen: 255}), nil
}

// --- 16 Write/Mutation RPCs ---

func (c *epochAuditClient) SetAttr(_ context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	c.record("SetAttr", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              sampleInode(req.Msg.GetIno(), syscall.S_IFREG|0o644),
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (c *epochAuditClient) Mknod(_ context.Context, req *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	c.record("Mknod", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.MknodResponse{
		Inode:               sampleInode(11, req.Msg.GetMode()),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *epochAuditClient) Mkdir(_ context.Context, req *connect.Request[storagev1.MkdirRequest]) (*connect.Response[storagev1.MkdirResponse], error) {
	c.record("Mkdir", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.MkdirResponse{
		Inode:               sampleInode(12, syscall.S_IFDIR|0o755),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *epochAuditClient) Symlink(_ context.Context, req *connect.Request[storagev1.SymlinkRequest]) (*connect.Response[storagev1.SymlinkResponse], error) {
	c.record("Symlink", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.SymlinkResponse{
		Inode:               sampleInode(13, syscall.S_IFLNK|0o777),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *epochAuditClient) Link(_ context.Context, req *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	c.record("Link", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.LinkResponse{
		Inode:               sampleInode(req.Msg.GetIno(), syscall.S_IFREG|0o644),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *epochAuditClient) Unlink(_ context.Context, req *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	c.record("Unlink", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.UnlinkResponse{}), nil
}

func (c *epochAuditClient) Rmdir(_ context.Context, req *connect.Request[storagev1.RmdirRequest]) (*connect.Response[storagev1.RmdirResponse], error) {
	c.record("Rmdir", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.RmdirResponse{}), nil
}

func (c *epochAuditClient) Rename(_ context.Context, req *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	c.record("Rename", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.RenameResponse{}), nil
}

func (c *epochAuditClient) Create(_ context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	c.record("Create", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode:               sampleInode(14, syscall.S_IFREG|0o644),
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (c *epochAuditClient) Write(_ context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	c.record("Write", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(len(req.Msg.GetData())),
		Inode: sampleInode(req.Msg.GetIno(), syscall.S_IFREG|0o644),
	}), nil
}

func (c *epochAuditClient) Fsync(_ context.Context, req *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	c.record("Fsync", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (c *epochAuditClient) FsyncDir(_ context.Context, req *connect.Request[storagev1.FsyncDirRequest]) (*connect.Response[storagev1.FsyncDirResponse], error) {
	c.record("FsyncDir", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.FsyncDirResponse{}), nil
}

func (c *epochAuditClient) Fallocate(_ context.Context, req *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	c.record("Fallocate", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (c *epochAuditClient) SetXattr(_ context.Context, req *connect.Request[storagev1.SetXattrRequest]) (*connect.Response[storagev1.SetXattrResponse], error) {
	c.record("SetXattr", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.SetXattrResponse{}), nil
}

func (c *epochAuditClient) RemoveXattr(_ context.Context, req *connect.Request[storagev1.RemoveXattrRequest]) (*connect.Response[storagev1.RemoveXattrResponse], error) {
	c.record("RemoveXattr", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.RemoveXattrResponse{}), nil
}

func (c *epochAuditClient) ReclaimOrphan(_ context.Context, req *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	c.record("ReclaimOrphan", req.Msg.GetDiskId(), req.Msg.GetEpoch())
	if c.writeErr != nil {
		return nil, c.writeErr
	}
	return connect.NewResponse(&storagev1.ReclaimOrphanResponse{}), nil
}

func TestConfig_FieldsAndSessionRemoval(t *testing.T) {
	cfgType := reflect.TypeFor[fusefs.Config]()
	for _, removed := range []string{"BranchName", "CommitID"} {
		_, exists := cfgType.FieldByName(removed)
		require.False(t, exists, "fusefs.Config 不得保留 %s 字段", removed)
	}
	epochField, exists := cfgType.FieldByName("Epoch")
	require.True(t, exists, "fusefs.Config 必须包含 Epoch 字段")
	require.Equal(t, reflect.Uint64, epochField.Type.Kind())
}

func TestRawFS_ReadAndWriteRPCsCarryDiskIDAndStaticEpoch(t *testing.T) {
	const (
		diskID = "disk-epoch-test"
		epoch  = uint64(42)
	)
	client := &epochAuditClient{}
	fs := fusefs.NewRawFS(client, fusefs.Config{
		DiskID:   diskID,
		Epoch:    epoch,
		RootIno:  1,
		DirectIO: true,
	})
	t.Cleanup(fs.OnUnmount)

	// 1. 验证全部只读 RPC 携带 DiskId: fs.cfg.DiskID
	var entryOut fuse.EntryOut
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file.txt", &entryOut))
	var attrOut fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 10}}, &attrOut))
	target, st := fs.Readlink(nil, &fuse.InHeader{NodeId: 13})
	require.Equal(t, fuse.OK, st)
	require.Equal(t, []byte("/target"), target)

	var openOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 10}, Flags: syscall.O_RDWR}, &openOut))
	readBuf := make([]byte, 16)
	_, st = fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Size: 5}, readBuf)
	require.Equal(t, fuse.OK, st)

	var dirOpenOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &dirOpenOut))
	dirBuf := make([]byte, 4096)
	require.Equal(t, fuse.OK, fs.ReadDir(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dirOpenOut.Fh, Offset: 0, Size: 4096}, fuse.NewDirEntryList(dirBuf, 0)))
	require.Equal(t, fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dirOpenOut.Fh, Offset: 0, Size: 4096}, fuse.NewDirEntryList(dirBuf, 0)))

	xattrDest := make([]byte, 64)
	_, st = fs.GetXAttr(nil, &fuse.InHeader{NodeId: 10}, "user.k", xattrDest)
	require.Equal(t, fuse.OK, st)
	_, st = fs.ListXAttr(nil, &fuse.InHeader{NodeId: 10}, xattrDest)
	require.Equal(t, fuse.OK, st)
	var statfsOut fuse.StatfsOut
	require.Equal(t, fuse.OK, fs.StatFs(nil, &fuse.InHeader{NodeId: 1}, &statfsOut))

	readCalls := client.takeCalls()
	require.NotEmpty(t, readCalls)
	for _, call := range readCalls {
		require.Equal(t, diskID, call.diskID, "只读 RPC %s 必须携带 DiskId", call.procedure)
		require.Zero(t, call.epoch, "只读 RPC %s 不得携带写 Epoch", call.procedure)
	}

	// 2. 验证全部 16 个写/变更 RPC 携带 DiskId: fs.cfg.DiskID 与 Epoch: fs.cfg.Epoch
	setAttrIn := &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: 10}, Valid: fuse.FATTR_MODE, Mode: 0o600}}
	require.Equal(t, fuse.OK, fs.SetAttr(nil, setAttrIn, &attrOut))
	require.Equal(t, fuse.OK, fs.Mknod(nil, &fuse.MknodIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: syscall.S_IFREG | 0o644}, "nod", &entryOut))
	require.Equal(t, fuse.OK, fs.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0o755}, "dir", &entryOut))
	require.Equal(t, fuse.OK, fs.Symlink(nil, &fuse.InHeader{NodeId: 1}, "/dst", "sym", &entryOut))
	require.Equal(t, fuse.OK, fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: 10}, "hardlink", &entryOut))
	require.Equal(t, fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: 1}, "dir"))
	require.Equal(t, fuse.OK, fs.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 1}, Newdir: 1}, "hardlink", "hardlink-renamed"))

	var createOut fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o644}, "created", &createOut))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: createOut.NodeId}, Fh: createOut.Fh})

	_, st = fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Size: 4}, []byte("data"))
	require.Equal(t, fuse.OK, st)
	require.Equal(t, fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh}))
	require.Equal(t, fuse.OK, fs.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dirOpenOut.Fh}))
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dirOpenOut.Fh})
	require.Equal(t, fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Length: 4096}))
	require.Equal(t, fuse.OK, fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 10}}, "user.k", []byte("v")))
	require.Equal(t, fuse.OK, fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: 10}, "user.k"))

	// Ioctl SETFLAGS 触发 SetAttr 写 RPC
	flagsVal := uint32(0)
	ioctlInBuf := unsafe.Slice((*byte)(unsafe.Pointer(&flagsVal)), 4)
	var ioctlOut fuse.IoctlOut
	require.Equal(t, fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
		InHeader: fuse.InHeader{NodeId: 10},
		Fh:       openOut.Fh,
		Cmd:      0x40086602, // FS_IOC_SETFLAGS
		InSize:   4,
	}, ioctlInBuf, &ioctlOut, nil))

	// Unlink 处于打开状态的 NodeId=10（指向 "hardlink-renamed"），并在最后句柄 Release 时触发 ReclaimOrphan
	require.Equal(t, fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "hardlink-renamed"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh})

	writeCalls := client.takeCalls()
	seenProcs := make(map[string]bool)
	for _, call := range writeCalls {
		require.Equal(t, diskID, call.diskID, "RPC %s 必须携带 DiskId", call.procedure)
		if call.procedure == "Lookup" || call.procedure == "GetAttr" {
			require.Zero(t, call.epoch, "辅助只读 RPC %s 不得携带写 Epoch", call.procedure)
			continue
		}
		seenProcs[call.procedure] = true
		require.Equal(t, epoch, call.epoch, "写 RPC %s 必须携带静态 Epoch", call.procedure)
	}

	expected16WriteRPCs := []string{
		"SetAttr", "Mknod", "Mkdir", "Symlink", "Link", "Unlink", "Rmdir", "Rename",
		"Create", "Write", "Fsync", "FsyncDir", "Fallocate", "SetXattr", "RemoveXattr", "ReclaimOrphan",
	}
	for _, proc := range expected16WriteRPCs {
		require.True(t, seenProcs[proc], "必须覆盖写 RPC %s", proc)
	}
}

func TestRawFS_StaleEpochReturnsEBUSYWithoutReconnect(t *testing.T) {
	staleErrWithInfo := connect.NewError(connect.CodeFailedPrecondition, errors.New("stale epoch"))
	detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{
		Reason: "EBUSY",
		Domain: "bowl.storage.v1",
	})
	require.NoError(t, err)
	staleErrWithInfo.AddDetail(detail)

	bareFailedPrecondition := connect.NewError(connect.CodeFailedPrecondition, errors.New("failed precondition"))

	for _, tc := range []struct {
		name string
		err  error
	}{
		{name: "ErrorInfo_EBUSY", err: staleErrWithInfo},
		{name: "Bare_CodeFailedPrecondition", err: bareFailedPrecondition},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &epochAuditClient{}
			fs := fusefs.NewRawFS(client, fusefs.Config{
				DiskID:   "disk-fenced",
				Epoch:    1,
				RootIno:  1,
				DirectIO: true,
			})
			t.Cleanup(fs.OnUnmount)

			var openOut fuse.OpenOut
			require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 10}, Flags: syscall.O_RDWR}, &openOut))

			client.writeErr = tc.err

			var entryOut fuse.EntryOut
			var attrOut fuse.AttrOut
			var createOut fuse.CreateOut

			require.Equal(t, fuse.Status(syscall.EBUSY), fs.SetAttr(nil, &fuse.SetAttrIn{SetAttrInCommon: fuse.SetAttrInCommon{InHeader: fuse.InHeader{NodeId: 10}, Valid: fuse.FATTR_MODE, Mode: 0o600}}, &attrOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Mknod(nil, &fuse.MknodIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: syscall.S_IFREG | 0o644}, "n", &entryOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0o755}, "d", &entryOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Symlink(nil, &fuse.InHeader{NodeId: 1}, "/t", "s", &entryOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: 10}, "l", &entryOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "u"))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Rmdir(nil, &fuse.InHeader{NodeId: 1}, "r"))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Rename(nil, &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 1}, Newdir: 1}, "a", "b"))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o644}, "c", &createOut))

			_, st := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Size: 4}, []byte("data"))
			require.Equal(t, fuse.Status(syscall.EBUSY), st)
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh}))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Fallocate(nil, &fuse.FallocateIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh, Offset: 0, Length: 4096}))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.SetXAttr(nil, &fuse.SetXAttrIn{InHeader: fuse.InHeader{NodeId: 10}}, "user.k", []byte("v")))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.RemoveXAttr(nil, &fuse.InHeader{NodeId: 10}, "user.k"))

			var tmpOut fuse.CreateOut
			require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &tmpOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh}))
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: tmpOut.NodeId}, Fh: tmpOut.Fh})

			var dirOpenOut fuse.OpenOut
			require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &dirOpenOut))
			require.Equal(t, fuse.Status(syscall.EBUSY), fs.FsyncDir(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dirOpenOut.Fh}))
			fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dirOpenOut.Fh})

			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 10}, Fh: openOut.Fh})

			client.mu.Lock()
			require.Zero(t, client.mountCalls, "遇到旧 Epoch 围栏拒绝时绝不触发后台重连或重新调用 MountDisk")
			client.mu.Unlock()
		})
	}
}
