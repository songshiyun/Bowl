package fusefs

import (
	"context"
	"encoding/binary"
	"errors"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/session"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func Test写入关闭和卸载同步(t *testing.T) {
	var syncRequests []*storagev1.FsyncRequest
	var syncErr error
	var writeCalls int
	client := &loopClient{
		renewFn: func(*storagev1.RenewSessionRequest) (*storagev1.RenewSessionResponse, error) {
			return &storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			writeCalls++
			if len(r.Data) > 0 && r.Offset == MaxRPCChunkSize {
				return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("空间不足"))
			}
			return &storagev1.WriteResponse{Size: uint32(len(r.Data))}, nil
		},
		fsyncFn: func(r *storagev1.FsyncRequest) (*storagev1.FsyncResponse, error) {
			syncRequests = append(syncRequests, r)
			if syncErr != nil {
				return nil, syncErr
			}
			return &storagev1.FsyncResponse{}, nil
		},
	}
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	defer mgr.Close()
	fs := NewRawFS(client, mgr, Config{RootIno: 1})
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2, 9, syscall.O_RDWR, false))
	write := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}
	flush := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}

	written, status := fs.Write(nil, write, make([]byte, MaxRPCChunkSize+1))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, uint32(MaxRPCChunkSize), written)
	require.Equal(t, 2, writeCalls)
	syncErr = connect.NewError(connect.CodeResourceExhausted, errors.New("空间不足"))
	require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Flush(nil, flush))
	syncErr = nil
	require.Equal(t, fuse.OK, fs.Flush(nil, flush))
	require.Len(t, syncRequests, 2)
	for _, r := range syncRequests {
		require.Equal(t, uint64(2), r.Ino)
		require.Equal(t, mgr.CurrentSessionID(), r.GetSessionId())
	}
	require.Equal(t, fuse.OK, fs.Flush(nil, flush))
	require.Len(t, syncRequests, 2)

	_, status = fs.Write(nil, write, []byte("x"))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9, FsyncFlags: 1}))
	require.True(t, syncRequests[2].GetDatasync())
	require.Equal(t, fuse.OK, fs.Flush(nil, flush))
	require.Len(t, syncRequests, 3)

	_, status = fs.Write(nil, write, []byte("y"))
	require.Equal(t, fuse.OK, status)
	fs.OnUnmount()
	require.Len(t, syncRequests, 4)
	require.Equal(t, mgr.CurrentSessionID(), syncRequests[3].GetSessionId())
	require.False(t, fs.handles.AnyOpen())
	require.Empty(t, fs.dirtyInodes)
}

func Test匿名文件关闭时同步(t *testing.T) {
	inode := testInode(300, syscall.S_IFREG|0o600, 1)
	var events []string
	client := &loopClient{
		mknodFn: func(*storagev1.MknodRequest) (*storagev1.MknodResponse, error) {
			events = append(events, "创建")
			return &storagev1.MknodResponse{Inode: inode}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			events = append(events, "写入")
			return &storagev1.WriteResponse{Size: uint32(len(r.Data))}, nil
		},
		setAttrFn: func(*storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			return &storagev1.SetAttrResponse{Inode: inode}, nil
		},
		unlinkFn: func(*storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			events = append(events, "解链")
			return &storagev1.UnlinkResponse{}, nil
		},
		fsyncFn: func(r *storagev1.FsyncRequest) (*storagev1.FsyncResponse, error) {
			require.Equal(t, uint64(300), r.Ino)
			events = append(events, "同步")
			return &storagev1.FsyncResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &out))
	_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}, []byte("内容"))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}))
	require.Equal(t, []string{"创建", "写入", "解链", "同步"}, events)
}

func TestIoctl更新属性清除缓存并携带会话(t *testing.T) {
	flags := uint32(0)
	var sid uint64
	client := &loopClient{
		renewFn: func(*storagev1.RenewSessionRequest) (*storagev1.RenewSessionResponse, error) {
			return &storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}, nil
		},
		getAttrFn: func(r *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			require.Equal(t, sid, r.GetSessionId())
			inode := testInode(r.Ino, syscall.S_IFREG|0o644, 1)
			inode.Attr.Flags = flags
			return &storagev1.GetAttrResponse{Inode: inode}, nil
		},
		setAttrFn: func(r *storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			require.Equal(t, sid, r.GetSessionId())
			flags = r.GetFlags()
			return &storagev1.SetAttrResponse{}, nil
		},
	}
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	defer mgr.Close()
	sid = mgr.CurrentSessionID()
	fs := NewRawFS(client, mgr, Config{RootIno: 1})
	defer fs.OnUnmount()
	fs.cache.putInode(2, testInode(2, syscall.S_IFREG|0o644, 1), 0, 0)
	for _, cmd := range []uint32{FS_IOC_SETFLAGS, FS_IOC_FSSETXATTR} {
		buf := make([]byte, 28)
		if cmd == FS_IOC_SETFLAGS {
			binary.LittleEndian.PutUint32(buf, FS_APPEND_FL)
		} else {
			binary.LittleEndian.PutUint32(buf, FS_XFLAG_APPEND)
		}
		require.Equal(t, fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: 2}, Cmd: cmd}, buf, nil, nil))
		_, cached := fs.cache.inode(2)
		require.False(t, cached)
		var out fuse.AttrOut
		require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 2}}, &out))
	}
	for _, cmd := range []uint32{FS_IOC_GETFLAGS, FS_IOC_FSGETXATTR} {
		require.Equal(t, fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{InHeader: fuse.InHeader{NodeId: 2}, Cmd: cmd}, nil, nil, make([]byte, 28)))
	}
}

type sessionRPCClient struct {
	*loopClient
	statFSFn   func(*storagev1.StatFSRequest) (*storagev1.StatFSResponse, error)
	readlinkFn func(*storagev1.ReadlinkRequest) (*storagev1.ReadlinkResponse, error)
}

func (c *sessionRPCClient) StatFS(_ context.Context, req *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	resp, err := c.statFSFn(req.Msg)
	return connect.NewResponse(resp), err
}

func (c *sessionRPCClient) Readlink(_ context.Context, req *connect.Request[storagev1.ReadlinkRequest]) (*connect.Response[storagev1.ReadlinkResponse], error) {
	resp, err := c.readlinkFn(req.Msg)
	return connect.NewResponse(resp), err
}

func TestStatFS和Readlink携带分支会话(t *testing.T) {
	var sid uint64
	client := &sessionRPCClient{loopClient: &loopClient{
		renewFn: func(*storagev1.RenewSessionRequest) (*storagev1.RenewSessionResponse, error) {
			return &storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}, nil
		},
	},
		statFSFn: func(r *storagev1.StatFSRequest) (*storagev1.StatFSResponse, error) {
			require.Equal(t, uint64(100), r.Ino)
			require.Equal(t, sid, r.GetSessionId())
			return &storagev1.StatFSResponse{}, nil
		},
		readlinkFn: func(r *storagev1.ReadlinkRequest) (*storagev1.ReadlinkResponse, error) {
			require.Equal(t, uint64(2), r.Ino)
			require.Equal(t, sid, r.GetSessionId())
			return &storagev1.ReadlinkResponse{Target: []byte("目标")}, nil
		},
	}
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	defer mgr.Close()
	sid = mgr.CurrentSessionID()
	fs := NewRawFS(client, mgr, Config{RootIno: 100})
	defer fs.OnUnmount()
	require.Equal(t, fuse.OK, fs.StatFs(nil, &fuse.InHeader{NodeId: 1}, &fuse.StatfsOut{}))
	target, status := fs.Readlink(nil, &fuse.InHeader{NodeId: 2})
	require.Equal(t, fuse.OK, status)
	require.Equal(t, "目标", string(target))
}

func Test同步未实现与失败处理(t *testing.T) {
	var callCount int
	var syncErr error
	client := &loopClient{
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			return &storagev1.WriteResponse{Size: uint32(len(r.Data))}, nil
		},
		fsyncFn: func(r *storagev1.FsyncRequest) (*storagev1.FsyncResponse, error) {
			callCount++
			if syncErr != nil {
				return nil, syncErr
			}
			return &storagev1.FsyncResponse{}, nil
		},
	}
	unmounted := false
	fs := NewRawFS(client, nil, Config{RootIno: 1, OnUnmount: func() { unmounted = true }})
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2, 9, syscall.O_RDWR, false))
	write := &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}
	flush := &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}
	_, status := fs.Write(nil, write, []byte("a"))
	require.Equal(t, fuse.OK, status)
	syncErr = connect.NewError(connect.CodeUnimplemented, errors.New("未实现"))
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}))
	require.Equal(t, fuse.OK, fs.Flush(nil, flush))
	require.Equal(t, fuse.OK, fs.Flush(nil, flush))
	require.Equal(t, 2, callCount)
	_, status = fs.Write(nil, write, []byte("b"))
	require.Equal(t, fuse.OK, status)
	syncErr = connect.NewError(connect.CodeInternal, errors.New("写盘失败"))
	require.Equal(t, fuse.EIO, fs.Flush(nil, flush))
	syncErr = nil
	fs.OnUnmount()
	require.Equal(t, 4, callCount)
	require.True(t, unmounted)
}

func Test卸载等待进行中的写入(t *testing.T) {
	entered := make(chan struct{})
	continueWrite := make(chan struct{})
	writeDone := make(chan fuse.Status, 1)
	synced := make(chan uint64, 1)
	unmounted := make(chan struct{})
	client := &loopClient{
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			close(entered)
			<-continueWrite
			return &storagev1.WriteResponse{Size: uint32(len(r.Data))}, nil
		},
		fsyncFn: func(r *storagev1.FsyncRequest) (*storagev1.FsyncResponse, error) {
			synced <- r.Ino
			return &storagev1.FsyncResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2, 9, syscall.O_RDWR, false))
	go func() {
		_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}, []byte("数据"))
		writeDone <- status
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("写入未开始")
	}
	go func() { fs.OnUnmount(); close(unmounted) }()
	close(continueWrite)
	select {
	case status := <-writeDone:
		require.Equal(t, fuse.OK, status)
	case <-time.After(time.Second):
		t.Fatal("写入未结束")
	}
	select {
	case ino := <-synced:
		require.Equal(t, uint64(2), ino)
	case <-time.After(time.Second):
		t.Fatal("卸载未同步写入")
	}
	select {
	case <-unmounted:
	case <-time.After(time.Second):
		t.Fatal("卸载未结束")
	}
}
