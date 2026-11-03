package fusefs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/session"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type loopClient struct {
	storagev1connect.StorageServiceClient
	renewFn     func(*storagev1.RenewSessionRequest) (*storagev1.RenewSessionResponse, error)
	lookupFn    func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error)
	getAttrFn   func(*storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error)
	dirFn       func(*storagev1.ReadDirPlusRequest) (*storagev1.ReadDirPlusResponse, error)
	createFn    func(*storagev1.CreateRequest) (*storagev1.CreateResponse, error)
	renameFn    func(*storagev1.RenameRequest) (*storagev1.RenameResponse, error)
	mknodFn     func(*storagev1.MknodRequest) (*storagev1.MknodResponse, error)
	setAttrFn   func(*storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error)
	readFn      func(*storagev1.ReadRequest) (*storagev1.ReadResponse, error)
	writeFn     func(*storagev1.WriteRequest) (*storagev1.WriteResponse, error)
	linkFn      func(*storagev1.LinkRequest) (*storagev1.LinkResponse, error)
	fallocateFn func(*storagev1.FallocateRequest) (*storagev1.FallocateResponse, error)
	fsyncFn     func(*storagev1.FsyncRequest) (*storagev1.FsyncResponse, error)
	unlinkFn    func(*storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error)
	reclaimFn   func(*storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error)
}

func (m *loopClient) RenewSession(_ context.Context, r *connect.Request[storagev1.RenewSessionRequest]) (*connect.Response[storagev1.RenewSessionResponse], error) {
	v, e := m.renewFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) CloseSession(_ context.Context, _ *connect.Request[storagev1.CloseSessionRequest]) (*connect.Response[storagev1.CloseSessionResponse], error) {
	return connect.NewResponse(&storagev1.CloseSessionResponse{}), nil
}
func (m *loopClient) Lookup(_ context.Context, r *connect.Request[storagev1.LookupRequest]) (*connect.Response[storagev1.LookupResponse], error) {
	v, e := m.lookupFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) GetAttr(_ context.Context, r *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	v, e := m.getAttrFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) ReadDirPlus(_ context.Context, r *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	v, e := m.dirFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Create(_ context.Context, r *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	v, e := m.createFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Rename(_ context.Context, r *connect.Request[storagev1.RenameRequest]) (*connect.Response[storagev1.RenameResponse], error) {
	v, e := m.renameFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Mknod(_ context.Context, r *connect.Request[storagev1.MknodRequest]) (*connect.Response[storagev1.MknodResponse], error) {
	v, e := m.mknodFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) SetAttr(_ context.Context, r *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	v, e := m.setAttrFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Read(_ context.Context, r *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	v, e := m.readFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Write(_ context.Context, r *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	v, e := m.writeFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Link(_ context.Context, r *connect.Request[storagev1.LinkRequest]) (*connect.Response[storagev1.LinkResponse], error) {
	v, e := m.linkFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Fallocate(_ context.Context, r *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	v, e := m.fallocateFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Fsync(_ context.Context, r *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	v, e := m.fsyncFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) Unlink(_ context.Context, r *connect.Request[storagev1.UnlinkRequest]) (*connect.Response[storagev1.UnlinkResponse], error) {
	v, e := m.unlinkFn(r.Msg)
	return connect.NewResponse(v), e
}
func (m *loopClient) ReclaimOrphan(_ context.Context, r *connect.Request[storagev1.ReclaimOrphanRequest]) (*connect.Response[storagev1.ReclaimOrphanResponse], error) {
	v, e := m.reclaimFn(r.Msg)
	return connect.NewResponse(v), e
}

func Test缓存目录流与本地句柄零RPC(t *testing.T) {
	calls := 0
	root := testInode(100, syscall.S_IFDIR|0o755, 2)
	file := testInode(200, syscall.S_IFREG|0o644, 1)
	client := &loopClient{
		getAttrFn: func(r *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			calls++
			require.Equal(t, uint64(100), r.Ino)
			return &storagev1.GetAttrResponse{Inode: root}, nil
		},
		dirFn: func(r *storagev1.ReadDirPlusRequest) (*storagev1.ReadDirPlusResponse, error) {
			calls++
			require.Zero(t, r.Fh)
			return &storagev1.ReadDirPlusResponse{Entries: []*storagev1.DirEntryPlus{
				{Ino: 100, Name: []byte("."), NextOffset: 1, Inode: root},
				{Ino: 100, Name: []byte(".."), NextOffset: 2, Inode: root},
				{Ino: 200, Name: []byte("file"), NextOffset: 3, Inode: file},
			}}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 100})
	defer fs.OnUnmount()
	var dir fuse.OpenOut
	require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &dir))
	in := &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dir.Fh}
	require.Equal(t, fuse.OK, fs.ReadDir(nil, in, fuse.NewDirEntryList(make([]byte, 4096), 0)))
	require.Equal(t, 2, calls)
	var entry fuse.EntryOut
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &entry))
	require.Equal(t, uint64(200), entry.NodeId)
	require.Equal(t, fuse.ENOENT, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "missing", &entry))
	var attr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 200}}, &attr))
	var opened fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 200}, Flags: syscall.O_RDONLY}, &opened))
	_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 200}, Fh: opened.Fh}, []byte("x"))
	require.Equal(t, fuse.EBADF, status)
	require.Equal(t, fuse.Status(syscall.EISDIR), fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &fuse.OpenOut{}))
	require.Equal(t, fuse.Status(syscall.ENOTDIR), fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 200}}, &fuse.OpenOut{}))
	require.Equal(t, fuse.OK, fs.ReadDirPlus(nil, in, fuse.NewDirEntryList(make([]byte, 4096), 0)))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 200}, Fh: opened.Fh})
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dir.Fh})
	require.Equal(t, 2, calls)
}

func Test短命Tmpfile始终保存在内存(t *testing.T) {
	fs := NewRawFS(nil, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	for _, flags := range []uint32{syscall.O_RDWR, syscall.O_RDWR | syscall.O_EXCL} {
		var out fuse.CreateOut
		require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: flags, Mode: 0o600}, &out))
		ino := out.NodeId
		require.NotZero(t, ino>>63)
		written, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out.Fh, Offset: 2}, []byte("abc"))
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(3), written)
		result, status := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out.Fh, Size: 5}, nil)
		require.Equal(t, fuse.OK, status)
		data, status := result.Bytes(nil)
		require.Equal(t, fuse.OK, status)
		require.Equal(t, []byte{0, 0, 'a', 'b', 'c'}, data)
		set := &fuse.SetAttrIn{}
		set.NodeId, set.Valid, set.Size = ino, fuse.FATTR_SIZE, 3
		var attr fuse.AttrOut
		require.Equal(t, fuse.OK, fs.SetAttr(nil, set, &attr))
		require.Equal(t, uint64(3), attr.Size)
		require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out.Fh}))
		if flags&syscall.O_EXCL != 0 {
			require.Equal(t, fuse.ENOENT, fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: ino}, "named", &fuse.EntryOut{}))
		}
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: out.Fh})
		require.Nil(t, fs.tmpFile(ino))
	}
}

func TestTmpfile具名化保存数据和节点映射(t *testing.T) {
	var contents []byte
	inode := testInode(300, syscall.S_IFREG|0o600, 1)
	client := &loopClient{
		mknodFn: func(r *storagev1.MknodRequest) (*storagev1.MknodResponse, error) {
			require.Equal(t, "named", string(r.Name))
			return &storagev1.MknodResponse{Inode: inode}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			require.Zero(t, r.Fh)
			require.Equal(t, uint64(300), r.Ino)
			contents = append(contents, r.Data...)
			return &storagev1.WriteResponse{Size: uint32(len(r.Data)), Inode: inode}, nil
		},
		setAttrFn: func(r *storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			inode.Attr.Size = r.GetSize()
			return &storagev1.SetAttrResponse{Inode: inode}, nil
		},
		readFn: func(r *storagev1.ReadRequest) (*storagev1.ReadResponse, error) {
			require.Equal(t, uint64(300), r.Ino)
			require.Zero(t, r.Fh)
			return &storagev1.ReadResponse{Data: contents}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &out))
	_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}, []byte("hello"))
	require.Equal(t, fuse.OK, status)
	var entry fuse.EntryOut
	link := &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: out.NodeId}
	require.Equal(t, fuse.OK, fs.Link(nil, link, "named", &entry))
	require.Equal(t, out.NodeId, entry.NodeId)
	require.Equal(t, []byte("hello"), contents)
	require.Equal(t, fuse.ENOENT, fs.Link(nil, link, "again", &fuse.EntryOut{}))
	result, status := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh, Size: 5}, nil)
	require.Equal(t, fuse.OK, status)
	data, status := result.Bytes(nil)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, []byte("hello"), data)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
}

func TestUnlink最后关闭后回收并重试(t *testing.T) {
	var mu sync.Mutex
	attempts := 0
	inode := testInode(22, syscall.S_IFREG|0o600, 1)
	client := &loopClient{
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return &storagev1.LookupResponse{Inode: inode}, nil
		},
		unlinkFn: func(r *storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			require.NotNil(t, r.OpenInClient)
			require.True(t, r.GetOpenInClient())
			return &storagev1.UnlinkResponse{}, nil
		},
		reclaimFn: func(r *storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			require.Equal(t, uint64(22), r.Ino)
			mu.Lock()
			defer mu.Unlock()
			attempts++
			if attempts == 1 {
				return nil, connect.NewError(connect.CodeUnavailable, errors.New("临时不可用"))
			}
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1, ReleaseRetryInterval: time.Millisecond})
	defer fs.OnUnmount()
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &fuse.EntryOut{}))
	var a, b fuse.OpenOut
	in := &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 22}, Flags: syscall.O_RDWR}
	require.Equal(t, fuse.OK, fs.Open(nil, in, &a))
	require.Equal(t, fuse.OK, fs.Open(nil, in, &b))
	require.Equal(t, fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "file"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 22}, Fh: a.Fh})
	mu.Lock()
	require.Zero(t, attempts)
	mu.Unlock()
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 22}, Fh: b.Fh})
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return attempts == 2
	}, time.Second, time.Millisecond)
}

func TestTmpfile同步后具名化取消孤儿回收(t *testing.T) {
	inode := testInode(300, syscall.S_IFREG|0o600, 1)
	var unlinked, synced, linked, reclaimed int
	client := &loopClient{
		mknodFn: func(r *storagev1.MknodRequest) (*storagev1.MknodResponse, error) {
			require.Contains(t, string(r.Name), ".bowl-tmp-")
			return &storagev1.MknodResponse{Inode: inode}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			require.Equal(t, []byte("data"), r.Data)
			return &storagev1.WriteResponse{Size: uint32(len(r.Data)), Inode: inode}, nil
		},
		setAttrFn: func(r *storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			inode.Attr.Size = r.GetSize()
			return &storagev1.SetAttrResponse{Inode: inode}, nil
		},
		unlinkFn: func(r *storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			require.True(t, r.GetOpenInClient())
			unlinked++
			return &storagev1.UnlinkResponse{}, nil
		},
		fsyncFn: func(r *storagev1.FsyncRequest) (*storagev1.FsyncResponse, error) {
			require.Equal(t, uint64(300), r.Ino)
			require.Zero(t, r.Fh)
			synced++
			return &storagev1.FsyncResponse{}, nil
		},
		linkFn: func(r *storagev1.LinkRequest) (*storagev1.LinkResponse, error) {
			require.Equal(t, uint64(300), r.Ino)
			require.Equal(t, "named", string(r.NewName))
			linked++
			return &storagev1.LinkResponse{Inode: inode}, nil
		},
		reclaimFn: func(*storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			reclaimed++
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &out))
	_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}, []byte("data"))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fuse.OK, fs.Fsync(nil, &fuse.FsyncIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}))
	require.Equal(t, 1, unlinked)
	require.Equal(t, 1, synced)
	require.Equal(t, fuse.OK, fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: out.NodeId}, "named", &fuse.EntryOut{}))
	require.Equal(t, 1, linked)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	require.Zero(t, reclaimed)
}

func Test创建更新负缓存和写入更新属性(t *testing.T) {
	lookups := 0
	inode := testInode(20, syscall.S_IFREG|0o600, 1)
	client := &loopClient{
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			lookups++
			return nil, connect.NewError(connect.CodeNotFound, errors.New("不存在"))
		},
		createFn: func(r *storagev1.CreateRequest) (*storagev1.CreateResponse, error) {
			require.Equal(t, "file", string(r.Name))
			return &storagev1.CreateResponse{Inode: inode, Fh: 987}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			require.Zero(t, r.Fh)
			inode.Attr.Size = 4
			return &storagev1.WriteResponse{Inode: inode, Size: 4}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	parent := &fuse.InHeader{NodeId: 1}
	require.Equal(t, fuse.ENOENT, fs.Lookup(nil, parent, "file", &fuse.EntryOut{}))
	require.Equal(t, fuse.ENOENT, fs.Lookup(nil, parent, "file", &fuse.EntryOut{}))
	require.Equal(t, 1, lookups)
	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{InHeader: *parent, Flags: syscall.O_RDWR, Mode: 0o600}, "file", &created))
	require.NotEqual(t, uint64(987), created.Fh)
	var entry fuse.EntryOut
	require.Equal(t, fuse.OK, fs.Lookup(nil, parent, "file", &entry))
	require.Equal(t, uint64(20), entry.NodeId)
	require.Equal(t, 1, lookups)
	_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 20}, Fh: created.Fh}, []byte("data"))
	require.Equal(t, fuse.OK, status)
	var attr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 20}}, &attr))
	require.Equal(t, uint64(4), attr.Size)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 20}, Fh: created.Fh})
}

func TestRename覆盖打开文件后按链接数回收(t *testing.T) {
	for _, nlink := range []uint32{1, 2} {
		t.Run(string(rune('0'+nlink)), func(t *testing.T) {
			source := testInode(21, syscall.S_IFREG|0o600, 1)
			target := testInode(22, syscall.S_IFREG|0o600, nlink)
			entries := map[string]*storagev1.Inode{"src": source, "dst": target}
			reclaims := 0
			client := &loopClient{
				lookupFn: func(r *storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
					if inode := entries[string(r.Name)]; inode != nil {
						return &storagev1.LookupResponse{Inode: inode}, nil
					}
					return nil, connect.NewError(connect.CodeNotFound, errors.New("不存在"))
				},
				renameFn: func(r *storagev1.RenameRequest) (*storagev1.RenameResponse, error) {
					require.True(t, r.GetTargetOpenInClient())
					entries[string(r.NewName)] = entries[string(r.OldName)]
					delete(entries, string(r.OldName))
					return &storagev1.RenameResponse{}, nil
				},
				reclaimFn: func(r *storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
					require.Equal(t, uint64(22), r.Ino)
					reclaims++
					return &storagev1.ReclaimOrphanResponse{}, nil
				},
			}
			fs := NewRawFS(client, nil, Config{RootIno: 1})
			defer fs.OnUnmount()
			header := &fuse.InHeader{NodeId: 1}
			require.Equal(t, fuse.OK, fs.Lookup(nil, header, "src", &fuse.EntryOut{}))
			require.Equal(t, fuse.OK, fs.Lookup(nil, header, "dst", &fuse.EntryOut{}))
			var out fuse.OpenOut
			require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 22}, Flags: syscall.O_RDWR}, &out))
			require.Equal(t, fuse.OK, fs.Rename(nil, &fuse.RenameIn{InHeader: *header, Newdir: 1}, "src", "dst"))
			require.Equal(t, fuse.ENOENT, fs.Lookup(nil, header, "src", &fuse.EntryOut{}))
			var entry fuse.EntryOut
			require.Equal(t, fuse.OK, fs.Lookup(nil, header, "dst", &entry))
			require.Equal(t, uint64(21), entry.NodeId)
			fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 22}, Fh: out.Fh})
			if nlink == 1 {
				require.Equal(t, 1, reclaims)
			} else {
				require.Zero(t, reclaims)
			}
		})
	}
}

func TestUnlink与最后关闭并发不丢回收(t *testing.T) {
	inode := testInode(22, syscall.S_IFREG|0o600, 1)
	started, proceed := make(chan struct{}), make(chan struct{})
	var reclaimed atomic.Int32
	client := &loopClient{
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return &storagev1.LookupResponse{Inode: inode}, nil
		},
		unlinkFn: func(r *storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			if !r.GetOpenInClient() {
				return nil, errors.New("未报告打开句柄")
			}
			close(started)
			<-proceed
			return &storagev1.UnlinkResponse{}, nil
		},
		reclaimFn: func(*storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			reclaimed.Add(1)
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &fuse.EntryOut{}))
	var out fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 22}}, &out))
	unlinkDone := make(chan fuse.Status, 1)
	go func() { unlinkDone <- fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "file") }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("Unlink 未启动")
	}
	releaseDone := make(chan struct{})
	go func() {
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 22}, Fh: out.Fh})
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
		t.Fatal("Unlink 完成前关闭了句柄")
	case <-time.After(20 * time.Millisecond):
	}
	close(proceed)
	require.Equal(t, fuse.OK, <-unlinkDone)
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		t.Fatal("最后关闭未完成")
	}
	require.Equal(t, int32(1), reclaimed.Load())
}

func Test会话过期不能重新写入旧属性缓存(t *testing.T) {
	started, proceed := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	client := &loopClient{getAttrFn: func(r *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
		n := calls.Add(1)
		inode := testInode(r.Ino, syscall.S_IFREG|0o600, 1)
		inode.Attr.Size = uint64(n)
		if n == 1 {
			close(started)
			<-proceed
		}
		return &storagev1.GetAttrResponse{Inode: inode}, nil
	}}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 2}}, &fuse.AttrOut{}) }()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("GetAttr 未启动")
	}
	fs.onSessionExpired(7)
	close(proceed)
	require.Equal(t, fuse.Status(syscall.ESTALE), <-done)
	var out fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 2}}, &out))
	require.Equal(t, uint64(2), out.Attr.Size)
	require.Equal(t, int32(2), calls.Load())
}

func Test失效会话的孤儿不会用新会话回收(t *testing.T) {
	var unlinkedSID uint64
	var reclaims atomic.Int32
	inode := testInode(22, syscall.S_IFREG|0o600, 1)
	client := &loopClient{
		renewFn: func(*storagev1.RenewSessionRequest) (*storagev1.RenewSessionResponse, error) {
			return &storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(100 * time.Millisecond))}, nil
		},
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return &storagev1.LookupResponse{Inode: inode}, nil
		},
		unlinkFn: func(r *storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			unlinkedSID = r.GetSessionId()
			return &storagev1.UnlinkResponse{}, nil
		},
		reclaimFn: func(*storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			reclaims.Add(1)
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	mgr, err := session.New(context.Background(), client, session.Config{})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()
	oldSID := mgr.CurrentSessionID()
	fs := NewRawFS(client, mgr, Config{RootIno: 1})
	defer fs.OnUnmount()
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &fuse.EntryOut{}))
	var out fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 22}}, &out))
	require.Equal(t, fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "file"))
	require.Equal(t, oldSID, unlinkedSID)
	require.Eventually(t, func() bool {
		_, status := fs.ResolveHandle(out.Fh)
		return status == fuse.EBADF
	}, time.Second, 10*time.Millisecond)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 22}, Fh: out.Fh})
	require.Zero(t, reclaims.Load())
}

func TestOpen截断与追加写入使用本地句柄(t *testing.T) {
	inode := testInode(25, syscall.S_IFREG|0o600, 1)
	inode.Attr.Size = 5
	var truncates int
	var offsets []uint64
	client := &loopClient{
		getAttrFn: func(*storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			return &storagev1.GetAttrResponse{Inode: cloneInode(inode)}, nil
		},
		setAttrFn: func(r *storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			truncates++
			require.NotNil(t, r.Size)
			require.Zero(t, r.GetSize())
			require.Nil(t, r.Fh)
			inode.Attr.Size = 0
			return &storagev1.SetAttrResponse{Inode: cloneInode(inode)}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			offsets = append(offsets, r.Offset)
			require.Zero(t, r.Fh)
			inode.Attr.Size = r.Offset + uint64(len(r.Data))
			return &storagev1.WriteResponse{Size: uint32(len(r.Data)), Inode: cloneInode(inode)}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 25}, Flags: syscall.O_WRONLY | syscall.O_APPEND | syscall.O_TRUNC}, &out))
	require.Equal(t, 1, truncates)
	for _, data := range []string{"ab", "cde"} {
		written, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 25}, Fh: out.Fh, Offset: 100}, []byte(data))
		require.Equal(t, fuse.OK, status)
		require.Equal(t, uint32(len(data)), written)
	}
	require.Equal(t, []uint64{0, 2}, offsets)
	_, status := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 25}, Fh: out.Fh, Size: 1}, nil)
	require.Equal(t, fuse.EBADF, status)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 25}, Fh: out.Fh})
}

func Test多页目录流缓存及续读(t *testing.T) {
	calls := 0
	client := &loopClient{
		getAttrFn: func(r *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			return &storagev1.GetAttrResponse{Inode: testInode(r.Ino, syscall.S_IFDIR|0o755, 2)}, nil
		},
		dirFn: func(r *storagev1.ReadDirPlusRequest) (*storagev1.ReadDirPlusResponse, error) {
			calls++
			var entries []*storagev1.DirEntryPlus
			end := min(r.Offset+uint64(r.Limit), uint64(515))
			for i := r.Offset; i < end; i++ {
				name := fmt.Sprintf("file-%03d", i)
				ino := i + 10
				entries = append(entries, &storagev1.DirEntryPlus{Ino: ino, Name: []byte(name), NextOffset: i + 1, Inode: testInode(ino, syscall.S_IFREG|0o600, 1)})
			}
			return &storagev1.ReadDirPlusResponse{Entries: entries}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1, NamespaceCacheCapacity: 1024})
	defer fs.OnUnmount()
	var dir fuse.OpenOut
	require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 1}}, &dir))
	input := &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dir.Fh}
	first := fuse.NewDirEntryList(make([]byte, 64), 0)
	require.Equal(t, fuse.OK, fs.ReadDir(nil, input, first))
	require.Equal(t, 2, calls)
	input.Offset = first.Offset
	next := fuse.NewDirEntryList(make([]byte, 2048), input.Offset)
	require.Equal(t, fuse.OK, fs.ReadDirPlus(nil, input, next))
	require.Greater(t, next.Offset, first.Offset)
	var entry fuse.EntryOut
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file-514", &entry))
	require.Equal(t, uint64(524), entry.NodeId)
	var attr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 524}}, &attr))
	require.Equal(t, 2, calls)
	fs.ReleaseDir(&fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 1}, Fh: dir.Fh})
}

func TestTmpfile预分配上云并在关闭后回收(t *testing.T) {
	inode := testInode(300, syscall.S_IFREG|0o600, 1)
	var allocations, reclaimed int
	client := &loopClient{
		mknodFn: func(*storagev1.MknodRequest) (*storagev1.MknodResponse, error) {
			return &storagev1.MknodResponse{Inode: inode}, nil
		},
		setAttrFn: func(*storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			return &storagev1.SetAttrResponse{Inode: inode}, nil
		},
		unlinkFn: func(r *storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			require.True(t, r.GetOpenInClient())
			return &storagev1.UnlinkResponse{}, nil
		},
		fallocateFn: func(r *storagev1.FallocateRequest) (*storagev1.FallocateResponse, error) {
			require.Equal(t, uint64(300), r.Ino)
			require.Zero(t, r.Fh)
			require.Equal(t, uint64(4096), r.Length)
			allocations++
			return &storagev1.FallocateResponse{}, nil
		},
		reclaimFn: func(r *storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			require.Equal(t, uint64(300), r.Ino)
			reclaimed++
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &out))
	require.Equal(t, fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh, Length: 4096}))
	require.Equal(t, 1, allocations)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	require.Equal(t, 1, reclaimed)
}

func TestTmpfile内核引用和句柄都释放后清除映射(t *testing.T) {
	inode := testInode(300, syscall.S_IFREG|0o600, 1)
	client := &loopClient{
		mknodFn: func(*storagev1.MknodRequest) (*storagev1.MknodResponse, error) {
			return &storagev1.MknodResponse{Inode: inode}, nil
		},
		setAttrFn: func(*storagev1.SetAttrRequest) (*storagev1.SetAttrResponse, error) {
			return &storagev1.SetAttrResponse{Inode: inode}, nil
		},
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return &storagev1.LookupResponse{Inode: inode}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &out))
	require.Equal(t, fuse.OK, fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: out.NodeId}, "file", &fuse.EntryOut{}))
	fs.Forget(out.NodeId, 2)
	require.NotNil(t, fs.tmpFile(out.NodeId), "活动句柄仍需使用映射")
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
	require.Nil(t, fs.tmpFile(out.NodeId))
	var entry fuse.EntryOut
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &entry))
	require.Equal(t, uint64(300), entry.NodeId)
}

func TestTmpfile具名化写入失败后保留本地数据(t *testing.T) {
	inode := testInode(300, syscall.S_IFREG|0o600, 1)
	cleanup := 0
	client := &loopClient{
		mknodFn: func(*storagev1.MknodRequest) (*storagev1.MknodResponse, error) {
			return &storagev1.MknodResponse{Inode: inode}, nil
		},
		writeFn: func(*storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			return nil, connect.NewError(connect.CodeResourceExhausted, errors.New("空间不足"))
		},
		unlinkFn: func(r *storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			require.Equal(t, "file", string(r.Name))
			require.NotNil(t, r.OpenInClient)
			require.False(t, r.GetOpenInClient())
			cleanup++
			return &storagev1.UnlinkResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	var out fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Tmpfile(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_RDWR, Mode: 0o600}, &out))
	_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh}, []byte("data"))
	require.Equal(t, fuse.OK, status)
	require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Link(nil, &fuse.LinkIn{InHeader: fuse.InHeader{NodeId: 1}, Oldnodeid: out.NodeId}, "file", &fuse.EntryOut{}))
	require.Equal(t, 1, cleanup)
	result, status := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh, Size: 4}, nil)
	require.Equal(t, fuse.OK, status)
	data, status := result.Bytes(nil)
	require.Equal(t, fuse.OK, status)
	require.Equal(t, []byte("data"), data)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: out.NodeId}, Fh: out.Fh})
}

func Test孤儿回收请求保留磁盘分支与原会话(t *testing.T) {
	const disk = "00000000-0000-0000-0000-000000000001"
	inode := testInode(22, syscall.S_IFREG|0o600, 1)
	var req *storagev1.ReclaimOrphanRequest
	client := &loopClient{
		renewFn: func(*storagev1.RenewSessionRequest) (*storagev1.RenewSessionResponse, error) {
			return &storagev1.RenewSessionResponse{ExpiresAt: timestamppb.New(time.Now().Add(time.Minute))}, nil
		},
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return &storagev1.LookupResponse{Inode: inode}, nil
		},
		unlinkFn: func(*storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			return &storagev1.UnlinkResponse{}, nil
		},
		reclaimFn: func(r *storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			req = r
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	mgr, err := session.New(context.Background(), client, session.Config{DiskID: disk})
	require.NoError(t, err)
	defer func() { _ = mgr.Close() }()
	fs := NewRawFS(client, mgr, Config{RootIno: 1, DiskID: disk, BranchName: "feature"})
	defer fs.OnUnmount()
	require.Equal(t, fuse.OK, fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "file", &fuse.EntryOut{}))
	var out fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 22}}, &out))
	require.Equal(t, fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "file"))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 22}, Fh: out.Fh})
	require.NotNil(t, req)
	require.Equal(t, uint64(22), req.Ino)
	require.Equal(t, disk, req.DiskId)
	require.Equal(t, "feature", req.GetBranchName())
	require.Equal(t, mgr.CurrentSessionID(), req.SessionId)
}
