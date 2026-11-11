package fusefs_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

type writeCallRecord struct {
	offset uint64
	length int
}

type fakeStorageClientForWriteBehind struct {
	storagev1connect.UnimplementedStorageServiceHandler

	mu          sync.Mutex
	fileData    []byte
	flags       uint32
	mtimeSec    int64
	writeCalls  []writeCallRecord
	fsyncCalls  int
	staleEpoch  bool
	onWriteHook func(ctx context.Context, offset uint64, data []byte) error
}

func (f *fakeStorageClientForWriteBehind) currentInodeLocked(ino uint64) *storagev1.Inode {
	if ino == 1 {
		return &storagev1.Inode{
			Ino: 1,
			Attr: &storagev1.InodeAttribute{
				Mode:  syscall.S_IFDIR | 0o755,
				Size:  4096,
				Nlink: 2,
			},
		}
	}
	return &storagev1.Inode{
		Ino: ino,
		Attr: &storagev1.InodeAttribute{
			Mode:  syscall.S_IFREG | 0o644,
			Size:  uint64(len(f.fileData)),
			Nlink: 1,
			Flags: f.flags,
			Mtime: &timestamppb.Timestamp{Seconds: f.mtimeSec},
			Ctime: &timestamppb.Timestamp{Seconds: f.mtimeSec},
		},
	}
}

func (f *fakeStorageClientForWriteBehind) Create(_ context.Context, req *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
	return connect.NewResponse(&storagev1.CreateResponse{
		Inode: &storagev1.Inode{
			Ino: 100,
			Attr: &storagev1.InodeAttribute{
				Mode:  syscall.S_IFREG | (req.Msg.GetMode() & 0o7777),
				Size:  0,
				Nlink: 1,
			},
		},
		EntryTimeoutSeconds: 1.0,
		AttrTimeoutSeconds:  1.0,
	}), nil
}

func (f *fakeStorageClientForWriteBehind) Write(ctx context.Context, req *connect.Request[storagev1.WriteRequest]) (*connect.Response[storagev1.WriteResponse], error) {
	off := req.Msg.GetOffset()
	payload := append([]byte(nil), req.Msg.GetData()...)

	f.mu.Lock()
	hook := f.onWriteHook
	f.mu.Unlock()

	if hook != nil {
		if err := hook(ctx, off, payload); err != nil {
			return nil, err
		}
	}

	f.mu.Lock()
	defer f.mu.Unlock()
	if f.flags&fusefs.ProtoFlagImmutable != 0 {
		cerr := connect.NewError(connect.CodePermissionDenied, syscall.EPERM)
		if detail, err := connect.NewErrorDetail(&errdetails.ErrorInfo{Domain: "bowl.storage", Reason: "EPERM"}); err == nil {
			cerr.AddDetail(detail)
		}
		return nil, cerr
	}
	f.writeCalls = append(f.writeCalls, writeCallRecord{offset: off, length: len(payload)})
	end := int(off) + len(payload)
	if end > len(f.fileData) {
		grown := make([]byte, end)
		copy(grown, f.fileData)
		f.fileData = grown
	}
	copy(f.fileData[off:end], payload)
	f.mtimeSec++

	return connect.NewResponse(&storagev1.WriteResponse{
		Size:  uint32(len(payload)),
		Inode: f.currentInodeLocked(req.Msg.GetIno()),
	}), nil
}

func (f *fakeStorageClientForWriteBehind) Read(_ context.Context, req *connect.Request[storagev1.ReadRequest]) (*connect.Response[storagev1.ReadResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	off := int(req.Msg.GetOffset())
	if off >= len(f.fileData) {
		return connect.NewResponse(&storagev1.ReadResponse{}), nil
	}
	end := min(off+int(req.Msg.GetSize()), len(f.fileData))
	return connect.NewResponse(&storagev1.ReadResponse{
		Data: append([]byte(nil), f.fileData[off:end]...),
	}), nil
}

func (f *fakeStorageClientForWriteBehind) GetAttr(_ context.Context, req *connect.Request[storagev1.GetAttrRequest]) (*connect.Response[storagev1.GetAttrResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return connect.NewResponse(&storagev1.GetAttrResponse{
		Inode:              f.currentInodeLocked(req.Msg.GetIno()),
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (f *fakeStorageClientForWriteBehind) ReadDirPlus(_ context.Context, req *connect.Request[storagev1.ReadDirPlusRequest]) (*connect.Response[storagev1.ReadDirPlusResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if req.Msg.GetOffset() > 0 {
		return connect.NewResponse(&storagev1.ReadDirPlusResponse{}), nil
	}
	return connect.NewResponse(&storagev1.ReadDirPlusResponse{
		Entries: []*storagev1.DirEntryPlus{
			{
				Ino:                 100,
				Name:                []byte("seq.bin"),
				NextOffset:          1,
				Inode:               f.currentInodeLocked(100),
				EntryTimeoutSeconds: 1.0,
				AttrTimeoutSeconds:  1.0,
			},
		},
	}), nil
}

func (f *fakeStorageClientForWriteBehind) SetAttr(_ context.Context, req *connect.Request[storagev1.SetAttrRequest]) (*connect.Response[storagev1.SetAttrResponse], error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.staleEpoch {
		return nil, connect.NewError(connect.CodeFailedPrecondition, syscall.EBUSY)
	}
	if req.Msg.Size != nil {
		newSize := int(req.Msg.GetSize())
		if newSize < len(f.fileData) {
			f.fileData = f.fileData[:newSize]
		} else if newSize > len(f.fileData) {
			grown := make([]byte, newSize)
			copy(grown, f.fileData)
			f.fileData = grown
		}
	}
	if req.Msg.Flags != nil {
		f.flags = req.Msg.GetFlags()
	}
	f.mtimeSec++
	return connect.NewResponse(&storagev1.SetAttrResponse{
		Inode:              f.currentInodeLocked(req.Msg.GetIno()),
		AttrTimeoutSeconds: 1.0,
	}), nil
}

func (f *fakeStorageClientForWriteBehind) Fsync(_ context.Context, _ *connect.Request[storagev1.FsyncRequest]) (*connect.Response[storagev1.FsyncResponse], error) {
	f.mu.Lock()
	f.fsyncCalls++
	f.mu.Unlock()
	return connect.NewResponse(&storagev1.FsyncResponse{}), nil
}

func (f *fakeStorageClientForWriteBehind) Fallocate(_ context.Context, _ *connect.Request[storagev1.FallocateRequest]) (*connect.Response[storagev1.FallocateResponse], error) {
	f.mu.Lock()
	f.mtimeSec++
	f.mu.Unlock()
	return connect.NewResponse(&storagev1.FallocateResponse{}), nil
}

func (f *fakeStorageClientForWriteBehind) StatFS(_ context.Context, _ *connect.Request[storagev1.StatFSRequest]) (*connect.Response[storagev1.StatFSResponse], error) {
	return connect.NewResponse(&storagev1.StatFSResponse{
		Blocks: 1024 * 1024,
		Bfree:  512 * 1024,
		Bavail: 512 * 1024,
		Bsize:  4096,
		Frsize: 4096,
	}), nil
}

func newWriteBehindTestFS(t *testing.T, client *fakeStorageClientForWriteBehind) (fs *fusefs.RawFS, nodeID, fh uint64) {
	t.Helper()
	fs = fusefs.NewRawFS(client, fusefs.Config{DiskID: "disk-wb", Epoch: 1, RootIno: 1})
	t.Cleanup(fs.OnUnmount)

	var out fuse.CreateOut
	st := fs.Create(nil, &fuse.CreateIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_CREAT | syscall.O_RDWR,
		Mode:     0o644,
	}, "seq.bin", &out)
	require.Equal(t, fuse.OK, st)
	return fs, out.NodeId, out.Fh
}

func TestWriteBehindCoalesces1MiBFuseWritesTo4MiBAndFlushesTail(t *testing.T) {
	const (
		fuseChunk = 1 << 20
		numChunks = 11 // 2x1MiB 同步探针 + 2x4MiB 异步聚合 + 1MiB 尾块
	)
	expected := makeDeterministicFile(numChunks * fuseChunk)
	client := &fakeStorageClientForWriteBehind{}
	fs, nodeID, fh := newWriteBehindTestFS(t, client)

	for i := range numChunks {
		off := uint64(i * fuseChunk)
		part := expected[off : off+fuseChunk]
		written, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   off,
			Size:     uint32(len(part)),
		}, part)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, uint32(fuseChunk), written)
	}

	st := fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
	})
	require.Equal(t, fuse.OK, st)

	client.mu.Lock()
	calls := append([]writeCallRecord(nil), client.writeCalls...)
	gotData := append([]byte(nil), client.fileData...)
	client.mu.Unlock()

	require.True(t, bytes.Equal(expected, gotData), "聚合写入后的文件内容必须逐字节完全一致")
	require.Len(t, calls, 5, "11 次 1MiB 顺序写应合并为 2 次 1MiB 探针 + 2 次 4MiB 满块 + 1 次 1MiB 尾块")
	lengths := []int{calls[0].length, calls[1].length, calls[2].length, calls[3].length, calls[4].length}
	require.ElementsMatch(t, []int{1 << 20, 1 << 20, 4 << 20, 4 << 20, 1 << 20}, lengths)
}

func TestWriteBehindBoundsInFlightConcurrencyAtFour(t *testing.T) {
	const fuseChunk = 1 << 20
	client := &fakeStorageClientForWriteBehind{}

	var activeRPCs atomic.Int32
	var maxActiveRPCs atomic.Int32
	fourInFlight := make(chan struct{})
	releaseGate := make(chan struct{})
	var fourOnce sync.Once

	client.onWriteHook = func(_ context.Context, _ uint64, data []byte) error {
		if len(data) < fusefs.WriteBehindChunkSize {
			return nil
		}
		cur := activeRPCs.Add(1)
		for {
			prev := maxActiveRPCs.Load()
			if cur <= prev || maxActiveRPCs.CompareAndSwap(prev, cur) {
				break
			}
		}
		if cur == fusefs.WriteBehindMaxInFlight {
			fourOnce.Do(func() { close(fourInFlight) })
		}
		<-releaseGate
		activeRPCs.Add(-1)
		return nil
	}

	fs, nodeID, fh := newWriteBehindTestFS(t, client)
	totalChunks := 2 + (fusefs.WriteBehindMaxInFlight+1)*4 // 2MiB 探针 + 5 个 4MiB 分片 = 22MiB
	payload := makeDeterministicFile(totalChunks * fuseChunk)

	doneCh := make(chan fuse.Status, 1)
	go func() {
		for i := range totalChunks {
			off := uint64(i * fuseChunk)
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   off,
				Size:     fuseChunk,
			}, payload[off:off+fuseChunk])
			if st != fuse.OK {
				doneCh <- st
				return
			}
		}
		doneCh <- fuse.OK
	}()

	select {
	case <-fourInFlight:
	case <-time.After(3 * time.Second):
		t.Fatal("等待达到 W*=4 并发在途 RPC 超时")
	}

	// 第 5 个 4MiB 分片触发背压阻塞，并发数严格受限于 4
	time.Sleep(30 * time.Millisecond)
	require.Equal(t, int32(fusefs.WriteBehindMaxInFlight), maxActiveRPCs.Load())
	select {
	case st := <-doneCh:
		t.Fatalf("在 4 个在途分片全部阻塞时，第 5 个分片发射必须触发背压等待 (got status=%v)", st)
	default:
	}

	close(releaseGate)
	select {
	case st := <-doneCh:
		require.Equal(t, fuse.OK, st)
	case <-time.After(3 * time.Second):
		t.Fatal("释放背压闸门后写入协程未能完成")
	}

	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
	}))
	require.LessOrEqual(t, maxActiveRPCs.Load(), int32(fusefs.WriteBehindMaxInFlight))

	client.mu.Lock()
	require.True(t, bytes.Equal(payload, client.fileData))
	client.mu.Unlock()
}

func TestWriteBehindGetAttrReadTruncateAndStickyErrorSemantics(t *testing.T) {
	const fuseChunk = 1 << 20

	t.Run("GetAttr与Read与SetAttr截断均精确排空未满槽缓冲", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{}
		fs, nodeID, fh := newWriteBehindTestFS(t, client)
		src := makeDeterministicFile(4 * fuseChunk)

		// 写入 3 x 1MiB：前 2MiB 同步落入 fs.cache (Size=2MiB)，第 3 个 1MiB 暂存于 4MiB 槽内
		for i := range 3 {
			off := uint64(i * fuseChunk)
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   off,
				Size:     fuseChunk,
			}, src[off:off+fuseChunk])
			require.Equal(t, fuse.OK, st)
		}

		// GetAttr 必须先通过 BarrierInode 刷出第 3 个 1MiB，绝不返回过期的缓存 Size=2MiB
		var attrOut fuse.AttrOut
		require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, &attrOut))
		require.Equal(t, uint64(3*fuseChunk), attrOut.Size)

		// Read 必须读出全部 3MiB 最新数据
		readBuf := make([]byte, 3*fuseChunk)
		res, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   0,
			Size:     uint32(len(readBuf)),
		}, readBuf)
		require.Equal(t, fuse.OK, st)
		gotBytes, st := res.Bytes(readBuf)
		require.Equal(t, fuse.OK, st)
		require.True(t, bytes.Equal(src[:3*fuseChunk], gotBytes))

		// SetAttr 截断到 1MiB：必须确保无迟到异步写回写放大尺寸
		var truncOut fuse.AttrOut
		setIn := &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_SIZE,
				Size:     fuseChunk,
			},
		}
		require.Equal(t, fuse.OK, fs.SetAttr(nil, setIn, &truncOut))
		require.Equal(t, uint64(fuseChunk), truncOut.Size)
	})

	t.Run("异步分片报错不被GetAttr吞没且由Flush或Fsync精确返回ENOSPC", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{}
		client.onWriteHook = func(_ context.Context, offset uint64, data []byte) error {
			if offset == 2*fuseChunk && len(data) == fusefs.WriteBehindChunkSize {
				return connect.NewError(connect.CodeResourceExhausted, syscall.ENOSPC)
			}
			return nil
		}

		fs, nodeID, fh := newWriteBehindTestFS(t, client)
		buf := bytes.Repeat([]byte{0x7e}, fuseChunk)

		// 写入 6 x 1MiB：前 2MiB 同步成功，[2MiB, 6MiB) 异步分片触发 ENOSPC
		for i := range 6 {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   uint64(i * fuseChunk),
				Size:     fuseChunk,
			}, buf)
			require.Equal(t, fuse.OK, st)
		}

		// GetAttr 只等待在途完成，不得吞掉 stickyErr
		var attrOut fuse.AttrOut
		require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
		}, &attrOut))

		// 随后的 Fsync 必须捕获并返回 ENOSPC，且不向服务端发送 Fsync RPC
		st := fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
		})
		require.Equal(t, fuse.Status(syscall.ENOSPC), st)
		client.mu.Lock()
		require.Zero(t, client.fsyncCalls)
		client.mu.Unlock()
	})
}

func TestWriteBehindServerReorderedInFlightCommitsKeepNewestAttr(t *testing.T) {
	const fuseChunk = 1 << 20
	client := &fakeStorageClientForWriteBehind{}

	// 模拟 HTTP/2 多路复用下：客户端先发 seq=1 [2M,6M)，后发 seq=2 [6M,10M)，
	// 但服务端先收到并提交 seq=2（Size=10M, Mtime=3），后提交 seq=1（Size=10M, Mtime=4）。
	seq2DoneOnServer := make(chan struct{})
	client.onWriteHook = func(_ context.Context, offset uint64, data []byte) error {
		if len(data) == fusefs.WriteBehindChunkSize && offset == 2*fuseChunk {
			<-seq2DoneOnServer
			time.Sleep(15 * time.Millisecond)
		}
		if len(data) == fusefs.WriteBehindChunkSize && offset == 6*fuseChunk {
			defer close(seq2DoneOnServer)
		}
		return nil
	}

	fs, nodeID, fh := newWriteBehindTestFS(t, client)
	payload := makeDeterministicFile(10 * fuseChunk)
	for i := range 10 {
		off := uint64(i * fuseChunk)
		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   off,
			Size:     fuseChunk,
		}, payload[off:off+fuseChunk])
		require.Equal(t, fuse.OK, st)
	}

	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
	}))

	client.mu.Lock()
	wantMtime := uint64(client.mtimeSec)
	client.mu.Unlock()

	var attrOut fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
	}, &attrOut))
	require.Equal(t, uint64(10*fuseChunk), attrOut.Size)
	require.Equal(t, wantMtime, attrOut.Mtime, "服务端后提交的分片哪怕客户端 seq 较小，其最新 Mtime/Ctime 也不得被丢弃")

	t.Run("并发返回时旧小尺寸响应绝不覆盖新大尺寸缓存", func(t *testing.T) {
		for range 32 {
			var committedCount atomic.Int32
			bothCommitted := make(chan struct{})
			var closeOnce sync.Once

			raceClient := &fakeStorageClientForWriteBehind{}
			// onWriteHook 在服务端更新前执行；用包装协调器模拟“服务端已按序提交 6M->10M，但网络响应同时到达或乱序完成”
			var commits []uint64
			var commitsMu sync.Mutex
			coord := fusefs.NewWriteBehindCoordinator(
				func(_ context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error) {
					resp, err := raceClient.Write(context.Background(), connect.NewRequest(&storagev1.WriteRequest{
						Ino:    ino,
						Offset: offset,
						Data:   data,
					}))
					if err != nil {
						return 0, nil, err
					}
					if len(data) == fusefs.WriteBehindChunkSize {
						if committedCount.Add(1) == 2 {
							closeOnce.Do(func() { close(bothCommitted) })
						}
						<-bothCommitted
						if offset == 2*fuseChunk {
							// 让旧 6M 响应比新 10M 响应稍晚或并发争抢提交锁
							time.Sleep(time.Millisecond)
						}
					}
					return resp.Msg.GetSize(), resp.Msg.GetInode(), nil
				},
				fusefs.WriteBehindHooks{
					OnCommit: func(_, _, _ uint64, inode *storagev1.Inode, refreshAttr bool) {
						if refreshAttr && inode != nil && inode.GetAttr() != nil {
							commitsMu.Lock()
							commits = append(commits, inode.GetAttr().GetSize())
							commitsMu.Unlock()
						}
					},
				},
			)

			coord.ObserveSyncWrite(2, 0, fuseChunk)
			coord.ObserveSyncWrite(2, fuseChunk, fuseChunk)
			buf := make([]byte, fusefs.WriteBehindChunkSize)
			_, handled, err := coord.Write(2, 2, 2*fuseChunk, buf, 1)
			require.True(t, handled)
			require.NoError(t, err)
			_, handled, err = coord.Write(2, 2, 6*fuseChunk, buf, 1)
			require.True(t, handled)
			require.NoError(t, err)
			require.NoError(t, coord.FlushInode(2))

			commitsMu.Lock()
			require.NotEmpty(t, commits)
			require.Equal(t, uint64(10*fuseChunk), commits[len(commits)-1], "最终刷入缓存的尺寸必须保持为 10MiB")
			commitsMu.Unlock()
		}
	})
}

func TestWriteBehindReadDirPlusFlushesPendingStageAndReturnsLatestSize(t *testing.T) {
	const fuseChunk = 1 << 20
	client := &fakeStorageClientForWriteBehind{}
	fs, nodeID, _ := newWriteBehindTestFS(t, client)
	src := makeDeterministicFile(3 * fuseChunk)

	// 前 2MiB 同步落盘，第 3 个 1MiB 暂存在 WriteBehind 槽位中尚未发往服务端
	for i := range 3 {
		off := uint64(i * fuseChunk)
		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       1,
			Offset:   off,
			Size:     fuseChunk,
		}, src[off:off+fuseChunk])
		require.Equal(t, fuse.OK, st)
	}

	var dirOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDONLY,
	}, &dirOut))

	dirBuf := make([]byte, 4096)
	require.Equal(t, fuse.OK, fs.ReadDirPlus(nil, &fuse.ReadIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Fh:       dirOut.Fh,
		Offset:   0,
		Size:     uint32(len(dirBuf)),
	}, fuse.NewDirEntryList(dirBuf, 0)))

	// fuse_direntplus 首部即为 fuse_entry_out，其中 Attr.Size 位于字节偏移 48..56：
	// ReadDirPlus 返回给内核的内联属性必须已是排空挂起写后的最新 3MiB，绝不能将旧 2MiB 喂给内核 fuse_change_attributes。
	gotDirentPlusSize := binary.LittleEndian.Uint64(dirBuf[48:56])
	require.Equal(t, uint64(3*fuseChunk), gotDirentPlusSize, "ReadDirPlus 返回给内核的内联 EntryOut.Attr.Size 不得为未排空的旧 2MiB")
}

func TestWriteBehindReadOnlyFlushDoesNotConsumeStickyWriteError(t *testing.T) {
	const fuseChunk = 1 << 20
	client := &fakeStorageClientForWriteBehind{}
	client.onWriteHook = func(_ context.Context, offset uint64, data []byte) error {
		if offset == 2*fuseChunk && len(data) == fusefs.WriteBehindChunkSize {
			return connect.NewError(connect.CodeResourceExhausted, syscall.ENOSPC)
		}
		return nil
	}

	fs, nodeID, fhWrite := newWriteBehindTestFS(t, client)
	buf := bytes.Repeat([]byte{0x33}, fuseChunk)
	for i := range 6 {
		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fhWrite,
			Offset:   uint64(i * fuseChunk),
			Size:     fuseChunk,
		}, buf)
		require.Equal(t, fuse.OK, st)
	}

	// 打开并关闭一个只读句柄：close(ro_fd) 触发的 Flush 不得报错，更不得偷走写句柄的 ENOSPC
	var roOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Flags:    syscall.O_RDONLY,
	}, &roOut))
	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       roOut.Fh,
	}))

	// 随后真正的写句柄 Flush 必须收到 ENOSPC
	require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fhWrite,
	}))
}

func TestWriteBehindIoctlImmutableBlocksSubsequentWriteSynchronously(t *testing.T) {
	const fuseChunk = 1 << 20
	client := &fakeStorageClientForWriteBehind{}
	fs, nodeID, fh := newWriteBehindTestFS(t, client)
	buf := bytes.Repeat([]byte{0xaa}, fuseChunk)

	// 写入 2MiB 激活顺序流探针
	for i := range 2 {
		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   uint64(i * fuseChunk),
			Size:     fuseChunk,
		}, buf)
		require.Equal(t, fuse.OK, st)
	}

	// 通过 ioctl(FS_IOC_SETFLAGS) 设置 FS_IMMUTABLE_FL
	var inbuf [8]byte
	binary.LittleEndian.PutUint64(inbuf[:], uint64(fusefs.FS_IMMUTABLE_FL))
	var ioctlOut fuse.IoctlOut
	require.Equal(t, fuse.OK, fs.Ioctl(nil, &fuse.IoctlIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
		Cmd:      fusefs.FS_IOC_SETFLAGS,
		InSize:   uint32(len(inbuf)),
	}, inbuf[:], &ioctlOut, nil))

	// 紧接着的 Write 必须当场同步失败返回 EPERM，严禁被异步槽静默吞入并返回 OK
	_, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
		Offset:   2 * fuseChunk,
		Size:     fuseChunk,
	}, buf)
	require.Equal(t, fuse.Status(syscall.EPERM), st)
}

func TestWriteBehindConcurrentBackpressureAndBarriersNoPanicOrPendingUnderflow(t *testing.T) {
	const fuseChunk = 1 << 20
	client := &fakeStorageClientForWriteBehind{}

	var activeRPCs atomic.Int32
	fourInFlight := make(chan struct{})
	releaseGate := make(chan struct{})
	var fourOnce sync.Once

	client.onWriteHook = func(_ context.Context, offset uint64, data []byte) error {
		if len(data) == fusefs.WriteBehindChunkSize && offset < 18*fuseChunk {
			if activeRPCs.Add(1) == fusefs.WriteBehindMaxInFlight {
				fourOnce.Do(func() { close(fourInFlight) })
			}
			<-releaseGate
			activeRPCs.Add(-1)
		}
		return nil
	}

	fs, nodeID, fh := newWriteBehindTestFS(t, client)
	var roOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Flags:    syscall.O_RDONLY,
	}, &roOut))
	var dirOut fuse.OpenOut
	require.Equal(t, fuse.OK, fs.OpenDir(nil, &fuse.OpenIn{
		InHeader: fuse.InHeader{NodeId: 1},
		Flags:    syscall.O_RDONLY,
	}, &dirOut))

	// 2MiB 同步探针 + 4x4MiB 在途阻塞 [2M,18M) + 第 5 个 4MiB [18M,22M) 在 dispatchLocked 背压等待 + 尾随 1MiB [22M,23M)
	expected := makeDeterministicFile(23 * fuseChunk)
	for i := range 21 {
		off := uint64(i * fuseChunk)
		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   off,
			Size:     fuseChunk,
		}, expected[off:off+fuseChunk])
		require.Equal(t, fuse.OK, st)
	}

	select {
	case <-fourInFlight:
	case <-time.After(3 * time.Second):
		t.Fatal("等待 4 个 4MiB 分片进入在途阻塞状态超时")
	}

	// 第 22 个 1MiB [21M,22M) 将凑满第 5 个 4MiB 槽并在 dispatchLocked 的 cond.Wait() 处挂起
	writerDone := make(chan fuse.Status, 1)
	go func() {
		off := uint64(21 * fuseChunk)
		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
			Offset:   off,
			Size:     fuseChunk,
		}, expected[off:off+fuseChunk])
		writerDone <- st
	}()

	// 给第 5 个分片的 Write 留出进入 cond.Wait() 的调度窗口，随后并发打入多种 Barrier / Flush 操作
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	errCh := make(chan fuse.Status, 5)
	wg.Add(5)
	go func() {
		defer wg.Done()
		var attrOut fuse.AttrOut
		errCh <- fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: nodeID}}, &attrOut)
	}()
	go func() {
		defer wg.Done()
		dirBuf := make([]byte, 4096)
		errCh <- fs.ReadDirPlus(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: 1},
			Fh:       dirOut.Fh,
			Offset:   0,
			Size:     uint32(len(dirBuf)),
		}, fuse.NewDirEntryList(dirBuf, 0))
	}()
	go func() {
		defer wg.Done()
		var statOut fuse.StatfsOut
		errCh <- fs.StatFs(nil, &fuse.InHeader{NodeId: 1}, &statOut)
	}()
	go func() {
		defer wg.Done()
		readBuf := make([]byte, 4096)
		_, st := fs.Read(nil, &fuse.ReadIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       roOut.Fh,
			Offset:   0,
			Size:     uint32(len(readBuf)),
		}, readBuf)
		errCh <- st
	}()
	go func() {
		defer wg.Done()
		errCh <- fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       roOut.Fh,
		})
	}()

	time.Sleep(20 * time.Millisecond)
	close(releaseGate)

	select {
	case st := <-writerDone:
		require.Equal(t, fuse.OK, st)
	case <-time.After(3 * time.Second):
		t.Fatal("背压释放后 Write 未能完成")
	}

	barriersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(barriersDone)
	}()
	select {
	case <-barriersDone:
	case <-time.After(3 * time.Second):
		t.Fatal("并发 Barrier / Flush 未能完成")
	}
	close(errCh)
	for st := range errCh {
		require.Equal(t, fuse.OK, st)
	}

	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
	}))

	// 反向验证 pendingCount 绝无下溢为负数：再暂存 1MiB [22M,23M)（未满 4MiB 槽），
	// 依赖 HasPending / HasAnyPending 的 GetAttr 与 ReadDirPlus 必须精确排空并返回 23MiB。
	tailOff := uint64(22 * fuseChunk)
	_, st := fs.Write(nil, &fuse.WriteIn{
		InHeader: fuse.InHeader{NodeId: nodeID},
		Fh:       fh,
		Offset:   tailOff,
		Size:     fuseChunk,
	}, expected[tailOff:tailOff+fuseChunk])
	require.Equal(t, fuse.OK, st)

	var finalAttr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: nodeID}}, &finalAttr))
	require.Equal(t, uint64(23*fuseChunk), finalAttr.Size, "pendingCount 若下溢会导致 GetAttr 漏刷尾部聚合块")

	client.mu.Lock()
	gotData := append([]byte(nil), client.fileData...)
	client.mu.Unlock()
	require.True(t, bytes.Equal(expected, gotData))
}

func TestWriteBehindConcurrentStraddlingWritesAndStreamResetsPreserveIntegrity(t *testing.T) {
	const fuseChunk = 1 << 20

	t.Run("背压等待期间并发同偏移Write绝不产生重叠在途分片或槽错位", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{}
		slot0Gate := make(chan struct{})
		restGate := make(chan struct{})
		fourInFlight := make(chan struct{})
		slot18MInFlight := make(chan struct{})
		var activeFirstWave atomic.Int32
		var fourOnce, slot18MOnce sync.Once

		type byteRange struct {
			start uint64
			end   uint64
		}
		var rangeMu sync.Mutex
		var inflightRanges []byteRange
		var overlapDetected atomic.Bool

		client.onWriteHook = func(_ context.Context, offset uint64, data []byte) error {
			if len(data) == 0 {
				return nil
			}
			cur := byteRange{
				start: (offset / fusefs.WriteBehindBlockAlign) * fusefs.WriteBehindBlockAlign,
				end:   ((offset + uint64(len(data)) + fusefs.WriteBehindBlockAlign - 1) / fusefs.WriteBehindBlockAlign) * fusefs.WriteBehindBlockAlign,
			}
			rangeMu.Lock()
			for _, r := range inflightRanges {
				if cur.start < r.end && r.start < cur.end {
					overlapDetected.Store(true)
				}
			}
			inflightRanges = append(inflightRanges, cur)
			rangeMu.Unlock()

			defer func() {
				rangeMu.Lock()
				for i, r := range inflightRanges {
					if r == cur {
						inflightRanges = append(inflightRanges[:i], inflightRanges[i+1:]...)
						break
					}
				}
				rangeMu.Unlock()
			}()

			switch {
			case len(data) == fusefs.WriteBehindChunkSize && offset == 2*fuseChunk:
				if activeFirstWave.Add(1) == fusefs.WriteBehindMaxInFlight {
					fourOnce.Do(func() { close(fourInFlight) })
				}
				<-slot0Gate
			case len(data) == fusefs.WriteBehindChunkSize && offset > 2*fuseChunk && offset < 18*fuseChunk:
				if activeFirstWave.Add(1) == fusefs.WriteBehindMaxInFlight {
					fourOnce.Do(func() { close(fourInFlight) })
				}
				<-restGate
			case len(data) == fusefs.WriteBehindChunkSize && offset == 18*fuseChunk:
				slot18MOnce.Do(func() { close(slot18MInFlight) })
				<-restGate
			case offset >= 22*fuseChunk:
				// 给并发冲突在途分片留出同时处于服务端执行窗口的重叠观测时间
				time.Sleep(25 * time.Millisecond)
			}
			return nil
		}

		fs, nodeID, fh := newWriteBehindTestFS(t, client)
		expected := makeDeterministicFile(26 * fuseChunk)

		// 写入 [0, 21MiB)：2MiB 探针 + 4x4MiB 在途阻塞 [2M,18M) + 3MiB 暂存于活跃槽 [18M,21M)
		for i := range 21 {
			off := uint64(i * fuseChunk)
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   off,
				Size:     fuseChunk,
			}, expected[off:off+fuseChunk])
			require.Equal(t, fuse.OK, st)
		}

		select {
		case <-fourInFlight:
		case <-time.After(3 * time.Second):
			t.Fatal("等待首波 4 个在途 RPC 阻塞超时")
		}

		// W0 填满 [18M,22M) 并作为 FIFO #1 挂起在 dispatchLocked
		w0Done := make(chan fuse.Status, 1)
		go func() {
			off := uint64(21 * fuseChunk)
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   off,
				Size:     fuseChunk,
			}, expected[off:off+fuseChunk])
			w0Done <- st
		}()
		time.Sleep(20 * time.Millisecond)

		// W1 写入 [22M,26M) (4MiB)
		w1Done := make(chan fuse.Status, 1)
		go func() {
			off := uint64(22 * fuseChunk)
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   off,
				Size:     4 * fuseChunk,
			}, expected[off:off+4*fuseChunk])
			w1Done <- st
		}()
		time.Sleep(20 * time.Millisecond)

		// 仅释放 1 个在途槽位：W0 发出 [18M,22M) 后再次占满 W*=4
		close(slot0Gate)
		select {
		case st := <-w0Done:
			require.Equal(t, fuse.OK, st)
		case <-time.After(3 * time.Second):
			t.Fatal("W0 超时")
		}
		select {
		case <-slot18MInFlight:
		case <-time.After(3 * time.Second):
			t.Fatal("等待 [18M,22M) 在途超时")
		}

		// W2 并发覆写 [22M,23M) (1MiB)
		w2Payload := bytes.Repeat([]byte{0xd5}, fuseChunk)
		w2Done := make(chan fuse.Status, 1)
		go func() {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   22 * fuseChunk,
				Size:     uint32(len(w2Payload)),
			}, w2Payload)
			w2Done <- st
		}()
		time.Sleep(20 * time.Millisecond)

		// 释放其余在途分片
		close(restGate)

		select {
		case st := <-w1Done:
			require.Equal(t, fuse.OK, st)
		case <-time.After(3 * time.Second):
			t.Fatal("W1 超时")
		}
		select {
		case st := <-w2Done:
			require.Equal(t, fuse.OK, st)
		case <-time.After(3 * time.Second):
			t.Fatal("W2 超时")
		}

		require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
		}))

		require.False(t, overlapDetected.Load(), "严禁两个覆盖相同 256KiB 物理块区间的 Write RPC 同时在途")

		client.mu.Lock()
		gotData := append([]byte(nil), client.fileData...)
		client.mu.Unlock()

		require.Len(t, gotData, 26*fuseChunk)
		require.True(t, bytes.Equal(expected[:22*fuseChunk], gotData[:22*fuseChunk]))
		require.True(t, bytes.Equal(expected[23*fuseChunk:26*fuseChunk], gotData[23*fuseChunk:26*fuseChunk]))
		got22To23 := gotData[22*fuseChunk : 23*fuseChunk]
		require.True(t,
			bytes.Equal(w2Payload, got22To23) || bytes.Equal(expected[22*fuseChunk:23*fuseChunk], got22To23),
			"[22MiB,23MiB) 必须完整等于 W1 或 W2 的写入载荷",
		)
	})

	t.Run("并发相邻Write与SetAttr及RemoveInode高压竞态无数据竞争且计数守恒", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{}
		client.onWriteHook = func(_ context.Context, _ uint64, data []byte) error {
			if len(data) == fusefs.WriteBehindChunkSize {
				time.Sleep(time.Millisecond)
			}
			return nil
		}

		fs, nodeID, fh := newWriteBehindTestFS(t, client)
		src := makeDeterministicFile(16 * fuseChunk)

		for round := range 8 {
			statusCh := make(chan fuse.Status, 20)
			var wg sync.WaitGroup
			for w := range 4 {
				wg.Add(1)
				go func(worker int) {
					defer wg.Done()
					for step := range 4 {
						off := uint64((worker*4 + step) * fuseChunk)
						_, st := fs.Write(nil, &fuse.WriteIn{
							InHeader: fuse.InHeader{NodeId: nodeID},
							Fh:       fh,
							Offset:   off,
							Size:     fuseChunk,
						}, src[off:off+fuseChunk])
						statusCh <- st
					}
				}(w)
			}
			wg.Add(2)
			go func() {
				defer wg.Done()
				var attrOut fuse.AttrOut
				statusCh <- fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: nodeID}}, &attrOut)
			}()
			go func(r int) {
				defer wg.Done()
				if r%2 == 0 {
					var truncOut fuse.AttrOut
					statusCh <- fs.SetAttr(nil, &fuse.SetAttrIn{
						SetAttrInCommon: fuse.SetAttrInCommon{
							InHeader: fuse.InHeader{NodeId: nodeID},
							Valid:    fuse.FATTR_MTIME,
							Mtime:    uint64(1000 + r),
						},
					}, &truncOut)
				} else {
					var openOut fuse.OpenOut
					if st := fs.Open(nil, &fuse.OpenIn{
						InHeader: fuse.InHeader{NodeId: nodeID},
						Flags:    syscall.O_RDONLY,
					}, &openOut); st != fuse.OK {
						statusCh <- st
						return
					}
					statusCh <- fs.Flush(nil, &fuse.FlushIn{
						InHeader: fuse.InHeader{NodeId: nodeID},
						Fh:       openOut.Fh,
					})
					fs.Release(nil, &fuse.ReleaseIn{
						InHeader: fuse.InHeader{NodeId: nodeID},
						Fh:       openOut.Fh,
					})
				}
			}(round)

			done := make(chan struct{})
			go func() {
				wg.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatalf("round %d 并发竞态超时", round)
			}
			close(statusCh)
			for st := range statusCh {
				require.Equal(t, fuse.OK, st)
			}
			require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
			}))
		}
	})

	t.Run("ResetInodeStream与RemoveInode及短写边界严格排空不留孤儿槽", func(t *testing.T) {
		var shortMode atomic.Bool
		var dispatchedBytes atomic.Uint64
		coord := fusefs.NewWriteBehindCoordinator(
			func(_ context.Context, ino, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
				dispatchedBytes.Add(uint64(len(data)))
				if shortMode.Load() {
					return uint32(len(data) - 1), nil, nil
				}
				return uint32(len(data)), &storagev1.Inode{
					Ino:  ino,
					Attr: &storagev1.InodeAttribute{Size: uint64(len(data))},
				}, nil
			},
			fusefs.WriteBehindHooks{},
		)

		const ino = 77
		coord.ObserveSyncWrite(ino, 0, fuseChunk)
		coord.ObserveSyncWrite(ino, fuseChunk, fuseChunk)

		// 暂存 1MiB [2M,3M) 后直接调用 ResetInodeStream：必须刷出该 1MiB 并重置流，使下一笔 Write 回落同步探针
		oneMeg := bytes.Repeat([]byte{0x42}, fuseChunk)
		w, handled, err := coord.Write(ino, ino, 2*fuseChunk, oneMeg, 1)
		require.NoError(t, err)
		require.True(t, handled)
		require.Equal(t, uint32(fuseChunk), w)
		require.True(t, coord.HasPending(ino))

		coord.ResetInodeStream(ino)
		require.False(t, coord.HasPending(ino))
		require.False(t, coord.HasAnyPending())
		require.Equal(t, uint64(fuseChunk), dispatchedBytes.Load())

		_, handled, err = coord.Write(ino, ino, 3*fuseChunk, oneMeg, 1)
		require.NoError(t, err)
		require.False(t, handled, "ResetInodeStream 后即使偏移连续也必须重新走同步探针")

		// 并发 RemoveInode 与重新激活写入竞态：任何已接管写入都不得遗留在被摘除的孤儿节点中
		for range 32 {
			errCh := make(chan error, 2)
			var wg sync.WaitGroup
			wg.Add(2)
			go func() {
				defer wg.Done()
				coord.ObserveSyncWrite(ino, 0, 2*fuseChunk)
				_, h, wErr := coord.Write(ino, ino, 2*fuseChunk, oneMeg, 1)
				if wErr != nil {
					errCh <- wErr
					return
				}
				if h {
					errCh <- coord.FlushInode(ino)
				}
			}()
			go func() {
				defer wg.Done()
				coord.RemoveInode(ino)
			}()
			wg.Wait()
			close(errCh)
			for gErr := range errCh {
				require.NoError(t, gErr)
			}
			require.NoError(t, coord.FlushInode(ino))
			require.False(t, coord.HasAnyPending())
		}

		// 短写 (< chunkLen) 必须转化为 EIO 粘性错误，且空写/超长写安全旁路
		shortMode.Store(true)
		coord.ObserveSyncWrite(ino, 0, 2*fuseChunk)
		_, handled, err = coord.Write(ino, ino, 2*fuseChunk, make([]byte, fusefs.WriteBehindChunkSize), 1)
		require.NoError(t, err)
		require.True(t, handled)
		require.ErrorIs(t, coord.FlushAndResetInode(ino), syscall.EIO)

		shortMode.Store(false)
		_, handled, err = coord.Write(ino, ino, 0, nil, 1)
		require.NoError(t, err)
		require.False(t, handled)
		_, handled, err = coord.Write(ino, ino, 0, make([]byte, fusefs.WriteBehindChunkSize+1), 1)
		require.NoError(t, err)
		require.False(t, handled)
	})
}

func TestWriteBehindStickyErrorIsolationAcrossSetAttrTruncateFallocateAndMultiHandles(t *testing.T) {
	const fuseChunk = 1 << 20
	failAsyncChunkWithENOSPC := func(_ context.Context, offset uint64, data []byte) error {
		if offset == 2*fuseChunk && len(data) == fusefs.WriteBehindChunkSize {
			return connect.NewError(connect.CodeResourceExhausted, syscall.ENOSPC)
		}
		return nil
	}

	t.Run("SetAttr与Fallocate不窃取写句柄粘性错误", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{onWriteHook: failAsyncChunkWithENOSPC}
		fs, nodeID, fhWrite := newWriteBehindTestFS(t, client)
		buf := bytes.Repeat([]byte{0x51}, fuseChunk)

		for i := range 6 {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fhWrite,
				Offset:   uint64(i * fuseChunk),
				Size:     fuseChunk,
			}, buf)
			require.Equal(t, fuse.OK, st)
		}

		// 元数据 SetAttr (chmod) 与打洞 Fallocate 不得因历史异步写 ENOSPC 而失败，更不得偷走 fhWrite 的错误
		var attrOut fuse.AttrOut
		require.Equal(t, fuse.OK, fs.SetAttr(nil, &fuse.SetAttrIn{
			SetAttrInCommon: fuse.SetAttrInCommon{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Valid:    fuse.FATTR_MODE,
				Mode:     syscall.S_IFREG | 0o600,
			},
		}, &attrOut))

		const fallocPunchKeep = 0x1 | 0x2 // FALLOC_FL_KEEP_SIZE | FALLOC_FL_PUNCH_HOLE
		require.Equal(t, fuse.OK, fs.Fallocate(nil, &fuse.FallocateIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fhWrite,
			Offset:   0,
			Length:   4096,
			Mode:     fallocPunchKeep,
		}))

		// 原写句柄的 Fsync 仍必须精准捕获到 ENOSPC；已向该句柄报告一次后，随后的 close Flush 返回 OK
		require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fhWrite,
		}))
		require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fhWrite,
		}))
	})

	t.Run("Open_O_TRUNC恢复满盘空间且不窃取旧写句柄粘性错误", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{onWriteHook: failAsyncChunkWithENOSPC}
		fs, nodeID, fh1 := newWriteBehindTestFS(t, client)
		buf := bytes.Repeat([]byte{0x62}, fuseChunk)

		for i := range 6 {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh1,
				Offset:   uint64(i * fuseChunk),
				Size:     fuseChunk,
			}, buf)
			require.Equal(t, fuse.OK, st)
		}

		// 新句柄通过 O_TRUNC 截断回收空间必须成功，且新句柄自身写盘与 Flush 不受历史错误污染
		var truncOpen fuse.OpenOut
		require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Flags:    syscall.O_RDWR | syscall.O_TRUNC,
		}, &truncOpen))

		_, st := fs.Write(nil, &fuse.WriteIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       truncOpen.Fh,
			Offset:   0,
			Size:     fuseChunk,
		}, buf)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       truncOpen.Fh,
		}))
		fs.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       truncOpen.Fh,
		})

		// 旧写句柄 fh1 在 close Flush 时仍必须收到其写入期间发生的 ENOSPC
		require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh1,
		}))
	})

	t.Run("双写句柄并发打开时各自独立观测一次异步写错误", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{onWriteHook: failAsyncChunkWithENOSPC}
		fs, nodeID, fh1 := newWriteBehindTestFS(t, client)

		var open2 fuse.OpenOut
		require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Flags:    syscall.O_WRONLY,
		}, &open2))

		buf := bytes.Repeat([]byte{0x73}, fuseChunk)
		for i := range 6 {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh1,
				Offset:   uint64(i * fuseChunk),
				Size:     fuseChunk,
			}, buf)
			require.Equal(t, fuse.OK, st)
		}

		require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh1,
		}))
		require.Equal(t, fuse.Status(syscall.ENOSPC), fs.Fsync(nil, &fuse.FsyncIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       open2.Fh,
		}))
	})

	t.Run("最后句柄Release在存在未消费错误时仍彻底回收节点且不污染后续新Open", func(t *testing.T) {
		client := &fakeStorageClientForWriteBehind{onWriteHook: failAsyncChunkWithENOSPC}
		fs, nodeID, fh1 := newWriteBehindTestFS(t, client)
		buf := bytes.Repeat([]byte{0x84}, fuseChunk)

		for i := range 6 {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh1,
				Offset:   uint64(i * fuseChunk),
				Size:     fuseChunk,
			}, buf)
			require.Equal(t, fuse.OK, st)
		}

		// 模拟进程异常退出未经过写句柄消费错误即触发最后句柄 Release
		fs.Release(nil, &fuse.ReleaseIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh1,
		})

		var freshOpen fuse.OpenOut
		require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Flags:    syscall.O_RDWR,
		}, &freshOpen))
		require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       freshOpen.Fh,
		}))
	})

	t.Run("并发ErrStaleEpoch栅栏拒绝时GetAttr与Open_O_TRUNC与SetAttr及Fsync精确返回预期状态", func(t *testing.T) {
		inFlightStarted := make(chan struct{})
		releaseStale := make(chan struct{})
		var once sync.Once

		client := &fakeStorageClientForWriteBehind{}
		client.onWriteHook = func(_ context.Context, offset uint64, data []byte) error {
			if offset == 2*fuseChunk && len(data) == fusefs.WriteBehindChunkSize {
				once.Do(func() { close(inFlightStarted) })
				<-releaseStale
				return connect.NewError(connect.CodeFailedPrecondition, syscall.EBUSY)
			}
			return nil
		}

		fs, nodeID, fh := newWriteBehindTestFS(t, client)
		buf := bytes.Repeat([]byte{0x95}, fuseChunk)
		for i := range 6 {
			_, st := fs.Write(nil, &fuse.WriteIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
				Offset:   uint64(i * fuseChunk),
				Size:     fuseChunk,
			}, buf)
			require.Equal(t, fuse.OK, st)
		}

		<-inFlightStarted
		client.mu.Lock()
		client.staleEpoch = true
		client.mu.Unlock()

		var wg sync.WaitGroup
		var getAttrSt, setAttrSt, openTruncSt, fsyncSt fuse.Status
		wg.Add(4)
		go func() {
			defer wg.Done()
			var out fuse.AttrOut
			getAttrSt = fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: nodeID}}, &out)
		}()
		go func() {
			defer wg.Done()
			var out fuse.AttrOut
			setAttrSt = fs.SetAttr(nil, &fuse.SetAttrIn{
				SetAttrInCommon: fuse.SetAttrInCommon{
					InHeader: fuse.InHeader{NodeId: nodeID},
					Valid:    fuse.FATTR_MODE,
					Mode:     syscall.S_IFREG | 0o600,
				},
			}, &out)
		}()
		go func() {
			defer wg.Done()
			var out fuse.OpenOut
			openTruncSt = fs.Open(nil, &fuse.OpenIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Flags:    syscall.O_WRONLY | syscall.O_TRUNC,
			}, &out)
		}()
		go func() {
			defer wg.Done()
			fsyncSt = fs.Fsync(nil, &fuse.FsyncIn{
				InHeader: fuse.InHeader{NodeId: nodeID},
				Fh:       fh,
			})
		}()

		time.Sleep(15 * time.Millisecond)
		close(releaseStale)
		wg.Wait()

		require.Equal(t, fuse.OK, getAttrSt, "只读 GetAttr 不携带 Epoch，不得被异步写 EBUSY 拒绝")
		require.Equal(t, fuse.Status(syscall.EBUSY), setAttrSt)
		require.Equal(t, fuse.Status(syscall.EBUSY), openTruncSt)
		require.Equal(t, fuse.Status(syscall.EBUSY), fsyncSt)
		// Fsync 已消费该句柄错误游标，随后 close Flush 不重复报第二次错误
		require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{
			InHeader: fuse.InHeader{NodeId: nodeID},
			Fh:       fh,
		}))
	})

	t.Run("Clear等待超时在途writer收口后pendingCount严格归零", func(t *testing.T) {
		started := make(chan struct{})
		release := make(chan struct{})
		var once sync.Once

		coord := fusefs.NewWriteBehindCoordinator(
			func(_ context.Context, _, _ uint64, data []byte) (uint32, *storagev1.Inode, error) {
				if len(data) == fusefs.WriteBehindChunkSize {
					once.Do(func() { close(started) })
					<-release
					return 0, nil, context.DeadlineExceeded
				}
				return uint32(len(data)), nil, nil
			},
			fusefs.WriteBehindHooks{},
		)

		const ino = 88
		coord.RegisterHandle(1)
		coord.ObserveSyncWrite(ino, 0, 2*fuseChunk)
		_, handled, err := coord.WriteForHandle(1, ino, ino, 2*fuseChunk, make([]byte, fusefs.WriteBehindChunkSize), 1)
		require.NoError(t, err)
		require.True(t, handled)
		// 再压入 1MiB 未满槽数据，验证 Clear 同时排空部分缓冲与超时在途协程
		_, handled, err = coord.WriteForHandle(1, ino, ino, 6*fuseChunk, make([]byte, fuseChunk), 1)
		require.NoError(t, err)
		require.True(t, handled)

		<-started
		clearDone := make(chan struct{})
		go func() {
			coord.Clear()
			close(clearDone)
		}()

		select {
		case <-clearDone:
			t.Fatal("在途 writer 未收口前 Clear 不得提前返回")
		case <-time.After(15 * time.Millisecond):
		}

		close(release)
		select {
		case <-clearDone:
		case <-time.After(3 * time.Second):
			t.Fatal("释放超时在途 writer 后 Clear 未能收口")
		}
		require.False(t, coord.HasAnyPending())
		require.False(t, coord.HasPending(ino))
	})
}
