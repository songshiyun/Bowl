// Package fusefs 实现基于 go-fuse RawFileSystem 的 Bowl 用户态文件系统适配层。
package fusefs

import (
	"context"
	"errors"
	"log/slog"
	"math"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// DefaultReleaseRetryInterval 是孤儿回收异步重试的默认轮询周期。
const DefaultReleaseRetryInterval = 200 * time.Millisecond

// DefaultRPCTimeout 是 FUSE 常规请求（不含阻塞锁 SetLkw）的默认单请求超时预算。
const DefaultRPCTimeout = 30 * time.Second

// DefaultWriteAttrTimeoutSeconds 是写操作成功后回填内核属性缓存的有效期。
// 严禁置 0：置 0 会使内核在连续 write() 前重新发起 GetAttr RPC 核对文件尺寸。
const DefaultWriteAttrTimeoutSeconds = 1.0

// 确保 RawFS 完整实现 fuse.RawFileSystem 接口。
var _ fuse.RawFileSystem = (*RawFS)(nil)

// Config 包含 FUSE 文件系统运行所需的所有静态与动态配置。
type Config struct {
	DiskID              string
	Epoch               uint64
	RootIno             uint64
	RootInode           *storagev1.Inode
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
	ReadOnly            bool
	DirectIO            bool
	AllowOther          bool
	Debug               bool
}

type reclaimRetryItem struct {
	ino uint64
}

// RawFS 封装底层 FUSE 请求处理，桥接 Linux 内核 VFS 与 StorageService RPC。
type RawFS struct {
	fuse.RawFileSystem

	client  storagev1connect.StorageServiceClient
	locks   *LockTable
	handles *HandleTable
	cache   *NamespaceCache
	caps    *capabilityCache
	// lifecycleMu 保证 OnUnmount 等待所有在途 FUSE 请求排空；常规请求只持读锁。
	lifecycleMu sync.RWMutex
	// stateMu 仅保护本地内存态的微秒级读写，绝不包裹跨机 RPC。
	stateMu      sync.Mutex
	dirtyInodes  map[uint64]struct{}
	orphanInodes map[uint64]struct{}
	// inodeLocks 按 Inode 粒度串行化 O_APPEND 定序与 Unlink/Release 孤儿标记。
	inodeLocks inodeLocks
	tmpMgr     *TmpFileManager
	cfg        Config
	nextReqID  atomic.Uint64

	releaseMu       sync.Mutex
	releaseQueue    []reclaimRetryItem
	releaseCtx      context.Context
	releaseCancel   context.CancelFunc
	releaseLoopDone chan struct{}
	releaseStarted  bool
	releaseClosed   bool

	readAhead   *ReadAheadCoordinator
	writeBehind *WriteBehindCoordinator
}

// NewRawFS 创建并初始化 RawFS 实例。
func NewRawFS(client storagev1connect.StorageServiceClient, cfg Config) *RawFS {
	if cfg.RootIno == 0 && cfg.RootInode != nil {
		cfg.RootIno = cfg.RootInode.GetIno()
	}
	releaseCtx, releaseCancel := context.WithCancel(context.Background())
	fs := &RawFS{
		RawFileSystem: fuse.NewDefaultRawFileSystem(),
		client:        client,
		locks:         NewLockTable(),
		cache:         NewNamespaceCache(DefaultNamespaceCacheCapacity),
		caps:          newCapabilityCache(),
		dirtyInodes:   make(map[uint64]struct{}),
		tmpMgr:        NewTmpFileManager(cfg.RootIno),
		orphanInodes:  make(map[uint64]struct{}),
		cfg:           cfg,
		releaseCtx:    releaseCtx,
		releaseCancel: releaseCancel,
		readAhead:     NewReadAheadCoordinator(),
	}
	fs.writeBehind = NewWriteBehindCoordinator(
		func(ctx context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error) {
			if fs.client == nil {
				return 0, nil, syscall.EIO
			}
			resp, err := fs.client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
				Ino:    ino,
				Fh:     0,
				Offset: offset,
				Data:   data,
				DiskId: fs.cfg.DiskID,
				Epoch:  fs.cfg.Epoch,
			}))
			if err != nil {
				return 0, nil, err
			}
			return resp.Msg.GetSize(), resp.Msg.GetInode(), nil
		},
		WriteBehindHooks{
			OnCommit: func(nodeID, ino, cacheEpoch uint64, inode *storagev1.Inode, refreshAttr bool) {
				fs.markDirty(ino)
				if refreshAttr && inode != nil {
					fs.cache.refreshInodeAt(cacheEpoch, nodeID, inode, DefaultWriteAttrTimeoutSeconds)
				}
				fs.caps.markAbsent(nodeID)
			},
			OnError: func(nodeID uint64) {
				fs.cache.invalidateInode(nodeID)
			},
		},
	)
	fs.handles = NewHandleTable(fs.reclaimOrphan)
	return fs
}

// Config 返回当前 RawFS 绑定的静态与握手配置副本。
func (fs *RawFS) Config() Config {
	return fs.cfg
}

// ToServerIno 将内核传入的 Inode 映射为服务端持久化 Inode。
// 严防内核越过根映射以非 1 且等于 RootIno 的编号直接访问，返回 ENOENT。
func (fs *RawFS) ToServerIno(kernelIno uint64) (uint64, fuse.Status) {
	return fs.tmpMgr.ToServerIno(kernelIno)
}

// ToKernelIno 将服务端持久化 Inode 映射为 Linux 内核可见的 Inode。
// 当 serverIno 等于目标 Disk.RootIno 时，强制映射为 1 (fuse.FUSE_ROOT_ID)。
func (fs *RawFS) ToKernelIno(serverIno uint64) uint64 {
	return fs.tmpMgr.ToKernelIno(serverIno)
}

// CheckWritable 检查文件系统当前是否允许写操作。只读挂载时返回 EROFS。
func (fs *RawFS) CheckWritable() fuse.Status {
	if fs.cfg.ReadOnly {
		return fuse.Status(syscall.EROFS)
	}
	return fuse.OK
}

// RPCContext 包装内核 cancel 信号通道与请求超时为派生 context，支持 FUSE_INTERRUPT 及时取消与 30s 超时保护。
func (fs *RawFS) RPCContext(cancel <-chan struct{}) (context.Context, context.CancelFunc) {
	baseCtx, cancelTimeout := context.WithTimeout(context.Background(), DefaultRPCTimeout)
	ctx, cancelChan := rpc.ContextWithCancelChan(baseCtx, cancel)
	return ctx, func() {
		cancelChan()
		cancelTimeout()
	}
}

// ToStatus 将 ConnectRPC 错误解码为对应的 Linux fuse.Status。
func (fs *RawFS) ToStatus(err error, cancel <-chan struct{}) fuse.Status {
	if err == nil {
		return fuse.OK
	}
	errno := rpc.MapErrorWithCancel(err, cancel)
	return fuse.Status(errno)
}

func (fs *RawFS) checkedHandle(ino, fh uint64, dir bool) (FileHandle, fuse.Status) {
	h, ok := fs.handles.Get(fh)
	if !ok || h.Ino != ino || h.IsDir != dir {
		return FileHandle{}, fuse.EBADF
	}
	return h, fuse.OK
}

func (fs *RawFS) openHandle(ino uint64, flags uint32, dir bool) (uint64, fuse.Status) {
	fh, err := fs.handles.Open(ino, flags, dir)
	if err != nil {
		if errno, ok := errors.AsType[syscall.Errno](err); ok {
			return 0, fuse.Status(errno)
		}
		return 0, fuse.EIO
	}
	fs.locks.RegisterHandle(fh, flags)
	if !dir {
		fs.writeBehind.RegisterHandle(fh)
	}
	return fh, fuse.OK
}

// NextRequestID 原子单调递增生成在 [1, math.MaxInt64] 范围内的唯一请求 ID。
func (fs *RawFS) NextRequestID() uint64 {
	for {
		curr := fs.nextReqID.Add(1)
		id := curr & math.MaxInt64
		if id != 0 {
			return id
		}
	}
}

// OnUnmount 在文件系统卸载完成时被调用。
func (fs *RawFS) OnUnmount() {
	fs.locks.Close()
	// 排他锁等待所有在途请求释放读锁，先排空异步写缓冲确保 dirtyInodes 完整，再逐一同步落盘。
	fs.lifecycleMu.Lock()
	fs.writeBehind.Clear()
	for _, ino := range fs.takeDirtyInodes() {
		ctx, done := fs.RPCContext(nil)
		_, err := fs.client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
			Ino:    ino,
			DiskId: fs.cfg.DiskID,
			Epoch:  fs.cfg.Epoch,
		}))
		done()
		if err != nil && connect.CodeOf(err) != connect.CodeUnimplemented {
			slog.Warn("卸载时同步文件失败", "ino", ino, "error", err)
		}
	}
	fs.handles.clear()
	fs.cache.clear()
	fs.caps.reset()
	fs.tmpMgr.Clear()
	fs.stateMu.Lock()
	clear(fs.orphanInodes)
	fs.stateMu.Unlock()
	fs.lifecycleMu.Unlock()
	fs.readAhead.Clear()
	fs.stopAndDrainReleaseQueue()
}

func shouldRetryReclaim(err error) bool {
	if err == nil {
		return false
	}
	switch connect.CodeOf(err) {
	case connect.CodeUnavailable, connect.CodeDeadlineExceeded, connect.CodeResourceExhausted, connect.CodeAborted, connect.CodeInternal:
		return true
	default:
		return errors.Is(err, context.DeadlineExceeded)
	}
}

// markDirty 记录尚未显式 Fsync 的节点。
func (fs *RawFS) markDirty(ino uint64) {
	fs.stateMu.Lock()
	fs.dirtyInodes[ino] = struct{}{}
	fs.stateMu.Unlock()
}

// clearDirty 在服务端确认落盘后清除脏标记。
func (fs *RawFS) clearDirty(ino uint64) {
	fs.stateMu.Lock()
	delete(fs.dirtyInodes, ino)
	fs.stateMu.Unlock()
}

// takeDirtyInodes 原子取走并清空全部脏节点，供卸载时排空。
func (fs *RawFS) takeDirtyInodes() []uint64 {
	fs.stateMu.Lock()
	defer fs.stateMu.Unlock()
	inodes := make([]uint64, 0, len(fs.dirtyInodes))
	for ino := range fs.dirtyInodes {
		inodes = append(inodes, ino)
	}
	clear(fs.dirtyInodes)
	return inodes
}

// markOrphan 记录待延迟回收的孤儿节点。
func (fs *RawFS) markOrphan(ino uint64) {
	fs.stateMu.Lock()
	fs.orphanInodes[ino] = struct{}{}
	fs.stateMu.Unlock()
	fs.handles.MarkOrphan(ino)
}

func (fs *RawFS) reclaimOrphan(ino uint64) {
	fs.stateMu.Lock()
	_, exists := fs.orphanInodes[ino]
	delete(fs.orphanInodes, ino)
	fs.stateMu.Unlock()
	if !exists {
		return
	}
	serverIno, status := fs.ToServerIno(ino)
	if status != fuse.OK || fs.client == nil {
		return
	}
	item := reclaimRetryItem{ino: serverIno}
	if fs.tryReclaim(context.Background(), item) {
		return
	}
	fs.releaseMu.Lock()
	if fs.releaseClosed {
		fs.releaseMu.Unlock()
		return
	}
	fs.releaseQueue = append(fs.releaseQueue, item)
	if !fs.releaseStarted {
		fs.releaseStarted = true
		fs.releaseLoopDone = make(chan struct{})
		go fs.releaseRetryLoop()
	}
	fs.releaseMu.Unlock()
}

// tryReclaim 返回 true 表示回收完成或错误不可重试。
func (fs *RawFS) tryReclaim(parent context.Context, item reclaimRetryItem) bool {
	ctx, cancel := context.WithTimeout(parent, DefaultRPCTimeout)
	defer cancel()
	req := &storagev1.ReclaimOrphanRequest{
		DiskId: fs.cfg.DiskID,
		Epoch:  fs.cfg.Epoch,
		Ino:    item.ino,
	}
	_, err := fs.client.ReclaimOrphan(ctx, connect.NewRequest(req))
	return !shouldRetryReclaim(err)
}

func (fs *RawFS) releaseRetryLoop() {
	defer close(fs.releaseLoopDone)
	ticker := time.NewTicker(DefaultReleaseRetryInterval)
	defer ticker.Stop()
	for {
		select {
		case <-fs.releaseCtx.Done():
			return
		case <-ticker.C:
			fs.drainReleaseQueueOnce(fs.releaseCtx)
		}
	}
}

func (fs *RawFS) drainReleaseQueueOnce(ctx context.Context) {
	fs.releaseMu.Lock()
	queue := fs.releaseQueue
	fs.releaseQueue = nil
	fs.releaseMu.Unlock()
	for _, item := range queue {
		if ctx.Err() != nil {
			fs.releaseMu.Lock()
			fs.releaseQueue = append(fs.releaseQueue, item)
			fs.releaseMu.Unlock()
			continue
		}
		if !fs.tryReclaim(ctx, item) {
			fs.releaseMu.Lock()
			fs.releaseQueue = append(fs.releaseQueue, item)
			fs.releaseMu.Unlock()
		}
	}
}

func (fs *RawFS) stopAndDrainReleaseQueue() {
	fs.releaseMu.Lock()
	fs.releaseClosed = true
	started, done := fs.releaseStarted, fs.releaseLoopDone
	fs.releaseMu.Unlock()
	fs.releaseCancel()
	if started {
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	fs.drainReleaseQueueOnce(ctx)
}

// FillAttr 精确转换 Inode 属性并填入 fuse.Attr。
func (fs *RawFS) FillAttr(out *fuse.Attr, ino *storagev1.Inode) {
	if out == nil || ino == nil {
		return
	}
	out.Ino = fs.ToKernelIno(ino.GetIno())

	attr := ino.GetAttr()
	if attr == nil {
		return
	}

	out.Size = attr.GetSize()
	out.Blocks = attr.GetBlocks()
	out.Mode = attr.GetMode()
	out.Nlink = attr.GetNlink()
	out.Uid = attr.GetUid()
	out.Gid = attr.GetGid()
	out.Rdev = attr.GetRdev()
	out.Blksize = attr.GetBlksize()

	if atime := attr.GetAtime(); atime != nil {
		out.Atime = uint64(atime.GetSeconds())
		out.Atimensec = uint32(atime.GetNanos())
	}
	if mtime := attr.GetMtime(); mtime != nil {
		out.Mtime = uint64(mtime.GetSeconds())
		out.Mtimensec = uint32(mtime.GetNanos())
	}
	if ctime := attr.GetCtime(); ctime != nil {
		out.Ctime = uint64(ctime.GetSeconds())
		out.Ctimensec = uint32(ctime.GetNanos())
	}
}

// FillEntryOut 精确填充 Lookup 等操作所需的 fuse.EntryOut 结构。
func (fs *RawFS) FillEntryOut(out *fuse.EntryOut, ino *storagev1.Inode, entryTimeoutSec, attrTimeoutSec float64) {
	if out == nil || ino == nil {
		return
	}
	out.NodeId = fs.ToKernelIno(ino.GetIno())
	out.Generation = ino.GetGeneration()
	out.SetEntryTimeout(time.Duration(entryTimeoutSec * float64(time.Second)))
	out.SetAttrTimeout(time.Duration(attrTimeoutSec * float64(time.Second)))
	fs.FillAttr(&out.Attr, ino)
}

// FillAttrOut 精确填充 GetAttr 等操作所需的 fuse.AttrOut 结构。
func (fs *RawFS) FillAttrOut(out *fuse.AttrOut, ino *storagev1.Inode, attrTimeoutSec float64) {
	if out == nil || ino == nil {
		return
	}
	out.SetTimeout(time.Duration(attrTimeoutSec * float64(time.Second)))
	fs.FillAttr(&out.Attr, ino)
}

// BuildMountOptions 根据 Config 构造标准化的 fuse.MountOptions。
func BuildMountOptions(cfg Config) *fuse.MountOptions {
	options := []string{"default_permissions"}
	if cfg.ReadOnly {
		options = append(options, "ro")
	}

	return &fuse.MountOptions{
		FsName:        "bowl:" + cfg.DiskID,
		Name:          "bowl",
		Options:       options,
		AllowOther:    cfg.AllowOther,
		EnableLocks:   true,
		DisableXAttrs: false,
		MaxWrite:      1 << 20,
		MaxReadAhead:  1 << 20,
		// 64 个后台异步请求配合 3/4 拥塞阈值，释放多线程大块顺序写与小文件并发吞吐。
		MaxBackground:       64,
		CongestionThreshold: 48,
		// 由文件系统自身在写批次中清除 security.capability，避免内核写前 GetXattr 探测。
		ExtraCapabilities: fuse.CAP_HANDLE_KILLPRIV,
		// 关闭内核 writeback 缓存，保持 1 MiB 直通写，避免 4 KiB 脏页回写与写前尺寸核对。
		// 同时禁用 CAP_AUTO_INVAL_DATA：Bowl 是单分支 SWMR（同一 (disk,branch) 同时最多 1 个可写会话，
		// 只读会话只能挂不可变 Commit），同一挂载视图内不存在第三方写入，内核页缓存只会被本地 write()
		// 更新，不会被“外部修改”。而服务端 mtime 来自 Go 时钟、内核 i_mtime 来自 file_update_time()，
		// 两者在非 fsync 路径上通常不等；保留 auto-inval 会让写后第一次 read() 先发一次 GetAttr，
		// 并因 mtime 不等整文件清空页缓存。这里只关闭 AUTO_INVAL，不开启 EXPLICIT_INVAL：
		// 尺寸变化仍由 fuse_change_attributes 的 oldsize != attr->size 分支做
		// truncate_pagecache + invalidate_inode_pages2 兜底。
		DisabledCapabilities: fuse.CAP_WRITEBACK_CACHE | fuse.CAP_AUTO_INVAL_DATA,
		DirectMount:          true,
		Debug:                cfg.Debug,
	}
}
