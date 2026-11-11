package fusefs

import (
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/prometheus/client_golang/prometheus"
)

// InstrumentedOption 配置 InstrumentedRawFS 的可选参数。
type InstrumentedOption func(*InstrumentedRawFS)

// WithMetrics 指定自定义 Metrics 实例；传入 nil 时忽略。
func WithMetrics(m *Metrics) InstrumentedOption {
	return func(f *InstrumentedRawFS) {
		if m != nil {
			f.metrics = m
		}
	}
}

// InstrumentedRawFS 为 RawFS 的全部 36 个 FUSE 操作提供零分配（0 B/op, 0 allocs/op）、零互斥锁的可观测性装饰器。
type InstrumentedRawFS struct {
	raw     *RawFS
	metrics *Metrics
}

var _ fuse.RawFileSystem = (*InstrumentedRawFS)(nil)

// NewInstrumentedRawFS 包装底层 RawFS 并绑定统一的 Metrics 数据源。
func NewInstrumentedRawFS(raw *RawFS, opts ...InstrumentedOption) *InstrumentedRawFS {
	var m *Metrics
	if raw != nil {
		m = raw.Metrics()
	}
	if m == nil {
		m = NewMetrics()
	}
	ins := &InstrumentedRawFS{
		raw:     raw,
		metrics: m,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(ins)
		}
	}
	if raw != nil {
		raw.bindMetrics(ins.metrics)
	}
	return ins
}

// Raw 返回底层被包装的 RawFS 实例。
func (f *InstrumentedRawFS) Raw() *RawFS {
	return f.raw
}

// Metrics 返回绑定的统一指标实例。
func (f *InstrumentedRawFS) Metrics() *Metrics {
	return f.metrics
}

// Collector 返回可直接注册至自定义 prometheus.Registerer 的指标收集器。
func (f *InstrumentedRawFS) Collector() prometheus.Collector {
	return f.metrics
}

// Snapshot 抓取当前时刻 FUSE 36 操作与 8 大子系统的结构化一致性快照。
func (f *InstrumentedRawFS) Snapshot() Snapshot {
	return f.metrics.Snapshot()
}

// --- 8 个非统计/未实现透传方法（满足 fuse.RawFileSystem 接口） ---

// String 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) String() string {
	return f.raw.String()
}

// SetDebug 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) SetDebug(debug bool) {
	f.raw.SetDebug(debug)
}

// Init 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) Init(server *fuse.Server) {
	f.raw.Init(server)
}

// Access 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) Access(cancel <-chan struct{}, input *fuse.AccessIn) fuse.Status {
	return f.raw.Access(cancel, input)
}

// Lseek 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	return f.raw.Lseek(cancel, in, out)
}

// Writev 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) Writev(cancel <-chan struct{}, input *fuse.WriteIn, data [][]byte) (uint32, fuse.Status) {
	return f.raw.Writev(cancel, input, data)
}

// CopyFileRange 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (uint32, fuse.Status) {
	return f.raw.CopyFileRange(cancel, input)
}

// Statx 直接透传至底层 RawFS，不计入操作统计。
func (f *InstrumentedRawFS) Statx(cancel <-chan struct{}, input *fuse.StatxIn, out *fuse.StatxOut) fuse.Status {
	return f.raw.Statx(cancel, input, out)
}

// --- 36 个 FUSE 操作零分配埋点入口 ---

// Lookup 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) fuse.Status {
	start := f.metrics.beginOp(OpLookup)
	status := f.raw.Lookup(cancel, header, name, out)
	f.metrics.endOp(OpLookup, start, status, 0)
	return status
}

// GetAttr 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) fuse.Status {
	start := f.metrics.beginOp(OpGetAttr)
	status := f.raw.GetAttr(cancel, input, out)
	f.metrics.endOp(OpGetAttr, start, status, 0)
	return status
}

// StatFs 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) StatFs(cancel <-chan struct{}, input *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	start := f.metrics.beginOp(OpStatFs)
	status := f.raw.StatFs(cancel, input, out)
	f.metrics.endOp(OpStatFs, start, status, 0)
	return status
}

// Mkdir 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) fuse.Status {
	start := f.metrics.beginOp(OpMkdir)
	status := f.raw.Mkdir(cancel, input, name, out)
	f.metrics.endOp(OpMkdir, start, status, 0)
	return status
}

// Mknod 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) fuse.Status {
	start := f.metrics.beginOp(OpMknod)
	status := f.raw.Mknod(cancel, input, name, out)
	f.metrics.endOp(OpMknod, start, status, 0)
	return status
}

// Rmdir 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	start := f.metrics.beginOp(OpRmdir)
	status := f.raw.Rmdir(cancel, header, name)
	f.metrics.endOp(OpRmdir, start, status, 0)
	return status
}

// Unlink 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) fuse.Status {
	start := f.metrics.beginOp(OpUnlink)
	status := f.raw.Unlink(cancel, header, name)
	f.metrics.endOp(OpUnlink, start, status, 0)
	return status
}

// Rename 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName, newName string) fuse.Status {
	start := f.metrics.beginOp(OpRename)
	status := f.raw.Rename(cancel, input, oldName, newName)
	f.metrics.endOp(OpRename, start, status, 0)
	return status
}

// Symlink 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo, linkName string, out *fuse.EntryOut) fuse.Status {
	start := f.metrics.beginOp(OpSymlink)
	status := f.raw.Symlink(cancel, header, pointedTo, linkName, out)
	f.metrics.endOp(OpSymlink, start, status, 0)
	return status
}

// Readlink 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Readlink(cancel <-chan struct{}, header *fuse.InHeader) ([]byte, fuse.Status) {
	start := f.metrics.beginOp(OpReadlink)
	target, status := f.raw.Readlink(cancel, header)
	f.metrics.endOp(OpReadlink, start, status, 0)
	return target, status
}

// Link 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Link(cancel <-chan struct{}, input *fuse.LinkIn, filename string, out *fuse.EntryOut) fuse.Status {
	start := f.metrics.beginOp(OpLink)
	status := f.raw.Link(cancel, input, filename, out)
	f.metrics.endOp(OpLink, start, status, 0)
	return status
}

// Create 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) fuse.Status {
	start := f.metrics.beginOp(OpCreate)
	status := f.raw.Create(cancel, input, name, out)
	f.metrics.endOp(OpCreate, start, status, 0)
	return status
}

// Open 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	start := f.metrics.beginOp(OpOpen)
	status := f.raw.Open(cancel, input, out)
	f.metrics.endOp(OpOpen, start, status, 0)
	return status
}

// Flush 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	start := f.metrics.beginOp(OpFlush)
	status := f.raw.Flush(cancel, input)
	f.metrics.endOp(OpFlush, start, status, 0)
	return status
}

// Fsync 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	start := f.metrics.beginOp(OpFsync)
	status := f.raw.Fsync(cancel, input)
	f.metrics.endOp(OpFsync, start, status, 0)
	return status
}

// Release 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
	start := f.metrics.beginOp(OpRelease)
	f.raw.Release(cancel, input)
	f.metrics.endOp(OpRelease, start, fuse.OK, 0)
}

// OpenDir 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) fuse.Status {
	start := f.metrics.beginOp(OpOpenDir)
	status := f.raw.OpenDir(cancel, input, out)
	f.metrics.endOp(OpOpenDir, start, status, 0)
	return status
}

// ReadDir 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	start := f.metrics.beginOp(OpReadDir)
	status := f.raw.ReadDir(cancel, input, out)
	f.metrics.endOp(OpReadDir, start, status, 0)
	return status
}

// ReadDirPlus 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	start := f.metrics.beginOp(OpReadDirPlus)
	status := f.raw.ReadDirPlus(cancel, input, out)
	f.metrics.endOp(OpReadDirPlus, start, status, 0)
	return status
}

// FsyncDir 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) fuse.Status {
	start := f.metrics.beginOp(OpFsyncDir)
	status := f.raw.FsyncDir(cancel, input)
	f.metrics.endOp(OpFsyncDir, start, status, 0)
	return status
}

// Read 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	start := f.metrics.beginOp(OpRead)
	res, status := f.raw.Read(cancel, input, buf)
	var n uint64
	if status == fuse.OK && res != nil {
		n = uint64(res.Size())
	}
	f.metrics.endOp(OpRead, start, status, n)
	return res, status
}

// Write 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (uint32, fuse.Status) {
	start := f.metrics.beginOp(OpWrite)
	written, status := f.raw.Write(cancel, input, data)
	var n uint64
	if status == fuse.OK {
		n = uint64(written)
	}
	f.metrics.endOp(OpWrite, start, status, n)
	return written, status
}

// SetAttr 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) fuse.Status {
	start := f.metrics.beginOp(OpSetAttr)
	status := f.raw.SetAttr(cancel, input, out)
	f.metrics.endOp(OpSetAttr, start, status, 0)
	return status
}

// Fallocate 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Fallocate(cancel <-chan struct{}, input *fuse.FallocateIn) fuse.Status {
	start := f.metrics.beginOp(OpFallocate)
	status := f.raw.Fallocate(cancel, input)
	f.metrics.endOp(OpFallocate, start, status, 0)
	return status
}

// SetXAttr 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	start := f.metrics.beginOp(OpSetXAttr)
	status := f.raw.SetXAttr(cancel, input, attr, data)
	f.metrics.endOp(OpSetXAttr, start, status, 0)
	return status
}

// GetXAttr 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (uint32, fuse.Status) {
	start := f.metrics.beginOp(OpGetXAttr)
	sz, status := f.raw.GetXAttr(cancel, header, attr, dest)
	f.metrics.endOp(OpGetXAttr, start, status, 0)
	return sz, status
}

// ListXAttr 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (uint32, fuse.Status) {
	start := f.metrics.beginOp(OpListXAttr)
	sz, status := f.raw.ListXAttr(cancel, header, dest)
	f.metrics.endOp(OpListXAttr, start, status, 0)
	return sz, status
}

// RemoveXAttr 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	start := f.metrics.beginOp(OpRemoveXAttr)
	status := f.raw.RemoveXAttr(cancel, header, attr)
	f.metrics.endOp(OpRemoveXAttr, start, status, 0)
	return status
}

// Ioctl 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Ioctl(cancel <-chan struct{}, input *fuse.IoctlIn, inbuf []byte, output *fuse.IoctlOut, outbuf []byte) fuse.Status {
	start := f.metrics.beginOp(OpIoctl)
	status := f.raw.Ioctl(cancel, input, inbuf, output, outbuf)
	f.metrics.endOp(OpIoctl, start, status, 0)
	return status
}

// OnUnmount 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) OnUnmount() {
	start := f.metrics.beginOp(OpOnUnmount)
	f.raw.OnUnmount()
	f.metrics.endOp(OpOnUnmount, start, fuse.OK, 0)
}

// ReleaseDir 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) ReleaseDir(input *fuse.ReleaseIn) {
	start := f.metrics.beginOp(OpReleaseDir)
	f.raw.ReleaseDir(input)
	f.metrics.endOp(OpReleaseDir, start, fuse.OK, 0)
}

// Tmpfile 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Tmpfile(cancel <-chan struct{}, input *fuse.CreateIn, out *fuse.CreateOut) fuse.Status {
	start := f.metrics.beginOp(OpTmpfile)
	status := f.raw.Tmpfile(cancel, input, out)
	f.metrics.endOp(OpTmpfile, start, status, 0)
	return status
}

// Forget 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) Forget(nodeid, nlookup uint64) {
	start := f.metrics.beginOp(OpForget)
	f.raw.Forget(nodeid, nlookup)
	f.metrics.endOp(OpForget, start, fuse.OK, 0)
}

// GetLk 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) GetLk(cancel <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	start := f.metrics.beginOp(OpGetLk)
	status := f.raw.GetLk(cancel, input, out)
	f.metrics.endOp(OpGetLk, start, status, 0)
	return status
}

// SetLk 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) SetLk(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	start := f.metrics.beginOp(OpSetLk)
	status := f.raw.SetLk(cancel, input)
	f.metrics.endOp(OpSetLk, start, status, 0)
	return status
}

// SetLkw 记录调用次数、在途数、耗时直方图与 errno 后委托底层 RawFS 执行。
func (f *InstrumentedRawFS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	start := f.metrics.beginOp(OpSetLkw)
	status := f.raw.SetLkw(cancel, input)
	f.metrics.endOp(OpSetLkw, start, status, 0)
	return status
}
