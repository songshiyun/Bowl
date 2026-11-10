package store

import (
	"log/slog"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// SetBeforeCompactRelocateHookForTest 注册在搬迁活块读取追加完成、向 StoreActor 投递 opCompactRelocate 前触发的测试钩子。
func (s *StoreActor) SetBeforeCompactRelocateHookForTest(hook func()) {
	s.beforeCompactRelocateHook = hook
}

// SetSyncFileHookForTest 注册在 BlobStore.syncPack 执行底层 syncFileData 前触发的测试钩子。
func (s *BlobStore) SetSyncFileHookForTest(hook func(packID uint64)) {
	s.mu.Lock()
	s.syncFileHook = hook
	s.mu.Unlock()
}

// CloseCachedPackFDForTest 关闭指定 packID 在常驻句柄表中的底层 fd（保留键项以触发真实 fdatasync EBADF/ErrClosed）。
func (s *BlobStore) CloseCachedPackFDForTest(packID uint64) {
	s.filesMu.RLock()
	f := s.packFiles[packID]
	s.filesMu.RUnlock()
	if f != nil {
		_ = f.Close()
	}
}

type slowWriteFS struct {
	vfs.FS
	mu      sync.Mutex
	armed   bool
	unblock chan struct{}
}

func (fs *slowWriteFS) maybeWaitSync() {
	fs.mu.Lock()
	if !fs.armed {
		fs.mu.Unlock()
		return
	}
	fs.armed = false
	ch := fs.unblock
	fs.mu.Unlock()

	select {
	case <-ch:
	case <-time.After(4 * time.Second):
	}
}

type slowWriteFile struct {
	vfs.File
	fs *slowWriteFS
}

func (f *slowWriteFile) Sync() error {
	f.fs.maybeWaitSync()
	return f.File.Sync()
}

func (f *slowWriteFile) SyncData() error {
	f.fs.maybeWaitSync()
	return f.File.SyncData()
}

func (f *slowWriteFile) SyncTo(length int64) (bool, error) {
	f.fs.maybeWaitSync()
	return f.File.SyncTo(length)
}

func (fs *slowWriteFS) Create(name string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.Create(name, category)
	if err != nil {
		return nil, err
	}
	return &slowWriteFile{File: f, fs: fs}, nil
}

func (fs *slowWriteFS) ReuseForWrite(oldname, newname string, category vfs.DiskWriteCategory) (vfs.File, error) {
	f, err := fs.FS.ReuseForWrite(oldname, newname, category)
	if err != nil {
		return nil, err
	}
	return &slowWriteFile{File: f, fs: fs}, nil
}

// OpenWithSlowPebbleSyncForTest 使用可挂载单次 Sync 阻塞栅栏的真实磁盘 VFS 打开 StoreActor，
// 用于端到端验证 vfs.WithDiskHealthChecks 后台监控协程（2s 周期采样 in-flight I/O）真实触发 DiskSlow 回调。
func OpenWithSlowPebbleSyncForTest(opts *Options, slowThreshold time.Duration) (*StoreActor, func() (release func()), error) {
	sfs := &slowWriteFS{FS: vfs.Default}
	cp := *opts
	cp.pebbleFS = sfs
	cp.diskSlowThreshold = slowThreshold
	actor, err := Open(cp)
	if err != nil {
		return nil, nil, err
	}
	arm := func() func() {
		ch := make(chan struct{})
		var once sync.Once
		sfs.mu.Lock()
		sfs.unblock = ch
		sfs.armed = true
		sfs.mu.Unlock()
		return func() {
			once.Do(func() { close(ch) })
		}
	}
	return actor, arm, nil
}

// SimulatePebbleStallForTest 触发底层 Pebble EventListener 的 WriteStallBegin / WriteStallEnd 回调。
func (s *StoreActor) SimulatePebbleStallForTest(begin bool, reason string) {
	if s.pebbleListener == nil {
		return
	}
	if begin {
		s.pebbleListener.WriteStallBegin(pebble.WriteStallBeginInfo{Reason: reason})
	} else {
		s.pebbleListener.WriteStallEnd()
	}
}

// ExerciseSlogPebbleLoggerForTest 供单测覆盖验证 slogPebbleLogger 的分级路由与 Fatalf 退出钩子。
func ExerciseSlogPebbleLoggerForTest(logger *slog.Logger, diskID string) (exitCode int) {
	pl := newSlogPebbleLogger(logger, diskID)
	pl.exitFn = func(code int) { exitCode = code }
	pl.Infof("compaction %d finished\n", 1)
	pl.Warnf("manifest slow %s\n", "op")
	pl.Errorf("wal write failed: %s\n", "eio")
	pl.Fatalf("corrupt sstable %d", 7)
	return exitCode
}

// ExerciseMetricsZeroAllocForTest 供单测验证 Metrics 热路径打点方法零堆分配。
func ExerciseMetricsZeroAllocForTest(m *Metrics) {
	m.observeQueueWait(100 * time.Microsecond)
	m.observeTickBatchSize(8)
	m.observePhaseValidateMutate(50 * time.Microsecond)
	m.observePhasePackSyncBarrier(time.Millisecond)
	m.observePhaseWALApplySync(time.Millisecond)
	m.observePhaseWALApplyNoSync(20 * time.Microsecond)
	m.observePhaseDeadPackRemove(100 * time.Microsecond)
	m.observeInodeLockWait(30 * time.Microsecond)
	m.recordBlobWrite(writeModeFullBlock, 1, BlobSize)
	m.recordBlobWrite(writeModeExtendTail, 1, 4096)
	m.recordBlobWrite(writeModeRMW, 1, 8192)
	m.observeBlobRMWRead(10 * time.Microsecond)
	m.observeBlobMuWait(5 * time.Microsecond)
	m.observeBlobPwrite(50 * time.Microsecond)
	m.observeBlobSync(syncTargetPackData, syncTriggerFsyncBarrier, time.Millisecond)
	m.incReadHotCache()
	m.incReadDiskPack()
	m.addHotCacheEvictions(1)
	m.incEpochFenced()
	m.incPebbleStallEvents()
	m.incPebbleDiskSlow()
	m.incBgSyncErrors()
	m.incPackRemoveErrors()
}

// HoldTickForTest 占住当前一轮 Actor Tick，使并发测试可将多条真实操作攒入同一 mb.active 批次并在下一轮 Tick 合并演进；
// 调用返回的 release 函数即可放行当前 Tick。
func HoldTickForTest(s *StoreActor) (release func()) {
	entered := make(chan struct{})
	unblock := make(chan struct{})
	done := make(chan struct{})
	var once sync.Once

	s.beforeTickHook = func() {
		once.Do(func() {
			close(entered)
			<-unblock
			s.beforeTickHook = nil
		})
	}
	go func() {
		cmd := s.acquireCmd()
		cmd.op = opNoop
		_ = s.submit(cmd)
		close(done)
	}()
	<-entered
	return func() {
		close(unblock)
		<-done
	}
}
