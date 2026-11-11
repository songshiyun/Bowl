package fusefs

import (
	"context"
	"sync"
	"sync/atomic"
	"syscall"

	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const (
	// WriteBehindChunkSize 定义客户端顺序写聚合槽位上限 (B* = 4 MiB = MaxRPCChunkSize)。
	WriteBehindChunkSize = MaxRPCChunkSize

	// WriteBehindMaxInFlight 定义单文件最大在途并发 Write RPC 数 (W* = 4，峰值 16 MiB 在途)。
	WriteBehindMaxInFlight = 4

	// WriteBehindBlockAlign 与服务端 store.BlobSize (256 KiB) 严格一致：
	// 仅当聚合起始偏移按 256 KiB 对齐且异步分片为 4 MiB 整倍数时才允许并发在途，
	// 从数学上保证任意两个在途 Write RPC 覆盖完全不相交的 256 KiB 物理块区间。
	WriteBehindBlockAlign = 256 * 1024

	// WriteBehindActivateBytes 为激活异步写聚合前的同步探针字节阈值 (2 MiB)：
	// 首段同步写入直接透传验证 Epoch 栅栏、Immutable/Append 标志、KILLPRIV 与小盘配额，
	// 对小文件与数据库页写保持零缓冲放大、零额外协程开销。
	WriteBehindActivateBytes = 2 * 1024 * 1024

	// WriteBehindMinStageWrite 为参与顺序流探测与聚合的最小单次写入字节数 (64 KiB)。
	WriteBehindMinStageWrite = 64 * 1024

	// writeBehindRetainedBufs 为常驻有界 noscan 4 MiB 缓冲槽数（8 x 4 MiB = 32 MiB，免受 GC 周期驱逐）。
	writeBehindRetainedBufs = 8
)

var (
	writeBehindFreeCh  = make(chan *[WriteBehindChunkSize]byte, writeBehindRetainedBufs)
	writeBehindBufPool = sync.Pool{
		New: func() any {
			return new([WriteBehindChunkSize]byte)
		},
	}
)

func acquireWriteBehindBuf() *[WriteBehindChunkSize]byte {
	select {
	case buf := <-writeBehindFreeCh:
		return buf
	default:
	}
	buf, _ := writeBehindBufPool.Get().(*[WriteBehindChunkSize]byte)
	if buf == nil {
		return new([WriteBehindChunkSize]byte)
	}
	return buf
}

func releaseWriteBehindBuf(buf *[WriteBehindChunkSize]byte) {
	if buf == nil {
		return
	}
	select {
	case writeBehindFreeCh <- buf:
	default:
		writeBehindBufPool.Put(buf)
	}
}

// ChunkWriter 定义向底层存储服务提交单块写入的端口函数。
type ChunkWriter func(ctx context.Context, ino, offset uint64, data []byte) (uint32, *storagev1.Inode, error)

// WriteBehindHooks 定义异步分片完成后的本地状态同步钩子。
type WriteBehindHooks struct {
	OnCommit func(nodeID, ino, cacheEpoch uint64, inode *storagev1.Inode, refreshAttr bool)
	OnError  func(nodeID uint64)
}

type inodeWriteBehind struct {
	// opMu 串行化同一 Inode 的前台 Write / Flush / Barrier / Reset / Remove 操作，
	// 杜绝 st.cond.Wait() 释放 st.mu 期间被并发前台请求穿插篡改聚合槽或流偏移；
	// 后台在途提交协程绝不获取 opMu，严格维持 opMu -> {mu, c.mu} -> c.seqMu 单向无环锁序。
	opMu           sync.Mutex
	mu             sync.Mutex
	cond           *sync.Cond
	ino            uint64
	removed        bool
	nodeID         uint64
	cacheEpoch     uint64
	probedBytes    uint64
	expectedOffset uint64
	hasStream      bool
	streamGen      uint64
	stageBuf       *[WriteBehindChunkSize]byte
	stageOffset    uint64
	stageLen       int
	stageGen       uint64
	inflight       int
	lastCommitted  *storagev1.Inode
	errGen         uint64
	errSeq         uint64
	lastErr        error
	defaultSeenSeq uint64
}

func newInodeWriteBehind(ino uint64) *inodeWriteBehind {
	st := &inodeWriteBehind{
		ino:       ino,
		streamGen: 1,
	}
	st.cond = sync.NewCond(&st.mu)
	return st
}

func compareProtoTimestamp(a, b *timestamppb.Timestamp) int {
	var aSec, bSec int64
	var aNano, bNano int32
	if a != nil {
		aSec, aNano = a.GetSeconds(), a.GetNanos()
	}
	if b != nil {
		bSec, bNano = b.GetSeconds(), b.GetNanos()
	}
	if aSec != bSec {
		if aSec > bSec {
			return 1
		}
		return -1
	}
	if aNano != bNano {
		if aNano > bNano {
			return 1
		}
		return -1
	}
	return 0
}

func isNotOlderCommittedInode(prev, cand *storagev1.Inode) bool {
	if cand == nil || cand.GetAttr() == nil {
		return false
	}
	if prev == nil || prev.GetAttr() == nil {
		return true
	}
	candAttr := cand.GetAttr()
	prevAttr := prev.GetAttr()
	// 在同一连续顺序写世代（streamGen）内无截断介入，服务端文件尺寸单调不减：
	// 更大 Size 必定晚于更小 Size 在服务端提交；Size 相同时（乱序先写高偏移再补低偏移）取时间戳更新者。
	if candAttr.GetSize() != prevAttr.GetSize() {
		return candAttr.GetSize() > prevAttr.GetSize()
	}
	if cmp := compareProtoTimestamp(candAttr.GetCtime(), prevAttr.GetCtime()); cmp != 0 {
		return cmp > 0
	}
	return compareProtoTimestamp(candAttr.GetMtime(), prevAttr.GetMtime()) >= 0
}

func (st *inodeWriteBehind) resetStreamLocked() {
	st.hasStream = false
	st.probedBytes = 0
	st.streamGen++
	st.lastCommitted = nil
}

func (st *inodeWriteBehind) discardStageLocked(pendingCount *atomic.Int64) {
	if st.stageBuf != nil {
		releaseWriteBehindBuf(st.stageBuf)
		st.stageBuf = nil
		pendingCount.Add(-1)
	}
	st.stageOffset = 0
	st.stageLen = 0
	st.stageGen = 0
}

func (st *inodeWriteBehind) waitInflightLocked() {
	for st.inflight > 0 {
		st.cond.Wait()
	}
}

func (st *inodeWriteBehind) hasUnseenErrLocked(c *WriteBehindCoordinator, fh uint64) bool {
	if st.errSeq == 0 {
		return false
	}
	if fh == 0 {
		return st.defaultSeenSeq < st.errSeq
	}
	return c.handleSeenSeq(fh) < st.errSeq
}

func (st *inodeWriteBehind) consumeErrLocked(c *WriteBehindCoordinator, fh uint64) error {
	if st.errSeq == 0 {
		return nil
	}
	if fh == 0 {
		if st.defaultSeenSeq >= st.errSeq {
			return nil
		}
		st.defaultSeenSeq = st.errSeq
		st.resetStreamLocked()
		return st.lastErr
	}
	if !c.advanceHandleSeenSeq(fh, st.errSeq) {
		return nil
	}
	st.resetStreamLocked()
	return st.lastErr
}

func (st *inodeWriteBehind) dispatchLocked(c *WriteBehindCoordinator) bool {
	for {
		if st.stageBuf == nil || st.stageLen == 0 {
			st.discardStageLocked(&c.pendingCount)
			return true
		}
		if st.stageGen != st.streamGen {
			st.discardStageLocked(&c.pendingCount)
			return false
		}
		if st.inflight < WriteBehindMaxInFlight {
			break
		}
		st.cond.Wait()
	}

	buf := st.stageBuf
	chunkOff := st.stageOffset
	chunkLen := st.stageLen
	ino := st.ino
	nodeID := st.nodeID
	cacheEpoch := st.cacheEpoch
	gen := st.stageGen
	writer := c.writer
	hooks := c.hooks

	// 所有权从 stageBuf 转移至在途协程，总挂起计数保持不变，在协程收口递减 inflight 时再减 1。
	st.stageBuf = nil
	st.stageOffset = 0
	st.stageLen = 0
	st.stageGen = 0
	st.inflight++

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), DefaultRPCTimeout)
		written, respInode, err := writer(ctx, ino, chunkOff, buf[:chunkLen:chunkLen])
		cancel()
		releaseWriteBehindBuf(buf)

		if err == nil && written < uint32(chunkLen) {
			err = syscall.EIO
		}

		// 在同一临界区内串行完成水位比较、本地缓存/脏集回填与 inflight 递减唤醒，
		// 彻底杜绝锁外并发 OnCommit 超车导致文件尺寸或时间戳倒退。
		st.mu.Lock()
		if err != nil {
			if st.errGen != gen {
				st.errGen = gen
				st.lastErr = err
				st.errSeq = c.errSeqGen.Add(1)
			}
			if st.streamGen == gen {
				st.resetStreamLocked()
			}
			if hooks.OnError != nil {
				hooks.OnError(nodeID)
			}
		} else {
			refreshAttr := false
			if gen == st.streamGen && isNotOlderCommittedInode(st.lastCommitted, respInode) {
				st.lastCommitted = cloneInode(respInode)
				refreshAttr = true
			}
			if hooks.OnCommit != nil {
				hooks.OnCommit(nodeID, ino, cacheEpoch, respInode, refreshAttr)
			}
		}
		st.inflight--
		c.pendingCount.Add(-1)
		st.cond.Broadcast()
		st.mu.Unlock()
	}()

	return true
}

func (st *inodeWriteBehind) barrierLocked(c *WriteBehindCoordinator) {
	if st.stageBuf != nil || st.stageLen > 0 {
		st.dispatchLocked(c)
	}
	st.waitInflightLocked()
}

// WriteBehindCoordinator 与 ReadAheadCoordinator 对称，负责将 Linux FUSE 拆碎的连续小块写（<= 1 MiB）
// 聚合成 B*=4 MiB 分片并以有界并发度 W*=4 流水线提交至服务端，同时在 Flush/Fsync/Read/GetAttr 等屏障处排空。
type WriteBehindCoordinator struct {
	mu           sync.RWMutex
	inodes       map[uint64]*inodeWriteBehind
	pendingCount atomic.Int64
	errSeqGen    atomic.Uint64
	seqMu        sync.Mutex
	handleSeq    map[uint64]uint64
	writer       ChunkWriter
	hooks        WriteBehindHooks
}

// NewWriteBehindCoordinator 创建并初始化 WriteBehindCoordinator。
func NewWriteBehindCoordinator(writer ChunkWriter, hooks WriteBehindHooks) *WriteBehindCoordinator {
	return &WriteBehindCoordinator{
		inodes:    make(map[uint64]*inodeWriteBehind),
		handleSeq: make(map[uint64]uint64),
		writer:    writer,
		hooks:     hooks,
	}
}

// RegisterHandle 在打开文件句柄时按 Linux errseq_t 语义采样当前全局错误水位，
// 确保新打开句柄（如 O_TRUNC 空间恢复）不会被打开前的历史异步错误污染，而旧句柄仍能精确收到自身写入期间的错误。
func (c *WriteBehindCoordinator) RegisterHandle(fh uint64) {
	if fh == 0 {
		return
	}
	seq := c.errSeqGen.Load()
	if seq == 0 {
		return
	}
	c.seqMu.Lock()
	if c.handleSeq == nil {
		c.handleSeq = make(map[uint64]uint64)
	}
	c.handleSeq[fh] = seq
	c.seqMu.Unlock()
}

// ForgetHandle 在 Release 关闭本地文件句柄时清理其错误游标。
func (c *WriteBehindCoordinator) ForgetHandle(fh uint64) {
	if fh == 0 || c.errSeqGen.Load() == 0 {
		return
	}
	c.seqMu.Lock()
	delete(c.handleSeq, fh)
	c.seqMu.Unlock()
}

func (c *WriteBehindCoordinator) handleSeenSeq(fh uint64) uint64 {
	c.seqMu.Lock()
	seen := c.handleSeq[fh]
	c.seqMu.Unlock()
	return seen
}

func (c *WriteBehindCoordinator) advanceHandleSeenSeq(fh, target uint64) bool {
	c.seqMu.Lock()
	if c.handleSeq == nil {
		c.handleSeq = make(map[uint64]uint64)
	}
	if c.handleSeq[fh] >= target {
		c.seqMu.Unlock()
		return false
	}
	c.handleSeq[fh] = target
	c.seqMu.Unlock()
	return true
}

func (c *WriteBehindCoordinator) getInode(ino uint64) *inodeWriteBehind {
	c.mu.RLock()
	st := c.inodes[ino]
	c.mu.RUnlock()
	return st
}

func (c *WriteBehindCoordinator) getOrCreateInode(ino uint64) *inodeWriteBehind {
	if st := c.getInode(ino); st != nil {
		return st
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.inodes == nil {
		c.inodes = make(map[uint64]*inodeWriteBehind)
	}
	st := c.inodes[ino]
	if st == nil {
		st = newInodeWriteBehind(ino)
		c.inodes[ino] = st
	}
	return st
}

func (c *WriteBehindCoordinator) acquireInode(ino uint64) *inodeWriteBehind {
	for {
		st := c.getOrCreateInode(ino)
		st.opMu.Lock()
		if !st.removed {
			return st
		}
		st.opMu.Unlock()
	}
}

func (c *WriteBehindCoordinator) lockExistingInode(ino uint64) *inodeWriteBehind {
	st := c.getInode(ino)
	if st == nil {
		return nil
	}
	st.opMu.Lock()
	if st.removed {
		st.opMu.Unlock()
		return nil
	}
	return st
}

// HasAnyPending 无锁报告当前是否存在任何尚未落地的聚合缓冲或在途异步 Write RPC。
func (c *WriteBehindCoordinator) HasAnyPending() bool {
	return c.pendingCount.Load() > 0
}

// HasPending 报告指定服务端 Inode 是否存在尚未落地的聚合缓冲或在途异步 Write RPC。
func (c *WriteBehindCoordinator) HasPending(ino uint64) bool {
	if c.pendingCount.Load() == 0 {
		return false
	}
	st := c.getInode(ino)
	if st == nil {
		return false
	}
	st.mu.Lock()
	pending := st.stageBuf != nil || st.stageLen > 0 || st.inflight > 0
	st.mu.Unlock()
	return pending
}

// Write 尝试将顺序写入纳入 4 MiB 聚合槽与 W*=4 异步流水线（默认无句柄游标模式）。
func (c *WriteBehindCoordinator) Write(ino, nodeID, offset uint64, data []byte, cacheEpoch uint64) (written uint32, handled bool, err error) {
	return c.WriteForHandle(0, ino, nodeID, offset, data, cacheEpoch)
}

// WriteForHandle 尝试将指定句柄 fh 的顺序写入纳入 4 MiB 聚合槽与 W*=4 异步流水线：
//   - 若 handled == true，说明本次请求已由流水线接管（或捕获到该句柄尚未观测的异步写错误 err）；
//   - 若 handled == false，说明当前处于首段探针期、小块直通或非连续跳转（已自动排空旧缓冲），调用方应走同步 RPC。
func (c *WriteBehindCoordinator) WriteForHandle(fh, ino, nodeID, offset uint64, data []byte, cacheEpoch uint64) (written uint32, handled bool, err error) {
	if len(data) == 0 || len(data) > WriteBehindChunkSize || c.writer == nil {
		if flushErr := c.FlushAndResetInodeForHandle(fh, ino); flushErr != nil {
			return 0, true, flushErr
		}
		return 0, false, nil
	}

	if len(data) < WriteBehindMinStageWrite && c.getInode(ino) == nil && c.errSeqGen.Load() == 0 {
		return 0, false, nil
	}
	st := c.acquireInode(ino)
	defer st.opMu.Unlock()

	st.mu.Lock()
	if st.hasUnseenErrLocked(c, fh) {
		st.barrierLocked(c)
		err = st.consumeErrLocked(c, fh)
		st.mu.Unlock()
		return 0, true, err
	}

	// 非连续写入：先排空已缓冲数据与在途分片，再回退至同步写入重新探测。
	if !st.hasStream || offset != st.expectedOffset {
		st.barrierLocked(c)
		if err = st.consumeErrLocked(c, fh); err != nil {
			st.mu.Unlock()
			return 0, true, err
		}
		st.resetStreamLocked()
		st.mu.Unlock()
		return 0, false, nil
	}

	// 尚未达到激活阈值，或当前无活跃槽且尚未对齐到 256 KiB 块边界 / 写入过小：保持同步直通。
	if st.probedBytes < WriteBehindActivateBytes ||
		(st.stageBuf == nil && (len(data) < WriteBehindMinStageWrite || offset%WriteBehindBlockAlign != 0)) {
		st.barrierLocked(c)
		if err = st.consumeErrLocked(c, fh); err != nil {
			st.mu.Unlock()
			return 0, true, err
		}
		if len(data) < WriteBehindMinStageWrite {
			st.resetStreamLocked()
		}
		st.mu.Unlock()
		return 0, false, nil
	}

	st.nodeID = nodeID
	st.cacheEpoch = cacheEpoch
	activeGen := st.streamGen

	rem := data
	curOff := offset
	for len(rem) > 0 {
		if st.streamGen != activeGen || st.hasUnseenErrLocked(c, fh) {
			st.barrierLocked(c)
			err = st.consumeErrLocked(c, fh)
			if err == nil {
				err = st.lastErr
			}
			st.resetStreamLocked()
			st.mu.Unlock()
			return 0, true, err
		}
		if st.stageBuf == nil {
			st.stageBuf = acquireWriteBehindBuf()
			c.pendingCount.Add(1)
			st.stageOffset = curOff
			st.stageLen = 0
			st.stageGen = activeGen
		}
		space := WriteBehindChunkSize - st.stageLen
		n := min(len(rem), space)
		copy(st.stageBuf[st.stageLen:st.stageLen+n], rem[:n])
		st.stageLen += n
		rem = rem[n:]
		curOff += uint64(n)
		st.expectedOffset = curOff

		if st.stageLen == WriteBehindChunkSize {
			if !st.dispatchLocked(c) {
				st.waitInflightLocked()
				err = st.consumeErrLocked(c, fh)
				if err == nil {
					err = st.lastErr
				}
				st.resetStreamLocked()
				st.mu.Unlock()
				return 0, true, err
			}
		}
	}

	st.mu.Unlock()
	return uint32(len(data)), true, nil
}

// ObserveSyncWrite 在同步直通写入成功后推进该 Inode 的顺序流探针进度。
func (c *WriteBehindCoordinator) ObserveSyncWrite(ino, offset uint64, written uint32) {
	if written < WriteBehindMinStageWrite {
		st := c.lockExistingInode(ino)
		if st == nil {
			return
		}
		st.mu.Lock()
		if st.stageLen == 0 && st.inflight == 0 {
			st.resetStreamLocked()
		}
		st.mu.Unlock()
		st.opMu.Unlock()
		return
	}
	st := c.acquireInode(ino)
	st.mu.Lock()
	if st.stageLen == 0 && st.inflight == 0 {
		if st.hasStream && offset == st.expectedOffset {
			st.probedBytes += uint64(written)
			st.expectedOffset = offset + uint64(written)
		} else {
			st.streamGen++
			st.lastCommitted = nil
			st.hasStream = true
			st.probedBytes = uint64(written)
			st.expectedOffset = offset + uint64(written)
		}
	}
	st.mu.Unlock()
	st.opMu.Unlock()
}

// ResetInodeStream 原子排空目标 Inode 的挂起写入并重置顺序流探测进度与属性提交世代（不消费句柄级写错误游标）。
// 供 Open(O_TRUNC)、SetAttr、Fallocate 与 Ioctl 调用。
func (c *WriteBehindCoordinator) ResetInodeStream(ino uint64) {
	st := c.lockExistingInode(ino)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.barrierLocked(c)
	st.resetStreamLocked()
	st.mu.Unlock()
	st.opMu.Unlock()
}

// FlushInode 刷出目标 Inode 的尾部分片缓冲，等待全部在途异步 Write RPC 完成，并按默认游标消费粘性错误。
func (c *WriteBehindCoordinator) FlushInode(ino uint64) error {
	return c.FlushInodeForHandle(0, ino)
}

// FlushInodeForHandle 刷出目标 Inode 的尾部分片缓冲，等待全部在途异步 Write RPC 完成，并按句柄 fh 的 errseq 游标消费未观测错误。
// 供可写句柄 Flush 与 Fsync 调用。
func (c *WriteBehindCoordinator) FlushInodeForHandle(fh, ino uint64) error {
	st := c.lockExistingInode(ino)
	if st == nil {
		return nil
	}
	st.mu.Lock()
	st.barrierLocked(c)
	err := st.consumeErrLocked(c, fh)
	st.mu.Unlock()
	st.opMu.Unlock()
	return err
}

// FlushAndResetInode 在同一原子临界区内刷出尾部分片、等待全部在途 RPC 收口、按默认游标消费错误并重置顺序流状态。
func (c *WriteBehindCoordinator) FlushAndResetInode(ino uint64) error {
	return c.FlushAndResetInodeForHandle(0, ino)
}

// FlushAndResetInodeForHandle 在同一原子临界区内刷出尾部分片、等待全部在途 RPC 收口、按句柄 fh 的 errseq 游标消费未观测错误并重置顺序流状态。
// 供同步旁路 Write（如 O_APPEND、O_SYNC、特殊标志校验）调用。
func (c *WriteBehindCoordinator) FlushAndResetInodeForHandle(fh, ino uint64) error {
	st := c.lockExistingInode(ino)
	if st == nil {
		return nil
	}
	st.mu.Lock()
	st.barrierLocked(c)
	err := st.consumeErrLocked(c, fh)
	st.resetStreamLocked()
	st.mu.Unlock()
	st.opMu.Unlock()
	return err
}

// BarrierInode 刷出目标 Inode 的尾部分片缓冲并等待全部在途异步 Write RPC 完成，
// 但保留未消费的 stickyErr 留待后续可写句柄 Write / Flush / Fsync 报告。
// 供 Read、GetAttr、Lookup、只读句柄 Flush、Link、Unlink、Rename、Release 等只读观测或拓扑屏障调用。
func (c *WriteBehindCoordinator) BarrierInode(ino uint64) {
	if c.pendingCount.Load() == 0 {
		return
	}
	st := c.lockExistingInode(ino)
	if st == nil {
		return
	}
	st.mu.Lock()
	st.barrierLocked(c)
	st.mu.Unlock()
	st.opMu.Unlock()
}

// BarrierAll 刷出所有活跃 Inode 的未落盘聚合缓冲并等待在途 RPC 全部收口。
func (c *WriteBehindCoordinator) BarrierAll() {
	if c.pendingCount.Load() == 0 {
		return
	}
	c.mu.RLock()
	targets := make([]*inodeWriteBehind, 0, len(c.inodes))
	for _, st := range c.inodes {
		targets = append(targets, st)
	}
	c.mu.RUnlock()

	for _, st := range targets {
		st.opMu.Lock()
		st.mu.Lock()
		st.barrierLocked(c)
		st.mu.Unlock()
		st.opMu.Unlock()
	}
}

// RemoveInode 在文件的最后一个本地句柄关闭后排空在途写入并回收其内存状态。
func (c *WriteBehindCoordinator) RemoveInode(ino uint64) {
	st := c.lockExistingInode(ino)
	if st == nil {
		return
	}
	defer st.opMu.Unlock()

	st.mu.Lock()
	st.barrierLocked(c)
	canDelete := st.stageBuf == nil && st.stageLen == 0 && st.inflight == 0
	if canDelete {
		st.removed = true
	}
	st.mu.Unlock()

	if canDelete {
		c.mu.Lock()
		if c.inodes[ino] == st {
			delete(c.inodes, ino)
		}
		c.mu.Unlock()
	}
}

// Clear 排空所有在途写入并清空全部状态（供卸载时调用）。
func (c *WriteBehindCoordinator) Clear() {
	c.mu.Lock()
	targets := make([]*inodeWriteBehind, 0, len(c.inodes))
	for _, st := range c.inodes {
		targets = append(targets, st)
	}
	clear(c.inodes)
	c.mu.Unlock()

	for _, st := range targets {
		st.opMu.Lock()
		st.mu.Lock()
		st.barrierLocked(c)
		st.discardStageLocked(&c.pendingCount)
		st.removed = true
		st.mu.Unlock()
		st.opMu.Unlock()
	}

	c.seqMu.Lock()
	clear(c.handleSeq)
	c.seqMu.Unlock()
}
