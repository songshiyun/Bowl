package store

import (
	"errors"
	"io"
	"math"
	"os"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
)

var (
	// partialWriteHook 供 STARSRC-622 挂载 < 1 MiB 或非对齐局部写流水线。
	partialWriteHook func(s *StoreActor, ino, offset uint64, data []byte) (uint32, Attribute, error)

	// testBeforeBlobReadHook 供单测在 Read 拿到 DataKey 后、打开 .blob 前注入并发覆写删块竞态。
	testBeforeBlobReadHook atomic.Pointer[func(ino, blockIdx, blobID uint64, attempt int)]
)

func init() {
	registerOp(opWrite, (*StoreActor).validateWriteCmd, (*StoreActor).mutateWriteCmd)
}

// SetTestBeforeBlobReadHook 设置或清除 Read 在读取物理块前的测试钩子。
func (s *StoreActor) SetTestBeforeBlobReadHook(fn func(ino, blockIdx, blobID uint64, attempt int)) {
	if fn == nil {
		testBeforeBlobReadHook.Store(nil)
		return
	}
	testBeforeBlobReadHook.Store(&fn)
}

type writeBlockSpec struct {
	blockIdx uint64
	newRef   BlobRef
	keep     bool
	sameRef  bool
	patch    *PatchLog

	// Phase A 校验时从 s.batch 读取的实时旧块状态
	oldRef BlobRef
	oldOK  bool
}

type writeDataOp struct {
	ino             uint64
	offset          uint64
	dataLen         uint32
	blocks          []writeBlockSpec
	preAllocatedIDs []uint64

	// Phase A 校验缓存至 Phase B
	target     Inode
	newSize    uint64
	bytesDelta uint64
	attr       Attribute
}

func isFullBlockAlignedWrite(offset uint64, data []byte) bool {
	if len(data) == 0 {
		return false
	}
	return offset%BlobSize == 0 && len(data)%BlobSize == 0
}

// Write 执行跨层 4 阶段数据写入流水线（ADR-0030 §3.4）：
//   - 满 1 MiB 整块写：阶段 1 在锁外无锁分配 newBlobID 并并行写入 <newBlobID>.blob，随后获取 s.inodeLocks[ino&0xfff] 投递进 Actor 双阶段队列；
//   - 局部写（< 1 MiB 或非块边界对齐）：若已挂载 partialWriteHook 则交由局部写流水线处理，否则持 s.inodeLocks[ino&0xfff] 执行块级写入。
func (s *StoreActor) Write(ino, offset uint64, data []byte) (uint32, Attribute, error) {
	if s.readOnly {
		return 0, Attribute{}, ErrReadOnly
	}
	if err := validateNodeIno(ino); err != nil {
		return 0, Attribute{}, err
	}
	if offset > MaxFileSize || uint64(len(data)) > MaxFileSize-offset {
		return 0, Attribute{}, ErrFileTooLarge
	}
	if len(data) > math.MaxUint32 {
		return 0, Attribute{}, ErrInvalid
	}

	if len(data) == 0 {
		return s.writeZeroBytes(ino, offset)
	}

	if isFullBlockAlignedWrite(offset, data) {
		return s.writeFullBlocks(ino, offset, data)
	}

	if partialWriteHook != nil {
		return partialWriteHook(s, ino, offset, data)
	}
	return s.writePartialFallback(ino, offset, data)
}

func (s *StoreActor) writeZeroBytes(ino, offset uint64) (uint32, Attribute, error) {
	op := &writeDataOp{
		ino:     ino,
		offset:  offset,
		dataLen: 0,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.req = op
	if err := s.submit(cmd); err != nil {
		return 0, Attribute{}, err
	}
	return 0, op.attr, nil
}

func (s *StoreActor) writeFullBlocks(ino, offset uint64, data []byte) (uint32, Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return 0, Attribute{}, err
	}
	defer unlock()

	blobs := s.Blobs()
	if blobs == nil {
		return 0, Attribute{}, ErrIO
	}

	numBlocks := len(data) / BlobSize
	specs := make([]writeBlockSpec, 0, numBlocks)
	preAllocated := make([]uint64, 0, numBlocks)

	// 阶段 1：在不持有 inodeLocks 的状态下向活跃 .pack 追加写入满块数据。
	for i := range numBlocks {
		blockIdx := (offset / BlobSize) + uint64(i)
		chunk := data[i*BlobSize : (i+1)*BlobSize]
		ref, writeErr := blobs.Append(ino, chunk)
		if writeErr != nil {
			return 0, Attribute{}, writeErr
		}
		preAllocated = append(preAllocated, ref.PackID)
		specs = append(specs, writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   ref,
			keep:     true,
		})
	}

	// 预写成功后获取 per-Inode 定序锁，确保持有至阶段 3 db.Apply 完成（<-cmd.done 唤醒）后释放。
	inoLock := &s.inodeLocks[ino&0xfff]
	inoLock.Lock()
	defer inoLock.Unlock()

	op := &writeDataOp{
		ino:             ino,
		offset:          offset,
		dataLen:         uint32(len(data)),
		blocks:          specs,
		preAllocatedIDs: preAllocated,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.req = op
	if err := s.enqueueAndWait(cmd, preAllocated); err != nil {
		return 0, Attribute{}, err
	}
	return op.dataLen, op.attr, nil
}

func (s *StoreActor) writePartialFallback(ino, offset uint64, data []byte) (uint32, Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return 0, Attribute{}, err
	}
	defer unlock()

	blobs := s.Blobs()
	if blobs == nil {
		return 0, Attribute{}, ErrIO
	}

	inoLock := &s.inodeLocks[ino&0xfff]
	inoLock.Lock()
	defer inoLock.Unlock()

	var specs []writeBlockSpec
	var preAllocated []uint64

	for pos, consumed := offset, 0; consumed < len(data); {
		blockIdx := pos / BlobSize
		inBlock := int(pos % BlobSize)
		length := min(len(data)-consumed, BlobSize-inBlock)
		chunk := data[consumed : consumed+length]

		oldRef, oldOK, err := s.readDataBlobRef(s.db, ino, blockIdx)
		if err != nil {
			return 0, Attribute{}, err
		}

		newLen := inBlock + length
		if oldOK && int(oldRef.Length) > newLen {
			newLen = int(oldRef.Length)
		}

		merged := make([]byte, newLen)
		if oldOK {
			if err := blobs.ReadAt(oldRef, 0, merged[:oldRef.Length]); err != nil {
				return 0, Attribute{}, err
			}
		}
		copy(merged[inBlock:], chunk)

		ref, err := blobs.Append(ino, merged)
		if err != nil {
			return 0, Attribute{}, err
		}
		preAllocated = append(preAllocated, ref.PackID)
		specs = append(specs, writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   ref,
			keep:     true,
		})

		pos += uint64(length)
		consumed += length
	}

	op := &writeDataOp{
		ino:             ino,
		offset:          offset,
		dataLen:         uint32(len(data)),
		blocks:          specs,
		preAllocatedIDs: preAllocated,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.req = op
	if err := s.enqueueAndWait(cmd, preAllocated); err != nil {
		return 0, Attribute{}, err
	}
	return op.dataLen, op.attr, nil
}

func (s *StoreActor) readDataBlobRef(reader pebbleReader, ino, blockIdx uint64) (BlobRef, bool, error) {
	var keyBuf [DataKeyLen]byte
	putDataKey(keyBuf[:], ino, blockIdx)
	val, closer, err := reader.Get(keyBuf[:])
	if errors.Is(err, pebble.ErrNotFound) {
		return BlobRef{}, false, nil
	}
	if err != nil {
		return BlobRef{}, false, err
	}
	ref, decErr := DecodeBlobRef(val)
	if closeErr := closer.Close(); decErr != nil || closeErr != nil {
		return BlobRef{}, false, errors.Join(decErr, closeErr)
	}
	return ref, true, nil
}

func (s *StoreActor) validateWritableFileInode(reader pebbleReader, ino uint64, offset uint64, dataLen uint32) (Inode, error) {
	inode, err := s.readInode(reader, ino)
	if err != nil {
		return Inode{}, err
	}
	if err := s.checkInodeLive(reader, inode); err != nil {
		return Inode{}, err
	}
	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return Inode{}, ErrIsDir
	case S_IFREG:
	default:
		return Inode{}, ErrInvalid
	}
	if inode.Flags&FlagImmutable != 0 {
		return Inode{}, ErrPerm
	}
	if dataLen > 0 && inode.Flags&FlagAppend != 0 && offset != inode.Size {
		return Inode{}, ErrPerm
	}
	return inode, nil
}

// validateWriteCmd 执行阶段 2 Phase A 纯内存只读预校验（绝不触碰 s.batch 写接口）。
// 若校验失败，将阶段 1 预写的 newBlobID 挂入 cmd.toDelete 并跳过，0 污染 s.batch。
func (s *StoreActor) validateWriteCmd(cmd *writeCmd) (err error) {
	op := cmd.req.(*writeDataOp)
	defer func() {
		if err != nil && len(op.preAllocatedIDs) > 0 {
			cmd.toDelete = append(cmd.toDelete, op.preAllocatedIDs...)
		}
	}()

	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.validateWritableFileInode(s.batch, op.ino, op.offset, op.dataLen)
	if err != nil {
		return err
	}
	if op.dataLen == 0 {
		op.target = inode
		op.newSize = inode.Size
		op.attr = inode.ToAttribute()
		return nil
	}

	endOffset := op.offset + uint64(op.dataLen)
	newSize := max(inode.Size, endOffset)
	var bytesDelta uint64
	if newSize > inode.Size {
		bytesDelta = newSize - inode.Size
		if bytesDelta > math.MaxUint64-s.stat.LogicalBytes {
			return ErrNoSpace
		}
		// 考虑覆盖已有块时的净增量配额校验
		if err := s.checkQuota(bytesDelta, 0); err != nil {
			return err
		}
	}

	for i := range op.blocks {
		oldRef, oldOK, err := s.readDataBlobRef(s.batch, op.ino, op.blocks[i].blockIdx)
		if err != nil {
			return err
		}
		op.blocks[i].oldRef = oldRef
		op.blocks[i].oldOK = oldOK
	}

	op.target = inode
	op.newSize = newSize
	op.bytesDelta = bytesDelta
	return nil
}

// mutateWriteCmd 执行阶段 2 Phase B 内存记账与零拷贝批次编码。
func (s *StoreActor) mutateWriteCmd(cmd *writeCmd) {
	op := cmd.req.(*writeDataOp)
	if op.dataLen == 0 {
		s.overlayAtime(&op.target)
		op.attr = op.target.ToAttribute()
		return
	}

	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	inode := op.target
	inode.Size = op.newSize
	inode.Mode &^= S_ISUID | S_ISGID
	inode.Mtime = nowNano
	inode.Ctime = nowNano
	inode.WriteSeq = seq

	s.stat.LogicalBytes += op.bytesDelta

	_ = batchSetInodeDeferred(s.batch, inode)
	batchDeleteXattrDeferred(s.batch, op.ino, securityCapabilityXattr)
	batchSetDirtyDeferred(s.batch, op.ino)

	for i := range op.blocks {
		b := &op.blocks[i]
		if b.patch != nil {
			s.mergedPatches = append(s.mergedPatches, *b.patch)
		}
		if b.keep {
			if b.oldOK && b.oldRef != b.newRef {
				cmd.toDelete = append(cmd.toDelete, b.oldRef.PackID)
			}
			if !b.sameRef {
				batchSetDataDeferred(s.batch, op.ino, b.blockIdx, b.newRef)
			}
		} else {
			if b.oldOK {
				cmd.toDelete = append(cmd.toDelete, b.oldRef.PackID)
				batchDeleteDataDeferred(s.batch, op.ino, b.blockIdx)
			}
		}
	}

	s.overlayAtime(&inode)
	op.attr = inode.ToAttribute()
}

// Read 在队列外执行 100% 无锁并发直读（ADR-0030 §3.3）：
//   - 单块读直接调 1 次 s.db.Get(DataKey)，跨块读调 s.db.NewIter(nil) + SeekPrefixGE(DataKey(ino, firstBlock))；
//   - 稀疏空洞直接填零；
//   - 优先从 hotShard 拷贝，未命中调 os.Open + ReadAt；
//   - 若并发覆写恰好在 Stage 4 删除旧块导致 os.Open 返回 os.ErrNotExist (ENOENT)，自动重读最新 DataKey 重试（最多 2 次），全程 0 Inode 锁开销。
func (s *StoreActor) Read(ino, offset uint64, size uint32) ([]byte, error) {
	if err := validateNodeIno(ino); err != nil {
		return nil, err
	}
	if offset > MaxFileSize {
		return nil, ErrInvalid
	}

	unlock, err := s.enter()
	if err != nil {
		return nil, err
	}
	defer unlock()

	inode, err := s.readInode(s.db, ino)
	if err != nil {
		return nil, err
	}
	if err := s.checkInodeLive(s.db, inode); err != nil {
		return nil, err
	}
	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return nil, ErrIsDir
	case S_IFREG:
	default:
		return nil, ErrInvalid
	}

	if size == 0 || offset >= inode.Size {
		return []byte{}, nil
	}

	readLen := min(uint64(size), inode.Size-offset)
	out := make([]byte, readLen)
	end := offset + readLen

	firstBlock := offset / BlobSize
	lastBlock := (end - 1) / BlobSize

	if firstBlock == lastBlock {
		if err := s.readSingleBlock(ino, firstBlock, offset, end, offset, out); err != nil {
			return nil, err
		}
	} else {
		if err := s.readMultiBlocks(ino, firstBlock, lastBlock, offset, end, out); err != nil {
			return nil, err
		}
	}

	s.touchReadAtime(inode)
	return out, nil
}

func (s *StoreActor) readSingleBlock(ino, blockIdx, rangeStart, rangeEnd, baseOffset uint64, out []byte) error {
	ref, ok, err := s.readDataBlobRef(s.db, ino, blockIdx)
	if err != nil || !ok {
		return err
	}
	return s.readBlockSliceWithRetry(ino, blockIdx, ref, rangeStart, rangeEnd, baseOffset, out)
}

func (s *StoreActor) readMultiBlocks(ino, firstBlock, lastBlock, offset, end uint64, out []byte) (err error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	var startKey [DataKeyLen]byte
	putDataKey(startKey[:], ino, firstBlock)

	for valid := iter.SeekPrefixGE(startKey[:]); valid; valid = iter.Next() {
		keyIno, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		if keyIno != ino || blockIdx > lastBlock {
			break
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return decErr
		}

		blockStart := blockIdx * BlobSize
		blockEnd := blockStart + BlobSize
		rangeStart := max(offset, blockStart)
		rangeEnd := min(end, blockEnd)
		if rangeStart >= rangeEnd {
			continue
		}
		if err := s.readBlockSliceWithRetry(ino, blockIdx, ref, rangeStart, rangeEnd, offset, out); err != nil {
			return err
		}
	}
	return iter.Error()
}

// readBlockSliceWithRetry 从 ref 指向的物理块读取 [rangeStart, rangeEnd) 填入 out；
// 若并发覆写在 Stage 4 删除了旧 .blob 文件触发 os.ErrNotExist (ENOENT)，自动重读最新 DataKey 重试（最多重试 2 次）。
func (s *StoreActor) readBlockSliceWithRetry(ino, blockIdx uint64, initialRef BlobRef, rangeStart, rangeEnd, baseOffset uint64, out []byte) error {
	blobs := s.Blobs()
	if blobs == nil {
		return ErrIO
	}
	inBlockStart := rangeStart % BlobSize
	reqLen := rangeEnd - rangeStart
	dstStart := rangeStart - baseOffset
	dst := out[dstStart : dstStart+reqLen]

	curRef := initialRef
	const maxRetries = 2
	for attempt := 0; attempt <= maxRetries; attempt++ {
		clear(dst)
		if inBlockStart >= uint64(curRef.Length) {
			return nil
		}
		n := min(reqLen, uint64(curRef.Length)-inBlockStart)

		if hookPtr := testBeforeBlobReadHook.Load(); hookPtr != nil && *hookPtr != nil {
			(*hookPtr)(ino, blockIdx, curRef.PackID, attempt)
		}

		err := blobs.ReadAt(curRef, int64(inBlockStart), dst[:n])
		if err == nil {
			return nil
		}
		if (!errors.Is(err, os.ErrNotExist) && !errors.Is(err, io.ErrUnexpectedEOF)) || attempt == maxRetries {
			if errors.Is(err, io.ErrUnexpectedEOF) {
				return nil
			}
			return err
		}
		// 捕获并发覆写删块 (ENOENT) 或窗口内块原地截断 (ErrUnexpectedEOF)：重读最新 DataKey(ino, blockIdx)
		latestRef, ok, getErr := s.readDataBlobRef(s.db, ino, blockIdx)
		if getErr != nil {
			return getErr
		}
		if !ok {
			// 最新状态已变为空洞（例如被并发截断或打洞），填零返回
			clear(dst)
			return nil
		}
		curRef = latestRef
	}
	return nil
}
