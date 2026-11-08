package store

import (
	"errors"
	"math"
)

type writeDataOp struct {
	ino     uint64
	offset  uint64
	dataLen uint32
	blocks  []writeBlockSpec

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

func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// Write 执行跨 256 KiB 逻辑块的追加式数据写入流水线（ADR-0031 §3.2）：
//   - 满 256 KiB 对齐整块写：阶段 1 在不持 inodeLocks 的状态下向活跃 .pack 追加写入满块数据，随后获取 s.inodeLocks[ino&0xfff] 投递进 Actor 双阶段队列；
//   - 局部或非对齐跨块写：全程持 s.inodeLocks[ino&0xfff]，若命中活跃 .pack 最末尾同块则走 ExtendTail 原地顺延写指针，否则读取旧块合并后通过 Append 追加至活跃 .pack；
//   - 在 Actor Phase B 的同一个 IndexedBatch 中原子更新 InodeKey 与涉及的全部 DataKey -> 16B BlobRef{PackID, Offset, Length}。
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

	return s.writePartialData(ino, offset, data)
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

	blobs := s.blobs
	if blobs == nil {
		return 0, Attribute{}, ErrIO
	}

	numBlocks := len(data) / BlobSize
	specs := make([]writeBlockSpec, 0, numBlocks)
	var staged []BlobRef

	// 阶段 1：在不持有 inodeLocks 的状态下向活跃 .pack 追加写入满块数据。
	for i := range numBlocks {
		blockIdx := (offset / BlobSize) + uint64(i)
		chunk := data[i*BlobSize : (i+1)*BlobSize]
		ref, writeErr := s.stageAppend(blobs, ino, chunk, &staged)
		if writeErr != nil {
			s.unpinStagedBlobs(staged)
			return 0, Attribute{}, writeErr
		}
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
		ino:     ino,
		offset:  offset,
		dataLen: uint32(len(data)),
		blocks:  specs,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.req = op
	cmd.stagedBlobs = staged
	if err := s.enqueueAndWait(cmd); err != nil {
		return 0, Attribute{}, err
	}
	return op.dataLen, op.attr, nil
}

func (s *StoreActor) writePartialData(ino, offset uint64, data []byte) (uint32, Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return 0, Attribute{}, err
	}
	defer unlock()

	blobs := s.blobs
	if blobs == nil {
		return 0, Attribute{}, ErrIO
	}

	inoLock := &s.inodeLocks[ino&0xfff]
	inoLock.Lock()
	defer inoLock.Unlock()

	var (
		specs  []writeBlockSpec
		staged []BlobRef
	)

	for pos, consumed := offset, 0; consumed < len(data); {
		blockIdx := pos / BlobSize
		inBlock := int(pos % BlobSize)
		length := min(len(data)-consumed, BlobSize-inBlock)
		chunk := data[consumed : consumed+length]

		spec, err := s.preparePartialWriteBlock(blobs, ino, blockIdx, inBlock, chunk, &staged)
		if err != nil {
			s.unpinStagedBlobs(staged)
			return 0, Attribute{}, err
		}
		specs = append(specs, spec)

		pos += uint64(length)
		consumed += length
	}

	op := &writeDataOp{
		ino:     ino,
		offset:  offset,
		dataLen: uint32(len(data)),
		blocks:  specs,
	}
	cmd := s.acquireCmd()
	cmd.op = opWrite
	cmd.req = op
	cmd.stagedBlobs = staged
	if err := s.enqueueAndWait(cmd); err != nil {
		return 0, Attribute{}, err
	}
	return op.dataLen, op.attr, nil
}

func (s *StoreActor) preparePartialWriteBlock(blobs *BlobStore, ino, blockIdx uint64, inBlock int, chunk []byte, staged *[]BlobRef) (writeBlockSpec, error) {
	// 跨块局部写中可能包含中间满块对齐整块：直接追加新块，无需读旧块。
	if inBlock == 0 && len(chunk) == BlobSize {
		if isAllZero(chunk) {
			return writeBlockSpec{
				blockIdx: blockIdx,
				keep:     false,
			}, nil
		}
		ref, err := s.stageAppend(blobs, ino, chunk, staged)
		if err != nil {
			return writeBlockSpec{}, err
		}
		return writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   ref,
			keep:     true,
		}, nil
	}

	oldRef, oldOK, err := s.readDataBlobRef(s.db, ino, blockIdx)
	if err != nil {
		return writeBlockSpec{}, err
	}

	newLen := inBlock + len(chunk)
	if oldOK && int(oldRef.Length) > newLen {
		newLen = int(oldRef.Length)
	}

	// 尝试活跃 .pack 尾部顺延扩展
	if oldOK && inBlock >= int(oldRef.Length) {
		if extRef, ok, extErr := s.stageExtendTail(blobs, ino, oldRef, uint32(inBlock), chunk, uint32(newLen), staged); extErr != nil {
			return writeBlockSpec{}, extErr
		} else if ok {
			return writeBlockSpec{
				blockIdx: blockIdx,
				newRef:   extRef,
				keep:     true,
				sameRef:  extRef == oldRef,
			}, nil
		}
	}

	if !oldOK && isAllZero(chunk) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, nil
	}

	merged := make([]byte, newLen)
	if oldOK {
		err = blobs.ReadAt(oldRef, 0, merged[:oldRef.Length])
		if err != nil {
			return writeBlockSpec{}, err
		}
	}
	copy(merged[inBlock:], chunk)
	if isAllZero(merged) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, nil
	}

	ref, err := s.stageAppend(blobs, ino, merged, staged)
	if err != nil {
		return writeBlockSpec{}, err
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   ref,
		keep:     true,
	}, nil
}

func (s *StoreActor) validateWritableFileInode(reader pebbleReader, ino, offset uint64, dataLen uint32) (Inode, error) {
	inode, err := s.readInode(reader, ino)
	if err != nil {
		return Inode{}, err
	}
	if err := s.checkInodeLive(reader, &inode); err != nil {
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
func (s *StoreActor) validateWriteCmd(cmd *writeCmd) error {
	op, _ := cmd.req.(*writeDataOp)

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
		if err := s.checkQuota(bytesDelta, 0); err != nil {
			return err
		}
	}

	if err := s.resolveWriteBlockSpecs(s.batch, op.ino, op.blocks); err != nil {
		return err
	}

	op.target = inode
	op.newSize = newSize
	op.bytesDelta = bytesDelta
	return nil
}

// mutateWriteCmd 执行阶段 2 Phase B 内存记账与零拷贝批次编码。
func (s *StoreActor) mutateWriteCmd(cmd *writeCmd) {
	op, _ := cmd.req.(*writeDataOp)
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

	_ = BatchSetInodeDeferred(s.batch, inode)
	BatchDeleteXattrDeferred(s.batch, op.ino, securityCapabilityXattr)
	BatchSetDirtyDeferred(s.batch, op.ino)

	s.applyWriteBlockSpecs(op.ino, op.blocks)

	s.overlayAtime(&inode)
	op.attr = inode.ToAttribute()
}

// Read 在队列外执行 100% 无锁并发直读（ADR-0031 §3.2）：
//   - 单块读直接调 1 次 s.db.Get(DataKey)，跨块读调 s.db.NewIter(nil) + SeekPrefixGE(DataKey(ino, firstBlock))；
//   - 稀疏空洞直接填零；
//   - 通过 16B BlobRef{PackID, Offset, Length} 优先从 hotShard 拷贝，未命中直接对常驻 .pack 句柄执行 ReadAt（0 open/close、0 撕裂读、无 ENOENT 重试）。
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
	if err := s.checkInodeLive(s.db, &inode); err != nil {
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

	s.touchReadAtime(&inode)
	return out, nil
}

func (s *StoreActor) readSingleBlock(ino, blockIdx, rangeStart, rangeEnd, baseOffset uint64, out []byte) error {
	ref, ok, err := s.readDataBlobRef(s.db, ino, blockIdx)
	if err != nil || !ok {
		return err
	}
	return s.readBlockSlice(ref, rangeStart, rangeEnd, baseOffset, out)
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
		if err := s.readBlockSlice(ref, rangeStart, rangeEnd, offset, out); err != nil {
			return err
		}
	}
	return iter.Error()
}

// readBlockSlice 从 ref 指向的物理封包区间读取 [rangeStart, rangeEnd) 填入 out。
func (s *StoreActor) readBlockSlice(ref BlobRef, rangeStart, rangeEnd, baseOffset uint64, out []byte) error {
	blobs := s.blobs
	if blobs == nil {
		return ErrIO
	}
	inBlockStart := rangeStart % BlobSize
	reqLen := rangeEnd - rangeStart
	dstStart := rangeStart - baseOffset
	dst := out[dstStart : dstStart+reqLen]
	clear(dst)
	if inBlockStart >= uint64(ref.Length) {
		return nil
	}
	n := min(reqLen, uint64(ref.Length)-inBlockStart)
	return blobs.ReadAt(ref, int64(inBlockStart), dst[:n])
}
