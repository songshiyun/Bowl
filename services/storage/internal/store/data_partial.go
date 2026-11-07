package store

import (
	"errors"
	"math"
)

func init() {
	partialWriteHook = (*StoreActor).writePartialData
	setAttrSizeHook = (*StoreActor).setAttrWithSize
	registerOp(opFallocate, (*StoreActor).validateFallocateCmd, (*StoreActor).mutateFallocateCmd)
}

func isAllZero(data []byte) bool {
	for _, b := range data {
		if b != 0 {
			return false
		}
	}
	return true
}

// writePartialData 执行 < 1 MiB 或非对齐跨块局部写的跨层 4 阶段双分支流水线（ADR-0030 §3.4）：
//   - 全程持 s.inodeLocks[ino & 0xfff] 跨越阶段 1 读旧块至阶段 3 db.Apply 完成（<-cmd.done 唤醒后释放）；
//   - 分支 1（旧块不存在或 !blobs.WindowLocal(oldID)）：阶段 1 读取旧块（优先命中 hotShard）合并新数据写出 <newBlobID>.blob，阶段 2 切换 DataKey 并将 oldID 加入 cmd.toDelete，阶段 4 由调用方协程物理删除 oldID；
//   - 分支 2（blobs.WindowLocal(oldID) == true 窗口内活跃块）：阶段 1 零磁盘 I/O，构造 PatchLog（跨越块内空洞时自动从 oldRef.Length 补零）；Phase A 失败直接丢弃 PatchLog（0 磁盘回滚），Phase B 推入 mergedPatches（有效 Length 未变时免重写 DataKey），阶段 3 按 blobID 合并执行 WriteAt + Truncate 并同步更新 hotShard。
func (s *StoreActor) writePartialData(ino, offset uint64, data []byte) (uint32, Attribute, error) {
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

		spec, newID, err := s.preparePartialWriteBlock(blobs, ino, blockIdx, inBlock, chunk)
		if err != nil {
			blobs.DetachMeta(preAllocated)
			_ = blobs.RemoveFiles(preAllocated)
			return 0, Attribute{}, err
		}
		if newID != 0 {
			preAllocated = append(preAllocated, newID)
		}
		specs = append(specs, spec)

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

func (s *StoreActor) preparePartialWriteBlock(blobs *BlobStore, ino, blockIdx uint64, inBlock int, chunk []byte) (writeBlockSpec, uint64, error) {
	// 跨块局部写中可能包含中间满 1 MiB 的对齐整块：直接写出新块，无需读旧块。
	if inBlock == 0 && len(chunk) == BlobSize {
		if isAllZero(chunk) {
			return writeBlockSpec{
				blockIdx: blockIdx,
				keep:     false,
			}, 0, nil
		}
		newID, err := blobs.NextBlobID()
		if err != nil {
			return writeBlockSpec{}, 0, err
		}
		if err := blobs.WriteForInode(ino, newID, chunk); err != nil {
			return writeBlockSpec{}, 0, err
		}
		return writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   BlobRef{ID: newID, Length: BlobSize},
			keep:     true,
		}, newID, nil
	}

	oldRef, oldOK, err := s.readDataBlobRef(s.db, ino, blockIdx)
	if err != nil {
		return writeBlockSpec{}, 0, err
	}

	newLen := inBlock + len(chunk)
	if oldOK && int(oldRef.Length) > newLen {
		newLen = int(oldRef.Length)
	}

	// 分支 2：旧块存在且为当前 Sync 窗口内新建的活跃块（WindowLocal(oldRef.ID) == true）-> 阶段 1 零磁盘 I/O 构造 PatchLog
	if oldOK && blobs.WindowLocal(oldRef.ID) {
		var patchOffset uint32
		var patchData []byte
		if inBlock <= int(oldRef.Length) {
			patchOffset = uint32(inBlock)
			patchData = append([]byte(nil), chunk...)
		} else {
			// 跨越旧块尾部空洞写入：从 oldRef.Length 补零至 inBlock，再拼接 chunk
			holeLen := inBlock - int(oldRef.Length)
			patchOffset = oldRef.Length
			patchData = make([]byte, holeLen+len(chunk))
			copy(patchData[holeLen:], chunk)
		}
		patch := &PatchLog{
			BlobID: oldRef.ID,
			Ino:    ino,
			Offset: patchOffset,
			Data:   patchData,
			NewLen: uint32(newLen),
		}
		return writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   BlobRef{ID: oldRef.ID, Length: uint32(newLen)},
			keep:     true,
			sameRef:  uint32(newLen) == oldRef.Length,
			patch:    patch,
		}, 0, nil
	}

	// 分支 1：旧块不存在或已持久化（!WindowLocal）-> 读取旧块合并写出全新 <newBlobID>.blob
	if !oldOK && isAllZero(chunk) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, 0, nil
	}

	merged := make([]byte, newLen)
	if oldOK {
		if err := blobs.ReadAt(oldRef.ID, 0, merged[:oldRef.Length]); err != nil {
			return writeBlockSpec{}, 0, err
		}
	}
	copy(merged[inBlock:], chunk)
	if isAllZero(merged) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, 0, nil
	}

	newID, err := blobs.NextBlobID()
	if err != nil {
		return writeBlockSpec{}, 0, err
	}
	if err := blobs.WriteForInode(ino, newID, merged); err != nil {
		return writeBlockSpec{}, 0, err
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   BlobRef{ID: newID, Length: uint32(newLen)},
		keep:     true,
	}, newID, nil
}

// collectDataKeysAndBlobsFrom 扫描指定节点从 startBlockIdx 起（闭区间 [startBlockIdx, +∞)）的所有 DataKey 与对应 BlobRef.ID。
func (s *StoreActor) collectDataKeysAndBlobsFrom(reader pebbleReader, ino, startBlockIdx uint64) (keys [][]byte, blobIDs []uint64, err error) {
	iter, err := reader.NewIter(nil)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	var startKey [DataKeyLen]byte
	putDataKey(startKey[:], ino, startBlockIdx)

	for valid := iter.SeekPrefixGE(startKey[:]); valid; valid = iter.Next() {
		keyIno, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return nil, nil, decErr
		}
		if keyIno != ino {
			break
		}
		if blockIdx < startBlockIdx {
			continue
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return nil, nil, decErr
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
		if ref.ID != 0 {
			blobIDs = append(blobIDs, ref.ID)
		}
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, nil, iterErr
	}
	return keys, blobIDs, nil
}

// setAttrWithSize 执行包含 Size 截断/扩容的四阶段 SetAttr 流水线（ADR-0030 §3.4）。
func (s *StoreActor) setAttrWithSize(ino uint64, req SetAttrReq) (Attribute, error) {
	unlock, err := s.enter()
	if err != nil {
		return Attribute{}, err
	}
	defer unlock()

	blobs := s.Blobs()
	if blobs == nil {
		return Attribute{}, ErrIO
	}

	inoLock := &s.inodeLocks[ino&0xfff]
	inoLock.Lock()
	defer inoLock.Unlock()

	newSize := *req.Size
	var (
		hasTailSpec  bool
		tailSpec     writeBlockSpec
		preAllocated []uint64
	)

	// 若截断后尾部落在某个 1 MiB 块内部（0 < tailLen < BlobSize），在阶段 1 准备尾块截断（CoW 或 WindowLocal PatchLog）。
	if newSize > 0 {
		tailBlock := (newSize - 1) / BlobSize
		tailLen := uint32(newSize - tailBlock*BlobSize)
		if tailLen < BlobSize {
			oldRef, oldOK, err := s.readDataBlobRef(s.db, ino, tailBlock)
			if err != nil {
				return Attribute{}, err
			}
			if oldOK && oldRef.Length > tailLen {
				spec, newID, err := s.prepareTruncateTailBlock(blobs, ino, tailBlock, oldRef, tailLen)
				if err != nil {
					return Attribute{}, err
				}
				hasTailSpec = true
				tailSpec = spec
				if newID != 0 {
					preAllocated = append(preAllocated, newID)
				}
			}
		}
	}

	var (
		cachedInode Inode
		deleteKeys  [][]byte
		deleteBlobs []uint64
		outAttr     Attribute
	)

	cmd := s.acquireCmd()
	cmd.op = opCustom
	cmd.validate = func(actor *StoreActor, c *writeCmd) (vErr error) {
		defer func() {
			if vErr != nil && len(preAllocated) > 0 {
				c.toDelete = append(c.toDelete, preAllocated...)
			}
		}()
		if actor.stat.NextWriteSeq == math.MaxUint64 {
			return ErrNoSpace
		}
		inode, err := actor.readInode(actor.batch, ino)
		if err != nil {
			return err
		}
		if err := actor.checkInodeLive(actor.batch, inode); err != nil {
			return err
		}
		if err := actor.validateSetAttrInode(inode, req); err != nil {
			return err
		}

		if newSize < inode.Size {
			var startDelBlock uint64
			if newSize == 0 {
				startDelBlock = 0
			} else {
				startDelBlock = ((newSize - 1) / BlobSize) + 1
			}
			keys, blobIDs, err := actor.collectDataKeysAndBlobsFrom(actor.batch, ino, startDelBlock)
			if err != nil {
				return err
			}
			deleteKeys = keys
			deleteBlobs = blobIDs

			if hasTailSpec {
				oldRef, oldOK, err := actor.readDataBlobRef(actor.batch, ino, tailSpec.blockIdx)
				if err != nil {
					return err
				}
				tailSpec.oldRef = oldRef
				tailSpec.oldOK = oldOK
			}
		} else if len(preAllocated) > 0 {
			// 若实际为扩容，无需尾块截断，回收阶段 1 预分配的临时块
			c.toDelete = append(c.toDelete, preAllocated...)
			hasTailSpec = false
		}

		cachedInode = inode
		return nil
	}
	cmd.mutate = func(actor *StoreActor, c *writeCmd) {
		oldSize := cachedInode.Size
		seq := actor.stat.NextWriteSeq + 1
		actor.stat.NextWriteSeq = seq
		nowNano := actor.now().UnixNano()

		actor.applySetAttrFields(&cachedInode, req, nowNano, seq)

		if newSize < oldSize {
			for _, k := range deleteKeys {
				batchDeleteDeferred(actor.batch, k)
			}
			if len(deleteBlobs) > 0 {
				c.toDelete = append(c.toDelete, deleteBlobs...)
			}
			if hasTailSpec {
				if tailSpec.patch != nil {
					actor.mergedPatches = append(actor.mergedPatches, *tailSpec.patch)
				}
				if tailSpec.keep {
					if tailSpec.oldOK && tailSpec.oldRef.ID != tailSpec.newRef.ID {
						c.toDelete = append(c.toDelete, tailSpec.oldRef.ID)
					}
					if !tailSpec.sameRef {
						batchSetDataDeferred(actor.batch, ino, tailSpec.blockIdx, tailSpec.newRef)
					}
				} else if tailSpec.oldOK {
					c.toDelete = append(c.toDelete, tailSpec.oldRef.ID)
					batchDeleteDataDeferred(actor.batch, ino, tailSpec.blockIdx)
				}
			}
		}

		_ = batchSetInodeDeferred(actor.batch, cachedInode)
		batchSetDirtyDeferred(actor.batch, ino)
		outAttr = cachedInode.ToAttribute()
	}

	if err := s.enqueueAndWait(cmd, preAllocated); err != nil {
		return Attribute{}, err
	}
	return outAttr, nil
}

func (s *StoreActor) prepareTruncateTailBlock(blobs *BlobStore, ino, blockIdx uint64, oldRef BlobRef, tailLen uint32) (writeBlockSpec, uint64, error) {
	if blobs.WindowLocal(oldRef.ID) {
		patch := &PatchLog{
			BlobID: oldRef.ID,
			Ino:    ino,
			Offset: 0,
			Data:   nil,
			NewLen: tailLen,
		}
		return writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   BlobRef{ID: oldRef.ID, Length: tailLen},
			keep:     true,
			sameRef:  tailLen == oldRef.Length,
			patch:    patch,
		}, 0, nil
	}

	data := make([]byte, tailLen)
	if err := blobs.ReadAt(oldRef.ID, 0, data); err != nil {
		return writeBlockSpec{}, 0, err
	}
	if isAllZero(data) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, 0, nil
	}
	newID, err := blobs.NextBlobID()
	if err != nil {
		return writeBlockSpec{}, 0, err
	}
	if err := blobs.WriteForInode(ino, newID, data); err != nil {
		return writeBlockSpec{}, 0, err
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   BlobRef{ID: newID, Length: tailLen},
		keep:     true,
	}, newID, nil
}

type fallocateDataOp struct {
	ino             uint64
	offset          uint64
	length          uint64
	mode            uint32
	zeroRange       bool
	partialSpecs    []writeBlockSpec
	preAllocatedIDs []uint64

	// Phase A 校验缓存至 Phase B
	target         Inode
	newSize        uint64
	bytesDelta     uint64
	fullDeleteKeys [][]byte
	fullDeleteIDs  []uint64
}

// Fallocate 执行文件空间预留扩展或区间打洞/置零（ADR-0030 §3.4）：
//   - 支持 mode: 0（默认预留扩展）、FallocKeepSize、FallocPunchHole|FallocKeepSize、FallocZeroRange、FallocZeroRange|FallocKeepSize；
//   - 整块打洞直接在阶段 2 扫描对应 DataKey 范围调 DeleteDeferred 并将 blobID 加入 cmd.toDelete；
//   - 块内局部打洞按 !WindowLocal（CoW 新块，全零则删 DataKey）或 WindowLocal（PatchLog 阶段 3 原地清零并同步 hotShard）处理。
func (s *StoreActor) Fallocate(ino, offset, length uint64, mode uint32) error {
	if s.readOnly {
		return ErrReadOnly
	}
	if err := validateNodeIno(ino); err != nil {
		return err
	}
	if length == 0 || offset > MaxFileSize || length > MaxFileSize-offset {
		return ErrInvalid
	}
	if mode == FallocPunchHole {
		return ErrInvalid
	}
	switch mode {
	case 0, FallocKeepSize, FallocPunchHole | FallocKeepSize, FallocZeroRange, FallocZeroRange | FallocKeepSize:
	default:
		return ErrNotSupported
	}

	unlock, err := s.enter()
	if err != nil {
		return err
	}
	defer unlock()

	blobs := s.Blobs()
	if blobs == nil {
		return ErrIO
	}

	inoLock := &s.inodeLocks[ino&0xfff]
	inoLock.Lock()
	defer inoLock.Unlock()

	zeroRange := mode&FallocPunchHole != 0 || mode&FallocZeroRange != 0
	var (
		partialSpecs []writeBlockSpec
		preAllocated []uint64
	)

	if zeroRange {
		// 仅在首尾非完整对齐的边界块（最多 2 个块）需要在阶段 1 读取旧块或构造 PatchLog；
		// 中间所有完整覆盖的 1 MiB 块无需阶段 1 磁盘 I/O，直接在阶段 2 扫描 DataKey 调 DeleteDeferred。
		end := offset + length
		firstBlock := offset / BlobSize
		lastBlock := (end - 1) / BlobSize

		candidates := []uint64{firstBlock}
		if lastBlock != firstBlock {
			candidates = append(candidates, lastBlock)
		}
		for _, blockIdx := range candidates {
			blockStart := blockIdx * BlobSize
			blockEnd := blockStart + BlobSize
			rStart := max(offset, blockStart)
			rEnd := min(end, blockEnd)
			inBlock := int(rStart - blockStart)
			clearLen := int(rEnd - rStart)
			if inBlock == 0 && clearLen == BlobSize {
				// 完整 1 MiB 块交由阶段 2 范围扫描删除
				continue
			}
			spec, hasSpec, newID, err := s.preparePunchPartialBlock(blobs, ino, blockIdx, inBlock, clearLen)
			if err != nil {
				blobs.DetachMeta(preAllocated)
				_ = blobs.RemoveFiles(preAllocated)
				return err
			}
			if newID != 0 {
				preAllocated = append(preAllocated, newID)
			}
			if hasSpec {
				partialSpecs = append(partialSpecs, spec)
			}
		}
	}

	op := &fallocateDataOp{
		ino:             ino,
		offset:          offset,
		length:          length,
		mode:            mode,
		zeroRange:       zeroRange,
		partialSpecs:    partialSpecs,
		preAllocatedIDs: preAllocated,
	}
	cmd := s.acquireCmd()
	cmd.op = opFallocate
	cmd.req = op
	return s.enqueueAndWait(cmd, preAllocated)
}

func (s *StoreActor) preparePunchPartialBlock(blobs *BlobStore, ino, blockIdx uint64, inBlock, clearLen int) (writeBlockSpec, bool, uint64, error) {
	oldRef, oldOK, err := s.readDataBlobRef(s.db, ino, blockIdx)
	if err != nil || !oldOK {
		return writeBlockSpec{}, false, 0, err
	}
	if inBlock >= int(oldRef.Length) {
		return writeBlockSpec{}, false, 0, nil
	}
	clearEnd := min(inBlock+clearLen, int(oldRef.Length))

	data := make([]byte, oldRef.Length)
	if err := blobs.ReadAt(oldRef.ID, 0, data); err != nil {
		return writeBlockSpec{}, false, 0, err
	}
	if isAllZero(data[inBlock:clearEnd]) {
		return writeBlockSpec{}, false, 0, nil
	}
	clear(data[inBlock:clearEnd])

	// 若局部打洞清零后整块全部变为 0，直接删除该块 DataKey 并回收物理块（无论是否 WindowLocal）。
	if isAllZero(data) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, true, 0, nil
	}

	if blobs.WindowLocal(oldRef.ID) {
		patch := &PatchLog{
			BlobID: oldRef.ID,
			Ino:    ino,
			Offset: uint32(inBlock),
			Data:   make([]byte, clearEnd-inBlock),
			NewLen: oldRef.Length,
		}
		return writeBlockSpec{
			blockIdx: blockIdx,
			newRef:   oldRef,
			keep:     true,
			sameRef:  true,
			patch:    patch,
		}, true, 0, nil
	}

	newID, err := blobs.NextBlobID()
	if err != nil {
		return writeBlockSpec{}, false, 0, err
	}
	if err := blobs.WriteForInode(ino, newID, data); err != nil {
		return writeBlockSpec{}, false, 0, err
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   BlobRef{ID: newID, Length: oldRef.Length},
		keep:     true,
	}, true, newID, nil
}

// collectDataKeysAndBlobsInRange 扫描指定节点在闭区间 [startBlockIdx, endBlockIdx] 内的全部 DataKey 与 BlobRef.ID。
func (s *StoreActor) collectDataKeysAndBlobsInRange(reader pebbleReader, ino, startBlockIdx, endBlockIdx uint64) (keys [][]byte, blobIDs []uint64, err error) {
	if startBlockIdx > endBlockIdx {
		return nil, nil, nil
	}
	iter, err := reader.NewIter(nil)
	if err != nil {
		return nil, nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	var startKey [DataKeyLen]byte
	putDataKey(startKey[:], ino, startBlockIdx)

	for valid := iter.SeekPrefixGE(startKey[:]); valid; valid = iter.Next() {
		keyIno, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return nil, nil, decErr
		}
		if keyIno != ino || blockIdx > endBlockIdx {
			break
		}
		if blockIdx < startBlockIdx {
			continue
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return nil, nil, decErr
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
		if ref.ID != 0 {
			blobIDs = append(blobIDs, ref.ID)
		}
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, nil, iterErr
	}
	return keys, blobIDs, nil
}

func (s *StoreActor) validateFallocateCmd(cmd *writeCmd) (err error) {
	op := cmd.req.(*fallocateDataOp)
	defer func() {
		if err != nil && len(op.preAllocatedIDs) > 0 {
			cmd.toDelete = append(cmd.toDelete, op.preAllocatedIDs...)
		}
	}()

	if s.stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	inode, err := s.readInode(s.batch, op.ino)
	if err != nil {
		return err
	}
	if err := s.checkInodeLive(s.batch, inode); err != nil {
		return err
	}
	switch inode.Mode & S_IFMT {
	case S_IFDIR:
		return ErrIsDir
	case S_IFREG:
	default:
		return ErrNotSupported
	}
	if inode.Flags&(FlagImmutable|FlagAppend) != 0 {
		return ErrPerm
	}

	end := op.offset + op.length
	newSize := inode.Size
	if op.mode&FallocKeepSize == 0 && end > newSize {
		newSize = end
	}

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

	if op.zeroRange {
		// 计算完整 1 MiB 块的闭区间 [fullStart, fullEnd]
		fullStart := (op.offset + BlobSize - 1) / BlobSize
		var fullEnd uint64
		hasFullRange := false
		if end >= BlobSize {
			fullEnd = (end / BlobSize) - 1
			if fullStart <= fullEnd {
				hasFullRange = true
			}
		}
		if hasFullRange {
			keys, ids, err := s.collectDataKeysAndBlobsInRange(s.batch, op.ino, fullStart, fullEnd)
			if err != nil {
				return err
			}
			op.fullDeleteKeys = keys
			op.fullDeleteIDs = ids
		}

		for i := range op.partialSpecs {
			oldRef, oldOK, err := s.readDataBlobRef(s.batch, op.ino, op.partialSpecs[i].blockIdx)
			if err != nil {
				return err
			}
			op.partialSpecs[i].oldRef = oldRef
			op.partialSpecs[i].oldOK = oldOK
		}
	}

	op.target = inode
	op.newSize = newSize
	op.bytesDelta = bytesDelta
	return nil
}

func (s *StoreActor) mutateFallocateCmd(cmd *writeCmd) {
	op := cmd.req.(*fallocateDataOp)
	seq := s.stat.NextWriteSeq + 1
	s.stat.NextWriteSeq = seq
	nowNano := s.now().UnixNano()

	inode := op.target
	oldSize := inode.Size
	inode.Size = op.newSize
	if op.zeroRange {
		inode.Mode &^= S_ISUID | S_ISGID
	}
	if op.zeroRange || op.newSize > oldSize {
		inode.Mtime = nowNano
	}
	inode.Ctime = nowNano
	inode.WriteSeq = seq

	s.stat.LogicalBytes += op.bytesDelta

	_ = batchSetInodeDeferred(s.batch, inode)
	batchDeleteXattrDeferred(s.batch, op.ino, securityCapabilityXattr)
	batchSetDirtyDeferred(s.batch, op.ino)

	if op.zeroRange {
		for _, k := range op.fullDeleteKeys {
			batchDeleteDeferred(s.batch, k)
		}
		if len(op.fullDeleteIDs) > 0 {
			cmd.toDelete = append(cmd.toDelete, op.fullDeleteIDs...)
		}
		for i := range op.partialSpecs {
			b := &op.partialSpecs[i]
			if b.patch != nil {
				s.mergedPatches = append(s.mergedPatches, *b.patch)
			}
			if b.keep {
				if b.oldOK && b.oldRef.ID != b.newRef.ID {
					cmd.toDelete = append(cmd.toDelete, b.oldRef.ID)
				}
				if !b.sameRef {
					batchSetDataDeferred(s.batch, op.ino, b.blockIdx, b.newRef)
				}
			} else if b.oldOK {
				cmd.toDelete = append(cmd.toDelete, b.oldRef.ID)
				batchDeleteDataDeferred(s.batch, op.ino, b.blockIdx)
			}
		}
	}
}
