package store

import (
	"errors"
	"math"
)

func init() {
	setAttrSizeHook = (*StoreActor).setAttrWithSize
	registerOp(opFallocate, (*StoreActor).validateFallocateCmd, (*StoreActor).mutateFallocateCmd)
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
		if ref.PackID != 0 {
			blobIDs = append(blobIDs, ref.PackID)
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
		outAttr     Attribute
	)

	cmd := s.acquireCmd()
	cmd.op = opCustom
	cmd.validate = func(actor *StoreActor, _ *writeCmd) error {
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
			keys, _, err := actor.collectDataKeysAndBlobsFrom(actor.batch, ino, startDelBlock)
			if err != nil {
				return err
			}
			deleteKeys = keys

			if hasTailSpec {
				oldRef, oldOK, err := actor.readDataBlobRef(actor.batch, ino, tailSpec.blockIdx)
				if err != nil {
					return err
				}
				tailSpec.oldRef = oldRef
				tailSpec.oldOK = oldOK
			}
		} else if len(preAllocated) > 0 {
			hasTailSpec = false
		}

		cachedInode = inode
		return nil
	}
	cmd.mutate = func(actor *StoreActor, _ *writeCmd) {
		oldSize := cachedInode.Size
		seq := actor.stat.NextWriteSeq + 1
		actor.stat.NextWriteSeq = seq
		nowNano := actor.now().UnixNano()

		actor.applySetAttrFields(&cachedInode, req, nowNano, seq)

		if newSize < oldSize {
			for _, k := range deleteKeys {
				batchDeleteDeferred(actor.batch, k)
			}
			if hasTailSpec {
				if tailSpec.keep {
					if !tailSpec.sameRef {
						batchSetDataDeferred(actor.batch, ino, tailSpec.blockIdx, tailSpec.newRef)
					}
				} else if tailSpec.oldOK {
					batchDeleteDataDeferred(actor.batch, ino, tailSpec.blockIdx)
				}
			}
		}

		_ = batchSetInodeDeferred(actor.batch, cachedInode)
		batchSetDirtyDeferred(actor.batch, ino)
		outAttr = cachedInode.ToAttribute()
	}

	if err := s.enqueueAndWait(cmd); err != nil {
		return Attribute{}, err
	}
	return outAttr, nil
}

func (s *StoreActor) prepareTruncateTailBlock(blobs *BlobStore, ino, blockIdx uint64, oldRef BlobRef, tailLen uint32) (writeBlockSpec, uint64, error) {
	data := make([]byte, tailLen)
	if err := blobs.ReadAt(oldRef, 0, data); err != nil {
		return writeBlockSpec{}, 0, err
	}
	if isAllZero(data) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, 0, nil
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef: BlobRef{
			PackID: oldRef.PackID,
			Offset: oldRef.Offset,
			Length: tailLen,
		},
		keep:    true,
		sameRef: tailLen == oldRef.Length,
	}, 0, nil
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
	return s.enqueueAndWait(cmd)
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
	if err := blobs.ReadAt(oldRef, 0, data); err != nil {
		return writeBlockSpec{}, false, 0, err
	}
	if isAllZero(data[inBlock:clearEnd]) {
		return writeBlockSpec{}, false, 0, nil
	}
	clear(data[inBlock:clearEnd])

	if isAllZero(data) {
		return writeBlockSpec{
			blockIdx: blockIdx,
			keep:     false,
		}, true, 0, nil
	}

	ref, err := blobs.Append(ino, data)
	if err != nil {
		return writeBlockSpec{}, false, 0, err
	}
	return writeBlockSpec{
		blockIdx: blockIdx,
		newRef:   ref,
		keep:     true,
	}, true, ref.PackID, nil
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
		if ref.PackID != 0 {
			blobIDs = append(blobIDs, ref.PackID)
		}
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, nil, iterErr
	}
	return keys, blobIDs, nil
}

func (s *StoreActor) validateFallocateCmd(cmd *writeCmd) error {
	op := cmd.req.(*fallocateDataOp)

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
		for i := range op.partialSpecs {
			b := &op.partialSpecs[i]
			if b.keep {
				if !b.sameRef {
					batchSetDataDeferred(s.batch, op.ino, b.blockIdx, b.newRef)
				}
			} else if b.oldOK {
				batchDeleteDataDeferred(s.batch, op.ino, b.blockIdx)
			}
		}
	}
}
