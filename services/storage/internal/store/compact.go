package store

import (
	"bytes"
	"errors"
	"slices"
)

func init() {
	registerOp(opCompactRelocate, (*StoreActor).validateCompactRelocateCmd, (*StoreActor).mutateCompactRelocateCmd)
}

type relocateBlockSpec struct {
	ino      uint64
	blockIdx uint64
	oldRef   BlobRef
	newRef   BlobRef

	// Phase A CAS 校验结果（true 表示节点仍存活且 curRef == oldRef）
	matched bool
}

type compactRelocateOp struct {
	specs []relocateBlockSpec
}

// SetBeforeCompactRelocateHookForTest 注册在搬迁活块读取追加完成、向 StoreActor 投递 opCompactRelocate 前触发的测试钩子。
func (s *StoreActor) SetBeforeCompactRelocateHookForTest(hook func()) {
	s.beforeCompactRelocateHook = hook
}

// CompactPacks 执行基于 ShardStore (SOSP '21) 的低水位已封口 .pack 搬迁回收（ADR-0031 §4.2）：
//  1. 筛选存活率低于水位线（srcPackID != activePackID && LiveBytes > 0 && LiveBytes * 2 <= TotalBytes）的已封口 .pack；
//  2. 扫描当前分支 Pebble DataKey 找出仍指向候选 srcPackID 的活块 (ino, blockIdx, oldRef)，在队列外从源包读取块内容并追加写入活跃 .pack 获得 newRef；
//  3. 投递 opCompactRelocate 至 StoreActor：
//     - Phase A：通过 s.batch.Get(DataKey(ino, blockIdx)) 原子 CAS 校验目标节点仍存活且 curRef == oldRef；若已被前台并发覆写、截断或删除，跳过指针更新并将预写 newRef 记为死空间（不计入 LiveBytes）；
//     - Phase B：仅对通过 CAS 校验的块将 DataKey 更新为 newRef，扣减 srcPackID 的 LiveBytes 并累加 newRef.PackID 的 LiveBytes；
//  4. 当 srcPackID 的 LiveBytes 归零后，强制先完成目标新 .pack 的 fdatasync（blobs.Sync()）与 Pebble WAL Sync（FlushWAL()）双重持久化屏障，随后删除旧 .pack 文件。
func (s *StoreActor) CompactPacks() error {
	if s.readOnly {
		return ErrReadOnly
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

	candidates, err := s.selectLowWatermarkPacks()
	if err != nil {
		return err
	}
	if len(candidates) == 0 {
		return s.FlushWAL()
	}

	candSet := make(map[uint64]struct{}, len(candidates))
	for _, id := range candidates {
		candSet[id] = struct{}{}
	}

	// 1. 扫描当前分支 Pebble DataKey，找出所有指向低水位候选包的活块
	liveBlocks, err := s.scanLiveBlocksInPacks(candSet)
	if err != nil {
		return err
	}

	if len(liveBlocks) > 0 {
		// 先钉住所有源候选包与即将写入的目标活跃包，使 Actor Step 3 不会在目标新包完成 fdatasync 前提前触发 RemovePack
		var pinned []uint64
		for _, srcID := range candidates {
			s.pinPack(srcID, &pinned)
		}

		specs := make([]relocateBlockSpec, 0, len(liveBlocks))
		for _, blk := range liveBlocks {
			data, readErr := blobs.Read(blk.oldRef)
			if readErr != nil {
				s.unpinPacks(pinned)
				return readErr
			}
			newRef, writeErr := s.appendBlob(blobs, blk.ino, data, &pinned)
			if writeErr != nil {
				s.unpinPacks(pinned)
				return writeErr
			}
			specs = append(specs, relocateBlockSpec{
				ino:      blk.ino,
				blockIdx: blk.blockIdx,
				oldRef:   blk.oldRef,
				newRef:   newRef,
			})
		}

		if s.beforeCompactRelocateHook != nil {
			s.beforeCompactRelocateHook()
		}

		op := &compactRelocateOp{specs: specs}
		cmd := s.acquireCmd()
		cmd.op = opCompactRelocate
		cmd.req = op
		// 注意：保留源候选包在 inFlightPacks 中的钉住计数直至完成 blobs.Sync()，
		// 仅将新分配包交由 cmd.pinnedPacks 在 Phase B 释放。
		s.unpinPacks(candidates)
		for _, srcID := range candidates {
			s.pinPack(srcID, nil)
		}
		cmd.pinnedPacks = filterNonCandidatePins(pinned, candSet)

		relocErr := s.enqueueAndWait(cmd)
		if relocErr != nil {
			s.unpinPacks(candidates)
			return relocErr
		}

		// 4. Soft Updates 双重持久化屏障：
		// 先将目标新 .pack 持久化落盘（fdatasync），再释放源包钉住并在 Actor 内归零回收，最后同步 Pebble WAL 并调用 RemovePack。
		syncErr := blobs.Sync()
		s.unpinPacks(candidates)
		if syncErr != nil {
			return syncErr
		}
	}

	// 触发已归零候选封口包的 sys:pack 删除与 WAL Sync + RemovePack
	if err := s.reapZeroLiveCandidatePacks(candidates); err != nil {
		return err
	}
	return s.FlushWAL()
}

func filterNonCandidatePins(pinned []uint64, candSet map[uint64]struct{}) []uint64 {
	out := make([]uint64, 0, len(pinned))
	for _, id := range pinned {
		if _, isCand := candSet[id]; !isCand {
			out = append(out, id)
		}
	}
	return out
}

func (s *StoreActor) selectLowWatermarkPacks() ([]uint64, error) {
	var candidates []uint64
	cmd := s.acquireCmd()
	cmd.op = opCustom
	cmd.validate = func(actor *StoreActor, _ *writeCmd) error {
		activeID := actor.activePackID()
		for packID, meta := range actor.packStats {
			if packID == 0 || packID == activeID {
				continue
			}
			if meta.LiveBytes > 0 && meta.LiveBytes*2 <= meta.TotalBytes {
				candidates = append(candidates, packID)
			}
		}
		slices.Sort(candidates)
		return nil
	}
	if err := s.enqueueAndWait(cmd); err != nil {
		return nil, err
	}
	return candidates, nil
}

type liveBlockCandidate struct {
	ino      uint64
	blockIdx uint64
	oldRef   BlobRef
}

func (s *StoreActor) scanLiveBlocksInPacks(candSet map[uint64]struct{}) (out []liveBlockCandidate, err error) {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()

	for valid := iter.SeekGE(dataPrefixBytes); valid && bytes.HasPrefix(iter.Key(), dataPrefixBytes); valid = iter.Next() {
		ino, blockIdx, decErr := DecodeDataKey(iter.Key())
		if decErr != nil {
			return nil, decErr
		}
		ref, decErr := DecodeBlobRef(iter.Value())
		if decErr != nil {
			return nil, decErr
		}
		if _, ok := candSet[ref.PackID]; ok {
			out = append(out, liveBlockCandidate{
				ino:      ino,
				blockIdx: blockIdx,
				oldRef:   ref,
			})
		}
	}
	if iterErr := iter.Error(); iterErr != nil {
		return nil, iterErr
	}
	return out, nil
}

// validateCompactRelocateCmd 执行 Phase A 原子 CAS 校验（防御 ShardStore 论文 Bug #1）：
// 严格通过 s.batch.Get(DataKey(ino, blockIdx)) 校验目标节点仍存活且 curRef == oldRef。
func (s *StoreActor) validateCompactRelocateCmd(cmd *writeCmd) error {
	op := cmd.req.(*compactRelocateOp)
	for i := range op.specs {
		spec := &op.specs[i]
		spec.matched = false

		inode, err := s.readInode(s.batch, spec.ino)
		if errors.Is(err, ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if err := s.checkInodeLive(s.batch, inode); errors.Is(err, ErrNotFound) {
			continue
		} else if err != nil {
			return err
		}
		if inode.Mode&S_IFMT != S_IFREG {
			continue
		}
		// 校验该逻辑块仍在当前文件 Size 范围内
		if spec.blockIdx*BlobSize >= inode.Size {
			continue
		}

		curRef, ok, err := s.readDataBlobRef(s.batch, spec.ino, spec.blockIdx)
		if err != nil {
			return err
		}
		if ok && curRef == spec.oldRef {
			spec.matched = true
		}
	}
	return nil
}

// mutateCompactRelocateCmd 执行 Phase B 原子指针切换与 sys:pack 记账更新：
// 仅对 Phase A 通过 CAS 校验的块更新 DataKey -> newRef；未通过 CAS 校验的预写块仅推进目标包的 TotalBytes（LiveBytes 不增加）。
func (s *StoreActor) mutateCompactRelocateCmd(cmd *writeCmd) {
	op := cmd.req.(*compactRelocateOp)
	for i := range op.specs {
		spec := &op.specs[i]
		if !spec.matched {
			s.recordDeadBlobWrite(spec.newRef)
			continue
		}
		s.removeBlobRef(spec.oldRef)
		s.addBlobRef(spec.newRef)
		batchSetDataDeferred(s.batch, spec.ino, spec.blockIdx, spec.newRef)
	}
}

func (s *StoreActor) recordDeadBlobWrite(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta := s.packStats[ref.PackID]
	endOffset := uint64(ref.Offset) + uint64(ref.Length)
	if endOffset > meta.TotalBytes {
		meta.TotalBytes = endOffset
	}
	s.packStats[ref.PackID] = meta
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) reapZeroLiveCandidatePacks(candidates []uint64) error {
	if len(candidates) == 0 {
		return nil
	}
	cmd := s.acquireCmd()
	cmd.op = opCustom
	cmd.mutate = func(actor *StoreActor, _ *writeCmd) {
		for _, id := range candidates {
			if meta, ok := actor.packStats[id]; ok && meta.LiveBytes == 0 {
				actor.dirtyPackStats[id] = struct{}{}
			}
		}
	}
	return s.enqueueAndWait(cmd)
}
