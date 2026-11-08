package store

import (
	"bytes"
	"errors"
	"slices"

	"github.com/cockroachdb/pebble/v2"
)

func (s *StoreActor) loadPackStats() error {
	iter, err := s.db.NewIter(nil)
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	prefix := PackStatPrefix()
	for valid := iter.SeekGE(prefix); valid && bytes.HasPrefix(iter.Key(), prefix); valid = iter.Next() {
		packID, decErr := DecodePackStatKey(iter.Key())
		if decErr != nil {
			return decErr
		}
		meta, decErr := DecodePackMeta(iter.Value())
		if decErr != nil {
			return decErr
		}
		s.packStats[packID] = meta
	}
	return iter.Error()
}

func (s *StoreActor) syncPackStatsWithBlobs() error {
	bs := s.blobs
	if bs == nil {
		return nil
	}
	var maxPackID uint64
	var zeroLiveMetaPacks []uint64
	for packID, meta := range s.packStats {
		if packID > maxPackID {
			maxPackID = packID
		}
		if meta.LiveBytes == 0 {
			zeroLiveMetaPacks = append(zeroLiveMetaPacks, packID)
		}
	}

	var deadDiskPacks []uint64
	for _, packID := range bs.existingPackIDs() {
		if packID > maxPackID {
			maxPackID = packID
		}
		if meta, ok := s.packStats[packID]; !ok || meta.LiveBytes == 0 {
			deadDiskPacks = append(deadDiskPacks, packID)
		}
	}

	if maxPackID > 0 {
		bs.AdvanceMinPackID(maxPackID)
	}
	if s.readOnly {
		return nil
	}

	if len(zeroLiveMetaPacks) > 0 {
		cleanBatch := s.db.NewBatch()
		defer func() { _ = cleanBatch.Close() }()
		for _, packID := range zeroLiveMetaPacks {
			delete(s.packStats, packID)
			BatchDeletePackMetaDeferred(cleanBatch, packID)
			if !slices.Contains(deadDiskPacks, packID) {
				deadDiskPacks = append(deadDiskPacks, packID)
			}
		}
		if err := s.db.Apply(cleanBatch, pebble.Sync); err != nil {
			return err
		}
	} else if len(deadDiskPacks) > 0 {
		if err := s.db.LogData(nil, pebble.Sync); err != nil {
			return err
		}
	}
	for _, packID := range deadDiskPacks {
		_ = bs.RemovePack(packID)
	}
	return nil
}

func (s *StoreActor) addBlobRef(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta := s.packStats[ref.PackID]
	meta.LiveBytes += uint64(ref.Length)
	endOffset := uint64(ref.Offset) + uint64(ref.Length)
	if endOffset > meta.TotalBytes {
		meta.TotalBytes = endOffset
	}
	if meta.LiveBytes > meta.TotalBytes {
		meta.TotalBytes = meta.LiveBytes
	}
	s.packStats[ref.PackID] = meta
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

func (s *StoreActor) removeBlobRef(ref BlobRef) {
	if ref.PackID == 0 || ref.Length == 0 {
		return
	}
	meta, ok := s.packStats[ref.PackID]
	if !ok {
		return
	}
	dec := uint64(ref.Length)
	if dec >= meta.LiveBytes {
		meta.LiveBytes = 0
	} else {
		meta.LiveBytes -= dec
	}
	s.packStats[ref.PackID] = meta
	s.dirtyPackStats[ref.PackID] = struct{}{}
}

// recordDeadBlobWrite 记录 Stage 1 已预写至 .pack 但 Stage 2 Phase A 校验失败（或 CAS 未命中）的死块字节，
// 确保目标 .pack 的 TotalBytes 准确反映物理已用偏移并在 LiveBytes == 0 时标记待回收。
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

func (s *StoreActor) activePackID() uint64 {
	if s.blobs != nil {
		return s.blobs.ActivePackID()
	}
	return 0
}

// stageAppend 在 Stage 1 向活跃 .pack 追加写入数据块，并将生成的 BlobRef 纳入命令级预写租约（自动 Pin 所属 PackID）。
func (s *StoreActor) stageAppend(blobs *BlobStore, ino uint64, data []byte, staged *[]BlobRef) (BlobRef, error) {
	ref, err := blobs.Append(ino, data)
	if err != nil {
		return BlobRef{}, err
	}
	s.pinPackID(ref.PackID)
	if staged != nil {
		*staged = append(*staged, ref)
	}
	return ref, nil
}

// stageExtendTail 在 Stage 1 尝试原地顺延扩展活跃 .pack 最末尾同块，成功后自动将更新后的 BlobRef 纳入预写租约。
func (s *StoreActor) stageExtendTail(blobs *BlobStore, ino uint64, oldRef BlobRef, patchOffset uint32, data []byte, newLen uint32, staged *[]BlobRef) (BlobRef, bool, error) {
	extRef, ok, err := blobs.ExtendTail(ino, oldRef, patchOffset, data, newLen)
	if err != nil || !ok {
		return BlobRef{}, false, err
	}
	s.pinPackID(extRef.PackID)
	if staged != nil {
		*staged = append(*staged, extRef)
	}
	return extRef, true, nil
}

func (s *StoreActor) pinPackID(packID uint64) {
	if packID == 0 {
		return
	}
	s.inFlightMu.Lock()
	s.inFlightPacks[packID]++
	s.inFlightMu.Unlock()
}

func (s *StoreActor) pinPackIDs(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, id := range packIDs {
		if id != 0 {
			s.inFlightPacks[id]++
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) unpinPackIDs(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, id := range packIDs {
		if c := s.inFlightPacks[id]; c <= 1 {
			delete(s.inFlightPacks, id)
		} else {
			s.inFlightPacks[id] = c - 1
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) unpinStagedBlobs(staged []BlobRef) {
	if len(staged) == 0 {
		return
	}
	s.inFlightMu.Lock()
	for _, ref := range staged {
		if ref.PackID == 0 {
			continue
		}
		if c := s.inFlightPacks[ref.PackID]; c <= 1 {
			delete(s.inFlightPacks, ref.PackID)
		} else {
			s.inFlightPacks[ref.PackID] = c - 1
		}
	}
	s.inFlightMu.Unlock()
}

func (s *StoreActor) isPackPinned(packID uint64) bool {
	s.inFlightMu.Lock()
	pinned := s.inFlightPacks[packID] > 0
	s.inFlightMu.Unlock()
	return pinned
}

func (s *StoreActor) flushDirtyPackStatsToBatch(batch *pebble.Batch) (deadPacks []uint64, wrote bool) {
	activeID := s.activePackID()
	if s.lastActivePackID != 0 && s.lastActivePackID != activeID {
		if meta, ok := s.packStats[s.lastActivePackID]; ok && meta.LiveBytes == 0 {
			s.dirtyPackStats[s.lastActivePackID] = struct{}{}
		}
	}
	s.lastActivePackID = activeID

	for packID := range s.dirtyPackStats {
		meta, ok := s.packStats[packID]
		if !ok {
			continue
		}
		if meta.LiveBytes == 0 && packID != activeID && !s.isPackPinned(packID) {
			delete(s.packStats, packID)
			BatchDeletePackMetaDeferred(batch, packID)
			deadPacks = append(deadPacks, packID)
			wrote = true
			continue
		}
		BatchSetPackMetaDeferred(batch, packID, meta)
		wrote = true
	}
	clear(s.dirtyPackStats)
	return deadPacks, wrote
}

func (s *StoreActor) addPendingDeadPacks(packIDs []uint64) {
	if len(packIDs) == 0 {
		return
	}
	s.deadPacksMu.Lock()
	for _, id := range packIDs {
		if id != 0 && !slices.Contains(s.pendingDeadPacks, id) {
			s.pendingDeadPacks = append(s.pendingDeadPacks, id)
		}
	}
	s.deadPacksMu.Unlock()
}

func (s *StoreActor) takePendingDeadPacks() []uint64 {
	s.deadPacksMu.Lock()
	defer s.deadPacksMu.Unlock()
	if len(s.pendingDeadPacks) == 0 {
		return nil
	}
	out := s.pendingDeadPacks
	s.pendingDeadPacks = nil
	return out
}

func (s *StoreActor) restorePendingDeadPacks(packIDs []uint64) {
	s.addPendingDeadPacks(packIDs)
}

func (s *StoreActor) removePacks(packIDs []uint64) {
	if len(packIDs) == 0 || s.blobs == nil {
		return
	}
	for _, packID := range packIDs {
		_ = s.blobs.RemovePack(packID)
	}
}

func (s *StoreActor) syncFileInodes(inodes []uint64) error {
	if s.blobs == nil || len(inodes) == 0 {
		return nil
	}
	var err error
	for _, ino := range inodes {
		err = errors.Join(err, s.blobs.SyncInode(ino))
	}
	return err
}

// syncWALAndReclaimDeadPacks 在存在未落盘 NoSync WAL 写入或挂起死包时执行同步 WAL 屏障，
// 并在 WAL 落盘确认后物理移除已无存活引用的封口 .pack 文件（ADR-0031 §4.2）。
func (s *StoreActor) syncWALAndReclaimDeadPacks() error {
	toReclaim := s.takePendingDeadPacks()
	wasDirty := s.walDirty.Swap(false)
	if !wasDirty && len(toReclaim) == 0 {
		return nil
	}
	if err := s.db.LogData(nil, pebble.Sync); err != nil {
		s.restorePendingDeadPacks(toReclaim)
		s.walDirty.Store(true)
		return err
	}
	s.removePacks(toReclaim)
	return nil
}
