package meta

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type chunkAppendStat struct {
	indx     int64
	octetLen int64
}

type compactDataSegment struct {
	pos    uint32
	length uint32
	slices []Slice
	data   []byte
}

type compactWrittenBlock struct {
	sliceID   uint64
	blockIdx  uint32
	blockSize uint32
}

type compactNewSliceDesc struct {
	id     uint64
	size   uint32
	pos    uint32
	length uint32
}

// CompactChunk 对指定 Inode 与 ChunkIndx 执行整体合并与乐观前缀 CAS 替换。
// 遵循 ADR-0019、ADR-0022 与 ADR-0027 数据层碎片整理与 CAS 替换架构：
// 1. 静态边界校验：ino 非零且在 int64 范围内，indx 在合法非负范围且不超过 math.MaxInt64/int64(ChunkSize)；
// 2. 阶段 1（短读保护事务内读取与锁外新物理块落盘）：
//   - 调用 GetChunk 获取 originSlices（若 pgx.ErrNoRows 或 len(originSlices) == 0 直接返回 nil）；
//   - DecodeSlices 获取当前原始切片序列 rawSlices，若 enforceThreshold && len(rawSlices) < m.compactSliceThreshold 则直接短路返回；
//   - 对整个 rawSlices 调用 BuildSlice 计算最新有效切片视图 view []Slice；
//   - 将 view 中连续的非空洞数据切片（s.ID > 0 且相邻切片连续）合并为紧致连续数据段；
//   - 对每个连续有效数据段：
//   - 从 BlockStore 读出各有效 Slice 对应的字节数据拼接为完整 segData；
//   - 调用 NextSliceID 分配新 newSliceID；
//   - 按 SliceBlocks 切分为不超过 4 MiB 的物理块并调用 PutBlock 落盘；
//   - 将紧致 Slice{Pos: segPos, ID: newSliceID, Size: segLen, Off: 0, Len: segLen} 编码追加到 compactedSlices；
//   - 若中途发生任何读取、NextSliceID 或 PutBlock 错误，立即清理已写入的物理块并返回错误；
//
// 3. 阶段 2（写事务内乐观前缀 CAS 替换）：
//   - 在事务内锁定当前目标行（GetChunkForUpdate）；
//   - 若行不存在或 !bytes.HasPrefix(currentChunk.Slices, originSlices)：
//   - 安全中止放弃替换，事务返回 nil，并在事务外立即清理阶段 1 生成的全部新物理块；
//   - 若 bytes.HasPrefix 成立：
//   - 构造 updatedSlices = compactedSlices || currentChunk.Slices[len(originSlices):]；
//   - 更新 chunks.slices；
//   - 插入新 slice_refs（refs = 1）；
//   - 对全部被替换的旧切片 originSlices 调用 decrementDeletedSlicesRefs 递减旧 slice_id 的引用计数；
//   - 若写事务执行失败，在确认未到达提交边界时于事务外立即清理新物理块并返回错误。
func (m *manager) CompactChunk(ctx context.Context, ino uint64, indx int64) error {
	_, _, _, err := m.compactChunkWithCount(ctx, ino, indx, false)
	return err
}

func (m *manager) compactChunkWithCount(ctx context.Context, ino uint64, indx int64, enforceThreshold bool) (int, bool, bool, error) {
	if ino == 0 || ino > math.MaxInt64 || indx < 0 || indx > math.MaxInt64/int64(ChunkSize) {
		return 0, false, false, ErrInvalidArgument
	}

	// 阶段 1：在短读保护事务内读取旧 Chunk，锁定有效 slice_refs 并从 BlockStore 读出各段数据拼装到内存
	var originSlices []byte
	var segments []compactDataSegment
	var shortCircuitSliceCount int

	bs := m.getBlockStore()

	readTxErr := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		const maxLockRetries = 8
		for attempt := 0; attempt < maxLockRetries; attempt++ {
			chunk, err := q.GetChunk(ctx, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: indx,
			})
			if err != nil {
				if errors.Is(err, pgx.ErrNoRows) {
					originSlices = nil
					segments = nil
					shortCircuitSliceCount = 0
					return nil
				}
				return fmt.Errorf("get chunk for compact (ino=%d, indx=%d): %w", ino, indx, err)
			}
			if len(chunk.Slices) == 0 {
				originSlices = nil
				segments = nil
				shortCircuitSliceCount = 0
				return nil
			}

			rawSlices, err := DecodeSlices(chunk.Slices)
			if err != nil {
				return fmt.Errorf("decode origin slices (ino=%d, indx=%d): %w", ino, indx, err)
			}

			if len(rawSlices) == 0 || (enforceThreshold && len(rawSlices) < m.compactSliceThreshold) {
				originSlices = nil
				segments = nil
				shortCircuitSliceCount = len(rawSlices)
				return nil
			}

			view := BuildSlice(rawSlices)

			var segs []compactDataSegment
			var curSeg *compactDataSegment

			for _, s := range view {
				if s.ID == 0 {
					if curSeg != nil {
						segs = append(segs, *curSeg)
						curSeg = nil
					}
					continue
				}

				if curSeg == nil {
					curSeg = &compactDataSegment{
						pos:    s.Pos,
						length: s.Len,
						slices: []Slice{s},
					}
				} else {
					if curSeg.pos+curSeg.length == s.Pos {
						curSeg.length += s.Len
						curSeg.slices = append(curSeg.slices, s)
					} else {
						segs = append(segs, *curSeg)
						curSeg = &compactDataSegment{
							pos:    s.Pos,
							length: s.Len,
							slices: []Slice{s},
						}
					}
				}
			}
			if curSeg != nil {
				segs = append(segs, *curSeg)
			}

			// 提取所有需读取物理文件的去重升序非零 sliceIDs
			var sliceIDs []int64
			for _, seg := range segs {
				for _, s := range seg.slices {
					if s.ID > 0 {
						sliceIDs = append(sliceIDs, int64(s.ID))
					}
				}
			}
			slices.Sort(sliceIDs)
			sliceIDs = slices.Compact(sliceIDs)

			if len(sliceIDs) > 0 {
				lockedIDs, err := q.LockValidSliceRefsKeyShare(ctx, sliceIDs)
				if err != nil {
					return fmt.Errorf("lock valid slice refs for compact: %w", err)
				}
				if len(lockedIDs) < len(sliceIDs) {
					if attempt == maxLockRetries-1 {
						return fmt.Errorf("%w: failed to lock slice refs for compact after %d retries", ErrIO, maxLockRetries)
					}
					continue
				}
			}

			// 在持有 FOR KEY SHARE 读保护事务期间调用 m.blockStore.GetBlock 拼装好内存合并数据
			for i := range segs {
				segs[i].data = make([]byte, segs[i].length)
				var segOff uint32
				for _, s := range segs[i].slices {
					locs, err := LocateBlocks(s.Size, s.Off, s.Len)
					if err != nil {
						return fmt.Errorf("locate blocks for compact slice %d: %w", s.ID, err)
					}
					var sOff uint32
					for _, loc := range locs {
						dst := segs[i].data[segOff+sOff : segOff+sOff+loc.Length]
						if err := bs.GetBlock(ctx, s.ID, loc.BlockIdx, loc.BlockSize, loc.BlockOff, dst); err != nil {
							return fmt.Errorf("%w: get block for compact (slice_id=%d, block_idx=%d): %v", ErrIO, s.ID, loc.BlockIdx, err)
						}
						sOff += loc.Length
					}
					segOff += s.Len
				}
			}

			originSlices = chunk.Slices
			segments = segs
			shortCircuitSliceCount = 0
			return nil
		}
		return nil
	})
	if readTxErr != nil {
		return 0, false, false, readTxErr
	}
	if len(originSlices) == 0 {
		return shortCircuitSliceCount, false, false, nil
	}

	var writtenBlocks []compactWrittenBlock

	cleanupBlocks := func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, b := range writtenBlocks {
			_ = bs.DeleteBlock(cleanupCtx, b.sliceID, b.blockIdx, b.blockSize)
		}
	}

	var newSlices []compactNewSliceDesc
	var compactedSlices []byte

	for _, seg := range segments {
		segData := seg.data

		newID, err := m.store.NextSliceID(ctx)
		if err != nil {
			cleanupBlocks()
			return 0, false, false, fmt.Errorf("next slice id for compact: %w", err)
		}

		blocks := SliceBlocks(seg.length)
		for _, b := range blocks {
			start := b.BlockIdx * BlockSize
			end := start + b.BlockSize
			bData := segData[start:end]
			if err := bs.PutBlock(ctx, uint64(newID), b.BlockIdx, bData); err != nil {
				cleanupBlocks()
				return 0, false, false, fmt.Errorf("%w: put block for compact (slice_id=%d, block_idx=%d): %v", ErrIO, newID, b.BlockIdx, err)
			}
			writtenBlocks = append(writtenBlocks, compactWrittenBlock{
				sliceID:   uint64(newID),
				blockIdx:  b.BlockIdx,
				blockSize: b.BlockSize,
			})
		}

		compactedSlice := Slice{
			Pos:  seg.pos,
			ID:   uint64(newID),
			Size: seg.length,
			Off:  0,
			Len:  seg.length,
		}
		compactedSlices = AppendEncodedSlice(compactedSlices, compactedSlice)
		newSlices = append(newSlices, compactNewSliceDesc{
			id:     uint64(newID),
			size:   seg.length,
			pos:    seg.pos,
			length: seg.length,
		})
	}

	if len(compactedSlices) == 0 {
		compactedSlices = EncodeSlice(Slice{
			Pos:  0,
			ID:   0,
			Size: 0,
			Off:  0,
			Len:  ChunkSize,
		})
	}

	// 阶段 2：数据库写事务内乐观前缀 CAS 替换
	var casAborted bool
	var everReachedCommit bool
	var finalSliceCount int

	txErr := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		everReachedCommit = false
		casAborted = false
		currentChunk, err := q.GetChunkForUpdate(ctx, db.GetChunkForUpdateParams{
			Ino:  int64(ino),
			Indx: indx,
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				casAborted = true
				return nil
			}
			return fmt.Errorf("get chunk for update in compact (ino=%d, indx=%d): %w", ino, indx, err)
		}

		if !bytes.HasPrefix(currentChunk.Slices, originSlices) {
			casAborted = true
			return nil
		}

		concurrentSuffix := currentChunk.Slices[len(originSlices):]

		var updatedSlices []byte
		if len(concurrentSuffix) == 0 {
			updatedSlices = compactedSlices
		} else {
			updatedSlices = make([]byte, 0, len(compactedSlices)+len(concurrentSuffix))
			updatedSlices = append(updatedSlices, compactedSlices...)
			updatedSlices = append(updatedSlices, concurrentSuffix...)
		}

		if err := q.UpdateChunkSlices(ctx, db.UpdateChunkSlicesParams{
			Ino:    int64(ino),
			Indx:   indx,
			Slices: updatedSlices,
		}); err != nil {
			return fmt.Errorf("update chunk slices in compact (ino=%d, indx=%d): %w", ino, indx, err)
		}

		for _, ns := range newSlices {
			if err := q.InsertSliceRef(ctx, db.InsertSliceRefParams{
				SliceID: int64(ns.id),
				Size:    int32(ns.size),
				Refs:    1,
			}); err != nil {
				return fmt.Errorf("insert slice ref in compact (slice_id=%d): %w", ns.id, err)
			}
		}

		if err := decrementDeletedSlicesRefs(ctx, q, originSlices); err != nil {
			return fmt.Errorf("decrement deleted slices refs in compact: %w", err)
		}

		finalSliceCount = len(updatedSlices) / SliceBytes
		everReachedCommit = true
		return nil
	})

	if txErr != nil {
		if !everReachedCommit {
			cleanupBlocks()
		}
		return 0, false, false, txErr
	}

	if casAborted {
		cleanupBlocks()
		return 0, true, false, nil
	}

	return finalSliceCount, false, true, nil
}

// maybeTriggerAsyncCompact 当追加后的切片字节长度达到压缩阈值时，将目标 Chunk 去重非阻塞地投递至唯一后台 Worker。
func (m *manager) maybeTriggerAsyncCompact(ino uint64, indx int64, octetLen int64) {
	if m.compactSliceThreshold <= 0 || int(octetLen)/SliceBytes < m.compactSliceThreshold {
		return
	}
	key := chunkKey{ino: ino, indx: indx}

	m.workerMu.Lock()
	if m.workerStopped {
		m.workerMu.Unlock()
		return
	}
	if m.compactState == nil {
		m.compactState = make(map[chunkKey]compactTaskState)
	}
	if m.compactWakeCh == nil {
		m.compactWakeCh = make(chan struct{}, 1)
	}

	switch m.compactState[key] {
	case compactStateIdle:
		m.compactState[key] = compactStateQueued
		m.compactQueue = append(m.compactQueue, key)
	case compactStateRunning:
		m.compactState[key] = compactStateRerun
	case compactStateQueued, compactStateRerun:
		// 同一 Chunk 的重复通知自动合并去重
	}

	if m.workerDoneCh == nil {
		m.startBackgroundWorkerLocked(context.Background(), m.gcInterval, m.gcBatchLimit, false)
	}
	select {
	case m.compactWakeCh <- struct{}{}:
	default:
	}
	m.workerMu.Unlock()
}
