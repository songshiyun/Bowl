package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/readbuf"
)

const (
	maxReadLockRetries      = 8
	maxConcurrentBlockReads = 8
)

type inFlightReadSliceKey struct {
	mgr     *manager
	sliceID int64
}

var (
	inFlightReadSlicesMu sync.Mutex
	inFlightReadSlices   = make(map[inFlightReadSliceKey]int)
)

func (m *manager) acquireInFlightReadSlices(sliceIDs []int64) {
	if m == nil || len(sliceIDs) == 0 {
		return
	}
	inFlightReadSlicesMu.Lock()
	for _, sid := range sliceIDs {
		if sid > 0 {
			inFlightReadSlices[inFlightReadSliceKey{mgr: m, sliceID: sid}]++
		}
	}
	inFlightReadSlicesMu.Unlock()
}

func (m *manager) releaseInFlightReadSlices(sliceIDs []int64) {
	if m == nil || len(sliceIDs) == 0 {
		return
	}
	inFlightReadSlicesMu.Lock()
	for _, sid := range sliceIDs {
		if sid <= 0 {
			continue
		}
		k := inFlightReadSliceKey{mgr: m, sliceID: sid}
		n := inFlightReadSlices[k] - 1
		if n <= 0 {
			delete(inFlightReadSlices, k)
		} else {
			inFlightReadSlices[k] = n
		}
	}
	inFlightReadSlicesMu.Unlock()
}

func (m *manager) isSliceReadInFlight(sliceID int64) bool {
	if m == nil || sliceID <= 0 {
		return false
	}
	inFlightReadSlicesMu.Lock()
	n := inFlightReadSlices[inFlightReadSliceKey{mgr: m, sliceID: sliceID}]
	inFlightReadSlicesMu.Unlock()
	return n > 0
}

func isBlockNotFoundErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrBlockNotFound) || errors.Is(err, os.ErrNotExist) {
		return true
	}
	msg := err.Error()
	return strings.Contains(msg, "no such file or directory") ||
		strings.Contains(msg, "file does not exist") ||
		strings.Contains(msg, "block not found")
}

type readChunkPlan struct {
	indx      int64
	slices    []Slice
	bufOffset uint32
	readStart uint32
	readEnd   uint32
}

type blockReadTask struct {
	sliceID   uint64
	blockIdx  uint32
	blockSize uint32
	blockOff  uint32
	dst       []byte
}

func buildReadChunkPlans(ino uint64, offset uint64, readLen uint32, currentRows []db.GetInodeSizeAndChunksInRangeRow) ([]readChunkPlan, []int64, error) {
	actualStart := offset
	actualEnd := offset + uint64(readLen)
	actualStartIndx := int64(actualStart / ChunkSize)
	actualEndIndx := int64((actualEnd - 1) / ChunkSize)

	chunksMap := make(map[int64][]byte, len(currentRows))
	for _, r := range currentRows {
		if r.ChunkIndx.Valid {
			chunksMap[r.ChunkIndx.Int64] = r.ChunkSlices
		}
	}

	var plans []readChunkPlan
	var sliceIDs []int64

	for indx := actualStartIndx; indx <= actualEndIndx; indx++ {
		chunkGlobalStart := uint64(indx) * ChunkSize
		chunkGlobalEnd := chunkGlobalStart + ChunkSize

		partGlobalStart := max(actualStart, chunkGlobalStart)
		partGlobalEnd := min(actualEnd, chunkGlobalEnd)
		if partGlobalStart >= partGlobalEnd {
			continue
		}

		chunkReadStart := uint32(partGlobalStart - chunkGlobalStart)
		chunkReadEnd := uint32(partGlobalEnd - chunkGlobalStart)
		bufOffset := uint32(partGlobalStart - actualStart)

		rawBytes := chunksMap[indx]
		if len(rawBytes) == 0 {
			continue
		}

		rawSlices, err := DecodeSlices(rawBytes)
		if err != nil {
			return nil, nil, fmt.Errorf("decode slices (ino=%d, indx=%d): %w", ino, indx, err)
		}

		projSlices := ProjectChunkSlices(rawSlices, chunkReadStart, chunkReadEnd)
		for _, ps := range projSlices {
			if ps.ID > 0 {
				sliceIDs = append(sliceIDs, int64(ps.ID))
			}
		}

		plans = append(plans, readChunkPlan{
			indx:      indx,
			slices:    projSlices,
			bufOffset: bufOffset,
			readStart: chunkReadStart,
			readEnd:   chunkReadEnd,
		})
	}

	slices.Sort(sliceIDs)
	sliceIDs = slices.Compact(sliceIDs)
	return plans, sliceIDs, nil
}

func buildBlockReadTasks(plans []readChunkPlan, finalBuf []byte) ([]blockReadTask, error) {
	var tasks []blockReadTask
	for _, cp := range plans {
		for _, s := range cp.slices {
			if s.ID == 0 {
				continue
			}

			bufStart := cp.bufOffset + (s.Pos - cp.readStart)
			locs, err := LocateBlocks(s.Size, s.Off, s.Len)
			if err != nil {
				return nil, fmt.Errorf("locate blocks (slice_id=%d, size=%d, off=%d, len=%d): %w", s.ID, s.Size, s.Off, s.Len, err)
			}

			curOverlapOff := uint32(0)
			for _, loc := range locs {
				dstStart := bufStart + curOverlapOff
				dstEnd := dstStart + loc.Length
				tasks = append(tasks, blockReadTask{
					sliceID:   s.ID,
					blockIdx:  loc.BlockIdx,
					blockSize: loc.BlockSize,
					blockOff:  loc.BlockOff,
					dst:       finalBuf[dstStart:dstEnd],
				})
				curOverlapOff += loc.Length
			}
		}
	}
	return tasks, nil
}

func executeBlockReadTasks(ctx context.Context, bs BlockStore, tasks []blockReadTask) error {
	if len(tasks) == 0 {
		return nil
	}
	if len(tasks) == 1 {
		t := tasks[0]
		if err := bs.GetBlock(ctx, t.sliceID, t.blockIdx, t.blockSize, t.blockOff, t.dst); err != nil {
			return fmt.Errorf("%w: get block (slice_id=%d, block_idx=%d): %w", ErrIO, t.sliceID, t.blockIdx, err)
		}
		return nil
	}

	readCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	concurrency := min(len(tasks), maxConcurrentBlockReads)
	sem := make(chan struct{}, concurrency)

	var (
		wg       sync.WaitGroup
		errOnce  sync.Once
		firstErr error
	)

	for _, task := range tasks {
		t := task
		wg.Add(1)
		go func() {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()

			if err := bs.GetBlock(readCtx, t.sliceID, t.blockIdx, t.blockSize, t.blockOff, t.dst); err != nil {
				errOnce.Do(func() {
					firstErr = fmt.Errorf("%w: get block (slice_id=%d, block_idx=%d): %w", ErrIO, t.sliceID, t.blockIdx, err)
					cancel()
				})
			}
		}()
	}

	wg.Wait()
	return firstErr
}

// ProjectChunkSlices 计算当前 Chunk 在 [chunkReadStart, chunkReadEnd) 区间内命中的投影切片列表。
// 内部通过 BuildSlice 构建紧致视图，并对有效交集切片做区间裁剪计算。
func ProjectChunkSlices(rawSlices []Slice, chunkReadStart, chunkReadEnd uint32) []Slice {
	view := BuildSlice(rawSlices)
	var result []Slice
	for _, s := range view {
		overlapStart := max(s.Pos, chunkReadStart)
		overlapEnd := min(s.Pos+s.Len, chunkReadEnd)
		if overlapStart >= overlapEnd {
			continue
		}
		overlapLen := overlapEnd - overlapStart
		sliceOff := s.Off + (overlapStart - s.Pos)
		result = append(result, Slice{
			Pos:  overlapStart,
			ID:   s.ID,
			Size: s.Size,
			Off:  sliceOff,
			Len:  overlapLen,
		})
	}
	return result
}

// Read 读取指定文件的数据块内容（对接 Linux FUSE FUSE_READ 协议）。
// 遵循 ADR-0019 / ADR-0028 数据层架构与 POSIX pread(2) 语义：
//  1. 静态边界校验：参数非空、(ino, fh, offset) 在合法正数范围内、size <= BlockSize (4 MiB)、session_id 合法；
//  2. 权限与类型校验：校验目标 Inode 必须为常规文件 S_IFREG（目录返回 ErrIsDir，其他非常规文件返回 ErrInvalidArgument），
//     校验 (ino, fh) 对应的 FileHandle 存在且具备读权限（O_RDONLY 或 O_RDWR；若是 O_WRONLY、句柄不存在或 ino 不匹配均返回 ErrBadFileHandle）；
//  3. 单条合并快照查询：首次查询使用 GetInodeChunksAndHandleForRead 一次性获取同一快照时刻的 (inode, file_handle, chunks)，
//     无需开启事务、无需二次查询句柄，杜绝跨 Chunk 循环多次 GetChunk 造成的并发写快照读撕裂；
//  4. 会话校验与访问时间：在 SQL 内直接校验有效 session_id 租约与重建代次；遵循配置的 AtimeMode（noatime/relatime/strictatime）决定是否更新 inodes.atime；
//  5. EOF 与截断读取：当 size == 0 或 offset >= inode.Size 时，直接返回空字节流（len(data) == 0）；
//     当 offset + size > inode.Size 时，自动截断实际读取长度 readLen = uint32(inode.Size - offset)；
//  6. 免数据库行锁快路径、并发 Compaction 乐观重试与多 Block 有界并发读盘：
//     - Read 通过无行锁只读查询拉取 Chunk 视图后立即释放数据库连接，不开启读写事务或对 slice_refs 加锁；
//     - 当单次读取涉及多个物理 Block 时，通过有界并发同时调用 BlockStore.GetBlock 填充返回缓冲区；
//     - 若读取物理块期间遇到并发 Compaction + GC 删块返回 ErrBlockNotFound，自动调用 GetInodeSizeAndChunksInRange 重新拉取最新合并后的 Chunk 视图透明重试。
func (m *manager) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	if req == nil {
		return nil, ErrInvalidArgument
	}
	if req.Ino == 0 || req.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Fh == 0 || req.Fh > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Offset > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Size > BlockSize {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(req.SessionID); err != nil {
		return nil, err
	}

	startIndx := int64(req.Offset / ChunkSize)
	endIndx := startIndx
	if req.Size > 0 {
		endIndx = int64((req.Offset + uint64(req.Size) - 1) / ChunkSize)
	}

	rows, err := m.store.GetInodeChunksAndHandleForRead(ctx, db.GetInodeChunksAndHandleForReadParams{
		Ino:       int64(req.Ino),
		Fh:        int64(req.Fh),
		SessionID: toPgSessionID(req.SessionID),
		StartIndx: startIndx,
		EndIndx:   endIndx,
	})
	if err != nil {
		return nil, fmt.Errorf("read tx: get inode chunks and handle (ino=%d, fh=%d): %w", req.Ino, req.Fh, err)
	}
	if len(rows) == 0 {
		return nil, ErrNotFound
	}

	firstRow := rows[0]
	mode := uint32(firstRow.Mode)
	if (mode & S_IFMT) == S_IFDIR {
		return nil, ErrIsDir
	}
	if (mode & S_IFMT) != S_IFREG {
		return nil, ErrInvalidArgument
	}
	if !firstRow.FhFound {
		return nil, ErrBadFileHandle
	}

	accMode := uint32(firstRow.FhFlags) & O_ACCMODE
	if accMode != O_RDONLY && accMode != O_RDWR {
		return nil, ErrBadFileHandle
	}

	if req.Size == 0 || req.Offset >= uint64(firstRow.Size) {
		return &ReadResponse{Data: []byte{}}, nil
	}

	currentRows := make([]db.GetInodeSizeAndChunksInRangeRow, len(rows))
	for i, r := range rows {
		currentRows[i] = db.GetInodeSizeAndChunksInRangeRow{
			Ino:           r.Ino,
			Generation:    r.Generation,
			Mode:          r.Mode,
			Size:          r.Size,
			Nlink:         r.Nlink,
			Uid:           r.Uid,
			Gid:           r.Gid,
			Rdev:          r.Rdev,
			Atime:         r.Atime,
			Mtime:         r.Mtime,
			Ctime:         r.Ctime,
			SymlinkTarget: r.SymlinkTarget,
			Flags:         r.Flags,
			ParentIno:     r.ParentIno,
			RootIno:       r.RootIno,
			ChunkIndx:     r.ChunkIndx,
			ChunkSlices:   r.ChunkSlices,
		}
	}

	bs := m.getBlockStore()
	buffer := readbuf.NewBuffer(ctx)

	for attempt := 0; attempt < maxReadLockRetries; attempt++ {
		if attempt > 0 {
			currentRows, err = m.store.GetInodeSizeAndChunksInRange(ctx, db.GetInodeSizeAndChunksInRangeParams{
				Ino:       int64(req.Ino),
				StartIndx: startIndx,
				EndIndx:   endIndx,
			})
			if err != nil {
				return nil, fmt.Errorf("read tx: re-get inode size and chunks (ino=%d): %w", req.Ino, err)
			}
			if len(currentRows) == 0 {
				return nil, ErrNotFound
			}
		}

		latestInode := currentRows[0]
		if req.Offset >= uint64(latestInode.Size) {
			return &ReadResponse{Data: []byte{}}, nil
		}

		readLen := req.Size
		if req.Offset+uint64(req.Size) > uint64(latestInode.Size) {
			readLen = uint32(uint64(latestInode.Size) - req.Offset)
		}

		plans, sliceIDs, err := buildReadChunkPlans(req.Ino, req.Offset, readLen, currentRows)
		if err != nil {
			return nil, fmt.Errorf("read tx: %w", err)
		}

		finalBuf := buffer.Bytes(readLen)
		tasks, err := buildBlockReadTasks(plans, finalBuf)
		if err != nil {
			if errors.Is(err, ErrInvalidArgument) {
				return nil, ErrInvalidArgument
			}
			return nil, fmt.Errorf("read tx: %w", err)
		}

		m.acquireInFlightReadSlices(sliceIDs)
		blockErr := executeBlockReadTasks(ctx, bs, tasks)
		m.releaseInFlightReadSlices(sliceIDs)

		if blockErr != nil {
			if isBlockNotFoundErr(blockErr) && attempt < maxReadLockRetries-1 {
				continue
			}
			if errors.Is(blockErr, ErrIO) {
				return nil, blockErr
			}
			return nil, fmt.Errorf("read tx: %w", blockErr)
		}

		if m.shouldTouchAtime(latestInode.Atime.Time, latestInode.Mtime.Time, latestInode.Ctime.Time, time.Now()) {
			if err := m.store.TouchInodeAtime(ctx, int64(req.Ino)); err != nil {
				return nil, fmt.Errorf("read tx: touch inode atime %d: %w", req.Ino, err)
			}
		}

		return &ReadResponse{Data: finalBuf}, nil
	}

	return nil, fmt.Errorf("%w: failed to read blocks after %d retries", ErrIO, maxReadLockRetries)
}
