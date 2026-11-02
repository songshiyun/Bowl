package meta

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
)

func mapWriteBizError(err error) error {
	switch {
	case errors.Is(err, ErrInvalidArgument):
		return ErrInvalidArgument
	case errors.Is(err, ErrNotFound):
		return ErrNotFound
	case errors.Is(err, ErrIsDir):
		return ErrIsDir
	case errors.Is(err, ErrBadFileHandle):
		return ErrBadFileHandle
	case errors.Is(err, ErrNoSpace):
		return ErrNoSpace
	case errors.Is(err, ErrIO):
		return err
	default:
		return fmt.Errorf("write commit tx: %w", err)
	}
}

// Write 向指定文件写入数据块内容（对接 Linux FUSE FUSE_WRITE 协议）。
// 遵循 ADR-0019、ADR-0022 与 ADR-0028 两阶段流水线与单语句 CTE + 异步 WAL 架构：
// 1. 静态边界校验：参数非空、(ino, fh, offset) 在正数范围内、len(data) <= 4 MiB、offset + len(data) <= MaxInt64；
// 2. 零字节写入：在事务内锁定 Inode 校验常规文件类型与写句柄有效性，不分配 slice_id 且不修改任何元数据；
// 3. 两阶段流水线落盘：
//   - 阶段 1（无锁并行写入页缓存）：从内存号段分配器领取 slice_id，将 data 直接写入底层物理块存储目标路径（PutBlock 直接创建最终块文件并完整写入后 Close，发布到 OS Page Cache）；失败立即清理物理块返回 ErrIO；
//   - 阶段 2（单语句 CTE 原子提交）：调用 WriteFileAtomic 在单条 SQL 语句内完成 synchronous_commit=off、锁定 Inode、校验句柄与会话、计算偏移（含 O_APPEND）、校验磁盘配额、追加 Chunk Slice、登记 slice_refs 与更新 Inode 属性（1 RTT）；若失败或返回非 ok 状态，立即调用 DeleteBlock 清理物理块（零孤儿块残留）。
func (m *manager) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
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
	dataLen := len(req.Data)
	if dataLen > BlockSize {
		return nil, ErrInvalidArgument
	}
	if req.Offset > uint64(math.MaxInt64)-uint64(dataLen) {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(req.SessionID); err != nil {
		return nil, err
	}

	// 零字节写入：校验 Inode 与句柄有效性，直接返回，不触发物理落盘与元数据修改
	if dataLen == 0 {
		return m.writeZeroBytes(ctx, req)
	}

	// 阶段 1：行锁外从内存号段分配器领取 slice_id 与并发落盘物理块
	sliceID, err := m.allocSliceID(ctx)
	if err != nil {
		return nil, fmt.Errorf("next slice id: %w", err)
	}

	bs := m.getBlockStore()
	blocks := SliceBlocks(uint32(dataLen))
	writtenBlocks := make([]SliceBlockInfo, 0, len(blocks))

	var cleanupOnce sync.Once
	cleanupBlocks := func() {
		cleanupOnce.Do(func() {
			cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			for _, b := range writtenBlocks {
				_ = bs.DeleteBlock(cleanupCtx, uint64(sliceID), b.BlockIdx, b.BlockSize)
			}
		})
	}

	for _, b := range blocks {
		start := b.BlockIdx * BlockSize
		end := start + b.BlockSize
		blockData := req.Data[start:end]
		if err := bs.PutBlock(ctx, uint64(sliceID), b.BlockIdx, blockData); err != nil {
			cleanupBlocks()
			return nil, fmt.Errorf("%w: put block (slice_id=%d, block_idx=%d): %v", ErrIO, sliceID, b.BlockIdx, err)
		}
		writtenBlocks = append(writtenBlocks, b)
	}

	// 阶段 2：单语句 CTE 原子提交
	const tightQuotaThreshold int64 = 64 * 1024 * 1024 // 64 MiB
	availBytes := m.getWriteQuotaAvailBytes(req.Ino)
	serializeQuota := availBytes < 0 || availBytes < tightQuotaThreshold || int64(req.Offset)+int64(dataLen) > availBytes
	if serializeQuota {
		m.writeQuotaMu.Lock()
		defer m.writeQuotaMu.Unlock()
	}

	var row db.WriteFileAtomicRow
	err = tx.Retry(ctx, func(attemptCtx context.Context) error {
		if err := attemptCtx.Err(); err != nil {
			return err
		}
		qAvail := m.getWriteQuotaAvailBytes(req.Ino)
		commitCtx, cancel := context.WithTimeout(context.WithoutCancel(attemptCtx), 30*time.Second)
		defer cancel()
		var callErr error
		row, callErr = m.store.WriteFileAtomic(commitCtx, db.WriteFileAtomicParams{
			Ino:             int64(req.Ino),
			Fh:              int64(req.Fh),
			SessionID:       toPgSessionID(req.SessionID),
			ReqOffset:       int64(req.Offset),
			DataLen:         int64(dataLen),
			QuotaAvailBytes: qAvail,
			SliceID:         sliceID,
		})
		return callErr
	}, tx.WithDefaultRetry(errRetryTx))
	if err != nil {
		cleanupBlocks()
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, mapWriteBizError(err)
	}

	switch row.Status {
	case "ok":
	case "not_found":
		cleanupBlocks()
		return nil, ErrNotFound
	case "is_dir":
		cleanupBlocks()
		return nil, ErrIsDir
	case "invalid_arg":
		cleanupBlocks()
		return nil, ErrInvalidArgument
	case "bad_handle":
		cleanupBlocks()
		return nil, ErrBadFileHandle
	case "quota_exceeded":
		if row.RootIno > 0 {
			m.recordInodeRootIno(req.Ino, row.RootIno)
		}
		cleanupBlocks()
		return nil, ErrNoSpace
	default:
		cleanupBlocks()
		return nil, fmt.Errorf("write commit tx: unexpected status %q", row.Status)
	}

	m.recordInodeRootIno(req.Ino, row.RootIno)
	if row.RootIno > 0 {
		deltaBytes := max(int64(0), row.NewSize-row.OldSize)
		m.commitDirectQuotaDelta(context.WithoutCancel(ctx), row.RootIno, deltaBytes, 0)
	}

	m.maybeTriggerAsyncCompact(req.Ino, row.StartIndx, row.OctetLen1)
	if row.EndIndx > row.StartIndx {
		m.maybeTriggerAsyncCompact(req.Ino, row.EndIndx, row.OctetLen2)
	}

	updatedInode := db.Inode{
		Ino:           row.Ino,
		Generation:    row.Generation,
		Mode:          row.Mode,
		Size:          row.NewSize,
		Nlink:         row.Nlink,
		Uid:           row.Uid,
		Gid:           row.Gid,
		Rdev:          row.Rdev,
		Atime:         row.Atime,
		Mtime:         row.Mtime,
		Ctime:         row.Ctime,
		SymlinkTarget: row.SymlinkTarget,
		Flags:         row.Flags,
		ParentIno:     row.ParentIno,
		RootIno:       row.RootIno,
	}

	m.recordKnownFileVersion(req.SessionID, req.Ino, updatedInode.Mtime.Time, updatedInode.Size)

	return &WriteResponse{
		Size:  uint32(dataLen),
		Inode: m.newInode(updatedInode),
	}, nil
}

// writeZeroBytes 处理 0 字节写入探针。
func (m *manager) writeZeroBytes(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	var resp *WriteResponse
	err := m.execWriteTxWithRetry(ctx, func(q db.Querier) error {
		inode, err := q.GetInodeForUpdate(ctx, int64(req.Ino))
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get inode %d for update: %w", req.Ino, err)
		}

		mode := uint32(inode.Mode)
		if (mode & S_IFMT) == S_IFDIR {
			return ErrIsDir
		}
		if (mode & S_IFMT) != S_IFREG {
			return ErrInvalidArgument
		}

		handle, err := q.GetFileHandle(ctx, db.GetFileHandleParams{
			Fh:        int64(req.Fh),
			Ino:       int64(req.Ino),
			SessionID: toPgSessionID(req.SessionID),
		})
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrBadFileHandle
			}
			return fmt.Errorf("get file handle (fh=%d, ino=%d): %w", req.Fh, req.Ino, err)
		}

		accMode := uint32(handle.Flags) & O_ACCMODE
		if accMode != O_WRONLY && accMode != O_RDWR {
			return ErrBadFileHandle
		}

		resp = &WriteResponse{
			Size:  0,
			Inode: m.newInode(inode),
		}
		return nil
	})

	if err != nil {
		switch {
		case errors.Is(err, ErrInvalidArgument):
			return nil, ErrInvalidArgument
		case errors.Is(err, ErrNotFound):
			return nil, ErrNotFound
		case errors.Is(err, ErrIsDir):
			return nil, ErrIsDir
		case errors.Is(err, ErrBadFileHandle):
			return nil, ErrBadFileHandle
		default:
			return nil, fmt.Errorf("write zero bytes tx: %w", err)
		}
	}

	return resp, nil
}
