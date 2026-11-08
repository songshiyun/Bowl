package meta

import (
	"context"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

//nolint:gocritic // hugeParam: Manager 接口公开契约由 internal/server 按值调用。
func (m *manager) SetAttr(ctx context.Context, ino uint64, input SetAttrInput) (*Inode, error) {
	// 事务前入参边界防御
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// 至少包含一项有效待更新属性
	if input.Mode == nil && input.UID == nil && input.GID == nil && input.Size == nil &&
		input.Atime == nil && input.Mtime == nil && !input.AtimeNow && !input.MtimeNow &&
		!input.CtimeNow && input.Flags == nil {
		return nil, ErrInvalidArgument
	}

	// 可选文件句柄与客户端会话标识静态边界防御
	if input.Fh != nil && (*input.Fh == 0 || *input.Fh > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}
	if err := validateOptionalSessionID(input.SessionID); err != nil {
		return nil, err
	}

	// 时间戳策略互斥防御
	if (input.Atime != nil && input.AtimeNow) || (input.Mtime != nil && input.MtimeNow) {
		return nil, ErrInvalidArgument
	}

	// Mode 范围边界防御（不超过 16 位 0o177777 = 65535）
	if input.Mode != nil && *input.Mode > 0o177777 {
		return nil, ErrInvalidArgument
	}

	// UID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.UID != nil && *input.UID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// GID 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.GID != nil && *input.GID > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}

	// Size 范围边界防御（防止转换为 PostgreSQL BIGINT 符号位溢出）
	if input.Size != nil && *input.Size > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}

	// Flags 范围边界防御（防止转换为 PostgreSQL INTEGER 符号位溢出）
	if input.Flags != nil && *input.Flags > math.MaxInt32 {
		return nil, ErrInvalidArgument
	}
	return m.pebbleSetAttr(ctx, input.SessionID, ino, &input)
}

func (m *manager) Fallocate(ctx context.Context, params FallocateParams) error {
	// 1. 入参边界与整型溢出防御
	if params.Ino == 0 || params.Ino > math.MaxInt64 ||
		params.Fh > math.MaxInt64 ||
		params.Length == 0 || params.Length > math.MaxInt64 ||
		params.Offset > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Offset > uint64(math.MaxInt64)-params.Length {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	return m.pebbleFallocate(ctx, params.SessionID, params)
}

func (m *manager) Read(ctx context.Context, req *ReadRequest) (*ReadResponse, error) {
	if req == nil {
		return nil, ErrInvalidArgument
	}
	if req.Ino == 0 || req.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Fh > math.MaxInt64 {
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
	view, err := m.requirePebbleSession(ctx, req.SessionID)
	if err != nil {
		return nil, err
	}
	return m.pebbleRead(ctx, &view, req)
}

func (m *manager) Write(ctx context.Context, req *WriteRequest) (*WriteResponse, error) {
	if req == nil {
		return nil, ErrInvalidArgument
	}
	if req.Ino == 0 || req.Ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if req.Fh > math.MaxInt64 {
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
	return m.pebbleWrite(ctx, req.SessionID, req)
}

func (m *manager) Fsync(ctx context.Context, params FsyncParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	return m.pebbleSync(ctx, params.SessionID, params.Ino, false)
}

func (m *manager) FsyncDir(ctx context.Context, params FsyncDirParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 || params.Fh > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateOptionalSessionID(params.SessionID); err != nil {
		return err
	}
	return m.pebbleSync(ctx, params.SessionID, params.Ino, true)
}

// readPebbleBlock 读取块引用指向的完整块内容。
func readPebbleBlock(blobs *pebblekv.BlobStore, ref pebblekv.BlobRef) ([]byte, error) {
	return blobs.Read(ref.ID, int(ref.Length))
}

// allZeroBytes 报告整块是否可视为空洞。
func allZeroBytes(data []byte) bool {
	for _, value := range data {
		if value != 0 {
			return false
		}
	}
	return true
}

func (m *manager) pebbleRead(ctx context.Context, session *pebbleReadSession, req *ReadRequest) (result *ReadResponse, err error) {
	err = m.withPebblePointReadSession(ctx, session, func(store pebbleReader, blobs *pebblekv.BlobStore) error {
		lockBranch := session.branch
		if session.readOnly {
			lockBranch = session.commitID.String()
		}
		unlock := m.locks.LockInodeRead(session.diskID.String(), lockBranch, req.Ino)
		defer unlock()
		inode, readErr := readPebbleInode(store, req.Ino)
		if readErr != nil {
			return readErr
		}
		if liveErr := checkPebbleSessionInodeLive(session, store, &inode); liveErr != nil {
			return liveErr
		}
		switch inode.Mode & S_IFMT {
		case S_IFDIR:
			return ErrIsDir
		case S_IFREG:
		default:
			return ErrInvalidArgument
		}
		if req.Size == 0 || req.Offset >= inode.Size {
			result = &ReadResponse{Data: []byte{}}
			return nil
		}
		readLen := min(uint64(req.Size), inode.Size-req.Offset)
		out := make([]byte, readLen)
		for pos, end := req.Offset, req.Offset+readLen; pos < end; {
			block := pos / pebblekv.BlobSize
			start := pos % pebblekv.BlobSize
			length := min(end-pos, uint64(pebblekv.BlobSize)-start)
			ref, ok, refErr := readPebbleBlobRef(store, pebblekv.DataKey(req.Ino, block))
			if refErr != nil {
				return refErr
			}
			if ok && start < uint64(ref.Length) {
				n := min(length, uint64(ref.Length)-start)
				relative := pos - req.Offset
				if readAtErr := blobs.ReadAt(ref.ID, int64(start), out[relative:relative+n]); readAtErr != nil {
					return readAtErr
				}
			}
			pos += length
		}
		m.markPebbleAtimeDirty(session, &inode, time.Now().UTC())
		result = &ReadResponse{Data: out}
		return nil
	})
	return result, err
}

// commitPebbleData 在同分支工作区变更事务内读取 BranchStat、构建批次并记账提交。
// beforeCommit 在配额与序列校验全部通过、批次真正提交前执行，用于把不可回滚的块文件
// 原地补丁放到“已确定不会 ErrNoSpace”之后，保证失败写入不触碰旧块内容。
func (m *manager) commitPebbleData(disk string, store *pebble.DB, blobs *pebblekv.BlobStore, quota *TreeQuota, branch string, inode *pebblekv.Inode, oldSize uint64, enforce bool, edit func(*BranchMutationTx) error, beforeCommit func() error) error {
	return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
		if err := tx.AdjustLogicalBytes(oldSize, inode.Size); err != nil {
			return err
		}
		if beforeCommit != nil {
			tx.SetBeforeCommit(beforeCommit)
		}
		if edit != nil {
			if err := edit(tx); err != nil {
				return err
			}
		}
		if err := tx.PutInode(inode); err != nil {
			return err
		}
		return tx.CommitApplyChange(inode.Ino, inode.Size, true, enforce)
	})
}

// buildWriteBlock 通过写时复制生成新的不可变块；整块为零时返回不保留实体。
// 窗口内脏块（本次 Sync 前新建、尚未被任何持久化提交引用或硬链接共享）不分配新编号，
// 而是返回原地补丁描述，由提交路径在配额校验通过后统一落盘。
func (m *manager) buildWriteBlock(disk string, store pebbleReader, blobs *pebblekv.BlobStore, ino, block uint64, start int, chunk []byte) (writeBlockUpdate, error) {
	key := pebblekv.DataKey(ino, block)
	oldRef, oldOK, err := readPebbleBlobRef(store, key)
	if err != nil {
		return writeBlockUpdate{}, err
	}
	update := writeBlockUpdate{key: key}
	if oldOK {
		update.oldID = oldRef.ID
	}
	// 满 1 MiB 整块写无需读取旧块，直接落新块文件。
	if start == 0 && len(chunk) == pebblekv.BlobSize {
		id, allocErr := m.pebble.AllocateBlobID(disk)
		if allocErr != nil {
			return writeBlockUpdate{}, allocErr
		}
		if writeErr := blobs.WriteForInode(ino, id, chunk); writeErr != nil {
			return writeBlockUpdate{}, writeErr
		}
		update.ref = pebblekv.BlobRef{ID: id, Length: pebblekv.BlobSize}
		update.keep = true
		update.newID = id
		return update, nil
	}
	length := start + len(chunk)
	if oldOK && int(oldRef.Length) > length {
		length = int(oldRef.Length)
	}
	// 窗口内无空洞补丁快路径：start <= 旧长度时（尾部追加或块内原地覆写），
	// 无需读取旧块构造整块缓冲区。旧实现每次 4 KiB 写都要分配并拷贝整个逻辑块
	// （1 MiB 块写满时累计 O(n²) 拷贝），这里只记录待写入的 chunk。
	// 热缓存由 Patch 在提交后剔除，后续读回落到块文件（OS Page Cache）。
	if oldOK && length <= pebblekv.BlobSize && start <= int(oldRef.Length) && blobs.WindowLocal(oldRef.ID) {
		if oldRef.Length == 0 && start == 0 && allZeroBytes(chunk) {
			update.keep = false
			return update, nil
		}
		update.ref = pebblekv.BlobRef{ID: oldRef.ID, Length: uint32(length)}
		update.keep = true
		update.sameRef = uint32(length) == oldRef.Length
		update.oldID = 0
		update.newID = 0
		update.patchID = oldRef.ID
		update.patchOffset = int64(start)
		update.patchBytes = chunk
		update.patchSize = int64(length)
		// patchFull 留空：提交路径调用 Patch 而不是 AppendTail。
		return update, nil
	}
	// 单缓冲构造新块：旧块内容直接读进新块缓冲区前缀，省掉一次整块临时分配与一次整块拷贝。
	data := make([]byte, length)
	if oldOK {
		if readErr := blobs.ReadAt(oldRef.ID, 0, data[:oldRef.Length]); readErr != nil {
			return writeBlockUpdate{}, readErr
		}
	}
	copy(data[start:], chunk)
	if allZeroBytes(data) {
		update.keep = false
		return update, nil
	}
	// 窗口内块原地改写：该块在本次 Sync 前新建、尚未被任何持久化提交引用或硬链接共享，
	// 直接按逻辑偏移覆写可消除长尾块反复 CoW 的 O(n²) 拷贝，也消除 1 MiB 整块重写的写放大。
	// 补丁推迟到配额校验之后执行，ErrNoSpace 时旧块内容保持 100% 不变。
	if oldOK && length <= pebblekv.BlobSize && blobs.WindowLocal(oldRef.ID) {
		patchOffset := int64(start)
		patchBytes := chunk
		if start > int(oldRef.Length) {
			// 跨越旧块尾部的空洞写入：从旧长度开始补零，直到本次写入末端。
			patchOffset = int64(oldRef.Length)
			patchBytes = data[oldRef.Length : start+len(chunk)]
		}
		update.ref = pebblekv.BlobRef{ID: oldRef.ID, Length: uint32(length)}
		update.keep = true
		update.sameRef = uint32(length) == oldRef.Length
		update.oldID = 0
		update.newID = 0
		update.patchID = oldRef.ID
		update.patchOffset = patchOffset
		update.patchBytes = patchBytes
		update.patchFull = data
		return update, nil
	}
	id, err := m.pebble.AllocateBlobID(disk)
	if err != nil {
		return writeBlockUpdate{}, err
	}
	if err := blobs.WriteForInode(ino, id, data); err != nil {
		return writeBlockUpdate{}, err
	}
	update.ref = pebblekv.BlobRef{ID: id, Length: uint32(length)}
	update.keep = true
	update.newID = id
	return update, nil
}

// fullBlockWrite 报告请求是否只包含按 1 MiB 边界对齐的完整块。
func fullBlockWrite(req *WriteRequest) bool {
	if len(req.Data) == 0 {
		return false
	}
	if req.Offset%pebblekv.BlobSize != 0 || len(req.Data)%pebblekv.BlobSize != 0 {
		return false
	}
	return true
}

// checkWritableInode 校验目标节点是允许写入的普通文件，并落实 immutable/append 写语义。
func checkWritableInode(store pebbleReader, _ uint64, inode *pebblekv.Inode, offset uint64, dataLen int) error {
	if err := checkPebbleInodeLive(store, inode); err != nil {
		return err
	}
	if inode.Mode&S_IFMT == S_IFDIR {
		return ErrIsDir
	}
	if inode.Mode&S_IFMT != S_IFREG {
		return ErrInvalidArgument
	}
	if inode.Flags&FlagImmutable != 0 {
		return ErrPerm
	}
	if dataLen > 0 && inode.Flags&FlagAppend != 0 && offset != inode.Size {
		return ErrPerm
	}
	return nil
}

type preparedFullBlock struct {
	block  uint64
	oldRef pebblekv.BlobRef
	oldOK  bool
	newID  uint64
}

// pebbleWriteFullBlocks 在 Inode 锁外完成整块 .blob 分配与磁盘写入，仅在提交元数据时持锁。
// 返回 retry=true 表示旧块引用在准备期间被并发改写，需回退到持锁路径重做。
func (m *manager) pebbleWriteFullBlocks(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore, req *WriteRequest) (result *WriteResponse, retry bool, err error) {
	unlockRead := m.locks.LockInodeRead(disk, branch, req.Ino)
	inode, err := readPebbleInode(store, req.Ino)
	if err != nil {
		unlockRead()
		return nil, false, err
	}
	if writeErr := checkWritableInode(store, req.Ino, &inode, req.Offset, len(req.Data)); writeErr != nil {
		unlockRead()
		return nil, false, writeErr
	}
	plans := make([]preparedFullBlock, 0, len(req.Data)/pebblekv.BlobSize)
	for pos := 0; pos < len(req.Data); pos += pebblekv.BlobSize {
		block := (req.Offset + uint64(pos)) / pebblekv.BlobSize
		ref, ok, refErr := readPebbleBlobRef(store, pebblekv.DataKey(req.Ino, block))
		if refErr != nil {
			unlockRead()
			return nil, false, refErr
		}
		plans = append(plans, preparedFullBlock{block: block, oldRef: ref, oldOK: ok})
	}
	unlockRead()

	// 锁外并行分配编号并写入不可变块文件。
	newIDs := make([]uint64, 0, len(plans))
	for i := range plans {
		id, allocErr := m.pebble.AllocateBlobID(disk)
		if allocErr != nil {
			return nil, false, errors.Join(allocErr, pebbleRemoveBlobs(blobs, newIDs))
		}
		chunk := req.Data[i*pebblekv.BlobSize : (i+1)*pebblekv.BlobSize]
		if writeErr := blobs.WriteForInode(req.Ino, id, chunk); writeErr != nil {
			return nil, false, errors.Join(writeErr, pebbleRemoveBlobs(blobs, newIDs))
		}
		plans[i].newID = id
		newIDs = append(newIDs, id)
	}

	unlockWrite := m.locks.LockInodeWrite(disk, branch, req.Ino)
	defer unlockWrite()
	current, err := readPebbleInode(store, req.Ino)
	if err != nil {
		return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
	}
	if writeErr := checkWritableInode(store, req.Ino, &current, req.Offset, len(req.Data)); writeErr != nil {
		return nil, false, errors.Join(writeErr, pebbleRemoveBlobs(blobs, newIDs))
	}
	for _, plan := range plans {
		ref, ok, refErr := readPebbleBlobRef(store, pebblekv.DataKey(req.Ino, plan.block))
		if refErr != nil {
			return nil, false, errors.Join(refErr, pebbleRemoveBlobs(blobs, newIDs))
		}
		if ok != plan.oldOK || ok && ref != plan.oldRef {
			// 乐观读取后旧块已被并发替换，丢弃临时块并在锁内重做。
			return nil, true, pebbleRemoveBlobs(blobs, newIDs)
		}
	}
	oldSize := current.Size
	current.Size = max(current.Size, req.Offset+uint64(len(req.Data)))
	current.Blocks = (current.Size + 511) / 512
	current.Mode &^= S_ISUID | S_ISGID
	now := time.Now().UTC()
	current.MtimeSec, current.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
	current.CtimeSec, current.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
	err = m.commitPebbleData(disk, store, blobs, quota, branch, &current, oldSize, true, func(tx *BranchMutationTx) error {
		tx.TrackNewBlobs(newIDs...)
		if capErr := deleteSecurityCapability(tx.Batch(), req.Ino); capErr != nil {
			return capErr
		}
		for _, plan := range plans {
			if plan.oldOK {
				tx.TrackRemovedBlobs(plan.oldRef.ID)
			}
			ref := pebblekv.BlobRef{ID: plan.newID, Length: pebblekv.BlobSize}
			if setErr := tx.Batch().Set(pebblekv.DataKey(req.Ino, plan.block), pebblekv.EncodeBlobRef(ref), nil); setErr != nil {
				return setErr
			}
		}
		return nil
	}, nil)
	if err != nil {
		return nil, false, err
	}
	return &WriteResponse{Size: uint32(len(req.Data)), Inode: m.newPebbleInode(&current)}, false, nil
}

func (m *manager) pebbleWrite(ctx context.Context, sid *uint64, req *WriteRequest) (result *WriteResponse, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		if fullBlockWrite(req) {
			response, retry, fullErr := m.pebbleWriteFullBlocks(disk, branch, quota, store, blobs, req)
			if fullErr != nil {
				return fullErr
			}
			if !retry {
				result = response
				return nil
			}
		}
		unlock := m.locks.LockInodeWrite(disk, branch, req.Ino)
		defer unlock()
		response, writeErr := m.pebbleWriteLocked(disk, branch, quota, store, blobs, req)
		result = response
		return writeErr
	})
	return result, err
}

type writeBlockUpdate struct {
	key     []byte
	ref     pebblekv.BlobRef
	keep    bool
	sameRef bool
	oldID   uint64
	newID   uint64
	// 窗口内脏块的原地补丁：在配额校验通过后、批次提交前执行。
	patchID     uint64
	patchOffset int64
	patchBytes  []byte
	patchFull   []byte
	patchSize   int64
}

// pebbleWriteLocked 在目标 Inode 写锁内处理局部 CoW、截断与跨块写入。
func (m *manager) pebbleWriteLocked(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore, req *WriteRequest) (result *WriteResponse, err error) {
	inode, err := readPebbleInode(store, req.Ino)
	if err != nil {
		return nil, err
	}
	if writeErr := checkWritableInode(store, req.Ino, &inode, req.Offset, len(req.Data)); writeErr != nil {
		return nil, writeErr
	}
	if len(req.Data) == 0 {
		return &WriteResponse{Inode: m.newPebbleInode(&inode)}, nil
	}
	oldSize := inode.Size
	inode.Size = max(inode.Size, req.Offset+uint64(len(req.Data)))
	inode.Blocks = (inode.Size + 511) / 512
	inode.Mode &^= S_ISUID | S_ISGID
	now := time.Now().UTC()
	inode.MtimeSec, inode.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
	inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
	var oldIDs, newIDs []uint64
	var updates []writeBlockUpdate
	// 局部 CoW 的旧块读取与新块落盘在 Inode 锁内完成，但不占用分支元数据锁。
	for pos, consumed := req.Offset, 0; consumed < len(req.Data); {
		block := pos / pebblekv.BlobSize
		start := int(pos % pebblekv.BlobSize)
		length := min(len(req.Data)-consumed, pebblekv.BlobSize-start)
		chunk := req.Data[consumed : consumed+length]
		update, buildErr := m.buildWriteBlock(disk, store, blobs, req.Ino, block, start, chunk)
		if buildErr != nil {
			return nil, errors.Join(buildErr, pebbleRemoveBlobs(blobs, newIDs))
		}
		updates = append(updates, update)
		if update.oldID != 0 {
			oldIDs = append(oldIDs, update.oldID)
		}
		if update.newID != 0 {
			newIDs = append(newIDs, update.newID)
		}
		pos += uint64(length)
		consumed += length
	}
	err = m.commitPebbleData(disk, store, blobs, quota, branch, &inode, oldSize, true, func(tx *BranchMutationTx) error {
		tx.TrackNewBlobs(newIDs...)
		tx.TrackRemovedBlobs(oldIDs...)
		batch := tx.Batch()
		if capErr := deleteSecurityCapability(batch, req.Ino); capErr != nil {
			return capErr
		}
		for i := range updates {
			update := &updates[i]
			if update.keep {
				// 原地补丁不改变 BlobRef 时无需重写 DataKey，避免一次无意义的 LSM 写入。
				if update.sameRef {
					continue
				}
				if setErr := batch.Set(update.key, pebblekv.EncodeBlobRef(update.ref), nil); setErr != nil {
					return setErr
				}
			} else if delErr := batch.Delete(update.key, nil); delErr != nil {
				return delErr
			}
		}
		return nil
	}, func() error {
		// 配额与序列校验已通过，此时才允许原地覆写窗口内脏块。
		for i := range updates {
			update := &updates[i]
			if update.patchID == 0 {
				continue
			}
			if update.patchFull == nil {
				if patchErr := blobs.Patch(update.patchID, update.patchOffset, update.patchBytes, update.patchSize); patchErr != nil {
					return patchErr
				}
				continue
			}
			if appendErr := blobs.AppendTail(update.patchID, update.patchOffset, update.patchBytes, update.patchFull); appendErr != nil {
				return appendErr
			}
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &WriteResponse{Size: uint32(len(req.Data)), Inode: m.newPebbleInode(&inode)}, nil
}

func (m *manager) pebbleSetAttr(ctx context.Context, sid *uint64, ino uint64, input *SetAttrInput) (result *Inode, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		unlock := m.locks.LockInodeWrite(disk, branch, ino)
		defer unlock()
		inode, readErr := readPebbleInode(store, ino)
		if readErr != nil {
			return readErr
		}
		if liveErr := checkPebbleInodeLive(store, &inode); liveErr != nil {
			return liveErr
		}
		fileType := inode.Mode & S_IFMT
		if input.Size != nil {
			if fileType == S_IFDIR {
				return ErrIsDir
			}
			if fileType != S_IFREG {
				return ErrInvalidArgument
			}
		}
		if inode.Flags&FlagImmutable != 0 && (input.Flags == nil || *input.Flags&FlagImmutable != 0) {
			return ErrPerm
		}
		if inode.Flags&FlagAppend != 0 && input.Size != nil {
			return ErrPerm
		}
		if input.Mode != nil {
			inputType := *input.Mode & S_IFMT
			if fileType == S_IFLNK || inputType != 0 && inputType != fileType {
				return ErrInvalidArgument
			}
			inode.Mode = fileType | (*input.Mode & PermMask)
		} else if fileType != S_IFDIR && (input.UID != nil || input.GID != nil || input.Size != nil) {
			// 客户端声明 CAP_HANDLE_KILLPRIV 后内核不再代为清位，由文件系统负责：
			// Linux 仅对非目录节点在 chown/truncate 时清除 S_ISUID/S_ISGID，
			// 目录保留 setgid（与 pjdfstest 对 Linux 的预期一致）。
			inode.Mode &^= S_ISUID | S_ISGID
		}
		if input.UID != nil {
			inode.UID = *input.UID
		}
		if input.GID != nil {
			inode.GID = *input.GID
		}
		if input.Flags != nil {
			inode.Flags = *input.Flags
		}
		oldSize := inode.Size
		if input.Size != nil {
			inode.Size = *input.Size
			inode.Blocks = (inode.Size + 511) / 512
		}
		now := time.Now().UTC()
		if input.AtimeNow {
			inode.AtimeSec, inode.AtimeNsec = now.Unix(), uint32(now.Nanosecond())
			m.atimes.DiscardInode(disk, branch, ino)
		} else if input.Atime != nil {
			atime := input.Atime.UTC()
			inode.AtimeSec, inode.AtimeNsec = atime.Unix(), uint32(atime.Nanosecond())
			m.atimes.DiscardInode(disk, branch, ino)
		}
		if input.MtimeNow || input.Size != nil && input.Mtime == nil {
			inode.MtimeSec, inode.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
		} else if input.Mtime != nil {
			mtime := input.Mtime.UTC()
			inode.MtimeSec, inode.MtimeNsec = mtime.Unix(), uint32(mtime.Nanosecond())
		}
		inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
		err = m.commitPebbleData(disk, store, blobs, quota, branch, &inode, oldSize, inode.Size > oldSize, func(tx *BranchMutationTx) error {
			batch := tx.Batch()
			if input.Size != nil || input.UID != nil || input.GID != nil || input.Mode != nil {
				if capErr := deleteSecurityCapability(batch, ino); capErr != nil {
					return capErr
				}
			}
			if inode.Size >= oldSize {
				return nil
			}
			if inode.Size == 0 {
				ids, idsErr := pebbleDataBlobIDs(store, ino, 0)
				if idsErr != nil {
					return idsErr
				}
				tx.TrackRemovedBlobs(ids...)
				lower, upper := pebblekv.DataRange(ino, 0)
				return batch.DeleteRange(lower, upper, nil)
			}
			tail := (inode.Size - 1) / pebblekv.BlobSize
			length := uint32(inode.Size - tail*pebblekv.BlobSize)
			ids, idsErr := pebbleDataBlobIDs(store, ino, tail+1)
			if idsErr != nil {
				return idsErr
			}
			tx.TrackRemovedBlobs(ids...)
			lower, upper := pebblekv.DataRange(ino, tail+1)
			if delErr := batch.DeleteRange(lower, upper, nil); delErr != nil {
				return delErr
			}
			if length == pebblekv.BlobSize {
				return nil
			}
			key := pebblekv.DataKey(ino, tail)
			ref, ok, refErr := readPebbleBlobRef(store, key)
			if refErr != nil || !ok || ref.Length <= length {
				return refErr
			}
			data, blockErr := readPebbleBlock(blobs, ref)
			if blockErr != nil {
				return blockErr
			}
			data = data[:length]
			tx.TrackRemovedBlobs(ref.ID)
			if allZeroBytes(data) {
				return batch.Delete(key, nil)
			}
			id, allocErr := m.pebble.AllocateBlobID(disk)
			if allocErr != nil {
				return allocErr
			}
			if writeErr := blobs.WriteForInode(ino, id, data); writeErr != nil {
				return writeErr
			}
			tx.TrackNewBlobs(id)
			return batch.Set(key, pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: id, Length: length}), nil)
		}, nil)
		if err != nil {
			return err
		}
		result = m.newPebbleInode(&inode)
		return nil
	})
	return result, err
}

// zeroPebbleRange 用局部补丁和整块删除清除区间，并登记需回收的块编号。
func (m *manager) zeroPebbleRange(disk string, store *pebble.DB, blobs *pebblekv.BlobStore, tx *BranchMutationTx, ino, start, end uint64) error {
	batch := tx.Batch()
	for pos := start; pos < end; {
		block := pos / pebblekv.BlobSize
		inBlock := int(pos % pebblekv.BlobSize)
		length := int(min(end-pos, uint64(pebblekv.BlobSize)-uint64(inBlock)))
		key := pebblekv.DataKey(ino, block)
		ref, ok, err := readPebbleBlobRef(store, key)
		if err != nil {
			return err
		}
		if !ok {
			pos += uint64(length)
			continue
		}
		if inBlock == 0 && length == pebblekv.BlobSize {
			tx.TrackRemovedBlobs(ref.ID)
			if delErr := batch.Delete(key, nil); delErr != nil {
				return delErr
			}
			pos += uint64(length)
			continue
		}
		data, err := readPebbleBlock(blobs, ref)
		if err != nil {
			return err
		}
		changed := false
		if inBlock < len(data) {
			clearEnd := min(inBlock+length, len(data))
			for _, value := range data[inBlock:clearEnd] {
				if value != 0 {
					changed = true
					break
				}
			}
			if changed {
				clear(data[inBlock:clearEnd])
			}
		}
		if !changed {
			pos += uint64(length)
			continue
		}
		tx.TrackRemovedBlobs(ref.ID)
		if allZeroBytes(data) {
			if delErr := batch.Delete(key, nil); delErr != nil {
				return delErr
			}
			pos += uint64(length)
			continue
		}
		id, err := m.pebble.AllocateBlobID(disk)
		if err != nil {
			return err
		}
		if err := blobs.WriteForInode(ino, id, data); err != nil {
			return err
		}
		tx.TrackNewBlobs(id)
		if err := batch.Set(key, pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: id, Length: uint32(len(data))}), nil); err != nil {
			return err
		}
		pos += uint64(length)
	}
	return nil
}

func (m *manager) pebbleFallocate(ctx context.Context, sid *uint64, params FallocateParams) error {
	if params.Mode == FallocFlPunchHole {
		return ErrInvalidArgument
	}
	switch params.Mode {
	case 0, FallocFlKeepSize, FallocFlPunchHole | FallocFlKeepSize, FallocFlZeroRange, FallocFlZeroRange | FallocFlKeepSize:
	default:
		return ErrNotSupported
	}
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		unlock := m.locks.LockInodeWrite(disk, branch, params.Ino)
		defer unlock()
		inode, err := readPebbleInode(store, params.Ino)
		if err != nil {
			return err
		}
		if liveErr := checkPebbleInodeLive(store, &inode); liveErr != nil {
			return liveErr
		}
		if inode.Mode&S_IFMT == S_IFDIR {
			return ErrIsDir
		}
		if inode.Mode&S_IFMT != S_IFREG {
			return ErrNotSupported
		}
		if inode.Flags&(FlagImmutable|FlagAppend) != 0 {
			return ErrPerm
		}
		oldSize := inode.Size
		end := params.Offset + params.Length
		if params.Mode&FallocFlKeepSize == 0 {
			inode.Size = max(oldSize, end)
			inode.Blocks = (inode.Size + 511) / 512
		}
		zero := params.Mode&FallocFlPunchHole != 0 || params.Mode&FallocFlZeroRange != 0
		now := time.Now().UTC()
		if zero {
			inode.Mode &^= S_ISUID | S_ISGID
		}
		if zero || inode.Size > oldSize {
			inode.MtimeSec, inode.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
		}
		inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
		return m.commitPebbleData(disk, store, blobs, quota, branch, &inode, oldSize, inode.Size > oldSize, func(tx *BranchMutationTx) error {
			if capErr := deleteSecurityCapability(tx.Batch(), params.Ino); capErr != nil {
				return capErr
			}
			if !zero {
				return nil
			}
			return m.zeroPebbleRange(disk, store, blobs, tx, params.Ino, params.Offset, min(end, inode.Size))
		}, nil)
	})
}

func (m *manager) pebbleSync(ctx context.Context, sid *uint64, ino uint64, dir bool) error {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return err
	}
	return m.withPebbleWriteSession(ctx, &session, func(store *pebble.DB, blobs *pebblekv.BlobStore) error {
		disk, branch := session.diskID.String(), session.branch
		if !session.readOnly {
			unlock := m.locks.LockInodeWrite(disk, branch, ino)
			defer unlock()
		}
		inode, readErr := readPebbleInode(store, ino)
		if readErr != nil {
			return readErr
		}
		if liveErr := checkPebbleSessionInodeLive(&session, store, &inode); liveErr != nil {
			return liveErr
		}
		if dir && inode.Mode&S_IFMT != S_IFDIR {
			return ErrNotDir
		}
		if !dir && inode.Mode&S_IFMT == S_IFDIR {
			return ErrIsDir
		}
		if session.readOnly {
			return nil
		}
		// 显式持久化屏障顺带合并内存中的脏访问时间。
		if flushErr := m.gate.FlushInodeAtimeLocked(disk, branch, store, ino); flushErr != nil {
			return flushErr
		}
		// 先取脏标记：块同步期间的新写入会重新置脏，不能被本次屏障吞掉。
		dirty := m.pebble.BranchDirty(disk, branch)
		if dir {
			// 目录项与属性只存在 Pebble WAL，不涉及 .blob 文件，FsyncDir 无需块文件屏障。
			if !dirty {
				return nil
			}
			if logErr := store.LogData(nil, pebble.Sync); logErr != nil {
				return logErr
			}
			// 仍有其它 Inode 的脏块时保持分支脏标记，交给后续 fsync 或后台兜底刷盘。
			if !blobs.HasDirty() {
				m.pebble.MarkBranchClean(disk, branch)
			}
			return nil
		}
		if !dirty {
			return blobs.SyncInode(ino)
		}
		if !blobs.HasDirty() {
			// 没有脏块时无需并发，直接在当前协程完成 WAL 屏障。
			if logErr := store.LogData(nil, pebble.Sync); logErr != nil {
				return logErr
			}
			m.pebble.MarkBranchClean(disk, branch)
			return nil
		}
		// 块文件屏障与元数据 WAL 屏障彼此独立，并发执行以少付一次 NVMe 缓存刷新时延。
		var walErr error
		var wg sync.WaitGroup
		wg.Go(func() {
			walErr = store.LogData(nil, pebble.Sync)
		})
		blobErr := blobs.SyncInode(ino)
		wg.Wait()
		if syncErr := errors.Join(blobErr, walErr); syncErr != nil {
			return syncErr
		}
		if !blobs.HasDirty() {
			m.pebble.MarkBranchClean(disk, branch)
		}
		return nil
	})
}
