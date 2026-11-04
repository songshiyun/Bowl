package meta

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

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

func (m *manager) pebbleRead(ctx context.Context, session pebbleReadSession, req *ReadRequest) (result *ReadResponse, err error) {
	err = m.withPebblePointReadSession(ctx, session, func(store pebbleReader, blobs *pebblekv.BlobStore) error {
		lockBranch := session.branch
		if session.readOnly {
			lockBranch = session.commitID.String()
		}
		unlock := m.lockInodeRead(session.diskID.String(), lockBranch, req.Ino)
		defer unlock()
		inode, err := readPebbleInode(store, req.Ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
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
			ref, ok, err := readPebbleBlobRef(store, pebblekv.DataKey(req.Ino, block))
			if err != nil {
				return err
			}
			if ok && start < uint64(ref.Length) {
				n := min(length, uint64(ref.Length)-start)
				relative := pos - req.Offset
				if err := blobs.ReadAt(ref.ID, int64(start), out[relative:relative+n]); err != nil {
					return err
				}
			}
			pos += length
		}
		m.markPebbleAtimeDirty(session, inode, time.Now().UTC())
		result = &ReadResponse{Data: out}
		return nil
	})
	return result, err
}

// commitPebbleData 在同分支元数据锁内读取 BranchStat、构建批次并记账提交。
func (m *manager) commitPebbleData(disk string, store *pebble.DB, quota *TreeQuota, branch string, inode *pebblekv.Inode, oldSize uint64, enforce bool, edit func(*pebble.Batch) error) (err error) {
	unlockMeta := m.branchMetaLock(disk, branch)
	defer unlockMeta()
	stat, err := readCreateStat(store)
	if err != nil {
		return err
	}
	if stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	if inode.Size > oldSize {
		delta := inode.Size - oldSize
		if delta > math.MaxUint64-stat.LogicalBytes {
			return ErrNoSpace
		}
		stat.LogicalBytes += delta
	} else {
		stat.LogicalBytes -= min(stat.LogicalBytes, oldSize-inode.Size)
	}
	inode.WriteSeq = stat.NextWriteSeq
	stat.NextWriteSeq++
	batch := store.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	if edit != nil {
		if err := edit(batch); err != nil {
			return err
		}
	}
	if err := setCreateInode(batch, *inode); err != nil {
		return err
	}
	if err := batch.Set(pebblekv.DirtyKey(inode.Ino), []byte{1}, nil); err != nil {
		return err
	}
	if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); err != nil {
		return err
	}
	quota.mu.Lock()
	defer quota.mu.Unlock()
	if err := quota.applyLocked(branch, inode.Ino, inode.Size, true, func() error { return batch.Commit(pebble.NoSync) }, enforce); err != nil {
		return err
	}
	return nil
}

// buildWriteBlock 通过写时复制生成新的不可变块；整块为零时返回不保留实体。
func (m *manager) buildWriteBlock(disk string, store pebbleReader, blobs *pebblekv.BlobStore, ino, block uint64, start int, chunk []byte) (ref pebblekv.BlobRef, keep bool, oldID, newID uint64, err error) {
	key := pebblekv.DataKey(ino, block)
	oldRef, oldOK, err := readPebbleBlobRef(store, key)
	if err != nil {
		return pebblekv.BlobRef{}, false, 0, 0, err
	}
	if oldOK {
		oldID = oldRef.ID
	}
	// 满 1 MiB 整块写无需读取旧块，直接落新块文件。
	if start == 0 && len(chunk) == pebblekv.BlobSize {
		id, err := m.pebble.AllocateBlobID(disk)
		if err != nil {
			return pebblekv.BlobRef{}, false, 0, 0, err
		}
		if err := blobs.Write(id, chunk); err != nil {
			return pebblekv.BlobRef{}, false, 0, 0, err
		}
		return pebblekv.BlobRef{ID: id, Length: pebblekv.BlobSize}, true, oldID, id, nil
	}
	length := start + len(chunk)
	if oldOK && int(oldRef.Length) > length {
		length = int(oldRef.Length)
	}
	// 单缓冲构造新块：旧块内容直接读进新块缓冲区前缀，省掉一次整块临时分配与一次整块拷贝。
	data := make([]byte, length)
	if oldOK {
		if err := blobs.ReadAt(oldRef.ID, 0, data[:oldRef.Length]); err != nil {
			return pebblekv.BlobRef{}, false, 0, 0, err
		}
	}
	copy(data[start:], chunk)
	if allZeroBytes(data) {
		return pebblekv.BlobRef{}, false, oldID, 0, nil
	}
	// 窗口内尾部原地追加：该块在本次 Sync 前新建、尚未被任何持久化提交引用或硬链接共享，
	// 直接追加可消除长尾块反复 CoW 的 O(n²) 拷贝，且不改变块内已有字节。
	if oldOK && start == int(oldRef.Length) && length <= pebblekv.BlobSize && blobs.WindowLocal(oldRef.ID) {
		if err := blobs.AppendTail(oldRef.ID, chunk, data); err != nil {
			return pebblekv.BlobRef{}, false, 0, 0, err
		}
		return pebblekv.BlobRef{ID: oldRef.ID, Length: uint32(length)}, true, 0, 0, nil
	}
	id, err := m.pebble.AllocateBlobID(disk)
	if err != nil {
		return pebblekv.BlobRef{}, false, 0, 0, err
	}
	if err := blobs.Write(id, data); err != nil {
		return pebblekv.BlobRef{}, false, 0, 0, err
	}
	return pebblekv.BlobRef{ID: id, Length: uint32(length)}, true, oldID, id, nil
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

// checkWritableInode 校验目标节点是允许写入的普通文件。
func checkWritableInode(store pebbleReader, ino uint64, inode pebblekv.Inode) error {
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
	unlockRead := m.lockInodeRead(disk, branch, req.Ino)
	inode, err := readPebbleInode(store, req.Ino)
	if err != nil {
		unlockRead()
		return nil, false, err
	}
	if err := checkWritableInode(store, req.Ino, inode); err != nil {
		unlockRead()
		return nil, false, err
	}
	plans := make([]preparedFullBlock, 0, len(req.Data)/pebblekv.BlobSize)
	for pos := 0; pos < len(req.Data); pos += pebblekv.BlobSize {
		block := (req.Offset + uint64(pos)) / pebblekv.BlobSize
		ref, ok, err := readPebbleBlobRef(store, pebblekv.DataKey(req.Ino, block))
		if err != nil {
			unlockRead()
			return nil, false, err
		}
		plans = append(plans, preparedFullBlock{block: block, oldRef: ref, oldOK: ok})
	}
	unlockRead()

	// 锁外并行分配编号并写入不可变块文件。
	newIDs := make([]uint64, 0, len(plans))
	for i := range plans {
		id, err := m.pebble.AllocateBlobID(disk)
		if err != nil {
			return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
		}
		chunk := req.Data[i*pebblekv.BlobSize : (i+1)*pebblekv.BlobSize]
		if err := blobs.Write(id, chunk); err != nil {
			return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
		}
		plans[i].newID = id
		newIDs = append(newIDs, id)
	}

	unlockWrite := m.lockInodeWrite(disk, branch, req.Ino)
	defer unlockWrite()
	current, err := readPebbleInode(store, req.Ino)
	if err != nil {
		return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
	}
	if err := checkWritableInode(store, req.Ino, current); err != nil {
		return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
	}
	for _, plan := range plans {
		ref, ok, err := readPebbleBlobRef(store, pebblekv.DataKey(req.Ino, plan.block))
		if err != nil {
			return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
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
	oldIDs := make([]uint64, 0, len(plans))
	for _, plan := range plans {
		if plan.oldOK {
			oldIDs = append(oldIDs, plan.oldRef.ID)
		}
	}
	err = m.commitPebbleData(disk, store, quota, branch, &current, oldSize, true, func(batch *pebble.Batch) error {
		if err := deleteSecurityCapability(batch, req.Ino); err != nil {
			return err
		}
		for _, plan := range plans {
			ref := pebblekv.BlobRef{ID: plan.newID, Length: pebblekv.BlobSize}
			if err := batch.Set(pebblekv.DataKey(req.Ino, plan.block), pebblekv.EncodeBlobRef(ref), nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, false, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
	}
	if err := pebbleRemoveBlobs(blobs, oldIDs); err != nil {
		return nil, false, err
	}
	return &WriteResponse{Size: uint32(len(req.Data)), Inode: m.newPebbleInode(current)}, false, nil
}

func (m *manager) pebbleWrite(ctx context.Context, sid *uint64, req *WriteRequest) (result *WriteResponse, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		if fullBlockWrite(req) {
			response, retry, err := m.pebbleWriteFullBlocks(disk, branch, quota, store, blobs, req)
			if err != nil {
				return err
			}
			if !retry {
				result = response
				return nil
			}
		}
		unlock := m.lockInodeWrite(disk, branch, req.Ino)
		defer unlock()
		response, err := m.pebbleWriteLocked(disk, branch, quota, store, blobs, req)
		result = response
		return err
	})
	return result, err
}

type writeBlockUpdate struct {
	key   []byte
	ref   pebblekv.BlobRef
	keep  bool
	oldID uint64
	newID uint64
}

// pebbleWriteLocked 在目标 Inode 写锁内处理局部 CoW、截断与跨块写入。
func (m *manager) pebbleWriteLocked(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore, req *WriteRequest) (result *WriteResponse, err error) {
	inode, err := readPebbleInode(store, req.Ino)
	if err != nil {
		return nil, err
	}
	if err := checkWritableInode(store, req.Ino, inode); err != nil {
		return nil, err
	}
	if len(req.Data) == 0 {
		return &WriteResponse{Inode: m.newPebbleInode(inode)}, nil
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
		ref, keep, oldID, newID, err := m.buildWriteBlock(disk, store, blobs, req.Ino, block, start, chunk)
		if err != nil {
			return nil, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
		}
		updates = append(updates, writeBlockUpdate{key: pebblekv.DataKey(req.Ino, block), ref: ref, keep: keep, oldID: oldID, newID: newID})
		if oldID != 0 {
			oldIDs = append(oldIDs, oldID)
		}
		if newID != 0 {
			newIDs = append(newIDs, newID)
		}
		pos += uint64(length)
		consumed += length
	}
	err = m.commitPebbleData(disk, store, quota, branch, &inode, oldSize, true, func(batch *pebble.Batch) error {
		if err := deleteSecurityCapability(batch, req.Ino); err != nil {
			return err
		}
		for _, update := range updates {
			if update.keep {
				if err := batch.Set(update.key, pebblekv.EncodeBlobRef(update.ref), nil); err != nil {
					return err
				}
			} else if err := batch.Delete(update.key, nil); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
	}
	if err := pebbleRemoveBlobs(blobs, oldIDs); err != nil {
		return nil, err
	}
	return &WriteResponse{Size: uint32(len(req.Data)), Inode: m.newPebbleInode(inode)}, nil
}

func (m *manager) pebbleSetAttr(ctx context.Context, sid *uint64, ino uint64, input SetAttrInput) (result *Inode, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		unlock := m.lockInodeWrite(disk, branch, ino)
		defer unlock()
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleInodeLive(store, inode); err != nil {
			return err
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
		} else if input.Atime != nil {
			atime := input.Atime.UTC()
			inode.AtimeSec, inode.AtimeNsec = atime.Unix(), uint32(atime.Nanosecond())
		}
		if input.MtimeNow || input.Size != nil && input.Mtime == nil {
			inode.MtimeSec, inode.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
		} else if input.Mtime != nil {
			mtime := input.Mtime.UTC()
			inode.MtimeSec, inode.MtimeNsec = mtime.Unix(), uint32(mtime.Nanosecond())
		}
		inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
		var oldIDs, newIDs []uint64
		err = m.commitPebbleData(disk, store, quota, branch, &inode, oldSize, inode.Size > oldSize, func(batch *pebble.Batch) error {
			if input.Size != nil || input.UID != nil || input.GID != nil || input.Mode != nil {
				if err := deleteSecurityCapability(batch, ino); err != nil {
					return err
				}
			}
			if inode.Size >= oldSize {
				return nil
			}
			if inode.Size == 0 {
				ids, err := pebbleDataBlobIDs(store, ino, 0)
				if err != nil {
					return err
				}
				oldIDs = append(oldIDs, ids...)
				lower, upper := pebblekv.DataRange(ino, 0)
				return batch.DeleteRange(lower, upper, nil)
			}
			tail := (inode.Size - 1) / pebblekv.BlobSize
			length := uint32(inode.Size - tail*pebblekv.BlobSize)
			ids, err := pebbleDataBlobIDs(store, ino, tail+1)
			if err != nil {
				return err
			}
			oldIDs = append(oldIDs, ids...)
			lower, upper := pebblekv.DataRange(ino, tail+1)
			if err := batch.DeleteRange(lower, upper, nil); err != nil {
				return err
			}
			if length == pebblekv.BlobSize {
				return nil
			}
			key := pebblekv.DataKey(ino, tail)
			ref, ok, err := readPebbleBlobRef(store, key)
			if err != nil || !ok || ref.Length <= length {
				return err
			}
			data, err := readPebbleBlock(blobs, ref)
			if err != nil {
				return err
			}
			data = data[:length]
			oldIDs = append(oldIDs, ref.ID)
			if allZeroBytes(data) {
				return batch.Delete(key, nil)
			}
			id, err := m.pebble.AllocateBlobID(disk)
			if err != nil {
				return err
			}
			if err := blobs.Write(id, data); err != nil {
				return err
			}
			newIDs = append(newIDs, id)
			return batch.Set(key, pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: id, Length: length}), nil)
		})
		if err != nil {
			return errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
		}
		if err := pebbleRemoveBlobs(blobs, oldIDs); err != nil {
			return err
		}
		result = m.newPebbleInode(inode)
		return nil
	})
	return result, err
}

// zeroPebbleRange 用局部补丁和整块删除清除区间，并登记需回收的块编号。
func (m *manager) zeroPebbleRange(disk string, store *pebble.DB, blobs *pebblekv.BlobStore, batch *pebble.Batch, ino, start, end uint64, oldIDs, newIDs *[]uint64) error {
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
			*oldIDs = append(*oldIDs, ref.ID)
			if err := batch.Delete(key, nil); err != nil {
				return err
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
		*oldIDs = append(*oldIDs, ref.ID)
		if allZeroBytes(data) {
			if err := batch.Delete(key, nil); err != nil {
				return err
			}
			pos += uint64(length)
			continue
		}
		id, err := m.pebble.AllocateBlobID(disk)
		if err != nil {
			return err
		}
		if err := blobs.Write(id, data); err != nil {
			return err
		}
		*newIDs = append(*newIDs, id)
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
		unlock := m.lockInodeWrite(disk, branch, params.Ino)
		defer unlock()
		inode, err := readPebbleInode(store, params.Ino)
		if err != nil {
			return err
		}
		if err := checkPebbleInodeLive(store, inode); err != nil {
			return err
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
		var oldIDs, newIDs []uint64
		err = m.commitPebbleData(disk, store, quota, branch, &inode, oldSize, inode.Size > oldSize, func(batch *pebble.Batch) error {
			if err := deleteSecurityCapability(batch, params.Ino); err != nil {
				return err
			}
			if !zero {
				return nil
			}
			return m.zeroPebbleRange(disk, store, blobs, batch, params.Ino, params.Offset, min(end, inode.Size), &oldIDs, &newIDs)
		})
		if err != nil {
			return errors.Join(err, pebbleRemoveBlobs(blobs, newIDs))
		}
		return pebbleRemoveBlobs(blobs, oldIDs)
	})
}

func (m *manager) pebbleSync(ctx context.Context, sid *uint64, ino uint64, dir bool) error {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return err
	}
	return m.withPebbleWriteSession(ctx, session, func(store *pebble.DB, blobs *pebblekv.BlobStore) error {
		disk, branch := session.diskID.String(), session.branch
		if !session.readOnly {
			unlock := m.lockInodeWrite(disk, branch, ino)
			defer unlock()
		}
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
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
		if err := m.flushPebbleAtimeInodeLocked(disk, branch, store, ino); err != nil {
			return err
		}
		// 先取脏标记：blobs.Sync 期间的新写入会重新置脏，不能被本次屏障吞掉。
		dirty := m.pebble.BranchDirty(disk, branch)
		// 先确认块文件落盘，再同步指向它们的元数据 WAL。
		if err := blobs.Sync(); err != nil {
			return err
		}
		// 分支已干净时跳过 WAL 屏障：file fsync + dir fsync 连发时，
		// 第二次屏障没有任何未同步写入，却要付一次完整 fsync。
		if !dirty {
			return nil
		}
		if err := store.LogData(nil, pebble.Sync); err != nil {
			return err
		}
		m.pebble.MarkBranchClean(disk, branch)
		return nil
	})
}
