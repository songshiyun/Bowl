package meta

import (
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func (m *manager) pebbleRead(ctx context.Context, session pebbleReadSession, req *ReadRequest) (result *ReadResponse, err error) {
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
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
			value, closer, err := store.Get(pebblekv.DataKey(req.Ino, block))
			if err != nil && !errors.Is(err, pebble.ErrNotFound) {
				return err
			}
			if err == nil {
				data, decodeErr := pebblekv.DecodeBlob(value)
				if err := errors.Join(decodeErr, closer.Close()); err != nil {
					return err
				}
				if start < uint64(len(data)) {
					copy(out[pos-req.Offset:], data[start:min(start+length, uint64(len(data)))])
				}
			}
			pos += length
		}
		result = &ReadResponse{Data: out}
		return nil
	})
	return result, err
}

// commitPebbleData 在配额锁内原子提交数据、节点与分支统计。
func commitPebbleData(store *pebble.DB, quota *TreeQuota, branch string, inode *pebblekv.Inode, stat pebblekv.BranchStat, oldSize uint64, enforce bool, edit func(*pebble.Batch) error) (err error) {
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
	quota.mu.Lock()
	defer quota.mu.Unlock()
	if quota.closed {
		return ErrNotFound
	}
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
	return quota.applyLocked(branch, inode.Ino, inode.Size, true, func() error { return batch.Commit(pebble.Sync) }, enforce)
}

func (m *manager) pebbleWrite(ctx context.Context, sid *uint64, req *WriteRequest) (result *WriteResponse, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(_ string, branch string, quota *TreeQuota, store *pebble.DB) error {
		inode, err := readPebbleInode(store, req.Ino)
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
			return ErrInvalidArgument
		}
		if inode.Flags&FlagImmutable != 0 {
			return ErrPerm
		}
		if len(req.Data) == 0 {
			result = &WriteResponse{Inode: m.newPebbleInode(inode)}
			return nil
		}
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		oldSize := inode.Size
		inode.Size = max(inode.Size, req.Offset+uint64(len(req.Data)))
		inode.Blocks = (inode.Size + 511) / 512
		inode.Mode &^= S_ISUID | S_ISGID
		now := time.Now().UTC()
		inode.MtimeSec, inode.MtimeNsec = now.Unix(), uint32(now.Nanosecond())
		inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
		err = commitPebbleData(store, quota, branch, &inode, stat, oldSize, true, func(batch *pebble.Batch) error {
			for pos, consumed := req.Offset, 0; consumed < len(req.Data); {
				block := pos / pebblekv.BlobSize
				start := uint32(pos % pebblekv.BlobSize)
				length := min(len(req.Data)-consumed, pebblekv.BlobSize-int(start))
				chunk := req.Data[consumed : consumed+length]
				var frame []byte
				var err error
				if start == 0 && length == pebblekv.BlobSize {
					frame, err = pebblekv.EncodeBase(chunk)
					if err == nil {
						err = batch.Set(pebblekv.DataKey(req.Ino, block), frame, nil)
					}
				} else {
					frame, err = pebblekv.EncodePatches(pebblekv.WritePatch(start, chunk))
					if err == nil {
						err = batch.Merge(pebblekv.DataKey(req.Ino, block), frame, nil)
					}
				}
				if err != nil {
					return err
				}
				pos += uint64(length)
				consumed += length
			}
			return nil
		})
		if err != nil {
			return err
		}
		result = &WriteResponse{Size: uint32(len(req.Data)), Inode: m.newPebbleInode(inode)}
		return nil
	})
	return result, err
}

func (m *manager) pebbleSetAttr(ctx context.Context, sid *uint64, ino uint64, input SetAttrInput) (result *Inode, err error) {
	err = m.withWritablePebbleBranch(ctx, sid, func(_ string, branch string, quota *TreeQuota, store *pebble.DB) error {
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
		} else if fileType == S_IFREG && (input.UID != nil || input.GID != nil || input.Size != nil) {
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
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		err = commitPebbleData(store, quota, branch, &inode, stat, oldSize, inode.Size > oldSize, func(batch *pebble.Batch) error {
			if inode.Size >= oldSize {
				return nil
			}
			if inode.Size == 0 {
				lower, upper := pebblekv.DataRange(ino, 0)
				return batch.DeleteRange(lower, upper, nil)
			}
			tail := (inode.Size - 1) / pebblekv.BlobSize
			length := uint32(inode.Size - tail*pebblekv.BlobSize)
			lower, upper := pebblekv.DataRange(ino, tail+1)
			if err := batch.DeleteRange(lower, upper, nil); err != nil {
				return err
			}
			if length < pebblekv.BlobSize {
				patch, err := pebblekv.EncodePatches(pebblekv.TruncatePatch(length))
				if err != nil {
					return err
				}
				return batch.Merge(pebblekv.DataKey(ino, tail), patch, nil)
			}
			return nil
		})
		if err != nil {
			return err
		}
		result = m.newPebbleInode(inode)
		return nil
	})
	return result, err
}

// zeroPebbleRange 用局部补丁和整块删除清除区间。
func zeroPebbleRange(batch *pebble.Batch, ino, start, end uint64) error {
	for pos := start; pos < end; {
		block := pos / pebblekv.BlobSize
		inBlock := uint32(pos % pebblekv.BlobSize)
		length := uint32(min(end-pos, uint64(pebblekv.BlobSize)-uint64(inBlock)))
		if inBlock == 0 && length == pebblekv.BlobSize {
			// 大范围空洞使用范围删除，避免按文件最大大小逐块枚举。
			fullBlocks := (end - pos) / pebblekv.BlobSize
			if fullBlocks > 1024 {
				if err := batch.DeleteRange(pebblekv.DataKey(ino, block), pebblekv.DataKey(ino, block+fullBlocks), nil); err != nil {
					return err
				}
				pos += fullBlocks * pebblekv.BlobSize
				continue
			}
			if err := batch.Delete(pebblekv.DataKey(ino, block), nil); err != nil {
				return err
			}
		} else {
			patch, err := pebblekv.EncodePatches(pebblekv.ZeroPatch(inBlock, length))
			if err != nil {
				return err
			}
			if err := batch.Merge(pebblekv.DataKey(ino, block), patch, nil); err != nil {
				return err
			}
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
	return m.withWritablePebbleBranch(ctx, sid, func(_ string, branch string, quota *TreeQuota, store *pebble.DB) error {
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
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		return commitPebbleData(store, quota, branch, &inode, stat, oldSize, inode.Size > oldSize, func(batch *pebble.Batch) error {
			if !zero {
				return nil
			}
			return zeroPebbleRange(batch, params.Ino, params.Offset, min(end, inode.Size))
		})
	})
}

func (m *manager) pebbleSync(ctx context.Context, sid *uint64, ino uint64, dir bool) error {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return err
	}
	return m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
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
		return store.LogData(nil, pebble.Sync)
	})
}
