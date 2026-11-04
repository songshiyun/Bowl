package meta

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func readTopologyParent(store *pebble.DB, ino uint64) (pebblekv.Inode, error) {
	parent, err := readPebbleInode(store, ino)
	if err != nil {
		return parent, err
	}
	if parent.Mode&S_IFMT != S_IFDIR {
		return parent, ErrNotDir
	}
	if parent.Nlink == 0 {
		return parent, ErrNotFound
	}
	return parent, nil
}

func readTopologyTarget(store *pebble.DB, parent uint64, name []byte) (pebblekv.Inode, error) {
	value, closer, err := store.Get(pebblekv.EntryKey(parent, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return pebblekv.Inode{}, ErrNotFound
	}
	if err != nil {
		return pebblekv.Inode{}, err
	}
	entry, decodeErr := pebblekv.DecodeEntry(value)
	if err := errors.Join(decodeErr, closer.Close()); err != nil {
		return pebblekv.Inode{}, err
	}
	inode, err := readPebbleInode(store, entry.Ino)
	if err != nil {
		return inode, err
	}
	if inode.Nlink == 0 {
		return inode, ErrNotFound
	}
	return inode, nil
}

func pebbleDirEmpty(store *pebble.DB, ino uint64) (empty bool, err error) {
	lower, upper := pebblekv.EntryRange(ino)
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return false, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	empty = !iter.First()
	return empty, iter.Error()
}

func isPebbleSubdirectory(store *pebble.DB, childIno, ancestorIno uint64) (bool, error) {
	seen := make(map[uint64]struct{})
	for curr := childIno; ; {
		if curr == ancestorIno {
			return true, nil
		}
		if curr == RootIno {
			return false, nil
		}
		if curr == 0 {
			return false, ErrNotFound
		}
		if _, ok := seen[curr]; ok {
			return false, ErrInvalidArgument
		}
		seen[curr] = struct{}{}
		inode, err := readTopologyParent(store, curr)
		if err != nil {
			return false, err
		}
		curr = inode.ParentIno
	}
}

func touchTopologyInode(inode *pebblekv.Inode, now time.Time, seq uint64) {
	inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
	inode.WriteSeq = seq
}

// lockEntryInodes 先窥探目标节点，再按 ino 升序锁定父目录与目标节点，并复核未发生替换。
func (m *manager) lockEntryInodes(disk, branch string, parentIno uint64, name []byte, store *pebble.DB) (func(), error) {
	for {
		child, err := readTopologyTarget(store, parentIno, name)
		if err != nil {
			return nil, err
		}
		unlock := m.lockInodesWrite(disk, branch, parentIno, child.Ino)
		current, err := readTopologyTarget(store, parentIno, name)
		if err != nil {
			unlock()
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		if current.Ino != child.Ino {
			unlock()
			continue
		}
		return unlock, nil
	}
}

// lockRenameInodes 按 ino 升序锁定源/目标父目录与节点，避免并发目录操作死锁。
func (m *manager) lockRenameInodes(disk, branch string, params RenameParams, store *pebble.DB) (func(), error) {
	for {
		oldInode, err := readTopologyTarget(store, params.OldParentIno, params.OldName)
		if err != nil {
			return nil, err
		}
		var newIno uint64
		newInode, err := readTopologyTarget(store, params.NewParentIno, params.NewName)
		if err == nil {
			newIno = newInode.Ino
		} else if !errors.Is(err, ErrNotFound) {
			return nil, err
		}
		unlock := m.lockInodesWrite(disk, branch, params.OldParentIno, params.NewParentIno, oldInode.Ino, newIno)
		current, err := readTopologyTarget(store, params.OldParentIno, params.OldName)
		if err != nil {
			unlock()
			if errors.Is(err, ErrNotFound) {
				continue
			}
			return nil, err
		}
		if current.Ino != oldInode.Ino {
			unlock()
			continue
		}
		if newIno != 0 {
			currentNew, err := readTopologyTarget(store, params.NewParentIno, params.NewName)
			if err != nil {
				unlock()
				if errors.Is(err, ErrNotFound) {
					continue
				}
				return nil, err
			}
			if currentNew.Ino != newIno {
				unlock()
				continue
			}
		}
		return unlock, nil
	}
}

// removeTopologyInode 在目录变更批次中删除节点及其关联数据，并登记需即时回收的块编号。
func removeTopologyInode(store *pebble.DB, batch *pebble.Batch, stat *pebblekv.BranchStat, inode pebblekv.Inode, removed *[]uint64) error {
	ids, err := pebbleDataBlobIDs(store, inode.Ino, 0)
	if err != nil {
		return err
	}
	*removed = append(*removed, ids...)
	if err := batch.Delete(pebblekv.InodeKey(inode.Ino), nil); err != nil {
		return err
	}
	if err := batch.Delete(pebblekv.OrphanKey(inode.Ino), nil); err != nil {
		return err
	}
	lower, upper := pebblekv.XattrRange(inode.Ino)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	lower, upper = pebblekv.DataRange(inode.Ino, 0)
	if err := batch.DeleteRange(lower, upper, nil); err != nil {
		return err
	}
	stat.Inodes -= min(stat.Inodes, 1)
	stat.LogicalBytes -= min(stat.LogicalBytes, inode.Size)
	return nil
}

func saveTopologyInode(store *pebble.DB, batch *pebble.Batch, stat *pebblekv.BranchStat, inode pebblekv.Inode, open bool, removed *[]uint64) (bool, error) {
	if inode.Nlink > 0 {
		return false, setCreateInode(batch, inode)
	}
	if !open {
		return true, removeTopologyInode(store, batch, stat, inode, removed)
	}
	if err := setCreateInode(batch, inode); err != nil {
		return false, err
	}
	return false, batch.Set(pebblekv.OrphanKey(inode.Ino), []byte{1}, nil)
}

func finishTopologyBatch(batch *pebble.Batch, stat pebblekv.BranchStat, dirty ...uint64) error {
	for _, ino := range dirty {
		if err := batch.Set(pebblekv.DirtyKey(ino), []byte{1}, nil); err != nil {
			return err
		}
	}
	stat.NextWriteSeq++
	return batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil)
}

func commitTopologyBatch(batch *pebble.Batch, quota *TreeQuota, branch string, removed uint64) error {
	quota.mu.Lock()
	defer quota.mu.Unlock()
	if removed != 0 {
		return quota.applyLocked(branch, removed, 0, false, func() error { return batch.Commit(pebble.NoSync) }, false)
	}
	if quota.closed {
		return ErrNotFound
	}
	return batch.Commit(pebble.NoSync)
}

func (m *manager) unlinkPebbleNode(ctx context.Context, sid *uint64, parentIno uint64, name []byte) error {
	return m.removePebbleEntry(ctx, sid, parentIno, name, false)
}

func (m *manager) rmdirPebbleNode(ctx context.Context, sid *uint64, parentIno uint64, name []byte) error {
	return m.removePebbleEntry(ctx, sid, parentIno, name, true)
}

func (m *manager) removePebbleEntry(ctx context.Context, sid *uint64, parentIno uint64, name []byte, directory bool) error {
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) (err error) {
		// 先校验父节点类型，保持与 POSIX 一致：非目录优先于名称错误返回。
		if _, err := readTopologyParent(store, parentIno); err != nil {
			return err
		}
		if len(name) == 0 {
			return ErrNotFound
		}
		if isDotOrDotDot(name) {
			if directory && bytes.Equal(name, []byte("..")) {
				return ErrNotEmpty
			}
			return ErrInvalidArgument
		}
		unlockInodes, err := m.lockEntryInodes(disk, branch, parentIno, name, store)
		if err != nil {
			return err
		}
		defer unlockInodes()
		unlockMeta := m.branchMetaLock(disk, branch)
		defer unlockMeta()
		parent, err := readTopologyParent(store, parentIno)
		if err != nil {
			return err
		}
		child, err := readTopologyTarget(store, parentIno, name)
		if err != nil {
			return err
		}
		if directory {
			if child.Mode&S_IFMT != S_IFDIR {
				return ErrNotDir
			}
			empty, err := pebbleDirEmpty(store, child.Ino)
			if err != nil {
				return err
			}
			if !empty {
				return ErrNotEmpty
			}
		} else if child.Mode&S_IFMT == S_IFDIR {
			return ErrIsDir
		}
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		if stat.NextWriteSeq == math.MaxUint64 {
			return ErrNoSpace
		}
		now, seq := time.Now().UTC(), stat.NextWriteSeq
		if directory {
			child.Nlink = 0
			if parent.Nlink > 0 {
				parent.Nlink--
			}
		} else {
			child.Nlink--
		}
		touchTopologyInode(&child, now, seq)
		touchCreateParent(&parent, now, seq)
		batch := store.NewBatch()
		defer func() { err = errors.Join(err, batch.Close()) }()
		if err := batch.Delete(pebblekv.EntryKey(parentIno, name), nil); err != nil {
			return err
		}
		if err := setCreateInode(batch, parent); err != nil {
			return err
		}
		open, _ := OpenInClientFromContext(ctx)
		var removedBlobs []uint64
		removed, err := saveTopologyInode(store, batch, &stat, child, open, &removedBlobs)
		if err != nil {
			return err
		}
		if err := finishTopologyBatch(batch, stat, parentIno, child.Ino); err != nil {
			return err
		}
		var removedIno uint64
		if removed {
			removedIno = child.Ino
		}
		if err := commitTopologyBatch(batch, quota, branch, removedIno); err != nil {
			return err
		}
		return pebbleRemoveBlobs(blobs, removedBlobs)
	})
}

func (m *manager) renamePebbleNode(ctx context.Context, sid *uint64, params RenameParams) error {
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) (err error) {
		// 先校验两个父节点类型，保持与 POSIX 一致：非目录优先于名称错误返回。
		if _, err := readTopologyParent(store, params.OldParentIno); err != nil {
			return err
		}
		if params.OldParentIno != params.NewParentIno {
			if _, err := readTopologyParent(store, params.NewParentIno); err != nil {
				return err
			}
		}
		if len(params.OldName) == 0 {
			return ErrNotFound
		}
		if isDotOrDotDot(params.OldName) || isDotOrDotDot(params.NewName) {
			return ErrInvalidArgument
		}
		unlockInodes, err := m.lockRenameInodes(disk, branch, params, store)
		if err != nil {
			return err
		}
		defer unlockInodes()
		unlockMeta := m.branchMetaLock(disk, branch)
		defer unlockMeta()
		oldParent, err := readTopologyParent(store, params.OldParentIno)
		if err != nil {
			return err
		}
		newParent := &oldParent
		if params.OldParentIno != params.NewParentIno {
			parent, err := readTopologyParent(store, params.NewParentIno)
			if err != nil {
				return err
			}
			newParent = &parent
		}
		oldInode, err := readTopologyTarget(store, params.OldParentIno, params.OldName)
		if err != nil {
			return err
		}
		if params.OldParentIno == params.NewParentIno && bytes.Equal(params.OldName, params.NewName) {
			if params.Flags == RenameNoReplace {
				return ErrAlreadyExists
			}
			return nil
		}
		newInode, err := readTopologyTarget(store, params.NewParentIno, params.NewName)
		hasNew := err == nil
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if !hasNew && params.Flags == RenameExchange {
			return ErrNotFound
		}
		if hasNew && params.Flags == RenameNoReplace {
			return ErrAlreadyExists
		}
		if hasNew && oldInode.Ino == newInode.Ino {
			return nil
		}
		oldIsDir := oldInode.Mode&S_IFMT == S_IFDIR
		newIsDir := hasNew && newInode.Mode&S_IFMT == S_IFDIR
		crossParent := params.OldParentIno != params.NewParentIno
		if oldIsDir && crossParent {
			isSub, err := isPebbleSubdirectory(store, params.NewParentIno, oldInode.Ino)
			if err != nil {
				return err
			}
			if isSub {
				return ErrInvalidArgument
			}
		}
		if params.Flags == RenameExchange && newIsDir && crossParent {
			isSub, err := isPebbleSubdirectory(store, params.OldParentIno, newInode.Ino)
			if err != nil {
				return err
			}
			if isSub {
				return ErrInvalidArgument
			}
		}
		if params.Flags == RenameExchange {
			if crossParent && oldIsDir && !newIsDir && newParent.Nlink >= MaxNlink ||
				crossParent && !oldIsDir && newIsDir && oldParent.Nlink >= MaxNlink {
				return ErrTooManyLinks
			}
		} else {
			if hasNew {
				if oldIsDir && !newIsDir {
					return ErrNotDir
				}
				if !oldIsDir && newIsDir {
					return ErrIsDir
				}
				if newIsDir {
					empty, err := pebbleDirEmpty(store, newInode.Ino)
					if err != nil {
						return err
					}
					if !empty {
						return ErrNotEmpty
					}
				}
			}
			if oldIsDir && crossParent && !hasNew && newParent.Nlink >= MaxNlink {
				return ErrTooManyLinks
			}
		}
		stat, err := readCreateStat(store)
		if err != nil {
			return err
		}
		if stat.NextWriteSeq == math.MaxUint64 {
			return ErrNoSpace
		}
		now, seq := time.Now().UTC(), stat.NextWriteSeq
		batch := store.NewBatch()
		defer func() { err = errors.Join(err, batch.Close()) }()
		oldKey := pebblekv.EntryKey(params.OldParentIno, params.OldName)
		newKey := pebblekv.EntryKey(params.NewParentIno, params.NewName)
		if params.Flags == RenameExchange {
			if err := batch.Set(oldKey, pebblekv.EncodeEntry(pebblekv.Entry{Ino: newInode.Ino, Mode: newInode.Mode}), nil); err != nil {
				return err
			}
		} else if err := batch.Delete(oldKey, nil); err != nil {
			return err
		}
		if err := batch.Set(newKey, pebblekv.EncodeEntry(pebblekv.Entry{Ino: oldInode.Ino, Mode: oldInode.Mode}), nil); err != nil {
			return err
		}
		if oldIsDir && crossParent {
			oldInode.ParentIno = params.NewParentIno
		}
		touchTopologyInode(&oldInode, now, seq)
		if err := setCreateInode(batch, oldInode); err != nil {
			return err
		}
		var removedBlobs []uint64
		var removedIno uint64
		if params.Flags == RenameExchange {
			if newIsDir && crossParent {
				newInode.ParentIno = params.OldParentIno
			}
			touchTopologyInode(&newInode, now, seq)
			if err := setCreateInode(batch, newInode); err != nil {
				return err
			}
			if crossParent && oldIsDir != newIsDir {
				if oldIsDir {
					oldParent.Nlink--
					newParent.Nlink++
				} else {
					oldParent.Nlink++
					newParent.Nlink--
				}
			}
		} else {
			if oldIsDir {
				if crossParent {
					oldParent.Nlink--
					if !hasNew {
						newParent.Nlink++
					}
				} else if hasNew {
					oldParent.Nlink--
				}
			}
			if hasNew {
				if newIsDir {
					newInode.Nlink = 0
				} else {
					newInode.Nlink--
				}
				touchTopologyInode(&newInode, now, seq)
				open, _ := OpenInClientFromContext(ctx)
				removed, err := saveTopologyInode(store, batch, &stat, newInode, open, &removedBlobs)
				if err != nil {
					return err
				}
				if removed {
					removedIno = newInode.Ino
				}
			}
		}
		touchCreateParent(&oldParent, now, seq)
		if err := setCreateInode(batch, oldParent); err != nil {
			return err
		}
		if crossParent {
			touchCreateParent(newParent, now, seq)
			if err := setCreateInode(batch, *newParent); err != nil {
				return err
			}
		}
		dirty := []uint64{params.OldParentIno, params.NewParentIno, oldInode.Ino}
		if hasNew {
			dirty = append(dirty, newInode.Ino)
		}
		if err := finishTopologyBatch(batch, stat, dirty...); err != nil {
			return err
		}
		if err := commitTopologyBatch(batch, quota, branch, removedIno); err != nil {
			return err
		}
		return pebbleRemoveBlobs(blobs, removedBlobs)
	})
}

// ReclaimOrphan 在当前写端关闭最后一个句柄后回收孤儿节点。
func (m *manager) ReclaimOrphan(ctx context.Context, params ReclaimOrphanParams) error {
	if params.SessionID == 0 || params.SessionID > math.MaxInt64 || params.Ino == 0 || params.Ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	diskID, err := parseDiskID(params.DiskID)
	if err != nil {
		return err
	}
	if params.BranchName != nil {
		name := *params.BranchName
		if name == "" || name == "." || name == ".." || len(name) > 255 || strings.ContainsAny(name, "/\\") {
			return ErrInvalidArgument
		}
		for _, c := range []byte(name) {
			if c < 0x20 || c == 0x7f {
				return ErrInvalidArgument
			}
		}
	}
	return m.withWritablePebbleBranchFor(ctx, &params.SessionID, func(sessionDisk pgtype.UUID, branch string) error {
		if sessionDisk != diskID || params.BranchName != nil && *params.BranchName != branch {
			return ErrInvalidArgument
		}
		return nil
	}, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) error {
		unlockInode := m.lockInodeWrite(disk, branch, params.Ino)
		defer unlockInode()
		unlockMeta := m.branchMetaLock(disk, branch)
		defer unlockMeta()
		inode, err := readPebbleInode(store, params.Ino)
		if err != nil && !errors.Is(err, ErrNotFound) {
			return err
		}
		if err == nil && inode.Nlink > 0 {
			return ErrInvalidArgument
		}
		return reclaimBranchOrphan(store, blobs, quota, branch, params.Ino)
	})
}
