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
	if closeErr := errors.Join(decodeErr, closer.Close()); closeErr != nil {
		return pebblekv.Inode{}, closeErr
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
		unlock := m.locks.LockInodesWrite(disk, branch, parentIno, child.Ino)
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
func (m *manager) lockRenameInodes(disk, branch string, params *RenameParams, store *pebble.DB) (func(), error) {
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
		unlock := m.locks.LockInodesWrite(disk, branch, params.OldParentIno, params.NewParentIno, oldInode.Ino, newIno)
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
			currentNew, newErr := readTopologyTarget(store, params.NewParentIno, params.NewName)
			if newErr != nil {
				unlock()
				if errors.Is(newErr, ErrNotFound) {
					continue
				}
				return nil, newErr
			}
			if currentNew.Ino != newIno {
				unlock()
				continue
			}
		}
		return unlock, nil
	}
}

// pointDeleteLimit 是逐键点删的键数上限；超过上限才回退范围墓碑。
// 范围墓碑会常驻 memtable，使后续每次读都要重建 range-del 迭代器；
// 对空文件、目录、符号链接和小文件改走点删可消除该退化。
const pointDeleteLimit = 64

// deletePebbleRangePointwise 删除 [lower, upper) 内的全部键：
// 0 键时零写入，不超过 pointDeleteLimit 时逐键点删，超过上限回退范围墓碑。
func deletePebbleRangePointwise(store *pebble.DB, batch *pebble.Batch, lower, upper []byte) error {
	keys := make([][]byte, 0, pointDeleteLimit)
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	overflow := false
	for iter.First(); iter.Valid(); iter.Next() {
		if len(keys) == pointDeleteLimit {
			overflow = true
			break
		}
		// batch 会持有该键，必须拷贝迭代器复用的字节切片。
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	if joinErr := errors.Join(iter.Error(), iter.Close()); joinErr != nil {
		return joinErr
	}
	if overflow {
		return batch.DeleteRange(lower, upper, nil)
	}
	for _, key := range keys {
		if delErr := batch.Delete(key, nil); delErr != nil {
			return delErr
		}
	}
	return nil
}

// deletePebbleDataKeys 删除 Inode 的全部数据键，并登记需即时回收的块编号。
func deletePebbleDataKeys(store *pebble.DB, batch *pebble.Batch, ino uint64, removed *[]uint64) error {
	lower, upper := pebblekv.DataRange(ino, 0)
	keys := make([][]byte, 0, pointDeleteLimit)
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return err
	}
	overflow := false
	for iter.First(); iter.Valid(); iter.Next() {
		ref, decodeErr := pebblekv.DecodeBlobRef(iter.Value())
		if decodeErr != nil {
			_ = iter.Close()
			return decodeErr
		}
		*removed = append(*removed, ref.ID)
		if len(keys) == pointDeleteLimit {
			overflow = true
			continue
		}
		keys = append(keys, append([]byte(nil), iter.Key()...))
	}
	if joinErr := errors.Join(iter.Error(), iter.Close()); joinErr != nil {
		return joinErr
	}
	if overflow {
		return batch.DeleteRange(lower, upper, nil)
	}
	for _, key := range keys {
		if delErr := batch.Delete(key, nil); delErr != nil {
			return delErr
		}
	}
	return nil
}

func (m *manager) Unlink(ctx context.Context, sid *uint64, parentIno uint64, name []byte, openInClient bool) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}
	return m.unlinkPebbleNode(ctx, sid, parentIno, name, openInClient)
}

func (m *manager) Rmdir(ctx context.Context, sid *uint64, parentIno uint64, name []byte) error {
	if parentIno == 0 || parentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateEntryNameAllowEmpty(name); err != nil {
		return err
	}
	return m.rmdirPebbleNode(ctx, sid, parentIno, name)
}

//nolint:gocritic // hugeParam: Manager 接口公开契约由 internal/server 按值调用。
func (m *manager) Rename(ctx context.Context, params RenameParams) error {
	if params.OldParentIno == 0 || params.OldParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.NewParentIno == 0 || params.NewParentIno > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if (params.Flags &^ (RenameNoReplace | RenameExchange | RenameWhiteout)) != 0 {
		return ErrInvalidArgument
	}
	if (params.Flags & (RenameNoReplace | RenameExchange)) == (RenameNoReplace | RenameExchange) {
		return ErrInvalidArgument
	}
	if (params.Flags & RenameWhiteout) != 0 {
		return ErrNotSupported
	}
	if err := validateEntryNameAllowEmpty(params.OldName); err != nil {
		return err
	}
	if err := validateEntryName(params.NewName); err != nil {
		return err
	}
	return m.renamePebbleNode(ctx, params.SessionID, &params)
}

func (m *manager) unlinkPebbleNode(ctx context.Context, sid *uint64, parentIno uint64, name []byte, openInClient bool) error {
	return m.removePebbleEntry(ctx, sid, parentIno, name, false, openInClient)
}

func (m *manager) rmdirPebbleNode(ctx context.Context, sid *uint64, parentIno uint64, name []byte) error {
	return m.removePebbleEntry(ctx, sid, parentIno, name, true, false)
}

func (m *manager) removePebbleEntry(ctx context.Context, sid *uint64, parentIno uint64, name []byte, directory, openInClient bool) error {
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) (err error) {
		// 先校验父节点类型，保持与 POSIX 一致：非目录优先于名称错误返回。
		if _, parentErr := readTopologyParent(store, parentIno); parentErr != nil {
			return parentErr
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
		return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
			parent, parentErr := readTopologyParent(store, parentIno)
			if parentErr != nil {
				return parentErr
			}
			child, childErr := readTopologyTarget(store, parentIno, name)
			if childErr != nil {
				return childErr
			}
			if child.Flags&FlagImmutable != 0 {
				return ErrPerm
			}
			if directory {
				if child.Mode&S_IFMT != S_IFDIR {
					return ErrNotDir
				}
				empty, emptyErr := pebbleDirEmpty(store, child.Ino)
				if emptyErr != nil {
					return emptyErr
				}
				if !empty {
					return ErrNotEmpty
				}
			} else if child.Mode&S_IFMT == S_IFDIR {
				return ErrIsDir
			}
			now, seq := tx.Now(), tx.WriteSeq()
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
			if delErr := tx.DeleteEntry(parentIno, name); delErr != nil {
				return delErr
			}
			if putErr := tx.PutInode(&parent); putErr != nil {
				return putErr
			}
			removed, reclaimErr := tx.SetOrphanedOrReclaim(&child, openInClient)
			if reclaimErr != nil {
				return reclaimErr
			}
			if removed {
				return tx.CommitReclaim(child.Ino)
			}
			return tx.CommitWithQuota(false)
		})
	})
}

func (m *manager) renamePebbleNode(ctx context.Context, sid *uint64, params *RenameParams) error {
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, quota *TreeQuota, store *pebble.DB, blobs *pebblekv.BlobStore) (err error) {
		// 先校验两个父节点类型，保持与 POSIX 一致：非目录优先于名称错误返回。
		if _, parentErr := readTopologyParent(store, params.OldParentIno); parentErr != nil {
			return parentErr
		}
		if params.OldParentIno != params.NewParentIno {
			if _, parentErr := readTopologyParent(store, params.NewParentIno); parentErr != nil {
				return parentErr
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
		return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
			oldParent, oldParentErr := readTopologyParent(store, params.OldParentIno)
			if oldParentErr != nil {
				return oldParentErr
			}
			newParent := &oldParent
			if params.OldParentIno != params.NewParentIno {
				parent, newParentErr := readTopologyParent(store, params.NewParentIno)
				if newParentErr != nil {
					return newParentErr
				}
				newParent = &parent
			}
			oldInode, oldInodeErr := readTopologyTarget(store, params.OldParentIno, params.OldName)
			if oldInodeErr != nil {
				return oldInodeErr
			}
			if params.OldParentIno == params.NewParentIno && bytes.Equal(params.OldName, params.NewName) {
				if params.Flags == RenameNoReplace {
					return ErrAlreadyExists
				}
				return nil
			}
			if oldInode.Flags&FlagImmutable != 0 {
				return ErrPerm
			}
			newInode, newInodeErr := readTopologyTarget(store, params.NewParentIno, params.NewName)
			hasNew := newInodeErr == nil
			if newInodeErr != nil && !errors.Is(newInodeErr, ErrNotFound) {
				return newInodeErr
			}
			if hasNew && newInode.Flags&FlagImmutable != 0 {
				return ErrPerm
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
				isSub, subErr := isPebbleSubdirectory(store, params.NewParentIno, oldInode.Ino)
				if subErr != nil {
					return subErr
				}
				if isSub {
					return ErrInvalidArgument
				}
			}
			if params.Flags == RenameExchange && newIsDir && crossParent {
				isSub, subErr := isPebbleSubdirectory(store, params.OldParentIno, newInode.Ino)
				if subErr != nil {
					return subErr
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
						empty, emptyErr := pebbleDirEmpty(store, newInode.Ino)
						if emptyErr != nil {
							return emptyErr
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
			now, seq := tx.Now(), tx.WriteSeq()
			if params.Flags == RenameExchange {
				if putErr := tx.PutEntry(params.OldParentIno, params.OldName, newInode.Ino, newInode.Mode); putErr != nil {
					return putErr
				}
			} else if delErr := tx.DeleteEntry(params.OldParentIno, params.OldName); delErr != nil {
				return delErr
			}
			if putErr := tx.PutEntry(params.NewParentIno, params.NewName, oldInode.Ino, oldInode.Mode); putErr != nil {
				return putErr
			}
			if oldIsDir && crossParent {
				oldInode.ParentIno = params.NewParentIno
			}
			touchTopologyInode(&oldInode, now, seq)
			if putErr := tx.PutInode(&oldInode); putErr != nil {
				return putErr
			}
			var removedIno uint64
			if params.Flags == RenameExchange {
				if newIsDir && crossParent {
					newInode.ParentIno = params.OldParentIno
				}
				touchTopologyInode(&newInode, now, seq)
				if putErr := tx.PutInode(&newInode); putErr != nil {
					return putErr
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
					removed, reclaimErr := tx.SetOrphanedOrReclaim(&newInode, params.TargetOpenInClient)
					if reclaimErr != nil {
						return reclaimErr
					}
					if removed {
						removedIno = newInode.Ino
					}
				}
			}
			touchCreateParent(&oldParent, now, seq)
			if putErr := tx.PutInode(&oldParent); putErr != nil {
				return putErr
			}
			if crossParent {
				touchCreateParent(newParent, now, seq)
				if putErr := tx.PutInode(newParent); putErr != nil {
					return putErr
				}
			}
			if removedIno != 0 {
				return tx.CommitReclaim(removedIno)
			}
			return tx.CommitWithQuota(false)
		})
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
		unlockInode := m.locks.LockInodeWrite(disk, branch, params.Ino)
		defer unlockInode()
		return m.withBranchReclaimTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
			inode, readErr := readPebbleInode(store, params.Ino)
			if readErr != nil && !errors.Is(readErr, ErrNotFound) {
				return readErr
			}
			if readErr == nil && inode.Nlink > 0 {
				return ErrInvalidArgument
			}
			if reclaimErr := tx.ReclaimOrphanInode(params.Ino); reclaimErr != nil {
				return reclaimErr
			}
			return tx.CommitReclaim(params.Ino)
		})
	})
}
