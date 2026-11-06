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
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		return err
	}
	if overflow {
		return batch.DeleteRange(lower, upper, nil)
	}
	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return err
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
	if err := errors.Join(iter.Error(), iter.Close()); err != nil {
		return err
	}
	if overflow {
		return batch.DeleteRange(lower, upper, nil)
	}
	for _, key := range keys {
		if err := batch.Delete(key, nil); err != nil {
			return err
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
	return m.renamePebbleNode(ctx, params.SessionID, params)
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
		return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
			parent, err := readTopologyParent(store, parentIno)
			if err != nil {
				return err
			}
			child, err := readTopologyTarget(store, parentIno, name)
			if err != nil {
				return err
			}
			if child.Flags&FlagImmutable != 0 {
				return ErrPerm
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
			if err := tx.DeleteEntry(parentIno, name); err != nil {
				return err
			}
			if err := tx.PutInode(&parent); err != nil {
				return err
			}
			removed, err := tx.SetOrphanedOrReclaim(&child, openInClient)
			if err != nil {
				return err
			}
			if removed {
				return tx.CommitReclaim(child.Ino)
			}
			return tx.CommitWithQuota(false)
		})
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
		return m.withBranchMutationTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
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
			if oldInode.Flags&FlagImmutable != 0 {
				return ErrPerm
			}
			newInode, err := readTopologyTarget(store, params.NewParentIno, params.NewName)
			hasNew := err == nil
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
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
			now, seq := tx.Now(), tx.WriteSeq()
			if params.Flags == RenameExchange {
				if err := tx.PutEntry(params.OldParentIno, params.OldName, newInode.Ino, newInode.Mode); err != nil {
					return err
				}
			} else if err := tx.DeleteEntry(params.OldParentIno, params.OldName); err != nil {
				return err
			}
			if err := tx.PutEntry(params.NewParentIno, params.NewName, oldInode.Ino, oldInode.Mode); err != nil {
				return err
			}
			if oldIsDir && crossParent {
				oldInode.ParentIno = params.NewParentIno
			}
			touchTopologyInode(&oldInode, now, seq)
			if err := tx.PutInode(&oldInode); err != nil {
				return err
			}
			var removedIno uint64
			if params.Flags == RenameExchange {
				if newIsDir && crossParent {
					newInode.ParentIno = params.OldParentIno
				}
				touchTopologyInode(&newInode, now, seq)
				if err := tx.PutInode(&newInode); err != nil {
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
					removed, err := tx.SetOrphanedOrReclaim(&newInode, params.TargetOpenInClient)
					if err != nil {
						return err
					}
					if removed {
						removedIno = newInode.Ino
					}
				}
			}
			touchCreateParent(&oldParent, now, seq)
			if err := tx.PutInode(&oldParent); err != nil {
				return err
			}
			if crossParent {
				touchCreateParent(newParent, now, seq)
				if err := tx.PutInode(newParent); err != nil {
					return err
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
		unlockInode := m.lockInodeWrite(disk, branch, params.Ino)
		defer unlockInode()
		return m.withBranchReclaimTx(disk, branch, store, blobs, quota, func(tx *BranchMutationTx) error {
			inode, err := readPebbleInode(store, params.Ino)
			if err != nil && !errors.Is(err, ErrNotFound) {
				return err
			}
			if err == nil && inode.Nlink > 0 {
				return ErrInvalidArgument
			}
			if err := tx.ReclaimOrphanInode(params.Ino); err != nil {
				return err
			}
			return tx.CommitReclaim(params.Ino)
		})
	})
}
