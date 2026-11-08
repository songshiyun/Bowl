package meta

import (
	"bytes"
	"context"
	"errors"
	"math"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// securityCapabilityXattr 是 Linux killpriv 语义要求清除的扩展属性名。
var securityCapabilityXattr = []byte("security.capability")

func validateXattrName(name []byte) error {
	if len(name) == 0 {
		return ErrInvalidArgument
	}
	if len(name) > MaxXattrNameBytes {
		return ErrRange
	}
	if bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}

// isAllowedUserXattrFileType 判定文件模式是否允许访问 user.* 命名空间扩展属性。
// 仅允许常规文件 (S_IFREG)、目录 (S_IFDIR) 与套接字 (S_IFSOCK)。
func isAllowedUserXattrFileType(mode uint32) bool {
	fileType := mode & S_IFMT
	return fileType == S_IFREG || fileType == S_IFDIR || fileType == S_IFSOCK
}

// deleteSecurityCapability 在同一 Pebble 批次内原子删除 security.capability，
// 使单次 Write / SetAttr / Fallocate RPC 即满足 Linux killpriv 语义。
func deleteSecurityCapability(batch *pebble.Batch, ino uint64) error {
	return batch.Delete(pebblekv.XattrKey(ino, securityCapabilityXattr), nil)
}

func pebbleXattrExists(store pebbleReader, ino uint64, name []byte) (bool, error) {
	_, closer, err := store.Get(pebblekv.XattrKey(ino, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

func pebbleXattrNames(ctx context.Context, store pebbleReader, ino uint64) (names [][]byte, err error) {
	lower, upper := pebblekv.XattrRange(ino)
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	names = [][]byte{}
	for iter.First(); iter.Valid(); iter.Next() {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		_, name, decodeErr := pebblekv.DecodeXattrKey(iter.Key())
		if decodeErr != nil {
			return nil, decodeErr
		}
		names = append(names, name)
	}
	return names, iter.Error()
}

func (m *manager) writePebbleXattr(disk, branch string, store *pebble.DB, inode *pebblekv.Inode, name, value []byte, remove bool) error {
	return m.withBranchMutationTx(disk, branch, store, nil, nil, func(tx *BranchMutationTx) error {
		now := tx.Now()
		inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
		if err := tx.PutXattr(inode.Ino, name, value, remove); err != nil {
			return err
		}
		if err := tx.PutInode(inode); err != nil {
			return err
		}
		return tx.CommitWithoutQuota()
	})
}

func (m *manager) SetXattr(ctx context.Context, params SetXattrParams) error {
	if params.Ino == 0 || params.Ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if params.Flags&^(XattrCreate|XattrReplace) != 0 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(params.Name); err != nil {
		return err
	}
	if len(params.Value) > MaxXattrValueBytes {
		return ErrRange
	}
	return m.withWritablePebbleBranch(ctx, params.SessionID, func(disk, branch string, _ *TreeQuota, store *pebble.DB, _ *pebblekv.BlobStore) error {
		unlock := m.locks.LockInodeWrite(disk, branch, params.Ino)
		defer unlock()
		inode, err := readPebbleInode(store, params.Ino)
		if err != nil {
			return err
		}
		if liveErr := checkPebbleInodeLive(store, &inode); liveErr != nil {
			return liveErr
		}
		if bytes.HasPrefix(params.Name, []byte("user.")) && !isAllowedUserXattrFileType(inode.Mode) {
			return ErrPerm
		}
		exists, err := pebbleXattrExists(store, params.Ino, params.Name)
		if err != nil {
			return err
		}
		if exists {
			if params.Flags&XattrCreate != 0 {
				return ErrAlreadyExists
			}
		} else {
			if params.Flags&XattrReplace != 0 {
				return ErrNoData
			}
			names, namesErr := pebbleXattrNames(ctx, store, params.Ino)
			if namesErr != nil {
				return namesErr
			}
			total := len(params.Name) + 1
			for _, name := range names {
				total += len(name) + 1
			}
			if total > MaxXattrListBytes {
				return ErrNoSpace
			}
		}
		return m.writePebbleXattr(disk, branch, store, &inode, params.Name, params.Value, false)
	})
}

func (m *manager) GetXattr(ctx context.Context, sid *uint64, ino uint64, name []byte) ([]byte, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return nil, err
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result []byte
	err = m.withPebblePointReadSession(ctx, &session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
		inode, readErr := readPebbleInode(store, ino)
		if readErr != nil {
			return readErr
		}
		if liveErr := checkPebbleSessionInodeLive(&session, store, &inode); liveErr != nil {
			return liveErr
		}
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(inode.Mode) {
			return ErrNoData
		}
		value, closer, getErr := store.Get(pebblekv.XattrKey(ino, name))
		if errors.Is(getErr, pebble.ErrNotFound) {
			return ErrNoData
		}
		if getErr != nil {
			return getErr
		}
		result = append([]byte{}, value...)
		return closer.Close()
	})
	return result, err
}

func (m *manager) ListXattr(ctx context.Context, sid *uint64, ino uint64) ([][]byte, error) {
	if ino == 0 || ino > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var names [][]byte
	err = m.withPebbleReadSession(ctx, &session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
		inode, readErr := readPebbleInode(store, ino)
		if readErr != nil {
			return readErr
		}
		if liveErr := checkPebbleSessionInodeLive(&session, store, &inode); liveErr != nil {
			return liveErr
		}
		var namesErr error
		names, namesErr = pebbleXattrNames(ctx, store, ino)
		return namesErr
	})
	return names, err
}

func (m *manager) RemoveXattr(ctx context.Context, sid *uint64, ino uint64, name []byte) error {
	if ino == 0 || ino > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := validateXattrName(name); err != nil {
		return err
	}
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, _ *TreeQuota, store *pebble.DB, _ *pebblekv.BlobStore) error {
		unlock := m.locks.LockInodeWrite(disk, branch, ino)
		defer unlock()
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if liveErr := checkPebbleInodeLive(store, &inode); liveErr != nil {
			return liveErr
		}
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(inode.Mode) {
			return ErrPerm
		}
		exists, err := pebbleXattrExists(store, ino, name)
		if err != nil {
			return err
		}
		if !exists {
			return ErrNoData
		}
		return m.writePebbleXattr(disk, branch, store, &inode, name, nil, true)
	})
}
