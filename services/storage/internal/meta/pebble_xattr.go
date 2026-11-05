package meta

import (
	"bytes"
	"context"
	"errors"
	"math"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// securityCapabilityXattr 是 Linux killpriv 语义要求清除的扩展属性名。
var securityCapabilityXattr = []byte("security.capability")

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
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		_, name, err := pebblekv.DecodeXattrKey(iter.Key())
		if err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	return names, iter.Error()
}

func (m *manager) writePebbleXattr(disk, branch string, store *pebble.DB, inode pebblekv.Inode, name, value []byte, remove bool) (err error) {
	unlockMeta := m.branchMetaLock(disk, branch)
	defer unlockMeta()
	stat, err := readCreateStat(store)
	if err != nil {
		return err
	}
	if stat.NextWriteSeq == math.MaxUint64 {
		return ErrNoSpace
	}
	now := time.Now().UTC()
	inode.CtimeSec, inode.CtimeNsec = now.Unix(), uint32(now.Nanosecond())
	inode.WriteSeq = stat.NextWriteSeq
	stat.NextWriteSeq++

	batch := store.NewBatch()
	defer func() { err = errors.Join(err, batch.Close()) }()
	key := pebblekv.XattrKey(inode.Ino, name)
	if remove {
		err = batch.Delete(key, nil)
	} else {
		if value == nil {
			value = []byte{}
		}
		err = batch.Set(key, value, nil)
	}
	if err != nil {
		return err
	}
	if err := setCreateInode(batch, inode); err != nil {
		return err
	}
	if err := batch.Set(pebblekv.DirtyKey(inode.Ino), []byte{1}, nil); err != nil {
		return err
	}
	if err := batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), nil); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (m *manager) pebbleSetXattr(ctx context.Context, sid *uint64, params SetXattrParams) error {
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, _ *TreeQuota, store *pebble.DB, _ *pebblekv.BlobStore) error {
		unlock := m.lockInodeWrite(disk, branch, params.Ino)
		defer unlock()
		inode, err := readPebbleInode(store, params.Ino)
		if err != nil {
			return err
		}
		if err := checkPebbleInodeLive(store, inode); err != nil {
			return err
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
			names, err := pebbleXattrNames(ctx, store, params.Ino)
			if err != nil {
				return err
			}
			total := len(params.Name) + 1
			for _, name := range names {
				total += len(name) + 1
			}
			if total > MaxXattrListBytes {
				return ErrNoSpace
			}
		}
		return m.writePebbleXattr(disk, branch, store, inode, params.Name, params.Value, false)
	})
}

func (m *manager) pebbleGetXattr(ctx context.Context, sid *uint64, ino uint64, name []byte) ([]byte, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result []byte
	err = m.withPebblePointReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
		}
		if bytes.HasPrefix(name, []byte("user.")) && !isAllowedUserXattrFileType(inode.Mode) {
			return ErrNoData
		}
		value, closer, err := store.Get(pebblekv.XattrKey(ino, name))
		if errors.Is(err, pebble.ErrNotFound) {
			return ErrNoData
		}
		if err != nil {
			return err
		}
		result = append([]byte{}, value...)
		return closer.Close()
	})
	return result, err
}

func (m *manager) pebbleListXattr(ctx context.Context, sid *uint64, ino uint64) ([][]byte, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var names [][]byte
	err = m.withPebbleReadSession(ctx, session, func(store pebbleReader, _ *pebblekv.BlobStore) error {
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleSessionInodeLive(session, store, inode); err != nil {
			return err
		}
		names, err = pebbleXattrNames(ctx, store, ino)
		return err
	})
	return names, err
}

func (m *manager) pebbleRemoveXattr(ctx context.Context, sid *uint64, ino uint64, name []byte) error {
	return m.withWritablePebbleBranch(ctx, sid, func(disk, branch string, _ *TreeQuota, store *pebble.DB, _ *pebblekv.BlobStore) error {
		unlock := m.lockInodeWrite(disk, branch, ino)
		defer unlock()
		inode, err := readPebbleInode(store, ino)
		if err != nil {
			return err
		}
		if err := checkPebbleInodeLive(store, inode); err != nil {
			return err
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
		return m.writePebbleXattr(disk, branch, store, inode, name, nil, true)
	})
}
