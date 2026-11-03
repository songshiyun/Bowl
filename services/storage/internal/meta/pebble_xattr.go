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

func pebbleXattrExists(store *pebble.DB, ino uint64, name []byte) (bool, error) {
	_, closer, err := store.Get(pebblekv.XattrKey(ino, name))
	if errors.Is(err, pebble.ErrNotFound) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, closer.Close()
}

func pebbleXattrNames(ctx context.Context, store *pebble.DB, ino uint64) (names [][]byte, err error) {
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

func writePebbleXattr(store *pebble.DB, inode pebblekv.Inode, name, value []byte, remove bool) (err error) {
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
	return batch.Commit(pebble.Sync)
}

func (m *manager) pebbleSetXattr(ctx context.Context, sid *uint64, params SetXattrParams) error {
	return m.withWritablePebbleBranch(ctx, sid, func(_, _ string, _ *TreeQuota, store *pebble.DB) error {
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
		return writePebbleXattr(store, inode, params.Name, params.Value, false)
	})
}

func (m *manager) pebbleGetXattr(ctx context.Context, sid *uint64, ino uint64, name []byte) ([]byte, error) {
	session, err := m.requirePebbleSession(ctx, sid)
	if err != nil {
		return nil, err
	}
	var result []byte
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
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
	err = m.withPebbleSession(ctx, session, func(store *pebble.DB) error {
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
	return m.withWritablePebbleBranch(ctx, sid, func(_, _ string, _ *TreeQuota, store *pebble.DB) error {
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
		return writePebbleXattr(store, inode, name, nil, true)
	})
}
