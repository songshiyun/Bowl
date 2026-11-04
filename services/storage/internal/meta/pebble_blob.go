package meta

import (
	"errors"

	"github.com/cockroachdb/pebble/v2"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// readPebbleBlobRef 读取 DataKey 中的 12 字节 BlobRef；未命中返回 ok=false。
func readPebbleBlobRef(store pebbleReader, key []byte) (ref pebblekv.BlobRef, ok bool, err error) {
	value, closer, err := store.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return pebblekv.BlobRef{}, false, nil
	}
	if err != nil {
		return pebblekv.BlobRef{}, false, err
	}
	ref, decodeErr := pebblekv.DecodeBlobRef(value)
	if err := errors.Join(decodeErr, closer.Close()); err != nil {
		return pebblekv.BlobRef{}, false, err
	}
	return ref, true, nil
}

// pebbleDataBlobIDs 返回 Inode 自 firstBlock 起所有数据键引用的块编号。
func pebbleDataBlobIDs(store *pebble.DB, ino, firstBlock uint64) (ids []uint64, err error) {
	lower, upper := pebblekv.DataRange(ino, firstBlock)
	iter, err := store.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, iter.Close()) }()
	for iter.First(); iter.Valid(); iter.Next() {
		ref, err := pebblekv.DecodeBlobRef(iter.Value())
		if err != nil {
			return nil, err
		}
		ids = append(ids, ref.ID)
	}
	return ids, iter.Error()
}

// pebbleRemoveBlobs 立即删除不再被当前分支引用的块文件。
func pebbleRemoveBlobs(blobs *pebblekv.BlobStore, ids []uint64) error {
	var errs []error
	for _, id := range ids {
		errs = append(errs, blobs.Remove(id))
	}
	return errors.Join(errs...)
}

// readPebbleBlob 读取完整块内容。
func readPebbleBlob(blobs *pebblekv.BlobStore, ref pebblekv.BlobRef) ([]byte, error) {
	return blobs.Read(ref.ID, int(ref.Length))
}

// pebbleBlobEqual 比较两个块引用的逻辑内容；引用完全相同时直接短路。
// 块内容不足 length 的部分按零值参与比较。
func pebbleBlobEqual(a *pebblekv.BlobStore, aRef pebblekv.BlobRef, b *pebblekv.BlobStore, bRef pebblekv.BlobRef) (bool, error) {
	if aRef.ID == bRef.ID && aRef.Length == bRef.Length {
		return true, nil
	}
	aData, err := readPebbleBlob(a, aRef)
	if err != nil {
		return false, err
	}
	bData, err := readPebbleBlob(b, bRef)
	if err != nil {
		return false, err
	}
	size := max(len(aData), len(bData))
	for i := 0; i < size; i++ {
		var av, bv byte
		if i < len(aData) {
			av = aData[i]
		}
		if i < len(bData) {
			bv = bData[i]
		}
		if av != bv {
			return false, nil
		}
	}
	return true, nil
}
