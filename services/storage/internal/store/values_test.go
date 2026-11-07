package store_test

import (
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestValuesCodecRoundTripAndValidation(t *testing.T) {
	// 1. BranchStat (25B)
	stat := store.BranchStat{
		LogicalBytes: 1048576,
		Inodes:       42,
		NextWriteSeq: 999,
	}
	statBytes := store.EncodeBranchStat(stat)
	require.Len(t, statBytes, store.BranchStatSize)
	require.Equal(t, 25, len(statBytes))
	gotStat, err := store.DecodeBranchStat(statBytes)
	require.NoError(t, err)
	require.Equal(t, stat, gotStat)

	_, err = store.DecodeBranchStat(statBytes[:24])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	badStat := append([]byte(nil), statBytes...)
	badStat[0] = 2
	_, err = store.DecodeBranchStat(badStat)
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 2. Entry (13B)
	entry := store.Entry{
		Ino:  12345,
		Mode: store.S_IFREG | 0o644,
	}
	entryBytes := store.EncodeEntry(entry)
	require.Len(t, entryBytes, store.EntryValueSize)
	require.Equal(t, 13, len(entryBytes))
	gotEntry, err := store.DecodeEntry(entryBytes)
	require.NoError(t, err)
	require.Equal(t, entry, gotEntry)

	_, err = store.DecodeEntry(entryBytes[:12])
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// 3. Inode (93B 定长头 + 可变长 SymlinkTarget)
	inode := store.Inode{
		Ino:           100,
		ParentIno:     1,
		Mode:          store.S_IFLNK | 0o777,
		Nlink:         1,
		UID:           1000,
		GID:           2000,
		Rdev:          7,
		Size:          11,
		Atime:         1700000000123456789,
		Mtime:         1700000001234567890,
		Ctime:         1700000002345678901,
		Flags:         store.FlagImmutable,
		Generation:    3,
		WriteSeq:      88,
		SymlinkTarget: []byte("/target/bin"),
	}
	inodeBytes, err := store.EncodeInode(inode)
	require.NoError(t, err)
	require.Equal(t, 93, store.InodeHeaderSize)
	require.Len(t, inodeBytes, store.InodeHeaderSize+len(inode.SymlinkTarget))

	gotInode, err := store.DecodeInode(inodeBytes)
	require.NoError(t, err)
	require.Equal(t, inode, gotInode)

	// 修改原编码缓冲区不影响已解码的 SymlinkTarget
	inodeBytes[len(inodeBytes)-1] = 'X'
	require.Equal(t, []byte("/target/bin"), gotInode.SymlinkTarget)

	// 截断或非法头返回 ErrInvalidValue
	_, err = store.DecodeInode(inodeBytes[:92])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeInode(inodeBytes[:len(inodeBytes)-1])
	require.ErrorIs(t, err, store.ErrInvalidValue)

	// ToAttribute 转换校验（含目录默认 Size=4096 与 Blocks 核算）
	dirInode := store.Inode{
		Ino:       1,
		ParentIno: 1,
		Mode:      store.S_IFDIR | 0o755,
		Nlink:     2,
		Atime:     100,
		Mtime:     200,
		Ctime:     300,
	}
	dirAttr := dirInode.ToAttribute()
	require.Equal(t, uint64(store.DefaultDirSize), dirAttr.Size)
	require.Equal(t, uint32(store.DefaultBlockSize), dirAttr.Blksize)
	require.Equal(t, uint64(store.DefaultDirSize/512), dirAttr.Blocks)

	// 4. BlobRef (12B)
	ref := store.BlobRef{
		ID:     4096,
		Length: 524288,
	}
	refBytes := store.EncodeBlobRef(ref)
	require.Len(t, refBytes, store.BlobRefSize)
	require.Equal(t, 12, len(refBytes))
	gotRef, err := store.DecodeBlobRef(refBytes)
	require.NoError(t, err)
	require.Equal(t, ref, gotRef)

	_, err = store.DecodeBlobRef(refBytes[:11])
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{ID: 0, Length: 10}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{ID: 1, Length: 0}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
	_, err = store.DecodeBlobRef(store.EncodeBlobRef(store.BlobRef{ID: 1, Length: store.BlobSize + 1}))
	require.ErrorIs(t, err, store.ErrInvalidValue)
}

func TestZeroCopyBatchDeferredHelpers(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	batch := db.NewIndexedBatch()
	defer func() { _ = batch.Close() }()

	stat := store.BranchStat{LogicalBytes: 4096, Inodes: 2, NextWriteSeq: 5}
	inode := store.Inode{
		Ino:       2,
		ParentIno: 1,
		Mode:      store.S_IFREG | 0o644,
		Nlink:     1,
		Size:      4096,
		Atime:     111,
		Mtime:     222,
		Ctime:     333,
	}
	entry := store.Entry{Ino: 2, Mode: inode.Mode}
	ref := store.BlobRef{ID: 7, Length: 4096}

	store.BatchSetStatDeferred(batch, stat)
	require.NoError(t, store.BatchSetInodeDeferred(batch, inode))
	store.BatchSetEntryDeferred(batch, 1, []byte("hello.txt"), entry)
	store.BatchSetXattrDeferred(batch, 2, []byte("user.tag"), []byte("v1"))
	store.BatchSetDirtyDeferred(batch, 2)
	store.BatchSetOrphanDeferred(batch, 2)
	store.BatchSetDataDeferred(batch, 2, 0, ref)

	// 验证同批写后读（Intra-Batch Read-Your-Writes）
	val, closer, err := batch.Get(store.InodeKey(2))
	require.NoError(t, err)
	gotIno, err := store.DecodeInode(val)
	require.NoError(t, closer.Close())
	require.NoError(t, err)
	require.Equal(t, inode, gotIno)

	require.NoError(t, db.Apply(batch, pebble.NoSync))

	// 验证覆盖写后调用 DeleteDeferred 不会像 SingleDelete 那样导致旧版本复活
	batch2 := db.NewIndexedBatch()
	defer func() { _ = batch2.Close() }()
	store.BatchSetXattrDeferred(batch2, 2, []byte("user.tag"), []byte("v2"))
	require.NoError(t, db.Apply(batch2, pebble.NoSync))
	require.NoError(t, db.Flush())

	batch3 := db.NewIndexedBatch()
	defer func() { _ = batch3.Close() }()
	store.BatchDeleteEntryDeferred(batch3, 1, []byte("hello.txt"))
	store.BatchDeleteXattrDeferred(batch3, 2, []byte("user.tag"))
	store.BatchDeleteDirtyDeferred(batch3, 2)
	store.BatchDeleteOrphanDeferred(batch3, 2)
	store.BatchDeleteDataDeferred(batch3, 2, 0)
	store.BatchDeleteInodeDeferred(batch3, 2)
	store.BatchDeleteDeferred(batch3, store.StatKey())
	require.NoError(t, db.Apply(batch3, pebble.NoSync))

	for _, k := range [][]byte{
		store.EntryKey(1, []byte("hello.txt")),
		store.XattrKey(2, []byte("user.tag")),
		store.DirtyKey(2),
		store.OrphanKey(2),
		store.DataKey(2, 0),
		store.InodeKey(2),
		store.StatKey(),
	} {
		_, _, err := db.Get(k)
		require.ErrorIs(t, err, pebble.ErrNotFound)
	}
}
