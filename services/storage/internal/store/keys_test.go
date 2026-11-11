package store_test

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestKeyEncodingAndSplitKey(t *testing.T) {
	// 1. StatKey: "sys:stat" (8B), SplitKey -> len(key) (8)
	statKey := store.StatKey()
	require.Equal(t, []byte("sys:stat"), statKey)
	require.Len(t, statKey, store.StatKeyLen)
	require.Equal(t, store.StatKeyLen, store.SplitKey(statKey))

	// 2. InodeKey: "inode:" + 8B BigEndian (14B), SplitKey -> len(key) (14)
	inodeKey := store.InodeKey(0x0102030405060708)
	require.Equal(t, []byte{'i', 'n', 'o', 'd', 'e', ':', 1, 2, 3, 4, 5, 6, 7, 8}, inodeKey)
	require.Len(t, inodeKey, store.InodeKeyLen)
	require.Equal(t, store.InodeKeyLen, store.SplitKey(inodeKey))
	gotIno, err := store.DecodeInodeKey(inodeKey)
	require.NoError(t, err)
	require.Equal(t, uint64(0x0102030405060708), gotIno)
	require.Negative(t, bytes.Compare(store.InodeKey(1), store.InodeKey(256)))

	_, err = store.DecodeInodeKey([]byte("inode:short"))
	require.ErrorIs(t, err, store.ErrInvalidKey)

	// 3. EntryKey: "entry:" + 8B BigEndian + ":" + name (15B + len(name)), SplitKey -> 15
	name := []byte{'f', 'o', 'o', ':', 0x00, 0xff}
	entryKey := store.EntryKey(42, name)
	require.Len(t, entryKey, store.EntryPrefixLen+len(name))
	require.Equal(t, store.EntryPrefixLen, store.SplitKey(entryKey))
	require.Equal(t, store.EntryPrefix(42), entryKey[:store.EntryPrefixLen])
	require.Equal(t, store.EntryPrefixLen, store.SplitKey(store.EntryPrefix(42)))
	parentIno, gotName, err := store.DecodeEntryKey(entryKey)
	require.NoError(t, err)
	require.Equal(t, uint64(42), parentIno)
	require.Equal(t, name, gotName)

	_, _, err = store.DecodeEntryKey([]byte("entry:bad"))
	require.ErrorIs(t, err, store.ErrInvalidKey)

	// 4. XattrKey: "xattr:" + 8B BigEndian + ":" + name (15B + len(name)), SplitKey -> 15
	xattrName := []byte("user.mime_type")
	xattrKey := store.XattrKey(99, xattrName)
	require.Len(t, xattrKey, store.XattrPrefixLen+len(xattrName))
	require.Equal(t, store.XattrPrefixLen, store.SplitKey(xattrKey))
	require.Equal(t, store.XattrPrefix(99), xattrKey[:store.XattrPrefixLen])
	require.Equal(t, store.XattrPrefixLen, store.SplitKey(store.XattrPrefix(99)))
	xIno, gotXName, err := store.DecodeXattrKey(xattrKey)
	require.NoError(t, err)
	require.Equal(t, uint64(99), xIno)
	require.Equal(t, xattrName, gotXName)

	_, _, err = store.DecodeXattrKey([]byte("xattr:bad"))
	require.ErrorIs(t, err, store.ErrInvalidKey)

	// 5. OrphanKey: "orphan:" + 8B BigEndian (15B), SplitKey -> len(key) (15)
	require.Equal(t, []byte("orphan:"), store.OrphanPrefix())
	orphanKey := store.OrphanKey(88)
	require.Len(t, orphanKey, store.OrphanKeyLen)
	require.Equal(t, len(orphanKey), store.SplitKey(orphanKey))
	oIno, err := store.DecodeOrphanKey(orphanKey)
	require.NoError(t, err)
	require.Equal(t, uint64(88), oIno)

	_, err = store.DecodeOrphanKey([]byte("orphan:bad"))
	require.ErrorIs(t, err, store.ErrInvalidKey)

	// 6. DataKey: "data:" + 8B BigEndian(ino) + ":" + 8B BigEndian(blockIdx) (22B), SplitKey -> 14
	dataKey := store.DataKey(0x0102030405060708, 0x1122334455667788)
	require.Equal(t, []byte{'d', 'a', 't', 'a', ':', 1, 2, 3, 4, 5, 6, 7, 8, ':', 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}, dataKey)
	require.Len(t, dataKey, store.DataKeyLen)
	require.Equal(t, store.DataPrefixLen, store.SplitKey(dataKey))
	require.Equal(t, store.DataPrefix(0x0102030405060708), dataKey[:store.DataPrefixLen])
	require.Equal(t, store.DataPrefixLen, store.SplitKey(store.DataPrefix(0x0102030405060708)))
	dKeyIno, dKeyBlock, err := store.DecodeDataKey(dataKey)
	require.NoError(t, err)
	require.Equal(t, uint64(0x0102030405060708), dKeyIno)
	require.Equal(t, uint64(0x1122334455667788), dKeyBlock)
	require.Negative(t, bytes.Compare(store.DataKey(10, 1), store.DataKey(10, 2)))
	require.Negative(t, bytes.Compare(store.DataKey(10, ^uint64(0)), store.DataKey(11, 0)))

	_, _, err = store.DecodeDataKey([]byte("data:bad"))
	require.ErrorIs(t, err, store.ErrInvalidKey)

	// 7. PackStatKey: "sys:pack:" + 8B BigEndian(packID) (17B), SplitKey -> len(key) (17)
	require.Equal(t, 256<<10, store.BlobSize)
	require.Equal(t, 64<<20, store.PackMaxSize)
	require.Equal(t, []byte("sys:pack:"), store.PackStatPrefix())
	require.Len(t, store.PackStatPrefix(), store.PackStatPrefixLen)

	packID := uint64(0x010203043a3a0708) // 含 ':' (0x3a) 字节以验证 SplitKey 不会误切分
	packKey := store.PackStatKey(packID)
	require.Equal(t, []byte{'s', 'y', 's', ':', 'p', 'a', 'c', 'k', ':', 1, 2, 3, 4, 0x3a, 0x3a, 7, 8}, packKey)
	require.Len(t, packKey, store.PackStatKeyLen)
	require.Equal(t, store.PackStatKeyLen, store.SplitKey(packKey))
	require.True(t, bytes.HasPrefix(packKey, store.PackStatPrefix()))
	gotPackID, err := store.DecodePackStatKey(packKey)
	require.NoError(t, err)
	require.Equal(t, packID, gotPackID)
	require.Negative(t, bytes.Compare(store.PackStatKey(1), store.PackStatKey(256)))

	_, err = store.DecodePackStatKey([]byte("sys:pack:short"))
	require.ErrorIs(t, err, store.ErrInvalidKey)
	_, err = store.DecodePackStatKey([]byte("sys:stat:12345678"))
	require.ErrorIs(t, err, store.ErrInvalidKey)
	_, err = store.DecodePackStatKey(store.PackStatKey(0))
	require.ErrorIs(t, err, store.ErrInvalidKey)
}
