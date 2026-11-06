package pebblekv_test

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
	"github.com/stretchr/testify/require"
)

func TestDataKeysOrderAndRange(t *testing.T) {
	key := pebblekv.DataKey(0x0102030405060708, 0x1122334455667788)
	require.Equal(t, []byte{'d', 'a', 't', 'a', ':', 1, 2, 3, 4, 5, 6, 7, 8, ':', 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88}, key)
	ino, block, err := pebblekv.DecodeDataKey(key)
	require.NoError(t, err)
	require.Equal(t, uint64(0x0102030405060708), ino)
	require.Equal(t, uint64(0x1122334455667788), block)

	lower, upper := pebblekv.DataRange(42, 3)
	require.True(t, bytes.Compare(pebblekv.DataKey(42, 2), lower) < 0)
	require.True(t, bytes.Compare(pebblekv.DataKey(42, 3), lower) >= 0)
	require.True(t, bytes.Compare(pebblekv.DataKey(42, ^uint64(0)), upper) < 0)
	require.True(t, bytes.Compare(pebblekv.DataKey(43, 0), upper) >= 0)
}

func TestEntryRangeDeleteKeepsAdjacentKeys(t *testing.T) {
	db := openTestPebble(t)
	before := pebblekv.EntryKey(41, []byte("n"))
	first := pebblekv.EntryKey(42, []byte("a"))
	last := pebblekv.EntryKey(42, []byte{0xff})
	after := pebblekv.EntryKey(43, []byte("n"))
	xattr := pebblekv.XattrKey(42, []byte("a"))
	require.NoError(t, db.Set(before, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(first, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(last, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(after, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(xattr, []byte("kept"), pebble.Sync))

	lower, upper := pebblekv.EntryRange(42)
	require.NoError(t, db.DeleteRange(lower, upper, pebble.Sync))

	requireStoredValue(t, db, before)
	requireDeletedKey(t, db, first)
	requireDeletedKey(t, db, last)
	requireStoredValue(t, db, after)
	requireStoredValue(t, db, xattr)
}

func TestXattrRangeDeleteKeepsAdjacentKeys(t *testing.T) {
	db := openTestPebble(t)
	before := pebblekv.XattrKey(41, []byte("n"))
	first := pebblekv.XattrKey(42, []byte("a"))
	last := pebblekv.XattrKey(42, []byte{0xff})
	after := pebblekv.XattrKey(43, []byte("n"))
	entry := pebblekv.EntryKey(42, []byte("a"))
	require.NoError(t, db.Set(before, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(first, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(last, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(after, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(entry, []byte("kept"), pebble.Sync))

	lower, upper := pebblekv.XattrRange(42)
	require.NoError(t, db.DeleteRange(lower, upper, pebble.Sync))

	requireStoredValue(t, db, before)
	requireDeletedKey(t, db, first)
	requireDeletedKey(t, db, last)
	requireStoredValue(t, db, after)
	requireStoredValue(t, db, entry)
}

func TestDataRangeDeleteKeepsAdjacentKeys(t *testing.T) {
	db := openTestPebble(t)
	beforeInode := pebblekv.DataKey(41, 99)
	beforeBlock := pebblekv.DataKey(42, 2)
	first := pebblekv.DataKey(42, 3)
	last := pebblekv.DataKey(42, ^uint64(0))
	afterInode := pebblekv.DataKey(43, 0)
	require.NoError(t, db.Set(beforeInode, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(beforeBlock, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(first, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(last, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(afterInode, []byte("kept"), pebble.Sync))

	lower, upper := pebblekv.DataRange(42, 3)
	require.NoError(t, db.DeleteRange(lower, upper, pebble.Sync))

	requireStoredValue(t, db, beforeInode)
	requireStoredValue(t, db, beforeBlock)
	requireDeletedKey(t, db, first)
	requireDeletedKey(t, db, last)
	requireStoredValue(t, db, afterInode)
}

func TestDirtyRangeDeleteKeepsAdjacentNamespaces(t *testing.T) {
	db := openTestPebble(t)
	first := pebblekv.DirtyKey(1)
	last := pebblekv.DirtyKey(^uint64(0))
	orphan := pebblekv.OrphanKey(1)
	inode := pebblekv.InodeKey(1)
	stat := pebblekv.StatKey()
	require.NoError(t, db.Set(first, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(last, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(orphan, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(inode, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(stat, []byte("kept"), pebble.Sync))

	lower, upper := pebblekv.DirtyRange()
	require.NoError(t, db.DeleteRange(lower, upper, pebble.Sync))

	requireDeletedKey(t, db, first)
	requireDeletedKey(t, db, last)
	requireStoredValue(t, db, orphan)
	requireStoredValue(t, db, inode)
	requireStoredValue(t, db, stat)
}

func TestOrphanRangeDeleteKeepsAdjacentNamespaces(t *testing.T) {
	db := openTestPebble(t)
	first := pebblekv.OrphanKey(1)
	last := pebblekv.OrphanKey(^uint64(0))
	dirty := pebblekv.DirtyKey(1)
	inode := pebblekv.InodeKey(1)
	stat := pebblekv.StatKey()
	require.NoError(t, db.Set(first, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(last, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(dirty, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(inode, []byte("kept"), pebble.Sync))
	require.NoError(t, db.Set(stat, []byte("kept"), pebble.Sync))

	lower, upper := pebblekv.OrphanRange()
	require.NoError(t, db.DeleteRange(lower, upper, pebble.Sync))

	requireDeletedKey(t, db, first)
	requireDeletedKey(t, db, last)
	requireStoredValue(t, db, dirty)
	requireStoredValue(t, db, inode)
	requireStoredValue(t, db, stat)
}

func openTestPebble(t *testing.T) *pebble.DB {
	t.Helper()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func requireStoredValue(t *testing.T, db *pebble.DB, key []byte) {
	t.Helper()
	value, closer, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, "kept", string(value))
	require.NoError(t, closer.Close())
}

func requireDeletedKey(t *testing.T, db *pebble.DB, key []byte) {
	t.Helper()
	_, _, err := db.Get(key)
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestKeyNamespacesRoundTripAndBounds(t *testing.T) {
	name := []byte{'a', ':', 0, 0xff}

	requireNamedNamespace(t, pebblekv.EntryKey, pebblekv.DecodeEntryKey, pebblekv.EntryRange, name)
	requireNamedNamespace(t, pebblekv.XattrKey, pebblekv.DecodeXattrKey, pebblekv.XattrRange, name)
	requireNumberNamespace(t, pebblekv.DirtyKey, pebblekv.DecodeDirtyKey, pebblekv.DirtyRange)
	requireNumberNamespace(t, pebblekv.OrphanKey, pebblekv.DecodeOrphanKey, pebblekv.OrphanRange)
	requireNumberNamespace(t, pebblekv.InodeKey, pebblekv.DecodeInodeKey, pebblekv.InodeRange)

	_, _, err := pebblekv.DecodeDataKey([]byte("data:bad"))
	require.ErrorIs(t, err, pebblekv.ErrInvalidKey)
	lower, upper := pebblekv.PrefixIterBounds([]byte{0xff, 0xff})
	require.Equal(t, []byte{0xff, 0xff}, lower)
	require.Nil(t, upper)
}

func requireNamedNamespace(
	t *testing.T,
	key func(uint64, []byte) []byte,
	decode func([]byte) (uint64, []byte, error),
	bounds func(uint64) ([]byte, []byte),
	name []byte,
) {
	t.Helper()
	encoded := key(42, name)
	ino, gotName, err := decode(encoded)
	require.NoError(t, err)
	require.Equal(t, uint64(42), ino)
	require.Equal(t, name, gotName)
	lower, upper := bounds(42)
	require.GreaterOrEqual(t, bytes.Compare(encoded, lower), 0)
	require.Less(t, bytes.Compare(encoded, upper), 0)
	require.Less(t, bytes.Compare(key(41, name), lower), 0)
	require.GreaterOrEqual(t, bytes.Compare(key(43, name), upper), 0)
}

func requireNumberNamespace(
	t *testing.T,
	key func(uint64) []byte,
	decode func([]byte) (uint64, error),
	bounds func() ([]byte, []byte),
) {
	t.Helper()
	ino, err := decode(key(255))
	require.NoError(t, err)
	require.Equal(t, uint64(255), ino)
	lower, upper := bounds()
	require.GreaterOrEqual(t, bytes.Compare(key(0), lower), 0)
	require.Less(t, bytes.Compare(key(^uint64(0)), upper), 0)
}
