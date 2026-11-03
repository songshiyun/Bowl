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

func TestPebbleRangeDeletesDoNotTouchAdjacentKeys(t *testing.T) {
	db, err := pebble.Open(t.TempDir(), &pebble.Options{Merger: pebblekv.Merger})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	keys := [][]byte{
		pebblekv.EntryKey(41, []byte("n")), pebblekv.EntryKey(42, []byte("a")), pebblekv.EntryKey(42, []byte{0xff}), pebblekv.EntryKey(43, []byte("n")),
		pebblekv.XattrKey(41, []byte("n")), pebblekv.XattrKey(42, []byte("a")), pebblekv.XattrKey(42, []byte{0xff}), pebblekv.XattrKey(43, []byte("n")),
		pebblekv.DataKey(41, 99), pebblekv.DataKey(42, 2), pebblekv.DataKey(42, 3), pebblekv.DataKey(42, ^uint64(0)), pebblekv.DataKey(43, 0),
		pebblekv.DirtyKey(1), pebblekv.DirtyKey(^uint64(0)), pebblekv.OrphanKey(1), pebblekv.OrphanKey(^uint64(0)), pebblekv.InodeKey(1), pebblekv.StatKey(),
	}
	for _, key := range keys {
		require.NoError(t, db.Set(key, []byte("kept"), pebble.Sync))
	}
	ranges := []struct{ lower, upper []byte }{}
	lower, upper := pebblekv.EntryRange(42)
	ranges = append(ranges, struct{ lower, upper []byte }{lower, upper})
	lower, upper = pebblekv.XattrRange(42)
	ranges = append(ranges, struct{ lower, upper []byte }{lower, upper})
	lower, upper = pebblekv.DataRange(42, 3)
	ranges = append(ranges, struct{ lower, upper []byte }{lower, upper})
	lower, upper = pebblekv.DirtyRange()
	ranges = append(ranges, struct{ lower, upper []byte }{lower, upper})
	lower, upper = pebblekv.OrphanRange()
	ranges = append(ranges, struct{ lower, upper []byte }{lower, upper})
	for i, expected := range [][]int{{1, 2}, {5, 6}, {10, 11}, {13, 14}, {15, 16}} {
		iter, err := db.NewIter(&pebble.IterOptions{LowerBound: ranges[i].lower, UpperBound: ranges[i].upper})
		require.NoError(t, err)
		var got [][]byte
		for valid := iter.First(); valid; valid = iter.Next() {
			got = append(got, bytes.Clone(iter.Key()))
		}
		require.NoError(t, iter.Error())
		require.NoError(t, iter.Close())
		want := make([][]byte, 0, len(expected))
		for _, index := range expected {
			want = append(want, keys[index])
		}
		require.Equal(t, want, got)
	}
	for _, r := range ranges {
		require.NoError(t, db.DeleteRange(r.lower, r.upper, pebble.Sync))
	}
	for i, key := range keys {
		value, closer, err := db.Get(key)
		switch i {
		case 1, 2, 5, 6, 10, 11, 13, 14, 15, 16:
			require.ErrorIs(t, err, pebble.ErrNotFound)
		default:
			require.NoError(t, err)
			require.Equal(t, "kept", string(value))
			require.NoError(t, closer.Close())
		}
	}
}

func TestKeyNamespacesRoundTripAndBounds(t *testing.T) {
	name := []byte{'a', ':', 0, 0xff}
	tests := []struct {
		name   string
		key    func(uint64, []byte) []byte
		decode func([]byte) (uint64, []byte, error)
		bounds func(uint64) ([]byte, []byte)
	}{
		{"entry", pebblekv.EntryKey, pebblekv.DecodeEntryKey, pebblekv.EntryRange},
		{"xattr", pebblekv.XattrKey, pebblekv.DecodeXattrKey, pebblekv.XattrRange},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := tt.key(42, name)
			ino, gotName, err := tt.decode(key)
			require.NoError(t, err)
			require.Equal(t, uint64(42), ino)
			require.Equal(t, name, gotName)
			lower, upper := tt.bounds(42)
			require.True(t, bytes.Compare(key, lower) >= 0 && bytes.Compare(key, upper) < 0)
			require.True(t, bytes.Compare(tt.key(41, name), lower) < 0)
			require.True(t, bytes.Compare(tt.key(43, name), upper) >= 0)
		})
	}
	for _, tt := range []struct {
		key    func(uint64) []byte
		decode func([]byte) (uint64, error)
		bounds func() ([]byte, []byte)
	}{{pebblekv.DirtyKey, pebblekv.DecodeDirtyKey, pebblekv.DirtyRange}, {pebblekv.OrphanKey, pebblekv.DecodeOrphanKey, pebblekv.OrphanRange}, {pebblekv.InodeKey, pebblekv.DecodeInodeKey, pebblekv.InodeRange}} {
		ino, err := tt.decode(tt.key(255))
		require.NoError(t, err)
		require.Equal(t, uint64(255), ino)
		lower, upper := tt.bounds()
		require.True(t, bytes.Compare(tt.key(0), lower) >= 0)
		require.True(t, bytes.Compare(tt.key(^uint64(0)), upper) < 0)
	}
	_, _, err := pebblekv.DecodeDataKey([]byte("data:bad"))
	require.ErrorIs(t, err, pebblekv.ErrInvalidKey)
	lower, upper := pebblekv.PrefixIterBounds([]byte{0xff, 0xff})
	require.Equal(t, []byte{0xff, 0xff}, lower)
	require.Nil(t, upper)
}
