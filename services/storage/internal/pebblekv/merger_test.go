package pebblekv_test

import (
	"bytes"
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
	"github.com/stretchr/testify/require"
)

func TestPartialMergePreservesUnderlyingBase(t *testing.T) {
	first, err := pebblekv.EncodePatches(pebblekv.WritePatch(2, []byte("XY")))
	require.NoError(t, err)
	second, err := pebblekv.EncodePatches(pebblekv.ZeroPatch(5, 2), pebblekv.TruncatePatch(8), pebblekv.WritePatch(7, []byte("!")))
	require.NoError(t, err)
	merger, err := pebblekv.Merger.Merge(pebblekv.DataKey(7, 0), second)
	require.NoError(t, err)
	require.NoError(t, merger.MergeOlder(first))
	partial, closer, err := merger.Finish(false)
	require.NoError(t, err)
	require.Nil(t, closer)
	require.Equal(t, byte(pebblekv.PatchListFrame), partial[1])

	base, err := pebblekv.EncodeBase([]byte("abcdefghij"))
	require.NoError(t, err)
	combined, err := pebblekv.Merger.Merge(pebblekv.DataKey(7, 0), partial)
	require.NoError(t, err)
	require.NoError(t, combined.MergeOlder(base))
	result, closer, err := combined.Finish(false)
	require.NoError(t, err)
	require.Nil(t, closer)
	require.Equal(t, byte(pebblekv.BaseFrame), result[1])
	data, err := pebblekv.DecodeBlob(result)
	require.NoError(t, err)
	require.Equal(t, []byte{'a', 'b', 'X', 'Y', 'e', 0, 0, '!'}, data)
}

type observedMerger struct {
	pebble.ValueMerger
	partial *atomic.Int64
}

func (m *observedMerger) Finish(includesBase bool) ([]byte, io.Closer, error) {
	if !includesBase {
		m.partial.Add(1)
	}
	return m.ValueMerger.Finish(includesBase)
}

func TestPebbleCompactionKeepsFullBlobAndPatchOrder(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var partial, compactions atomic.Int64
	merger := &pebble.Merger{Name: pebblekv.Merger.Name, Merge: func(key, value []byte) (pebble.ValueMerger, error) {
		m, err := pebblekv.Merger.Merge(key, value)
		if err != nil {
			return nil, err
		}
		return &observedMerger{ValueMerger: m, partial: &partial}, nil
	}}
	db, err := pebble.Open(t.TempDir(), &pebble.Options{Merger: merger, EventListener: &pebble.EventListener{CompactionEnd: func(info pebble.CompactionInfo) {
		if info.Err == nil {
			compactions.Add(1)
		}
	}}})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	key := pebblekv.DataKey(3, 9)
	original := bytes.Repeat([]byte("0123456789abcdef"), pebblekv.BlobSize/16)
	base, err := pebblekv.EncodeBase(original)
	require.NoError(t, err)
	require.NoError(t, db.Set(key, base, pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact(ctx, key, append(bytes.Clone(key), 0), false))
	for _, p := range []pebblekv.Patch{
		pebblekv.WritePatch(8, []byte("WXYZ")),
		pebblekv.WritePatch(2, []byte("AAA")),
		pebblekv.WritePatch(4, []byte("q")),
		pebblekv.ZeroPatch(5, 3),
	} {
		operand, err := pebblekv.EncodePatches(p)
		require.NoError(t, err)
		require.NoError(t, db.Merge(key, operand, pebble.Sync))
	}
	require.NoError(t, db.Flush())
	require.Greater(t, partial.Load(), int64(0), "实际压实必须包含未触底的局部合并")
	require.NoError(t, db.Compact(ctx, key, append(bytes.Clone(key), 0), false))
	require.Greater(t, compactions.Load(), int64(0))
	encoded, closer, err := db.Get(key)
	require.NoError(t, err)
	got, err := pebblekv.DecodeBlob(encoded)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	expected := bytes.Clone(original)
	copy(expected[8:12], []byte("WXYZ"))
	copy(expected[2:5], []byte("AAA"))
	expected[4] = 'q'
	expected[5], expected[6], expected[7] = 0, 0, 0
	require.Equal(t, expected, got)

	tail, err := pebblekv.EncodePatches(pebblekv.TruncatePatch(12), pebblekv.WritePatch(11, []byte("!")))
	require.NoError(t, err)
	require.NoError(t, db.Merge(key, tail, pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact(ctx, key, append(bytes.Clone(key), 0), false))
	encoded, closer, err = db.Get(key)
	require.NoError(t, err)
	got, err = pebblekv.DecodeBlob(encoded)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Equal(t, []byte{'0', '1', 'A', 'A', 'q', 0, 0, 0, 'W', 'X', 'Y', '!'}, got)
}

func TestMergeAssociativityAndOverwrite(t *testing.T) {
	a, err := pebblekv.EncodePatches(pebblekv.WritePatch(0, []byte("abc")))
	require.NoError(t, err)
	b, err := pebblekv.EncodePatches(pebblekv.TruncatePatch(2))
	require.NoError(t, err)
	c, err := pebblekv.EncodePatches(pebblekv.WritePatch(4, []byte("Z")))
	require.NoError(t, err)
	key := pebblekv.DataKey(1, 0)
	combine := func(first, second []byte) []byte {
		m, err := pebblekv.Merger.Merge(key, first)
		require.NoError(t, err)
		require.NoError(t, m.MergeNewer(second))
		result, closer, err := m.Finish(false)
		require.NoError(t, err)
		require.Nil(t, closer)
		return result
	}
	left := combine(combine(a, b), c)
	right := combine(a, combine(b, c))
	require.Equal(t, left, right)
	base, err := pebblekv.EncodeBase([]byte("ABCDEFGH"))
	require.NoError(t, err)
	withBase := combine(base, left)
	got, err := pebblekv.DecodeBlob(withBase)
	require.NoError(t, err)
	require.Equal(t, []byte{'a', 'b', 0, 0, 'Z'}, got)
	noBase, err := pebblekv.DecodeBlob(right)
	require.NoError(t, err)
	require.Equal(t, []byte{'a', 'b', 0, 0, 'Z'}, noBase)

	newerBase, err := pebblekv.EncodeBase([]byte("reset"))
	require.NoError(t, err)
	got, err = pebblekv.DecodeBlob(combine(combine(a, newerBase), c))
	require.NoError(t, err)
	require.Equal(t, []byte("reseZ"), got)
	m, err := pebblekv.Merger.Merge([]byte("inode:ordinary"), []byte("middle"))
	require.NoError(t, err)
	require.NoError(t, m.MergeOlder([]byte("old")))
	require.NoError(t, m.MergeNewer([]byte("new")))
	value, _, err := m.Finish(false)
	require.NoError(t, err)
	require.Equal(t, []byte("new"), value)
}

func TestBlobFramesRejectInvalidInput(t *testing.T) {
	for _, p := range []pebblekv.Patch{
		pebblekv.WritePatch(pebblekv.BlobSize-1, []byte("ab")),
		pebblekv.ZeroPatch(pebblekv.BlobSize, 1),
		pebblekv.TruncatePatch(pebblekv.BlobSize + 1),
		pebblekv.WritePatch(0, nil),
	} {
		_, err := pebblekv.EncodePatches(p)
		require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	}
	_, err := pebblekv.EncodeBase(make([]byte, pebblekv.BlobSize+1))
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	for _, encoded := range [][]byte{nil, {1, 3}, {1, 2, 1}, {1, 2, 1, 0, 0, 0, 0, 0, 0, 0, 2, 'x'}} {
		_, err := pebblekv.DecodeBlob(encoded)
		require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	}
}

func TestPebbleReadsPatchOnlyBlobAndFullSetOverridesPatches(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	db, err := pebble.Open(t.TempDir(), &pebble.Options{Merger: pebblekv.Merger})
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	key := pebblekv.DataKey(9, 0)
	patch, err := pebblekv.EncodePatches(pebblekv.WritePatch(3, []byte("abc")))
	require.NoError(t, err)
	require.NoError(t, db.Merge(key, patch, pebble.Sync))
	value, closer, err := db.Get(key)
	require.NoError(t, err)
	require.Equal(t, byte(pebblekv.BaseFrame), value[1])
	got, err := pebblekv.DecodeBlob(value)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Equal(t, []byte{0, 0, 0, 'a', 'b', 'c'}, got)

	base, err := pebblekv.EncodeBase([]byte("new"))
	require.NoError(t, err)
	require.NoError(t, db.Set(key, base, pebble.Sync))
	require.NoError(t, db.Flush())
	require.NoError(t, db.Compact(ctx, key, append(bytes.Clone(key), 0), false))
	value, closer, err = db.Get(key)
	require.NoError(t, err)
	got, err = pebblekv.DecodeBlob(value)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Equal(t, []byte("new"), got)
}
