package pebblekv_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
	"github.com/stretchr/testify/require"
)

func TestBlobRefRoundTripAndValidation(t *testing.T) {
	ref := pebblekv.BlobRef{ID: 0x0102030405060708, Length: pebblekv.BlobSize}
	encoded := pebblekv.EncodeBlobRef(ref)
	require.Len(t, encoded, 12)
	decoded, err := pebblekv.DecodeBlobRef(encoded)
	require.NoError(t, err)
	require.Equal(t, ref, decoded)

	for _, invalid := range [][]byte{
		nil,
		make([]byte, 11),
		pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: 0, Length: 1}),
		pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: 1, Length: 0}),
		pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: 1, Length: pebblekv.BlobSize + 1}),
	} {
		_, err := pebblekv.DecodeBlobRef(invalid)
		require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	}
}

func TestBlobStoreWriteReadRemoveAndShardLayout(t *testing.T) {
	root := t.TempDir()
	store := pebblekv.NewBlobStore(filepath.Join(root, "branches", "main"))
	data := bytes.Repeat([]byte{0x7f}, 1024)
	require.NoError(t, store.Write(0x10a, data))

	path := store.Path(0x10a)
	require.Equal(t, filepath.Join(root, "branches", "main", "blobs", "0a", "000000000000010a.blob"), path)
	info, err := os.Stat(path)
	require.NoError(t, err)
	require.Equal(t, int64(len(data)), info.Size())

	got, err := store.Read(0x10a, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)

	partial := make([]byte, 16)
	require.NoError(t, store.ReadAt(0x10a, 100, partial))
	require.Equal(t, data[100:116], partial)

	require.NoError(t, store.Remove(0x10a))
	require.NoError(t, store.Remove(0x10a))
	require.False(t, store.Exists(0x10a))
}

func TestBlobStoreLinksAndSharesImmutableFiles(t *testing.T) {
	root := t.TempDir()
	source := pebblekv.NewBlobStore(filepath.Join(root, "branches", "main"))
	target := pebblekv.NewBlobStore(filepath.Join(root, "commits", "c1"))
	data := bytes.Repeat([]byte{0x33}, 2048)
	require.NoError(t, source.Write(7, data))
	require.NoError(t, source.Write(0x107, data))
	require.NoError(t, source.LinkAllTo(target))

	for _, id := range []uint64{7, 0x107} {
		from, err := os.Stat(source.Path(id))
		require.NoError(t, err)
		to, err := os.Stat(target.Path(id))
		require.NoError(t, err)
		require.True(t, os.SameFile(from, to))
	}

	// 删除当前分支块只减少硬链接计数，不影响快照副本。
	require.NoError(t, source.Remove(7))
	require.False(t, source.Exists(7))
	got, err := target.Read(7, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestBlobStoreSyncPersistsDirtyFilesAndDirectories(t *testing.T) {
	store := pebblekv.NewBlobStore(t.TempDir())
	require.NoError(t, store.Write(1, []byte("abc")))
	require.NoError(t, store.Sync())
	require.NoError(t, store.Sync())
	require.NoError(t, store.Remove(1))
	require.NoError(t, store.Sync())
}
