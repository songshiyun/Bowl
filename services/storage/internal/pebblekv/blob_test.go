package pebblekv_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
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

// 模拟宿主文件系统硬链接上限 EMLINK，验证 Commit / Fork 会回退为流式拷贝并生成独立 Inode。
func TestBlobStoreLinkAllToFallsBackOnEMLINK(t *testing.T) {
	root := t.TempDir()
	storage := pebblekv.NewManager(root, pebblekv.WithLinkFunc(func(string, string) error {
		return syscall.EMLINK
	}))
	t.Cleanup(func() { require.NoError(t, storage.Close()) })

	data := bytes.Repeat([]byte{0x5a}, 4096)
	branch, err := storage.OpenBranch("disk", "main")
	require.NoError(t, err)
	require.NoError(t, branch.Blobs().Write(7, data))
	require.NoError(t, branch.Checkpoint("c1"))
	require.NoError(t, branch.Close())

	commit, err := storage.OpenCommit("disk", "c1")
	require.NoError(t, err)
	got, err := commit.Blobs().Read(7, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
	sourceInfo, err := os.Stat(branchBlobPath(root, "main", 7))
	require.NoError(t, err)
	commitInfo, err := os.Stat(filepath.Join(root, "disks", "disk", "commits", "c1", "blobs", "07", "0000000000000007.blob"))
	require.NoError(t, err)
	require.False(t, os.SameFile(sourceInfo, commitInfo), "EMLINK 回退必须复制出新的物理 Inode")
	require.NoError(t, commit.Close())

	require.NoError(t, storage.Fork("disk", "c1", "dev"))
	fork, err := storage.OpenBranch("disk", "dev")
	require.NoError(t, err)
	got, err = fork.Blobs().Read(7, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
	forkInfo, err := os.Stat(branchBlobPath(root, "dev", 7))
	require.NoError(t, err)
	require.False(t, os.SameFile(commitInfo, forkInfo), "Fork 在 EMLINK 时也必须回退拷贝")
	require.NoError(t, fork.Close())
}

func branchBlobPath(root, branch string, id uint64) string {
	return filepath.Join(root, "disks", "disk", "branches", branch, "blobs", fmt.Sprintf("%02x", id&0xff), fmt.Sprintf("%016x.blob", id))
}

func TestBlobStoreSyncPersistsDirtyFilesAndDirectories(t *testing.T) {
	store := pebblekv.NewBlobStore(t.TempDir())
	require.NoError(t, store.Write(1, []byte("abc")))
	require.NoError(t, store.Sync())
	require.NoError(t, store.Sync())
	require.NoError(t, store.Remove(1))
	require.NoError(t, store.Sync())
}
