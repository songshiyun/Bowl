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

// TestBlobSyncSkipsTransientShards 锁定「窗口内过渡块不产生分片目录屏障」与
// 「窗口内尾部原地追加保持热缓存最新」两条契约，对应 SQLite WAL 单事务 40 次 4 KiB CoW。
func TestBlobSyncSkipsTransientShards(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	data := bytes.Repeat([]byte{0x7f}, 4096)

	// 单事务内 40 次 4 KiB CoW：每块写入后立刻被替换删除，散列到 40 个不同分片目录。
	for id := uint64(1); id <= 40; id++ {
		require.NoError(t, store.Write(id, data))
		require.NoError(t, store.Remove(id))
	}

	// 窗口内只剩一个存活块。
	const liveID = 41
	require.NoError(t, store.Write(liveID, data))
	require.True(t, store.WindowLocal(liveID), "窗口内新建块必须可原地追加")

	// 窗口内尾部原地追加：块文件与热缓存都必须呈现追加后的完整内容。
	tail := bytes.Repeat([]byte{0x2a}, 4096)
	full := append(append([]byte(nil), data...), tail...)
	require.NoError(t, store.AppendTail(liveID, tail, full))
	onDisk, err := os.ReadFile(store.Path(liveID))
	require.NoError(t, err)
	require.Equal(t, full, onDisk)

	// 独立判据：清空磁盘上的块文件后仍能读到完整内容，证明热缓存已替换为追加后的整块。
	require.NoError(t, os.Truncate(store.Path(liveID), 0))
	cached, err := store.Read(liveID, len(full))
	require.NoError(t, err)
	require.Equal(t, full, cached, "追加后热缓存必须保存最新的完整块")
	require.NoError(t, os.WriteFile(store.Path(liveID), full, 0o644))

	require.NoError(t, store.Sync())
	require.EqualValues(t, 1, store.FileSyncOps(), "只应同步 1 个存活块")
	require.EqualValues(t, 1, store.DirSyncOps(), "40 个已消失过渡块不应产生分片目录 fsync")
	require.False(t, store.WindowLocal(liveID), "Sync 后块已持久化，不再属于窗口内块")

	// 删除已持久化块仍必须落目录屏障，保证 unlink 先于 WAL 引用删除落盘。
	require.NoError(t, store.Remove(liveID))
	require.NoError(t, store.Sync())
	require.EqualValues(t, 2, store.DirSyncOps(), "删除已落盘块必须 fsync 其分片目录")
}
