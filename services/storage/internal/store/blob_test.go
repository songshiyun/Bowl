package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestBlobStorePathAndSegmentAllocation(t *testing.T) {
	base := t.TempDir()
	var reserveCalls int
	var lastReserved uint64

	blobs := store.NewBlobStoreWithAllocator(base, func(count uint64) (uint64, uint64, error) {
		require.Equal(t, store.BlobIDSegment, count)
		reserveCalls++
		first := lastReserved + 1
		last := lastReserved + count
		lastReserved = last
		return first, last, nil
	})

	require.Equal(t, filepath.Join(base, "blobs"), blobs.Root())
	require.Equal(t, filepath.Join(base, "blobs", "ab", "00000000000001ab.blob"), blobs.Path(0x1ab))

	var firstID, lastInFirstSeg uint64
	for i := range int(store.BlobIDSegment) {
		id, err := blobs.NextBlobID()
		require.NoError(t, err)
		if i == 0 {
			firstID = id
		}
		if i == int(store.BlobIDSegment)-1 {
			lastInFirstSeg = id
		}
	}
	require.Equal(t, uint64(1), firstID)
	require.Equal(t, uint64(4096), lastInFirstSeg)
	require.Equal(t, 1, reserveCalls)

	nextID, err := blobs.NextBlobID()
	require.NoError(t, err)
	require.Equal(t, uint64(4097), nextID)
	require.Equal(t, 2, reserveCalls)
}

func TestBlobStoreScanRecoveryOnReopen(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)
	require.NoError(t, blobs.WriteForInode(2, 0x0250, []byte("existing-blob")))

	reopened := store.NewBlobStore(base)
	id, err := reopened.NextBlobID()
	require.NoError(t, err)
	require.Greater(t, id, uint64(0x0250))
}

func TestBlobStoreHotShardCacheEvictionAndReadAt(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)

	// 同一分片 (id % 16 == 1) 预算为 4 MiB（4 个 1 MiB 块）
	// 写入 5 个 1 MiB 块到同一分片，第 1 个块应从 hotShard 被淘汰，第 2..5 个块保留在 hotShard
	ids := []uint64{1, 17, 33, 49, 65}
	payloads := make([][]byte, len(ids))
	for i, id := range ids {
		payloads[i] = bytes.Repeat([]byte{byte(0x10 + i)}, store.BlobSize)
		require.NoError(t, blobs.WriteForInode(10, id, payloads[i]))
	}

	// 验证 ids[1..4] 仍在 hotShard 中：截断底层物理文件为 0 后 ReadAt 依然命中内存缓存返回正确内容
	for i := 1; i < len(ids); i++ {
		id := ids[i]
		require.NoError(t, os.Truncate(blobs.Path(id), 0))
		buf := make([]byte, 4096)
		require.NoError(t, blobs.ReadAt(id, 128, buf))
		require.Equal(t, payloads[i][128:128+4096], buf)
		// 恢复磁盘文件
		require.NoError(t, os.WriteFile(blobs.Path(id), payloads[i], 0o644))
	}

	// 验证 ids[0] 已从 hotShard 被淘汰：截断底层物理文件后 ReadAt 走磁盘 pread 返回 EOF 错误
	require.NoError(t, os.Truncate(blobs.Path(ids[0]), 0))
	buf := make([]byte, 4096)
	require.Error(t, blobs.ReadAt(ids[0], 0, buf))

	// 恢复 ids[0] 磁盘文件后，ReadAt 正常回退磁盘 pread 读取成功
	require.NoError(t, os.WriteFile(blobs.Path(ids[0]), payloads[0], 0o644))
	require.NoError(t, blobs.ReadAt(ids[0], 512, buf))
	require.Equal(t, payloads[0][512:512+4096], buf)
}

func TestBlobStoreDetachMetaAndRemoveFiles(t *testing.T) {
	base := t.TempDir()
	blobs := store.NewBlobStore(base)

	// 块 101：窗口内新建但未 Sync 的过渡块（dirtyNew）
	// 块 102：已 Sync 持久化的旧块（非 dirtyNew）
	data1 := bytes.Repeat([]byte{0xaa}, 8192)
	data2 := bytes.Repeat([]byte{0xbb}, 8192)
	require.NoError(t, blobs.WriteForInode(20, 101, data1))
	require.NoError(t, blobs.WriteForInode(20, 102, data2))
	require.True(t, blobs.WindowLocal(101))
	require.True(t, blobs.WindowLocal(102))

	// 先对 102 所在文件或全局执行 Sync，使 102 固化（不再是 dirtyNew / WindowLocal）
	require.NoError(t, blobs.SyncInode(20))
	require.False(t, blobs.WindowLocal(101))
	require.False(t, blobs.WindowLocal(102))
	require.False(t, blobs.HasDirty())

	// 再写入一个窗口内新块 103
	data3 := bytes.Repeat([]byte{0xcc}, 8192)
	require.NoError(t, blobs.WriteForInode(20, 103, data3))
	require.True(t, blobs.WindowLocal(103))

	// Step 4 Actor 内纯内存 DetachMeta：
	// - 103 是窗口内未 Sync 过渡块（dirtyNew），摘除后不产生 dirtyDir
	// - 102 是已持久化旧块（非 dirtyNew），摘除后必须登记父分片目录到 dirtyDir
	blobs.DetachMeta([]uint64{103})
	require.False(t, blobs.WindowLocal(103))
	require.False(t, blobs.HasDirty(), "删除窗口内过渡块 103 不得产生 dirtyDir")

	blobs.DetachMeta([]uint64{102})
	require.True(t, blobs.HasDirty(), "删除已持久化旧块 102 必须登记父分片目录至 dirtyDir")

	// DetachMeta 是 0 I/O 纯内存操作，此时物理文件仍在磁盘上
	require.True(t, blobs.Exists(102))
	require.True(t, blobs.Exists(103))

	// RPC 协程在队列外调用 RemoveFiles 删除物理文件（覆盖单块与多块并发路径）
	require.NoError(t, blobs.RemoveFiles([]uint64{102}))
	require.False(t, blobs.Exists(102))

	require.NoError(t, blobs.WriteForInode(20, 104, data1))
	blobs.DetachMeta([]uint64{103, 104})
	require.NoError(t, blobs.RemoveFiles([]uint64{103, 104, 999999}))
	require.False(t, blobs.Exists(103))
	require.False(t, blobs.Exists(104))
}
