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
	require.NoError(t, store.AppendTail(liveID, int64(len(data)), tail, full))
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

// TestBlobAppendTailOverwritesStaleResidual 锁定窗口内尾块追加的幂等覆盖语义：
// 上一次未提交/失败批次可能在块文件尾部留下残留字节，再次从原逻辑偏移追加时必须
// 精确覆写并截断残留，磁盘与热缓存都必须与逻辑块内容 100% 一致。
func TestBlobAppendTailOverwritesStaleResidual(t *testing.T) {
	base := filepath.Join(t.TempDir(), "branches", "main")
	store := pebblekv.NewBlobStore(base)
	const id = 9

	head := bytes.Repeat([]byte{0x41}, 4096)
	require.NoError(t, store.Write(id, head))

	// 模拟失败批次遗留：块文件尾部多出未被任何 DataKey 引用的残留字节。
	residual := bytes.Repeat([]byte{0xee}, 777)
	file, err := os.OpenFile(store.Path(id), os.O_WRONLY|os.O_APPEND, 0o644)
	require.NoError(t, err)
	_, err = file.Write(residual)
	require.NoError(t, err)
	require.NoError(t, file.Close())

	tail := bytes.Repeat([]byte{0x42}, 2048)
	full := append(append([]byte(nil), head...), tail...)
	require.NoError(t, store.AppendTail(id, int64(len(head)), tail, full))

	onDisk, err := os.ReadFile(store.Path(id))
	require.NoError(t, err)
	require.Equal(t, full, onDisk, "逻辑偏移写入必须覆盖并截断上一次失败批次的残留字节")

	// 命中热缓存：块内容必须是写入后的完整逻辑块。
	cached, err := store.Read(id, len(full))
	require.NoError(t, err)
	require.Equal(t, full, cached)

	// 重新从磁盘读取（新句柄无热缓存）：必须与热缓存逐字节一致。
	cold := pebblekv.NewBlobStore(base)
	reloaded, err := cold.Read(id, len(full))
	require.NoError(t, err)
	require.Equal(t, full, reloaded)

	partial := make([]byte, len(tail))
	require.NoError(t, cold.ReadAt(id, int64(len(head)), partial))
	require.Equal(t, tail, partial, "尾部区间不得因残留字节产生错位")
}

// TestBlobStoreHotCacheRetains400SmallBlocks 锁定 16 分片 × 64 槽位 × 4 MiB/分片预算的热块缓存：
// 400 个 128 KiB 块（50 MiB）必须全部驻留，且命中路径完全不需要打开底层块文件。
func TestBlobStoreHotCacheRetains400SmallBlocks(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	const (
		blockCount = 400
		blockSize  = 128 << 10
	)

	for i := 1; i <= blockCount; i++ {
		data := bytes.Repeat([]byte{byte(i)}, blockSize)
		require.NoError(t, store.Write(uint64(i), data))
	}

	// 直接移除底层块目录：任何未命中热缓存的读取都会因 open 失败而暴露。
	require.NoError(t, os.RemoveAll(store.Root()))

	for i := 1; i <= blockCount; i++ {
		got, err := store.Read(uint64(i), blockSize)
		require.NoError(t, err, "块 %d 必须命中热缓存而无需打开底层文件", i)
		require.Equal(t, byte(i), got[0])
		require.Equal(t, byte(i), got[blockSize-1])
	}
}

// TestBlobStoreHotCacheEvictsByShardByteBudget 锁定分片字节预算淘汰：
// 同一分片连续写入 40 个 128 KiB 块（5 MiB > 4 MiB）后，必须按环形顺序淘汰最旧的 8 个块，
// 恰好保留最新 32 个块（4 MiB），且保留集合与字节预算精确守恒。
func TestBlobStoreHotCacheEvictsByShardByteBudget(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	const (
		blockSize     = 128 << 10
		blockCount    = 40
		shardBudget   = 4 << 20
		retainedCount = shardBudget / blockSize // 32
		evictedCount  = blockCount - retainedCount
	)

	// 全部落在同一分片（id % 16 == 1）。
	for i := 0; i < blockCount; i++ {
		id := uint64(1 + i*16)
		require.NoError(t, store.Write(id, bytes.Repeat([]byte{byte(i + 1)}, blockSize)))
	}

	// 移除底层块目录，用“能否读取”精确观测内存驻留集合。
	require.NoError(t, os.RemoveAll(store.Root()))

	for i := 0; i < evictedCount; i++ {
		id := uint64(1 + i*16)
		_, err := store.Read(id, blockSize)
		require.Error(t, err, "最旧的块 %d 必须已被环形淘汰", id)
	}
	for i := evictedCount; i < blockCount; i++ {
		id := uint64(1 + i*16)
		got, err := store.Read(id, blockSize)
		require.NoError(t, err, "最新的块 %d 必须仍在热缓存中", id)
		require.Equal(t, byte(i+1), got[0])
	}
}

// SyncInode 只刷目标 Inode 的脏块，其它文件的脏块必须保持待同步状态，
// 避免多进程写同一分支时一次 fsync 把别人的数据一起拖进屏障。
func TestBlobStoreSyncInodeOnlySyncsTargetInode(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	require.NoError(t, store.WriteForInode(10, 1, []byte("aaaa")))
	require.NoError(t, store.WriteForInode(20, 2, []byte("bbbb")))
	require.True(t, store.HasDirty())

	require.NoError(t, store.SyncInode(10))
	require.EqualValues(t, 1, store.FileSyncOps(), "只应同步目标 Inode 的 1 个块")
	require.False(t, store.WindowLocal(1), "目标块同步后必须离开窗口")
	require.True(t, store.WindowLocal(2), "其它 Inode 的脏块必须保持待同步")
	require.True(t, store.HasDirty())

	require.NoError(t, store.SyncInode(20))
	require.EqualValues(t, 2, store.FileSyncOps())
	require.False(t, store.HasDirty())
}

// 未登记 Inode 的脏块（旧调用方或上次同步失败重试）必须跟随任意一次 SyncInode 落盘。
func TestBlobStoreSyncInodeFlushesUnassociatedBlocks(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	require.NoError(t, store.WriteForInode(10, 1, []byte("aaaa")))
	require.NoError(t, store.Write(2, []byte("bbbb")))
	require.True(t, store.HasDirty())

	require.NoError(t, store.SyncInode(10))
	require.EqualValues(t, 2, store.FileSyncOps(), "未登记 Inode 的脏块必须一并落盘")
	require.False(t, store.HasDirty())
}

// 多块并行同步必须覆盖全部脏块且不重不漏。
func TestBlobStoreSyncParallelCoversAllBlocks(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	for i := uint64(1); i <= 64; i++ {
		require.NoError(t, store.WriteForInode(10, i, []byte("data")))
	}
	require.NoError(t, store.SyncInode(10))
	require.EqualValues(t, 64, store.FileSyncOps())
	require.False(t, store.HasDirty())
}

// TestBlobSyncOnlyNewFilesNeedDirBarrier 锁定目录屏障只由“新建 / 删除块文件”触发：
// AppendTail / Patch 原地覆写不改变目录项，绝不能为它们多付一次分片目录 fsync。
func TestBlobSyncOnlyNewFilesNeedDirBarrier(t *testing.T) {
	store := pebblekv.NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
	data := bytes.Repeat([]byte{0x11}, 4096)

	require.NoError(t, store.Write(0x21, data))
	require.NoError(t, store.Sync())
	require.EqualValues(t, 1, store.FileSyncOps())
	require.EqualValues(t, 1, store.DirSyncOps(), "新建块必须落一次目录项屏障")

	// 已落盘块的原地追加 + 原地补丁：目录项从未变化。
	tail := bytes.Repeat([]byte{0x22}, 1024)
	require.NoError(t, store.AppendTail(0x21, int64(len(data)), tail, nil))
	require.NoError(t, store.Patch(0x21, 8, []byte("patch"), int64(len(data)+len(tail))))

	require.NoError(t, store.Sync())
	require.EqualValues(t, 2, store.FileSyncOps(), "只应同步被原地覆写的块文件")
	require.EqualValues(t, 1, store.DirSyncOps(), "AppendTail / Patch 不得产生目录屏障")

	// 新建块仍然必须落目录项屏障。
	require.NoError(t, store.Write(0x22, data))
	require.NoError(t, store.Sync())
	require.EqualValues(t, 3, store.FileSyncOps())
	require.EqualValues(t, 2, store.DirSyncOps(), "新建块必须落一次目录项屏障")

	// 删除已落盘块必须落 unlink 的目录屏障。
	require.NoError(t, store.Remove(0x22))
	require.NoError(t, store.Sync())
	require.EqualValues(t, 3, store.DirSyncOps(), "删除已落盘块必须 fsync 其分片目录")
}

// TestBlobPatchWindowLocal 锁定 Patch 的原地覆写语义：按逻辑偏移覆写 chunk 并截断到
// 目标长度，同时剔除热缓存，让后续读回落到块文件而不是读到旧内容。
func TestBlobPatchWindowLocal(t *testing.T) {
	base := filepath.Join(t.TempDir(), "branches", "main")
	store := pebblekv.NewBlobStore(base)
	const id = 0x31

	head := bytes.Repeat([]byte{0x11}, 4096)
	require.NoError(t, store.Write(id, head))

	// 从旧长度开始追加：目标长度越过旧块尾，Patch 必须扩展文件而不是截掉新数据。
	tail := bytes.Repeat([]byte{0x22}, 4096)
	require.NoError(t, store.Patch(id, int64(len(head)), tail, int64(len(head)+len(tail))))

	want := append(append([]byte(nil), head...), tail...)
	onDisk, err := os.ReadFile(store.Path(id))
	require.NoError(t, err)
	require.Equal(t, want, onDisk, "Patch 后块文件必须等于旧内容 + chunk")

	// 独立判据：清空块文件后读必须失败，证明热缓存已被剔除而不是继续返回旧前缀。
	require.NoError(t, os.Truncate(store.Path(id), 0))
	_, err = store.Read(id, len(want))
	require.Error(t, err, "Patch 必须剔除热缓存，读必须回落到块文件")
	require.NoError(t, os.WriteFile(store.Path(id), want, 0o644))

	// 块内原地覆写不得改变逻辑长度。
	patch := []byte("patch")
	require.NoError(t, store.Patch(id, 100, patch, int64(len(want))))
	got, err := store.Read(id, len(want))
	require.NoError(t, err)
	copy(want[100:], patch)
	require.Equal(t, want, got, "块内覆写必须精确保留后续字节")
}

// TestBlobFdatasyncPath 锁定块文件屏障的落盘契约：Sync 后新的只读句柄必须读到完整内容。
// Linux 上的 fdatasync 系统调用选择由 fdatasync_linux_test.go 单独校验。
func TestBlobFdatasyncPath(t *testing.T) {
	base := filepath.Join(t.TempDir(), "branches", "main")
	store := pebblekv.NewBlobStore(base)
	data := bytes.Repeat([]byte{0x5a}, 8192)
	require.NoError(t, store.WriteForInode(10, 0x41, data))

	require.NoError(t, store.SyncInode(10))
	require.EqualValues(t, 1, store.FileSyncOps())

	cold := pebblekv.NewBlobStore(base)
	got, err := cold.Read(0x41, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
}
