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

	_, err = pebblekv.DecodeBlobRef(nil)
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	_, err = pebblekv.DecodeBlobRef(make([]byte, 11))
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	_, err = pebblekv.DecodeBlobRef(pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: 0, Length: 1}))
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	_, err = pebblekv.DecodeBlobRef(pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: 1, Length: 0}))
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	_, err = pebblekv.DecodeBlobRef(pebblekv.EncodeBlobRef(pebblekv.BlobRef{ID: 1, Length: pebblekv.BlobSize + 1}))
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
}

func TestBlobStoreLinksAndSharesImmutableFiles(t *testing.T) {
	root := t.TempDir()
	source := pebblekv.NewBlobStore(filepath.Join(root, "branches", "main"))
	target := pebblekv.NewBlobStore(filepath.Join(root, "commits", "c1"))
	data := bytes.Repeat([]byte{0x33}, 2048)
	require.NoError(t, source.Write(7, data))
	require.NoError(t, source.Write(0x107, data))
	require.NoError(t, source.LinkAllTo(target))

	fromSmall, err := os.Stat(source.Path(7))
	require.NoError(t, err)
	toSmall, err := os.Stat(target.Path(7))
	require.NoError(t, err)
	require.True(t, os.SameFile(fromSmall, toSmall))

	fromSharded, err := os.Stat(source.Path(0x107))
	require.NoError(t, err)
	toSharded, err := os.Stat(target.Path(0x107))
	require.NoError(t, err)
	require.True(t, os.SameFile(fromSharded, toSharded))

	// 删除当前分支块只减少硬链接计数，不影响快照副本。
	require.NoError(t, source.Remove(7))
	require.False(t, source.Exists(7))
	got, err := target.Read(7, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
}

func TestBlobStoreSyncPersistsDirtyFilesAndDirectories(t *testing.T) {
	base := filepath.Join(t.TempDir(), "branches", "main")
	store := pebblekv.NewBlobStore(base)
	require.NoError(t, store.Write(1, []byte("abc")))
	require.NoError(t, store.Sync())
	require.NoError(t, store.Sync())
	require.NoError(t, store.Remove(1))
	require.NoError(t, store.Sync())

	reopened := pebblekv.NewBlobStore(base)
	require.False(t, reopened.Exists(1))
}

// TestBlobSyncSkipsTransientShards 覆盖单事务内多次 4 KiB CoW 后只保留存活块的持久化行为：
// 已消失的过渡块不得影响存活块的磁盘内容与热缓存。
func TestBlobSyncSkipsTransientShards(t *testing.T) {
	base := filepath.Join(t.TempDir(), "branches", "main")
	store := pebblekv.NewBlobStore(base)
	data := bytes.Repeat([]byte{0x7f}, 4096)

	// 单事务内多次 4 KiB CoW：每块写入后立刻被替换删除，散列到不同分片目录。
	require.NoError(t, store.Write(1, data))
	require.NoError(t, store.Remove(1))
	require.NoError(t, store.Write(2, data))
	require.NoError(t, store.Remove(2))

	// 窗口内尾部原地追加：块文件与热缓存都必须呈现追加后的完整内容。
	const liveID = 3
	require.NoError(t, store.Write(liveID, data))
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

	// 重新打开块存储：存活块内容必须与追加后的逻辑块逐字节一致。
	reopened := pebblekv.NewBlobStore(base)
	persisted, err := reopened.Read(liveID, len(full))
	require.NoError(t, err)
	require.Equal(t, full, persisted)
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
func TestBlobFdatasyncPath(t *testing.T) {
	base := filepath.Join(t.TempDir(), "branches", "main")
	store := pebblekv.NewBlobStore(base)
	data := bytes.Repeat([]byte{0x5a}, 8192)
	require.NoError(t, store.WriteForInode(10, 0x41, data))

	require.NoError(t, store.SyncInode(10))

	cold := pebblekv.NewBlobStore(base)
	got, err := cold.Read(0x41, len(data))
	require.NoError(t, err)
	require.Equal(t, data, got)
}
