package pebblekv

import (
	"bytes"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestHotShardByteAccounting 直接校验分片热缓存的字节计数守恒：
// 400 × 128 KiB 必须精确等于 50 MiB；单分片超预算时只保留 32 块 / 4 MiB；
// Remove 必须精确扣减被剔除块的字节数。
func TestHotShardByteAccounting(t *testing.T) {
	const blockSize = 128 << 10

	t.Run("400 blocks stay resident with exact byte total", func(t *testing.T) {
		store := NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
		for i := 1; i <= 400; i++ {
			require.NoError(t, store.Write(uint64(i), bytes.Repeat([]byte{byte(i)}, blockSize)))
		}

		total := 0
		for i := range store.hot {
			sh := &store.hot[i]
			sh.mu.Lock()
			require.LessOrEqual(t, sh.bytes, hotBlobShardBudget, "分片字节数不得超过预算")
			total += sh.bytes
			sh.mu.Unlock()
		}
		require.Equal(t, 400*blockSize, total, "50 MiB 热缓存字节计数必须精确守恒")
	})

	t.Run("shard budget eviction keeps exact byte count", func(t *testing.T) {
		store := NewBlobStore(filepath.Join(t.TempDir(), "branches", "main"))
		for i := 0; i < 40; i++ {
			require.NoError(t, store.Write(uint64(1+i*16), bytes.Repeat([]byte{byte(i + 1)}, blockSize)))
		}

		sh := store.hotShardFor(1)
		sh.mu.Lock()
		require.Equal(t, hotBlobShardBudget, sh.bytes, "5 MiB 写入后必须精确淘汰到 4 MiB")
		require.Equal(t, hotBlobSlotsPerShard/2, sh.count, "4 MiB / 128 KiB = 32 块")
		sh.mu.Unlock()

		require.NoError(t, store.Remove(uint64(1+39*16)))
		sh.mu.Lock()
		require.Equal(t, hotBlobShardBudget-blockSize, sh.bytes, "Remove 必须精确扣减被剔除块的字节数")
		require.Equal(t, hotBlobSlotsPerShard/2-1, sh.count)
		sh.mu.Unlock()
	})
}
