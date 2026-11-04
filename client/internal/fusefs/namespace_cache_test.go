package fusefs

import (
	"syscall"
	"testing"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/stretchr/testify/require"
)

func testInode(ino uint64, mode uint32, nlink uint32) *storagev1.Inode {
	return &storagev1.Inode{Ino: ino, Generation: 1, Attr: &storagev1.InodeAttribute{Mode: mode, Nlink: nlink}}
}

func TestNamespaceCache容量与最近使用淘汰(t *testing.T) {
	cache := NewNamespaceCache(3)
	cache.putInode(1, testInode(1, syscall.S_IFREG|0o644, 1), 1, 2)
	cache.putEntry(10, "缺失", 0)
	cache.putEntry(10, "文件", 1)
	_, ok := cache.inode(1)
	require.True(t, ok)
	cache.putEntry(10, "新文件", 2)
	_, ok = cache.entry(10, "缺失")
	require.False(t, ok)
	ino, ok := cache.entry(10, "文件")
	require.True(t, ok)
	require.Equal(t, uint64(1), ino)

	// 调用方修改返回的属性不得污染缓存。
	cached, ok := cache.inode(1)
	require.True(t, ok)
	cached.inode.Attr.Mode = 0
	again, ok := cache.inode(1)
	require.True(t, ok)
	require.NotZero(t, again.inode.Attr.Mode)
}

func TestNamespaceCache目录流和属性更新(t *testing.T) {
	cache := NewNamespaceCache(8)
	entries := []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o600, 1)},
	}
	cache.putDir(10, entries)
	entries[0].Name[0] = 'z'
	ino, ok := cache.entry(10, "a")
	require.True(t, ok)
	require.Equal(t, uint64(11), ino)
	ino, ok = cache.entry(10, "missing")
	require.True(t, ok)
	require.Zero(t, ino)
	updated := testInode(11, syscall.S_IFREG|0o700, 1)
	cache.putInode(11, updated, 0, 0)
	cached, ok := cache.dir(10)
	require.True(t, ok)
	require.Equal(t, updated.Attr.Mode, cached[0].Inode.Attr.Mode)
	cache.invalidateInode(11)
	_, ok = cache.dir(10)
	require.False(t, ok)

	// 大目录按目录项数计费，不能以一个缓存项绕过容量。
	cache = NewNamespaceCache(2)
	cache.putDir(10, entries)
	_, ok = cache.dir(10)
	require.False(t, ok)
}

func TestNamespaceCache目录属性失效时清除父目录流(t *testing.T) {
	cache := NewNamespaceCache(8)
	cache.putDir(1, []*storagev1.DirEntryPlus{{Ino: 10, Name: []byte("dir"), NextOffset: 1, Inode: testInode(10, syscall.S_IFDIR|0o755, 2)}})
	cache.invalidateDir(10)
	_, ok := cache.inode(10)
	require.False(t, ok)
}

func TestNamespaceCache定向索引失效与回填(t *testing.T) {
	cache := NewNamespaceCache(64)
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o644, 1)},
	})
	cache.putDir(20, []*storagev1.DirEntryPlus{
		{Ino: 12, Name: []byte("c"), NextOffset: 1, Inode: testInode(12, syscall.S_IFREG|0o644, 1)},
	})

	// inode() 通过目录流二级索引回退命中共享子节点。
	got, ok := cache.inode(12)
	require.True(t, ok)
	require.Equal(t, uint64(12), got.inode.GetIno())

	// putInode 定向更新所有包含该子节点的目录流。
	updated := testInode(12, syscall.S_IFREG|0o700, 1)
	cache.putInode(12, updated, 0, 0)
	d10, ok := cache.dir(10)
	require.True(t, ok)
	d20, ok := cache.dir(20)
	require.True(t, ok)
	require.Equal(t, uint32(syscall.S_IFREG|0o700), d10[1].Inode.Attr.Mode)
	require.Equal(t, uint32(syscall.S_IFREG|0o700), d20[0].Inode.Attr.Mode)

	// invalidateInode 只删除包含该子节点的目录流，并清除独立属性。
	cache.invalidateInode(12)
	_, ok = cache.dir(10)
	require.False(t, ok)
	_, ok = cache.dir(20)
	require.False(t, ok)
	_, ok = cache.inode(12)
	require.False(t, ok)
}

func TestNamespaceCache按父目录定向清除独立目录项(t *testing.T) {
	cache := NewNamespaceCache(16)
	cache.putEntry(10, "a", 11)
	cache.putEntry(10, "b", 0)
	cache.putEntry(20, "c", 12)
	cache.invalidateDir(10)
	_, ok := cache.entry(10, "a")
	require.False(t, ok)
	_, ok = cache.entry(10, "b")
	require.False(t, ok)
	ino, ok := cache.entry(20, "c")
	require.True(t, ok)
	require.Equal(t, uint64(12), ino)
}

func TestNamespaceCache目录流替换清除独立目录项(t *testing.T) {
	cache := NewNamespaceCache(16)
	cache.putEntry(10, "a", 11)
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
	})
	// 目录流写入后，独立目录项必须被移除，避免旧目录项覆盖新目录流。
	ino, ok := cache.entry(10, "a")
	require.True(t, ok)
	require.Equal(t, uint64(11), ino)
	cache.putEntry(10, "b", 12)
	// putEntry 清除父目录流，随后 inode() 只能通过独立属性命中。
	cache.putInode(12, testInode(12, syscall.S_IFREG|0o600, 1), 0, 0)
	_, ok = cache.dir(10)
	require.False(t, ok)
}

// 目录内新建/删除单个名字时，未变动的兄弟目录项必须保持命中，避免冷查询全部穿透到服务端。
func TestNamespaceCache定向失效保留兄弟目录项(t *testing.T) {
	cache := NewNamespaceCache(64)
	cache.putEntry(10, "a", 11)
	cache.putEntry(10, "b", 12)
	cache.putEntry(20, "c", 13)

	// 模拟 Create：只失效目录流与父属性，再回填新名字。
	cache.invalidateDirListing(10)
	cache.putEntryAt(^uint64(0), 10, "d", 14)
	for name, want := range map[string]uint64{"a": 11, "b": 12, "d": 14} {
		ino, ok := cache.entry(10, name)
		require.True(t, ok, "兄弟目录项 %s 不应被清除", name)
		require.Equal(t, want, ino)
	}

	// 模拟 Unlink：只定向删除被删名字。
	cache.invalidateDirListing(10)
	cache.removeEntry(10, "a")
	_, ok := cache.entry(10, "a")
	require.False(t, ok, "被删除的目录项必须失效")
	for name, want := range map[string]uint64{"b": 12, "d": 14} {
		ino, ok := cache.entry(10, name)
		require.True(t, ok, "未变动的兄弟目录项 %s 应保持命中", name)
		require.Equal(t, want, ino)
	}

	// 模拟 Rename：旧名精确失效，新名回填，兄弟项保留。
	cache.invalidateDirListing(10)
	cache.removeEntry(10, "b")
	cache.putEntryAt(^uint64(0), 10, "e", 12)
	_, ok = cache.entry(10, "b")
	require.False(t, ok, "改名后的旧名必须失效")
	ino, ok := cache.entry(10, "e")
	require.True(t, ok)
	require.Equal(t, uint64(12), ino)
	ino, ok = cache.entry(10, "d")
	require.True(t, ok)
	require.Equal(t, uint64(14), ino)

	// 其它父目录不受影响。
	ino, ok = cache.entry(20, "c")
	require.True(t, ok)
	require.Equal(t, uint64(13), ino)
}

// 父目录属性变化必须失效引用它的祖先目录流，但不影响其它目录的目录项。
func TestNamespaceCache定向失效清除父属性与祖先目录流(t *testing.T) {
	cache := NewNamespaceCache(64)
	cache.putInode(10, testInode(10, syscall.S_IFDIR|0o755, 2), 1, 1)
	cache.putDir(1, []*storagev1.DirEntryPlus{
		{Ino: 10, Name: []byte("dir"), NextOffset: 1, Inode: testInode(10, syscall.S_IFDIR|0o755, 2)},
	})
	cache.putEntry(10, "a", 11)

	cache.invalidateDirListing(10)
	_, ok := cache.inode(10)
	require.False(t, ok, "父目录属性必须失效")
	_, ok = cache.dir(1)
	require.False(t, ok, "引用父目录的祖先目录流必须失效")
	ino, ok := cache.entry(10, "a")
	require.True(t, ok, "父目录下的兄弟目录项应保留")
	require.Equal(t, uint64(11), ino)
}

// 默认容量必须覆盖 mdtest 1proc 的约 1.2 万节点工作集（含目录项与目录流）。
func TestNamespaceCache默认容量覆盖大规模工作集(t *testing.T) {
	require.GreaterOrEqual(t, DefaultNamespaceCacheCapacity, 65536)
	cache := NewNamespaceCache(0)
	for i := uint64(1); i <= 20000; i++ {
		cache.putInode(i, testInode(i, syscall.S_IFREG|0o644, 1), 0, 0)
	}
	// 超出旧容量 16384 后，最早写入的节点仍应留在缓存中。
	_, ok := cache.inode(1)
	require.True(t, ok)
	_, ok = cache.inode(20000)
	require.True(t, ok)
}
