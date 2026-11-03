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
