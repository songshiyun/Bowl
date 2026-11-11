package fusefs

import (
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func testInode(ino uint64, mode, nlink uint32) *storagev1.Inode {
	return &storagev1.Inode{Ino: ino, Generation: 1, Attr: &storagev1.InodeAttribute{Mode: mode, Nlink: nlink}}
}

func TestNamespaceCache权重淘汰与深拷贝隔离(t *testing.T) {
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

	cached, ok := cache.inode(1)
	require.True(t, ok)
	cached.inode.Attr.Mode = 0
	again, ok := cache.inode(1)
	require.True(t, ok)
	require.NotZero(t, again.inode.Attr.Mode)

	entries := []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o600, 1)},
	}
	overCapCache := NewNamespaceCache(2)
	overCapCache.putDir(10, entries)
	_, ok = overCapCache.dir(10)
	require.False(t, ok, "目录流按项数加权超出容量时不得绕过上限入缓存")
}

func TestNamespaceCache跨目录共享节点二级索引更新与失效(t *testing.T) {
	cache := NewNamespaceCache(64)
	entries10 := []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o644, 2)},
	}
	cache.putDir(10, entries10)
	entries10[0].Name[0] = 'z'
	ino, ok := cache.entry(10, "a")
	require.True(t, ok)
	require.Equal(t, uint64(11), ino)

	cache.putDir(20, []*storagev1.DirEntryPlus{
		{Ino: 12, Name: []byte("c"), NextOffset: 1, Inode: testInode(12, syscall.S_IFREG|0o644, 2)},
	})

	got, ok := cache.inode(12)
	require.True(t, ok)
	require.Equal(t, uint64(12), got.inode.GetIno())

	updated := testInode(12, syscall.S_IFREG|0o700, 2)
	cache.putInode(12, updated, 0, 0)
	d10, ok := cache.dir(10)
	require.True(t, ok)
	d20, ok := cache.dir(20)
	require.True(t, ok)
	require.Equal(t, uint32(syscall.S_IFREG|0o700), d10[1].Inode.Attr.Mode)
	require.Equal(t, uint32(syscall.S_IFREG|0o700), d20[0].Inode.Attr.Mode)

	cache.invalidateInode(12)
	_, ok = cache.dir(10)
	require.False(t, ok)
	_, ok = cache.dir(20)
	require.False(t, ok)
	_, ok = cache.inode(12)
	require.False(t, ok)
}

func TestNamespaceCache完整目录降级阈值与权威ENOENT撤销(t *testing.T) {
	cache := NewNamespaceCache(64)
	epoch := cache.version()
	cache.markNewDirAt(epoch, 30)
	ino, ok := cache.entry(30, "missing")
	require.True(t, ok)
	require.Zero(t, ino)

	cache.putDir(1, []*storagev1.DirEntryPlus{
		{Ino: 10, Name: []byte("dir"), NextOffset: 1, Inode: testInode(10, syscall.S_IFDIR|0o755, 2)},
	})
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o644, 1)},
	})

	cache.invalidateDirListing(10)
	_, ok = cache.entry(1, "missing")
	require.False(t, ok, "祖先目录流被动移除后必须同步撤销权威完整标记")

	ino, ok = cache.entry(10, "a")
	require.True(t, ok)
	require.Equal(t, uint64(11), ino)
	ino, ok = cache.entry(10, "missing")
	require.True(t, ok, "未超容量四分之一时降级保留权威完整标记")
	require.Zero(t, ino)

	cache.removeEntry(10, "a")
	_, ok = cache.entry(10, "other")
	require.False(t, ok, "删除目录项后必须撤销权威完整标记")

	smallCache := NewNamespaceCache(4)
	smallCache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o644, 1)},
	})
	smallCache.invalidateDirListing(10)
	_, ok = smallCache.entry(10, "missing")
	require.False(t, ok, "目录项数超过容量四分之一时不降级且不得声称完整")
}

func TestNamespaceCache父目录nlink下溢保护与过期世代拒绝回填(t *testing.T) {
	cache := NewNamespaceCache(64)
	parent := testInode(10, syscall.S_IFDIR|0o755, 2)
	parent.Attr.Mtime = &timestamppb.Timestamp{Seconds: 100}
	parent.Attr.Ctime = &timestamppb.Timestamp{Seconds: 100}
	cache.putInode(10, parent, 1, 2)

	child := testInode(11, syscall.S_IFDIR|0o755, 2)
	child.Attr.Ctime = &timestamppb.Timestamp{Seconds: 200, Nanos: 7}
	child.Attr.Mtime = &timestamppb.Timestamp{Seconds: 200, Nanos: 7}

	epoch := cache.version()
	cache.invalidateDirListingTouchParentAt(epoch, 10, child, 1, 3)
	got, ok := cache.inode(10)
	require.True(t, ok)
	require.Equal(t, uint32(3), got.inode.Attr.Nlink)
	require.Equal(t, int64(200), got.inode.Attr.Mtime.Seconds)
	require.InDelta(t, float64(3), got.attrTimeout, 1e-9)

	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)
	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)
	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)
	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)
	got, ok = cache.inode(10)
	require.True(t, ok)
	require.Zero(t, got.inode.Attr.Nlink, "父目录 nlink 递减不得发生无符号下溢")

	cache.invalidateDirListingTouchParentAt(epoch, 10, &storagev1.Inode{Ino: 12}, 0, 0)
	_, ok = cache.inode(10)
	require.False(t, ok, "子节点缺失属性时必须保守失效父属性")

	cache.putInode(20, testInode(20, syscall.S_IFDIR|0o755, 2), 1, 2)
	cache.invalidateDirListingTouchParentNowAt(cache.version()+1, 20, 0)
	_, ok = cache.inode(20)
	require.False(t, ok, "世代号不匹配时必须拒绝用陈旧响应回填父目录属性")
}
