package fusefs

import (
	"syscall"
	"testing"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"
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

// 新建目录登记为权威完整目录后，缺失名字必须直接判定 ENOENT，不再穿透网络 Lookup。
func TestNamespaceCache新建目录权威判定ENOENT(t *testing.T) {
	cache := NewNamespaceCache(64)
	epoch := cache.version()
	cache.markNewDirAt(epoch, 10)

	ino, ok := cache.entry(10, "missing")
	require.True(t, ok, "新建空目录的缺失名字必须权威命中")
	require.Zero(t, ino)

	// 目录内创建文件后，已知名字命中，其它名字仍权威 ENOENT。
	cache.putEntryAt(epoch, 10, "file", 11)
	ino, ok = cache.entry(10, "file")
	require.True(t, ok)
	require.Equal(t, uint64(11), ino)
	ino, ok = cache.entry(10, "other")
	require.True(t, ok)
	require.Zero(t, ino)
}

// 目录流失效后必须降级为独立目录项并保持权威完整：兄弟项继续命中，缺失名仍 ENOENT。
func TestNamespaceCache目录流降级保留权威完整(t *testing.T) {
	cache := NewNamespaceCache(64)
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o644, 1)},
	})
	_, ok := cache.dir(10)
	require.True(t, ok)

	cache.invalidateDirListing(10)
	for name, want := range map[string]uint64{"a": 11, "b": 12} {
		ino, ok := cache.entry(10, name)
		require.True(t, ok, "降级后兄弟项 %s 必须保持命中", name)
		require.Equal(t, want, ino)
	}
	ino, ok := cache.entry(10, "missing")
	require.True(t, ok, "完整降级后缺失名字必须权威 ENOENT")
	require.Zero(t, ino)

	// 显式删除目录项后不得再声称目录内容完整。
	cache.removeEntry(10, "a")
	_, ok = cache.entry(10, "other")
	require.False(t, ok, "删除目录项后必须撤销权威完整标记")
}

// 目录流条目超过容量四分之一时不降级，也不得声称内容完整。
func TestNamespaceCache大目录降级不声称完整(t *testing.T) {
	cache := NewNamespaceCache(4)
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
		{Ino: 12, Name: []byte("b"), NextOffset: 2, Inode: testInode(12, syscall.S_IFREG|0o644, 1)},
	})
	cache.invalidateDirListing(10)
	_, ok := cache.entry(10, "missing")
	require.False(t, ok, "未能完整降级时不得权威判定 ENOENT")
}

// 祖先目录流被失效后，其权威完整标记必须一并撤销，避免把仍存在的名字误判为 ENOENT。
func TestNamespaceCache祖先目录流失效撤销完整标记(t *testing.T) {
	cache := NewNamespaceCache(64)
	cache.putDir(1, []*storagev1.DirEntryPlus{
		{Ino: 10, Name: []byte("dir"), NextOffset: 1, Inode: testInode(10, syscall.S_IFDIR|0o755, 2)},
	})
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("a"), NextOffset: 1, Inode: testInode(11, syscall.S_IFREG|0o644, 1)},
	})
	cache.invalidateDirListing(10)
	_, ok := cache.entry(1, "missing")
	require.False(t, ok, "祖先目录流被移除后不得再声称内容完整")
}

// Create/Mkdir 等操作返回的子节点时间戳必须就地刷新父目录属性，
// 避免内核随后触发的 FUSE_GETATTR(parent) 再发一次网络 RPC。
func TestNamespaceCache刷新父目录属性(t *testing.T) {
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
	require.True(t, ok, "父目录属性必须就地刷新而不是被清除")
	require.Equal(t, uint32(3), got.inode.Attr.Nlink)
	require.Equal(t, int64(200), got.inode.Attr.Mtime.Seconds)
	require.Equal(t, int32(7), got.inode.Attr.Mtime.Nanos)
	require.Equal(t, int64(200), got.inode.Attr.Ctime.Seconds)
	require.Equal(t, float64(3), got.attrTimeout)
	require.Equal(t, float64(1), got.entryTimeout)

	// nlink 递减不得下溢到 0 以下。
	cache.invalidateDirListingTouchParentAt(epoch, 10, child, -1, 0)
	got, ok = cache.inode(10)
	require.True(t, ok)
	require.Equal(t, uint32(2), got.inode.Attr.Nlink)
	require.Equal(t, float64(3), got.attrTimeout, "attrTimeout<=0 时必须沿用父目录原有有效期")

	// 子节点属性缺失时无法刷新父属性，只能保守失效。
	cache.invalidateDirListingTouchParentAt(epoch, 10, &storagev1.Inode{Ino: 12}, 0, 0)
	_, ok = cache.inode(10)
	require.False(t, ok, "无法刷新时必须保守失效父属性")
}

// Unlink/Rmdir/Rename 不返回子节点时间戳，使用当前时间刷新父目录属性并保留父目录 Inode 缓存。
func TestNamespaceCacheTouchParentNowKeepsInode(t *testing.T) {
	cache := NewNamespaceCache(64)
	parent := testInode(10, syscall.S_IFDIR|0o755, 3)
	parent.Attr.Mtime = &timestamppb.Timestamp{Seconds: 100}
	parent.Attr.Ctime = &timestamppb.Timestamp{Seconds: 100}
	cache.putInode(10, parent, 1, 2)
	cache.putDir(10, []*storagev1.DirEntryPlus{
		{Ino: 11, Name: []byte("sub"), NextOffset: 1, Inode: testInode(11, syscall.S_IFDIR|0o755, 2)},
	})

	epoch := cache.version()
	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)

	got, ok := cache.inode(10)
	require.True(t, ok, "Rmdir/Unlink/Rename 后父目录 Inode 必须保留")
	require.Equal(t, uint32(2), got.inode.Attr.Nlink)
	require.Greater(t, got.inode.Attr.Mtime.Seconds, int64(100))
	require.Greater(t, got.inode.Attr.Ctime.Seconds, int64(100))
	require.Equal(t, float64(2), got.attrTimeout)
	require.Equal(t, float64(1), got.entryTimeout)

	// 目录流本身已被降级为独立目录项，不再命中完整目录流。
	_, ok = cache.dir(10)
	require.False(t, ok)
	ino, ok := cache.entry(10, "sub")
	require.True(t, ok)
	require.Equal(t, uint64(11), ino)

	// nlink 递减不得下溢。
	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)
	cache.invalidateDirListingTouchParentNowAt(epoch, 10, -1)
	got, ok = cache.inode(10)
	require.True(t, ok)
	require.Zero(t, got.inode.Attr.Nlink)

	// epoch 已过期时不得用旧会话的响应回填父目录属性。
	cache.putInode(20, testInode(20, syscall.S_IFDIR|0o755, 2), 1, 2)
	cache.invalidateDirListingTouchParentNowAt(cache.version()+1, 20, 0)
	_, ok = cache.inode(20)
	require.False(t, ok, "epoch 过期时必须放弃回填父目录属性")
}
