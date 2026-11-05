package fusefs

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// TestUnlinkKeepsParentAttr 验证 Unlink 后父目录属性仍命中客户端缓存，
// 下一次内核 inode_permission(MAY_EXEC) 的 GETATTR 不再穿透到服务端。
func TestUnlinkKeepsParentAttr(t *testing.T) {
	client := &loopClient{
		unlinkFn: func(*storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			return &storagev1.UnlinkResponse{}, nil
		},
		getAttrFn: func(*storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			t.Fatal("Unlink 后父目录属性必须命中缓存，不得发起 GetAttr RPC")
			return nil, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	fs.cache.putInode(1, testInode(1, syscall.S_IFDIR|0o755, 2), 1, 2)

	require.Equal(t, fuse.OK, fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "file"))

	var attr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 1}}, &attr))
	require.Equal(t, uint32(2), attr.Nlink, "Unlink 不改变父目录 nlink")
}

// TestRmdirDecrementsParentNlink 验证 Rmdir 后父目录 nlink 减 1 且属性仍命中缓存。
func TestRmdirDecrementsParentNlink(t *testing.T) {
	client := &loopClient{
		rmdirFn: func(*storagev1.RmdirRequest) (*storagev1.RmdirResponse, error) {
			return &storagev1.RmdirResponse{}, nil
		},
		getAttrFn: func(*storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			t.Fatal("Rmdir 后父目录属性必须命中缓存，不得发起 GetAttr RPC")
			return nil, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	fs.cache.putInode(1, testInode(1, syscall.S_IFDIR|0o755, 3), 1, 2)

	require.Equal(t, fuse.OK, fs.Rmdir(nil, &fuse.InHeader{NodeId: 1}, "dir"))

	var attr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 1}}, &attr))
	require.Equal(t, uint32(2), attr.Nlink, "Rmdir 必须使父目录 nlink 减 1")
}

// TestRenameDirAcrossParentsAdjustsNlink 验证目录跨父目录移动时旧父 -1、新父 +1，
// 且两个父目录属性都保留在客户端缓存中。
func TestRenameDirAcrossParentsAdjustsNlink(t *testing.T) {
	client := &loopClient{
		renameFn: func(*storagev1.RenameRequest) (*storagev1.RenameResponse, error) {
			return &storagev1.RenameResponse{}, nil
		},
		getAttrFn: func(*storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			t.Fatal("Rename 后父目录属性必须命中缓存，不得发起 GetAttr RPC")
			return nil, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	fs.cache.putInode(1, testInode(1, syscall.S_IFDIR|0o755, 3), 1, 2)
	fs.cache.putInode(2, testInode(2, syscall.S_IFDIR|0o755, 2), 1, 2)
	fs.cache.putInode(20, testInode(20, syscall.S_IFDIR|0o755, 2), 1, 2)
	fs.cache.putEntry(1, "dir", 20)

	in := &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 1}, Newdir: 2}
	require.Equal(t, fuse.OK, fs.Rename(nil, in, "dir", "moved"))

	oldParent, ok := fs.cache.inode(1)
	require.True(t, ok)
	require.Equal(t, uint32(2), oldParent.inode.GetAttr().GetNlink(), "旧父目录 nlink 必须减 1")
	newParent, ok := fs.cache.inode(2)
	require.True(t, ok)
	require.Equal(t, uint32(3), newParent.inode.GetAttr().GetNlink(), "新父目录 nlink 必须加 1")

	var attr fuse.AttrOut
	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 1}}, &attr))
	require.Equal(t, uint32(2), attr.Nlink)
}

// TestRenameSameDirKeepsNlink 验证同目录重命名不改变父目录 nlink（目录本身没有移动）。
func TestRenameSameDirKeepsNlink(t *testing.T) {
	client := &loopClient{
		renameFn: func(*storagev1.RenameRequest) (*storagev1.RenameResponse, error) {
			return &storagev1.RenameResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	defer fs.OnUnmount()
	fs.cache.putInode(1, testInode(1, syscall.S_IFDIR|0o755, 3), 1, 2)
	fs.cache.putInode(20, testInode(20, syscall.S_IFDIR|0o755, 2), 1, 2)
	fs.cache.putEntry(1, "a", 20)

	in := &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 1}, Newdir: 1}
	require.Equal(t, fuse.OK, fs.Rename(nil, in, "a", "b"))

	parent, ok := fs.cache.inode(1)
	require.True(t, ok)
	require.Equal(t, uint32(3), parent.inode.GetAttr().GetNlink(), "同目录重命名不得改变父目录 nlink")

	// RENAME_EXCHANGE 同样不改变目录链接数。
	fs.cache.putEntry(1, "c", 20)
	exchange := &fuse.RenameIn{InHeader: fuse.InHeader{NodeId: 1}, Newdir: 1, Flags: renameExchange}
	require.Equal(t, fuse.OK, fs.Rename(nil, exchange, "c", "b"))
	parent, ok = fs.cache.inode(1)
	require.True(t, ok)
	require.Equal(t, uint32(3), parent.inode.GetAttr().GetNlink(), "RENAME_EXCHANGE 不得改变父目录 nlink")
}
