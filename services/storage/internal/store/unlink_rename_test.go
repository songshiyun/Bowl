package store_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func assertKeyNotFound(t *testing.T, db *pebble.DB, key []byte) {
	t.Helper()
	_, closer, err := db.Get(key)
	if err == nil {
		_ = closer.Close()
	}
	require.ErrorIs(t, err, pebble.ErrNotFound)
}

func TestRmdirEmptyNonEmptyAndErrorPaths(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-rmdir",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 创建子目录 dirA，并在 dirA 中创建常规文件 child.txt，预置 dirA 的 XattrKey
	dirAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dirA"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	rootAttrBefore, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint32(3), rootAttrBefore.Nlink)

	childAttr, err := actor.Create(store.CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("child.txt"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	require.NoError(t, actor.SetXattr(dirAttr.Ino, []byte("user.tag"), []byte("v1"), 0))
	require.NoError(t, actor.SetXattr(dirAttr.Ino, []byte("user.meta"), []byte("v2"), 0))

	// 2. 非空目录 Rmdir 必须返回 ErrNotEmpty，且不破坏目录或统计
	err = actor.Rmdir(store.RootIno, []byte("dirA"))
	require.ErrorIs(t, err, store.ErrNotEmpty)

	// 3. Rmdir 目标为非目录返回 ErrNotDir，"." 返回 ErrInvalid，".." 返回 ErrNotEmpty，不存在返回 ErrNotFound
	err = actor.Rmdir(dirAttr.Ino, []byte("child.txt"))
	require.ErrorIs(t, err, store.ErrNotDir)

	err = actor.Rmdir(childAttr.Ino, []byte("sub"))
	require.ErrorIs(t, err, store.ErrNotDir)

	err = actor.Rmdir(store.RootIno, []byte("."))
	require.ErrorIs(t, err, store.ErrInvalid)

	err = actor.Rmdir(store.RootIno, []byte(".."))
	require.ErrorIs(t, err, store.ErrNotEmpty)

	err = actor.Rmdir(store.RootIno, []byte("missing"))
	require.ErrorIs(t, err, store.ErrNotFound)

	// 4. 先移除子项使 dirA 变为空目录，再执行 Rmdir("dirA")
	require.NoError(t, actor.Unlink(dirAttr.Ino, []byte("child.txt"), false))

	require.NoError(t, actor.Rmdir(store.RootIno, []byte("dirA")))

	// 5. 直接检查 EntryKey、InodeKey 与全部关联 XattrKey 已从 Pebble 删除，父目录 Nlink 与 StatFS.Files 已递减
	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("dirA")))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(dirAttr.Ino))
	assertKeyNotFound(t, actor.DB(), store.XattrKey(dirAttr.Ino, []byte("user.tag")))
	assertKeyNotFound(t, actor.DB(), store.XattrKey(dirAttr.Ino, []byte("user.meta")))

	rootAttrAfter, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint32(2), rootAttrAfter.Nlink)

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(1), st.Files)
}

func seedFileWithBlobsAndXattr(t *testing.T, actor *store.StoreActor, ino uint64, xattrName string, blockLens map[uint64]uint32) uint64 {
	t.Helper()
	if xattrName != "" {
		require.NoError(t, actor.SetXattr(ino, []byte(xattrName), []byte("val"), 0))
	}
	var maxEnd uint64
	for idx, length := range blockLens {
		off := idx * store.BlobSize
		_, _, err := actor.Write(ino, off, bytes.Repeat([]byte{0x5a}, int(length)))
		require.NoError(t, err)
		if end := off + uint64(length); end > maxEnd {
			maxEnd = end
		}
	}
	return maxEnd
}

func assertKeyExists(t *testing.T, db *pebble.DB, key []byte) {
	t.Helper()
	_, closer, err := db.Get(key)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
}

func TestUnlinkHardlinksOrphanLifecycleAndDataReclaim(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-unlink",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 错误分支校验：目录返 ErrIsDir、"."/".." 返 ErrInvalid、不存在返 ErrNotFound
	dirAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dir1"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	require.ErrorIs(t, actor.Unlink(store.RootIno, []byte("dir1"), false), store.ErrIsDir)
	require.ErrorIs(t, actor.Unlink(store.RootIno, []byte("."), false), store.ErrInvalid)
	require.ErrorIs(t, actor.Unlink(store.RootIno, []byte(".."), false), store.ErrInvalid)
	require.ErrorIs(t, actor.Unlink(store.RootIno, []byte("no-such"), false), store.ErrNotFound)

	// 2. 创建文件 f1 并建立硬链接 f1_link (Nlink=2)，预置 Size=4096、XattrKey 与 2 个 DataKey
	f1Attr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	_, err = actor.Link(store.LinkReq{
		Ino:          f1Attr.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("f1_link"),
	})
	require.NoError(t, err)

	f1Size := seedFileWithBlobsAndXattr(t, actor, f1Attr.Ino, "user.k1", map[uint64]uint32{
		0: 2048,
		1: 2048,
	})

	// 3. Unlink("f1")：Nlink 从 2 降为 1，仅更新 InodeKey(Ctime)，不写 OrphanKey，不扣减统计或删 DataKey
	require.NoError(t, actor.Unlink(store.RootIno, []byte("f1"), false))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("f1")))
	assertKeyNotFound(t, actor.DB(), store.OrphanKey(f1Attr.Ino))
	assertKeyExists(t, actor.DB(), store.DataKey(f1Attr.Ino, 0))
	assertKeyExists(t, actor.DB(), store.DataKey(f1Attr.Ino, 1))

	f1AfterFirstUnlink, err := actor.GetAttr(f1Attr.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(1), f1AfterFirstUnlink.Nlink)
	require.Equal(t, f1Size, f1AfterFirstUnlink.Size)

	// 4. Unlink("f1_link", openHandle=true)：Nlink 降为 0，写入 OrphanKey(ino)，保留 Inode、DataKey 与 Size 不扣减逻辑空间
	require.NoError(t, actor.Unlink(store.RootIno, []byte("f1_link"), true))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("f1_link")))
	assertKeyExists(t, actor.DB(), store.OrphanKey(f1Attr.Ino))
	assertKeyExists(t, actor.DB(), store.DataKey(f1Attr.Ino, 0))
	assertKeyExists(t, actor.DB(), store.DataKey(f1Attr.Ino, 1))

	orphanAttr, err := actor.GetAttr(f1Attr.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(0), orphanAttr.Nlink)
	require.Equal(t, f1Size, orphanAttr.Size)

	stBeforePurge, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(3), stBeforePurge.Files) // Root + dir1 + orphaned f1
	require.Equal(t, f1Size, stBeforePurge.UsedBytes)

	// 5. PurgeOrphanInode(f1Attr.Ino)：清理 OrphanKey、InodeKey、XattrKey、DataKey 并扣减统计
	require.NoError(t, actor.PurgeOrphanInode(f1Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.OrphanKey(f1Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(f1Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.XattrKey(f1Attr.Ino, []byte("user.k1")))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f1Attr.Ino, 0))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f1Attr.Ino, 1))

	stAfterPurge, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(2), stAfterPurge.Files) // Root + dir1
	require.Equal(t, uint64(0), stAfterPurge.UsedBytes)

	// 重复 PurgeOrphanInode 幂等成功；对存活节点 PurgeOrphanInode 返回 ErrInvalid
	require.NoError(t, actor.PurgeOrphanInode(f1Attr.Ino))
	require.ErrorIs(t, actor.PurgeOrphanInode(dirAttr.Ino), store.ErrInvalid)

	// 6. Unlink(openHandle=false) 直接删除 InodeKey、XattrKey、OrphanKey、DataKey 并回收配额
	f2Attr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f2"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	seedFileWithBlobsAndXattr(t, actor, f2Attr.Ino, "user.k2", map[uint64]uint32{
		0: 1024,
	})

	require.NoError(t, actor.Unlink(store.RootIno, []byte("f2"), false))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("f2")))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(f2Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.XattrKey(f2Attr.Ino, []byte("user.k2")))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f2Attr.Ino, 0))

	// 7. CleanOrphans 批量回收多个孤儿节点
	o1Attr, err := actor.Symlink(store.SymlinkReq{
		ParentIno: store.RootIno,
		Name:      []byte("sym1"),
		Target:    []byte("/target/one"),
	})
	require.NoError(t, err)
	o2Attr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("orphan2"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	seedFileWithBlobsAndXattr(t, actor, o2Attr.Ino, "", map[uint64]uint32{
		0: 512,
	})

	require.NoError(t, actor.Unlink(store.RootIno, []byte("sym1"), true))
	require.NoError(t, actor.Unlink(store.RootIno, []byte("orphan2"), true))

	require.NoError(t, actor.CleanOrphans())
	assertKeyNotFound(t, actor.DB(), store.OrphanKey(o1Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(o1Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.OrphanKey(o2Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(o2Attr.Ino))
	assertKeyNotFound(t, actor.DB(), store.DataKey(o2Attr.Ino, 0))

	stFinal, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(2), stFinal.Files) // Root + dir1
	require.Equal(t, uint64(0), stFinal.UsedBytes)
}

func TestRenameOverwriteNoreplaceAndOrphanTarget(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-rename",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	dirA, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dirA"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	dirB, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dirB"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	// 1. RenameNoreplace：目标不存在时成功，目标已存在（含自身重命名）返回 ErrExist
	f1, err := actor.Create(store.CreateReq{
		ParentIno: dirA.Ino,
		Name:      []byte("f1"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	f2, err := actor.Create(store.CreateReq{
		ParentIno: dirB.Ino,
		Name:      []byte("f2"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("f1"),
		NewParentIno: dirA.Ino,
		NewName:      []byte("f1"),
		Flags:        store.RenameNoreplace,
	}), store.ErrExist)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("f1"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("f2"),
		Flags:        store.RenameNoreplace,
	}), store.ErrExist)

	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno: dirA.Ino,
		OldName:      []byte("f1"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("f1_moved"),
		Flags:        store.RenameNoreplace,
	}))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(dirA.Ino, []byte("f1")))
	movedEntry, movedAttr, err := actor.Lookup(dirB.Ino, []byte("f1_moved"))
	require.NoError(t, err)
	require.Equal(t, f1.Ino, movedEntry.Ino)
	require.Equal(t, f1.Ino, movedAttr.Ino)

	// 2. 默认覆盖非目录且 TargetOpenHandle=false：立即删除被覆盖目标的 InodeKey、XattrKey 与 DataKey
	seedFileWithBlobsAndXattr(t, actor, f2.Ino, "user.f2", map[uint64]uint32{
		0: 2048,
	})

	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno:     dirB.Ino,
		OldName:          []byte("f1_moved"),
		NewParentIno:     dirB.Ino,
		NewName:          []byte("f2"),
		TargetOpenHandle: false,
	}))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(dirB.Ino, []byte("f1_moved")))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(f2.Ino))
	assertKeyNotFound(t, actor.DB(), store.XattrKey(f2.Ino, []byte("user.f2")))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f2.Ino, 0))

	// 3. 默认覆盖非目录且 TargetOpenHandle=true：被覆盖目标转为 OrphanKey，保留 Inode、DataKey 与配额直至 PurgeOrphanInode
	f3, err := actor.Create(store.CreateReq{
		ParentIno: dirB.Ino,
		Name:      []byte("f3"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	seedFileWithBlobsAndXattr(t, actor, f3.Ino, "user.f3", map[uint64]uint32{
		0: 1024,
	})

	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno:     dirB.Ino,
		OldName:          []byte("f2"),
		NewParentIno:     dirB.Ino,
		NewName:          []byte("f3"),
		TargetOpenHandle: true,
	}))
	assertKeyExists(t, actor.DB(), store.OrphanKey(f3.Ino))
	assertKeyExists(t, actor.DB(), store.DataKey(f3.Ino, 0))
	f3OrphanAttr, err := actor.GetAttr(f3.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(0), f3OrphanAttr.Nlink)

	require.NoError(t, actor.PurgeOrphanInode(f3.Ino))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(f3.Ino))
	assertKeyNotFound(t, actor.DB(), store.OrphanKey(f3.Ino))
	assertKeyNotFound(t, actor.DB(), store.DataKey(f3.Ino, 0))

	// 4. 目录覆盖类型校验与空目录覆盖：非目录覆盖目录返 ErrIsDir，目录覆盖非目录返 ErrNotDir，目录覆盖非空目录返 ErrNotEmpty
	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: dirB.Ino,
		OldName:      []byte("f3"),
		NewParentIno: store.RootIno,
		NewName:      []byte("dirA"),
	}), store.ErrIsDir)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("f3"),
	}), store.ErrNotDir)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: store.RootIno,
		NewName:      []byte("dirB"),
	}), store.ErrNotEmpty)

	// 将 dirA 移入 dirB 下成为子目录 dirB/subA，再覆盖 dirB 下的空目录 emptyDir
	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: dirB.Ino,
		NewName:      []byte("subA"),
	}))
	rootAttr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint32(3), rootAttr.Nlink) // Root(2) + dirB(1)
	dirBAttr, err := actor.GetAttr(dirB.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(3), dirBAttr.Nlink) // dirB(2) + subA(1)

	emptyDir, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("emptyDir"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	require.NoError(t, actor.SetXattr(emptyDir.Ino, []byte("user.empty"), []byte("1"), 0))

	// 跨父目录将 dirB/subA 覆盖到 Root/emptyDir
	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno: dirB.Ino,
		OldName:      []byte("subA"),
		NewParentIno: store.RootIno,
		NewName:      []byte("emptyDir"),
	}))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(emptyDir.Ino))
	assertKeyNotFound(t, actor.DB(), store.XattrKey(emptyDir.Ino, []byte("user.empty")))
	dirBAttrAfter, err := actor.GetAttr(dirB.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(2), dirBAttrAfter.Nlink)
	rootAttrAfter, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint32(4), rootAttrAfter.Nlink) // 根目录基础链接数 2 加上 dirB 与 emptyDir 两个子目录共 4 个链接
}

func TestRenameExchangeAndSubdirCyclePrevention(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-exchange",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	dirA, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dirA"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	dirSub, err := actor.Mkdir(store.MkdirReq{
		ParentIno: dirA.Ino,
		Name:      []byte("dirSub"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	fileRoot, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("fileRoot"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 1. 子目录环路防御：将 dirA 移入自身子孙 dirSub 必须返回 ErrInvalid；RenameExchange 构成环路同样返回 ErrInvalid
	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: dirSub.Ino,
		NewName:      []byte("cycle"),
	}), store.ErrInvalid)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: dirA.Ino,
		NewName:      []byte("dirSub"),
		Flags:        store.RenameExchange,
	}), store.ErrInvalid)

	// 2. RenameExchange 目标不存在返回 ErrNotFound；互斥或不支持的 Flags 返回对应错误
	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("fileRoot"),
		NewParentIno: store.RootIno,
		NewName:      []byte("missing"),
		Flags:        store.RenameExchange,
	}), store.ErrNotFound)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("fileRoot"),
		NewParentIno: store.RootIno,
		NewName:      []byte("dirA"),
		Flags:        store.RenameNoreplace | store.RenameExchange,
	}), store.ErrInvalid)

	require.ErrorIs(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("fileRoot"),
		NewParentIno: store.RootIno,
		NewName:      []byte("dirA"),
		Flags:        store.RenameWhiteout,
	}), store.ErrNotSupported)

	// 3. 跨父目录交换非目录 (Root/fileRoot) 与目录 (dirA/dirSub)：
	// 交换后 Root/"fileRoot" 指向 dirSub (ParentIno 变为 RootIno)，dirA/"dirSub" 指向 fileRoot；
	// Root.Nlink 从 3 增至 4，dirA.Nlink 从 3 减至 2
	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("fileRoot"),
		NewParentIno: dirA.Ino,
		NewName:      []byte("dirSub"),
		Flags:        store.RenameExchange,
	}))

	swappedRootEntry, _, err := actor.Lookup(store.RootIno, []byte("fileRoot"))
	require.NoError(t, err)
	require.Equal(t, dirSub.Ino, swappedRootEntry.Ino)

	swappedDirAEntry, _, err := actor.Lookup(dirA.Ino, []byte("dirSub"))
	require.NoError(t, err)
	require.Equal(t, fileRoot.Ino, swappedDirAEntry.Ino)

	rootAttr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint32(4), rootAttr.Nlink)

	dirAAttr, err := actor.GetAttr(dirA.Ino)
	require.NoError(t, err)
	require.Equal(t, uint32(2), dirAAttr.Nlink)

	// 验证交换后原 dirSub (现位于 Root/"fileRoot") 的 ParentIno 已更新为 RootIno：
	// 此时将 dirA 移入 dirSub 下合法（不再是子孙环路）
	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno: store.RootIno,
		OldName:      []byte("dirA"),
		NewParentIno: dirSub.Ino,
		NewName:      []byte("movedDirA"),
	}))
}

func TestUnlinkRenameIntraBatchReadYourWritesAndPhaseAIsolation(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-ryow",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	dirSub, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dirSub"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	fInit, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f_init"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 用门控命令占住当前 Tick，使后续 6 条命令在下一轮 Tick 内合并执行
	release := store.HoldTickForTest(actor)

	// 按序入队 6 条命令：
	// 1. Rename: Root/"f_init" -> Root/"f_step1"
	// 2. Rename: Root/"f_step1" -> dirSub/"f_step2"（依赖同批命令 1 的写入）
	// 3. RenameNoreplace: 尝试覆盖已存在的 dirSub/"f_step2" -> Phase A 失败返回 ErrExist（0 污染）
	// 4. Rmdir: 尝试删除非空的 dirSub -> Phase A 失败返回 ErrNotEmpty（0 污染）
	// 5. Unlink: 删除 dirSub/"f_step2"（依赖同批命令 2 的写入，使 dirSub 在同批内变为空目录）
	// 6. Rmdir: 删除已变空的 dirSub（依赖同批命令 5 的 DeleteDeferred）
	type asyncStep struct {
		fn   func() error
		done chan error
	}
	steps := []asyncStep{
		{
			fn: func() error {
				return actor.Rename(store.RenameReq{
					OldParentIno: store.RootIno,
					OldName:      []byte("f_init"),
					NewParentIno: store.RootIno,
					NewName:      []byte("f_step1"),
				})
			},
			done: make(chan error, 1),
		},
		{
			fn: func() error {
				return actor.Rename(store.RenameReq{
					OldParentIno: store.RootIno,
					OldName:      []byte("f_step1"),
					NewParentIno: dirSub.Ino,
					NewName:      []byte("f_step2"),
				})
			},
			done: make(chan error, 1),
		},
		{
			fn: func() error {
				return actor.Rename(store.RenameReq{
					OldParentIno: dirSub.Ino,
					OldName:      []byte("f_step2"),
					NewParentIno: dirSub.Ino,
					NewName:      []byte("f_step2"),
					Flags:        store.RenameNoreplace,
				})
			},
			done: make(chan error, 1),
		},
		{
			fn: func() error {
				return actor.Rmdir(store.RootIno, []byte("dirSub"))
			},
			done: make(chan error, 1),
		},
		{
			fn: func() error {
				return actor.Unlink(dirSub.Ino, []byte("f_step2"), false)
			},
			done: make(chan error, 1),
		},
		{
			fn: func() error {
				return actor.Rmdir(store.RootIno, []byte("dirSub"))
			},
			done: make(chan error, 1),
		},
	}

	for _, st := range steps {
		go func(s asyncStep) {
			s.done <- s.fn()
		}(st)
		// 保持入队顺序
		require.Eventually(t, func() bool { return true }, 10*time.Millisecond, time.Millisecond)
		time.Sleep(3 * time.Millisecond)
	}

	ticksBefore := actor.TickCount()
	release()

	require.NoError(t, <-steps[0].done)
	require.NoError(t, <-steps[1].done)
	require.ErrorIs(t, <-steps[2].done, store.ErrExist)
	require.ErrorIs(t, <-steps[3].done, store.ErrNotEmpty)
	require.NoError(t, <-steps[4].done)
	require.NoError(t, <-steps[5].done)

	// gate 占 1 个 Tick，6 条命令合并在紧随其后的 1 个 Tick 内完成
	require.LessOrEqual(t, actor.TickCount()-ticksBefore, uint64(2))

	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("f_init")))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("f_step1")))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(dirSub.Ino, []byte("f_step2")))
	assertKeyNotFound(t, actor.DB(), store.EntryKey(store.RootIno, []byte("dirSub")))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(fInit.Ino))
	assertKeyNotFound(t, actor.DB(), store.InodeKey(dirSub.Ino))

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(1), st.Files) // 仅剩 RootIno
}
