package store_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestMkdirCreateMknodSymlinkAndLookupReadlink(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-ns",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. Mkdir 创建带 S_ISGID 的子目录，验证父目录 Nlink 递增 (2 -> 3) 与 umask 过滤
	dirAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("sub"),
		Mode:      store.S_IFDIR | store.S_ISGID | 0o777,
		UID:       1000,
		GID:       2000,
		Umask:     0o022,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), dirAttr.Ino)
	require.Equal(t, store.RootIno, dirAttr.ParentIno)
	require.Equal(t, store.S_IFDIR|store.S_ISGID|uint32(0o755), dirAttr.Mode)
	require.Equal(t, uint32(2), dirAttr.Nlink)
	require.Equal(t, uint32(1000), dirAttr.UID)
	require.Equal(t, uint32(2000), dirAttr.GID)

	rootAttr, err := actor.GetAttr(store.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint32(3), rootAttr.Nlink)

	// 2. 在带 S_ISGID 的子目录中 Mkdir 创建孙目录，验证自动继承父 GID 与 S_ISGID
	nestedDirAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("nested"),
		Mode:      0o750,
		UID:       1001,
		GID:       3000, // 应被父目录 GID=2000 覆盖
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), nestedDirAttr.GID)
	require.Equal(t, store.S_IFDIR|store.S_ISGID|uint32(0o750), nestedDirAttr.Mode)

	// 3. 在带 S_ISGID 的子目录中 Create 常规文件，验证自动继承父 GID（但不置文件 S_ISGID）
	regAttr, err := actor.Create(store.CreateReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("app.bin"),
		Mode:      store.S_ISUID | 0o777,
		UID:       1001,
		GID:       3000,
		Umask:     0o027,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), regAttr.GID)
	require.Equal(t, store.S_IFREG|store.S_ISUID|uint32(0o750), regAttr.Mode)
	require.Equal(t, uint32(1), regAttr.Nlink)

	// 4. Mknod 创建字符设备与 FIFO，验证 Rdev 仅对块/字符设备保留
	chrAttr, err := actor.Mknod(store.MknodReq{
		ParentIno: store.RootIno,
		Name:      []byte("null"),
		Mode:      store.S_IFCHR | 0o666,
		UID:       0,
		GID:       0,
		Rdev:      0x0103,
		Umask:     0o022,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFCHR|uint32(0o644), chrAttr.Mode)
	require.Equal(t, uint32(0x0103), chrAttr.Rdev)

	fifoAttr, err := actor.Mknod(store.MknodReq{
		ParentIno: store.RootIno,
		Name:      []byte("pipe"),
		Mode:      store.S_IFIFO | 0o640,
		Rdev:      0x9999,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFIFO|uint32(0o640), fifoAttr.Mode)
	require.Equal(t, uint32(0), fifoAttr.Rdev)

	// 5. Symlink 创建符号链接并验证 Readlink、StatFS 逻辑字节数与 dirtyAtime 叠加
	target := []byte("../sub/app.bin")
	symAttr, err := actor.Symlink(store.SymlinkReq{
		ParentIno: dirAttr.Ino,
		Name:      []byte("latest"),
		Target:    target,
		UID:       1001,
		GID:       3000,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFLNK|uint32(0o777), symAttr.Mode)
	require.Equal(t, uint64(len(target)), symAttr.Size)
	require.Equal(t, uint32(2000), symAttr.GID)

	gotTarget, err := actor.Readlink(symAttr.Ino)
	require.NoError(t, err)
	require.Equal(t, target, gotTarget)

	// 验证 Symlink 计入 StatFS 逻辑字节数，且 Readlink 触发的 dirtyAtime 在 Lookup / GetAttr 中叠加可见
	stAfterSym, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(len(target)), stAfterSym.UsedBytes)

	symAfterReadlink, err := actor.GetAttr(symAttr.Ino)
	require.NoError(t, err)
	require.GreaterOrEqual(t, symAfterReadlink.Atime, symAttr.Atime)

	futureSymAtime := time.Unix(0, symAfterReadlink.Atime).Add(5 * time.Minute)
	actor.MarkAtimeDirty(symAttr.Ino, futureSymAtime)
	_, symLookedAttr, err := actor.Lookup(dirAttr.Ino, []byte("latest"))
	require.NoError(t, err)
	require.Equal(t, futureSymAtime.UnixNano(), symLookedAttr.Atime)

	// 6. Lookup 验证普通项、"."、".." 及根目录 ".."
	entry, lookedAttr, err := actor.Lookup(dirAttr.Ino, []byte("app.bin"))
	require.NoError(t, err)
	require.Equal(t, regAttr.Ino, entry.Ino)
	require.Equal(t, regAttr.Mode, entry.Mode)
	require.Equal(t, regAttr.Ino, lookedAttr.Ino)

	dotEntry, dotAttr, err := actor.Lookup(dirAttr.Ino, []byte("."))
	require.NoError(t, err)
	require.Equal(t, dirAttr.Ino, dotEntry.Ino)
	require.Equal(t, dirAttr.Ino, dotAttr.Ino)

	dotDotEntry, dotDotAttr, err := actor.Lookup(dirAttr.Ino, []byte(".."))
	require.NoError(t, err)
	require.Equal(t, store.RootIno, dotDotEntry.Ino)
	require.Equal(t, store.RootIno, dotDotAttr.Ino)

	rootDotDotEntry, _, err := actor.Lookup(store.RootIno, []byte(".."))
	require.NoError(t, err)
	require.Equal(t, store.RootIno, rootDotDotEntry.Ino)
}

func TestLinkAndNamespaceErrorPaths(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-link",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	subAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dir"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	fileAttr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("origin.txt"),
		Mode:      0o644,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(1), fileAttr.Nlink)

	// 1. Link 常规文件到子目录，验证 Nlink 递增为 2，且 StatFS 文件节点总数不变
	beforeStat, err := actor.StatFS()
	require.NoError(t, err)

	linkedAttr, err := actor.Link(store.LinkReq{
		Ino:          fileAttr.Ino,
		NewParentIno: subAttr.Ino,
		NewName:      []byte("hardlink.txt"),
	})
	require.NoError(t, err)
	require.Equal(t, fileAttr.Ino, linkedAttr.Ino)
	require.Equal(t, uint32(2), linkedAttr.Nlink)

	afterStat, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, beforeStat.Files, afterStat.Files)

	e, gotAttr, err := actor.Lookup(subAttr.Ino, []byte("hardlink.txt"))
	require.NoError(t, err)
	require.Equal(t, fileAttr.Ino, e.Ino)
	require.Equal(t, uint32(2), gotAttr.Nlink)

	// 2. Link 源节点为目录必须返回 ErrPerm
	_, err = actor.Link(store.LinkReq{
		Ino:          subAttr.Ino,
		NewParentIno: store.RootIno,
		NewName:      []byte("dir-link"),
	})
	require.ErrorIs(t, err, store.ErrPerm)

	// 3. 同名 Entry 冲突返回 ErrExist (含 "." / "..")
	_, err = actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("origin.txt"),
		Mode:      0o644,
	})
	require.ErrorIs(t, err, store.ErrExist)

	_, err = actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("."),
		Mode:      0o755,
	})
	require.ErrorIs(t, err, store.ErrExist)

	_, err = actor.Link(store.LinkReq{
		Ino:          fileAttr.Ino,
		NewParentIno: subAttr.Ino,
		NewName:      []byte("hardlink.txt"),
	})
	require.ErrorIs(t, err, store.ErrExist)

	// 4. 父节点不是目录返回 ErrNotDir，父节点不存在返回 ErrNotFound
	_, err = actor.Create(store.CreateReq{
		ParentIno: fileAttr.Ino,
		Name:      []byte("child"),
		Mode:      0o644,
	})
	require.ErrorIs(t, err, store.ErrNotDir)

	_, _, err = actor.Lookup(fileAttr.Ino, []byte("child"))
	require.ErrorIs(t, err, store.ErrNotDir)

	_, err = actor.Mkdir(store.MkdirReq{
		ParentIno: 99999,
		Name:      []byte("child"),
		Mode:      0o755,
	})
	require.ErrorIs(t, err, store.ErrNotFound)

	// 5. Readlink 对非符号链接返回 ErrInvalid
	_, err = actor.Readlink(fileAttr.Ino)
	require.ErrorIs(t, err, store.ErrInvalid)

	// 6. Symlink 目标过长返回 ErrNameTooLong，空目标或含 \x00 返回 ErrInvalid
	_, err = actor.Symlink(store.SymlinkReq{
		ParentIno: store.RootIno,
		Name:      []byte("bad-sym"),
		Target:    bytes.Repeat([]byte("a"), store.MaxSymlinkTargetBytes+1),
	})
	require.ErrorIs(t, err, store.ErrNameTooLong)

	_, err = actor.Symlink(store.SymlinkReq{
		ParentIno: store.RootIno,
		Name:      []byte("bad-sym"),
		Target:    []byte("a\x00b"),
	})
	require.ErrorIs(t, err, store.ErrInvalid)

	// 7. 文件名过长返回 ErrNameTooLong，含 '/' 或 '\x00' 返回 ErrInvalid
	_, err = actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      bytes.Repeat([]byte("f"), store.MaxFileNameBytes+1),
		Mode:      0o644,
	})
	require.ErrorIs(t, err, store.ErrNameTooLong)

	_, err = actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("a/b"),
		Mode:      0o644,
	})
	require.ErrorIs(t, err, store.ErrInvalid)
}

func TestReadDirAndReadDirPlusWithPaginationAndAscendingBatchSeek(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-readdir",
		BranchID: "main",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	subAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("dir"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	// 故意让字典序名称与 Ino 编号逆序（名称 "z_first" -> Ino 3, "m_second" -> Ino 4, "a_third" -> Ino 5），
	// 验证 ReadDirPlus 在内部按 Ino 升序调用 SeekPrefixGE(InodeKey) 后，仍按目录项名称字典序与正确 NextOffset 返回。
	zAttr, err := actor.Create(store.CreateReq{
		ParentIno: subAttr.Ino,
		Name:      []byte("z_first"),
		Mode:      0o640,
		UID:       101,
	})
	require.NoError(t, err)

	mAttr, err := actor.Symlink(store.SymlinkReq{
		ParentIno: subAttr.Ino,
		Name:      []byte("m_second"),
		Target:    []byte("z_first"),
		UID:       102,
	})
	require.NoError(t, err)

	aAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: subAttr.Ino,
		Name:      []byte("a_third"),
		Mode:      0o700,
		UID:       103,
	})
	require.NoError(t, err)

	// 再加一个指向 zAttr (Ino 3) 的硬链接 "b_hardlink"，验证重复 Ino 也能正确填充属性
	_, err = actor.Link(store.LinkReq{
		Ino:          zAttr.Ino,
		NewParentIno: subAttr.Ino,
		NewName:      []byte("b_hardlink"),
	})
	require.NoError(t, err)

	// 预期完整顺序：
	// 0->1: "." (subAttr.Ino)
	// 1->2: ".." (RootIno)
	// 2->3: "a_third" (aAttr.Ino)
	// 3->4: "b_hardlink" (zAttr.Ino, Nlink=2)
	// 4->5: "m_second" (mAttr.Ino)
	// 5->6: "z_first" (zAttr.Ino, Nlink=2)

	// 1. ReadDir 按 maxEntries=2 游标分页读取
	var allDirEntries []store.DirEntry
	var cursor uint64
	for {
		page, err := actor.ReadDir(subAttr.Ino, cursor, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		require.LessOrEqual(t, len(page), 2)
		allDirEntries = append(allDirEntries, page...)
		cursor = page[len(page)-1].NextOffset
	}
	require.Len(t, allDirEntries, 6)
	require.Equal(t, []byte("."), allDirEntries[0].Name)
	require.Equal(t, subAttr.Ino, allDirEntries[0].Ino)
	require.Equal(t, uint64(1), allDirEntries[0].NextOffset)

	require.Equal(t, []byte(".."), allDirEntries[1].Name)
	require.Equal(t, store.RootIno, allDirEntries[1].Ino)
	require.Equal(t, uint64(2), allDirEntries[1].NextOffset)

	require.Equal(t, []byte("a_third"), allDirEntries[2].Name)
	require.Equal(t, aAttr.Ino, allDirEntries[2].Ino)
	require.Equal(t, uint64(3), allDirEntries[2].NextOffset)

	require.Equal(t, []byte("b_hardlink"), allDirEntries[3].Name)
	require.Equal(t, zAttr.Ino, allDirEntries[3].Ino)
	require.Equal(t, uint64(4), allDirEntries[3].NextOffset)

	require.Equal(t, []byte("m_second"), allDirEntries[4].Name)
	require.Equal(t, mAttr.Ino, allDirEntries[4].Ino)
	require.Equal(t, uint64(5), allDirEntries[4].NextOffset)

	require.Equal(t, []byte("z_first"), allDirEntries[5].Name)
	require.Equal(t, zAttr.Ino, allDirEntries[5].Ino)
	require.Equal(t, uint64(6), allDirEntries[5].NextOffset)

	// 2. ReadDirPlus 按 maxEntries=2 游标分页读取并验证完整 Attribute 及 dirtyAtime 叠加
	var allPlusEntries []store.DirEntryPlus
	cursor = 0
	for {
		page, err := actor.ReadDirPlus(subAttr.Ino, cursor, 2)
		require.NoError(t, err)
		if len(page) == 0 {
			break
		}
		require.LessOrEqual(t, len(page), 2)
		allPlusEntries = append(allPlusEntries, page...)
		cursor = page[len(page)-1].NextOffset
	}
	require.Len(t, allPlusEntries, 6)
	require.Equal(t, subAttr.Ino, allPlusEntries[0].Attr.Ino)
	require.Equal(t, uint32(3), allPlusEntries[0].Attr.Nlink) // sub 有子目录 a_third，因此 Nlink=3
	require.Equal(t, store.RootIno, allPlusEntries[1].Attr.Ino)
	require.Equal(t, uint32(103), allPlusEntries[2].Attr.UID)
	require.Equal(t, uint32(2), allPlusEntries[3].Attr.Nlink)
	require.Equal(t, uint64(len("z_first")), allPlusEntries[4].Attr.Size)
	require.Equal(t, uint32(2), allPlusEntries[5].Attr.Nlink)

	// 3. 非目录调用 ReadDir / ReadDirPlus 返回 ErrNotDir
	_, err = actor.ReadDir(zAttr.Ino, 0, 10)
	require.ErrorIs(t, err, store.ErrNotDir)
	_, err = actor.ReadDirPlus(zAttr.Ino, 0, 10)
	require.ErrorIs(t, err, store.ErrNotDir)
}

func TestNamespaceIntraBatchReadYourWritesAndPhaseAFailureIsolation(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:       t.TempDir(),
		DiskID:    "disk-batch-ns",
		BranchID:  "main",
		MaxBytes:  100 << 20,
		MaxInodes: 100,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 阻塞当前 Tick，将后续 5 条命名空间命令攒入同一个 mb.active 批次，在下一轮单次 Tick 内合并演进：
	// Cmd 1: Mkdir(1, "batch_dir") -> 预计分配 Ino=2 (Phase A 预览 2, Phase B 消费 2)
	// Cmd 2: Create(1, "batch_dir") -> 同名冲突在 Phase A 返回 ErrExist，验证不消耗 Ino=3、不污染 s.batch
	// Cmd 3: Create(2, "inside.txt") -> 同批写后读（Intra-Batch Read-Your-Writes）依赖 Cmd 1 创建的目录 Ino=2，成功拿到 Ino=3
	// Cmd 4: Link(2, 1, "illegal_dir_link") -> 对目录(Ino=2)创建硬链接在 Phase A 返回 ErrPerm，零污染
	// Cmd 5: Link(3, 2, "inside_link.txt") -> 同批写后读对 Cmd 3 创建的文件(Ino=3)创建硬链接，Nlink 变为 2
	blockCh := make(chan struct{})
	enteredCh := make(chan struct{})
	gateDone := make(chan error, 1)
	go func() {
		gateDone <- actor.Execute(store.WriteOp{
			Validate: func(v store.ReadView) error {
				close(enteredCh)
				<-blockCh
				return nil
			},
			Mutate: func(m store.MutateView) {},
		})
	}()
	<-enteredCh

	ticksBefore := actor.TickCount()

	type res struct {
		attr store.Attribute
		err  error
	}
	ch1 := make(chan res, 1)
	ch2 := make(chan res, 1)
	ch3 := make(chan res, 1)
	ch4 := make(chan res, 1)
	ch5 := make(chan res, 1)

	go func() {
		a, e := actor.Mkdir(store.MkdirReq{ParentIno: store.RootIno, Name: []byte("batch_dir"), Mode: 0o755})
		ch1 <- res{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		a, e := actor.Create(store.CreateReq{ParentIno: store.RootIno, Name: []byte("batch_dir"), Mode: 0o644})
		ch2 <- res{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		a, e := actor.Create(store.CreateReq{ParentIno: 2, Name: []byte("inside.txt"), Mode: 0o644})
		ch3 <- res{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		a, e := actor.Link(store.LinkReq{Ino: 2, NewParentIno: store.RootIno, NewName: []byte("illegal_dir_link")})
		ch4 <- res{a, e}
	}()
	time.Sleep(5 * time.Millisecond)

	go func() {
		a, e := actor.Link(store.LinkReq{Ino: 3, NewParentIno: 2, NewName: []byte("inside_link.txt")})
		ch5 <- res{a, e}
	}()
	time.Sleep(15 * time.Millisecond)

	close(blockCh)
	require.NoError(t, <-gateDone)

	r1 := <-ch1
	r2 := <-ch2
	r3 := <-ch3
	r4 := <-ch4
	r5 := <-ch5

	require.NoError(t, r1.err)
	require.Equal(t, uint64(2), r1.attr.Ino)

	require.ErrorIs(t, r2.err, store.ErrExist)

	require.NoError(t, r3.err)
	require.Equal(t, uint64(3), r3.attr.Ino)

	require.ErrorIs(t, r4.err, store.ErrPerm)

	require.NoError(t, r5.err)
	require.Equal(t, uint64(3), r5.attr.Ino)
	require.Equal(t, uint32(2), r5.attr.Nlink)

	// 验证上述 5 条命令确实在门控 Tick 之后的单次 Tick 内合并完成（ticksDelta == 2：1 次门控 + 1 次合并批次）
	require.Equal(t, ticksBefore+2, actor.TickCount())

	// 验证最终持久态与统计值精确一致：Root(1) + batch_dir(2) + inside.txt(3) 共 3 个 Inode
	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(3), st.Files)

	plus, err := actor.ReadDirPlus(2, 0, 10)
	require.NoError(t, err)
	require.Len(t, plus, 4) // ".", "..", "inside.txt", "inside_link.txt"
	require.Equal(t, []byte("inside.txt"), plus[2].Name)
	require.Equal(t, uint32(2), plus[2].Attr.Nlink)
	require.Equal(t, []byte("inside_link.txt"), plus[3].Name)
	require.Equal(t, uint32(2), plus[3].Attr.Nlink)
}
