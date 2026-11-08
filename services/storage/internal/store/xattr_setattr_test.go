package store_test

import (
	"bytes"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestSetAndGetXattrSemantics(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-xattr",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	fileAttr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("doc.txt"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	symAttr, err := actor.Symlink(store.SymlinkReq{
		ParentIno: store.RootIno,
		Name:      []byte("doc.lnk"),
		Target:    []byte("doc.txt"),
	})
	require.NoError(t, err)

	// 1. 未设置前 GetXattr 返回 ErrNoData；XattrReplace 在属性不存在时返回 ErrNoData
	_, err = actor.GetXattr(fileAttr.Ino, []byte("user.author"))
	require.ErrorIs(t, err, store.ErrNoData)

	err = actor.SetXattr(fileAttr.Ino, []byte("user.author"), []byte("alice"), store.XattrReplace)
	require.ErrorIs(t, err, store.ErrNoData)

	// 2. XattrCreate 创建新扩展属性，Ctime 与 WriteSeq 推进
	require.NoError(t, actor.SetXattr(fileAttr.Ino, []byte("user.author"), []byte("alice"), store.XattrCreate))

	val, err := actor.GetXattr(fileAttr.Ino, []byte("user.author"))
	require.NoError(t, err)
	require.Equal(t, []byte("alice"), val)

	afterCreateAttr, err := actor.GetAttr(fileAttr.Ino)
	require.NoError(t, err)
	require.Greater(t, afterCreateAttr.WriteSeq, fileAttr.WriteSeq)
	require.GreaterOrEqual(t, afterCreateAttr.Ctime, fileAttr.Ctime)

	// 3. 已存在时 XattrCreate 返回 ErrExist，且原值保持不变
	err = actor.SetXattr(fileAttr.Ino, []byte("user.author"), []byte("bob"), store.XattrCreate)
	require.ErrorIs(t, err, store.ErrExist)

	val, err = actor.GetXattr(fileAttr.Ino, []byte("user.author"))
	require.NoError(t, err)
	require.Equal(t, []byte("alice"), val)

	// 4. XattrReplace 覆盖已有属性，以及 flags=0 支持覆盖为空值 []byte{}
	require.NoError(t, actor.SetXattr(fileAttr.Ino, []byte("user.author"), []byte("charlie"), store.XattrReplace))
	val, err = actor.GetXattr(fileAttr.Ino, []byte("user.author"))
	require.NoError(t, err)
	require.Equal(t, []byte("charlie"), val)

	require.NoError(t, actor.SetXattr(fileAttr.Ino, []byte("user.empty"), nil, 0))
	emptyVal, err := actor.GetXattr(fileAttr.Ino, []byte("user.empty"))
	require.NoError(t, err)
	require.Empty(t, emptyVal)

	// 5. 符号链接访问 user.* 扩展属性：SetXattr 返回 ErrPerm，GetXattr 返回 ErrNoData；trusted.* 则允许
	err = actor.SetXattr(symAttr.Ino, []byte("user.tag"), []byte("v1"), 0)
	require.ErrorIs(t, err, store.ErrPerm)
	_, err = actor.GetXattr(symAttr.Ino, []byte("user.tag"))
	require.ErrorIs(t, err, store.ErrNoData)

	require.NoError(t, actor.SetXattr(symAttr.Ino, []byte("trusted.tag"), []byte("v1"), 0))
	trustedVal, err := actor.GetXattr(symAttr.Ino, []byte("trusted.tag"))
	require.NoError(t, err)
	require.Equal(t, []byte("v1"), trustedVal)

	// 6. 参数校验边界：空名、含 NUL、非法 flags、超长名/值、不存在节点
	require.ErrorIs(t, actor.SetXattr(0, []byte("user.k"), []byte("v"), 0), store.ErrInvalid)
	require.ErrorIs(t, actor.SetXattr(fileAttr.Ino, nil, []byte("v"), 0), store.ErrInvalid)
	require.ErrorIs(t, actor.SetXattr(fileAttr.Ino, []byte("user.\x00k"), []byte("v"), 0), store.ErrInvalid)
	require.ErrorIs(t, actor.SetXattr(fileAttr.Ino, []byte("user.k"), []byte("v"), 0x4), store.ErrInvalid)
	require.ErrorIs(t, actor.SetXattr(fileAttr.Ino, bytes.Repeat([]byte("a"), store.MaxXattrNameBytes+1), []byte("v"), 0), store.ErrRange)
	require.ErrorIs(t, actor.SetXattr(fileAttr.Ino, []byte("user.k"), make([]byte, store.MaxXattrValueBytes+1), 0), store.ErrRange)
	require.ErrorIs(t, actor.SetXattr(999999, []byte("user.k"), []byte("v"), 0), store.ErrNotFound)

	_, err = actor.GetXattr(0, []byte("user.k"))
	require.ErrorIs(t, err, store.ErrInvalid)
	_, err = actor.GetXattr(fileAttr.Ino, nil)
	require.ErrorIs(t, err, store.ErrInvalid)
	_, err = actor.GetXattr(fileAttr.Ino, bytes.Repeat([]byte("a"), store.MaxXattrNameBytes+1))
	require.ErrorIs(t, err, store.ErrRange)
	_, err = actor.GetXattr(999999, []byte("user.k"))
	require.ErrorIs(t, err, store.ErrNotFound)
}

func TestListRemoveXattrAndIntraBatchReadYourWrites(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-xattr-list",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f1, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1.txt"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	f2, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f2.txt"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 1. 初始无扩展属性时 ListXattr 返回空切片；乱序写入后按字典序升序返回，且与相邻节点严格隔离
	names, err := actor.ListXattr(f1.Ino)
	require.NoError(t, err)
	require.Empty(t, names)

	require.NoError(t, actor.SetXattr(f1.Ino, []byte("user.zeta"), []byte("z"), 0))
	require.NoError(t, actor.SetXattr(f1.Ino, []byte("user.alpha"), []byte("a"), 0))
	require.NoError(t, actor.SetXattr(f1.Ino, []byte("security.selinux"), []byte("s"), 0))
	require.NoError(t, actor.SetXattr(f2.Ino, []byte("user.other"), []byte("o"), 0))

	names, err = actor.ListXattr(f1.Ino)
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		[]byte("security.selinux"),
		[]byte("user.alpha"),
		[]byte("user.zeta"),
	}, names)

	// 2. RemoveXattr 删除已存在属性并推进 Ctime / WriteSeq；再次删除返回 ErrNoData
	beforeRmAttr, err := actor.GetAttr(f1.Ino)
	require.NoError(t, err)

	require.NoError(t, actor.RemoveXattr(f1.Ino, []byte("user.alpha")))
	_, err = actor.GetXattr(f1.Ino, []byte("user.alpha"))
	require.ErrorIs(t, err, store.ErrNoData)

	afterRmAttr, err := actor.GetAttr(f1.Ino)
	require.NoError(t, err)
	require.Greater(t, afterRmAttr.WriteSeq, beforeRmAttr.WriteSeq)

	err = actor.RemoveXattr(f1.Ino, []byte("user.alpha"))
	require.ErrorIs(t, err, store.ErrNoData)

	names, err = actor.ListXattr(f1.Ino)
	require.NoError(t, err)
	require.Equal(t, [][]byte{
		[]byte("security.selinux"),
		[]byte("user.zeta"),
	}, names)

	// 3. 同批写后读（Intra-Batch Read-Your-Writes）与 Phase A 失败隔离：
	// 在同一轮 Tick 内依次执行：
	//   Cmd 1: SetXattr(f2, "user.batch", "v1", XattrCreate) -> 成功
	//   Cmd 2: SetXattr(f2, "user.batch", "v2", XattrCreate) -> 读到同批 Cmd 1 已存在，返回 ErrExist（不污染批次）
	//   Cmd 3: SetXattr(f2, "user.batch", "v3", XattrReplace) -> 读到同批 Cmd 1 已存在，成功覆盖为 "v3"
	//   Cmd 4: RemoveXattr(f2, "user.batch") -> 读到同批已存在，成功删除
	//   Cmd 5: RemoveXattr(f2, "user.batch") -> 读到同批 Cmd 4 已删除，返回 ErrNoData
	release := store.HoldTickForTest(actor)

	errCh := make([]chan error, 5)
	for i := range errCh {
		errCh[i] = make(chan error, 1)
	}

	go func() {
		errCh[0] <- actor.SetXattr(f2.Ino, []byte("user.batch"), []byte("v1"), store.XattrCreate)
	}()
	waitQueued(t, actor, 1)

	go func() {
		errCh[1] <- actor.SetXattr(f2.Ino, []byte("user.batch"), []byte("v2"), store.XattrCreate)
	}()
	waitQueued(t, actor, 2)

	go func() {
		errCh[2] <- actor.SetXattr(f2.Ino, []byte("user.batch"), []byte("v3"), store.XattrReplace)
	}()
	waitQueued(t, actor, 3)

	go func() {
		errCh[3] <- actor.RemoveXattr(f2.Ino, []byte("user.batch"))
	}()
	waitQueued(t, actor, 4)

	go func() {
		errCh[4] <- actor.RemoveXattr(f2.Ino, []byte("user.batch"))
	}()
	waitQueued(t, actor, 5)

	release()

	require.NoError(t, <-errCh[0])
	require.ErrorIs(t, <-errCh[1], store.ErrExist)
	require.NoError(t, <-errCh[2])
	require.NoError(t, <-errCh[3])
	require.ErrorIs(t, <-errCh[4], store.ErrNoData)

	_, err = actor.GetXattr(f2.Ino, []byte("user.batch"))
	require.ErrorIs(t, err, store.ErrNoData)
}

func waitQueued(t *testing.T, _ *store.StoreActor, _ int) {
	t.Helper()
	time.Sleep(5 * time.Millisecond)
}

func TestSetAttrKillPrivDirtyAtimeAndFlags(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-setattr",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. 创建带 S_ISUID | S_ISGID 的常规文件与带 S_ISGID 的目录，并设置 security.capability
	regAttr, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("suid.bin"),
		Mode:      store.S_ISUID | store.S_ISGID | 0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFREG|store.S_ISUID|store.S_ISGID|uint32(0o755), regAttr.Mode)

	require.NoError(t, actor.SetXattr(regAttr.Ino, []byte("security.capability"), []byte("cap_net_raw"), 0))
	require.NoError(t, actor.SetXattr(regAttr.Ino, []byte("user.keep"), []byte("stay"), 0))

	// 2. 修改常规文件 UID/GID：自动清除 S_ISUID / S_ISGID，并同批删除 security.capability（保留其他 xattr）
	newUID := uint32(2000)
	newGID := uint32(2001)
	afterChown, err := actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		UID: &newUID,
		GID: &newGID,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), afterChown.UID)
	require.Equal(t, uint32(2001), afterChown.GID)
	require.Equal(t, store.S_IFREG|uint32(0o755), afterChown.Mode)

	_, err = actor.GetXattr(regAttr.Ino, []byte("security.capability"))
	require.ErrorIs(t, err, store.ErrNoData)

	keepVal, err := actor.GetXattr(regAttr.Ino, []byte("user.keep"))
	require.NoError(t, err)
	require.Equal(t, []byte("stay"), keepVal)

	// 3. 目录节点修改 UID/GID：保留目录上的 S_ISGID 位，同时删除 security.capability
	dirAttr, err := actor.Mkdir(store.MkdirReq{
		ParentIno: store.RootIno,
		Name:      []byte("sgid-dir"),
		Mode:      store.S_ISGID | 0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFDIR|store.S_ISGID|uint32(0o755), dirAttr.Mode)
	require.NoError(t, actor.SetXattr(dirAttr.Ino, []byte("security.capability"), []byte("dir_cap"), 0))

	dirAfterChown, err := actor.SetAttr(dirAttr.Ino, store.SetAttrReq{
		UID: &newUID,
		GID: &newGID,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFDIR|store.S_ISGID|uint32(0o755), dirAfterChown.Mode)
	_, err = actor.GetXattr(dirAttr.Ino, []byte("security.capability"))
	require.ErrorIs(t, err, store.ErrNoData)

	// 4. 显式更新 Atime / AtimeNow 时必须清除内存挂起的 dirtyAtime，防止旧的延迟 atime 覆盖
	futureDirty := time.Now().Add(1 * time.Hour).UTC()
	actor.MarkAtimeDirty(regAttr.Ino, futureDirty)
	gotOverlay, err := actor.GetAttr(regAttr.Ino)
	require.NoError(t, err)
	require.Equal(t, futureDirty.UnixNano(), gotOverlay.Atime)

	explicitAtime := time.Unix(1700000000, 123456789).UTC()
	explicitMtime := time.Unix(1700000500, 987654321).UTC()
	afterTime, err := actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		Atime: &explicitAtime,
		Mtime: &explicitMtime,
	})
	require.NoError(t, err)
	require.Equal(t, explicitAtime.UnixNano(), afterTime.Atime)
	require.Equal(t, explicitMtime.UnixNano(), afterTime.Mtime)

	// 触发一次其他写批次使 flushDirtyAtimeToBatch 执行，验证 explicitAtime 未被先前的 futureDirty 覆盖
	require.NoError(t, actor.SetXattr(regAttr.Ino, []byte("user.keep"), []byte("stay2"), 0))
	afterFlushAttr, err := actor.GetAttr(regAttr.Ino)
	require.NoError(t, err)
	require.Equal(t, explicitAtime.UnixNano(), afterFlushAttr.Atime)
	require.Equal(t, explicitMtime.UnixNano(), afterFlushAttr.Mtime)

	// 验证 AtimeNow 与 MtimeNow
	actor.MarkAtimeDirty(regAttr.Ino, futureDirty)
	afterNow, err := actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		AtimeNow: true,
		MtimeNow: true,
	})
	require.NoError(t, err)
	require.Less(t, afterNow.Atime, futureDirty.UnixNano())
	gotAfterNow, err := actor.GetAttr(regAttr.Ino)
	require.NoError(t, err)
	require.Equal(t, afterNow.Atime, gotAfterNow.Atime)

	// 5. Mode 与 Flags (FlagImmutable) 语义校验
	newMode := uint32(0o600)
	require.NoError(t, actor.SetXattr(regAttr.Ino, []byte("security.capability"), []byte("cap2"), 0))
	afterChmod, err := actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		Mode: &newMode,
	})
	require.NoError(t, err)
	require.Equal(t, store.S_IFREG|uint32(0o600), afterChmod.Mode)
	_, err = actor.GetXattr(regAttr.Ino, []byte("security.capability"))
	require.ErrorIs(t, err, store.ErrNoData)

	immFlags := store.FlagImmutable
	afterImm, err := actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		Flags: &immFlags,
	})
	require.NoError(t, err)
	require.Equal(t, store.FlagImmutable, afterImm.Flags)

	// Immutable 节点拒绝除清除 FlagImmutable 以外的属性修改
	_, err = actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		UID: &newUID,
	})
	require.ErrorIs(t, err, store.ErrPerm)

	clearFlags := uint32(0)
	afterClearImm, err := actor.SetAttr(regAttr.Ino, store.SetAttrReq{
		Flags: &clearFlags,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(0), afterClearImm.Flags)

	// 6. SetAttr 参数与类型错误边界校验
	_, err = actor.SetAttr(regAttr.Ino, store.SetAttrReq{})
	require.ErrorIs(t, err, store.ErrInvalid)
	_, err = actor.SetAttr(regAttr.Ino, store.SetAttrReq{Atime: &explicitAtime, AtimeNow: true})
	require.ErrorIs(t, err, store.ErrInvalid)
	_, err = actor.SetAttr(regAttr.Ino, store.SetAttrReq{Mtime: &explicitMtime, MtimeNow: true})
	require.ErrorIs(t, err, store.ErrInvalid)

	wrongTypeMode := store.S_IFDIR | 0o755
	_, err = actor.SetAttr(regAttr.Ino, store.SetAttrReq{Mode: &wrongTypeMode})
	require.ErrorIs(t, err, store.ErrInvalid)
}

func TestXattrListLimitReadOnlyAndClosedGuards(t *testing.T) {
	dir := t.TempDir()
	actor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-xattr-guard",
		MaxBytes: 100 << 20,
	})
	require.NoError(t, err)

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("quota.txt"),
		Mode:      0o644,
	})
	require.NoError(t, err)

	// 1. 填充扩展属性名称直至逼近 MaxXattrListBytes (65536)：
	// 每条名称 255 字节，计入 '\x00' 占 256 字节，256 * 255 = 65280 字节；
	// 第 256 条 255 字节名称使总长度达 256 * 256 = 65536（恰好允许），第 257 条超出上限返回 ErrNoSpace。
	for i := range 256 {
		prefix := []byte("user.")
		suffix := []byte{byte('a' + (i / 26)), byte('a' + (i % 26))}
		padLen := store.MaxXattrNameBytes - len(prefix) - len(suffix)
		name := append(append([]byte{}, prefix...), suffix...)
		name = append(name, bytes.Repeat([]byte("x"), padLen)...)
		require.Len(t, name, store.MaxXattrNameBytes)
		require.NoError(t, actor.SetXattr(f.Ino, name, []byte("v"), store.XattrCreate))
	}

	err = actor.SetXattr(f.Ino, []byte("user.overflow"), []byte("v"), 0)
	require.ErrorIs(t, err, store.ErrNoSpace)

	// 覆盖已存在的属性名称不增加列表字节数，必须成功
	firstKey := append([]byte("user.aa"), bytes.Repeat([]byte("x"), store.MaxXattrNameBytes-7)...)
	require.NoError(t, actor.SetXattr(f.Ino, firstKey, []byte("updated"), store.XattrReplace))

	require.NoError(t, actor.Close())

	// 2. 已关闭实例全部读写操作返回 ErrClosed
	require.ErrorIs(t, actor.SetXattr(f.Ino, []byte("user.k"), []byte("v"), 0), store.ErrClosed)
	require.ErrorIs(t, actor.RemoveXattr(f.Ino, firstKey), store.ErrClosed)
	_, err = actor.GetXattr(f.Ino, firstKey)
	require.ErrorIs(t, err, store.ErrClosed)
	_, err = actor.ListXattr(f.Ino)
	require.ErrorIs(t, err, store.ErrClosed)
	uid := uint32(1000)
	_, err = actor.SetAttr(f.Ino, store.SetAttrReq{UID: &uid})
	require.ErrorIs(t, err, store.ErrClosed)

	// 3. 只读模式重新打开：GetXattr 与 ListXattr 正常读取，写操作全部返回 ErrReadOnly
	roActor, err := store.Open(store.Options{
		Dir:      dir,
		DiskID:   "disk-xattr-guard",
		ReadOnly: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roActor.Close()) }()

	gotVal, err := roActor.GetXattr(f.Ino, firstKey)
	require.NoError(t, err)
	require.Equal(t, []byte("updated"), gotVal)

	gotNames, err := roActor.ListXattr(f.Ino)
	require.NoError(t, err)
	require.Len(t, gotNames, 256)

	require.ErrorIs(t, roActor.SetXattr(f.Ino, firstKey, []byte("ro"), 0), store.ErrReadOnly)
	require.ErrorIs(t, roActor.RemoveXattr(f.Ino, firstKey), store.ErrReadOnly)
	_, err = roActor.SetAttr(f.Ino, store.SetAttrReq{UID: &uid})
	require.ErrorIs(t, err, store.ErrReadOnly)
}
