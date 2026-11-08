package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func countLiveBlobFiles(t *testing.T, blobs *store.BlobStore) int {
	t.Helper()
	entries, err := os.ReadDir(blobs.Root())
	if os.IsNotExist(err) {
		return 0
	}
	require.NoError(t, err)
	var count int
	for _, f := range entries {
		if !f.IsDir() && filepath.Ext(f.Name()) == ".pack" {
			count++
		}
	}
	return count
}

func TestPartialWriteWindowLocalPatchLogSingleBlobAndHotRead(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-partial-append",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("append.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 连续 8 次 4 KiB 小块追加写：
	// 第 1 次（旧块不存在）写出 <blobID=1>.blob（此时 WindowLocal(1) == true）；
	// 第 2..8 次（WindowLocal(1) == true）必须在阶段 1 零磁盘 I/O 走 PatchLog 并在阶段 3 原地合并，
	// 严禁分配新 BlobID 或产生超过 1 个物理 .blob 文件。
	const chunkSize = 4096
	const rounds = 8
	expected := make([]byte, 0, chunkSize*rounds)

	for i := range rounds {
		chunk := bytes.Repeat([]byte{byte(0x21 + i)}, chunkSize)
		offset := uint64(i * chunkSize)
		n, attr, wErr := actor.Write(f.Ino, offset, chunk)
		require.NoError(t, wErr)
		require.Equal(t, uint32(chunkSize), n)
		expected = append(expected, chunk...)
		require.Equal(t, uint64(len(expected)), attr.Size)

		// 每次写入后立即无锁 Read（命中 hotShard），必须读到最新完整字节
		got, rErr := actor.Read(f.Ino, 0, uint32(len(expected)))
		require.NoError(t, rErr)
		require.Equal(t, expected, got)
	}

	// 验证磁盘上仅存在 1 个物理 .blob 文件（ID = 1）
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))
	require.True(t, actor.Blobs().Exists(1))
	require.False(t, actor.Blobs().Exists(2))

	// 进一步验证块内原地覆盖写（有效长度 Length 不变时免重写 DataKey）与跨越块内空洞写（自动从 oldRef.Length 补零）
	overwrite := bytes.Repeat([]byte{0x99}, 1024)
	n, attr, err := actor.Write(f.Ino, 2048, overwrite)
	require.NoError(t, err)
	require.Equal(t, uint32(1024), n)
	require.Equal(t, uint64(len(expected)), attr.Size)
	copy(expected[2048:2048+1024], overwrite)

	// 跨越旧块尾部 (32 KiB) 到 40 KiB 处写入 2 KiB，中间 [32 KiB, 40 KiB) 必须自动补零
	holeTail := bytes.Repeat([]byte{0x77}, 2048)
	n, attr, err = actor.Write(f.Ino, 40*1024, holeTail)
	require.NoError(t, err)
	require.Equal(t, uint32(2048), n)
	require.Equal(t, uint64(42*1024), attr.Size)

	expectedWithHole := make([]byte, 42*1024)
	copy(expectedWithHole, expected)
	copy(expectedWithHole[40*1024:], holeTail)

	// 1. 命中 hotShard 读取
	gotHot, err := actor.Read(f.Ino, 0, uint32(len(expectedWithHole)))
	require.NoError(t, err)
	require.Equal(t, expectedWithHole, gotHot)

	// 2. 驱逐 hotShard 后回退磁盘文件读取，验证阶段 3 WriteAt + Truncate 落盘字节完全一致
	actor.Blobs().EvictHot(store.BlobRef{PackID: 1, Offset: 0, Length: 42 * 1024})
	gotDisk, err := actor.Read(f.Ino, 0, uint32(len(expectedWithHole)))
	require.NoError(t, err)
	require.Equal(t, expectedWithHole, gotDisk)
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))
}

func TestPartialWriteSyncedBlockCoWAndPhaseAPatchDiscard(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-partial-cow",
		BranchID: "main",
		MaxBytes: 16 * 1024, // 配额上限 16 KiB
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("cow.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. 首次写入 8 KiB
	initial := bytes.Repeat([]byte{0x41}, 8192)
	_, _, err = actor.Write(f.Ino, 0, initial)
	require.NoError(t, err)
	require.True(t, actor.Blobs().Exists(1))
	require.True(t, actor.Blobs().HasDirty())

	badPatch := bytes.Repeat([]byte{0xff}, 8192)
	_, _, err = actor.Write(f.Ino, 12*1024, badPatch)
	require.ErrorIs(t, err, store.ErrNoSpace)

	// 验证 hotShard 未被污染
	gotHotAfterFail, err := actor.Read(f.Ino, 0, 16*1024)
	require.NoError(t, err)
	require.Equal(t, initial, gotHotAfterFail)

	// 验证 1.pack 中原块区间 [0, 8192) 字节未被篡改
	rawDisk, err := os.ReadFile(actor.Blobs().Path(1))
	require.NoError(t, err)
	require.Equal(t, initial, rawDisk[:len(initial)])
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	// 3. 调用 SyncInode 将 1.pack 持久化落盘
	require.NoError(t, actor.Blobs().SyncInode(f.Ino))
	require.False(t, actor.Blobs().HasDirty())

	// 4. 对块 0 执行局部覆写（offset=4096, len=4096）：追加至 1.pack 尾部新偏移并切换 DataKey
	cowPatch := bytes.Repeat([]byte{0x42}, 4096)
	_, attr, err := actor.Write(f.Ino, 4096, cowPatch)
	require.NoError(t, err)
	require.Equal(t, uint64(8192), attr.Size)
	require.True(t, actor.Blobs().Exists(1))
	require.True(t, actor.Blobs().HasDirty())
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	expectedCoW := append(bytes.Repeat([]byte{0x41}, 4096), cowPatch...)
	gotCoW, err := actor.Read(f.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, expectedCoW, gotCoW)

	// 5. 再次测试 Phase A 配额超限失败时，已提交块数据完好无损
	require.NoError(t, actor.Blobs().SyncInode(f.Ino))
	require.False(t, actor.Blobs().HasDirty())
	_, _, err = actor.Write(f.Ino, 12*1024, badPatch)
	require.ErrorIs(t, err, store.ErrNoSpace)
	gotAfterFail2, err := actor.Read(f.Ino, 0, 8192)
	require.NoError(t, err)
	require.Equal(t, expectedCoW, gotAfterFail2)
}

func TestSetAttrSizeTruncateAndExpandAcrossBlocks(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-truncate",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("trunc.bin"),
		Mode:      store.S_IFREG | store.S_ISUID | store.S_ISGID | 0o644,
	})
	require.NoError(t, err)

	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, store.BlobSize)
	_, _, err = actor.Write(f.Ino, 0, b0)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, store.BlobSize, b1)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, 2*store.BlobSize, b2)
	require.NoError(t, err)
	require.Equal(t, 3, countLiveBlobFiles(t, actor.Blobs())) // blobID 1, 2, 3

	// 1. 截断至 1.5 MiB（此时块 2 为 WindowLocal 活跃块）：
	//    - 整块 2（blobID=3）必须从 DataKey 删除且物理文件 3.blob 被删除；
	//    - 块 1（blobID=2, WindowLocal=true）走 PatchLog 原地截断为 512 KiB，不产生新 BlobID；
	//    - 块 0（blobID=1）保持不变。
	size15M := uint64(store.BlobSize + store.BlobSize/2)
	attr, err := actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size15M})
	require.NoError(t, err)
	require.Equal(t, size15M, attr.Size)
	require.False(t, actor.Blobs().Exists(3), "被整块截断移除的 3.blob 必须被物理删除")
	require.True(t, actor.Blobs().Exists(2), "WindowLocal 块 2.blob 应原地截断保留")
	require.Equal(t, 2, countLiveBlobFiles(t, actor.Blobs()))

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, size15M, st.UsedBytes)

	// 2. 扩容至 2 MiB（稀疏空洞扩容），随后读 [1 MiB, 2 MiB)：前半 512 KiB 为 0x22，后半 512 KiB 为 0（因为先前已被物理截断）
	size2M := uint64(2 * store.BlobSize)
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size2M})
	require.NoError(t, err)
	require.Equal(t, size2M, attr.Size)

	gotBlock1, err := actor.Read(f.Ino, store.BlobSize, store.BlobSize)
	require.NoError(t, err)
	expectedBlock1 := append(bytes.Repeat([]byte{0x22}, store.BlobSize/2), make([]byte, store.BlobSize/2)...)
	require.Equal(t, expectedBlock1, gotBlock1)

	// 3. 固化块 0 和块 1（WindowLocal 翻转为 false），再截断至 256 KiB：
	//    - 块 1（blobID=2）整块移除；
	//    - 块 0（blobID=1, !WindowLocal）走 CoW 生成新块 blobID=4（长度 256 KiB），旧块 1.blob 被物理删除。
	require.NoError(t, actor.Blobs().SyncInode(f.Ino))
	size256K := uint64(256 * 1024)
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &size256K})
	require.NoError(t, err)
	require.Equal(t, size256K, attr.Size)
	require.False(t, actor.Blobs().Exists(1), "已固化旧块 1.blob 在块内截断 CoW 后必须被删除")
	require.False(t, actor.Blobs().Exists(2), "整块移除的 2.blob 必须被删除")
	require.True(t, actor.Blobs().Exists(4), "CoW 截断新块 4.blob 必须存在")
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	got256K, err := actor.Read(f.Ino, 0, uint32(size256K))
	require.NoError(t, err)
	require.Equal(t, b0[:size256K], got256K)

	// 4. Truncate(0)：删除全部剩余 DataKey 与物理块文件
	sizeZero := uint64(0)
	attr, err = actor.SetAttr(f.Ino, store.SetAttrReq{Size: &sizeZero})
	require.NoError(t, err)
	require.Equal(t, uint64(0), attr.Size)
	require.Equal(t, 0, countLiveBlobFiles(t, actor.Blobs()))

	stZero, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(0), stZero.UsedBytes)
}

func TestFallocateExtendKeepSizeAndPunchHole(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-falloc",
		BranchID: "main",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("falloc.bin"),
		Mode:      store.S_IFREG | store.S_ISUID | store.S_ISGID | 0o644,
	})
	require.NoError(t, err)

	// 1. mode = 0 默认预留扩展：Size 从 0 扩展到 1 MiB，更新 LogicalBytes，不分配全零物理块
	require.NoError(t, actor.Fallocate(f.Ino, 0, store.BlobSize, 0))
	attr, err := actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), attr.Size)
	require.Equal(t, 0, countLiveBlobFiles(t, actor.Blobs()))

	// 2. mode = FallocKeepSize：超出当前 Size 预留但不改变文件 Size
	require.NoError(t, actor.Fallocate(f.Ino, store.BlobSize, store.BlobSize, store.FallocKeepSize))
	attr, err = actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), attr.Size)

	// 3. 写入 3 个 1 MiB 块（block 0, 1, 2 -> blobID 1, 2, 3，当前均为 WindowLocal=true）
	b0 := bytes.Repeat([]byte{0x11}, store.BlobSize)
	b1 := bytes.Repeat([]byte{0x22}, store.BlobSize)
	b2 := bytes.Repeat([]byte{0x33}, store.BlobSize)
	_, _, err = actor.Write(f.Ino, 0, b0)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, store.BlobSize, b1)
	require.NoError(t, err)
	_, _, err = actor.Write(f.Ino, 2*store.BlobSize, b2)
	require.NoError(t, err)
	require.Equal(t, 3, countLiveBlobFiles(t, actor.Blobs()))

	// 4. FallocPunchHole | FallocKeepSize 跨块打洞：[512 KiB, 2.5 MiB)
	//    - 块 0 (WindowLocal=true)：[512 KiB, 1 MiB) 走 PatchLog 原地清零（保留 1.blob）；
	//    - 块 1：整块打洞，直接 DeleteDeferred DataKey 并物理删除 2.blob；
	//    - 块 2 (WindowLocal=true)：[0, 512 KiB) 走 PatchLog 原地清零（保留 3.blob）；
	//    - Size 保持 3 MiB 不变。
	holeStart := uint64(store.BlobSize / 2)
	holeLen := uint64(2 * store.BlobSize) // [0.5 MiB, 2.5 MiB)
	require.NoError(t, actor.Fallocate(f.Ino, holeStart, holeLen, store.FallocPunchHole|store.FallocKeepSize))

	attrAfterHole, err := actor.GetAttr(f.Ino)
	require.NoError(t, err)
	require.Equal(t, uint64(3*store.BlobSize), attrAfterHole.Size)
	require.True(t, actor.Blobs().Exists(1), "WindowLocal 块 0 应通过 PatchLog 原地清零保留 1.blob")
	require.False(t, actor.Blobs().Exists(2), "整块打洞的块 1 (2.blob) 必须被物理删除")
	require.True(t, actor.Blobs().Exists(3), "WindowLocal 块 2 应通过 PatchLog 原地清零保留 3.blob")
	require.Equal(t, 2, countLiveBlobFiles(t, actor.Blobs()))

	gotAll, err := actor.Read(f.Ino, 0, 3*store.BlobSize)
	require.NoError(t, err)
	expectedAfterHole := make([]byte, 3*store.BlobSize)
	copy(expectedAfterHole[:store.BlobSize/2], b0[:store.BlobSize/2])
	copy(expectedAfterHole[2*store.BlobSize+store.BlobSize/2:], b2[store.BlobSize/2:])
	require.Equal(t, expectedAfterHole, gotAll)

	// 5. 固化剩余块 0 (1.blob) 与块 2 (3.blob)，再对块 0 的 [0, 512 KiB) 局部打洞使整块变全零，
	//    并对块 2 的 [512 KiB, 768 KiB) 局部打洞触发 !WindowLocal CoW：
	require.NoError(t, actor.Blobs().SyncInode(f.Ino))
	// 块 0 前半 512 KiB 打洞后整块全为 0 -> 必须删除 DataKey 并物理删除 1.blob，不分配新块
	require.NoError(t, actor.Fallocate(f.Ino, 0, store.BlobSize/2, store.FallocPunchHole|store.FallocKeepSize))
	require.False(t, actor.Blobs().Exists(1), "打洞后变全零的块 0 (1.blob) 必须删除 DataKey 与物理文件")
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	// 块 2 (3.blob, !WindowLocal) 在 [2.5 MiB, 2.75 MiB) 打洞 -> CoW 生成新块 4.blob，物理删除 3.blob
	require.NoError(t, actor.Fallocate(f.Ino, 2*store.BlobSize+store.BlobSize/2, store.BlobSize/4, store.FallocPunchHole|store.FallocKeepSize))
	require.False(t, actor.Blobs().Exists(3), "被 CoW 局部打洞替换的旧固化块 3.blob 必须被物理删除")
	require.True(t, actor.Blobs().Exists(4), "CoW 新块 4.blob 必须存在")
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	gotBlock2, err := actor.Read(f.Ino, 2*store.BlobSize, store.BlobSize)
	require.NoError(t, err)
	expectedBlock2 := make([]byte, store.BlobSize)
	copy(expectedBlock2[store.BlobSize*3/4:], b2[store.BlobSize*3/4:])
	require.Equal(t, expectedBlock2, gotBlock2)

	// 6. 非法 mode 校验：单独 FallocPunchHole 不带 FallocKeepSize 返回 ErrInvalid
	require.ErrorIs(t, actor.Fallocate(f.Ino, 0, 4096, store.FallocPunchHole), store.ErrInvalid)
}

func TestPhysicalBlobCleanupOnUnlinkRenamePurgeOrphanAndCleanOrphans(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-reclaim-e2e",
		BranchID: "main",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	// 1. Unlink(openHandle=false) 直接删除含多个跨块（整块 + 局部块）的文件 -> DataKey 归零
	f1, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("f1"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(f1.Ino, 0, bytes.Repeat([]byte{0x11}, store.BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(f1.Ino, store.BlobSize, bytes.Repeat([]byte{0x12}, 8192))
	require.NoError(t, err)
	require.GreaterOrEqual(t, countLiveBlobFiles(t, actor.Blobs()), 1)

	require.NoError(t, actor.Unlink(store.RootIno, []byte("f1"), false))

	// 2. Rename 覆盖删除目标文件 (TargetOpenHandle=false)
	src, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("src"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	dst, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("dst"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(src.Ino, 0, bytes.Repeat([]byte{0x21}, 4096))
	require.NoError(t, err)
	_, _, err = actor.Write(dst.Ino, 0, bytes.Repeat([]byte{0x22}, store.BlobSize))
	require.NoError(t, err)
	_, _, err = actor.Write(dst.Ino, store.BlobSize, bytes.Repeat([]byte{0x23}, 16384))
	require.NoError(t, err)

	require.NoError(t, actor.Rename(store.RenameReq{
		OldParentIno:     store.RootIno,
		OldName:          []byte("src"),
		NewParentIno:     store.RootIno,
		NewName:          []byte("dst"),
		TargetOpenHandle: false,
	}))

	// 3. Unlink(openHandle=true) 转为孤儿节点时保留物理块可读，PurgeOrphanInode 后清理
	require.NoError(t, actor.Unlink(store.RootIno, []byte("dst"), true))
	gotOrphan, err := actor.Read(src.Ino, 0, 4096)
	require.NoError(t, err)
	require.Equal(t, bytes.Repeat([]byte{0x21}, 4096), gotOrphan)

	require.NoError(t, actor.PurgeOrphanInode(src.Ino))

	// 4. CleanOrphans 批量清理多个含数据块的孤儿节点
	for i := range 3 {
		fo, cErr := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte{byte('o'), byte('0' + i)},
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, cErr)
		_, _, wErr := actor.Write(fo.Ino, 0, bytes.Repeat([]byte{byte(0x30 + i)}, 8192))
		require.NoError(t, wErr)
		require.NoError(t, actor.Unlink(store.RootIno, []byte{byte('o'), byte('0' + i)}, true))
	}

	require.NoError(t, actor.CleanOrphans())

	st, err := actor.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(0), st.UsedBytes)
	require.Equal(t, uint64(1), st.Files) // 仅剩 RootIno
}

func TestCrossBlockPartialWriteAndHotShardBudgetEvictionOnPatch(t *testing.T) {
	actor, err := store.Open(store.Options{
		Dir:      t.TempDir(),
		DiskID:   "disk-cross-partial",
		BranchID: "main",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, actor.Close()) }()

	f, err := actor.Create(store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("cross.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	// 1. 跨 1 MiB 块边界的非对齐写入：从 offset = 1 MiB - 4 KiB 写入 1 MiB + 8 KiB（跨越 block 0 尾部、block 1 整块、block 2 头部）
	startOffset := uint64(store.BlobSize - 4096)
	crossPayload := make([]byte, 4096+store.BlobSize+4096)
	for i := range crossPayload {
		crossPayload[i] = byte((i*31 + 7) & 0xff)
	}
	n, attr, err := actor.Write(f.Ino, startOffset, crossPayload)
	require.NoError(t, err)
	require.Equal(t, uint32(len(crossPayload)), n)
	require.Equal(t, startOffset+uint64(len(crossPayload)), attr.Size)
	require.Equal(t, 1, countLiveBlobFiles(t, actor.Blobs()))

	// 再次在跨块边界覆写（此时 block 0 与 block 2 均为 WindowLocal=true，走 PatchLog）
	crossPatch := bytes.Repeat([]byte{0xab}, 8192)
	_, _, err = actor.Write(f.Ino, uint64(store.BlobSize-4096), crossPatch)
	require.NoError(t, err)

	gotCross, err := actor.Read(f.Ino, uint64(store.BlobSize-4096), 8192)
	require.NoError(t, err)
	require.Equal(t, crossPatch, gotCross)

	// 2. 验证 PatchLog 扩容内存切片超出单分片预算（4 MiB）时自动触发 trimLocked 淘汰最旧热块：
	// 向不同文件写入多个小块，随后通过 WindowLocal PatchLog 扩容至 1 MiB，验证数据完整性。
	for i := range 20 {
		fi, cErr := actor.Create(store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte{'s', byte('a' + i)},
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, cErr)
		// 先写 4 KiB
		_, _, wErr := actor.Write(fi.Ino, 0, bytes.Repeat([]byte{byte(i + 1)}, 4096))
		require.NoError(t, wErr)
		// 再通过 WindowLocal PatchLog 扩充到接近 1 MiB
		_, _, wErr = actor.Write(fi.Ino, 4096, bytes.Repeat([]byte{byte(i + 1)}, store.BlobSize-4096))
		require.NoError(t, wErr)

		got, rErr := actor.Read(fi.Ino, 0, 8192)
		require.NoError(t, rErr)
		require.Equal(t, bytes.Repeat([]byte{byte(i + 1)}, 8192), got)
	}
}
