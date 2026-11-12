package store_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/store"
)

func fileNlink(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	v := reflect.ValueOf(info.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	field := v.FieldByName("Nlink")
	require.True(t, field.IsValid(), "Stat_t 必须包含 Nlink 字段")
	return field.Uint()
}

type mockLineageAllocator struct {
	mu         sync.Mutex
	nextIno    uint64
	nextPackID uint64
	inoCalls   int
	packCalls  int
}

func newMockLineageAllocator() *mockLineageAllocator {
	return &mockLineageAllocator{
		nextIno:    2,
		nextPackID: 1,
	}
}

func (a *mockLineageAllocator) allocInodeSegment(minNext uint64) (first, last uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.inoCalls++
	start := max(a.nextIno, minNext)
	end := start + store.InodeSegment - 1
	a.nextIno = end + 1
	return start, end, nil
}

func (a *mockLineageAllocator) allocPackSegment(minNext uint64) (first, last uint64, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.packCalls++
	start := max(a.nextPackID, minNext)
	end := start + store.PackSegment - 1
	a.nextPackID = end + 1
	return start, end, nil
}

func TestCheckpointSealParallelHardLinksAndCloneMetadataCopy(t *testing.T) {
	ctx := context.Background()
	srcDir := filepath.Join(t.TempDir(), "src-disk")
	commitDir := filepath.Join(t.TempDir(), "commits", "commit-01")
	childDir := filepath.Join(t.TempDir(), "disks", "child-01")

	srcActor, err := store.Open(store.Options{
		Dir:       srcDir,
		DiskID:    "src-disk",
		MaxBytes:  256 << 20,
		AtimeMode: store.AtimeModeStrictAtime,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, srcActor.Close()) }()

	// 将单包上限设为 1 个满块（256 KiB），写入 35 个满块以覆盖 >= 32 个包的 8 worker 并发硬链接路径，
	// 再追加 1 个 64 KiB 尾块保持最后一个 .pack 处于活跃未满状态。
	srcActor.Blobs().SetMaxPackSize(store.BlobSize)

	ep1, _, err := srcActor.AdvanceEpoch(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), ep1)
	ep2, _, err := srcActor.AdvanceEpoch(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(2), ep2)

	fAttr, err := srcActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("dataset.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     ep2,
	})
	require.NoError(t, err)

	const fullPackCount = 35
	for i := range fullPackCount {
		blk := bytes.Repeat([]byte{byte(i + 1)}, store.BlobSize)
		_, _, writeErr := srcActor.Write(ctx, fAttr.Ino, uint64(i)*store.BlobSize, blk, ep2)
		require.NoError(t, writeErr)
	}
	tailPayload := bytes.Repeat([]byte{0xFE}, 64*1024)
	_, _, err = srcActor.Write(ctx, fAttr.Ino, uint64(fullPackCount)*store.BlobSize, tailPayload, ep2)
	require.NoError(t, err)
	require.NotZero(t, srcActor.Blobs().ActivePackID(), "Checkpoint 前最后一个包应处于活跃可写状态")

	expectedAtime := time.Now().Add(10 * time.Minute).Truncate(time.Nanosecond)
	srcActor.MarkAtimeDirty(fAttr.Ino, expectedAtime)

	hasOrphans, err := srcActor.HasOrphans()
	require.NoError(t, err)
	require.False(t, hasOrphans)

	stats, err := srcActor.Checkpoint(ctx, commitDir)
	require.NoError(t, err)
	require.Equal(t, uint64(fullPackCount*store.BlobSize+len(tailPayload)), stats.LogicalBytes)
	require.Equal(t, uint64(2), stats.Inodes)
	require.Equal(t, uint64(256<<20), stats.MaxBytes)
	require.False(t, stats.HasOrphans)

	// Checkpoint 全程不得递增源盘 WriterEpoch，且活跃 .pack 已被封口重置为 0。
	require.Equal(t, ep2, srcActor.WriterEpoch())
	require.Zero(t, srcActor.Blobs().ActivePackID())

	// 校验 36 个存活 .pack 在源盘与 commitDir 之间为同一硬链接且 st_nlink == 2。
	commitPackEntries, err := os.ReadDir(filepath.Join(commitDir, "packs"))
	require.NoError(t, err)
	require.Len(t, commitPackEntries, fullPackCount+1)
	for _, entry := range commitPackEntries {
		srcPackPath := filepath.Join(srcDir, "packs", entry.Name())
		commitPackPath := filepath.Join(commitDir, "packs", entry.Name())
		srcInfo, statErr := os.Stat(srcPackPath)
		require.NoError(t, statErr)
		commitInfo, statErr := os.Stat(commitPackPath)
		require.NoError(t, statErr)
		require.True(t, os.SameFile(srcInfo, commitInfo), "存活 .pack 必须通过硬链接共享底层 inode")
		require.Equal(t, uint64(2), fileNlink(t, commitPackPath))
	}

	// 克隆切面目录至子盘目录（再次覆盖 >= 32 个包的并发克隆路径）。
	require.NoError(t, store.CloneCheckpointDir(commitDir, childDir))

	// 验证子盘目录的硬链接与元数据字节复制隔离契约。
	topEntries, err := os.ReadDir(childDir)
	require.NoError(t, err)
	sawSST := false
	sawMetadata := false
	for _, entry := range topEntries {
		name := entry.Name()
		require.NotEqual(t, "LOCK", name, "CloneCheckpointDir 必须跳过 LOCK 文件")
		if entry.IsDir() {
			continue
		}
		commitFile := filepath.Join(commitDir, name)
		childFile := filepath.Join(childDir, name)
		commitInfo, statErr := os.Stat(commitFile)
		require.NoError(t, statErr)
		childInfo, statErr := os.Stat(childFile)
		require.NoError(t, statErr)

		if strings.HasSuffix(name, ".sst") || strings.HasSuffix(name, ".blob") {
			sawSST = true
			require.True(t, os.SameFile(commitInfo, childInfo), "不可变 SST/Blob 文件必须硬链接")
			require.GreaterOrEqual(t, fileNlink(t, childFile), uint64(2))
		} else {
			sawMetadata = true
			require.False(t, os.SameFile(commitInfo, childInfo), "可变或元数据文件 %s 严禁硬链接", name)
			require.Equal(t, uint64(1), fileNlink(t, childFile), "元数据文件 %s 引用计数必须为 1", name)
		}
	}
	require.True(t, sawSST, "db.Flush() 后切面目录中必须存在不可变 .sst 文件")
	require.True(t, sawMetadata, "克隆目录中必须包含字节复制的 Pebble 元数据文件")

	for _, entry := range commitPackEntries {
		childPackPath := filepath.Join(childDir, "packs", entry.Name())
		require.Equal(t, uint64(3), fileNlink(t, childPackPath), "源盘 + 切面 + 子盘三方共享时 .pack 引用计数应为 3")
	}
}

func TestCloneIsolationResetEpochAndMaxBytesOverride(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	srcDir := filepath.Join(rootDir, "src")
	commitDir := filepath.Join(rootDir, "commit")
	childShrinkDir := filepath.Join(rootDir, "child-shrink")
	childInheritDir := filepath.Join(rootDir, "child-inherit")
	childExpandDir := filepath.Join(rootDir, "child-expand")

	srcActor, err := store.Open(store.Options{
		Dir:       srcDir,
		DiskID:    "src",
		MaxBytes:  64 << 20,
		AtimeMode: store.AtimeModeStrictAtime,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, srcActor.Close()) }()

	ep, _, err := srcActor.AdvanceEpoch(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(1), ep)

	baseFile, err := srcActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("shared.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     ep,
	})
	require.NoError(t, err)
	originalPayload := bytes.Repeat([]byte("BASE-COMMIT-DATA"), store.BlobSize/16) // 256 KiB
	_, _, err = srcActor.Write(ctx, baseFile.Ino, 0, originalPayload, ep)
	require.NoError(t, err)

	atimeNano := time.Now().Add(5 * time.Minute).UnixNano()
	srcActor.MarkAtimeDirty(baseFile.Ino, time.Unix(0, atimeNano))

	_, err = srcActor.Checkpoint(ctx, commitDir)
	require.NoError(t, err)

	// 1. 验证缩容低于已用空间时 ResetEpoch 打开被拒绝。
	require.NoError(t, store.CloneCheckpointDir(commitDir, childShrinkDir))
	_, err = store.Open(store.Options{
		Dir:        childShrinkDir,
		DiskID:     "child-shrink",
		MaxBytes:   128 << 10, // 128 KiB < 256 KiB 已用逻辑字节
		ResetEpoch: true,
	})
	require.ErrorIs(t, err, store.ErrQuotaShrinkBelowUsed)

	// 2. 验证默认继承配额（MaxBytes: 0）且 ResetEpoch 归零持久化。
	require.NoError(t, store.CloneCheckpointDir(commitDir, childInheritDir))
	childInherit, err := store.Open(store.Options{
		Dir:        childInheritDir,
		DiskID:     "child-inherit",
		ResetEpoch: true,
	})
	require.NoError(t, err)
	require.Zero(t, childInherit.WriterEpoch())
	require.Equal(t, uint64(64<<20), childInherit.MaxBytes())
	gotAttr, err := childInherit.GetAttr(ctx, baseFile.Ino)
	require.NoError(t, err)
	require.Equal(t, atimeNano, gotAttr.Atime, "opCheckpoint 前登记的 dirtyAtime 必须已持久化至切面")
	require.NoError(t, childInherit.Close())

	// 3. 验证显式扩容 MaxBytes + ResetEpoch 持久化，并在子盘和源盘各自写入、删除，互不污染切面。
	require.NoError(t, store.CloneCheckpointDir(commitDir, childExpandDir))
	childExpand, err := store.Open(store.Options{
		Dir:        childExpandDir,
		DiskID:     "child-expand",
		MaxBytes:   512 << 20,
		ResetEpoch: true,
	})
	require.NoError(t, err)
	require.Zero(t, childExpand.WriterEpoch())
	require.Equal(t, uint64(512<<20), childExpand.MaxBytes())

	// 子盘以 Epoch 0 覆写 shared.txt 并新建 child_only.txt。
	childPayload := bytes.Repeat([]byte("CHILD-MUTATION!!"), store.BlobSize/16)
	_, _, err = childExpand.Write(ctx, baseFile.Ino, 0, childPayload, 0)
	require.NoError(t, err)
	_, err = childExpand.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("child_only.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     0,
	})
	require.NoError(t, err)
	require.NoError(t, childExpand.DB().Flush())
	require.NoError(t, childExpand.Close())

	// 重启打开（ResetEpoch: false）验证 WriterEpoch == 0 与 MaxBytes == 512 MiB 已在 sys:stat 持久化。
	reopenedChild, err := store.Open(store.Options{
		Dir:    childExpandDir,
		DiskID: "child-expand",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, reopenedChild.Close()) }()
	require.Zero(t, reopenedChild.WriterEpoch())
	require.Equal(t, uint64(512<<20), reopenedChild.MaxBytes())

	// 源盘删除 shared.txt 并刷盘回收源盘侧死包，验证切面与子盘不受影响。
	require.NoError(t, srcActor.Unlink(ctx, store.RootIno, []byte("shared.txt"), false, ep))
	require.NoError(t, srcActor.FlushWAL())

	// 以只读模式打开不可变切面 commitDir，验证切面内容与统计丝毫未受源盘和子盘污染。
	roCommit, err := store.Open(store.Options{
		Dir:      commitDir,
		DiskID:   "ro-commit",
		ReadOnly: true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, roCommit.Close()) }()

	roCommit.Blobs().EvictHot(store.BlobRef{PackID: 1, Offset: 0, Length: store.BlobSize})
	gotCommitData, err := roCommit.Read(ctx, baseFile.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, originalPayload, gotCommitData)
	_, _, err = roCommit.Lookup(ctx, store.RootIno, []byte("child_only.txt"))
	require.ErrorIs(t, err, store.ErrNotFound)

	gotChildData, err := reopenedChild.Read(ctx, baseFile.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, childPayload, gotChildData)
}

func TestSanitizeCheckpointOrphansAndDeadPackUnlink(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	srcDir := filepath.Join(rootDir, "src")
	commitDir := filepath.Join(rootDir, "commit")
	childDir := filepath.Join(rootDir, "child")

	srcActor, err := store.Open(store.Options{
		Dir:      srcDir,
		DiskID:   "src-orphan",
		MaxBytes: 128 << 20,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, srcActor.Close()) }()
	srcActor.Blobs().SetMaxPackSize(store.BlobSize)

	ep, _, err := srcActor.AdvanceEpoch(ctx)
	require.NoError(t, err)

	// Pack 1：写入存活文件 keep.bin（256 KiB）
	keepAttr, err := srcActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("keep.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     ep,
	})
	require.NoError(t, err)
	keepData := bytes.Repeat([]byte{0x41}, store.BlobSize)
	_, _, err = srcActor.Write(ctx, keepAttr.Ino, 0, keepData, ep)
	require.NoError(t, err)

	// Pack 2 & Pack 3：写入临时文件 temp_open.bin（512 KiB），随后以 openHandle=true 执行 Unlink 转为孤儿节点。
	orphanAttr, err := srcActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("temp_open.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     ep,
	})
	require.NoError(t, err)
	orphanData := bytes.Repeat([]byte{0x7F}, 2*store.BlobSize)
	_, _, err = srcActor.Write(ctx, orphanAttr.Ino, 0, orphanData, ep)
	require.NoError(t, err)
	require.NoError(t, srcActor.SetXattr(ctx, orphanAttr.Ino, []byte("user.tmp"), []byte("ephemeral"), 0, ep))

	hasBefore, err := srcActor.HasOrphans()
	require.NoError(t, err)
	require.False(t, hasBefore)

	require.NoError(t, srcActor.Unlink(ctx, store.RootIno, []byte("temp_open.bin"), true, ep))
	hasAfter, err := srcActor.HasOrphans()
	require.NoError(t, err)
	require.True(t, hasAfter)

	// 生成切面：此时切面包含孤儿节点，且硬链接了 Pack 1、2、3（st_nlink == 2）。
	ckStats, err := srcActor.Checkpoint(ctx, commitDir)
	require.NoError(t, err)
	require.True(t, ckStats.HasOrphans)
	require.Equal(t, uint64(3*store.BlobSize), ckStats.LogicalBytes)
	require.Equal(t, uint64(3), ckStats.Inodes)

	require.Equal(t, uint64(2), fileNlink(t, srcActor.Blobs().Path(1)))
	require.Equal(t, uint64(2), fileNlink(t, srcActor.Blobs().Path(2)))
	require.Equal(t, uint64(2), fileNlink(t, srcActor.Blobs().Path(3)))

	// 执行切面孤儿净化：移除孤儿节点及其独占的 Pack 2、Pack 3。
	sharedCache := pebble.NewCache(16 << 20)
	defer sharedCache.Unref()

	sanitizedStat, err := store.SanitizeCheckpoint(commitDir, sharedCache)
	require.NoError(t, err)
	require.Equal(t, uint64(store.BlobSize), sanitizedStat.UsedBytes)
	require.Equal(t, uint64(2), sanitizedStat.Files)

	// Pack 2、3 已从 commitDir 解链删除，源盘对应 .pack 的 st_nlink 立即恢复为 1；Pack 1 仍为 2。
	require.Equal(t, uint64(2), fileNlink(t, srcActor.Blobs().Path(1)))
	require.Equal(t, uint64(1), fileNlink(t, srcActor.Blobs().Path(2)))
	require.Equal(t, uint64(1), fileNlink(t, srcActor.Blobs().Path(3)))
	_, err = os.Stat(filepath.Join(commitDir, "packs", "0000000000000002.pack"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Stat(filepath.Join(commitDir, "packs", "0000000000000003.pack"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// 源盘上的未关闭孤儿句柄仍然正常可读，不受切面净化影响。
	srcActor.Blobs().EvictHot(store.BlobRef{PackID: 2, Offset: 0, Length: store.BlobSize})
	gotOrphanFromSrc, err := srcActor.Read(ctx, orphanAttr.Ino, 0, 2*store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, orphanData, gotOrphanFromSrc)

	// 从净化后的切面克隆出子盘打开：WriterEpoch == 0、HasOrphans == false、keep.bin 完好、孤儿彻底不存在。
	require.NoError(t, store.CloneCheckpointDir(commitDir, childDir))
	childActor, err := store.Open(store.Options{
		Dir:    childDir,
		DiskID: "child-sanitized",
		Cache:  sharedCache,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, childActor.Close()) }()

	require.Zero(t, childActor.WriterEpoch())
	childHasOrphans, err := childActor.HasOrphans()
	require.NoError(t, err)
	require.False(t, childHasOrphans)
	_, err = childActor.GetAttr(ctx, orphanAttr.Ino)
	require.ErrorIs(t, err, store.ErrNotFound)

	gotKeep, err := childActor.Read(ctx, keepAttr.Ino, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, keepData, gotKeep)
}

func TestExternalSegmentAllocatorsCrossBranchCollisionFree(t *testing.T) {
	ctx := context.Background()
	lineage := newMockLineageAllocator()
	rootDir := t.TempDir()

	parentDir := filepath.Join(rootDir, "parent")
	commitDir := filepath.Join(rootDir, "commit-lca")
	childADir := filepath.Join(rootDir, "child-a")
	childBDir := filepath.Join(rootDir, "child-b")

	// 先在未接分配器的目录模拟存量高水位节点与封包（验证首次分配 minNext 保底推进）。
	seedActor, err := store.Open(store.Options{
		Dir:      parentDir,
		DiskID:   "parent",
		MaxBytes: 512 << 20,
	})
	require.NoError(t, err)
	seedFile, err := seedActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("seed.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = seedActor.Write(ctx, seedFile.Ino, 0, bytes.Repeat([]byte{0x12}, 4096))
	require.NoError(t, err)
	require.NoError(t, seedActor.Close())

	// 重新打开父盘并注入血缘号段分配器。
	parentActor, err := store.Open(store.Options{
		Dir:           parentDir,
		DiskID:        "parent",
		MaxBytes:      512 << 20,
		InoAllocator:  lineage.allocInodeSegment,
		PackAllocator: lineage.allocPackSegment,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, parentActor.Close()) }()
	parentActor.Blobs().SetMaxPackSize(store.BlobSize)

	baseFile, err := parentActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("base.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	require.Greater(t, baseFile.Ino, seedFile.Ino, "外部 Inode 分配器必须以本地已有最大 Inode+1 保底")
	_, _, err = parentActor.Write(ctx, baseFile.Ino, 0, bytes.Repeat([]byte{0x34}, store.BlobSize))
	require.NoError(t, err)

	// 固化共同祖先切面并克隆两个兄弟子盘，三盘共享同一血缘分配器。
	_, err = parentActor.Checkpoint(ctx, commitDir)
	require.NoError(t, err)
	require.NoError(t, store.CloneCheckpointDir(commitDir, childADir))
	require.NoError(t, store.CloneCheckpointDir(commitDir, childBDir))

	childA, err := store.Open(store.Options{
		Dir:           childADir,
		DiskID:        "child-a",
		InoAllocator:  lineage.allocInodeSegment,
		PackAllocator: lineage.allocPackSegment,
		ResetEpoch:    true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, childA.Close()) }()
	childA.Blobs().SetMaxPackSize(store.BlobSize)

	childB, err := store.Open(store.Options{
		Dir:           childBDir,
		DiskID:        "child-b",
		InoAllocator:  lineage.allocInodeSegment,
		PackAllocator: lineage.allocPackSegment,
		ResetEpoch:    true,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, childB.Close()) }()
	childB.Blobs().SetMaxPackSize(store.BlobSize)

	actors := []*store.StoreActor{parentActor, childA, childB}
	branchInos := make([][]uint64, len(actors))

	// 每个分支并发创建 70 个满块文件（跨越 PackSegment = 64 边界触发二次号段预留）。
	const filesPerBranch = 70
	var wg sync.WaitGroup
	for idx, act := range actors {
		wg.Go(func() {
			inos := make([]uint64, 0, filesPerBranch)
			for i := range filesPerBranch {
				name := []byte("f_" + strconv.Itoa(idx) + "_" + strconv.Itoa(i))
				attr, createErr := act.Create(ctx, store.CreateReq{
					ParentIno: store.RootIno,
					Name:      name,
					Mode:      store.S_IFREG | 0o644,
				})
				require.NoError(t, createErr)
				inos = append(inos, attr.Ino)

				payload := bytes.Repeat([]byte{byte(idx*80 + i + 1)}, store.BlobSize)
				_, _, writeErr := act.Write(ctx, attr.Ino, 0, payload)
				require.NoError(t, writeErr)
			}
			branchInos[idx] = inos
		})
	}
	wg.Wait()

	// 收集各盘在基准切面之后新分配的全部 Inode 与 PackID，验证全局零碰撞。
	basePackEntries, err := os.ReadDir(filepath.Join(commitDir, "packs"))
	require.NoError(t, err)
	basePacks := make(map[string]struct{}, len(basePackEntries))
	for _, entry := range basePackEntries {
		basePacks[entry.Name()] = struct{}{}
	}

	seenInos := make(map[uint64]int)
	seenPacks := make(map[string]int)
	for branchIdx, act := range actors {
		for _, ino := range branchInos[branchIdx] {
			require.Greater(t, ino, baseFile.Ino)
			prevBranch, exists := seenInos[ino]
			require.False(t, exists, "Inode %d 在分支 %d 与分支 %d 之间发生碰撞", ino, prevBranch, branchIdx)
			seenInos[ino] = branchIdx
		}
		packEntries, readErr := os.ReadDir(act.Blobs().Root())
		require.NoError(t, readErr)
		for _, entry := range packEntries {
			if _, inBase := basePacks[entry.Name()]; inBase {
				continue
			}
			prevBranch, exists := seenPacks[entry.Name()]
			require.False(t, exists, "Pack %s 在分支 %d 与分支 %d 之间发生碰撞", entry.Name(), prevBranch, branchIdx)
			seenPacks[entry.Name()] = branchIdx
		}
	}
	require.Len(t, seenInos, len(actors)*filesPerBranch)
	require.Len(t, seenPacks, len(actors)*filesPerBranch)
}

func TestCheckpointUnderConcurrentWritesAndCompactPacks(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	srcDir := filepath.Join(rootDir, "src-concurrent")

	srcActor, err := store.Open(store.Options{
		Dir:      srcDir,
		DiskID:   "src-concurrent",
		MaxBytes: 1 << 30,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, srcActor.Close()) }()

	// 每包上限 2 个块（512 KiB），高频制造切包与低水位碎片搬迁。
	srcActor.Blobs().SetMaxPackSize(2 * store.BlobSize)

	const (
		workerCount     = 4
		checkpointCount = 5
	)

	// 预创建各 worker 专属文件并写入初始块，同时写入将要被覆写的块以产生低水位包供 CompactPacks 搬迁。
	workerInos := make([]uint64, workerCount)
	for w := range workerCount {
		attr, createErr := srcActor.Create(ctx, store.CreateReq{
			ParentIno: store.RootIno,
			Name:      []byte("worker_" + strconv.Itoa(w) + ".bin"),
			Mode:      store.S_IFREG | 0o644,
		})
		require.NoError(t, createErr)
		workerInos[w] = attr.Ino
		initPayload := bytes.Repeat([]byte{byte(w + 1)}, 2*store.BlobSize)
		_, _, writeErr := srcActor.Write(ctx, attr.Ino, 0, initPayload)
		require.NoError(t, writeErr)
	}

	var stopFlag atomic.Bool
	var bgWG sync.WaitGroup

	// 启动并发前台写入协程（覆盖满块 Stage 1 并发写与非对齐局部 RMW/ExtendTail 写）。
	for w := range workerCount {
		bgWG.Go(func() {
			ino := workerInos[w]
			var iter uint32 = 1
			for !stopFlag.Load() {
				tag := byte((int(iter) + w*17) & 0xFF)
				if tag == 0 {
					tag = 1
				}
				// 覆写块 0（满 256 KiB 对齐 Stage 1 并发写，持续产生旧包死块触发低水位）
				fullBlk := bytes.Repeat([]byte{tag}, store.BlobSize)
				_, _, writeErr := srcActor.Write(ctx, ino, 0, fullBlk)
				require.NoError(t, writeErr)

				// 局部写入块 1 的后半段（走持锁局部 RMW / ExtendTail 路径）
				partial := bytes.Repeat([]byte{tag ^ 0x5A}, 32*1024)
				_, _, writeErr = srcActor.Write(ctx, ino, store.BlobSize+16*1024, partial)
				require.NoError(t, writeErr)

				iter++
			}
		})
	}

	// 启动并发后台 CompactPacks 协程。
	bgWG.Go(func() {
		for !stopFlag.Load() {
			require.NoError(t, srcActor.CompactPacks(ctx))
			time.Sleep(2 * time.Millisecond)
		}
	})

	commitDirs := make([]string, checkpointCount)
	for i := range checkpointCount {
		time.Sleep(8 * time.Millisecond)
		cDir := filepath.Join(rootDir, "commits", "ck_"+strconv.Itoa(i))
		_, ckErr := srcActor.Checkpoint(ctx, cDir)
		require.NoError(t, ckErr)
		commitDirs[i] = cDir
	}

	stopFlag.Store(true)
	bgWG.Wait()

	// 逐个克隆并打开在线并发期间生成的切面，验证所有块冷读无损且每个单块内部字节完全原子一致（无撕裂写）。
	for i, cDir := range commitDirs {
		cloneDir := filepath.Join(rootDir, "clones", "clone_"+strconv.Itoa(i))
		require.NoError(t, store.CloneCheckpointDir(cDir, cloneDir))

		cloneActor, openErr := store.Open(store.Options{
			Dir:        cloneDir,
			DiskID:     "clone-" + strconv.Itoa(i),
			ResetEpoch: true,
		})
		require.NoError(t, openErr)

		for w, ino := range workerInos {
			attr, attrErr := cloneActor.GetAttr(ctx, ino)
			require.NoError(t, attrErr)
			require.Equal(t, uint64(2*store.BlobSize), attr.Size, "worker %d 文件大小在任意切面必须一致", w)

			blk0, readErr := cloneActor.Read(ctx, ino, 0, store.BlobSize)
			require.NoError(t, readErr)
			require.Len(t, blk0, store.BlobSize)
			expectedByte := blk0[0]
			require.Equal(t, bytes.Repeat([]byte{expectedByte}, store.BlobSize), blk0, "256 KiB 满块写不得出现跨版本撕裂")

			blk1, readErr := cloneActor.Read(ctx, ino, store.BlobSize, store.BlobSize)
			require.NoError(t, readErr)
			require.Len(t, blk1, store.BlobSize)
		}

		require.NoError(t, cloneActor.Close())
	}
}

func TestForkErrorBoundariesAndAllocatorFailure(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	srcDir := filepath.Join(rootDir, "src")

	allocErr := errors.New("lineage db unavailable")
	failActor, err := store.Open(store.Options{
		Dir:    filepath.Join(rootDir, "fail-alloc"),
		DiskID: "fail-alloc",
		InoAllocator: func(_ uint64) (uint64, uint64, error) {
			return 0, 0, allocErr
		},
		PackAllocator: func(_ uint64) (uint64, uint64, error) {
			return 0, 0, allocErr
		},
	})
	require.NoError(t, err)
	_, err = failActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("a.txt"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.ErrorIs(t, err, allocErr)
	_, err = failActor.Blobs().Append(2, []byte("payload"))
	require.ErrorIs(t, err, allocErr)
	require.NoError(t, failActor.Close())

	srcActor, err := store.Open(store.Options{
		Dir:    srcDir,
		DiskID: "src",
	})
	require.NoError(t, err)

	// Checkpoint 目标为空或已存在目录时应拒绝且不破坏已有目录内容。
	_, err = srcActor.Checkpoint(ctx, "")
	require.ErrorIs(t, err, store.ErrInvalid)

	existingDir := filepath.Join(rootDir, "existing")
	require.NoError(t, os.MkdirAll(existingDir, 0o755))
	sentinelPath := filepath.Join(existingDir, "keep.txt")
	require.NoError(t, os.WriteFile(sentinelPath, []byte("safe"), 0o644))

	_, err = srcActor.Checkpoint(ctx, existingDir)
	require.ErrorIs(t, err, store.ErrExist)
	gotSentinel, err := os.ReadFile(sentinelPath)
	require.NoError(t, err)
	require.Equal(t, []byte("safe"), gotSentinel, "Checkpoint 拒绝已存在目录时不得删除原目录文件")

	commitDir := filepath.Join(rootDir, "valid-commit")
	_, err = srcActor.Checkpoint(ctx, commitDir)
	require.NoError(t, err)
	require.NoError(t, srcActor.Close())

	// 只读实例调用 Checkpoint 返回 ErrReadOnly。
	roActor, err := store.Open(store.Options{
		Dir:      commitDir,
		DiskID:   "ro",
		ReadOnly: true,
	})
	require.NoError(t, err)
	_, err = roActor.Checkpoint(ctx, filepath.Join(rootDir, "ro-ck"))
	require.ErrorIs(t, err, store.ErrReadOnly)
	require.NoError(t, roActor.Close())

	// CloneCheckpointDir 边界校验：空路径、同路径、目标已存在（不误删已有目录）。
	require.ErrorIs(t, store.CloneCheckpointDir("", filepath.Join(rootDir, "dst")), store.ErrInvalid)
	require.ErrorIs(t, store.CloneCheckpointDir(commitDir, commitDir), store.ErrInvalid)
	require.ErrorIs(t, store.CloneCheckpointDir(commitDir, existingDir), store.ErrExist)
	gotSentinel, err = os.ReadFile(sentinelPath)
	require.NoError(t, err)
	require.Equal(t, []byte("safe"), gotSentinel, "CloneCheckpointDir 拒绝已存在目标目录时不得破坏已有文件")
}
