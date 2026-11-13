package control

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

func TestFindLCA_LinearMultiMergeAndCrissCrossDAG(t *testing.T) {
	// Given: 构造包含线性提交、单向合并以及经典 Criss-Cross 交叉合并的有向无环图
	lineageID := uuid.Must(uuid.NewV7())
	c0 := uuid.Must(uuid.NewV7())
	c1 := uuid.Must(uuid.NewV7())
	c2 := uuid.Must(uuid.NewV7())
	c3 := uuid.Must(uuid.NewV7())
	c4 := uuid.Must(uuid.NewV7())
	c5 := uuid.Must(uuid.NewV7())

	// DAG:
	//   c0 -> c1 -> c3 (merge parent: c2)
	//   c0 -> c2 -> c4 (merge parent: c1)
	//   c3 -> c5
	//   c1 与 c2 同为 (c3, c4) 的极大公共祖先 (Criss-Cross)
	commits := []db.CommitRecord{
		{LineageID: lineageID, CommitID: c0},
		{LineageID: lineageID, CommitID: c1, ParentCommitID: c0},
		{LineageID: lineageID, CommitID: c2, ParentCommitID: c0},
		{LineageID: lineageID, CommitID: c3, ParentCommitID: c1, MergeParentCommitID: c2},
		{LineageID: lineageID, CommitID: c4, ParentCommitID: c2, MergeParentCommitID: c1},
		{LineageID: lineageID, CommitID: c5, ParentCommitID: c3},
	}

	// When / Then 1: 同一提交 LCA 为其自身
	gotSame, err := findLCA(commits, c1, c1)
	require.NoError(t, err)
	require.Equal(t, c1, gotSame)

	// When / Then 2: 单向合并后继续前进 (c5 与 c2 的公共祖先为 {c0, c2}，唯一 MCA 为 c2)
	gotMerged, err := findLCA(commits, c5, c2)
	require.NoError(t, err)
	require.Equal(t, c2, gotMerged)

	// When / Then 3: Criss-Cross (c3 与 c4 的公共祖先为 {c0, c1, c2}，MCA 为 {c1, c2}) 应拒绝并返回 ErrAmbiguousLCA
	_, err = findLCA(commits, c3, c4)
	require.ErrorIs(t, err, ErrAmbiguousLCA)
}

func TestDiffDisk_SelfBaseAndTargetDisk_UsesCheckpointForOnlineRWActors(t *testing.T) {
	// Given: 在线挂载的源盘与派生子盘，各自写入未关闭刷盘的新修改
	ctx := context.Background()
	rootDir := t.TempDir()
	mgr := New(db.NewMemStore(), WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 64 << 20})
	require.NoError(t, err)
	srcActor, err := mgr.Actor(ctx, src.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, srcActor, 0, "base.txt", []byte("base-v1\n"))
	writeRegularFile(t, ctx, srcActor, 0, "shared.txt", []byte("base-shared\n"))

	child, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "feat"})
	require.NoError(t, err)

	// 在保持两个磁盘 Actor 均处于在线 RW 打开状态下执行写入
	childActor, err := mgr.Actor(ctx, child.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, childActor, 0, "feat.txt", []byte("feature-added\n"))
	overwriteRegularFile(t, ctx, childActor, 0, "shared.txt", []byte("child-shared\n"))
	overwriteRegularFile(t, ctx, srcActor, 0, "shared.txt", []byte("main-shared\n"))

	// When 1: 省略 TargetDiskID，将 child 与其自身 BaseCommitID 对比（两路 diff）
	twoWay, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: child.DiskID,
	})

	// Then 1: 返回新增 feat.txt 与修改 shared.txt，且未泄露临时快照目录
	require.NoError(t, err)
	require.NotEmpty(t, twoWay.LCACommitID)
	require.Equal(t, uint64(1), twoWay.Summary.Added)
	require.Equal(t, uint64(1), twoWay.Summary.Modified)
	require.Equal(t, uint64(0), twoWay.Summary.Conflicts)
	require.Len(t, twoWay.Entries, 2)

	// When 2: 指定 TargetDiskID=main，执行三路 diff 与 ConflictsOnly 过滤
	threeWay, err := mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID:  child.DiskID,
		TargetDiskID:  src.DiskID,
		ConflictsOnly: true,
	})

	// Then 2: 精准检出 shared.txt 的 CONTENT 冲突
	require.NoError(t, err)
	require.Equal(t, twoWay.LCACommitID, threeWay.LCACommitID)
	require.Equal(t, uint64(1), threeWay.Summary.Conflicts)
	require.Len(t, threeWay.Entries, 1)
	require.Equal(t, "shared.txt", string(threeWay.Entries[0].Path))
	require.Equal(t, store.ConflictContent, threeWay.Entries[0].Conflict)

	// When 3: 跨血缘另一独立盘 Diff 应被拒绝
	unrelated, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "other", MaxBytes: 16 << 20})
	require.NoError(t, err)
	_, err = mgr.DiffDisk(ctx, &DiffDiskParams{
		SourceDiskID: child.DiskID,
		TargetDiskID: unrelated.DiskID,
	})
	require.ErrorIs(t, err, ErrCrossLineage)

	// Then 3: .tmp 临时快照目录已被完全清理
	tmpEntries, readErr := os.ReadDir(filepath.Join(rootDir, ".tmp"))
	if readErr == nil {
		require.Empty(t, tmpEntries, "DiffDisk 完成后不应残留临时快照目录")
	}
}

func TestReadCommitNode_EnforcesLineageMembershipAndReadsImmutableSlice(t *testing.T) {
	// Given: 包含普通文件与符号链接的基线提交，以及同血缘与跨血缘磁盘
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	mainDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 32 << 20})
	require.NoError(t, err)
	mainActor, err := mgr.Actor(ctx, mainDisk.DiskID)
	require.NoError(t, err)
	ino := writeRegularFile(t, ctx, mainActor, 0, "doc.txt", []byte("hello-immutable-commit"))
	symAttr, err := mainActor.Symlink(ctx, store.SymlinkReq{
		ParentIno: RootIno,
		Name:      []byte("link.txt"),
		Target:    []byte("/doc.txt"),
	})
	require.NoError(t, err)

	forkDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: mainDisk.DiskID, Name: "branch"})
	require.NoError(t, err)
	otherDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "unrelated", MaxBytes: 16 << 20})
	require.NoError(t, err)

	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{SourceDiskID: forkDisk.DiskID})
	require.NoError(t, err)
	commitID := diffRes.LCACommitID

	// 修改主盘覆盖文件后，不可变切面内容必须保持原样
	overwriteRegularFile(t, ctx, mainActor, 0, "doc.txt", []byte("mutated-after-fork"))

	// When 1: 分片读取普通文件
	slice1, err := mgr.ReadCommitNode(ctx, &ReadCommitNodeParams{
		DiskID:   forkDisk.DiskID,
		CommitID: commitID,
		Ino:      ino,
		Offset:   0,
		Size:     5,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), slice1.Data)
	require.False(t, slice1.EOF)
	require.Equal(t, uint64(len("hello-immutable-commit")), slice1.FileSize)

	slice2, err := mgr.ReadCommitNode(ctx, &ReadCommitNodeParams{
		DiskID:   forkDisk.DiskID,
		CommitID: commitID,
		Ino:      ino,
		Offset:   5,
		Size:     100,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("-immutable-commit"), slice2.Data)
	require.True(t, slice2.EOF)

	// When 2: 读取符号链接
	symRes, err := mgr.ReadCommitNode(ctx, &ReadCommitNodeParams{
		DiskID:   mainDisk.DiskID,
		CommitID: commitID,
		Ino:      symAttr.Ino,
		Offset:   0,
		Size:     1024,
	})
	require.NoError(t, err)
	require.Equal(t, []byte("/doc.txt"), symRes.SymlinkTarget)

	// When 3: 使用跨血缘 disk_id 读取该 commit 应拒绝并返回 ErrCrossLineage
	_, err = mgr.ReadCommitNode(ctx, &ReadCommitNodeParams{
		DiskID:   otherDisk.DiskID,
		CommitID: commitID,
		Ino:      ino,
		Offset:   0,
		Size:     16,
	})
	require.ErrorIs(t, err, ErrCrossLineage)
}

func TestMergeDisk_DryRunConflictResolutionEpochFenceAndSkipSourceCommit(t *testing.T) {
	// Given: 主盘与分支盘产生内容冲突以及源盘独立新增文件
	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "cfg.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)

	// 挂载 target 获取 epoch0
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: target.DiskID})
	require.NoError(t, err)
	epoch0 := mnt.Epoch

	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	overwriteRegularFile(t, ctx, sActor, 0, "cfg.txt", []byte("from-source"))
	writeRegularFile(t, ctx, sActor, 0, "new.txt", []byte("new-from-source"))

	// target 在 epoch0 下也修改 cfg.txt，制造冲突
	overwriteRegularFile(t, ctx, tActor, epoch0, "cfg.txt", []byte("from-target"))

	// 未传入 Force 且未 UnmountDisk 时，正式合并必须被在线租约门禁拦截为 ErrDiskMounted
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.ErrorIs(t, err, ErrDiskMounted)

	// When 1: Strategy=Fail + Force=true 合并应返回 *MergeConflictError 且不推进 epoch 或修改 target
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
		Force:        true,
	})
	var conflictErr *MergeConflictError
	require.ErrorAs(t, err, &conflictErr)
	require.Equal(t, uint64(1), conflictErr.Report.TotalConflicts)
	require.Equal(t, []byte("from-target"), readRegularFile(t, ctx, tActor, "cfg.txt", 1024))

	// When 2: DryRun=true + Strategy=Ours 应成功预演且不改动磁盘与 DB 提交历史
	tRow, err := memDB.GetDisk(ctx, uuid.MustParse(target.DiskID))
	require.NoError(t, err)
	lineageID := tRow.LineageID
	commitsBefore, err := memDB.ListLineageCommits(ctx, lineageID)
	require.NoError(t, err)

	newMaxBytes := uint64(64 << 20)
	dryRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyOurs,
		DryRun:       true,
		MaxBytes:     &newMaxBytes,
	})
	require.NoError(t, err)
	require.Empty(t, dryRes.MergeCommitID)
	require.Nil(t, dryRes.Disk)
	require.Equal(t, newMaxBytes, dryRes.TargetMaxBytes)

	commitsAfterDry, err := memDB.ListLineageCommits(ctx, lineageID)
	require.NoError(t, err)
	require.Len(t, commitsAfterDry, len(commitsBefore))

	// When 3: 正式合并（Force=true 抢占在线租约），cfg.txt 用 theirs，并扩容至 64 MiB
	res, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID:   source.DiskID,
		TargetDiskID:   target.DiskID,
		Strategy:       store.MergeStrategyOurs,
		PathStrategies: map[string]store.MergeStrategy{"/cfg.txt": store.MergeStrategyTheirs},
		Force:          true,
		MaxBytes:       &newMaxBytes,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.MergeCommitID)
	require.NotNil(t, res.Disk)
	require.Equal(t, newMaxBytes, res.Disk.MaxBytes)

	// 验证新 targetActor 已接管数据且旧 epoch0 被栅栏拒绝
	newTActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, epoch0+1, newTActor.WriterEpoch())
	require.Equal(t, []byte("from-source"), readRegularFile(t, ctx, newTActor, "cfg.txt", 1024))
	require.Equal(t, []byte("new-from-source"), readRegularFile(t, ctx, newTActor, "new.txt", 1024))

	_, staleErr := newTActor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte("stale.txt"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch0,
	})
	require.ErrorIs(t, staleErr, store.ErrStaleEpoch)

	// When 4: 创建一个未发生任何修改的 cleanBranch，合并进 target 应触发 SkipSourceCommit（仅新增 1 个合并提交而非 2 个）
	cleanBranch, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "clean"})
	require.NoError(t, err)
	commitsPreClean, err := memDB.ListLineageCommits(ctx, lineageID)
	require.NoError(t, err)

	cleanRes, err := mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: cleanBranch.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
	require.NotEmpty(t, cleanRes.MergeCommitID)

	commitsPostClean, err := memDB.ListLineageCommits(ctx, lineageID)
	require.NoError(t, err)
	require.Len(t, commitsPostClean, len(commitsPreClean)+1, "源盘无变更时 SkipSourceCommit 应仅追加 1 个合并提交")
}

func TestMergeDisk_Phase2CASConcurrentWrite_AbortsAndCleansStaging(t *testing.T) {
	// Given: Phase 1 结束后、Phase 2 CAS 前在目标盘产生一次并发写入
	ctx := context.Background()
	rootDir := t.TempDir()
	mgr, ok := New(db.NewMemStore(), WithStorageRoot(rootDir)).(*manager)
	require.True(t, ok)
	t.Cleanup(func() { _ = mgr.Close() })

	target, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, tActor, 0, "base.txt", []byte("base"))

	source, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, source.DiskID)
	require.NoError(t, err)
	writeRegularFile(t, ctx, sActor, 0, "from-source.txt", []byte("source"))

	mgr.beforePhase2CutoverHook = func() {
		// 模拟客户端在 Phase 1 期间向仍在在线的目标盘写入并发变更
		writeRegularFile(t, ctx, tActor, 0, "concurrent.txt", []byte("live-write"))
	}

	// When: 执行 MergeDisk
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: source.DiskID,
		TargetDiskID: target.DiskID,
		Strategy:     store.MergeStrategyFail,
	})

	// Then: CAS 检测到 nextWriteSeq1 != nextWriteSeq0，返回 ErrConcurrentMerge 并清理 Staging 与临时目录
	require.ErrorIs(t, err, ErrConcurrentMerge)

	entries, readErr := os.ReadDir(filepath.Join(rootDir, "disks"))
	require.NoError(t, readErr)
	for _, e := range entries {
		require.NotContains(t, e.Name(), ".merge.", "CAS 中止后不应残留 .merge 临时目录: %s", e.Name())
		require.NotContains(t, e.Name(), ".old.", "CAS 中止后不应残留 .old 目录: %s", e.Name())
	}

	// 验证并发写入在重新加载 targetActor 后完好保留且未被源盘覆盖
	reloaded, err := mgr.Actor(ctx, target.DiskID)
	require.NoError(t, err)
	require.Equal(t, []byte("live-write"), readRegularFile(t, ctx, reloaded, "concurrent.txt", 1024))
}

func TestOpenDiskActor_ReconcilePendingMerge_RollForwardAndRollback(t *testing.T) {
	ctx := context.Background()

	t.Run("PG已提交C_merge时崩溃前滚", func(t *testing.T) {
		// Given: 模拟 PG 已推进 base_commit_id=C_merge，但目录仍处于 target + target.merge.<C_merge> 状态
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "old.txt", []byte("old-data"))

		source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
		require.NoError(t, err)
		sActor, err := mgr1.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, sActor, 0, "merged.txt", []byte("new-merged-data"))

		// 准备真实 merge staging 并直接提交 DB 事务，模拟 PG 提交后、目录切换前崩溃
		targetID := uuid.MustParse(target.DiskID)
		sourceID := uuid.MustParse(source.DiskID)
		tRow, err := memDB.GetDisk(ctx, targetID)
		require.NoError(t, err)
		sRow, err := memDB.GetDisk(ctx, sourceID)
		require.NoError(t, err)

		cSrc := uuid.Must(uuid.NewV7())
		cMerge := uuid.Must(uuid.NewV7())
		snapSrc := filepath.Join(rootDir, "tmp-src")
		snapTgt := filepath.Join(rootDir, "tmp-tgt")
		_, err = sActor.Checkpoint(ctx, snapSrc)
		require.NoError(t, err)
		_, err = tActor.Checkpoint(ctx, snapTgt)
		require.NoError(t, err)

		stagingDir := mgr1.stagingMergeDir(target.DiskID, cMerge)
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            mgr1.commitDir(tRow.BaseCommitID.String()),
			TargetRoot:          snapTgt,
			SourceRoot:          snapSrc,
			StagingDir:          stagingDir,
			Strategy:            store.MergeStrategyFail,
			ExpectedWriterEpoch: tActor.WriterEpoch(),
		})
		require.NoError(t, err)
		require.NoError(t, store.CloneCheckpointDir(ctx, snapSrc, mgr1.commitDir(cSrc.String())))
		require.NoError(t, store.CloneCheckpointDir(ctx, stagingDir, mgr1.commitDir(cMerge.String())))

		_, err = memDB.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                  tRow.LineageID,
			SourceDiskID:               sourceID,
			TargetDiskID:               targetID,
			ExpectedSourceBaseCommitID: sRow.BaseCommitID,
			ExpectedTargetBaseCommitID: tRow.BaseCommitID,
			NewSourceCommitID:          cSrc,
			NewMergeCommitID:           cMerge,
		})
		require.NoError(t, err)
		require.NoError(t, mgr1.Close())
		_ = os.RemoveAll(snapSrc)
		_ = os.RemoveAll(snapTgt)

		// When: 新控制器实例重启打开目标盘
		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })

		reopened, err := mgr2.Actor(ctx, target.DiskID)

		// Then: 自动完成前滚，清理 .merge 目录并加载 merged.txt
		require.NoError(t, err)
		require.Equal(t, []byte("new-merged-data"), readRegularFile(t, ctx, reopened, "merged.txt", 1024))
		_, statErr := os.Stat(stagingDir)
		require.ErrorIs(t, statErr, os.ErrNotExist)
	})

	t.Run("PG未提交C_merge时崩溃回滚清理", func(t *testing.T) {
		// Given: 残留未提交的 .merge.<orphanID> 与 .old.<orphanID>
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "keep.txt", []byte("keep-original"))
		require.NoError(t, mgr1.Close())

		uncommittedID := uuid.Must(uuid.NewV7())
		staleStaging := mgr1.stagingMergeDir(target.DiskID, uncommittedID)
		staleOld := mgr1.oldMergeDir(target.DiskID, uncommittedID)
		require.NoError(t, os.MkdirAll(staleStaging, 0o755))
		require.NoError(t, os.MkdirAll(staleOld, 0o755))

		// When: 重启打开目标盘
		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })

		reopened, err := mgr2.Actor(ctx, target.DiskID)

		// Then: 自动回滚清理残留临时目录并保持原盘数据完好
		require.NoError(t, err)
		require.Equal(t, []byte("keep-original"), readRegularFile(t, ctx, reopened, "keep.txt", 1024))
		_, errStaging := os.Stat(staleStaging)
		require.ErrorIs(t, errStaging, os.ErrNotExist)
		_, errOld := os.Stat(staleOld)
		require.ErrorIs(t, errOld, os.ErrNotExist)
	})
}

func TestReadCommitNode_ConcurrentWithDeleteDisk_NoRace(t *testing.T) {
	// Given: 存在共享切面，并发执行 ReadCommitNode 与 DeleteDisk（验证 lineageCommitMu 读写锁保护）
	ctx := context.Background()
	mgr := New(db.NewMemStore(), WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	mainDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 16 << 20})
	require.NoError(t, err)
	mainActor, err := mgr.Actor(ctx, mainDisk.DiskID)
	require.NoError(t, err)
	ino := writeRegularFile(t, ctx, mainActor, 0, "data.bin", []byte("concurrent-safe-data"))

	childDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: mainDisk.DiskID, Name: "child"})
	require.NoError(t, err)
	diffRes, err := mgr.DiffDisk(ctx, &DiffDiskParams{SourceDiskID: childDisk.DiskID})
	require.NoError(t, err)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for range 10 {
			_, _ = mgr.ReadCommitNode(ctx, &ReadCommitNodeParams{
				DiskID:   childDisk.DiskID,
				CommitID: diffRes.LCACommitID,
				Ino:      ino,
				Offset:   0,
				Size:     64,
			})
			time.Sleep(time.Millisecond)
		}
	}()
	go func() {
		defer wg.Done()
		time.Sleep(2 * time.Millisecond)
		_ = mgr.DeleteDisk(ctx, mainDisk.DiskID)
	}()
	wg.Wait()
}

func TestMergeDisk_AllocatesLineageSegmentsOnFormalMergeAndSkipsOnDryRun(t *testing.T) {
	// Given: Base 存在硬链接组 (a.txt, b.txt -> inoA)，Target 与 Source 同时修改后按不同策略拆解硬链接组触发重分配 Inode
	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	mainDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "main", MaxBytes: 32 << 20})
	require.NoError(t, err)
	tActor, err := mgr.Actor(ctx, mainDisk.DiskID)
	require.NoError(t, err)
	inoA := writeRegularFile(t, ctx, tActor, 0, "a.txt", []byte("base-content"))
	_, err = tActor.Link(ctx, store.LinkReq{
		Ino:          inoA,
		NewParentIno: RootIno,
		NewName:      []byte("b.txt"),
	})
	require.NoError(t, err)

	sourceDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: mainDisk.DiskID, Name: "source"})
	require.NoError(t, err)
	sActor, err := mgr.Actor(ctx, sourceDisk.DiskID)
	require.NoError(t, err)

	overwriteRegularFile(t, ctx, tActor, 0, "a.txt", []byte("target-content"))
	overwriteRegularFile(t, ctx, sActor, 0, "a.txt", []byte("source-content"))

	tRec, err := memDB.GetDisk(ctx, uuid.MustParse(mainDisk.DiskID))
	require.NoError(t, err)
	lineageID := tRec.LineageID

	pathStrategies := map[string]store.MergeStrategy{
		"/a.txt": store.MergeStrategyTheirs,
		"/b.txt": store.MergeStrategyOurs,
	}

	// When 1: DryRun 预演合并不得调用 DB ReserveSegment
	snapBeforeDry := memDB.Inspect()
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID:   sourceDisk.DiskID,
		TargetDiskID:   mainDisk.DiskID,
		Strategy:       store.MergeStrategyFail,
		PathStrategies: pathStrategies,
		DryRun:         true,
	})
	require.NoError(t, err)
	snapAfterDry := memDB.Inspect()
	require.Equal(t, snapBeforeDry.InodeReserveCalls, snapAfterDry.InodeReserveCalls, "DryRun 不得推进 Inode 预留调用次数")
	require.Equal(t, snapBeforeDry.Lineages[lineageID].NextIno, snapAfterDry.Lineages[lineageID].NextIno, "DryRun 不得推进 DB NextIno 水位")

	// When 2: 正式合并触发硬链接分裂重分配 Inode，必须向 DB 申请新号段并推进 NextIno 水位
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID:   sourceDisk.DiskID,
		TargetDiskID:   mainDisk.DiskID,
		Strategy:       store.MergeStrategyFail,
		PathStrategies: pathStrategies,
	})
	require.NoError(t, err)
	snapAfterMerge := memDB.Inspect()
	require.Greater(t, snapAfterMerge.InodeReserveCalls, snapAfterDry.InodeReserveCalls, "正式合并重分配 Inode 必须调 ReserveSegment")
	require.Greater(t, snapAfterMerge.Lineages[lineageID].NextIno, snapAfterDry.Lineages[lineageID].NextIno, "正式合并必须推进 DB 血缘 NextIno 水位")
}

func TestMergeDisk_SanitizedOrphanBaseCommit_WithSingleWriteStillCreatesSourceCommit(t *testing.T) {
	// Given: 源盘携带孤儿节点被 Fork（SanitizeCheckpoint 使 commitDir.NextWriteSeq 比源盘多 1），随后源盘恰好写入 1 次
	ctx := context.Background()
	memDB := db.NewMemStore()
	mgr := New(memDB, WithStorageRoot(t.TempDir()))
	t.Cleanup(func() { _ = mgr.Close() })

	srcDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "src-with-orphan", MaxBytes: 32 << 20})
	require.NoError(t, err)
	srcActor, err := mgr.Actor(ctx, srcDisk.DiskID)
	require.NoError(t, err)
	dataIno := writeRegularFile(t, ctx, srcActor, 0, "data.txt", []byte("v0"))
	writeRegularFile(t, ctx, srcActor, 0, "orphan.bin", []byte("orphan-payload"))
	require.NoError(t, srcActor.Unlink(ctx, RootIno, []byte("orphan.bin"), true))

	tgtDisk, err := mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: srcDisk.DiskID, Name: "tgt"})
	require.NoError(t, err)

	// 仅执行单次 Write（使 srcActor NextWriteSeq 与经 CleanOrphans +1 后的 BaseCommitID 恰好相等）
	_, _, err = srcActor.Write(ctx, dataIno, 0, []byte("v1"), 0)
	require.NoError(t, err)

	srcID := uuid.MustParse(srcDisk.DiskID)
	srcRecBefore, err := memDB.GetDisk(ctx, srcID)
	require.NoError(t, err)
	commitsBefore, err := memDB.ListLineageCommits(ctx, srcRecBefore.LineageID)
	require.NoError(t, err)

	// When: 首次 MergeDisk(src -> tgt)
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: srcDisk.DiskID,
		TargetDiskID: tgtDisk.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)

	// Then: 不得误判 skipSourceCommit，必须生成 C_src 与 C_merge 且推进 src.BaseCommitID
	srcRecAfter, err := memDB.GetDisk(ctx, srcID)
	require.NoError(t, err)
	require.NotEqual(t, srcRecBefore.BaseCommitID, srcRecAfter.BaseCommitID, "源盘发生真实写入后必须创建 C_src 并推进 BaseCommitID")
	commitsAfter, err := memDB.ListLineageCommits(ctx, srcRecBefore.LineageID)
	require.NoError(t, err)
	require.Len(t, commitsAfter, len(commitsBefore)+2, "应同时新增 C_src 与 C_merge")

	// 连续第二次修改后再次合并，不应出现虚假冲突
	_, _, err = srcActor.Write(ctx, dataIno, 0, []byte("v2"), 0)
	require.NoError(t, err)
	_, err = mgr.MergeDisk(ctx, &MergeDiskParams{
		SourceDiskID: srcDisk.DiskID,
		TargetDiskID: tgtDisk.DiskID,
		Strategy:     store.MergeStrategyFail,
	})
	require.NoError(t, err)
}

func TestCrashRecovery_DeleteSourceBeforeReopenTargetAndPreCommitSrcCleanup(t *testing.T) {
	ctx := context.Background()

	t.Run("PG提交C_merge并在Rename旧盘后崩溃_先删源盘清空BaseCommitID再开目标盘仍前滚保数据", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "old.txt", []byte("old-data"))

		source, err := mgr1.ForkDisk(ctx, ForkDiskParams{SourceDiskID: target.DiskID, Name: "source"})
		require.NoError(t, err)
		sActor, err := mgr1.Actor(ctx, source.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, sActor, 0, "merged.txt", []byte("must-survive"))

		targetID := uuid.MustParse(target.DiskID)
		sourceID := uuid.MustParse(source.DiskID)
		tRow, err := memDB.GetDisk(ctx, targetID)
		require.NoError(t, err)
		sRow, err := memDB.GetDisk(ctx, sourceID)
		require.NoError(t, err)

		cSrc := uuid.Must(uuid.NewV7())
		cMerge := uuid.Must(uuid.NewV7())
		snapSrc := filepath.Join(rootDir, "tmp-src")
		snapTgt := filepath.Join(rootDir, "tmp-tgt")
		_, err = sActor.Checkpoint(ctx, snapSrc)
		require.NoError(t, err)
		_, err = tActor.Checkpoint(ctx, snapTgt)
		require.NoError(t, err)

		stagingDir := mgr1.stagingMergeDir(target.DiskID, cMerge)
		oldDir := mgr1.oldMergeDir(target.DiskID, cMerge)
		_, err = store.MergeTrees(ctx, &store.MergeOptions{
			BaseRoot:            mgr1.commitDir(tRow.BaseCommitID.String()),
			TargetRoot:          snapTgt,
			SourceRoot:          snapSrc,
			StagingDir:          stagingDir,
			Strategy:            store.MergeStrategyFail,
			ExpectedWriterEpoch: tActor.WriterEpoch(),
		})
		require.NoError(t, err)
		require.NoError(t, store.CloneCheckpointDir(ctx, snapSrc, mgr1.commitDir(cSrc.String())))
		require.NoError(t, store.CloneCheckpointDir(ctx, stagingDir, mgr1.commitDir(cMerge.String())))

		_, err = memDB.MergeDisk(ctx, &db.MergeDiskInput{
			LineageID:                  tRow.LineageID,
			SourceDiskID:               sourceID,
			TargetDiskID:               targetID,
			ExpectedSourceBaseCommitID: sRow.BaseCommitID,
			ExpectedTargetBaseCommitID: tRow.BaseCommitID,
			NewSourceCommitID:          cSrc,
			NewMergeCommitID:           cMerge,
		})
		require.NoError(t, err)
		require.NoError(t, mgr1.Close())
		_ = os.RemoveAll(snapSrc)
		_ = os.RemoveAll(snapTgt)

		// 模拟已执行 os.Rename(tgtDir, oldDir)，正准备 Rename(stagingDir, tgtDir) 时进程崩溃
		require.NoError(t, os.Rename(mgr1.diskDir(target.DiskID), oldDir))

		// When: 重启后用户先删掉 source 盘，再打开 target 盘
		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })
		require.NoError(t, mgr2.DeleteDisk(ctx, source.DiskID))

		reopened, err := mgr2.Actor(ctx, target.DiskID)
		require.NoError(t, err)

		// Then: 已提交的 merged.txt 必须完好存在，不能误回滚为 old.txt
		require.Equal(t, []byte("must-survive"), readRegularFile(t, ctx, reopened, "merged.txt", 1024))
		_, errStg := os.Stat(stagingDir)
		require.ErrorIs(t, errStg, os.ErrNotExist)
		_, errOld := os.Stat(oldDir)
		require.ErrorIs(t, errOld, os.ErrNotExist)
	})

	t.Run("PG提交前物化C_src与C_merge后崩溃_回滚并同步清理未提交commits与标记", func(t *testing.T) {
		rootDir := t.TempDir()
		memDB := db.NewMemStore()
		mgr1, ok := New(memDB, WithStorageRoot(rootDir)).(*manager)
		require.True(t, ok)

		target, err := mgr1.CreateDisk(ctx, CreateDiskParams{Name: "target", MaxBytes: 32 << 20})
		require.NoError(t, err)
		tActor, err := mgr1.Actor(ctx, target.DiskID)
		require.NoError(t, err)
		writeRegularFile(t, ctx, tActor, 0, "keep.txt", []byte("keep-data"))
		require.NoError(t, mgr1.Close())

		cMerge := uuid.Must(uuid.NewV7())
		cSrc := uuid.Must(uuid.NewV7())
		stagingDir := mgr1.stagingMergeDir(target.DiskID, cMerge)
		mergeCommitDir := mgr1.commitDir(cMerge.String())
		srcCommitDir := mgr1.commitDir(cSrc.String())
		markerPath := mgr1.srcCommitMarkerPath(target.DiskID, cMerge, cSrc)

		require.NoError(t, os.MkdirAll(stagingDir, 0o755))
		require.NoError(t, os.MkdirAll(mergeCommitDir, 0o755))
		require.NoError(t, os.MkdirAll(srcCommitDir, 0o755))
		require.NoError(t, os.WriteFile(markerPath, nil, 0o644))

		// When: 重启打开目标盘触发回滚清理
		mgr2 := New(memDB, WithStorageRoot(rootDir))
		t.Cleanup(func() { _ = mgr2.Close() })
		reopened, err := mgr2.Actor(ctx, target.DiskID)
		require.NoError(t, err)

		// Then: 原盘数据完好且未提交 staging/commits/marker 全数清理
		require.Equal(t, []byte("keep-data"), readRegularFile(t, ctx, reopened, "keep.txt", 1024))
		for _, leftover := range []string{stagingDir, mergeCommitDir, srcCommitDir, markerPath} {
			_, statErr := os.Stat(leftover)
			require.ErrorIs(t, statErr, os.ErrNotExist, "回滚后不得残留未提交产物: %s", leftover)
		}
	})
}
