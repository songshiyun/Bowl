package meta_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestTreeQuotaDeduplicatesInodesAcrossBranches(t *testing.T) {
	quota := meta.NewTreeQuota(150)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0, 2: 100}))
	require.NoError(t, quota.ReplaceBranch("feature", map[uint64]uint64{1: 0, 2: 100}))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 100, Inodes: 2}, quota.Usage())

	require.NoError(t, quota.Apply("feature", 2, 100, true, nil))
	require.NoError(t, quota.Apply("feature", 2, 150, true, nil))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 150, Inodes: 2}, quota.Usage())
	require.ErrorIs(t, quota.Apply("main", 3, 1, true, nil), meta.ErrNoSpace)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 150, Inodes: 2}, quota.Usage())
}

func TestTreeQuotaDoesNotCommitFailedPersistence(t *testing.T) {
	quota := meta.NewTreeQuota(100)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0, 2: 80}))
	failure := errors.New("写入失败")
	require.ErrorIs(t, quota.Apply("main", 2, 100, true, func() error { return failure }), failure)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 80, Inodes: 2}, quota.Usage())
	require.NoError(t, quota.Apply("main", 3, 20, true, nil))
	require.ErrorIs(t, quota.SetMaxBytes(99), meta.ErrInvalidShrink)
	require.NoError(t, quota.SetMaxBytes(100))
}

func TestTreeQuotaConcurrentReservations(t *testing.T) {
	quota := meta.NewTreeQuota(10)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0}))
	errs := make(chan error, 2)
	go func() { errs <- quota.Apply("main", 2, 10, true, nil) }()
	go func() { errs <- quota.Apply("main", 3, 10, true, nil) }()
	first, second := <-errs, <-errs
	require.True(t, first == nil && errors.Is(second, meta.ErrNoSpace) ||
		second == nil && errors.Is(first, meta.ErrNoSpace), "并发预留必须恰好一个成功、一个返回 ErrNoSpace")
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 10, Inodes: 2}, quota.Usage())
}

func TestTreeQuotaReleasesOnlyUnsharedBytes(t *testing.T) {
	quota := meta.NewTreeQuota(200)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0, 2: 100}))
	require.NoError(t, quota.ReplaceBranch("feature", map[uint64]uint64{1: 0, 2: 150, 3: 30}))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 180, Inodes: 3}, quota.Usage())
	require.NoError(t, quota.ReplaceBranch("feature", nil))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 100, Inodes: 2}, quota.Usage())
	require.NoError(t, quota.Apply("main", 2, 40, true, nil))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 40, Inodes: 2}, quota.Usage())
	require.NoError(t, quota.Apply("main", 2, 0, false, nil))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 0, Inodes: 1}, quota.Usage())
}

func TestTreeQuotaApplyChangeAndCommitWithQuota(t *testing.T) {
	quota := meta.NewTreeQuota(100)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0, 2: 100}))

	// 磁盘已满时，Create(size=0) 与 Link(enforce=true) 均被拒绝且不触发回调
	hookCalled := false
	require.ErrorIs(t, quota.Create("main", 3, 0, nil, func() error {
		hookCalled = true
		return nil
	}), meta.ErrNoSpace)
	require.False(t, hookCalled)

	require.ErrorIs(t, quota.CommitWithQuota(true, func() error {
		hookCalled = true
		return nil
	}), meta.ErrNoSpace)
	require.False(t, hookCalled)

	// 磁盘已满时，CommitWithQuota(enforce=false) 允许通过（如 Unlink 孤儿标记或 Rename 拓扑变更）
	require.NoError(t, quota.CommitWithQuota(false, func() error {
		hookCalled = true
		return nil
	}))
	require.True(t, hookCalled)

	// 磁盘超额时（例如缩容前或多分支场景下），ApplyChange(enforce=false) 允许截断或回收且按序执行 beforeCommit -> persist
	quota.InitMaxBytes(50)
	var steps []string
	require.NoError(t, quota.ApplyChange("main", 2, 80, true, false, func() error {
		steps = append(steps, "persist-truncate")
		return nil
	}))
	require.Equal(t, []string{"persist-truncate"}, steps)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 80, Inodes: 2}, quota.Usage())

	// 配额超限时，ApplyChange(enforce=true) 拒绝扩容且不触发 persist
	require.ErrorIs(t, quota.ApplyChange("main", 2, 90, true, true, func() error {
		steps = append(steps, "should-not-run")
		return nil
	}), meta.ErrNoSpace)
	require.Equal(t, []string{"persist-truncate"}, steps)

	// Reclaim 释放节点不受配额上限限制
	require.NoError(t, quota.Reclaim("main", 2, func() error {
		steps = append(steps, "persist-reclaim")
		return nil
	}))
	require.Equal(t, []string{"persist-truncate", "persist-reclaim"}, steps)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 0, Inodes: 1}, quota.Usage())

	// 空间充足时，Create 在锁内先执行 prepare 再执行 persist
	var createdIno uint64
	require.NoError(t, quota.Create("main", 4, 30, func() error {
		steps = append(steps, "prepare-create")
		createdIno = 4
		return nil
	}, func() error {
		steps = append(steps, "persist-create")
		return nil
	}))
	require.Equal(t, uint64(4), createdIno)
	require.Equal(t, []string{"persist-truncate", "persist-reclaim", "prepare-create", "persist-create"}, steps)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 30, Inodes: 2}, quota.Usage())
}

func TestTreeQuotaStageBranchReplaceAndPatchRollbackConservation(t *testing.T) {
	quota := meta.NewTreeQuota(250)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0, 2: 100, 3: 50}))
	require.NoError(t, quota.ReplaceBranch("feature", map[uint64]uint64{1: 0, 2: 120, 4: 40}))
	initialUsage := meta.TreeQuotaUsage{Bytes: 210, Inodes: 4}
	require.Equal(t, initialUsage, quota.Usage())

	// 1. StageBranchReplace 超额失败时不留锁、不破坏现有分支状态
	stage, err := quota.StageBranchReplace("fork", map[uint64]uint64{1: 0, 5: 50}, true)
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.Nil(t, stage)
	require.Equal(t, initialUsage, quota.Usage())

	// 2. StageBranchReplace 新分支暂存成功 -> Rollback 恢复无该分支的原始状态
	stage, err = quota.StageBranchReplace("fork", map[uint64]uint64{1: 0, 5: 30}, true)
	require.NoError(t, err)
	require.NoError(t, stage.Rollback())
	require.Equal(t, initialUsage, quota.Usage())

	// 3. StageBranchReplace 删除分支暂存 -> Rollback 恢复被删分支配额
	stage, err = quota.StageBranchReplace("feature", nil, false)
	require.NoError(t, err)
	require.NoError(t, stage.Rollback())
	require.Equal(t, initialUsage, quota.Usage())

	// 4. StageBranchReplace 替换现有分支 -> Commit 使变更生效且不可再被 Rollback 撤销
	stage, err = quota.StageBranchReplace("feature", map[uint64]uint64{1: 0, 2: 80, 4: 40}, false)
	require.NoError(t, err)
	stage.Commit()
	require.NoError(t, stage.Rollback())
	afterRollbackFeature := meta.TreeQuotaUsage{Bytes: 190, Inodes: 4}
	require.Equal(t, afterRollbackFeature, quota.Usage())

	// 5. StageBranchPatch 增量合并补丁：
	// dirty 标记 2, 3, 5 为合并脏节点；changed 更新 2->130、新增 5->20（3 不在 changed 中即在合并中被删除，1 未脏保留）
	dirty := map[uint64]bool{2: true, 3: true, 5: true}
	changed := map[uint64]uint64{2: 130, 5: 20}

	// enforce=true 时若超额（130 + 40 + 20 = 190 <= 250，若新增旁路节点 6->70 则总计 260 > 250），立即拒绝且不破坏状态
	_, err = quota.StageBranchPatch("main", dirty, map[uint64]uint64{2: 130, 5: 20, 6: 70}, true)
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.Equal(t, afterRollbackFeature, quota.Usage())

	// 暂存增量补丁后 Rollback 恢复 main 原有状态（含节点 3）
	stage, err = quota.StageBranchPatch("main", dirty, changed, true)
	require.NoError(t, err)
	require.NoError(t, stage.Rollback())
	require.Equal(t, afterRollbackFeature, quota.Usage())

	// 暂存增量补丁后 Commit 正式生效：main 拥有 {1:0, 2:130, 5:20}，feature 拥有 {1:0, 2:80, 4:40}
	stage, err = quota.StageBranchPatch("main", dirty, changed, false)
	require.NoError(t, err)
	stage.Commit()
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 190, Inodes: 4}, quota.Usage())
}

func TestTreeQuotaDiskLifecycleAndShrinkCheck(t *testing.T) {
	quota := meta.NewTreeQuota(100)
	require.NoError(t, quota.ReplaceBranch("main", map[uint64]uint64{1: 0, 2: 60}))

	// 1. WithShrinkCheck 低于当前用量时返回 ErrInvalidShrink，且上限保持不变
	require.ErrorIs(t, quota.WithShrinkCheck(func() (uint64, error) {
		return 59, nil
	}), meta.ErrInvalidShrink)
	maxBytes, usage := quota.Snapshot()
	require.Equal(t, uint64(100), maxBytes)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 60, Inodes: 2}, usage)

	// 2. WithShrinkCheck 成功时原子更新上限
	require.NoError(t, quota.WithShrinkCheck(func() (uint64, error) {
		return 80, nil
	}))
	maxBytes, _ = quota.Snapshot()
	require.Equal(t, uint64(80), maxBytes)

	// 3. BeginClose 后拒绝一切配额操作及重复 BeginClose；rollback 恢复可用
	rollback, err := quota.BeginClose()
	require.NoError(t, err)
	_, err = quota.BeginClose()
	require.ErrorIs(t, err, meta.ErrNotFound)
	require.ErrorIs(t, quota.Apply("main", 3, 1, true, nil), meta.ErrNotFound)
	require.ErrorIs(t, quota.CommitWithQuota(false, nil), meta.ErrNotFound)
	require.ErrorIs(t, quota.WithShrinkCheck(func() (uint64, error) { return 80, nil }), meta.ErrNotFound)

	rollback()
	require.NoError(t, quota.Apply("main", 3, 10, true, nil))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 70, Inodes: 3}, quota.Usage())
}
