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
