package db_test

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

type storeAdapter struct {
	name     string
	newStore func(t *testing.T) (db.Store, context.Context)
}

func storeAdapters() []storeAdapter {
	return []storeAdapter{
		{
			name: "MemStore",
			newStore: func(t *testing.T) (db.Store, context.Context) {
				t.Helper()
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				t.Cleanup(cancel)
				return db.NewMemStore(), ctx
			},
		},
		{
			name: "SQLStore",
			newStore: func(t *testing.T) (db.Store, context.Context) {
				t.Helper()
				pool, ctx := setupMetaTestDB(t)
				return db.NewStore(pool), ctx
			},
		},
	}
}

func runStoreAdapters(t *testing.T, fn func(t *testing.T, s db.Store, ctx context.Context)) {
	t.Helper()
	for _, adapter := range storeAdapters() {
		t.Run(adapter.name, func(t *testing.T) {
			s, ctx := adapter.newStore(t)
			fn(t, s, ctx)
		})
	}
}

func runSubtests[T any](t *testing.T, cases []T, nameFn func(T) string, fn func(*testing.T, T)) {
	t.Helper()
	for _, tc := range cases {
		t.Run(nameFn(tc), func(t *testing.T) {
			fn(t, tc)
		})
	}
}

func mustNewUUIDv7(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err, "生成 UUIDv7 失败")
	return id
}

func createTestDisks(t *testing.T, ctx context.Context, s db.Store, names ...string) []db.DiskRecord {
	t.Helper()
	created := make([]db.DiskRecord, 0, len(names))
	for _, name := range names {
		rec, err := s.CreateDisk(ctx, mustNewUUIDv7(t), name)
		require.NoError(t, err, "创建磁盘 %s 失败", name)
		created = append(created, rec)
	}
	return created
}

func requireOrderedDesc(t *testing.T, disks []db.DiskRecord) {
	t.Helper()
	for i := 1; i < len(disks); i++ {
		prev, curr := disks[i-1], disks[i]
		if prev.CreatedAt.Equal(curr.CreatedAt) {
			require.Positive(t, bytes.Compare(prev.DiskID[:], curr.DiskID[:]),
				"同微秒时间戳应按 DiskID 降序: prev=%s curr=%s", prev.DiskID, curr.DiskID)
			continue
		}
		require.True(t, prev.CreatedAt.After(curr.CreatedAt),
			"磁盘列表应按 CreatedAt 降序: prev=%s curr=%s", prev.CreatedAt, curr.CreatedAt)
	}
}

func collectDiskIDs(disks []db.DiskRecord) []uuid.UUID {
	ids := make([]uuid.UUID, 0, len(disks))
	for _, d := range disks {
		ids = append(ids, d.DiskID)
	}
	return ids
}

func runConcurrentSegmentReserves(t *testing.T, ctx context.Context, s db.Store, kind db.SegmentKind, lineageID uuid.UUID, workers int, count int64) []db.SegmentRange {
	t.Helper()
	start := make(chan struct{})
	results := make([]db.SegmentRange, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := range workers {
		go func(idx int) {
			defer wg.Done()
			<-start
			results[idx], errs[idx] = s.ReserveSegment(ctx, kind, lineageID, 2, count)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "并发预留号段 worker=%d 失败", i)
	}
	return results
}

func requireDisjointSortedSegments(t *testing.T, segs []db.SegmentRange, wantCount int64) {
	t.Helper()
	ordered := slices.Clone(segs)
	slices.SortFunc(ordered, func(a, b db.SegmentRange) int {
		return int(a.First - b.First)
	})
	for i, seg := range ordered {
		require.Equal(t, wantCount, seg.Last-seg.First+1, "号段长度 want=%d, got=%d", wantCount, seg.Last-seg.First+1)
		if i > 0 {
			require.Greater(t, seg.First, ordered[i-1].Last, "并发预留号段发生重叠: prev=%+v curr=%+v", ordered[i-1], seg)
		}
	}
}

func runConcurrentForkAndDelete(t *testing.T, ctx context.Context, s db.Store, sourceID uuid.UUID) (forkErr, delErr error) {
	t.Helper()
	childID := mustNewUUIDv7(t)
	commitID := mustNewUUIDv7(t)
	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-start
		_, forkErr = s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: sourceID,
			ChildDiskID:  childID,
			CommitID:     commitID,
			Name:         "race-child",
		})
	}()
	go func() {
		defer wg.Done()
		<-start
		_, delErr = s.DeleteDisk(ctx, sourceID)
	}()
	close(start)
	wg.Wait()
	return forkErr, delErr
}

func runConcurrentForkDeleteRounds(t *testing.T, ctx context.Context, s db.Store, rounds int) {
	t.Helper()
	for i := range rounds {
		rootID := mustNewUUIDv7(t)
		_, err := s.CreateDisk(ctx, rootID, fmt.Sprintf("race-root-%d", i))
		require.NoError(t, err)

		forkErr, delErr := runConcurrentForkAndDelete(t, ctx, s, rootID)
		require.NoError(t, delErr, "DeleteDisk 在并发 ForkDisk 下不应发生死锁或意外失败 (round=%d)", i)
		require.Condition(t, func() bool {
			return forkErr == nil || errors.Is(forkErr, db.ErrNotFound)
		}, "并发 ForkDisk 只能成功或因源盘已删返回 ErrNotFound (round=%d), got=%v", i, forkErr)
	}
}

func TestStoreContract_GivenValidDisk_WhenCreatedUpdatedRead_ThenPersistsEndState(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		diskID := mustNewUUIDv7(t)

		created, err := s.CreateDisk(ctx, diskID, "alpha")
		require.NoError(t, err)
		require.Equal(t, diskID, created.DiskID, "DiskID want=%s, got=%s", diskID, created.DiskID)
		require.Equal(t, uuid.Nil, created.ParentDiskID, "根盘 ParentDiskID 应为 Nil UUID, got=%s", created.ParentDiskID)
		require.Equal(t, diskID, created.LineageID, "根盘 LineageID want=%s, got=%s", diskID, created.LineageID)
		require.Equal(t, "alpha", created.Name, "Name want=alpha, got=%s", created.Name)
		require.False(t, created.CreatedAt.IsZero(), "CreatedAt 不应为零值")
		require.Equal(t, created.CreatedAt.Truncate(time.Microsecond), created.CreatedAt, "CreatedAt 应为微秒精度")

		got, err := s.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, created, got, "GetDisk 返回应与 CreateDisk 一致")

		updated, err := s.UpdateDiskName(ctx, diskID, "alpha-renamed")
		require.NoError(t, err)
		require.Equal(t, "alpha-renamed", updated.Name, "更新后 Name want=alpha-renamed, got=%s", updated.Name)
		require.Equal(t, created.CreatedAt, updated.CreatedAt, "UpdateDiskName 不得篡改 CreatedAt")
		require.False(t, updated.UpdatedAt.Before(created.UpdatedAt), "UpdatedAt 不得早于创建时间")

		reloaded, err := s.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, updated, reloaded, "GetDisk 应观测到更新后的名称")

		_, dupErr := s.CreateDisk(ctx, diskID, "alpha-dup")
		require.Error(t, dupErr, "重复 DiskID 创建必须失败")
		afterDup, err := s.GetDisk(ctx, diskID)
		require.NoError(t, err)
		require.Equal(t, "alpha-renamed", afterDup.Name, "失败的重复创建不得覆盖已有记录")
	})
}

func TestStoreContract_GivenInvalidInputsOrMissingTargets_WhenCalled_ThenFailsFastWithSentinelErrors(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		validID := mustNewUUIDv7(t)

		_, err := s.CreateDisk(ctx, uuid.Nil, "bad")
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.GetDisk(ctx, uuid.Nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.UpdateDiskName(ctx, uuid.Nil, "bad")
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.DeleteDisk(ctx, uuid.Nil)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{SourceDiskID: validID, ChildDiskID: validID, CommitID: mustNewUUIDv7(t), Name: "self"})
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, uuid.Nil, 2, 10)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, validID, 2, 0)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, validID, 2, -5)
		require.ErrorIs(t, err, db.ErrInvalidArgument)
		_, err = s.ReserveSegment(ctx, db.SegmentKind(99), validID, 2, 10)
		require.ErrorIs(t, err, db.ErrInvalidArgument)

		missingID := mustNewUUIDv7(t)
		_, err = s.GetDisk(ctx, missingID)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.UpdateDiskName(ctx, missingID, "missing")
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.DeleteDisk(ctx, missingID)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ForkDisk(ctx, db.ForkDiskInput{SourceDiskID: missingID, ChildDiskID: mustNewUUIDv7(t), CommitID: mustNewUUIDv7(t), Name: "child"})
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, missingID, 2, 10)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ReserveSegment(ctx, db.SegmentKindPack, missingID, 1, 10)
		require.ErrorIs(t, err, db.ErrNotFound)
	})
}

func TestStoreContract_GivenMalformedPageTokens_WhenListDisksCalled_ThenReturnsErrInvalidArgument(t *testing.T) {
	type malformedTokenCase struct {
		name  string
		token string
	}
	badTokens := []malformedTokenCase{
		{name: "invalid_base64", token: "not-valid-base64!!!"},
		{name: "empty_json_object", token: base64.RawURLEncoding.EncodeToString([]byte(`{}`))},
		{name: "unknown_json_field", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"11111111-1111-7111-8111-111111111111","extra":1}`))},
		{name: "nil_uuid_in_token", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"00000000-0000-0000-0000-000000000000"}`))},
		{name: "invalid_uuid_in_token", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"not-a-uuid"}`))},
		{name: "trailing_json_bytes", token: base64.RawURLEncoding.EncodeToString([]byte(`{"t":1,"id":"11111111-1111-7111-8111-111111111111"}{}`))},
	}

	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		runSubtests(t, badTokens, func(tc malformedTokenCase) string { return tc.name }, func(t *testing.T, tc malformedTokenCase) {
			_, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 10, PageToken: tc.token})
			require.ErrorIs(t, err, db.ErrInvalidArgument, "畸形分页令牌 %s 应返回 ErrInvalidArgument", tc.name)
		})
	})
}

func TestStoreContract_GivenFiveDisks_WhenListedWithCursorPagination_ThenReturnsStableDescPagesWithoutGaps(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		emptyPage, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 0})
		require.NoError(t, err)
		require.Empty(t, emptyPage.Disks, "空库查询磁盘列表应为空")
		require.Empty(t, emptyPage.NextPageToken, "空库 NextPageToken 应为空")

		created := createTestDisks(t, ctx, s, "d0", "d1", "d2", "d3", "d4")

		fullPage, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 0})
		require.NoError(t, err)
		require.Len(t, fullPage.Disks, 5, "PageSize=0 应回退默认 50 并返回全部 5 块盘")
		require.Empty(t, fullPage.NextPageToken)
		requireOrderedDesc(t, fullPage.Disks)

		page1, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 2})
		require.NoError(t, err)
		require.Len(t, page1.Disks, 2, "第 1 页条数 want=2, got=%d", len(page1.Disks))
		require.NotEmpty(t, page1.NextPageToken, "第 1 页应返回下一页游标")

		page2, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 2, PageToken: page1.NextPageToken})
		require.NoError(t, err)
		require.Len(t, page2.Disks, 2, "第 2 页条数 want=2, got=%d", len(page2.Disks))
		require.NotEmpty(t, page2.NextPageToken, "第 2 页应返回下一页游标")

		page3, err := s.ListDisks(ctx, db.ListDisksQuery{PageSize: 2, PageToken: page2.NextPageToken})
		require.NoError(t, err)
		require.Len(t, page3.Disks, 1, "第 3 页条数 want=1, got=%d", len(page3.Disks))
		require.Empty(t, page3.NextPageToken, "末页 NextPageToken 应为空")

		allPaged := slices.Concat(page1.Disks, page2.Disks, page3.Disks)
		requireOrderedDesc(t, allPaged)
		require.ElementsMatch(t, collectDiskIDs(created), collectDiskIDs(allPaged), "分页汇总盘集合应与创建集合完全一致")
	})
}

func TestStoreContract_GivenForkedLineage_WhenDeletedInTriStateOrder_ThenReclaimsCommitsAndLineage(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "root")
		require.NoError(t, err)

		c1ID, cm1ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c1, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c1ID,
			CommitID:     cm1ID,
			Name:         "child-1",
		})
		require.NoError(t, err)
		require.Equal(t, rootID, c1.ParentDiskID, "子盘 ParentDiskID want=%s, got=%s", rootID, c1.ParentDiskID)
		require.Equal(t, root.LineageID, c1.LineageID, "子盘必须继承源盘 LineageID")

		c2ID, cm2ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c2, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c2ID,
			CommitID:     cm2ID,
			Name:         "child-2",
		})
		require.NoError(t, err)
		require.Equal(t, root.LineageID, c2.LineageID)

		// 重复 ChildDiskID 或重复 CommitID 必须失败且不污染状态。
		_, dupChildErr := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  c1ID,
			CommitID:     mustNewUUIDv7(t),
			Name:         "dup-child",
		})
		require.Error(t, dupChildErr, "重复 ChildDiskID ForkDisk 必须失败")
		_, dupCommitErr := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  mustNewUUIDv7(t),
			CommitID:     cm1ID,
			Name:         "dup-commit",
		})
		require.Error(t, dupCommitErr, "重复 CommitID ForkDisk 必须失败")

		// |D| == 2：删 root 后必须保留全部切面。
		reclaimed, err := s.DeleteDisk(ctx, rootID)
		require.NoError(t, err)
		require.Empty(t, reclaimed, "|D|==2 时不得提前回收任何切面")
		_, err = s.GetDisk(ctx, rootID)
		require.ErrorIs(t, err, db.ErrNotFound)

		// |D| == 1：删 child-2 后必须自动回收 cm1, cm2 并清理幸存盘基准切面指针。
		reclaimed, err = s.DeleteDisk(ctx, c2ID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{cm1ID, cm2ID}, reclaimed, "|D|==1 时应返回全部待清理切面")

		// 唯一幸存盘 child-1 的 base_commit_id 被清空后，仍可继续作为源盘派生新子盘。
		c3ID, cm3ID := mustNewUUIDv7(t), mustNewUUIDv7(t)
		c3, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: c1ID,
			ChildDiskID:  c3ID,
			CommitID:     cm3ID,
			Name:         "child-3",
		})
		require.NoError(t, err, "幸存盘清空基准切面后再次 ForkDisk 必须成功")
		require.Equal(t, c1ID, c3.ParentDiskID)
		require.Equal(t, root.LineageID, c3.LineageID)

		// 删掉 child-3（|D| == 1）回收 cm3。
		reclaimed, err = s.DeleteDisk(ctx, c3ID)
		require.NoError(t, err)
		require.ElementsMatch(t, []uuid.UUID{cm3ID}, reclaimed)

		// |D| == 0：删掉最后幸存盘 child-1 后必须彻底删除血缘行。
		reclaimed, err = s.DeleteDisk(ctx, c1ID)
		require.NoError(t, err)
		require.Empty(t, reclaimed)
		_, err = s.GetDisk(ctx, c1ID)
		require.ErrorIs(t, err, db.ErrNotFound)
		_, err = s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 2, 10)
		require.ErrorIs(t, err, db.ErrNotFound, "|D|==0 清理血缘后 ReserveSegment 必须返回 ErrNotFound")
	})
}

func TestStoreContract_GivenSharedAndIsolatedLineages_WhenReserveSegmentCalled_ThenMonotonicAndDisjoint(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		rootID := mustNewUUIDv7(t)
		root, err := s.CreateDisk(ctx, rootID, "seg-root")
		require.NoError(t, err)

		otherID := mustNewUUIDv7(t)
		other, err := s.CreateDisk(ctx, otherID, "seg-other")
		require.NoError(t, err)

		r1, err := s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 1, 100)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 2, Last: 101}, r1, "首次 Inode 号段应从 2 起分配")

		r2, err := s.ReserveSegment(ctx, db.SegmentKindInode, root.LineageID, 500, 50)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 500, Last: 549}, r2, "minNext 超过水位线时应抬升起始号")

		p1, err := s.ReserveSegment(ctx, db.SegmentKindPack, root.LineageID, 0, 64)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 1, Last: 64}, p1, "首次 Pack 号段应从 1 起分配")

		p2, err := s.ReserveSegment(ctx, db.SegmentKindPack, root.LineageID, 1, 64)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 65, Last: 128}, p2)

		child, err := s.ForkDisk(ctx, db.ForkDiskInput{
			SourceDiskID: rootID,
			ChildDiskID:  mustNewUUIDv7(t),
			CommitID:     mustNewUUIDv7(t),
			Name:         "seg-child",
		})
		require.NoError(t, err)

		r3, err := s.ReserveSegment(ctx, db.SegmentKindInode, child.LineageID, 2, 10)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 550, Last: 559}, r3, "子盘应与源盘共享同一血缘号段单调递增")

		otherIno, err := s.ReserveSegment(ctx, db.SegmentKindInode, other.LineageID, 2, 5)
		require.NoError(t, err)
		require.Equal(t, db.SegmentRange{First: 2, Last: 6}, otherIno, "独立血缘号段应彼此隔离")

		concurrentSegs := runConcurrentSegmentReserves(t, ctx, s, db.SegmentKindInode, other.LineageID, 8, 10)
		requireDisjointSortedSegments(t, concurrentSegs, 10)
	})
}

func TestStoreContract_GivenConcurrentForkDelete_WhenExecuted_ThenConsistentWithoutDeadlock(t *testing.T) {
	runStoreAdapters(t, func(t *testing.T, s db.Store, ctx context.Context) {
		runConcurrentForkDeleteRounds(t, ctx, s, 5)
	})
}
