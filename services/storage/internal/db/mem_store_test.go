package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func mustNewUUIDv7(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err)
	return id
}

func TestMemStoreCRUDAndValidation(t *testing.T) {
	ctx := context.Background()
	var s Store = NewMemStore()

	// Nil UUID 与非法参数校验。
	_, err := s.CreateDisk(ctx, uuid.Nil, "bad")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.GetDisk(ctx, uuid.Nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.UpdateDiskName(ctx, uuid.Nil, "bad")
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.DeleteDisk(ctx, uuid.Nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.ForkDisk(ctx, ForkDiskInput{})
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.ReserveSegment(ctx, SegmentKindInode, uuid.Nil, 2, 10)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.ReserveSegment(ctx, SegmentKindInode, mustNewUUIDv7(t), 2, 0)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = s.ReserveSegment(ctx, SegmentKind(99), mustNewUUIDv7(t), 2, 10)
	require.ErrorIs(t, err, ErrInvalidArgument)

	missingID := mustNewUUIDv7(t)
	_, err = s.GetDisk(ctx, missingID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = s.UpdateDiskName(ctx, missingID, "nope")
	require.ErrorIs(t, err, ErrNotFound)
	_, err = s.DeleteDisk(ctx, missingID)
	require.ErrorIs(t, err, ErrNotFound)
	_, err = s.ReserveSegment(ctx, SegmentKindPack, missingID, 1, 10)
	require.ErrorIs(t, err, ErrNotFound)

	diskID := mustNewUUIDv7(t)
	created, err := s.CreateDisk(ctx, diskID, "alpha")
	require.NoError(t, err)
	require.Equal(t, diskID, created.DiskID)
	require.Equal(t, uuid.Nil, created.ParentDiskID)
	require.Equal(t, diskID, created.LineageID)
	require.Equal(t, "alpha", created.Name)
	require.Equal(t, created.CreatedAt.Truncate(time.Microsecond), created.CreatedAt)

	got, err := s.GetDisk(ctx, diskID)
	require.NoError(t, err)
	require.Equal(t, created, got)

	updated, err := s.UpdateDiskName(ctx, diskID, "alpha-renamed")
	require.NoError(t, err)
	require.Equal(t, "alpha-renamed", updated.Name)
	require.Equal(t, created.CreatedAt, updated.CreatedAt)
}

func TestMemStoreListDisksMicrosecondCursorPagination(t *testing.T) {
	ctx := context.Background()
	mem := NewMemStore()

	// 写入 5 块同微秒与跨微秒混合时间戳的磁盘，验证稳定倒序游标翻页不重不漏。
	const total = 5
	ids := make([]uuid.UUID, total)
	for i := range total {
		id := mustNewUUIDv7(t)
		ids[i] = id
		_, err := mem.CreateDisk(ctx, id, fmt.Sprintf("disk-%d", i))
		require.NoError(t, err)
	}

	// 强制前 3 块共享完全相同的微秒时间戳，覆盖同时间戳按 disk_id DESC 决胜边界。
	sharedTS := time.Now().UTC().Truncate(time.Microsecond)
	mem.mu.Lock()
	for i := range 3 {
		v := mem.disks[ids[i]]
		v.CreatedAt = sharedTS
		mem.disks[ids[i]] = v
	}
	mem.mu.Unlock()

	var collected []uuid.UUID
	var token string
	for {
		page, err := mem.ListDisks(ctx, ListDisksQuery{
			PageSize:  2,
			PageToken: token,
		})
		require.NoError(t, err)
		for _, d := range page.Disks {
			collected = append(collected, d.DiskID)
		}
		if page.NextPageToken == "" {
			break
		}
		token = page.NextPageToken
	}
	require.Len(t, collected, total)
	require.Len(t, map[uuid.UUID]struct{}{
		collected[0]: {},
		collected[1]: {},
		collected[2]: {},
		collected[3]: {},
		collected[4]: {},
	}, total)

	_, err := mem.ListDisks(ctx, ListDisksQuery{PageSize: 2, PageToken: "not-valid-base64!!!"})
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestMemStoreForkDeleteTriStateGCAndFaultInjection(t *testing.T) {
	ctx := context.Background()
	mem := NewMemStore()

	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "root")
	require.NoError(t, err)

	// 故障注入仅使下一次写失败且原子回滚，之后恢复正常。
	injected := errors.New("injected write error")
	mem.SetFailNextWrite(injected)
	cFailID := mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  cFailID,
		CommitID:     mustNewUUIDv7(t),
		Name:         "fail-child",
	})
	require.ErrorIs(t, err, injected)
	snapAfterFail := mem.Inspect()
	require.Len(t, snapAfterFail.Disks, 1)
	require.Empty(t, snapAfterFail.Commits)

	c1ID := mustNewUUIDv7(t)
	cm1ID := mustNewUUIDv7(t)
	c1, err := mem.ForkDisk(ctx, ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  c1ID,
		CommitID:     cm1ID,
		Name:         "child-1",
	})
	require.NoError(t, err)
	require.Equal(t, rootID, c1.ParentDiskID)
	require.Equal(t, rootID, c1.LineageID)

	c2ID := mustNewUUIDv7(t)
	cm2ID := mustNewUUIDv7(t)
	_, err = mem.ForkDisk(ctx, ForkDiskInput{
		SourceDiskID: rootID,
		ChildDiskID:  c2ID,
		CommitID:     cm2ID,
		Name:         "child-2",
	})
	require.NoError(t, err)

	// |D| == 2：删 root 后必须保留全部切面。
	reclaimed, err := mem.DeleteDisk(ctx, rootID)
	require.NoError(t, err)
	require.Empty(t, reclaimed)
	snap := mem.Inspect()
	require.Len(t, snap.Commits, 2)
	require.Len(t, snap.Lineages, 1)

	// |D| == 1：删 child-2 后必须自动清空 child-1 的 BaseCommitID 并返回两个待清理切面。
	reclaimed, err = mem.DeleteDisk(ctx, c2ID)
	require.NoError(t, err)
	require.ElementsMatch(t, []uuid.UUID{cm1ID, cm2ID}, reclaimed)
	snap = mem.Inspect()
	require.Empty(t, snap.Commits)
	require.Len(t, snap.Lineages, 1)
	require.Equal(t, uuid.Nil, snap.Disks[c1ID].BaseCommitID)

	// |D| == 0：删最后一个幸存盘后血缘彻底清空。
	reclaimed, err = mem.DeleteDisk(ctx, c1ID)
	require.NoError(t, err)
	require.Empty(t, reclaimed)
	snap = mem.Inspect()
	require.Empty(t, snap.Disks)
	require.Empty(t, snap.Lineages)
}

func TestMemStoreReserveSegmentMonotonic(t *testing.T) {
	ctx := context.Background()
	mem := NewMemStore()
	rootID := mustNewUUIDv7(t)
	_, err := mem.CreateDisk(ctx, rootID, "seg-root")
	require.NoError(t, err)

	r1, err := mem.ReserveSegment(ctx, SegmentKindInode, rootID, 2, 100)
	require.NoError(t, err)
	require.Equal(t, SegmentRange{First: 2, Last: 101}, r1)

	// minNext 跨越当前水位线时自动抬升。
	r2, err := mem.ReserveSegment(ctx, SegmentKindInode, rootID, 500, 50)
	require.NoError(t, err)
	require.Equal(t, SegmentRange{First: 500, Last: 549}, r2)

	p1, err := mem.ReserveSegment(ctx, SegmentKindPack, rootID, 1, 64)
	require.NoError(t, err)
	require.Equal(t, SegmentRange{First: 1, Last: 64}, p1)

	p2, err := mem.ReserveSegment(ctx, SegmentKindPack, rootID, 1, 64)
	require.NoError(t, err)
	require.Equal(t, SegmentRange{First: 65, Last: 128}, p2)

	snap := mem.Inspect()
	require.Equal(t, 2, snap.InodeReserveCalls)
	require.Equal(t, 2, snap.PackReserveCalls)
}
