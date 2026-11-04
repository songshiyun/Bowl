package meta_test

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func TestBranchSessionWriterLease(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_session")
	mgr := newTestManager(t, db.NewStore(pool), t.TempDir())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "lease", MaxBytes: 1024})
	require.NoError(t, err)
	params := meta.RenewBranchSessionParams{SessionID: 101, DiskID: disk.DiskID}
	first, err := mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)
	require.True(t, first.SessionRecreated)
	second, err := mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)
	require.False(t, second.SessionRecreated)
	params.SessionID = 102
	_, err = mgr.RenewBranchSession(ctx, params)
	require.ErrorIs(t, err, meta.ErrBusy)
	require.NoError(t, mgr.CloseSession(ctx, 101))
	third, err := mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)
	require.True(t, third.SessionRecreated)
}

func TestBranchSessionOrphanSweepOnClose(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_orphan")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "orphan", MaxBytes: 1024})
	require.NoError(t, err)
	params := meta.RenewBranchSessionParams{SessionID: 201, DiskID: disk.DiskID}
	_, err = mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG | 0o644, Size: 100})
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
	require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.XattrKey(2, []byte("user.test")), []byte("value"), nil))
	require.NoError(t, batch.Set(pebblekv.DataKey(2, 0), []byte("data"), nil))
	require.NoError(t, batch.Set(pebblekv.DirtyKey(2), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 100, Inodes: 2, NextWriteSeq: 4}), nil))
	require.NoError(t, quota.Apply("main", 2, 100, true, func() error { return batch.Commit(pebble.Sync) }))
	require.NoError(t, batch.Close())
	inode, err = pebblekv.EncodeInode(pebblekv.Inode{Ino: 3, Mode: meta.S_IFREG | 0o644, Size: 30})
	require.NoError(t, err)
	batch = branch.DB().NewBatch()
	require.NoError(t, batch.Set(pebblekv.InodeKey(3), inode, nil))
	require.NoError(t, batch.Set(pebblekv.OrphanKey(3), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.XattrKey(3, []byte("user.other")), []byte("value"), nil))
	require.NoError(t, batch.Set(pebblekv.DataKey(3, 2), []byte("data"), nil))
	require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 130, Inodes: 3, NextWriteSeq: 4}), nil))
	require.NoError(t, quota.Apply("main", 3, 30, true, func() error { return batch.Commit(pebble.Sync) }))
	require.NoError(t, batch.Close())
	require.NoError(t, branch.Close())
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 130, Inodes: 3}, quota.Usage())
	err = mgr.CloseSession(ctx, 9999)
	require.NoError(t, err)
	branch, err = storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	_, closer, err := branch.DB().Get(pebblekv.OrphanKey(2))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.NoError(t, branch.Close())
	require.NoError(t, mgr.CloseSession(ctx, params.SessionID))
	branch, err = storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	for _, key := range [][]byte{
		pebblekv.InodeKey(2), pebblekv.OrphanKey(2), pebblekv.XattrKey(2, []byte("user.test")), pebblekv.DataKey(2, 0),
		pebblekv.InodeKey(3), pebblekv.OrphanKey(3), pebblekv.XattrKey(3, []byte("user.other")), pebblekv.DataKey(3, 2),
	} {
		_, _, err = branch.DB().Get(key)
		require.ErrorIs(t, err, pebble.ErrNotFound)
	}
	_, closer, err = branch.DB().Get(pebblekv.DirtyKey(2))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	value, closer, err := branch.DB().Get(pebblekv.StatKey())
	require.NoError(t, err)
	stat, err := pebblekv.DecodeBranchStat(value)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Equal(t, pebblekv.BranchStat{Inodes: 1, NextWriteSeq: 4}, stat)
	require.NoError(t, branch.Close())
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
}

func TestBranchSessionExpiredSweep(t *testing.T) {
	for _, action := range []string{"其他会话关闭", "写端接管"} {
		t.Run(action, func(t *testing.T) {
			pool, ctx := setupIsolatedMetaTestDB(t, "branch_expired")
			root := t.TempDir()
			storage := pebblekv.NewManager(root)
			t.Cleanup(func() { _ = storage.Close() })
			mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
			disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "expired", MaxBytes: 1024})
			require.NoError(t, err)
			_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: 301, DiskID: disk.DiskID})
			require.NoError(t, err)
			quota, err := mgr.TreeQuota(ctx, disk.DiskID)
			require.NoError(t, err)
			branch, err := storage.OpenBranch(disk.DiskID, "main")
			require.NoError(t, err)
			inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG | 0o644, Size: 80})
			require.NoError(t, err)
			batch := branch.DB().NewBatch()
			require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
			require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
			require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 80, Inodes: 2, NextWriteSeq: 1}), nil))
			require.NoError(t, quota.Apply("main", 2, 80, true, func() error { return batch.Commit(pebble.Sync) }))
			require.NoError(t, batch.Close())
			require.NoError(t, branch.Close())
			_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, 301)
			require.NoError(t, err)
			switch action {
			case "其他会话关闭":
				err = mgr.CloseSession(ctx, 302)
			case "写端接管":
				_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: 302, DiskID: disk.DiskID})
			}
			require.NoError(t, err)
			branch, err = storage.OpenBranch(disk.DiskID, "main")
			require.NoError(t, err)
			_, _, err = branch.DB().Get(pebblekv.OrphanKey(2))
			require.ErrorIs(t, err, pebble.ErrNotFound)
			require.NoError(t, branch.Close())
			require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
		})
	}
}

func TestBranchSessionsIndependentWritersAndReaders(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_readers")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "readers", MaxBytes: 1024})
	require.NoError(t, err)
	commit := *disk.Branches[0].HeadCommitID
	require.NoError(t, storage.Fork(disk.DiskID, commit, "feature"))
	_, err = pool.Exec(ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', $2::text, 'head_commit_id', $2::text, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, disk.DiskID, commit)
	require.NoError(t, err)
	feature := "feature"
	mainWriter := meta.RenewBranchSessionParams{SessionID: 401, DiskID: disk.DiskID}
	featureWriter := meta.RenewBranchSessionParams{SessionID: 402, DiskID: disk.DiskID, BranchName: &feature}
	_, err = mgr.RenewBranchSession(ctx, mainWriter)
	require.NoError(t, err)
	_, err = mgr.RenewBranchSession(ctx, featureWriter)
	require.NoError(t, err)
	_, err = mgr.RenewBranchSession(ctx, mainWriter)
	require.NoError(t, err)
	_, err = mgr.RenewBranchSession(ctx, featureWriter)
	require.NoError(t, err)
	reader := meta.RenewBranchSessionParams{SessionID: 403, DiskID: disk.DiskID, ReadOnly: true}
	_, err = mgr.RenewBranchSession(ctx, reader)
	require.NoError(t, err)
	reader.SessionID = 404
	reader.CommitID = &commit
	_, err = mgr.RenewBranchSession(ctx, reader)
	require.NoError(t, err)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE disk_id=$1 AND read_only AND commit_id=$2`, disk.DiskID, commit).Scan(&count))
	require.Equal(t, 2, count)
	reader.CommitID = nil
	reader.SessionID = mainWriter.SessionID
	_, err = mgr.RenewBranchSession(ctx, reader)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	mainWriter.SessionID = 405
	mainWriter.CommitID = &commit
	_, err = mgr.RenewBranchSession(ctx, mainWriter)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	missing := "missing"
	mainWriter.CommitID = nil
	mainWriter.BranchName = &missing
	_, err = mgr.RenewBranchSession(ctx, mainWriter)
	require.ErrorIs(t, err, meta.ErrNotFound)
	empty := ""
	mainWriter.BranchName = &empty
	_, err = mgr.RenewBranchSession(ctx, mainWriter)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	reader.SessionID = 406
	other, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "other", MaxBytes: 1024})
	require.NoError(t, err)
	reader.CommitID = other.Branches[0].HeadCommitID
	_, err = mgr.RenewBranchSession(ctx, reader)
	require.ErrorIs(t, err, meta.ErrNotFound)
}

func TestBranchSessionsConcurrentWriterTakeover(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_race")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "race", MaxBytes: 1024})
	require.NoError(t, err)
	const writers = 8
	start := make(chan struct{})
	results := make(chan error, writers)
	for i := 0; i < writers; i++ {
		go func(id uint64) {
			<-start
			_, err := mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: id, DiskID: disk.DiskID})
			results <- err
		}(uint64(500 + i))
	}
	close(start)
	var successes, busy int
	for i := 0; i < writers; i++ {
		err := <-results
		switch {
		case err == nil:
			successes++
		case errors.Is(err, meta.ErrBusy):
			busy++
		default:
			t.Fatalf("并发续期失败: %v", err)
		}
	}
	require.Equal(t, 1, successes)
	require.Equal(t, writers-1, busy)
}

func TestBranchSessionSweepFailureKeepsExpiredSessionForRetry(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_retry")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "retry", MaxBytes: 1024})
	require.NoError(t, err)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: 601, DiskID: disk.DiskID})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG, Size: 50})
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
	require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.StatKey(), []byte("invalid"), nil))
	require.NoError(t, quota.Apply("main", 2, 50, true, func() error { return batch.Commit(pebble.Sync) }))
	require.NoError(t, batch.Close())
	require.NoError(t, branch.Close())
	require.ErrorIs(t, mgr.CloseSession(ctx, 601), pebblekv.ErrInvalidValue)
	var active bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT expires_at > clock_timestamp() FROM client_sessions WHERE session_id = $1`, 601).Scan(&active))
	require.False(t, active)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: 602, DiskID: disk.DiskID})
	require.ErrorIs(t, err, pebblekv.ErrInvalidValue)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 50, Inodes: 2}, quota.Usage())
	branch, err = storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	_, closer, err := branch.DB().Get(pebblekv.OrphanKey(2))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.NoError(t, branch.DB().Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 50, Inodes: 2}), pebble.Sync))
	require.NoError(t, branch.Close())
	require.NoError(t, mgr.CloseSession(ctx, 601))
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
}

func TestBranchSessionSweepPreservesSharedInodeQuota(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_shared")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "shared", MaxBytes: 1024})
	require.NoError(t, err)
	require.NoError(t, storage.Fork(disk.DiskID, *disk.Branches[0].HeadCommitID, "feature"))
	_, err = pool.Exec(ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, disk.DiskID)
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	put := func(name string, size uint64, orphan bool) {
		t.Helper()
		branch, err := storage.OpenBranch(disk.DiskID, name)
		require.NoError(t, err)
		inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG, Size: size})
		require.NoError(t, err)
		batch := branch.DB().NewBatch()
		require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
		require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: size, Inodes: 2}), nil))
		if orphan {
			require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
		}
		require.NoError(t, quota.Apply(name, 2, size, true, func() error { return batch.Commit(pebble.Sync) }))
		require.NoError(t, batch.Close())
		require.NoError(t, branch.Close())
	}
	put("main", 100, false)
	put("feature", 150, true)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 150, Inodes: 2}, quota.Usage())
	feature := "feature"
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: 701, DiskID: disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 100, Inodes: 2}, quota.Usage())
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	_, closer, err := branch.DB().Get(pebblekv.InodeKey(2))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.NoError(t, branch.Close())
}

type pauseBranchRenewTracer struct {
	armed   atomic.Bool
	reached chan struct{}
	resume  chan struct{}
}

func (tracer *pauseBranchRenewTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "-- name: RenewControlPlaneSession") && tracer.armed.CompareAndSwap(true, false) {
		close(tracer.reached)
		select {
		case <-tracer.resume:
		case <-ctx.Done():
		}
	}
	return ctx
}

func (*pauseBranchRenewTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func TestBranchSessionExpiryDuringRenew(t *testing.T) {
	tracer := &pauseBranchRenewTracer{reached: make(chan struct{}), resume: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(tracer.resume)
		}
	}()
	pool, observed, ctx := setupIsolatedMetaTestDBWithTracer(t, "branch_expire_renew", tracer)
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(observed), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "expiry", MaxBytes: 1024})
	require.NoError(t, err)
	params := meta.RenewBranchSessionParams{SessionID: 801, DiskID: disk.DiskID}
	_, err = mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG, Size: 40})
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
	require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 40, Inodes: 2}), nil))
	require.NoError(t, quota.Apply("main", 2, 40, true, func() error { return batch.Commit(pebble.Sync) }))
	require.NoError(t, batch.Close())
	require.NoError(t, branch.Close())
	tracer.armed.Store(true)
	type outcome struct {
		result meta.RenewSessionResult
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		result, err := mgr.RenewBranchSession(ctx, params)
		completed <- outcome{result, err}
	}()
	select {
	case <-tracer.reached:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, params.SessionID)
	require.NoError(t, err)
	close(tracer.resume)
	released = true
	out := <-completed
	require.NoError(t, out.err)
	require.True(t, out.result.SessionRecreated)
	branch, err = storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	_, _, err = branch.DB().Get(pebblekv.OrphanKey(2))
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.NoError(t, branch.Close())
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
}

func TestBranchSessionExpiresInsideDatabaseRenew(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_expire_sql")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "expiry", MaxBytes: 1024})
	require.NoError(t, err)
	params := meta.RenewBranchSessionParams{SessionID: 901, DiskID: disk.DiskID}
	_, err = mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG, Size: 25})
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
	require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 25, Inodes: 2}), nil))
	require.NoError(t, quota.Apply("main", 2, 25, true, func() error { return batch.Commit(pebble.Sync) }))
	require.NoError(t, batch.Close())
	require.NoError(t, branch.Close())
	_, err = pool.Exec(ctx, `CREATE FUNCTION test_pause_renew_insert() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_xact_lock(987654321); RETURN NEW; END $$;
		CREATE TRIGGER test_pause_renew BEFORE INSERT ON client_sessions FOR EACH ROW EXECUTE FUNCTION test_pause_renew_insert()`)
	require.NoError(t, err)
	lockConn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	defer lockConn.Release()
	_, err = lockConn.Exec(ctx, `SELECT pg_advisory_lock(987654321)`)
	require.NoError(t, err)
	defer func() { _, _ = lockConn.Exec(context.Background(), `SELECT pg_advisory_unlock(987654321)`) }()
	type outcome struct {
		result meta.RenewSessionResult
		err    error
	}
	completed := make(chan outcome, 1)
	go func() {
		result, err := mgr.RenewBranchSession(ctx, params)
		completed <- outcome{result, err}
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 987654321 AND NOT granted)`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, params.SessionID)
	require.NoError(t, err)
	_, err = lockConn.Exec(ctx, `SELECT pg_advisory_unlock(987654321)`)
	require.NoError(t, err)
	out := <-completed
	require.NoError(t, out.err)
	require.True(t, out.result.SessionRecreated)
	branch, err = storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	_, _, err = branch.DB().Get(pebblekv.OrphanKey(2))
	require.ErrorIs(t, err, pebble.ErrNotFound)
	require.NoError(t, branch.Close())
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
}

func TestBranchSessionCloseCommitFailureDoesNotLeaveActiveWriter(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "branch_close_rollback")
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "rollback", MaxBytes: 1024})
	require.NoError(t, err)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: 1001, DiskID: disk.DiskID})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	inode, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 2, Mode: meta.S_IFREG, Size: 25})
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	require.NoError(t, batch.Set(pebblekv.InodeKey(2), inode, nil))
	require.NoError(t, batch.Set(pebblekv.OrphanKey(2), []byte{1}, nil))
	require.NoError(t, batch.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(pebblekv.BranchStat{LogicalBytes: 25, Inodes: 2}), nil))
	require.NoError(t, quota.Apply("main", 2, 25, true, func() error { return batch.Commit(pebble.Sync) }))
	require.NoError(t, batch.Close())
	require.NoError(t, branch.Close())
	_, err = pool.Exec(ctx, `CREATE FUNCTION test_reject_session_delete() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION 'test'; END $$;
		CREATE CONSTRAINT TRIGGER test_reject_delete AFTER DELETE ON client_sessions DEFERRABLE INITIALLY DEFERRED
		FOR EACH ROW EXECUTE FUNCTION test_reject_session_delete()`)
	require.NoError(t, err)
	require.Error(t, mgr.CloseSession(ctx, 1001))
	var active bool
	require.NoError(t, pool.QueryRow(ctx, `SELECT expires_at > clock_timestamp() FROM client_sessions WHERE session_id = $1`, 1001).Scan(&active))
	require.False(t, active, "回滚后不能恢复已清扫分支的活跃写端")
	require.Equal(t, meta.TreeQuotaUsage{Inodes: 1}, quota.Usage())
	_, err = pool.Exec(ctx, `DROP TRIGGER test_reject_delete ON client_sessions`)
	require.NoError(t, err)
	err = mgr.CloseSession(ctx, 9999)
	require.NoError(t, err)
	var count int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id = $1`, 1001).Scan(&count))
	require.Zero(t, count)
}
