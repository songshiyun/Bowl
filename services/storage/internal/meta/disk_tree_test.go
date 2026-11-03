package meta_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func TestDiskCreatesMainAndInitialCommit(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree")
	root := t.TempDir()
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 1024})
	require.NoError(t, err)
	require.Equal(t, "main", disk.DefaultBranch)
	require.Len(t, disk.Branches, 1)
	require.NotNil(t, disk.Branches[0].HeadCommitID)
	require.Equal(t, disk.Branches[0].HeadCommitID, disk.Branches[0].ForkCommitID)
	require.Equal(t, uint64(0), disk.UsedBytes)
	require.Equal(t, uint64(1), disk.UsedInodes)

	storage := pebblekv.NewManager(root)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	value, closer, err := branch.DB().Get(pebblekv.InodeKey(1))
	require.NoError(t, err)
	inode, err := pebblekv.DecodeInode(value)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Equal(t, uint64(1), inode.Ino)
	require.Equal(t, uint32(2), inode.Nlink)
	value, closer, err = branch.DB().Get(pebblekv.StatKey())
	require.NoError(t, err)
	stat, err := pebblekv.DecodeBranchStat(value)
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.Equal(t, pebblekv.BranchStat{Inodes: 1, NextWriteSeq: 1}, stat)
	require.NoError(t, branch.Close())
	commit, err := storage.OpenCommit(disk.DiskID, *disk.Branches[0].HeadCommitID)
	require.NoError(t, err)
	_, closer, err = commit.DB().Get(pebblekv.InodeKey(1))
	require.NoError(t, err)
	require.NoError(t, closer.Close())
	require.NoError(t, commit.Close())
	nextIno, err := storage.AllocateInode(disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(2), nextIno)
	require.NoError(t, storage.Close())
	var storageDir, checkpointDir string
	require.NoError(t, pool.QueryRow(ctx, `SELECT d.storage_dir, c.checkpoint_dir FROM disks d JOIN commits c ON c.disk_id = d.disk_id WHERE d.disk_id = $1 AND c.commit_id = $2`, disk.DiskID, *disk.Branches[0].HeadCommitID).Scan(&storageDir, &checkpointDir))
	require.Equal(t, filepath.Join(root, "disks", disk.DiskID), storageDir)
	require.Equal(t, filepath.Join(storageDir, "commits", *disk.Branches[0].HeadCommitID), checkpointDir)

	got, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, disk.Branches, got.Branches)
	list, err := mgr.ListDisks(ctx, meta.ListDisksParams{})
	require.NoError(t, err)
	require.Len(t, list.Disks, 1)
	require.Equal(t, disk.Branches, list.Disks[0].Branches)
	fs, err := mgr.StatFS(ctx, disk.RootIno)
	require.NoError(t, err)
	require.Equal(t, disk.UsedBytes, fs.UsedBytes)
	require.Equal(t, disk.UsedInodes, fs.Files)

	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
	_, err = os.Stat(filepath.Join(root, "disks", disk.DiskID))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskRestoresDeduplicatedBranchUsage(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_quota")
	root := t.TempDir()
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 400})
	require.NoError(t, err)

	storage := pebblekv.NewManager(root)
	main, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	put := func(branch *pebblekv.Branch, ino, size uint64, nlink uint32) {
		t.Helper()
		value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: ino, Mode: meta.S_IFREG | 0o644, Nlink: nlink, Size: size})
		require.NoError(t, err)
		require.NoError(t, branch.DB().Set(pebblekv.InodeKey(ino), value, pebble.Sync))
	}
	put(main, 2, 100, 1)
	put(main, 3, 20, 1)
	require.NoError(t, main.Checkpoint("test-fork"))
	require.NoError(t, main.Close())
	require.NoError(t, storage.Fork(disk.DiskID, "test-fork", "feature"))
	feature, err := storage.OpenBranch(disk.DiskID, "feature")
	require.NoError(t, err)
	put(feature, 2, 150, 1)
	put(feature, 4, 30, 1)
	put(feature, 5, 1000, 0)
	put(feature, 6, 10, 0)
	require.NoError(t, feature.DB().Set(pebblekv.OrphanKey(6), []byte{1}, pebble.Sync))
	require.NoError(t, feature.Close())
	require.NoError(t, storage.Close())
	_, err = pool.Exec(ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, disk.DiskID)
	require.NoError(t, err)

	restarted := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	got, err := restarted.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(210), got.UsedBytes)
	require.Equal(t, uint64(5), got.UsedInodes)
	require.Len(t, got.Branches, 2)
	list, err := restarted.ListDisks(ctx, meta.ListDisksParams{})
	require.NoError(t, err)
	require.Equal(t, uint64(210), list.Disks[0].UsedBytes)
	fs, err := restarted.StatFS(ctx, disk.RootIno)
	require.NoError(t, err)
	require.Equal(t, uint64(210), fs.UsedBytes)
	require.Equal(t, uint64(5), fs.Files)
	tooSmall := uint64(209)
	_, err = restarted.UpdateDisk(ctx, meta.UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &tooSmall})
	require.ErrorIs(t, err, meta.ErrInvalidShrink)
	newLimit := uint64(220)
	_, err = restarted.UpdateDisk(ctx, meta.UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &newLimit})
	require.NoError(t, err)
	quota, err := restarted.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	storage = pebblekv.NewManager(root)
	feature, err = storage.OpenBranch(disk.DiskID, "feature")
	require.NoError(t, err)
	value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 4, Mode: meta.S_IFREG | 0o644, Nlink: 1, Size: 40})
	require.NoError(t, err)
	require.NoError(t, quota.Apply("feature", 4, 40, true, func() error {
		return feature.DB().Set(pebblekv.InodeKey(4), value, pebble.Sync)
	}))
	require.ErrorIs(t, quota.Apply("feature", 7, 1, true, nil), meta.ErrNoSpace)
	require.NoError(t, feature.Close())
	require.NoError(t, storage.Close())
	got, err = restarted.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(220), got.UsedBytes)
	require.NoError(t, restarted.DeleteDisk(ctx, disk.DiskID))
}

func TestDiskLegacyWritesRespectBranchQuota(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_legacy")
	root := t.TempDir()
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 150})
	require.NoError(t, err)
	file, err := mgr.Mknod(ctx, meta.MknodParams{ParentIno: disk.RootIno, Name: []byte("old"), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	size := uint64(100)
	_, err = mgr.SetAttr(ctx, file.Ino, meta.SetAttrInput{Size: &size})
	require.NoError(t, err)

	storage := pebblekv.NewManager(root)
	require.NoError(t, storage.Fork(disk.DiskID, *disk.Branches[0].HeadCommitID, "feature"))
	feature, err := storage.OpenBranch(disk.DiskID, "feature")
	require.NoError(t, err)
	value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: file.Ino, Mode: meta.S_IFREG | 0o644, Nlink: 1, Size: 150})
	require.NoError(t, err)
	require.NoError(t, feature.DB().Set(pebblekv.InodeKey(file.Ino), value, pebble.Sync))
	require.NoError(t, feature.Close())
	require.NoError(t, storage.Close())
	_, err = pool.Exec(ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, disk.DiskID)
	require.NoError(t, err)

	restarted := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	got, err := restarted.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(150), got.UsedBytes)
	size = 140
	_, err = restarted.SetAttr(ctx, file.Ino, meta.SetAttrInput{Size: &size})
	require.NoError(t, err)
	firstHandle, err := restarted.Open(ctx, meta.OpenParams{Ino: file.Ino, Flags: meta.O_RDWR})
	require.NoError(t, err)
	_, err = restarted.Write(ctx, &meta.WriteRequest{Ino: file.Ino, Fh: firstHandle.Fh, Offset: 140, Data: []byte("12345")})
	require.NoError(t, err)
	_, err = restarted.Write(ctx, &meta.WriteRequest{Ino: file.Ino, Fh: firstHandle.Fh, Offset: 150, Data: []byte("x")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.NoError(t, restarted.Release(ctx, meta.ReleaseParams{Ino: file.Ino, Fh: firstHandle.Fh}))
	second, err := restarted.Mknod(ctx, meta.MknodParams{ParentIno: disk.RootIno, Name: []byte("new"), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	size = 1
	_, err = restarted.SetAttr(ctx, second.Ino, meta.SetAttrInput{Size: &size})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	handle, err := restarted.Open(ctx, meta.OpenParams{Ino: second.Ino, Flags: meta.O_RDWR})
	require.NoError(t, err)
	_, err = restarted.Write(ctx, &meta.WriteRequest{Ino: second.Ino, Fh: handle.Fh, Data: []byte("x")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.NoError(t, restarted.Release(ctx, meta.ReleaseParams{Ino: second.Ino, Fh: handle.Fh}))
	require.NoError(t, restarted.DeleteDisk(ctx, disk.DiskID))
}

func TestDiskDeleteChecksAllBranchSessions(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_session")
	root := t.TempDir()
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 1024})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, disk_id, expires_at)
		VALUES (41, $1, clock_timestamp() + interval '1 minute')`, disk.DiskID)
	require.NoError(t, err)
	require.ErrorIs(t, mgr.DeleteDisk(ctx, disk.DiskID), meta.ErrBusy)
	_, err = pool.Exec(ctx, `DELETE FROM client_sessions WHERE session_id = 41`)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO client_sessions (session_id, disk_id, branch_name, read_only, lease_managed, expires_at)
		VALUES (42, $1, 'feature', false, true, clock_timestamp() + interval '1 minute')`, disk.DiskID)
	require.NoError(t, err)
	require.ErrorIs(t, mgr.DeleteDisk(ctx, disk.DiskID), meta.ErrBusy)
	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 minute' WHERE session_id = 42`)
	require.NoError(t, err)
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
	var commits, disks, sessions int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM commits WHERE disk_id = $1`, disk.DiskID).Scan(&commits))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM disks WHERE disk_id = $1`, disk.DiskID).Scan(&disks))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id IN (41, 42)`).Scan(&sessions))
	require.Zero(t, commits)
	require.Zero(t, disks)
	require.Zero(t, sessions)
	_, err = os.Stat(filepath.Join(root, "disks", disk.DiskID))
	require.ErrorIs(t, err, os.ErrNotExist)
	require.ErrorIs(t, quota.Apply("main", 2, 1, true, nil), meta.ErrNotFound)
}

func TestTreeQuotaTracksLegacyCreateAndUnlink(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_inode")
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(t.TempDir()))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 100})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Mknod(ctx, meta.MknodParams{ParentIno: disk.RootIno, Name: []byte("file"), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 0, Inodes: 2}, quota.Usage())
	size := uint64(50)
	_, err = mgr.SetAttr(ctx, file.Ino, meta.SetAttrInput{Size: &size})
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 50, Inodes: 2}, quota.Usage())
	require.NoError(t, mgr.Unlink(ctx, disk.RootIno, []byte("file")))
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 0, Inodes: 1}, quota.Usage())
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
}

func TestDiskDeleteDoesNotRequireReadableBranch(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_missing")
	root := t.TempDir()
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 100})
	require.NoError(t, err)
	require.NoError(t, os.RemoveAll(filepath.Join(root, "disks", disk.DiskID, "branches", "main")))
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
	_, err = os.Stat(filepath.Join(root, "disks", disk.DiskID))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestDiskCreateRemovesStorageWhenCommitRegistrationFails(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_create_error")
	root := t.TempDir()
	_, err := pool.Exec(ctx, `CREATE FUNCTION reject_initial_commit() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION '拒绝登记快照'; END $$;
		CREATE TRIGGER reject_initial_commit BEFORE INSERT ON commits FOR EACH ROW EXECUTE FUNCTION reject_initial_commit()`)
	require.NoError(t, err)
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root))
	_, err = mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 100})
	require.Error(t, err)
	var disks, inodes int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM disks`).Scan(&disks))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM inodes`).Scan(&inodes))
	require.Zero(t, disks)
	require.Zero(t, inodes)
	entries, err := os.ReadDir(filepath.Join(root, "disks"))
	require.NoError(t, err)
	require.Empty(t, entries)
}

func TestTreeQuotaDoesNotReportUncommittedReservation(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "disk_tree_pending")
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(t.TempDir()))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "tree", MaxBytes: 100})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	file, err := mgr.Mknod(ctx, meta.MknodParams{ParentIno: disk.RootIno, Name: []byte("file"), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `CREATE FUNCTION wait_quota_test() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN PERFORM pg_advisory_xact_lock(573001); RETURN NEW; END $$;
		CREATE TRIGGER wait_quota_test BEFORE UPDATE OF size ON inodes FOR EACH ROW EXECUTE FUNCTION wait_quota_test()`)
	require.NoError(t, err)
	conn, err := pool.Acquire(ctx)
	require.NoError(t, err)
	t.Cleanup(conn.Release)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_lock(573001)`)
	require.NoError(t, err)
	t.Cleanup(func() { _, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock(573001)`) })
	result := make(chan error, 1)
	go func() {
		size := uint64(70)
		_, err := mgr.SetAttr(ctx, file.Ino, meta.SetAttrInput{Size: &size})
		result <- err
	}()
	require.Eventually(t, func() bool {
		var waiting bool
		err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND objid = 573001 AND NOT granted)`).Scan(&waiting)
		return err == nil && waiting
	}, 5*time.Second, 10*time.Millisecond)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 0, Inodes: 2}, quota.Usage())
	require.ErrorIs(t, quota.Apply("feature", file.Ino+1000, 40, true, nil), meta.ErrNoSpace)
	beforeCommit, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Zero(t, beforeCommit.UsedBytes)
	_, err = conn.Exec(ctx, `SELECT pg_advisory_unlock(573001)`)
	require.NoError(t, err)
	require.NoError(t, <-result)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 70, Inodes: 2}, quota.Usage())
	_, err = pool.Exec(ctx, `DROP TRIGGER wait_quota_test ON inodes;
		CREATE FUNCTION reject_quota_test() RETURNS trigger LANGUAGE plpgsql AS $$
		BEGIN RAISE EXCEPTION '写入失败'; END $$;
		CREATE TRIGGER reject_quota_test BEFORE UPDATE OF size ON inodes FOR EACH ROW EXECUTE FUNCTION reject_quota_test()`)
	require.NoError(t, err)
	size := uint64(90)
	_, err = mgr.SetAttr(ctx, file.Ino, meta.SetAttrInput{Size: &size})
	require.Error(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 70, Inodes: 2}, quota.Usage())
	require.NoError(t, quota.Apply("feature", file.Ino+1000, 30, true, nil))
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
}
