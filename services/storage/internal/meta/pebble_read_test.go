package meta_test

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type pebbleReadTracer struct {
	mu      sync.Mutex
	enabled bool
	queries []string
}

var pebbleLegacyTables = regexp.MustCompile(`(?i)\b(inodes|entries)\b`)

func (tracer *pebbleReadTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	if tracer.enabled && pebbleLegacyTables.MatchString(data.SQL) {
		tracer.queries = append(tracer.queries, data.SQL)
	}
	return ctx
}

func (*pebbleReadTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type pebbleReadFixture struct {
	ctx      context.Context
	pool     *pgxpool.Pool
	observed *pgxpool.Pool
	mgr      meta.Manager
	storage  *pebblekv.Manager
	disk     *meta.Disk
	root     string
	main     uint64
	feature  uint64
	snap     uint64
	tracer   *pebbleReadTracer
}

func newPebbleReadFixture(t *testing.T) *pebbleReadFixture {
	t.Helper()
	tracer := &pebbleReadTracer{}
	pool, observed, _ := setupIsolatedMetaTestDBWithTracer(t, "pebble_read", tracer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	root := t.TempDir()
	mgr := meta.New(db.NewStore(observed), meta.WithStorageRoot(root), meta.WithEntryTimeout(2), meta.WithDirEntryTimeout(3), meta.WithAttrTimeout(4), meta.WithSessionLeaseTTL(3*time.Minute))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "读取测试", MaxBytes: 1 << 20})
	require.NoError(t, err)
	mainID, featureID, snapID := uint64(501), uint64(502), uint64(503)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: mainID, DiskID: disk.DiskID})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	mainSizes := map[uint64]uint64{1: 0}
	put := func(inode pebblekv.Inode, name string, orphan bool) {
		t.Helper()
		inode.ParentIno = 1
		inode.UID, inode.GID, inode.Generation = 42, 43, 7
		inode.AtimeSec, inode.MtimeSec, inode.CtimeSec = 1, 2, 3
		value, err := pebblekv.EncodeInode(inode)
		require.NoError(t, err)
		require.NoError(t, batch.Set(pebblekv.InodeKey(inode.Ino), value, nil))
		if name != "" {
			require.NoError(t, batch.Set(pebblekv.EntryKey(1, []byte(name)), pebblekv.EncodeEntry(pebblekv.Entry{Ino: inode.Ino, Mode: inode.Mode}), nil))
		}
		if orphan {
			require.NoError(t, batch.Set(pebblekv.OrphanKey(inode.Ino), []byte{1}, nil))
		}
		if inode.Nlink > 0 || orphan {
			mainSizes[inode.Ino] = inode.Size
		}
	}
	put(pebblekv.Inode{Ino: 2, Mode: meta.S_IFDIR | 0o755, Nlink: 2}, "dir", false)
	put(pebblekv.Inode{Ino: 3, Mode: meta.S_IFREG | 0o640, Nlink: 1, Size: 513, Blksize: 8192}, "file", false)
	put(pebblekv.Inode{Ino: 4, Mode: meta.S_IFLNK | 0o777, Nlink: 1, Size: 3, SymlinkTarget: []byte{'x', 0xff, 'y'}}, "link", false)
	put(pebblekv.Inode{Ino: 5, Mode: meta.S_IFIFO, Nlink: 1, Size: 9, Rdev: 5}, "fifo", false)
	put(pebblekv.Inode{Ino: 6, Mode: meta.S_IFSOCK, Nlink: 1, Size: 9, Rdev: 5}, "sock", false)
	put(pebblekv.Inode{Ino: 7, Mode: meta.S_IFCHR, Nlink: 1, Size: 9, Rdev: 123}, "chr", false)
	put(pebblekv.Inode{Ino: 8, Mode: meta.S_IFBLK, Nlink: 1, Size: 9, Rdev: 456}, "blk", false)
	put(pebblekv.Inode{Ino: 9, Mode: meta.S_IFREG, Size: 11}, "orphan", true)
	put(pebblekv.Inode{Ino: 10, Mode: meta.S_IFREG, Size: 100}, "dead", false)
	put(pebblekv.Inode{Ino: 11, Mode: meta.S_IFDIR}, "", true)
	put(pebblekv.Inode{Ino: 12, Mode: meta.S_IFDIR}, "", false)
	put(pebblekv.Inode{Ino: 13, Mode: meta.S_IFLNK, Size: 3, SymlinkTarget: []byte{'x', 0xff, 'y'}}, "", true)
	put(pebblekv.Inode{Ino: 14, Mode: meta.S_IFLNK, Nlink: 1}, "empty", false)
	put(pebblekv.Inode{Ino: 15, Mode: meta.S_IFLNK, SymlinkTarget: []byte("目标")}, "", false)
	require.NoError(t, batch.Set(pebblekv.EntryKey(1, []byte("dangling")), pebblekv.EncodeEntry(pebblekv.Entry{Ino: 9999}), nil))
	for _, name := range [][]byte{{0x80}, {0xff}} {
		require.NoError(t, batch.Set(pebblekv.EntryKey(1, name), pebblekv.EncodeEntry(pebblekv.Entry{Ino: 3, Mode: meta.S_IFREG}), nil))
	}
	for i := range 230 {
		ino := uint64(1000 + i)
		value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: ino, ParentIno: 2, Mode: meta.S_IFREG | 0o644, Nlink: 1})
		require.NoError(t, err)
		require.NoError(t, batch.Set(pebblekv.InodeKey(ino), value, nil))
		name := []byte(fmt.Sprintf("p%03d", i))
		require.NoError(t, batch.Set(pebblekv.EntryKey(2, name), pebblekv.EncodeEntry(pebblekv.Entry{Ino: ino, Mode: meta.S_IFREG | 0o644}), nil))
		mainSizes[ino] = 0
	}
	require.NoError(t, batch.Commit(pebble.Sync))
	require.NoError(t, batch.Close())
	commit := uuid.NewString()
	require.NoError(t, branch.Checkpoint(commit))
	require.NoError(t, branch.Close())
	require.NoError(t, storage.Fork(disk.DiskID, commit, "feature"))
	_, err = pool.Exec(ctx, `INSERT INTO commits (commit_id, disk_id, branch_name, checkpoint_dir) VALUES ($1, $2, 'main', $3)`, commit, disk.DiskID, filepath.Join(root, "disks", disk.DiskID, "commits", commit))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', $2::text, 'head_commit_id', $2::text, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, disk.DiskID, commit)
	require.NoError(t, err)
	require.NoError(t, quota.ReplaceBranch("main", mainSizes))
	feature := "feature"
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: featureID, DiskID: disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	branch, err = storage.OpenBranch(disk.DiskID, feature)
	require.NoError(t, err)
	value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 3000, ParentIno: 1, Mode: meta.S_IFREG, Nlink: 1, Size: 25})
	require.NoError(t, err)
	require.NoError(t, branch.DB().Set(pebblekv.InodeKey(3000), value, pebble.Sync))
	require.NoError(t, branch.DB().Set(pebblekv.EntryKey(1, []byte("feature")), pebblekv.EncodeEntry(pebblekv.Entry{Ino: 3000, Mode: meta.S_IFREG}), pebble.Sync))
	require.NoError(t, branch.Close())
	featureSizes := make(map[uint64]uint64, len(mainSizes))
	for ino, size := range mainSizes {
		if ino != 9 && ino != 11 && ino != 13 {
			featureSizes[ino] = size
		}
	}
	featureSizes[3000] = 25
	require.NoError(t, quota.ReplaceBranch("feature", featureSizes))
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: snapID, DiskID: disk.DiskID, ReadOnly: true, CommitID: &commit})
	require.NoError(t, err)
	tracer.mu.Lock()
	tracer.enabled = true
	tracer.mu.Unlock()
	return &pebbleReadFixture{ctx: ctx, pool: pool, observed: observed, mgr: mgr, storage: storage, disk: disk, root: root, main: mainID, feature: featureID, snap: snapID, tracer: tracer}
}

func (f *pebbleReadFixture) mutate(t *testing.T, branchName string, fn func(*pebble.DB)) {
	t.Helper()
	branch, err := f.storage.OpenBranch(f.disk.DiskID, branchName)
	require.NoError(t, err)
	fn(branch.DB())
	require.NoError(t, branch.Close())
}

func TestPebbleReadIntegration(t *testing.T) {
	f := newPebbleReadFixture(t)
	ctx := meta.WithSessionID(f.ctx, &f.main)
	snapshot := meta.WithSessionID(f.ctx, &f.snap)
	feature := meta.WithSessionID(f.ctx, &f.feature)

	t.Run("分支和快照隔离", func(t *testing.T) {
		got, err := f.mgr.Lookup(feature, 1, []byte("feature"))
		require.NoError(t, err)
		require.Equal(t, uint64(3000), got.Ino)
		_, err = f.mgr.Lookup(ctx, 1, []byte("feature"))
		require.ErrorIs(t, err, meta.ErrNotFound)
		_, err = f.mgr.Lookup(snapshot, 1, []byte("feature"))
		require.ErrorIs(t, err, meta.ErrNotFound)
		_, err = f.mgr.GetAttr(snapshot, 9)
		require.ErrorIs(t, err, meta.ErrNotFound)
		results := make(chan error, 2)
		for _, view := range []context.Context{feature, snapshot} {
			go func(view context.Context) {
				for range 5 {
					if _, err := f.mgr.GetAttr(view, 3); err != nil {
						results <- err
						return
					}
				}
				results <- nil
			}(view)
		}
		require.NoError(t, <-results)
		require.NoError(t, <-results)
	})

	t.Run("查找与孤儿", func(t *testing.T) {
		for _, tc := range []struct {
			parent uint64
			name   string
			want   uint64
			err    error
		}{
			{1, ".", 1, nil}, {1, "..", 1, nil}, {2, "..", 1, nil}, {11, "..", 1, nil},
			{11, ".", 0, meta.ErrNotFound}, {11, "x", 0, meta.ErrNotFound}, {12, "..", 0, meta.ErrNotFound},
			{1, "dead", 0, meta.ErrNotFound}, {1, "dangling", 0, meta.ErrNotFound}, {1, "", 0, meta.ErrNotFound},
			{9999, "x", 0, meta.ErrNotFound}, {3, "x", 0, meta.ErrNotDir}, {4, ".", 0, meta.ErrNotDir},
		} {
			got, err := f.mgr.Lookup(ctx, tc.parent, []byte(tc.name))
			if tc.err != nil {
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, got.Ino)
			}
		}
		_, err := f.mgr.Lookup(ctx, 0, []byte("x"))
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = f.mgr.Lookup(ctx, 1, make([]byte, meta.MaxFileNameBytes+1))
		require.ErrorIs(t, err, meta.ErrNameTooLong)
		_, err = f.mgr.Lookup(ctx, 1, []byte("a/b"))
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = f.mgr.Lookup(ctx, 1, []byte{'a', 0})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("父目录孤儿与根目录孤儿", func(t *testing.T) {
		var rootValue []byte
		f.mutate(t, "main", func(store *pebble.DB) {
			value, closer, err := store.Get(pebblekv.InodeKey(1))
			require.NoError(t, err)
			rootValue = append([]byte(nil), value...)
			require.NoError(t, closer.Close())
			root, err := pebblekv.DecodeInode(rootValue)
			require.NoError(t, err)
			root.Nlink = 0
			value, err = pebblekv.EncodeInode(root)
			require.NoError(t, err)
			require.NoError(t, store.Set(pebblekv.InodeKey(1), value, pebble.Sync))
			for ino, parent := range map[uint64]uint64{16: 12, 17: 11, 18: 9999, 19: 19} {
				value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: ino, ParentIno: parent, Mode: meta.S_IFDIR, Nlink: 1})
				require.NoError(t, err)
				require.NoError(t, store.Set(pebblekv.InodeKey(ino), value, pebble.Sync))
			}
		})
		t.Cleanup(func() {
			f.mutate(t, "main", func(store *pebble.DB) {
				require.NoError(t, store.Set(pebblekv.InodeKey(1), rootValue, pebble.Sync))
				require.NoError(t, store.Delete(pebblekv.OrphanKey(1), pebble.Sync))
				for ino := uint64(16); ino <= 19; ino++ {
					require.NoError(t, store.Delete(pebblekv.InodeKey(ino), pebble.Sync))
				}
			})
		})
		_, err := f.mgr.Lookup(ctx, 1, []byte(".."))
		require.ErrorIs(t, err, meta.ErrNotFound)
		f.mutate(t, "main", func(store *pebble.DB) {
			require.NoError(t, store.Set(pebblekv.OrphanKey(1), []byte{1}, pebble.Sync))
		})
		root, err := f.mgr.Lookup(ctx, 1, []byte(".."))
		require.NoError(t, err)
		require.Equal(t, uint64(1), root.Ino)
		_, err = f.mgr.Lookup(ctx, 1, []byte("."))
		require.ErrorIs(t, err, meta.ErrNotFound)
		for _, ino := range []uint64{16, 18, 19} {
			_, err = f.mgr.Lookup(ctx, ino, []byte(".."))
			require.ErrorIs(t, err, meta.ErrNotFound)
		}
		parent, err := f.mgr.Lookup(ctx, 17, []byte(".."))
		require.NoError(t, err)
		require.Equal(t, uint64(11), parent.Ino)
	})

	t.Run("节点属性", func(t *testing.T) {
		for _, tc := range []struct {
			ino           uint64
			size, blocks  uint64
			rdev, blksize uint32
			entry         float64
		}{
			{2, 4096, 8, 0, 65536, 3}, {3, 513, 2, 0, 8192, 2}, {4, 3, 1, 0, 65536, 2},
			{5, 0, 0, 0, 65536, 2}, {6, 0, 0, 0, 65536, 2}, {7, 0, 0, 123, 65536, 2}, {8, 0, 0, 456, 65536, 2},
			{9, 11, 1, 0, 65536, 2},
		} {
			got, err := f.mgr.GetAttr(ctx, tc.ino)
			require.NoError(t, err)
			require.Equal(t, tc.size, got.Attr.Size)
			require.Equal(t, tc.blocks, got.Attr.Blocks)
			require.Equal(t, tc.rdev, got.Attr.Rdev)
			require.Equal(t, tc.blksize, got.Attr.Blksize)
			require.Equal(t, tc.entry, got.EntryTimeoutSeconds)
			require.Equal(t, float64(4), got.AttrTimeoutSeconds)
			require.Equal(t, uint32(42), got.Attr.UID)
			require.Equal(t, uint32(43), got.Attr.GID)
			require.Equal(t, uint64(7), got.Generation)
			require.Equal(t, time.Unix(1, 0).UTC(), got.Attr.Atime)
		}
		for _, ino := range []uint64{10, 12, 9999} {
			_, err := f.mgr.GetAttr(ctx, ino)
			require.ErrorIs(t, err, meta.ErrNotFound)
		}
		_, err := f.mgr.GetAttr(ctx, math.MaxUint64)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("软链接", func(t *testing.T) {
		for _, ino := range []uint64{4, 13} {
			target, err := f.mgr.Readlink(ctx, ino)
			require.NoError(t, err)
			require.Equal(t, []byte{'x', 0xff, 'y'}, target)
		}
		for _, ino := range []uint64{2, 3} {
			_, err := f.mgr.Readlink(ctx, ino)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		}
		_, err := f.mgr.Readlink(ctx, 14)
		require.ErrorIs(t, err, meta.ErrIO)
		_, err = f.mgr.Readlink(ctx, 10)
		require.ErrorIs(t, err, meta.ErrNotFound)
		_, err = f.mgr.Readlink(ctx, 15)
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目录错误和点项", func(t *testing.T) {
		for _, tc := range []struct {
			ino uint64
			err error
		}{{9999, meta.ErrNotFound}, {3, meta.ErrNotDir}, {12, meta.ErrNotFound}} {
			_, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: tc.ino, Limit: 10, SessionID: &f.main})
			require.ErrorIs(t, err, tc.err)
			_, err = f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: tc.ino, Limit: 10, SessionID: &f.main})
			require.ErrorIs(t, err, tc.err)
		}
		empty, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 11, Limit: 10, SessionID: &f.main})
		require.NoError(t, err)
		require.Empty(t, empty)
		emptyPlus, err := f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: 11, Limit: 10, SessionID: &f.main})
		require.NoError(t, err)
		require.Empty(t, emptyPlus)
		plus, err := f.mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{Ino: 2, Limit: 3})
		require.NoError(t, err)
		require.Equal(t, []byte("."), plus[0].Name)
		require.Equal(t, []byte(".."), plus[1].Name)
		require.Equal(t, uint64(1), plus[0].NextOffset)
		require.Equal(t, uint64(2), plus[1].NextOffset)
		require.Equal(t, uint64(1), plus[1].Ino)
		require.Zero(t, plus[0].EntryTimeoutSeconds)
		require.Zero(t, plus[1].Inode.EntryTimeoutSeconds)
		require.Equal(t, float64(4), plus[1].AttrTimeoutSeconds)
		require.Equal(t, []byte("p000"), plus[2].Name)
		root, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 1, Limit: 30, SessionID: &f.main})
		require.NoError(t, err)
		var names [][]byte
		for _, entry := range root {
			names = append(names, entry.Name)
		}
		require.Equal(t, [][]byte{[]byte("."), []byte(".."), []byte("blk"), []byte("chr"), []byte("dir"), []byte("empty"), []byte("fifo"), []byte("file"), []byte("link"), []byte("sock"), {0x80}, {0xff}}, names)
		_, err = f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 1, Limit: 1})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("分页", func(t *testing.T) {
		for _, plus := range []bool{false, true} {
			var names []string
			var offset uint64
			for {
				if plus {
					page, err := f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: 2, Fh: 7, Offset: offset, Limit: 7, SessionID: &f.main})
					require.NoError(t, err)
					if len(page) == 0 {
						break
					}
					for _, e := range page {
						names = append(names, string(e.Name))
						offset = e.NextOffset
					}
				} else {
					page, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 2, Offset: offset, Limit: 7, SessionID: &f.main})
					require.NoError(t, err)
					if len(page) == 0 {
						break
					}
					for _, e := range page {
						names = append(names, string(e.Name))
						offset = e.NextOffset
					}
				}
			}
			want := []string{".", ".."}
			for i := range 230 {
				want = append(want, fmt.Sprintf("p%03d", i))
			}
			require.Equal(t, want, names)
		}
		page, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 2, Offset: 150, Limit: 1, SessionID: &f.snap})
		require.NoError(t, err)
		require.Equal(t, []byte("p148"), page[0].Name)
	})

	t.Run("分页期间增删", func(t *testing.T) {
		for _, tc := range []struct {
			branch string
			sid    *uint64
			plus   bool
		}{{"main", &f.main, false}, {"feature", &f.feature, true}} {
			var offset uint64
			if tc.plus {
				page, err := f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: 2, Limit: 4, SessionID: tc.sid})
				require.NoError(t, err)
				require.Equal(t, []byte("p001"), page[3].Name)
				offset = page[3].NextOffset
			} else {
				page, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 2, Limit: 4, SessionID: tc.sid})
				require.NoError(t, err)
				require.Equal(t, []byte("p001"), page[3].Name)
				offset = page[3].NextOffset
			}
			f.mutate(t, tc.branch, func(store *pebble.DB) {
				require.NoError(t, store.Delete(pebblekv.EntryKey(2, []byte("p000")), pebble.Sync))
				require.NoError(t, store.Delete(pebblekv.EntryKey(2, []byte("p001")), pebble.Sync))
				for _, name := range []string{"p000a", "p001a"} {
					require.NoError(t, store.Set(pebblekv.EntryKey(2, []byte(name)), pebblekv.EncodeEntry(pebblekv.Entry{Ino: 1002, Mode: meta.S_IFREG}), pebble.Sync))
				}
			})
			if tc.plus {
				page, err := f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: 2, Offset: offset, Limit: 3, SessionID: tc.sid})
				require.NoError(t, err)
				require.Equal(t, []byte("p001a"), page[0].Name)
				require.Equal(t, []byte("p002"), page[1].Name)
			} else {
				page, err := f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 2, Offset: offset, Limit: 3, SessionID: tc.sid})
				require.NoError(t, err)
				require.Equal(t, []byte("p001a"), page[0].Name)
				require.Equal(t, []byte("p002"), page[1].Name)
			}
		}
	})

	t.Run("容量", func(t *testing.T) {
		f.tracer.mu.Lock()
		f.tracer.enabled = false
		f.tracer.mu.Unlock()
		defer func() {
			f.tracer.mu.Lock()
			f.tracer.enabled = true
			f.tracer.mu.Unlock()
		}()
		stat, err := f.mgr.StatFS(ctx, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(1<<20), stat.TotalBytes)
		require.Equal(t, uint64(591), stat.UsedBytes)
		require.Equal(t, uint64(243), stat.Files)
		require.Equal(t, uint64(1<<20-591), stat.AvailBytes)
		_, err = f.mgr.StatFS(ctx, 10)
		require.ErrorIs(t, err, meta.ErrNotFound)
		_, err = f.mgr.StatFS(ctx, 9)
		require.NoError(t, err)
		_, err = f.mgr.StatFS(ctx, 9999)
		require.ErrorIs(t, err, meta.ErrNotFound)
		cold := meta.New(db.NewStore(f.observed), meta.WithStorageRoot(f.root))
		stat, err = cold.StatFS(snapshot, 3)
		require.NoError(t, err)
		require.Equal(t, uint64(591), stat.UsedBytes)
	})

	t.Run("会话校验", func(t *testing.T) {
		zero, missing := uint64(0), uint64(999999)
		_, err := f.mgr.GetAttr(meta.WithSessionID(f.ctx, &zero), 1)
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = f.mgr.GetAttr(meta.WithSessionID(f.ctx, &missing), 1)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 1, Limit: 1, SessionID: &missing})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: 1, Limit: 1, SessionID: &zero})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = f.mgr.Lookup(meta.WithSessionID(f.ctx, &missing), 1, []byte("file"))
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = f.mgr.Readlink(meta.WithSessionID(f.ctx, &missing), 4)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = f.mgr.StatFS(meta.WithSessionID(f.ctx, &missing), 1)
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = f.pool.Exec(f.ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, f.feature)
		require.NoError(t, err)
		_, err = f.mgr.ReadDir(f.ctx, meta.ReadDirParams{Ino: 1, Fh: 7, Limit: 1, SessionID: &f.feature})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
		_, err = f.mgr.ReadDirPlus(f.ctx, meta.ReadDirPlusParams{Ino: 1, Limit: 1, SessionID: &f.feature})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	f.tracer.mu.Lock()
	defer f.tracer.mu.Unlock()
	require.Empty(t, f.tracer.queries, "Pebble 读取不能访问旧节点表")
}

func TestPebbleReadAtime(t *testing.T) {
	f := newPebbleReadFixture(t)
	for _, tc := range []struct {
		mode          meta.AtimeMode
		before, mtime int64
		update        bool
	}{
		{meta.AtimeModeNoAtime, 1, 2, false},
		{meta.AtimeModeRelAtime, 1, 2, true},
		{meta.AtimeModeRelAtime, time.Now().Unix(), 1, false},
		{meta.AtimeModeStrictAtime, 1, 2, true},
	} {
		t.Run(fmt.Sprintf("模式%d-时间%d", tc.mode, tc.before), func(t *testing.T) {
			f.mutate(t, "main", func(store *pebble.DB) {
				value, err := pebblekv.EncodeInode(pebblekv.Inode{Ino: 4, ParentIno: 1, Mode: meta.S_IFLNK, Nlink: 1, SymlinkTarget: []byte("目标"), AtimeSec: tc.before, MtimeSec: tc.mtime, CtimeSec: tc.mtime})
				require.NoError(t, err)
				require.NoError(t, store.Set(pebblekv.InodeKey(4), value, pebble.Sync))
			})
			mgr := meta.New(db.NewStore(f.pool), meta.WithStorageRoot(f.root), meta.WithAtimeMode(tc.mode))
			ctx := meta.WithSessionID(f.ctx, &f.main)
			_, err := mgr.Readlink(ctx, 4)
			require.NoError(t, err)
			got, err := mgr.GetAttr(ctx, 4)
			require.NoError(t, err)
			if tc.update {
				require.Greater(t, got.Attr.Atime.Unix(), tc.before)
			} else {
				require.Equal(t, tc.before, got.Attr.Atime.Unix())
			}
			snapshot := meta.WithSessionID(f.ctx, &f.snap)
			_, err = mgr.Readlink(snapshot, 4)
			require.NoError(t, err)
			got, err = mgr.GetAttr(snapshot, 4)
			require.NoError(t, err)
			require.Equal(t, int64(1), got.Attr.Atime.Unix())
		})
	}
}

func TestPebbleReadStatFSLegacyOnRestart(t *testing.T) {
	f := newPebbleReadFixture(t)
	legacy := meta.New(db.NewStore(f.pool), meta.WithStorageRoot(f.root))
	file, err := legacy.Mknod(f.ctx, meta.MknodParams{ParentIno: f.disk.RootIno, Name: []byte("旧文件"), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	size := uint64(1000)
	_, err = legacy.SetAttr(f.ctx, file.Ino, meta.SetAttrInput{Size: &size})
	require.NoError(t, err)
	want, err := legacy.GetDisk(f.ctx, f.disk.DiskID)
	require.NoError(t, err)
	restarted := meta.New(db.NewStore(f.observed), meta.WithStorageRoot(f.root))
	got, err := restarted.StatFS(meta.WithSessionID(f.ctx, &f.snap), 3)
	require.NoError(t, err)
	require.Equal(t, want.UsedBytes, got.UsedBytes)
	require.Equal(t, want.UsedInodes, got.Files)
}
