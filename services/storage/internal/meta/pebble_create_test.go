package meta_test

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

type pebbleCreateEnv struct {
	pool    *pgxpool.Pool
	ctx     context.Context
	mgr     meta.Manager
	storage *pebblekv.Manager
	disk    *meta.Disk
	root    string
	sid     uint64
}

// newTestStorage 创建测试专用的 Pebble 句柄池，并在测试结束时释放。
func newTestStorage(t *testing.T, root string) *pebblekv.Manager {
	t.Helper()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	return storage
}

// newTestManager 创建与测试句柄池共用的元数据管理器。
func newTestManager(t *testing.T, store db.Store, root string, opts ...meta.Option) meta.Manager {
	t.Helper()
	return meta.New(store, append(opts, meta.WithStorageRoot(root), meta.WithPebbleManager(newTestStorage(t, root)))...)
}

func setupPebbleCreate(t *testing.T, maxBytes uint64) *pebbleCreateEnv {
	t.Helper()
	pool, _ := setupIsolatedMetaTestDB(t, "pebble_create")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	root := t.TempDir()
	storage := newTestStorage(t, root)
	mgr := meta.New(db.NewStore(pool), meta.WithStorageRoot(root), meta.WithSessionLeaseTTL(time.Minute), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "创建测试", MaxBytes: maxBytes})
	require.NoError(t, err)
	sid := uint64(101)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: disk.DiskID})
	require.NoError(t, err)
	return &pebbleCreateEnv{pool: pool, ctx: ctx, mgr: mgr, storage: storage, disk: disk, root: root, sid: sid}
}

func (e *pebbleCreateEnv) branchContext() context.Context {
	return meta.WithSessionID(e.ctx, &e.sid)
}

func (e *pebbleCreateEnv) withBranch(t *testing.T, name string, fn func(*pebble.DB)) {
	t.Helper()
	branch, err := e.storage.OpenBranch(e.disk.DiskID, name)
	require.NoError(t, err)
	defer func() { require.NoError(t, branch.Close()) }()
	fn(branch.DB())
}

func getPebbleValue(t *testing.T, store *pebble.DB, key []byte) []byte {
	t.Helper()
	value, closer, err := store.Get(key)
	require.NoError(t, err)
	result := append([]byte(nil), value...)
	require.NoError(t, closer.Close())
	return result
}

func readPebbleCreateInode(t *testing.T, store *pebble.DB, ino uint64) pebblekv.Inode {
	t.Helper()
	inode, err := pebblekv.DecodeInode(getPebbleValue(t, store, pebblekv.InodeKey(ino)))
	require.NoError(t, err)
	return inode
}

func changePebbleCreateInode(t *testing.T, e *pebbleCreateEnv, ino uint64, mutate func(*pebblekv.Inode)) {
	t.Helper()
	e.withBranch(t, "main", func(store *pebble.DB) {
		inode := readPebbleCreateInode(t, store, ino)
		mutate(&inode)
		value, err := pebblekv.EncodeInode(inode)
		require.NoError(t, err)
		require.NoError(t, store.Set(pebblekv.InodeKey(ino), value, pebble.Sync))
	})
}

func TestPebbleCreateBranchesAndRecords(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	feature := "feature"
	require.NoError(t, e.storage.Fork(e.disk.DiskID, *e.disk.Branches[0].HeadCommitID, feature))
	_, err := e.pool.Exec(e.ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, e.disk.DiskID)
	require.NoError(t, err)
	featureSID := uint64(102)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featureSID, DiskID: e.disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	mainCtx := e.branchContext()
	featureCtx := meta.WithSessionID(e.ctx, &featureSID)

	created, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("file"), Mode: 0o640, SessionID: &e.sid})
	require.NoError(t, err)
	require.Equal(t, uint64(1), created.Fh)
	featureFile, err := e.mgr.Create(featureCtx, meta.CreateParams{ParentIno: 1, Name: []byte("file"), Mode: 0o644})
	require.NoError(t, err)
	node, err := e.mgr.Mknod(mainCtx, meta.MknodParams{ParentIno: 1, Name: []byte("node"), Mode: meta.S_IFCHR | 0o600, Rdev: 123})
	require.NoError(t, err)
	featureNode, err := e.mgr.Mknod(featureCtx, meta.MknodParams{ParentIno: 1, Name: []byte("node"), Mode: meta.S_IFIFO | 0o600, Rdev: 123})
	require.NoError(t, err)
	dir, err := e.mgr.Mkdir(mainCtx, meta.MkdirParams{ParentIno: 1, Name: []byte("dir"), Mode: 0o755})
	require.NoError(t, err)
	featureDir, err := e.mgr.Mkdir(featureCtx, meta.MkdirParams{ParentIno: 1, Name: []byte("dir"), Mode: 0o700})
	require.NoError(t, err)
	sym, err := e.mgr.Symlink(mainCtx, meta.SymlinkParams{ParentIno: 1, Name: []byte("sym"), Target: []byte("target")})
	require.NoError(t, err)
	featureSym, err := e.mgr.Symlink(featureCtx, meta.SymlinkParams{ParentIno: 1, Name: []byte("sym"), Target: []byte("feature")})
	require.NoError(t, err)
	linked, err := e.mgr.Link(mainCtx, meta.LinkParams{Ino: created.Inode.Ino, NewParentIno: 1, NewName: []byte("link")})
	require.NoError(t, err)
	require.Equal(t, uint32(2), linked.Attr.Nlink)
	for i, inode := range []*meta.Inode{created.Inode, featureFile.Inode, node, featureNode, dir, featureDir, sym, featureSym} {
		require.Equal(t, uint64(i+2), inode.Ino)
	}
	require.Equal(t, uint32(123), node.Attr.Rdev)
	require.Zero(t, featureNode.Attr.Rdev)
	require.Equal(t, meta.DefaultDirSize, dir.Attr.Size)
	require.Equal(t, uint64(6), sym.Attr.Size)

	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 13, Inodes: 9}, quota.Usage())
	for _, check := range []struct {
		branch  string
		stat    pebblekv.BranchStat
		entries map[string]*meta.Inode
	}{
		{"main", pebblekv.BranchStat{Inodes: 5, LogicalBytes: 6, NextWriteSeq: 6}, map[string]*meta.Inode{"file": linked, "node": node, "dir": dir, "sym": sym, "link": linked}},
		{"feature", pebblekv.BranchStat{Inodes: 5, LogicalBytes: 7, NextWriteSeq: 5}, map[string]*meta.Inode{"file": featureFile.Inode, "node": featureNode, "dir": featureDir, "sym": featureSym}},
	} {
		e.withBranch(t, check.branch, func(store *pebble.DB) {
			stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
			require.NoError(t, err)
			require.Equal(t, check.stat, stat)
			getPebbleValue(t, store, pebblekv.DirtyKey(1))
			parent := readPebbleCreateInode(t, store, 1)
			require.Equal(t, uint32(3), parent.Nlink)
			require.Equal(t, stat.NextWriteSeq-1, parent.WriteSeq)
			require.NotZero(t, parent.MtimeSec)
			require.NotZero(t, parent.CtimeSec)
			for name, expected := range check.entries {
				entry, err := pebblekv.DecodeEntry(getPebbleValue(t, store, pebblekv.EntryKey(1, []byte(name))))
				require.NoError(t, err)
				require.Equal(t, pebblekv.Entry{Ino: expected.Ino, Mode: expected.Attr.Mode}, entry)
				inode := readPebbleCreateInode(t, store, entry.Ino)
				require.Equal(t, expected.Attr.Mode, inode.Mode)
				require.Equal(t, expected.Attr.Nlink, inode.Nlink)
				seq := map[string]uint64{"file": 1, "node": 2, "dir": 3, "sym": 4, "link": 5}[name]
				if check.branch == "main" && name == "file" {
					seq = 5
				}
				require.Equal(t, seq, inode.WriteSeq)
				if name == "sym" {
					if check.branch == "main" {
						require.Equal(t, []byte("target"), inode.SymlinkTarget)
					} else {
						require.Equal(t, []byte("feature"), inode.SymlinkTarget)
					}
				}
				getPebbleValue(t, store, pebblekv.DirtyKey(entry.Ino))
			}
		})
	}
	restarted := meta.New(db.NewStore(e.pool), meta.WithStorageRoot(e.root), meta.WithPebbleManager(e.storage))
	recoveredQuota, err := restarted.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 13, Inodes: 9}, recoveredQuota.Usage())
	for _, table := range []string{"inodes", "entries", "file_handles"} {
		var exists bool
		require.NoError(t, e.pool.QueryRow(e.ctx, "SELECT to_regclass($1) IS NOT NULL", "public."+table).Scan(&exists))
		require.False(t, exists, table)
	}
}

func TestPebbleCreateConcurrentBranches(t *testing.T) {
	e := setupPebbleCreate(t, 1000)
	feature := "feature"
	require.NoError(t, e.storage.Fork(e.disk.DiskID, *e.disk.Branches[0].HeadCommitID, feature))
	_, err := e.pool.Exec(e.ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, e.disk.DiskID)
	require.NoError(t, err)
	featureSID := uint64(102)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featureSID, DiskID: e.disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	var wg sync.WaitGroup
	inos := make(chan uint64, 12)
	errs := make(chan error, 12)
	for i := range 12 {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sid := e.sid
			if i%2 != 0 {
				sid = featureSID
			}
			inode, err := e.mgr.Mknod(meta.WithSessionID(e.ctx, &sid), meta.MknodParams{ParentIno: 1, Name: []byte(fmt.Sprintf("f%d", i)), Mode: meta.S_IFREG | 0o600})
			if err != nil {
				errs <- err
				return
			}
			inos <- inode.Ino
		}(i)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	close(inos)
	var got []uint64
	for ino := range inos {
		got = append(got, ino)
	}
	sort.Slice(got, func(i, j int) bool { return got[i] < got[j] })
	require.Len(t, got, 12)
	for i, ino := range got {
		require.Equal(t, uint64(i+2), ino)
	}
}

func TestPebbleCreateValidationAndSessions(t *testing.T) {
	e := setupPebbleCreate(t, 10000)
	ctx := e.branchContext()
	file, err := e.mgr.Mknod(ctx, meta.MknodParams{ParentIno: 1, Name: []byte("file"), Mode: meta.S_IFREG | 0o644})
	require.NoError(t, err)
	sym, err := e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("sym"), Target: []byte("target")})
	require.NoError(t, err)
	dir, err := e.mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: 1, Name: []byte("dir"), Mode: 0o755})
	require.NoError(t, err)
	create := func(ctx context.Context, parent uint64, name string) error {
		_, err := e.mgr.Create(ctx, meta.CreateParams{ParentIno: parent, Name: []byte(name), Mode: 0o644})
		return err
	}
	mknod := func(ctx context.Context, parent uint64, name string) error {
		_, err := e.mgr.Mknod(ctx, meta.MknodParams{ParentIno: parent, Name: []byte(name), Mode: meta.S_IFREG | 0o644})
		return err
	}
	mkdir := func(ctx context.Context, parent uint64, name string) error {
		_, err := e.mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: parent, Name: []byte(name), Mode: 0o755})
		return err
	}
	symlink := func(ctx context.Context, parent uint64, name string) error {
		_, err := e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: parent, Name: []byte(name), Target: []byte("x")})
		return err
	}
	link := func(ctx context.Context, parent uint64, name string) error {
		_, err := e.mgr.Link(ctx, meta.LinkParams{Ino: file.Ino, NewParentIno: parent, NewName: []byte(name)})
		return err
	}
	ops := map[string]func(context.Context, uint64, string) error{"create": create, "mknod": mknod, "mkdir": mkdir, "symlink": symlink, "link": link}
	reader := uint64(102)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: reader, DiskID: e.disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	unknown := uint64(999)
	legacy := uint64(103)
	for name, op := range ops {
		t.Run(name, func(t *testing.T) {
			for _, entry := range []string{"file", ".", ".."} {
				require.ErrorIs(t, op(ctx, 1, entry), meta.ErrAlreadyExists)
			}
			require.ErrorIs(t, op(ctx, 9999, "new"), meta.ErrNotFound)
			require.ErrorIs(t, op(ctx, file.Ino, "new"), meta.ErrNotDir)
			require.ErrorIs(t, op(ctx, sym.Ino, "new"), meta.ErrNotDir)
			require.ErrorIs(t, op(meta.WithSessionID(e.ctx, &reader), 1, "new"), meta.ErrPerm)
			require.ErrorIs(t, op(meta.WithSessionID(e.ctx, &unknown), 1, "new"), meta.ErrBadFileHandle)
			require.ErrorIs(t, op(meta.WithSessionID(e.ctx, &legacy), 1, "new"), meta.ErrBadFileHandle)
			require.ErrorIs(t, op(ctx, 0, "new"), meta.ErrInvalidArgument)
			require.ErrorIs(t, op(ctx, 1, ""), meta.ErrInvalidArgument)
			require.ErrorIs(t, op(ctx, 1, "x/y"), meta.ErrInvalidArgument)
			require.ErrorIs(t, op(ctx, 1, "x\x00y"), meta.ErrInvalidArgument)
			require.ErrorIs(t, op(ctx, 1, strings.Repeat("x", meta.MaxFileNameBytes+1)), meta.ErrNameTooLong)
		})
	}
	_, err = e.mgr.Link(ctx, meta.LinkParams{Ino: dir.Ino, NewParentIno: 1, NewName: []byte("dirlink")})
	require.ErrorIs(t, err, meta.ErrPerm)
	_, err = e.mgr.Link(ctx, meta.LinkParams{Ino: 9999, NewParentIno: 1, NewName: []byte("missing")})
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.Link(ctx, meta.LinkParams{Ino: 0, NewParentIno: 1, NewName: []byte("invalid")})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("invalid"), Mode: meta.S_IFDIR})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("invalid"), Flags: meta.O_TMPFILE})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Create(ctx, meta.CreateParams{ParentIno: math.MaxUint64, Name: []byte("invalid")})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Mknod(ctx, meta.MknodParams{ParentIno: 1, Name: []byte("invalid"), Mode: meta.S_IFDIR})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	_, err = e.mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: 1, Name: []byte("invalid"), Mode: meta.S_IFREG})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	for _, target := range [][]byte{nil, []byte("x\x00y")} {
		_, err = e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("invalid"), Target: target})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
	_, err = e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("invalid"), Target: []byte(strings.Repeat("x", meta.MaxSymlinkTargetBytes+1))})
	require.ErrorIs(t, err, meta.ErrNameTooLong)
	changePebbleCreateInode(t, e, dir.Ino, func(inode *pebblekv.Inode) { inode.Nlink = 0 })
	for _, op := range ops {
		require.ErrorIs(t, op(ctx, dir.Ino, "new"), meta.ErrNotFound)
	}
	changePebbleCreateInode(t, e, file.Ino, func(inode *pebblekv.Inode) { inode.Nlink = 0 })
	require.ErrorIs(t, link(ctx, 1, "orphan"), meta.ErrNotFound)
	changePebbleCreateInode(t, e, file.Ino, func(inode *pebblekv.Inode) { inode.Nlink = meta.MaxNlink })
	require.ErrorIs(t, link(ctx, 1, "full"), meta.ErrTooManyLinks)
	changePebbleCreateInode(t, e, 1, func(inode *pebblekv.Inode) { inode.Nlink = meta.MaxNlink })
	require.ErrorIs(t, mkdir(ctx, 1, "full"), meta.ErrTooManyLinks)
	_, err = e.pool.Exec(e.ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() WHERE session_id = $1`, int64(e.sid))
	require.NoError(t, err)
	awaitSessionRejected(t, e.mgr, e.ctx, e.sid)
	for _, op := range ops {
		require.ErrorIs(t, op(ctx, 1, "expired"), meta.ErrBadFileHandle)
	}
	_, err = e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("expired"), SessionID: &e.sid})
	require.ErrorIs(t, err, meta.ErrBadFileHandle)
	_, err = e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: e.disk.RootIno, Name: []byte("legacy"), SessionID: &legacy})
	require.ErrorIs(t, err, meta.ErrBadFileHandle)
}

func TestPebbleCreateSetgidAndQuota(t *testing.T) {
	e := setupPebbleCreate(t, 5)
	ctx := e.branchContext()
	changePebbleCreateInode(t, e, 1, func(inode *pebblekv.Inode) { inode.Mode |= meta.S_ISGID; inode.GID = 2000 })
	file, err := e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("file"), Mode: 0o6777, Umask: 0o027, GID: 3})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), file.Inode.Attr.GID)
	require.Equal(t, meta.S_IFREG|uint32(0o6750), file.Inode.Attr.Mode)
	node, err := e.mgr.Mknod(ctx, meta.MknodParams{ParentIno: 1, Name: []byte("node"), Mode: meta.S_IFBLK | 0o666, Umask: 0o022, Rdev: 91})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), node.Attr.GID)
	require.Equal(t, uint32(91), node.Attr.Rdev)
	dir, err := e.mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: 1, Name: []byte("dir"), Mode: 0o777, Umask: 0o027})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), dir.Attr.GID)
	require.Equal(t, meta.S_IFDIR|meta.S_ISGID|uint32(0o750), dir.Attr.Mode)
	plain, err := e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("plain"), Mode: 0o644, GID: 3})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), plain.Inode.Attr.GID)
	require.Zero(t, plain.Inode.Attr.Mode&meta.S_ISGID)
	_, err = e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("too-large"), Target: []byte("123456")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	sym, err := e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("sym"), Target: []byte("12345")})
	require.NoError(t, err)
	require.Equal(t, uint32(2000), sym.Attr.GID)
	require.Equal(t, meta.S_IFLNK|uint32(0o777), sym.Attr.Mode)
	// 配额不足的符号链接已提前领号，编号按单调递增跳过该号。
	require.Equal(t, uint64(7), sym.Ino)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	before := quota.Usage()
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 5, Inodes: 6}, before)
	var statBefore pebblekv.BranchStat
	e.withBranch(t, "main", func(store *pebble.DB) {
		var err error
		statBefore, err = pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		_, _, err = store.Get(pebblekv.EntryKey(1, []byte("too-large")))
		require.ErrorIs(t, err, pebble.ErrNotFound)
	})
	_, err = e.mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("full-create")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	_, err = e.mgr.Mknod(ctx, meta.MknodParams{ParentIno: 1, Name: []byte("full-node"), Mode: meta.S_IFREG})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	_, err = e.mgr.Mkdir(ctx, meta.MkdirParams{ParentIno: 1, Name: []byte("full-dir")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	_, err = e.mgr.Symlink(ctx, meta.SymlinkParams{ParentIno: 1, Name: []byte("full-sym"), Target: []byte("x")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	_, err = e.mgr.Link(ctx, meta.LinkParams{Ino: file.Inode.Ino, NewParentIno: 1, NewName: []byte("full-link")})
	require.ErrorIs(t, err, meta.ErrNoSpace)
	require.Equal(t, before, quota.Usage())
	e.withBranch(t, "main", func(store *pebble.DB) {
		stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		require.Equal(t, statBefore, stat)
		require.Equal(t, uint32(1), readPebbleCreateInode(t, store, file.Inode.Ino).Nlink)
		for _, name := range []string{"full-create", "full-node", "full-dir", "full-sym", "full-link"} {
			_, _, err := store.Get(pebblekv.EntryKey(1, []byte(name)))
			require.ErrorIs(t, err, pebble.ErrNotFound)
		}
		_, _, err = store.Get(pebblekv.InodeKey(6))
		require.ErrorIs(t, err, pebble.ErrNotFound, "配额失败不得写入节点")
	})
}

func TestPebbleCreateConcurrentQuota(t *testing.T) {
	e := setupPebbleCreate(t, 10)
	feature := "feature"
	require.NoError(t, e.storage.Fork(e.disk.DiskID, *e.disk.Branches[0].HeadCommitID, feature))
	_, err := e.pool.Exec(e.ctx, `UPDATE disks SET branches = branches || jsonb_build_array(jsonb_build_object(
		'branch_id', gen_random_uuid(), 'name', 'feature', 'parent_branch', 'main',
		'fork_commit_id', NULL, 'head_commit_id', NULL, 'state', 'NORMAL', 'merge_state', NULL)) WHERE disk_id = $1`, e.disk.DiskID)
	require.NoError(t, err)
	featureSID := uint64(102)
	_, err = e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: featureSID, DiskID: e.disk.DiskID, BranchName: &feature})
	require.NoError(t, err)
	quota, err := e.mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, sid := range []uint64{e.sid, featureSID} {
		go func(sid uint64) {
			<-start
			_, err := e.mgr.Symlink(meta.WithSessionID(e.ctx, &sid), meta.SymlinkParams{ParentIno: 1, Name: []byte("race"), Target: []byte("123456")})
			errs <- err
		}(sid)
	}
	close(start)
	var success, rejected int
	for range 2 {
		err := <-errs
		if err == nil {
			success++
		} else {
			require.ErrorIs(t, err, meta.ErrNoSpace)
			rejected++
		}
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, rejected)
	require.Equal(t, meta.TreeQuotaUsage{Bytes: 6, Inodes: 2}, quota.Usage())
	var createdBranches int
	for _, name := range []string{"main", feature} {
		e.withBranch(t, name, func(store *pebble.DB) {
			stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
			require.NoError(t, err)
			if stat.Inodes == 2 {
				createdBranches++
				require.Equal(t, uint64(6), stat.LogicalBytes)
				getPebbleValue(t, store, pebblekv.EntryKey(1, []byte("race")))
			} else {
				require.Equal(t, pebblekv.BranchStat{Inodes: 1, NextWriteSeq: 1}, stat)
				_, _, err := store.Get(pebblekv.EntryKey(1, []byte("race")))
				require.ErrorIs(t, err, pebble.ErrNotFound)
			}
		})
	}
	require.Equal(t, 1, createdBranches)
}
