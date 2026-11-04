package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
	"github.com/starsrc/bowl/services/storage/internal/server"
)

type xattrQueryTracer struct {
	mu      sync.Mutex
	queries []string
}

func (tracer *xattrQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.queries = append(tracer.queries, data.SQL)
	return ctx
}

func (*xattrQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *xattrQueryTracer) take() []string {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	queries := tracer.queries
	tracer.queries = nil
	return queries
}

type pebbleXattrFixture struct {
	ctx     context.Context
	pool    *pgxpool.Pool
	traced  *pgxpool.Pool
	tracer  *xattrQueryTracer
	mgr     meta.Manager
	storage *pebblekv.Manager
	disk    *meta.Disk
	main    uint64
	feature uint64
	snap    uint64
}

func newPebbleXattrFixture(t *testing.T) *pebbleXattrFixture {
	t.Helper()
	tracer := &xattrQueryTracer{}
	pool, traced, _ := setupIsolatedMetaTestDBWithTracer(t, "pebble_xattr", tracer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { require.NoError(t, storage.Close()) })
	mgr := meta.New(db.NewStore(traced), meta.WithStorageRoot(root), meta.WithSessionLeaseTTL(3*time.Minute), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "扩展属性测试", MaxBytes: 1 << 20})
	require.NoError(t, err)
	main, feature, snap := uint64(601), uint64(602), uint64(603)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: main, DiskID: disk.DiskID})
	require.NoError(t, err)
	branch, err := storage.OpenBranch(disk.DiskID, "main")
	require.NoError(t, err)
	batch := branch.DB().NewBatch()
	for _, node := range []struct {
		ino    uint64
		mode   uint32
		nlink  uint32
		orphan bool
	}{
		{2, meta.S_IFREG, 1, false}, {3, meta.S_IFDIR, 2, false}, {4, meta.S_IFSOCK, 1, false},
		{5, meta.S_IFLNK, 1, false}, {6, meta.S_IFIFO, 1, false}, {7, meta.S_IFCHR, 1, false},
		{8, meta.S_IFBLK, 1, false}, {9, meta.S_IFREG, 0, true}, {10, meta.S_IFREG, 0, false},
		{11, meta.S_IFREG, 1, false}, {12, meta.S_IFREG, 1, false}, {13, meta.S_IFREG, 1, false},
	} {
		inode := pebblekv.Inode{Ino: node.ino, Mode: node.mode, Nlink: node.nlink, AtimeSec: 1, MtimeSec: 2, CtimeSec: 3}
		value, err := pebblekv.EncodeInode(inode)
		require.NoError(t, err)
		require.NoError(t, batch.Set(pebblekv.InodeKey(node.ino), value, nil))
		if node.orphan {
			require.NoError(t, batch.Set(pebblekv.OrphanKey(node.ino), []byte{1}, nil))
		}
	}
	require.NoError(t, batch.Set(pebblekv.XattrKey(2, []byte("user.shared")), []byte("快照"), nil))
	require.NoError(t, batch.Set(pebblekv.XattrKey(9, []byte("user.orphan")), []byte("孤儿"), nil))
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
	branchName := "feature"
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: feature, DiskID: disk.DiskID, BranchName: &branchName})
	require.NoError(t, err)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: snap, DiskID: disk.DiskID, ReadOnly: true, CommitID: &commit})
	require.NoError(t, err)
	return &pebbleXattrFixture{ctx: ctx, pool: pool, traced: traced, tracer: tracer, mgr: mgr, storage: storage, disk: disk, main: main, feature: feature, snap: snap}
}

func (f *pebbleXattrFixture) mutate(t *testing.T, name string, fn func(*pebble.DB)) {
	t.Helper()
	branch, err := f.storage.OpenBranch(f.disk.DiskID, name)
	require.NoError(t, err)
	fn(branch.DB())
	require.NoError(t, branch.Close())
}

func xattrOperations(mgr meta.Manager, ctx context.Context, ino uint64, name []byte) []error {
	setErr := mgr.SetXattr(ctx, meta.SetXattrParams{Ino: ino, Name: name})
	_, getErr := mgr.GetXattr(ctx, ino, name)
	_, listErr := mgr.ListXattr(ctx, ino)
	removeErr := mgr.RemoveXattr(ctx, ino, name)
	return []error{setErr, getErr, listErr, removeErr}
}

func TestPebbleXattrBranchesAndSQL(t *testing.T) {
	f := newPebbleXattrFixture(t)
	main := meta.WithSessionID(f.ctx, &f.main)
	feature := meta.WithSessionID(f.ctx, &f.feature)
	snapshot := meta.WithSessionID(f.ctx, &f.snap)
	name := []byte("user.shared")

	// 先确认观测器能捕获旧表检查，再单独记录扩展属性操作。
	f.tracer.take()
	var exists bool
	require.NoError(t, f.traced.QueryRow(f.ctx, `SELECT to_regclass('public.inode_xattrs') IS NOT NULL`).Scan(&exists))
	require.False(t, exists)
	require.Contains(t, strings.Join(f.tracer.take(), "\n"), "inode_xattrs")

	require.NoError(t, f.mgr.SetXattr(main, meta.SetXattrParams{Ino: 2, Name: name, Value: []byte("主分支"), Flags: meta.XattrReplace}))
	require.NoError(t, f.mgr.SetXattr(feature, meta.SetXattrParams{Ino: 2, Name: name, Value: []byte("子分支")}))
	for _, tc := range []struct {
		ctx   context.Context
		value []byte
	}{{main, []byte("主分支")}, {feature, []byte("子分支")}, {snapshot, []byte("快照")}} {
		value, err := f.mgr.GetXattr(tc.ctx, 2, name)
		require.NoError(t, err)
		require.Equal(t, tc.value, value)
		value[0] ^= 0xff
		value, err = f.mgr.GetXattr(tc.ctx, 2, name)
		require.NoError(t, err)
		require.Equal(t, tc.value, value)
		names, err := f.mgr.ListXattr(tc.ctx, 2)
		require.NoError(t, err)
		require.Equal(t, [][]byte{name}, names)
	}
	require.ErrorIs(t, f.mgr.SetXattr(snapshot, meta.SetXattrParams{Ino: 2, Name: name}), meta.ErrPerm)
	require.ErrorIs(t, f.mgr.RemoveXattr(snapshot, 2, name), meta.ErrPerm)
	require.NoError(t, f.mgr.RemoveXattr(main, 2, name))
	_, err := f.mgr.GetXattr(main, 2, name)
	require.ErrorIs(t, err, meta.ErrNoData)
	value, err := f.mgr.GetXattr(snapshot, 2, name)
	require.NoError(t, err)
	require.Equal(t, []byte("快照"), value)

	queries := strings.ToLower(strings.Join(f.tracer.take(), "\n"))
	require.NotContains(t, queries, "client_sessions", "会话缓存命中时数据面不得查询控制面")
	require.NotContains(t, queries, "inode_xattrs")
}

func TestPebbleXattrRPC(t *testing.T) {
	f := newPebbleXattrFixture(t)
	srv, err := server.New(server.Params{Config: config.ServerConfig{}, MetaManager: f.mgr})
	require.NoError(t, err)
	name := []byte("user.rpc")
	_, err = srv.SetXattr(f.ctx, connect.NewRequest(&storagev1.SetXattrRequest{Ino: 1, Name: name, Value: []byte("分支值"), SessionId: &f.main}))
	require.NoError(t, err)
	branch, err := srv.GetXattr(f.ctx, connect.NewRequest(&storagev1.GetXattrRequest{Ino: 1, Name: name, SessionId: &f.main}))
	require.NoError(t, err)
	require.Equal(t, []byte("分支值"), branch.Msg.Value)
	list, err := srv.ListXattr(f.ctx, connect.NewRequest(&storagev1.ListXattrRequest{Ino: 1, SessionId: &f.main}))
	require.NoError(t, err)
	require.Equal(t, [][]byte{name}, list.Msg.Names)
	_, err = srv.SetXattr(f.ctx, connect.NewRequest(&storagev1.SetXattrRequest{Ino: 1, Name: name, SessionId: &f.snap}))
	require.Error(t, err)
	require.Equal(t, "EPERM", server.ExtractErrorInfo(err).GetReason())
	_, err = srv.RemoveXattr(f.ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{Ino: 1, Name: name, SessionId: &f.main}))
	require.NoError(t, err)

}

func TestPebbleXattrSemantics(t *testing.T) {
	f := newPebbleXattrFixture(t)
	ctx := meta.WithSessionID(f.ctx, &f.main)
	snapshot := meta.WithSessionID(f.ctx, &f.snap)
	name := []byte("user.flags")

	for _, flags := range []uint32{meta.XattrReplace, meta.XattrCreate | meta.XattrReplace} {
		require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: name, Flags: flags}), meta.ErrNoData)
	}
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: name, Flags: meta.XattrCreate}))
	value, err := f.mgr.GetXattr(ctx, 2, name)
	require.NoError(t, err)
	require.NotNil(t, value)
	require.Empty(t, value)
	for _, flags := range []uint32{meta.XattrCreate, meta.XattrCreate | meta.XattrReplace} {
		require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: name, Flags: flags}), meta.ErrAlreadyExists)
	}
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: name, Value: []byte{0, 0xff}, Flags: meta.XattrReplace}))
	value, err = f.mgr.GetXattr(ctx, 2, name)
	require.NoError(t, err)
	require.Equal(t, []byte{0, 0xff}, value)
	require.NoError(t, f.mgr.RemoveXattr(ctx, 2, name))
	require.ErrorIs(t, f.mgr.RemoveXattr(ctx, 2, name), meta.ErrNoData)

	for _, ino := range []uint64{2, 3, 4} {
		require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: ino, Name: name, Value: []byte("允许")}))
		value, err := f.mgr.GetXattr(ctx, ino, name)
		require.NoError(t, err)
		require.Equal(t, []byte("允许"), value)
		require.NoError(t, f.mgr.RemoveXattr(ctx, ino, name))
	}
	for _, ino := range []uint64{5, 6, 7, 8} {
		f.mutate(t, "main", func(store *pebble.DB) {
			require.NoError(t, store.Set(pebblekv.XattrKey(ino, name), []byte("隐藏"), pebble.Sync))
		})
		require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: ino, Name: name}), meta.ErrPerm)
		_, err := f.mgr.GetXattr(ctx, ino, name)
		require.ErrorIs(t, err, meta.ErrNoData)
		require.ErrorIs(t, f.mgr.RemoveXattr(ctx, ino, name), meta.ErrPerm)
		names, err := f.mgr.ListXattr(ctx, ino)
		require.NoError(t, err)
		require.Equal(t, [][]byte{name}, names)
		other := []byte("trusted.test")
		require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: ino, Name: other, Value: []byte("值")}))
		value, err := f.mgr.GetXattr(ctx, ino, other)
		require.NoError(t, err)
		require.Equal(t, []byte("值"), value)
		require.NoError(t, f.mgr.RemoveXattr(ctx, ino, other))
	}

	for _, ino := range []uint64{10, 9999} {
		for _, err := range xattrOperations(f.mgr, ctx, ino, name) {
			require.ErrorIs(t, err, meta.ErrNotFound)
		}
	}
	value, err = f.mgr.GetXattr(snapshot, 9, []byte("user.orphan"))
	require.ErrorIs(t, err, meta.ErrNotFound)
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 9, Name: name, Value: []byte("孤儿")}))
	value, err = f.mgr.GetXattr(ctx, 9, name)
	require.NoError(t, err)
	require.Equal(t, []byte("孤儿"), value)
	names, err := f.mgr.ListXattr(ctx, 9)
	require.NoError(t, err)
	require.Equal(t, [][]byte{name, []byte("user.orphan")}, names)
	require.NoError(t, f.mgr.RemoveXattr(ctx, 9, name))

	ordered := [][]byte{{1}, {0x7f}, {0x80}, {0xff}}
	for i := len(ordered) - 1; i >= 0; i-- {
		require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 11, Name: ordered[i]}))
	}
	names, err = f.mgr.ListXattr(ctx, 11)
	require.NoError(t, err)
	require.Equal(t, ordered, names)
	names, err = f.mgr.ListXattr(ctx, 12)
	require.NoError(t, err)
	require.NotNil(t, names)
	require.Empty(t, names)
}

func TestPebbleXattrValidationAndSessions(t *testing.T) {
	f := newPebbleXattrFixture(t)
	missing := uint64(999999)
	ctx := meta.WithSessionID(f.ctx, &missing)
	f.tracer.take()
	for _, ino := range []uint64{0, math.MaxUint64} {
		for _, err := range xattrOperations(f.mgr, ctx, ino, []byte("user.x")) {
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		}
	}
	for _, tc := range []struct {
		name []byte
		err  error
	}{{nil, meta.ErrInvalidArgument}, {[]byte{'x', 0}, meta.ErrInvalidArgument}, {bytes.Repeat([]byte{'x'}, 256), meta.ErrRange}} {
		require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: tc.name}), tc.err)
		_, err := f.mgr.GetXattr(ctx, 2, tc.name)
		require.ErrorIs(t, err, tc.err)
		require.ErrorIs(t, f.mgr.RemoveXattr(ctx, 2, tc.name), tc.err)
	}
	require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: []byte("x"), Flags: 4}), meta.ErrInvalidArgument)
	require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: []byte("x"), Value: make([]byte, meta.MaxXattrValueBytes+1)}), meta.ErrRange)
	require.Empty(t, f.tracer.take(), "静态参数校验不能查询数据库")

	_, err := f.pool.Exec(f.ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, f.feature)
	require.NoError(t, err)
	awaitSessionRejected(t, f.mgr, f.ctx, f.feature)
	zero, tooLarge := uint64(0), uint64(math.MaxInt64)+1
	for _, tc := range []struct {
		sid uint64
		err error
	}{{missing, meta.ErrBadFileHandle}, {f.feature, meta.ErrBadFileHandle}, {zero, meta.ErrInvalidArgument}, {tooLarge, meta.ErrInvalidArgument}} {
		ctx := meta.WithSessionID(f.ctx, &tc.sid)
		for _, err := range xattrOperations(f.mgr, ctx, 2, []byte("user.x")) {
			require.ErrorIs(t, err, tc.err)
		}
	}
}

func TestPebbleXattrQuotaAndConcurrency(t *testing.T) {
	f := newPebbleXattrFixture(t)
	ctx := meta.WithSessionID(f.ctx, &f.main)
	makeName := func(prefix string, length int) []byte {
		return []byte(prefix + strings.Repeat("x", length-len(prefix)))
	}
	f.mutate(t, "main", func(store *pebble.DB) {
		batch := store.NewBatch()
		for i := range 255 {
			name := makeName(fmt.Sprintf("user.%03d.", i), 255)
			require.NoError(t, batch.Set(pebblekv.XattrKey(11, name), []byte("值"), nil))
			require.NoError(t, batch.Set(pebblekv.XattrKey(12, name), []byte("值"), nil))
		}
		require.NoError(t, batch.Commit(pebble.Sync))
		require.NoError(t, batch.Close())
	})
	// 剩余 256 字节，两次并发创建只能有一次成功。
	start := make(chan struct{})
	errs := make(chan error, 2)
	for _, prefix := range []string{"user.last-a.", "user.last-b."} {
		go func(prefix string) {
			<-start
			errs <- f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 11, Name: makeName(prefix, 255), Flags: meta.XattrCreate})
		}(prefix)
	}
	close(start)
	var success, full int
	for range 2 {
		err := <-errs
		switch {
		case err == nil:
			success++
		case errors.Is(err, meta.ErrNoSpace):
			full++
		default:
			t.Fatalf("并发创建失败: %v", err)
		}
	}
	require.Equal(t, 1, success)
	require.Equal(t, 1, full)
	names, err := f.mgr.ListXattr(ctx, 11)
	require.NoError(t, err)
	require.Len(t, names, 256)
	total := 0
	for _, name := range names {
		total += len(name) + 1
	}
	require.Equal(t, meta.MaxXattrListBytes, total)
	require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 11, Name: []byte("a")}), meta.ErrNoSpace)
	// 多个最大值不计入名称列表，满额后仍可覆盖已有值。
	large := bytes.Repeat([]byte{'v'}, meta.MaxXattrValueBytes)
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 11, Name: names[0], Value: large, Flags: meta.XattrReplace}))
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 11, Name: names[1], Value: large, Flags: meta.XattrReplace}))
	require.NoError(t, f.mgr.RemoveXattr(ctx, 11, names[0]))
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 11, Name: []byte("a")}))

	// 剩余 1 字节不足以容纳单字节名称与终止符。
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 12, Name: makeName("user.short.", 254)}))
	require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 12, Name: []byte("a")}), meta.ErrNoSpace)

	start = make(chan struct{})
	errs = make(chan error, 8)
	for range 8 {
		go func() {
			<-start
			errs <- f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 13, Name: []byte("user.race"), Flags: meta.XattrCreate})
		}()
	}
	close(start)
	success = 0
	for range 8 {
		err := <-errs
		if err == nil {
			success++
		} else {
			require.ErrorIs(t, err, meta.ErrAlreadyExists)
		}
	}
	require.Equal(t, 1, success)
}

func TestPebbleXattrInodeUpdates(t *testing.T) {
	f := newPebbleXattrFixture(t)
	ctx := meta.WithSessionID(f.ctx, &f.main)
	type state struct {
		inode pebblekv.Inode
		stat  pebblekv.BranchStat
		dirty bool
	}
	readState := func() state {
		var got state
		f.mutate(t, "main", func(store *pebble.DB) {
			got.inode = readPebbleCreateInode(t, store, 2)
			var err error
			got.stat, err = pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
			require.NoError(t, err)
			_, closer, err := store.Get(pebblekv.DirtyKey(2))
			if err == nil {
				got.dirty = true
				require.NoError(t, closer.Close())
			} else {
				require.ErrorIs(t, err, pebble.ErrNotFound)
			}
		})
		return got
	}
	clearDirty := func() {
		f.mutate(t, "main", func(store *pebble.DB) {
			require.NoError(t, store.Delete(pebblekv.DirtyKey(2), pebble.Sync))
		})
	}
	checkUpdate := func(before, after state) {
		require.True(t, after.dirty)
		require.Equal(t, before.stat.NextWriteSeq, after.inode.WriteSeq)
		wantStat := before.stat
		wantStat.NextWriteSeq++
		require.Equal(t, wantStat, after.stat)
		afterCtime := time.Unix(after.inode.CtimeSec, int64(after.inode.CtimeNsec))
		beforeCtime := time.Unix(before.inode.CtimeSec, int64(before.inode.CtimeNsec))
		require.False(t, afterCtime.Before(beforeCtime))
		wantInode := before.inode
		wantInode.CtimeSec, wantInode.CtimeNsec = after.inode.CtimeSec, after.inode.CtimeNsec
		wantInode.WriteSeq = before.stat.NextWriteSeq
		require.Equal(t, wantInode, after.inode)
	}

	clearDirty()
	before := readState()
	name := []byte("user.update")
	require.NoError(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: name, Value: []byte("值")}))
	afterSet := readState()
	checkUpdate(before, afterSet)
	require.Greater(t, afterSet.inode.CtimeSec, before.inode.CtimeSec)
	value, err := f.mgr.GetXattr(ctx, 2, name)
	require.NoError(t, err)
	require.Equal(t, []byte("值"), value)
	_, err = f.mgr.ListXattr(ctx, 2)
	require.NoError(t, err)
	require.Equal(t, afterSet, readState(), "读取不能修改节点状态")

	clearDirty()
	f.mutate(t, "main", func(store *pebble.DB) {
		inode := readPebbleCreateInode(t, store, 2)
		inode.CtimeSec, inode.CtimeNsec = 3, 0
		value, err := pebblekv.EncodeInode(inode)
		require.NoError(t, err)
		require.NoError(t, store.Set(pebblekv.InodeKey(2), value, pebble.Sync))
	})
	before = readState()
	require.NoError(t, f.mgr.RemoveXattr(ctx, 2, name))
	afterRemove := readState()
	checkUpdate(before, afterRemove)
	require.Greater(t, afterRemove.inode.CtimeSec, before.inode.CtimeSec)
	_, err = f.mgr.GetXattr(ctx, 2, name)
	require.ErrorIs(t, err, meta.ErrNoData)

	clearDirty()
	f.mutate(t, "main", func(store *pebble.DB) {
		stat, err := pebblekv.DecodeBranchStat(getPebbleValue(t, store, pebblekv.StatKey()))
		require.NoError(t, err)
		stat.NextWriteSeq = math.MaxUint64
		require.NoError(t, store.Set(pebblekv.StatKey(), pebblekv.EncodeBranchStat(stat), pebble.Sync))
	})
	before = readState()
	require.ErrorIs(t, f.mgr.SetXattr(ctx, meta.SetXattrParams{Ino: 2, Name: name}), meta.ErrNoSpace)
	require.ErrorIs(t, f.mgr.RemoveXattr(ctx, 2, []byte("user.shared")), meta.ErrNoSpace)
	require.Equal(t, before, readState())
	value, err = f.mgr.GetXattr(ctx, 2, []byte("user.shared"))
	require.NoError(t, err)
	require.Equal(t, []byte("快照"), value)
	_, err = f.mgr.GetXattr(ctx, 2, name)
	require.ErrorIs(t, err, meta.ErrNoData)
}
