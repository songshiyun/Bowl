package meta_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

func versionSession(t *testing.T, e *pebbleCreateEnv, branch string, sid uint64, readOnly bool, commit *string) {
	t.Helper()
	_, err := e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID, BranchName: &branch, ReadOnly: readOnly, CommitID: commit})
	require.NoError(t, err)
}

func versionRead(t *testing.T, e *pebbleCreateEnv, sid, ino uint64, expected []byte) {
	t.Helper()
	got, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: uint32(len(expected) + 10), SessionID: &sid})
	require.NoError(t, err)
	require.Equal(t, expected, got.Data)
}

func TestVersionSnapshotForkAndCompaction(t *testing.T) {
	e := setupPebbleCreate(t, 1<<30)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("文件"), Mode: 0o644})
	require.NoError(t, err)
	original := bytes.Repeat([]byte("快照内容"), 4096)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: original, SessionID: &e.sid})
	require.NoError(t, err)
	first, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main", Message: "第一个版本"})
	require.NoError(t, err)
	require.Equal(t, e.disk.Branches[0].HeadCommitID, first.ParentCommitID)
	e.withBranch(t, "main", func(store *pebble.DB) {
		_, closer, err := store.Get(pebblekv.DirtyKey(file.Inode.Ino))
		require.ErrorIs(t, err, pebble.ErrNotFound)
		require.Nil(t, closer)
	})
	storage := pebblekv.NewManager(e.root)
	snapshot, err := storage.OpenCommit(e.disk.DiskID, first.CommitID)
	require.NoError(t, err)
	getPebbleValue(t, snapshot.DB(), pebblekv.DirtyKey(file.Inode.Ino))
	require.NoError(t, snapshot.Close())
	require.NoError(t, storage.Close())

	dev, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	require.Equal(t, &first.CommitID, dev.ForkCommitID)
	devSID := uint64(102)
	versionSession(t, e, "dev", devSID, false, nil)
	versionSession(t, e, "main", 103, true, &first.CommitID)
	versionRead(t, e, devSID, file.Inode.Ino, original)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("修改"), SessionID: &devSID})
	require.NoError(t, err)
	short := uint64(5)
	_, err = e.mgr.SetAttr(e.ctx, file.Inode.Ino, meta.SetAttrInput{Size: &short, SessionID: &devSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Unlink(meta.WithSessionID(e.ctx, &devSID), 1, []byte("文件")))
	e.withBranch(t, "dev", func(store *pebble.DB) {
		require.NoError(t, store.Flush())
		require.NoError(t, store.Compact(e.ctx, []byte("a"), []byte("z"), true))
	})
	versionRead(t, e, e.sid, file.Inode.Ino, original)
	versionRead(t, e, 103, file.Inode.Ino, original)
	branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
}

func TestVersionQuotaRollbackAndDelete(t *testing.T) {
	const size = uint64(1 << 30)
	e := setupPebbleCreate(t, 3*size)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("文件"), Mode: 0o644})
	require.NoError(t, err)
	require.NoError(t, e.mgr.Fallocate(e.ctx, meta.FallocateParams{Ino: file.Inode.Ino, Length: size, SessionID: &e.sid}))
	first, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("覆盖"), SessionID: &e.sid})
	require.NoError(t, err)
	b1, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "b1"})
	require.NoError(t, err)
	require.NotEqual(t, first.CommitID, *b1.HeadCommitID)
	b2, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "b2"})
	require.NoError(t, err)
	require.Equal(t, b1.HeadCommitID, b2.HeadCommitID)
	assertUsage := func(want uint64) {
		t.Helper()
		disk, err := e.mgr.GetDisk(e.ctx, e.disk.DiskID)
		require.NoError(t, err)
		require.Equal(t, want, disk.UsedBytes)
		stat, err := e.mgr.StatFS(e.branchContext(), 1)
		require.NoError(t, err)
		require.Equal(t, want, stat.UsedBytes)
	}
	assertUsage(size)
	versionSession(t, e, "b1", 102, false, nil)
	versionSession(t, e, "b2", 103, false, nil)
	expanded := size + 4096
	_, err = e.mgr.SetAttr(e.ctx, file.Inode.Ino, meta.SetAttrInput{Size: &expanded, SessionID: ptrVersionSID(102)})
	require.NoError(t, err)
	assertUsage(expanded)
	extra, err := e.mgr.Create(meta.WithSessionID(e.ctx, ptrVersionSID(102)), meta.CreateParams{ParentIno: 1, Name: []byte("新文件")})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: extra.Inode.Ino, Data: []byte("123456"), SessionID: ptrVersionSID(102)})
	require.NoError(t, err)
	assertUsage(expanded + 6)
	require.ErrorIs(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "b1", CommitID: first.CommitID}), meta.ErrBusy)
	require.ErrorIs(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "b1"}), meta.ErrBusy)
	require.ErrorIs(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "main"}), meta.ErrInvalidArgument)
	require.NoError(t, e.mgr.CloseSession(e.ctx, 102))
	require.NoError(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "b1", CommitID: first.CommitID}))
	assertUsage(size)
	versionSession(t, e, "b1", 104, false, nil)
	_, err = e.mgr.Lookup(meta.WithSessionID(e.ctx, ptrVersionSID(104)), 1, []byte("新文件"))
	require.ErrorIs(t, err, meta.ErrNotFound)
	restored, err := e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: file.Inode.Ino, Size: 6, SessionID: ptrVersionSID(104)})
	require.NoError(t, err)
	require.Equal(t, make([]byte, 6), restored.Data)
	require.NoError(t, e.mgr.CloseSession(e.ctx, 104))
	require.NoError(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "b1"}))
	_, err = os.Stat(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "b1"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = e.mgr.SetAttr(e.ctx, file.Inode.Ino, meta.SetAttrInput{Size: &expanded, SessionID: ptrVersionSID(103)})
	require.NoError(t, err)
	assertUsage(expanded)
	require.NoError(t, e.mgr.CloseSession(e.ctx, 103))
	require.NoError(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "b2"}))
	assertUsage(size)
}

func ptrVersionSID(id uint64) *uint64 { return &id }

func TestVersionValidationAndHistoricalCommit(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	first := *e.disk.Branches[0].HeadCommitID
	for _, name := range []string{"", ".", "..", "a/b", "a\x00b", "a\x1fb", "a\x7fb", strings.Repeat("a", 256)} {
		_, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: name})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
		_, err = e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: name})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	}
	_, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "历史", SourceCommitID: &first})
	require.NoError(t, err)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "历史"})
	require.ErrorIs(t, err, meta.ErrAlreadyExists)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "无效", SourceCommitID: ptrVersionString("00000000-0000-0000-0000-000000000000")})
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
	require.ErrorIs(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "历史", CommitID: "11111111-1111-7111-8111-111111111111"}), meta.ErrNotFound)
	other, err := e.mgr.CreateDisk(e.ctx, meta.CreateDiskParams{Name: "其他磁盘", MaxBytes: 1 << 20})
	require.NoError(t, err)
	foreign := other.Branches[0].HeadCommitID
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "跨磁盘", SourceCommitID: foreign})
	require.ErrorIs(t, err, meta.ErrNotFound)
	require.ErrorIs(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "历史", CommitID: *foreign}), meta.ErrNotFound)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "反\\斜杠"})
	require.NoError(t, err)
	versionSession(t, e, "反\\斜杠", 102, false, nil)
	require.NoError(t, e.mgr.CloseSession(e.ctx, 102))
	require.NoError(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "反\\斜杠"}))
	_, err = e.pool.Exec(e.ctx, `UPDATE disks SET branches = jsonb_set(branches, '{0,state}', '"MERGING"'::jsonb) WHERE disk_id = $1`, e.disk.DiskID)
	require.NoError(t, err)
	_, err = e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.ErrorIs(t, err, meta.ErrBusy)
}

func ptrVersionString(value string) *string { return &value }

func TestVersionOrphansAndReadOnlyGate(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("孤儿")})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("数据"), SessionID: &e.sid})
	require.NoError(t, err)
	require.NoError(t, e.mgr.SetXattr(e.branchContext(), meta.SetXattrParams{Ino: file.Inode.Ino, Name: []byte("user.key"), Value: []byte("值")}))
	require.NoError(t, e.mgr.Unlink(meta.WithOpenInClient(e.branchContext(), true), 1, []byte("孤儿")))
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	versionRead(t, e, e.sid, file.Inode.Ino, []byte("数据"))
	versionSession(t, e, "main", 102, true, &commit.CommitID)
	_, err = e.mgr.Read(e.ctx, &meta.ReadRequest{Ino: file.Inode.Ino, Size: 16, SessionID: ptrVersionSID(102)})
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.GetAttr(meta.WithSessionID(e.ctx, ptrVersionSID(102)), file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.GetXattr(meta.WithSessionID(e.ctx, ptrVersionSID(102)), file.Inode.Ino, []byte("user.key"))
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.ListXattr(meta.WithSessionID(e.ctx, ptrVersionSID(102)), file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrNotFound)
	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "child", SourceCommitID: &commit.CommitID})
	require.NoError(t, err)
	e.withBranch(t, "child", func(store *pebble.DB) {
		assertPebbleMissing(t, store, pebblekv.OrphanKey(file.Inode.Ino))
		assertPebbleMissing(t, store, pebblekv.InodeKey(file.Inode.Ino))
		assertPebbleMissing(t, store, pebblekv.DataKey(file.Inode.Ino, 0))
		assertPebbleMissing(t, store, pebblekv.XattrKey(file.Inode.Ino, []byte("user.key")))
		assertPebbleMissing(t, store, pebblekv.DirtyKey(file.Inode.Ino))
	})
	versionSession(t, e, "child", 103, true, &commit.CommitID)
	require.ErrorIs(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "child"}), meta.ErrBusy)
	require.NoError(t, e.mgr.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "child", CommitID: commit.CommitID}))
	require.NoError(t, e.mgr.CloseSession(e.ctx, 103))
	require.NoError(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "child"}))
}

var errVersionTransaction = errors.New("模拟事务回滚")

type rollbackVersionStore struct {
	db.Store
	pool *pgxpool.Pool
}

func (s *rollbackVersionStore) Pool() *pgxpool.Pool { return s.pool }

func (s *rollbackVersionStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.Store.ExecTx(ctx, func(q db.Querier) error {
		if err := fn(q); err != nil {
			return err
		}
		return errVersionTransaction
	})
}

func TestVersionTransactionFailureRestoresState(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	file, err := e.mgr.Create(e.branchContext(), meta.CreateParams{ParentIno: 1, Name: []byte("文件")})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("初始"), SessionID: &e.sid})
	require.NoError(t, err)
	commit, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("变更"), SessionID: &e.sid})
	require.NoError(t, err)
	failing := meta.New(&rollbackVersionStore{Store: db.NewStore(e.pool), pool: e.pool}, meta.WithStorageRoot(e.root))
	commitDir := filepath.Join(e.root, "disks", e.disk.DiskID, "commits")
	before, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	_, err = failing.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.ErrorIs(t, err, errVersionTransaction)
	_, err = failing.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "失败"})
	require.ErrorIs(t, err, errVersionTransaction)
	after, err := os.ReadDir(commitDir)
	require.NoError(t, err)
	require.Equal(t, len(before), len(after))
	_, err = os.Stat(filepath.Join(e.root, "disks", e.disk.DiskID, "branches", "失败"))
	require.ErrorIs(t, err, os.ErrNotExist)
	e.withBranch(t, "main", func(store *pebble.DB) { getPebbleValue(t, store, pebblekv.DirtyKey(file.Inode.Ino)) })
	branches, err := e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 1)
	require.Equal(t, &commit.CommitID, branches[0].HeadCommitID)

	_, err = e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	versionSession(t, e, "dev", 102, false, nil)
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: []byte("开发"), SessionID: ptrVersionSID(102)})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, 102))
	require.ErrorIs(t, failing.Rollback(e.ctx, meta.RollbackParams{DiskID: e.disk.DiskID, BranchName: "dev", CommitID: commit.CommitID}), errVersionTransaction)
	require.ErrorIs(t, failing.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: "dev"}), errVersionTransaction)
	versionSession(t, e, "dev", 103, false, nil)
	versionRead(t, e, 103, file.Inode.Ino, []byte("开发"))
	branches, err = e.mgr.ListBranches(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	require.Len(t, branches, 2)
}

func TestVersionForkIncludesReadlinkAtime(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	e.mgr = meta.New(db.NewStore(e.pool), meta.WithStorageRoot(e.root), meta.WithAtimeMode(meta.AtimeModeStrictAtime))
	link, err := e.mgr.Symlink(e.branchContext(), meta.SymlinkParams{ParentIno: 1, Name: []byte("链接"), Target: []byte("目标")})
	require.NoError(t, err)
	oldAtime := time.Unix(1, 0).UTC()
	_, err = e.mgr.SetAttr(e.ctx, link.Ino, meta.SetAttrInput{Atime: &oldAtime, SessionID: &e.sid})
	require.NoError(t, err)
	first, err := e.mgr.Commit(e.ctx, meta.CommitParams{DiskID: e.disk.DiskID, BranchName: "main"})
	require.NoError(t, err)
	_, err = e.mgr.Readlink(e.branchContext(), link.Ino)
	require.NoError(t, err)
	live, err := e.mgr.GetAttr(e.branchContext(), link.Ino)
	require.NoError(t, err)
	require.True(t, live.Attr.Atime.After(oldAtime))
	fork, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
	require.NoError(t, err)
	require.NotEqual(t, first.CommitID, *fork.HeadCommitID)
	versionSession(t, e, "dev", 102, false, nil)
	copied, err := e.mgr.GetAttr(meta.WithSessionID(e.ctx, ptrVersionSID(102)), link.Ino)
	require.NoError(t, err)
	require.Equal(t, live.Attr.Atime, copied.Attr.Atime)
}

func TestVersionConcurrentReadOnlySessionAndDelete(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	for i := range 4 {
		name := "并发分支" + string(rune('a'+i))
		_, err := e.mgr.CreateBranch(e.ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: name})
		require.NoError(t, err)
		sid := uint64(200 + i)
		start := make(chan struct{})
		sessionDone := make(chan error, 1)
		deleteDone := make(chan error, 1)
		go func() {
			<-start
			_, err := e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID, BranchName: &name, ReadOnly: true})
			sessionDone <- err
		}()
		go func() {
			<-start
			deleteDone <- e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: name})
		}()
		close(start)
		sessionErr, deleteErr := <-sessionDone, <-deleteDone
		if deleteErr == nil {
			require.ErrorIs(t, sessionErr, meta.ErrNotFound)
		} else {
			require.ErrorIs(t, deleteErr, meta.ErrBusy)
			require.NoError(t, sessionErr)
			require.NoError(t, e.mgr.CloseSession(e.ctx, sid))
			require.NoError(t, e.mgr.DeleteBranch(e.ctx, meta.DeleteBranchParams{DiskID: e.disk.DiskID, Name: name}))
		}
	}
}

type versionLockStore struct {
	db.Store
	pool            *pgxpool.Pool
	rowLocked       chan struct{}
	continueVersion chan struct{}
	updateStarted   chan struct{}
	versionOnce     sync.Once
	updateOnce      sync.Once
}

func (s *versionLockStore) Pool() *pgxpool.Pool { return s.pool }

func (s *versionLockStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.Store.ExecTx(ctx, func(q db.Querier) error { return fn(&versionLockQueries{Querier: q, store: s}) })
}

func (s *versionLockStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error { return fn(&versionLockQueries{Querier: q, store: s}) }, opts...)
}

type versionLockQueries struct {
	db.Querier
	store *versionLockStore
}

func (q *versionLockQueries) RawDBTX() db.DBTX {
	return q.Querier.(interface{ RawDBTX() db.DBTX }).RawDBTX()
}

func (q *versionLockQueries) GetDiskControlPlaneForUpdate(ctx context.Context, id pgtype.UUID) (db.GetDiskControlPlaneForUpdateRow, error) {
	row, err := q.Querier.GetDiskControlPlaneForUpdate(ctx, id)
	if err == nil {
		q.store.versionOnce.Do(func() {
			close(q.store.rowLocked)
			select {
			case <-q.store.continueVersion:
			case <-ctx.Done():
			}
		})
	}
	return row, err
}

func (q *versionLockQueries) GetDiskForUpdate(ctx context.Context, id pgtype.UUID) (db.GetDiskForUpdateRow, error) {
	q.store.updateOnce.Do(func() { close(q.store.updateStarted) })
	return q.Querier.GetDiskForUpdate(ctx, id)
}

func TestVersionConcurrentDiskUpdate(t *testing.T) {
	e := setupPebbleCreate(t, 1<<20)
	store := &versionLockStore{Store: db.NewStore(e.pool), pool: e.pool, rowLocked: make(chan struct{}), continueVersion: make(chan struct{}), updateStarted: make(chan struct{})}
	mgr := meta.New(store, meta.WithStorageRoot(e.root))
	_, err := mgr.TreeQuota(e.ctx, e.disk.DiskID)
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(e.ctx, 4*time.Second)
	defer cancel()
	versionDone := make(chan error, 1)
	updateDone := make(chan error, 1)
	go func() {
		_, err := mgr.CreateBranch(ctx, meta.CreateBranchParams{DiskID: e.disk.DiskID, Name: "dev"})
		versionDone <- err
	}()
	select {
	case <-store.rowLocked:
	case <-ctx.Done():
		t.Fatal("等待版本事务锁定磁盘超时")
	}
	go func() {
		name := "更新磁盘"
		_, err := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{DiskID: e.disk.DiskID, Name: &name})
		updateDone <- err
	}()
	select {
	case <-store.updateStarted:
	case <-time.After(300 * time.Millisecond):
	}
	close(store.continueVersion)
	require.NoError(t, <-versionDone)
	require.NoError(t, <-updateDone)
}
