package meta_test

import (
	"context"
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
