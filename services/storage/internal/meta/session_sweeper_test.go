package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// triggerSessionSweep 通过一次不存在的合并目标确定性地触发一轮过期会话清扫。
func triggerSessionSweep(t *testing.T, ctx context.Context, mgr meta.Manager, diskID string) {
	t.Helper()
	_, err := mgr.Merge(ctx, meta.MergeParams{DiskID: diskID, SourceBranch: "main", TargetBranch: "missing"})
	require.ErrorIs(t, err, meta.ErrNotFound)
}

// TestSessionSweeper 通过服务生命周期验证活跃写端保护和过期后的孤儿回收。
func TestSessionSweeper(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "session_sweeper")
	cfg := &config.MetaConfig{DataDirVal: t.TempDir(), SessionLeaseTTLVal: time.Minute, SessionSweepIntervalVal: time.Hour}
	store := db.NewStore(pool)
	var mgr meta.Manager
	app := fxtest.New(t, fx.Supply(fx.Annotate(store, fx.As(new(db.Store))), cfg), meta.Module, fx.Populate(&mgr), fx.NopLogger)
	app.RequireStart()
	defer app.RequireStop()

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "session-sweeper", MaxBytes: 1024})
	require.NoError(t, err)
	sid := uint64(1001)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: disk.DiskID})
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{ParentIno: 1, Name: []byte("orphan"), SessionID: &sid})
	require.NoError(t, err)
	require.NoError(t, mgr.Unlink(ctx, &sid, 1, []byte("orphan"), true))
	triggerSessionSweep(t, ctx, mgr, disk.DiskID)
	_, err = mgr.GetAttr(ctx, &sid, file.Inode.Ino)
	require.NoError(t, err, "活跃写端的孤儿节点不得被回收")

	reader := uint64(1003)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: reader, DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	branchRes, err := mgr.CreateBranch(ctx, meta.CreateBranchParams{DiskID: disk.DiskID, Name: "feature"})
	require.NoError(t, err)
	featureReader := uint64(1004)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: featureReader, DiskID: disk.DiskID, BranchName: &branchRes.Name, ReadOnly: true})
	require.NoError(t, err)
	require.ErrorIs(t, mgr.DeleteBranch(ctx, meta.DeleteBranchParams{DiskID: disk.DiskID, Name: branchRes.Name}), meta.ErrBusy, "存在活跃只读会话时禁止删除分支")
	require.NoError(t, mgr.CloseSession(ctx, featureReader))
	require.NoError(t, mgr.DeleteBranch(ctx, meta.DeleteBranchParams{DiskID: disk.DiskID, Name: branchRes.Name}))

	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, reader)
	require.NoError(t, err)
	triggerSessionSweep(t, ctx, mgr, disk.DiskID)
	_, err = mgr.GetAttr(ctx, &reader, file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrBadFileHandle, "清理后会话缓存必须立即失效")
	_, err = mgr.GetAttr(ctx, &sid, file.Inode.Ino)
	require.NoError(t, err, "清理只读会话不得影响活跃写端的孤儿节点")

	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, sid)
	require.NoError(t, err)
	triggerSessionSweep(t, ctx, mgr, disk.DiskID)
	newID := uint64(1002)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: newID, DiskID: disk.DiskID})
	require.NoError(t, err)
	_, err = mgr.GetAttr(ctx, &newID, file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrNotFound)
	require.NoError(t, mgr.CloseSession(ctx, newID))
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
}
