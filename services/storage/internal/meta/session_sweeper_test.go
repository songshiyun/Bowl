package meta_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestSessionSweeper 通过服务生命周期验证活跃写端保护和过期后的孤儿回收。
func TestSessionSweeper(t *testing.T) {
	pool, ctx := setupIsolatedMetaTestDB(t, "session_sweeper")
	cfg := &config.MetaConfig{DataDirVal: t.TempDir(), SessionLeaseTTLVal: time.Minute, SessionSweepIntervalVal: 20 * time.Millisecond}
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
	branchCtx := meta.WithSessionID(ctx, &sid)
	file, err := mgr.Create(branchCtx, meta.CreateParams{ParentIno: 1, Name: []byte("orphan"), SessionID: &sid})
	require.NoError(t, err)
	require.NoError(t, mgr.Unlink(meta.WithOpenInClient(branchCtx, true), 1, []byte("orphan")))
	require.Never(t, func() bool {
		_, err := mgr.GetAttr(branchCtx, file.Inode.Ino)
		return err != nil
	}, 120*time.Millisecond, 20*time.Millisecond, "活跃写端的孤儿节点不得被回收")

	reader := uint64(1003)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: reader, DiskID: disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, reader)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var count int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id = $1`, reader).Scan(&count)
		return err == nil && count == 0
	}, 3*time.Second, 20*time.Millisecond, "清理器应注销过期只读会话")
	_, err = mgr.GetAttr(branchCtx, file.Inode.Ino)
	require.NoError(t, err, "清理只读会话不得影响活跃写端的孤儿节点")

	_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, sid)
	require.NoError(t, err)
	require.Eventually(t, func() bool {
		var count int
		err := pool.QueryRow(ctx, `SELECT count(*) FROM client_sessions WHERE session_id = $1`, sid).Scan(&count)
		return err == nil && count == 0
	}, 3*time.Second, 20*time.Millisecond, "清理器应注销过期写端")
	newID := uint64(1002)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: newID, DiskID: disk.DiskID})
	require.NoError(t, err)
	_, err = mgr.GetAttr(meta.WithSessionID(ctx, &newID), file.Inode.Ino)
	require.ErrorIs(t, err, meta.ErrNotFound)
	require.NoError(t, mgr.CloseSession(ctx, newID))
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))
}
