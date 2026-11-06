package meta_test

import (
	"bytes"
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestManagerDelegatesSessionCacheToBranchWorkspaceGate 验证根管理器结构体不再直接持有裸 sessionCache / sessionCacheMu 字段，
// 而是收拢至 BranchWorkspaceGate 深模块管理。
func TestManagerDelegatesSessionCacheToBranchWorkspaceGate(t *testing.T) {
	mgr := meta.New(nil)
	rt := reflect.TypeOf(mgr).Elem()
	for _, forbidden := range []string{"sessionCache", "sessionCacheMu"} {
		_, found := rt.FieldByName(forbidden)
		require.Falsef(t, found, "manager 根结构体不得保留裸 %s 字段，必须收拢至 BranchWorkspaceGate", forbidden)
	}
}

// TestBranchWorkspaceGateRejectsSessionDriftWhileWaitingForBranchLock 验证当读快照、单键点查、Fsync 写会话及可写分支变更 4 类操作
// 在等待 Branch 锁期间遭遇 ClientSession 关闭、过期清扫或被重新创建替换（缓存漂移）时，
// BranchWorkspaceGate 的锁内二次校验 100% 拦截并返回 ErrBadFileHandle，绝不将请求落入底层 Pebble。
func TestBranchWorkspaceGateRejectsSessionDriftWhileWaitingForBranchLock(t *testing.T) {
	e := setupPebbleCreate(t, 4<<20)
	require.NoError(t, e.mgr.CloseSession(e.ctx, e.sid))

	fileSID := uint64(900)
	_, err := e.mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: fileSID, DiskID: e.disk.DiskID})
	require.NoError(t, err)
	created, err := e.mgr.Create(e.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("target.txt"), Mode: 0o644, SessionID: &fileSID})
	require.NoError(t, err)
	ino := created.Inode.Ino
	_, err = e.mgr.Write(e.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("initial"), SessionID: &fileSID})
	require.NoError(t, err)
	require.NoError(t, e.mgr.CloseSession(e.ctx, fileSID))

	locks := meta.NewBranchLockTable()
	gate := meta.NewBranchWorkspaceGate(db.NewStore(e.pool), e.storage, locks, nil, time.Minute)
	mgr := meta.New(
		db.NewStore(e.pool),
		meta.WithStorageRoot(e.root),
		meta.WithSessionLeaseTTL(time.Minute),
		meta.WithPebbleManager(e.storage),
		meta.WithBranchLockTable(locks),
		meta.WithBranchWorkspaceGate(gate),
	)

	type opCase struct {
		name string
		run  func(ctx context.Context, sid *uint64) error
	}
	ops := []opCase{
		{
			name: "SnapshotRead_Lookup",
			run: func(ctx context.Context, sid *uint64) error {
				_, err := mgr.Lookup(ctx, sid, 1, []byte("target.txt"))
				return err
			},
		},
		{
			name: "PointRead_GetAttr",
			run: func(ctx context.Context, sid *uint64) error {
				_, err := mgr.GetAttr(ctx, sid, ino)
				return err
			},
		},
		{
			name: "WriteSession_Fsync",
			run: func(ctx context.Context, sid *uint64) error {
				return mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, SessionID: sid})
			},
		},
		{
			name: "WritableBranch_Write",
			run: func(ctx context.Context, sid *uint64) error {
				_, err := mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Data: []byte("mutated"), SessionID: sid})
				return err
			},
		},
	}

	// 场景 1：等待 Branch 锁期间会话被 CloseSession 显式注销（真实控制面注销并驱逐缓存）
	for i, tc := range ops {
		t.Run("ClosedDuringLockWait/"+tc.name, func(t *testing.T) {
			sid := uint64(910 + i)
			_, err := mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
			require.NoError(t, err)
			require.True(t, gate.HasCachedSession(sid))

			entered := make(chan struct{})
			gate.SetBeforeBranchLockHook(func(hookSID uint64) {
				if hookSID == sid {
					close(entered)
				}
			})
			defer gate.SetBeforeBranchLockHook(nil)

			// 独占持有 main 分支写锁，使待测操作在通过锁前会话解析后阻塞在获取 Branch 读锁处。
			unlockBranch := locks.LockBranchWrite(e.disk.DiskID, "main")
			errCh := make(chan error, 1)
			go func() {
				errCh <- tc.run(e.ctx, &sid)
			}()

			<-entered
			// 并发发起真实 CloseSession：其控制面事务将写端租约失效并立即驱逐会话缓存，随后在孤儿清扫处等待分支写锁。
			closeErrCh := make(chan error, 1)
			go func() {
				closeErrCh <- mgr.CloseSession(e.ctx, sid)
			}()
			require.Eventually(t, func() bool {
				return !gate.HasCachedSession(sid)
			}, 2*time.Second, 5*time.Millisecond)
			unlockBranch()

			require.NoError(t, <-closeErrCh)
			require.ErrorIs(t, <-errCh, meta.ErrBadFileHandle, "%s 在等待分支锁期间会话被 CloseSession 注销必须被二次校验拦截", tc.name)
		})
	}

	// 场景 2：等待 Branch 锁期间同一 SessionID 过期并被重新创建（createdAt 发生缓存漂移）
	for i, tc := range ops {
		t.Run("RecreatedDriftDuringLockWait/"+tc.name, func(t *testing.T) {
			sid := uint64(930 + i)
			_, err := mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
			require.NoError(t, err)

			entered := make(chan struct{})
			gate.SetBeforeBranchLockHook(func(hookSID uint64) {
				if hookSID == sid {
					close(entered)
				}
			})
			defer gate.SetBeforeBranchLockHook(nil)

			unlockBranch := locks.LockBranchWrite(e.disk.DiskID, "main")
			errCh := make(chan error, 1)
			go func() {
				errCh <- tc.run(e.ctx, &sid)
			}()

			<-entered
			// 将会话在数据库中置为已过期，并并发发起过期会话清扫（通过 DeleteBranch/Merge 等入口触发 sweepExpiredBranchSessions），
			// 或直接等待清扫/新写端接管在锁释放时完成。
			// 此处测试在等待 Branch 锁期间同一 SessionID 过期并在控制面被重新创建（createdAt 发生缓存漂移）：
			_, err = e.pool.Exec(e.ctx, `UPDATE client_sessions SET created_at = created_at + interval '5 seconds', expires_at = clock_timestamp() + interval '1 minute' WHERE session_id = $1`, int64(sid))
			require.NoError(t, err)
			require.NoError(t, gate.ReloadSession(e.ctx, sid))
			unlockBranch()

			require.ErrorIs(t, <-errCh, meta.ErrBadFileHandle, "%s 在等待分支锁期间遭遇同 SessionID 重建漂移必须被 samePebbleSession 拦截", tc.name)
			require.NoError(t, mgr.CloseSession(e.ctx, sid))
		})
	}

	// 场景 3：等待 Branch 锁期间会话因租约到期被过期清扫器（sweepExpiredBranchSessions）自动回收驱逐
	for i, tc := range ops {
		t.Run("ExpiredSweptDuringLockWait/"+tc.name, func(t *testing.T) {
			sid := uint64(940 + i)
			_, err := mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: e.disk.DiskID})
			require.NoError(t, err)

			entered := make(chan struct{})
			gate.SetBeforeBranchLockHook(func(hookSID uint64) {
				if hookSID == sid {
					close(entered)
				}
			})
			defer gate.SetBeforeBranchLockHook(nil)

			// 阻塞在 main 分支写锁上，同时在后台推进会话过期并触发清扫（清扫删除无孤儿或等待写锁）：
			// 为使 sweepExpiredBranchSessions 能在不与 main 分支写锁死锁的情况下立即驱逐过期会话，
			// 将非根分支或无分支目录/或在 sweep 删除记录后立即驱逐缓存：
			unlockBranch := locks.LockBranchWrite(e.disk.DiskID, "main")
			errCh := make(chan error, 1)
			go func() {
				errCh <- tc.run(e.ctx, &sid)
			}()

			<-entered
			_, err = e.pool.Exec(e.ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '2 seconds' WHERE session_id = $1`, int64(sid))
			require.NoError(t, err)
			sweepDone := make(chan struct{})
			go func() {
				defer close(sweepDone)
				triggerSessionSweep(t, e.ctx, mgr, e.disk.DiskID)
			}()
			// 等待过期清扫事务驱逐内存会话缓存（清扫协程在驱逐缓存后进入 sweepBranchOrphans 等待分支写锁）
			require.Eventually(t, func() bool {
				return !gate.HasCachedSession(sid)
			}, 2*time.Second, 5*time.Millisecond)
			unlockBranch()
			<-sweepDone

			require.ErrorIs(t, <-errCh, meta.ErrBadFileHandle, "%s 在等待分支锁期间遭遇会话过期清扫必须被二次校验拦截", tc.name)
		})
	}

	// 场景 3：等待 Branch 锁期间目标分支被回档/删除或整盘被清理（InvalidateBranchSessions / InvalidateDiskSessions）
	t.Run("BranchAndDiskInvalidationEvictsCachedSessions", func(t *testing.T) {
		sidBranch := uint64(950)
		_, err := mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: sidBranch, DiskID: e.disk.DiskID})
		require.NoError(t, err)
		require.True(t, gate.HasCachedSession(sidBranch))
		gate.InvalidateBranchSessions(e.disk.DiskID, "other-branch")
		require.True(t, gate.HasCachedSession(sidBranch), "失效其他分支不得误伤当前分支会话缓存")
		gate.InvalidateBranchSessions(e.disk.DiskID, "main")
		require.False(t, gate.HasCachedSession(sidBranch), "InvalidateBranchSessions 必须驱逐目标分支的会话缓存")

		// 缓存驱逐后再次发起 GetAttr 会自动回源控制面重新加载并缓存
		_, err = mgr.GetAttr(e.ctx, &sidBranch, ino)
		require.NoError(t, err)
		require.True(t, gate.HasCachedSession(sidBranch))

		gate.InvalidateDiskSessions(e.disk.DiskID)
		require.False(t, gate.HasCachedSession(sidBranch), "InvalidateDiskSessions 必须驱逐目标磁盘下全部会话缓存")
		require.NoError(t, mgr.CloseSession(e.ctx, sidBranch))
	})

	// 验证被拦截的 WritableBranch_Write 绝未篡改文件原始数据。
	verifySID := uint64(999)
	_, err = mgr.RenewBranchSession(e.ctx, meta.RenewBranchSessionParams{SessionID: verifySID, DiskID: e.disk.DiskID, ReadOnly: true})
	require.NoError(t, err)
	readResp, err := mgr.Read(e.ctx, &meta.ReadRequest{Ino: ino, Size: 64, SessionID: &verifySID})
	require.NoError(t, err)
	require.True(t, bytes.Equal([]byte("initial"), readResp.Data), "被锁内二次校验拦截的写操作不得污染底层数据")
	require.NoError(t, mgr.CloseSession(e.ctx, verifySID))
}
