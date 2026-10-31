package meta_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

type flushSnapshotQueryKey struct{}

type flushSnapshotQueryCounter struct{ count atomic.Int64 }

func (c *flushSnapshotQueryCounter) TraceQueryStart(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryStartData) context.Context {
	if ctx.Value(flushSnapshotQueryKey{}) != nil {
		c.count.Add(1)
	}
	return ctx
}

func (*flushSnapshotQueryCounter) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

type flushSnapshotFixture struct {
	pool         *pgxpool.Pool
	ctx          context.Context
	store        db.Store
	mgr          meta.Manager
	root         int64
	ino          uint64
	fh           uint64
	session      uint64
	otherSession uint64
}

func newFlushSnapshotFixture(t *testing.T, pool *pgxpool.Pool, ctx context.Context, withSession bool) *flushSnapshotFixture {
	t.Helper()
	f := &flushSnapshotFixture{pool: pool, ctx: ctx, store: db.NewStore(pool), session: uint64(time.Now().UnixNano())}
	f.otherSession = f.session + 1
	f.mgr = meta.New(f.store)
	root, err := f.store.CreateInode(ctx, db.CreateInodeParams{Mode: int32(meta.S_IFDIR | 0o700), Nlink: 2})
	require.NoError(t, err)
	f.root = root.Ino
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := pool.Exec(cleanup, `DELETE FROM client_sessions WHERE session_id = ANY($1::bigint[])`, []int64{int64(f.session), int64(f.otherSession)})
		require.NoError(t, err)
		_, err = pool.Exec(cleanup, `DELETE FROM entries WHERE parent_ino = $1`, f.root)
		require.NoError(t, err)
		_, err = pool.Exec(cleanup, `DELETE FROM inodes WHERE ino = ANY($1::bigint[])`, []int64{int64(f.ino), f.root})
		require.NoError(t, err)
	})
	// 两个会话均在句柄之前建立，使跨会话反证不会被代际校验提前拦截。
	_, err = f.mgr.RenewSession(ctx, f.session)
	require.NoError(t, err)
	_, err = f.mgr.RenewSession(ctx, f.otherSession)
	require.NoError(t, err)
	params := meta.CreateParams{ParentIno: uint64(f.root), Name: []byte("snapshot"), Mode: meta.S_IFREG | 0o600, Flags: meta.O_RDWR}
	if withSession {
		params.SessionID = &f.session
	}
	created, err := f.mgr.Create(ctx, params)
	require.NoError(t, err)
	f.ino, f.fh = created.Inode.Ino, created.Fh
	return f
}

func TestFlush_PostgresSingleSnapshot(t *testing.T) {
	counter := &flushSnapshotQueryCounter{}
	pool, ctx := setupMetaTestDBWithTracer(t, counter)
	if pool == nil {
		return
	}
	f := newFlushSnapshotFixture(t, pool, ctx, false)
	owner := uint64(41)
	counted := context.WithValue(ctx, flushSnapshotQueryKey{}, true)
	require.NoError(t, f.mgr.Flush(counted, meta.FlushParams{Ino: f.ino, Fh: f.fh, LockOwner: &owner}))
	require.Equal(t, int64(1), counter.count.Load(), "无会话、无 POSIX 锁的 Flush 应使用一次真实 SQL 快照完成校验")
	var handles int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_handles WHERE fh = $1`, int64(f.fh)).Scan(&handles))
	require.Equal(t, 1, handles, "Flush 不得销毁持久化句柄")
}

func TestFlush_PostgresSnapshotErrorPrecedence(t *testing.T) {
	for _, scenario := range []string{"有效会话", "错误活跃会话", "遗漏会话", "错误句柄", "跨文件句柄", "目录优先于坏句柄", "缺失节点优先于坏句柄", "无会话句柄隔离", "取消上下文"} {
		t.Run(scenario, func(t *testing.T) {
			pool, ctx := setupMetaTestDB(t)
			if pool == nil {
				return
			}
			f := newFlushSnapshotFixture(t, pool, ctx, scenario != "无会话句柄隔离")
			owner := uint64(42)
			params := meta.FlushParams{Ino: f.ino, Fh: f.fh, LockOwner: &owner, SessionID: &f.session}
			runCtx := ctx
			var expected error
			switch scenario {
			case "错误活跃会话":
				params.SessionID = &f.otherSession
				expected = meta.ErrBadFileHandle
			case "遗漏会话":
				params.SessionID = nil
				expected = meta.ErrBadFileHandle
			case "错误句柄", "目录优先于坏句柄", "缺失节点优先于坏句柄":
				params.Fh += 1_000_000_000
				expected = meta.ErrBadFileHandle
				if scenario == "目录优先于坏句柄" {
					params.Ino, expected = uint64(f.root), meta.ErrIsDir
				} else if scenario == "缺失节点优先于坏句柄" {
					params.Ino, expected = 1<<62, meta.ErrNotFound
				}
			case "无会话句柄隔离":
				expected = meta.ErrBadFileHandle
			case "跨文件句柄":
				other := newFlushSnapshotFixture(t, pool, ctx, true)
				params.Ino = other.ino
				expected = meta.ErrBadFileHandle
			case "取消上下文":
				cancelled, cancel := context.WithCancel(ctx)
				cancel()
				runCtx, expected = cancelled, context.Canceled
			}
			err := f.mgr.Flush(runCtx, params)
			if expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, expected)
			}
			var handles int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_handles WHERE fh = $1`, int64(f.fh)).Scan(&handles))
			require.Equal(t, 1, handles, "有效会话的句柄不得被错误请求清理")
		})
	}
}

func TestFlush_PostgresSnapshotExpiredAndRecreatedSession(t *testing.T) {
	for _, expired := range []bool{true, false} {
		name := "重建代际"
		if expired {
			name = "过期会话清理"
		}
		t.Run(name, func(t *testing.T) {
			pool, ctx := setupMetaTestDB(t)
			if pool == nil {
				return
			}
			f := newFlushSnapshotFixture(t, pool, ctx, true)
			other := newFlushSnapshotFixture(t, pool, ctx, true)
			owner := uint64(43)
			var expiredLockIno uint64
			if expired {
				locked, err := f.mgr.Create(ctx, meta.CreateParams{ParentIno: uint64(f.root), Name: []byte("locked"), Mode: meta.S_IFREG | 0o600, Flags: meta.O_RDWR, SessionID: &f.session})
				require.NoError(t, err)
				expiredLockIno = locked.Inode.Ino
				t.Cleanup(func() {
					_, err := pool.Exec(ctx, `DELETE FROM entries WHERE parent_ino = $1 AND name = $2`, f.root, []byte("locked"))
					require.NoError(t, err)
					_, err = pool.Exec(ctx, `DELETE FROM inodes WHERE ino = $1`, int64(expiredLockIno))
					require.NoError(t, err)
				})
				// 目标节点无锁；同一过期会话在另一节点持锁，确保快路径清理断言不是空表假绿。
				_, err = f.store.CreateFileLock(ctx, db.CreateFileLockParams{SessionID: int64(f.session), Ino: int64(expiredLockIno), Fh: int64(locked.Fh), LockOwner: int64(owner), EndOffset: 100, LockType: int16(meta.F_WRLCK), Pid: 1})
				require.NoError(t, err)
				var locks int
				require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_locks WHERE ino = $1`, int64(expiredLockIno)).Scan(&locks))
				require.Equal(t, 1, locks)
				require.NoError(t, f.mgr.Unlink(ctx, uint64(f.root), []byte("snapshot")))
				_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1`, int64(f.session))
				require.NoError(t, err)
			} else {
				_, err := pool.Exec(ctx, `UPDATE client_sessions SET created_at = clock_timestamp() WHERE session_id = $1`, int64(f.session))
				require.NoError(t, err)
				var newer bool
				require.NoError(t, pool.QueryRow(ctx, `SELECT s.created_at > h.created_at FROM client_sessions s, file_handles h WHERE s.session_id = $1 AND h.fh = $2`, int64(f.session), int64(f.fh)).Scan(&newer))
				require.True(t, newer, "必须实际建立晚于旧句柄的会话代际")
			}
			require.ErrorIs(t, f.mgr.Flush(ctx, meta.FlushParams{Ino: f.ino, Fh: f.fh, LockOwner: &owner, SessionID: &f.session}), meta.ErrBadFileHandle)
			if expired {
				for _, query := range []string{`SELECT count(*) FROM file_handles WHERE fh = $1`, `SELECT count(*) FROM file_locks WHERE ino = $1`} {
					id := int64(f.ino)
					if query == `SELECT count(*) FROM file_handles WHERE fh = $1` {
						id = int64(f.fh)
					}
					var count int
					require.NoError(t, pool.QueryRow(ctx, query, id).Scan(&count))
					require.Zero(t, count, "过期句柄及其锁必须清理")
				}
				var expiredLocks int
				require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_locks WHERE ino = $1`, int64(expiredLockIno)).Scan(&expiredLocks))
				require.Zero(t, expiredLocks, "无锁目标的 Flush 仍应清理同一过期会话的其他节点锁")
				// ADR-0023 由显式续期承担孤儿回收，Flush 不承诺立即回收孤儿。
				_, err := other.mgr.RenewSession(ctx, other.session)
				require.NoError(t, err)
				var inodes int
				require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM inodes WHERE ino = $1`, int64(f.ino)).Scan(&inodes))
				require.Zero(t, inodes)
			}
			var otherHandles int
			require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_handles WHERE fh = $1`, int64(other.fh)).Scan(&otherHandles))
			require.Equal(t, 1, otherHandles)
			require.NoError(t, other.mgr.Flush(ctx, meta.FlushParams{Ino: other.ino, Fh: other.fh, LockOwner: &owner, SessionID: &other.session}))
		})
	}
}

func TestFlush_PostgresSnapshotFindsLocksAfterManagerRestart(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	f := newFlushSnapshotFixture(t, pool, ctx, true)
	owner := uint64(44)
	for _, flags := range []int32{0, int32(meta.FuseLkFlock)} {
		_, err := f.store.CreateFileLock(ctx, db.CreateFileLockParams{SessionID: int64(f.session), Ino: int64(f.ino), Fh: int64(f.fh), LockOwner: int64(owner), LkFlags: flags, EndOffset: 100, LockType: int16(meta.F_WRLCK), Pid: 1})
		require.NoError(t, err)
	}
	// 新管理器没有进程内 POSIX 活动标记，必须从持久化锁表决定回退。
	restarted := meta.New(f.store)
	require.NoError(t, restarted.Flush(ctx, meta.FlushParams{Ino: f.ino, Fh: f.fh, LockOwner: &owner, SessionID: &f.session}))
	for _, flags := range []int32{0, int32(meta.FuseLkFlock)} {
		var count int
		require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM file_locks WHERE ino = $1 AND lk_flags = $2`, int64(f.ino), flags).Scan(&count))
		want := 0
		if flags != 0 {
			want = 1
		}
		require.Equal(t, want, count)
	}
}
