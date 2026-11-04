package meta_test

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// cacheQueryTracer 记录管理器实际发往 PostgreSQL 的全部语句。
type cacheQueryTracer struct {
	mu      sync.Mutex
	queries []string
}

func (tracer *cacheQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	tracer.queries = append(tracer.queries, data.SQL)
	return ctx
}

func (*cacheQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (tracer *cacheQueryTracer) take() []string {
	tracer.mu.Lock()
	defer tracer.mu.Unlock()
	queries := tracer.queries
	tracer.queries = nil
	return queries
}

type asyncFixture struct {
	ctx     context.Context
	tracer  *cacheQueryTracer
	mgr     meta.Manager
	storage *pebblekv.Manager
	disk    *meta.Disk
	sid     uint64
}

func newAsyncFixture(t *testing.T) *asyncFixture {
	t.Helper()
	tracer := &cacheQueryTracer{}
	_, observed, _ := setupIsolatedMetaTestDBWithTracer(t, "pebble_async", tracer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(observed), meta.WithStorageRoot(root), meta.WithSessionLeaseTTL(3*time.Minute), meta.WithPebbleManager(storage))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "异步测试", MaxBytes: 1 << 20})
	require.NoError(t, err)
	sid := uint64(9001)
	_, err = mgr.RenewBranchSession(ctx, meta.RenewBranchSessionParams{SessionID: sid, DiskID: disk.DiskID})
	require.NoError(t, err)
	return &asyncFixture{ctx: ctx, tracer: tracer, mgr: mgr, storage: storage, disk: disk, sid: sid}
}

// 会话与配额缓存命中后，常规数据面 RPC 不得再访问 PostgreSQL。
func TestDataPlaneRPCsHitCacheWithoutPostgresQueries(t *testing.T) {
	f := newAsyncFixture(t)
	file, err := f.mgr.Create(f.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("cached"), Mode: 0o644, SessionID: &f.sid})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = f.mgr.Write(f.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("hello"), SessionID: &f.sid})
	require.NoError(t, err)
	f.tracer.take()

	_, err = f.mgr.Read(f.ctx, &meta.ReadRequest{Ino: ino, Size: 5, SessionID: &f.sid})
	require.NoError(t, err)
	_, err = f.mgr.Write(f.ctx, &meta.WriteRequest{Ino: ino, Offset: 5, Data: []byte("!"), SessionID: &f.sid})
	require.NoError(t, err)
	_, err = f.mgr.Lookup(meta.WithSessionID(f.ctx, &f.sid), 1, []byte("cached"))
	require.NoError(t, err)
	_, err = f.mgr.GetAttr(meta.WithSessionID(f.ctx, &f.sid), ino)
	require.NoError(t, err)
	_, err = f.mgr.Create(f.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("second"), Mode: 0o644, SessionID: &f.sid})
	require.NoError(t, err)

	require.Empty(t, f.tracer.take(), "缓存命中时数据面不得查询 PostgreSQL")
}

// CloseSession 必须立即驱逐会话缓存，后续请求立刻被拒绝。
func TestSessionCacheRejectsClosedSessionImmediately(t *testing.T) {
	f := newAsyncFixture(t)
	file, err := f.mgr.Create(f.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("closed"), Mode: 0o644, SessionID: &f.sid})
	require.NoError(t, err)
	ino := file.Inode.Ino
	_, err = f.mgr.GetAttr(meta.WithSessionID(f.ctx, &f.sid), ino)
	require.NoError(t, err)

	require.NoError(t, f.mgr.CloseSession(f.ctx, f.sid))
	_, err = f.mgr.GetAttr(meta.WithSessionID(f.ctx, &f.sid), ino)
	require.ErrorIs(t, err, meta.ErrBadFileHandle)
	_, err = f.mgr.Write(f.ctx, &meta.WriteRequest{Ino: ino, Data: []byte("x"), SessionID: &f.sid})
	require.ErrorIs(t, err, meta.ErrBadFileHandle)
}

// 并发读与单写并发时必须始终看到自洽的 Inode 与 Blob 视图。
func TestConcurrentReadsNeverSeeTornInodeAndBlobView(t *testing.T) {
	f := newAsyncFixture(t)
	file, err := f.mgr.Create(f.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("race"), Mode: 0o644, SessionID: &f.sid})
	require.NoError(t, err)
	ino := file.Inode.Ino
	const (
		small = 64
		large = 96
	)
	patterns := map[uint64]byte{0: 0, small: byte(small), large: byte(large)}

	const rounds = 120
	var wg sync.WaitGroup
	errs := make(chan error, 17)
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for range rounds {
			zero := uint64(0)
			if _, err := f.mgr.SetAttr(f.ctx, ino, meta.SetAttrInput{Size: &zero, SessionID: &f.sid}); err != nil {
				errs <- err
				return
			}
			for _, length := range []uint64{small, large} {
				data := bytes.Repeat([]byte{byte(length)}, int(length))
				if _, err := f.mgr.Write(f.ctx, &meta.WriteRequest{Ino: ino, Offset: 0, Data: data, SessionID: &f.sid}); err != nil {
					errs <- err
					return
				}
			}
		}
	}()
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
				}
				got, err := f.mgr.Read(f.ctx, &meta.ReadRequest{Ino: ino, Size: large, SessionID: &f.sid})
				if err != nil {
					errs <- err
					return
				}
				if len(got.Data) == 0 {
					continue
				}
				want := patterns[uint64(len(got.Data))]
				for _, value := range got.Data {
					if value != want {
						errs <- errors.New("读取到撕裂的 Inode/Blob 视图")
						return
					}
				}
				if _, err := f.mgr.Lookup(meta.WithSessionID(f.ctx, &f.sid), 1, []byte("race")); err != nil {
					errs <- err
					return
				}
				if _, err := f.mgr.GetAttr(meta.WithSessionID(f.ctx, &f.sid), ino); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
}

// UpdateDisk 调整 max_bytes 时必须同步内存配额，写路径无需重新查询控制面。
func TestUpdateDiskRefreshesCachedTreeQuota(t *testing.T) {
	f := newAsyncFixture(t)
	quota, err := f.mgr.TreeQuota(f.ctx, f.disk.DiskID)
	require.NoError(t, err)
	maxBytes, _ := quota.Snapshot()
	require.Equal(t, uint64(1<<20), maxBytes)

	file, err := f.mgr.Create(f.ctx, meta.CreateParams{ParentIno: 1, Name: []byte("big"), Mode: 0o644, SessionID: &f.sid})
	require.NoError(t, err)
	_, err = f.mgr.UpdateDisk(f.ctx, meta.UpdateDiskParams{DiskID: f.disk.DiskID, MaxBytes: ptr(uint64(4 << 20))})
	require.NoError(t, err)
	maxBytes, _ = quota.Snapshot()
	require.Equal(t, uint64(4<<20), maxBytes)

	f.tracer.take()
	_, err = f.mgr.Write(f.ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Data: bytes.Repeat([]byte{0x7f}, 3<<20), SessionID: &f.sid})
	require.NoError(t, err)
	require.Empty(t, f.tracer.take(), "配额缓存命中时写路径不得查询 PostgreSQL")
}

type pauseRenewControlTracer struct {
	armed   atomic.Bool
	reached chan struct{}
	resume  chan struct{}
}

func (tracer *pauseRenewControlTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if strings.Contains(data.SQL, "-- name: RenewControlPlaneSession") && tracer.armed.CompareAndSwap(true, false) {
		close(tracer.reached)
		select {
		case <-tracer.resume:
		case <-ctx.Done():
		}
	}
	return ctx
}

func (*pauseRenewControlTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

// 续期心跳不得持有全局控制面锁，跨盘 UpdateDisk 必须能并行完成。
func TestRenewBranchSessionDoesNotBlockCrossDiskControlPlane(t *testing.T) {
	tracer := &pauseRenewControlTracer{reached: make(chan struct{}), resume: make(chan struct{})}
	released := false
	defer func() {
		if !released {
			close(tracer.resume)
		}
	}()
	_, observed, ctx := setupIsolatedMetaTestDBWithTracer(t, "renew_concurrency", tracer)
	root := t.TempDir()
	storage := pebblekv.NewManager(root)
	t.Cleanup(func() { _ = storage.Close() })
	mgr := meta.New(db.NewStore(observed), meta.WithStorageRoot(root), meta.WithPebbleManager(storage))
	first, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "first", MaxBytes: 1 << 20})
	require.NoError(t, err)
	second, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "second", MaxBytes: 1 << 20})
	require.NoError(t, err)
	sid := uint64(9101)
	params := meta.RenewBranchSessionParams{SessionID: sid, DiskID: first.DiskID}
	_, err = mgr.RenewBranchSession(ctx, params)
	require.NoError(t, err)

	tracer.armed.Store(true)
	completed := make(chan error, 1)
	go func() {
		_, err := mgr.RenewBranchSession(ctx, params)
		completed <- err
	}()
	select {
	case <-tracer.reached:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	updateCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	_, err = mgr.UpdateDisk(updateCtx, meta.UpdateDiskParams{DiskID: second.DiskID, MaxBytes: ptr(uint64(2 << 20))})
	require.NoError(t, err, "续期心跳期间跨盘控制面操作不得被阻塞")
	close(tracer.resume)
	released = true
	require.NoError(t, <-completed)
}
