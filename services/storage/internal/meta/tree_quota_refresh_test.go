package meta_test

import (
	"bytes"
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

type quotaRefreshTraceKey struct{}

type quotaRefreshTracer struct {
	enabled atomic.Bool
	ready   chan struct{}
	release chan struct{}
	once    sync.Once
}

func (tracer *quotaRefreshTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if tracer.enabled.Load() && strings.Contains(data.SQL, "SELECT ino, mode, size FROM inodes WHERE root_ino") {
		return context.WithValue(ctx, quotaRefreshTraceKey{}, true)
	}
	return ctx
}

func (tracer *quotaRefreshTracer) TraceQueryEnd(ctx context.Context, _ *pgx.Conn, _ pgx.TraceQueryEndData) {
	if ctx.Value(quotaRefreshTraceKey{}) == nil || !tracer.enabled.CompareAndSwap(true, false) {
		return
	}
	close(tracer.ready)
	select {
	case <-tracer.release:
	case <-ctx.Done():
	}
}

func TestTreeQuotaRefreshDoesNotLoseCommittedWrite(t *testing.T) {
	tracer := &quotaRefreshTracer{ready: make(chan struct{}), release: make(chan struct{})}
	_, observed, _ := setupIsolatedMetaTestDBWithTracer(t, "quota_refresh", tracer)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	t.Cleanup(func() { tracer.once.Do(func() { close(tracer.release) }) })
	mgr := meta.New(db.NewStore(observed), meta.WithStorageRoot(t.TempDir()), meta.WithBlockStore(meta.NewLocalFSBlockStore(t.TempDir())))
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: "并发配额刷新", MaxBytes: 5000})
	require.NoError(t, err)
	file, err := mgr.Create(ctx, meta.CreateParams{ParentIno: disk.RootIno, Name: []byte("file"), Mode: 0o644, Flags: meta.O_RDWR})
	require.NoError(t, err)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Fh: file.Fh, Data: bytes.Repeat([]byte{'a'}, 4000)})
	require.NoError(t, err)
	quota, err := mgr.TreeQuota(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(4000), quota.Usage().Bytes)

	tracer.enabled.Store(true)
	refreshDone := make(chan error, 1)
	go func() {
		_, err := mgr.TreeQuota(ctx, disk.DiskID)
		refreshDone <- err
	}()
	select {
	case <-tracer.ready:
	case <-ctx.Done():
		t.Fatal("配额刷新未读取旧状态")
	}
	writeDone := make(chan error, 1)
	go func() {
		_, err := mgr.Write(ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Fh: file.Fh, Offset: 4000, Data: bytes.Repeat([]byte{'b'}, 800)})
		writeDone <- err
	}()
	var writeErr error
	var wrote bool
	select {
	case writeErr = <-writeDone:
		wrote = true
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		t.Fatal("并发写入超时")
	}
	tracer.once.Do(func() { close(tracer.release) })
	select {
	case err := <-refreshDone:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("配额刷新超时")
	}
	if !wrote {
		select {
		case writeErr = <-writeDone:
		case <-ctx.Done():
			t.Fatal("并发写入超时")
		}
	}
	require.NoError(t, writeErr)
	require.Equal(t, uint64(4800), quota.Usage().Bytes)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: file.Inode.Ino, Fh: file.Fh, Offset: 4800, Data: bytes.Repeat([]byte{'c'}, 300)})
	require.ErrorIs(t, err, meta.ErrNoSpace)
}
