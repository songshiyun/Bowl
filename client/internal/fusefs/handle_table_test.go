package fusefs_test

import (
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestHandleTable句柄计数与打开模式(t *testing.T) {
	table := fusefs.NewHandleTable(nil)
	file, err := table.Open(101, syscall.O_RDONLY, false)
	require.NoError(t, err)
	second, err := table.Open(101, syscall.O_WRONLY|syscall.O_APPEND, false)
	require.NoError(t, err)
	dir, err := table.Open(102, syscall.O_RDONLY|syscall.O_EXCL, true)
	require.NoError(t, err)
	require.NotEqual(t, file, second)
	require.NotEqual(t, second, dir)
	require.True(t, table.IsOpen(101))
	require.Equal(t, 2, table.OpenCount(101))

	handle, ok := table.Get(dir)
	require.True(t, ok)
	require.Equal(t, uint64(102), handle.Ino)
	require.Equal(t, uint32(syscall.O_RDONLY|syscall.O_EXCL), handle.Flags)
	require.True(t, handle.IsDir)

	_, ok = table.Release(file)
	require.True(t, ok)
	require.Equal(t, 1, table.OpenCount(101))
	_, ok = table.Release(file)
	require.False(t, ok)
	_, ok = table.Release(second)
	require.True(t, ok)
	require.False(t, table.IsOpen(101))
	require.Zero(t, table.OpenCount(101))
	require.True(t, table.IsOpen(102))
	_, ok = table.Release(dir)
	require.True(t, ok)
}

func TestHandleTable并发关闭与标记孤儿只回收一次(t *testing.T) {
	for i := 0; i < 100; i++ {
		reclaimed := make(chan uint64, 2)
		var table *fusefs.HandleTable
		table = fusefs.NewHandleTable(func(ino uint64) {
			_ = table.OpenCount(ino)
			reclaimed <- ino
		})
		fh, err := table.Open(401, syscall.O_RDONLY, false)
		require.NoError(t, err)
		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			table.Release(fh)
		}()
		go func() {
			defer wg.Done()
			<-start
			table.MarkOrphan(401)
		}()
		close(start)
		done := make(chan struct{})
		go func() {
			wg.Wait()
			close(done)
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("回收回调阻塞了句柄表")
		}
		select {
		case ino := <-reclaimed:
			require.Equal(t, uint64(401), ino)
		case <-time.After(time.Second):
			t.Fatal("并发关闭和标记后未回收孤儿")
		}
		select {
		case ino := <-reclaimed:
			t.Fatalf("重复回收节点 %d", ino)
		default:
		}
	}
}

func TestHandleTable孤儿在最后一个句柄关闭后回收(t *testing.T) {
	reclaimed := make(chan uint64, 2)
	table := fusefs.NewHandleTable(func(ino uint64) { reclaimed <- ino })
	first, err := table.Open(201, syscall.O_RDONLY, false)
	require.NoError(t, err)
	second, err := table.Open(201, syscall.O_RDWR, false)
	require.NoError(t, err)

	table.MarkOrphan(201)
	_, ok := table.Release(first)
	require.True(t, ok)
	select {
	case ino := <-reclaimed:
		t.Fatalf("仍有句柄时回收了节点 %d", ino)
	default:
	}

	_, ok = table.Release(second)
	require.True(t, ok)
	select {
	case ino := <-reclaimed:
		require.Equal(t, uint64(201), ino)
	case <-time.After(time.Second):
		t.Fatal("最后一个句柄关闭后未回收孤儿")
	}

	_, ok = table.Release(second)
	require.False(t, ok)
	select {
	case ino := <-reclaimed:
		t.Fatalf("重复回收节点 %d", ino)
	default:
	}
}

func TestHandleTable标记孤儿时句柄已关闭(t *testing.T) {
	reclaimed := make(chan uint64, 1)
	table := fusefs.NewHandleTable(func(ino uint64) { reclaimed <- ino })
	fh, err := table.Open(202, syscall.O_RDONLY, false)
	require.NoError(t, err)
	_, ok := table.Release(fh)
	require.True(t, ok)
	table.MarkOrphan(202)
	select {
	case ino := <-reclaimed:
		require.Equal(t, uint64(202), ino)
	case <-time.After(time.Second):
		t.Fatal("标记时没有句柄，未回收孤儿")
	}
}

func TestHandleTable校验模式并保留读写权限(t *testing.T) {
	table := fusefs.NewHandleTable(nil)
	for _, tc := range []struct {
		flags    uint32
		readable bool
		writable bool
	}{
		{syscall.O_RDONLY, true, false},
		{syscall.O_WRONLY | syscall.O_APPEND, false, true},
		{syscall.O_RDWR | syscall.O_EXCL, true, true},
	} {
		fh, err := table.Open(301, tc.flags, false)
		require.NoError(t, err)
		handle, ok := table.Get(fh)
		require.True(t, ok)
		require.Equal(t, tc.flags, handle.Flags)
		require.Equal(t, tc.readable, handle.CanRead())
		require.Equal(t, tc.writable, handle.CanWrite())
	}
	_, err := table.Open(0, syscall.O_RDONLY, false)
	require.ErrorIs(t, err, syscall.EINVAL)
	_, err = table.Open(301, syscall.O_ACCMODE, false)
	require.ErrorIs(t, err, syscall.EINVAL)
	_, err = table.Open(302, syscall.O_WRONLY, true)
	require.ErrorIs(t, err, syscall.EISDIR)
	require.Zero(t, table.OpenCount(302))
}
