package fusefs_test

import (
	"sync"
	"syscall"
	"testing"

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
	wg.Wait()
	require.Equal(t, uint64(401), <-reclaimed)
	require.Empty(t, reclaimed, "孤儿只能回收一次")
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
	require.Empty(t, reclaimed, "仍有句柄时不得回收孤儿")

	_, ok = table.Release(second)
	require.True(t, ok)
	require.Equal(t, uint64(201), <-reclaimed)

	_, ok = table.Release(second)
	require.False(t, ok)
	require.Empty(t, reclaimed, "孤儿只能回收一次")
}

func TestHandleTable标记孤儿时句柄已关闭(t *testing.T) {
	reclaimed := make(chan uint64, 1)
	table := fusefs.NewHandleTable(func(ino uint64) { reclaimed <- ino })
	fh, err := table.Open(202, syscall.O_RDONLY, false)
	require.NoError(t, err)
	_, ok := table.Release(fh)
	require.True(t, ok)
	table.MarkOrphan(202)
	require.Equal(t, uint64(202), <-reclaimed)
}

func TestHandleTable校验模式并保留读写权限(t *testing.T) {
	table := fusefs.NewHandleTable(nil)

	readOnly, err := table.Open(301, syscall.O_RDONLY, false)
	require.NoError(t, err)
	readHandle, ok := table.Get(readOnly)
	require.True(t, ok)
	require.Equal(t, uint32(syscall.O_RDONLY), readHandle.Flags)
	require.True(t, readHandle.CanRead())
	require.False(t, readHandle.CanWrite())

	writeOnly, err := table.Open(301, syscall.O_WRONLY|syscall.O_APPEND, false)
	require.NoError(t, err)
	writeHandle, ok := table.Get(writeOnly)
	require.True(t, ok)
	require.Equal(t, uint32(syscall.O_WRONLY|syscall.O_APPEND), writeHandle.Flags)
	require.False(t, writeHandle.CanRead())
	require.True(t, writeHandle.CanWrite())

	readWrite, err := table.Open(301, syscall.O_RDWR|syscall.O_EXCL, false)
	require.NoError(t, err)
	readWriteHandle, ok := table.Get(readWrite)
	require.True(t, ok)
	require.Equal(t, uint32(syscall.O_RDWR|syscall.O_EXCL), readWriteHandle.Flags)
	require.True(t, readWriteHandle.CanRead())
	require.True(t, readWriteHandle.CanWrite())

	_, err = table.Open(0, syscall.O_RDONLY, false)
	require.ErrorIs(t, err, syscall.EINVAL)
	_, err = table.Open(301, syscall.O_ACCMODE, false)
	require.ErrorIs(t, err, syscall.EINVAL)
	_, err = table.Open(302, syscall.O_WRONLY, true)
	require.ErrorIs(t, err, syscall.EISDIR)
	require.Zero(t, table.OpenCount(302))
}
