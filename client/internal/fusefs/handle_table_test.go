package fusefs_test

import (
	"sync"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestHandleTable异常入参拦截与重复释放幂等(t *testing.T) {
	table := fusefs.NewHandleTable(nil)

	_, err := table.Open(0, syscall.O_RDONLY, false)
	require.ErrorIs(t, err, syscall.EINVAL)

	_, err = table.Open(301, syscall.O_ACCMODE, false)
	require.ErrorIs(t, err, syscall.EINVAL)

	_, err = table.Open(302, syscall.O_WRONLY, true)
	require.ErrorIs(t, err, syscall.EISDIR)

	_, err = table.Open(302, syscall.O_RDONLY|syscall.O_TRUNC, true)
	require.ErrorIs(t, err, syscall.EISDIR)
	require.Zero(t, table.OpenCount(302))

	first, err := table.Open(301, syscall.O_RDONLY, false)
	require.NoError(t, err)
	second, err := table.Open(301, syscall.O_RDWR, false)
	require.NoError(t, err)
	require.Equal(t, 2, table.OpenCount(301))

	_, ok := table.Release(first)
	require.True(t, ok)
	_, ok = table.Release(first)
	require.False(t, ok, "重复释放已关闭句柄不得扣减同节点其余活动句柄计数")
	require.Equal(t, 1, table.OpenCount(301))

	_, ok = table.Release(second)
	require.True(t, ok)
	require.Zero(t, table.OpenCount(301))
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
	require.Empty(t, reclaimed, "并发关闭与标记孤儿竞态下只能触发一次回收")
	require.False(t, table.IsOrphan(401))
}
