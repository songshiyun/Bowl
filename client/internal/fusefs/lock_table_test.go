package fusefs_test

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestLockTable无符号极限区间合并与空句柄协议回退(t *testing.T) {
	table := fusefs.NewLockTable()
	t.Cleanup(table.Close)

	table.RegisterHandle(11, syscall.O_RDONLY)
	table.RegisterHandle(12, syscall.O_WRONLY)

	require.Equal(t, fuse.EBADF, table.SetLk(&fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 2},
		Fh:       11,
		Owner:    1,
		Lk:       fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_WRLCK)},
	}), "只读句柄申请写锁必须返回 EBADF")

	require.Equal(t, fuse.EBADF, table.SetLk(&fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 2},
		Fh:       12,
		Owner:    2,
		Lk:       fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_RDLCK)},
	}), "只写句柄申请读锁必须返回 EBADF")

	maxEnd := ^uint64(0)
	req := func(owner, start, end uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 2},
			Fh:       21,
			Owner:    owner,
			Lk:       fuse.FileLock{Start: start, End: end, Typ: typ, Pid: 10},
		}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)

	require.Equal(t, fuse.OK, table.SetLk(req(1, 0, maxEnd, read)))
	require.Equal(t, fuse.OK, table.SetLk(req(1, 20, maxEnd, write)))
	require.Equal(t, fuse.OK, table.SetLk(req(1, maxEnd-5, maxEnd, unlock)))
	require.Equal(t, fuse.FileLock{Start: maxEnd - 5, End: maxEnd, Typ: unlock}, table.GetLk(req(2, maxEnd-5, maxEnd, write)))

	require.Equal(t, fuse.OK, table.SetLk(req(1, 20, maxEnd, read)))
	require.Equal(t, fuse.OK, table.SetLk(req(1, maxEnd, maxEnd, read)), "End == ^uint64(0) 时重复邻接合并不得发生无符号溢出")
	require.Equal(t, fuse.FileLock{Start: 0, End: maxEnd, Typ: read, Pid: 10}, table.GetLk(req(2, 0, maxEnd, write)))

	flockFallback := func(owner uint64) *fuse.LkIn {
		return &fuse.LkIn{
			InHeader: fuse.InHeader{NodeId: 3},
			Fh:       0,
			Owner:    owner,
			LkFlags:  fuse.FUSE_LK_FLOCK,
			Lk:       fuse.FileLock{Start: 0, End: maxEnd, Typ: write},
		}
	}
	require.Equal(t, fuse.OK, table.SetLk(flockFallback(7)))
	require.Equal(t, fuse.EAGAIN, table.SetLk(flockFallback(8)))
	table.ReleaseFlockByFH(3, 0, 7)
	require.Equal(t, fuse.OK, table.SetLk(flockFallback(8)))
}
