package fusefs_test

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

func TestTmpFileManager根节点越权拦截与未知高位节点防护(t *testing.T) {
	const rootIno = uint64(100)
	mgr := fusefs.NewTmpFileManager(rootIno)

	srvRoot, st := mgr.ToServerIno(fuse.FUSE_ROOT_ID)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, rootIno, srvRoot)

	_, st = mgr.ToServerIno(rootIno)
	require.Equal(t, fuse.ENOENT, st, "非 1 的 RootIno 直接作为内核节点标识访问必须拒绝")
	require.Equal(t, uint64(fuse.FUSE_ROOT_ID), mgr.ToKernelIno(rootIno))
	require.Equal(t, uint64(200), mgr.ToKernelIno(200))

	_, st = mgr.ToServerIno((uint64(1) << 63) | 999)
	require.Equal(t, fuse.ENOENT, st)
}

func TestTmpFileManager异步Forget与句柄释放乱序双重归零状态机(t *testing.T) {
	mgr := fusefs.NewTmpFileManager(1)
	localIno, _ := mgr.Create(1, 0, 0, 0o600, syscall.O_RDWR)
	_, _, st := mgr.Write(localIno, 0, []byte("pure-memory-data"), false)
	require.Equal(t, fuse.OK, st)

	openHandles := map[uint64]int{localIno: 1}
	hasOpen := func(ino uint64) bool { return openHandles[ino] > 0 }

	require.False(t, mgr.OnForget(localIno, 1, hasOpen), "句柄仍打开时内核先行 Forget 不得提前回收")
	buf := make([]byte, 16)
	n, handled, st := mgr.Read(localIno, 0, buf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, []byte("pure-memory-data"), buf[:n])

	mgr.AddLookup(localIno)
	openHandles[localIno] = 0
	require.False(t, mgr.OnHandleReleased(localIno, false, hasOpen), "nlookup 未归零时句柄先行关闭不得提前回收")
	n, handled, st = mgr.Read(localIno, 0, buf)
	require.True(t, handled)
	require.Equal(t, fuse.OK, st)
	require.Equal(t, []byte("pure-memory-data"), buf[:n])

	require.True(t, mgr.OnForget(localIno, 1, hasOpen), "句柄与 nlookup 双重归零后必须立即回收")
	_, st = mgr.ToServerIno(localIno)
	require.Equal(t, fuse.ENOENT, st)

	clearedIno, _ := mgr.Create(1, 0, 0, 0o600, syscall.O_RDWR)
	mgr.Clear()
	_, st = mgr.ToServerIno(clearedIno)
	require.Equal(t, fuse.ENOENT, st)
}

func TestTmpFileManager无符号偏移溢出与非法预分配标志拦截(t *testing.T) {
	mgr := fusefs.NewTmpFileManager(1)
	ino, _ := mgr.Create(1, 100, 200, 0o644, syscall.O_RDWR)

	_, handled, st := mgr.Write(ino, uint64(1)<<63, []byte("a"), false)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EFBIG), st)

	setBig := &fuse.SetAttrIn{}
	setBig.NodeId = ino
	setBig.Valid = fuse.FATTR_SIZE
	setBig.Size = uint64(1) << 63
	_, handled, st = mgr.SetAttr(ino, setBig, nil, &fuse.Attr{})
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EFBIG), st)

	handled, st = mgr.Allocate(ino, uint64(1)<<63, 4096, 0)
	require.True(t, handled)
	require.Equal(t, fuse.EINVAL, st)

	handled, st = mgr.Allocate(ino, 0, 0, 0)
	require.True(t, handled)
	require.Equal(t, fuse.EINVAL, st)

	handled, st = mgr.Allocate(ino, 0, 4096, unix.FALLOC_FL_PUNCH_HOLE)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)

	handled, st = mgr.Allocate(ino, 0, 4096, unix.FALLOC_FL_ZERO_RANGE)
	require.True(t, handled)
	require.Equal(t, fuse.Status(syscall.EOPNOTSUPP), st)
}
