package fusefs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// 本地锁类型常量。
const (
	ProtoLockTypeRead   uint32 = 0 // F_RDLCK (共享读锁)
	ProtoLockTypeWrite  uint32 = 1 // F_WRLCK (排他写锁)
	ProtoLockTypeUnlock uint32 = 2 // F_UNLCK (释放锁)
)

// ToProtoLockType 将宿主机操作系统锁类型显式转换为本地锁类型。
// 宿主机 syscall.F_RDLCK -> 0，syscall.F_WRLCK -> 1，syscall.F_UNLCK -> 2；非法值返回 fuse.EINVAL。
func ToProtoLockType(hostType uint32) (uint32, fuse.Status) {
	switch hostType {
	case uint32(syscall.F_RDLCK):
		return ProtoLockTypeRead, fuse.OK
	case uint32(syscall.F_WRLCK):
		return ProtoLockTypeWrite, fuse.OK
	case uint32(syscall.F_UNLCK):
		return ProtoLockTypeUnlock, fuse.OK
	default:
		return 0, fuse.EINVAL
	}
}

// ToHostLockType 将本地锁类型显式转换为宿主机操作系统锁类型。
// 本地值 0 -> syscall.F_RDLCK，1 -> syscall.F_WRLCK，2 -> syscall.F_UNLCK；非法值返回 fuse.EINVAL。
func ToHostLockType(protoType uint32) (uint32, fuse.Status) {
	switch protoType {
	case ProtoLockTypeRead:
		return uint32(syscall.F_RDLCK), fuse.OK
	case ProtoLockTypeWrite:
		return uint32(syscall.F_WRLCK), fuse.OK
	case ProtoLockTypeUnlock:
		return uint32(syscall.F_UNLCK), fuse.OK
	default:
		return 0, fuse.EINVAL
	}
}

// NormalizeLockRange 校验 POSIX 区间，并将 flock 归一化为整个文件。
func NormalizeLockRange(lkFlags uint32, start, end uint64) (normStart, normEnd uint64, status fuse.Status) {
	if lkFlags&fuse.FUSE_LK_FLOCK != 0 {
		return 0, ^uint64(0), fuse.OK
	}
	if end < start {
		return 0, 0, fuse.EINVAL
	}
	return start, end, fuse.OK
}

// prepareLock 校验句柄和锁参数，并归一化锁区间。
func (fs *RawFS) prepareLock(input *fuse.LkIn, get bool) (fuse.LkIn, fuse.Status) {
	if _, status := fs.ToServerIno(input.NodeId); status != fuse.OK {
		return fuse.LkIn{}, status
	}
	if _, status := fs.checkedHandle(input.NodeId, input.Fh, false); status != fuse.OK {
		return fuse.LkIn{}, status
	}
	if _, status := ToProtoLockType(input.Lk.Typ); status != fuse.OK {
		return fuse.LkIn{}, status
	}
	if get && input.Lk.Typ == uint32(syscall.F_UNLCK) {
		return fuse.LkIn{}, fuse.EINVAL
	}
	start, end, status := NormalizeLockRange(input.LkFlags, input.Lk.Start, input.Lk.End)
	if status != fuse.OK {
		return fuse.LkIn{}, status
	}
	in := *input
	in.Lk.Start, in.Lk.End = start, end
	return in, fuse.OK
}

// GetLk 在本地查询冲突文件锁。
func (fs *RawFS) GetLk(_ <-chan struct{}, input *fuse.LkIn, out *fuse.LkOut) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	in, status := fs.prepareLock(input, true)
	if status != fuse.OK {
		return status
	}
	out.Lk = fs.locks.GetLk(&in)
	return fuse.OK
}

// SetLk 在本地非阻塞设置文件锁。
func (fs *RawFS) SetLk(_ <-chan struct{}, input *fuse.LkIn) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	in, status := fs.prepareLock(input, false)
	if status != fuse.OK {
		return status
	}
	return fs.locks.SetLk(&in)
}

// SetLkw 在本地阻塞设置文件锁。
func (fs *RawFS) SetLkw(cancel <-chan struct{}, input *fuse.LkIn) fuse.Status {
	fs.lifecycleMu.RLock()
	defer fs.lifecycleMu.RUnlock()
	in, status := fs.prepareLock(input, false)
	if status != fuse.OK {
		return status
	}
	return fs.locks.SetLkw(context.Background(), cancel, &in)
}
