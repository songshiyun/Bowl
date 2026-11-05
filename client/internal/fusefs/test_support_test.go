package fusefs

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
)

// PrepareTestHandle 通过公开的打开路径准备本地句柄，供读写与锁测试使用。
func (fs *RawFS) PrepareTestHandle(node, fh uint64, flags uint32, dir bool) fuse.Status {
	ino, status := fs.ToServerIno(node)
	if status != fuse.OK {
		return status
	}
	mode := uint32(syscall.S_IFREG | 0o666)
	if dir {
		mode = syscall.S_IFDIR | 0o777
	}
	fs.cache.putInode(node, testInode(ino, mode, 1), 0, 0)
	fs.handles.mu.Lock()
	fs.handles.next = fh - 1
	fs.handles.mu.Unlock()
	var out fuse.OpenOut
	if dir {
		status = fs.OpenDir(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: node}, Flags: flags}, &out)
	} else {
		status = fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: node}, Flags: flags}, &out)
	}
	if status == fuse.OK && out.Fh != fh {
		return fuse.EIO
	}
	return status
}
