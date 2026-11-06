package fusefs

import (
	"syscall"
	"testing"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"
)

// PrepareTestFileHandle 通过公开的打开路径准备常规文件句柄，供读写与锁测试使用。
func (fs *RawFS) PrepareTestFileHandle(t *testing.T, node, fh uint64, flags uint32) {
	t.Helper()
	ino, status := fs.ToServerIno(node)
	require.Equal(t, fuse.OK, status)
	fs.cache.putInode(node, testInode(ino, syscall.S_IFREG|0o666, 1), 0, 0)
	fs.handles.mu.Lock()
	fs.handles.next = fh - 1
	fs.handles.mu.Unlock()
	var out fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: node}, Flags: flags}, &out))
	require.Equal(t, fh, out.Fh)
}
