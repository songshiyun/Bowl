package fusefs

import (
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// Test并发请求不被全局锁串行化 验证 Write / Read / Lookup / GetAttr 的跨机 RPC
// 可以同时处于在途状态；任何包裹整个 RPC 的全局互斥锁都会让屏障超时。
func Test并发请求不被全局锁串行化(t *testing.T) {
	const callers = 4
	release := make(chan struct{})
	var arrived sync.WaitGroup
	arrived.Add(callers)
	go func() {
		arrived.Wait()
		close(release)
	}()
	barrier := func() {
		arrived.Done()
		select {
		case <-release:
		case <-time.After(2 * time.Second):
			t.Error("跨机请求被全局锁串行化，未能并发抵达")
		}
	}

	client := &loopClient{
		readFn: func(r *storagev1.ReadRequest) (*storagev1.ReadResponse, error) {
			barrier()
			return &storagev1.ReadResponse{Data: make([]byte, r.GetSize())}, nil
		},
		writeFn: func(r *storagev1.WriteRequest) (*storagev1.WriteResponse, error) {
			barrier()
			return &storagev1.WriteResponse{Size: uint32(len(r.GetData()))}, nil
		},
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			barrier()
			return &storagev1.LookupResponse{Inode: testInode(30, syscall.S_IFREG|0o644, 1), EntryTimeoutSeconds: 1, AttrTimeoutSeconds: 1}, nil
		},
		getAttrFn: func(r *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			barrier()
			return &storagev1.GetAttrResponse{Inode: testInode(r.GetIno(), syscall.S_IFREG|0o644, 1), AttrTimeoutSeconds: 1}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2, 9, syscall.O_RDWR, false))

	type outcome struct {
		name   string
		status fuse.Status
	}
	results := make(chan outcome, callers)
	go func() {
		_, status := fs.Read(nil, &fuse.ReadIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9, Size: 16}, nil)
		results <- outcome{"Read", status}
	}()
	go func() {
		_, status := fs.Write(nil, &fuse.WriteIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9}, []byte("x"))
		results <- outcome{"Write", status}
	}()
	go func() {
		status := fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "child", &fuse.EntryOut{})
		results <- outcome{"Lookup", status}
	}()
	go func() {
		status := fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 3}}, &fuse.AttrOut{})
		results <- outcome{"GetAttr", status}
	}()

	for i := 0; i < callers; i++ {
		select {
		case result := <-results:
			require.Equal(t, fuse.OK, result.status, result.name)
		case <-time.After(5 * time.Second):
			t.Fatal("并发请求未返回")
		}
	}
}

// TestUnlink与并发Release串行保护 验证按 Inode 粒度锁会让 Release 等待在途 Unlink
// 完成孤儿标记，保证最后一个句柄关闭时恰好触发一次物理回收。
func TestUnlink与并发Release串行保护(t *testing.T) {
	unlinkEntered := make(chan struct{})
	allowUnlink := make(chan struct{})
	releaseDone := make(chan struct{})
	var reclaims atomic.Int32

	client := &loopClient{
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			return &storagev1.LookupResponse{
				Inode: testInode(2, syscall.S_IFREG|0o644, 1), EntryTimeoutSeconds: 1, AttrTimeoutSeconds: 1,
			}, nil
		},
		unlinkFn: func(*storagev1.UnlinkRequest) (*storagev1.UnlinkResponse, error) {
			close(unlinkEntered)
			<-allowUnlink
			return &storagev1.UnlinkResponse{}, nil
		},
		reclaimFn: func(*storagev1.ReclaimOrphanRequest) (*storagev1.ReclaimOrphanResponse, error) {
			reclaims.Add(1)
			return &storagev1.ReclaimOrphanResponse{}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	require.Equal(t, fuse.OK, fs.PrepareTestHandle(2, 9, syscall.O_RDWR, false))

	unlinkDone := make(chan fuse.Status, 1)
	go func() {
		unlinkDone <- fs.Unlink(nil, &fuse.InHeader{NodeId: 1}, "f")
	}()
	select {
	case <-unlinkEntered:
	case <-time.After(time.Second):
		t.Fatal("Unlink 未发起 RPC")
	}

	go func() {
		fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 9})
		close(releaseDone)
	}()
	select {
	case <-releaseDone:
		t.Fatal("Release 未等待在途 Unlink 完成")
	case <-time.After(100 * time.Millisecond):
	}

	close(allowUnlink)
	select {
	case status := <-unlinkDone:
		require.Equal(t, fuse.OK, status)
	case <-time.After(time.Second):
		t.Fatal("Unlink 未结束")
	}
	select {
	case <-releaseDone:
	case <-time.After(time.Second):
		t.Fatal("Release 未结束")
	}
	require.Eventually(t, func() bool { return reclaims.Load() == 1 }, time.Second, 10*time.Millisecond)
}

// Test在途Lookup不覆盖新目录项 验证在途 Lookup 的 ENOENT 结果不会覆盖并发创建后的正向目录项。
func Test在途Lookup不覆盖新目录项(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	client := &loopClient{
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			close(entered)
			<-allow
			return nil, connect.NewError(connect.CodeNotFound, errors.New("不存在"))
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)

	done := make(chan fuse.Status, 1)
	go func() {
		done <- fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "f", &fuse.EntryOut{})
	}()
	<-entered
	// 模拟并发 Create 成功后写入的权威目录项。
	fs.cache.putEntry(1, "f", 42)
	fs.cache.putInode(42, testInode(42, syscall.S_IFREG|0o644, 1), 0, 0)
	close(allow)

	require.Equal(t, fuse.ENOENT, <-done)
	ino, ok := fs.cache.entry(1, "f")
	require.True(t, ok)
	require.Equal(t, uint64(42), ino)
}

// Test在途Lookup不复活已删除目录项 验证在途 Lookup 的正向结果不会覆盖并发 Unlink 后的负向目录项。
func Test在途Lookup不复活已删除目录项(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	client := &loopClient{
		lookupFn: func(*storagev1.LookupRequest) (*storagev1.LookupResponse, error) {
			close(entered)
			<-allow
			return &storagev1.LookupResponse{Inode: testInode(42, syscall.S_IFREG|0o644, 1)}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)

	done := make(chan fuse.Status, 1)
	go func() {
		done <- fs.Lookup(nil, &fuse.InHeader{NodeId: 1}, "f", &fuse.EntryOut{})
	}()
	<-entered
	// 模拟并发 Unlink 成功后的权威负向目录项。
	fs.cache.putEntry(1, "f", 0)
	close(allow)

	require.Equal(t, fuse.OK, <-done)
	ino, ok := fs.cache.entry(1, "f")
	require.True(t, ok)
	require.Zero(t, ino)
}

// Test在途GetAttr不覆盖新属性 验证在途 GetAttr 的旧属性不会覆盖并发写入后的新属性。
func Test在途GetAttr不覆盖新属性(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	stale := testInode(5, syscall.S_IFREG|0o644, 1)
	stale.Attr.Size = 10
	client := &loopClient{
		getAttrFn: func(*storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			close(entered)
			<-allow
			return &storagev1.GetAttrResponse{Inode: stale, AttrTimeoutSeconds: 1}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)

	done := make(chan fuse.Status, 1)
	go func() {
		done <- fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 5}}, &fuse.AttrOut{})
	}()
	<-entered
	fresh := testInode(5, syscall.S_IFREG|0o644, 1)
	fresh.Attr.Size = 100
	fs.cache.invalidateInode(5)
	fs.cache.putInode(5, fresh, 0, 0)
	close(allow)

	require.Equal(t, fuse.OK, <-done)
	cached, ok := fs.cache.inode(5)
	require.True(t, ok)
	require.Equal(t, uint64(100), cached.inode.GetAttr().GetSize())
}

// Test会话失效拦截在途GetAttr回填 验证缓存代数变化后，在途属性响应不会污染新会话缓存。
func Test会话失效拦截在途GetAttr回填(t *testing.T) {
	entered := make(chan struct{})
	allow := make(chan struct{})
	var once sync.Once

	client := &loopClient{
		getAttrFn: func(r *storagev1.GetAttrRequest) (*storagev1.GetAttrResponse, error) {
			once.Do(func() { close(entered) })
			<-allow
			return &storagev1.GetAttrResponse{Inode: testInode(r.GetIno(), syscall.S_IFREG|0o644, 1), AttrTimeoutSeconds: 1}, nil
		},
	}
	fs := NewRawFS(client, nil, Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)

	done := make(chan fuse.Status, 1)
	go func() {
		done <- fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 5}}, &fuse.AttrOut{})
	}()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("GetAttr 未发起 RPC")
	}

	fs.onSessionExpired(0)
	close(allow)

	select {
	case status := <-done:
		require.Equal(t, fuse.Status(syscall.ESTALE), status)
	case <-time.After(time.Second):
		t.Fatal("GetAttr 未结束")
	}
	_, cached := fs.cache.inode(5)
	require.False(t, cached, "过期会话的属性不得回填缓存")

	require.Equal(t, fuse.OK, fs.GetAttr(nil, &fuse.GetAttrIn{InHeader: fuse.InHeader{NodeId: 5}}, &fuse.AttrOut{}))
}
