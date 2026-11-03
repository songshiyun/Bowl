package fusefs_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func waitForQueuedWriter(t *testing.T, tryRead, unlock func() fuse.Status) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := tryRead()
		if status == fuse.OK {
			require.Equal(t, fuse.OK, unlock())
		}
		return status == fuse.EAGAIN
	}, time.Second, time.Millisecond, "等待中的写锁应阻止新读锁")
}

func TestLocalPOSIXLockConflictsWithoutRPC(t *testing.T) {
	var calls atomic.Int32
	server := &lockMockStorageServer{
		getLkFn: func(context.Context, *connect.Request[storagev1.GetLkRequest]) (*connect.Response[storagev1.GetLkResponse], error) {
			calls.Add(1)
			return nil, errors.New("不应调用 GetLk RPC")
		},
		setLkFn: func(context.Context, *connect.Request[storagev1.SetLkRequest]) (*connect.Response[storagev1.SetLkResponse], error) {
			calls.Add(1)
			return nil, errors.New("不应调用 SetLk RPC")
		},
		setLkwFn: func(context.Context, *connect.Request[storagev1.SetLkwRequest]) (*connect.Response[storagev1.SetLkwResponse], error) {
			calls.Add(1)
			return nil, errors.New("不应调用 SetLkw RPC")
		},
	}
	client := setupLockTestRPCServer(t, server)
	sessions := setupTestSessionManager(t, client)
	sessions.BindHandle(11, sessions.CurrentSessionID())
	sessions.BindHandle(12, sessions.CurrentSessionID())
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{RootIno: 1001})
	t.Cleanup(fs.OnUnmount)

	first := &fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 2001}, Fh: 11, Owner: 1,
		Lk: fuse.FileLock{Start: 10, End: 30, Typ: uint32(syscall.F_WRLCK), Pid: 123},
	}
	require.Equal(t, fuse.OK, fs.SetLk(nil, first))
	require.Equal(t, fuse.OK, fs.SetLkw(nil, first))

	second := &fuse.LkIn{
		InHeader: fuse.InHeader{NodeId: 2001}, Fh: 12, Owner: 2,
		Lk: fuse.FileLock{Start: 20, End: 40, Typ: uint32(syscall.F_RDLCK)},
	}
	var out fuse.LkOut
	require.Equal(t, fuse.OK, fs.GetLk(nil, second, &out))
	require.Equal(t, first.Lk, out.Lk)
	require.Equal(t, fuse.EAGAIN, fs.SetLk(nil, second))
	require.Zero(t, calls.Load())
}

func TestLocalPOSIXLockRangesAndConversions(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	request := func(owner, start, end uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: start, End: end, Typ: typ, Pid: 10}}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(1, 0, 99, read)))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(1, 20, 39, write)))
	check := func(start, end uint64, typ uint32, expected fuse.FileLock) {
		t.Helper()
		var out fuse.LkOut
		require.Equal(t, fuse.OK, fs.GetLk(nil, request(2, start, end, typ), &out))
		require.Equal(t, expected, out.Lk)
	}
	check(0, 19, write, fuse.FileLock{Start: 0, End: 19, Typ: read, Pid: 10})
	check(20, 39, read, fuse.FileLock{Start: 20, End: 39, Typ: write, Pid: 10})
	check(40, 99, write, fuse.FileLock{Start: 40, End: 99, Typ: read, Pid: 10})
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(1, 25, 34, unlock)))
	check(25, 34, read, fuse.FileLock{Start: 25, End: 34, Typ: unlock})
	check(20, 24, read, fuse.FileLock{Start: 20, End: 24, Typ: write, Pid: 10})
	check(35, 39, read, fuse.FileLock{Start: 35, End: 39, Typ: write, Pid: 10})
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(1, 20, 39, read)))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(1, 100, ^uint64(0), read)))
	check(0, ^uint64(0), write, fuse.FileLock{Start: 0, End: ^uint64(0), Typ: read, Pid: 10})
}

func TestLocalSetLkwWaitsAndWakes(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	first := &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 1, Owner: 1,
		Lk: fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_RDLCK)}}
	second := *first
	second.Fh, second.Owner = 2, 2
	second.Lk.Typ = uint32(syscall.F_WRLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, first))
	cancel := make(chan struct{})
	defer close(cancel)
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.SetLkw(cancel, &second) }()
	probe := *first
	probe.Fh, probe.Owner = 3, 3
	waitForQueuedWriter(t, func() fuse.Status { return fs.SetLk(nil, &probe) }, func() fuse.Status {
		unlock := probe
		unlock.Lk.Typ = uint32(syscall.F_UNLCK)
		return fs.SetLk(nil, &unlock)
	})
	first.Lk.Typ = uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, first))
	select {
	case status := <-done:
		require.Equal(t, fuse.OK, status)
	case <-time.After(time.Second):
		t.Fatal("释放锁后等待者未被唤醒")
	}
}

func TestWaitingWriterPreventsNewReadersFromOvertaking(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	read := func(owner uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: 0, End: 10, Typ: typ}}
	}
	require.Equal(t, fuse.OK, fs.SetLk(nil, read(1, uint32(syscall.F_RDLCK))))
	cancel := make(chan struct{})
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.SetLkw(cancel, read(2, uint32(syscall.F_WRLCK))) }()
	defer func() { close(cancel); <-done }()
	require.Eventually(t, func() bool {
		status := fs.SetLk(nil, read(3, uint32(syscall.F_RDLCK)))
		if status == fuse.OK {
			require.Equal(t, fuse.OK, fs.SetLk(nil, read(3, uint32(syscall.F_UNLCK))))
		}
		return status == fuse.EAGAIN
	}, time.Second, time.Millisecond, "等待中的写锁必须阻止新的读锁插队")
}

func TestLocalPOSIXDeadlockCycle(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	req := func(owner, start, end uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: start, End: end, Typ: typ}}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(1, 0, 9, read)))
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(2, 20, 29, read)))
	cancelFirst := make(chan struct{})
	firstDone := make(chan fuse.Status, 1)
	go func() { firstDone <- fs.SetLkw(cancelFirst, req(1, 20, 29, write)) }()
	defer func() { close(cancelFirst); <-firstDone }()
	require.Eventually(t, func() bool {
		status := fs.SetLk(nil, req(3, 20, 29, read))
		if status == fuse.OK {
			require.Equal(t, fuse.OK, fs.SetLk(nil, req(3, 20, 29, unlock)))
		}
		return status == fuse.EAGAIN
	}, time.Second, time.Millisecond)

	cancelSecond := make(chan struct{})
	secondDone := make(chan fuse.Status, 1)
	go func() { secondDone <- fs.SetLkw(cancelSecond, req(2, 0, 9, write)) }()
	select {
	case status := <-secondDone:
		require.Equal(t, fuse.Status(syscall.EDEADLK), status)
	case <-time.After(time.Second):
		close(cancelSecond)
		<-secondDone
		t.Fatal("环形等待未返回 EDEADLK")
	}
}

func TestLocalFlockNamespaceAndConversion(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	read, write := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK)
	posix := &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 11, Owner: 1,
		Lk: fuse.FileLock{Start: 1, End: 2, Typ: write}}
	require.Equal(t, fuse.OK, fs.SetLk(nil, posix))
	flock1 := *posix
	flock1.LkFlags = fuse.FUSE_LK_FLOCK
	require.Equal(t, fuse.OK, fs.SetLk(nil, &flock1))
	flock1.Owner, flock1.Lk.Typ = 99, read
	require.Equal(t, fuse.OK, fs.SetLk(nil, &flock1))
	flock2 := flock1
	flock2.Fh, flock2.Owner = 12, 99
	require.Equal(t, fuse.OK, fs.SetLk(nil, &flock2))
	flock1.Lk.Typ = write
	require.Equal(t, fuse.EAGAIN, fs.SetLk(nil, &flock1))
	flock2.Lk.Typ = write
	require.Equal(t, fuse.OK, fs.SetLk(nil, &flock2))
	var out fuse.LkOut
	require.Equal(t, fuse.OK, fs.GetLk(nil, &flock1, &out))
	require.Equal(t, fuse.FileLock{Start: 0, End: ^uint64(0), Typ: write}, out.Lk)

	fallback := flock1
	fallback.NodeId, fallback.Fh, fallback.Owner = 3, 0, 55
	require.Equal(t, fuse.OK, fs.SetLk(nil, &fallback))
	fallback.Owner = 56
	require.Equal(t, fuse.EAGAIN, fs.SetLk(nil, &fallback))
}

func TestPOSIXLockRespectsKnownOpenMode(t *testing.T) {
	var nextFH uint64 = 100
	srv := &handleMockStorageServer{
		openFn: func(_ context.Context, _ *connect.Request[storagev1.OpenRequest]) (*connect.Response[storagev1.OpenResponse], error) {
			nextFH++
			return connect.NewResponse(&storagev1.OpenResponse{Fh: nextFH}), nil
		},
		createFn: func(_ context.Context, _ *connect.Request[storagev1.CreateRequest]) (*connect.Response[storagev1.CreateResponse], error) {
			nextFH++
			return connect.NewResponse(&storagev1.CreateResponse{Fh: nextFH, Inode: &storagev1.Inode{Ino: 2}}), nil
		},
	}
	client := setupHandleTestRPCServer(t, srv)
	sessions := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	var readHandle, writeHandle, otherHandle fuse.OpenOut
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 2}, Flags: syscall.O_RDONLY}, &readHandle))
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 2}, Flags: syscall.O_WRONLY}, &writeHandle))
	require.Equal(t, fuse.OK, fs.Open(nil, &fuse.OpenIn{InHeader: fuse.InHeader{NodeId: 2}, Flags: syscall.O_ACCMODE}, &otherHandle))
	request := func(fh uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: fh, Owner: fh,
			Lk: fuse.FileLock{Start: fh, End: fh, Typ: typ}}
	}
	require.Equal(t, fuse.EBADF, fs.SetLk(nil, request(readHandle.Fh, uint32(syscall.F_WRLCK))))
	require.Equal(t, fuse.EBADF, fs.SetLk(nil, request(writeHandle.Fh, uint32(syscall.F_RDLCK))))
	require.Equal(t, fuse.EBADF, fs.SetLk(nil, request(otherHandle.Fh, uint32(syscall.F_RDLCK))))
	require.Equal(t, fuse.EBADF, fs.SetLk(nil, request(otherHandle.Fh, uint32(syscall.F_WRLCK))))
	var created fuse.CreateOut
	require.Equal(t, fuse.OK, fs.Create(nil, &fuse.CreateIn{InHeader: fuse.InHeader{NodeId: 1}, Flags: syscall.O_CREAT | syscall.O_RDONLY}, "file", &created))
	require.Equal(t, fuse.EBADF, fs.SetLk(nil, request(created.Fh, uint32(syscall.F_WRLCK))))
	flock := request(readHandle.Fh, uint32(syscall.F_WRLCK))
	flock.LkFlags = fuse.FUSE_LK_FLOCK
	require.Equal(t, fuse.OK, fs.SetLk(nil, flock))
	var out fuse.LkOut
	require.Equal(t, fuse.OK, fs.GetLk(nil, request(readHandle.Fh, uint32(syscall.F_WRLCK)), &out))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(readHandle.Fh, uint32(syscall.F_RDLCK))))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(writeHandle.Fh, uint32(syscall.F_WRLCK))))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(readHandle.Fh, uint32(syscall.F_UNLCK))))
	sessions.BindHandle(200, sessions.CurrentSessionID())
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(200, uint32(syscall.F_WRLCK))))
}

func TestFlushAndReleaseCleanOnlyTheirLockNamespace(t *testing.T) {
	client := setupHandleTestRPCServer(t, &handleMockStorageServer{})
	sessions := setupTestSessionManager(t, client)
	sessions.BindHandle(11, sessions.CurrentSessionID())
	sessions.BindHandle(12, sessions.CurrentSessionID())
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	request := func(fh, owner uint64, flags uint32, start uint64) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: fh, Owner: owner, LkFlags: flags,
			Lk: fuse.FileLock{Start: start, End: start + 10, Typ: uint32(syscall.F_WRLCK)}}
	}
	reader := request(11, 1, 0, 0)
	reader.Lk.Typ = uint32(syscall.F_RDLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, reader))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(11, 1, fuse.FUSE_LK_FLOCK, 0)))
	cancel := make(chan struct{})
	defer close(cancel)
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.SetLkw(cancel, request(12, 2, 0, 0)) }()
	probe := request(12, 3, 0, 0)
	probe.Lk.Typ = uint32(syscall.F_RDLCK)
	waitForQueuedWriter(t, func() fuse.Status { return fs.SetLk(nil, probe) }, func() fuse.Status {
		unlock := *probe
		unlock.Lk.Typ = uint32(syscall.F_UNLCK)
		return fs.SetLk(nil, &unlock)
	})
	require.Equal(t, fuse.OK, fs.Flush(nil, &fuse.FlushIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 12, LockOwner: 1}))
	select {
	case status := <-done:
		require.Equal(t, fuse.OK, status)
	case <-time.After(time.Second):
		t.Fatal("Flush 后 POSIX 等待者未被唤醒")
	}
	require.Equal(t, fuse.EAGAIN, fs.SetLk(nil, request(12, 2, fuse.FUSE_LK_FLOCK, 0)))
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(11, 1, 0, 50)))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 11, LockOwner: 1})
	require.Equal(t, fuse.OK, fs.SetLk(nil, request(12, 2, fuse.FUSE_LK_FLOCK, 0)))
	require.Equal(t, fuse.EAGAIN, fs.SetLk(nil, request(12, 2, 0, 50)))
}

func TestLockWaitCancellationRemovesQueueEntry(t *testing.T) {
	for _, byContext := range []bool{false, true} {
		name := "内核中断"
		if byContext {
			name = "上下文取消"
		}
		t.Run(name, func(t *testing.T) {
			table := fusefs.NewLockTable()
			defer table.Close()
			req := func(owner uint64, typ uint32) *fuse.LkIn {
				return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
					Lk: fuse.FileLock{Start: 0, End: 10, Typ: typ}}
			}
			read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
			require.Equal(t, fuse.OK, table.SetLk(req(1, read)))
			ctx, cancelCtx := context.WithCancel(context.Background())
			defer cancelCtx()
			cancelFuse := make(chan struct{})
			done := make(chan fuse.Status, 1)
			go func() { done <- table.SetLkw(ctx, cancelFuse, req(2, write)) }()
			require.Eventually(t, func() bool {
				status := table.SetLk(req(3, read))
				if status == fuse.OK {
					require.Equal(t, fuse.OK, table.SetLk(req(3, unlock)))
				}
				return status == fuse.EAGAIN
			}, time.Second, time.Millisecond)
			if byContext {
				cancelCtx()
			} else {
				close(cancelFuse)
			}
			select {
			case status := <-done:
				require.Equal(t, fuse.EINTR, status)
			case <-time.After(time.Second):
				t.Fatal("取消后等待者未退出")
			}
			require.Equal(t, fuse.OK, table.SetLk(req(3, read)), "取消后不得留下阻挡读锁的等待项")
		})
	}
}

func TestFlushCancelsSameOwnerWaiter(t *testing.T) {
	table := fusefs.NewLockTable()
	defer table.Close()
	held := &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 1, Owner: 1,
		Lk: fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_RDLCK)}}
	waiter := *held
	waiter.Fh, waiter.Owner = 2, 2
	waiter.Lk.Typ = uint32(syscall.F_WRLCK)
	require.Equal(t, fuse.OK, table.SetLk(held))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	done := make(chan fuse.Status, 1)
	go func() { done <- table.SetLkw(ctx, nil, &waiter) }()
	probe := *held
	probe.Fh, probe.Owner = 3, 3
	waitForQueuedWriter(t, func() fuse.Status { return table.SetLk(&probe) }, func() fuse.Status {
		unlock := probe
		unlock.Lk.Typ = uint32(syscall.F_UNLCK)
		return table.SetLk(&unlock)
	})
	table.ReleasePOSIXByOwner(2, 2)
	select {
	case status := <-done:
		require.Equal(t, fuse.EINTR, status)
	case <-time.After(time.Second):
		t.Fatal("Flush 未中断对应 LockOwner 的等待")
	}
}

func TestLocalPOSIXThreeOwnerDeadlockCycle(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	req := func(owner, start uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: start, End: start + 9, Typ: typ}}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
	for owner := uint64(1); owner <= 3; owner++ {
		require.Equal(t, fuse.OK, fs.SetLk(nil, req(owner, (owner-1)*10, read)))
	}
	startWait := func(owner, start uint64) {
		cancel := make(chan struct{})
		done := make(chan fuse.Status, 1)
		go func() { done <- fs.SetLkw(cancel, req(owner, start, write)) }()
		t.Cleanup(func() {
			close(cancel)
			select {
			case status := <-done:
				require.Equal(t, fuse.EINTR, status)
			case <-time.After(time.Second):
				t.Error("取消后等待者未退出")
			}
		})
		require.Eventually(t, func() bool {
			status := fs.SetLk(nil, req(9, start, read))
			if status == fuse.OK {
				require.Equal(t, fuse.OK, fs.SetLk(nil, req(9, start, unlock)))
			}
			return status == fuse.EAGAIN
		}, time.Second, time.Millisecond)
	}
	startWait(1, 10)
	startWait(2, 20)
	cancelThird := make(chan struct{})
	thirdDone := make(chan fuse.Status, 1)
	go func() { thirdDone <- fs.SetLkw(cancelThird, req(3, 0, write)) }()
	select {
	case status := <-thirdDone:
		require.Equal(t, fuse.Status(syscall.EDEADLK), status)
	case <-time.After(time.Second):
		close(cancelThird)
		<-thirdDone
		t.Fatal("三方环形等待未返回 EDEADLK")
	}
}

func TestReleaseWakesFlockWaiterAndFallbackOwner(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	flock := func(ino, fh, owner uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: ino}, Fh: fh, Owner: owner,
			LkFlags: fuse.FUSE_LK_FLOCK, Lk: fuse.FileLock{Typ: typ}}
	}
	require.Equal(t, fuse.OK, fs.SetLk(nil, flock(2, 11, 1, uint32(syscall.F_RDLCK))))
	cancel := make(chan struct{})
	defer close(cancel)
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.SetLkw(cancel, flock(2, 12, 2, uint32(syscall.F_WRLCK))) }()
	waitForQueuedWriter(t,
		func() fuse.Status { return fs.SetLk(nil, flock(2, 13, 3, uint32(syscall.F_RDLCK))) },
		func() fuse.Status { return fs.SetLk(nil, flock(2, 13, 3, uint32(syscall.F_UNLCK))) },
	)
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 11, LockOwner: 1})
	select {
	case status := <-done:
		require.Equal(t, fuse.OK, status)
	case <-time.After(time.Second):
		t.Fatal("Release 后 flock 等待者未被唤醒")
	}
	require.Equal(t, fuse.OK, fs.SetLk(nil, flock(3, 0, 7, uint32(syscall.F_WRLCK))))
	fs.Release(nil, &fuse.ReleaseIn{InHeader: fuse.InHeader{NodeId: 3}, Fh: 0, LockOwner: 7})
	require.Equal(t, fuse.OK, fs.SetLk(nil, flock(3, 0, 8, uint32(syscall.F_WRLCK))))
}

func TestConcurrentPOSIXWritersRemainExclusive(t *testing.T) {
	table := fusefs.NewLockTable()
	defer table.Close()
	var active atomic.Int32
	var failed atomic.Bool
	var workers sync.WaitGroup
	for owner := uint64(1); owner <= 8; owner++ {
		workers.Add(1)
		go func(owner uint64) {
			defer workers.Done()
			req := &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
				Lk: fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_WRLCK)}}
			for i := 0; i < 40; i++ {
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				status := table.SetLkw(ctx, nil, req)
				cancel()
				if status != fuse.OK {
					failed.Store(true)
					return
				}
				if active.Add(1) != 1 {
					failed.Store(true)
				}
				active.Add(-1)
				unlock := *req
				unlock.Lk.Typ = uint32(syscall.F_UNLCK)
				if table.SetLk(&unlock) != fuse.OK {
					failed.Store(true)
					return
				}
			}
		}(owner)
	}
	workers.Wait()
	require.False(t, failed.Load(), "写锁必须排他，且等待者必须及时取得锁")
}

func TestDeadlockGraphsDoNotMixLockNamespaces(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	req := func(owner, start uint64, typ, flags uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Owner: owner, LkFlags: flags,
			Lk: fuse.FileLock{Start: start, End: start + 9, Typ: typ}}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(2, 20, read, 0)))
	cancelPOSIX := make(chan struct{})
	posixDone := make(chan fuse.Status, 1)
	go func() { posixDone <- fs.SetLkw(cancelPOSIX, req(1, 20, write, 0)) }()
	defer func() { close(cancelPOSIX); <-posixDone }()
	require.Eventually(t, func() bool {
		status := fs.SetLk(nil, req(3, 20, read, 0))
		if status == fuse.OK {
			require.Equal(t, fuse.OK, fs.SetLk(nil, req(3, 20, unlock, 0)))
		}
		return status == fuse.EAGAIN
	}, time.Second, time.Millisecond)

	require.Equal(t, fuse.OK, fs.SetLk(nil, req(1, 0, read, fuse.FUSE_LK_FLOCK)))
	cancelFlock := make(chan struct{})
	defer close(cancelFlock)
	flockDone := make(chan fuse.Status, 1)
	go func() { flockDone <- fs.SetLkw(cancelFlock, req(2, 0, write, fuse.FUSE_LK_FLOCK)) }()
	waitForQueuedWriter(t,
		func() fuse.Status { return fs.SetLk(nil, req(3, 0, read, fuse.FUSE_LK_FLOCK)) },
		func() fuse.Status { return fs.SetLk(nil, req(3, 0, unlock, fuse.FUSE_LK_FLOCK)) },
	)
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(1, 0, unlock, fuse.FUSE_LK_FLOCK)))
	select {
	case status := <-flockDone:
		require.Equal(t, fuse.OK, status)
	case <-time.After(time.Second):
		t.Fatal("释放 flock 后等待者未被唤醒")
	}
}

func TestRawFSLockRejectsInvalidRequests(t *testing.T) {
	client := setupHandleTestRPCServer(t, &handleMockStorageServer{})
	sessions := setupTestSessionManager(t, client)
	fs := fusefs.NewRawFS(client, sessions, fusefs.Config{RootIno: 1001})
	t.Cleanup(fs.OnUnmount)
	request := &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: 10, Owner: 1,
		Lk: fuse.FileLock{Start: 0, End: 10, Typ: uint32(syscall.F_WRLCK)}}
	require.Equal(t, fuse.EBADF, fs.SetLk(nil, request))
	sessions.BindHandle(10, sessions.CurrentSessionID())
	request.Lk.Typ = 999
	require.Equal(t, fuse.EINVAL, fs.SetLk(nil, request))
	request.Lk.Typ, request.Lk.Start, request.Lk.End = uint32(syscall.F_WRLCK), 20, 10
	require.Equal(t, fuse.EINVAL, fs.SetLkw(nil, request))
	request.Lk.Typ, request.Lk.Start = uint32(syscall.F_UNLCK), 0
	var out fuse.LkOut
	require.Equal(t, fuse.EINVAL, fs.GetLk(nil, request, &out))
}
