package fusefs_test

import (
	"context"
	"syscall"
	"testing"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/client/internal/fusefs"
)

// requireLockStatus 在有限时间内接收阻塞锁操作结果，避免回归时测试无限挂起。
func requireLockStatus(t *testing.T, want fuse.Status, done <-chan fuse.Status) {
	t.Helper()
	require.Eventually(t, func() bool { return len(done) == 1 }, 5*time.Second, time.Millisecond, "阻塞锁操作未在超时内返回")
	require.Equal(t, want, <-done)
}

// waitForQueuedWriter 轮询直到等待中的写锁阻止新读锁，并释放探测读锁。
func waitForQueuedWriter(t *testing.T, tryRead, unlock func() fuse.Status) {
	t.Helper()
	require.Eventually(t, func() bool {
		status := tryRead()
		unlockStatus := unlock()
		return unlockStatus == fuse.OK && status == fuse.EAGAIN
	}, time.Second, time.Millisecond, "等待中的写锁应阻止新读锁")
}

func TestLocalPOSIXLockConflictsWithoutRPC(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1001})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2001, 11, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2001, 12, syscall.O_RDWR)

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
}

func TestLocalPOSIXLockRangesAndConversions(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 1, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 2, syscall.O_RDWR)
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
	fs.PrepareTestFileHandle(t, 2, 1, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 2, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 3, syscall.O_RDWR)
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
	requireLockStatus(t, fuse.OK, done)
}

func TestWaitingWriterPreventsNewReadersFromOvertaking(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 1, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 2, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 3, syscall.O_RDWR)
	read := func(owner uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: 0, End: 10, Typ: typ}}
	}
	require.Equal(t, fuse.OK, fs.SetLk(nil, read(1, uint32(syscall.F_RDLCK))))
	cancel := make(chan struct{})
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.SetLkw(cancel, read(2, uint32(syscall.F_WRLCK))) }()
	defer func() { close(cancel); requireLockStatus(t, fuse.EINTR, done) }()
	waitForQueuedWriter(t,
		func() fuse.Status { return fs.SetLk(nil, read(3, uint32(syscall.F_RDLCK))) },
		func() fuse.Status { return fs.SetLk(nil, read(3, uint32(syscall.F_UNLCK))) },
	)
}

func TestLocalPOSIXDeadlockCycle(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 1, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 2, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 3, syscall.O_RDWR)
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
	defer func() { close(cancelFirst); requireLockStatus(t, fuse.EINTR, firstDone) }()
	waitForQueuedWriter(t,
		func() fuse.Status { return fs.SetLk(nil, req(3, 20, 29, read)) },
		func() fuse.Status { return fs.SetLk(nil, req(3, 20, 29, unlock)) },
	)

	cancelSecond := make(chan struct{})
	secondDone := make(chan fuse.Status, 1)
	go func() { secondDone <- fs.SetLkw(cancelSecond, req(2, 0, 9, write)) }()
	t.Cleanup(func() { close(cancelSecond) })
	requireLockStatus(t, fuse.Status(syscall.EDEADLK), secondDone)
}

func TestLocalFlockNamespaceAndConversion(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 11, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 12, syscall.O_RDWR)
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
	table := fusefs.NewLockTable()
	defer table.Close()
	require.Equal(t, fuse.OK, table.SetLk(&fallback))
	fallback.Owner = 56
	require.Equal(t, fuse.EAGAIN, table.SetLk(&fallback))
}

func testLockWaitCancellation(t *testing.T, cancelWait func(cancelFuse chan struct{}, cancelCtx context.CancelFunc)) {
	t.Helper()
	table := fusefs.NewLockTable()
	defer table.Close()
	req := func(owner uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: 0, End: 10, Typ: typ}}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, table.SetLk(req(1, read)))
	ctx, cancelCtx := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelCtx()
	cancelFuse := make(chan struct{})
	done := make(chan fuse.Status, 1)
	go func() { done <- table.SetLkw(ctx, cancelFuse, req(2, write)) }()
	waitForQueuedWriter(t,
		func() fuse.Status { return table.SetLk(req(3, read)) },
		func() fuse.Status { return table.SetLk(req(3, unlock)) },
	)
	cancelWait(cancelFuse, cancelCtx)
	requireLockStatus(t, fuse.EINTR, done)
	require.Equal(t, fuse.OK, table.SetLk(req(3, read)), "取消后不得留下阻挡读锁的等待项")
}

func TestLockWaitCancellationByKernelInterrupt(t *testing.T) {
	testLockWaitCancellation(t, func(cancelFuse chan struct{}, _ context.CancelFunc) {
		close(cancelFuse)
	})
}

func TestLockWaitCancellationByContext(t *testing.T) {
	testLockWaitCancellation(t, func(_ chan struct{}, cancelCtx context.CancelFunc) {
		cancelCtx()
	})
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
	requireLockStatus(t, fuse.EINTR, done)
}

func startDeadlockWait(t *testing.T, fs *fusefs.RawFS, req func(owner, start uint64, typ uint32) *fuse.LkIn, owner, start uint64) {
	t.Helper()
	cancel := make(chan struct{})
	done := make(chan fuse.Status, 1)
	go func() { done <- fs.SetLkw(cancel, req(owner, start, uint32(syscall.F_WRLCK))) }()
	t.Cleanup(func() {
		close(cancel)
		requireLockStatus(t, fuse.EINTR, done)
	})
	waitForQueuedWriter(t,
		func() fuse.Status { return fs.SetLk(nil, req(9, start, uint32(syscall.F_RDLCK))) },
		func() fuse.Status { return fs.SetLk(nil, req(9, start, uint32(syscall.F_UNLCK))) },
	)
}

func TestLocalPOSIXThreeOwnerDeadlockCycle(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 1, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 2, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 3, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 9, syscall.O_RDWR)
	req := func(owner, start uint64, typ uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner,
			Lk: fuse.FileLock{Start: start, End: start + 9, Typ: typ}}
	}
	read, write := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(1, 0, read)))
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(2, 10, read)))
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(3, 20, read)))
	startDeadlockWait(t, fs, req, 1, 10)
	startDeadlockWait(t, fs, req, 2, 20)
	cancelThird := make(chan struct{})
	thirdDone := make(chan fuse.Status, 1)
	go func() { thirdDone <- fs.SetLkw(cancelThird, req(3, 0, write)) }()
	t.Cleanup(func() { close(cancelThird) })
	requireLockStatus(t, fuse.Status(syscall.EDEADLK), thirdDone)
}

func TestReleaseWakesFlockWaiterAndFallbackOwner(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 11, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 12, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 13, syscall.O_RDWR)
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
	requireLockStatus(t, fuse.OK, done)
	table := fusefs.NewLockTable()
	defer table.Close()
	require.Equal(t, fuse.OK, table.SetLk(flock(3, 0, 7, uint32(syscall.F_WRLCK))))
	table.ReleaseFlockByFH(3, 0, 7)
	require.Equal(t, fuse.OK, table.SetLk(flock(3, 0, 8, uint32(syscall.F_WRLCK))))
}

func TestDeadlockGraphsDoNotMixLockNamespaces(t *testing.T) {
	fs := fusefs.NewRawFS(nil, nil, fusefs.Config{RootIno: 1})
	t.Cleanup(fs.OnUnmount)
	fs.PrepareTestFileHandle(t, 2, 1, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 2, syscall.O_RDWR)
	fs.PrepareTestFileHandle(t, 2, 3, syscall.O_RDWR)
	req := func(owner, start uint64, typ, flags uint32) *fuse.LkIn {
		return &fuse.LkIn{InHeader: fuse.InHeader{NodeId: 2}, Fh: owner, Owner: owner, LkFlags: flags,
			Lk: fuse.FileLock{Start: start, End: start + 9, Typ: typ}}
	}
	read, write, unlock := uint32(syscall.F_RDLCK), uint32(syscall.F_WRLCK), uint32(syscall.F_UNLCK)
	require.Equal(t, fuse.OK, fs.SetLk(nil, req(2, 20, read, 0)))
	cancelPOSIX := make(chan struct{})
	posixDone := make(chan fuse.Status, 1)
	go func() { posixDone <- fs.SetLkw(cancelPOSIX, req(1, 20, write, 0)) }()
	defer func() { close(cancelPOSIX); requireLockStatus(t, fuse.EINTR, posixDone) }()
	waitForQueuedWriter(t,
		func() fuse.Status { return fs.SetLk(nil, req(3, 20, read, 0)) },
		func() fuse.Status { return fs.SetLk(nil, req(3, 20, unlock, 0)) },
	)

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
	requireLockStatus(t, fuse.OK, flockDone)
}
