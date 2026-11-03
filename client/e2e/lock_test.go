//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const lockHelperEnv = "BOWL_TEST_LOCK_HELPER"

func init() {
	switch os.Getenv(lockHelperEnv) {
	case "interrupt":
		runLockInterruptHelper()
		os.Exit(0)
	case "controller":
		runLockController()
		os.Exit(0)
	}
}

// 控制子进程持有独立的 POSIX 锁主体。
func runLockController() {
	f, err := os.OpenFile(os.Getenv("BOWL_TEST_LOCK_TARGET"), os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(10)
	}
	defer f.Close()
	r := bufio.NewReader(os.Stdin)
	for {
		var op string
		var start, length int64
		if _, err := fmt.Fscan(r, &op, &start, &length); err != nil {
			return
		}
		lock := unix.Flock_t{Whence: 0, Start: start, Len: length}
		cmd := unix.F_SETLK
		switch op {
		case "read":
			lock.Type = unix.F_RDLCK
		case "write":
			lock.Type = unix.F_WRLCK
		case "wait":
			lock.Type = unix.F_WRLCK
			cmd = unix.F_SETLKW
			fmt.Println("WAITING")
		case "get":
			lock.Type = unix.F_WRLCK
			cmd = unix.F_GETLK
		case "unlock":
			lock.Type = unix.F_UNLCK
		default:
			os.Exit(11)
		}
		err := unix.FcntlFlock(f.Fd(), cmd, &lock)
		var errno syscall.Errno
		if err != nil && !errors.As(err, &errno) {
			os.Exit(12)
		}
		fmt.Printf("%d %d %d\n", errno, lock.Type, lock.Start)
	}
}

type lockController struct {
	input  io.WriteCloser
	output *bufio.Reader
}

func startLockController(t *testing.T, path string) *lockController {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestE2E_Lock$")
	cmd.Env = append(os.Environ(), lockHelperEnv+"=controller", "BOWL_TEST_LOCK_TARGET="+path)
	input, err := cmd.StdinPipe()
	require.NoError(t, err)
	output, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	t.Cleanup(func() { _ = input.Close(); require.NoError(t, cmd.Wait()); cancel() })
	return &lockController{input, bufio.NewReader(output)}
}

func (c *lockController) call(t *testing.T, op string, start, length int64) (syscall.Errno, int16, int64) {
	t.Helper()
	_, err := fmt.Fprintf(c.input, "%s %d %d\n", op, start, length)
	require.NoError(t, err)
	var errno syscall.Errno
	var typ int16
	var gotStart int64
	_, err = fmt.Fscan(c.output, &errno, &typ, &gotStart)
	require.NoError(t, err)
	return errno, typ, gotStart
}

func runLockInterruptHelper() {
	path := os.Getenv("BOWL_TEST_LOCK_TARGET")
	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGUSR1)
	var sa struct {
		handler  uintptr
		flags    uint64
		restorer uintptr
		mask     uint64
	}
	_, _, _ = syscall.RawSyscall6(syscall.SYS_RT_SIGACTION, uintptr(syscall.SIGUSR1), 0, uintptr(unsafe.Pointer(&sa)), 8, 0, 0)
	sa.flags &^= 0x10000000
	_, _, _ = syscall.RawSyscall6(syscall.SYS_RT_SIGACTION, uintptr(syscall.SIGUSR1), uintptr(unsafe.Pointer(&sa)), 0, 8, 0, 0)
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(10)
	}
	defer f.Close()
	fmt.Println("READY_TO_LOCK")
	fl := unix.Flock_t{Type: unix.F_WRLCK}
	err = unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &fl)
	if !errors.Is(err, syscall.EINTR) {
		fmt.Fprintln(os.Stderr, "锁等待未返回 EINTR:", err)
		os.Exit(11)
	}
	fmt.Println("INTERRUPTED_EINTR")
}

func TestE2E_Lock(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mp := t.TempDir()
	cmd, _, _ := startBowlMount(t, getStorageEndpoint(), disk.GetDiskId(), mp)
	require.NoError(t, waitForMount(mp, 10*time.Second))
	defer stopBowlMountGracefully(t, cmd, mp, 5*time.Second)

	t.Run("进程锁冲突分裂和关闭释放", func(t *testing.T) {
		path := filepath.Join(mp, "posix.txt")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()
		fl := unix.Flock_t{Type: unix.F_WRLCK, Start: 0, Len: 100}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &fl))
		fl.Start = 100
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &fl))
		child := startLockController(t, path)
		errno, typ, start := child.call(t, "get", 50, 50)
		require.Zero(t, errno)
		require.Equal(t, int16(unix.F_WRLCK), typ)
		require.Equal(t, int64(0), start)
		errno, _, _ = child.call(t, "write", 50, 50)
		require.Equal(t, syscall.EAGAIN, errno)
		fl = unix.Flock_t{Type: unix.F_UNLCK, Start: 40, Len: 20}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &fl))
		errno, _, _ = child.call(t, "write", 40, 20)
		require.Zero(t, errno)
		errno, _, _ = child.call(t, "unlock", 40, 20)
		require.Zero(t, errno)
		dup, err := unix.Dup(int(f.Fd()))
		require.NoError(t, err)
		require.NoError(t, unix.Close(dup))
		errno, _, _ = child.call(t, "write", 0, 100)
		require.Zero(t, errno)
	})

	t.Run("flock独立句柄和dup", func(t *testing.T) {
		path := filepath.Join(mp, "flock.txt")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		fA, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		fB, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer fB.Close()
		require.NoError(t, unix.Flock(int(fA.Fd()), unix.LOCK_EX|unix.LOCK_NB))
		dup, err := unix.Dup(int(fA.Fd()))
		require.NoError(t, err)
		require.ErrorIs(t, unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
		require.NoError(t, unix.Close(dup))
		require.ErrorIs(t, unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB), syscall.EAGAIN)
		require.NoError(t, fA.Close())
		require.Eventually(t, func() bool { return unix.Flock(int(fB.Fd()), unix.LOCK_EX|unix.LOCK_NB) == nil }, 5*time.Second, 10*time.Millisecond)
	})

	t.Run("不同进程升级锁时检测死锁", func(t *testing.T) {
		path := filepath.Join(mp, "deadlock.txt")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer f.Close()
		readLock := unix.Flock_t{Type: unix.F_RDLCK}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &readLock))
		child := startLockController(t, path)
		errno, _, _ := child.call(t, "read", 0, 0)
		require.Zero(t, errno)
		_, err = fmt.Fprintln(child.input, "wait 0 0")
		require.NoError(t, err)
		var line string
		_, err = fmt.Fscan(child.output, &line)
		require.NoError(t, err)
		require.Equal(t, "WAITING", line)
		time.Sleep(150 * time.Millisecond)
		result := make(chan error, 1)
		go func() {
			writeLock := unix.Flock_t{Type: unix.F_WRLCK}
			result <- unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &writeLock)
		}()
		select {
		case err := <-result:
			require.ErrorIs(t, err, syscall.EDEADLK)
		case <-time.After(5 * time.Second):
			t.Fatal("升级锁未在限时内返回死锁错误")
		}
		unlock := unix.Flock_t{Type: unix.F_UNLCK}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unlock))
		var typ int16
		var start int64
		_, err = fmt.Fscan(child.output, &errno, &typ, &start)
		require.NoError(t, err)
		require.Zero(t, errno)
	})

	t.Run("等待锁时信号中断", func(t *testing.T) {
		path := filepath.Join(mp, "interrupt.txt")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		fl := unix.Flock_t{Type: unix.F_WRLCK}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &fl))
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		child := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestE2E_Lock$")
		child.Env = append(os.Environ(), lockHelperEnv+"=interrupt", "BOWL_TEST_LOCK_TARGET="+path)
		stdout, err := child.StdoutPipe()
		require.NoError(t, err)
		child.Stderr = os.Stderr
		require.NoError(t, child.Start())
		reader := bufio.NewReader(stdout)
		line, err := reader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "READY_TO_LOCK\n", line)
		time.Sleep(200 * time.Millisecond)
		require.NoError(t, child.Process.Signal(syscall.SIGUSR1))
		line, err = reader.ReadString('\n')
		require.NoError(t, err)
		require.Equal(t, "INTERRUPTED_EINTR\n", line)
		require.NoError(t, child.Wait())
		require.NoError(t, f.Close())
		verify, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer verify.Close()
		require.NoError(t, unix.FcntlFlock(verify.Fd(), unix.F_SETLK, &fl))
	})
}
