//go:build e2e && linux

package e2e_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	lockHelperEnv = "BOWL_TEST_LOCK_OP"
	lockTargetEnv = "BOWL_TEST_LOCK_TARGET"
	lockResultEnv = "BOWL_TEST_LOCK_RESULT"
)

// lockOpSpecs 把辅助子进程的单次 fcntl 操作映射到锁类型、命令与等待前钩子。
var lockOpSpecs = map[string]struct {
	typ    int16
	cmd    int
	before func()
}{
	"read":   {typ: unix.F_RDLCK, cmd: unix.F_SETLK, before: func() {}},
	"write":  {typ: unix.F_WRLCK, cmd: unix.F_SETLK, before: func() {}},
	"unlock": {typ: unix.F_UNLCK, cmd: unix.F_SETLK, before: func() {}},
	"get":    {typ: unix.F_WRLCK, cmd: unix.F_GETLK, before: func() {}},
	"wait": {typ: unix.F_WRLCK, cmd: unix.F_SETLKW, before: func() {
		runtime.LockOSThread()
		_, _ = fmt.Fprintf(os.Stdout, "WAITING %d\n", syscall.Gettid())
	}},
}

// TestMain 在设置 BOWL_TEST_LOCK_OP 时充当单次 fcntl 操作的子进程入口。
func TestMain(m *testing.M) {
	os.Exit(lockHelperOps[os.Getenv(lockHelperEnv)](m))
}

var lockHelperOps = map[string]func(*testing.M) int{
	"":          (*testing.M).Run,
	"read":      func(*testing.M) int { runLockOpHelper("read"); return 0 },
	"write":     func(*testing.M) int { runLockOpHelper("write"); return 0 },
	"unlock":    func(*testing.M) int { runLockOpHelper("unlock"); return 0 },
	"get":       func(*testing.M) int { runLockOpHelper("get"); return 0 },
	"wait":      func(*testing.M) int { runLockOpHelper("wait"); return 0 },
	"interrupt": func(*testing.M) int { runLockInterruptHelper(); return 0 },
}

// runLockOpHelper 在子进程中执行一次 fcntl，并把 errno、锁类型、起始偏移与全部系统调用错误写入结果文件。
func runLockOpHelper(op string) {
	spec := lockOpSpecs[op]
	fd, openErr := unix.Open(os.Getenv(lockTargetEnv), unix.O_RDWR, 0)
	defer func() { _ = unix.Close(fd) }()
	start, startErr := strconv.ParseInt(os.Getenv("BOWL_TEST_LOCK_START"), 10, 64)
	length, lenErr := strconv.ParseInt(os.Getenv("BOWL_TEST_LOCK_LEN"), 10, 64)
	lock := unix.Flock_t{Whence: 0, Start: start, Len: length, Type: spec.typ}
	spec.before()
	fcntlErr := unix.FcntlFlock(uintptr(fd), spec.cmd, &lock)
	var errno syscall.Errno
	_ = errors.As(fcntlErr, &errno)
	_ = os.WriteFile(os.Getenv(lockResultEnv), fmt.Appendf(nil,
		"errno=%d type=%d start=%d open=%v parse_start=%v parse_len=%v fcntl=%v\n",
		errno, lock.Type, lock.Start, openErr, startErr, lenErr, fcntlErr), 0o644)
}

// interruptOutcomes 把 fcntl 等待的返回值映射为可供父进程断言的稳定文本。
var interruptOutcomes = map[error]string{
	syscall.EINTR: "INTERRUPTED_EINTR",
	syscall.EBADF: "OPEN_FAILED_EBADF",
}

func runLockInterruptHelper() {
	path := os.Getenv(lockTargetEnv)
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
	fd, openErr := unix.Open(path, unix.O_RDWR, 0)
	defer func() { _ = unix.Close(fd) }()
	runtime.LockOSThread()
	_, _ = fmt.Fprintf(os.Stdout, "READY_TO_LOCK %d\n", syscall.Gettid())
	fl := unix.Flock_t{Type: unix.F_WRLCK}
	_, _ = fmt.Fprintf(os.Stdout, "%s open=%v\n", interruptOutcomes[unix.FcntlFlock(uintptr(fd), unix.F_SETLKW, &fl)], openErr)
}

type lockResult struct {
	errno syscall.Errno
	typ   int16
	start int64
}

// readLockResult 解析子进程写入的 fcntl 结果文件，并断言打开与解析阶段没有吞错。
func readLockResult(t *testing.T, path string) lockResult {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	content := string(data)
	var result lockResult
	_, err = fmt.Sscanf(content, "errno=%d type=%d start=%d", &result.errno, &result.typ, &result.start)
	require.NoError(t, err)
	require.Contains(t, content, "open=<nil>", "锁辅助进程打开目标文件失败")
	require.Contains(t, content, "parse_start=<nil>", "锁辅助进程解析起始偏移失败")
	require.Contains(t, content, "parse_len=<nil>", "锁辅助进程解析锁长度失败")
	require.Equal(t, result.errno == 0, strings.Contains(content, "fcntl=<nil>"), "fcntl 错误必须与 errno 一致: %s", content)
	return result
}

// startLockHelper 启动持有独立 POSIX 锁主体的子进程，并返回其进程句柄与结果文件路径。
func startLockHelper(t *testing.T, path, op string, start, length int64) (cmd *exec.Cmd, resultPath string) {
	t.Helper()
	resultPath = filepath.Join(t.TempDir(), "lock-result")
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	t.Cleanup(cancel)
	cmd = exec.CommandContext(ctx, os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		lockHelperEnv+"="+op,
		lockTargetEnv+"="+path,
		lockResultEnv+"="+resultPath,
		"BOWL_TEST_LOCK_START="+strconv.FormatInt(start, 10),
		"BOWL_TEST_LOCK_LEN="+strconv.FormatInt(length, 10),
	)
	return cmd, resultPath
}

// runLockOp 在独立子进程中执行一次 fcntl 锁操作并返回结果。
func runLockOp(t *testing.T, path, op string, start, length int64) lockResult {
	t.Helper()
	cmd, resultPath := startLockHelper(t, path, op, start, length)
	output, err := cmd.CombinedOutput()
	require.NoError(t, err, "锁辅助进程执行 %s 失败: %s", op, output)
	return readLockResult(t, resultPath)
}

// startLockProcess 启动持有独立 POSIX 锁主体的子进程并返回其输出流与结果文件路径。
func startLockProcess(t *testing.T, path, op string, start, length int64) (*exec.Cmd, *bufio.Reader, string) {
	t.Helper()
	cmd, resultPath := startLockHelper(t, path, op, start, length)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)
	cmd.Stderr = os.Stderr
	require.NoError(t, cmd.Start())
	return cmd, bufio.NewReader(stdout), resultPath
}

// blockingLockHelper 是阻塞在 fcntl 等待中的子进程句柄。
type blockingLockHelper struct {
	cmd        *exec.Cmd
	reader     *bufio.Reader
	resultPath string
	tid        int
}

// startBlockingLockHelper 启动阻塞在 fcntl 等待中的子进程，并确认其已进入内核等待。
func startBlockingLockHelper(t *testing.T, path, op, readyPrefix string) blockingLockHelper {
	t.Helper()
	cmd, reader, resultPath := startLockProcess(t, path, op, 0, 0)
	tid := readReadyTID(t, reader, readyPrefix)
	waitForBlockedInFcntl(t, cmd.Process.Pid, tid)
	return blockingLockHelper{cmd: cmd, reader: reader, resultPath: resultPath, tid: tid}
}

// readReadyTID 解析子进程就绪行中的线程 ID。
func readReadyTID(t *testing.T, reader *bufio.Reader, prefix string) int {
	t.Helper()
	fields := strings.Fields(readLine(t, reader))
	require.Len(t, fields, 2)
	require.Equal(t, prefix, fields[0])
	tid, err := strconv.Atoi(fields[1])
	require.NoError(t, err)
	return tid
}

// waitForBlockedInFcntl 确认子进程主线程已阻塞在 fcntl 系统调用中，替代固定时长休眠。
func waitForBlockedInFcntl(t *testing.T, pid, tid int) {
	t.Helper()
	require.Eventually(t, func() bool {
		data, err := os.ReadFile(fmt.Sprintf("/proc/%d/task/%d/syscall", pid, tid))
		return err == nil && (strings.HasPrefix(string(data), "72 ") || strings.HasPrefix(string(data), "25 "))
	}, 5*time.Second, 10*time.Millisecond, "子进程未进入 fcntl 锁等待")
}

func TestE2E_Lock(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mp := t.TempDir()
	cmd, _, _ := startBowlMount(t, getStorageEndpoint(), disk.GetDiskId(), mp)
	waitForMount(t, mp)
	defer stopBowlMountGracefully(t, cmd, mp)

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
		result := runLockOp(t, path, "get", 50, 50)
		require.Zero(t, result.errno)
		require.Equal(t, int16(unix.F_WRLCK), result.typ)
		require.Equal(t, int64(0), result.start)
		result = runLockOp(t, path, "write", 50, 50)
		require.Equal(t, syscall.EAGAIN, result.errno)
		fl = unix.Flock_t{Type: unix.F_UNLCK, Start: 40, Len: 20}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &fl))
		result = runLockOp(t, path, "write", 40, 20)
		require.Zero(t, result.errno)
		result = runLockOp(t, path, "unlock", 40, 20)
		require.Zero(t, result.errno)
		dup, err := unix.Dup(int(f.Fd()))
		require.NoError(t, err)
		require.NoError(t, unix.Close(dup))
		result = runLockOp(t, path, "write", 0, 100)
		require.Zero(t, result.errno)
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
		child := startBlockingLockHelper(t, path, "wait", "WAITING")
		writeLock := unix.Flock_t{Type: unix.F_WRLCK}
		require.ErrorIs(t, unix.FcntlFlock(f.Fd(), unix.F_SETLKW, &writeLock), syscall.EDEADLK)
		unlock := unix.Flock_t{Type: unix.F_UNLCK}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &unlock))
		require.NoError(t, child.cmd.Wait())
		require.Zero(t, readLockResult(t, child.resultPath).errno)
	})

	t.Run("等待锁时信号中断", func(t *testing.T) {
		path := filepath.Join(mp, "interrupt.txt")
		require.NoError(t, os.WriteFile(path, nil, 0o644))
		f, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		fl := unix.Flock_t{Type: unix.F_WRLCK}
		require.NoError(t, unix.FcntlFlock(f.Fd(), unix.F_SETLK, &fl))
		child := startBlockingLockHelper(t, path, "interrupt", "READY_TO_LOCK")
		// 定向到阻塞线程，避免进程级信号被投递到其它线程导致 fcntl 不返回 EINTR。
		require.NoError(t, unix.Tgkill(child.cmd.Process.Pid, child.tid, syscall.SIGUSR1))
		require.Equal(t, "INTERRUPTED_EINTR open=<nil>\n", readLine(t, child.reader))
		require.NoError(t, child.cmd.Wait())
		require.NoError(t, f.Close())
		verify, err := os.OpenFile(path, os.O_RDWR, 0)
		require.NoError(t, err)
		defer verify.Close()
		require.NoError(t, unix.FcntlFlock(verify.Fd(), unix.F_SETLK, &fl))
	})

	recordArtifact(t, map[string]string{
		"posix_conflict":        "EAGAIN",
		"posix_split_unlock":    "ok",
		"flock_exclusive":       "EAGAIN",
		"flock_dup_hold":        "EAGAIN",
		"deadlock_upgrade":      "EDEADLK",
		"signal_interrupt_wait": "EINTR",
	})
}

func readLine(t *testing.T, reader *bufio.Reader) string {
	t.Helper()
	line, err := reader.ReadString('\n')
	require.NoError(t, err)
	return line
}
