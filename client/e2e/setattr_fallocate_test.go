//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func TestE2E_SetAttr(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint, _ := mountWithCleanup(t, getStorageEndpoint(), disk.GetDiskId(), "--debug")

	t.Run("权限变更与变更属主自动剥离特权位", func(t *testing.T) {
		filePath := filepath.Join(mountpoint, "priv-test.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("privileged content"), 0o644))

		require.NoError(t, os.Chmod(filePath, 0o755|os.ModeSetuid|os.ModeSetgid))
		info, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o755), info.Mode().Perm())
		require.NotZero(t, info.Mode()&os.ModeSetuid)
		require.NotZero(t, info.Mode()&os.ModeSetgid)

		require.NoError(t, os.Chown(filePath, 1001, 1002))
		infoAfterChown, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o755), infoAfterChown.Mode().Perm())
		require.Equal(t, os.FileMode(0), infoAfterChown.Mode()&(os.ModeSetuid|os.ModeSetgid))

		var st syscall.Stat_t
		require.NoError(t, syscall.Stat(filePath, &st))
		require.Equal(t, uint32(1001), st.Uid)
		require.Equal(t, uint32(1002), st.Gid)
		require.Equal(t, uint32(0), st.Mode&(syscall.S_ISUID|syscall.S_ISGID))

		require.NoError(t, os.Chmod(filePath, 0o600))
		infoFinal, err := os.Stat(filePath)
		require.NoError(t, err)
		require.Equal(t, os.FileMode(0o600), infoFinal.Mode().Perm())
	})

	t.Run("显式微秒时间戳与UTIME_NOW更新", func(t *testing.T) {
		filePath := filepath.Join(mountpoint, "time-test.txt")
		require.NoError(t, os.WriteFile(filePath, []byte("time stamps"), 0o644))

		expAtime := unix.Timespec{Sec: 1680000000, Nsec: 123456000}
		expMtime := unix.Timespec{Sec: 1680000010, Nsec: 987654000}
		require.NoError(t, unix.UtimesNanoAt(unix.AT_FDCWD, filePath, []unix.Timespec{expAtime, expMtime}, 0))

		var st syscall.Stat_t
		require.NoError(t, syscall.Stat(filePath, &st))
		require.Equal(t, expAtime.Sec, st.Atim.Sec)
		require.Equal(t, expAtime.Nsec, st.Atim.Nsec)
		require.Equal(t, expMtime.Sec, st.Mtim.Sec)
		require.Equal(t, expMtime.Nsec, st.Mtim.Nsec)

		nowBefore := time.Now().Unix()
		timesNow := []unix.Timespec{
			{Sec: 0, Nsec: unix.UTIME_NOW},
			{Sec: 0, Nsec: unix.UTIME_NOW},
		}
		require.NoError(t, unix.UtimesNanoAt(unix.AT_FDCWD, filePath, timesNow, 0))

		var stNow syscall.Stat_t
		require.NoError(t, syscall.Stat(filePath, &stNow))
		require.GreaterOrEqual(t, stNow.Atim.Sec, nowBefore-2)
		require.GreaterOrEqual(t, stNow.Mtim.Sec, nowBefore-2)
	})

	t.Run("路径截断与句柄截断稀疏扩容缩容", func(t *testing.T) {
		filePath := filepath.Join(mountpoint, "trunc-test.txt")
		initialData := bytes.Repeat([]byte("A"), 4096)
		require.NoError(t, os.WriteFile(filePath, initialData, 0o644))

		require.NoError(t, os.Truncate(filePath, 16384))
		readBack, err := os.ReadFile(filePath)
		require.NoError(t, err)
		require.Len(t, readBack, 16384)
		require.Equal(t, initialData, readBack[:4096])
		require.Equal(t, make([]byte, 16384-4096), readBack[4096:])

		f, err := os.OpenFile(filePath, os.O_RDWR, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })

		require.NoError(t, f.Truncate(2048))
		bufShrink := make([]byte, 2048)
		_, err = f.ReadAt(bufShrink, 0)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte("A"), 2048), bufShrink)

		require.NoError(t, f.Truncate(8192))
		bufGrow := make([]byte, 8192)
		_, err = f.ReadAt(bufGrow, 0)
		require.NoError(t, err)
		require.Equal(t, bytes.Repeat([]byte("A"), 2048), bufGrow[:2048])
		require.Equal(t, make([]byte, 8192-2048), bufGrow[2048:])
		require.NoError(t, f.Close())
	})

	t.Run("预分配空间与打洞及不支持模式拦截", func(t *testing.T) {
		filePath := filepath.Join(mountpoint, "falloc-test.bin")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR, 0o644)
		require.NoError(t, err)
		t.Cleanup(func() { _ = f.Close() })

		partA := bytes.Repeat([]byte{'A'}, 8192)
		partB := bytes.Repeat([]byte{'B'}, 8192)
		partC := bytes.Repeat([]byte{'C'}, 16384)
		var initialData []byte
		initialData = append(initialData, partA...)
		initialData = append(initialData, partB...)
		initialData = append(initialData, partC...)
		_, err = f.Write(initialData)
		require.NoError(t, err)

		require.NoError(t, unix.Fallocate(int(f.Fd()), 0, 0, 64*1024))
		st1, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(64*1024), st1.Size())

		require.NoError(t, unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_KEEP_SIZE, 0, 128*1024))
		st2, err := f.Stat()
		require.NoError(t, err)
		require.Equal(t, int64(64*1024), st2.Size())

		require.NoError(t, unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE, 8192, 8192))
		readPunched := make([]byte, 32*1024)
		_, err = f.ReadAt(readPunched, 0)
		require.NoError(t, err)
		require.Equal(t, partA, readPunched[:8192])
		require.Equal(t, make([]byte, 8192), readPunched[8192:16384])
		require.Equal(t, partC, readPunched[16384:32*1024])

		errZeroRange := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_ZERO_RANGE, 0, 4096)
		require.True(t, errors.Is(errZeroRange, syscall.EOPNOTSUPP) || errors.Is(errZeroRange, unix.EOPNOTSUPP))

		errPunchSolo := unix.Fallocate(int(f.Fd()), unix.FALLOC_FL_PUNCH_HOLE, 0, 4096)
		require.True(t, errors.Is(errPunchSolo, syscall.EOPNOTSUPP) || errors.Is(errPunchSolo, unix.EOPNOTSUPP))

		require.NoError(t, f.Close())
	})

	recordArtifact(t, map[string]string{
		"chown_strips_suid_sgid": "true",
		"utimensat_explicit":     "ok",
		"truncate_expand":        "16384",
		"truncate_shrink":        "2048",
		"handle_truncate_grow":   "8192",
		"fallocate_default":      "65536",
		"punch_hole":             "ok",
		"fallocate_unsupported":  "EOPNOTSUPP",
	})
}
