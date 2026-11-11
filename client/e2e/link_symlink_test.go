//go:build e2e && linux

package e2e_test

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestE2E_LinkAndSymlink 验证符号链接（相对/悬空/长路径/非UTF8字节）与同盘多硬链接生命周期及错误边界。
func TestE2E_LinkAndSymlink(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint, _ := mountWithCleanup(t, getStorageEndpoint(), disk.GetDiskId(), "--debug")

	var targetRel, targetDangling string
	var targetLongLen int
	var maxNlink uint64
	var hardlinkSameIno bool

	t.Run("符号链接创建读取悬空长路径与非UTF8无损传输", func(t *testing.T) {
		regPath := filepath.Join(mountpoint, "real_file.txt")
		require.NoError(t, unix.Mknod(regPath, syscall.S_IFREG|0o644, 0))

		var regStat unix.Stat_t
		require.NoError(t, unix.Stat(regPath, &regStat))

		symRel := filepath.Join(mountpoint, "sym_rel")
		require.NoError(t, os.Symlink("real_file.txt", symRel))

		var relLstat unix.Stat_t
		require.NoError(t, unix.Lstat(symRel, &relLstat))
		require.Equal(t, uint32(syscall.S_IFLNK), relLstat.Mode&syscall.S_IFMT)
		require.Equal(t, uint32(0o777), relLstat.Mode&0o777)

		var err error
		targetRel, err = os.Readlink(symRel)
		require.NoError(t, err)
		require.Equal(t, "real_file.txt", targetRel)

		var statFollowed unix.Stat_t
		require.NoError(t, unix.Stat(symRel, &statFollowed))
		require.Equal(t, regStat.Ino, statFollowed.Ino)

		symDangling := filepath.Join(mountpoint, "sym_dangling")
		require.NoError(t, os.Symlink("/nonexistent/target/path/to/ghost", symDangling))

		var danglingLstat unix.Stat_t
		require.NoError(t, unix.Lstat(symDangling, &danglingLstat))
		require.Equal(t, uint32(syscall.S_IFLNK), danglingLstat.Mode&syscall.S_IFMT)

		targetDangling, err = os.Readlink(symDangling)
		require.NoError(t, err)
		require.Equal(t, "/nonexistent/target/path/to/ghost", targetDangling)
		require.ErrorIs(t, unix.Stat(symDangling, &statFollowed), syscall.ENOENT)

		longTarget := strings.Repeat("x", 4095)
		symLong := filepath.Join(mountpoint, "sym_long")
		require.NoError(t, os.Symlink(longTarget, symLong))
		targetLong, err := os.Readlink(symLong)
		require.NoError(t, err)
		require.Equal(t, longTarget, targetLong)
		targetLongLen = len(targetLong)

		rawBytes := []byte{'r', 'a', 'w', '_', 0xff, 0xfe, 0x80, '_', 'p', 'a', 't', 'h'}
		symNonUTF8 := filepath.Join(mountpoint, "sym_non_utf8")
		require.NoError(t, unix.Symlink(string(rawBytes), symNonUTF8))

		buf := make([]byte, 256)
		n, err := unix.Readlink(symNonUTF8, buf)
		require.NoError(t, err)
		require.Equal(t, rawBytes, buf[:n])

		_, err = unix.Readlink(regPath, buf)
		require.ErrorIs(t, err, syscall.EINVAL)
		require.ErrorIs(t, os.Symlink("conflict_target", regPath), syscall.EEXIST)
	})

	t.Run("同盘跨目录硬链接计数增减与非法硬链接拦截", func(t *testing.T) {
		origPath := filepath.Join(mountpoint, "orig_file.txt")
		require.NoError(t, unix.Mknod(origPath, syscall.S_IFREG|0o644, 0))

		var origStat unix.Stat_t
		require.NoError(t, unix.Stat(origPath, &origStat))
		require.Equal(t, uint64(1), uint64(origStat.Nlink))
		origIno := origStat.Ino

		link1 := filepath.Join(mountpoint, "link1.txt")
		require.NoError(t, os.Link(origPath, link1))

		var stat1 unix.Stat_t
		require.NoError(t, unix.Stat(link1, &stat1))
		require.Equal(t, origIno, stat1.Ino)
		require.Equal(t, uint64(2), uint64(stat1.Nlink))

		subDir := filepath.Join(mountpoint, "subdir")
		require.NoError(t, os.Mkdir(subDir, 0o755))

		link2 := filepath.Join(subDir, "link2.txt")
		require.NoError(t, os.Link(origPath, link2))

		var stat2 unix.Stat_t
		require.NoError(t, unix.Stat(link2, &stat2))
		require.Equal(t, origIno, stat2.Ino)
		require.Equal(t, uint64(3), uint64(stat2.Nlink))
		maxNlink = uint64(stat2.Nlink)
		hardlinkSameIno = stat1.Ino == origIno && stat2.Ino == origIno

		require.NoError(t, os.Remove(origPath))
		require.ErrorIs(t, unix.Stat(origPath, &origStat), syscall.ENOENT)

		require.NoError(t, unix.Stat(link1, &stat1))
		require.Equal(t, uint64(2), uint64(stat1.Nlink))

		require.NoError(t, os.Remove(link1))
		require.NoError(t, unix.Stat(link2, &stat2))
		require.Equal(t, origIno, stat2.Ino)
		require.Equal(t, uint64(1), uint64(stat2.Nlink))

		require.ErrorIs(t, os.Link(subDir, filepath.Join(mountpoint, "dir_hardlink")), syscall.EPERM)
		require.ErrorIs(t, os.Link(link2, link2), syscall.EEXIST)
		require.ErrorIs(t, os.Link(filepath.Join(mountpoint, "ghost_file.txt"), filepath.Join(mountpoint, "ghost_link.txt")), syscall.ENOENT)
	})

	recordArtifact(t, map[string]string{
		"symlink_create":    targetRel,
		"readlink_dangling": targetDangling,
		"readlink_long_len": strconv.Itoa(targetLongLen),
		"readlink_on_file":  "EINVAL",
		"symlink_conflict":  "EEXIST",
		"hardlink_nlink":    strconv.FormatUint(maxNlink, 10),
		"hardlink_same_ino": strconv.FormatBool(hardlinkSameIno),
		"dir_link":          "EPERM",
		"link_conflict":     "EEXIST",
		"link_missing_src":  "ENOENT",
	})
}
