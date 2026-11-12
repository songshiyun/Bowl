//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

func readXattrNames(t *testing.T, targetPath string) []string {
	t.Helper()
	size, err := unix.Listxattr(targetPath, nil)
	require.NoError(t, err)
	buf := make([]byte, size)
	read, err := unix.Listxattr(targetPath, buf)
	require.NoError(t, err)
	return strings.Split(strings.TrimSuffix(string(buf[:read]), "\x00"), "\x00")
}

func requireXattrPresent(t *testing.T, targetPath, name string) {
	t.Helper()
	require.Contains(t, readXattrNames(t, targetPath), name)
}

func requireXattrAbsent(t *testing.T, targetPath, name string) {
	t.Helper()
	require.NotContains(t, readXattrNames(t, targetPath), name)
}

func requireXattrSemantics(t *testing.T, targetPath string) {
	t.Helper()
	attrFoo := "user.foo"
	attrBinary := "user.binary"
	attrNonExistent := "user.nonexistent"

	_, err := unix.Getxattr(targetPath, attrNonExistent, nil)
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA))

	err = unix.Removexattr(targetPath, attrNonExistent)
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA))

	err = unix.Setxattr(targetPath, attrFoo, []byte("bar"), unix.XATTR_REPLACE)
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA))

	valFoo := []byte("value-of-foo")
	require.NoError(t, unix.Setxattr(targetPath, attrFoo, valFoo, unix.XATTR_CREATE))

	err = unix.Setxattr(targetPath, attrFoo, []byte("conflict"), unix.XATTR_CREATE)
	require.True(t, errors.Is(err, unix.EEXIST) || errors.Is(err, syscall.EEXIST))

	require.NoError(t, unix.Setxattr(targetPath, attrFoo, []byte("value-of-foo-updated"), unix.XATTR_REPLACE))

	valBinary := []byte("prefix\x00middle\xff\x00suffix")
	require.NoError(t, unix.Setxattr(targetPath, attrBinary, valBinary, 0))

	sz, err := unix.Getxattr(targetPath, attrBinary, nil)
	require.NoError(t, err)
	require.Equal(t, len(valBinary), sz)

	_, err = unix.Getxattr(targetPath, attrBinary, make([]byte, len(valBinary)-2))
	require.True(t, errors.Is(err, unix.ERANGE) || errors.Is(err, syscall.ERANGE))

	exactBuf := make([]byte, len(valBinary)+8)
	szRead, err := unix.Getxattr(targetPath, attrBinary, exactBuf)
	require.NoError(t, err)
	require.Equal(t, len(valBinary), szRead)
	require.Equal(t, valBinary, exactBuf[:szRead])

	listSz, err := unix.Listxattr(targetPath, nil)
	require.NoError(t, err)
	require.Greater(t, listSz, len(attrFoo)+1+len(attrBinary))

	_, err = unix.Listxattr(targetPath, make([]byte, listSz-1))
	require.True(t, errors.Is(err, unix.ERANGE) || errors.Is(err, syscall.ERANGE))

	listBuf := make([]byte, listSz+16)
	szListRead, err := unix.Listxattr(targetPath, listBuf)
	require.NoError(t, err)
	require.Equal(t, listSz, szListRead)
	names := strings.Split(strings.TrimSuffix(string(listBuf[:szListRead]), "\x00"), "\x00")
	require.Contains(t, names, attrFoo)
	require.Contains(t, names, attrBinary)

	err = unix.Setxattr(targetPath, "user.toolarge", bytes.Repeat([]byte("A"), 65537), 0)
	require.True(t, errors.Is(err, unix.E2BIG) || errors.Is(err, syscall.E2BIG))

	require.NoError(t, unix.Removexattr(targetPath, attrFoo))
	_, err = unix.Getxattr(targetPath, attrFoo, nil)
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA))

	newNames := readXattrNames(t, targetPath)
	require.NotContains(t, newNames, attrFoo)
	require.Contains(t, newNames, attrBinary)

	require.NoError(t, unix.Removexattr(targetPath, attrBinary))
}

func TestE2E_Xattr(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("扩展属性增删改查长度探测与标志校验", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, _ := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

		filePath := filepath.Join(mountpoint, "test-file.txt")
		require.NoError(t, os.WriteFile(filePath, nil, 0o644))

		dirPath := filepath.Join(mountpoint, "test-dir")
		require.NoError(t, os.Mkdir(dirPath, 0o755))

		t.Run("常规文件", func(t *testing.T) { requireXattrSemantics(t, filePath) })
		t.Run("子目录", func(t *testing.T) { requireXattrSemantics(t, dirPath) })
		t.Run("挂载根目录", func(t *testing.T) { requireXattrSemantics(t, mountpoint) })
	})

	var capabilityLen int
	t.Run("写入自动剥离security_capability并重挂持久化", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

		filePath := filepath.Join(mountpoint, "capability.bin")
		require.NoError(t, os.WriteFile(filePath, []byte("base"), 0o755))

		capability := []byte{
			0x01, 0x00, 0x00, 0x02,
			0x00, 0x20, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}
		capabilityLen = len(capability)

		requireXattrAbsent(t, filePath, "security.capability")
		require.NoError(t, unix.Setxattr(filePath, "security.capability", capability, 0))
		requireXattrPresent(t, filePath, "security.capability")

		require.NoError(t, os.WriteFile(filePath, []byte("killpriv"), 0o755))
		requireXattrAbsent(t, filePath, "security.capability")
		stop()

		remountpoint, stopRemount := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")
		remountedPath := filepath.Join(remountpoint, "capability.bin")
		requireXattrAbsent(t, remountedPath, "security.capability")

		require.NoError(t, unix.Setxattr(remountedPath, "security.capability", capability, 0))
		requireXattrPresent(t, remountedPath, "security.capability")
		require.NoError(t, unix.Removexattr(remountedPath, "security.capability"))
		requireXattrAbsent(t, remountedPath, "security.capability")
		stopRemount()
	})

	recordArtifact(t, map[string]string{
		"missing_get":              "ENODATA",
		"replace_missing":          "ENODATA",
		"create_existing":          "EEXIST",
		"small_buffer":             "ERANGE",
		"oversize_value":           "E2BIG",
		"binary_value":             "prefix\\x00middle\\xff\\x00suffix",
		"capability_bytes":         strconv.Itoa(capabilityLen),
		"capability_after_write":   "absent",
		"capability_after_remount": "absent",
		"capability_after_remove":  "absent",
	})
}
