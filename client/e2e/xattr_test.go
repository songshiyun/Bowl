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

// readXattrNames 读取目标路径当前的全部扩展属性名。
func readXattrNames(t *testing.T, targetPath string) []string {
	t.Helper()
	size, err := unix.Listxattr(targetPath, nil)
	require.NoError(t, err)
	buf := make([]byte, size)
	read, err := unix.Listxattr(targetPath, buf)
	require.NoError(t, err)
	return strings.Split(strings.TrimSuffix(string(buf[:read]), "\x00"), "\x00")
}

// requireXattrPresent 断言目标路径的扩展属性列表包含指定名称。
func requireXattrPresent(t *testing.T, targetPath, name string) {
	t.Helper()
	require.Contains(t, readXattrNames(t, targetPath), name)
}

// requireXattrAbsent 断言目标路径的扩展属性列表不包含指定名称。
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
	require.Error(t, err, "获取不存在的属性必须报错")
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

	err = unix.Removexattr(targetPath, attrNonExistent)
	require.Error(t, err, "删除不存在属性必须报错")
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

	err = unix.Setxattr(targetPath, attrFoo, []byte("bar"), unix.XATTR_REPLACE)
	require.Error(t, err, "对不存在的属性使用 XATTR_REPLACE 必须报错")
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

	valFoo := []byte("value-of-foo")
	require.NoError(t, unix.Setxattr(targetPath, attrFoo, valFoo, unix.XATTR_CREATE), "XATTR_CREATE 设置新属性必须成功")

	err = unix.Setxattr(targetPath, attrFoo, []byte("conflict"), unix.XATTR_CREATE)
	require.Error(t, err, "对已存在属性使用 XATTR_CREATE 必须报错")
	require.True(t, errors.Is(err, unix.EEXIST) || errors.Is(err, syscall.EEXIST), "错误必须为 EEXIST")

	require.NoError(t, unix.Setxattr(targetPath, attrFoo, []byte("value-of-foo-updated"), unix.XATTR_REPLACE), "XATTR_REPLACE 必须成功")

	valBinary := []byte("prefix\x00middle\xff\x00suffix")
	require.NoError(t, unix.Setxattr(targetPath, attrBinary, valBinary, 0), "设置二进制属性值必须成功")

	sz, err := unix.Getxattr(targetPath, attrBinary, nil)
	require.NoError(t, err, "nil 探测 Getxattr 必须成功")
	require.Equal(t, len(valBinary), sz, "零长度探测应返回精确字节数")

	_, err = unix.Getxattr(targetPath, attrBinary, make([]byte, len(valBinary)-2))
	require.Error(t, err, "缓冲区过小 Getxattr 必须报错")
	require.True(t, errors.Is(err, unix.ERANGE) || errors.Is(err, syscall.ERANGE), "错误必须为 ERANGE")

	exactBuf := make([]byte, len(valBinary)+8)
	szRead, err := unix.Getxattr(targetPath, attrBinary, exactBuf)
	require.NoError(t, err, "充足缓冲区 Getxattr 必须成功")
	require.Equal(t, len(valBinary), szRead)
	require.Equal(t, valBinary, exactBuf[:szRead])

	listSz, err := unix.Listxattr(targetPath, nil)
	require.NoError(t, err, "nil 探测 Listxattr 必须成功")
	require.Greater(t, listSz, len(attrFoo)+1+len(attrBinary), "Listxattr 探测长度应包含所有属性名")

	_, err = unix.Listxattr(targetPath, make([]byte, listSz-1))
	require.Error(t, err, "缓冲区过小 Listxattr 必须报错")
	require.True(t, errors.Is(err, unix.ERANGE) || errors.Is(err, syscall.ERANGE), "错误必须为 ERANGE")

	listBuf := make([]byte, listSz+16)
	szListRead, err := unix.Listxattr(targetPath, listBuf)
	require.NoError(t, err, "充足缓冲区 Listxattr 必须成功")
	require.Equal(t, listSz, szListRead)
	names := strings.Split(strings.TrimSuffix(string(listBuf[:szListRead]), "\x00"), "\x00")
	require.Contains(t, names, attrFoo)
	require.Contains(t, names, attrBinary)

	err = unix.Setxattr(targetPath, "user.toolarge", bytes.Repeat([]byte("A"), 65537), 0)
	require.Error(t, err, "设置超 64 KiB 属性值必须报错")
	require.True(t, errors.Is(err, unix.E2BIG) || errors.Is(err, syscall.E2BIG), "错误必须为 E2BIG")

	require.NoError(t, unix.Removexattr(targetPath, attrFoo), "删除属性 user.foo 必须成功")

	_, err = unix.Getxattr(targetPath, attrFoo, nil)
	require.Error(t, err, "删除后再获取必须报错")
	require.True(t, errors.Is(err, unix.ENODATA) || errors.Is(err, syscall.ENODATA), "错误必须为 ENODATA")

	newListSz, err := unix.Listxattr(targetPath, nil)
	require.NoError(t, err)
	newBuf := make([]byte, newListSz)
	_, err = unix.Listxattr(targetPath, newBuf)
	require.NoError(t, err)
	newNames := strings.Split(strings.TrimSuffix(string(newBuf), "\x00"), "\x00")
	require.NotContains(t, newNames, attrFoo)
	require.Contains(t, newNames, attrBinary)

	require.NoError(t, unix.Removexattr(targetPath, attrBinary))
}

func TestE2E_Xattr(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	t.Run("xattr_crud_probes_and_flags", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint := t.TempDir()

		cmd, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
		waitForMount(t, mountpoint, "挂载点就绪")
		defer stopBowlMountGracefully(t, cmd, mountpoint)

		filePath := filepath.Join(mountpoint, "test-file.txt")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		require.NoError(t, err, "创建测试文件成功")
		require.NoError(t, f.Close(), "关闭测试文件成功")

		dirPath := filepath.Join(mountpoint, "test-dir")
		require.NoError(t, os.Mkdir(dirPath, 0o755), "创建测试目录成功")

		t.Run("regular_file", func(t *testing.T) { requireXattrSemantics(t, filePath) })
		t.Run("directory", func(t *testing.T) { requireXattrSemantics(t, dirPath) })
		t.Run("mount_root", func(t *testing.T) { requireXattrSemantics(t, mountpoint) })
	})

	t.Run("security_capability_killpriv", func(t *testing.T) {
		disk := createTestDisk(t, client)
		mountpoint, stop := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")

		filePath := filepath.Join(mountpoint, "capability.bin")
		require.NoError(t, os.WriteFile(filePath, []byte("base"), 0o755))

		// VFS_CAP_REVISION_2 | VFS_CAP_FLAGS_EFFECTIVE，授予 CAP_NET_RAW。
		capability := []byte{
			0x01, 0x00, 0x00, 0x02,
			0x00, 0x20, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
			0x00, 0x00, 0x00, 0x00,
		}

		// Linux capability LSM 会在内核态截获 FUSE 上 security.capability 的 getxattr
		// （直接返回 EINVAL，不会下发 FUSE_GETXATTR），因此以 listxattr 作为跨进程可观测面，
		// 验证设置、服务端 killpriv 原子删除与重挂后的持久化。
		requireXattrAbsent(t, filePath, "security.capability")

		require.NoError(t, unix.Setxattr(filePath, "security.capability", capability, 0), "设置 security.capability 必须成功")
		requireXattrPresent(t, filePath, "security.capability")

		require.NoError(t, os.WriteFile(filePath, []byte("killpriv"), 0o755), "写入必须成功")
		requireXattrAbsent(t, filePath, "security.capability")
		stop()

		remountpoint, stopRemount := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--debug")
		remountedPath := filepath.Join(remountpoint, "capability.bin")
		requireXattrAbsent(t, remountedPath, "security.capability")

		require.NoError(t, unix.Setxattr(remountedPath, "security.capability", capability, 0), "重挂后再次设置必须成功")
		requireXattrPresent(t, remountedPath, "security.capability")
		require.NoError(t, unix.Removexattr(remountedPath, "security.capability"))
		requireXattrAbsent(t, remountedPath, "security.capability")
		stopRemount()

		recordArtifact(t, map[string]string{
			"capability_bytes":         strconv.Itoa(len(capability)),
			"capability_initial":       "absent",
			"capability_after_set":     "present",
			"capability_after_write":   "absent",
			"capability_after_remount": "absent",
			"capability_after_remove":  "absent",
		})
	})

	t.Run("xattr_read_only_mount", func(t *testing.T) {
		disk := createTestDisk(t, client)

		mountpointRW := t.TempDir()
		cmdRW, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpointRW, "--debug")
		waitForMount(t, mountpointRW, "挂载读写就绪")

		filePath := filepath.Join(mountpointRW, "ro-file.txt")
		f, err := os.OpenFile(filePath, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
		require.NoError(t, err, "创建只读测试文件成功")
		require.NoError(t, f.Close(), "关闭只读测试文件成功")
		require.NoError(t, unix.Setxattr(filePath, "user.ro_key", []byte("ro_value"), 0))
		commitMain(t, client, disk.GetDiskId())
		stopBowlMountGracefully(t, cmdRW, mountpointRW)

		mountpointRO := t.TempDir()
		cmdRO, _, _ := startBowlMount(t, endpoint, disk.GetDiskId(), mountpointRO, "--ro", "--debug")
		waitForMount(t, mountpointRO, "挂载只读就绪")
		defer stopBowlMountGracefully(t, cmdRO, mountpointRO)

		roFilePath := filepath.Join(mountpointRO, "ro-file.txt")
		valBuf := make([]byte, 32)
		sz, err := unix.Getxattr(roFilePath, "user.ro_key", valBuf)
		require.NoError(t, err, "只读挂载点读取已存在的 xattr 必须成功")
		require.Equal(t, "ro_value", string(valBuf[:sz]))

		listSz, err := unix.Listxattr(roFilePath, nil)
		require.NoError(t, err, "只读挂载点列出 xattr 必须成功")
		require.Positive(t, listSz)

		err = unix.Setxattr(roFilePath, "user.ro_new", []byte("val"), 0)
		require.Error(t, err, "只读挂载点 Setxattr 必须报错")
		require.True(t, errors.Is(err, unix.EROFS) || errors.Is(err, syscall.EROFS), "错误必须为 EROFS")

		err = unix.Removexattr(roFilePath, "user.ro_key")
		require.Error(t, err, "只读挂载点 Removexattr 必须报错")
		require.True(t, errors.Is(err, unix.EROFS) || errors.Is(err, syscall.EROFS), "错误必须为 EROFS")
	})

	recordArtifact(t, map[string]string{
		"missing_get":     "ENODATA",
		"replace_missing": "ENODATA",
		"create_existing": "EEXIST",
		"small_buffer":    "ERANGE",
		"oversize_value":  "E2BIG",
		"binary_value":    "prefix\\x00middle\\xff\\x00suffix",
		"readonly_set":    "EROFS",
		"readonly_remove": "EROFS",
	})
}
