//go:build e2e && linux

package e2e_test

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestE2E_ReadDir 完整验证目录句柄生命周期、空与嵌套目录以及 1500+ 个子项的分页遍历。
func TestE2E_ReadDir(t *testing.T) {
	storageClient := newTestStorageClient(t)
	disk := createTestDisk(t, storageClient)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	err := waitForMount(mountpoint, 10*time.Second)
	if err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("等待挂载就绪失败: %v: %s %s", err, stdout.String(), stderr.String())
	}
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)

	// 通过分支挂载准备目录树和分页数据。
	for _, dir := range []string{"empty_dir", "level1/level2", "bulk_1500"} {
		require.NoError(t, os.MkdirAll(filepath.Join(mountpoint, dir), 0o755))
	}
	require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "level1", "level2", "child.txt"), nil, 0o644))
	const totalFiles = 1500
	expectedNames := make(map[string]struct{}, totalFiles)
	for i := 0; i < totalFiles; i++ {
		name := fmt.Sprintf("item_%04d.dat", i)
		expectedNames[name] = struct{}{}
		require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "bulk_1500", name), nil, 0o644))
	}

	// -------------------------------------------------------------
	// 3. 验证场景 1：空目录、多级嵌套目录以及 Inode 根节点映射
	// -------------------------------------------------------------
	t.Run("空目录与多级嵌套目录遍历及Inode重写", func(t *testing.T) {
		// 根目录遍历验证
		rootEntries, err := os.ReadDir(mountpoint)
		require.NoError(t, err, "读取根目录失败")
		rootNameSet := make(map[string]bool)
		for _, e := range rootEntries {
			rootNameSet[e.Name()] = true
		}
		require.True(t, rootNameSet["empty_dir"])
		require.True(t, rootNameSet["level1"])
		require.True(t, rootNameSet["bulk_1500"])

		// 空目录读取必须为空切片
		emptyEntries, err := os.ReadDir(filepath.Join(mountpoint, "empty_dir"))
		require.NoError(t, err, "读取 empty_dir 失败")
		require.Empty(t, emptyEntries, "empty_dir 必须无任何子项")

		// 多级嵌套目录读取
		l1Entries, err := os.ReadDir(filepath.Join(mountpoint, "level1"))
		require.NoError(t, err)
		require.Len(t, l1Entries, 1)
		require.Equal(t, "level2", l1Entries[0].Name())
		require.True(t, l1Entries[0].IsDir())

		l2Entries, err := os.ReadDir(filepath.Join(mountpoint, "level1", "level2"))
		require.NoError(t, err)
		require.Len(t, l2Entries, 1)
		require.Equal(t, "child.txt", l2Entries[0].Name())
		require.False(t, l2Entries[0].IsDir())

		// 验证根目录与一级子目录的 '.' 与 '..' Inode 映射为 1
		var statDot unix.Stat_t
		require.NoError(t, unix.Stat(filepath.Join(mountpoint, "."), &statDot))
		require.Equal(t, uint64(1), statDot.Ino, "根目录 '.' 的 Inode 必须为 1")

		var statL1Parent unix.Stat_t
		require.NoError(t, unix.Stat(filepath.Join(mountpoint, "level1", ".."), &statL1Parent))
		require.Equal(t, uint64(1), statL1Parent.Ino, "一级子目录的 '..' 必须映射为内核根节点 1")
	})

	// -------------------------------------------------------------
	// 5. 验证场景 3：1500+ 个子文件的单目录 os.ReadDir 完整分页遍历
	// -------------------------------------------------------------
	t.Run("1500+子文件单目录分页遍历完整性与确定性", func(t *testing.T) {
		bulkPath := filepath.Join(mountpoint, "bulk_1500")

		readStartTime := time.Now()
		entries, err := os.ReadDir(bulkPath)
		require.NoError(t, err, "os.ReadDir 读取 1500+ 文件单目录失败")
		t.Logf("os.ReadDir 遍历 %d 个条目耗时: %v", len(entries), time.Since(readStartTime))

		require.Equal(t, totalFiles, len(entries), "遍历返回条目总数必须严格等于 1500")

		seenMap := make(map[string]struct{}, totalFiles)
		for _, entry := range entries {
			name := entry.Name()
			_, expected := expectedNames[name]
			require.True(t, expected, "发现未知条目: %s", name)

			_, alreadySeen := seenMap[name]
			require.False(t, alreadySeen, "发现重复条目: %s", name)
			seenMap[name] = struct{}{}

			require.True(t, entry.Type().IsRegular(), "条目类型必须为常规文件: %s", name)
		}

		require.Equal(t, totalFiles, len(seenMap), "必须 100% 覆盖预期 1500 个文件名且零遗漏零重复")
	})

}

// TestE2E_OpenDirectoryAfterRemoval 验证目录在本地打开期间被删除后的同步。
func TestE2E_OpenDirectoryAfterRemoval(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint := t.TempDir()
	cmd, stdout, stderr := startBowlMount(t, getStorageEndpoint(), disk.GetDiskId(), mountpoint)
	if err := waitForMount(mountpoint, 10*time.Second); err != nil {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		t.Fatalf("等待挂载就绪失败: %v: %s %s", err, stdout.String(), stderr.String())
	}
	defer stopBowlMountGracefully(t, cmd, mountpoint, 5*time.Second)
	path := filepath.Join(mountpoint, "open_then_rmdir")
	require.NoError(t, os.Mkdir(path, 0o755))
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	require.NoError(t, err)
	defer func() {
		if fd >= 0 {
			_ = syscall.Close(fd)
		}
	}()
	second, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	require.NoError(t, err)
	defer func() {
		if second >= 0 {
			_ = syscall.Close(second)
		}
	}()
	require.NoError(t, os.Remove(path))
	var attr unix.Stat_t
	require.NoError(t, unix.Fstat(fd, &attr), "已删除的打开目录应保留属性")
	require.Zero(t, attr.Nlink)
	require.NoError(t, syscall.Fsync(fd), "已删除的打开目录应能同步")
	require.NoError(t, unix.Fdatasync(fd), "已删除的打开目录应能同步数据")
	_, err = unix.Getdents(fd, make([]byte, 4096))
	require.ErrorIs(t, err, syscall.ENOENT)
	require.NoError(t, syscall.Close(fd))
	fd = -1
	require.NoError(t, unix.Fstat(second, &attr))
	require.Zero(t, attr.Nlink)
	require.NoError(t, syscall.Fsync(second))
	require.NoError(t, syscall.Close(second))
	second = -1
	require.Eventually(t, func() bool { _, err := os.Stat(path); return os.IsNotExist(err) },
		5*time.Second, 50*time.Millisecond)
}
