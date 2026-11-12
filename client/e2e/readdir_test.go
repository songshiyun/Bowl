//go:build e2e && linux

package e2e_test

import (
	"context"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

// bulkFileNames 是分页测试使用的固定小规模文件集，避免在测试代码中循环创建大量文件。
var bulkFileNames = []string{"item_0000.dat", "item_0001.dat", "item_0002.dat", "item_0003.dat"}

// createReadDirTree 创建空目录、两级嵌套目录与固定小规模文件目录。
func createReadDirTree(t *testing.T, mountpoint string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(filepath.Join(mountpoint, "empty_dir"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(mountpoint, "level1", "level2"), 0o755))
	require.NoError(t, os.MkdirAll(filepath.Join(mountpoint, "bulk"), 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "level1", "level2", "child.txt"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "bulk", "item_0000.dat"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "bulk", "item_0001.dat"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "bulk", "item_0002.dat"), nil, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(mountpoint, "bulk", "item_0003.dat"), nil, 0o644))
}

func dirEntryNames(entries []os.DirEntry) []string {
	return []string{entries[0].Name(), entries[1].Name(), entries[2].Name(), entries[3].Name()}
}

// TestE2E_ReadDir 完整验证目录句柄生命周期、空与嵌套目录以及小批量目录的 RPC 分页遍历。
func TestE2E_ReadDir(t *testing.T) {
	storageClient := newTestStorageClient(t)
	disk := createTestDisk(t, storageClient)
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()
	cmd, stdout, stderr := startBowlMount(t, endpoint, disk.GetDiskId(), mountpoint, "--debug")
	waitForMount(t, mountpoint, "等待挂载就绪失败: %s %s", stdout.String(), stderr.String())
	t.Cleanup(func() { stopBowlMountGracefully(t, cmd, mountpoint) })

	createReadDirTree(t, mountpoint)

	var rootEntriesCount, emptyEntriesCount, l1EntriesCount, l2EntriesCount, bulkEntriesCount int
	var rootIno, parentIno uint64

	t.Run("空目录与多级嵌套目录遍历及Inode重写", func(t *testing.T) {
		rootEntries, err := os.ReadDir(mountpoint)
		require.NoError(t, err, "读取根目录失败")
		rootEntriesCount = len(rootEntries)
		require.Len(t, rootEntries, 3)
		require.Equal(t, []string{"bulk", "empty_dir", "level1"}, []string{rootEntries[0].Name(), rootEntries[1].Name(), rootEntries[2].Name()})

		emptyEntries, err := os.ReadDir(filepath.Join(mountpoint, "empty_dir"))
		require.NoError(t, err, "读取 empty_dir 失败")
		emptyEntriesCount = len(emptyEntries)
		require.Empty(t, emptyEntries, "empty_dir 必须无任何子项")

		l1Entries, err := os.ReadDir(filepath.Join(mountpoint, "level1"))
		require.NoError(t, err)
		l1EntriesCount = len(l1Entries)
		require.Len(t, l1Entries, 1)
		require.Equal(t, "level2", l1Entries[0].Name())
		require.True(t, l1Entries[0].IsDir())

		l2Entries, err := os.ReadDir(filepath.Join(mountpoint, "level1", "level2"))
		require.NoError(t, err)
		l2EntriesCount = len(l2Entries)
		require.Len(t, l2Entries, 1)
		require.Equal(t, "child.txt", l2Entries[0].Name())
		require.False(t, l2Entries[0].IsDir())

		var statDot unix.Stat_t
		require.NoError(t, unix.Stat(filepath.Join(mountpoint, "."), &statDot))
		rootIno = statDot.Ino
		require.Equal(t, uint64(1), rootIno, "根目录 '.' 的 Inode 必须为 1")

		var statL1Parent unix.Stat_t
		require.NoError(t, unix.Stat(filepath.Join(mountpoint, "level1", ".."), &statL1Parent))
		parentIno = statL1Parent.Ino
		require.Equal(t, uint64(1), parentIno, "一级子目录的 '..' 必须映射为内核根节点 1")
	})

	t.Run("小批量目录分页遍历完整性与确定性", func(t *testing.T) {
		bulkPath := filepath.Join(mountpoint, "bulk")
		entries, err := os.ReadDir(bulkPath)
		require.NoError(t, err, "os.ReadDir 读取 bulk 目录失败")
		bulkEntriesCount = len(entries)
		require.Equal(t, bulkFileNames, dirEntryNames(entries), "条目必须无遗漏、无重复且顺序确定")
		require.False(t, slices.ContainsFunc(entries, func(entry os.DirEntry) bool {
			return !entry.Type().IsRegular()
		}), "所有条目类型必须为常规文件")

		// 通过公开 RPC 用小 limit 验证跨页游标：第一页 3 项，第二页补齐剩余 3 项。
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		diskID := disk.GetDiskId()
		lookup, err := storageClient.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{DiskId: diskID, ParentIno: 1, Name: []byte("bulk")}))
		require.NoError(t, err)
		bulkIno := lookup.Msg.GetInode().GetIno()

		first, err := storageClient.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{DiskId: diskID, Ino: bulkIno, Offset: 0, Limit: 3}))
		require.NoError(t, err)
		require.Len(t, first.Msg.Entries, 3)
		require.Equal(t, ".", string(first.Msg.Entries[0].Name))
		require.Equal(t, "..", string(first.Msg.Entries[1].Name))
		require.Equal(t, bulkFileNames[0], string(first.Msg.Entries[2].Name))
		next := first.Msg.Entries[2].GetNextOffset()
		require.Equal(t, uint64(3), next)

		second, err := storageClient.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{DiskId: diskID, Ino: bulkIno, Offset: next, Limit: 4096}))
		require.NoError(t, err)
		require.Len(t, second.Msg.Entries, 3)
		require.Equal(t, []string{bulkFileNames[1], bulkFileNames[2], bulkFileNames[3]}, []string{
			string(second.Msg.Entries[0].Name), string(second.Msg.Entries[1].Name), string(second.Msg.Entries[2].Name),
		})
	})

	recordArtifact(t, map[string]string{
		"root_entries":       strconv.Itoa(rootEntriesCount),
		"empty_dir_entries":  strconv.Itoa(emptyEntriesCount),
		"level1_entries":     strconv.Itoa(l1EntriesCount),
		"level2_entries":     strconv.Itoa(l2EntriesCount),
		"bulk_entries":       strconv.Itoa(bulkEntriesCount),
		"root_ino":           strconv.FormatUint(rootIno, 10),
		"parent_ino_rewrite": strconv.FormatUint(parentIno, 10),
	})
}

// TestE2E_OpenDirectoryAfterRemoval 验证目录在本地打开期间被删除后的同步。
func TestE2E_OpenDirectoryAfterRemoval(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	mountpoint := t.TempDir()
	cmd, stdout, stderr := startBowlMount(t, getStorageEndpoint(), disk.GetDiskId(), mountpoint)
	waitForMount(t, mountpoint, "等待挂载就绪失败: %s %s", stdout.String(), stderr.String())
	t.Cleanup(func() { stopBowlMountGracefully(t, cmd, mountpoint) })
	path := filepath.Join(mountpoint, "open_then_rmdir")
	require.NoError(t, os.Mkdir(path, 0o755))
	fd, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = syscall.Close(fd) })
	second, err := syscall.Open(path, syscall.O_RDONLY|syscall.O_DIRECTORY, 0)
	require.NoError(t, err)
	t.Cleanup(func() { _ = syscall.Close(second) })
	require.NoError(t, os.Remove(path))
	var attr unix.Stat_t
	require.NoError(t, unix.Fstat(fd, &attr), "已删除的打开目录应保留属性")
	require.Zero(t, attr.Nlink)
	require.NoError(t, syscall.Fsync(fd), "已删除的打开目录应能同步")
	require.NoError(t, unix.Fdatasync(fd), "已删除的打开目录应能同步数据")
	_, err = unix.Getdents(fd, make([]byte, 4096))
	require.ErrorIs(t, err, syscall.ENOENT)
	require.NoError(t, syscall.Close(fd))
	require.NoError(t, unix.Fstat(second, &attr))
	require.Zero(t, attr.Nlink)
	require.NoError(t, syscall.Fsync(second))
	require.NoError(t, syscall.Close(second))
	_, err = os.Stat(path)
	require.ErrorIs(t, err, os.ErrNotExist)

	recordArtifact(t, map[string]string{
		"deleted_dir_nlink":     "0",
		"fsync_after_unlink":    "ok",
		"getdents_after_unlink": "ENOENT",
		"path_removed":          "true",
	})
}
