//go:build e2e && linux

package e2e_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

func TestE2E_DiskCLI_FullLifecycleAndSafeDelete(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	bowlBin := getBowlBin()

	// 1. Create: bowl disk create --endpoint <ep> --size 1GiB --name e2e-lifecycle --json
	createCmd := exec.Command(bowlBin, "disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "e2e-lifecycle", "--json")
	createOut, err := createCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk create 失败: %s", string(createOut))

	var created struct {
		DiskID   string `json:"disk_id"`
		Name     string `json:"name"`
		RootIno  uint64 `json:"root_ino"`
		MaxBytes uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal(createOut, &created), "解析 create JSON 失败: %s", string(createOut))
	require.NotEmpty(t, created.DiskID)
	require.Equal(t, "e2e-lifecycle", created.Name)
	require.Equal(t, uint64(1073741824), created.MaxBytes)
	createdMaxBytes := created.MaxBytes
	diskID := created.DiskID

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
	})

	// 2. Get & List
	getCmd := exec.Command(bowlBin, "disk", "get", "--endpoint", endpoint, diskID, "--json")
	getOut, err := getCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk get 失败: %s", string(getOut))

	var got struct {
		DiskID   string `json:"disk_id"`
		Name     string `json:"name"`
		MaxBytes uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal(getOut, &got))
	require.Equal(t, diskID, got.DiskID)
	require.Equal(t, "e2e-lifecycle", got.Name)

	listCmd := exec.Command(bowlBin, "disk", "list", "--endpoint", endpoint, "--limit", "20", "--json")
	listOut, err := listCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk list 失败: %s", string(listOut))

	var listed struct {
		Disks []struct {
			DiskID string `json:"disk_id"`
			Name   string `json:"name"`
		} `json:"disks"`
	}
	require.NoError(t, json.Unmarshal(listOut, &listed))
	require.True(t, slices.ContainsFunc(listed.Disks, func(d struct {
		DiskID string `json:"disk_id"`
		Name   string `json:"name"`
	}) bool {
		return d.DiskID == diskID && d.Name == "e2e-lifecycle"
	}), "bowl disk list 应包含新建的 disk_id %s", diskID)

	// 3. Mount & Write
	mountpoint := t.TempDir()
	mountCmd := exec.Command(bowlBin, "mount", "--endpoint", endpoint, diskID, mountpoint)
	mountOut, err := mountCmd.CombinedOutput()
	require.NoError(t, err, "bowl mount 失败: %s", string(mountOut))

	daemonPID := findBowlDaemonPID(t, mountpoint)

	testFilePath := filepath.Join(mountpoint, "lifecycle_test.bin")
	payload := bytes.Repeat([]byte("BOWL-E2E-LIFECYCLE-DATA-"), 2731) // ~64 KiB
	payload = payload[:65536]
	f, err := os.OpenFile(testFilePath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = f.Write(payload)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	// 4. Update (在线扩容成功 & 缩容低于已用空间被拦截)
	updateCmd := exec.Command(bowlBin, "disk", "update", "--endpoint", endpoint, diskID, "--size", "2GiB", "--name", "e2e-resized", "--json")
	updateOut, err := updateCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk update 在线扩容失败: %s", string(updateOut))

	var updated struct {
		DiskID    string `json:"disk_id"`
		Name      string `json:"name"`
		MaxBytes  uint64 `json:"max_bytes"`
		UsedBytes uint64 `json:"used_bytes"`
	}
	require.NoError(t, json.Unmarshal(updateOut, &updated))
	require.Equal(t, diskID, updated.DiskID)
	require.Equal(t, "e2e-resized", updated.Name)
	require.Equal(t, uint64(2147483648), updated.MaxBytes)
	resizedMaxBytes := updated.MaxBytes
	require.GreaterOrEqual(t, updated.UsedBytes, uint64(65536))

	// 缩容低于已用空间（1B < 64KiB）应被拦截并返回非零退出码
	shrinkCmd := exec.Command(bowlBin, "disk", "update", "--endpoint", endpoint, diskID, "--size", "1B")
	shrinkOut, shrinkErr := shrinkCmd.CombinedOutput()
	require.Error(t, shrinkErr, "缩容至已用空间以下必须被拦截: %s", string(shrinkOut))

	// 5. 本机挂载态删盘拦截：被 /proc/self/mountinfo 快检拦截并提示先执行 bowl umount
	localDelCmd := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, diskID, "-y")
	localDelOut, localDelErr := localDelCmd.CombinedOutput()
	require.Error(t, localDelErr, "本机挂载状态下 bowl disk delete 必须被拦截")
	var exitErr *exec.ExitError
	require.ErrorAs(t, localDelErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(localDelOut), "bowl umount")

	// 验证挂载点内的测试文件依然完好可读
	readBack, err := os.ReadFile(testFilePath)
	require.NoError(t, err)
	require.Equal(t, payload, readBack)
	writtenBytes := len(readBack)

	// 6. 正常卸载（bowl umount）后交互式删盘
	umountCmd := exec.Command(bowlBin, "umount", mountpoint)
	umountOut, err := umountCmd.CombinedOutput()
	require.NoError(t, err, "bowl umount 失败: %s", string(umountOut))
	waitForUnmount(t, mountpoint)
	waitForProcessExit(t, daemonPID)

	// 6a. stdin 输入 n 取消删除：退出码 1，磁盘必须保留。
	cancelCmd := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, diskID)
	cancelCmd.Stdin = strings.NewReader("n\n")
	var cancelStderr bytes.Buffer
	cancelCmd.Stderr = &cancelStderr
	_, cancelErr := cancelCmd.Output()
	require.Error(t, cancelErr, "交互式取消删除应返回非零退出码: %s", cancelStderr.String())
	require.ErrorAs(t, cancelErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, cancelStderr.String(), "操作已取消")
	keptCtx, keptCancel := context.WithTimeout(context.Background(), 5*time.Second)
	kept, keptErr := client.GetDisk(keptCtx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	keptCancel()
	require.NoError(t, keptErr, "交互式取消后磁盘必须保留")
	require.Equal(t, diskID, kept.Msg.GetDisk().GetDiskId())

	// 6b. stdin 输入 y 确认删除：退出码 0，磁盘立即消失。
	delStart := time.Now()
	delCmd := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, diskID, "--json")
	delCmd.Stdin = strings.NewReader("y\n")
	var delStderr bytes.Buffer
	delCmd.Stderr = &delStderr
	delOut, err := delCmd.Output()
	delElapsed := time.Since(delStart)
	require.NoError(t, err, "卸载后立即执行 bowl disk delete 失败: %s", delStderr.String())
	require.Less(t, delElapsed, 2*time.Second, "正常卸载后删盘应在 2s 内完成")

	var delRes struct {
		DiskID  string `json:"disk_id"`
		Deleted bool   `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal(delOut, &delRes))
	require.Equal(t, diskID, delRes.DiskID)
	require.True(t, delRes.Deleted)

	// 再次 get 应返回退出码 1（不存在）
	getAfterDelCmd := exec.Command(bowlBin, "disk", "get", "--endpoint", endpoint, diskID)
	getAfterOut, getAfterErr := getAfterDelCmd.CombinedOutput()
	require.Error(t, getAfterErr)
	require.ErrorAs(t, getAfterErr, &exitErr)
	require.Equal(t, 1, exitErr.ExitCode())
	require.Contains(t, string(getAfterOut), "磁盘不存在")

	// 7. 守护进程被 kill -9 强杀并解除本机挂载后，零等待会话租约 TTL 超时即可立即执行 bowl disk delete 销毁磁盘
	createCmd2 := exec.Command(bowlBin, "disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "e2e-kill9-disk", "-q")
	createOut2, err := createCmd2.CombinedOutput()
	require.NoError(t, err, "创建第二块测试盘失败: %s", string(createOut2))
	disk2ID := string(bytes.TrimSpace(createOut2))
	require.NotEmpty(t, disk2ID)

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk2ID}))
	})

	mountpoint2 := t.TempDir()
	metaPath2 := mountMetaPath(t, mountpoint2)
	fallbackMetaPath2 := filepath.Join(os.TempDir(), "bowl-mounts", filepath.Base(metaPath2))
	t.Cleanup(func() {
		_ = os.Remove(metaPath2)
		_ = os.Remove(fallbackMetaPath2)
	})
	mountCmd2 := exec.Command(bowlBin, "mount", "--endpoint", endpoint, "--admin-addr", "off", disk2ID, mountpoint2)
	mountOut2, err := mountCmd2.CombinedOutput()
	require.NoError(t, err, "挂载第二块测试盘失败: %s", string(mountOut2))

	daemonPID2 := findBowlDaemonPID(t, mountpoint2)

	// 直接发送 SIGKILL (kill -9) 强杀守护进程
	require.NoError(t, syscall.Kill(daemonPID2, syscall.SIGKILL))
	waitForProcessExit(t, daemonPID2)

	// 解除本机残留挂载点并清理强杀遗留的发现元数据文件
	_ = unix.Unmount(mountpoint2, unix.MNT_DETACH)
	waitForUnmount(t, mountpoint2)
	_ = os.Remove(metaPath2)
	_ = os.Remove(fallbackMetaPath2)

	// 无需等待任何会话租约 TTL 超时，单次调用立即执行 bowl disk delete 销毁磁盘
	kill9DelStart := time.Now()
	delCmd2 := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, disk2ID, "-y", "--json")
	delOut2, delErr2 := delCmd2.CombinedOutput()
	kill9DelElapsed := time.Since(kill9DelStart)
	require.NoError(t, delErr2, "kill -9 并解除本机挂载后无需等待 TTL 即可立即删盘: %s", string(delOut2))
	require.Less(t, kill9DelElapsed, 2*time.Second, "kill -9 后删盘必须零 TTL 等待立即完成")

	var delRes2 struct {
		DiskID  string `json:"disk_id"`
		Deleted bool   `json:"deleted"`
	}
	require.NoError(t, json.Unmarshal(delOut2, &delRes2))
	require.Equal(t, disk2ID, delRes2.DiskID)
	require.True(t, delRes2.Deleted)

	getAfterKillDelCtx, getAfterKillDelCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer getAfterKillDelCancel()
	_, getDisk2Err := client.GetDisk(getAfterKillDelCtx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: disk2ID}))
	require.Error(t, getDisk2Err)
	require.Equal(t, connect.CodeNotFound, connect.CodeOf(getDisk2Err))

	recordArtifact(t, map[string]string{
		"created_max_bytes":    strconv.FormatUint(createdMaxBytes, 10),
		"resized_max_bytes":    strconv.FormatUint(resizedMaxBytes, 10),
		"written_bytes":        strconv.Itoa(writtenBytes),
		"shrink_blocked":       strconv.FormatBool(shrinkErr != nil),
		"local_delete_blocked": strconv.FormatBool(localDelErr != nil),
		"interactive_cancel":   "kept",
		"interactive_confirm":  "deleted",
		"unmount_delete":       strconv.FormatBool(delRes.Deleted),
		"kill9_zero_wait_del":  strconv.FormatBool(delRes2.Deleted),
	})
}

type diskListEntry struct {
	DiskID    string `json:"disk_id"`
	UsedBytes uint64 `json:"used_bytes"`
}

type diskListPage struct {
	Disks         []diskListEntry `json:"disks"`
	NextPageToken string          `json:"next_page_token"`
}

// listDiskPage 执行一页 disk list 并解析磁盘与下一页游标。
func listDiskPage(t *testing.T, limit, pageToken string) diskListPage {
	t.Helper()
	cmd := exec.Command(getBowlBin(), "disk", "list", "--endpoint", getStorageEndpoint(), "--json", "--limit", limit, "--page-token", pageToken)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "bowl disk list 分页查询失败: %s", string(out))
	var page diskListPage
	require.NoError(t, json.Unmarshal(out, &page))
	return page
}

// TestE2E_DiskCLI_Pagination 用两块固定磁盘线性验证 --limit 与 --page-token 分页契约。
func TestE2E_DiskCLI_Pagination(t *testing.T) {
	client := newTestStorageClient(t)
	first := createTestDisk(t, client)
	second := createTestDisk(t, client)

	page1 := listDiskPage(t, "1", "")
	require.Len(t, page1.Disks, 1)
	require.NotEmpty(t, page1.NextPageToken, "第一页必须返回 next_page_token")
	page2 := listDiskPage(t, "1", page1.NextPageToken)
	require.Len(t, page2.Disks, 1)
	require.ElementsMatch(t,
		[]string{first.GetDiskId(), second.GetDiskId()},
		[]string{page1.Disks[0].DiskID, page2.Disks[0].DiskID},
		"最新创建的两块磁盘必须恰好占据前两页",
	)
	recordArtifact(t, map[string]string{
		"page1_count":    strconv.Itoa(len(page1.Disks)),
		"page1_has_next": strconv.FormatBool(page1.NextPageToken != ""),
		"page2_count":    strconv.Itoa(len(page2.Disks)),
	})
}

// TestE2E_DiskCLI_OnlineForkDualMountIsolationAndDeleteParent 验证父盘 FUSE 在线挂载写入期间
// 执行 bowl disk fork、父子双挂载点并发读写隔离，以及卸载删除父盘后子盘依然完好可读可写。
func TestE2E_DiskCLI_OnlineForkDualMountIsolationAndDeleteParent(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()
	bowlBin := getBowlBin()

	// 1. 创建父盘并挂载至 parentMnt，写入跨块文件与目录结构。
	createCmd := exec.Command(bowlBin, "disk", "create", "--endpoint", endpoint, "--size", "1GiB", "--name", "e2e-fork-parent", "--json")
	createOut, err := createCmd.CombinedOutput()
	require.NoError(t, err, "创建父盘失败: %s", string(createOut))

	var parentDisk struct {
		DiskID       string `json:"disk_id"`
		ParentDiskID string `json:"parent_disk_id"`
		Name         string `json:"name"`
		MaxBytes     uint64 `json:"max_bytes"`
	}
	require.NoError(t, json.Unmarshal(createOut, &parentDisk))
	require.NotEmpty(t, parentDisk.DiskID)
	require.Empty(t, parentDisk.ParentDiskID)
	parentID := parentDisk.DiskID

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: parentID}))
	})

	parentMnt := t.TempDir()
	mountParentCmd := exec.Command(bowlBin, "mount", "--endpoint", endpoint, parentID, parentMnt)
	mountParentOut, err := mountParentCmd.CombinedOutput()
	require.NoError(t, err, "挂载父盘失败: %s", string(mountParentOut))
	parentDaemonPID := findBowlDaemonPID(t, parentMnt)
	parentMounted := true
	t.Cleanup(func() {
		if parentMounted {
			_ = exec.Command(bowlBin, "umount", parentMnt).Run()
		}
	})

	require.NoError(t, os.MkdirAll(filepath.Join(parentMnt, "app"), 0o755))
	baseConfig := []byte("{\"env\":\"staging\",\"version\":1}\n")
	require.NoError(t, os.WriteFile(filepath.Join(parentMnt, "app", "config.json"), baseConfig, 0o644))

	largePayload := bytes.Repeat([]byte("BOWL-FUSE-ONLINE-FORK-BLOCK-"), 18725) // ~512 KiB
	largePayload = largePayload[:512*1024]
	largeFile, err := os.OpenFile(filepath.Join(parentMnt, "large.bin"), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o644)
	require.NoError(t, err)
	_, err = largeFile.Write(largePayload)
	require.NoError(t, err)
	require.NoError(t, largeFile.Sync())
	require.NoError(t, largeFile.Close())

	// 2. 保持父盘在线挂载，测试缩容拦截后执行在线 bowl disk fork。
	shrinkForkCmd := exec.Command(bowlBin, "disk", "fork", parentID, "--endpoint", endpoint, "--size", "4KiB")
	shrinkForkOut, shrinkForkErr := shrinkForkCmd.CombinedOutput()
	require.Error(t, shrinkForkErr, "在线 Fork 目标配额低于已用空间必须被拦截: %s", string(shrinkForkOut))

	forkCmd := exec.Command(bowlBin, "disk", "fork", parentID, "--endpoint", endpoint, "--name", "e2e-fork-child", "--size", "2GiB", "--json")
	forkOut, err := forkCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk fork 失败: %s", string(forkOut))

	var childDisk struct {
		DiskID       string `json:"disk_id"`
		ParentDiskID string `json:"parent_disk_id"`
		Name         string `json:"name"`
		MaxBytes     uint64 `json:"max_bytes"`
		UsedBytes    uint64 `json:"used_bytes"`
	}
	require.NoError(t, json.Unmarshal(forkOut, &childDisk), "解析 fork JSON 失败: %s", string(forkOut))
	require.NotEmpty(t, childDisk.DiskID)
	require.Equal(t, parentID, childDisk.ParentDiskID)
	require.Equal(t, "e2e-fork-child", childDisk.Name)
	require.Equal(t, uint64(2*1024*1024*1024), childDisk.MaxBytes)
	require.GreaterOrEqual(t, childDisk.UsedBytes, uint64(len(largePayload)))
	childID := childDisk.DiskID

	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, _ = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: childID}))
	})

	// 校验人类可读详情输出包含 Parent Disk 字段。
	getDetailCmd := exec.Command(bowlBin, "disk", "get", "--endpoint", endpoint, childID)
	getDetailOut, err := getDetailCmd.CombinedOutput()
	require.NoError(t, err, "bowl disk get 详情失败: %s", string(getDetailOut))
	require.Contains(t, string(getDetailOut), "Parent Disk:  "+parentID)

	// 3. 将子盘挂载至 childMnt，父子双挂载点同时在线并发读写变更。
	childMnt := t.TempDir()
	mountChildCmd := exec.Command(bowlBin, "mount", "--endpoint", endpoint, childID, childMnt)
	mountChildOut, err := mountChildCmd.CombinedOutput()
	require.NoError(t, err, "挂载子盘失败: %s", string(mountChildOut))
	childDaemonPID := findBowlDaemonPID(t, childMnt)
	childMounted := true
	t.Cleanup(func() {
		if childMounted {
			_ = exec.Command(bowlBin, "umount", childMnt).Run()
		}
	})

	// 先核对子盘初始继承内容与父盘 Fork 时刻完全一致。
	gotChildLarge, err := os.ReadFile(filepath.Join(childMnt, "large.bin"))
	require.NoError(t, err)
	require.Equal(t, largePayload, gotChildLarge)
	gotChildCfg, err := os.ReadFile(filepath.Join(childMnt, "app", "config.json"))
	require.NoError(t, err)
	require.Equal(t, baseConfig, gotChildCfg)

	// 父子双盘各自覆写同路径文件并新增独有文件，验证严格隔离。
	parentNewCfg := []byte("{\"env\":\"production\",\"version\":2}\n")
	childNewCfg := []byte("{\"env\":\"experiment\",\"version\":99}\n")
	require.NoError(t, os.WriteFile(filepath.Join(parentMnt, "app", "config.json"), parentNewCfg, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(parentMnt, "only_parent.txt"), []byte("parent-branch"), 0o644))

	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "app", "config.json"), childNewCfg, 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "only_child.txt"), []byte("child-branch"), 0o644))

	readParentCfg, err := os.ReadFile(filepath.Join(parentMnt, "app", "config.json"))
	require.NoError(t, err)
	require.Equal(t, parentNewCfg, readParentCfg)
	_, err = os.Stat(filepath.Join(parentMnt, "only_child.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	readChildCfg, err := os.ReadFile(filepath.Join(childMnt, "app", "config.json"))
	require.NoError(t, err)
	require.Equal(t, childNewCfg, readChildCfg)
	_, err = os.Stat(filepath.Join(childMnt, "only_parent.txt"))
	require.ErrorIs(t, err, os.ErrNotExist)

	// 4. 卸载并删除父盘，保持子盘挂载不断开，验证子盘继承的数据块与新写文件依然完整可读可写。
	umountParentOut, err := exec.Command(bowlBin, "umount", parentMnt).CombinedOutput()
	require.NoError(t, err, "卸载父盘失败: %s", string(umountParentOut))
	waitForUnmount(t, parentMnt)
	waitForProcessExit(t, parentDaemonPID)
	parentMounted = false

	delParentOut, err := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, parentID, "-y").CombinedOutput()
	require.NoError(t, err, "删除父盘失败: %s", string(delParentOut))

	gotChildLargeAfterParentDel, err := os.ReadFile(filepath.Join(childMnt, "large.bin"))
	require.NoError(t, err)
	require.Equal(t, largePayload, gotChildLargeAfterParentDel, "删除父盘后子盘共享 .pack 数据必须完好可读")

	gotChildCfgAfterParentDel, err := os.ReadFile(filepath.Join(childMnt, "app", "config.json"))
	require.NoError(t, err)
	require.Equal(t, childNewCfg, gotChildCfgAfterParentDel)

	require.NoError(t, os.WriteFile(filepath.Join(childMnt, "after_parent_del.txt"), []byte("still-alive"), 0o644))
	gotAlive, err := os.ReadFile(filepath.Join(childMnt, "after_parent_del.txt"))
	require.NoError(t, err)
	require.Equal(t, []byte("still-alive"), gotAlive)

	// 5. 正常卸载并删除子盘。
	umountChildOut, err := exec.Command(bowlBin, "umount", childMnt).CombinedOutput()
	require.NoError(t, err, "卸载子盘失败: %s", string(umountChildOut))
	waitForUnmount(t, childMnt)
	waitForProcessExit(t, childDaemonPID)
	childMounted = false

	delChildOut, err := exec.Command(bowlBin, "disk", "delete", "--endpoint", endpoint, childID, "-y").CombinedOutput()
	require.NoError(t, err, "删除子盘失败: %s", string(delChildOut))
}
