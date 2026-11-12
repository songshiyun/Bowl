//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const modeRegular0644 uint32 = 0o100644

// TestControlPlaneDiskQuotaAndPagination 验证单盘控制面 CreateDisk/GetDisk/ListDisks 游标分页与非法游标拒绝、
// UpdateDisk 配额水位边界与在线实时生效、以及 DeleteDisk 回收与重复删除错误码契约。
func TestControlPlaneDiskQuotaAndPagination(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	// 1. 创建两块独立测试盘，验证初始元数据与 ListDisks 游标分页及非法游标拦截。
	createdA, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "ctrl-quota-alpha",
		MaxBytes: 4096,
	}))
	require.NoError(t, err)
	diskA := createdA.Msg.GetDisk()
	diskAID := diskA.GetDiskId()
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskAID}))
	})

	createdB, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "ctrl-quota-beta",
		MaxBytes: 8192,
	}))
	require.NoError(t, err)
	diskB := createdB.Msg.GetDisk()
	diskBID := diskB.GetDiskId()
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, deleteErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskBID}))
		require.NoError(t, deleteErr)
	})

	gotA, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskAID}))
	require.NoError(t, err)
	require.Equal(t, uint64(1), gotA.Msg.GetDisk().GetRootIno())
	require.Equal(t, uint64(4096), gotA.Msg.GetDisk().GetMaxBytes())
	require.Zero(t, gotA.Msg.GetDisk().GetUsedBytes())
	require.Equal(t, uint64(1), gotA.Msg.GetDisk().GetUsedInodes())

	page1, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{PageSize: 1}))
	require.NoError(t, err)
	require.Len(t, page1.Msg.GetDisks(), 1)
	require.NotEmpty(t, page1.Msg.GetNextPageToken())

	page2, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  1,
		PageToken: page1.Msg.GetNextPageToken(),
	}))
	require.NoError(t, err)
	require.Len(t, page2.Msg.GetDisks(), 1)
	require.ElementsMatch(t,
		[]string{diskAID, diskBID},
		[]string{page1.Msg.GetDisks()[0].GetDiskId(), page2.Msg.GetDisks()[0].GetDiskId()},
	)

	_, badBase64Err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  2,
		PageToken: "%%%not-base64%%%",
	}))
	testutil.RequireConnectError(t, badBase64Err, connect.CodeInvalidArgument, "EINVAL")

	_, emptyJSONTokenErr := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  2,
		PageToken: "e30",
	}))
	testutil.RequireConnectError(t, emptyJSONTokenErr, connect.CodeInvalidArgument, "EINVAL")
	invalidTokenReason := testutil.ExtractErrorInfo(emptyJSONTokenErr).GetReason()

	// 2. 挂载 diskA 并写入 1024B，验证 UpdateDisk 改名、缩容水位保护与原子性、满水位 ENOSPC 及在线扩容立即生效。
	mounted, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskAID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch := mounted.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch)

	fileResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskAID,
		Epoch:     epoch,
		ParentIno: 1,
		Name:      []byte("quota.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	fileIno := fileResp.Msg.GetInode().GetIno()

	seedData := bytes.Repeat([]byte{0x5a}, 1024)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskAID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 0,
		Data:   seedData,
	}))
	require.NoError(t, err)

	renamedResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId: diskAID,
		Name:   new("ctrl-quota-renamed"),
	}))
	require.NoError(t, err)
	require.Equal(t, "ctrl-quota-renamed", renamedResp.Msg.GetDisk().GetName())
	require.Equal(t, uint64(4096), renamedResp.Msg.GetDisk().GetMaxBytes())
	require.Equal(t, uint64(1024), renamedResp.Msg.GetDisk().GetUsedBytes())

	// 缩容低于 UsedBytes (512 < 1024) 必须返回 CodeFailedPrecondition (EINVAL)，且不得污染磁盘名称。
	_, shrinkBelowErr := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskAID,
		Name:     new("polluted-name"),
		MaxBytes: new(uint64(512)),
	}))
	testutil.RequireConnectError(t, shrinkBelowErr, connect.CodeFailedPrecondition, "EINVAL")
	shrinkBelowReason := testutil.ExtractErrorInfo(shrinkBelowErr).GetReason()

	afterFailedShrink, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskAID}))
	require.NoError(t, err)
	require.Equal(t, "ctrl-quota-renamed", afterFailedShrink.Msg.GetDisk().GetName())
	require.Equal(t, uint64(4096), afterFailedShrink.Msg.GetDisk().GetMaxBytes())

	// 缩容至恰好等于 UsedBytes (1024 == 1024) 成功，AvailBytes 归零，继续追加写入或预分配返回 ENOSPC。
	exactShrinkResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskAID,
		MaxBytes: new(uint64(1024)),
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1024), exactShrinkResp.Msg.GetDisk().GetMaxBytes())

	statAtExact, err := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{
		DiskId: diskAID,
		Ino:    1,
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(1024), statAtExact.Msg.GetTotalBytes())
	require.Equal(t, uint64(1024), statAtExact.Msg.GetUsedBytes())
	require.Zero(t, statAtExact.Msg.GetAvailBytes())

	_, writeNoSpaceErr := client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskAID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 1024,
		Data:   []byte("x"),
	}))
	testutil.RequireConnectError(t, writeNoSpaceErr, connect.CodeResourceExhausted, "ENOSPC")
	writeNoSpaceReason := testutil.ExtractErrorInfo(writeNoSpaceErr).GetReason()

	_, fallocNoSpaceErr := client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		DiskId: diskAID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 1024,
		Length: 4096,
	}))
	testutil.RequireConnectError(t, fallocNoSpaceErr, connect.CodeResourceExhausted, "ENOSPC")

	// 在线扩容至 8192B 后无需重新挂载即可立即追加写入。
	expandedResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   diskAID,
		MaxBytes: new(uint64(8192)),
	}))
	require.NoError(t, err)
	require.Equal(t, uint64(8192), expandedResp.Msg.GetDisk().GetMaxBytes())

	appendData := bytes.Repeat([]byte{0x6b}, 512)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskAID,
		Epoch:  epoch,
		Ino:    fileIno,
		Offset: 1024,
		Data:   appendData,
	}))
	require.NoError(t, err)

	afterExpandWrite, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskAID}))
	require.NoError(t, err)
	finalUsedBytes := afterExpandWrite.Msg.GetDisk().GetUsedBytes()
	require.Equal(t, uint64(1536), finalUsedBytes)

	// 3. DeleteDisk 回收后，GetDisk 与重复 DeleteDisk 均返回 CodeNotFound (ENOENT)。
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskAID}))
	require.NoError(t, err)

	_, getDeletedErr := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskAID}))
	testutil.RequireConnectError(t, getDeletedErr, connect.CodeNotFound, "ENOENT")

	_, repeatDeleteErr := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskAID}))
	testutil.RequireConnectError(t, repeatDeleteErr, connect.CodeNotFound, "ENOENT")
	repeatDeleteReason := testutil.ExtractErrorInfo(repeatDeleteErr).GetReason()

	recordArtifact(t, map[string]string{
		"page1_count":          strconv.Itoa(len(page1.Msg.GetDisks())),
		"page2_count":          strconv.Itoa(len(page2.Msg.GetDisks())),
		"invalid_page_token":   invalidTokenReason,
		"renamed_disk_name":    afterFailedShrink.Msg.GetDisk().GetName(),
		"shrink_below_used":    shrinkBelowReason,
		"exact_shrink_avail":   strconv.FormatUint(statAtExact.Msg.GetAvailBytes(), 10),
		"full_quota_write":     writeNoSpaceReason,
		"expanded_max_bytes":   strconv.FormatUint(expandedResp.Msg.GetDisk().GetMaxBytes(), 10),
		"final_used_bytes":     strconv.FormatUint(finalUsedBytes, 10),
		"repeat_delete_reason": repeatDeleteReason,
	})
}

// TestSingleDiskEpochFencingOrphanSweepAndRPCs 验证单盘 MountDisk 世代号抢占与遗留孤儿自动清扫、
// 全部 16 个写 RPC 过期世代围栏、FUSE 未直接调用的 ReadDir 分页续读契约以及 h2c 并发 4MiB 大块读写往返。
func TestSingleDiskEpochFencingOrphanSweepAndRPCs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	created, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "single-disk-epoch-orphan",
		MaxBytes: 32 * 1024 * 1024,
	}))
	require.NoError(t, err)
	disk := created.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	require.Equal(t, uint64(1), rootIno)
	diskID := disk.GetDiskId()
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		_, deleteErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
		require.NoError(t, deleteErr)
	})

	// 1. 只读挂载返回 Epoch = 0 且不推进世代号；首次可写挂载分配 Epoch = 1。
	mountROInitial, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, mountROInitial.Msg.GetEpoch())

	mount1, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch1 := mount1.Msg.GetEpoch()
	require.Equal(t, uint64(1), epoch1)

	mountROAfterRW, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: true,
	}))
	require.NoError(t, err)
	require.Zero(t, mountROAfterRW.Msg.GetEpoch())

	// 2. 在 Epoch = 1 下构建目录与文件，覆盖 FUSE 客户端未直接调用的 ReadDir 基本流分页续读契约。
	subDir, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("sub"),
		Mode:      0o755,
	}))
	require.NoError(t, err)
	subDirIno := subDir.Msg.GetInode().GetIno()

	keepFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("keep.txt"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	keepIno := keepFile.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    keepIno,
		Data:   []byte("survivor"),
	}))
	require.NoError(t, err)
	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    keepIno,
		Name:   []byte("user.keep"),
		Value:  []byte("v1"),
	}))
	require.NoError(t, err)

	readDirPage1, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		DiskId: diskID,
		Ino:    1,
		Offset: 0,
		Limit:  2,
	}))
	require.NoError(t, err)
	require.Len(t, readDirPage1.Msg.GetEntries(), 2)
	require.Equal(t, []byte("."), readDirPage1.Msg.GetEntries()[0].GetName())
	require.Equal(t, []byte(".."), readDirPage1.Msg.GetEntries()[1].GetName())

	cursorAfterDots := readDirPage1.Msg.GetEntries()[1].GetNextOffset()
	readDirPage2, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		DiskId: diskID,
		Ino:    1,
		Offset: cursorAfterDots,
		Limit:  10,
	}))
	require.NoError(t, err)
	require.Len(t, readDirPage2.Msg.GetEntries(), 2)
	page2Names := []string{
		string(readDirPage2.Msg.GetEntries()[0].GetName()),
		string(readDirPage2.Msg.GetEntries()[1].GetName()),
	}
	slices.Sort(page2Names)
	require.Equal(t, []string{"keep.txt", "sub"}, page2Names)

	readDirEOF, err := client.ReadDir(ctx, connect.NewRequest(&storagev1.ReadDirRequest{
		DiskId: diskID,
		Ino:    1,
		Offset: readDirPage2.Msg.GetEntries()[1].GetNextOffset(),
		Limit:  10,
	}))
	require.NoError(t, err)
	require.Empty(t, readDirEOF.Msg.GetEntries())

	// 3a. 显式孤儿节点生命周期：Unlink(OpenInClient: true) 仍可读，ReclaimOrphan 后返回 ENOENT。
	explicitOrphan, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("explicit_orphan"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	explicitIno := explicitOrphan.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    explicitIno,
		Data:   []byte("hello"),
	}))
	require.NoError(t, err)
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:       diskID,
		Epoch:        epoch1,
		ParentIno:    1,
		Name:         []byte("explicit_orphan"),
		OpenInClient: new(true),
	}))
	require.NoError(t, err)
	explicitRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    explicitIno,
		Size:   5,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("hello"), explicitRead.Msg.GetData())

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    explicitIno,
	}))
	require.NoError(t, err)
	_, explicitAfterErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    explicitIno,
		Size:   5,
	}))
	testutil.RequireConnectError(t, explicitAfterErr, connect.CodeNotFound, "ENOENT")
	explicitReclaimReason := testutil.ExtractErrorInfo(explicitAfterErr).GetReason()

	// 3b. 崩溃遗留孤儿（Unlink 与 Rename 覆盖打开目标，均不调用 ReclaimOrphan）：待下一世代 MountDisk 自动清扫。
	unlinkedLeak, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("unlinked_leak"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	unlinkedLeakIno := unlinkedLeak.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    unlinkedLeakIno,
		Data:   []byte("orphan-unlink-16"),
	}))
	require.NoError(t, err)
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:       diskID,
		Epoch:        epoch1,
		ParentIno:    1,
		Name:         []byte("unlinked_leak"),
		OpenInClient: new(true),
	}))
	require.NoError(t, err)

	renameSrc, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("rename_src"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    renameSrc.Msg.GetInode().GetIno(),
		Data:   []byte("src4"),
	}))
	require.NoError(t, err)

	renameVictim, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch1,
		ParentIno: 1,
		Name:      []byte("rename_victim"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	renameVictimIno := renameVictim.Msg.GetInode().GetIno()
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID,
		Epoch:  epoch1,
		Ino:    renameVictimIno,
		Data:   []byte("orphan-rename-16"),
	}))
	require.NoError(t, err)

	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		DiskId:             diskID,
		Epoch:              epoch1,
		OldParentIno:       1,
		OldName:            []byte("rename_src"),
		NewParentIno:       1,
		NewName:            []byte("rename_victim"),
		TargetOpenInClient: new(true),
	}))
	require.NoError(t, err)

	beforeSweepDisk, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	usedBeforeSweep := beforeSweepDisk.Msg.GetDisk().GetUsedBytes()
	// survivor(8) + src4(4) + 两个未回收孤儿(16 + 16) = 44B
	require.Equal(t, uint64(44), usedBeforeSweep)

	// 4. 新可写挂载推进 Epoch = 2：自动清扫两个遗留孤儿并释放 32B 配额。
	mount2, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   diskID,
		ReadOnly: false,
	}))
	require.NoError(t, err)
	epoch2 := mount2.Msg.GetEpoch()
	require.Equal(t, uint64(2), epoch2)

	afterSweepDisk, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{DiskId: diskID}))
	require.NoError(t, err)
	usedAfterSweep := afterSweepDisk.Msg.GetDisk().GetUsedBytes()
	// 仅保留 survivor(8) + rename_victim 新内容 src4(4) = 12B
	require.Equal(t, uint64(12), usedAfterSweep)

	_, sweptReadErr := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    unlinkedLeakIno,
		Size:   16,
	}))
	testutil.RequireConnectError(t, sweptReadErr, connect.CodeNotFound, "ENOENT")
	_, sweptAttrErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
		DiskId: diskID,
		Ino:    renameVictimIno,
	}))
	testutil.RequireConnectError(t, sweptAttrErr, connect.CodeNotFound, "ENOENT")
	sweptOrphanReason := testutil.ExtractErrorInfo(sweptAttrErr).GetReason()

	// 5. 验证旧客户端只读 RPC 正常，而全部 16 个写 RPC 携带过期 Epoch = 1 均被围栏拦截返回 EBUSY。
	survivingRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: diskID,
		Ino:    keepIno,
		Size:   8,
	}))
	require.NoError(t, err)
	require.Equal(t, []byte("survivor"), survivingRead.Msg.GetData())

	assertAll16WriteRPCsFenced(t, ctx, client, diskID, epoch1, keepIno, subDirIno)

	// 6. HTTP/2 h2c 单连接 W*=4 并发 4 MiB 大块读写往返（总计 16 MiB），验证服务端流控窗口与最大块完整性。
	h2cClient := testutil.NewH2CStorageClient(testutil.GetStorageServiceAddr())
	bigFile, err := h2cClient.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    diskID,
		Epoch:     epoch2,
		ParentIno: 1,
		Name:      []byte("h2c_16m.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	bigIno := bigFile.Msg.GetInode().GetIno()

	const chunkBytes = 4 * 1024 * 1024
	chunk0 := bytes.Repeat([]byte{0x11, 0xa1, 0x3c, 0xc3}, chunkBytes/4)
	chunk1 := bytes.Repeat([]byte{0x22, 0xb2, 0x5a, 0xa5}, chunkBytes/4)
	chunk2 := bytes.Repeat([]byte{0x33, 0xc3, 0x69, 0x96}, chunkBytes/4)
	chunk3 := bytes.Repeat([]byte{0x44, 0xd4, 0x7e, 0xe7}, chunkBytes/4)

	var (
		wg             sync.WaitGroup
		wResp0, wResp1 *connect.Response[storagev1.WriteResponse]
		wResp2, wResp3 *connect.Response[storagev1.WriteResponse]
		wErr0, wErr1   error
		wErr2, wErr3   error
	)
	wg.Go(func() {
		wResp0, wErr0 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID, Epoch: epoch2, Ino: bigIno, Offset: 0 * chunkBytes, Data: chunk0,
		}))
	})
	wg.Go(func() {
		wResp1, wErr1 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID, Epoch: epoch2, Ino: bigIno, Offset: 1 * chunkBytes, Data: chunk1,
		}))
	})
	wg.Go(func() {
		wResp2, wErr2 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID, Epoch: epoch2, Ino: bigIno, Offset: 2 * chunkBytes, Data: chunk2,
		}))
	})
	wg.Go(func() {
		wResp3, wErr3 = h2cClient.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
			DiskId: diskID, Epoch: epoch2, Ino: bigIno, Offset: 3 * chunkBytes, Data: chunk3,
		}))
	})
	wg.Wait()

	require.NoError(t, wErr0)
	require.NoError(t, wErr1)
	require.NoError(t, wErr2)
	require.NoError(t, wErr3)
	require.Equal(t, uint32(chunkBytes), wResp0.Msg.GetSize())
	require.Equal(t, uint32(chunkBytes), wResp1.Msg.GetSize())
	require.Equal(t, uint32(chunkBytes), wResp2.Msg.GetSize())
	require.Equal(t, uint32(chunkBytes), wResp3.Msg.GetSize())

	r0, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: diskID, Ino: bigIno, Offset: 0 * chunkBytes, Size: chunkBytes}))
	require.NoError(t, err)
	r1, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: diskID, Ino: bigIno, Offset: 1 * chunkBytes, Size: chunkBytes}))
	require.NoError(t, err)
	r2, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: diskID, Ino: bigIno, Offset: 2 * chunkBytes, Size: chunkBytes}))
	require.NoError(t, err)
	r3, err := h2cClient.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{DiskId: diskID, Ino: bigIno, Offset: 3 * chunkBytes, Size: chunkBytes}))
	require.NoError(t, err)

	require.Equal(t, chunk0, r0.Msg.GetData())
	require.Equal(t, chunk1, r1.Msg.GetData())
	require.Equal(t, chunk2, r2.Msg.GetData())
	require.Equal(t, chunk3, r3.Msg.GetData())

	combined16M := slices.Concat(r0.Msg.GetData(), r1.Msg.GetData(), r2.Msg.GetData(), r3.Msg.GetData())

	recordArtifact(t, map[string]string{
		"root_ino":               strconv.FormatUint(rootIno, 10),
		"epoch_1":                strconv.FormatUint(epoch1, 10),
		"epoch_2":                strconv.FormatUint(epoch2, 10),
		"readdir_page1_entries":  strconv.Itoa(len(readDirPage1.Msg.GetEntries())),
		"readdir_page2_entries":  strconv.Itoa(len(readDirPage2.Msg.GetEntries())),
		"explicit_reclaim":       explicitReclaimReason,
		"used_before_sweep":      strconv.FormatUint(usedBeforeSweep, 10),
		"used_after_sweep":       strconv.FormatUint(usedAfterSweep, 10),
		"swept_orphan_reason":    sweptOrphanReason,
		"fenced_write_rpc_count": "16",
		"h2c_total_bytes":        strconv.Itoa(len(combined16M)),
		"h2c_16m_sha256":         fileSHA256(combined16M),
	})
}

// assertAll16WriteRPCsFenced 平铺断言全部 16 个变更/写 RPC 在携带过期世代号时均返回 CodeFailedPrecondition (EBUSY)。
func assertAll16WriteRPCsFenced(
	t *testing.T,
	ctx context.Context,
	client storagev1connect.StorageServiceClient,
	diskID string,
	staleEpoch, fileIno, dirIno uint64,
) {
	t.Helper()

	_, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
		DiskId: diskID, Epoch: staleEpoch, ParentIno: 1, Name: []byte("fenced_nod"), Mode: modeRegular0644,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
		DiskId: diskID, Epoch: staleEpoch, ParentIno: 1, Name: []byte("fenced_dir"), Mode: 0o755,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
		DiskId: diskID, Epoch: staleEpoch, ParentIno: 1, Name: []byte("fenced_sym"), Target: []byte("keep.txt"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Link(ctx, connect.NewRequest(&storagev1.LinkRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, NewParentIno: 1, NewName: []byte("fenced_hard"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId: diskID, Epoch: staleEpoch, ParentIno: 1, Name: []byte("keep.txt"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
		DiskId: diskID, Epoch: staleEpoch, ParentIno: 1, Name: []byte("sub"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Rename(ctx, connect.NewRequest(&storagev1.RenameRequest{
		DiskId: diskID, Epoch: staleEpoch, OldParentIno: 1, OldName: []byte("keep.txt"), NewParentIno: 1, NewName: []byte("keep_moved.txt"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: diskID, Epoch: staleEpoch, ParentIno: 1, Name: []byte("fenced_create"), Mode: 0o644,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, Offset: 0, Data: []byte("x"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, Mode: new(uint32(0o600)),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Fallocate(ctx, connect.NewRequest(&storagev1.FallocateRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, Offset: 0, Length: 128,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.SetXattr(ctx, connect.NewRequest(&storagev1.SetXattrRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, Name: []byte("user.fenced"), Value: []byte("v"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.RemoveXattr(ctx, connect.NewRequest(&storagev1.RemoveXattrRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, Name: []byte("user.keep"),
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno, Fh: 1,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.FsyncDir(ctx, connect.NewRequest(&storagev1.FsyncDirRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: dirIno, Fh: 1,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")

	_, err = client.ReclaimOrphan(ctx, connect.NewRequest(&storagev1.ReclaimOrphanRequest{
		DiskId: diskID, Epoch: staleEpoch, Ino: fileIno,
	}))
	testutil.RequireConnectError(t, err, connect.CodeFailedPrecondition, "EBUSY")
}

// TestForkDiskOnlineIsolationOrphanSanitizeAndLineageDelete 验证真实 PostgreSQL + Storage 容器下
// 在线无感 ForkDisk、孤儿净化配额水位线、级联 Fork DAG 及同血缘 |D|>=2 / |D|==1 / |D|==0 删盘回收契约。
func TestForkDiskOnlineIsolationOrphanSanitizeAndLineageDelete(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	client := testutil.NewStorageClient()

	createdParent, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-fork-parent",
		MaxBytes: 16 << 20,
	}))
	require.NoError(t, err)
	parentID := createdParent.Msg.GetDisk().GetDiskId()
	require.Nil(t, createdParent.Msg.GetDisk().ParentDiskId)

	var createdDiskIDs []string
	createdDiskIDs = append(createdDiskIDs, parentID)
	t.Cleanup(func() {
		cleanupCtx, stop := context.WithTimeout(context.Background(), 15*time.Second)
		defer stop()
		for _, id := range createdDiskIDs {
			_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: id}))
		}
	})

	// 1. 校验 ForkDisk 错误码契约（ENOENT / EINVAL / ENAMETOOLONG）。
	_, notFoundErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: "018f0000-0000-7000-8000-000000009999",
		Name:         "missing-src",
	}))
	testutil.RequireConnectError(t, notFoundErr, connect.CodeNotFound, "ENOENT")

	_, nilUUIDErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: "00000000-0000-0000-0000-000000000000",
		Name:         "nil-src",
	}))
	testutil.RequireConnectError(t, nilUUIDErr, connect.CodeInvalidArgument, "EINVAL")

	_, longNameErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: parentID,
		Name:         string(bytes.Repeat([]byte("n"), 256)),
	}))
	testutil.RequireConnectError(t, longNameErr, connect.CodeInvalidArgument, "ENAMETOOLONG")

	// 2. 挂载父盘，写入 300 KiB 常规文件（跨 256 KiB Blob 边界）与 256 KiB 孤儿文件。
	parentMount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: parentID,
	}))
	require.NoError(t, err)
	parentEpoch := parentMount.Msg.GetEpoch()
	require.Equal(t, uint64(1), parentEpoch)

	sharedCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    parentID,
		Epoch:     parentEpoch,
		ParentIno: 1,
		Name:      []byte("shared.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	sharedIno := sharedCreate.Msg.GetInode().GetIno()

	sharedPayload := bytes.Repeat([]byte("FORK-BASE-DATA-"), 20480) // 307,200 B (300 KiB)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: parentID,
		Epoch:  parentEpoch,
		Ino:    sharedIno,
		Offset: 0,
		Data:   sharedPayload,
	}))
	require.NoError(t, err)

	orphanCreate, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId:    parentID,
		Epoch:     parentEpoch,
		ParentIno: 1,
		Name:      []byte("orphan.bin"),
		Mode:      0o644,
	}))
	require.NoError(t, err)
	orphanIno := orphanCreate.Msg.GetInode().GetIno()
	orphanPayload := bytes.Repeat([]byte{0xEE}, 256*1024)
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: parentID,
		Epoch:  parentEpoch,
		Ino:    orphanIno,
		Offset: 0,
		Data:   orphanPayload,
	}))
	require.NoError(t, err)
	_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
		DiskId:       parentID,
		Epoch:        parentEpoch,
		ParentIno:    1,
		Name:         []byte("orphan.bin"),
		OpenInClient: new(true),
	}))
	require.NoError(t, err)

	// 2a. 缩容至低于净化后已用空间（100 KiB < 300 KiB）必须返回 FailedPrecondition (EINVAL)。
	_, shrinkErr := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: parentID,
		Name:         "too-small-fork",
		MaxBytes:     new(uint64(100 * 1024)),
	}))
	testutil.RequireConnectError(t, shrinkErr, connect.CodeFailedPrecondition, "EINVAL")

	// 2b. 指定 MaxBytes = 512 KiB（介于净化后 300 KiB 与源盘含孤儿 556 KiB 之间），在线无感派生 child1。
	child1Resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: parentID,
		Name:         "e2e-fork-child-1",
		MaxBytes:     new(uint64(512 * 1024)),
	}))
	require.NoError(t, err)
	child1 := child1Resp.Msg.GetDisk()
	child1ID := child1.GetDiskId()
	createdDiskIDs = append(createdDiskIDs, child1ID)
	require.NotNil(t, child1.ParentDiskId)
	require.Equal(t, parentID, child1.GetParentDiskId())
	require.Equal(t, uint64(512*1024), child1.GetMaxBytes())
	require.Equal(t, uint64(len(sharedPayload)), child1.GetUsedBytes())
	require.Equal(t, uint64(2), child1.GetUsedInodes())

	// 3. 父盘原挂载 Epoch=1 零中断继续覆写，且父盘孤儿句柄仍可读。
	parentMutated := []byte("PARENT-NEW-PAYLOAD-AFTER-ONLINE-FORK")
	_, err = client.Write(ctx, connect.NewRequest(&storagev1.WriteRequest{
		DiskId: parentID,
		Epoch:  parentEpoch,
		Ino:    sharedIno,
		Offset: 0,
		Data:   parentMutated,
	}))
	require.NoError(t, err)
	_, err = client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
		DiskId: parentID,
		Epoch:  parentEpoch,
		Ino:    sharedIno,
		Size:   new(uint64(len(parentMutated))),
	}))
	require.NoError(t, err)
	orphanRead, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: parentID,
		Ino:    orphanIno,
		Offset: 0,
		Size:   uint32(len(orphanPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, orphanPayload, orphanRead.Msg.GetData())

	// 4. 子盘首次可写挂载返回 Epoch=1，读取 shared.bin 保持 Fork 时刻 300 KiB 数据，并写入新文件验证号段防撞。
	child1Mount, err := client.MountDisk(ctx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId: child1ID,
	}))
	require.NoError(t, err)
	child1Epoch := child1Mount.Msg.GetEpoch()
	require.Equal(t, uint64(1), child1Epoch)

	child1Lookup, err := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
		DiskId:    child1ID,
		ParentIno: 1,
		Name:      []byte("shared.bin"),
	}))
	require.NoError(t, err)
	child1Read, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: child1ID,
		Ino:    child1Lookup.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   uint32(len(sharedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload, child1Read.Msg.GetData())

	parentNewFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: parentID, Epoch: parentEpoch, ParentIno: 1, Name: []byte("p-new.txt"), Mode: 0o644,
	}))
	require.NoError(t, err)
	childNewFile, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		DiskId: child1ID, Epoch: child1Epoch, ParentIno: 1, Name: []byte("c-new.txt"), Mode: 0o644,
	}))
	require.NoError(t, err)
	require.NotEqual(t, parentNewFile.Msg.GetInode().GetIno(), childNewFile.Msg.GetInode().GetIno(), "同源派生分支新分配 Inode 严禁冲突")

	// 5. 二次 Fork 与级联 Fork，随后按 |D|>=2 -> |D|==1 -> |D|==0 顺序删盘，验证 PG 外键与切面回收。
	child2Resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: parentID,
		Name:         "e2e-fork-child-2",
	}))
	require.NoError(t, err)
	child2ID := child2Resp.Msg.GetDisk().GetDiskId()
	createdDiskIDs = append(createdDiskIDs, child2ID)
	require.Equal(t, uint64(16<<20), child2Resp.Msg.GetDisk().GetMaxBytes())

	grandResp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: child1ID,
		Name:         "e2e-fork-grandchild",
	}))
	require.NoError(t, err)
	grandID := grandResp.Msg.GetDisk().GetDiskId()
	createdDiskIDs = append(createdDiskIDs, grandID)
	require.Equal(t, child1ID, grandResp.Msg.GetDisk().GetParentDiskId())

	// 删除父盘（|D| == 3 >= 2），所有子孙分支完好可读。
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: parentID}))
	require.NoError(t, err)

	// 删除 child2 与 grandChild，使同血缘仅剩唯一幸存盘 child1（|D| == 1，自动清空切面 DAG）。
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: child2ID}))
	require.NoError(t, err)
	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: grandID}))
	require.NoError(t, err)

	// 唯一幸存盘 child1 仍可正常读取与再次删除（|D| == 0）。
	child1ReadAfterGC, err := client.Read(ctx, connect.NewRequest(&storagev1.ReadRequest{
		DiskId: child1ID,
		Ino:    child1Lookup.Msg.GetInode().GetIno(),
		Offset: 0,
		Size:   uint32(len(sharedPayload)),
	}))
	require.NoError(t, err)
	require.Equal(t, sharedPayload, child1ReadAfterGC.Msg.GetData())

	_, err = client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: child1ID}))
	require.NoError(t, err)
}
