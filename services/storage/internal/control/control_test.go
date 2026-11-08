package control_test

import (
	"context"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/control"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type failingCreateDiskStore struct {
	db.Store
}

func (f *failingCreateDiskStore) ExecTx(_ context.Context, _ func(db.Querier) error) error {
	return errors.New("simulated pg insert failure")
}

func (f *failingCreateDiskStore) CreateDisk(_ context.Context, _ db.CreateDiskParams) (db.Disk, error) {
	return db.Disk{}, errors.New("simulated pg insert failure")
}

func newTestControl(t *testing.T, prefix string) (control.Manager, db.Store, string, context.Context) {
	t.Helper()
	pool, ctx := setupIsolatedTestDB(t, prefix)
	dbStore := db.NewStore(pool)
	root := t.TempDir()
	mgr := control.New(dbStore, control.WithStorageRoot(root))
	t.Cleanup(func() {
		require.NoError(t, mgr.Close())
	})
	return mgr, dbStore, root, ctx
}

func TestCreateAndGetDisk(t *testing.T) {
	mgr, _, root, ctx := newTestControl(t, "ctrl_create_get")

	disk, err := mgr.CreateDisk(ctx, control.CreateDiskParams{
		Name:     "workspace-a",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	require.NotEmpty(t, disk.DiskID)
	require.Equal(t, "workspace-a", disk.Name)
	require.Equal(t, uint64(1), disk.RootIno)
	require.Equal(t, uint64(64<<20), disk.MaxBytes)
	require.Zero(t, disk.UsedBytes)
	require.Equal(t, uint64(1), disk.UsedInodes)
	require.False(t, disk.CreatedAt.IsZero())
	require.False(t, disk.UpdatedAt.IsZero())

	diskDir := filepath.Join(root, "disks", disk.DiskID)
	info, err := os.Stat(diskDir)
	require.NoError(t, err)
	require.True(t, info.IsDir())

	got, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, disk.DiskID, got.DiskID)
	require.Equal(t, "workspace-a", got.Name)
	require.Equal(t, uint64(1), got.RootIno)
	require.Equal(t, uint64(64<<20), got.MaxBytes)
	require.Zero(t, got.UsedBytes)
	require.Equal(t, uint64(1), got.UsedInodes)
}

func TestCreateAndGetDiskValidationAndRollback(t *testing.T) {
	mgr, baseStore, root, ctx := newTestControl(t, "ctrl_create_val")

	_, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "zero", MaxBytes: 0})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "overflow", MaxBytes: uint64(math.MaxInt64) + 1})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.CreateDisk(ctx, control.CreateDiskParams{Name: strings.Repeat("a", 256), MaxBytes: 4096})
	require.ErrorIs(t, err, control.ErrNameTooLong)

	_, err = mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "bad\x00name", MaxBytes: 4096})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.GetDisk(ctx, "")
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.GetDisk(ctx, "not-a-uuid")
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.GetDisk(ctx, "00000000-0000-0000-0000-000000000000")
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.GetDisk(ctx, "018f0000-0000-7000-8000-000000000099")
	require.ErrorIs(t, err, control.ErrNotFound)

	// PG 插入失败时必须关闭 StoreActor 并清理本地磁盘目录
	failRoot := filepath.Join(root, "fail-case")
	failMgr := control.New(&failingCreateDiskStore{Store: baseStore}, control.WithStorageRoot(failRoot))
	defer func() { require.NoError(t, failMgr.Close()) }()

	_, err = failMgr.CreateDisk(ctx, control.CreateDiskParams{Name: "rollback-disk", MaxBytes: 4096})
	require.Error(t, err)
	entries, readErr := os.ReadDir(filepath.Join(failRoot, "disks"))
	if readErr == nil {
		require.Empty(t, entries, "PG 插入失败后不得残留本地磁盘子目录")
	} else {
		require.ErrorIs(t, readErr, os.ErrNotExist)
	}
	_ = pgtype.UUID{}
}

func TestListDisksCursorPaginationAndValidation(t *testing.T) {
	mgr, _, _, ctx := newTestControl(t, "ctrl_list_disks")

	// 空列表分页
	emptyRes, err := mgr.ListDisks(ctx, control.ListDisksParams{})
	require.NoError(t, err)
	require.Empty(t, emptyRes.Disks)
	require.Empty(t, emptyRes.NextPageToken)

	// 创建 3 个盘，不同配额
	d1, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "d1", MaxBytes: 1 << 20})
	require.NoError(t, err)
	d2, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "d2", MaxBytes: 2 << 20})
	require.NoError(t, err)
	d3, err := mgr.CreateDisk(ctx, control.CreateDiskParams{Name: "d3", MaxBytes: 3 << 20})
	require.NoError(t, err)

	// 按 created_at DESC, disk_id DESC 分页读取（每页 2 条）
	page1, err := mgr.ListDisks(ctx, control.ListDisksParams{PageSize: 2})
	require.NoError(t, err)
	require.Len(t, page1.Disks, 2)
	require.NotEmpty(t, page1.NextPageToken)
	require.Equal(t, d3.DiskID, page1.Disks[0].DiskID)
	require.Equal(t, uint64(3<<20), page1.Disks[0].MaxBytes)
	require.Equal(t, d2.DiskID, page1.Disks[1].DiskID)
	require.Equal(t, uint64(2<<20), page1.Disks[1].MaxBytes)

	page2, err := mgr.ListDisks(ctx, control.ListDisksParams{
		PageSize:  2,
		PageToken: page1.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.Disks, 1)
	require.Empty(t, page2.NextPageToken)
	require.Equal(t, d1.DiskID, page2.Disks[0].DiskID)
	require.Equal(t, uint64(1<<20), page2.Disks[0].MaxBytes)

	// 非法参数与游标令牌校验
	_, err = mgr.ListDisks(ctx, control.ListDisksParams{PageSize: 1001})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.ListDisks(ctx, control.ListDisksParams{PageToken: "%%%not-base64%%%"})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.ListDisks(ctx, control.ListDisksParams{PageToken: "e30"}) // "{}"
	require.ErrorIs(t, err, control.ErrInvalidArgument)
}

func TestMountDiskAndActorLazyPool(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "ctrl_mount_actor")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	mgr1 := control.New(dbStore,
		control.WithStorageRoot(root),
		control.WithEntryTimeout(2.5),
		control.WithAttrTimeout(3.5),
	)

	disk, err := mgr1.CreateDisk(ctx, control.CreateDiskParams{
		Name:     "mount-target",
		MaxBytes: 1 << 20,
	})
	require.NoError(t, err)

	// 只读挂载：返回 Epoch = 0 且不推进 WriterEpoch
	roMount, err := mgr1.MountDisk(ctx, control.MountDiskParams{
		DiskID:   disk.DiskID,
		ReadOnly: true,
	})
	require.NoError(t, err)
	require.Zero(t, roMount.Epoch)
	require.Equal(t, uint64(1), roMount.RootAttr.Ino)
	require.Equal(t, store.S_IFDIR|uint32(0o755), roMount.RootAttr.Mode)
	require.InDelta(t, 2.5, roMount.EntryTimeoutSeconds, 1e-6)
	require.InDelta(t, 3.5, roMount.AttrTimeoutSeconds, 1e-6)

	actor1, err := mgr1.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Zero(t, actor1.WriterEpoch())

	// 可写挂载 1：原子递增 WriterEpoch = 1
	rwMount1, err := mgr1.MountDisk(ctx, control.MountDiskParams{
		DiskID:   disk.DiskID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1), rwMount1.Epoch)
	require.Equal(t, uint64(1), rwMount1.RootAttr.Ino)

	// 在 epoch=1 下创建文件并解链为孤儿节点 (openInClient=true)
	createdAttr, err := actor1.Mknod(store.MknodReq{
		ParentIno: 1,
		Name:      []byte("orphan.bin"),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     rwMount1.Epoch,
	})
	require.NoError(t, err)
	_, _, err = actor1.Write(createdAttr.Ino, 0, []byte("orphan-payload"), rwMount1.Epoch)
	require.NoError(t, err)
	require.NoError(t, actor1.Unlink(1, []byte("orphan.bin"), true, rwMount1.Epoch))

	// 此时孤儿仍占用配额与 Inode
	statBefore, err := actor1.StatFS()
	require.NoError(t, err)
	require.Equal(t, uint64(14), statBefore.UsedBytes)
	require.Equal(t, uint64(2), statBefore.Files)

	// 可写挂载 2：原子递增 WriterEpoch = 2 并自动清扫孤儿节点
	rwMount2, err := mgr1.MountDisk(ctx, control.MountDiskParams{
		DiskID:   disk.DiskID,
		ReadOnly: false,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(2), rwMount2.Epoch)

	statAfter, err := actor1.StatFS()
	require.NoError(t, err)
	require.Zero(t, statAfter.UsedBytes)
	require.Equal(t, uint64(1), statAfter.Files)

	// 旧 epoch=1 被栅栏拦截
	_, err = actor1.Mkdir(store.MkdirReq{
		ParentIno: 1,
		Name:      []byte("stale-dir"),
		Mode:      0o755,
		Epoch:     rwMount1.Epoch,
	})
	require.ErrorIs(t, err, store.ErrStaleEpoch)

	// 关闭 mgr1 后用新实例 mgr2 验证 Actor(ctx, diskID) 从磁盘懒加载打开并复用实例池
	require.NoError(t, mgr1.Close())

	mgr2 := control.New(dbStore, control.WithStorageRoot(root))
	defer func() { require.NoError(t, mgr2.Close()) }()

	lazyActorA, err := mgr2.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	lazyActorB, err := mgr2.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Same(t, lazyActorA, lazyActorB, "同一 diskID 必须复用实例池中的 StoreActor")
	require.Equal(t, uint64(2), lazyActorA.WriterEpoch())

	// 非法或不存在 diskID 校验
	_, err = mgr2.Actor(ctx, "invalid-uuid")
	require.ErrorIs(t, err, control.ErrInvalidArgument)
	_, err = mgr2.Actor(ctx, "018f0000-0000-7000-8000-000000000099")
	require.ErrorIs(t, err, control.ErrNotFound)
}

func TestUpdateDiskNameExpandAndShrinkWaterline(t *testing.T) {
	mgr, _, _, ctx := newTestControl(t, "ctrl_update_disk")

	disk, err := mgr.CreateDisk(ctx, control.CreateDiskParams{
		Name:     "initial-name",
		MaxBytes: 4096,
	})
	require.NoError(t, err)

	mount, err := mgr.MountDisk(ctx, control.MountDiskParams{
		DiskID:   disk.DiskID,
		ReadOnly: false,
	})
	require.NoError(t, err)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	fAttr, err := actor.Create(store.CreateReq{
		ParentIno: 1,
		Name:      []byte("data.txt"),
		Mode:      0o644,
		Epoch:     mount.Epoch,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(fAttr.Ino, 0, make([]byte, 1024), mount.Epoch)
	require.NoError(t, err)

	// 1. 仅改名
	newName := "renamed-disk"
	updated, err := mgr.UpdateDisk(ctx, control.UpdateDiskParams{
		DiskID: disk.DiskID,
		Name:   &newName,
	})
	require.NoError(t, err)
	require.Equal(t, "renamed-disk", updated.Name)
	require.Equal(t, uint64(4096), updated.MaxBytes)
	require.Equal(t, uint64(1024), updated.UsedBytes)
	require.Equal(t, uint64(2), updated.UsedInodes)

	// 2. 仅扩容
	expandedBytes := uint64(8192)
	updated, err = mgr.UpdateDisk(ctx, control.UpdateDiskParams{
		DiskID:   disk.DiskID,
		MaxBytes: &expandedBytes,
	})
	require.NoError(t, err)
	require.Equal(t, "renamed-disk", updated.Name)
	require.Equal(t, uint64(8192), updated.MaxBytes)

	// 3. 安全缩容至 >= LogicalBytes (1024)
	shrunkBytes := uint64(1024)
	updated, err = mgr.UpdateDisk(ctx, control.UpdateDiskParams{
		DiskID:   disk.DiskID,
		MaxBytes: &shrunkBytes,
	})
	require.NoError(t, err)
	require.Equal(t, uint64(1024), updated.MaxBytes)

	// 4. 缩容低于 LogicalBytes (512 < 1024) 被拦截，且不得污染名称
	tooSmall := uint64(512)
	untouchedName := "should-not-apply"
	_, err = mgr.UpdateDisk(ctx, control.UpdateDiskParams{
		DiskID:   disk.DiskID,
		Name:     &untouchedName,
		MaxBytes: &tooSmall,
	})
	require.ErrorIs(t, err, store.ErrQuotaShrinkBelowUsed)
	require.ErrorIs(t, err, control.ErrInvalidShrink)

	gotAfterFailedShrink, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, "renamed-disk", gotAfterFailedShrink.Name)
	require.Equal(t, uint64(1024), gotAfterFailedShrink.MaxBytes)

	// 5. 参数校验与不存在磁盘
	_, err = mgr.UpdateDisk(ctx, control.UpdateDiskParams{DiskID: disk.DiskID})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	zero := uint64(0)
	_, err = mgr.UpdateDisk(ctx, control.UpdateDiskParams{DiskID: disk.DiskID, MaxBytes: &zero})
	require.ErrorIs(t, err, control.ErrInvalidArgument)

	_, err = mgr.UpdateDisk(ctx, control.UpdateDiskParams{
		DiskID: "018f0000-0000-7000-8000-000000000099",
		Name:   &newName,
	})
	require.ErrorIs(t, err, control.ErrNotFound)
}

func TestDeleteDiskCleansPGPoolAndPhysicalDir(t *testing.T) {
	mgr, dbStore, root, ctx := newTestControl(t, "ctrl_delete_disk")

	disk, err := mgr.CreateDisk(ctx, control.CreateDiskParams{
		Name:     "victim-disk",
		MaxBytes: 1 << 20,
	})
	require.NoError(t, err)

	mount, err := mgr.MountDisk(ctx, control.MountDiskParams{
		DiskID:   disk.DiskID,
		ReadOnly: false,
	})
	require.NoError(t, err)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)
	fAttr, err := actor.Create(store.CreateReq{
		ParentIno: 1,
		Name:      []byte("payload.dat"),
		Mode:      0o644,
		Epoch:     mount.Epoch,
	})
	require.NoError(t, err)
	_, _, err = actor.Write(fAttr.Ino, 0, []byte("some-data"), mount.Epoch)
	require.NoError(t, err)

	diskDir := filepath.Join(root, "disks", disk.DiskID)
	_, err = os.Stat(diskDir)
	require.NoError(t, err)

	// 无需等待任何会话租约，直接删除磁盘
	require.NoError(t, mgr.DeleteDisk(ctx, disk.DiskID))

	// 1. PG 记录已删除
	var pgID pgtype.UUID
	require.NoError(t, pgID.Scan(disk.DiskID))
	_, err = dbStore.GetDisk(ctx, pgID)
	require.ErrorIs(t, err, pgx.ErrNoRows)

	// 2. Actor 已关闭且从池中摘除
	_, err = actor.StatFS()
	require.ErrorIs(t, err, store.ErrClosed)
	_, err = mgr.Actor(ctx, disk.DiskID)
	require.ErrorIs(t, err, control.ErrNotFound)

	// 3. 本地物理目录已彻底清理
	_, err = os.Stat(diskDir)
	require.ErrorIs(t, err, os.ErrNotExist)

	// 4. 重复删除返回 NotFound，非法 UUID 返回 InvalidArgument
	require.ErrorIs(t, mgr.DeleteDisk(ctx, disk.DiskID), control.ErrNotFound)
	require.ErrorIs(t, mgr.DeleteDisk(ctx, "bad-uuid"), control.ErrInvalidArgument)
}

func TestFxModuleLifecycleAndConcurrentLazyOpen(t *testing.T) {
	pool, ctx := setupIsolatedTestDB(t, "ctrl_fx_lifecycle")
	dbStore := db.NewStore(pool)
	root := t.TempDir()

	metaCfg := config.MetaConfig{
		DataDirVal:                root,
		AtimeModeVal:              "relatime",
		EntryTimeoutSecondsVal:    1.25,
		AttrTimeoutSecondsVal:     2.25,
		DirEntryTimeoutSecondsVal: 3.25,
		OpenKeepCacheVal:          true,
		OpenDirCacheVal:           true,
	}

	var mgr control.Manager
	app := fx.New(
		fx.NopLogger,
		fx.Supply(fx.Annotate(dbStore, fx.As(new(db.Store)))),
		fx.Supply(metaCfg),
		control.Module,
		fx.Populate(&mgr),
	)
	require.NoError(t, app.Start(ctx))

	require.Equal(t, root, mgr.StorageRoot())
	require.Equal(t, control.AtimeModeRelAtime, mgr.AtimeMode())
	require.InDelta(t, 1.25, mgr.EntryTimeoutSeconds(), 1e-6)
	require.InDelta(t, 2.25, mgr.AttrTimeoutSeconds(), 1e-6)
	require.InDelta(t, 3.25, mgr.DirEntryTimeoutSeconds(), 1e-6)
	require.InDelta(t, 3.25, mgr.EntryTimeoutForMode(store.S_IFDIR|0o755), 1e-6)
	require.InDelta(t, 1.25, mgr.EntryTimeoutForMode(store.S_IFREG|0o644), 1e-6)
	require.True(t, mgr.OpenKeepCache())
	require.True(t, mgr.OpenDirCache())

	disk, err := mgr.CreateDisk(ctx, control.CreateDiskParams{
		Name:     "fx-disk",
		MaxBytes: 1 << 20,
	})
	require.NoError(t, err)

	actor, err := mgr.Actor(ctx, disk.DiskID)
	require.NoError(t, err)

	// OnStop 优雅关闭所有 StoreActor 与共享 Cache
	require.NoError(t, app.Stop(ctx))
	_, err = actor.StatFS()
	require.ErrorIs(t, err, store.ErrClosed)

	// 新建未预热实例，测试并发 Actor(ctx, diskID) 单盘级互斥锁不会重复开库
	mgr2 := control.New(dbStore, control.WithStorageRoot(root))
	defer func() { require.NoError(t, mgr2.Close()) }()

	const workers = 8
	var wg sync.WaitGroup
	actors := make([]*store.StoreActor, workers)
	errs := make([]error, workers)
	start := make(chan struct{})
	for i := range workers {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			<-start
			actors[idx], errs[idx] = mgr2.Actor(ctx, disk.DiskID)
		}(i)
	}
	close(start)
	wg.Wait()
	for i := range workers {
		require.NoError(t, errs[i])
		require.Same(t, actors[0], actors[i])
	}
}
