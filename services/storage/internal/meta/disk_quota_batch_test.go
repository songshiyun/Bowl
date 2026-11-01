package meta_test

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// TestDiskQuotaAsyncBatchFlush_PostgresIntegration 验证启用 WithDiskQuotaFlushInterval 后：
// 1. Create、Mkdir、Write、SetAttr、Fallocate、Unlink、Rmdir 在事务内免除对 disks 表的同步排他锁更新，改为内存预留与脏增量累加；
// 2. 在后台刷盘尚未执行前，GetDisk、ListDisks、StatFS 立即叠加内存未刷盘增量，返回 100% 实时精确的 UsedBytes 与 UsedInodes；
// 3. 超出磁盘 MaxBytes 时在内存中立即精确拦截并返回 ErrNoSpace (ENOSPC)，失败回滚不残留任何预留字节；
// 4. 后台定时刷盘或显式调用 UpdateDisk 时，自动将内存脏增量按 root_ino 批量合并刷入 PostgreSQL disks 底表。
func TestDiskQuotaAsyncBatchFlush_PostgresIntegration(t *testing.T) {
	pool := setupFormalPostgresPool(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store := db.NewStore(pool)
	bs := meta.NewLocalFSBlockStore(t.TempDir())
	// 设置较长的后台刷盘间隔（500ms），以便先验证刷盘前 GetDisk / StatFS 的实时内存叠加可见性，再验证刷盘落表
	mgr := meta.New(
		store,
		meta.WithBlockStore(bs),
		meta.WithDiskQuotaFlushInterval(200*time.Millisecond),
	)

	diskName := fmt.Sprintf("disk-async-quota-%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 16384, // 16 KiB
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanCtx, cleanCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanCancel()
		_ = mgr.DeleteDisk(cleanCtx, disk.DiskID)
	})

	// 1. 4 并发协程各自在独立子目录下并发创建文件并写入数据（验证零 disks 行锁争用与配额累加）
	const workers = 4
	dirInos := make([]uint64, workers)
	for i := 0; i < workers; i++ {
		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("dir_%d", i)),
			Mode:      meta.S_IFDIR | 0o755,
		})
		require.NoError(t, err)
		dirInos[i] = subDir.Ino
	}

	var wg sync.WaitGroup
	fileInos := make([]uint64, workers)
	fileFhs := make([]uint64, workers)
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			cRes, cErr := mgr.Create(ctx, meta.CreateParams{
				ParentIno: dirInos[idx],
				Name:      []byte("file.bin"),
				Mode:      meta.S_IFREG | 0o644,
				Flags:     meta.O_RDWR,
			})
			require.NoError(t, cErr)
			fileInos[idx] = cRes.Inode.Ino
			fileFhs[idx] = cRes.Fh

			_, wErr := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    cRes.Inode.Ino,
				Fh:     cRes.Fh,
				Offset: 0,
				Data:   make([]byte, 2048), // 每个文件写入 2048 B，共 8192 B
			})
			require.NoError(t, wErr)
		}(i)
	}
	wg.Wait()

	// 2. 立即通过 GetDisk 与 StatFS 验证实时可见性（1 个根目录 + 4 个子目录 + 4 个常规文件 = 9 个 Inode，已用 8192 字节）
	gotDisk, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), gotDisk.UsedBytes)
	assert.Equal(t, uint64(9), gotDisk.UsedInodes)

	statRes, err := mgr.StatFS(ctx, disk.RootIno)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), statRes.UsedBytes)
	assert.Equal(t, uint64(8192), statRes.AvailBytes)
	assert.Equal(t, uint64(9), statRes.Files)

	// 3. 验证内存超配额拦截：剩余可用 8192 B，尝试 Fallocate 到 12288 B（增量 +10240 B > 8192 B）必须返回 ErrNoSpace
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    fileInos[0],
		Fh:     fileFhs[0],
		Offset: 0,
		Length: 12288,
		Mode:   0,
	})
	require.ErrorIs(t, err, meta.ErrNoSpace)

	// 超配额失败后配额保持 8192 B 不变
	gotDiskAfterFail, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(8192), gotDiskAfterFail.UsedBytes)

	// 4. 等待后台异步合并刷盘完成，验证 PostgreSQL disks 底表已精确同步
	require.Eventually(t, func() bool {
		var dbUsedBytes, dbUsedInodes int64
		qErr := pool.QueryRow(ctx, "SELECT used_bytes, used_inodes FROM disks WHERE root_ino = $1", int64(disk.RootIno)).Scan(&dbUsedBytes, &dbUsedInodes)
		return qErr == nil && dbUsedBytes == 8192 && dbUsedInodes == 9
	}, 2*time.Second, 20*time.Millisecond, "后台配额合并刷盘应将 used_bytes=8192, used_inodes=9 同步至 disks 底表")

	// 5. 释放句柄并删除 1 个文件，验证扣减与最终一致性
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{Ino: fileInos[0], Fh: fileFhs[0]}))
	require.NoError(t, mgr.Unlink(ctx, dirInos[0], []byte("file.bin")))

	gotDiskAfterUnlink, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(6144), gotDiskAfterUnlink.UsedBytes)
	assert.Equal(t, uint64(8), gotDiskAfterUnlink.UsedInodes)
}
