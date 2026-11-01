package meta_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func TestSliceBlocks(t *testing.T) {
	const fourMiB = 4 * 1024 * 1024

	t.Run("0 字节返回空切片", func(t *testing.T) {
		blocks := meta.SliceBlocks(0)
		assert.Empty(t, blocks)
	})

	t.Run("小于 4 MiB 的单个尾块", func(t *testing.T) {
		blocks := meta.SliceBlocks(1024)
		require.Len(t, blocks, 1)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(1024), blocks[0].BlockSize)
	})

	t.Run("恰好 4 MiB 单个整块", func(t *testing.T) {
		blocks := meta.SliceBlocks(fourMiB)
		require.Len(t, blocks, 1)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
	})

	t.Run("4 MiB + 1 字节切分为 1 整块 + 1 尾块", func(t *testing.T) {
		blocks := meta.SliceBlocks(fourMiB + 1)
		require.Len(t, blocks, 2)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
		assert.Equal(t, uint32(1), blocks[1].BlockIdx)
		assert.Equal(t, uint32(1), blocks[1].BlockSize)
	})

	t.Run("8 MiB 切分为 2 个整块", func(t *testing.T) {
		blocks := meta.SliceBlocks(2 * fourMiB)
		require.Len(t, blocks, 2)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
		assert.Equal(t, uint32(1), blocks[1].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[1].BlockSize)
	})

	t.Run("9 MiB 切分为 2 个整块 + 1 尾块", func(t *testing.T) {
		blocks := meta.SliceBlocks(2*fourMiB + 1024*1024)
		require.Len(t, blocks, 3)
		assert.Equal(t, uint32(0), blocks[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[0].BlockSize)
		assert.Equal(t, uint32(1), blocks[1].BlockIdx)
		assert.Equal(t, uint32(fourMiB), blocks[1].BlockSize)
		assert.Equal(t, uint32(2), blocks[2].BlockIdx)
		assert.Equal(t, uint32(1024*1024), blocks[2].BlockSize)
	})
}

func TestLocateBlocks(t *testing.T) {
	const fourMiB = 4 * 1024 * 1024

	t.Run("单块内局部读取", func(t *testing.T) {
		locs, err := meta.LocateBlocks(1000, 100, 500)
		require.NoError(t, err)
		require.Len(t, locs, 1)
		assert.Equal(t, uint32(0), locs[0].BlockIdx)
		assert.Equal(t, uint32(1000), locs[0].BlockSize)
		assert.Equal(t, uint32(100), locs[0].BlockOff)
		assert.Equal(t, uint32(500), locs[0].Length)
	})

	t.Run("跨两块边界读取", func(t *testing.T) {
		sliceSize := uint32(5 * 1024 * 1024) // 5 MiB (block0=4MiB, block1=1MiB)
		sliceOff := uint32(fourMiB - 100)
		reqLen := uint32(250)

		locs, err := meta.LocateBlocks(sliceSize, sliceOff, reqLen)
		require.NoError(t, err)
		require.Len(t, locs, 2)

		// 块 0：读末尾 100 字节
		assert.Equal(t, uint32(0), locs[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), locs[0].BlockSize)
		assert.Equal(t, uint32(fourMiB-100), locs[0].BlockOff)
		assert.Equal(t, uint32(100), locs[0].Length)

		// 块 1：读头部 150 字节
		assert.Equal(t, uint32(1), locs[1].BlockIdx)
		assert.Equal(t, uint32(1024*1024), locs[1].BlockSize)
		assert.Equal(t, uint32(0), locs[1].BlockOff)
		assert.Equal(t, uint32(150), locs[1].Length)
	})

	t.Run("跨多块全量读取", func(t *testing.T) {
		sliceSize := uint32(9 * 1024 * 1024)
		locs, err := meta.LocateBlocks(sliceSize, 0, sliceSize)
		require.NoError(t, err)
		require.Len(t, locs, 3)

		assert.Equal(t, uint32(0), locs[0].BlockIdx)
		assert.Equal(t, uint32(fourMiB), locs[0].BlockSize)
		assert.Equal(t, uint32(0), locs[0].BlockOff)
		assert.Equal(t, uint32(fourMiB), locs[0].Length)

		assert.Equal(t, uint32(1), locs[1].BlockIdx)
		assert.Equal(t, uint32(fourMiB), locs[1].BlockSize)
		assert.Equal(t, uint32(0), locs[1].BlockOff)
		assert.Equal(t, uint32(fourMiB), locs[1].Length)

		assert.Equal(t, uint32(2), locs[2].BlockIdx)
		assert.Equal(t, uint32(1024*1024), locs[2].BlockSize)
		assert.Equal(t, uint32(0), locs[2].BlockOff)
		assert.Equal(t, uint32(1024*1024), locs[2].Length)
	})

	t.Run("reqLen 为 0 返回空切片", func(t *testing.T) {
		locs, err := meta.LocateBlocks(1000, 100, 0)
		require.NoError(t, err)
		assert.Empty(t, locs)
	})

	t.Run("请求超出 sliceSize 报错 ErrInvalidArgument", func(t *testing.T) {
		_, err := meta.LocateBlocks(1000, 800, 300)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
	})

	t.Run("起始偏移超出 sliceSize 报错 ErrInvalidArgument", func(t *testing.T) {
		_, err := meta.LocateBlocks(1000, 1001, 10)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
	})
}

func TestLocalFSBlockStore_Lifecycle(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	sliceID := uint64(42)
	blockIdx := uint32(0)
	testData := []byte("hello nvme block storage!")
	blockSize := uint32(len(testData))

	t.Run("PutBlock 原子写入并落盘", func(t *testing.T) {
		err := store.PutBlock(ctx, sliceID, blockIdx, testData)
		require.NoError(t, err)

		// 验证文件路径格式：<data_dir>/chunks/%02X/%v/<slice_id>_<block_idx>_<block_size>
		// 42 % 256 = 42 = 0x2A; 42 / 1000 / 1000 = 0
		expectedPath := filepath.Join(dataDir, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", blockSize))
		content, err := os.ReadFile(expectedPath)
		require.NoError(t, err)
		assert.Equal(t, testData, content)
	})

	t.Run("GetBlock 全量读取", func(t *testing.T) {
		dst := make([]byte, blockSize)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, 0, dst)
		require.NoError(t, err)
		assert.Equal(t, testData, dst)
	})

	t.Run("GetBlock 分段偏移读取", func(t *testing.T) {
		offset := uint32(6)
		readLen := uint32(4) // "nvme"
		dst := make([]byte, readLen)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
		require.NoError(t, err)
		assert.Equal(t, []byte("nvme"), dst)
	})

	t.Run("DeleteBlock 物理删除", func(t *testing.T) {
		err := store.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
		require.NoError(t, err)

		expectedPath := filepath.Join(dataDir, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", blockSize))
		_, err = os.Stat(expectedPath)
		assert.True(t, os.IsNotExist(err))
	})

	t.Run("DeleteBlock 幂等删除不存在的文件成功", func(t *testing.T) {
		err := store.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
		assert.NoError(t, err)
	})
}

func TestLocalFSBlockStore_LargeBlock(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	// 模拟 4 MiB 整块写入与读取
	const blockSize = 4 * 1024 * 1024
	largeData := make([]byte, blockSize)
	_, err := rand.Read(largeData)
	require.NoError(t, err)

	sliceID := uint64(1000000) // 1000000 % 256 = 64 (0x40), 1000000 / 1000 / 1000 = 1
	blockIdx := uint32(0)

	err = store.PutBlock(ctx, sliceID, blockIdx, largeData)
	require.NoError(t, err)

	expectedPath := filepath.Join(dataDir, "chunks", "40", "1", fmt.Sprintf("1000000_0_%d", blockSize))
	stat, err := os.Stat(expectedPath)
	require.NoError(t, err)
	assert.Equal(t, int64(blockSize), stat.Size())

	// 块内末尾部分读取
	offset := uint32(blockSize - 1024)
	dst := make([]byte, 1024)
	err = store.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
	require.NoError(t, err)
	assert.True(t, bytes.Equal(largeData[offset:], dst))
}

func TestLocalFSBlockStore_Errors(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	t.Run("文件不存在时 GetBlock 返回包含 ErrIO 的错误", func(t *testing.T) {
		dst := make([]byte, 100)
		err := store.GetBlock(ctx, 9999, 0, 100, 0, dst)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIO), "expected ErrIO but got: %v", err)
	})

	t.Run("读取超出块大小时 GetBlock 返回包含 ErrIO 的错误", func(t *testing.T) {
		sliceID := uint64(88)
		blockIdx := uint32(0)
		data := []byte("short")
		blockSize := uint32(len(data))

		require.NoError(t, store.PutBlock(ctx, sliceID, blockIdx, data))

		// offset + len > blockSize
		dst := make([]byte, 10)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, 0, dst)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIO))
	})

	t.Run("物理文件长度短于预期时 GetBlock 返回包含 ErrIO 的错误", func(t *testing.T) {
		sliceID := uint64(89)
		blockIdx := uint32(0)
		blockSize := uint32(100)

		// 手工创建一个只有 10 字节的文件但声明 blockSize 为 100
		dir := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", sliceID%256), fmt.Sprintf("%d", sliceID/1000/1000))
		require.NoError(t, os.MkdirAll(dir, 0o755))
		filePath := filepath.Join(dir, fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize))
		require.NoError(t, os.WriteFile(filePath, []byte("short_data"), 0o644))

		dst := make([]byte, 50)
		err := store.GetBlock(ctx, sliceID, blockIdx, blockSize, 0, dst)
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIO))
	})

	t.Run("Context 取消时操作立即返回错误", func(t *testing.T) {
		canceledCtx, cancel := context.WithCancel(context.Background())
		cancel()

		err := store.PutBlock(canceledCtx, 1, 0, []byte("test"))
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))

		dst := make([]byte, 10)
		err = store.GetBlock(canceledCtx, 1, 0, 10, 0, dst)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))

		err = store.DeleteBlock(canceledCtx, 1, 0, 10)
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	})
}

func TestLocalFSBlockStore_ConcurrentWrites(t *testing.T) {
	dataDir := t.TempDir()
	store := meta.NewLocalFSBlockStore(dataDir)
	ctx := context.Background()

	const concurrency = 20
	var wg sync.WaitGroup
	wg.Add(concurrency)

	for i := 0; i < concurrency; i++ {
		go func(idx int) {
			defer wg.Done()
			sliceID := uint64(idx + 100)
			data := fmt.Appendf(nil, "payload-from-goroutine-%d", idx)
			blockSize := uint32(len(data))

			err := store.PutBlock(ctx, sliceID, 0, data)
			assert.NoError(t, err)

			dst := make([]byte, blockSize)
			err = store.GetBlock(ctx, sliceID, 0, blockSize, 0, dst)
			assert.NoError(t, err)
			assert.Equal(t, data, dst)
		}(i)
	}

	wg.Wait()
}

type managerAccessor interface {
	BlockStore() meta.BlockStore
}

func TestManager_BlockStoreInjection(t *testing.T) {
	t.Run("meta.New 不传 WithBlockStore 默认向后兼容", func(t *testing.T) {
		mgr := meta.New(nil)
		require.NotNil(t, mgr)
		acc, ok := mgr.(managerAccessor)
		require.True(t, ok)
		assert.Nil(t, acc.BlockStore())
	})

	t.Run("meta.New 显式注入 BlockStore 生效", func(t *testing.T) {
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(nil, meta.WithBlockStore(mockBS))
		require.NotNil(t, mgr)
		acc, ok := mgr.(managerAccessor)
		require.True(t, ok)
		assert.Equal(t, mockBS, acc.BlockStore())
	})

	t.Run("Fx Module 注入自定义 BlockStore 生效", func(t *testing.T) {
		mockBS := metamock.NewMockBlockStore(t)
		mockStore := dbmock.NewMockStore(t)
		var extractedMgr meta.Manager

		app := fxtest.New(
			t,
			fx.Provide(
				func() db.Store { return mockStore },
				func() meta.BlockStore { return mockBS },
				func() config.MetaConfig {
					return config.MetaConfig{
						DataDirVal: t.TempDir(),
					}
				},
			),
			meta.Module,
			fx.Populate(&extractedMgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, extractedMgr)

		acc, ok := extractedMgr.(managerAccessor)
		require.True(t, ok)
		assert.Equal(t, mockBS, acc.BlockStore())
	})

	t.Run("Fx Module 未提供 BlockStore 时根据 MetaConfig 自动构造默认 LocalFSBlockStore", func(t *testing.T) {
		tempDir := t.TempDir()
		mockStore := dbmock.NewMockStore(t)
		var extractedMgr meta.Manager

		app := fxtest.New(
			t,
			fx.Provide(
				func() db.Store { return mockStore },
				func() config.MetaConfig {
					return config.MetaConfig{
						DataDirVal: tempDir,
					}
				},
			),
			meta.Module,
			fx.Populate(&extractedMgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, extractedMgr)

		acc, ok := extractedMgr.(managerAccessor)
		require.True(t, ok)
		require.NotNil(t, acc.BlockStore())
		bs, isLocalFS := acc.BlockStore().(*meta.LocalFSBlockStore)
		assert.True(t, isLocalFS)

		ctx := context.Background()
		probeData := []byte("datadir-probe")
		err := bs.PutBlock(ctx, 42, 0, probeData)
		require.NoError(t, err)

		// 验证数据确实落盘在 tempDir 对应的分片路径下，而非默认目录或其它路径
		expectedPath := filepath.Join(tempDir, "chunks", "2A", "0", "42_0_13")
		content, err := os.ReadFile(expectedPath)
		require.NoError(t, err)
		assert.Equal(t, probeData, content)

		// 构造第二个独立 tempDir2 的 Manager 实例，验证不同 DataDir 配置下物理落盘目录完全隔离互不干扰
		tempDir2 := t.TempDir()
		mockStore2 := dbmock.NewMockStore(t)
		var extractedMgr2 meta.Manager

		app2 := fxtest.New(
			t,
			fx.Provide(
				func() db.Store { return mockStore2 },
				func() config.MetaConfig {
					return config.MetaConfig{
						DataDirVal: tempDir2,
					}
				},
			),
			meta.Module,
			fx.Populate(&extractedMgr2),
		)
		require.NoError(t, app2.Err())
		require.NotNil(t, extractedMgr2)

		acc2, ok := extractedMgr2.(managerAccessor)
		require.True(t, ok)
		require.NotNil(t, acc2.BlockStore())
		bs2, ok := acc2.BlockStore().(*meta.LocalFSBlockStore)
		require.True(t, ok)

		probeData2 := []byte("datadir-probe-isolated-2")
		err = bs2.PutBlock(ctx, 42, 0, probeData2)
		require.NoError(t, err)

		expectedPath2 := filepath.Join(tempDir2, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", len(probeData2)))
		content2, err := os.ReadFile(expectedPath2)
		require.NoError(t, err)
		assert.Equal(t, probeData2, content2)

		// 确保 tempDir 与 tempDir2 互不干扰：tempDir 内的文件保持不变，tempDir2 的文件不在 tempDir 中
		origContent, err := os.ReadFile(expectedPath)
		require.NoError(t, err)
		assert.Equal(t, probeData, origContent)

		_, statErr := os.Stat(filepath.Join(tempDir, "chunks", "2A", "0", fmt.Sprintf("42_0_%d", len(probeData2))))
		assert.True(t, errors.Is(statErr, os.ErrNotExist))

		_, statErr2 := os.Stat(filepath.Join(tempDir2, "chunks", "2A", "0", "42_0_13"))
		assert.True(t, errors.Is(statErr2, os.ErrNotExist))
	})
}

func TestLocalFSBlockStore_DirectorySyncEnforcedOnPutAndDelete(t *testing.T) {
	ctx := context.Background()

	t.Run("PutBlock 原子 Rename 发布至 OS Page Cache 不阻塞等待叶子分片目录 dir.Sync", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("requires non-root user to enforce directory 0o300 read permission")
		}
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		const sliceID = uint64(1)
		const blockIdx = uint32(0)
		payload := []byte("durable-payload")
		blockSize := uint32(len(payload))

		// 真实分片路径：%02X = 1%256 ("01"), %d = 1/1000/1000 ("0")
		shardDir := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", sliceID%256), fmt.Sprintf("%d", sliceID/1000/1000))
		require.NoError(t, os.MkdirAll(shardDir, 0o755))

		t.Cleanup(func() {
			_ = os.Chmod(shardDir, 0o755)
		})

		// 设为 0o300（d-wx------）：允许在该目录内创建临时文件、写入并 os.Rename，但禁止 os.Open(shardDir) 获取目录 fd 执行 Sync()
		require.NoError(t, os.Chmod(shardDir, 0o300))

		err := store.PutBlock(ctx, sliceID, blockIdx, payload)
		require.NoError(t, err, "PutBlock 发布至 OS Page Cache，不再在 Rename 后同步叶子分片目录")

		require.NoError(t, os.Chmod(shardDir, 0o755))
		finalPath := filepath.Join(shardDir, fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize))
		got, readErr := os.ReadFile(finalPath)
		require.NoError(t, readErr)
		assert.Equal(t, payload, got)
	})

	t.Run("DeleteBlock 删除实际存在的文件时直接删除而不阻塞等待 dir.Sync", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("requires non-root user to enforce directory 0o300 read permission")
		}
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		const sliceID = uint64(2)
		const blockIdx = uint32(0)
		payload := []byte("to-delete")
		blockSize := uint32(len(payload))

		// 在正常 0o755 权限下先成功落盘
		require.NoError(t, store.PutBlock(ctx, sliceID, blockIdx, payload))

		shardDir := filepath.Join(dataDir, "chunks", fmt.Sprintf("%02X", sliceID%256), fmt.Sprintf("%d", sliceID/1000/1000))
		t.Cleanup(func() {
			_ = os.Chmod(shardDir, 0o755)
		})

		// 设为 0o300（允许 os.Remove 删除目录项，但禁止 os.Open 同步目录）
		require.NoError(t, os.Chmod(shardDir, 0o300))

		err := store.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
		require.NoError(t, err, "DeleteBlock 直接 os.Remove 块文件，不再同步触发目录 Sync")

		require.NoError(t, os.Chmod(shardDir, 0o755))
		finalPath := filepath.Join(shardDir, fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize))
		_, statErr := os.Stat(finalPath)
		assert.ErrorIs(t, statErr, os.ErrNotExist)
	})
}

func TestLocalFSBlockStore_AncestorDirCreationWithoutSync(t *testing.T) {
	ctx := context.Background()

	t.Run("祖先目录权限为 0o300 只写不可读时，PutBlock 按需创建子目录并发布至 Page Cache 不打开目录句柄执行 Sync", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("跳过权限测试: root 用户忽略 POSIX 0o300 目录只写不可读限制")
		}
		parent := t.TempDir()
		dataDir := filepath.Join(parent, "bowl", "data")
		store := meta.NewLocalFSBlockStore(dataDir)

		t.Cleanup(func() {
			_ = os.Chmod(parent, 0o755)
		})
		// 设为 0o300 (-wx------)：允许在其下创建子目录与路径穿梭，但禁止 os.Open 打开目录句柄执行 Sync
		require.NoError(t, os.Chmod(parent, 0o300))

		// 1. 首次创建多级未创建目录 parent/bowl/data/chunks/01/0 并写入块
		payload1 := []byte("first-in-bucket-01")
		require.NoError(t, store.PutBlock(ctx, 1, 0, payload1))

		got1 := make([]byte, len(payload1))
		require.NoError(t, store.GetBlock(ctx, 1, 0, uint32(len(payload1)), 0, got1))
		assert.Equal(t, payload1, got1)

		// 2. 将 chunks 与 chunks/01 也设为 0o300，验证在其下新建第二个一级桶 02 与第二个二级分组目录 1 均成功
		chunksDir := filepath.Join(dataDir, "chunks")
		bucket01Dir := filepath.Join(chunksDir, "01")
		t.Cleanup(func() {
			_ = os.Chmod(bucket01Dir, 0o755)
			_ = os.Chmod(chunksDir, 0o755)
		})
		require.NoError(t, os.Chmod(chunksDir, 0o300))
		require.NoError(t, os.Chmod(bucket01Dir, 0o300))

		payload2 := []byte("second-bucket-02")
		require.NoError(t, store.PutBlock(ctx, 2, 0, payload2))
		got2 := make([]byte, len(payload2))
		require.NoError(t, store.GetBlock(ctx, 2, 0, uint32(len(payload2)), 0, got2))
		assert.Equal(t, payload2, got2)

		// 1000193 % 256 == 1 ("01"), 1000193 / 1_000_000 == 1 ("1")
		payload3 := []byte("second-group-1")
		require.NoError(t, store.PutBlock(ctx, 1000193, 0, payload3))
		got3 := make([]byte, len(payload3))
		require.NoError(t, store.GetBlock(ctx, 1000193, 0, uint32(len(payload3)), 0, got3))
		assert.Equal(t, payload3, got3)
	})

	t.Run("16 协程并发首写同一全新散列分片目录竞态检验", func(t *testing.T) {
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		const concurrency = 16
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		errs := make([]error, concurrency)

		for i := 0; i < concurrency; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				<-startCh
				payload := fmt.Appendf(nil, "concurrency-payload-%d", idx)
				sliceID := uint64(idx*256 + 3)
				errs[idx] = store.PutBlock(ctx, sliceID, 0, payload)
			}(i)
		}

		close(startCh)
		wg.Wait()

		for i := 0; i < concurrency; i++ {
			require.NoError(t, errs[i], "并发首写全新散列分片目录不得报错 (协程 %d)", i)
			expected := fmt.Appendf(nil, "concurrency-payload-%d", i)
			sliceID := uint64(i*256 + 3)
			data := make([]byte, len(expected))
			err := store.GetBlock(ctx, sliceID, 0, uint32(len(expected)), 0, data)
			require.NoError(t, err)
			assert.Equal(t, expected, data)
		}
	})
}

func TestLocalFSBlockStore_DirSyncedCacheFastPathAndSelfHeal(t *testing.T) {
	ctx := context.Background()

	t.Run("目标散列子目录首次创建并缓存后，后续 PutBlock 直接命中内存缓存零重复目录 Stat，且目标目录无 .tmp_* 残留", func(t *testing.T) {
		dataDir := t.TempDir()
		var statCalls atomic.Int64

		store := meta.NewLocalFSBlockStore(
			dataDir,
			meta.WithStatHook(func(name string) (os.FileInfo, error) {
				statCalls.Add(1)
				return os.Stat(name)
			}),
		)

		// 1. 首次写入 sliceID=7（散列目录 chunks/07/0）：创建目录并加入缓存
		payload1 := []byte("first-block-in-bucket-07")
		require.NoError(t, store.PutBlock(ctx, 7, 0, payload1))
		assert.Greater(t, statCalls.Load(), int64(0), "首次创建散列子目录应触发目录 Stat")

		// 2. 重置 Stat 计数，连续 10 次向同一散列目录（sliceID = 7 + i*256）执行 PutBlock
		statCalls.Store(0)
		for i := 1; i <= 10; i++ {
			sid := uint64(7 + i*256)
			data := fmt.Appendf(nil, "cached-dir-put-payload-%d", i)
			require.NoError(t, store.PutBlock(ctx, sid, 0, data))

			got := make([]byte, len(data))
			require.NoError(t, store.GetBlock(ctx, sid, 0, uint32(len(data)), 0, got))
			assert.Equal(t, data, got)
		}

		// 核心断言：缓存命中快速放行后，10 次 PutBlock 产生 0 次目录 Stat
		assert.Equal(t, int64(0), statCalls.Load(), "已缓存散列目录后续 PutBlock 必须直接命中内存缓存返回，不得重复执行 os.Stat")

		// 验证目标目录无 .tmp_* 临时文件残留
		expectedShardDir := filepath.Join(dataDir, "chunks", "07", "0")
		entries, err := os.ReadDir(expectedShardDir)
		require.NoError(t, err)
		for _, e := range entries {
			assert.False(t, strings.HasPrefix(e.Name(), ".tmp_"), "目录 %s 下不应残留临时文件 %s", expectedShardDir, e.Name())
		}
	})

	t.Run("已缓存散列子目录被外部误删后，后续写入自动剔除失效缓存并重建目录自愈成功", func(t *testing.T) {
		dataDir := t.TempDir()
		store := meta.NewLocalFSBlockStore(dataDir)

		require.NoError(t, store.PutBlock(ctx, 9, 0, []byte("initial-block")))

		shardDir := filepath.Join(dataDir, "chunks", "09", "0")
		require.NoError(t, os.RemoveAll(shardDir))

		healedData := []byte("self-healed-after-external-rmdir")
		require.NoError(t, store.PutBlock(ctx, 265, 0, healedData))

		got := make([]byte, len(healedData))
		require.NoError(t, store.GetBlock(ctx, 265, 0, uint32(len(healedData)), 0, got))
		assert.Equal(t, healedData, got)
	})

	t.Run("路径存在非目录同名文件或 WithStatHook 注入错误时，PutBlock 准确返回包装 ErrIO 的错误", func(t *testing.T) {
		// 1. 路径上存在非目录同名文件（chunks 为普通文件）
		dataDirFileConflict := t.TempDir()
		chunksFile := filepath.Join(dataDirFileConflict, "chunks")
		require.NoError(t, os.WriteFile(chunksFile, []byte("not-a-dir"), 0o644))

		storeConflict := meta.NewLocalFSBlockStore(dataDirFileConflict)
		err := storeConflict.PutBlock(ctx, 10, 0, []byte("conflict-payload"))
		require.ErrorIs(t, err, meta.ErrIO, "路径存在非目录同名文件时必须返回包装 ErrIO 的错误")

		// 2. WithStatHook 注入非 ErrNotExist 错误
		dataDirStatErr := t.TempDir()
		injectedErr := errors.New("injected stat hardware failure")
		storeStatErr := meta.NewLocalFSBlockStore(
			dataDirStatErr,
			meta.WithStatHook(func(name string) (os.FileInfo, error) {
				return nil, injectedErr
			}),
		)
		err = storeStatErr.PutBlock(ctx, 11, 0, []byte("stat-err-payload"))
		require.ErrorIs(t, err, meta.ErrIO, "WithStatHook 注入错误时必须返回包装 ErrIO 的错误")
		assert.Contains(t, err.Error(), injectedErr.Error())
	})
}
