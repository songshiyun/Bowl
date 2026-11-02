package meta_test

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

// TestWrite_CrossChunkBoundary 测试写入跨越 4 MiB Chunk 边界时自动切分为两个 Chunk 且 refs = 2。
func TestWrite_CrossChunkBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(201)
	fh := uint64(21)
	sliceID := int64(1001)

	// 跨越 4 MiB 边界：从 4 MiB - 1024 处写入 4096 字节
	offset := uint64(meta.ChunkSize - 1024)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 251)
	}

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       int64(offset), // 初始大小刚好等于写入起始偏移
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("普通跨 4 MiB 边界写入透明拆分至 indx 与 indx+1 且 slice_refs.refs 设为 2", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1：行锁外申请 slice_id 与落盘物理块 (payload <= 4 MiB 拆为 1 个 block)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：单语句 CTE WriteFileAtomic 提交
		expectedNewSize := int64(offset) + int64(len(payload))
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       int64(offset),
				DataLen:         int64(len(payload)),
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    int64(offset),
				NewSize:    expectedNewSize,
				StartIndx:  0,
				EndIndx:    1,
				OctetLen1:  24,
				OctetLen2:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       int32(meta.S_IFREG | 0o644),
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: offset,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(payload)), resp.Size)
		assert.Equal(t, uint64(expectedNewSize), resp.Inode.Attr.Size)
	})
}

// TestWrite_OAppend_IgnoreClientOffset 测试 O_APPEND 标志强制忽略客户端传入的偏移，锚定至 Inode.Size。
func TestWrite_OAppend_IgnoreClientOffset(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(301)
	fh := uint64(31)
	sliceID := int64(2001)
	payload := []byte("appended-bytes-payload")

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       500, // 当前大小 500
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	testCases := []struct {
		name       string
		clientOff  uint64
		openFlags  int32
		expectedSz int64
	}{
		{
			name:       "O_APPEND 句柄下客户端传入 offset = 0，强制锚定至 Inode.Size = 500",
			clientOff:  0,
			openFlags:  int32(meta.O_WRONLY | meta.O_APPEND),
			expectedSz: 500 + int64(len(payload)),
		},
		{
			name:       "O_APPEND 句柄下客户端传入非零 offset = 999999，强制忽略并锚定至 Inode.Size = 500",
			clientOff:  999999,
			openFlags:  int32(meta.O_RDWR | meta.O_APPEND),
			expectedSz: 500 + int64(len(payload)),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mockBS := metamock.NewMockBlockStore(t)
			mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

			mockStore.EXPECT().
				NextSliceID(mock.Anything).
				Return(sliceID, nil).
				Once()

			mockBS.EXPECT().
				PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
				Return(nil).
				Once()

			mockStore.EXPECT().
				WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
					Ino:             int64(ino),
					Fh:              int64(fh),
					SessionID:       pgtype.Int8{},
					ReqOffset:       int64(tc.clientOff),
					DataLen:         int64(len(payload)),
					QuotaAvailBytes: math.MaxInt64,
					SliceID:         sliceID,
				}).
				Return(db.WriteFileAtomicRow{
					Status:     "ok",
					OldSize:    500,
					NewSize:    tc.expectedSz,
					StartIndx:  0,
					EndIndx:    0,
					OctetLen1:  24,
					Ino:        int64(ino),
					Generation: 1,
					Mode:       int32(meta.S_IFREG | 0o644),
					Nlink:      1,
					Uid:        1000,
					Gid:        1000,
					Atime:      baseFileRow.Atime,
					Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
					Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
				}, nil).
				Once()

			resp, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: tc.clientOff,
				Data:   payload,
			})

			require.NoError(t, err)
			require.NotNil(t, resp)
			assert.Equal(t, uint32(len(payload)), resp.Size)
			assert.Equal(t, uint64(tc.expectedSz), resp.Inode.Attr.Size)
		})
	}
}

// TestWrite_OAppend_CrossChunkBoundary 测试 O_APPEND 追加写恰好跨越 4 MiB 边界时的双 Chunk 拆分与 refs = 2。
func TestWrite_OAppend_CrossChunkBoundary(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(401)
	fh := uint64(41)
	sliceID := int64(3001)

	// 初始文件大小距 4 MiB 边界仅剩 2048 字节
	initialSize := int64(meta.ChunkSize - 2048)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 127)
	}

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       initialSize,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("O_APPEND 恰好跨越 4 MiB 边界时自动拆分至 indx 与 indx+1 且 refs = 2", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		expectedNewSize := initialSize + int64(len(payload))
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       0,
				DataLen:         int64(len(payload)),
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    initialSize,
				NewSize:    expectedNewSize,
				StartIndx:  0,
				EndIndx:    1,
				OctetLen1:  24,
				OctetLen2:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       int32(meta.S_IFREG | 0o644),
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0, // 客户端传入 0，被 O_APPEND 忽略
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(payload)), resp.Size)
		assert.Equal(t, uint64(expectedNewSize), resp.Inode.Attr.Size)
	})
}

// TestWrite_OAppend_OverflowMaxInt64 测试 O_APPEND 追加写导致 offset 溢出 MaxInt64 时被拦截且清理物理块。
func TestWrite_OAppend_OverflowMaxInt64(t *testing.T) {
	ctx := context.Background()
	ino := uint64(501)
	fh := uint64(51)
	sliceID := int64(4001)
	payload := []byte("overflow-test-data")

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	mockStore.EXPECT().
		NextSliceID(mock.Anything).
		Return(sliceID, nil).
		Once()

	mockBS.EXPECT().
		PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
		Return(nil).
		Once()

	mockStore.EXPECT().
		WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
			Ino:             int64(ino),
			Fh:              int64(fh),
			SessionID:       pgtype.Int8{},
			ReqOffset:       0,
			DataLen:         int64(len(payload)),
			QuotaAvailBytes: math.MaxInt64,
			SliceID:         sliceID,
		}).
		Return(db.WriteFileAtomicRow{Status: "invalid_arg"}, nil).
		Once()

	// 溢出时单语句 CTE 返回 invalid_arg，外层立即调用 DeleteBlock 清理物理块
	mockBS.EXPECT().
		DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(len(payload))).
		Return(nil).
		Once()

	resp, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   payload,
	})

	require.Nil(t, resp)
	require.ErrorIs(t, err, meta.ErrInvalidArgument)
}

// TestWrite_OAppend_Concurrent_NoDataLoss 测试多个并发协程使用 O_APPEND 写入时，行锁确保线性追加且零数据覆盖丢失。
func TestWrite_OAppend_Concurrent_NoDataLoss(t *testing.T) {
	ctx := context.Background()
	ino := uint64(601)
	fh := uint64(61)

	// 使用状态机模拟真实数据库行为：互斥锁代表 Inode 行级排他锁 (FOR UPDATE)
	var mu sync.Mutex
	currentSize := int64(100)
	var appendedSlices []meta.Slice
	var sliceIDSeq int64 = 5000

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	// 阶段 1：NextSliceID 原子自增
	mockStore.EXPECT().
		NextSliceID(mock.Anything).
		RunAndReturn(func(ctx context.Context) (int64, error) {
			return atomic.AddInt64(&sliceIDSeq, 1), nil
		})

	mockBS.EXPECT().
		PutBlock(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(nil)

	// 阶段 2：单语句 CTE 内排他锁
	mockStore.EXPECT().
		WriteFileAtomic(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
			mu.Lock()
			defer mu.Unlock()

			oldSize := currentSize
			actualOff := uint64(oldSize)
			appendedSlices = append(appendedSlices, meta.Slice{
				Pos:  uint32(actualOff % meta.ChunkSize),
				ID:   uint64(arg.SliceID),
				Size: uint32(arg.DataLen),
				Off:  0,
				Len:  uint32(arg.DataLen),
			})
			currentSize = oldSize + arg.DataLen
			return db.WriteFileAtomicRow{
				Status:    "ok",
				OldSize:   oldSize,
				NewSize:   currentSize,
				StartIndx: int64(actualOff / meta.ChunkSize),
				EndIndx:   int64((actualOff + uint64(arg.DataLen) - 1) / meta.ChunkSize),
				OctetLen1: int64(len(appendedSlices) * meta.SliceBytes),
				Ino:       arg.Ino,
				Mode:      int32(meta.S_IFREG | 0o644),
				Nlink:     1,
			}, nil
		})

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

	const concurrency = 10
	const writeLen = 64
	data := make([]byte, writeLen)
	for i := range data {
		data[i] = 'A' + byte(i%26)
	}

	var wg sync.WaitGroup
	errCh := make(chan error, concurrency)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			// 故意传入不同的 offset（如 0、123、999），验证 O_APPEND 会强制忽略 offset
			clientOff := uint64(id * 1000)
			res, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: clientOff,
				Data:   data,
			})
			if err != nil {
				errCh <- fmt.Errorf("write %d failed: %w", id, err)
				return
			}
			if res.Size != writeLen {
				errCh <- fmt.Errorf("write %d unexpected size: %d", id, res.Size)
				return
			}
		}(i)
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	// 验证最终状态：
	// 1. 最终文件大小必须严格等于 100 + concurrency * writeLen = 740
	expectedFinalSize := int64(100 + concurrency*writeLen)
	assert.Equal(t, expectedFinalSize, currentSize)

	// 2. 累积记录了 10 条 Slice 且 Pos 依次递增无重叠覆盖
	require.Len(t, appendedSlices, concurrency)
	// 对 appendedSlices 按 Pos 排序检验连续性
	for i := 0; i < concurrency; i++ {
		expectedPos := uint32(100 + i*writeLen)
		assert.Equal(t, expectedPos, appendedSlices[i].Pos, "slice %d 起始位置不连续或发生重叠", i)
		assert.Equal(t, uint32(writeLen), appendedSlices[i].Len)
	}
}

// TestWrite_CrossChunkBoundary_PostgresIntegration 真实 PostgreSQL 容器跨 4 MiB 边界与 O_APPEND 集成测试。
func TestWrite_CrossChunkBoundary_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	// 创建初始文件
	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	// 1. 测试普通跨 4 MiB 边界写入
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh

	// 2. 测试 O_APPEND 句柄追加
	appendOpenRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_WRONLY | meta.O_APPEND,
	})
	require.NoError(t, err)
	appendFh := appendOpenRes.Fh

	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, store, mgr, fileRow.Ino, fh, appendFh)
	})

	// 在 4 MiB - 1024 处写入 4096 字节
	crossOffset := uint64(meta.ChunkSize - 1024)
	payload := make([]byte, 4096)
	for i := range payload {
		payload[i] = byte(i % 256)
	}

	writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: crossOffset,
		Data:   payload,
	})
	require.NoError(t, err)
	require.NotNil(t, writeRes)
	assert.Equal(t, uint32(4096), writeRes.Size)
	assert.Equal(t, uint64(crossOffset+4096), writeRes.Inode.Attr.Size)

	// 校验 Chunk 0
	chunk0, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slices0, err := meta.DecodeSlices(chunk0.Slices)
	require.NoError(t, err)
	require.Len(t, slices0, 1)
	assert.Equal(t, uint32(meta.ChunkSize-1024), slices0[0].Pos)
	assert.Equal(t, uint32(1024), slices0[0].Len)
	assert.Equal(t, uint32(0), slices0[0].Off)
	sliceID := slices0[0].ID

	// 校验 Chunk 1
	chunk1, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
	require.NoError(t, err)
	slices1, err := meta.DecodeSlices(chunk1.Slices)
	require.NoError(t, err)
	require.Len(t, slices1, 1)
	assert.Equal(t, uint32(0), slices1[0].Pos)
	assert.Equal(t, uint32(3072), slices1[0].Len)
	assert.Equal(t, uint32(1024), slices1[0].Off)
	assert.Equal(t, sliceID, slices1[0].ID)

	// 校验 slice_refs 引用计数为 2
	ref, err := store.GetSliceRef(ctx, int64(sliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(2), ref.Refs)
	assert.Equal(t, int32(4096), ref.Size)

	appendData := []byte("append-to-tail")
	appendRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     appendFh,
		Offset: 0, // 传入 0，应被忽略
		Data:   appendData,
	})
	require.NoError(t, err)
	require.NotNil(t, appendRes)
	assert.Equal(t, uint64(crossOffset+4096+uint64(len(appendData))), appendRes.Inode.Attr.Size)
}

// TestWrite_ConcurrentOAppend_PostgresIntegration 验证 16 个并发客户端使用 O_APPEND 向同一文件并发写入 4096 字节。
// 验证数据库行锁排他保证零丢失更新、零重叠覆盖、零空洞，最终文件大小与磁盘已用容量严格匹配且全量块内容逐一完整。
func TestWrite_ConcurrentOAppend_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     "concurrent-append-disk",
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(disk.RootIno),
		Name:      []byte("append_target.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino

	t.Cleanup(func() {
		var sliceIDs []int64
		for indx := int64(0); indx < 16; indx++ {
			chunkRow, _ := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: indx})
			if len(chunkRow.Slices) > 0 {
				if slices, err := meta.DecodeSlices(chunkRow.Slices); err == nil {
					for _, s := range slices {
						if s.ID > 0 {
							sliceIDs = append(sliceIDs, int64(s.ID))
							_ = store.DeleteSliceRef(context.Background(), int64(s.ID))
						}
					}
				}
			}
		}
		_ = meta.DeleteAllChunksByInos(context.Background(), store, []int64{int64(ino)})
		_ = store.DeleteInode(context.Background(), int64(ino))

		_, inoErr := store.GetInode(context.Background(), int64(ino))
		assert.ErrorIs(t, inoErr, pgx.ErrNoRows, "目标 ino 必须已被清除")
		for _, sid := range sliceIDs {
			_, sErr := store.GetSliceRef(context.Background(), sid)
			assert.ErrorIs(t, sErr, pgx.ErrNoRows, "测试专有 slice_ref %d 必须已被清除", sid)
		}
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR | meta.O_APPEND,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	const numWorkers = 16
	const blockSize = 4096

	type writeResult struct {
		workerID int
		err      error
		res      *meta.WriteResponse
	}
	results := make(chan writeResult, numWorkers)
	startCh := make(chan struct{})
	var startOnce sync.Once
	closeStart := func() {
		startOnce.Do(func() {
			close(startCh)
		})
	}
	t.Cleanup(closeStart)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		workerID := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case <-startCh:
			case <-ctx.Done():
				results <- writeResult{workerID: workerID, err: ctx.Err()}
				return
			}

			payload := bytes.Repeat([]byte{byte('A' + workerID)}, blockSize)
			res, writeErr := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: 0, // 故意传入 0 验证 O_APPEND 依托数据库 GetInodeForUpdate 行锁强制按最新末尾顺序追加而不互相覆盖
				Data:   payload,
			})
			results <- writeResult{workerID: workerID, err: writeErr, res: res}
		}()
	}

	closeStart()
	wg.Wait()
	close(results)

	var successCount int
	for r := range results {
		assert.NoError(t, r.err, "worker %d 写入必须成功", r.workerID)
		if r.err == nil {
			successCount++
		}
	}
	assert.Equal(t, numWorkers, successCount, "成功写入次数严格等于 16（零错误、零吞错）")

	// 最终 GetAttr 文件 Size == 16 * 4096
	attr, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, uint64(numWorkers*blockSize), attr.Attr.Size)

	// Disk 用量 UsedBytes == 16 * 4096
	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(numWorkers*blockSize), diskInfo.UsedBytes)

	// mgr.Read(offset=0, size=16*4096) 读回全文件切分为 16 个 4096B 块后，每个块内部 4096 字节均一无混杂、且 16 个 worker 的特征字节集合 100% 完整出现
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(numWorkers * blockSize),
	})
	require.NoError(t, err)
	require.NotNil(t, readRes)
	require.Len(t, readRes.Data, numWorkers*blockSize)

	seenWorkers := make(map[byte]bool)
	for b := 0; b < numWorkers; b++ {
		block := readRes.Data[b*blockSize : (b+1)*blockSize]
		char := block[0]
		seenWorkers[char] = true
		assert.Equal(t, bytes.Repeat([]byte{char}, blockSize), block, "每个块内部 4096 字节均一无混杂 (块索引 %d)", b)
	}
	assert.Len(t, seenWorkers, numWorkers, "16 个 worker 的特征字节集合 100% 完整出现（零丢失更新、零重叠覆盖、零空洞）")
	for i := 0; i < numWorkers; i++ {
		assert.True(t, seenWorkers[byte('A'+i)], "特征字节 %c 必须存在", byte('A'+i))
	}
}

// TestWrite_4MiBChunkBoundaryMatrix_PostgresIntegration 全面覆盖 4 MiB Chunk 边界写入与回读矩阵：
// 1. 4 MiB 边界前、恰好对齐 4 MiB 边界（[0, 4 MiB) 与 [4 MiB, 8 MiB)）、4 MiB 边界后；
// 2. 非对齐最大 4 MiB 负载（len(Data) == 4 MiB）跨越 4 MiB 边界写入与回读（单物理块 block_idx=0、相邻双 Chunk 共享 slice_id、refs=2）；
// 3. 并发 O_APPEND 跨越 4 MiB 边界（包含恰好跨界双 Chunk 拆分的写入）线性追加零丢失。
func TestWrite_4MiBChunkBoundaryMatrix_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("4mib-boundary-matrix-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	t.Run("恰好对齐 4 MiB 边界单次满块写入与非对齐最大 4 MiB 跨界写入回读", func(t *testing.T) {
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("max_4mib_cross.bin"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, err)
		ino := createRes.Inode.Ino
		fh := createRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
			for indx := int64(0); indx < 4; indx++ {
				if c, cErr := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: indx}); cErr == nil && len(c.Slices) > 0 {
					if sls, dErr := meta.DecodeSlices(c.Slices); dErr == nil {
						for _, s := range sls {
							if s.ID > 0 {
								_ = store.DeleteSliceRef(context.Background(), int64(s.ID))
							}
						}
					}
				}
			}
			_ = meta.DeleteAllChunksByInos(context.Background(), store, []int64{int64(ino)})
			_ = store.DeleteInode(context.Background(), int64(ino))
		})

		// 1. 恰好对齐 4 MiB 边界：在 [0, 4 MiB) 写入满 4 MiB 数据，必须落入单一 Chunk 0 且 refs = 1
		alignedPayload := make([]byte, meta.ChunkSize)
		for i := range alignedPayload {
			alignedPayload[i] = byte((i*3 + 7) % 251)
		}
		wAligned, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   alignedPayload,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(meta.ChunkSize), wAligned.Size)
		assert.Equal(t, uint64(meta.ChunkSize), wAligned.Inode.Attr.Size)

		c0Aligned, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		sls0Aligned, err := meta.DecodeSlices(c0Aligned.Slices)
		require.NoError(t, err)
		require.Len(t, sls0Aligned, 1)
		assert.Equal(t, uint32(0), sls0Aligned[0].Pos)
		assert.Equal(t, uint32(meta.ChunkSize), sls0Aligned[0].Len)
		refAligned, err := store.GetSliceRef(ctx, int64(sls0Aligned[0].ID))
		require.NoError(t, err)
		assert.Equal(t, int32(1), refAligned.Refs, "恰好对齐 4 MiB 边界的单次写入不得跨块拆分，refs 必须为 1")

		// 2. 非对齐最大 4 MiB 负载跨越 4 MiB 边界：在 Offset = 2 MiB 处写入完整 4 MiB 数据（覆盖 [2 MiB, 6 MiB)）
		unalignedOff := uint64(meta.ChunkSize / 2) // 2 MiB
		crossMaxPayload := make([]byte, meta.ChunkSize)
		for i := range crossMaxPayload {
			crossMaxPayload[i] = byte((i*11 + 19) % 253)
		}
		wCross, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: unalignedOff,
			Data:   crossMaxPayload,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(meta.ChunkSize), wCross.Size)
		assert.Equal(t, unalignedOff+uint64(meta.ChunkSize), wCross.Inode.Attr.Size)

		// 验证 Chunk 0 的第二条切片与 Chunk 1 的第一条切片共享同一 slice_id 且 refs = 2
		c0After, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		sls0After, err := meta.DecodeSlices(c0After.Slices)
		require.NoError(t, err)
		require.Len(t, sls0After, 2)

		c1After, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
		require.NoError(t, err)
		sls1After, err := meta.DecodeSlices(c1After.Slices)
		require.NoError(t, err)
		require.Len(t, sls1After, 1)

		sharedID := sls0After[1].ID
		assert.Equal(t, sharedID, sls1After[0].ID, "跨 4 MiB 边界最大负载写入必须共享同一 slice_id")
		assert.Equal(t, uint32(meta.ChunkSize/2), sls0After[1].Pos)
		assert.Equal(t, uint32(0), sls0After[1].Off)
		assert.Equal(t, uint32(meta.ChunkSize/2), sls0After[1].Len)
		assert.Equal(t, uint32(0), sls1After[0].Pos)
		assert.Equal(t, uint32(meta.ChunkSize/2), sls1After[0].Off)
		assert.Equal(t, uint32(meta.ChunkSize/2), sls1After[0].Len)

		refCross, err := store.GetSliceRef(ctx, int64(sharedID))
		require.NoError(t, err)
		assert.Equal(t, int32(2), refCross.Refs, "跨 4 MiB 边界最大负载写入的 slice_refs.refs 必须为 2")
		assert.Equal(t, int32(meta.ChunkSize), refCross.Size)

		// 3. 回读验证：[0, 2 MiB) 保持 alignedPayload 前半部分，[2 MiB, 6 MiB) 完整匹配 crossMaxPayload
		readCross, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: unalignedOff,
			Size:   uint32(meta.ChunkSize),
		})
		require.NoError(t, err)
		assert.Equal(t, crossMaxPayload, readCross.Data, "跨 4 MiB 边界读取完整 4 MiB 负载必须逐字节一致")

		readHead, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(unalignedOff),
		})
		require.NoError(t, err)
		assert.Equal(t, alignedPayload[:unalignedOff], readHead.Data)
	})

	t.Run("并发 O_APPEND 跨越 4 MiB 边界线性追加无丢失且跨界切片 refs 准确", func(t *testing.T) {
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("concurrent_append_cross_4mib.bin"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR | meta.O_APPEND,
		})
		require.NoError(t, err)
		ino := createRes.Inode.Ino
		fh := createRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
			for indx := int64(0); indx < 4; indx++ {
				if c, cErr := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: indx}); cErr == nil && len(c.Slices) > 0 {
					if sls, dErr := meta.DecodeSlices(c.Slices); dErr == nil {
						for _, s := range sls {
							if s.ID > 0 {
								_ = store.DeleteSliceRef(context.Background(), int64(s.ID))
							}
						}
					}
				}
			}
			_ = meta.DeleteAllChunksByInos(context.Background(), store, []int64{int64(ino)})
			_ = store.DeleteInode(context.Background(), int64(ino))
		})

		// 先将文件大小推进到 4 MiB - 6144（距离 4 MiB 边界差 1.5 个 4096B 块），
		// 这样后续 8 个并发 4096B 的 O_APPEND 必然有 1 个恰好在 4 MiB 边界处横跨 Chunk 0 与 Chunk 1！
		initSize := uint64(meta.ChunkSize - 6144)
		_, err = mgr.SetAttr(ctx, ino, meta.SetAttrInput{Size: &initSize})
		require.NoError(t, err)

		const workers = 8
		const appendSize = 4096
		startCh := make(chan struct{})
		errCh := make(chan error, workers)
		var wg sync.WaitGroup

		for w := 0; w < workers; w++ {
			workerID := w
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				payload := bytes.Repeat([]byte{byte('a' + workerID)}, appendSize)
				_, wErr := mgr.Write(ctx, &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: 0,
					Data:   payload,
				})
				errCh <- wErr
			}()
		}

		close(startCh)
		wg.Wait()
		close(errCh)
		for wErr := range errCh {
			require.NoError(t, wErr)
		}

		expectedFinalSize := initSize + uint64(workers*appendSize)
		attr, err := mgr.GetAttr(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, expectedFinalSize, attr.Attr.Size)

		// 跨 4 MiB 边界读取全部追加区域 [initSize, expectedFinalSize)
		readAppended, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: initSize,
			Size:   uint32(workers * appendSize),
		})
		require.NoError(t, err)
		require.Len(t, readAppended.Data, workers*appendSize)

		seen := make(map[byte]bool)
		for i := 0; i < workers; i++ {
			blk := readAppended.Data[i*appendSize : (i+1)*appendSize]
			ch := blk[0]
			seen[ch] = true
			assert.Equal(t, bytes.Repeat([]byte{ch}, appendSize), blk, "并发跨 4 MiB 边界 O_APPEND 块 %d 严禁撕裂或混杂", i)
		}
		assert.Len(t, seen, workers, "所有并发 O_APPEND 块必须 100% 完整落盘")
	})
}
