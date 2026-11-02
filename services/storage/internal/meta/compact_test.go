package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func setupPostgresForCompact(t *testing.T) (db.Store, meta.BlockStore, meta.Manager, string, *pgxpool.Pool) {
	t.Helper()
	pool, _ := setupMetaTestDB(t)
	if pool == nil {
		return nil, nil, nil, "", nil
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	return store, bs, mgr, dataDir, pool
}

func cleanupCompactTestIno(ctx context.Context, store db.Store, ino int64) {
	for indx := int64(0); indx < 16; indx++ {
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: ino, Indx: indx})
		if err == nil && len(chunkRow.Slices) > 0 {
			if slices, err := meta.DecodeSlices(chunkRow.Slices); err == nil {
				for _, s := range slices {
					_ = store.DeleteSliceRef(ctx, int64(s.ID))
				}
			}
		}
	}
	_ = meta.DeleteAllChunksByInos(ctx, store, []int64{ino})
	_ = store.DeleteInode(ctx, ino)
}

type recordedBlockRead struct {
	sliceID   uint64
	blockIdx  uint32
	blockSize uint32
	offset    uint32
	length    uint32
}

type recordedBlockWrite struct {
	sliceID  uint64
	blockIdx uint32
	length   uint32
}

// recordingBlockStore 包装实际 BlockStore 用于统计记录与注入回调
type recordingBlockStore struct {
	realBS              meta.BlockStore
	mu                  sync.Mutex
	gets                []uint64
	puts                []uint64
	deletes             []uint64
	readOps             []recordedBlockRead
	writeOps            []recordedBlockWrite
	onBeforePutBlock    func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
	onAfterPutBlock     func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
	onBeforeGetBlock    func(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32) error
	onBeforeDeleteBlock func(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error
}

func (r *recordingBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if r.onBeforePutBlock != nil {
		if err := r.onBeforePutBlock(ctx, sliceID, blockIdx, data); err != nil {
			return err
		}
	}
	if err := r.realBS.PutBlock(ctx, sliceID, blockIdx, data); err != nil {
		return err
	}
	r.mu.Lock()
	r.puts = append(r.puts, sliceID)
	r.writeOps = append(r.writeOps, recordedBlockWrite{
		sliceID:  sliceID,
		blockIdx: blockIdx,
		length:   uint32(len(data)),
	})
	r.mu.Unlock()
	if r.onAfterPutBlock != nil {
		if err := r.onAfterPutBlock(ctx, sliceID, blockIdx, data); err != nil {
			return err
		}
	}
	return nil
}

func (r *recordingBlockStore) GetBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, offset uint32, dst []byte) error {
	if r.onBeforeGetBlock != nil {
		if err := r.onBeforeGetBlock(ctx, sliceID, blockIdx, blockSize, offset); err != nil {
			return err
		}
	}
	r.mu.Lock()
	r.gets = append(r.gets, sliceID)
	r.readOps = append(r.readOps, recordedBlockRead{
		sliceID:   sliceID,
		blockIdx:  blockIdx,
		blockSize: blockSize,
		offset:    offset,
		length:    uint32(len(dst)),
	})
	r.mu.Unlock()
	return r.realBS.GetBlock(ctx, sliceID, blockIdx, blockSize, offset, dst)
}

func (r *recordingBlockStore) DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
	r.mu.Lock()
	r.deletes = append(r.deletes, sliceID)
	r.mu.Unlock()
	if r.onBeforeDeleteBlock != nil {
		if err := r.onBeforeDeleteBlock(ctx, sliceID, blockIdx, blockSize); err != nil {
			return err
		}
	}
	return r.realBS.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
}

// TestCompactChunk_StaticValidation 测试静态参数校验
func TestCompactChunk_StaticValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	tests := []struct {
		name string
		ino  uint64
		indx int64
	}{
		{
			name: "ino 为 0",
			ino:  0,
			indx: 0,
		},
		{
			name: "ino 超出 MaxInt64",
			ino:  uint64(math.MaxInt64) + 1,
			indx: 0,
		},
		{
			name: "indx 为负数",
			ino:  1,
			indx: -1,
		},
		{
			name: "indx 超出 MaxInt64/ChunkSize",
			ino:  1,
			indx: math.MaxInt64/int64(meta.ChunkSize) + 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.CompactChunk(ctx, tt.ino, tt.indx)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

// TestCompactChunk_NoRowsOrEmpty 测试 Chunk 不存在或切片为空时返回 nil
func TestCompactChunk_NoRowsOrEmpty(t *testing.T) {
	ctx := context.Background()

	t.Run("chunk 不存在返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{}, pgx.ErrNoRows).Once()

		mgr := meta.New(mockStore)
		err := mgr.CompactChunk(ctx, 1, 0)
		assert.NoError(t, err)
	})

	t.Run("chunk slices 为空返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{
			Ino:    1,
			Indx:   0,
			Slices: nil,
		}, nil).Once()

		mgr := meta.New(mockStore)
		err := mgr.CompactChunk(ctx, 1, 0)
		assert.NoError(t, err)
	})
}

// TestCompactChunk_RandomOverwritesAndHoles 集成测试 1：
// 高频随机覆写（包含跨越多层 Slice、局部覆写、稀疏空洞保留）后触发 CompactChunk，
// 消除 Self-Oracle 自证：基于独立构造的内存黄金期望数据分别断言合并前后 Read 逐字节一致，
// 且 chunks.slices 长度显著缩减为紧致 Slice。
func TestCompactChunk_RandomOverwritesAndHoles(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _, _ := setupPostgresForCompact(t)

	// 创建测试常规文件
	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 独立构造期望内存黄金字节数组（220KB）
	expected := make([]byte, 220*1024)

	// 执行多次局部随机覆写：
	// 1. 先写 0..64KB 填入 0xAA
	data1 := bytes.Repeat([]byte{0xAA}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data1})
	require.NoError(t, err)
	copy(expected[0:64*1024], data1)

	// 2. 在 16KB..48KB 覆写 0xBB
	data2 := bytes.Repeat([]byte{0xBB}, 32*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 16 * 1024, Data: data2})
	require.NoError(t, err)
	copy(expected[16*1024:48*1024], data2)

	// 3. 在 32KB..40KB 覆写 0xCC
	data3 := bytes.Repeat([]byte{0xCC}, 8*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 32 * 1024, Data: data3})
	require.NoError(t, err)
	copy(expected[32*1024:40*1024], data3)

	// 4. 在 128KB..192KB 写入 0xDD（产生 64KB..128KB 的稀疏空洞）
	data4 := bytes.Repeat([]byte{0xDD}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 128 * 1024, Data: data4})
	require.NoError(t, err)
	copy(expected[128*1024:192*1024], data4)

	// 5. 在 140KB..160KB 打洞（Punch Hole 填零）
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: 140 * 1024,
		Length: 20 * 1024,
	})
	require.NoError(t, err)
	copy(expected[140*1024:160*1024], make([]byte, 20*1024))

	// 6. 在 200KB..220KB 写入 0xEE
	data6 := bytes.Repeat([]byte{0xEE}, 20*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 200 * 1024, Data: data6})
	require.NoError(t, err)
	copy(expected[200*1024:220*1024], data6)

	// 合并前读取全量数据
	readBefore, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   250 * 1024,
	})
	require.NoError(t, err)
	require.Len(t, readBefore.Data, 220*1024)
	// 消除自证：与内存黄金期望数据比对
	assert.Equal(t, expected, readBefore.Data, "合并前 Read 必须与独立期望黄金数组严格一致")

	// 校验合并前 chunk 切片条数 >= 5
	chunkBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(chunkBefore.Slices)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(slicesBefore), 5)

	// 执行 CompactChunk
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 合并后读取全量数据
	readAfter, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   250 * 1024,
	})
	require.NoError(t, err)

	// 消除自证：合并后 Read 直接与内存黄金期望数据比对（严禁仅与 readBefore 比对）
	assert.Equal(t, expected, readAfter.Data, "合并后 Read 必须与独立期望黄金数组严格一致")

	// 断言 chunks.slices 数量显著缩减为紧致 Slice（非空洞有效数据段数量）
	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
	require.NoError(t, err)

	// 有效紧致切片段应远小于之前的切片数
	assert.Less(t, len(slicesAfter), len(slicesBefore))
	t.Logf("Compaction 完成: 切片数量从 %d 缩减至 %d", len(slicesBefore), len(slicesAfter))
}

// TestCompactChunk_ConcurrentWriteSuffixPreserved 集成测试 2：
// 真实命中阶段 1 ➔ 阶段 2 并发追加 concurrentSuffix 分支：
// 在阶段 1 调用 PutBlock 落盘时通过钩子同步执行第 3 次 mgr.Write 追加片段；
// CompactChunk 返回后断言合并后的 chunk.Slices 精确为 2 条（1 条合并 + 1 条保留的 concurrentSuffix），
// 旧切片 refs=0，新合并与并发写入切片 refs=1，mgr.Read 精确读出最新完整数据。
func TestCompactChunk_ConcurrentWriteSuffixPreserved(t *testing.T) {
	ctx := context.Background()
	store, bs, mgr, _, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 初始写入 2 段数据
	data1 := bytes.Repeat([]byte{0x11}, 10*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data1})
	require.NoError(t, err)

	data2 := bytes.Repeat([]byte{0x22}, 10*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 10 * 1024, Data: data2})
	require.NoError(t, err)

	// 记录合并前旧 2 条 Slice 的 ID
	chunkRowBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(chunkRowBefore.Slices)
	require.NoError(t, err)
	require.Len(t, slicesBefore, 2)
	oldID0 := int64(slicesBefore[0].ID)
	oldID1 := int64(slicesBefore[1].ID)

	data3 := bytes.Repeat([]byte{0x33}, 10*1024)
	var write3SliceID uint64
	var hookOnce sync.Once

	hookBS := &recordingBlockStore{
		realBS: bs,
		onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
			var hookErr error
			hookOnce.Do(func() {
				// 在阶段 1 PutBlock 落盘时，模拟并发执行第 3 次 Write 追加覆盖片段
				_, writeErr := mgr.Write(context.Background(), &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: 20 * 1024,
					Data:   data3,
				})
				if writeErr != nil {
					hookErr = writeErr
					return
				}
				// 从底表获取并发第 3 次写入所分配的 sliceID
				c, err := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: 0})
				if err != nil {
					hookErr = err
					return
				}
				sl, err := meta.DecodeSlices(c.Slices)
				if err != nil {
					hookErr = err
					return
				}
				if len(sl) >= 3 {
					write3SliceID = sl[2].ID
				}
			})
			return hookErr
		},
	}

	mgrWithHook := meta.New(store, meta.WithBlockStore(hookBS))

	// 触发 CompactChunk
	err = mgrWithHook.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 查询底表 GetChunk 断言合并后的 len(chunk.Slices) == 2 * meta.SliceBytes（精确为 1 条合并 Slice + 1 条被保留的 concurrentSuffix Slice）
	chunkRowAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	assert.Equal(t, 2*meta.SliceBytes, len(chunkRowAfter.Slices), "合并后 slices 字节数必须精确为 2 * SliceBytes (1 条合并切片 + 1 条保留的 concurrentSuffix 切片)")

	slicesAfter, err := meta.DecodeSlices(chunkRowAfter.Slices)
	require.NoError(t, err)
	require.Len(t, slicesAfter, 2)
	compactedSliceID := slicesAfter[0].ID
	suffixSliceID := slicesAfter[1].ID
	assert.Equal(t, write3SliceID, suffixSliceID, "保留的后缀切片必须与并发追加切片 ID 精确一致")

	// 旧 2 条 slice_refs.refs == 0
	ref0, err := store.GetSliceRef(ctx, oldID0)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref0.Refs, "旧切片 0 的 refs 必须降为 0")

	ref1, err := store.GetSliceRef(ctx, oldID1)
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref1.Refs, "旧切片 1 的 refs 必须降为 0")

	// 新合并 Slice 与第 3 次并发写 Slice 的 refs == 1
	refCompacted, err := store.GetSliceRef(ctx, int64(compactedSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(1), refCompacted.Refs, "新合并切片的 refs 必须为 1")

	refSuffix, err := store.GetSliceRef(ctx, int64(suffixSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(1), refSuffix.Refs, "保留的并发追加切片 refs 必须为 1")

	// 读取全量 30KB 数据，mgr.Read 精确读出包含第 3 次并发覆盖的完整数据
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   30 * 1024,
	})
	require.NoError(t, err)
	require.Len(t, readRes.Data, 30*1024)

	assert.Equal(t, data1, readRes.Data[0:10*1024])
	assert.Equal(t, data2, readRes.Data[10*1024:20*1024])
	assert.Equal(t, data3, readRes.Data[20*1024:30*1024])
}

// TestCompactChunk_TruncateOrDeleteCausesCASAbortAndCleanup 集成测试 3：
// 真实命中阶段 1 落盘后并发 O_TRUNC 回滚删块分支：
// 在 recordingBlockStore.PutBlock 完成新合并物理块落盘后的钩子内，同步执行 mgr.Open(..., O_RDWR|O_TRUNC) 清空该文件的 chunks；
// 待 CompactChunk 返回 nil 后，显式对 recordingBlockStore 断言新合并物理块确实经历了 PutBlock >= 1 且 DeleteBlock == PutBlock（回滚删块），
// 且磁盘与底表 chunks 中无孤儿残留。
func TestCompactChunk_TruncateOrDeleteCausesCASAbortAndCleanup(t *testing.T) {
	ctx := context.Background()
	store, bs, _, dataDir, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	initMgr := meta.New(store, meta.WithBlockStore(bs))

	openRes, err := initMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = initMgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入 16KB 数据
	data := bytes.Repeat([]byte{0x42}, 16*1024)
	_, err = initMgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
	require.NoError(t, err)

	blockFilePath := func(sliceID uint64, blockIdx uint32, blockSize uint32) string {
		return filepath.Join(
			dataDir,
			"chunks",
			fmt.Sprintf("%02X", sliceID%256),
			fmt.Sprintf("%d", sliceID/1000/1000),
			fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, blockSize),
		)
	}

	var truncOnce sync.Once
	mockBS := &recordingBlockStore{
		realBS: bs,
		onAfterPutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, putData []byte) error {
			var hookErr error
			truncOnce.Do(func() {
				// 在 recordingBlockStore.PutBlock 完成新合并物理块落盘后的钩子内，同步执行 mgr.Open(..., O_RDWR|O_TRUNC) 清空该文件的 chunks
				openTruncRes, err := initMgr.Open(context.Background(), meta.OpenParams{
					Ino:   ino,
					Flags: meta.O_RDWR | meta.O_TRUNC,
				})
				if err != nil {
					hookErr = err
					return
				}
				_ = initMgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: openTruncRes.Fh})
			})
			return hookErr
		},
	}

	mgr := meta.New(store, meta.WithBlockStore(mockBS))

	// 触发 CompactChunk，阶段 2 CAS 发现前缀不匹配中止并清理已落盘的新物理块，返回 nil
	err = mgr.CompactChunk(ctx, ino, 0)
	assert.NoError(t, err)

	// 显式对 recordingBlockStore 断言新合并物理块确实经历了 PutBlock 计数 >= 1 且 DeleteBlock 计数与 PutBlock 相等（已回滚物理删块）
	mockBS.mu.Lock()
	putsCount := len(mockBS.puts)
	deletesCount := len(mockBS.deletes)
	compactedSliceIDs := append([]uint64(nil), mockBS.puts...)
	mockBS.mu.Unlock()

	assert.GreaterOrEqual(t, putsCount, 1, "阶段 1 新合并物理块必须至少经历 1 次 PutBlock 落盘")
	assert.Equal(t, putsCount, deletesCount, "DeleteBlock 计数必须与 PutBlock 相等（新合并块已彻底回滚物理删块）")

	// 验证磁盘无新合并块孤儿残留
	for _, sid := range compactedSliceIDs {
		pPath := blockFilePath(sid, 0, 16*1024)
		assert.NoFileExists(t, pPath, "回滚删块后磁盘上绝不能残留新合并物理块孤儿文件")
	}

	// 验证底表 chunks 中无残留
	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	assert.True(t, errors.Is(err, pgx.ErrNoRows) || len(chunkAfter.Slices) == 0, "底表 chunks 中不得残留切片")
}

// TestCompactChunk_RefZeroCollectedByGC 集成测试 4：
// CompactChunk 完成后旧 Slice 的 slice_refs.refs 归零，
// 随后调用 CollectGarbage 将旧物理块文件与 slice_refs 记录彻底回收，而新合并生成的物理块完好无损。
func TestCompactChunk_RefZeroCollectedByGC(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 连续覆写产生 3 个旧 Slice
	for i := 0; i < 3; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, 16*1024)
		_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
		require.NoError(t, err)
	}

	// 记录合并前的 chunk
	chunkBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(chunkBefore.Slices)
	require.NoError(t, err)
	require.Len(t, slicesBefore, 3)

	// 记录旧 slice_ids
	var oldSliceIDs []int64
	for _, s := range slicesBefore {
		oldSliceIDs = append(oldSliceIDs, int64(s.ID))
	}

	// 执行 CompactChunk
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	// 校验旧 slice_refs.refs 已经 <= 0
	for _, oldID := range oldSliceIDs {
		ref, err := store.GetSliceRef(ctx, oldID)
		require.NoError(t, err)
		assert.LessOrEqual(t, ref.Refs, int32(0))
	}

	// 校验新生成的 slice_refs.refs == 1
	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
	require.NoError(t, err)
	require.Len(t, slicesAfter, 1)
	newSliceID := int64(slicesAfter[0].ID)

	newRef, err := store.GetSliceRef(ctx, newSliceID)
	require.NoError(t, err)
	assert.Equal(t, int32(1), newRef.Refs)

	// 执行 GC 回收旧物理块
	reclaimed, err := mgr.CollectGarbage(ctx, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, reclaimed, len(oldSliceIDs))

	// 验证旧 slice_refs 已在数据库中被删除
	for _, oldID := range oldSliceIDs {
		_, err := store.GetSliceRef(ctx, oldID)
		assert.ErrorIs(t, err, pgx.ErrNoRows)
	}

	// 验证新切片完好无损，仍可正常读取
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   16 * 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{3}, 16*1024), readRes.Data)
}

// TestCompactChunk_AutoTriggerThreshold 集成测试 5：
// 配置较小的 WithCompactSliceThreshold(4)，连续执行 4 次 Write 自动异步触发 CompactChunk 并完成合并。
func TestCompactChunk_AutoTriggerThreshold(t *testing.T) {
	ctx := context.Background()
	store, bs, _, _, _ := setupPostgresForCompact(t)

	// 使用 Threshold = 4
	mgr := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入 4 次，每次覆写相同区间
	for i := 0; i < 4; i++ {
		data := bytes.Repeat([]byte{byte(i + 1)}, 8*1024)
		_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: data})
		require.NoError(t, err)
	}

	// 等待后台异步 Compact 完成
	var compacted bool
	for wait := 0; wait < 30; wait++ {
		time.Sleep(100 * time.Millisecond)
		chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		if err != nil {
			continue
		}
		slices, err := meta.DecodeSlices(chunkRow.Slices)
		if err != nil {
			continue
		}
		if len(slices) == 1 {
			compacted = true
			break
		}
	}
	assert.True(t, compacted, "异步 Compaction 应在达到阈值 4 后触发并将切片合并为 1 条")

	// 验证最终数据为第 4 次写入的数据 (0x04)
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   8 * 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{4}, 8*1024), readRes.Data)
}

// TestCompactChunk_Phase1GetBlockErrorReturnsErrIO 单元测试：阶段 1 GetBlock 读取失败立即中止并返回 ErrIO，不调用 NextSliceID 或 PutBlock
func TestCompactChunk_Phase1GetBlockErrorReturnsErrIO(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	setupMockWriteTx(mockStore, mockQuerier)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	getErr := fmt.Errorf("corrupted disk sector")
	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(getErr).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	require.Error(t, err)
	assert.ErrorIs(t, err, meta.ErrIO)
	mockStore.AssertNotCalled(t, "NextSliceID", mock.Anything)
	mockBS.AssertNotCalled(t, "PutBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

// TestCompactChunk_Phase1PutBlockErrorCleansUp 单元测试：阶段 1 PutBlock 出错立即清理并返回错误
func TestCompactChunk_Phase1PutBlockErrorCleansUp(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	setupMockWriteTx(mockStore, mockQuerier)

	// 1 条有效 Slice (Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024)
	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	// GetBlock 成功
	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	// 分配新 ID: 99
	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	// PutBlock 失败
	putErr := fmt.Errorf("disk full simulation")
	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(putErr).Once()

	// 应当调用 DeleteBlock 清理
	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	assert.Error(t, err)
	assert.ErrorIs(t, err, meta.ErrIO)
}

// TestCompactChunk_Phase2TxErrorCleansUp 单元测试：阶段 2 事务内更新失败清理已落盘的新物理块
func TestCompactChunk_Phase2TxErrorCleansUp(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 阶段 2 事务内锁定行
	mockQuerier2 := dbmock.NewMockQuerier(t)
	mockQuerier2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	// UpdateChunkSlices 模拟数据库错误
	mockQuerier2.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).
		Return(fmt.Errorf("db disk failure")).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			return fn(mockQuerier)
		}
		return fn(mockQuerier2)
	}).Twice()

	// 事务失败后，阶段 1 写入的新物理块 (sliceID=99) 必须被 DeleteBlock 清理
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(99), uint32(0), uint32(1024)).
		Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "db disk failure")
}

// TestCompactChunk_CASPrefixMismatchCleansUp 单元测试：CAS 前缀不匹配安全中止并清理新物理块
func TestCompactChunk_CASPrefixMismatchCleansUp(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 事务内锁定时发现已经被改写为完全不同的切片（例如打洞或覆盖截断）
	differentSlice := meta.Slice{Pos: 0, ID: 88, Size: 2048, Off: 0, Len: 2048}
	differentBytes := meta.EncodeSlice(differentSlice)

	mockQuerier2 := dbmock.NewMockQuerier(t)
	mockQuerier2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: differentBytes}, nil).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			return fn(mockQuerier)
		}
		return fn(mockQuerier2)
	}).Twice()

	// CAS 前缀不匹配中止时，新物理块必须被清理
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(99), uint32(0), uint32(1024)).
		Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	assert.NoError(t, err)
}

// TestCompactChunk_AllHoles 测试全空洞 Chunk 合并生成合法全空洞 Slice 且不产生任何物理 Block
func TestCompactChunk_AllHoles(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	recBS := &recordingBlockStore{realBS: realBS}
	mgr := meta.New(store, meta.WithBlockStore(recBS))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	defer cleanupCompactTestIno(context.Background(), store, fileRow.Ino)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 写入 16KB 数据
	_, err = mgr.Write(ctx, &meta.WriteRequest{Ino: ino, Fh: fh, Offset: 0, Data: bytes.Repeat([]byte{0xFF}, 16*1024)})
	require.NoError(t, err)

	// 打洞完全覆盖该 16KB（使整个 chunk 没有任何 ID > 0 的切片）
	err = mgr.Fallocate(ctx, meta.FallocateParams{
		Ino:    ino,
		Fh:     fh,
		Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		Offset: 0,
		Length: 16 * 1024,
	})
	require.NoError(t, err)

	recBS.mu.Lock()
	recBS.gets = nil
	recBS.puts = nil
	recBS.readOps = nil
	recBS.writeOps = nil
	recBS.mu.Unlock()

	// 执行 Compaction
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	recBS.mu.Lock()
	assert.Empty(t, recBS.gets, "全空洞 Chunk 合并不得读取物理 Block")
	assert.Empty(t, recBS.puts, "全空洞 Chunk 合并不得生成任何物理 Block")
	recBS.mu.Unlock()

	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
	require.NoError(t, err)
	require.Len(t, slicesAfter, 1)
	assert.Equal(t, uint64(0), slicesAfter[0].ID, "全空洞合并后仅保留 ID=0 的空洞标记 Slice")
	assert.Equal(t, uint32(meta.ChunkSize), slicesAfter[0].Len)

	// 读取全量为 0x00
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   16 * 1024,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{0}, 16*1024), readRes.Data)
}

type compactCommitAckErrorStore struct {
	db.Store
	onExecTxWithOptions func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error
}

func (s *compactCommitAckErrorStore) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	if s.onExecTxWithOptions != nil {
		return s.onExecTxWithOptions(ctx, fn, opts...)
	}
	return s.Store.ExecTxWithOptions(ctx, fn, opts...)
}

func (s *compactCommitAckErrorStore) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

type multiBlockCancelBlockStore struct {
	meta.BlockStore
	onPutBlock func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error
}

func (s *multiBlockCancelBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if s.onPutBlock != nil {
		if err := s.onPutBlock(ctx, sliceID, blockIdx, data); err != nil {
			return err
		}
	}
	return s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data)
}

func TestCompactChunk_CommitAckErrorAfterServerCommit_DoesNotDeleteCompactedBlock(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, bs, _, dataDir, _ := setupPostgresForCompact(t)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	initMgr := meta.New(store, meta.WithBlockStore(bs))
	openRes, err := initMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = initMgr.Release(context.Background(), meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
	})

	// 写入两段数据制造碎片切片
	data1 := bytes.Repeat([]byte("A"), 4096)
	_, err = initMgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   data1,
	})
	require.NoError(t, err)

	data2 := bytes.Repeat([]byte("B"), 4096)
	_, err = initMgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 4096,
		Data:   data2,
	})
	require.NoError(t, err)

	expectedData := append(data1, data2...)

	t.Run("CompactedBlockPreservedOnCommitAckError", func(t *testing.T) {
		mockAckErr := errors.New("commit ack network lost")
		var txCount int
		wrappedStore := &compactCommitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				txCount++
				if txCount == 1 {
					// 阶段 1 短读保护事务正常提交
					return store.ExecTxWithOptions(attemptCtx, fn, opts...)
				}
				// 阶段 2 写事务模拟 COMMIT ACK 丢失
				if err := store.ExecTxWithOptions(attemptCtx, fn, opts...); err != nil {
					return err
				}
				return mockAckErr
			},
		}
		mgr := meta.New(wrappedStore, meta.WithBlockStore(bs))

		compactErr := mgr.CompactChunk(ctx, ino, 0)
		require.Error(t, compactErr)
		assert.Contains(t, compactErr.Error(), "commit ack network lost")

		// 阶段 2 事务服务端已 COMMIT，元数据已替换为合并切片，已落盘物理块不得被误删
		readRes, readErr := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(expectedData)),
		})
		require.NoError(t, readErr, "阶段 2 已成功 COMMIT 的合并数据块绝不能因确认断连被误删，Read 必须成功且不报 ErrIO")
		require.NotNil(t, readRes)
		assert.Equal(t, expectedData, readRes.Data)
	})

	t.Run("InFlightServerCommitNotYetVisible_DoesNotDeleteCompactedBlock", func(t *testing.T) {
		subFileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		subIno := uint64(subFileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, subFileRow.Ino)
		})

		subOpenRes, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   subIno,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		subFh := subOpenRes.Fh
		t.Cleanup(func() {
			_ = initMgr.Release(context.Background(), meta.ReleaseParams{
				Ino: subIno,
				Fh:  subFh,
			})
		})

		d1 := bytes.Repeat([]byte("X"), 4096)
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    subIno,
			Fh:     subFh,
			Offset: 0,
			Data:   d1,
		})
		require.NoError(t, err)

		d2 := bytes.Repeat([]byte("Y"), 4096)
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    subIno,
			Fh:     subFh,
			Offset: 4096,
			Data:   d2,
		})
		require.NoError(t, err)

		subExpectedData := append(d1, d2...)

		allowServerCommitCh := make(chan struct{})
		var closeOnce sync.Once
		closeAllowCommit := func() {
			closeOnce.Do(func() {
				close(allowServerCommitCh)
			})
		}
		t.Cleanup(closeAllowCommit)

		reachedCommitCh := make(chan struct{})
		var onceReached sync.Once
		var bgErr error
		var bgWg sync.WaitGroup
		var txCount int
		var txMu sync.Mutex

		wrappedStore := &compactCommitAckErrorStore{
			Store: store,
			onExecTxWithOptions: func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				txMu.Lock()
				txCount++
				currentCount := txCount
				txMu.Unlock()

				if currentCount == 1 {
					// 阶段 1：短读只读事务正常提交
					return store.ExecTxWithOptions(attemptCtx, fn, opts...)
				}

				// 阶段 2：写事务在后台 goroutine 中执行
				bgWg.Add(1)
				go func() {
					defer bgWg.Done()
					bgCtx, bgCancel := context.WithTimeout(context.Background(), 15*time.Second)
					defer bgCancel()
					bgErr = store.ExecTxWithOptions(bgCtx, func(q db.Querier) error {
						if err := fn(q); err != nil {
							return err
						}
						onceReached.Do(func() {
							close(reachedCommitCh)
						})
						select {
						case <-allowServerCommitCh:
							return nil
						case <-bgCtx.Done():
							return bgCtx.Err()
						}
					}, opts...)
				}()

				select {
				case <-reachedCommitCh:
					// 立即向 CompactChunk 返回 DeadlineExceeded，模拟客户端先超时并走完补偿分支
					return context.DeadlineExceeded
				case <-attemptCtx.Done():
					return attemptCtx.Err()
				}
			},
		}

		mgr := meta.New(wrappedStore, meta.WithBlockStore(bs))
		compactErr := mgr.CompactChunk(ctx, subIno, 0)
		require.Error(t, compactErr)
		assert.True(t, errors.Is(compactErr, context.DeadlineExceeded) || strings.Contains(compactErr.Error(), context.DeadlineExceeded.Error()))

		// 放行后台独立 goroutine 执行真正的 PostgreSQL COMMIT 并等待其完成
		closeAllowCommit()
		bgWg.Wait()
		require.NoError(t, bgErr, "后台服务端事务 COMMIT 必须成功")

		// 阶段 2 事务服务端已 COMMIT，元数据已替换为合并切片，物理块不得被误删，
		// Read 必须能完整读出历史已提交数据！
		readRes, readErr := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    subIno,
			Fh:     subFh,
			Offset: 0,
			Size:   uint32(len(subExpectedData)),
		})
		require.NoError(t, readErr, "在途 COMMIT 的合并数据块绝不能因外部超时检查被误删，Read 必须成功且不报 ErrIO")
		require.NotNil(t, readRes)
		assert.Equal(t, subExpectedData, readRes.Data)
	})

	t.Run("MultiSegmentPutBlockErrorWithCanceledContext_CleansUpBlocks", func(t *testing.T) {
		// 构造同一 4 MiB Chunk 内的 2 个不相邻有效数据段（中间隔开稀疏空洞）：
		// Segment 0: [0, 64 KiB)
		// Segment 1: [128 KiB, 192 KiB)
		// 当第 2 个 Segment 的 PutBlock 报错且同时取消上下文时，第 1 个 Segment 已落盘的物理块必须被立即清理干净。
		fileRow2, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino2 := uint64(fileRow2.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow2.Ino)
		})

		openRes2, err := initMgr.Open(ctx, meta.OpenParams{
			Ino:   ino2,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh2 := openRes2.Fh
		t.Cleanup(func() {
			_ = initMgr.Release(context.Background(), meta.ReleaseParams{
				Ino: ino2,
				Fh:  fh2,
			})
		})

		part1 := bytes.Repeat([]byte{0x55}, 64*1024)
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino2,
			Fh:     fh2,
			Offset: 0,
			Data:   part1,
		})
		require.NoError(t, err)

		part2 := bytes.Repeat([]byte{0x66}, 64*1024)
		_, err = initMgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino2,
			Fh:     fh2,
			Offset: 128 * 1024,
			Data:   part2,
		})
		require.NoError(t, err)

		var initialFiles int
		_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
			if !d.IsDir() {
				initialFiles++
			}
			return nil
		})

		reqCtx, reqCancel := context.WithCancel(ctx)
		var putCall int
		wrappedBS := &multiBlockCancelBlockStore{
			BlockStore: bs,
			onPutBlock: func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
				putCall++
				if putCall == 2 {
					reqCancel()
					return errors.New("simulated put segment 1 disk error")
				}
				return nil
			},
		}
		mgr2 := meta.New(store, meta.WithBlockStore(wrappedBS))

		compactErr := mgr2.CompactChunk(reqCtx, ino2, 0)
		require.Error(t, compactErr)

		var currentFiles int
		_ = filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
			if !d.IsDir() {
				currentFiles++
			}
			return nil
		})
		assert.Equal(t, initialFiles, currentFiles, "即使上下文取消，多段合并中第 2 段 PutBlock 失败时第 1 段物理块必须被立即清理干净")
	})
}

func TestCompactChunk_TriggeredByPunchHoleAndTruncateAcrossRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	store, bs, _, _, _ := setupPostgresForCompact(t)

	t.Run("PunchHoleAccumulationTriggersCompaction", func(t *testing.T) {
		mgr := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{
				Ino: ino,
				Fh:  fh,
			})
		})

		// 1. 写入 16 KiB 初始非零数据
		initData := bytes.Repeat([]byte{0xAA}, 16*1024)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   initData,
		})
		require.NoError(t, err)

		// 2. 连续 3 次打洞（分别对 [0, 4096), [4096, 8192), [8192, 12288) 打洞）
		for i := 0; i < 3; i++ {
			err = mgr.Fallocate(ctx, meta.FallocateParams{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * 4096),
				Length: 4096,
				Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
			})
			require.NoError(t, err)
		}

		// 打洞完成后，加上初始写入切片，数据库中该 chunk 的 slices 累积达到 4 条切片（4 * 24 = 96 字节）
		// 断言异步 Compaction 被自动触发并压缩至 < 4 条切片
		require.Eventually(t, func() bool {
			chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			})
			if err != nil {
				return false
			}
			return len(chunkRow.Slices)/meta.SliceBytes < 4
		}, 2*time.Second, 20*time.Millisecond, "打洞累积切片达到阈值必须自动触发异步 Compaction 并压缩切片数至 < 4")

		// 验证 Read 读取数据精确无误：前 12 KiB 为 0，后 4 KiB 仍为 0xAA
		expected := make([]byte, 16*1024)
		copy(expected[12*1024:], bytes.Repeat([]byte{0xAA}, 4*1024))

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   16 * 1024,
		})
		require.NoError(t, err)
		assert.Equal(t, expected, readRes.Data)
	})

	t.Run("TruncateAcrossRestartTriggersCompaction", func(t *testing.T) {
		mgr1 := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o755),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr1.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr1.Release(context.Background(), meta.ReleaseParams{
				Ino: ino,
				Fh:  fh,
			})
		})

		// 用 mgr1 写入 3 次产生 3 条切片（文件总大小 12288）
		for i := 0; i < 3; i++ {
			_, err = mgr1.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * 4096),
				Data:   bytes.Repeat([]byte{byte(i + 1)}, 4096),
			})
			require.NoError(t, err)
		}

		// 创建全新 mgr2（模拟进程重启，丢失任何内存写入计数）
		mgr2 := meta.New(store, meta.WithBlockStore(bs), meta.WithCompactSliceThreshold(4))

		// 执行 1 次非对齐半块缩容截断（从 12288 截断至 10000），追加第 4 条空洞覆盖切片，达到阈值 4
		targetSize := uint64(10000)
		_, err = mgr2.SetAttr(ctx, ino, meta.SetAttrInput{
			Size: &targetSize,
		})
		require.NoError(t, err)

		// 断言跨重启后基于权威元数据切片数量，SetAttr 同样自动触发异步 Compaction 并压缩至 < 4
		require.Eventually(t, func() bool {
			chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{
				Ino:  int64(ino),
				Indx: 0,
			})
			if err != nil {
				return false
			}
			return len(chunkRow.Slices)/meta.SliceBytes < 4
		}, 2*time.Second, 20*time.Millisecond, "跨重启后半块截断达到切片阈值必须自动触发异步 Compaction 并压缩切片数至 < 4")
	})
}

// asyncCompactRerunsBlockStore 包装 BlockStore，精确拦截首轮后台异步 CompactChunk 的 PutBlock 一次。
type asyncCompactRerunsBlockStore struct {
	meta.BlockStore
	inFlightEntered chan struct{}
	releaseCompact  chan struct{}
	onceEntered     sync.Once
}

type fgWriteContextKey struct{}

func (s *asyncCompactRerunsBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	// 区分前台写入与后台异步 Compaction：
	// 前台 Write 显式注入 fgWriteContextKey，后台异步 Compact 使用独立的 context.WithTimeout，无此 key。
	if ctx.Value(fgWriteContextKey{}) == nil {
		s.onceEntered.Do(func() {
			close(s.inFlightEntered)
		})
		select {
		case <-s.releaseCompact:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data)
}

func TestAsyncCompact_RerunsWhenConcurrentWritesExceedThresholdDuringInFlightCompact(t *testing.T) {
	ctx := context.Background()
	store, rawBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	inFlightEntered := make(chan struct{})
	releaseFirstCompact := make(chan struct{})
	var closeOnce sync.Once
	releaseCompact := func() {
		closeOnce.Do(func() {
			close(releaseFirstCompact)
		})
	}
	t.Cleanup(releaseCompact)

	interceptBS := &asyncCompactRerunsBlockStore{
		BlockStore:      rawBS,
		inFlightEntered: inFlightEntered,
		releaseCompact:  releaseFirstCompact,
	}

	mgr := meta.New(store, meta.WithBlockStore(interceptBS), meta.WithCompactSliceThreshold(4))

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
	})

	fgCtx := context.WithValue(ctx, fgWriteContextKey{}, true)

	// 前台连续执行 4 次 Write，触发首轮异步 CompactChunk
	for i := 0; i < 4; i++ {
		writeData := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 4096),
			Data:   writeData,
		})
		require.NoError(t, err)
	}

	// 等待首轮后台异步 CompactChunk 进入 PutBlock（此时 inFlightCompacts 中已登记该 chunkKey）
	select {
	case <-inFlightEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("超时: 首轮后台异步 CompactChunk 未在预期时间内进入 PutBlock")
	}

	// 在途运行期间，前台立即在同一 Chunk 0 上再连续执行 4 次新的 Write
	// 使 concurrentSuffix 自身达到 4 条 Slice >= 阈值 4，且每次 maybeTriggerAsyncCompact 均撞上在途 inFlightCompacts 被丢弃
	for i := 4; i < 8; i++ {
		writeData := bytes.Repeat([]byte{byte(i + 1)}, 4096)
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 4096),
			Data:   writeData,
		})
		require.NoError(t, err)
	}

	// 前台彻底停止一切后续写入，放行首轮后台 Compaction 完成
	releaseCompact()

	// 首轮 CAS 提交后，Chunk 0 剩 1 条合并前缀 + 4 条并发后缀 = 5 条 Slice，仍 >= 阈值 4
	// 断言无需任何新前台写入，后台自动续跑第 2 轮 Compaction 将切片数收敛至 1
	require.Eventually(t, func() bool {
		chunkRow, err := store.GetChunk(context.Background(), db.GetChunkParams{
			Ino:  int64(ino),
			Indx: 0,
		})
		if err != nil {
			return false
		}
		sliceCount := len(chunkRow.Slices) / meta.SliceBytes
		return sliceCount == 1
	}, 3*time.Second, 25*time.Millisecond, "在途 Compaction 遇突发超阈值写入，停止写入后必须自动续跑收敛切片数至 1")

	// 验证数据完整一致
	var expectedData []byte
	for i := 0; i < 8; i++ {
		expectedData = append(expectedData, bytes.Repeat([]byte{byte(i + 1)}, 4096)...)
	}
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(expectedData)),
	})
	require.NoError(t, err)
	assert.Equal(t, expectedData, readRes.Data)
}

// TestCompactChunk_SliceRefLockRetryAndExhaustion Mock 单测验证 CompactChunk 的 LockValidSliceRefsKeyShare 重试与耗尽行为
func TestCompactChunk_SliceRefLockRetryAndExhaustion(t *testing.T) {
	ctx := context.Background()

	t.Run("首次锁返回不足重读 GetChunk 成功后完成合并", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier1 := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)

		slices1011 := append(
			meta.EncodeSlice(meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}),
			meta.EncodeSlice(meta.Slice{Pos: 1024, ID: 11, Size: 1024, Off: 0, Len: 1024})...,
		)
		slice20 := meta.EncodeSlice(
			meta.Slice{Pos: 0, ID: 20, Size: 2048, Off: 0, Len: 2048},
		)

		// 阶段 1：读事务 attempt 0 查出 [10, 11]
		mockQuerier1.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slices1011}, nil).Once()

		// 锁定不足：锁定 [10, 11] 仅返回 [10]
		mockQuerier1.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10, 11}).
			Return([]int64{10}, nil).Once()

		// attempt 1 触发重读 GetChunk 拿到新 slice [20]
		mockQuerier1.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice20}, nil).Once()

		// 成功锁定 [20]
		mockQuerier1.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{20}).
			Return([]int64{20}, nil).Once()

		// 仅从 sliceID=20 读取数据，绝不调用 10 或 11
		mockBS.EXPECT().GetBlock(mock.Anything, uint64(20), uint32(0), uint32(2048), uint32(0), mock.Anything).
			Return(nil).Once()

		mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()
		mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).Return(nil).Once()

		// 阶段 2：写事务
		mockQuerier2 := dbmock.NewMockQuerier(t)
		mockQuerier2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice20}, nil).Once()

		mockQuerier2.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).Return(nil).Once()
		mockQuerier2.EXPECT().InsertSliceRef(mock.Anything, mock.Anything).Return(nil).Once()
		mockQuerier2.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
			SliceID: 20,
			Delta:   -1,
		}).Return(nil).Once()

		var txCallCount int
		mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
			txCallCount++
			if txCallCount == 1 {
				return fn(mockQuerier1)
			}
			return fn(mockQuerier2)
		}).Twice()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		err := mgr.CompactChunk(ctx, 1, 0)
		require.NoError(t, err)

		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, uint64(10), mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, uint64(11), mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("锁查询报错立即中止且不调 NextSliceID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)

		setupMockWriteTx(mockStore, mockQuerier)

		slice10 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024})
		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice10}, nil).Once()

		lockErr := errors.New("database connection refused on lock")
		mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
			Return(nil, lockErr).Once()

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		err := mgr.CompactChunk(ctx, 1, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "lock valid slice refs for compact")
		mockStore.AssertNotCalled(t, "NextSliceID", mock.Anything)
		mockBS.AssertNotCalled(t, "GetBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything)
		mockBS.AssertNotCalled(t, "PutBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})

	t.Run("连续达最大重试上限 8 次锁定不足后返回 meta.ErrIO", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mockBS := metamock.NewMockBlockStore(t)

		setupMockWriteTx(mockStore, mockQuerier)

		slice10 := meta.EncodeSlice(meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024})
		mockQuerier.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
			Ino:  1,
			Indx: 0,
		}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: slice10}, nil).Times(8)

		// 8 次 LockValidSliceRefsKeyShare 返回空切片
		mockQuerier.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
			Return([]int64{}, nil).Times(8)

		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
		err := mgr.CompactChunk(ctx, 1, 0)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrIO)
		mockStore.AssertNotCalled(t, "NextSliceID", mock.Anything)
		mockBS.AssertNotCalled(t, "PutBlock", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
	})
}

// TestCompactChunk_TxRetryResetsEverReachedCommit_CleansUpBlockOnSecondAttemptFailure 验证写事务重试首行重置 everReachedCommit 并精确清理物理块
func TestCompactChunk_TxRetryResetsEverReachedCommit_CleansUpBlockOnSecondAttemptFailure(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mockQuerier1 := dbmock.NewMockQuerier(t)
	mockBS := metamock.NewMockBlockStore(t)

	origSlice := meta.Slice{Pos: 0, ID: 10, Size: 1024, Off: 0, Len: 1024}
	origBytes := meta.EncodeSlice(origSlice)

	// 阶段 1：读事务
	mockQuerier1.EXPECT().GetChunk(mock.Anything, db.GetChunkParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier1.EXPECT().LockValidSliceRefsKeyShare(mock.Anything, []int64{10}).
		Return([]int64{10}, nil).Once()

	mockBS.EXPECT().GetBlock(mock.Anything, uint64(10), uint32(0), uint32(1024), uint32(0), mock.Anything).
		Return(nil).Once()

	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(99), nil).Once()

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(99), uint32(0), mock.Anything).
		Return(nil).Once()

	// 阶段 2 事务第 1 次尝试
	mockQuerier2_1 := dbmock.NewMockQuerier(t)
	mockQuerier2_1.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{Ino: 1, Indx: 0, Slices: origBytes}, nil).Once()

	mockQuerier2_1.EXPECT().UpdateChunkSlices(mock.Anything, mock.Anything).Return(nil).Once()
	mockQuerier2_1.EXPECT().InsertSliceRef(mock.Anything, mock.Anything).Return(nil).Once()
	mockQuerier2_1.EXPECT().AddSliceRefCount(mock.Anything, db.AddSliceRefCountParams{
		SliceID: 10,
		Delta:   -1,
	}).Return(nil).Once()

	// 阶段 2 事务第 2 次尝试（在 GetChunkForUpdate 直接返回不可重试 DB 错误）
	mockQuerier2_2 := dbmock.NewMockQuerier(t)
	dbErr := errors.New("fatal db connection failure on retry")
	mockQuerier2_2.EXPECT().GetChunkForUpdate(mock.Anything, db.GetChunkForUpdateParams{
		Ino:  1,
		Indx: 0,
	}).Return(db.Chunk{}, dbErr).Once()

	var txCallCount int
	mockStore.EXPECT().ExecTxWithOptions(mock.Anything, mock.Anything).RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
		txCallCount++
		if txCallCount == 1 {
			// 阶段 1 读事务
			return fn(mockQuerier1)
		}
		if txCallCount == 2 {
			// 阶段 2 写事务第 1 次执行：完整执行 fn(mockQuerier2_1)，置 everReachedCommit = true，但事务提交返回 40001
			if err := fn(mockQuerier2_1); err != nil {
				return err
			}
			return &pgconn.PgError{Code: "40001"}
		}
		// 阶段 2 写事务第 2 次重试
		return fn(mockQuerier2_2)
	}).Times(3)

	// 精确调用 1 次 DeleteBlock 清理阶段 1 写入的新物理块 (sliceID=99)
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(99), uint32(0), uint32(1024)).Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	err := mgr.CompactChunk(ctx, 1, 0)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "fatal db connection failure on retry")
}

// TestCompactChunk_HighSparseChunkIndxBeyondInt32_PostgresIntegration 验证超过 128 PiB
// (indx >= 1<<31) 的高稀疏偏移 Chunk 碎片整理：CompactChunk 正常接受 64 位 indx 并完成合并，
// 且绝不污染 indx=0 的数据。
func TestCompactChunk_HighSparseChunkIndxBeyondInt32_PostgresIntegration(t *testing.T) {
	ctx := context.Background()
	store, _, mgr, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	highIndx := int64(1) << 31 // 2147483648 (offset = 128 PiB = 1<<57)
	highOffset := uint64(highIndx) * uint64(meta.ChunkSize)

	t.Cleanup(func() {
		cleanupCtx := context.Background()
		for _, indx := range []int64{0, highIndx} {
			chunkRow, cErr := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: fileRow.Ino, Indx: indx})
			if cErr == nil && len(chunkRow.Slices) > 0 {
				if slices, dErr := meta.DecodeSlices(chunkRow.Slices); dErr == nil {
					for _, s := range slices {
						if s.ID > 0 {
							_ = store.DeleteSliceRef(cleanupCtx, int64(s.ID))
						}
					}
				}
			}
		}
		_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{fileRow.Ino})
		_ = store.DeleteInode(cleanupCtx, fileRow.Ino)
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	defer func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	}()

	// 在 indx=0 写入对照数据
	lowData := []byte("origin-low-chunk-data")
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   lowData,
	})
	require.NoError(t, err)

	// 在 highIndx 连续写入并局部覆写产生 2 条碎片 Slice
	partA := bytes.Repeat([]byte("A"), 2048)
	partB := bytes.Repeat([]byte("B"), 1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset,
		Data:   partA,
	})
	require.NoError(t, err)

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset + 512,
		Data:   partB,
	})
	require.NoError(t, err)

	chunkBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: highIndx})
	require.NoError(t, err)
	assert.Equal(t, 2*meta.SliceBytes, len(chunkBefore.Slices), "合并前 highIndx 应包含 2 条 Slice")

	// 调用 CompactChunk(ctx, ino, highIndx)
	err = mgr.CompactChunk(ctx, ino, highIndx)
	require.NoError(t, err, "CompactChunk 必须支持 indx >= 1<<31 的 64 位块索引")

	chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: highIndx})
	require.NoError(t, err)
	assert.Equal(t, meta.SliceBytes, len(chunkAfter.Slices), "合并后 highIndx 应缩减为 1 条紧致 Slice")

	// 验证合并后 highIndx 读取数据精确一致，且 indx=0 未受影响
	expectedHigh := make([]byte, 2048)
	copy(expectedHigh, partA)
	copy(expectedHigh[512:1536], partB)

	readHigh, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset,
		Size:   2048,
	})
	require.NoError(t, err)
	assert.Equal(t, expectedHigh, readHigh.Data)

	readLow, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(lowData)),
	})
	require.NoError(t, err)
	assert.Equal(t, lowData, readLow.Data)
}

func readFullFileForCompactTest(t *testing.T, ctx context.Context, mgr meta.Manager, ino uint64, fh uint64, totalSize int) []byte {
	t.Helper()
	out := make([]byte, 0, totalSize)
	for off := 0; off < totalSize; {
		chunkLen := min(int(meta.BlockSize), totalSize-off)
		res, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(off),
			Size:   uint32(chunkLen),
		})
		require.NoError(t, err)
		require.Len(t, res.Data, chunkLen)
		out = append(out, res.Data...)
		off += chunkLen
	}
	return out
}

// TestCompactChunk_SequentialWrites_1MiBAnd128KiB_ZeroBackgroundRewrite 验收标准：
// 以 1 MiB 步长和 128 KiB 步长分别顺序写满一个 4 MiB Chunk，确认产生 4 条和 32 条 Slice，
// 且记录型 BlockStore 未观察到后台额外重写（零后台合并），Read 完整读回 4 MiB 数据。
func TestCompactChunk_SequentialWrites_1MiBAnd128KiB_ZeroBackgroundRewrite(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	steps := []struct {
		name      string
		stepBytes int
		wantCount int
	}{
		{
			name:      "1MiB步长顺序写满4MiB产生4条Slice零后台合并",
			stepBytes: 1 << 20, // 1 MiB
			wantCount: 4,
		},
		{
			name:      "128KiB步长顺序写满4MiB产生32条Slice零后台合并",
			stepBytes: 128 * 1024, // 128 KiB
			wantCount: 32,
		},
	}

	for _, tc := range steps {
		t.Run(tc.name, func(t *testing.T) {
			recBS := &recordingBlockStore{realBS: realBS}
			// 使用默认阈值（64）
			mgr := meta.New(store, meta.WithBlockStore(recBS))
			workerCtx, workerCancel := context.WithCancel(ctx)
			defer workerCancel()
			mgr.StartGCWorker(workerCtx, time.Minute, 50)

			fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
				Mode:  int32(meta.S_IFREG | 0o644),
				Nlink: 1,
				Uid:   1000,
				Gid:   1000,
			})
			require.NoError(t, err)
			ino := uint64(fileRow.Ino)
			t.Cleanup(func() {
				cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
			})

			openRes, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   ino,
				Flags: meta.O_RDWR,
			})
			require.NoError(t, err)
			fh := openRes.Fh
			t.Cleanup(func() {
				_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
			})

			expected := make([]byte, int(meta.ChunkSize))
			for i := 0; i < tc.wantCount; i++ {
				buf := bytes.Repeat([]byte{byte(0x10 + (i % 200))}, tc.stepBytes)
				offset := uint64(i * tc.stepBytes)
				_, err := mgr.Write(ctx, &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: offset,
					Data:   buf,
				})
				require.NoError(t, err)
				copy(expected[i*tc.stepBytes:(i+1)*tc.stepBytes], buf)
			}

			// 短暂等待确认后台 Worker 绝不会对 4 条或 32 条 Slice 发起任何自动合并
			time.Sleep(60 * time.Millisecond)

			chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
			require.NoError(t, err)
			slicesNow, err := meta.DecodeSlices(chunkRow.Slices)
			require.NoError(t, err)
			require.Len(t, slicesNow, tc.wantCount, "顺序写满 4 MiB Chunk 后应精确保留 %d 条 Slice", tc.wantCount)

			// 断言期间零 GetBlock 读取、零额外 PutBlock 重写（总 PutBlock 仅等于 tc.wantCount 次前台 Write）
			recBS.mu.Lock()
			getsCount := len(recBS.gets)
			putsCount := len(recBS.puts)
			deletesCount := len(recBS.deletes)
			recBS.mu.Unlock()

			assert.Equal(t, 0, getsCount, "%d 条顺序切片不得触发后台读取物理 Block", tc.wantCount)
			assert.Equal(t, tc.wantCount, putsCount, "%d 条顺序切片不得触发额外后台写入物理 Block", tc.wantCount)
			assert.Equal(t, 0, deletesCount, "%d 条顺序切片不得触发后台删除物理 Block", tc.wantCount)

			for _, s := range slicesNow {
				ref, err := store.GetSliceRef(ctx, int64(s.ID))
				require.NoError(t, err)
				assert.Equal(t, int32(1), ref.Refs, "slice_id=%d 的引用计数必须保持为 1", s.ID)
			}

			// 验证 Read 完整读回 4 MiB 数据
			readData := readFullFileForCompactTest(t, ctx, mgr, ino, fh, len(expected))
			assert.Equal(t, expected, readData, "Read 必须完整读回 4 MiB 顺序写入数据")
		})
	}
}

// TestCompactChunk_DefaultThreshold64_FullSnapshotAndNoRepeatOnPrefixOverwrite 验收标准：
//  1. 在真实自动触发入口（Write，默认阈值 64）验证第 63 条不合并、第 64 条触发后台整体合并；
//  2. 合并压平为 1 条 <= 4 MiB Slice 后，再追加少量覆写（包括覆盖 offset 0 起始位置的前缀），
//     原始 Slice 总数远低于 64，确认不会触发重复合并（根除原 skipCount == 0 漏洞）；
//  3. 排队期间若继续写入累积超过 64 条（如 68 条），合并时一次性处理已读取的完整快照并压平为 1 条紧致 Slice。
func TestCompactChunk_DefaultThreshold64_FullSnapshotAndNoRepeatOnPrefixOverwrite(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	t.Run("63条不合并_第64条触发整体合并_且合并后覆盖offset0不触发重复合并", func(t *testing.T) {
		recBS := &recordingBlockStore{realBS: realBS}
		// 不传 WithCompactSliceThreshold，直接验证默认阈值 CompactSliceThreshold (64)
		mgr := meta.New(store, meta.WithBlockStore(recBS))

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
		})

		// 先写入 1 条 1 MiB 起始大块 + 62 条 16 KiB 写入 = 共 63 条 Slice
		const firstLen = 1 << 20 // 1 MiB
		const stepLen = 16 * 1024
		totalLogicalSize := firstLen + 63*stepLen
		expected := make([]byte, totalLogicalSize)

		firstData := bytes.Repeat([]byte{0x11}, firstLen)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   firstData,
		})
		require.NoError(t, err)
		copy(expected[0:firstLen], firstData)

		for i := 0; i < 62; i++ {
			off := firstLen + i*stepLen
			buf := bytes.Repeat([]byte{byte(0x20 + (i % 90))}, stepLen)
			_, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(off),
				Data:   buf,
			})
			require.NoError(t, err)
			copy(expected[off:off+stepLen], buf)
		}

		// 第 63 条 Slice 写入后：等待并确认绝不会触发自动合并
		time.Sleep(80 * time.Millisecond)
		chunkAt63, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesAt63, err := meta.DecodeSlices(chunkAt63.Slices)
		require.NoError(t, err)
		require.Len(t, slicesAt63, 63, "累积 63 条 Slice 时低于默认阈值 64，不得自动合并")

		recBS.mu.Lock()
		assert.Empty(t, recBS.gets, "63 条 Slice 时后台不得发起任何 GetBlock")
		assert.Len(t, recBS.puts, 63, "63 条 Slice 时 PutBlock 次数必须严格等于 63 次前台写入")
		recBS.mu.Unlock()

		// 记录前 63 条旧 Slice ID
		oldSliceIDs := make([]int64, 0, 64)
		for _, s := range slicesAt63 {
			oldSliceIDs = append(oldSliceIDs, int64(s.ID))
		}

		// 写入第 64 条 Slice：达到默认阈值 64，自动触发后台整体合并
		off64 := firstLen + 62*stepLen
		buf64 := bytes.Repeat([]byte{0xEE}, stepLen)
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(off64),
			Data:   buf64,
		})
		require.NoError(t, err)
		copy(expected[off64:off64+stepLen], buf64)

		// 等待后台整体合并将全部 64 条 Slice（含头部 1 MiB Slice）一次性压平为 1 条紧致 Slice
		var compactedSlice meta.Slice
		require.Eventually(t, func() bool {
			c, gErr := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
			if gErr != nil {
				return false
			}
			sl, dErr := meta.DecodeSlices(c.Slices)
			if dErr != nil || len(sl) != 1 {
				return false
			}
			compactedSlice = sl[0]
			return true
		}, 5*time.Second, 20*time.Millisecond, "第 64 条 Slice 写入后必须自动触发后台整体合并并压平为 1 条紧致 Slice")

		assert.Equal(t, uint32(0), compactedSlice.Pos)
		assert.Equal(t, uint32(totalLogicalSize), compactedSlice.Len)
		assert.LessOrEqual(t, compactedSlice.Len, uint32(meta.ChunkSize), "合并后的单条紧致 Slice 长度不得超过 4 MiB")

		// 全部旧 Slice（含第 1 条 1 MiB Slice）的 slice_refs 均应被扣减并由同一后台 Worker 立即回收
		require.Eventually(t, func() bool {
			for _, oldID := range oldSliceIDs {
				_, gErr := store.GetSliceRef(ctx, oldID)
				if !errors.Is(gErr, pgx.ErrNoRows) {
					return false
				}
			}
			return true
		}, 5*time.Second, 20*time.Millisecond, "被整体合并替换的全部旧 Slice 均应被扣减引用并立即回收")

		// 清空 recordingBlockStore 统计，测试合并后再追加少量覆写（含覆盖 offset 0 起始前缀）
		recBS.mu.Lock()
		recBS.gets = nil
		recBS.puts = nil
		recBS.deletes = nil
		recBS.mu.Unlock()

		// 在 offset 0 起始位置及中间位置追加 3 次小覆写（若仍保留旧 skipSome 漏洞，覆盖 offset 0 会使 skipCount==0 导致误触发重写）
		overwrites := []struct {
			off  int
			data []byte
		}{
			{off: 0, data: bytes.Repeat([]byte{0x91}, 4096)},
			{off: 8192, data: bytes.Repeat([]byte{0x92}, 4096)},
			{off: 65536, data: bytes.Repeat([]byte{0x93}, 4096)},
		}
		for _, ow := range overwrites {
			_, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(ow.off),
				Data:   ow.data,
			})
			require.NoError(t, err)
			copy(expected[ow.off:ow.off+len(ow.data)], ow.data)
		}

		time.Sleep(80 * time.Millisecond)

		chunkAfterOw, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesAfterOw, err := meta.DecodeSlices(chunkAfterOw.Slices)
		require.NoError(t, err)
		require.Len(t, slicesAfterOw, 1+len(overwrites), "合并后追加 3 次覆写（总计 4 条 < 64）绝不能触发重复合并")

		recBS.mu.Lock()
		assert.Empty(t, recBS.gets, "合并后少量覆写不得触发任何后台 GetBlock 读取")
		assert.Len(t, recBS.puts, len(overwrites), "合并后少量覆写只能产生前台自身的 PutBlock")
		recBS.mu.Unlock()

		readData := readFullFileForCompactTest(t, ctx, mgr, ino, fh, len(expected))
		assert.Equal(t, expected, readData, "Read 必须精确返回包含全部覆写的最新字节")
	})

	t.Run("排队期间累积超过64条时一次性合并完整已读快照", func(t *testing.T) {
		var (
			chunk0BarrierEntered = make(chan struct{})
			releaseChunk0Barrier = make(chan struct{})
			enteredOnce          sync.Once
			releaseOnce          sync.Once
		)
		t.Cleanup(func() {
			releaseOnce.Do(func() { close(releaseChunk0Barrier) })
		})

		barrierBS := &recordingBlockStore{
			realBS: realBS,
			onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
				if putCtx.Value(fgWriteContextKey{}) != nil {
					return nil
				}
				enteredOnce.Do(func() {
					close(chunk0BarrierEntered)
					<-releaseChunk0Barrier
				})
				return nil
			},
		}

		// 默认阈值 64
		mgr := meta.New(store, meta.WithBlockStore(barrierBS))
		fgCtx := context.WithValue(ctx, fgWriteContextKey{}, true)

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
		})

		// 1. 先向 Chunk 0 写入 64 条触发后台 Worker 并停留在 Chunk 0 的 PutBlock 屏障中
		for i := 0; i < 64; i++ {
			_, err := mgr.Write(fgCtx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * 1024),
				Data:   bytes.Repeat([]byte{byte(0x30 + (i % 50))}, 1024),
			})
			require.NoError(t, err)
		}

		select {
		case <-chunk0BarrierEntered:
		case <-time.After(5 * time.Second):
			t.Fatal("超时: Chunk 0 未进入后台合并屏障")
		}

		// 2. 在 Chunk 0 占用唯一 Worker 期间，向 Chunk 1 连续写入 68 条 Slice（排队等待期间积累超过 64 条）
		const chunk1Slices = 68
		const step = 4096
		chunk1Base := uint64(meta.ChunkSize)
		expectedChunk1 := make([]byte, chunk1Slices*step)
		for i := 0; i < chunk1Slices; i++ {
			buf := bytes.Repeat([]byte{byte(0x50 + (i % 70))}, step)
			_, err := mgr.Write(fgCtx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: chunk1Base + uint64(i*step),
				Data:   buf,
			})
			require.NoError(t, err)
			copy(expectedChunk1[i*step:(i+1)*step], buf)
		}

		// 确认放行前 Chunk 1 在数据库中确实累积了 68 条原始 Slice
		c1Before, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
		require.NoError(t, err)
		assert.Equal(t, chunk1Slices, len(c1Before.Slices)/meta.SliceBytes)

		// 3. 放行屏障，Worker 处理 Chunk 1 时一次性读取并合并全部 68 条快照为 1 条紧致 Slice
		releaseOnce.Do(func() { close(releaseChunk0Barrier) })

		require.Eventually(t, func() bool {
			c1, gErr := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
			if gErr != nil {
				return false
			}
			return len(c1.Slices)/meta.SliceBytes == 1
		}, 5*time.Second, 20*time.Millisecond, "排队期间积累的 68 条 Slice 应一次性合并为 1 条紧致 Slice")

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: chunk1Base,
			Size:   uint32(len(expectedChunk1)),
		})
		require.NoError(t, err)
		assert.Equal(t, expectedChunk1, readRes.Data)
	})
}

func TestSingleWorker_SerializesMultipleChunkCompactionsAndGC_NonBlockingEnqueue(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	var (
		activeStorageOps atomic.Int32
		maxConcurrentOps atomic.Int32
		chunk0EnteredCh  = make(chan struct{})
		releaseBarrierCh = make(chan struct{})
		enteredOnce      sync.Once
		releaseOnce      sync.Once
	)
	t.Cleanup(func() {
		releaseOnce.Do(func() { close(releaseBarrierCh) })
	})

	recordConcurrency := func() func() {
		cur := activeStorageOps.Add(1)
		for {
			prev := maxConcurrentOps.Load()
			if cur <= prev || maxConcurrentOps.CompareAndSwap(prev, cur) {
				break
			}
		}
		return func() {
			activeStorageOps.Add(-1)
		}
	}

	barrierBS := &recordingBlockStore{
		realBS: realBS,
		onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
			if putCtx.Value(fgWriteContextKey{}) != nil {
				return nil
			}
			done := recordConcurrency()
			defer done()

			enteredOnce.Do(func() {
				close(chunk0EnteredCh)
			})
			select {
			case <-releaseBarrierCh:
				return nil
			case <-putCtx.Done():
				return putCtx.Err()
			}
		},
	}

	mgr := meta.New(
		store,
		meta.WithBlockStore(barrierBS),
		meta.WithCompactSliceThreshold(4),
		meta.WithGCInterval(30*time.Millisecond),
	)
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	mgr.StartGCWorker(workerCtx, 30*time.Millisecond, 50)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	fgCtx := context.WithValue(ctx, fgWriteContextKey{}, true)

	// 1. 触发 Chunk 0 的自动合并并使其停留在后台 PutBlock 屏障上
	for i := 0; i < 4; i++ {
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 4096),
			Data:   bytes.Repeat([]byte{byte(0x10 + i)}, 4096),
		})
		require.NoError(t, err)
	}

	select {
	case <-chunk0EnteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("超时: Chunk 0 后台合并未进入 PutBlock 屏障")
	}

	// 2. 在 Chunk 0 后台合并被阻塞期间，向 Chunk 1 和 Chunk 2 各写入 4 次触发自动合并，
	// 验证前台投递非阻塞立即返回，且后台唯一 Worker 不会并发启动第 2、第 3 个合并协程。
	enqueueStart := time.Now()
	for chunkIndx := int64(1); chunkIndx <= 2; chunkIndx++ {
		baseOff := uint64(chunkIndx) * uint64(meta.ChunkSize)
		for i := 0; i < 4; i++ {
			_, err := mgr.Write(fgCtx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: baseOff + uint64(i*4096),
				Data:   bytes.Repeat([]byte{byte(0x20 + int(chunkIndx)*4 + i)}, 4096),
			})
			require.NoError(t, err)
		}
	}
	assert.Less(t, time.Since(enqueueStart), 2*time.Second, "前台任务投递不能等待被阻塞的后台合并完成")

	// 给一点时间观察是否有其他后台协程并发进入 PutBlock
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, int32(1), maxConcurrentOps.Load(), "唯一后台 Worker 在屏障阻塞期间绝不能并发执行其他 Chunk 的合并操作")

	// 3. 放行屏障，等待 Chunk 0、1、2 依次完成合并
	releaseOnce.Do(func() { close(releaseBarrierCh) })

	for chunkIndx := int64(0); chunkIndx <= 2; chunkIndx++ {
		idx := chunkIndx
		require.Eventually(t, func() bool {
			c, gErr := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: idx})
			if gErr != nil {
				return false
			}
			return len(c.Slices)/meta.SliceBytes == 1
		}, 5*time.Second, 25*time.Millisecond, "Chunk %d 应当在单 Worker 队列中依次得到合并", idx)
	}

	assert.Equal(t, int32(1), maxConcurrentOps.Load(), "全过程后台存储操作的最大并发数必须严格为 1")
}

// TestSingleWorker_ImmediatePostCompactBlockReclamation_ProtectsSharedAndInFlightRead_AndRetriesDeleteError
// 验证验收标准 4、5：
// 1. 后台 Worker 成功合并并扣减引用后，在同一个 Worker 内立即推进 refs <= 0 的废弃 Block 物理删除与 slice_refs 清理（无需等待长 gcInterval）；
// 2. 仍有引用（refs > 0）或受在途读取保护（isSliceReadInFlight）的 Block 绝不提前删除；在途读取释放后可被后续回收；
// 3. 物理删除失败时保留 slice_refs 记录供后续重试，不谎报完成且不阻塞后续 Chunk 合并。
func TestSingleWorker_ImmediatePostCompactBlockReclamation_ProtectsSharedAndInFlightRead_AndRetriesDeleteError(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, tmpDir, pool := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	blockFilePath := func(sliceID uint64, blockIdx uint32, size uint32) string {
		h1 := sliceID % 256
		h2 := sliceID / 1000000
		return filepath.Join(tmpDir, "chunks", fmt.Sprintf("%02X", h1), fmt.Sprintf("%d", h2), fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, size))
	}

	var (
		failDeleteSliceID atomic.Uint64
		readInFlightID    atomic.Uint64
		readEnteredCh     = make(chan struct{})
		releaseReadCh     = make(chan struct{})
		readEnteredOnce   sync.Once
		releaseReadOnce   sync.Once
	)
	t.Cleanup(func() {
		releaseReadOnce.Do(func() { close(releaseReadCh) })
	})

	hookBS := &recordingBlockStore{
		realBS: realBS,
		onBeforeGetBlock: func(getCtx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32, blockOff uint32) error {
			if getCtx.Value(fgWriteContextKey{}) != nil && sliceID == readInFlightID.Load() {
				readEnteredOnce.Do(func() { close(readEnteredCh) })
				select {
				case <-releaseReadCh:
				case <-getCtx.Done():
					return getCtx.Err()
				}
			}
			return nil
		},
		onBeforeDeleteBlock: func(delCtx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
			if failID := failDeleteSliceID.Load(); failID != 0 && sliceID == failID {
				return fmt.Errorf("%w: injected delete failure for slice %d", meta.ErrIO, sliceID)
			}
			return nil
		},
	}

	// 将 gcInterval 设为极长（10 分钟），证明废弃块回收是由合并完成后的同一 Worker 立即触发，而非依赖周期 GC 定时器
	mgr := meta.New(
		store,
		meta.WithBlockStore(hookBS),
		meta.WithCompactSliceThreshold(4),
		meta.WithGCInterval(10*time.Minute),
	)
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	mgr.StartGCWorker(workerCtx, 10*time.Minute, 50)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	// 先在 Chunk 0 写入 3 个 4KiB 切片（低于阈值 4，尚未触发自动合并）
	for i := 0; i < 3; i++ {
		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 4096),
			Data:   bytes.Repeat([]byte{byte(0x31 + i)}, 4096),
		})
		require.NoError(t, err)
	}

	cBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	slicesBefore, err := meta.DecodeSlices(cBefore.Slices)
	require.NoError(t, err)
	require.Len(t, slicesBefore, 3)

	// S0: 模拟仍有额外引用的共享切片（refs 从 1 增加到 2，合并扣减 1 后仍剩 refs == 1）
	sharedSliceID := slicesBefore[0].ID
	_, err = pool.Exec(ctx, "UPDATE slice_refs SET refs = refs + 1 WHERE slice_id = $1", int64(sharedSliceID))
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = store.DeleteSliceRef(context.Background(), int64(sharedSliceID))
		_ = realBS.DeleteBlock(context.Background(), sharedSliceID, 0, 4096)
	})

	// S1: 模拟受在途读取保护（isSliceReadInFlight）的切片
	inFlightSliceID := slicesBefore[1].ID
	readInFlightID.Store(inFlightSliceID)

	// S2: 模拟物理删除失败的切片
	failSliceID := slicesBefore[2].ID
	failDeleteSliceID.Store(failSliceID)

	// 启动一个针对 S1 的前台在途 Read 并停留在 GetBlock 屏障中（此时 isSliceReadInFlight(S1) == true）
	readDoneCh := make(chan error, 1)
	go func() {
		readCtx := context.WithValue(ctx, fgWriteContextKey{}, true)
		_, rErr := mgr.Read(readCtx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 4096,
			Size:   4096,
		})
		readDoneCh <- rErr
	}()

	select {
	case <-readEnteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("超时: 前台 Read 未进入 GetBlock 屏障")
	}

	// 写入第 4 个切片 S3，达到阈值 4 触发后台 Worker 合并并立即推进废弃块回收
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 3 * 4096,
		Data:   bytes.Repeat([]byte{0x34}, 4096),
	})
	require.NoError(t, err)

	// 等待 Chunk 0 合并为 1 条紧致 Slice 且第 4 个旧切片 S3 的物理文件与 slice_refs 被同一 Worker 立即回收
	var normalOldSliceID uint64
	require.Eventually(t, func() bool {
		c, gErr := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		if gErr != nil {
			return false
		}
		sl, dErr := meta.DecodeSlices(c.Slices)
		if dErr != nil || len(sl) != 1 {
			return false
		}
		// 检查 hookBS.deletes 中是否已对 S2（失败）和 S3（成功）发起了立即删除
		hookBS.mu.Lock()
		delCount := len(hookBS.deletes)
		for _, id := range hookBS.deletes {
			if id != sharedSliceID && id != inFlightSliceID && id != failSliceID {
				normalOldSliceID = id
			}
		}
		hookBS.mu.Unlock()
		if delCount < 2 || normalOldSliceID == 0 {
			return false
		}
		_, refErr := store.GetSliceRef(ctx, int64(normalOldSliceID))
		return errors.Is(refErr, pgx.ErrNoRows)
	}, 5*time.Second, 20*time.Millisecond, "合并后同一 Worker 必须立即物理删除并清除无保护的 refs==0 旧切片 S3")

	assert.NoFileExists(t, blockFilePath(normalOldSliceID, 0, 4096), "无保护的旧切片 S3 物理文件必须已被立即删除")

	// 校验 S0（refs == 1）：物理文件与 slice_refs 必须完好保留
	ref0, err := store.GetSliceRef(ctx, int64(sharedSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(1), ref0.Refs, "仍有引用的 Block (refs=1) 绝不能被提前删除")
	assert.FileExists(t, blockFilePath(sharedSliceID, 0, 4096))

	// 校验 S1（refs == 0 但受在途 Read 保护）：物理文件与 slice_refs 必须完好保留
	ref1, err := store.GetSliceRef(ctx, int64(inFlightSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref1.Refs, "在途 Read 保护的切片 refs 已降为 0 但记录不可提前删除")
	assert.FileExists(t, blockFilePath(inFlightSliceID, 0, 4096), "在途 Read 保护的物理块文件绝不能提前删除")

	// 校验 S2（DeleteBlock 失败）：物理文件与 slice_refs 记录必须保留供后续重试
	ref2, err := store.GetSliceRef(ctx, int64(failSliceID))
	require.NoError(t, err)
	assert.Equal(t, int32(0), ref2.Refs, "物理删除失败的切片必须保留 slice_refs 记录")
	assert.FileExists(t, blockFilePath(failSliceID, 0, 4096), "物理删除失败的块文件仍在磁盘上")

	// 验证 S2 的删除失败不阻塞其他任务：向 Chunk 1 写入 4 个切片触发合并，Chunk 1 正常合并并立即回收其旧块，
	// 同时放行 S1 的在途 Read 并解除 S2 的删除故障，验证在 Chunk 1 合并后的立即回收中 S1 与 S2 一并被成功清理！
	releaseReadOnce.Do(func() { close(releaseReadCh) })
	require.NoError(t, <-readDoneCh)
	failDeleteSliceID.Store(0)

	chunk1Base := uint64(meta.ChunkSize)
	for i := 0; i < 4; i++ {
		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: chunk1Base + uint64(i*4096),
			Data:   bytes.Repeat([]byte{byte(0x41 + i)}, 4096),
		})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool {
		c1, gErr := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
		if gErr != nil || len(c1.Slices)/meta.SliceBytes != 1 {
			return false
		}
		_, err1 := store.GetSliceRef(ctx, int64(inFlightSliceID))
		_, err2 := store.GetSliceRef(ctx, int64(failSliceID))
		return errors.Is(err1, pgx.ErrNoRows) && errors.Is(err2, pgx.ErrNoRows)
	}, 5*time.Second, 20*time.Millisecond, "在途读取结束及删除故障恢复后，S1 与 S2 应在后续回收中被彻底清除")

	assert.NoFileExists(t, blockFilePath(inFlightSliceID, 0, 4096))
	assert.NoFileExists(t, blockFilePath(failSliceID, 0, 4096))
}

// TestSingleWorker_PeriodicGCInterleavesDuringContinuousCompaction_AndAuditsChildTablesDirectly
// 验证验收标准 6、7、9：
// 持续有合并任务到来时，唯一后台 Worker 在到达 gcInterval 周期时仍能穿插推进：
// 1. 超期 ClientSession（直接按目标 ID 审计 client_sessions、file_handles、file_locks、file_lock_waiters 子表物理清零）；
// 2. 无活跃句柄的 Orphan Inode（直接按目标 Ino 审计 inodes、chunks 物理清零，其释放的块被回收）；
// 3. 超过安全宽限期的未登记孤儿 Block 文件（未满宽限期的在途未登记文件与已登记有效块不被误删）。
func TestSingleWorker_PeriodicGCInterleavesDuringContinuousCompaction_AndAuditsChildTablesDirectly(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, tmpDir, pool := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	blockFilePath := func(sliceID uint64, blockIdx uint32, size uint32) string {
		h1 := sliceID % 256
		h2 := sliceID / 1000000
		return filepath.Join(tmpDir, "chunks", fmt.Sprintf("%02X", h1), fmt.Sprintf("%d", h2), fmt.Sprintf("%d_%d_%d", sliceID, blockIdx, size))
	}

	var compactPutCount atomic.Int32
	slowCompactBS := &recordingBlockStore{
		realBS: realBS,
		onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
			if putCtx.Value(fgWriteContextKey{}) != nil {
				return nil
			}
			compactPutCount.Add(1)
			// 每次后台 Compaction PutBlock 耗时 35ms，制造多 Chunk 持续合并积压（超过 45ms 的 gcInterval）
			select {
			case <-time.After(35 * time.Millisecond):
				return nil
			case <-putCtx.Done():
				return putCtx.Err()
			}
		},
	}

	// forwardingBlockStore 同时暴露底层 LocalFSBlockStore 的 BlockLister 能力
	listerBS := &listerForwardingBlockStore{
		recordingBlockStore: slowCompactBS,
		lister:              realBS.(meta.BlockLister),
	}

	mgr := meta.New(
		store,
		meta.WithBlockStore(listerBS),
		meta.WithCompactSliceThreshold(4),
		meta.WithGCInterval(45*time.Millisecond),
		meta.WithGCOrphanGracePeriod(time.Hour),
	)
	workerCtx, workerCancel := context.WithCancel(ctx)
	defer workerCancel()
	mgr.StartGCWorker(workerCtx, 45*time.Millisecond, 50)

	diskName := fmt.Sprintf("test_interleave_gc_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{Name: diskName, MaxBytes: 1024 * 1024 * 1024})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	// 准备待由周期 GC 清理的目标：
	// (a) 绑定会话 sessID 的孤儿 Inode（含 file_handles、file_locks、file_lock_waiters）
	sessID := uint64(880192)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
	})
	_, err = mgr.RenewSession(ctx, sessID)
	require.NoError(t, err)

	orphanRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("interleave_orphan.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	orphanIno := orphanRes.Inode.Ino
	orphanFh := orphanRes.Fh

	fgCtx := context.WithValue(ctx, fgWriteContextKey{}, true)
	_, err = mgr.Write(fgCtx, &meta.WriteRequest{
		Ino:       orphanIno,
		Fh:        orphanFh,
		Offset:    0,
		Data:      bytes.Repeat([]byte{0x7A}, 4096),
		SessionID: &sessID,
	})
	require.NoError(t, err)

	require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
		Ino:       orphanIno,
		Fh:        orphanFh,
		SessionID: sessID,
		LockOwner: 2001,
		Lk: meta.FileLock{
			Start: 0,
			End:   64,
			Type:  meta.F_WRLCK,
			PID:   4321,
		},
	}))
	_, err = pool.Exec(ctx, `
		INSERT INTO file_lock_waiters (session_id, request_id, ino, fh, lock_owner, lk_flags, start_offset, end_offset, lock_type, pid)
		VALUES ($1, 1, $2, $3, 2002, 0, 0, 64, $4, 4322)
	`, int64(sessID), int64(orphanIno), int64(orphanFh), int16(meta.F_WRLCK))
	require.NoError(t, err)

	require.NoError(t, mgr.Unlink(ctx, disk.RootIno, []byte("interleave_orphan.bin")))

	// 令 sessID 立即超期
	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(sessID))
	require.NoError(t, err)

	// (b) 1 个超过安全宽限期的未登记孤儿 Block 文件，以及 1 个未满宽限期的在途未登记 Block 文件
	agedUnregisteredSliceID := uint64(910000001)
	freshUnregisteredSliceID := uint64(910000002)
	require.NoError(t, realBS.PutBlock(ctx, agedUnregisteredSliceID, 0, bytes.Repeat([]byte("A"), 4096)))
	require.NoError(t, realBS.PutBlock(ctx, freshUnregisteredSliceID, 0, bytes.Repeat([]byte("N"), 4096)))
	t.Cleanup(func() {
		_ = realBS.DeleteBlock(context.Background(), agedUnregisteredSliceID, 0, 4096)
		_ = realBS.DeleteBlock(context.Background(), freshUnregisteredSliceID, 0, 4096)
	})
	agedPath := blockFilePath(agedUnregisteredSliceID, 0, 4096)
	freshPath := blockFilePath(freshUnregisteredSliceID, 0, 4096)
	twoHoursAgo := time.Now().Add(-2 * time.Hour)
	require.NoError(t, os.Chtimes(agedPath, twoHoursAgo, twoHoursAgo))

	// 创建持续触发多 Chunk 合并的常规文件，一次性向 6 个不同 Chunk 各写入 4 次（共 6 个待合并任务排队，总合并时间 > 200ms）
	compRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("continuous_compact.bin"),
		Mode:      meta.S_IFREG | 0o644,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	compIno := compRes.Inode.Ino
	compFh := compRes.Fh

	for chunkIndx := int64(0); chunkIndx < 6; chunkIndx++ {
		baseOff := uint64(chunkIndx) * uint64(meta.ChunkSize)
		for i := 0; i < 4; i++ {
			_, err := mgr.Write(fgCtx, &meta.WriteRequest{
				Ino:    compIno,
				Fh:     compFh,
				Offset: baseOff + uint64(i*4096),
				Data:   bytes.Repeat([]byte{byte(0x50 + int(chunkIndx)*4 + i)}, 4096),
			})
			require.NoError(t, err)
		}
	}

	// 验证在多 Chunk 持续合并期间（无需等待全部 6 个 Chunk 合并结束），周期 GC 已穿插推进并清理了超期会话、孤儿 Inode 及老化未登记块
	require.Eventually(t, func() bool {
		var sessCnt, fhCnt, lockCnt, waiterCnt, inodeCnt, chunkCnt, entryCnt int64
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM client_sessions WHERE session_id = $1", int64(sessID)).Scan(&sessCnt); err != nil || sessCnt != 0 {
			return false
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM file_handles WHERE fh = $1", int64(orphanFh)).Scan(&fhCnt); err != nil || fhCnt != 0 {
			return false
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM file_locks WHERE session_id = $1", int64(sessID)).Scan(&lockCnt); err != nil || lockCnt != 0 {
			return false
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM file_lock_waiters WHERE session_id = $1", int64(sessID)).Scan(&waiterCnt); err != nil || waiterCnt != 0 {
			return false
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM inodes WHERE ino = $1", int64(orphanIno)).Scan(&inodeCnt); err != nil || inodeCnt != 0 {
			return false
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM chunks WHERE ino = $1", int64(orphanIno)).Scan(&chunkCnt); err != nil || chunkCnt != 0 {
			return false
		}
		if err := pool.QueryRow(ctx, "SELECT count(*) FROM entries WHERE parent_ino = $1 AND ino = $2", int64(disk.RootIno), int64(orphanIno)).Scan(&entryCnt); err != nil || entryCnt != 0 {
			return false
		}
		_, agedStatErr := os.Stat(agedPath)
		return errors.Is(agedStatErr, os.ErrNotExist)
	}, 5*time.Second, 20*time.Millisecond, "持续合并期间必须按 gcInterval 穿插推进超期会话、孤儿 Inode 与未登记孤儿 Block 回收")

	// 未超宽限期的在途未登记物理块绝不能被误删
	assert.FileExists(t, freshPath, "未满安全宽限期的在途未登记物理块必须受到保护")

	// 最终所有 6 个排队的 Chunk 合并任务也全部完成收敛
	for chunkIndx := int64(0); chunkIndx < 6; chunkIndx++ {
		idx := chunkIndx
		require.Eventually(t, func() bool {
			c, gErr := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(compIno), Indx: idx})
			return gErr == nil && len(c.Slices)/meta.SliceBytes == 1
		}, 5*time.Second, 25*time.Millisecond, "Chunk %d 最终必须完成合并", idx)
	}
}

type listerForwardingBlockStore struct {
	*recordingBlockStore
	lister meta.BlockLister
}

func (l *listerForwardingBlockStore) ListBlocksBefore(ctx context.Context, before time.Time, limit int) ([]meta.PhysicalBlockFile, error) {
	return l.lister.ListBlocksBefore(ctx, before, limit)
}

// TestSingleWorker_FxLifecycle_StartAndStopDrainsCleanlyWithoutDetachedTasks
// 验证验收标准 8：
// 通过真实生产生命周期启动路径（meta.Module + Fx OnStart/OnStop）验证：
// 1. 服务启动只启用唯一一套后台执行器；
// 2. 服务停止（OnStop）时取消并等待后台执行器退出，停止后无脱离生命周期的自动整理任务继续运行。
func TestSingleWorker_FxLifecycle_StartAndStopDrainsCleanlyWithoutDetachedTasks(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, tmpDir, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	var (
		bgPutEnteredCh = make(chan struct{})
		bgPutExitedCh  = make(chan struct{})
		bgPutCalls     atomic.Int32
		enteredOnce    sync.Once
		exitedOnce     sync.Once
	)

	hookBS := &recordingBlockStore{
		realBS: realBS,
		onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
			if putCtx.Value(fgWriteContextKey{}) != nil {
				return nil
			}
			bgPutCalls.Add(1)
			enteredOnce.Do(func() { close(bgPutEnteredCh) })
			defer exitedOnce.Do(func() { close(bgPutExitedCh) })

			<-putCtx.Done()
			return putCtx.Err()
		},
	}

	metaCfg := &config.MetaConfig{
		DataDirVal:    tmpDir,
		GCIntervalVal: 40 * time.Millisecond,
	}

	var mgr meta.Manager
	app := fx.New(
		fx.NopLogger,
		fx.Provide(func() db.Store { return store }),
		fx.Provide(func() meta.BlockStore { return hookBS }),
		fx.Supply(metaCfg),
		meta.Module,
		fx.Populate(&mgr),
	)

	startCtx, startCancel := context.WithTimeout(ctx, 5*time.Second)
	defer startCancel()
	require.NoError(t, app.Start(startCtx))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	fgCtx := context.WithValue(ctx, fgWriteContextKey{}, true)
	for i := 0; i < meta.CompactSliceThreshold; i++ {
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(i * 64),
			Data:   bytes.Repeat([]byte{byte(0x60 + (i % 32))}, 64),
		})
		require.NoError(t, err)
	}

	select {
	case <-bgPutEnteredCh:
	case <-time.After(5 * time.Second):
		t.Fatal("超时: Fx 启动的后台 Worker 未处理自动 Compaction 任务")
	}

	// 调用 app.Stop：必须取消正在执行的后台 Worker 并等待其彻底退出后才返回
	stopCtx, stopCancel := context.WithTimeout(ctx, 5*time.Second)
	defer stopCancel()
	require.NoError(t, app.Stop(stopCtx))

	select {
	case <-bgPutExitedCh:
	default:
		t.Fatal("app.Stop 返回时后台 Worker 必须已完全退出")
	}

	callsAfterStop := bgPutCalls.Load()

	// 服务停止后再次写入达到阈值的数据，验证绝不会重新拉起脱离生命周期的后台整理协程
	for i := 0; i < meta.CompactSliceThreshold; i++ {
		_, err := mgr.Write(fgCtx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(meta.ChunkSize) + uint64(i*64),
			Data:   bytes.Repeat([]byte{byte(0x70 + (i % 32))}, 64),
		})
		require.NoError(t, err)
	}
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, callsAfterStop, bgPutCalls.Load(), "服务停止后绝不能再启动脱离生命周期的后台自动整理任务")
}

// TestCompactChunk_Bounded4MiBIO_IsolatedAdjacentChunk_AndSharedCrossBoundaryRef 验收标准：
//  1. 记录实际 BlockStore 数据读写范围，证明每次合并读取与写出的有效数据总量不超过 4 MiB；
//  2. 合并 Chunk 0 时绝不会读取或重写相邻 Chunk 1 独有的物理块；
//  3. 跨 Chunk 边界写入产生的共享 Block（初始 refs=2），在 Chunk 0 合并时仅扣减 Chunk 0 本次实际替换的 1 次引用（剩余 refs=1），
//     相邻 Chunk 1 仍可完整读取该跨边界数据；合并前后 Read 与独立字节模型一致。
func TestCompactChunk_Bounded4MiBIO_IsolatedAdjacentChunk_AndSharedCrossBoundaryRef(t *testing.T) {
	ctx := context.Background()
	store, realBS, _, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	recBS := &recordingBlockStore{realBS: realBS}
	mgr := meta.New(store, meta.WithBlockStore(recBS))

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o644),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)
	t.Cleanup(func() {
		cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
	})

	openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
	})

	const oneMiB = 1 << 20
	const chunkSize = int(meta.ChunkSize) // 4 MiB
	expected := make([]byte, 2*chunkSize)

	// 1. 在 Chunk 0 顺序写满 4 MiB（4 × 1 MiB）
	for i := 0; i < 4; i++ {
		buf := bytes.Repeat([]byte{byte(0xA0 + i)}, oneMiB)
		off := i * oneMiB
		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(off),
			Data:   buf,
		})
		require.NoError(t, err)
		copy(expected[off:off+oneMiB], buf)
	}

	// 2. 在 Chunk 1 写入一段独有数据 [4 MiB + 128 KiB, 4 MiB + 1 MiB + 128 KiB)
	chunk1ExclusiveOff := chunkSize + 128*1024
	chunk1ExclusiveData := bytes.Repeat([]byte{0xB5}, oneMiB)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(chunk1ExclusiveOff),
		Data:   chunk1ExclusiveData,
	})
	require.NoError(t, err)
	copy(expected[chunk1ExclusiveOff:chunk1ExclusiveOff+oneMiB], chunk1ExclusiveData)

	chunk1Before, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
	require.NoError(t, err)
	chunk1SlicesInit, err := meta.DecodeSlices(chunk1Before.Slices)
	require.NoError(t, err)
	require.Len(t, chunk1SlicesInit, 1)
	chunk1ExclusiveSliceID := chunk1SlicesInit[0].ID

	// 3. 发起一次跨 Chunk 0 与 Chunk 1 边界的写入：[4 MiB - 64 KiB, 4 MiB + 64 KiB)，共 128 KiB
	// 该写入生成一个共享物理 Block，拆分为两个描述符分别追加到 Chunk 0 和 Chunk 1，初始 slice_refs.refs == 2
	crossOff := chunkSize - 64*1024
	crossData := bytes.Repeat([]byte{0xCC}, 128*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(crossOff),
		Data:   crossData,
	})
	require.NoError(t, err)
	copy(expected[crossOff:crossOff+len(crossData)], crossData)

	// 4. 在 Chunk 0 内局部覆写 [1.5 MiB, 1.5 MiB + 64 KiB)
	overOff := oneMiB + oneMiB/2
	overData := bytes.Repeat([]byte{0xDD}, 64*1024)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(overOff),
		Data:   overData,
	})
	require.NoError(t, err)
	copy(expected[overOff:overOff+len(overData)], overData)

	chunk0Before, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	chunk0SlicesBefore, err := meta.DecodeSlices(chunk0Before.Slices)
	require.NoError(t, err)
	require.Len(t, chunk0SlicesBefore, 6)

	// 定位跨界共享 Slice ID（第 5 条，索引 4）并验证合并前 refs == 2
	sharedCrossSliceID := chunk0SlicesBefore[4].ID
	refCrossBefore, err := store.GetSliceRef(ctx, int64(sharedCrossSliceID))
	require.NoError(t, err)
	require.Equal(t, int32(2), refCrossBefore.Refs, "跨 Chunk 边界写入的共享 Slice 合并前 refs 必须为 2")

	chunk1SnapshotBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
	require.NoError(t, err)

	// 合并前验证全量 Read 与独立黄金模型一致
	readBefore := readFullFileForCompactTest(t, ctx, mgr, ino, fh, chunk1ExclusiveOff+oneMiB)
	assert.Equal(t, expected[:chunk1ExclusiveOff+oneMiB], readBefore)

	// 清空 recordingBlockStore 读写记录，精确测量 Chunk 0 合并期间的物理读写范围
	recBS.mu.Lock()
	recBS.gets = nil
	recBS.puts = nil
	recBS.deletes = nil
	recBS.readOps = nil
	recBS.writeOps = nil
	recBS.mu.Unlock()

	// 5. 对 Chunk 0 执行整体合并
	err = mgr.CompactChunk(ctx, ino, 0)
	require.NoError(t, err)

	recBS.mu.Lock()
	readOps := append([]recordedBlockRead(nil), recBS.readOps...)
	writeOps := append([]recordedBlockWrite(nil), recBS.writeOps...)
	recBS.mu.Unlock()

	var totalReadBytes uint64
	for _, rop := range readOps {
		totalReadBytes += uint64(rop.length)
		assert.NotEqual(t, chunk1ExclusiveSliceID, rop.sliceID, "合并 Chunk 0 绝不能读取相邻 Chunk 1 独有的物理块")
		if rop.sliceID == sharedCrossSliceID {
			assert.Equal(t, uint32(0), rop.offset, "跨界共享块在 Chunk 0 中只允许读取前半段 [0, 64KiB)")
			assert.Equal(t, uint32(64*1024), rop.length, "跨界共享块在 Chunk 0 中不得读取属于 Chunk 1 的后半段")
		}
	}

	var totalWriteBytes uint64
	for _, wop := range writeOps {
		totalWriteBytes += uint64(wop.length)
		assert.Equal(t, uint32(0), wop.blockIdx, "4 MiB Chunk 下合并生成的紧致 Slice 物理块 block_idx 必须为 0")
	}

	assert.Equal(t, uint64(meta.ChunkSize), totalReadBytes, "写满的 4 MiB Chunk 合并读取有效字节总量必须精确为 4 MiB 且不超过 4 MiB")
	assert.Equal(t, uint64(meta.ChunkSize), totalWriteBytes, "写满的 4 MiB Chunk 合并写出有效字节总量必须精确为 4 MiB 且不超过 4 MiB")
	require.Len(t, writeOps, 1, "连续无空洞 4 MiB Chunk 整体合并后只应写出 1 个 4 MiB 物理 Block")

	// 6. 验证 Chunk 0 已整体压平为 1 条 4 MiB 紧致 Slice，而相邻 Chunk 1 的 chunks.slices 完全未受触碰
	chunk0After, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
	require.NoError(t, err)
	chunk0SlicesAfter, err := meta.DecodeSlices(chunk0After.Slices)
	require.NoError(t, err)
	require.Len(t, chunk0SlicesAfter, 1, "Chunk 0 整体合并后应压平为 1 条 Slice")
	assert.Equal(t, uint32(0), chunk0SlicesAfter[0].Pos)
	assert.Equal(t, uint32(meta.ChunkSize), chunk0SlicesAfter[0].Len)

	chunk1After, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 1})
	require.NoError(t, err)
	assert.Equal(t, chunk1SnapshotBefore.Slices, chunk1After.Slices, "合并 Chunk 0 绝不能改动相邻 Chunk 1 的元数据")

	// 7. 验证引用计数：Chunk 0 独有的旧 Slice refs 全部归零，而跨 Chunk 共享的 sharedCrossSliceID refs 从 2 精准减为 1
	for _, s := range chunk0SlicesBefore {
		ref, rErr := store.GetSliceRef(ctx, int64(s.ID))
		require.NoError(t, rErr)
		if s.ID == sharedCrossSliceID {
			assert.Equal(t, int32(1), ref.Refs, "跨 Chunk 共享 Slice 在 Chunk 0 合并后只能扣减 1 次引用，保留 refs=1 供 Chunk 1 使用")
		} else {
			assert.Equal(t, int32(0), ref.Refs, "Chunk 0 独有旧 Slice (id=%d) 合并后 refs 必须归零", s.ID)
		}
	}

	// 执行 GC 清理 refs==0 的废弃块后，验证 Chunk 0 与相邻 Chunk 1（含跨边界区域）读取依然与独立模型 100% 一致
	_, err = mgr.CollectGarbage(ctx, 100)
	require.NoError(t, err)

	readAfter := readFullFileForCompactTest(t, ctx, mgr, ino, fh, chunk1ExclusiveOff+oneMiB)
	assert.Equal(t, expected[:chunk1ExclusiveOff+oneMiB], readAfter, "合并及 GC 后跨 Chunk 读取必须与独立黄金模型完全一致")
}

// TestCompactChunk_PunchHolePartialAndFull_CompactsEntireSnapshot 验收标准：
// 1. 存在空洞时整体合并保持稀疏区间（移除完全被打洞抹除的旧 Slice 并归零其 refs，连续实心区间合成为紧致 Slice）；
// 2. 全空洞时不生成任何新物理 Block（0 次 PutBlock）；
// 3. 阶段 1 与阶段 2 之间若有并发追加写入，整体合并结果与并发追加后缀一并保留，且被替换的全部旧 Slice 引用计数归零。
func TestCompactChunk_PunchHolePartialAndFull_CompactsEntireSnapshot(t *testing.T) {
	ctx := context.Background()
	store, realBS, initMgr, _, _ := setupPostgresForCompact(t)
	if store == nil {
		return
	}

	const oneMiB = 1 << 20

	t.Run("局部打洞与跨段写入整体合并保持稀疏区间并释放被抹除Slice引用", func(t *testing.T) {
		recBS := &recordingBlockStore{realBS: realBS}
		mgr := meta.New(store, meta.WithBlockStore(recBS))

		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = mgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
		})

		expected := make([]byte, 3*oneMiB)
		// S0: [0, 1 MiB), S1: [1 MiB, 2 MiB), S2: [2 MiB, 3 MiB)
		for i := 0; i < 3; i++ {
			buf := bytes.Repeat([]byte{byte(0xB0 + i)}, oneMiB)
			_, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * oneMiB),
				Data:   buf,
			})
			require.NoError(t, err)
			copy(expected[i*oneMiB:(i+1)*oneMiB], buf)
		}

		// 对中间 [1 MiB, 2 MiB) 执行 PunchHole 打洞，形成 [0, 1 MiB) 和 [2 MiB, 3 MiB) 两个不相邻实心段
		err = mgr.Fallocate(ctx, meta.FallocateParams{
			Ino:    ino,
			Fh:     fh,
			Offset: uint64(oneMiB),
			Length: uint64(oneMiB),
			Mode:   meta.FallocFlPunchHole | meta.FallocFlKeepSize,
		})
		require.NoError(t, err)
		clear(expected[oneMiB : 2*oneMiB])

		chunkInit, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesInit, err := meta.DecodeSlices(chunkInit.Slices)
		require.NoError(t, err)
		require.Len(t, slicesInit, 4) // 3 条实心 + 1 条空洞

		recBS.mu.Lock()
		recBS.readOps = nil
		recBS.writeOps = nil
		recBS.mu.Unlock()

		err = mgr.CompactChunk(ctx, ino, 0)
		require.NoError(t, err)

		// 断言被打洞抹除的中间 1 MiB (S1) 绝不会被 GetBlock 读取，且只写出 2 个 1 MiB 的实心段（空洞不产生物理 Block）
		recBS.mu.Lock()
		readOps := append([]recordedBlockRead(nil), recBS.readOps...)
		writeOps := append([]recordedBlockWrite(nil), recBS.writeOps...)
		recBS.mu.Unlock()

		for _, rop := range readOps {
			assert.NotEqual(t, slicesInit[1].ID, rop.sliceID, "被打洞完全覆盖的 S1 不应被读取")
		}
		require.Len(t, writeOps, 2, "由空洞隔开的 2 个实心区间应生成 2 个紧致物理 Block，空洞本身不产生物理 Block")
		assert.Equal(t, uint32(oneMiB), writeOps[0].length)
		assert.Equal(t, uint32(oneMiB), writeOps[1].length)

		chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
		require.NoError(t, err)
		require.Len(t, slicesAfter, 2, "合并后应包含 2 条不相邻的紧致实心 Slice，中间保持稀疏空洞")
		assert.Equal(t, uint32(0), slicesAfter[0].Pos)
		assert.Equal(t, uint32(oneMiB), slicesAfter[0].Len)
		assert.Equal(t, uint32(2*oneMiB), slicesAfter[1].Pos)
		assert.Equal(t, uint32(oneMiB), slicesAfter[1].Len)

		// 原 3 条旧实心 Slice 的 refs 均归零，新 2 条紧致 Slice 的 refs 均为 1
		for _, s := range slicesInit {
			if s.ID > 0 {
				ref, rErr := store.GetSliceRef(ctx, int64(s.ID))
				require.NoError(t, rErr)
				assert.Equal(t, int32(0), ref.Refs)
			}
		}
		for _, s := range slicesAfter {
			ref, rErr := store.GetSliceRef(ctx, int64(s.ID))
			require.NoError(t, rErr)
			assert.Equal(t, int32(1), ref.Refs)
		}

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(expected)),
		})
		require.NoError(t, err)
		assert.Equal(t, expected, readRes.Data)
	})

	t.Run("整体合并期间并发追加写入保留后缀并扣减全部旧前缀引用", func(t *testing.T) {
		fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		ino := uint64(fileRow.Ino)
		t.Cleanup(func() {
			cleanupCompactTestIno(context.Background(), store, fileRow.Ino)
		})

		openRes, err := initMgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
		require.NoError(t, err)
		fh := openRes.Fh
		t.Cleanup(func() {
			_ = initMgr.Release(context.Background(), meta.ReleaseParams{Ino: ino, Fh: fh})
		})

		const tailFragLen = 16 * 1024
		expected := make([]byte, 2*oneMiB+3*tailFragLen)

		// 1. 写入 2 个 1 MiB 大块 P0, P1 与 2 个 16 KiB 尾部切片 T0, T1（共 4 条连续无空洞切片）
		for i := 0; i < 2; i++ {
			buf := bytes.Repeat([]byte{byte(0xC0 + i)}, oneMiB)
			_, err := initMgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * oneMiB),
				Data:   buf,
			})
			require.NoError(t, err)
			copy(expected[i*oneMiB:(i+1)*oneMiB], buf)
		}
		for j := 0; j < 2; j++ {
			buf := bytes.Repeat([]byte{byte(0xD0 + j)}, tailFragLen)
			off := 2*oneMiB + j*tailFragLen
			_, err := initMgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(off),
				Data:   buf,
			})
			require.NoError(t, err)
			copy(expected[off:off+tailFragLen], buf)
		}

		chunkBefore, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesBefore, err := meta.DecodeSlices(chunkBefore.Slices)
		require.NoError(t, err)
		require.Len(t, slicesBefore, 4)

		// 2. 在整体合并阶段 1 PutBlock 时并发追加 C0
		concurrentData := bytes.Repeat([]byte{0xEE}, tailFragLen)
		concurrentOff := 2*oneMiB + 2*tailFragLen
		copy(expected[concurrentOff:concurrentOff+tailFragLen], concurrentData)

		var concurrentSliceID uint64
		var hookOnce sync.Once
		hookBS := &recordingBlockStore{
			realBS: realBS,
			onBeforePutBlock: func(putCtx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
				var hookErr error
				hookOnce.Do(func() {
					_, wErr := initMgr.Write(context.Background(), &meta.WriteRequest{
						Ino:    ino,
						Fh:     fh,
						Offset: uint64(concurrentOff),
						Data:   concurrentData,
					})
					if wErr != nil {
						hookErr = wErr
						return
					}
					c, gErr := store.GetChunk(context.Background(), db.GetChunkParams{Ino: int64(ino), Indx: 0})
					if gErr != nil {
						hookErr = gErr
						return
					}
					sl, dErr := meta.DecodeSlices(c.Slices)
					if dErr != nil {
						hookErr = dErr
						return
					}
					if len(sl) >= 5 {
						concurrentSliceID = sl[4].ID
					}
				})
				return hookErr
			},
		}

		mgrWithHook := meta.New(store, meta.WithBlockStore(hookBS))
		err = mgrWithHook.CompactChunk(ctx, ino, 0)
		require.NoError(t, err)

		// 断言整体合并后 chunks.slices == [compactedFullSnapshot, C0] 共 2 条
		chunkAfter, err := store.GetChunk(ctx, db.GetChunkParams{Ino: int64(ino), Indx: 0})
		require.NoError(t, err)
		slicesAfter, err := meta.DecodeSlices(chunkAfter.Slices)
		require.NoError(t, err)
		require.Len(t, slicesAfter, 2, "整体合并 CAS 更新后应包含 1 条完整快照合并切片 + 1 条并发追加后缀切片")

		assert.Equal(t, uint32(0), slicesAfter[0].Pos)
		assert.Equal(t, uint32(2*oneMiB+2*tailFragLen), slicesAfter[0].Len)
		assert.Equal(t, concurrentSliceID, slicesAfter[1].ID, "并发追加切片 C0 必须保留在末尾")

		// 新合并切片与并发追加切片的 refs == 1；原全部 4 条旧切片的 refs == 0
		for _, activeID := range []uint64{slicesAfter[0].ID, concurrentSliceID} {
			ref, err := store.GetSliceRef(ctx, int64(activeID))
			require.NoError(t, err)
			assert.Equal(t, int32(1), ref.Refs, "有效 slice_id=%d 的 refs 必须为 1", activeID)
		}
		for _, oldSlice := range slicesBefore {
			ref, err := store.GetSliceRef(ctx, int64(oldSlice.ID))
			require.NoError(t, err)
			assert.Equal(t, int32(0), ref.Refs, "被整体合并替换的旧切片 slice_id=%d 的 refs 必须归零", oldSlice.ID)
		}

		readRes, err := initMgr.Read(ctx, &meta.ReadRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Size:   uint32(len(expected)),
		})
		require.NoError(t, err)
		assert.Equal(t, expected, readRes.Data)
	})
}
