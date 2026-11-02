package meta_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// gateBlockStore 包装 BlockStore，用于在测试中同步汇聚指定数量的并发请求完成阶段 1 PutBlock，
// 同时统计 PutBlock 与 DeleteBlock 调用次数。
type gateBlockStore struct {
	meta.BlockStore
	gateCount   int
	arrived     atomic.Int64
	gateCh      chan struct{}
	gateOnce    sync.Once
	putCalls    atomic.Int64
	deleteCalls atomic.Int64

	mu         sync.Mutex
	deletedIDs []uint64
}

func newGateBlockStore(inner meta.BlockStore, gateCount int) *gateBlockStore {
	return &gateBlockStore{
		BlockStore: inner,
		gateCount:  gateCount,
		gateCh:     make(chan struct{}),
	}
}

func (g *gateBlockStore) releaseGate() {
	g.gateOnce.Do(func() {
		close(g.gateCh)
	})
}

func (g *gateBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	g.putCalls.Add(1)
	if err := g.BlockStore.PutBlock(ctx, sliceID, blockIdx, data); err != nil {
		return err
	}
	if g.gateCount > 0 {
		if int(g.arrived.Add(1)) >= g.gateCount {
			g.releaseGate()
		}
		select {
		case <-g.gateCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (g *gateBlockStore) DeleteBlock(ctx context.Context, sliceID uint64, blockIdx uint32, blockSize uint32) error {
	g.deleteCalls.Add(1)
	g.mu.Lock()
	g.deletedIDs = append(g.deletedIDs, sliceID)
	g.mu.Unlock()
	return g.BlockStore.DeleteBlock(ctx, sliceID, blockIdx, blockSize)
}

// countRegularFilesInDir 统计给定数据目录下残留的物理块常规文件数量。
func countRegularFilesInDir(t *testing.T, dir string) int {
	t.Helper()
	var count int
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, walkErr error) error {
		if walkErr != nil {
			if errors.Is(walkErr, os.ErrNotExist) {
				return nil
			}
			return walkErr
		}
		if !d.IsDir() {
			count++
		}
		return nil
	})
	require.NoError(t, err)
	return count
}

type writeCountingStore struct {
	db.Store
	pool                *pgxpool.Pool
	onWriteFileAtomicFn func()
}

func (s *writeCountingStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *writeCountingStore) WriteFileAtomic(ctx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
	if s.onWriteFileAtomicFn != nil {
		s.onWriteFileAtomicFn()
	}
	return s.Store.WriteFileAtomic(ctx, arg)
}

// TestWrite_GroupCommit_BatchAggregationAndConservation_PostgresIntegration 验证高并发单语句 CTE Write 与账本守恒：
// 覆盖同一 Inode 并发写、同一 Disk 下多 Inode 并发扩容写、跨 4 MiB Chunk 边界写，
// 断言阶段 2 每个并发请求各自通过 1 次 WriteFileAtomic 单语句完成，且全文件 Read 数据视图、Inode 属性（含 S_ISUID/S_ISGID 剥离）及 Disk.used_bytes 100% 精确守恒。
func TestWrite_GroupCommit_BatchAggregationAndConservation_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	baseStore := db.NewStore(pool)
	var atomicWriteCalls atomic.Int64
	countingStore := &writeCountingStore{
		Store: baseStore,
		pool:  pool,
		onWriteFileAtomicFn: func() {
			atomicWriteCalls.Add(1)
		},
	}

	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)

	const numWriters = 12
	gatedBS := newGateBlockStore(localBS, numWriters)
	t.Cleanup(gatedBS.releaseGate)

	mgr := meta.New(
		countingStore,
		meta.WithBlockStore(gatedBS),
		meta.WithSliceIDBatchSize(64),
	)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("group-commit-conservation-%d", time.Now().UnixNano()),
		MaxBytes: 500 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	// 创建 2 个带 S_ISUID | S_ISGID 特权位的文件（覆盖同一 Disk 下多 Inode 并发写与跨 4 MiB Chunk 边界写）
	createFile := func(name string) (uint64, uint64) {
		res, cErr := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(name),
			Mode:      meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
		})
		require.NoError(t, cErr)
		// Create 时若受 umask 影响，显式通过底表赋予 S_ISUID | S_ISGID 以验证 Write 剥离特权位
		_, uErr := pool.Exec(ctx, "UPDATE inodes SET mode = $1 WHERE ino = $2", int32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), int64(res.Inode.Ino))
		require.NoError(t, uErr)
		return res.Inode.Ino, res.Fh
	}

	ino1, fh1 := createFile("file_multi_seg.bin")
	ino2, fh2 := createFile("file_cross_chunk.bin")

	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, baseStore, mgr, int64(ino1), fh1)
		cleanupWriteTestInoAndSlices(t, baseStore, mgr, int64(ino2), fh2)
	})

	atomicWriteCalls.Store(0)

	// 规划 12 个并发写请求：
	// - 前 8 个写 ino1 的不重叠 4096B 分段 [0, 32768)
	// - 第 9..11 个写 ino2 的前 3 个 4096B 分段 [0, 12288)
	// - 第 12 个写 ino2 跨越 4 MiB Chunk 边界的区间 [ChunkSize - 2048, ChunkSize + 2048)
	const segSize = 4096
	crossChunkOffset := uint64(meta.ChunkSize - 2048)

	type writeJob struct {
		id     int
		ino    uint64
		fh     uint64
		offset uint64
		data   []byte
	}

	jobs := make([]writeJob, 0, numWriters)
	for i := 0; i < 8; i++ {
		jobs = append(jobs, writeJob{
			id:     i,
			ino:    ino1,
			fh:     fh1,
			offset: uint64(i * segSize),
			data:   bytes.Repeat([]byte{byte('a' + i)}, segSize),
		})
	}
	for i := 0; i < 3; i++ {
		jobs = append(jobs, writeJob{
			id:     8 + i,
			ino:    ino2,
			fh:     fh2,
			offset: uint64(i * segSize),
			data:   bytes.Repeat([]byte{byte('x' + i)}, segSize),
		})
	}
	crossPayload := bytes.Repeat([]byte{'Z'}, segSize)
	jobs = append(jobs, writeJob{
		id:     11,
		ino:    ino2,
		fh:     fh2,
		offset: crossChunkOffset,
		data:   crossPayload,
	})

	var wg sync.WaitGroup
	errs := make([]error, numWriters)
	resps := make([]*meta.WriteResponse, numWriters)

	for _, job := range jobs {
		wg.Add(1)
		go func(jb writeJob) {
			defer wg.Done()
			resps[jb.id], errs[jb.id] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:    jb.ino,
				Fh:     jb.fh,
				Offset: jb.offset,
				Data:   jb.data,
			})
		}(job)
	}
	wg.Wait()

	for i, err := range errs {
		require.NoError(t, err, "并发写请求 %d 必须成功", i)
		require.NotNil(t, resps[i])
		assert.Equal(t, uint32(segSize), resps[i].Size)
		// S_ISUID 与 S_ISGID 必须已被剥离
		assert.Equal(t, uint32(meta.S_IFREG|0o755), resps[i].Inode.Attr.Mode)
	}

	// 1. 验证 12 个并发请求各自执行 1 次 WriteFileAtomic 与 1 次 PutBlock
	assert.Equal(t, int64(numWriters), atomicWriteCalls.Load(), "12 个并发 Write 请求各自调用 1 次 WriteFileAtomic")
	assert.Equal(t, int64(numWriters), gatedBS.putCalls.Load(), "12 个并发请求各自完成阶段 1 PutBlock")

	// 2. 验证 ino1 的大小与全量数据读取视图
	expectedIno1Size := uint64(8 * segSize)
	attr1, err := mgr.GetAttr(ctx, ino1)
	require.NoError(t, err)
	assert.Equal(t, expectedIno1Size, attr1.Attr.Size)
	assert.Equal(t, uint32(meta.S_IFREG|0o755), attr1.Attr.Mode)

	read1, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino1,
		Fh:     fh1,
		Offset: 0,
		Size:   uint32(expectedIno1Size),
	})
	require.NoError(t, err)
	for i := 0; i < 8; i++ {
		assert.Equal(t, bytes.Repeat([]byte{byte('a' + i)}, segSize), read1.Data[i*segSize:(i+1)*segSize])
	}

	// 3. 验证 ino2 的跨 Chunk 边界大小与数据读取视图
	expectedIno2Size := crossChunkOffset + segSize
	attr2, err := mgr.GetAttr(ctx, ino2)
	require.NoError(t, err)
	assert.Equal(t, expectedIno2Size, attr2.Attr.Size)

	read2Front, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino2,
		Fh:     fh2,
		Offset: 0,
		Size:   uint32(3 * segSize),
	})
	require.NoError(t, err)
	for i := 0; i < 3; i++ {
		assert.Equal(t, bytes.Repeat([]byte{byte('x' + i)}, segSize), read2Front.Data[i*segSize:(i+1)*segSize])
	}

	read2Cross, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino2,
		Fh:     fh2,
		Offset: crossChunkOffset,
		Size:   uint32(segSize),
	})
	require.NoError(t, err)
	assert.Equal(t, crossPayload, read2Cross.Data)

	// 4. 验证 Disk.UsedBytes 账本 100% 精确守恒（等于 ino1 最终大小 + ino2 最终大小）
	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, expectedIno1Size+expectedIno2Size, diskInfo.UsedBytes)
}

// TestWrite_GroupCommit_OAppendOffsetEvolution_UnitAndPostgres 验证并发多句柄/多协程 O_APPEND 偏移演进：
// 多个 O_APPEND 请求严格按行锁顺序从最新演进 Inode 大小依次累加偏移，零覆盖、零丢失，
// 且各调用方收到的 WriteResponse.Inode.Attr.Size 精确对应其自身写入完成时刻的演进大小快照。
func TestWrite_GroupCommit_OAppendOffsetEvolution_UnitAndPostgres(t *testing.T) {
	ctx := context.Background()
	ino := uint64(9001)
	fh1 := uint64(91)
	fh2 := uint64(92)
	rootIno := int64(90)

	const numAppendTasks = 8
	const writeLen = 512
	const initialSize = 1024

	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	gatedBS := newGateBlockStore(localBS, numAppendTasks)
	t.Cleanup(gatedBS.releaseGate)

	mockStore := dbmock.NewMockStore(t)
	mockStore.EXPECT().
		NextSliceID(mock.Anything).
		Return(int64(7001), nil).
		Once()

	var mu sync.Mutex
	currentSize := int64(initialSize)
	var recordedSlices []meta.Slice
	var finalInodeSizeRecorded int64

	// 先通过 GetInode 将初始配额或文件状态同步，模拟已有 initialSize
	mockStore.EXPECT().
		WriteFileAtomic(mock.Anything, mock.Anything).
		RunAndReturn(func(attemptCtx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
			mu.Lock()
			defer mu.Unlock()

			oldSize := currentSize
			actualOff := uint64(oldSize)
			newSize := oldSize + arg.DataLen
			currentSize = newSize
			finalInodeSizeRecorded = newSize

			recordedSlices = append(recordedSlices, meta.Slice{
				Pos:  uint32(actualOff),
				ID:   uint64(arg.SliceID),
				Size: uint32(arg.DataLen),
				Off:  0,
				Len:  uint32(arg.DataLen),
			})

			return db.WriteFileAtomicRow{
				Status:    "ok",
				OldSize:   oldSize,
				NewSize:   newSize,
				StartIndx: 0,
				EndIndx:   0,
				OctetLen1: int64(len(recordedSlices) * meta.SliceBytes),
				Ino:       arg.Ino,
				Mode:      int32(meta.S_IFREG | 0o644),
				Nlink:     1,
				RootIno:   rootIno,
			}, nil
		}).
		Times(numAppendTasks)

	mgr := meta.New(mockStore, meta.WithBlockStore(gatedBS))

	var wg sync.WaitGroup
	resps := make([]*meta.WriteResponse, numAppendTasks)
	errs := make([]error, numAppendTasks)

	for i := 0; i < numAppendTasks; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			useFh := fh1
			if idx%2 == 1 {
				useFh = fh2
			}
			payload := bytes.Repeat([]byte{byte('0' + idx)}, writeLen)
			resps[idx], errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     useFh,
				Offset: uint64(idx * 9999),
				Data:   payload,
			})
		}(i)
	}
	wg.Wait()

	var returnedSizes []uint64
	for i := 0; i < numAppendTasks; i++ {
		require.NoError(t, errs[i])
		require.NotNil(t, resps[i])
		assert.Equal(t, uint32(writeLen), resps[i].Size)
		returnedSizes = append(returnedSizes, resps[i].Inode.Attr.Size)
	}

	assert.Equal(t, int64(initialSize+numAppendTasks*writeLen), finalInodeSizeRecorded, "最终 Inode.Size 必须精确等于 1024 + 8 * 512")

	// 验证内存配额状态已精确更新为 initialSize + 8 * 512
	mockStore.EXPECT().
		GetInode(mock.Anything, int64(ino)).
		Return(db.Inode{Ino: int64(ino), RootIno: rootIno, Mode: int32(meta.S_IFREG | 0o644), Size: finalInodeSizeRecorded, Nlink: 1}, nil).
		Once()
	mockStore.EXPECT().
		GetDiskByRootIno(mock.Anything, int64(rootIno)).
		Return(db.GetDiskByRootInoRow{RootIno: rootIno, MaxBytes: 100000, UsedBytes: initialSize}, nil).
		Once()
	statRes, statErr := mgr.StatFS(ctx, ino)
	require.NoError(t, statErr)
	assert.Equal(t, uint64(initialSize+numAppendTasks*writeLen), statRes.UsedBytes, "Disk 内存配额必须精确反映 1024 + 8 * 512")

	// 断言 8 条 Slice 的 Pos 严格按 [1024, 1536, 2048, ..., 4608] 顺序演进，零重叠、零跳空
	require.Len(t, recordedSlices, numAppendTasks)
	for i, s := range recordedSlices {
		assert.Equal(t, uint32(initialSize+i*writeLen), s.Pos, "第 %d 条追加 Slice 的起始偏移必须严格紧随前一条", i)
		assert.Equal(t, uint32(writeLen), s.Len)
	}

	// 断言 8 个调用方收到的 WriteResponse.Inode.Attr.Size 排序后恰好是 [1536, 2048, 2560, ..., 5120]
	slices.Sort(returnedSizes)
	for i, sz := range returnedSizes {
		assert.Equal(t, uint64(initialSize+(i+1)*writeLen), sz, "每个 O_APPEND 调用方必须收到其自身完成时刻的精确演进文件大小")
	}
}

// TestWrite_GroupCommit_BadFileHandleIsolationAndBlockCleanup 验证并发混入无效句柄/只读句柄/超期会话（EBADF）时的异常隔离：
// 违规请求单独返回 ErrBadFileHandle 且其阶段 1 物理 Block 文件被彻底删除（零孤儿块残留），
// 其余合法请求正常提交且数据完整可读。
func TestWrite_GroupCommit_BadFileHandleIsolationAndBlockCleanup(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	baseStore := db.NewStore(pool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)

	const totalReqs = 6
	gatedBS := newGateBlockStore(localBS, totalReqs)
	t.Cleanup(gatedBS.releaseGate)

	mgr := meta.New(baseStore, meta.WithBlockStore(gatedBS))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("group-commit-ebadf-%d", time.Now().UnixNano()),
		MaxBytes: 50 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	activeSess := uint64(991001)
	expiredSess := uint64(991002)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(activeSess), int64(expiredSess))
	})
	_, err = mgr.RenewSession(ctx, activeSess)
	require.NoError(t, err)
	_, err = mgr.RenewSession(ctx, expiredSess)
	require.NoError(t, err)

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("ebadf_isolation.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	validFh := createRes.Fh

	roOpenRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       ino,
		Flags:     meta.O_RDONLY,
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	readOnlyFh := roOpenRes.Fh

	expOpenRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       ino,
		Flags:     meta.O_RDWR,
		SessionID: &expiredSess,
	})
	require.NoError(t, err)
	expiredFh := expOpenRes.Fh

	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, baseStore, mgr, int64(ino), validFh, readOnlyFh, expiredFh)
	})

	// 将 expiredSess 在数据库中设为已过期
	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(expiredSess))
	require.NoError(t, err)

	const blockLen = 2048
	type reqSpec struct {
		fh        uint64
		sessionID *uint64
		offset    uint64
		data      []byte
		wantErr   error
	}

	specs := []reqSpec{
		{fh: validFh, sessionID: &activeSess, offset: 0, data: bytes.Repeat([]byte{'A'}, blockLen), wantErr: nil},
		{fh: validFh, sessionID: &activeSess, offset: blockLen, data: bytes.Repeat([]byte{'B'}, blockLen), wantErr: nil},
		{fh: validFh, sessionID: &activeSess, offset: 2 * blockLen, data: bytes.Repeat([]byte{'C'}, blockLen), wantErr: nil},
		{fh: 99999999, sessionID: &activeSess, offset: 3 * blockLen, data: bytes.Repeat([]byte{'X'}, blockLen), wantErr: meta.ErrBadFileHandle},
		{fh: readOnlyFh, sessionID: &activeSess, offset: 4 * blockLen, data: bytes.Repeat([]byte{'Y'}, blockLen), wantErr: meta.ErrBadFileHandle},
		{fh: expiredFh, sessionID: &expiredSess, offset: 5 * blockLen, data: bytes.Repeat([]byte{'Z'}, blockLen), wantErr: meta.ErrBadFileHandle},
	}

	var wg sync.WaitGroup
	errs := make([]error, totalReqs)
	resps := make([]*meta.WriteResponse, totalReqs)

	for i, sp := range specs {
		wg.Add(1)
		go func(idx int, s reqSpec) {
			defer wg.Done()
			resps[idx], errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:       ino,
				Fh:        s.fh,
				Offset:    s.offset,
				Data:      s.data,
				SessionID: s.sessionID,
			})
		}(i, sp)
	}
	wg.Wait()

	for i, sp := range specs {
		if sp.wantErr != nil {
			require.ErrorIs(t, errs[i], sp.wantErr, "请求 %d 应返回 %v", i, sp.wantErr)
			assert.Nil(t, resps[i])
		} else {
			require.NoError(t, errs[i], "合法请求 %d 不应受并发违规请求牵连", i)
			require.NotNil(t, resps[i])
			assert.Equal(t, uint32(blockLen), resps[i].Size)
		}
	}

	// 1. 验证 3 个失败请求的物理块均已被立即清理，磁盘上仅存 3 个合法已提交物理块文件（零孤儿块残留）
	assert.Equal(t, int64(3), gatedBS.deleteCalls.Load(), "3 个 EBADF 违规请求的阶段 1 物理块必须全部调用 DeleteBlock 清理")
	assert.Equal(t, 3, countRegularFilesInDir(t, dataDir), "数据目录中必须精确仅存 3 个已提交物理块文件，零孤儿块残留")

	// 2. 验证文件最终大小与 Disk.UsedBytes 仅包含 3 个合法请求（3 * 2048 = 6144 字节）
	expectedValidSize := uint64(3 * blockLen)
	attr, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, expectedValidSize, attr.Attr.Size)

	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, expectedValidSize, diskInfo.UsedBytes)

	// 3. 验证 3 个合法请求的数据完整可读
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:       ino,
		Fh:        validFh,
		Offset:    0,
		Size:      uint32(expectedValidSize),
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{'A'}, blockLen), readRes.Data[0:blockLen])
	assert.Equal(t, bytes.Repeat([]byte{'B'}, blockLen), readRes.Data[blockLen:2*blockLen])
	assert.Equal(t, bytes.Repeat([]byte{'C'}, blockLen), readRes.Data[2*blockLen:3*blockLen])
}

// TestWrite_GroupCommit_QuotaExceededIsolationAndBlockCleanup 验证并发部分请求超出 Disk 配额（ENOSPC）时的异常隔离：
// 超出剩余配额的扩容请求准确返回 ErrNoSpace 且其阶段 1 物理 Block 被彻底清理；
// 未超配额的扩容请求及原地覆盖写（delta_bytes == 0）正常提交成功，Disk.used_bytes 绝不突破 max_bytes。
func TestWrite_GroupCommit_QuotaExceededIsolationAndBlockCleanup(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	baseStore := db.NewStore(pool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)

	// 设置 Disk 最大配额为 5000 字节
	const maxDiskBytes = 5000
	mgrInit := meta.New(baseStore, meta.WithBlockStore(localBS))
	disk, err := mgrInit.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("group-commit-enospc-%d", time.Now().UnixNano()),
		MaxBytes: maxDiskBytes,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgrInit.DeleteDisk(context.Background(), disk.DiskID)
	})

	createRes, err := mgrInit.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("enospc_isolation.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	fh := createRes.Fh
	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, baseStore, mgrInit, int64(ino), fh)
	})

	// 先写入初始 4000 字节，使 Disk 剩余可用配额恰好只剩 1000 字节（UsedBytes = 4000, MaxBytes = 5000）
	initData := bytes.Repeat([]byte{'I'}, 4000)
	_, err = mgrInit.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   initData,
	})
	require.NoError(t, err)

	const batchReqs = 4
	gatedBS := newGateBlockStore(localBS, batchReqs)
	t.Cleanup(gatedBS.releaseGate)
	mgr := meta.New(baseStore, meta.WithBlockStore(gatedBS))

	type quotaTask struct {
		name    string
		offset  uint64
		data    []byte
		wantErr error
	}
	tasks := []quotaTask{
		{name: "超配额扩容A_2000B", offset: 4000, data: bytes.Repeat([]byte{'X'}, 2000), wantErr: meta.ErrNoSpace},
		{name: "超配额扩容B_1500B", offset: 4000, data: bytes.Repeat([]byte{'Y'}, 1500), wantErr: meta.ErrNoSpace},
		{name: "满盘原地覆写C_2000B", offset: 0, data: bytes.Repeat([]byte{'O'}, 2000), wantErr: nil},
		{name: "合法小扩容D_800B", offset: 4000, data: bytes.Repeat([]byte{'S'}, 800), wantErr: nil},
	}

	var wg sync.WaitGroup
	errs := make([]error, batchReqs)
	resps := make([]*meta.WriteResponse, batchReqs)

	for i, qt := range tasks {
		wg.Add(1)
		go func(idx int, task quotaTask) {
			defer wg.Done()
			resps[idx], errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: task.offset,
				Data:   task.data,
			})
		}(i, qt)
	}
	wg.Wait()

	for i, qt := range tasks {
		if qt.wantErr != nil {
			require.ErrorIs(t, errs[i], qt.wantErr, "%s 必须返回 ErrNoSpace", qt.name)
			assert.Nil(t, resps[i])
		} else {
			require.NoError(t, errs[i], "%s 必须成功", qt.name)
			require.NotNil(t, resps[i])
			assert.Equal(t, uint32(len(qt.data)), resps[i].Size)
		}
	}

	// 1. 验证 2 个超配额请求的阶段 1 物理块已被彻底清理（磁盘仅存初始 1 块 + 本批成功的 C、D 2 块 = 3 块）
	assert.Equal(t, int64(2), gatedBS.deleteCalls.Load(), "2 个超出配额的请求必须调用 DeleteBlock 清理物理块")
	assert.Equal(t, 3, countRegularFilesInDir(t, dataDir), "数据目录中必须仅存 3 个已提交块文件，零孤儿块残留")

	// 2. 验证 Disk.UsedBytes == 4800 <= MaxBytes (5000)，且 Inode.Size == 4800
	diskInfo, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(4800), diskInfo.UsedBytes, "Disk.UsedBytes 必须精确等于 4000 + 800 = 4800，绝不突破 5000")

	attr, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, uint64(4800), attr.Attr.Size)

	// 3. 验证全文件数据视图：[0, 2000) 为覆写的 'O'，[2000, 4000) 为初始的 'I'，[4000, 4800) 为小扩容的 'S'
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   4800,
	})
	require.NoError(t, err)
	assert.Equal(t, bytes.Repeat([]byte{'O'}, 2000), readRes.Data[0:2000])
	assert.Equal(t, bytes.Repeat([]byte{'I'}, 2000), readRes.Data[2000:4000])
	assert.Equal(t, bytes.Repeat([]byte{'S'}, 800), readRes.Data[4000:4800])
}

// TestWrite_GroupCommit_PageCachePublishAndInodeModeIsolation_Unit 验证：
//  1. 阶段 1 PutBlock 发布至页缓存后，阶段 2 直接执行 WriteFileAtomic 提交；
//  2. 混入不存在的 Inode (ErrNotFound)、目录 Inode (ErrIsDir)、FIFO 节点 (ErrInvalidArgument) 及 O_APPEND 溢出 MaxInt64 时，
//     各异常请求精确返回对应哨兵错误并清理物理块，不影响合法 Inode 正常提交。
func TestWrite_GroupCommit_PageCachePublishAndInodeModeIsolation_Unit(t *testing.T) {
	ctx := context.Background()

	t.Run("阶段 1 PutBlock 发布至页缓存后阶段 2 所有合法任务正常提交", func(t *testing.T) {
		dataDir := t.TempDir()
		localBS := meta.NewLocalFSBlockStore(dataDir)
		gatedBS := newGateBlockStore(localBS, 3)
		t.Cleanup(gatedBS.releaseGate)

		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(1001), nil).
			Once()

		ino := uint64(300)
		fh := uint64(30)

		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.Anything).
			Return(db.WriteFileAtomicRow{
				Status:    "ok",
				OldSize:   0,
				NewSize:   300,
				StartIndx: 0,
				EndIndx:   0,
				OctetLen1: 24,
				Ino:       int64(ino),
				Mode:      int32(meta.S_IFREG | 0o644),
				Nlink:     1,
			}, nil).
			Times(3)

		mgr := meta.New(mockStore, meta.WithBlockStore(gatedBS))

		var wg sync.WaitGroup
		errs := make([]error, 3)
		for i := 0; i < 3; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				_, errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
					Ino:    ino,
					Fh:     fh,
					Offset: uint64(idx * 100),
					Data:   bytes.Repeat([]byte{byte('a' + idx)}, 100),
				})
			}(i)
		}
		wg.Wait()

		for i, err := range errs {
			require.NoError(t, err, "请求 %d 应成功提交", i)
		}
		assert.Equal(t, int64(3), gatedBS.putCalls.Load(), "3 个请求各自完成阶段 1 PutBlock")
		assert.Equal(t, int64(0), gatedBS.deleteCalls.Load(), "全部成功提交的任务不应触发 DeleteBlock")
		assert.Equal(t, 3, countRegularFilesInDir(t, dataDir), "数据目录中应保留全部 3 个成功任务的物理块")
	})

	t.Run("并发混入 ErrNotFound、ErrIsDir、FIFO 与 O_APPEND 溢出时逐一隔离并清理物理块", func(t *testing.T) {
		dataDir := t.TempDir()
		localBS := meta.NewLocalFSBlockStore(dataDir)
		const numTasks = 5
		gatedBS := newGateBlockStore(localBS, numTasks)
		t.Cleanup(gatedBS.releaseGate)

		mockStore := dbmock.NewMockStore(t)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(8001), nil).
			Once()

		inoMissing := uint64(101)
		inoDir := uint64(102)
		inoFifo := uint64(103)
		inoOverflow := uint64(104)
		inoValid := uint64(105)
		fh := uint64(11)

		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.Anything).
			RunAndReturn(func(attemptCtx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
				switch uint64(arg.Ino) {
				case inoMissing:
					return db.WriteFileAtomicRow{Status: "not_found"}, nil
				case inoDir:
					return db.WriteFileAtomicRow{Status: "is_dir"}, nil
				case inoFifo, inoOverflow:
					return db.WriteFileAtomicRow{Status: "invalid_arg"}, nil
				default:
					return db.WriteFileAtomicRow{
						Status:    "ok",
						OldSize:   0,
						NewSize:   64,
						StartIndx: 0,
						EndIndx:   0,
						OctetLen1: 24,
						Ino:       arg.Ino,
						Mode:      int32(meta.S_IFREG | 0o644),
						Nlink:     1,
					}, nil
				}
			}).
			Times(numTasks)

		mgr := meta.New(mockStore, meta.WithBlockStore(gatedBS))

		inos := []uint64{inoMissing, inoDir, inoFifo, inoOverflow, inoValid}
		wantErrs := []error{meta.ErrNotFound, meta.ErrIsDir, meta.ErrInvalidArgument, meta.ErrInvalidArgument, nil}
		errs := make([]error, numTasks)

		var wg sync.WaitGroup
		for i, targetIno := range inos {
			wg.Add(1)
			go func(idx int, inoVal uint64) {
				defer wg.Done()
				_, errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
					Ino:    inoVal,
					Fh:     fh,
					Offset: 0,
					Data:   bytes.Repeat([]byte{'k'}, 64),
				})
			}(i, targetIno)
		}
		wg.Wait()

		for i, want := range wantErrs {
			if want != nil {
				require.ErrorIs(t, errs[i], want, "任务 %d 应返回 %v", i, want)
			} else {
				require.NoError(t, errs[i], "合法任务 %d 应成功提交", i)
			}
		}
		assert.Equal(t, int64(4), gatedBS.deleteCalls.Load(), "4 个异常任务的物理块必须全部被清理")
		assert.Equal(t, 1, countRegularFilesInDir(t, dataDir), "仅保留 1 个合法提交任务的物理块文件")
	})
}

type sqlOrderTracer struct {
	mu      sync.Mutex
	queries []string
}

func (t *sqlOrderTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.mu.Lock()
	t.queries = append(t.queries, data.SQL)
	t.mu.Unlock()
	return ctx
}

func (t *sqlOrderTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *sqlOrderTracer) reset() {
	t.mu.Lock()
	t.queries = nil
	t.mu.Unlock()
}

func (t *sqlOrderTracer) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.queries)
}

type orderSpyBlockStore struct {
	meta.BlockStore
	putBlockCalls atomic.Int64
}

func (s *orderSpyBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	s.putBlockCalls.Add(1)
	return s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data)
}

// TestWrite_DeferredSyncAndFsyncDurabilityBarrier_PostgresIntegration 验证 ADR-0026 与 ADR-0028：
//  1. 普通 Write 阶段 1 调用 BlockStore.PutBlock 写入 OS Page Cache，阶段 2 仅发送 1 条包含 set_config('synchronous_commit', 'off', true) 的单语句 CTE SQL（零显式 BEGIN/COMMIT），且执行后连接会话级 SHOW synchronous_commit 仍为 'on'；
//  2. Write 返回后立即发起 Read、GetAttr、Lookup 100% 读到最新数据与最新属性；
//  3. Fsync 与 FsyncDir 句柄校验失败时不触发 WAL 刷盘；
//  4. Fsync 与 FsyncDir 句柄校验通过后仅执行 SELECT pg_current_wal_flush_lsn()::text 刷 PG WAL；
//  5. 当数据库 WAL 刷盘失败时准确返回包装 ErrIO 的错误。
func TestWrite_DeferredSyncAndFsyncDurabilityBarrier_PostgresIntegration(t *testing.T) {
	tracer := &sqlOrderTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)

	spyBS := &orderSpyBlockStore{
		BlockStore: localBS,
	}

	mgr := meta.New(store, meta.WithBlockStore(spyBS))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0028-barrier-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	fileName := []byte("deferred_sync.bin")
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      fileName,
		Mode:      meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	fh := createRes.Fh
	_, err = pool.Exec(ctx, "UPDATE inodes SET mode = $1 WHERE ino = $2", int32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o755), int64(ino))
	require.NoError(t, err)

	openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:   disk.RootIno,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	dirFh := openDirRes.Fh

	t.Cleanup(func() {
		_ = mgr.ReleaseDir(context.Background(), meta.ReleaseDirParams{Ino: disk.RootIno, Fh: dirFh})
		cleanupWriteTestInoAndSlices(t, store, mgr, int64(ino), fh)
	})

	// 预先消费 1 次 NextSliceID 以填满 slice_id 内存预取队列，从而精确测量热路径单次 Write 的 SQL 条数
	warmupData := []byte("w")
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   warmupData,
	})
	require.NoError(t, err)

	// 1. 验证 Write 热路径统一调用 PutBlock，阶段 2 仅发送 1 条包含 set_config('synchronous_commit', 'off', true) 的单语句 CTE SQL（零 BEGIN/COMMIT）
	tracer.reset()
	spyBS.putBlockCalls.Store(0)

	payload1 := []byte("hello-page-cache-immediate-visibility")
	wRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   payload1,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(len(payload1)), wRes.Size)
	assert.Equal(t, int64(1), spyBS.putBlockCalls.Load(), "Write 阶段 1 必须统一调用 PutBlock")

	queries := tracer.snapshot()
	require.Len(t, queries, 1, "Write 热路径在 slice_id 预取命中时必须仅向数据库发送 1 条 SQL 语句（1 RTT，零显式 BEGIN/COMMIT）")
	assert.Contains(t, queries[0], "set_config('synchronous_commit', 'off', true)", "普通 Write 单语句 CTE 必须内联 set_config('synchronous_commit', 'off', true)")
	assert.False(t, strings.Contains(queries[0], "pg_current_wal_flush_lsn"), "普通 Write 不得触发 pg_current_wal_flush_lsn")

	// 验证单语句 CTE 执行完毕后，连接池中连接的会话级 synchronous_commit 仍恢复为 'on'（零污染后续事务）
	var syncCommitVal string
	require.NoError(t, pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommitVal))
	assert.Equal(t, "on", syncCommitVal, "set_config('synchronous_commit', 'off', true) 仅在单语句隐式事务内生效，结束后会话级配置必须仍为 on")

	// 2. Write 返回后立即发起 Read、GetAttr、Lookup，100% 读到最新内容与属性
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(payload1)),
	})
	require.NoError(t, err)
	assert.Equal(t, payload1, readRes.Data)

	attrRes, err := mgr.GetAttr(ctx, ino)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(payload1)), attrRes.Attr.Size)
	assert.Equal(t, uint32(meta.S_IFREG|0o755), attrRes.Attr.Mode)

	lookupRes, err := mgr.Lookup(ctx, disk.RootIno, fileName)
	require.NoError(t, err)
	assert.Equal(t, uint64(len(payload1)), lookupRes.Attr.Size)
	assert.Equal(t, uint32(meta.S_IFREG|0o755), lookupRes.Attr.Mode)

	// 3. Fsync 与 FsyncDir 句柄校验失败时保持错误优先级，且不触发 WAL 刷盘
	tracer.reset()
	require.ErrorIs(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: 99999999}), meta.ErrBadFileHandle)
	require.ErrorIs(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: 99999999}), meta.ErrBadFileHandle)
	for _, q := range tracer.snapshot() {
		assert.False(t, strings.Contains(q, "pg_current_wal_flush_lsn"), "句柄校验失败时不得触发数据库 WAL 刷盘")
	}

	// 4. Fsync 校验通过后仅执行 SELECT pg_current_wal_flush_lsn()::text 刷 PG WAL
	tracer.reset()
	require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh}))

	var walFlushMeasured bool
	for _, q := range tracer.snapshot() {
		if strings.Contains(q, "SELECT pg_current_wal_flush_lsn()::text") {
			walFlushMeasured = true
		}
	}
	assert.True(t, walFlushMeasured, "Fsync 必须执行 SELECT pg_current_wal_flush_lsn()::text")

	// 5. 再次写入新数据后调用 FsyncDir：同样仅执行 SELECT pg_current_wal_flush_lsn()::text
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: uint64(len(payload1)),
		Data:   []byte("-tail"),
	})
	require.NoError(t, err)
	assert.Equal(t, int64(2), spyBS.putBlockCalls.Load(), "第 2 次 Write 同样调用 PutBlock")

	tracer.reset()
	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: dirFh}))

	var dirWalFlushed bool
	for _, q := range tracer.snapshot() {
		if strings.Contains(q, "SELECT pg_current_wal_flush_lsn()::text") {
			dirWalFlushed = true
		}
	}
	assert.True(t, dirWalFlushed, "FsyncDir 必须执行 SELECT pg_current_wal_flush_lsn()::text")

	// 6. 验证数据库 WAL 刷盘失败时，Fsync 与 FsyncDir 均返回包装 ErrIO 的错误
	closedPool, err := pgxpool.New(ctx, getStorageTestDSN())
	require.NoError(t, err)
	closedPool.Close()

	walFailStore := &walFlushFailStore{
		Store:    store,
		failPool: closedPool,
	}
	walFailMgr := meta.New(walFailStore, meta.WithBlockStore(spyBS))

	fsyncWalErr := walFailMgr.Fsync(ctx, meta.FsyncParams{Ino: ino, Fh: fh})
	require.ErrorIs(t, fsyncWalErr, meta.ErrIO, "WAL 刷盘失败时 Fsync 必须返回包装 ErrIO 的错误")

	fsyncDirWalErr := walFailMgr.FsyncDir(ctx, meta.FsyncDirParams{Ino: disk.RootIno, Fh: dirFh})
	require.ErrorIs(t, fsyncDirWalErr, meta.ErrIO, "WAL 刷盘失败时 FsyncDir 必须返回包装 ErrIO 的错误")
}

type walFlushFailStore struct {
	db.Store
	failPool *pgxpool.Pool
}

func (s *walFlushFailStore) Pool() *pgxpool.Pool {
	return s.failPool
}
