package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func TestWrite_Validation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	tests := []struct {
		name string
		req  *meta.WriteRequest
	}{
		{
			name: "req 为 nil",
			req:  nil,
		},
		{
			name: "ino 为 0",
			req: &meta.WriteRequest{
				Ino:  0,
				Fh:   1,
				Data: []byte("a"),
			},
		},
		{
			name: "ino 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:  uint64(math.MaxInt64) + 1,
				Fh:   1,
				Data: []byte("a"),
			},
		},
		{
			name: "fh 为 0",
			req: &meta.WriteRequest{
				Ino:  1,
				Fh:   0,
				Data: []byte("a"),
			},
		},
		{
			name: "fh 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:  1,
				Fh:   uint64(math.MaxInt64) + 1,
				Data: []byte("a"),
			},
		},
		{
			name: "offset 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:    1,
				Fh:     1,
				Offset: uint64(math.MaxInt64) + 1,
				Data:   []byte("a"),
			},
		},
		{
			name: "data 超过 4 MiB (BlockSize)",
			req: &meta.WriteRequest{
				Ino:  1,
				Fh:   1,
				Data: make([]byte, meta.BlockSize+1),
			},
		},
		{
			name: "offset + len(data) 溢出 MaxInt64",
			req: &meta.WriteRequest{
				Ino:    1,
				Fh:     1,
				Offset: uint64(math.MaxInt64),
				Data:   []byte("a"),
			},
		},
		{
			name: "session_id 为 0",
			req: &meta.WriteRequest{
				Ino:       1,
				Fh:        1,
				Data:      []byte("a"),
				SessionID: ptrOf(uint64(0)),
			},
		},
		{
			name: "session_id 超过 MaxInt64",
			req: &meta.WriteRequest{
				Ino:       1,
				Fh:        1,
				Data:      []byte("a"),
				SessionID: ptrOf(uint64(math.MaxInt64) + 1),
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := mgr.Write(ctx, tc.req)
			require.Nil(t, resp)
			require.ErrorIs(t, err, meta.ErrInvalidArgument)
		})
	}
}

func TestWrite_ZeroLengthData(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("0 字节写常规文件且句柄具备写权限时直接返回且不修改元数据与特权位", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 10,
			Data:   nil, // len == 0
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(0), resp.Size)
		require.NotNil(t, resp.Inode)
		assert.Equal(t, ino, resp.Inode.Ino)
		assert.Equal(t, uint64(1024), resp.Inode.Attr.Size)
		// 特权位不应被修改
		assert.Equal(t, uint32(meta.S_IFREG|meta.S_ISUID|meta.S_ISGID|0o644), resp.Inode.Attr.Mode)
	})

	t.Run("0 字节写带 session_id 时内联校验句柄与会话且不执行搭便车续期", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)
		sessionID := uint64(999)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(db.FileHandle{
				Fh:        int64(fh),
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:       ino,
			Fh:        fh,
			Offset:    0,
			Data:      []byte{},
			SessionID: &sessionID,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(0), resp.Size)
		mockQuerier.AssertNotCalled(t, "UpsertClientSession", mock.Anything, mock.Anything)
		mockQuerier.AssertNotCalled(t, "DeleteExpiredClientSessions", mock.Anything)
		mockQuerier.AssertNotCalled(t, "DeleteUnreferencedOrphanInodes", mock.Anything)
	})

	t.Run("0 字节写目标为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		dirRow := baseFileRow
		dirRow.Mode = int32(meta.S_IFDIR | 0o755)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(dirRow, nil).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("0 字节写目标是非常规文件返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fifoRow := baseFileRow
		fifoRow.Mode = int32(meta.S_IFIFO | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fifoRow, nil).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("0 字节写目标 Inode 不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("0 字节写句柄为只读 O_RDONLY 返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}, nil).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("0 字节写句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:  ino,
			Fh:   fh,
			Data: nil,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

func TestWrite_TwoPhaseCommit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(10)
	sliceID := int64(42)
	payload := []byte("hello-data-layer")

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
		Size:       50,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("两阶段正常写入成功：文件扩容、特权位剥离、mtime/ctime刷新", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1：锁外申请 slice_id 与写入物理块
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：单语句 CTE WriteFileAtomic 提交
		expectedNewSize := int64(50 + len(payload))
		expectedNewMode := int32(meta.S_IFREG | 0o755) // S_ISUID 与 S_ISGID 被剥离

		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       50,
				DataLen:         int64(len(payload)),
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    50,
				NewSize:    expectedNewSize,
				StartIndx:  0,
				EndIndx:    0,
				OctetLen1:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       expectedNewMode,
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
			Offset: 50,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(len(payload)), resp.Size)
		require.NotNil(t, resp.Inode)
		assert.Equal(t, uint64(expectedNewSize), resp.Inode.Attr.Size)
		assert.Equal(t, uint32(expectedNewMode), resp.Inode.Attr.Mode)
	})

	t.Run("内部覆写：offset+len <= oldSize 时文件尺寸保持不变，但特权位剥离且更新时间戳", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2
		// offset=100, len=16 -> newEnd=116 <= 1000，size 保持 1000
		expectedNewMode := int32(meta.S_IFREG | 0o755)
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       100,
				DataLen:         int64(len(payload)),
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    1000,
				NewSize:    1000,
				StartIndx:  0,
				EndIndx:    0,
				OctetLen1:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       expectedNewMode,
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
			Offset: 100,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint64(1000), resp.Inode.Attr.Size)
		assert.Equal(t, uint32(expectedNewMode), resp.Inode.Attr.Mode)
	})

	t.Run("阶段 1 PutBlock 失败：返回 ErrIO 且不污染数据库元数据", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(errors.New("disk write failure")).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.Nil(t, resp)
		require.ErrorIs(t, err, meta.ErrIO)
	})

	t.Run("阶段 2 事务回滚失败：在事务外自动调用 DeleteBlock 清理物理块确保零残留", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		// 阶段 1 成功
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2 单语句 CTE 返回 not_found
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.Anything).
			Return(db.WriteFileAtomicRow{Status: "not_found"}, nil).
			Once()

		// 关键断言：事务外必须调用 DeleteBlock 回滚清理阶段 1 物理块！
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
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("阶段 2 句柄只读返回 ErrBadFileHandle 并清理物理块", func(t *testing.T) {
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
			WriteFileAtomic(mock.Anything, mock.Anything).
			Return(db.WriteFileAtomicRow{Status: "bad_handle"}, nil).
			Once()

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
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})
}

func TestFsync_ValidationAndPersistence(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)
	fh := uint64(10)

	t.Run("目标不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino: ino,
			Fh:  fh,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("句柄不存在返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{}, pgx.ErrNoRows).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino: ino,
			Fh:  fh,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("合法常规文件与句柄 Fsync 成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockReadOnlyRepeatableReadTx(mockStore, mockQuerier, 1)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{
				Ino:  int64(ino),
				Mode: int32(meta.S_IFREG | 0o644),
			}, nil).
			Once()

		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{
				Fh:    int64(fh),
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY),
			}, nil).
			Once()

		err := mgr.Fsync(ctx, meta.FsyncParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})
}

// 辅助方法：生成指针
func ptrOf[T any](v T) *T {
	return &v
}

// cleanupWriteTestInoAndSlices 清理目标 Inode 关联的所有 chunks 及产生的测试专有 slice_refs，并断言零残留
func cleanupWriteTestInoAndSlices(t *testing.T, store db.Store, mgr meta.Manager, ino int64, fhs ...uint64) {
	t.Helper()
	cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if mgr != nil {
		for _, fh := range fhs {
			_ = mgr.Release(cleanupCtx, meta.ReleaseParams{Ino: uint64(ino), Fh: fh})
		}
	}

	var sliceIDs []int64
	for indx := int64(0); indx < 16; indx++ {
		chunkRow, err := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: ino, Indx: indx})
		if err == nil && len(chunkRow.Slices) > 0 {
			if slices, err := meta.DecodeSlices(chunkRow.Slices); err == nil {
				for _, s := range slices {
					if s.ID > 0 {
						sliceIDs = append(sliceIDs, int64(s.ID))
					}
				}
			}
		}
	}

	_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{ino})
	for _, sid := range sliceIDs {
		_ = store.DeleteSliceRef(cleanupCtx, sid)
	}
	_ = store.DeleteInode(cleanupCtx, ino)

	// 严格断言目标 ino 的 inodes、chunks 与本测试产生的 slice_refs 均已清除无残留！
	_, inoErr := store.GetInode(cleanupCtx, ino)
	assert.ErrorIs(t, inoErr, pgx.ErrNoRows, "目标 ino 必须已被清除")

	for indx := int64(0); indx < 16; indx++ {
		_, cErr := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: ino, Indx: indx})
		assert.ErrorIs(t, cErr, pgx.ErrNoRows, "目标 ino 的 chunk 行必须已被清除")
	}

	for _, sid := range sliceIDs {
		_, sErr := store.GetSliceRef(cleanupCtx, sid)
		assert.ErrorIs(t, sErr, pgx.ErrNoRows, "测试专有 slice_ref %d 必须已被清除", sid)
	}
}

func TestWrite_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

	// 创建测试常规文件
	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	// 打开句柄
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, store, mgr, fileRow.Ino, fh)
	})

	// 1. 首次写入 10 字节
	data1 := []byte("0123456789")
	writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   data1,
	})
	require.NoError(t, err)
	require.NotNil(t, writeRes)
	assert.Equal(t, uint32(10), writeRes.Size)
	assert.Equal(t, uint64(10), writeRes.Inode.Attr.Size)
	// 特权位被剥离
	assert.Equal(t, uint32(meta.S_IFREG|0o755), writeRes.Inode.Attr.Mode)

	// 验证 chunks 表记录
	chunkRow, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  int64(ino),
		Indx: 0,
	})
	require.NoError(t, err)
	slices, err := meta.DecodeSlices(chunkRow.Slices)
	require.NoError(t, err)
	require.Len(t, slices, 1)
	assert.Equal(t, uint32(0), slices[0].Pos)
	assert.Equal(t, uint32(10), slices[0].Len)
	sliceID1 := slices[0].ID

	// 验证 slice_refs 表记录
	sliceRef, err := store.GetSliceRef(ctx, int64(sliceID1))
	require.NoError(t, err)
	assert.Equal(t, int32(10), sliceRef.Size)
	assert.Equal(t, int32(1), sliceRef.Refs)

	// 验证本地 NVMe 物理块文件精确可读
	readBuf := make([]byte, 10)
	err = bs.GetBlock(ctx, sliceID1, 0, 10, 0, readBuf)
	require.NoError(t, err)
	assert.Equal(t, data1, readBuf)

	// 2. 内部覆写：offset = 2, len = 4 (data = "xxxx")，写入区间 [2, 6) <= 10
	data2 := []byte("xxxx")
	writeRes2, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 2,
		Data:   data2,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(4), writeRes2.Size)
	assert.Equal(t, uint64(10), writeRes2.Inode.Attr.Size, "内部覆写不应改变文件总尺寸")

	// 验证 chunks 追加了第 2 个 slice
	chunkRow2, err := store.GetChunk(ctx, db.GetChunkParams{
		Ino:  int64(ino),
		Indx: 0,
	})
	require.NoError(t, err)
	slices2, err := meta.DecodeSlices(chunkRow2.Slices)
	require.NoError(t, err)
	require.Len(t, slices2, 2)
	assert.Equal(t, uint32(2), slices2[1].Pos)
	assert.Equal(t, uint32(4), slices2[1].Len)

	// 3. Fsync 验证
	err = mgr.Fsync(ctx, meta.FsyncParams{
		Ino: ino,
		Fh:  fh,
	})
	require.NoError(t, err)
}

// cancelOnPutBlockStore 包装真实 BlockStore，在 PutBlock 落盘成功后立即取消 ctx。
type cancelOnPutBlockStore struct {
	meta.BlockStore
	cancelFn context.CancelFunc
}

func (s *cancelOnPutBlockStore) PutBlock(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
	if err := s.BlockStore.PutBlock(ctx, sliceID, blockIdx, data); err != nil {
		return err
	}
	if s.cancelFn != nil {
		s.cancelFn()
	}
	return nil
}

func TestWrite_ContextCanceledAfterPutBlock_CleansUpOrphanBlock(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)

	fileRow, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFREG | 0o755),
		Nlink: 1,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	ino := uint64(fileRow.Ino)

	initMgr := meta.New(store, meta.WithBlockStore(bs))
	openRes, err := initMgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh
	t.Cleanup(func() {
		cleanupWriteTestInoAndSlices(t, store, initMgr, fileRow.Ino, fh)
	})

	reqCtx, reqCancel := context.WithCancel(ctx)
	wrappedBS := &cancelOnPutBlockStore{
		BlockStore: bs,
		cancelFn:   reqCancel,
	}
	mgr := meta.New(store, meta.WithBlockStore(wrappedBS))

	writeData := []byte("hello context canceled orphan cleanup test")
	_, writeErr := mgr.Write(reqCtx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   writeData,
	})
	require.Error(t, writeErr, "请求取消后阶段 2 事务应失败")

	var regularFiles int
	walkErr := filepath.WalkDir(dataDir, func(path string, d os.DirEntry, walkErr error) error {
		require.NoError(t, walkErr)
		if !d.IsDir() {
			regularFiles++
		}
		return nil
	})
	require.NoError(t, walkErr)
	assert.Equal(t, 0, regularFiles, "阶段 2 事务因 context 取消失败回滚时，阶段 1 已落盘物理块必须被立即回滚清理，不能残留孤儿块")
}

// TestWrite_TxRetryOnRetryableError_CleansUpBlockOnSecondAttemptFailure 验证 WriteFileAtomic 遇到可重试事务错误 (40001) 自动重试，并在第 2 次失败时精确清理物理块
func TestWrite_TxRetryOnRetryableError_CleansUpBlockOnSecondAttemptFailure(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)
	fh := uint64(10)

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(101), uint32(0), []byte("test-data")).Return(nil).Once()
	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(101), nil).Once()

	var callCount int
	mockStore.EXPECT().
		WriteFileAtomic(mock.Anything, mock.Anything).
		RunAndReturn(func(attemptCtx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
			callCount++
			if callCount == 1 {
				return db.WriteFileAtomicRow{}, &pgconn.PgError{Code: "40001"}
			}
			return db.WriteFileAtomicRow{Status: "not_found"}, nil
		}).
		Twice()

	// 精确调用 1 次 mockBS.DeleteBlock 清理阶段 1 物理块
	mockBS.EXPECT().DeleteBlock(mock.Anything, uint64(101), uint32(0), uint32(len("test-data"))).Return(nil).Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	resp, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   []byte("test-data"),
	})
	require.Nil(t, resp)
	require.Error(t, err)
	assert.ErrorIs(t, err, meta.ErrNotFound, "第 2 次重试 WriteFileAtomic 返回 not_found 应映射为 ErrNotFound")
}

// TestWrite_ContextCanceledDuringInFlightWriteFileAtomic_CompletesWithoutDeletingCommittedBlock 验证一旦进入 WriteFileAtomic 在途提交，
// 即使客户端连接断开或取消 reqCtx，WriteFileAtomic 仍使用独立的未取消上下文完成提交，绝不误删已提交的物理块。
func TestWrite_ContextCanceledDuringInFlightWriteFileAtomic_CompletesWithoutDeletingCommittedBlock(t *testing.T) {
	reqCtx, reqCancel := context.WithCancel(context.Background())
	defer reqCancel()

	ino := uint64(100)
	fh := uint64(10)
	data := []byte("in-flight-commit-data")

	mockStore := dbmock.NewMockStore(t)
	mockBS := metamock.NewMockBlockStore(t)

	mockBS.EXPECT().PutBlock(mock.Anything, uint64(101), uint32(0), data).Return(nil).Once()
	mockStore.EXPECT().NextSliceID(mock.Anything).Return(int64(101), nil).Once()

	mockStore.EXPECT().
		WriteFileAtomic(mock.Anything, mock.Anything).
		RunAndReturn(func(commitCtx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
			// 模拟客户端在 WriteFileAtomic SQL 在途执行期间触发 FUSE_INTERRUPT / RST_STREAM 取消请求上下文
			reqCancel()
			assert.NoError(t, commitCtx.Err(), "在途 WriteFileAtomic 的 commitCtx 不得随请求取消而中断")
			return db.WriteFileAtomicRow{
				Status:  "ok",
				OldSize: 0,
				NewSize: int64(len(data)),
			}, nil
		}).
		Once()

	mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))
	resp, err := mgr.Write(reqCtx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   data,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	assert.Equal(t, uint32(len(data)), resp.Size)
}

// TestWrite_HighSparseChunkIndxBeyondInt32_PostgresIntegration 验证在超过 128 PiB
// (indx >= 1<<31) 的高稀疏偏移处写入与跨块写入时，chunks.indx 保持 64 位精度，
// 绝不因 uint32/int32 截断回绕覆盖 indx=0 或产生负数索引违规。
func TestWrite_HighSparseChunkIndxBeyondInt32_PostgresIntegration(t *testing.T) {
	pool, ctx := setupMetaTestDB(t)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	bs := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(bs))

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
	wrapIndx := int64(1) << 32 // 4294967296 (uint32 强转会回绕为 0！)
	wrapOffset := uint64(wrapIndx) * uint64(meta.ChunkSize)

	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh := openRes.Fh

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.Release(cleanupCtx, meta.ReleaseParams{Ino: ino, Fh: fh})
		for _, indx := range []int64{0, highIndx, wrapIndx} {
			chunkRow, cErr := store.GetChunk(cleanupCtx, db.GetChunkParams{Ino: int64(ino), Indx: indx})
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
		_ = meta.DeleteAllChunksByInos(cleanupCtx, store, []int64{int64(ino)})
		_ = store.DeleteInode(cleanupCtx, int64(ino))
	})

	// 1. 在 offset = 0 (indx = 0) 写入基准载荷
	lowPayload := []byte("chunk-zero-origin-payload")
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   lowPayload,
	})
	require.NoError(t, err)

	// 2. 在 offset = 1<<57 (indx = 1<<31 = 2147483648，int32 强转会变成 -2147483648 触发 chk_chunks_indx 报错) 写入载荷
	highPayload := []byte("high-sparse-128pib-payload")
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset,
		Data:   highPayload,
	})
	require.NoError(t, err, "offset=128PiB (indx=1<<31) 写入必须成功，不得因 int32 符号位溢出报错")

	// 3. 在 offset = 1<<58 (indx = 1<<32 = 4294967296，uint32 强转会回绕为 indx = 0 覆盖首块) 写入载荷
	wrapPayload := []byte("wrap-boundary-256pib-payload")
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: wrapOffset,
		Data:   wrapPayload,
	})
	require.NoError(t, err, "offset=256PiB (indx=1<<32) 写入必须成功")

	// 4. 验证数据库底表与 Read 返回：三个偏移的数据相互独立，indx=0 绝未被覆盖
	readLow, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Size:   uint32(len(lowPayload)),
	})
	require.NoError(t, err)
	assert.Equal(t, lowPayload, readLow.Data, "indx=0 的数据绝不能被 indx=1<<32 的写入回绕覆盖")

	readHigh, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: highOffset,
		Size:   uint32(len(highPayload)),
	})
	require.NoError(t, err)
	assert.Equal(t, highPayload, readHigh.Data, "indx=1<<31 的数据必须精确可读")

	readWrap, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: wrapOffset,
		Size:   uint32(len(wrapPayload)),
	})
	require.NoError(t, err)
	assert.Equal(t, wrapPayload, readWrap.Data, "indx=1<<32 的数据必须精确可读")
}

func TestWrite_SliceIDBatchPreallocation(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(701)
	fh := uint64(71)

	t.Run("配置步长为 8 时连续写入 20 次仅触发 3 次底层 NextSliceID 且分配 ID 严格单调递增", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		var nextSliceCalls atomic.Int64
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			RunAndReturn(func(ctx context.Context) (int64, error) {
				callIdx := nextSliceCalls.Add(1)
				// 第 1 次返回 1001，第 2 次返回 1009，第 3 次返回 1017
				return 1000 + (callIdx-1)*8 + 1, nil
			}).
			Times(3)

		var allocatedIDs []uint64
		mockBS.EXPECT().
			PutBlock(mock.Anything, mock.Anything, uint32(0), mock.Anything).
			RunAndReturn(func(ctx context.Context, sliceID uint64, blockIdx uint32, data []byte) error {
				allocatedIDs = append(allocatedIDs, sliceID)
				return nil
			}).
			Times(20)

		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.Anything).
			RunAndReturn(func(attemptCtx context.Context, arg db.WriteFileAtomicParams) (db.WriteFileAtomicRow, error) {
				return db.WriteFileAtomicRow{
					Status:    "ok",
					OldSize:   arg.ReqOffset,
					NewSize:   arg.ReqOffset + arg.DataLen,
					StartIndx: 0,
					EndIndx:   0,
					OctetLen1: 24,
					Ino:       int64(ino),
					Mode:      int32(meta.S_IFREG | 0o644),
					Nlink:     1,
					Atime:     pgtype.Timestamptz{Time: now, Valid: true},
					Mtime:     pgtype.Timestamptz{Time: now, Valid: true},
					Ctime:     pgtype.Timestamptz{Time: now, Valid: true},
				}, nil
			}).
			Times(20)

		mgr := meta.New(
			mockStore,
			meta.WithBlockStore(mockBS),
			meta.WithSliceIDBatchSize(8),
		)

		payload := []byte("batch-alloc-test")
		for i := 0; i < 20; i++ {
			resp, err := mgr.Write(ctx, &meta.WriteRequest{
				Ino:    ino,
				Fh:     fh,
				Offset: uint64(i * len(payload)),
				Data:   payload,
			})
			require.NoError(t, err)
			require.NotNil(t, resp)
		}

		assert.Equal(t, int64(3), nextSliceCalls.Load(), "步长为 8 时连续 20 次写入应仅触发 3 次底层批次拉取")
		require.Len(t, allocatedIDs, 20)
		for i := 0; i < 20; i++ {
			assert.Equal(t, uint64(1001+i), allocatedIDs[i], "第 %d 次分配的 slice_id 必须严格单调递增", i)
		}
	})

	t.Run("PostgreSQL 集成验证：内存号段预取真实推进 slice_id_seq 且跨实例写入零主键冲突", func(t *testing.T) {
		pool, pgCtx := setupMetaTestDB(t)
		if pool == nil {
			return
		}
		acquireTestSliceGCAdvisoryLock(t, pool)

		baseStore := db.NewStore(pool)
		var nextSliceCalls atomic.Int64
		countingStore := &sliceIDCountingStore{
			Store: baseStore,
			pool:  pool,
			onNextSliceID: func() {
				nextSliceCalls.Add(1)
			},
		}

		dataDir := t.TempDir()
		bs := meta.NewLocalFSBlockStore(dataDir)
		mgr1 := meta.New(countingStore, meta.WithBlockStore(bs), meta.WithSliceIDBatchSize(8))
		mgr2 := meta.New(baseStore, meta.WithBlockStore(bs), meta.WithSliceIDBatchSize(4))

		fileRow, err := baseStore.CreateInode(pgCtx, db.CreateInodeParams{
			Mode:  int32(meta.S_IFREG | 0o644),
			Nlink: 1,
			Uid:   1000,
			Gid:   1000,
		})
		require.NoError(t, err)
		targetIno := uint64(fileRow.Ino)

		openRes, err := mgr1.Open(pgCtx, meta.OpenParams{
			Ino:   targetIno,
			Flags: meta.O_RDWR,
		})
		require.NoError(t, err)
		targetFh := openRes.Fh
		t.Cleanup(func() {
			cleanupWriteTestInoAndSlices(t, baseStore, mgr1, fileRow.Ino, targetFh)
		})

		// mgr1 (batchSize=8) 连续写入 20 次，其间穿插 mgr2 写入，验证序列推进无冲突
		for i := 0; i < 20; i++ {
			data := fmt.Appendf(nil, "pg-batch-m1-%02d", i)
			_, wErr := mgr1.Write(pgCtx, &meta.WriteRequest{
				Ino:    targetIno,
				Fh:     targetFh,
				Offset: uint64(i * 64),
				Data:   data,
			})
			require.NoError(t, wErr)

			if i%5 == 0 {
				data2 := fmt.Appendf(nil, "pg-batch-m2-%02d", i)
				_, wErr2 := mgr2.Write(pgCtx, &meta.WriteRequest{
					Ino:    targetIno,
					Fh:     targetFh,
					Offset: uint64(2000 + i*64),
					Data:   data2,
				})
				require.NoError(t, wErr2)
			}
		}

		assert.Equal(t, int64(3), nextSliceCalls.Load(), "PostgreSQL 下步长为 8 的 mgr1 写入 20 次仅调用 3 次 NextSliceID")
	})
}

type sliceIDCountingStore struct {
	db.Store
	pool          *pgxpool.Pool
	onNextSliceID func()
}

func (s *sliceIDCountingStore) Pool() *pgxpool.Pool {
	return s.pool
}

func (s *sliceIDCountingStore) NextSliceID(ctx context.Context) (int64, error) {
	if s.onNextSliceID != nil {
		s.onNextSliceID()
	}
	return s.Store.NextSliceID(ctx)
}

func TestWrite_SessionRenewalThrottleAndStaleSessionIntercept(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(801)
	fh := uint64(81)
	sessionID := uint64(88001)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("同一活跃 ClientSession 连续与并发多次 Write 零次触发 UpsertClientSession 且零全表扫描", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(2001), nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, mock.Anything, uint32(0), mock.Anything).
			Return(nil)

		handleCreatedAt := now.Add(-5 * time.Minute)

		// 非零字节写入走单语句 WriteFileAtomic，0 字节写入走 writeZeroBytes (ExecTxWithOptions)
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.Anything).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    1024,
				NewSize:    1024,
				StartIndx:  0,
				EndIndx:    0,
				OctetLen1:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       int32(meta.S_IFREG | 0o644),
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Times(12)

		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything).
			RunAndReturn(func(attemptCtx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				mockQuerier := dbmock.NewMockQuerier(t)
				mockQuerier.EXPECT().
					GetInodeForUpdate(mock.Anything, int64(ino)).
					Return(baseFileRow, nil).
					Once()
				mockQuerier.EXPECT().
					GetFileHandle(mock.Anything, db.GetFileHandleParams{
						Fh:        int64(fh),
						Ino:       int64(ino),
						SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
					}).
					Return(db.FileHandle{
						Fh:        int64(fh),
						Ino:       int64(ino),
						Flags:     int32(meta.O_RDWR),
						CreatedAt: pgtype.Timestamptz{Time: handleCreatedAt, Valid: true},
						SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
					}, nil).
					Once()
				err := fn(mockQuerier)
				mockQuerier.AssertNotCalled(t, "UpsertClientSession", mock.Anything, mock.Anything)
				mockQuerier.AssertNotCalled(t, "DeleteExpiredClientSessions", mock.Anything)
				mockQuerier.AssertNotCalled(t, "DeleteUnreferencedOrphanInodes", mock.Anything)
				return err
			}).
			Times(4)

		mgr := meta.New(
			mockStore,
			meta.WithBlockStore(mockBS),
		)

		const totalWrites = 16
		var wg sync.WaitGroup
		errs := make([]error, totalWrites)
		for i := 0; i < totalWrites; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				var data []byte
				if idx < 12 {
					data = fmt.Appendf(nil, "unidirectional-write-%d", idx)
				}
				_, errs[idx] = mgr.Write(ctx, &meta.WriteRequest{
					Ino:       ino,
					Fh:        fh,
					Offset:    uint64(idx * 32),
					Data:      data,
					SessionID: &sessionID,
				})
			}(i)
		}
		wg.Wait()
		for i, err := range errs {
			require.NoError(t, err, "第 %d 次并发写入应成功", i)
		}
	})

	t.Run("WriteFileAtomic 内联拦截已超期或已退役重建会话并返回 ErrBadFileHandle 且回滚清理物理块", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		payload := []byte("intercepted-write")

		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(3001), nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, mock.Anything, uint32(0), payload).
			Return(nil).
			Times(2)

		mockBS.EXPECT().
			DeleteBlock(mock.Anything, mock.Anything, uint32(0), uint32(len(payload))).
			Return(nil).
			Times(2)

		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.Anything).
			Return(db.WriteFileAtomicRow{Status: "bad_handle"}, nil).
			Times(2)

		mgr := meta.New(
			mockStore,
			meta.WithBlockStore(mockBS),
		)

		// Step 1: 退役旧代际句柄 -> WriteFileAtomic 返回 bad_handle -> ErrBadFileHandle
		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:       ino,
			Fh:        fh,
			Offset:    0,
			Data:      payload,
			SessionID: &sessionID,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		// Step 2: 已超期会话 -> WriteFileAtomic 返回 bad_handle -> ErrBadFileHandle
		expiredSess := sessionID + 1
		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:       ino,
			Fh:        fh + 2,
			Offset:    0,
			Data:      payload,
			SessionID: &expiredSess,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)
	})

	t.Run("PostgreSQL 集成验证：Write 不推进 client_sessions.updated_at 且会话过期或退役重建均被 GetFileHandle 100% 实时拦截", func(t *testing.T) {
		pool, pgCtx := setupMetaTestDB(t)
		if pool == nil {
			return
		}
		acquireTestSliceGCAdvisoryLock(t, pool)

		store := db.NewStore(pool)
		dataDir := t.TempDir()
		bs := meta.NewLocalFSBlockStore(dataDir)
		mgr := meta.New(store, meta.WithBlockStore(bs))

		disk, err := mgr.CreateDisk(pgCtx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("write-session-check-%d", time.Now().UnixNano()),
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		})

		sess := uint64(970532)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sess))
		})

		_, err = mgr.RenewSession(pgCtx, sess)
		require.NoError(t, err)

		createRes, err := mgr.Create(pgCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("unidirectional_session_check.bin"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sess,
		})
		require.NoError(t, err)
		targetIno := createRes.Inode.Ino
		targetFh := createRes.Fh
		t.Cleanup(func() {
			cleanupWriteTestInoAndSlices(t, store, mgr, int64(targetIno), targetFh)
		})

		var beforeUpdatedAt time.Time
		require.NoError(t, pool.QueryRow(pgCtx, "SELECT updated_at FROM client_sessions WHERE session_id = $1", int64(sess)).Scan(&beforeUpdatedAt))
		time.Sleep(10 * time.Millisecond)

		// 1. 第 1 次写入成功，且不修改 client_sessions.updated_at
		_, err = mgr.Write(pgCtx, &meta.WriteRequest{
			Ino:       targetIno,
			Fh:        targetFh,
			Offset:    0,
			Data:      []byte("first-ok"),
			SessionID: &sess,
		})
		require.NoError(t, err)

		var afterUpdatedAt time.Time
		require.NoError(t, pool.QueryRow(pgCtx, "SELECT updated_at FROM client_sessions WHERE session_id = $1", int64(sess)).Scan(&afterUpdatedAt))
		assert.True(t, beforeUpdatedAt.Equal(afterUpdatedAt), "Write 绝不能搭便车推进 client_sessions.updated_at")

		// 2. 模拟该会话在数据库中已重建为更新的 created_at（使 targetFh.created_at < session.created_at）
		_, err = pool.Exec(pgCtx, "UPDATE client_sessions SET created_at = clock_timestamp() + interval '5 seconds' WHERE session_id = $1", int64(sess))
		require.NoError(t, err)

		_, err = mgr.Write(pgCtx, &meta.WriteRequest{
			Ino:       targetIno,
			Fh:        targetFh,
			Offset:    8,
			Data:      []byte("stale-handle-in-window"),
			SessionID: &sess,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "已退役重建会话的旧句柄必须 100% 返回 ErrBadFileHandle")

		// 3. 恢复 created_at 但令会话过期，同样必须 100% 拦截返回 ErrBadFileHandle
		_, err = pool.Exec(pgCtx, "UPDATE client_sessions SET created_at = clock_timestamp() - interval '1 hour', expires_at = clock_timestamp() - interval '1 second' WHERE session_id = $1", int64(sess))
		require.NoError(t, err)

		_, err = mgr.Write(pgCtx, &meta.WriteRequest{
			Ino:       targetIno,
			Fh:        targetFh,
			Offset:    8,
			Data:      []byte("expired-in-window"),
			SessionID: &sess,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle, "已过期会话必须 100% 返回 ErrBadFileHandle")
	})
}
