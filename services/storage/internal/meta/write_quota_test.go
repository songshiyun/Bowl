package meta_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
	metamock "github.com/starsrc/bowl/services/storage/internal/meta/mock"
)

func TestWrite_DiskQuota(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	rootIno := int64(10)
	fh := uint64(1)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       0,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		RootIno:    rootIno,
	}

	t.Run("首次写入扣减配额：文件大小从 0 扩容至 1024 字节，WriteFileAtomic 返回新旧大小并在内存中精确累加 usedBytes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(1)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xAA
		}

		// 阶段 1：落盘物理块
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：单语句 CTE 提交
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       0,
				DataLen:         1024,
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    0,
				NewSize:    1024,
				StartIndx:  0,
				EndIndx:    0,
				OctetLen1:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       baseFileRow.Mode,
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
				RootIno:    rootIno,
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(1024), resp.Size)
		require.NotNil(t, resp.Inode)
		assert.Equal(t, uint64(1024), resp.Inode.Attr.Size)
		mockStore.AssertExpectations(t)
	})

	t.Run("追加与跨洞扩展写：原 size 1024，在 offset 2048 写入 1024 字节，逻辑增量为 2048 字节原子核算", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(2)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xBB
		}

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
				ReqOffset:       2048,
				DataLen:         1024,
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    1024,
				NewSize:    3072,
				StartIndx:  0,
				EndIndx:    0,
				OctetLen1:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       baseFileRow.Mode,
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
				RootIno:    rootIno,
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 2048,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(1024), resp.Size)
		assert.Equal(t, uint64(3072), resp.Inode.Attr.Size)
		mockStore.AssertExpectations(t)
	})

	t.Run("100% 满盘状态下内部覆写成功：newEnd <= inode.Size 时 delta_bytes 为 0，内部覆写依然成功", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(3)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xCC
		}

		// 阶段 1：落盘物理块
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：单语句 CTE 提交
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       512,
				DataLen:         1024,
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:     "ok",
				OldSize:    4096,
				NewSize:    4096,
				StartIndx:  0,
				EndIndx:    0,
				OctetLen1:  24,
				Ino:        int64(ino),
				Generation: 1,
				Mode:       baseFileRow.Mode,
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
				RootIno:    rootIno,
			}, nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 512,
			Data:   payload,
		})

		require.NoError(t, err)
		require.NotNil(t, resp)
		assert.Equal(t, uint32(1024), resp.Size)
		assert.Equal(t, uint64(4096), resp.Inode.Attr.Size)
		mockStore.AssertExpectations(t)
	})

	t.Run("超配额写入拦截返回 ErrNoSpace 且临时物理块被彻底删除", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(4)
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 0xDD
		}

		// 阶段 1：落盘物理块成功
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(sliceID, nil).
			Once()

		mockBS.EXPECT().
			PutBlock(mock.Anything, uint64(sliceID), uint32(0), payload).
			Return(nil).
			Once()

		// 阶段 2：单语句 CTE 检查配额超限并返回 quota_exceeded
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, db.WriteFileAtomicParams{
				Ino:             int64(ino),
				Fh:              int64(fh),
				SessionID:       pgtype.Int8{},
				ReqOffset:       0,
				DataLen:         1024,
				QuotaAvailBytes: math.MaxInt64,
				SliceID:         sliceID,
			}).
			Return(db.WriteFileAtomicRow{
				Status:        "quota_exceeded",
				QuotaExceeded: true,
				OldSize:       0,
				NewSize:       0,
				Ino:           int64(ino),
				RootIno:       rootIno,
			}, nil).
			Once()

		// 关键断言：超配额拦截后，阶段 1 写入的 Block 必须被 DeleteBlock 清理！
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(1024)).
			Return(nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.Nil(t, resp)
		require.Error(t, err)
		assert.ErrorIs(t, err, meta.ErrNoSpace)
		mockStore.AssertExpectations(t)
		mockBS.AssertExpectations(t)
	})

	t.Run("WriteFileAtomic 数据库错误：回滚并清理物理块", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockBS := metamock.NewMockBlockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(mockBS))

		sliceID := int64(6)
		payload := make([]byte, 512)

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
			Return(db.WriteFileAtomicRow{}, errors.New("connection failed")).
			Once()

		// 数据库调用失败，物理块被清理
		mockBS.EXPECT().
			DeleteBlock(mock.Anything, uint64(sliceID), uint32(0), uint32(512)).
			Return(nil).
			Once()

		resp, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:    ino,
			Fh:     fh,
			Offset: 0,
			Data:   payload,
		})

		require.Nil(t, resp)
		require.Error(t, err)
		mockStore.AssertExpectations(t)
		mockBS.AssertExpectations(t)
	})
}
