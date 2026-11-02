package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestOpen_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("常规文件只读模式正常打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      1,
				Mode:    baseFileRow.Mode,
				Size:    baseFileRow.Size,
				Mtime:   baseFileRow.Mtime,
				RootIno: baseFileRow.RootIno,
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(1), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("常规文件读写模式正常打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      2,
				Mode:    baseFileRow.Mode,
				Size:    baseFileRow.Size,
				Mtime:   baseFileRow.Mtime,
				RootIno: baseFileRow.RootIno,
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(2), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	for _, spec := range []struct {
		name string
		mode uint32
		rdev uint32
	}{
		{name: "FIFO 命名管道", mode: meta.S_IFIFO | 0o644, rdev: 0},
		{name: "字符设备文件", mode: meta.S_IFCHR | 0o660, rdev: 136},
		{name: "块设备文件", mode: meta.S_IFBLK | 0o660, rdev: 8},
		{name: "UNIX 本地域套接字", mode: meta.S_IFSOCK | 0o777, rdev: 0},
	} {
		t.Run("特殊设备文件正常打开: "+spec.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			mockStore.EXPECT().
				OpenFileFast(mock.Anything, db.OpenFileFastParams{
					Ino:   int64(ino),
					Flags: int32(meta.O_RDWR),
				}).
				Return(db.OpenFileFastRow{
					Status:  "ok",
					Fh:      42,
					Mode:    int32(spec.mode),
					Size:    baseFileRow.Size,
					Mtime:   baseFileRow.Mtime,
					RootIno: baseFileRow.RootIno,
				}, nil).
				Once()

			res, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   ino,
				Flags: meta.O_RDWR,
			})

			require.NoError(t, err)
			require.NotNil(t, res)
			assert.Equal(t, uint64(42), res.Fh)
			assert.Equal(t, uint32(0), res.OpenFlags)
		})
	}

	t.Run("多次打开同一 Inode 分配不同 fh", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		// 第一次打开
		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      10,
				Mode:    baseFileRow.Mode,
				Size:    baseFileRow.Size,
				Mtime:   baseFileRow.Mtime,
				RootIno: baseFileRow.RootIno,
			}, nil).
			Once()

		res1, err1 := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err1)
		assert.Equal(t, uint64(10), res1.Fh)

		// 第二次打开
		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      11,
				Mode:    baseFileRow.Mode,
				Size:    baseFileRow.Size,
				Mtime:   baseFileRow.Mtime,
				RootIno: baseFileRow.RootIno,
			}, nil).
			Once()

		res2, err2 := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err2)
		assert.Equal(t, uint64(11), res2.Fh)
		assert.NotEqual(t, res1.Fh, res2.Fh)
	})
}

func TestOpen_InputValidation(t *testing.T) {
	ctx := context.Background()

	t.Run("ino 为 0 时直接返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   0,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("ino 超过 math.MaxInt64 时直接返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   uint64(math.MaxInt64) + 1,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	for _, tc := range []struct {
		name  string
		flags uint32
	}{
		{name: "flags 仅携带 O_TMPFILE", flags: meta.O_TMPFILE},
		{name: "flags 携带 O_TMPFILE | O_RDONLY", flags: meta.O_TMPFILE | meta.O_RDONLY},
		{name: "flags 携带 O_TMPFILE | O_WRONLY", flags: meta.O_TMPFILE | meta.O_WRONLY},
		{name: "flags 携带 O_TMPFILE | O_RDWR", flags: meta.O_TMPFILE | meta.O_RDWR},
		{name: "flags 携带 O_TMPFILE | O_EXCL | O_RDWR", flags: meta.O_TMPFILE | meta.O_EXCL | meta.O_RDWR},
	} {
		t.Run("flags 伪造 O_TMPFILE 标志位时静态入参校验直接拒绝: "+tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			// 零 Mock 预期设置：静态入参校验严禁触发任何数据库读写算子，不分配新 FileHandle
			res, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   100,
				Flags: tc.flags,
			})

			assert.Nil(t, res)
			require.Error(t, err)
			assert.ErrorIs(t, err, meta.ErrInvalidArgument)

			// 验证错误契约映射为 EINVAL
			st := meta.ToStatus(err)
			require.NotNil(t, st)
			require.NotEmpty(t, st.Details())
			info, ok := st.Details()[0].(*errdetails.ErrorInfo)
			require.True(t, ok)
			assert.Equal(t, "EINVAL", info.Reason)
			assert.Equal(t, meta.ErrorDomain, info.Domain)
		})
	}
}

func TestOpen_TypeChecking(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)

	t.Run("目标为目录 (S_IFDIR) 时返回 ErrIsDir (POSIX EISDIR)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status: "is_dir",
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrIsDir)
	})

	t.Run("目标为符号链接 (S_IFLNK) 时返回 ErrInvalidArgument (POSIX EINVAL)", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status: "is_symlink",
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})
}

func TestOpen_NotFoundAndErrors(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)

	t.Run("目标节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status: "not_found",
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("底层查询 Inode 发生非 NoRows 数据库错误时透传错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		dbErr := errors.New("database connection lost")
		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{}, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})
}

func TestOpen_OrphanInodeAdmission(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	orphanRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      0, // 已解链孤儿节点
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("死孤儿节点 (nlink == 0 且无活跃句柄) 禁止打开并返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status: "not_found",
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("活跃孤儿节点 (nlink == 0 且存在活跃句柄) 准入通过并成功分配句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      88,
				Mode:    orphanRow.Mode,
				Size:    orphanRow.Size,
				Mtime:   orphanRow.Mtime,
				RootIno: orphanRow.RootIno,
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(88), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("O_TRUNC 模式下查询孤儿节点活跃句柄失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanRow, nil).
			Once()

		dbErr := errors.New("query has_file_handles failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})
}

func TestOpen_OTrunc(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | meta.S_ISUID | meta.S_ISGID | 0o755),
		Size:       4096,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now.Add(-time.Hour), Valid: true},
	}

	t.Run("只读模式携带 O_TRUNC 返回 ErrInvalidArgument", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDONLY | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrInvalidArgument)
	})

	t.Run("常规文件在 O_WRONLY|O_TRUNC 时原子截断并剥离 S_ISUID/S_ISGID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		// 剥离 S_ISUID 与 S_ISGID 后的模式
		expectedTruncatedMode := int32(meta.S_IFREG | 0o755)

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        expectedTruncatedMode,
				Size:        0,
				Uid:         baseFileRow.Uid,
				Gid:         baseFileRow.Gid,
				SetAtimeNow: false,
				Atime:       baseFileRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(db.Inode{
				Ino:        int64(ino),
				Generation: 1,
				Mode:       expectedTruncatedMode,
				Size:       0,
				Nlink:      1,
				Uid:        1000,
				Gid:        1000,
				Rdev:       0,
				Atime:      baseFileRow.Atime,
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        99,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(99), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("常规文件在 O_RDWR|O_TRUNC 时原子截断", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		normalRegRow := baseFileRow
		normalRegRow.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(normalRegRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, db.UpdateInodeAttrParams{
				Ino:         int64(ino),
				Mode:        int32(meta.S_IFREG | 0o644),
				Size:        0,
				Uid:         normalRegRow.Uid,
				Gid:         normalRegRow.Gid,
				SetAtimeNow: false,
				Atime:       normalRegRow.Atime,
				SetMtimeNow: true,
				Mtime:       pgtype.Timestamptz{},
			}).
			Return(normalRegRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        100,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(100), res.Fh)
	})

	t.Run("特殊文件 (FIFO) 携带 O_TRUNC 时忽略截断正常分配句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fifoRow := baseFileRow
		fifoRow.Mode = int32(meta.S_IFIFO | 0o644)
		fifoRow.Size = 0

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fifoRow, nil).
			Once()

		// 特殊文件不调用 UpdateInodeAttr，直接调用 CreateFileHandle
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        101,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(101), res.Fh)
	})

	t.Run("截断更新属性失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		dbErr := errors.New("update inode attr failed")
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.Anything).
			Return(db.Inode{}, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("创建句柄失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.Anything).
			Return(baseFileRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		dbErr := errors.New("insert file handle failed")
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, mock.Anything).
			Return(db.FileHandle{}, dbErr).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})

		assert.Nil(t, res)
		require.Error(t, err)
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("常规文件在携带 O_TRUNC 打开且 RootIno>0, Size>0 时同一事务内联动扣减 used_bytes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootIno := int64(1)
		fileSize := int64(4096)
		fileRow := baseFileRow
		fileRow.RootIno = rootIno
		fileRow.Size = fileSize

		diskUUID := pgtype.UUID{Bytes: [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}, Valid: true}
		diskState := db.GetDiskRow{
			DiskID:     diskUUID,
			Name:       "test-disk",
			RootIno:    rootIno,
			MaxBytes:   10 * 1024 * 1024,
			UsedBytes:  fileSize,
			UsedInodes: 1,
			CreatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
			UpdatedAt:  pgtype.Timestamptz{Time: now, Valid: true},
		}

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        201,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_RDWR | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(201), res.Fh)

		// 验证通过 GetDisk 观察到 used_bytes 准确扣减归零
		mockStore.EXPECT().
			GetDisk(mock.Anything, diskUUID).
			RunAndReturn(func(ctx context.Context, id pgtype.UUID) (db.GetDiskRow, error) {
				return diskState, nil
			}).
			Once()

		disk, err := mgr.GetDisk(ctx, "01020304-0506-0708-090a-0b0c0d0e0f10")
		require.NoError(t, err)
		assert.Equal(t, uint64(0), disk.UsedBytes)

		// 验证通过 StatFS 观察到 used_bytes 准确扣减归零（Bfree/Bavail 增加）
		mockStore.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockStore.EXPECT().
			GetDiskByRootIno(mock.Anything, rootIno).
			RunAndReturn(func(ctx context.Context, rIno int64) (db.GetDiskByRootInoRow, error) {
				return db.GetDiskByRootInoRow(diskState), nil
			}).
			Once()

		statRes, err := mgr.StatFS(ctx, ino)
		require.NoError(t, err)
		assert.Equal(t, uint64(diskState.MaxBytes)/uint64(meta.DefaultStatFSBlockSize), statRes.Bfree)
	})

	t.Run("常规文件在携带 O_WRONLY|O_TRUNC 打开且 RootIno>0, Size>0 时联动扣减 used_bytes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootIno := int64(2)
		fileSize := int64(8192)
		fileRow := baseFileRow
		fileRow.RootIno = rootIno
		fileRow.Size = fileSize

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        202,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(202), res.Fh)
	})

	t.Run("常规文件 Size 为 0 且携带 O_TRUNC 打开时正常截断", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		rootIno := int64(1)
		fileRow := baseFileRow
		fileRow.RootIno = rootIno
		fileRow.Size = 0

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(fileRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_WRONLY | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        203,
				Ino:       int64(ino),
				Flags:     int32(meta.O_WRONLY | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Open(ctx, meta.OpenParams{
			Ino:   ino,
			Flags: meta.O_WRONLY | meta.O_TRUNC,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(203), res.Fh)
	})
}

func TestOpen_CreateFileHandlePgErrors(t *testing.T) {
	ctx := context.Background()
	ino := uint64(100)

	for _, tc := range []struct {
		name           string
		createErr      error
		expectNotFound bool
	}{
		{
			name: "直接返回 23503 外键约束冲突映射为 ErrNotFound",
			createErr: &pgconn.PgError{
				Code:    "23503",
				Message: "insert or update on table \"file_handles\" violates foreign key constraint",
			},
			expectNotFound: true,
		},
		{
			name: "包装后的 wrapped 23503 外键冲突同样映射为 ErrNotFound",
			createErr: fmt.Errorf("wrapped fk error: %w", &pgconn.PgError{
				Code:    "23503",
				Message: "foreign key violation",
			}),
			expectNotFound: true,
		},
		{
			name: "非 23503 错误 (23505 唯一键冲突) 负面边界对照不得映射为 ErrNotFound",
			createErr: &pgconn.PgError{
				Code:    "23505",
				Message: "duplicate key value violates unique constraint",
			},
			expectNotFound: false,
		},
		{
			name: "非 23503 错误 (08006 连接断开) 负面边界对照不得映射为 ErrNotFound",
			createErr: &pgconn.PgError{
				Code:    "08006",
				Message: "connection failure",
			},
			expectNotFound: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			mockStore.EXPECT().
				OpenFileFast(mock.Anything, db.OpenFileFastParams{
					Ino:   int64(ino),
					Flags: int32(meta.O_RDONLY),
				}).
				Return(db.OpenFileFastRow{}, tc.createErr).
				Once()

			res, err := mgr.Open(ctx, meta.OpenParams{
				Ino:   ino,
				Flags: meta.O_RDONLY,
			})

			assert.Nil(t, res)
			require.Error(t, err)

			if tc.expectNotFound {
				require.ErrorIs(t, err, meta.ErrNotFound)

				st := meta.ToStatus(err)
				require.NotNil(t, st)
				assert.Equal(t, codes.NotFound, st.Code())
				require.NotEmpty(t, st.Details())
				info, ok := st.Details()[0].(*errdetails.ErrorInfo)
				require.True(t, ok)
				assert.Equal(t, meta.ErrorDomain, info.Domain)
				assert.Equal(t, "ENOENT", info.Reason)
			} else {
				assert.False(t, errors.Is(err, meta.ErrNotFound), "非 23503 错误严禁误映射为 ErrNotFound")

				st := meta.ToStatus(err)
				require.NotNil(t, st)
				assert.NotEqual(t, codes.NotFound, st.Code(), "非 23503 错误状态码严禁映射为 NotFound")
				// 契约锚定：未匹配 24 种领域哨兵的底层未知错误按 status.go 既有契约映射为 Unknown
				assert.Equal(t, codes.Unknown, st.Code())
				assert.Empty(t, st.Details(), "底层未映射数据库错误不应携带 ErrorInfo 详情")
			}
		})
	}
}

func TestOpen_SmartKeepCache_Unit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseFileRow := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFREG | 0o644),
		Size:       1024,
		Nlink:      1,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("默认配置下首次 Open 返回 0，未修改再次 Open 返回 FOPEN_KEEP_CACHE，mtime/size 变更或 O_TRUNC 后返回 0 再恢复", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		expectOpen := func(row db.Inode, flags uint32, fh int64) {
			mockStore.EXPECT().
				OpenFileFast(mock.Anything, db.OpenFileFastParams{
					Ino:   int64(ino),
					Flags: int32(flags),
				}).
				Return(db.OpenFileFastRow{
					Status:  "ok",
					Fh:      fh,
					Mode:    row.Mode,
					Size:    row.Size,
					Mtime:   row.Mtime,
					RootIno: row.RootIno,
				}, nil).
				Once()
		}

		// 1. 首次 Open -> 返回 0
		expectOpen(baseFileRow, meta.O_RDONLY, 1)
		res1, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res1.OpenFlags)

		// 2. 未修改第二次 Open -> 返回 FOPEN_KEEP_CACHE
		expectOpen(baseFileRow, meta.O_RDONLY, 2)
		res2, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res2.OpenFlags)

		// 3. 未修改第三次 Open -> 持续返回 FOPEN_KEEP_CACHE
		expectOpen(baseFileRow, meta.O_RDWR, 3)
		res3, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res3.OpenFlags)

		// 4. 等长覆盖写仅刷新 mtime -> 返回 0
		mtimeRow := baseFileRow
		mtimeRow.Mtime = pgtype.Timestamptz{Time: now.Add(time.Millisecond), Valid: true}
		expectOpen(mtimeRow, meta.O_RDONLY, 4)
		res4, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res4.OpenFlags)

		// 5. 再次未修改 Open -> 恢复返回 FOPEN_KEEP_CACHE
		expectOpen(mtimeRow, meta.O_RDONLY, 5)
		res5, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res5.OpenFlags)

		// 6. size 发生变化 -> 返回 0
		sizeRow := mtimeRow
		sizeRow.Size = 2048
		expectOpen(sizeRow, meta.O_RDONLY, 6)
		res6, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res6.OpenFlags)

		// 7. 再次未修改 Open -> 恢复返回 FOPEN_KEEP_CACHE
		expectOpen(sizeRow, meta.O_RDONLY, 7)
		res7, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res7.OpenFlags)

		// 8. 携带 O_TRUNC 打开 -> 不设置 FOPEN_KEEP_CACHE（返回 0），并更新版本为截断后的 (mtime, size=0)
		truncatedRow := sizeRow
		truncatedRow.Size = 0
		truncatedRow.Mtime = pgtype.Timestamptz{Time: now.Add(2 * time.Millisecond), Valid: true}
		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(sizeRow, nil).
			Once()
		mockQuerier.EXPECT().
			UpdateInodeAttr(mock.Anything, mock.MatchedBy(func(p db.UpdateInodeAttrParams) bool {
				return p.Ino == int64(ino) && p.Size == 0
			})).
			Return(truncatedRow, nil).
			Once()
		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()
		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDWR | meta.O_TRUNC),
			}).
			Return(db.FileHandle{
				Fh:        8,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDWR | meta.O_TRUNC),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res8, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDWR | meta.O_TRUNC})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res8.OpenFlags)

		// 9. O_TRUNC 之后未再修改，再次普通 Open -> 因 O_TRUNC 已记录截断后版本，本次直接命中 FOPEN_KEEP_CACHE
		expectOpen(truncatedRow, meta.O_RDONLY, 9)
		res9, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res9.OpenFlags)
	})

	t.Run("单客户端 Create -> Write -> Open 版本追踪命中 FOPEN_KEEP_CACHE", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore, meta.WithBlockStore(meta.NewLocalFSBlockStore(t.TempDir())))

		sessID := uint64(5001)
		pgSid := pgtype.Int8{Int64: int64(sessID), Valid: true}
		createTime := now
		writeTime := now.Add(10 * time.Millisecond)

		// 1. Create 记录初始版本 (createTime, size=0)
		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, mock.MatchedBy(func(p db.CreateFileAtomicParams) bool {
				return p.ParentIno == 1 && string(p.Name) == "new.txt" && p.SessionID == pgSid
			})).
			Return(db.CreateFileAtomicRow{
				Status: "ok",
				Ino:    int64(ino),
				Mode:   int32(meta.S_IFREG | 0o644),
				Size:   0,
				Nlink:  1,
				Mtime:  pgtype.Timestamptz{Time: createTime, Valid: true},
				Ctime:  pgtype.Timestamptz{Time: createTime, Valid: true},
				Atime:  pgtype.Timestamptz{Time: createTime, Valid: true},
				Fh:     10,
			}, nil).
			Once()

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("new.txt"),
			Mode:      0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), createRes.OpenFlags)

		// 2. Write 更新本会话已知版本为 (writeTime, size=5)
		mockStore.EXPECT().
			NextSliceID(mock.Anything).
			Return(int64(1), nil).
			Once()
		mockStore.EXPECT().
			WriteFileAtomic(mock.Anything, mock.MatchedBy(func(p db.WriteFileAtomicParams) bool {
				return p.Ino == int64(ino) && p.Fh == 10 && p.SessionID == pgSid && p.DataLen == 5
			})).
			Return(db.WriteFileAtomicRow{
				Status:  "ok",
				Ino:     int64(ino),
				Mode:    int32(meta.S_IFREG | 0o644),
				OldSize: 0,
				NewSize: 5,
				Nlink:   1,
				Mtime:   pgtype.Timestamptz{Time: writeTime, Valid: true},
				Ctime:   pgtype.Timestamptz{Time: writeTime, Valid: true},
				Atime:   pgtype.Timestamptz{Time: createTime, Valid: true},
			}, nil).
			Once()

		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:       ino,
			Fh:        10,
			Offset:    0,
			Data:      []byte("hello"),
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 3. 同一客户端 SessionID 再次 Open，因 Write 已记录最新 (writeTime, size=5)，直接返回 FOPEN_KEEP_CACHE
		writtenRow := baseFileRow
		writtenRow.Size = 5
		writtenRow.Mtime = pgtype.Timestamptz{Time: writeTime, Valid: true}

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				SessionID: pgSid,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      11,
				Mode:    writtenRow.Mode,
				Size:    writtenRow.Size,
				Mtime:   writtenRow.Mtime,
				RootIno: writtenRow.RootIno,
			}, nil).
			Once()

		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, openRes.OpenFlags)
	})

	t.Run("多客户端 SessionID 交错打开隔离：客户端 B 修改并先行 Open 后，客户端 A 随后 Open 仍返回 0", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		sessA := uint64(2001)
		sessB := uint64(2002)

		expectSessionOpen := func(row db.Inode, fh int64, sessID uint64) {
			pgSid := pgtype.Int8{Int64: int64(sessID), Valid: true}
			mockStore.EXPECT().
				OpenFileFast(mock.Anything, db.OpenFileFastParams{
					SessionID: pgSid,
					Ino:       int64(ino),
					Flags:     int32(meta.O_RDONLY),
				}).
				Return(db.OpenFileFastRow{
					Status:  "ok",
					Fh:      fh,
					Mode:    row.Mode,
					Size:    row.Size,
					Mtime:   row.Mtime,
					RootIno: row.RootIno,
				}, nil).
				Once()
		}

		// 客户端 A 连续两次 Open -> 0, FOPEN_KEEP_CACHE
		expectSessionOpen(baseFileRow, 10, sessA)
		resA1, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resA1.OpenFlags)

		expectSessionOpen(baseFileRow, 11, sessA)
		resA2, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, resA2.OpenFlags)

		// 客户端 B 连续两次 Open -> 0, FOPEN_KEEP_CACHE
		expectSessionOpen(baseFileRow, 20, sessB)
		resB1, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resB1.OpenFlags)

		expectSessionOpen(baseFileRow, 21, sessB)
		resB2, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, resB2.OpenFlags)

		// 文件被修改（mtime 推进），客户端 B 先行连续两次 Open -> 0, FOPEN_KEEP_CACHE
		mutatedRow := baseFileRow
		mutatedRow.Mtime = pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true}

		expectSessionOpen(mutatedRow, 22, sessB)
		resB3, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resB3.OpenFlags)

		expectSessionOpen(mutatedRow, 23, sessB)
		resB4, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, resB4.OpenFlags)

		// 客户端 A 随后 Open：必须返回 0（绝不被客户端 B 的 Open 掩盖变更），再次 Open 恢复返回 FOPEN_KEEP_CACHE
		expectSessionOpen(mutatedRow, 12, sessA)
		resA3, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resA3.OpenFlags)

		expectSessionOpen(mutatedRow, 13, sessA)
		resA4, err := mgr.Open(ctx, meta.OpenParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, resA4.OpenFlags)
	})
}

func TestOpen_PostgresIntegration_SmartKeepCache(t *testing.T) {
	pool, testCtx := setupMetaTestDB(t)
	if pool == nil {
		return
	}

	store := db.NewStore(pool)
	mgr := meta.New(store, meta.WithBlockStore(meta.NewLocalFSBlockStore(t.TempDir())))

	disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-open-keep-cache-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	sessA := uint64(time.Now().UnixNano() & 0x3FFFFFFF)
	sessB := sessA + 100
	_, err = mgr.RenewSessionWithDisk(testCtx, sessA, &disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.RenewSessionWithDisk(testCtx, sessB, &disk.DiskID)
	require.NoError(t, err)

	// 1. 验证单客户端 Create -> Write -> Release -> Open 返回 FOPEN_KEEP_CACHE
	createRes, err := mgr.Create(testCtx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("file1.txt"),
		Mode:      0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), createRes.OpenFlags)
	fileIno := createRes.Inode.Ino

	_, err = mgr.Write(testCtx, &meta.WriteRequest{
		Ino:       fileIno,
		Fh:        createRes.Fh,
		Offset:    0,
		Data:      []byte("hello-world"),
		SessionID: &sessA,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
		Ino:       fileIno,
		Fh:        createRes.Fh,
		SessionID: &sessA,
	}))

	openAndClose := func(sessID uint64, flags uint32) uint32 {
		res, openErr := mgr.Open(testCtx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     flags,
			SessionID: &sessID,
		})
		require.NoError(t, openErr)
		require.NotNil(t, res)
		require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        res.Fh,
			SessionID: &sessID,
		}))
		return res.OpenFlags
	}

	assert.Equal(t, meta.FOPEN_KEEP_CACHE, openAndClose(sessA, meta.O_RDONLY), "单客户端 Create -> Write -> Release -> Open 应返回 FOPEN_KEEP_CACHE")
	assert.Equal(t, meta.FOPEN_KEEP_CACHE, openAndClose(sessA, meta.O_RDONLY), "未修改再次 Open 应持续返回 FOPEN_KEEP_CACHE")

	// 2. 验证默认配置下客户端 B 首次 Open 既有文件返回 0，未修改再次 Open 返回 FOPEN_KEEP_CACHE
	assert.Equal(t, uint32(0), openAndClose(sessB, meta.O_RDONLY), "客户端 B 首次 Open 既有文件应返回 0")
	assert.Equal(t, meta.FOPEN_KEEP_CACHE, openAndClose(sessB, meta.O_RDONLY), "客户端 B 未修改再次 Open 应返回 FOPEN_KEEP_CACHE")

	// 辅助断言：客户端 B 修改文件并由客户端 B 先行 Open 后，客户端 A 随后 Open 依然返回 0，再下一次未修改重开恢复返回 FOPEN_KEEP_CACHE
	assertClientBMutationInvalidatesClientA := func(step string, mutateByB func(), expectBFirstOpenFlags uint32) {
		t.Helper()
		mutateByB()

		assert.Equal(t, expectBFirstOpenFlags, openAndClose(sessB, meta.O_RDONLY), "%s: 客户端 B 先行 Open 校验", step)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, openAndClose(sessB, meta.O_RDONLY), "%s: 客户端 B 次轮 Open 应返回 FOPEN_KEEP_CACHE", step)

		assert.Equal(t, uint32(0), openAndClose(sessA, meta.O_RDONLY), "%s: 客户端 B 修改并先行 Open 后，客户端 A 随后 Open 必返回 0", step)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, openAndClose(sessA, meta.O_RDONLY), "%s: 客户端 A 再次未修改 Open 恢复返回 FOPEN_KEEP_CACHE", step)
	}

	// 3. 客户端 B 等长覆盖写（刷新 mtime，size 不变）
	// 客户端 B 自己通过 Write 更新了自身的已知版本，故客户端 B 随后 Open 命中 FOPEN_KEEP_CACHE；而客户端 A 的已知版本仍为旧 mtime，必返回 0
	assertClientBMutationInvalidatesClientA("客户端 B 等长覆盖写", func() {
		bOpen, oErr := mgr.Open(testCtx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, oErr)
		_, wErr := mgr.Write(testCtx, &meta.WriteRequest{
			Ino:       fileIno,
			Fh:        bOpen.Fh,
			Offset:    0,
			Data:      []byte("HELLO-WORLD"), // 同样 11 字节，等长覆盖
			SessionID: &sessB,
		})
		require.NoError(t, wErr)
		require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        bOpen.Fh,
			SessionID: &sessB,
		}))
	}, meta.FOPEN_KEEP_CACHE)

	// 4. 客户端 B 扩容写（刷新 mtime 与 size）
	assertClientBMutationInvalidatesClientA("客户端 B 扩容写", func() {
		bOpen, oErr := mgr.Open(testCtx, meta.OpenParams{
			Ino:       fileIno,
			Flags:     meta.O_RDWR,
			SessionID: &sessB,
		})
		require.NoError(t, oErr)
		_, wErr := mgr.Write(testCtx, &meta.WriteRequest{
			Ino:       fileIno,
			Fh:        bOpen.Fh,
			Offset:    11,
			Data:      []byte("-extended"),
			SessionID: &sessB,
		})
		require.NoError(t, wErr)
		require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
			Ino:       fileIno,
			Fh:        bOpen.Fh,
			SessionID: &sessB,
		}))
	}, meta.FOPEN_KEEP_CACHE)

	// 5. SetAttr 截断 size（刷新 mtime 与 size） -> 客户端 B 与客户端 A 在 SetAttr 后的首次 Open 均返回 0
	assertClientBMutationInvalidatesClientA("SetAttr 截断文件", func() {
		newSize := uint64(5)
		_, sErr := mgr.SetAttr(testCtx, fileIno, meta.SetAttrInput{
			Size:      &newSize,
			SessionID: &sessB,
		})
		require.NoError(t, sErr)
	}, uint32(0))

	// 6. SetAttr 显式修改 mtime -> 客户端 B 与客户端 A 在 SetAttr 后的首次 Open 均返回 0
	assertClientBMutationInvalidatesClientA("SetAttr 修改 mtime", func() {
		newMtime := time.Now().Add(10 * time.Second).Truncate(time.Microsecond)
		_, sErr := mgr.SetAttr(testCtx, fileIno, meta.SetAttrInput{
			Mtime:     &newMtime,
			SessionID: &sessB,
		})
		require.NoError(t, sErr)
	}, uint32(0))

	// 7. 客户端 B 执行 Open(O_TRUNC)：本次 Open(O_TRUNC) 自身返回 0 并记录客户端 B 截断后版本；
	// 客户端 B 紧接着普通 Open 返回 FOPEN_KEEP_CACHE；客户端 A 随后 Open 返回 0，再次 Open 恢复返回 FOPEN_KEEP_CACHE
	assertClientBMutationInvalidatesClientA("客户端 B Open(O_TRUNC)", func() {
		truncFlags := openAndClose(sessB, meta.O_RDWR|meta.O_TRUNC)
		assert.Equal(t, uint32(0), truncFlags, "Open(O_TRUNC) 本身必须返回 OpenFlags == 0")
	}, meta.FOPEN_KEEP_CACHE)
}

type openFastQueryTracer struct {
	mu      sync.Mutex
	queries []string
}

func (t *openFastQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	sql := strings.TrimSpace(data.SQL)
	if sql != "" {
		t.mu.Lock()
		t.queries = append(t.queries, sql)
		t.mu.Unlock()
	}
	return ctx
}

func (t *openFastQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *openFastQueryTracer) Reset() {
	t.mu.Lock()
	t.queries = nil
	t.mu.Unlock()
}

func (t *openFastQueryTracer) Snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.queries))
	copy(out, t.queries)
	return out
}

func TestOpen_PostgresIntegration_SingleStatementCTEAndOrphanConcurrency(t *testing.T) {
	tracer := &openFastQueryTracer{}
	tracedPool, testCtx := setupMetaTestDBWithTracer(t, tracer)
	if tracedPool == nil {
		return
	}
	store := db.NewStore(tracedPool)
	mgr := meta.New(store, meta.WithBlockStore(meta.NewLocalFSBlockStore(t.TempDir())))

	disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-open-fast-cte-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	otherDisk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-open-fast-other-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.DeleteDisk(cleanupCtx, otherDisk.DiskID)
	})

	sessID := uint64(time.Now().UnixNano() & 0x3FFFFFFF)
	_, err = mgr.RenewSessionWithDisk(testCtx, sessID, &disk.DiskID)
	require.NoError(t, err)

	otherDiskSessID := sessID + 1
	_, err = mgr.RenewSessionWithDisk(testCtx, otherDiskSessID, &otherDisk.DiskID)
	require.NoError(t, err)

	createRes, err := mgr.Create(testCtx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("fast_open.txt"),
		Mode:      0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
		Ino:       createRes.Inode.Ino,
		Fh:        createRes.Fh,
		SessionID: &sessID,
	}))
	fileIno := createRes.Inode.Ino

	// 1. 验证非 O_TRUNC Open 正常路径（带 SessionID 与不带 SessionID）仅触发 1 条 SQL，且无 BEGIN/COMMIT/独立 SET LOCAL，不含 FOR SHARE / FOR UPDATE
	for _, tc := range []struct {
		name string
		sid  *uint64
	}{
		{name: "带有效 SessionID", sid: &sessID},
		{name: "不带 SessionID", sid: nil},
	} {
		t.Run("单语句 CTE 打开验证_"+tc.name, func(t *testing.T) {
			tracer.Reset()
			openRes, openErr := mgr.Open(testCtx, meta.OpenParams{
				Ino:       fileIno,
				Flags:     meta.O_RDONLY,
				SessionID: tc.sid,
			})
			require.NoError(t, openErr)
			require.NotNil(t, openRes)

			queries := tracer.Snapshot()
			require.Len(t, queries, 1, "非 O_TRUNC Open 正常路径必须仅触发 1 条单语句 CTE SQL，实际: %v", queries)
			sqlText := strings.ToUpper(queries[0])
			assert.Contains(t, queries[0], "SELECT set_config('synchronous_commit', 'off', true)", "单语句 CTE 必须内联异步 WAL 提交设置")
			assert.Contains(t, sqlText, "INSERT INTO FILE_HANDLES", "单语句 CTE 必须原子插入 file_handles")
			assert.NotContains(t, sqlText, "FOR SHARE", "OpenFileFast 不得显式对 inodes 加 FOR SHARE 锁（依赖外键 FOR KEY SHARE）")
			assert.NotContains(t, sqlText, "FOR UPDATE", "OpenFileFast 不得对 inodes 加 FOR UPDATE 锁")
			for _, q := range queries {
				upper := strings.ToUpper(strings.TrimSpace(q))
				assert.False(t, strings.HasPrefix(upper, "BEGIN"), "不应开启显式事务 BEGIN: %s", q)
				assert.False(t, strings.HasPrefix(upper, "COMMIT"), "不应存在显式事务 COMMIT: %s", q)
				assert.False(t, strings.HasPrefix(upper, "SET LOCAL"), "不应存在独立 SET LOCAL 语句: %s", q)
			}

			require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
				Ino:       fileIno,
				Fh:        openRes.Fh,
				SessionID: tc.sid,
			}))
		})
	}

	// 2. 验证真实 PG 下的错误契约（ENOENT, EISDIR, EINVAL 符号链接, EBADF 未知/过期/跨 Disk 会话）且均只执行 1 条 SQL
	subDir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("subdir_for_open"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	symNode, err := mgr.Symlink(testCtx, meta.SymlinkParams{
		ParentIno: disk.RootIno,
		Name:      []byte("symlink_for_open"),
		Target:    []byte("fast_open.txt"),
	})
	require.NoError(t, err)

	unknownSessID := sessID + 999999
	for _, errTC := range []struct {
		name      string
		params    meta.OpenParams
		expectErr error
	}{
		{
			name:      "目标不存在返回 ErrNotFound (ENOENT)",
			params:    meta.OpenParams{Ino: 999999999, Flags: meta.O_RDONLY, SessionID: &sessID},
			expectErr: meta.ErrNotFound,
		},
		{
			name:      "目标为目录返回 ErrIsDir (EISDIR)",
			params:    meta.OpenParams{Ino: subDir.Ino, Flags: meta.O_RDONLY, SessionID: &sessID},
			expectErr: meta.ErrIsDir,
		},
		{
			name:      "目标为符号链接返回 ErrInvalidArgument (EINVAL)",
			params:    meta.OpenParams{Ino: symNode.Ino, Flags: meta.O_RDONLY, SessionID: &sessID},
			expectErr: meta.ErrInvalidArgument,
		},
		{
			name:      "未知会话返回 ErrBadFileHandle (EBADF)",
			params:    meta.OpenParams{Ino: fileIno, Flags: meta.O_RDONLY, SessionID: &unknownSessID},
			expectErr: meta.ErrBadFileHandle,
		},
		{
			name:      "跨 Disk 会话返回 ErrBadFileHandle (EBADF)",
			params:    meta.OpenParams{Ino: fileIno, Flags: meta.O_RDONLY, SessionID: &otherDiskSessID},
			expectErr: meta.ErrBadFileHandle,
		},
	} {
		t.Run("错误契约与单语句验证_"+errTC.name, func(t *testing.T) {
			tracer.Reset()
			res, oErr := mgr.Open(testCtx, errTC.params)
			assert.Nil(t, res)
			require.ErrorIs(t, oErr, errTC.expectErr)
			assert.Len(t, tracer.Snapshot(), 1, "错误路径也必须仅通过 1 条单语句 CTE 完成判定")
		})
	}

	// 3. 验证 nlink == 0 但仍有存活句柄的 Open-Unlink 孤儿文件可正常重新 Open，且最后句柄关闭后物理回收、再次 Open 返回 ErrNotFound
	orphanCreate, err := mgr.Create(testCtx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("orphan_reopen.txt"),
		Mode:      0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	orphanIno := orphanCreate.Inode.Ino
	require.NoError(t, mgr.Unlink(testCtx, disk.RootIno, []byte("orphan_reopen.txt")))

	// 孤儿文件存在存活句柄 orphanCreate.Fh 时，单语句 Open 成功
	tracer.Reset()
	reopenRes, err := mgr.Open(testCtx, meta.OpenParams{
		Ino:       orphanIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessID,
	})
	require.NoError(t, err)
	require.NotNil(t, reopenRes)
	assert.Len(t, tracer.Snapshot(), 1)

	// 关闭全部 2 个句柄后，孤儿节点物理删除，再次 Open 返回 ErrNotFound
	require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
		Ino:       orphanIno,
		Fh:        reopenRes.Fh,
		SessionID: &sessID,
	}))
	require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
		Ino:       orphanIno,
		Fh:        orphanCreate.Fh,
		SessionID: &sessID,
	}))

	afterReclaimRes, err := mgr.Open(testCtx, meta.OpenParams{
		Ino:       orphanIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessID,
	})
	assert.Nil(t, afterReclaimRes)
	require.ErrorIs(t, err, meta.ErrNotFound)

	// 4. 验证孤儿节点在并发最后句柄 Release 回收交错下，Open 要么成功（阻止回收）要么返回 ErrNotFound（包含 23503 外键冲突兜底），绝不产生残留句柄或未映射错误
	for round := 0; round < 5; round++ {
		raceCreate, cErr := mgr.Create(testCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("orphan_race_%d.txt", round)),
			Mode:      0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, cErr)
		raceIno := raceCreate.Inode.Ino
		require.NoError(t, mgr.Unlink(testCtx, disk.RootIno, []byte(fmt.Sprintf("orphan_race_%d.txt", round))))

		startCh := make(chan struct{})
		var wg sync.WaitGroup
		var openResult *meta.OpenResult
		var openErr error
		var releaseErr error

		wg.Add(2)
		go func() {
			defer wg.Done()
			<-startCh
			openResult, openErr = mgr.Open(testCtx, meta.OpenParams{
				Ino:       raceIno,
				Flags:     meta.O_RDONLY,
				SessionID: &sessID,
			})
		}()
		go func() {
			defer wg.Done()
			<-startCh
			releaseErr = mgr.Release(testCtx, meta.ReleaseParams{
				Ino:       raceIno,
				Fh:        raceCreate.Fh,
				SessionID: &sessID,
			})
		}()
		close(startCh)
		wg.Wait()

		require.NoError(t, releaseErr)
		if openErr != nil {
			require.ErrorIs(t, openErr, meta.ErrNotFound, "并发孤儿回收时 Open 失败必须映射为 ErrNotFound")
			hasHandles, hErr := store.HasFileHandlesByIno(testCtx, int64(raceIno))
			require.NoError(t, hErr)
			assert.False(t, hasHandles, "Open 失败时严禁残留任何 file_handles 记录")
		} else {
			require.NotNil(t, openResult)
			require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
				Ino:       raceIno,
				Fh:        openResult.Fh,
				SessionID: &sessID,
			}))
		}
	}
}
