package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
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
)

func TestReleaseDir_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name string
		ino  uint64
		fh   uint64
	}{
		{
			name: "ino 为 0",
			ino:  0,
			fh:   1,
		},
		{
			name: "ino 超过 math.MaxInt64",
			ino:  uint64(math.MaxInt64) + 1,
			fh:   1,
		},
		{
			name: "fh 为 0",
			ino:  1,
			fh:   0,
		},
		{
			name: "fh 超过 math.MaxInt64",
			ino:  1,
			fh:   uint64(math.MaxInt64) + 1,
		},
		{
			name: "ino 与 fh 均为 0",
			ino:  0,
			fh:   0,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
				Ino: tc.ino,
				Fh:  tc.fh,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}
}

func TestReleaseDir_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseDir := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("正常目录释放 (nlink > 0): 走 ReleaseDirFast 单语句 CTE 销毁句柄、清理游标且免除显式事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		// 先通过 ReadDir 建立内存游标，验证 ReleaseDirFast ok 路径清理游标
		mockStore.EXPECT().
			ExecTxWithOptions(mock.Anything, mock.Anything, mock.Anything, mock.Anything).
			RunAndReturn(func(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
				return fn(mockQuerier)
			}).
			Once()
		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDir, nil).
			Once()
		mockQuerier.EXPECT().
			GetFileHandle(mock.Anything, db.GetFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(db.FileHandle{Fh: int64(fh), Ino: int64(ino)}, nil).
			Once()
		mockQuerier.EXPECT().
			ListDirEntries(mock.Anything, db.ListDirEntriesParams{
				ParentIno: int64(ino),
				Limit:     1,
				Offset:    0,
			}).
			Return([]db.ListDirEntriesRow{
				{Name: []byte("child1"), Ino: 101, Mode: int32(meta.S_IFREG | 0o644)},
			}, nil).
			Once()

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:    ino,
			Fh:     fh,
			Offset: 2,
			Limit:  1,
		})
		require.NoError(t, err)
		require.NotEmpty(t, entries)
		require.Equal(t, 1, meta.InoDirCursorsCount(mgr, ino))

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "ok",
				Nlink:  2,
			}, nil).
			Once()

		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
		assert.Equal(t, 0, meta.InoDirCursorsCount(mgr, ino), "ReleaseDir 快速路径成功后必须清理对应目录游标")
	})

	t.Run("多句柄并发打开的 Orphan 目录 (nlink == 0): ReleaseDirFast 返回 orphan 转入慢路径，释放其一句柄保留 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan 目录 (nlink == 0): ReleaseDirFast 返回 orphan 转入慢路径物理回收 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan 目录 (nlink == 0, RootIno > 0): 物理回收 Inode 并退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0
		orphanDir.RootIno = 10

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("存在剩余活跃句柄的 Orphan 目录 (nlink == 0, RootIno > 0): 仅释放句柄，不调用 DeleteInode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0
		orphanDir.RootIno = 10

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("携带 SessionID 关闭 Orphan 目录 (nlink == 0) 的最后一个句柄：转入慢路径删除句柄并回收孤儿节点及退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		sessionID := uint64(888)
		orphanDir := baseDir
		orphanDir.Nlink = 0
		orphanDir.RootIno = 10

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       ino,
			Fh:        fh,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
	})
}

func TestReleaseDir_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseDir := db.Inode{
		Ino:        int64(ino),
		Generation: 1,
		Mode:       int32(meta.S_IFDIR | 0o755),
		Size:       4096,
		Nlink:      2,
		Uid:        1000,
		Gid:        1000,
		Rdev:       0,
		Atime:      pgtype.Timestamptz{Time: now, Valid: true},
		Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
		Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
	}

	t.Run("目标节点不存在 (status=not_found) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "not_found",
				Nlink:  0,
			}, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("ReleaseDirFast 返回 pgx.ErrNoRows 时映射为 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{}, pgx.ErrNoRows).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("目标节点为普通文件 (status=not_dir) 返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "not_dir",
				Nlink:  1,
			}, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotDir))
	})

	t.Run("句柄不存在或归属不匹配 (status=bad_handle) 返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "bad_handle",
				Nlink:  2,
			}, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("ReleaseDirFast 返回未知状态时返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "weird_status",
				Nlink:  2,
			}, nil).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "unexpected status")
	})

	t.Run("ReleaseDirFast 遇到 40001/40P01 自动重试并在成功后返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		attempts := 0
		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			RunAndReturn(func(ctx context.Context, arg db.ReleaseDirFastParams) (db.ReleaseDirFastRow, error) {
				attempts++
				if attempts == 1 {
					return db.ReleaseDirFastRow{}, &pgconn.PgError{Code: "40001"}
				}
				if attempts == 2 {
					return db.ReleaseDirFastRow{}, &pgconn.PgError{Code: "40P01"}
				}
				return db.ReleaseDirFastRow{Status: "ok", Nlink: 2}, nil
			}).
			Times(3)

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("ReleaseDirFast 底层数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		dbErr := errors.New("db error")
		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{}, dbErr).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "releasedir tx")
		assert.ErrorIs(t, err, dbErr)
	})

	t.Run("孤儿慢路径底层 GetInodeForUpdate 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(db.Inode{}, errors.New("db error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "get inode")
	})

	t.Run("孤儿慢路径底层 DeleteFileHandle 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), errors.New("db delete handle error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete file handle")
	})

	t.Run("孤儿慢路径底层 HasFileHandlesByIno 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, errors.New("db query remaining handles error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "check remaining file handles")
	})

	t.Run("孤儿慢路径底层 DeleteInode 数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanDir := baseDir
		orphanDir.Nlink = 0

		mockStore.EXPECT().
			ReleaseDirFast(mock.Anything, db.ReleaseDirFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseDirFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDir, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(errors.New("db delete inode error")).
			Once()

		err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "delete orphan inode")
	})
}

func TestReleaseDir_QuotaReclaim_PostgresIntegration(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Skipf("skipping postgres test: %v", err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Skipf("skipping postgres test (ping failed): %v", err)
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	t.Run("携带 SessionID 关闭孤儿目录的最后一个句柄：验证 UsedInodes 仅被扣减一次（无双重扣减）", func(t *testing.T) {
		diskName := fmt.Sprintf("test_releasedir_session_quota_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessID := uint64(770002)
		_, err = mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)

		// 1. 创建常驻子目录 normal_dir，提供配额基底
		_, err = mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("normal_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)

		// 2. 创建即将被 rmdir 的目录 orphan_dir
		orphanDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)

		// 打开 orphan_dir 句柄（携带 SessionID）
		openRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       orphanDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 当前配额状态：根目录 + normal_dir + orphan_dir = 3 inodes
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)

		// 3. Rmdir orphan_dir：因句柄打开，nlink 变 0 但暂不回收，配额不变
		err = mgr.Rmdir(ctx, disk.RootIno, []byte("orphan_dir"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)

		// 4. ReleaseDir 关闭句柄（携带 SessionID）
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       orphanDir.Ino,
			Fh:        openRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 5. 校验配额：必须且仅扣减 1 个 inode（剩余 2 个 inodes：根目录与 normal_dir）
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "孤儿目录释放后应恰好剩余 2 个 inode (根目录与 normal_dir)")
	})
}

func TestReleaseDir_FastPathSingleStatementAndOrphanReclaim_PostgresIntegration(t *testing.T) {
	tracer := &releaseFastQueryTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	diskName := fmt.Sprintf("test_releasedir_fast_cte_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 32 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	sessID := uint64(time.Now().UnixNano()&0x7fffffff) + 565
	_, err = mgr.RenewSession(ctx, sessID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM client_sessions WHERE session_id = $1`, int64(sessID))
	})

	t.Run("正常目录 ReleaseDir 快速路径仅触发 1 条单语句 CTE SQL 且复用连接 SHOW synchronous_commit 保持 on", func(t *testing.T) {
		cfg := pool.Config().Copy()
		cfg.MaxConns = 1
		cfg.MinConns = 1
		cfg.ConnConfig.Tracer = tracer
		singlePool, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		defer singlePool.Close()

		singleStore := db.NewStore(singlePool)
		singleMgr := meta.New(singleStore)

		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("fast_single_stmt_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)

		for _, tc := range []struct {
			name string
			sid  *uint64
		}{
			{name: "携带活跃SessionID", sid: &sessID},
			{name: "不带SessionID", sid: nil},
		} {
			t.Run(tc.name, func(t *testing.T) {
				openRes, err := singleMgr.OpenDir(ctx, meta.OpenDirParams{
					Ino:       subDir.Ino,
					Flags:     meta.O_RDONLY,
					SessionID: tc.sid,
				})
				require.NoError(t, err)

				tracer.reset()
				tracedCtx := context.WithValue(ctx, releaseFastQueryTraceKey{}, true)
				err = singleMgr.ReleaseDir(tracedCtx, meta.ReleaseDirParams{
					Ino:       subDir.Ino,
					Fh:        openRes.Fh,
					SessionID: tc.sid,
				})
				require.NoError(t, err)

				sqls := tracer.snapshot()
				require.Len(t, sqls, 1, "正常目录 ReleaseDir 快速路径必须且仅执行 1 条 SQL 语句，实际: %v", sqls)
				assert.Contains(t, sqls[0], "ReleaseDirFast")
				assert.Contains(t, sqls[0], "set_config('synchronous_commit', 'off', true)")
				for _, q := range sqls {
					upper := strings.ToUpper(strings.TrimSpace(q))
					assert.False(t, strings.HasPrefix(upper, "BEGIN"), "不应存在 BEGIN: %s", q)
					assert.False(t, strings.HasPrefix(upper, "COMMIT"), "不应存在 COMMIT: %s", q)
					assert.False(t, strings.HasPrefix(upper, "SET LOCAL"), "不应存在 SET LOCAL: %s", q)
				}

				var syncCommit string
				require.NoError(t, singlePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit))
				assert.Equal(t, "on", syncCommit, "内联 set_config(..., true) 仅在当前语句隐式事务生效，连接归还后必须保持 on")

				var handleCount int
				require.NoError(t, singlePool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE fh = $1`, int64(openRes.Fh)).Scan(&handleCount))
				assert.Zero(t, handleCount, "目录句柄应已被物理删除")
			})
		}
	})

	t.Run("Rmdir 与 Rename 覆盖解链的孤儿目录在多句柄关闭下仅最后句柄触发物理回收与配额退还", func(t *testing.T) {
		// 场景 A：Rmdir 解链持有多个打开句柄与扩展属性的目录
		rmdirOrphan, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("rmdir_multi_fh_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)

		require.NoError(t, mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   rmdirOrphan.Ino,
			Name:  []byte("user.dir.tag"),
			Value: []byte("orphan-dir-xattr"),
		}))

		fh1, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       rmdirOrphan.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		fh2, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   rmdirOrphan.Ino,
			Flags: meta.O_RDONLY,
		})
		require.NoError(t, err)

		diskBeforeRmdir, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		// Rmdir 解链目录使其变为 nlink == 0 孤儿
		require.NoError(t, mgr.Rmdir(ctx, disk.RootIno, []byte("rmdir_multi_fh_dir")))

		var nlink int32
		require.NoError(t, pool.QueryRow(ctx, `SELECT nlink FROM inodes WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&nlink))
		assert.Zero(t, nlink)

		// 关闭第一个句柄 fh1（非最后句柄）：Inode 与 xattr 仍保留，UsedInodes 不变
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       rmdirOrphan.Ino,
			Fh:        fh1.Fh,
			SessionID: &sessID,
		}))

		var inodeCount, fhCount, xattrCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inodes WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&inodeCount))
		assert.Equal(t, 1, inodeCount, "非最后目录句柄关闭时不得提前删除孤儿目录 Inode")
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&fhCount))
		assert.Equal(t, 1, fhCount)
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inode_xattrs WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&xattrCount))
		assert.Equal(t, 1, xattrCount)

		diskAfterFirstRelease, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, diskBeforeRmdir.UsedInodes, diskAfterFirstRelease.UsedInodes)

		// 关闭最后一个句柄 fh2：触发慢路径孤儿回收，inodes / file_handles / inode_xattrs 清零且 UsedInodes 减 1
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: rmdirOrphan.Ino,
			Fh:  fh2.Fh,
		}))

		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inodes WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&inodeCount))
		assert.Zero(t, inodeCount, "最后目录句柄关闭后 inodes 表不得残留")
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&fhCount))
		assert.Zero(t, fhCount, "最后目录句柄关闭后 file_handles 表不得残留")
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inode_xattrs WHERE ino = $1`, int64(rmdirOrphan.Ino)).Scan(&xattrCount))
		assert.Zero(t, xattrCount, "最后目录句柄关闭后 inode_xattrs 表不得残留")

		diskAfterLastRelease, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, diskBeforeRmdir.UsedInodes-1, diskAfterLastRelease.UsedInodes)

		// 场景 B：Rename 覆盖持有打开句柄的空目标目录使其成为孤儿，并在最后句柄 ReleaseDir 时回收
		srcDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("rename_src_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		dstDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("rename_dst_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   dstDir.Ino,
			Name:  []byte("user.rename.victim"),
			Value: []byte("victim-xattr"),
		}))

		dstOpen, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       dstDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		diskBeforeRename, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		require.NoError(t, mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: disk.RootIno,
			OldName:      []byte("rename_src_dir"),
			NewParentIno: disk.RootIno,
			NewName:      []byte("rename_dst_dir"),
		}))

		// 被覆盖的 dstDir 因持有打开句柄暂未物理删除，UsedInodes 保持不变
		require.NoError(t, pool.QueryRow(ctx, `SELECT nlink FROM inodes WHERE ino = $1`, int64(dstDir.Ino)).Scan(&nlink))
		assert.Zero(t, nlink)

		// ReleaseDir 关闭被覆盖目录的最后句柄
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dstDir.Ino,
			Fh:        dstOpen.Fh,
			SessionID: &sessID,
		}))

		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inodes WHERE ino = $1`, int64(dstDir.Ino)).Scan(&inodeCount))
		assert.Zero(t, inodeCount)
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE ino = $1`, int64(dstDir.Ino)).Scan(&fhCount))
		assert.Zero(t, fhCount)
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inode_xattrs WHERE ino = $1`, int64(dstDir.Ino)).Scan(&xattrCount))
		assert.Zero(t, xattrCount)

		diskAfterVictimRelease, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, diskBeforeRename.UsedInodes-1, diskAfterVictimRelease.UsedInodes)
		assert.NotZero(t, srcDir.Ino)
	})
}
