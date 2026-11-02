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
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestRelease_InputValidation(t *testing.T) {
	ctx := context.Background()

	zeroSession := uint64(0)
	overflowSession := uint64(math.MaxInt64) + 1

	for _, tc := range []struct {
		name         string
		ino          uint64
		fh           uint64
		releaseFlags uint32
		sessionID    *uint64
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
		{
			name:         "release_flags 包含未知位 (0x4)",
			ino:          1,
			fh:           1,
			releaseFlags: 0x4,
		},
		{
			name:         "release_flags 包含未知高位 (0x7)",
			ino:          1,
			fh:           1,
			releaseFlags: meta.FuseReleaseFlush | meta.FuseReleaseFlockUnlock | 0x4,
		},
		{
			name:      "session_id 为 0",
			ino:       1,
			fh:        1,
			sessionID: &zeroSession,
		},
		{
			name:      "session_id 超过 math.MaxInt64",
			ino:       1,
			fh:        1,
			sessionID: &overflowSession,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			err := mgr.Release(ctx, meta.ReleaseParams{
				Ino:          tc.ino,
				Fh:           tc.fh,
				ReleaseFlags: tc.releaseFlags,
				SessionID:    tc.sessionID,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
		})
	}

	t.Run("允许携带 FuseReleaseFlush (0x1) 标志位正常销毁文件句柄", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: 10,
				Fh:  1,
			}).
			Return(db.ReleaseFileFastRow{
				Status: "ok",
				Nlink:  1,
			}, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:          10,
			Fh:           1,
			ReleaseFlags: meta.FuseReleaseFlush,
		})
		require.NoError(t, err)
	})
}

func TestRelease_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseInode := db.Inode{
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

	t.Run("正常节点释放 (nlink > 0): 走 ReleaseFileFast 单语句 CTE 销毁句柄且免除显式事务", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "ok",
				Nlink:  1,
			}, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("携带 FuseReleaseFlockUnlock 但未传 session_id: 销毁句柄并按 (ino, fh) 清理 BSD flock 锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFlockFileLocksByFh(mock.Anything, db.DeleteFlockFileLocksByFhParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(int64(1), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:          ino,
			Fh:           fh,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
		})
		require.NoError(t, err)
	})

	t.Run("同时携带 FuseReleaseFlockUnlock 与 session_id: 销毁句柄、按配置的 WithSessionLeaseTTL 续期会话并按 (session_id, ino, fh) 清理 BSD flock 锁", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		customTTL := 30 * time.Second
		mgr := meta.New(mockStore, meta.WithSessionLeaseTTL(customTTL))

		setupMockWriteTx(mockStore, mockQuerier)

		sessionID := uint64(888)
		lockOwner := uint64(12345)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
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
			DeleteFlockFileLocksBySessionAndFh(mock.Anything, db.DeleteFlockFileLocksBySessionAndFhParams{
				SessionID: int64(sessionID),
				Ino:       int64(ino),
				Fh:        int64(fh),
			}).
			Return(int64(1), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:          ino,
			Fh:           fh,
			LockOwner:    &lockOwner,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
			SessionID:    &sessionID,
		})
		require.NoError(t, err)
	})

	t.Run("多句柄并发打开的 Orphan Inode (nlink == 0): 释放其中一个句柄，仍有剩余句柄时不删除 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
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

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan Inode (nlink == 0): 无剩余活跃句柄时物理回收 Inode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
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
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("最后一个句柄释放的 Orphan 常规文件 (nlink == 0, RootIno > 0, Size = 8192): 无剩余活跃句柄时物理回收 Inode 并退还配额", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 8192

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
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
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("存在剩余活跃句柄的 Orphan 常规文件 (nlink == 0, RootIno > 0): 仅释放句柄，不调用 DeleteInode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 8192

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
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

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})

	t.Run("携带 SessionID 关闭 Orphan Inode (nlink == 0) 的最后一个句柄：先续期会话再删句柄且配额仅扣减一次", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		sessionID := uint64(888)
		orphanInode := baseInode
		orphanInode.Nlink = 0
		orphanInode.RootIno = 10
		orphanInode.Size = 4096

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino:       int64(ino),
				Fh:        int64(fh),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		// 2. 删除句柄 (DeleteFileHandle)
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:        int64(fh),
				Ino:       int64(ino),
				SessionID: pgtype.Int8{Int64: int64(sessionID), Valid: true},
			}).
			Return(int64(fh), nil).
			Once()

		// 4. 最终无句柄后回收孤儿节点 (reclaimOrphanIfNoHandles)
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       ino,
			Fh:        fh,
			SessionID: &sessionID,
		})
		require.NoError(t, err)
	})
}

func TestRelease_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)
	fh := uint64(1)

	baseInode := db.Inode{
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

	t.Run("目标节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "not_found",
				Nlink:  0,
			}, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
	})

	t.Run("目标节点为目录返回 ErrIsDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "is_dir",
				Nlink:  2,
			}, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrIsDir))
	})

	t.Run("句柄不存在或归属不匹配返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "bad_handle",
				Nlink:  1,
			}, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("二次重复释放同一句柄返回 ErrBadFileHandle", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "bad_handle",
				Nlink:  1,
			}, nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrBadFileHandle))
	})

	t.Run("ReleaseFileFast 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		dbErr := errors.New("db query failed")
		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{}, dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("孤儿慢路径 DeleteFileHandle 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		dbErr := errors.New("db delete handle failed")
		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(0), dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("HasFileHandlesByIno 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		dbErr := errors.New("db check handles failed")
		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("DeleteInode 底层数据库错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		orphanInode := baseInode
		orphanInode.Nlink = 0

		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			Return(db.ReleaseFileFastRow{
				Status: "orphan",
				Nlink:  0,
			}, nil).
			Once()

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanInode, nil).
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
			DeleteChunksByIno(mock.Anything, int64(ino)).
			Return(nil, nil).
			Once()

		dbErr := errors.New("db delete inode failed")
		mockQuerier.EXPECT().
			DeleteInode(mock.Anything, int64(ino)).
			Return(dbErr).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "release tx")
		assert.True(t, errors.Is(err, dbErr))
	})

	t.Run("ReleaseFileFast 遇到 40001/40P01 自动重试并在成功后返回 nil", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		attempts := 0
		mockStore.EXPECT().
			ReleaseFileFast(mock.Anything, db.ReleaseFileFastParams{
				Ino: int64(ino),
				Fh:  int64(fh),
			}).
			RunAndReturn(func(ctx context.Context, arg db.ReleaseFileFastParams) (db.ReleaseFileFastRow, error) {
				attempts++
				if attempts == 1 {
					return db.ReleaseFileFastRow{}, &pgconn.PgError{Code: "40001"}
				}
				if attempts == 2 {
					return db.ReleaseFileFastRow{}, &pgconn.PgError{Code: "40P01"}
				}
				return db.ReleaseFileFastRow{Status: "ok", Nlink: 1}, nil
			}).
			Times(3)

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
		assert.Equal(t, 3, attempts)
	})

	t.Run("目标 Inode 有 POSIX 锁活动时回退到写事务慢路径", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		meta.RecordPosixLockActivityForTest(mgr, ino)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(baseInode, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteFileHandle(mock.Anything, db.DeleteFileHandleParams{
				Fh:  int64(fh),
				Ino: int64(ino),
			}).
			Return(int64(fh), nil).
			Once()

		err := mgr.Release(ctx, meta.ReleaseParams{
			Ino: ino,
			Fh:  fh,
		})
		require.NoError(t, err)
	})
}

func TestRelease_PostgresFlockAndOrphanCleanupSemantics(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	rootDir, err := store.CreateInode(ctx, db.CreateInodeParams{
		Mode:  int32(meta.S_IFDIR | 0o755),
		Nlink: 2,
		Uid:   1000,
		Gid:   1000,
	})
	require.NoError(t, err)
	defer func() {
		_ = store.DeleteInode(context.Background(), rootDir.Ino)
	}()

	now := time.Now()
	sess1 := uint64(930001)
	defer func() {
		_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sess1))
	}()

	_, err = store.UpsertClientSession(ctx, db.UpsertClientSessionParams{
		SessionID: int64(sess1),
		ExpiresAt: pgtype.Timestamptz{Time: now.Add(time.Minute), Valid: true},
	})
	require.NoError(t, err)

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: uint64(rootDir.Ino),
		Name:      []byte("release_lock_test.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fileIno := createRes.Inode.Ino
	fh1 := createRes.Fh
	defer func() {
		_ = store.DeleteInode(context.Background(), int64(fileIno))
	}()

	openRes2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       fileIno,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	fh2 := openRes2.Fh

	openRes3, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   fileIno,
		Flags: meta.O_RDWR,
	})
	require.NoError(t, err)
	fh3 := openRes3.Fh

	ownerA := uint64(3001)

	// 1. 通过 fh1 申请 POSIX 记录锁 [0, 500]
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh1),
		LockOwner:   int64(ownerA),
		LkFlags:     0,
		StartOffset: 0,
		EndOffset:   500,
		LockType:    int16(meta.F_WRLCK),
		Pid:         1111,
	})
	require.NoError(t, err)

	// 2. fh2 与 fh3 各自持有一把 BSD flock 共享锁
	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh2),
		LockOwner:   0,
		LkFlags:     int32(meta.FuseLkFlock),
		StartOffset: 0,
		EndOffset:   int64(meta.OffsetMax),
		LockType:    int16(meta.F_RDLCK),
		Pid:         2222,
	})
	require.NoError(t, err)

	_, err = store.CreateFileLock(ctx, db.CreateFileLockParams{
		SessionID:   int64(sess1),
		Ino:         int64(fileIno),
		Fh:          int64(fh3),
		LockOwner:   0,
		LkFlags:     int32(meta.FuseLkFlock),
		StartOffset: 0,
		EndOffset:   int64(meta.OffsetMax),
		LockType:    int16(meta.F_RDLCK),
		Pid:         3333,
	})
	require.NoError(t, err)

	// AC3: 对曾用于申请 POSIX 锁的 fh1 调用 Release（即使携带 FuseReleaseFlockUnlock）：
	// 由于无 fh 外键级联且 Release 绝不越权删 POSIX 锁，ownerA 的 POSIX 锁依然保持有效
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:          fileIno,
		Fh:           fh1,
		LockOwner:    &ownerA,
		ReleaseFlags: meta.FuseReleaseFlockUnlock,
		SessionID:    &sess1,
	})
	require.NoError(t, err)

	posixLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: 0,
	})
	require.NoError(t, err)
	require.Len(t, posixLocks, 1, "销毁 fh1 后 POSIX 锁必须依然有效")
	assert.Equal(t, int64(ownerA), posixLocks[0].LockOwner)

	// AC4: 对 fh2 调用携带 FuseReleaseFlockUnlock 的 Release：
	// 精确释放 (sess1, fileIno, fh2) 绑定的 flock 锁，不影响同文件上的 POSIX 锁或 fh3 的 flock 锁
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:          fileIno,
		Fh:           fh2,
		ReleaseFlags: meta.FuseReleaseFlockUnlock,
		SessionID:    &sess1,
	})
	require.NoError(t, err)

	flockLocks, err := store.ListActiveFileLocksByInoAndFlags(ctx, db.ListActiveFileLocksByInoAndFlagsParams{
		Ino:     int64(fileIno),
		LkFlags: int32(meta.FuseLkFlock),
	})
	require.NoError(t, err)
	require.Len(t, flockLocks, 1, "fh2 的 flock 锁应被释放，仅保留 fh3 的 flock 锁")
	assert.Equal(t, int64(fh3), flockLocks[0].Fh)

	// AC5: 将文件 Unlink 变为 Orphan Inode (nlink == 0)，随后关闭最后一个句柄 fh3：
	// 触发节点物理回收，数据库 ON DELETE CASCADE 自动清理残留的全部 POSIX 锁与 flock 锁
	err = mgr.Unlink(ctx, uint64(rootDir.Ino), []byte("release_lock_test.txt"))
	require.NoError(t, err)

	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino: fileIno,
		Fh:  fh3,
	})
	require.NoError(t, err)

	_, err = mgr.GetAttr(ctx, fileIno)
	assert.ErrorIs(t, err, meta.ErrNotFound, "孤儿节点最后一个句柄释放后 Inode 应被物理回收")

	var remainingLocks int
	err = pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_locks WHERE ino = $1", int64(fileIno)).Scan(&remainingLocks)
	require.NoError(t, err)
	assert.Zero(t, remainingLocks, "孤儿节点物理回收后级联清理所有残留文件锁")
}

func TestRelease_QuotaReclaim_PostgresIntegration(t *testing.T) {
	dsn := getStorageTestDSN()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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

	t.Run("CreateDisk -> Create -> SetAttr(size=4096) -> Unlink(有活跃句柄暂不退还) -> Release(最后一个句柄关闭时退还 used_bytes 与 used_inodes)", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_quota_file_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// 初始配额：仅包含根目录 Inode
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// Create 创建常规文件并获得初始句柄
		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("file_unlink.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, createRes)
		fileIno := createRes.Inode.Ino
		fileFh := createRes.Fh

		// 验证创建后配额：used_inodes + 1
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// SetAttr 扩容常规文件至 4096 字节
		size4096 := uint64(4096)
		_, err = mgr.SetAttr(ctx, fileIno, meta.SetAttrInput{
			Size: &size4096,
		})
		require.NoError(t, err)

		// 验证扩容后配额：used_bytes + 4096
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(4096), d.UsedBytes)

		// Unlink 文件：存在活跃句柄 fileFh，进入 unlink-whilst-open，nlink 变 0 但暂不回收 Inode 也不退还配额
		err = mgr.Unlink(ctx, disk.RootIno, []byte("file_unlink.txt"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "有活跃句柄时 Unlink 不得退还 used_inodes")
		assert.Equal(t, uint64(4096), d.UsedBytes, "有活跃句柄时 Unlink 不得退还 used_bytes")

		// Release 关闭最后一个句柄：触发物理删除与配额归还
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: fileIno,
			Fh:  fileFh,
		})
		require.NoError(t, err)

		// 验证配额已退还：used_inodes 恢复为 1，used_bytes 恢复为 0
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes, "关闭最后一个句柄时应扣减 1 个 inode 配额")
		assert.Equal(t, uint64(0), d.UsedBytes, "关闭最后一个句柄时应退还 4096 字节配额")

		// Inode 物理回收确认
		_, err = mgr.GetAttr(ctx, fileIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后 Inode 应不可见")
	})

	t.Run("CreateDisk -> TmpFile -> SetAttr(size=8192) -> Release(最后一个句柄关闭时退还 used_bytes 与 used_inodes)", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_quota_tmpfile_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// TmpFile 创建匿名临时常规文件
		tmpRes, err := mgr.TmpFile(ctx, meta.TmpFileParams{
			ParentIno: disk.RootIno,
			Mode:      meta.S_IFREG | 0o600,
			Flags:     meta.O_RDWR,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, tmpRes)
		tmpIno := tmpRes.Inode.Ino
		tmpFh := tmpRes.Fh

		// 验证配额：used_inodes + 1
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// SetAttr 扩容至 8192 字节
		size8192 := uint64(8192)
		_, err = mgr.SetAttr(ctx, tmpIno, meta.SetAttrInput{
			Size: &size8192,
		})
		require.NoError(t, err)

		// 验证配额：used_bytes + 8192
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(8192), d.UsedBytes)

		// Release 关闭临时文件唯一句柄：物理回收 Inode 并退还配额
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: tmpIno,
			Fh:  tmpFh,
		})
		require.NoError(t, err)

		// 验证配额归还
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes, "释放 TmpFile 句柄应退还 1 个 inode 配额")
		assert.Equal(t, uint64(0), d.UsedBytes, "释放 TmpFile 句柄应退还 8192 字节配额")

		// Inode 物理回收确认
		_, err = mgr.GetAttr(ctx, tmpIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后 TmpFile Inode 应不可见")
	})

	t.Run("CreateDisk -> Mkdir -> OpenDir -> Rmdir(有活跃句柄暂不退还) -> ReleaseDir(最后一个句柄关闭时退还 used_inodes)", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_quota_dir_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		// Mkdir 创建子目录
		dirInode, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("subdir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		require.NoError(t, err)
		require.NotNil(t, dirInode)
		dirIno := dirInode.Ino

		// 验证配额：used_inodes + 1
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes)
		assert.Equal(t, uint64(0), d.UsedBytes)

		// OpenDir 打开目录获取目录句柄
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino: dirIno,
		})
		require.NoError(t, err)
		require.NotNil(t, openDirRes)
		dirFh := openDirRes.Fh

		// Rmdir 删除目录：因存在活跃目录句柄 dirFh，进入 rmdir-whilst-open，nlink 变 0 但暂不回收 Inode 也不退还配额
		err = mgr.Rmdir(ctx, disk.RootIno, []byte("subdir"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "有活跃目录句柄时 Rmdir 不得退还 used_inodes")

		// ReleaseDir 关闭最后一个句柄：触发物理删除与退还 used_inodes 配额
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino: dirIno,
			Fh:  dirFh,
		})
		require.NoError(t, err)

		// 验证配额归还
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(1), d.UsedInodes, "关闭最后一个目录句柄时应扣减 1 个 inode 配额")
		assert.Equal(t, uint64(0), d.UsedBytes)

		// Inode 物理回收确认
		_, err = mgr.GetAttr(ctx, dirIno)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后目录 Inode 应不可见")
	})

	t.Run("携带 SessionID 关闭孤儿常规文件的最后一个句柄：验证 Size 和 UsedInodes 仅被扣减一次（无双重扣减）", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_session_quota_file_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		require.NotNil(t, disk)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessID := uint64(770001)
		_, err = mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)

		// 1. 创建常驻文件 normal.txt，大小 8192 字节，用于提供配额基底
		normRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("normal.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		size8192 := uint64(8192)
		_, err = mgr.SetAttr(ctx, normRes.Inode.Ino, meta.SetAttrInput{
			Size:      &size8192,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 2. 创建即将被 unlink 的文件 orphan.txt，大小 4096 字节
		orphanRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan.txt"),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		size4096 := uint64(4096)
		_, err = mgr.SetAttr(ctx, orphanRes.Inode.Ino, meta.SetAttrInput{
			Size:      &size4096,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 当前配额状态：根目录 + normal.txt + orphan.txt = 3 inodes；8192 + 4096 = 12288 bytes
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)
		assert.Equal(t, uint64(12288), d.UsedBytes)

		// 3. Unlink orphan.txt：此时 orphanRes.Fh 依然打开，nlink 变 0 但节点暂不回收，配额不变
		err = mgr.Unlink(ctx, disk.RootIno, []byte("orphan.txt"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)
		assert.Equal(t, uint64(12288), d.UsedBytes)

		// 4. Release 关闭 orphan.txt 句柄（携带 SessionID）
		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino:       orphanRes.Inode.Ino,
			Fh:        orphanRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 5. 校验配额：必须且仅扣减 1 个 inode 和 4096 字节（剩余 2 个 inodes 和 8192 字节）
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "孤儿文件释放后应恰好剩余 2 个 inode (根目录与 normal.txt)")
		assert.Equal(t, uint64(8192), d.UsedBytes, "孤儿文件释放后应恰好剩余 8192 字节 (normal.txt 占用)")
	})

	t.Run("携带 SessionID 关闭孤儿目录的最后一个句柄：验证 UsedInodes 仅被扣减一次（无双重扣减）", func(t *testing.T) {
		diskName := fmt.Sprintf("test_releasedir_session_quota_dir_%d", time.Now().UnixNano())
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

		// 1. 创建常驻子目录 dir1，用于提供配额基底
		dir1, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dir1"),
			Mode:      0o755,
		})
		require.NoError(t, err)
		require.NotNil(t, dir1)

		// 2. 创建即将被 rmdir 的孤儿目录 dir2
		dir2, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dir2"),
			Mode:      0o755,
		})
		require.NoError(t, err)
		require.NotNil(t, dir2)

		// 3. 打开 dir2 句柄（携带 SessionID）
		openRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       dir2.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 当前配额状态：根目录 + dir1 + dir2 = 3 inodes
		d, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes)

		// 4. Rmdir 删除 dir2：因句柄打开，nlink 变 0 成为孤儿，配额保持 3
		err = mgr.Rmdir(ctx, disk.RootIno, []byte("dir2"))
		require.NoError(t, err)

		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(3), d.UsedInodes, "有活跃目录句柄时 Rmdir 不得退还 used_inodes")

		// 5. ReleaseDir 关闭 dir2 句柄（携带 SessionID）
		err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       dir2.Ino,
			Fh:        openRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 6. 校验配额：必须且仅扣减 1 个 inode（剩余 2 个 inodes：根目录与 dir1）
		d, err = mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, uint64(2), d.UsedInodes, "孤儿目录释放后应恰好剩余 2 个 inode (根目录与 dir1)")

		// 确认物理删除
		_, err = mgr.GetAttr(ctx, dir2.Ino)
		assert.True(t, errors.Is(err, meta.ErrNotFound), "物理回收后目录 Inode 应不可见")
	})

	t.Run("正常节点 (nlink > 0) Release 使用 FOR SHARE 协同锁，ReleaseDir 免除 Inode 排他锁", func(t *testing.T) {
		diskName := fmt.Sprintf("test_release_lockless_non_orphan_%d", time.Now().UnixNano())
		disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName,
			MaxBytes: 10 * 1024 * 1024,
		})
		require.NoError(t, err)
		defer func() {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}()

		sessID := uint64(775371)
		defer func() {
			_, _ = pool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
		}()
		_, err = mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("lockless_release.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("lockless_dir"),
			Mode:      0o755,
		})
		require.NoError(t, err)
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       subDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 外部事务持有目标文件与目录 Inode 的 FOR SHARE 协同锁（兼容并发 ReleaseFileFast / ReleaseDirFast 的 FOR SHARE）
		extTx, err := pool.Begin(ctx)
		require.NoError(t, err)
		defer func() {
			_ = extTx.Rollback(context.Background())
		}()

		var lockedFileIno, lockedDirIno int64
		require.NoError(t, extTx.QueryRow(ctx, `SELECT ino FROM inodes WHERE ino = $1 FOR SHARE`, int64(createRes.Inode.Ino)).Scan(&lockedFileIno))
		require.NoError(t, extTx.QueryRow(ctx, `SELECT ino FROM inodes WHERE ino = $1 FOR SHARE`, int64(subDir.Ino)).Scan(&lockedDirIno))

		opCtx, opCancel := context.WithTimeout(ctx, 1500*time.Millisecond)
		defer opCancel()

		require.NoError(t, mgr.Release(opCtx, meta.ReleaseParams{
			Ino:       createRes.Inode.Ino,
			Fh:        createRes.Fh,
			SessionID: &sessID,
		}))
		require.NoError(t, mgr.ReleaseDir(opCtx, meta.ReleaseDirParams{
			Ino:       subDir.Ino,
			Fh:        openDirRes.Fh,
			SessionID: &sessID,
		}))
	})
}

type releaseFastQueryTraceKey struct{}

type releaseFastQueryTracer struct {
	mu      sync.Mutex
	queries []string
}

func (t *releaseFastQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(releaseFastQueryTraceKey{}) != nil {
		t.mu.Lock()
		t.queries = append(t.queries, strings.TrimSpace(data.SQL))
		t.mu.Unlock()
	}
	return ctx
}

func (t *releaseFastQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *releaseFastQueryTracer) reset() {
	t.mu.Lock()
	t.queries = nil
	t.mu.Unlock()
}

func (t *releaseFastQueryTracer) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.queries))
	copy(out, t.queries)
	return out
}

func TestRelease_FastPathSingleStatementAndOrphanFallback_PostgresIntegration(t *testing.T) {
	tracer := &releaseFastQueryTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	diskName := fmt.Sprintf("test_release_fast_cte_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 32 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	sessID := uint64(time.Now().UnixNano() & 0x7fffffff)
	_, err = mgr.RenewSession(ctx, sessID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM client_sessions WHERE session_id = $1`, int64(sessID))
	})

	t.Run("常规 Release 快速路径仅触发 1 条 SQL 语句且复用连接 SHOW synchronous_commit 保持 on", func(t *testing.T) {
		cfg := pool.Config().Copy()
		cfg.MaxConns = 1
		cfg.MinConns = 1
		cfg.ConnConfig.Tracer = tracer
		singlePool, err := pgxpool.NewWithConfig(ctx, cfg)
		require.NoError(t, err)
		defer singlePool.Close()

		createRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("fast_single_stmt.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		singleStore := db.NewStore(singlePool)
		singleMgr := meta.New(singleStore)

		// 登记一条同句柄的锁等待者，验证单语句 CTE 同步清理 file_lock_waiters
		_, err = singleStore.UpsertFileLockWaiter(ctx, db.UpsertFileLockWaiterParams{
			SessionID:   int64(sessID),
			RequestID:   1,
			Ino:         int64(createRes.Inode.Ino),
			Fh:          int64(createRes.Fh),
			LockOwner:   100,
			LkFlags:     0,
			StartOffset: 0,
			EndOffset:   64,
			LockType:    int16(meta.F_WRLCK),
			Pid:         1234,
			Status:      1,
		})
		require.NoError(t, err)

		tracer.reset()
		tracedCtx := context.WithValue(ctx, releaseFastQueryTraceKey{}, true)
		err = singleMgr.Release(tracedCtx, meta.ReleaseParams{
			Ino:       createRes.Inode.Ino,
			Fh:        createRes.Fh,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		sqls := tracer.snapshot()
		require.Len(t, sqls, 1, "常规 Release 快速路径必须且仅执行 1 条 SQL 语句，实际: %v", sqls)
		assert.Contains(t, sqls[0], "ReleaseFileFast")
		for _, q := range sqls {
			upper := strings.ToUpper(q)
			assert.NotEqual(t, "BEGIN", upper)
			assert.NotEqual(t, "COMMIT", upper)
			assert.False(t, strings.HasPrefix(upper, "SET LOCAL"))
		}

		// 验证同连接复用后 synchronous_commit 仍为 "on"
		var syncCommit string
		require.NoError(t, singlePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit))
		assert.Equal(t, "on", syncCommit, "内联 set_config(..., true) 仅在当前语句隐式事务生效，连接归还后必须保持 on")

		// 验证句柄与关联锁等待者均已被删除
		var handleCount, waiterCount int
		require.NoError(t, singlePool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE fh = $1`, int64(createRes.Fh)).Scan(&handleCount))
		assert.Zero(t, handleCount)
		require.NoError(t, singlePool.QueryRow(ctx, `SELECT COUNT(*) FROM file_lock_waiters WHERE ino = $1 AND fh = $2`, int64(createRes.Inode.Ino), int64(createRes.Fh)).Scan(&waiterCount))
		assert.Zero(t, waiterCount)
	})

	t.Run("目录拒绝 (ErrIsDir)、不存在节点 (ErrNotFound) 与无效/过期会话拒绝 (ErrBadFileHandle)", func(t *testing.T) {
		subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("dir_reject"),
			Mode:      0o755,
		})
		require.NoError(t, err)

		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: subDir.Ino,
			Fh:  999999,
		})
		require.ErrorIs(t, err, meta.ErrIsDir)

		err = mgr.Release(ctx, meta.ReleaseParams{
			Ino: 1 << 60,
			Fh:  999999,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)

		expSess := sessID + 100
		_, err = mgr.RenewSession(ctx, expSess)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM client_sessions WHERE session_id = $1`, int64(expSess))
		})

		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("expired_sess.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &expSess,
		})
		require.NoError(t, err)

		// 1. 传错误 sessionID 拒绝
		wrongSess := expSess + 1
		require.ErrorIs(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fRes.Inode.Ino,
			Fh:        fRes.Fh,
			SessionID: &wrongSess,
		}), meta.ErrBadFileHandle)

		// 2. 不传 sessionID 关闭带会话句柄拒绝
		require.ErrorIs(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino: fRes.Inode.Ino,
			Fh:  fRes.Fh,
		}), meta.ErrBadFileHandle)

		// 3. 将会话置为过期后拒绝
		_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '5 seconds' WHERE session_id = $1`, int64(expSess))
		require.NoError(t, err)
		require.ErrorIs(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fRes.Inode.Ino,
			Fh:        fRes.Fh,
			SessionID: &expSess,
		}), meta.ErrBadFileHandle)
	})

	t.Run("多句柄并发关闭与 Open -> Unlink -> Release 最后一个句柄回收", func(t *testing.T) {
		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("multi_handle_orphan.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		const extraHandles = 5
		fhs := make([]uint64, 0, extraHandles+1)
		fhs = append(fhs, fRes.Fh)
		for i := 0; i < extraHandles; i++ {
			oRes, err := mgr.Open(ctx, meta.OpenParams{
				Ino:       fRes.Inode.Ino,
				Flags:     meta.O_RDWR,
				SessionID: &sessID,
			})
			require.NoError(t, err)
			fhs = append(fhs, oRes.Fh)
		}

		// 先并发关闭前 extraHandles 个句柄（nlink > 0 快速路径）
		var wg sync.WaitGroup
		for _, h := range fhs[:extraHandles] {
			wg.Add(1)
			go func(fh uint64) {
				defer wg.Done()
				require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
					Ino:       fRes.Inode.Ino,
					Fh:        fh,
					SessionID: &sessID,
				}))
			}(h)
		}
		wg.Wait()

		// Unlink 使文件变为孤儿 (nlink == 0)，此时仍剩最后一个句柄 fhs[extraHandles]
		require.NoError(t, mgr.Unlink(ctx, disk.RootIno, []byte("multi_handle_orphan.txt")))

		var nlink int32
		require.NoError(t, pool.QueryRow(ctx, `SELECT nlink FROM inodes WHERE ino = $1`, int64(fRes.Inode.Ino)).Scan(&nlink))
		assert.Zero(t, nlink)

		// 关闭最后一个句柄：ReleaseFileFast 返回 orphan，无缝进入慢路径完成物理回收
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:       fRes.Inode.Ino,
			Fh:        fhs[extraHandles],
			SessionID: &sessID,
		}))

		var inodeCount, handleCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inodes WHERE ino = $1`, int64(fRes.Inode.Ino)).Scan(&inodeCount))
		assert.Zero(t, inodeCount, "最后一个句柄 Release 后孤儿 Inode 必须被物理删除")
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE ino = $1`, int64(fRes.Inode.Ino)).Scan(&handleCount))
		assert.Zero(t, handleCount)
	})

	t.Run("并发 Unlink 与多句柄 Release 交错执行零孤儿泄漏", func(t *testing.T) {
		const rounds = 16
		const handlesPerFile = 3
		recordedInos := make([]int64, 0, rounds)

		for r := 0; r < rounds; r++ {
			name := []byte(fmt.Sprintf("race_unlink_release_%d.txt", r))
			cRes, err := mgr.Create(ctx, meta.CreateParams{
				ParentIno: disk.RootIno,
				Name:      name,
				Mode:      meta.S_IFREG | 0o644,
				Flags:     meta.O_RDWR,
				SessionID: &sessID,
			})
			require.NoError(t, err)
			recordedInos = append(recordedInos, int64(cRes.Inode.Ino))

			fhs := make([]uint64, 0, handlesPerFile)
			fhs = append(fhs, cRes.Fh)
			for i := 1; i < handlesPerFile; i++ {
				oRes, err := mgr.Open(ctx, meta.OpenParams{
					Ino:       cRes.Inode.Ino,
					Flags:     meta.O_RDWR,
					SessionID: &sessID,
				})
				require.NoError(t, err)
				fhs = append(fhs, oRes.Fh)
			}

			startCh := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1 + len(fhs))

			go func() {
				defer wg.Done()
				<-startCh
				require.NoError(t, mgr.Unlink(ctx, disk.RootIno, name))
			}()

			for _, fh := range fhs {
				go func(h uint64) {
					defer wg.Done()
					<-startCh
					require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
						Ino:       cRes.Inode.Ino,
						Fh:        h,
						SessionID: &sessID,
					}))
				}(fh)
			}

			close(startCh)
			wg.Wait()
		}

		// 直接查表校验：所有并发 Unlink + Release 的文件均无残留 inodes 或 file_handles
		var remainingInodes, remainingHandles int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM inodes WHERE ino = ANY($1)`, recordedInos).Scan(&remainingInodes))
		assert.Zero(t, remainingInodes, "并发 Unlink 与 Release 交错后不得残留任何孤儿 Inode")
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE ino = ANY($1)`, recordedInos).Scan(&remainingHandles))
		assert.Zero(t, remainingHandles, "并发 Unlink 与 Release 交错后不得残留任何文件句柄")
	})

	t.Run("FuseReleaseFlockUnlock 与 POSIX 锁活动回退慢路径验证", func(t *testing.T) {
		fRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("lock_fallback.txt"),
			Mode:      meta.S_IFREG | 0o644,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		oRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       fRes.Inode.Ino,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		// 在 fRes.Fh 上加 BSD flock 锁
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fRes.Inode.Ino,
			Fh:        fRes.Fh,
			LockOwner: 1,
			LkFlags:   meta.FuseLkFlock,
			Lk: meta.FileLock{
				Start: 0,
				End:   meta.OffsetMax,
				Type:  meta.F_WRLCK,
				PID:   100,
			},
			SessionID: sessID,
		}))

		// 携带 FuseReleaseFlockUnlock 关闭 fRes.Fh：回退慢路径清理 flock 锁
		require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
			Ino:          fRes.Inode.Ino,
			Fh:           fRes.Fh,
			ReleaseFlags: meta.FuseReleaseFlockUnlock,
			SessionID:    &sessID,
		}))

		var flockCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_locks WHERE ino = $1 AND lk_flags = $2`, int64(fRes.Inode.Ino), int32(meta.FuseLkFlock)).Scan(&flockCount))
		assert.Zero(t, flockCount)

		// 在 oRes.Fh 上申请 POSIX 记录锁，触发 RecordPosixLockActivity(ino)
		require.NoError(t, mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       fRes.Inode.Ino,
			Fh:        oRes.Fh,
			LockOwner: 2002,
			LkFlags:   0,
			Lk: meta.FileLock{
				Start: 0,
				End:   128,
				Type:  meta.F_WRLCK,
				PID:   200,
			},
			SessionID: sessID,
		}))

		tracer.reset()
		tracedCtx := context.WithValue(ctx, releaseFastQueryTraceKey{}, true)
		require.NoError(t, mgr.Release(tracedCtx, meta.ReleaseParams{
			Ino:       fRes.Inode.Ino,
			Fh:        oRes.Fh,
			SessionID: &sessID,
		}))

		// 验证因 hasPosixLockActivity 为 true 走了显式写事务慢路径（包含 GetInodeForUpdate 与 DeleteFileHandle）
		sqls := tracer.snapshot()
		var sawForUpdate, sawDeleteHandle bool
		for _, q := range sqls {
			if strings.Contains(q, "GetInodeForUpdate") {
				sawForUpdate = true
			}
			if strings.Contains(q, "DeleteFileHandle") {
				sawDeleteHandle = true
			}
		}
		assert.True(t, sawForUpdate, "POSIX 锁活动节点 Release 应走慢路径执行 GetInodeForUpdate")
		assert.True(t, sawDeleteHandle, "POSIX 锁活动节点 Release 应走慢路径执行 DeleteFileHandle")
	})
}
