package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
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

func TestOpenDir_InputValidation(t *testing.T) {
	ctx := context.Background()

	for _, tc := range []struct {
		name  string
		ino   uint64
		flags uint32
	}{
		{
			name:  "ino 为 0",
			ino:   0,
			flags: meta.O_RDONLY,
		},
		{
			name:  "ino 超过 math.MaxInt64",
			ino:   uint64(math.MaxInt64) + 1,
			flags: meta.O_RDONLY,
		},
		{
			name:  "flags 包含 O_WRONLY",
			ino:   100,
			flags: meta.O_WRONLY,
		},
		{
			name:  "flags 包含 O_RDWR",
			ino:   100,
			flags: meta.O_RDWR,
		},
		{
			name:  "flags 包含 O_TRUNC",
			ino:   100,
			flags: meta.O_RDONLY | meta.O_TRUNC,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			mockStore := dbmock.NewMockStore(t)
			mgr := meta.New(mockStore)

			res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
				Ino:   tc.ino,
				Flags: tc.flags,
			})
			require.Error(t, err)
			assert.True(t, errors.Is(err, meta.ErrInvalidArgument))
			assert.Nil(t, res)
		})
	}
}

func TestOpenDir_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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

	t.Run("常规目录只读模式正常打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{
				Fh:        1,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(1), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})

	t.Run("孤儿目录 (nlink == 0) 存在活跃句柄时允许打开", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDirRow := baseDirRow
		orphanDirRow.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDirRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(true, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{
				Fh:        2,
				Ino:       int64(ino),
				Flags:     int32(meta.O_RDONLY),
				CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(2), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
	})
}

func TestOpenDir_NegativeAndErrorPaths(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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

	t.Run("目标节点不存在返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, pgx.ErrNoRows).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		assert.Nil(t, res)
	})

	t.Run("目标节点为普通文件返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		fileRow := baseDirRow
		fileRow.Mode = int32(meta.S_IFREG | 0o644)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(fileRow, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotDir))
		assert.Nil(t, res)
	})

	t.Run("死孤儿目录 (nlink == 0 且无活跃句柄) 返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		deadOrphanRow := baseDirRow
		deadOrphanRow.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(deadOrphanRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(deadOrphanRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.True(t, errors.Is(err, meta.ErrNotFound))
		assert.Nil(t, res)
	})

	t.Run("底层 GetInode 错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(db.Inode{}, errors.New("db error")).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Contains(t, err.Error(), "get inode")
	})

	t.Run("底层 HasFileHandlesByIno 错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		orphanDirRow := baseDirRow
		orphanDirRow.Nlink = 0

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(orphanDirRow, nil).
			Once()
		mockQuerier.EXPECT().
			GetInodeForUpdate(mock.Anything, int64(ino)).
			Return(orphanDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			HasFileHandlesByIno(mock.Anything, int64(ino)).
			Return(false, errors.New("db query handles error")).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Contains(t, err.Error(), "check has file handles")
	})

	t.Run("底层 CreateFileHandle 错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{}, errors.New("create handle error")).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.Error(t, err)
		assert.Nil(t, res)
		assert.Contains(t, err.Error(), "create file handle")
	})

	t.Run("启用 WithOpenDirCache(true) 时返回 FOPEN_CACHE_DIR", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore, meta.WithOpenDirCache(true))

		setupMockWriteTx(mockStore, mockQuerier)

		mockQuerier.EXPECT().
			GetInode(mock.Anything, int64(ino)).
			Return(baseDirRow, nil).
			Once()

		mockQuerier.EXPECT().
			CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
				Ino:   int64(ino),
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.FileHandle{Fh: 77, Ino: int64(ino), Flags: int32(meta.O_RDONLY)}, nil).
			Once()

		res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:   ino,
			Flags: meta.O_RDONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(77), res.Fh)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res.OpenFlags)
	})
}

func TestOpenDir_CreateFileHandlePgErrors(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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
			mockQuerier := dbmock.NewMockQuerier(t)
			mgr := meta.New(mockStore)

			setupMockWriteTx(mockStore, mockQuerier)

			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(baseDirRow, nil).
				Once()

			mockQuerier.EXPECT().
				CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
					Ino:   int64(ino),
					Flags: int32(meta.O_RDONLY),
				}).
				Return(db.FileHandle{}, tc.createErr).
				Once()

			res, err := mgr.OpenDir(ctx, meta.OpenDirParams{
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

func TestOpenDir_SmartCacheRetention_Unit(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	ino := uint64(100)

	baseDirRow := db.Inode{
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

	t.Run("默认配置下同一会话首次 OpenDir 返回 0，未变更时后续 OpenDir 返回 FOPEN_CACHE_DIR，版本变化后刷新并恢复", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		expectOpenDir := func(row db.Inode, fh int64, sessID *uint64) {
			setupMockWriteTx(mockStore, mockQuerier)
			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(row, nil).
				Once()
			var pgSid pgtype.Int8
			if sessID != nil {
				mockQuerier.EXPECT().
					HasActiveClientSession(mock.Anything, db.HasActiveClientSessionParams{
						SessionID: int64(*sessID),
						RootIno:   row.RootIno,
					}).
					Return(true, nil).
					Once()
				pgSid = pgtype.Int8{Int64: int64(*sessID), Valid: true}
			}
			mockQuerier.EXPECT().
				CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
					Ino:       int64(ino),
					Flags:     int32(meta.O_RDONLY),
					SessionID: pgSid,
				}).
				Return(db.FileHandle{
					Fh:        fh,
					Ino:       int64(ino),
					Flags:     int32(meta.O_RDONLY),
					SessionID: pgSid,
					CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				}, nil).
				Once()
		}

		// 1. 首次 OpenDir (nil SessionID -> key.sessionID = 0)
		expectOpenDir(baseDirRow, 1, nil)
		res1, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res1.OpenFlags)

		// 2. 未变更第二次 OpenDir -> 命中 FOPEN_CACHE_DIR
		expectOpenDir(baseDirRow, 2, nil)
		res2, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res2.OpenFlags)

		// 3. 未变更第三次 OpenDir -> 持续命中 FOPEN_CACHE_DIR
		expectOpenDir(baseDirRow, 3, nil)
		res3, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res3.OpenFlags)

		// 4. mtime 发生推进 -> 返回 0
		mtimeRow := baseDirRow
		mtimeRow.Mtime = pgtype.Timestamptz{Time: now.Add(time.Millisecond), Valid: true}
		expectOpenDir(mtimeRow, 4, nil)
		res4, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res4.OpenFlags)

		// 5. 再次未变更 OpenDir -> 恢复 FOPEN_CACHE_DIR
		expectOpenDir(mtimeRow, 5, nil)
		res5, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res5.OpenFlags)

		// 6. ctime 发生推进 -> 返回 0
		ctimeRow := mtimeRow
		ctimeRow.Ctime = pgtype.Timestamptz{Time: now.Add(2 * time.Millisecond), Valid: true}
		expectOpenDir(ctimeRow, 6, nil)
		res6, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res6.OpenFlags)

		// 7. 再次未变更 OpenDir -> 恢复 FOPEN_CACHE_DIR
		expectOpenDir(ctimeRow, 7, nil)
		res7, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res7.OpenFlags)

		// 8. nlink 发生变化 -> 返回 0
		nlinkRow := ctimeRow
		nlinkRow.Nlink = 3
		expectOpenDir(nlinkRow, 8, nil)
		res8, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), res8.OpenFlags)

		// 9. 再次未变更 OpenDir -> 恢复 FOPEN_CACHE_DIR
		expectOpenDir(nlinkRow, 9, nil)
		res9, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, res9.OpenFlags)
	})

	t.Run("不同 SessionID 之间版本追踪互不干扰", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mockQuerier := dbmock.NewMockQuerier(t)
		mgr := meta.New(mockStore)

		sessA := uint64(1001)
		sessB := uint64(1002)

		expectSessionOpenDir := func(row db.Inode, fh int64, sessID uint64) {
			setupMockWriteTx(mockStore, mockQuerier)
			mockQuerier.EXPECT().
				GetInode(mock.Anything, int64(ino)).
				Return(row, nil).
				Once()
			mockQuerier.EXPECT().
				HasActiveClientSession(mock.Anything, db.HasActiveClientSessionParams{
					SessionID: int64(sessID),
					RootIno:   row.RootIno,
				}).
				Return(true, nil).
				Once()
			pgSid := pgtype.Int8{Int64: int64(sessID), Valid: true}
			mockQuerier.EXPECT().
				CreateFileHandle(mock.Anything, db.CreateFileHandleParams{
					Ino:       int64(ino),
					Flags:     int32(meta.O_RDONLY),
					SessionID: pgSid,
				}).
				Return(db.FileHandle{
					Fh:        fh,
					Ino:       int64(ino),
					Flags:     int32(meta.O_RDONLY),
					SessionID: pgSid,
					CreatedAt: pgtype.Timestamptz{Time: now, Valid: true},
				}, nil).
				Once()
		}

		// 客户端 A 连续两次 OpenDir -> 0, FOPEN_CACHE_DIR
		expectSessionOpenDir(baseDirRow, 10, sessA)
		resA1, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resA1.OpenFlags)

		expectSessionOpenDir(baseDirRow, 11, sessA)
		resA2, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, resA2.OpenFlags)

		// 客户端 B 连续两次 OpenDir -> 0, FOPEN_CACHE_DIR
		expectSessionOpenDir(baseDirRow, 20, sessB)
		resB1, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resB1.OpenFlags)

		expectSessionOpenDir(baseDirRow, 21, sessB)
		resB2, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, resB2.OpenFlags)

		// 目录版本推进后，客户端 B 先行连续两次 OpenDir -> 0, FOPEN_CACHE_DIR
		mutatedRow := baseDirRow
		mutatedRow.Mtime = pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true}
		mutatedRow.Ctime = pgtype.Timestamptz{Time: now.Add(time.Second), Valid: true}

		expectSessionOpenDir(mutatedRow, 22, sessB)
		resB3, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resB3.OpenFlags)

		expectSessionOpenDir(mutatedRow, 23, sessB)
		resB4, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessB})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, resB4.OpenFlags)

		// 客户端 A 随后调用 OpenDir，绝不被客户端 B 的 OpenDir 掩盖变更 -> 首轮返回 0，次轮返回 FOPEN_CACHE_DIR
		expectSessionOpenDir(mutatedRow, 12, sessA)
		resA3, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), resA3.OpenFlags)

		expectSessionOpenDir(mutatedRow, 13, sessA)
		resA4, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: ino, Flags: meta.O_RDONLY, SessionID: &sessA})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, resA4.OpenFlags)
	})
}

func TestOpenDir_PostgresIntegration_CacheRetentionAndMutations(t *testing.T) {
	pool, testCtx := setupMetaTestDB(t)
	if pool == nil {
		return
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	disk, err := mgr.CreateDisk(testCtx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("disk-opendir-cache-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	dir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("work"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	openAndCloseDir := func(sessID *uint64) uint32 {
		res, openErr := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       dir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: sessID,
		})
		require.NoError(t, openErr)
		require.NotNil(t, res)
		relErr := mgr.ReleaseDir(testCtx, meta.ReleaseDirParams{
			Ino:       dir.Ino,
			Fh:        res.Fh,
			SessionID: sessID,
		})
		require.NoError(t, relErr)
		return res.OpenFlags
	}

	// 1. 默认配置下同一客户端首次 OpenDir 返回 0，未变更时第二次及第三次返回 FOPEN_CACHE_DIR
	sess1 := uint64(time.Now().UnixNano() & 0x3FFFFFFF)
	_, err = mgr.RenewSessionWithDisk(testCtx, sess1, &disk.DiskID)
	require.NoError(t, err)

	assert.Equal(t, uint32(0), openAndCloseDir(&sess1), "首次 OpenDir 应返回 OpenFlags == 0")
	assert.Equal(t, meta.FOPEN_CACHE_DIR, openAndCloseDir(&sess1), "目录未变更时第二次 OpenDir 应返回 FOPEN_CACHE_DIR")
	assert.Equal(t, meta.FOPEN_CACHE_DIR, openAndCloseDir(&sess1), "目录未变更时后续 OpenDir 应持续返回 FOPEN_CACHE_DIR")

	// 2. 验证目录经历 Create, Mkdir, Mknod, Symlink, Link, Unlink, Rmdir, Rename 任一变更后，
	// 下一次 OpenDir 返回 0（刷新缓存），再下一次未变更 OpenDir 恢复返回 FOPEN_CACHE_DIR。
	assertInvalidatedThenCached := func(step string) {
		t.Helper()
		assert.Equal(t, uint32(0), openAndCloseDir(&sess1), "%s 变更后首次 OpenDir 必须返回 OpenFlags == 0", step)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, openAndCloseDir(&sess1), "%s 变更后未再修改的次轮 OpenDir 必须恢复返回 FOPEN_CACHE_DIR", step)
	}

	// (a) Create
	createdRes, err := mgr.Create(testCtx, meta.CreateParams{
		ParentIno: dir.Ino,
		Name:      []byte("created.txt"),
		Mode:      0o644,
		Flags:     meta.O_RDWR,
		SessionID: &sess1,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
		Ino:       createdRes.Inode.Ino,
		Fh:        createdRes.Fh,
		SessionID: &sess1,
	}))
	assertInvalidatedThenCached("Create")

	// (b) Mkdir
	_, err = mgr.Mkdir(testCtx, meta.MkdirParams{
		ParentIno: dir.Ino,
		Name:      []byte("subdir"),
		Mode:      0o755,
	})
	require.NoError(t, err)
	assertInvalidatedThenCached("Mkdir")

	// (c) Mknod
	mknodNode, err := mgr.Mknod(testCtx, meta.MknodParams{
		ParentIno: dir.Ino,
		Name:      []byte("mknod.txt"),
		Mode:      meta.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	assertInvalidatedThenCached("Mknod")

	// (d) Symlink
	_, err = mgr.Symlink(testCtx, meta.SymlinkParams{
		ParentIno: dir.Ino,
		Name:      []byte("sym.lnk"),
		Target:    []byte("mknod.txt"),
	})
	require.NoError(t, err)
	assertInvalidatedThenCached("Symlink")

	// (e) Link
	_, err = mgr.Link(testCtx, meta.LinkParams{
		Ino:          mknodNode.Ino,
		NewParentIno: dir.Ino,
		NewName:      []byte("hardlink.txt"),
	})
	require.NoError(t, err)
	assertInvalidatedThenCached("Link")

	// (f) Unlink
	require.NoError(t, mgr.Unlink(testCtx, dir.Ino, []byte("hardlink.txt")))
	assertInvalidatedThenCached("Unlink")

	// (g) Rmdir
	require.NoError(t, mgr.Rmdir(testCtx, dir.Ino, []byte("subdir")))
	assertInvalidatedThenCached("Rmdir")

	// (h) Rename
	require.NoError(t, mgr.Rename(testCtx, meta.RenameParams{
		OldParentIno: dir.Ino,
		OldName:      []byte("created.txt"),
		NewParentIno: dir.Ino,
		NewName:      []byte("renamed.txt"),
	}))
	assertInvalidatedThenCached("Rename")

	// 3. 多客户端 SessionID 隔离测试：
	// 客户端 A 与客户端 B 各自完成两次 OpenDir（均命中 FOPEN_CACHE_DIR）后，
	// 客户端 B 在目录内创建/删除/重命名文件并由客户端 B 先行调用 OpenDir（客户端 B 自身首轮返回 0、次轮返回 FOPEN_CACHE_DIR），
	// 随后客户端 A 调用 OpenDir 必须返回 OpenFlags == 0（绝不被客户端 B 的 OpenDir 掩盖变更）并通过 ReadDir/ReadDirPlus 读到最新目录项列表。
	multiDir, err := mgr.Mkdir(testCtx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("multi_session_dir"),
		Mode:      0o755,
	})
	require.NoError(t, err)

	sessA := sess1 + 100
	sessB := sess1 + 200
	_, err = mgr.RenewSessionWithDisk(testCtx, sessA, &disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.RenewSessionWithDisk(testCtx, sessB, &disk.DiskID)
	require.NoError(t, err)

	openMultiDir := func(sessID uint64) *meta.OpenDirResult {
		res, openErr := mgr.OpenDir(testCtx, meta.OpenDirParams{
			Ino:       multiDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, openErr)
		require.NotNil(t, res)
		return res
	}
	closeMultiDir := func(sessID uint64, fh uint64) {
		require.NoError(t, mgr.ReleaseDir(testCtx, meta.ReleaseDirParams{
			Ino:       multiDir.Ino,
			Fh:        fh,
			SessionID: &sessID,
		}))
	}

	// 客户端 A 与客户端 B 各自完成两次 OpenDir，第二次均命中 FOPEN_CACHE_DIR
	resA1 := openMultiDir(sessA)
	assert.Equal(t, uint32(0), resA1.OpenFlags)
	closeMultiDir(sessA, resA1.Fh)

	resA2 := openMultiDir(sessA)
	assert.Equal(t, meta.FOPEN_CACHE_DIR, resA2.OpenFlags)
	closeMultiDir(sessA, resA2.Fh)

	resB1 := openMultiDir(sessB)
	assert.Equal(t, uint32(0), resB1.OpenFlags)
	closeMultiDir(sessB, resB1.Fh)

	resB2 := openMultiDir(sessB)
	assert.Equal(t, meta.FOPEN_CACHE_DIR, resB2.OpenFlags)
	closeMultiDir(sessB, resB2.Fh)

	assertClientBFirstThenClientASeesMutation := func(actionName string, mutateFn func(), expectedEntries []string) {
		t.Helper()
		mutateFn()

		// 客户端 B 先行调用 OpenDir：首轮返回 0，次轮返回 FOPEN_CACHE_DIR
		bFirst := openMultiDir(sessB)
		assert.Equal(t, uint32(0), bFirst.OpenFlags, "%s: 客户端 B 首轮 OpenDir 应返回 0", actionName)
		closeMultiDir(sessB, bFirst.Fh)

		bSecond := openMultiDir(sessB)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, bSecond.OpenFlags, "%s: 客户端 B 次轮 OpenDir 应返回 FOPEN_CACHE_DIR", actionName)
		closeMultiDir(sessB, bSecond.Fh)

		// 客户端 A 随后调用 OpenDir：必须返回 OpenFlags == 0（绝不被客户端 B 掩盖变更）
		aFirst := openMultiDir(sessA)
		assert.Equal(t, uint32(0), aFirst.OpenFlags, "%s: 客户端 A 在客户端 B OpenDir 后调用 OpenDir 仍必须返回 0", actionName)

		// 客户端 A 通过 ReadDir 与 ReadDirPlus 读到最新目录项列表
		entries, rdErr := mgr.ReadDir(testCtx, meta.ReadDirParams{
			Ino:       multiDir.Ino,
			Fh:        aFirst.Fh,
			Offset:    0,
			Limit:     100,
			SessionID: &sessA,
		})
		require.NoError(t, rdErr)
		var gotNames []string
		for _, e := range entries {
			name := string(e.Name)
			if name != "." && name != ".." {
				gotNames = append(gotNames, name)
			}
		}
		assert.Equal(t, expectedEntries, gotNames, "%s: 客户端 A ReadDir 应读到最新目录项", actionName)

		plusEntries, rdpErr := mgr.ReadDirPlus(testCtx, meta.ReadDirPlusParams{
			Ino:       multiDir.Ino,
			Fh:        aFirst.Fh,
			Offset:    0,
			Limit:     100,
			SessionID: &sessA,
		})
		require.NoError(t, rdpErr)
		var gotPlusNames []string
		for _, e := range plusEntries {
			name := string(e.Name)
			if name != "." && name != ".." {
				gotPlusNames = append(gotPlusNames, name)
			}
		}
		assert.Equal(t, expectedEntries, gotPlusNames, "%s: 客户端 A ReadDirPlus 应读到最新目录项", actionName)
		closeMultiDir(sessA, aFirst.Fh)

		// 客户端 A 再次调用未变更的 OpenDir 恢复命中 FOPEN_CACHE_DIR
		aSecond := openMultiDir(sessA)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, aSecond.OpenFlags, "%s: 客户端 A 次轮 OpenDir 应恢复返回 FOPEN_CACHE_DIR", actionName)
		closeMultiDir(sessA, aSecond.Fh)
	}

	// 客户端 B 创建文件 a.txt 与 b.txt
	assertClientBFirstThenClientASeesMutation("客户端 B Create 文件", func() {
		for _, name := range []string{"a.txt", "b.txt"} {
			cRes, cErr := mgr.Create(testCtx, meta.CreateParams{
				ParentIno: multiDir.Ino,
				Name:      []byte(name),
				Mode:      0o644,
				Flags:     meta.O_RDWR,
				SessionID: &sessB,
			})
			require.NoError(t, cErr)
			require.NoError(t, mgr.Release(testCtx, meta.ReleaseParams{
				Ino:       cRes.Inode.Ino,
				Fh:        cRes.Fh,
				SessionID: &sessB,
			}))
		}
	}, []string{"a.txt", "b.txt"})

	// 客户端 B 重命名 a.txt -> a_renamed.txt
	assertClientBFirstThenClientASeesMutation("客户端 B Rename 文件", func() {
		require.NoError(t, mgr.Rename(testCtx, meta.RenameParams{
			OldParentIno: multiDir.Ino,
			OldName:      []byte("a.txt"),
			NewParentIno: multiDir.Ino,
			NewName:      []byte("a_renamed.txt"),
		}))
	}, []string{"a_renamed.txt", "b.txt"})

	// 客户端 B 删除 b.txt
	assertClientBFirstThenClientASeesMutation("客户端 B Unlink 文件", func() {
		require.NoError(t, mgr.Unlink(testCtx, multiDir.Ino, []byte("b.txt")))
	}, []string{"a_renamed.txt"})
}
