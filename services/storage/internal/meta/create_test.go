package meta_test

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"

	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestCreate_PositivePath(t *testing.T) {
	ctx := context.Background()
	now := time.Now().Truncate(time.Microsecond)
	parentIno := uint64(10)
	newIno := uint64(100)
	expectedFh := int64(1)

	t.Run("常规文件原子创建并归一化仅含权限位的 mode", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      []byte("test_create.txt"),
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1001,
				Gid:       1002,
				Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(db.CreateFileAtomicRow{
				Status:     "ok",
				Fh:         expectedFh,
				Ino:        int64(newIno),
				Generation: 0,
				Mode:       int32(meta.S_IFREG | 0o644),
				Size:       0,
				Nlink:      1,
				Uid:        1001,
				Gid:        1002,
				Rdev:       0,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("test_create.txt"),
			Mode:      0o644, // 仅传入权限位，预期自动补齐 S_IFREG
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(expectedFh), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
		require.NotNil(t, res.Inode)
		assert.Equal(t, newIno, res.Inode.Ino)
		assert.Equal(t, meta.S_IFREG|0o644, res.Inode.Attr.Mode)
		assert.Equal(t, uint64(0), res.Inode.Attr.Size)
		assert.Equal(t, uint32(1), res.Inode.Attr.Nlink)
		assert.Equal(t, uint32(1001), res.Inode.Attr.UID)
		assert.Equal(t, uint32(1002), res.Inode.Attr.GID)
		assert.Equal(t, uint32(0), res.Inode.Attr.Rdev)
	})

	t.Run("显式携带 S_IFREG 模式位创建常规文件", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      []byte("script.sh"),
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o755),
				Uid:       1001,
				Gid:       1002,
				Flags:     int32(meta.O_CREAT | meta.O_WRONLY),
			}).
			Return(db.CreateFileAtomicRow{
				Status:     "ok",
				Fh:         2,
				Ino:        int64(newIno),
				Generation: 0,
				Mode:       int32(meta.S_IFREG | 0o755),
				Size:       0,
				Nlink:      1,
				Uid:        1001,
				Gid:        1002,
				Rdev:       0,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("script.sh"),
			Mode:      meta.S_IFREG | 0o755,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_WRONLY,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(2), res.Fh)
		assert.Equal(t, uint32(0), res.OpenFlags)
		assert.Equal(t, meta.S_IFREG|0o755, res.Inode.Attr.Mode)
	})

	t.Run("父目录包含 S_ISGID 标志时子文件继承父目录 GID", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      []byte("sgid_child.txt"),
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1001,
				Gid:       1002,
				Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(db.CreateFileAtomicRow{
				Status:     "ok",
				Fh:         3,
				Ino:        int64(newIno),
				Generation: 0,
				Mode:       int32(meta.S_IFREG | 0o644),
				Size:       0,
				Nlink:      1,
				Uid:        1001,
				Gid:        9999, // SQL CTE 内继承自父目录 GID
				Rdev:       0,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("sgid_child.txt"),
			Mode:      0o644,
			UID:       1001,
			GID:       1002, // 传入 1002，但由于父目录开启 S_ISGID，预期被覆盖为 9999
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint32(9999), res.Inode.Attr.GID)
	})

	t.Run("非 UTF-8 原始字节序列正常创建文件与目录项", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		rawName := []byte{0xff, 0xfe, 0x80, 0x81, 't', 'e', 's', 't'}

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      rawName,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1001,
				Gid:       1002,
				Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(db.CreateFileAtomicRow{
				Status:     "ok",
				Fh:         4,
				Ino:        int64(newIno),
				Generation: 0,
				Mode:       int32(meta.S_IFREG | 0o644),
				Size:       0,
				Nlink:      1,
				Uid:        1001,
				Gid:        1002,
				Rdev:       0,
				Atime:      pgtype.Timestamptz{Time: now, Valid: true},
				Mtime:      pgtype.Timestamptz{Time: now, Valid: true},
				Ctime:      pgtype.Timestamptz{Time: now, Valid: true},
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      rawName,
			Mode:      0o644,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(4), res.Fh)
		assert.Equal(t, newIno, res.Inode.Ino)
	})
}

func TestCreate_InputValidation(t *testing.T) {
	ctx := context.Background()
	mockStore := dbmock.NewMockStore(t)
	mgr := meta.New(mockStore)

	testCases := []struct {
		name        string
		params      meta.CreateParams
		expectedErr error
	}{
		{
			name: "parent_ino 为 0",
			params: meta.CreateParams{
				ParentIno: 0,
				Name:      []byte("file.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "parent_ino 超过 math.MaxInt64",
			params: meta.CreateParams{
				ParentIno: uint64(math.MaxInt64) + 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "name 为空字符串",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte(""),
				Mode:      0o644,
			},
		},
		{
			name: "name 长度超过 255 字节",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte(strings.Repeat("a", 256)),
				Mode:      0o644,
			},
			expectedErr: meta.ErrNameTooLong,
		},
		{
			name: "name 包含斜杠 /",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("sub/file.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "name 包含空字符 \\x00",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file\x00.txt"),
				Mode:      0o644,
			},
		},
		{
			name: "mode 超过 0xffff (65535)",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      65536,
			},
		},
		{
			name: "mode 文件类型为目录 S_IFDIR",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFDIR | 0o755,
			},
		},
		{
			name: "mode 文件类型为符号链接 S_IFLNK",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFLNK | 0o777,
			},
		},
		{
			name: "mode 文件类型为字符设备 S_IFCHR",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFCHR | 0o666,
			},
		},
		{
			name: "mode 文件类型为块设备 S_IFBLK",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFBLK | 0o666,
			},
		},
		{
			name: "mode 文件类型为管道 S_IFIFO",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFIFO | 0o644,
			},
		},
		{
			name: "mode 文件类型为套接字 S_IFSOCK",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      meta.S_IFSOCK | 0o666,
			},
		},
		{
			name: "flags 携带 O_TMPFILE 标志位",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
				Flags:     meta.O_TMPFILE,
			},
		},
		{
			name: "flags 携带 O_TMPFILE | O_RDWR 组合标志位",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
				Flags:     meta.O_TMPFILE | meta.O_RDWR,
			},
		},
		{
			name: "flags 携带 O_TMPFILE | O_CREAT | O_WRONLY 组合标志位",
			params: meta.CreateParams{
				ParentIno: 1,
				Name:      []byte("file.txt"),
				Mode:      0o644,
				Flags:     meta.O_TMPFILE | meta.O_CREAT | meta.O_WRONLY,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			res, err := mgr.Create(ctx, tc.params)
			assert.Nil(t, res)
			exp := tc.expectedErr
			if exp == nil {
				exp = meta.ErrInvalidArgument
			}
			assert.ErrorIs(t, err, exp)
		})
	}
}

func TestCreate_ParentInvariantsAndErrors(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)

	validParams := meta.CreateParams{
		ParentIno: parentIno,
		Name:      []byte("valid_file.txt"),
		Mode:      0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	}

	t.Run("父节点不存在时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      validParams.Name,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(validParams.Flags),
			}).
			Return(db.CreateFileAtomicRow{Status: "not_found"}, nil).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("父节点非目录 (S_IFREG) 时返回 ErrNotDir", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      validParams.Name,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(validParams.Flags),
			}).
			Return(db.CreateFileAtomicRow{Status: "not_dir"}, nil).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotDir)
	})

	t.Run("父目录为已解链孤儿目录 (nlink == 0) 时返回 ErrNotFound", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      validParams.Name,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(validParams.Flags),
			}).
			Return(db.CreateFileAtomicRow{Status: "not_found"}, nil).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrNotFound)
	})

	t.Run("name 为 . 或 .. 时在确认父目录有效后返回 ErrAlreadyExists", func(t *testing.T) {
		for _, reserved := range [][]byte{[]byte("."), []byte("..")} {
			t.Run(string(reserved), func(t *testing.T) {
				mockStore := dbmock.NewMockStore(t)
				mgr := meta.New(mockStore)

				mockStore.EXPECT().
					CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
						Name:      reserved,
						ParentIno: int64(parentIno),
						Mode:      int32(meta.S_IFREG | 0o644),
						Uid:       1000,
						Gid:       1000,
						Flags:     int32(validParams.Flags),
					}).
					Return(db.CreateFileAtomicRow{Status: "already_exists"}, nil).
					Once()

				params := validParams
				params.Name = reserved

				res, err := mgr.Create(ctx, params)
				assert.Nil(t, res)
				assert.ErrorIs(t, err, meta.ErrAlreadyExists)
			})
		}
	})

	t.Run("底层未捕获数据库错误返回包装错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		dbErr := errors.New("db disk failure")
		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      validParams.Name,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(validParams.Flags),
			}).
			Return(db.CreateFileAtomicRow{}, dbErr).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "create tx")
	})
}

func TestCreate_AlreadyExistsAndFailureHandling(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)

	validParams := meta.CreateParams{
		ParentIno: parentIno,
		Name:      []byte("conflict_file.txt"),
		Mode:      0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_CREAT | meta.O_RDWR,
	}

	t.Run("已有同名 Entry 捕获 SQLSTATE 23505 映射为 ErrAlreadyExists", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		pgErr := &pgconn.PgError{Code: "23505", Message: "duplicate key value violates unique constraint pk_entries"}
		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      validParams.Name,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(validParams.Flags),
			}).
			Return(db.CreateFileAtomicRow{}, pgErr).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		assert.ErrorIs(t, err, meta.ErrAlreadyExists)
	})

	t.Run("CreateFileAtomic 失败时透传底层错误", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		handleErr := errors.New("file handle sequence overflow")
		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      validParams.Name,
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(validParams.Flags),
			}).
			Return(db.CreateFileAtomicRow{}, handleErr).
			Once()

		res, err := mgr.Create(ctx, validParams)
		assert.Nil(t, res)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "file handle sequence overflow")
	})
}

// TestCreate_RejectOTmpfile 验证 Create 收到携带 O_TMPFILE 标志位时静态入参校验直接拒绝。
func TestCreate_RejectOTmpfile(t *testing.T) {
	ctx := context.Background()

	t.Run("Create 收到 flags 携带 O_TMPFILE 时直接静态拒绝并映射为 EINVAL", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		// 零 Mock 预期设置：静态入参校验严禁触发任何数据库读写算子
		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("forged_tmpfile.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR | meta.O_TMPFILE,
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

	t.Run("Create 应用 Umask 并下发 WithOpenKeepCache 及自定义缓存时效", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore,
			meta.WithOpenKeepCache(true),
			meta.WithEntryTimeout(3.5),
			meta.WithAttrTimeout(4.5),
		)

		expectedMode := int32(meta.S_IFREG | 0o640)
		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      []byte("masked.txt"),
				ParentIno: 1,
				Mode:      expectedMode,
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(db.CreateFileAtomicRow{
				Status: "ok",
				Fh:     55,
				Ino:    100,
				Mode:   expectedMode,
				Nlink:  1,
				Uid:    1000,
				Gid:    1000,
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1,
			Name:      []byte("masked.txt"),
			Mode:      0o666,
			Umask:     0o026,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint32(expectedMode), res.Inode.Attr.Mode)
		assert.Equal(t, uint64(0), res.Inode.Attr.ParentIno)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, res.OpenFlags)
		assert.Equal(t, 3.5, res.EntryTimeoutSeconds)
		assert.Equal(t, 4.5, res.AttrTimeoutSeconds)
	})
}

// TestCreate_InheritRootInoAndQuotaIncrement 验证 Create 联动继承父目录 root_ino 并递增 used_inodes。
func TestCreate_InheritRootInoAndQuotaIncrement(t *testing.T) {
	ctx := context.Background()
	parentIno := uint64(10)
	rootIno := int64(1001)
	newIno := uint64(100)
	expectedFh := int64(1)

	t.Run("当 parent.RootIno > 0 时继承 root_ino 并通过 commitDirectQuotaDelta 递增内存配额账本 used_inodes", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		mgr := meta.New(mockStore)

		mockStore.EXPECT().
			CreateFileAtomic(mock.Anything, db.CreateFileAtomicParams{
				Name:      []byte("quota_file.txt"),
				ParentIno: int64(parentIno),
				Mode:      int32(meta.S_IFREG | 0o644),
				Uid:       1000,
				Gid:       1000,
				Flags:     int32(meta.O_CREAT | meta.O_RDWR),
			}).
			Return(db.CreateFileAtomicRow{
				Status:  "ok",
				Fh:      expectedFh,
				Ino:     int64(newIno),
				Mode:    int32(meta.S_IFREG | 0o644),
				Nlink:   1,
				Uid:     1000,
				Gid:     1000,
				RootIno: rootIno,
			}, nil).
			Once()

		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: parentIno,
			Name:      []byte("quota_file.txt"),
			Mode:      0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})

		require.NoError(t, err)
		require.NotNil(t, res)
		assert.Equal(t, uint64(expectedFh), res.Fh)
		require.NotNil(t, res.Inode)
		assert.Equal(t, newIno, res.Inode.Ino)
	})
}

type createAtomicQueryTraceKey struct{}

type createAtomicQueryTracer struct {
	mu      sync.Mutex
	queries []string
}

func (t *createAtomicQueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	if ctx.Value(createAtomicQueryTraceKey{}) == nil {
		return ctx
	}
	sql := strings.TrimSpace(data.SQL)
	if sql == "" {
		return ctx
	}
	t.mu.Lock()
	t.queries = append(t.queries, sql)
	t.mu.Unlock()
	return ctx
}

func (t *createAtomicQueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *createAtomicQueryTracer) reset() {
	t.mu.Lock()
	t.queries = nil
	t.mu.Unlock()
}

func (t *createAtomicQueryTracer) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, len(t.queries))
	copy(out, t.queries)
	return out
}

func TestCreate_SingleStatementAtomicCTEAndAsyncWAL_PostgresIntegration(t *testing.T) {
	tracer := &createAtomicQueryTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}

	store := db.NewStore(pool)
	mgr := meta.New(store)

	diskName := fmt.Sprintf("test_create_atomic_cte_%d", time.Now().UnixNano())
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     diskName,
		MaxBytes: 32 * 1024 * 1024,
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
	})

	sessID := uint64(time.Now().UnixNano() & 0x7fffffff)
	_, err = mgr.RenewSessionWithDisk(ctx, sessID, &disk.DiskID)
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), `DELETE FROM client_sessions WHERE session_id = $1`, int64(sessID))
	})

	t.Run("单语句 CTE 与内联异步提交验证：Create 仅触发 1 条 SQL 语句且复用连接 SHOW synchronous_commit 保持 on", func(t *testing.T) {
		tracer.reset()
		tracedCtx := context.WithValue(ctx, createAtomicQueryTraceKey{}, true)
		createRes, err := mgr.Create(tracedCtx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("single_stmt_create.txt"),
			Mode:      0o666,
			Umask:     0o022,
			UID:       1001,
			GID:       1002,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotNil(t, createRes)
		assert.Equal(t, uint32(meta.S_IFREG|0o644), createRes.Inode.Attr.Mode)
		assert.Equal(t, uint32(1), createRes.Inode.Attr.Nlink)
		assert.Equal(t, uint32(1001), createRes.Inode.Attr.UID)
		assert.Equal(t, uint32(1002), createRes.Inode.Attr.GID)
		assert.NotZero(t, createRes.Fh)

		sqls := tracer.snapshot()
		require.Len(t, sqls, 1, "Create 必须且仅执行 1 条 SQL 语句，实际: %v", sqls)
		assert.Contains(t, sqls[0], "CreateFileAtomic")
		for _, q := range sqls {
			upper := strings.ToUpper(q)
			assert.NotEqual(t, "BEGIN", upper)
			assert.NotEqual(t, "COMMIT", upper)
			assert.False(t, strings.HasPrefix(upper, "SET LOCAL"))
		}

		// 验证同连接复用后 synchronous_commit 仍为 "on"
		var syncCommit string
		require.NoError(t, pool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit))
		assert.Equal(t, "on", syncCommit, "内联 set_config('synchronous_commit', 'off', true) 仅在当前语句隐式事务生效，连接归还后必须保持 on")
	})

	t.Run("S_ISGID 属组继承、Umask 掩码与磁盘 UsedInodes 配额账本核算", func(t *testing.T) {
		beforeDisk, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		sgidDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("sgid_parent_dir"),
			Mode:      meta.S_ISGID | 0o775,
			UID:       1000,
			GID:       7777,
		})
		require.NoError(t, err)
		assert.Equal(t, uint32(meta.S_IFDIR|meta.S_ISGID|0o775), sgidDir.Attr.Mode)
		assert.Equal(t, uint32(7777), sgidDir.Attr.GID)

		childRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: sgidDir.Ino,
			Name:      []byte("inherited_gid.txt"),
			Mode:      meta.S_ISGID | 0o777,
			Umask:     0o027,
			UID:       2001,
			GID:       2002,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NotNil(t, childRes)
		// Umask 仅屏蔽低 9 位权限位 (0o777 &^ 0o027 = 0o750)，保留特殊位 S_ISGID，且 GID 继承父目录 7777
		assert.Equal(t, uint32(meta.S_IFREG|meta.S_ISGID|0o750), childRes.Inode.Attr.Mode)
		assert.Equal(t, uint32(2001), childRes.Inode.Attr.UID)
		assert.Equal(t, uint32(7777), childRes.Inode.Attr.GID)

		afterDisk, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, beforeDisk.UsedInodes+2, afterDisk.UsedInodes, "Mkdir + Create 应使磁盘 UsedInodes 精确增加 2")
	})

	t.Run("并发同名创建：恰好 1 个成功、其余返回 ErrAlreadyExists 且零孤儿节点/句柄残留", func(t *testing.T) {
		beforeDisk, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)

		const workers = 16
		conflictName := []byte("concurrent_same_name.txt")
		startCh := make(chan struct{})
		var wg sync.WaitGroup
		var successCount atomic.Int32
		var existCount atomic.Int32
		var winningRes *meta.CreateResult
		var winMu sync.Mutex

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				res, cErr := mgr.Create(ctx, meta.CreateParams{
					ParentIno: disk.RootIno,
					Name:      conflictName,
					Mode:      0o644,
					UID:       1000,
					GID:       1000,
					Flags:     meta.O_CREAT | meta.O_EXCL | meta.O_RDWR,
					SessionID: &sessID,
				})
				if cErr == nil {
					successCount.Add(1)
					winMu.Lock()
					winningRes = res
					winMu.Unlock()
					return
				}
				require.ErrorIs(t, cErr, meta.ErrAlreadyExists)
				existCount.Add(1)
			}()
		}

		close(startCh)
		wg.Wait()

		assert.Equal(t, int32(1), successCount.Load(), "并发同名创建必须恰好 1 个成功")
		assert.Equal(t, int32(workers-1), existCount.Load(), "其余并发请求必须全部返回 ErrAlreadyExists")
		require.NotNil(t, winningRes)

		// 直接查表校验：entries 仅 1 条，inodes 无多余残留，file_handles 仅胜出者 1 条
		var entryCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM entries WHERE parent_ino = $1 AND name = $2`, int64(disk.RootIno), conflictName).Scan(&entryCount))
		assert.Equal(t, 1, entryCount)

		var handleCount int
		require.NoError(t, pool.QueryRow(ctx, `SELECT COUNT(*) FROM file_handles WHERE ino = $1`, int64(winningRes.Inode.Ino)).Scan(&handleCount))
		assert.Equal(t, 1, handleCount)

		afterDisk, err := mgr.GetDisk(ctx, disk.DiskID)
		require.NoError(t, err)
		assert.Equal(t, beforeDisk.UsedInodes+1, afterDisk.UsedInodes, "并发冲突失败的请求不得污染 UsedInodes 配额")
	})

	t.Run("父目录不存在、非目录、孤儿目录、点与点点拒绝，以及会话不存在/过期/跨盘越权拒绝", func(t *testing.T) {
		// 1. 父目录不存在 -> ErrNotFound
		_, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: 1 << 60,
			Name:      []byte("no_parent.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)

		// 2. 父节点为常规文件 -> ErrNotDir
		regFile, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("not_a_dir.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &sessID,
		})
		require.NoError(t, err)

		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: regFile.Inode.Ino,
			Name:      []byte("child.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
		})
		require.ErrorIs(t, err, meta.ErrNotDir)

		// 3. "." 与 ".." -> ErrAlreadyExists
		for _, dotName := range [][]byte{[]byte("."), []byte("..")} {
			_, err = mgr.Create(ctx, meta.CreateParams{
				ParentIno: disk.RootIno,
				Name:      dotName,
				Mode:      0o644,
				Flags:     meta.O_CREAT | meta.O_RDWR,
			})
			require.ErrorIs(t, err, meta.ErrAlreadyExists)
		}

		// 4. 父目录为已解链孤儿目录 (nlink == 0) -> ErrNotFound
		orphanDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("orphan_dir"),
			Mode:      0o755,
		})
		require.NoError(t, err)
		dirOpen, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       orphanDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		require.NoError(t, err)
		require.NoError(t, mgr.Rmdir(ctx, disk.RootIno, []byte("orphan_dir")))

		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: orphanDir.Ino,
			Name:      []byte("in_orphan_dir.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &sessID,
		})
		require.ErrorIs(t, err, meta.ErrNotFound)
		require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       orphanDir.Ino,
			Fh:        dirOpen.Fh,
			SessionID: &sessID,
		}))

		// 5. 会话不存在 -> ErrBadFileHandle
		missingSess := sessID + 999999
		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("missing_sess.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &missingSess,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		// 6. 会话已过期 -> ErrBadFileHandle
		expiredSess := sessID + 888888
		_, err = mgr.RenewSessionWithDisk(ctx, expiredSess, &disk.DiskID)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM client_sessions WHERE session_id = $1`, int64(expiredSess))
		})
		_, err = pool.Exec(ctx, `UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1`, int64(expiredSess))
		require.NoError(t, err)

		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("expired_sess.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &expiredSess,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		// 7. 跨盘越权会话 -> ErrBadFileHandle
		otherDisk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     diskName + "_other",
			MaxBytes: 16 * 1024 * 1024,
		})
		require.NoError(t, err)
		t.Cleanup(func() {
			_ = mgr.DeleteDisk(context.Background(), otherDisk.DiskID)
		})

		otherSess := sessID + 777777
		_, err = mgr.RenewSessionWithDisk(ctx, otherSess, &otherDisk.DiskID)
		require.NoError(t, err)
		t.Cleanup(func() {
			_, _ = pool.Exec(context.Background(), `DELETE FROM client_sessions WHERE session_id = $1`, int64(otherSess))
		})

		_, err = mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte("cross_disk_sess.txt"),
			Mode:      0o644,
			Flags:     meta.O_CREAT | meta.O_RDWR,
			SessionID: &otherSess,
		})
		require.ErrorIs(t, err, meta.ErrBadFileHandle)

		// 验证被拒绝的会话请求未插入任何脏目录项
		var dirtyEntries int
		require.NoError(t, pool.QueryRow(ctx, `
			SELECT COUNT(*) FROM entries
			WHERE parent_ino = $1 AND name IN ('missing_sess.txt'::bytea, 'expired_sess.txt'::bytea, 'cross_disk_sess.txt'::bytea)
		`, int64(disk.RootIno)).Scan(&dirtyEntries))
		assert.Zero(t, dirtyEntries)
	})
}
