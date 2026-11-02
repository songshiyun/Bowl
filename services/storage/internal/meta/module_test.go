package meta_test

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	dbmock "github.com/starsrc/bowl/services/storage/internal/db/mock"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

func TestMetaModule_DefaultInjection(t *testing.T) {
	mockStore := dbmock.NewMockStore(t)
	var mgr meta.Manager

	app := fxtest.New(
		t,
		fx.Provide(func() db.Store { return mockStore }),
		meta.Module,
		fx.Populate(&mgr),
	)
	require.NoError(t, app.Err())
	require.NotNil(t, mgr)

	ctx := context.Background()

	t.Run("默认缓存超时: 普通文件与目录均为 1.0 秒", func(t *testing.T) {
		mockStore.EXPECT().
			LookupEntryWithParent(mock.Anything, db.LookupEntryWithParentParams{Ino: 1, Name: []byte("file.txt")}).
			Return(db.LookupEntryWithParentRow{
				ParentMode:  int32(meta.S_IFDIR | 0o755),
				ParentNlink: 2,
				ChildFound:  true,
				ChildIno:    pgtype.Int8{Int64: 10, Valid: true},
				ChildMode:   pgtype.Int4{Int32: int32(meta.S_IFREG | 0o644), Valid: true},
				ChildNlink:  pgtype.Int4{Int32: 1, Valid: true},
			}, nil).
			Once()

		regInode, err := mgr.Lookup(ctx, 1, []byte("file.txt"))
		require.NoError(t, err)
		assert.Equal(t, 1.0, regInode.EntryTimeoutSeconds)
		assert.Equal(t, 1.0, regInode.AttrTimeoutSeconds)

		mockStore.EXPECT().
			LookupEntryWithParent(mock.Anything, db.LookupEntryWithParentParams{Ino: 1, Name: []byte("sub")}).
			Return(db.LookupEntryWithParentRow{
				ParentMode:  int32(meta.S_IFDIR | 0o755),
				ParentNlink: 2,
				ChildFound:  true,
				ChildIno:    pgtype.Int8{Int64: 20, Valid: true},
				ChildMode:   pgtype.Int4{Int32: int32(meta.S_IFDIR | 0o755), Valid: true},
				ChildNlink:  pgtype.Int4{Int32: 2, Valid: true},
			}, nil).
			Once()

		dirInode, err := mgr.Lookup(ctx, 1, []byte("sub"))
		require.NoError(t, err)
		assert.Equal(t, 1.0, dirInode.EntryTimeoutSeconds)
		assert.Equal(t, 1.0, dirInode.AttrTimeoutSeconds)
	})

	t.Run("默认缓存标志: Open 与 OpenDir 均不设置保留缓存标志", func(t *testing.T) {
		regRow := db.Inode{Ino: 30, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}
		dirRow := db.Inode{Ino: 40, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}

		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   30,
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      101,
				Mode:    regRow.Mode,
				Size:    regRow.Size,
				Mtime:   regRow.Mtime,
				RootIno: regRow.RootIno,
			}, nil).
			Once()

		openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: 30, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), openRes.OpenFlags)

		mockStore.EXPECT().
			OpenDirFast(mock.Anything, db.OpenDirFastParams{
				Ino:   40,
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenDirFastRow{
				Status:  "ok",
				Fh:      102,
				Nlink:   dirRow.Nlink,
				Mtime:   dirRow.Mtime,
				Ctime:   dirRow.Ctime,
				RootIno: dirRow.RootIno,
			}, nil).
			Once()

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: 40, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, uint32(0), openDirRes.OpenFlags)
	})
}

func TestMetaModule_CustomMetaConfigRuntimeBehavior(t *testing.T) {
	customCfg := config.MetaConfig{
		SessionLeaseTTLVal:        90 * time.Second,
		AtimeModeVal:              "StrictAtime",
		EntryTimeoutSecondsVal:    3.5,
		AttrTimeoutSecondsVal:     2.5,
		DirEntryTimeoutSecondsVal: 6.5,
		OpenKeepCacheVal:          true,
		OpenDirCacheVal:           true,
	}

	t.Run("通过值类型 MetaConfig 注入并验证完整运行期行为", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var mgr meta.Manager

		app := fxtest.New(
			t,
			fx.Provide(func() db.Store { return mockStore }),
			fx.Supply(customCfg),
			meta.Module,
			fx.Populate(&mgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, mgr)

		ctx := context.Background()
		mockQuerier := dbmock.NewMockQuerier(t)

		// 1. 自定义缓存超时: 普通文件 entryTimeout=3.5, attrTimeout=2.5; 目录 dirEntryTimeout=6.5
		regRow := db.Inode{Ino: 50, Mode: int32(meta.S_IFREG | 0o644), Nlink: 1}
		dirRow := db.Inode{Ino: 60, Mode: int32(meta.S_IFDIR | 0o755), Nlink: 2}

		mockStore.EXPECT().
			LookupEntryWithParent(mock.Anything, db.LookupEntryWithParentParams{Ino: 1, Name: []byte("doc.txt")}).
			Return(db.LookupEntryWithParentRow{
				ParentMode:  int32(meta.S_IFDIR | 0o755),
				ParentNlink: 2,
				ChildFound:  true,
				ChildIno:    pgtype.Int8{Int64: 50, Valid: true},
				ChildMode:   pgtype.Int4{Int32: int32(meta.S_IFREG | 0o644), Valid: true},
				ChildNlink:  pgtype.Int4{Int32: 1, Valid: true},
			}, nil).
			Once()

		regInode, err := mgr.Lookup(ctx, 1, []byte("doc.txt"))
		require.NoError(t, err)
		assert.Equal(t, 3.5, regInode.EntryTimeoutSeconds)
		assert.Equal(t, 2.5, regInode.AttrTimeoutSeconds)

		mockStore.EXPECT().
			LookupEntryWithParent(mock.Anything, db.LookupEntryWithParentParams{Ino: 1, Name: []byte("subfolder")}).
			Return(db.LookupEntryWithParentRow{
				ParentMode:  int32(meta.S_IFDIR | 0o755),
				ParentNlink: 2,
				ChildFound:  true,
				ChildIno:    pgtype.Int8{Int64: 60, Valid: true},
				ChildMode:   pgtype.Int4{Int32: int32(meta.S_IFDIR | 0o755), Valid: true},
				ChildNlink:  pgtype.Int4{Int32: 2, Valid: true},
			}, nil).
			Once()

		dirInode, err := mgr.Lookup(ctx, 1, []byte("subfolder"))
		require.NoError(t, err)
		assert.Equal(t, 6.5, dirInode.EntryTimeoutSeconds)
		assert.Equal(t, 2.5, dirInode.AttrTimeoutSeconds)

		// 2. OpenKeepCache 与 OpenDirCache
		mockStore.EXPECT().
			OpenFileFast(mock.Anything, db.OpenFileFastParams{
				Ino:   50,
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenFileFastRow{
				Status:  "ok",
				Fh:      201,
				Mode:    regRow.Mode,
				Size:    regRow.Size,
				Mtime:   regRow.Mtime,
				RootIno: regRow.RootIno,
			}, nil).
			Once()

		openRes, err := mgr.Open(ctx, meta.OpenParams{Ino: 50, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_KEEP_CACHE, openRes.OpenFlags)

		mockStore.EXPECT().
			OpenDirFast(mock.Anything, db.OpenDirFastParams{
				Ino:   60,
				Flags: int32(meta.O_RDONLY),
			}).
			Return(db.OpenDirFastRow{
				Status:  "ok",
				Fh:      202,
				Nlink:   dirRow.Nlink,
				Mtime:   dirRow.Mtime,
				Ctime:   dirRow.Ctime,
				RootIno: dirRow.RootIno,
			}, nil).
			Once()

		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{Ino: 60, Flags: meta.O_RDONLY})
		require.NoError(t, err)
		assert.Equal(t, meta.FOPEN_CACHE_DIR, openDirRes.OpenFlags)

		// 3. SessionLeaseTTL 验证
		sessID := uint64(888)
		startCall := time.Now()

		setupMockWriteTx(mockStore, mockQuerier)
		mockQuerier.EXPECT().
			DeleteExpiredClientSessions(mock.Anything).
			Return(int64(0), nil).
			Once()

		var actualExpiresAt time.Time
		mockQuerier.EXPECT().
			UpsertClientSession(mock.Anything, mock.MatchedBy(func(arg db.UpsertClientSessionParams) bool {
				if arg.SessionID == int64(sessID) && arg.ExpiresAt.Valid {
					actualExpiresAt = arg.ExpiresAt.Time
					return true
				}
				return false
			})).
			Return(db.ClientSession{
				SessionID: int64(sessID),
				ExpiresAt: pgtype.Timestamptz{Time: startCall.Add(90 * time.Second), Valid: true},
			}, nil).
			Once()

		mockQuerier.EXPECT().
			DeleteUnreferencedOrphanInodes(mock.Anything).
			Return([]db.DeleteUnreferencedOrphanInodesRow(nil), nil).
			Once()

		expiresAt, err := mgr.RenewSession(ctx, sessID)
		require.NoError(t, err)
		assert.False(t, expiresAt.IsZero())
		assert.InDelta(t, (90 * time.Second).Seconds(), actualExpiresAt.Sub(startCall).Seconds(), 2.0)

		// 4. AtimeMode 验证 (StrictAtime 下触发 TouchInodeAtime)
		linkIno := int64(70)
		targetBytes := []byte("/data/link_target")
		now := time.Now()

		mockStore.EXPECT().
			GetSymlinkTarget(mock.Anything, linkIno).
			Return(db.GetSymlinkTargetRow{
				Mode:          int32(meta.S_IFLNK | 0o777),
				SymlinkTarget: targetBytes,
			}, nil).
			Once()
		mockStore.EXPECT().
			GetInode(mock.Anything, linkIno).
			Return(db.Inode{
				Ino:   linkIno,
				Atime: pgtype.Timestamptz{Time: now.Add(-10 * time.Second), Valid: true},
				Mtime: pgtype.Timestamptz{Time: now.Add(-20 * time.Second), Valid: true},
				Ctime: pgtype.Timestamptz{Time: now.Add(-20 * time.Second), Valid: true},
			}, nil).
			Once()
		mockStore.EXPECT().
			TouchInodeAtime(mock.Anything, linkIno).
			Return(nil).
			Once()

		linkTarget, err := mgr.Readlink(ctx, uint64(linkIno))
		require.NoError(t, err)
		assert.Equal(t, targetBytes, linkTarget)
	})

	t.Run("通过指针类型 *MetaConfig 注入验证", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		var mgr meta.Manager

		cfgPtr := &customCfg

		app := fxtest.New(
			t,
			fx.Provide(func() db.Store { return mockStore }),
			fx.Supply(cfgPtr),
			meta.Module,
			fx.Populate(&mgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, mgr)

		ctx := context.Background()
		mockStore.EXPECT().
			LookupEntryWithParent(mock.Anything, db.LookupEntryWithParentParams{Ino: 1, Name: []byte("ptr_file.txt")}).
			Return(db.LookupEntryWithParentRow{
				ParentMode:  int32(meta.S_IFDIR | 0o755),
				ParentNlink: 2,
				ChildFound:  true,
				ChildIno:    pgtype.Int8{Int64: 90, Valid: true},
				ChildMode:   pgtype.Int4{Int32: int32(meta.S_IFREG | 0o644), Valid: true},
				ChildNlink:  pgtype.Int4{Int32: 1, Valid: true},
			}, nil).
			Once()

		regInode, err := mgr.Lookup(ctx, 1, []byte("ptr_file.txt"))
		require.NoError(t, err)
		assert.Equal(t, 3.5, regInode.EntryTimeoutSeconds)
		assert.Equal(t, 2.5, regInode.AttrTimeoutSeconds)
	})

	t.Run("config.Module 与 meta.Module 联动装配集成验证", func(t *testing.T) {
		mockStore := dbmock.NewMockStore(t)
		rootCfg := &config.Config{
			ServerConfig: config.ServerConfig{
				ServerHost: "127.0.0.1",
				ServerPort: 9000,
			},
			PoolConfig: config.DBPoolConfig{
				DSN: "postgres://localhost/bowl",
			},
			TelemetryConfig: config.TelemetryConfig{
				OtelServiceName: "bowl-storage",
			},
			LogConfig: config.LogConfig{
				LogLevel:  "info",
				LogFormat: "json",
			},
			Meta: config.MetaConfig{
				SessionLeaseTTLVal:        80 * time.Second,
				AtimeModeVal:              "noatime",
				EntryTimeoutSecondsVal:    4.0,
				AttrTimeoutSecondsVal:     2.0,
				DirEntryTimeoutSecondsVal: 8.0,
				OpenKeepCacheVal:          false,
				OpenDirCacheVal:           false,
			},
		}

		var mgr meta.Manager

		app := fxtest.New(
			t,
			fx.Provide(func() db.Store { return mockStore }),
			fx.Supply(rootCfg),
			config.Module,
			meta.Module,
			fx.Populate(&mgr),
		)
		require.NoError(t, app.Err())
		require.NotNil(t, mgr)

		ctx := context.Background()
		mockStore.EXPECT().
			LookupEntryWithParent(mock.Anything, db.LookupEntryWithParentParams{Ino: 1, Name: []byte("integ_dir")}).
			Return(db.LookupEntryWithParentRow{
				ParentMode:  int32(meta.S_IFDIR | 0o755),
				ParentNlink: 2,
				ChildFound:  true,
				ChildIno:    pgtype.Int8{Int64: 110, Valid: true},
				ChildMode:   pgtype.Int4{Int32: int32(meta.S_IFDIR | 0o755), Valid: true},
				ChildNlink:  pgtype.Int4{Int32: 2, Valid: true},
			}, nil).
			Once()

		dirInode, err := mgr.Lookup(ctx, 1, []byte("integ_dir"))
		require.NoError(t, err)
		assert.Equal(t, 8.0, dirInode.EntryTimeoutSeconds)
		assert.Equal(t, 2.0, dirInode.AttrTimeoutSeconds)
	})
}
