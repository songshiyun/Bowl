package meta

import (
	"bytes"
	"context"
	"log/slog"
	"math"
	"sync"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/pebblekv"
)

// Manager 提供分支存储与控制面操作。
type Manager interface {
	Lookup(context.Context, uint64, []byte) (*Inode, error)
	GetAttr(context.Context, uint64) (*Inode, error)
	SetAttr(context.Context, uint64, SetAttrInput) (*Inode, error)
	Mknod(context.Context, MknodParams) (*Inode, error)
	Mkdir(context.Context, MkdirParams) (*Inode, error)
	Symlink(context.Context, SymlinkParams) (*Inode, error)
	Readlink(context.Context, uint64) ([]byte, error)
	Link(context.Context, LinkParams) (*Inode, error)
	Unlink(context.Context, uint64, []byte) error
	Rmdir(context.Context, uint64, []byte) error
	Rename(context.Context, RenameParams) error
	ReclaimOrphan(context.Context, ReclaimOrphanParams) error
	Create(context.Context, CreateParams) (*CreateResult, error)
	Fsync(context.Context, FsyncParams) error
	FsyncDir(context.Context, FsyncDirParams) error
	ReadDir(context.Context, ReadDirParams) ([]DirEntry, error)
	ReadDirPlus(context.Context, ReadDirPlusParams) ([]DirEntryPlus, error)
	Fallocate(context.Context, FallocateParams) error
	SetXattr(context.Context, SetXattrParams) error
	GetXattr(context.Context, uint64, []byte) ([]byte, error)
	ListXattr(context.Context, uint64) ([][]byte, error)
	RemoveXattr(context.Context, uint64, []byte) error
	RenewBranchSession(context.Context, RenewBranchSessionParams) (RenewSessionResult, error)
	CloseSession(context.Context, uint64) error
	CreateDisk(context.Context, CreateDiskParams) (*Disk, error)
	GetDisk(context.Context, string) (*Disk, error)
	ListDisks(context.Context, ListDisksParams) (*ListDisksResult, error)
	UpdateDisk(context.Context, UpdateDiskParams) (*Disk, error)
	DeleteDisk(context.Context, string) error
	CreateBranch(context.Context, CreateBranchParams) (*Branch, error)
	ListBranches(context.Context, string) ([]Branch, error)
	DeleteBranch(context.Context, DeleteBranchParams) error
	Commit(context.Context, CommitParams) (*CommitInfo, error)
	Merge(context.Context, MergeParams) (*MergeResult, error)
	ResolveMerge(context.Context, ResolveMergeParams) (*CommitInfo, error)
	AbortMerge(context.Context, AbortMergeParams) error
	Rollback(context.Context, RollbackParams) error
	Diff(context.Context, DiffParams) ([]DiffEntry, error)
	StatFS(context.Context, uint64) (*StatFSResult, error)
	TreeQuota(context.Context, string) (*TreeQuota, error)
	Write(context.Context, *WriteRequest) (*WriteResponse, error)
	Read(context.Context, *ReadRequest) (*ReadResponse, error)
}

type manager struct {
	store                                      db.Store
	storageRoot                                string
	pebble                                     *pebblekv.Manager
	entryTimeout, attrTimeout, dirEntryTimeout float64
	openKeepCache                              bool
	atimeMode                                  AtimeMode
	sessionLeaseTTL                            time.Duration
	branchLocks                                sync.Map
	writeQuotaMu                               sync.Mutex
	treeQuotaMu                                sync.Mutex
	treeQuotas                                 map[string]*TreeQuota
	dirCursorMu                                sync.Mutex
	pebbleDirCursors                           map[pebbleDirCursorKey][]byte
}

type poolProvider interface{ Pool() *pgxpool.Pool }

func (m *manager) hasPool() bool {
	p, ok := m.store.(poolProvider)
	return ok && p != nil && p.Pool() != nil
}

// Option 配置存储管理器。
type Option func(*manager)

func WithStorageRoot(root string) Option { return func(m *manager) { m.storageRoot = root } }
func WithSessionLeaseTTL(ttl time.Duration) Option {
	return func(m *manager) {
		if ttl > 0 {
			m.sessionLeaseTTL = ttl
		}
	}
}
func WithEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.entryTimeout = d
		}
	}
}
func WithAttrTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.attrTimeout = d
		}
	}
}
func WithDirEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.dirEntryTimeout = d
		}
	}
}
func WithOpenKeepCache(enable bool) Option { return func(m *manager) { m.openKeepCache = enable } }
func WithAtimeMode(mode AtimeMode) Option  { return func(m *manager) { m.atimeMode = mode } }

// WithPebbleManager 复用外部创建的 Pebble 句柄池；同一存储根目录必须共用一个句柄池。
func WithPebbleManager(pool *pebblekv.Manager) Option {
	return func(m *manager) {
		if pool != nil {
			m.pebble = pool
		}
	}
}

// New 创建存储管理器。
func New(store db.Store, opts ...Option) Manager {
	m := &manager{store: store, storageRoot: config.DefaultDataDir,
		entryTimeout: DefaultEntryTimeoutSeconds, attrTimeout: DefaultAttrTimeoutSeconds,
		dirEntryTimeout: DefaultEntryTimeoutSeconds, sessionLeaseTTL: DefaultSessionLeaseTTL}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.pebble == nil {
		m.pebble = pebblekv.NewManager(m.storageRoot)
	}
	return m
}

func validateEntryName(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if len(name) == 0 || bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}
func validateEntryNameAllowEmpty(name []byte) error {
	if len(name) > MaxFileNameBytes {
		return ErrNameTooLong
	}
	if bytes.IndexByte(name, '/') >= 0 || bytes.IndexByte(name, 0) >= 0 {
		return ErrInvalidArgument
	}
	return nil
}
func isDotOrDotDot(name []byte) bool {
	return bytes.Equal(name, []byte(".")) || bytes.Equal(name, []byte(".."))
}
func validateOptionalSessionID(id *uint64) error {
	if id != nil && (*id == 0 || *id > math.MaxInt64) {
		return ErrInvalidArgument
	}
	return nil
}
func (m *manager) shouldTouchAtime(atime, mtime, ctime, now time.Time) bool {
	switch m.atimeMode {
	case AtimeModeStrictAtime:
		return now.Sub(atime) >= time.Second
	case AtimeModeRelAtime:
		return atime.Before(mtime) || atime.Before(ctime) || now.Sub(atime) >= 24*time.Hour
	default:
		return false
	}
}
func (m *manager) execSyncWriteTxWithRetry(ctx context.Context, fn func(db.Querier) error) error {
	return tx.Retry(ctx, func(ctx context.Context) error { return m.store.ExecTx(ctx, fn) }, tx.WithDefaultRetry())
}

// CloseSession 注销分支会话并回收无写端的孤儿节点。
func (m *manager) CloseSession(ctx context.Context, sessionID uint64) error {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return ErrInvalidArgument
	}
	if err := m.sweepExpiredBranchSessions(ctx); err != nil {
		return err
	}
	_, err := m.closeBranchSession(ctx, sessionID)
	if err != nil {
		return err
	}
	m.clearSessionCursors(sessionID)
	return nil
}

func (m *manager) clearSessionCursors(sessionID uint64) {
	m.dirCursorMu.Lock()
	defer m.dirCursorMu.Unlock()
	for key := range m.pebbleDirCursors {
		if key.sessionID == sessionID {
			delete(m.pebbleDirCursors, key)
		}
	}
}

type ManagerParams struct {
	fx.In
	Lifecycle fx.Lifecycle `optional:"true"`
	Store     db.Store
	MetaCfg   *config.MetaConfig `optional:"true"`
	MetaVal   config.MetaConfig  `optional:"true"`
}

func newManagerFromParams(params ManagerParams) Manager {
	cfg := params.MetaCfg
	if cfg == nil && params.MetaVal != (config.MetaConfig{}) {
		cfg = &params.MetaVal
	}
	var opts []Option
	interval := config.DefaultSessionSweepInterval
	if cfg != nil {
		mode := AtimeModeNoAtime
		switch cfg.AtimeMode() {
		case "relatime":
			mode = AtimeModeRelAtime
		case "strictatime":
			mode = AtimeModeStrictAtime
		}
		opts = append(opts, WithStorageRoot(cfg.DataDir()), WithSessionLeaseTTL(cfg.SessionLeaseTTL()), WithAtimeMode(mode),
			WithEntryTimeout(cfg.EntryTimeoutSeconds()), WithAttrTimeout(cfg.AttrTimeoutSeconds()),
			WithDirEntryTimeout(cfg.DirEntryTimeoutSeconds()), WithOpenKeepCache(cfg.OpenKeepCache()))
		interval = cfg.SessionSweepInterval()
	}
	m := New(params.Store, opts...).(*manager)
	if params.Lifecycle != nil {
		var cancel context.CancelFunc
		var done chan struct{}
		params.Lifecycle.Append(fx.Hook{
			OnStart: func(context.Context) error {
				var ctx context.Context
				ctx, cancel = context.WithCancel(context.Background())
				done = make(chan struct{})
				go func() {
					defer close(done)
					ticker := time.NewTicker(interval)
					defer ticker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-ticker.C:
							if err := m.sweepExpiredBranchSessions(ctx); err != nil && ctx.Err() == nil {
								slog.Error("清理过期分支会话失败", "error", err)
							}
						}
					}
				}()
				return nil
			},
			OnStop: func(ctx context.Context) error {
				if cancel != nil {
					cancel()
				}
				if done != nil {
					select {
					case <-done:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
				return m.pebble.Close()
			},
		})
	}
	return m
}

// Module 提供元数据管理器。
var Module = fx.Module("meta", fx.Provide(newManagerFromParams))
