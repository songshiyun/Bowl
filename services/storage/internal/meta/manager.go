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
	Lookup(context.Context, *uint64, uint64, []byte) (*Inode, error)
	GetAttr(context.Context, *uint64, uint64) (*Inode, error)
	SetAttr(context.Context, uint64, SetAttrInput) (*Inode, error)
	Mknod(context.Context, MknodParams) (*Inode, error)
	Mkdir(context.Context, MkdirParams) (*Inode, error)
	Symlink(context.Context, SymlinkParams) (*Inode, error)
	Readlink(context.Context, *uint64, uint64) ([]byte, error)
	Link(context.Context, LinkParams) (*Inode, error)
	Unlink(context.Context, *uint64, uint64, []byte, bool) error
	Rmdir(context.Context, *uint64, uint64, []byte) error
	Rename(context.Context, RenameParams) error
	ReclaimOrphan(context.Context, ReclaimOrphanParams) error
	Create(context.Context, CreateParams) (*CreateResult, error)
	Fsync(context.Context, FsyncParams) error
	FsyncDir(context.Context, FsyncDirParams) error
	ReadDir(context.Context, ReadDirParams) ([]DirEntry, error)
	ReadDirPlus(context.Context, ReadDirPlusParams) ([]DirEntryPlus, error)
	Fallocate(context.Context, FallocateParams) error
	SetXattr(context.Context, SetXattrParams) error
	GetXattr(context.Context, *uint64, uint64, []byte) ([]byte, error)
	ListXattr(context.Context, *uint64, uint64) ([][]byte, error)
	RemoveXattr(context.Context, *uint64, uint64, []byte) error
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
	StatFS(context.Context, *uint64, uint64) (*StatFSResult, error)
	TreeQuota(context.Context, string) (*TreeQuota, error)
	Write(context.Context, *WriteRequest) (*WriteResponse, error)
	Read(context.Context, *ReadRequest) (*ReadResponse, error)
}

type manager struct {
	store                                      db.Store
	storageRoot                                string
	pebble                                     *pebblekv.Manager
	locks                                      *BranchLockTable
	entryTimeout, attrTimeout, dirEntryTimeout float64
	openKeepCache                              bool
	atimeMode                                  AtimeMode
	sessionLeaseTTL                            time.Duration
	writeQuotaMu                               sync.Mutex
	treeQuotaMu                                sync.Mutex
	treeQuotas                                 map[string]*TreeQuota
	dirCursorMu                                sync.Mutex
	pebbleDirCursors                           map[pebbleDirCursorKey][]byte
	atimes                                     *PebbleAtimeTracker
	gate                                       *BranchWorkspaceGate
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

// WithBranchLockTable 注入自定义分支与节点锁表。
func WithBranchLockTable(locks *BranchLockTable) Option {
	return func(m *manager) {
		if locks != nil {
			m.locks = locks
		}
	}
}

// WithBranchWorkspaceGate 注入自定义分支工作区会话与访问时间网关。
func WithBranchWorkspaceGate(gate *BranchWorkspaceGate) Option {
	return func(m *manager) {
		if gate != nil {
			m.gate = gate
		}
	}
}

// New 创建存储管理器。
func New(store db.Store, opts ...Option) Manager {
	m := &manager{store: store, storageRoot: config.DefaultDataDir,
		entryTimeout: DefaultEntryTimeoutSeconds, attrTimeout: DefaultAttrTimeoutSeconds,
		dirEntryTimeout: DefaultEntryTimeoutSeconds, sessionLeaseTTL: DefaultSessionLeaseTTL,
		locks: NewBranchLockTable()}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.pebble == nil {
		m.pebble = pebblekv.NewManager(m.storageRoot)
	}
	m.atimes = NewPebbleAtimeTracker(m.atimeMode, m.pebble, m.locks, m.storageRoot)
	if m.gate == nil {
		m.gate = NewBranchWorkspaceGate(m.store, m.pebble, m.locks, m.atimes, defaultSessionCacheTTL)
	} else {
		m.gate.bindDependencies(m.store, m.pebble, m.locks, m.atimes)
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
func (m *manager) execSyncWriteTxWithRetry(ctx context.Context, fn func(db.Querier) error) error {
	return tx.Retry(ctx, func(ctx context.Context) error { return m.store.ExecTx(ctx, fn) }, tx.WithDefaultRetry())
}

// CloseSession 注销分支会话并回收无写端的孤儿节点。
func (m *manager) CloseSession(ctx context.Context, sessionID uint64) error {
	if sessionID == 0 || sessionID > math.MaxInt64 {
		return ErrInvalidArgument
	}
	_, err := m.closeBranchSession(ctx, sessionID)
	// 无论清扫是否成功都驱逐缓存：写端租约已在事务内失效，旧缓存必须立即失效。
	m.invalidatePebbleSession(sessionID)
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
					sessionTicker := time.NewTicker(interval)
					defer sessionTicker.Stop()
					// 访问时间延迟合并：读热路径只登记内存脏项，由 1 秒周期协程刷入 Pebble。
					atimeTicker := time.NewTicker(defaultAtimeFlushInterval)
					defer atimeTicker.Stop()
					for {
						select {
						case <-ctx.Done():
							return
						case <-sessionTicker.C:
							if err := m.sweepExpiredBranchSessions(ctx); err != nil && ctx.Err() == nil {
								slog.Error("清理过期分支会话失败", "error", err)
							}
						case <-atimeTicker.C:
							if err := m.atimes.FlushAll(ctx); err != nil && ctx.Err() == nil {
								slog.Error("合并访问时间失败", "error", err)
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
				// 停机前把内存中尚未合并的访问时间刷入 Pebble。
				if err := m.atimes.FlushAll(context.WithoutCancel(ctx)); err != nil {
					return err
				}
				return m.pebble.Close()
			},
		})
	}
	return m
}

// Module 提供元数据管理器。
var Module = fx.Module("meta", fx.Provide(newManagerFromParams))
