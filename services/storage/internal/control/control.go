// Package control 实现单盘 StoreActor 控制面管理、MountDisk 世代号颁发与实例池路由。
package control

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"

	"github.com/starsrc/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/config"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

const controlInstrumentationName = "github.com/starsrc/bowl/services/storage/internal/control"

// 控制面常量定义。
const (
	RootIno                       uint64  = store.RootIno
	MaxDiskNameBytes                      = 255
	DefaultListDisksPageSize      uint32  = 50
	MaxListDisksPageSize          uint32  = 1000
	DefaultEntryTimeoutSeconds    float64 = 1.0
	DefaultAttrTimeoutSeconds     float64 = 1.0
	DefaultDirEntryTimeoutSeconds float64 = 1.0
	DefaultSharedCacheSize                = store.DefaultCacheSize // 64 MiB

	segmentReserveTimeout = 5 * time.Second
)

// 领域哨兵错误（与 internal/store 哨兵错误对齐，支持双向 errors.Is 判定）。
var (
	ErrInvalidArgument = store.ErrInvalid
	ErrNameTooLong     = store.ErrNameTooLong
	ErrNotFound        = store.ErrNotFound
	ErrInvalidShrink   = store.ErrQuotaShrinkBelowUsed
	ErrStaleEpoch      = store.ErrStaleEpoch
	ErrClosed          = store.ErrClosed
	ErrAlreadyExists   = store.ErrExist
	ErrNotDir          = store.ErrNotDir
	ErrIsDir           = store.ErrIsDir
	ErrNotEmpty        = store.ErrNotEmpty
	ErrPerm            = store.ErrPerm
	ErrNoData          = store.ErrNoData
	ErrNoSpace         = store.ErrNoSpace
	ErrNotSupported    = store.ErrNotSupported
	ErrRange           = store.ErrRange
	ErrTooManyLinks    = store.ErrTooManyLinks
	ErrFileTooLarge    = store.ErrFileTooLarge
	ErrIO              = store.ErrIO
)

// AtimeMode 复用 store.AtimeMode。
type AtimeMode = store.AtimeMode

// 访问时间更新策略常量。
const (
	AtimeModeNoAtime     = store.AtimeModeNoAtime
	AtimeModeRelAtime    = store.AtimeModeRelAtime
	AtimeModeStrictAtime = store.AtimeModeStrictAtime
)

// Disk 描述单盘控制面领域实体。
type Disk struct {
	DiskID       string
	ParentDiskID string
	Name         string
	RootIno      uint64
	MaxBytes     uint64
	UsedBytes    uint64
	UsedInodes   uint64
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// CreateDiskParams 描述创建虚拟磁盘入参。
type CreateDiskParams struct {
	Name     string
	MaxBytes uint64
}

// ForkDiskParams 描述基于已有虚拟磁盘零拷贝派生新盘的入参。
type ForkDiskParams struct {
	SourceDiskID string
	Name         string
	MaxBytes     *uint64
}

// ListDisksParams 描述分页查询虚拟磁盘列表入参。
type ListDisksParams struct {
	PageSize  uint32
	PageToken string
}

// ListDisksResult 描述分页查询虚拟磁盘列表返回结果。
type ListDisksResult struct {
	Disks         []*Disk
	NextPageToken string
}

// UpdateDiskParams 描述更新虚拟磁盘入参。
type UpdateDiskParams struct {
	DiskID   string
	Name     *string
	MaxBytes *uint64
}

// MountDiskParams 描述挂载虚拟磁盘握手入参。
type MountDiskParams struct {
	DiskID   string
	ReadOnly bool
}

// MountDiskResult 描述挂载虚拟磁盘握手返回结果。
type MountDiskResult struct {
	Epoch               uint64
	RootAttr            store.Attribute
	EntryTimeoutSeconds float64
	AttrTimeoutSeconds  float64
}

// Manager 定义单盘控制面与 StoreActor 实例池管理接口。
type Manager interface {
	CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error)
	ForkDisk(ctx context.Context, params ForkDiskParams) (*Disk, error)
	GetDisk(ctx context.Context, diskID string) (*Disk, error)
	ListDisks(ctx context.Context, params ListDisksParams) (*ListDisksResult, error)
	UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error)
	DeleteDisk(ctx context.Context, diskID string) error
	MountDisk(ctx context.Context, params MountDiskParams) (*MountDiskResult, error)
	Actor(ctx context.Context, diskID string) (*store.StoreActor, error)
	Collector() prometheus.Collector

	EntryTimeoutSeconds() float64
	AttrTimeoutSeconds() float64
	DirEntryTimeoutSeconds() float64
	EntryTimeoutForMode(mode uint32) float64
	OpenKeepCache() bool
	OpenDirCache() bool
	AtimeMode() AtimeMode
	StorageRoot() string
	Close() error
}

type diskLockEntry struct {
	mu   sync.Mutex
	refs int
}

type manager struct {
	store                                      db.Store
	storageRoot                                string
	entryTimeout, attrTimeout, dirEntryTimeout float64
	openKeepCache                              bool
	openDirCache                               bool
	atimeMode                                  AtimeMode

	baseLogger   *slog.Logger
	logger       *slog.Logger
	storeMetrics *store.Metrics
	collector    *storageEngineCollector
	removeAll    func(string) error

	cache *pebble.Cache

	diskLocksMu sync.Mutex
	diskLocks   map[string]*diskLockEntry

	poolMu sync.RWMutex
	closed bool
	actors map[string]*store.StoreActor
}

// Option 配置单盘控制面管理器。
type Option func(*manager)

// WithStorageRoot 配置磁盘存储根目录。
func WithStorageRoot(root string) Option {
	return func(m *manager) {
		if root != "" {
			m.storageRoot = root
		}
	}
}

// WithEntryTimeout 配置非目录条目缓存时效（秒）。
func WithEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.entryTimeout = d
		}
	}
}

// WithAttrTimeout 配置节点属性缓存时效（秒）。
func WithAttrTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.attrTimeout = d
		}
	}
}

// WithDirEntryTimeout 配置目录条目缓存时效（秒）。
func WithDirEntryTimeout(d float64) Option {
	return func(m *manager) {
		if d >= 0 {
			m.dirEntryTimeout = d
		}
	}
}

// WithOpenKeepCache 配置打开文件时是否下发 FOPEN_KEEP_CACHE。
func WithOpenKeepCache(enable bool) Option {
	return func(m *manager) { m.openKeepCache = enable }
}

// WithOpenDirCache 配置打开目录时是否保留内核目录缓存。
func WithOpenDirCache(enable bool) Option {
	return func(m *manager) { m.openDirCache = enable }
}

// WithAtimeMode 配置访问时间更新策略。
func WithAtimeMode(mode AtimeMode) Option {
	return func(m *manager) { m.atimeMode = mode }
}

// WithLogger 配置控制面与下属 StoreActor 共享的结构化日志器（nil 时保持默认 slog.Default()）。
func WithLogger(logger *slog.Logger) Option {
	return func(m *manager) {
		if logger != nil {
			m.baseLogger = logger
			m.logger = logger.With("component", "control")
		}
	}
}

// WithStoreMetrics 配置下属 StoreActor 共享的热路径指标收集器（nil 时保持默认隔离实例）。
func WithStoreMetrics(metrics *store.Metrics) Option {
	return func(m *manager) {
		if metrics != nil {
			m.storeMetrics = metrics
		}
	}
}

func withEngineCollector(collector *storageEngineCollector) Option {
	return func(m *manager) {
		if collector != nil {
			m.collector = collector
		}
	}
}

// New 创建单盘控制面管理器。
func New(dbStore db.Store, opts ...Option) Manager {
	return newManager(dbStore, opts...)
}

func newManager(dbStore db.Store, opts ...Option) *manager {
	baseLogger := slog.Default()
	m := &manager{
		store:           dbStore,
		storageRoot:     config.DefaultDataDir,
		entryTimeout:    DefaultEntryTimeoutSeconds,
		attrTimeout:     DefaultAttrTimeoutSeconds,
		dirEntryTimeout: DefaultDirEntryTimeoutSeconds,
		baseLogger:      baseLogger,
		logger:          baseLogger.With("component", "control"),
		removeAll:       os.RemoveAll,
		cache:           pebble.NewCache(DefaultSharedCacheSize),
		diskLocks:       make(map[string]*diskLockEntry),
		actors:          make(map[string]*store.StoreActor),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(m)
		}
	}
	if m.storeMetrics == nil {
		m.storeMetrics = store.NewMetrics()
	}
	if m.collector == nil {
		m.collector = newStorageEngineCollector()
	}
	m.collector.bind(m)
	return m
}

func (m *manager) Collector() prometheus.Collector {
	if m == nil || m.collector == nil {
		return newStorageEngineCollector()
	}
	return m.collector
}

func (m *manager) EntryTimeoutSeconds() float64    { return m.entryTimeout }
func (m *manager) AttrTimeoutSeconds() float64     { return m.attrTimeout }
func (m *manager) DirEntryTimeoutSeconds() float64 { return m.dirEntryTimeout }
func (m *manager) EntryTimeoutForMode(mode uint32) float64 {
	if mode&store.S_IFMT == store.S_IFDIR {
		return m.dirEntryTimeout
	}
	return m.entryTimeout
}
func (m *manager) OpenKeepCache() bool { return m.openKeepCache }
func (m *manager) OpenDirCache() bool  { return m.openDirCache }
func (m *manager) AtimeMode() AtimeMode {
	return m.atimeMode
}
func (m *manager) StorageRoot() string { return m.storageRoot }

func (m *manager) diskDir(diskID string) string {
	return filepath.Join(m.storageRoot, "disks", diskID)
}

func (m *manager) commitDir(commitID string) string {
	return filepath.Join(m.storageRoot, "commits", commitID)
}

func (m *manager) newSegmentAllocators(lineageID pgtype.UUID) (store.InodeSegmentAllocator, store.PackSegmentAllocator) {
	if m.store == nil || !lineageID.Valid {
		return nil, nil
	}
	inoAlloc := func(minNext uint64) (uint64, uint64, error) {
		if minNext < 2 {
			minNext = 2
		}
		if minNext > uint64(math.MaxInt64) {
			return 0, 0, store.ErrInodeExhausted
		}
		count := store.InodeSegment
		if rem := uint64(math.MaxInt64) - minNext; rem < count {
			if rem == 0 {
				return 0, 0, store.ErrInodeExhausted
			}
			count = rem
		}

		allocCtx, cancel := context.WithTimeout(context.Background(), segmentReserveTimeout)
		defer cancel()

		var row db.ReserveInodeSegmentRow
		err := tx.Retry(allocCtx, func(attemptCtx context.Context) error {
			var rErr error
			row, rErr = m.store.ReserveInodeSegment(attemptCtx, db.ReserveInodeSegmentParams{
				MinNext:   int64(minNext),
				Count:     int64(count),
				LineageID: lineageID,
			})
			return rErr
		}, tx.WithDefaultRetry())
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, 0, ErrNotFound
			}
			return 0, 0, fmt.Errorf("预留 Inode 号段: %w", err)
		}
		if row.FirstIno < 2 || row.LastIno < row.FirstIno {
			return 0, 0, store.ErrInodeExhausted
		}
		return uint64(row.FirstIno), uint64(row.LastIno), nil
	}

	packAlloc := func(minNext uint64) (uint64, uint64, error) {
		if minNext < 1 {
			minNext = 1
		}
		if minNext > uint64(math.MaxInt64) {
			return 0, 0, store.ErrNoSpace
		}
		count := store.PackSegment
		if rem := uint64(math.MaxInt64) - minNext; rem < count {
			if rem == 0 {
				return 0, 0, store.ErrNoSpace
			}
			count = rem
		}

		allocCtx, cancel := context.WithTimeout(context.Background(), segmentReserveTimeout)
		defer cancel()

		var row db.ReservePackSegmentRow
		err := tx.Retry(allocCtx, func(attemptCtx context.Context) error {
			var rErr error
			row, rErr = m.store.ReservePackSegment(attemptCtx, db.ReservePackSegmentParams{
				MinNext:   int64(minNext),
				Count:     int64(count),
				LineageID: lineageID,
			})
			return rErr
		}, tx.WithDefaultRetry())
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return 0, 0, ErrNotFound
			}
			return 0, 0, fmt.Errorf("预留 Pack 号段: %w", err)
		}
		if row.FirstPackID < 1 || row.LastPackID < row.FirstPackID {
			return 0, 0, store.ErrNoSpace
		}
		return uint64(row.FirstPackID), uint64(row.LastPackID), nil
	}
	return inoAlloc, packAlloc
}

func (m *manager) lockDisk(diskID string) func() {
	m.diskLocksMu.Lock()
	entry := m.diskLocks[diskID]
	if entry == nil {
		entry = &diskLockEntry{}
		m.diskLocks[diskID] = entry
	}
	entry.refs++
	m.diskLocksMu.Unlock()

	entry.mu.Lock()
	return func() {
		entry.mu.Unlock()
		m.diskLocksMu.Lock()
		entry.refs--
		if entry.refs == 0 {
			delete(m.diskLocks, diskID)
		}
		m.diskLocksMu.Unlock()
	}
}

func (m *manager) execSyncWriteTxWithRetry(ctx context.Context, fn func(context.Context, db.Querier) error) error {
	return tx.Retry(ctx, func(attemptCtx context.Context) error {
		return m.store.ExecTx(attemptCtx, func(q db.Querier) error {
			return fn(attemptCtx, q)
		})
	}, tx.WithDefaultRetry())
}

// validateDiskName 校验磁盘名称合法性（优先校验超长 > 255B，后校验空字节 '\x00'）。
func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("%w: disk name length %d exceeds max %d", ErrNameTooLong, len(name), MaxDiskNameBytes)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: disk name contains null byte", ErrInvalidArgument)
	}
	return nil
}

// parseDiskID 解析字符串形式的 UUID 为 pgtype.UUID，并拦截非法格式或全零 Nil UUID。
func parseDiskID(diskID string) (pgtype.UUID, error) {
	var id pgtype.UUID
	if err := id.Scan(diskID); err != nil || !id.Valid {
		return pgtype.UUID{}, fmt.Errorf("%w: invalid disk id %q", ErrInvalidArgument, diskID)
	}
	if id.Bytes == [16]byte{} {
		return pgtype.UUID{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	return id, nil
}

func assembleDisk(row *db.Disk, stat *store.StatFSResult) *Disk {
	var diskID, parentDiskID string
	if row.DiskID.Valid {
		diskID = row.DiskID.String()
	}
	if row.ParentDiskID.Valid {
		parentDiskID = row.ParentDiskID.String()
	}
	return &Disk{
		DiskID:       diskID,
		ParentDiskID: parentDiskID,
		Name:         row.Name,
		RootIno:      RootIno,
		MaxBytes:     stat.TotalBytes,
		UsedBytes:    stat.UsedBytes,
		UsedInodes:   stat.Files,
		CreatedAt:    row.CreatedAt.Time,
		UpdatedAt:    row.UpdatedAt.Time,
	}
}

//nolint:gocritic // hugeParam: store.Options 按值传递保持与 store.Open 构造契约一致。
func (m *manager) openDiskActor(ctx context.Context, opts store.Options) (*store.StoreActor, error) {
	var span trace.Span
	if ctx != nil && trace.SpanFromContext(ctx).IsRecording() {
		_, span = otel.GetTracerProvider().Tracer(controlInstrumentationName).Start(ctx, "control.OpenDiskActor",
			trace.WithAttributes(
				attribute.String("disk.id", opts.DiskID),
				attribute.Bool("disk.read_only", opts.ReadOnly),
			),
		)
	}
	openStart := time.Now()
	actor, err := store.Open(opts)
	m.collector.observeOpenActor(err == nil, time.Since(openStart))
	if span != nil {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
	return actor, err
}

func (m *manager) getOrOpenActorLocked(ctx context.Context, diskID string) (*store.StoreActor, error) {
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	var lineageID pgtype.UUID
	if m.store != nil {
		pgID, err := parseDiskID(diskID)
		if err != nil {
			return nil, err
		}
		row, err := m.store.GetDisk(ctx, pgID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return nil, ErrNotFound
			}
			return nil, err
		}
		lineageID = row.LineageID
	}
	return m.getOrOpenActorWithLineageLocked(ctx, diskID, lineageID)
}

func (m *manager) getOrOpenActorWithLineageLocked(ctx context.Context, diskID string, lineageID pgtype.UUID) (*store.StoreActor, error) {
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	cache := m.cache
	m.poolMu.RUnlock()

	inoAlloc, packAlloc := m.newSegmentAllocators(lineageID)
	actor, err := m.openDiskActor(ctx, store.Options{
		Dir:           m.diskDir(diskID),
		DiskID:        diskID,
		AtimeMode:     m.atimeMode,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
	})
	if err != nil {
		return nil, err
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		if closeErr := actor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "actor close failed after manager closed",
				"disk_id", diskID,
				"mode", actorModeLabel(actor.ReadOnly()),
				"err", closeErr,
			)
		}
		return nil, ErrClosed
	}
	if existing := m.actors[diskID]; existing != nil {
		m.poolMu.Unlock()
		if closeErr := actor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "actor race loser cleanup failed",
				"disk_id", diskID,
				"mode", actorModeLabel(actor.ReadOnly()),
				"err", closeErr,
			)
		}
		return existing, nil
	}
	m.actors[diskID] = actor
	m.poolMu.Unlock()

	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorLoad(actor.ReadOnly())
	m.logger.InfoContext(ctx, "actor loaded",
		"disk_id", diskID,
		"epoch", actor.WriterEpoch(),
		"mode", mode,
	)
	return actor, nil
}

func (m *manager) evictIdleActor(ctx context.Context, diskID string) (bool, error) {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return false, err
	}
	diskID = pgID.String()

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		return false, ErrClosed
	}
	actor := m.actors[diskID]
	if actor == nil {
		m.poolMu.Unlock()
		return false, nil
	}
	delete(m.actors, diskID)
	m.poolMu.Unlock()

	epoch := actor.WriterEpoch()
	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorEviction()

	if closeErr := actor.Close(); closeErr != nil {
		m.collector.incCleanupError()
		m.logger.WarnContext(ctx, "evict idle actor close failed",
			"disk_id", diskID,
			"epoch", epoch,
			"mode", mode,
			"err", closeErr,
		)
		return true, fmt.Errorf("驱逐空闲磁盘存储实例: %w", closeErr)
	}

	m.logger.InfoContext(ctx, "actor evicted",
		"disk_id", diskID,
		"epoch", epoch,
		"mode", mode,
	)
	return true, nil
}

func (m *manager) CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error) {
	if params.MaxBytes == 0 || params.MaxBytes > math.MaxInt64 {
		return nil, ErrInvalidArgument
	}
	if err := validateDiskName(params.Name); err != nil {
		return nil, err
	}
	id, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	diskID := id.String()
	pgID := pgtype.UUID{Bytes: id, Valid: true}

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	cache := m.cache
	m.poolMu.RUnlock()

	dir := m.diskDir(diskID)
	inoAlloc, packAlloc := m.newSegmentAllocators(pgID)
	actor, err := m.openDiskActor(ctx, store.Options{
		Dir:           dir,
		DiskID:        diskID,
		MaxBytes:      params.MaxBytes,
		AtimeMode:     m.atimeMode,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
	})
	if err != nil {
		return nil, fmt.Errorf("初始化磁盘存储: %w", err)
	}

	var row db.Disk
	err = m.execSyncWriteTxWithRetry(ctx, func(txCtx context.Context, q db.Querier) error {
		if _, linErr := q.CreateDiskLineage(txCtx, pgID); linErr != nil {
			return linErr
		}
		var createErr error
		row, createErr = q.CreateDisk(txCtx, db.CreateDiskParams{
			DiskID:    pgID,
			Name:      params.Name,
			LineageID: pgID,
		})
		return createErr
	})
	if err != nil {
		if closeErr := actor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create disk rollback close failed",
				"disk_id", diskID,
				"err", closeErr,
			)
		}
		if removeErr := m.removeAll(dir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create disk rollback remove dir failed",
				"disk_id", diskID,
				"dir", dir,
				"err", removeErr,
			)
		}
		return nil, fmt.Errorf("创建磁盘: %w", err)
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		if closeErr := actor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "create disk close failed after manager closed",
				"disk_id", diskID,
				"err", closeErr,
			)
		}
		return nil, ErrClosed
	}
	m.actors[diskID] = actor
	m.poolMu.Unlock()

	mode := actorModeLabel(actor.ReadOnly())
	m.collector.incActorLoad(actor.ReadOnly())

	stat, err := actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk created",
		"disk_id", diskID,
		"name", params.Name,
		"max_bytes", params.MaxBytes,
		"epoch", actor.WriterEpoch(),
		"mode", mode,
	)
	return assembleDisk(&row, &stat), nil
}

func (m *manager) ForkDisk(ctx context.Context, params ForkDiskParams) (*Disk, error) {
	sourcePGID, err := parseDiskID(params.SourceDiskID)
	if err != nil {
		return nil, err
	}
	sourceDiskID := sourcePGID.String()

	err = validateDiskName(params.Name)
	if err != nil {
		return nil, err
	}
	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}

	unlockSource := m.lockDisk(sourceDiskID)
	defer unlockSource()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	cache := m.cache
	m.poolMu.RUnlock()

	sourceRow, err := m.store.GetDisk(ctx, sourcePGID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	sourceActor, err := m.getOrOpenActorWithLineageLocked(ctx, sourceDiskID, sourceRow.LineageID)
	if err != nil {
		return nil, err
	}

	if params.MaxBytes != nil {
		hasOrphans, orphanErr := sourceActor.HasOrphans()
		if orphanErr != nil {
			return nil, orphanErr
		}
		if !hasOrphans {
			srcStat, statErr := sourceActor.StatFS(ctx)
			if statErr != nil {
				return nil, statErr
			}
			if *params.MaxBytes < srcStat.UsedBytes {
				return nil, ErrInvalidShrink
			}
		}
	}

	commitUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	childUUID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	commitID := commitUUID.String()
	childDiskID := childUUID.String()
	if childDiskID == sourceDiskID {
		return nil, ErrIO
	}
	pgCommitID := pgtype.UUID{Bytes: commitUUID, Valid: true}
	pgChildID := pgtype.UUID{Bytes: childUUID, Valid: true}

	unlockChild := m.lockDisk(childDiskID)
	defer unlockChild()

	commitDir := m.commitDir(commitID)
	ckStats, err := sourceActor.Checkpoint(ctx, commitDir)
	if err != nil {
		return nil, fmt.Errorf("固化源盘基准切面: %w", err)
	}

	removeCommitDirOnRollback := func(stage string) {
		if removeErr := m.removeAll(commitDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback remove commit dir failed",
				"stage", stage,
				"source_disk_id", sourceDiskID,
				"commit_id", commitID,
				"dir", commitDir,
				"err", removeErr,
			)
		}
	}

	effectiveUsedBytes := ckStats.LogicalBytes
	if ckStats.HasOrphans {
		sanitizedStat, sanErr := store.SanitizeCheckpoint(commitDir, cache)
		if sanErr != nil {
			removeCommitDirOnRollback("sanitize")
			return nil, fmt.Errorf("净化基准切面孤儿节点: %w", sanErr)
		}
		effectiveUsedBytes = sanitizedStat.UsedBytes
	}

	targetMaxBytes := ckStats.MaxBytes
	if params.MaxBytes != nil {
		if *params.MaxBytes < effectiveUsedBytes {
			removeCommitDirOnRollback("quota_check")
			return nil, ErrInvalidShrink
		}
		targetMaxBytes = *params.MaxBytes
	}

	childDir := m.diskDir(childDiskID)
	if err = store.CloneCheckpointDir(commitDir, childDir); err != nil {
		removeCommitDirOnRollback("clone")
		return nil, fmt.Errorf("克隆派生子盘目录: %w", err)
	}

	removeChildDirOnRollback := func(stage string) {
		if removeErr := m.removeAll(childDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback remove child dir failed",
				"stage", stage,
				"disk_id", childDiskID,
				"dir", childDir,
				"err", removeErr,
			)
		}
	}

	inoAlloc, packAlloc := m.newSegmentAllocators(sourceRow.LineageID)
	childActor, err := m.openDiskActor(ctx, store.Options{
		Dir:           childDir,
		DiskID:        childDiskID,
		MaxBytes:      targetMaxBytes,
		AtimeMode:     m.atimeMode,
		Cache:         cache,
		Logger:        m.baseLogger,
		Metrics:       m.storeMetrics,
		InoAllocator:  inoAlloc,
		PackAllocator: packAlloc,
		ResetEpoch:    true,
	})
	if err != nil {
		removeChildDirOnRollback("open_child")
		removeCommitDirOnRollback("open_child")
		return nil, fmt.Errorf("打开派生子盘存储实例: %w", err)
	}

	var childRow db.Disk
	err = m.execSyncWriteTxWithRetry(ctx, func(txCtx context.Context, q db.Querier) error {
		if _, lockErr := q.LockDiskLineage(txCtx, sourceRow.LineageID); lockErr != nil {
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return lockErr
		}
		lockedSource, getErr := q.GetDiskForUpdate(txCtx, sourcePGID)
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return getErr
		}
		if _, commitErr := q.CreateDiskCommit(txCtx, db.CreateDiskCommitParams{
			CommitID:       pgCommitID,
			LineageID:      lockedSource.LineageID,
			SourceDiskID:   sourcePGID,
			ParentCommitID: lockedSource.BaseCommitID,
		}); commitErr != nil {
			return commitErr
		}
		if setErr := q.SetDiskBaseCommit(txCtx, db.SetDiskBaseCommitParams{
			DiskID:       sourcePGID,
			BaseCommitID: pgCommitID,
		}); setErr != nil {
			return setErr
		}
		var createErr error
		childRow, createErr = q.CreateDisk(txCtx, db.CreateDiskParams{
			DiskID:       pgChildID,
			Name:         params.Name,
			LineageID:    lockedSource.LineageID,
			ParentDiskID: sourcePGID,
			BaseCommitID: pgCommitID,
		})
		return createErr
	})
	if err != nil {
		if closeErr := childActor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk rollback close child actor failed",
				"disk_id", childDiskID,
				"err", closeErr,
			)
		}
		removeChildDirOnRollback("tx")
		removeCommitDirOnRollback("tx")
		return nil, fmt.Errorf("派生磁盘: %w", err)
	}

	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		if closeErr := childActor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "fork disk close failed after manager closed",
				"disk_id", childDiskID,
				"err", closeErr,
			)
		}
		return nil, ErrClosed
	}
	m.actors[childDiskID] = childActor
	m.poolMu.Unlock()

	mode := actorModeLabel(childActor.ReadOnly())
	m.collector.incActorLoad(childActor.ReadOnly())

	stat, err := childActor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk forked",
		"disk_id", childDiskID,
		"source_disk_id", sourceDiskID,
		"commit_id", commitID,
		"name", params.Name,
		"max_bytes", targetMaxBytes,
		"epoch", childActor.WriterEpoch(),
		"mode", mode,
	)
	return assembleDisk(&childRow, &stat), nil
}

func (m *manager) GetDisk(ctx context.Context, diskID string) (*Disk, error) {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	diskID = pgID.String()

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()

	row, err := m.store.GetDisk(ctx, pgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
	if err != nil {
		return nil, err
	}
	stat, err := actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	return assembleDisk(&row, &stat), nil
}

// diskPageCursor 定义 ListDisks 游标令牌的序列化载荷。
// PostgreSQL TIMESTAMPTZ 精度为微秒，使用 UnixMicro() 编码与还原以消除纳秒截断带来的翻页漂移。
type diskPageCursor struct {
	CreatedAtMicro *int64 `json:"t"`
	DiskID         string `json:"id"`
}

func encodeDiskPageCursor(createdAt time.Time, diskID string) string {
	us := createdAt.UTC().UnixMicro()
	raw, err := json.Marshal(diskPageCursor{
		CreatedAtMicro: &us,
		DiskID:         diskID,
	})
	if err != nil {
		return ""
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func decodeDiskPageCursor(token string) (pgtype.Timestamptz, pgtype.UUID, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return pgtype.Timestamptz{}, pgtype.UUID{}, fmt.Errorf("%w: invalid page_token base64 encoding", ErrInvalidArgument)
	}
	var cursor diskPageCursor
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err = dec.Decode(&cursor); err != nil || dec.More() || cursor.CreatedAtMicro == nil || cursor.DiskID == "" {
		return pgtype.Timestamptz{}, pgtype.UUID{}, fmt.Errorf("%w: malformed page_token payload", ErrInvalidArgument)
	}
	pgID, err := parseDiskID(cursor.DiskID)
	if err != nil {
		return pgtype.Timestamptz{}, pgtype.UUID{}, fmt.Errorf("%w: invalid page_token disk_id", ErrInvalidArgument)
	}
	ts := pgtype.Timestamptz{
		Time:  time.UnixMicro(*cursor.CreatedAtMicro).UTC(),
		Valid: true,
	}
	return ts, pgID, nil
}

func (m *manager) ListDisks(ctx context.Context, params ListDisksParams) (*ListDisksResult, error) {
	if params.PageSize > MaxListDisksPageSize {
		return nil, fmt.Errorf("%w: page_size %d exceeds max %d", ErrInvalidArgument, params.PageSize, MaxListDisksPageSize)
	}
	pageSize := params.PageSize
	if pageSize == 0 {
		pageSize = DefaultListDisksPageSize
	}

	var (
		hasCursor       bool
		cursorCreatedAt pgtype.Timestamptz
		cursorDiskID    pgtype.UUID
	)
	if params.PageToken != "" {
		var err error
		cursorCreatedAt, cursorDiskID, err = decodeDiskPageCursor(params.PageToken)
		if err != nil {
			return nil, err
		}
		hasCursor = true
	}

	rows, err := m.store.ListDisks(ctx, db.ListDisksParams{
		HasCursor:       hasCursor,
		CursorCreatedAt: cursorCreatedAt,
		CursorDiskID:    cursorDiskID,
		PageLimit:       int32(pageSize + 1),
	})
	if err != nil {
		return nil, fmt.Errorf("list disks: %w", err)
	}

	var nextPageToken string
	if len(rows) > int(pageSize) {
		rows = rows[:pageSize]
		lastRow := rows[pageSize-1]
		var lastID string
		if lastRow.DiskID.Valid {
			lastID = lastRow.DiskID.String()
		}
		nextPageToken = encodeDiskPageCursor(lastRow.CreatedAt.Time, lastID)
	}

	disks := make([]*Disk, 0, len(rows))
	for i := range rows {
		row := rows[i]
		diskID := row.DiskID.String()
		actor, actorErr := m.actorForDisk(ctx, diskID, row.LineageID)
		if actorErr != nil {
			return nil, actorErr
		}
		stat, statErr := actor.StatFS(ctx)
		if statErr != nil {
			return nil, statErr
		}
		disks = append(disks, assembleDisk(&row, &stat))
	}

	return &ListDisksResult{
		Disks:         disks,
		NextPageToken: nextPageToken,
	}, nil
}

func (m *manager) actorForDisk(ctx context.Context, diskID string, lineageID pgtype.UUID) (*store.StoreActor, error) {
	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()
	return m.getOrOpenActorWithLineageLocked(ctx, diskID, lineageID)
}

func (m *manager) UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error) {
	pgID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	diskID := pgID.String()

	if params.Name == nil && params.MaxBytes == nil {
		return nil, ErrInvalidArgument
	}
	if params.Name != nil {
		if nameErr := validateDiskName(*params.Name); nameErr != nil {
			return nil, nameErr
		}
	}
	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()

	row, err := m.store.GetDisk(ctx, pgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}

	actor, err := m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
	if err != nil {
		return nil, err
	}

	if params.MaxBytes != nil {
		if maxErr := actor.UpdateMaxBytes(ctx, *params.MaxBytes); maxErr != nil {
			return nil, maxErr
		}
	}

	if params.Name != nil {
		err = m.execSyncWriteTxWithRetry(ctx, func(txCtx context.Context, q db.Querier) error {
			var updateErr error
			row, updateErr = q.UpdateDisk(txCtx, db.UpdateDiskParams{
				DiskID: pgID,
				Name:   *params.Name,
			})
			if errors.Is(updateErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return updateErr
		})
		if err != nil {
			return nil, err
		}
	}

	stat, err := actor.StatFS(ctx)
	if err != nil {
		return nil, err
	}
	return assembleDisk(&row, &stat), nil
}

func (m *manager) DeleteDisk(ctx context.Context, diskID string) error {
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return err
	}
	diskID = pgID.String()

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()

	var commitsToClean []pgtype.UUID
	err = m.execSyncWriteTxWithRetry(ctx, func(txCtx context.Context, q db.Querier) error {
		commitsToClean = nil

		diskRow, getErr := q.GetDisk(txCtx, pgID)
		if getErr != nil {
			if errors.Is(getErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return getErr
		}
		lineageID := diskRow.LineageID

		// 统一遵守 disk_lineages -> disks 的全局加锁顺序，消除并发 ForkDisk 与 DeleteDisk 死锁风险。
		if _, lockErr := q.LockDiskLineage(txCtx, lineageID); lockErr != nil {
			if errors.Is(lockErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return lockErr
		}

		if _, lockDiskErr := q.GetDiskForUpdate(txCtx, pgID); lockDiskErr != nil {
			if errors.Is(lockDiskErr, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return lockDiskErr
		}

		count, delErr := q.DeleteDisk(txCtx, pgID)
		if delErr != nil {
			return delErr
		}
		if count == 0 {
			return ErrNotFound
		}

		remaining, cntErr := q.CountDisksByLineage(txCtx, lineageID)
		if cntErr != nil {
			return cntErr
		}
		switch remaining {
		case 1:
			if clearErr := q.ClearLineageBaseCommits(txCtx, lineageID); clearErr != nil {
				return clearErr
			}
			deletedCommits, cleanErr := q.DeleteLineageCommits(txCtx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = deletedCommits
		case 0:
			deletedCommits, cleanErr := q.DeleteLineageCommits(txCtx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = deletedCommits
			if delLinErr := q.DeleteDiskLineage(txCtx, lineageID); delLinErr != nil {
				return delLinErr
			}
		}
		return nil
	})
	if err != nil {
		return err
	}

	m.poolMu.Lock()
	actor := m.actors[diskID]
	delete(m.actors, diskID)
	m.poolMu.Unlock()

	var (
		epoch       uint64
		mode        = actorModeRW
		cleanupErrs []error
	)
	if actor != nil {
		epoch = actor.WriterEpoch()
		mode = actorModeLabel(actor.ReadOnly())
		if closeErr := actor.CloseDiscard(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "delete disk actor close failed",
				"disk_id", diskID,
				"epoch", epoch,
				"mode", mode,
				"err", closeErr,
			)
			cleanupErrs = append(cleanupErrs, fmt.Errorf("关闭磁盘存储实例: %w", closeErr))
		}
	}

	dir := m.diskDir(diskID)
	if removeErr := m.removeAll(dir); removeErr != nil {
		m.collector.incCleanupError()
		m.logger.WarnContext(ctx, "delete disk directory cleanup failed",
			"disk_id", diskID,
			"dir", dir,
			"epoch", epoch,
			"mode", mode,
			"err", removeErr,
		)
		cleanupErrs = append(cleanupErrs, fmt.Errorf("清理磁盘存储目录: %w", removeErr))
	}

	for _, cID := range commitsToClean {
		if !cID.Valid {
			continue
		}
		cidStr := cID.String()
		cDir := m.commitDir(cidStr)
		if removeErr := m.removeAll(cDir); removeErr != nil {
			m.collector.incCleanupError()
			m.logger.WarnContext(ctx, "delete disk commit directory cleanup failed",
				"disk_id", diskID,
				"commit_id", cidStr,
				"dir", cDir,
				"err", removeErr,
			)
			cleanupErrs = append(cleanupErrs, fmt.Errorf("清理切面快照目录 %s: %w", cidStr, removeErr))
		}
	}

	if len(cleanupErrs) > 0 {
		return errors.Join(cleanupErrs...)
	}

	m.logger.InfoContext(ctx, "disk deleted",
		"disk_id", diskID,
		"reclaimed_commits", len(commitsToClean),
		"epoch", epoch,
		"mode", mode,
	)
	return nil
}

func (m *manager) MountDisk(ctx context.Context, params MountDiskParams) (*MountDiskResult, error) {
	pgID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	diskID := pgID.String()

	actor, err := m.Actor(ctx, diskID)
	if err != nil {
		return nil, err
	}
	if params.ReadOnly {
		rootAttr, attrErr := actor.GetAttr(ctx, store.RootIno)
		if attrErr != nil {
			return nil, attrErr
		}
		m.logger.InfoContext(ctx, "disk mounted",
			"disk_id", diskID,
			"epoch", uint64(0),
			"mode", actorModeRO,
		)
		return &MountDiskResult{
			Epoch:               0,
			RootAttr:            rootAttr,
			EntryTimeoutSeconds: m.entryTimeout,
			AttrTimeoutSeconds:  m.attrTimeout,
		}, nil
	}

	prevEpoch := actor.WriterEpoch()
	epoch, rootAttr, err := actor.AdvanceEpoch(ctx)
	if err != nil {
		return nil, err
	}
	m.logger.InfoContext(ctx, "disk epoch fenced",
		"disk_id", diskID,
		"prev_epoch", prevEpoch,
		"epoch", epoch,
		"mode", actorModeRW,
	)
	return &MountDiskResult{
		Epoch:               epoch,
		RootAttr:            rootAttr,
		EntryTimeoutSeconds: m.entryTimeout,
		AttrTimeoutSeconds:  m.attrTimeout,
	}, nil
}

func (m *manager) Actor(ctx context.Context, diskID string) (*store.StoreActor, error) {
	// 热路径零分配快查：m.actors 仅登记规范化小写非零 UUID 键，已打开的磁盘可直接命中返回。
	m.poolMu.RLock()
	if !m.closed {
		if actor := m.actors[diskID]; actor != nil {
			m.poolMu.RUnlock()
			return actor, nil
		}
	}
	m.poolMu.RUnlock()

	pgID, err := parseDiskID(diskID)
	if err != nil {
		return nil, err
	}
	diskID = pgID.String()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	unlockDisk := m.lockDisk(diskID)
	defer unlockDisk()

	m.poolMu.RLock()
	if m.closed {
		m.poolMu.RUnlock()
		return nil, ErrClosed
	}
	if actor := m.actors[diskID]; actor != nil {
		m.poolMu.RUnlock()
		return actor, nil
	}
	m.poolMu.RUnlock()

	row, err := m.store.GetDisk(ctx, pgID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, err
	}
	return m.getOrOpenActorWithLineageLocked(ctx, diskID, row.LineageID)
}

func (m *manager) Close() error {
	m.poolMu.Lock()
	if m.closed {
		m.poolMu.Unlock()
		return nil
	}
	m.closed = true
	actors := m.actors
	m.actors = make(map[string]*store.StoreActor)
	cache := m.cache
	m.cache = nil
	m.poolMu.Unlock()

	var errs []error
	for diskID, actor := range actors {
		if closeErr := actor.Close(); closeErr != nil {
			m.collector.incCleanupError()
			m.logger.Warn("manager shutdown actor close failed",
				"disk_id", diskID,
				"err", closeErr,
			)
			errs = append(errs, closeErr)
		}
	}
	if cache != nil {
		cache.Unref()
	}
	return errors.Join(errs...)
}
