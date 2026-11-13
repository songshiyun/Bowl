// Package db 封装 Storage 控制面 PostgreSQL 连接池、事务与领域持久化接缝。
package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/fx"

	"github.com/starsrc/pkg/observability"
	"github.com/starsrc/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/config"
)

// 领域哨兵错误（吞并底层驱动细节，避免 pgx.ErrNoRows 泄露至调用方）。
var (
	ErrNotFound        = errors.New("db: not found")
	ErrInvalidArgument = errors.New("db: invalid argument")
)

// SegmentKind 标识血缘单调号段资源类别。
type SegmentKind uint8

// 血缘单调号段资源类别枚举。
const (
	SegmentKindInode SegmentKind = iota + 1
	SegmentKindPack
)

// DiskRecord 描述控制面持久化的单盘领域记录（隐藏 base_commit_id 等内部表字段）。
type DiskRecord struct {
	DiskID       uuid.UUID
	ParentDiskID uuid.UUID
	LineageID    uuid.UUID
	Name         string
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

// ForkDiskInput 描述派生子盘事务入参（64B，满足 gocritic hugeParam <= 80B）。
type ForkDiskInput struct {
	SourceDiskID uuid.UUID
	ChildDiskID  uuid.UUID
	CommitID     uuid.UUID
	Name         string
}

// ListDisksQuery 描述游标分页查询参数。
type ListDisksQuery struct {
	PageSize  uint32
	PageToken string
}

// ListDisksPage 描述游标分页查询结果。
type ListDisksPage struct {
	Disks         []DiskRecord
	NextPageToken string
}

// SegmentRange 描述号段分配闭区间 [First, Last]。
type SegmentRange struct {
	First int64
	Last  int64
}

// Store 定义控制面粗粒度持久化接缝接口。
type Store interface {
	CreateDisk(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error)
	ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error)
	GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error)
	ListDisks(ctx context.Context, query ListDisksQuery) (ListDisksPage, error)
	UpdateDiskName(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error)
	DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error)
	ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error)
}

var _ Store = (*SQLStore)(nil)

// SQLStore 提供 PostgreSQL 控制面持久化适配器。
type SQLStore struct {
	queries *Queries
	tx      *tx.PGXTransactor[*Queries]
}

// NewStore 基于连接池创建控制面访问器。
func NewStore(pool *pgxpool.Pool) Store {
	return &SQLStore{
		queries: New(pool),
		tx: tx.NewPGXTransactor(pool, func(rawTx pgx.Tx) *Queries {
			return New(rawTx)
		}),
	}
}

func toPGUUID(id uuid.UUID) pgtype.UUID {
	if id == uuid.Nil {
		return pgtype.UUID{}
	}
	return pgtype.UUID{Bytes: id, Valid: true}
}

func fromPGUUID(id pgtype.UUID) uuid.UUID {
	if !id.Valid {
		return uuid.Nil
	}
	return uuid.UUID(id.Bytes)
}

func toDiskRecord(d *Disk) DiskRecord {
	return DiskRecord{
		DiskID:       fromPGUUID(d.DiskID),
		ParentDiskID: fromPGUUID(d.ParentDiskID),
		LineageID:    fromPGUUID(d.LineageID),
		Name:         d.Name,
		CreatedAt:    d.CreatedAt.Time.UTC(),
		UpdatedAt:    d.UpdatedAt.Time.UTC(),
	}
}

func mapPGXErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, pgx.ErrNoRows) {
		return ErrNotFound
	}
	return err
}

// CreateDisk 原子写入新血缘行与根磁盘记录。
func (s *SQLStore) CreateDisk(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		if _, linErr := q.CreateDiskLineage(ctx, pgID); linErr != nil {
			return linErr
		}
		created, createErr := q.CreateDisk(ctx, CreateDiskParams{
			DiskID:    pgID,
			Name:      name,
			LineageID: pgID,
		})
		if createErr != nil {
			return createErr
		}
		row = created
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

// ForkDisk 在单次可重试事务内按 disk_lineages -> disks 锁序派生子盘并写入基准切面。
func (s *SQLStore) ForkDisk(ctx context.Context, arg ForkDiskInput) (DiskRecord, error) {
	if arg.SourceDiskID == uuid.Nil || arg.ChildDiskID == uuid.Nil || arg.CommitID == uuid.Nil || arg.SourceDiskID == arg.ChildDiskID {
		return DiskRecord{}, fmt.Errorf("%w: invalid fork disk identifiers", ErrInvalidArgument)
	}
	pgSource := toPGUUID(arg.SourceDiskID)
	pgChild := toPGUUID(arg.ChildDiskID)
	pgCommit := toPGUUID(arg.CommitID)

	var childRow Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		srcSnapshot, getErr := q.GetDisk(ctx, pgSource)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		if _, lockErr := q.LockDiskLineage(ctx, srcSnapshot.LineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		lockedSource, lockSrcErr := q.GetDiskForUpdate(ctx, pgSource)
		if lockSrcErr != nil {
			return mapPGXErr(lockSrcErr)
		}
		if _, commitErr := q.CreateDiskCommit(ctx, CreateDiskCommitParams{
			CommitID:       pgCommit,
			LineageID:      lockedSource.LineageID,
			SourceDiskID:   pgSource,
			ParentCommitID: lockedSource.BaseCommitID,
		}); commitErr != nil {
			return commitErr
		}
		if setErr := q.SetDiskBaseCommit(ctx, SetDiskBaseCommitParams{
			DiskID:       pgSource,
			BaseCommitID: pgCommit,
		}); setErr != nil {
			return setErr
		}
		created, createErr := q.CreateDisk(ctx, CreateDiskParams{
			DiskID:       pgChild,
			Name:         arg.Name,
			LineageID:    lockedSource.LineageID,
			ParentDiskID: pgSource,
			BaseCommitID: pgCommit,
		})
		if createErr != nil {
			return createErr
		}
		childRow = created
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&childRow), nil
}

// GetDisk 按标识查询单盘元数据。
func (s *SQLStore) GetDisk(ctx context.Context, diskID uuid.UUID) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	row, err := s.queries.GetDisk(ctx, toPGUUID(diskID))
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

// ListDisks 按 (created_at DESC, disk_id DESC) 稳定游标分页查询磁盘。
func (s *SQLStore) ListDisks(ctx context.Context, query ListDisksQuery) (ListDisksPage, error) {
	pageSize := normalizePageSize(query.PageSize)

	var (
		hasCursor       bool
		cursorCreatedAt pgtype.Timestamptz
		cursorDiskID    pgtype.UUID
	)
	if query.PageToken != "" {
		ts, id, decErr := decodeDiskPageCursor(query.PageToken)
		if decErr != nil {
			return ListDisksPage{}, decErr
		}
		hasCursor = true
		cursorCreatedAt = pgtype.Timestamptz{Time: ts, Valid: true}
		cursorDiskID = toPGUUID(id)
	}

	rows, err := s.queries.ListDisks(ctx, ListDisksParams{
		HasCursor:       hasCursor,
		CursorCreatedAt: cursorCreatedAt,
		CursorDiskID:    cursorDiskID,
		PageLimit:       int32(pageSize + 1),
	})
	if err != nil {
		return ListDisksPage{}, err
	}

	var nextPageToken string
	if len(rows) > int(pageSize) {
		rows = rows[:pageSize]
		lastRow := rows[pageSize-1]
		nextPageToken = encodeDiskPageCursor(lastRow.CreatedAt.Time, fromPGUUID(lastRow.DiskID))
	}

	disks := make([]DiskRecord, 0, len(rows))
	for i := range rows {
		disks = append(disks, toDiskRecord(&rows[i]))
	}
	return ListDisksPage{
		Disks:         disks,
		NextPageToken: nextPageToken,
	}, nil
}

// UpdateDiskName 更新磁盘名称。
func (s *SQLStore) UpdateDiskName(ctx context.Context, diskID uuid.UUID, name string) (DiskRecord, error) {
	if diskID == uuid.Nil {
		return DiskRecord{}, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)
	var row Disk
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		updated, updateErr := q.UpdateDisk(ctx, UpdateDiskParams{
			DiskID: pgID,
			Name:   name,
		})
		if updateErr != nil {
			return mapPGXErr(updateErr)
		}
		row = updated
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return DiskRecord{}, mapPGXErr(err)
	}
	return toDiskRecord(&row), nil
}

// DeleteDisk 在单次可重试事务内按 disk_lineages -> disks 锁序删盘并执行三态血缘切面回收。
func (s *SQLStore) DeleteDisk(ctx context.Context, diskID uuid.UUID) ([]uuid.UUID, error) {
	if diskID == uuid.Nil {
		return nil, fmt.Errorf("%w: disk id cannot be nil uuid", ErrInvalidArgument)
	}
	pgID := toPGUUID(diskID)

	var commitsToClean []uuid.UUID
	err := s.tx.ExecTxWithOptions(ctx, func(q *Queries) error {
		commitsToClean = nil

		diskRow, getErr := q.GetDisk(ctx, pgID)
		if getErr != nil {
			return mapPGXErr(getErr)
		}
		lineageID := diskRow.LineageID

		// 统一遵守 disk_lineages -> disks 的全局加锁顺序，消除并发 ForkDisk 与 DeleteDisk 死锁风险。
		if _, lockErr := q.LockDiskLineage(ctx, lineageID); lockErr != nil {
			return mapPGXErr(lockErr)
		}
		if _, lockDiskErr := q.GetDiskForUpdate(ctx, pgID); lockDiskErr != nil {
			return mapPGXErr(lockDiskErr)
		}

		count, delErr := q.DeleteDisk(ctx, pgID)
		if delErr != nil {
			return delErr
		}
		if count == 0 {
			return ErrNotFound
		}

		remaining, cntErr := q.CountDisksByLineage(ctx, lineageID)
		if cntErr != nil {
			return cntErr
		}
		switch remaining {
		case 1:
			if clearErr := q.ClearLineageBaseCommits(ctx, lineageID); clearErr != nil {
				return clearErr
			}
			deletedCommits, cleanErr := q.DeleteLineageCommits(ctx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = toUUIDSlice(deletedCommits)
		case 0:
			deletedCommits, cleanErr := q.DeleteLineageCommits(ctx, lineageID)
			if cleanErr != nil {
				return cleanErr
			}
			commitsToClean = toUUIDSlice(deletedCommits)
			if delLinErr := q.DeleteDiskLineage(ctx, lineageID); delLinErr != nil {
				return delLinErr
			}
		}
		return nil
	}, tx.WithDefaultRetry())
	if err != nil {
		return nil, mapPGXErr(err)
	}
	return commitsToClean, nil
}

func toUUIDSlice(items []pgtype.UUID) []uuid.UUID {
	if len(items) == 0 {
		return nil
	}
	out := make([]uuid.UUID, 0, len(items))
	for _, item := range items {
		if id := fromPGUUID(item); id != uuid.Nil {
			out = append(out, id)
		}
	}
	return out
}

// ReserveSegment 为指定血缘预留单调递增的 Inode 或 Pack 号段。
func (s *SQLStore) ReserveSegment(ctx context.Context, kind SegmentKind, lineageID uuid.UUID, minNext, count int64) (SegmentRange, error) {
	if lineageID == uuid.Nil || count <= 0 {
		return SegmentRange{}, fmt.Errorf("%w: invalid segment reserve parameters", ErrInvalidArgument)
	}
	if kind != SegmentKindInode && kind != SegmentKindPack {
		return SegmentRange{}, fmt.Errorf("%w: unknown segment kind %d", ErrInvalidArgument, kind)
	}

	pgLineage := toPGUUID(lineageID)
	var seg SegmentRange
	err := tx.Retry(ctx, func(attemptCtx context.Context) error {
		switch kind {
		case SegmentKindInode:
			row, rErr := s.queries.ReserveInodeSegment(attemptCtx, ReserveInodeSegmentParams{
				MinNext:   minNext,
				Count:     count,
				LineageID: pgLineage,
			})
			if rErr != nil {
				return rErr
			}
			seg = SegmentRange{First: row.FirstIno, Last: row.LastIno}
			return nil
		case SegmentKindPack:
			row, rErr := s.queries.ReservePackSegment(attemptCtx, ReservePackSegmentParams{
				MinNext:   minNext,
				Count:     count,
				LineageID: pgLineage,
			})
			if rErr != nil {
				return rErr
			}
			seg = SegmentRange{First: row.FirstPackID, Last: row.LastPackID}
			return nil
		default:
			return ErrInvalidArgument
		}
	}, tx.WithDefaultRetry())
	if err != nil {
		return SegmentRange{}, mapPGXErr(err)
	}
	return seg, nil
}

// OpenPool 根据配置创建连接池。
func OpenPool(cfg config.DBPoolConfig) (*pgxpool.Pool, error) {
	pool, err := tx.OpenPool(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to parse dbpool config: %w", err)
	}
	return pool, nil
}

// PoolParams 包含连接池所需依赖。
type PoolParams struct {
	fx.In
	Ready     observability.Ready `optional:"true"`
	Lifecycle fx.Lifecycle
	Config    config.DBPoolConfig
}

// ProvidePool 创建连接池并注册生命周期。
func ProvidePool(params PoolParams) (*pgxpool.Pool, error) {
	pool, err := OpenPool(params.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize dbpool: %w", err)
	}
	tx.RegisterPoolLifecycle(params.Lifecycle, pool)
	return pool, nil
}

// Module 提供控制面数据库依赖。
var Module = fx.Module("db", fx.Provide(ProvidePool, NewStore))
