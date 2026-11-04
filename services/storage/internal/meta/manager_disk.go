package meta

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

// validateDiskName 校验磁盘名称合法性。
// 优先校验超长（> MaxDiskNameBytes），后校验是否包含空字节（'\x00'）。
func validateDiskName(name string) error {
	if len(name) > MaxDiskNameBytes {
		return fmt.Errorf("%w: disk name length %d exceeds max %d", ErrNameTooLong, len(name), MaxDiskNameBytes)
	}
	if strings.IndexByte(name, 0) >= 0 {
		return fmt.Errorf("%w: disk name contains null byte", ErrInvalidArgument)
	}
	return nil
}

// toDomainDisk 将数据库持久层 db.GetDiskRow 行模型转换为领域层 Disk 实体。
func toDomainDisk(row db.GetDiskRow) *Disk {
	var diskID string
	if row.DiskID.Valid {
		diskID = row.DiskID.String()
	}
	return &Disk{
		DiskID:     diskID,
		Name:       row.Name,
		RootIno:    RootIno,
		MaxBytes:   uint64(row.MaxBytes),
		UsedBytes:  uint64(row.UsedBytes),
		UsedInodes: uint64(row.UsedInodes),
		CreatedAt:  row.CreatedAt.Time,
		UpdatedAt:  row.UpdatedAt.Time,
	}
}

// CreateDisk 创建新的虚拟磁盘实体并原子创建其根目录 Inode。
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
	commitID, err := uuid.NewV7()
	if err != nil {
		return nil, err
	}
	if err := m.createDiskStorage(id.String(), commitID.String()); err != nil {
		return nil, errors.Join(fmt.Errorf("初始化磁盘存储: %w", err), m.pebble.RemoveDisk(id.String()))
	}
	pgID := pgtype.UUID{Bytes: id, Valid: true}
	var created db.GetDiskRow
	var branches []byte
	err = m.execSyncWriteTxWithRetry(ctx, func(q db.Querier) error {
		row, err := q.CreateDisk(ctx, db.CreateDiskParams{DiskID: pgID, Name: params.Name, MaxBytes: int64(params.MaxBytes)})
		if err != nil {
			return fmt.Errorf("创建磁盘: %w", err)
		}
		created = db.GetDiskRow(row)
		branches, err = m.registerDiskStorage(ctx, q, created, commitID)
		return err
	})
	if err != nil {
		return nil, errors.Join(err, m.pebble.RemoveDisk(id.String()))
	}
	disk := toDomainDisk(created)
	if err := setDiskBranches(disk, branches); err != nil {
		return nil, err
	}
	disk.UsedInodes = 1
	// 预热配额协调器，使首个数据面 RPC 无需再查询控制面。
	control := db.GetDiskControlPlaneRow{
		DiskID: created.DiskID, StorageDir: filepath.Join(m.storageRoot, "disks", id.String()), Branches: branches,
	}
	if _, err := m.loadTreeQuota(ctx, created, control); err != nil {
		return nil, err
	}
	return disk, nil
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

// GetDisk 根据磁盘 ID 查询虚拟磁盘详情与配额状态。
func (m *manager) GetDisk(ctx context.Context, diskID string) (*Disk, error) {
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
	return m.diskWithBranches(ctx, row)
}

// diskPageCursor 定义 ListDisks 游标令牌的序列化载荷。
// PostgreSQL TIMESTAMPTZ 精度为微秒，使用 UnixMicro() 编码与还原以彻底消除纳秒截断带来的翻页漂移。
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
	if err := dec.Decode(&cursor); err != nil || dec.More() || cursor.CreatedAtMicro == nil || cursor.DiskID == "" {
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

// ListDisks 分页列出虚拟磁盘实体（按 created_at DESC, disk_id DESC 排序）。
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
		r := db.GetDiskRow(rows[i])
		disk, err := m.diskWithBranches(ctx, r)
		if err != nil {
			return nil, err
		}
		disks = append(disks, disk)
	}

	return &ListDisksResult{
		Disks:         disks,
		NextPageToken: nextPageToken,
	}, nil
}

// UpdateDisk 更新虚拟磁盘属性（名称、最大容量）。
// 在事务内通过 FOR UPDATE 行锁排他锁定磁盘，校验缩容水位线（new_max_bytes >= used_bytes）并原子持久化新配额与名称。
func (m *manager) UpdateDisk(ctx context.Context, params UpdateDiskParams) (*Disk, error) {
	id, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}
	if params.Name == nil && params.MaxBytes == nil {
		return nil, ErrInvalidArgument
	}
	if params.Name != nil {
		if err := validateDiskName(*params.Name); err != nil {
			return nil, err
		}
	}
	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, ErrInvalidArgument
	}
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	tree, err := m.TreeQuota(ctx, params.DiskID)
	if err != nil {
		return nil, err
	}
	tree.mu.Lock()
	var updated db.GetDiskRow
	err = m.execSyncWriteTxWithRetry(ctx, func(q db.Querier) error {
		current, err := q.GetDiskForUpdate(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		name, limit := current.Name, uint64(current.MaxBytes)
		if params.Name != nil {
			name = *params.Name
		}
		if params.MaxBytes != nil {
			limit = *params.MaxBytes
		}
		if limit < tree.usage.Bytes {
			return ErrInvalidShrink
		}
		row, err := q.UpdateDisk(ctx, db.UpdateDiskParams{DiskID: id, Name: name, MaxBytes: int64(limit)})
		if err != nil {
			return err
		}
		updated = db.GetDiskRow(row)
		return nil
	})
	if err == nil {
		tree.mu.Unlock()
		err = tree.SetMaxBytes(uint64(updated.MaxBytes))
	} else {
		tree.mu.Unlock()
	}
	if err != nil {
		return nil, err
	}
	return m.diskWithBranches(ctx, updated)
}

// DeleteDisk 在没有活跃会话时删除控制面记录和磁盘存储。
func (m *manager) DeleteDisk(ctx context.Context, diskID string) error {
	id, err := parseDiskID(diskID)
	if err != nil {
		return err
	}
	diskID = id.String()
	m.writeQuotaMu.Lock()
	defer m.writeQuotaMu.Unlock()
	m.treeQuotaMu.Lock()
	if m.treeQuotas == nil {
		m.treeQuotas = make(map[string]*TreeQuota)
	}
	tree := m.treeQuotas[diskID]
	temporary := tree == nil
	if temporary {
		tree = NewTreeQuota(0)
		m.treeQuotas[diskID] = tree
	}
	tree.mu.Lock()
	if tree.closed {
		tree.mu.Unlock()
		m.treeQuotaMu.Unlock()
		return ErrNotFound
	}
	tree.closed = true
	tree.mu.Unlock()
	m.treeQuotaMu.Unlock()
	err = m.execSyncWriteTxWithRetry(ctx, func(q db.Querier) error {
		_, err := q.GetDiskForUpdate(ctx, id)
		if errors.Is(err, pgx.ErrNoRows) {
			return ErrNotFound
		}
		if err != nil {
			return err
		}
		sessions, err := q.ListActiveSessionIDsByDisk(ctx, id)
		if err != nil {
			return err
		}
		if len(sessions) > 0 {
			return ErrBusy
		}
		count, err := q.DeleteDisk(ctx, id)
		if err != nil {
			return err
		}
		if count == 0 {
			return ErrNotFound
		}
		return nil
	})
	if err != nil {
		m.treeQuotaMu.Lock()
		tree.mu.Lock()
		tree.closed = false
		tree.mu.Unlock()
		if temporary {
			delete(m.treeQuotas, diskID)
		}
		m.treeQuotaMu.Unlock()
		return err
	}
	m.treeQuotaMu.Lock()
	delete(m.treeQuotas, diskID)
	m.treeQuotaMu.Unlock()
	if err := m.pebble.RemoveDisk(diskID); err != nil {
		return fmt.Errorf("清理磁盘存储目录: %w", err)
	}
	m.invalidatePebbleDiskSessions(diskID)
	return nil
}

func statFSForDisk(disk *Disk) *StatFSResult {
	// 3. 计算容量与 4096 字节块统计信息
	totalBytes := disk.MaxBytes
	usedBytes := disk.UsedBytes

	var availBytes uint64
	if totalBytes > usedBytes {
		availBytes = totalBytes - usedBytes
	}

	const blockSize uint64 = DefaultStatFSBlockSize
	blocks := totalBytes / blockSize
	if blocks == 0 && totalBytes > 0 {
		blocks = 1
	}

	bavail := availBytes / blockSize
	bfree := bavail

	var files uint64
	if disk.UsedInodes > 0 {
		files = disk.UsedInodes
	}

	return &StatFSResult{
		TotalBytes: totalBytes,
		UsedBytes:  usedBytes,
		AvailBytes: availBytes,
		Bsize:      DefaultStatFSBlockSize,
		Frsize:     DefaultStatFSBlockSize,
		Blocks:     blocks,
		Bfree:      bfree,
		Bavail:     bavail,
		Files:      files,
		Ffree:      0,
		Namelen:    MaxFileNameBytes,
	}
}
