package meta

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
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
		RootIno:    uint64(row.RootIno),
		MaxBytes:   uint64(row.MaxBytes),
		UsedBytes:  uint64(row.UsedBytes),
		UsedInodes: uint64(row.UsedInodes),
		CreatedAt:  row.CreatedAt.Time,
		UpdatedAt:  row.UpdatedAt.Time,
	}
}

// CreateDisk 创建新的虚拟磁盘实体并原子创建其根目录 Inode。
func (m *manager) CreateDisk(ctx context.Context, params CreateDiskParams) (*Disk, error) {
	// 静态参数校验（无需触达数据库）：
	// 1. 数值边界优先：max_bytes 必须在 1 到 math.MaxInt64 之间
	if params.MaxBytes == 0 || params.MaxBytes > math.MaxInt64 {
		return nil, fmt.Errorf("%w: max_bytes must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), params.MaxBytes)
	}

	// 2. 字符串边界校验：优先校验超长，后校验空字节
	if err := validateDiskName(params.Name); err != nil {
		return nil, err
	}

	// 3. 生成 UUIDv7 磁盘主键
	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate uuidv7: %w", err)
	}

	diskID := pgtype.UUID{
		Bytes: id,
		Valid: true,
	}
	var commitID uuid.UUID
	if m.hasPool() {
		commitID, err = uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("生成初始快照编号: %w", err)
		}
		if err := m.createDiskStorage(id.String(), commitID.String()); err != nil {
			return nil, errors.Join(fmt.Errorf("初始化磁盘存储: %w", err), m.pebble.RemoveDisk(id.String()))
		}
	}

	// 4. 单次读写事务内原子创建根目录 Inode 与 Disk 记录（控制面保持 synchronous_commit = on 同步提交）
	var createdDisk db.GetDiskRow
	var branches []byte
	err = m.execSyncWriteTxWithRetry(ctx, func(q db.Querier) error {
		rootInode, err := q.CreateRootInode(ctx, db.CreateRootInodeParams{
			Mode:  int32(S_IFDIR | 0o755),
			Nlink: 2,
			Uid:   0,
			Gid:   0,
			Rdev:  0,
			Size:  int64(DefaultDirSize),
		})
		if err != nil {
			return fmt.Errorf("create root inode: %w", err)
		}

		row, err := q.CreateDisk(ctx, db.CreateDiskParams{
			DiskID:   diskID,
			Name:     params.Name,
			RootIno:  rootInode.Ino,
			MaxBytes: int64(params.MaxBytes),
		})
		if err != nil {
			var pgErr *pgconn.PgError
			if errors.As(err, &pgErr) && pgErr.Code == "23505" {
				return fmt.Errorf("%w: root_ino %d already exists", ErrAlreadyExists, rootInode.Ino)
			}
			return fmt.Errorf("create disk: %w", err)
		}

		createdDisk = db.GetDiskRow(row)
		if m.hasPool() {
			branches, err = m.registerDiskStorage(ctx, q, createdDisk, commitID)
			if err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		if m.hasPool() {
			return nil, errors.Join(err, m.pebble.RemoveDisk(id.String()))
		}
		return nil, err
	}

	m.initDiskQuotaOnCreate(createdDisk)
	disk := toDomainDisk(createdDisk)
	if len(branches) > 0 {
		if err := setDiskBranches(disk, branches); err != nil {
			return nil, err
		}
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
	// 静态参数校验（无需触达数据库）：
	pgID, err := parseDiskID(params.DiskID)
	if err != nil {
		return nil, err
	}

	if params.Name == nil && params.MaxBytes == nil {
		return nil, fmt.Errorf("%w: at least one of name or max_bytes must be set", ErrInvalidArgument)
	}

	if params.MaxBytes != nil && (*params.MaxBytes == 0 || *params.MaxBytes > math.MaxInt64) {
		return nil, fmt.Errorf("%w: max_bytes must be in range [1, %d]", ErrInvalidArgument, int64(math.MaxInt64))
	}

	if params.Name != nil {
		if err := validateDiskName(*params.Name); err != nil {
			return nil, err
		}
	}
	if m.hasPool() {
		m.writeQuotaMu.Lock()
		defer m.writeQuotaMu.Unlock()
	}
	var tree *TreeQuota
	if m.hasPool() {
		tree, err = m.TreeQuota(ctx, params.DiskID)
		if err != nil {
			return nil, err
		}
		tree.mu.Lock()
	}

	var (
		prevRow    db.GetDiskRow
		hasPrevRow bool
		updatedRow db.GetDiskRow
	)
	err = m.execSyncWriteTxWithRetry(ctx, func(q db.Querier) error {
		if hasPrevRow {
			m.updateDiskQuotaOnUpdate(prevRow)
			hasPrevRow = false
		}
		current, err := q.GetDiskForUpdate(ctx, pgID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get disk for update %s: %w", params.DiskID, err)
		}

		if params.MaxBytes != nil && current.RootIno > 0 && m.hasPool() {
			if r, ok := q.(interface{ RawDBTX() db.DBTX }); ok && r != nil && r.RawDBTX() != nil {
				_, _ = r.RawDBTX().Exec(ctx, `SELECT 1 FROM inodes WHERE root_ino = $1 AND (mode & 61440) = 32768 FOR SHARE`, current.RootIno)
			}
		}

		newName := current.Name
		if params.Name != nil {
			newName = *params.Name
		}

		newMaxBytes := uint64(current.MaxBytes)
		if params.MaxBytes != nil {
			newMaxBytes = *params.MaxBytes
		}

		// 缩容水位保护：严禁缩容至当前已用空间以下
		currentUsedBytes := m.currentDiskUsedBytes(current.RootIno, current.UsedBytes)
		if tree != nil {
			currentUsedBytes = max(currentUsedBytes, tree.usage.Bytes)
		}
		if params.MaxBytes != nil && *params.MaxBytes < currentUsedBytes {
			return ErrInvalidShrink
		}

		updated, err := q.UpdateDisk(ctx, db.UpdateDiskParams{
			DiskID:   pgID,
			Name:     newName,
			MaxBytes: int64(newMaxBytes),
		})
		if err != nil {
			return fmt.Errorf("update disk %s: %w", params.DiskID, err)
		}

		prevRow = db.GetDiskRow(current)
		hasPrevRow = true
		updatedRow = db.GetDiskRow(updated)
		m.updateDiskQuotaOnUpdate(updatedRow)
		return nil
	})
	if tree != nil {
		if err == nil {
			tree.maxBytes = uint64(updatedRow.MaxBytes)
		}
		tree.mu.Unlock()
	}
	if err != nil {
		if hasPrevRow {
			m.updateDiskQuotaOnUpdate(prevRow)
		}
		return nil, err
	}

	return m.diskWithBranches(ctx, updatedRow)
}

// DeleteDisk 级联删除指定虚拟磁盘实体并强制回收其全部元数据节点。
// 在单次读写事务内排他锁定目标磁盘、清理过期客户端会话、校验是否存在存活挂载会话，随后删除 Disk 记录并按 root_ino 批量强制删除该磁盘下的全部 Inode（依赖外键 ON DELETE CASCADE 自动级联清除关联的 entries、inode_xattrs、file_handles、file_locks）。
// 事务提交成功后，主动关闭并清理内存中对应 Inode 的阻塞等待通道，唤醒正在等待锁的协程。
func (m *manager) DeleteDisk(ctx context.Context, diskID string) error {
	// 静态参数校验（无需触达数据库）：非法格式或全零 Nil UUID 直接返回 ErrInvalidArgument
	pgID, err := parseDiskID(diskID)
	if err != nil {
		return err
	}
	diskID = pgID.String()
	var tree *TreeQuota
	var temporaryTree bool
	if m.hasPool() {
		m.writeQuotaMu.Lock()
		defer m.writeQuotaMu.Unlock()
		m.treeQuotaMu.Lock()
		if m.treeQuotas == nil {
			m.treeQuotas = make(map[string]*TreeQuota)
		}
		tree = m.treeQuotas[diskID]
		if tree == nil {
			tree = NewTreeQuota(0)
			m.treeQuotas[diskID] = tree
			temporaryTree = true
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
	}

	var (
		rootIno       int64
		expiredCount  int64
		reclaimedInos []int64
		deletedInos   []int64
	)

	err = m.execSyncWriteTxWithRetry(ctx, func(q db.Querier) error {
		rootIno = 0
		expiredCount = 0
		reclaimedInos = nil
		deletedInos = nil

		// 1. 获取并排他锁定目标 Disk 行与关联 Inodes
		diskRow, err := q.GetDiskForUpdate(ctx, pgID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return ErrNotFound
			}
			return fmt.Errorf("get disk for update %s: %w", diskID, err)
		}
		rootIno = diskRow.RootIno

		// 2. 先清理已过期的客户端会话，若清理了过期会话则回收无引用孤儿节点与配额
		expiredCount, err = q.DeleteExpiredClientSessions(ctx)
		if err != nil {
			return fmt.Errorf("delete expired client sessions before delete disk %s: %w", diskID, err)
		}
		if expiredCount > 0 {
			reclaimedInos, err = m.reclaimUnreferencedOrphansInTx(ctx, q)
			if err != nil {
				return err
			}
		}

		// 3. 查询绑定该磁盘或持有该磁盘下句柄的未过期会话
		activeSIDs, err := q.ListActiveSessionIDsByDisk(ctx, db.ListActiveSessionIDsByDiskParams{
			DiskID:  pgID,
			RootIno: diskRow.RootIno,
		})
		if err != nil {
			return fmt.Errorf("list active sessions for disk %s: %w", diskID, err)
		}
		if len(activeSIDs) > 0 {
			return fmt.Errorf("%w: disk %s has active mounted session %d", ErrBusy, diskID, uint64(activeSIDs[0]))
		}

		// 4. 删除该 Disk 行
		rows, err := q.DeleteDisk(ctx, pgID)
		if err != nil {
			return fmt.Errorf("delete disk %s: %w", diskID, err)
		}
		if rows == 0 {
			return ErrNotFound
		}

		// 5. 按 root_ino 批量强制删除该磁盘下的全部 Inode（依赖外键 ON DELETE CASCADE 自动级联清除关联项）
		deletedInos, err = q.DeleteInodesByRootIno(ctx, diskRow.RootIno)
		if err != nil {
			return fmt.Errorf("delete inodes by root_ino %d for disk %s: %w", diskRow.RootIno, diskID, err)
		}

		if len(deletedInos) > 0 {
			if err := deleteAllChunksByInos(ctx, q, deletedInos); err != nil {
				return fmt.Errorf("delete chunks for disk %s (root_ino=%d): %w", diskID, diskRow.RootIno, err)
			}
		}

		return nil
	})
	if tree != nil {
		if err != nil {
			m.treeQuotaMu.Lock()
			if temporaryTree {
				delete(m.treeQuotas, diskID)
			}
			tree.mu.Lock()
			tree.closed = false
			tree.mu.Unlock()
			m.treeQuotaMu.Unlock()
		}
	}
	if err != nil {
		return err
	}
	m.treeQuotaMu.Lock()
	delete(m.treeQuotas, diskID)
	m.treeQuotaMu.Unlock()
	if m.hasPool() {
		if err := m.pebble.RemoveDisk(diskID); err != nil {
			return fmt.Errorf("清理磁盘存储目录: %w", err)
		}
	}

	// 6. 事务成功提交后，主动清理孤儿节点状态、锁等待通道、配额状态与目录游标，并通知锁等待者
	m.finalizeReclaimedOrphans(reclaimedInos)
	m.finalizeReclaimedOrphans(deletedInos)
	if rootIno > 0 {
		m.deleteDiskQuotaState(rootIno)
		m.treeQuotaRoots.Delete(rootIno)
		m.clearDirCursorsForInos(uint64(rootIno))
	}
	if expiredCount > 0 {
		m.cleanExpiredDirCursors(ctx)
	}
	m.notifyAllLockWaiters()

	return nil
}

// StatFS 查询指定 Inode 所属虚拟磁盘的字节容量与 POSIX 4096 字节块统计信息（对接 Linux FUSE FUSE_STATFS = 17 协议）。
// 接收目标节点的 uint64 ino，按 root_ino O(1) 查询所属 Disk 配额并返回 4096 字节块及已用 Inode 统计信息。
func (m *manager) StatFS(ctx context.Context, ino uint64) (*StatFSResult, error) {
	// 静态参数校验（无需触达数据库）：ino 必须在 [1, math.MaxInt64] 区间内
	if ino == 0 || ino > math.MaxInt64 {
		return nil, fmt.Errorf("%w: ino must be between 1 and %d, got %d", ErrInvalidArgument, int64(math.MaxInt64), ino)
	}

	// 1. 查询目标 Inode（支持已解链但仍打开的孤儿节点 nlink == 0）
	inode, err := m.store.GetInode(ctx, int64(ino))
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get inode %d for statfs: %w", ino, err)
	}
	if inode.RootIno <= 0 {
		return nil, ErrNotFound
	}

	// 2. 根据 root_ino 查询其绑定的 Disk 实体
	row, err := m.store.GetDiskByRootIno(ctx, inode.RootIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("get disk by root_ino %d: %w", inode.RootIno, err)
	}
	disk, err := m.diskWithBranches(ctx, db.GetDiskRow(row))
	if err != nil {
		return nil, err
	}

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
	}, nil
}
