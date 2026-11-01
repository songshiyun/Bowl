package meta

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/starsrc/bowl/services/storage/internal/db"
)

const (
	// DefaultDiskQuotaFlushBatchThreshold 定义触发立即异步合并刷盘的脏配额操作数阈值。
	DefaultDiskQuotaFlushBatchThreshold = 64
)

type diskQuotaState struct {
	rootIno        int64
	diskID         pgtype.UUID
	maxBytes       int64
	baseUsedBytes  int64
	baseInodes     int64
	flushingBytes  int64
	flushingInodes int64
	dirtyBytes     int64
	dirtyInodes    int64
	reservedBytes  int64
	loaded         bool
}

func (s *diskQuotaState) effectiveCommittedBytes() int64 {
	v := s.baseUsedBytes + s.flushingBytes + s.dirtyBytes
	if v < 0 {
		return 0
	}
	return v
}

func (s *diskQuotaState) effectiveUsedBytesWithReserved() int64 {
	return s.effectiveCommittedBytes() + s.reservedBytes
}

func (s *diskQuotaState) effectiveCommittedInodes() int64 {
	v := s.baseInodes + s.flushingInodes + s.dirtyInodes
	if v < 0 {
		return 0
	}
	return v
}

// WithDiskQuotaFlushInterval 配置磁盘配额（used_bytes / used_inodes）在内存中预留与异步批量合并刷盘的时间窗口。
// 当 d > 0 且底层 Store 为真实 PostgreSQL 连接池时，元数据与数据写事务免除事务内对 disks 表的行级排他锁，改为内存原子预留与后台按 root_ino 批量合并刷盘；
// 当 d == 0 时保持事务内同步更新 disks 表。
func WithDiskQuotaFlushInterval(d time.Duration) Option {
	return func(m *manager) {
		if d <= 0 {
			m.diskQuotaFlushInterval = 0
		} else {
			m.diskQuotaFlushInterval = d
		}
	}
}

func isNativeTxQuerier(q db.Querier) bool {
	r, ok := q.(interface{ RawDBTX() db.DBTX })
	return ok && r != nil && r.RawDBTX() != nil
}

func (m *manager) isAsyncDiskQuotaEnabled() bool {
	if m == nil || m.diskQuotaFlushInterval <= 0 {
		return false
	}
	pp, ok := m.store.(poolProvider)
	return ok && pp != nil && pp.Pool() != nil
}

func (m *manager) shouldUseAsyncDiskQuota(q db.Querier) bool {
	return m.isAsyncDiskQuotaEnabled() && isNativeTxQuerier(q)
}

func (m *manager) getOrCreateDiskQuotaStateLocked(rootIno int64) *diskQuotaState {
	if m.diskQuotas == nil {
		m.diskQuotas = make(map[int64]*diskQuotaState)
	}
	st, ok := m.diskQuotas[rootIno]
	if !ok {
		st = &diskQuotaState{rootIno: rootIno}
		m.diskQuotas[rootIno] = st
	}
	return st
}

func (m *manager) initDiskQuotaOnCreate(row db.Disk) {
	if row.RootIno <= 0 {
		return
	}
	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(row.RootIno)
	st.diskID = row.DiskID
	st.maxBytes = row.MaxBytes
	st.baseUsedBytes = row.UsedBytes
	st.baseInodes = row.UsedInodes
	st.flushingBytes = 0
	st.flushingInodes = 0
	st.dirtyBytes = 0
	st.dirtyInodes = 0
	st.reservedBytes = 0
	st.loaded = true
	m.quotaMu.Unlock()
}

func (m *manager) updateDiskQuotaOnUpdate(row db.Disk) {
	if row.RootIno <= 0 {
		return
	}
	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(row.RootIno)
	st.diskID = row.DiskID
	st.maxBytes = row.MaxBytes
	if !st.loaded {
		st.baseUsedBytes = row.UsedBytes
		st.baseInodes = row.UsedInodes
		st.loaded = true
	}
	m.quotaMu.Unlock()
}

func (m *manager) evictDiskQuota(rootIno int64) {
	if rootIno <= 0 {
		return
	}
	m.quotaMu.Lock()
	delete(m.diskQuotas, rootIno)
	m.quotaMu.Unlock()
}

func (m *manager) ensureDiskQuotaLoaded(ctx context.Context, rootIno int64) (*diskQuotaState, bool, error) {
	m.quotaMu.Lock()
	if st, ok := m.diskQuotas[rootIno]; ok && st.loaded {
		m.quotaMu.Unlock()
		return st, true, nil
	}
	m.quotaMu.Unlock()

	m.quotaFlushExecMu.Lock()
	defer m.quotaFlushExecMu.Unlock()

	m.quotaMu.Lock()
	if st, ok := m.diskQuotas[rootIno]; ok && st.loaded {
		m.quotaMu.Unlock()
		return st, true, nil
	}
	m.quotaMu.Unlock()

	row, err := m.store.GetDiskByRootIno(ctx, rootIno)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("get disk by root_ino %d: %w", rootIno, err)
	}

	m.quotaMu.Lock()
	st := m.getOrCreateDiskQuotaStateLocked(rootIno)
	if !st.loaded {
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		st.baseUsedBytes = row.UsedBytes
		st.baseInodes = row.UsedInodes
		st.loaded = true
	}
	m.quotaMu.Unlock()
	return st, true, nil
}

func (m *manager) syncDiskRowWithQuotaState(ctx context.Context, row *db.Disk) error {
	if row == nil || row.RootIno <= 0 || !m.isAsyncDiskQuotaEnabled() {
		return nil
	}

	m.quotaMu.Lock()
	st, ok := m.diskQuotas[row.RootIno]
	if !ok {
		st = m.getOrCreateDiskQuotaStateLocked(row.RootIno)
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		st.baseUsedBytes = row.UsedBytes
		st.baseInodes = row.UsedInodes
		st.loaded = true
		m.quotaMu.Unlock()
		return nil
	}
	if st.loaded {
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		row.UsedBytes = st.effectiveCommittedBytes()
		row.UsedInodes = st.effectiveCommittedInodes()
		m.quotaMu.Unlock()
		return nil
	}
	if st.dirtyBytes == 0 && st.dirtyInodes == 0 && st.flushingBytes == 0 && st.flushingInodes == 0 {
		st.diskID = row.DiskID
		st.maxBytes = row.MaxBytes
		st.baseUsedBytes = row.UsedBytes
		st.baseInodes = row.UsedInodes
		st.loaded = true
		m.quotaMu.Unlock()
		return nil
	}
	m.quotaMu.Unlock()

	if err := m.FlushDiskQuotas(ctx); err != nil {
		return err
	}
	if _, found, err := m.ensureDiskQuotaLoaded(ctx, row.RootIno); err != nil {
		return err
	} else if !found {
		return ErrNotFound
	}

	m.quotaMu.Lock()
	if st, ok := m.diskQuotas[row.RootIno]; ok && st.loaded {
		row.UsedBytes = st.effectiveCommittedBytes()
		row.UsedInodes = st.effectiveCommittedInodes()
	}
	m.quotaMu.Unlock()
	return nil
}

func (m *manager) scheduleQuotaFlushLocked() {
	if m.quotaFlushImmediateCh == nil {
		m.quotaFlushImmediateCh = make(chan struct{}, 1)
	}
	if !m.quotaFlusherActive {
		m.quotaFlusherActive = true
		go m.runQuotaFlushLoop()
	}
	if m.dirtyQuotaOps >= DefaultDiskQuotaFlushBatchThreshold {
		select {
		case m.quotaFlushImmediateCh <- struct{}{}:
		default:
		}
	}
}

func (m *manager) runQuotaFlushLoop() {
	for {
		interval := m.diskQuotaFlushInterval
		if interval <= 0 {
			interval = DefaultDiskQuotaFlushInterval
		}
		timer := time.NewTimer(interval)
		select {
		case <-timer.C:
		case <-m.quotaFlushImmediateCh:
			timer.Stop()
		}

		flushCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_ = m.FlushDiskQuotas(flushCtx)
		cancel()

		m.quotaMu.Lock()
		hasDirty := false
		for _, st := range m.diskQuotas {
			if st.dirtyBytes != 0 || st.dirtyInodes != 0 {
				hasDirty = true
				break
			}
		}
		if !hasDirty {
			m.quotaFlusherActive = false
			m.quotaMu.Unlock()
			return
		}
		m.quotaMu.Unlock()
	}
}

// FlushDiskQuotas 将内存中累积的所有未刷盘磁盘配额增量按 root_ino 升序批量合并刷入 PostgreSQL disks 表。
func (m *manager) FlushDiskQuotas(ctx context.Context) error {
	if m == nil || !m.isAsyncDiskQuotaEnabled() {
		return nil
	}

	m.quotaFlushExecMu.Lock()
	defer m.quotaFlushExecMu.Unlock()

	type pendingFlush struct {
		rootIno     int64
		deltaBytes  int64
		deltaInodes int64
	}

	m.quotaMu.Lock()
	if len(m.diskQuotas) == 0 {
		m.dirtyQuotaOps = 0
		m.quotaMu.Unlock()
		return nil
	}
	var batch []pendingFlush
	for rootIno, st := range m.diskQuotas {
		if st.dirtyBytes == 0 && st.dirtyInodes == 0 {
			continue
		}
		batch = append(batch, pendingFlush{
			rootIno:     rootIno,
			deltaBytes:  st.dirtyBytes,
			deltaInodes: st.dirtyInodes,
		})
		st.flushingBytes += st.dirtyBytes
		st.flushingInodes += st.dirtyInodes
		st.dirtyBytes = 0
		st.dirtyInodes = 0
	}
	m.dirtyQuotaOps = 0
	m.quotaMu.Unlock()

	if len(batch) == 0 {
		return nil
	}

	slices.SortFunc(batch, func(a, b pendingFlush) int {
		if a.rootIno < b.rootIno {
			return -1
		}
		if a.rootIno > b.rootIno {
			return 1
		}
		return 0
	})

	var firstErr error
	for _, item := range batch {
		err := m.store.AddDiskQuotaByRootIno(ctx, db.AddDiskQuotaByRootInoParams{
			RootIno:     item.rootIno,
			DeltaBytes:  item.deltaBytes,
			DeltaInodes: item.deltaInodes,
		})
		m.quotaMu.Lock()
		st, exists := m.diskQuotas[item.rootIno]
		if exists {
			st.flushingBytes -= item.deltaBytes
			st.flushingInodes -= item.deltaInodes
			if err != nil {
				st.dirtyBytes += item.deltaBytes
				st.dirtyInodes += item.deltaInodes
			} else if st.loaded {
				st.baseUsedBytes = max(int64(0), st.baseUsedBytes+item.deltaBytes)
				st.baseInodes = max(int64(0), st.baseInodes+item.deltaInodes)
			}
		}
		m.quotaMu.Unlock()
		if err != nil && firstErr == nil {
			firstErr = fmt.Errorf("flush disk quota for root_ino %d: %w", item.rootIno, err)
		}
	}

	return firstErr
}

type quotaBufferedQuerier struct {
	db.Querier
	m                 *manager
	txReservedBytes   map[int64]int64
	txUnreservedBytes map[int64]int64
	txInodes          map[int64]int64
	seenSnapshot      map[int64]bool
}

func (q *quotaBufferedQuerier) RawDBTX() db.DBTX {
	if r, ok := q.Querier.(interface{ RawDBTX() db.DBTX }); ok && r != nil {
		return r.RawDBTX()
	}
	return nil
}

func (q *quotaBufferedQuerier) GetDiskByRootInoForUpdate(ctx context.Context, rootIno int64) (db.Disk, error) {
	if rootIno <= 0 {
		return db.Disk{}, pgx.ErrNoRows
	}
	_, found, err := q.m.ensureDiskQuotaLoaded(ctx, rootIno)
	if err != nil {
		return db.Disk{}, err
	}
	if !found {
		return db.Disk{}, pgx.ErrNoRows
	}

	q.m.quotaMu.Lock()
	st := q.m.diskQuotas[rootIno]
	usedBytes := st.effectiveUsedBytesWithReserved() + q.txUnreservedBytes[rootIno]
	if usedBytes < 0 {
		usedBytes = 0
	}
	usedInodes := st.effectiveCommittedInodes() + q.txInodes[rootIno]
	if usedInodes < 0 {
		usedInodes = 0
	}
	if q.seenSnapshot == nil {
		q.seenSnapshot = make(map[int64]bool)
	}
	q.seenSnapshot[rootIno] = true
	row := db.Disk{
		DiskID:     st.diskID,
		RootIno:    rootIno,
		MaxBytes:   st.maxBytes,
		UsedBytes:  usedBytes,
		UsedInodes: usedInodes,
	}
	q.m.quotaMu.Unlock()
	return row, nil
}

func (q *quotaBufferedQuerier) AddDiskQuotaByRootIno(ctx context.Context, arg db.AddDiskQuotaByRootInoParams) error {
	if arg.RootIno <= 0 || (arg.DeltaBytes == 0 && arg.DeltaInodes == 0) {
		return nil
	}

	if arg.DeltaBytes > 0 {
		_, found, err := q.m.ensureDiskQuotaLoaded(ctx, arg.RootIno)
		if err != nil {
			return err
		}
		if found {
			q.m.quotaMu.Lock()
			st := q.m.diskQuotas[arg.RootIno]
			usedBytes := st.effectiveUsedBytesWithReserved() + q.txUnreservedBytes[arg.RootIno]
			if usedBytes < 0 {
				usedBytes = 0
			}
			if uint64(usedBytes) > uint64(st.maxBytes) || uint64(arg.DeltaBytes) > uint64(st.maxBytes)-uint64(usedBytes) {
				hadSnapshot := q.seenSnapshot[arg.RootIno]
				q.m.quotaMu.Unlock()
				if hadSnapshot {
					return errRetryTx
				}
				return ErrNoSpace
			}
			st.reservedBytes += arg.DeltaBytes
			if q.txReservedBytes == nil {
				q.txReservedBytes = make(map[int64]int64)
			}
			q.txReservedBytes[arg.RootIno] += arg.DeltaBytes
			q.m.quotaMu.Unlock()
		}
	} else if arg.DeltaBytes < 0 {
		if q.txUnreservedBytes == nil {
			q.txUnreservedBytes = make(map[int64]int64)
		}
		q.txUnreservedBytes[arg.RootIno] += arg.DeltaBytes
	}

	if arg.DeltaInodes != 0 {
		if q.txInodes == nil {
			q.txInodes = make(map[int64]int64)
		}
		q.txInodes[arg.RootIno] += arg.DeltaInodes
	}

	return nil
}

func (q *quotaBufferedQuerier) rollback() {
	if len(q.txReservedBytes) > 0 {
		q.m.quotaMu.Lock()
		for rootIno, res := range q.txReservedBytes {
			if st, ok := q.m.diskQuotas[rootIno]; ok {
				st.reservedBytes -= res
			}
		}
		q.m.quotaMu.Unlock()
	}
	clear(q.txReservedBytes)
	clear(q.txUnreservedBytes)
	clear(q.txInodes)
	clear(q.seenSnapshot)
}

func (q *quotaBufferedQuerier) commit() {
	if len(q.txReservedBytes) == 0 && len(q.txUnreservedBytes) == 0 && len(q.txInodes) == 0 {
		return
	}

	q.m.quotaMu.Lock()
	seen := make(map[int64]struct{}, len(q.txReservedBytes)+len(q.txUnreservedBytes)+len(q.txInodes))
	for r := range q.txReservedBytes {
		seen[r] = struct{}{}
	}
	for r := range q.txUnreservedBytes {
		seen[r] = struct{}{}
	}
	for r := range q.txInodes {
		seen[r] = struct{}{}
	}

	hasDirty := false
	for rootIno := range seen {
		st := q.m.getOrCreateDiskQuotaStateLocked(rootIno)
		res := q.txReservedBytes[rootIno]
		unres := q.txUnreservedBytes[rootIno]
		inos := q.txInodes[rootIno]
		st.reservedBytes -= res
		st.dirtyBytes += res + unres
		st.dirtyInodes += inos
		if st.dirtyBytes != 0 || st.dirtyInodes != 0 {
			hasDirty = true
		}
	}
	if hasDirty {
		q.m.dirtyQuotaOps++
		q.m.scheduleQuotaFlushLocked()
	}
	q.m.quotaMu.Unlock()

	clear(q.txReservedBytes)
	clear(q.txUnreservedBytes)
	clear(q.txInodes)
	clear(q.seenSnapshot)
}
