package control

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"maps"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/starsrc/pkg/tx"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/store"
)

type memControlDB struct {
	mu          sync.Mutex
	disks       map[[16]byte]db.Disk
	lineages    map[[16]byte]db.DiskLineage
	commits     map[[16]byte]db.DiskCommit
	failNextTx  error
	inoReserves int
	pckReserves int
}

func newMemControlDB() *memControlDB {
	return &memControlDB{
		disks:    make(map[[16]byte]db.Disk),
		lineages: make(map[[16]byte]db.DiskLineage),
		commits:  make(map[[16]byte]db.DiskCommit),
	}
}

func (m *memControlDB) setFailNextTx(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failNextTx = err
}

func (m *memControlDB) ExecTx(_ context.Context, fn func(db.Querier) error) error {
	m.mu.Lock()
	if m.failNextTx != nil {
		err := m.failNextTx
		m.failNextTx = nil
		m.mu.Unlock()
		return err
	}
	// 事务快照拷贝，确保 fn 返回错误时内存状态原子回滚。
	snapDisks := maps.Clone(m.disks)
	snapLineages := maps.Clone(m.lineages)
	snapCommits := maps.Clone(m.commits)
	m.mu.Unlock()

	if err := fn(m); err != nil {
		m.mu.Lock()
		m.disks = snapDisks
		m.lineages = snapLineages
		m.commits = snapCommits
		m.mu.Unlock()
		return err
	}
	return nil
}

func (m *memControlDB) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, _ ...tx.Option) error {
	return m.ExecTx(ctx, fn)
}

func (m *memControlDB) CreateDiskLineage(_ context.Context, lineageID pgtype.UUID) (db.DiskLineage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row := db.DiskLineage{
		LineageID:  lineageID,
		NextIno:    2,
		NextPackID: 1,
		CreatedAt:  pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	}
	m.lineages[lineageID.Bytes] = row
	return row, nil
}

func (m *memControlDB) LockDiskLineage(_ context.Context, lineageID pgtype.UUID) (db.DiskLineage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.lineages[lineageID.Bytes]
	if !ok {
		return db.DiskLineage{}, pgx.ErrNoRows
	}
	return row, nil
}

func (m *memControlDB) ReserveInodeSegment(_ context.Context, arg db.ReserveInodeSegmentParams) (db.ReserveInodeSegmentRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.lineages[arg.LineageID.Bytes]
	if !ok || arg.Count <= 0 {
		return db.ReserveInodeSegmentRow{}, pgx.ErrNoRows
	}
	m.inoReserves++
	start := max(row.NextIno, arg.MinNext)
	row.NextIno = start + arg.Count
	m.lineages[arg.LineageID.Bytes] = row
	return db.ReserveInodeSegmentRow{
		FirstIno: start,
		LastIno:  row.NextIno - 1,
	}, nil
}

func (m *memControlDB) ReservePackSegment(_ context.Context, arg db.ReservePackSegmentParams) (db.ReservePackSegmentRow, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.lineages[arg.LineageID.Bytes]
	if !ok || arg.Count <= 0 {
		return db.ReservePackSegmentRow{}, pgx.ErrNoRows
	}
	m.pckReserves++
	start := max(row.NextPackID, arg.MinNext)
	row.NextPackID = start + arg.Count
	m.lineages[arg.LineageID.Bytes] = row
	return db.ReservePackSegmentRow{
		FirstPackID: start,
		LastPackID:  row.NextPackID - 1,
	}, nil
}

//nolint:gocritic // 实现 sqlc 生成的 db.Querier 接口值签名
func (m *memControlDB) CreateDiskCommit(_ context.Context, arg db.CreateDiskCommitParams) (db.DiskCommit, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row := db.DiskCommit{
		CommitID:            arg.CommitID,
		LineageID:           arg.LineageID,
		SourceDiskID:        arg.SourceDiskID,
		ParentCommitID:      arg.ParentCommitID,
		MergeParentCommitID: arg.MergeParentCommitID,
		CreatedAt:           pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true},
	}
	m.commits[arg.CommitID.Bytes] = row
	return row, nil
}

//nolint:gocritic // 实现 sqlc 生成的 db.Querier 接口值签名
func (m *memControlDB) CreateDisk(_ context.Context, arg db.CreateDiskParams) (db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	row := db.Disk{
		DiskID:       arg.DiskID,
		Name:         arg.Name,
		CreatedAt:    now,
		UpdatedAt:    now,
		LineageID:    arg.LineageID,
		ParentDiskID: arg.ParentDiskID,
		BaseCommitID: arg.BaseCommitID,
	}
	m.disks[arg.DiskID.Bytes] = row
	return row, nil
}

func (m *memControlDB) SetDiskBaseCommit(_ context.Context, arg db.SetDiskBaseCommitParams) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.disks[arg.DiskID.Bytes]
	if !ok {
		return pgx.ErrNoRows
	}
	row.BaseCommitID = arg.BaseCommitID
	m.disks[arg.DiskID.Bytes] = row
	return nil
}

func (m *memControlDB) ClearLineageBaseCommits(_ context.Context, lineageID pgtype.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for id := range m.disks {
		row := m.disks[id]
		if row.LineageID == lineageID && row.BaseCommitID.Valid {
			row.BaseCommitID = pgtype.UUID{}
			m.disks[id] = row
		}
	}
	return nil
}

func (m *memControlDB) DeleteLineageCommits(_ context.Context, lineageID pgtype.UUID) ([]pgtype.UUID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var deleted []pgtype.UUID
	for id, c := range m.commits {
		if c.LineageID == lineageID {
			deleted = append(deleted, c.CommitID)
			delete(m.commits, id)
		}
	}
	return deleted, nil
}

func (m *memControlDB) CountDisksByLineage(_ context.Context, lineageID pgtype.UUID) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var cnt int64
	for id := range m.disks {
		if m.disks[id].LineageID == lineageID {
			cnt++
		}
	}
	return cnt, nil
}

func (m *memControlDB) DeleteDiskLineage(_ context.Context, lineageID pgtype.UUID) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.lineages, lineageID.Bytes)
	return nil
}

func (m *memControlDB) DeleteDisk(_ context.Context, diskID pgtype.UUID) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.disks[diskID.Bytes]; !ok {
		return 0, nil
	}
	delete(m.disks, diskID.Bytes)
	return 1, nil
}

func (m *memControlDB) GetDisk(_ context.Context, diskID pgtype.UUID) (db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.disks[diskID.Bytes]
	if !ok {
		return db.Disk{}, pgx.ErrNoRows
	}
	return row, nil
}

func (m *memControlDB) GetDiskForUpdate(ctx context.Context, diskID pgtype.UUID) (db.Disk, error) {
	return m.GetDisk(ctx, diskID)
}

func (m *memControlDB) ListDisks(_ context.Context, arg db.ListDisksParams) ([]db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]db.Disk, 0, len(m.disks))
	for id := range m.disks {
		out = append(out, m.disks[id])
	}
	slices.SortFunc(out, func(a, b db.Disk) int {
		if cmp := b.CreatedAt.Time.Compare(a.CreatedAt.Time); cmp != 0 {
			return cmp
		}
		return bytes.Compare(b.DiskID.Bytes[:], a.DiskID.Bytes[:])
	})
	if int(arg.PageLimit) > 0 && len(out) > int(arg.PageLimit) {
		out = out[:arg.PageLimit]
	}
	return out, nil
}

func (m *memControlDB) UpdateDisk(_ context.Context, arg db.UpdateDiskParams) (db.Disk, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	row, ok := m.disks[arg.DiskID.Bytes]
	if !ok {
		return db.Disk{}, pgx.ErrNoRows
	}
	row.Name = arg.Name
	row.UpdatedAt = pgtype.Timestamptz{Time: time.Now().UTC(), Valid: true}
	m.disks[arg.DiskID.Bytes] = row
	return row, nil
}

func fileHardLinkCount(t *testing.T, path string) uint64 {
	t.Helper()
	info, err := os.Stat(path)
	require.NoError(t, err)
	v := reflect.ValueOf(info.Sys())
	if v.Kind() == reflect.Pointer {
		v = v.Elem()
	}
	field := v.FieldByName("Nlink")
	require.True(t, field.IsValid(), "Stat_t 必须包含 Nlink 字段")
	return field.Uint()
}

func writeRegularFile(t *testing.T, ctx context.Context, actor *store.StoreActor, epoch uint64, name string, data []byte) uint64 {
	t.Helper()
	attr, err := actor.Create(ctx, store.CreateReq{
		ParentIno: RootIno,
		Name:      []byte(name),
		Mode:      store.S_IFREG | 0o644,
		Epoch:     epoch,
	})
	require.NoError(t, err)
	if len(data) > 0 {
		_, _, err = actor.Write(ctx, attr.Ino, 0, data, epoch)
		require.NoError(t, err)
	}
	return attr.Ino
}

func readRegularFile(t *testing.T, ctx context.Context, actor *store.StoreActor, name string, size uint32) []byte {
	t.Helper()
	_, attr, err := actor.Lookup(ctx, RootIno, []byte(name))
	require.NoError(t, err)
	data, err := actor.Read(ctx, attr.Ino, 0, size)
	require.NoError(t, err)
	return data
}

// TestForkDiskReadWriteIsolationAndCascadingDAG 验证在线无感 Fork 后的父子读写隔离、
// 源盘 WriterEpoch 零中断及多级级联 Fork 的 DAG 拓扑关系。
func TestForkDiskReadWriteIsolationAndCascadingDAG(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := newMemControlDB()
	mgr := newManager(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	parentDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{
		Name:     "root-disk",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	require.Empty(t, parentDisk.ParentDiskID)

	parentMount, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: parentDisk.DiskID})
	require.NoError(t, err)
	require.Equal(t, uint64(1), parentMount.Epoch)

	parentActor, err := mgr.Actor(ctx, parentDisk.DiskID)
	require.NoError(t, err)
	basePayload := bytes.Repeat([]byte("BASE-ROOT-CONTENT-"), 512)
	writeRegularFile(t, ctx, parentActor, parentMount.Epoch, "shared.txt", basePayload)

	// 在源盘在线挂载状态下派生 Child1（默认继承配额）。
	child1, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: parentDisk.DiskID,
		Name:         "child-1",
	})
	require.NoError(t, err)
	require.Equal(t, parentDisk.DiskID, child1.ParentDiskID)
	require.Equal(t, parentDisk.MaxBytes, child1.MaxBytes)
	require.Equal(t, uint64(len(basePayload)), child1.UsedBytes)
	require.Equal(t, uint64(2), child1.UsedInodes)

	// 源盘 WriterEpoch 不变，原挂载世代号可无中断继续覆写与新增文件。
	require.Equal(t, uint64(1), parentActor.WriterEpoch())
	parentUpdated := []byte("PARENT-MUTATED-AFTER-FORK")
	_, parentAttr, err := parentActor.Lookup(ctx, RootIno, []byte("shared.txt"))
	require.NoError(t, err)
	_, _, err = parentActor.Write(ctx, parentAttr.Ino, 0, parentUpdated, parentMount.Epoch)
	require.NoError(t, err)
	_, err = parentActor.SetAttr(ctx, parentAttr.Ino, store.SetAttrReq{
		Size:  new(uint64(len(parentUpdated))),
		Epoch: parentMount.Epoch,
	}, parentMount.Epoch)
	require.NoError(t, err)
	writeRegularFile(t, ctx, parentActor, parentMount.Epoch, "only-in-parent.txt", []byte("parent-only"))

	// 子盘初始 WriterEpoch 必须归零；首次可写挂载颁发 Epoch = 1，读取到 Fork 时刻基准内容。
	child1Actor, err := mgr.Actor(ctx, child1.DiskID)
	require.NoError(t, err)
	require.Zero(t, child1Actor.WriterEpoch())

	child1Mount, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: child1.DiskID})
	require.NoError(t, err)
	require.Equal(t, uint64(1), child1Mount.Epoch)
	require.Equal(t, basePayload, readRegularFile(t, ctx, child1Actor, "shared.txt", uint32(len(basePayload)+64)))
	_, _, err = child1Actor.Lookup(ctx, RootIno, []byte("only-in-parent.txt"))
	require.ErrorIs(t, err, ErrNotFound)

	// 子盘修改 shared.txt 并新增 child-only.txt，再从 Child1 级联派生 GrandChild，同时从 Root 二次派生 Child2。
	child1Mutated := bytes.Repeat([]byte("CHILD1-BRANCH-DATA-"), 256)
	_, childSharedAttr, err := child1Actor.Lookup(ctx, RootIno, []byte("shared.txt"))
	require.NoError(t, err)
	_, _, err = child1Actor.Write(ctx, childSharedAttr.Ino, 0, child1Mutated, child1Mount.Epoch)
	require.NoError(t, err)
	_, err = child1Actor.SetAttr(ctx, childSharedAttr.Ino, store.SetAttrReq{
		Size:  new(uint64(len(child1Mutated))),
		Epoch: child1Mount.Epoch,
	}, child1Mount.Epoch)
	require.NoError(t, err)
	writeRegularFile(t, ctx, child1Actor, child1Mount.Epoch, "only-in-child1.txt", []byte("c1"))

	grandChild, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: child1.DiskID,
		Name:         "grand-child",
	})
	require.NoError(t, err)
	require.Equal(t, child1.DiskID, grandChild.ParentDiskID)

	child2, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: parentDisk.DiskID,
		Name:         "child-2",
	})
	require.NoError(t, err)
	require.Equal(t, parentDisk.DiskID, child2.ParentDiskID)

	// 验证三块分支盘各自内容严格隔离。
	require.Equal(t, parentUpdated, readRegularFile(t, ctx, parentActor, "shared.txt", 128))
	require.Equal(t, child1Mutated, readRegularFile(t, ctx, child1Actor, "shared.txt", uint32(len(child1Mutated)+64)))

	grandActor, err := mgr.Actor(ctx, grandChild.DiskID)
	require.NoError(t, err)
	require.Equal(t, child1Mutated, readRegularFile(t, ctx, grandActor, "shared.txt", uint32(len(child1Mutated)+64)))
	require.Equal(t, []byte("c1"), readRegularFile(t, ctx, grandActor, "only-in-child1.txt", 16))

	child2Actor, err := mgr.Actor(ctx, child2.DiskID)
	require.NoError(t, err)
	require.Equal(t, parentUpdated, readRegularFile(t, ctx, child2Actor, "shared.txt", 128))
	require.Equal(t, []byte("parent-only"), readRegularFile(t, ctx, child2Actor, "only-in-parent.txt", 32))

	// 校验数据库血缘与 DAG 拓扑：4 块盘同属一个 lineage，共 3 个切面（C1 <- C2, C1 <- C3）。
	pgRoot, err := parseDiskID(parentDisk.DiskID)
	require.NoError(t, err)
	pgChild1, err := parseDiskID(child1.DiskID)
	require.NoError(t, err)
	pgChild2, err := parseDiskID(child2.DiskID)
	require.NoError(t, err)
	pgGrand, err := parseDiskID(grandChild.DiskID)
	require.NoError(t, err)

	memDB.mu.Lock()
	require.Len(t, memDB.lineages, 1)
	require.Len(t, memDB.commits, 3)
	dbRoot := memDB.disks[pgRoot.Bytes]
	dbChild1 := memDB.disks[pgChild1.Bytes]
	dbChild2 := memDB.disks[pgChild2.Bytes]
	dbGrand := memDB.disks[pgGrand.Bytes]
	c2 := memDB.commits[dbRoot.BaseCommitID.Bytes]
	c3 := memDB.commits[dbChild1.BaseCommitID.Bytes]
	c1 := memDB.commits[c2.ParentCommitID.Bytes]
	memDB.mu.Unlock()

	require.Equal(t, pgRoot, dbRoot.LineageID)
	require.Equal(t, pgRoot, dbChild1.LineageID)
	require.Equal(t, pgRoot, dbChild2.LineageID)
	require.Equal(t, pgRoot, dbGrand.LineageID)
	require.False(t, c1.ParentCommitID.Valid, "首次 Fork 切面的父切面应为空")
	require.Equal(t, c1.CommitID, c2.ParentCommitID, "Root 二次 Fork 切面应指向首次切面 C1")
	require.Equal(t, c1.CommitID, c3.ParentCommitID, "Child1 级联 Fork 切面应指向首次切面 C1")
	require.Equal(t, c2.CommitID, dbChild2.BaseCommitID)
	require.Equal(t, c3.CommitID, dbGrand.BaseCommitID)

	// 验证 ListDisks 与 GetDisk 返回正确的 ParentDiskID。
	listed, err := mgr.ListDisks(ctx, ListDisksParams{PageSize: 10})
	require.NoError(t, err)
	require.Len(t, listed.Disks, 4)
	gotGrand, err := mgr.GetDisk(ctx, grandChild.DiskID)
	require.NoError(t, err)
	require.Equal(t, child1.DiskID, gotGrand.ParentDiskID)
}

// TestForkDiskQuotaValidationAndOrphanSanitization 验证配额缩容前置零 I/O 快检、
// 以及存在未关闭孤儿节点时先净化孤儿再复核目标 MaxBytes 水位线的不变量。
func TestForkDiskQuotaValidationAndOrphanSanitization(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := newMemControlDB()
	mgr := newManager(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	srcDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{
		Name:     "quota-src",
		MaxBytes: 16 << 20,
	})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: srcDisk.DiskID})
	require.NoError(t, err)
	srcActor, err := mgr.Actor(ctx, srcDisk.DiskID)
	require.NoError(t, err)

	keepData := bytes.Repeat([]byte{0xAA}, 4096)
	writeRegularFile(t, ctx, srcActor, mnt.Epoch, "keep.bin", keepData)

	// 1. 无孤儿节点时：MaxBytes < UsedBytes (2048 < 4096) 在零磁盘 I/O 前直接拦截，commits 目录尚未创建。
	_, err = mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: srcDisk.DiskID,
		Name:         "too-small-fast",
		MaxBytes:     new(uint64(2048)),
	})
	require.ErrorIs(t, err, ErrInvalidShrink)
	_, statCommitsErr := os.Stat(filepath.Join(rootDir, "commits"))
	require.ErrorIs(t, statCommitsErr, os.ErrNotExist, "无孤儿前置快检拦截不得产生任何切面目录 I/O")

	// 2. 制造一个 256 KiB 的未关闭孤儿文件（openInClient=true 解链），此时源盘 UsedBytes = 4096 + 262144。
	orphanPayload := bytes.Repeat([]byte{0xBB}, store.BlobSize)
	orphanIno := writeRegularFile(t, ctx, srcActor, mnt.Epoch, "temp-orphan.bin", orphanPayload)
	err = srcActor.Unlink(ctx, RootIno, []byte("temp-orphan.bin"), true, mnt.Epoch)
	require.NoError(t, err)

	srcStatBefore, err := srcActor.StatFS(ctx)
	require.NoError(t, err)
	require.Equal(t, uint64(4096+store.BlobSize), srcStatBefore.UsedBytes)

	// 2a. 目标配额 2048 < 净化后已用空间 4096：净化后复核被拦截，且临时切面目录必须清理干净。
	_, err = mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: srcDisk.DiskID,
		Name:         "too-small-after-sanitize",
		MaxBytes:     new(uint64(2048)),
	})
	require.ErrorIs(t, err, ErrInvalidShrink)
	commitEntries, readErr := os.ReadDir(filepath.Join(rootDir, "commits"))
	if readErr == nil {
		require.Empty(t, commitEntries, "配额拦截回滚后 commits 目录必须零残留")
	}

	// 2b. 目标配额 8192（介于净化后 4096 与净化前 266240 之间）：净化孤儿后成功派生！
	forked, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: srcDisk.DiskID,
		Name:         "sanitized-child",
		MaxBytes:     new(uint64(8192)),
	})
	require.NoError(t, err)
	require.Equal(t, uint64(8192), forked.MaxBytes)
	require.Equal(t, uint64(4096), forked.UsedBytes)
	require.Equal(t, uint64(2), forked.UsedInodes)

	// 验证源盘上的孤儿节点未受影响，依然可按原句柄 Inode 读取完整 256 KiB 内容。
	orphanData, err := srcActor.Read(ctx, orphanIno, 0, store.BlobSize)
	require.NoError(t, err)
	require.Equal(t, orphanPayload, orphanData)
}

// TestDeleteDiskLineageCommitGCAndHardLinkRefcount 验证同血缘三态回收规则：
// |D| >= 2 删父盘保留兄弟分支公共祖先切面；|D| == 1 自动回收切面使幸存盘 .pack st_nlink 回落至 1；|D| == 0 级联清空血缘。
func TestDeleteDiskLineageCommitGCAndHardLinkRefcount(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := newMemControlDB()
	mgr := newManager(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr.Close() })

	rootDisk, err := mgr.CreateDisk(ctx, CreateDiskParams{
		Name:     "lineage-root",
		MaxBytes: 64 << 20,
	})
	require.NoError(t, err)
	mnt, err := mgr.MountDisk(ctx, MountDiskParams{DiskID: rootDisk.DiskID})
	require.NoError(t, err)
	rootActor, err := mgr.Actor(ctx, rootDisk.DiskID)
	require.NoError(t, err)

	packPayload := bytes.Repeat([]byte{0x5C}, store.BlobSize)
	writeRegularFile(t, ctx, rootActor, mnt.Epoch, "shared-pack.bin", packPayload)

	// 派生 Child1：此时共享 .pack 同时存在于 Root、Commit1、Child1 三处（st_nlink == 3）。
	child1, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: rootDisk.DiskID,
		Name:         "sibling-1",
	})
	require.NoError(t, err)

	child1PacksDir := filepath.Join(rootDir, "disks", child1.DiskID, "packs")
	packEntries, err := os.ReadDir(child1PacksDir)
	require.NoError(t, err)
	require.Len(t, packEntries, 1)
	sharedPackPath := filepath.Join(child1PacksDir, packEntries[0].Name())
	require.Equal(t, uint64(3), fileHardLinkCount(t, sharedPackPath))

	// Root 再派生 Child2（此时 |D| == 3：Root, Child1, Child2；切面数 == 2：C1, C2，其中 C2 也硬链接了该共享 .pack，st_nlink == 5）。
	child2, err := mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: rootDisk.DiskID,
		Name:         "sibling-2",
	})
	require.NoError(t, err)
	require.Equal(t, uint64(5), fileHardLinkCount(t, sharedPackPath))

	// 阶段 1：删除父盘 Root，剩余存活兄弟分支 |D| == 2（Child1, Child2）。
	// 两个切面 C1、C2 与物理快照目录必须保留，供兄弟分支未来三路 Merge 定位 LCA。
	require.NoError(t, mgr.DeleteDisk(ctx, rootDisk.DiskID))
	_, err = os.Stat(filepath.Join(rootDir, "disks", rootDisk.DiskID))
	require.ErrorIs(t, err, os.ErrNotExist)

	memDB.mu.Lock()
	commitsAfterRootDel := len(memDB.commits)
	lineagesAfterRootDel := len(memDB.lineages)
	memDB.mu.Unlock()
	require.Equal(t, 2, commitsAfterRootDel, "|D|==2 时必须保留全部历史切面 DAG")
	require.Equal(t, 1, lineagesAfterRootDel)

	commitDirEntries, err := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, err)
	require.Len(t, commitDirEntries, 2)
	// Root 自身的 1 个链接已解链，剩余 C1 + Child1 + C2 + Child2 = 4 个硬链接。
	require.Equal(t, uint64(4), fileHardLinkCount(t, sharedPackPath))

	// 阶段 2：删除兄弟盘 Child2，剩余唯一幸存盘 |D| == 1（Child1）。
	// 必须自动清空 Child1.base_commit_id 并清理全部历史切面记录及 /commits/<id> 目录，使 Child1 .pack st_nlink 立即回落至 1！
	require.NoError(t, mgr.DeleteDisk(ctx, child2.DiskID))

	pgChild1, err := parseDiskID(child1.DiskID)
	require.NoError(t, err)
	memDB.mu.Lock()
	commitsAfterSiblingDel := len(memDB.commits)
	lineagesAfterSiblingDel := len(memDB.lineages)
	survivorRow := memDB.disks[pgChild1.Bytes]
	memDB.mu.Unlock()

	require.Zero(t, commitsAfterSiblingDel, "|D|==1 时必须自动清空同血缘全部历史 disk_commits")
	require.Equal(t, 1, lineagesAfterSiblingDel, "唯一幸存盘仍在，disk_lineages 必须保留")
	require.False(t, survivorRow.BaseCommitID.Valid, "唯一幸存盘 base_commit_id 必须置空")

	commitDirEntries, err = os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, err)
	require.Empty(t, commitDirEntries, "|D|==1 时 /commits/ 下所有切面目录必须全部回收")
	require.Equal(t, uint64(1), fileHardLinkCount(t, sharedPackPath), "唯一幸存盘 .pack 硬链接计数必须回落至 st_nlink == 1")

	// 验证唯一幸存盘数据完好可读，且仍可继续 Fork 出新子盘并删除回收至 |D| == 0。
	child1Actor, err := mgr.Actor(ctx, child1.DiskID)
	require.NoError(t, err)
	require.Equal(t, packPayload, readRegularFile(t, ctx, child1Actor, "shared-pack.bin", store.BlobSize))

	// 阶段 3：删除最后一块幸存盘 Child1（|D| == 0），血缘记录彻底清除。
	require.NoError(t, mgr.DeleteDisk(ctx, child1.DiskID))
	memDB.mu.Lock()
	require.Empty(t, memDB.disks)
	require.Empty(t, memDB.commits)
	require.Empty(t, memDB.lineages, "|D|==0 时必须清理 disk_lineages 行")
	memDB.mu.Unlock()
}

// TestCrossBranchSegmentCollisionFreeAndRestartRecovery 验证父子分支并发分配跨越 PackSegment 号段边界时
// Inode 与 PackID 100% 零碰撞，以及控制面重启懒加载恢复后继续沿用数据库水位线。
func TestCrossBranchSegmentCollisionFreeAndRestartRecovery(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := newMemControlDB()
	mgr1 := newManager(memDB, WithStorageRoot(rootDir))

	parentDisk, err := mgr1.CreateDisk(ctx, CreateDiskParams{
		Name:     "seg-parent",
		MaxBytes: 512 << 20,
	})
	require.NoError(t, err)
	childDisk, err := mgr1.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: parentDisk.DiskID,
		Name:         "seg-child",
	})
	require.NoError(t, err)

	pMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: parentDisk.DiskID})
	require.NoError(t, err)
	cMnt, err := mgr1.MountDisk(ctx, MountDiskParams{DiskID: childDisk.DiskID})
	require.NoError(t, err)

	pActor, err := mgr1.Actor(ctx, parentDisk.DiskID)
	require.NoError(t, err)
	cActor, err := mgr1.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err)

	// 将每包上限调为 1 个 Blob（256 KiB），各写 70 个文件跨越 PackSegment=64 触发二次号段申请。
	pActor.Blobs().SetMaxPackSize(store.BlobSize)
	cActor.Blobs().SetMaxPackSize(store.BlobSize)

	const filesPerBranch = 70
	blk := bytes.Repeat([]byte{0x33}, store.BlobSize)

	var (
		pInos [filesPerBranch]uint64
		cInos [filesPerBranch]uint64
		pErrs [filesPerBranch]error
		cErrs [filesPerBranch]error
		wg    sync.WaitGroup
	)
	wg.Go(func() {
		for i := range filesPerBranch {
			attr, createErr := pActor.Create(ctx, store.CreateReq{
				ParentIno: RootIno,
				Name:      []byte("p-" + time.Duration(i).String()),
				Mode:      store.S_IFREG | 0o644,
				Epoch:     pMnt.Epoch,
			})
			if createErr != nil {
				pErrs[i] = createErr
				return
			}
			_, _, writeErr := pActor.Write(ctx, attr.Ino, 0, blk, pMnt.Epoch)
			if writeErr != nil {
				pErrs[i] = writeErr
				return
			}
			pInos[i] = attr.Ino
		}
	})
	wg.Go(func() {
		for i := range filesPerBranch {
			attr, createErr := cActor.Create(ctx, store.CreateReq{
				ParentIno: RootIno,
				Name:      []byte("c-" + time.Duration(i).String()),
				Mode:      store.S_IFREG | 0o644,
				Epoch:     cMnt.Epoch,
			})
			if createErr != nil {
				cErrs[i] = createErr
				return
			}
			_, _, writeErr := cActor.Write(ctx, attr.Ino, 0, blk, cMnt.Epoch)
			if writeErr != nil {
				cErrs[i] = writeErr
				return
			}
			cInos[i] = attr.Ino
		}
	})
	wg.Wait()

	allInos := make(map[uint64]string, filesPerBranch*2)
	for i := range filesPerBranch {
		require.NoError(t, pErrs[i])
		require.NoError(t, cErrs[i])
		_, pDup := allInos[pInos[i]]
		require.False(t, pDup, "父盘分配到重复 Inode %d", pInos[i])
		allInos[pInos[i]] = "parent"
		_, cDup := allInos[cInos[i]]
		require.False(t, cDup, "子盘分配到冲突 Inode %d", cInos[i])
		allInos[cInos[i]] = "child"
	}
	require.Len(t, allInos, filesPerBranch*2)

	// 模拟进程重启：关闭 mgr1，基于同一 memDB 与 rootDir 启动 mgr2，懒加载后继续分配节点与 Pack。
	require.NoError(t, mgr1.Close())

	mgr2 := newManager(memDB, WithStorageRoot(rootDir))
	t.Cleanup(func() { _ = mgr2.Close() })

	pActor2, err := mgr2.Actor(ctx, parentDisk.DiskID)
	require.NoError(t, err)
	cActor2, err := mgr2.Actor(ctx, childDisk.DiskID)
	require.NoError(t, err)

	pInoAfterRestart := writeRegularFile(t, ctx, pActor2, pMnt.Epoch, "after-restart-p.bin", blk)
	cInoAfterRestart := writeRegularFile(t, ctx, cActor2, cMnt.Epoch, "after-restart-c.bin", blk)
	_, pDup := allInos[pInoAfterRestart]
	_, cDup := allInos[cInoAfterRestart]
	require.False(t, pDup, "重启后父盘新 Inode 不得与历史号段冲突")
	require.False(t, cDup, "重启后子盘新 Inode 不得与历史号段冲突")
	require.NotEqual(t, pInoAfterRestart, cInoAfterRestart)

	// 校验父子两盘物理 packs/ 目录下的全部 .pack 文件名零交集（由于初始盘为空才 Fork，全部 .pack 均由各自独立写入）。
	pPacks, err := os.ReadDir(filepath.Join(rootDir, "disks", parentDisk.DiskID, "packs"))
	require.NoError(t, err)
	cPacks, err := os.ReadDir(filepath.Join(rootDir, "disks", childDisk.DiskID, "packs"))
	require.NoError(t, err)
	seenPacks := make(map[string]struct{}, len(pPacks))
	for _, e := range pPacks {
		seenPacks[e.Name()] = struct{}{}
	}
	for _, e := range cPacks {
		_, exists := seenPacks[e.Name()]
		require.False(t, exists, "同源父子分支新生成的 .pack 编号发生碰撞: %s", e.Name())
	}

	memDB.mu.Lock()
	require.GreaterOrEqual(t, memDB.inoReserves, 4, "初始 + 重启懒加载均应通过外部 InoAllocator 预留号段")
	require.GreaterOrEqual(t, memDB.pckReserves, 6, "跨越 PackSegment=64 加上重启后应多次调用 PackAllocator")
	memDB.mu.Unlock()
}

// TestForkDiskValidationAndTxFailureRollback 验证入参边界拦截、事务失败时子盘实例与切面目录的全量回滚无残留。
func TestForkDiskValidationAndTxFailureRollback(t *testing.T) {
	ctx := context.Background()
	rootDir := t.TempDir()
	memDB := newMemControlDB()
	mgr := newManager(memDB, WithStorageRoot(rootDir))

	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "rollback-src", MaxBytes: 8 << 20})
	require.NoError(t, err)

	// 1. 入参校验边界。
	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: "bad-uuid", Name: "x"})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: "00000000-0000-0000-0000-000000000000", Name: "x"})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: strings.Repeat("a", MaxDiskNameBytes+1)})
	require.ErrorIs(t, err, ErrNameTooLong)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "bad\x00name"})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "zero", MaxBytes: new(uint64(0))})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "overflow", MaxBytes: new(uint64(math.MaxInt64) + 1)})
	require.ErrorIs(t, err, ErrInvalidArgument)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: "018f0000-0000-7000-8000-000000009999", Name: "missing"})
	require.ErrorIs(t, err, ErrNotFound)

	// 2. 注入不可重试的数据库事务错误，验证 childActor 关闭及 childDir / commitDir 彻底清理。
	injectedErr := errors.New("simulated database failure")
	memDB.setFailNextTx(injectedErr)

	_, err = mgr.ForkDisk(ctx, ForkDiskParams{
		SourceDiskID: src.DiskID,
		Name:         "tx-fail-child",
	})
	require.ErrorIs(t, err, injectedErr)

	diskDirEntries, err := os.ReadDir(filepath.Join(rootDir, "disks"))
	require.NoError(t, err)
	require.Len(t, diskDirEntries, 1, "事务回滚后 disks/ 下只能保留原源盘目录")
	require.Equal(t, src.DiskID, diskDirEntries[0].Name())

	commitDirEntries, err := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, err)
	require.Empty(t, commitDirEntries, "事务回滚后 commits/ 下不得残留任何切面目录")

	mgr.poolMu.RLock()
	require.Len(t, mgr.actors, 1, "事务回滚后实例池不得残留失败的子盘 StoreActor")
	mgr.poolMu.RUnlock()

	// 3. 实例池关闭后调用 ForkDisk 必须返回 ErrClosed。
	require.NoError(t, mgr.Close())
	_, err = mgr.ForkDisk(ctx, ForkDiskParams{SourceDiskID: src.DiskID, Name: "after-close"})
	require.ErrorIs(t, err, ErrClosed)
}

type threadSafeLogBuf struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (w *threadSafeLogBuf) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.Write(p)
}

func (w *threadSafeLogBuf) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buf.String()
}

func TestForkPhysicalDeduplicationCommitsMetricsAndTracing(t *testing.T) {
	prevTP := otel.GetTracerProvider()
	exporter := tracetest.NewInMemoryExporter()
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSyncer(exporter),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(tp)
	t.Cleanup(func() {
		_ = tp.Shutdown(context.Background())
		otel.SetTracerProvider(prevTP)
	})

	var logBuf threadSafeLogBuf
	logger := slog.New(slog.NewJSONHandler(&logBuf, &slog.HandlerOptions{Level: slog.LevelInfo}))

	rootDir := t.TempDir()
	memDB := newMemControlDB()
	mgr := newManager(memDB, WithStorageRoot(rootDir), WithLogger(logger))
	t.Cleanup(func() { _ = mgr.Close() })

	reg := prometheus.NewRegistry()
	require.NoError(t, reg.Register(mgr.Collector()))

	ctx, rootSpan := tp.Tracer("test-fork-ctrl").Start(context.Background(), "rpc.ForkLifecycle")

	src, err := mgr.CreateDisk(ctx, CreateDiskParams{Name: "dedup-parent", MaxBytes: 64 << 20})
	require.NoError(t, err)

	srcActor, err := mgr.Actor(ctx, src.DiskID)
	require.NoError(t, err)
	fAttr, err := srcActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("payload-1m.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)

	const payloadSize = 1 << 20 // 1 MiB
	_, _, err = srcActor.Write(ctx, fAttr.Ino, 0, bytes.Repeat([]byte{0x5A}, payloadSize))
	require.NoError(t, err)

	// 写入后制造一个孤儿文件，触发 SanitizeCheckpoint（需走 openDiskActor 并产生 commit:<id> 事件日志）。
	orphanAttr, err := srcActor.Create(ctx, store.CreateReq{
		ParentIno: store.RootIno,
		Name:      []byte("orphan.bin"),
		Mode:      store.S_IFREG | 0o644,
	})
	require.NoError(t, err)
	_, _, err = srcActor.Write(ctx, orphanAttr.Ino, 0, bytes.Repeat([]byte{0x7E}, 8192))
	require.NoError(t, err)
	require.NoError(t, srcActor.Unlink(ctx, store.RootIno, []byte("orphan.bin"), true))

	// 1. 连续基于父盘 Fork 3 个子盘。
	children := make([]*Disk, 0, 3)
	for i := range 3 {
		child, fErr := mgr.ForkDisk(ctx, ForkDiskParams{
			SourceDiskID: src.DiskID,
			Name:         "child-" + string(rune('a'+i)),
		})
		require.NoError(t, fErr)
		children = append(children, child)
	}

	// 验证 UpdateDisk 记录结构化审计日志。
	newName := "dedup-parent-renamed"
	_, err = mgr.UpdateDisk(ctx, UpdateDiskParams{DiskID: src.DiskID, Name: &newName})
	require.NoError(t, err)

	gatherMap := func() map[string]*dto.MetricFamily {
		fams, gErr := reg.Gather()
		require.NoError(t, gErr)
		out := make(map[string]*dto.MetricFamily, len(fams))
		for _, f := range fams {
			out[f.GetName()] = f
		}
		return out
	}

	m1 := gatherMap()
	physBytes := m1["bowl_storage_pack_physical_bytes"].GetMetric()[0].GetGauge().GetValue()
	refBytes := m1["bowl_storage_pack_referenced_bytes"].GetMetric()[0].GetGauge().GetValue()
	require.InDelta(t, float64(payloadSize), physBytes, 65536, "3 次 Fork 后去重物理封包占用应维持约 1 MiB")
	require.GreaterOrEqual(t, refBytes, float64(4*payloadSize), "逻辑引用字节总和应反映 4 份挂载盘的逻辑累加")
	require.InDelta(t, float64(3), m1["bowl_storage_commits_active"].GetMetric()[0].GetGauge().GetValue(), 1e-9)
	require.GreaterOrEqual(t, m1["bowl_storage_commits_physical_bytes"].GetMetric()[0].GetGauge().GetValue(), float64(payloadSize))
	require.GreaterOrEqual(t, m1["bowl_storage_data_dir_physical_bytes"].GetMetric()[0].GetGauge().GetValue(), float64(payloadSize))
	require.Greater(t, m1["bowl_storage_volume_total_bytes"].GetMetric()[0].GetGauge().GetValue(), float64(0))
	require.Greater(t, m1["bowl_storage_volume_available_bytes"].GetMetric()[0].GetGauge().GetValue(), float64(0))

	// 检查 SanitizeCheckpoint 的 commit:<id> pebble_events.log 存在性。
	commitEntries, err := os.ReadDir(filepath.Join(rootDir, "commits"))
	require.NoError(t, err)
	require.Len(t, commitEntries, 3)
	commitID := commitEntries[0].Name()
	rawCommitEvents, err := os.ReadFile(filepath.Join(rootDir, "commits", commitID, store.PebbleEventsLogFileName))
	require.NoError(t, err)
	require.Contains(t, string(rawCommitEvents), `"disk_id":"commit:`+commitID+`"`)

	// 检查控制面直方图与 open_actor（Create src + Sanitize*3 + 3 open_child = 7）。
	openOKCount := uint64(0)
	for _, m := range m1["bowl_storage_control_open_actor_duration_seconds"].GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "result" && lp.GetValue() == "ok" {
				openOKCount = m.GetHistogram().GetSampleCount()
			}
		}
	}
	require.GreaterOrEqual(t, openOKCount, uint64(7), "SanitizeCheckpoint 临时开盘必须纳入 open_actor_duration 统计")

	stageCounts := make(map[string]uint64)
	for _, m := range m1["bowl_storage_control_fork_stage_duration_seconds"].GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == "stage" {
				stageCounts[lp.GetValue()] = m.GetHistogram().GetSampleCount()
			}
		}
	}
	for _, stage := range []string{"checkpoint", "sanitize", "clone_dir", "open_child", "commit_tx"} {
		require.Equal(t, uint64(3), stageCounts[stage], "stage=%s 应记录 3 次", stage)
	}

	// 2. 删除父盘与前两个子盘：尚余最后一个子盘，其基准 commit 仍然驻留不可清理。
	require.NoError(t, mgr.DeleteDisk(ctx, src.DiskID))
	m2 := gatherMap()
	require.GreaterOrEqual(t, m2["bowl_storage_commits_active"].GetMetric()[0].GetGauge().GetValue(), float64(1))
	require.GreaterOrEqual(t, m2["bowl_storage_pack_physical_bytes"].GetMetric()[0].GetGauge().GetValue(), float64(payloadSize))

	require.NoError(t, mgr.DeleteDisk(ctx, children[0].DiskID))
	require.NoError(t, mgr.DeleteDisk(ctx, children[1].DiskID))

	// 删除最后一个子盘：触发血缘 Commit 目录物理回收。
	require.NoError(t, mgr.DeleteDisk(ctx, children[2].DiskID))
	m3 := gatherMap()
	require.InDelta(t, float64(0), m3["bowl_storage_commits_active"].GetMetric()[0].GetGauge().GetValue(), 1e-9)
	require.InDelta(t, float64(0), m3["bowl_storage_commits_physical_bytes"].GetMetric()[0].GetGauge().GetValue(), 1e-9)
	require.InDelta(t, float64(0), m3["bowl_storage_pack_physical_bytes"].GetMetric()[0].GetGauge().GetValue(), 1e-9)
	require.InDelta(t, float64(3), m3["bowl_storage_control_commits_reclaimed_total"].GetMetric()[0].GetCounter().GetValue(), 1e-9)

	rootSpan.End()

	spanCounts := make(map[string]int)
	for _, sp := range exporter.GetSpans() {
		spanCounts[sp.Name]++
	}
	for _, expected := range []string{
		"control.ReserveSegment",
		"control.OpenDiskActor",
		"control.ForkCommitTx",
		"control.ReclaimCommits",
		"store.Checkpoint",
		"store.SanitizeCheckpoint",
		"store.CloneCheckpointDir",
	} {
		require.GreaterOrEqual(t, spanCounts[expected], 1, "缺少控制面/切面链路 Span %s", expected)
	}

	require.Contains(t, logBuf.String(), `"msg":"disk updated"`)
}
