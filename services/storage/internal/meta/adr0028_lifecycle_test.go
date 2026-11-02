package meta_test

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// adr0028QueryTracer 拦截并记录通过 pgxpool.Pool 发送到 PostgreSQL 的每一条 SQL 查询。
type adr0028QueryTracer struct {
	mu      sync.Mutex
	queries []string
}

func (t *adr0028QueryTracer) TraceQueryStart(ctx context.Context, _ *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	t.mu.Lock()
	t.queries = append(t.queries, data.SQL)
	t.mu.Unlock()
	return ctx
}

func (t *adr0028QueryTracer) TraceQueryEnd(context.Context, *pgx.Conn, pgx.TraceQueryEndData) {}

func (t *adr0028QueryTracer) reset() {
	t.mu.Lock()
	t.queries = nil
	t.mu.Unlock()
}

func (t *adr0028QueryTracer) snapshot() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return slices.Clone(t.queries)
}

func assertNoExplicitTxOrSetLocalSQL(t *testing.T, step string, sqlText string) {
	t.Helper()
	upper := strings.ToUpper(strings.TrimSpace(sqlText))
	assert.False(t, upper == "BEGIN" || strings.HasPrefix(upper, "BEGIN ") || strings.HasPrefix(upper, "BEGIN;"),
		"%s 不得发送显式 BEGIN 语句, got: %s", step, sqlText)
	assert.False(t, upper == "COMMIT" || strings.HasPrefix(upper, "COMMIT ") || strings.HasPrefix(upper, "COMMIT;"),
		"%s 不得发送显式 COMMIT 语句, got: %s", step, sqlText)
	assert.False(t, strings.HasPrefix(upper, "SET LOCAL") || strings.Contains(upper, "SET LOCAL "),
		"%s 不得发送独立 SET LOCAL 语句, got: %s", step, sqlText)
}

// TestADR0028_FullLifecycle_FiveRoundTripsAndReadBack 验证 ADR-0028 小文件 5 步完整生命周期：
//  1. 预热 CreateDisk 与 SliceID 号段后，依次执行 Lookup(ErrNotFound) -> Create -> Write(4 KiB) -> Flush -> Release，
//     断言 5 个操作各自仅触发 1 条 SQL 查询（总计精确 5 次数据库往返），且全程零 BEGIN、零 COMMIT、零独立 SET LOCAL；
//  2. 同一客户端会话执行 Open -> Read，断言 Open 返回 OpenFlags == FOPEN_KEEP_CACHE，Read 读出 4 KiB 载荷逐字节一致，
//     磁盘配额 UsedBytes == 4096、UsedInodes == 2（根目录 + 新文件）；
//  3. 另一客户端会话执行覆盖 Write 后，原客户端会话再次 Open -> Read，断言 Open 返回 OpenFlags == 0（作废旧缓存）且 Read 读到新数据。
func TestADR0028_FullLifecycle_FiveRoundTripsAndReadBack(t *testing.T) {
	tracer := &adr0028QueryTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(
		store,
		meta.WithBlockStore(localBS),
		meta.WithSliceIDBatchSize(64),
	)

	// 1. 预热 CreateDisk、双客户端会话与 SliceID 内存号段分配器
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0028-lifecycle-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	sessA := uint64(time.Now().UnixNano() & 0x3fffffffffffffff)
	sessB := sessA + 1

	var createdIno uint64
	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if createdIno > 0 {
			cleanupWriteTestInoAndSlices(t, store, mgr, int64(createdIno))
		}
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(sessA), int64(sessB))
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	_, err = mgr.RenewSessionWithDisk(ctx, sessA, &disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.RenewSessionWithDisk(ctx, sessB, &disk.DiskID)
	require.NoError(t, err)

	// 在临时热身盘上预热 SliceID 内存号段队列，确保目标盘配额保持 UsedBytes == 0, UsedInodes == 1
	warmupDisk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0028-warmup-%d", time.Now().UnixNano()),
		MaxBytes: 10 * 1024 * 1024,
	})
	require.NoError(t, err)
	warmupCreate, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: warmupDisk.RootIno,
		Name:      []byte("warmup.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    warmupCreate.Inode.Ino,
		Fh:     warmupCreate.Fh,
		Offset: 0,
		Data:   []byte("warmup"),
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino: warmupCreate.Inode.Ino,
		Fh:  warmupCreate.Fh,
	}))
	cleanupWriteTestInoAndSlices(t, store, mgr, int64(warmupCreate.Inode.Ino))
	require.NoError(t, mgr.DeleteDisk(ctx, warmupDisk.DiskID))

	// 确认目标盘初始配额精确为 UsedBytes == 0, UsedInodes == 1（仅根目录）
	initialDisk, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	require.Equal(t, uint64(0), initialDisk.UsedBytes)
	require.Equal(t, uint64(1), initialDisk.UsedInodes)

	// 2. 清空 QueryTracer，开始精确测量 5 步完整生命周期的数据库往返
	tracer.reset()
	var allLifecycleQueries []string

	targetName := []byte("lifecycle_4k.bin")
	payload4K := make([]byte, 4096)
	for i := range payload4K {
		payload4K[i] = byte((i*31 + 17) & 0xff)
	}

	// Step 1: Lookup (返回 ErrNotFound) -> 精确 1 条 SQL
	_, err = mgr.Lookup(ctx, disk.RootIno, targetName)
	require.ErrorIs(t, err, meta.ErrNotFound)
	lookupQueries := tracer.snapshot()
	require.Len(t, lookupQueries, 1, "Lookup 必须精确仅触发 1 条 SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Lookup", lookupQueries[0])
	allLifecycleQueries = append(allLifecycleQueries, lookupQueries...)

	// Step 2: Create -> 精确 1 条 SQL
	tracer.reset()
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      targetName,
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	createdIno = createRes.Inode.Ino
	fhA := createRes.Fh
	createQueries := tracer.snapshot()
	require.Len(t, createQueries, 1, "Create 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Create", createQueries[0])
	assert.Contains(t, createQueries[0], "set_config('synchronous_commit', 'off', true)", "Create CTE 必须内联异步 WAL 提交")
	allLifecycleQueries = append(allLifecycleQueries, createQueries...)

	// Step 3: Write (4 KiB 载荷) -> 精确 1 条 SQL
	tracer.reset()
	writeRes, err := mgr.Write(ctx, &meta.WriteRequest{
		Ino:       createdIno,
		Fh:        fhA,
		Offset:    0,
		Data:      payload4K,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	require.Equal(t, uint32(len(payload4K)), writeRes.Size)
	writeQueries := tracer.snapshot()
	require.Len(t, writeQueries, 1, "Write 热路径必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Write", writeQueries[0])
	assert.Contains(t, writeQueries[0], "set_config('synchronous_commit', 'off', true)", "Write CTE 必须内联异步 WAL 提交")
	allLifecycleQueries = append(allLifecycleQueries, writeQueries...)

	// Step 4: Flush -> 精确 1 条 SQL
	tracer.reset()
	lockOwnerA := uint64(0x1001)
	err = mgr.Flush(ctx, meta.FlushParams{
		Ino:       createdIno,
		Fh:        fhA,
		LockOwner: &lockOwnerA,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	flushQueries := tracer.snapshot()
	require.Len(t, flushQueries, 1, "Flush 必须精确仅触发 1 条快照 SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Flush", flushQueries[0])
	allLifecycleQueries = append(allLifecycleQueries, flushQueries...)

	// Step 5: Release -> 精确 1 条 SQL
	tracer.reset()
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:       createdIno,
		Fh:        fhA,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	releaseQueries := tracer.snapshot()
	require.Len(t, releaseQueries, 1, "Release 常规文件必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Release", releaseQueries[0])
	assert.Contains(t, releaseQueries[0], "set_config('synchronous_commit', 'off', true)", "Release CTE 必须内联异步 WAL 提交")
	allLifecycleQueries = append(allLifecycleQueries, releaseQueries...)

	// 汇总断言：5 个步骤总计精确 5 次数据库往返，且全程零 BEGIN、零 COMMIT、零独立 SET LOCAL
	require.Len(t, allLifecycleQueries, 5, "Lookup -> Create -> Write -> Flush -> Release 全程必须精确总计 5 次数据库往返")
	for idx, q := range allLifecycleQueries {
		assertNoExplicitTxOrSetLocalSQL(t, fmt.Sprintf("LifecycleStep[%d]", idx+1), q)
	}

	// 3. 同一客户端会话 (sessA) 执行 Open -> Read，验证智能命中 FOPEN_KEEP_CACHE、数据一致与配额账本精确
	openResA1, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       createdIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(meta.FOPEN_KEEP_CACHE), openResA1.OpenFlags, "同一客户端会话在文件未发生外部修改时重新 Open 必须返回 FOPEN_KEEP_CACHE")

	readResA1, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:       createdIno,
		Fh:        openResA1.Fh,
		Offset:    0,
		Size:      4096,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, payload4K, readResA1.Data, "Read 读出字节必须与写入的 4 KiB 载荷逐字节一致")

	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       createdIno,
		Fh:        openResA1.Fh,
		SessionID: &sessA,
	}))

	// 校验磁盘配额：UsedBytes == 4096，UsedInodes == 2（根目录 + 新文件）
	diskAfterWrite, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), diskAfterWrite.UsedBytes, "磁盘 UsedBytes 必须精确为 4096")
	assert.Equal(t, uint64(2), diskAfterWrite.UsedInodes, "磁盘 UsedInodes 必须精确为 2（根目录 + 新文件）")

	statfsRes, err := mgr.StatFS(ctx, disk.RootIno)
	require.NoError(t, err)
	assert.Equal(t, uint64(4096), statfsRes.UsedBytes)
	assert.Equal(t, uint64(2), statfsRes.Files)

	// 4. 另一客户端会话 (sessB) 执行覆盖 Write 后，原客户端会话 (sessA) 再次 Open -> Read
	openResB, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       createdIno,
		Flags:     meta.O_WRONLY,
		SessionID: &sessB,
	})
	require.NoError(t, err)

	overwrittenPayload := bytes.Repeat([]byte("MODIFIED_BY_CLIENT_B_"), 200)[:4096]
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       createdIno,
		Fh:        openResB.Fh,
		Offset:    0,
		Data:      overwrittenPayload,
		SessionID: &sessB,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       createdIno,
		Fh:        openResB.Fh,
		SessionID: &sessB,
	}))

	// 原客户端会话 (sessA) 重新 Open -> Read：OpenFlags 必须为 0（作废旧缓存）且读到 sessB 写入的新数据
	openResA2, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       createdIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(0), openResA2.OpenFlags, "另一客户端覆盖写入后，原客户端会话重新 Open 必须返回 OpenFlags == 0 以作废旧内核页缓存")

	readResA2, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:       createdIno,
		Fh:        openResA2.Fh,
		Offset:    0,
		Size:      4096,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, overwrittenPayload, readResA2.Data, "原客户端会话必须立即读到另一客户端覆盖写入的新数据")

	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       createdIno,
		Fh:        openResA2.Fh,
		SessionID: &sessA,
	}))

	// 原客户端会话 (sessA) 在无后续修改时再次 Open，已知版本已更新为最新，应重新命中 FOPEN_KEEP_CACHE
	openResA3, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       createdIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(meta.FOPEN_KEEP_CACHE), openResA3.OpenFlags, "原客户端更新已知版本后若无新修改，再次 Open 应恢复命中 FOPEN_KEEP_CACHE")
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       createdIno,
		Fh:        openResA3.Fh,
		SessionID: &sessA,
	}))
}

// TestADR0028_AsyncWAL_NonLeakageAndControlPlaneSyncCommit 验证：
//  1. 执行 Create、Write、Release 后，在连接池（包括设置 MaxConns = 1 强制复用同一物理连接的 pgxpool.Pool）上执行 SHOW synchronous_commit，返回值恒为 "on"；
//  2. 通过 QueryTracer 执行控制面操作 CreateDisk、UpdateDisk、DeleteDisk、RenewSession，断言其执行的 SQL 语句中绝不包含 synchronous_commit 为 off 的设置，
//     且在事务内查询 current_setting('synchronous_commit') 恒为 "on"。
func TestADR0028_AsyncWAL_NonLeakageAndControlPlaneSyncCommit(t *testing.T) {
	basePool, ctx := setupMetaTestDB(t)
	if basePool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, basePool)

	// 构造 MaxConns = 1 且挂载 QueryTracer 的连接池，强制所有操作复用同一条物理 PostgreSQL 连接
	tracer := &adr0028QueryTracer{}
	singleConnCfg := basePool.Config().Copy()
	singleConnCfg.MaxConns = 1
	singleConnCfg.MinConns = 1
	singleConnCfg.ConnConfig.Tracer = tracer

	singlePool, err := pgxpool.NewWithConfig(ctx, singleConnCfg)
	require.NoError(t, err)
	t.Cleanup(singlePool.Close)
	require.NoError(t, singlePool.Ping(ctx))

	store := db.NewStore(singlePool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(localBS))

	// 1. 先创建测试磁盘并在单连接池上执行 Create -> Write -> Release（全部使用内联 set_config('synchronous_commit', 'off', true)）
	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0028-nonleak-%d", time.Now().UnixNano()),
		MaxBytes: 50 * 1024 * 1024,
	})
	require.NoError(t, err)
	diskDeleted := false
	t.Cleanup(func() {
		if !diskDeleted {
			_ = mgr.DeleteDisk(context.Background(), disk.DiskID)
		}
	})

	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("async_wal_check.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
	})
	require.NoError(t, err)
	ino := createRes.Inode.Ino
	fh := createRes.Fh
	t.Cleanup(func() {
		if !diskDeleted {
			cleanupWriteTestInoAndSlices(t, store, mgr, int64(ino), fh)
		}
	})

	var syncCommitAfterCreate string
	require.NoError(t, singlePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommitAfterCreate))
	require.Equal(t, "on", syncCommitAfterCreate, "Create 后复用同一物理连接执行 SHOW synchronous_commit 必须为 on")

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   bytes.Repeat([]byte{'W'}, 4096),
	})
	require.NoError(t, err)

	var syncCommitAfterWrite string
	require.NoError(t, singlePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommitAfterWrite))
	require.Equal(t, "on", syncCommitAfterWrite, "Write 后复用同一物理连接执行 SHOW synchronous_commit 必须为 on")

	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino: ino,
		Fh:  fh,
	}))

	var syncCommitAfterRelease string
	require.NoError(t, singlePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommitAfterRelease))
	require.Equal(t, "on", syncCommitAfterRelease, "Release 后复用同一物理连接执行 SHOW synchronous_commit 必须为 on")

	// 在默认多连接池上同样验证 SHOW synchronous_commit 为 "on"
	var basePoolSyncCommit string
	require.NoError(t, basePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&basePoolSyncCommit))
	require.Equal(t, "on", basePoolSyncCommit)

	// 清理测试文件的 chunks/slice_refs，为后续 DeleteDisk 控制面操作做准备
	cleanupWriteTestInoAndSlices(t, store, mgr, int64(ino))

	// 2. 通过 QueryTracer 执行控制面操作 CreateDisk、UpdateDisk、DeleteDisk、RenewSession
	assertControlPlaneSyncCommit := func(opName string, fn func() error) {
		tracer.reset()
		require.NoError(t, fn(), "控制面操作 %s 必须成功", opName)
		queries := tracer.snapshot()
		require.NotEmpty(t, queries, "控制面操作 %s 必须触发数据库查询", opName)
		for _, q := range queries {
			lower := strings.ToLower(q)
			assert.False(t, strings.Contains(lower, "synchronous_commit") && strings.Contains(lower, "off"),
				"控制面操作 %s 的 SQL 语句中绝不得包含将 synchronous_commit 设为 off 的设置, got: %s", opName, q)
		}

		// 在同一物理连接的显式事务内查询 current_setting('synchronous_commit') 恒为 "on"
		err := store.ExecTxWithOptions(ctx, func(q db.Querier) error {
			raw, ok := q.(interface{ RawDBTX() db.DBTX })
			require.True(t, ok, "Querier 必须实现 RawDBTX")
			var setting string
			if err := raw.RawDBTX().QueryRow(ctx, "SELECT current_setting('synchronous_commit')").Scan(&setting); err != nil {
				return err
			}
			assert.Equal(t, "on", setting, "%s 执行后事务内 current_setting('synchronous_commit') 必须恒为 on", opName)
			return nil
		})
		require.NoError(t, err)
	}

	var ctrlDisk *meta.Disk
	assertControlPlaneSyncCommit("CreateDisk", func() error {
		var cErr error
		ctrlDisk, cErr = mgr.CreateDisk(ctx, meta.CreateDiskParams{
			Name:     fmt.Sprintf("adr0028-ctrl-disk-%d", time.Now().UnixNano()),
			MaxBytes: 20 * 1024 * 1024,
		})
		return cErr
	})
	ctrlDiskDeleted := false
	t.Cleanup(func() {
		if !ctrlDiskDeleted && ctrlDisk != nil {
			_ = mgr.DeleteDisk(context.Background(), ctrlDisk.DiskID)
		}
	})

	assertControlPlaneSyncCommit("UpdateDisk", func() error {
		newName := fmt.Sprintf("adr0028-ctrl-disk-updated-%d", time.Now().UnixNano())
		newMax := uint64(40 * 1024 * 1024)
		_, uErr := mgr.UpdateDisk(ctx, meta.UpdateDiskParams{
			DiskID:   ctrlDisk.DiskID,
			Name:     &newName,
			MaxBytes: &newMax,
		})
		return uErr
	})

	sessID := uint64(time.Now().UnixNano() & 0x3fffffffffffffff)
	t.Cleanup(func() {
		_, _ = singlePool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
	})
	assertControlPlaneSyncCommit("RenewSession", func() error {
		_, rErr := mgr.RenewSession(ctx, sessID)
		return rErr
	})

	assertControlPlaneSyncCommit("DeleteDisk", func() error {
		dErr := mgr.DeleteDisk(ctx, ctrlDisk.DiskID)
		if dErr == nil {
			ctrlDiskDeleted = true
		}
		return dErr
	})
}

// TestADR0028_FsyncAndFsyncDir_WALFlushDurability 验证：
//  1. 执行 Create + Write（内联异步 WAL 提交）后，记录当前 pg_stat_wal / pg_current_wal_insert_lsn() 与 pg_current_wal_flush_lsn()，
//     调用 Fsync 与 FsyncDir，断言 pg_current_wal_flush_lsn() >= writeInsertLSN（WAL 刷盘位点真实推进至覆盖此前所有异步提交记录）；
//  2. 非法句柄与过期会话调用 Fsync / FsyncDir 立即返回 ErrBadFileHandle。
func TestADR0028_FsyncAndFsyncDir_WALFlushDurability(t *testing.T) {
	tracer := &adr0028QueryTracer{}
	pool, ctx := setupMetaTestDBWithTracer(t, tracer)
	if pool == nil {
		return
	}
	acquireTestSliceGCAdvisoryLock(t, pool)

	store := db.NewStore(pool)
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(store, meta.WithBlockStore(localBS))

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0028-fsync-wal-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	activeSess := uint64(time.Now().UnixNano() & 0x3fffffffffffffff)
	expiredSess := activeSess + 1

	var ino uint64
	var fh, dirFh uint64
	var expFh, expDirFh uint64

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if dirFh > 0 {
			_ = mgr.ReleaseDir(cleanupCtx, meta.ReleaseDirParams{Ino: disk.RootIno, Fh: dirFh, SessionID: &activeSess})
		}
		if expDirFh > 0 {
			_ = mgr.ReleaseDir(cleanupCtx, meta.ReleaseDirParams{Ino: disk.RootIno, Fh: expDirFh})
		}
		if ino > 0 {
			cleanupWriteTestInoAndSlices(t, store, mgr, int64(ino), fh, expFh)
		}
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id IN ($1, $2)", int64(activeSess), int64(expiredSess))
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	_, err = mgr.RenewSessionWithDisk(ctx, activeSess, &disk.DiskID)
	require.NoError(t, err)
	_, err = mgr.RenewSessionWithDisk(ctx, expiredSess, &disk.DiskID)
	require.NoError(t, err)

	// 1. 执行 Create + Write（内联异步 WAL 提交）
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("wal_durability.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	ino = createRes.Inode.Ino
	fh = createRes.Fh

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       ino,
		Fh:        fh,
		Offset:    0,
		Data:      bytes.Repeat([]byte("ASYNC_WAL_DATA_"), 256),
		SessionID: &activeSess,
	})
	require.NoError(t, err)

	// 记录异步 Write 完成后的 pg_stat_wal.wal_records、pg_current_wal_insert_lsn() 与 pg_current_wal_flush_lsn()
	var (
		walRecordsAfterWrite1 int64
		writeInsertLSN1       string
		preFsyncFlushLSN1     string
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT wal_records, pg_current_wal_insert_lsn()::text, pg_current_wal_flush_lsn()::text
		FROM pg_stat_wal
	`).Scan(&walRecordsAfterWrite1, &writeInsertLSN1, &preFsyncFlushLSN1))
	require.NotEmpty(t, writeInsertLSN1)
	require.Greater(t, walRecordsAfterWrite1, int64(0))

	// 调用 Fsync 并断言 pg_current_wal_flush_lsn() >= writeInsertLSN1
	tracer.reset()
	require.NoError(t, mgr.Fsync(ctx, meta.FsyncParams{
		Ino:       ino,
		Fh:        fh,
		Datasync:  false,
		SessionID: &activeSess,
	}))

	var (
		postFsyncFlushLSN string
		fsyncCovered      bool
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT pg_current_wal_flush_lsn()::text,
		       pg_current_wal_flush_lsn() >= $1::pg_lsn
	`, writeInsertLSN1).Scan(&postFsyncFlushLSN, &fsyncCovered))
	assert.True(t, fsyncCovered,
		"Fsync 后 pg_current_wal_flush_lsn() (%s) 必须 >= Write 时的 insert LSN (%s) (preFlush=%s)",
		postFsyncFlushLSN, writeInsertLSN1, preFsyncFlushLSN1)

	// 2. 再次执行异步 Create / Write 后测试 FsyncDir 的 WAL 刷盘推进
	openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       disk.RootIno,
		Flags:     meta.O_RDONLY,
		SessionID: &activeSess,
	})
	require.NoError(t, err)
	dirFh = openDirRes.Fh

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       ino,
		Fh:        fh,
		Offset:    4096,
		Data:      bytes.Repeat([]byte("ASYNC_WAL_DIR_"), 256),
		SessionID: &activeSess,
	})
	require.NoError(t, err)

	var (
		walRecordsAfterWrite2 int64
		writeInsertLSN2       string
		preFsyncDirFlushLSN   string
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT wal_records, pg_current_wal_insert_lsn()::text, pg_current_wal_flush_lsn()::text
		FROM pg_stat_wal
	`).Scan(&walRecordsAfterWrite2, &writeInsertLSN2, &preFsyncDirFlushLSN))
	require.NotEmpty(t, writeInsertLSN2)

	require.NoError(t, mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       disk.RootIno,
		Fh:        dirFh,
		Datasync:  false,
		SessionID: &activeSess,
	}))

	var (
		postFsyncDirFlushLSN string
		fsyncDirCovered      bool
	)
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT pg_current_wal_flush_lsn()::text,
		       pg_current_wal_flush_lsn() >= $1::pg_lsn
	`, writeInsertLSN2).Scan(&postFsyncDirFlushLSN, &fsyncDirCovered))
	assert.True(t, fsyncDirCovered,
		"FsyncDir 后 pg_current_wal_flush_lsn() (%s) 必须 >= Write 时的 insert LSN (%s) (preFlush=%s)",
		postFsyncDirFlushLSN, writeInsertLSN2, preFsyncDirFlushLSN)

	// 3. 验证非法句柄与过期会话调用 Fsync / FsyncDir 立即返回 ErrBadFileHandle
	// 3.1 非法句柄 (不存在的 Fh)
	err = mgr.Fsync(ctx, meta.FsyncParams{
		Ino:       ino,
		Fh:        999999999,
		SessionID: &activeSess,
	})
	require.ErrorIs(t, err, meta.ErrBadFileHandle, "非法文件句柄调用 Fsync 必须立即返回 ErrBadFileHandle")

	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       disk.RootIno,
		Fh:        999999999,
		SessionID: &activeSess,
	})
	require.ErrorIs(t, err, meta.ErrBadFileHandle, "非法目录句柄调用 FsyncDir 必须立即返回 ErrBadFileHandle")

	// 3.2 过期会话持有的句柄调用 Fsync / FsyncDir 立即返回 ErrBadFileHandle
	expOpenFile, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       ino,
		Flags:     meta.O_RDWR,
		SessionID: &expiredSess,
	})
	require.NoError(t, err)
	expFh = expOpenFile.Fh

	expOpenDir, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       disk.RootIno,
		Flags:     meta.O_RDONLY,
		SessionID: &expiredSess,
	})
	require.NoError(t, err)
	expDirFh = expOpenDir.Fh

	_, err = pool.Exec(ctx, "UPDATE client_sessions SET expires_at = clock_timestamp() - interval '10 seconds' WHERE session_id = $1", int64(expiredSess))
	require.NoError(t, err)

	err = mgr.Fsync(ctx, meta.FsyncParams{
		Ino:       ino,
		Fh:        expFh,
		SessionID: &expiredSess,
	})
	require.ErrorIs(t, err, meta.ErrBadFileHandle, "过期会话句柄调用 Fsync 必须立即返回 ErrBadFileHandle")

	err = mgr.FsyncDir(ctx, meta.FsyncDirParams{
		Ino:       disk.RootIno,
		Fh:        expDirFh,
		SessionID: &expiredSess,
	})
	require.ErrorIs(t, err, meta.ErrBadFileHandle, "过期会话句柄调用 FsyncDir 必须立即返回 ErrBadFileHandle")
}

// setupBenchmarkMetaTestDB 为基准测试建立真实 PostgreSQL 连接池与元数据管理器。
func setupBenchmarkMetaTestDB(b *testing.B) (*pgxpool.Pool, db.Store, meta.Manager, *meta.Disk, uint64) {
	b.Helper()
	ctx := context.Background()
	pool, _ := resolveAndConnectTestDB(ctx, b.Fatalf, b.Skipf)
	if pool == nil {
		return nil, nil, nil, nil, 0
	}
	b.Cleanup(pool.Close)

	store := db.NewStore(pool)
	dataDir := b.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(
		store,
		meta.WithBlockStore(localBS),
		meta.WithSliceIDBatchSize(1024),
	)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("bench-adr0028-%d", time.Now().UnixNano()),
		MaxBytes: 10 * 1024 * 1024 * 1024,
	})
	require.NoError(b, err)

	sessID := uint64(time.Now().UnixNano() & 0x3fffffffffffffff)
	_, err = mgr.RenewSessionWithDisk(ctx, sessID, &disk.DiskID)
	require.NoError(b, err)

	// 预热 SliceID 号段
	warmRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("bench_warmup.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(b, err)
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       warmRes.Inode.Ino,
		Fh:        warmRes.Fh,
		Offset:    0,
		Data:      []byte("w"),
		SessionID: &sessID,
	})
	require.NoError(b, err)
	require.NoError(b, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       warmRes.Inode.Ino,
		Fh:        warmRes.Fh,
		SessionID: &sessID,
	}))
	require.NoError(b, mgr.Unlink(ctx, disk.RootIno, []byte("bench_warmup.bin")))

	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, _ = pool.Exec(cleanupCtx, `
			DELETE FROM slice_refs WHERE slice_id IN (
				SELECT DISTINCT (get_byte(c.slices, s.off+4)::bigint << 56) |
				                (get_byte(c.slices, s.off+5)::bigint << 48) |
				                (get_byte(c.slices, s.off+6)::bigint << 40) |
				                (get_byte(c.slices, s.off+7)::bigint << 32) |
				                (get_byte(c.slices, s.off+8)::bigint << 24) |
				                (get_byte(c.slices, s.off+9)::bigint << 16) |
				                (get_byte(c.slices, s.off+10)::bigint << 8) |
				                (get_byte(c.slices, s.off+11)::bigint)
				FROM chunks c
				JOIN inodes i ON c.ino = i.ino
				CROSS JOIN generate_series(0, octet_length(c.slices) - 24, 24) AS s(off)
				WHERE i.root_ino = $1
			)
		`, int64(disk.RootIno))
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	return pool, store, mgr, disk, sessID
}

func reportADR0028BenchMetrics(b *testing.B, name string, elapsed time.Duration) {
	b.Helper()
	if b.N <= 0 || elapsed <= 0 {
		return
	}
	avgLatencyUs := float64(elapsed.Microseconds()) / float64(b.N)
	opsPerSec := float64(b.N) / elapsed.Seconds()
	b.ReportMetric(avgLatencyUs, "us/op")
	b.ReportMetric(opsPerSec, "ops/s")
	b.Logf("[%s] N=%d total=%v avg_latency=%.2f us/op throughput=%.1f ops/s", name, b.N, elapsed, avgLatencyUs, opsPerSec)
}

func BenchmarkADR0028_Create(b *testing.B) {
	_, _, mgr, disk, sessID := setupBenchmarkMetaTestDB(b)
	if mgr == nil {
		return
	}
	ctx := context.Background()
	var seq atomic.Uint64

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		id := seq.Add(1)
		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("create_%d.bin", id)),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("Create failed: %v", err)
		}
		if res.Fh == 0 {
			b.Fatalf("unexpected zero fh")
		}
	}
	b.StopTimer()
	reportADR0028BenchMetrics(b, "BenchmarkADR0028_Create", time.Since(start))
}

func BenchmarkADR0028_Write4K(b *testing.B) {
	_, _, mgr, disk, sessID := setupBenchmarkMetaTestDB(b)
	if mgr == nil {
		return
	}
	ctx := context.Background()
	res, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("write_4k_target.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(b, err)

	payload4K := bytes.Repeat([]byte{'D'}, 4096)

	b.ResetTimer()
	b.SetBytes(4096)
	start := time.Now()
	for i := 0; i < b.N; i++ {
		_, err := mgr.Write(ctx, &meta.WriteRequest{
			Ino:       res.Inode.Ino,
			Fh:        res.Fh,
			Offset:    uint64(i) * 4096,
			Data:      payload4K,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("Write4K failed: %v", err)
		}
	}
	b.StopTimer()
	reportADR0028BenchMetrics(b, "BenchmarkADR0028_Write4K", time.Since(start))
}

func BenchmarkADR0028_Release(b *testing.B) {
	_, _, mgr, disk, sessID := setupBenchmarkMetaTestDB(b)
	if mgr == nil {
		return
	}
	ctx := context.Background()
	fhs := make([]uint64, b.N)
	inos := make([]uint64, b.N)
	for i := 0; i < b.N; i++ {
		res, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      []byte(fmt.Sprintf("release_%d.bin", i)),
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("pre-Create for Release failed: %v", err)
		}
		inos[i] = res.Inode.Ino
		fhs[i] = res.Fh
	}

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		if err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       inos[i],
			Fh:        fhs[i],
			SessionID: &sessID,
		}); err != nil {
			b.Fatalf("Release failed: %v", err)
		}
	}
	b.StopTimer()
	reportADR0028BenchMetrics(b, "BenchmarkADR0028_Release", time.Since(start))
}

func BenchmarkADR0028_FullLifecycle4K(b *testing.B) {
	_, _, mgr, disk, sessID := setupBenchmarkMetaTestDB(b)
	if mgr == nil {
		return
	}
	ctx := context.Background()
	payload4K := bytes.Repeat([]byte{'L'}, 4096)
	lockOwner := uint64(0x2028)
	var seq atomic.Uint64

	b.ResetTimer()
	b.SetBytes(4096)
	start := time.Now()
	for i := 0; i < b.N; i++ {
		id := seq.Add(1)
		name := []byte(fmt.Sprintf("full_lifecycle_%d.bin", id))

		_, err := mgr.Lookup(ctx, disk.RootIno, name)
		if err != meta.ErrNotFound {
			b.Fatalf("expected ErrNotFound on Lookup, got: %v", err)
		}

		cRes, err := mgr.Create(ctx, meta.CreateParams{
			ParentIno: disk.RootIno,
			Name:      name,
			Mode:      meta.S_IFREG | 0o644,
			UID:       1000,
			GID:       1000,
			Flags:     meta.O_RDWR,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("Create failed: %v", err)
		}

		_, err = mgr.Write(ctx, &meta.WriteRequest{
			Ino:       cRes.Inode.Ino,
			Fh:        cRes.Fh,
			Offset:    0,
			Data:      payload4K,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("Write failed: %v", err)
		}

		if err := mgr.Flush(ctx, meta.FlushParams{
			Ino:       cRes.Inode.Ino,
			Fh:        cRes.Fh,
			LockOwner: &lockOwner,
			SessionID: &sessID,
		}); err != nil {
			b.Fatalf("Flush failed: %v", err)
		}

		if err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       cRes.Inode.Ino,
			Fh:        cRes.Fh,
			SessionID: &sessID,
		}); err != nil {
			b.Fatalf("Release failed: %v", err)
		}
	}
	b.StopTimer()
	reportADR0028BenchMetrics(b, "BenchmarkADR0028_FullLifecycle4K", time.Since(start))
}

func BenchmarkADR0028_SmallFileLifecycle(b *testing.B) {
	b.Run("Create", BenchmarkADR0028_Create)
	b.Run("Write4K", BenchmarkADR0028_Write4K)
	b.Run("Release", BenchmarkADR0028_Release)
	b.Run("FullLifecycle4K", BenchmarkADR0028_FullLifecycle4K)
}
