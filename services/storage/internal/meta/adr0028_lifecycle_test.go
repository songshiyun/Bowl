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
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/Aether/pkg/tx"
	"github.com/starsrc/bowl/services/storage/internal/db"
	"github.com/starsrc/bowl/services/storage/internal/meta"
)

// txSyncCommitInspector 包装 db.Store，在每次写事务回调内部读取真实的 PostgreSQL current_setting('synchronous_commit')，
// 并支持按事务尝试次数注入可重试冲突错误（如 SQLSTATE 40001）以验证回滚重试后的事务级设置。
type txSyncCommitInspector struct {
	db.Store
	mu             sync.Mutex
	attemptCount   int
	observed       []string
	onBeforeReturn func(ctx context.Context, attempt int, q db.Querier) error
}

func (s *txSyncCommitInspector) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.attemptCount = 0
	s.observed = nil
	s.onBeforeReturn = nil
}

func (s *txSyncCommitInspector) snapshotObserved() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.observed)
}

func (s *txSyncCommitInspector) ExecTxWithOptions(ctx context.Context, fn func(db.Querier) error, opts ...tx.Option) error {
	return s.Store.ExecTxWithOptions(ctx, func(q db.Querier) error {
		s.mu.Lock()
		s.attemptCount++
		attempt := s.attemptCount
		hook := s.onBeforeReturn
		s.mu.Unlock()

		err := fn(q)
		if raw, ok := q.(interface{ RawDBTX() db.DBTX }); ok && raw.RawDBTX() != nil {
			var setting string
			if qErr := raw.RawDBTX().QueryRow(ctx, "SELECT current_setting('synchronous_commit')").Scan(&setting); qErr == nil {
				s.mu.Lock()
				s.observed = append(s.observed, setting)
				s.mu.Unlock()
			}
		}
		if err != nil {
			return err
		}
		if hook != nil {
			return hook(ctx, attempt, q)
		}
		return nil
	}, opts...)
}

func (s *txSyncCommitInspector) ExecTx(ctx context.Context, fn func(db.Querier) error) error {
	return s.ExecTxWithOptions(ctx, fn)
}

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

// assertLegacyDataQuery 验证会话分流查询和旧数据路径各自只执行一条 SQL。
func assertLegacyDataQuery(t *testing.T, queries []string, hasSession bool) string {
	t.Helper()
	if hasSession {
		require.Len(t, queries, 2, "携带会话时必须先识别会话，再执行一条旧数据查询")
		assert.Contains(t, strings.ToLower(queries[0]), "from client_sessions")
		assert.Contains(t, strings.ToLower(queries[0]), "lease_managed")
		return queries[1]
	}
	require.Len(t, queries, 1, "无会话时必须直接执行一条旧数据查询")
	return queries[0]
}

// TestLegacyFullLifecycle_RoutedReadWriteAndReadBack 验证旧版小文件 5 步完整生命周期：
//  1. 预热 CreateDisk 与 SliceID 号段后，依次执行 Lookup(ErrNotFound) -> Create -> Write(4 KiB) -> Flush -> Release，
//     断言各操作的数据查询仍为单条 SQL，Write 另有一次会话识别，且全程零 BEGIN、零 COMMIT、零独立 SET LOCAL；
//  2. 同一客户端会话执行 Open -> Read，断言 Open 返回 OpenFlags == FOPEN_KEEP_CACHE，Read 读出 4 KiB 载荷逐字节一致，
//     磁盘配额 UsedBytes == 4096、UsedInodes == 2（根目录 + 新文件）；
//  3. 另一客户端会话执行覆盖 Write 后，原客户端会话再次 Open -> Read，断言 Open 返回 OpenFlags == 0（作废旧缓存）且 Read 读到新数据。
func TestLegacyFullLifecycle_RoutedReadWriteAndReadBack(t *testing.T) {
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

	// Step 3: Write (4 KiB 载荷) -> 先识别会话，再执行一条写入 SQL
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
	writeSQL := assertLegacyDataQuery(t, writeQueries, true)
	assertNoExplicitTxOrSetLocalSQL(t, "Write", writeSQL)
	assert.Contains(t, writeSQL, "set_config('synchronous_commit', 'off', true)", "Write CTE 必须内联异步 WAL 提交")
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

	// 汇总断言：5 个步骤加会话识别总计 6 次数据库往返。
	require.Len(t, allLifecycleQueries, 6)
	for idx, q := range allLifecycleQueries {
		assertNoExplicitTxOrSetLocalSQL(t, fmt.Sprintf("LifecycleStep[%d]", idx+1), q)
	}

	// 3. 同一客户端会话执行 Open -> Read -> Release，并验证数据和配额。
	var readLifecycleQueries []string
	tracer.reset()
	openResA1, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       createdIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(meta.FOPEN_KEEP_CACHE), openResA1.OpenFlags, "同一客户端会话在文件未发生外部修改时重新 Open 必须返回 FOPEN_KEEP_CACHE")
	openQueriesA1 := tracer.snapshot()
	require.Len(t, openQueriesA1, 1, "Open(O_RDONLY) 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Open(O_RDONLY)", openQueriesA1[0])
	assert.Contains(t, openQueriesA1[0], "set_config('synchronous_commit', 'off', true)", "Open CTE 必须内联异步 WAL 提交")
	readLifecycleQueries = append(readLifecycleQueries, openQueriesA1...)

	tracer.reset()
	readResA1, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:       createdIno,
		Fh:        openResA1.Fh,
		Offset:    0,
		Size:      4096,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, payload4K, readResA1.Data, "Read 读出字节必须与写入的 4 KiB 载荷逐字节一致")
	readQueriesA1 := tracer.snapshot()
	readSQL := assertLegacyDataQuery(t, readQueriesA1, true)
	assertNoExplicitTxOrSetLocalSQL(t, "Read", readSQL)
	assert.Contains(t, strings.ToLower(readSQL), "from inodes", "Read 合并 SQL 必须查询 inodes")
	assert.Contains(t, strings.ToLower(readSQL), "file_handles", "Read 合并 SQL 必须内联校验 file_handles")
	assert.Contains(t, strings.ToLower(readSQL), "left join chunks", "Read 合并 SQL 必须合并拉取 chunks")
	readLifecycleQueries = append(readLifecycleQueries, readQueriesA1...)

	tracer.reset()
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       createdIno,
		Fh:        openResA1.Fh,
		SessionID: &sessA,
	}))
	releaseQueriesA1 := tracer.snapshot()
	require.Len(t, releaseQueriesA1, 1, "Release 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Release(Read)", releaseQueriesA1[0])
	assert.Contains(t, releaseQueriesA1[0], "set_config('synchronous_commit', 'off', true)", "Release CTE 必须内联异步 WAL 提交")
	readLifecycleQueries = append(readLifecycleQueries, releaseQueriesA1...)

	require.Len(t, readLifecycleQueries, 4)
	for idx, q := range readLifecycleQueries {
		assertNoExplicitTxOrSetLocalSQL(t, fmt.Sprintf("ReadLifecycleStep[%d]", idx+1), q)
	}

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

// TestADR0028_ReadAndDirLifecycle_RoundTripsAndOrphanReclaim 验证：
//  1. 小文件读全生命周期（锁无关无重试）：对已写入 4 KiB 数据的文件执行 Open(O_RDONLY) -> Read(4096) -> Release，
//     服务端精确触发 4 次数据库往返，包括 Read 的会话识别和单条只读合并查询，
//     零 BEGIN / COMMIT / SET LOCAL，命中 FOPEN_KEEP_CACHE 且读出字节与写入载荷一致；
//  2. 目录遍历与 FOPEN_CACHE_DIR 生命周期：执行 Mkdir 创建子目录并写入文件，OpenDir 与常规目录 ReleaseDir 均精确触发 1 条单语句 CTE SQL；
//     同一会话重复 OpenDir（目录未变）返回 FOPEN_CACHE_DIR，发生子项增删后再次 OpenDir 自动作废 FOPEN_CACHE_DIR；
//  3. 孤儿目录生命周期：在持有目录句柄期间执行 Rmdir 将目录解链为孤儿（nlink == 0），随后调用 ReleaseDir 识别 orphan 信号，
//     无缝转入写事务慢路径回收孤儿目录 Inode、精确退还磁盘 UsedInodes 配额，且 inodes 与 file_handles 无残留。
func TestADR0028_ReadAndDirLifecycle_RoundTripsAndOrphanReclaim(t *testing.T) {
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
		meta.WithSliceIDBatchSize(128),
	)

	disk, err := mgr.CreateDisk(ctx, meta.CreateDiskParams{
		Name:     fmt.Sprintf("adr0028-read-dir-lifecycle-%d", time.Now().UnixNano()),
		MaxBytes: 100 * 1024 * 1024,
	})
	require.NoError(t, err)

	sessA := uint64(time.Now().UnixNano() & 0x3fffffffffffffff)
	var readFileIno, subDirFileIno, addedFileIno uint64

	t.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, ino := range []uint64{readFileIno, subDirFileIno, addedFileIno} {
			if ino > 0 {
				cleanupWriteTestInoAndSlices(t, store, mgr, int64(ino))
			}
		}
		_, _ = pool.Exec(cleanupCtx, "DELETE FROM client_sessions WHERE session_id = $1", int64(sessA))
		_ = mgr.DeleteDisk(cleanupCtx, disk.DiskID)
	})

	_, err = mgr.RenewSessionWithDisk(ctx, sessA, &disk.DiskID)
	require.NoError(t, err)

	// 1. 准备 4 KiB 小文件并验证 Open -> Read -> Release 的查询。
	payload4K := bytes.Repeat([]byte("READ_LIFECYCLE_4K_"), 256)[:4096]
	createRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("read_target_4k.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	readFileIno = createRes.Inode.Ino

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       readFileIno,
		Fh:        createRes.Fh,
		Offset:    0,
		Data:      payload4K,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       readFileIno,
		Fh:        createRes.Fh,
		SessionID: &sessA,
	}))

	var allReadQueries []string

	// Read Step 1: Open(O_RDONLY) -> 精确 1 条单语句 CTE SQL，命中 FOPEN_KEEP_CACHE
	tracer.reset()
	openRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:       readFileIno,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, uint32(meta.FOPEN_KEEP_CACHE), openRes.OpenFlags, "Open(O_RDONLY) 必须命中 FOPEN_KEEP_CACHE")
	openQueries := tracer.snapshot()
	require.Len(t, openQueries, 1, "Open 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Open", openQueries[0])
	assert.Contains(t, openQueries[0], "set_config('synchronous_commit', 'off', true)", "Open CTE 必须内联异步 WAL 提交")
	allReadQueries = append(allReadQueries, openQueries...)

	// Read Step 2: 识别会话后执行一条只读合并 SQL。
	tracer.reset()
	readRes, err := mgr.Read(ctx, &meta.ReadRequest{
		Ino:       readFileIno,
		Fh:        openRes.Fh,
		Offset:    0,
		Size:      4096,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Equal(t, payload4K, readRes.Data, "Read 返回字节必须与写入载荷完全一致")
	readQueries := tracer.snapshot()
	readSQL := assertLegacyDataQuery(t, readQueries, true)
	assertNoExplicitTxOrSetLocalSQL(t, "Read", readSQL)
	assert.Contains(t, strings.ToLower(readSQL), "from inodes", "Read 合并 SQL 必须查询 inodes")
	assert.Contains(t, strings.ToLower(readSQL), "file_handles", "Read 合并 SQL 必须内联校验 file_handles")
	assert.Contains(t, strings.ToLower(readSQL), "left join chunks", "Read 合并 SQL 必须合并拉取 chunks")
	allReadQueries = append(allReadQueries, readQueries...)

	// Read Step 3: Release -> 精确 1 条单语句 CTE SQL
	tracer.reset()
	err = mgr.Release(ctx, meta.ReleaseParams{
		Ino:       readFileIno,
		Fh:        openRes.Fh,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	releaseQueries := tracer.snapshot()
	require.Len(t, releaseQueries, 1, "Release 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "Release", releaseQueries[0])
	assert.Contains(t, releaseQueries[0], "set_config('synchronous_commit', 'off', true)", "Release CTE 必须内联异步 WAL 提交")
	allReadQueries = append(allReadQueries, releaseQueries...)

	// 汇总断言包含一次会话识别。
	require.Len(t, allReadQueries, 4)
	for idx, q := range allReadQueries {
		assertNoExplicitTxOrSetLocalSQL(t, fmt.Sprintf("ReadLifecycle[%d]", idx+1), q)
	}

	// 2. 目录遍历与 FOPEN_CACHE_DIR 生命周期验证
	subDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("dir_lifecycle"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)

	subChildRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: subDir.Ino,
		Name:      []byte("entry_a.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	subDirFileIno = subChildRes.Inode.Ino
	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       subDirFileIno,
		Fh:        subChildRes.Fh,
		Offset:    0,
		Data:      []byte("dir-child-payload"),
		SessionID: &sessA,
	})
	require.NoError(t, err)
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       subDirFileIno,
		Fh:        subChildRes.Fh,
		SessionID: &sessA,
	}))

	// 首次 OpenDir：精确 1 条单语句 CTE SQL，记录该会话下已知目录版本
	tracer.reset()
	openDir1, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       subDir.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	openDirQueries1 := tracer.snapshot()
	require.Len(t, openDirQueries1, 1, "OpenDir 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "OpenDir(1)", openDirQueries1[0])
	assert.Contains(t, openDirQueries1[0], "set_config('synchronous_commit', 'off', true)", "OpenDir CTE 必须内联异步 WAL 提交")

	// ReadDir 与 ReadDirPlus 遍历验证
	dirEntries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
		Ino:       subDir.Ino,
		Fh:        openDir1.Fh,
		Offset:    0,
		Limit:     16,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	require.Len(t, dirEntries, 3, "ReadDir 必须返回 '.', '..' 与 'entry_a.txt' 共 3 项")
	assert.Equal(t, []byte("."), dirEntries[0].Name)
	assert.Equal(t, []byte(".."), dirEntries[1].Name)
	assert.Equal(t, []byte("entry_a.txt"), dirEntries[2].Name)
	assert.Equal(t, subDirFileIno, dirEntries[2].Ino)

	dirEntriesPlus, err := mgr.ReadDirPlus(ctx, meta.ReadDirPlusParams{
		Ino:       subDir.Ino,
		Fh:        openDir1.Fh,
		Offset:    0,
		Limit:     16,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	require.Len(t, dirEntriesPlus, 3, "ReadDirPlus 必须返回 '.', '..' 与 'entry_a.txt' 共 3 项")
	assert.Equal(t, []byte("entry_a.txt"), dirEntriesPlus[2].Name)
	require.NotNil(t, dirEntriesPlus[2].Inode)
	assert.Equal(t, uint64(len("dir-child-payload")), dirEntriesPlus[2].Inode.Attr.Size)

	// 常规目录 ReleaseDir：精确 1 条单语句 CTE SQL（ReleaseDirFast）
	tracer.reset()
	err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       subDir.Ino,
		Fh:        openDir1.Fh,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	releaseDirQueries1 := tracer.snapshot()
	require.Len(t, releaseDirQueries1, 1, "常规目录 ReleaseDir 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "ReleaseDir(Normal)", releaseDirQueries1[0])
	assert.Contains(t, releaseDirQueries1[0], "set_config('synchronous_commit', 'off', true)", "ReleaseDirFast CTE 必须内联异步 WAL 提交")

	// 同一会话重复 OpenDir（目录未发生修改）：命中 FOPEN_CACHE_DIR
	tracer.reset()
	openDir2, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       subDir.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	openDirQueries2 := tracer.snapshot()
	require.Len(t, openDirQueries2, 1, "重复 OpenDir 必须精确仅触发 1 条单语句 CTE SQL 查询")
	assertNoExplicitTxOrSetLocalSQL(t, "OpenDir(2)", openDirQueries2[0])
	assert.NotZero(t, openDir2.OpenFlags&meta.FOPEN_CACHE_DIR, "同一会话在目录未变时重复 OpenDir 必须返回 FOPEN_CACHE_DIR")
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       subDir.Ino,
		Fh:        openDir2.Fh,
		SessionID: &sessA,
	}))

	// 在目录内新增子项后，再次 OpenDir 必须作废 FOPEN_CACHE_DIR
	addedRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: subDir.Ino,
		Name:      []byte("entry_b.txt"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	addedFileIno = addedRes.Inode.Ino
	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       addedFileIno,
		Fh:        addedRes.Fh,
		SessionID: &sessA,
	}))

	openDir3, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       subDir.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Zero(t, openDir3.OpenFlags&meta.FOPEN_CACHE_DIR, "目录发生子项新增后 OpenDir 必须作废 FOPEN_CACHE_DIR")
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       subDir.Ino,
		Fh:        openDir3.Fh,
		SessionID: &sessA,
	}))

	// 删除新增子项后，再次 OpenDir 同样作废 FOPEN_CACHE_DIR，随后无修改再次 OpenDir 重新命中 FOPEN_CACHE_DIR
	openDir4, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       subDir.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.NotZero(t, openDir4.OpenFlags&meta.FOPEN_CACHE_DIR, "已知目录版本更新后未再修改时 OpenDir 必须重新命中 FOPEN_CACHE_DIR")
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       subDir.Ino,
		Fh:        openDir4.Fh,
		SessionID: &sessA,
	}))

	require.NoError(t, mgr.Unlink(ctx, subDir.Ino, []byte("entry_b.txt")))
	addedFileIno = 0

	openDir5, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       subDir.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	assert.Zero(t, openDir5.OpenFlags&meta.FOPEN_CACHE_DIR, "目录发生子项删除后 OpenDir 必须作废 FOPEN_CACHE_DIR")
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       subDir.Ino,
		Fh:        openDir5.Fh,
		SessionID: &sessA,
	}))

	// 3. 孤儿目录生命周期验证：持有目录句柄期间执行 Rmdir 解链为孤儿（nlink == 0），ReleaseDir 识别 orphan 信号并转入慢路径回收
	diskBeforeOrphanDir, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)

	orphanDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("orphan_dir_target"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(t, err)

	diskAfterMkdirOrphan, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, diskBeforeOrphanDir.UsedInodes+1, diskAfterMkdirOrphan.UsedInodes, "Mkdir 后磁盘 UsedInodes 必须增加 1")

	openOrphanDir, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:       orphanDir.Ino,
		Flags:     meta.O_RDONLY,
		SessionID: &sessA,
	})
	require.NoError(t, err)

	// 在持有目录句柄 openOrphanDir.Fh 期间执行 Rmdir：目录解链为孤儿（nlink == 0），但因仍有活跃句柄暂不物理删除
	require.NoError(t, mgr.Rmdir(ctx, disk.RootIno, []byte("orphan_dir_target")))

	var orphanNlink int32
	require.NoError(t, pool.QueryRow(ctx, "SELECT nlink FROM inodes WHERE ino = $1", int64(orphanDir.Ino)).Scan(&orphanNlink))
	assert.Equal(t, int32(0), orphanNlink, "持有目录句柄期间 Rmdir 后目录必须处于 nlink == 0 的孤儿状态")

	var handleCountBeforeRelease int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_handles WHERE ino = $1 AND fh = $2", int64(orphanDir.Ino), int64(openOrphanDir.Fh)).Scan(&handleCountBeforeRelease))
	assert.Equal(t, int64(1), handleCountBeforeRelease, "ReleaseDir 前孤儿目录句柄必须仍存在于 file_handles")

	diskWhileOrphanOpen, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, diskBeforeOrphanDir.UsedInodes+1, diskWhileOrphanOpen.UsedInodes, "孤儿目录句柄未释放前磁盘 UsedInodes 不得提前退还")

	// 调用 ReleaseDir：ReleaseDirFast 返回 orphan 信号，无缝转入慢路径删除句柄、物理回收孤儿目录 Inode 并退还 UsedInodes 配额
	tracer.reset()
	err = mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino:       orphanDir.Ino,
		Fh:        openOrphanDir.Fh,
		SessionID: &sessA,
	})
	require.NoError(t, err)
	orphanReleaseDirQueries := tracer.snapshot()
	require.Greater(t, len(orphanReleaseDirQueries), 1, "孤儿目录 ReleaseDir 必须在 ReleaseDirFast 返回 orphan 后转入多语句写事务慢路径")

	var remainingInodeRows, remainingHandleRows int64
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM inodes WHERE ino = $1", int64(orphanDir.Ino)).Scan(&remainingInodeRows))
	require.NoError(t, pool.QueryRow(ctx, "SELECT COUNT(*) FROM file_handles WHERE ino = $1", int64(orphanDir.Ino)).Scan(&remainingHandleRows))
	assert.Equal(t, int64(0), remainingInodeRows, "孤儿目录 ReleaseDir 后 inodes 表必须无残留")
	assert.Equal(t, int64(0), remainingHandleRows, "孤儿目录 ReleaseDir 后 file_handles 表必须无残留")

	diskAfterOrphanReclaim, err := mgr.GetDisk(ctx, disk.DiskID)
	require.NoError(t, err)
	assert.Equal(t, diskBeforeOrphanDir.UsedInodes, diskAfterOrphanReclaim.UsedInodes, "孤儿目录 ReleaseDir 物理回收后必须精确退还磁盘 UsedInodes 配额")
}

// TestADR0028_AsyncWAL_NonLeakageAndControlPlaneSyncCommit 验证：
//  1. 执行单语句快路径（Create、Write、Release）与多语句数据面写事务（Mkdir、SetAttr、SetXattr、RemoveXattr、SetLk、Open(O_TRUNC)、
//     Rename、Unlink、Rmdir、RenewSession、CloseSession）时，事务内均执行 set_config('synchronous_commit', 'off', true) 且事务内
//     current_setting('synchronous_commit') 为 "off"；事务提交或回滚归还连接后，在 MaxConns = 1 连接池上执行 SHOW synchronous_commit 恒为 "on"；
//  2. 当数据面写事务遇到可重试冲突（如 SQLSTATE 40001）回滚并重开新事务重试时，重试事务内依然生效 synchronous_commit = off；
//  3. 通过 QueryTracer 与事务内探针执行控制面操作 CreateDisk、UpdateDisk、DeleteDisk，断言其执行的 SQL 语句中绝不包含
//     synchronous_commit 为 off 的设置，且在控制面事务内查询 current_setting('synchronous_commit') 恒为 "on"。
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

	rawStore := db.NewStore(singlePool)
	inspector := &txSyncCommitInspector{Store: rawStore}
	dataDir := t.TempDir()
	localBS := meta.NewLocalFSBlockStore(dataDir)
	mgr := meta.New(inspector, meta.WithBlockStore(localBS))

	assertPoolSyncCommitOn := func(step string) {
		var syncCommit string
		require.NoError(t, singlePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&syncCommit))
		require.Equal(t, "on", syncCommit, "%s 后复用同一物理连接执行 SHOW synchronous_commit 必须为 on", step)
	}

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
			cleanupWriteTestInoAndSlices(t, rawStore, mgr, int64(ino), fh)
		}
	})

	assertPoolSyncCommitOn("Create")

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:    ino,
		Fh:     fh,
		Offset: 0,
		Data:   bytes.Repeat([]byte{'W'}, 4096),
	})
	require.NoError(t, err)
	assertPoolSyncCommitOn("Write")

	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino: ino,
		Fh:  fh,
	}))
	assertPoolSyncCommitOn("Release")

	// 单语句 Open（非 O_TRUNC）、OpenDir 与常规目录 ReleaseDir 在 MaxConns = 1 连接池上同样不污染 SHOW synchronous_commit
	tracer.reset()
	openNonTruncRes, err := mgr.Open(ctx, meta.OpenParams{
		Ino:   ino,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	openNonTruncQueries := tracer.snapshot()
	require.Len(t, openNonTruncQueries, 1, "单语句 Open(非 O_TRUNC) 必须精确仅触发 1 条 CTE SQL")
	assertNoExplicitTxOrSetLocalSQL(t, "Open(non-O_TRUNC)", openNonTruncQueries[0])
	assert.Contains(t, openNonTruncQueries[0], "set_config('synchronous_commit', 'off', true)")
	assertPoolSyncCommitOn("Open(non-O_TRUNC)")

	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino: ino,
		Fh:  openNonTruncRes.Fh,
	}))
	assertPoolSyncCommitOn("Release(after Open non-O_TRUNC)")

	tracer.reset()
	openDirSingleRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
		Ino:   disk.RootIno,
		Flags: meta.O_RDONLY,
	})
	require.NoError(t, err)
	openDirSingleQueries := tracer.snapshot()
	require.Len(t, openDirSingleQueries, 1, "单语句 OpenDir 必须精确仅触发 1 条 CTE SQL")
	assertNoExplicitTxOrSetLocalSQL(t, "OpenDir", openDirSingleQueries[0])
	assert.Contains(t, openDirSingleQueries[0], "set_config('synchronous_commit', 'off', true)")
	assertPoolSyncCommitOn("OpenDir")

	tracer.reset()
	require.NoError(t, mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
		Ino: disk.RootIno,
		Fh:  openDirSingleRes.Fh,
	}))
	releaseDirSingleQueries := tracer.snapshot()
	require.Len(t, releaseDirSingleQueries, 1, "常规目录 ReleaseDir 必须精确仅触发 1 条 CTE SQL")
	assertNoExplicitTxOrSetLocalSQL(t, "ReleaseDir", releaseDirSingleQueries[0])
	assert.Contains(t, releaseDirSingleQueries[0], "set_config('synchronous_commit', 'off', true)")
	assertPoolSyncCommitOn("ReleaseDir")

	// 在默认多连接池上同样验证 SHOW synchronous_commit 为 "on"
	var basePoolSyncCommit string
	require.NoError(t, basePool.QueryRow(ctx, "SHOW synchronous_commit").Scan(&basePoolSyncCommit))
	require.Equal(t, "on", basePoolSyncCommit)

	// 2. 验证数据面多语句写事务（含 execWriteTxWithRetry 及 SetXattr/RemoveXattr）开启事务级 synchronous_commit = off 且无连接池状态泄漏
	assertDataPlaneAsyncWAL := func(opName string, fn func() error) {
		tracer.reset()
		inspector.reset()
		require.NoError(t, fn(), "数据面写操作 %s 必须成功", opName)

		queries := tracer.snapshot()
		require.NotEmpty(t, queries, "数据面写操作 %s 必须触发数据库查询", opName)
		hasAsyncWALConfig := false
		for _, q := range queries {
			lower := strings.ToLower(q)
			if strings.Contains(lower, "set_config('synchronous_commit', 'off', true)") {
				hasAsyncWALConfig = true
				break
			}
		}
		assert.True(t, hasAsyncWALConfig, "数据面写事务 %s 必须执行 set_config('synchronous_commit', 'off', true), queries: %v", opName, queries)

		observed := inspector.snapshotObserved()
		require.NotEmpty(t, observed, "数据面写事务 %s 必须在事务内被观测到 synchronous_commit 设置", opName)
		for i, val := range observed {
			assert.Equal(t, "off", val, "数据面写事务 %s 第 %d 次尝试事务内 current_setting('synchronous_commit') 必须为 off", opName, i+1)
		}

		assertPoolSyncCommitOn(opName)
	}

	var dirInode *meta.Inode
	assertDataPlaneAsyncWAL("Mkdir", func() error {
		var mErr error
		dirInode, mErr = mgr.Mkdir(ctx, meta.MkdirParams{
			ParentIno: disk.RootIno,
			Name:      []byte("sub_dir"),
			Mode:      0o755,
			UID:       1000,
			GID:       1000,
		})
		return mErr
	})

	// 验证数据面写事务回滚路径（重复 Mkdir 触发唯一约束冲突回滚 + Unlink 目录触发 ErrIsDir 业务错误回滚）
	// 同样开启异步 WAL，且回滚归还连接后连接池 SHOW synchronous_commit 恒为 "on"
	tracer.reset()
	inspector.reset()
	_, dupErr := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("sub_dir"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.ErrorIs(t, dupErr, meta.ErrAlreadyExists)
	assertPoolSyncCommitOn("Mkdir(Rollback)")

	tracer.reset()
	inspector.reset()
	unlinkDirErr := mgr.Unlink(ctx, disk.RootIno, []byte("sub_dir"))
	require.ErrorIs(t, unlinkDirErr, meta.ErrIsDir)
	assert.Equal(t, []string{"off"}, inspector.snapshotObserved(), "回滚的数据面写事务内 current_setting('synchronous_commit') 必须为 off")
	assertPoolSyncCommitOn("Unlink(ErrIsDir Rollback)")

	// 验证数据面写事务遇到可重试冲突（首次尝试返回 SQLSTATE 40001 回滚并重开新事务重试）时，重试事务内依然保持 synchronous_commit = off
	tracer.reset()
	inspector.reset()
	inspector.onBeforeReturn = func(_ context.Context, attempt int, _ db.Querier) error {
		if attempt == 1 {
			return &pgconn.PgError{Code: "40001", Message: "injected serialization_failure for retry test"}
		}
		return nil
	}
	newMode := uint32(0o600)
	_, err = mgr.SetAttr(ctx, ino, meta.SetAttrInput{
		Mode: &newMode,
	})
	require.NoError(t, err, "注入一次 40001 后 SetAttr 重试必须成功")
	assert.Equal(t, []string{"off", "off"}, inspector.snapshotObserved(),
		"首次回滚尝试与重开的重试事务内 current_setting('synchronous_commit') 必须均为 off")
	assertPoolSyncCommitOn("SetAttr(RetryAfter40001)")
	inspector.reset()

	assertDataPlaneAsyncWAL("SetAttr", func() error {
		mode644 := uint32(0o644)
		_, sErr := mgr.SetAttr(ctx, ino, meta.SetAttrInput{
			Mode: &mode644,
		})
		return sErr
	})

	assertDataPlaneAsyncWAL("SetXattr", func() error {
		return mgr.SetXattr(ctx, meta.SetXattrParams{
			Ino:   ino,
			Name:  []byte("user.adr0028"),
			Value: []byte("async-wal"),
			Flags: 0,
		})
	})

	assertDataPlaneAsyncWAL("RemoveXattr", func() error {
		return mgr.RemoveXattr(ctx, ino, []byte("user.adr0028"))
	})

	sessID := uint64(time.Now().UnixNano() & 0x3fffffffffffffff)
	t.Cleanup(func() {
		_, _ = singlePool.Exec(context.Background(), "DELETE FROM client_sessions WHERE session_id = $1", int64(sessID))
	})

	assertDataPlaneAsyncWAL("RenewSession", func() error {
		_, rErr := mgr.RenewSession(ctx, sessID)
		return rErr
	})

	var truncFh uint64
	assertDataPlaneAsyncWAL("Open(O_TRUNC)", func() error {
		openRes, oErr := mgr.Open(ctx, meta.OpenParams{
			Ino:       ino,
			Flags:     meta.O_RDWR | meta.O_TRUNC,
			SessionID: &sessID,
		})
		if oErr == nil {
			truncFh = openRes.Fh
		}
		return oErr
	})

	assertDataPlaneAsyncWAL("SetLk", func() error {
		return mgr.SetLk(ctx, meta.SetLkParams{
			Ino:       ino,
			Fh:        truncFh,
			SessionID: sessID,
			LockOwner: 1001,
			Lk: meta.FileLock{
				Start: 0,
				End:   127,
				Type:  meta.F_WRLCK,
				PID:   1234,
			},
		})
	})

	require.NoError(t, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       ino,
		Fh:        truncFh,
		SessionID: &sessID,
	}))
	assertPoolSyncCommitOn("Release(truncFh)")

	assertDataPlaneAsyncWAL("Rename", func() error {
		return mgr.Rename(ctx, meta.RenameParams{
			OldParentIno: disk.RootIno,
			OldName:      []byte("async_wal_check.bin"),
			NewParentIno: dirInode.Ino,
			NewName:      []byte("renamed_check.bin"),
		})
	})

	assertDataPlaneAsyncWAL("Unlink", func() error {
		return mgr.Unlink(ctx, dirInode.Ino, []byte("renamed_check.bin"))
	})

	assertDataPlaneAsyncWAL("Rmdir", func() error {
		return mgr.Rmdir(ctx, disk.RootIno, []byte("sub_dir"))
	})

	assertDataPlaneAsyncWAL("CloseSession", func() error {
		return mgr.CloseSession(ctx, sessID)
	})

	// 清理测试文件的 chunks/slice_refs，为后续 DeleteDisk 控制面操作做准备
	cleanupWriteTestInoAndSlices(t, rawStore, mgr, int64(ino))

	// 3. 通过 QueryTracer 与事务内探针执行控制面操作 CreateDisk、UpdateDisk、DeleteDisk
	assertControlPlaneSyncCommit := func(opName string, fn func() error) {
		tracer.reset()
		inspector.reset()
		require.NoError(t, fn(), "控制面操作 %s 必须成功", opName)
		queries := tracer.snapshot()
		require.NotEmpty(t, queries, "控制面操作 %s 必须触发数据库查询", opName)
		for _, q := range queries {
			lower := strings.ToLower(q)
			assert.False(t, strings.Contains(lower, "synchronous_commit") && strings.Contains(lower, "off"),
				"控制面操作 %s 的 SQL 语句中绝不得包含将 synchronous_commit 设为 off 的设置, got: %s", opName, q)
		}

		observed := inspector.snapshotObserved()
		require.NotEmpty(t, observed, "控制面操作 %s 必须在事务内被观测到 synchronous_commit 设置", opName)
		for i, val := range observed {
			assert.Equal(t, "on", val, "控制面操作 %s 第 %d 次尝试事务内 current_setting('synchronous_commit') 必须恒为 on", opName, i+1)
		}

		assertPoolSyncCommitOn(opName)
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

func BenchmarkADR0028_ReadLifecycle4K(b *testing.B) {
	_, _, mgr, disk, sessID := setupBenchmarkMetaTestDB(b)
	if mgr == nil {
		return
	}
	ctx := context.Background()
	payload4K := bytes.Repeat([]byte{'R'}, 4096)

	cRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: disk.RootIno,
		Name:      []byte("read_lifecycle_4k_target.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(b, err)

	_, err = mgr.Write(ctx, &meta.WriteRequest{
		Ino:       cRes.Inode.Ino,
		Fh:        cRes.Fh,
		Offset:    0,
		Data:      payload4K,
		SessionID: &sessID,
	})
	require.NoError(b, err)

	require.NoError(b, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       cRes.Inode.Ino,
		Fh:        cRes.Fh,
		SessionID: &sessID,
	}))

	targetIno := cRes.Inode.Ino

	b.ResetTimer()
	b.SetBytes(4096)
	start := time.Now()
	for i := 0; i < b.N; i++ {
		openRes, err := mgr.Open(ctx, meta.OpenParams{
			Ino:       targetIno,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("Open failed: %v", err)
		}

		readRes, err := mgr.Read(ctx, &meta.ReadRequest{
			Ino:       targetIno,
			Fh:        openRes.Fh,
			Offset:    0,
			Size:      4096,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("Read failed: %v", err)
		}
		if len(readRes.Data) != 4096 {
			b.Fatalf("unexpected read len: %d", len(readRes.Data))
		}

		if err := mgr.Release(ctx, meta.ReleaseParams{
			Ino:       targetIno,
			Fh:        openRes.Fh,
			SessionID: &sessID,
		}); err != nil {
			b.Fatalf("Release failed: %v", err)
		}
	}
	b.StopTimer()
	reportADR0028BenchMetrics(b, "BenchmarkADR0028_ReadLifecycle4K", time.Since(start))
}

func BenchmarkADR0028_DirLifecycle(b *testing.B) {
	_, _, mgr, disk, sessID := setupBenchmarkMetaTestDB(b)
	if mgr == nil {
		return
	}
	ctx := context.Background()

	benchDir, err := mgr.Mkdir(ctx, meta.MkdirParams{
		ParentIno: disk.RootIno,
		Name:      []byte("bench_dir_lifecycle"),
		Mode:      0o755,
		UID:       1000,
		GID:       1000,
	})
	require.NoError(b, err)

	childRes, err := mgr.Create(ctx, meta.CreateParams{
		ParentIno: benchDir.Ino,
		Name:      []byte("dir_entry.bin"),
		Mode:      meta.S_IFREG | 0o644,
		UID:       1000,
		GID:       1000,
		Flags:     meta.O_RDWR,
		SessionID: &sessID,
	})
	require.NoError(b, err)
	require.NoError(b, mgr.Release(ctx, meta.ReleaseParams{
		Ino:       childRes.Inode.Ino,
		Fh:        childRes.Fh,
		SessionID: &sessID,
	}))

	b.ResetTimer()
	start := time.Now()
	for i := 0; i < b.N; i++ {
		openDirRes, err := mgr.OpenDir(ctx, meta.OpenDirParams{
			Ino:       benchDir.Ino,
			Flags:     meta.O_RDONLY,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("OpenDir failed: %v", err)
		}

		entries, err := mgr.ReadDir(ctx, meta.ReadDirParams{
			Ino:       benchDir.Ino,
			Fh:        openDirRes.Fh,
			Offset:    0,
			Limit:     64,
			SessionID: &sessID,
		})
		if err != nil {
			b.Fatalf("ReadDir failed: %v", err)
		}
		if len(entries) != 3 {
			b.Fatalf("expected 3 dir entries, got %d", len(entries))
		}

		if err := mgr.ReleaseDir(ctx, meta.ReleaseDirParams{
			Ino:       benchDir.Ino,
			Fh:        openDirRes.Fh,
			SessionID: &sessID,
		}); err != nil {
			b.Fatalf("ReleaseDir failed: %v", err)
		}
	}
	b.StopTimer()
	reportADR0028BenchMetrics(b, "BenchmarkADR0028_DirLifecycle", time.Since(start))
}

func BenchmarkADR0028_SmallFileLifecycle(b *testing.B) {
	b.Run("Create", BenchmarkADR0028_Create)
	b.Run("Write4K", BenchmarkADR0028_Write4K)
	b.Run("Release", BenchmarkADR0028_Release)
	b.Run("FullLifecycle4K", BenchmarkADR0028_FullLifecycle4K)
	b.Run("ReadLifecycle4K", BenchmarkADR0028_ReadLifecycle4K)
	b.Run("DirLifecycle", BenchmarkADR0028_DirLifecycle)
}
