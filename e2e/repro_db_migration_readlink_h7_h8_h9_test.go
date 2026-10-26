//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"connectrpc.com/connect"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/starsrc/bowl/e2e/testutil"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// getPostgresContainer 返回 PostgreSQL 容器名称，默认值为 "bowl-postgres"。
func getPostgresContainer() string {
	if c := os.Getenv("BOWL_POSTGRES_CONTAINER"); c != "" {
		return c
	}
	return "bowl-postgres"
}

// getPostgresDB 返回目标数据库名称，默认值为 "bowl"。
func getPostgresDB() string {
	if db := os.Getenv("BOWL_POSTGRES_DB"); db != "" {
		return db
	}
	return "bowl"
}

// runPsql 在真实 PostgreSQL 容器中执行 SQL 语句并返回标准输出或错误。
// 所有 exec.Command 统一使用 exec.CommandContext(ctx, ...) 保证受测试超时控制。
func runPsql(ctx context.Context, dbName string, sqlText string) (string, error) {
	container := getPostgresContainer()
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container,
		"psql", "-U", "postgres", "-d", dbName, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1")
	cmd.Stdin = strings.NewReader(sqlText)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("执行 psql 失败 (container: %s, db: %s): %w, stderr: %s, sql:\n%s",
			container, dbName, err, stderr.String(), sqlText)
	}

	return strings.TrimSpace(stdout.String()), nil
}

// execPsql 在真实 PostgreSQL 容器中执行 SQL 语句，并在失败时调用 t.Fatalf。
func execPsql(t testing.TB, ctx context.Context, dbName string, sqlText string) string {
	t.Helper()
	out, err := runPsql(ctx, dbName, sqlText)
	if err != nil {
		t.Fatalf("%v", err)
	}
	return out
}

// createUniqueTestDB 创建一个测试期间专用的唯一临时数据库，并注册严格的清理检查。
func createUniqueTestDB(t testing.TB, ctx context.Context, prefix string) string {
	t.Helper()
	dbName := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano())
	execPsql(t, ctx, "postgres", fmt.Sprintf("CREATE DATABASE %s;", dbName))
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		if _, err := runPsql(cleanupCtx, "postgres", fmt.Sprintf("DROP DATABASE IF EXISTS %s WITH (FORCE);", dbName)); err != nil {
			t.Errorf("清理临时数据库 %s 失败: %v", dbName, err)
		}
	})
	return dbName
}

// runMigrationTx 将迁移 SQL 包裹在显式事务 BEGIN; ... COMMIT; 中执行，
// 对齐 Goose 的单迁移事务原子性语义，且执行失败时返回 error 供测试判定安全回滚。
func runMigrationTx(ctx context.Context, dbName string, sqlText string) error {
	txSQL := fmt.Sprintf("BEGIN;\n%s\nCOMMIT;\n", sqlText)
	_, err := runPsql(ctx, dbName, txSQL)
	return err
}

// verifyBackendDatabaseConsistency 核验 RPC 客户端与 SQL Helper 必须连接同一个后端数据库实例。
func verifyBackendDatabaseConsistency(t testing.TB, ctx context.Context, dbName string, diskID string, rootIno uint64) {
	t.Helper()
	query := fmt.Sprintf("SELECT root_ino FROM disks WHERE disk_id = '%s';", diskID)
	out := execPsql(t, ctx, dbName, query)
	require.Equal(t, fmt.Sprintf("%d", rootIno), out,
		"RPC 客户端创建的 disk 与 SQL Helper 连接的数据库实例不一致 (diskID=%s)", diskID)
}

// getMigrationsDir 获取 services/storage/db/migrations 目录的绝对路径。
func getMigrationsDir(t testing.TB) string {
	t.Helper()
	candidates := []string{
		"services/storage/db/migrations",
		"../services/storage/db/migrations",
		"/Users/fanxing/code/starsrc/bowl/services/storage/db/migrations",
	}
	for _, dir := range candidates {
		if info, err := os.Stat(dir); err == nil && info.IsDir() {
			abs, err := filepath.Abs(dir)
			require.NoError(t, err)
			return abs
		}
	}
	t.Fatalf("未找到迁移脚本目录 services/storage/db/migrations")
	return ""
}

// parseMigrationSQL 从指定迁移文件中解析出 -- +goose Up 与 -- +goose Down 的原始 SQL 语句。
func parseMigrationSQL(t testing.TB, migrationFile string) (upSQL string, downSQL string) {
	t.Helper()
	contentBytes, err := os.ReadFile(migrationFile)
	require.NoError(t, err, "读取迁移文件失败: %s", migrationFile)
	content := string(contentBytes)

	const upMarker = "-- +goose Up"
	const downMarker = "-- +goose Down"

	upIdx := strings.Index(content, upMarker)
	downIdx := strings.Index(content, downMarker)
	require.True(t, upIdx >= 0, "迁移文件缺少 -- +goose Up: %s", migrationFile)

	var rawUp, rawDown string
	if downIdx >= 0 {
		rawUp = content[upIdx+len(upMarker) : downIdx]
		rawDown = content[downIdx+len(downMarker):]
	} else {
		rawUp = content[upIdx+len(upMarker):]
	}

	cleanLines := func(raw string) string {
		lines := strings.Split(raw, "\n")
		var out []string
		for _, line := range lines {
			trimmed := strings.TrimSpace(line)
			if strings.HasPrefix(trimmed, "-- +goose") {
				continue
			}
			out = append(out, line)
		}
		return strings.TrimSpace(strings.Join(out, "\n"))
	}

	return cleanLines(rawUp), cleanLines(rawDown)
}

// extractGooseSection 从指定迁移文件中解析并返回 -- +goose Up 或 Down 的原始 SQL 语句。
// filenameOrPath 支持绝对路径、相对路径或仅文件名（将在 services/storage/db/migrations 目录下查找）。
func extractGooseSection(t testing.TB, filenameOrPath string, isUp bool) string {
	t.Helper()
	var fullPath string
	if filepath.IsAbs(filenameOrPath) {
		fullPath = filenameOrPath
	} else if strings.Contains(filenameOrPath, string(filepath.Separator)) {
		fullPath = filenameOrPath
	} else {
		fullPath = filepath.Join(getMigrationsDir(t), filenameOrPath)
	}
	upSQL, downSQL := parseMigrationSQL(t, fullPath)
	if isUp {
		return upSQL
	}
	return downSQL
}

// startLockWaitMonitor 启动一个轻量协程每 15ms 监控一次 disks/client_sessions/inodes 跨表锁等待，
// 返回 stop 闭包，在调用时停止监控并返回记录的锁等待详情。
func startLockWaitMonitor(ctx context.Context, dbName string) func() []string {
	monCtx, cancel := context.WithCancel(ctx)
	var obs []string
	var mu sync.Mutex
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		query := `SELECT format('pid=%s blocked on rel=%s query=[%s]', l.pid, l.relation::regclass, a.query) ` +
			`FROM pg_locks l JOIN pg_stat_activity a ON l.pid = a.pid ` +
			`WHERE a.datname = current_database() AND NOT l.granted ` +
			`AND l.relation IN ('disks'::regclass, 'client_sessions'::regclass, 'inodes'::regclass);`
		for {
			select {
			case <-monCtx.Done():
				return
			case <-time.After(25 * time.Millisecond):
				out, err := runPsql(monCtx, dbName, query)
				if err == nil && strings.TrimSpace(out) != "" {
					mu.Lock()
					for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
						if line != "" {
							obs = append(obs, line)
						}
					}
					mu.Unlock()
				}
			}
		}
	}()

	return func() []string {
		cancel()
		wg.Wait()
		mu.Lock()
		defer mu.Unlock()
		res := make([]string, len(obs))
		copy(res, obs)
		return res
	}
}

// fetchPostgresDeadlockLogs 从 PostgreSQL 服务端提取 startTs 以来的死锁检测日志详情。
func fetchPostgresDeadlockLogs(ctx context.Context, startTs string) string {
	cmd := exec.CommandContext(ctx, "docker", "logs", "--since", startTs, getPostgresContainer())
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Sprintf("获取 PostgreSQL 日志失败: %v", err)
	}
	var matched []string
	for _, line := range strings.Split(string(out), "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.Contains(trimmed, "deadlock detected") ||
			strings.Contains(trimmed, "waits for") ||
			strings.Contains(trimmed, "blocked by process") ||
			strings.HasPrefix(trimmed, "Process ") {
			matched = append(matched, trimmed)
		}
	}
	if len(matched) == 0 {
		return ""
	}
	// 最多返回前 20 条匹配行，避免超长日志截断
	if len(matched) > 20 {
		matched = matched[:20]
	}
	return strings.Join(matched, "\n")
}

// newStressStorageClient 创建一个针对高并发测试优化连接池与超时设置的客户端。
func newStressStorageClient() storagev1connect.StorageServiceClient {
	transport := &http.Transport{
		MaxIdleConns:        100,
		MaxIdleConnsPerHost: 50,
		MaxConnsPerHost:     50,
		IdleConnTimeout:     90 * time.Second,
	}
	return storagev1connect.NewStorageServiceClient(
		&http.Client{
			Timeout:   120 * time.Second,
			Transport: transport,
		},
		testutil.GetStorageServiceAddr(),
	)
}

// TestE2E_H7_CrossTableLockOrderDeadlock 复现 H7 缺陷：
// 1. Create (Disk->Session) 与 TmpFile (Session->Disk) 锁序反转死锁；
// 2. DeleteDisk (Disk->Inode) 与 SetAttr 扩容 (Inode->Disk) 锁序反转死锁。
func TestE2E_H7_CrossTableLockOrderDeadlock(t *testing.T) {
	client := newStressStorageClient()
	dbName := getPostgresDB()

	// 子测试 1：Create (Disk -> Session) 与 TmpFile (Session -> Disk) 锁序反转
	t.Run("CreateVsTmpFile_DiskSessionLockOrderInversion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 180*time.Second)
		defer cancel()

		diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "repro-h7-create-tmpfile-disk",
			MaxBytes: 1024 * 1024 * 1024,
		}))
		require.NoError(t, err, "创建测试虚拟磁盘失败")
		disk := diskResp.Msg.GetDisk()
		rootIno := disk.GetRootIno()
		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
			if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
				t.Errorf("清理磁盘失败 (diskID: %s): %v", disk.GetDiskId(), err)
			}
		})

		// 核验后端一致性
		verifyBackendDatabaseConsistency(t, ctx, dbName, disk.GetDiskId(), rootIno)

		// 注册共享客户端会话 (SessionId: 410001)
		const sessionID = uint64(410001)
		_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: sessionID,
		}))
		require.NoError(t, err, "注册测试会话失败")

		// 在磁盘根目录下预先创建 24 个子目录，使并发请求分散于不同父目录 Inode
		const numDirs = 24
		dirInos := make([]uint64, numDirs)
		for i := 0; i < numDirs; i++ {
			mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				ParentIno: rootIno,
				Name:      []byte(fmt.Sprintf("dir_deadlock_%d", i)),
				Mode:      0o755,
			}))
			require.NoError(t, err, "创建测试子目录 %d 失败", i)
			dirInos[i] = mkdirResp.Msg.GetInode().GetIno()
		}

		// 记录测试开始前的数据库时间戳（UTC ISO8601，带 5 秒前向冗余以便完整提取服务端日志）与 deadlocks 计数
		startTs := execPsql(t, ctx, dbName, "SELECT to_char((clock_timestamp() - interval '5 seconds') at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"');")
		deadlocksBeforeStr := execPsql(t, ctx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
		deadlocksBefore, err := strconv.ParseInt(deadlocksBeforeStr, 10, 64)
		require.NoError(t, err, "解析 deadlocksBefore 失败: %s", deadlocksBeforeStr)

		// 启动轻量监控协程观测跨表锁等待
		stopMonitor := startLockWaitMonitor(ctx, dbName)

		// 启动 12 个 Create 协程与 12 个 TmpFile 协程并发执行，每个协程执行 15 次循环，
		// 共 12 * 15 = 180 次 Create 与 180 次 TmpFile 并发交错，确保未修复版本稳定触发死锁
		const numWorkers = 12
		const loopCount = 15
		type opResult struct {
			op  string
			err error
		}
		totalOps := numWorkers * loopCount * 2
		resCh := make(chan opResult, totalOps)
		var wg sync.WaitGroup
		startCh := make(chan struct{})

		// 启动 12 个 Create 协程（锁序：Disk -> Session），分别作用于子目录 0..11
		for i := 0; i < numWorkers; i++ {
			workerID := i
			dirIno := dirInos[workerID]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				for j := 0; j < loopCount; j++ {
					reqCtx, reqCancel := context.WithTimeout(ctx, 15*time.Second)
					resp, err := client.Create(reqCtx, connect.NewRequest(&storagev1.CreateRequest{
						ParentIno: dirIno,
						Name:      []byte(fmt.Sprintf("create_file_%d_%d", workerID, j)),
						Mode:      0o644,
						Flags:     1, // O_WRONLY
						SessionId: protoUint64(sessionID),
					}))
					reqCancel()
					if err == nil {
						if resp.Msg.GetInode() == nil || resp.Msg.GetFh() == 0 {
							err = fmt.Errorf("Create 返回了无效的 Inode 或 Fh: inode=%v, fh=%d", resp.Msg.GetInode(), resp.Msg.GetFh())
						}
					}
					resCh <- opResult{op: "Create", err: err}
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		// 启动 12 个 TmpFile 协程（锁序：Session -> Disk），分别作用于子目录 12..23
		for i := 0; i < numWorkers; i++ {
			workerID := i
			dirIno := dirInos[workerID+numWorkers]
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-startCh
				for j := 0; j < loopCount; j++ {
					reqCtx, reqCancel := context.WithTimeout(ctx, 15*time.Second)
					resp, err := client.TmpFile(reqCtx, connect.NewRequest(&storagev1.TmpFileRequest{
						ParentIno: dirIno,
						Mode:      0o644,
						Flags:     2, // O_RDWR
						SessionId: protoUint64(sessionID),
					}))
					reqCancel()
					if err == nil {
						if resp.Msg.GetInode() == nil || resp.Msg.GetFh() == 0 {
							err = fmt.Errorf("TmpFile 返回了无效的 Inode 或 Fh: inode=%v, fh=%d", resp.Msg.GetInode(), resp.Msg.GetFh())
						}
					}
					resCh <- opResult{op: "TmpFile", err: err}
					time.Sleep(5 * time.Millisecond)
				}
			}()
		}

		close(startCh)
		wg.Wait()
		close(resCh)

		// 停止锁等待监控
		obs := stopMonitor()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer verifyCancel()

		// 最多 500ms 内轮询 pg_stat_database 的 deadlocks 计数
		deadline := time.Now().Add(500 * time.Millisecond)
		var deadlocksAfter int64
		for {
			deadlocksAfterStr := execPsql(t, verifyCtx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
			deadlocksAfter, err = strconv.ParseInt(deadlocksAfterStr, 10, 64)
			require.NoError(t, err, "解析 deadlocksAfter 失败: %s", deadlocksAfterStr)
			if deadlocksAfter > deadlocksBefore || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		deadlockLogs := fetchPostgresDeadlockLogs(verifyCtx, startTs)
		assert.Equal(t, deadlocksBefore, deadlocksAfter,
			"Create(Disk->Session) 与 TmpFile(Session->Disk) 锁序反转触发了 %d 次 PostgreSQL 40P01 死锁\n观测到的锁等待: %s\nPostgreSQL 日志:\n%s",
			deadlocksAfter-deadlocksBefore, strings.Join(obs, "; "), deadlockLogs)

		// 逐一检查所有并发 RPC 结果
		for res := range resCh {
			require.NoError(t, res.err, "并发 %s 请求不应失败", res.op)
		}
	})

	// 子测试 2：DeleteDisk (Disk -> Inode) 与 SetAttr (Inode -> Disk) 锁序反转
	t.Run("DeleteDiskVsSetAttr_DiskInodeLockOrderInversion", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()

		const numDisks = 3
		const filesPerDisk = 12

		startTs := execPsql(t, ctx, dbName, "SELECT to_char((clock_timestamp() - interval '5 seconds') at time zone 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"');")
		deadlocksBeforeStr := execPsql(t, ctx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
		deadlocksBefore, err := strconv.ParseInt(deadlocksBeforeStr, 10, 64)
		require.NoError(t, err, "解析 deadlocksBefore 失败: %s", deadlocksBeforeStr)

		stopMonitor := startLockWaitMonitor(ctx, dbName)

		var allDeleteErrs []error
		var allSetAttrErrs []error

		for d := 0; d < numDisks; d++ {
			diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
				Name:     fmt.Sprintf("repro-h7-del-setattr-disk-%d", d),
				MaxBytes: 100 * 1024 * 1024,
			}))
			require.NoError(t, err, "创建测试虚拟磁盘失败")
			disk := diskResp.Msg.GetDisk()
			diskID := disk.GetDiskId()
			rootIno := disk.GetRootIno()

			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: diskID}))
				if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
					t.Errorf("清理磁盘失败 (diskID: %s): %v", diskID, err)
				}
			})

			if d == 0 {
				verifyBackendDatabaseConsistency(t, ctx, dbName, diskID, rootIno)
			}

			// 在该磁盘下预先创建常规文件
			fileInos := make([]uint64, filesPerDisk)
			for f := 0; f < filesPerDisk; f++ {
				mknodResp, err := client.Mknod(ctx, connect.NewRequest(&storagev1.MknodRequest{
					ParentIno: rootIno,
					Name:      []byte(fmt.Sprintf("file_%d", f)),
					Mode:      0o100644,
				}))
				require.NoError(t, err, "预先创建常规文件失败 (disk %d, file %d)", d, f)
				fileInos[f] = mknodResp.Msg.GetInode().GetIno()
			}

			// 启动并发协程：多个协程连续调用 SetAttr 修改不同 Size（触发 Inode -> Disk 加锁），
			// 同时另一个协程并发调用 DeleteDisk 删除该磁盘（触发 Disk -> Inode 加锁）。
			const numSetAttrWorkers = 4
			const setattrLoops = 6
			setAttrCh := make(chan error, numSetAttrWorkers*setattrLoops)
			deleteCh := make(chan error, 1)

			var diskWg sync.WaitGroup
			startCh := make(chan struct{})

			for w := 0; w < numSetAttrWorkers; w++ {
				diskWg.Add(1)
				go func(workerID int) {
					defer diskWg.Done()
					<-startCh
					for l := 0; l < setattrLoops; l++ {
						ino := fileInos[(workerID+l)%filesPerDisk]
						newSize := uint64(1024 * (l + 1))
						_, err := client.SetAttr(ctx, connect.NewRequest(&storagev1.SetAttrRequest{
							Ino:  ino,
							Size: protoUint64(newSize),
						}))
						setAttrCh <- err
					}
				}(w)
			}

			diskWg.Add(1)
			go func() {
				defer diskWg.Done()
				<-startCh
				_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
					DiskId: diskID,
				}))
				deleteCh <- err
			}()

			close(startCh)
			diskWg.Wait()
			close(setAttrCh)
			close(deleteCh)

			for dErr := range deleteCh {
				allDeleteErrs = append(allDeleteErrs, dErr)
			}
			for sErr := range setAttrCh {
				allSetAttrErrs = append(allSetAttrErrs, sErr)
			}
		}

		obs := stopMonitor()

		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer verifyCancel()

		// 汇合后轮询 pg_stat_database.deadlocks，断言死锁未增加
		deadline := time.Now().Add(500 * time.Millisecond)
		var deadlocksAfter int64
		for {
			deadlocksAfterStr := execPsql(t, verifyCtx, dbName, fmt.Sprintf("SELECT deadlocks FROM pg_stat_database WHERE datname = '%s';", dbName))
			deadlocksAfter, err = strconv.ParseInt(deadlocksAfterStr, 10, 64)
			require.NoError(t, err, "解析 deadlocksAfter 失败: %s", deadlocksAfterStr)
			if deadlocksAfter > deadlocksBefore || time.Now().After(deadline) {
				break
			}
			time.Sleep(50 * time.Millisecond)
		}

		deadlockLogs := fetchPostgresDeadlockLogs(verifyCtx, startTs)
		require.Equal(t, deadlocksBefore, deadlocksAfter,
			"DeleteDisk(Disk->Inode) 与 SetAttr(Inode->Disk) 锁序反转触发了 PostgreSQL 40P01 死锁\n观测到的锁等待: %s\nPostgreSQL 日志:\n%s",
			strings.Join(obs, "; "), deadlockLogs)

		// 严格校验所有 DeleteDisk 与 SetAttr 的返回错误（修复 Standards #11，无条件校验 NotFound + ENOENT）
		for _, dErr := range allDeleteErrs {
			require.NoError(t, dErr, "DeleteDisk 必须成功")
		}
		for _, sErr := range allSetAttrErrs {
			if sErr != nil {
				testutil.RequireConnectError(t, sErr, connect.CodeNotFound, "ENOENT")
			}
		}
	})
}

// prepareCorruptedSymlinkTarget 负责安全地保存约束、临时解除约束、注入损坏数据并在测试清理时还原约束。
func prepareCorruptedSymlinkTarget(t *testing.T, ctx context.Context, dbName string, rootIno, targetIno uint64, entryName, sqlValueExpr string) {
	t.Helper()
	// 1. 查询约束定义与 NOT NULL 属性
	queryDef := "SELECT COALESCE((SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE conrelid = 'inodes'::regclass AND conname = 'chk_inodes_symlink_target'), '');"
	origConstraintDef := execPsql(t, ctx, dbName, queryDef)
	queryNotNull := "SELECT COALESCE((SELECT attnotnull::text FROM pg_attribute WHERE attrelid = 'inodes'::regclass AND attname = 'symlink_target'), 'false');"
	wasNotNull := execPsql(t, ctx, dbName, queryNotNull)

	// 2. 在修改表结构前立即注册 t.Cleanup
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cleanupCancel()
		// ① 删除损坏数据（避免脏数据阻止约束恢复）
		_, _ = runPsql(cleanupCtx, dbName, fmt.Sprintf("DELETE FROM entries WHERE parent_ino = %d AND ino = %d;", rootIno, targetIno))
		if _, err := runPsql(cleanupCtx, dbName, fmt.Sprintf("DELETE FROM inodes WHERE ino = %d;", targetIno)); err != nil {
			t.Errorf("清理损坏 Inode %d 失败: %v", targetIno, err)
		}
		// ② 恢复 NOT NULL（若原本为 true）
		if wasNotNull == "true" {
			if _, err := runPsql(cleanupCtx, dbName, "ALTER TABLE inodes ALTER COLUMN symlink_target SET NOT NULL;"); err != nil {
				t.Errorf("恢复 inodes.symlink_target NOT NULL 约束失败: %v", err)
			}
		}
		// ③ 恢复 chk_inodes_symlink_target 约束
		if origConstraintDef != "" {
			checkSQL := "SELECT count(*) FROM pg_constraint WHERE conrelid = 'inodes'::regclass AND conname = 'chk_inodes_symlink_target';"
			cntStr, err := runPsql(cleanupCtx, dbName, checkSQL)
			if err != nil || cntStr == "0" {
				restoreSQL := fmt.Sprintf("ALTER TABLE inodes ADD CONSTRAINT chk_inodes_symlink_target %s;", origConstraintDef)
				if _, err := runPsql(cleanupCtx, dbName, restoreSQL); err != nil {
					t.Errorf("恢复 chk_inodes_symlink_target 约束失败: %v", err)
				}
			}
		}
	})

	// 3. 临时解除约束
	execPsql(t, ctx, dbName, "ALTER TABLE inodes DROP CONSTRAINT IF EXISTS chk_inodes_symlink_target;")
	if wasNotNull == "true" {
		execPsql(t, ctx, dbName, "ALTER TABLE inodes ALTER COLUMN symlink_target DROP NOT NULL;")
	}

	// 4. 执行 UPDATE 注入损坏数据并断言返回值
	updateSQL := fmt.Sprintf("UPDATE inodes SET symlink_target = %s WHERE ino = %d RETURNING ino;", sqlValueExpr, targetIno)
	out := execPsql(t, ctx, dbName, updateSQL)
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.True(t, len(lines) >= 1, "UPDATE 应有输出")
	require.Equal(t, fmt.Sprintf("%d", targetIno), lines[0], "UPDATE 必须精确命中 1 行目标符号链接")
}

// TestE2E_H8_ReadlinkCorruptedEmptyTargetReturnsEIO 复现 H8 缺陷：
// 当符号链接元数据损坏（symlink_target 为空或 NULL）时，Readlink 错误伪装成 ErrInvalidArgument，
// 导致服务端错误返回 InvalidArgument (EINVAL) 而非 ADR-0007 要求的 Internal (EIO)。
func TestE2E_H8_ReadlinkCorruptedEmptyTargetReturnsEIO(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()
	dbName := getPostgresDB()

	// 创建测试虚拟磁盘
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "repro-h8-readlink-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err, "创建测试虚拟磁盘失败")
	disk := diskResp.Msg.GetDisk()
	rootIno := disk.GetRootIno()
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
		if err != nil && connect.CodeOf(err) != connect.CodeNotFound {
			t.Errorf("清理磁盘失败 (diskID: %s): %v", disk.GetDiskId(), err)
		}
	})

	// 核验后端一致性
	verifyBackendDatabaseConsistency(t, ctx, dbName, disk.GetDiskId(), rootIno)

	// 子场景 1：符号链接目标损坏为空字节串 (''::bytea)
	t.Run("EmptyBytea", func(t *testing.T) {
		// 1. 创建合法符号链接
		symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: rootIno,
			Name:      []byte("symlink_empty_corrupted"),
			Target:    []byte("/valid/target/path"),
		}))
		require.NoError(t, err, "创建符号链接失败")
		inoEmpty := symResp.Msg.GetInode().GetIno()

		// 确认正常状态下 Readlink 可成功读取
		rlResp, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{Ino: inoEmpty}))
		require.NoError(t, err, "正常状态下 Readlink 应成功")
		require.Equal(t, []byte("/valid/target/path"), rlResp.Msg.GetTarget())

		// 2. 模拟后端元数据损坏（统一调用 prepareCorruptedSymlinkTarget 辅助函数）
		prepareCorruptedSymlinkTarget(t, ctx, dbName, rootIno, inoEmpty, "symlink_empty_corrupted", "''::bytea")

		// 回读确认 symlink_target 确已变为 ''::bytea (octet_length == 0)
		gotOctetLen := execPsql(t, ctx, dbName, fmt.Sprintf("SELECT octet_length(symlink_target) FROM inodes WHERE ino = %d;", inoEmpty))
		require.Equal(t, "0", gotOctetLen, "回读确认 symlink_target octet_length 必须为 0")

		// 3. 调用 Readlink 读取损坏的空符号链接目标
		_, err = client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{Ino: inoEmpty}))

		// 4. 正确性断言（触发真红）：
		// ADR-0007 明确规定符号链接元数据损坏必须返回 ConnectRPC CodeInternal 与 ErrorInfo.Reason == "EIO"。
		// 当前代码错误匹配 ErrInvalidArgument，实际返回 CodeInvalidArgument 与 "EINVAL"，断言直接报红！
		testutil.RequireConnectError(t, err, connect.CodeInternal, "EIO")
	})

	// 子场景 2：符号链接目标损坏为 NULL
	t.Run("NullBytea", func(t *testing.T) {
		// 1. 创建合法符号链接
		symResp, err := client.Symlink(ctx, connect.NewRequest(&storagev1.SymlinkRequest{
			ParentIno: rootIno,
			Name:      []byte("symlink_null_corrupted"),
			Target:    []byte("/valid/target/path"),
		}))
		require.NoError(t, err, "创建符号链接失败")
		inoNull := symResp.Msg.GetInode().GetIno()

		// 确认正常状态下 Readlink 可成功读取
		rlResp, err := client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{Ino: inoNull}))
		require.NoError(t, err, "正常状态下 Readlink 应成功")
		require.Equal(t, []byte("/valid/target/path"), rlResp.Msg.GetTarget())

		// 2. 模拟后端元数据损坏（统一调用 prepareCorruptedSymlinkTarget 辅助函数）
		prepareCorruptedSymlinkTarget(t, ctx, dbName, rootIno, inoNull, "symlink_null_corrupted", "NULL")

		// 回读确认 symlink_target 确已变为 NULL
		checkNull := execPsql(t, ctx, dbName, fmt.Sprintf("SELECT (symlink_target IS NULL)::text FROM inodes WHERE ino = %d;", inoNull))
		require.Equal(t, "true", checkNull, "回读确认 symlink_target 必须为 NULL")

		// 3. 调用 Readlink 读取损坏的 NULL 符号链接目标
		_, err = client.Readlink(ctx, connect.NewRequest(&storagev1.ReadlinkRequest{Ino: inoNull}))

		// 4. 正确性断言（触发真红）：
		// 必须返回 ConnectRPC CodeInternal 与 ErrorInfo.Reason == "EIO"。
		// 当前代码错误返回 CodeInvalidArgument 与 "EINVAL"，断言直接报红！
		testutil.RequireConnectError(t, err, connect.CodeInternal, "EIO")
	})
}

// TestE2E_H9_MigrationDataFidelityAndNestedDiskTopology 复现 H9 缺陷：
// 覆盖数据库迁移中的 Unicode 私用区静默篡改、回滚截断丢失数据及存量嵌套磁盘破坏。
func TestE2E_H9_MigrationDataFidelityAndNestedDiskTopology(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	dbName := getPostgresDB()

	// 子测试 1：Unicode 私用区字符在升级迁移中被静默篡改 (PUACharacterSilentCorruptionOnUp)
	t.Run("PUACharacterSilentCorruptionOnUp", func(t *testing.T) {
		testDB := createUniqueTestDB(t, ctx, "bowl_h9_mig_test_pua")

		// 依次在显式事务中执行前 3 个迁移的 Up SQL
		earlyMigrations := []string{
			"20261015165500_create_metadata_tables.sql",
			"20261016180000_add_symlink_target_to_inodes.sql",
			"20261017000000_create_file_handles_table.sql",
		}
		for _, f := range earlyMigrations {
			upSQL := extractGooseSection(t, f, true)
			err := runMigrationTx(ctx, testDB, upSQL)
			require.NoError(t, err, "执行前置迁移 %s 失败", f)
		}

		// 插入包含合法 Unicode 私用区字符 U+E041 (UTF-8 编码为 \xee\x81\x81，对应 chr(57409)) 的数据
		setupSQL := `
			INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev) VALUES (1, 16877, 4096, 2, 0, 0, 0);
			INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, symlink_target) VALUES (2, 41453, 3, 1, 0, 0, 0, chr(57409));
			INSERT INTO entries (parent_ino, name, ino) VALUES (1, chr(57409), 2);
		`
		execPsql(t, ctx, testDB, setupSQL)

		// 确认升级前 UTF-8 编码十六进制为 ee8181 (3 字节)
		beforeHex := execPsql(t, ctx, testDB, "SELECT encode(convert_to(name, 'UTF8'), 'hex') FROM entries WHERE ino = 2;")
		require.Equal(t, "ee8181", beforeHex, "升级前 entries.name 必须为 3 字节 UTF-8 码值 ee8181")

		// 在显式事务中执行 20261017010000 与 20261017020000 的真实 Up SQL
		upSymlink := extractGooseSection(t, "20261017010000_convert_symlink_target_to_bytea.sql", true)
		err := runMigrationTx(ctx, testDB, upSymlink)
		require.NoError(t, err, "执行 20261017010000 Up 迁移失败")

		upEntries := extractGooseSection(t, "20261017020000_convert_entries_name_to_bytea.sql", true)
		err = runMigrationTx(ctx, testDB, upEntries)
		require.NoError(t, err, "执行 20261017020000 Up 迁移失败")

		// 正确性断言（触发真红）：
		// 升级为 BYTEA 后，原始 UTF-8 字节必须无损保持 "ee8181"！
		// 当前迁移脚本误将 U+E041 判定为回滚转义字符并扣减 57344，被静默篡改为 ASCII 'A' ("41")，断言直接报红！
		gotNameHex := execPsql(t, ctx, testDB, "SELECT encode(name, 'hex') FROM entries WHERE ino = 2;")
		gotSymHex := execPsql(t, ctx, testDB, "SELECT encode(symlink_target, 'hex') FROM inodes WHERE ino = 2;")
		assert.Equal(t, "ee8181", gotNameHex, "迁移升级后 entries.name 应保持原始 UTF-8 字节 ee8181")
		assert.Equal(t, "ee8181", gotSymHex, "迁移升级后 inodes.symlink_target 应保持原始 UTF-8 字节 ee8181")
	})

	// 子测试 2：长符号链接在 Down/Up 往返中被强行截断丢失数据 (SymlinkTargetDownUpTruncationDataLoss)
	t.Run("SymlinkTargetDownUpTruncationDataLoss", func(t *testing.T) {
		testDB := createUniqueTestDB(t, ctx, "bowl_h9_mig_test_trunc")

		// 依次执行前 3 个迁移的 Up SQL
		earlyMigrations := []string{
			"20261015165500_create_metadata_tables.sql",
			"20261016180000_add_symlink_target_to_inodes.sql",
			"20261017000000_create_file_handles_table.sql",
		}
		for _, f := range earlyMigrations {
			upSQL := extractGooseSection(t, f, true)
			err := runMigrationTx(ctx, testDB, upSQL)
			require.NoError(t, err, "执行前置迁移 %s 失败", f)
		}

		// 执行 20261017010000 的 Up SQL
		upSymlink := extractGooseSection(t, "20261017010000_convert_symlink_target_to_bytea.sql", true)
		downSymlink := extractGooseSection(t, "20261017010000_convert_symlink_target_to_bytea.sql", false)
		err := runMigrationTx(ctx, testDB, upSymlink)
		require.NoError(t, err, "执行 20261017010000 Up 迁移失败")

		// 插入长度为 2000 字节的合法非 UTF-8 符号链接目标 (2000 个 \x80，完全符合 PATH_MAX 契约)
		insertSQL := `
			INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, symlink_target)
			VALUES (3, 41453, 2000, 1, 0, 0, 0, decode(repeat('80', 2000), 'hex'));
		`
		execPsql(t, ctx, testDB, insertSQL)

		initialLen := execPsql(t, ctx, testDB, "SELECT octet_length(symlink_target) FROM inodes WHERE ino = 3;")
		require.Equal(t, "2000", initialLen, "初始符号链接目标长度必须为 2000 字节")

		// 使用 runMigrationTx（包裹在 BEGIN; ... COMMIT; 单事务中）执行 20261017010000 的 Down SQL
		downErr := runMigrationTx(ctx, testDB, downSymlink)
		if downErr != nil {
			// 若未来采用显式拒绝回滚方案，绝不能接受任意 SQL 错误：
			// 必须断言明确的业务拒绝错误，且不包含语法/不存在/权限错误
			errStr := downErr.Error()
			require.False(t, strings.Contains(strings.ToLower(errStr), "syntax error"), "Down 失败绝不能是语法错误: %v", downErr)
			require.False(t, strings.Contains(strings.ToLower(errStr), "does not exist"), "Down 失败绝不能是缺失对象错误: %v", downErr)
			require.False(t, strings.Contains(strings.ToLower(errStr), "permission denied"), "Down 失败绝不能是权限错误: %v", downErr)

			// 验证事务回滚后 symlink_target 列类型仍为 bytea
			colType := execPsql(t, ctx, testDB, "SELECT udt_name FROM information_schema.columns WHERE table_name = 'inodes' AND column_name = 'symlink_target';")
			require.Equal(t, "bytea", colType, "事务安全回滚后 symlink_target 必须保持 bytea 类型")
		} else {
			// 若 Down 执行成功，则继续使用 runMigrationTx 执行 Up SQL 完成往返
			upErr := runMigrationTx(ctx, testDB, upSymlink)
			require.NoError(t, upErr, "Down 成功后重新执行 Up 必须成功")
		}

		// 无论哪种情况（Down 回滚保持 BYTEA 原数据无损，或 Down/Up 往返后），
		// 最终查询 Inode 3 的 octet_length(symlink_target) 与完整十六进制内容
		gotLenStr := execPsql(t, ctx, testDB, "SELECT octet_length(symlink_target) FROM inodes WHERE ino = 3;")
		assert.Equal(t, "2000", gotLenStr, "Down -> Up 往返后符号链接目标字节长度必须保持 2000 字节无损")

		gotHex := execPsql(t, ctx, testDB, "SELECT encode(symlink_target, 'hex') FROM inodes WHERE ino = 3;")
		assert.Equal(t, strings.Repeat("80", 2000), gotHex, "Down -> Up 往返后符号链接目标的完整字节内容必须 100% 保真")
	})

	// 子测试 3：存量嵌套磁盘在迁移后被父盘跨盘 Rmdir 摧毁子盘根 (NestedDiskMigrationCrossDiskRmdirDestroysChildRoot)
	t.Run("NestedDiskMigrationCrossDiskRmdirDestroysChildRoot", func(t *testing.T) {
		// 阶段 A：在独立唯一数据库 testDB 中 100% 还原前 9 个迁移 + 旧 disks 表历史前态，真实执行第 10 个迁移
		testDB := createUniqueTestDB(t, ctx, "bowl_h9_nested")

		// 依次读取并调用 runMigrationTx 执行前 9 个原始迁移文件
		earlyMigrations := []string{
			"20261015165500_create_metadata_tables.sql",
			"20261016180000_add_symlink_target_to_inodes.sql",
			"20261017000000_create_file_handles_table.sql",
			"20261017010000_convert_symlink_target_to_bytea.sql",
			"20261017020000_convert_entries_name_to_bytea.sql",
			"20261017030000_create_inode_xattrs_table.sql",
			"20261018000000_create_file_locks_tables.sql",
			"20261018010000_add_session_id_to_file_handles.sql",
			"20261019000000_align_juicefs_meta_foundation.sql",
		}
		for _, f := range earlyMigrations {
			upSQL := extractGooseSection(t, f, true)
			err := runMigrationTx(ctx, testDB, upSQL)
			require.NoError(t, err, "执行前置迁移 %s 失败", f)
		}

		// 建立 ADR-0014 时代的旧版 disks 表（当时尚无 used_inodes 列）与存量嵌套 Disk 数据
		oldSchemaAndDataSQL := `
			CREATE TABLE disks (
				disk_id UUID PRIMARY KEY,
				name VARCHAR(255) NOT NULL DEFAULT '',
				root_ino BIGINT NOT NULL UNIQUE,
				max_bytes BIGINT NOT NULL,
				used_bytes BIGINT NOT NULL DEFAULT 0,
				created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
				updated_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
				CONSTRAINT chk_disks_root_ino CHECK (root_ino > 0),
				CONSTRAINT chk_disks_capacity CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)
			);
			-- 父盘根 Inode 10 (自指 parent_ino = 10)
			INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino) VALUES (10, 16877, 4096, 3, 0, 0, 0, 10);
			-- 子盘根 Inode 20 (作为父盘下的子目录创建，parent_ino = 10)
			INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino) VALUES (20, 16877, 4096, 2, 0, 0, 0, 10);
			-- 父盘指向子盘根的目录项
			INSERT INTO entries (parent_ino, name, ino) VALUES (10, convert_to('nested_mount', 'UTF8'), 20);
			-- 子盘下的一个常规文件 Inode 21 (size = 4096)
			INSERT INTO inodes (ino, mode, size, nlink, uid, gid, rdev, parent_ino) VALUES (21, 33188, 4096, 1, 0, 0, 0, 20);
			INSERT INTO entries (parent_ino, name, ino) VALUES (20, convert_to('child_file.txt', 'UTF8'), 21);
			-- 在旧版 disks 表注册父盘 (root_ino=10) 与嵌套子盘 (root_ino=20)
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes) VALUES ('11111111-1111-4111-8111-111111111111', 'parent-disk', 10, 104857600, 0);
			INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes) VALUES ('22222222-2222-4222-8222-222222222222', 'nested-child-disk', 20, 52428800, 4096);
		`
		execPsql(t, ctx, testDB, oldSchemaAndDataSQL)

		// 真实读取并调用 migErr := runMigrationTx(ctx, testDB, upSQL10) 执行第 10 个迁移 20261023000000 原文
		upSQL10 := extractGooseSection(t, "20261023000000_add_disks_and_inode_root_ino.sql", true)
		migErr := runMigrationTx(ctx, testDB, upSQL10)

		if migErr != nil {
			// 分支 1：迁移采用“预检阻断嵌套磁盘拓扑”方案
			errStr := migErr.Error()
			require.False(t, strings.Contains(strings.ToLower(errStr), "syntax error"), "迁移失败绝不能是语法错误: %v", migErr)
			require.False(t, strings.Contains(strings.ToLower(errStr), "does not exist"), "迁移失败绝不能是缺失对象错误: %v", migErr)
			require.False(t, strings.Contains(strings.ToLower(errStr), "permission denied"), "迁移失败绝不能是权限错误: %v", migErr)

			// 验证事务原子回滚后 inodes 表未残留 root_ino 列，原数据完好
			colCount := execPsql(t, ctx, testDB, "SELECT count(*) FROM information_schema.columns WHERE table_name = 'inodes' AND column_name = 'root_ino';")
			require.Equal(t, "0", colCount, "事务回滚后 inodes 表不应存在 root_ino 列")
		} else {
			// 分支 2：迁移成功完成
			// 直接审计迁移后的数据库不变量（INV-1 / INV-5）
			gotChildParentIno := execPsql(t, ctx, testDB, "SELECT parent_ino FROM inodes WHERE ino = 20;")
			crossDiskEntryCount := execPsql(t, ctx, testDB, "SELECT COUNT(*) FROM entries WHERE parent_ino = 10 AND ino = 20;")
			gotParentNlink := execPsql(t, ctx, testDB, "SELECT nlink FROM inodes WHERE ino = 10;")

			assert.Equal(t, "20", gotChildParentIno, "20261023000000 迁移后子盘根 parent_ino 必须自指 20，实际仍指向父盘 10")
			assert.Equal(t, "0", crossDiskEntryCount, "20261023000000 迁移后绝不应残留父盘指向子盘根的跨盘 entry")
			assert.Equal(t, "2", gotParentNlink, "解除子目录绑定后父盘 nlink 应回退为 2")

			// 验证当该迁移产物在真实 storage-service 上执行父盘 Rmdir 时的行为
			client := testutil.NewStorageClient()
			parentResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
				Name:     "repro-h9-parent-disk",
				MaxBytes: 50 * 1024 * 1024,
			}))
			require.NoError(t, err, "创建父虚拟磁盘失败")
			parentDisk := parentResp.Msg.GetDisk()
			parentRootIno := parentDisk.GetRootIno()

			mkdirResp, err := client.Mkdir(ctx, connect.NewRequest(&storagev1.MkdirRequest{
				ParentIno: parentRootIno,
				Name:      []byte("nested_mount"),
				Mode:      0o755,
			}))
			require.NoError(t, err, "在父盘创建子目录失败")
			childRootIno := mkdirResp.Msg.GetInode().GetIno()

			childDiskID := execPsql(t, ctx, dbName, "SELECT gen_random_uuid();")
			t.Cleanup(func() {
				cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cleanupCancel()
				_, _ = runPsql(cleanupCtx, dbName, fmt.Sprintf("DELETE FROM entries WHERE parent_ino IN (%d, %d);", parentRootIno, childRootIno))
				_, _ = runPsql(cleanupCtx, dbName, fmt.Sprintf("DELETE FROM inodes WHERE ino = %d;", childRootIno))
				_, _ = runPsql(cleanupCtx, dbName, fmt.Sprintf("DELETE FROM disks WHERE disk_id = '%s';", childDiskID))
				_, _ = client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: parentDisk.GetDiskId()}))
			})

			// 在在线库中注册子盘并将 root_ino 设置为 childRootIno
			registerChildSQL := fmt.Sprintf(`
				INSERT INTO disks (disk_id, name, root_ino, max_bytes, used_bytes, used_inodes)
				VALUES ('%s', 'repro-h9-nested-child', %d, 52428800, 0, 1);
				UPDATE inodes SET root_ino = %d WHERE ino = %d;
			`, childDiskID, childRootIno, childRootIno, childRootIno)
			execPsql(t, ctx, dbName, registerChildSQL)

			// 将 testDB 中经 20261023000000 迁移实际计算出的拓扑状态同步应用于在线库
			if crossDiskEntryCount == "0" {
				syncSQL := fmt.Sprintf(`
					DELETE FROM entries WHERE parent_ino = %d AND ino = %d;
					UPDATE inodes SET parent_ino = %d WHERE ino = %d;
					UPDATE inodes SET nlink = nlink - 1 WHERE ino = %d;
				`, parentRootIno, childRootIno, childRootIno, childRootIno, parentRootIno)
				execPsql(t, ctx, dbName, syncSQL)
			}

			// 调用父盘 Rmdir：若迁移已解除跨盘 entry，则必须返回 NotFound (ENOENT)
			_, rmdirErr := client.Rmdir(ctx, connect.NewRequest(&storagev1.RmdirRequest{
				ParentIno: parentRootIno,
				Name:      []byte("nested_mount"),
			}))
			testutil.RequireConnectError(t, rmdirErr, connect.CodeNotFound, "ENOENT")

			// 随后调用 StatFS 与 Lookup(".")
			_, statErr := client.StatFS(ctx, connect.NewRequest(&storagev1.StatFSRequest{Ino: childRootIno}))
			require.NoError(t, statErr, "父盘执行 Rmdir 后，独立子盘根 Inode 绝不能被跨盘删除，StatFS 必须成功")

			_, lookupErr := client.Lookup(ctx, connect.NewRequest(&storagev1.LookupRequest{
				ParentIno: childRootIno,
				Name:      []byte("."),
			}))
			require.NoError(t, lookupErr, "独立子盘根目录 Lookup(.) 必须成功")
		}
	})
}

func protoUint64(v uint64) *uint64 {
	return &v
}
