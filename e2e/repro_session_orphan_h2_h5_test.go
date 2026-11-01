//go:build e2e

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
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

// fastPsqlSession 提供常驻交互式 psql 管道，单次查询延迟 < 1ms，避免单次 exec 的 60ms 启动开销。
type fastPsqlSession struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	reader *bufio.Reader
	mu     sync.Mutex
}

func newFastPsqlSession(ctx context.Context, t testing.TB) *fastPsqlSession {
	t.Helper()
	container, db := getSessionPsqlConfig()
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container,
		"psql", "-U", "postgres", "-d", db, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1")
	stdin, err := cmd.StdinPipe()
	require.NoError(t, err)
	stdout, err := cmd.StdoutPipe()
	require.NoError(t, err)

	err = cmd.Start()
	require.NoError(t, err)

	ps := &fastPsqlSession{
		cmd:    cmd,
		stdin:  stdin,
		reader: bufio.NewReader(stdout),
	}

	t.Cleanup(func() {
		_ = stdin.Close()
		_ = cmd.Wait()
	})

	return ps
}

func (ps *fastPsqlSession) query(t testing.TB, sql string) string {
	t.Helper()
	ps.mu.Lock()
	defer ps.mu.Unlock()

	_, err := fmt.Fprintf(ps.stdin, "%s\n", sql)
	require.NoError(t, err)
	line, err := ps.reader.ReadString('\n')
	require.NoError(t, err)
	return strings.TrimSpace(line)
}

// getSessionPsqlConfig 获取数据库容器名与数据库名配置。
func getSessionPsqlConfig() (string, string) {
	container := os.Getenv("BOWL_POSTGRES_CONTAINER")
	if container == "" {
		container = "bowl-postgres"
	}
	db := os.Getenv("BOWL_POSTGRES_DB")
	if db == "" {
		db = "bowl"
	}
	return container, db
}

// execSessionPsql 在目标 Postgres 容器中执行 SQL 语句并返回标准输出。
func execSessionPsql(ctx context.Context, t testing.TB, sql string) string {
	t.Helper()
	container, db := getSessionPsqlConfig()
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container,
		"psql", "-U", "postgres", "-d", db, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1")
	cmd.Stdin = strings.NewReader(sql)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	if err := cmd.Run(); err != nil {
		t.Fatalf("执行 psql 失败: %v, stderr: %s, sql:\n%s", err, stderr.String(), sql)
	}

	return strings.TrimSpace(stdout.String())
}

// verifySessionDiskInDB 验证 RPC 客户端与 SQL 连接指向同一个后端实例。
func verifySessionDiskInDB(ctx context.Context, t testing.TB, diskID string, wantRootIno uint64) {
	t.Helper()
	sql := fmt.Sprintf("SELECT root_ino FROM disks WHERE disk_id = '%s'", diskID)
	out := execSessionPsql(ctx, t, sql)
	require.NotEmpty(t, out, "psql 查询 disk_id=%s 结果不能为空，必须与 RPC 指向同一后端实例", diskID)
	gotRootIno, err := strconv.ParseUint(out, 10, 64)
	require.NoError(t, err, "解析 root_ino 失败: %s", out)
	require.Equal(t, wantRootIno, gotRootIno, "RPC 客户端与 SQL 连接必须指向同一个后端实例")
}

// requireShortLeaseTTL 验证当前测试环境租约 TTL <= 5 秒，杜绝 60s 默认租约导致超时。
func requireShortLeaseTTL(ctx context.Context, t testing.TB, client storagev1connect.StorageServiceClient, sessionID uint64) {
	t.Helper()
	_, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
		SessionId: sessionID,
	}))
	require.NoError(t, err, "前置续约探测失败")
	sql := fmt.Sprintf("SELECT EXTRACT(EPOCH FROM (expires_at - clock_timestamp()))::int FROM client_sessions WHERE session_id = %d", sessionID)
	out := execSessionPsql(ctx, t, sql)
	require.NotEmpty(t, out, "查询 session TTL 结果不能为空")
	ttl, err := strconv.Atoi(out)
	require.NoError(t, err, "解析 session TTL 失败: %s", out)
	require.LessOrEqual(t, ttl, 5, "当前环境租约 TTL=%d 秒 > 5 秒；请确保测试环境配置短租约（如 2s/3s），杜绝 60s 默认租约耗尽测试预算", ttl)
}

// sleepWithContext 封装受 context 控制的睡眠，严禁使用裸 time.Sleep。
func sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// TestE2E_H2_OrphanReclaimVsRenewSessionRace 复现 H2 核心缺陷：
// 孤儿节点清理与会话续期并发时，DeleteUnreferencedOrphanInodes (metadata.sql:350)
// 在单条 DELETE 中基于语句启动时的旧快照读取 client_sessions.expires_at，却与执行期间递增的 clock_timestamp() 比较。
// 导致目标会话在过期前虽已成功调用 RenewSession 续约，但清理语句因快照旧值与时钟比较误判会话已过期，
// 从而将刚刚成功续期的活跃孤儿文件物理删除。
func TestE2E_H2_OrphanReclaimVsRenewSessionRace(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建目标磁盘 targetDisk（承载孤儿节点与清理竞争）与控制磁盘 controlDisk（对照金丝雀）
	targetDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "h2-race-target-disk",
		MaxBytes: 100 * 1024 * 1024,
	}))
	require.NoError(t, err)
	targetDisk := targetDiskResp.Msg.GetDisk()
	verifySessionDiskInDB(ctx, t, targetDisk.GetDiskId(), targetDisk.GetRootIno())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		cleanSQL := fmt.Sprintf("DELETE FROM inodes WHERE root_ino = %d AND ino <> %d; DELETE FROM client_sessions WHERE session_id = 210001 OR (session_id >= 220000 AND session_id < 240000);", targetDisk.GetRootIno(), targetDisk.GetRootIno())
		execSessionPsql(cleanupCtx, t, cleanSQL)
		if _, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: targetDisk.GetDiskId()})); err != nil {
			if connect.CodeOf(err) != connect.CodeNotFound {
				t.Errorf("清理 targetDisk 失败: %v", err)
			}
		}
	})

	controlDiskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "h2-race-control-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err)
	controlDisk := controlDiskResp.Msg.GetDisk()
	verifySessionDiskInDB(ctx, t, controlDisk.GetDiskId(), controlDisk.GetRootIno())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		execSessionPsql(cleanupCtx, t, "DELETE FROM client_sessions WHERE session_id >= 220000 AND session_id < 240000;")
		if _, err := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: controlDisk.GetDiskId()})); err != nil {
			if connect.CodeOf(err) != connect.CodeNotFound {
				t.Errorf("清理 controlDisk 失败: %v", err)
			}
		}
	})

	// 2. 使用 helperSessionID 在 targetDisk 预填充孤儿文件，并由受控协程持续保活
	helperSessionID := uint64(210001)
	requireShortLeaseTTL(ctx, t, client, helperSessionID)

	t.Log("正在 targetDisk 上预填充 25,000 个孤儿文件以建立稳定的 ~30ms 扫描时间窗...")
	// 保留 1 次 client.TmpFile RPC 调用以验证 helperSessionID 正常创建孤儿节点
	_, cErr := client.TmpFile(ctx, connect.NewRequest(&storagev1.TmpFileRequest{
		ParentIno: targetDisk.GetRootIno(),
		Mode:      0o644,
		Flags:     oRdWr,
		SessionId: &helperSessionID,
	}))
	require.NoError(t, cErr)

	// 随后通过 execSessionPsql 执行单条批量 SQL，在 targetDisk.GetRootIno() 下为 helperSessionID 批量插入 25,000 个 nlink = 0 的孤儿 inode 及对应句柄
	bulkInsertSQL := fmt.Sprintf(`WITH ins_inodes AS (
		INSERT INTO inodes (parent_ino, root_ino, mode, nlink, uid, gid, size, atime, mtime, ctime)
		SELECT %d, %d, 33188, 0, 0, 0, 0, now(), now(), now()
		FROM generate_series(1, 25000)
		RETURNING ino
	)
	INSERT INTO file_handles (ino, session_id, flags)
	SELECT ino, %d, 2 FROM ins_inodes;`,
		targetDisk.GetRootIno(), targetDisk.GetRootIno(), helperSessionID)
	execSessionPsql(ctx, t, bulkInsertSQL)
	t.Log("批量预填充 25,000 个孤儿文件完成")

	// 保持 helperSessionID 租约在测试期间不过期，避免背景孤儿被提前清理，
	// 使用直接的 SQL 更新租约并更新统计信息（不调用全表扫描的 RenewSession RPC，避免占用 DB 连接池与触发锁竞争）
	execSessionPsql(ctx, t, fmt.Sprintf("UPDATE client_sessions SET expires_at = clock_timestamp() + interval '1 hour' WHERE session_id = %d; ANALYZE inodes; ANALYZE file_handles;", helperSessionID))

	// 3. 循环轮次，最多 8 轮复现 H2 竞态
	targetSessionBase := uint64(220000)
	sweeperSessionBase := uint64(230000)
	validObservedRounds := 0
	fastPsql := newFastPsqlSession(ctx, t)

	for round := 0; round < 8; round++ {
		targetSessionID := targetSessionBase + uint64(round*20)
		sweeperSessionID := sweeperSessionBase + uint64(round*20)

		// 3.0 预先通过 RenewSession 注册激活 targetSessionID（Create 仅做只读会话存活校验，不隐式创建未注册会话）
		_, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: targetSessionID,
		}))
		require.NoError(t, err)

		// 3.1 在 controlDisk 上创建金丝雀常规文件（保持 nlink=1，绝不 Unlink）
		canaryName := []byte(fmt.Sprintf("canary_r%d.txt", round))
		canaryCreateResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: controlDisk.GetRootIno(),
			Name:      canaryName,
			Mode:      0o644,
			Flags:     oRdWr,
			SessionId: &targetSessionID,
		}))
		require.NoError(t, err)
		canaryIno := canaryCreateResp.Msg.GetInode().GetIno()
		canaryFh := canaryCreateResp.Msg.GetFh()

		// 3.2 在 targetDisk 上创建受害者孤儿文件
		victimName := []byte(fmt.Sprintf("victim_r%d.tmp", round))
		victimCreateResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: targetDisk.GetRootIno(),
			Name:      victimName,
			Mode:      0o644,
			Flags:     oRdWr,
			SessionId: &targetSessionID,
		}))
		require.NoError(t, err)
		victimIno := victimCreateResp.Msg.GetInode().GetIno()
		victimFh := victimCreateResp.Msg.GetFh()

		_, err = client.Unlink(ctx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: targetDisk.GetRootIno(),
			Name:      victimName,
		}))
		require.NoError(t, err)

		// 3.3 重新续期并记录精确过期时间点 E
		_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: targetSessionID,
		}))
		require.NoError(t, err)

		// 保持 helperSessionID 不过期（直接通过 SQL 更新，不触发全表孤儿扫描）
		execSessionPsql(ctx, t, fmt.Sprintf("UPDATE client_sessions SET expires_at = clock_timestamp() + interval '1 hour' WHERE session_id = %d;", helperSessionID))

		// 注册 6 个 sweeper 会话（各分配独立会话 ID 避免行锁自死锁）
		for s := 0; s < 6; s++ {
			_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
				SessionId: sweeperSessionID + uint64(s),
			}))
			require.NoError(t, err)
		}

		// 通过 fastPsql (<1ms) 获取距离旧过期时刻 E 的精确剩余毫秒数与过期微秒值
		querySql := fmt.Sprintf(
			"SELECT COALESCE((SELECT (EXTRACT(EPOCH FROM (expires_at - clock_timestamp())) * 1000)::bigint::text || '|' || expires_at::text FROM client_sessions WHERE session_id = %d), '0|1970-01-01');",
			targetSessionID,
		)
		outStr := fastPsql.query(t, querySql)
		parts := strings.Split(outStr, "|")
		require.Len(t, parts, 2, "psql 输出格式必须为 remMs|expiresAt")
		remMs, err := strconv.ParseInt(parts[0], 10, 64)
		require.NoError(t, err)
		oldExpiresAtStr := parts[1]

		if remMs < 300 {
			_, err = client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
				SessionId: targetSessionID,
			}))
			require.NoError(t, err)
			execSessionPsql(ctx, t, fmt.Sprintf("UPDATE client_sessions SET expires_at = clock_timestamp() + interval '1 hour' WHERE session_id = %d;", helperSessionID))
			outStr = fastPsql.query(t, querySql)
			parts = strings.Split(outStr, "|")
			require.Len(t, parts, 2)
			remMs, err = strconv.ParseInt(parts[0], 10, 64)
			require.NoError(t, err)
			oldExpiresAtStr = parts[1]
		}

		// 使用 sleepWithContext 等待至距离旧过期时刻 E 剩余约 115ms
		waitToWindow := time.Duration(remMs-115) * time.Millisecond
		if waitToWindow > 0 {
			require.NoError(t, sleepWithContext(ctx, waitToWindow))
		}

		// 启动 6 个 sweeper 协程以 28ms 错峰跨越过期点 (0ms~140ms) 调用独立会话的 RenewSession
		sweepGroupCtx, sweepGroupCancel := context.WithCancel(ctx)
		sweeperErrCh := make(chan error, 16)
		var sweeperWg sync.WaitGroup

		sweepersDone := false
		t.Cleanup(func() {
			if !sweepersDone {
				sweepGroupCancel()
				sweeperWg.Wait()
			}
		})

		for s := 0; s < 6; s++ {
			sweeperWg.Add(1)
			go func(idx int) {
				defer sweeperWg.Done()
				sSessionID := sweeperSessionID + uint64(idx)
				_ = sleepWithContext(sweepGroupCtx, time.Duration(idx)*28*time.Millisecond)
				sCtx, sCancel := context.WithTimeout(sweepGroupCtx, 5*time.Second)
				defer sCancel()
				_, sErr := client.RenewSession(sCtx, connect.NewRequest(&storagev1.RenewSessionRequest{
					SessionId: sSessionID,
				}))
				if sErr != nil && sweepGroupCtx.Err() == nil {
					sweeperErrCh <- sErr
				}
			}(s)
		}

		// 主协程在启动 sweeper 组后错峰等待 25ms~40ms（使 Target 在约 E - 45ms 提交，稳稳落入 S4/S5 旧快照评估跨越时刻）
		targetDelay := time.Duration(25+(round%4)*5) * time.Millisecond
		_ = sleepWithContext(ctx, targetDelay)

		_, renewErr := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: targetSessionID,
		}))
		require.NoError(t, renewErr, "目标会话续约操作必须成功")

		// 立即通过 fastPsql 查询数据库，验证提交时刻是否在旧过期时刻之前（彻底删除 updated_at 判定）
		checkSql := fmt.Sprintf(
			"SELECT COALESCE((SELECT (clock_timestamp() < '%s'::timestamptz)::text || '|' || (expires_at > '%s'::timestamptz)::text FROM client_sessions WHERE session_id = %d), 'false|false');",
			oldExpiresAtStr, oldExpiresAtStr, targetSessionID,
		)
		checkOut := fastPsql.query(t, checkSql)
		checkParts := strings.Split(checkOut, "|")
		require.Len(t, checkParts, 2)
		committedBeforeOldExpiry := checkParts[0]
		newExpiryCommitted := checkParts[1]

		// 等待全部 sweeper 协程完成并 require.NoError 校验所有 sweeper 调用成功
		sweeperWg.Wait()
		sweepersDone = true
		sweepGroupCancel()
		close(sweeperErrCh)
		for sErr := range sweeperErrCh {
			require.NoError(t, sErr, "sweeper 协程 RenewSession 必须全部成功")
		}

		// 查询 sweeper 是否已跨越旧到期时间 E
		diffMs := fastPsql.query(t, fmt.Sprintf("SELECT (EXTRACT(EPOCH FROM (clock_timestamp() - '%s'::timestamptz)) * 1000)::bigint::text;", oldExpiresAtStr))
		sweptSql := fmt.Sprintf("SELECT (clock_timestamp() > '%s'::timestamptz)::text;", oldExpiresAtStr)
		sweptPastOldExpiry := fastPsql.query(t, sweptSql)

		if committedBeforeOldExpiry == "true" && newExpiryCommitted == "true" && sweptPastOldExpiry == "true" {
			validObservedRounds++
			t.Logf("第 %d 轮：100%% 确证目标会话 %d 在旧到期时间 (%s) 前提交续期成功，且 sweeper 跨越旧到期时间，0 纳秒空窗！",
				round, targetSessionID, oldExpiresAtStr)

			// 先对 controlDisk 的 canaryIno 调用只读 Fsync
			_, canaryErr := client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
				Ino:       canaryIno,
				Fh:        canaryFh,
				SessionId: &targetSessionID,
			}))
			require.NoError(t, canaryErr, "金丝雀句柄 Fsync 必须成功，证实会话从未被 DeleteExpiredClientSessions 清理")

			// 直接通过 fastPsql 审计 victimIno 在 inodes 表和 file_handles 表中的存在性
			inodeCountStr := fastPsql.query(t, fmt.Sprintf("SELECT COUNT(*) FROM inodes WHERE ino = %d;", victimIno))
			fhCountStr := fastPsql.query(t, fmt.Sprintf("SELECT COUNT(*) FROM file_handles WHERE fh = %d;", victimFh))
			inodeCount, convErr1 := strconv.Atoi(inodeCountStr)
			require.NoError(t, convErr1, "解析 inodeCount 失败: %s", inodeCountStr)
			fhCount, convErr2 := strconv.Atoi(fhCountStr)
			require.NoError(t, convErr2, "解析 fhCount 失败: %s", fhCountStr)

			// 无条件断言
			require.Equal(t, 1, inodeCount, "H2 缺陷触发: 会话 %d 已在旧租约到期前成功提交续期且金丝雀句柄完好，但活跃孤儿文件 ino=%d 仍被 DeleteOrphanInodesByRoot 因旧快照与 clock_timestamp() 时间差物理误删", targetSessionID, victimIno)
			require.Equal(t, 1, fhCount, "活跃孤儿文件的句柄 fh=%d 不应被级联误删", victimFh)

			_, getAttrErr := client.GetAttr(ctx, connect.NewRequest(&storagev1.GetAttrRequest{
				Ino: victimIno,
			}))
			require.NoError(t, getAttrErr, "受害者孤儿文件 GetAttr 必须成功")

			_, fsyncErr := client.Fsync(ctx, connect.NewRequest(&storagev1.FsyncRequest{
				Ino:       victimIno,
				Fh:        victimFh,
				SessionId: &targetSessionID,
			}))
			require.NoError(t, fsyncErr, "受害者孤儿文件 Fsync 必须成功")

			t.Logf("第 %d 轮：续约在旧到期前提交且本轮 victim 未被误删，继续下一轮检测...", round)
		} else {
			t.Logf("第 %d 轮：未满足有效观测窗口 (diffMs=%s, committedBeforeOldExpiry=%s, newExpiryCommitted=%s, sweptPastOldExpiry=%s)，继续下一轮...",
				round, diffMs, committedBeforeOldExpiry, newExpiryCommitted, sweptPastOldExpiry)
		}
	}

	require.Greater(t, validObservedRounds, 0, "必须至少有 1 轮成功建立 [到期前提交续约 + sweeper 跨越旧到期时间 E] 的有效观测窗口")
}

// TestE2E_H5_ExpiredHandleRevivedBySessionUpsert 复现 H5 核心缺陷：
// 句柄校验与会话续期时间差，已过期句柄借写操作“死而复生”或触发外键异常。
func TestE2E_H5_ExpiredHandleRevivedBySessionUpsert(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	probeSessionID := uint64(290001)
	requireShortLeaseTTL(ctx, t, client, probeSessionID)

	// 子场景 1: 常规文件场景 (RegularFile_ExpiredHandleSetLkSucceedsAndLeavesGhostLock)
	// 句柄在 SetLk 事务内因租约超期被级联删除，但随后的 renewClientSession 无条件 UpsertClientSession 将会话自救复活，
	// 导致 SetLk 居然对已死句柄授锁成功（返回 nil），留下无法通过正常 Release 释放的幽灵锁，破坏 ADR-0013 租约 Fencing 安全边界。
	t.Run("RegularFile_ExpiredHandleSetLkSucceedsAndLeavesGhostLock", func(t *testing.T) {
		subCtx, subCancel := context.WithTimeout(ctx, 20*time.Second)
		defer subCancel()

		diskResp, err := client.CreateDisk(subCtx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "h5-reg-disk",
			MaxBytes: 50 * 1024 * 1024,
		}))
		require.NoError(t, err)
		disk := diskResp.Msg.GetDisk()
		verifySessionDiskInDB(subCtx, t, disk.GetDiskId(), disk.GetRootIno())

		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cCancel()
			if _, err := client.DeleteDisk(cCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()})); err != nil {
				if connect.CodeOf(err) != connect.CodeNotFound {
					t.Errorf("清理磁盘失败: %v", err)
				}
			}
		})

		sessionA := uint64(250001)
		blockerSessionID := uint64(249999)

		// 1. 先注册激活 sessionA，再创建常规文件并获得句柄 fhA
		_, err = client.RenewSession(subCtx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: sessionA,
		}))
		require.NoError(t, err)

		fileName := []byte("regular_h5.txt")
		createResp, err := client.Create(subCtx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      fileName,
			Mode:      0o644,
			Flags:     oRdWr,
			SessionId: &sessionA,
		}))
		require.NoError(t, err)
		fileIno := createResp.Msg.GetInode().GetIno()
		fhA := createResp.Msg.GetFh()

		// 2. 轮询数据库等待 sessionA 租约跨期失效
		waitExpireCtx, waitExpireCancel := context.WithTimeout(subCtx, 5*time.Second)
		defer waitExpireCancel()
		expired := false
		for {
			select {
			case <-waitExpireCtx.Done():
				t.Fatalf("等待 sessionA=%d 租约跨期失效超时: %v", sessionA, waitExpireCtx.Err())
			default:
			}
			status := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT (clock_timestamp() > expires_at)::text FROM client_sessions WHERE session_id = %d", sessionA))
			if status == "true" {
				expired = true
				break
			}
			_ = sleepWithContext(subCtx, 20*time.Millisecond)
		}
		require.True(t, expired, "sessionA 租约必须已跨期失效")
		t.Logf("【确证】sessionA 已跨期失效")

		// 3. 在 ADR-0025 语义下，SetLk 不再执行随路会话续期，携带已过期 sessionA / fhA 调用 SetLk 必须立即返回 EBADF 且绝不复活会话或写入幽灵锁
		_, setLkErr := client.SetLk(subCtx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       fileIno,
			Fh:        fhA,
			LockOwner: 1001,
			SessionId: sessionA,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  fWrLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		testutil.RequireConnectError(t, setLkErr, connect.CodeFailedPrecondition, "EBADF")

		stillExpired := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT (clock_timestamp() > expires_at)::text FROM client_sessions WHERE session_id = %d", sessionA))
		require.Equal(t, "true", stillExpired, "SetLk 绝不能随路复活已过期的 sessionA")

		// 4. 通过 sweeper 会话 (blockerSessionID) 调用 RenewSession 触发过期会话与关联句柄清理
		_, err = client.RenewSession(subCtx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: blockerSessionID,
		}))
		require.NoError(t, err)
		defer func() {
			_, _ = client.CloseSession(subCtx, connect.NewRequest(&storagev1.CloseSessionRequest{
				SessionId: blockerSessionID,
			}))
		}()

		// 5. 直接审计数据库并执行无条件断言
		handleCountStr := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT COUNT(*) FROM file_handles WHERE fh = %d AND ino = %d", fhA, fileIno))
		lockCountStr := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT COUNT(*) FROM file_locks WHERE ino = %d AND fh = %d AND session_id = %d", fileIno, fhA, sessionA))
		handleCount, convErr1 := strconv.Atoi(handleCountStr)
		require.NoError(t, convErr1, "解析 handleCount 失败: %s", handleCountStr)
		lockCount, convErr2 := strconv.Atoi(lockCountStr)
		require.NoError(t, convErr2, "解析 lockCount 失败: %s", lockCountStr)

		assert.Equal(t, 0, handleCount, "跨期后旧句柄 fh=%d 应已被级联清理", fhA)
		assert.Equal(t, 0, lockCount, "已过期会话的旧句柄 fh=%d (当前 file_handles 存活数=%d) 绝不能在 file_locks 表中写入幽灵锁", fhA, handleCount)
	})

	// 子场景 2: 孤儿文件场景 (OrphanFile_ExpiredHandleSetLkLeaksForeignKeyError)
	// 孤儿文件在句柄被删后遭物理回收，随后 SetLk 往已被删除的 Inode 插入锁，
	// 触发未映射的 PostgreSQL 23503 外键错误并泄露为 CodeInternal。
	t.Run("OrphanFile_ExpiredHandleSetLkLeaksForeignKeyError", func(t *testing.T) {
		subCtx, subCancel := context.WithTimeout(ctx, 20*time.Second)
		defer subCancel()

		diskResp, err := client.CreateDisk(subCtx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "h5-orphan-disk",
			MaxBytes: 50 * 1024 * 1024,
		}))
		require.NoError(t, err)
		disk := diskResp.Msg.GetDisk()
		verifySessionDiskInDB(subCtx, t, disk.GetDiskId(), disk.GetRootIno())

		t.Cleanup(func() {
			cCtx, cCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cCancel()
			if _, err := client.DeleteDisk(cCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()})); err != nil {
				if connect.CodeOf(err) != connect.CodeNotFound {
					t.Errorf("清理磁盘失败: %v", err)
				}
			}
		})

		sessionOrphan := uint64(260001)
		blockerSessionID := uint64(259999)

		// 1. 先注册激活 sessionOrphan，再创建文件并 Unlink 成为打开孤儿 (nlink = 0)
		_, err = client.RenewSession(subCtx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: sessionOrphan,
		}))
		require.NoError(t, err)

		orphanName := []byte("orphan_h5.tmp")
		createResp, err := client.Create(subCtx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      orphanName,
			Mode:      0o644,
			Flags:     oRdWr,
			SessionId: &sessionOrphan,
		}))
		require.NoError(t, err)
		orphanIno := createResp.Msg.GetInode().GetIno()
		fhOrphan := createResp.Msg.GetFh()

		_, err = client.Unlink(subCtx, connect.NewRequest(&storagev1.UnlinkRequest{
			ParentIno: disk.GetRootIno(),
			Name:      orphanName,
		}))
		require.NoError(t, err)

		// 2. 轮询确认 sessionOrphan 租约跨期失效
		waitExpireCtx, waitExpireCancel := context.WithTimeout(subCtx, 5*time.Second)
		defer waitExpireCancel()
		expired := false
		for {
			select {
			case <-waitExpireCtx.Done():
				t.Fatalf("等待 sessionOrphan=%d 租约跨期失效超时: %v", sessionOrphan, waitExpireCtx.Err())
			default:
			}
			status := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT (clock_timestamp() > expires_at)::text FROM client_sessions WHERE session_id = %d", sessionOrphan))
			if status == "true" {
				expired = true
				break
			}
			_ = sleepWithContext(subCtx, 20*time.Millisecond)
		}
		require.True(t, expired, "sessionOrphan 租约必须已跨期失效")

		// 3. 携带已过期 sessionOrphan / fhOrphan 对孤儿文件调用 SetLk：必须立即返回 EBADF 且绝不复活会话或插入任何锁
		_, setLkErr := client.SetLk(subCtx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       orphanIno,
			Fh:        fhOrphan,
			LockOwner: 1001,
			SessionId: sessionOrphan,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  fWrLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		testutil.RequireConnectError(t, setLkErr, connect.CodeFailedPrecondition, "EBADF")

		stillExpired := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT (clock_timestamp() > expires_at)::text FROM client_sessions WHERE session_id = %d", sessionOrphan))
		require.Equal(t, "true", stillExpired, "SetLk 绝不能随路复活已过期的 sessionOrphan")

		// 4. 通过 sweeper 会话 (blockerSessionID) 调用 RenewSession 触发过期会话、句柄及孤儿节点物理清理
		_, err = client.RenewSession(subCtx, connect.NewRequest(&storagev1.RenewSessionRequest{
			SessionId: blockerSessionID,
		}))
		require.NoError(t, err)
		defer func() {
			_, _ = client.CloseSession(subCtx, connect.NewRequest(&storagev1.CloseSessionRequest{
				SessionId: blockerSessionID,
			}))
		}()

		// 5. 查询 handleCount、orphanLockCount 与 orphanInodeCount，无条件断言全部为 0
		handleCountStr := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT COUNT(*) FROM file_handles WHERE fh = %d AND ino = %d", fhOrphan, orphanIno))
		orphanInodeCountStr := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT COUNT(*) FROM inodes WHERE ino = %d", orphanIno))
		orphanLockCountStr := execSessionPsql(subCtx, t, fmt.Sprintf("SELECT COUNT(*) FROM file_locks WHERE ino = %d", orphanIno))
		handleCount, convErr0 := strconv.Atoi(handleCountStr)
		require.NoError(t, convErr0, "解析 handleCount 失败: %s", handleCountStr)
		orphanLockCount, convErr1 := strconv.Atoi(orphanLockCountStr)
		require.NoError(t, convErr1, "解析 orphanLockCount 失败: %s", orphanLockCountStr)
		orphanInodeCount, convErr2 := strconv.Atoi(orphanInodeCountStr)
		require.NoError(t, convErr2, "解析 orphanInodeCount 失败: %s", orphanInodeCountStr)

		assert.Equal(t, 0, handleCount, "跨期清理后旧孤儿句柄 fh=%d 应已被级联清理", fhOrphan)
		assert.Equal(t, 0, orphanLockCount, "已物理删除的孤儿 Inode 不应存在任何锁残留")
		assert.Equal(t, 0, orphanInodeCount, "跨期清理后无引用孤儿 Inode=%d 应已被物理回收", orphanIno)
	})
}
