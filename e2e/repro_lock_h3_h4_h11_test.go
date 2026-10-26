//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
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

// execPsqlQuery 在 Docker 容器中执行 SQL 并返回单行或纯文本输出。
// 支持通过环境变量 BOWL_POSTGRES_CONTAINER（默认 "bowl-postgres"）与 BOWL_POSTGRES_DB（默认 "bowl"）配置。
func execPsqlQuery(ctx context.Context, sqlQuery string) (string, error) {
	container := os.Getenv("BOWL_POSTGRES_CONTAINER")
	if container == "" {
		container = "bowl-postgres"
	}
	dbName := os.Getenv("BOWL_POSTGRES_DB")
	if dbName == "" {
		dbName = "bowl"
	}
	cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container,
		"psql", "-U", "postgres", "-d", dbName, "-X", "-A", "-t", "-v", "ON_ERROR_STOP=1")
	cmd.Stdin = strings.NewReader(sqlQuery)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("执行 psql 失败: %w, stderr: %s, sql: %s", err, stderr.String(), sqlQuery)
	}
	return strings.TrimSpace(stdout.String()), nil
}

// verifyLockBackendDBConsistency 查询 SELECT root_ino FROM disks WHERE disk_id = '$diskID' 断言等于 wantRootIno，
// 确保 RPC 客户端与 SQL 审计连接指向同一个后端数据库实例。
func verifyLockBackendDBConsistency(t *testing.T, ctx context.Context, diskID string, wantRootIno uint64) {
	t.Helper()
	query := fmt.Sprintf("SELECT root_ino FROM disks WHERE disk_id = '%s';", diskID)
	out, err := execPsqlQuery(ctx, query)
	require.NoError(t, err, "核验后端数据库一致性 SQL 查询失败")
	gotRootIno, err := strconv.ParseUint(strings.TrimSpace(out), 10, 64)
	require.NoError(t, err, "解析 disks.root_ino 失败: %q", out)
	require.Equal(t, wantRootIno, gotRootIno, "RPC 创建的磁盘 root_ino 必须与 SQL 直接查询到的 root_ino 完全一致，以确保审计目标同一")
}

// waitForPendingWaiterInDB 通过直接轮询 PostgreSQL 确认等待者记录已真实提交落盘为 PENDING。
// 彻底替代不可靠的固定休眠 time.Sleep(150ms)。
func waitForPendingWaiterInDB(t *testing.T, ctx context.Context, ino, sessionID, requestID, fh uint64, waitErrCh <-chan error) {
	t.Helper()
	pollCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()

	query := fmt.Sprintf("SELECT COUNT(*) FROM file_lock_waiters WHERE ino = %d AND session_id = %d AND request_id = %d AND fh = %d AND status = 1;",
		ino, sessionID, requestID, fh)

	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case err, ok := <-waitErrCh:
			if ok && err != nil {
				t.Fatalf("后台 SetLkw 协程在等待者记录落盘前提前退出: %v", err)
			}
		case <-pollCtx.Done():
			t.Fatalf("等待 file_lock_waiters 记录提交为 PENDING 超时 (ino=%d, session=%d, req=%d, fh=%d): %v",
				ino, sessionID, requestID, fh, pollCtx.Err())
		case <-ticker.C:
			stepCtx, stepCancel := context.WithTimeout(pollCtx, 1*time.Second)
			out, err := execPsqlQuery(stepCtx, query)
			stepCancel()
			if err != nil {
				// 单次查询抖动允许重试，直至 pollCtx 超时
				continue
			}
			if out == "1" {
				return
			}
		}
	}
}

// startSessionKeepalive 为指定的一个或多个会话启动后台续期保活协程。
// 协程受 keepaliveCtx 控制，每 500ms 发起一次 RenewSession，并使用 WaitGroup 与 t.Cleanup 保证退出汇合与错误收集。
func startSessionKeepalive(t *testing.T, ctx context.Context, client storagev1connect.StorageServiceClient, sessionIDs ...uint64) {
	t.Helper()
	keepaliveCtx, keepaliveCancel := context.WithCancel(ctx)
	var keepaliveWg sync.WaitGroup
	errCh := make(chan error, len(sessionIDs)*32)

	// t.Cleanup 执行顺序为后进先出 (LIFO)，在 CreateDisk 之后注册的 Cleanup 将优先于 DeleteDisk 执行。
	t.Cleanup(func() {
		keepaliveCancel()
		keepaliveWg.Wait()
		close(errCh)
		for err := range errCh {
			if err != nil && !errors.Is(err, context.Canceled) && connect.CodeOf(err) != connect.CodeCanceled {
				t.Errorf("后台会话保活续租失败: %v", err)
			}
		}
	})

	for _, sid := range sessionIDs {
		sid := sid
		keepaliveWg.Add(1)
		go func() {
			defer keepaliveWg.Done()
			ticker := time.NewTicker(500 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-keepaliveCtx.Done():
					return
				case <-ticker.C:
					stepCtx, cancel := context.WithTimeout(keepaliveCtx, 2*time.Second)
					_, err := client.RenewSession(stepCtx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sid}))
					cancel()
					if err != nil {
						if keepaliveCtx.Err() != nil || errors.Is(err, context.Canceled) || connect.CodeOf(err) == connect.CodeCanceled {
							return
						}
						select {
						case errCh <- fmt.Errorf("会话 %d 续租失败: %w", sid, err):
						default:
						}
					}
				}
			}
		}()
	}
}

// TestE2E_H3_SessionlessFlushDeletesOtherSessionLocks 复现 H3：
// 不带会话 ID 的 Flush(SessionId: nil) 会跨客户端误删别人的 POSIX 文件锁。
func TestE2E_H3_SessionlessFlushDeletesOtherSessionLocks(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建独立测试虚拟磁盘并在测试结束时立即登记清理
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h3-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	verifyLockBackendDBConsistency(t, ctx, disk.GetDiskId(), disk.GetRootIno())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, delErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
		if delErr != nil && connect.CodeOf(delErr) != connect.CodeNotFound {
			t.Errorf("清理测试磁盘 %s 失败: %v", disk.GetDiskId(), delErr)
		}
	})

	// 注册两个独立活跃会话 sessionA (310001) 与 sessionC (310002)
	sessionA := uint64(310001)
	sessionC := uint64(310002)

	renewA, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sessionA}))
	require.NoError(t, err, "RenewSession sessionA 必须成功")
	require.NotNil(t, renewA.Msg.GetExpiresAt())

	renewC, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sessionC}))
	require.NoError(t, err, "RenewSession sessionC 必须成功")
	require.NotNil(t, renewC.Msg.GetExpiresAt())

	// 启动受控会话保活协程，确保在 DeleteDisk 之前汇合退出
	startSessionKeepalive(t, ctx, client, sessionA, sessionC)

	// 2. 客户端 A 在 sessionA 下调用 Create 创建文件 fileIno 并获得句柄 fhA
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("h3_locked_file.bin"),
		Mode:      0o666,
		Flags:     oRdWr,
		SessionId: &sessionA,
	}))
	require.NoError(t, err, "客户端 A Create 文件应成功")
	fileIno := createResp.Msg.GetInode().GetIno()
	fhA := createResp.Msg.GetFh()

	// 3. 客户端 A 调用 SetLk 获取区间 [0, 100] 的排他写锁 F_WRLCK
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fhA,
		LockOwner: 8888,
		SessionId: sessionA,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "客户端 A 加排他写锁应成功")

	// 4. 客户端 B 使用无会话模式 (SessionId: nil) 调用 Open 打开同一文件获得 fhB
	openBResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdOnly,
		SessionId: nil,
	}))
	require.NoError(t, err, "客户端 B 无会话 Open 应成功")
	fhB := openBResp.Msg.GetFh()

	// 客户端 B 调用无会话 Flush(Ino: fileIno, Fh: fhB, LockOwner: 8888, SessionId: nil)
	lockOwner8888 := uint64(8888)
	_, err = client.Flush(ctx, connect.NewRequest(&storagev1.FlushRequest{
		Ino:       fileIno,
		Fh:        fhB,
		LockOwner: &lockOwner8888,
		SessionId: nil,
	}))
	require.NoError(t, err, "客户端 B 无会话 Flush 应成功")

	// 5. 客户端 C 在 sessionC 下调用 Open 获得 fhC
	openCResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &sessionC,
	}))
	require.NoError(t, err, "客户端 C 打开文件应成功")
	fhC := openCResp.Msg.GetFh()

	// 6. 正确性断言（预期无 Bug 行为，当前因 H3 缺陷将触发报红）：
	// 客户端 C 调用 GetLk 探测 [0, 100] 区间时，必须仍然探测到客户端 A (Pid: 1001) 持有的 F_WRLCK 冲突写锁
	probeResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
		Ino:       fileIno,
		Fh:        fhC,
		LockOwner: 9999,
		SessionId: sessionC,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1002,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "客户端 C 探测锁状态应成功")
	assert.Equal(t, fWrLck, probeResp.Msg.GetLk().GetType(), "客户端 A 的锁应依然存在，GetLk 应探测到冲突写锁")
	assert.Equal(t, uint32(1001), probeResp.Msg.GetLk().GetPid(), "冲突锁持有人 PID 应为客户端 A 的 PID 1001")

	// 客户端 C 调用非阻塞 SetLk 申请排他写锁，必须因客户端 A 仍持有锁而被拒绝并返回 EAGAIN
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fhC,
		LockOwner: 9999,
		SessionId: sessionC,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1002,
		},
		LkFlags: 0,
	}))
	testutil.RequireConnectError(t, err, connect.CodeResourceExhausted, "EAGAIN")
}

// TestE2E_H4_SetLkwReleaseFhLeavesZombieWaiter 复现 H4：
// 阻塞等锁 SetLkw 期间关闭文件句柄，留下僵尸排队记录导致该文件永远无法再加锁。
func TestE2E_H4_SetLkwReleaseFhLeavesZombieWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	client := testutil.NewStorageClient()

	// 1. 创建独立测试虚拟磁盘并在测试结束时立即登记清理
	diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     "e2e-h4-disk",
		MaxBytes: 50 * 1024 * 1024,
	}))
	require.NoError(t, err, "CreateDisk 必须成功")
	disk := diskResp.Msg.GetDisk()
	verifyLockBackendDBConsistency(t, ctx, disk.GetDiskId(), disk.GetRootIno())

	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, delErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
		if delErr != nil && connect.CodeOf(delErr) != connect.CodeNotFound {
			t.Errorf("清理测试磁盘 %s 失败: %v", disk.GetDiskId(), delErr)
		}
	})

	// 注册三个活跃会话 session1 (320001), session2 (320002), session3 (320003)
	session1 := uint64(320001)
	session2 := uint64(320002)
	session3 := uint64(320003)

	for _, sid := range []uint64{session1, session2, session3} {
		renew, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sid}))
		require.NoError(t, err, "RenewSession 必须成功")
		require.NotNil(t, renew.Msg.GetExpiresAt())
	}
	startSessionKeepalive(t, ctx, client, session1, session2, session3)

	// 2. 会话 1 创建并打开文件 fileIno (fh1)，获取 [0, 100] 排他写锁
	createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
		ParentIno: disk.GetRootIno(),
		Name:      []byte("h4_zombie_waiter_file.bin"),
		Mode:      0o666,
		Flags:     oRdWr,
		SessionId: &session1,
	}))
	require.NoError(t, err, "会话 1 Create 文件应成功")
	fileIno := createResp.Msg.GetInode().GetIno()
	fh1 := createResp.Msg.GetFh()

	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "会话 1 加排他写锁应成功")

	// 3. 会话 2 打开 fileIno 得到 fh2
	open2Resp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &session2,
	}))
	require.NoError(t, err, "会话 2 Open 文件应成功")
	fh2 := open2Resp.Msg.GetFh()

	// 启动后台 goroutine 在 fh2 上调用阻塞 SetLkw，并注册取消与汇合清理屏障
	waiterCtx, waiterCancel := context.WithTimeout(ctx, 5*time.Second)
	waiterDone := make(chan struct{})
	waitErrCh := make(chan error, 1)
	go func() {
		defer close(waiterDone)
		defer waiterCancel()

		_, waitErr := client.SetLkw(waiterCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fh2,
			LockOwner: 202,
			SessionId: session2,
			RequestId: 70001,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   100,
				Type:  fWrLck,
				Pid:   2002,
			},
			LkFlags: 0,
		}))
		waitErrCh <- waitErr
	}()
	t.Cleanup(func() {
		waiterCancel()
		<-waiterDone
	})

	// 4. 真实数据库轮询：确认会话 2 的 SetLkw 已在 file_lock_waiters 中登记为 PENDING 并提交落盘
	waitForPendingWaiterInDB(t, ctx, fileIno, session2, 70001, fh2, waitErrCh)

	// 5. 会话 2 调用 Release 关闭正在等待锁的句柄 fh2
	_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
		Ino:       fileIno,
		Fh:        fh2,
		SessionId: &session2,
	}))
	require.NoError(t, err, "会话 2 Release fh2 应成功")

	// 6. 等待后台 SetLkw 协程返回，断言其因句柄已关闭而返回 EBADF
	var waitErr error
	select {
	case waitErr = <-waitErrCh:
	case <-time.After(5 * time.Second):
		t.Fatal("等待后台 SetLkw 退出超时")
	}
	testutil.RequireConnectError(t, waitErr, connect.CodeFailedPrecondition, "EBADF")

	// 审计 file_lock_waiters 表中是否存在残留僵尸等待记录
	auditCtx, auditCancel := context.WithTimeout(ctx, 3*time.Second)
	zombieCountStr, err := execPsqlQuery(auditCtx, fmt.Sprintf("SELECT COUNT(*) FROM file_lock_waiters WHERE ino = %d AND session_id = %d AND request_id = %d;",
		fileIno, session2, 70001))
	auditCancel()
	require.NoError(t, err, "直接表审计 file_lock_waiters 失败")
	zombieWaiterCount, err := strconv.Atoi(zombieCountStr)
	require.NoError(t, err, "解析 zombieWaiterCount 失败: %q", zombieCountStr)

	// 7. 会话 1 调用 SetLk(F_UNLCK) 释放 [0, 100] 上的写锁
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh1,
		LockOwner: 101,
		SessionId: session1,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fUnLck,
			Pid:   1001,
		},
		LkFlags: 0,
	}))
	require.NoError(t, err, "会话 1 释放写锁应成功")

	// 8. 此时 fileIno 上已没有任何活跃锁，也没有任何存活的等待协程！会话 3 打开 fileIno 得到 fh3，调用非阻塞 SetLk 申请 [0, 100] 排他写锁
	open3Resp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
		Ino:       fileIno,
		Flags:     oRdWr,
		SessionId: &session3,
	}))
	require.NoError(t, err, "会话 3 Open 文件应成功")
	fh3 := open3Resp.Msg.GetFh()

	// 9. 正确性断言（预期无 Bug 行为，当前因 H4 缺陷残留僵尸等待者将触发报红）：
	// 原持锁者已解锁且原等待句柄已关闭，新加锁请求不应被僵尸等待记录拦截，必须立即获锁成功
	_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
		Ino:       fileIno,
		Fh:        fh3,
		LockOwner: 303,
		SessionId: session3,
		Lk: &storagev1.FileLock{
			Start: 0,
			End:   100,
			Type:  fWrLck,
			Pid:   3003,
		},
		LkFlags: 0,
	}))

	assert.Equal(t, 0, zombieWaiterCount, "句柄 Release 且 SetLkw 返回 EBADF 退出后，file_lock_waiters 表中绝不应残留 PENDING 僵尸记录")
	require.NoError(t, err, "原持锁者已解锁且原等待句柄已关闭，新加锁请求不应被僵尸等待记录拦截")
}

// TestE2E_H11_BlockingSetLkwTimeoutAndWakeupLiveness 复现 H11：
// 阻塞文件锁 SetLkw 显式 context.WithTimeout 超时保护与异常挂起拦截。
// 分为两个子测试：真实 Docker 环境验证挂起拦截与单元测试套件 AST 静态审计。
func TestE2E_H11_BlockingSetLkwTimeoutAndWakeupLiveness(t *testing.T) {
	t.Run("LiveDocker_SetLkwExplicitTimeoutCatchesUnboundedHang", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		client := testutil.NewStorageClient()

		// 1. 创建独立测试虚拟磁盘并在测试结束时立即登记清理
		diskResp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
			Name:     "e2e-h11-disk",
			MaxBytes: 50 * 1024 * 1024,
		}))
		require.NoError(t, err, "CreateDisk 必须成功")
		disk := diskResp.Msg.GetDisk()
		verifyLockBackendDBConsistency(t, ctx, disk.GetDiskId(), disk.GetRootIno())

		t.Cleanup(func() {
			cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cleanupCancel()
			_, delErr := client.DeleteDisk(cleanupCtx, connect.NewRequest(&storagev1.DeleteDiskRequest{DiskId: disk.GetDiskId()}))
			if delErr != nil && connect.CodeOf(delErr) != connect.CodeNotFound {
				t.Errorf("清理测试磁盘 %s 失败: %v", disk.GetDiskId(), delErr)
			}
		})

		// 注册三个活跃会话 holderSession (330001), abortedWaiterSession (330002), callerSession (330003)
		holderSession := uint64(330001)
		abortedWaiterSession := uint64(330002)
		callerSession := uint64(330003)

		for _, sid := range []uint64{holderSession, abortedWaiterSession, callerSession} {
			renew, err := client.RenewSession(ctx, connect.NewRequest(&storagev1.RenewSessionRequest{SessionId: sid}))
			require.NoError(t, err, "RenewSession 必须成功")
			require.NotNil(t, renew.Msg.GetExpiresAt())
		}
		startSessionKeepalive(t, ctx, client, holderSession, abortedWaiterSession, callerSession)

		// 2. holderSession 创建文件并在 [0, 4096] 上持有写锁
		createResp, err := client.Create(ctx, connect.NewRequest(&storagev1.CreateRequest{
			ParentIno: disk.GetRootIno(),
			Name:      []byte("h11_liveness_file.bin"),
			Mode:      0o666,
			Flags:     oRdWr,
			SessionId: &holderSession,
		}))
		require.NoError(t, err, "holderSession Create 文件应成功")
		fileIno := createResp.Msg.GetInode().GetIno()
		fhHolder := createResp.Msg.GetFh()

		_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       fileIno,
			Fh:        fhHolder,
			LockOwner: 101,
			SessionId: holderSession,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fWrLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		require.NoError(t, err, "holderSession 加锁应成功")

		// 3. abortedWaiterSession 打开文件并在 [0, 4096] 上发起带超时的后台 SetLkw
		openWaiterResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:       fileIno,
			Flags:     oRdWr,
			SessionId: &abortedWaiterSession,
		}))
		require.NoError(t, err, "abortedWaiterSession Open 文件应成功")
		fhWaiter := openWaiterResp.Msg.GetFh()

		waiterCtx, waiterCancel := context.WithTimeout(ctx, 5*time.Second)
		waiterDone := make(chan struct{})
		waiterErrCh := make(chan error, 1)
		go func() {
			defer close(waiterDone)
			defer waiterCancel()

			_, waiterErr := client.SetLkw(waiterCtx, connect.NewRequest(&storagev1.SetLkwRequest{
				Ino:       fileIno,
				Fh:        fhWaiter,
				LockOwner: 202,
				SessionId: abortedWaiterSession,
				RequestId: 70002,
				Lk: &storagev1.FileLock{
					Start: 0,
					End:   4096,
					Type:  fWrLck,
					Pid:   2002,
				},
				LkFlags: 0,
			}))
			waiterErrCh <- waiterErr
		}()
		t.Cleanup(func() {
			waiterCancel()
			<-waiterDone
		})

		// 轮询数据库确认 abortedWaiterSession 的 SetLkw 成功入队为 PENDING
		waitForPendingWaiterInDB(t, ctx, fileIno, abortedWaiterSession, 70002, fhWaiter, waiterErrCh)

		// abortedWaiterSession 关闭 fhWaiter
		_, err = client.Release(ctx, connect.NewRequest(&storagev1.ReleaseRequest{
			Ino:       fileIno,
			Fh:        fhWaiter,
			SessionId: &abortedWaiterSession,
		}))
		require.NoError(t, err, "Release fhWaiter 应成功")

		// 等待后台协程退出并断言 EBADF
		var waiterErr error
		select {
		case waiterErr = <-waiterErrCh:
		case <-time.After(5 * time.Second):
			t.Fatal("等待后台 SetLkw 退出超时")
		}
		testutil.RequireConnectError(t, waiterErr, connect.CodeFailedPrecondition, "EBADF")

		// 4. holderSession 释放 [0, 4096] 写锁
		_, err = client.SetLk(ctx, connect.NewRequest(&storagev1.SetLkRequest{
			Ino:       fileIno,
			Fh:        fhHolder,
			LockOwner: 101,
			SessionId: holderSession,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fUnLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		require.NoError(t, err, "holderSession 释放写锁应成功")

		// 调用 GetLk 确认当前文件 [0, 4096] 区间已无任何活跃冲突锁
		getLkResp, err := client.GetLk(ctx, connect.NewRequest(&storagev1.GetLkRequest{
			Ino:       fileIno,
			Fh:        fhHolder,
			LockOwner: 101,
			SessionId: holderSession,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fWrLck,
				Pid:   1001,
			},
			LkFlags: 0,
		}))
		require.NoError(t, err, "GetLk 探测应成功")
		assert.Equal(t, fUnLck, getLkResp.Msg.GetLk().GetType(), "文件当前应无任何活跃锁 (F_UNLCK)")

		// 5. callerSession 打开文件
		openCallerResp, err := client.Open(ctx, connect.NewRequest(&storagev1.OpenRequest{
			Ino:       fileIno,
			Flags:     oRdWr,
			SessionId: &callerSession,
		}))
		require.NoError(t, err, "callerSession Open 文件应成功")
		fhCaller := openCallerResp.Msg.GetFh()

		// 此时文件已无任何活跃冲突锁！callerSession 遵循 H11 规范，绑定显式 context.WithTimeout 调用预期立即获锁返回的 SetLkw
		stepCtx, stepCancel := context.WithTimeout(ctx, 1200*time.Millisecond)
		defer stepCancel()

		_, err = client.SetLkw(stepCtx, connect.NewRequest(&storagev1.SetLkwRequest{
			Ino:       fileIno,
			Fh:        fhCaller,
			LockOwner: 303,
			SessionId: callerSession,
			RequestId: 80001,
			Lk: &storagev1.FileLock{
				Start: 0,
				End:   4096,
				Type:  fWrLck,
				Pid:   3003,
			},
			LkFlags: 0,
		}))

		// 6. 正确性断言（预期无 Bug 行为，当前因 H11/H4 异常挂起缺陷将触发超时报红）：
		// 既然 GetLk 已确认文件无任何活跃冲突锁，预期不阻塞的 SetLkw 在显式 Timeout 保护下必须在几十毫秒内立即成功获锁返回，绝不能无限挂起至超时
		require.NoError(t, err, "预期不阻塞的 SetLkw 在显式 Timeout 保护下必须立即成功，绝不能无限挂起至超时")
	})

	t.Run("UnitTestSuiteAudit_BlockingSetLkwMissingContextTimeout", func(t *testing.T) {
		fset := token.NewFileSet()

		// 自检反例 1: 合规函数（WithTimeout + select { case <-waiterRegistered: case <-ctx.Done(): }）
		const testCodeCompliant = `package sample
import (
	"context"
	"time"
)
func testCompliant() {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waiterRegistered := make(chan struct{})
	select {
	case <-waiterRegistered:
	case <-ctx.Done():
	}
	_ = mgr.SetLkw(ctx, nil)
}
`
		node1, err := parser.ParseFile(fset, "sample_compliant.go", testCodeCompliant, 0)
		require.NoError(t, err)
		violations1 := auditASTFileForH11(fset, "sample_compliant.go", node1)
		require.Empty(t, violations1, "合规函数不应报出任何 AST 违规，但报出: %v", violations1)

		// 自检反例 2: 作用域遮蔽违规（外层有 WithTimeout，后一个子测试遮蔽为 context.Background()）
		const testCodeShadowed = `package sample
import (
	"context"
	"testing"
	"time"
)
func testShadowed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	t.Run("sub1", func(t *testing.T) {
		_ = mgr.SetLkw(ctx, nil)
	})
	t.Run("sub2", func(t *testing.T) {
		ctx := context.Background()
		_ = mgr.SetLkw(ctx, nil)
	})
}
`
		node2, err := parser.ParseFile(fset, "sample_shadowed.go", testCodeShadowed, 0)
		require.NoError(t, err)
		violations2 := auditASTFileForH11(fset, "sample_shadowed.go", node2)
		require.Len(t, violations2, 1, "遮蔽违规测试必须准确检出 1 条违规，实际: %v", violations2)

		// 自检反例 3: 继承有界上下文（WithCancel 继承自 WithTimeout）
		const testCodeInherited = `package sample
import (
	"context"
	"time"
)
func testInherited() {
	parentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	childCtx, childCancel := context.WithCancel(parentCtx)
	defer childCancel()
	_ = mgr.SetLkw(childCtx, nil)
}
`
		node3, err := parser.ParseFile(fset, "sample_inherited.go", testCodeInherited, 0)
		require.NoError(t, err)
		violations3 := auditASTFileForH11(fset, "sample_inherited.go", node3)
		require.Empty(t, violations3, "继承有界上下文的调用不应报出任何违规，但报出: %v", violations3)

		// 直接针对原始报告 H11 会话 5-S1 指出的 disk_test.go 与 setlkw_test.go：
		// 使用 go/parser 与 go/ast 审计是否存在无截止时间的 context 与裸通道等待
		violations := auditTestSuiteSetLkwTimeouts(t)
		assert.Empty(t, violations, "单元测试套件必须符合 CODING_STANDARDS.md §2 规范，阻塞操作与通道等待必须绑定 context.WithTimeout 超时保护：\n%s", strings.Join(violations, "\n"))
	})
}

// findRepoRoot 查找包含 go.mod 的仓库根目录绝对路径。
func findRepoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	require.NoError(t, err)
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("未找到仓库根目录 (未找到包含 go.mod 的祖先目录)")
		}
		dir = parent
	}
}

// auditTestSuiteSetLkwTimeouts 使用 go/parser 和 go/ast 静态审计 disk_test.go 与 setlkw_test.go，
// 核验 CODING_STANDARDS.md §2 与原始报告 H11 指出的无界阻塞 SetLkw 及裸通道等待问题。
func auditTestSuiteSetLkwTimeouts(t *testing.T) []string {
	t.Helper()
	repoRoot := findRepoRoot(t)
	diskTestPath := filepath.Join(repoRoot, "services/storage/internal/meta/disk_test.go")
	setlkwTestPath := filepath.Join(repoRoot, "services/storage/internal/meta/setlkw_test.go")

	fset := token.NewFileSet()
	var violations []string

	// 1. 审计 disk_test.go
	diskNode, err := parser.ParseFile(fset, diskTestPath, nil, parser.ParseComments)
	require.NoError(t, err, "解析 disk_test.go 失败")
	violations = append(violations, auditASTFileForH11(fset, diskTestPath, diskNode)...)

	// 2. 审计 setlkw_test.go
	setlkwNode, err := parser.ParseFile(fset, setlkwTestPath, nil, parser.ParseComments)
	require.NoError(t, err, "解析 setlkw_test.go 失败")
	violations = append(violations, auditASTFileForH11(fset, setlkwTestPath, setlkwNode)...)

	return violations
}

// auditASTFileForH11 是对单个 AST 文件进行 H11 规范审计的纯函数：
// 1. 识别裸通道接收 <-waiterRegistered（必须置于包含超时或 ctx.Done() 分支的 select 中）；
// 2. 按词法作用域与上下文继承链精确判定传递给 SetLkw 的 context 是否有界。
func auditASTFileForH11(fset *token.FileSet, filePath string, fileNode *ast.File) []string {
	var violations []string
	var stack []ast.Node

	ast.Inspect(fileNode, func(n ast.Node) bool {
		if n == nil {
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			return true
		}
		stack = append(stack, n)

		switch stmt := n.(type) {
		case *ast.UnaryExpr:
			if stmt.Op == token.ARROW {
				if ident, ok := stmt.X.(*ast.Ident); ok && ident.Name == "waiterRegistered" {
					var ancestorSelect *ast.SelectStmt
					for i := len(stack) - 2; i >= 0; i-- {
						if sel, ok := stack[i].(*ast.SelectStmt); ok {
							ancestorSelect = sel
							break
						}
					}

					pos := fset.Position(stmt.Pos())
					if ancestorSelect == nil {
						violations = append(violations, fmt.Sprintf("%s:%d: 存在未包裹在带超时或 ctx.Done() 分支 select 中的裸通道接收 <-waiterRegistered",
							filepath.ToSlash(filepath.Clean(filePath)), pos.Line))
					} else if !hasTimeoutOrCancellationBranch(ancestorSelect) {
						violations = append(violations, fmt.Sprintf("%s:%d: select 中接收 <-waiterRegistered 但未包含任何超时 (time.After/timer.C) 或 ctx.Done() 取消分支",
							filepath.ToSlash(filepath.Clean(filePath)), pos.Line))
					}
				}
			}

		case *ast.CallExpr:
			if sel, ok := stmt.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "SetLkw" {
				if len(stmt.Args) > 0 {
					arg0 := stmt.Args[0]
					pos := fset.Position(stmt.Pos())
					if isCallTo(arg0, "context", "Background") || isCallTo(arg0, "context", "TODO") {
						violations = append(violations, fmt.Sprintf("%s:%d: 调用 SetLkw 直接传入无超时的 context",
							filepath.ToSlash(filepath.Clean(filePath)), pos.Line))
					} else if ident, ok := arg0.(*ast.Ident); ok {
						if !isVarBoundedAtPos(stack, ident.Name, stmt.Pos()) {
							violations = append(violations, fmt.Sprintf("%s:%d: 调用 SetLkw 使用的 context 变量 %q 未绑定 WithTimeout/WithDeadline",
								filepath.ToSlash(filepath.Clean(filePath)), pos.Line, ident.Name))
						}
					}
				}
			}
		}

		return true
	})

	return violations
}

// hasTimeoutOrCancellationBranch 检查 select 语句中是否至少包含一个超时或取消分支。
func hasTimeoutOrCancellationBranch(selectStmt *ast.SelectStmt) bool {
	if selectStmt.Body == nil {
		return false
	}
	for _, clause := range selectStmt.Body.List {
		comm, ok := clause.(*ast.CommClause)
		if !ok || comm.Comm == nil {
			continue
		}
		var recvExpr ast.Expr
		switch s := comm.Comm.(type) {
		case *ast.ExprStmt:
			recvExpr = s.X
		case *ast.AssignStmt:
			if len(s.Rhs) > 0 {
				recvExpr = s.Rhs[0]
			}
		}
		if recvExpr == nil {
			continue
		}
		unary, ok := recvExpr.(*ast.UnaryExpr)
		if !ok || unary.Op != token.ARROW {
			continue
		}
		ch := unary.X
		if call, ok := ch.(*ast.CallExpr); ok {
			if sel, ok := call.Fun.(*ast.SelectorExpr); ok && sel.Sel.Name == "Done" {
				return true
			}
			if isCallToExpr(call, "time", "After") {
				return true
			}
		}
		if sel, ok := ch.(*ast.SelectorExpr); ok && sel.Sel.Name == "C" {
			return true
		}
	}
	return false
}

// isVarBoundedAtPos 按词法作用域从内向外检查 varName 在 usePos 前的绑定是否具备超时/截止时间保证。
func isVarBoundedAtPos(stack []ast.Node, varName string, usePos token.Pos) bool {
	for i := len(stack) - 1; i >= 0; i-- {
		block, ok := stack[i].(*ast.BlockStmt)
		if !ok {
			continue
		}

		var lastAssign *ast.AssignStmt
		for _, stmt := range block.List {
			if stmt.Pos() >= usePos {
				continue
			}
			assign, ok := stmt.(*ast.AssignStmt)
			if !ok {
				continue
			}
			for _, lhs := range assign.Lhs {
				if ident, ok := lhs.(*ast.Ident); ok && ident.Name == varName {
					lastAssign = assign
					break
				}
			}
		}

		if lastAssign != nil {
			return isAssignmentBounded(stack[:i+1], lastAssign, varName)
		}
	}
	return false
}

// isAssignmentBounded 递归判定变量赋值右侧是否为有界 context 或继承自另一个有界 context。
func isAssignmentBounded(stack []ast.Node, assign *ast.AssignStmt, varName string) bool {
	varIndex := -1
	for idx, lhs := range assign.Lhs {
		if ident, ok := lhs.(*ast.Ident); ok && ident.Name == varName {
			varIndex = idx
			break
		}
	}
	if varIndex == -1 {
		return false
	}

	var rhsExpr ast.Expr
	if len(assign.Rhs) == 1 {
		rhsExpr = assign.Rhs[0]
	} else if varIndex < len(assign.Rhs) {
		rhsExpr = assign.Rhs[varIndex]
	} else {
		return false
	}

	call, ok := rhsExpr.(*ast.CallExpr)
	if !ok {
		if ident, ok := rhsExpr.(*ast.Ident); ok {
			return isVarBoundedAtPos(stack, ident.Name, assign.Pos())
		}
		return false
	}

	if isCallToExpr(call, "context", "WithTimeout") || isCallToExpr(call, "context", "WithDeadline") {
		return true
	}

	if isCallToExpr(call, "context", "Background") || isCallToExpr(call, "context", "TODO") {
		return false
	}

	if isCallToExpr(call, "context", "WithCancel") || isCallToExpr(call, "context", "WithValue") {
		if len(call.Args) > 0 {
			parentArg := call.Args[0]
			if parentIdent, ok := parentArg.(*ast.Ident); ok {
				return isVarBoundedAtPos(stack, parentIdent.Name, assign.Pos())
			}
			if parentCall, ok := parentArg.(*ast.CallExpr); ok {
				if isCallToExpr(parentCall, "context", "WithTimeout") || isCallToExpr(parentCall, "context", "WithDeadline") {
					return true
				}
				if isCallToExpr(parentCall, "context", "Background") || isCallToExpr(parentCall, "context", "TODO") {
					return false
				}
			}
		}
	}

	return false
}

func isCallTo(expr ast.Expr, pkg, fn string) bool {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return false
	}
	return isCallToExpr(call, pkg, fn)
}

func isCallToExpr(call *ast.CallExpr, pkg, fn string) bool {
	if call == nil {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return pkgIdent.Name == pkg && sel.Sel.Name == fn
}
