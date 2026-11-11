//go:build e2e && linux

package e2e_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

const (
	virtualStatsExpectedIno  = (uint64(1) << 63) - 1
	virtualStatsExpectedSize = 65536
)

var (
	statsTwoColumnPattern = regexp.MustCompile(`^(?:[a-zA-Z0-9_]+ -?\d+(?:\.\d+)?\n)+$`)
	jfsOpsTotalLineRegex  = regexp.MustCompile(`(?m)^juicefs_fuse_ops_durations_histogram_seconds_total (\d+)$`)
)

func runBowlSuccess(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, getBowlBin(), args...).CombinedOutput()
	require.NoError(t, err, "执行 bowl %v 失败: %s", args, string(out))
	return string(out)
}

func runBowlExpectExit(t *testing.T, wantCode int, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperProcessTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, getBowlBin(), args...).CombinedOutput()
	require.Error(t, err, "预期 bowl %v 返回非零退出码 %d，实际成功: %s", args, wantCode, string(out))
	var exitErr *exec.ExitError
	require.ErrorAs(t, err, &exitErr)
	require.Equal(t, wantCode, exitErr.ExitCode(), "输出: %s", string(out))
	return string(out)
}

func httpGetStatusAndBody(t *testing.T, targetURL string) (statusCode int, bodyText string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, targetURL, http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(body)
}

func mountMetaPath(t *testing.T, mountpoint string) string {
	t.Helper()
	absMP, err := filepath.Abs(mountpoint)
	require.NoError(t, err)
	cleaned := filepath.Clean(absMP)
	sum := sha256.Sum256([]byte(cleaned))
	return "/run/bowl/mounts/" + hex.EncodeToString(sum[:16]) + ".json"
}

func parseStatsOpsTotal(t *testing.T, content string) uint64 {
	t.Helper()
	matches := jfsOpsTotalLineRegex.FindStringSubmatch(content)
	require.Len(t, matches, 2, "未找到 juicefs_fuse_ops_durations_histogram_seconds_total 统计行")
	val, err := strconv.ParseUint(matches[1], 10, 64)
	require.NoError(t, err)
	return val
}

// TestE2E_CLI_ValidationFailures 验证 CLI 参数校验、已裁撤命令标志拦截、可观测性非法标志与不存在磁盘退出码契约。
func TestE2E_CLI_ValidationFailures(t *testing.T) {
	mountpoint := t.TempDir()
	endpoint := getStorageEndpoint()
	validDiskID := uuid.NewString()

	outInvalidUUID := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, "invalid-uuid-format", mountpoint)
	require.Contains(t, outInvalidUUID, "UUID")

	outNilUUID := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, uuid.Nil.String(), mountpoint)
	require.Contains(t, outNilUUID, "UUID")

	outMissingDisk := runBowlExpectExit(t, 1, "mount", "-f", "--endpoint", endpoint, validDiskID, mountpoint)
	require.Contains(t, outMissingDisk, "不存在")

	outRemovedCmd := runBowlExpectExit(t, 2, "branch")
	require.Contains(t, outRemovedCmd, "未知命令")

	outRemovedFlag := runBowlExpectExit(t, 2, "mount", "--branch", "main", validDiskID, mountpoint)
	require.Contains(t, outRemovedFlag, "未知标志")

	outBadAdmin := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, "--admin-addr", "not-a-host-port", validDiskID, mountpoint)
	require.Contains(t, outBadAdmin, "--admin-addr")

	outBadLevel := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, "--log-level", "verbose", validDiskID, mountpoint)
	require.Contains(t, outBadLevel, "--log-level")

	outBadFormat := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, "--log-format", "yaml", validDiskID, mountpoint)
	require.Contains(t, outBadFormat, "--log-format")

	outBadRatio := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, "--trace-sample-ratio", "1.5", validDiskID, mountpoint)
	require.Contains(t, outBadRatio, "--trace-sample-ratio")

	outBadStatsFlag := runBowlExpectExit(t, 2, "mount", "-f", "--endpoint", endpoint, "--stats-file=maybe", validDiskID, mountpoint)
	require.Contains(t, outBadStatsFlag, "--stats-file")

	outStatsMutEx := runBowlExpectExit(t, 2, "stats", "--json", "--raw")
	require.Contains(t, outStatsMutEx, "不能同时指定")

	outStatsBadAddr := runBowlExpectExit(t, 2, "stats", "--admin-addr", "bad-addr")
	require.Contains(t, outStatsBadAddr, "--admin-addr")

	outStatsExtraPos := runBowlExpectExit(t, 2, "stats", mountpoint, mountpoint)
	require.Contains(t, outStatsExtraPos, "位置参数")

	recordArtifact(t, map[string]string{
		"invalid_uuid_exit":     "2",
		"nil_uuid_exit":         "2",
		"nonexistent_disk_exit": "1",
		"removed_cmd_exit":      "2",
		"removed_flag_exit":     "2",
		"bad_admin_addr_exit":   "2",
		"bad_log_level_exit":    "2",
		"bad_log_format_exit":   "2",
		"bad_sample_ratio_exit": "2",
		"bad_stats_file_exit":   "2",
		"stats_mutex_exit":      "2",
		"stats_bad_addr_exit":   "2",
		"stats_extra_pos_exit":  "2",
	})
}

// TestE2E_CLI_AdminAndStats 验证挂载进程本地管理面 HTTP 端点、发现元数据生命周期与 bowl stats 自动发现及多挂载路由。
func TestE2E_CLI_AdminAndStats(t *testing.T) {
	client := newTestStorageClient(t)
	endpoint := getStorageEndpoint()

	outZeroMounts := runBowlExpectExit(t, 1, "stats")
	require.Contains(t, outZeroMounts, "未发现活跃的 bowl 挂载")

	disk1 := createTestDisk(t, client)
	mp1 := t.TempDir()
	runBowlSuccess(t, "mount", "--endpoint", endpoint, "--admin-addr", "127.0.0.1:0", disk1.GetDiskId(), mp1)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mp1).Run() })
	pid1 := findBowlDaemonPID(t, mp1)

	probePath := filepath.Join(mp1, "admin-probe.txt")
	require.NoError(t, os.WriteFile(probePath, []byte("stats-probe"), 0o644))
	probeRead, err := os.ReadFile(probePath)
	require.NoError(t, err)
	require.Equal(t, "stats-probe", string(probeRead))

	autoTextOut := runBowlSuccess(t, "stats")
	require.Contains(t, autoTextOut, "Uptime:")
	require.Contains(t, autoTextOut, "FUSE:")
	require.Contains(t, autoTextOut, "NamespaceCache:")

	jsonOut := runBowlSuccess(t, "stats", mp1, "--json")
	var snap struct {
		UptimeSeconds float64 `json:"uptime_seconds"`
		FUSE          struct {
			TotalCalls uint64            `json:"total_calls"`
			Ops        []json.RawMessage `json:"ops"`
		} `json:"fuse"`
	}
	require.NoError(t, json.Unmarshal([]byte(jsonOut), &snap))
	require.Positive(t, snap.FUSE.TotalCalls)
	require.Len(t, snap.FUSE.Ops, 36)

	rawOut := runBowlSuccess(t, "stats", mp1, "--raw")
	require.Regexp(t, statsTwoColumnPattern, rawOut)
	require.Contains(t, rawOut, "juicefs_uptime ")
	require.Contains(t, rawOut, "bowl_client_fuse_ops_total ")

	metaFile1 := mountMetaPath(t, mp1)
	metaBytes, err := os.ReadFile(metaFile1)
	require.NoError(t, err, "挂载发现元数据文件必须存在")
	var meta struct {
		AdminAddr string `json:"admin_addr"`
		PID       int    `json:"pid"`
		DiskID    string `json:"disk_id"`
	}
	require.NoError(t, json.Unmarshal(metaBytes, &meta))
	require.Equal(t, pid1, meta.PID)
	require.Equal(t, disk1.GetDiskId(), meta.DiskID)
	require.NotEmpty(t, meta.AdminAddr)
	require.NotEqual(t, "off", meta.AdminAddr)

	directAddrOut := runBowlSuccess(t, "stats", "--admin-addr", meta.AdminAddr)
	require.Contains(t, directAddrOut, "FUSE:")

	healthCode, healthBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/healthz")
	require.Equal(t, http.StatusOK, healthCode)
	require.Contains(t, healthBody, "ok")

	readyCode, readyBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/readyz")
	require.Equal(t, http.StatusOK, readyCode)
	require.Contains(t, readyBody, "ok")

	metricsCode, metricsBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/metrics")
	require.Equal(t, http.StatusOK, metricsCode)
	require.Contains(t, metricsBody, "bowl_client_fuse_ops_total")
	require.Contains(t, metricsBody, "rpc_client_")

	statsDefaultCode, statsDefaultBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/debug/stats")
	require.Equal(t, http.StatusOK, statsDefaultCode)
	require.True(t, json.Valid([]byte(statsDefaultBody)))

	statsTextCode, statsTextBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/debug/stats?format=text")
	require.Equal(t, http.StatusOK, statsTextCode)
	require.Contains(t, statsTextBody, "Uptime:")

	statsKvCode, statsKvBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/debug/stats?format=stats")
	require.Equal(t, http.StatusOK, statsKvCode)
	require.Regexp(t, statsTwoColumnPattern, statsKvBody)

	statsBadCode, statsBadBody := httpGetStatusAndBody(t, "http://"+meta.AdminAddr+"/debug/stats?format=xml")
	require.Equal(t, http.StatusBadRequest, statsBadCode)
	require.Contains(t, statsBadBody, "unsupported format")

	disk2 := createTestDisk(t, client)
	mp2 := t.TempDir()
	runBowlSuccess(t, "mount", "--endpoint", endpoint, "--admin-addr", "off", disk2.GetDiskId(), mp2)
	t.Cleanup(func() { _ = exec.Command(getBowlBin(), "umount", "-f", mp2).Run() })
	pid2 := findBowlDaemonPID(t, mp2)

	outMultiAmbiguous := runBowlExpectExit(t, 1, "stats")
	require.Contains(t, outMultiAmbiguous, "发现多个活跃挂载")

	explicitMP1Out := runBowlSuccess(t, "stats", mp1)
	require.Contains(t, explicitMP1Out, "FUSE:")

	outAdminOff := runBowlExpectExit(t, 1, "stats", mp2)
	require.Contains(t, outAdminOff, "未开启管理面")

	runBowlSuccess(t, "umount", mp2)
	waitForUnmount(t, mp2)
	waitForProcessExit(t, pid2)

	runBowlSuccess(t, "umount", mp1)
	waitForUnmount(t, mp1)
	waitForProcessExit(t, pid1)

	_, err = os.Stat(metaFile1)
	require.ErrorIs(t, err, os.ErrNotExist, "卸载后挂载发现元数据必须被清理")

	conn, dialErr := net.DialTimeout("tcp", meta.AdminAddr, time.Second)
	require.Error(t, dialErr, "卸载后管理面监听端口必须已关闭")
	_ = conn

	outAfterUnmount := runBowlExpectExit(t, 1, "stats", mp1)
	require.Contains(t, outAfterUnmount, "未找到挂载点")

	recordArtifact(t, map[string]string{
		"zero_mounts_exit":       "1",
		"single_auto_discover":   "ok",
		"json_ops_count":         strconv.Itoa(len(snap.FUSE.Ops)),
		"raw_two_column_valid":   "true",
		"admin_healthz_status":   strconv.Itoa(healthCode),
		"admin_readyz_status":    strconv.Itoa(readyCode),
		"admin_metrics_status":   strconv.Itoa(metricsCode),
		"admin_stats_bad_status": strconv.Itoa(statsBadCode),
		"multi_mounts_exit":      "1",
		"admin_off_exit":         "1",
		"meta_cleaned_on_umount": "true",
		"port_closed_on_umount":  "true",
	})
}

// TestE2E_VirtualStatsFileAndSendfile 验证挂载根目录隐藏虚拟 .stats/.jfs.stats 行为与内核 sendfile 零拷贝兼容。
func TestE2E_VirtualStatsFileAndSendfile(t *testing.T) {
	client := newTestStorageClient(t)
	disk := createTestDisk(t, client)
	endpoint := getStorageEndpoint()

	mpDisabled, stopDisabled := mountWithCleanup(t, endpoint, disk.GetDiskId())
	_, err := os.Stat(filepath.Join(mpDisabled, ".stats"))
	require.ErrorIs(t, err, os.ErrNotExist, "默认未开启 --stats-file 时 .stats 必须返回 ENOENT")
	_, err = os.Stat(filepath.Join(mpDisabled, ".jfs.stats"))
	require.ErrorIs(t, err, os.ErrNotExist, "默认未开启 --stats-file 时 .jfs.stats 必须返回 ENOENT")
	stopDisabled()

	mp, _ := mountWithCleanup(t, endpoint, disk.GetDiskId(), "--stats-file")

	statsPath := filepath.Join(mp, ".stats")
	jfsStatsPath := filepath.Join(mp, ".jfs.stats")

	var stStats, stJFS unix.Stat_t
	require.NoError(t, unix.Stat(statsPath, &stStats))
	require.NoError(t, unix.Stat(jfsStatsPath, &stJFS))
	require.Equal(t, virtualStatsExpectedIno, stStats.Ino)
	require.Equal(t, virtualStatsExpectedIno, stJFS.Ino)
	require.Equal(t, int64(virtualStatsExpectedSize), stStats.Size)
	require.Equal(t, int64(virtualStatsExpectedSize), stJFS.Size)
	require.Equal(t, uint32(syscall.S_IFREG|0o444), stStats.Mode)
	require.Equal(t, uint32(syscall.S_IFREG|0o444), stJFS.Mode)

	subDir := filepath.Join(mp, "sub")
	require.NoError(t, os.Mkdir(subDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(mp, "visible.txt"), []byte("hello"), 0o644))

	entries, err := os.ReadDir(mp)
	require.NoError(t, err)
	require.Len(t, entries, 2, "ReadDir 不得暴露隐藏的 .stats 或 .jfs.stats")
	entryNames := entries[0].Name() + "," + entries[1].Name()
	require.Equal(t, "sub,visible.txt", entryNames)

	subStatsPath := filepath.Join(subDir, ".stats")
	_, err = os.Stat(subStatsPath)
	require.ErrorIs(t, err, os.ErrNotExist, "子目录下同名 .stats 不得被虚拟统计文件拦截")
	require.NoError(t, os.WriteFile(subStatsPath, []byte("regular-child-stats"), 0o644))
	subData, err := os.ReadFile(subStatsPath)
	require.NoError(t, err)
	require.Equal(t, "regular-child-stats", string(subData))

	_, err = os.OpenFile(statsPath, os.O_WRONLY, 0)
	require.ErrorIs(t, err, syscall.EACCES)
	_, err = os.OpenFile(statsPath, os.O_RDWR, 0)
	require.ErrorIs(t, err, syscall.EACCES)
	_, err = os.OpenFile(statsPath, os.O_RDONLY|os.O_TRUNC, 0)
	require.ErrorIs(t, err, syscall.EACCES)
	_, err = os.OpenFile(statsPath, os.O_RDONLY|os.O_APPEND, 0)
	require.ErrorIs(t, err, syscall.EACCES)
	_, err = os.OpenFile(jfsStatsPath, os.O_CREATE|os.O_WRONLY, 0o644)
	require.ErrorIs(t, err, syscall.EACCES)

	srcFile, err := os.Open(statsPath)
	require.NoError(t, err)
	t.Cleanup(func() { _ = srcFile.Close() })

	dstPath := filepath.Join(t.TempDir(), "sendfile-out.txt")
	dstFile, err := os.OpenFile(dstPath, os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0o600)
	require.NoError(t, err)
	t.Cleanup(func() { _ = dstFile.Close() })

	var sendOffset int64
	sentBytes, err := unix.Sendfile(int(dstFile.Fd()), int(srcFile.Fd()), &sendOffset, virtualStatsExpectedSize)
	require.NoError(t, err, "内核 sendfile 读取虚拟 .stats 必须成功")
	require.Positive(t, sentBytes, "sendfile 返回字节数必须大于 0")
	require.NoError(t, srcFile.Close())
	require.NoError(t, dstFile.Close())

	sentContentBytes, err := os.ReadFile(dstPath)
	require.NoError(t, err)
	sentContent := string(sentContentBytes)
	require.Len(t, sentContent, sentBytes)
	require.Regexp(t, statsTwoColumnPattern, sentContent)
	opsBefore := parseStatsOpsTotal(t, sentContent)

	jfsSrc, err := os.Open(jfsStatsPath)
	require.NoError(t, err)
	var jfsBuilder strings.Builder
	copiedBytes, err := io.Copy(&jfsBuilder, jfsSrc)
	require.NoError(t, err)
	require.Positive(t, copiedBytes)
	require.NoError(t, jfsSrc.Close())
	require.Regexp(t, statsTwoColumnPattern, jfsBuilder.String())

	var stfs unix.Statfs_t
	require.NoError(t, unix.Statfs(mp, &stfs))
	_, err = os.Stat(filepath.Join(mp, "visible.txt"))
	require.NoError(t, err)

	afterBytes, err := os.ReadFile(statsPath)
	require.NoError(t, err)
	opsAfter := parseStatsOpsTotal(t, string(afterBytes))
	require.Greater(t, opsAfter, opsBefore, "后续 FUSE 操作完成后重新打开 .stats 计数必须递增")

	recordArtifact(t, map[string]string{
		"disabled_stats_err":    "ENOENT",
		"virtual_mode":          "0100444",
		"virtual_size":          strconv.Itoa(virtualStatsExpectedSize),
		"readdir_entries":       entryNames,
		"subdir_stats_writable": "true",
		"write_open_err":        "EACCES",
		"sendfile_two_col":      "true",
		"jfs_alias_two_col":     "true",
		"counter_monotonic":     strconv.FormatBool(opsAfter > opsBefore),
	})
}
