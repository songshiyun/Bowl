package cli

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/starsrc/Aether/pkg/observability/telemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/starsrc/bowl/client/internal/fusefs"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

var w3cTraceparentPattern = regexp.MustCompile(`^00-[0-9a-f]{32}-[0-9a-f]{16}-(00|01)$`)

type capturedHeader struct {
	path        string
	traceparent string
	requestID   string
}

func newHeaderCapturingStorageServer(t *testing.T, diskID string) (endpoint string, getHeaders func() []capturedHeader) {
	t.Helper()
	svc := &fakeStorageService{
		disks: make(map[string]*storagev1.Disk),
	}
	now := timestamppb.New(time.Unix(1700000000, 0).UTC())
	svc.disks[diskID] = &storagev1.Disk{
		DiskId:     diskID,
		Name:       "obs-disk",
		RootIno:    1,
		MaxBytes:   1 << 30,
		UsedBytes:  4096,
		UsedInodes: 1,
		CreatedAt:  now,
		UpdatedAt:  now,
	}

	mux := http.NewServeMux()
	path, handler := storagev1connect.NewStorageServiceHandler(svc)
	var (
		mu      sync.Mutex
		headers []capturedHeader
	)
	mux.Handle(path, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		headers = append(headers, capturedHeader{
			path:        r.URL.Path,
			traceparent: r.Header.Get("Traceparent"),
			requestID:   r.Header.Get("X-Request-Id"),
		})
		mu.Unlock()
		handler.ServeHTTP(w, r)
	}))

	var protocols http.Protocols
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := httptest.NewUnstartedServer(mux)
	srv.Config.Protocols = &protocols
	srv.Start()
	t.Cleanup(srv.Close)

	return srv.URL, func() []capturedHeader {
		mu.Lock()
		defer mu.Unlock()
		out := make([]capturedHeader, len(headers))
		copy(out, headers)
		return out
	}
}

func parseJSONLogLines(t *testing.T, raw string) []map[string]any {
	t.Helper()
	var entries []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(raw))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || !strings.HasPrefix(line, "{") {
			continue
		}
		var m map[string]any
		require.NoError(t, json.Unmarshal([]byte(line), &m), "invalid JSON log line: %s", line)
		entries = append(entries, m)
	}
	require.NoError(t, scanner.Err())
	return entries
}

func findLogByMsg(entries []map[string]any, msg string) (map[string]any, bool) {
	for _, e := range entries {
		if e["msg"] == msg {
			return e, true
		}
	}
	return nil, false
}

func httpGetBody(t *testing.T, rawURL string) (statusCode int, body string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	require.NoError(t, err)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	rawBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	return resp.StatusCode, string(rawBody)
}

func TestBuildObsConfig_ExporterSwitchAndEnvOverride(t *testing.T) {
	opts := &mountOptions{
		logLevel:         "debug",
		logFormat:        "json",
		traceSampleRatio: 0.05,
	}

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_SERVICE_NAME", "")
	cfgNone := buildObsConfig(opts)
	require.True(t, cfgNone.Enabled())
	require.Equal(t, telemetry.ExporterNone, cfgNone.TracesExporter())
	require.Equal(t, "bowl-client", cfgNone.ServiceName())
	require.InDelta(t, 0.05, cfgNone.SampleRatio(), 1e-9)
	require.NoError(t, cfgNone.Validate())

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317")
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "false")
	t.Setenv("OTEL_SERVICE_NAME", "bowl-client-bench")
	cfgOTLP := buildObsConfig(opts)
	require.True(t, cfgOTLP.Enabled())
	require.Equal(t, telemetry.ExporterOTLP, cfgOTLP.TracesExporter())
	require.Equal(t, "127.0.0.1:4317", cfgOTLP.Endpoint())
	require.False(t, cfgOTLP.Insecure())
	require.Equal(t, "bowl-client-bench", cfgOTLP.ServiceName())
	require.NoError(t, cfgOTLP.Validate())
}

func TestMount_FlagValidationAndEnvFallback(t *testing.T) {
	var stdout, stderr bytes.Buffer
	require.Equal(t, 0, Execute([]string{"mount", "--help"}, &stdout, &stderr))
	for _, flagName := range []string{"--admin-addr", "--stats-file", "--log-level", "--log-format", "--trace-sample-ratio"} {
		assert.Contains(t, stdout.String(), flagName)
	}

	diskID := uuid.NewString()
	mp := t.TempDir()

	invalidCases := [][]string{
		{"mount", "--admin-addr", "not-a-host-port", diskID, mp},
		{"mount", "--admin-addr", diskID, mp},
		{"mount", "--log-level", "verbose", diskID, mp},
		{"mount", "--log-format", "yaml", diskID, mp},
		{"mount", "--trace-sample-ratio", "-0.01", diskID, mp},
		{"mount", "--trace-sample-ratio", "1.5", diskID, mp},
		{"mount", "--trace-sample-ratio", "NaN", diskID, mp},
		{"mount", "--stats-file=maybe", diskID, mp},
	}
	for _, args := range invalidCases {
		stdout.Reset()
		stderr.Reset()
		code := Execute(args, &stdout, &stderr)
		require.Equal(t, 2, code, "args=%v stderr=%s", args, stderr.String())
	}

	// 验证环境变量兜底与显式标志覆盖优先级
	t.Setenv("BOWL_ADMIN_ADDR", "off")
	t.Setenv("LOG_LEVEL", "warn")
	t.Setenv("LOG_FORMAT", "text")
	t.Setenv("OTEL_TRACES_SAMPLER_ARG", "0.25")

	opts := mountOptions{
		adminAddr:        defaultAdminAddr,
		logLevel:         "info",
		logFormat:        "json",
		traceSampleRatio: defaultTraceSampleRatio,
	}
	require.NoError(t, applyEnvAndValidateOptions(&opts, mountFlagPresence{}))
	require.Equal(t, "off", opts.adminAddr)
	require.Equal(t, "warn", opts.logLevel)
	require.Equal(t, "text", opts.logFormat)
	require.InDelta(t, 0.25, opts.traceSampleRatio, 1e-9)

	explicit := mountOptions{
		adminAddr:        "127.0.0.1:0",
		logLevel:         "debug",
		logFormat:        "json",
		traceSampleRatio: 0.001,
	}
	require.NoError(t, applyEnvAndValidateOptions(&explicit, mountFlagPresence{
		adminAddr:        true,
		logLevel:         true,
		logFormat:        true,
		traceSampleRatio: true,
	}))
	require.Equal(t, "127.0.0.1:0", explicit.adminAddr)
	require.Equal(t, "debug", explicit.logLevel)
	require.Equal(t, "json", explicit.logFormat)
	require.InDelta(t, 0.001, explicit.traceSampleRatio, 1e-9)
}

func TestMount_FullObservabilityAdminStatsAndLifecycle(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	diskID := uuid.NewString()
	endpoint, getHeaders := newHeaderCapturingStorageServer(t, diskID)

	// 构造不可建目录的主发现路径（父路径为普通文件，容器内 root 同样无法写入）以验证自动回退至次选目录
	blockerFile := filepath.Join(t.TempDir(), "blocked-run")
	require.NoError(t, os.WriteFile(blockerFile, []byte("blocked"), 0o444))
	unwritablePrimary := filepath.Join(blockerFile, "mounts")
	fallbackDir := filepath.Join(t.TempDir(), "bowl-mounts")

	origMetaDirs := mountMetaDirs
	mountMetaDirs = func() []string { return []string{unwritablePrimary, fallbackDir} }
	t.Cleanup(func() { mountMetaDirs = origMetaDirs })

	// 预先植入一条死进程残留发现文件，验证读取端自动忽略
	staleMeta := mountMeta{
		Mountpoint: "/mnt/stale-dead",
		PID:        -1,
		DiskID:     uuid.NewString(),
		AdminAddr:  "127.0.0.1:65530",
		StartedAt:  time.Now().UTC(),
	}
	staleBytes, err := json.Marshal(staleMeta)
	require.NoError(t, err)
	require.NoError(t, os.MkdirAll(fallbackDir, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(fallbackDir, mountMetaFileName(staleMeta.Mountpoint)), staleBytes, 0o644))

	origServe := serveFUSE
	t.Cleanup(func() { serveFUSE = origServe })

	mp := t.TempDir()
	var activeAdminAddr string
	var metaFilePath string

	serveFUSE = func(_ context.Context, instFS *fusefs.InstrumentedRawFS, cfg *fusefs.Config, mountpoint string, ready func()) error {
		defer instFS.OnUnmount()
		require.True(t, cfg.EnableStatsFile, "--stats-file 必须注入 fusefs.Config.EnableStatsFile")
		require.NotNil(t, cfg.Logger)
		require.NotNil(t, cfg.Metrics)
		require.NotNil(t, cfg.TracerProvider)

		// 触发真实 FUSE 操作（含根目录隐藏虚拟文件 .stats 与跨机 RPC Mkdir/Lookup）
		var statsOut fuse.EntryOut
		st := instFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, ".stats", &statsOut)
		require.Equal(t, fuse.OK, st)
		require.Equal(t, fusefs.VirtualStatsIno, statsOut.NodeId)

		var entryOut fuse.EntryOut
		require.Equal(t, fuse.OK, instFS.Mkdir(nil, &fuse.MkdirIn{InHeader: fuse.InHeader{NodeId: 1}, Mode: 0o755}, "dir1", &entryOut))
		require.Equal(t, fuse.OK, instFS.Lookup(nil, &fuse.InHeader{NodeId: 1}, "f1", &entryOut))

		ready()

		// 验证发现元数据已落盘到回退目录且跳过死进程残留
		metas := listMountMetas()
		require.Len(t, metas, 1)
		require.Equal(t, filepath.Clean(mountpoint), metas[0].Mountpoint)
		require.Equal(t, os.Getpid(), metas[0].PID)
		require.Equal(t, diskID, metas[0].DiskID)
		require.NotEmpty(t, metas[0].AdminAddr)
		require.NotEqual(t, "off", metas[0].AdminAddr)
		activeAdminAddr = metas[0].AdminAddr
		metaFilePath = filepath.Join(fallbackDir, mountMetaFileName(mountpoint))
		require.FileExists(t, metaFilePath)

		baseURL := "http://" + activeAdminAddr

		// 1) /healthz 与 /readyz
		code, body := httpGetBody(t, baseURL+"/healthz")
		require.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, `"status":"ok"`)

		code, body = httpGetBody(t, baseURL+"/readyz")
		require.Equal(t, http.StatusOK, code)
		assert.Contains(t, body, `"status":"ok"`)

		// 2) /metrics 同时包含 FUSE 36 槽位指标与客户端 RPC 指标
		code, metricsBody := httpGetBody(t, baseURL+"/metrics")
		require.Equal(t, http.StatusOK, code)
		assert.Contains(t, metricsBody, `bowl_client_fuse_op_duration_seconds_count{op="Mkdir"} 1`)
		assert.Contains(t, metricsBody, `bowl_client_fuse_op_duration_seconds_count{op="Lookup"} 2`)
		assert.Contains(t, metricsBody, "bowl_client_stale_epoch_total")
		assert.Contains(t, metricsBody, "rpc_client_call_duration")

		// 3) /debug/stats 支持默认 json、?format=text、?format=stats 以及非法格式 400
		code, statsJSON := httpGetBody(t, baseURL+"/debug/stats")
		require.Equal(t, http.StatusOK, code)
		assert.True(t, json.Valid([]byte(statsJSON)), "默认 /debug/stats 应返回合法 JSON")

		code, statsText := httpGetBody(t, baseURL+"/debug/stats?format=text")
		require.Equal(t, http.StatusOK, code)
		assert.Contains(t, statsText, "FUSE: calls=3 errors=0")

		code, statsRaw := httpGetBody(t, baseURL+"/debug/stats?format=stats")
		require.Equal(t, http.StatusOK, code)
		assert.Contains(t, statsRaw, "juicefs_fuse_ops_durations_histogram_seconds_total 3")
		assert.Contains(t, statsRaw, "bowl_client_fuse_op_calls_Mkdir 1")
		assert.Contains(t, statsRaw, "bowl_client_fuse_op_calls_Lookup 2")

		code, _ = httpGetBody(t, baseURL+"/debug/stats?format=xml")
		require.Equal(t, http.StatusBadRequest, code)

		// 4) /debug/pprof/* 正常响应且未污染全局 http.DefaultServeMux
		for _, pprofPath := range []string{"/debug/pprof/", "/debug/pprof/cmdline", "/debug/pprof/heap", "/debug/pprof/mutex"} {
			pCode, pBody := httpGetBody(t, baseURL+pprofPath)
			require.Equal(t, http.StatusOK, pCode, "pprof path %s failed", pprofPath)
			require.NotEmpty(t, pBody)
		}
		rec := httptest.NewRecorder()
		http.DefaultServeMux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/debug/pprof/", http.NoBody))
		require.Equal(t, http.StatusNotFound, rec.Code, "不得向全局 http.DefaultServeMux 泄露 pprof 路由")

		// 5) bowl stats 单挂载免参数自动发现、显式挂载点、--json、--raw、--admin-addr
		var sOut, sErr bytes.Buffer
		require.Equal(t, 0, Execute([]string{"stats"}, &sOut, &sErr), sErr.String())
		assert.Contains(t, sOut.String(), "FUSE: calls=3 errors=0")

		sOut.Reset()
		sErr.Reset()
		require.Equal(t, 0, Execute([]string{"stats", mp, "--json"}, &sOut, &sErr), sErr.String())
		assert.True(t, json.Valid(bytes.TrimSpace(sOut.Bytes())))

		sOut.Reset()
		sErr.Reset()
		require.Equal(t, 0, Execute([]string{"stats", "--raw"}, &sOut, &sErr), sErr.String())
		assert.Contains(t, sOut.String(), "bowl_client_fuse_op_calls_Mkdir 1")

		sOut.Reset()
		sErr.Reset()
		require.Equal(t, 0, Execute([]string{"stats", "--admin-addr", activeAdminAddr, "--json"}, &sOut, &sErr), sErr.String())
		assert.True(t, json.Valid(bytes.TrimSpace(sOut.Bytes())))

		return nil
	}

	var stdout, stderr bytes.Buffer
	exitCode := Execute([]string{
		"mount", "-f",
		"--endpoint", endpoint,
		"--admin-addr", "127.0.0.1:0",
		"--stats-file",
		"--log-level", "info",
		"--log-format", "json",
		"--trace-sample-ratio", "1.0",
		diskID, mp,
	}, &stdout, &stderr)
	require.Equal(t, 0, exitCode, "stderr=%s", stderr.String())

	// 卸载后：发现文件已清理、管理端口已优雅关闭
	_, statErr := os.Stat(metaFilePath)
	require.ErrorIs(t, statErr, os.ErrNotExist, "卸载后必须清理挂载发现元数据文件")

	connCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	closedReq, err := http.NewRequestWithContext(connCtx, http.MethodGet, "http://"+activeAdminAddr+"/healthz", http.NoBody)
	require.NoError(t, err)
	closedResp, closedErr := http.DefaultClient.Do(closedReq)
	if closedResp != nil {
		_ = closedResp.Body.Close()
	}
	require.Error(t, closedErr, "卸载后管理面 HTTP 端口必须已关闭")

	// 验证全部出站 RPC 均携带 W3C traceparent 与 X-Request-Id
	captured := getHeaders()
	require.Len(t, captured, 3, "MountDisk + Mkdir + Lookup 共 3 次 RPC")
	for _, h := range captured {
		assert.Regexp(t, w3cTraceparentPattern, h.traceparent, "RPC %s 缺失合法 traceparent", h.path)
		assert.NotEmpty(t, h.requestID, "RPC %s 缺失 X-Request-Id", h.path)
	}

	// 验证结构化生命周期日志 mount_ready 与 unmount_complete
	entries := parseJSONLogLines(t, stderr.String())
	readyEntry, ok := findLogByMsg(entries, "mount_ready")
	require.True(t, ok, "缺失 mount_ready 日志: %s", stderr.String())
	assert.Equal(t, filepath.Clean(mp), readyEntry["mountpoint"])
	assert.Equal(t, endpoint, readyEntry["server_addr"])
	assert.Equal(t, diskID, readyEntry["disk_id"])
	assert.InDelta(t, 1.0, readyEntry["epoch"], 1e-9)
	assert.InDelta(t, 1.0, readyEntry["root_ino"], 1e-9)
	assert.Equal(t, activeAdminAddr, readyEntry["admin_addr"])
	assert.InDelta(t, float64(os.Getpid()), readyEntry["pid"], 1e-9)

	unmountEntry, ok := findLogByMsg(entries, "unmount_complete")
	require.True(t, ok, "缺失 unmount_complete 日志: %s", stderr.String())
	assert.Equal(t, filepath.Clean(mp), unmountEntry["mountpoint"])
	assert.Equal(t, diskID, unmountEntry["disk_id"])
}

func TestStats_DiscoveryEdgeCasesAndAdminOff(t *testing.T) {
	metaDir := t.TempDir()
	origMetaDirs := mountMetaDirs
	mountMetaDirs = func() []string { return []string{metaDir} }
	t.Cleanup(func() { mountMetaDirs = origMetaDirs })

	var stdout, stderr bytes.Buffer

	// 1) --help 与参数错误
	require.Equal(t, 0, Execute([]string{"stats", "--help"}, &stdout, &stderr))
	assert.Contains(t, stdout.String(), "--admin-addr")

	for _, badArgs := range [][]string{
		{"stats", "--unknown"},
		{"stats", "--json", "--raw"},
		{"stats", "/mnt/a", "/mnt/b"},
		{"stats", "--admin-addr"},
		{"stats", "--admin-addr", "off"},
		{"stats", "--admin-addr", "bad-host-port"},
	} {
		stdout.Reset()
		stderr.Reset()
		require.Equal(t, 2, Execute(badArgs, &stdout, &stderr), "args=%v", badArgs)
	}

	// 2) 零挂载
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, Execute([]string{"stats"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "未发现活跃的 bowl 挂载")

	// 3) --admin-addr off 挂载时，发现文件写入 "off"，bowl stats 明确报错
	mp1 := filepath.Join(t.TempDir(), "mp1")
	require.NoError(t, os.MkdirAll(mp1, 0o755))
	offMeta := mountMeta{
		Mountpoint: mp1,
		PID:        os.Getpid(),
		DiskID:     uuid.NewString(),
		AdminAddr:  "off",
		StartedAt:  time.Now().UTC(),
	}
	offPath, err := writeMountMeta(&offMeta)
	require.NoError(t, err)

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, Execute([]string{"stats"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "未开启管理面")

	require.NoError(t, os.Remove(offPath))

	// 4) 多活跃挂载时无参调用提示指定挂载点；指定其一成功
	fakeAdmin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = io.WriteString(w, "ok-stats-output\n")
	}))
	t.Cleanup(fakeAdmin.Close)
	adminHostPort := strings.TrimPrefix(fakeAdmin.URL, "http://")

	mp2 := filepath.Join(t.TempDir(), "mp2")
	require.NoError(t, os.MkdirAll(mp2, 0o755))
	for _, mp := range []string{mp1, mp2} {
		m := mountMeta{
			Mountpoint: mp,
			PID:        os.Getpid(),
			DiskID:     uuid.NewString(),
			AdminAddr:  adminHostPort,
			StartedAt:  time.Now().UTC(),
		}
		_, werr := writeMountMeta(&m)
		require.NoError(t, werr)
	}

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, Execute([]string{"stats"}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "发现多个活跃挂载")

	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 0, Execute([]string{"stats", mp2}, &stdout, &stderr), stderr.String())
	assert.Equal(t, "ok-stats-output\n", stdout.String())

	// 不存在的指定挂载点返回退出码 1
	stdout.Reset()
	stderr.Reset()
	require.Equal(t, 1, Execute([]string{"stats", filepath.Join(t.TempDir(), "nonexistent")}, &stdout, &stderr))
	assert.Contains(t, stderr.String(), "未找到挂载点")
}

// TestDaemonWorker_SubprocessFlagPassthroughAndHandshake 通过真实子进程验证 BOWL_DAEMON_WORKER=1
// 下 fd 3 就绪管道握手、新增可观测性标志透传、日志文件结构化输出与优雅退出。
func TestDaemonWorker_SubprocessFlagPassthroughAndHandshake(t *testing.T) {
	if os.Getenv("BOWL_TEST_DAEMON_SUBPROC") == "1" {
		metaDir := os.Getenv("BOWL_TEST_META_DIR")
		mountMetaDirs = func() []string { return []string{metaDir} }
		serveFUSE = func(ctx context.Context, instFS *fusefs.InstrumentedRawFS, _ *fusefs.Config, _ string, ready func()) error {
			defer instFS.OnUnmount()
			ready()
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGTERM)
			defer signal.Stop(sigCh)
			timer := time.NewTimer(5 * time.Second)
			defer timer.Stop()
			select {
			case <-ctx.Done():
			case <-sigCh:
			case <-timer.C:
			}
			return nil
		}
		sepIdx := -1
		for i, a := range os.Args {
			if a == "--" {
				sepIdx = i
				break
			}
		}
		if sepIdx < 0 {
			os.Exit(2)
		}
		os.Exit(Execute(os.Args[sepIdx+1:], os.Stdout, os.Stderr))
	}

	diskID := uuid.NewString()
	endpoint, _ := newHeaderCapturingStorageServer(t, diskID)
	mp := t.TempDir()
	metaDir := t.TempDir()
	logPath := filepath.Join(t.TempDir(), "daemon.log")

	pipeR, pipeW, err := os.Pipe()
	require.NoError(t, err)
	defer pipeR.Close()

	testBin, err := os.Executable()
	require.NoError(t, err)

	cmd := exec.Command(
		testBin,
		"-test.run=^TestDaemonWorker_SubprocessFlagPassthroughAndHandshake$",
		"--",
		"mount",
		"--endpoint", endpoint,
		"--log-file", logPath,
		"--admin-addr", "127.0.0.1:0",
		"--stats-file",
		"--log-level", "debug",
		"--log-format", "json",
		"--trace-sample-ratio", "0.01",
		diskID, mp,
	)
	cmd.ExtraFiles = []*os.File{pipeW}
	cmd.Env = append(os.Environ(),
		"BOWL_TEST_DAEMON_SUBPROC=1",
		"BOWL_DAEMON_WORKER=1",
		"BOWL_TEST_META_DIR="+metaDir,
		"OTEL_EXPORTER_OTLP_ENDPOINT=",
	)
	require.NoError(t, cmd.Start())
	require.NoError(t, pipeW.Close())

	var handshakeErr bytes.Buffer
	require.Equal(t, 0, readDaemonHandshake(pipeR, &handshakeErr), handshakeErr.String())

	// 子进程握手返回 OK 的同一时刻，发现元数据与管理面 HTTP 已立即可用
	origMetaDirs := mountMetaDirs
	mountMetaDirs = func() []string { return []string{metaDir} }
	t.Cleanup(func() { mountMetaDirs = origMetaDirs })

	meta, ok := findMountMeta(mp)
	require.True(t, ok, "守护子进程就绪后必须已写入发现元数据")
	require.Equal(t, cmd.Process.Pid, meta.PID)

	code, body := httpGetBody(t, "http://"+meta.AdminAddr+"/healthz")
	require.Equal(t, http.StatusOK, code)
	assert.Contains(t, body, `"status":"ok"`)

	require.NoError(t, cmd.Process.Signal(syscall.SIGTERM))
	require.NoError(t, cmd.Wait())

	logBytes, err := os.ReadFile(logPath)
	require.NoError(t, err)
	entries := parseJSONLogLines(t, string(logBytes))
	_, hasReady := findLogByMsg(entries, "mount_ready")
	_, hasUnmount := findLogByMsg(entries, "unmount_complete")
	assert.True(t, hasReady, "守护进程日志必须包含 mount_ready: %s", string(logBytes))
	assert.True(t, hasUnmount, "守护进程日志必须包含 unmount_complete: %s", string(logBytes))

	// 子进程退出后，再次查询不再返回残留记录
	_, stillAlive := findMountMeta(mp)
	require.False(t, stillAlive)
}
