package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/google/uuid"
	"github.com/hanwen/go-fuse/v2/fuse"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/client/internal/rpc"
	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const mountUsageHelp = `用法:
  bowl mount <disk-id> <mountpoint> [flags]

参数:
  <disk-id>       目标虚拟磁盘 UUID（必填，非 Nil UUID）
  <mountpoint>    本地挂载点目录路径（必填，已存在的目录）

标志:
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -f, --foreground     前台阻塞模式运行，输出日志到标准错误并响应 SIGINT/SIGTERM 退出
  --log-file string    日志文件路径 (仅用于后台守护模式)
  --allow-other        允许本机其他非 root 用户访问挂载点
  --ro                 只读挂载
  --direct-io          绕过内核页缓存直通数据
  --debug              开启调试日志
  -h, --help           查看帮助信息
`

var serveFUSE = defaultServeFUSE

// mountHandshake 向服务端发起单次 MountDisk 握手，返回注入了权威 Epoch、RootIno 与缓存超时配置的 fusefs.Config。
func mountHandshake(ctx context.Context, client storagev1connect.StorageServiceClient, baseCfg fusefs.Config) (fusefs.Config, error) {
	rpcCtx, rpcCancel := context.WithTimeout(ctx, 10*time.Second)
	defer rpcCancel()

	resp, err := client.MountDisk(rpcCtx, connect.NewRequest(&storagev1.MountDiskRequest{
		DiskId:   baseCfg.DiskID,
		ReadOnly: baseCfg.ReadOnly,
	}))
	if err != nil {
		if connectErr, ok := errors.AsType[*connect.Error](err); ok && connectErr.Code() == connect.CodeNotFound {
			return fusefs.Config{}, fmt.Errorf("磁盘不存在: %s", baseCfg.DiskID)
		}
		return fusefs.Config{}, fmt.Errorf("挂载磁盘握手失败: %w", err)
	}

	rootInode := resp.Msg.GetRootInode()
	if rootInode == nil || rootInode.GetIno() == 0 {
		return fusefs.Config{}, errors.New("服务端返回无效的磁盘根目录元数据")
	}

	epoch := resp.Msg.GetEpoch()
	if !baseCfg.ReadOnly && epoch == 0 {
		return fusefs.Config{}, errors.New("服务端未返回有效的写挂载世代号")
	}

	cfg := baseCfg
	cfg.Epoch = epoch
	cfg.RootIno = rootInode.GetIno()
	cfg.RootInode = rootInode
	cfg.EntryTimeoutSeconds = resp.Msg.GetEntryTimeoutSeconds()
	cfg.AttrTimeoutSeconds = resp.Msg.GetAttrTimeoutSeconds()
	return cfg, nil
}

func defaultServeFUSE(ctx context.Context, rawFS *fusefs.RawFS, cfg fusefs.Config, mountpoint string) error {
	opts := fusefs.BuildMountOptions(cfg)

	server, err := fuse.NewServer(rawFS, mountpoint, opts)
	if err != nil {
		return fmt.Errorf("创建 FUSE 服务失败: %w", err)
	}

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()

	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		<-serveDone
		return fmt.Errorf("等待挂载就绪失败: %w", err)
	}

	sigCh := make(chan os.Signal, 2)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	stopWait := make(chan struct{})
	go func() {
		select {
		case <-sigCh:
			_ = server.Unmount()
		case <-ctx.Done():
			_ = server.Unmount()
		case <-stopWait:
		}
	}()

	<-serveDone
	close(stopWait)
	return nil
}

// runForegroundMount 执行单次 MountDisk 握手并启动前台 FUSE 挂载服务。
func runForegroundMount(ctx context.Context, client storagev1connect.StorageServiceClient, baseCfg fusefs.Config, mountpoint string, _, _ io.Writer) error {
	cfg, err := mountHandshake(ctx, client, baseCfg)
	if err != nil {
		return err
	}

	rawFS := fusefs.NewRawFS(client, cfg)
	return serveFUSE(ctx, rawFS, cfg, mountpoint)
}

// executeMount 解析 mount 命令参数并启动挂载流程。
func executeMount(args []string, stdout, stderr io.Writer) int {
	var (
		endpoint   string
		token      string
		foreground bool
		logFile    string
		allowOther bool
		ro         bool
		directIO   bool
		debug      bool
		help       bool
	)

	var posArgs []string
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			posArgs = append(posArgs, args[i+1:]...)
			break
		}
		if strings.HasPrefix(arg, "-") {
			name := strings.TrimLeft(arg, "-")
			hasVal := strings.Contains(name, "=")
			var val string
			if hasVal {
				parts := strings.SplitN(name, "=", 2)
				name = parts[0]
				val = parts[1]
			}
			switch name {
			case "h", "help":
				help = true
			case "f", "foreground":
				foreground = true
			case "allow-other":
				allowOther = true
			case "ro":
				ro = true
			case "direct-io":
				directIO = true
			case "debug":
				debug = true
			case "endpoint":
				switch {
				case hasVal:
					endpoint = val
				case i+1 < len(args):
					i++
					endpoint = args[i]
				default:
					fmt.Fprintln(stderr, "错误: --endpoint 需要指定参数值")
					return 2
				}
			case "token":
				switch {
				case hasVal:
					token = val
				case i+1 < len(args):
					i++
					token = args[i]
				default:
					fmt.Fprintln(stderr, "错误: --token 需要指定参数值")
					return 2
				}
			case "log-file":
				switch {
				case hasVal:
					logFile = val
				case i+1 < len(args):
					i++
					logFile = args[i]
				default:
					fmt.Fprintln(stderr, "错误: --log-file 需要指定参数值")
					return 2
				}
			default:
				fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, mountUsageHelp)
				return 2
			}
		} else {
			posArgs = append(posArgs, arg)
		}
	}

	if help {
		fmt.Fprint(stdout, mountUsageHelp)
		return 0
	}

	if len(posArgs) != 2 {
		fmt.Fprintf(stderr, "错误: mount 命令需要且仅需要 2 个位置参数 (<disk-id> 与 <mountpoint>)\n\n%s", mountUsageHelp)
		return 2
	}

	diskIDStr := posArgs[0]
	mountpoint := posArgs[1]

	// 1. 静态前置校验: disk-id 必须为合法且非 Nil UUID
	parsedUUID, err := uuid.Parse(diskIDStr)
	if err != nil || parsedUUID == uuid.Nil {
		fmt.Fprintf(stderr, "错误: 无效的 disk-id \"%s\"，必须为合法的非 Nil UUID\n", diskIDStr)
		return 2
	}

	// 2. 静态前置校验: 挂载点必须为已存在的目录
	absMountpoint, err := filepath.Abs(mountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 挂载点路径解析失败: %v\n", err)
		return 2
	}
	absMountpoint = filepath.Clean(absMountpoint)

	fi, err := os.Stat(absMountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 挂载点 \"%s\" 不存在或无法访问: %v\n", absMountpoint, err)
		return 2
	}
	if !fi.IsDir() {
		fmt.Fprintf(stderr, "错误: 挂载点 \"%s\" 不是目录\n", absMountpoint)
		return 2
	}

	// 挂载前重复挂载校验：解析 /proc/self/mountinfo，若目标 <mountpoint> 已是活跃挂载点，在前台立即报错
	mounted, err := IsMountPoint(absMountpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 检查挂载点状态失败: %v\n", err)
		return 1
	}
	if mounted {
		fmt.Fprintf(stderr, "错误: 目标挂载点 \"%s\" 当前已挂载\n", absMountpoint)
		return 1
	}

	// 3. 解析 endpoint 与 token
	resolvedEndpoint, err := rpc.ResolveEndpoint(endpoint)
	if err != nil {
		fmt.Fprintf(stderr, "错误: 无效的 endpoint: %v\n", err)
		return 2
	}
	resolvedToken := rpc.ResolveToken(token)

	// 4. 创建 StorageClient
	client, err := rpc.NewClientFromConfig(rpc.ClientConfig{
		Endpoint: resolvedEndpoint,
		Token:    resolvedToken,
	})
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建 RPC 客户端失败: %v\n", err)
		return 1
	}

	baseCfg := fusefs.Config{
		DiskID:     diskIDStr,
		ReadOnly:   ro,
		DirectIO:   directIO,
		AllowOther: allowOther,
		Debug:      debug,
	}

	// 5. 模式选择：检测是否为内部后台工作子进程
	if os.Getenv("BOWL_DAEMON_WORKER") == "1" {
		return runDaemonWorker(context.Background(), client, baseCfg, absMountpoint, logFile)
	}

	// 若未指定 -f / --foreground，执行默认后台守护派生与就绪握手
	if !foreground {
		return spawnDaemonProcess(args, stderr)
	}

	runnerCtx, runnerCancel := context.WithCancel(context.Background())
	defer runnerCancel()

	if err := runForegroundMount(runnerCtx, client, baseCfg, absMountpoint, stdout, stderr); err != nil {
		fmt.Fprintf(stderr, "错误: 挂载执行失败: %v\n", err)
		return 1
	}

	return 0
}
