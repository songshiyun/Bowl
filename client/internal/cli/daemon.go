package cli

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"

	"github.com/starsrc/bowl/client/internal/fusefs"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

// readDaemonHandshake 阻塞读取守护进程回传的挂载就绪状态。
func readDaemonHandshake(r io.Reader, stderr io.Writer) int {
	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "OK" {
			return 0
		}
		if after, ok := strings.CutPrefix(line, "ERR:"); ok {
			errMsg := after
			fmt.Fprintf(stderr, "错误: 后台守护进程初始化失败: %s\n", errMsg)
			return 1
		}
	}
	fmt.Fprintf(stderr, "错误: 后台守护进程异常退出，未完成挂载握手\n")
	return 1
}

// spawnDaemonProcess 由前台父进程派生后台守护进程并阻塞等待就绪握手。
func spawnDaemonProcess(args []string, stderr io.Writer) int {
	r, w, err := os.Pipe()
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建就绪通知管道失败: %v\n", err)
		return 1
	}
	defer r.Close()

	binPath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "错误: 获取可执行文件路径失败: %v\n", err)
		_ = w.Close()
		return 1
	}

	subArgs := append([]string{"mount"}, args...)
	cmd := exec.Command(binPath, subArgs...)
	cmd.ExtraFiles = []*os.File{w}
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Env = append(os.Environ(), "BOWL_DAEMON_WORKER=1")

	devNull, devNullErr := os.Open(os.DevNull)
	if devNullErr == nil {
		cmd.Stdin = devNull
		defer devNull.Close()
	}
	if err := cmd.Start(); err != nil {
		_ = w.Close()
		fmt.Fprintf(stderr, "错误: 启动后台守护进程失败: %v\n", err)
		return 1
	}

	// 父进程必须立即关闭自身的管道写端，否则子进程退出时读 r 无法触发 EOF
	_ = w.Close()

	return readDaemonHandshake(r, stderr)
}

// runDaemonWorker 是子进程内部的后台挂载执行器（单次 MountDisk 握手，无后台心跳续期协程）。
func runDaemonWorker(ctx context.Context, client storagev1connect.StorageServiceClient, baseCfg fusefs.Config, mountpoint, explicitLogFile string) int {
	pipeWriter := os.NewFile(3, "readiness-pipe")
	notifyErr := func(err error) {
		if pipeWriter != nil {
			fmt.Fprintf(pipeWriter, "ERR:%v\n", err)
			_ = pipeWriter.Close()
			pipeWriter = nil
		}
	}
	notifyOK := func() {
		if pipeWriter != nil {
			fmt.Fprintln(pipeWriter, "OK")
			_ = pipeWriter.Close()
			pipeWriter = nil
		}
	}

	// 1. 准备日志文件并重定向标准输出/错误
	logF, _, err := ResolveLogFile(explicitLogFile, baseCfg.DiskID)
	if err != nil {
		notifyErr(err)
		return 1
	}
	defer logF.Close()

	logFd := int(logF.Fd())
	_ = unix.Dup2(logFd, int(os.Stdout.Fd()))
	_ = unix.Dup2(logFd, int(os.Stderr.Fd()))

	// 2. 执行单次 MountDisk 握手
	cfg, err := mountHandshake(ctx, client, baseCfg)
	if err != nil {
		notifyErr(err)
		return 1
	}

	// 3. 构建 FUSE 服务
	rawFS := fusefs.NewRawFS(client, cfg)
	opts := fusefs.BuildMountOptions(cfg)

	server, err := fuse.NewServer(rawFS, mountpoint, opts)
	if err != nil {
		notifyErr(fmt.Errorf("创建 FUSE 服务失败: %w", err))
		return 1
	}

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		server.Serve()
	}()

	// 4. 等待内核挂载彻底就绪
	if err := server.WaitMount(); err != nil {
		_ = server.Unmount()
		<-serveDone
		notifyErr(fmt.Errorf("等待挂载就绪失败: %w", err))
		return 1
	}

	// 5. 挂载已彻底就绪，通知父进程退出 0
	notifyOK()

	// 6. 优雅卸载信号与阻塞等待
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
	_ = logF.Sync()
	return 0
}
