package cli

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diskDeleteUsageHelp = `用法:
  bowl disk delete <disk-id> [flags]

参数:
  <disk-id>            目标虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  -y, --yes            跳过交互式确认提示，直接执行删除
  --json               以 JSON 格式输出删除结果
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

type diskDeleteJSON struct {
	DiskID  string `json:"disk_id"`
	Deleted bool   `json:"deleted"`
}

var diskDeleteSpec = diskSubcommandSpec{
	name:        "delete",
	usageHelp:   diskDeleteUsageHelp,
	posArgLabel: "<disk-id>",
	uuidLabel:   "disk-id",
	spanName:    "cli.disk.delete",
	timeout:     15 * time.Second,
	newAdapter:  func() diskRequestAdapter { return &diskDeleteAdapter{} },
}

type diskDeleteAdapter struct {
	diskID string
	yes    bool
}

func (a *diskDeleteAdapter) parseFlag(name, _ string, _ bool, _ []string, _ *int) (bool, error) {
	if name == "y" || name == "yes" {
		a.yes = true
		return true, nil
	}
	return false, nil
}

func (a *diskDeleteAdapter) finalize(posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	return false, nil
}

func (a *diskDeleteAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskDeleteAdapter) preRPC(ports diskPorts, stderr io.Writer) *diskCLIError {
	// 步骤 1：本机挂载表快检
	info, err := ports.findMountpoint(a.diskID)
	if err != nil {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("检查本机挂载表失败: %v", err),
		}
	}
	if info.Mounted {
		return &diskCLIError{
			exitCode: 1,
			msg:      fmt.Sprintf("磁盘 %s 当前正在本机挂载点 %s 处于挂载状态，请先执行 bowl umount %s 卸载后再删除", a.diskID, info.Mountpoint, info.Mountpoint),
		}
	}

	// 步骤 2：交互式二次确认
	if !a.yes {
		fmt.Fprintf(stderr, "确认删除虚拟磁盘 %s？此操作不可逆 [y/N]: ", a.diskID)
		line, readErr := bufio.NewReader(ports.stdin).ReadString('\n')
		if readErr != nil && !errors.Is(readErr, io.EOF) {
			return &diskCLIError{
				exitCode: 1,
				msg:      fmt.Sprintf("读取终端确认输入失败: %v", readErr),
			}
		}
		answer := strings.ToLower(strings.TrimSpace(line))
		if answer != "y" && answer != "yes" {
			fmt.Fprintln(stderr, "操作已取消")
			return &diskCLIError{exitCode: 1}
		}
	}

	return nil
}

func (a *diskDeleteAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	_, err := client.DeleteDisk(ctx, connect.NewRequest(&storagev1.DeleteDiskRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:           a.diskID,
			defaultOp:        "删除虚拟磁盘失败",
			checkInvalidArg:  true,
			checkEBUSYReason: true,
			preconditionMsg: func(id string, _ error) string {
				return fmt.Sprintf("磁盘 %s 当前仍处于挂载状态（存在活跃客户端会话），请先卸载后再删除", id)
			},
		})
	}

	return diskOutcome{
		jsonPayload: diskDeleteJSON{
			DiskID:  a.diskID,
			Deleted: true,
		},
		renderHuman: func(w io.Writer) {
			fmt.Fprintf(w, "已删除虚拟磁盘 %s\n", a.diskID)
		},
	}, nil
}
