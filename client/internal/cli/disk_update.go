package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diskUpdateUsageHelp = `用法:
  bowl disk update <disk-id> [flags]
  bowl disk resize <disk-id> [flags]

参数:
  <disk-id>            目标虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  --size string        更新后的配额容量上限 (支持 B/K/KB/KiB/M/MB/MiB/G/GB/GiB/T/TB/TiB/P/PB/PiB，如 20GiB)
  --name string        更新后的虚拟磁盘名称 (最大 255 字节，可传入 "" 清空名称)
  --json               以 JSON 格式输出更新后的虚拟磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

var diskUpdateSpec = diskSubcommandSpec{
	name:        "update",
	usageHelp:   diskUpdateUsageHelp,
	posArgLabel: "<disk-id>",
	uuidLabel:   "disk-id",
	spanName:    "cli.disk.update",
	timeout:     10 * time.Second,
	newAdapter:  func() diskRequestAdapter { return &diskUpdateAdapter{} },
}

type diskUpdateAdapter struct {
	diskID  string
	sizeRaw *string
	namePtr *string
	sizePtr *uint64
}

func (a *diskUpdateAdapter) parseFlag(name, val string, hasVal bool, args []string, idx *int) (bool, error) {
	switch name {
	case "size":
		v, err := takeFlagValue(args, idx, name, val, hasVal)
		if err != nil {
			return true, err
		}
		a.sizeRaw = &v
		return true, nil
	case "name":
		v, err := takeFlagValue(args, idx, name, val, hasVal)
		if err != nil {
			return true, err
		}
		a.namePtr = &v
		return true, nil
	default:
		return false, nil
	}
}

func (a *diskUpdateAdapter) finalize(posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	if a.sizeRaw != nil {
		maxBytes, err := ParseSize(*a.sizeRaw)
		if err != nil {
			return false, fmt.Errorf("无效的 --size 参数: %w", err)
		}
		a.sizePtr = &maxBytes
	}
	if a.namePtr != nil {
		if err := validateDiskName(*a.namePtr); err != nil {
			return false, err
		}
	}
	if a.sizePtr == nil && a.namePtr == nil {
		return true, errors.New("必须至少指定 --size 或 --name 之一")
	}
	return false, nil
}

func (a *diskUpdateAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskUpdateAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskUpdateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	// 第一层：当指定 --size 时，执行客户端 GetDisk 缩容水位线前置预检
	if a.sizePtr != nil {
		newMaxBytes := *a.sizePtr
		getResp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
			DiskId: a.diskID,
		}))
		if err != nil {
			return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
				diskID:    a.diskID,
				defaultOp: "获取磁盘元数据失败",
			})
		}
		currentDisk, cliErr := requireNonNilDisk(getResp.Msg.GetDisk())
		if cliErr != nil {
			return diskOutcome{}, cliErr
		}
		if newMaxBytes < currentDisk.GetUsedBytes() {
			return diskOutcome{}, &diskCLIError{
				exitCode: 1,
				msg: fmt.Sprintf(
					"目标容量 %d (%s) 小于当前已用容量 %d (%s)，不允许缩容至已用水位以下",
					newMaxBytes, FormatSize(newMaxBytes),
					currentDisk.GetUsedBytes(), FormatSize(currentDisk.GetUsedBytes()),
				),
			}
		}
	}

	// 第二层：调用 UpdateDisk 由服务端事务终审
	updateResp, err := client.UpdateDisk(ctx, connect.NewRequest(&storagev1.UpdateDiskRequest{
		DiskId:   a.diskID,
		Name:     a.namePtr,
		MaxBytes: a.sizePtr,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:    a.diskID,
			defaultOp: "更新磁盘元数据失败",
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("缩容失败，目标容量小于磁盘当前已用容量: %v", rpcErr)
			},
		})
	}
	updatedDisk, cliErr := requireNonNilDisk(updateResp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: updatedDisk}, nil
}
