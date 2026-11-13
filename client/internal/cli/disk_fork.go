package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diskForkUsageHelp = `用法:
  bowl disk fork <source-disk-id> [flags]

参数:
  <source-disk-id>     派生源虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  --name string        新建虚拟磁盘名称 (可选，最大 255 字节)
  --size string        新建磁盘配额容量上限 (可选，省略时继承源盘配额；支持 B/K/KB/KiB/M/MB/MiB/G/GB/GiB/T/TB/TiB/P/PB/PiB，如 10GiB)
  -q, --quiet          静默模式，仅输出新建磁盘的 <disk-id>
  --json               以 JSON 格式输出新建磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

var diskForkSpec = diskSubcommandSpec{
	name:        "fork",
	usageHelp:   diskForkUsageHelp,
	posArgLabel: "<source-disk-id>",
	uuidLabel:   "source-disk-id",
	allowQuiet:  true,
	spanName:    "cli.disk.fork",
	timeout:     30 * time.Second,
	newAdapter:  func() diskRequestAdapter { return &diskForkAdapter{} },
}

type diskForkAdapter struct {
	sourceDiskID string
	sizeRaw      *string
	name         string
	sizePtr      *uint64
}

func (a *diskForkAdapter) parseFlag(name, val string, hasVal bool, args []string, idx *int) (bool, error) {
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
		a.name = v
		return true, nil
	default:
		return false, nil
	}
}

func (a *diskForkAdapter) finalize(posArgs []string) (bool, error) {
	a.sourceDiskID = posArgs[0]
	if a.sizeRaw != nil {
		maxBytes, err := ParseSize(*a.sizeRaw)
		if err != nil {
			return false, fmt.Errorf("无效的 --size 参数: %w", err)
		}
		a.sizePtr = &maxBytes
	}
	if err := validateDiskName(a.name); err != nil {
		return false, err
	}
	return false, nil
}

func (a *diskForkAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.source_disk_id", a.sourceDiskID)}
}

func (a *diskForkAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskForkAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ForkDisk(ctx, connect.NewRequest(&storagev1.ForkDiskRequest{
		SourceDiskId: a.sourceDiskID,
		Name:         a.name,
		MaxBytes:     a.sizePtr,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:    a.sourceDiskID,
			defaultOp: "派生虚拟磁盘失败",
			preconditionMsg: func(_ string, rpcErr error) string {
				return fmt.Sprintf("目标容量小于源磁盘已用容量: %v", rpcErr)
			},
		})
	}
	disk, cliErr := requireNonNilDisk(resp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: disk, setSpanDiskID: true}, nil
}
