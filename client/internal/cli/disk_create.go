package cli

import (
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

const diskCreateUsageHelp = `用法:
  bowl disk create --size <size> [flags]

标志:
  --size string        配额容量上限，必填 (支持 B/K/KB/KiB/M/MB/MiB/G/GB/GiB/T/TB/TiB/P/PB/PiB，如 10GiB)
  --name string        虚拟磁盘名称 (可选，最大 255 字节)
  -q, --quiet          静默模式，仅输出新建磁盘的 <disk-id>
  --json               以 JSON 格式输出新建磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

var diskCreateSpec = diskSubcommandSpec{
	name:       "create",
	usageHelp:  diskCreateUsageHelp,
	allowQuiet: true,
	spanName:   "cli.disk.create",
	timeout:    10 * time.Second,
	newAdapter: func() diskRequestAdapter { return &diskCreateAdapter{} },
}

type diskCreateAdapter struct {
	sizeRaw  string
	name     string
	maxBytes uint64
}

func (a *diskCreateAdapter) parseFlag(name, val string, hasVal bool, args []string, idx *int) (bool, error) {
	var err error
	switch name {
	case "size":
		a.sizeRaw, err = takeFlagValue(args, idx, name, val, hasVal)
		return true, err
	case "name":
		a.name, err = takeFlagValue(args, idx, name, val, hasVal)
		return true, err
	default:
		return false, nil
	}
}

func (a *diskCreateAdapter) finalize(_ []string) (bool, error) {
	if strings.TrimSpace(a.sizeRaw) == "" {
		return true, errors.New("必须指定 --size 参数")
	}
	maxBytes, err := ParseSize(a.sizeRaw)
	if err != nil {
		return false, fmt.Errorf("无效的 --size 参数: %w", err)
	}
	if err := validateDiskName(a.name); err != nil {
		return false, err
	}
	a.maxBytes = maxBytes
	return false, nil
}

func (a *diskCreateAdapter) spanAttrs() []attribute.KeyValue {
	return nil
}

func (a *diskCreateAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskCreateAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.CreateDisk(ctx, connect.NewRequest(&storagev1.CreateDiskRequest{
		Name:     a.name,
		MaxBytes: a.maxBytes,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			defaultOp: "创建虚拟磁盘失败",
		})
	}
	disk, cliErr := requireNonNilDisk(resp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: disk, setSpanDiskID: true}, nil
}
