package cli

import (
	"context"
	"io"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diskGetUsageHelp = `用法:
  bowl disk get <disk-id> [flags]

参数:
  <disk-id>            目标虚拟磁盘 UUID（必填，非 Nil UUID）

标志:
  --json               以 JSON 格式输出虚拟磁盘详情
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

var diskGetSpec = diskSubcommandSpec{
	name:        "get",
	usageHelp:   diskGetUsageHelp,
	posArgLabel: "<disk-id>",
	uuidLabel:   "disk-id",
	spanName:    "cli.disk.get",
	timeout:     10 * time.Second,
	newAdapter:  func() diskRequestAdapter { return &diskGetAdapter{} },
}

type diskGetAdapter struct {
	diskID string
}

func (a *diskGetAdapter) parseFlag(_, _ string, _ bool, _ []string, _ *int) (bool, error) {
	return false, nil
}

func (a *diskGetAdapter) finalize(posArgs []string) (bool, error) {
	a.diskID = posArgs[0]
	return false, nil
}

func (a *diskGetAdapter) spanAttrs() []attribute.KeyValue {
	return []attribute.KeyValue{attribute.String("bowl.disk_id", a.diskID)}
}

func (a *diskGetAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskGetAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.GetDisk(ctx, connect.NewRequest(&storagev1.GetDiskRequest{
		DiskId: a.diskID,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			diskID:    a.diskID,
			defaultOp: "获取磁盘元数据失败",
		})
	}
	disk, cliErr := requireNonNilDisk(resp.Msg.GetDisk())
	if cliErr != nil {
		return diskOutcome{}, cliErr
	}
	return diskOutcome{disk: disk}, nil
}
