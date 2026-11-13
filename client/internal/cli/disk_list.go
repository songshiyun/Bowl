package cli

import (
	"context"
	"fmt"
	"io"
	"strconv"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"
	"go.opentelemetry.io/otel/attribute"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
	"github.com/starsrc/bowl/gen/go/storage/v1/storagev1connect"
)

const diskListUsageHelp = `用法:
  bowl disk list [flags]

标志:
  --limit int          单页最大返回条数，取值范围 [1, 1000] (默认 50)
  --page-token string  游标分页令牌
  --json               以 JSON 格式输出虚拟磁盘列表与下一页游标
  --endpoint string    StorageService 基础地址 (默认读取 $BOWL_STORAGE_ENDPOINT 或 http://127.0.0.1:9000)
  --token string       访问令牌 (默认读取 $BOWL_TOKEN)
  -h, --help           查看帮助信息
`

type diskListJSON struct {
	Disks         []diskJSON `json:"disks"`
	NextPageToken string     `json:"next_page_token"`
}

var diskListSpec = diskSubcommandSpec{
	name:       "list",
	usageHelp:  diskListUsageHelp,
	spanName:   "cli.disk.list",
	timeout:    10 * time.Second,
	newAdapter: func() diskRequestAdapter { return &diskListAdapter{} },
}

type diskListAdapter struct {
	limitRaw  string
	limitSet  bool
	pageToken string
	pageSize  uint32
}

func (a *diskListAdapter) parseFlag(name, val string, hasVal bool, args []string, idx *int) (bool, error) {
	var err error
	switch name {
	case "limit":
		a.limitSet = true
		a.limitRaw, err = takeFlagValue(args, idx, name, val, hasVal)
		return true, err
	case "page-token":
		a.pageToken, err = takeFlagValue(args, idx, name, val, hasVal)
		return true, err
	default:
		return false, nil
	}
}

func (a *diskListAdapter) finalize(_ []string) (bool, error) {
	if a.limitSet {
		parsedLimit, err := strconv.Atoi(a.limitRaw)
		if err != nil || parsedLimit < 1 || parsedLimit > 1000 {
			return false, fmt.Errorf("无效的 --limit 参数 \"%s\"，必须为 [1, 1000] 范围内的整数", a.limitRaw)
		}
		a.pageSize = uint32(parsedLimit)
	}
	return false, nil
}

func (a *diskListAdapter) spanAttrs() []attribute.KeyValue {
	return nil
}

func (a *diskListAdapter) preRPC(_ diskPorts, _ io.Writer) *diskCLIError {
	return nil
}

func (a *diskListAdapter) invoke(ctx context.Context, client storagev1connect.StorageServiceClient) (diskOutcome, *diskCLIError) {
	resp, err := client.ListDisks(ctx, connect.NewRequest(&storagev1.ListDisksRequest{
		PageSize:  a.pageSize,
		PageToken: a.pageToken,
	}))
	if err != nil {
		return diskOutcome{}, mapDiskRPCError(err, diskRPCErrorPolicy{
			defaultOp: "列出虚拟磁盘失败",
		})
	}

	disks := resp.Msg.GetDisks()
	nextPageToken := resp.Msg.GetNextPageToken()

	items := make([]diskJSON, 0, len(disks))
	for _, d := range disks {
		items = append(items, toDiskJSON(d))
	}

	return diskOutcome{
		jsonPayload: diskListJSON{
			Disks:         items,
			NextPageToken: nextPageToken,
		},
		renderHuman: func(w io.Writer) {
			renderDiskListTable(w, items, nextPageToken)
		},
	}, nil
}

func renderDiskListTable(w io.Writer, items []diskJSON, nextPageToken string) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "DISK ID\tNAME\tUSED\tMAX\tUSAGE%\tINODES\tCREATED AT")
	for _, dj := range items {
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%.2f%%\t%d\t%s\n",
			dj.DiskID,
			dj.Name,
			FormatSize(dj.UsedBytes),
			FormatSize(dj.MaxBytes),
			diskUsagePct(dj.UsedBytes, dj.MaxBytes),
			dj.UsedInodes,
			dj.CreatedAt,
		)
	}
	_ = tw.Flush()

	if nextPageToken != "" {
		fmt.Fprintf(w, "\nNext Page Token: %s\n", nextPageToken)
	}
}
