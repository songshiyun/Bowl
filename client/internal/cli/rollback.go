package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const rollbackHelp = `用法:
  bowl rollback <disk-id> [--branch <分支>] --to <commit-id> [--json] [--endpoint <url>] [--token <token>]

默认分支为 main；回档前须卸载目标分支的可写客户端。
`

func executeRollback(args []string, stdout, stderr io.Writer) int {
	opts, code := parseVersionArgs(args, map[string]bool{"branch": true, "to": true}, rollbackHelp, stdout, stderr)
	if code >= 0 {
		return code
	}
	diskID, ok := opts.diskID(stderr, rollbackHelp)
	if !ok {
		return 2
	}
	branch := "main"
	if opts.set["branch"] {
		branch = opts.values["branch"]
	}
	if !validVersionBranchName(branch) {
		fmt.Fprintln(stderr, "错误: --branch 必须是有效分支名")
		return 2
	}
	commit, ok := parseVersionUUID(opts.values["to"])
	if !ok {
		fmt.Fprintln(stderr, "错误: --to 必须是非零 UUID")
		return 2
	}
	client, code, err := newDiskStorageClient(opts.values["endpoint"], opts.values["token"])
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	_, err = client.Rollback(ctx, connect.NewRequest(&storagev1.RollbackRequest{DiskId: diskID, BranchName: branch, CommitId: commit}))
	if err != nil {
		if connect.CodeOf(err) == connect.CodeFailedPrecondition {
			fmt.Fprintf(stderr, "错误: 目标分支存在活跃写端 (EBUSY)，请先卸载可写挂载后再执行回档: %v\n", err)
		} else {
			fmt.Fprintf(stderr, "错误: 回档分支失败: %v\n", err)
		}
		return 1
	}
	if opts.set["json"] {
		return writeVersionJSON(stdout, stderr, map[string]string{"disk_id": diskID, "branch": branch, "commit_id": commit})
	}
	fmt.Fprintf(stdout, "分支 %s 已回档至 %s\n", branch, commit)
	return 0
}
