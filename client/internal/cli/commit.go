package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const commitHelp = `用法:
  bowl commit <disk-id> [--branch <分支>] [-m <消息> | --message <消息>] [--json] [--endpoint <url>] [--token <token>]

默认分支为 main。
`

func executeCommit(args []string, stdout, stderr io.Writer) int {
	opts, code := parseVersionArgs(args, map[string]bool{"branch": true, "message": true}, commitHelp, stdout, stderr)
	if code >= 0 {
		return code
	}
	diskID, ok := opts.diskID(stderr, commitHelp)
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
	client, code, err := newDiskStorageClient(opts.values["endpoint"], opts.values["token"])
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	resp, err := client.Commit(ctx, connect.NewRequest(&storagev1.CommitRequest{DiskId: diskID, BranchName: branch, Message: opts.values["message"]}))
	if err != nil {
		fmt.Fprintf(stderr, "错误: 创建版本快照失败: %v\n", err)
		return 1
	}
	commit := resp.Msg.GetCommit()
	if commit == nil || commit.GetCommitId() == "" {
		fmt.Fprintln(stderr, "错误: 服务端返回空版本快照")
		return 1
	}
	if opts.set["json"] {
		return writeVersionProtoJSON(stdout, stderr, commit)
	}
	fmt.Fprintf(stdout, "Commit ID: %s\nBranch:    %s\nMessage:   %s\n", commit.GetCommitId(), commit.GetBranchName(), commit.GetMessage())
	return 0
}
