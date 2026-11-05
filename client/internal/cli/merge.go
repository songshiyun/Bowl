package cli

import (
	"context"
	"fmt"
	"io"
	"time"

	"connectrpc.com/connect"
	"google.golang.org/protobuf/encoding/protojson"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const mergeHelp = `用法:
  bowl merge <disk-id> --from <源分支> --into <目标分支> [--strategy abort|ours|theirs|manual] [--json] [--endpoint <url>] [--token <token>]
  bowl merge <disk-id> --into <目标分支> --continue [-m <消息> | --message <消息>] [--json] [--endpoint <url>] [--token <token>]
  bowl merge <disk-id> --into <目标分支> --abort [--json] [--endpoint <url>] [--token <token>]

目标分支须先卸载可写客户端；manual 也可写为 materialize。
`

func executeMerge(args []string, stdout, stderr io.Writer) int {
	opts, code := parseVersionArgs(args, map[string]bool{"from": true, "into": true, "strategy": true, "continue": false, "abort": false, "message": true}, mergeHelp, stdout, stderr)
	if code >= 0 {
		return code
	}
	diskID, ok := opts.diskID(stderr, mergeHelp)
	if !ok {
		return 2
	}
	into := opts.values["into"]
	if !validVersionBranchName(into) {
		fmt.Fprintln(stderr, "错误: --into 必须是有效目标分支名")
		return 2
	}
	continuing, aborting := opts.set["continue"], opts.set["abort"]
	if continuing && aborting || (continuing || aborting) && (opts.set["from"] || opts.set["strategy"]) {
		fmt.Fprintln(stderr, "错误: --continue、--abort 与 --from、--strategy 不能同时使用")
		return 2
	}
	if opts.set["message"] && !continuing {
		fmt.Fprintln(stderr, "错误: --message 只能与 --continue 一起使用")
		return 2
	}
	strategy := storagev1.ConflictStrategy_CONFLICT_STRATEGY_ABORT_ON_CONFLICT
	if !continuing && !aborting {
		if !validVersionBranchName(opts.values["from"]) {
			fmt.Fprintln(stderr, "错误: --from 必须是有效源分支名")
			return 2
		}
		switch opts.values["strategy"] {
		case "", "abort":
		case "ours":
			strategy = storagev1.ConflictStrategy_CONFLICT_STRATEGY_OURS
		case "theirs":
			strategy = storagev1.ConflictStrategy_CONFLICT_STRATEGY_THEIRS
		case "manual", "materialize":
			strategy = storagev1.ConflictStrategy_CONFLICT_STRATEGY_MATERIALIZE
		default:
			fmt.Fprintf(stderr, "错误: 无效的冲突策略 %q\n", opts.values["strategy"])
			return 2
		}
	}
	client, code, err := newDiskStorageClient(opts.values["endpoint"], opts.values["token"])
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if continuing {
		resp, err := client.ResolveMerge(ctx, connect.NewRequest(&storagev1.ResolveMergeRequest{DiskId: diskID, BranchName: into, Message: opts.values["message"]}))
		if err != nil {
			fmt.Fprintf(stderr, "错误: 完成合并失败: %v\n", err)
			return 1
		}
		commit := resp.Msg.GetCommit()
		if commit == nil || commit.GetCommitId() == "" {
			fmt.Fprintln(stderr, "错误: 服务端返回空合并版本")
			return 1
		}
		if opts.set["json"] {
			return writeVersionProtoJSON(stdout, stderr, commit)
		}
		fmt.Fprintf(stdout, "合并已完成\nMerge Commit ID: %s\n", commit.GetCommitId())
		return 0
	}
	if aborting {
		_, err := client.AbortMerge(ctx, connect.NewRequest(&storagev1.AbortMergeRequest{DiskId: diskID, BranchName: into}))
		if err != nil {
			fmt.Fprintf(stderr, "错误: 中止合并失败: %v\n", err)
			return 1
		}
		if opts.set["json"] {
			return writeVersionJSON(stdout, stderr, map[string]any{"disk_id": diskID, "branch": into, "aborted": true})
		}
		fmt.Fprintf(stdout, "已中止分支 %s 的合并\n", into)
		return 0
	}
	resp, err := client.Merge(ctx, connect.NewRequest(&storagev1.MergeRequest{
		DiskId: diskID, SourceBranch: opts.values["from"], TargetBranch: into, Strategy: strategy,
	}))
	if err != nil {
		fmt.Fprintf(stderr, "错误: 合并分支失败: %v\n", err)
		return 1
	}
	result := resp.Msg.GetResult()
	if result != storagev1.MergeResult_MERGE_RESULT_MERGED && result != storagev1.MergeResult_MERGE_RESULT_CONFLICTS && result != storagev1.MergeResult_MERGE_RESULT_MATERIALIZED {
		fmt.Fprintf(stderr, "错误: 服务端返回未知合并结果: %s\n", result)
		return 1
	}
	if result == storagev1.MergeResult_MERGE_RESULT_MERGED && resp.Msg.GetMergeCommitId() == "" {
		fmt.Fprintln(stderr, "错误: 服务端未返回合并版本 ID")
		return 1
	}
	if opts.set["json"] {
		if code := writeVersionProtoJSON(stdout, stderr, resp.Msg); code != 0 {
			return code
		}
	} else {
		switch result {
		case storagev1.MergeResult_MERGE_RESULT_MERGED:
			fmt.Fprintf(stdout, "合并成功\nMerge Commit ID: %s\n", resp.Msg.GetMergeCommitId())
		case storagev1.MergeResult_MERGE_RESULT_CONFLICTS:
			fmt.Fprintln(stdout, "合并存在冲突:")
		case storagev1.MergeResult_MERGE_RESULT_MATERIALIZED:
			fmt.Fprintln(stdout, "冲突已物化至目标分支:")
		}
		for _, conflict := range resp.Msg.GetConflicts() {
			fmt.Fprintf(stdout, "Path: %s\nReason: %s\n", conflict.GetPath(), conflict.GetReason())
			for _, item := range []struct {
				name string
				attr *storagev1.InodeAttribute
			}{{"Base", conflict.GetBaseAttr()}, {"Ours", conflict.GetOursAttr()}, {"Theirs", conflict.GetTheirsAttr()}} {
				value := "不存在"
				if item.attr != nil {
					data, err := (protojson.MarshalOptions{UseProtoNames: true, EmitUnpopulated: true}).Marshal(item.attr)
					if err != nil {
						fmt.Fprintf(stderr, "错误: 序列化冲突属性失败: %v\n", err)
						return 1
					}
					value = string(data)
				}
				fmt.Fprintf(stdout, "%s: %s\n", item.name, value)
			}
		}
		if result == storagev1.MergeResult_MERGE_RESULT_MATERIALIZED {
			fmt.Fprintf(stdout, "解决冲突后运行 bowl merge %s --into %s --continue，或使用 --abort 中止合并。\n", diskID, into)
		}
	}
	if result == storagev1.MergeResult_MERGE_RESULT_CONFLICTS {
		return 1
	}
	return 0
}
