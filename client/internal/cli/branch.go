package cli

import (
	"context"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	"connectrpc.com/connect"

	storagev1 "github.com/starsrc/bowl/gen/go/storage/v1"
)

const branchHelp = `用法:
  bowl branch create <disk-id> --name <分支> [--from <源分支>[@<commit-id>]] [--commit <commit-id>] [--json] [--endpoint <url>] [--token <token>]
  bowl branch ls <disk-id> [--json] [--endpoint <url>] [--token <token>]
  bowl branch rm <disk-id> --name <分支> [--json] [--endpoint <url>] [--token <token>]

ls 的别名为 list；rm 的别名为 delete。
`

type branchJSON struct {
	BranchID     string `json:"branch_id"`
	DiskID       string `json:"disk_id"`
	Name         string `json:"name"`
	ParentBranch string `json:"parent_branch,omitempty"`
	ForkCommitID string `json:"fork_commit_id,omitempty"`
	HeadCommitID string `json:"head_commit_id,omitempty"`
	State        string `json:"state"`
	CreatedAt    string `json:"created_at"`
}

func toBranchJSON(branch *storagev1.Branch) branchJSON {
	var createdAt string
	if branch.GetCreatedAt() != nil {
		createdAt = branch.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}
	return branchJSON{
		BranchID: branch.GetBranchId(), DiskID: branch.GetDiskId(), Name: branch.GetName(),
		ParentBranch: branch.GetParentBranch(), ForkCommitID: branch.GetForkCommitId(),
		HeadCommitID: branch.GetHeadCommitId(),
		State:        strings.TrimPrefix(branch.GetState().String(), "BRANCH_STATE_"),
		CreatedAt:    createdAt,
	}
}

func executeBranch(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, branchHelp)
		return 2
	}
	subcommand := args[0]
	if subcommand == "help" || subcommand == "--help" || subcommand == "-h" {
		fmt.Fprint(stdout, branchHelp)
		return 0
	}
	var flags map[string]bool
	switch subcommand {
	case "create":
		flags = map[string]bool{"name": true, "from": true, "commit": true}
	case "ls", "list":
		flags = map[string]bool{}
	case "rm", "delete":
		flags = map[string]bool{"name": true}
	default:
		fmt.Fprintf(stderr, "错误: 未知 branch 子命令 %q\n%s", subcommand, branchHelp)
		return 2
	}
	opts, code := parseVersionArgs(args[1:], flags, branchHelp, stdout, stderr)
	if code >= 0 {
		return code
	}
	diskID, ok := opts.diskID(stderr, branchHelp)
	if !ok {
		return 2
	}
	if subcommand == "create" || subcommand == "rm" || subcommand == "delete" {
		if !validVersionBranchName(opts.values["name"]) {
			fmt.Fprintln(stderr, "错误: --name 必须是有效分支名")
			return 2
		}
	}
	var sourceBranch, sourceCommit *string
	if subcommand == "create" {
		if opts.set["from"] {
			from := opts.values["from"]
			branch, commit, hasCommit := strings.Cut(from, "@")
			if hasCommit {
				if opts.set["commit"] {
					fmt.Fprintln(stderr, "错误: --from 中的提交和 --commit 不能同时指定")
					return 2
				}
				commit, ok = parseVersionUUID(commit)
				if !ok {
					fmt.Fprintln(stderr, "错误: 源 commit-id 必须是非零 UUID")
					return 2
				}
				sourceCommit = &commit
			} else if branch == "" {
				fmt.Fprintln(stderr, "错误: --from 不能为空")
				return 2
			}
			if branch != "" {
				if !validVersionBranchName(branch) {
					fmt.Fprintln(stderr, "错误: 源分支名无效")
					return 2
				}
				sourceBranch = &branch
			}
		}
		if opts.set["commit"] {
			commit := opts.values["commit"]
			commit, ok = parseVersionUUID(commit)
			if !ok {
				fmt.Fprintln(stderr, "错误: --commit 必须是非零 UUID")
				return 2
			}
			sourceCommit = &commit
		}
	}
	client, code, err := newDiskStorageClient(opts.values["endpoint"], opts.values["token"])
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return code
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	switch subcommand {
	case "create":
		resp, err := client.CreateBranch(ctx, connect.NewRequest(&storagev1.CreateBranchRequest{
			DiskId: diskID, Name: opts.values["name"], SourceBranch: sourceBranch, SourceCommitId: sourceCommit,
		}))
		if err != nil {
			fmt.Fprintf(stderr, "错误: 创建分支失败: %v\n", err)
			return 1
		}
		if resp.Msg.GetBranch() == nil {
			fmt.Fprintln(stderr, "错误: 服务端返回空分支")
			return 1
		}
		branch := toBranchJSON(resp.Msg.GetBranch())
		if opts.set["json"] {
			return writeVersionJSON(stdout, stderr, branch)
		}
		fmt.Fprintf(stdout, "Branch ID:     %s\nName:          %s\nParent Branch: %s\nFork Commit:   %s\nHead Commit:   %s\nState:         %s\nCreated At:    %s\n", branch.BranchID, branch.Name, branch.ParentBranch, branch.ForkCommitID, branch.HeadCommitID, branch.State, branch.CreatedAt)
	case "ls", "list":
		resp, err := client.ListBranches(ctx, connect.NewRequest(&storagev1.ListBranchesRequest{DiskId: diskID}))
		if err != nil {
			fmt.Fprintf(stderr, "错误: 列出分支失败: %v\n", err)
			return 1
		}
		branches := make([]branchJSON, 0, len(resp.Msg.GetBranches()))
		for _, branch := range resp.Msg.GetBranches() {
			branches = append(branches, toBranchJSON(branch))
		}
		if opts.set["json"] {
			return writeVersionJSON(stdout, stderr, struct {
				Branches []branchJSON `json:"branches"`
			}{Branches: branches})
		}
		tw := tabwriter.NewWriter(stdout, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "NAME\tPARENT\tHEAD COMMIT\tSTATE\tCREATED AT")
		for _, branch := range branches {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\n", branch.Name, branch.ParentBranch, branch.HeadCommitID, branch.State, branch.CreatedAt)
		}
		_ = tw.Flush()
	case "rm", "delete":
		_, err := client.DeleteBranch(ctx, connect.NewRequest(&storagev1.DeleteBranchRequest{DiskId: diskID, Name: opts.values["name"]}))
		if err != nil {
			if connect.CodeOf(err) == connect.CodeFailedPrecondition {
				fmt.Fprintf(stderr, "错误: 分支存在活跃会话 (EBUSY)，无法删除: %v\n", err)
			} else {
				fmt.Fprintf(stderr, "错误: 删除分支失败: %v\n", err)
			}
			return 1
		}
		if opts.set["json"] {
			return writeVersionJSON(stdout, stderr, map[string]any{"disk_id": diskID, "name": opts.values["name"], "deleted": true})
		}
		fmt.Fprintf(stdout, "已删除分支 %s\n", opts.values["name"])
	}
	return 0
}
