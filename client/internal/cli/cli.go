package cli

import (
	"fmt"
	"io"

	"github.com/starsrc/bowl/client/internal/version"
)

const usageHelp = `bowl - 分布式虚拟磁盘 Linux FUSE 客户端

用法:
  bowl <command> [flags] [arguments]

可用命令:
  disk        管理虚拟磁盘生命周期 (创建、列表、详情、更新/扩缩容、删除)
  branch      管理虚拟磁盘逻辑分支 (create、ls、rm)
  commit      定格分支版本快照
  rollback    回档分支至历史快照
  merge       合并分支或完成、终止冲突合并
  diff        比较分支或版本快照
  mount       挂载远端虚拟磁盘至本地目录
  umount      卸载本地已挂载的虚拟磁盘 (别名: unmount)
  version     打印客户端版本信息
  help        查看命令帮助

全局标志:
  -h, --help      查看帮助信息
  -v, --version   查看版本信息
`

// Execute 执行 bowl CLI 命令行。
// 参数 args 为命令行入参列表（不含可执行文件路径）。
// 返回进程退出码 (0 表示成功，非 0 表示异常或参数错误)。
func Execute(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usageHelp)
		return 2
	}

	cmd := args[0]
	switch cmd {
	case "version", "--version", "-v":
		fmt.Fprintf(stdout, "bowl version %s\n", version.Version)
		return 0
	case "help", "--help", "-h":
		fmt.Fprint(stdout, usageHelp)
		return 0
	case "disk":
		return executeDisk(args[1:], stdout, stderr)
	case "branch":
		return executeBranch(args[1:], stdout, stderr)
	case "commit":
		return executeCommit(args[1:], stdout, stderr)
	case "rollback":
		return executeRollback(args[1:], stdout, stderr)
	case "merge":
		return executeMerge(args[1:], stdout, stderr)
	case "diff":
		return executeDiff(args[1:], stdout, stderr)
	case "mount":
		return executeMount(args[1:], stdout, stderr)
	case "umount", "unmount":
		return executeUmount(args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "错误: 未知命令 \"%s\"\n\n%s", cmd, usageHelp)
		return 2
	}
}
