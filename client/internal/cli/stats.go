package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"
	"time"
)

const statsUsageHelp = `用法:
  bowl stats [mountpoint] [flags]

参数:
  [mountpoint]    已挂载目录（可选；本机仅一个活跃挂载时自动发现）

标志:
  --json                 输出 JSON 统计快照
  --raw                  输出 .stats 兼容键值格式
  --admin-addr string    直接指定挂载进程管理面地址 (host:port)，跳过自动发现
  -h, --help             查看帮助信息
`

const statsRequestTimeout = 5 * time.Second

var statsHTTPClient = http.DefaultClient

// executeStats 解析 stats 命令参数，定位挂载进程管理面并输出 /debug/stats 统计视图。
func executeStats(args []string, stdout, stderr io.Writer) int {
	var (
		asJSON    bool
		raw       bool
		help      bool
		adminAddr string
		posArgs   []string
	)
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			posArgs = append(posArgs, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") {
			posArgs = append(posArgs, arg)
			continue
		}
		name, val, hasVal := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		switch name {
		case "h", "help":
			help = true
		case "json":
			asJSON = true
		case "raw":
			raw = true
		case "admin-addr":
			v, err := takeFlagValue(args, &i, name, val, hasVal)
			if err != nil {
				fmt.Fprintf(stderr, "错误: %v\n", err)
				return 2
			}
			adminAddr = v
		default:
			fmt.Fprintf(stderr, "错误: 未知标志 \"%s\"\n\n%s", arg, statsUsageHelp)
			return 2
		}
	}

	if help {
		fmt.Fprint(stdout, statsUsageHelp)
		return 0
	}
	if len(posArgs) > 1 {
		fmt.Fprintf(stderr, "错误: stats 命令最多接受 1 个位置参数 [mountpoint]\n\n%s", statsUsageHelp)
		return 2
	}
	if asJSON && raw {
		fmt.Fprintln(stderr, "错误: --json 与 --raw 不能同时指定")
		return 2
	}

	if adminAddr != "" {
		normalized, err := normalizeStatsAdminAddr(adminAddr)
		if err != nil {
			fmt.Fprintf(stderr, "错误: %v\n", err)
			return 2
		}
		adminAddr = normalized
	} else {
		addr, err := discoverAdminAddr(posArgs)
		if err != nil {
			fmt.Fprintf(stderr, "错误: %v\n", err)
			return 1
		}
		adminAddr = addr
	}

	format := "text"
	switch {
	case asJSON:
		format = "json"
	case raw:
		format = "stats"
	}

	body, err := fetchStats(adminAddr, format)
	if err != nil {
		fmt.Fprintf(stderr, "错误: %v\n", err)
		return 1
	}
	_, _ = stdout.Write(body)
	if len(body) > 0 && body[len(body)-1] != '\n' {
		fmt.Fprintln(stdout)
	}
	return 0
}

func normalizeStatsAdminAddr(raw string) (string, error) {
	trimmed := strings.TrimSpace(raw)
	trimmed = strings.TrimPrefix(trimmed, "http://")
	trimmed = strings.TrimSuffix(trimmed, "/")
	if trimmed == "" || strings.EqualFold(trimmed, adminAddrOff) {
		return "", fmt.Errorf("无效的 --admin-addr \"%s\"", raw)
	}
	if _, _, err := net.SplitHostPort(trimmed); err != nil {
		return "", fmt.Errorf("无效的 --admin-addr \"%s\": %w", raw, err)
	}
	return trimmed, nil
}

// discoverAdminAddr 依据挂载点（或唯一活跃挂载）解析管理面地址。
func discoverAdminAddr(posArgs []string) (string, error) {
	var meta mountMeta
	if len(posArgs) == 1 {
		absMP, err := filepath.Abs(posArgs[0])
		if err != nil {
			return "", fmt.Errorf("挂载点路径解析失败: %w", err)
		}
		m, ok := findMountMeta(absMP)
		if !ok {
			return "", fmt.Errorf("未找到挂载点 \"%s\" 的活跃 bowl 挂载进程", filepath.Clean(absMP))
		}
		meta = m
	} else {
		metas := listMountMetas()
		switch len(metas) {
		case 0:
			return "", errors.New("未发现活跃的 bowl 挂载")
		case 1:
			meta = metas[0]
		default:
			mps := make([]string, 0, len(metas))
			for i := range metas {
				mps = append(mps, metas[i].Mountpoint)
			}
			return "", fmt.Errorf("发现多个活跃挂载，请指定挂载点: %s", strings.Join(mps, ", "))
		}
	}
	if meta.AdminAddr == "" || strings.EqualFold(meta.AdminAddr, adminAddrOff) {
		return "", fmt.Errorf("挂载点 \"%s\" 未开启管理面 (--admin-addr off)", meta.Mountpoint)
	}
	return meta.AdminAddr, nil
}

// fetchStats 请求 http://<addr>/debug/stats?format=<format>。
func fetchStats(addr, format string) ([]byte, error) {
	u := url.URL{Scheme: "http", Host: addr, Path: "/debug/stats", RawQuery: url.Values{"format": {format}}.Encode()}
	ctx, cancel := context.WithTimeout(context.Background(), statsRequestTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("构造统计请求失败: %w", err)
	}
	resp, err := statsHTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("请求管理面 %s 失败: %w", addr, err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取统计响应失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("管理面返回 %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return body, nil
}
