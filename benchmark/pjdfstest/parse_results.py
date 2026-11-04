#!/usr/bin/env python3
"""Parse pjdfstest TAP output, directory timings, and cloud metadata into summary.json and REPORT.md."""

from __future__ import annotations

import argparse
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


SYSCALL_DIR_DESCRIPTIONS: dict[str, str] = {
    "chflags": "文件扩展标志位（FS_IOC_GETFLAGS / FS_IOC_SETFLAGS，验证 immutable / append 约束）",
    "chmod": "权限位变更、SUID/SGID 粘滞位保留与剥离、多身份权限检查",
    "chown": "文件与符号链接属主/属组变更（chown/lchown/fchown）、非特权组约束与 SUID/SGID 清除",
    "ftruncate": "基于已打开文件描述符的长度截断与扩展、只读句柄拒绝（EBADF/EINVAL）",
    "granular": "细粒度访问控制与基础文件操作边界校验",
    "link": "硬链接创建、nlink 计数递增、跨目录硬链接与目录硬链接禁止（EPERM）",
    "mkdir": "目录创建、权限掩码、父目录写/执行权限校验、路径长度与存在性冲突（EEXIST）",
    "mkfifo": "命名管道（FIFO 特殊文件）创建与权限位验证",
    "mknod": "字符设备、块设备、FIFO 与套接字特殊节点创建及权限验证",
    "open": "文件打开标志组合（O_CREAT/O_EXCL/O_TRUNC/O_RDONLY/O_WRONLY/O_RDWR/O_NOFOLLOW）与权限矩阵",
    "posix_fallocate": "物理空间预分配（fallocate Mode=0）、文件大小扩展与配额联动",
    "rename": "同目录/跨目录原子重命名、覆盖非空目录拦截（ENOTEMPTY/EISDIR/ENOTDIR）及权限矩阵",
    "rmdir": "空目录删除、非空目录拒绝（ENOTEMPTY）、当前目录/点目录拒绝与权限检查",
    "symlink": "符号链接创建、目标路径长度边界与 readlink 解析",
    "truncate": "基于路径的文件截断、稀疏扩展零填充、尺寸变更触发 ctime/mtime 更新",
    "unlink": "文件与特殊节点删除、nlink 递减、Open-Unlink 孤儿句柄读写保活与延迟回收",
    "utimensat": "纳秒级时间戳更新（UTIME_NOW / UTIME_OMIT / 显式时间戳）与权限校验",
}


def parse_tap_log(log_text: str, dir_timings: list[dict[str, Any]]) -> dict[str, Any]:
    """Parse prove -rv --timer output into per-file and per-directory results."""
    lines = log_text.splitlines()

    # Track per-file results in order
    # Header line example with or without --timer:
    # [14:50:12] /opt/pjdfstest/tests/chflags/00.t ..
    # or: /opt/pjdfstest/tests/chflags/00.t ..
    header_re = re.compile(
        r"^(?:\[\d{2}:\d{2}:\d{2}\]\s+)?(?:\S*/tests/)?(?P<rel>[a-zA-Z0-9_]+/[0-9a-zA-Z_.-]+\.t)\s+\.\.\s*$"
    )
    plan_re = re.compile(r"^1\.\.(?P<plan>\d+)")
    ok_line_re = re.compile(r"^ok\s+(?P<num>\d+)\b(?P<rest>.*)$")
    not_ok_line_re = re.compile(r"^not ok\s+(?P<num>\d+)\b(?P<rest>.*)$")
    done_timer_re = re.compile(r"^ok\s+(?P<ms>\d+)\s+ms\b")
    dubious_re = re.compile(r"^Dubious,\s+test returned\s+(?P<rc>\d+)")
    failed_subtests_re = re.compile(r"^Failed\s+(?P<failed>\d+)/(?P<total>\d+)\s+subtests")

    cases: dict[str, dict[str, Any]] = {}
    current_rel: str | None = None

    for raw_line in lines:
        line = raw_line.strip()
        m_hdr = header_re.match(line)
        if m_hdr:
            current_rel = m_hdr.group("rel")
            dir_name = current_rel.split("/", 1)[0]
            cases[current_rel] = {
                "case": current_rel,
                "directory": dir_name,
                "planned": 0,
                "ok_count": 0,
                "not_ok_count": 0,
                "todo_count": 0,
                "todo_expected_failures": 0,
                "duration_ms": None,
                "failed_nums": [],
                "dubious_rc": 0,
            }
            continue

        if current_rel is None:
            continue

        entry = cases[current_rel]

        m_plan = plan_re.match(line)
        if m_plan:
            entry["planned"] = int(m_plan.group("plan"))
            continue

        m_done = done_timer_re.match(line)
        if m_done:
            entry["duration_ms"] = int(m_done.group("ms"))
            current_rel = None
            continue

        if line == "ok":
            current_rel = None
            continue

        m_ok = ok_line_re.match(line)
        if m_ok:
            entry["ok_count"] += 1
            if "# TODO" in m_ok.group("rest"):
                entry["todo_count"] += 1
            continue

        m_not_ok = not_ok_line_re.match(line)
        if m_not_ok:
            rest = m_not_ok.group("rest")
            if "# TODO" in rest:
                entry["ok_count"] += 1
                entry["todo_count"] += 1
                entry["todo_expected_failures"] += 1
            else:
                entry["not_ok_count"] += 1
                entry["failed_nums"].append(int(m_not_ok.group("num")))
            continue

        m_dub = dubious_re.match(line)
        if m_dub:
            entry["dubious_rc"] = int(m_dub.group("rc"))
            continue

        m_fsub = failed_subtests_re.match(line)
        if m_fsub:
            failed_n = int(m_fsub.group("failed"))
            total_n = int(m_fsub.group("total"))
            if entry["planned"] == 0:
                entry["planned"] = total_n
            if entry["not_ok_count"] < failed_n:
                entry["not_ok_count"] = failed_n
            current_rel = None
            continue

    # Finalize per-case counts
    case_list: list[dict[str, Any]] = []
    for rel in sorted(cases.keys()):
        c = cases[rel]
        total = c["ok_count"] + c["not_ok_count"]
        if total == 0 and c["planned"] > 0:
            total = c["planned"]
        passed = max(total - c["not_ok_count"], 0)
        failed = c["not_ok_count"]
        if c["dubious_rc"] != 0 and failed == 0:
            failed = 1
            if total == 0:
                total = 1
            passed = max(total - failed, 0)
        case_list.append({
            "case": rel,
            "directory": c["directory"],
            "tests": total,
            "passed": passed,
            "failed": failed,
            "todo": c["todo_count"],
            "todo_expected_failures": c["todo_expected_failures"],
            "duration_ms": c["duration_ms"],
            "failed_nums": c["failed_nums"],
            "status": "PASS" if failed == 0 and total > 0 else "FAIL",
        })

    # Map dir_timings by directory name
    timing_by_dir: dict[str, dict[str, Any]] = {
        item["directory"]: item for item in dir_timings if "directory" in item
    }

    # Aggregate by directory
    dirs_map: dict[str, dict[str, Any]] = {}
    for c in case_list:
        d = c["directory"]
        if d not in dirs_map:
            dirs_map[d] = {
                "directory": d,
                "files": 0,
                "tests": 0,
                "passed": 0,
                "failed": 0,
                "duration_sec": 0.0,
                "rc": 0,
            }
        dirs_map[d]["files"] += 1
        dirs_map[d]["tests"] += c["tests"]
        dirs_map[d]["passed"] += c["passed"]
        dirs_map[d]["failed"] += c["failed"]

    # 即使目录没有输出任何 TAP 用例，也要保留其退出码，避免报告漏掉失败目录。
    for d in timing_by_dir:
        dirs_map.setdefault(d, {
            "directory": d,
            "files": 0,
            "tests": 0,
            "passed": 0,
            "failed": 0,
            "duration_sec": 0.0,
            "rc": 0,
        })

    for d, info in dirs_map.items():
        if d in timing_by_dir:
            info["duration_sec"] = round(float(timing_by_dir[d].get("duration_sec", 0.0)), 3)
            info["rc"] = int(timing_by_dir[d].get("rc", 0))
        else:
            ms_sum = sum((c["duration_ms"] or 0) for c in case_list if c["directory"] == d)
            info["duration_sec"] = round(ms_sum / 1000.0, 3)

    dir_list = [dirs_map[k] for k in sorted(dirs_map.keys())]
    total_files = sum(d["files"] for d in dir_list)
    total_tests = sum(d["tests"] for d in dir_list)
    total_passed = sum(d["passed"] for d in dir_list)
    total_failed = sum(d["failed"] for d in dir_list)
    total_duration_sec = round(sum(d["duration_sec"] for d in dir_list), 3)

    failed_cases = [c for c in case_list if c["failed"] > 0]

    return {
        "schema": "bowl-pjdfstest-report/v1",
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "total_directories": len(dir_list),
        "total_files": total_files,
        "total_tests": total_tests,
        "passed_tests": total_passed,
        "failed_tests": total_failed,
        "todo_tests": sum(c["todo"] for c in case_list),
        "todo_expected_failures": sum(c["todo_expected_failures"] for c in case_list),
        "pass_rate": round(total_passed / total_tests, 6) if total_tests > 0 else 0.0,
        "total_duration_sec": total_duration_sec,
        "overall_throughput_tests_per_sec": round(total_tests / total_duration_sec, 2)
        if total_duration_sec > 0
        else 0.0,
        "directories": dir_list,
        "failed_cases": failed_cases,
        "cases": case_list,
    }


def render_report_md(summary: dict[str, Any], env_meta: dict[str, Any]) -> str:
    """Render comprehensive Markdown benchmark & compatibility report."""
    total_dirs = summary["total_directories"]
    total_files = summary["total_files"]
    total_tests = summary["total_tests"]
    passed_tests = summary["passed_tests"]
    failed_tests = summary["failed_tests"]
    pass_rate_pct = summary["pass_rate"] * 100.0
    total_dur = summary["total_duration_sec"]
    throughput = summary["overall_throughput_tests_per_sec"]

    cloud = env_meta.get("cloud", {})
    server = env_meta.get("server_node", {})
    client = env_meta.get("client_node", {})
    net = env_meta.get("network_baseline", {})
    bowl_cfg = env_meta.get("bowl_config", {})
    audit = env_meta.get("cleanup_audit", {})

    directories_ok = all(d["rc"] == 0 and d["files"] > 0 for d in summary["directories"])
    status_badge = "100% PASS (全量通过)" if failed_tests == 0 and total_tests > 0 and directories_ok else f"PASS {passed_tests}/{total_tests} ({pass_rate_pct:.2f}%)；目录退出码见明细"

    lines: list[str] = []
    lines.append("# Bowl 分布式文件系统 — 阿里云硅谷双机内网 `pjdfstest` 基准测试报告")
    lines.append("")
    lines.append("被测版本采用 ADR-0029：PostgreSQL 仅保存 `disks`、`commits` 和 `client_sessions` 控制面表；每个分支独立使用 Pebble 保存 Inode、Entry、Xattr、Orphan 和 4 MiB 数据块，整块写入与局部补丁分别使用 Set 和 ValueMerger。Checkpoint 和 Fork 通过硬链接共享文件。客户端在本地维护 FileHandle、FileLock、O_TMPFILE 和按需加载的 NamespaceCache。")
    lines.append("")
    lines.append("当前提交禁用 FUSE 内核 `writeback_cache`；Flush 和 Fsync 请求服务端同步，Release 负责释放本地句柄。此处记录实际配置，与 ADR-0029 的 writeback_cache 设计不同。")
    lines.append("")
    lines.append("## 1. 执行摘要（Executive Summary）")
    lines.append("")
    lines.append(
        "本报告记录了 **Bowl** 在阿里云硅谷地域（`us-west-1`）真实开启 2 台按量计费云服务器（1 台 `storage-service` + PostgreSQL 服务端节点，1 台 Linux FUSE 客户端节点），"
        "在同可用区 VPC 私网下运行权威 POSIX 文件系统兼容性与基准测试集 **[pjdfstest](https://github.com/pjd/pjdfstest)** 的完整实测结果。"
    )
    lines.append("")
    lines.append("| 核心指标 | 实测结果 |")
    lines.append("| :--- | :--- |")
    lines.append(f"| **测试结论** | **{status_badge}** |")
    lines.append(f"| **测试系统调用目录数** | `{total_dirs}` 个系统调用族 |")
    lines.append(f"| **测试脚本文件数 (`.t`)** | `{total_files}` 个测试脚本 |")
    lines.append(f"| **总测试断言数 (Subtests)** | `{total_tests:,}` 项 POSIX 断言 |")
    lines.append(f"| **通过 / 失败断言数** | `{passed_tests:,}` 通过 / `{failed_tests}` 失败（通过率 **{pass_rate_pct:.2f}%**） |")
    lines.append(f"| **TAP TODO** | `{summary['todo_tests']}` 项，其中 `{summary['todo_expected_failures']}` 项为预期失败；按 `prove` 规则不计入失败 |")
    lines.append(f"| **全量执行总耗时** | `{total_dur:.2f} s`（约 `{total_dur / 60.0:.2f} min`） |")
    lines.append(f"| **内网综合执行吞吐率** | `{throughput:.2f} tests/s`（平均单断言耗时 `{1000.0 / throughput if throughput else 0:.2f} ms`） |")
    lines.append(f"| **云端地域与网络拓扑** | 阿里云 `{cloud.get('region_id', 'us-west-1')}` (`{cloud.get('zone_id', 'us-west-1a')}`) 同可用区 VPC 私网直连 |")
    lines.append(f"| **测试执行时间 (UTC)** | `{env_meta.get('test_started_at', summary['generated_at'])}` ~ `{env_meta.get('test_completed_at', summary['generated_at'])}` |")
    lines.append("")

    # Section 2: Cloud Topology & Environment
    lines.append("---")
    lines.append("")
    lines.append("## 2. 阿里云硅谷双机测试拓扑与环境基线")
    lines.append("")
    lines.append("### 2.1 双机云服务器配置（按量计费实测节点）")
    lines.append("")
    lines.append("| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |")
    lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    lines.append(
        f"| **Server 服务端** (`bowl-bench-server`) | `{server.get('instance_id', '-')}` | `{cloud.get('instance_type', '-')}` | "
        f"{server.get('cpu_model', '-')} | {server.get('vcpu', '-')} vCPU / {server.get('memory_gib', '-')} | "
        f"`{cloud.get('system_disk_category', 'cloud_essd')}` ({cloud.get('system_disk_size_gib', 40)} GiB) | "
        f"{server.get('os_release', '-')} (`{server.get('kernel', '-')}`) | `{server.get('private_ip', '-')}:9000` |"
    )
    lines.append(
        f"| **Client 客户端** (`bowl-bench-client`) | `{client.get('instance_id', '-')}` | `{cloud.get('instance_type', '-')}` | "
        f"{client.get('cpu_model', '-')} | {client.get('vcpu', '-')} vCPU / {client.get('memory_gib', '-')} | "
        f"`{cloud.get('system_disk_category', 'cloud_essd')}` ({cloud.get('system_disk_size_gib', 40)} GiB) | "
        f"{client.get('os_release', '-')} (`{client.get('kernel', '-')}`) | `{client.get('private_ip', '-')}` |"
    )
    lines.append("")

    lines.append("### 2.2 同可用区 VPC 内网性能基线（Client <-> Server）")
    lines.append("")
    lines.append("| 网络指标 | 实测数值 | 采集方式 |")
    lines.append("| :--- | :--- | :--- |")
    lines.append(f"| **VPC ID / 交换机 ID** | `{cloud.get('vpc_id', '-')}` / `{cloud.get('vswitch_id', '-')}` | 阿里云 `{cloud.get('zone_id', 'us-west-1a')}` 私网网段 `{cloud.get('cidr_block', '172.16.1.0/24')}` |")
    lines.append(f"| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `{net.get('ping_rtt_ms', '-')}` ms | `ping -c 20 -i 0.05 {server.get('private_ip', '')}` |")
    lines.append(f"| **内网单流 TCP 吞吐带宽** | `{net.get('iperf3_gbps', '-')}` Gbps | `iperf3 -c {server.get('private_ip', '')} -t 5` |")
    lines.append("")

    lines.append("### 2.3 软件版本与挂载运行参数")
    lines.append("")
    lines.append("| 组件 / 参数项 | 配置值 | 说明 |")
    lines.append("| :--- | :--- | :--- |")
    lines.append(f"| **Bowl Git Commit** | `{bowl_cfg.get('bowl_git_commit', '-')}` | 仓库当前评测版本 |")
    lines.append(f"| **pjdfstest Git Commit** | `{bowl_cfg.get('pjdfstest_git_commit', '-')}` | 上游 `https://github.com/pjd/pjdfstest` |")
    lines.append(f"| **PostgreSQL 版本** | `{server.get('pg_version', '-')}` | Server 节点控制面数据库 |")
    lines.append(f"| **FUSE 用户态版本** | `{client.get('fusermount_version', '-')}` | Linux 内核 FUSE 协议 + `github.com/hanwen/go-fuse/v2` |")
    lines.append(f"| **测试虚拟磁盘 (`DiskID` / `RootIno`)** | `{bowl_cfg.get('disk_id', '-')}` (`root_ino={bowl_cfg.get('root_ino', '-')}`) | 配额 `{bowl_cfg.get('disk_size', '-')}` |")
    lines.append(f"| **服务端 `STORAGE_ATIME_MODE`** | `{bowl_cfg.get('atime_mode', 'strictatime')}` | 开启严格访问时间更新，验证 `atime` 刷新语义 |")
    lines.append(f"| **FUSE 缓存超时配置** | `entry={bowl_cfg.get('entry_timeout', '0')}s, attr={bowl_cfg.get('attr_timeout', '0')}s, direntry={bowl_cfg.get('dir_entry_timeout', '0')}s` | 内核属性与目录项缓存超时；客户端 NamespaceCache 仍可命中 |")
    lines.append(f"| **客户端挂载命令** | `bowl mount <disk_id> /mnt/bowl --endpoint http://{server.get('private_ip', '<server_ip>')}:9000 --allow-other` | 启用 `default_permissions` 与 `--allow-other` 支持多 UID/GID 权限切换测试 |")
    lines.append("")

    # Section 3: Summary by Directory
    lines.append("---")
    lines.append("")
    lines.append("## 3. 17 个系统调用目录兼容性与耗时汇总")
    lines.append("")
    lines.append("| 系统调用目录 | 脚本数 (Files) | 断言数 (Tests) | PASS / ALL | 退出码 | 通过率 | 耗时 (s) | 吞吐率 (Tests/s) | 耗时占比 | 核心覆盖语义 |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |")

    for d in summary["directories"]:
        dname = d["directory"]
        files = d["files"]
        tests = d["tests"]
        passed = d["passed"]
        dur = d["duration_sec"]
        rate = (passed / tests * 100.0) if tests > 0 else 0.0
        tput = (tests / dur) if dur > 0 else 0.0
        share = (dur / total_dur * 100.0) if total_dur > 0 else 0.0
        desc = SYSCALL_DIR_DESCRIPTIONS.get(dname, "-")
        lines.append(
            f"| `{dname}/` | {files} | {tests:,} | {passed}/{tests} | {d['rc']} | {rate:.2f}% | {dur:.2f}s | {tput:.1f} | {share:.1f}% | {desc} |"
        )

    lines.append(
        f"| **Total (合计)** | **{total_files}** | **{total_tests:,}** | **{passed_tests}/{total_tests}** | **{'全部为 0' if directories_ok else '见各目录'}** | **{pass_rate_pct:.2f}%** | **{total_dur:.2f}s** | **{throughput:.1f}** | **100.0%** | **{total_dirs} 个系统调用目录** |"
    )
    lines.append("")

    # Section 4: Performance & Semantic Analysis
    lines.append("---")
    lines.append("")
    lines.append("## 4. 内网性能特征与关键 POSIX 语义验证分析")
    lines.append("")
    lines.append("### 4.1 跨机内网 RPC 延迟与吞吐分布剖析")
    lines.append("")

    # Sort directories by duration descending for top bottleneck analysis
    top_dirs = sorted(summary["directories"], key=lambda x: x["duration_sec"], reverse=True)[:5]
    lines.append(
        f"- **内网通信底座**：Client 与 Server 位于阿里云硅谷 `us-west-1a` 同一交换机（`{cloud.get('vswitch_id', '-')}`），"
        f"内网 ICMP RTT 延迟仅 `{net.get('ping_rtt_ms', '-')}` ms，内网带宽达 `{net.get('iperf3_gbps', '-')}` Gbps。"
        "内核属性与目录项缓存超时设为 `0s`；客户端 NamespaceCache 仍会影响请求路径。"
    )
    if top_dirs:
        top_summary = "、".join(
            f"`{d['directory']}/`（{d['tests']:,} 项断言，耗时 {d['duration_sec']:.2f}s，占比 {d['duration_sec']/total_dur*100.0 if total_dur else 0:.1f}%）"
            for d in top_dirs[:3]
        )
        lines.append(f"- **耗时集中度分析**：耗时排名前三的子集为 {top_summary}。")
    lines.append("")

    lines.append("### 4.2 关键 POSIX 契约与边界语义验证结论")
    lines.append("")
    lines.append(f"本轮磁盘根 Inode 为 `{bowl_cfg.get('root_ino', '-')}`。用例覆盖标志位、多用户权限、打开后解链和空间预分配；各目录及脚本结果见第 3、5 节。")
    lines.append("")

    if summary["failed_cases"]:
        lines.append("### 4.3 未通过子项")
        lines.append("")
        lines.append("| 用例文件 | 总断言数 | 失败数 | 失败子测试序号 |")
        lines.append("| :--- | ---: | ---: | :--- |")
        for fc in summary["failed_cases"]:
            nums = ", ".join(str(n) for n in fc["failed_nums"][:20])
            if len(fc["failed_nums"]) > 20:
                nums += " ..."
            lines.append(f"| `{fc['case']}` | {fc['tests']} | {fc['failed']} | {nums or '-'} |")
        lines.append("")

    # Section 5: Full 238 Case Breakdown
    lines.append("---")
    lines.append("")
    lines.append(f"## 5. 全量 {total_files} 个测试脚本明细表（Case-by-Case Breakdown）")
    lines.append("")
    lines.append("| 测试脚本 (`Case`) | 所属目录 | 断言数 (`Tests`) | 通过/总数 (`PASS/ALL`) | 耗时 (`ms`) | 状态 |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | :--- |")

    for c in summary["cases"]:
        dur_str = f"{c['duration_ms']:,} ms" if c["duration_ms"] is not None else "-"
        lines.append(
            f"| `{c['case']}` | `{c['directory']}/` | {c['tests']} | {c['passed']}/{c['tests']} | {dur_str} | {c['status']} |"
        )
    lines.append(
        f"| **Total** | **{total_dirs} dirs** | **{total_tests:,}** | **{passed_tests}/{total_tests}** | **{int(total_dur * 1000):,} ms** | **{status_badge}** |"
    )
    lines.append("")

    # Section 6: Cloud Resource Cleanup Audit
    lines.append("---")
    lines.append("")
    lines.append("## 6. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）")
    lines.append("")
    lines.append("| 审计项 | 记录值 |")
    lines.append("| :--- | :--- |")
    lines.append(f"| **临时 SSH 密钥对** | `{audit.get('key_pair_name', '-')}`（已调用 `DeleteKeyPairs` 销毁） |")
    lines.append(f"| **Server 节点实例 ID** | `{server.get('instance_id', '-')}`（已调用 `DeleteInstances --Force true` 销毁） |")
    lines.append(f"| **Client 节点实例 ID** | `{client.get('instance_id', '-')}`（已调用 `DeleteInstances --Force true` 销毁） |")
    lines.append(f"| **实例创建时间 (UTC)** | `{audit.get('instances_created_at', '-')}` |")
    lines.append(f"| **实例销毁时间 (UTC)** | `{audit.get('instances_deleted_at', '-')}` |")
    lines.append(f"| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `{audit.get('remaining_benchmark_instances', 0)}`（存量 VPC/VSwitch/安全组完整保留） |")
    lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse pjdfstest logs and generate REPORT.md")
    parser.add_argument("--log", required=True, type=Path, help="Path to pjdfstest.log")
    parser.add_argument("--timings", required=True, type=Path, help="Path to dir_timings.jsonl")
    parser.add_argument("--env-meta", required=True, type=Path, help="Path to env_metadata.json")
    parser.add_argument("--summary-out", required=True, type=Path, help="Output path for summary.json")
    parser.add_argument("--report-out", required=True, type=Path, help="Output path for REPORT.md")
    args = parser.parse_args()

    log_text = args.log.read_text(encoding="utf-8", errors="replace") if args.log.exists() else ""

    dir_timings: list[dict[str, Any]] = []
    if args.timings.exists():
        for line in args.timings.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                dir_timings.append(json.loads(line))

    env_meta: dict[str, Any] = {}
    if args.env_meta.exists():
        env_meta = json.loads(args.env_meta.read_text(encoding="utf-8"))

    summary = parse_tap_log(log_text, dir_timings)
    summary["environment"] = env_meta

    args.summary_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    report_md = render_report_md(summary, env_meta)
    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    args.report_out.write_text(report_md, encoding="utf-8")

    print(
        f"[parse_results] Parsed {summary['total_files']} files, "
        f"{summary['passed_tests']}/{summary['total_tests']} tests passed "
        f"({summary['pass_rate'] * 100:.2f}%) in {summary['total_duration_sec']:.2f}s"
    )


if __name__ == "__main__":
    main()
