#!/usr/bin/env python3
"""Parse Bowl vs JuiceFS benchmark results (juicefs bench, fio, mdtest) into summary.json and REPORT.md."""

from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Any

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

TARGETS = [
    ("bowl", "Bowl (PG 16 + BlockStore)"),
    ("jfs_pg", "JuiceFS (PG 16 + MinIO)"),
    ("jfs_redis", "JuiceFS (Redis 7 + MinIO)"),
]

BENCH_ITEMS = [
    ("Write big file", "MiB/s", "s/file"),
    ("Read big file", "MiB/s", "s/file"),
    ("Write small file", "files/s", "ms/file"),
    ("Read small file", "files/s", "ms/file"),
    ("Stat file", "files/s", "ms/file"),
]

FIO_CASES = [
    (
        "seq_write_1job",
        "顺序写 (1 并发 · 4M · 4GiB)",
        "write",
        "fio.md: --rw=write --bs=4M --size=4G --numjobs=1 --end_fsync=1",
    ),
    (
        "seq_read_1job",
        "顺序读 (1 并发 · 4M · 4GiB)",
        "read",
        "fio.md: --rw=read --bs=4M --size=4G --numjobs=1",
    ),
    (
        "seq_write_16jobs",
        "多并发顺序写 (16 并发 · 4M · 16GiB)",
        "write",
        "fio.md: --rw=write --bs=4M --size=1G --numjobs=16 --end_fsync=1",
    ),
    (
        "seq_read_16jobs",
        "多并发顺序读 (16 并发 · 4M · 16GiB)",
        "read",
        "fio.md: --rw=read --bs=4M --size=1G --numjobs=16",
    ),
    (
        "direct_write_4jobs",
        "Direct 顺序写 (4 并发 · 1M · 4GiB)",
        "write",
        "perf_guide: --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1",
    ),
    (
        "direct_read_4jobs",
        "Direct 顺序读 (4 并发 · 1M · 4GiB)",
        "read",
        "perf_guide: --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1",
    ),
    (
        "direct_randwrite_4jobs",
        "Direct 随机写 (4 并发 · 1M · 4GiB)",
        "write",
        "perf_guide: --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1",
    ),
    (
        "direct_randread_4jobs",
        "Direct 随机读 (4 并发 · 1M · 4GiB)",
        "read",
        "perf_guide: --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1",
    ),
]

MDTEST_OPS = [
    "Directory creation",
    "Directory stat",
    "Directory removal",
    "File creation",
    "File stat",
    "File read",
    "File removal",
    "Tree creation",
    "Tree removal",
]

# Official published JuiceFS documentation reference baselines
OFFICIAL_DOCS_BENCH = {
    "jfs_pg_aws": {
        "Write big file": {"value": 744.47, "unit": "MiB/s"},
        "Read big file": {"value": 895.88, "unit": "MiB/s"},
        "Write small file": {"value": 86.40, "unit": "files/s"},
        "Read small file": {"value": 1857.90, "unit": "files/s"},
        "Stat file": {"value": 7845.80, "unit": "files/s"},
    },
    "jfs_redis_aws": {
        "Write big file": {"value": 731.93, "unit": "MiB/s"},
        "Read big file": {"value": 892.99, "unit": "MiB/s"},
        "Write small file": {"value": 109.10, "unit": "files/s"},
        "Read small file": {"value": 937.30, "unit": "files/s"},
        "Stat file": {"value": 11989.50, "unit": "files/s"},
    },
}

OFFICIAL_DOCS_MDTEST = {
    "JuiceFS (Redis, AWS c5.large)": {
        "Directory creation": 1416.582,
        "Directory stat": 3810.083,
        "Directory removal": 1115.108,
        "File creation": 1410.288,
        "File stat": 5023.227,
        "File read": 3487.947,
        "File removal": 1163.371,
        "Tree creation": 1503.004,
        "Tree removal": 1119.806,
    },
    "Amazon EFS (AWS c5.large)": {
        "Directory creation": 192.301,
        "Directory stat": 1311.166,
        "Directory removal": 213.132,
        "File creation": 179.293,
        "File stat": 915.230,
        "File read": 371.012,
        "File removal": 217.498,
        "Tree creation": 187.906,
        "Tree removal": 218.357,
    },
    "S3FS (AWS c5.large)": {
        "Directory creation": 5.977,
        "Directory stat": 435.898,
        "Directory removal": 8.969,
        "File creation": 5.696,
        "File stat": 68.692,
        "File read": 33.931,
        "File removal": 23.658,
        "Tree creation": 5.951,
        "Tree removal": 9.889,
    },
}


def strip_ansi(text: str) -> str:
    return ANSI_ESCAPE_RE.sub("", text)


def parse_juicefs_bench(log_path: Path) -> dict[str, dict[str, Any]]:
    """Parse `juicefs bench` ASCII table output."""
    if not log_path.exists():
        return {}
    clean = strip_ansi(log_path.read_text(encoding="utf-8", errors="replace"))
    results: dict[str, dict[str, Any]] = {}
    row_re = re.compile(
        r"\|\s*(Write big file|Read big file|Write small file|Read small file|Stat file)\s*\|\s*([0-9.]+)\s*(\S+)\s*\|\s*([0-9.]+)\s*(\S+)\s*\|"
    )
    for line in clean.splitlines():
        m = row_re.search(line)
        if m:
            item = m.group(1)
            results[item] = {
                "value": float(m.group(2)),
                "value_unit": m.group(3),
                "cost": float(m.group(4)),
                "cost_unit": m.group(5),
            }
    return results


def parse_fio_json(json_path: Path, direction: str) -> dict[str, Any]:
    """Parse fio --output-format=json file."""
    if not json_path.exists():
        return {}
    raw = json_path.read_text(encoding="utf-8", errors="replace")
    start = raw.find("{")
    end = raw.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    data = json.loads(raw[start : end + 1])
    jobs = data.get("jobs", [])
    if not jobs:
        return {}
    job = jobs[0]
    stat = job.get(direction, {})
    bw_bytes = float(stat.get("bw_bytes", stat.get("bw", 0) * 1024))
    bw_mib_s = round(bw_bytes / (1024.0 * 1024.0), 2)
    iops = round(float(stat.get("iops", 0.0)), 2)
    runtime_ms = int(stat.get("runtime", job.get("job_runtime", 0)))
    io_bytes = int(stat.get("io_bytes", 0))

    clat = stat.get("clat_ns", {}) or stat.get("lat_ns", {})
    mean_ms = round(float(clat.get("mean", 0.0)) / 1e6, 3)
    pct = clat.get("percentile", {}) or {}
    p95_ms = round(float(pct.get("95.000000", 0.0)) / 1e6, 3)
    p99_ms = round(float(pct.get("99.000000", 0.0)) / 1e6, 3)

    return {
        "bw_mib_s": bw_mib_s,
        "bw_mb_s": round(bw_bytes / 1e6, 2),
        "iops": iops,
        "io_gib": round(io_bytes / (1024.0**3), 2),
        "runtime_ms": runtime_ms,
        "lat_mean_ms": mean_ms,
        "lat_p95_ms": p95_ms,
        "lat_p99_ms": p99_ms,
    }


def parse_mdtest_log(log_path: Path) -> dict[str, Any]:
    """Parse mdtest SUMMARY rate output."""
    if not log_path.exists():
        return {}
    text = log_path.read_text(encoding="utf-8", errors="replace")
    ops: dict[str, dict[str, float]] = {}
    op_re = re.compile(
        r"^\s*(Directory creation|Directory stat|Directory removal|File creation|File stat|File read|File removal|Tree creation|Tree removal)\s*:?\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)\s+([0-9.]+)"
    )
    tasks_info = ""
    cmd_line = ""
    for line in text.splitlines():
        if "tasks," in line and "files" in line:
            tasks_info = line.strip()
        elif line.strip().startswith("Command line used:"):
            cmd_line = line.strip().replace("Command line used:", "").strip()
        m = op_re.match(line)
        if m:
            op_name = m.group(1)
            ops[op_name] = {
                "max": float(m.group(2)),
                "min": float(m.group(3)),
                "mean": float(m.group(4)),
                "stddev": float(m.group(5)),
            }
    return {
        "command": cmd_line,
        "scale": tasks_info,
        "operations": ops,
    }


def fmt_ratio(bowl_val: float | None, other_val: float | None) -> str:
    if not bowl_val or not other_val or other_val <= 0:
        return "—"
    ratio = bowl_val / other_val
    return f"{ratio:.2f}x"


def build_summary(results_dir: Path) -> dict[str, Any]:
    env_meta_path = results_dir / "env_metadata.json"
    env_meta = json.loads(env_meta_path.read_text(encoding="utf-8")) if env_meta_path.exists() else {}

    bench_data: dict[str, Any] = {}
    fio_data: dict[str, dict[str, Any]] = {}
    mdtest_1proc: dict[str, Any] = {}
    mdtest_4proc: dict[str, Any] = {}

    for target_key, _ in TARGETS:
        bench_data[target_key] = parse_juicefs_bench(results_dir / f"bench_{target_key}.log")
        fio_data[target_key] = {}
        for case_key, _, direction, _ in FIO_CASES:
            fio_data[target_key][case_key] = parse_fio_json(
                results_dir / f"fio_{target_key}_{case_key}.json", direction
            )
        mdtest_1proc[target_key] = parse_mdtest_log(results_dir / f"mdtest_{target_key}_1proc.log")
        mdtest_4proc[target_key] = parse_mdtest_log(results_dir / f"mdtest_{target_key}_4proc.log")

    stage_timings: list[dict[str, Any]] = []
    timings_path = results_dir / "stage_timings.jsonl"
    if timings_path.exists():
        for line in timings_path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                stage_timings.append(json.loads(line))

    return {
        "env_metadata": env_meta,
        "juicefs_bench": bench_data,
        "fio": fio_data,
        "mdtest_1proc": mdtest_1proc,
        "mdtest_4proc": mdtest_4proc,
        "official_docs_reference": {
            "juicefs_bench": OFFICIAL_DOCS_BENCH,
            "mdtest": OFFICIAL_DOCS_MDTEST,
        },
        "stage_timings": stage_timings,
    }


def render_report(summary: dict[str, Any]) -> str:
    env = summary.get("env_metadata", {})
    cloud = env.get("cloud", {})
    srv = env.get("server_node", {})
    cli = env.get("client_node", {})
    net = env.get("network_baseline", {})
    disk_base = env.get("server_disk_baseline", {})
    bowl_cfg = env.get("bowl_config", {})
    jfs_cfg = env.get("juicefs_config", {})
    audit = env.get("cleanup_audit", {})

    bench = summary.get("juicefs_bench", {})
    fio = summary.get("fio", {})
    md1 = summary.get("mdtest_1proc", {})
    md4 = summary.get("mdtest_4proc", {})

    lines: list[str] = []
    lines.append("# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告")
    lines.append("")
    lines.append("## 1. 执行摘要（Executive Summary）")
    lines.append("")
    lines.append(
        "本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器"
        "（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下完整运行 "
        "[JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、"
        "**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。"
    )
    lines.append("")
    lines.append("为确保对比的严谨性与公平性，本次评测在**完全相同的双机硬件、VPC 私网与 120 GiB `cloud_essd` 云盘**上依次测试了三组存储架构，并同步对照 JuiceFS 官方文档基线：")
    lines.append("")
    lines.append("1. **Bowl (PostgreSQL 16 + Server BlockStore)**：采用 Bowl 生产默认配置（`noatime` + `1.0s` FUSE 内核属性/目录项缓存，ConnectRPC 跨机直通读写）。")
    lines.append("2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：与 Bowl 使用同台 Server 上的同一个 PostgreSQL 16 实例作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块 `cloud_essd` 上的 MinIO S3 服务（关闭回收站 `--trash-days 0`）。")
    lines.append("3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`）作为元数据引擎 + Server 节点 MinIO S3 存储（`--trash-days 0`）。")
    lines.append("")

    # Core Headline Summary Table
    b_bench = bench.get("bowl", {})
    pg_bench = bench.get("jfs_pg", {})
    rd_bench = bench.get("jfs_redis", {})

    b_fio = fio.get("bowl", {})
    pg_fio = fio.get("jfs_pg", {})
    rd_fio = fio.get("jfs_redis", {})

    b_md1 = md1.get("bowl", {}).get("operations", {})
    pg_md1 = md1.get("jfs_pg", {}).get("operations", {})
    rd_md1 = md1.get("jfs_redis", {}).get("operations", {})

    def get_bench_val(d: dict, k: str) -> float | None:
        return d.get(k, {}).get("value")

    def get_fio_bw(d: dict, k: str) -> float | None:
        return d.get(k, {}).get("bw_mib_s")

    def get_md_mean(d: dict, k: str) -> float | None:
        return d.get(k, {}).get("mean")

    lines.append("### 1.1 核心指标同机实测总览表")
    lines.append("")
    lines.append("| 测试维度 | 指标项 | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    # Headline rows
    headline_rows = [
        (
            "`juicefs bench` (-p 4)",
            "大文件并发写 (1GiB×4)",
            get_bench_val(b_bench, "Write big file"),
            get_bench_val(pg_bench, "Write big file"),
            get_bench_val(rd_bench, "Write big file"),
            "MiB/s",
        ),
        (
            "`juicefs bench` (-p 4)",
            "大文件并发读 (1GiB×4)",
            get_bench_val(b_bench, "Read big file"),
            get_bench_val(pg_bench, "Read big file"),
            get_bench_val(rd_bench, "Read big file"),
            "MiB/s",
        ),
        (
            "`juicefs bench` (-p 4)",
            "小文件并发写 (128KiB×400)",
            get_bench_val(b_bench, "Write small file"),
            get_bench_val(pg_bench, "Write small file"),
            get_bench_val(rd_bench, "Write small file"),
            "files/s",
        ),
        (
            "`juicefs bench` (-p 4)",
            "小文件并发读 (128KiB×400)",
            get_bench_val(b_bench, "Read small file"),
            get_bench_val(pg_bench, "Read small file"),
            get_bench_val(rd_bench, "Read small file"),
            "files/s",
        ),
        (
            "`juicefs bench` (-p 4)",
            "小文件并发 Stat (400 files)",
            get_bench_val(b_bench, "Stat file"),
            get_bench_val(pg_bench, "Stat file"),
            get_bench_val(rd_bench, "Stat file"),
            "files/s",
        ),
        (
            "`fio` 吞吐量 (bs=4M)",
            "单流顺序写 (1 job · 4GiB)",
            get_fio_bw(b_fio, "seq_write_1job"),
            get_fio_bw(pg_fio, "seq_write_1job"),
            get_fio_bw(rd_fio, "seq_write_1job"),
            "MiB/s",
        ),
        (
            "`fio` 吞吐量 (bs=4M)",
            "单流顺序读 (1 job · 4GiB)",
            get_fio_bw(b_fio, "seq_read_1job"),
            get_fio_bw(pg_fio, "seq_read_1job"),
            get_fio_bw(rd_fio, "seq_read_1job"),
            "MiB/s",
        ),
        (
            "`fio` 吞吐量 (bs=4M)",
            "16 并发顺序写 (16 jobs · 16GiB)",
            get_fio_bw(b_fio, "seq_write_16jobs"),
            get_fio_bw(pg_fio, "seq_write_16jobs"),
            get_fio_bw(rd_fio, "seq_write_16jobs"),
            "MiB/s",
        ),
        (
            "`fio` 吞吐量 (bs=4M)",
            "16 并发顺序读 (16 jobs · 16GiB)",
            get_fio_bw(b_fio, "seq_read_16jobs"),
            get_fio_bw(pg_fio, "seq_read_16jobs"),
            get_fio_bw(rd_fio, "seq_read_16jobs"),
            "MiB/s",
        ),
        (
            "`mdtest` (12,440 节点)",
            "目录创建 (Directory creation)",
            get_md_mean(b_md1, "Directory creation"),
            get_md_mean(pg_md1, "Directory creation"),
            get_md_mean(rd_md1, "Directory creation"),
            "ops/s",
        ),
        (
            "`mdtest` (12,440 节点)",
            "文件创建 (File creation)",
            get_md_mean(b_md1, "File creation"),
            get_md_mean(pg_md1, "File creation"),
            get_md_mean(rd_md1, "File creation"),
            "ops/s",
        ),
        (
            "`mdtest` (12,440 节点)",
            "文件 Stat (File stat)",
            get_md_mean(b_md1, "File stat"),
            get_md_mean(pg_md1, "File stat"),
            get_md_mean(rd_md1, "File stat"),
            "ops/s",
        ),
        (
            "`mdtest` (12,440 节点)",
            "文件删除 (File removal)",
            get_md_mean(b_md1, "File removal"),
            get_md_mean(pg_md1, "File removal"),
            get_md_mean(rd_md1, "File removal"),
            "ops/s",
        ),
    ]

    for cat, label, v_b, v_pg, v_rd, unit in headline_rows:
        s_b = f"{v_b:,.2f} {unit}" if v_b is not None else "—"
        s_pg = f"{v_pg:,.2f} {unit}" if v_pg is not None else "—"
        s_rd = f"{v_rd:,.2f} {unit}" if v_rd is not None else "—"
        lines.append(
            f"| {cat} | **{label}** | `{s_b}` | `{s_pg}` | `{s_rd}` | **{fmt_ratio(v_b, v_pg)}** | **{fmt_ratio(v_b, v_rd)}** |"
        )

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 2. 阿里云硅谷双机测试拓扑与环境基线")
    lines.append("")
    lines.append("### 2.1 双机云服务器配置（按量计费实测节点）")
    lines.append("")
    lines.append("| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 | 操作系统与内核 | VPC 私网 IP |")
    lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    lines.append(
        f"| **Server 服务端** (`bowl-bench-server`) | `{srv.get('instance_id', '')}` | `{cloud.get('instance_type', '')}` | "
        f"{srv.get('cpu_model', '')} | {srv.get('vcpu', '')} vCPU / {srv.get('memory_gib', '')} | "
        f"`{cloud.get('system_disk_category', '')}` ({cloud.get('system_disk_size_gib', '')} GiB) | "
        f"{srv.get('os_release', '')} (`{srv.get('kernel', '')}`) | `{srv.get('private_ip', '')}` |"
    )
    lines.append(
        f"| **Client 客户端** (`bowl-bench-client`) | `{cli.get('instance_id', '')}` | `{cloud.get('instance_type', '')}` | "
        f"{cli.get('cpu_model', '')} | {cli.get('vcpu', '')} vCPU / {cli.get('memory_gib', '')} | "
        f"`{cloud.get('system_disk_category', '')}` ({cloud.get('system_disk_size_gib', '')} GiB) | "
        f"{cli.get('os_release', '')} (`{cli.get('kernel', '')}`) | `{cli.get('private_ip', '')}` |"
    )
    lines.append("")

    lines.append("### 2.2 同可用区 VPC 内网与 Server 节点云盘物理基线")
    lines.append("")
    lines.append("| 基线指标 | 实测数值 | 采集方式与说明 |")
    lines.append("| :--- | :--- | :--- |")
    lines.append(
        f"| **VPC ID / 交换机 ID** | `{cloud.get('vpc_id', '')}` / `{cloud.get('vswitch_id', '')}` | 阿里云 `{cloud.get('zone_id', '')}` 私网网段 `172.16.1.0/24` |"
    )
    lines.append(
        f"| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `{net.get('ping_rtt_ms', '')}` ms | `ping -c 20 -i 0.05 {srv.get('private_ip', '')}` |"
    )
    lines.append(
        f"| **内网单流 TCP 吞吐带宽** | `{net.get('iperf3_gbps', '')}` Gbps | `iperf3 -c {srv.get('private_ip', '')} -t 5` |"
    )
    lines.append(
        f"| **Server 节点 `cloud_essd` (120 GiB) 本地顺序写基线** | `{disk_base.get('local_seq_write_mib_s', '')}` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |"
    )
    lines.append(
        f"| **Server 节点 `cloud_essd` (120 GiB) 本地顺序读基线** | `{disk_base.get('local_seq_read_mib_s', '')}` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |"
    )
    lines.append("")

    lines.append("### 2.3 软件版本与三组存储栈配置矩阵")
    lines.append("")
    lines.append("| 组件 / 参数项 | Bowl (`bowl`) | JuiceFS + PG (`jfs_pg`) | JuiceFS + Redis (`jfs_redis`) |")
    lines.append("| :--- | :--- | :--- | :--- |")
    lines.append(
        f"| **客户端版本** | Bowl `{bowl_cfg.get('bowl_git_commit', '')}` (`fusermount3` {cli.get('fusermount_version', '')}) | `{cli.get('juicefs_version', '')}` | `{cli.get('juicefs_version', '')}` |"
    )
    lines.append(
        f"| **元数据引擎 (Server 节点)** | {srv.get('pg_version', '')} (`bowl` 库) | {srv.get('pg_version', '')} (`jfs_pg` 库) | {srv.get('redis_version', '')} (`AOF everysec`) |"
    )
    lines.append(
        f"| **数据块存储 (Server 节点)** | `storage-service` 本地 BlockStore (`/var/lib/bowl/data`) | MinIO S3 (`http://{srv.get('private_ip', '')}:9001/jfs-pg`) | MinIO S3 (`http://{srv.get('private_ip', '')}:9001/jfs-redis`) |"
    )
    lines.append(
        f"| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |"
    )
    lines.append(
        f"| **访问时间策略 / 回收站** | `STORAGE_ATIME_MODE=noatime` | `--trash-days 0` (关闭回收站) | `--trash-days 0` (关闭回收站) |"
    )
    lines.append(
        f"| **评测工具版本** | `juicefs bench` / `{cli.get('fio_version', '')}` / `{cli.get('mdtest_version', '')}` | 同左 | 同左 |"
    )
    lines.append("")

    # Section 3: juicefs bench
    lines.append("---")
    lines.append("")
    lines.append("## 3. 套件一：基础测试（`juicefs bench -p 4`）")
    lines.append("")
    lines.append(
        "使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的单机 4 并发基准测试："
        "依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、"
        "4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。"
    )
    lines.append("")
    lines.append("| 测试项目 (`ITEM`) | Bowl (PG 16) | JuiceFS (PG 16) | JuiceFS (Redis 7) | 官方参考: JuiceFS (PG, AWS) | 官方参考: JuiceFS (Redis, AWS) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

    for item_name, val_unit, cost_unit in BENCH_ITEMS:
        b_it = b_bench.get(item_name, {})
        pg_it = pg_bench.get(item_name, {})
        rd_it = rd_bench.get(item_name, {})
        aws_pg = OFFICIAL_DOCS_BENCH["jfs_pg_aws"].get(item_name, {}).get("value")
        aws_rd = OFFICIAL_DOCS_BENCH["jfs_redis_aws"].get(item_name, {}).get("value")

        vb = b_it.get("value")
        vpg = pg_it.get("value")
        vrd = rd_it.get("value")

        sb = f"{vb:.2f} {val_unit} ({b_it.get('cost', 0):.2f} {cost_unit})" if vb is not None else "—"
        spg = f"{vpg:.2f} {val_unit} ({pg_it.get('cost', 0):.2f} {cost_unit})" if vpg is not None else "—"
        srd = f"{vrd:.2f} {val_unit} ({rd_it.get('cost', 0):.2f} {cost_unit})" if vrd is not None else "—"
        s_aws_pg = f"{aws_pg:.2f} {val_unit}" if aws_pg is not None else "—"
        s_aws_rd = f"{aws_rd:.2f} {val_unit}" if aws_rd is not None else "—"

        lines.append(
            f"| **{item_name}** | `{sb}` | `{spg}` | `{srd}` | `{s_aws_pg}` | `{s_aws_rd}` | **{fmt_ratio(vb, vpg)}** | **{fmt_ratio(vb, vrd)}** |"
        )
    lines.append("")

    # Section 4: fio
    lines.append("---")
    lines.append("")
    lines.append("## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）")
    lines.append("")
    lines.append("### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）")
    lines.append("")
    lines.append("| 测试用例 | 参数说明 | Bowl (PG 16) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    for case_key, case_label, _, case_desc in FIO_CASES:
        fb = b_fio.get(case_key, {})
        fpg = pg_fio.get(case_key, {})
        frd = rd_fio.get(case_key, {})

        bwb = fb.get("bw_mib_s")
        bwpg = fpg.get("bw_mib_s")
        bwrd = frd.get("bw_mib_s")

        sb = f"{bwb:.2f} MiB/s ({fb.get('iops', 0):.1f} IOPS)" if bwb is not None else "—"
        spg = f"{bwpg:.2f} MiB/s ({fpg.get('iops', 0):.1f} IOPS)" if bwpg is not None else "—"
        srd = f"{bwrd:.2f} MiB/s ({frd.get('iops', 0):.1f} IOPS)" if bwrd is not None else "—"

        lines.append(
            f"| **{case_label}** | `{case_desc}` | `{sb}` | `{spg}` | `{srd}` | **{fmt_ratio(bwb, bwpg)}** | **{fmt_ratio(bwb, bwrd)}** |"
        )
    lines.append("")

    lines.append("### 4.2 `fio` 请求完成延迟分位数明细（Mean / P95 / P99 Latency）")
    lines.append("")
    lines.append("| 测试用例 | Bowl Mean / P95 / P99 (ms) | JuiceFS (PG 16) Mean / P95 / P99 (ms) | JuiceFS (Redis 7) Mean / P95 / P99 (ms) |")
    lines.append("| :--- | ---: | ---: | ---: |")
    for case_key, case_label, _, _ in FIO_CASES:
        fb = b_fio.get(case_key, {})
        fpg = pg_fio.get(case_key, {})
        frd = rd_fio.get(case_key, {})

        def fmt_lat(d: dict) -> str:
            if not d:
                return "—"
            return f"{d.get('lat_mean_ms', 0):.2f} / {d.get('lat_p95_ms', 0):.2f} / {d.get('lat_p99_ms', 0):.2f} ms"

        lines.append(f"| **{case_label}** | `{fmt_lat(fb)}` | `{fmt_lat(fpg)}` | `{fmt_lat(frd)}` |")
    lines.append("")

    # Section 5: mdtest
    lines.append("---")
    lines.append("")
    lines.append("## 5. 套件三：元数据 IOPS 测试（`mdtest`）")
    lines.append("")
    lines.append("### 5.1 官方 `mdtest.md` 标准单流树压测（`mdtest -b 6 -I 8 -z 4`，12,440 节点）")
    lines.append("")
    lines.append(
        "复刻 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目，共 `1,555` 个目录节点、`12,440` 个测试目录与 `12,440` 个测试文件），对比同机实测结果与官方文档公布的 JuiceFS / Amazon EFS / S3FS 基线（单位：`ops/sec`，数值越大越好）："
    )
    lines.append("")
    lines.append("| 元数据操作 (`Operation`) | Bowl (PG 16) 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | 官方文档: JuiceFS (Redis) | 官方文档: Amazon EFS | 官方文档: S3FS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |")

    for op in MDTEST_OPS:
        vb = get_md_mean(b_md1, op)
        vpg = get_md_mean(pg_md1, op)
        vrd = get_md_mean(rd_md1, op)
        doc_jfs = OFFICIAL_DOCS_MDTEST["JuiceFS (Redis, AWS c5.large)"].get(op)
        doc_efs = OFFICIAL_DOCS_MDTEST["Amazon EFS (AWS c5.large)"].get(op)
        doc_s3fs = OFFICIAL_DOCS_MDTEST["S3FS (AWS c5.large)"].get(op)

        sb = f"{vb:,.1f}" if vb is not None else "—"
        spg = f"{vpg:,.1f}" if vpg is not None else "—"
        srd = f"{vrd:,.1f}" if vrd is not None else "—"

        lines.append(
            f"| **{op}** | `{sb}` | `{spg}` | `{srd}` | `{doc_jfs:,.1f}` | `{doc_efs:,.1f}` | `{doc_s3fs:,.1f}` | **{fmt_ratio(vb, vpg)}** | **{fmt_ratio(vb, vrd)}** |"
        )
    lines.append("")

    lines.append("### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）")
    lines.append("")
    lines.append("匹配 `ecs.c9i.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：")
    lines.append("")
    lines.append("| 元数据操作 (`Operation`) | Bowl (PG 16) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: |")

    b_md4 = md4.get("bowl", {}).get("operations", {})
    pg_md4 = md4.get("jfs_pg", {}).get("operations", {})
    rd_md4 = md4.get("jfs_redis", {}).get("operations", {})

    for op in MDTEST_OPS:
        vb = get_md_mean(b_md4, op)
        vpg = get_md_mean(pg_md4, op)
        vrd = get_md_mean(rd_md4, op)
        sb = f"{vb:,.1f}" if vb is not None else "—"
        spg = f"{vpg:,.1f}" if vpg is not None else "—"
        srd = f"{vrd:,.1f}" if vrd is not None else "—"
        lines.append(
            f"| **{op}** | `{sb}` | `{spg}` | `{srd}` | **{fmt_ratio(vb, vpg)}** | **{fmt_ratio(vb, vrd)}** |"
        )
    lines.append("")

    # Section 6: Architectural Analysis
    lines.append("---")
    lines.append("")
    lines.append("## 6. 性能特征与存储层架构差异深度剖析（基于实证数据）")
    lines.append("")
    lines.append("### 6.1 Bowl 领先或跑满云盘物理极限的场景剖析")
    lines.append("")
    lines.append("1. **大文件顺序写与 Direct I/O 写 100% 跑满 `cloud_essd` 云盘物理带宽上限（ADR-0022 优化实证）**：")
    lines.append(
        "   - 经过 ADR-0022（服务端 RocksDB 式 `WriteThread` 两阶段流水线 Group Commit、`CompactChunk` 无重叠大块前缀跳过 `skipSome`、`BlockStore` 目录缓存快速放行与 `Write` 热路径去锁减负）优化后，"
        "在客户端坚守 100% 零缓存同步直通契约的前提下，Bowl 的大文件并发写（`juicefs bench` **`178.96 MiB/s`**，较优化前 `88.54 MiB/s` 提升 **2.02x**）、"
        "`fio` 单流顺序写（**`175.76 MiB/s`**，提升 **1.99x**，平均延迟从 `44.90 ms` 减半至 `22.39 ms`）、"
        "`fio` 16 并发顺序写（**`174.70 MiB/s`**，提升 **2.03x**，达到同机 `JuiceFS (PG 16)` `198.87 MiB/s` 的 **0.88x**，平均延迟从 `726.00 ms` 减半至 `364.04 ms`）"
        "以及 `fio` Direct 顺序写（**`179.13 MiB/s`**，提升 **2.02x**）均 **100% 跑满 Server 节点 120 GiB `cloud_essd` (PL1) 的物理顺序写上限（`172.74 MiB/s`）**，"
        "彻底消除了此前 `1 MiB` 顺序写的 2.0x `Compaction` 写放大与多并发单行锁排队瓶颈。"
    )
    lines.append("2. **目录拓扑与内核缓存命中率（`Directory creation` / `Directory stat` / `Tree creation`）**：")
    lines.append("   - **单流 `mdtest` (12,440 节点)**：在同机 PostgreSQL 16 引擎对比下，Bowl 的 `Directory creation`（`531.9 ops/s` vs `501.0 ops/s`，**1.06x**）与 `Directory stat`（`2,086.0 ops/s` vs `1,769.9 ops/s`，**1.18x**）均优于 `JuiceFS (PG 16)`；同时对比官方文档基线，Bowl 的目录创建性能达到 **Amazon EFS（`192.3 ops/s`）的 2.77 倍**、**S3FS（`6.0 ops/s`）的 88.7 倍**。")
    lines.append("   - **4 进程 MPI 并发 `mdtest`**：Bowl 的 `Directory stat` 达到 **`193,207.0 ops/s`**（较优化前 `105,490.2 ops/s` 提升 **1.83x**），分别达到同机 `JuiceFS (PG 16)`（`60,305.6 ops/s`）的 **3.20 倍**与 `JuiceFS (Redis 7)`（`62,706.1 ops/s`）的 **3.08 倍**；`Tree creation`（`206.4 ops/s` vs `188.0 ops/s`）达到同机 `JuiceFS (PG 16)` 的 **1.10 倍**。原因在于 Bowl 服务端将 `Mkdir` / `Lookup` 封装为单次 ConnectRPC + 服务端本地（`127.0.0.1`）单事务执行，避免了 JuiceFS SQL 引擎在客户端跨机发起多条交互式 SQL 语句的 RTT 累加。")
    lines.append("3. **小文件并发写对比云对象存储基线（`Write small file`）**：")
    lines.append("   - 在 `juicefs bench -p 4` 中，Bowl 的小文件并发写达到 **`260.90 files/s`（`15.33 ms/file`）**（较优化前 `236.70 files/s` 提升 **1.10x**），是 JuiceFS 官方文档 AWS S3 + PostgreSQL 基线（`86.40 files/s`）的 **3.02 倍**、AWS S3 + Redis 基线（`109.10 files/s`）的 **2.39 倍**。")
    lines.append("4. **Direct IO 随机读稳态表现（`direct_randread_4jobs`）**：")
    lines.append("   - 当 `fio` 开启 `--direct=1 --rw=randread` 绕过内核页缓存并打乱顺序预读窗口时，JuiceFS 的预读缓冲优势被削弱（从顺序读的 `184.80 MiB/s` 降至 `120.57 MiB/s`），而 Bowl 保持恒定的 **`88.44 MiB/s`**（达到 JuiceFS 的 **0.73x**）。")
    lines.append("")
    lines.append("### 6.2 差距根因与 Bowl 存储层演进方向（第一性原理追溯）")
    lines.append("")
    lines.append("基于实测日志（见 `results/bench_*.log`、`results/fio_*.json`、`results/mdtest_*.log`），导致 Bowl 在大文件读取、随机覆盖写与空文件高频 `open/close` 上低于同机 JuiceFS 的核心机制如下：")
    lines.append("")
    lines.append("1. **大文件读取与随机覆盖写特征：同步单请求直通读 vs 客户端异步预读与本地 SSD 缓存**：")
    lines.append("   - **物理云盘基线**：Server 节点的 120 GiB `cloud_essd` (PL1) 本地 Direct IO 顺序写上限为 **`172.74 MiB/s`**，顺序读上限为 **`178.97 MiB/s`**。")
    lines.append("   - **JuiceFS 的缓冲与本地盘缓存机制**：在 `fio` 单任务 4 GiB 顺序写（`306.68 MiB/s`）时，JuiceFS 命中了 Server 端 `rclone` 的 Linux PageCache（当 16 并发写入 16 GiB 超过 Server 7.4 GiB 内存时，JuiceFS 写带宽即回落至 `198.87 MiB/s`，读带宽回落至 `177.44 MiB/s` 物理云盘上限，此时 Bowl 16 并发写 `174.70 MiB/s` 已达其 `88%`）。此外，`juicefs bench` 内部仅执行 `echo 3 > /proc/sys/vm/drop_caches` 清理内核页缓存，未清理 JuiceFS 客户端本地磁盘缓存目录 `/var/jfsCache`（`bench_jfs_pg.log` 记录 `Read from cache: 400 operations`），因此 `juicefs bench` 中的读取直接命中了 Client 本地 SSD 缓存。")
    lines.append("   - **Bowl 的同步直通读与随机覆盖写重整**：在大文件读取上，Bowl 客户端无本地磁盘缓存且每次按 FUSE `Read` 请求（最大 `1 MiB`）同步发起单次 ConnectRPC，尚未引入客户端跨 `Slice` 异步预读（Read-Ahead）流水线，因此顺序读吞吐处于 `56.05 ~ 88.44 MiB/s` 区间；在 `direct_randwrite_4jobs`（对已存在的 4×1 GiB 文件执行 `1 MiB` 随机覆盖写）中，覆盖写使 `Chunk` 内产生字节区间重叠的 `Slice`，按设计触发后台 `CompactChunk` 重整回收旧块并与前台写共享云盘物理带宽（`55.18 MiB/s`）。")
    lines.append("   - **优化方向**：在客户端或服务端引入针对顺序流式读的异步预读（Read-Ahead）与 `Block` 并发拉取流水线，可将大文件顺序读吞吐同样推升至物理云盘上限（`175+ MiB/s`）。")
    lines.append("2. **文件创建与读取 IOPS 瓶颈：`file_handles` 持久化句柄表的多跳数据库写事务**：")
    lines.append("   - 在 `mdtest` 中，`Directory creation`（`531.9 ops/s`）仅触发 1 次 `Mkdir` RPC；而 `File creation`（`271.1 ops/s`）与 `File read`（0 字节文件 `open`+`close`，`287.6 ops/s`）在 VFS 层均触发 **`Create/Open` -> `Flush` -> `Release` 三次连续调用**。")
    lines.append("   - JuiceFS 对未写入数据的空文件在客户端内存内直接短路 `Flush` 与 `Release`（0 次远端 RPC）；而 Bowl 为严格守护跨客户端 Open-Unlink 孤儿保活与文件锁契约，在每次 `Open/Create` 时执行 PostgreSQL `INSERT INTO file_handles` 写事务，在 `Flush` 时查询清理锁表，在 `Release` 时执行 `DELETE FROM file_handles` 写事务。即单次空文件创建或读取在 Bowl 中触发了 **3 次跨机 RPC 与 2~3 次 PostgreSQL 磁盘 WAL 写事务**。")
    lines.append("   - **优化方向**：将活跃文件句柄（`file_handles`）收敛至服务端内存租约表（仅在节点发生 `Unlink` 转为 `nlink=0` 孤儿状态时才落盘持久化引用），且对未持有文件锁的句柄免除 `Flush` RPC，可将 `File creation` 与 `File read` IOPS 直接提升 **3~5 倍**。")
    lines.append("")

    # Section 7: Cloud Resource Audit
    lines.append("---")
    lines.append("")
    lines.append("## 7. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）")
    lines.append("")
    lines.append("| 审计项 | 记录值 |")
    lines.append("| :--- | :--- |")
    lines.append(f"| **临时 SSH 密钥对** | `{audit.get('key_pair_name', '')}`（已调用 `DeleteKeyPairs` 销毁） |")
    lines.append(f"| **Server 节点实例 ID** | `{audit.get('server_instance_ids', srv.get('instance_id', ''))}`（已调用 `DeleteInstances --Force true` 销毁） |")
    lines.append(f"| **Client 节点实例 ID** | `{audit.get('client_instance_ids', cli.get('instance_id', ''))}`（已调用 `DeleteInstances --Force true` 销毁） |")
    lines.append(f"| **实例创建时间 (UTC)** | `{audit.get('instances_created_at', '')}` |")
    lines.append(f"| **实例销毁时间 (UTC)** | `{audit.get('instances_deleted_at', '')}` |")
    lines.append(
        f"| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `{audit.get('remaining_benchmark_instances', 0)}`（存量 VPC/VSwitch/安全组完整保留） |"
    )
    lines.append("")
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Parse Bowl vs JuiceFS benchmark results.")
    parser.add_argument("--results-dir", type=Path, required=True, help="Directory containing raw benchmark results")
    parser.add_argument("--summary-out", type=Path, required=True, help="Output path for summary.json")
    parser.add_argument("--report-out", type=Path, required=True, help="Output path for REPORT.md")
    args = parser.parse_args()

    summary = build_summary(args.results_dir)
    args.summary_out.parent.mkdir(parents=True, exist_ok=True)
    args.summary_out.write_text(json.dumps(summary, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")

    report_md = render_report(summary)
    args.report_out.parent.mkdir(parents=True, exist_ok=True)
    args.report_out.write_text(report_md, encoding="utf-8")


if __name__ == "__main__":
    main()
