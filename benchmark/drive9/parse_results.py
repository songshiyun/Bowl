#!/usr/bin/env python3
"""Parse Bowl vs Drive9 benchmark results (juicefs bench, drive9_fuse_bench, fuse_perf_baseline) into summary.json and REPORT.md."""

from __future__ import annotations

import argparse
import json
import math
import re
import sys
from pathlib import Path
from typing import Any

COMMON_DIR = Path(__file__).resolve().parent.parent / "common"
if str(COMMON_DIR) not in sys.path:
    sys.path.insert(0, str(COMMON_DIR))

from collect_telemetry import load_telemetry_overview, render_report_telemetry_section  # noqa: E402

ANSI_ESCAPE_RE = re.compile(r"\x1b\[[0-9;]*[a-zA-Z]")

TARGETS = [
    ("bowl", "Bowl (PostgreSQL 控制面 + Pebble)"),
    ("drive9_default", "Drive9 Default (coding-agent + interactive)"),
    ("drive9_sync", "Drive9 Sync (profile=none + close-sync)"),
]

BENCH_ITEMS = [
    ("Write big file", "MiB/s", "s/file"),
    ("Read big file", "MiB/s", "s/file"),
    ("Write small file", "files/s", "ms/file"),
    ("Read small file", "files/s", "ms/file"),
    ("Stat file", "files/s", "ms/file"),
]

MICRO_WORKLOADS = [
    ("sequential_write", "顺序写 (256 MiB + fsync)", "MiB/s"),
    ("sequential_read", "顺序读 (256 MiB)", "MiB/s"),
    ("random_read_4k", "随机 4K 读 (1,000 次)", "MiB/s"),
    ("small_write_4k_fsync", "4K 小文件写 + fsync (200 个)", "ops/s"),
    ("mkdir", "目录创建 mkdir (500 个)", "ops/s"),
    ("rename", "目录重命名 rename (500 个)", "ops/s"),
    ("unlink", "文件删除 unlink (500 个)", "ops/s"),
    ("readdir_small", "小目录列举 readdir (20 项)", "ops/s"),
    ("readdir_large", "大目录列举 readdir (500 项)", "ops/s"),
    ("stat", "文件属性查询 stat (500 个)", "ops/s"),
]

MACRO_WORKLOADS = [
    ("macro_git_clone", "Git Clone (--no-local · 200 文件 · 20 MiB)", "s"),
    ("macro_git_status_dirty", "Git Status (--porcelain · dirty 工作区)", "s"),
    ("macro_git_diff_dirty", "Git Diff (--name-only · dirty 工作区)", "s"),
    ("macro_find_files", "目录树遍历 (find . -type f | wc -l)", "s"),
    ("macro_go_build", "Go 编译构建 (go build ./...)", "s"),
]


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
            try:
                val = float(m.group(2))
                cost = float(m.group(4))
            except ValueError:
                continue
            results[item] = {
                "value": val,
                "value_unit": m.group(3),
                "cost": cost,
                "cost_unit": m.group(5),
            }
    return results


def load_json(path: Path) -> dict[str, Any]:
    """安全加载 JSON 字典，空文件、畸形 JSON 或非字典对象返回空字典。"""
    if not path.exists() or path.stat().st_size == 0:
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def fmt_throughput_ratio(bowl_val: float | None, other_val: float | None) -> str:
    """Format throughput ratio where higher is better (bowl / other)."""
    if bowl_val is None or other_val is None:
        return "—"
    b_num = float(bowl_val)
    o_num = float(other_val)
    if not (math.isfinite(b_num) and math.isfinite(o_num)) or o_num <= 0:
        return "—"
    return f"{b_num / o_num:.2f}x"


def fmt_latency_speedup(bowl_sec: float | None, other_sec: float | None) -> str:
    """Format latency speedup where lower is better (other_sec / bowl_sec)."""
    if bowl_sec is None or other_sec is None:
        return "—"
    b_num = float(bowl_sec)
    o_num = float(other_sec)
    if not (math.isfinite(b_num) and math.isfinite(o_num)) or b_num <= 0:
        return "—"
    return f"{o_num / b_num:.2f}x"


def build_summary(results_dir: Path) -> dict[str, Any]:
    env_meta = load_json(results_dir / "env_metadata.json")
    bench_data: dict[str, Any] = {}
    perf_baseline_data: dict[str, Any] = {}

    for target_key, _ in TARGETS:
        bench_data[target_key] = parse_juicefs_bench(results_dir / f"bench_{target_key}.log")
        perf_baseline_data[target_key] = load_json(results_dir / f"perf_baseline_{target_key}.json")

    d9_cold = load_json(results_dir / "drive9_fuse_bench_cold.json")
    d9_hot = load_json(results_dir / "drive9_fuse_bench_hot.json")

    stage_timings: list[dict[str, Any]] = []
    timings_path = results_dir / "stage_timings.jsonl"
    if timings_path.exists():
        for line in timings_path.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                stage_timings.append(row)

    out: dict[str, Any] = {
        "env_metadata": env_meta,
        "juicefs_bench": bench_data,
        "drive9_fuse_bench_cold": d9_cold,
        "drive9_fuse_bench_hot": d9_hot,
        "fuse_perf_baseline": perf_baseline_data,
        "stage_timings": stage_timings,
    }
    telem = load_telemetry_overview(results_dir)
    if telem:
        out["telemetry_overview"] = telem
    return out


def render_report(summary: dict[str, Any]) -> str:
    env = summary.get("env_metadata", {})
    cloud = env.get("cloud", {})
    srv = env.get("server_node", {})
    cli = env.get("client_node", {})
    net = env.get("network_baseline", {})
    disk_base = env.get("server_disk_baseline", {})
    bowl_cfg = env.get("bowl_config", {})
    d9_cfg = env.get("drive9_config", {})
    audit = env.get("cleanup_audit", {})

    bench = summary.get("juicefs_bench") or {}
    d9_cold = (summary.get("drive9_fuse_bench_cold") or {}).get("targets") or {}
    d9_hot = (summary.get("drive9_fuse_bench_hot") or {}).get("targets") or {}
    pbase = summary.get("fuse_perf_baseline") or {}

    b_bench = bench.get("bowl") or {}
    d9d_bench = bench.get("drive9_default") or {}
    d9s_bench = bench.get("drive9_sync") or {}

    def get_bench_val(d: dict, k: str) -> float | None:
        return (d.get(k) or {}).get("value")

    def get_d9_median(targets_dict: dict, target_key: str, workload: str) -> float | None:
        res = ((targets_dict.get(target_key) or {}).get("results") or {}).get(workload) or {}
        val = res.get("median")
        return float(val) if val is not None else None

    def get_d9_p95(targets_dict: dict, target_key: str, workload: str) -> float | None:
        res = ((targets_dict.get(target_key) or {}).get("results") or {}).get(workload) or {}
        val = res.get("p95")
        return float(val) if val is not None else None

    def get_pbase_work(target_key: str, workload: str) -> dict[str, Any]:
        return ((pbase.get(target_key) or {}).get("workloads") or {}).get(workload) or {}

    lines: list[str] = []
    lines.append("# Bowl vs Drive9 (mem9-ai/drive9) — 阿里云硅谷双机内网性能基准测试报告")
    lines.append("")
    for note in env.get("execution_notes", []):
        lines.extend([f"> {note}", ""])
    lines.append("## 1. 执行摘要（Executive Summary）")
    lines.append("")
    lines.append(
        "本报告记录了 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器"
        "（1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下依次运行 "
        "**`juicefs bench` 基础测试套件**、**Drive9 内置 [`drive9_fuse_bench.py`](drive9_fuse_bench.py) 微观与宏观基准套件（冷缓存 + 热缓存）** 以及 "
        "**Drive9 内置 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)（SHA-256 完整性校验读写 + SQLite `DELETE`/`WAL` 事务基准）** 的实测对比结果。"
    )
    lines.append("")
    lines.append("三组目标的客户端缓存和写入确认条件不同；本次在同台机器上依次测试默认与同步模式：")
    lines.append("")
    bowl_pg_ver = srv.get("bowl_pg_version") or srv.get("pg_version", "PostgreSQL")
    bowl_cp_label = "PostgreSQL 18（deploy/prod 容器栈）" if bowl_cfg.get("deploy_profile") else "PostgreSQL 16"

    lines.append(
        f"1. **Bowl (`bowl`)**：{bowl_cp_label}仅保存单张 `disks` 控制面表；"
        "每个 `Disk` 由单个常驻 `StoreActor`（单写者串行执行器 + Pebble 引擎）承载，"
        "保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef`（`PackID + Offset + Length`）索引；"
        "文件数据追加写入 `packs/<pack_id_hex16>.pack` 的 64 MiB Append-Only Pack 文件中（`256 KiB` 逻辑块网格、`AppendBlocks` 整块零冗余拷贝、连续块合并单次 `pread`、`8 MiB` `SyncFileRange` 渐进回写、后台异步死包回收、`WriteRequest`/`ReadResponse` 池化零分配编解码与 `64 MiB` 分片热块缓存），"
        "常规写批次以 `pebble.NoSync` 提交异步 WAL，显式 `Fsync`/`Sync` 触发 Pack `fdatasync` + Pebble WAL `LogData(Sync)` 双层并行落盘屏障。"
        "客户端挂载时执行单次 `MountDisk` 握手获取 `WriterEpoch`（无后台心跳），禁用内核 `CAP_WRITEBACK_CACHE`，"
        "配备客户端 `1+8` H2C 多连接条带池（`32M / 8×64M`）与服务端 `128 MiB` H2C 窗口、`MarshalAppend` 零分配编码、"
        "`WriteBehindCoordinator`（`4 MiB × 4` 并发写流水线，`2 MiB` 同步探测 + `Create` 预热与小块连续写内存聚合）、"
        "`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4, ReadAheadMaxGlobalInflight=32`）、`completeDirs` 本地目录流合成、"
        "`FOPEN_KEEP_CACHE` + `CAP_HANDLE_KILLPRIV` 负缓存 + 写后 `AttrTimeout=1.0s` + `MaxBackground=64`。"
    )
    lines.append("2. **Drive9 默认模式 (`drive9_default`)**：TiDB v8.5.5 + Drive9 Server Local S3（全部落于 Server 本地 NVMe SSD `/mnt/nvme`），客户端挂载参数为 `--profile=coding-agent --durability=interactive --write-cache-size-mb=0`（启用本地 NVMe SSD Write-Back 异步后台提交、128 MiB 内存读缓存 + 1024 MiB 本地 NVMe 磁盘读缓存、`30s` 属性缓存，以及 `.git`/构建产物本地 Overlay）。")
    lines.append("3. **Drive9 远端同步模式 (`drive9_sync`)**：与 `drive9_default` 连接同台 Drive9 Server，但客户端挂载参数设为 `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`（禁用本地 Overlay，对齐 `1.0s` 内核缓存，并在每次 `close`/`fsync` 时同步提交至远端服务器）。")
    lines.append("")

    # Section 1.1 Headline Table
    lines.append("### 1.1 核心指标同机实测总览表")
    lines.append("")
    lines.append("| 测试套件 | 指标项 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    headline_rows = [
        (
            "`juicefs bench` (-p 4)",
            "大文件并发写 (1GiB×4)",
            get_bench_val(b_bench, "Write big file"),
            get_bench_val(d9d_bench, "Write big file"),
            get_bench_val(d9s_bench, "Write big file"),
            "MiB/s",
            "throughput",
        ),
        (
            "`juicefs bench` (-p 4)",
            "大文件并发读 (1GiB×4)",
            get_bench_val(b_bench, "Read big file"),
            get_bench_val(d9d_bench, "Read big file"),
            get_bench_val(d9s_bench, "Read big file"),
            "MiB/s",
            "throughput",
        ),
        (
            "`juicefs bench` (-p 4)",
            "小文件并发写 (128KiB×400)",
            get_bench_val(b_bench, "Write small file"),
            get_bench_val(d9d_bench, "Write small file"),
            get_bench_val(d9s_bench, "Write small file"),
            "files/s",
            "throughput",
        ),
        (
            "`juicefs bench` (-p 4)",
            "小文件并发读 (128KiB×400)",
            get_bench_val(b_bench, "Read small file"),
            get_bench_val(d9d_bench, "Read small file"),
            get_bench_val(d9s_bench, "Read small file"),
            "files/s",
            "throughput",
        ),
        (
            "`juicefs bench` (-p 4)",
            "小文件并发 Stat (400 files)",
            get_bench_val(b_bench, "Stat file"),
            get_bench_val(d9d_bench, "Stat file"),
            get_bench_val(d9s_bench, "Stat file"),
            "files/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "顺序写 (256 MiB + fsync)",
            get_d9_median(d9_cold, "bowl", "sequential_write"),
            get_d9_median(d9_cold, "drive9_default", "sequential_write"),
            get_d9_median(d9_cold, "drive9_sync", "sequential_write"),
            "MiB/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "顺序读 (256 MiB)",
            get_d9_median(d9_cold, "bowl", "sequential_read"),
            get_d9_median(d9_cold, "drive9_default", "sequential_read"),
            get_d9_median(d9_cold, "drive9_sync", "sequential_read"),
            "MiB/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "4K 小文件写 + fsync (200 个)",
            get_d9_median(d9_cold, "bowl", "small_write_4k_fsync"),
            get_d9_median(d9_cold, "drive9_default", "small_write_4k_fsync"),
            get_d9_median(d9_cold, "drive9_sync", "small_write_4k_fsync"),
            "ops/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "目录创建 mkdir (500 个)",
            get_d9_median(d9_cold, "bowl", "mkdir"),
            get_d9_median(d9_cold, "drive9_default", "mkdir"),
            get_d9_median(d9_cold, "drive9_sync", "mkdir"),
            "ops/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "目录重命名 rename (500 个)",
            get_d9_median(d9_cold, "bowl", "rename"),
            get_d9_median(d9_cold, "drive9_default", "rename"),
            get_d9_median(d9_cold, "drive9_sync", "rename"),
            "ops/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "文件删除 unlink (500 个)",
            get_d9_median(d9_cold, "bowl", "unlink"),
            get_d9_median(d9_cold, "drive9_default", "unlink"),
            get_d9_median(d9_cold, "drive9_sync", "unlink"),
            "ops/s",
            "throughput",
        ),
        (
            "`drive9_fuse_bench` (Cold)",
            "Git Clone (200 文件 · 20 MiB, 耗时越低越好)",
            get_d9_median(d9_cold, "bowl", "macro_git_clone"),
            get_d9_median(d9_cold, "drive9_default", "macro_git_clone"),
            get_d9_median(d9_cold, "drive9_sync", "macro_git_clone"),
            "s",
            "latency",
        ),
        (
            "SQLite Baseline (`WAL`)",
            "SQLite WAL 事务插入 (256 rows)",
            get_pbase_work("bowl", "sqlite_wal_insert_transaction").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_wal_insert_transaction").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_wal_insert_transaction").get("rows_per_second"),
            "rows/s",
            "throughput",
        ),
    ]

    for cat, label, vb, vd, vs, unit, kind in headline_rows:
        sb = f"{vb:,.2f} {unit}" if vb is not None else "—"
        sd = f"{vd:,.2f} {unit}" if vd is not None else "—"
        ss = f"{vs:,.2f} {unit}" if vs is not None else "—"
        if kind == "latency":
            r_d = fmt_latency_speedup(vb, vd)
            r_s = fmt_latency_speedup(vb, vs)
        else:
            r_d = fmt_throughput_ratio(vb, vd)
            r_s = fmt_throughput_ratio(vb, vs)
        lines.append(f"| {cat} | **{label}** | `{sb}` | `{sd}` | `{ss}` | **{r_d}** | **{r_s}** |")

    lines.append("")
    lines.append("---")
    lines.append("")
    lines.append("## 2. 阿里云硅谷双机测试拓扑与环境基线")
    lines.append("")
    lines.append("### 2.1 双机云服务器配置（按量计费实测节点）")
    lines.append("")
    lines.append("| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 系统盘 + 本地数据盘 | 操作系统与内核 | VPC 私网 IP |")
    lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    lines.append(
        f"| **Server 服务端** (`bowl-d9-server`) | `{srv.get('instance_id', '')}` | `{cloud.get('instance_type', '')}` | "
        f"{srv.get('cpu_model', '')} | {srv.get('vcpu', '')} vCPU / {srv.get('memory_gib', '')} | "
        f"`{cloud.get('system_disk_category', '')}` ({cloud.get('system_disk_size_gib', '')} GiB) + `{cloud.get('data_disk_category', 'local_ssd_pro')}` ({srv.get('data_disk_size_gib', cloud.get('data_disk_size_gib', 894))} GiB NVMe) | "
        f"{srv.get('os_release', '')} (`{srv.get('kernel', '')}`) | `{srv.get('private_ip', '')}` |"
    )
    lines.append(
        f"| **Client 客户端** (`bowl-d9-client`) | `{cli.get('instance_id', '')}` | `{cloud.get('instance_type', '')}` | "
        f"{cli.get('cpu_model', '')} | {cli.get('vcpu', '')} vCPU / {cli.get('memory_gib', '')} | "
        f"`{cloud.get('system_disk_category', '')}` ({cloud.get('system_disk_size_gib', '')} GiB) + `{cloud.get('data_disk_category', 'local_ssd_pro')}` ({cloud.get('data_disk_size_gib', 894)} GiB NVMe) | "
        f"{cli.get('os_release', '')} (`{cli.get('kernel', '')}`) | `{cli.get('private_ip', '')}` |"
    )
    lines.append("")

    lines.append("### 2.2 同可用区 VPC 内网与 Server 节点本地 NVMe SSD 物理基线")
    lines.append("")
    lines.append("| 基线指标 | 实测数值 | 采集方式与说明 |")
    lines.append("| :--- | :--- | :--- |")
    lines.append(
        f"| **VPC ID / 交换机 ID** | `{cloud.get('vpc_id', '')}` / `{cloud.get('vswitch_id', '')}` | 阿里云 `{cloud.get('zone_id', '')}` 私网网段 `172.16.1.0/24` |"
    )
    lines.append(
        f"| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `{net.get('ping_rtt_ms', '')}` | `ping -c 20 -i 0.05 {srv.get('private_ip', '')}` |"
    )
    lines.append(
        f"| **内网单流 TCP 吞吐带宽** | `{net.get('iperf3_gbps', '')}` Gbps | `iperf3 -c {srv.get('private_ip', '')} -t 5` |"
    )
    lines.append(
        f"| **Server 节点本地 NVMe SSD (`{srv.get('data_disk_device', '/dev/nvme1n1')}`) 顺序写基线** | `{disk_base.get('local_seq_write_mib_s', '')}` MiB/s | `/mnt/nvme` `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |"
    )
    lines.append(
        f"| **Server 节点本地 NVMe SSD (`{srv.get('data_disk_device', '/dev/nvme1n1')}`) 顺序读基线** | `{disk_base.get('local_seq_read_mib_s', '')}` MiB/s | `/mnt/nvme` `fio --rw=read --bs=4M --size=2G --direct=1` |"
    )
    lines.append(
        f"| **Server 节点本地 NVMe SSD 4K 随机写 / 随机读 IOPS** | `{disk_base.get('local_randwrite_4k_iops', '')}` / `{disk_base.get('local_randread_4k_iops', '')}` IOPS | `/mnt/nvme` `fio --rw=randwrite/randread --bs=4k --numjobs=4 --iodepth=32 --direct=1` |"
    )
    lines.append(
        f"| **Server 节点本地 NVMe SSD 4K 同步写 (`fsync=1`)** | `{disk_base.get('local_fsync_4k_iops', '')}` IOPS (P99 `{disk_base.get('local_fsync_4k_p99_us', '')}` µs) | `/mnt/nvme` `fio --rw=write --bs=4k --iodepth=1 --direct=1 --fsync=1` |"
    )
    lines.append("")

    lines.append("### 2.3 三组被测文件系统架构与参数对照矩阵")
    lines.append("")
    lines.append("| 维度 | Bowl (`bowl`) | Drive9 Default (`drive9_default`) | Drive9 Sync (`drive9_sync`) |")
    lines.append("| :--- | :--- | :--- | :--- |")
    lines.append(
        f"| **代码版本 (Git SHA)** | Bowl `{bowl_cfg.get('git_commit', '')}` | Drive9 `{d9_cfg.get('git_commit', '')}` | Drive9 `{d9_cfg.get('git_commit', '')}` |"
    )
    lines.append(
        f"| **控制面 / 文件元数据** | {bowl_pg_ver} 控制面 / 每盘单写者 `StoreActor` (Pebble) | TiDB `{srv.get('tidb_version', 'v8.5.5')}` Unistore (`127.0.0.1:4000`) | TiDB `{srv.get('tidb_version', 'v8.5.5')}` Unistore (`127.0.0.1:4000`) |"
    )
    if bowl_cfg.get("deploy_profile"):
        lines.append(
            f"| **部署编排与可观测性栈** | `{bowl_cfg.get('deploy_profile')}` · `{bowl_cfg.get('observability_stack', '')}` | systemd (`tidb` + `drive9-server`) | systemd (`tidb` + `drive9-server`) |"
        )
    lines.append(
        "| **服务端数据块引擎** | 每盘 `StoreActor` Pebble + `packs/<pack_id_hex16>.pack` 64 MiB Pack 文件 (`/var/lib/bowl/data` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) | `drive9-server` 内置 Local S3 (`/var/lib/drive9/s3` -> `/mnt/nvme`) |"
    )
    lines.append(
        "| **客户端挂载参数** | `bowl mount` (生产默认 `noatime`，单次 `MountDisk` 握手) | `--profile=coding-agent --durability=interactive --write-cache-size-mb=0` | `--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s` |"
    )
    lines.append(
        "| **写入持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发写流水线 + 小块连续写聚合，`writeback_cache=false`）+ 客户端 `1+8` H2C 池 (`32M/8×64M`) / 服务端 `128M` 窗口 + 零分配编解码；Group Commit `pebble.NoSync` + `8 MiB` `SyncFileRange` 渐进回写 + Fsync 双层并行落盘屏障 | 客户端本地 NVMe SSD Write-Back 暂存，后台 `CommitQueue` 异步上传 | 客户端本地暂存后在 `fsync`/`close` 时同步阻塞上传至 Server |"
    )
    lines.append(
        "| **客户端读缓存与预读** | 无本地磁盘缓存；`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4, ReadAheadMaxGlobalInflight=32`）+ `completeDirs` 本地目录流合成 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 | 128 MiB 内存读缓存 + 1024 MiB 本地 NVMe SSD 读缓存 + 小文件整文件预取 |"
    )
    lines.append(
        "| **FUSE 内核属性/目录缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` | `attr=30s, entry=10s, dir=10s` + `.git`/构建目录 Local Overlay | `attr=1.0s, entry=1.0s, dir=1.0s`（禁用 Local Overlay） |"
    )
    lines.append("")

    # Section 3: juicefs bench
    lines.append("---")
    lines.append("")
    lines.append("## 3. 套件一：基础并发测试（`juicefs bench -p 4`）")
    lines.append("")
    lines.append(
        "使用 `juicefs bench <mountpoint> -p 4` 对三组挂载点执行完全相同的 4 并发基准测试："
        "依次执行 4×1 GiB 大文件写（`1 MiB` 块）、清理内核页缓存（`drop_caches`）、4×1 GiB 大文件读、"
        "4×100 个 `128 KiB` 小文件写、清理页缓存、400 个小文件读、清理页缓存、400 个小文件 `stat`。"
    )
    lines.append("")
    lines.append("| 测试项目 (`ITEM`) | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: |")

    for item_name, val_unit, cost_unit in BENCH_ITEMS:
        b_it = b_bench.get(item_name, {})
        d_it = d9d_bench.get(item_name, {})
        s_it = d9s_bench.get(item_name, {})

        vb = b_it.get("value")
        vd = d_it.get("value")
        vs = s_it.get("value")

        sb = f"{vb:.2f} {val_unit} ({b_it.get('cost') or 0:.2f} {cost_unit})" if vb is not None else "—"
        sd = f"{vd:.2f} {val_unit} ({d_it.get('cost') or 0:.2f} {cost_unit})" if vd is not None else "—"
        ss = f"{vs:.2f} {val_unit} ({s_it.get('cost') or 0:.2f} {cost_unit})" if vs is not None else "—"

        lines.append(
            f"| **{item_name}** | `{sb}` | `{sd}` | `{ss}` | **{fmt_throughput_ratio(vb, vd)}** | **{fmt_throughput_ratio(vb, vs)}** |"
        )
    lines.append("")

    # Section 4: drive9_fuse_bench.py
    lines.append("---")
    lines.append("")
    lines.append("## 4. 套件二：Drive9 内置 `drive9_fuse_bench.py` 微观与宏观基准测试")
    lines.append("")
    lines.append("### 4.1 冷缓存（Cold Cache，`sync; echo 3 > /proc/sys/vm/drop_caches`）对比")
    lines.append("")
    lines.append("#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）")
    lines.append("")
    lines.append("| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    for w_key, w_label, unit in MICRO_WORKLOADS:
        vb = get_d9_median(d9_cold, "bowl", w_key)
        pb = get_d9_p95(d9_cold, "bowl", w_key)
        vd = get_d9_median(d9_cold, "drive9_default", w_key)
        pd = get_d9_p95(d9_cold, "drive9_default", w_key)
        vs = get_d9_median(d9_cold, "drive9_sync", w_key)
        ps = get_d9_p95(d9_cold, "drive9_sync", w_key)

        sb = f"{vb:,.2f} ({pb:,.2f}) {unit}" if vb is not None and pb is not None else "—"
        sd = f"{vd:,.2f} ({pd:,.2f}) {unit}" if vd is not None and pd is not None else "—"
        ss = f"{vs:,.2f} ({ps:,.2f}) {unit}" if vs is not None and ps is not None else "—"

        lines.append(
            f"| `{w_key}` | **{w_label}** | `{sb}` | `{sd}` | `{ss}` | **{fmt_throughput_ratio(vb, vd)}** | **{fmt_throughput_ratio(vb, vs)}** |"
        )
    lines.append("")

    lines.append("#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）")
    lines.append("")
    lines.append("| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    for w_key, w_label, unit in MACRO_WORKLOADS:
        vb = get_d9_median(d9_cold, "bowl", w_key)
        pb = get_d9_p95(d9_cold, "bowl", w_key)
        vd = get_d9_median(d9_cold, "drive9_default", w_key)
        pd = get_d9_p95(d9_cold, "drive9_default", w_key)
        vs = get_d9_median(d9_cold, "drive9_sync", w_key)
        ps = get_d9_p95(d9_cold, "drive9_sync", w_key)

        sb = f"{vb:.3f}s ({pb:.3f}s)" if vb is not None and pb is not None else "—"
        sd = f"{vd:.3f}s ({pd:.3f}s)" if vd is not None and pd is not None else "—"
        ss = f"{vs:.3f}s ({ps:.3f}s)" if vs is not None and ps is not None else "—"

        lines.append(
            f"| `{w_key}` | **{w_label}** | `{sb}` | `{sd}` | `{ss}` | **{fmt_latency_speedup(vb, vd)}** | **{fmt_latency_speedup(vb, vs)}** |"
        )
    lines.append("")

    lines.append("### 4.2 热缓存（Hot Cache，预热后直接测量）对比")
    lines.append("")
    lines.append("#### (A) 微观吞吐量负载（Micro Workloads · 数值越大越好）")
    lines.append("")
    lines.append("| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    for w_key, w_label, unit in MICRO_WORKLOADS:
        vb = get_d9_median(d9_hot, "bowl", w_key)
        pb = get_d9_p95(d9_hot, "bowl", w_key)
        vd = get_d9_median(d9_hot, "drive9_default", w_key)
        pd = get_d9_p95(d9_hot, "drive9_default", w_key)
        vs = get_d9_median(d9_hot, "drive9_sync", w_key)
        ps = get_d9_p95(d9_hot, "drive9_sync", w_key)

        sb = f"{vb:,.2f} ({pb:,.2f}) {unit}" if vb is not None and pb is not None else "—"
        sd = f"{vd:,.2f} ({pd:,.2f}) {unit}" if vd is not None and pd is not None else "—"
        ss = f"{vs:,.2f} ({ps:,.2f}) {unit}" if vs is not None and ps is not None else "—"

        lines.append(
            f"| `{w_key}` | **{w_label}** | `{sb}` | `{sd}` | `{ss}` | **{fmt_throughput_ratio(vb, vd)}** | **{fmt_throughput_ratio(vb, vs)}** |"
        )
    lines.append("")

    lines.append("#### (B) 宏观研发工作流负载（Macro Workloads · 耗时越短越好，比值为加速比 `Drive9耗时 / Bowl耗时`）")
    lines.append("")
    lines.append("| 工作负载 (`Workload`) | 指标说明 | Bowl Median (P95) | Drive9 Default Median (P95) | Drive9 Sync Median (P95) | Bowl 加速比 vs Default | Bowl 加速比 vs Sync |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    for w_key, w_label, unit in MACRO_WORKLOADS:
        vb = get_d9_median(d9_hot, "bowl", w_key)
        pb = get_d9_p95(d9_hot, "bowl", w_key)
        vd = get_d9_median(d9_hot, "drive9_default", w_key)
        pd = get_d9_p95(d9_hot, "drive9_default", w_key)
        vs = get_d9_median(d9_hot, "drive9_sync", w_key)
        ps = get_d9_p95(d9_hot, "drive9_sync", w_key)

        sb = f"{vb:.3f}s ({pb:.3f}s)" if vb is not None and pb is not None else "—"
        sd = f"{vd:.3f}s ({pd:.3f}s)" if vd is not None and pd is not None else "—"
        ss = f"{vs:.3f}s ({ps:.3f}s)" if vs is not None and ps is not None else "—"

        lines.append(
            f"| `{w_key}` | **{w_label}** | `{sb}` | `{sd}` | `{ss}` | **{fmt_latency_speedup(vb, vd)}** | **{fmt_latency_speedup(vb, vs)}** |"
        )
    lines.append("")

    # Section 5: fuse_perf_baseline.py
    lines.append("---")
    lines.append("")
    lines.append("## 5. 套件三：Drive9 E2E 完整性校验读写与 SQLite (`DELETE` / `WAL`) 事务基准")
    lines.append("")
    lines.append(
        "运行提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 [`fuse_perf_baseline.py`](fuse_perf_baseline.py)："
        "包含 64 个 `1 KiB` 小文件写入（含每个文件 `fsync` + 父目录 `fsync`）与 SHA-256 校验读、"
        "`16 MiB` 大文件写入与 2 轮 SHA-256 校验读，以及 SQLite 在 `journal_mode=DELETE` 与 `journal_mode=WAL`（`PRAGMA synchronous=FULL`）下的 256 行事务插入、86 行事务更新、全表聚合校验读、`PRAGMA integrity_check` 与 `PRAGMA wal_checkpoint(TRUNCATE)`。"
    )
    lines.append("")
    lines.append("| 测试工作负载 (`Workload`) | 指标单位 | Bowl (Pebble) | Drive9 Default (`interactive`) | Drive9 Sync (`close-sync`) | Bowl vs Drive9 Default | Bowl vs Drive9 Sync |")
    lines.append("| :--- | :--- | ---: | ---: | ---: | ---: | ---: |")

    def get_large_pass_mib(target_key: str, idx: int) -> float | None:
        passes = get_pbase_work(target_key, "large_file_reads")
        if isinstance(passes, list) and len(passes) > idx:
            return passes[idx].get("mib_per_second")
        return None

    pbase_rows = [
        (
            "64×1KiB 小文件写 (`fsync` + dir `fsync`)",
            "files/s",
            get_pbase_work("bowl", "small_file_write").get("files_per_second"),
            get_pbase_work("drive9_default", "small_file_write").get("files_per_second"),
            get_pbase_work("drive9_sync", "small_file_write").get("files_per_second"),
            "throughput",
        ),
        (
            "64×1KiB 小文件 SHA-256 校验读",
            "files/s",
            get_pbase_work("bowl", "small_file_read").get("files_per_second"),
            get_pbase_work("drive9_default", "small_file_read").get("files_per_second"),
            get_pbase_work("drive9_sync", "small_file_read").get("files_per_second"),
            "throughput",
        ),
        (
            "16 MiB 大文件写 (`fsync` + dir `fsync`)",
            "MiB/s",
            get_pbase_work("bowl", "large_file_write").get("mib_per_second"),
            get_pbase_work("drive9_default", "large_file_write").get("mib_per_second"),
            get_pbase_work("drive9_sync", "large_file_write").get("mib_per_second"),
            "throughput",
        ),
        (
            "16 MiB 大文件 SHA-256 校验读 (Pass 1)",
            "MiB/s",
            get_large_pass_mib("bowl", 0),
            get_large_pass_mib("drive9_default", 0),
            get_large_pass_mib("drive9_sync", 0),
            "throughput",
        ),
        (
            "16 MiB 大文件 SHA-256 校验读 (Pass 2)",
            "MiB/s",
            get_large_pass_mib("bowl", 1),
            get_large_pass_mib("drive9_default", 1),
            get_large_pass_mib("drive9_sync", 1),
            "throughput",
        ),
        (
            "SQLite `DELETE` 模式：事务插入 (256 rows)",
            "rows/s",
            get_pbase_work("bowl", "sqlite_insert_transaction").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_insert_transaction").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_insert_transaction").get("rows_per_second"),
            "throughput",
        ),
        (
            "SQLite `DELETE` 模式：事务更新 (86 rows)",
            "rows/s",
            get_pbase_work("bowl", "sqlite_update_transaction").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_update_transaction").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_update_transaction").get("rows_per_second"),
            "throughput",
        ),
        (
            "SQLite `DELETE` 模式：全表聚合校验读 (256 rows)",
            "rows/s",
            get_pbase_work("bowl", "sqlite_read_aggregate").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_read_aggregate").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_read_aggregate").get("rows_per_second"),
            "throughput",
        ),
        (
            "SQLite `WAL` 模式：事务插入 (256 rows)",
            "rows/s",
            get_pbase_work("bowl", "sqlite_wal_insert_transaction").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_wal_insert_transaction").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_wal_insert_transaction").get("rows_per_second"),
            "throughput",
        ),
        (
            "SQLite `WAL` 模式：事务更新 (86 rows)",
            "rows/s",
            get_pbase_work("bowl", "sqlite_wal_update_transaction").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_wal_update_transaction").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_wal_update_transaction").get("rows_per_second"),
            "throughput",
        ),
        (
            "SQLite `WAL` 模式：全表聚合校验读 (256 rows)",
            "rows/s",
            get_pbase_work("bowl", "sqlite_wal_read_aggregate").get("rows_per_second"),
            get_pbase_work("drive9_default", "sqlite_wal_read_aggregate").get("rows_per_second"),
            get_pbase_work("drive9_sync", "sqlite_wal_read_aggregate").get("rows_per_second"),
            "throughput",
        ),
        (
            "SQLite `WAL` 模式：`wal_checkpoint(TRUNCATE)` 耗时",
            "s",
            get_pbase_work("bowl", "sqlite_wal_checkpoint_truncate").get("seconds"),
            get_pbase_work("drive9_default", "sqlite_wal_checkpoint_truncate").get("seconds"),
            get_pbase_work("drive9_sync", "sqlite_wal_checkpoint_truncate").get("seconds"),
            "latency",
        ),
    ]

    for label, unit, vb, vd, vs, kind in pbase_rows:
        if unit == "s":
            sb = f"{vb:.4f} s" if vb is not None else "—"
            sd = f"{vd:.4f} s" if vd is not None else "—"
            ss = f"{vs:.4f} s" if vs is not None else "—"
            rd = fmt_latency_speedup(vb, vd)
            rs = fmt_latency_speedup(vb, vs)
        else:
            sb = f"{vb:,.2f} {unit}" if vb is not None else "—"
            sd = f"{vd:,.2f} {unit}" if vd is not None else "—"
            ss = f"{vs:,.2f} {unit}" if vs is not None else "—"
            rd = fmt_throughput_ratio(vb, vd)
            rs = fmt_throughput_ratio(vb, vs)
        lines.append(f"| **{label}** | `{unit}` | `{sb}` | `{sd}` | `{ss}` | **{rd}** | **{rs}** |")
    lines.append("")

    # 本节从当轮结果生成，历史数值不能作为当前结果输出。
    lines.extend(["---", "", "## 6. 本轮观测与比较边界", ""])
    for item, unit, _ in BENCH_ITEMS:
        value = get_bench_val(b_bench, item)
        other = get_bench_val(d9s_bench, item)
        if value is not None and other is not None:
            lines.append(f"- `{item}`：Bowl `{value:.2f} {unit}`，Drive9 Sync `{other:.2f} {unit}`，吞吐比 `{fmt_throughput_ratio(value, other)}`。")
    lines.append("")
    lines.append("Drive9 Default 使用 `interactive`，Drive9 Sync 使用 `close-sync`，两者的写入确认条件不同。表内数值保留各自配置，吞吐比较不等于持久化契约相同。")
    lines.append("")
    bench_write = get_bench_val(b_bench, "Write big file")
    seq_write = get_d9_median(d9_cold, "bowl", "sequential_write")
    if bench_write is not None and seq_write is not None:
        if bench_write < seq_write:
            verdict = "4 并发聚合吞吐低于单流，说明并发写路径存在额外开销，需单独剖析；不能把 `juicefs bench` 数值当作 Bowl 的单流写上限。"
        else:
            verdict = "4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。"
        lines.append(
            f"Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `{bench_write:.2f} MiB/s`，同轮冷缓存单流 256 MiB 顺序写（含 `fsync`）为 `{seq_write:.2f} MiB/s`。"
            + verdict
        )
        lines.append("")
    lines.append("冷缓存准备命令同时清理 Client 与 Server 双端内核页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`），且各目标栈在 Server 端按进程隔离启停；但未在每轮内部重启 FUSE 进程或清空 Drive9 用户态内存缓存。因此本报告的 cold 表示双端 OS 页缓存清空下的冷状态。")
    lines.append("")
    lines.append("微观与宏观工作负载各运行 3 次，表中保留中位数、P95 与原始样本；3 个样本不足以估计稳定的尾延迟或统计显著性。")
    lines.append("")

    lines.append("本轮架构演进同时引入每盘单写者 `StoreActor`、64 MiB Pack 文件引擎与单次 `MountDisk` 握手；单轮数据反映当前 `labs/actor-architecture` 分支整体表现，本套件未测试整机断电恢复。")
    lines.append("")

    telem_overview = summary.get("telemetry_overview") or {}
    if telem_overview:
        lines.extend(
            render_report_telemetry_section(
                telem_overview, "## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）"
            )
        )
        audit_sec_no = 8
    else:
        audit_sec_no = 7

    # Cloud Resource Audit
    lines.append("---")
    lines.append("")
    lines.append(f"## {audit_sec_no}. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）")
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
    parser = argparse.ArgumentParser(description="Parse Bowl vs Drive9 benchmark results.")
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
