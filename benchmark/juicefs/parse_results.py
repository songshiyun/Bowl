#!/usr/bin/env python3
"""Parse Bowl vs JuiceFS benchmark results (juicefs bench, fio, mdtest) into summary.json and REPORT.md."""

from __future__ import annotations

import argparse
import json
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
    ("jfs_pg", "JuiceFS (PG 16 + rclone S3)"),
    ("jfs_redis", "JuiceFS (Redis 7 + rclone S3)"),
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
    # fio 即使失败也输出吞吐；错误结果不能进入成功指标表。
    if any(job.get("error", 0) != 0 for job in jobs):
        return {}
    job = jobs[0]
    stat = job.get(direction, {})
    bw_bytes = float(stat.get("bw_bytes", stat.get("bw", 0) * 1024))
    bw_mib_s = round(bw_bytes / (1024.0 * 1024.0), 2)
    iops = round(float(stat.get("iops", 0.0)), 2)
    runtime_ms = int(stat.get("runtime", job.get("job_runtime", 0)))
    io_bytes = int(stat.get("io_bytes", 0))

    lat_ns = stat.get("lat_ns") or {}
    slat_ns = stat.get("slat_ns") or {}
    clat_ns = stat.get("clat_ns") or {}

    slat_mean = float(slat_ns.get("mean", 0.0))
    clat_mean = float(clat_ns.get("mean", 0.0))
    lat_mean = float(lat_ns.get("mean", 0.0))
    total_mean_ns = lat_mean if lat_mean > 0 else (slat_mean + clat_mean)
    mean_ms = round(total_mean_ns / 1e6, 3)

    lat_pct = lat_ns.get("percentile") or {}
    if lat_pct:
        p95_ms = round(float(lat_pct["95.000000"]) / 1e6, 3) if "95.000000" in lat_pct else None
        p99_ms = round(float(lat_pct["99.000000"]) / 1e6, 3) if "99.000000" in lat_pct else None
    elif slat_mean > clat_mean:
        # 旧版 fio JSON 未开启 --lat_percentiles=1，且 libaio+direct=1 下真实耗时落在 slat_ns，
        # 严禁用微秒级 clat_ns 分位数冒充总延迟。
        p95_ms = None
        p99_ms = None
    else:
        clat_pct = clat_ns.get("percentile") or {}
        p95_ms = round(float(clat_pct["95.000000"]) / 1e6, 3) if "95.000000" in clat_pct else None
        p99_ms = round(float(clat_pct["99.000000"]) / 1e6, 3) if "99.000000" in clat_pct else None

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

    out: dict[str, Any] = {
        "env_metadata": env_meta,
        "juicefs_bench": bench_data,
        "fio": fio_data,
        "mdtest_1proc": mdtest_1proc,
        "mdtest_4proc": mdtest_4proc,
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
    jfs_cfg = env.get("juicefs_config", {})
    audit = env.get("cleanup_audit", {})

    bench = summary.get("juicefs_bench", {})
    fio = summary.get("fio", {})
    md1 = summary.get("mdtest_1proc", {})
    md4 = summary.get("mdtest_4proc", {})

    lines: list[str] = []
    lines.append("# Bowl vs JuiceFS 分布式文件系统 — 阿里云硅谷双机内网性能基准测试报告")
    lines.append("")
    for note in env.get("execution_notes", []):
        lines.extend([f"> {note}", ""])
    lines.append("## 1. 执行摘要（Executive Summary）")
    lines.append("")
    lines.append(
        "本报告记录了 **Bowl** 与 **JuiceFS** 在阿里云硅谷地域（`us-west-1a`）使用按量计费本地 NVMe SSD 双机环境"
        "（`ecs.i5.xlarge` × 2，1 台服务端存储与元数据节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网下运行 "
        "[JuiceFS 官方常规测试套件](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 **`juicefs bench` 基础测试**、"
        "**`fio` 顺序/随机大文件吞吐量测试**、**`mdtest` 元数据 IOPS 压测**）的实测对比结果。"
    )
    lines.append("")
    lines.append("本次评测采用同规格双机（4 vCPU / 32 GiB 内存）、VPC 私网与 894 GiB `local_ssd_pro` 本地 NVMe SSD 数据盘，服务端按目标隔离运行以下三组存储栈：")
    lines.append("")
    bowl_pg_ver = srv.get("bowl_pg_version") or srv.get("pg_version", "")
    bowl_deploy_desc = (
        "deploy/prod 容器栈（PostgreSQL 18 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack + Jaeger/Prometheus/Loki 全栈可观测）"
        if bowl_cfg.get("deploy_profile")
        else "PostgreSQL 16 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack"
    )

    lines.append(
        f"1. **Bowl（{bowl_deploy_desc}）**："
        "PostgreSQL 仅保存单张 `disks` 表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；"
        "每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环"
        "（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；"
        "文件数据落在本地 NVMe SSD 的 `packs/<pack_id_hex16>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、"
        "常驻句柄池与 `64 MiB` 分片热块缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `128 MiB` H2C 连接窗口、"
        "`WriteRequest`/`ReadResponse` 池化零分配编解码、活跃包末尾同块 `ExtendTail` 顺延扩展、`8 MiB` `SyncFileRange` 渐进异步回写、"
        "相邻物理连续块合并单次 `pread` 无锁并发 `Read`、"
        "`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及后台异步全死封口包回收。"
    )
    lines.append(
        "   客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，"
        "声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；"
        "配备客户端 `1+8` H2C 多连接条带池（`32 MiB` 元数据连接 + `8×64 MiB` 数据连接，单流 `8 MiB`）与 `MarshalAppend` 零分配编码、"
        "`WriteBehindCoordinator`（大块流式写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发 Write RPC 流水线，小块连续写经首笔探针或 `Create` 预热后内存聚合）、"
        "`ReadAheadCoordinator` 单飞有序槽位预读（`W_read=4`、`ReadAheadMaxGlobalInflight=32`）与 `completeDirs` 本地目录流合成，"
        "写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。"
    )
    lines.append("2. **JuiceFS + PostgreSQL 16 (`jfs_pg`)**：使用 PostgreSQL 16 作为元数据引擎，数据块通过 VPC 内网写入 Server 节点同块本地 NVMe SSD 上的 rclone S3 服务（关闭回收站 `--trash-days 0`）。")
    lines.append("3. **JuiceFS + Redis 7 (`jfs_redis`)**：采用 JuiceFS 官方常规测试默认的 Redis 7 内存数据库（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）作为元数据引擎 + Server 节点本地 NVMe SSD rclone S3 存储（`--trash-days 0`）。")
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

    lines.append("### 1.1 核心指标本轮实测总览表")
    lines.append("")
    lines.append("| 测试维度 | 指标项 | Bowl (Pebble) | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JuiceFS(PG) | Bowl vs JuiceFS(Redis) |")
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
    lines.append("| 节点角色 | 实例 ID | 实例规格 | 处理器 (CPU) | vCPU / 内存 | 数据盘 (本地 NVMe SSD) | 系统盘 | 操作系统与内核 | VPC 私网 IP |")
    lines.append("| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |")
    lines.append(
        f"| **Server 服务端** (`bowl-bench-server`) | `{srv.get('instance_id', '')}` | `{cloud.get('instance_type', '')}` | "
        f"{srv.get('cpu_model', '')} | {srv.get('vcpu', '')} vCPU / {srv.get('memory_gib', '')} | "
        f"`{cloud.get('data_disk_category', 'local_ssd_pro')}` ({cloud.get('data_disk_size_gib', 894)} GiB `{srv.get('data_disk_device', '/dev/nvme1n1')}`) | "
        f"`{cloud.get('system_disk_category', '')}` ({cloud.get('system_disk_size_gib', '')} GiB) | "
        f"{srv.get('os_release', '')} (`{srv.get('kernel', '')}`) | `{srv.get('private_ip', '')}` |"
    )
    lines.append(
        f"| **Client 客户端** (`bowl-bench-client`) | `{cli.get('instance_id', '')}` | `{cloud.get('instance_type', '')}` | "
        f"{cli.get('cpu_model', '')} | {cli.get('vcpu', '')} vCPU / {cli.get('memory_gib', '')} | "
        f"`{cloud.get('data_disk_category', 'local_ssd_pro')}` ({cloud.get('data_disk_size_gib', 894)} GiB) | "
        f"`{cloud.get('system_disk_category', '')}` ({cloud.get('system_disk_size_gib', '')} GiB) | "
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
        f"| **内网 ICMP RTT 延迟 (min / avg / max / mdev)** | `{net.get('ping_rtt_ms', '')}` ms | `ping -c 20 -i 0.05 {srv.get('private_ip', '')}` |"
    )
    lines.append(
        f"| **内网单流 TCP 吞吐带宽** | `{net.get('iperf3_gbps', '')}` Gbps | `iperf3 -c {srv.get('private_ip', '')} -t 5` |"
    )
    lines.append(
        f"| **Server 本地 NVMe SSD (894 GiB) 顺序写基线** | `{disk_base.get('local_seq_write_mib_s', '')}` MiB/s | Server 本地 `fio --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1` |"
    )
    lines.append(
        f"| **Server 本地 NVMe SSD (894 GiB) 顺序读基线** | `{disk_base.get('local_seq_read_mib_s', '')}` MiB/s | Server 本地 `fio --rw=read --bs=4M --size=2G --direct=1` |"
    )
    if disk_base.get("local_randwrite_4k_iops") is not None:
        lines.append(
            f"| **Server 本地 NVMe SSD 4 KiB 随机写 / 读 IOPS** | `{disk_base.get('local_randwrite_4k_iops', '')}` / `{disk_base.get('local_randread_4k_iops', '')}` IOPS | Server 本地 `fio --bs=4k --numjobs=4 --iodepth=32 --direct=1` |"
        )
        lines.append(
            f"| **Server 本地 NVMe SSD 4 KiB 单流 `fsync` 写** | `{disk_base.get('local_fsync_4k_iops', '')}` IOPS (P99 `{disk_base.get('local_fsync_4k_p99_us', '')}` µs) | Server 本地 `fio --bs=4k --numjobs=1 --direct=1 --fsync=1` |"
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
        f"| **控制面 / 文件元数据** | {bowl_pg_ver} 控制面 / 单盘 `StoreActor` Pebble | {srv.get('pg_version', '')} (`jfs_pg` 库) | {srv.get('redis_version', '')} (`AOF everysec`) |"
    )
    if bowl_cfg.get("deploy_profile"):
        lines.append(
            f"| **部署编排与可观测性栈** | `{bowl_cfg.get('deploy_profile')}` · `{bowl_cfg.get('observability_stack', '')}` | systemd (`postgresql` + `jfs-s3`) | systemd (`redis-server` + `jfs-s3`) |"
        )
    lines.append(
        f"| **数据块存储 (Server 节点本地 NVMe SSD)** | 单盘 Pebble + `packs/<pack_id_hex16>.pack` `64 MiB` 封包文件 (`/var/lib/bowl/data`) | rclone S3 (`http://{srv.get('private_ip', '')}:9001/jfs-pg`) | rclone S3 (`http://{srv.get('private_ip', '')}:9001/jfs-redis`) |"
    )
    lines.append(
        f"| **FUSE 内核属性/目录项缓存** | `entry=1.0s, attr=1.0s, direntry=1.0s` (生产默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) | `attr-cache=1s, entry-cache=1s, dir-entry-cache=1s` (默认) |"
    )
    lines.append(
        f"| **回写缓存与持久化路径** | `WriteBehindCoordinator`（`4 MiB × 4` 并发写流水线 + 小块连续写聚合，`writeback_cache=false`）+ 客户端 `1+8` H2C 池 (`32M/8×64M`) / 服务端 `128M` 窗口 + 零分配编解码；Group Commit `pebble.NoSync` + `8 MiB` `SyncFileRange` 渐进回写 + Fsync 双层并行落盘屏障 | 客户端回写缓存 + 对象存储上传 | 客户端回写缓存 + 对象存储上传 |"
    )
    lines.append(
        f"| **数据分片** | `64 MiB` `.pack` Append-Only 封包 + `256 KiB` 逻辑块网格 + 连续块合并单次 `pread` + `AppendBlocks` 零冗余拷贝 + `64 MiB` 热块缓存 + `ExtendTail` | `4 MiB` block / `64 MiB` chunk (默认) | `4 MiB` block / `64 MiB` chunk (默认) |"
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
    lines.append("| 测试项目 (`ITEM`) | Bowl | JuiceFS (PG 16) | JuiceFS (Redis 7) | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: |")

    for item_name, val_unit, cost_unit in BENCH_ITEMS:
        b_it = b_bench.get(item_name, {})
        pg_it = pg_bench.get(item_name, {})
        rd_it = rd_bench.get(item_name, {})
        vb = b_it.get("value")
        vpg = pg_it.get("value")
        vrd = rd_it.get("value")

        sb = f"{vb:.2f} {val_unit} ({b_it.get('cost', 0):.2f} {cost_unit})" if vb is not None else "—"
        spg = f"{vpg:.2f} {val_unit} ({pg_it.get('cost', 0):.2f} {cost_unit})" if vpg is not None else "—"
        srd = f"{vrd:.2f} {val_unit} ({rd_it.get('cost', 0):.2f} {cost_unit})" if vrd is not None else "—"
        lines.append(
            f"| **{item_name}** | `{sb}` | `{spg}` | `{srd}` | **{fmt_ratio(vb, vpg)}** | **{fmt_ratio(vb, vrd)}** |"
        )
    lines.append("")

    # Section 4: fio
    lines.append("---")
    lines.append("")
    lines.append("## 4. 套件二：大文件吞吐量与 Direct IO 测试（`fio`）")
    lines.append("")
    lines.append("### 4.1 吞吐量与 IOPS 对比总表（8 组标准负载）")
    lines.append("")
    lines.append("| 测试用例 | 参数说明 | Bowl (Pebble) 带宽 / IOPS | JuiceFS (PG 16) 带宽 / IOPS | JuiceFS (Redis 7) 带宽 / IOPS | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
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
            if not d or d.get("lat_mean_ms") is None:
                return "—"
            mean_str = f"{d['lat_mean_ms']:.2f}"
            p95_val = d.get("lat_p95_ms")
            p99_val = d.get("lat_p99_ms")
            p95_str = f"{p95_val:.2f}" if p95_val is not None else "N/A"
            p99_str = f"{p99_val:.2f}" if p99_val is not None else "N/A"
            return f"{mean_str} / {p95_str} / {p99_str} ms"

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
        "使用 [JuiceFS `mdtest` 基准测试文档](https://juicefs.com/docs/zh/community/mdtest/)的参数（分支因子 `6`、深度 `4`、每目录 `8` 个条目），比较本轮三组实测结果（单位：`ops/sec`）："
    )
    lines.append("")
    lines.append("| 元数据操作 (`Operation`) | Bowl 实测 | JuiceFS (PG 16) 实测 | JuiceFS (Redis 7) 实测 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
    lines.append("| :--- | ---: | ---: | ---: | ---: | ---: |")

    for op in MDTEST_OPS:
        vb = get_md_mean(b_md1, op)
        vpg = get_md_mean(pg_md1, op)
        vrd = get_md_mean(rd_md1, op)
        sb = f"{vb:,.1f}" if vb is not None else "—"
        spg = f"{vpg:,.1f}" if vpg is not None else "—"
        srd = f"{vrd:,.1f}" if vrd is not None else "—"

        lines.append(
            f"| **{op}** | `{sb}` | `{spg}` | `{srd}` | **{fmt_ratio(vb, vpg)}** | **{fmt_ratio(vb, vrd)}** |"
        )
    lines.append("")

    lines.append("### 5.2 4 进程 MPI 并发元数据压测（`mpirun -np 4 mdtest -b 3 -z 2 -I 20 -u`）")
    lines.append("")
    lines.append("匹配 `ecs.i5.xlarge` 的 4 vCPU 并发能力，开启 4 个 MPI 进程并发创建、查询、读取和删除各自独立目录树下的目录与文件（单位：`ops/sec`）：")
    lines.append("")
    lines.append("| 元数据操作 (`Operation`) | Bowl (Pebble) 4 并发 | JuiceFS (PG 16) 4 并发 | JuiceFS (Redis 7) 4 并发 | Bowl vs JFS(PG) | Bowl vs JFS(Redis) |")
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

    # 本节仅引用当轮结果，避免重跑时保留旧数值或未经实验验证的归因。
    lines.extend(["---", "", "## 6. 本轮观测与比较边界", ""])
    for key, label in [("seq_write_1job", "单流顺序写"),
                       ("seq_read_1job", "单流顺序读"),
                       ("seq_write_16jobs", "16 并发顺序写"),
                       ("seq_read_16jobs", "16 并发顺序读")]:
        value = get_fio_bw(b_fio, key)
        other = get_fio_bw(pg_fio, key)
        if value is not None and other is not None:
            lines.append(f"- Bowl {label}：`{value:.2f} MiB/s`；同轮 JuiceFS(PG) 为 `{other:.2f} MiB/s`。")
    lines.append("")
    lines.append("服务端本地 NVMe SSD 顺序读写与 4K IOPS 物理基线、请求大小与并发数见第 2、4 节。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`（及清理 `/var/jfsCache`），且三组存储栈服务端进程按目标隔离启停。")
    lines.append("")
    bench_write = get_bench_val(b_bench, "Write big file")
    fio_write = get_fio_bw(b_fio, "seq_write_1job")
    if bench_write is not None and fio_write is not None:
        if bench_write < fio_write:
            verdict = "4 并发聚合吞吐低于单流，说明并发写路径存在额外开销，需单独剖析；不能把 `juicefs bench` 数值当作 Bowl 的单流写上限。"
        else:
            verdict = "4 并发聚合吞吐高于单流，说明当前写入路径可从并发中获益；该对照只描述本轮实测，仍需结合缓存与持久化条件解读。"
        lines.append(
            f"Bowl 并发写对照：`juicefs bench` 4 并发 1 GiB 大文件写为 `{bench_write:.2f} MiB/s`，同轮 `fio` 单流 4 GiB 顺序写为 `{fio_write:.2f} MiB/s`。"
            + verdict
        )
        lines.append("")
    lines.append("Bowl 的写入和同步路径使用单盘 `StoreActor` Pebble 与 `64 MiB` `.pack` 封包文件；本套件未测试整机断电恢复，吞吐结果不能证明断电后的数据完整性。各存储栈的缓存与持久化路径不同，性能数值应结合各自配置解读。")
    lines.append("")
    lines.append("`juicefs bench` 内部清理客户端内核页缓存，但不清理 JuiceFS 客户端本地块缓存与服务端页缓存；同轮小文件与大文件读取包含缓存行为差异。套件之间的双端缓存清理不能改变 `juicefs bench` 单次运行内部这一边界。")
    lines.append("")
    lines.append("`mdtest` 4 进程的属性查询包含 FUSE 缓存命中。该测试与单进程树测试规模不同，不能仅按两者吞吐比推导并发扩展能力。")
    lines.append("")

    lines.append('本轮架构演进同时改变控制面协议、单盘 Actor 并发模型与 64 MiB Pack 数据层；单轮数据不能将变化归因于其中某一项。')
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
