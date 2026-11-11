#!/usr/bin/env python3
"""Aggregate and distill Bowl prod observability artifacts into results/telemetry/."""

from __future__ import annotations

import argparse
import json
import math
import re
import shutil
import subprocess
from pathlib import Path
from typing import Any

# Client OpSnapshot.Buckets upper bounds in microseconds: 2^0 .. 2^19 us (non-cumulative)
CLIENT_HIST_UPPER_US: list[float] = [float(1 << i) for i in range(20)]

PROM_LINE_RE = re.compile(
    r"^(?P<name>[a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(?P<labels>[^}]*)\})?\s+(?P<val>[-+0-9.eE]+|NaN|\+Inf|-Inf)(?:\s+\d+)?$"
)
PROM_LABEL_RE = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"')


def load_json_safe(path: Path) -> Any:
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8", errors="replace"))
    except Exception:
        return None


# ==============================================================================
# 1. Client `bowl stats` Snapshot & Phase Delta Analysis
# ==============================================================================


def noncum_quantile_us(buckets: list[int], total_calls: int, q: float) -> float:
    """Compute approximate quantile (in us) from non-cumulative log2 microsecond buckets."""
    if total_calls <= 0 or not buckets:
        return 0.0
    target = math.ceil(total_calls * q)
    cum = 0
    prev_upper = 0.0
    for idx, count in enumerate(buckets):
        upper = CLIENT_HIST_UPPER_US[idx] if idx < len(CLIENT_HIST_UPPER_US) else CLIENT_HIST_UPPER_US[-1] * 2.0
        if count > 0 and cum + count >= target:
            frac = (target - cum) / count
            return round(prev_upper + frac * (upper - prev_upper), 2)
        cum += count
        prev_upper = upper
    # Overflow beyond 524,288 us bucket
    return round(CLIENT_HIST_UPPER_US[-1], 2)


def summarize_op_entry(op_obj: dict[str, Any], total_fuse_ns: int) -> dict[str, Any]:
    calls = int(op_obj.get("calls", 0))
    errors = int(op_obj.get("errors", 0))
    dur_ns = int(op_obj.get("duration_nanos", 0))
    buckets = [int(x) for x in op_obj.get("buckets", [])]
    mean_us = round((dur_ns / 1000.0) / calls, 2) if calls > 0 else 0.0
    share_pct = round((dur_ns / total_fuse_ns) * 100.0, 2) if total_fuse_ns > 0 else 0.0
    return {
        "op": op_obj.get("op", "Unknown"),
        "calls": calls,
        "errors": errors,
        "bytes": int(op_obj.get("bytes", 0)),
        "total_duration_ms": round(dur_ns / 1e6, 3),
        "share_of_fuse_time_pct": share_pct,
        "mean_us": mean_us,
        "p50_us": noncum_quantile_us(buckets, calls, 0.50),
        "p95_us": noncum_quantile_us(buckets, calls, 0.95),
        "p99_us": noncum_quantile_us(buckets, calls, 0.99),
        "errnos": op_obj.get("errnos") or {},
    }


def diff_client_snapshots(pre: dict[str, Any], post: dict[str, Any]) -> dict[str, Any]:
    pre_ops = {o["op"]: o for o in pre.get("fuse", {}).get("ops", []) if "op" in o}
    post_ops = {o["op"]: o for o in post.get("fuse", {}).get("ops", []) if "op" in o}

    delta_ops_raw: list[dict[str, Any]] = []
    total_delta_ns = 0
    for op_name, post_o in post_ops.items():
        pre_o = pre_ops.get(op_name, {})
        d_calls = max(0, int(post_o.get("calls", 0)) - int(pre_o.get("calls", 0)))
        if d_calls == 0:
            continue
        d_errs = max(0, int(post_o.get("errors", 0)) - int(pre_o.get("errors", 0)))
        d_bytes = max(0, int(post_o.get("bytes", 0)) - int(pre_o.get("bytes", 0)))
        d_ns = max(0, int(post_o.get("duration_nanos", 0)) - int(pre_o.get("duration_nanos", 0)))
        post_b = [int(x) for x in post_o.get("buckets", [])]
        pre_b = [int(x) for x in pre_o.get("buckets", [0] * len(post_b))]
        d_buckets = [max(0, post_b[i] - (pre_b[i] if i < len(pre_b) else 0)) for i in range(len(post_b))]

        post_errnos = post_o.get("errnos") or {}
        pre_errnos = pre_o.get("errnos") or {}
        d_errnos = {
            k: int(v) - int(pre_errnos.get(k, 0))
            for k, v in post_errnos.items()
            if int(v) - int(pre_errnos.get(k, 0)) > 0
        }
        total_delta_ns += d_ns
        delta_ops_raw.append({
            "op": op_name,
            "calls": d_calls,
            "errors": d_errs,
            "bytes": d_bytes,
            "duration_nanos": d_ns,
            "buckets": d_buckets,
            "errnos": d_errnos,
        })

    ops_summary = [summarize_op_entry(o, total_delta_ns) for o in delta_ops_raw]
    ops_summary.sort(key=lambda x: x["total_duration_ms"], reverse=True)

    def delta_field(section: str, key: str) -> float:
        a = float(pre.get(section, {}).get(key, 0) or 0)
        b = float(post.get(section, {}).get(key, 0) or 0)
        return max(0.0, b - a)

    attr_h, attr_m = delta_field("namespace_cache", "attr_hits"), delta_field("namespace_cache", "attr_misses")
    ent_h, ent_m = delta_field("namespace_cache", "entry_hits"), delta_field("namespace_cache", "entry_misses")
    ra_hit_b = delta_field("read_ahead", "hit_bytes")
    ra_pref_b = delta_field("read_ahead", "prefetched_bytes")

    return {
        "window_sec": round(
            max(0.0, (int(post.get("timestamp_unix_nano", 0)) - int(pre.get("timestamp_unix_nano", 0))) / 1e9), 3
        ),
        "cpu_delta_sec": round(max(0.0, float(post.get("cpu_usage_seconds", 0)) - float(pre.get("cpu_usage_seconds", 0))), 3),
        "read_bytes": int(max(0, int(post.get("fuse", {}).get("read_bytes", 0)) - int(pre.get("fuse", {}).get("read_bytes", 0)))),
        "write_bytes": int(max(0, int(post.get("fuse", {}).get("write_bytes", 0)) - int(pre.get("fuse", {}).get("write_bytes", 0)))),
        "nscache_attr_hit_rate_pct": round(attr_h / (attr_h + attr_m) * 100.0, 2) if (attr_h + attr_m) > 0 else None,
        "nscache_entry_hit_rate_pct": round(ent_h / (ent_h + ent_m) * 100.0, 2) if (ent_h + ent_m) > 0 else None,
        "readahead_hit_bytes": int(ra_hit_b),
        "readahead_prefetched_bytes": int(ra_pref_b),
        "inode_lock_contended": int(delta_field("inode_locks", "contended_acquisitions")),
        "inode_lock_wait_ms": round(delta_field("inode_locks", "wait_seconds") * 1000.0, 3),
        "top_ops_by_total_time": ops_summary[:10],
    }


def process_client_stats(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    stats_src = stage_dir / "client" / "stats"
    stats_dst = out_dir / "client_stats"
    stats_dst.mkdir(parents=True, exist_ok=True)
    if not stats_src.exists():
        return {}

    snapshots: dict[str, dict[str, Any]] = {}
    for p in sorted(stats_src.glob("*.json")):
        data = load_json_safe(p)
        if isinstance(data, dict) and "fuse" in data:
            snapshots[p.stem] = data
            shutil.copy2(p, stats_dst / p.name)
    for p in sorted(stats_src.glob("*.txt")):
        shutil.copy2(p, stats_dst / p.name)

    if not snapshots:
        return {}

    # Pick most complete terminal snapshot (before_umount > suite_post > final > latest timestamp)
    terminal_name = next(
        (cand for cand in ("before_umount", "suite_post", "final") if cand in snapshots),
        max(snapshots.keys(), key=lambda k: int(snapshots[k].get("timestamp_unix_nano", 0))),
    )
    term = snapshots[terminal_name]
    raw_ops = [o for o in term.get("fuse", {}).get("ops", []) if int(o.get("calls", 0)) > 0]
    total_ns = sum(int(o.get("duration_nanos", 0)) for o in raw_ops)
    summarized_ops = [summarize_op_entry(o, total_ns) for o in raw_ops]

    by_total = sorted(summarized_ops, key=lambda x: x["total_duration_ms"], reverse=True)
    by_p99 = sorted(summarized_ops, key=lambda x: (x["p99_us"], x["mean_us"]), reverse=True)

    phase_deltas: dict[str, Any] = {}
    for name in sorted(snapshots.keys()):
        if name.endswith("_pre"):
            base = name[: -len("_pre")]
            post_name = f"{base}_post"
            if post_name in snapshots:
                phase_deltas[base] = diff_client_snapshots(snapshots[name], snapshots[post_name])

    if phase_deltas:
        (stats_dst / "phase_deltas.json").write_text(
            json.dumps(phase_deltas, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    ns = term.get("namespace_cache", {})
    ra = term.get("read_ahead", {})
    il = term.get("inode_locks", {})
    attr_tot = int(ns.get("attr_hits", 0)) + int(ns.get("attr_misses", 0))
    entry_tot = int(ns.get("entry_hits", 0)) + int(ns.get("entry_misses", 0))

    return {
        "terminal_snapshot": terminal_name,
        "uptime_seconds": round(float(term.get("uptime_seconds", 0.0)), 3),
        "cpu_usage_seconds": round(float(term.get("cpu_usage_seconds", 0.0)), 3),
        "memory_mib": round(int(term.get("memory_bytes", 0)) / (1024 * 1024), 2),
        "total_calls": int(term.get("fuse", {}).get("total_calls", 0)),
        "total_errors": int(term.get("fuse", {}).get("total_errors", 0)),
        "read_mib": round(int(term.get("fuse", {}).get("read_bytes", 0)) / (1024 * 1024), 2),
        "write_mib": round(int(term.get("fuse", {}).get("write_bytes", 0)) / (1024 * 1024), 2),
        "nscache": {
            "attr_hit_rate_pct": round(int(ns.get("attr_hits", 0)) / attr_tot * 100.0, 2) if attr_tot > 0 else None,
            "entry_hit_rate_pct": round(int(ns.get("entry_hits", 0)) / entry_tot * 100.0, 2) if entry_tot > 0 else None,
            "negative_entry_hits": int(ns.get("negative_entry_hits", 0)),
            "readdir_hits": int(ns.get("readdir_hits", 0)),
            "invalidations": int(ns.get("invalidations", 0)),
            "evictions": int(ns.get("evictions", 0)),
        },
        "read_ahead": {
            "triggers": int(ra.get("triggers", 0)),
            "prefetched_mib": round(int(ra.get("prefetched_bytes", 0)) / (1024 * 1024), 2),
            "hit_mib": round(int(ra.get("hit_bytes", 0)) / (1024 * 1024), 2),
            "discarded_mib": round(int(ra.get("discarded_bytes", 0)) / (1024 * 1024), 2),
        },
        "inode_locks": {
            "fast_acquisitions": int(il.get("fast_acquisitions", 0)),
            "contended_acquisitions": int(il.get("contended_acquisitions", 0)),
            "wait_ms": round(float(il.get("wait_seconds", 0.0)) * 1000.0, 3),
        },
        "top_ops_by_total_time": by_total[:12],
        "top_ops_by_p99_latency": by_p99[:12],
        "phase_deltas": phase_deltas,
    }


# ==============================================================================
# 2. Jaeger Distributed Trace Analysis (Cross-Node Client -> Server -> DB/Store)
# ==============================================================================

INTERESTING_SPAN_TAGS = {
    "rpc.method",
    "rpc.connect.status_code",
    "rpc.grpc.status_code",
    "http.status_code",
    "fuse.op",
    "disk_id",
    "ino",
    "parent",
    "offset",
    "size",
    "db.system",
    "db.operation",
    "error",
    "otel.status_code",
    "otel.status_description",
}


def distill_jaeger_trace(trace_obj: dict[str, Any]) -> dict[str, Any] | None:
    spans = trace_obj.get("spans") or []
    processes = trace_obj.get("processes") or {}
    if not spans:
        return None

    proc_service = {
        pid: (pinfo.get("serviceName") or "unknown")
        for pid, pinfo in processes.items()
        if isinstance(pinfo, dict)
    }

    span_ids = {s.get("spanID") for s in spans if s.get("spanID")}
    root_span = None
    min_start = None
    max_end = None
    has_error = False
    services_set: set[str] = set()
    service_max_span_us: dict[str, int] = {}

    enriched_spans: list[dict[str, Any]] = []
    for sp in spans:
        st = int(sp.get("startTime", 0))
        dur = int(sp.get("duration", 0))
        ed = st + dur
        min_start = st if min_start is None else min(min_start, st)
        max_end = ed if max_end is None else max(max_end, ed)

        svc = proc_service.get(sp.get("processID", ""), "unknown")
        services_set.add(svc)
        service_max_span_us[svc] = max(service_max_span_us.get(svc, 0), dur)

        tags_map: dict[str, Any] = {}
        span_err = False
        for t in sp.get("tags") or []:
            k = t.get("key")
            v = t.get("value")
            if k == "error" and (v is True or str(v).lower() == "true"):
                span_err = True
            if k == "otel.status_code" and str(v).upper() == "ERROR":
                span_err = True
            if k in INTERESTING_SPAN_TAGS:
                tags_map[k] = v
        if span_err:
            has_error = True

        refs = sp.get("references") or []
        parent_in_trace = any(r.get("spanID") in span_ids for r in refs)
        if not parent_in_trace and (root_span is None or st < int(root_span.get("startTime", 0))):
            root_span = sp

        enriched_spans.append({
            "service": svc,
            "operation": sp.get("operationName", ""),
            "duration_ms": round(dur / 1000.0, 3),
            "start_offset_ms": st,
            "error": span_err,
            "tags": tags_map,
        })

    if root_span is None:
        root_span = max(spans, key=lambda s: int(s.get("duration", 0)))

    base_start = min_start or 0
    for item in enriched_spans:
        item["start_offset_ms"] = round((item["start_offset_ms"] - base_start) / 1000.0, 3)

    enriched_spans.sort(key=lambda s: s["duration_ms"], reverse=True)
    wall_us = max(int(root_span.get("duration", 0)), (max_end - min_start) if (max_end and min_start) else 0)
    root_svc = proc_service.get(root_span.get("processID", ""), "unknown")

    client_ms = round(service_max_span_us.get("bowl-client", 0) / 1000.0, 3)
    server_ms = round(service_max_span_us.get("bowl-storage", 0) / 1000.0, 3)

    return {
        "trace_id": trace_obj.get("traceID", ""),
        "total_duration_ms": round(wall_us / 1000.0, 3),
        "root_service": root_svc,
        "root_operation": root_span.get("operationName", ""),
        "span_count": len(spans),
        "services": sorted(services_set),
        "has_error": has_error,
        "client_max_span_ms": client_ms,
        "server_max_span_ms": server_ms,
        "network_or_client_overhead_ms": round(max(0.0, client_ms - server_ms), 3) if (client_ms > 0 and server_ms > 0) else None,
        "top_spans": enriched_spans[:8],
    }


def process_jaeger_traces(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    jaeger_src = stage_dir / "server" / "jaeger"
    jaeger_dst = out_dir / "jaeger"
    jaeger_dst.mkdir(parents=True, exist_ok=True)
    if not jaeger_src.exists():
        return {}

    by_id: dict[str, dict[str, Any]] = {}
    for p in sorted(jaeger_src.glob("*.json")):
        if p.name == "services.json":
            continue
        payload = load_json_safe(p)
        if not isinstance(payload, dict):
            continue
        for tr in payload.get("data") or []:
            if not isinstance(tr, dict) or not tr.get("traceID"):
                continue
            tid = tr["traceID"]
            # Prefer trace record with more spans (in case partial & complete both fetched)
            if tid not in by_id or len(tr.get("spans") or []) > len(by_id[tid].get("spans") or []):
                by_id[tid] = tr

    distilled = [d for tr in by_id.values() if (d := distill_jaeger_trace(tr)) is not None]
    distilled.sort(key=lambda x: x["total_duration_ms"], reverse=True)

    slow_top = distilled[:30]
    err_traces = [d for d in distilled if d["has_error"]][:30]

    (jaeger_dst / "slow_traces.json").write_text(
        json.dumps(slow_top, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (jaeger_dst / "error_traces.json").write_text(
        json.dumps(err_traces, ensure_ascii=False, indent=2), encoding="utf-8"
    )

    cross_node_count = sum(1 for d in distilled if "bowl-client" in d["services"] and "bowl-storage" in d["services"])
    services_payload = load_json_safe(jaeger_src / "services.json")
    services_list = (services_payload.get("data") or []) if isinstance(services_payload, dict) else []

    return {
        "services_discovered": services_list,
        "sampled_traces_count": len(distilled),
        "cross_node_traces_count": cross_node_count,
        "error_traces_count": sum(1 for d in distilled if d["has_error"]),
        "slow_traces_top15": slow_top[:15],
        "error_traces_top10": err_traces[:10],
    }


# ==============================================================================
# 3. Prometheus Range Queries & Exact `.prom` Histogram / Counter Analysis
# ==============================================================================


def parse_prom_text(path: Path) -> dict[str, list[tuple[dict[str, str], float]]]:
    if not path.exists():
        return {}
    metrics: dict[str, list[tuple[dict[str, str], float]]] = {}
    for raw_line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        m = PROM_LINE_RE.match(line)
        if not m:
            continue
        val_str = m.group("val")
        if val_str == "NaN":
            continue
        val = math.inf if val_str == "+Inf" else (-math.inf if val_str == "-Inf" else float(val_str))
        labels: dict[str, str] = {}
        raw_labels = m.group("labels")
        if raw_labels:
            for lm in PROM_LABEL_RE.finditer(raw_labels):
                labels[lm.group(1)] = lm.group(2)
        metrics.setdefault(m.group("name"), []).append((labels, val))
    return metrics


def prom_cum_quantile(buckets: list[tuple[float, float]], q: float) -> float:
    """Compute Prometheus histogram_quantile from cumulative (upper_bound, cum_count) pairs."""
    finite = sorted((b, c) for b, c in buckets if not math.isinf(b))
    inf_counts = [c for b, c in buckets if math.isinf(b)]
    total = inf_counts[0] if inf_counts else (finite[-1][1] if finite else 0.0)
    if total <= 0 or not finite:
        return 0.0
    rank = q * total
    prev_b = 0.0
    prev_c = 0.0
    for b, c in finite:
        if c >= rank:
            delta_c = c - prev_c
            if delta_c <= 0:
                return b
            return prev_b + ((rank - prev_c) / delta_c) * (b - prev_b)
        prev_b = b
        prev_c = c
    return finite[-1][0]


def summarize_prom_histogram(
    parsed: dict[str, list[tuple[dict[str, str], float]]],
    base_name: str,
    group_key: str | None = None,
    pre_parsed: dict[str, list[tuple[dict[str, str], float]]] | None = None,
    scale: float = 1000.0,
) -> list[dict[str, Any]]:
    """Summarize cumulative Prometheus histogram (optionally subtracting pre_parsed)."""
    bucket_entries = parsed.get(f"{base_name}_bucket", [])
    sum_entries = parsed.get(f"{base_name}_sum", [])
    cnt_entries = parsed.get(f"{base_name}_count", [])
    if not cnt_entries:
        return []

    def index_by_key(
        entries: list[tuple[dict[str, str], float]], include_le: bool = False
    ) -> dict[tuple[str, str], float]:
        out: dict[tuple[str, str], float] = {}
        for labels, val in entries:
            g = labels.get(group_key, "all") if group_key else "all"
            le = labels.get("le", "") if include_le else ""
            out[(g, le)] = out.get((g, le), 0.0) + val
        return out

    post_cnt = index_by_key(cnt_entries)
    post_sum = index_by_key(sum_entries)
    post_bkt = index_by_key(bucket_entries, include_le=True)

    pre_cnt = index_by_key(pre_parsed.get(f"{base_name}_count", [])) if pre_parsed else {}
    pre_sum = index_by_key(pre_parsed.get(f"{base_name}_sum", [])) if pre_parsed else {}
    pre_bkt = index_by_key(pre_parsed.get(f"{base_name}_bucket", []), include_le=True) if pre_parsed else {}

    results: list[dict[str, Any]] = []
    for (g, _), c_val in post_cnt.items():
        d_cnt = max(0.0, c_val - pre_cnt.get((g, ""), 0.0))
        if d_cnt <= 0:
            continue
        d_sum = max(0.0, post_sum.get((g, ""), 0.0) - pre_sum.get((g, ""), 0.0))
        buckets_for_g: list[tuple[float, float]] = []
        for (bg, le_str), b_val in post_bkt.items():
            if bg != g or not le_str:
                continue
            bound = math.inf if le_str == "+Inf" else float(le_str)
            d_b = max(0.0, b_val - pre_bkt.get((bg, le_str), 0.0))
            buckets_for_g.append((bound, d_b))

        results.append({
            "group": g,
            "count": int(round(d_cnt)),
            "total": round(d_sum * scale, 3),
            "mean": round((d_sum / d_cnt) * scale, 3),
            "p50": round(prom_cum_quantile(buckets_for_g, 0.50) * scale, 3),
            "p95": round(prom_cum_quantile(buckets_for_g, 0.95) * scale, 3),
            "p99": round(prom_cum_quantile(buckets_for_g, 0.99) * scale, 3),
        })

    results.sort(key=lambda x: x["total"], reverse=True)
    return results


def summarize_prom_query_range(query_range_payload: dict[str, Any]) -> dict[str, Any]:
    queries = query_range_payload.get("queries") or {}
    summarized: dict[str, list[dict[str, Any]]] = {}

    for qname, qentry in queries.items():
        resp = (qentry or {}).get("response") or {}
        if resp.get("status") != "success":
            continue
        series_list = (resp.get("data") or {}).get("result") or []
        series_summaries: list[dict[str, Any]] = []
        for s in series_list:
            metric_labels = s.get("metric") or {}
            vals: list[float] = []
            for pair in s.get("values") or []:
                if len(pair) >= 2:
                    try:
                        fv = float(pair[1])
                        if not math.isnan(fv) and not math.isinf(fv):
                            vals.append(fv)
                    except ValueError:
                        pass
            if not vals:
                continue
            vals_sorted = sorted(vals)
            p95_idx = min(len(vals_sorted) - 1, int(math.ceil(0.95 * len(vals_sorted))) - 1)
            series_summaries.append({
                "labels": metric_labels,
                "samples": len(vals),
                "mean": round(sum(vals) / len(vals), 6),
                "p95": round(vals_sorted[max(0, p95_idx)], 6),
                "max": round(vals_sorted[-1], 6),
            })
        if series_summaries:
            series_summaries.sort(key=lambda x: x["max"], reverse=True)
            summarized[qname] = series_summaries[:15]
    return summarized


def process_prometheus_data(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    prom_dst = out_dir / "prometheus"
    prom_dst.mkdir(parents=True, exist_ok=True)

    srv_prom_api = stage_dir / "server" / "prometheus"
    srv_metrics_dir = stage_dir / "server" / "metrics"

    targets_health: list[dict[str, Any]] = []
    targets_json = load_json_safe(srv_prom_api / "targets.json")
    if isinstance(targets_json, dict):
        for t in (targets_json.get("data") or {}).get("activeTargets") or []:
            targets_health.append({
                "job": (t.get("labels") or {}).get("job", ""),
                "scrape_url": t.get("scrapeUrl", ""),
                "health": t.get("health", ""),
                "last_error": t.get("lastError", ""),
            })

    range_summary: dict[str, Any] = {}
    qr_json = load_json_safe(srv_prom_api / "query_range.json")
    if isinstance(qr_json, dict):
        range_summary = summarize_prom_query_range(qr_json)

    # Exact histogram & counter breakdown from server `.prom` snapshots
    server_engine_summary: dict[str, Any] = {}
    if srv_metrics_dir.exists():
        prom_files = {p.stem: p for p in srv_metrics_dir.glob("*.prom")}
        term_stem = next(
            (cand for cand in ("before_umount", "suite_post", "final") if cand in prom_files),
            next(iter(sorted(prom_files.keys())), None),
        )
        if term_stem:
            term_parsed = parse_prom_text(prom_files[term_stem])

            def sum_metric_by_label(mname: str, lkey: str | None = None) -> Any:
                entries = term_parsed.get(mname, [])
                if not entries:
                    return None
                if lkey is None:
                    return round(sum(v for _, v in entries if not math.isinf(v)), 3)
                grouped: dict[str, float] = {}
                for lbls, v in entries:
                    k = lbls.get(lkey, "all")
                    grouped[k] = round(grouped.get(k, 0.0) + v, 3)
                return grouped

            bc_hits = sum_metric_by_label("bowl_storage_pebble_block_cache_hits_total") or 0.0
            bc_miss = sum_metric_by_label("bowl_storage_pebble_block_cache_misses_total") or 0.0

            server_engine_summary = {
                "snapshot": term_stem,
                "rpc_server_latency_ms": summarize_prom_histogram(
                    term_parsed, "rpc_server_call_duration_seconds", group_key="rpc_method", scale=1000.0
                ),
                "actor_queue_wait_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_actor_queue_wait_seconds", scale=1000.0
                ),
                "actor_tick_batch_size": summarize_prom_histogram(
                    term_parsed, "bowl_storage_actor_tick_batch_size", scale=1.0
                ),
                "actor_tick_phase_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_actor_tick_phase_duration_seconds", group_key="phase", scale=1000.0
                ),
                "blob_pwrite_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_blob_pwrite_duration_seconds", scale=1000.0
                ),
                "blob_sync_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_blob_sync_duration_seconds", group_key="target", scale=1000.0
                ),
                "blob_rmw_read_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_blob_rmw_read_duration_seconds", scale=1000.0
                ),
                "inode_lock_wait_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_inode_lock_wait_seconds", scale=1000.0
                ),
                "pebble_wal_fsync_ms": summarize_prom_histogram(
                    term_parsed, "bowl_storage_pebble_wal_fsync_duration_seconds", scale=1000.0
                ),
                "counters_and_gauges": {
                    "blob_write_bytes_by_mode": sum_metric_by_label("bowl_storage_blob_write_bytes_total", "mode"),
                    "blob_read_blocks_by_source": sum_metric_by_label("bowl_storage_blob_read_source_total", "source"),
                    "pebble_wal_bytes_written": sum_metric_by_label("bowl_storage_pebble_wal_bytes_written_total"),
                    "pebble_block_cache_hit_rate_pct": (
                        round(bc_hits / (bc_hits + bc_miss) * 100.0, 2) if (bc_hits + bc_miss) > 0 else None
                    ),
                    "pebble_read_amp": sum_metric_by_label("bowl_storage_pebble_read_amp"),
                    "pebble_stall_events": sum_metric_by_label("bowl_storage_pebble_stall_events_total"),
                    "pebble_disk_slow_events": sum_metric_by_label("bowl_storage_pebble_disk_slow_total"),
                },
            }

            # Per-phase server RPC & Actor phase deltas
            phase_server_deltas: dict[str, Any] = {}
            for stem in sorted(prom_files.keys()):
                if stem.endswith("_pre"):
                    base = stem[: -len("_pre")]
                    post_stem = f"{base}_post"
                    if post_stem in prom_files:
                        pre_p = parse_prom_text(prom_files[stem])
                        post_p = parse_prom_text(prom_files[post_stem])
                        phase_server_deltas[base] = {
                            "rpc_server_latency_ms": summarize_prom_histogram(
                                post_p, "rpc_server_call_duration_seconds", "rpc_method", pre_parsed=pre_p, scale=1000.0
                            )[:8],
                            "actor_tick_phase_ms": summarize_prom_histogram(
                                post_p, "bowl_storage_actor_tick_phase_duration_seconds", "phase", pre_parsed=pre_p, scale=1000.0
                            ),
                            "blob_pwrite_ms": summarize_prom_histogram(
                                post_p, "bowl_storage_blob_pwrite_duration_seconds", pre_parsed=pre_p, scale=1000.0
                            ),
                            "blob_sync_ms": summarize_prom_histogram(
                                post_p, "bowl_storage_blob_sync_duration_seconds", "target", pre_parsed=pre_p, scale=1000.0
                            ),
                        }
            if phase_server_deltas:
                server_engine_summary["phase_deltas"] = phase_server_deltas

    summary = {
        "targets": targets_health,
        "range_queries": range_summary,
        "server_engine": server_engine_summary,
    }
    (prom_dst / "metrics_summary.json").write_text(
        json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    return summary


# ==============================================================================
# 4. Loki & Client Structured Logs Extraction
# ==============================================================================


def process_logs(stage_dir: Path, out_dir: Path) -> dict[str, Any]:
    loki_dst = out_dir / "loki"
    loki_dst.mkdir(parents=True, exist_ok=True)

    loki_json = load_json_safe(stage_dir / "server" / "loki" / "query_range.json")
    container_entries: list[dict[str, Any]] = []
    server_alerts: list[dict[str, Any]] = []

    if isinstance(loki_json, dict) and loki_json.get("status") == "success":
        for stream in (loki_json.get("data") or {}).get("result") or []:
            s_labels = stream.get("stream") or {}
            svc = s_labels.get("service") or s_labels.get("container") or "unknown"
            for ts_ns, raw_line in stream.get("values") or []:
                rec: dict[str, Any] = {"ts_ns": ts_ns, "service": svc}
                stripped = raw_line.strip()
                if stripped.startswith("{") and stripped.endswith("}"):
                    try:
                        parsed = json.loads(stripped)
                        if isinstance(parsed, dict):
                            rec.update(parsed)
                        else:
                            rec["msg"] = stripped
                    except Exception:
                        rec["msg"] = stripped
                else:
                    rec["msg"] = stripped
                container_entries.append(rec)
                lvl = str(rec.get("level", "")).upper()
                if svc == "storage-service" or lvl in ("WARN", "WARNING", "ERROR"):
                    server_alerts.append(rec)

    container_entries.sort(key=lambda x: str(x.get("ts_ns", "")))
    if container_entries:
        with (loki_dst / "container_logs.jsonl").open("w", encoding="utf-8") as f:
            for item in container_entries[:1000]:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    client_alerts: list[dict[str, Any]] = []
    client_lifecycle: list[dict[str, Any]] = []
    cli_log_dir = stage_dir / "client" / "logs"
    if cli_log_dir.exists():
        log_cand = next(
            (cli_log_dir / n for n in ("bowl-mount-phase.log", "bowl-mount.log") if (cli_log_dir / n).exists()),
            None,
        )
        if log_cand is not None:
            for raw_line in log_cand.read_text(encoding="utf-8", errors="replace").splitlines():
                s = raw_line.strip()
                if not s:
                    continue
                if s.startswith("{") and s.endswith("}"):
                    try:
                        obj = json.loads(s)
                        if isinstance(obj, dict):
                            lvl = str(obj.get("level", "")).upper()
                            msg = str(obj.get("msg", ""))
                            if msg in ("mount_ready", "unmount_complete"):
                                client_lifecycle.append(obj)
                            if lvl in ("WARN", "WARNING", "ERROR"):
                                client_alerts.append(obj)
                            continue
                    except Exception:
                        pass
                if "ERR" in s or "错误" in s:
                    client_alerts.append({"level": "ERROR", "msg": s})

    if client_alerts or client_lifecycle:
        with (loki_dst / "client_logs.jsonl").open("w", encoding="utf-8") as f:
            for item in client_lifecycle + client_alerts[:500]:
                f.write(json.dumps(item, ensure_ascii=False) + "\n")

    return {
        "loki_log_entries": len(container_entries),
        "server_alert_count": len(server_alerts),
        "client_alert_count": len(client_alerts),
        "server_alerts_sample": server_alerts[:15],
        "client_alerts_sample": client_alerts[:15],
        "client_lifecycle": client_lifecycle,
    }


# ==============================================================================
# 5. pprof Top Hotspot Extraction via `go tool pprof`
# ==============================================================================


def process_pprof_profiles(
    stage_dir: Path, out_dir: Path, server_bin: Path | None, client_bin: Path | None
) -> list[dict[str, Any]]:
    pprof_dst = out_dir / "pprof_top"
    go_bin = shutil.which("go")
    if not go_bin:
        return []

    extracted: list[dict[str, Any]] = []
    for side, bin_path in (("server", server_bin), ("client", client_bin)):
        src = stage_dir / side / "pprof"
        if not src.exists():
            continue
        for pb in sorted(src.glob("*.pb.gz")):
            if pb.stat().st_size == 0:
                continue
            cmd = [go_bin, "tool", "pprof", "-top", "-cum", "-nodecount=25"]
            if bin_path is not None and bin_path.exists():
                cmd.append(str(bin_path))
            cmd.append(str(pb))
            try:
                proc = subprocess.run(cmd, capture_output=True, text=True, timeout=20, check=False)
            except Exception:
                continue
            out_text = proc.stdout.strip()
            if proc.returncode != 0 or not out_text:
                continue
            pprof_dst.mkdir(parents=True, exist_ok=True)
            txt_name = f"{side}_{pb.name[:-len('.pb.gz')]}.txt"
            (pprof_dst / txt_name).write_text(out_text + "\n", encoding="utf-8")

            top_lines = [
                ln.strip()
                for ln in out_text.splitlines()
                if ln.strip() and not ln.startswith(("File:", "Build ID:", "Type:", "Time:", "Duration:", "Showing", "Dropped", "      flat"))
            ][:5]
            extracted.append({
                "side": side,
                "profile": pb.name,
                "report_file": f"pprof_top/{txt_name}",
                "top_entries": top_lines,
            })
    return extracted


# ==============================================================================
# 6. Overview Markdown & JSON Rendering
# ==============================================================================


def load_stage_timings(results_dir: Path) -> list[dict[str, Any]]:
    timings_file = results_dir / "stage_timings.jsonl"
    if not timings_file.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in timings_file.read_text(encoding="utf-8", errors="replace").splitlines():
        if line.strip():
            try:
                rows.append(json.loads(line))
            except Exception:
                pass
    rows.sort(key=lambda r: float(r.get("duration_sec", 0.0)), reverse=True)
    return rows


def render_overview_markdown(overview: dict[str, Any]) -> str:
    lines: list[str] = [
        f"# Bowl `{overview['suite']}` 生产可观测性与慢路径遥测诊断总览",
        "",
        f"- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）",
        f"- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、全量 `.prom` 快照、Pebble `LOG` 与 `diskstats`）",
        "",
    ]

    # 1. Slowest Stages
    stages = overview.get("slowest_stages") or []
    if stages:
        lines.extend([
            "## 1. 最慢测试阶段排序（Stage Timings）",
            "",
            "| 排名 | 阶段 / 目录 | 耗时 (s) |",
            "| :--- | :--- | ---: |",
        ])
        for idx, st in enumerate(stages[:10], 1):
            label = st.get("stage") or st.get("directory") or f"{st.get('target', '')}:{st.get('suite', '')}"
            lines.append(f"| {idx} | `{label}` | {float(st.get('duration_sec', 0.0)):.3f} |")
        lines.append("")

    # 2. Client FUSE Operations
    cli = overview.get("client_stats") or {}
    if cli.get("top_ops_by_total_time"):
        nsc = cli.get("nscache") or {}
        ra = cli.get("read_ahead") or {}
        il = cli.get("inode_locks") or {}
        lines.extend([
            "## 2. 客户端 FUSE 操作耗时拆解（来自 `bowl stats`）",
            "",
            f"- **累计调用**：`{cli.get('total_calls', 0):,}` 次（错误返回 `{cli.get('total_errors', 0):,}` 次），读 `{cli.get('read_mib', 0):.2f} MiB` / 写 `{cli.get('write_mib', 0):.2f} MiB`",
            f"- **NamespaceCache 命中率**：Attr `{nsc.get('attr_hit_rate_pct')}%` · Entry `{nsc.get('entry_hit_rate_pct')}%` · 负缓存命中 `{nsc.get('negative_entry_hits', 0):,}` · 失效 `{nsc.get('invalidations', 0):,}`",
            f"- **ReadAhead 预读**：触发 `{ra.get('triggers', 0):,}` 次 · 预读 `{ra.get('prefetched_mib', 0):.2f} MiB` · 命中 `{ra.get('hit_mib', 0):.2f} MiB` · 丢弃 `{ra.get('discarded_mib', 0):.2f} MiB`",
            f"- **Inode 串行锁竞争**：快速获取 `{il.get('fast_acquisitions', 0):,}` · 竞争获取 `{il.get('contended_acquisitions', 0):,}`（累计等待 `{il.get('wait_ms', 0):.2f} ms`）",
            "",
            "| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |",
        ])
        for op in cli["top_ops_by_total_time"][:12]:
            errno_str = ", ".join(f"{k}:{v}" for k, v in (op.get("errnos") or {}).items()) or "—"
            lines.append(
                f"| `{op['op']}` | {op['calls']:,} | {op['total_duration_ms']:.2f} | {op['share_of_fuse_time_pct']:.1f}% | "
                f"{op['mean_us']:.1f} | {op['p50_us']:.1f} | {op['p95_us']:.1f} | {op['p99_us']:.1f} | {errno_str} |"
            )
        lines.append("")

    # 3. Server RPC & Engine Internal Phases
    srv_eng = (overview.get("prometheus") or {}).get("server_engine") or {}
    if srv_eng.get("rpc_server_latency_ms"):
        lines.extend([
            "## 3. 服务端 ConnectRPC 与存储引擎内部延迟拆解（Prometheus）",
            "",
            "### 3.1 ConnectRPC 接口耗时 Top 榜",
            "",
            "| RPC 方法 (`rpc_method`) | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for r in srv_eng["rpc_server_latency_ms"][:12]:
            lines.append(
                f"| `{r['group']}` | {r['count']:,} | {r['total']:.2f} | {r['mean']:.3f} | {r['p50']:.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
            )
        lines.append("")

    if srv_eng.get("actor_tick_phase_ms"):
        lines.extend([
            "### 3.2 `StoreActor` Group Commit 与 `BlobStore` 内部子阶段延迟",
            "",
            "| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for ph in srv_eng["actor_tick_phase_ms"]:
            lines.append(
                f"| `actor_phase:{ph['group']}` | {ph['count']:,} | {ph['total']:.2f} | {ph['mean']:.3f} | {ph['p50']:.3f} | {ph['p95']:.3f} | {ph['p99']:.3f} |"
            )
        for label, key in (
            ("actor_queue_wait", "actor_queue_wait_ms"),
            ("blob_pwrite", "blob_pwrite_ms"),
            ("blob_rmw_read", "blob_rmw_read_ms"),
            ("inode_lock_wait", "inode_lock_wait_ms"),
            ("pebble_wal_fsync", "pebble_wal_fsync_ms"),
        ):
            for item in srv_eng.get(key) or []:
                lines.append(
                    f"| `{label}` | {item['count']:,} | {item['total']:.2f} | {item['mean']:.3f} | {item['p50']:.3f} | {item['p95']:.3f} | {item['p99']:.3f} |"
                )
        for item in srv_eng.get("blob_sync_ms") or []:
            lines.append(
                f"| `blob_sync:{item['group']}` | {item['count']:,} | {item['total']:.2f} | {item['mean']:.3f} | {item['p50']:.3f} | {item['p95']:.3f} | {item['p99']:.3f} |"
            )
        cg = srv_eng.get("counters_and_gauges") or {}
        lines.extend([
            "",
            f"- **Pebble 状态**：BlockCache 命中率 `{cg.get('pebble_block_cache_hit_rate_pct')}%` · ReadAmp `{cg.get('pebble_read_amp')}` · WriteStall `{cg.get('pebble_stall_events')}` · WAL 写入 `{cg.get('pebble_wal_bytes_written')}` B",
            f"- **BlobStore 读写分布**：写块字节 `{json.dumps(cg.get('blob_write_bytes_by_mode'), ensure_ascii=False)}` · 读源块计数 `{json.dumps(cg.get('blob_read_blocks_by_source'), ensure_ascii=False)}`",
            "",
        ])

    # 4. Jaeger Slow Traces
    jg = overview.get("jaeger") or {}
    if jg.get("slow_traces_top15"):
        lines.extend([
            "## 4. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）",
            "",
            f"- 已采样链路：`{jg.get('sampled_traces_count', 0)}` 条（跨 Client+Server 全链路 `{jg.get('cross_node_traces_count', 0)}` 条，异常链路 `{jg.get('error_traces_count', 0)}` 条；完整明细见 `jaeger/slow_traces.json`）",
            "",
            "| TraceID | 总耗时 (ms) | 入口操作 | Client Span (ms) | Server Span (ms) | 网络/排队差值 (ms) | 最耗时子 Span |",
            "| :--- | ---: | :--- | ---: | ---: | ---: | :--- |",
        ])
        for tr in jg["slow_traces_top15"]:
            top_sub = tr["top_spans"][1] if len(tr["top_spans"]) > 1 else tr["top_spans"][0]
            ovh = f"{tr['network_or_client_overhead_ms']:.2f}" if tr.get("network_or_client_overhead_ms") is not None else "—"
            lines.append(
                f"| `{tr['trace_id'][:16]}` | {tr['total_duration_ms']:.2f} | `{tr['root_service']}:{tr['root_operation']}` | "
                f"{tr['client_max_span_ms']:.2f} | {tr['server_max_span_ms']:.2f} | {ovh} | "
                f"`{top_sub['service']}:{top_sub['operation']}` ({top_sub['duration_ms']:.2f} ms) |"
            )
        lines.append("")

    # 5. Loki & Client Alerts
    lg = overview.get("logs") or {}
    lines.extend([
        "## 5. 结构化日志与异常审计（Loki + Client JSON Log）",
        "",
        f"- **Loki 容器日志条数**：`{lg.get('loki_log_entries', 0)}` · **服务端 Warn/Error 数**：`{lg.get('server_alert_count', 0)}` · **客户端 Warn/Error 数**：`{lg.get('client_alert_count', 0)}`（明细见 `loki/`）",
        "",
    ])

    # 6. pprof Hotspots
    pprofs = overview.get("pprof_profiles") or []
    if pprofs:
        lines.extend([
            "## 6. 在线剖析热点索引（`pprof_top/`）",
            "",
            "| 端 | 剖析文件 | Top-1 累计热点 (`-cum`) | 文本明细 |",
            "| :--- | :--- | :--- | :--- |",
        ])
        for pp in pprofs:
            top1 = pp["top_entries"][0] if pp.get("top_entries") else "—"
            lines.append(f"| `{pp['side']}` | `{pp['profile']}` | `{top1}` | `{pp['report_file']}` |")
        lines.append("")

    return "\n".join(lines)


def load_telemetry_overview(results_dir: Path) -> dict[str, Any]:
    """Load distilled results/telemetry/overview.json if present."""
    p = results_dir / "telemetry" / "overview.json"
    if not p.exists():
        return {}
    data = load_json_safe(p)
    return data if isinstance(data, dict) else {}


def render_report_telemetry_section(overview: dict[str, Any], heading: str) -> list[str]:
    """Render a concise telemetry & bottleneck diagnosis section for suite REPORT.md."""
    if not overview:
        return []

    lines: list[str] = [
        "---",
        "",
        heading,
        "",
        f"- **生产可观测性编排栈**：`{overview.get('deploy_profile', 'deploy/prod')}`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）",
    ]

    lg = overview.get("logs") or {}
    jg = overview.get("jaeger") or {}
    lines.append(
        f"- **遥测信号覆盖**：Jaeger 采样链路 `{jg.get('sampled_traces_count', 0)}` 条"
        f"（跨 Client+Server 全链路 `{jg.get('cross_node_traces_count', 0)}` 条，异常链路 `{jg.get('error_traces_count', 0)}` 条）· "
        f"Loki 容器日志 `{lg.get('loki_log_entries', 0)}` 条（服务端 Warn/Error `{lg.get('server_alert_count', 0)}` 条，客户端 Warn/Error `{lg.get('client_alert_count', 0)}` 条）"
    )
    lines.append("")

    cli = overview.get("client_stats") or {}
    if cli.get("top_ops_by_total_time"):
        nsc = cli.get("nscache") or {}
        ra = cli.get("read_ahead") or {}
        lines.extend([
            "### 客户端 FUSE 累计耗时 Top 操作（`bowl stats`）",
            "",
            f"NamespaceCache 命中率：Attr `{nsc.get('attr_hit_rate_pct')}%` / Entry `{nsc.get('entry_hit_rate_pct')}%`（负缓存命中 `{nsc.get('negative_entry_hits', 0):,}`）；"
            f"ReadAhead 预读 `{ra.get('prefetched_mib', 0):.2f} MiB`（命中 `{ra.get('hit_mib', 0):.2f} MiB`）。",
            "",
            "| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P95 (µs) | P99 (µs) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: | ---: |",
        ])
        for op in cli["top_ops_by_total_time"][:8]:
            lines.append(
                f"| `{op['op']}` | {op['calls']:,} | {op['total_duration_ms']:.2f} | {op['share_of_fuse_time_pct']:.1f}% | "
                f"{op['mean_us']:.1f} | {op['p95_us']:.1f} | {op['p99_us']:.1f} |"
            )
        lines.append("")

    srv_eng = (overview.get("prometheus") or {}).get("server_engine") or {}
    if srv_eng.get("rpc_server_latency_ms"):
        lines.extend([
            "### 服务端 ConnectRPC 与存储引擎内部延迟（Prometheus）",
            "",
            "| RPC / 引擎内部阶段 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P95 (ms) | P99 (ms) |",
            "| :--- | ---: | ---: | ---: | ---: | ---: |",
        ])
        for r in srv_eng["rpc_server_latency_ms"][:6]:
            lines.append(
                f"| `{r['group']}` | {r['count']:,} | {r['total']:.2f} | {r['mean']:.3f} | {r['p95']:.3f} | {r['p99']:.3f} |"
            )
        for ph in srv_eng.get("actor_tick_phase_ms") or []:
            lines.append(
                f"| `actor_phase:{ph['group']}` | {ph['count']:,} | {ph['total']:.2f} | {ph['mean']:.3f} | {ph['p95']:.3f} | {ph['p99']:.3f} |"
            )
        cg = srv_eng.get("counters_and_gauges") or {}
        lines.extend([
            "",
            f"Pebble BlockCache 命中率 `{cg.get('pebble_block_cache_hit_rate_pct')}%` · ReadAmp `{cg.get('pebble_read_amp')}` · WriteStall `{cg.get('pebble_stall_events')}`。",
            "",
        ])

    if jg.get("slow_traces_top15"):
        lines.extend([
            "### Jaeger 端到端最慢采样链路 Top 5",
            "",
            "| TraceID | 总耗时 (ms) | 入口操作 | Client Span (ms) | Server Span (ms) | 最耗时子 Span |",
            "| :--- | ---: | :--- | ---: | ---: | :--- |",
        ])
        for tr in jg["slow_traces_top15"][:5]:
            top_sub = tr["top_spans"][1] if len(tr["top_spans"]) > 1 else tr["top_spans"][0]
            lines.append(
                f"| `{tr['trace_id'][:16]}` | {tr['total_duration_ms']:.2f} | `{tr['root_service']}:{tr['root_operation']}` | "
                f"{tr['client_max_span_ms']:.2f} | {tr['server_max_span_ms']:.2f} | `{top_sub['service']}:{top_sub['operation']}` ({top_sub['duration_ms']:.2f} ms) |"
            )
        lines.append("")

    pprofs = overview.get("pprof_profiles") or []
    if pprofs:
        lines.extend([
            "### `pprof` CPU / Heap / Mutex 采样热点索引",
            "",
            "| 端 | 剖析文件 | Top-1 累计热点 (`-cum`) | 文本明细 |",
            "| :--- | :--- | :--- | :--- |",
        ])
        for pp in pprofs:
            top1 = pp["top_entries"][0] if pp.get("top_entries") else "—"
            lines.append(f"| `{pp['side']}` | `{pp['profile']}` | `{top1}` | [`{pp['report_file']}`](results/telemetry/{pp['report_file']}) |")
        lines.append("")

    return lines


def build_telemetry_bundle(
    suite: str,
    stage_dir: Path,
    results_dir: Path,
    server_bin: Path | None = None,
    client_bin: Path | None = None,
) -> dict[str, Any]:
    telem_out = results_dir / "telemetry"
    if telem_out.exists():
        shutil.rmtree(telem_out)
    telem_out.mkdir(parents=True, exist_ok=True)

    client_summary = process_client_stats(stage_dir, telem_out)
    jaeger_summary = process_jaeger_traces(stage_dir, telem_out)
    prom_summary = process_prometheus_data(stage_dir, telem_out)
    logs_summary = process_logs(stage_dir, telem_out)
    pprof_summary = process_pprof_profiles(stage_dir, telem_out, server_bin, client_bin)
    slowest_stages = load_stage_timings(results_dir)

    overview = {
        "suite": suite,
        "deploy_profile": "deploy/prod (docker-compose.yaml + storage.env)",
        "slowest_stages": slowest_stages[:15],
        "client_stats": client_summary,
        "jaeger": jaeger_summary,
        "prometheus": prom_summary,
        "logs": logs_summary,
        "pprof_profiles": pprof_summary,
    }

    (telem_out / "overview.json").write_text(
        json.dumps(overview, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    (telem_out / "overview.md").write_text(render_overview_markdown(overview), encoding="utf-8")
    return overview


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--suite", required=True, help="Benchmark suite name (juicefs, drive9, pjdfstest)")
    parser.add_argument("--stage-dir", required=True, type=Path, help="Staging directory containing client/ and server/")
    parser.add_argument("--results-dir", required=True, type=Path, help="Benchmark results directory")
    parser.add_argument("--server-bin", type=Path, default=None, help="Optional storage-service binary for pprof")
    parser.add_argument("--client-bin", type=Path, default=None, help="Optional bowl CLI binary for pprof")
    args = parser.parse_args()

    build_telemetry_bundle(
        suite=args.suite,
        stage_dir=args.stage_dir,
        results_dir=args.results_dir,
        server_bin=args.server_bin,
        client_bin=args.client_bin,
    )


if __name__ == "__main__":
    main()
