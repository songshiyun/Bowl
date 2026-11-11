#!/usr/bin/env python3
"""Unit tests for benchmark/common/collect_telemetry.py."""

from __future__ import annotations

import importlib.util
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import collect_telemetry

BENCH_ROOT = Path(__file__).resolve().parent.parent


def load_suite_parser(suite: str):
    spec = importlib.util.spec_from_file_location(
        f"parse_results_{suite}", BENCH_ROOT / suite / "parse_results.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class CollectTelemetryTest(unittest.TestCase):
    def test_end_to_end_telemetry_bundle_distillation(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage_dir = root / "stage"
            results_dir = root / "results"
            results_dir.mkdir(parents=True)

            # 1. Stage timings
            (results_dir / "stage_timings.jsonl").write_text(
                "\n".join([
                    json.dumps({"target": "bowl", "suite": "fio", "duration_sec": 42.5}),
                    json.dumps({"target": "bowl", "suite": "mdtest", "duration_sec": 18.2}),
                ]) + "\n",
                encoding="utf-8",
            )

            # 2. Client `bowl stats` pre/post & before_umount snapshots
            cli_stats = stage_dir / "client" / "stats"
            cli_stats.mkdir(parents=True)
            empty_buckets = [0] * 20
            pre_buckets = list(empty_buckets)
            pre_buckets[10] = 10  # <= 1024 us
            post_buckets = list(empty_buckets)
            post_buckets[10] = 10
            post_buckets[12] = 90  # 90 calls in (2048..4096] us

            snap_pre = {
                "timestamp_unix_nano": 1_000_000_000,
                "uptime_seconds": 1.0,
                "cpu_usage_seconds": 0.2,
                "memory_bytes": 64 * 1024 * 1024,
                "fuse": {
                    "total_calls": 10,
                    "total_errors": 0,
                    "read_bytes": 0,
                    "write_bytes": 40960,
                    "ops": [
                        {
                            "op": "Write",
                            "calls": 10,
                            "errors": 0,
                            "bytes": 40960,
                            "duration_nanos": 8_000_000,
                            "buckets": pre_buckets,
                        }
                    ],
                },
                "namespace_cache": {"attr_hits": 10, "attr_misses": 2, "entry_hits": 5, "entry_misses": 5},
                "read_ahead": {"triggers": 0, "prefetched_bytes": 0, "hit_bytes": 0, "discarded_bytes": 0},
                "inode_locks": {"fast_acquisitions": 10, "contended_acquisitions": 0, "wait_seconds": 0.0},
            }
            snap_post = {
                "timestamp_unix_nano": 5_000_000_000,
                "uptime_seconds": 5.0,
                "cpu_usage_seconds": 1.7,
                "memory_bytes": 128 * 1024 * 1024,
                "fuse": {
                    "total_calls": 110,
                    "total_errors": 2,
                    "read_bytes": 1048576,
                    "write_bytes": 419430400,
                    "ops": [
                        {
                            "op": "Write",
                            "calls": 100,
                            "errors": 0,
                            "bytes": 419430400,
                            "duration_nanos": 308_000_000,
                            "buckets": post_buckets,
                        },
                        {
                            "op": "Lookup",
                            "calls": 10,
                            "errors": 2,
                            "bytes": 0,
                            "duration_nanos": 5_000_000,
                            "buckets": [0, 0, 0, 0, 0, 0, 0, 0, 10] + [0] * 11,
                            "errnos": {"ENOENT": 2},
                        },
                    ],
                },
                "namespace_cache": {"attr_hits": 90, "attr_misses": 12, "entry_hits": 45, "entry_misses": 15},
                "read_ahead": {"triggers": 4, "prefetched_bytes": 4194304, "hit_bytes": 3145728, "discarded_bytes": 0},
                "inode_locks": {"fast_acquisitions": 100, "contended_acquisitions": 5, "wait_seconds": 0.012},
            }
            (cli_stats / "fio_seq_write_1job_pre.json").write_text(json.dumps(snap_pre), encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_post.json").write_text(json.dumps(snap_post), encoding="utf-8")
            (cli_stats / "before_umount.json").write_text(json.dumps(snap_post), encoding="utf-8")

            # 3. Jaeger cross-node traces
            jg_dir = stage_dir / "server" / "jaeger"
            jg_dir.mkdir(parents=True)
            (jg_dir / "services.json").write_text(json.dumps({"data": ["bowl-client", "bowl-storage"]}), encoding="utf-8")
            jaeger_traces = {
                "data": [
                    {
                        "traceID": "abcdef1234567890abcdef1234567890",
                        "processes": {
                            "p1": {"serviceName": "bowl-client"},
                            "p2": {"serviceName": "bowl-storage"},
                        },
                        "spans": [
                            {
                                "traceID": "abcdef1234567890abcdef1234567890",
                                "spanID": "s1",
                                "operationName": "fuse.Write",
                                "startTime": 1700000000000000,
                                "duration": 4500,  # 4.5 ms
                                "processID": "p1",
                                "references": [],
                                "tags": [{"key": "fuse.op", "value": "Write"}],
                            },
                            {
                                "traceID": "abcdef1234567890abcdef1234567890",
                                "spanID": "s2",
                                "operationName": "storage.v1.StorageService/Write",
                                "startTime": 1700000000000500,
                                "duration": 3200,  # 3.2 ms
                                "processID": "p2",
                                "references": [{"refType": "CHILD_OF", "spanID": "s1"}],
                                "tags": [
                                    {"key": "rpc.method", "value": "storage.v1.StorageService/Write"},
                                    {"key": "error", "value": True},
                                ],
                            },
                        ],
                    }
                ]
            }
            (jg_dir / "traces_bowl-client.json").write_text(json.dumps(jaeger_traces), encoding="utf-8")

            # 4. Prometheus range queries & server .prom snapshots
            prom_dir = stage_dir / "server" / "prometheus"
            prom_dir.mkdir(parents=True)
            (prom_dir / "targets.json").write_text(
                json.dumps({
                    "data": {
                        "activeTargets": [
                            {"labels": {"job": "bowl-storage"}, "scrapeUrl": "http://storage-service:9090/metrics", "health": "up"},
                            {"labels": {"job": "bowl-client"}, "scrapeUrl": "http://172.16.1.11:9567/metrics", "health": "up"},
                        ]
                    }
                }),
                encoding="utf-8",
            )
            (prom_dir / "query_range.json").write_text(
                json.dumps({
                    "queries": {
                        "process_cpu_cores_by_job": {
                            "expr": "rate(process_cpu_seconds_total[20s])",
                            "response": {
                                "status": "success",
                                "data": {
                                    "result": [
                                        {
                                            "metric": {"job": "bowl-storage"},
                                            "values": [[1700000000, "1.25"], [1700000002, "2.80"]],
                                        }
                                    ]
                                },
                            },
                        }
                    }
                }),
                encoding="utf-8",
            )

            srv_m = stage_dir / "server" / "metrics"
            srv_m.mkdir(parents=True)
            prom_text = "\n".join([
                'rpc_server_call_duration_seconds_bucket{rpc_method="storage.v1.StorageService/Write",le="0.005"} 80',
                'rpc_server_call_duration_seconds_bucket{rpc_method="storage.v1.StorageService/Write",le="0.01"} 100',
                'rpc_server_call_duration_seconds_bucket{rpc_method="storage.v1.StorageService/Write",le="+Inf"} 100',
                'rpc_server_call_duration_seconds_sum{rpc_method="storage.v1.StorageService/Write"} 0.35',
                'rpc_server_call_duration_seconds_count{rpc_method="storage.v1.StorageService/Write"} 100',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="validate_mutate",le="0.001"} 95',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="validate_mutate",le="+Inf"} 100',
                'bowl_storage_actor_tick_phase_duration_seconds_sum{phase="validate_mutate"} 0.04',
                'bowl_storage_actor_tick_phase_duration_seconds_count{phase="validate_mutate"} 100',
                'bowl_storage_pebble_block_cache_hits_total 900',
                'bowl_storage_pebble_block_cache_misses_total 100',
            ])
            (srv_m / "final.prom").write_text(prom_text + "\n", encoding="utf-8")

            # 5. Loki & client logs
            loki_dir = stage_dir / "server" / "loki"
            loki_dir.mkdir(parents=True)
            (loki_dir / "query_range.json").write_text(
                json.dumps({
                    "status": "success",
                    "data": {
                        "result": [
                            {
                                "stream": {"service": "storage-service"},
                                "values": [
                                    [
                                        "1700000000000000000",
                                        json.dumps({"level": "WARN", "msg": "slow_rpc", "trace_id": "abcdef1234567890abcdef1234567890"}),
                                    ]
                                ],
                            }
                        ]
                    },
                }),
                encoding="utf-8",
            )
            cli_logs = stage_dir / "client" / "logs"
            cli_logs.mkdir(parents=True)
            (cli_logs / "bowl-mount.log").write_text(
                "\n".join([
                    json.dumps({"level": "INFO", "msg": "mount_ready", "disk_id": "d1"}),
                    json.dumps({"level": "WARN", "msg": "slow_fuse_op", "trace_id": "abcdef1234567890abcdef1234567890"}),
                ]) + "\n",
                encoding="utf-8",
            )

            overview = collect_telemetry.build_telemetry_bundle(
                suite="juicefs",
                stage_dir=stage_dir,
                results_dir=results_dir,
            )

            # Verify output files
            telem_dir = results_dir / "telemetry"
            self.assertTrue((telem_dir / "overview.json").exists())
            self.assertTrue((telem_dir / "overview.md").exists())
            self.assertTrue((telem_dir / "client_stats" / "phase_deltas.json").exists())
            self.assertTrue((telem_dir / "jaeger" / "slow_traces.json").exists())
            self.assertTrue((telem_dir / "jaeger" / "error_traces.json").exists())
            self.assertTrue((telem_dir / "prometheus" / "metrics_summary.json").exists())
            self.assertTrue((telem_dir / "loki" / "container_logs.jsonl").exists())
            self.assertTrue((telem_dir / "loki" / "client_logs.jsonl").exists())

            # Verify client phase delta math
            fio_delta = overview["client_stats"]["phase_deltas"]["fio_seq_write_1job"]
            self.assertEqual(fio_delta["top_ops_by_total_time"][0]["op"], "Write")
            self.assertEqual(fio_delta["top_ops_by_total_time"][0]["calls"], 90)
            self.assertAlmostEqual(fio_delta["top_ops_by_total_time"][0]["total_duration_ms"], 300.0, places=2)
            self.assertEqual(fio_delta["nscache_attr_hit_rate_pct"], 88.89)

            # Verify cross-node Jaeger trace distillation
            self.assertEqual(overview["jaeger"]["cross_node_traces_count"], 1)
            self.assertEqual(overview["jaeger"]["error_traces_count"], 1)
            tr0 = overview["jaeger"]["slow_traces_top15"][0]
            self.assertAlmostEqual(tr0["total_duration_ms"], 4.5, places=2)
            self.assertAlmostEqual(tr0["network_or_client_overhead_ms"], 1.3, places=2)

            # Verify Prometheus histogram & cache hit rate
            srv_eng = overview["prometheus"]["server_engine"]
            self.assertEqual(srv_eng["counters_and_gauges"]["pebble_block_cache_hit_rate_pct"], 90.0)
            self.assertEqual(srv_eng["rpc_server_latency_ms"][0]["group"], "storage.v1.StorageService/Write")
            self.assertEqual(srv_eng["rpc_server_latency_ms"][0]["count"], 100)
            self.assertAlmostEqual(srv_eng["rpc_server_latency_ms"][0]["mean"], 3.5, places=2)

            # Verify logs counts
            self.assertEqual(overview["logs"]["server_alert_count"], 1)
            self.assertEqual(overview["logs"]["client_alert_count"], 1)

            # Verify report section rendering
            loaded = collect_telemetry.load_telemetry_overview(results_dir)
            self.assertEqual(loaded["suite"], "juicefs")
            sec_lines = collect_telemetry.render_report_telemetry_section(
                loaded, "## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）"
            )
            sec_md = "\n".join(sec_lines)
            self.assertIn("results/telemetry/overview.md", sec_md)
            self.assertIn("storage.v1.StorageService/Write", sec_md)
            self.assertIn("abcdef1234567890", sec_md)

    def test_all_three_suite_parsers_support_legacy_and_deploy_prod_telemetry(self) -> None:
        sample_overview = {
            "suite": "bench",
            "deploy_profile": "deploy/prod (docker-compose.yaml + storage.env)",
            "client_stats": {
                "top_ops_by_total_time": [
                    {
                        "op": "Write",
                        "calls": 100,
                        "total_duration_ms": 250.0,
                        "share_of_fuse_time_pct": 85.0,
                        "mean_us": 2500.0,
                        "p95_us": 4096.0,
                        "p99_us": 4096.0,
                    }
                ],
                "nscache": {"attr_hit_rate_pct": 92.5, "entry_hit_rate_pct": 88.0, "negative_entry_hits": 12},
                "read_ahead": {"prefetched_mib": 64.0, "hit_mib": 60.0},
            },
            "prometheus": {
                "server_engine": {
                    "rpc_server_latency_ms": [
                        {
                            "group": "storage.v1.StorageService/Write",
                            "count": 100,
                            "total": 200.0,
                            "mean": 2.0,
                            "p95": 3.5,
                            "p99": 4.2,
                        }
                    ],
                    "actor_tick_phase_ms": [
                        {
                            "group": "wal_commit",
                            "count": 50,
                            "total": 40.0,
                            "mean": 0.8,
                            "p95": 1.2,
                            "p99": 1.5,
                        }
                    ],
                    "counters_and_gauges": {
                        "pebble_block_cache_hit_rate_pct": 95.0,
                        "pebble_read_amp": 1.0,
                        "pebble_stall_events": 0.0,
                    },
                }
            },
            "jaeger": {
                "sampled_traces_count": 12,
                "cross_node_traces_count": 10,
                "error_traces_count": 0,
                "slow_traces_top15": [
                    {
                        "trace_id": "11223344556677889900aabbccddeeff",
                        "total_duration_ms": 6.4,
                        "root_service": "bowl-client",
                        "root_operation": "fuse.Write",
                        "client_max_span_ms": 6.4,
                        "server_max_span_ms": 4.8,
                        "top_spans": [
                            {"service": "bowl-client", "operation": "fuse.Write", "duration_ms": 6.4},
                            {"service": "bowl-storage", "operation": "storage.v1.StorageService/Write", "duration_ms": 4.8},
                        ],
                    }
                ],
            },
            "logs": {"loki_log_entries": 42, "server_alert_count": 0, "client_alert_count": 0},
            "pprof_profiles": [
                {
                    "side": "server",
                    "profile": "fio_seq_write_1job_cpu.pb.gz",
                    "report_file": "pprof_top/server_fio_seq_write_1job_cpu.txt",
                    "top_entries": ["1.20s 45.0% runtime.Syscall"],
                }
            ],
        }

        for suite in ("juicefs", "drive9", "pjdfstest"):
            mod = load_suite_parser(suite)
            src_results = BENCH_ROOT / suite / "results"
            with tempfile.TemporaryDirectory() as tmp:
                tmp_res = Path(tmp) / "results"
                shutil.copytree(src_results, tmp_res)

                # 1. Legacy results without telemetry/overview.json
                if suite == "pjdfstest":
                    env_meta = json.loads((tmp_res / "env_metadata.json").read_text(encoding="utf-8"))
                    stage_timings = [
                        json.loads(ln)
                        for ln in (tmp_res / "stage_timings.jsonl").read_text(encoding="utf-8").splitlines()
                        if ln.strip()
                    ]
                    legacy_sum = mod.parse_tap_log(
                        (tmp_res / "pjdfstest.log").read_text(encoding="utf-8"), stage_timings
                    )
                    legacy_sum["environment"] = env_meta
                    legacy_md = mod.render_report_md(legacy_sum, env_meta)
                    self.assertIn("## 6. 阿里云按量资源生命周期与销毁审计", legacy_md)
                    self.assertNotIn("生产可观测性与慢路径遥测诊断", legacy_md)
                else:
                    legacy_sum = mod.build_summary(tmp_res)
                    self.assertNotIn("telemetry_overview", legacy_sum)
                    legacy_md = mod.render_report(legacy_sum)
                    self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", legacy_md)
                    self.assertNotIn("生产可观测性与慢路径遥测诊断", legacy_md)

                # 2. deploy/prod results with telemetry/overview.json
                env_meta = json.loads((tmp_res / "env_metadata.json").read_text(encoding="utf-8"))
                env_meta.setdefault("bowl_config", {})["deploy_profile"] = "deploy/prod (docker-compose.yaml + storage.env)"
                env_meta["bowl_config"]["observability_stack"] = "Jaeger + Prometheus + Loki + Alloy + Grafana"
                env_meta.setdefault("server_node", {})["bowl_pg_version"] = "postgres (PostgreSQL) 18.1 (alpine)"
                (tmp_res / "env_metadata.json").write_text(json.dumps(env_meta), encoding="utf-8")

                (tmp_res / "telemetry").mkdir(parents=True, exist_ok=True)
                (tmp_res / "telemetry" / "overview.json").write_text(
                    json.dumps(sample_overview), encoding="utf-8"
                )

                if suite == "pjdfstest":
                    stage_timings = [
                        json.loads(ln)
                        for ln in (tmp_res / "stage_timings.jsonl").read_text(encoding="utf-8").splitlines()
                        if ln.strip()
                    ]
                    prod_sum = mod.parse_tap_log(
                        (tmp_res / "pjdfstest.log").read_text(encoding="utf-8"), stage_timings
                    )
                    prod_sum["environment"] = env_meta
                    prod_sum["telemetry_overview"] = collect_telemetry.load_telemetry_overview(tmp_res)
                    prod_md = mod.render_report_md(prod_sum, env_meta)
                    self.assertIn("PostgreSQL 18（deploy/prod 容器栈）", prod_md)
                    self.assertIn("## 6. 生产可观测性与慢路径遥测诊断", prod_md)
                    self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", prod_md)
                else:
                    prod_sum = mod.build_summary(tmp_res)
                    self.assertIn("telemetry_overview", prod_sum)
                    prod_md = mod.render_report(prod_sum)
                    self.assertIn("deploy/prod", prod_md)
                    self.assertIn("## 7. 生产可观测性与慢路径遥测诊断", prod_md)
                    self.assertIn("## 8. 阿里云按量资源生命周期与销毁审计", prod_md)
                    self.assertIn("1122334455667788", prod_md)


if __name__ == "__main__":
    unittest.main()
