#!/usr/bin/env python3
"""验证 `benchmark/fork/parse_results.py` 公开接口契约与极值表现。"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

BENCH_ROOT = Path(__file__).resolve().parent.parent
if str(BENCH_ROOT) not in sys.path:
    sys.path.insert(0, str(BENCH_ROOT))

from fork import parse_results as fork_parse_results  # noqa: E402


def _stat_box(median_ms: float) -> dict[str, float | int]:
    """构造单组延迟统计字面量字典（无分支逻辑）。"""
    return {
        "count": 20,
        "trimmed_count": 18,
        "median": median_ms,
        "p95": round(median_ms * 1.2, 3),
        "p99": round(median_ms * 1.4, 3),
        "mean": median_ms,
    }


class ForkParseResultsContractTest(unittest.TestCase):
    """覆盖 `fork/parse_results.py` 统计、评分、摘要构建与报告渲染契约。"""

    def test_given_twenty_samples_with_outliers_when_summarizing_then_trims_five_percent_each_tail(self) -> None:
        # Given: 20 个样本，含极小与极大各 1 个离群值
        samples = [
            9999.0, 0.01, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0,
            10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0,
        ]

        # When: 按 5% 双侧截尾统计
        stats = fork_parse_results.summarize_samples(samples, trim_fraction=0.05)

        # Then: 截去首尾各 1 个离群值，保留 18 个核心样本
        self.assertEqual(stats["count"], 20)
        self.assertEqual(stats["trimmed_count"], 18)
        self.assertAlmostEqual(stats["median"], 10.5, places=3)
        self.assertAlmostEqual(stats["min"], 0.01, places=3)
        self.assertAlmostEqual(stats["max"], 9999.0, places=3)
        self.assertLess(stats["p99"], 20.0)

    def test_given_empty_or_overtrimmed_samples_when_summarizing_then_avoids_zero_division(self) -> None:
        # Given: 空样本与截尾比例 >= 0.5 的 20 样本集合
        twenty_samples = [
            1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0,
            11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0, 20.0,
        ]

        # When: 执行汇总统计
        empty_stats = fork_parse_results.summarize_samples([])
        half_trim_stats = fork_parse_results.summarize_samples(twenty_samples, trim_fraction=0.5)

        # Then: 空集归零，过度截尾不触发除零异常
        self.assertEqual(empty_stats["count"], 0)
        self.assertEqual(empty_stats["median"], 0.0)
        self.assertEqual(half_trim_stats["count"], 20)
        self.assertGreater(half_trim_stats["trimmed_count"], 0)

    def test_given_verification_and_latency_when_computing_composite_score_then_enforces_ceiling_and_gate(self) -> None:
        # Given / When / Then: 未校验返回 0，超上限归零，零延迟满分，6ms 得 99.99
        self.assertEqual(fork_parse_results.compute_composite_score(5.0, 2.0, verified=False), 0.0)
        self.assertEqual(fork_parse_results.compute_composite_score(60000.0, 60000.0, verified=True), 0.0)
        self.assertEqual(fork_parse_results.compute_composite_score(0.0, 0.0, verified=True), 100.0)
        self.assertEqual(fork_parse_results.compute_composite_score(6.0, 6.0, verified=True), 99.99)
        self.assertEqual(fork_parse_results.compute_composite_score(-10.0, -5.0, verified=True), 100.0)

    def test_given_empty_or_malformed_json_and_jsonl_when_building_summary_then_returns_safe_defaults(self) -> None:
        # Given: 空 env_metadata.json、畸形 suite1_matrix.json 与含坏行的 stage_timings.jsonl
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            (res_dir / "env_metadata.json").write_text("", encoding="utf-8")
            (res_dir / "suite1_matrix.json").write_text("{corrupt", encoding="utf-8")
            (res_dir / "suite2_online_fork.json").write_text("[]", encoding="utf-8")
            (res_dir / "stage_timings.jsonl").write_text(
                "{bad-json\n"
                + json.dumps({"stage": "suite1_fork_matrix", "duration_sec": 12.3}) + "\n",
                encoding="utf-8",
            )

            # When: 构建摘要并渲染报告
            summary = fork_parse_results.build_summary(res_dir)
            report_md = fork_parse_results.render_report(summary)

            # Then: 不崩溃且保留合法 stage_timings 行
            self.assertEqual(summary["env_metadata"], {})
            self.assertEqual(summary["suite1_matrix"], {})
            self.assertEqual(summary["suite2_online_fork"], {})
            self.assertEqual(len(summary["stage_timings"]), 1)
            self.assertIn("## 1. 执行摘要", report_md)

    def test_given_null_nested_fields_in_suites_when_rendering_report_then_handles_without_type_error(self) -> None:
        # Given: suite1 与 suite3 中存在显式 null 统计子字段
        summary = {
            "env_metadata": {"server_node": {"bowl_pg_version": None, "pg_version": "PG 16"}},
            "suite1_matrix": {
                "preset_order": ["small"],
                "presets": {
                    "small": {
                        "label": "small",
                        "dataset_mib": None,
                        "seed_ms": None,
                        "bowl_fork": {
                            "fork_rpc_ms": None,
                            "fork_cli_ms": None,
                            "end_to_end_tti_ms": {"median": None},
                            "cold_first_read_ms": None,
                            "fork_plus_mount_ms": None,
                        },
                        "bowl_copy": {"copy_sync_ms": None, "end_to_end_tti_ms": None, "provision_plus_copy_ms": None},
                        "comparison": {"fork_rpc_vs_copy_sync_speedup": None},
                    }
                },
            },
            "suite2_online_fork": {},
            "suite3_fanout_cow": {"parent_seed_ms": None, "checkpoints": {"after_parent_seed": {"logical_bytes": 1024}}},
        }

        # When: 渲染报告
        report_md = fork_parse_results.render_report(summary)

        # Then: 顺利渲染且不包含未捕获异常
        self.assertIn("PG 16", report_md)
        self.assertIn("## 1. 执行摘要", report_md)

    def test_given_complete_legacy_results_when_building_and_rendering_then_produces_seven_sections(self) -> None:
        # Given: 无遥测目录的完整三套件临时结果
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            env_meta = {
                "cloud": {"provider": "Aliyun ECS", "zone_id": "us-west-1a", "instance_type": "ecs.i5.xlarge"},
                "server_node": {"instance_id": "i-srv-001", "private_ip": "172.27.96.10", "vcpu": 4, "memory_gib": "30Gi"},
                "client_node": {"instance_id": "i-cli-001", "private_ip": "172.27.96.11", "vcpu": 4, "memory_gib": "30Gi"},
                "network_baseline": {"ping_rtt_ms": "0.15", "iperf3_gbps": 9.85},
                "server_disk_baseline": {"local_seq_write_mib_s": 1350.0, "local_seq_read_mib_s": 2800.0},
                "bowl_config": {"git_commit": "8a07a55"},
                "cleanup_audit": {"remaining_benchmark_instances": 0, "remaining_keypairs": 0},
            }
            suite1 = {
                "preset_order": ["small", "large_1gib", "tree_5k"],
                "presets": {
                    "small": {
                        "label": "ComputeSDK small (10 × 1 MiB)",
                        "dataset_mib": 10.0,
                        "seed_ms": 42.0,
                        "bowl_fork": {
                            "iterations": 20,
                            "verified_count": 20,
                            "physical_delta_bytes_on_fork": 32768,
                            "fork_rpc_ms": _stat_box(4.5),
                            "fork_cli_ms": _stat_box(18.0),
                            "mount_ready_ms": _stat_box(35.0),
                            "fork_plus_mount_ms": _stat_box(39.5),
                            "cold_first_read_ms": _stat_box(3.2),
                            "end_to_end_tti_ms": _stat_box(42.7),
                            "delete_ms": _stat_box(22.0),
                            "computesdk_score_rpc": 99.99,
                            "computesdk_score_with_mount": 99.93,
                        },
                        "bowl_copy": {
                            "physical_delta_bytes_on_copy": 10485760,
                            "copy_sync_ms": _stat_box(68.0),
                            "provision_plus_copy_ms": _stat_box(105.0),
                            "cold_first_read_ms": _stat_box(3.3),
                            "end_to_end_tti_ms": _stat_box(108.3),
                            "computesdk_score": 99.84,
                        },
                        "comparison": {
                            "fork_rpc_vs_copy_sync_speedup": 15.11,
                            "fork_mount_vs_provision_copy_speedup": 2.66,
                            "end_to_end_tti_speedup": 2.54,
                            "physical_bytes_saved_bytes": 10452992,
                            "physical_bytes_ratio_copy_over_fork": 320.0,
                        },
                    },
                    "large_1gib": {
                        "label": "1 GiB",
                        "dataset_mib": 1024.0,
                        "seed_ms": 1450.0,
                        "bowl_fork": {
                            "iterations": 20,
                            "verified_count": 20,
                            "physical_delta_bytes_on_fork": 40960,
                            "fork_rpc_ms": _stat_box(5.2),
                            "fork_cli_ms": _stat_box(19.0),
                            "mount_ready_ms": _stat_box(36.0),
                            "fork_plus_mount_ms": _stat_box(41.2),
                            "cold_first_read_ms": _stat_box(92.0),
                            "end_to_end_tti_ms": _stat_box(133.2),
                            "delete_ms": _stat_box(24.0),
                            "computesdk_score_rpc": 99.98,
                            "computesdk_score_with_mount": 99.92,
                        },
                        "bowl_copy": {
                            "physical_delta_bytes_on_copy": 1073741824,
                            "copy_sync_ms": _stat_box(1820.0),
                            "provision_plus_copy_ms": _stat_box(1860.0),
                            "cold_first_read_ms": _stat_box(94.0),
                            "end_to_end_tti_ms": _stat_box(1954.0),
                            "computesdk_score": 97.19,
                        },
                        "comparison": {
                            "fork_rpc_vs_copy_sync_speedup": 350.0,
                            "fork_mount_vs_provision_copy_speedup": 45.15,
                            "end_to_end_tti_speedup": 14.67,
                            "physical_bytes_saved_bytes": 1073700864,
                            "physical_bytes_ratio_copy_over_fork": 26214.4,
                        },
                    },
                    "tree_5k": {
                        "label": "5k",
                        "dataset_mib": 19.5,
                        "seed_ms": 2100.0,
                        "bowl_fork": {
                            "iterations": 20,
                            "verified_count": 20,
                            "physical_delta_bytes_on_fork": 65536,
                            "fork_rpc_ms": _stat_box(6.1),
                            "fork_cli_ms": _stat_box(20.0),
                            "mount_ready_ms": _stat_box(36.0),
                            "fork_plus_mount_ms": _stat_box(42.1),
                            "cold_first_read_ms": _stat_box(1.4),
                            "end_to_end_tti_ms": _stat_box(43.5),
                            "delete_ms": _stat_box(25.0),
                            "computesdk_score_rpc": 99.99,
                            "computesdk_score_with_mount": 99.94,
                        },
                        "bowl_copy": {
                            "physical_delta_bytes_on_copy": 20480000,
                            "copy_sync_ms": _stat_box(2450.0),
                            "provision_plus_copy_ms": _stat_box(2490.0),
                            "cold_first_read_ms": _stat_box(1.5),
                            "end_to_end_tti_ms": _stat_box(2491.5),
                            "computesdk_score": 96.26,
                        },
                        "comparison": {
                            "fork_rpc_vs_copy_sync_speedup": 401.64,
                            "fork_mount_vs_provision_copy_speedup": 59.14,
                            "end_to_end_tti_speedup": 57.28,
                            "physical_bytes_saved_bytes": 20414464,
                            "physical_bytes_ratio_copy_over_fork": 312.5,
                        },
                    },
                },
            }
            suite2 = {
                "parent_used_bytes_after_seed_and_orphans": 50331648,
                "online_fork_count": 10,
                "writer_errors": 0,
                "writer_summary": {
                    "aggregate_throughput_mib_s": 310.5,
                    "stream_4mib_ops": 180,
                    "stream_4mib_before_fork_ms": _stat_box(24.0),
                    "stream_4mib_during_fork_ms": _stat_box(26.5),
                    "small_4kib_ops": 1400,
                    "small_4kib_ops_per_sec": 290.0,
                    "small_4kib_before_fork_ms": _stat_box(3.1),
                    "small_4kib_during_fork_ms": _stat_box(3.5),
                },
                "online_fork_rpc_ms": _stat_box(6.8),
                "online_forks": [{"fork_index": 0, "fork_rpc_ms": 6.5, "child_used_bytes": 100, "child_live_visible_bytes": 100, "orphan_bytes_in_child": 0, "sanitized_orphan_bytes": 16777216, "committed_files_verified": 32, "small_files_verified": 420, "corrupted_files": 0}],
                "parent_reclaimed_orphan_bytes_on_close": 16777216,
            }
            suite3 = {
                "parent_seed_ms": 1420.0,
                "fanout_fork_total_ms": 88.5,
                "fanout_fork_rpc_ms": _stat_box(5.4),
                "checkpoints": {
                    "after_parent_seed": {"logical_bytes": 1073741824, "du_sb_bytes": 1073807360, "commits_dir_count": 0, "parent_pack_count": 16, "parent_pack_max_nlink": 1},
                    "after_16_forks": {"logical_bytes": 18253611008, "du_sb_bytes": 1074200000, "commits_dir_count": 16, "shared_pack_max_nlink": 33, "logical_to_physical_ratio": 16.99},
                    "after_concurrent_cow_writes": {"logical_bytes": 18320719872, "du_sb_bytes": 1141500000, "commits_dir_count": 16, "shared_pack_max_nlink": 33, "delta_pack_nlink": 1, "space_amplification_vs_unique": 1.0006, "physical_savings_pct": 93.77},
                    "after_parent_delete": {"logical_bytes": 17246978048, "du_sb_bytes": 1141450000, "commits_dir_count": 16, "shared_pack_max_nlink": 32, "children_verified": 16},
                    "after_15_children_delete_sole_survivor": {"logical_bytes": 1077936128, "du_sb_bytes": 1078000000, "commits_dir_count": 0, "survivor_pack_count": 17, "survivor_pack_min_nlink": 1, "survivor_pack_max_nlink": 1},
                },
            }
            (res_dir / "env_metadata.json").write_text(json.dumps(env_meta), encoding="utf-8")
            (res_dir / "suite1_matrix.json").write_text(json.dumps(suite1), encoding="utf-8")
            (res_dir / "suite2_online_fork.json").write_text(json.dumps(suite2), encoding="utf-8")
            (res_dir / "suite3_fanout_cow.json").write_text(json.dumps(suite3), encoding="utf-8")

            # When: 构建摘要与渲染报告
            summary = fork_parse_results.build_summary(res_dir)
            report_md = fork_parse_results.render_report(summary)

            # Then: 输出 7 节结构且包含对照数据与审计节
            self.assertNotIn("telemetry_overview", summary)
            self.assertIn("Tensorlake", report_md)
            self.assertIn("i-srv-001", report_md)
            self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", report_md)
            self.assertNotIn("生产可观测性与慢路径遥测诊断", report_md)

    def test_given_prod_telemetry_when_running_cli_then_renders_idempotent_eight_sections(self) -> None:
        # Given: 含 deploy/prod telemetry/overview.json 的临时结果目录
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            telem_dir = res_dir / "telemetry"
            telem_dir.mkdir(parents=True)
            summary_out = Path(tmp) / "out" / "summary.json"
            report_out = Path(tmp) / "out" / "REPORT.md"

            (res_dir / "env_metadata.json").write_text(
                json.dumps({
                    "bowl_config": {"deploy_profile": "deploy/prod", "observability_stack": "Jaeger+Prom+Loki"},
                    "server_node": {"bowl_pg_version": "PostgreSQL 18.1"},
                }),
                encoding="utf-8",
            )
            (telem_dir / "overview.json").write_text(
                json.dumps({
                    "suite": "fork",
                    "phase_deltas": {
                        "fork_small": {
                            "phase": "fork_small",
                            "wall_clock_sec": 1.0,
                            "wall_clock_ms": 1000.0,
                            "fuse_active_ms": 50.0,
                            "cache_and_pipeline": {"pebble_block_cache_hit_rate_pct": None},
                            "hops": [{"hop_stage": "1. FUSE 入口", "component": "fuse.*", "calls": 10, "total_ms": 50.0, "mean_ms": 5.0, "p50_ms": 4.0, "p99_ms": 9.0, "share_pct": 100.0}],
                        }
                    },
                    "logs": {"loki_log_entries": 2, "server_alert_count": 0, "client_alert_count": 0},
                }),
                encoding="utf-8",
            )

            # When: 通过命令行接口执行 parse_results.py
            proc = subprocess.run(
                [
                    sys.executable,
                    str(BENCH_ROOT / "fork" / "parse_results.py"),
                    "--results-dir",
                    str(res_dir),
                    "--summary-out",
                    str(summary_out),
                    "--report-out",
                    str(report_out),
                ],
                capture_output=True,
                text=True,
                check=False,
            )

            # Then: 退出码为 0，生成 8 节报告且重复渲染幂等一致，无 None%
            self.assertEqual(proc.returncode, 0, msg=f"stderr={proc.stderr}")
            report_md = report_out.read_text(encoding="utf-8")
            rerendered_md = fork_parse_results.render_report(fork_parse_results.build_summary(res_dir))
            self.assertEqual(report_md, rerendered_md)
            self.assertIn("PostgreSQL 18（deploy/prod 容器栈）", report_md)
            self.assertIn("## 7. 生产可观测性与慢路径遥测诊断", report_md)
            self.assertIn("## 8. 阿里云按量资源生命周期与销毁审计", report_md)
            self.assertIn("N/A (0 reads)", report_md)
            self.assertNotIn("None%", report_md)


if __name__ == "__main__":
    unittest.main()
