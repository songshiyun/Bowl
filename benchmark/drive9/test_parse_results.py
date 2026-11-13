#!/usr/bin/env python3
"""验证 `benchmark/drive9/parse_results.py` 公开接口契约与极值表现。"""

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

from drive9 import parse_results as drive9_parse_results  # noqa: E402


class Drive9ParseResultsContractTest(unittest.TestCase):
    """覆盖 `drive9/parse_results.py` 解析、比值计算、摘要构建与报告渲染契约。"""

    def test_given_ansi_bench_table_and_malformed_row_when_parsing_then_extracts_valid_items(self) -> None:
        # Given: 含 ANSI 转义、畸形数字行与正常数据行的 bench 日志
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "bench_bowl.log"
            log_path.write_text(
                "\x1b[36m| Write big file | 512.50 MiB/s | 2.00 s/file |\x1b[0m\n"
                "| Read big file | 9.8.7 MiB/s | 1.00 s/file |\n",
                encoding="utf-8",
            )
            empty_path = Path(tmp) / "empty.log"
            empty_path.write_text("", encoding="utf-8")

            # When: 解析日志文件
            parsed = drive9_parse_results.parse_juicefs_bench(log_path)
            empty_parsed = drive9_parse_results.parse_juicefs_bench(empty_path)

            # Then: 有效项正确解析，畸形行被忽略，空文件返回空字典
            self.assertEqual(empty_parsed, {})
            self.assertEqual(parsed["Write big file"]["value"], 512.5)
            self.assertNotIn("Read big file", parsed)

    def test_given_zero_or_nonfinite_inputs_when_formatting_ratios_then_returns_dash(self) -> None:
        # Given / When / Then: 吞吐比与耗时加速比在零分母、None 与 NaN/Inf 下返回破折号
        self.assertEqual(drive9_parse_results.fmt_throughput_ratio(100.0, 0.0), "—")
        self.assertEqual(drive9_parse_results.fmt_throughput_ratio(100.0, None), "—")
        self.assertEqual(drive9_parse_results.fmt_throughput_ratio(float("nan"), 10.0), "—")
        self.assertEqual(drive9_parse_results.fmt_throughput_ratio(200.0, 50.0), "4.00x")

        self.assertEqual(drive9_parse_results.fmt_latency_speedup(0.0, 5.0), "—")
        self.assertEqual(drive9_parse_results.fmt_latency_speedup(None, 5.0), "—")
        self.assertEqual(drive9_parse_results.fmt_latency_speedup(2.0, float("inf")), "—")
        self.assertEqual(drive9_parse_results.fmt_latency_speedup(1.5, 3.0), "2.00x")

    def test_given_empty_or_malformed_json_and_jsonl_when_building_summary_then_returns_safe_defaults(self) -> None:
        # Given: 空 env_metadata.json、畸形 JSON 文件、非字典 JSON 与含坏行的 stage_timings.jsonl
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            (res_dir / "env_metadata.json").write_text("", encoding="utf-8")
            (res_dir / "drive9_fuse_bench_cold.json").write_text("{broken", encoding="utf-8")
            (res_dir / "perf_baseline_bowl.json").write_text("[]", encoding="utf-8")
            (res_dir / "stage_timings.jsonl").write_text(
                "{corrupt\n" + json.dumps({"stage": "d9_cold", "duration_sec": 8.4}) + "\n",
                encoding="utf-8",
            )

            # When: 构建摘要并渲染报告
            summary = drive9_parse_results.build_summary(res_dir)
            report_md = drive9_parse_results.render_report(summary)

            # Then: 不抛异常，保留合法 stage_timings
            self.assertEqual(summary["env_metadata"], {})
            self.assertEqual(summary["drive9_fuse_bench_cold"], {})
            self.assertEqual(summary["fuse_perf_baseline"]["bowl"], {})
            self.assertEqual(len(summary["stage_timings"]), 1)
            self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", report_md)

    def test_given_null_nested_targets_and_cost_when_rendering_report_then_handles_gracefully(self) -> None:
        # Given: targets 与 cost 包含显式 null 的摘要结构
        summary = {
            "env_metadata": {},
            "juicefs_bench": {"bowl": {"Write big file": {"value": 300.0, "cost": None}}},
            "drive9_fuse_bench_cold": {"targets": {"bowl": None, "drive9_default": {"results": None}}},
            "drive9_fuse_bench_hot": {"targets": {}},
            "fuse_perf_baseline": {"bowl": None},
        }

        # When: 渲染报告
        report_md = drive9_parse_results.render_report(summary)

        # Then: 顺利完成渲染
        self.assertIn("300.00 MiB/s", report_md)

    def test_given_complete_legacy_results_when_building_and_rendering_then_computes_micro_and_macro_comparisons(self) -> None:
        # Given: 无遥测目录的完整 Drive9 对比数据
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            (res_dir / "env_metadata.json").write_text(
                json.dumps({
                    "cloud": {"provider": "Aliyun ECS", "zone_id": "us-west-1a", "instance_type": "ecs.i5.xlarge"},
                    "server_node": {"instance_id": "i-d9-srv", "private_ip": "172.16.1.10"},
                    "client_node": {"instance_id": "i-d9-cli", "private_ip": "172.16.1.11"},
                    "bowl_config": {"git_commit": "abc1234"},
                    "drive9_config": {"git_commit": "def5678"},
                }),
                encoding="utf-8",
            )
            (res_dir / "bench_bowl.log").write_text("| Write big file | 400.00 MiB/s | 2.56 s/file |\n", encoding="utf-8")
            (res_dir / "bench_drive9_default.log").write_text("| Write big file | 200.00 MiB/s | 5.12 s/file |\n", encoding="utf-8")
            (res_dir / "bench_drive9_sync.log").write_text("| Write big file | 100.00 MiB/s | 10.24 s/file |\n", encoding="utf-8")
            (res_dir / "drive9_fuse_bench_cold.json").write_text(
                json.dumps({
                    "targets": {
                        "bowl": {"results": {"sequential_write": {"median": 360.0, "p95": 320.0}, "macro_git_clone": {"median": 0.500, "p95": 0.600}}},
                        "drive9_default": {"results": {"sequential_write": {"median": 180.0, "p95": 150.0}, "macro_git_clone": {"median": 1.000, "p95": 1.200}}},
                        "drive9_sync": {"results": {"sequential_write": {"median": 90.0, "p95": 80.0}, "macro_git_clone": {"median": 2.000, "p95": 2.500}}},
                    }
                }),
                encoding="utf-8",
            )
            (res_dir / "perf_baseline_bowl.json").write_text(
                json.dumps({"workloads": {"sqlite_wal_insert_transaction": {"rows_per_second": 2000.0}, "large_file_reads": [{"mib_per_second": 600.0}, {"mib_per_second": 620.0}]}}),
                encoding="utf-8",
            )
            (res_dir / "perf_baseline_drive9_sync.json").write_text(
                json.dumps({"workloads": {"sqlite_wal_insert_transaction": {"rows_per_second": 500.0}, "large_file_reads": [{"mib_per_second": 300.0}, {"mib_per_second": 310.0}]}}),
                encoding="utf-8",
            )

            # When: 构建摘要与渲染报告
            summary = drive9_parse_results.build_summary(res_dir)
            report_md = drive9_parse_results.render_report(summary)

            # Then: 输出 7 节报告且微/宏观对比比值正确
            self.assertNotIn("telemetry_overview", summary)
            self.assertIn("**4.00x**", report_md)
            self.assertIn("`0.500s (0.600s)`", report_md)
            self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", report_md)
            self.assertNotIn("生产可观测性与慢路径遥测诊断", report_md)

    def test_given_prod_telemetry_when_running_cli_then_renders_eight_sections_without_none_pct(self) -> None:
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
                    "suite": "drive9",
                    "phase_deltas": {
                        "d9_cold_seq_read": {
                            "phase": "d9_cold_seq_read",
                            "wall_clock_sec": 2.0,
                            "wall_clock_ms": 2000.0,
                            "fuse_active_ms": 200.0,
                            "cache_and_pipeline": {"pebble_block_cache_hit_rate_pct": None},
                            "hops": [
                                {
                                    "hop_stage": "3. 跨机网络+编解码差值",
                                    "component": "同跳 Client ConnectRPC - Server ConnectRPC",
                                    "calls": 64,
                                    "total_ms": 32.0,
                                    "mean_ms": 0.5,
                                    "p50_ms": 0.4,
                                    "p99_ms": 0.8,
                                    "share_pct": 16.0,
                                }
                            ],
                        }
                    },
                    "logs": {"loki_log_entries": 5, "server_alert_count": 0, "client_alert_count": 0},
                }),
                encoding="utf-8",
            )

            # When: 通过命令行接口执行 parse_results.py
            proc = subprocess.run(
                [
                    sys.executable,
                    str(BENCH_ROOT / "drive9" / "parse_results.py"),
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

            # Then: 退出码 0，生成 8 节报告与 summary.json，含 N/A (0 reads) 且无 None%
            self.assertEqual(proc.returncode, 0, msg=f"stderr={proc.stderr}")
            self.assertTrue(summary_out.exists())
            report_md = report_out.read_text(encoding="utf-8")
            self.assertIn("PostgreSQL 18（deploy/prod 容器栈）", report_md)
            self.assertIn("## 7. 生产可观测性与慢路径遥测诊断", report_md)
            self.assertIn("## 8. 阿里云按量资源生命周期与销毁审计", report_md)
            self.assertIn("N/A (0 reads)", report_md)
            self.assertNotIn("None%", report_md)


if __name__ == "__main__":
    unittest.main()
