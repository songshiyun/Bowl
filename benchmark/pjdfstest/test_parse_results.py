#!/usr/bin/env python3
"""验证 `benchmark/pjdfstest/parse_results.py` 公开接口契约与极值表现。"""

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

from pjdfstest import parse_results as pjdfstest_parse_results  # noqa: E402


class PjdfstestParseResultsContractTest(unittest.TestCase):
    """覆盖 `pjdfstest/parse_results.py` TAP 解析、退出码聚合与报告渲染契约。"""

    def test_given_passing_tap_log_with_timer_when_parsing_then_computes_exact_totals(self) -> None:
        # Given: 2 个目录共 3 个通过的 TAP 用例与目录耗时记录
        log_text = "\n".join([
            "[13:12:48] /opt/pjdfstest/tests/chflags/00.t .. ",
            "1..2",
            "ok 1",
            "ok 2",
            "ok        6 ms ( 0.00 usr  0.00 sys =  0.00 CPU)",
            "[13:12:49] /opt/pjdfstest/tests/chmod/00.t .. ",
            "1..3",
            "ok 1",
            "ok 2",
            "ok 3",
            "ok       14 ms ( 0.00 usr  0.00 sys =  0.00 CPU)",
        ])
        dir_timings = [
            {"directory": "chflags", "rc": 0, "duration_sec": 0.5},
            {"directory": "chmod", "rc": 0, "duration_sec": 1.5},
        ]

        # When: 解析 TAP 日志
        summary = pjdfstest_parse_results.parse_tap_log(log_text, dir_timings)

        # Then: 精确汇总目录、文件、断言数、通过率与吞吐率
        self.assertEqual(summary["total_directories"], 2)
        self.assertEqual(summary["total_files"], 2)
        self.assertEqual(summary["total_tests"], 5)
        self.assertEqual(summary["passed_tests"], 5)
        self.assertEqual(summary["failed_tests"], 0)
        self.assertEqual(summary["pass_rate"], 1.0)
        self.assertAlmostEqual(summary["total_duration_sec"], 2.0, places=3)
        self.assertAlmostEqual(summary["overall_throughput_tests_per_sec"], 2.5, places=2)
        self.assertEqual(summary["cases"][0]["status"], "PASS")
        self.assertEqual(summary["cases"][0]["duration_ms"], 6)

    def test_given_failures_todo_and_dubious_crash_when_parsing_then_aggregates_accurately(self) -> None:
        # Given: 含显式 not ok、not ok # TODO 预期失败、以及无 not ok 行的 Dubious 非零退出用例
        log_text = "\n".join([
            "[13:12:48] /opt/pjdfstest/tests/chmod/00.t .. ",
            "1..4",
            "ok 1",
            "not ok 2 # TODO known linux behavior",
            "not ok 3",
            "Failed 2/4 subtests",
            "[13:12:49] /opt/pjdfstest/tests/unlink/00.t .. ",
            "1..2",
            "ok 1 # TODO posix allows",
            "ok 2",
            "ok        5 ms ( 0.00 usr  0.00 sys =  0.00 CPU)",
            "[13:12:50] /opt/pjdfstest/tests/rename/00.t .. ",
            "Dubious, test returned 2 (wstat 512, 0x200)",
            "ok        3 ms ( 0.00 usr  0.00 sys =  0.00 CPU)",
        ])
        dir_timings = [
            {"directory": "chmod", "rc": 1, "duration_sec": 1.0},
            {"directory": "unlink", "rc": 0, "duration_sec": 0.5},
            {"directory": "rename", "rc": 2, "duration_sec": 0.2},
        ]

        # When: 解析 TAP 日志
        summary = pjdfstest_parse_results.parse_tap_log(log_text, dir_timings)

        # Then: TODO 不计入失败，chmod 记 2 失败，rename Dubious 记 1 失败
        self.assertEqual(summary["todo_tests"], 2)
        self.assertEqual(summary["todo_expected_failures"], 1)
        self.assertEqual(summary["passed_tests"], 4)
        self.assertEqual(summary["failed_tests"], 3)
        self.assertEqual(len(summary["failed_cases"]), 2)
        self.assertEqual(summary["failed_cases"][0]["case"], "chmod/00.t")
        self.assertEqual(summary["failed_cases"][0]["failed_nums"], [3])
        self.assertEqual(summary["failed_cases"][1]["case"], "rename/00.t")
        self.assertEqual(summary["failed_cases"][1]["status"], "FAIL")

    def test_given_directory_with_nonzero_rc_and_no_tap_cases_when_parsing_then_preserves_directory_rc(self) -> None:
        # Given: 日志中没有该目录的 TAP 用例，且 dir_timings 中存在重复记录与非零退出码及 None 字段
        dir_timings = [
            {"directory": "broken_dir", "rc": 2, "duration_sec": 0.3},
            {"directory": "broken_dir", "rc": 0, "duration_sec": None},
        ]

        # When: 解析日志并渲染报告
        summary = pjdfstest_parse_results.parse_tap_log("", dir_timings)
        report_md = pjdfstest_parse_results.render_report_md(summary, {})

        # Then: 保留 broken_dir 且非零退出码不被后续 rc=0 覆盖，报告不误判为全量通过
        self.assertEqual(summary["total_directories"], 1)
        self.assertEqual(summary["directories"][0]["directory"], "broken_dir")
        self.assertEqual(summary["directories"][0]["rc"], 2)
        self.assertIn("目录退出码见明细", report_md)
        self.assertNotIn("100% PASS", report_md)

    def test_given_empty_log_and_timings_when_parsing_and_rendering_then_handles_zero_denominators(self) -> None:
        # Given: 完全空的 TAP 日志与空 dir_timings
        # When: 解析并渲染报告
        summary = pjdfstest_parse_results.parse_tap_log("", [])
        report_md = pjdfstest_parse_results.render_report_md(summary, {})

        # Then: 零分母归零，不输出「全部为 0」或「100% PASS」
        self.assertEqual(summary["pass_rate"], 0.0)
        self.assertEqual(summary["overall_throughput_tests_per_sec"], 0.0)
        self.assertNotIn("100% PASS", report_md)
        self.assertNotIn("全部为 0", report_md)

    def test_given_legacy_mode_when_rendering_report_then_produces_six_sections(self) -> None:
        # Given: 不含 telemetry_overview 的汇总数据
        summary = pjdfstest_parse_results.parse_tap_log(
            "[13:12:48] /opt/pjdfstest/tests/mkdir/00.t .. \n1..1\nok 1\nok 5 ms\n",
            [{"directory": "mkdir", "rc": 0, "duration_sec": 0.5}],
        )
        env_meta = {"cloud": {"region_id": "us-west-1"}, "bowl_config": {"bowl_git_commit": "abc"}}

        # When: 渲染报告
        report_md = pjdfstest_parse_results.render_report_md(summary, env_meta)

        # Then: 输出 6 节结构且无遥测诊断章节
        self.assertIn("100% PASS (全量通过)", report_md)
        self.assertIn("## 6. 阿里云按量资源生命周期与销毁审计", report_md)
        self.assertNotIn("生产可观测性与慢路径遥测诊断", report_md)

    def test_given_prod_telemetry_and_malformed_jsonl_when_running_cli_then_renders_seven_sections(self) -> None:
        # Given: 含空 env_metadata.json（或有效 deploy/prod 配置）、坏行 stage_timings.jsonl 与 telemetry/overview.json
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            telem_dir = res_dir / "telemetry"
            telem_dir.mkdir(parents=True)
            summary_out = Path(tmp) / "out" / "summary.json"
            report_out = Path(tmp) / "out" / "REPORT.md"

            (res_dir / "pjdfstest.log").write_text(
                "[13:12:48] /opt/pjdfstest/tests/open/00.t .. \n1..2\nok 1\nok 2\nok 10 ms\n",
                encoding="utf-8",
            )
            (res_dir / "stage_timings.jsonl").write_text(
                "{malformed\n" + json.dumps({"directory": "open", "rc": 0, "duration_sec": 0.4}) + "\n",
                encoding="utf-8",
            )
            (res_dir / "env_metadata.json").write_text(
                json.dumps({
                    "bowl_config": {"deploy_profile": "deploy/prod", "observability_stack": "Jaeger+Prom+Loki"},
                    "server_node": {"bowl_pg_version": "PostgreSQL 18.1"},
                }),
                encoding="utf-8",
            )
            (telem_dir / "overview.json").write_text(
                json.dumps({
                    "suite": "pjdfstest",
                    "phase_deltas": {
                        "open": {
                            "phase": "open",
                            "wall_clock_sec": 0.4,
                            "wall_clock_ms": 400.0,
                            "fuse_active_ms": 120.0,
                            "cache_and_pipeline": {"pebble_block_cache_hit_rate_pct": None},
                            "hops": [
                                {
                                    "hop_stage": "1. FUSE 入口",
                                    "component": "fuse.*",
                                    "calls": 10,
                                    "total_ms": 120.0,
                                    "mean_ms": 12.0,
                                    "p50_ms": 10.0,
                                    "p99_ms": 20.0,
                                    "share_pct": 100.0,
                                }
                            ],
                        }
                    },
                    "logs": {"loki_log_entries": 1, "server_alert_count": 0, "client_alert_count": 0},
                }),
                encoding="utf-8",
            )

            # When: 通过命令行接口执行 parse_results.py
            proc = subprocess.run(
                [
                    sys.executable,
                    str(BENCH_ROOT / "pjdfstest" / "parse_results.py"),
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

            # Then: 退出码 0，生成 7 节报告，含 N/A (0 reads) 且无 None%
            self.assertEqual(proc.returncode, 0, msg=f"stderr={proc.stderr}")
            self.assertTrue(summary_out.exists())
            report_md = report_out.read_text(encoding="utf-8")
            self.assertIn("PostgreSQL 18（deploy/prod 容器栈）", report_md)
            self.assertIn("## 6. 生产可观测性与慢路径遥测诊断", report_md)
            self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", report_md)
            self.assertIn("N/A (0 reads)", report_md)
            self.assertNotIn("None%", report_md)

    def test_given_empty_env_metadata_file_when_running_cli_then_does_not_crash(self) -> None:
        # Given: 0 字节 env_metadata.json 与空 pjdfstest.log
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            summary_out = Path(tmp) / "summary.json"
            report_out = Path(tmp) / "REPORT.md"
            (res_dir / "env_metadata.json").write_text("", encoding="utf-8")
            (res_dir / "pjdfstest.log").write_text("", encoding="utf-8")

            # When: 通过命令行接口执行
            proc = subprocess.run(
                [
                    sys.executable,
                    str(BENCH_ROOT / "pjdfstest" / "parse_results.py"),
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

            # Then: 退出码 0，正常生成空摘要
            self.assertEqual(proc.returncode, 0, msg=f"stderr={proc.stderr}")
            parsed_summary = json.loads(summary_out.read_text(encoding="utf-8"))
            self.assertEqual(parsed_summary["environment"], {})
            self.assertEqual(parsed_summary["total_tests"], 0)


if __name__ == "__main__":
    unittest.main()
