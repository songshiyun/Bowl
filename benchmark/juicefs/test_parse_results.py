#!/usr/bin/env python3
"""验证 `benchmark/juicefs/parse_results.py` 公开接口契约与极值表现。"""

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

from juicefs import parse_results as juicefs_parse_results  # noqa: E402


class JuicefsParseResultsContractTest(unittest.TestCase):
    """覆盖 `juicefs/parse_results.py` 日志解析、延迟口径与报告渲染契约。"""

    def test_given_ansi_bench_table_and_malformed_row_when_parsing_then_extracts_valid_items(self) -> None:
        # Given: 含 ANSI 转义序列、畸形数值行与有效指标行的 bench 日志
        with tempfile.TemporaryDirectory() as tmp:
            log_path = Path(tmp) / "bench_bowl.log"
            log_path.write_text(
                "\x1b[32m| Write big file | 450.25 MiB/s | 2.27 s/file |\x1b[0m\n"
                "| Read big file | 1.2.3 MiB/s | 1.00 s/file |\n"
                "| Stat file | 1200.50 files/s | 0.83 ms/file |\n",
                encoding="utf-8",
            )
            empty_path = Path(tmp) / "empty.log"
            empty_path.write_text("", encoding="utf-8")

            # When: 解析日志与空文件
            parsed = juicefs_parse_results.parse_juicefs_bench(log_path)
            empty_parsed = juicefs_parse_results.parse_juicefs_bench(empty_path)

            # Then: 有效行精确提取，畸形行被跳过，空文件返回空映射
            self.assertEqual(empty_parsed, {})
            self.assertEqual(parsed["Write big file"]["value"], 450.25)
            self.assertEqual(parsed["Stat file"]["cost"], 0.83)
            self.assertNotIn("Read big file", parsed)

    def test_given_legacy_direct_io_fio_json_when_parsing_then_masks_clat_percentiles(self) -> None:
        # Given: libaio+direct=1 下无 lat_ns.percentile 且 slat_ns > clat_ns 的旧版 fio JSON
        payload = {
            "jobs": [
                {
                    "error": 0,
                    "job_runtime": 4000,
                    "write": {
                        "bw_bytes": 456130560,
                        "iops": 435.0,
                        "runtime": 4000,
                        "io_bytes": 4294967296,
                        "slat_ns": {"mean": 2296000.0},
                        "clat_ns": {"mean": 3000.0, "percentile": {"95.000000": 4000, "99.000000": 10000}},
                        "lat_ns": {"mean": 2299000.0},
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp)
            json_path = res_dir / "fio_bowl_direct_write_4jobs.json"
            json_path.write_text(json.dumps(payload), encoding="utf-8")

            # When: 解析 fio JSON 并渲染报告
            parsed = juicefs_parse_results.parse_fio_json(json_path, "write")
            report_md = juicefs_parse_results.render_report(juicefs_parse_results.build_summary(res_dir))

            # Then: 均值保留 2.299ms，P95/P99 置为 None 并在报告输出 N/A
            self.assertEqual(parsed["lat_mean_ms"], 2.299)
            self.assertIsNone(parsed["lat_p95_ms"])
            self.assertIsNone(parsed["lat_p99_ms"])
            self.assertIn("| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `2.30 / N/A / N/A ms` |", report_md)

    def test_given_legacy_psync_fio_json_when_parsing_then_preserves_clat_percentiles(self) -> None:
        # Given: slat_ns 极小而 clat_ns 占主导的 psync fio JSON
        payload = {
            "jobs": [
                {
                    "error": 0,
                    "job_runtime": 4000,
                    "write": {
                        "bw_bytes": 429916160,
                        "iops": 102.5,
                        "runtime": 4000,
                        "io_bytes": 4294967296,
                        "slat_ns": {"mean": 12000.0},
                        "clat_ns": {"mean": 2431000.0, "percentile": {"95.000000": 2638000, "99.000000": 3162000}},
                        "lat_ns": {"mean": 2443000.0},
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "fio_bowl_seq_write_1job.json"
            p.write_text(json.dumps(payload), encoding="utf-8")

            # When: 解析
            parsed = juicefs_parse_results.parse_fio_json(p, "write")

            # Then: 保留 clat_ns 的 P95/P99
            self.assertEqual(parsed["lat_mean_ms"], 2.443)
            self.assertEqual(parsed["lat_p95_ms"], 2.638)
            self.assertEqual(parsed["lat_p99_ms"], 3.162)

    def test_given_fio_json_with_lat_percentiles_when_parsing_then_prefers_lat_ns_percentile(self) -> None:
        # Given: 开启 --lat_percentiles=1 的 fio JSON
        payload = {
            "jobs": [
                {
                    "error": 0,
                    "job_runtime": 4000,
                    "write": {
                        "bw_bytes": 104857600,
                        "iops": 100.0,
                        "runtime": 4000,
                        "io_bytes": 419430400,
                        "slat_ns": {"mean": 3400000.0},
                        "clat_ns": {"mean": 3000.0, "percentile": {"95.000000": 4500, "99.000000": 10000}},
                        "lat_ns": {"mean": 3403000.0, "percentile": {"95.000000": 4850000, "99.000000": 6210000}},
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "fio_direct_lat_pct.json"
            p.write_text(json.dumps(payload), encoding="utf-8")

            # When: 解析
            parsed = juicefs_parse_results.parse_fio_json(p, "write")

            # Then: 优先采用 lat_ns.percentile
            self.assertEqual(parsed["lat_mean_ms"], 3.403)
            self.assertEqual(parsed["lat_p95_ms"], 4.85)
            self.assertEqual(parsed["lat_p99_ms"], 6.21)

    def test_given_errored_malformed_or_missing_direction_fio_json_when_parsing_then_returns_empty(self) -> None:
        # Given: 失败任务 JSON、畸形 JSON、空文件以及缺少目标方向的 JSON
        with tempfile.TemporaryDirectory() as tmp:
            p_err = Path(tmp) / "err.json"
            p_err.write_text(json.dumps({"jobs": [{"error": 5, "write": {"bw_bytes": 999}}]}), encoding="utf-8")

            p_bad = Path(tmp) / "bad.json"
            p_bad.write_text("prefix {corrupt json} suffix", encoding="utf-8")

            p_empty = Path(tmp) / "empty.json"
            p_empty.write_text("", encoding="utf-8")

            p_wrong_dir = Path(tmp) / "read_only.json"
            p_wrong_dir.write_text(json.dumps({"jobs": [{"error": 0, "read": {"bw_bytes": 1048576}}]}), encoding="utf-8")

            # When / Then: 全部安全返回空字典，绝不伪造零吞吐记录
            self.assertEqual(juicefs_parse_results.parse_fio_json(p_err, "write"), {})
            self.assertEqual(juicefs_parse_results.parse_fio_json(p_bad, "write"), {})
            self.assertEqual(juicefs_parse_results.parse_fio_json(p_empty, "write"), {})
            self.assertEqual(juicefs_parse_results.parse_fio_json(p_wrong_dir, "write"), {})

    def test_given_mdtest_log_with_valid_and_malformed_rows_when_parsing_then_extracts_valid_ops(self) -> None:
        # Given: 含命令行、规模行、正常速率行与畸形数字行的 mdtest 日志
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "mdtest.log"
            p.write_text(
                "Command line used: mdtest -b 6 -I 8 -z 4\n"
                "1 tasks, 12440 files\n"
                "   Directory creation        :       100.0       90.0       95.0       2.1\n"
                "   File creation             :       1.2.3       90.0       95.0       2.1\n",
                encoding="utf-8",
            )

            # When: 解析日志
            parsed = juicefs_parse_results.parse_mdtest_log(p)

            # Then: 合法操作入表，畸形数字行跳过
            self.assertEqual(parsed["command"], "mdtest -b 6 -I 8 -z 4")
            self.assertEqual(parsed["scale"], "1 tasks, 12440 files")
            self.assertEqual(parsed["operations"]["Directory creation"]["mean"], 95.0)
            self.assertNotIn("File creation", parsed["operations"])

    def test_given_zero_denominator_when_formatting_ratio_then_returns_dash(self) -> None:
        # Given / When / Then: 零分母或缺失值均返回破折号
        self.assertEqual(juicefs_parse_results.fmt_ratio(100.0, 0.0), "—")
        self.assertEqual(juicefs_parse_results.fmt_ratio(100.0, None), "—")
        self.assertEqual(juicefs_parse_results.fmt_ratio(None, 100.0), "—")
        self.assertEqual(juicefs_parse_results.fmt_ratio(150.0, 50.0), "3.00x")

    def test_given_empty_metadata_and_malformed_jsonl_when_building_summary_then_succeeds(self) -> None:
        # Given: 0 字节 env_metadata.json 与含坏行的 stage_timings.jsonl
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True)
            (res_dir / "env_metadata.json").write_text("", encoding="utf-8")
            (res_dir / "stage_timings.jsonl").write_text(
                "{corrupt\n" + json.dumps({"target": "bowl", "stage": "jfs_bench", "duration_sec": 3.5}) + "\n",
                encoding="utf-8",
            )

            # When: 构建摘要与渲染报告
            summary = juicefs_parse_results.build_summary(res_dir)
            report_md = juicefs_parse_results.render_report(summary)

            # Then: 不崩溃且保留合法记录
            self.assertEqual(summary["env_metadata"], {})
            self.assertEqual(len(summary["stage_timings"]), 1)
            self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", report_md)

    def test_given_prod_telemetry_when_running_cli_then_renders_eight_sections_without_none_pct(self) -> None:
        # Given: 含 deploy/prod telemetry/overview.json 与真实基准文件的临时目录
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
            (res_dir / "bench_bowl.log").write_text("| Write big file | 480.00 MiB/s | 2.13 s/file |\n", encoding="utf-8")
            (res_dir / "bench_jfs_pg.log").write_text("| Write big file | 240.00 MiB/s | 4.26 s/file |\n", encoding="utf-8")
            (telem_dir / "overview.json").write_text(
                json.dumps({
                    "suite": "juicefs",
                    "phase_deltas": {
                        "fio_seq_write_1job": {
                            "phase": "fio_seq_write_1job",
                            "wall_clock_sec": 4.0,
                            "wall_clock_ms": 4000.0,
                            "fuse_active_ms": 250.0,
                            "cache_and_pipeline": {"pebble_block_cache_hit_rate_pct": None},
                            "hops": [
                                {
                                    "hop_stage": "3. 跨机网络+编解码差值",
                                    "component": "同跳 Client ConnectRPC - Server ConnectRPC",
                                    "calls": 96,
                                    "total_ms": 48.0,
                                    "mean_ms": 0.5,
                                    "p50_ms": 0.4,
                                    "p99_ms": 0.9,
                                    "share_pct": 19.2,
                                }
                            ],
                        }
                    },
                    "logs": {"loki_log_entries": 4, "server_alert_count": 0, "client_alert_count": 0},
                }),
                encoding="utf-8",
            )

            # When: 通过命令行接口执行脚本
            proc = subprocess.run(
                [
                    sys.executable,
                    str(BENCH_ROOT / "juicefs" / "parse_results.py"),
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

            # Then: 退出码 0，包含比值 2.00x、遥测 8 节结构、N/A (0 reads) 且无 None%
            self.assertEqual(proc.returncode, 0, msg=f"stderr={proc.stderr}")
            report_md = report_out.read_text(encoding="utf-8")
            self.assertIn("2.00x", report_md)
            self.assertIn("## 7. 生产可观测性与慢路径遥测诊断", report_md)
            self.assertIn("## 8. 阿里云按量资源生命周期与销毁审计", report_md)
            self.assertIn("N/A (0 reads)", report_md)
            self.assertNotIn("None%", report_md)


if __name__ == "__main__":
    unittest.main()
