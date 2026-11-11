#!/usr/bin/env python3
"""Unit tests for benchmark/juicefs/parse_results.py fio latency & telemetry report semantics."""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPT_DIR = Path(__file__).resolve().parent
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import parse_results  # noqa: E402


class ParseFioLatencyTest(unittest.TestCase):
    def test_legacy_direct_io_uses_total_mean_and_masks_bogus_clat_percentiles(self) -> None:
        # Synthetic legacy fio JSON (without --lat_percentiles=1) where libaio+direct=1 puts real I/O latency in slat_ns
        legacy_direct_payload = {
            "jobs": [
                {
                    "error": 0,
                    "job_runtime": 4000,
                    "write": {
                        "bw_bytes": 456_130_560,
                        "iops": 435.0,
                        "runtime": 4000,
                        "io_bytes": 4_294_967_296,
                        "slat_ns": {"mean": 2_296_000.0},
                        "clat_ns": {
                            "mean": 3_000.0,
                            "percentile": {"95.000000": 4_000, "99.000000": 10_000},
                        },
                        "lat_ns": {"mean": 2_299_000.0},
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp)
            json_path = res_dir / "fio_bowl_direct_write_4jobs.json"
            json_path.write_text(json.dumps(legacy_direct_payload), encoding="utf-8")

            parsed = parse_results.parse_fio_json(json_path, "write")
            self.assertEqual(parsed["lat_mean_ms"], 2.299)
            self.assertIsNone(parsed["lat_p95_ms"])
            self.assertIsNone(parsed["lat_p99_ms"])

            summary = parse_results.build_summary(res_dir)
            report_md = parse_results.render_report(summary)
            self.assertIn("| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `2.30 / N/A / N/A ms` |", report_md)
            self.assertNotIn("0.00 / 0.00 / 0.01 ms", report_md)

    def test_legacy_psync_seq_io_preserves_clat_percentiles_when_slat_negligible(self) -> None:
        legacy_psync_payload = {
            "jobs": [
                {
                    "error": 0,
                    "job_runtime": 4000,
                    "write": {
                        "bw_bytes": 429_916_160,
                        "iops": 102.5,
                        "runtime": 4000,
                        "io_bytes": 4_294_967_296,
                        "slat_ns": {"mean": 12_000.0},
                        "clat_ns": {
                            "mean": 2_431_000.0,
                            "percentile": {"95.000000": 2_638_000, "99.000000": 3_162_000},
                        },
                        "lat_ns": {"mean": 2_443_000.0},
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "fio_bowl_seq_write_1job.json"
            p.write_text(json.dumps(legacy_psync_payload), encoding="utf-8")
            parsed = parse_results.parse_fio_json(p, "write")
            self.assertEqual(parsed["lat_mean_ms"], 2.443)
            self.assertEqual(parsed["lat_p95_ms"], 2.638)
            self.assertEqual(parsed["lat_p99_ms"], 3.162)

    def test_lat_percentiles_enabled_prefers_lat_ns_percentile(self) -> None:
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
                        "slat_ns": {"mean": 3_400_000.0},
                        "clat_ns": {
                            "mean": 3_000.0,
                            "percentile": {"95.000000": 4_500, "99.000000": 10_000},
                        },
                        "lat_ns": {
                            "mean": 3_403_000.0,
                            "percentile": {"95.000000": 4_850_000, "99.000000": 6_210_000},
                        },
                    },
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "fio_direct_lat_pct.json"
            p.write_text(json.dumps(payload), encoding="utf-8")
            parsed = parse_results.parse_fio_json(p, "write")
            self.assertEqual(parsed["lat_mean_ms"], 3.403)
            self.assertEqual(parsed["lat_p95_ms"], 4.85)
            self.assertEqual(parsed["lat_p99_ms"], 6.21)

    def test_falls_back_to_slat_plus_clat_mean_and_rejects_errored_fio_job(self) -> None:
        fallback_payload = {
            "jobs": [
                {
                    "error": 0,
                    "job_runtime": 1000,
                    "read": {
                        "bw_bytes": 1048576,
                        "iops": 1.0,
                        "runtime": 1000,
                        "io_bytes": 1048576,
                        "slat_ns": {"mean": 2_500_000.0},
                        "clat_ns": {"mean": 500_000.0},
                    },
                }
            ]
        }
        errored_payload = {
            "jobs": [
                {
                    "error": 5,
                    "job_runtime": 100,
                    "write": {"bw_bytes": 999999999, "iops": 9999.0},
                }
            ]
        }
        with tempfile.TemporaryDirectory() as tmp:
            p_ok = Path(tmp) / "fio_fallback.json"
            p_ok.write_text(json.dumps(fallback_payload), encoding="utf-8")
            parsed = parse_results.parse_fio_json(p_ok, "read")
            self.assertEqual(parsed["lat_mean_ms"], 3.0)
            self.assertIsNone(parsed["lat_p95_ms"])
            self.assertIsNone(parsed["lat_p99_ms"])

            p_err = Path(tmp) / "fio_err.json"
            p_err.write_text(json.dumps(errored_payload), encoding="utf-8")
            self.assertEqual(parse_results.parse_fio_json(p_err, "write"), {})


if __name__ == "__main__":
    unittest.main()
