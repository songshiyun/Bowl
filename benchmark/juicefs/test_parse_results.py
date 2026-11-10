#!/usr/bin/env python3
"""Unit tests for benchmark/juicefs/parse_results.py fio latency semantics."""

from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

import parse_results

SCRIPT_DIR = Path(__file__).resolve().parent
RESULTS_DIR = SCRIPT_DIR / "results"


class ParseFioLatencyTest(unittest.TestCase):
    def test_legacy_direct_io_uses_total_mean_and_masks_bogus_clat_percentiles(self) -> None:
        json_path = RESULTS_DIR / "fio_bowl_direct_write_4jobs.json"
        parsed = parse_results.parse_fio_json(json_path, "write")
        self.assertEqual(parsed["lat_mean_ms"], 3.43)
        self.assertIsNone(parsed["lat_p95_ms"])
        self.assertIsNone(parsed["lat_p99_ms"])

        summary = parse_results.build_summary(RESULTS_DIR)
        report_md = parse_results.render_report(summary)
        self.assertIn("| **Direct 顺序写 (4 并发 · 1M · 4GiB)** | `3.43 / N/A / N/A ms` |", report_md)
        self.assertNotIn("0.00 / 0.00 / 0.01 ms", report_md)

    def test_legacy_psync_seq_io_preserves_clat_percentiles_when_slat_negligible(self) -> None:
        json_path = RESULTS_DIR / "fio_bowl_seq_write_1job.json"
        parsed = parse_results.parse_fio_json(json_path, "write")
        self.assertEqual(parsed["lat_mean_ms"], 7.557)
        self.assertEqual(parsed["lat_p95_ms"], 8.356)
        self.assertEqual(parsed["lat_p99_ms"], 9.11)

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

    def test_falls_back_to_slat_plus_clat_mean_when_lat_ns_missing(self) -> None:
        payload = {
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
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "fio_fallback.json"
            p.write_text(json.dumps(payload), encoding="utf-8")
            parsed = parse_results.parse_fio_json(p, "read")
            self.assertEqual(parsed["lat_mean_ms"], 3.0)
            self.assertIsNone(parsed["lat_p95_ms"])
            self.assertIsNone(parsed["lat_p99_ms"])


if __name__ == "__main__":
    unittest.main()
