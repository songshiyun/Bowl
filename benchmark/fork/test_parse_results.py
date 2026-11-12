#!/usr/bin/env python3
"""Bowl Disk Fork 基准结果解析器单元测试。"""

from __future__ import annotations

import json
from pathlib import Path
import sys
import tempfile
import unittest

FORK_DIR = Path(__file__).resolve().parent
if str(FORK_DIR) not in sys.path:
    sys.path.insert(0, str(FORK_DIR))

import parse_results  # noqa: E402


class ParseForkResultsTest(unittest.TestCase):
    def test_trimmed_statistics_drops_five_percent_tails_on_n20(self) -> None:
        # 20 个样本：0.01 与 9999.0 为两端各 5% 极端离群值，截尾后应剩余 18 个样本 (2..19)
        samples = [9999.0, 0.01] + [float(x) for x in range(2, 20)]
        st = parse_results.summarize_samples(samples, trim_fraction=0.05)
        self.assertEqual(st["count"], 20)
        self.assertEqual(st["trimmed_count"], 18)
        self.assertAlmostEqual(st["median"], 10.5, places=3)
        self.assertAlmostEqual(st["min"], 0.01, places=3)
        self.assertAlmostEqual(st["max"], 9999.0, places=3)
        self.assertLess(st["p99"], 20.0)

    def test_compute_composite_score_guards_and_formula(self) -> None:
        self.assertEqual(parse_results.compute_composite_score(5.0, 2.0, verified=False), 0.0)
        self.assertEqual(parse_results.compute_composite_score(60000.0, 60000.0, verified=True), 0.0)
        self.assertEqual(parse_results.compute_composite_score(0.0, 0.0, verified=True), 100.0)
        # fork=6.0ms (99.99), first_read=6.0ms (99.99) -> 99.99
        self.assertEqual(parse_results.compute_composite_score(6.0, 6.0, verified=True), 99.99)

    def test_build_summary_and_render_report_legacy_and_prod_telemetry(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            res_dir = Path(tmp) / "results"
            res_dir.mkdir(parents=True, exist_ok=True)

            env_meta = {
                "cloud": {
                    "provider": "Aliyun ECS",
                    "region": "us-west-1",
                    "zone_id": "us-west-1a",
                    "vpc_id": "vpc-<redacted>",
                    "vswitch_id": "vsw-<redacted>",
                    "security_group_id": "sg-<redacted>",
                    "instance_type": "ecs.i5.xlarge",
                    "data_disk_category": "local_ssd_pro (NVMe)",
                },
                "server_node": {
                    "instance_id": "i-<redacted-server>",
                    "private_ip": "172.27.96.10",
                    "vcpu": 4,
                    "memory_gib": "30Gi",
                    "cpu_model": "Intel Xeon",
                    "os_release": "Ubuntu 24.04 LTS",
                    "kernel": "6.8.0",
                    "data_disk_size_gib": 894,
                    "data_disk_device": "/dev/nvme1n1",
                },
                "client_node": {
                    "instance_id": "i-<redacted-client>",
                    "private_ip": "172.27.96.11",
                    "vcpu": 4,
                    "memory_gib": "30Gi",
                    "cpu_model": "Intel Xeon",
                    "os_release": "Ubuntu 24.04 LTS",
                    "kernel": "6.8.0",
                    "data_disk_size_gib": 894,
                    "data_disk_device": "/dev/nvme1n1",
                    "fusermount_version": "fusermount3 version: 3.14.0",
                    "python_version": "Python 3.12.3",
                },
                "network_baseline": {"ping_rtt_ms": "rtt min/avg/max = 0.12/0.15/0.22 ms", "iperf3_gbps": 9.85},
                "server_disk_baseline": {
                    "local_seq_write_mib_s": 1350.0,
                    "local_seq_read_mib_s": 2800.0,
                    "local_randwrite_4k_iops": 165000,
                    "local_randread_4k_iops": 240000,
                    "local_fsync_4k_iops": 19500,
                    "local_fsync_4k_p99_us": 62.5,
                },
                "bowl_config": {"git_commit": "8a07a55"},
                "cleanup_audit": {
                    "key_pair_name": "bowl-forkbench-kp-20261112-150000",
                    "server_instance_ids": "i-<redacted-server>",
                    "client_instance_ids": "i-<redacted-client>",
                    "instances_created_at": "2026-11-12T15:00:00Z",
                    "instances_deleted_at": "2026-11-12T15:10:00Z",
                    "remaining_benchmark_instances": 0,
                    "remaining_keypairs": 0,
                },
            }
            (res_dir / "env_metadata.json").write_text(json.dumps(env_meta), encoding="utf-8")

            def _mk_stat(med: float) -> dict:
                return {"count": 20, "trimmed_count": 18, "median": med, "p95": med * 1.2, "p99": med * 1.4, "mean": med}

            suite1 = {
                "preset_order": ["small", "large_1gib", "tree_5k"],
                "presets": {
                    "small": {
                        "preset": "small",
                        "label": "ComputeSDK small (10 × 1 MiB)",
                        "dataset_mib": 10.0,
                        "seed_ms": 42.0,
                        "bowl_fork": {
                            "iterations": 20,
                            "verified_count": 20,
                            "all_verified": True,
                            "physical_delta_bytes_on_fork": 32768,
                            "fork_rpc_ms": _mk_stat(4.5),
                            "fork_cli_ms": _mk_stat(18.0),
                            "mount_ready_ms": _mk_stat(35.0),
                            "fork_plus_mount_ms": _mk_stat(39.5),
                            "cold_first_read_ms": _mk_stat(3.2),
                            "end_to_end_tti_ms": _mk_stat(42.7),
                            "delete_ms": _mk_stat(22.0),
                            "computesdk_score_rpc": 99.99,
                            "computesdk_score_with_mount": 99.93,
                        },
                        "bowl_copy": {
                            "iterations": 5,
                            "verified_count": 5,
                            "all_verified": True,
                            "physical_delta_bytes_on_copy": 10485760,
                            "copy_sync_ms": _mk_stat(68.0),
                            "provision_plus_copy_ms": _mk_stat(105.0),
                            "cold_first_read_ms": _mk_stat(3.3),
                            "end_to_end_tti_ms": _mk_stat(108.3),
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
                        "preset": "large_1gib",
                        "label": "多封包 NVMe 规模 (16 × 64 MiB = 1 GiB)",
                        "dataset_mib": 1024.0,
                        "seed_ms": 1450.0,
                        "bowl_fork": {
                            "iterations": 20,
                            "verified_count": 20,
                            "all_verified": True,
                            "physical_delta_bytes_on_fork": 40960,
                            "fork_rpc_ms": _mk_stat(5.2),
                            "fork_cli_ms": _mk_stat(19.0),
                            "mount_ready_ms": _mk_stat(36.0),
                            "fork_plus_mount_ms": _mk_stat(41.2),
                            "cold_first_read_ms": _mk_stat(92.0),
                            "end_to_end_tti_ms": _mk_stat(133.2),
                            "delete_ms": _mk_stat(24.0),
                            "computesdk_score_rpc": 99.98,
                            "computesdk_score_with_mount": 99.92,
                        },
                        "bowl_copy": {
                            "iterations": 3,
                            "verified_count": 3,
                            "all_verified": True,
                            "physical_delta_bytes_on_copy": 1073741824,
                            "copy_sync_ms": _mk_stat(1820.0),
                            "provision_plus_copy_ms": _mk_stat(1860.0),
                            "cold_first_read_ms": _mk_stat(94.0),
                            "end_to_end_tti_ms": _mk_stat(1954.0),
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
                        "preset": "tree_5k",
                        "label": "POSIX 多目录树规模 (50 子目录 × 100 × 4 KiB)",
                        "dataset_mib": 19.531,
                        "seed_ms": 2100.0,
                        "bowl_fork": {
                            "iterations": 20,
                            "verified_count": 20,
                            "all_verified": True,
                            "physical_delta_bytes_on_fork": 65536,
                            "fork_rpc_ms": _mk_stat(6.1),
                            "fork_cli_ms": _mk_stat(20.0),
                            "mount_ready_ms": _mk_stat(36.0),
                            "fork_plus_mount_ms": _mk_stat(42.1),
                            "cold_first_read_ms": _mk_stat(1.4),
                            "end_to_end_tti_ms": _mk_stat(43.5),
                            "delete_ms": _mk_stat(25.0),
                            "computesdk_score_rpc": 99.99,
                            "computesdk_score_with_mount": 99.94,
                        },
                        "bowl_copy": {
                            "iterations": 3,
                            "verified_count": 3,
                            "all_verified": True,
                            "physical_delta_bytes_on_copy": 20480000,
                            "copy_sync_ms": _mk_stat(2450.0),
                            "provision_plus_copy_ms": _mk_stat(2490.0),
                            "cold_first_read_ms": _mk_stat(1.5),
                            "end_to_end_tti_ms": _mk_stat(2491.5),
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
                    "stream_4mib_before_fork_ms": _mk_stat(24.0),
                    "stream_4mib_during_fork_ms": _mk_stat(26.5),
                    "small_4kib_ops": 1400,
                    "small_4kib_ops_per_sec": 290.0,
                    "small_4kib_before_fork_ms": _mk_stat(3.1),
                    "small_4kib_during_fork_ms": _mk_stat(3.5),
                },
                "online_fork_rpc_ms": _mk_stat(6.8),
                "online_forks": [
                    {
                        "fork_index": 0,
                        "fork_rpc_ms": 6.5,
                        "child_used_bytes": 100663296,
                        "child_live_visible_bytes": 100663296,
                        "orphan_bytes_in_child": 0,
                        "sanitized_orphan_bytes": 16777216,
                        "committed_files_verified": 32,
                        "small_files_verified": 420,
                        "corrupted_files": 0,
                        "verified": True,
                    }
                ],
                "parent_reclaimed_orphan_bytes_on_close": 16777216,
                "all_children_verified": True,
                "all_orphans_sanitized": True,
            }
            suite3 = {
                "parent_seed_ms": 1420.0,
                "fanout_fork_total_ms": 88.5,
                "fanout_fork_rpc_ms": _mk_stat(5.4),
                "checkpoints": {
                    "after_parent_seed": {
                        "logical_bytes": 1073741824,
                        "du_sb_bytes": 1073807360,
                        "commits_dir_count": 0,
                        "parent_pack_count": 16,
                        "parent_pack_max_nlink": 1,
                    },
                    "after_16_forks": {
                        "logical_bytes": 18253611008,
                        "du_sb_bytes": 1074200000,
                        "commits_dir_count": 16,
                        "shared_pack_max_nlink": 33,
                        "logical_to_physical_ratio": 16.99,
                    },
                    "after_concurrent_cow_writes": {
                        "logical_bytes": 18320719872,
                        "du_sb_bytes": 1141500000,
                        "commits_dir_count": 16,
                        "shared_pack_max_nlink": 33,
                        "delta_pack_nlink": 1,
                        "space_amplification_vs_unique": 1.0006,
                        "physical_savings_pct": 93.77,
                    },
                    "after_parent_delete": {
                        "logical_bytes": 17246978048,
                        "du_sb_bytes": 1141450000,
                        "commits_dir_count": 16,
                        "shared_pack_max_nlink": 32,
                        "children_verified": 16,
                    },
                    "after_15_children_delete_sole_survivor": {
                        "logical_bytes": 1077936128,
                        "du_sb_bytes": 1078000000,
                        "commits_dir_count": 0,
                        "survivor_pack_count": 17,
                        "survivor_pack_min_nlink": 1,
                        "survivor_pack_max_nlink": 1,
                    },
                },
            }

            (res_dir / "suite1_matrix.json").write_text(json.dumps(suite1), encoding="utf-8")
            (res_dir / "suite2_online_fork.json").write_text(json.dumps(suite2), encoding="utf-8")
            (res_dir / "suite3_fanout_cow.json").write_text(json.dumps(suite3), encoding="utf-8")
            (res_dir / "stage_timings.jsonl").write_text(
                json.dumps({"stage": "suite1_fork_matrix", "duration_sec": 35.2}) + "\n", encoding="utf-8"
            )

            # 1. 无 telemetry 目录时输出 7 节结构
            legacy_sum = parse_results.build_summary(res_dir)
            self.assertNotIn("telemetry_overview", legacy_sum)
            legacy_md = parse_results.render_report(legacy_sum)
            self.assertIn("Tensorlake", legacy_md)
            self.assertIn("267.37", legacy_md)
            self.assertIn("Tigris", legacy_md)
            self.assertIn("Azure Blob", legacy_md)
            self.assertIn("i-<redacted-server>", legacy_md)
            self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", legacy_md)
            self.assertNotIn("生产可观测性与慢路径遥测诊断", legacy_md)

            # 2. 含 deploy/prod telemetry 时输出 8 节结构且幂等一致
            env_meta["bowl_config"]["deploy_profile"] = "deploy/prod (docker-compose.yaml + storage.env)"
            env_meta["bowl_config"]["observability_stack"] = "Jaeger + Prometheus + Loki + Alloy + Grafana"
            env_meta["server_node"]["bowl_pg_version"] = "postgres (PostgreSQL) 18.1 (alpine)"
            (res_dir / "env_metadata.json").write_text(json.dumps(env_meta), encoding="utf-8")
            (res_dir / "telemetry").mkdir(parents=True, exist_ok=True)
            (res_dir / "telemetry" / "overview.json").write_text(
                json.dumps(
                    {
                        "suite": "fork",
                        "client_stats": {"total_calls": 500, "total_errors": 0, "fuse_active_ms": 120.0},
                        "prometheus": {"targets": [{"job": "bowl-storage", "health": "up"}]},
                        "jaeger": {"trace_count": 20, "slow_traces_top15": []},
                        "logs": {"loki_log_entries": 10, "server_alert_count": 0, "client_alert_count": 0},
                    }
                ),
                encoding="utf-8",
            )
            prod_sum = parse_results.build_summary(res_dir)
            self.assertIn("telemetry_overview", prod_sum)
            prod_md_1 = parse_results.render_report(prod_sum)
            prod_md_2 = parse_results.render_report(parse_results.build_summary(res_dir))
            self.assertEqual(prod_md_1, prod_md_2)
            self.assertIn("PostgreSQL 18（deploy/prod 容器栈）", prod_md_1)
            self.assertIn("## 7. 生产可观测性与慢路径遥测诊断", prod_md_1)
            self.assertIn("## 8. 阿里云按量资源生命周期与销毁审计", prod_md_1)


if __name__ == "__main__":
    unittest.main()
