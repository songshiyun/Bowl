#!/usr/bin/env python3
"""Unit tests for benchmark/common/collect_telemetry.py."""

from __future__ import annotations

import importlib.util
import json
import os
import shutil
import stat
import sys
import tempfile
import unittest
from pathlib import Path
from unittest import mock

THIS_DIR = Path(__file__).resolve().parent
if str(THIS_DIR) not in sys.path:
    sys.path.insert(0, str(THIS_DIR))

import collect_telemetry  # noqa: E402

BENCH_ROOT = THIS_DIR.parent


def load_suite_parser(suite: str):
    spec = importlib.util.spec_from_file_location(
        f"parse_results_{suite}", BENCH_ROOT / suite / "parse_results.py"
    )
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class CollectTelemetryTest(unittest.TestCase):
    def test_26_bucket_fuse_quantiles_and_heavy_tail_support(self) -> None:
        # Bucket index 22 covers (2^21 .. 2^22] = (2,097,152 .. 4,194,304] us (~2.1s..4.19s)
        # Bucket index 25 covers (2^24 .. 2^25] = (16,777,216 .. 33,554,432] us (~16.78s..33.55s)
        self.assertEqual(len(collect_telemetry.CLIENT_HIST_UPPER_US), 26)
        self.assertEqual(collect_telemetry.CLIENT_HIST_UPPER_US[0], 1.0)
        self.assertEqual(collect_telemetry.CLIENT_HIST_UPPER_US[25], float(1 << 25))

        buckets = [0] * 26
        buckets[10] = 90  # <= 1024 us
        buckets[22] = 9   # (2,097,152 .. 4,194,304] us
        buckets[25] = 1   # (16,777,216 .. 33,554,432] us
        p50 = collect_telemetry.noncum_quantile_us(buckets, 100, 0.50)
        p95 = collect_telemetry.noncum_quantile_us(buckets, 100, 0.95)
        p99_9 = collect_telemetry.noncum_quantile_us(buckets, 100, 1.00)
        self.assertGreater(p50, 512.0)
        self.assertLessEqual(p50, 1024.0)
        self.assertGreater(p95, 2_097_152.0)
        self.assertLessEqual(p95, 4_194_304.0)
        self.assertEqual(p99_9, 33_554_432.0)

    def test_jaeger_same_hop_rpc_delta_never_subtracts_from_fuse_root_span(self) -> None:
        # Regression test: fuse.Create takes 9.0 ms (6.6 ms local client lock/cache work + 2.4 ms Client RPC),
        # while Server ConnectRPC takes 1.9 ms. Network+codec hop delta MUST be 2.4 - 1.9 = 0.5 ms, NOT 9.0 - 1.9 = 7.1 ms!
        trace_obj = {
            "traceID": "deadbeef12345678deadbeef12345678",
            "processes": {
                "p1": {"serviceName": "bowl-client"},
                "p2": {"serviceName": "bowl-storage"},
            },
            "spans": [
                {
                    "traceID": "deadbeef12345678deadbeef12345678",
                    "spanID": "root_fuse",
                    "operationName": "fuse.Create",
                    "startTime": 1_700_000_000_000_000,
                    "duration": 9000,  # 9.0 ms
                    "processID": "p1",
                    "references": [],
                    "tags": [
                        {"key": "fuse.op", "value": "Create"},
                        {"key": "fuse.errno", "value": 0},
                    ],
                },
                {
                    "traceID": "deadbeef12345678deadbeef12345678",
                    "spanID": "cli_rpc",
                    "operationName": "bowl.storage.v1.StorageService/Create",
                    "startTime": 1_700_000_000_006_600,
                    "duration": 2400,  # 2.4 ms
                    "processID": "p1",
                    "references": [{"refType": "CHILD_OF", "spanID": "root_fuse"}],
                    "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Create"}],
                },
                {
                    "traceID": "deadbeef12345678deadbeef12345678",
                    "spanID": "srv_rpc",
                    "operationName": "bowl.storage.v1.StorageService/Create",
                    "startTime": 1_700_000_000_006_850,
                    "duration": 1900,  # 1.9 ms
                    "processID": "p2",
                    "references": [
                        {"refType": "CHILD_OF", "spanID": "cli_rpc"},
                        {"refType": "FOLLOWS_FROM", "spanID": "batch_link"},
                    ],
                    "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Create"}],
                },
            ],
        }
        distilled = collect_telemetry.distill_jaeger_trace(trace_obj)
        assert distilled is not None
        self.assertAlmostEqual(distilled["total_duration_ms"], 9.0, places=3)
        self.assertAlmostEqual(distilled["client_max_span_ms"], 9.0, places=3)
        self.assertAlmostEqual(distilled["client_rpc_span_ms"], 2.4, places=3)
        self.assertAlmostEqual(distilled["server_rpc_span_ms"], 1.9, places=3)
        self.assertAlmostEqual(distilled["rpc_network_codec_ms"], 0.5, places=3)
        self.assertAlmostEqual(distilled["client_local_overhead_ms"], 6.6, places=3)
        self.assertEqual(distilled["span_links_count"], 1)

    def test_end_to_end_telemetry_bundle_distillation_with_phase_deltas_and_new_contracts(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            stage_dir = root / "stage"
            results_dir = root / "results"
            results_dir.mkdir(parents=True)

            # 1. Stage timings (including non-Bowl & setup stages that MUST be filtered out)
            (results_dir / "stage_timings.jsonl").write_text(
                "\n".join([
                    json.dumps({"stage": "cross_compile_binaries", "duration_sec": 95.0}),
                    json.dumps({"target": "jfs_pg", "suite": "fio", "duration_sec": 120.0}),
                    json.dumps({"stage": "drive9_default_perf_baseline", "duration_sec": 80.0}),
                    json.dumps({"stage": "drive9_fuse_bench_cold_and_hot", "duration_sec": 300.0}),
                    json.dumps({"target": "bowl", "stage": "fio_seq_write_1job", "duration_sec": 4.0}),
                    json.dumps({"target": "bowl", "stage": "mdtest_1p", "duration_sec": 18.2}),
                ]) + "\n",
                encoding="utf-8",
            )

            # 2. Client `bowl stats` 26-bucket + 9-subsystem pre/post & before_umount snapshots + .epoch
            cli_stats = stage_dir / "client" / "stats"
            cli_stats.mkdir(parents=True)
            empty_buckets = [0] * 26
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
                "namespace_cache": {
                    "attr_hits": 10,
                    "attr_misses": 2,
                    "entry_hits": 5,
                    "entry_misses": 5,
                    "readlink_hits": 2,
                    "readlink_misses": 1,
                },
                "read_ahead": {
                    "triggers": 0,
                    "prefetched_bytes": 0,
                    "hit_bytes": 0,
                    "discarded_bytes": 0,
                    "waits": 0,
                    "wait_seconds": 0.0,
                },
                "write_behind": {
                    "staged_calls": 0,
                    "staged_bytes": 0,
                    "passthrough_calls": 10,
                    "passthrough_bytes": 40960,
                    "flush_rpcs": 0,
                    "flush_errors": 0,
                    "inflight": 0,
                    "pending_slots": 0,
                    "backpressure_waits": 0,
                    "backpressure_wait_seconds": 0.0,
                    "barrier_waits": 0,
                    "barrier_wait_seconds": 0.0,
                },
                "inode_locks": {"fast_acquisitions": 10, "contended_acquisitions": 0, "wait_seconds": 0.0},
                "queues": {"fuse_waiting": 0, "fuse_congestion_threshold": 48},
            }

            statx_buckets = list(empty_buckets)
            statx_buckets[6] = 5
            lookup_buckets = list(empty_buckets)
            lookup_buckets[8] = 10
            snap_post = {
                "timestamp_unix_nano": 5_000_000_000,
                "uptime_seconds": 5.0,
                "cpu_usage_seconds": 1.7,
                "memory_bytes": 128 * 1024 * 1024,
                "fuse": {
                    "total_calls": 115,
                    "total_errors": 2,
                    "read_bytes": 1048576,
                    "write_bytes": 419430400,
                    "ops": [
                        {
                            "op": "Write",
                            "calls": 100,
                            "errors": 0,
                            "bytes": 419430400,
                            "duration_nanos": 308_000_000,  # delta = 300 ms across 90 calls
                            "buckets": post_buckets,
                        },
                        {
                            "op": "Lookup",
                            "calls": 10,
                            "errors": 2,
                            "bytes": 0,
                            "duration_nanos": 5_000_000,
                            "buckets": lookup_buckets,
                            "errnos": {"ENOENT": 2},
                        },
                        {
                            "op": "Statx",
                            "calls": 5,
                            "errors": 0,
                            "bytes": 0,
                            "duration_nanos": 250_000,
                            "buckets": statx_buckets,
                        },
                    ],
                },
                "namespace_cache": {
                    "attr_hits": 90,
                    "attr_misses": 12,
                    "entry_hits": 45,
                    "entry_misses": 15,
                    "readlink_hits": 10,
                    "readlink_misses": 3,
                },
                "read_ahead": {
                    "triggers": 4,
                    "prefetched_bytes": 4194304,
                    "hit_bytes": 3145728,
                    "discarded_bytes": 0,
                    "waits": 2,
                    "wait_seconds": 0.004,
                },
                "write_behind": {
                    "staged_calls": 85,
                    "staged_bytes": 402653184,
                    "passthrough_calls": 15,
                    "passthrough_bytes": 16777216,
                    "flush_rpcs": 96,
                    "flush_errors": 0,
                    "inflight": 2,
                    "pending_slots": 1,
                    "backpressure_waits": 6,
                    "backpressure_wait_seconds": 0.018,
                    "barrier_waits": 1,
                    "barrier_wait_seconds": 0.005,
                },
                "inode_locks": {"fast_acquisitions": 100, "contended_acquisitions": 5, "wait_seconds": 0.012},
                "queues": {"fuse_waiting": 3, "fuse_congestion_threshold": 48},
            }
            (cli_stats / "fio_seq_write_1job_pre.json").write_text(json.dumps(snap_pre), encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_pre.epoch").write_text("1700000001.000\n", encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_post.json").write_text(json.dumps(snap_post), encoding="utf-8")
            (cli_stats / "fio_seq_write_1job_post.epoch").write_text("1700000005.250\n", encoding="utf-8")
            (cli_stats / "before_umount.json").write_text(json.dumps(snap_post), encoding="utf-8")

            # 3. Client & Server Prometheus pre/post & before_umount snapshots (testing exact same-hop RPC delta & 0/0 BlockCache)
            cli_m = stage_dir / "client" / "metrics"
            cli_m.mkdir(parents=True)
            cli_prom_pre = "\n".join([
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 4',
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 4',
                'rpc_client_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.010',
                'rpc_client_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 4',
            ]) + "\n"
            cli_prom_post = "\n".join([
                # 100 Write RPCs during phase (total 104 - 4 = 100), sum = 0.260 - 0.010 = 0.250s (mean 2.50 ms)
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.0025"} 64',
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 104',
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 104',
                'rpc_client_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.260',
                'rpc_client_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 104',
                # 2 Lookup ENOENT non-OK RPCs
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found",le="0.001"} 2',
                'rpc_client_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found",le="+Inf"} 2',
                'rpc_client_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found"} 0.001',
                'rpc_client_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found"} 2',
                # Client WB & Lock wait histograms
                'bowl_client_write_behind_backpressure_wait_duration_seconds_bucket{le="0.005"} 6',
                'bowl_client_write_behind_backpressure_wait_duration_seconds_bucket{le="+Inf"} 6',
                'bowl_client_write_behind_backpressure_wait_duration_seconds_sum 0.018',
                'bowl_client_write_behind_backpressure_wait_duration_seconds_count 6',
                'bowl_client_inodelock_wait_duration_seconds_bucket{le="0.005"} 5',
                'bowl_client_inodelock_wait_duration_seconds_bucket{le="+Inf"} 5',
                'bowl_client_inodelock_wait_duration_seconds_sum 0.012',
                'bowl_client_inodelock_wait_duration_seconds_count 5',
                'bowl_client_fuse_connection_waiting 3',
                'bowl_client_fuse_connection_congestion_threshold 48',
            ]) + "\n"
            (cli_m / "fio_seq_write_1job_pre.prom").write_text(cli_prom_pre, encoding="utf-8")
            (cli_m / "fio_seq_write_1job_post.prom").write_text(cli_prom_post, encoding="utf-8")
            (cli_m / "before_umount.prom").write_text(cli_prom_post, encoding="utf-8")

            srv_m = stage_dir / "server" / "metrics"
            srv_m.mkdir(parents=True)
            srv_prom_pre = "\n".join([
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 4',
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 4',
                'rpc_server_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.008',
                'rpc_server_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 4',
                'bowl_storage_pebble_block_cache_hits_total 0',
                'bowl_storage_pebble_block_cache_misses_total 0',
            ]) + "\n"
            srv_prom_post = "\n".join([
                # Outer HTTP: 100 Write calls, sum = 0.220s (mean 2.20 ms)
                'bowl_storage_http_request_duration_seconds_bucket{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx",le="0.0025"} 70',
                'bowl_storage_http_request_duration_seconds_bucket{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx",le="0.005"} 100',
                'bowl_storage_http_request_duration_seconds_bucket{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx",le="+Inf"} 100',
                'bowl_storage_http_request_duration_seconds_sum{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx"} 0.220',
                'bowl_storage_http_request_duration_seconds_count{procedure="/bowl.storage.v1.StorageService/Write",status_class="2xx"} 100',
                'bowl_storage_http_request_bytes_total{procedure="/bowl.storage.v1.StorageService/Write"} 419430400',
                'bowl_storage_http_response_bytes_total{procedure="/bowl.storage.v1.StorageService/Write"} 6400',
                # Server ConnectRPC: delta = 100 Write calls, sum = 0.188 - 0.008 = 0.180s (mean 1.80 ms -> net+codec delta = 2.50 - 1.80 = 0.70 ms!)
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.0025"} 84',
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="0.005"} 104',
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok",le="+Inf"} 104',
                'rpc_server_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 0.188',
                'rpc_server_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Write",rpc_response_status_code="ok"} 104',
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found",le="0.001"} 2',
                'rpc_server_call_duration_seconds_bucket{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found",le="+Inf"} 2',
                'rpc_server_call_duration_seconds_sum{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found"} 0.0006',
                'rpc_server_call_duration_seconds_count{rpc_method="bowl.storage.v1.StorageService/Lookup",rpc_response_status_code="not_found"} 2',
                # StoreActor split phases + read path + blob pwrite + sync
                'bowl_storage_actor_queue_wait_seconds_bucket{le="0.0005"} 100',
                'bowl_storage_actor_queue_wait_seconds_bucket{le="+Inf"} 100',
                'bowl_storage_actor_queue_wait_seconds_sum 0.015',
                'bowl_storage_actor_queue_wait_seconds_count 100',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="phase_a_validate",le="0.0005"} 50',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="phase_a_validate",le="+Inf"} 50',
                'bowl_storage_actor_tick_phase_duration_seconds_sum{phase="phase_a_validate"} 0.010',
                'bowl_storage_actor_tick_phase_duration_seconds_count{phase="phase_a_validate"} 50',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="phase_b_mutate",le="0.0005"} 50',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="phase_b_mutate",le="+Inf"} 50',
                'bowl_storage_actor_tick_phase_duration_seconds_sum{phase="phase_b_mutate"} 0.008',
                'bowl_storage_actor_tick_phase_duration_seconds_count{phase="phase_b_mutate"} 50',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="wal_apply_sync",le="0.005"} 4',
                'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="wal_apply_sync",le="+Inf"} 4',
                'bowl_storage_actor_tick_phase_duration_seconds_sum{phase="wal_apply_sync"} 0.012',
                'bowl_storage_actor_tick_phase_duration_seconds_count{phase="wal_apply_sync"} 4',
                'bowl_storage_read_op_duration_seconds_bucket{op="lookup",le="0.0005"} 2',
                'bowl_storage_read_op_duration_seconds_bucket{op="lookup",le="+Inf"} 2',
                'bowl_storage_read_op_duration_seconds_sum{op="lookup"} 0.0005',
                'bowl_storage_read_op_duration_seconds_count{op="lookup"} 2',
                'bowl_storage_kv_read_duration_seconds_bucket{op="point_get",le="0.00025"} 2',
                'bowl_storage_kv_read_duration_seconds_bucket{op="point_get",le="+Inf"} 2',
                'bowl_storage_kv_read_duration_seconds_sum{op="point_get"} 0.0003',
                'bowl_storage_kv_read_duration_seconds_count{op="point_get"} 2',
                'bowl_storage_blob_pwrite_duration_seconds_bucket{le="0.0025"} 100',
                'bowl_storage_blob_pwrite_duration_seconds_bucket{le="+Inf"} 100',
                'bowl_storage_blob_pwrite_duration_seconds_sum 0.110',
                'bowl_storage_blob_pwrite_duration_seconds_count 100',
                'bowl_storage_blob_pread_duration_seconds_bucket{le="0.001"} 4',
                'bowl_storage_blob_pread_duration_seconds_bucket{le="+Inf"} 4',
                'bowl_storage_blob_pread_duration_seconds_sum 0.002',
                'bowl_storage_blob_pread_duration_seconds_count 4',
                'bowl_storage_blob_read_source_total{source="hot_cache"} 12',
                'bowl_storage_blob_read_source_total{source="disk_pack"} 4',
                # Intentionally 0/0 Pebble BlockCache to assert "N/A (0 reads)" and ZERO "None%"
                'bowl_storage_pebble_block_cache_hits_total 0',
                'bowl_storage_pebble_block_cache_misses_total 0',
                'bowl_storage_pebble_read_amp 1',
                'bowl_storage_pebble_stall_events_total 0',
            ]) + "\n"
            (srv_m / "fio_seq_write_1job_pre.prom").write_text(srv_prom_pre, encoding="utf-8")
            (srv_m / "fio_seq_write_1job_post.prom").write_text(srv_prom_post, encoding="utf-8")
            (srv_m / "final.prom").write_text(srv_prom_post, encoding="utf-8")

            # 4. Jaeger traces (merged across traces_bowl-client.json and errors_bowl-storage.json)
            jg_dir = stage_dir / "server" / "jaeger"
            jg_dir.mkdir(parents=True)
            (jg_dir / "services.json").write_text(json.dumps({"data": ["bowl-client", "bowl-storage"]}), encoding="utf-8")
            (jg_dir / "operations_bowl-client.json").write_text(
                json.dumps({"service": "bowl-client", "operations": ["fuse.Write", "fuse.Lookup"]}),
                encoding="utf-8",
            )
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
                                "spanID": "s_cli_rpc",
                                "operationName": "bowl.storage.v1.StorageService/Write",
                                "startTime": 1700000000000500,
                                "duration": 3800,  # 3.8 ms
                                "processID": "p1",
                                "references": [{"refType": "CHILD_OF", "spanID": "s1"}],
                                "tags": [{"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Write"}],
                            },
                            {
                                "traceID": "abcdef1234567890abcdef1234567890",
                                "spanID": "s2",
                                "operationName": "bowl.storage.v1.StorageService/Write",
                                "startTime": 1700000000000700,
                                "duration": 3200,  # 3.2 ms -> exact hop delta = 0.6 ms!
                                "processID": "p2",
                                "references": [{"refType": "CHILD_OF", "spanID": "s_cli_rpc"}],
                                "tags": [
                                    {"key": "rpc.method", "value": "bowl.storage.v1.StorageService/Write"},
                                    {"key": "otel.status_code", "value": "ERROR"},
                                ],
                            },
                        ],
                    }
                ]
            }
            (jg_dir / "traces_bowl-client.json").write_text(json.dumps(jaeger_traces), encoding="utf-8")

            # 5. Prometheus live targets (captured BEFORE umount) & range queries
            prom_dir = stage_dir / "server" / "prometheus"
            prom_dir.mkdir(parents=True)
            (prom_dir / "targets_live.json").write_text(
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
                            "expr": "sum by (job) (rate(process_cpu_seconds_total[20s]))",
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

            # 6. Loki (with loki/prometheus self-noise that MUST be filtered out) + Client Logs + Pebble Events
            loki_dir = stage_dir / "server" / "loki"
            loki_dir.mkdir(parents=True)
            (loki_dir / "query_range.json").write_text(
                json.dumps({
                    "status": "success",
                    "data": {
                        "result": [
                            {
                                "stream": {"service": "loki"},
                                "values": [["1700000000000000000", 'level=warn msg="loki internal compaction noise"']],
                            },
                            {
                                "stream": {"service": "storage-service"},
                                "values": [
                                    [
                                        "1700000001000000000",
                                        json.dumps({"level": "WARN", "msg": "slow_rpc", "trace_id": "abcdef1234567890abcdef1234567890"}),
                                    ]
                                ],
                            },
                        ]
                    },
                }),
                encoding="utf-8",
            )
            cli_logs = stage_dir / "client" / "logs"
            cli_logs.mkdir(parents=True)
            (cli_logs / "bowl-mount.log").write_text(
                "\n".join([
                    '2026/11/11 16:00:00 parse "//172.16.1.10:4317": first path segment in URL cannot contain colon',
                    'time=2026-11-11T16:00:01Z level=warn msg="transient_retry" attempt=1',
                    json.dumps({"level": "INFO", "msg": "mount_ready", "disk_id": "d1"}),
                    json.dumps({"level": "WARN", "msg": "slow_fuse_op", "trace_id": "abcdef1234567890abcdef1234567890"}),
                    json.dumps({"level": "INFO", "msg": "unmount_complete", "disk_id": "d1"}),
                ]) + "\n",
                encoding="utf-8",
            )

            pebble_dir = stage_dir / "server" / "pebble" / "disk_d1"
            pebble_dir.mkdir(parents=True)
            (pebble_dir / "pebble_events.log").write_text(
                "\n".join([
                    json.dumps({"event": "open", "disk_id": "d1"}),
                    json.dumps({"event": "flush_end", "bytes": 4194304}),
                    json.dumps({"event": "write_stall_begin", "reason": "memtable count"}),
                ]) + "\n",
                encoding="utf-8",
            )
            (pebble_dir / "LOG").write_text("I261111 16:00:02 [pebble] flushing 2 memtables\n", encoding="utf-8")

            # 7. Synthetic pprof `.pb.gz` + fake `go` tool shim verifying both `-flat` and `-cum`
            srv_pprof_dir = stage_dir / "server" / "pprof"
            srv_pprof_dir.mkdir(parents=True)
            (srv_pprof_dir / "fio_seq_write_1job_cpu.pb.gz").write_bytes(b"fake-pb-gz")
            fake_go = root / "fake_go"
            fake_go.write_text(
                "#!/usr/bin/env bash\n"
                "if [[ \"$*\" == *\"-flat\"* ]]; then\n"
                "  printf 'Showing nodes accounting for 1.8s\\n      flat  flat%%   sum%%        cum   cum%%\\n     0.95s 42.0%% 42.0%%      0.95s 42.0%%  runtime.memmove\\n'\n"
                "else\n"
                "  printf 'Showing nodes accounting for 1.8s\\n      flat  flat%%   sum%%        cum   cum%%\\n     0.05s  2.2%%  2.2%%      1.70s 75.5%%  store.(*StoreActor).runTick\\n'\n"
                "fi\n",
                encoding="utf-8",
            )
            fake_go.chmod(fake_go.stat().st_mode | stat.S_IXUSR)

            with mock.patch("collect_telemetry.shutil.which", return_value=str(fake_go)):
                overview = collect_telemetry.build_telemetry_bundle(
                    suite="juicefs",
                    stage_dir=stage_dir,
                    results_dir=results_dir,
                )

            # Verify output files
            telem_dir = results_dir / "telemetry"
            self.assertTrue((telem_dir / "overview.json").exists())
            self.assertTrue((telem_dir / "overview.md").exists())
            self.assertTrue((telem_dir / "phase_deltas.json").exists())
            self.assertTrue((telem_dir / "client_stats" / "phase_deltas.json").exists())
            self.assertTrue((telem_dir / "jaeger" / "slow_traces.json").exists())
            self.assertTrue((telem_dir / "jaeger" / "error_traces.json").exists())
            self.assertTrue((telem_dir / "prometheus" / "metrics_summary.json").exists())
            self.assertTrue((telem_dir / "loki" / "container_logs.jsonl").exists())
            self.assertTrue((telem_dir / "loki" / "client_logs.jsonl").exists())
            self.assertTrue((telem_dir / "pebble" / "disk_d1" / "pebble_events.log").exists())

            # Verify Bowl-only stage filtering
            stage_names = [s.get("stage") or s.get("suite") for s in overview["slowest_stages"]]
            self.assertEqual(stage_names, ["mdtest_1p", "fio_seq_write_1job"])

            # Verify client phase delta & 9th write_behind subsystem + .epoch window
            fio_cli = overview["client_stats"]["phase_deltas"]["fio_seq_write_1job"]
            self.assertAlmostEqual(fio_cli["window_sec"], 4.25, places=2)
            self.assertEqual(fio_cli["write_behind"]["staged_calls"], 85)
            self.assertEqual(fio_cli["write_behind"]["flush_rpcs"], 96)
            self.assertAlmostEqual(fio_cli["write_behind"]["backpressure_wait_ms"], 18.0, places=2)
            self.assertEqual(fio_cli["readahead_waits"], 2)
            self.assertEqual(fio_cli["nscache_readlink_hit_rate_pct"], 80.0)
            self.assertEqual(fio_cli["queues"]["fuse_waiting"], 3)

            # Verify end-to-end hop-by-hop breakdown (`overview["phase_deltas"]`) & exact same-hop RPC delta
            fio_bd = overview["phase_deltas"]["fio_seq_write_1job"]
            self.assertAlmostEqual(fio_bd["wall_clock_sec"], 4.25, places=2)
            self.assertAlmostEqual(fio_bd["fuse_active_ms"], 305.25, places=2)
            hop_delta = fio_bd["rpc_hop_deltas"]
            self.assertEqual(hop_delta["matched_calls"], 102)
            write_hop = next(m for m in hop_delta["by_method"] if m["method"] == "Write")
            self.assertAlmostEqual(write_hop["client_rpc_mean_ms"], 2.50, places=3)
            self.assertAlmostEqual(write_hop["server_http_mean_ms"], 2.20, places=3)
            self.assertAlmostEqual(write_hop["server_rpc_mean_ms"], 1.80, places=3)
            self.assertAlmostEqual(write_hop["net_and_codec_mean_ms"], 0.70, places=3)
            self.assertAlmostEqual(write_hop["wire_and_client_codec_mean_ms"], 0.30, places=3)
            self.assertAlmostEqual(write_hop["server_http_codec_mean_ms"], 0.40, places=3)

            # Verify RPC status split (104 OK vs 2 non-OK not_found) & 0/0 BlockCache -> None ("N/A (0 reads)")
            srv_eng = overview["prometheus"]["server_engine"]
            self.assertIsNone(srv_eng["counters_and_gauges"]["pebble_block_cache_hit_rate_pct"])
            self.assertEqual(srv_eng["rpc_server_status"]["ok_calls"], 104)
            self.assertEqual(srv_eng["rpc_server_status"]["error_calls"], 2)
            self.assertEqual(srv_eng["rpc_server_status"]["errors_by_code"], {"not_found": 2})

            # Verify cross-node Jaeger trace exact hop delta (3.8 - 3.2 = 0.6 ms)
            self.assertEqual(overview["jaeger"]["cross_node_traces_count"], 1)
            self.assertEqual(overview["jaeger"]["error_traces_count"], 1)
            tr0 = overview["jaeger"]["slow_traces_top15"][0]
            self.assertAlmostEqual(tr0["total_duration_ms"], 4.5, places=2)
            self.assertAlmostEqual(tr0["rpc_network_codec_ms"], 0.6, places=2)

            # Verify Loki filtered out `loki` noise, captured all 3 client alerts (raw SDK + kv level=warn + JSON WARN), and parsed Pebble events
            self.assertEqual(overview["logs"]["loki_log_entries"], 1)
            self.assertEqual(overview["logs"]["server_alert_count"], 1)
            self.assertEqual(overview["logs"]["client_alert_count"], 3)
            self.assertEqual(len(overview["logs"]["client_lifecycle"]), 2)
            self.assertEqual(overview["pebble_logs"]["event_log_files"], 1)
            self.assertEqual(overview["pebble_logs"]["alert_count"], 1)

            # Verify pprof extracted BOTH -flat and -cum
            self.assertEqual(len(overview["pprof_profiles"]), 1)
            pp0 = overview["pprof_profiles"][0]
            self.assertIn("runtime.memmove", pp0["top_flat_entries"][0])
            self.assertIn("StoreActor", pp0["top_cum_entries"][0])

            # Verify overview.md & REPORT.md rendering: phase_deltas present, "N/A (0 reads)" present, ZERO "None%"
            overview_md = (telem_dir / "overview.md").read_text(encoding="utf-8")
            self.assertIn("分工作负载全链路逐跳耗时拆解表（`phase_deltas`）", overview_md)
            self.assertIn("同跳 Client ConnectRPC - Server ConnectRPC", overview_md)
            self.assertIn("N/A (0 reads)", overview_md)
            self.assertNotIn("None%", overview_md)

            loaded = collect_telemetry.load_telemetry_overview(results_dir)
            sec_md = "\n".join(
                collect_telemetry.render_report_telemetry_section(
                    loaded, "## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）"
                )
            )
            self.assertIn("### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）", sec_md)
            self.assertIn("同跳 Client ConnectRPC - Server ConnectRPC", sec_md)
            self.assertIn("N/A (0 reads)", sec_md)
            self.assertIn("runtime.memmove", sec_md)
            self.assertNotIn("None%", sec_md)

    def test_all_three_suite_parsers_support_legacy_and_deploy_prod_telemetry(self) -> None:
        sample_overview = {
            "suite": "bench",
            "deploy_profile": "deploy/prod (docker-compose.yaml + storage.env)",
            "phase_deltas": {
                "fio_seq_write_1job": {
                    "phase": "fio_seq_write_1job",
                    "wall_clock_sec": 4.0,
                    "wall_clock_ms": 4000.0,
                    "fuse_active_ms": 250.0,
                    "fuse_active_concurrency": 0.063,
                    "reference_basis": "FUSE 活跃总耗时",
                    "reference_total_ms": 250.0,
                    "cache_and_pipeline": {
                        "nscache_attr_hit_rate_pct": 92.5,
                        "nscache_entry_hit_rate_pct": 88.0,
                        "nscache_readlink_hit_rate_pct": None,
                        "write_behind_staged_mib": 384.0,
                        "write_behind_passthrough_mib": 16.0,
                        "write_behind_flush_rpcs": 96,
                        "readahead_prefetched_mib": 0.0,
                        "readahead_hit_mib": 0.0,
                        "pebble_block_cache_hit_rate_pct": None,
                    },
                    "hops": [
                        {
                            "hop_stage": "1. FUSE 入口",
                            "component": "fuse.* (总计)",
                            "calls": 100,
                            "total_ms": 250.0,
                            "mean_ms": 2.5,
                            "p50_ms": 2.1,
                            "p99_ms": 4.0,
                            "share_pct": 100.0,
                            "note": "错误返回 0 次",
                        },
                        {
                            "hop_stage": "3. 跨机网络+编解码差值",
                            "component": "同跳 Client ConnectRPC - Server ConnectRPC",
                            "calls": 96,
                            "total_ms": 48.0,
                            "mean_ms": 0.5,
                            "p50_ms": 0.4,
                            "p99_ms": 0.9,
                            "share_pct": 19.2,
                            "note": "Write:0.500ms",
                        },
                    ],
                }
            },
            "client_stats": {
                "top_ops_by_total_time": [
                    {
                        "op": "Write",
                        "calls": 100,
                        "total_duration_ms": 250.0,
                        "share_of_fuse_time_pct": 85.0,
                        "mean_us": 2500.0,
                        "p50_us": 2100.0,
                        "p95_us": 4096.0,
                        "p99_us": 4096.0,
                    }
                ],
                "nscache": {"attr_hit_rate_pct": 92.5, "entry_hit_rate_pct": 88.0, "negative_entry_hits": 12},
                "write_behind": {"staged_mib": 384.0, "passthrough_mib": 16.0, "flush_rpcs": 96},
                "read_ahead": {"prefetched_mib": 64.0, "hit_mib": 60.0},
            },
            "prometheus": {
                "server_engine": {
                    "rpc_server_latency_ms": [
                        {
                            "group": "Write",
                            "count": 100,
                            "total": 200.0,
                            "mean": 2.0,
                            "p50": 1.8,
                            "p95": 3.5,
                            "p99": 4.2,
                        }
                    ],
                    "actor_tick_phase_ms": [
                        {
                            "group": "phase_a_validate",
                            "count": 50,
                            "total": 10.0,
                            "mean": 0.2,
                            "p50": 0.18,
                            "p95": 0.35,
                            "p99": 0.5,
                        },
                        {
                            "group": "wal_apply_sync",
                            "count": 50,
                            "total": 40.0,
                            "mean": 0.8,
                            "p50": 0.7,
                            "p95": 1.2,
                            "p99": 1.5,
                        },
                    ],
                    "counters_and_gauges": {
                        "pebble_block_cache_hit_rate_pct": None,
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
                        "client_rpc_span_ms": 5.3,
                        "server_rpc_span_ms": 4.8,
                        "rpc_network_codec_ms": 0.5,
                        "top_spans": [
                            {"service": "bowl-client", "operation": "fuse.Write", "duration_ms": 6.4},
                            {"service": "bowl-storage", "operation": "StorageService/Write", "duration_ms": 4.8},
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
                    "top_flat_entries": ["0.90s 38.0% runtime.memmove"],
                    "top_cum_entries": ["1.20s 45.0% runtime.Syscall"],
                }
            ],
        }

        suites = ["juicefs", "drive9", "pjdfstest"]
        if (BENCH_ROOT / "fork" / "results" / "env_metadata.json").exists():
            suites.append("fork")
        for suite in suites:
            mod = load_suite_parser(suite)
            src_results = BENCH_ROOT / suite / "results"
            with tempfile.TemporaryDirectory() as tmp:
                tmp_res = Path(tmp) / "results"
                shutil.copytree(src_results, tmp_res)
                shutil.rmtree(tmp_res / "telemetry", ignore_errors=True)
                env_meta_clean = json.loads((tmp_res / "env_metadata.json").read_text(encoding="utf-8"))
                if isinstance(env_meta_clean.get("bowl_config"), dict):
                    env_meta_clean["bowl_config"].pop("deploy_profile", None)
                (tmp_res / "env_metadata.json").write_text(json.dumps(env_meta_clean), encoding="utf-8")

                # 1. Legacy results without telemetry/overview.json
                if suite == "pjdfstest":
                    stage_timings = [
                        json.loads(ln)
                        for ln in (tmp_res / "stage_timings.jsonl").read_text(encoding="utf-8").splitlines()
                        if ln.strip()
                    ]
                    legacy_sum = mod.parse_tap_log(
                        (tmp_res / "pjdfstest.log").read_text(encoding="utf-8"), stage_timings
                    )
                    legacy_sum["environment"] = env_meta_clean
                    legacy_md = mod.render_report_md(legacy_sum, env_meta_clean)
                    self.assertIn("## 6. 阿里云按量资源生命周期与销毁审计", legacy_md)
                    self.assertNotIn("生产可观测性与慢路径遥测诊断", legacy_md)
                else:
                    legacy_sum = mod.build_summary(tmp_res)
                    self.assertNotIn("telemetry_overview", legacy_sum)
                    legacy_md = mod.render_report(legacy_sum)
                    self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", legacy_md)
                    self.assertNotIn("生产可观测性与慢路径遥测诊断", legacy_md)

                # 2. deploy/prod results with telemetry/overview.json & phase_deltas
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
                    self.assertIn("### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）", prod_md)
                    self.assertIn("N/A (0 reads)", prod_md)
                    self.assertNotIn("None%", prod_md)
                    self.assertIn("## 7. 阿里云按量资源生命周期与销毁审计", prod_md)
                else:
                    prod_sum = mod.build_summary(tmp_res)
                    self.assertIn("telemetry_overview", prod_sum)
                    prod_md = mod.render_report(prod_sum)
                    self.assertIn("deploy/prod", prod_md)
                    self.assertIn("## 7. 生产可观测性与慢路径遥测诊断", prod_md)
                    self.assertIn("### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）", prod_md)
                    self.assertIn("同跳 Client ConnectRPC - Server ConnectRPC", prod_md)
                    self.assertIn("N/A (0 reads)", prod_md)
                    self.assertNotIn("None%", prod_md)
                    self.assertIn("## 8. 阿里云按量资源生命周期与销毁审计", prod_md)
                    self.assertIn("1122334455667788", prod_md)

    def test_async_pipeline_share_cap_hierarchy_and_stage_dedup(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            res_dir = root / "results"
            out_dir = res_dir / "telemetry"
            res_dir.mkdir(parents=True, exist_ok=True)
            out_dir.mkdir(parents=True, exist_ok=True)

            # 1. Stage timings alias deduplication (bowl_juicefs_bench + jfs_bench -> single jfs_bench)
            (res_dir / "stage_timings.jsonl").write_text(
                "\n".join([
                    json.dumps({"stage": "bowl_juicefs_bench", "duration_sec": 12.5}),
                    json.dumps({"stage": "jfs_bench", "duration_sec": 12.5}),
                    json.dumps({"stage": "bowl_fuse_perf_baseline", "duration_sec": 4.2}),
                    json.dumps({"stage": "fuse_perf_baseline", "duration_sec": 4.2}),
                    json.dumps({"directory": "mkdir", "duration_sec": 1.1}),
                ]) + "\n",
                encoding="utf-8",
            )
            stages = collect_telemetry.load_stage_timings(res_dir)
            labels = [s.get("stage") or s.get("directory") for s in stages]
            self.assertEqual(labels, ["jfs_bench", "fuse_perf_baseline", "mkdir"])

            # 2. Async RPC pipeline (407.5% raw ratio) bounded <= 100% + parent/child phase hierarchy
            cli_summary = {
                "phase_deltas": {
                    "d9_cold_io": {
                        "pre_epoch": 100.0,
                        "post_epoch": 130.0,
                        "window_sec": 30.0,
                        "fuse_active_ms": 5000.0,
                        "fuse_aggregate": {"calls": 1000, "total_ms": 5000.0, "mean_ms": 5.0, "p50_ms": 3.0, "p99_ms": 12.0, "errors": 0},
                    },
                    "d9_cold_seq_read": {
                        "pre_epoch": 105.0,
                        "post_epoch": 115.0,
                        "window_sec": 10.0,
                        "fuse_active_ms": 1000.0,
                        "fuse_aggregate": {"calls": 256, "total_ms": 1000.0, "mean_ms": 3.9, "p50_ms": 0.1, "p99_ms": 8.0, "errors": 0},
                    },
                }
            }
            prom_summary = {
                "client_engine": {
                    "phase_deltas": {
                        "d9_cold_io": {
                            "rpc_client_latency_ms": [{"group": "Read", "count": 300, "total": 4500.0, "mean": 15.0, "p50": 12.0, "p99": 25.0}],
                        },
                        "d9_cold_seq_read": {
                            "rpc_client_latency_ms": [{"group": "Read", "count": 256, "total": 4075.0, "mean": 15.9, "p50": 14.0, "p99": 28.0}],
                        },
                    }
                },
                "server_engine": {
                    "phase_deltas": {
                        "d9_cold_seq_read": {
                            "http_request_latency_ms": [{"group": "/storage.v1.StorageService/Read", "count": 256, "total": 3200.0, "mean": 12.5, "p50": 10.0, "p99": 22.0}],
                            "rpc_server_latency_ms": [{"group": "Read", "count": 256, "total": 500.0, "mean": 1.95, "p50": 1.5, "p99": 4.0}],
                        }
                    }
                },
            }
            bds = collect_telemetry.build_phase_hop_breakdowns(cli_summary, prom_summary, stages, out_dir)
            seq_bd = bds["d9_cold_seq_read"]
            self.assertEqual(seq_bd["parent_phase"], "d9_cold_io")
            self.assertEqual(bds["d9_cold_io"]["child_phases"], ["d9_cold_seq_read"])
            self.assertAlmostEqual(seq_bd["async_rpc_to_fuse_ratio"], 4.08, places=2)
            for h in seq_bd["hops"]:
                if h["share_pct"] is not None:
                    self.assertLessEqual(h["share_pct"], 100.0, f"hop {h['component']} exceeded 100%: {h['share_pct']}")

            md = "\n".join(collect_telemetry.render_phase_deltas_markdown(bds))
            self.assertIn("父汇总阶段 · 含子阶段 `d9_cold_seq_read`", md)
            self.assertIn("子阶段 · 隶属 `d9_cold_io`", md)
            self.assertIn("异步并发流水线 (累计为 FUSE 活跃 4.08x)", md)

    def test_process_logs_ignores_benign_pebble_empty_err_json(self) -> None:
        with tempfile.TemporaryDirectory() as td:
            stage_dir = Path(td) / "stage"
            out_dir = Path(td) / "out"
            loki_dir = stage_dir / "server" / "loki"
            loki_dir.mkdir(parents=True, exist_ok=True)
            out_dir.mkdir(parents=True, exist_ok=True)

            loki_payload = {
                "status": "success",
                "data": {
                    "result": [
                        {
                            "stream": {"service": "pebble_events"},
                            "values": [
                                ["1000000001", json.dumps({"disk_id": "d1", "event": "wal_created", "file_num": 5, "err": ""})],
                                ["1000000002", json.dumps({"disk_id": "d1", "event": "flush_end", "err": ""})],
                                ["1000000003", json.dumps({"disk_id": "d1", "event": "compaction_end", "err": ""})],
                                ["1000000004", json.dumps({"disk_id": "d1", "event": "write_stall_begin", "reason": "memtable count"})],
                                ["1000000005", json.dumps({"disk_id": "d1", "event": "table_deleted", "err": "enosys"})],
                            ],
                        },
                        {
                            "stream": {"service": "bowl-storage"},
                            "values": [
                                ["1000000006", json.dumps({"level": "INFO", "msg": "server started", "err": ""})],
                                ["1000000007", json.dumps({"level": "WARN", "msg": "store actor slow tick", "total_ms": 22.1})],
                                ["1000000008", "panic: unexpected raw crash"],
                            ],
                        },
                    ]
                },
            }
            (loki_dir / "query_range.json").write_text(json.dumps(loki_payload), encoding="utf-8")

            res = collect_telemetry.process_logs(stage_dir, out_dir)
            self.assertEqual(res["loki_log_entries"], 8)
            self.assertEqual(res["server_alert_count"], 4)
            alert_events_or_msgs = [
                a.get("event") or a.get("msg")
                for a in res["server_alerts_sample"]
            ]
            self.assertEqual(
                alert_events_or_msgs,
                [
                    "write_stall_begin",
                    "table_deleted",
                    "store actor slow tick",
                    "panic: unexpected raw crash",
                ],
            )


    def test_extract_server_prom_snapshot_fork_and_storage_usage(self):
        with tempfile.TemporaryDirectory() as tmp:
            pre_path = Path(tmp) / "pre.prom"
            post_path = Path(tmp) / "post.prom"
            pre_path.write_text(
                "\n".join([
                    'bowl_storage_control_forks_total{result="ok"} 1',
                    'bowl_storage_control_commits_reclaimed_total 0',
                ]),
                encoding="utf-8",
            )
            post_path.write_text(
                "\n".join([
                    'bowl_storage_pack_physical_bytes 1048576',
                    'bowl_storage_pack_referenced_bytes 4194304',
                    'bowl_storage_data_dir_physical_bytes 2097152',
                    'bowl_storage_commits_active 3',
                    'bowl_storage_commits_physical_bytes 1048576',
                    'bowl_storage_control_commits_reclaimed_total 2',
                    'bowl_storage_control_forks_total{result="ok"} 4',
                    'bowl_storage_control_forks_total{result="error"} 1',
                    'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="checkpoint",le="0.05"} 3',
                    'bowl_storage_actor_tick_phase_duration_seconds_bucket{phase="checkpoint",le="+Inf"} 3',
                    'bowl_storage_actor_tick_phase_duration_seconds_sum{phase="checkpoint"} 0.060',
                    'bowl_storage_actor_tick_phase_duration_seconds_count{phase="checkpoint"} 3',
                    'bowl_storage_control_fork_stage_duration_seconds_bucket{stage="sanitize",le="0.05"} 3',
                    'bowl_storage_control_fork_stage_duration_seconds_bucket{stage="sanitize",le="+Inf"} 3',
                    'bowl_storage_control_fork_stage_duration_seconds_sum{stage="sanitize"} 0.045',
                    'bowl_storage_control_fork_stage_duration_seconds_count{stage="sanitize"} 3',
                ]),
                encoding="utf-8",
            )
            pre_p = collect_telemetry.parse_prom_text(pre_path)
            post_p = collect_telemetry.parse_prom_text(post_path)
            snap = collect_telemetry.extract_server_prom_snapshot(post_p, pre_p=pre_p)
            cg = snap["counters_and_gauges"]
            self.assertEqual(cg["pack_physical_bytes"], 1048576.0)
            self.assertEqual(cg["pack_referenced_bytes"], 4194304.0)
            self.assertEqual(cg["data_dir_physical_bytes"], 2097152.0)
            self.assertEqual(cg["commits_active"], 3.0)
            self.assertEqual(cg["commits_physical_bytes"], 1048576.0)
            self.assertEqual(cg["commits_reclaimed"], 2.0)
            self.assertEqual(cg["forks_by_result"], {"ok": 3.0, "error": 1.0})
            self.assertEqual([r["group"] for r in snap["control_fork_stage_ms"]], ["sanitize"])
            self.assertEqual(snap["actor_tick_phase_ms"][0]["group"], "checkpoint")


if __name__ == "__main__":
    unittest.main()
