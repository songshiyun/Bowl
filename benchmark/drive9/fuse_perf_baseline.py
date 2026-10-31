#!/usr/bin/env python3
"""Drive9 E2E FUSE performance baseline workload (extracted from drive9/e2e/fuse-performance-baseline.sh).

Runs SHA-256 verified small-file write/read, large-file write/multi-pass read,
and SQLite DELETE + WAL journal transaction workloads with integrity & WAL
TRUNCATE checkpoint verification on any mounted FUSE directory.
"""

from __future__ import annotations

import hashlib
import json
import math
import os
import shutil
import sqlite3
import sys
import time

if len(sys.argv) < 8:
    raise SystemExit(
        "usage: fuse_perf_baseline.py <work_mount_dir> <metrics_json_out> "
        "<small_files> <small_bytes> <large_mb> <read_passes> <sqlite_rows>"
    )

root = os.path.abspath(sys.argv[1])
metrics_path = sys.argv[2]
small_files = int(sys.argv[3])
small_bytes = int(sys.argv[4])
large_mb = int(sys.argv[5])
read_passes = int(sys.argv[6])
sqlite_rows = int(sys.argv[7])
large_bytes = large_mb * 1024 * 1024
chunk_size = 1024 * 1024


def deterministic_bytes(label: str, size: int) -> bytes:
    seed = hashlib.sha256(label.encode()).digest()
    body = bytearray()
    counter = 0
    while len(body) < size:
        body.extend(hashlib.sha256(seed + counter.to_bytes(8, "big")).digest())
        counter += 1
    return bytes(body[:size])


def iter_large_chunks(label: str, total_bytes: int):
    chunks = int(math.ceil(total_bytes / chunk_size))
    for index in range(chunks):
        size = min(chunk_size, total_bytes - index * chunk_size)
        yield deterministic_bytes(f"{label}:{index}", size)


def sha256_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            chunk = handle.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def fsync_parent(path: str) -> None:
    parent = os.path.dirname(path)
    fd = os.open(parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def rate(bytes_count: int, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return bytes_count / seconds


def metric(seconds: float, bytes_count: int = 0, files: int = 0, rows: int = 0, extra: dict | None = None) -> dict:
    out: dict = {
        "seconds": seconds,
        "bytes": bytes_count,
        "files": files,
        "rows": rows,
        "bytes_per_second": rate(bytes_count, seconds),
        "mib_per_second": rate(bytes_count, seconds) / (1024 * 1024),
    }
    if files:
        out["files_per_second"] = files / seconds if seconds > 0 else 0.0
    if rows:
        out["rows_per_second"] = rows / seconds if seconds > 0 else 0.0
    if extra:
        out.update(extra)
    return out


def timed(call):
    start = time.perf_counter()
    result = call()
    return time.perf_counter() - start, result


def small_file_write(small_dir: str):
    manifest = {}
    bytes_count = 0
    os.makedirs(small_dir, exist_ok=True)
    for index in range(small_files):
        rel = f"small-{index:04d}.bin"
        path = os.path.join(small_dir, rel)
        data = deterministic_bytes(f"small:{index}", small_bytes)
        with open(path, "wb") as handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        manifest[rel] = hashlib.sha256(data).hexdigest()
        bytes_count += len(data)
    fsync_parent(small_dir)
    return manifest, bytes_count


def small_file_read(small_dir: str, manifest: dict) -> int:
    bytes_count = 0
    for rel, want_hash in sorted(manifest.items()):
        path = os.path.join(small_dir, rel)
        got_hash = sha256_file(path)
        if got_hash != want_hash:
            raise AssertionError(f"small file hash mismatch: {rel}")
        bytes_count += os.path.getsize(path)
    return bytes_count


def large_file_write(path: str):
    digest = hashlib.sha256()
    bytes_count = 0
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "wb") as handle:
        for chunk in iter_large_chunks("large", large_bytes):
            handle.write(chunk)
            digest.update(chunk)
            bytes_count += len(chunk)
        handle.flush()
        os.fsync(handle.fileno())
    fsync_parent(path)
    return digest.hexdigest(), bytes_count


def large_file_read(path: str, want_hash: str) -> int:
    got_hash = sha256_file(path)
    if got_hash != want_hash:
        raise AssertionError(f"large file hash mismatch: {got_hash} != {want_hash}")
    return os.path.getsize(path)


def sqlite_workload(db_path: str, requested_journal_mode: str, label: str) -> dict:
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path, timeout=30.0)
    conn.execute("PRAGMA busy_timeout=30000")
    conn.execute("PRAGMA mmap_size=0")
    journal_mode = conn.execute(f"PRAGMA journal_mode={requested_journal_mode.upper()}").fetchone()[0].lower()
    if journal_mode != requested_journal_mode.lower():
        raise AssertionError(f"sqlite journal_mode={journal_mode} want={requested_journal_mode.lower()}")
    conn.execute("PRAGMA synchronous=FULL")
    if journal_mode == "wal":
        conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS items "
        "(id INTEGER PRIMARY KEY, version INTEGER NOT NULL, payload BLOB NOT NULL, checksum TEXT NOT NULL)"
    )
    conn.commit()

    insert_start = time.perf_counter()
    with conn:
        for row_id in range(1, sqlite_rows + 1):
            payload = deterministic_bytes(f"{label}-insert:{row_id}", 512)
            conn.execute(
                "INSERT INTO items(id, version, payload, checksum) VALUES (?, ?, ?, ?)",
                (row_id, 1, payload, hashlib.sha256(payload).hexdigest()),
            )
    insert_seconds = time.perf_counter() - insert_start

    update_rows = 0
    update_start = time.perf_counter()
    with conn:
        for row_id in range(1, sqlite_rows + 1, 3):
            payload = deterministic_bytes(f"{label}-update:{row_id}", 512)
            conn.execute(
                "UPDATE items SET version=?, payload=?, checksum=? WHERE id=?",
                (2, payload, hashlib.sha256(payload).hexdigest(), row_id),
            )
            update_rows += 1
    update_seconds = time.perf_counter() - update_start

    read_start = time.perf_counter()
    rows = list(conn.execute("SELECT id, version, checksum, payload FROM items ORDER BY id"))
    read_seconds = time.perf_counter() - read_start

    digest = hashlib.sha256()
    payload_bytes = 0
    max_version = 0
    for row_id, version, checksum, payload in rows:
        payload_hash = hashlib.sha256(payload).hexdigest()
        if payload_hash != checksum:
            raise AssertionError(f"sqlite payload checksum mismatch row={row_id}")
        size = len(payload)
        digest.update(f"{row_id}:{version}:{checksum}:{size}\n".encode())
        payload_bytes += size
        max_version = max(max_version, version)
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    if integrity != "ok":
        raise AssertionError(f"sqlite integrity_check={integrity}")
    checkpoint_seconds = 0.0
    checkpoint_result = None
    post_checkpoint_integrity = integrity
    if journal_mode == "wal":
        checkpoint_start = time.perf_counter()
        checkpoint_result = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
        checkpoint_seconds = time.perf_counter() - checkpoint_start
        if checkpoint_result is None or checkpoint_result[0] != 0:
            raise AssertionError(f"sqlite wal_checkpoint(TRUNCATE)={checkpoint_result}")
        post_checkpoint_integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
        if post_checkpoint_integrity != "ok":
            raise AssertionError(f"sqlite post-checkpoint integrity_check={post_checkpoint_integrity}")
    conn.close()

    return {
        "journal_mode": journal_mode,
        "insert_seconds": insert_seconds,
        "update_seconds": update_seconds,
        "read_seconds": read_seconds,
        "rows": len(rows),
        "update_rows": update_rows,
        "payload_verified_rows": len(rows),
        "max_version": max_version,
        "payload_bytes": payload_bytes,
        "fingerprint": digest.hexdigest(),
        "integrity_check": integrity,
        "checkpoint_seconds": checkpoint_seconds,
        "checkpoint_result": list(checkpoint_result) if checkpoint_result is not None else None,
        "post_checkpoint_integrity_check": post_checkpoint_integrity,
    }


def main() -> None:
    shutil.rmtree(root, ignore_errors=True)
    os.makedirs(root, exist_ok=True)

    metrics = {
        "schema": "drive9-fuse-performance/v1",
        "generated_at_unix": time.time(),
        "params": {
            "small_files": small_files,
            "small_bytes": small_bytes,
            "large_mb": large_mb,
            "large_bytes": large_bytes,
            "read_passes": read_passes,
            "sqlite_rows": sqlite_rows,
        },
        "workloads": {},
        "correctness": {},
    }

    small_dir = os.path.join(root, "small-files")
    seconds, small_write_result = timed(lambda: small_file_write(small_dir))
    small_manifest, small_write_bytes = small_write_result
    metrics["workloads"]["small_file_write"] = metric(seconds, small_write_bytes, files=small_files)

    seconds, small_read_bytes = timed(lambda: small_file_read(small_dir, small_manifest))
    metrics["workloads"]["small_file_read"] = metric(seconds, small_read_bytes, files=small_files)
    metrics["correctness"]["small_file_manifest_sha256"] = hashlib.sha256(
        json.dumps(small_manifest, sort_keys=True).encode()
    ).hexdigest()

    large_path = os.path.join(root, "large-file", "large.bin")
    seconds, large_write_result = timed(lambda: large_file_write(large_path))
    large_hash, large_write_bytes = large_write_result
    metrics["workloads"]["large_file_write"] = metric(seconds, large_write_bytes, files=1)
    metrics["correctness"]["large_file_sha256"] = large_hash

    large_reads = []
    for read_index in range(read_passes):
        seconds, large_read_bytes = timed(lambda: large_file_read(large_path, large_hash))
        large_reads.append(metric(seconds, large_read_bytes, files=1, extra={"pass": read_index + 1}))
    metrics["workloads"]["large_file_reads"] = large_reads

    sqlite_path = os.path.join(root, "sqlite", "perf.db")
    sqlite_result = sqlite_workload(sqlite_path, "delete", "sqlite-delete")
    metrics["workloads"]["sqlite_insert_transaction"] = metric(
        sqlite_result["insert_seconds"],
        rows=sqlite_result["rows"],
    )
    metrics["workloads"]["sqlite_update_transaction"] = metric(
        sqlite_result["update_seconds"],
        rows=sqlite_result["update_rows"],
    )
    metrics["workloads"]["sqlite_read_aggregate"] = metric(
        sqlite_result["read_seconds"],
        bytes_count=sqlite_result["payload_bytes"],
        rows=sqlite_result["rows"],
        extra={
            "journal_mode": sqlite_result["journal_mode"],
            "max_version": sqlite_result["max_version"],
            "payload_verified_rows": sqlite_result["payload_verified_rows"],
            "integrity_check": sqlite_result["integrity_check"],
        },
    )
    metrics["correctness"]["sqlite_fingerprint"] = sqlite_result["fingerprint"]

    sqlite_wal_path = os.path.join(root, "sqlite-wal", "perf.db")
    sqlite_wal_result = sqlite_workload(sqlite_wal_path, "wal", "sqlite-wal")
    metrics["workloads"]["sqlite_wal_insert_transaction"] = metric(
        sqlite_wal_result["insert_seconds"],
        rows=sqlite_wal_result["rows"],
    )
    metrics["workloads"]["sqlite_wal_update_transaction"] = metric(
        sqlite_wal_result["update_seconds"],
        rows=sqlite_wal_result["update_rows"],
    )
    metrics["workloads"]["sqlite_wal_read_aggregate"] = metric(
        sqlite_wal_result["read_seconds"],
        bytes_count=sqlite_wal_result["payload_bytes"],
        rows=sqlite_wal_result["rows"],
        extra={
            "journal_mode": sqlite_wal_result["journal_mode"],
            "max_version": sqlite_wal_result["max_version"],
            "payload_verified_rows": sqlite_wal_result["payload_verified_rows"],
            "integrity_check": sqlite_wal_result["integrity_check"],
        },
    )
    metrics["workloads"]["sqlite_wal_checkpoint_truncate"] = metric(
        sqlite_wal_result["checkpoint_seconds"],
        rows=sqlite_wal_result["rows"],
        extra={
            "checkpoint_busy": sqlite_wal_result["checkpoint_result"][0],
            "checkpoint_log_frames": sqlite_wal_result["checkpoint_result"][1],
            "checkpointed_frames": sqlite_wal_result["checkpoint_result"][2],
            "integrity_check": sqlite_wal_result["post_checkpoint_integrity_check"],
        },
    )
    metrics["correctness"]["sqlite_wal_fingerprint"] = sqlite_wal_result["fingerprint"]

    os.makedirs(os.path.dirname(os.path.abspath(metrics_path)), exist_ok=True)
    with open(metrics_path, "w", encoding="utf-8") as handle:
        json.dump(metrics, handle, indent=2, sort_keys=True)
        handle.write("\n")
    shutil.rmtree(root, ignore_errors=True)


if __name__ == "__main__":
    main()
