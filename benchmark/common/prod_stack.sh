#!/usr/bin/env bash
# ==============================================================================
# Bowl Benchmark — Shared `deploy/prod` Stack & Telemetry Orchestration Helpers
# ==============================================================================
# 以 deploy/prod/docker-compose.yaml + deploy/prod/storage.env 为唯一权威部署源：
# 1. 打包 deploy/prod 编排配置、数据库迁移 SQL 与交叉编译好的 linux/amd64 二进制；
# 2. 在远端 Server 节点将 /var/lib/docker 绑定至本地 NVMe SSD (/mnt/nvme/docker)，
#    构建与线上 Dockerfile 运行阶段一致的 bowl-storage / bowl-storage-migration 镜像，
#    并通过最小 docker-compose.bench.yaml override 打通跨机 VPC 内网 Jaeger OTLP 与
#    Prometheus 双端（Server :9090 + Client :9567）2s 高频抓取；
# 3. 提供 Client 侧阶段快照（含 bowl stats 8 大子系统 JSON、Prometheus 指标、pprof）
#    与 Server 侧可观测性后端（Jaeger / Prometheus / Loki / Pebble LOG）统一收割能力。

BOWL_BENCH_TRACE_SAMPLE_RATIO="${BOWL_BENCH_TRACE_SAMPLE_RATIO:-0.10}"
BOWL_BENCH_LOG_LEVEL="${BOWL_BENCH_LOG_LEVEL:-warn}"

# prepare_prod_bundle <repo_root> <work_dir>
# 将 deploy/prod、services/storage/db/migrations 及预编译二进制打包为 prod-bundle.tar.gz。
prepare_prod_bundle() {
  local repo_root="$1"
  local work_dir="$2"
  local bundle_root="${work_dir}/prod-bundle"

  rm -rf "${bundle_root}"
  mkdir -p "${bundle_root}/deploy" "${bundle_root}/services/storage/db" "${bundle_root}/bin"

  cp -a "${repo_root}/deploy/prod" "${bundle_root}/deploy/prod"
  cp -a "${repo_root}/services/storage/db/migrations" "${bundle_root}/services/storage/db/migrations"
  cp -a "${work_dir}/storage-service" "${bundle_root}/bin/storage-service"
  cp -a "${work_dir}/goose" "${bundle_root}/bin/goose"
  chmod +x "${bundle_root}/bin/storage-service" "${bundle_root}/bin/goose"
  find "${bundle_root}" -name '._*' -delete 2>/dev/null || true

  COPYFILE_DISABLE=1 tar -czf "${work_dir}/prod-bundle.tar.gz" -C "${bundle_root}" .
  rm -rf "${bundle_root}"
}

# install_remote_prod_stack_ctl <server_priv_ip> <client_priv_ip> <suite_mode>
# 在远端 Server 节点标准输入执行（需已上传 /tmp/prod-bundle.tar.gz 且挂载好 /mnt/nvme）。
# suite_mode:
#   - "prod"      : 100% 保持 deploy/prod/storage.env 生产配置（noatime + 1.0s 缓存 + warn JSON + OTel 压测采样）
#   - "pjdfstest" : 基于 deploy/prod/storage.env，仅覆写 strictatime 与 0s 缓存 TTL 以满足 POSIX 语义合规断言
remote_setup_prod_stack() {
  local server_priv_ip="$1"
  local client_priv_ip="$2"
  local suite_mode="${3:-prod}"
  local trace_ratio="${BOWL_BENCH_TRACE_SAMPLE_RATIO:-0.10}"

  cat <<EOF_PROD_SETUP
set -euo pipefail

# 0. 关闭宿主机 TCP autocorking 防御双次小写与 Delayed ACK 交互延迟
sysctl -qw net.ipv4.tcp_autocorking=0 2>/dev/null || true

# 1. 将 /var/lib/docker 绑定至本地 NVMe SSD，确保 PG18 / Storage / Prometheus / Loki 命名卷全落 NVMe
mkdir -p /mnt/nvme/docker /var/lib/docker
if ! mountpoint -q /var/lib/docker; then
  systemctl stop docker docker.socket containerd 2>/dev/null || true
  if [[ -d /var/lib/docker ]] && [[ -n "\$(ls -A /var/lib/docker 2>/dev/null || true)" ]] && [[ -z "\$(ls -A /mnt/nvme/docker 2>/dev/null || true)" ]]; then
    cp -a /var/lib/docker/. /mnt/nvme/docker/
  fi
  mount --bind /mnt/nvme/docker /var/lib/docker
fi

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
if ! command -v docker >/dev/null 2>&1 || ! docker compose version >/dev/null 2>&1; then
  apt-get update -y -qq
  apt-get install -y -qq docker.io docker-compose-v2 python3 jq curl ca-certificates
fi
systemctl enable --now docker

# 2. 解压 deploy/prod 与 migrations 到 /opt/bowl（保持与仓库一致的相对目录结构，防御性剔除 ._ 元数据文件）
rm -rf /opt/bowl/deploy/prod /opt/bowl/services/storage/db/migrations /opt/bowl/image-build
mkdir -p /opt/bowl
tar -xzf /tmp/prod-bundle.tar.gz -C /opt/bowl
find /opt/bowl -name '._*' -delete 2>/dev/null || true
rm -f /tmp/prod-bundle.tar.gz

# 3. 基于预编译二进制构建与 services/storage/Dockerfile 运行态完全同构的本地镜像
mkdir -p /opt/bowl/image-build/storage /opt/bowl/image-build/migration
cp -a /opt/bowl/bin/storage-service /opt/bowl/image-build/storage/storage-service
cat >/opt/bowl/image-build/storage/Dockerfile <<'DOCKERFILE_STORAGE'
FROM alpine:3.24
RUN apk --no-cache add ca-certificates tzdata netcat-openbsd && \
    addgroup -g 10001 -S appgroup && \
    adduser -u 10001 -S appuser -G appgroup && \
    mkdir -p /var/lib/bowl/data && \
    chown -R appuser:appgroup /var/lib/bowl
WORKDIR /app
COPY storage-service /app/storage-service
USER appuser:appgroup
EXPOSE 9000 9090
ENTRYPOINT ["/app/storage-service"]
DOCKERFILE_STORAGE
docker build -q -t bowl-storage:latest /opt/bowl/image-build/storage >/dev/null

cp -a /opt/bowl/bin/goose /opt/bowl/image-build/migration/goose
cat >/opt/bowl/image-build/migration/Dockerfile <<'DOCKERFILE_MIGRATION'
FROM alpine:3.24
RUN apk --no-cache add ca-certificates tzdata
COPY goose /bin/goose
ENTRYPOINT ["/bin/goose"]
DOCKERFILE_MIGRATION
docker build -q -t bowl-storage-migration:latest /opt/bowl/image-build/migration >/dev/null

# 4. 生成 Benchmark 专用高频 Prometheus 抓取配置（同时抓取 Server :9090 与 Client :9567）
cat >/opt/bowl/deploy/prod/prometheus/prometheus.bench.yml <<'PROM_CFG'
global:
  scrape_interval: 2s
  evaluation_interval: 2s

scrape_configs:
  - job_name: "bowl-storage"
    static_configs:
      - targets: ["storage-service:9090"]
        labels:
          service: "storage-service"
          env: "production"

  - job_name: "bowl-client"
    static_configs:
      - targets: ["${client_priv_ip}:9567"]
        labels:
          service: "bowl-client"
          env: "production"

  - job_name: "bowl-alloy"
    static_configs:
      - targets: ["alloy:12345"]
        labels:
          service: "alloy"
          env: "production"

  - job_name: "prometheus"
    static_configs:
      - targets: ["localhost:9090"]
PROM_CFG

# 5. 生成轻量 docker-compose.bench.yaml override（自适应绑定 VPC OTLP 接收端，零复制复用 deploy/prod）
OTLP_RECEIVER_SVC="jaeger"
if grep -Eq '^OTEL_EXPORTER_OTLP_ENDPOINT=.*alloy:4317' /opt/bowl/deploy/prod/storage.env 2>/dev/null; then
  OTLP_RECEIVER_SVC="alloy"
fi

{
  cat <<'COMPOSE_OVERRIDE_HEAD'
services:
  storage-service:
    ulimits:
      nofile:
        soft: 1048576
        hard: 1048576
    environment:
COMPOSE_OVERRIDE_HEAD
  echo "      OTEL_TRACES_SAMPLER_ARG: \"${trace_ratio}\""
  if [[ "${suite_mode}" == "pjdfstest" ]]; then
    cat <<'PJDFS_ENV'
      STORAGE_ATIME_MODE: "strictatime"
      STORAGE_ENTRY_TIMEOUT_SECONDS: "0"
      STORAGE_ATTR_TIMEOUT_SECONDS: "0"
      STORAGE_DIR_ENTRY_TIMEOUT_SECONDS: "0"
PJDFS_ENV
  fi
  if ! grep -Eq '0\.0\.0\.0:.*:4317|^ *- *"?4317:4317' /opt/bowl/deploy/prod/docker-compose.yaml 2>/dev/null; then
    cat <<COMPOSE_OTLP_PORTS

  \${OTLP_RECEIVER_SVC}:
    ports:
      - "${server_priv_ip}:4317:4317"
      - "${server_priv_ip}:4318:4318"
COMPOSE_OTLP_PORTS
  fi
  cat <<'COMPOSE_OVERRIDE_TAIL'

  prometheus:
    volumes:
      - ./prometheus/prometheus.bench.yml:/etc/prometheus/prometheus.yml:ro
      - prometheus_data:/prometheus
COMPOSE_OVERRIDE_TAIL
} >/opt/bowl/deploy/prod/docker-compose.bench.yaml

# 6. 安装远端统一控制与遥测收割脚本 /usr/local/bin/bowl-prod-ctl
cat >/usr/local/bin/bowl-prod-ctl <<'PROD_CTL'
#!/usr/bin/env bash
set -euo pipefail

COMPOSE_ARGS=(
  -p bowl
  -f /opt/bowl/deploy/prod/docker-compose.yaml
  -f /opt/bowl/deploy/prod/docker-compose.bench.yaml
)

wait_http_ok() {
  local url="\$1"
  local tries="\${2:-30}"
  local i
  for ((i=1; i<=tries; i++)); do
    if curl -fsS --max-time 2 "\${url}" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

fetch_nonempty() {
  local url="\$1"
  local dest="\$2"
  local max_t="\${3:-5}"
  local tmp="\${dest}.tmp.\$\$"
  if curl -fsS --max-time "\${max_t}" "\${url}" > "\${tmp}" 2>/dev/null && [[ -s "\${tmp}" ]]; then
    mv "\${tmp}" "\${dest}"
    return 0
  fi
  rm -f "\${tmp}"
  return 1
}

cmd="\${1:-}"
shift || true

case "\${cmd}" in
  up)
    COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" up -d --remove-orphans
    wait_http_ok "http://127.0.0.1:9090/readyz" 45
    wait_http_ok "http://127.0.0.1:9686/" 30
    wait_http_ok "http://127.0.0.1:9091/-/ready" 30
    wait_http_ok "http://127.0.0.1:9310/ready" 30
    if [[ ! -f /var/run/bowl-prod-started.epoch ]]; then
      date +%s > /var/run/bowl-prod-started.epoch
    fi
    ;;

  start-storage)
    COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" up -d --no-deps storage-service
    wait_http_ok "http://127.0.0.1:9090/readyz" 45
    ;;

  stop-storage)
    # 仅停止业务存储服务以释放 CPU/磁盘资源，严禁停止 jaeger/prometheus/loki/alloy 以免清空内存遥测与抓取状态
    COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" stop storage-service
    ;;

  snapshot-targets)
    out_dir="\${1:-/tmp/bowl-telemetry/server}"
    mkdir -p "\${out_dir}/prometheus" "\${out_dir}/metrics" "\${out_dir}/diskstats"
    fetch_nonempty "http://127.0.0.1:9091/api/v1/targets" "\${out_dir}/prometheus/targets_live.json" 5 || true
    if [[ -s "\${out_dir}/prometheus/targets_live.json" ]]; then
      cp -a "\${out_dir}/prometheus/targets_live.json" "\${out_dir}/prometheus/targets.json"
    fi
    fetch_nonempty "http://127.0.0.1:9090/metrics" "\${out_dir}/metrics/final.prom" 5 || true
    cat /proc/diskstats > "\${out_dir}/diskstats/final.txt" 2>/dev/null || true
    ;;

  stop)
    COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" stop
    ;;

  down-volumes)
    COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" down -v --remove-orphans
    rm -f /var/run/bowl-prod-started.epoch
    ;;

  pg-version)
    docker exec bowl-postgres postgres --version 2>/dev/null || echo "postgres (PostgreSQL) 18 (alpine)"
    ;;

  export-telemetry)
    out_dir="\${1:-/tmp/bowl-telemetry/server}"
    start_epoch="\${2:-}"
    end_epoch="\${3:-}"
    mkdir -p "\${out_dir}/jaeger" "\${out_dir}/prometheus" "\${out_dir}/loki" "\${out_dir}/logs" "\${out_dir}/metrics" "\${out_dir}/diskstats" "\${out_dir}/pprof" "\${out_dir}/pebble"

    # 若可观测性容器处于停止态，仅拉起可观测性服务查询历史库
    if ! curl -fsS --max-time 2 "http://127.0.0.1:9686/" >/dev/null 2>&1 || ! curl -fsS --max-time 2 "http://127.0.0.1:9091/-/ready" >/dev/null 2>&1; then
      COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" up -d --no-deps jaeger prometheus loki alloy >/dev/null 2>&1 || true
      wait_http_ok "http://127.0.0.1:9686/" 20 || true
      wait_http_ok "http://127.0.0.1:9091/-/ready" 20 || true
      wait_http_ok "http://127.0.0.1:9310/ready" 20 || true
    fi

    # 等待 OTLP Batch 与 Alloy 日志流刷盘落库
    sleep 2

    if [[ -z "\${start_epoch}" ]]; then
      if [[ -f /var/run/bowl-prod-started.epoch ]]; then
        start_epoch="\$(cat /var/run/bowl-prod-started.epoch)"
      else
        start_epoch="\$(( \$(date +%s) - 1800 ))"
      fi
    fi
    if [[ -z "\${end_epoch}" ]]; then
      end_epoch="\$(date +%s)"
    fi
    if (( end_epoch <= start_epoch )); then
      end_epoch="\$(( start_epoch + 60 ))"
    fi

    OUT_DIR="\${out_dir}" START_EPOCH="\${start_epoch}" END_EPOCH="\${end_epoch}" python3 - <<'PY_EXPORT'
import json
import os
import urllib.parse
import urllib.request
from pathlib import Path

out_dir = Path(os.environ["OUT_DIR"])
start_s = int(os.environ["START_EPOCH"])
end_s = int(os.environ["END_EPOCH"])
# 留出前后缓冲窗口，确保边界 Span 与日志完整命中
win_start_s = max(0, start_s - 15)
win_end_s = end_s + 15
start_us = win_start_s * 1_000_000
end_us = win_end_s * 1_000_000
start_ns = win_start_s * 1_000_000_000
end_ns = win_end_s * 1_000_000_000
step_s = max(2, (win_end_s - win_start_s) // 240)


def fetch_json(url: str, timeout: float = 12.0):
    try:
        with urllib.request.urlopen(url, timeout=timeout) as resp:
            return json.loads(resp.read().decode("utf-8", errors="replace"))
    except Exception as exc:
        return {"_fetch_error": str(exc), "_url": url}


def discover_jaeger_operations(jaeger_base: str, svc: str) -> list[str]:
    ops: list[str] = []
    svc_enc = urllib.parse.quote(svc, safe="")
    for url in (
        f"{jaeger_base}/api/services/{svc_enc}/operations",
        f"{jaeger_base}/api/operations?service={svc_enc}",
    ):
        payload = fetch_json(url, timeout=8.0)
        data = payload.get("data") if isinstance(payload, dict) else None
        if isinstance(data, list):
            for item in data:
                if isinstance(item, str) and item:
                    ops.append(item)
                elif isinstance(item, dict) and item.get("name"):
                    ops.append(str(item["name"]))
            if ops:
                break
    return sorted(set(ops))


def merge_trace_map(dst: dict[str, dict], payload: object) -> None:
    if not isinstance(payload, dict):
        return
    data = payload.get("data")
    if not isinstance(data, list):
        return
    for tr in data:
        if not isinstance(tr, dict):
            continue
        tid = str(tr.get("traceID") or "")
        if not tid:
            continue
        spans = tr.get("spans") or []
        prev = dst.get(tid)
        if prev is None or len(spans) > len(prev.get("spans") or []):
            dst[tid] = tr


# 1. Jaeger: services + per-(service, operation) + slow (5ms/50ms) + error traces
jaeger_base = "http://127.0.0.1:9686"
services_payload = fetch_json(f"{jaeger_base}/api/services")
(out_dir / "jaeger" / "services.json").write_text(
    json.dumps(services_payload, ensure_ascii=False, indent=2),
    encoding="utf-8",
)
discovered_svcs = set(services_payload.get("data") or []) if isinstance(services_payload, dict) else set()
target_svcs = sorted({"bowl-client", "bowl-storage"} | (discovered_svcs - {"jaeger-all-in-one", "jaeger-query"}))

for svc in target_svcs:
    ops = discover_jaeger_operations(jaeger_base, svc)
    (out_dir / "jaeger" / f"operations_{svc}.json").write_text(
        json.dumps({"service": svc, "operations": ops}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    traces_by_id: dict[str, dict] = {}
    base_params = {
        "service": svc,
        "start": str(start_us),
        "end": str(end_us),
        "lookback": "custom",
    }
    # 全局分层抽样：常规窗口 + >=5ms 慢链路 + >=50ms 重尾慢链路
    for extra in (
        {"limit": "200"},
        {"limit": "200", "minDuration": "5ms"},
        {"limit": "200", "minDuration": "50ms"},
    ):
        q = urllib.parse.urlencode({**base_params, **extra})
        merge_trace_map(traces_by_id, fetch_json(f"{jaeger_base}/api/traces?{q}", timeout=15.0))

    # 按 operation 独立保底抓取（彻底防止高频 mdtest 元数据刷屏挤空 Write/Read/Fsync 数据面链路）
    for op in ops:
        for extra in (
            {"operation": op, "limit": "40"},
            {"operation": op, "limit": "40", "minDuration": "5ms"},
        ):
            q = urllib.parse.urlencode({**base_params, **extra})
            merge_trace_map(traces_by_id, fetch_json(f"{jaeger_base}/api/traces?{q}", timeout=12.0))

    (out_dir / "jaeger" / f"traces_{svc}.json").write_text(
        json.dumps({"data": list(traces_by_id.values())}, ensure_ascii=False),
        encoding="utf-8",
    )

    err_by_id: dict[str, dict] = {}
    for err_tag in ({"error": "true"}, {"otel.status_code": "ERROR"}):
        q_err = urllib.parse.urlencode({
            **base_params,
            "tags": json.dumps(err_tag),
            "limit": "100",
        })
        merge_trace_map(err_by_id, fetch_json(f"{jaeger_base}/api/traces?{q_err}", timeout=12.0))
    (out_dir / "jaeger" / f"errors_{svc}.json").write_text(
        json.dumps({"data": list(err_by_id.values())}, ensure_ascii=False),
        encoding="utf-8",
    )

# 2. Prometheus: targets (优先保留卸载前活跃抓取快照) + core range queries
prom_base = "http://127.0.0.1:9091"
live_targets_path = out_dir / "prometheus" / "targets_live.json"
post_targets = fetch_json(f"{prom_base}/api/v1/targets")
(out_dir / "prometheus" / "targets_post_umount.json").write_text(
    json.dumps(post_targets, ensure_ascii=False, indent=2),
    encoding="utf-8",
)
use_live = False
if live_targets_path.exists():
    try:
        live_obj = json.loads(live_targets_path.read_text(encoding="utf-8"))
        if (live_obj.get("data") or {}).get("activeTargets"):
            use_live = True
            (out_dir / "prometheus" / "targets.json").write_text(
                json.dumps(live_obj, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
    except Exception:
        use_live = False
if not use_live:
    (out_dir / "prometheus" / "targets.json").write_text(
        json.dumps(post_targets, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

prom_queries = {
    "rpc_server_qps_by_proc": 'sum by (rpc_method) (rate(rpc_server_call_duration_seconds_count[20s]))',
    "rpc_server_qps_by_proc_status": 'sum by (rpc_method, rpc_response_status_code) (rate(rpc_server_call_duration_seconds_count[20s]))',
    "rpc_server_p99_sec_by_proc": 'histogram_quantile(0.99, sum by (rpc_method, le) (rate(rpc_server_call_duration_seconds_bucket[20s])))',
    "rpc_server_p50_sec_by_proc": 'histogram_quantile(0.50, sum by (rpc_method, le) (rate(rpc_server_call_duration_seconds_bucket[20s])))',
    "http_server_qps_by_proc_status": 'sum by (procedure, status_class) (rate(bowl_storage_http_request_duration_seconds_count[20s]))',
    "http_server_p99_sec_by_proc": 'histogram_quantile(0.99, sum by (procedure, le) (rate(bowl_storage_http_request_duration_seconds_bucket[20s])))',
    "http_server_req_bytes_rate": 'sum by (procedure) (rate(bowl_storage_http_request_bytes_total[20s]))',
    "http_server_resp_bytes_rate": 'sum by (procedure) (rate(bowl_storage_http_response_bytes_total[20s]))',
    "rpc_client_qps_by_proc": 'sum by (rpc_method) (rate(rpc_client_call_duration_seconds_count[20s]))',
    "rpc_client_qps_by_proc_status": 'sum by (rpc_method, rpc_response_status_code) (rate(rpc_client_call_duration_seconds_count[20s]))',
    "rpc_client_p99_sec_by_proc": 'histogram_quantile(0.99, sum by (rpc_method, le) (rate(rpc_client_call_duration_seconds_bucket[20s])))',
    "rpc_client_logical_p99_sec_by_proc": 'histogram_quantile(0.99, sum by (procedure, le) (rate(bowl_client_rpc_logical_duration_seconds_bucket[20s])))',
    "actor_queue_wait_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_actor_queue_wait_seconds_bucket[20s])))',
    "actor_tick_batch_p50": 'histogram_quantile(0.50, sum by (le) (rate(bowl_storage_actor_tick_batch_size_bucket[20s])))',
    "actor_tick_batch_p99": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_actor_tick_batch_size_bucket[20s])))',
    "actor_phase_rate_sec_by_phase": 'sum by (phase) (rate(bowl_storage_actor_tick_phase_duration_seconds_sum[20s]))',
    "actor_phase_p99_sec_by_phase": 'histogram_quantile(0.99, sum by (phase, le) (rate(bowl_storage_actor_tick_phase_duration_seconds_bucket[20s])))',
    "read_op_p99_sec_by_op": 'histogram_quantile(0.99, sum by (op, le) (rate(bowl_storage_read_op_duration_seconds_bucket[20s])))',
    "kv_read_p99_sec_by_op": 'histogram_quantile(0.99, sum by (op, le) (rate(bowl_storage_kv_read_duration_seconds_bucket[20s])))',
    "blob_pread_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_blob_pread_duration_seconds_bucket[20s])))',
    "blob_pwrite_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_blob_pwrite_duration_seconds_bucket[20s])))',
    "blob_sync_p99_sec": 'histogram_quantile(0.99, sum by (target, trigger, le) (rate(bowl_storage_blob_sync_duration_seconds_bucket[20s])))',
    "blob_rmw_read_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_blob_rmw_read_duration_seconds_bucket[20s])))',
    "inode_lock_wait_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_inode_lock_wait_seconds_bucket[20s])))',
    "pebble_wal_fsync_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_storage_pebble_wal_fsync_duration_seconds_bucket[20s])))',
    "pebble_read_amp": 'bowl_storage_pebble_read_amp',
    "pebble_compaction_debt_bytes": 'bowl_storage_pebble_compaction_debt_bytes',
    "pebble_stall_events_total": 'bowl_storage_pebble_stall_events_total',
    "client_fuse_qps_by_op": 'sum by (op) (rate(bowl_client_fuse_ops_total[20s]))',
    "client_fuse_p99_sec_by_op": 'histogram_quantile(0.99, sum by (op, le) (rate(bowl_client_fuse_op_duration_seconds_bucket[20s])))',
    "client_wb_bytes_rate_by_mode": 'sum by (mode) (rate(bowl_client_write_behind_bytes_total[20s]))',
    "client_wb_backpressure_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_client_write_behind_backpressure_wait_duration_seconds_bucket[20s])))',
    "client_wb_barrier_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_client_write_behind_barrier_wait_duration_seconds_bucket[20s])))',
    "client_ra_wait_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_client_read_ahead_wait_duration_seconds_bucket[20s])))',
    "client_lock_wait_p99_sec": 'histogram_quantile(0.99, sum by (le) (rate(bowl_client_inodelock_wait_duration_seconds_bucket[20s])))',
    "client_fuse_waiting": 'max(bowl_client_fuse_connection_waiting)',
    "client_rpc_retries_total": 'sum by (procedure) (bowl_client_rpc_retries_total)',
    "process_cpu_cores_by_job": 'sum by (job) (rate(process_cpu_seconds_total[20s]))',
    "process_rss_bytes_by_job": 'sum by (job) (process_resident_memory_bytes)',
    "go_goroutines_by_job": 'sum by (job) (go_goroutines)',
}

range_results = {
    "window": {"start_epoch": win_start_s, "end_epoch": win_end_s, "step_sec": step_s},
    "queries": {},
}
for key, expr in prom_queries.items():
    params = urllib.parse.urlencode({
        "query": expr,
        "start": str(win_start_s),
        "end": str(win_end_s),
        "step": f"{step_s}s",
    })
    range_results["queries"][key] = {
        "expr": expr,
        "response": fetch_json(f"{prom_base}/api/v1/query_range?{params}", timeout=15.0),
    }

(out_dir / "prometheus" / "query_range.json").write_text(
    json.dumps(range_results, ensure_ascii=False),
    encoding="utf-8",
)

# 3. Loki: 分页拉取完整测试窗口业务容器日志，过滤 loki/prometheus/jaeger/grafana/alloy 自身噪音
loki_base = "http://127.0.0.1:9310"
loki_expr = '{service=~".+", service!~"loki|prometheus|jaeger|grafana|alloy"}'
streams_merged: dict[str, dict] = {}
seen_entries: set[tuple[str, str, str]] = set()
cur_start_ns = start_ns
page_limit = 2000
pages_fetched = 0
last_error = None

for _ in range(50):
    if cur_start_ns > end_ns:
        break
    loki_params = urllib.parse.urlencode({
        "query": loki_expr,
        "start": str(cur_start_ns),
        "end": str(end_ns),
        "limit": str(page_limit),
        "direction": "forward",
    })
    resp = fetch_json(f"{loki_base}/loki/api/v1/query_range?{loki_params}", timeout=15.0)
    if not isinstance(resp, dict) or "_fetch_error" in resp:
        last_error = resp
        break
    pages_fetched += 1
    result_list = (resp.get("data") or {}).get("result") or []
    batch_count = 0
    max_ts_ns = cur_start_ns
    for stream_item in result_list:
        labels = stream_item.get("stream") or {}
        svc_name = str(labels.get("service") or labels.get("container") or "")
        if svc_name in {"loki", "prometheus", "jaeger", "grafana", "alloy"}:
            continue
        label_key = json.dumps(labels, sort_keys=True, ensure_ascii=False)
        bucket = streams_merged.setdefault(label_key, {"stream": labels, "values": []})
        for pair in (stream_item.get("values") or []):
            if not isinstance(pair, list) or len(pair) < 2:
                continue
            ts_str, line_str = str(pair[0]), str(pair[1])
            batch_count += 1
            try:
                ts_int = int(ts_str)
                if ts_int > max_ts_ns:
                    max_ts_ns = ts_int
            except ValueError:
                pass
            dedup_key = (label_key, ts_str, line_str)
            if dedup_key not in seen_entries:
                seen_entries.add(dedup_key)
                bucket["values"].append([ts_str, line_str])
    if batch_count < page_limit or max_ts_ns < cur_start_ns:
        break
    cur_start_ns = max_ts_ns + 1

loki_out = {
    "status": "success" if (pages_fetched > 0 or last_error is None) else "error",
    "query": loki_expr,
    "pages_fetched": pages_fetched,
    "data": {
        "resultType": "streams",
        "result": list(streams_merged.values()),
    },
}
if last_error is not None and pages_fetched == 0:
    loki_out["_fetch_error"] = last_error.get("_fetch_error", str(last_error))

(out_dir / "loki" / "query_range.json").write_text(
    json.dumps(loki_out, ensure_ascii=False),
    encoding="utf-8",
)
PY_EXPORT

    # 仅在拉取结果非空时更新 final.prom，防止 storage-service 已停止时用 0 B 覆盖卸载前快照
    fetch_nonempty "http://127.0.0.1:9090/metrics" "\${out_dir}/metrics/final.prom" 5 || true
    cat /proc/diskstats > "\${out_dir}/diskstats/final.txt" 2>/dev/null || true
    { free -m; df -h; docker ps -a; } > "\${out_dir}/logs/server-state.log" 2>/dev/null || true
    dmesg -T 2>/dev/null | tail -n 200 > "\${out_dir}/logs/kernel-dmesg.log" || true
    COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" logs --no-color storage-service postgres migration > "\${out_dir}/logs/compose-prod.log" 2>&1 || \
      COMPOSE_PROJECT_NAME=bowl docker compose "\${COMPOSE_ARGS[@]}" logs --no-color > "\${out_dir}/logs/compose-prod.log" 2>&1 || true

    vol_mp="\$(docker volume inspect bowl_storage_data -f '{{.Mountpoint}}' 2>/dev/null || true)"
    if [[ -n "\${vol_mp}" && -d "\${vol_mp}" ]]; then
      (cd "\${vol_mp}" && find . \( -name 'LOG*' -o -name 'pebble_events.log*' \) -exec cp --parents -a {} "\${out_dir}/pebble/" \;) 2>/dev/null || true
    fi

    date -u +%Y-%m-%dT%H:%M:%SZ > "\${out_dir}/.exported"
    ;;

  *)
    echo "Usage: bowl-prod-ctl {up|start-storage|stop-storage|snapshot-targets [out_dir]|stop|down-volumes|pg-version|export-telemetry [out_dir] [start_epoch] [end_epoch]}" >&2
    exit 2
    ;;
esac
PROD_CTL
chmod +x /usr/local/bin/bowl-prod-ctl
EOF_PROD_SETUP
}

# emit_client_telemetry_helper
# 输出安装到 Client 节点 /opt/bowl/client_telemetry.sh 的遥测快照与 pprof 助手脚本内容。
emit_client_telemetry_helper() {
  cat <<'EOF_CLI_HELPER'
sysctl -qw net.ipv4.tcp_autocorking=0 2>/dev/null || true
mkdir -p /tmp/bowl-telemetry/client/logs /tmp/bowl-telemetry/client/metrics /tmp/bowl-telemetry/client/stats /tmp/bowl-telemetry/client/diskstats /tmp/bowl-telemetry/client/pprof
BOWL_PROFILE_PIDS=()

bowl_fetch_nonempty() {
  local url="$1"
  local dest="$2"
  local max_t="${3:-5}"
  local tmp="${dest}.tmp.$$"
  if curl -fsS --max-time "${max_t}" "${url}" > "${tmp}" 2>/dev/null && [[ -s "${tmp}" ]]; then
    mv "${tmp}" "${dest}"
    return 0
  fi
  rm -f "${tmp}"
  return 1
}

capture_bowl_snapshot() {
  local phase="$1"
  date +%s.%N > "/tmp/bowl-telemetry/client/stats/${phase}.epoch" 2>/dev/null || date +%s > "/tmp/bowl-telemetry/client/stats/${phase}.epoch"
  bowl_fetch_nonempty "http://127.0.0.1:9567/metrics" "/tmp/bowl-telemetry/client/metrics/${phase}.prom" 5 || true
  bowl_fetch_nonempty "http://127.0.0.1:9567/debug/stats?format=json" "/tmp/bowl-telemetry/client/stats/${phase}.json" 5 || true
  bowl_fetch_nonempty "http://127.0.0.1:9567/debug/stats?format=text" "/tmp/bowl-telemetry/client/stats/${phase}.txt" 5 || true
  cat /proc/diskstats > "/tmp/bowl-telemetry/client/diskstats/${phase}.txt" 2>/dev/null || true
  if [[ ${#SSH_SRV[@]} -gt 0 ]]; then
    "${SSH_SRV[@]}" "mkdir -p /tmp/bowl-telemetry/server/metrics /tmp/bowl-telemetry/server/diskstats /tmp/bowl-telemetry/server/pprof /tmp/bowl-telemetry/server/pebble && tmp_p='/tmp/bowl-telemetry/server/metrics/${phase}.prom.tmp.\$\$'; if curl -fsS --max-time 5 http://127.0.0.1:9090/metrics > \"\${tmp_p}\" 2>/dev/null && [[ -s \"\${tmp_p}\" ]]; then mv \"\${tmp_p}\" '/tmp/bowl-telemetry/server/metrics/${phase}.prom'; else rm -f \"\${tmp_p}\"; fi; cat /proc/diskstats > '/tmp/bowl-telemetry/server/diskstats/${phase}.txt' 2>/dev/null || true" || true
  fi
}

# 在卸载前采集完整双端 final 快照与 Prometheus 活跃 targets，卸载并等待守护进程刷盘退出后再完整收割 bowl-mount.log
harvest_bowl_before_and_after_umount() {
  local mp="${1:-/mnt/bowl}"
  capture_bowl_snapshot "before_umount"
  [[ -s /tmp/bowl-telemetry/client/metrics/before_umount.prom ]] && cp -a /tmp/bowl-telemetry/client/metrics/before_umount.prom /tmp/bowl-telemetry/client/metrics/final.prom || true
  [[ -s /tmp/bowl-telemetry/client/stats/before_umount.json ]] && cp -a /tmp/bowl-telemetry/client/stats/before_umount.json /tmp/bowl-telemetry/client/stats/final.json || true
  [[ -s /tmp/bowl-telemetry/client/stats/before_umount.txt ]] && cp -a /tmp/bowl-telemetry/client/stats/before_umount.txt /tmp/bowl-telemetry/client/stats/final.txt || true
  [[ -s /tmp/bowl-telemetry/client/diskstats/before_umount.txt ]] && cp -a /tmp/bowl-telemetry/client/diskstats/before_umount.txt /tmp/bowl-telemetry/client/diskstats/final.txt || true
  if [[ ${#SSH_SRV[@]} -gt 0 ]]; then
    "${SSH_SRV[@]}" "/usr/local/bin/bowl-prod-ctl snapshot-targets /tmp/bowl-telemetry/server" || true
  fi

  /usr/local/bin/bowl umount "${mp}" || fusermount3 -u "${mp}" 2>/dev/null || umount "${mp}" 2>/dev/null || umount -l "${mp}" 2>/dev/null || true
  local _i
  for ((_i=1; _i<=50; _i++)); do
    if ! pgrep -f "bowl mount .* ${mp}" >/dev/null 2>&1; then
      break
    fi
    sleep 0.2
  done
  sync
  rm -f /tmp/bowl-telemetry/client/logs/bowl-mount-phase.log
  if [[ -f /var/log/bowl-mount.log ]]; then
    cp -a /var/log/bowl-mount.log /tmp/bowl-telemetry/client/logs/bowl-mount.log || true
  fi
}

start_bowl_cpu_profile() {
  local stage="$1"
  local sec="${2:-10}"
  local delay="${3:-0}"
  BOWL_PROFILE_PIDS=()
  ( [[ "${delay}" != "0" ]] && sleep "${delay}"; curl -fsS --max-time 30 "http://127.0.0.1:9567/debug/pprof/profile?seconds=${sec}" -o "/tmp/bowl-telemetry/client/pprof/${stage}_cpu.pb.gz" 2>/dev/null || true ) &
  BOWL_PROFILE_PIDS+=("$!")
  if [[ ${#SSH_SRV[@]} -gt 0 ]]; then
    ( [[ "${delay}" != "0" ]] && sleep "${delay}"; "${SSH_SRV[@]}" "mkdir -p /tmp/bowl-telemetry/server/pprof && curl -fsS --max-time 30 'http://127.0.0.1:9090/debug/pprof/profile?seconds=${sec}' -o '/tmp/bowl-telemetry/server/pprof/${stage}_cpu.pb.gz' 2>/dev/null || true" ) &
    BOWL_PROFILE_PIDS+=("$!")
  fi
}

finish_bowl_stage_profiles() {
  local stage="$1"
  local pid
  if [[ ${#BOWL_PROFILE_PIDS[@]} -gt 0 ]]; then
    for pid in "${BOWL_PROFILE_PIDS[@]}"; do
      [[ -n "${pid}" ]] && wait "${pid}" 2>/dev/null || true
    done
  fi
  BOWL_PROFILE_PIDS=()
  curl -fsS --max-time 10 "http://127.0.0.1:9567/debug/pprof/heap" -o "/tmp/bowl-telemetry/client/pprof/${stage}_heap.pb.gz" 2>/dev/null || true
  curl -fsS --max-time 10 "http://127.0.0.1:9567/debug/pprof/mutex" -o "/tmp/bowl-telemetry/client/pprof/${stage}_mutex.pb.gz" 2>/dev/null || true
  if [[ ${#SSH_SRV[@]} -gt 0 ]]; then
    "${SSH_SRV[@]}" "mkdir -p /tmp/bowl-telemetry/server/pprof && curl -fsS --max-time 10 'http://127.0.0.1:9090/debug/pprof/heap' -o '/tmp/bowl-telemetry/server/pprof/${stage}_heap.pb.gz' 2>/dev/null || true; curl -fsS --max-time 10 'http://127.0.0.1:9090/debug/pprof/mutex' -o '/tmp/bowl-telemetry/server/pprof/${stage}_mutex.pb.gz' 2>/dev/null || true" || true
  fi
}
EOF_CLI_HELPER
}

# collect_and_process_bench_telemetry <repo_root> <work_dir> <results_dir> <suite_name> <start_epoch> <end_epoch>
# 调用者需在环境中提供 CLIENT_SSH_CMD_ARR 与 SERVER_SSH_CMD_ARR 数组（完整 ssh 前缀，如 ssh -i key ... root@host）。
collect_and_process_bench_telemetry() {
  local repo_root="$1"
  local work_dir="$2"
  local results_dir="$3"
  local suite_name="$4"
  local start_epoch="${5:-}"
  local end_epoch="${6:-}"

  local stage_dir="${work_dir}/telemetry-stage"
  local raw_archive="${results_dir}/telemetry-raw.tar.gz"
  rm -rf "${stage_dir}" "${raw_archive}" "${results_dir}/telemetry"
  mkdir -p "${stage_dir}"

  if [[ ${#CLIENT_SSH_CMD_ARR[@]} -gt 0 ]]; then
    "${CLIENT_SSH_CMD_ARR[@]}" "bash -s" <<'EOF_CLI_FINAL' >/dev/null 2>&1 || true
mkdir -p /tmp/bowl-telemetry/client/logs /tmp/bowl-telemetry/client/metrics /tmp/bowl-telemetry/client/stats /tmp/bowl-telemetry/client/diskstats /tmp/bowl-telemetry/client/pprof
[[ -f /var/log/bowl-mount.log ]] && cp -a /var/log/bowl-mount.log /tmp/bowl-telemetry/client/logs/bowl-mount.log || true
fetch_if_live() {
  local url="$1"
  local dest="$2"
  local tmp="${dest}.tmp.$$"
  if curl -fsS --max-time 3 "${url}" > "${tmp}" 2>/dev/null && [[ -s "${tmp}" ]]; then
    mv "${tmp}" "${dest}"
  else
    rm -f "${tmp}"
  fi
}
fetch_if_live "http://127.0.0.1:9567/metrics" "/tmp/bowl-telemetry/client/metrics/final.prom"
fetch_if_live "http://127.0.0.1:9567/debug/stats?format=json" "/tmp/bowl-telemetry/client/stats/final.json"
fetch_if_live "http://127.0.0.1:9567/debug/stats?format=text" "/tmp/bowl-telemetry/client/stats/final.txt"
[[ ! -s /tmp/bowl-telemetry/client/diskstats/final.txt ]] && cat /proc/diskstats > /tmp/bowl-telemetry/client/diskstats/final.txt 2>/dev/null || true
dmesg -T 2>/dev/null | tail -n 200 > /tmp/bowl-telemetry/client/logs/dmesg-tail.log || true
EOF_CLI_FINAL
    "${CLIENT_SSH_CMD_ARR[@]}" "tar -czf - -C /tmp/bowl-telemetry client" 2>/dev/null | tar -xzf - -C "${stage_dir}" 2>/dev/null || true
  fi

  if [[ ${#SERVER_SSH_CMD_ARR[@]} -gt 0 ]]; then
    "${SERVER_SSH_CMD_ARR[@]}" "START_E='${start_epoch}' END_E='${end_epoch}' bash -s" <<'EOF_SRV_FINAL' >/dev/null 2>&1 || true
mkdir -p /tmp/bowl-telemetry/server
if [[ ! -f /tmp/bowl-telemetry/server/.exported ]] && [[ -x /usr/local/bin/bowl-prod-ctl ]]; then
  /usr/local/bin/bowl-prod-ctl export-telemetry /tmp/bowl-telemetry/server "${START_E}" "${END_E}" || true
fi
EOF_SRV_FINAL
    "${SERVER_SSH_CMD_ARR[@]}" "tar -czf - -C /tmp/bowl-telemetry server" 2>/dev/null | tar -xzf - -C "${stage_dir}" 2>/dev/null || true
  fi

  if [[ -n "$(find "${stage_dir}" -type f -print -quit 2>/dev/null)" ]]; then
    COPYFILE_DISABLE=1 tar -czf "${raw_archive}" -C "${stage_dir}" . 2>/dev/null || true
    PYTHONDONTWRITEBYTECODE=1 python3 "${repo_root}/benchmark/common/collect_telemetry.py" \
      --suite "${suite_name}" \
      --stage-dir "${stage_dir}" \
      --results-dir "${results_dir}" \
      --server-bin "${work_dir}/storage-service" \
      --client-bin "${work_dir}/bowl" || true
  fi
}
