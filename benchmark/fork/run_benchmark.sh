#!/usr/bin/env bash
# ==============================================================================
# Bowl Disk Fork — 阿里云硅谷 (us-west-1) 双机内网分叉性能与一致性基准测试套件
# ==============================================================================
# 拓扑结构:
#   - Node 1 (Server): ecs.i5.xlarge (4 vCPU, 32 GiB, 894 GiB local_ssd_pro NVMe SSD)
#       运行 Bowl `deploy/prod` Docker Compose 栈 (PostgreSQL 18 + Storage :9000 + Jaeger + Prometheus + Loki + Alloy + Grafana)
#   - Node 2 (Client): ecs.i5.xlarge (4 vCPU, 32 GiB, 894 GiB local_ssd_pro NVMe SSD)
#       通过同可用区 VPC 私网挂载 Bowl FUSE 卷并依次执行:
#         - Suite 1: ComputeSDK 快照/分叉等价矩阵 + POSIX 扩展预设 (`bowl_fork` N=20 vs `bowl_copy` 同机全量拷贝基线)
#         - Suite 2: 并发持续写入（4 MiB 流 + 4 KiB 小文件 + 16×1 MiB 未关闭孤儿句柄）下连续 10 次无停顿在线分叉与孤儿清洗核验
#         - Suite 3: 1 GiB 父盘 `1 -> 16` 多分支扇出、并发读写 CoW 隔离及父/兄销毁后血统提交零残留自动回收核验
# ==============================================================================

set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"
REPORT_FILE="${REPORT_FILE:-${SCRIPT_DIR}/REPORT.md}"
SUMMARY_JSON="${RESULTS_DIR}/summary.json"
# shellcheck source=../common/prod_stack.sh
source "${REPO_ROOT}/benchmark/common/prod_stack.sh"

REGION_ID="${REGION_ID:-${REGION:-us-west-1}}"
ZONE_ID="${ZONE_ID:-${ZONE:-us-west-1a}}"
VPC_ID="${VPC_ID:?VPC_ID is required}"
VSWITCH_ID="${VSWITCH_ID:?VSWITCH_ID is required}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:?SECURITY_GROUP_ID is required}"
INSTANCE_TYPE="${INSTANCE_TYPE:-ecs.i5.xlarge}"
IMAGE_ID="${IMAGE_ID:-ubuntu_24_04_x64_20G_alibase_20261016.vhd}"
SYSTEM_DISK_CATEGORY="${SYSTEM_DISK_CATEGORY:-cloud_essd}"
SYSTEM_DISK_SIZE="${SYSTEM_DISK_SIZE:-40}"
KEEP_INSTANCES="${KEEP_INSTANCES:-0}"

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
KEY_PAIR_NAME="bowl-forkbench-kp-${RUN_TS}"
WORK_DIR="$(mktemp -d "/tmp/bowl-forkbench-${RUN_TS}-XXXXXX")"
KEY_FILE="${WORK_DIR}/id_rsa"

SERVER_INSTANCE_ID=""
CLIENT_INSTANCE_ID=""
INSTANCES_CREATED_AT=""
INSTANCES_DELETED_AT=""
TELEMETRY_FETCHED=0
TEST_START_EPOCH=""
TEST_END_EPOCH=""

SSH_OPTS=(
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o ConnectTimeout=10
  -o ServerAliveInterval=15
  -o ServerAliveCountMax=6
  -o LogLevel=ERROR
)

log() {
  printf '[%s] %s\n' "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" "$*"
}

fetch_telemetry_archive() {
  if [[ "${TELEMETRY_FETCHED:-0}" == "1" ]]; then
    return 0
  fi
  TELEMETRY_FETCHED=1

  local old_opts
  old_opts="$(set +o)"
  set +e

  CLIENT_SSH_CMD_ARR=()
  SERVER_SSH_CMD_ARR=()
  if [[ -n "${CLIENT_PUB_IP:-}" && -f "${KEY_FILE}" ]]; then
    CLIENT_SSH_CMD_ARR=(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}")
  fi
  if [[ -n "${SERVER_PUB_IP:-}" && -f "${KEY_FILE}" ]]; then
    SERVER_SSH_CMD_ARR=(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}")
  fi

  log "Collecting and distilling deploy/prod telemetry into ${RESULTS_DIR}/telemetry/ ..."
  collect_and_process_bench_telemetry \
    "${REPO_ROOT}" \
    "${WORK_DIR}" \
    "${RESULTS_DIR}" \
    "fork" \
    "${TEST_START_EPOCH:-}" \
    "${TEST_END_EPOCH:-}"

  eval "${old_opts}"
  return 0
}

cleanup() {
  local exit_code=$?
  set +e
  log "=== Triggering Cloud Resource Cleanup Trap (exit_code=${exit_code}) ==="

  if [[ "${exit_code}" != "0" && -n "${CLIENT_PUB_IP:-}" && -f "${KEY_FILE}" ]]; then
    scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}:/tmp/bench_results/*" "${RESULTS_DIR}/" 2>/dev/null
  fi

  fetch_telemetry_archive

  if [[ "${KEEP_INSTANCES}" == "1" ]]; then
    log "KEEP_INSTANCES=1 is set; skipping automatic deletion of ECS instances and keypair."
    log "Temporary workdir preserved at: ${WORK_DIR}"
    exit "${exit_code}"
  fi

  local instance_ids=()
  [[ -n "${SERVER_INSTANCE_ID}" ]] && instance_ids+=("${SERVER_INSTANCE_ID}")
  [[ -n "${CLIENT_INSTANCE_ID}" ]] && instance_ids+=("${CLIENT_INSTANCE_ID}")

  if [[ ${#instance_ids[@]} -gt 0 ]]; then
    log "Deleting temporary ECS instances in ${REGION_ID}: ${instance_ids[*]}"
    local del_args=(ecs DeleteInstances --RegionId "${REGION_ID}" --Force true)
    local idx=1
    for id in "${instance_ids[@]}"; do
      del_args+=("--InstanceId.${idx}" "${id}")
      idx=$((idx + 1))
    done

    for attempt in 1 2 3 4 5 6; do
      if aliyun "${del_args[@]}" >/dev/null 2>&1; then
        INSTANCES_DELETED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
        log "DeleteInstances succeeded on attempt ${attempt}."
        break
      fi
      log "DeleteInstances attempt ${attempt} failed (instances may still be initializing); retrying in 5s..."
      sleep 5
    done
  fi

  if [[ -n "${KEY_PAIR_NAME}" ]]; then
    log "Deleting temporary SSH KeyPair: ${KEY_PAIR_NAME}"
    aliyun ecs DeleteKeyPairs \
      --RegionId "${REGION_ID}" \
      --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null 2>&1 || true
  fi

  rm -rf "${WORK_DIR}"
  log "=== Cloud Resource Cleanup Finished ==="
  exit "${exit_code}"
}

trap cleanup EXIT INT TERM

mkdir -p "${RESULTS_DIR}"
rm -f "${RESULTS_DIR}/stage_timings.jsonl"

record_stage() {
  local stage_name="$1"
  local duration_sec="$2"
  jq -n -c \
    --arg stage "${stage_name}" \
    --argjson duration_sec "${duration_sec}" \
    --arg ts "$(date -u +'%Y-%m-%dT%H:%M:%SZ')" \
    '{stage: $stage, duration_sec: $duration_sec, timestamp: $ts}' >> "${RESULTS_DIR}/stage_timings.jsonl"
}

# ------------------------------------------------------------------------------
# Step 1: 交叉编译 Bowl 服务端、客户端与数据库迁移二进制 (linux/amd64)
# ------------------------------------------------------------------------------
STAGE_START=$(date +%s)
log "Cross-compiling Bowl binaries (storage-service, bowl, goose) for linux/amd64..."
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/services/storage" build -mod=mod -trimpath -o "${WORK_DIR}/storage-service" ./cmd

GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/client" build -mod=mod -trimpath -o "${WORK_DIR}/bowl" ./cmd/bowl

GOOSE_CACHE_BIN="/tmp/bowl-bench-goose-linux-amd64"
if [[ ! -x "${GOOSE_CACHE_BIN}" ]]; then
  GOOSE_TMP_GOPATH="${WORK_DIR}/gopath"
  GOWORK=off GOPATH="${GOOSE_TMP_GOPATH}" GOMODCACHE="$(go env GOMODCACHE)" CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go install -trimpath -ldflags="-w -s" github.com/pressly/goose/v3/cmd/goose@v3.28.0
  if [[ -f "${GOOSE_TMP_GOPATH}/bin/linux_amd64/goose" ]]; then
    cp "${GOOSE_TMP_GOPATH}/bin/linux_amd64/goose" "${GOOSE_CACHE_BIN}"
  else
    cp "${GOOSE_TMP_GOPATH}/bin/goose" "${GOOSE_CACHE_BIN}"
  fi
  chmod +x "${GOOSE_CACHE_BIN}"
  chmod -R u+w "${GOOSE_TMP_GOPATH}" 2>/dev/null || true
  rm -rf "${GOOSE_TMP_GOPATH}"
fi
cp "${GOOSE_CACHE_BIN}" "${WORK_DIR}/goose"
chmod +x "${WORK_DIR}/storage-service" "${WORK_DIR}/bowl" "${WORK_DIR}/goose"

BOWL_GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse --short HEAD 2>/dev/null || echo 'unknown')"
if [[ "${BOWL_GIT_COMMIT}" != "unknown" && -n "$(git -C "${REPO_ROOT}" status --porcelain -- services client 2>/dev/null)" ]]; then
  BOWL_GIT_COMMIT="${BOWL_GIT_COMMIT}-dirty"
fi

cp "${SCRIPT_DIR}/fork_bench.py" "${WORK_DIR}/fork_bench.py"
prepare_prod_bundle "${REPO_ROOT}" "${WORK_DIR}"
COPYFILE_DISABLE=1 tar -czf "${WORK_DIR}/client-bins.tar.gz" -C "${WORK_DIR}" bowl fork_bench.py

STAGE_END=$(date +%s)
record_stage "cross_compile_binaries" "$((STAGE_END - STAGE_START))"
log "Binaries compiled & packaged (Bowl=${BOWL_GIT_COMMIT})."

# ------------------------------------------------------------------------------
# Step 2: 创建临时 SSH 密钥对并申请 2 台按量计费 ECS 实例 (Server + Client)
# ------------------------------------------------------------------------------
STAGE_START=$(date +%s)
log "Creating temporary SSH KeyPair ${KEY_PAIR_NAME} in ${REGION_ID}..."
aliyun ecs CreateKeyPair \
  --RegionId "${REGION_ID}" \
  --KeyPairName "${KEY_PAIR_NAME}" \
  | jq -r '.PrivateKeyBody' > "${KEY_FILE}"
chmod 600 "${KEY_FILE}"

log "Launching 2 PostPaid ECS instances (${INSTANCE_TYPE}, ${IMAGE_ID}, ${SYSTEM_DISK_SIZE}GiB ${SYSTEM_DISK_CATEGORY}) in ${ZONE_ID}..."
RUN_RESP="$(aliyun ecs RunInstances \
  --RegionId "${REGION_ID}" \
  --ZoneId "${ZONE_ID}" \
  --ImageId "${IMAGE_ID}" \
  --InstanceType "${INSTANCE_TYPE}" \
  --SecurityGroupId "${SECURITY_GROUP_ID}" \
  --VSwitchId "${VSWITCH_ID}" \
  --InstanceChargeType PostPaid \
  --SpotStrategy NoSpot \
  --InternetChargeType PayByTraffic \
  --InternetMaxBandwidthOut 100 \
  --SystemDisk.Category "${SYSTEM_DISK_CATEGORY}" \
  --SystemDisk.Size "${SYSTEM_DISK_SIZE}" \
  --KeyPairName "${KEY_PAIR_NAME}" \
  --Amount 2 \
  --InstanceName "bowl-forkbench" \
  --UniqueSuffix true)"

SERVER_INSTANCE_ID="$(echo "${RUN_RESP}" | jq -r '.InstanceIdSets.InstanceIdSet[0]')"
CLIENT_INSTANCE_ID="$(echo "${RUN_RESP}" | jq -r '.InstanceIdSets.InstanceIdSet[1]')"
INSTANCES_CREATED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
log "Created Server=${SERVER_INSTANCE_ID}, Client=${CLIENT_INSTANCE_ID}"

wait_for_instances_running() {
  local ids_json
  ids_json=$(printf '%s\n' "${SERVER_INSTANCE_ID}" "${CLIENT_INSTANCE_ID}" | jq -R . | jq -s -c .)
  log "Waiting for instances ${ids_json} to reach Running status..."
  for _ in $(seq 1 60); do
    local info
    info=$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${ids_json}")
    local running_count
    running_count=$(echo "${info}" | jq '[.Instances.Instance[] | select(.Status == "Running")] | length')
    if [[ "${running_count}" -eq 2 ]]; then
      echo "${info}" > "${WORK_DIR}/instances_info.json"
      return 0
    fi
    sleep 5
  done
  log "ERROR: Timed out waiting for ECS instances to enter Running state."
  return 1
}

wait_for_instances_running

SERVER_PUB_IP=$(jq -r --arg id "${SERVER_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .PublicIpAddress.IpAddress[0]' "${WORK_DIR}/instances_info.json")
SERVER_PRIV_IP=$(jq -r --arg id "${SERVER_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .VpcAttributes.PrivateIpAddress.IpAddress[0]' "${WORK_DIR}/instances_info.json")
CLIENT_PUB_IP=$(jq -r --arg id "${CLIENT_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .PublicIpAddress.IpAddress[0]' "${WORK_DIR}/instances_info.json")
CLIENT_PRIV_IP=$(jq -r --arg id "${CLIENT_INSTANCE_ID}" '.Instances.Instance[] | select(.InstanceId == $id) | .VpcAttributes.PrivateIpAddress.IpAddress[0]' "${WORK_DIR}/instances_info.json")

log "Server Node: Public=${SERVER_PUB_IP}, Private=${SERVER_PRIV_IP}"
log "Client Node: Public=${CLIENT_PUB_IP}, Private=${CLIENT_PRIV_IP}"

wait_for_ssh() {
  local host="$1"
  local label="$2"
  log "Waiting for SSH readiness on ${label} (${host})..."
  for _ in $(seq 1 40); do
    if ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${host}" "echo ready" >/dev/null 2>&1; then
      log "SSH is ready on ${label} (${host})."
      return 0
    fi
    sleep 3
  done
  log "ERROR: Timed out waiting for SSH on ${label} (${host})."
  return 1
}

wait_for_ssh "${SERVER_PUB_IP}" "Server"
wait_for_ssh "${CLIENT_PUB_IP}" "Client"

ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "mkdir -p /root/.ssh && chmod 700 /root/.ssh"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" "${KEY_FILE}" "root@${CLIENT_PUB_IP}:/root/.ssh/id_rsa"
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "chmod 600 /root/.ssh/id_rsa"

STAGE_END=$(date +%s)
record_stage "provision_ecs_instances" "$((STAGE_END - STAGE_START))"

# ------------------------------------------------------------------------------
# Step 3: 部署 Server 节点（本地 NVMe 挂载 + fio 基线 + deploy/prod 容器栈）
# ------------------------------------------------------------------------------
STAGE_START=$(date +%s)
log "Uploading prod-bundle.tar.gz to Server (${SERVER_PUB_IP})..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "mkdir -p /opt/bowl"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" \
  "${WORK_DIR}/prod-bundle.tar.gz" \
  "root@${SERVER_PUB_IP}:/tmp/prod-bundle.tar.gz"

log "Formatting local NVMe SSD and measuring Server hardware baseline..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "bash -s" <<'EOF_SERVER_BASE'
set -euo pipefail

ROOT_PART="$(findmnt -n -o SOURCE /)"
ROOT_DISK="/dev/$(lsblk -ndo pkname "${ROOT_PART}" 2>/dev/null || basename "${ROOT_PART}")"
NVME_DEV="$(lsblk -dbpno NAME,SIZE,TYPE | awk -v root="${ROOT_DISK}" '$3 == "disk" && $1 != root {print $2, $1}' | sort -rn | head -n1 | awk '{print $2}')"
if [[ -z "${NVME_DEV}" ]]; then
  echo "ERROR: Local NVMe SSD device not found on Server!" >&2
  lsblk >&2
  exit 1
fi
echo "${NVME_DEV}" > /etc/bowl_nvme_dev
mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "${NVME_DEV}" >/dev/null
mkdir -p /mnt/nvme
mount -o noatime,nodiratime "${NVME_DEV}" /mnt/nvme

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq iperf3 fio python3 jq curl ca-certificates >/dev/null

mkdir -p /mnt/nvme/disk_bench
fio --name=nvme_seq_write --directory=/mnt/nvme/disk_bench --rw=write --bs=4M --size=2G --numjobs=1 --direct=1 --end_fsync=1 --group_reporting --output-format=json > /tmp/server_disk_write.json
fio --name=nvme_seq_read --directory=/mnt/nvme/disk_bench --rw=read --bs=4M --size=2G --numjobs=1 --direct=1 --group_reporting --output-format=json > /tmp/server_disk_read.json
fio --name=nvme_rand_write --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randwrite --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json > /tmp/server_disk_randw.json
fio --name=nvme_rand_read --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randread --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json > /tmp/server_disk_randr.json
fio --name=nvme_fsync_write --directory=/mnt/nvme/disk_bench --ioengine=psync --rw=write --bs=4k --size=64M --numjobs=1 --direct=1 --fsync=1 --runtime=5 --time_based=1 --group_reporting --output-format=json > /tmp/server_disk_fsync.json
rm -rf /mnt/nvme/disk_bench

iperf3 -s -D
EOF_SERVER_BASE

log "Deploying Bowl deploy/prod Docker Compose stack on Server..."
remote_setup_prod_stack "${SERVER_PRIV_IP}" "${CLIENT_PRIV_IP}" "prod" \
  | ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "bash -s"
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "/usr/local/bin/bowl-prod-ctl up"

STAGE_END=$(date +%s)
record_stage "deploy_server_node" "$((STAGE_END - STAGE_START))"

# ------------------------------------------------------------------------------
# Step 4: 部署 Client 节点（本地 NVMe + 遥测助手 + 主锚点 FUSE 挂载 /mnt/bowl）
# ------------------------------------------------------------------------------
STAGE_START=$(date +%s)
log "Uploading client-bins.tar.gz and telemetry helper to Client (${CLIENT_PUB_IP})..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "mkdir -p /opt/bowl /opt/bench /mnt/bowl /tmp/bench_results"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" \
  "${WORK_DIR}/client-bins.tar.gz" \
  "root@${CLIENT_PUB_IP}:/tmp/client-bins.tar.gz"
emit_client_telemetry_helper \
  | ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "cat >/opt/bowl/client_telemetry.sh && chmod +x /opt/bowl/client_telemetry.sh"

ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" \
  "SERVER_PRIV_IP='${SERVER_PRIV_IP}' BOWL_BENCH_LOG_LEVEL='${BOWL_BENCH_LOG_LEVEL}' BOWL_BENCH_TRACE_SAMPLE_RATIO='${BOWL_BENCH_TRACE_SAMPLE_RATIO}' bash -s" <<'EOF_CLIENT_SETUP'
set -euo pipefail

ROOT_PART="$(findmnt -n -o SOURCE /)"
ROOT_DISK="/dev/$(lsblk -ndo pkname "${ROOT_PART}" 2>/dev/null || basename "${ROOT_PART}")"
NVME_DEV="$(lsblk -dbpno NAME,SIZE,TYPE | awk -v root="${ROOT_DISK}" '$3 == "disk" && $1 != root {print $2, $1}' | sort -rn | head -n1 | awk '{print $2}')"
if [[ -z "${NVME_DEV}" ]]; then
  echo "ERROR: Local NVMe SSD device not found on Client!" >&2
  lsblk >&2
  exit 1
fi
echo "${NVME_DEV}" > /etc/bowl_nvme_dev
mkfs.ext4 -F -E lazy_itable_init=0,lazy_journal_init=0 "${NVME_DEV}" >/dev/null
mkdir -p /mnt/nvme
mount -o noatime,nodiratime "${NVME_DEV}" /mnt/nvme
mkdir -p /mnt/nvme/fork_work /mnt/nvme/bench_tmp

tar -xzf /tmp/client-bins.tar.gz -C /tmp/
mv /tmp/bowl /usr/local/bin/bowl
mv /tmp/fork_bench.py /opt/bench/fork_bench.py
chmod +x /usr/local/bin/bowl /opt/bench/fork_bench.py
rm -f /tmp/client-bins.tar.gz

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq fuse3 python3 iperf3 jq curl ca-certificates util-linux psmisc >/dev/null

sysctl -qw net.ipv4.tcp_autocorking=0 2>/dev/null || true
ulimit -n 1048576 || true
grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

rm -f /var/log/bowl-mount.log /var/log/bowl-mount-ephemeral.log
ENDPOINT="http://${SERVER_PRIV_IP}:9000"
CREATE_JSON="$(/usr/local/bin/bowl disk create --size 2PiB --name "fork-bench-anchor" --endpoint "${ENDPOINT}" --json)"
DISK_ID="$(echo "${CREATE_JSON}" | jq -r '.disk_id')"
echo "${DISK_ID}" > /opt/bench/anchor_disk_id.txt

OTEL_SERVICE_NAME=bowl-client \
OTEL_EXPORTER_OTLP_ENDPOINT="http://${SERVER_PRIV_IP}:4317" \
OTEL_EXPORTER_OTLP_INSECURE=true \
/usr/local/bin/bowl mount "${DISK_ID}" /mnt/bowl \
  --endpoint "${ENDPOINT}" \
  --allow-other \
  --admin-addr 0.0.0.0:9567 \
  --stats-file \
  --log-level "${BOWL_BENCH_LOG_LEVEL:-warn}" \
  --log-format json \
  --trace-sample-ratio "${BOWL_BENCH_TRACE_SAMPLE_RATIO:-0.10}" \
  --log-file /var/log/bowl-mount.log

for _ in $(seq 1 30); do
  if mountpoint -q /mnt/bowl; then
    break
  fi
  sleep 1
done
mountpoint -q /mnt/bowl
df -h /mnt/bowl
EOF_CLIENT_SETUP

ANCHOR_DISK_ID="$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "cat /opt/bench/anchor_disk_id.txt")"
log "Client anchor mount /mnt/bowl ready (disk_id=${ANCHOR_DISK_ID})."

STAGE_END=$(date +%s)
record_stage "setup_client_and_mounts" "$((STAGE_END - STAGE_START))"

# ------------------------------------------------------------------------------
# Step 5: 采集双机硬件元数据与 VPC 私网 RTT / 带宽基线
# ------------------------------------------------------------------------------
log "Collecting VPC network RTT & bandwidth baselines..."
PING_STATS=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "ping -c 20 -i 0.05 ${SERVER_PRIV_IP} | tail -n 1")
IPERF_GBPS=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "iperf3 -c ${SERVER_PRIV_IP} -t 5 -J | jq -r '.end.sum_received.bits_per_second / 1e9'")
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "pkill -x iperf3 2>/dev/null || true"

SERVER_META=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "bash -s" <<'EOF_SMETA'
CPU_MODEL=$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)
VCPU=$(nproc)
MEM_GIB=$(free -h | awk '/^Mem:/ {print $2}')
KERNEL=$(uname -r)
OS_REL=$(. /etc/os-release && echo "${PRETTY_NAME}")
PG_VER=$(/usr/local/bin/bowl-prod-ctl pg-version 2>/dev/null | head -n 1 || echo "postgres (PostgreSQL) 18 (alpine)")
DOCKER_VER=$(docker --version 2>/dev/null | head -n 1 || echo "n/a")
NVME_DEV=$(cat /etc/bowl_nvme_dev)
NVME_SIZE_GIB=$(lsblk -dbno SIZE "${NVME_DEV}" | awk '{printf "%.0f", $1 / (1024*1024*1024)}')
DISK_WRITE_MIB=$(sed -n '/^{/,$p' /tmp/server_disk_write.json | jq -r '.jobs[0].write.bw_bytes / (1024*1024)')
DISK_READ_MIB=$(sed -n '/^{/,$p' /tmp/server_disk_read.json | jq -r '.jobs[0].read.bw_bytes / (1024*1024)')
RANDW_IOPS=$(sed -n '/^{/,$p' /tmp/server_disk_randw.json | jq -r '.jobs[0].write.iops // 0')
RANDR_IOPS=$(sed -n '/^{/,$p' /tmp/server_disk_randr.json | jq -r '.jobs[0].read.iops // 0')
FSYNC_IOPS=$(sed -n '/^{/,$p' /tmp/server_disk_fsync.json | jq -r '.jobs[0].write.iops // 0')
FSYNC_P99_US=$(sed -n '/^{/,$p' /tmp/server_disk_fsync.json | jq -r '((.jobs[0].sync.lat_ns.percentile["99.000000"] // 0) + (.jobs[0].write.clat_ns.percentile["99.000000"] // 0)) / 1000.0')
jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg kernel "${KERNEL}" \
  --arg os "${OS_REL}" \
  --arg pg "${PG_VER}" \
  --arg docker_ver "${DOCKER_VER}" \
  --arg nvme_dev "${NVME_DEV}" \
  --argjson nvme_gib "${NVME_SIZE_GIB}" \
  --argjson disk_write_mib "${DISK_WRITE_MIB}" \
  --argjson disk_read_mib "${DISK_READ_MIB}" \
  --argjson randw_iops "${RANDW_IOPS}" \
  --argjson randr_iops "${RANDR_IOPS}" \
  --argjson fsync_iops "${FSYNC_IOPS}" \
  --argjson fsync_p99_us "${FSYNC_P99_US}" \
  '{
    cpu_model: $cpu,
    vcpu: ($vcpu|tonumber),
    memory_gib: $mem,
    kernel: $kernel,
    os_release: $os,
    pg_version: $pg,
    bowl_pg_version: $pg,
    docker_version: $docker_ver,
    data_disk_device: $nvme_dev,
    data_disk_size_gib: $nvme_gib,
    disk_seq_write_mib_s: ($disk_write_mib * 100 | round / 100),
    disk_seq_read_mib_s: ($disk_read_mib * 100 | round / 100),
    local_randwrite_4k_iops: ($randw_iops | round),
    local_randread_4k_iops: ($randr_iops | round),
    local_fsync_4k_iops: ($fsync_iops | round),
    local_fsync_4k_p99_us: ($fsync_p99_us * 10 | round / 10)
  }'
EOF_SMETA
)

CLIENT_META=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "bash -s" <<'EOF_CMETA'
CPU_MODEL=$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)
VCPU=$(nproc)
MEM_GIB=$(free -h | awk '/^Mem:/ {print $2}')
KERNEL=$(uname -r)
OS_REL=$(. /etc/os-release && echo "${PRETTY_NAME}")
FUSE_VER=$(fusermount3 --version | head -n 1)
PY_VER=$(python3 --version | head -n 1)
NVME_DEV=$(cat /etc/bowl_nvme_dev)
NVME_SIZE_GIB=$(lsblk -dbno SIZE "${NVME_DEV}" | awk '{printf "%.0f", $1 / (1024*1024*1024)}')
jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg kernel "${KERNEL}" \
  --arg os "${OS_REL}" \
  --arg fuse "${FUSE_VER}" \
  --arg py "${PY_VER}" \
  --arg nvme_dev "${NVME_DEV}" \
  --argjson nvme_gib "${NVME_SIZE_GIB}" \
  '{
    cpu_model: $cpu,
    vcpu: ($vcpu|tonumber),
    memory_gib: $mem,
    kernel: $kernel,
    os_release: $os,
    fusermount_version: $fuse,
    python_version: $py,
    data_disk_device: $nvme_dev,
    data_disk_size_gib: $nvme_gib
  }'
EOF_CMETA
)

# 确保 Prometheus 4/4 抓取目标全部已进入 UP 态后再开启正式压测窗口
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "bash -s" <<'EOF_WAIT_PROM'
for _ in $(seq 1 15); do
  up_cnt=$(curl -fsS http://127.0.0.1:9091/api/v1/targets 2>/dev/null | jq '[.data.activeTargets[] | select(.health == "up")] | length' 2>/dev/null || echo 0)
  if [[ "${up_cnt}" -ge 4 ]]; then
    break
  fi
  sleep 1
done
EOF_WAIT_PROM

# ------------------------------------------------------------------------------
# Step 6: 执行 Disk Fork 三大基准测试套件 (Suite 1 / Suite 2 / Suite 3)
# ------------------------------------------------------------------------------
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "date +%s > /var/run/bowl-prod-started.epoch"
TEST_START_EPOCH="$(date +%s)"
log "=== Executing fork_bench.py (Suite 1 Matrix / Suite 2 Online Fork / Suite 3 1->16 Fan-Out) ==="

ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" \
  "SERVER_PRIV_IP='${SERVER_PRIV_IP}' ANCHOR_DISK_ID='${ANCHOR_DISK_ID}' TRACE_RATIO='${BOWL_BENCH_TRACE_SAMPLE_RATIO}' bash -s" <<'EOF_RUN_BENCH'
set -euo pipefail
ulimit -n 1048576 || true
export TMPDIR=/mnt/nvme/bench_tmp
PYTHONDONTWRITEBYTECODE=1 python3 /opt/bench/fork_bench.py \
  --endpoint "http://${SERVER_PRIV_IP}:9000" \
  --server-priv-ip "${SERVER_PRIV_IP}" \
  --ssh-key /root/.ssh/id_rsa \
  --bowl-bin /usr/local/bin/bowl \
  --primary-mount /mnt/bowl \
  --primary-disk-id "${ANCHOR_DISK_ID}" \
  --work-mount-root /mnt/nvme/fork_work \
  --ephemeral-log /var/log/bowl-mount-ephemeral.log \
  --trace-sample-ratio "${TRACE_RATIO}" \
  --telemetry-helper /opt/bowl/client_telemetry.sh \
  --out-dir /tmp/bench_results
EOF_RUN_BENCH

TEST_END_EPOCH="$(date +%s)"

log "Harvesting Bowl pre/post-umount telemetry for window [${TEST_START_EPOCH}, ${TEST_END_EPOCH}]..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" \
  "SERVER_PRIV_IP='${SERVER_PRIV_IP}' ANCHOR_DISK_ID='${ANCHOR_DISK_ID}' bash -s" <<'EOF_CLI_FLUSH'
set -euo pipefail
SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa "root@${SERVER_PRIV_IP}")
source /opt/bowl/client_telemetry.sh
harvest_bowl_before_and_after_umount /mnt/bowl
if [[ -s /var/log/bowl-mount-ephemeral.log ]]; then
  cat /var/log/bowl-mount-ephemeral.log >> /tmp/bowl-telemetry/client/logs/bowl-mount.log
fi
/usr/local/bin/bowl disk delete "${ANCHOR_DISK_ID}" --endpoint "http://${SERVER_PRIV_IP}:9000" >/dev/null 2>&1 || true
EOF_CLI_FLUSH

ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" \
  "/usr/local/bin/bowl-prod-ctl export-telemetry /tmp/bowl-telemetry/server '${TEST_START_EPOCH}' '${TEST_END_EPOCH}' && /usr/local/bin/bowl-prod-ctl stop-storage"

# ------------------------------------------------------------------------------
# Step 7: 回传结果、提炼遥测诊断包、强制销毁云资源并执行零残留审计
# ------------------------------------------------------------------------------
log "Downloading raw benchmark results to ${RESULTS_DIR}..."
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}:/tmp/bench_results/*" "${RESULTS_DIR}/"
if [[ -f "${RESULTS_DIR}/fork_stages.jsonl" ]]; then
  cat "${RESULTS_DIR}/fork_stages.jsonl" >> "${RESULTS_DIR}/stage_timings.jsonl"
  rm -f "${RESULTS_DIR}/fork_stages.jsonl"
fi

fetch_telemetry_archive
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "/usr/local/bin/bowl-prod-ctl down-volumes" >/dev/null 2>&1 || true

DELETED_KEY_PAIR="${KEY_PAIR_NAME}"
REMAINING_COUNT=0
REMAINING_KEYPAIRS=0

if [[ "${KEEP_INSTANCES}" != "1" ]]; then
  log "Destroying temporary ECS instances now that benchmarks are complete..."
  IDS_JSON=$(printf '%s\n' "${SERVER_INSTANCE_ID}" "${CLIENT_INSTANCE_ID}" | jq -R . | jq -s -c .)
  aliyun ecs DeleteInstances \
    --RegionId "${REGION_ID}" \
    --Force true \
    --InstanceId.1 "${SERVER_INSTANCE_ID}" \
    --InstanceId.2 "${CLIENT_INSTANCE_ID}" >/dev/null
  INSTANCES_DELETED_AT="$(date -u +'%Y-%m-%dT%H:%M:%SZ')"

  REMAINING_COUNT=2
  for _ in {1..30}; do
    REMAINING_COUNT=$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${IDS_JSON}" | jq '.Instances.Instance | length')
    [[ "${REMAINING_COUNT}" == "0" ]] && break
    sleep 5
  done
  log "Verified remaining target ECS instances count after DeleteInstances: ${REMAINING_COUNT}"
  if [[ "${REMAINING_COUNT}" != "0" ]]; then
    log "ERROR: 仍有 ${REMAINING_COUNT} 台测试实例残留，清理钩子将继续尝试。"
    exit 1
  fi
  SERVER_INSTANCE_ID=""
  CLIENT_INSTANCE_ID=""

  aliyun ecs DeleteKeyPairs \
    --RegionId "${REGION_ID}" \
    --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null
  KEY_PAIR_NAME=""

  REMAINING_KEYPAIRS=$(aliyun ecs DescribeKeyPairs --RegionId "${REGION_ID}" --KeyPairName "${DELETED_KEY_PAIR}" | jq '.KeyPairs.KeyPair | length')
  log "Verified remaining target SSH KeyPairs count after DeleteKeyPairs: ${REMAINING_KEYPAIRS}"
  if [[ "${REMAINING_KEYPAIRS}" != "0" ]]; then
    log "ERROR: 密钥对 ${DELETED_KEY_PAIR} 未能彻底清理！"
    exit 1
  fi
fi

jq -n \
  --arg region "${REGION_ID}" \
  --arg zone "${ZONE_ID}" \
  --arg vpc_id "vpc-<redacted>" \
  --arg vswitch_id "vsw-<redacted>" \
  --arg sg_id "sg-<redacted>" \
  --arg instance_type "${INSTANCE_TYPE}" \
  --arg image_id "${IMAGE_ID}" \
  --arg disk_cat "${SYSTEM_DISK_CATEGORY}" \
  --arg disk_size "${SYSTEM_DISK_SIZE}" \
  --arg srv_id "i-<redacted-server>" \
  --arg srv_priv "${SERVER_PRIV_IP}" \
  --arg cli_id "i-<redacted-client>" \
  --arg cli_priv "${CLIENT_PRIV_IP}" \
  --arg created_at "${INSTANCES_CREATED_AT}" \
  --arg deleted_at "${INSTANCES_DELETED_AT:-kept}" \
  --arg key_pair "${DELETED_KEY_PAIR}" \
  --argjson remaining "${REMAINING_COUNT}" \
  --argjson remaining_kp "${REMAINING_KEYPAIRS}" \
  --arg ping_stats "${PING_STATS}" \
  --arg iperf_gbps "${IPERF_GBPS}" \
  --arg bowl_commit "${BOWL_GIT_COMMIT}" \
  --argjson srv_meta "${SERVER_META}" \
  --argjson cli_meta "${CLIENT_META}" \
  '{
    cloud: {
      provider: "Aliyun ECS",
      region: $region,
      zone_id: $zone,
      vpc_id: $vpc_id,
      vswitch_id: $vswitch_id,
      security_group_id: $sg_id,
      instance_type: $instance_type,
      image_id: $image_id,
      system_disk_category: $disk_cat,
      system_disk_size_gib: ($disk_size|tonumber),
      data_disk_category: "local_ssd_pro (NVMe)",
      data_disk_size_gib: $srv_meta.data_disk_size_gib
    },
    server_node: ({
      instance_id: $srv_id,
      private_ip: $srv_priv
    } + $srv_meta),
    client_node: ({
      instance_id: $cli_id,
      private_ip: $cli_priv
    } + $cli_meta),
    network_baseline: {
      ping_rtt_ms: $ping_stats,
      iperf3_gbps: ($iperf_gbps|tonumber * 100 | round / 100)
    },
    server_disk_baseline: {
      local_seq_write_mib_s: $srv_meta.disk_seq_write_mib_s,
      local_seq_read_mib_s: $srv_meta.disk_seq_read_mib_s,
      local_randwrite_4k_iops: $srv_meta.local_randwrite_4k_iops,
      local_randread_4k_iops: $srv_meta.local_randread_4k_iops,
      local_fsync_4k_iops: $srv_meta.local_fsync_4k_iops,
      local_fsync_4k_p99_us: $srv_meta.local_fsync_4k_p99_us
    },
    bowl_config: {
      git_commit: $bowl_commit,
      deploy_profile: "deploy/prod (docker-compose.yaml + storage.env)",
      observability_stack: "Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=\(env.BOWL_BENCH_TRACE_SAMPLE_RATIO // "0.10")) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof",
      telemetry_dir: "results/telemetry",
      storage_engine: "StoreActor per disk (Pebble + 128 MiB 共享缓存 + 双缓冲 MPSC Group Commit + 单步在线 ForkDisk 硬链接切面与孤儿离线净化)",
      blob_layout: "packs/<pack_id_hex16>.pack (64 MiB Append-Only Pack + 256 KiB 块网格 + 跨分支 os.Link 零拷贝共享与血统提交回收)",
      blob_size_bytes: 262144,
      pack_max_bytes: 67108864,
      blob_ref_bytes: 16,
      write_behind_chunk_bytes: 4194304,
      write_behind_max_inflight: 4,
      http2_stream_window_bytes: 8388608,
      http2_conn_window_bytes: 33554432,
      kernel_writeback_cache: false,
      open_keep_cache: true,
      handle_killpriv: true,
      max_background: 64,
      congestion_threshold: 48,
      flush_issues_fsync_rpc: false,
      wal_sync_mode: "pebble.NoSync Group Commit + Fsync/FsyncDir 双层并行落盘屏障 (fdatasync + LogData)",
      concurrency_model: "单盘独立 StoreActor 串行切面固化 + WriterEpoch 世代号栅栏 + 跨盘独立并发读写",
      atime_mode: "noatime",
      fuse_cache_timeout: "1.0s (entry=1s, attr=1s, direntry=1s)"
    },
    cleanup_audit: {
      key_pair_name: $key_pair,
      server_instance_ids: $srv_id,
      client_instance_ids: $cli_id,
      instances_created_at: $created_at,
      instances_deleted_at: $deleted_at,
      remaining_benchmark_instances: $remaining,
      remaining_keypairs: $remaining_kp
    }
  }' > "${RESULTS_DIR}/env_metadata.json"

log "Generating summary.json and REPORT.md via parse_results.py..."
PYTHONDONTWRITEBYTECODE=1 python3 "${SCRIPT_DIR}/parse_results.py" \
  --results-dir "${RESULTS_DIR}" \
  --summary-out "${SUMMARY_JSON}" \
  --report-out "${REPORT_FILE}"

log "=== Disk Fork Benchmark Suite Completed Successfully! Report saved to ${REPORT_FILE} ==="
