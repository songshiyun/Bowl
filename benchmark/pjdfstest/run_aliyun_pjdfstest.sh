#!/usr/bin/env bash
# ==============================================================================
# Bowl - Aliyun Silicon Valley (us-west-1) Dual-ECS Internal Network pjdfstest
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"
REPORT_FILE="${REPORT_FILE:-${SCRIPT_DIR}/REPORT.md}"

# Aliyun Configuration (Silicon Valley us-west-1a)
REGION_ID="${REGION_ID:-us-west-1}"
ZONE_ID="${ZONE_ID:-us-west-1a}"
VPC_ID="${VPC_ID:-vpc-rj9gjwyzzh4v0cn576fhu}"
VSWITCH_ID="${VSWITCH_ID:-vsw-rj9rsvsgpihtsgxej6me5}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-sg-rj97ly79h6cp3mnfpo2q}"
INSTANCE_TYPE="${INSTANCE_TYPE:-ecs.c9i.xlarge}"
IMAGE_ID="${IMAGE_ID:-ubuntu_24_04_x64_20G_alibase_20261016.vhd}"
SYSTEM_DISK_CATEGORY="${SYSTEM_DISK_CATEGORY:-cloud_essd}"
SYSTEM_DISK_SIZE="${SYSTEM_DISK_SIZE:-40}"
KEEP_INSTANCES="${KEEP_INSTANCES:-0}"

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
KEY_PAIR_NAME="bowl-pjdfstest-kp-${RUN_TS}"
WORK_DIR="$(mktemp -d "/tmp/bowl-pjdfstest-${RUN_TS}-XXXXXX")"
SSH_KEY_FILE="${WORK_DIR}/id_rsa"
CREATED_INSTANCES_FILE="${WORK_DIR}/created_instances.txt"
KEY_CREATED=0
INSTANCES_CREATED_AT=""
INSTANCES_DELETED_AT=""
REMAINING_INSTANCES=0

mkdir -p "${RESULTS_DIR}"

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

cleanup() {
  local exit_code=$?
  set +e

  if [[ "${KEEP_INSTANCES}" == "1" ]]; then
    log "KEEP_INSTANCES=1 is set; skipping automatic deletion of ECS instances and keypair."
    log "Temporary workdir preserved at: ${WORK_DIR}"
    return "${exit_code}"
  fi

  if [[ -f "${CREATED_INSTANCES_FILE}" ]]; then
    local ids=()
    while IFS= read -r line; do
      [[ -n "${line}" ]] && ids+=("${line}")
    done < "${CREATED_INSTANCES_FILE}"

    if [[ ${#ids[@]} -gt 0 ]]; then
      log "Cleaning up ${#ids[@]} pay-as-you-go ECS instance(s) in ${REGION_ID}: ${ids[*]}"
      local del_args=(ecs DeleteInstances --RegionId "${REGION_ID}" --Force true)
      local idx=1
      for id in "${ids[@]}"; do
        del_args+=("--InstanceId.${idx}" "${id}")
        idx=$((idx + 1))
      done

      for attempt in {1..6}; do
        if aliyun "${del_args[@]}" >/dev/null 2>&1; then
          break
        fi
        sleep 5
      done

      # Poll until instances disappear from DescribeInstances
      local id_json
      id_json="$(printf '%s\n' "${ids[@]}" | jq -R . | jq -sc .)"
      for poll in {1..24}; do
        local count
        count="$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${id_json}" 2>/dev/null | jq -r '.TotalCount // 0')"
        if [[ "${count}" == "0" ]]; then
          REMAINING_INSTANCES=0
          INSTANCES_DELETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
          log "Verified all ${#ids[@]} benchmark ECS instances are completely terminated (Remaining: 0)."
          break
        fi
        REMAINING_INSTANCES="${count}"
        sleep 5
      done
    fi
    rm -f "${CREATED_INSTANCES_FILE}"
  fi

  if [[ "${KEY_CREATED}" == "1" ]]; then
    log "Deleting temporary SSH keypair: ${KEY_PAIR_NAME}"
    aliyun ecs DeleteKeyPairs --RegionId "${REGION_ID}" --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null 2>&1 || true
    KEY_CREATED=0
  fi

  rm -rf "${WORK_DIR}"
  return "${exit_code}"
}
trap cleanup EXIT

# ==============================================================================
# Step 1: Cross-compile Linux amd64 binaries locally (GOWORK=off)
# ==============================================================================
log "Step 1/6: Cross-compiling storage-service, bowl CLI, and goose for linux/amd64..."
GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/services/storage" build -trimpath -ldflags="-w -s" \
  -o "${WORK_DIR}/storage-service" ./cmd

GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/client" build -trimpath -ldflags="-w -s" \
  -o "${WORK_DIR}/bowl" ./cmd/bowl

GOOSE_CACHE_BIN="/tmp/bowl-pjdfstest-goose-linux-amd64"
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
log "Compiled binaries ready in ${WORK_DIR}"

# ==============================================================================
# Step 2: Provision 2 Pay-As-You-Go ECS Instances in us-west-1a
# ==============================================================================
log "Step 2/6: Creating temporary SSH KeyPair ${KEY_PAIR_NAME} in ${REGION_ID}..."
aliyun ecs CreateKeyPair \
  --RegionId "${REGION_ID}" \
  --KeyPairName "${KEY_PAIR_NAME}" \
  | jq -r '.PrivateKeyBody' > "${SSH_KEY_FILE}"
chmod 600 "${SSH_KEY_FILE}"
KEY_CREATED=1

log "Launching 2 PostPaid ECS instances (${INSTANCE_TYPE}, ${IMAGE_ID}) in ${ZONE_ID}..."
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
  --InstanceName "bowl-pjdfstest" \
  --UniqueSuffix true)"

echo "${RUN_RESP}" | jq -r '.InstanceIdSets.InstanceIdSet[]' > "${CREATED_INSTANCES_FILE}"
INSTANCES_CREATED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

SERVER_ID="$(sed -n '1p' "${CREATED_INSTANCES_FILE}")"
CLIENT_ID="$(sed -n '2p' "${CREATED_INSTANCES_FILE}")"
if [[ -z "${SERVER_ID}" || -z "${CLIENT_ID}" ]]; then
  log "ERROR: Failed to parse 2 instance IDs from RunInstances response: ${RUN_RESP}"
  exit 1
fi
log "Created Server Instance: ${SERVER_ID}, Client Instance: ${CLIENT_ID}"

INSTANCE_IDS_JSON="[\"${SERVER_ID}\",\"${CLIENT_ID}\"]"
SERVER_PUB_IP=""
SERVER_PRIV_IP=""
CLIENT_PUB_IP=""
CLIENT_PRIV_IP=""

log "Waiting for both instances to reach Running state..."
for poll in {1..60}; do
  DESC_JSON="$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${INSTANCE_IDS_JSON}")"
  RUNNING_COUNT="$(echo "${DESC_JSON}" | jq '[.Instances.Instance[] | select(.Status == "Running")] | length')"
  if [[ "${RUNNING_COUNT}" == "2" ]]; then
    SERVER_PUB_IP="$(echo "${DESC_JSON}" | jq -r ".Instances.Instance[] | select(.InstanceId == \"${SERVER_ID}\") | .PublicIpAddress.IpAddress[0]")"
    SERVER_PRIV_IP="$(echo "${DESC_JSON}" | jq -r ".Instances.Instance[] | select(.InstanceId == \"${SERVER_ID}\") | .VpcAttributes.PrivateIpAddress.IpAddress[0]")"
    CLIENT_PUB_IP="$(echo "${DESC_JSON}" | jq -r ".Instances.Instance[] | select(.InstanceId == \"${CLIENT_ID}\") | .PublicIpAddress.IpAddress[0]")"
    CLIENT_PRIV_IP="$(echo "${DESC_JSON}" | jq -r ".Instances.Instance[] | select(.InstanceId == \"${CLIENT_ID}\") | .VpcAttributes.PrivateIpAddress.IpAddress[0]")"
    break
  fi
  sleep 5
done

if [[ -z "${SERVER_PUB_IP}" || -z "${CLIENT_PUB_IP}" ]]; then
  log "ERROR: Timed out waiting for instances to enter Running state."
  exit 1
fi

log "Server (${SERVER_ID}): Public=${SERVER_PUB_IP}, Private=${SERVER_PRIV_IP}"
log "Client (${CLIENT_ID}): Public=${CLIENT_PUB_IP}, Private=${CLIENT_PRIV_IP}"

SSH_OPTS=(-o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -o ConnectTimeout=8 -o ServerAliveInterval=15 -i "${SSH_KEY_FILE}")
SERVER_SSH_OPTS=("${SSH_OPTS[@]}")
CLIENT_SSH_OPTS=("${SSH_OPTS[@]}")
SERVER_SSH_HOST="${SERVER_PUB_IP}"
CLIENT_SSH_HOST="${CLIENT_PUB_IP}"

log "Waiting for SSH readiness on Server (${SERVER_PUB_IP}) and Client (${CLIENT_PUB_IP})..."
SERVER_OK=0
CLIENT_OK=0
for ((i=1; i<=30; i++)); do
  if [[ "${SERVER_OK}" == "0" ]] && ssh "${SSH_OPTS[@]}" "root@${SERVER_PUB_IP}" "true" >/dev/null 2>&1; then
    SERVER_OK=1
    log "Direct SSH ready on bowl-bench-server (${SERVER_PUB_IP})"
  fi
  if [[ "${CLIENT_OK}" == "0" ]] && ssh "${SSH_OPTS[@]}" "root@${CLIENT_PUB_IP}" "true" >/dev/null 2>&1; then
    CLIENT_OK=1
    log "Direct SSH ready on bowl-bench-client (${CLIENT_PUB_IP})"
  fi
  if [[ "${SERVER_OK}" == "1" && "${CLIENT_OK}" == "1" ]]; then
    break
  fi
  sleep 3
done

if [[ "${SERVER_OK}" == "0" && "${CLIENT_OK}" == "1" ]]; then
  log "Server public IP (${SERVER_PUB_IP}) has cross-border lag; routing Server SSH via Client (${CLIENT_PUB_IP}) -> VPC (${SERVER_PRIV_IP})..."
  SERVER_SSH_HOST="${SERVER_PRIV_IP}"
  SERVER_SSH_OPTS=("${SSH_OPTS[@]}" -o "ProxyCommand=ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i ${SSH_KEY_FILE} -W %h:%p root@${CLIENT_PUB_IP}")
  ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "true"
  log "VPC ProxyJump SSH ready on bowl-bench-server (${SERVER_PRIV_IP})"
elif [[ "${CLIENT_OK}" == "0" && "${SERVER_OK}" == "1" ]]; then
  log "Client public IP (${CLIENT_PUB_IP}) has cross-border lag; routing Client SSH via Server (${SERVER_PUB_IP}) -> VPC (${CLIENT_PRIV_IP})..."
  CLIENT_SSH_HOST="${CLIENT_PRIV_IP}"
  CLIENT_SSH_OPTS=("${SSH_OPTS[@]}" -o "ProxyCommand=ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i ${SSH_KEY_FILE} -W %h:%p root@${SERVER_PUB_IP}")
  ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "true"
  log "VPC ProxyJump SSH ready on bowl-bench-client (${CLIENT_PRIV_IP})"
elif [[ "${SERVER_OK}" == "0" && "${CLIENT_OK}" == "0" ]]; then
  log "ERROR: Both instances timed out on direct SSH."
  exit 1
fi

# ==============================================================================
# Step 3: Deploy Storage Service + PostgreSQL on Server Node
# ==============================================================================
log "Step 3/6: Deploying PostgreSQL, Goose migrations, and storage-service on Server (${SERVER_PRIV_IP})..."
ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "mkdir -p /opt/bowl/migrations /var/lib/bowl/data"
scp "${SERVER_SSH_OPTS[@]}" "${WORK_DIR}/storage-service" "${WORK_DIR}/goose" "root@${SERVER_SSH_HOST}:/usr/local/bin/"
scp "${SERVER_SSH_OPTS[@]}" "${REPO_ROOT}"/services/storage/db/migrations/*.sql "root@${SERVER_SSH_HOST}:/opt/bowl/migrations/"

ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq postgresql iperf3 jq

systemctl enable --now postgresql
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'postgres';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'bowl'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE DATABASE bowl;"
sudo -u postgres psql -c "ALTER SYSTEM SET max_connections = '200';"
systemctl restart postgresql

/usr/local/bin/goose -dir /opt/bowl/migrations postgres "postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable" up

cat >/etc/systemd/system/bowl-storage.service <<'UNIT'
[Unit]
Description=Bowl Storage Service
After=network.target postgresql.service

[Service]
Type=simple
Environment="SERVER_HOST=0.0.0.0"
Environment="SERVER_PORT=9000"
Environment="SERVER_READ_MAX_BYTES=16777216"
Environment="STORAGE_DATA_DIR=/var/lib/bowl/data"
Environment="DB_DSN=postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable"
Environment="DB_MAX_CONNS=64"
Environment="DB_MIN_CONNS=8"
Environment="STORAGE_ATIME_MODE=strictatime"
Environment="STORAGE_ENTRY_TIMEOUT_SECONDS=0"
Environment="STORAGE_ATTR_TIMEOUT_SECONDS=0"
Environment="STORAGE_DIR_ENTRY_TIMEOUT_SECONDS=0"
Environment="LOG_LEVEL=warn"
Environment="OTEL_ENABLED=false"
ExecStart=/usr/local/bin/storage-service
Restart=always
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now bowl-storage

for i in {1..30}; do
  if ss -tln | grep -q ':9000 '; then
    break
  fi
  sleep 1
done
ss -tln | grep -q ':9000 '

# Start iperf3 daemon for internal network benchmark
pkill -f "iperf3 -s" || true
iperf3 -s -D
EOF
log "Server node (${SERVER_PRIV_IP}:9000) is up and healthy."

# ==============================================================================
# Step 4: Deploy Bowl FUSE Client & Build pjdfstest on Client Node
# ==============================================================================
log "Step 4/6: Preparing Client (${CLIENT_PRIV_IP}), building pjdfstest, and mounting Bowl over VPC internal network..."
scp "${CLIENT_SSH_OPTS[@]}" "${WORK_DIR}/bowl" "root@${CLIENT_SSH_HOST}:/usr/local/bin/bowl"

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq fuse3 libfuse3-dev iperf3 git build-essential autoconf automake libtool pkg-config perl python3 jq

grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

if [[ ! -x /opt/pjdfstest/pjdfstest ]]; then
  rm -rf /opt/pjdfstest
  git clone --depth 1 https://github.com/pjd/pjdfstest.git /opt/pjdfstest
  cd /opt/pjdfstest
  autoreconf -ifs
  ./configure
  make -j"$(nproc)"
  ln -sf /opt/pjdfstest/pjdfstest /usr/local/bin/pjdfstest
fi
EOF

# Collect server & client metadata + internal VPC network baseline
log "Measuring internal VPC RTT and throughput (${CLIENT_PRIV_IP} -> ${SERVER_PRIV_IP})..."
SERVER_INFO_JSON="$(ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" <<'EOF'
CPU_MODEL="$(lscpu | awk -F: '/Model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}')"
VCPU="$(nproc)"
MEM_GIB="$(awk '/MemTotal/ {printf "%.1f GiB", $2/1024/1024}' /proc/meminfo)"
OS_REL="$(. /etc/os-release && echo "${PRETTY_NAME}")"
KERNEL="$(uname -r)"
PG_VER="$(psql --version | head -n1)"
jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg os "${OS_REL}" \
  --arg kernel "${KERNEL}" \
  --arg pg "${PG_VER}" \
  '{cpu_model: $cpu, vcpu: $vcpu, memory_gib: $mem, os_release: $os, kernel: $kernel, pg_version: $pg}'
EOF
)"

CLIENT_MOUNT_JSON="$(ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "SERVER_PRIV_IP=${SERVER_PRIV_IP} bash -s" <<'EOF'
set -euo pipefail
ENDPOINT="http://${SERVER_PRIV_IP}:9000"

# Ping RTT over internal network
PING_OUT="$(ping -c 20 -i 0.05 "${SERVER_PRIV_IP}" | tail -n 1)"
PING_RTT="$(echo "${PING_OUT}" | awk -F'=' '{gsub(/ ms/, "", $2); gsub(/^[ \t]+/, "", $2); print $2}')"

# iperf3 internal TCP bandwidth
IPERF_BPS="$(iperf3 -c "${SERVER_PRIV_IP}" -t 5 -J | jq -r '.end.sum_received.bits_per_second // 0')"
IPERF_GBPS="$(awk -v bps="${IPERF_BPS}" 'BEGIN {printf "%.2f", bps / 1e9}')"

# Create disk with root_ino > 1 to test non-identity root inode mapping
DISK_ID=""
ROOT_INO=0
for i in {1..5}; do
  CREATE_JSON="$(/usr/local/bin/bowl disk create --size 2PiB --name "pjdfstest-disk-${i}" --endpoint "${ENDPOINT}" --json)"
  DISK_ID="$(echo "${CREATE_JSON}" | jq -r '.disk_id')"
  ROOT_INO="$(echo "${CREATE_JSON}" | jq -r '.root_ino')"
  if [[ "${ROOT_INO}" -gt 1 ]]; then
    break
  fi
done

mkdir -p /mnt/bowl
if grep -q " /mnt/bowl " /proc/self/mountinfo; then
  /usr/local/bin/bowl umount /mnt/bowl || umount -l /mnt/bowl || true
fi
/usr/local/bin/bowl mount "${DISK_ID}" /mnt/bowl --endpoint "${ENDPOINT}" --allow-other --log-file /var/log/bowl-mount.log
mkdir -p /mnt/bowl/work

CPU_MODEL="$(lscpu | awk -F: '/Model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}')"
VCPU="$(nproc)"
MEM_GIB="$(awk '/MemTotal/ {printf "%.1f GiB", $2/1024/1024}' /proc/meminfo)"
OS_REL="$(. /etc/os-release && echo "${PRETTY_NAME}")"
KERNEL="$(uname -r)"
FUSER_VER="$(fusermount3 -V | head -n1)"
PJDFS_COMMIT="$(git -C /opt/pjdfstest rev-parse --short HEAD)"

jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg os "${OS_REL}" \
  --arg kernel "${KERNEL}" \
  --arg fuser "${FUSER_VER}" \
  --arg pjdfs_commit "${PJDFS_COMMIT}" \
  --arg ping_rtt "${PING_RTT}" \
  --arg iperf_gbps "${IPERF_GBPS}" \
  --arg disk_id "${DISK_ID}" \
  --argjson root_ino "${ROOT_INO}" \
  '{
    client: {cpu_model: $cpu, vcpu: $vcpu, memory_gib: $mem, os_release: $os, kernel: $kernel, fusermount_version: $fuser},
    network: {ping_rtt_ms: $ping_rtt, iperf3_gbps: $iperf_gbps},
    bowl: {disk_id: $disk_id, root_ino: $root_ino, pjdfstest_git_commit: $pjdfs_commit}
  }'
EOF
)"

log "Client mounted /mnt/bowl over VPC internal network: $(echo "${CLIENT_MOUNT_JSON}" | jq -c '.')"

# ==============================================================================
# Step 5: Execute Full pjdfstest Suite over Internal Network
# ==============================================================================
TEST_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "Step 5/6: Running full pjdfstest suite on /mnt/bowl/work..."

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "python3 -u -" <<'PYEOF'
import json
import os
import subprocess
import sys
import time
from pathlib import Path

tests_root = Path("/opt/pjdfstest/tests")
work_dir = Path("/mnt/bowl/work")
work_dir.mkdir(parents=True, exist_ok=True)

log_path = Path("/tmp/pjdfstest.log")
timings_path = Path("/tmp/dir_timings.jsonl")

env = os.environ.copy()
env["PATH"] = f"/opt/pjdfstest:/usr/local/bin:/usr/bin:/bin:{env.get('PATH', '')}"

subdirs = sorted([d.name for d in tests_root.iterdir() if d.is_dir()])
print(f"[pjdfstest-runner] Discovered {len(subdirs)} syscall directories: {', '.join(subdirs)}", flush=True)

with log_path.open("w", encoding="utf-8") as log_f, timings_path.open("w", encoding="utf-8") as tim_f:
    for idx, dname in enumerate(subdirs, 1):
        target_dir = tests_root / dname
        t0 = time.monotonic()
        print(f"[{idx:02d}/{len(subdirs):02d}] Running prove --timer -rv {target_dir} ...", flush=True)
        proc = subprocess.run(
            ["prove", "--timer", "-rv", str(target_dir)],
            cwd=str(work_dir),
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            errors="replace",
        )
        elapsed = round(time.monotonic() - t0, 3)
        log_f.write(f"# === DIRECTORY: {dname} (rc={proc.returncode}, duration_sec={elapsed}) ===\n")
        log_f.write(proc.stdout)
        if not proc.stdout.endswith("\n"):
            log_f.write("\n")
        log_f.flush()

        # Extract quick summary line for progress output
        summary_line = ""
        for line in reversed(proc.stdout.splitlines()):
            if line.startswith("Files=") or line.startswith("Result:"):
                summary_line = f"{line} {summary_line}".strip()
        print(f"    -> Done {dname} in {elapsed:.2f}s (rc={proc.returncode}): {summary_line}", flush=True)

        tim_f.write(json.dumps({
            "directory": dname,
            "rc": proc.returncode,
            "duration_sec": elapsed,
        }) + "\n")
        tim_f.flush()
PYEOF

TEST_COMPLETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "pjdfstest execution finished at ${TEST_COMPLETED_AT}."

# Unmount cleanly and fetch logs
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "/usr/local/bin/bowl umount /mnt/bowl || umount /mnt/bowl || true"
scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/pjdfstest.log" "${RESULTS_DIR}/pjdfstest.log"
scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/dir_timings.jsonl" "${RESULTS_DIR}/dir_timings.jsonl"

# ==============================================================================
# Step 6: Destroy Cloud Instances, Audit Cleanup & Generate REPORT.md
# ==============================================================================
log "Step 6/6: Releasing Aliyun PostPaid ECS instances and generating REPORT.md..."
if [[ "${KEEP_INSTANCES}" != "1" ]]; then
  aliyun ecs DeleteInstances \
    --RegionId "${REGION_ID}" \
    --Force true \
    --InstanceId.1 "${SERVER_ID}" \
    --InstanceId.2 "${CLIENT_ID}" >/dev/null

  for poll in {1..30}; do
    COUNT="$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${INSTANCE_IDS_JSON}" | jq -r '.TotalCount // 0')"
    if [[ "${COUNT}" == "0" ]]; then
      REMAINING_INSTANCES=0
      INSTANCES_DELETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
      log "Verified both ECS instances (${SERVER_ID}, ${CLIENT_ID}) are deleted (Remaining: 0)."
      break
    fi
    REMAINING_INSTANCES="${COUNT}"
    sleep 5
  done
  rm -f "${CREATED_INSTANCES_FILE}"

  aliyun ecs DeleteKeyPairs --RegionId "${REGION_ID}" --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null 2>&1 || true
  KEY_CREATED=0
fi

BOWL_GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"

jq -n \
  --arg test_started_at "${TEST_STARTED_AT}" \
  --arg test_completed_at "${TEST_COMPLETED_AT}" \
  --arg region_id "${REGION_ID}" \
  --arg zone_id "${ZONE_ID}" \
  --arg vpc_id "${VPC_ID}" \
  --arg vswitch_id "${VSWITCH_ID}" \
  --arg sg_id "${SECURITY_GROUP_ID}" \
  --arg instance_type "${INSTANCE_TYPE}" \
  --arg image_id "${IMAGE_ID}" \
  --arg disk_cat "${SYSTEM_DISK_CATEGORY}" \
  --argjson disk_size "${SYSTEM_DISK_SIZE}" \
  --arg server_id "${SERVER_ID}" \
  --arg server_priv_ip "${SERVER_PRIV_IP}" \
  --arg client_id "${CLIENT_ID}" \
  --arg client_priv_ip "${CLIENT_PRIV_IP}" \
  --arg bowl_commit "${BOWL_GIT_COMMIT}" \
  --arg key_pair "${KEY_PAIR_NAME}" \
  --arg created_at "${INSTANCES_CREATED_AT}" \
  --arg deleted_at "${INSTANCES_DELETED_AT:-kept}" \
  --argjson remaining "${REMAINING_INSTANCES}" \
  --argjson srv "${SERVER_INFO_JSON}" \
  --argjson cli "${CLIENT_MOUNT_JSON}" \
  '{
    test_started_at: $test_started_at,
    test_completed_at: $test_completed_at,
    cloud: {
      provider: "Alibaba Cloud (阿里云)",
      region_id: $region_id,
      zone_id: $zone_id,
      vpc_id: $vpc_id,
      vswitch_id: $vswitch_id,
      cidr_block: "172.16.1.0/24",
      security_group_id: $sg_id,
      instance_type: $instance_type,
      instance_charge_type: "PostPaid (按量计费)",
      image_id: $image_id,
      system_disk_category: $disk_cat,
      system_disk_size_gib: $disk_size
    },
    server_node: ($srv + {instance_id: $server_id, private_ip: $server_priv_ip}),
    client_node: ($cli.client + {instance_id: $client_id, private_ip: $client_priv_ip}),
    network_baseline: $cli.network,
    bowl_config: ($cli.bowl + {
      bowl_git_commit: $bowl_commit,
      disk_size: "2PiB",
      atime_mode: "strictatime",
      entry_timeout: "0",
      attr_timeout: "0",
      dir_entry_timeout: "0"
    }),
    cleanup_audit: {
      key_pair_name: $key_pair,
      instances_created_at: $created_at,
      instances_deleted_at: $deleted_at,
      remaining_benchmark_instances: $remaining
    }
  }' > "${RESULTS_DIR}/env_metadata.json"

PYTHONDONTWRITEBYTECODE=1 python3 "${SCRIPT_DIR}/parse_results.py" \
  --log "${RESULTS_DIR}/pjdfstest.log" \
  --timings "${RESULTS_DIR}/dir_timings.jsonl" \
  --env-meta "${RESULTS_DIR}/env_metadata.json" \
  --summary-out "${RESULTS_DIR}/summary.json" \
  --report-out "${REPORT_FILE}"

log "Benchmark complete! Report written to: ${REPORT_FILE}"
