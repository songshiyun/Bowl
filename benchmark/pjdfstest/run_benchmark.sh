#!/usr/bin/env bash
# ==============================================================================
# Bowl - Aliyun Silicon Valley (us-west-1) Dual-ECS Internal Network pjdfstest
# ==============================================================================
# ADR-0030 / ADR-0031 / ADR-0032：
# - 控制面：PostgreSQL 16 仅维护单张 disks 表；MountDisk 握手原子递增 WriterEpoch。
# - 元数据与并发：每块磁盘独立 StoreActor（单盘 Pebble + 共享 128 MiB Block Cache），
#   双缓冲 MPSC Group Commit 循环（Phase A 内存校验与 WriterEpoch 栅栏 + SetDeferred 零拷贝编码，
#   Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 BlobRef（PackID + Offset + Length）。
# - 数据层：BlobStore 采用扁平 64 MiB Append-Only .pack 封包文件、256 KiB 逻辑块网格、
#   常驻句柄池与尾块分片热缓存（AppendBlocks 整块顺序写跳过用户态冗余缓存拷贝），
#   支持活跃包末尾同块 ExtendTail 顺延扩展、无锁并发 Read、WriteRequest 池化零分配解码、
#   Fsync/FsyncDir 双层并行落盘屏障（脏 .pack fdatasync + Pebble LogData 并发同步）及已封口包自动回收。
# - 客户端：无会话 RawFS FUSE 客户端，禁用 CAP_WRITEBACK_CACHE（保留 FOPEN_KEEP_CACHE 与 CAP_HANDLE_KILLPRIV），
#   配备 WriteBehindCoordinator 顺序写 4 MiB 聚批与 W*=4 并发流水线（2 MiB 同步探测 + <64 KiB 直通写）+
#   ReadAheadCoordinator 顺序预读 + 双端 8 MiB / 32 MiB HTTP/2 流控窗口；Flush 不触发跨机 Fsync RPC。
set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"
REPORT_FILE="${REPORT_FILE:-${SCRIPT_DIR}/REPORT.md}"

# Aliyun Configuration (Silicon Valley us-west-1a, Local NVMe SSD ecs.i5.xlarge)
REGION_ID="${REGION_ID:-us-west-1}"
ZONE_ID="${ZONE_ID:-us-west-1a}"
VPC_ID="${VPC_ID:-vpc-rj9gjwyzzh4v0cn576fhu}"
VSWITCH_ID="${VSWITCH_ID:-vsw-rj9rsvsgpihtsgxej6me5}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:-sg-rj97ly79h6cp3mnfpo2q}"
INSTANCE_TYPE="${INSTANCE_TYPE:-ecs.i5.xlarge}"
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
TELEMETRY_FETCHED=0

mkdir -p "${RESULTS_DIR}"

log() {
  printf "[%s] %s\n" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$*"
}

fetch_telemetry_archive() {
  if [[ "${TELEMETRY_FETCHED:-0}" == "1" ]]; then
    return 0
  fi
  TELEMETRY_FETCHED=1

  local old_opts
  old_opts="$(set +o)"
  set +e

  local stage_dir="${WORK_DIR}/telemetry-${RUN_TS}"
  local archive_path="${RESULTS_DIR}/telemetry-${RUN_TS}.tar.gz"
  mkdir -p "${stage_dir}"

  if [[ -n "${CLIENT_SSH_HOST:-}" && -f "${SSH_KEY_FILE}" ]]; then
    log "Collecting client telemetry from ${CLIENT_SSH_HOST}..."
    ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash -s" <<'EOF_CLI_TELEM' >/dev/null 2>&1
mkdir -p /tmp/bowl-telemetry/client/logs /tmp/bowl-telemetry/client/metrics /tmp/bowl-telemetry/client/diskstats /tmp/bowl-telemetry/client/pprof
[[ -f /var/log/bowl-mount.log ]] && cp -a /var/log/bowl-mount.log /tmp/bowl-telemetry/client/logs/bowl-mount.log || true
curl -fsS --max-time 5 http://127.0.0.1:9567/metrics > /tmp/bowl-telemetry/client/metrics/final.prom 2>/dev/null || true
cat /proc/diskstats > /tmp/bowl-telemetry/client/diskstats/final.txt 2>/dev/null || true
dmesg -T 2>/dev/null | tail -n 200 > /tmp/bowl-telemetry/client/logs/dmesg-tail.log || true
EOF_CLI_TELEM
    if ! ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "tar -czf - -C /tmp/bowl-telemetry client" 2>/dev/null | tar -xzf - -C "${stage_dir}" 2>/dev/null; then
      log "WARN: Failed to stream client telemetry from ${CLIENT_SSH_HOST} (non-fatal)."
    fi
  fi

  if [[ -n "${SERVER_SSH_HOST:-}" && -f "${SSH_KEY_FILE}" ]]; then
    log "Collecting server telemetry from ${SERVER_SSH_HOST}..."
    ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" <<'EOF_SRV_TELEM' >/dev/null 2>&1
mkdir -p /tmp/bowl-telemetry/server/logs /tmp/bowl-telemetry/server/metrics /tmp/bowl-telemetry/server/diskstats /tmp/bowl-telemetry/server/pprof /tmp/bowl-telemetry/server/pebble
journalctl -u bowl-storage --no-pager > /tmp/bowl-telemetry/server/logs/bowl-storage.log 2>/dev/null || true
journalctl -u postgresql -u 'postgresql@*' --no-pager -n 300 > /tmp/bowl-telemetry/server/logs/postgresql.log 2>/dev/null || true
journalctl -k --no-pager 2>/dev/null | tail -n 200 > /tmp/bowl-telemetry/server/logs/kernel.log || true
{ free -m; df -h; } > /tmp/bowl-telemetry/server/logs/server-state.log 2>/dev/null || true
if [[ -d /var/lib/bowl/data ]]; then
  (cd /var/lib/bowl/data && find . -name 'LOG*' -exec cp --parents -a {} /tmp/bowl-telemetry/server/pebble/ \;) 2>/dev/null || true
fi
curl -fsS --max-time 5 http://127.0.0.1:9090/metrics > /tmp/bowl-telemetry/server/metrics/final.prom 2>/dev/null || true
cat /proc/diskstats > /tmp/bowl-telemetry/server/diskstats/final.txt 2>/dev/null || true
EOF_SRV_TELEM
    if ! ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "tar -czf - -C /tmp/bowl-telemetry server" 2>/dev/null | tar -xzf - -C "${stage_dir}" 2>/dev/null; then
      log "WARN: Failed to stream server telemetry from ${SERVER_SSH_HOST} (non-fatal)."
    fi
  fi

  if find "${stage_dir}" -type f 2>/dev/null | grep -q .; then
    if tar -czf "${archive_path}" -C "${WORK_DIR}" "telemetry-${RUN_TS}" 2>/dev/null; then
      log "Telemetry archive saved to ${archive_path}"
    else
      log "WARN: Failed to create telemetry archive ${archive_path} (non-fatal)."
    fi
  else
    log "WARN: No remote telemetry files available to archive (instances may not have initialized)."
  fi

  eval "${old_opts}"
  return 0
}

cleanup() {
  local exit_code=$?
  set +e

  if [[ "${exit_code}" != "0" && -n "${CLIENT_SSH_HOST:-}" && -f "${SSH_KEY_FILE}" ]]; then
    scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/pjdfstest.log" "${RESULTS_DIR}/pjdfstest.log" 2>/dev/null
    scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/stage_timings.jsonl" "${RESULTS_DIR}/stage_timings.jsonl" 2>/dev/null
  fi

  fetch_telemetry_archive

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
  go -C "${REPO_ROOT}/services/storage" build -trimpath \
  -o "${WORK_DIR}/storage-service" ./cmd

GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${REPO_ROOT}/client" build -trimpath \
  -o "${WORK_DIR}/bowl" ./cmd/bowl

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

# Allow Client root SSH into Server over VPC private IP for Server localhost (:9090) telemetry scrapes
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "mkdir -p /root/.ssh && chmod 700 /root/.ssh"
scp "${CLIENT_SSH_OPTS[@]}" "${SSH_KEY_FILE}" "root@${CLIENT_SSH_HOST}:/root/.ssh/id_rsa"
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "chmod 600 /root/.ssh/id_rsa"

# ==============================================================================
# Step 3: Deploy Storage Service + PostgreSQL on Server Node (Local NVMe SSD)
# ==============================================================================
log "Step 3/6: Mounting local NVMe SSD and deploying PostgreSQL, Goose migrations, and storage-service on Server (${SERVER_PRIV_IP})..."
ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "mkdir -p /opt/bowl/migrations"
scp "${SERVER_SSH_OPTS[@]}" "${WORK_DIR}/storage-service" "${WORK_DIR}/goose" "root@${SERVER_SSH_HOST}:/usr/local/bin/"
scp "${SERVER_SSH_OPTS[@]}" "${REPO_ROOT}"/services/storage/db/migrations/*.sql "root@${SERVER_SSH_HOST}:/opt/bowl/migrations/"

ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail

# 1. Detect, format, and mount the 894 GiB local NVMe SSD (local_ssd_pro)
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

mkdir -p /mnt/nvme/postgresql /mnt/nvme/bowl/data /var/lib/postgresql /var/lib/bowl
mount --bind /mnt/nvme/postgresql /var/lib/postgresql
mount --bind /mnt/nvme/bowl /var/lib/bowl

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq postgresql iperf3 fio jq curl ca-certificates

systemctl enable --now postgresql
sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'postgres';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'bowl'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE DATABASE bowl;"
sudo -u postgres psql -c "ALTER SYSTEM SET max_connections = '200';"
sudo -u postgres psql -c "ALTER SYSTEM SET shared_buffers = '2GB';"
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
Environment="INTERNAL_SERVER_HOST=127.0.0.1"
Environment="INTERNAL_SERVER_PORT=9090"
Environment="SERVER_READ_MAX_BYTES=16777216"
Environment="STORAGE_DATA_DIR=/var/lib/bowl/data"
Environment="DB_DSN=postgres://postgres:postgres@127.0.0.1:5432/bowl?sslmode=disable"
Environment="DB_MAX_CONNS=64"
Environment="DB_MIN_CONNS=8"
Environment="STORAGE_ATIME_MODE=strictatime"
Environment="STORAGE_ENTRY_TIMEOUT_SECONDS=0"
Environment="STORAGE_ATTR_TIMEOUT_SECONDS=0"
Environment="STORAGE_DIR_ENTRY_TIMEOUT_SECONDS=0"
Environment="LOG_LEVEL=info"
Environment="OTEL_ENABLED=false"
ExecStart=/usr/local/bin/storage-service
Restart=always
LimitNOFILE=1048576

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
log "Server node (${SERVER_PRIV_IP}:9000) is up and healthy on local NVMe SSD."

# ==============================================================================
# Step 4: Deploy Bowl FUSE Client & Build pjdfstest on Client Node
# ==============================================================================
log "Step 4/6: Preparing Client (${CLIENT_PRIV_IP}), mounting local NVMe SSD, building pjdfstest, and mounting Bowl over VPC internal network..."
scp "${CLIENT_SSH_OPTS[@]}" "${WORK_DIR}/bowl" "root@${CLIENT_SSH_HOST}:/usr/local/bin/bowl"

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail

# Mount local NVMe SSD on Client
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

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq fuse3 libfuse3-dev iperf3 git build-essential autoconf automake libtool pkg-config perl python3 jq curl ca-certificates

grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

if [[ ! -x /opt/pjdfstest/pjdfstest ]]; then
  rm -rf /opt/pjdfstest /mnt/nvme/pjdfstest
  mkdir -p /mnt/nvme/pjdfstest /opt/pjdfstest
  mount --bind /mnt/nvme/pjdfstest /opt/pjdfstest
  git clone --depth 1 https://github.com/pjd/pjdfstest.git /opt/pjdfstest
  cd /opt/pjdfstest
  autoreconf -ifs
  ./configure
  make -j"$(nproc)"
  ln -sf /opt/pjdfstest/pjdfstest /usr/local/bin/pjdfstest
fi
EOF

# Collect server & client metadata + internal VPC network & NVMe baseline
log "Measuring Server local NVMe SSD baseline and internal VPC RTT/throughput (${CLIENT_PRIV_IP} -> ${SERVER_PRIV_IP})..."
SERVER_INFO_JSON="$(ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail
mkdir -p /mnt/nvme/disk_bench
WRITE_BW_BPS="$(fio --name=nvme-seq-w --directory=/mnt/nvme/disk_bench --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1 --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].write.bw_bytes // 0')"
READ_BW_BPS="$(fio --name=nvme-seq-r --directory=/mnt/nvme/disk_bench --rw=read --bs=4M --size=2G --direct=1 --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].read.bw_bytes // 0')"
RANDW_IOPS="$(fio --name=nvme-rand-w --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randwrite --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].write.iops // 0')"
RANDR_IOPS="$(fio --name=nvme-rand-r --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randread --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].read.iops // 0')"
FSYNC_JSON="$(fio --name=nvme-fsync-w --directory=/mnt/nvme/disk_bench --ioengine=psync --rw=write --bs=4k --size=64M --numjobs=1 --direct=1 --fsync=1 --runtime=5 --time_based=1 --output-format=json | sed -n '/^{/,$p')"
FSYNC_IOPS="$(echo "${FSYNC_JSON}" | jq -r '.jobs[0].write.iops // 0')"
FSYNC_P99_US="$(echo "${FSYNC_JSON}" | jq -r '((.jobs[0].sync.lat_ns.percentile["99.000000"] // 0) + (.jobs[0].write.clat_ns.percentile["99.000000"] // 0)) / 1000.0')"
rm -rf /mnt/nvme/disk_bench

WRITE_MIB="$(awk -v b="${WRITE_BW_BPS}" 'BEGIN {printf "%.2f", b / 1048576.0}')"
READ_MIB="$(awk -v b="${READ_BW_BPS}" 'BEGIN {printf "%.2f", b / 1048576.0}')"
RANDW_IOPS_FMT="$(awk -v v="${RANDW_IOPS}" 'BEGIN {printf "%.0f", v}')"
RANDR_IOPS_FMT="$(awk -v v="${RANDR_IOPS}" 'BEGIN {printf "%.0f", v}')"
FSYNC_IOPS_FMT="$(awk -v v="${FSYNC_IOPS}" 'BEGIN {printf "%.0f", v}')"
FSYNC_P99_FMT="$(awk -v v="${FSYNC_P99_US}" 'BEGIN {printf "%.1f", v}')"

NVME_DEV="$(cat /etc/bowl_nvme_dev)"
NVME_SIZE_GIB="$(lsblk -dbno SIZE "${NVME_DEV}" | awk '{printf "%.0f", $1 / (1024*1024*1024)}')"
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
  --arg nvme_dev "${NVME_DEV}" \
  --argjson nvme_gib "${NVME_SIZE_GIB}" \
  --arg disk_w "${WRITE_MIB}" \
  --arg disk_r "${READ_MIB}" \
  --arg randw_iops "${RANDW_IOPS_FMT}" \
  --arg randr_iops "${RANDR_IOPS_FMT}" \
  --arg fsync_iops "${FSYNC_IOPS_FMT}" \
  --arg fsync_p99_us "${FSYNC_P99_FMT}" \
  '{
    server: {cpu_model: $cpu, vcpu: $vcpu, memory_gib: $mem, os_release: $os, kernel: $kernel, pg_version: $pg, data_disk_device: $nvme_dev, data_disk_size_gib: $nvme_gib},
    disk_baseline: {
      local_seq_write_mib_s: ($disk_w|tonumber),
      local_seq_read_mib_s: ($disk_r|tonumber),
      local_randwrite_4k_iops: ($randw_iops|tonumber),
      local_randread_4k_iops: ($randr_iops|tonumber),
      local_fsync_4k_iops: ($fsync_iops|tonumber),
      local_fsync_4k_p99_us: ($fsync_p99_us|tonumber)
    }
  }'
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

# ADR-0029 中每个磁盘的根 Inode 固定为 1。
CREATE_JSON="$(/usr/local/bin/bowl disk create --size 2PiB --name "pjdfstest-disk" --endpoint "${ENDPOINT}" --json)"
DISK_ID="$(echo "${CREATE_JSON}" | jq -r '.disk_id')"
ROOT_INO="$(echo "${CREATE_JSON}" | jq -r '.root_ino')"

mkdir -p /mnt/bowl
if grep -q " /mnt/bowl " /proc/self/mountinfo; then
  /usr/local/bin/bowl umount /mnt/bowl || umount -l /mnt/bowl || true
fi
/usr/local/bin/bowl mount "${DISK_ID}" /mnt/bowl \
  --endpoint "${ENDPOINT}" \
  --allow-other \
  --admin-addr 127.0.0.1:9567 \
  --log-level info \
  --log-file /var/log/bowl-mount.log
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

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' python3 -u -" <<'PYEOF'
import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

tests_root = Path("/opt/pjdfstest/tests")
work_dir = Path("/mnt/bowl/work")
work_dir.mkdir(parents=True, exist_ok=True)

log_path = Path("/tmp/pjdfstest.log")
timings_path = Path("/tmp/stage_timings.jsonl")
server_priv_ip = os.environ.get("SERVER_PRIV_IP", "")

cli_telem = Path("/tmp/bowl-telemetry/client")
for sub in ("logs", "metrics", "diskstats", "pprof"):
    (cli_telem / sub).mkdir(parents=True, exist_ok=True)

ssh_srv_base = [
    "ssh",
    "-n",
    "-o", "StrictHostKeyChecking=no",
    "-o", "UserKnownHostsFile=/dev/null",
    "-o", "IdentitiesOnly=yes",
    "-i", "/root/.ssh/id_rsa",
    f"root@{server_priv_ip}",
]


def capture_snapshot(phase: str) -> None:
    subprocess.run(
        ["sh", "-c", f"curl -fsS --max-time 5 http://127.0.0.1:9567/metrics > '{cli_telem}/metrics/{phase}.prom' 2>/dev/null || true"],
        check=False,
    )
    try:
        shutil.copyfile("/proc/diskstats", cli_telem / "diskstats" / f"{phase}.txt")
    except OSError:
        pass
    if server_priv_ip:
        subprocess.run(
            ssh_srv_base + [
                "mkdir -p /tmp/bowl-telemetry/server/metrics /tmp/bowl-telemetry/server/diskstats /tmp/bowl-telemetry/server/pprof /tmp/bowl-telemetry/server/pebble && "
                f"curl -fsS --max-time 5 http://127.0.0.1:9090/metrics > '/tmp/bowl-telemetry/server/metrics/{phase}.prom' 2>/dev/null || true; "
                f"cat /proc/diskstats > '/tmp/bowl-telemetry/server/diskstats/{phase}.txt' 2>/dev/null || true"
            ],
            check=False,
        )


def start_cpu_profile(stage: str, sec: int = 10) -> list[subprocess.Popen]:
    procs: list[subprocess.Popen] = []
    procs.append(
        subprocess.Popen(
            ["sh", "-c", f"curl -fsS --max-time 25 'http://127.0.0.1:9567/debug/pprof/profile?seconds={sec}' -o '{cli_telem}/pprof/{stage}_cpu.pb.gz' 2>/dev/null || true"]
        )
    )
    if server_priv_ip:
        procs.append(
            subprocess.Popen(
                ssh_srv_base + [
                    f"mkdir -p /tmp/bowl-telemetry/server/pprof && curl -fsS --max-time 25 'http://127.0.0.1:9090/debug/pprof/profile?seconds={sec}' -o '/tmp/bowl-telemetry/server/pprof/{stage}_cpu.pb.gz' 2>/dev/null || true"
                ]
            )
        )
    return procs


def finish_stage_profiles(stage: str, procs: list[subprocess.Popen]) -> None:
    for p in procs:
        try:
            p.wait(timeout=30)
        except subprocess.TimeoutExpired:
            p.kill()
    subprocess.run(
        ["sh", "-c", f"curl -fsS --max-time 10 'http://127.0.0.1:9567/debug/pprof/heap' -o '{cli_telem}/pprof/{stage}_heap.pb.gz' 2>/dev/null || true; curl -fsS --max-time 10 'http://127.0.0.1:9567/debug/pprof/mutex' -o '{cli_telem}/pprof/{stage}_mutex.pb.gz' 2>/dev/null || true"],
        check=False,
    )
    if server_priv_ip:
        subprocess.run(
            ssh_srv_base + [
                f"mkdir -p /tmp/bowl-telemetry/server/pprof && curl -fsS --max-time 10 'http://127.0.0.1:9090/debug/pprof/heap' -o '/tmp/bowl-telemetry/server/pprof/{stage}_heap.pb.gz' 2>/dev/null || true; curl -fsS --max-time 10 'http://127.0.0.1:9090/debug/pprof/mutex' -o '/tmp/bowl-telemetry/server/pprof/{stage}_mutex.pb.gz' 2>/dev/null || true"
            ],
            check=False,
        )


env = os.environ.copy()
env["PATH"] = f"/opt/pjdfstest:/usr/local/bin:/usr/bin:/bin:{env.get('PATH', '')}"

subdirs = sorted([d.name for d in tests_root.iterdir() if d.is_dir()])
print(f"[pjdfstest-runner] Discovered {len(subdirs)} syscall directories: {', '.join(subdirs)}", flush=True)

capture_snapshot("suite_pre")
slow_dirs = {"chown", "rename", "unlink"}

with log_path.open("w", encoding="utf-8") as log_f, timings_path.open("w", encoding="utf-8") as tim_f:
    for idx, dname in enumerate(subdirs, 1):
        target_dir = tests_root / dname
        if dname in slow_dirs:
            capture_snapshot(f"{dname}_pre")
        prof_procs = start_cpu_profile(dname, 10) if dname == "chown" else []
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
        if dname == "chown":
            finish_stage_profiles(dname, prof_procs)
        if dname in slow_dirs:
            capture_snapshot(f"{dname}_post")
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

capture_snapshot("suite_post")
PYEOF

TEST_COMPLETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
log "pjdfstest execution finished at ${TEST_COMPLETED_AT}."

# Unmount cleanly and fetch logs
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "/usr/local/bin/bowl umount /mnt/bowl || umount /mnt/bowl || true"
ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "systemctl stop bowl-storage 2>/dev/null || true"
scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/pjdfstest.log" "${RESULTS_DIR}/pjdfstest.log"
scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/stage_timings.jsonl" "${RESULTS_DIR}/stage_timings.jsonl"
fetch_telemetry_archive

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
  if [[ "${REMAINING_INSTANCES}" != "0" ]]; then
    log "ERROR: 仍有 ${REMAINING_INSTANCES} 台测试实例，清理钩子将继续尝试。"
    exit 1
  fi
  rm -f "${CREATED_INSTANCES_FILE}"

  aliyun ecs DeleteKeyPairs --RegionId "${REGION_ID}" --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null
  KEY_CREATED=0
fi

BOWL_GIT_COMMIT="$(git -C "${REPO_ROOT}" rev-parse --short HEAD)"
if [[ -n "$(git -C "${REPO_ROOT}" status --porcelain -- services client 2>/dev/null)" ]]; then
  BOWL_GIT_COMMIT="${BOWL_GIT_COMMIT}-dirty"
fi

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
      system_disk_size_gib: $disk_size,
      data_disk_category: "local_ssd_pro (NVMe)",
      data_disk_size_gib: $srv.server.data_disk_size_gib
    },
    server_node: ($srv.server + {instance_id: $server_id, private_ip: $server_priv_ip}),
    server_disk_baseline: $srv.disk_baseline,
    client_node: ($cli.client + {instance_id: $client_id, private_ip: $client_priv_ip}),
    network_baseline: $cli.network,
    bowl_config: ($cli.bowl + {
      bowl_git_commit: $bowl_commit,
      storage_engine: "StoreActor per disk (Pebble + 128 MiB 共享缓存 + 双缓冲 MPSC Group Commit + 16B BlobRef + WriteRequest 池化零分配解码)",
      blob_layout: "packs/<pack_id_hex16>.pack (64 MiB Append-Only Pack + 256 KiB 块网格 + 尾块分片热缓存 + AppendBlocks 零冗余拷贝 + ExtendTail)",
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
      concurrency_model: "单盘独立 StoreActor 串行 Phase A 内存校验 + WriterEpoch 世代号栅栏 + 无锁并发 Read",
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
  --results-dir "${RESULTS_DIR}" \
  --summary-out "${RESULTS_DIR}/summary.json" \
  --report-out "${REPORT_FILE}"

log "Benchmark complete! Report written to: ${REPORT_FILE}"
