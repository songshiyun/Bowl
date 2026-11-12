#!/usr/bin/env bash
# ==============================================================================
# Bowl vs Drive9 — Aliyun Silicon Valley (us-west-1) Dual-ECS Benchmark Suite
# ==============================================================================
# Topology:
#   - Node 1 (Server): ecs.i5.xlarge (4 vCPU, 32 GiB, 894 GiB local_ssd_pro NVMe SSD)
#       Runs Bowl `deploy/prod` Docker Compose Stack (PostgreSQL 18 + Storage :9000 + Jaeger + Prometheus + Loki + Alloy + Grafana)
#       Runs TiDB v8.5.5 (Unistore, :4000) + Drive9 Server (:9009, local S3)
#   - Node 2 (Client): ecs.i5.xlarge (4 vCPU, 32 GiB, 894 GiB local_ssd_pro NVMe SSD)
#       Runs Linux FUSE Clients over VPC Private Network:
#         1. Bowl（deploy/prod 栈：PG 18 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack，noatime + 1s 缓存 + OTLP 全链路遥测）
#         2. Drive9 Default (--profile=coding-agent --durability=interactive)
#         3. Drive9 Remote Sync (--profile=none --durability=close-sync, 1s cache)
#       Executes:
#         - Suite 1: `juicefs bench -p 4`
#         - Suite 2: `drive9_fuse_bench.py` (Cold & Hot cache, 10 micro + 5 macro)
#         - Suite 3: `fuse_perf_baseline.py` (SHA-256 I/O + SQLite DELETE & WAL)
# ADR-0030 / ADR-0031 / ADR-0032 + deploy/prod 生产编排栈：
# - 控制面与可观测性：默认基于 deploy/prod/docker-compose.yaml + deploy/prod/storage.env 部署 PostgreSQL 18 控制面、
#   storage-service 及 Jaeger (OTLP 默认 10% + 慢链路分层抓取) + Prometheus (2s 双端抓取) + Loki + Alloy + Grafana。
# - 元数据与并发：每块磁盘独立 StoreActor（单盘 Pebble + 共享 128 MiB Block Cache），
#   双缓冲 MPSC Group Commit 循环（Phase A 内存校验与 WriterEpoch 栅栏 + SetDeferred 零拷贝编码，
#   Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 BlobRef（PackID + Offset + Length）。
# - 数据层：BlobStore 采用扁平 64 MiB Append-Only .pack 封包文件、256 KiB 逻辑块网格、
#   常驻句柄池与 64 MiB 分片热块缓存（AppendBlocks 整块顺序写跳过用户态冗余缓存拷贝），
#   支持活跃包末尾同块 ExtendTail 顺延扩展、8 MiB SyncFileRange 渐进异步回写、相邻物理连续块合并单次 pread 无锁并发 Read、
#   WriteRequest/ReadResponse 池化零分配编解码、服务端 128 MiB H2C 连接窗口、
#   Fsync/FsyncDir 双层并行落盘屏障（脏 .pack fdatasync + Pebble LogData 并发同步）及后台异步全死封口包回收。
# - 客户端：无会话 RawFS FUSE 客户端，禁用 CAP_WRITEBACK_CACHE（保留 FOPEN_KEEP_CACHE 与 CAP_HANDLE_KILLPRIV），
#   配备 1+8 H2C 多连接条带池（32 MiB 元数据连接 + 8×64 MiB 数据连接，单流 8 MiB）与 MarshalAppend 零分配编码、
#   WriteBehindCoordinator（4 MiB × W*=4 并发写流水线，2 MiB 同步探测 + Create 预热与 <=256 KiB 小块连续写内存聚合）、
#   ReadAheadCoordinator 单飞有序槽位预读（W_read=4, ReadAheadMaxGlobalInflight=32）与 completeDirs 本地目录流合成；Flush 不触发跨机 Fsync RPC。
# ==============================================================================

set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
DRIVE9_REPO="${DRIVE9_REPO:-${REPO_ROOT}/../drive9}"
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
KEY_PAIR_NAME="bowl-d9bench-kp-${RUN_TS}"
WORK_DIR="$(mktemp -d "/tmp/bowl-d9bench-${RUN_TS}-XXXXXX")"
KEY_FILE="${WORK_DIR}/id_rsa"

SERVER_INSTANCE_ID=""
CLIENT_INSTANCE_ID=""
INSTANCES_CREATED_AT=""
INSTANCES_DELETED_AT=""
TELEMETRY_FETCHED=0

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

TEST_START_EPOCH=""
TEST_END_EPOCH=""

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
    "drive9" \
    "${TEST_START_EPOCH:-}" \
    "${TEST_END_EPOCH:-}"

  eval "${old_opts}"
  return 0
}

cleanup() {
  local exit_code=$?
  set +e
  log "=== Triggering Cloud Resource Cleanup Trap (exit_code=${exit_code}) ==="

  # 失败时先回传已有输出，保留退出原因，再释放按量资源。
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
# Step 1: Cross-compile Bowl & Drive9 binaries for linux/amd64
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

log "Cross-compiling Drive9 binaries (drive9-server, drive9) from ${DRIVE9_REPO} for linux/amd64..."
DRIVE9_GO_TAGS="nogspt,noredis,nosqlite,nomysql,nopg,notikv,nobadger,noetcd,nocos,nobos,nohdfs,noibmcos,noobs,nooss,noqingstor,nosftp,noswift,noazure,nogs,noufile,nob2,nonfs,nodragonfly,nocifs,nostorj,noqiniu,notos,noks3,nowebdav,nogateway"
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${DRIVE9_REPO}" build -tags "${DRIVE9_GO_TAGS}" -trimpath -ldflags="-s -w" -o "${WORK_DIR}/drive9-server" ./cmd/drive9-server

CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
  go -C "${DRIVE9_REPO}" build -tags "${DRIVE9_GO_TAGS}" -trimpath -ldflags="-s -w" -o "${WORK_DIR}/drive9" ./cmd/drive9

chmod +x "${WORK_DIR}/drive9-server" "${WORK_DIR}/drive9"
DRIVE9_GIT_COMMIT="$(git -C "${DRIVE9_REPO}" rev-parse --short HEAD 2>/dev/null || echo 'unknown')"

cp "${SCRIPT_DIR}/drive9_fuse_bench.py" "${WORK_DIR}/drive9_fuse_bench.py"
cp "${SCRIPT_DIR}/fuse_perf_baseline.py" "${WORK_DIR}/fuse_perf_baseline.py"

prepare_prod_bundle "${REPO_ROOT}" "${WORK_DIR}"
COPYFILE_DISABLE=1 tar -czf "${WORK_DIR}/server-bins.tar.gz" -C "${WORK_DIR}" drive9-server
COPYFILE_DISABLE=1 tar -czf "${WORK_DIR}/client-bins.tar.gz" -C "${WORK_DIR}" bowl drive9 drive9_fuse_bench.py fuse_perf_baseline.py

STAGE_END=$(date +%s)
record_stage "cross_compile_binaries" "$((STAGE_END - STAGE_START))"
log "Binaries compiled & packaged (Bowl=${BOWL_GIT_COMMIT}, Drive9=${DRIVE9_GIT_COMMIT})."

# ------------------------------------------------------------------------------
# Step 2: Create SSH KeyPair & Provision 2 ECS Instances (Server + Client)
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
  --InstanceName "bowl-d9bench" \
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

# Allow Client root SSH into Server over VPC private IP for dual-node drop_caches and service isolation
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "mkdir -p /root/.ssh && chmod 700 /root/.ssh"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" "${KEY_FILE}" "root@${CLIENT_PUB_IP}:/root/.ssh/id_rsa"
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "chmod 600 /root/.ssh/id_rsa"

STAGE_END=$(date +%s)
record_stage "provision_ecs_instances" "$((STAGE_END - STAGE_START))"

# ------------------------------------------------------------------------------
# Step 3: Deploy Server Node (Local NVMe SSD + Bowl deploy/prod Stack + TiDB v8.5.5 + Drive9 Server)
# ------------------------------------------------------------------------------
STAGE_START=$(date +%s)
log "Uploading prod-bundle.tar.gz and server-bins.tar.gz to Server (${SERVER_PUB_IP})..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "mkdir -p /opt/bowl /opt/drive9"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" \
  "${WORK_DIR}/prod-bundle.tar.gz" \
  "root@${SERVER_PUB_IP}:/tmp/prod-bundle.tar.gz"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" \
  "${WORK_DIR}/server-bins.tar.gz" \
  "root@${SERVER_PUB_IP}:/tmp/server-bins.tar.gz"

log "Mounting local NVMe SSD and configuring TiDB v8.5.5 + Drive9 Server on Server node..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' bash -s" <<'EOF_SERVER'
set -euo pipefail

# 0. Detect, format, and mount the 894 GiB local NVMe SSD (local_ssd_pro)
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

mkdir -p /mnt/nvme/drive9/s3 /mnt/nvme/tidb
mkdir -p /var/lib/drive9 /var/lib/tidb
mount --bind /mnt/nvme/drive9 /var/lib/drive9
mount --bind /mnt/nvme/tidb /var/lib/tidb

tar -xzf /tmp/server-bins.tar.gz -C /usr/local/bin/
chmod +x /usr/local/bin/drive9-server
rm -f /tmp/server-bins.tar.gz

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq default-mysql-client iperf3 fio python3 jq curl ca-certificates >/dev/null

# Measure Server local NVMe SSD disk baseline before starting heavy background daemons
mkdir -p /mnt/nvme/disk_bench
fio --name=nvme_seq_write --directory=/mnt/nvme/disk_bench --rw=write --bs=4M --size=2G --numjobs=1 --direct=1 --end_fsync=1 --group_reporting --output-format=json > /tmp/server_disk_write.json
fio --name=nvme_seq_read --directory=/mnt/nvme/disk_bench --rw=read --bs=4M --size=2G --numjobs=1 --direct=1 --group_reporting --output-format=json > /tmp/server_disk_read.json
fio --name=nvme_rand_write --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randwrite --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json > /tmp/server_disk_randw.json
fio --name=nvme_rand_read --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randread --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --group_reporting --output-format=json > /tmp/server_disk_randr.json
fio --name=nvme_fsync_write --directory=/mnt/nvme/disk_bench --ioengine=psync --rw=write --bs=4k --size=64M --numjobs=1 --direct=1 --fsync=1 --runtime=5 --time_based=1 --group_reporting --output-format=json > /tmp/server_disk_fsync.json
rm -rf /mnt/nvme/disk_bench

# 1. Install TiDB v8.5.5 via TiUP and run Unistore on :4000
if [ ! -x /root/.tiup/bin/tiup ]; then
  curl --proto '=https' --tlsv1.2 -sSf --retry 5 --retry-delay 2 --retry-all-errors https://tiup-mirrors.pingcap.com/install.sh | sh
fi
export PATH="/root/.tiup/bin:$PATH"
tiup telemetry disable || true
tiup install tidb:v8.5.5
TIDB_BIN="/root/.tiup/components/tidb/v8.5.5/tidb-server"

cat > /etc/systemd/system/tidb.service <<UNIT
[Unit]
Description=TiDB Unistore Server for Drive9
After=network.target

[Service]
Type=simple
User=root
LimitNOFILE=1048576
ExecStart=${TIDB_BIN} --store=unistore --path=/var/lib/tidb -P 4000 --status=10080
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now tidb.service

# Wait for TiDB SQL readiness and create drive9_local database
for _ in $(seq 1 60); do
  if curl -sf http://127.0.0.1:10080/status >/dev/null 2>&1 \
    && mysql --protocol=tcp -h 127.0.0.1 -P 4000 -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;" >/dev/null 2>&1; then
    break
  fi
  sleep 2
done
mysql --protocol=tcp -h 127.0.0.1 -P 4000 -u root -e "CREATE DATABASE IF NOT EXISTS drive9_local;"

# 2. Start Drive9 Server on :9009
cat > /etc/systemd/system/drive9-server.service <<UNIT
[Unit]
Description=Drive9 Server (Local Tenant Provider + Local S3)
After=network.target tidb.service

[Service]
Type=simple
User=root
LimitNOFILE=1048576
Environment="DRIVE9_TENANT_PROVIDER=local"
Environment="DRIVE9_LISTEN_ADDR=0.0.0.0:9009"
Environment="DRIVE9_PUBLIC_URL=http://${SERVER_PRIV_IP}:9009"
Environment="DRIVE9_LOCAL_DSN=root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
Environment="DRIVE9_META_DSN=root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
Environment="DRIVE9_LOCAL_MYSQL_DSN=root@tcp(127.0.0.1:4000)/drive9_local?parseTime=true"
Environment="DRIVE9_S3_DIR=/var/lib/drive9/s3"
Environment="DRIVE9_DEFAULT_STORAGE_QUOTA_BYTES=2199023255552"
Environment="DRIVE9_TOKEN_SIGNING_KEY=a1b2c3d4e5f60718293a4b5c6d7e8f90a1b2c3d4e5f60718293a4b5c6d7e8f90"
Environment="DRIVE9_MASTER_KEY=f0e1d2c3b4a5968778695a4b3c2d1e0ff0e1d2c3b4a5968778695a4b3c2d1e0f"
Environment="DRIVE9_DISABLE_AUTO_EMBEDDING=true"
Environment="DRIVE9_LOCAL_EMBEDDING_MODE=app"
Environment="DRIVE9_LOG_LEVEL=warn"
ExecStart=/usr/local/bin/drive9-server
Restart=always
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

# 3. Start iperf3 daemon for VPC network baseline
cat > /etc/systemd/system/iperf3-bench.service <<'UNIT'
[Unit]
Description=iperf3 server for VPC baseline
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/iperf3 -s
Restart=always

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now drive9-server.service iperf3-bench.service

for _ in $(seq 1 30); do
  if curl -sf http://127.0.0.1:9009/healthz >/dev/null; then
    break
  fi
  sleep 1
done
curl -sf http://127.0.0.1:9009/healthz >/dev/null
EOF_SERVER

log "Initializing Bowl deploy/prod Docker Compose stack on Server (${SERVER_PRIV_IP})..."
remote_setup_prod_stack "${SERVER_PRIV_IP}" "${CLIENT_PRIV_IP}" "prod" \
  | ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "bash -s"
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "/usr/local/bin/bowl-prod-ctl up"
log "Bowl deploy/prod (:9000 + Jaeger/Prometheus/Loki) and Drive9 Server (:9009) are healthy on local NVMe SSD."

STAGE_END=$(date +%s)
record_stage "deploy_server_node" "$((STAGE_END - STAGE_START))"

# ------------------------------------------------------------------------------
# Step 4: Configure Client Node (Local NVMe SSD) & Mount Bowl + Drive9 (Default & Sync)
# ------------------------------------------------------------------------------
STAGE_START=$(date +%s)
log "Uploading client-bins.tar.gz to Client (${CLIENT_PUB_IP})..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "mkdir -p /opt/bench /tmp/bench_results /mnt/bowl /mnt/drive9_default /mnt/drive9_sync"
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" \
  "${WORK_DIR}/client-bins.tar.gz" \
  "root@${CLIENT_PUB_IP}:/tmp/client-bins.tar.gz"

emit_client_telemetry_helper | ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "mkdir -p /opt/bowl && cat >/opt/bowl/client_telemetry.sh && chmod +x /opt/bowl/client_telemetry.sh"

log "Mounting local NVMe SSD, installing benchmark tools, and mounting Bowl + Drive9 on Client..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' BOWL_BENCH_LOG_LEVEL='${BOWL_BENCH_LOG_LEVEL}' BOWL_BENCH_TRACE_SAMPLE_RATIO='${BOWL_BENCH_TRACE_SAMPLE_RATIO}' bash -s" <<'EOF_CLIENT_SETUP'
set -euo pipefail

# 0. Detect, format, and mount the 894 GiB local NVMe SSD on Client
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

mkdir -p /mnt/nvme/cache_drive9_default /mnt/nvme/cache_drive9_sync /mnt/nvme/bench_tmp
mkdir -p /var/cache/drive9_default /var/cache/drive9_sync /opt/bench_tmp
mount --bind /mnt/nvme/cache_drive9_default /var/cache/drive9_default
mount --bind /mnt/nvme/cache_drive9_sync /var/cache/drive9_sync
mount --bind /mnt/nvme/bench_tmp /opt/bench_tmp

tar -xzf /tmp/client-bins.tar.gz -C /tmp/
mv /tmp/bowl /tmp/drive9 /usr/local/bin/
mv /tmp/drive9_fuse_bench.py /tmp/fuse_perf_baseline.py /opt/bench/
chmod +x /usr/local/bin/bowl /usr/local/bin/drive9 /opt/bench/drive9_fuse_bench.py /opt/bench/fuse_perf_baseline.py
rm -f /tmp/client-bins.tar.gz

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq fuse3 git golang-go sqlite3 python3 iperf3 jq curl ca-certificates util-linux >/dev/null

# Pre-warm host Go standard library build cache (/root/.cache/go-build) on /tmp so
# the first measured target does not pay one-off stdlib compilation cost alone.
GO_WARMUP_DIR="$(mktemp -d /tmp/go-build-warmup.XXXXXX)"
printf 'module drive9bench\n\ngo 1.22\n' > "${GO_WARMUP_DIR}/go.mod"
printf 'package main\n\nfunc main() {}\n' > "${GO_WARMUP_DIR}/main.go"
(cd "${GO_WARMUP_DIR}" && go build -o /dev/null ./...)
rm -rf "${GO_WARMUP_DIR}"

grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

# Install JuiceFS v1.2.1 Linux amd64 binary for `juicefs bench`
if [[ ! -x /usr/local/bin/juicefs ]]; then
  curl -fsSL --retry 5 --retry-delay 2 --retry-all-errors https://github.com/juicedata/juicefs/releases/download/v1.2.1/juicefs-1.2.1-linux-amd64.tar.gz -o /tmp/juicefs.tar.gz
  tar -xzf /tmp/juicefs.tar.gz -C /tmp juicefs
  mv /tmp/juicefs /usr/local/bin/juicefs
  chmod +x /usr/local/bin/juicefs
  rm -f /tmp/juicefs.tar.gz
fi
/usr/local/bin/juicefs --version

# 1. Create Bowl Disk & Mount (/mnt/bowl) with full OTLP tracing & Prometheus/:9567 admin endpoint
ENDPOINT="http://${SERVER_PRIV_IP}:9000"
CREATE_JSON="$(/usr/local/bin/bowl disk create --size 2PiB --name "d9-bench-disk" --endpoint "${ENDPOINT}" --json)"
DISK_ID="$(echo "${CREATE_JSON}" | jq -r '.disk_id')"
echo "${CREATE_JSON}" > /tmp/bench_results/bowl_disk.json
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

# 2. Provision two isolated Drive9 tenants (one for drive9_default, one for drive9_sync)
provision_drive9_tenant() {
  local api_key
  api_key=$(curl -sf -X POST "http://${SERVER_PRIV_IP}:9009/v1/provision" | jq -r '.api_key')
  if [[ -z "${api_key}" || "${api_key}" == "null" ]]; then
    echo "Failed to provision Drive9 tenant" >&2
    exit 1
  fi
  for _ in $(seq 1 60); do
    local st
    st=$(curl -sf -H "Authorization: Bearer ${api_key}" "http://${SERVER_PRIV_IP}:9009/v1/status" | jq -r '.status // empty')
    if [[ "${st}" == "active" ]]; then
      echo "${api_key}"
      return 0
    fi
    sleep 1
  done
  echo "Timed out waiting for Drive9 tenant ${api_key} to become active" >&2
  exit 1
}

API_KEY_DEFAULT="$(provision_drive9_tenant)"
API_KEY_SYNC="$(provision_drive9_tenant)"
echo "${API_KEY_DEFAULT}" > /opt/bench/api_key_default.txt
echo "${API_KEY_SYNC}" > /opt/bench/api_key_sync.txt

# 3. Mount Drive9 Default (/mnt/drive9_default: --profile=coding-agent --durability=interactive)
cat > /etc/systemd/system/drive9-default-fuse.service <<UNIT
[Unit]
Description=Drive9 Default Mode FUSE Mount (coding-agent + interactive)
After=network.target

[Service]
Type=simple
User=root
LimitNOFILE=1048576
Environment="TMPDIR=/opt/bench_tmp"
Environment="DRIVE9_SERVER=http://${SERVER_PRIV_IP}:9009"
Environment="DRIVE9_API_KEY=${API_KEY_DEFAULT}"
ExecStart=/usr/local/bin/drive9 mount --mode=fuse --foreground --profile=coding-agent --durability=interactive --write-cache-size-mb=0 --cache-dir=/var/cache/drive9_default /mnt/drive9_default
ExecStop=/bin/fusermount3 -u /mnt/drive9_default
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

# 4. Mount Drive9 Sync (/mnt/drive9_sync: --profile=none --durability=close-sync)
cat > /etc/systemd/system/drive9-sync-fuse.service <<UNIT
[Unit]
Description=Drive9 Remote Sync Mode FUSE Mount (profile=none + close-sync)
After=network.target

[Service]
Type=simple
User=root
LimitNOFILE=1048576
Environment="TMPDIR=/opt/bench_tmp"
Environment="DRIVE9_SERVER=http://${SERVER_PRIV_IP}:9009"
Environment="DRIVE9_API_KEY=${API_KEY_SYNC}"
ExecStart=/usr/local/bin/drive9 mount --mode=fuse --foreground --profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s --cache-dir=/var/cache/drive9_sync /mnt/drive9_sync
ExecStop=/bin/fusermount3 -u /mnt/drive9_sync
Restart=on-failure
RestartSec=2

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now drive9-default-fuse.service drive9-sync-fuse.service

for mp in /mnt/bowl /mnt/drive9_default /mnt/drive9_sync; do
  for _ in $(seq 1 30); do
    if mountpoint -q "${mp}"; then
      echo "${mp} is mounted."
      break
    fi
    sleep 1
  done
  if ! mountpoint -q "${mp}"; then
    echo "ERROR: ${mp} failed to mount" >&2
    journalctl -u drive9-default-fuse.service -u drive9-sync-fuse.service -n 50 --no-pager || true
    exit 1
  fi
done

df -h /mnt/bowl /mnt/drive9_default /mnt/drive9_sync
EOF_CLIENT_SETUP

STAGE_END=$(date +%s)
record_stage "setup_client_and_mounts" "$((STAGE_END - STAGE_START))"

# ------------------------------------------------------------------------------
# Step 5: Collect Environment & Network Baseline Metadata
# ------------------------------------------------------------------------------
log "Collecting VPC network RTT & bandwidth baselines..."
PING_STATS=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "ping -c 20 -i 0.05 ${SERVER_PRIV_IP} | tail -n 1")
IPERF_GBPS=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "iperf3 -c ${SERVER_PRIV_IP} -t 5 -J | jq -r '.end.sum_received.bits_per_second / 1e9'")

SERVER_META=$(ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "bash -s" <<'EOF_SMETA'
CPU_MODEL=$(awk -F: '/model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}' /proc/cpuinfo)
VCPU=$(nproc)
MEM_GIB=$(free -h | awk '/^Mem:/ {print $2}')
KERNEL=$(uname -r)
OS_REL=$(. /etc/os-release && echo "${PRETTY_NAME}")
PG_VER=$(/usr/local/bin/bowl-prod-ctl pg-version 2>/dev/null | head -n 1 || echo "postgres (PostgreSQL) 18 (alpine)")
DOCKER_VER=$(docker --version 2>/dev/null | head -n 1 || echo "n/a")
TIDB_VER=$(/root/.tiup/components/tidb/v8.5.5/tidb-server -V | awk -F: '/Release Version/ {gsub(/^[ \t]+/, "", $2); print $2; exit}')
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
  --arg tidb "${TIDB_VER}" \
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
    tidb_version: $tidb,
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
JFS_VER=$(/usr/local/bin/juicefs --version | head -n 1)
GO_VER=$(go version)
SQLITE_VER=$(sqlite3 --version | awk '{print $1}')
jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg kernel "${KERNEL}" \
  --arg os "${OS_REL}" \
  --arg fuse "${FUSE_VER}" \
  --arg jfs "${JFS_VER}" \
  --arg go "${GO_VER}" \
  --arg sqlite "${SQLITE_VER}" \
  '{cpu_model: $cpu, vcpu: ($vcpu|tonumber), memory_gib: $mem, kernel: $kernel, os_release: $os, fusermount_version: $fuse, juicefs_version: $jfs, go_version: $go, sqlite_version: $sqlite}'
EOF_CMETA
)

# ------------------------------------------------------------------------------
# Step 6: Execute Benchmark Suites (1: juicefs bench, 2: drive9_fuse_bench, 3: fuse_perf_baseline)
# 为确保 Bowl 全链路遥测（Jaeger 内存 Trace 与 Prometheus 计数器）零重启连续完整，且 TEST_START/END_EPOCH
# 严格仅包裹 Bowl 压测窗口：先在停用 TiDB/Drive9 的纯净环境下连续跑完 Bowl 的 Suite 1/2/3 并完成卸载前快照与
# 遥测导出，随后执行 bowl-prod-ctl stop-storage（仅停业务容器，不动可观测性栈），再运行 Drive9 Default & Sync。
# ------------------------------------------------------------------------------
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "mkdir -p /tmp/bench_results"
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "systemctl stop drive9-server tidb 2>/dev/null || true; /usr/local/bin/bowl-prod-ctl up; date +%s > /var/run/bowl-prod-started.epoch"
TEST_START_EPOCH="$(date +%s)"

run_juicefs_bench_for_target() {
  local target_key="$1"
  local mountpoint="$2"
  local s_start
  s_start=$(date +%s)
  log "=== [Suite 1: juicefs bench -p 4] Target: ${target_key} (${mountpoint}) ==="
  ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "TARGET_KEY='${target_key}' MP='${mountpoint}' SERVER_PRIV_IP='${SERVER_PRIV_IP}' bash -s" <<'EOF_JBENCH'
set -euo pipefail
export TMPDIR=/opt/bench_tmp
SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa "root@${SERVER_PRIV_IP}")
source /opt/bowl/client_telemetry.sh
rm -rf "${MP:?}"/*
sync && echo 3 > /proc/sys/vm/drop_caches || true
"${SSH_SRV[@]}" "sync && echo 3 > /proc/sys/vm/drop_caches" || true
[[ "${TARGET_KEY}" == "bowl" ]] && capture_bowl_snapshot "jfs_bench_pre"
/usr/local/bin/juicefs bench "${MP}" -p 4 2>&1 | tee "/tmp/bench_results/bench_${TARGET_KEY}.log"
[[ "${TARGET_KEY}" == "bowl" ]] && capture_bowl_snapshot "jfs_bench_post"
if [[ "${TARGET_KEY}" == "drive9_default" ]]; then
  DRIVE9_SERVER="http://${SERVER_PRIV_IP}:9009" DRIVE9_API_KEY="$(cat /opt/bench/api_key_default.txt)" \
    /usr/local/bin/drive9 mount drain --timeout 300s "${MP}" || true
fi
rm -rf "${MP:?}"/*
sync
EOF_JBENCH
  local s_end
  s_end=$(date +%s)
  if [[ "${target_key}" == "bowl" ]]; then
    record_stage "jfs_bench" "$((s_end - s_start))"
  else
    record_stage "${target_key}_juicefs_bench" "$((s_end - s_start))"
  fi
}

run_perf_baseline_for_target() {
  local target_key="$1"
  local mountpoint="$2"
  local s_start
  s_start=$(date +%s)
  log "=== [Suite 3: fuse_perf_baseline.py] Target: ${target_key} (${mountpoint}) ==="
  ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "TARGET_KEY='${target_key}' MP='${mountpoint}' SERVER_PRIV_IP='${SERVER_PRIV_IP}' bash -s" <<'EOF_PBASE'
set -euo pipefail
export TMPDIR=/opt/bench_tmp
SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa "root@${SERVER_PRIV_IP}")
source /opt/bowl/client_telemetry.sh
rm -rf "${MP:?}"/*
sync && echo 3 > /proc/sys/vm/drop_caches || true
"${SSH_SRV[@]}" "sync && echo 3 > /proc/sys/vm/drop_caches" || true
if [[ "${TARGET_KEY}" == "bowl" ]]; then
  capture_bowl_snapshot "fuse_perf_baseline_pre"
  start_bowl_cpu_profile "sqlite_wal" 10 2
fi
python3 /opt/bench/fuse_perf_baseline.py \
  "${MP}/perf_baseline" \
  "/tmp/bench_results/perf_baseline_${TARGET_KEY}.json" \
  64 1024 16 2 256
if [[ "${TARGET_KEY}" == "bowl" ]]; then
  finish_bowl_stage_profiles "sqlite_wal"
  capture_bowl_snapshot "fuse_perf_baseline_post"
fi
if [[ "${TARGET_KEY}" == "drive9_default" ]]; then
  DRIVE9_SERVER="http://${SERVER_PRIV_IP}:9009" DRIVE9_API_KEY="$(cat /opt/bench/api_key_default.txt)" \
    /usr/local/bin/drive9 mount drain --timeout 300s "${MP}" || true
fi
rm -rf "${MP:?}"/*
sync
EOF_PBASE
  local s_end
  s_end=$(date +%s)
  if [[ "${target_key}" == "bowl" ]]; then
    record_stage "fuse_perf_baseline" "$((s_end - s_start))"
  else
    record_stage "${target_key}_fuse_perf_baseline" "$((s_end - s_start))"
  fi
}

# --- Phase 6A: Bowl Suites 1 -> 2 (Cold I/O + Cold Meta/Macro + Hot) -> 3 ---
run_juicefs_bench_for_target "bowl" "/mnt/bowl"

BOWL_D9_START=$(date +%s)
log "=== [Suite 2: drive9_fuse_bench.py] Target: bowl (Cold I/O + Cold Meta/Macro + Hot, zero-modification instrumentation) ==="
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' DRIVE9_GIT_COMMIT='${DRIVE9_GIT_COMMIT}' bash -s" <<'EOF_D9_BOWL'
set -euo pipefail
export TMPDIR=/opt/bench_tmp
SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa "root@${SERVER_PRIV_IP}")
source /opt/bowl/client_telemetry.sh
DROP_CMD="sync; echo 3 > /proc/sys/vm/drop_caches; ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa root@${SERVER_PRIV_IP} 'sync && echo 3 > /proc/sys/vm/drop_caches'"

# 通过轻量外部导入钩子在 timed() 计时窗外打细粒度快照与 pprof（不改动 drive9_fuse_bench.py 任何逻辑与参数）
SERVER_PRIV_IP="${SERVER_PRIV_IP}" DROP_CMD="${DROP_CMD}" DRIVE9_GIT_COMMIT="${DRIVE9_GIT_COMMIT}" python3 - <<'PY_BOWL_COLD'
import functools
import os
import subprocess
import sys

sys.path.insert(0, "/opt/bench")
import drive9_fuse_bench as b

srv_ip = os.environ["SERVER_PRIV_IP"]
runs = 3


def run_helper(cmd: str) -> None:
    full = (
        f"SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f"-o IdentitiesOnly=yes -i /root/.ssh/id_rsa 'root@{srv_ip}'); "
        f"source /opt/bowl/client_telemetry.sh; {cmd}"
    )
    subprocess.run(["bash", "-c", full], check=False)


def start_async_cpu(stage: str, sec: int = 8) -> subprocess.Popen:
    full = (
        f"SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null "
        f"-o IdentitiesOnly=yes -i /root/.ssh/id_rsa 'root@{srv_ip}'); "
        f"source /opt/bowl/client_telemetry.sh; "
        f"start_bowl_cpu_profile '{stage}' {sec} 0; "
        f"for pid in \"${{BOWL_PROFILE_PIDS[@]}}\"; do wait \"$pid\" 2>/dev/null || true; done"
    )
    return subprocess.Popen(["bash", "-c", full])


def finish_async_prof(stage: str, proc: subprocess.Popen | None) -> None:
    if proc is not None:
        try:
            proc.wait(timeout=35)
        except subprocess.TimeoutExpired:
            proc.kill()
    run_helper(f"finish_bowl_stage_profiles '{stage}'")


orig_seq_write = b.bench_sequential_write
orig_seq_read = b.bench_sequential_read
orig_small_write = b.bench_small_write
orig_mkdir = b.bench_mkdir
orig_macro = b.bench_macro
orig_run_cmd = b.run_command
orig_prepare_cache = b.prepare_measurement_cache

state = {
    "sw": 0,
    "sr": 0,
    "smw": 0,
    "mk": 0,
    "mc": 0,
    "arm_go_prof": False,
    "go_prof": None,
    "sr_prof": None,
    "smw_prof": None,
}


@functools.wraps(orig_seq_write)
def hooked_seq_write(root, mib, config, *args, **kwargs):
    idx = state["sw"]
    state["sw"] += 1
    if idx == 0:
        run_helper("capture_bowl_snapshot 'd9_cold_io_pre'")
    return orig_seq_write(root, mib, config, *args, **kwargs)


@functools.wraps(orig_seq_read)
def hooked_seq_read(root, mib, config, *args, **kwargs):
    idx = state["sr"]
    state["sr"] += 1
    if idx == 0:
        run_helper("capture_bowl_snapshot 'd9_cold_seq_read_pre'")
        state["sr_prof"] = start_async_cpu("seq_read", 6)
    try:
        return orig_seq_read(root, mib, config, *args, **kwargs)
    finally:
        if idx == runs - 1:
            finish_async_prof("seq_read", state["sr_prof"])
            state["sr_prof"] = None
            run_helper("capture_bowl_snapshot 'd9_cold_seq_read_post'")


@functools.wraps(orig_small_write)
def hooked_small_write(root, count, config, *args, **kwargs):
    idx = state["smw"]
    state["smw"] += 1
    if idx == 0:
        run_helper("capture_bowl_snapshot 'd9_cold_small_write_fsync_pre'")
        state["smw_prof"] = start_async_cpu("small_write_fsync", 8)
    try:
        return orig_small_write(root, count, config, *args, **kwargs)
    finally:
        if idx == runs - 1:
            finish_async_prof("small_write_fsync", state["smw_prof"])
            state["smw_prof"] = None
            run_helper("capture_bowl_snapshot 'd9_cold_small_write_fsync_post'")
            run_helper("capture_bowl_snapshot 'd9_cold_io_post'")


@functools.wraps(orig_mkdir)
def hooked_mkdir(root, count, config, *args, **kwargs):
    idx = state["mk"]
    state["mk"] += 1
    if idx == 0:
        run_helper("capture_bowl_snapshot 'd9_cold_meta_macro_pre'")
    return orig_mkdir(root, count, config, *args, **kwargs)


@functools.wraps(orig_run_cmd)
def hooked_run_cmd(cmd, cwd=None, *args, **kwargs):
    if cmd and cmd[0] == "sh" and len(cmd) >= 3 and "find " in cmd[2] and state["go_prof"] is None:
        state["arm_go_prof"] = True
    return orig_run_cmd(cmd, cwd, *args, **kwargs)


@functools.wraps(orig_prepare_cache)
def hooked_prepare_cache(config, *args, **kwargs):
    res = orig_prepare_cache(config, *args, **kwargs)
    if state["arm_go_prof"]:
        state["arm_go_prof"] = False
        if state["go_prof"] is None:
            state["go_prof"] = start_async_cpu("macro_go_build", 10)
    return res


@functools.wraps(orig_macro)
def hooked_macro(root, config, hot, errors_out, *args, **kwargs):
    idx = state["mc"]
    state["mc"] += 1
    try:
        return orig_macro(root, config, hot, errors_out, *args, **kwargs)
    finally:
        state["arm_go_prof"] = False
        if idx == runs - 1:
            finish_async_prof("macro_go_build", state["go_prof"])
            state["go_prof"] = None
            run_helper("capture_bowl_snapshot 'd9_cold_meta_macro_post'")


b.bench_sequential_write = hooked_seq_write
b.bench_sequential_read = hooked_seq_read
b.bench_small_write = hooked_small_write
b.bench_mkdir = hooked_mkdir
b.run_command = hooked_run_cmd
b.prepare_measurement_cache = hooked_prepare_cache
b.bench_macro = hooked_macro

rc = b.main([
    "--target", "bowl=/mnt/bowl",
    "--runs", str(runs),
    "--cache-state", "cold",
    "--drop-caches-command", os.environ["DROP_CMD"],
    "--environment", "lan",
    "--server-url", f"http://{srv_ip}:9000",
    "--server-host", srv_ip,
    "--version", os.environ["DRIVE9_GIT_COMMIT"],
    "--small-count", "200",
    "--stat-count", "500",
    "--dir-large-count", "500",
    "--large-mib", "256",
    "--random-reads", "1000",
    "--macro-files", "200",
    "--macro-total-mib", "20",
    "--out", "/tmp/bench_results/d9_cold_bowl.json",
])
raise SystemExit(rc)
PY_BOWL_COLD

capture_bowl_snapshot "d9_hot_pre"
python3 /opt/bench/drive9_fuse_bench.py \
  --target bowl=/mnt/bowl \
  --runs 3 \
  --cache-state hot \
  --environment lan \
  --server-url "http://${SERVER_PRIV_IP}:9000" \
  --server-host "${SERVER_PRIV_IP}" \
  --version "${DRIVE9_GIT_COMMIT}" \
  --small-count 200 \
  --stat-count 500 \
  --dir-large-count 500 \
  --large-mib 256 \
  --random-reads 1000 \
  --macro-files 200 \
  --macro-total-mib 20 \
  --out /tmp/bench_results/d9_hot_bowl.json
capture_bowl_snapshot "d9_hot_post"
EOF_D9_BOWL
BOWL_D9_END=$(date +%s)

run_perf_baseline_for_target "bowl" "/mnt/bowl"
TEST_END_EPOCH="$(date +%s)"

# 在卸载 Bowl 前采集完整 final 快照与活跃 Prometheus targets，卸载并等守护进程退出后导出连续无断层遥测，再仅停止 storage-service
log "Harvesting Bowl pre/post-umount telemetry for exact Bowl window [${TEST_START_EPOCH}, ${TEST_END_EPOCH}] and stopping storage-service..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' bash -s" <<'EOF_CLI_FLUSH'
set -euo pipefail
SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa "root@${SERVER_PRIV_IP}")
source /opt/bowl/client_telemetry.sh
harvest_bowl_before_and_after_umount /mnt/bowl
EOF_CLI_FLUSH
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "/usr/local/bin/bowl-prod-ctl export-telemetry /tmp/bowl-telemetry/server '${TEST_START_EPOCH}' '${TEST_END_EPOCH}' && /usr/local/bin/bowl-prod-ctl stop-storage"

# --- Phase 6B: Drive9 Default & Sync Suites 1 -> 2 -> 3 (with Bowl storage-service stopped) ---
log "Starting TiDB + Drive9 Server for isolated Drive9 Default & Sync benchmark suites..."
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "/usr/local/bin/bowl-prod-ctl stop-storage 2>/dev/null || true; systemctl start tidb drive9-server"
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' bash -s" <<'EOF_WAIT_D9'
set -euo pipefail
for _ in $(seq 1 30); do
  if curl -sf "http://${SERVER_PRIV_IP}:9009/healthz" >/dev/null; then break; fi
  sleep 1
done
curl -sf "http://${SERVER_PRIV_IP}:9009/healthz" >/dev/null
EOF_WAIT_D9

run_juicefs_bench_for_target "drive9_default" "/mnt/drive9_default"
run_juicefs_bench_for_target "drive9_sync" "/mnt/drive9_sync"

D9_SUITE2_START=$(date +%s)
log "=== [Suite 2: drive9_fuse_bench.py] Targets: drive9_default & drive9_sync (Cold & Hot) ==="
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' DRIVE9_GIT_COMMIT='${DRIVE9_GIT_COMMIT}' bash -s" <<'EOF_D9_COMP'
set -euo pipefail
export TMPDIR=/opt/bench_tmp
DROP_CMD="sync; echo 3 > /proc/sys/vm/drop_caches; ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa root@${SERVER_PRIV_IP} 'sync && echo 3 > /proc/sys/vm/drop_caches'"

python3 /opt/bench/drive9_fuse_bench.py \
  --target drive9_default=/mnt/drive9_default \
  --target drive9_sync=/mnt/drive9_sync \
  --runs 3 \
  --cache-state cold \
  --drop-caches-command "${DROP_CMD}" \
  --environment lan \
  --server-url "http://${SERVER_PRIV_IP}:9009" \
  --server-host "${SERVER_PRIV_IP}" \
  --version "${DRIVE9_GIT_COMMIT}" \
  --small-count 200 \
  --stat-count 500 \
  --dir-large-count 500 \
  --large-mib 256 \
  --random-reads 1000 \
  --macro-files 200 \
  --macro-total-mib 20 \
  --out /tmp/bench_results/d9_cold_drive9.json

DRIVE9_SERVER="http://${SERVER_PRIV_IP}:9009" DRIVE9_API_KEY="$(cat /opt/bench/api_key_default.txt)" \
  /usr/local/bin/drive9 mount drain --timeout 300s /mnt/drive9_default || true

python3 /opt/bench/drive9_fuse_bench.py \
  --target drive9_default=/mnt/drive9_default \
  --target drive9_sync=/mnt/drive9_sync \
  --runs 3 \
  --cache-state hot \
  --environment lan \
  --server-url "http://${SERVER_PRIV_IP}:9009" \
  --server-host "${SERVER_PRIV_IP}" \
  --version "${DRIVE9_GIT_COMMIT}" \
  --small-count 200 \
  --stat-count 500 \
  --dir-large-count 500 \
  --large-mib 256 \
  --random-reads 1000 \
  --macro-files 200 \
  --macro-total-mib 20 \
  --out /tmp/bench_results/d9_hot_drive9.json

DRIVE9_SERVER="http://${SERVER_PRIV_IP}:9009" DRIVE9_API_KEY="$(cat /opt/bench/api_key_default.txt)" \
  /usr/local/bin/drive9 mount drain --timeout 300s /mnt/drive9_default || true

# Merge isolated Bowl and Drive9 JSON reports
jq -s '.[0] * {
  mount_params: (.[0].mount_params * {targets: (.[0].mount_params.targets + .[1].mount_params.targets)}),
  targets: (.[0].targets + .[1].targets),
  results: (.[0].results + .[1].results)
}' /tmp/bench_results/d9_cold_bowl.json /tmp/bench_results/d9_cold_drive9.json > /tmp/bench_results/drive9_fuse_bench_cold.json

jq -s '.[0] * {
  mount_params: (.[0].mount_params * {targets: (.[0].mount_params.targets + .[1].mount_params.targets)}),
  targets: (.[0].targets + .[1].targets),
  results: (.[0].results + .[1].results)
}' /tmp/bench_results/d9_hot_bowl.json /tmp/bench_results/d9_hot_drive9.json > /tmp/bench_results/drive9_fuse_bench_hot.json

python3 - <<'PY'
import json, sys
sys.path.insert(0, "/opt/bench")
import drive9_fuse_bench as b
for stem in ("drive9_fuse_bench_cold", "drive9_fuse_bench_hot"):
    p = f"/tmp/bench_results/{stem}.json"
    with open(p, "r", encoding="utf-8") as f:
        rep = json.load(f)
    with open(f"/tmp/bench_results/{stem}.md", "w", encoding="utf-8") as f:
        f.write(b.render_summary(rep))
PY

rm -f /tmp/bench_results/d9_cold_bowl.json /tmp/bench_results/d9_cold_drive9.json /tmp/bench_results/d9_hot_bowl.json /tmp/bench_results/d9_hot_drive9.json
EOF_D9_COMP
D9_SUITE2_END=$(date +%s)
record_stage "drive9_fuse_bench_cold_and_hot" "$(( (BOWL_D9_END - BOWL_D9_START) + (D9_SUITE2_END - D9_SUITE2_START) ))"

run_perf_baseline_for_target "drive9_default" "/mnt/drive9_default"
run_perf_baseline_for_target "drive9_sync" "/mnt/drive9_sync"

# ------------------------------------------------------------------------------
# Step 7: Download Results, Distill deploy/prod Telemetry & Destroy Cloud Instances
# ------------------------------------------------------------------------------
log "Downloading raw benchmark results to ${RESULTS_DIR}..."
scp "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${CLIENT_PUB_IP}:/tmp/bench_results/*" "${RESULTS_DIR}/"
fetch_telemetry_archive
ssh "${SSH_OPTS[@]}" -i "${KEY_FILE}" "root@${SERVER_PUB_IP}" "/usr/local/bin/bowl-prod-ctl down-volumes" >/dev/null 2>&1 || true

DELETED_SERVER_ID="${SERVER_INSTANCE_ID}"
DELETED_CLIENT_ID="${CLIENT_INSTANCE_ID}"
DELETED_KEY_PAIR="${KEY_PAIR_NAME}"
REMAINING_COUNT=0

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
  for poll in {1..30}; do
    REMAINING_COUNT=$(aliyun ecs DescribeInstances --RegionId "${REGION_ID}" --InstanceIds "${IDS_JSON}" | jq '.Instances.Instance | length')
    [[ "${REMAINING_COUNT}" == "0" ]] && break
    sleep 5
  done
  log "Verified remaining target ECS instances count after DeleteInstances: ${REMAINING_COUNT}"
  if [[ "${REMAINING_COUNT}" != "0" ]]; then
    log "ERROR: 仍有 ${REMAINING_COUNT} 台测试实例，清理钩子将继续尝试。"
    exit 1
  fi
  SERVER_INSTANCE_ID=""
  CLIENT_INSTANCE_ID=""

  aliyun ecs DeleteKeyPairs \
    --RegionId "${REGION_ID}" \
    --KeyPairNames "[\"${KEY_PAIR_NAME}\"]" >/dev/null
  KEY_PAIR_NAME=""
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
  --arg ping_stats "${PING_STATS}" \
  --arg iperf_gbps "${IPERF_GBPS}" \
  --arg bowl_commit "${BOWL_GIT_COMMIT}" \
  --arg drive9_commit "${DRIVE9_GIT_COMMIT}" \
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
      atime_mode: "noatime",
      fuse_cache_timeout: "1.0s (entry=1s, attr=1s, direntry=1s)"
    },
    drive9_config: {
      git_commit: $drive9_commit,
      default_mode: "--profile=coding-agent --durability=interactive --write-cache-size-mb=0",
      sync_mode: "--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s"
    },
    cleanup_audit: {
      key_pair_name: $key_pair,
      server_instance_ids: $srv_id,
      client_instance_ids: $cli_id,
      instances_created_at: $created_at,
      instances_deleted_at: $deleted_at,
      remaining_benchmark_instances: $remaining
    }
  }' > "${RESULTS_DIR}/env_metadata.json"

log "Generating summary.json and REPORT.md via parse_results.py..."
PYTHONDONTWRITEBYTECODE=1 python3 "${SCRIPT_DIR}/parse_results.py" \
  --results-dir "${RESULTS_DIR}" \
  --summary-out "${SUMMARY_JSON}" \
  --report-out "${REPORT_FILE}"

log "=== Benchmark Suite Completed Successfully! Report saved to ${REPORT_FILE} ==="
