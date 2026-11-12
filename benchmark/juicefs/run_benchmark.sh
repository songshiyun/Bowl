#!/usr/bin/env bash
# ==============================================================================
# Bowl vs JuiceFS - Aliyun Silicon Valley (us-west-1) Dual-ECS Benchmark Suite
# ==============================================================================
# ADR-0030 / ADR-0031 / ADR-0032 + deploy/prod 生产编排栈：
# - 部署与可观测性：默认基于 deploy/prod/docker-compose.yaml + deploy/prod/storage.env 容器化部署
#   PostgreSQL 18 控制面、Goose 迁移、storage-service 及 Jaeger + Prometheus + Loki + Alloy + Grafana。
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
#   ReadAheadCoordinator 单飞有序槽位预读（W_read=4, ReadAheadMaxGlobalInflight=32）、completeDirs 本地目录流合成 + OTLP 全链路追踪与 bowl stats；Flush 不触发跨机 Fsync RPC。
set -euo pipefail
export GOWORK=off

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
RESULTS_DIR="${RESULTS_DIR:-${SCRIPT_DIR}/results}"
REPORT_FILE="${REPORT_FILE:-${SCRIPT_DIR}/REPORT.md}"
# shellcheck source=../common/prod_stack.sh
source "${REPO_ROOT}/benchmark/common/prod_stack.sh"

# Aliyun Configuration (Silicon Valley us-west-1a, Local NVMe SSD ecs.i5.xlarge)
REGION_ID="${REGION_ID:-us-west-1}"
ZONE_ID="${ZONE_ID:-us-west-1a}"
VPC_ID="${VPC_ID:?VPC_ID is required}"
VSWITCH_ID="${VSWITCH_ID:?VSWITCH_ID is required}"
SECURITY_GROUP_ID="${SECURITY_GROUP_ID:?SECURITY_GROUP_ID is required}"
INSTANCE_TYPE="${INSTANCE_TYPE:-ecs.i5.xlarge}"
IMAGE_ID="${IMAGE_ID:-ubuntu_24_04_x64_20G_alibase_20261016.vhd}"
SYSTEM_DISK_CATEGORY="${SYSTEM_DISK_CATEGORY:-cloud_essd}"
SYSTEM_DISK_SIZE="${SYSTEM_DISK_SIZE:-40}"
KEEP_INSTANCES="${KEEP_INSTANCES:-0}"
TARGETS_TO_RUN="${TARGETS_TO_RUN:-bowl jfs_pg jfs_redis}"

RUN_TS="$(date -u +%Y%m%d-%H%M%S)"
KEY_PAIR_NAME="bowl-jfsbench-kp-${RUN_TS}"
WORK_DIR="$(mktemp -d "/tmp/bowl-jfsbench-${RUN_TS}-XXXXXX")"
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
  if [[ -n "${CLIENT_SSH_HOST:-}" && -f "${SSH_KEY_FILE}" ]]; then
    CLIENT_SSH_CMD_ARR=(ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}")
  fi
  if [[ -n "${SERVER_SSH_HOST:-}" && -f "${SSH_KEY_FILE}" ]]; then
    SERVER_SSH_CMD_ARR=(ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}")
  fi

  log "Collecting and distilling deploy/prod telemetry into ${RESULTS_DIR}/telemetry/ ..."
  collect_and_process_bench_telemetry \
    "${REPO_ROOT}" \
    "${WORK_DIR}" \
    "${RESULTS_DIR}" \
    "juicefs" \
    "${TEST_START_EPOCH:-}" \
    "${TEST_END_EPOCH:-}"

  eval "${old_opts}"
  return 0
}

cleanup() {
  local exit_code=$?
  set +e

  # 失败时先回传原始输出，避免释放实例后丢失失败证据。
  if [[ "${exit_code}" != "0" && -n "${CLIENT_SSH_HOST:-}" ]]; then
    scp "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/bench_results/*" "${RESULTS_DIR}/" 2>/dev/null
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
if [[ " ${TARGETS_TO_RUN} " == *" bowl "* ]]; then
  log "Step 1/6: Cross-compiling storage-service, bowl CLI, and goose for linux/amd64..."
  GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go -C "${REPO_ROOT}/services/storage" build -mod=mod -trimpath \
    -o "${WORK_DIR}/storage-service" ./cmd

  GOWORK=off CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go -C "${REPO_ROOT}/client" build -mod=mod -trimpath \
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
  prepare_prod_bundle "${REPO_ROOT}" "${WORK_DIR}"
  COPYFILE_DISABLE=1 tar -czf "${WORK_DIR}/client-bins.tar.gz" -C "${WORK_DIR}" bowl
  log "Compiled binaries and deploy/prod bundle ready in ${WORK_DIR}"
else
  log "Step 1/6: Skipping Bowl binary cross-compilation (TARGETS_TO_RUN=${TARGETS_TO_RUN})"
fi

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
  --InstanceName "bowl-jfsbench" \
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

# Also allow Client root SSH into Server over VPC private IP so Client runner can drop Server caches & reset storage cleanly
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "mkdir -p /root/.ssh && chmod 700 /root/.ssh"
scp "${CLIENT_SSH_OPTS[@]}" "${SSH_KEY_FILE}" "root@${CLIENT_SSH_HOST}:/root/.ssh/id_rsa"
ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "chmod 600 /root/.ssh/id_rsa"

# ==============================================================================
# 第 3 步：在服务端挂载本地 NVMe SSD，部署 deploy/prod 栈及 JuiceFS 对比组后端。
# ==============================================================================
log "Step 3/6: Mounting local NVMe SSD and deploying deploy/prod stack (Bowl + Observability) & JuiceFS backends on Server (${SERVER_PRIV_IP})..."
if [[ " ${TARGETS_TO_RUN} " == *" bowl "* ]]; then
  scp "${SERVER_SSH_OPTS[@]}" "${WORK_DIR}/prod-bundle.tar.gz" "root@${SERVER_SSH_HOST}:/tmp/prod-bundle.tar.gz"
fi

ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "TARGETS_TO_RUN='${TARGETS_TO_RUN}' bash -s" <<'EOF'
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

mkdir -p /mnt/nvme/postgresql /mnt/nvme/minio/data/jfs-pg /mnt/nvme/minio/data/jfs-redis /mnt/nvme/redis
mkdir -p /var/lib/postgresql /var/lib/minio /var/lib/redis
mount --bind /mnt/nvme/postgresql /var/lib/postgresql
mount --bind /mnt/nvme/minio /var/lib/minio
mount --bind /mnt/nvme/redis /var/lib/redis

while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq postgresql redis-server iperf3 fio jq curl unzip python3 ca-certificates

# Install rclone v1.68.2 (with native `rclone serve s3` and `rclone serve webdav` support)
curl -fsSL https://github.com/rclone/rclone/releases/download/v1.68.2/rclone-v1.68.2-linux-amd64.deb -o /tmp/rclone.deb
dpkg -i /tmp/rclone.deb
rm -f /tmp/rclone.deb

# 1. Configure host PostgreSQL 16 (exclusively for jfs_pg comparison target on :5432)
systemctl enable --now postgresql
PG_CONF_FILE="$(sudo -u postgres psql -tAc 'SHOW config_file;' | xargs)"
PG_HBA_FILE="$(sudo -u postgres psql -tAc 'SHOW hba_file;' | xargs)"
grep -q "^listen_addresses = '\*'" "${PG_CONF_FILE}" || echo "listen_addresses = '*'" >> "${PG_CONF_FILE}"
grep -q "172.16.0.0/12" "${PG_HBA_FILE}" || echo "host all all 172.16.0.0/12 md5" >> "${PG_HBA_FILE}"

sudo -u postgres psql -c "ALTER USER postgres WITH PASSWORD 'postgres';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname = 'jfs_pg'" | grep -q 1 || \
  sudo -u postgres psql -c "CREATE DATABASE jfs_pg;"
sudo -u postgres psql -c "ALTER SYSTEM SET max_connections = '300';"
sudo -u postgres psql -c "ALTER SYSTEM SET shared_buffers = '2GB';"
systemctl restart postgresql

# 2. Configure Redis 7 (AOF everysec, bind 0.0.0.0)
sed -i 's/^bind /# bind /' /etc/redis/redis.conf
cat >>/etc/redis/redis.conf <<'REDIS_EOF'
bind 0.0.0.0
protected-mode no
appendonly yes
appendfsync everysec
REDIS_EOF
systemctl enable --now redis-server
systemctl restart redis-server

# 3. Start S3 (:9001) & WebDAV (:9002) Object Storage Servers via rclone on Server node local NVMe SSD
mkdir -p /var/lib/minio/data/jfs-pg /var/lib/minio/data/jfs-redis

cat >/etc/systemd/system/jfs-s3.service <<'UNIT'
[Unit]
Description=Rclone S3 Object Storage Server for JuiceFS Benchmark
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/rclone serve s3 /var/lib/minio/data --addr 0.0.0.0:9001 --auth-key minioadmin,minioadmin
Restart=always
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

cat >/etc/systemd/system/jfs-webdav.service <<'UNIT'
[Unit]
Description=Rclone WebDAV Object Storage Server for JuiceFS Benchmark
After=network.target

[Service]
Type=simple
ExecStart=/usr/bin/rclone serve webdav /var/lib/minio/data --addr 0.0.0.0:9002
Restart=always
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT

systemctl daemon-reload
systemctl enable --now jfs-s3 jfs-webdav

for i in {1..30}; do
  if ss -tln | grep -q ':9001 ' && ss -tln | grep -q ':9002 ' && ss -tln | grep -q ':6379 '; then
    break
  fi
  sleep 1
done
if ! (ss -tln | grep -q ':9001 ' && ss -tln | grep -q ':9002 ' && ss -tln | grep -q ':6379 '); then
  echo "ERROR: Some server ports failed to listen:"
  ss -tlnp
  journalctl -u jfs-s3 -u jfs-webdav -u redis-server -n 30 --no-pager || true
  exit 1
fi

pkill -f "iperf3 -s" || true
iperf3 -s -D
EOF

if [[ " ${TARGETS_TO_RUN} " == *" bowl "* ]]; then
  log "Initializing Bowl deploy/prod Docker Compose stack on Server (${SERVER_PRIV_IP})..."
  remote_setup_prod_stack "${SERVER_PRIV_IP}" "${CLIENT_PRIV_IP}" "prod" \
    | ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s"
  ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "/usr/local/bin/bowl-prod-ctl up"
fi
log "Server node (${SERVER_PRIV_IP}) services (Bowl deploy/prod:9000 + Jaeger/Prometheus/Loki, rclone S3:9001, PG:5432, Redis:6379) are ready on local NVMe SSD."

# Measure Server local NVMe SSD disk baseline + Server system info
log "Measuring Server local NVMe SSD disk baseline..."
SERVER_INFO_JSON="$(ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail
mkdir -p /mnt/nvme/disk_bench
WRITE_BW_BPS="$(fio --name=nvme-seq-w --directory=/mnt/nvme/disk_bench --rw=write --bs=4M --size=2G --direct=1 --end_fsync=1 --lat_percentiles=1 --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].write.bw_bytes // 0')"
READ_BW_BPS="$(fio --name=nvme-seq-r --directory=/mnt/nvme/disk_bench --rw=read --bs=4M --size=2G --direct=1 --lat_percentiles=1 --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].read.bw_bytes // 0')"
RANDW_IOPS="$(fio --name=nvme-rand-w --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randwrite --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --lat_percentiles=1 --group_reporting --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].write.iops // 0')"
RANDR_IOPS="$(fio --name=nvme-rand-r --directory=/mnt/nvme/disk_bench --ioengine=libaio --rw=randread --bs=4k --size=1G --numjobs=4 --iodepth=32 --direct=1 --runtime=5 --time_based=1 --lat_percentiles=1 --group_reporting --output-format=json | sed -n '/^{/,$p' | jq -r '.jobs[0].read.iops // 0')"
FSYNC_JSON="$(fio --name=nvme-fsync-w --directory=/mnt/nvme/disk_bench --ioengine=psync --rw=write --bs=4k --size=64M --numjobs=1 --direct=1 --fsync=1 --runtime=5 --time_based=1 --lat_percentiles=1 --output-format=json | sed -n '/^{/,$p')"
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
BOWL_PG_VER="$(/usr/local/bin/bowl-prod-ctl pg-version 2>/dev/null | head -n1 || echo 'postgres (PostgreSQL) 18 (alpine)')"
DOCKER_VER="$(docker --version 2>/dev/null | head -n1 || echo 'n/a')"
REDIS_VER="$(redis-server --version | awk '{print $1, $2, $3}')"
RCLONE_VER="$(rclone --version | head -n1)"

jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg os "${OS_REL}" \
  --arg kernel "${KERNEL}" \
  --arg pg "${PG_VER}" \
  --arg bowl_pg "${BOWL_PG_VER}" \
  --arg docker_ver "${DOCKER_VER}" \
  --arg redis "${REDIS_VER}" \
  --arg rclone "${RCLONE_VER}" \
  --arg nvme_dev "${NVME_DEV}" \
  --argjson nvme_gib "${NVME_SIZE_GIB}" \
  --arg disk_w "${WRITE_MIB}" \
  --arg disk_r "${READ_MIB}" \
  --arg randw_iops "${RANDW_IOPS_FMT}" \
  --arg randr_iops "${RANDR_IOPS_FMT}" \
  --arg fsync_iops "${FSYNC_IOPS_FMT}" \
  --arg fsync_p99_us "${FSYNC_P99_FMT}" \
  '{
    server: {cpu_model: $cpu, vcpu: $vcpu, memory_gib: $mem, os_release: $os, kernel: $kernel, pg_version: $pg, bowl_pg_version: $bowl_pg, docker_version: $docker_ver, redis_version: $redis, rclone_version: $rclone, data_disk_device: $nvme_dev, data_disk_size_gib: $nvme_gib},
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
log "Server baseline: $(echo "${SERVER_INFO_JSON}" | jq -c '.')"

# ==============================================================================
# Step 4: Deploy Bowl FUSE Client, JuiceFS CLI, fio, and mdtest on Client Node
# ==============================================================================
log "Step 4/6: Mounting local NVMe SSD and installing benchmark tools (bowl, juicefs, fio, mdtest) on Client (${CLIENT_PRIV_IP})..."
if [[ " ${TARGETS_TO_RUN} " == *" bowl "* ]]; then
  scp "${CLIENT_SSH_OPTS[@]}" "${WORK_DIR}/client-bins.tar.gz" "root@${CLIENT_SSH_HOST}:/tmp/client-bins.tar.gz"
fi

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "bash -s" <<'EOF'
set -euo pipefail

# Mount local NVMe SSD on Client and bind-mount /var/jfsCache
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

mkdir -p /mnt/nvme/jfsCache /var/jfsCache
mount --bind /mnt/nvme/jfsCache /var/jfsCache

if [[ -f /tmp/client-bins.tar.gz ]]; then
  tar -xzf /tmp/client-bins.tar.gz -C /usr/local/bin/
  chmod +x /usr/local/bin/bowl
  rm -f /tmp/client-bins.tar.gz
fi
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do sleep 2; done
export DEBIAN_FRONTEND=noninteractive
apt-get update -y -qq
apt-get install -y -qq fuse3 libfuse3-dev iperf3 fio openmpi-bin libopenmpi-dev git build-essential autoconf automake libtool pkg-config python3 jq curl

grep -q '^user_allow_other' /etc/fuse.conf 2>/dev/null || echo 'user_allow_other' >> /etc/fuse.conf

# Install JuiceFS v1.2.1 Linux amd64 binary
if [[ ! -x /usr/local/bin/juicefs ]]; then
  curl -fsSL https://github.com/juicedata/juicefs/releases/download/v1.2.1/juicefs-1.2.1-linux-amd64.tar.gz -o /tmp/juicefs.tar.gz
  tar -xzf /tmp/juicefs.tar.gz -C /tmp juicefs
  mv /tmp/juicefs /usr/local/bin/juicefs
  chmod +x /usr/local/bin/juicefs
  rm -f /tmp/juicefs.tar.gz
fi

# Build mdtest from hpc/ior
if [[ ! -x /usr/local/bin/mdtest ]]; then
  rm -rf /opt/ior /mnt/nvme/ior
  mkdir -p /mnt/nvme/ior /opt/ior
  mount --bind /mnt/nvme/ior /opt/ior
  git clone --depth 1 --branch 3.3.0 https://github.com/hpc/ior.git /opt/ior || git clone --depth 1 https://github.com/hpc/ior.git /opt/ior
  cd /opt/ior
  ./bootstrap
  ./configure
  make -j"$(nproc)"
  cp src/mdtest /usr/local/bin/mdtest
  chmod +x /usr/local/bin/mdtest
fi
EOF

# Collect Client metadata + internal VPC network baseline
log "Measuring internal VPC RTT and throughput (${CLIENT_PRIV_IP} -> ${SERVER_PRIV_IP})..."
CLIENT_INFO_JSON="$(ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "SERVER_PRIV_IP=${SERVER_PRIV_IP} bash -s" <<'EOF'
set -euo pipefail
PING_OUT="$(ping -c 20 -i 0.05 "${SERVER_PRIV_IP}" | tail -n 1)"
PING_RTT="$(echo "${PING_OUT}" | awk -F'=' '{gsub(/ ms/, "", $2); gsub(/^[ \t]+/, "", $2); print $2}')"

IPERF_BPS="$(iperf3 -c "${SERVER_PRIV_IP}" -t 5 -J | jq -r '.end.sum_received.bits_per_second // 0')"
IPERF_GBPS="$(awk -v bps="${IPERF_BPS}" 'BEGIN {printf "%.2f", bps / 1e9}')"

CPU_MODEL="$(lscpu | awk -F: '/Model name/ {gsub(/^[ \t]+/, "", $2); print $2; exit}')"
VCPU="$(nproc)"
MEM_GIB="$(awk '/MemTotal/ {printf "%.1f GiB", $2/1024/1024}' /proc/meminfo)"
OS_REL="$(. /etc/os-release && echo "${PRETTY_NAME}")"
KERNEL="$(uname -r)"
FUSER_VER="$(fusermount3 -V | head -n1)"
JFS_VER="$(/usr/local/bin/juicefs --version | head -n1)"
FIO_VER="$(fio --version | head -n1)"
MDTEST_VER="$(/usr/local/bin/mdtest --help 2>&1 | grep -m1 -o 'mdtest-[0-9.+a-zA-Z_-]*' || echo 'mdtest-3.3.0')"

jq -n \
  --arg cpu "${CPU_MODEL}" \
  --arg vcpu "${VCPU}" \
  --arg mem "${MEM_GIB}" \
  --arg os "${OS_REL}" \
  --arg kernel "${KERNEL}" \
  --arg fuser "${FUSER_VER}" \
  --arg jfs_ver "${JFS_VER}" \
  --arg fio_ver "${FIO_VER}" \
  --arg mdtest_ver "${MDTEST_VER}" \
  --arg ping_rtt "${PING_RTT}" \
  --arg iperf_gbps "${IPERF_GBPS}" \
  '{
    client: {
      cpu_model: $cpu,
      vcpu: $vcpu,
      memory_gib: $mem,
      os_release: $os,
      kernel: $kernel,
      fusermount_version: $fuser,
      juicefs_version: $jfs_ver,
      fio_version: $fio_ver,
      mdtest_version: $mdtest_ver
    },
    network: {ping_rtt_ms: $ping_rtt, iperf3_gbps: $iperf_gbps}
  }'
EOF
)"
log "Client info & VPC network baseline: $(echo "${CLIENT_INFO_JSON}" | jq -c '.')"

# ==============================================================================
# Step 5: Execute Full Benchmark Suites on Bowl, JuiceFS (PG), JuiceFS (Redis)
# ==============================================================================
emit_client_telemetry_helper | ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "mkdir -p /opt/bowl && cat >/opt/bowl/client_telemetry.sh && chmod +x /opt/bowl/client_telemetry.sh"

TEST_STARTED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TEST_START_EPOCH="$(date +%s)"
log "Step 5/6: Running benchmark suites (${TARGETS_TO_RUN}) on Client -> Server..."

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "cat >/tmp/run_client_bench.sh && chmod +x /tmp/run_client_bench.sh" <<'BENCH_EOF'
#!/usr/bin/env bash
set -euo pipefail

OUT_DIR="/tmp/bench_results"
rm -rf "${OUT_DIR}"
mkdir -p "${OUT_DIR}"
TIMINGS_FILE="${OUT_DIR}/stage_timings.jsonl"
: > "${TIMINGS_FILE}"

SSH_SRV=(ssh -n -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o IdentitiesOnly=yes -i /root/.ssh/id_rsa "root@${SERVER_PRIV_IP}")
source /opt/bowl/client_telemetry.sh
"${SSH_SRV[@]}" "mkdir -p /tmp/bowl-telemetry/server/logs /tmp/bowl-telemetry/server/metrics /tmp/bowl-telemetry/server/diskstats /tmp/bowl-telemetry/server/pprof /tmp/bowl-telemetry/server/pebble" || true

capture_target_snapshot() {
  local target="$1"
  local phase="$2"
  [[ "${target}" == "bowl" ]] || return 0
  capture_bowl_snapshot "${phase}"
}

start_target_cpu_profile() {
  local target="$1"
  local stage="$2"
  local sec="${3:-10}"
  [[ "${target}" == "bowl" ]] || return 0
  start_bowl_cpu_profile "${stage}" "${sec}" 0
}

finish_target_stage_profiles() {
  local target="$1"
  local stage="$2"
  [[ "${target}" == "bowl" ]] || return 0
  finish_bowl_stage_profiles "${stage}"
}

drop_all_caches() {
  sync
  echo 3 > /proc/sys/vm/drop_caches
  # Clear JuiceFS local client read cache staging so reads hit Server storage fairly
  find /var/jfsCache -mindepth 2 -type f -delete 2>/dev/null || true
  "${SSH_SRV[@]}" "sync && echo 3 > /proc/sys/vm/drop_caches"
}

record_timing() {
  local target="$1"
  local suite="$2"
  local elapsed="$3"
  jq -nc --arg t "${target}" --arg s "${suite}" --argjson d "${elapsed}" \
    '{target: $t, suite: $s, duration_sec: $d}' >> "${TIMINGS_FILE}"
}

run_suites_on_mount() {
  local target="$1"
  local mp="$2"

  echo "=================================================================="
  echo "[${target}] Starting benchmark suites on ${mp} ..."
  echo "=================================================================="

  # 1. juicefs bench -p 4
  echo "[${target}] [1/3] Running juicefs bench ${mp} -p 4 ..."
  drop_all_caches
  capture_target_snapshot "${target}" "juicefs_bench_pre"
  local t0
  t0="$(date +%s.%N)"
  /usr/local/bin/juicefs bench "${mp}" -p 4 </dev/null 2>&1 | tee "${OUT_DIR}/bench_${target}.log"
  local t1
  t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "juicefs_bench_post"
  record_timing "${target}" "juicefs_bench" "$(awk -v a="${t0}" -v b="${t1}" 'BEGIN {printf "%.3f", b - a}')"

  # 2. fio suite (8 workloads)
  echo "[${target}] [2/3] Running fio suite (8 workloads) on ${mp}/fio_work ..."
  local fio_dir="${mp}/fio_work"
  rm -rf "${fio_dir}"
  mkdir -p "${fio_dir}"

  t0="$(date +%s.%N)"
  local sub_t0 sub_t1

  # 2.1 seq_write_1job (4M, 4G, 1 job)
  echo "  -> [${target}] fio seq_write_1job (bs=4M, size=4G, numjobs=1)"
  drop_all_caches
  capture_target_snapshot "${target}" "fio_seq_write_1job_pre"
  sub_t0="$(date +%s.%N)"
  start_target_cpu_profile "${target}" "fio_seq_write_1job" 10
  fio --name=seq1 --directory="${fio_dir}" --rw=write --refill_buffers --bs=4M --size=4G --numjobs=1 --end_fsync=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_seq_write_1job.json"
  finish_target_stage_profiles "${target}" "fio_seq_write_1job"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "fio_seq_write_1job_post"
  record_timing "${target}" "fio_seq_write_1job" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"

  # 2.2 seq_read_1job (4M, 4G, 1 job)
  echo "  -> [${target}] fio seq_read_1job (bs=4M, size=4G, numjobs=1)"
  drop_all_caches
  capture_target_snapshot "${target}" "fio_seq_read_1job_pre"
  sub_t0="$(date +%s.%N)"
  start_target_cpu_profile "${target}" "fio_seq_read_1job" 10
  fio --name=seq1 --directory="${fio_dir}" --rw=read --refill_buffers --bs=4M --size=4G --numjobs=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_seq_read_1job.json"
  finish_target_stage_profiles "${target}" "fio_seq_read_1job"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "fio_seq_read_1job_post"
  record_timing "${target}" "fio_seq_read_1job" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"
  rm -f "${fio_dir}"/seq1*

  # 2.3 & 2.4 seq_16jobs (4M, 1G x 16 = 16GiB write + read)
  echo "  -> [${target}] fio seq_write_16jobs (bs=4M, size=1G, numjobs=16)"
  drop_all_caches
  capture_target_snapshot "${target}" "fio_seq_16jobs_pre"
  sub_t0="$(date +%s.%N)"
  fio --name=seq16 --directory="${fio_dir}" --rw=write --refill_buffers --bs=4M --size=1G --numjobs=16 --end_fsync=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_seq_write_16jobs.json"

  echo "  -> [${target}] fio seq_read_16jobs (bs=4M, size=1G, numjobs=16)"
  drop_all_caches
  fio --name=seq16 --directory="${fio_dir}" --rw=read --refill_buffers --bs=4M --size=1G --numjobs=16 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_seq_read_16jobs.json"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "fio_seq_16jobs_post"
  record_timing "${target}" "fio_seq_16jobs" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"
  rm -f "${fio_dir}"/seq16*

  # 2.5 & 2.6 direct_seq (1M, 1G x 4 = 4GiB, direct=1 write + read)
  echo "  -> [${target}] fio direct_write_4jobs (bs=1m, size=1g, numjobs=4, direct=1)"
  drop_all_caches
  capture_target_snapshot "${target}" "fio_direct_seq_pre"
  sub_t0="$(date +%s.%N)"
  fio --name=direct4 --directory="${fio_dir}" --ioengine=libaio --rw=write --bs=1m --size=1g --numjobs=4 --direct=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_direct_write_4jobs.json"

  echo "  -> [${target}] fio direct_read_4jobs (bs=1m, size=1g, numjobs=4, direct=1)"
  drop_all_caches
  fio --name=direct4 --directory="${fio_dir}" --ioengine=libaio --rw=read --bs=1m --size=1g --numjobs=4 --direct=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_direct_read_4jobs.json"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "fio_direct_seq_post"
  record_timing "${target}" "fio_direct_seq" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"

  # 2.7 direct_randwrite_4jobs (1M, 1G x 4 = 4GiB, direct=1)
  echo "  -> [${target}] fio direct_randwrite_4jobs (bs=1m, size=1g, numjobs=4, direct=1)"
  drop_all_caches
  capture_target_snapshot "${target}" "fio_direct_randwrite_pre"
  sub_t0="$(date +%s.%N)"
  start_target_cpu_profile "${target}" "fio_direct_randwrite" 10
  fio --name=direct4 --directory="${fio_dir}" --ioengine=libaio --rw=randwrite --bs=1m --size=1g --numjobs=4 --direct=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_direct_randwrite_4jobs.json"
  finish_target_stage_profiles "${target}" "fio_direct_randwrite"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "fio_direct_randwrite_post"
  record_timing "${target}" "fio_direct_randwrite" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"

  # 2.8 direct_randread_4jobs (1M, 1G x 4 = 4GiB, direct=1)
  echo "  -> [${target}] fio direct_randread_4jobs (bs=1m, size=1g, numjobs=4, direct=1)"
  drop_all_caches
  capture_target_snapshot "${target}" "fio_direct_randread_pre"
  sub_t0="$(date +%s.%N)"
  fio --name=direct4 --directory="${fio_dir}" --ioengine=libaio --rw=randread --bs=1m --size=1g --numjobs=4 --direct=1 --lat_percentiles=1 --group_reporting --output-format=json </dev/null > "${OUT_DIR}/fio_${target}_direct_randread_4jobs.json"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "fio_direct_randread_post"
  record_timing "${target}" "fio_direct_randread" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"

  rm -rf "${fio_dir}"
  t1="$(date +%s.%N)"
  record_timing "${target}" "fio" "$(awk -v a="${t0}" -v b="${t1}" 'BEGIN {printf "%.3f", b - a}')"

  # 3. mdtest suite (1-proc standard + 4-proc MPI)
  echo "[${target}] [3/3] Running mdtest suite on ${mp} ..."
  t0="$(date +%s.%N)"
  drop_all_caches
  capture_target_snapshot "${target}" "mdtest_1p_pre"
  sub_t0="$(date +%s.%N)"
  start_target_cpu_profile "${target}" "mdtest_1p" 10
  /usr/local/bin/mdtest -d "${mp}/mdtest_1p" -b 6 -I 8 -z 4 </dev/null 2>&1 | tee "${OUT_DIR}/mdtest_${target}_1proc.log"
  finish_target_stage_profiles "${target}" "mdtest_1p"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "mdtest_1p_post"
  record_timing "${target}" "mdtest_1p" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"
  rm -rf "${mp}/mdtest_1p"

  drop_all_caches
  capture_target_snapshot "${target}" "mdtest_4p_pre"
  sub_t0="$(date +%s.%N)"
  start_target_cpu_profile "${target}" "mdtest_4p" 10
  mpirun --allow-run-as-root --oversubscribe -np 4 /usr/local/bin/mdtest -d "${mp}/mdtest_4p" -b 3 -z 2 -I 20 -u </dev/null 2>&1 | tee "${OUT_DIR}/mdtest_${target}_4proc.log"
  finish_target_stage_profiles "${target}" "mdtest_4p"
  sub_t1="$(date +%s.%N)"
  capture_target_snapshot "${target}" "mdtest_4p_post"
  record_timing "${target}" "mdtest_4p" "$(awk -v a="${sub_t0}" -v b="${sub_t1}" 'BEGIN {printf "%.3f", b - a}')"
  rm -rf "${mp}/mdtest_4p"
  t1="$(date +%s.%N)"
  record_timing "${target}" "mdtest" "$(awk -v a="${t0}" -v b="${t1}" 'BEGIN {printf "%.3f", b - a}')"
}

has_target() {
  [[ " ${TARGETS_TO_RUN} " == *" $1 "* ]]
}

# ------------------------------------------------------------------------------
# 对象 1：Bowl（deploy/prod 容器栈：PostgreSQL 18 + StoreActor 单盘 Pebble + 64 MiB Pack + 全栈可观测）
# ------------------------------------------------------------------------------
if has_target "bowl"; then
  "${SSH_SRV[@]}" "systemctl stop postgresql redis-server jfs-s3 jfs-webdav 2>/dev/null || true; date +%s > /var/run/bowl-prod-started.epoch; /usr/local/bin/bowl-prod-ctl up"
  ENDPOINT="http://${SERVER_PRIV_IP}:9000"
  CREATE_JSON="$(/usr/local/bin/bowl disk create --size 2PiB --name "jfs-bench-disk" --endpoint "${ENDPOINT}" --json)"
  DISK_ID="$(echo "${CREATE_JSON}" | jq -r '.disk_id')"
  echo "${CREATE_JSON}" > "${OUT_DIR}/bowl_disk.json"

  mkdir -p /mnt/bowl
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
  BOWL_PHASE_START="$(date +%s)"
  run_suites_on_mount "bowl" "/mnt/bowl"
  BOWL_PHASE_END="$(date +%s)"
  jq -nc --argjson s "${BOWL_PHASE_START}" --argjson e "${BOWL_PHASE_END}" '{start_epoch: $s, end_epoch: $e}' > "${OUT_DIR}/bowl_window.json"
  harvest_bowl_before_and_after_umount /mnt/bowl
  "${SSH_SRV[@]}" "/usr/local/bin/bowl-prod-ctl export-telemetry /tmp/bowl-telemetry/server '${BOWL_PHASE_START}' '${BOWL_PHASE_END}' && /usr/local/bin/bowl-prod-ctl stop-storage"
fi

# ------------------------------------------------------------------------------
# Target 2: JuiceFS + PostgreSQL 16 (jfs_pg)
# ------------------------------------------------------------------------------
if has_target "jfs_pg"; then
  "${SSH_SRV[@]}" "/usr/local/bin/bowl-prod-ctl stop-storage 2>/dev/null || true; systemctl stop redis-server 2>/dev/null || true; systemctl start postgresql jfs-s3 jfs-webdav"
  export AWS_ACCESS_KEY_ID=minioadmin
  export AWS_SECRET_ACCESS_KEY=minioadmin
  export MINIO_ACCESS_KEY=minioadmin
  export MINIO_SECRET_KEY=minioadmin
  PG_META_URL="postgres://postgres:postgres@${SERVER_PRIV_IP}:5432/jfs_pg?sslmode=disable"
  if ! /usr/local/bin/juicefs format \
    --storage minio \
    --bucket "http://${SERVER_PRIV_IP}:9001/jfs-pg" \
    --access-key minioadmin \
    --secret-key minioadmin \
    --trash-days 0 \
    --force \
    "${PG_META_URL}" \
    jfs-pg-bench </dev/null; then
    /usr/local/bin/juicefs format \
      --storage webdav \
      --bucket "http://${SERVER_PRIV_IP}:9002/jfs-pg/" \
      --trash-days 0 \
      --force \
      "${PG_META_URL}" \
      jfs-pg-bench </dev/null
  fi

  mkdir -p /mnt/jfs_pg
  /usr/local/bin/juicefs mount -d --no-usage-report "${PG_META_URL}" /mnt/jfs_pg </dev/null
  run_suites_on_mount "jfs_pg" "/mnt/jfs_pg"
  /usr/local/bin/juicefs umount /mnt/jfs_pg || umount -l /mnt/jfs_pg || true
  rm -rf /var/jfsCache/*
  "${SSH_SRV[@]}" "rm -rf /var/lib/minio/data/jfs-pg/* && mkdir -p /var/lib/minio/data/jfs-pg"
fi

# ------------------------------------------------------------------------------
# Target 3: JuiceFS + Redis 7 (jfs_redis)
# ------------------------------------------------------------------------------
if has_target "jfs_redis"; then
  "${SSH_SRV[@]}" "/usr/local/bin/bowl-prod-ctl stop-storage 2>/dev/null || true; systemctl stop postgresql 2>/dev/null || true; systemctl start redis-server jfs-s3 jfs-webdav"
  export AWS_ACCESS_KEY_ID=minioadmin
  export AWS_SECRET_ACCESS_KEY=minioadmin
  export MINIO_ACCESS_KEY=minioadmin
  export MINIO_SECRET_KEY=minioadmin
  REDIS_META_URL="redis://${SERVER_PRIV_IP}:6379/1"
  if ! /usr/local/bin/juicefs format \
    --storage minio \
    --bucket "http://${SERVER_PRIV_IP}:9001/jfs-redis" \
    --access-key minioadmin \
    --secret-key minioadmin \
    --trash-days 0 \
    --force \
    "${REDIS_META_URL}" \
    jfs-redis-bench </dev/null; then
    /usr/local/bin/juicefs format \
      --storage webdav \
      --bucket "http://${SERVER_PRIV_IP}:9002/jfs-redis/" \
      --trash-days 0 \
      --force \
      "${REDIS_META_URL}" \
      jfs-redis-bench </dev/null
  fi

  mkdir -p /mnt/jfs_redis
  /usr/local/bin/juicefs mount -d --no-usage-report "${REDIS_META_URL}" /mnt/jfs_redis </dev/null
  run_suites_on_mount "jfs_redis" "/mnt/jfs_redis"
  /usr/local/bin/juicefs umount /mnt/jfs_redis || umount -l /mnt/jfs_redis || true
  rm -rf /var/jfsCache/*
fi
BENCH_EOF

ssh "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}" "SERVER_PRIV_IP='${SERVER_PRIV_IP}' TARGETS_TO_RUN='${TARGETS_TO_RUN}' BOWL_BENCH_LOG_LEVEL='${BOWL_BENCH_LOG_LEVEL}' BOWL_BENCH_TRACE_SAMPLE_RATIO='${BOWL_BENCH_TRACE_SAMPLE_RATIO}' bash /tmp/run_client_bench.sh </dev/null"

TEST_COMPLETED_AT="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
TEST_END_EPOCH="$(date +%s)"
log "Benchmark targets (${TARGETS_TO_RUN}) completed at ${TEST_COMPLETED_AT}. Fetching results..."

if [[ -f "${RESULTS_DIR}/stage_timings.jsonl" ]]; then
  cp "${RESULTS_DIR}/stage_timings.jsonl" "${WORK_DIR}/prev_stage_timings.jsonl"
fi
scp -r "${CLIENT_SSH_OPTS[@]}" "root@${CLIENT_SSH_HOST}:/tmp/bench_results/"* "${RESULTS_DIR}/"
if [[ -f "${WORK_DIR}/prev_stage_timings.jsonl" ]]; then
  cat "${RESULTS_DIR}/stage_timings.jsonl" "${WORK_DIR}/prev_stage_timings.jsonl" | jq -sc 'unique_by(.target + ":" + .suite) | .[]' > "${WORK_DIR}/merged_timings.jsonl"
  mv "${WORK_DIR}/merged_timings.jsonl" "${RESULTS_DIR}/stage_timings.jsonl"
fi
if [[ -f "${RESULTS_DIR}/bowl_window.json" ]]; then
  TEST_START_EPOCH="$(jq -r '.start_epoch // empty' "${RESULTS_DIR}/bowl_window.json")"
  TEST_END_EPOCH="$(jq -r '.end_epoch // empty' "${RESULTS_DIR}/bowl_window.json")"
fi
if [[ -f "${RESULTS_DIR}/bowl_disk.json" ]]; then
  BOWL_DISK_JSON="$(cat "${RESULTS_DIR}/bowl_disk.json")"
elif [[ " ${TARGETS_TO_RUN} " == *" bowl "* ]]; then
  log "ERROR: Bowl 磁盘信息缺失，无法生成本轮报告。"
  exit 1
else
  BOWL_DISK_JSON='{}'
fi

# ==============================================================================
# Step 6: Destroy Cloud Instances, Audit Cleanup & Generate REPORT.md
# ==============================================================================
fetch_telemetry_archive
ssh "${SERVER_SSH_OPTS[@]}" "root@${SERVER_SSH_HOST}" "/usr/local/bin/bowl-prod-ctl down-volumes" >/dev/null 2>&1 || true
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
  --arg vpc_id "vpc-<redacted>" \
  --arg vswitch_id "vsw-<redacted>" \
  --arg sg_id "sg-<redacted>" \
  --arg instance_type "${INSTANCE_TYPE}" \
  --arg image_id "${IMAGE_ID}" \
  --arg disk_cat "${SYSTEM_DISK_CATEGORY}" \
  --argjson disk_size "${SYSTEM_DISK_SIZE}" \
  --arg server_id "i-<redacted-server>" \
  --arg server_priv_ip "${SERVER_PRIV_IP}" \
  --arg client_id "i-<redacted-client>" \
  --arg client_priv_ip "${CLIENT_PRIV_IP}" \
  --arg bowl_commit "${BOWL_GIT_COMMIT}" \
  --arg key_pair "${KEY_PAIR_NAME}" \
  --arg created_at "${INSTANCES_CREATED_AT}" \
  --arg deleted_at "${INSTANCES_DELETED_AT:-kept}" \
  --argjson remaining "${REMAINING_INSTANCES}" \
  --argjson srv "${SERVER_INFO_JSON}" \
  --argjson cli "${CLIENT_INFO_JSON}" \
  --argjson bowl_disk "${BOWL_DISK_JSON}" \
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
    bowl_config: {
      bowl_git_commit: $bowl_commit,
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
      disk_id: $bowl_disk.disk_id,
      root_ino: $bowl_disk.root_ino,
      disk_size: "2PiB",
      atime_mode: "noatime",
      entry_timeout: "1.0s",
      attr_timeout: "1.0s",
      dir_entry_timeout: "1.0s"
    },
    juicefs_config: {
      storage_backend: "rclone S3 (Server node local NVMe SSD)",
      trash_days: 0,
      block_size: "4MiB (default)",
      chunk_size: "64MiB (default)"
    },
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
