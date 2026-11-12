# Bowl vs JuiceFS 阿里云硅谷双机内网存储性能基准测试套件

> 最新复跑：2026-11-12，Bowl `6399122`（`ecs.i5.xlarge` 本地 NVMe SSD `local_ssd_pro` 894 GiB）。三组存储栈（`bowl`、`jfs_pg`、`jfs_redis`）同机按目标隔离完成 `juicefs bench`、8 项 `fio`（24 份，错误码均为 0）和单进程/四进程 `mdtest`（31 项指标对 `jfs_pg` **31/31 全胜**、对 `jfs_redis` **30/31 胜出**）。本轮 Bowl `juicefs bench` 大文件写达 **`1,705.25 MiB/s`**（为 JuiceFS PG/Redis 的 **`2.39x`/`2.43x`**）、大文件读达 **`1,904.12 MiB/s`**（**`1.12x`/`1.18x`**，较 `8afc63c` 提升 **`+46.89%`**）、小文件写 **`2,001.50 files/s`**（**`2.93x`/`1.76x`**）、小文件读 **`4,390.70 files/s`**（为 PG 的 **`1.37x`**、Redis 的 `0.84x`）、Stat **`29,482.90 files/s`**（**`2.08x`/`1.70x`**）；`fio` **单流顺序写 `1,453.51 MiB/s`**（**`2.04x`/`2.06x`**）、**单流顺序读 `1,662.34 MiB/s`**（**`1.13x`/`1.17x`**，较 `8afc63c` 提升 **`+57.22%`**）、**16 并发顺序写 `1,612.92 MiB/s`**（**`2.28x`/`2.31x`**）、**16 并发顺序读 `1,972.31 MiB/s`**（**`1.16x`/`1.15x`**，较 `8afc63c` 提升 **`+115.13%`**）、4 并发 Direct 顺序写 **`1,760.21 MiB/s`**（**`2.30x`/`2.33x`**）、**4 并发 Direct 顺序读 `2,352.67 MiB/s`**（**`1.41x`/`1.41x`**，较 `8afc63c` 提升 **`+65.59%`**）、**4 并发 Direct 随机写 `1,361.25 MiB/s`**（**`3.99x`/`4.03x`**）、**4 并发 Direct 随机读 `1,404.18 MiB/s`**（**`2.80x`/`2.80x`**）；单进程 `mdtest` 目录创建/文件创建/文件删除达 **`3,928.74 / 3,564.46 / 4,116.35 ops/s`**（为 PG 的 `6.11x / 2.99x / 4.25x`、Redis 的 `1.87x / 1.72x / 2.11x`），四进程 `mdtest` 目录创建/文件创建/文件删除达 **`9,015.71 / 10,282.53 / 12,098.09 ops/s`**、文件 Stat `1,315,254.21 ops/s`。完整结果见 [REPORT.md](REPORT.md) 与 [results/telemetry/overview.md](results/telemetry/overview.md)。

本目录包含对标 [JuiceFS 官方常规测试](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 [`juicefs bench`](https://juicefs.com/docs/zh/community/performance_evaluation_guide/#juicefs-bench)、[`fio` 吞吐量测试](https://juicefs.com/docs/zh/community/fio/)、[`mdtest` 元数据 IOPS 测试](https://juicefs.com/docs/zh/community/mdtest/)）的端到端自动化评测脚本、实测原始数据及完整对比报告。

默认测试在与 [`benchmark/pjdfstest`](../pjdfstest) 相同地域的**阿里云硅谷（`us-west-1a`）按量计费本地 NVMe SSD 双机 VPC 私网环境**（`ecs.i5.xlarge` × 2，配备 894 GiB `local_ssd_pro` 本地 NVMe SSD）中自动拉起，并在同一套硬件与内网拓扑下按目标隔离服务端进程对以下三组存储栈进行同机实测对比：

1. **Bowl（`deploy/prod` 生产容器栈：PostgreSQL 18 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack + 全栈可观测性）**：默认直接复用 [`deploy/prod/docker-compose.yaml`](../../deploy/prod/docker-compose.yaml) 与 [`deploy/prod/storage.env`](../../deploy/prod/storage.env) 拉起完整生产栈（PostgreSQL 18、Goose 迁移、`storage-service`、Jaeger、Prometheus、Loki、Alloy、Grafana，`/var/lib/docker` 绑定至本地 NVMe SSD `/mnt/nvme/docker`）。PostgreSQL 18 仅保存单张 `disks` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在本地 NVMe SSD 的 `packs/<pack_id_hex16>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与 `64 MiB` 分片热块缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `128 MiB` H2C 连接窗口、`WriteRequest`/`ReadResponse` 池化零分配编解码、`8 MiB` `SyncFileRange` 渐进异步回写与封口包预刷盘、活跃包末尾同块 `ExtendTail` 顺延扩展、相邻物理连续块合并单次 `pread` 无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及后台异步全死封口包回收。
2. **JuiceFS (PostgreSQL 16 + Server rclone S3)**：PostgreSQL 16 元数据引擎 + Server 节点本地 NVMe SSD（`/mnt/nvme`）上的 rclone S3 对象存储（`--trash-days 0`），客户端缓存目录 `/var/jfsCache` 挂载于 Client 本地 NVMe SSD。
3. **JuiceFS (Redis 7 + Server rclone S3)**：JuiceFS 官方常规测试默认的 Redis 元数据引擎（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）+ Server 节点本地 NVMe SSD 上的 rclone S3 对象存储（`--trash-days 0`）。

Bowl 客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备客户端 `1+8` H2C 多连接条带池（`32 MiB` 元数据连接 + `8×64 MiB` 数据连接，单流 `8 MiB`）、`h2CoalesceConn`（HTTP/2 `HEADERS+DATA` 请求帧合并 + `TCP_NODELAY`）、`MarshalAppend` 零分配编码、`WriteBehindCoordinator`（大块流式写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发 Write RPC 流水线，小块连续写经首笔探针或 `Create` 预热后内存聚合）、`ReadAheadCoordinator` 单飞有序槽位预读（单流 `W_read=4`、全局并发预算 `ReadAheadMaxGlobalInflight=32`）与 `completeDirs` 本地目录流合成，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。同时默认开启跨机 OTLP 追踪上报至 Alloy 尾部采样流水线（`http://<server_ip>:4317`，`Fsync` 提权采样 + 错误/`>10ms` 慢链路 100% 保留入库 Jaeger）、`--admin-addr 0.0.0.0:9567`（暴露 `bowl stats` 9 大子系统快照、26 桶延迟直方图、Prometheus `/metrics` 与 `pprof`）及 `--stats-file` 挂载点实时统计。Flush 排空异步写缓冲但不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`。不同存储栈的缓存与写入确认条件不同，性能数值需结合各自配置解读。

## 目录结构

| 文件 / 目录 | 说明 |
| :--- | :--- |
| [`REPORT.md`](./REPORT.md) | 阿里云硅谷（`us-west-1a`）双机内网 Bowl vs JuiceFS 完整性能基准测试报告（含环境拓扑、内网与磁盘基线、`juicefs bench`、`fio` 8 组负载、`mdtest` 单流与多并发对比、生产可观测性与慢路径遥测诊断、比较边界及云资源销毁审计） |
| [`run_benchmark.sh`](./run_benchmark.sh) | 端到端自动化脚本：本地交叉编译 -> 阿里云硅谷双机按量开通 -> 基于 [`deploy/prod`](../../deploy/prod) 部署 Bowl 生产与可观测性栈及 JuiceFS 对照栈 -> 串行执行三大基准测试套件 -> 收割全栈遥测并生成报告 -> 自动强制释放云服务器 |
| [`../common/prod_stack.sh`](../common/prod_stack.sh) | 公共 `deploy/prod` 容器栈打包、远端 NVMe Docker 部署控制（`bowl-prod-ctl`）与双端遥测采集脚本 |
| [`../common/collect_telemetry.py`](../common/collect_telemetry.py) | 遥测蒸馏器：将原始监控/追踪/剖析数据提炼为可读慢路径诊断摘要与 `REPORT.md` 遥测章节 |
| [`parse_results.py`](./parse_results.py) | `juicefs bench`、`fio` JSON、`mdtest` 输出及 `results/telemetry/` 解析器与 `REPORT.md` 渲染工具 |
| [`results/summary.json`](./results/summary.json) | 结构化测试汇总数据（三大存储栈在全部测试套件中的吞吐量、IOPS、延迟、相对倍数及遥测诊断摘要） |
| [`results/env_metadata.json`](./results/env_metadata.json) | 双机硬件规格、软件版本、`deploy/prod` 配置快照、VPC 内网 RTT / `iperf3` 带宽基线、云盘本地基线及云资源销毁审计元数据 |
| [`results/telemetry/`](./results) | 轻量级可追踪遥测诊断产物：`overview.{md,json}`、`phase_deltas.json`（9 大子工作负载全链路逐跳拆解）、`client_stats/`、`jaeger/`、`prometheus/metrics_summary.json`、`loki/*.jsonl`、`pebble/`、`pprof_top/*.txt` |
| `results/telemetry-raw.tar.gz` | 原始二进制遥测归档（`.pb.gz` CPU/Heap/Mutex/Block 火焰图、`.prom` 快照、Pebble `LOG*`/`pebble_events.log`、`/proc/diskstats`，已加入 `.gitignore`） |
| [`results/`](./results) | 各存储栈的 `bench_*.log`、`fio_*.json`、`mdtest_*.log` 原始实测输出文件 |

## 快速复现

### 1. 前置条件
- 本机已安装并登录阿里云 CLI（`aliyun`）：
  ```bash
  aliyun configure --mode OAuth --oauth-site-type CN
  ```
- 本机已安装 `go`（>= 1.26）、`jq`、`python3`。

### 2. 一键运行阿里云硅谷双机实测
```bash
VPC_ID=vpc-xxx VSWITCH_ID=vsw-xxx SECURITY_GROUP_ID=sg-xxx bash benchmark/juicefs/run_benchmark.sh
```

必填网络参数：`VPC_ID`、`VSWITCH_ID`、`SECURITY_GROUP_ID`。支持通过环境变量覆盖默认云资源与可观测性参数：
- `REGION_ID`（默认 `us-west-1`）
- `ZONE_ID`（默认 `us-west-1a`）
- `INSTANCE_TYPE`（默认 `ecs.i5.xlarge`，配备 894 GiB 本地 NVMe SSD `local_ssd_pro`）
- `SYSTEM_DISK_SIZE`（默认 `40` GiB `cloud_essd` 系统盘）
- `BOWL_BENCH_TRACE_SAMPLE_RATIO`（默认 `0.10` 即 10% OTel 采样并配合 Jaeger 按操作与慢链路分层抓取；全量采样可设为 `1.0`）
- `BOWL_BENCH_LOG_LEVEL`（默认 `warn`）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/juicefs/parse_results.py \
  --results-dir benchmark/juicefs/results \
  --summary-out benchmark/juicefs/results/summary.json \
  --report-out benchmark/juicefs/REPORT.md
```
