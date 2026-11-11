# Bowl `pjdfstest` 阿里云硅谷双机内网基准测试套件

> 最新复跑：2026-11-12，Bowl `8afc63c`（`ecs.i5.xlarge` 本地 NVMe SSD `local_ssd_pro` 894 GiB）。完整结果见 [REPORT.md](REPORT.md) 与 [results/telemetry/overview.md](results/telemetry/overview.md)。

本轮在 17 个系统调用目录、238 个 `.t` 脚本、8,798 项 POSIX 断言中达成 **`8,798 / 8,798` 全量通过（通过率 `100.00%`，`0` 失败；28 项 TAP TODO 中 9 项预期失败按 `prove` 规则不计入失败）**；全量执行耗时 `172.84s`（`summary.json` 精确值 `172.845s`，较 `9c60a34-dirty` 的 `191.94s` 缩短 `19.09s` / `-10.0%`），内网综合吞吐 `50.90 tests/s`。此前存量的 9 项 256 字节超长文件名边界断言已通过 `NamespaceCache` 权威完整目录查找前置校验 `len(name) > 255` 返回 `ENAMETOOLONG` 彻底清零；同时 `h2CoalesceConn` 请求帧合并 + `net.ipv4.tcp_autocorking=0` 使全套件 `9,471` 次数据/元数据请求在服务端外层 HTTP 100% 落入 `<= 2.5 ms`（客户端 RPC `99.84% <= 1.0 ms`，Jaeger 最慢单条 Trace 仅 `0.984 ms`）。

本目录包含基于阿里云 CLI（`aliyun`）在硅谷地域（`us-west-1`）自动拉起 2 台按量计费本地 NVMe SSD 云服务器（`ecs.i5.xlarge` × 2，1 台 `storage-service` 服务端节点 + 1 台 FUSE 客户端节点），在同可用区 VPC 私网下运行完整 [`pjdfstest`](https://github.com/pjd/pjdfstest) POSIX 兼容性与性能基准测试集的自动化脚本、实测原始数据及完整测试报告。

默认评测直接复用 [`deploy/prod/docker-compose.yaml`](../../deploy/prod/docker-compose.yaml) 与 [`deploy/prod/storage.env`](../../deploy/prod/storage.env) 拉起完整生产与可观测性容器栈（PostgreSQL 18、Goose 迁移、`storage-service`、Jaeger、Prometheus、Loki、Alloy、Grafana，`/var/lib/docker` 绑定至本地 NVMe SSD `/mnt/nvme/docker`）。被测版本采用 ADR-0030 / ADR-0031 / ADR-0032 单盘独立 Actor 存储引擎与 `64 MiB` Append-Only Pack 数据层：PostgreSQL 18 仅保存单张 `disks` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在 `packs/<pack_id_hex16>.pack` 的 `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `WriteRequest` 池化零分配解码、活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。

客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备 `h2CoalesceConn`（HTTP/2 `HEADERS+DATA` 请求帧合并 + `TCP_NODELAY`，根除 `tcp_autocorking` 与 Delayed ACK 互锁）、`WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 异步顺序预读流水线（单流 `W_read=4`、全局并发预算 `ReadAheadMaxGlobalInflight=12`）与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。同时默认开启跨机 OTLP 追踪上报至 Alloy 尾部采样流水线（`http://<server_ip>:4317`，`Fsync` 提权采样 + 错误/`>10ms` 慢链路 100% 保留入库 Jaeger）与 `--admin-addr 0.0.0.0:9567`（暴露 `/debug/stats` 9 大子系统快照、26 桶延迟直方图、Prometheus `/metrics` 与 `pprof`；不启用 `--stats-file` 以避免根目录虚拟 `.stats` 干扰 POSIX 目录断言）。Flush 排空异步写缓冲但不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。本套件在 `deploy/prod/storage.env` 之上仅覆写 `STORAGE_ATIME_MODE=strictatime` 与 0 秒属性/目录项超时以覆盖真实 POSIX 元数据路径，双端数据与构建工作目录全量挂载于 894 GiB 本地 NVMe SSD（`/mnt/nvme`）。

## 目录结构

| 文件 / 目录 | 说明 |
| :--- | :--- |
| [`REPORT.md`](./REPORT.md) | 阿里云硅谷（`us-west-1a`）双机内网 `pjdfstest` 完整测试报告（含环境拓扑、内网基线、17 个系统调用目录汇总、238 个 `.t` 用例明细、生产可观测性与慢路径遥测诊断及云资源清理审计） |
| [`run_benchmark.sh`](./run_benchmark.sh) | 端到端自动化脚本：本地交叉编译 -> 阿里云双机按量开通 -> 基于 [`deploy/prod`](../../deploy/prod) 部署挂载 -> 执行 `pjdfstest` -> 收割全栈遥测并生成报告 -> 自动强制释放云服务器 |
| [`../common/prod_stack.sh`](../common/prod_stack.sh) | 公共 `deploy/prod` 容器栈打包、远端 NVMe Docker 部署控制（`bowl-prod-ctl`）与双端遥测采集脚本 |
| [`../common/collect_telemetry.py`](../common/collect_telemetry.py) | 遥测蒸馏器：将原始监控/追踪/剖析数据提炼为可读慢路径诊断摘要与 `REPORT.md` 遥测章节 |
| [`parse_results.py`](./parse_results.py) | `prove --timer -rv` TAP 输出与 `results/telemetry/` 解析器及 `REPORT.md` 渲染工具 |
| [`results/summary.json`](./results/summary.json) | 结构化测试汇总数据（总体通过率、耗时、17 个目录、238 个脚本明细及遥测诊断摘要） |
| [`results/env_metadata.json`](./results/env_metadata.json) | 双机硬件规格、内核版本、`deploy/prod` 配置快照、VPC 内网 ICMP RTT 与 `iperf3` 带宽基线、云资源销毁审计元数据 |
| [`results/stage_timings.jsonl`](./results/stage_timings.jsonl) | 17 个系统调用目录的分项执行耗时与退出码记录 |
| [`results/telemetry/`](./results) | 轻量级可追踪遥测诊断产物：`overview.{md,json}`、`phase_deltas.json`（分工作负载全链路逐跳拆解）、`client_stats/`、`jaeger/`、`prometheus/metrics_summary.json`、`loki/*.jsonl`、`pebble/`、`pprof_top/*.txt` |
| `results/telemetry-raw.tar.gz` | 原始二进制遥测归档（`.pb.gz` 火焰图、`.prom` 快照、Pebble `LOG*`/`pebble_events.log`、`/proc/diskstats`，已加入 `.gitignore`） |
| [`results/pjdfstest.log`](./results/pjdfstest.log) | 全量 238 个 `.t` 测试脚本的原始 TAP 输出日志 |

## 快速复现

### 1. 前置条件
- 本机已安装并登录阿里云 CLI（`aliyun`）：
  ```bash
  aliyun configure --mode OAuth --oauth-site-type CN
  ```
- 本机已安装 `go`（>= 1.26）、`jq`、`python3`。

### 2. 一键运行阿里云硅谷双机实测
```bash
bash benchmark/pjdfstest/run_benchmark.sh
```

支持通过环境变量覆盖默认云资源与可观测性参数：
- `REGION_ID`（默认 `us-west-1`）
- `ZONE_ID`（默认 `us-west-1a`）
- `INSTANCE_TYPE`（默认 `ecs.i5.xlarge`，配备 894 GiB 本地 NVMe SSD `local_ssd_pro`）
- `BOWL_BENCH_TRACE_SAMPLE_RATIO`（默认 `0.10` 即 10% OTel 采样并配合 Jaeger 按操作与慢链路分层抓取；全量采样可设为 `1.0`）
- `BOWL_BENCH_LOG_LEVEL`（默认 `warn`）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/pjdfstest/parse_results.py \
  --results-dir benchmark/pjdfstest/results \
  --summary-out benchmark/pjdfstest/results/summary.json \
  --report-out benchmark/pjdfstest/REPORT.md
```
