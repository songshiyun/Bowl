# Bowl vs Drive9 (`mem9-ai/drive9`) 性能基准测试套件

> 最新复跑：2026-11-11，Bowl `9c60a34-dirty`（`ecs.i5.xlarge` 本地 NVMe SSD `local_ssd_pro` 894 GiB），Drive9 参考版本 `b529a36f`。冷缓存与热缓存各 45 项微观/宏观负载完成，三组 SHA-256 与 SQLite 完整性指纹完全一致。完整结果见 [REPORT.md](REPORT.md)。

本轮在双端 `drop_caches` 与服务端进程级隔离下，相对 Drive9 远端同步模式（`close-sync`）：`juicefs bench` 大文件写 **`2.52x`**（`1,699.27` vs `675.41 MiB/s`）、小文件写 `15.35x`（`1,392.00` vs `90.70 files/s`）、小文件读 `5.14x`（`3,970.50` vs `772.90 files/s`）、Stat `6.17x`（`115,646.00` vs `18,735.90 files/s`）；冷缓存 **`sequential_write` `3.31x`（`1,142.87` vs `345.52 MiB/s`，较 `c6e884e` 的 `186.86 MiB/s` 提升 `6.12x`）**、`sequential_read` `12.21x`（`653.52` vs `53.54 MiB/s`）、`mkdir` `3.02x`（`674.77` vs `223.72 ops/s`）、`rename` `42.47x`（`2,495.70` vs `58.76 ops/s`）、`unlink` `62.42x`（`2,596.60` vs `41.60 ops/s`）、`git clone` 加速 `1.15x`（`5.668s` vs `6.535s`）、`git status` 加速 `9.52x`（`0.080s` vs `0.761s`）。相对 Drive9 默认模式（`interactive` 本地 Write-Back + Local Overlay）：**`juicefs bench` 大文件写 `1.29x`（`1,699.27` vs `1,319.70 MiB/s`）**、**冷缓存 `sequential_write` `1.76x`（`1,142.87` vs `650.38 MiB/s`）**、`sequential_read` `1.14x`、`mkdir` `2.91x`、`rename` `27.38x`、`unlink` `26.15x`、`git status` 加速 `20.76x`、`git diff` 加速 `26.42x`；热缓存 `sequential_write` `1.79x`（`1,120.72` vs `627.21 MiB/s`）、`sequential_read` `30.54x`（`7,827.43` vs `256.27 MiB/s`）、`readdir_large` `13.27x`（`1,063,198.65` vs `80,094.33 ops/s`）、`go build` 加速 `1.62x`（`0.073s` vs `0.118s`）。

本目录包含 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）双机本地 NVMe SSD VPC 私网环境下的一键自动化对比基准测试脚本、内置评测负载与实测报告。

## 1. 评测矩阵与测试套件

在相同的 2 台 `ecs.i5.xlarge`（4 vCPU / 32 GiB / 894 GiB 本地 NVMe SSD `local_ssd_pro` + 40 GiB `cloud_essd` 系统盘）节点上，按目标隔离服务端守护进程，对比三组挂载模式：

1. **`bowl`（`deploy/prod` 生产容器栈 + 全栈可观测性）**：默认直接复用 [`deploy/prod/docker-compose.yaml`](../../deploy/prod/docker-compose.yaml) 与 [`deploy/prod/storage.env`](../../deploy/prod/storage.env) 拉起完整生产容器栈（PostgreSQL 18、Goose 迁移、`storage-service`、Jaeger、Prometheus、Loki、Alloy、Grafana，`/var/lib/docker` 绑定至本地 NVMe SSD `/mnt/nvme/docker`）。PostgreSQL 18 仅保存单张 `disks` 控制面表（ADR-0032）；每个 `Disk` 由独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache，ADR-0030/0031）承载，保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef`（`PackID + Offset + Length`）索引；文件数据落在本地 NVMe SSD 的 `packs/<pack_id_hex16>.pack` `64 MiB` Append-Only 封包文件中（`256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存，`AppendBlocks` 整块顺序写跳过用户态冗余拷贝，配合服务端 `WriteRequest` 池化零分配解码与封口包后台异步预刷盘）；常规写批次采用双缓冲 MPSC Group Commit 以 `pebble.NoSync` 异步提交 WAL，显式 `Fsync`/`FsyncDir` 执行双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData`）。
2. **`drive9_default`**：Drive9 默认模式（TiDB v8.5.5 + Drive9 Server Local S3 均挂载于 Server 本地 NVMe SSD，`--profile=coding-agent --durability=interactive --write-cache-size-mb=0`，启用 Client 本地 NVMe SSD Write-Back 暂存、本地磁盘读缓存与 `.git`/构建目录 Local Overlay）。
3. **`drive9_sync`**：Drive9 远端同步模式（`--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`，禁用 Local Overlay，对齐 `1.0s` 内核缓存与同步 `close`/`fsync` 落盘语义）。

Bowl 客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备对称的 `WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 顺序预读与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。同时默认开启跨机 OTLP 追踪上报（`http://<server_ip>:4317`）、`--admin-addr 0.0.0.0:9567`（暴露 `bowl stats` 9 大子系统快照、26 桶延迟直方图、Prometheus `/metrics` 与 `pprof`）及 `--stats-file` 挂载点实时统计。Flush 排空异步写缓冲但不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。冷缓存每轮测量前同时清理 Client 与 Server 双端 OS 页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`）。不同存储栈的缓存与写入确认条件不同，性能数值需结合各自配置解读。

每组目标依次运行三大基准测试套件：

- **套件一（`juicefs bench -p 4`）**：4 并发 `1 GiB × 4` 大文件读写、400 个 `128 KiB` 小文件并发写/读/`stat`。
- **套件二（[`drive9_fuse_bench.py`](drive9_fuse_bench.py)）**：Drive9 官方内置 FUSE 基准工具，分别在 **冷缓存（Cold Cache，`drop_caches`）** 与 **热缓存（Hot Cache）** 下运行 10 项微观负载（`sequential_write`、`sequential_read`、`random_read_4k`、`small_write_4k_fsync`、`mkdir`、`rename`、`unlink`、`readdir_small`、`readdir_large`、`stat`）与 5 项宏观研发负载（`macro_git_clone`、`macro_git_status_dirty`、`macro_git_diff_dirty`、`macro_find_files`、`macro_go_build`）。
- **套件三（[`fuse_perf_baseline.py`](fuse_perf_baseline.py)）**：提取自 `drive9/e2e/fuse-performance-baseline.sh`，执行带 SHA-256 全量校验的小文件/大文件读写及 SQLite `DELETE` 与 `WAL`（`PRAGMA synchronous=FULL`）事务插入、更新、聚合校验读与 `PRAGMA wal_checkpoint(TRUNCATE)`。

## 2. 目录结构

- [`run_benchmark.sh`](run_benchmark.sh)：端到端自动化脚本（交叉编译 Bowl 与 Drive9 -> 创建阿里云按量双机 -> 基于 [`deploy/prod`](../../deploy/prod) 部署 Bowl 生产与可观测性栈及 TiDB v8.5.5 / Drive9 Server -> 按目标隔离挂载并运行三大测试套件 -> 收割全栈遥测并拉取结果 -> 销毁云主机与密钥对 -> 生成报告）。
- [`../common/prod_stack.sh`](../common/prod_stack.sh) / [`../common/collect_telemetry.py`](../common/collect_telemetry.py)：公共 `deploy/prod` 容器栈部署编排、双端遥测采集与慢路径诊断提炼工具。
- [`drive9_fuse_bench.py`](drive9_fuse_bench.py)：同步自 `drive9/tools/bench/drive9_fuse_bench.py` 的无依赖 FUSE 微观/宏观基准测试工具。
- [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 SHA-256 文件读写与 SQLite `DELETE`/`WAL` 基准测试脚本。
- [`parse_results.py`](parse_results.py)：解析三套基准原始日志、JSON 数据与 `results/telemetry/` 遥测摘要，生成 [`results/summary.json`](results/summary.json) 与 [`REPORT.md`](REPORT.md)。
- `results/telemetry/` 与 `results/telemetry-raw.tar.gz`：轻量级可追踪慢路径诊断文件（`overview.{md,json}`、`phase_deltas.json` 分工作负载逐跳拆解、`client_stats/`、`jaeger/`、`prometheus/`、`loki/`、`pebble/`、`pprof_top/`）与原始二进制火焰图/指标/日志归档（已加入 `.gitignore`）。
- [`REPORT.md`](REPORT.md)：最新一轮阿里云硅谷双机实测完整对比报告。

## 3. 运行方式

```bash
# 默认从 /Users/fanxing/code/other/filesystem/drive9 编译 Drive9，可通过 DRIVE9_REPO 指定路径
# 支持通过 BOWL_BENCH_TRACE_SAMPLE_RATIO（默认 0.10）与 BOWL_BENCH_LOG_LEVEL（默认 warn）调整遥测采样
bash benchmark/drive9/run_benchmark.sh
```

### 基于已有日志重新生成报告
```bash
python3 benchmark/drive9/parse_results.py \
  --results-dir benchmark/drive9/results \
  --summary-out benchmark/drive9/results/summary.json \
  --report-out benchmark/drive9/REPORT.md
```
