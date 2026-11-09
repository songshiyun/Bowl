# Bowl vs Drive9 (`mem9-ai/drive9`) 性能基准测试套件

> 最新复跑：2026-11-09，Bowl `c6e884e`（`ecs.i5.xlarge` 本地 NVMe SSD `local_ssd_pro` 894 GiB），Drive9 参考版本 `b529a36f`。冷缓存与热缓存各 45 项微观/宏观负载完成，三组 SHA-256 与 SQLite 完整性指纹完全一致。完整结果见 [REPORT.md](REPORT.md)。

本轮在双端 `drop_caches` 与服务端进程级隔离下，相对 Drive9 远端同步模式（`close-sync`）：`juicefs bench` 大文件写 `1.23x`（`805.15` vs `656.10 MiB/s`）、小文件写 `9.55x`（`791.80` vs `82.90 files/s`）、小文件读 `3.66x`（`1,049.30` vs `286.40 files/s`）、Stat `208.77x`（`93,674.30` vs `448.70 files/s`）；冷缓存 `sequential_read` `9.15x`（`487.53` vs `53.28 MiB/s`）、`mkdir` `2.92x`（`607.89` vs `208.11 ops/s`）、`rename` `7.64x`（`390.35` vs `51.10 ops/s`）、`unlink` `10.44x`（`349.17` vs `33.43 ops/s`）、`git clone` 加速 `1.28x`（`5.587s` vs `7.152s`）、`git diff` 加速 `10.86x`（`0.066s` vs `0.715s`）。相对 Drive9 默认模式（`interactive` 本地 Write-Back + Local Overlay）：冷缓存 `sequential_read` `7.89x`（`487.53` vs `61.75 MiB/s`）、`mkdir` `2.82x`、`rename` `4.58x`、`unlink` `3.89x`、`git status` 加速 `12.68x`、`git diff` 加速 `13.06x`；热缓存 `sequential_read` `14.62x`（`6,503.59` vs `444.84 MiB/s`）、`readdir_large` `14.42x`（`1,155,025.17` vs `80,105.10 ops/s`）、`go build` 加速 `1.47x`（`0.086s` vs `0.126s`）。

本目录包含 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）双机本地 NVMe SSD VPC 私网环境下的一键自动化对比基准测试脚本、内置评测负载与实测报告。

## 1. 评测矩阵与测试套件

在相同的 2 台 `ecs.i5.xlarge`（4 vCPU / 32 GiB / 894 GiB 本地 NVMe SSD `local_ssd_pro` + 40 GiB `cloud_essd` 系统盘）节点上，按目标隔离服务端守护进程，对比三组挂载模式：

1. **`bowl`**：PostgreSQL 16 仅保存 `disks` 与 `commits` 控制面表（ADR-0032）；每个 `Disk` 由独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache，ADR-0030/0031）承载，保存 `sys:stat`（含持久化 `WriterEpoch`）、Inode、Entry、Xattr、Orphan 与 16 字节 `BlobRef` 索引；文件数据落在本地 NVMe SSD 的 `packs/pack-<id>.pack` `64 MiB` Append-Only 封包文件中（`256 KiB` 逻辑块网格、常驻句柄池与 `64 MiB` 分片热块缓存，支持 `ExtendTail` 与无锁并发 `Read`）；常规写批次采用双缓冲 MPSC Group Commit 以 `pebble.NoSync` 异步提交 WAL，显式 `Fsync`/`FsyncDir` 执行双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData`）。
2. **`drive9_default`**：Drive9 默认模式（TiDB v8.5.5 + Drive9 Server Local S3 均挂载于 Server 本地 NVMe SSD，`--profile=coding-agent --durability=interactive --write-cache-size-mb=0`，启用 Client 本地 NVMe SSD Write-Back 暂存、本地磁盘读缓存与 `.git`/构建目录 Local Overlay）。
3. **`drive9_sync`**：Drive9 远端同步模式（`--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`，禁用 Local Overlay，对齐 `1.0s` 内核缓存与同步 `close`/`fsync` 落盘语义）。

Bowl 客户端采用无会话 `RawFS` 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。冷缓存每轮测量前同时清理 Client 与 Server 双端 OS 页缓存（`sync && echo 3 > /proc/sys/vm/drop_caches`）。不同存储栈的缓存与写入确认条件不同，性能数值需结合各自配置解读。

每组目标依次运行三大基准测试套件：

- **套件一（`juicefs bench -p 4`）**：4 并发 `1 GiB × 4` 大文件读写、400 个 `128 KiB` 小文件并发写/读/`stat`。
- **套件二（[`drive9_fuse_bench.py`](drive9_fuse_bench.py)）**：Drive9 官方内置 FUSE 基准工具，分别在 **冷缓存（Cold Cache，`drop_caches`）** 与 **热缓存（Hot Cache）** 下运行 10 项微观负载（`sequential_write`、`sequential_read`、`random_read_4k`、`small_write_4k_fsync`、`mkdir`、`rename`、`unlink`、`readdir_small`、`readdir_large`、`stat`）与 5 项宏观研发负载（`macro_git_clone`、`macro_git_status_dirty`、`macro_git_diff_dirty`、`macro_find_files`、`macro_go_build`）。
- **套件三（[`fuse_perf_baseline.py`](fuse_perf_baseline.py)）**：提取自 `drive9/e2e/fuse-performance-baseline.sh`，执行带 SHA-256 全量校验的小文件/大文件读写及 SQLite `DELETE` 与 `WAL`（`PRAGMA synchronous=FULL`）事务插入、更新、聚合校验读与 `PRAGMA wal_checkpoint(TRUNCATE)`。

## 2. 目录结构

- [`run_benchmark.sh`](run_benchmark.sh)：端到端自动化脚本（交叉编译 Bowl 与 Drive9 -> 创建阿里云按量双机 -> 部署 PG 16 / Bowl / TiDB v8.5.5 / Drive9 Server -> 挂载并运行三大测试套件 -> 拉取结果 -> 销毁云主机与密钥对 -> 生成报告）。
- [`drive9_fuse_bench.py`](drive9_fuse_bench.py)：同步自 `drive9/tools/bench/drive9_fuse_bench.py` 的无依赖 FUSE 微观/宏观基准测试工具。
- [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 SHA-256 文件读写与 SQLite `DELETE`/`WAL` 基准测试脚本。
- [`parse_results.py`](parse_results.py)：解析三套基准原始日志与 JSON 数据，生成 [`results/summary.json`](results/summary.json) 与 [`REPORT.md`](REPORT.md)。
- [`REPORT.md`](REPORT.md)：最新一轮阿里云硅谷双机实测完整对比报告。

## 3. 运行方式

```bash
# 默认从 /Users/fanxing/code/other/filesystem/drive9 编译 Drive9，可通过 DRIVE9_REPO 指定路径
bash benchmark/drive9/run_benchmark.sh
```

### 基于已有日志重新生成报告
```bash
python3 benchmark/drive9/parse_results.py \
  --results-dir benchmark/drive9/results \
  --summary-out benchmark/drive9/results/summary.json \
  --report-out benchmark/drive9/REPORT.md
```
