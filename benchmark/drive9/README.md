# Bowl vs Drive9 (`mem9-ai/drive9`) 性能基准测试套件

> 最新复跑：2026-11-02，Bowl `ee8d113`。完整结果见 [REPORT.md](REPORT.md)。

本目录包含 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）双机 VPC 私网环境下的一键自动化对比基准测试脚本、内置评测负载与实测报告。

## 1. 评测矩阵与测试套件

在相同的 2 台 `ecs.c9i.xlarge`（4 vCPU / 8 GiB / 120 GiB `cloud_essd`）节点上，同时对比三组挂载模式：

1. **`bowl`**：Bowl 生产默认配置（PostgreSQL 16 + Server BlockStore，`noatime` + `1.0s` FUSE 内核缓存，无客户端本地盘缓存，普通 Write 写入服务端页缓存，显式 Fsync 仅同步数据库 WAL，块数据由内核后台回写（ADR-0026））。
2. **`drive9_default`**：Drive9 默认模式（TiDB v8.5.5 + Drive9 Server Local S3，`--profile=coding-agent --durability=interactive --write-cache-size-mb=0`，启用本地 Write-Back 暂存、本地磁盘读缓存与 `.git`/构建目录 Local Overlay）。
3. **`drive9_sync`**：Drive9 远端同步模式（`--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`，禁用 Local Overlay，对齐 `1.0s` 内核缓存与同步 `close`/`fsync` 落盘语义）。

每组目标依次运行三大基准测试套件：

- **套件一（`juicefs bench -p 4`）**：4 并发 `1 GiB × 4` 大文件读写、400 个 `128 KiB` 小文件并发写/读/`stat`。
- **套件二（[`drive9_fuse_bench.py`](drive9_fuse_bench.py)）**：Drive9 官方内置 FUSE 基准工具，分别在 **冷缓存（Cold Cache，`drop_caches`）** 与 **热缓存（Hot Cache）** 下运行 10 项微观负载（`sequential_write`、`sequential_read`、`random_read_4k`、`small_write_4k_fsync`、`mkdir`、`rename`、`unlink`、`readdir_small`、`readdir_large`、`stat`）与 5 项宏观研发负载（`macro_git_clone`、`macro_git_status_dirty`、`macro_git_diff_dirty`、`macro_find_files`、`macro_go_build`）。
- **套件三（[`fuse_perf_baseline.py`](fuse_perf_baseline.py)）**：提取自 `drive9/e2e/fuse-performance-baseline.sh`，执行带 SHA-256 全量校验的小文件/大文件读写及 SQLite `DELETE` 与 `WAL`（`PRAGMA synchronous=FULL`）事务插入、更新、聚合校验读与 `PRAGMA wal_checkpoint(TRUNCATE)`。

## 2. 目录结构

- [`run_aliyun_drive9_benchmark.sh`](run_aliyun_drive9_benchmark.sh)：端到端自动化脚本（交叉编译 Bowl 与 Drive9 -> 创建阿里云按量双机 -> 部署 PG 16 / Bowl / TiDB v8.5.5 / Drive9 Server -> 挂载并运行三大测试套件 -> 拉取结果 -> 销毁云主机与密钥对 -> 生成报告）。
- [`drive9_fuse_bench.py`](drive9_fuse_bench.py)：同步自 `drive9/tools/bench/drive9_fuse_bench.py` 的无依赖 FUSE 微观/宏观基准测试工具。
- [`fuse_perf_baseline.py`](fuse_perf_baseline.py)：提取自 `drive9/e2e/fuse-performance-baseline.sh` 的 SHA-256 文件读写与 SQLite `DELETE`/`WAL` 基准测试脚本。
- [`parse_results.py`](parse_results.py)：解析三套基准原始日志与 JSON 数据，生成 [`results/summary.json`](results/summary.json) 与 [`REPORT.md`](REPORT.md)。
- [`REPORT.md`](REPORT.md)：最新一轮阿里云硅谷双机实测完整对比报告。

## 3. 运行方式

```bash
# 默认从 /Users/fanxing/code/other/filesystem/drive9 编译 Drive9，可通过 DRIVE9_REPO 指定路径
bash benchmark/drive9/run_aliyun_drive9_benchmark.sh
```
