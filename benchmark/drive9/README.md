# Bowl vs Drive9 (`mem9-ai/drive9`) 性能基准测试套件

> 最新复跑：2026-11-04，Bowl `1f7cde9`（BlobRef + blobs 块文件解耦、生命周期锁与 1 MiB Write-Through），Drive9 参考版本 `b529a36f`。冷缓存与热缓存各 45 项微观/宏观负载完成，三组 SHA-256 与 SQLite 完整性指纹一致。完整结果见 [REPORT.md](REPORT.md)。

本轮相对 Drive9 默认模式：冷缓存 `mkdir` 3.60x、`rename` 8.99x、`unlink` 7.33x、顺序读 3.23x；`juicefs bench` 大文件写达到 295.12 MiB/s（上一轮 11.63 MiB/s），冷缓存单流 256 MiB 顺序写 183.19 MiB/s。小文件与 `stat` 低于同轮 juicefs 套件的 Bowl 结果，差异未定位，需单独复跑确认。

本目录包含 **Bowl** 与 **[mem9-ai/drive9](https://github.com/mem9-ai/drive9)** 在阿里云硅谷地域（`us-west-1a`）双机 VPC 私网环境下的一键自动化对比基准测试脚本、内置评测负载与实测报告。

## 1. 评测矩阵与测试套件

在相同的 2 台 `ecs.c9i.xlarge`（4 vCPU / 8 GiB / 120 GiB `cloud_essd`）节点上，同时对比三组挂载模式：

1. **`bowl`**：PostgreSQL 16 仅保存 `disks`、`commits`、`client_sessions` 控制面表；每个 `(disk, branch)` 使用常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 与 12 字节 `BlobRef` 索引，文件数据落在 `blobs/<shard>/<blob_id>.blob` 的 1 MiB 不可变块文件中，整块直写、局部写 CoW，删除即时 `os.Remove` 回收，Checkpoint 与 Fork 以硬链接共享块；服务端分支锁降为拓扑生命周期 `RLock`，完整 1 MiB 块在 Inode 锁外并行落盘，单键点查免 Snapshot，读路径 atime 先登记内存再按 1 秒周期合并；常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`）。FileHandle、FileLock、O_TMPFILE 和 O(1) 索引的 NamespaceCache 由客户端在内存中维护。
2. **`drive9_default`**：Drive9 默认模式（TiDB v8.5.5 + Drive9 Server Local S3，`--profile=coding-agent --durability=interactive --write-cache-size-mb=0`，启用本地 Write-Back 暂存、本地磁盘读缓存与 `.git`/构建目录 Local Overlay）。
3. **`drive9_sync`**：Drive9 远端同步模式（`--profile=none --durability=close-sync --write-cache-size-mb=0 --attr-ttl=1s --entry-ttl=1s --dir-ttl=1s`，禁用 Local Overlay，对齐 `1.0s` 内核缓存与同步 `close`/`fsync` 落盘语义）。

Bowl 客户端采用 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。不同存储栈的缓存与写入确认条件不同，性能数值需结合各自配置解读。

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
