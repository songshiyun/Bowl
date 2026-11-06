# Bowl `pjdfstest` 阿里云硅谷双机内网基准测试套件

> 最新复跑：2026-11-06，Bowl `a253128`。完整结果见 [REPORT.md](REPORT.md)。

本轮在 17 个系统调用目录、238 个 `.t` 脚本、8,798 项 POSIX 断言中通过 `8,789 / 8,798`（通过率 `99.90%`，28 项 TODO 中 9 项预期失败）；全量执行耗时 `169.14s`，内网综合吞吐 `52.01 tests/s`。未通过的 9 项子测试（分布于 `chmod/02.t`、`chown/02.t`、`ftruncate/02.t`、`link/02.t`、`rename/01.t`、`rmdir/02.t`、`truncate/02.t`、`unlink/02.t`）均为 256 字节超长文件名边界断言（预期 `ENAMETOOLONG`，因 `29c2bc3` 引入 `completeDirs` 权威完整目录本地未命中短路提前返回 `ENOENT`）。

本目录包含基于阿里云 CLI（`aliyun`）在硅谷地域（`us-west-1`）自动拉起 2 台按量计费云服务器（1 台 `storage-service` 服务端节点 + 1 台 FUSE 客户端节点），在同可用区 VPC 私网下运行完整 [`pjdfstest`](https://github.com/pjd/pjdfstest) POSIX 兼容性与性能基准测试集的自动化脚本、实测原始数据及完整测试报告。

被测版本采用 ADR-0029：PostgreSQL 16 仅保存 `disks`、`commits` 和 `client_sessions` 控制面表。每个 `(disk, branch)` 使用常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 与 12 字节 `BlobRef` 索引；文件数据落在 `blobs/<shard>/<blob_id>.blob` 的 1 MiB 不可变块文件中，整块直写、局部写 CoW，截断/打洞/删除在提交后 `os.Remove` 即时回收旧块，Checkpoint 与 Fork 以硬链接共享块。服务端分支锁降为拓扑生命周期 `RLock`，常规读写按 inode 细粒度加锁，完整 1 MiB 块在 Inode 锁外并行落盘；`GetAttr`/`GetXattr`/`Readlink` 单键点查免 Snapshot；读路径 atime 先登记内存再按 1 秒周期合并；常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`）。FileHandle、FileLock、O_TMPFILE 和 O(1) 索引的 NamespaceCache 由客户端在内存中维护。

客户端采用 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，避免 4 KiB 脏页回写与写前尺寸核对；保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。本套件为覆盖真实元数据路径，保持 `strictatime` 与 0 秒属性/目录项超时。

## 目录结构

| 文件 / 目录 | 说明 |
| :--- | :--- |
| [`REPORT.md`](./REPORT.md) | 阿里云硅谷（`us-west-1a`）双机内网 `pjdfstest` 完整测试报告（含环境拓扑、内网基线、17 个系统调用目录汇总、238 个 `.t` 用例明细及云资源清理审计） |
| [`run_benchmark.sh`](./run_benchmark.sh) | 端到端自动化脚本：本地交叉编译 -> 阿里云双机按量开通 -> 内网部署挂载 -> 执行 `pjdfstest` -> 回传日志并生成报告 -> 自动强制释放云服务器 |
| [`parse_results.py`](./parse_results.py) | `prove --timer -rv` TAP 输出解析器与 `REPORT.md` 渲染工具 |
| [`results/summary.json`](./results/summary.json) | 结构化测试汇总数据（总体通过率、耗时、17 个目录与 238 个脚本明细） |
| [`results/env_metadata.json`](./results/env_metadata.json) | 双机硬件规格、内核版本、VPC 内网 ICMP RTT 与 `iperf3` 带宽基线、云资源销毁审计元数据 |
| [`results/stage_timings.jsonl`](./results/stage_timings.jsonl) | 17 个系统调用目录的分项执行耗时与退出码记录 |
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

支持通过环境变量覆盖默认云资源参数：
- `REGION_ID`（默认 `us-west-1`）
- `ZONE_ID`（默认 `us-west-1a`）
- `INSTANCE_TYPE`（默认 `ecs.c9i.xlarge`）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/pjdfstest/parse_results.py \
  --results-dir benchmark/pjdfstest/results \
  --summary-out benchmark/pjdfstest/results/summary.json \
  --report-out benchmark/pjdfstest/REPORT.md
```
