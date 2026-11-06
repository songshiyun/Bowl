# Bowl vs JuiceFS 阿里云硅谷双机内网存储性能基准测试套件

> 最新复跑：2026-11-06，Bowl `a253128`。三组存储栈（`bowl`、`jfs_pg`、`jfs_redis`）同机完成 `juicefs bench`、8 项 `fio`（24 份，错误码均为 0）和单进程/四进程 `mdtest`。本轮 Bowl `juicefs bench` 大文件写 `334.66 MiB/s`、大文件读 `1,701.99 MiB/s`、小文件写 `1,110.80 files/s`、小文件读 `4,570.30 files/s`、Stat `121,621.00 files/s`；单进程 `mdtest` 目录创建 `3,533.74 ops/s`、文件删除 `3,069.28 ops/s`；四进程 `mdtest` 目录创建 `8,885.83 ops/s`、文件创建 `8,142.70 ops/s`、文件 Stat `1,466,449.72 ops/s`、文件删除 `9,511.20 ops/s`。完整结果见 [REPORT.md](REPORT.md)。

本目录包含对标 [JuiceFS 官方常规测试](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 [`juicefs bench`](https://juicefs.com/docs/zh/community/performance_evaluation_guide/#juicefs-bench)、[`fio` 吞吐量测试](https://juicefs.com/docs/zh/community/fio/)、[`mdtest` 元数据 IOPS 测试](https://juicefs.com/docs/zh/community/mdtest/)）的端到端自动化评测脚本、实测原始数据及完整对比报告。

默认测试在与 [`benchmark/pjdfstest`](../pjdfstest) 相同地域的**阿里云硅谷（`us-west-1a`）按量计费双机 VPC 私网环境**（`ecs.c9i.xlarge` × 2）中自动拉起，并在同一套硬件与内网拓扑下对以下三组存储栈进行同机实测对比：

1. **Bowl（PostgreSQL 16 控制面 + Pebble）**：PostgreSQL 仅保存 `disks`、`commits`、`client_sessions`；每个 `(disk, branch)` 使用常驻 Pebble 句柄池保存 Inode、Entry、Xattr、Orphan 与 12 字节 `BlobRef` 索引，文件数据落在 `blobs/<shard>/<blob_id>.blob` 的 1 MiB 不可变块文件中；整块直写、局部写 CoW，截断/打洞/删除在提交后 `os.Remove` 即时回收旧块，Checkpoint 与 Fork 以硬链接共享块。服务端分支锁降为拓扑生命周期 `RLock`，完整 1 MiB 块在 Inode 锁外并行落盘，单键点查免 Snapshot，读路径 atime 先登记内存再按 1 秒周期合并；常规写批次以 `pebble.NoSync` 提交异步 WAL，后台 1 秒刷盘与 `WALBytesPerSync=4 MiB` 兜底（`MemTableSize=16 MiB`）。客户端在内存中维护 FileHandle、FileLock、O_TMPFILE 和 O(1) 索引的 NamespaceCache。
2. **JuiceFS (PostgreSQL 16 + Server rclone S3)**：PostgreSQL 16 元数据引擎 + Server 节点 `cloud_essd` 上的 rclone S3 对象存储（`--trash-days 0`）。
3. **JuiceFS (Redis 7 + Server rclone S3)**：JuiceFS 官方常规测试默认的 Redis 元数据引擎（`appendonly yes`, `appendfsync everysec`）+ Server 节点 `cloud_essd` 上的 rclone S3 对象存储（`--trash-days 0`）。

Bowl 客户端采用 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。不同存储栈的缓存与写入确认条件不同，性能数值需结合各自配置解读。

## 目录结构

| 文件 / 目录 | 说明 |
| :--- | :--- |
| [`REPORT.md`](./REPORT.md) | 阿里云硅谷（`us-west-1a`）双机内网 Bowl vs JuiceFS 完整性能基准测试报告（含环境拓扑、内网与磁盘基线、`juicefs bench`、`fio` 8 组负载、`mdtest` 单流与多并发对比、性能观测、比较边界及云资源销毁审计） |
| [`run_benchmark.sh`](./run_benchmark.sh) | 端到端自动化脚本：本地交叉编译 -> 阿里云硅谷双机按量开通 -> 部署 Bowl / PostgreSQL / Redis / rclone S3 / JuiceFS -> 串行执行三大基准测试套件 -> 回传日志生成报告 -> 自动强制释放云服务器 |
| [`parse_results.py`](./parse_results.py) | `juicefs bench`、`fio` JSON 与 `mdtest` 输出解析器及 `REPORT.md` 渲染工具 |
| [`results/summary.json`](./results/summary.json) | 结构化测试汇总数据（三大存储栈在全部测试套件中的吞吐量、IOPS、延迟及相对倍数） |
| [`results/env_metadata.json`](./results/env_metadata.json) | 双机硬件规格、软件版本、VPC 内网 RTT / `iperf3` 带宽基线、云盘本地基线及云资源销毁审计元数据 |
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
bash benchmark/juicefs/run_benchmark.sh
```

支持通过环境变量覆盖默认云资源参数：
- `REGION_ID`（默认 `us-west-1`）
- `ZONE_ID`（默认 `us-west-1a`）
- `INSTANCE_TYPE`（默认 `ecs.c9i.xlarge`）
- `SYSTEM_DISK_SIZE`（默认 `120` GiB `cloud_essd`）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/juicefs/parse_results.py \
  --results-dir benchmark/juicefs/results \
  --summary-out benchmark/juicefs/results/summary.json \
  --report-out benchmark/juicefs/REPORT.md
```
