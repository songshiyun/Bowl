# Bowl vs JuiceFS 阿里云硅谷双机内网存储性能基准测试套件

> 最新复跑：2026-11-09，Bowl `c6e884e`（`ecs.i5.xlarge` 本地 NVMe SSD `local_ssd_pro` 894 GiB）。三组存储栈（`bowl`、`jfs_pg`、`jfs_redis`）同机按目标隔离完成 `juicefs bench`、8 项 `fio`（24 份，错误码均为 0）和单进程/四进程 `mdtest`。本轮 Bowl `juicefs bench` 大文件写 `1,076.63 MiB/s`（为 JuiceFS PG/Redis 的 `1.56x`/`1.58x`）、大文件读 `1,671.20 MiB/s`、小文件写 `2,028.10 files/s`（`2.74x`/`1.80x`）、小文件读 `4,089.90 files/s`、Stat `103,368.30 files/s`；`fio` 16 并发顺序写 `1,059.29 MiB/s`、4 并发 Direct 随机写 `1,189.66 MiB/s`（`3.45x`/`3.51x`）；单进程 `mdtest` 目录创建 `4,274.69 ops/s`、文件创建 `2,531.41 ops/s`、文件删除 `2,634.70 ops/s`；四进程 `mdtest` 目录创建 `8,947.4 ops/s`、文件创建 `9,422.8 ops/s`、文件 Stat `1,387,122.5 ops/s`、文件删除 `12,127.1 ops/s`。完整结果见 [REPORT.md](REPORT.md)。

本目录包含对标 [JuiceFS 官方常规测试](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 [`juicefs bench`](https://juicefs.com/docs/zh/community/performance_evaluation_guide/#juicefs-bench)、[`fio` 吞吐量测试](https://juicefs.com/docs/zh/community/fio/)、[`mdtest` 元数据 IOPS 测试](https://juicefs.com/docs/zh/community/mdtest/)）的端到端自动化评测脚本、实测原始数据及完整对比报告。

默认测试在与 [`benchmark/pjdfstest`](../pjdfstest) 相同地域的**阿里云硅谷（`us-west-1a`）按量计费本地 NVMe SSD 双机 VPC 私网环境**（`ecs.i5.xlarge` × 2，配备 894 GiB `local_ssd_pro` 本地 NVMe SSD）中自动拉起，并在同一套硬件与内网拓扑下按目标隔离服务端进程对以下三组存储栈进行同机实测对比：

1. **Bowl（PostgreSQL 16 控制面 + StoreActor 单盘 Pebble + 64 MiB Pack）**：采用 ADR-0030 / ADR-0031 / ADR-0032 架构，PostgreSQL 仅保存 `disks` 与 `commits` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`；文件数据落在本地 NVMe SSD 的 `packs/pack-<id>.pack` `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与 `64 MiB` 分片热块缓存，支持活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。
2. **JuiceFS (PostgreSQL 16 + Server rclone S3)**：PostgreSQL 16 元数据引擎 + Server 节点本地 NVMe SSD（`/mnt/nvme`）上的 rclone S3 对象存储（`--trash-days 0`），客户端缓存目录 `/var/jfsCache` 挂载于 Client 本地 NVMe SSD。
3. **JuiceFS (Redis 7 + Server rclone S3)**：JuiceFS 官方常规测试默认的 Redis 元数据引擎（`appendonly yes`, `appendfsync everysec`，AOF 落本地 NVMe SSD）+ Server 节点本地 NVMe SSD 上的 rclone S3 对象存储（`--trash-days 0`）。

Bowl 客户端采用无会话 `RawFS` 1 MiB Write-Through 直通写：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 不再触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。每个子测试前均执行 Client 与 Server 双端 `sync && echo 3 > /proc/sys/vm/drop_caches`。不同存储栈的缓存与写入确认条件不同，性能数值需结合各自配置解读。

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
- `INSTANCE_TYPE`（默认 `ecs.i5.xlarge`，配备 894 GiB 本地 NVMe SSD `local_ssd_pro`）
- `SYSTEM_DISK_SIZE`（默认 `40` GiB `cloud_essd` 系统盘）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/juicefs/parse_results.py \
  --results-dir benchmark/juicefs/results \
  --summary-out benchmark/juicefs/results/summary.json \
  --report-out benchmark/juicefs/REPORT.md
```
