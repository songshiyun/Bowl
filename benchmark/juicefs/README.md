# Bowl vs JuiceFS 阿里云硅谷双机内网存储性能基准测试套件

> 最新复跑：2026-11-02，Bowl `7fbc2fe`。Bowl 在两次独立运行的 4 GiB 写入最终 `fsync` 均返回 EIO；其余未执行项不沿用旧值。JuiceFS PG/Redis 对照组完整完成。详见 [REPORT.md](REPORT.md) 的执行说明与失败证据。

本目录包含对标 [JuiceFS 官方常规测试](https://juicefs.com/docs/zh/community/benchmark/)（涵盖 [`juicefs bench`](https://juicefs.com/docs/zh/community/performance_evaluation_guide/#juicefs-bench)、[`fio` 吞吐量测试](https://juicefs.com/docs/zh/community/fio/)、[`mdtest` 元数据 IOPS 测试](https://juicefs.com/docs/zh/community/mdtest/)）的端到端自动化评测脚本、实测原始数据及完整对比报告。

默认测试在与 [`benchmark/pjdfstest`](../pjdfstest) 完全相同的**阿里云硅谷地域（`us-west-1a`）按量计费双机 VPC 私网环境**（`ecs.c9i.xlarge` × 2）中自动拉起，并在同一套硬件与内网拓扑下对以下三组存储栈进行同机实测对比，同时保留 JuiceFS 官方文档历史参考数据：

1. **Bowl (PostgreSQL 16 + Server BlockStore)**：Bowl 生产默认配置（`noatime` + `1.0s` FUSE 属性/目录项缓存，ConnectRPC 跨机直通）。
2. **JuiceFS (PostgreSQL 16 + Server rclone S3)**：与 Bowl 同款的 PostgreSQL 16 元数据引擎 + Server 节点 `cloud_essd` 上的 rclone S3 对象存储（`--trash-days 0`）。
3. **JuiceFS (Redis 7 + Server rclone S3)**：JuiceFS 官方常规测试默认的 Redis 元数据引擎（`appendonly yes`, `appendfsync everysec`）+ Server 节点 `cloud_essd` 上的 rclone S3 对象存储（`--trash-days 0`）。

## 目录结构

| 文件 / 目录 | 说明 |
| :--- | :--- |
| [`REPORT.md`](./REPORT.md) | 阿里云硅谷（`us-west-1a`）双机内网 Bowl vs JuiceFS 完整性能基准测试报告（含环境拓扑、内网与磁盘基线、`juicefs bench`、`fio` 8 组负载、`mdtest` 单流与多并发对比、性能观测、比较边界及云资源销毁审计） |
| [`run_aliyun_benchmark.sh`](./run_aliyun_benchmark.sh) | 端到端自动化脚本：本地交叉编译 -> 阿里云硅谷双机按量开通 -> 部署 Bowl / PostgreSQL / Redis / rclone S3 / JuiceFS -> 串行执行三大基准测试套件 -> 回传日志生成报告 -> 自动强制释放云服务器 |
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
bash benchmark/juicefs/run_aliyun_benchmark.sh
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
