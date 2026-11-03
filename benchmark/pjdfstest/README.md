# Bowl `pjdfstest` 阿里云硅谷双机内网基准测试套件

> 最新复跑：2026-11-02，Bowl `0655a17`。完整结果见 [REPORT.md](REPORT.md)。

本目录包含基于阿里云 CLI（`aliyun`）在硅谷地域（`us-west-1`）自动拉起 2 台按量计费云服务器（1 台 `storage-service` 服务端节点 + 1 台 FUSE 客户端节点），在同可用区 VPC 私网下运行完整 [`pjdfstest`](https://github.com/pjd/pjdfstest) POSIX 兼容性与性能基准测试集的自动化脚本、实测原始数据及完整测试报告。

## 目录结构

| 文件 / 目录 | 说明 |
| :--- | :--- |
| [`REPORT.md`](./REPORT.md) | 阿里云硅谷（`us-west-1a`）双机内网 `pjdfstest` 完整测试报告（含环境拓扑、内网基线、17 个系统调用目录汇总、238 个 `.t` 用例明细及云资源清理审计） |
| [`run_aliyun_pjdfstest.sh`](./run_aliyun_pjdfstest.sh) | 端到端自动化脚本：本地交叉编译 -> 阿里云双机按量开通 -> 内网部署挂载 -> 执行 `pjdfstest` -> 回传日志并生成报告 -> 自动强制释放云服务器 |
| [`parse_results.py`](./parse_results.py) | `prove --timer -rv` TAP 输出解析器与 `REPORT.md` 渲染工具 |
| [`results/summary.json`](./results/summary.json) | 结构化测试汇总数据（总体通过率、耗时、17 个目录与 238 个脚本明细） |
| [`results/env_metadata.json`](./results/env_metadata.json) | 双机硬件规格、内核版本、VPC 内网 ICMP RTT 与 `iperf3` 带宽基线、云资源销毁审计元数据 |
| [`results/dir_timings.jsonl`](./results/dir_timings.jsonl) | 17 个系统调用目录的分项执行耗时与退出码记录 |
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
bash benchmark/pjdfstest/run_aliyun_pjdfstest.sh
```

支持通过环境变量覆盖默认云资源参数：
- `REGION_ID`（默认 `us-west-1`）
- `ZONE_ID`（默认 `us-west-1a`）
- `INSTANCE_TYPE`（默认 `ecs.c9i.xlarge`）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/pjdfstest/parse_results.py \
  --log benchmark/pjdfstest/results/pjdfstest.log \
  --timings benchmark/pjdfstest/results/dir_timings.jsonl \
  --env-meta benchmark/pjdfstest/results/env_metadata.json \
  --summary-out benchmark/pjdfstest/results/summary.json \
  --report-out benchmark/pjdfstest/REPORT.md
```
