# Bowl `pjdfstest` 阿里云硅谷双机内网基准测试套件

> 最新复跑：2026-11-11，Bowl `9c60a34-dirty`（`ecs.i5.xlarge` 本地 NVMe SSD `local_ssd_pro` 894 GiB）。完整结果见 [REPORT.md](REPORT.md)。

本轮在 17 个系统调用目录、238 个 `.t` 脚本、8,798 项 POSIX 断言中通过 `8,789 / 8,798`（通过率 `99.90%`，28 项 TODO 中 9 项预期失败）；全量执行耗时 `191.94s`，内网综合吞吐 `45.84 tests/s`，与 `c6e884e` 兼容性结果完全一致（0 新增失败）。未通过的 9 项子测试（分布于 `chmod/02.t`、`chown/02.t`、`ftruncate/02.t`、`link/02.t`、`rename/01.t`、`rmdir/02.t`、`truncate/02.t`、`unlink/02.t`）均为存量 256 字节超长文件名边界断言（预期 `ENAMETOOLONG`，因 `completeDirs` 权威完整目录本地未命中短路提前返回 `ENOENT`）。

本目录包含基于阿里云 CLI（`aliyun`）在硅谷地域（`us-west-1`）自动拉起 2 台按量计费本地 NVMe SSD 云服务器（`ecs.i5.xlarge` × 2，1 台 `storage-service` 服务端节点 + 1 台 FUSE 客户端节点），在同可用区 VPC 私网下运行完整 [`pjdfstest`](https://github.com/pjd/pjdfstest) POSIX 兼容性与性能基准测试集的自动化脚本、实测原始数据及完整测试报告。

被测版本采用 ADR-0030 / ADR-0031 / ADR-0032 单盘独立 Actor 存储引擎与 `64 MiB` Append-Only Pack 数据层：PostgreSQL 16 仅保存单张 `disks` 控制面表，客户端 `MountDisk` 握手原子递增 `WriterEpoch` 世代号；每块磁盘对应独立 `StoreActor`（单盘 Pebble + 共享 `128 MiB` Block Cache），采用双缓冲 MPSC Group Commit 循环（Phase A 内存校验、`WriterEpoch` 栅栏与零拷贝 `SetDeferred` 编码，Phase B 批量提交 Pebble WAL），DataKey 保存 16 字节 `BlobRef`（`PackID + Offset + Length`）；文件数据落在 `packs/<pack_id_hex16>.pack` 的 `64 MiB` Append-Only 封包文件中，采用 `256 KiB` 逻辑块网格、常驻句柄池与尾块分片热缓存（`AppendBlocks` 整块顺序写跳过用户态冗余拷贝），配合服务端 `WriteRequest` 池化零分配解码、活跃包末尾同块 `ExtendTail` 顺延扩展、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障（脏 `.pack` `fdatasync` + Pebble `LogData` 并发同步）及已封口包自动回收。

客户端采用无会话 `RawFS`：禁用内核 `CAP_WRITEBACK_CACHE`，保留 `FOPEN_KEEP_CACHE`，声明 `CAP_HANDLE_KILLPRIV` 在写批次内清除 `security.capability`；配备对称的 `WriteBehindCoordinator`（连续顺序写经 `2 MiB` 同步探测后激活 `4 MiB` 聚批与 `W*=4` 并发流水线，`<64 KiB` 小写直通）、`ReadAheadCoordinator` 顺序预读与双端 `8 MiB / 32 MiB` HTTP/2 流控窗口，写后回填 `AttrTimeout=1.0s`，并配置 `MaxBackground=64` / `CongestionThreshold=48`。Flush 排空异步写缓冲但不触发跨机 Fsync RPC，仅显式 Fsync/Sync 请求服务端持久化屏障。本套件为覆盖真实元数据路径，保持 `strictatime` 与 0 秒属性/目录项超时，双端数据与构建工作目录全量挂载于 894 GiB 本地 NVMe SSD（`/mnt/nvme`）。

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
- `INSTANCE_TYPE`（默认 `ecs.i5.xlarge`，配备 894 GiB 本地 NVMe SSD `local_ssd_pro`）
- `KEEP_INSTANCES`（默认 `0` 测试完成后立即销毁实例；设为 `1` 可保留实例用于现场排查）

### 3. 基于已有日志重新生成报告
```bash
python3 benchmark/pjdfstest/parse_results.py \
  --results-dir benchmark/pjdfstest/results \
  --summary-out benchmark/pjdfstest/results/summary.json \
  --report-out benchmark/pjdfstest/REPORT.md
```
