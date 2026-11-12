# Bowl 磁盘分叉（`ForkDisk`）性能、在线并发一致性与写时复制基准测试套件

> 最新实测：2026-11-12，Bowl `8a07a55`（阿里云硅谷 `us-west-1a` 双机 `ecs.i5.xlarge` 4 vCPU / 32 GiB / 894 GiB 本地 NVMe SSD `local_ssd_pro`）。三大测试套件全部完成且 SHA-256 数据完整性 100% 通过，客户端 Warn/Error 与服务端 Error 日志均为 `0` 条，测试完成后云端残留实例与密钥对均为 `0`。完整报告见 [REPORT.md](REPORT.md) 与 [results/telemetry/overview.md](results/telemetry/overview.md)。

本轮在双端 `drop_caches` 冷首读核验与 [`deploy/prod`](../../deploy/prod) 全栈生产容器环境下实测核心结果：

- **套件一（ComputeSDK 等价矩阵 + POSIX 扩展，`bowl_fork` $O(1)$ 硬链接分叉 vs `bowl_copy` 同机多线程全量拷贝）**：
  - `small`（`10 × 1 MiB = 10 MiB`）：`ForkDisk` ConnectRPC 中位数 **`5.73 ms`**（P95 `8.82 ms` / P99 `8.87 ms`，CLI `bowl disk fork` 中位数 `10.17 ms`）、FUSE 挂载就绪 **`11.72 ms`**、双端清缓存冷首读 **`9.85 ms`**、端到端可用耗时（TTI）**`27.72 ms`**；相对同机 `bowl_copy` 纯分叉加速 **`7.54x`**、端到端 TTI 加速 **`2.44x`**，ComputeSDK 综合得分 **`99.99`**（含挂载 **`99.97`**）。
  - `wide`（`100 × 1 MiB = 100 MiB`）：`ForkDisk` RPC **`5.72 ms`**，TTI **`27.32 ms`** vs `bowl_copy` `249.18 ms`（纯分叉加速 **`39.22x`**，TTI 加速 **`9.12x`**，综合得分 **`99.99`**）。
  - `deep`（`10 × 16 MiB = 160 MiB`）：`ForkDisk` RPC **`5.75 ms`**，TTI **`74.91 ms`** vs `bowl_copy` `340.23 ms`（纯分叉加速 **`45.72x`**，TTI 加速 **`4.54x`**，综合得分 **`99.98`**）。
  - `large_1gib`（`16 × 64 MiB = 1,024 MiB`）：数据集容量扩大 `102.4` 倍，`ForkDisk` RPC 仍保持 **`6.27 ms`**（P95 `6.97 ms`），TTI **`135.00 ms`** vs `bowl_copy` `1,257.46 ms`（纯分叉加速 **`179.57x`**，分叉+挂载相对建盘+拷贝加速 **`58.61x`**，TTI 加速 **`9.31x`**）；单次分叉服务端仅新增 **`107.8 KiB`** Pebble 清单与提交目录元数据（相对全量拷贝物理写入减少 **`9,728.2x`**）。
  - `tree_5k`（`50` 个子目录共 `5,000 × 4 KiB` 小文件）：`ForkDisk` RPC **`6.39 ms`**，TTI **`21.64 ms`** vs `bowl_copy` `3,247.78 ms`（纯分叉加速 **`505.16x`**，TTI 加速 **`150.05x`**）。
- **套件二（持续并发写入下在线无停顿分叉与孤儿文件离线清洗）**：父盘持续运行 4 并发写线程（聚合写吞吐 **`552.46 MiB/s`**，写错误 **`0` 次**）并持有 `16 × 1 MiB = 16 MiB` 已 `unlink` 未关闭孤儿句柄，不暂停写入连续执行 `10` 次在线 `ForkDisk`（RPC 中位数 **`23.98 ms`**）；父盘分叉前后 `4 MiB` 顺序写+`fsync` 中位数保持 **`13.80 ms` vs `13.88 ms`**，全部 `10/10` 个子盘基线文件与并发自校验小文件 SHA-256 100% 完整（损坏/撕裂 **`0` 个**），且 `16 MiB` 未关闭孤儿占用在全部子盘中 100% 归零清洗（父盘关闭句柄后精确回收 `16.00 MiB`）。
- **套件三（`1 -> 16` 多分支扇出、写时复制隔离与血统提交自动回收）**：`1 GiB` 父盘连续扇出 `16` 个子盘总耗时 **`100.39 ms`**（单次中位数 **`4.73 ms`**）；16 个子盘并发挂载共享读并各写 `4 MiB` 私有增量（逻辑总容量 `17.062 GiB`）后，服务端 `/var/lib/bowl/data` 物理总占用仅 **`1.113 GiB`**（相对唯一数据空间放大 **`1.048x`**，物理空间节省 **`93.47%`**，共享 `.pack` `st_nlink = 33`，私有增量 `.pack` `st_nlink = 1`）；销毁父盘后 `16/16` 个子盘冷读 SHA-256 全部通过；再销毁 `15` 个兄弟子盘仅留唯一幸存盘后，`/var/lib/bowl/data/commits` 剩余提交目录归零（**`0` 个**），幸存盘全部 `17` 个 `.pack` 硬链接计数收敛回 **`st_nlink == 1`**。

## 1. 评测矩阵与三大测试套件

在相同的 2 台阿里云按量 `ecs.i5.xlarge`（4 vCPU / 32 GiB / 894 GiB 本地 NVMe SSD `local_ssd_pro` + 40 GiB `cloud_essd` 系统盘）节点上，直接复用 [`deploy/prod/docker-compose.yaml`](../../deploy/prod/docker-compose.yaml) 与 [`deploy/prod/storage.env`](../../deploy/prod/storage.env) 拉起完整生产容器与可观测性栈（PostgreSQL 18、Goose 迁移、`storage-service`、Jaeger、Prometheus 2s 双端抓取、Loki、Alloy、Grafana，Docker 数据根目录绑定至本地 NVMe SSD `/mnt/nvme/docker`），依次运行三大测试套件：

1. **套件一：ComputeSDK `Snapshot / Fork` 等价矩阵 + POSIX 扩展（`bowl_fork` vs `bowl_copy`）**
   - 覆盖 5 个预设数据集：`small`（`10 × 1 MiB`）、`wide`（`100 × 1 MiB`）、`deep`（`10 × 16 MiB`）、`large_1gib`（`16 × 64 MiB`）与 `tree_5k`（`50` 个子目录 × `100 × 4 KiB`）。
   - `bowl_fork` 每个预设连续执行 `N=20` 轮（5% 截尾均值统计中位数 / P95 / P99），逐轮测量 ConnectRPC `ForkDisk` 延迟、`bowl disk fork` CLI 延迟、`bowl mount` 就绪延迟、双端 `drop_caches` 后冷首读 SHA-256 校验延迟、端到端 TTI 与卸载删除延迟，并按 ComputeSDK 公式（`LATENCY_CEILING_MS = 60000`，单步在线分叉权重 `0.90` + 首读权重 `0.10`）计算综合评分。
   - `bowl_copy` 在相同双机与 FUSE 参数下执行 `CreateDisk + bowl mount + 8 线程并发拷贝 + 文件与目录 fsync + 双端 drop_caches 冷首读 SHA-256 校验`，作为同机 $O(N)$ 全量拷贝对照基线。
2. **套件二：并发持续写入下在线无停顿分叉与未关闭孤儿文件清洗**
   - 在主挂载点固化 `32 × 1 MiB` 基线文件并保持 `16 × 1 MiB` 已 `unlink` 未关闭孤儿句柄，启动 2 个 `4 MiB` 顺序写线程 + 2 个 `4 KiB` 自校验小文件写线程持续写入，不暂停写入连续发起 `10` 次在线 `ForkDisk`，核验父盘写延迟扰动、子盘时间点切面一致性与孤儿空间清洗。
3. **套件三：`1 -> 16` 多分支扇出、写时复制（CoW）隔离与最近公共祖先提交回收**
   - 在 `1 GiB`（`16 × 64 MiB`）父盘上连续分叉 `16` 个子盘并全部并发挂载，并发执行共享读校验与每子盘 `4 MiB` 独立写入，随后依次删除父盘与 `15` 个兄弟子盘，通过服务端远程存储审计脚本精确核验 `du -sb` 物理字节、`packs/*.pack` 的 `st_nlink` 引用计数及 `commits/` 目录自动回收。

## 2. 目录结构

- [`run_benchmark.sh`](run_benchmark.sh)：端到端自动化脚本（交叉编译 Linux `amd64` 二进制 -> 创建阿里云按量双机与临时密钥对 -> 基于 [`deploy/prod`](../../deploy/prod) 部署 Bowl 生产与可观测性栈 -> 测量 VPC 网络与本地 NVMe 硬件基线 -> 执行三大分叉基准套件 -> 收割全栈遥测并拉取结果 -> `trap cleanup EXIT` 强制销毁云主机与密钥对并核验零残留 -> 生成报告）。
- [`fork_bench.py`](fork_bench.py)：双机编排与负载驱动器（含直连 ConnectRPC 控制面计时、多挂载点生命周期管理、双端 `drop_caches` 冷读 SHA-256 校验、并发自校验写压测器与服务端物理存储/硬链接审计）。
- [`parse_results.py`](parse_results.py) / [`test_parse_results.py`](test_parse_results.py)：汇总 JSON 原始结果与 `results/telemetry/` 遥测摘要，生成 [`results/summary.json`](results/summary.json) 与 [`REPORT.md`](REPORT.md)，并附带离线单元测试。
- [`../common/prod_stack.sh`](../common/prod_stack.sh) / [`../common/collect_telemetry.py`](../common/collect_telemetry.py)：公共 `deploy/prod` 容器栈部署编排、子阶段 `[pre, post]` 遥测快照差分与慢路径诊断工具。
- `results/`：本轮阿里云双机实测产物（脱敏 [`results/env_metadata.json`](results/env_metadata.json)、[`results/stage_timings.jsonl`](results/stage_timings.jsonl)、[`results/suite1_matrix.json`](results/suite1_matrix.json)、[`results/suite2_online_fork.json`](results/suite2_online_fork.json)、[`results/suite3_fanout_cow.json`](results/suite3_fanout_cow.json)、[`results/summary.json`](results/summary.json) 与 [`results/telemetry/`](results/telemetry/overview.md)；原始归档 `telemetry-raw.tar.gz` 已由 `.gitignore` 忽略）。
- [`REPORT.md`](REPORT.md)：阿里云硅谷双机实测完整对比与一致性核验报告。

## 3. 运行方式

```bash
# 需显式传入阿里云同可用区 VPC_ID、VSWITCH_ID、SECURITY_GROUP_ID
# 脚本内置 trap cleanup EXIT，无论正常结束或中途报错均强制销毁双机与临时密钥对并核验零残留
VPC_ID=vpc-xxx VSWITCH_ID=vsw-xxx SECURITY_GROUP_ID=sg-xxx bash benchmark/fork/run_benchmark.sh
```

### 基于已有结果重新生成报告与运行单测

```bash
python3 benchmark/fork/parse_results.py \
  --results-dir benchmark/fork/results \
  --summary-out benchmark/fork/results/summary.json \
  --report-out benchmark/fork/REPORT.md

PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v \
  benchmark/common/test_collect_telemetry.py \
  benchmark/fork/test_parse_results.py
```
