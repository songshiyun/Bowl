# Bowl Disk Fork — 阿里云硅谷双机内网分叉性能与一致性基准测试报告

## 1. 执行摘要（Executive Summary）

本报告记录了 **Bowl 虚拟磁盘分叉（`ForkDisk`）** 在阿里云硅谷地域（`us-west-1a`）真实开启 2 台按量计费云服务器（1 台服务端存储与控制面节点 + 1 台 Linux FUSE 客户端节点），在同可用区 VPC 私网与本地 NVMe SSD（`local_ssd_pro`）环境下执行三组核心基准套件的实测结果：

1. **存储与分叉架构**：PostgreSQL 18（deploy/prod 容器栈）维护磁盘血统树（`disk_lineages` / `disk_commits` / `disks`）；单步在线 `ForkDisk` 在源盘 `StoreActor` 串行切面点封口活跃 `.pack` 封包并通过 `os.Link` 硬链接不可变 Pebble `.sst` 与 `packs/*.pack` 生成不可变提交切面（若源盘存在未关闭孤儿句柄则对切面执行离线净化并解链孤儿独占封包），随后零拷贝硬链接派生独立可写子盘。分叉时间复杂度与新增物理容量均为元数据级 $O(1)$，与源盘数据容量无关。
2. **三组实测套件覆盖**：套件一对比 5 组预设规模（`10 MiB` 至 `1 GiB` 大文件与 `5,000` 小文件目录树）下 `bowl_fork`（`N=20`，5% 双侧截尾统计）与同机整盘全量拷贝基线 `bowl_copy`；套件二验证持续并发写入（`4 MiB` 顺序流 + `4 KiB` 小文件 + `16 × 1 MiB` 未关闭孤儿句柄）下连续 10 次无停顿在线 `ForkDisk` 的父盘写延迟、子盘 SHA-256 完整性与孤儿空间清洗；套件三验证 `1 GiB` 父盘 `1 -> 16` 多分支并发挂载读写写时复制（CoW）隔离、物理空间放大比，以及父盘与 15 个兄弟盘销毁后唯一幸存盘的血统提交目录零残留自动回收。

### 1.1 核心指标同机实测总览表

| 测试套件 | 场景 / 指标项 | 实测结果 (`bowl_fork`) | 同机全量拷贝基线 / 预期不变量 | 加速比 / 结论 |
| :--- | :--- | ---: | ---: | :--- |
| **Suite 1 (`small` · 10 MiB)** | `ForkDisk` RPC 中位数 (`P95`) / 端到端可用 TTI | `5.73 (8.82) ms` / `27.72 ms` | 拷贝同步 `43.25 ms` / TTI `67.67 ms` | **RPC 快 `7.54x` · TTI 快 `2.44x`** |
| **Suite 1 (`large_1gib` · 1 GiB)** | `ForkDisk` RPC 中位数 (`P95`) / 端到端可用 TTI | `6.27 (6.97) ms` / `135.00 ms` | 拷贝同步 `1125.37 ms` / TTI `1257.46 ms` | **RPC 快 `179.57x` · TTI 快 `9.31x`** |
| **Suite 1 (`large_1gib` · 物理增量)** | 单次分叉服务端新增物理落盘 (`du -sb`) | `107.8 KiB` | 整盘拷贝新增 `1.000 GiB` | **节省 `1.000 GiB` (`9728.2x`)** |
| **Suite 1 (`tree_5k` · 5,000 小文件)** | `ForkDisk` RPC 中位数 (`P95`) / 端到端可用 TTI | `6.39 (7.73) ms` / `21.64 ms` | 拷贝同步 `3227.49 ms` / TTI `3247.78 ms` | **RPC 快 `505.16x` · TTI 快 `150.05x`** |
| **Suite 2 (并发写在线分叉 ×10)** | 父盘持续写吞吐 / 在线 `ForkDisk` 中位数 (`P95`) | `552.46 MiB/s` / `23.98 (38.22) ms` | 写错误 `0` 次 · 16 MiB 孤儿句柄保持打开 | **`10/10` 子盘 SHA-256 全绿且孤儿 100% 清洗** |
| **Suite 3 (`1 -> 16` 扇出 + CoW)** | 17 盘总逻辑容量 vs 服务端 `du -sb` 物理占用 | `1.113 GiB` (物理) | `17.062 GiB` (逻辑总容量) | **相对唯一数据空间放大 `1.048x` · 节省 `93.5%`** |
| **Suite 3 (父盘+15 子盘销毁 GC)** | 唯一幸存子盘保留时 `commits/` 残留与 `.pack` `st_nlink` | `commits=0` · `st_nlink=1` | 预期 `commits == 0` 且全部 `.pack` `st_nlink == 1` | **血统提交 100% 自动回收且幸存盘全量 SHA-256 通过** |

### 1.2 ComputeSDK `2026-11-06` 公开参考数据与架构边界对照（`small` · `10 × 1 MiB`）

下表列出 ComputeSDK 于 `2026-11-06` 发布的 `small`（`10 × 1 MiB`）快照/分叉公开参考值，并与本轮 Bowl 实测的纯控制面分叉（`ForkDisk` RPC）及含内核 FUSE 挂载就绪（`ForkDisk + bowl mount`）两组口径并列对照。**架构边界说明**：Tensorlake / Tigris / Azure Blob 为云对象存储或托管沙箱 API 口径（通过 HTTP SDK 直接读写对象，无客户端 Linux 内核 FUSE 文件系统挂载生命周期）；Bowl 为同可用区 VPC 私网下提供完整 POSIX 文件系统语义（支持 `mmap`、随机覆写、原子 `rename`、目录 `fsync`、未关闭孤儿句柄隔离与 SQLite WAL）的内核 FUSE 挂载块/文件融合卷。

| 方案 / 口径 | 分叉耗时 `forkFromLive` (ms) | 首次校验读 `firstRead` (ms) | ComputeSDK 综合得分 | 架构层级与边界说明 |
| :--- | ---: | ---: | ---: | :--- |
| **Bowl `bowl_fork` (ConnectRPC 口径)** | `5.73` | `9.85` | **`99.99`** | 同 VPC 双机 POSIX 存储控制面单步在线分叉 + 双端 `drop_caches` 后 FUSE 冷首读 SHA-256 校验 |
| **Bowl `bowl_fork` (含 FUSE 挂载就绪口径)** | `18.18` | `9.85` | **`99.97`** | 包含 `ForkDisk` RPC + 客户端 `bowl mount` 守护进程拉起与 `MountDisk` 握手 + 双端清缓存冷首读 |
| **Bowl `bowl_copy` (同机全量拷贝基线)** | `60.20` | `8.40` | `99.91` | 同 VPC 双机 `CreateDisk` + `bowl mount` + 多线程全量文件拷贝与 `fsync` 基线 |
| Tensorlake (ComputeSDK `2026-11-06` 参考) | `267.37` | `89.20` | `99.76` | 云沙箱与对象存储快照接口（API 级读取，不含内核 POSIX FUSE 挂载握手） |
| Tigris (ComputeSDK `2026-11-06` 参考) | `340.75` | `231.63` | `99.67` | 对象存储分支接口（HTTP API 读取，不含内核 POSIX FUSE 挂载握手） |
| Azure Blob (ComputeSDK `2026-11-06` 参考) | `3550.97` | `31.73` | `96.84` | 云对象存储复制快照接口（HTTP API 读取，不含内核 POSIX FUSE 挂载握手） |

---

## 2. 测试环境与双机内网基线（Environment & Hardware Baseline）

| 维度 | Server 节点（存储服务与控制面） | Client 节点（FUSE 挂载与压测端） |
| :--- | :--- | :--- |
| **云厂商与可用区** | Aliyun ECS `us-west-1a` | Aliyun ECS `us-west-1a` |
| **实例规格** | `ecs.i5.xlarge` (4 vCPU, 30Gi) | `ecs.i5.xlarge` (4 vCPU, 30Gi) |
| **实例 ID（脱敏）** | `i-<redacted-server>` (`172.16.1.205`) | `i-<redacted-client>` (`172.16.1.204`) |
| **CPU 型号** | Intel(R) Xeon(R) 6982P-C | Intel(R) Xeon(R) 6982P-C |
| **操作系统与内核** | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) | Ubuntu 24.04.5 LTS (`6.8.0-147-generic`) |
| **本地数据盘规格** | `local_ssd_pro (NVMe)` `894 GiB` (`/dev/nvme1n1`) | `local_ssd_pro (NVMe)` `894 GiB` (`/dev/nvme1n1`) |
| **部署编排与数据库** | `deploy/prod (docker-compose.yaml + storage.env)` · `postgres (PostgreSQL) 18.6` | `fusermount3 version: 3.14.0` · `Python 3.12.3` |
| **可观测性栈** | `Jaeger (OTLP HTTP/gRPC :4317, sample_ratio=0.10) + Prometheus (2s dual-node scrape) + Loki + Alloy + Grafana + bowl stats + pprof` | `bowl stats (:9567)` + OTLP Trace + Prometheus Metrics |
| **VPC 私网基线** | `ping` RTT: `rtt min/avg/max/mdev = 0.042/0.084/0.236/0.049 ms` | `iperf3` 单流带宽: `21.22 Gbps` |
| **Server 本地 NVMe 基线** | 4M 顺序写 `1998.05 MiB/s` · 4M 顺序读 `3494.88 MiB/s` | 4K 随机写 `69933 IOPS` · 4K 随机读 `699772 IOPS` · 4K fsync `51492 IOPS` (P99 `62.3 µs`) |
| **代码提交版本** | Bowl `8a07a55` | 同左 |

---

## 3. 套件一：ComputeSDK 快照/分叉等价矩阵与整盘全量拷贝基线对比（`bowl_fork` vs `bowl_copy`）

对 5 组预设数据集先通过 FUSE 写入源盘并完成全量文件与目录 `fsync`，随后分别测量：(1) **`bowl_fork`**（每组 `N=20` 轮迭代，按 ComputeSDK 方法论执行 5% 双侧截尾后输出 `Median / P95 / P99`）：每轮依次测量 `ForkDisk` ConnectRPC 延迟、`bowl mount` 挂载就绪延迟、Client+Server 双端 `drop_caches` 后冷首读 SHA-256 校验延迟、全量文件 SHA-256 校验耗时以及卸载删除耗时，并额外测量 5 轮 `bowl disk fork --json` CLI 进程耗时；(2) **`bowl_copy`**：同机新建空盘、挂载、多线程从源挂载点全量拷贝并 `fsync`、双端 `drop_caches` 后冷首读与全量 SHA-256 校验。

### 3.1 `bowl_fork` 分阶段延迟分布（单位：`ms`，格式：`Median / P95 / P99`，`N=20` 截尾统计）

| 数据集预设 | 规模 (`文件数 × 大小`) | 源盘写入 `seed_ms` | `ForkDisk` RPC (`ms`) | `bowl disk fork` CLI P50 (`ms`) | `bowl mount` 就绪 (`ms`) | 双端清缓存首读 (`ms`) | 端到端可用 TTI (`ms`) | 卸载+删盘 (`ms`) | 全量 SHA-256 校验 | ComputeSDK 得分 (RPC / 含挂载) |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :---: | ---: |
| `small` | ComputeSDK small (10 × 1 MiB) (`10.0 MiB`) | `43.9` | `5.73 / 8.82 / 8.87` | `10.17` | `11.72 / 13.32 / 13.60` | `9.85 / 12.35 / 12.45` | `27.72 / 33.05 / 33.89` | `13.26 / 17.09 / 18.39` | `20/20` | **`99.99`** / `99.97` |
| `wide` | ComputeSDK wide (100 × 1 MiB) (`100.0 MiB`) | `249.3` | `5.72 / 6.09 / 6.12` | `9.75` | `11.81 / 12.84 / 12.94` | `9.60 / 10.12 / 10.12` | `27.32 / 28.17 / 28.22` | `17.32 / 18.23 / 18.43` | `20/20` | **`99.99`** / `99.97` |
| `deep` | ComputeSDK deep (10 × 16 MiB) (`160.0 MiB`) | `333.7` | `5.75 / 6.34 / 6.37` | `10.26` | `12.70 / 13.77 / 13.97` | `56.01 / 69.14 / 70.23` | `74.91 / 87.15 / 88.84` | `21.22 / 22.80 / 22.88` | `20/20` | **`99.98`** / `99.96` |
| `large_1gib` | 多封包 NVMe 规模 (16 × 64 MiB = 1 GiB) (`1024.0 MiB`) | `2032.9` | `6.27 / 6.97 / 7.00` | `11.07` | `13.15 / 14.07 / 14.76` | `114.81 / 128.40 / 129.81` | `135.00 / 147.74 / 149.63` | `58.98 / 62.80 / 62.87` | `20/20` | **`99.97`** / `99.95` |
| `tree_5k` | POSIX 多目录树规模 (50 子目录 × 100 × 4 KiB) (`19.5 MiB`) | `2659.2` | `6.39 / 7.73 / 8.25` | `10.88` | `12.48 / 13.25 / 13.45` | `2.54 / 2.76 / 2.82` | `21.64 / 25.58 / 32.63` | `17.75 / 18.94 / 19.18` | `20/20` | **`99.99`** / `99.97` |

### 3.2 同机整盘全量拷贝基线（`bowl_copy`）及 $O(1)$ 分叉 vs $O(N)$ 拷贝对比

| 数据集预设 | `bowl_copy` 拷贝+`fsync` P50 (`ms`) | `bowl_copy` 端到端 TTI P50 (`ms`) | `bowl_fork` 端到端 TTI P50 (`ms`) | 纯分叉 RPC vs 拷贝加速比 | 含挂载分叉 vs 建盘拷贝加速比 | 端到端 TTI 加速比 | 分叉新增物理字节 (`du -sb`) | 拷贝新增物理字节 (`du -sb`) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `small` | `43.25` | `67.67` | `27.72` | **`7.54x`** | **`3.31x`** | **`2.44x`** | `10.5 KiB` | `10.01 MiB` |
| `wide` | `224.37` | `249.18` | `27.32` | **`39.22x`** | **`13.63x`** | **`9.12x`** | `52.0 KiB` | `100.07 MiB` |
| `deep` | `262.85` | `340.23` | `74.91` | **`45.72x`** | **`15.11x`** | **`4.54x`** | `23.0 KiB` | `160.05 MiB` |
| `large_1gib` | `1125.37` | `1257.46` | `135.00` | **`179.57x`** | **`58.61x`** | **`9.31x`** | `107.8 KiB` | `1.000 GiB` |
| `tree_5k` | `3227.49` | `3247.78` | `21.64` | **`505.16x`** | **`169.69x`** | **`150.05x`** | `0 B` | `22.68 MiB` |

---

## 4. 套件二：并发持续写入下在线无停顿分叉与未关闭孤儿文件清洗验证

在主挂载点 `/mnt/bowl` 预先固化 `32 × 1 MiB`（`32 MiB`）基线文件，并打开 `16 × 1 MiB`（`16 MiB`）已 `unlink` 但句柄未关闭的孤儿文件；随后启动 2 个 `4 MiB` 顺序流写线程与 2 个 `4 KiB` 带自校验摘要头的小文件创建+`fsync` 线程持续并发写入（全程不暂停写入线程），连续发起 `10` 次在线 `ForkDisk`。

### 4.1 父盘并发写入吞吐、写延迟对比与孤儿回收审计

| 观测项 | 分叉前稳态窗口 (`Before Fork`) | 连续 10 次在线分叉窗口 (`During Fork`) | 全程汇总 / 核验结论 |
| :--- | ---: | ---: | :--- |
| **4 MiB 顺序写+`fsync` 延迟 (`Median / P95 / P99`)** | `13.80 / 20.33 / 26.49 ms` (`n=301`) | `13.88 / 19.68 / 25.03 ms` (`n=260`) | 全程完成 `619` 次 · 写错误 `0` 次 |
| **4 KiB 小文件写+`fsync` 延迟 (`Median / P95 / P99`)** | `3.72 / 7.03 / 10.64 ms` (`n=1045`) | `3.92 / 8.06 / 11.57 ms` (`n=881`) | 全程完成 `2103` 次 (`467.7 ops/s`) |
| **父盘聚合写入吞吐与孤儿句柄回收** | 初始含孤儿占用 `48.00 MiB` | 在线 `ForkDisk` RPC `23.98 / 38.22 / 44.29 ms` | 聚合写吞吐 **`552.46 MiB/s`** · 关句柄后父盘精确回收 **`16.00 MiB`** |

### 4.2 连续 10 次在线分叉子盘逐次核验明细

| 分叉序号 | `ForkDisk` RPC (`ms`) | 子盘 `used_bytes` | 子盘可见常规文件字节和 | 子盘残留孤儿字节 | 已清洗孤儿字节 | 基线文件 SHA-256 | 并发 4K 文件自校验数 | 损坏/撕裂文件数 |
| :--- | ---: | ---: | ---: | ---: | ---: | :---: | ---: | ---: |
| `fork_00` | `28.95` | `1,308,717,056` | `1,308,717,056` | `0` | `16.00 MiB` | `32/32` | `1047` | `0` |
| `fork_01` | `21.61` | `1,430,798,336` | `1,430,798,336` | `0` | `16.00 MiB` | `32/32` | `1156` | `0` |
| `fork_02` | `21.90` | `1,531,789,312` | `1,531,789,312` | `0` | `16.00 MiB` | `32/32` | `1236` | `0` |
| `fork_03` | `27.51` | `1,654,951,936` | `1,654,951,936` | `0` | `16.00 MiB` | `32/32` | `1353` | `0` |
| `fork_04` | `22.68` | `1,780,199,424` | `1,780,199,424` | `0` | `16.00 MiB` | `32/32` | `1467` | `0` |
| `fork_05` | `23.70` | `1,910,648,832` | `1,910,648,832` | `0` | `16.00 MiB` | `32/32` | `1571` | `0` |
| `fork_06` | `45.81` | `2,028,462,080` | `2,028,462,080` | `0` | `16.00 MiB` | `32/32` | `1662` | `0` |
| `fork_07` | `25.44` | `2,142,048,256` | `2,142,048,256` | `0` | `16.00 MiB` | `32/32` | `1745` | `0` |
| `fork_08` | `23.59` | `2,264,055,808` | `2,264,055,808` | `0` | `16.00 MiB` | `32/32` | `1836` | `0` |
| `fork_09` | `24.27` | `2,386,030,592` | `2,386,030,592` | `0` | `16.00 MiB` | `32/32` | `1919` | `0` |

---

## 5. 套件三：`1 -> 16` 多分支扇出、写时复制（CoW）隔离与血统提交自动回收验证

创建 `1 GiB`（`16 × 64 MiB`）父盘并落盘后，连续扇出分叉 `16` 个子盘并在客户端全部并发挂载；16 个并发工作线程同时读取共享父盘文件校验 SHA-256 并各自写入 `4 MiB` 唯一私有增量（合计 `64 MiB`），随后依次销毁父盘与 `15` 个兄弟子盘，核验服务端 `/var/lib/bowl/data` 物理空间（`du -sb`）、`.pack` 硬链接计数（`st_nlink`）与 `commits/` 目录回收状态。

| 生命周期阶段 | 活跃逻辑总容量 | 服务端 `du -sb` 物理占用 | `commits/` 目录数 | 共享 `.pack` `st_nlink` | 一致性与隔离核验结果 |
| :--- | ---: | ---: | ---: | ---: | :--- |
| **1. 父盘写入 `1 GiB` (`16 × 64 MiB`)** | `1.000 GiB` | `1.049 GiB` | `0` | `1` | 源盘 `16` 个 `.pack` 封包落盘 (`seed=2107.4 ms`) |
| **2. 扇出分叉 `16` 个子盘 (`17 GiB` 逻辑)** | `17.000 GiB` | `1.051 GiB` | `16` | `33` | 16 次分叉总耗时 `100.39 ms` (P50 `4.73 ms`) · 逻辑/物理比 `16.18x` |
| **3. 16 子盘并发挂载读 + 各写 `4 MiB` 增量** | `17.062 GiB` | `1.113 GiB` | `16` | `33` (增量包 `1`) | 相对唯一数据空间放大 **`1.048x`** · 父子与兄弟间私有文件 100% 不可见 |
| **4. 销毁父盘后双端清缓存冷读核验** | `16.062 GiB` | `1.113 GiB` | `16` | `32` | 全部 `16/16` 个子盘共享数据与私有增量冷读 SHA-256 100% 通过 |
| **5. 再销毁 15 个子盘保留唯一幸存盘** | `1.004 GiB` | `1.053 GiB` | **`0`** | **`1..1`** | `commits/` 归零回收 · 幸存盘全部 `17` 个 `.pack` `st_nlink == 1` 且全量冷读 SHA-256 通过 |

---

## 6. 本轮观测与比较边界

- **$O(1)$ 容量无关性实证**：数据集从 `small`（`10 MiB`）增长 `102.4` 倍至 `large_1gib`（`1,024 MiB`），`ForkDisk` ConnectRPC 中位数延迟由 `5.73 ms` 变为 `6.27 ms`，而同机 `bowl_copy` 拷贝同步耗时由 `43.25 ms` 线性增长至 `1125.37 ms`。
- **高密度目录与小文件元数据场景（`tree_5k`）**：面对 50 个子目录共 `5,000` 个 `4 KiB` 小文件，逐文件创建的 `bowl_copy` 需要 `3227.49 ms`，而直接硬链接底层 Pebble SST 树与 `.pack` 文件的 `bowl_fork` 仅需 `6.39 ms`（纯分叉加速 `505.16x`）。
- **冷缓存定义边界**：每轮冷首读前在 Client 与 Server 双端执行 `sync && echo 3 > /proc/sys/vm/drop_caches` 清空双端内核页缓存、目录项与 Inode 缓存，但不重启 `storage-service` 容器进程（保留服务端进程内 128 MiB Pebble BlockCache 与 64 MiB 尾块缓存）。对于超过 64 MiB 的 `wide`、`deep`、`large_1gib` 场景，数据读取穿透至本地 NVMe SSD。
- **外部参考值比较边界**：ComputeSDK 公开参考值（Tensorlake / Tigris / Azure Blob）运行于各自云端托管环境且通过 HTTP 对象接口直接读取；本报告中的 Bowl 运行于阿里云同可用区双机 VPC 私网并通过完整 Linux FUSE 挂载提供 POSIX 文件系统接口，两者仅在 `small`（`10 × 1 MiB`）工作负载形状与评分公式上对齐，底层部署拓扑与访问协议不同。
- **遥测日志构成说明**：全轮客户端 Warn/Error 日志为 `0` 条、Pebble 引擎告警为 `0` 条、服务端 `ERROR` 级别日志为 `0` 条；Loki 统计中的服务端 `WARN` 记录来自对未新增写入的源盘连续执行 `Checkpoint` 刷盘时 Pebble 空内存表返回的良性提示 `pebble: empty table`，以及套件二 `552 MiB/s` 高并发持续写入下的少量 `20~24 ms` `slow actor tick` 记录。

---

## 7. 生产可观测性与慢路径遥测诊断（`deploy/prod` Telemetry）

- **生产可观测性编排栈**：`deploy/prod (docker-compose.yaml + storage.env)`（完整诊断明细见 [`results/telemetry/overview.md`](results/telemetry/overview.md)，原始采样包见 `results/telemetry-raw.tar.gz`）
- **遥测信号覆盖**：Jaeger 采样链路 `1330` 条（跨 Client+Server 全链路 `763` 条，异常链路 `132` 条）· Loki 业务日志 `904` 条（服务端 Warn/Error `455` 条，客户端 Warn/Error `0` 条） · Pebble 事件日志 `1` 份（告警 `0` 条）

### 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

#### 工作负载 `suite1_copy_baseline` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `25.817 s` (`25817.0 ms`) vs FUSE 累计活跃 `0.00 ms`（等效并发度 `0.00x`）；占比基准：`Server ConnectRPC 总耗时` (`53542.86 ms`)
- **缓存与流水线命中**：NSCache Attr `N/A` / Entry `N/A` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.78%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 83,734 | 53542.86 | 0.639 | 0.216 | 93.500 | 100.0% | HTTP Body+Codec 差值均值 0.263 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 83,734 | 31491.85 | 0.376 | 0.143 | 93.500 | 58.8% | 非 OK 状态 798 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 16,714 | 11530.66 | 0.690 | 0.182 | 15.770 | 21.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 34,537 | 9536.47 | 0.276 | 0.046 | 5.699 | 17.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 48,352 | 4262.70 | 0.088 | 0.004 | 1.532 | 8.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 109 | 1011.87 | 9.283 | 8.833 | 24.969 | 1.9% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 35,356 | 9002.31 | 0.255 | 0.031 | 5.610 | 16.8% | read(34537), lookup(798), statfs(21) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 82,429 | 779.98 | 0.009 | 0.004 | 0.046 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 3,036 | 50.00 | 0.016 | 0.014 | 0.070 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 42,015 | 395.74 | 0.009 | 0.008 | 0.041 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 42,015 | 114.69 | 0.003 | 0.003 | 0.015 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 26,465 | 250.24 | 0.009 | 0.008 | 0.025 | 0.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 29,965 | 7853.33 | 0.262 | 0.003 | 4.716 | 14.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 16,716 | 2875.16 | 0.172 | 0.016 | 3.600 | 5.4% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 13,820 | 2885.62 | 0.209 | 0.181 | 1.928 | 5.4% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 15,358 | 2860.74 | 0.186 | 0.145 | 2.005 | 5.3% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 10,355 | 2671.48 | 0.258 | 0.187 | 2.402 | 5.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 46 | 14.94 | 0.325 | 0.004 | 3.850 | 0.0% | — |

#### 工作负载 `suite1_fork_matrix` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `53.009 s` (`53009.0 ms`) vs FUSE 累计活跃 `0.00 ms`（等效并发度 `0.00x`）；占比基准：`Server ConnectRPC 总耗时` (`67828.35 ms`)
- **缓存与流水线命中**：NSCache Attr `N/A` / Entry `N/A` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.57%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 215,436 | 67828.35 | 0.315 | 0.094 | 15.625 | 100.0% | HTTP Body+Codec 差值均值 0.172 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 215,436 | 30833.98 | 0.143 | 0.046 | 15.625 | 45.5% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 111,366 | 24665.72 | 0.221 | 0.040 | 4.723 | 36.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Lookup` | 103,720 | 5162.54 | 0.050 | 0.042 | 0.223 | 7.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 225 | 0.72 | 0.003 | 0.003 | 0.014 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 215,211 | 25312.99 | 0.118 | 0.021 | 4.699 | 37.3% | read(111366), lookup(103720), statfs(125) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 525,246 | 2896.26 | 0.006 | 0.003 | 0.030 | 4.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 8,120 | 261.14 | 0.032 | 0.018 | 0.239 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 225 | 6.26 | 0.028 | 0.005 | 0.166 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 225 | 0.40 | 0.002 | 0.003 | 0.007 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 115,740 | 19688.65 | 0.170 | 0.003 | 3.910 | 29.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 100 | 14.73 | 0.147 | 0.176 | 0.250 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 5 | 0.01 | 0.002 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 125 | 4.55 | 0.036 | 0.054 | 0.186 | 0.0% | — |

#### 工作负载 `suite2_online_fork` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `11.998 s` (`11998.0 ms`) vs FUSE 累计活跃 `16992.63 ms`（等效并发度 `1.42x`）；占比基准：`FUSE 活跃总耗时` (`16992.63 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.86%` / Readlink `N/A` · WriteBehind 聚批 `1246.21 MiB` / 直通 `1286.00 MiB` (Flush RPC `2722`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `97.49%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 34,396 | 16992.63 | 0.494 | 0.004 | 7.216 | 100.0% | 错误返回 7405 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 2,770 | 10344.15 | 3.734 | 3.397 | 14.031 | 60.9% | 占 FUSE 60.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,627 | 3521.00 | 0.761 | 0.052 | 6.326 | 20.7% | 占 FUSE 20.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 2,770 | 2304.79 | 0.832 | 0.544 | 4.061 | 13.6% | 占 FUSE 13.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 12,358 | 16492.45 | 1.335 | 1.096 | 24.400 | 97.1% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 12,358 | 7677.69 | 0.621 | 0.572 | 23.910 | 45.2% | Write:1.096ms, Create:0.505ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 28,474 | 13735.67 | 0.482 | 0.396 | 49.375 | 80.8% | HTTP Body+Codec 差值均值 0.136 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 28,474 | 9838.37 | 0.346 | 0.259 | 49.375 | 57.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,008 | 4235.57 | 1.057 | 0.731 | 5.688 | 24.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 2,770 | 3604.53 | 1.301 | 1.142 | 6.106 | 21.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 12,364 | 3399.82 | 0.275 | 0.004 | 3.822 | 20.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 16,110 | 535.97 | 0.033 | 0.024 | 0.954 | 3.2% | read(15632), readdirplus(110), lookup(344) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 34,279 | 246.53 | 0.007 | 0.005 | 0.044 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 430 | 26.81 | 0.062 | 0.020 | 0.805 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 10,478 | 153.89 | 0.015 | 0.013 | 0.051 | 0.9% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 10,478 | 36.33 | 0.003 | 0.003 | 0.010 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 7,853 | 66.12 | 0.008 | 0.007 | 0.025 | 0.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 15,632 | 118.31 | 0.008 | 0.003 | 0.194 | 0.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,036 | 1210.49 | 0.300 | 0.036 | 2.000 | 7.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 2,194 | 591.80 | 0.270 | 0.135 | 2.433 | 3.5% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 2,614 | 1834.48 | 0.702 | 0.208 | 4.653 | 10.8% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 10 | 68.12 | 6.812 | 7.500 | 9.950 | 0.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 1,523 | 1454.16 | 0.955 | 0.806 | 4.499 | 8.6% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 42 | 0.80 | 0.019 | 0.003 | 0.218 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 2,207 | 517.66 | 0.235 | 0.090 | 1.999 | 3.0% | — |

#### 工作负载 `suite3_fanout_cow` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.489 s` (`7489.0 ms`) vs FUSE 累计活跃 `0.00 ms`（等效并发度 `0.00x`）；占比基准：`Server ConnectRPC 总耗时` (`15920.51 ms`)
- **缓存与流水线命中**：NSCache Attr `N/A` / Entry `N/A` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `96.69%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,831 | 15920.51 | 8.695 | 4.955 | 91.500 | 100.0% | HTTP Body+Codec 差值均值 5.658 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,831 | 5560.79 | 3.037 | 1.529 | 91.500 | 34.9% | 非 OK 状态 64 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 336 | 3014.70 | 8.972 | 4.104 | 46.182 | 18.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,253 | 2113.00 | 1.686 | 0.934 | 9.766 | 13.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 450 | 298.13 | 0.663 | 0.003 | 20.781 | 1.9% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 39 | 391.83 | 10.047 | 9.821 | 24.692 | 2.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,380 | 2076.24 | 1.505 | 0.833 | 9.761 | 13.0% | read(1253), lookup(110), statfs(17) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,041 | 32.06 | 0.031 | 0.021 | 0.182 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,600 | 23.15 | 0.014 | 0.012 | 0.092 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 437 | 16.06 | 0.037 | 0.015 | 0.493 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 437 | 3.62 | 0.008 | 0.006 | 0.046 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 355 | 4.39 | 0.012 | 0.012 | 0.044 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,926 | 1934.67 | 1.005 | 0.494 | 4.982 | 12.2% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 345 | 589.33 | 1.708 | 1.596 | 5.688 | 3.7% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 47 | 55.48 | 1.180 | 0.241 | 7.650 | 0.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 32 | 203.04 | 6.345 | 3.333 | 24.314 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 25 | 122.31 | 4.892 | 5.833 | 9.917 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 40 | 158.14 | 3.954 | 2.812 | 22.000 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 26 | 6.50 | 0.250 | 0.004 | 4.350 | 0.0% | — |

### 客户端 FUSE 累计耗时 Top 操作与子系统统计（`bowl stats`）

NamespaceCache 命中率：Attr `100.00%` / Entry `99.86%` / Readlink `N/A`（负缓存命中 `2,778`）；WriteBehind 聚批 `1246.21 MiB` / 直通 `1286.00 MiB`（Flush RPC `2,722`，背压等待 `0.00 ms`，屏障等待 `0.00 ms`）；ReadAhead 预读 `0.00 MiB`（命中 `0.00 MiB`，等待 `0.00 ms`）。

| FUSE 操作 | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `Fsync` | 2,770 | 10344.15 | 60.9% | 3734.3 | 3397.2 | 7992.2 | 14031.0 |
| `Writev` | 4,627 | 3521.00 | 20.7% | 761.0 | 51.9 | 3729.5 | 6326.0 |
| `Create` | 2,770 | 2304.79 | 13.6% | 832.1 | 543.9 | 2769.4 | 4061.1 |
| `Unlink` | 2,770 | 671.87 | 4.0% | 242.6 | 213.1 | 464.8 | 507.1 |
| `Release` | 2,770 | 72.81 | 0.4% | 26.3 | 4.3 | 13.6 | 104.0 |
| `FsyncDir` | 4 | 19.40 | 0.1% | 4850.0 | 426.7 | 32768.0 | 32768.0 |
| `GetXAttr` | 4,627 | 13.07 | 0.1% | 2.8 | 2.0 | 7.6 | 14.8 |
| `Flush` | 2,770 | 10.26 | 0.1% | 3.7 | 3.2 | 7.8 | 14.6 |

### 服务端 HTTP / ConnectRPC 与存储引擎内部延迟（Prometheus）

| RPC / 引擎内部阶段 | 调用次数 (OK/Err) | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Read` | 162,788 | 92165.75 | 0.566 | 0.099 | 3.335 | 9.971 |
| `http_outer:Write` | 26,516 | 36061.66 | 1.360 | 0.225 | 7.128 | 24.535 |
| `http_outer:Fsync` | 23,586 | 13971.23 | 0.592 | 0.395 | 2.104 | 4.457 |
| `http_outer:Lookup` | 105,158 | 10304.49 | 0.098 | 0.084 | 0.233 | 0.395 |
| `rpc:Read` | 162,788 (162788/0) | 36999.31 | 0.227 | 0.040 | 1.195 | 4.764 |
| `rpc:Write` | 26,516 (26516/0) | 22954.11 | 0.866 | 0.197 | 3.704 | 18.135 |
| `rpc:Fsync` | 23,586 (23586/0) | 12681.61 | 0.538 | 0.361 | 2.028 | 4.337 |
| `rpc:Lookup` | 105,158 (104110/1048) | 5265.34 | 0.050 | 0.042 | 0.097 | 0.224 |
| `rpc:Create` | 23,586 (23586/0) | 4675.72 | 0.198 | 0.095 | 0.583 | 2.171 |
| `rpc:ForkDisk` | 151 (151/0) | 1088.35 | 7.208 | 7.230 | 19.675 | 40.563 |
| `actor_phase:pack_sync_barrier` | 23,074 | 5743.91 | 0.249 | 0.146 | 1.087 | 2.638 |
| `actor_phase:wal_apply_sync` | 20,455 | 4290.56 | 0.210 | 0.177 | 0.485 | 2.135 |
| `actor_phase:phase_a_validate` | 67,153 | 690.84 | 0.010 | 0.008 | 0.024 | 0.046 |
| `actor_phase:wal_apply_nosync` | 43,540 | 398.13 | 0.009 | 0.007 | 0.021 | 0.025 |
| `actor_phase:dead_pack_remove` | 41 | 225.21 | 5.493 | 6.694 | 9.669 | 9.934 |
| `actor_phase:phase_b_mutate` | 67,153 | 188.83 | 0.003 | 0.003 | 0.007 | 0.014 |
| `read_op:read` | 162,788 | 34721.96 | 0.213 | 0.025 | 1.137 | 4.742 |
| `read_op:lookup` | 105,158 | 2181.27 | 0.021 | 0.017 | 0.045 | 0.185 |
| `read_op:readdirplus` | 111 | 25.40 | 0.229 | 0.156 | 0.769 | 0.954 |
| `read_op:getattr` | 11 | 0.47 | 0.043 | 0.038 | 0.072 | 0.095 |
| `kv_read:point_get` | 648,928 | 4046.65 | 0.006 | 0.004 | 0.020 | 0.041 |
| `kv_read:prefix_scan` | 12,628 | 370.03 | 0.029 | 0.018 | 0.048 | 0.247 |
| `blob_pread` | 163,263 | 29594.97 | 0.181 | 0.003 | 1.554 | 4.046 |
| `blob_pwrite` | 26,560 | 5432.09 | 0.205 | 0.017 | 1.354 | 3.401 |

Pebble BlockCache 命中率 `99.40%` · ReadAmp `1.0` · WriteStall `0.0`。

### Jaeger 端到端最慢采样链路 Top 5

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `c7327a5fd108d453` | 283.73 | `bowl-client:read_ahead.Prefetch` | 282.39 | 20.03 | 262.37 | `bowl-client:storage.v1.StorageService/Read` (282.39 ms) |
| `a87bc190b9bc1138` | 199.88 | `bowl-client:read_ahead.Prefetch` | 199.68 | 3.35 | 196.33 | `bowl-client:storage.v1.StorageService/Read` (199.68 ms) |
| `1f273b275834d808` | 198.07 | `bowl-client:read_ahead.Prefetch` | 198.00 | 3.53 | 194.48 | `bowl-client:storage.v1.StorageService/Read` (198.00 ms) |
| `e83a9a76de70ba2e` | 194.69 | `bowl-client:read_ahead.Prefetch` | 194.26 | 3.81 | 190.45 | `bowl-client:storage.v1.StorageService/Read` (194.26 ms) |
| `da39a3148575c77b` | 183.56 | `bowl-client:read_ahead.Prefetch` | 181.08 | 2.69 | 178.39 | `bowl-client:storage.v1.StorageService/Read` (181.08 ms) |

### `pprof` CPU / Heap / Mutex 采样热点索引（`-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `suite1_fork_matrix_cpu.pb.gz` | `3.61s 50.14% 50.14%      3.61s 50.14%  internal/runtime/syscall/linux.Syscall6` | `3.61s 50.14% 50.14%      3.61s 50.14%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_suite1_fork_matrix_cpu.txt`](results/telemetry/pprof_top/server_suite1_fork_matrix_cpu.txt) |
| `server` | `suite1_fork_matrix_heap.pb.gz` | `32.01MB 29.04% 29.04%    32.01MB 29.04%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    74.84MB 67.91%  connectrpc.com/connect.(*Handler).ServeHTTP` | [`pprof_top/server_suite1_fork_matrix_heap.txt`](results/telemetry/pprof_top/server_suite1_fork_matrix_heap.txt) |
| `server` | `suite1_fork_matrix_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_suite1_fork_matrix_mutex.txt`](results/telemetry/pprof_top/server_suite1_fork_matrix_mutex.txt) |
| `server` | `suite2_online_fork_cpu.pb.gz` | `3.34s 38.79% 38.79%      3.34s 38.79%  internal/runtime/syscall/linux.Syscall6` | `0.02s  0.23%  0.23%      3.94s 45.76%  net/http.(*http2serverConn).runHandler` | [`pprof_top/server_suite2_online_fork_cpu.txt`](results/telemetry/pprof_top/server_suite2_online_fork_cpu.txt) |
| `server` | `suite2_online_fork_heap.pb.gz` | `32.01MB 31.63% 31.63%    32.01MB 31.63%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    77.46MB 76.54%  github.com/starsrc/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_suite2_online_fork_heap.txt`](results/telemetry/pprof_top/server_suite2_online_fork_heap.txt) |
| `server` | `suite2_online_fork_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_suite2_online_fork_mutex.txt`](results/telemetry/pprof_top/server_suite2_online_fork_mutex.txt) |
| `server` | `suite3_fanout_cow_cpu.pb.gz` | `3.68s 55.34% 55.34%      3.68s 55.34%  internal/runtime/syscall/linux.Syscall6` | `3.68s 55.34% 55.34%      3.68s 55.34%  internal/runtime/syscall/linux.Syscall6` | [`pprof_top/server_suite3_fanout_cow_cpu.txt`](results/telemetry/pprof_top/server_suite3_fanout_cow_cpu.txt) |
| `server` | `suite3_fanout_cow_heap.pb.gz` | `94.94MB 48.62% 48.62%    94.94MB 48.62%  bytes.growSlice` | `0     0%     0%   169.65MB 86.87%  github.com/starsrc/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | [`pprof_top/server_suite3_fanout_cow_heap.txt`](results/telemetry/pprof_top/server_suite3_fanout_cow_heap.txt) |
| `server` | `suite3_fanout_cow_mutex.pb.gz` | `—` | `—` | [`pprof_top/server_suite3_fanout_cow_mutex.txt`](results/telemetry/pprof_top/server_suite3_fanout_cow_mutex.txt) |
| `client` | `suite1_fork_matrix_cpu.pb.gz` | `10ms   100%   100%       10ms   100%  compress/flate.(*compressor).deflate` | `0     0%     0%       10ms   100%  bufio.(*Writer).Flush` | [`pprof_top/client_suite1_fork_matrix_cpu.txt`](results/telemetry/pprof_top/client_suite1_fork_matrix_cpu.txt) |
| `client` | `suite1_fork_matrix_heap.pb.gz` | `5953.18kB 57.58% 57.58%  5953.18kB 57.58%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%     5.81MB 57.58%  github.com/hanwen/go-fuse/v2/fuse.(*fuseFD).readRequest` | [`pprof_top/client_suite1_fork_matrix_heap.txt`](results/telemetry/pprof_top/client_suite1_fork_matrix_heap.txt) |
| `client` | `suite1_fork_matrix_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_suite1_fork_matrix_mutex.txt`](results/telemetry/pprof_top/client_suite1_fork_matrix_mutex.txt) |
| `client` | `suite2_online_fork_cpu.pb.gz` | `1.24s 37.80% 37.80%      1.24s 37.80%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.51s 46.04%  net/http.(*http2clientStream).doRequest` | [`pprof_top/client_suite2_online_fork_cpu.txt`](results/telemetry/pprof_top/client_suite2_online_fork_cpu.txt) |
| `client` | `suite2_online_fork_heap.pb.gz` | `16.01MB 31.60% 31.60%    16.01MB 31.60%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%    35.95MB 70.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_suite2_online_fork_heap.txt`](results/telemetry/pprof_top/client_suite2_online_fork_heap.txt) |
| `client` | `suite2_online_fork_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_suite2_online_fork_mutex.txt`](results/telemetry/pprof_top/client_suite2_online_fork_mutex.txt) |
| `client` | `suite3_fanout_cow_cpu.pb.gz` | `10ms 33.33% 33.33%       10ms 33.33%  aeshashbody` | `0     0%     0%       20ms 66.67%  bufio.(*Writer).Flush` | [`pprof_top/client_suite3_fanout_cow_cpu.txt`](results/telemetry/pprof_top/client_suite3_fanout_cow_cpu.txt) |
| `client` | `suite3_fanout_cow_heap.pb.gz` | `16.01MB 31.60% 31.60%    16.01MB 31.60%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%    35.95MB 70.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | [`pprof_top/client_suite3_fanout_cow_heap.txt`](results/telemetry/pprof_top/client_suite3_fanout_cow_heap.txt) |
| `client` | `suite3_fanout_cow_mutex.pb.gz` | `—` | `—` | [`pprof_top/client_suite3_fanout_cow_mutex.txt`](results/telemetry/pprof_top/client_suite3_fanout_cow_mutex.txt) |

---

## 8. 阿里云按量资源生命周期与销毁审计（Cloud Resource Audit）

| 审计项 | 记录值 |
| :--- | :--- |
| **临时 SSH 密钥对** | `bowl-forkbench-kp-20261112-151158`（已调用 `DeleteKeyPairs` 销毁，残留密钥对数 `0`） |
| **Server 节点实例 ID** | `i-<redacted-server>`（已调用 `DeleteInstances --Force true` 销毁） |
| **Client 节点实例 ID** | `i-<redacted-client>`（已调用 `DeleteInstances --Force true` 销毁） |
| **实例创建时间 (UTC)** | `2026-11-12T15:12:07Z` |
| **实例销毁时间 (UTC)** | `2026-11-12T15:19:34Z` |
| **销毁后存量实例核验** | `DescribeInstances` 核验目标实例残留数为 `0`（存量 VPC/VSwitch/安全组完整保留） |
