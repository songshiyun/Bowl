# Store (Labs 实验性新一代存储引擎)

## 定位与目标

`store` 是 `services/storage` 下的新一代实验性存储引擎（Labs 第一步），完整架构决策见 [ADR-0030](../../../../docs/adr/0030-store-actor-group-commit-and-four-stage-data-pipeline.md)。

本模块采用**完全重写（Clean-slate）**的方式设计，不迁就现有 `meta`、`pebblekv` 或 `server` 的历史实现包袱，唯一目标是追求**极致的性能与清晰的架构边界**。

## 核心架构：Meta + Data 融合的 Actor 模型

在传统文件存储实现中，文件元数据（Metadata）与文件数据块（Data）往往割裂在不同的组件与锁域中，导致跨层调用开销大、并发锁竞争复杂、状态一致性难以维护。

`store` 将 **文件元数据层（Meta Layer）** 与 **真实数据存储层（Data Layer）** 内聚封装为统一的 **Actor 架构模型**：

1. **Actor 隔离与无锁串行化（Share-Nothing per Actor）**
   - 每个 `Store` Actor 独占管理其辖区内的文件元数据状态与底层数据读写通道。
   - 外部请求通过轻量级消息/命令投递（Mailbox / Channel）进入 Actor，避免传统多线程共享内存模型下复杂的跨层读写锁（Mutex / RWMutex）与死锁风险。
2. **文件元数据层（Meta Layer）**
   - 负责 Inode 属性、目录树命名空间（Dentry）、硬链接引用计数、权限与时间戳等核心 POSIX 文件元数据的极速寻址与变更。
   - 针对高频元数据操作（`Lookup`、`GetAttr`、`Readdir`、`Create`、`Unlink`、`Rename`）设计紧凑的内存与持久化布局。
3. **数据存储层（Data Layer）**
   - 负责文件实际内容（Payload / Extent / Block）的高吞吐写入、零拷贝或最小拷贝读取、追加与截断回收。
   - 与元数据层在同一 Actor 生命周期内协同，实现“元数据变更 + 数据落盘”的最小路径闭环，消除多余的跨模块同步开销。
4. **极致性能导向**
   - **批量化与流水线（Batching & Pipelining）**：利用 Actor 消息队列天然支持 Group Commit 与 I/O 批量合并。
   - **内存友好（Zero/Low Allocation）**：热路径严控堆内存分配与 GC 压力。
   - **清晰可测**：Actor 边界即唯一契约接缝，状态转换确定可控，便于基准压测（Benchmark）与故障注入验证。
