# 0032. 收缩为单盘 `StoreActor` 架构与无会话 `Epoch` 世代号栅栏控制面

## 状态
已采纳 (accepted) — 取代 [ADR-0025](0025-unidirectional-session-heartbeat-and-client-expiry-cleanup.md) 与 [ADR-0029](0029-pebble-per-branch-storage-swmr-and-checkpoint-commit.md) 中的逻辑分支树（`Branch` / `Commit` / `Fork` / `Merge` / `Rollback` / `Diff`）及 `ClientSession` 心跳租约表设计，并演进 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 的 `"sys:stat"` 持久化统计结构。

## 背景
在 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 与 [ADR-0031](0031-shardstore-append-only-pack-storage-and-256kib-block-grid.md) 完成独立 `StoreActor` 存储引擎与 `64 MiB` Append-Only `.pack` 数据层之后，仓库同时残留了旧的 `internal/meta`、`internal/pebblekv`、`internal/readbuf` 实现以及面向多分支版本树（`Branch` / `Commit`）与有状态会话租约（`ClientSession`）的复杂控制面：
1. **分支树与双套引擎包袱过重**：`meta` + `pebblekv` 承载了跨分支配额去重（`TreeQuota`）、三路合并（`Merge`）、版本快照（`Commit`）与分支锁表（`BranchLockTable`），而当前核心阶段目标是打磨极简、高性能的单盘存储底座。
2. **`ClientSession` 租约表与周期性心跳开销**：客户端挂载后需常驻后台协程周期性发送 `RenewSession` RPC 更新 PostgreSQL `client_sessions.expires_at`，服务端在每次数据面 RPC 中需经 `BranchWorkspaceGate` 查 `sessionCache`（软过期 `1s`）并在锁内二次复查，且旧写端崩溃后存在 TTL 超时等待窗口。
3. **配额上限（`max_bytes`）双源分裂**：磁盘的实时用量（`LogicalBytes`、`Inodes`）已在 `StoreActor` 的 Pebble `"sys:stat"` 中权威维护，但配额上限 `max_bytes` 仍滞留在 PostgreSQL `disks` 表中，导致控制面与数据面状态割裂。

## 决策

1. **全面扶正 `internal/store` 单盘模型，裁撤 `meta` / `pebblekv` / `readbuf` 与分支特性**
   - `1 个 Disk = 1 个 StoreActor 实例`（数据目录位于 `<storage_root>/disks/<disk_id>`），彻底移除 `internal/meta`、`internal/pebblekv`、`internal/readbuf`。
   - 从 `storage.proto`、服务端 `server`、客户端 `bowl` CLI 及 `GLOSSARY.md` 中彻底移除 `Branch`、`Fork`（`CreateBranch` / `ListBranches` / `DeleteBranch`）、`Commit`、`Rollback`、`Merge`（`Merge` / `ResolveMerge` / `AbortMerge`）、`Diff` 以及 `Disk` 消息体中的分支字段。
   - 移除 `internal/store` 中仅供跨分支 Diff/Merge 与快照硬链接使用的 `DirtyKey`（`"dirty:<ino>"`）、`SealActivePack`、`LinkAllPacksTo` 及外部 `ReserveInode` / `ReservePack` 钩子。

2. **Pebble `"sys:stat"` 统管单盘配额上限（`MaxBytes`）与写世代号（`WriterEpoch`），PostgreSQL `disks` 表极简化**
   - 将 Pebble `StatKey`（`"sys:stat"`）定长值从 `25 B` 扩充为 `41 B`：`[ver(1B) | LogicalBytes(8B) | Inodes(8B) | NextWriteSeq(8B) | WriterEpoch(8B) | MaxBytes(8B)]`。
   - 容量配额上限 `MaxBytes`、实时用量 `LogicalBytes` / `Inodes`、根目录 `RootIno(1)` 与写世代号 `WriterEpoch` 100% 由单盘 `StoreActor`（Pebble）权威持久化并在 Phase A 内存校验（含 `UpdateDisk` 缩容水位线校验 `newMaxBytes >= LogicalBytes`）。
   - PostgreSQL 控制面（`services/storage/db`）彻底删除 `commits` 表、`client_sessions` 表与 `renew_control_plane_session` 函数，`disks` 表仅保留 `(disk_id, name, created_at, updated_at)` 4 列，仅负责盘身份登记、改名与 `ListDisks` 游标分页。
   - 新建极简控制面包 `services/storage/internal/control`（保持 `internal/store` 零 PostgreSQL 依赖），仅负责 `CreateDisk`、`GetDisk`、`ListDisks`、`UpdateDisk`、`DeleteDisk`、`MountDisk` 6 个控制面操作及 `Actor(ctx, diskID) (*store.StoreActor, error)` 实例池路由；`internal/server` 数据面 Handler 通过 `control.Actor` 拿到 `*store.StoreActor` 后直接调用引擎方法，不设 22 个数据面方法的中间透传接口。

3. **以无会话 `Epoch` 世代号栅栏与单次 `MountDisk` 握手替代 `ClientSession`（移除 `RenewSession` / `CloseSession` / `UmountDisk`）**
   - **挂载握手（`MountDisk`）**：
     - 可写挂载（`read_only = false`）：`StoreActor` 原子递增并持久化 `WriterEpoch++` 至 `"sys:stat"`，顺手调用 `CleanOrphans()` 清扫上一任写端残留的 `OrphanKey`，返回新 `epoch` 与根 `Inode`。新挂载 0 等待立即接管并废除旧写端。
     - 只读挂载（`read_only = true`）：仅校验目标 `Disk` 存在并返回根 `Inode`（`epoch = 0`），直接无状态共享直读实时 `StoreActor`。
   - **数据面寻址与毫秒级世代号拦截**：
     - 所有数据面 Request 将原 `session_id` 替换为必填 `string disk_id`；所有写/变更类 Request 额外携带必填 `uint64 epoch`（读请求仅携带 `disk_id`）。
     - `StoreActor` 在 Phase A 纯内存比对 `req.Epoch == s.stat.WriterEpoch`；若不相等则当场拒绝并返回 `ErrStaleEpoch`（映射为 `CodeFailedPrecondition` / POSIX `EBUSY`）。
     - 客户端彻底移除后台心跳续期协程与过期自动重连轮换逻辑（被新 `Epoch` 抢占的旧客户端直接向本地 FUSE 返回 `EBUSY`，防止新旧写端互相抢占乒乓）。
   - **卸载与删盘（无 `UmountDisk` RPC、无服务端 TTL 删盘阻塞）**：
     - `bowl umount` 仅在客户端本地排空在途请求、释放本地句柄并卸载宿主机 FUSE 挂载点，无需调用服务端 RPC。
     - `bowl disk delete` 在客户端 CLI 保留本机 `/proc/self/mountinfo` 挂载点防误删快检；服务端 `DeleteDisk` RPC 直接原子关闭 `StoreActor`、删除 PostgreSQL `disks` 记录并清理磁盘物理目录，消除无心跳架构下客户端 `kill -9` 崩溃后磁盘无法删除的死锁问题。
