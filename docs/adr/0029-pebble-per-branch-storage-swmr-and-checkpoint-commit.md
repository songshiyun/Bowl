# 0029. 单盘逻辑分支树（Branch Tree）与整树去重逻辑配额、Pebble Checkpoint 分支存储后端、单分支 SWMR 客户端闭环 POSIX 与控制面三路 Merge

## 状态
已采纳 (accepted) — 取代并重构 [ADR-0009](0009-fuse-file-handle-lifecycle-and-orphan-reclamation.md)、[ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)、[ADR-0014](0014-disk-service-and-quota-ledger.md)、[ADR-0019](0019-chunk-slice-block-nvme-data-layer.md)、[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)、[ADR-0027](0027-4mib-chunk-boundary-and-single-worker-compaction-gc.md) 与 [ADR-0028](0028-single-statement-atomic-cte-async-wal-and-smart-keep-cache.md)

## 背景
Bowl 面向 MicroVM Sandbox 提供支持随时打断点（`Commit`）、开分支（`Fork`）、分支合并（`Merge`）、版本回档（`Rollback`）与差异对比（`Diff`）的云端根文件系统（rootfs）。原架构将服务端定位为「多写多读（MWMR）实时 POSIX 裁决器」，把全局控制面账本与高频文件系统状态（`inodes`、`entries`、`inode_xattrs`、`chunks`、`slice_refs`、`file_handles`、`file_locks`）混装在集中式 PostgreSQL 中，配合自定义 `Chunk -> Slice -> Block` 切块与后台轮询 Compaction/GC；而初版 0029 草案又存在三个方向偏差：
1. **误将 `Disk` 限制为无分支的单线模型，或误把分支当成相互割裂的独立 `Disk`**：在 MicroVM Sandbox 树状演进场景中（例如根分支 `main`/A 派生出分支 B，B 再派生出分支 C 和 D，C 与 D 完成后合并回 B），分支本质上是同一块 `Disk` 内部的**逻辑态树节点（Branch）**，整棵树应当共享同一个 `Disk` 的总配额池，且绝不能把 `Commit` 快照或分支共享未修改文件的后台物理成本转嫁给用户。
2. **未明确 `Rollback` 与 `Merge` 的控制面边界**：每个 Sandbox 将某个分支挂载为自己的根目录（`/` rootfs）。当分支存在活跃写端时，若直接在线篡改底层 rootfs 会导致沙箱内运行进程与内核缓存错乱；`Rollback` 与 `Merge` 应当在目标分支**无活跃写端**（控制面已卸载或销毁写端）的前提下，由沙盒控制面调用服务端完成。
3. **初版客户端全局 1 秒内存写缓冲与服务端全文 Diff 越界**：客户端用户态 1 秒定时缓冲在进程崩溃时会丢失已确认写入，不如标准本地文件系统语义（`close` / `fsync` / `Commit` 强持久化屏障）可靠；服务端直接做大文本全文行级 Diff 也会阻塞存储节点 CPU。

## 决策

### 1. 顶层 `Disk` 承载整棵逻辑分支树（`Branch Tree`），PostgreSQL 管逻辑拓扑，Pebble 专职分支与快照存储

#### 1.1 PostgreSQL 仅维护 `Disk` 逻辑分支树、版本 DAG 与会话租约（0 文件系统状态管理）
PostgreSQL 彻底退化为轻量逻辑控制面，删除 `inodes`、`entries`、`inode_xattrs`、`chunks`、`slice_refs`、`file_handles`、`file_locks` 等 7 张文件系统实体表，仅保留纯逻辑绑定关系：
- **`disks` 表（顶层虚拟磁盘与逻辑分支树容器）**：
  - 维护 `disk_id`、名称、整棵树共享的总配额上限（`max_bytes`）、存储根目录路径及逻辑分支树状态（`branches`：记录该 `Disk` 下所有逻辑分支 `Branch` 的名称、父分支、当前指向的 `head_commit_id`、底层 Pebble 分支工作区子目录及合并状态；创建 `Disk` 时自动初始化默认根分支 `main`）；
  - **Fork 开分支不新建独立 `Disk`**：从任意分支或 Commit 执行 `Fork` 时，仅在当前 `Disk` 的逻辑分支树中新增一个 `Branch` 节点（如 `main -> B -> C, D`），对外始终归属于同一个 `disk_id`；
- **`commits` 表（Git 风格有向无环图 DAG 版本节点）**：
  - 记录隶属于某 `Disk` 的不可变 `Commit` 快照节点，包含 `commit_id`、`disk_id`、`branch_name`、主父版本指针 `parent_commit_id`、合并父版本指针 `merge_parent_commit_id`（支撑三路合并求最近共同祖先 LCA）及对应的底层 Pebble `Checkpoint` 快照目录路径；
- **`client_sessions` 表（按 `(disk_id, branch_name)` 粒度的单写多读租约锁）**：
  - 管理客户端挂载会话租约（`RenewSession` / `CloseSession`），强控**同一个 `(disk_id, branch_name)` 同一时刻最多 1 个独占可写会话挂载该分支实时工作区**，不同逻辑分支（如分支 `C` 与分支 `D`）可同时被不同 Sandbox 独占可写挂载；多个共享只读会话可挂载任意不可变 `Commit` 快照；
  - `DeleteDisk` 时校验该 `Disk` 下所有分支均无活跃挂载会话（否则返回 `ErrBusy`），删除逻辑记录后直接清理该 `Disk` 根目录下的全部分支与快照目录。

#### 1.2 底层基于官方 Pebble 纯元数据索引 + 分支内 `blobs/` 不可变块文件硬链接高效承载分支树与 `1 MiB` 分片存储
- **1 个 `Disk` 对应 1 个根存储目录（`/data/disks/<disk_id>/`），内部按 `branches/<branch_id>` 与 `commits/<commit_id>` 组织分支/快照目录，并由进程内常驻句柄池复用**：
  - 每个活跃逻辑分支（`Branch`）拥有自己独立的工作区子目录（`branches/<branch_id>`，内含 Pebble 元数据实例与 `blobs/<shard>/<blob_id>.blob` 物理数据块子目录），从根本上避免多分支（如 Sandbox C 和 Sandbox D 同时运行）争抢目录文件锁；
  - **分支与快照 Pebble 句柄常驻池化复用**：服务端按 `(disk_id, branch_id)` 与 `(disk_id, commit_id)` 维护带引用计数与共享 `pebble.Cache` 的常驻 `*pebble.DB` 句柄池（严禁每次 RPC 重复 `pebble.Open` / `Close`），暂存目录（`.fork-` / `.merge-` / `.restore-`）使用独立短命句柄；在 `Rollback`、`Merge`、`DeleteBranch`、`DeleteDisk` 执行底层目录重命名或删除前，必须在分支拓扑独占写锁下驱逐并关闭对应池化句柄；
  - **Pebble 纯元数据索引与 `blobs/` 不可变物理块文件解耦（消除 LSM 双写放大与缓存污染）**：
    - 分支内的目录项（`Entry`）、节点属性（`Inode`）、扩展属性（`Xattr`）、自上次分叉以来的变更节点索引（`dirty:<ino>`）与按 **`1 MiB` 固定边界切分**（对齐 Linux FUSE `MaxWrite = 1 MiB`）的文件数据块引用索引（`data:<ino>:<block_idx>`，`block_idx = offset / 1 MiB`，值仅存储定长紧凑 `BlobRef{blob_id, length}`）存入 Pebble，而实际文件数据存储在当前分支或快照目录下的 `blobs/<shard>/<blob_id>.blob`（256 分片子目录 `00`~`ff`）不可变物理块文件中；
    - `Inode` 编号（`ino`）与物理块编号（`blob_id`）均在整棵 `Disk` 树内单调唯一，通过磁盘级分配器的内存批量号段预留（一次预留 4096 个编号）持久化上界，消除逐次分配 I/O；
    - **满 `1 MiB` 整块锁外直写 Page Cache + 旧块即时回收**：写入整块 `1 MiB` 时，在 Inode 锁与 Pebble 提交外（仅持分支生命周期读锁）直接将数据写入 `blobs/<shard>/<new_blob_id>.blob`（进入 OS Page Cache，不经过 Pebble WAL 与 MemTable、不做压缩、不触发同步 `fsync`）；随后在目标文件 Inode 写锁内将 Pebble `data:<ino>:<block_idx>` 更新为 `new_blob_id`，并在 `Batch.Commit(pebble.NoSync)` 成功后立即 `os.Remove` 当前分支 `blobs/` 下被替换的 `<old_blob_id>.blob`（若该旧块未被任何历史 `Commit` 硬链接则 OS 当场释放磁盘空间与页缓存；若已被快照硬链接则仅递减 `st_nlink` 计数，历史快照零影响）；
    - **不足 `1 MiB` 局部写 CoW、截断与打洞即时清理**：彻底移除 Pebble `Merge` 补丁算子（`BlockValueMerger`）；已跨越 `Sync`/`Commit`/`Fork` 落盘固化的 `.blob` 块绝对不可变（对其局部覆写采用写时复制 CoW 写出全新 `<new_blob_id>.blob`）；处于当前 `Sync` 窗口内新建、尚未持久化且未被硬链接共享的活跃尾块允许按逻辑偏移执行窗口内尾部追加（`WriteAt + Truncate`，同时截断上一次失败批次可能遗留的残留字节），一旦跨越 `Sync`/`Commit`/`Fork` 即转为不可变；更新 Pebble `DataKey` 后立即 `os.Remove` 当前分支下的旧块文件；跨块截断（`SetAttr`）、打洞/清零（`Fallocate`）、孤儿回收与拓扑删除在扫描并清理 `DataKey` 范围后，同步 `os.Remove` 当前分支 `blobs/` 下对应的块文件；未持久化的过渡块删除不触发冗余目录屏障，崩溃残留由分支孤儿清扫兜底；进程重启恢复或清扫分支孤儿时，对比分支 `blobs/` 目录与 Pebble `DataKey` 引用集清理崩溃残留的未提交孤儿块文件；
  - **秒级零拷贝 `Commit`、逻辑 `Fork` 与 `Merge` 块硬链接**：
    - 执行 `Commit` 时，先刷盘分支脏块文件并调用带 `pebble.WithFlushedWAL()` 的 Pebble 原生 **`Checkpoint`**，同时将分支 `blobs/<shard>/*.blob` 通过 Linux 硬链接（`os.Link`，若遇极端 `EMLINK` 硬链接上限自动回退为文件复制）在毫秒级生成 `commits/<commit_id>` 快照目录（0 字节数据复制）；
    - 执行 `Fork`（如从分支 `B` 派生逻辑分支 `C` 和 `D`）时，直接基于基准 Commit 快照目录通过硬链接瞬间派生出 `branches/C` 和 `branches/D` 工作区目录（含 Pebble `.sst` 与 `blobs/` 下的块文件）——未修改的物理文件在 `A`、`B`、`C`、`D` 之间 100% 零拷贝共享，删除分支或快照时直接 `os.RemoveAll` 目录即可由文件系统引用计数自动回收物理空间；
  - **细粒度并发锁模型与单键点查免快照**：
    - 分支级 `RWMutex` 严格收敛为**分支拓扑生命周期锁**：仅 `Commit`、`Rollback`、`Merge`、`DeleteBranch`、`DeleteDisk` 及无活跃写端时的孤儿清扫拿独占写锁 `Lock()`；所有常规 POSIX 读写改删 RPC 全部拿共享读锁 `RLock()`；
    - 分支内部按 `Inode` 细粒度读写锁串行化单文件读改写与块替换定序（`Read` 在读取数据块期间持目标文件 Inode `RLock()`，防止并发覆写执行 `os.Remove` 造成 `ENOENT`；目录项变更按父目录与目标 Inode 定序加锁）；单键只读点查（`GetAttr`、`GetXattr`）直接读取 `branch.DB()`，免除高频创建与销毁 Pebble `Snapshot` 的全局锁争用。

---

### 2. 整棵 Disk 树共享总配额池：严格按「用户视角跨分支同文件最大逻辑体积」去重核算
- **绝不向用户暴露或转嫁快照与分支的物理冗余成本**：
  - 无论用户在同一个文件中反复覆盖写多少次、打了多少个 `Commit` 断点，或者从 `A` 派生出多少个未修改的分支 `B`、`C`、`D`，后台因保留历史 `Commit` 硬链接而多占用的物理空间**一律不计入用户配额**；
- **整棵树共享 `Disk.max_bytes`，按同一文件（`Inode`）跨分支最大逻辑大小去重统计 `used_bytes`**：
  - 整棵 `Disk` 树的实时已用逻辑空间定义为该 `Disk` 下所有存活逻辑分支中每个有效文件 `Inode` 的最大逻辑 `size` 之和：
    $$\text{used\_bytes}(\text{Disk}) = \sum_{\text{ino} \in \bigcup_{b \in \text{Branches}} \text{LiveInodes}(b)} \max_{b \in \text{Branches}} \text{size}(b, \text{ino})$$
  - **直观效果**：
    1. 根分支 `A` 写入 `1 GB` 文件后，用户视角占用为 **`1 GB`**；
    2. 打 `Commit` 断点并继续把该 `1 GB` 文件覆盖写为新的 `1 GB` 内容，该 `Inode` 逻辑大小仍为 `1 GB`，用户视角占用依然是 **`1 GB`**（而非 `2 GB`）；
    3. 从 `A` 派生出分支 `B` 和 `C` 时，未修改文件以及在 `B` 中覆盖写但未超过原大小的文件，在整棵树上仍只算 **`1 GB`**；只有当 `B` 或 `C` 真正创建了新文件（新 `Inode`）或将已有文件扩容写大（如从 `100 MB` 扩到 `150 MB`）时，整棵树才增加对应的净增逻辑字节数（`+50 MB`）；
  - 由服务端内存中的每盘配额协调器（Quota Coordinator）结合 Pebble 内的 `sys:stat` 在写入时 O(1) 实时强控（超额立即拦截返回 `ENOSPC`），`GetDisk`、`ListDisks`、`UpdateDisk` 与 `StatFS` 直接读取该用户视角逻辑用量。

---

### 3. 单分支 SWMR 客户端本地闭环 POSIX + 标准本地文件系统持久化屏障
- **单分支一写多读（SWMR）与客户端本地闭环**：
  - 客户端挂载时通过 `bowl mount <disk-id> <mountpoint> [--branch <branch>] [--commit <commit-id> --ro]` 指定目标 `Disk` 与逻辑分支（默认挂载根分支 `main`）；
  - 服务端彻底裁撤 `Open`、`Flush`、`Release`、`OpenDir`、`ReleaseDir`、`GetLk`、`SetLk`、`SetLkw`、`TmpFile` 等运行期细碎接口；
  - `FileHandle`（打开引用计数）与 `FileLock`（POSIX 记录锁与 BSD `flock`）100% 在客户端本地内存闭环（0 次跨机 RPC）；客户端按需懒加载并长效缓存所挂载分支的权威目录树；
  - **孤儿节点（`Orphan Inode`）闭环治理**：写端本地打开期间被解链（`nlink == 0`）的已上云文件在对应分支 Pebble 内写入 `orphan:<ino>` 标记，最后一个本地句柄 `Release`（`close`）时当场通知服务端物理删除；只要分支存在活跃写端，后台 GC 绝不清理该分支的 `orphan:*`，仅在无活跃写端时（卸载 `CloseSession`、会话超时或新写端挂载前）按 `orphan:*` 扫描清理；`Commit` 快照生成时无需在热路径重写剔除孤儿，读取与 `Diff` 忽略 `nlink == 0` 即可。
- **标准本地文件系统持久化语义（客户端 `1 MiB` 直通写 + 服务端 OS Page Cache / 异步 WAL + `CAP_HANDLE_KILLPRIV` 零额外 RPC + 显式持久化屏障）**：
  - 客户端保持 `MaxWrite = 1 MiB` 大块直通写（Write-Through，不启用内核 `CAP_WRITEBACK_CACHE` 以免内核将 `1 MiB` 整块拆碎为 `4 KiB` 页触发服务端高频 CoW 重写放大、引发 `O_WRONLY` 读 `EBADF` 及破坏 `O_APPEND` 定序），显式调大 FUSE `MaxBackground` 并发队列深度（如 `64`），并在 `Open`/`Create`（非 `O_TRUNC` 且非 `DirectIO`）时默认启用 `FOPEN_KEEP_CACHE`；
  - 客户端在 `Write` / `Fallocate` 成功后回填本地 `NamespaceCache` 时保留服务端返回的 `AttrTimeoutSeconds`（严禁置 `0` 导致后续每次写入前触发 `GetAttr` RPC）；同时开启 FUSE `CAP_HANDLE_KILLPRIV` 并对未设置过的 `security.capability` 维护客户端本地负缓存，服务端在 `Write`、`SetAttr(SIZE|UID|GID)`、`Fallocate` 的同一 Pebble 批次中顺带删除 `XattrKey(ino, "security.capability")`，彻底消除每次 `write()` 前穿透至服务端的 `GetXattr("security.capability")` 同步 RPC；服务端拦截器对 `ErrNotFound`、`ErrNoData` 等常规 POSIX 状态码静默返回，不打印 `WARN` 错误日志；
  - 服务端常规写入将 `.blob` 块文件直接写入 OS Page Cache，并将轻量元数据批次通过 `pebble.NoSync` 提交至 Pebble WAL（OS Page Cache，保障服务进程崩溃不丢数据），由后台 1 秒周期任务兜底刷盘；
  - **客户端 `Flush`（即 `close`）仅释放本地 POSIX 锁（不触发服务端物理盘 `fsync`）；仅在显式 `Fsync`、`FsyncDir`、客户端 `Umount` 以及触发 `Commit` 快照时，强制同步服务端脏块文件与 `pebble.Sync` 确认物理落盘**，兼顾 POSIX 标准持久化预期与大文件/小文件高吞吐。

---

### 4. 控制面离线（无活跃写端）`Rollback`、三路 `Merge` 与 Git 风格 `Conflict` 裁决、以及轻量 `Diff`
- **铁律：存在活跃可写会话时严禁直接执行 `Rollback` 或作为 `Merge` 目标分支**：
  - 因为 Sandbox 将逻辑分支挂载为自身根目录（`/` rootfs），当目标分支存在活跃写端 `ClientSession` 时，直接调用 `Rollback` 或向该分支执行 `Merge` 一律返回 `ErrBusy`（`EBUSY`）；
  - 沙盒控制面（Orchestrator）的标准工作流为：**先停止/卸载目标分支的写端会话 $\rightarrow$ 调用服务端控制面接口执行 `Rollback` 或 `Merge`（例如将子分支 `C`、`D` 合并回父分支 `B`，或将 `B` 合并回根分支 `main`） $\rightarrow$ 合并完成后重新挂载启动新的 Sandbox 写端**。
- **服务端控制面文件级三路合并（3-Way `Merge`）与 Git 风格冲突（`Conflict`）解决**：
  - **求取 LCA 与增量对比**：当控制面调用 `Merge(disk_id, source_branch, target_branch)` 时（要求 `target_branch` 无活跃写端），服务端先为源分支定格最新 `Commit`，通过 `commits` DAG 找到源分支与目标分支的最近共同祖先 Commit（`Base`），直接扫描两边自 `Base` 以来的变更集（`dirty:<ino>`）执行三路对比；由于每个 `.blob` 物理块文件严格不可变且拥有全局唯一 `blob_id`，`Diff` 与 `Merge` 在对比 `DataKey(ino, block_idx)` 时可直接通过 `BlobRef.blob_id` 相等性实现 $O(1)$ 短路跳过（无需读取未变更 `.blob` 文件），仅在两侧 `blob_id` 不同时比对块字节内容，并在物化合并结果时直接将胜出侧的 `<blob_id>.blob` 硬链接至合并目标目录；
  - **文件级冲突判定（绝不盲目拼接二进制块）**：只在一边发生修改的文件或目录项自动合入目标分支 Pebble；两边做出完全相同修改的自动幂等合入；若**同一个文件（或同名目录项）在 `Base -> Source` 与 `Base -> Target` 均发生了不兼容修改**，一律按**文件级**判定为 `Conflict`（绝不在存储层自动拼接 `1 MiB` 数据块以免损坏二进制/ELF/数据库文件）；
  - **冲突解决双模式**：
    1. **控制面返回冲突清单 + 策略裁决**：默认检测到冲突时先不破坏目标分支原状态，直接向控制面返回结构化冲突文件列表（包含冲突路径及 `Base / Ours / Theirs` 三个版本的属性与标识），支持控制面直接指定 `--strategy=ours|theirs`（或按文件指定采纳哪一方）一键完成合并；
    2. **工作区物化挂载解决（Git Working Tree 模式）**：支持将无冲突变更应用至目标分支工作区，同时将冲突文件在工作区物化为 `<file>`（当前分支版本）与 `<file>.theirs` / `<file>.base` 旁路副本，将目标分支标记为 `MERGING` 状态；控制面可挂载一个临时沙盒进入工作区修改定稿文件并清理冲突副本，随后调用 `Commit`（或 `merge --continue`）生成记录双亲指针（`parent_commit_id` + `merge_parent_commit_id`）的 Merge Commit，或随时调用 `merge --abort` 恢复合并前状态。
- **服务端元数据/块摘要 `Diff` + 客户端按需渲染行级对比**：
  - 服务端 `Diff` RPC 对比同一 `Disk` 下任意两个分支/Commit 的变更索引与 `Inode` / `1 MiB` 分片修改序列号，秒级返回**变更文件列表（新增/删除/修改/重命名、大小与模式变化）及变更的 `1 MiB` 分片索引摘要**（0 字节未变更数据读取，不在服务端跑大文件全文对比）；
  - `bowl diff` CLI 客户端拿到变更摘要后，对小于安全阈值（如 `<= 1 MiB`）的变更文件按需拉取前 `8 KB` 嗅探文本/二进制类型，在客户端本地计算并展示纯文本行级 Unified Diff 或二进制摘要。

## 结果
- **正面效果**：
  - **概念与计费高度统一**：对外只有一个 `Disk` 顶层实体，分支（`A -> B -> C, D`）全部收敛为 `Disk` 内部的逻辑态节点，共享同一个总配额池且按跨分支同文件最大逻辑体积去重核算，写 `1 GB` 再覆盖写 `1 GB` 或开任意多空分支永远只算 `1 GB`，绝不将后台快照物理开销转嫁给用户；
  - **Pebble `Checkpoint` 支撑毫秒级开分支与快照且多沙盒零锁冲突**：同一 `Disk` 下各逻辑分支通过 Pebble `Checkpoint` 硬链接零拷贝共享未修改 `.sst / .blob` 物理文件，同时各自拥有独立 Pebble 工作区子目录，多个子分支沙盒并发运行时互不抢锁；
  - **控制面安全合并与闭环 POSIX**：明确“无活跃写端时执行 `Rollback` / `Merge`”的控制面铁律，服务端直接基于整棵版本树完成文件级三路 `Merge` 与冲突治理，彻底消除运行期 rootfs 在线合并导致的内核状态撕裂。
- **代价与取舍**：
  - `Rollback` 与 `Merge` 要求目标分支处于无活跃写端状态（由沙盒控制面先销毁/卸载旧写端再执行合并并重挂新写端），这与 MicroVM Sandbox 不可变基础设施的编排生命周期完全契合；
  - 服务端后台需通过快照保留策略与引用清理定期回收不再被任何存活分支或保留 `Commit` 引用的历史 Checkpoint 物理空间。
