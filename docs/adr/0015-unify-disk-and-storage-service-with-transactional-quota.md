# 0015. 将 Disk 管理合并进 Storage 服务并在同一个数据库事务内更新配额

## 状态
部分被 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 与 [ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md) 替代 — 单一 `StorageService` 服务边界保留，PostgreSQL 事务配额账本已迁至单盘 `StoreActor` Pebble `"sys:stat"`（原替代 [ADR-0014](0014-disk-service-and-quota-ledger.md)，演进 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md)）

## 背景
在 [ADR-0014](0014-disk-service-and-quota-ledger.md) 中，我们曾经把 `Disk` 磁盘管理、容量配额统计（`UpdateDiskUsage`）和 `StatFS` 拆成了一个单独的 `DiskService`（`services/disk`），而文件的 `Inode` 大小修改和孤儿节点（`Orphan Inode`）清理则放在 `StorageService`（`services/storage`）里。

把这两个紧密相关的事情拆成两个服务后，暴露出了三个很严重的问题：
1. **跨服务分两步改数据，没法保证配额账目准确**：改变文件大小或删除文件的操作分散在 7 个地方：`SetAttr`（截断或改大小）、`Open(O_TRUNC)`、`Fallocate`、`Unlink`、`Rename`（覆盖已有文件）、`Release`（关闭最后一个句柄时真正删掉孤儿文件）以及 `RenewSession`（会话过期时后台批量删掉孤儿文件）。如果让外部网关去分别调用 `StorageService` 和 `DiskService`，外部调用方既不知道被覆盖或截断的文件原来有多大、不知道文件硬链接数有没有减到 0、不知道还有没有打开的 `FileHandle` 正在占用这个已删文件，更不可能知道后台 `RenewSession` 什么时候清掉了多少孤儿文件。两边的数据很容易对不上。
2. **`StatFS` 查询参数对不上**：Linux FUSE 的 `FUSE_STATFS`（操作码 17）和其他所有文件操作一样，内核传过来的参数只有节点编号 `uint64 ino`，而之前放在 `DiskService` 里的 `StatFS` 却要求传磁盘的 UUID，导致客户端挂载层很难对接。
3. **创建磁盘（`CreateDisk`）分成两步容易出错**：之前 `CreateDisk` 要求外部调用方自己先去建好根目录拿到 `root_ino` 再传进来，没法在一个数据库事务里同时把根目录 `Inode` 和 `Disk` 配额记录一起建好。

## 决策
1. **去掉单独的 `DiskService`，把磁盘管理和配额统计全部合进 `StorageService`**：
   - 删掉独立的 `proto/disk/v1` 接口定义和 `services/disk` 目录，把 `Disk` 的增删改查（`CreateDisk`、`GetDisk`、`UpdateDisk`、`DeleteDisk`）和 `StatFS` 全部合进 `StorageService`（`proto/storage/v1/storage.proto` 与 `services/storage`）。
   - 删掉之前对外暴露的手动改配额接口 `UpdateDiskUsage`；磁盘容量和节点数的检查与增减，全部由 `StorageService` 在修改文件或节点的同一个内部过程中自动完成。
2. **在一个事务里创建磁盘和根目录，支持直接级联删除整块磁盘**：
   - **`CreateDisk`**：不再让调用方传 `root_ino`。`StorageService` 在一个数据库事务里自动创建一个根目录 `Inode`（权限 `S_IFDIR | 0755`，`nlink = 2`，`parent_ino = root_ino`，`root_ino = 自己的 ino`），并同时创建绑定这个 `root_ino` 的 `Disk` 记录（初始 `used_bytes = 0`、`used_inodes = 1`）。
   - **`DeleteDisk`**：在一个数据库事务里，按 `root_ino` 把这块 `Disk` 下面的所有 `Inode`（关联的 `entries`、`inode_xattrs`、`file_handles`、`file_locks` 会通过数据库外键自动一并删除）和 `Disk` 记录全部删掉，并唤醒清理内存里正在等待该盘文件锁的请求。
3. **每个 `Inode` 记录自己所属的 `root_ino`，文件变大或真正删除时同步更新配额**：
   - 在 `inodes` 表里为每个节点记录它属于哪个磁盘根目录 `root_ino`（根目录自己的 `root_ino = ino`；在 `Mknod`、`Mkdir`、`Create`、`Symlink`、`TmpFile` 创建新节点时，直接复制父目录的 `root_ino`）。
   - **文件大小变化时检查并更新已用空间**：在 `SetAttr`、`Open(O_TRUNC)`、`Fallocate`（以及后续的 `Write`）改变常规文件 `size` 时，检查 `used_bytes + delta_bytes <= max_bytes`（如果超出配额上限就返回 `ResourceExhausted` / `ENOSPC`），并同步更新已用空间。
   - **只有文件真正从数据库删除时才退还配额**：当 `Unlink` 或 `Rename` 覆盖导致某个文件的 `nlink` 变成 `0` 时，如果这个文件当前没有任何打开的 `FileHandle`，就当场在事务里把这条 `Inode` 删掉，并减去它的 `used_bytes` 和 `used_inodes`；如果当时还有打开的 `FileHandle`（即变成了 `Orphan Inode`），就暂时不退还配额，一直等到最后一个 `FileHandle` 调用 `Release` 关闭、或者会话过期由 `RenewSession` 把这个 `Orphan Inode` 真正删掉时，才退还它占用的 `size` 和节点数。
   - **禁止跨磁盘硬链接或移动文件（返回 `EXDEV`）**：`Link` 和 `Rename`（包含 `RENAME_NOREPLACE` 和 `RENAME_EXCHANGE`）在锁住源父目录、目标父目录和对应节点后，必须检查它们所属的 `root_ino` 是否完全相同；如果发现有人试图把一个磁盘里的文件硬链接或移动到另一个磁盘去（`root_ino` 不同），立即拒绝并返回 `ErrCrossDevice`（对应 gRPC `FailedPrecondition`、错误原因 `EXDEV`），防止不同磁盘的文件混在一起搞乱配额和删盘逻辑。
4. **`StatFS` 改为直接接收 `ino` 参数**：
   - `StorageService.StatFS` 接收 `uint64 ino`，先查出该 `Inode` 所属的 `root_ino`，再拿到对应 `Disk` 的总容量 `max_bytes`、已用空间 `used_bytes` 和已用节点数 `used_inodes`，按 `4096` 字节一个块换算出 `blocks`、`bfree`、`bavail`，并返回真实已用节点数 `files = used_inodes`。

## 结果
- **正面效果**：彻底解决了拆成两个服务时配额算不准、孤儿文件删了没退配额以及并发冲突的问题；少维护一个多余的服务模块；FUSE 客户端可以直接拿 `ino` 调 `StatFS`。
- **代价与取舍**：`StorageService` 的数据库表结构里需要加上 `disks` 表以及 `inodes.root_ino` 字段（注：关于 `disks` 表里 `used_bytes` / `used_inodes` 的存储方式，后续在 [ADR-0024](0024-deferred-block-sync-and-fsync-durability-barrier.md) 中进一步简化为查 `inodes` 覆盖索引加载 + 内存实时加减）。
