# 0014. 早期独立 Disk 服务、容量配额账本与 StatFS 设计

## 状态
已被 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 替代 (superseded by ADR-0015) — 早期部分替代 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md)

## 背景
在 [ADR-0002](0002-disk-concept-and-client-root-inode-mapping.md) 中，系统最初假设服务端完全不保存任何 `Disk` 表，只由客户端自己记住挂载的是哪个目录。随着产品需求发展，我们需要按独立的 `Disk`（用 UUID 标识）限制每块盘的最大可用字节数，并支持 POSIX `StatFS` 查询磁盘空间。当时设想由一个外部网关（Gateway）接收 `disk_id` 并把它转换成对应的根目录 `Inode`（`root_ino`）。

当时为了不改动 `storage` 元数据服务的代码，决定先建一个独立的 `disk` 服务来单独管 `Disk` 表和容量配额。（注：后来在实践中发现拆成两个服务会导致文件删除、截断和孤儿回收时没法同步更新配额，因此在 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 中将两者合并到了 `StorageService`）。

## 决策
1. **初期将服务拆成三层（演进 ADR-0002，后被 ADR-0015 替代）**：
   - **`storage` 服务（`proto/storage/v1/storage.proto`）**：只管 `uint64 ino` 的文件目录树操作，不管 `disk_id` 和容量配额。
   - **`disk` 服务（`proto/disk/v1/disk.proto` 与 `services/disk`）**：作为独立服务保存 `Disk` 记录，包含 `disk_id`（UUIDv7）、`name`、绑定的根目录 `root_ino`、最大容量 `max_bytes` 和当前已用字节数 `used_bytes`。
   - **外部 `gateway` 项目**：负责接收 `disk_id`，调 `DiskService` 查出对应的 `root_ino`，并在文件大小变化时调 `DiskService` 检查和加减配额。
2. **磁盘容量增减与扩缩容规则**：
   - `disks` 表保存 `max_bytes BIGINT NOT NULL` 和 `used_bytes BIGINT NOT NULL DEFAULT 0`，并加上数据库 `CHECK (max_bytes > 0 AND used_bytes >= 0 AND used_bytes <= max_bytes)` 约束。
   - **扩容与缩容检查（`UpdateDisk`）**：修改 `max_bytes` 时用行锁（`FOR UPDATE`）锁住记录；**不允许把 `max_bytes` 改得比当前已用空间 `used_bytes` 还小**（如果 `new_max_bytes < used_bytes`，直接拒绝并返回 `FailedPrecondition` / `EINVAL`）。
   - **已用空间增减（`UpdateDiskUsage`）**：按传入的字节变化量 `delta_bytes`（`int64`）更新 `used_bytes`：
     - 当增加空间（`delta_bytes > 0`）且 `used_bytes + delta_bytes > max_bytes` 时，拒绝写入并返回 gRPC `ResourceExhausted`（附带 `ErrorInfo{Reason: "ENOSPC", Domain: "bowl.disk"}`）；
     - 当减少空间（`delta_bytes < 0`）且 `used_bytes + delta_bytes < 0` 时，直接把 `used_bytes` 归零（`0`），防止因为一点账目偏差导致删文件报错。
3. **`StatFS` 返回值的块大小换算**：
   - `DiskService.StatFS` 接收 `disk_id`，查出字节容量（`total_bytes = max_bytes`、`used_bytes`、`avail_bytes = max_bytes - used_bytes`），并按 `4096` 字节一个块（`bsize = 4096`、`frsize = 4096`、`namelen = 255`）换算出：
     - `blocks = max(total_bytes / 4096, 1)`；
     - `bavail = avail_bytes / 4096`，`bfree = bavail`；
     - 不限制 Inode 总数，`files = 0`、`ffree = 0`。
4. **数据库迁移表分开**：
   - `services/disk` 使用独立的 `sqlc.yaml`、`.mockery.yml` 和 `services/disk/db/migrations`，在开发数据库 `bowl` 中使用单独的迁移记录表 `goose_db_version_disk`。

## 结果
- **正面效果**：明确了磁盘容量上下限、缩容不能低于已用空间、以及按 4096 字节块换算 `StatFS` 指标的业务规则。
- **代价与取舍**：需要外部网关在每次截断、扩容、预分配和删文件时跨服务去调 `DiskService.UpdateDiskUsage`，无法保证两边在同一个事务里成功或失败，因此随后被 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 的单服务方案取代。
