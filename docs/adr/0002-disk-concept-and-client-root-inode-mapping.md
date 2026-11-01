# 0002. 早期客户端根目录 Inode 编号转换设计

## 状态
部分被 [ADR-0014](0014-disk-service-and-quota-ledger.md) 与 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 替代 (partially superseded by ADR-0014 and ADR-0015)

## 背景
本项目采用客户端-服务端架构：服务端提供文件元数据和存储接口，客户端通过 Linux FUSE 协议挂载成本地目录供用户使用。在产品使用上，每个挂载实例对应一块独立的“Disk（磁盘）”。

Linux FUSE 协议硬性规定：挂载点根目录的 Inode 编号必须固定是 `1`（即内核里的 `FUSE_ROOT_ID = 1`）。但服务端要同时管理很多块 Disk，如果服务端给每块 Disk 都从 1 开始分配 Inode 编号，不同 Disk 之间的 Inode 编号就会全部撞车。因此需要明确：怎么在服务端保证全局 Inode 编号唯一，同时又让客户端满足 FUSE 内核“根目录编号必须是 1”的要求。

## 决策
1. **服务端全局递增分配 Inode 编号，每块 Disk 对应一个独立的根目录 Inode**：
   - 服务端的 Inode 编号全局递增、绝不重复（比如：Disk A 的根目录 Inode 是 `2`，它里面的文件是 `3..1000`；新建的 Disk B 的根目录 Inode 是 `1001`）。
   - （注：本 ADR 早期曾假设服务端完全不建 `disks` 表；后续在 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 中，服务端增加了 `disks` 表来保存每块盘的 UUID、`root_ino` 和容量上限，但“每块 Disk 对应一个全局唯一 `root_ino`”的做法保持不变）。
2. **客户端挂载时，把该 Disk 的 `root_ino` 和内核要求的编号 `1` 互相转换**：
   - 客户端在挂载某块 Disk 时，先拿到这块 Disk 真实的根目录编号 `root_ino`。
   - 当 FUSE 内核传进来的请求里节点编号是 `1` 时，客户端把它替换成真实的 `root_ino` 再发给服务端；当服务端返回的结果（比如 `Lookup` 或 `ReadDirPlus` 里的 `.` 和 `..`）里节点编号是 `root_ino` 时，客户端把它替换成 `1` 再报给 FUSE 内核。
   - 除了根目录之外，其他所有子目录和文件的 Inode 编号都原样透传，不做改动。
3. **服务端日常文件操作直接传 `ino`，不用每次都传 `disk_id`**：
   - 像 `Lookup`、`GetAttr`、`Read`、`Write` 这些日常文件操作，请求里只需要传 `(parent_ino, name)` 或 `ino`，不需要每次都传 `disk_id`。

## 结果
- **正面效果**：
  - 服务端所有文件共用一套全局唯一的 `ino` 主键，表结构简单清晰，不会出现不同磁盘的 Inode 撞号问题。
  - 客户端做一次简单的 `1 <-> root_ino` 数字替换，就满足了 FUSE 内核对根目录编号为 `1` 的硬性要求。
- **代价与取舍**：
  - 客户端 FUSE 适配层需要在收发请求和返回目录项时，记得把根目录的 `root_ino` 和 `1` 互相转换。
