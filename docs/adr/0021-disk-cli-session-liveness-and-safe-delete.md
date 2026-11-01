# 0021. Disk 管理命令行（CLI）、挂载会话绑定磁盘与删除磁盘前的存活检查

## 状态
已采纳 (accepted) — 演进 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 与 [ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)

## 背景
在 [ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md) 完成客户端挂载与卸载命令（`bowl mount` / `bowl umount`）后，客户端预留的 `bowl disk` 命令还需要补齐创建、列表查询、查看详情、修改名称或容量、删除磁盘这五个管理命令。在设计这套命令行与服务端配合方式时，遇到四个实际问题：
1. **扩容和缩容用同一个命令更直观，且需要前后端双重检查**：因为 `Disk` 既能调大容量也能调小容量（只要新容量不小于当前已用空间 `used_bytes`），如果命令叫 `expand`（扩容）就会让人误以为不能缩容；同时需要在命令行和服务端两边都检查缩容不能小于已用空间。
2. **磁盘多了之后需要分页查询**：`StorageService` 之前只有查单个磁盘的 `GetDisk`，没有列表接口；当磁盘数量增多时，需要支持稳定的分页查询。
3. **防止误删正在挂载使用的磁盘（演进 ADR-0015）**：[ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 当时把 `DeleteDisk` 设计成不管三七二十一直接强制删除。但在实际使用中，如果某块 `Disk` 正在本机或其他机器上挂载使用，一旦误执行 `bowl disk delete`，整块盘的数据会瞬间被清空，还在挂载的目录也会变成报错的死挂载点。然而当时 `client_sessions` 表里没有记录会话挂载的是哪块 `disk_id`（如果挂载后没有打开任何文件，服务端根本不知道这块盘正被挂载着），而且执行 `bowl umount` 卸载时也没有通知服务端注销会话。
4. **如何区分客户端是“还在正常挂载”还是“已经死机/断电了”**：如果只看数据库里 `client_sessions` 的 60 秒过期时间，一旦客户端进程被 `kill -9` 强杀或者机器直接断电，在接下来的 60 秒内执行 `bowl disk delete` 都会被拦住删不掉；反过来，如果让服务端主动去连客户端上报的 `IP:Port` 检查死活，在容器、NAT 或有防火墙的网络里，服务端的连接会被防火墙拦住，从而**把明明活着的客户端误判成死机**并把正在用的磁盘删掉（详见 [`docs/research/session-liveness-and-disk-delete.md`](../research/session-liveness-and-disk-delete.md)）。

## 决策
1. **提供统一的 `bowl disk` 子命令，并在命令行与服务端双重检查容量**：
   - 在客户端模块 `client/` 中提供 `bowl disk <create|list|get|update|delete>` 五个子命令（`update` 同时支持别名 `resize`），共用 `--endpoint`（`$BOWL_STORAGE_ENDPOINT`）与 `--token`（`$BOWL_TOKEN`）连接参数：
     - `bowl disk create --size <size> [--name <name>] [-q|--quiet] [--json]`
     - `bowl disk list [--limit <n>] [--page-token <token>] [--json]`
     - `bowl disk get <disk-id> [--json]`
     - `bowl disk update <disk-id> [--size <size>] [--name <name>] [--json]`
     - `bowl disk delete <disk-id> [-y|--yes] [--json]`
   - **容量单位支持与缩容检查**：`--size` 支持不区分大小写的容量单位（`K/M/G/T`、`KiB/MiB/GiB/TiB`、`KB/MB/GB/TB`）以及纯数字字节数。执行 `bowl disk update` 改容量时，命令行先调一次 `GetDisk` 检查新容量是否大于等于当前已用空间 `used_bytes`（且大于 `0`），不够时直接打印包含当前已用空间的友好报错；检查通过后再调服务端 `UpdateDisk`，由服务端在数据库行锁保护下做最终校验。
2. **新增 `StorageService.ListDisks` 分页查询接口**：
   - 在 `proto/storage/v1/storage.proto` 中新增 `ListDisks(ListDisksRequest) returns (ListDisksResponse)`，支持 `page_size`（默认 `50`，最大 `1000`）和 `page_token` 游标分页，按创建时间倒序（`created_at DESC, disk_id DESC`）返回磁盘列表和下一页游标 `next_page_token`。
3. **在 `ClientSession` 中记录挂载的 `disk_id`，并在正常卸载 `umount` 时立即调 `CloseSession` 注销**：
   - 在 PostgreSQL `client_sessions` 表中增加 `disk_id UUID REFERENCES disks(disk_id) ON DELETE CASCADE` 列和 `(disk_id, expires_at)` 索引；在 `RenewSessionRequest` 中增加 `optional string disk_id`，由 `bowl mount` 在建立和续期会话时传入。
   - 在 `StorageService` 中新增 `CloseSession(CloseSessionRequest) returns (CloseSessionResponse)`：当用户执行 `bowl umount` 退出挂载守护进程时，客户端在退出前主动调用 `CloseSession` 立即删掉服务端的会话记录，并释放关联的文件句柄、文件锁和孤儿节点。这样正常卸载完就能立刻执行 `bowl disk delete`，不用干等会话过期。
4. **挂载期间保持 `WatchSession` 双向长连接，删盘时通过 `1.5s` 问答探测确认客户端死活**：
   - **客户端主动连向服务端的双向流**：在 `StorageService` 中新增 `WatchSession(stream WatchSessionRequest) returns (stream WatchSessionResponse)` 双向流接口。客户端在 `bowl mount` 挂载期间主动向服务端建立并保持这条长连接（因为是由客户端向外发起连接，不受 NAT、Docker 容器或防火墙阻拦），平时不发轮询消息；一旦客户端进程被 `kill -9` 强杀或容器被销毁，客户端操作系统内核会立刻发送 TCP `FIN/RST` 断开连接，服务端马上就能感知到并自动清理掉这个会话。
   - **删除磁盘时的两步保护与 1.5 秒问答探测**：
     1. **本机挂载检查**：执行 `bowl disk delete <disk-id>` 时，命令行先检查本机的 `/proc/self/mountinfo`，如果发现这块盘正挂载在本机上，直接报错拒绝并提示用户先执行 `bowl umount`；如果没有加 `-y / --yes` 参数，还会在终端询问用户确认。
     2. **服务端向未过期会话发 `Ping` 确认死活**：服务端执行 `StorageService.DeleteDisk` 时，先清掉已经过期的会话；如果发现这块盘名下还有没过期的 `client_sessions`，服务端就通过这些会话保持的 `WatchSession` 长连接发一个 `Ping(ping_seq)` 消息，最多等 `1.5s` 看对方回不回 `Pong(pong_seq)`：
        - 只要有任何一个会话在 `1.5s` 内回了 `Pong`（说明客户端还活着、正在挂载中），服务端立刻拒绝删盘并返回 `FailedPrecondition`（错误原因为 `EBUSY`）；
        - 如果某个会话没有建立 `WatchSession` 连接，或者超过 `1.5s` 都没回 `Pong`（说明客户端机器已经断电或死机），服务端就当场清掉这个死会话；等所有未过期会话都确认已死并清理干净后，服务端在数据库事务里加锁再次确认没有新挂载进来，然后把这块 `Disk` 和它下面的所有 `Inode`、`Chunk` 一并删除。

## 结果
- **正面效果**：用一个 `bowl` 命令行工具串起了「创建磁盘 → 查看列表/详情 → 挂载 → 在线扩缩容 → 卸载 → 安全删除」的完整流程；通过客户端主动发起的 `WatchSession` 长连接和删盘时的 `Ping-Pong` 问答，不需要客户端暴露任何端口，就同时做到了：正在挂载时防误删（返回 `EBUSY`）、正常 `umount` 后立刻可删、进程被 `kill -9` 后毫秒级感知清理、机器断电后 1.5 秒内自动识别死会话并完成删盘。
- **代价与取舍**：`StorageService` 需要在内存里记录当前连着的 `WatchSession` 连接以便在删盘时发送 `Ping` 探测；同时原来 [ADR-0015](0015-unify-disk-and-storage-service-with-transactional-quota.md) 里“不管有没有客户端挂载都强制删盘”的旧测试需要相应改为测试“有活跃挂载时拦截报错（`EBUSY`）、死机挂载自动清理后允许删盘”。
