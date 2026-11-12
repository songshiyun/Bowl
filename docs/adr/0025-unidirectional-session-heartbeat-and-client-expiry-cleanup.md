# 0025. 客户端单向心跳保活、移除随路续约与断网超期即時清理自愈

## 状态
已被 [ADR-0032](0032-single-disk-control-plane-and-stateless-epoch-fencing.md) 替代 (superseded by ADR-0032) — `ClientSession` 15s 单向心跳与过期自动清理已全面收缩为单次 `MountDisk` 握手与无会话 `Epoch` 世代号栅栏

## 背景
在 [ADR-0012](0012-fuse-file-locks-and-session-lease-semantics.md)、[ADR-0020](0020-linux-fuse-client-passthrough-and-session-lifecycle.md)、[ADR-0021](0021-disk-cli-session-liveness-and-safe-delete.md) 与 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 的演进过程中，`ClientSession` 的保活与探活机制叠加了四层复杂逻辑：
1. **双向探活流与流代数门控开销大**：为了在 60 秒长租约下支持删盘秒级感知死机客户端，[ADR-0021](0021-disk-cli-session-liveness-and-safe-delete.md) 引入了 `WatchSession` 双向流与 `SessionWatchHub`（含流代数追踪、断流自动调 `CloseSession`、删盘 1.5s `Ping-Pong` 问答及并发重连提交门控），状态机分支繁琐。
2. **读写与打开请求随路续约耦合度高**：除 `RenewSession` 外，`Create`、`TmpFile`、`Open`、`OpenDir`、`Read`、`Write`、`Flush`、`Release`、`ReleaseDir`、`SetAttr`、`Fallocate`、`SetLk`、`SetLkw` 均在业务路径中顺带写库延长会话租约，为此又不得不引入 [ADR-0023](0023-session-touch-throttling-lockless-handle-close-and-read-pipeline.md) 的 `5s` 内存节流缓存（`writeSessionTouches`）来缓解 `client_sessions` 行锁争用。
3. **客户端断网超期后被动等待网络恢复才清理旧状态**：客户端断网超过租约有效期后，在连上服务端收到 `session_recreated == true` 之前，本地一直保留着已失效的 `currentSessionID`、`handleSessions`、待关闭句柄重试队列和预读缓存，没有在宽限期耗尽当下立即清理并进入新会话重建状态。

## 决策
1. **改为纯客户端单向心跳，服务端租约缩短为 `15s`（演进 ADR-0012 / ADR-0020 / ADR-0021）**：
   - 将服务端默认会话租约时长 `DefaultSessionLeaseTTL` 从 `60s` 缩短为 `15s`。
   - **彻底移除 `WatchSession` 双向流**：从 Protobuf（`storage.proto`）、服务端（`SessionWatchHub`、`WatchSession` 处理器）及客户端（`watchLoop`）移除全部双向流代码。
   - **`DeleteDisk` 防误删简化**：服务端 `DeleteDisk` 先清理已过期的 `client_sessions`；只要目标 `Disk` 下仍存在未过期的 `client_sessions`（`expires_at > clock_timestamp()`），直接返回 `FailedPrecondition`（`EBUSY`）。正常 `bowl umount` 仍通过 `CloseSession` 立即注销会话，断电或 `kill -9` 死机最多等待 `15s` 租约自然到期即可删盘。
   - **客户端心跳间隔与退避重试**：客户端正常心跳周期按 `LeaseTTL / 3`（`15s` 下为 `5s`）计算；单次 `RenewSession` RPC 超时设为 `min(3s, 剩余租约时间)`。心跳失败后以初始 `200ms`、指数翻倍、上限 `2s`（且单次等待不跨过租约截止时间 `expiresAt`）在剩余宽限期内退避重试。

2. **仅保留 `RenewSession` 作为唯一续约入口，移除所有业务接口的随路续约与 `5s` 节流（演进 ADR-0012 / ADR-0013 / ADR-0023）**：
   - 彻底删除 `renewOptionalClientSession`、`touchActiveClientSession*`、`touchWriteClientSessionForHandle` 以及 `5s` 内存节流状态 `writeSessionTouches`。
   - **业务接口只做只读会话存活校验**：
     - 所有携带 `fh` 的接口（`Read`、`Write`、`Flush`、`Fsync`、`Release`、`ReadDir`、`ReadDirPlus`、`FsyncDir`、`ReleaseDir`、`Fallocate`、`GetLk`、`SetLk`、`SetLkw` 及带 `fh` 的 `SetAttr`）直接依赖 `GetFileHandle` / `DeleteFileHandle` SQL 内联的 `s.expires_at > clock_timestamp() AND fh.created_at >= s.created_at` 校验，无需额外查询 `client_sessions`；会话不存在或已超期直接返回 `ErrBadFileHandle`（`EBADF`）。
     - 创建新句柄的接口（`Create`、`TmpFile`、`Open`、`OpenDir`）及未传 `fh` 但传入非空 `session_id` 的 `SetAttr`，仅通过只读查询确认该 `session_id` 在 `client_sessions` 中存在且 `expires_at > clock_timestamp()`；若不存在或已超期，直接返回 `ErrBadFileHandle`（`EBADF`），绝不隐式创建或延长会话。
   - **过期会话与孤儿节点清理时机**：统一在 `RenewSession`、`CloseSession`、`DeleteDisk` 以及服务端后台周期运行的 `CollectGarbage`（GC Worker）中执行 `DeleteExpiredClientSessions` 与 `reclaimUnreferencedOrphans`。

3. **客户端断网超过宽限期立即清理本地状态，并由 `renewLoop` 单协程统一负责新会话重建（演进 ADR-0020）**：
   - **宽限期超期当场清理**：客户端以当前租约截止时间 `expiresAt`（`15s`）作为断网宽限期上限。一旦 `time.Now() >= expiresAt` 仍未续约成功（或边界时刻服务端返回 `session_recreated == true`），客户端当场将当前会话置为未激活状态，清空本地 `handleSessions` 句柄表（不再维护 `retiredSessions` 集合，旧句柄因查无绑定直接返回 `EBADF`），并通知 `RawFS` 清空属于该旧会话的 `releaseQueue`（后台 `Release` 重试队列）与 `readAheadHandles`（预读缓存）。
   - **单协程重建与按需唤醒等待**：清理旧会话后，客户端生成新的随机 `session_id`，所有 `RenewSession` RPC 严格收敛在后台 `renewLoop` 单协程中按退避策略重试注册。当 `Create` / `Open` / `OpenDir` 获取 `session_id` 发现当前无已激活会话时，绝不自行发起 `RenewSession` RPC，而是通过非阻塞通道立即唤醒 `renewLoop` 跳过剩余退避睡眠立刻重试一次，并阻塞等待新会话激活就绪信号（受请求 Context / 5s 超时与 FUSE `cancel` 控制，超时仍未激活则返回 `EIO`）。

## 结果
- **正面效果**：删去了 `WatchSession` 双向流、`SessionWatchHub`、所有业务接口的随路写库续约及 `5s` 内存节流代码，会话生命周期收敛为纯粹的客户端单向心跳；数据读写与文件打开彻底解耦了 `client_sessions` 表更新；客户端在断网超过 `15s` 宽限期时立即回收本地句柄、重试队列与预读缓存，网络恢复瞬间通过唤醒 `renewLoop` 即可平滑重建新会话。
- **代价与取舍**：客户端进程被 `kill -9` 强杀或机器断电后，需要等待最多 `15s` 租约自然过期后才能执行 `bowl disk delete`（正常 `bowl umount` 仍通过 `CloseSession` 秒级释放）。
