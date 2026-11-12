# 0034. 双端 H2C `1+8` 条带连接隔离、帧合并防 `tcp_autocorking` 死锁、零分配租约编解码与 `.pack` 渐进回写/合并读

## 状态
已采纳 (accepted) — 演进 [ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md) 与 [ADR-0031](0031-shardstore-append-only-pack-storage-and-256kib-block-grid.md) 的网络传输、Protobuf 编解码与 `BlobStore` 物理 I/O 调度机制。

## 背景
在本地 NVMe SSD 云端基准实测（`pjdfstest`、`juicefs`、`drive9`）与全栈遥测归因中，单连接标准 HTTP/2 传输、标准 Protobuf 编解码与同步 `.pack` 落盘/回收暴露了四项底层系统瓶颈：
1. **单 HTTP/2 连接队头阻塞与流控受限**：Go `net/http` 单条 HTTP/2 连接共享单一 `readLoop`/`writeSched` 与连接级流控窗口；`4 MiB` `Read`/`Write` `DATA` 帧会直接阻塞同连接上的 `Lookup`/`Create`/`Fsync` 元数据小包，且 16 路并发顺序读受限于单连接接收窗口。
2. **Go H2 双次 `Flush` 触发 Linux `tcp_autocorking` + `Delayed ACK` `~10ms` 伪死锁**：Go `http2ClientConn` 在单次 Unary POST 中对 `HEADERS(END_STREAM=0)` 与紧随其后的 `DATA(END_STREAM=1)` 分别执行两次底层 `Write`；在 Linux 默认 `net.ipv4.tcp_autocorking=1` 与 `virtio-net` 环境下，服务端收到 `HEADERS` 后等待请求体并进入 TCP Delayed ACK，而客户端小 `DATA` 帧被 `tcp_should_autocork` 挂起在 `TSQ_THROTTLED` 等待前包释放，造成确定性的 `~9~10ms` 延迟双峰。
3. **`4 MiB` 读写载荷序列化触发 GB/s 级堆分配与 GC 抖动**：标准 `proto.Marshal`/`Unmarshal` 为每次 `4 MiB` `WriteRequest` 与 `ReadResponse` 重新分配并清零大切片；若简单使用 `sync.Pool` 复用底层数组而无生命周期守卫，拦截器早退或未接管响应切片会导致悬垂引用（Use-After-Free）。
4. **`64 MiB` `.pack` 尾部集中刷盘、同步死包删除与逐块 `pread` 系统调用开销**：大文件写末尾 `Fsync` 集中同步多个 `64 MiB` 脏包会产生数百毫秒尾延迟；Actor 批次收敛后同步 `os.Remove` 全死包会被宿主文件系统 `unlink` 抖动拖慢后续 Tick；`4 MiB` 冷读逐个对 16 个 `256 KiB` 块调 `pread` 产生 10 倍以上冗余系统调用。

## 决策

1. **客户端 `1 + 8` H2C/TLS 条带连接池（`shardedRoundTripper`）与大流控窗口**
   - 双端统一启用 `MaxReadFrameSize = 1 MiB` 与单流接收窗口 `MaxReceiveBufferPerStream = 8 MiB`（单次 `4 MiB` 载荷零 `WINDOW_UPDATE` 停顿），服务端单连接窗口设为 `128 MiB`。
   - 客户端传输层物理隔离为 **1 条专用元数据/控制轻量连接**（连接窗口 `32 MiB`，承载 `Lookup`/`Create`/`Unlink`/`Fsync` 等非大块 RPC）与 **8 条独立大块数据连接**（每连接窗口 `64 MiB`，总计 `512 MiB`，对 `Read`/`Write` 原子轮询打散），彻底根除 `4 MiB` `DATA` 帧对元数据请求的队头阻塞及单连接 `readLoop` 串行瓶颈。

2. **明文 H2C 请求帧合并连接包装器（`h2CoalesceConn` + `TCP_NODELAY`）**
   - 客户端拨号显式开启 `TCP_NODELAY`，并在明文 `h2c` 连接外包覆 `h2CoalesceConn`：识别完整 `HEADERS(END_HEADERS=1, END_STREAM=0, len <= 1024B)` 帧后暂存至栈内缓冲（附 `150µs` 极端流控耗尽兜底定时器），与微秒级紧随到达的 `DATA` 帧合并为单次连续 `Write`（`<= 8 KiB`）或 `writev`（`net.Buffers`，`4 MiB` 大块零拷贝）系统调用下发，根除 `tcp_autocorking` + `Delayed ACK` 跨机 `~10ms` 死锁。

3. **基于 `weak.Pointer` 与 `runtime.AddCleanup` 的双端零分配 Protobuf 二进制编解码**
   - 双端注册定制 `proto` 二进制 `Codec`（基于 `protowire` 手写编解码），维护常驻有界 `*[4 MiB]byte` `noscan` 缓冲通道 + `sync.Pool` 二级缓冲池：
     - **写路径**：客户端 `WriteRequest` 通过 `MarshalAppend` 原地编码避免二次拷贝；服务端解码大于阈值载荷时租借 `4 MiB` 缓冲切片，经最外层拦截器在 Handler 返回后幂等归还，并挂载 `runtime.AddCleanup` 兜底异常泄漏。
     - **读路径**：服务端 `ReadResponse` 直接向池化 `4 MiB` 缓冲填充块数据（仅对稀疏空洞尾段按需 `clear`，消除全量预清零）；客户端解码 `> 256 KiB` 读响应时绑定 `weak.Pointer[ReadResponse]` 租约，由 `TakeReadPayload` 移交原子引用计数包装（`sharedReadBuf`）并在所有窗口与前台读取者释放后归还池（未显式接管时仅由 GC Cleanup 移除弱表项，绝不提前重用底层数组，保障零 UAF）；`<= 256 KiB` 小读直接紧凑克隆，不占用 `4 MiB` 槽位。

4. **`BlobStore` 渐进回写、后台死包回收、满块批量追加与相邻冷块合并 `pread`**
   - **渐进回写与并发屏障**：活跃 `.pack` 每推进满 `8 MiB` 对齐窗口向后台 `wbLoop` 投递 `sync_file_range` 异步回写提示；写满 `64 MiB` 切包封口时唤醒后台 `bgSyncLoop` 异步执行 `fdatasync`（「先刷盘、后消脏」并继承新建包 `packs/` 目录同步位）；显式 `Fsync` 对剩余脏包按最多 `8` 并发执行 `fdatasync` 并带单飞合并（`packSyncFlight` / `dirSyncFlight`）。
   - **后台异步死包物理回收**：已封口包 `LiveBytes == 0` 且 WAL 持久化屏障完成后，从句柄池摘除并投递至 `BlobStore` 后台 `reclaimLoop` 异步执行 `os.Remove`，将宿主文件系统删文件耗时彻底移出 `StoreActor` 主循环。
   - **满块批量追加与连续冷块合并 `pread`**：对齐 `256 KiB` 的多块 `Write` 通过 `AppendBlocks` 单次加锁、单次 `WriteAt` 连续落入活跃 `.pack`；`StoreActor.Read` 扫描 `DataKey` 后，**优先检查 `hotShard` 内存热缓存**以守护并发尾块扩展与新写块的读一致性，仅对不在热缓存且位于同一 `.pack` 内物理偏移紧邻的连续冷块合并执行单次 `ReadRangeAtContext`（`pread`），将大文件顺序冷读系统调用压降 90% 以上。
