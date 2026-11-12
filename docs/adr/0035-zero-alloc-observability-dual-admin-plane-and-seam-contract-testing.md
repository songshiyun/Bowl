# 0035. 全栈零分配可观测性、双端口/本地管理面、虚拟 `.stats` 节点与契约接缝黑盒测试收敛

## 状态
已采纳 (accepted) — 演进 [ADR-0017](0017-storage-service-fx-bootstrap-and-e2e-boundary.md) 与 [ADR-0018](0018-unify-runtime-and-db-pool-with-aether-shared-pkgs.md) 的运行时可观测性与测试边界规范，并替代 [ADR-0016](0016-formal-state-machine-verification-and-metadata-invariants.md) 的内部白盒内存状态机与 Mock 单测策略。

## 背景
随着客户端异步流水线（[ADR-0033](0033-client-write-behind-multi-window-readahead-and-local-dir-synthesis.md)）与单盘微秒级 `StoreActor` 引擎（[ADR-0030](0030-store-actor-group-commit-and-four-stage-data-pipeline.md)~[ADR-0034](0034-h2c-striped-transport-zero-alloc-codec-and-pack-io-coalescing.md)）落地，可观测性与测试体系面临四个具体矛盾：
1. **默认指标与采样失真、高基数爆炸**：OpenTelemetry 默认直方图首桶高达 `5ms`，完全掩盖微秒级 Pebble/NVMe/H2C 延迟分布；若在引擎热路径指标上挂载 `disk_id` 标签或每次操作动态分配属性切片，会在成百上千虚拟盘挂载时引发时间序列基数爆炸与额外 GC 压力；同时 `0.1%` 基础采样率会漏采低频关键的 `Fsync`/`Umount` 屏障及跨协程异步刷盘链路。
2. **FUSE 客户端黑盒运行态内省缺失与内核 `sendfile` 陷阱**：守护进程化的 `bowl mount` 缺少像服务端一样的实时管理探针与 `juicefs bench` 兼容的挂载内 `.stats` 统计视图；且在 Linux 内核中若虚拟统计文件 `Attr.Size == 0`，`do_sendfile`（如 BusyBox `cat`）会在 `i_size == 0` 处直接短路返回 0 字节导致空输出。
3. **跨进程日志与追踪关联断链**：`Lookup`/`GetXattr` 常规 `ENOENT`/`ENODATA` 未命中若按默认错误级别记录会产生海量日志噪音，而出站重试、异步预读/回写与服务端慢操作（`> 50ms`）需要统一的请求关联 ID 与链路上下文贯通。
4. **白盒 Mock 单测与内部状态机测试高耦合易脆**：仓库历史积累了近两万行针对内部未导出结构、Mock 接口及重复白盒状态机的单元测试，每次引擎内部并发结构优化均引发大面积脆弱断裂，却无法覆盖跨进程 H2C 编解码与真实 Linux FUSE 内核交互。

## 决策

1. **双端隔离管理面、挂载自动发现（`bowl stats`）与挂载根虚拟 `.stats` 节点**
   - **服务端双端口隔离**：公网端口（`:9000`）仅暴露 ConnectRPC 数据/控制服务（外挂全包 HTTP 流量与耗时统计）；内网管理端口（默认 `:9090`）独立暴露 `/metrics`、`/debug/pprof/*`、`/healthz` 与 `/readyz`。
   - **客户端管理面与 `bowl stats`**：`bowl mount` 默认启用本地回环管理面（`127.0.0.1:0`，可通过 `--admin-addr` 指定或传 `off` 关闭），暴露 `/metrics`、`/debug/pprof/*`、`/healthz`、`/readyz` 与多格式 `/debug/stats`，并写入挂载发现元数据以支撑 `bowl stats [mountpoint] [--json|--raw]` 免参自动发现。
   - **挂载根隐藏虚拟 `.stats` / `.jfs.stats`（`--stats-file`）**：启用时在根目录 `Lookup`/`Open`/`Read` 拦截 `.stats` 与 `.jfs.stats`（`ReadDir` 隐藏不可见），固定绑定内核 `VirtualStatsIno = (1<<63) - 1`（避开匿名临时文件高位 `1<<63` 与服务端正整数 `int64` Inode 空间），以只读 `FOPEN_DIRECT_IO` 打开并按句柄快照输出指标；**显式返回非零 `Attr.Size = 64 KiB`**，绕开 Linux 内核 `do_sendfile` 在 `i_size == 0` 时的零字节短路缺陷。

2. **热路径零堆分配埋点、无 `disk_id` 标签约束与 `18` 级亚毫秒直方图桶**
   - **零分配与基数封顶**：客户端 `InstrumentedRawFS` 采用定长无锁原子 `opSlot` 数组覆盖全部 41 个 FUSE 操作与 9 大有状态子系统（含 Linux `/sys/fs/fuse/connections/<dev>` 拥塞采集）；服务端 `StoreActor` 与 `BlobStore` 暴露队列等待、Phase A/B、块写模式、热缓存与落盘耗时，**严禁在高频引擎指标上附加 `disk_id` 标签**，且未采样路径 Span 零堆分配。
   - **亚毫秒直方图与日志降噪**：双端通过 OTel `MeterProvider` View 为 RPC 与引擎耗时注册 `18` 级亚毫秒起跳桶（`25µs ~ 10s`，客户端 FUSE 扩至 `26` 桶覆盖 `~33.55s`）；服务端将 `Lookup`/`GetXattr` 的 `CodeNotFound` 访问日志降级为 `DEBUG`，对数据面 `> 50ms` 慢 RPC 与 `> 500ms` Pebble 慢盘停顿输出结构化告警并独立落盘 `pebble_events.log`。

3. **全量 `X-Request-Id` 透传、`PriorityOpSampler` 确定性提权与 Alloy 尾部保留**
   - 客户端全部 31 个出站 RPC 100% 注入栈缓冲生成的 `X-Request-Id` 与 W3C `traceparent`（未配置 `OTEL_EXPORTER_OTLP_ENDPOINT` 时自动使用零网络开销的 `none` 导出器，并对裸 `host:port` 自动补齐 `http://`）；服务端提取 `X-Request-Id` 注入结构化日志与录制态 Span 属性。
   - 客户端 `PriorityOpSampler` 对低频高价值屏障（`fuse.Fsync`、`fuse.FsyncDir`、`fuse.OnUnmount`）以及携带已采样调用方 `SpanLink` 的异步 `WriteBehind` / `ReadAhead` 根 Span 执行确定性 100% 提权采样；生产编排栈（`deploy/prod`，区别于轻量 `deploy/dev`）通过 Grafana Alloy 对 `> 10ms` 慢链路与 `ERROR` Span 执行尾部采样保留。

4. **裁撤内部脆弱白盒/Mock 单测，收敛至公开契约接缝（替代 ADR-0016）**
   - 移除 `services/storage` 与 `client` 内部依赖白盒实现细节的 Mock 单元测试与内存重复状态机测试，仅保留纯算法/编解码/键格式/紧凑回收边界单测；系统正确性统一由公开契约接缝守门：`tests/contract`（Protobuf/校验器契约）、`e2e/storage_rpc_contract_test.go` 与 `e2e/storage_process_test.go`（真实进程与 PostgreSQL/Pebble RPC 契约）、`client/e2e`（Linux 容器真实 FUSE 挂载验收）以及 `pjdfstest` 8,798 项 POSIX 全量回归。
