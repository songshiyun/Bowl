# Bowl `pjdfstest` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `chown` | 80.185 |
| 2 | `rename` | 23.858 |
| 3 | `unlink` | 18.800 |
| 4 | `chmod` | 13.558 |
| 5 | `link` | 11.654 |
| 6 | `open` | 4.597 |
| 7 | `mknod` | 4.186 |
| 8 | `ftruncate` | 3.031 |
| 9 | `truncate` | 3.005 |
| 10 | `rmdir` | 2.153 |
| 11 | `mkfifo` | 2.048 |
| 12 | `mkdir` | 2.045 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `chmod` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `13.792 s` (`13792.0 ms`) vs FUSE 累计活跃 `206.18 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`206.18 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `96.76%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,323 | 206.18 | 0.062 | 0.003 | 0.693 | 100.0% | 错误返回 128 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 189 | 46.46 | 0.246 | 0.219 | 0.512 | 22.5% | 占 FUSE 22.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 72 | 31.31 | 0.435 | 0.397 | 1.024 | 15.2% | 占 FUSE 15.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 61 | 28.96 | 0.475 | 0.418 | 1.024 | 14.0% | 占 FUSE 14.1% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 549 | 176.25 | 0.321 | 0.291 | 2.455 | 85.5% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 549 | 149.98 | 0.273 | 0.246 | 1.963 | 72.7% | Readlink:0.205ms, SetAttr:0.346ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 549 | 49.91 | 0.091 | 0.084 | 2.065 | 24.2% | HTTP Body+Codec 差值均值 0.043 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 549 | 26.27 | 0.048 | 0.045 | 0.492 | 12.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 69 | 4.84 | 0.070 | 0.065 | 0.235 | 2.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 61 | 4.80 | 0.079 | 0.077 | 0.242 | 2.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 284 | 0.49 | 0.002 | 0.003 | 0.008 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 281 | 1.30 | 0.005 | 0.004 | 0.046 | 0.6% | readlink(189), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 357 | 0.63 | 0.002 | 0.003 | 0.012 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.16 | 0.005 | 0.004 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 284 | 3.04 | 0.011 | 0.009 | 0.046 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 284 | 0.71 | 0.003 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 284 | 1.73 | 0.006 | 0.005 | 0.024 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.03 | 0.011 | 0.018 | 0.025 | 0.0% | — |

### 工作负载 `chown` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `80.849 s` (`80849.0 ms`) vs FUSE 累计活跃 `629.74 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`629.74 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `98.94%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 9,165 | 629.74 | 0.069 | 0.005 | 0.815 | 100.0% | 错误返回 599 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 559 | 190.89 | 0.341 | 0.315 | 0.983 | 30.3% | 占 FUSE 30.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 398 | 154.37 | 0.388 | 0.382 | 1.014 | 24.5% | 占 FUSE 24.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 164 | 79.28 | 0.483 | 0.416 | 1.536 | 12.6% | 占 FUSE 12.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,422 | 527.60 | 0.371 | 0.348 | 1.540 | 83.8% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,422 | 446.30 | 0.314 | 0.296 | 1.200 | 70.9% | Readlink:0.278ms, SetAttr:0.339ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,422 | 147.16 | 0.103 | 0.096 | 0.680 | 23.4% | HTTP Body+Codec 差值均值 0.046 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,422 | 81.31 | 0.057 | 0.052 | 0.340 | 12.9% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 342 | 24.84 | 0.073 | 0.068 | 0.245 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 559 | 18.67 | 0.033 | 0.026 | 0.216 | 3.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 782 | 1.48 | 0.002 | 0.003 | 0.008 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 654 | 3.96 | 0.006 | 0.005 | 0.049 | 0.6% | readlink(559), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 736 | 1.87 | 0.003 | 0.003 | 0.022 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.18 | 0.006 | 0.006 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 782 | 10.56 | 0.014 | 0.010 | 0.049 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 782 | 2.35 | 0.003 | 0.003 | 0.010 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 782 | 5.55 | 0.007 | 0.006 | 0.024 | 0.9% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.30 | 0.297 | 0.250 | 0.299 | 0.0% | — |

### 工作负载 `ftruncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.263 s` (`3263.0 ms`) vs FUSE 累计活跃 `96.18 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`96.18 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.13%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 2,046 | 96.18 | 0.047 | 0.002 | 0.512 | 100.0% | 错误返回 75 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 17.58 | 0.220 | 0.204 | 1.024 | 18.3% | 占 FUSE 18.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 12.58 | 0.331 | 0.326 | 1.024 | 13.1% | 占 FUSE 13.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 10.88 | 0.286 | 0.234 | 1.024 | 11.3% | 占 FUSE 11.3% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 283 | 82.32 | 0.291 | 0.267 | 0.995 | 85.6% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 283 | 69.46 | 0.245 | 0.227 | 0.746 | 72.2% | Readlink:0.184ms, Mkdir:0.252ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 283 | 23.32 | 0.082 | 0.082 | 0.462 | 24.2% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 283 | 12.86 | 0.045 | 0.040 | 0.423 | 13.4% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 2.02 | 0.053 | 0.048 | 0.231 | 2.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.88 | 0.049 | 0.046 | 0.099 | 2.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 125 | 0.20 | 0.002 | 0.003 | 0.007 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 1.30 | 0.007 | 0.006 | 0.204 | 1.3% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.47 | 0.002 | 0.003 | 0.019 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.26 | 0.008 | 0.008 | 0.042 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 125 | 1.41 | 0.011 | 0.009 | 0.047 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 125 | 0.27 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 125 | 0.66 | 0.005 | 0.005 | 0.021 | 0.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.03 | 0.017 | 0.018 | 0.025 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

### 工作负载 `link` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `11.890 s` (`11890.0 ms`) vs FUSE 累计活跃 `185.62 ms`（等效并发度 `0.02x`）；占比基准：`FUSE 活跃总耗时` (`185.62 ms`)
- **缓存与流水线命中**：NSCache Attr `99.19%` / Entry `97.01%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,594 | 185.62 | 0.052 | 0.003 | 0.589 | 100.0% | 错误返回 167 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 160 | 32.81 | 0.205 | 0.198 | 0.494 | 17.7% | 占 FUSE 17.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 65 | 30.63 | 0.471 | 0.412 | 1.024 | 16.5% | 占 FUSE 16.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 987 | 20.39 | 0.021 | 0.002 | 0.443 | 11.0% | 占 FUSE 11.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 507 | 156.51 | 0.309 | 0.286 | 0.982 | 84.3% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 507 | 133.10 | 0.263 | 0.243 | 0.849 | 71.7% | Readlink:0.172ms, Unlink:0.344ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 507 | 42.89 | 0.085 | 0.084 | 0.489 | 23.1% | HTTP Body+Codec 差值均值 0.039 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 507 | 23.41 | 0.046 | 0.042 | 0.243 | 12.6% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 65 | 5.21 | 0.080 | 0.074 | 0.241 | 2.8% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 160 | 2.96 | 0.019 | 0.014 | 0.048 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 244 | 0.43 | 0.002 | 0.003 | 0.009 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 284 | 1.40 | 0.005 | 0.005 | 0.042 | 0.8% | readlink(160), readdirplus(31), lookup(51) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 396 | 0.56 | 0.001 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.18 | 0.006 | 0.005 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 244 | 3.24 | 0.013 | 0.012 | 0.049 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 244 | 0.58 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 244 | 1.46 | 0.006 | 0.006 | 0.023 | 0.8% | — |

### 工作负载 `open` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `4.834 s` (`4834.0 ms`) vs FUSE 累计活跃 `149.70 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`149.70 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `95.97%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,026 | 149.70 | 0.049 | 0.003 | 0.506 | 100.0% | 错误返回 97 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 60 | 26.52 | 0.442 | 0.398 | 1.024 | 17.7% | 占 FUSE 17.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 38 | 17.63 | 0.464 | 0.413 | 1.024 | 11.8% | 占 FUSE 11.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 16.42 | 0.205 | 0.198 | 0.512 | 11.0% | 占 FUSE 11.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 396 | 124.38 | 0.314 | 0.296 | 0.995 | 83.1% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 396 | 105.19 | 0.266 | 0.250 | 0.846 | 70.3% | SetAttr:0.339ms, Readlink:0.173ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 396 | 34.22 | 0.086 | 0.085 | 0.497 | 22.9% | HTTP Body+Codec 差值均值 0.038 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 396 | 19.18 | 0.048 | 0.046 | 0.497 | 12.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 60 | 3.74 | 0.062 | 0.064 | 0.220 | 2.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 38 | 3.12 | 0.082 | 0.081 | 0.242 | 2.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 223 | 0.35 | 0.002 | 0.003 | 0.006 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 203 | 1.29 | 0.006 | 0.007 | 0.249 | 0.9% | readdirplus(33), lookup(29), readlink(80) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 268 | 0.44 | 0.002 | 0.003 | 0.012 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 33 | 0.27 | 0.008 | 0.008 | 0.024 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 223 | 2.67 | 0.012 | 0.009 | 0.046 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 223 | 0.48 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 223 | 1.40 | 0.006 | 0.005 | 0.024 | 0.9% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.05 | 0.016 | 0.018 | 0.025 | 0.0% | — |

### 工作负载 `rename` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `24.320 s` (`24320.0 ms`) vs FUSE 累计活跃 `1788.68 ms`（等效并发度 `0.07x`）；占比基准：`FUSE 活跃总耗时` (`1788.68 ms`)
- **缓存与流水线命中**：NSCache Attr `96.97%` / Entry `99.75%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 33,517 | 1788.68 | 0.053 | 0.004 | 0.508 | 100.0% | 错误返回 2653 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 11,907 | 404.35 | 0.034 | 0.003 | 0.486 | 22.6% | 占 FUSE 22.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rename` | 836 | 362.98 | 0.434 | 0.393 | 0.964 | 20.3% | 占 FUSE 20.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 713 | 296.56 | 0.416 | 0.389 | 0.891 | 16.6% | 占 FUSE 16.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,868 | 1467.30 | 0.379 | 0.368 | 0.989 | 82.0% | 非 OK 状态 105 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,868 | 1219.94 | 0.315 | 0.304 | 0.774 | 68.2% | Lookup:0.322ms, Rename:0.309ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,868 | 411.06 | 0.106 | 0.121 | 0.395 | 23.0% | HTTP Body+Codec 差值均值 0.042 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,868 | 247.36 | 0.064 | 0.064 | 0.243 | 13.8% | 非 OK 状态 105 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rename` | 836 | 68.89 | 0.082 | 0.079 | 0.242 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 713 | 44.26 | 0.062 | 0.069 | 0.116 | 2.5% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,682 | 4.31 | 0.002 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,214 | 8.75 | 0.007 | 0.007 | 0.042 | 0.5% | lookup(868), getxattr(98), readlink(161) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3,051 | 4.62 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.23 | 0.007 | 0.006 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,682 | 55.90 | 0.021 | 0.018 | 0.085 | 3.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,682 | 6.45 | 0.002 | 0.003 | 0.006 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,675 | 19.63 | 0.007 | 0.007 | 0.024 | 1.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.37 | 0.368 | 0.350 | 0.399 | 0.0% | — |

### 工作负载 `suite` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `chmod`, `chown`, `ftruncate`, `link`, `open`, `rename`, `truncate`, `unlink`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `177.720 s` (`177720.0 ms`) vs FUSE 累计活跃 `3937.04 ms`（等效并发度 `0.02x`）；占比基准：`FUSE 活跃总耗时` (`3937.04 ms`)
- **缓存与流水线命中**：NSCache Attr `98.46%` / Entry `98.26%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.02 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 72,217 | 3937.04 | 0.055 | 0.003 | 0.561 | 100.0% | 错误返回 4444 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 1,479 | 612.34 | 0.414 | 0.389 | 0.967 | 15.6% | 占 FUSE 15.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 21,985 | 546.68 | 0.025 | 0.003 | 0.463 | 13.9% | 占 FUSE 13.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 1,792 | 456.74 | 0.255 | 0.218 | 0.869 | 11.6% | 占 FUSE 11.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 9,468 | 3281.45 | 0.347 | 0.326 | 2.365 | 83.3% | 非 OK 状态 126 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 9,468 | 2753.99 | 0.291 | 0.272 | 1.887 | 70.0% | SetAttr:0.326ms, Readlink:0.211ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 9,468 | 917.49 | 0.097 | 0.100 | 0.955 | 23.3% | HTTP Body+Codec 差值均值 0.041 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 9,468 | 527.46 | 0.056 | 0.053 | 0.495 | 13.4% | 非 OK 状态 126 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 1,420 | 93.13 | 0.066 | 0.068 | 0.226 | 2.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 857 | 69.53 | 0.081 | 0.078 | 0.242 | 1.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 5,629 | 9.30 | 0.002 | 0.003 | 0.007 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,116 | 25.27 | 0.006 | 0.006 | 0.247 | 0.6% | lookup(1244), readlink(1792), readdirplus(411) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,883 | 11.33 | 0.002 | 0.003 | 0.010 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 411 | 2.99 | 0.007 | 0.007 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 5,629 | 95.31 | 0.017 | 0.014 | 0.075 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 5,629 | 13.65 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 5,615 | 38.98 | 0.007 | 0.007 | 0.024 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 11 | 0.17 | 0.016 | 0.018 | 0.047 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 2 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 0.94 | 0.314 | 0.275 | 0.397 | 0.0% | — |

### 工作负载 `truncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.239 s` (`3239.0 ms`) vs FUSE 累计活跃 `89.00 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`89.00 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.06%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 1,986 | 89.00 | 0.045 | 0.002 | 0.501 | 100.0% | 错误返回 71 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 17.82 | 0.223 | 0.204 | 0.512 | 20.0% | 占 FUSE 20.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 12.41 | 0.327 | 0.310 | 1.024 | 13.9% | 占 FUSE 13.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 10.70 | 0.282 | 0.250 | 1.024 | 12.0% | 占 FUSE 12.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 276 | 76.07 | 0.276 | 0.246 | 0.973 | 85.5% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 276 | 64.62 | 0.234 | 0.208 | 0.814 | 72.6% | Readlink:0.185ms, Mkdir:0.249ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 276 | 20.88 | 0.076 | 0.075 | 0.467 | 23.5% | HTTP Body+Codec 差值均值 0.035 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 276 | 11.45 | 0.041 | 0.039 | 0.249 | 12.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 1.99 | 0.052 | 0.048 | 0.231 | 2.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.98 | 0.052 | 0.050 | 0.221 | 2.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 118 | 0.19 | 0.002 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 0.96 | 0.005 | 0.005 | 0.046 | 1.1% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.41 | 0.002 | 0.003 | 0.010 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.26 | 0.008 | 0.008 | 0.025 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 118 | 1.51 | 0.013 | 0.012 | 0.049 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 118 | 0.24 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 118 | 0.72 | 0.006 | 0.006 | 0.024 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.04 | 0.021 | 0.025 | 0.049 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

### 工作负载 `unlink` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `19.033 s` (`19033.0 ms`) vs FUSE 累计活跃 `218.70 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`218.70 ms`)
- **缓存与流水线命中**：NSCache Attr `99.75%` / Entry `97.20%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,780 | 218.70 | 0.058 | 0.003 | 0.739 | 100.0% | 错误返回 217 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 81 | 43.88 | 0.542 | 0.447 | 2.048 | 20.1% | 占 FUSE 20.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 92 | 38.79 | 0.422 | 0.390 | 1.024 | 17.7% | 占 FUSE 17.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mknod` | 44 | 21.56 | 0.490 | 0.427 | 2.048 | 9.9% | 占 FUSE 9.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 524 | 183.28 | 0.350 | 0.320 | 1.840 | 83.8% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 524 | 153.55 | 0.293 | 0.266 | 1.623 | 70.2% | Unlink:0.392ms, SetAttr:0.324ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 524 | 51.80 | 0.099 | 0.102 | 0.492 | 23.7% | HTTP Body+Codec 差值均值 0.042 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 524 | 29.73 | 0.057 | 0.053 | 0.245 | 13.6% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 81 | 8.04 | 0.099 | 0.085 | 0.245 | 3.7% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 92 | 5.35 | 0.058 | 0.061 | 0.112 | 2.4% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 351 | 0.57 | 0.002 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 191 | 1.13 | 0.006 | 0.006 | 0.050 | 0.5% | readdirplus(31), lookup(35), readlink(80) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 285 | 0.43 | 0.002 | 0.003 | 0.009 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.25 | 0.008 | 0.008 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 351 | 5.37 | 0.015 | 0.013 | 0.087 | 2.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 351 | 0.79 | 0.002 | 0.003 | 0.009 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 351 | 2.27 | 0.006 | 0.006 | 0.024 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1 | 0.01 | 0.015 | 0.018 | 0.025 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`72,229` 次（错误返回 `4,445` 次），FUSE 累计活跃 `3939.08 ms`，读 `0.00 MiB` / 写 `0.02 MiB`
- **NamespaceCache 命中率**：Attr `98.46%` · Entry `98.26%` · Readlink `0.00%` · 负缓存命中 `3,801` · 失效 `8,314`
- **WriteBehind 流水线**：聚批写 `0.00 MiB` (`0` 次) · 直通写 `0.02 MiB` (`9` 次) · Flush RPC `0` (错误 `0`) · 背压等待 `0` 次 (`0.00 ms`) · 屏障等待 `0` 次 (`0.00 ms`)
- **ReadAhead 预读**：触发 `0` 次 · 预读 `0.00 MiB` · 命中 `0.00 MiB` · 丢弃 `0.00 MiB` · 等待 `0` 次 (`0.00 ms`)
- **Inode 锁与内核队列**：快速获取 `1,537` · 竞争获取 `0`（累计等待 `0.00 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `SetAttr` | 1,479 | 612.34 | 15.6% | 414.0 | 389.0 | 687.4 | 967.1 | — |
| `Lookup` | 21,988 | 547.43 | 13.9% | 24.9 | 3.1 | 191.3 | 463.0 | ENAMETOOLONG:18, ENOENT:3348 |
| `Readlink` | 1,792 | 456.74 | 11.6% | 254.9 | 217.8 | 492.6 | 869.4 | — |
| `Unlink` | 857 | 404.07 | 10.3% | 471.5 | 410.2 | 888.7 | 1013.0 | — |
| `Rename` | 836 | 362.98 | 9.2% | 434.2 | 392.7 | 632.5 | 963.8 | ENOTEMPTY:7 |
| `Mknod` | 682 | 319.45 | 8.1% | 468.4 | 408.6 | 875.7 | 1009.6 | — |
| `Mkdir` | 644 | 238.97 | 6.1% | 371.1 | 365.2 | 823.1 | 991.6 | — |
| `GetAttr` | 35,309 | 216.63 | 5.5% | 6.1 | 3.2 | 17.5 | 31.3 | — |
| `Rmdir` | 640 | 211.25 | 5.4% | 330.1 | 309.5 | 614.4 | 947.2 | ENOTEMPTY:7 |
| `Create` | 261 | 122.23 | 3.1% | 468.3 | 413.4 | 893.3 | 1013.1 | — |
| `Symlink` | 241 | 118.80 | 3.0% | 493.0 | 416.5 | 904.2 | 1013.1 | — |
| `ReadDirPlus` | 822 | 118.02 | 3.0% | 143.6 | 32.0 | 475.7 | 509.9 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `9,473` 次 · 正常 (`OK`) `9,346` 次 · 业务/协议非 OK `127` 次 (`1.34%`，分布 `{"FAILED_PRECONDITION": 14, "NOT_FOUND": 113}`)
- **客户端 RPC 状态码拆分**：总调用 `9,472` 次 · 正常 (`OK`) `9,345` 次 · 非 OK `127` 次 (`1.34%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:SetAttr` | 1,420 | — | 158.36 | 0.112 | 0.105 | 0.243 | 0.413 |
| `http_outer:Unlink` | 857 | — | 111.71 | 0.130 | 0.148 | 0.291 | 0.462 |
| `http_outer:Rename` | 836 | — | 103.16 | 0.123 | 0.155 | 0.242 | 0.250 |
| `http_outer:Readlink` | 1,792 | — | 96.24 | 0.054 | 0.045 | 0.133 | 0.235 |
| `http_outer:Lookup` | 1,245 | — | 93.66 | 0.075 | 0.075 | 0.188 | 0.238 |
| `http_outer:Mknod` | 682 | — | 85.59 | 0.126 | 0.149 | 0.242 | 0.292 |
| `http_outer:Mkdir` | 644 | — | 67.07 | 0.104 | 0.091 | 0.234 | 0.249 |
| `http_outer:Rmdir` | 640 | — | 61.35 | 0.096 | 0.087 | 0.233 | 0.307 |
| `http_outer:Symlink` | 241 | — | 32.60 | 0.135 | 0.159 | 0.287 | 0.473 |
| `http_outer:Create` | 261 | — | 31.93 | 0.122 | 0.145 | 0.241 | 0.249 |
| `http_outer:ReadDirPlus` | 411 | — | 30.30 | 0.074 | 0.073 | 0.191 | 0.243 |
| `http_outer:StatFS` | 277 | — | 23.86 | 0.086 | 0.083 | 0.223 | 0.246 |
| `http_outer:GetXattr` | 112 | — | 13.47 | 0.120 | 0.152 | 0.245 | 0.407 |
| `http_outer:Link` | 38 | — | 5.75 | 0.151 | 0.160 | 0.432 | 0.486 |
| `http_outer:CreateDisk` | 1 | — | 4.84 | 4.840 | 3.750 | 4.875 | 4.975 |
| `http_outer:Write` | 9 | — | 1.94 | 0.215 | 0.184 | 0.775 | 0.955 |
| `http_outer:Read` | 2 | — | 0.56 | 0.279 | 0.250 | 0.475 | 0.495 |
| `http_outer:MountDisk` | 1 | — | 0.51 | 0.508 | 0.750 | 0.975 | 0.995 |
| `http_outer:ReclaimOrphan` | 2 | — | 0.25 | 0.125 | 0.175 | 0.242 | 0.249 |
| `http_outer:GetAttr` | 2 | — | 0.24 | 0.118 | 0.100 | 0.235 | 0.247 |
| `rpc:SetAttr` | 1,420 | 1,420 / 0 | 93.13 | 0.066 | 0.068 | 0.116 | 0.226 |
| `rpc:Unlink` | 857 | 857 / 0 | 69.53 | 0.081 | 0.078 | 0.205 | 0.242 |
| `rpc:Rename` | 836 | 829 / 7 | 68.89 | 0.082 | 0.079 | 0.206 | 0.242 |
| `rpc:Mknod` | 682 | 682 / 0 | 53.00 | 0.078 | 0.075 | 0.198 | 0.241 |
| `rpc:Lookup` | 1,245 | 1,244 / 1 | 44.89 | 0.036 | 0.036 | 0.081 | 0.097 |
| `rpc:Readlink` | 1,792 | 1,792 / 0 | 43.34 | 0.024 | 0.018 | 0.058 | 0.142 |
| `rpc:Mkdir` | 644 | 644 / 0 | 39.26 | 0.061 | 0.054 | 0.186 | 0.239 |
| `rpc:Rmdir` | 640 | 633 / 7 | 38.62 | 0.060 | 0.059 | 0.131 | 0.229 |
| `rpc:Symlink` | 241 | 241 / 0 | 19.73 | 0.082 | 0.078 | 0.211 | 0.245 |
| `rpc:Create` | 261 | 261 / 0 | 19.11 | 0.073 | 0.074 | 0.200 | 0.240 |
| `rpc:ReadDirPlus` | 411 | 411 / 0 | 16.02 | 0.039 | 0.037 | 0.088 | 0.157 |
| `rpc:StatFS` | 277 | 277 / 0 | 10.93 | 0.039 | 0.039 | 0.089 | 0.099 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:phase_a_validate` | 5,631 | 95.36 | 0.017 | 0.014 | 0.046 | 0.075 |
| `actor_phase:wal_apply_nosync` | 5,616 | 38.99 | 0.007 | 0.007 | 0.019 | 0.024 |
| `actor_phase:phase_b_mutate` | 5,631 | 13.66 | 0.002 | 0.003 | 0.005 | 0.009 |
| `actor_phase:wal_apply_sync` | 1 | 0.17 | 0.165 | 0.175 | 0.242 | 0.249 |
| `read_op:lookup` | 1,245 | 9.02 | 0.007 | 0.007 | 0.022 | 0.025 |
| `read_op:readlink` | 1,792 | 7.09 | 0.004 | 0.003 | 0.013 | 0.023 |
| `read_op:readdirplus` | 411 | 6.04 | 0.015 | 0.014 | 0.036 | 0.050 |
| `read_op:getattr` | 279 | 1.73 | 0.006 | 0.005 | 0.018 | 0.024 |
| `read_op:getxattr` | 112 | 1.18 | 0.011 | 0.009 | 0.024 | 0.043 |
| `read_op:read` | 2 | 0.18 | 0.091 | 0.050 | 0.235 | 0.247 |
| `read_op:statfs` | 278 | 0.04 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:point_get` | 6,886 | 11.34 | 0.002 | 0.003 | 0.005 | 0.010 |
| `kv_read:prefix_scan` | 411 | 2.99 | 0.007 | 0.007 | 0.022 | 0.024 |
| `control_open_actor:ok` | 1 | 2.58 | 2.576 | 3.750 | 4.875 | 4.975 |
| `actor_queue_wait` | 5,631 | 9.31 | 0.002 | 0.003 | 0.005 | 0.007 |
| `blob_pwrite` | 11 | 0.17 | 0.016 | 0.018 | 0.036 | 0.047 |
| `blob_rmw_read` | 2 | 0.00 | 0.001 | 0.003 | 0.005 | 0.005 |
| `pebble_wal_fsync` | 5 | 1.14 | 0.228 | 0.225 | 0.375 | 0.395 |

- **Pebble 状态**：BlockCache 命中率 `N/A (0 reads)` · ReadAmp `0.0` · WriteStall `0.0` · WAL 写入 `2028460.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 24712.0, "falloc_zero": 0.0, "full_block": 0.0, "rmw": 4.0, "zero_elide": 0.0}` · 读源块计数 `{"disk_pack": 0.0, "hot_cache": 4.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`297` 条（跨 Client+Server 全链路 `88` 条，异常链路 `13` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `0e9ba3ed20830f31` | 0.98 | `bowl-client:fuse.StatFs` | 0.93 | 0.15 | 0.78 | `bowl-client:storage.v1.StorageService/StatFS` (0.93 ms) |
| `55cf7026f255aaa8` | 0.98 | `bowl-client:fuse.Symlink` | 0.90 | 0.17 | 0.73 | `bowl-client:storage.v1.StorageService/Symlink` (0.90 ms) |
| `6f0d59f959f65c45` | 0.67 | `bowl-client:fuse.Create` | 0.63 | 0.14 | 0.49 | `bowl-client:storage.v1.StorageService/Create` (0.63 ms) |
| `366fee47a4df479a` | 0.61 | `bowl-client:fuse.Mkdir` | 0.55 | 0.12 | 0.43 | `bowl-client:storage.v1.StorageService/Mkdir` (0.55 ms) |
| `bf5764c81053578c` | 0.59 | `bowl-client:fuse.Rmdir` | 0.55 | 0.12 | 0.42 | `bowl-client:storage.v1.StorageService/Rmdir` (0.55 ms) |
| `076765436a6cd78c` | 0.57 | `bowl-client:fuse.Readlink` | 0.53 | 0.11 | 0.41 | `bowl-client:storage.v1.StorageService/Readlink` (0.53 ms) |
| `6a3f9335467f3db3` | 0.56 | `bowl-client:fuse.Link` | 0.53 | 0.10 | 0.43 | `bowl-client:storage.v1.StorageService/Link` (0.53 ms) |
| `d1d63e156c9897c4` | 0.56 | `bowl-client:fuse.GetXAttr` | 0.52 | 0.06 | 0.46 | `bowl-client:storage.v1.StorageService/GetXattr` (0.52 ms) |
| `e35b92f417ed8b8d` | 0.56 | `bowl-client:fuse.Rename` | 0.52 | 0.12 | 0.40 | `bowl-client:storage.v1.StorageService/Rename` (0.52 ms) |
| `5b4662a12e8f2529` | 0.55 | `bowl-client:fuse.Mkdir` | 0.52 | 0.10 | 0.42 | `bowl-client:storage.v1.StorageService/Mkdir` (0.52 ms) |
| `46cdf348d21803e5` | 0.55 | `bowl-client:fuse.StatFs` | 0.51 | 0.07 | 0.44 | `bowl-client:storage.v1.StorageService/StatFS` (0.51 ms) |
| `324f7468fe10e96c` | 0.55 | `bowl-client:fuse.Rmdir` | 0.49 | 0.10 | 0.40 | `bowl-client:storage.v1.StorageService/Rmdir` (0.49 ms) |
| `dfc3bb18c859a4dc` | 0.53 | `bowl-client:fuse.Rename` | 0.49 | 0.11 | 0.38 | `bowl-client:storage.v1.StorageService/Rename` (0.49 ms) |
| `f570a0aa43843a04` | 0.52 | `bowl-client:fuse.GetXAttr` | 0.47 | 0.07 | 0.39 | `bowl-client:storage.v1.StorageService/GetXattr` (0.47 ms) |
| `4c2b812089e603a1` | 0.52 | `bowl-client:fuse.Mknod` | 0.47 | 0.10 | 0.37 | `bowl-client:storage.v1.StorageService/Mknod` (0.47 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`20` · **服务端 Warn/Error 数**：`18` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 4}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `chown_cpu.pb.gz` | `10ms  5.88%  5.88%       10ms  5.88%  bytes.NewBuffer` | `0     0%     0%      100ms 58.82%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_chown_cpu.txt` |
| `server` | `chown_heap.pb.gz` | `4097.37kB 22.80% 22.80%  4097.37kB 22.80%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%     8.74MB 49.81%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_chown_heap.txt` |
| `server` | `chown_mutex.pb.gz` | `—` | `—` | `pprof_top/server_chown_mutex.txt` |
| `server` | `rename_cpu.pb.gz` | `20ms  9.09%  9.09%       20ms  9.09%  runtime.futex` | `0     0%     0%       80ms 36.36%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_rename_cpu.txt` |
| `server` | `rename_heap.pb.gz` | `4097.37kB 17.34% 17.34%  4097.37kB 17.34%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%     9.91MB 42.96%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_rename_heap.txt` |
| `server` | `rename_mutex.pb.gz` | `—` | `—` | `pprof_top/server_rename_mutex.txt` |
| `client` | `chown_cpu.pb.gz` | `30ms 10.34% 10.34%       30ms 10.34%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      160ms 55.17%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_chown_cpu.txt` |
| `client` | `chown_heap.pb.gz` | `7143.82kB 47.54% 47.54%  7143.82kB 47.54%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%     6.98MB 47.54%  github.com/hanwen/go-fuse/v2/fuse.(*fuseFD).readRequest` | `pprof_top/client_chown_heap.txt` |
| `client` | `chown_mutex.pb.gz` | `—` | `—` | `pprof_top/client_chown_mutex.txt` |
| `client` | `rename_cpu.pb.gz` | `50ms 16.67% 16.67%       50ms 16.67%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      170ms 56.67%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_rename_cpu.txt` |
| `client` | `rename_heap.pb.gz` | `5953.18kB 36.39% 36.39%  5953.18kB 36.39%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%    10.82MB 67.69%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_rename_heap.txt` |
| `client` | `rename_mutex.pb.gz` | `—` | `—` | `pprof_top/client_rename_mutex.txt` |
