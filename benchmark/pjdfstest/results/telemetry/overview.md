# Bowl `pjdfstest` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `chown` | 79.868 |
| 2 | `rename` | 23.058 |
| 3 | `unlink` | 18.674 |
| 4 | `chmod` | 13.456 |
| 5 | `link` | 11.541 |
| 6 | `open` | 4.508 |
| 7 | `mknod` | 4.107 |
| 8 | `ftruncate` | 2.951 |
| 9 | `truncate` | 2.942 |
| 10 | `rmdir` | 2.073 |
| 11 | `mkfifo` | 1.989 |
| 12 | `mkdir` | 1.986 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `chmod` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `13.679 s` (`13679.0 ms`) vs FUSE 累计活跃 `183.50 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`183.50 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `96.76%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,320 | 183.50 | 0.055 | 0.002 | 0.509 | 100.0% | 错误返回 125 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 189 | 41.91 | 0.222 | 0.207 | 0.505 | 22.8% | 占 FUSE 22.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 72 | 27.88 | 0.387 | 0.394 | 1.024 | 15.2% | 占 FUSE 15.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 61 | 26.35 | 0.432 | 0.412 | 1.024 | 14.4% | 占 FUSE 14.4% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 549 | 158.29 | 0.288 | 0.277 | 4.925 | 86.3% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 549 | 135.28 | 0.246 | 0.237 | 4.433 | 73.7% | Readlink:0.184ms, SetAttr:0.305ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 549 | 43.24 | 0.079 | 0.072 | 0.985 | 23.6% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 549 | 23.01 | 0.042 | 0.040 | 0.492 | 12.5% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 69 | 4.34 | 0.063 | 0.064 | 0.224 | 2.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 61 | 4.30 | 0.071 | 0.068 | 0.237 | 2.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 284 | 0.48 | 0.002 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 281 | 1.18 | 0.004 | 0.004 | 0.025 | 0.6% | readlink(189), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 357 | 0.55 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.14 | 0.004 | 0.003 | 0.020 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 284 | 2.78 | 0.010 | 0.008 | 0.043 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 284 | 0.61 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 284 | 1.43 | 0.005 | 0.004 | 0.021 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.03 | 0.009 | 0.009 | 0.025 | 0.0% | — |

### 工作负载 `chown` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `80.526 s` (`80526.0 ms`) vs FUSE 累计活跃 `556.62 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`556.62 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `98.94%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 9,165 | 556.62 | 0.061 | 0.004 | 0.697 | 100.0% | 错误返回 599 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 559 | 173.92 | 0.311 | 0.300 | 0.964 | 31.2% | 占 FUSE 31.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 398 | 142.12 | 0.357 | 0.373 | 0.980 | 25.5% | 占 FUSE 25.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 164 | 68.87 | 0.420 | 0.406 | 1.003 | 12.4% | 占 FUSE 12.4% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,422 | 474.06 | 0.333 | 0.334 | 0.991 | 85.2% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,422 | 401.87 | 0.283 | 0.287 | 0.834 | 72.2% | Readlink:0.254ms, SetAttr:0.314ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,422 | 130.78 | 0.092 | 0.085 | 0.620 | 23.5% | HTTP Body+Codec 差值均值 0.041 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,422 | 72.19 | 0.051 | 0.047 | 0.620 | 13.0% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 342 | 22.56 | 0.066 | 0.064 | 0.234 | 4.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 559 | 16.87 | 0.030 | 0.024 | 0.110 | 3.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 782 | 1.33 | 0.002 | 0.003 | 0.007 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 654 | 3.58 | 0.005 | 0.004 | 0.048 | 0.6% | readlink(559), readdirplus(31), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 736 | 1.71 | 0.002 | 0.003 | 0.015 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.17 | 0.005 | 0.005 | 0.023 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 782 | 9.59 | 0.012 | 0.010 | 0.048 | 1.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 782 | 1.96 | 0.003 | 0.003 | 0.010 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 782 | 4.89 | 0.006 | 0.005 | 0.022 | 0.9% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.22 | 0.223 | 0.250 | 0.299 | 0.0% | — |

### 工作负载 `ftruncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.176 s` (`3176.0 ms`) vs FUSE 累计活跃 `84.88 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`84.88 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.13%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 2,045 | 84.88 | 0.042 | 0.002 | 0.484 | 100.0% | 错误返回 74 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 16.40 | 0.205 | 0.196 | 0.512 | 19.3% | 占 FUSE 19.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 11.28 | 0.297 | 0.244 | 1.024 | 13.3% | 占 FUSE 13.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 9.86 | 0.259 | 0.222 | 1.024 | 11.6% | 占 FUSE 11.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 283 | 73.39 | 0.259 | 0.245 | 2.485 | 86.5% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 283 | 62.56 | 0.221 | 0.209 | 2.236 | 73.7% | Readlink:0.171ms, Mkdir:0.226ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 283 | 20.06 | 0.071 | 0.067 | 0.497 | 23.6% | HTTP Body+Codec 差值均值 0.033 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 283 | 10.83 | 0.038 | 0.037 | 0.249 | 12.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 1.80 | 0.047 | 0.040 | 0.231 | 2.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.77 | 0.047 | 0.044 | 0.099 | 2.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 125 | 0.18 | 0.001 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 0.88 | 0.005 | 0.005 | 0.042 | 1.0% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.38 | 0.001 | 0.003 | 0.010 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.19 | 0.006 | 0.005 | 0.024 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 125 | 1.27 | 0.010 | 0.009 | 0.042 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 125 | 0.24 | 0.002 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 125 | 0.61 | 0.005 | 0.004 | 0.016 | 0.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.03 | 0.016 | 0.018 | 0.025 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

### 工作负载 `link` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `11.764 s` (`11764.0 ms`) vs FUSE 累计活跃 `172.14 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`172.14 ms`)
- **缓存与流水线命中**：NSCache Attr `99.19%` / Entry `97.01%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,594 | 172.14 | 0.048 | 0.002 | 0.540 | 100.0% | 错误返回 167 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 160 | 31.93 | 0.200 | 0.197 | 0.489 | 18.5% | 占 FUSE 18.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 65 | 29.53 | 0.454 | 0.425 | 2.048 | 17.2% | 占 FUSE 17.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 987 | 18.71 | 0.019 | 0.002 | 0.431 | 10.9% | 占 FUSE 10.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 507 | 147.77 | 0.291 | 0.272 | 1.525 | 85.8% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 507 | 125.50 | 0.248 | 0.231 | 1.283 | 72.9% | Readlink:0.167ms, Unlink:0.335ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 507 | 41.03 | 0.081 | 0.076 | 0.489 | 23.8% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 507 | 22.26 | 0.044 | 0.041 | 0.243 | 12.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 65 | 5.01 | 0.077 | 0.074 | 0.242 | 2.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Readlink` | 160 | 2.87 | 0.018 | 0.014 | 0.049 | 1.7% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 244 | 0.44 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 284 | 1.34 | 0.005 | 0.005 | 0.042 | 0.8% | readlink(160), lookup(51), readdirplus(31) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 396 | 0.57 | 0.001 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.18 | 0.006 | 0.005 | 0.023 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 244 | 3.07 | 0.013 | 0.011 | 0.048 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 244 | 0.51 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 244 | 1.44 | 0.006 | 0.006 | 0.023 | 0.8% | — |

### 工作负载 `open` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `4.742 s` (`4742.0 ms`) vs FUSE 累计活跃 `141.83 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`141.83 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `95.97%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,023 | 141.83 | 0.047 | 0.002 | 0.501 | 100.0% | 错误返回 94 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 60 | 24.55 | 0.409 | 0.391 | 1.024 | 17.3% | 占 FUSE 17.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 38 | 16.23 | 0.427 | 0.395 | 1.024 | 11.4% | 占 FUSE 11.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 15.94 | 0.199 | 0.198 | 0.512 | 11.2% | 占 FUSE 11.2% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 396 | 121.14 | 0.306 | 0.295 | 4.975 | 85.4% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 396 | 100.31 | 0.253 | 0.244 | 2.490 | 70.7% | SetAttr:0.313ms, Readlink:0.167ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 396 | 35.88 | 0.091 | 0.089 | 2.485 | 25.3% | HTTP Body+Codec 差值均值 0.038 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 396 | 20.83 | 0.053 | 0.051 | 2.485 | 14.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 60 | 3.42 | 0.057 | 0.055 | 0.160 | 2.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 38 | 2.93 | 0.077 | 0.076 | 0.239 | 2.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 223 | 0.35 | 0.002 | 0.003 | 0.005 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 203 | 1.18 | 0.006 | 0.006 | 0.100 | 0.8% | readdirplus(33), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 268 | 0.49 | 0.002 | 0.003 | 0.020 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 33 | 0.27 | 0.008 | 0.008 | 0.024 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 223 | 2.69 | 0.012 | 0.010 | 0.045 | 1.9% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 223 | 0.47 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 223 | 1.19 | 0.005 | 0.005 | 0.021 | 0.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 3 | 0.04 | 0.015 | 0.018 | 0.025 | 0.0% | — |

### 工作负载 `rename` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `23.520 s` (`23520.0 ms`) vs FUSE 累计活跃 `1583.43 ms`（等效并发度 `0.07x`）；占比基准：`FUSE 活跃总耗时` (`1583.43 ms`)
- **缓存与流水线命中**：NSCache Attr `96.97%` / Entry `99.75%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 33,517 | 1583.43 | 0.047 | 0.003 | 0.496 | 100.0% | 错误返回 2653 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 11,907 | 355.49 | 0.030 | 0.003 | 0.480 | 22.5% | 占 FUSE 22.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rename` | 836 | 325.96 | 0.390 | 0.388 | 0.913 | 20.6% | 占 FUSE 20.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 713 | 268.85 | 0.377 | 0.386 | 0.725 | 17.0% | 占 FUSE 17.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,868 | 1319.55 | 0.341 | 0.364 | 1.090 | 83.3% | 非 OK 状态 105 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,868 | 1085.79 | 0.281 | 0.302 | 0.856 | 68.6% | Lookup:0.285ms, Rename:0.272ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,868 | 382.42 | 0.099 | 0.108 | 0.377 | 24.2% | HTTP Body+Codec 差值均值 0.039 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,868 | 233.76 | 0.060 | 0.061 | 0.243 | 14.8% | 非 OK 状态 105 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rename` | 836 | 65.78 | 0.079 | 0.077 | 0.241 | 4.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 713 | 41.98 | 0.059 | 0.064 | 0.100 | 2.7% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,682 | 4.02 | 0.001 | 0.003 | 0.006 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,214 | 8.74 | 0.007 | 0.007 | 0.046 | 0.6% | lookup(868), getxattr(98), readlink(161) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3,051 | 4.74 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.22 | 0.007 | 0.007 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,682 | 54.92 | 0.020 | 0.018 | 0.077 | 3.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,682 | 5.81 | 0.002 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,675 | 16.62 | 0.006 | 0.006 | 0.019 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.40 | 0.402 | 0.450 | 0.499 | 0.0% | — |

### 工作负载 `suite` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `chmod`, `chown`, `ftruncate`, `link`, `open`, `rename`, `truncate`, `unlink`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `175.546 s` (`175546.0 ms`) vs FUSE 累计活跃 `3513.55 ms`（等效并发度 `0.02x`）；占比基准：`FUSE 活跃总耗时` (`3513.55 ms`)
- **缓存与流水线命中**：NSCache Attr `98.46%` / Entry `98.26%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.02 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 72,208 | 3513.55 | 0.049 | 0.003 | 0.501 | 100.0% | 错误返回 4435 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 1,479 | 556.70 | 0.376 | 0.384 | 0.914 | 15.8% | 占 FUSE 15.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 21,985 | 475.55 | 0.022 | 0.003 | 0.453 | 13.5% | 占 FUSE 13.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 1,792 | 412.16 | 0.230 | 0.212 | 0.783 | 11.7% | 占 FUSE 11.7% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 9,468 | 2977.02 | 0.314 | 0.315 | 4.950 | 84.7% | 非 OK 状态 126 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 9,468 | 2480.73 | 0.262 | 0.265 | 4.297 | 70.6% | SetAttr:0.296ms, Readlink:0.191ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 9,468 | 853.54 | 0.090 | 0.091 | 2.470 | 24.3% | HTTP Body+Codec 差值均值 0.038 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 9,468 | 496.29 | 0.052 | 0.051 | 2.470 | 14.1% | 非 OK 状态 126 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 1,420 | 86.64 | 0.061 | 0.063 | 0.208 | 2.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rename` | 836 | 65.78 | 0.079 | 0.077 | 0.241 | 1.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 5,629 | 8.81 | 0.002 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,116 | 24.65 | 0.006 | 0.006 | 0.099 | 0.7% | lookup(1244), readlink(1792), readdirplus(411) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,883 | 11.30 | 0.002 | 0.003 | 0.010 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 411 | 3.11 | 0.008 | 0.007 | 0.033 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 5,629 | 92.03 | 0.016 | 0.014 | 0.062 | 2.6% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 5,629 | 12.16 | 0.002 | 0.003 | 0.008 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 5,615 | 34.25 | 0.006 | 0.006 | 0.021 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 11 | 0.16 | 0.015 | 0.017 | 0.047 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 2 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 0.92 | 0.308 | 0.275 | 0.497 | 0.0% | — |

### 工作负载 `truncate` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.170 s` (`3170.0 ms`) vs FUSE 累计活跃 `80.97 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`80.97 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `94.06%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.01 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 1,985 | 80.97 | 0.041 | 0.002 | 0.501 | 100.0% | 错误返回 70 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Readlink` | 80 | 13.79 | 0.172 | 0.194 | 0.512 | 17.0% | 占 FUSE 17.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 38 | 12.91 | 0.340 | 0.274 | 1.024 | 15.9% | 占 FUSE 15.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 38 | 9.91 | 0.261 | 0.229 | 0.512 | 12.2% | 占 FUSE 12.2% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 276 | 71.14 | 0.258 | 0.241 | 0.982 | 87.9% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 276 | 59.09 | 0.214 | 0.201 | 0.753 | 73.0% | Readlink:0.146ms, Mkdir:0.251ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 276 | 21.80 | 0.079 | 0.075 | 0.810 | 26.9% | HTTP Body+Codec 差值均值 0.035 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 276 | 12.05 | 0.044 | 0.040 | 0.249 | 14.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 38 | 2.53 | 0.067 | 0.059 | 0.240 | 3.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 38 | 1.90 | 0.050 | 0.053 | 0.099 | 2.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 118 | 0.20 | 0.002 | 0.003 | 0.008 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 176 | 0.96 | 0.005 | 0.006 | 0.046 | 1.2% | readdirplus(31), readlink(80), lookup(29) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 258 | 0.41 | 0.002 | 0.003 | 0.012 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.29 | 0.009 | 0.008 | 0.042 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 118 | 1.79 | 0.015 | 0.015 | 0.070 | 2.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 118 | 0.26 | 0.002 | 0.003 | 0.010 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 118 | 0.73 | 0.006 | 0.006 | 0.024 | 0.9% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 2 | 0.05 | 0.024 | 0.025 | 0.049 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 1 | 0.00 | 0.001 | 0.003 | 0.005 | 0.0% | — |

### 工作负载 `unlink` 全链路逐跳拆解（子阶段 · 隶属 `suite`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `19.117 s` (`19117.0 ms`) vs FUSE 累计活跃 `189.95 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时` (`189.95 ms`)
- **缓存与流水线命中**：NSCache Attr `99.75%` / Entry `97.20%` / Readlink `0.00%` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,779 | 189.95 | 0.050 | 0.003 | 0.510 | 100.0% | 错误返回 216 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 81 | 39.76 | 0.491 | 0.431 | 2.048 | 20.9% | 占 FUSE 20.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.SetAttr` | 92 | 34.11 | 0.371 | 0.384 | 0.512 | 18.0% | 占 FUSE 18.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mknod` | 44 | 18.14 | 0.412 | 0.400 | 1.024 | 9.5% | 占 FUSE 9.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 524 | 161.90 | 0.309 | 0.307 | 1.285 | 85.2% | 非 OK 状态 7 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 524 | 134.08 | 0.256 | 0.258 | 1.041 | 70.6% | Unlink:0.357ms, SetAttr:0.282ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 524 | 47.29 | 0.090 | 0.091 | 0.484 | 24.9% | HTTP Body+Codec 差值均值 0.037 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 524 | 27.82 | 0.053 | 0.049 | 0.244 | 14.6% | 非 OK 状态 7 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Unlink` | 81 | 7.47 | 0.092 | 0.082 | 0.244 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:SetAttr` | 92 | 4.91 | 0.053 | 0.049 | 0.112 | 2.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 351 | 0.57 | 0.002 | 0.003 | 0.009 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 191 | 1.10 | 0.006 | 0.006 | 0.050 | 0.6% | readdirplus(31), lookup(35), readlink(80) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 285 | 0.45 | 0.002 | 0.003 | 0.014 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 31 | 0.25 | 0.008 | 0.008 | 0.024 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 351 | 5.24 | 0.015 | 0.013 | 0.081 | 2.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 351 | 0.72 | 0.002 | 0.003 | 0.008 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 351 | 1.98 | 0.006 | 0.005 | 0.023 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1 | 0.01 | 0.011 | 0.018 | 0.025 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`72,220` 次（错误返回 `4,436` 次），FUSE 累计活跃 `3515.15 ms`，读 `0.00 MiB` / 写 `0.02 MiB`
- **NamespaceCache 命中率**：Attr `98.46%` · Entry `98.26%` · Readlink `0.00%` · 负缓存命中 `3,801` · 失效 `8,314`
- **WriteBehind 流水线**：聚批写 `0.00 MiB` (`0` 次) · 直通写 `0.02 MiB` (`9` 次) · Flush RPC `0` (错误 `0`) · 背压等待 `0` 次 (`0.00 ms`) · 屏障等待 `0` 次 (`0.00 ms`)
- **ReadAhead 预读**：触发 `0` 次 · 预读 `0.00 MiB` · 命中 `0.00 MiB` · 丢弃 `0.00 MiB` · 等待 `0` 次 (`0.00 ms`)
- **Inode 锁与内核队列**：快速获取 `1,537` · 竞争获取 `0`（累计等待 `0.00 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `SetAttr` | 1,479 | 556.70 | 15.8% | 376.4 | 384.1 | 510.5 | 913.7 | — |
| `Lookup` | 21,988 | 476.44 | 13.6% | 21.7 | 2.5 | 182.8 | 453.7 | ENAMETOOLONG:18, ENOENT:3348 |
| `Readlink` | 1,792 | 412.16 | 11.7% | 230.0 | 212.1 | 476.9 | 783.1 | — |
| `Unlink` | 857 | 366.90 | 10.4% | 428.1 | 400.5 | 811.6 | 996.8 | — |
| `Rename` | 836 | 325.96 | 9.3% | 389.9 | 388.0 | 507.2 | 912.7 | ENOTEMPTY:7 |
| `Mknod` | 682 | 286.84 | 8.2% | 420.6 | 393.5 | 659.9 | 978.5 | — |
| `Mkdir` | 644 | 216.94 | 6.2% | 336.9 | 318.8 | 706.6 | 972.8 | — |
| `Rmdir` | 640 | 193.29 | 5.5% | 302.0 | 269.7 | 502.3 | 889.3 | ENOTEMPTY:7 |
| `GetAttr` | 35,309 | 155.99 | 4.4% | 4.4 | 2.2 | 15.1 | 27.6 | — |
| `ReadDirPlus` | 822 | 110.20 | 3.1% | 134.1 | 32.0 | 456.2 | 510.4 | — |
| `Create` | 261 | 109.49 | 3.1% | 419.5 | 393.4 | 632.5 | 963.8 | — |
| `StatFs` | 277 | 107.87 | 3.1% | 389.4 | 389.2 | 707.0 | 975.2 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `9,472` 次 · 正常 (`OK`) `9,345` 次 · 业务/协议非 OK `127` 次 (`1.34%`，分布 `{"FAILED_PRECONDITION": 14, "NOT_FOUND": 113}`)
- **客户端 RPC 状态码拆分**：总调用 `9,471` 次 · 正常 (`OK`) `9,344` 次 · 非 OK `127` 次 (`1.34%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:SetAttr` | 1,420 | — | 146.30 | 0.103 | 0.092 | 0.237 | 0.352 |
| `http_outer:Unlink` | 857 | — | 101.58 | 0.119 | 0.124 | 0.246 | 0.429 |
| `http_outer:Rename` | 836 | — | 96.83 | 0.116 | 0.136 | 0.240 | 0.249 |
| `http_outer:Readlink` | 1,792 | — | 88.36 | 0.049 | 0.043 | 0.110 | 0.223 |
| `http_outer:Lookup` | 1,245 | — | 86.03 | 0.069 | 0.072 | 0.165 | 0.235 |
| `http_outer:Mknod` | 682 | — | 77.90 | 0.114 | 0.129 | 0.240 | 0.250 |
| `http_outer:Mkdir` | 644 | — | 63.21 | 0.098 | 0.088 | 0.238 | 0.395 |
| `http_outer:Rmdir` | 640 | — | 56.99 | 0.089 | 0.082 | 0.225 | 0.249 |
| `http_outer:Create` | 261 | — | 30.24 | 0.116 | 0.127 | 0.239 | 0.249 |
| `http_outer:Symlink` | 241 | — | 30.22 | 0.125 | 0.145 | 0.242 | 0.299 |
| `http_outer:ReadDirPlus` | 411 | — | 29.15 | 0.071 | 0.071 | 0.183 | 0.250 |
| `http_outer:StatFS` | 277 | — | 23.97 | 0.087 | 0.083 | 0.222 | 0.244 |
| `http_outer:GetXattr` | 112 | — | 13.22 | 0.118 | 0.152 | 0.240 | 0.248 |
| `http_outer:CreateDisk` | 1 | — | 7.96 | 7.955 | 7.500 | 9.750 | 9.950 |
| `http_outer:Link` | 38 | — | 5.27 | 0.139 | 0.139 | 0.405 | 0.481 |
| `http_outer:Write` | 9 | — | 2.26 | 0.251 | 0.196 | 0.775 | 0.955 |
| `http_outer:Read` | 2 | — | 2.12 | 1.063 | 0.100 | 2.350 | 2.470 |
| `http_outer:MountDisk` | 1 | — | 0.47 | 0.467 | 0.375 | 0.487 | 0.497 |
| `http_outer:ReclaimOrphan` | 2 | — | 0.20 | 0.102 | 0.100 | 0.235 | 0.247 |
| `http_outer:GetAttr` | 1 | — | 0.13 | 0.131 | 0.175 | 0.242 | 0.249 |
| `rpc:SetAttr` | 1,420 | 1,420 / 0 | 86.64 | 0.061 | 0.063 | 0.099 | 0.208 |
| `rpc:Rename` | 836 | 829 / 7 | 65.78 | 0.079 | 0.077 | 0.184 | 0.241 |
| `rpc:Unlink` | 857 | 857 / 0 | 63.84 | 0.074 | 0.074 | 0.189 | 0.238 |
| `rpc:Mknod` | 682 | 682 / 0 | 49.10 | 0.072 | 0.072 | 0.183 | 0.238 |
| `rpc:Lookup` | 1,245 | 1,244 / 1 | 42.43 | 0.034 | 0.035 | 0.077 | 0.096 |
| `rpc:Readlink` | 1,792 | 1,792 / 0 | 39.63 | 0.022 | 0.017 | 0.050 | 0.092 |
| `rpc:Mkdir` | 644 | 644 / 0 | 36.59 | 0.057 | 0.049 | 0.151 | 0.230 |
| `rpc:Rmdir` | 640 | 633 / 7 | 36.49 | 0.057 | 0.053 | 0.099 | 0.218 |
| `rpc:Symlink` | 241 | 241 / 0 | 18.56 | 0.077 | 0.076 | 0.206 | 0.241 |
| `rpc:Create` | 261 | 261 / 0 | 18.32 | 0.070 | 0.072 | 0.180 | 0.236 |
| `rpc:ReadDirPlus` | 411 | 411 / 0 | 15.32 | 0.037 | 0.036 | 0.086 | 0.100 |
| `rpc:StatFS` | 277 | 277 / 0 | 11.10 | 0.040 | 0.039 | 0.091 | 0.112 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:phase_a_validate` | 5,631 | 92.08 | 0.016 | 0.014 | 0.045 | 0.062 |
| `actor_phase:wal_apply_nosync` | 5,616 | 34.26 | 0.006 | 0.006 | 0.010 | 0.021 |
| `actor_phase:phase_b_mutate` | 5,631 | 12.17 | 0.002 | 0.003 | 0.005 | 0.008 |
| `actor_phase:wal_apply_sync` | 1 | 0.15 | 0.152 | 0.175 | 0.242 | 0.249 |
| `read_op:lookup` | 1,245 | 9.00 | 0.007 | 0.007 | 0.021 | 0.024 |
| `read_op:readlink` | 1,792 | 6.52 | 0.004 | 0.003 | 0.010 | 0.022 |
| `read_op:readdirplus` | 411 | 5.99 | 0.015 | 0.013 | 0.038 | 0.050 |
| `read_op:getattr` | 278 | 1.87 | 0.007 | 0.006 | 0.021 | 0.025 |
| `read_op:getxattr` | 112 | 1.16 | 0.010 | 0.009 | 0.024 | 0.025 |
| `read_op:read` | 2 | 0.08 | 0.040 | 0.050 | 0.095 | 0.099 |
| `read_op:statfs` | 278 | 0.04 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:point_get` | 6,885 | 11.31 | 0.002 | 0.003 | 0.005 | 0.010 |
| `kv_read:prefix_scan` | 411 | 3.11 | 0.008 | 0.007 | 0.022 | 0.033 |
| `control_open_actor:ok` | 1 | 3.65 | 3.647 | 3.750 | 4.875 | 4.975 |
| `actor_queue_wait` | 5,631 | 8.82 | 0.002 | 0.003 | 0.005 | 0.007 |
| `blob_pwrite` | 11 | 0.16 | 0.015 | 0.017 | 0.036 | 0.047 |
| `blob_rmw_read` | 2 | 0.00 | 0.001 | 0.003 | 0.005 | 0.005 |
| `pebble_wal_fsync` | 5 | 1.12 | 0.224 | 0.225 | 0.475 | 0.495 |

- **Pebble 状态**：BlockCache 命中率 `N/A (0 reads)` · ReadAmp `0.0` · WriteStall `0.0` · WAL 写入 `2028451.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 24712.0, "falloc_zero": 0.0, "full_block": 0.0, "rmw": 4.0, "zero_elide": 0.0}` · 读源块计数 `{"disk_pack": 0.0, "hot_cache": 4.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`319` 条（跨 Client+Server 全链路 `108` 条，异常链路 `8` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `d01448d3047d1bac` | 1.01 | `bowl-client:fuse.Readlink` | 0.94 | 0.13 | 0.81 | `bowl-client:storage.v1.StorageService/Readlink` (0.94 ms) |
| `cd91c57e0a033b29` | 0.72 | `bowl-client:fuse.Unlink` | 0.67 | 0.22 | 0.45 | `bowl-client:storage.v1.StorageService/Unlink` (0.67 ms) |
| `e4ef0e45499339f0` | 0.67 | `bowl-client:fuse.StatFs` | 0.63 | 0.11 | 0.52 | `bowl-client:storage.v1.StorageService/StatFS` (0.63 ms) |
| `f0db8de01301fc3d` | 0.66 | `bowl-client:fuse.Mkdir` | 0.62 | 0.16 | 0.47 | `bowl-client:storage.v1.StorageService/Mkdir` (0.62 ms) |
| `6ea05ae633b21197` | 0.66 | `bowl-client:fuse.Mkdir` | 0.62 | 0.15 | 0.47 | `bowl-client:storage.v1.StorageService/Mkdir` (0.62 ms) |
| `a7ea79cf0de3944e` | 0.52 | `bowl-client:fuse.GetXAttr` | 0.49 | 0.09 | 0.40 | `bowl-client:storage.v1.StorageService/GetXattr` (0.49 ms) |
| `bda90815e0bda7ed` | 0.52 | `bowl-client:fuse.SetAttr` | 0.49 | 0.11 | 0.37 | `bowl-client:storage.v1.StorageService/SetAttr` (0.49 ms) |
| `3aa1b4799c5760f4` | 0.50 | `bowl-client:fuse.GetXAttr` | 0.46 | 0.08 | 0.38 | `bowl-client:storage.v1.StorageService/GetXattr` (0.46 ms) |
| `3ea9d362f4326605` | 0.50 | `bowl-client:fuse.Rmdir` | 0.46 | 0.10 | 0.36 | `bowl-client:storage.v1.StorageService/Rmdir` (0.46 ms) |
| `5d9f6e0a8c0d6898` | 0.50 | `bowl-client:fuse.Lookup` | 0.42 | 0.05 | 0.38 | `bowl-client:storage.v1.StorageService/Lookup` (0.42 ms) |
| `89a620c217f1f4e2` | 0.49 | `bowl-client:fuse.SetAttr` | 0.45 | 0.09 | 0.36 | `bowl-client:storage.v1.StorageService/SetAttr` (0.45 ms) |
| `6354d02f952e38cc` | 0.49 | `bowl-client:fuse.Rmdir` | 0.45 | 0.11 | 0.34 | `bowl-client:storage.v1.StorageService/Rmdir` (0.45 ms) |
| `65f2d8096e9f986c` | 0.48 | `bowl-client:fuse.SetAttr` | 0.43 | 0.09 | 0.35 | `bowl-client:storage.v1.StorageService/SetAttr` (0.43 ms) |
| `f4ce78114fe5a2b0` | 0.47 | `bowl-client:fuse.GetXAttr` | 0.44 | 0.08 | 0.36 | `bowl-client:storage.v1.StorageService/GetXattr` (0.44 ms) |
| `631676534db50812` | 0.47 | `bowl-client:fuse.Readlink` | 0.43 | 0.07 | 0.36 | `bowl-client:storage.v1.StorageService/Readlink` (0.43 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`20` · **服务端 Warn/Error 数**：`14` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 4}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `chown_cpu.pb.gz` | `10ms  5.88%  5.88%       10ms  5.88%  connectrpc.com/connect.checkServerStreamsCanFlush` | `0     0%     0%       80ms 47.06%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_chown_cpu.txt` |
| `server` | `chown_heap.pb.gz` | `4097.37kB 20.88% 20.88%  4097.37kB 20.88%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%     8.82MB 46.01%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_chown_heap.txt` |
| `server` | `chown_mutex.pb.gz` | `—` | `—` | `pprof_top/server_chown_mutex.txt` |
| `server` | `rename_cpu.pb.gz` | `20ms  8.33%  8.33%       20ms  8.33%  runtime.memclrNoHeapPointers` | `0     0%     0%      100ms 41.67%  net/http.HandlerFunc.ServeHTTP` | `pprof_top/server_rename_cpu.txt` |
| `server` | `rename_heap.pb.gz` | `4097.37kB 18.15% 18.15%  4097.37kB 18.15%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    10.55MB 47.88%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_rename_heap.txt` |
| `server` | `rename_mutex.pb.gz` | `—` | `—` | `pprof_top/server_rename_mutex.txt` |
| `client` | `chown_cpu.pb.gz` | `30ms 10.71% 10.71%       30ms 10.71%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      120ms 42.86%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_chown_cpu.txt` |
| `client` | `chown_heap.pb.gz` | `4762.55kB 43.25% 43.25%  4762.55kB 43.25%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%  4762.55kB 43.25%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_chown_heap.txt` |
| `client` | `chown_mutex.pb.gz` | `—` | `—` | `pprof_top/client_chown_mutex.txt` |
| `client` | `rename_cpu.pb.gz` | `20ms  6.25%  6.25%       50ms 15.62%  runtime.mallocgcSmallScanNoHeader` | `0     0%     0%      160ms 50.00%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_rename_cpu.txt` |
| `client` | `rename_heap.pb.gz` | `7143.82kB 67.32% 67.32%  7143.82kB 67.32%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%  8168.23kB 76.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_rename_heap.txt` |
| `client` | `rename_mutex.pb.gz` | `—` | `—` | `pprof_top/client_rename_mutex.txt` |
