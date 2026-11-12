# Bowl `juicefs` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `fio` | 69.594 |
| 2 | `mdtest` | 27.108 |
| 3 | `fio_seq_16jobs` | 21.324 |
| 4 | `mdtest_1p` | 14.871 |
| 5 | `fio_seq_write_1job` | 10.449 |
| 6 | `mdtest_4p` | 10.449 |
| 7 | `fio_direct_randwrite` | 10.442 |
| 8 | `fio_seq_read_1job` | 10.442 |
| 9 | `juicefs_bench` | 5.492 |
| 10 | `fio_direct_seq` | 5.315 |
| 11 | `fio_direct_randread` | 3.228 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `fio_direct_randread` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `3.528 s` (`3528.0 ms`) vs FUSE 累计活跃 `10671.66 ms`（等效并发度 `3.02x`）；占比基准：`FUSE 活跃总耗时` (`10671.66 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `368.00 MiB` / 命中 `44.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 4,118 | 10671.66 | 2.591 | 2.709 | 7.863 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 10671.51 | 2.605 | 2.717 | 7.885 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 4 | 0.07 | 0.016 | 0.016 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Release` | 4 | 0.04 | 0.011 | 0.012 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 20 | 189.56 | 9.478 | 10.000 | 24.700 | 1.8% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 4,258 | 10726.89 | 2.519 | 2.202 | 9.971 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,142 | 10663.75 | 2.575 | 2.225 | 10.207 | 99.4% | 非 OK 状态 41 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,140 | 7925.65 | 1.914 | 1.642 | 7.724 | 73.9% | Read:1.914ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,141 | 6343.49 | 1.532 | 1.517 | 5.308 | 59.1% | HTTP Body+Codec 差值均值 0.871 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,140 | 2738.11 | 0.661 | 0.583 | 2.483 | 25.5% | 非 OK 状态 18 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 4,140 | 2738.11 | 0.661 | 0.583 | 2.483 | 25.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,124 | 2618.23 | 0.635 | 0.576 | 2.472 | 24.4% | read(4124) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4,124 | 98.73 | 0.024 | 0.021 | 0.063 | 0.9% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 4,124 | 30.08 | 0.007 | 0.008 | 0.024 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 4,464 | 2388.18 | 0.535 | 0.453 | 2.440 | 22.3% | — |

### 工作负载 `fio_direct_randwrite` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.740 s` (`10740.0 ms`) vs FUSE 累计活跃 `11174.42 ms`（等效并发度 `1.04x`）；占比基准：`FUSE 活跃总耗时` (`11174.42 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `13.00 MiB` / 直通 `4083.00 MiB` (Flush RPC `12`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 8,214 | 11174.42 | 1.360 | 0.032 | 6.890 | 100.0% | 错误返回 4096 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 11152.80 | 2.723 | 2.885 | 7.557 | 99.8% | 占 FUSE 99.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 4,096 | 21.46 | 0.005 | 0.003 | 0.028 | 0.2% | 占 FUSE 0.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 4 | 0.07 | 0.018 | 0.021 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,099 | 11012.43 | 2.687 | 2.688 | 5.734 | 98.6% | 非 OK 状态 4 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,099 | 6483.22 | 1.582 | 1.716 | 1.610 | 58.0% | Write:1.582ms, GetXattr:0.730ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,099 | 7671.95 | 1.872 | 1.841 | 4.881 | 68.7% | HTTP Body+Codec 差值均值 0.767 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,099 | 4529.21 | 1.105 | 0.972 | 4.124 | 40.5% | 非 OK 状态 4 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,095 | 4528.54 | 1.106 | 0.973 | 4.124 | 40.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:GetXattr` | 4 | 0.67 | 0.168 | 0.175 | 0.249 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,095 | 22.09 | 0.005 | 0.003 | 0.032 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4 | 0.27 | 0.068 | 0.075 | 0.100 | 0.0% | getxattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 8 | 0.24 | 0.030 | 0.025 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,090 | 92.44 | 0.023 | 0.019 | 0.050 | 0.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,090 | 37.35 | 0.009 | 0.007 | 0.024 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,090 | 67.42 | 0.016 | 0.018 | 0.044 | 0.6% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,095 | 2236.39 | 0.546 | 0.432 | 2.295 | 20.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 283.86 | 4.435 | 4.310 | 9.867 | 2.5% | — |

### 工作负载 `fio_direct_seq` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `5.616 s` (`5616.0 ms`) vs FUSE 累计活跃 `13804.08 ms`（等效并发度 `2.46x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`13804.08 ms` / RPC `58806.08 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4088.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 12,377 | 13804.08 | 1.115 | 0.080 | 22.406 | 100.0% | 错误返回 4116 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 8322.61 | 2.032 | 0.111 | 26.526 | 60.3% | 占 FUSE 60.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 5330.15 | 1.301 | 0.108 | 25.206 | 38.6% | 占 FUSE 38.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 12 | 131.34 | 10.945 | 0.011 | 65.536 | 1.0% | 占 FUSE 0.9% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 989 | 7904.76 | 7.993 | 7.217 | 24.597 | 57.3% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 7.89 | 1.974 | 2.000 | 4.900 | 0.1% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 518 | 4750.00 | 9.170 | 7.597 | 45.204 | 34.4% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,079 | 58761.34 | 28.264 | 28.471 | 49.794 | 99.9% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 4.26x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,079 | 24170.76 | 11.626 | 8.571 | 39.595 | 41.1% | Read:17.624ms, Write:5.743ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,079 | 46867.76 | 22.543 | 23.724 | 49.744 | 79.7% | HTTP Body+Codec 差值均值 5.905 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,079 | 34590.58 | 16.638 | 19.900 | 49.732 | 58.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,032 | 30923.70 | 29.965 | 36.618 | 49.732 | 52.6% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,035 | 3666.17 | 3.542 | 3.461 | 9.932 | 6.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,040 | 5.57 | 0.005 | 0.003 | 0.009 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,039 | 3625.17 | 3.489 | 3.426 | 9.931 | 6.2% | read(1035), getattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,028 | 33.16 | 0.032 | 0.030 | 0.201 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,039 | 9.79 | 0.009 | 0.008 | 0.046 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 1,039 | 113.12 | 0.109 | 0.075 | 1.661 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 1,039 | 12.56 | 0.012 | 0.009 | 0.033 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,039 | 19.35 | 0.019 | 0.017 | 0.070 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,898 | 3531.06 | 1.860 | 1.795 | 7.909 | 6.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,096 | 2315.32 | 2.113 | 1.951 | 4.949 | 3.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 33.01 | 0.516 | 0.003 | 15.400 | 0.1% | — |

### 工作负载 `fio_seq_16jobs` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `21.627 s` (`21627.0 ms`) vs FUSE 累计活跃 `259856.09 ms`（等效并发度 `12.02x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`259856.09 ms` / RPC `757332.50 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `16352.00 MiB` / 直通 `32.00 MiB` (Flush RPC `4096`) · ReadAhead 预读 `16384.00 MiB` / 命中 `16372.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 86,373 | 259856.09 | 3.009 | 0.046 | 59.848 | 100.0% | 错误返回 4176 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 16,384 | 142445.74 | 8.694 | 0.145 | 107.799 | 54.8% | 占 FUSE 54.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 65,536 | 114520.54 | 1.747 | 0.037 | 28.101 | 44.1% | 占 FUSE 44.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 16 | 2845.59 | 177.850 | 187.246 | 262.144 | 1.1% | 占 FUSE 1.1% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 3,832 | 139769.23 | 36.474 | 34.921 | 99.879 | 53.8% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 13 | 68.49 | 5.268 | 1.250 | 24.350 | 0.0% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 925 | 20497.95 | 22.160 | 14.576 | 202.155 | 7.9% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 8,322 | 757332.50 | 91.004 | 98.891 | 248.497 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 8,321 | 757106.92 | 90.987 | 98.896 | 248.497 | 100.0% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 2.91x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 8,321 | 136073.63 | 16.353 | 11.926 | 114.275 | 18.0% | Read:26.439ms, Write:6.506ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 8,321 | 676967.14 | 81.356 | 90.569 | 248.450 | 89.4% | HTTP Body+Codec 差值均值 6.722 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 8,321 | 621033.29 | 74.634 | 86.970 | 248.446 | 82.0% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,128 | 607171.18 | 147.086 | 172.309 | 248.446 | 80.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 4,129 | 13696.52 | 3.317 | 2.995 | 14.284 | 1.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,176 | 997.29 | 0.239 | 0.003 | 0.397 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 35 | 1148.36 | 32.810 | 33.036 | 97.812 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,145 | 13309.61 | 3.211 | 2.951 | 11.862 | 1.8% | read(4129), getattr(16) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4,096 | 110.74 | 0.027 | 0.022 | 0.091 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 4,161 | 37.54 | 0.009 | 0.008 | 0.038 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,123 | 409.96 | 0.099 | 0.073 | 1.464 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,123 | 52.86 | 0.013 | 0.008 | 0.041 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,116 | 93.11 | 0.023 | 0.018 | 0.082 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 8,124 | 12961.74 | 1.595 | 1.447 | 7.951 | 1.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,381 | 10025.13 | 2.288 | 2.113 | 5.135 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 6 | 25.39 | 4.231 | 1.000 | 24.100 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 7 | 92.28 | 13.183 | 0.375 | 96.500 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 64 | 17.98 | 0.281 | 0.021 | 6.800 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 257 | 18.05 | 0.070 | 0.003 | 3.692 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 8 | 9.24 | 1.156 | 0.375 | 9.600 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 6 | 22.54 | 3.756 | 0.400 | 12.572 | 0.0% | — |

### 工作负载 `fio_seq_read_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.747 s` (`10747.0 ms`) vs FUSE 累计活跃 `921.62 ms`（等效并发度 `0.09x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`921.62 ms` / RPC `6706.35 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4095.25 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 16,391 | 921.62 | 0.056 | 0.028 | 0.814 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 16,384 | 921.56 | 0.056 | 0.028 | 0.814 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 1 | 0.03 | 0.028 | 0.032 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 3 | 0.01 | 0.004 | 0.004 | 0.008 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 174 | 287.30 | 1.651 | 1.386 | 9.033 | 31.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,026 | 6677.95 | 6.509 | 7.159 | 17.671 | 99.6% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 7.28x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,026 | 4951.85 | 4.826 | 5.346 | 12.830 | 73.8% | Read:4.827ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,026 | 4420.40 | 4.308 | 4.058 | 9.796 | 65.9% | HTTP Body+Codec 差值均值 2.626 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,026 | 1726.10 | 1.682 | 1.813 | 4.841 | 25.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,026 | 1726.10 | 1.682 | 1.813 | 4.841 | 25.7% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,026 | 1689.96 | 1.647 | 1.794 | 4.812 | 25.2% | read(1026) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,024 | 20.85 | 0.020 | 0.019 | 0.049 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,027 | 6.75 | 0.007 | 0.007 | 0.023 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,164 | 1631.09 | 1.401 | 1.624 | 4.591 | 24.3% | — |

### 工作负载 `fio_seq_write_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.756 s` (`10756.0 ms`) vs FUSE 累计活跃 `354.21 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`354.21 ms` / RPC `6869.81 ms`)
- **缓存与流水线命中**：NSCache Attr `99.98%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4094.00 MiB` / 直通 `2.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 5,138 | 354.21 | 0.069 | 0.074 | 0.227 | 100.0% | 错误返回 1029 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 334.34 | 0.082 | 0.086 | 0.232 | 94.4% | 占 FUSE 94.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 1 | 12.29 | 12.293 | 16.384 | 16.384 | 3.5% | 占 FUSE 3.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 1,025 | 5.92 | 0.006 | 0.005 | 0.029 | 1.7% | 占 FUSE 1.7% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 11 | 12.94 | 1.176 | 0.708 | 9.450 | 3.7% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 1.42 | 1.420 | 1.750 | 2.485 | 0.4% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,030 | 6852.48 | 6.653 | 7.415 | 20.841 | 99.7% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 19.39x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,030 | 4022.91 | 3.906 | 5.021 | 11.780 | 58.6% | Write:3.919ms, Create:0.506ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,030 | 5462.18 | 5.303 | 5.303 | 9.988 | 79.5% | HTTP Body+Codec 差值均值 2.556 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,030 | 2829.56 | 2.747 | 2.394 | 9.950 | 41.2% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,026 | 2822.55 | 2.751 | 2.396 | 9.061 | 41.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 1 | 6.73 | 6.732 | 7.500 | 9.950 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,029 | 3.09 | 0.003 | 0.003 | 0.012 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 3 | 4.17 | 1.390 | 1.375 | 2.477 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1 | 0.01 | 0.005 | 0.008 | 0.010 | 0.0% | getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1 | 0.00 | 0.003 | 0.003 | 0.005 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 1,029 | 66.29 | 0.064 | 0.053 | 0.428 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 1,029 | 9.52 | 0.009 | 0.008 | 0.025 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,028 | 23.35 | 0.023 | 0.017 | 0.043 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,026 | 2093.97 | 2.041 | 1.891 | 4.933 | 30.5% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 1 | 4.31 | 4.309 | 3.750 | 4.975 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 1 | 6.57 | 6.570 | 7.500 | 9.950 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 64 | 0.91 | 0.014 | 0.011 | 0.154 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 65 | 247.48 | 3.807 | 3.802 | 15.250 | 3.6% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 2 | 7.50 | 3.748 | 3.300 | 4.298 | 0.1% | — |

### 工作负载 `juicefs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `5.796 s` (`5796.0 ms`) vs FUSE 累计活跃 `10782.13 ms`（等效并发度 `1.86x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`10782.13 ms` / RPC `47412.10 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.94%` / Readlink `N/A` · WriteBehind 聚批 `4138.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1424`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4094.50 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 48,323 | 10782.13 | 0.223 | 0.015 | 6.082 | 100.0% | 错误返回 4921 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,505 | 6521.55 | 1.448 | 0.105 | 20.681 | 60.5% | 占 FUSE 60.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 3557.43 | 0.107 | 0.016 | 1.872 | 33.0% | 占 FUSE 33.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 810 | 385.23 | 0.476 | 0.030 | 1.792 | 3.6% | 占 FUSE 3.6% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 858 | 5985.11 | 6.976 | 5.989 | 24.534 | 55.5% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 3 | 10.10 | 3.365 | 2.125 | 9.850 | 0.1% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 14 | 555.66 | 39.690 | 39.286 | 98.250 | 5.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,676 | 47342.94 | 12.879 | 15.420 | 52.267 | 99.9% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 4.40x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,676 | 16093.14 | 4.378 | 3.258 | 32.863 | 33.9% | Read:6.301ms, Write:4.807ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,676 | 39013.32 | 10.613 | 13.899 | 49.793 | 82.3% | HTTP Body+Codec 差值均值 2.112 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,676 | 31249.80 | 8.501 | 12.162 | 49.610 | 65.9% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 30348.49 | 21.193 | 30.522 | 49.610 | 64.0% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,432 | 842.74 | 0.589 | 0.664 | 2.359 | 1.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 57.85 | 0.026 | 0.003 | 0.021 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 3 | 5.03 | 1.677 | 0.175 | 4.925 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,435 | 798.76 | 0.557 | 0.660 | 2.311 | 1.7% | read(1432), lookup(1), getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,024 | 21.36 | 0.021 | 0.018 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,239 | 16.52 | 0.007 | 0.007 | 0.036 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,236 | 34.02 | 0.015 | 0.012 | 0.080 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,236 | 18.10 | 0.008 | 0.005 | 0.023 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,236 | 53.37 | 0.024 | 0.009 | 0.048 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,943 | 722.46 | 0.372 | 0.388 | 1.282 | 1.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,432 | 2316.49 | 1.618 | 1.633 | 4.920 | 4.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 15.68 | 0.245 | 0.003 | 6.800 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 11.91 | 3.970 | 4.750 | 7.929 | 0.0% | — |

### 工作负载 `mdtest_1p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `15.176 s` (`15176.0 ms`) vs FUSE 累计活跃 `11897.75 ms`（等效并发度 `0.78x`）；占比基准：`FUSE 活跃总耗时` (`11897.75 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.41%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 262,143 | 11897.75 | 0.045 | 0.002 | 0.408 | 100.0% | 错误返回 26439 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 13,996 | 3120.18 | 0.223 | 0.203 | 0.496 | 26.2% | 占 FUSE 26.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 13,997 | 3064.48 | 0.219 | 0.199 | 0.489 | 25.8% | 占 FUSE 25.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 12,440 | 2820.96 | 0.227 | 0.205 | 0.499 | 23.7% | 占 FUSE 23.7% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 52,875 | 10765.16 | 0.204 | 0.182 | 0.995 | 90.5% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 52,875 | 8556.60 | 0.162 | 0.142 | 0.746 | 71.9% | Mkdir:0.165ms, Rmdir:0.159ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 52,875 | 3535.62 | 0.067 | 0.066 | 0.249 | 29.7% | HTTP Body+Codec 差值均值 0.025 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 52,875 | 2208.56 | 0.042 | 0.040 | 0.249 | 18.6% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 13,996 | 657.33 | 0.047 | 0.044 | 0.188 | 5.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 13,997 | 552.68 | 0.039 | 0.039 | 0.099 | 4.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 52,873 | 50.84 | 0.001 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.06 | 0.020 | 0.020 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.05 | 0.017 | 0.021 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 52,873 | 624.58 | 0.012 | 0.010 | 0.047 | 5.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 52,873 | 59.98 | 0.001 | 0.003 | 0.005 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 52,873 | 234.82 | 0.004 | 0.003 | 0.022 | 2.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.53 | 0.525 | 0.550 | 0.599 | 0.0% | — |

### 工作负载 `mdtest_4p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.750 s` (`10750.0 ms`) vs FUSE 累计活跃 `1466.11 ms`（等效并发度 `0.14x`）；占比基准：`FUSE 活跃总耗时` (`1466.11 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.95%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `96.83%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 17,957 | 1466.11 | 0.082 | 0.002 | 0.507 | 100.0% | 错误返回 2136 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 1,094 | 397.84 | 0.364 | 0.362 | 0.881 | 27.1% | 占 FUSE 27.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 1,093 | 331.96 | 0.304 | 0.337 | 0.740 | 22.6% | 占 FUSE 22.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 1,040 | 330.87 | 0.318 | 0.358 | 0.801 | 22.6% | 占 FUSE 22.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,269 | 1274.74 | 0.299 | 0.316 | 0.995 | 86.9% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,269 | 1067.63 | 0.250 | 0.273 | 0.895 | 72.8% | Mkdir:0.293ms, Create:0.253ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,269 | 387.55 | 0.091 | 0.085 | 0.249 | 26.4% | HTTP Body+Codec 差值均值 0.042 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,269 | 207.11 | 0.049 | 0.043 | 0.228 | 14.1% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 1,093 | 57.34 | 0.052 | 0.046 | 0.223 | 3.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 1,094 | 55.72 | 0.051 | 0.044 | 0.228 | 3.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,267 | 10.82 | 0.003 | 0.003 | 0.025 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.04 | 0.012 | 0.016 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.03 | 0.009 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,197 | 45.92 | 0.011 | 0.010 | 0.041 | 3.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,197 | 4.75 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,197 | 17.36 | 0.004 | 0.003 | 0.017 | 1.2% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.18 | 0.178 | 0.150 | 0.199 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`461,153` 次（错误返回 `46,916` 次），FUSE 累计活跃 `321004.15 ms`，读 `32818.01 MiB` / 写 `32818.00 MiB`
- **NamespaceCache 命中率**：Attr `99.99%` · Entry `99.99%` · Readlink `N/A` · 负缓存命中 `29,065` · 失效 `102,108`
- **WriteBehind 流水线**：聚批写 `28685.00 MiB` (`29,035` 次) · 直通写 `4133.00 MiB` (`4,133` 次) · Flush RPC `7,580` (错误 `0`) · 背压等待 `5,690` 次 (`153672.04 ms`) · 屏障等待 `21` 次 (`87.90 ms`)
- **ReadAhead 预读**：触发 `7,379` 次 · 预读 `29040.00 MiB` · 命中 `28693.75 MiB` · 丢弃 `289.75 MiB` · 等待 `1,651` 次 (`26280.47 ms`)
- **Inode 锁与内核队列**：快速获取 `41,744` · 竞争获取 `0`（累计等待 `0.00 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `Writev` | 33,177 | 168777.04 | 52.6% | 5087.2 | 126.1 | 40559.0 | 83811.8 | EINTR:9 |
| `Read` | 123,297 | 135001.19 | 42.1% | 1094.9 | 29.9 | 7435.8 | 22014.2 | — |
| `Mkdir` | 15,093 | 3465.00 | 1.1% | 229.6 | 203.5 | 431.6 | 500.4 | — |
| `Rmdir` | 15,093 | 3463.41 | 1.1% | 229.5 | 206.7 | 446.2 | 501.8 | — |
| `Create` | 13,905 | 3317.60 | 1.0% | 238.6 | 212.0 | 462.3 | 506.2 | — |
| `Unlink` | 13,905 | 3047.75 | 0.9% | 219.2 | 200.6 | 409.4 | 498.5 | — |
| `Fsync` | 17 | 2857.89 | 0.9% | 168111.0 | 187245.7 | 262144.0 | 262144.0 | — |
| `Flush` | 27,841 | 553.72 | 0.2% | 19.9 | 1.2 | 4.3 | 678.2 | — |
| `Release` | 27,841 | 148.47 | 0.1% | 5.3 | 2.1 | 6.8 | 14.2 | — |
| `Lookup` | 76,192 | 139.17 | 0.0% | 1.8 | 1.5 | 3.7 | 7.4 | ENOENT:29069 |
| `GetXAttr` | 17,838 | 88.68 | 0.0% | 5.0 | 3.6 | 14.0 | 27.7 | ENODATA:17838 |
| `GetAttr` | 53,102 | 61.43 | 0.0% | 1.2 | 0.9 | 1.9 | 3.8 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `81,546` 次 · 正常 (`OK`) `81,520` 次 · 业务/协议非 OK `26` 次 (`0.03%`，分布 `{"CANCELED": 2, "UNKNOWN": 16, "NOT_FOUND": 8}`)
- **客户端 RPC 状态码拆分**：总调用 `81,547` 次 · 正常 (`OK`) `81,496` 次 · 非 OK `51` 次 (`0.06%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Write` | 11,713 | — | 705709.90 | 60.250 | 27.795 | 228.005 | 245.601 |
| `http_outer:Read` | 11,763 | — | 80771.46 | 6.867 | 4.055 | 23.659 | 46.311 |
| `http_outer:Rmdir` | 15,093 | — | 1094.58 | 0.073 | 0.072 | 0.190 | 0.239 |
| `http_outer:Create` | 13,905 | — | 1054.00 | 0.076 | 0.075 | 0.196 | 0.240 |
| `http_outer:Mkdir` | 15,093 | — | 1016.34 | 0.067 | 0.069 | 0.151 | 0.231 |
| `http_outer:Unlink` | 13,905 | — | 902.13 | 0.065 | 0.056 | 0.143 | 0.233 |
| `http_outer:Fsync` | 17 | — | 171.14 | 10.067 | 5.833 | 57.500 | 91.500 |
| `http_outer:CreateDisk` | 1 | — | 4.28 | 4.277 | 3.750 | 4.875 | 4.975 |
| `http_outer:Fallocate` | 21 | — | 2.03 | 0.097 | 0.080 | 0.218 | 0.244 |
| `http_outer:GetXattr` | 4 | — | 1.31 | 0.327 | 0.375 | 0.487 | 0.497 |
| `http_outer:GetAttr` | 21 | — | 1.08 | 0.052 | 0.044 | 0.100 | 0.218 |
| `http_outer:Lookup` | 4 | — | 1.01 | 0.253 | 0.200 | 0.450 | 0.490 |
| `http_outer:StatFS` | 6 | — | 0.96 | 0.160 | 0.160 | 0.241 | 0.248 |
| `http_outer:MountDisk` | 1 | — | 0.48 | 0.482 | 0.375 | 0.487 | 0.497 |
| `rpc:Write` | 11,713 | 11,713 / 0 | 675794.45 | 57.696 | 26.099 | 227.955 | 245.591 |
| `rpc:Read` | 11,762 | 11,744 / 18 | 22669.64 | 1.927 | 1.348 | 6.586 | 9.657 |
| `rpc:Rmdir` | 15,093 | 15,093 / 0 | 715.33 | 0.047 | 0.044 | 0.096 | 0.194 |
| `rpc:Create` | 13,905 | 13,905 / 0 | 628.11 | 0.045 | 0.042 | 0.094 | 0.157 |
| `rpc:Mkdir` | 15,093 | 15,093 / 0 | 608.56 | 0.040 | 0.039 | 0.089 | 0.100 |
| `rpc:Unlink` | 13,905 | 13,905 / 0 | 577.53 | 0.042 | 0.035 | 0.087 | 0.119 |
| `rpc:Fsync` | 17 | 17 / 0 | 170.15 | 10.009 | 5.833 | 57.500 | 91.500 |
| `rpc:CreateDisk` | 1 | 1 / 0 | 4.04 | 4.038 | 3.750 | 4.875 | 4.975 |
| `rpc:Fallocate` | 21 | 21 / 0 | 1.16 | 0.055 | 0.049 | 0.197 | 0.239 |
| `rpc:GetXattr` | 4 | 0 / 4 | 0.67 | 0.168 | 0.175 | 0.242 | 0.249 |
| `rpc:Lookup` | 4 | 0 / 4 | 0.50 | 0.125 | 0.100 | 0.235 | 0.247 |
| `rpc:GetAttr` | 21 | 21 / 0 | 0.48 | 0.023 | 0.017 | 0.046 | 0.049 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:phase_a_validate` | 69,613 | 1404.94 | 0.020 | 0.012 | 0.068 | 0.098 |
| `actor_phase:wal_apply_nosync` | 69,604 | 525.67 | 0.008 | 0.004 | 0.022 | 0.027 |
| `actor_phase:phase_b_mutate` | 69,613 | 212.10 | 0.003 | 0.003 | 0.009 | 0.019 |
| `actor_phase:pack_sync_barrier` | 8 | 98.85 | 12.356 | 0.500 | 80.000 | 96.000 |
| `actor_phase:wal_apply_sync` | 8 | 29.85 | 3.732 | 1.000 | 19.000 | 23.800 |
| `actor_phase:dead_pack_remove` | 128 | 18.88 | 0.148 | 0.014 | 0.467 | 4.650 |
| `read_op:read` | 11,746 | 22041.63 | 1.877 | 1.306 | 6.491 | 9.594 |
| `read_op:getxattr` | 4 | 0.27 | 0.068 | 0.075 | 0.098 | 0.100 |
| `read_op:getattr` | 27 | 0.15 | 0.006 | 0.005 | 0.020 | 0.024 |
| `read_op:lookup` | 4 | 0.11 | 0.027 | 0.025 | 0.048 | 0.049 |
| `read_op:statfs` | 7 | 0.00 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:prefix_scan` | 11,296 | 284.84 | 0.025 | 0.021 | 0.047 | 0.081 |
| `kv_read:point_get` | 12,610 | 101.04 | 0.008 | 0.008 | 0.021 | 0.033 |
| `control_open_actor:ok` | 1 | 2.26 | 2.263 | 1.750 | 2.425 | 2.485 |
| `actor_queue_wait` | 69,748 | 1147.61 | 0.016 | 0.003 | 0.005 | 0.009 |
| `inode_lock_wait` | 41 | 1157.56 | 28.233 | 27.679 | 87.187 | 97.437 |
| `blob_pread` | 17,593 | 21234.53 | 1.207 | 0.904 | 4.177 | 6.234 |
| `blob_pwrite` | 12,030 | 18987.31 | 1.578 | 1.397 | 4.398 | 4.934 |
| `pebble_wal_fsync` | 17 | 43.13 | 2.537 | 0.375 | 11.133 | 12.372 |
| `blob_sync:pack_data` | 514 | 598.07 | 1.164 | 0.004 | 7.424 | 9.659 |
| `blob_sync:packs_dir` | 9 | 9.25 | 1.028 | 0.312 | 7.750 | 9.550 |

- **Pebble 状态**：BlockCache 命中率 `99.04%` · ReadAmp `1.0` · WriteStall `0.0` · WAL 写入 `29365228.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 52428800.0, "falloc_zero": 0.0, "full_block": 34359738368.0, "rmw": 0.0, "zero_elide": 0.0}` · 读源块计数 `{"disk_pack": 132589.0, "hot_cache": 400.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`1330` 条（跨 Client+Server 全链路 `990` 条，异常链路 `42` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `98d4f011a0a17780` | 237.24 | `bowl-client:fuse.Fsync` | 215.80 | 211.04 | 4.75 | `bowl-client:storage.v1.StorageService/Write` (215.80 ms) |
| `a0744ba7d441b1d9` | 232.40 | `bowl-client:fuse.Fsync` | 210.87 | 208.75 | 2.12 | `bowl-client:storage.v1.StorageService/Write` (210.87 ms) |
| `4abe4a1dc6179c88` | 225.95 | `bowl-client:fuse.Fsync` | 143.34 | 140.76 | 2.58 | `bowl-client:storage.v1.StorageService/Write` (143.34 ms) |
| `a025dfb0c9e4dd16` | 220.49 | `bowl-client:write_behind.FlushChunk` | 220.46 | 215.47 | 5.00 | `bowl-client:storage.v1.StorageService/Write` (220.46 ms) |
| `b65953061d1ebe30` | 220.03 | `bowl-client:fuse.Fsync` | 205.37 | 203.07 | 2.30 | `bowl-client:storage.v1.StorageService/Write` (205.37 ms) |
| `16be670b4aacb4df` | 214.06 | `bowl-client:write_behind.FlushChunk` | 214.05 | 209.97 | 4.08 | `bowl-client:storage.v1.StorageService/Write` (214.05 ms) |
| `559ac7067094afc9` | 210.54 | `bowl-client:write_behind.FlushChunk` | 210.53 | 206.34 | 4.19 | `bowl-client:storage.v1.StorageService/Write` (210.53 ms) |
| `66ae8d50a92ea658` | 207.51 | `bowl-client:write_behind.FlushChunk` | 207.47 | 200.62 | 6.85 | `bowl-client:storage.v1.StorageService/Write` (207.47 ms) |
| `8e4aa44bdb648bfc` | 205.41 | `bowl-client:fuse.Fsync` | 198.23 | 194.44 | 3.79 | `bowl-client:storage.v1.StorageService/Write` (198.23 ms) |
| `8003e1055540dc9e` | 204.81 | `bowl-client:fuse.Fsync` | 184.00 | 179.90 | 4.10 | `bowl-client:storage.v1.StorageService/Write` (184.00 ms) |
| `19e107da2191b30b` | 203.56 | `bowl-client:write_behind.FlushChunk` | 203.54 | 191.08 | 12.46 | `bowl-client:storage.v1.StorageService/Write` (203.54 ms) |
| `25e712528213b0a1` | 203.37 | `bowl-client:fuse.Fsync` | 202.14 | 191.19 | 10.96 | `bowl-client:storage.v1.StorageService/Write` (202.14 ms) |
| `249f09595934d57a` | 201.05 | `bowl-client:write_behind.FlushChunk` | 201.04 | 197.88 | 3.16 | `bowl-client:storage.v1.StorageService/Write` (201.04 ms) |
| `aa20d4587f922cad` | 199.21 | `bowl-client:write_behind.FlushChunk` | 199.17 | 193.40 | 5.77 | `bowl-client:storage.v1.StorageService/Write` (199.17 ms) |
| `d7a5494b71244e9c` | 191.30 | `bowl-client:write_behind.FlushChunk` | 191.28 | 185.04 | 6.24 | `bowl-client:storage.v1.StorageService/Write` (191.28 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`4074` · **服务端 Warn/Error 数**：`4043` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 9, "flush_begin": 3, "table_created": 6, "flush_end": 3, "wal_deleted": 3, "compaction_begin": 3, "compaction_end": 3}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `fio_direct_randwrite_cpu.pb.gz` | `4.24s 62.54% 62.54%      4.24s 62.54%  internal/runtime/syscall/linux.Syscall6` | `4.24s 62.54% 62.54%      4.24s 62.54%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_fio_direct_randwrite_cpu.txt` |
| `server` | `fio_direct_randwrite_heap.pb.gz` | `100MB 30.38% 30.38%      100MB 30.38%  bytes.growSlice` | `0     0%     0%   278.06MB 84.48%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_fio_direct_randwrite_heap.txt` |
| `server` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | `pprof_top/server_fio_direct_randwrite_mutex.txt` |
| `server` | `fio_seq_read_1job_cpu.pb.gz` | `2.60s 57.65% 57.65%      2.60s 57.65%  internal/runtime/syscall/linux.Syscall6` | `2.60s 57.65% 57.65%      2.60s 57.65%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_fio_seq_read_1job_cpu.txt` |
| `server` | `fio_seq_read_1job_heap.pb.gz` | `99.20MB 59.10% 59.10%    99.20MB 59.10%  bytes.growSlice` | `0     0%     0%   143.71MB 85.62%  connectrpc.com/connect.(*Handler).ServeHTTP` | `pprof_top/server_fio_seq_read_1job_heap.txt` |
| `server` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/server_fio_seq_read_1job_mutex.txt` |
| `server` | `fio_seq_write_1job_cpu.pb.gz` | `4.21s 74.51% 74.51%      4.21s 74.51%  internal/runtime/syscall/linux.Syscall6` | `4.21s 74.51% 74.51%      4.21s 74.51%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_fio_seq_write_1job_cpu.txt` |
| `server` | `fio_seq_write_1job_heap.pb.gz` | `99.20MB 59.10% 59.10%    99.20MB 59.10%  bytes.growSlice` | `0     0%     0%   143.71MB 85.62%  connectrpc.com/connect.(*Handler).ServeHTTP` | `pprof_top/server_fio_seq_write_1job_heap.txt` |
| `server` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/server_fio_seq_write_1job_mutex.txt` |
| `server` | `mdtest_1p_cpu.pb.gz` | `0.71s 11.15% 11.15%      0.71s 11.15%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.44s 38.30%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_mdtest_1p_cpu.txt` |
| `server` | `mdtest_1p_heap.pb.gz` | `51MB 28.86% 28.86%       51MB 28.86%  github.com/cockroachdb/pebble/v2/internal/manual.New` | `0     0%     0%   101.09MB 57.20%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_mdtest_1p_heap.txt` |
| `server` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | `pprof_top/server_mdtest_1p_mutex.txt` |
| `server` | `mdtest_4p_cpu.pb.gz` | `120ms 17.65% 17.65%      120ms 17.65%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      220ms 32.35%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_mdtest_4p_cpu.txt` |
| `server` | `mdtest_4p_heap.pb.gz` | `51MB 36.96% 36.96%       51MB 36.96%  github.com/cockroachdb/pebble/v2/internal/manual.New` | `0     0%     0%    64.54MB 46.78%  sync.(*Pool).Get` | `pprof_top/server_mdtest_4p_heap.txt` |
| `server` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | `pprof_top/server_mdtest_4p_mutex.txt` |
| `client` | `fio_direct_randwrite_cpu.pb.gz` | `1.51s 47.34% 47.34%      1.51s 47.34%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.92s 60.19%  net/http.(*http2clientStream).doRequest` | `pprof_top/client_fio_direct_randwrite_cpu.txt` |
| `client` | `fio_direct_randwrite_heap.pb.gz` | `320.11MB 45.40% 45.40%   320.11MB 45.40%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   401.57MB 56.95%  sync.(*Pool).Get` | `pprof_top/client_fio_direct_randwrite_heap.txt` |
| `client` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | `pprof_top/client_fio_direct_randwrite_mutex.txt` |
| `client` | `fio_seq_read_1job_cpu.pb.gz` | `2.12s 48.18% 48.18%      2.12s 48.18%  internal/runtime/syscall/linux.Syscall6` | `2.12s 48.18% 48.18%      2.12s 48.18%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/client_fio_seq_read_1job_cpu.txt` |
| `client` | `fio_seq_read_1job_heap.pb.gz` | `110.04MB 50.33% 50.33%   110.04MB 50.33%  bytes.growSlice` | `0     0%     0%   154.06MB 70.46%  connectrpc.com/connect.(*Client[go.shape.e80ba5ebc5f7385136069f97c3374d2892f8c315d57d93e4b627333ca6474eba,go.shape.struct { github.com/starsrc/bowl/gen/go/storage/v1.state google.golang.org/protobuf/internal/impl.MessageState "protogen:\"open.v1\""; Data []uint8 "protobuf:\"bytes,1,opt,name=data,proto3\" json:\"data,omitempty\""; github.com/starsrc/bowl/gen/go/storage/v1.unknownFields []uint8; github.com/starsrc/bowl/gen/go/storage/v1.sizeCache int32 }]).CallUnary` | `pprof_top/client_fio_seq_read_1job_heap.txt` |
| `client` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/client_fio_seq_read_1job_mutex.txt` |
| `client` | `fio_seq_write_1job_cpu.pb.gz` | `1.47s 46.37% 46.37%      1.47s 46.37%  runtime.memmove` | `1.47s 46.37% 46.37%      1.47s 46.37%  runtime.memmove` | `pprof_top/client_fio_seq_write_1job_cpu.txt` |
| `client` | `fio_seq_write_1job_heap.pb.gz` | `110.04MB 50.33% 50.33%   110.04MB 50.33%  bytes.growSlice` | `0     0%     0%   154.06MB 70.46%  connectrpc.com/connect.(*Client[go.shape.e80ba5ebc5f7385136069f97c3374d2892f8c315d57d93e4b627333ca6474eba,go.shape.struct { github.com/starsrc/bowl/gen/go/storage/v1.state google.golang.org/protobuf/internal/impl.MessageState "protogen:\"open.v1\""; Data []uint8 "protobuf:\"bytes,1,opt,name=data,proto3\" json:\"data,omitempty\""; github.com/starsrc/bowl/gen/go/storage/v1.unknownFields []uint8; github.com/starsrc/bowl/gen/go/storage/v1.sizeCache int32 }]).CallUnary` | `pprof_top/client_fio_seq_write_1job_heap.txt` |
| `client` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/client_fio_seq_write_1job_mutex.txt` |
| `client` | `mdtest_1p_cpu.pb.gz` | `0.79s 20.31% 20.31%      0.79s 20.31%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.10s 53.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_mdtest_1p_cpu.txt` |
| `client` | `mdtest_1p_heap.pb.gz` | `320.11MB 32.51% 32.51%   320.11MB 32.51%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   586.74MB 59.59%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_mdtest_1p_heap.txt` |
| `client` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | `pprof_top/client_mdtest_1p_mutex.txt` |
| `client` | `mdtest_4p_cpu.pb.gz` | `140ms 26.92% 26.92%      140ms 26.92%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      300ms 57.69%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_mdtest_4p_cpu.txt` |
| `client` | `mdtest_4p_heap.pb.gz` | `320.11MB 32.51% 32.51%   320.11MB 32.51%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   586.74MB 59.59%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_mdtest_4p_heap.txt` |
| `client` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | `pprof_top/client_mdtest_4p_mutex.txt` |
