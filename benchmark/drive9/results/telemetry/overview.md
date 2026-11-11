# Bowl `drive9` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `fuse_perf_baseline` | 16.000 |
| 2 | `jfs_bench` | 10.000 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `d9_cold_io` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `d9_cold_seq_read`, `d9_cold_small_write_fsync`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `21.858 s` (`21858.0 ms`) vs FUSE 累计活跃 `3698.32 ms`（等效并发度 `0.17x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`3698.32 ms` / RPC `9243.53 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `97.54%` / Readlink `N/A` · WriteBehind 聚批 `2286.00 MiB` / 直通 `20.34 MiB` (Flush RPC `576`) · ReadAhead 预读 `840.00 MiB` / 命中 `764.62 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 23,665 | 3698.32 | 0.156 | 0.015 | 0.978 | 100.0% | 错误返回 7078 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 9,114 | 2099.61 | 0.230 | 0.028 | 1.011 | 56.8% | 占 FUSE 56.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 609 | 663.94 | 1.090 | 0.415 | 40.960 | 18.0% | 占 FUSE 17.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 2,904 | 553.83 | 0.191 | 0.115 | 1.555 | 15.0% | 占 FUSE 15.0% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 59 | 76.26 | 1.293 | 0.882 | 7.050 | 2.1% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 9 | 51.06 | 5.674 | 6.786 | 9.936 | 1.4% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 21 | 130.28 | 6.204 | 4.375 | 24.370 | 3.5% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 6,087 | 9243.53 | 1.519 | 0.504 | 33.083 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 6,086 | 9192.26 | 1.510 | 0.498 | 33.083 | 99.4% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 2.50x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 6,086 | 5920.41 | 0.973 | 0.310 | 15.902 | 64.0% | Write:2.529ms, Read:0.788ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 6,086 | 6279.73 | 1.032 | 0.325 | 33.083 | 67.9% | HTTP Body+Codec 差值均值 0.494 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 6,086 | 3271.85 | 0.538 | 0.187 | 33.083 | 35.4% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,194 | 1711.62 | 1.434 | 0.248 | 8.806 | 18.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 3,187 | 1018.45 | 0.320 | 0.197 | 2.969 | 11.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,866 | 5.53 | 0.002 | 0.003 | 0.009 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 111 | 45.73 | 0.412 | 0.160 | 3.613 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3,220 | 966.13 | 0.300 | 0.192 | 2.914 | 10.5% | read(3187), readdirplus(22), lookup(11) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,108 | 29.19 | 0.005 | 0.003 | 0.040 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 961 | 15.91 | 0.017 | 0.018 | 0.048 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,866 | 34.64 | 0.012 | 0.010 | 0.049 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,866 | 180.80 | 0.063 | 0.003 | 2.106 | 2.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,257 | 24.15 | 0.011 | 0.008 | 0.041 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 7,062 | 875.72 | 0.124 | 0.087 | 1.417 | 9.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,194 | 1134.14 | 0.950 | 0.042 | 4.448 | 12.3% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 609 | 118.25 | 0.194 | 0.176 | 1.955 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 609 | 326.26 | 0.536 | 0.176 | 33.083 | 3.5% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 10 | 66.43 | 6.643 | 6.667 | 24.500 | 0.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 645 | 1330.18 | 2.062 | 0.177 | 45.393 | 14.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 11 | 0.05 | 0.005 | 0.003 | 0.023 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 610 | 105.03 | 0.172 | 0.131 | 2.090 | 1.1% | — |

### 工作负载 `d9_cold_meta_macro` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `27.292 s` (`27292.0 ms`) vs FUSE 累计活跃 `9428.29 ms`（等效并发度 `0.34x`）；占比基准：`FUSE 活跃总耗时` (`9428.29 ms`)
- **缓存与流水线命中**：NSCache Attr `96.67%` / Entry `99.25%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `64.17 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `95.47%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 132,174 | 9428.29 | 0.071 | 0.002 | 0.496 | 100.0% | 错误返回 34084 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 9,076 | 2716.47 | 0.299 | 0.349 | 0.511 | 28.8% | 占 FUSE 28.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,307 | 1487.80 | 0.280 | 0.278 | 0.510 | 15.8% | 占 FUSE 15.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,056 | 1352.61 | 0.268 | 0.228 | 0.568 | 14.3% | 占 FUSE 14.3% |
| `2. 客户端等待` | `Client InodeLock 等待` | 1 | 0.26 | 0.261 | 0.375 | 0.497 | 0.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 33,137 | 8560.65 | 0.258 | 0.245 | 2.365 | 90.8% | 非 OK 状态 47 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 33,137 | 6776.49 | 0.204 | 0.192 | 1.951 | 71.9% | Write:0.213ms, Create:0.207ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 33,137 | 2964.37 | 0.089 | 0.088 | 2.365 | 31.4% | HTTP Body+Codec 差值均值 0.035 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 33,137 | 1784.16 | 0.054 | 0.052 | 2.365 | 18.9% | 非 OK 状态 47 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 9,076 | 647.46 | 0.071 | 0.072 | 0.237 | 6.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Create` | 5,307 | 292.83 | 0.055 | 0.054 | 0.163 | 3.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 27,217 | 34.46 | 0.001 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 5,923 | 91.13 | 0.015 | 0.015 | 0.077 | 1.0% | readdirplus(3158), read(1088), getattr(1539) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 19,465 | 121.45 | 0.006 | 0.004 | 0.040 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 3,158 | 27.76 | 0.009 | 0.006 | 0.047 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 27,217 | 278.74 | 0.010 | 0.008 | 0.046 | 3.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 27,217 | 69.00 | 0.003 | 0.003 | 0.009 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 27,208 | 160.69 | 0.006 | 0.005 | 0.022 | 1.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 9,076 | 96.57 | 0.011 | 0.009 | 0.036 | 1.0% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 4 | 0.03 | 0.007 | 0.007 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 6 | 1.09 | 0.182 | 0.190 | 0.485 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 9 | 2.58 | 0.287 | 0.088 | 2.365 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 0.23 | 0.230 | 0.175 | 0.249 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 7 | 6.31 | 0.901 | 0.138 | 9.650 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 8 | 1.26 | 0.157 | 0.160 | 0.296 | 0.0% | — |

### 工作负载 `d9_cold_seq_read` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `6.856 s` (`6856.0 ms`) vs FUSE 累计活跃 `777.76 ms`（等效并发度 `0.11x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`777.76 ms` / RPC `3775.17 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `81.82%` / Readlink `N/A` · WriteBehind 聚批 `762.00 MiB` / 直通 `6.00 MiB` (Flush RPC `192`) · ReadAhead 预读 `840.00 MiB` / 命中 `764.62 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 8,555 | 777.76 | 0.091 | 0.017 | 0.482 | 100.0% | 错误返回 1555 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 6,144 | 499.69 | 0.081 | 0.019 | 0.290 | 64.2% | 占 FUSE 64.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 3 | 148.62 | 49.540 | 54.613 | 65.536 | 19.1% | 占 FUSE 19.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 768 | 114.74 | 0.149 | 0.103 | 1.955 | 14.8% | 占 FUSE 14.8% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 25 | 29.15 | 1.166 | 0.875 | 4.792 | 3.7% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 3 | 18.14 | 6.047 | 7.500 | 9.950 | 2.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 21 | 130.28 | 6.204 | 4.375 | 24.370 | 16.8% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 439 | 3775.17 | 8.599 | 8.172 | 49.750 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 438 | 3764.88 | 8.596 | 8.162 | 49.750 | 99.7% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 4.85x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 438 | 2621.05 | 5.984 | 5.907 | 19.625 | 69.4% | Read:7.384ms, Write:5.109ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 438 | 2636.61 | 6.020 | 5.934 | 49.750 | 69.8% | HTTP Body+Codec 差值均值 3.409 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 438 | 1143.82 | 2.611 | 2.256 | 49.750 | 30.3% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 198 | 614.96 | 3.106 | 2.453 | 9.550 | 16.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 217 | 398.79 | 1.838 | 1.781 | 4.923 | 10.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 215 | 0.59 | 0.003 | 0.003 | 0.010 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 51 | 11.58 | 0.227 | 0.153 | 2.118 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 223 | 390.88 | 1.753 | 1.719 | 4.921 | 10.4% | read(217), readdirplus(4), lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 214 | 4.27 | 0.020 | 0.019 | 0.049 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 234 | 1.61 | 0.007 | 0.007 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 215 | 5.23 | 0.024 | 0.016 | 0.164 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 215 | 91.39 | 0.425 | 0.009 | 4.042 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 212 | 3.77 | 0.018 | 0.017 | 0.097 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 3,363 | 372.45 | 0.111 | 0.041 | 1.995 | 9.9% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 198 | 377.31 | 1.906 | 1.785 | 4.847 | 10.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 3 | 22.70 | 7.568 | 13.750 | 24.775 | 0.6% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 3 | 83.68 | 27.895 | 37.500 | 49.750 | 2.2% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 3 | 42.90 | 14.301 | 13.750 | 24.775 | 1.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 15 | 430.23 | 28.682 | 34.375 | 49.688 | 11.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 3 | 0.01 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 22.58 | 7.527 | 9.689 | 10.836 | 0.6% | — |

### 工作负载 `d9_cold_small_write_fsync` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `8.865 s` (`8865.0 ms`) vs FUSE 累计活跃 `830.70 ms`（等效并发度 `0.09x`）；占比基准：`FUSE 活跃总耗时` (`830.70 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.51%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `2.34 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 7,274 | 830.70 | 0.114 | 0.003 | 0.789 | 100.0% | 错误返回 2410 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 600 | 292.65 | 0.488 | 0.413 | 1.024 | 35.2% | 占 FUSE 35.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 600 | 211.64 | 0.353 | 0.387 | 0.805 | 25.5% | 占 FUSE 25.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 600 | 206.52 | 0.344 | 0.383 | 0.896 | 24.9% | 占 FUSE 24.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,215 | 781.07 | 0.353 | 0.348 | 1.000 | 94.0% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,215 | 505.58 | 0.228 | 0.233 | 0.732 | 60.9% | Create:0.252ms, Write:0.243ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,215 | 376.66 | 0.170 | 0.204 | 0.990 | 45.3% | HTTP Body+Codec 差值均值 0.046 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,215 | 275.49 | 0.124 | 0.116 | 0.499 | 33.2% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 600 | 152.85 | 0.255 | 0.232 | 0.499 | 18.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 600 | 56.41 | 0.094 | 0.086 | 0.246 | 6.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,209 | 3.52 | 0.002 | 0.003 | 0.006 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 6 | 0.50 | 0.083 | 0.070 | 0.247 | 0.1% | readdirplus(4), lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 614 | 13.32 | 0.022 | 0.020 | 0.050 | 1.6% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4 | 0.43 | 0.108 | 0.050 | 0.247 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,209 | 22.14 | 0.010 | 0.008 | 0.043 | 2.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,209 | 5.87 | 0.003 | 0.003 | 0.010 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,609 | 11.59 | 0.007 | 0.007 | 0.024 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 600 | 8.64 | 0.014 | 0.017 | 0.033 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 600 | 79.81 | 0.133 | 0.175 | 0.249 | 9.6% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 600 | 84.36 | 0.141 | 0.175 | 0.249 | 10.2% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 0.48 | 0.482 | 0.375 | 0.497 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 600 | 77.94 | 0.130 | 0.171 | 0.249 | 9.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.007 | 0.005 | 0.025 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 600 | 66.79 | 0.111 | 0.130 | 0.200 | 8.0% | — |

### 工作负载 `d9_hot` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `15.994 s` (`15994.0 ms`) vs FUSE 累计活跃 `10997.22 ms`（等效并发度 `0.69x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`10997.22 ms` / RPC `14709.05 ms`)
- **缓存与流水线命中**：NSCache Attr `96.79%` / Entry `98.83%` / Readlink `N/A` · WriteBehind 聚批 `2286.00 MiB` / 直通 `84.58 MiB` (Flush RPC `576`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.63%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 137,428 | 10997.22 | 0.080 | 0.003 | 0.499 | 100.0% | 错误返回 41556 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 11,988 | 3289.76 | 0.274 | 0.307 | 0.697 | 29.9% | 占 FUSE 29.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,922 | 1594.52 | 0.269 | 0.250 | 0.509 | 14.5% | 占 FUSE 14.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,898 | 1565.68 | 0.265 | 0.223 | 0.535 | 14.2% | 占 FUSE 14.2% |
| `2. 客户端等待` | `Client InodeLock 等待` | 1 | 0.00 | 0.001 | 0.013 | 0.025 | 0.0% | — |
| `2. 客户端等待` | `WriteBehind 背压等待` | 65 | 69.33 | 1.067 | 0.554 | 8.375 | 0.6% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 9 | 55.79 | 6.199 | 7.188 | 9.944 | 0.5% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 35,960 | 14530.99 | 0.404 | 0.250 | 55.857 | 98.8% | 非 OK 状态 75 次 · 异步并发流水线 (累计为 FUSE 活跃 1.34x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 35,960 | 10137.73 | 0.282 | 0.190 | 7.357 | 68.9% | Write:0.494ms, Create:0.195ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 35,960 | 7314.76 | 0.203 | 0.102 | 48.500 | 49.7% | HTTP Body+Codec 差值均值 0.081 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 35,960 | 4393.27 | 0.122 | 0.060 | 48.500 | 29.9% | 非 OK 状态 75 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 10,278 | 2482.43 | 0.242 | 0.078 | 4.503 | 16.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 618 | 631.88 | 1.022 | 0.208 | 48.500 | 4.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 30,563 | 41.17 | 0.001 | 0.003 | 0.006 | 0.3% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 142 | 61.04 | 0.430 | 0.174 | 2.470 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 5,403 | 82.35 | 0.015 | 0.014 | 0.091 | 0.6% | readdirplus(3220), read(473), getattr(1552) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 19,003 | 147.76 | 0.008 | 0.004 | 0.046 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 3,220 | 38.93 | 0.012 | 0.009 | 0.049 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 30,563 | 357.67 | 0.012 | 0.010 | 0.047 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 30,563 | 294.32 | 0.010 | 0.003 | 0.014 | 2.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 29,945 | 209.23 | 0.007 | 0.006 | 0.024 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 10,314 | 1216.62 | 0.118 | 0.010 | 2.333 | 8.3% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 4 | 0.03 | 0.007 | 0.007 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 615 | 114.13 | 0.186 | 0.176 | 1.855 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 618 | 356.50 | 0.577 | 0.176 | 32.833 | 2.4% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 10 | 210.58 | 21.058 | 16.667 | 24.833 | 1.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 652 | 1339.88 | 2.055 | 0.175 | 45.472 | 9.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 13 | 0.05 | 0.004 | 0.003 | 0.009 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 616 | 101.86 | 0.165 | 0.120 | 1.384 | 0.7% | — |

### 工作负载 `fuse_perf_baseline` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `12.934 s` (`12934.0 ms`) vs FUSE 累计活跃 `406.66 ms`（等效并发度 `0.03x`）；占比基准：`FUSE 活跃总耗时` (`406.66 ms`)
- **缓存与流水线命中**：NSCache Attr `99.91%` / Entry `94.87%` / Readlink `N/A` · WriteBehind 聚批 `14.00 MiB` / 直通 `3.16 MiB` (Flush RPC `4`) · ReadAhead 预读 `0.38 MiB` / 命中 `0.20 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 3,398 | 406.66 | 0.120 | 0.002 | 0.878 | 100.0% | 错误返回 1480 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 689 | 246.80 | 0.358 | 0.374 | 1.002 | 60.7% | 占 FUSE 60.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 83 | 58.61 | 0.706 | 0.563 | 16.384 | 14.4% | 占 FUSE 14.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 73 | 34.36 | 0.471 | 0.308 | 16.384 | 8.5% | 占 FUSE 8.4% |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 3.79 | 3.795 | 3.750 | 4.975 | 0.9% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,012 | 404.89 | 0.400 | 0.367 | 14.050 | 98.8% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,012 | 278.76 | 0.275 | 0.270 | 13.807 | 68.0% | Write:0.271ms, Unlink:0.386ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,012 | 195.04 | 0.193 | 0.172 | 3.963 | 47.6% | HTTP Body+Codec 差值均值 0.068 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,012 | 126.12 | 0.125 | 0.097 | 3.963 | 30.8% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 679 | 76.08 | 0.112 | 0.090 | 0.702 | 18.6% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 83 | 32.74 | 0.394 | 0.253 | 3.963 | 8.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 935 | 1.99 | 0.002 | 0.003 | 0.008 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 77 | 1.70 | 0.022 | 0.019 | 0.098 | 0.4% | read(68), readdirplus(5), lookup(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 832 | 6.14 | 0.007 | 0.003 | 0.047 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 6 | 0.17 | 0.028 | 0.025 | 0.097 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 935 | 8.62 | 0.009 | 0.007 | 0.047 | 2.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 935 | 3.81 | 0.004 | 0.003 | 0.014 | 0.9% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 845 | 6.84 | 0.008 | 0.008 | 0.025 | 1.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 672 | 15.81 | 0.024 | 0.009 | 0.285 | 3.9% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 51 | 0.53 | 0.010 | 0.010 | 0.037 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 82 | 15.06 | 0.184 | 0.180 | 1.270 | 3.7% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 83 | 18.60 | 0.224 | 0.140 | 3.963 | 4.5% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 83 | 21.34 | 0.257 | 0.099 | 4.308 | 5.2% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.01 | 0.005 | 0.008 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 82 | 12.99 | 0.158 | 0.143 | 1.718 | 3.2% | — |

### 工作负载 `jfs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `6.755 s` (`6755.0 ms`) vs FUSE 累计活跃 `16059.59 ms`（等效并发度 `2.38x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`16059.59 ms` / RPC `63552.29 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `58.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4732.00 MiB` / 命中 `4076.25 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 52,853 | 16059.59 | 0.304 | 0.018 | 7.212 | 100.0% | 错误返回 9420 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 9768.94 | 0.294 | 0.022 | 4.007 | 60.8% | 占 FUSE 60.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,503 | 5793.13 | 1.287 | 0.126 | 22.784 | 36.1% | 占 FUSE 36.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 404 | 187.66 | 0.465 | 0.436 | 1.024 | 1.2% | 占 FUSE 1.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 657 | 4856.43 | 7.392 | 5.291 | 42.179 | 30.2% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 92.84 | 23.211 | 25.000 | 49.500 | 0.6% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 181 | 1926.63 | 10.644 | 6.761 | 77.375 | 12.0% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 3,871 | 63552.29 | 16.418 | 17.631 | 94.131 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,869 | 63468.17 | 16.404 | 17.612 | 94.131 | 99.9% | 非 OK 状态 34 次 · 异步并发流水线 (累计为 FUSE 活跃 3.96x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,866 | 52091.89 | 13.474 | 15.816 | 54.896 | 82.0% | Write:18.229ms, Read:15.857ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,867 | 35508.65 | 9.182 | 8.259 | 64.200 | 55.9% | HTTP Body+Codec 差值均值 6.239 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,866 | 11376.28 | 2.943 | 1.797 | 45.738 | 17.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 10279.65 | 7.179 | 4.041 | 45.738 | 16.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,622 | 1029.13 | 0.634 | 0.684 | 2.341 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 1264.98 | 0.564 | 0.003 | 13.525 | 2.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 298 | 1963.39 | 6.589 | 4.198 | 35.100 | 3.1% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,625 | 978.79 | 0.602 | 0.677 | 2.271 | 1.5% | read(1622), readdirplus(1), getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,215 | 19.60 | 0.016 | 0.018 | 0.047 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,429 | 17.89 | 0.007 | 0.007 | 0.029 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,072 | 27.84 | 0.013 | 0.011 | 0.074 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,072 | 820.91 | 0.396 | 0.004 | 8.822 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,072 | 52.24 | 0.025 | 0.009 | 0.049 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 19,428 | 876.57 | 0.045 | 0.042 | 0.100 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,433 | 2068.13 | 1.443 | 1.492 | 4.685 | 3.3% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2173.50 | 33.961 | 37.500 | 49.750 | 3.4% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 20.20 | 6.734 | 8.631 | 12.627 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`351,025` 次（错误返回 `93,632` 次），FUSE 累计活跃 `40739.18 ms`，读 `5203.65 MiB` / 写 `8904.25 MiB`
- **NamespaceCache 命中率**：Attr `97.13%` · Entry `99.01%` · Readlink `N/A` · 负缓存命中 `27,878` · 失效 `68,173`
- **WriteBehind 流水线**：聚批写 `8674.00 MiB` (`8,674` 次) · 直通写 `230.25 MiB` (`20,479` 次) · Flush RPC `2,180` (错误 `0`) · 背压等待 `781` 次 (`5002.02 ms`) · 屏障等待 `23` 次 (`203.49 ms`)
- **ReadAhead 预读**：触发 `1,268` 次 · 预读 `5572.38 MiB` · 命中 `4841.07 MiB` · 丢弃 `741.05 MiB` · 等待 `202` 次 (`2056.90 ms`)
- **Inode 锁与内核队列**：快速获取 `28,436` · 竞争获取 `2`（累计等待 `0.26 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `Write` | 29,160 | 12599.99 | 30.9% | 432.1 | 296.7 | 511.9 | 6826.7 | EINTR:7 |
| `Read` | 43,979 | 12435.51 | 30.5% | 282.8 | 24.1 | 797.8 | 3494.1 | — |
| `Create` | 12,315 | 3508.77 | 8.6% | 284.9 | 280.3 | 495.5 | 726.9 | — |
| `Unlink` | 12,271 | 3298.88 | 8.1% | 268.8 | 225.9 | 482.8 | 615.3 | — |
| `ReadDirPlus` | 14,532 | 1779.72 | 4.4% | 122.5 | 6.2 | 452.9 | 508.6 | — |
| `Rmdir` | 6,254 | 1760.20 | 4.3% | 281.4 | 293.2 | 498.2 | 779.1 | — |
| `Fsync` | 1,319 | 1553.51 | 3.8% | 1177.8 | 412.4 | 950.9 | 40478.1 | — |
| `Mkdir` | 6,254 | 1543.89 | 3.8% | 246.9 | 224.0 | 477.3 | 508.5 | — |
| `Rename` | 3,104 | 769.98 | 1.9% | 248.1 | 214.6 | 470.1 | 510.7 | — |
| `OpenDir` | 6,725 | 702.77 | 1.7% | 104.5 | 6.9 | 312.4 | 473.6 | — |
| `Lookup` | 31,863 | 164.21 | 0.4% | 5.2 | 1.8 | 7.2 | 15.6 | ENOENT:24940 |
| `Flush` | 16,077 | 156.66 | 0.4% | 9.7 | 1.7 | 6.5 | 13.1 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `80,564` 次 · 正常 (`OK`) `80,442` 次 · 业务/协议非 OK `122` 次 (`0.15%`，分布 `{"NOT_FOUND": 122}`)
- **客户端 RPC 状态码拆分**：总调用 `80,566` 次 · 正常 (`OK`) `80,410` 次 · 非 OK `156` 次 (`0.19%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Write` | 22,659 | — | 27971.63 | 1.234 | 0.144 | 8.054 | 25.523 |
| `http_outer:Read` | 6,439 | — | 18979.09 | 2.948 | 0.342 | 19.590 | 24.971 |
| `http_outer:Create` | 12,315 | — | 1194.70 | 0.097 | 0.090 | 0.232 | 0.248 |
| `http_outer:Fsync` | 1,319 | — | 1192.11 | 0.904 | 0.363 | 0.494 | 35.021 |
| `http_outer:Unlink` | 12,271 | — | 1008.55 | 0.082 | 0.077 | 0.214 | 0.246 |
| `http_outer:Rmdir` | 6,254 | — | 566.49 | 0.091 | 0.084 | 0.223 | 0.245 |
| `http_outer:ReadDirPlus` | 6,429 | — | 495.37 | 0.077 | 0.074 | 0.186 | 0.248 |
| `http_outer:Mkdir` | 6,254 | — | 455.82 | 0.073 | 0.073 | 0.178 | 0.237 |
| `http_outer:Rename` | 3,104 | — | 232.22 | 0.075 | 0.071 | 0.204 | 0.244 |
| `http_outer:GetAttr` | 3,083 | — | 167.97 | 0.054 | 0.050 | 0.097 | 0.186 |
| `http_outer:Lookup` | 320 | — | 33.88 | 0.106 | 0.098 | 0.236 | 0.249 |
| `http_outer:SetAttr` | 72 | — | 7.70 | 0.107 | 0.100 | 0.235 | 0.247 |
| `http_outer:CreateDisk` | 1 | — | 4.71 | 4.714 | 3.750 | 4.875 | 4.975 |
| `http_outer:Link` | 18 | — | 2.00 | 0.111 | 0.138 | 0.239 | 0.248 |
| `http_outer:StatFS` | 11 | — | 1.38 | 0.125 | 0.138 | 0.362 | 0.473 |
| `http_outer:FsyncDir` | 7 | — | 1.02 | 0.146 | 0.175 | 0.242 | 0.249 |
| `http_outer:Symlink` | 6 | — | 0.60 | 0.101 | 0.088 | 0.227 | 0.245 |
| `http_outer:MountDisk` | 1 | — | 0.50 | 0.496 | 0.375 | 0.487 | 0.497 |
| `http_outer:ReclaimOrphan` | 2 | — | 0.34 | 0.172 | 0.175 | 0.242 | 0.249 |
| `rpc:Write` | 22,659 | 22,659 / 0 | 15197.23 | 0.671 | 0.079 | 3.356 | 17.251 |
| `rpc:Read` | 6,438 | 6,438 / 0 | 2113.07 | 0.328 | 0.167 | 0.992 | 2.377 |
| `rpc:Fsync` | 1,319 | 1,319 / 0 | 1131.09 | 0.858 | 0.221 | 0.485 | 35.021 |
| `rpc:Create` | 12,315 | 12,315 / 0 | 738.27 | 0.060 | 0.060 | 0.099 | 0.214 |
| `rpc:Unlink` | 12,271 | 12,271 / 0 | 653.79 | 0.053 | 0.046 | 0.121 | 0.229 |
| `rpc:Rmdir` | 6,254 | 6,254 / 0 | 377.36 | 0.060 | 0.065 | 0.099 | 0.209 |
| `rpc:ReadDirPlus` | 6,429 | 6,429 / 0 | 265.67 | 0.041 | 0.038 | 0.090 | 0.211 |
| `rpc:Mkdir` | 6,254 | 6,254 / 0 | 264.76 | 0.042 | 0.040 | 0.092 | 0.141 |
| `rpc:Rename` | 3,104 | 3,104 / 0 | 142.22 | 0.046 | 0.041 | 0.099 | 0.217 |
| `rpc:GetAttr` | 3,083 | 3,083 / 0 | 74.00 | 0.024 | 0.018 | 0.047 | 0.077 |
| `rpc:Lookup` | 320 | 198 / 122 | 17.16 | 0.054 | 0.049 | 0.100 | 0.220 |
| `rpc:SetAttr` | 72 | 72 / 0 | 4.63 | 0.064 | 0.067 | 0.115 | 0.223 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:phase_b_mutate` | 64,112 | 1370.27 | 0.021 | 0.003 | 0.007 | 0.021 |
| `actor_phase:phase_a_validate` | 64,112 | 716.00 | 0.011 | 0.009 | 0.030 | 0.047 |
| `actor_phase:pack_sync_barrier` | 1,319 | 703.95 | 0.534 | 0.175 | 0.246 | 31.681 |
| `actor_phase:wal_apply_nosync` | 62,785 | 456.34 | 0.007 | 0.006 | 0.018 | 0.024 |
| `actor_phase:dead_pack_remove` | 21 | 277.24 | 13.202 | 11.875 | 23.688 | 24.738 |
| `actor_phase:wal_apply_sync` | 1,313 | 248.71 | 0.189 | 0.176 | 0.246 | 1.812 |
| `read_op:read` | 6,438 | 1986.70 | 0.309 | 0.164 | 0.977 | 2.351 |
| `read_op:readdirplus` | 6,429 | 118.53 | 0.018 | 0.016 | 0.044 | 0.092 |
| `read_op:getattr` | 3,094 | 11.88 | 0.004 | 0.003 | 0.010 | 0.021 |
| `read_op:lookup` | 320 | 4.56 | 0.014 | 0.013 | 0.041 | 0.049 |
| `read_op:statfs` | 12 | 0.00 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:point_get` | 47,935 | 322.71 | 0.007 | 0.004 | 0.024 | 0.043 |
| `kv_read:prefix_scan` | 8,583 | 103.40 | 0.012 | 0.010 | 0.024 | 0.048 |
| `control_open_actor:ok` | 1 | 2.41 | 2.413 | 1.750 | 2.425 | 2.485 |
| `actor_queue_wait` | 64,282 | 1348.67 | 0.021 | 0.003 | 0.005 | 0.010 |
| `inode_lock_wait` | 551 | 2070.16 | 3.757 | 0.904 | 20.236 | 24.892 |
| `blob_pread` | 26,490 | 1752.29 | 0.066 | 0.044 | 0.211 | 0.477 |
| `blob_pwrite` | 22,689 | 4531.27 | 0.200 | 0.011 | 1.778 | 2.468 |
| `blob_rmw_read` | 59 | 0.58 | 0.010 | 0.009 | 0.024 | 0.035 |
| `pebble_wal_fsync` | 1,321 | 241.55 | 0.183 | 0.127 | 0.196 | 2.089 |
| `blob_sync:pack_data` | 1,451 | 4871.20 | 3.357 | 0.178 | 36.565 | 47.313 |
| `blob_sync:packs_dir` | 27 | 0.12 | 0.004 | 0.003 | 0.009 | 0.021 |

- **Pebble 状态**：BlockCache 命中率 `99.27%` · ReadAmp `1.0` · WriteStall `0.0` · WAL 写入 `22366726.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 192978210.0, "falloc_zero": 0.0, "full_block": 9143582720.0, "rmw": 9016106.0, "zero_elide": 8.0}` · 读源块计数 `{"disk_pack": 26490.0, "hot_cache": 2087.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`1399` 条（跨 Client+Server 全链路 `956` 条，异常链路 `104` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `375a8c353706c2ac` | 92.81 | `bowl-client:read_ahead.Prefetch` | 83.06 | 0.98 | 82.08 | `bowl-client:storage.v1.StorageService/Read` (83.06 ms) |
| `dfb5b6235019e7ce` | 79.56 | `bowl-client:write_behind.FlushChunk` | 79.51 | 30.70 | 48.81 | `bowl-client:storage.v1.StorageService/Write` (79.51 ms) |
| `9aded1d0acff00ae` | 77.96 | `bowl-client:write_behind.FlushChunk` | 77.90 | 35.26 | 42.65 | `bowl-client:storage.v1.StorageService/Write` (77.90 ms) |
| `84b3532889e074d4` | 74.10 | `bowl-client:write_behind.FlushChunk` | 74.07 | 11.54 | 62.54 | `bowl-client:storage.v1.StorageService/Write` (74.07 ms) |
| `eaf81b4a1b8444f0` | 73.16 | `bowl-client:write_behind.FlushChunk` | 73.09 | 22.22 | 50.87 | `bowl-client:storage.v1.StorageService/Write` (73.09 ms) |
| `435384f0e9b0d7f4` | 72.57 | `bowl-client:write_behind.FlushChunk` | 72.51 | 27.51 | 45.00 | `bowl-client:storage.v1.StorageService/Write` (72.51 ms) |
| `438fbc1c06109faa` | 72.11 | `bowl-client:write_behind.FlushChunk` | 72.07 | 49.17 | 22.90 | `bowl-client:storage.v1.StorageService/Write` (72.07 ms) |
| `e98801f75bbc7635` | 68.91 | `bowl-client:write_behind.FlushChunk` | 68.88 | 10.66 | 58.23 | `bowl-client:storage.v1.StorageService/Write` (68.88 ms) |
| `ea2dacea018c06e0` | 68.61 | `bowl-client:write_behind.FlushChunk` | 68.57 | 35.11 | 33.47 | `bowl-client:storage.v1.StorageService/Write` (68.57 ms) |
| `d066225b038a0926` | 67.75 | `bowl-client:fuse.Read` | 67.66 | 1.58 | 66.09 | `bowl-client:storage.v1.StorageService/Read` (67.66 ms) |
| `5199f0c2f07e1264` | 67.69 | `bowl-client:write_behind.FlushChunk` | 67.66 | 27.10 | 40.56 | `bowl-client:storage.v1.StorageService/Write` (67.66 ms) |
| `cb19d5410ad0c889` | 67.62 | `bowl-client:write_behind.FlushChunk` | 67.59 | 32.45 | 35.14 | `bowl-client:storage.v1.StorageService/Write` (67.59 ms) |
| `5a2b8653750bd4cb` | 66.16 | `bowl-client:write_behind.FlushChunk` | 66.13 | 28.75 | 37.38 | `bowl-client:storage.v1.StorageService/Write` (66.13 ms) |
| `403a5db08222af5e` | 64.53 | `bowl-client:write_behind.FlushChunk` | 64.50 | 32.17 | 32.34 | `bowl-client:storage.v1.StorageService/Write` (64.50 ms) |
| `ba028807ee96fff8` | 64.41 | `bowl-client:fuse.Fsync` | 55.09 | 52.94 | 2.14 | `bowl-client:storage.v1.StorageService/Fsync` (55.09 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`50` · **服务端 Warn/Error 数**：`42` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 8, "flush_begin": 2, "table_created": 4, "flush_end": 2, "compaction_begin": 2, "wal_deleted": 3, "compaction_end": 2}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `macro_go_build_cpu.pb.gz` | `0.23s 15.65% 15.65%      0.23s 15.65%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      570ms 38.78%  net/http.HandlerFunc.ServeHTTP` | `pprof_top/server_macro_go_build_cpu.txt` |
| `server` | `macro_go_build_heap.pb.gz` | `33.30MB 25.91% 25.91%    33.30MB 25.91%  github.com/starsrc/bowl/services/storage/internal/store.(*BlobStore).extendHotShard` | `0     0%     0%    88.04MB 68.49%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_macro_go_build_heap.txt` |
| `server` | `macro_go_build_mutex.pb.gz` | `—` | `—` | `pprof_top/server_macro_go_build_mutex.txt` |
| `server` | `seq_read_cpu.pb.gz` | `1.09s 61.24% 61.24%      1.09s 61.24%  internal/runtime/syscall/linux.Syscall6` | `1090ms 61.24% 61.24%     1090ms 61.24%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_seq_read_cpu.txt` |
| `server` | `seq_read_heap.pb.gz` | `96MB 51.64% 51.64%       96MB 51.64%  bytes.growSlice` | `0     0%     0%   165.05MB 88.79%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_seq_read_heap.txt` |
| `server` | `seq_read_mutex.pb.gz` | `—` | `—` | `pprof_top/server_seq_read_mutex.txt` |
| `server` | `small_write_fsync_cpu.pb.gz` | `90ms 13.04% 13.04%       90ms 13.04%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      230ms 33.33%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_small_write_fsync_cpu.txt` |
| `server` | `small_write_fsync_heap.pb.gz` | `72MB 28.37% 28.37%       72MB 28.37%  bytes.growSlice` | `0     0%     0%   233.65MB 92.07%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_small_write_fsync_heap.txt` |
| `server` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | `pprof_top/server_small_write_fsync_mutex.txt` |
| `server` | `sqlite_wal_cpu.pb.gz` | `10ms 33.33% 33.33%       10ms 33.33%  github.com/prometheus/client_model/go.(*Gauge).GetValue (inline)` | `0     0%     0%       10ms 33.33%  github.com/cockroachdb/pebble/v2.(*ConcurrencyLimitScheduler).periodicGranter` | `pprof_top/server_sqlite_wal_cpu.txt` |
| `server` | `sqlite_wal_heap.pb.gz` | `47.20MB 27.41% 27.41%    47.20MB 27.41%  github.com/cockroachdb/pebble/v2/internal/manual.New (inline)` | `0     0%     0%   115.71MB 67.19%  connectrpc.com/connect.(*Handler).ServeHTTP` | `pprof_top/server_sqlite_wal_heap.txt` |
| `server` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | `pprof_top/server_sqlite_wal_mutex.txt` |
| `client` | `macro_go_build_cpu.pb.gz` | `190ms 20.00% 20.00%      190ms 20.00%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      490ms 51.58%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_macro_go_build_cpu.txt` |
| `client` | `macro_go_build_heap.pb.gz` | `64.02MB 54.90% 54.90%    64.02MB 54.90%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   103.01MB 88.34%  sync.(*Pool).Get` | `pprof_top/client_macro_go_build_heap.txt` |
| `client` | `macro_go_build_mutex.pb.gz` | `—` | `—` | `pprof_top/client_macro_go_build_mutex.txt` |
| `client` | `seq_read_cpu.pb.gz` | `0.73s 40.78% 40.78%      0.73s 40.78%  internal/runtime/syscall/linux.Syscall6` | `730ms 40.78% 40.78%      730ms 40.78%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/client_seq_read_cpu.txt` |
| `client` | `seq_read_heap.pb.gz` | `95.15MB 32.86% 32.86%    95.15MB 32.86%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   182.04MB 62.86%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_seq_read_heap.txt` |
| `client` | `seq_read_mutex.pb.gz` | `—` | `—` | `pprof_top/client_seq_read_mutex.txt` |
| `client` | `small_write_fsync_cpu.pb.gz` | `60ms 20.69% 20.69%       60ms 20.69%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      190ms 65.52%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_small_write_fsync_cpu.txt` |
| `client` | `small_write_fsync_heap.pb.gz` | `139.98MB 49.77% 49.77%   139.98MB 49.77%  bytes.growSlice` | `0     0%     0%   221.02MB 78.58%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_small_write_fsync_heap.txt` |
| `client` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | `pprof_top/client_small_write_fsync_mutex.txt` |
| `client` | `sqlite_wal_cpu.pb.gz` | `10ms 33.33% 33.33%       10ms 33.33%  aeshashbody` | `0     0%     0%       30ms   100%  github.com/prometheus/client_golang/prometheus/promhttp.HandlerForTransactional.func1` | `pprof_top/client_sqlite_wal_cpu.txt` |
| `client` | `sqlite_wal_heap.pb.gz` | `64.02MB 53.15% 53.15%    64.02MB 53.15%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   103.01MB 85.52%  sync.(*Pool).Get` | `pprof_top/client_sqlite_wal_heap.txt` |
| `client` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | `pprof_top/client_sqlite_wal_mutex.txt` |
