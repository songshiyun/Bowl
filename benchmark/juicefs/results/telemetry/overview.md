# Bowl `juicefs` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `fio` | 82.475 |
| 2 | `fio_seq_16jobs` | 31.443 |
| 3 | `mdtest` | 29.703 |
| 4 | `mdtest_1p` | 17.393 |
| 5 | `mdtest_4p` | 10.472 |
| 6 | `fio_direct_randwrite` | 10.468 |
| 7 | `fio_seq_read_1job` | 10.462 |
| 8 | `fio_seq_write_1job` | 10.456 |
| 9 | `juicefs_bench` | 6.796 |
| 10 | `fio_direct_seq` | 6.746 |
| 11 | `fio_direct_randread` | 4.200 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `fio_direct_randread` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `4.516 s` (`4516.0 ms`) vs FUSE 累计活跃 `14559.92 ms`（等效并发度 `3.22x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`14559.92 ms` / RPC `14853.67 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `452.00 MiB` / 命中 `44.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 4,118 | 14559.92 | 3.536 | 3.285 | 12.706 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 14559.53 | 3.555 | 3.293 | 12.873 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Open` | 4 | 0.16 | 0.040 | 0.024 | 0.128 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 9 | 0.11 | 0.012 | 0.008 | 0.032 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 16 | 286.74 | 17.921 | 17.000 | 24.840 | 2.0% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 4,342 | 14853.67 | 3.421 | 3.418 | 16.542 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,221 | 14789.89 | 3.504 | 3.459 | 16.777 | 99.6% | 非 OK 状态 120 次 · 异步并发流水线 (累计为 FUSE 活跃 1.02x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,207 | 11811.92 | 2.808 | 3.095 | 12.603 | 79.5% | Read:2.796ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,210 | 9722.59 | 2.309 | 2.003 | 15.133 | 65.5% | HTTP Body+Codec 差值均值 1.601 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,207 | 2977.97 | 0.708 | 0.364 | 4.174 | 20.0% | 非 OK 状态 31 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 4,207 | 2977.97 | 0.708 | 0.364 | 4.174 | 20.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4,176 | 2844.89 | 0.681 | 0.286 | 4.132 | 19.2% | read(4176) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 4,161 | 114.79 | 0.028 | 0.029 | 0.075 | 0.8% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 4,176 | 31.05 | 0.007 | 0.007 | 0.024 | 0.2% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 18,720 | 2571.94 | 0.137 | 0.040 | 2.073 | 17.3% | — |

### 工作负载 `fio_direct_randwrite` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.783 s` (`10783.0 ms`) vs FUSE 累计活跃 `13678.61 ms`（等效并发度 `1.27x`）；占比基准：`FUSE 活跃总耗时` (`13678.61 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `13.00 MiB` / 直通 `4083.00 MiB` (Flush RPC `12`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 12,310 | 13678.61 | 1.111 | 0.005 | 7.584 | 100.0% | 错误返回 8192 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,096 | 13650.31 | 3.333 | 3.262 | 8.023 | 99.8% | 占 FUSE 99.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 4,096 | 26.61 | 0.006 | 0.005 | 0.028 | 0.2% | 占 FUSE 0.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,096 | 1.46 | 0.000 | 0.001 | 0.001 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 12 | 37.06 | 3.089 | 3.333 | 4.967 | 0.3% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,099 | 13490.33 | 3.291 | 3.460 | 9.001 | 98.6% | 非 OK 状态 4 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,099 | 8933.03 | 2.179 | 2.498 | 4.558 | 65.3% | Write:2.180ms, GetXattr:0.771ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,099 | 8219.70 | 2.005 | 1.965 | 4.937 | 60.1% | HTTP Body+Codec 差值均值 0.893 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,099 | 4557.30 | 1.112 | 0.962 | 4.443 | 33.3% | 非 OK 状态 4 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,095 | 4556.45 | 1.113 | 0.963 | 4.443 | 33.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:GetXattr` | 4 | 0.85 | 0.212 | 0.200 | 0.490 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,095 | 166.20 | 0.041 | 0.004 | 0.789 | 1.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 4 | 0.27 | 0.067 | 0.075 | 0.100 | 0.0% | getxattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 8 | 0.25 | 0.032 | 0.025 | 0.099 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 3,865 | 93.97 | 0.024 | 0.019 | 0.073 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 3,865 | 461.33 | 0.119 | 0.008 | 1.518 | 3.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 3,865 | 76.56 | 0.020 | 0.018 | 0.050 | 0.6% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,095 | 2187.10 | 0.534 | 0.440 | 2.249 | 16.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2116.71 | 33.074 | 37.302 | 49.746 | 15.5% | — |

### 工作负载 `fio_direct_seq` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.057 s` (`7057.0 ms`) vs FUSE 累计活跃 `18796.64 ms`（等效并发度 `2.66x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`18796.64 ms` / RPC `70835.65 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4087.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 16,473 | 18796.64 | 1.141 | 0.032 | 29.164 | 100.0% | 错误返回 8212 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 4,096 | 10385.01 | 2.535 | 0.109 | 49.744 | 55.2% | 占 FUSE 55.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,096 | 8288.13 | 2.023 | 0.117 | 29.810 | 44.1% | 占 FUSE 44.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 12 | 87.66 | 7.305 | 0.008 | 65.536 | 0.5% | 占 FUSE 0.5% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 808 | 7806.42 | 9.661 | 7.906 | 44.541 | 41.5% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 84.91 | 21.229 | 17.500 | 49.000 | 0.5% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 732 | 9789.11 | 13.373 | 8.684 | 49.019 | 52.1% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,079 | 70785.35 | 34.048 | 34.986 | 97.270 | 99.9% | 非 OK 状态 1 次 · 异步并发流水线 (累计为 FUSE 活跃 3.77x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,079 | 53272.96 | 25.624 | 28.034 | 48.239 | 75.2% | Read:27.553ms, Write:23.984ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,079 | 47878.20 | 23.029 | 22.711 | 84.364 | 67.6% | HTTP Body+Codec 差值均值 14.606 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,079 | 17512.39 | 8.423 | 6.953 | 49.031 | 24.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,032 | 14426.33 | 13.979 | 11.438 | 49.031 | 20.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,035 | 3085.17 | 2.981 | 2.560 | 9.928 | 4.4% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,040 | 1874.88 | 1.803 | 0.028 | 22.136 | 2.6% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 434 | 3483.48 | 8.026 | 6.667 | 36.437 | 4.9% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,039 | 3042.30 | 2.928 | 2.515 | 9.897 | 4.3% | read(1035), getattr(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,028 | 33.69 | 0.033 | 0.034 | 0.089 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,039 | 10.09 | 0.010 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 813 | 94.78 | 0.117 | 0.078 | 1.401 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 813 | 1270.99 | 1.563 | 0.017 | 16.289 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 813 | 18.86 | 0.023 | 0.018 | 0.077 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 16,400 | 2926.54 | 0.178 | 0.044 | 2.297 | 4.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,095 | 2213.28 | 2.021 | 1.810 | 4.913 | 3.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2154.56 | 33.665 | 37.302 | 49.746 | 3.0% | — |

### 工作负载 `fio_seq_16jobs` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `31.755 s` (`31755.0 ms`) vs FUSE 累计活跃 `577306.14 ms`（等效并发度 `18.18x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`577306.14 ms` / RPC `1118809.11 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `16352.00 MiB` / 直通 `32.00 MiB` (Flush RPC `4096`) · ReadAhead 预读 `27680.00 MiB` / 命中 `15237.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 102,757 | 577306.14 | 5.618 | 0.038 | 114.338 | 100.0% | 错误返回 20560 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 65,536 | 436167.21 | 6.655 | 0.041 | 118.676 | 75.6% | 占 FUSE 75.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 16,384 | 137191.41 | 8.373 | 0.204 | 110.440 | 23.8% | 占 FUSE 23.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 16 | 3869.31 | 241.832 | 262.144 | 524.288 | 0.7% | 占 FUSE 0.7% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 3,643 | 134095.52 | 36.809 | 33.780 | 178.099 | 23.2% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 16 | 1484.56 | 92.785 | 100.000 | 247.000 | 0.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 3,744 | 171019.92 | 45.678 | 46.970 | 130.511 | 29.6% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 11,149 | 1118510.72 | 100.324 | 109.235 | 345.970 | 100.0% | 非 OK 状态 4 次 · 异步并发流水线 (累计为 FUSE 活跃 1.94x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 11,148 | 1039548.17 | 93.250 | 105.151 | 160.470 | 92.9% | Write:143.673ms, Read:64.151ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 11,148 | 457042.56 | 40.998 | 44.395 | 247.600 | 40.9% | HTTP Body+Codec 差值均值 33.915 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 11,148 | 78962.55 | 7.083 | 4.087 | 247.600 | 7.1% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,128 | 64183.15 | 15.548 | 8.935 | 185.500 | 5.7% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 6,956 | 13412.67 | 1.928 | 0.949 | 9.712 | 1.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,176 | 11789.15 | 2.823 | 0.051 | 41.325 | 1.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 435 | 10753.45 | 24.721 | 9.683 | 215.658 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 6,972 | 13131.10 | 1.883 | 0.929 | 9.694 | 1.2% | read(6956), getattr(16) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 6,921 | 201.11 | 0.029 | 0.029 | 0.087 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,988 | 66.60 | 0.010 | 0.008 | 0.027 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,625 | 304.74 | 0.116 | 0.082 | 0.918 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,625 | 4397.22 | 1.675 | 0.021 | 15.156 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,621 | 82.84 | 0.032 | 0.022 | 0.187 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 110,752 | 12466.51 | 0.113 | 0.042 | 1.954 | 1.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,381 | 8750.56 | 1.997 | 1.792 | 4.914 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 3 | 69.77 | 23.258 | 7.500 | 98.500 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 4 | 139.97 | 34.991 | 50.000 | 99.000 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 4.22 | 4.216 | 3.750 | 4.975 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 257 | 8716.53 | 33.916 | 37.500 | 49.846 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 4 | 0.02 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 17.82 | 5.940 | 8.297 | 9.279 | 0.0% | — |

### 工作负载 `fio_seq_read_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.773 s` (`10773.0 ms`) vs FUSE 累计活跃 `3120.88 ms`（等效并发度 `0.29x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`3120.88 ms` / RPC `12657.49 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `4816.00 MiB` / 命中 `4050.25 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 16,391 | 3120.88 | 0.190 | 0.028 | 8.722 | 100.0% | 错误返回 0 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 16,384 | 3120.70 | 0.190 | 0.028 | 8.722 | 100.0% | 占 FUSE 100.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Lookup` | 3 | 0.09 | 0.029 | 0.032 | 0.064 | 0.0% | 占 FUSE 0.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetAttr` | 1 | 0.04 | 0.035 | 0.064 | 0.064 | 0.0% | 占 FUSE 0.0% |
| `2. 客户端等待` | `ReadAhead 预读等待` | 193 | 579.41 | 3.002 | 1.891 | 23.070 | 18.6% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 1,211 | 12657.49 | 10.452 | 10.160 | 24.727 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,210 | 12627.55 | 10.436 | 10.074 | 24.726 | 99.8% | 非 OK 状态 3 次 · 异步并发流水线 (累计为 FUSE 活跃 4.06x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,210 | 10448.23 | 8.635 | 8.326 | 19.773 | 82.5% | Read:8.635ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,210 | 9204.56 | 7.607 | 7.634 | 24.127 | 72.7% | HTTP Body+Codec 差值均值 5.806 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,210 | 2179.32 | 1.801 | 1.748 | 4.953 | 17.2% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,210 | 2179.32 | 1.801 | 1.748 | 4.953 | 17.2% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,210 | 2129.34 | 1.760 | 1.735 | 4.927 | 16.8% | read(1210) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,207 | 27.66 | 0.023 | 0.020 | 0.076 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,211 | 9.61 | 0.008 | 0.008 | 0.024 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 19,313 | 2013.78 | 0.104 | 0.039 | 2.042 | 15.9% | — |

### 工作负载 `fio_seq_write_1job` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.764 s` (`10764.0 ms`) vs FUSE 累计活跃 `503.18 ms`（等效并发度 `0.05x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`503.18 ms` / RPC `8461.31 ms`)
- **缓存与流水线命中**：NSCache Attr `99.98%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4094.00 MiB` / 直通 `2.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 9,234 | 503.18 | 0.054 | 0.006 | 0.252 | 100.0% | 错误返回 5125 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,096 | 455.60 | 0.111 | 0.086 | 1.004 | 90.5% | 占 FUSE 90.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 1 | 36.02 | 36.024 | 65.536 | 65.536 | 7.2% | 占 FUSE 7.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.GetXAttr` | 1,025 | 8.62 | 0.008 | 0.006 | 0.041 | 1.7% | 占 FUSE 1.7% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 89 | 117.73 | 1.323 | 0.837 | 18.325 | 23.4% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 6.12 | 6.119 | 7.500 | 9.950 | 1.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 1,030 | 8443.88 | 8.198 | 8.194 | 49.750 | 99.8% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 16.82x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 1,030 | 5011.90 | 4.866 | 5.680 | 14.349 | 59.2% | Write:4.883ms, Create:0.498ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,030 | 6315.37 | 6.131 | 6.014 | 49.750 | 74.6% | HTTP Body+Codec 差值均值 2.799 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,030 | 3431.97 | 3.332 | 2.515 | 49.750 | 40.6% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,026 | 3402.35 | 3.316 | 2.488 | 9.961 | 40.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 1 | 29.40 | 29.398 | 37.500 | 49.750 | 0.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 1,029 | 3.81 | 0.004 | 0.003 | 0.022 | 0.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 307 | 163.38 | 0.532 | 0.189 | 4.983 | 1.9% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1 | 0.01 | 0.006 | 0.008 | 0.010 | 0.0% | getattr(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 1,029 | 63.65 | 0.062 | 0.061 | 0.229 | 0.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 1,029 | 521.15 | 0.506 | 0.012 | 4.143 | 6.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,028 | 31.12 | 0.030 | 0.017 | 0.046 | 0.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,026 | 2075.17 | 2.023 | 1.845 | 4.863 | 24.5% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 1 | 3.13 | 3.133 | 3.750 | 4.975 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 1 | 26.38 | 26.384 | 37.500 | 49.750 | 0.3% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 2.78 | 2.782 | 3.750 | 4.975 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 65 | 2158.22 | 33.203 | 37.305 | 49.746 | 25.5% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 2 | 17.42 | 8.710 | 3.100 | 14.767 | 0.2% | — |

### 工作负载 `juicefs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.110 s` (`7110.0 ms`) vs FUSE 累计活跃 `18083.62 ms`（等效并发度 `2.54x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`18083.62 ms` / RPC `69579.71 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.94%` / Readlink `N/A` · WriteBehind 聚批 `4088.00 MiB` / 直通 `58.00 MiB` (Flush RPC `1024`) · ReadAhead 预读 `4864.00 MiB` / 命中 `4072.12 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 52,868 | 18083.62 | 0.342 | 0.015 | 10.756 | 100.0% | 错误返回 9420 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 10812.11 | 0.326 | 0.019 | 7.048 | 59.8% | 占 FUSE 59.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Write` | 4,503 | 6766.40 | 1.503 | 0.120 | 25.836 | 37.4% | 占 FUSE 37.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 404 | 213.65 | 0.529 | 0.447 | 2.926 | 1.2% | 占 FUSE 1.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 684 | 5838.10 | 8.535 | 7.132 | 38.600 | 32.3% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 4 | 54.12 | 13.530 | 8.333 | 49.000 | 0.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 207 | 2317.73 | 11.197 | 7.500 | 49.930 | 12.8% | — |
| `3. 客户端 RPC (含重试)` | `bowl_client_rpc_logical` | 3,894 | 69579.71 | 17.868 | 18.900 | 95.837 | 100.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,893 | 69511.08 | 17.855 | 18.880 | 95.837 | 99.9% | 非 OK 状态 25 次 · 异步并发流水线 (累计为 FUSE 活跃 3.85x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,893 | 55590.43 | 14.280 | 16.691 | 48.379 | 79.9% | Read:17.953ms, Write:17.994ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,893 | 44038.92 | 11.312 | 10.281 | 76.133 | 63.3% | HTTP Body+Codec 差值均值 7.736 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,893 | 13920.66 | 3.576 | 2.189 | 47.458 | 20.0% | 非 OK 状态 2 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 12739.42 | 8.896 | 5.101 | 47.458 | 18.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,648 | 1103.49 | 0.670 | 0.704 | 2.399 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 1523.57 | 0.680 | 0.003 | 15.391 | 2.2% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 342 | 2599.29 | 7.600 | 6.477 | 24.941 | 3.7% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,651 | 1044.54 | 0.633 | 0.691 | 2.363 | 1.5% | read(1647), readdirplus(1), lookup(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,239 | 21.48 | 0.017 | 0.018 | 0.047 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,456 | 21.48 | 0.009 | 0.007 | 0.041 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,045 | 34.67 | 0.017 | 0.013 | 0.086 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,045 | 932.46 | 0.456 | 0.005 | 9.635 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,045 | 38.12 | 0.019 | 0.009 | 0.048 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 19,812 | 931.97 | 0.047 | 0.043 | 0.101 | 1.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,433 | 2207.02 | 1.540 | 1.527 | 4.810 | 3.2% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 2184.95 | 34.140 | 37.500 | 49.750 | 3.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 7.48 | 2.493 | 3.150 | 4.097 | 0.0% | — |

### 工作负载 `mdtest_1p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `17.709 s` (`17709.0 ms`) vs FUSE 累计活跃 `14178.06 ms`（等效并发度 `0.80x`）；占比基准：`FUSE 活跃总耗时` (`14178.06 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.41%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 264,902 | 14178.06 | 0.054 | 0.003 | 0.483 | 100.0% | 错误返回 26439 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 13,996 | 3808.35 | 0.272 | 0.267 | 0.509 | 26.9% | 占 FUSE 26.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 13,997 | 3540.03 | 0.253 | 0.231 | 0.507 | 25.0% | 占 FUSE 25.0% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 12,440 | 3294.56 | 0.265 | 0.255 | 0.509 | 23.2% | 占 FUSE 23.2% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 52,875 | 12721.27 | 0.241 | 0.209 | 0.995 | 89.7% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 52,875 | 9994.19 | 0.189 | 0.161 | 0.746 | 70.5% | Rmdir:0.194ms, Mkdir:0.187ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 52,875 | 4401.16 | 0.083 | 0.080 | 0.249 | 31.0% | HTTP Body+Codec 差值均值 0.031 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 52,875 | 2727.07 | 0.052 | 0.048 | 0.249 | 19.2% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 13,996 | 827.75 | 0.059 | 0.056 | 0.224 | 5.8% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 13,997 | 697.48 | 0.050 | 0.047 | 0.179 | 4.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 52,873 | 64.56 | 0.001 | 0.003 | 0.006 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.04 | 0.013 | 0.015 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.03 | 0.011 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 52,873 | 773.49 | 0.015 | 0.014 | 0.049 | 5.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 52,873 | 76.15 | 0.001 | 0.003 | 0.005 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 52,873 | 299.71 | 0.006 | 0.004 | 0.024 | 2.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.60 | 0.602 | 0.650 | 0.699 | 0.0% | — |

### 工作负载 `mdtest_4p` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `10.777 s` (`10777.0 ms`) vs FUSE 累计活跃 `1771.14 ms`（等效并发度 `0.16x`）；占比基准：`FUSE 活跃总耗时` (`1771.14 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.95%` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `97.02%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 17,957 | 1771.14 | 0.099 | 0.003 | 0.801 | 100.0% | 错误返回 2136 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Mkdir` | 1,094 | 469.03 | 0.429 | 0.393 | 1.229 | 26.5% | 占 FUSE 26.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Rmdir` | 1,093 | 445.01 | 0.407 | 0.390 | 2.389 | 25.1% | 占 FUSE 25.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 1,040 | 405.64 | 0.390 | 0.393 | 0.981 | 22.9% | 占 FUSE 22.9% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 4,269 | 1567.89 | 0.367 | 0.369 | 1.816 | 88.5% | 非 OK 状态 1 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 4,269 | 1302.87 | 0.305 | 0.313 | 1.570 | 73.6% | Mkdir:0.341ms, Rmdir:0.309ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 4,269 | 496.73 | 0.116 | 0.114 | 0.456 | 28.0% | HTTP Body+Codec 差值均值 0.054 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 4,269 | 265.02 | 0.062 | 0.056 | 0.249 | 15.0% | 非 OK 状态 1 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Rmdir` | 1,093 | 77.15 | 0.071 | 0.064 | 0.246 | 4.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Mkdir` | 1,094 | 71.91 | 0.066 | 0.059 | 0.247 | 4.1% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 4,267 | 17.02 | 0.004 | 0.003 | 0.043 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3 | 0.04 | 0.012 | 0.015 | 0.050 | 0.0% | lookup(1), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 3 | 0.03 | 0.010 | 0.008 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 4,166 | 58.79 | 0.014 | 0.014 | 0.049 | 3.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 4,166 | 5.86 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 4,166 | 21.93 | 0.005 | 0.004 | 0.022 | 1.2% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 1 | 0.26 | 0.255 | 0.250 | 0.299 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`497,130` 次（错误返回 `80,087` 次），FUSE 累计活跃 `662077.70 ms`，读 `32818.01 MiB` / 写 `32818.00 MiB`
- **NamespaceCache 命中率**：Attr `99.99%` · Entry `99.99%` · Readlink `N/A` · 负缓存命中 `29,065` · 失效 `102,108`
- **WriteBehind 流水线**：聚批写 `28635.00 MiB` (`28,635` 次) · 直通写 `4183.00 MiB` (`4,533` 次) · Flush RPC `7,180` (错误 `0`) · 背压等待 `5,224` 次 (`147857.77 ms`) · 屏障等待 `37` 次 (`1666.78 ms`)
- **ReadAhead 预读**：触发 `6,654` 次 · 预读 `41908.00 MiB` · 命中 `27490.38 MiB` · 丢弃 `16668.75 MiB` · 等待 `4,892` 次 (`183992.91 ms`)
- **Inode 锁与内核队列**：快速获取 `41,744` · 竞争获取 `0`（累计等待 `0.00 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `Read` | 123,297 | 475044.56 | 71.8% | 3852.8 | 31.4 | 33173.8 | 107395.4 | — |
| `Write` | 33,175 | 166351.85 | 25.1% | 5014.4 | 186.5 | 37476.7 | 87785.9 | EINTR:7 |
| `Rmdir` | 15,093 | 4255.36 | 0.6% | 281.9 | 281.1 | 493.9 | 611.5 | — |
| `Mkdir` | 15,093 | 4012.58 | 0.6% | 265.9 | 238.7 | 486.8 | 511.6 | — |
| `Create` | 13,905 | 3921.80 | 0.6% | 282.0 | 278.8 | 497.6 | 802.5 | — |
| `Fsync` | 17 | 3905.33 | 0.6% | 229725.4 | 262144.0 | 524288.0 | 524288.0 | — |
| `Unlink` | 13,905 | 3519.58 | 0.5% | 253.1 | 218.7 | 473.5 | 509.1 | — |
| `Lookup` | 79,098 | 228.64 | 0.0% | 2.9 | 2.5 | 7.1 | 12.7 | ENOENT:29069 |
| `Flush` | 27,841 | 228.17 | 0.0% | 8.2 | 1.5 | 4.7 | 9.1 | — |
| `GetAttr` | 53,002 | 170.74 | 0.0% | 3.2 | 2.6 | 7.4 | 14.5 | — |
| `GetXAttr` | 17,836 | 162.82 | 0.0% | 9.1 | 5.5 | 15.7 | 31.3 | ENODATA:17836 |
| `Release` | 27,841 | 136.93 | 0.0% | 4.9 | 2.6 | 7.1 | 14.1 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `84,847` 次 · 正常 (`OK`) `84,807` 次 · 业务/协议非 OK `40` 次 (`0.05%`，分布 `{"UNKNOWN": 32, "NOT_FOUND": 8}`)
- **客户端 RPC 状态码拆分**：总调用 `84,861` 次 · 正常 (`OK`) `84,701` 次 · 非 OK `160` 次 (`0.19%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Read` | 15,059 | — | 410047.15 | 27.229 | 22.378 | 91.105 | 98.288 |
| `http_outer:Write` | 11,713 | — | 170846.36 | 14.586 | 8.456 | 47.336 | 111.750 |
| `http_outer:Rmdir` | 15,093 | — | 1413.71 | 0.094 | 0.086 | 0.229 | 0.248 |
| `http_outer:Fsync` | 17 | — | 1399.42 | 82.319 | 122.500 | 237.250 | 247.450 |
| `http_outer:Create` | 13,905 | — | 1279.41 | 0.092 | 0.084 | 0.226 | 0.249 |
| `http_outer:Mkdir` | 15,093 | — | 1277.48 | 0.085 | 0.080 | 0.216 | 0.245 |
| `http_outer:Unlink` | 13,905 | — | 1111.61 | 0.080 | 0.074 | 0.202 | 0.243 |
| `http_outer:CreateDisk` | 1 | — | 4.56 | 4.561 | 3.750 | 4.875 | 4.975 |
| `http_outer:ReadDirPlus` | 6 | — | 2.31 | 0.385 | 0.375 | 0.850 | 0.970 |
| `http_outer:Fallocate` | 21 | — | 2.29 | 0.109 | 0.129 | 0.238 | 0.248 |
| `http_outer:GetXattr` | 4 | — | 1.79 | 0.449 | 0.417 | 0.900 | 0.980 |
| `http_outer:GetAttr` | 22 | — | 1.64 | 0.074 | 0.079 | 0.209 | 0.242 |
| `http_outer:Lookup` | 4 | — | 1.15 | 0.288 | 0.200 | 0.900 | 0.980 |
| `http_outer:StatFS` | 6 | — | 0.92 | 0.154 | 0.138 | 0.239 | 0.248 |
| `http_outer:MountDisk` | 1 | — | 0.46 | 0.463 | 0.375 | 0.487 | 0.497 |
| `rpc:Write` | 11,713 | 11,713 / 0 | 99307.69 | 8.478 | 3.161 | 34.835 | 92.232 |
| `rpc:Read` | 15,056 | 15,024 / 32 | 22758.62 | 1.512 | 0.896 | 5.065 | 9.294 |
| `rpc:Fsync` | 17 | 17 / 0 | 1393.84 | 81.990 | 122.500 | 237.250 | 247.450 |
| `rpc:Rmdir` | 15,093 | 15,093 / 0 | 905.41 | 0.060 | 0.057 | 0.128 | 0.228 |
| `rpc:Mkdir` | 15,093 | 15,093 / 0 | 769.57 | 0.051 | 0.047 | 0.097 | 0.198 |
| `rpc:Create` | 13,905 | 13,905 / 0 | 765.14 | 0.055 | 0.050 | 0.099 | 0.221 |
| `rpc:Unlink` | 13,905 | 13,905 / 0 | 691.60 | 0.050 | 0.042 | 0.096 | 0.203 |
| `rpc:CreateDisk` | 1 | 1 / 0 | 4.33 | 4.329 | 3.750 | 4.875 | 4.975 |
| `rpc:ReadDirPlus` | 6 | 6 / 0 | 1.40 | 0.233 | 0.213 | 0.462 | 0.492 |
| `rpc:Fallocate` | 21 | 21 / 0 | 1.23 | 0.059 | 0.053 | 0.100 | 0.218 |
| `rpc:GetXattr` | 4 | 0 / 4 | 0.85 | 0.212 | 0.200 | 0.450 | 0.490 |
| `rpc:GetAttr` | 22 | 22 / 0 | 0.70 | 0.032 | 0.033 | 0.082 | 0.096 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:phase_b_mutate` | 67,442 | 7683.25 | 0.114 | 0.003 | 0.019 | 3.997 |
| `actor_phase:phase_a_validate` | 67,442 | 1447.37 | 0.021 | 0.016 | 0.062 | 0.166 |
| `actor_phase:wal_apply_nosync` | 67,436 | 587.22 | 0.009 | 0.005 | 0.023 | 0.045 |
| `actor_phase:pack_sync_barrier` | 5 | 166.35 | 33.270 | 43.750 | 93.750 | 98.750 |
| `actor_phase:wal_apply_sync` | 5 | 73.08 | 14.616 | 3.750 | 87.500 | 97.500 |
| `actor_phase:dead_pack_remove` | 2 | 7.00 | 3.499 | 3.750 | 4.875 | 4.975 |
| `read_op:read` | 15,024 | 22191.78 | 1.477 | 0.877 | 4.988 | 9.263 |
| `read_op:readdirplus` | 6 | 0.91 | 0.152 | 0.150 | 0.425 | 0.485 |
| `read_op:getxattr` | 4 | 0.27 | 0.067 | 0.075 | 0.098 | 0.100 |
| `read_op:getattr` | 28 | 0.17 | 0.006 | 0.004 | 0.021 | 0.024 |
| `read_op:lookup` | 4 | 0.12 | 0.030 | 0.033 | 0.048 | 0.050 |
| `read_op:statfs` | 7 | 0.00 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:prefix_scan` | 14,561 | 399.11 | 0.027 | 0.026 | 0.049 | 0.082 |
| `kv_read:point_get` | 15,901 | 139.37 | 0.009 | 0.008 | 0.022 | 0.028 |
| `control_open_actor:ok` | 1 | 2.52 | 2.518 | 3.750 | 4.875 | 4.975 |
| `actor_queue_wait` | 69,748 | 15439.27 | 0.221 | 0.003 | 0.045 | 6.804 |
| `inode_lock_wait` | 1,518 | 16999.60 | 11.199 | 4.754 | 52.583 | 130.158 |
| `blob_pread` | 184,997 | 20910.74 | 0.113 | 0.042 | 0.600 | 1.970 |
| `blob_pwrite` | 12,030 | 17433.12 | 1.449 | 1.310 | 3.479 | 4.783 |
| `pebble_wal_fsync` | 14 | 44.06 | 3.147 | 0.400 | 13.320 | 14.512 |
| `blob_sync:pack_data` | 514 | 17330.96 | 33.718 | 37.426 | 48.787 | 49.797 |
| `blob_sync:packs_dir` | 5 | 0.02 | 0.004 | 0.003 | 0.005 | 0.005 |

- **Pebble 状态**：BlockCache 命中率 `99.04%` · ReadAmp `1.0` · WriteStall `0.0` · WAL 写入 `29111690.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 52428800.0, "falloc_zero": 0.0, "full_block": 34359738368.0, "rmw": 0.0, "zero_elide": 0.0}` · 读源块计数 `{"disk_pack": 184997.0, "hot_cache": 400.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`1363` 条（跨 Client+Server 全链路 `962` 条，异常链路 `71` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `66bdcb02f4c4cc45` | 374.20 | `bowl-client:fuse.Fsync` | 130.88 | 114.41 | 16.47 | `bowl-client:write_behind.BarrierWait` (134.34 ms) |
| `6d89f3e2db04874b` | 363.70 | `bowl-client:fuse.Fsync` | 129.78 | 116.06 | 13.72 | `bowl-client:write_behind.BackpressureWait` (138.86 ms) |
| `090cf5cbbd22ce4f` | 347.64 | `bowl-client:write_behind.FlushChunk` | 347.61 | 185.12 | 162.50 | `bowl-client:storage.v1.StorageService/Write` (347.61 ms) |
| `23dbdd566db32805` | 340.60 | `bowl-client:fuse.Fsync` | 136.67 | 124.57 | 12.10 | `bowl-client:storage.v1.StorageService/Fsync` (136.67 ms) |
| `d4c9e82b09bdfd8f` | 330.11 | `bowl-client:fuse.Fsync` | 76.17 | 64.08 | 12.08 | `bowl-client:write_behind.BarrierWait` (229.50 ms) |
| `ba68b9690783a00c` | 326.42 | `bowl-client:write_behind.FlushChunk` | 326.39 | 174.48 | 151.91 | `bowl-client:storage.v1.StorageService/Write` (326.39 ms) |
| `8dff858261775f74` | 321.84 | `bowl-client:write_behind.FlushChunk` | 321.80 | 176.12 | 145.68 | `bowl-client:storage.v1.StorageService/Write` (321.80 ms) |
| `1a957569cc992d30` | 319.89 | `bowl-client:write_behind.FlushChunk` | 319.86 | 165.16 | 154.69 | `bowl-client:storage.v1.StorageService/Write` (319.86 ms) |
| `b3c60c64e0f45895` | 315.81 | `bowl-client:write_behind.FlushChunk` | 315.78 | 156.43 | 159.35 | `bowl-client:storage.v1.StorageService/Write` (315.78 ms) |
| `c74e545d610f0c51` | 313.46 | `bowl-client:write_behind.FlushChunk` | 313.43 | 177.43 | 136.00 | `bowl-client:storage.v1.StorageService/Write` (313.43 ms) |
| `9ae77df0db5fe6fe` | 309.82 | `bowl-client:write_behind.FlushChunk` | 309.78 | 172.89 | 136.89 | `bowl-client:storage.v1.StorageService/Write` (309.78 ms) |
| `00e50b0f8ff0ff82` | 308.59 | `bowl-client:write_behind.FlushChunk` | 308.56 | 168.11 | 140.45 | `bowl-client:storage.v1.StorageService/Write` (308.56 ms) |
| `369bebb781a52d16` | 306.08 | `bowl-client:write_behind.FlushChunk` | 306.05 | 143.24 | 162.82 | `bowl-client:storage.v1.StorageService/Write` (306.05 ms) |
| `71a83b8d9a3b22ef` | 305.01 | `bowl-client:write_behind.FlushChunk` | 304.98 | 143.83 | 161.15 | `bowl-client:storage.v1.StorageService/Write` (304.98 ms) |
| `3fbcedbd1fe4d7f2` | 302.99 | `bowl-client:write_behind.FlushChunk` | 302.95 | 152.30 | 150.65 | `bowl-client:storage.v1.StorageService/Write` (302.95 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`310` · **服务端 Warn/Error 数**：`297` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 9, "flush_begin": 3, "table_created": 6, "flush_end": 3, "wal_deleted": 3, "compaction_begin": 3, "compaction_end": 3}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `fio_direct_randwrite_cpu.pb.gz` | `4.43s 60.60% 60.60%      4.43s 60.60%  internal/runtime/syscall/linux.Syscall6` | `4.43s 60.60% 60.60%      4.43s 60.60%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_fio_direct_randwrite_cpu.txt` |
| `server` | `fio_direct_randwrite_heap.pb.gz` | `256.09MB 55.95% 55.95%   256.09MB 55.95%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%   424.49MB 92.74%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_fio_direct_randwrite_heap.txt` |
| `server` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | `pprof_top/server_fio_direct_randwrite_mutex.txt` |
| `server` | `fio_seq_read_1job_cpu.pb.gz` | `3.36s 56.00% 56.00%      3.36s 56.00%  internal/runtime/syscall/linux.Syscall6` | `3.36s 56.00% 56.00%      3.36s 56.00%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_fio_seq_read_1job_cpu.txt` |
| `server` | `fio_seq_read_1job_heap.pb.gz` | `96MB 52.33% 52.33%       96MB 52.33%  bytes.growSlice` | `0     0%     0%   165.54MB 90.23%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_fio_seq_read_1job_heap.txt` |
| `server` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/server_fio_seq_read_1job_mutex.txt` |
| `server` | `fio_seq_write_1job_cpu.pb.gz` | `4.03s 66.17% 66.17%      4.03s 66.17%  internal/runtime/syscall/linux.Syscall6` | `4.03s 66.17% 66.17%      4.03s 66.17%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_fio_seq_write_1job_cpu.txt` |
| `server` | `fio_seq_write_1job_heap.pb.gz` | `96MB 52.33% 52.33%       96MB 52.33%  bytes.growSlice` | `0     0%     0%   165.54MB 90.23%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_fio_seq_write_1job_heap.txt` |
| `server` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/server_fio_seq_write_1job_mutex.txt` |
| `server` | `mdtest_1p_cpu.pb.gz` | `0.78s 11.49% 11.49%      0.78s 11.49%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.31s 34.02%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_mdtest_1p_cpu.txt` |
| `server` | `mdtest_1p_heap.pb.gz` | `50.63MB 39.62% 39.62%    50.63MB 39.62%  github.com/cockroachdb/pebble/v2/internal/manual.New (inline)` | `0     0%     0%    67.10MB 52.52%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_mdtest_1p_heap.txt` |
| `server` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | `pprof_top/server_mdtest_1p_mutex.txt` |
| `server` | `mdtest_4p_cpu.pb.gz` | `110ms 12.79% 12.79%      110ms 12.79%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      310ms 36.05%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_mdtest_4p_cpu.txt` |
| `server` | `mdtest_4p_heap.pb.gz` | `50.63MB 39.59% 39.59%    50.63MB 39.59%  github.com/cockroachdb/pebble/v2/internal/manual.New (inline)` | `0     0%     0%    66.04MB 51.65%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_mdtest_4p_heap.txt` |
| `server` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | `pprof_top/server_mdtest_4p_mutex.txt` |
| `client` | `fio_direct_randwrite_cpu.pb.gz` | `1.36s 39.65% 39.65%      1.36s 39.65%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.82s 53.06%  net/http.(*http2clientStream).doRequest` | `pprof_top/client_fio_direct_randwrite_cpu.txt` |
| `client` | `fio_direct_randwrite_heap.pb.gz` | `332.57MB 44.12% 44.12%   332.57MB 44.12%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   402.73MB 53.42%  sync.(*Pool).Get` | `pprof_top/client_fio_direct_randwrite_heap.txt` |
| `client` | `fio_direct_randwrite_mutex.pb.gz` | `—` | `—` | `pprof_top/client_fio_direct_randwrite_mutex.txt` |
| `client` | `fio_seq_read_1job_cpu.pb.gz` | `2.25s 43.95% 43.95%      2.25s 43.95%  internal/runtime/syscall/linux.Syscall6` | `2.25s 43.95% 43.95%      2.25s 43.95%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/client_fio_seq_read_1job_cpu.txt` |
| `client` | `fio_seq_read_1job_heap.pb.gz` | `128MB 45.40% 45.40%      128MB 45.40%  bytes.growSlice` | `0     0%     0%   217.03MB 76.98%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_fio_seq_read_1job_heap.txt` |
| `client` | `fio_seq_read_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/client_fio_seq_read_1job_mutex.txt` |
| `client` | `fio_seq_write_1job_cpu.pb.gz` | `1.53s 45.13% 45.13%      1.53s 45.13%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.55s 45.72%  syscall.Syscall` | `pprof_top/client_fio_seq_write_1job_cpu.txt` |
| `client` | `fio_seq_write_1job_heap.pb.gz` | `95.15MB 34.38% 34.38%    95.15MB 34.38%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   159.74MB 57.71%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_fio_seq_write_1job_heap.txt` |
| `client` | `fio_seq_write_1job_mutex.pb.gz` | `—` | `—` | `pprof_top/client_fio_seq_write_1job_mutex.txt` |
| `client` | `mdtest_1p_cpu.pb.gz` | `0.73s 19.73% 19.73%      0.73s 19.73%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      2.05s 55.41%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_mdtest_1p_cpu.txt` |
| `client` | `mdtest_1p_heap.pb.gz` | `335.54MB 29.84% 29.84%   335.54MB 29.84%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   733.09MB 65.19%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_mdtest_1p_heap.txt` |
| `client` | `mdtest_1p_mutex.pb.gz` | `—` | `—` | `pprof_top/client_mdtest_1p_mutex.txt` |
| `client` | `mdtest_4p_cpu.pb.gz` | `120ms 21.82% 21.82%      120ms 21.82%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      330ms 60.00%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_mdtest_4p_cpu.txt` |
| `client` | `mdtest_4p_heap.pb.gz` | `335.54MB 29.84% 29.84%   335.54MB 29.84%  google.golang.org/protobuf/proto.MarshalOptions.marshal` | `0     0%     0%   733.09MB 65.19%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_mdtest_4p_heap.txt` |
| `client` | `mdtest_4p_mutex.pb.gz` | `—` | `—` | `pprof_top/client_mdtest_4p_mutex.txt` |
