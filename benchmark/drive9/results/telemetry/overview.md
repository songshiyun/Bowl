# Bowl `drive9` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `fuse_perf_baseline` | 16.000 |
| 2 | `jfs_bench` | 11.000 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `d9_cold_io` 全链路逐跳拆解（父汇总阶段 · 含子阶段 `d9_cold_seq_read`, `d9_cold_small_write_fsync`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `23.947 s` (`23947.0 ms`) vs FUSE 累计活跃 `3070.05 ms`（等效并发度 `0.13x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`3070.05 ms` / RPC `8271.91 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `97.54%` / Readlink `N/A` · WriteBehind 聚批 `2288.34 MiB` / 直通 `18.00 MiB` (Flush RPC `1176`) · ReadAhead 预读 `768.00 MiB` / 命中 `766.88 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 20,764 | 3070.05 | 0.148 | 0.016 | 0.997 | 100.0% | 错误返回 4174 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 9,114 | 1801.22 | 0.198 | 0.027 | 0.998 | 58.7% | 占 FUSE 58.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 609 | 601.94 | 0.988 | 0.787 | 10.923 | 19.6% | 占 FUSE 19.6% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 2,904 | 291.63 | 0.100 | 0.085 | 1.065 | 9.5% | 占 FUSE 9.5% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 73 | 44.50 | 0.610 | 0.575 | 3.175 | 1.4% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 6 | 10.71 | 1.785 | 1.750 | 2.485 | 0.3% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 19 | 114.82 | 6.043 | 6.750 | 24.050 | 3.7% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 6,045 | 8222.54 | 1.360 | 0.496 | 23.095 | 99.4% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 2.69x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 6,045 | 4936.10 | 0.817 | 0.318 | 21.458 | 59.7% | Write:2.257ms, Read:0.586ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 6,045 | 5825.09 | 0.964 | 0.329 | 19.573 | 70.4% | HTTP Body+Codec 差值均值 0.420 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 6,045 | 3286.45 | 0.544 | 0.178 | 9.513 | 39.7% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,194 | 2123.16 | 1.778 | 0.250 | 9.513 | 25.7% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 3,168 | 898.69 | 0.284 | 0.184 | 2.473 | 10.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,866 | 4.75 | 0.002 | 0.003 | 0.008 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 3,179 | 848.41 | 0.267 | 0.182 | 2.453 | 10.3% | read(3168), lookup(11) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 6,045 | 29.24 | 0.005 | 0.003 | 0.043 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 921 | 14.02 | 0.015 | 0.018 | 0.044 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,866 | 38.03 | 0.013 | 0.011 | 0.068 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,866 | 11.93 | 0.004 | 0.003 | 0.021 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,257 | 25.22 | 0.011 | 0.008 | 0.033 | 0.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 3,213 | 769.83 | 0.240 | 0.180 | 2.370 | 9.3% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,194 | 1274.05 | 1.067 | 0.040 | 4.833 | 15.4% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 609 | 110.65 | 0.182 | 0.174 | 2.955 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 609 | 123.03 | 0.202 | 0.173 | 5.650 | 1.5% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 100 | 57.64 | 0.576 | 0.021 | 8.333 | 0.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 645 | 244.69 | 0.379 | 0.171 | 8.297 | 3.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 11 | 0.04 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 610 | 98.14 | 0.161 | 0.093 | 3.290 | 1.2% | — |

### 工作负载 `d9_cold_meta_macro` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `33.778 s` (`33778.0 ms`) vs FUSE 累计活跃 `7346.96 ms`（等效并发度 `0.22x`）；占比基准：`FUSE 活跃总耗时` (`7346.96 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `99.26%` / Readlink `N/A` · WriteBehind 聚批 `64.16 MiB` / 直通 `0.00 MiB` (Flush RPC `5279`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `91.68%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 123,184 | 7346.96 | 0.060 | 0.002 | 0.501 | 100.0% | 错误返回 25008 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 6,623 | 1641.10 | 0.248 | 0.268 | 0.964 | 22.3% | 占 FUSE 22.3% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,307 | 1479.44 | 0.279 | 0.280 | 0.510 | 20.1% | 占 FUSE 20.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,056 | 1220.87 | 0.241 | 0.214 | 0.509 | 16.6% | 占 FUSE 16.6% |
| `2. 客户端等待` | `Client InodeLock 等待` | 3 | 0.54 | 0.179 | 0.075 | 0.492 | 0.0% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 26,305 | 6633.36 | 0.252 | 0.230 | 20.500 | 90.3% | 非 OK 状态 47 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 26,305 | 5255.77 | 0.200 | 0.181 | 20.401 | 71.5% | Write:0.220ms, Create:0.204ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 26,305 | 2233.52 | 0.085 | 0.080 | 2.365 | 30.4% | HTTP Body+Codec 差值均值 0.033 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 26,305 | 1377.60 | 0.052 | 0.050 | 2.365 | 18.8% | 非 OK 状态 47 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 5,315 | 400.74 | 0.075 | 0.072 | 0.242 | 5.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Create` | 5,307 | 290.55 | 0.055 | 0.053 | 0.148 | 4.0% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 23,456 | 26.27 | 0.001 | 0.003 | 0.005 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 2,852 | 55.70 | 0.020 | 0.019 | 0.063 | 0.8% | read(1118), readdirplus(1560), lookup(129) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 11,053 | 104.72 | 0.009 | 0.008 | 0.045 | 1.4% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,560 | 10.47 | 0.007 | 0.004 | 0.040 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 23,456 | 245.41 | 0.010 | 0.009 | 0.045 | 3.3% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 23,456 | 49.84 | 0.002 | 0.003 | 0.008 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 23,447 | 134.83 | 0.006 | 0.005 | 0.023 | 1.8% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 5,330 | 65.47 | 0.012 | 0.008 | 0.091 | 0.9% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 3 | 0.03 | 0.009 | 0.008 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 9 | 1.81 | 0.201 | 0.196 | 0.489 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 9 | 3.27 | 0.363 | 0.281 | 2.365 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 1 | 0.36 | 0.362 | 0.375 | 0.497 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 10 | 5.25 | 0.525 | 0.190 | 4.750 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.003 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 11 | 1.85 | 0.168 | 0.150 | 0.395 | 0.0% | — |

### 工作负载 `d9_cold_seq_read` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.163 s` (`7163.0 ms`) vs FUSE 累计活跃 `453.88 ms`（等效并发度 `0.06x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`453.88 ms` / RPC `2818.44 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `81.82%` / Readlink `N/A` · WriteBehind 聚批 `762.00 MiB` / 直通 `6.00 MiB` (Flush RPC `192`) · ReadAhead 预读 `768.00 MiB` / 命中 `766.88 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 7,788 | 453.88 | 0.058 | 0.018 | 0.562 | 100.0% | 错误返回 787 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 6,144 | 312.89 | 0.051 | 0.018 | 0.479 | 68.9% | 占 FUSE 68.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 768 | 92.50 | 0.120 | 0.096 | 1.024 | 20.4% | 占 FUSE 20.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 3 | 37.29 | 12.431 | 13.653 | 16.384 | 8.2% | 占 FUSE 8.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 12 | 11.04 | 0.920 | 0.643 | 4.700 | 2.4% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 2 | 3.65 | 1.827 | 1.750 | 2.485 | 0.8% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 19 | 114.82 | 6.043 | 6.750 | 24.050 | 25.3% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 415 | 2809.41 | 6.770 | 7.195 | 23.437 | 99.7% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 6.21x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 415 | 1838.29 | 4.430 | 5.056 | 15.233 | 65.2% | Read:4.959ms, Write:4.295ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 415 | 2016.48 | 4.859 | 4.778 | 21.288 | 71.5% | HTTP Body+Codec 差值均值 2.519 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 415 | 971.11 | 2.340 | 2.139 | 10.150 | 34.5% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 198 | 599.70 | 3.029 | 2.595 | 10.150 | 21.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 198 | 353.75 | 1.787 | 1.781 | 8.020 | 12.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 215 | 0.58 | 0.003 | 0.003 | 0.010 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 200 | 347.25 | 1.736 | 1.747 | 8.020 | 12.3% | read(198), lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 192 | 3.63 | 0.019 | 0.019 | 0.049 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 207 | 1.14 | 0.005 | 0.006 | 0.015 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 215 | 4.04 | 0.019 | 0.016 | 0.207 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 215 | 2.12 | 0.010 | 0.008 | 0.049 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 212 | 3.38 | 0.016 | 0.016 | 0.094 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 235 | 336.96 | 1.434 | 1.573 | 7.650 | 12.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 198 | 402.83 | 2.035 | 1.881 | 4.928 | 14.3% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 3 | 8.79 | 2.930 | 2.125 | 4.925 | 0.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 3 | 14.87 | 4.955 | 6.250 | 9.925 | 0.5% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 12 | 38.09 | 3.175 | 3.750 | 9.800 | 1.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 15 | 47.12 | 3.141 | 2.350 | 9.850 | 1.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 3 | 0.01 | 0.003 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 8.66 | 2.887 | 2.250 | 4.297 | 0.3% | — |

### 工作负载 `d9_cold_small_write_fsync` 全链路逐跳拆解（子阶段 · 隶属 `d9_cold_io`）

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `8.809 s` (`8809.0 ms`) vs FUSE 累计活跃 `815.80 ms`（等效并发度 `0.09x`）；占比基准：`FUSE 活跃总耗时` (`815.80 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.51%` / Readlink `N/A` · WriteBehind 聚批 `2.34 MiB` / 直通 `0.00 MiB` (Flush RPC `600`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 6,676 | 815.80 | 0.122 | 0.002 | 0.997 | 100.0% | 错误返回 1810 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 600 | 490.15 | 0.817 | 0.782 | 1.856 | 60.1% | 占 FUSE 60.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 600 | 199.85 | 0.333 | 0.370 | 0.832 | 24.5% | 占 FUSE 24.5% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 400 | 110.22 | 0.276 | 0.211 | 0.504 | 13.5% | 占 FUSE 13.5% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 2,211 | 769.57 | 0.348 | 0.341 | 0.991 | 94.3% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 2,211 | 496.81 | 0.225 | 0.217 | 0.732 | 60.9% | Create:0.241ms, Write:0.240ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 2,211 | 366.85 | 0.166 | 0.189 | 0.499 | 45.0% | HTTP Body+Codec 差值均值 0.043 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 2,211 | 272.76 | 0.123 | 0.124 | 0.495 | 33.4% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 600 | 139.14 | 0.232 | 0.201 | 0.495 | 17.1% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 600 | 68.08 | 0.113 | 0.151 | 0.249 | 8.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,209 | 2.78 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 2 | 0.01 | 0.006 | 0.008 | 0.010 | 0.0% | lookup(2) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 606 | 14.60 | 0.024 | 0.021 | 0.050 | 1.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,209 | 22.48 | 0.010 | 0.008 | 0.043 | 2.8% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,209 | 5.18 | 0.002 | 0.003 | 0.009 | 0.6% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 1,609 | 11.61 | 0.007 | 0.007 | 0.023 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 600 | 8.16 | 0.014 | 0.017 | 0.025 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 600 | 75.23 | 0.125 | 0.173 | 0.250 | 9.2% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 600 | 75.68 | 0.126 | 0.172 | 0.249 | 9.3% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 4 | 0.58 | 0.145 | 0.025 | 0.490 | 0.1% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 600 | 72.32 | 0.121 | 0.166 | 0.249 | 8.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 2 | 0.01 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 600 | 62.93 | 0.105 | 0.091 | 0.220 | 7.7% | — |

### 工作负载 `d9_hot` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `13.701 s` (`13701.0 ms`) vs FUSE 累计活跃 `8640.22 ms`（等效并发度 `0.63x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`8640.22 ms` / RPC `12231.66 ms`)
- **缓存与流水线命中**：NSCache Attr `99.90%` / Entry `98.84%` / Readlink `N/A` · WriteBehind 聚批 `2352.56 MiB` / 直通 `18.00 MiB` (Flush RPC `6458`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 126,631 | 8640.22 | 0.068 | 0.002 | 0.510 | 100.0% | 错误返回 29562 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 5,922 | 1735.50 | 0.293 | 0.333 | 0.511 | 20.1% | 占 FUSE 20.1% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 8,290 | 1703.04 | 0.205 | 0.245 | 0.915 | 19.7% | 占 FUSE 19.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Unlink` | 5,899 | 1398.44 | 0.237 | 0.207 | 0.505 | 16.2% | 占 FUSE 16.2% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 53 | 60.62 | 1.144 | 0.614 | 8.675 | 0.7% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 6 | 18.93 | 3.156 | 2.500 | 9.850 | 0.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 29,158 | 12063.64 | 0.414 | 0.261 | 10.843 | 98.6% | 非 OK 状态 74 次 · 异步并发流水线 (累计为 FUSE 活跃 1.42x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 29,158 | 8384.48 | 0.288 | 0.202 | 5.877 | 68.5% | Write:0.615ms, Create:0.212ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 29,158 | 6239.74 | 0.214 | 0.094 | 9.413 | 51.0% | HTTP Body+Codec 差值均值 0.088 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 29,158 | 3679.16 | 0.126 | 0.059 | 6.910 | 30.1% | 非 OK 状态 74 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 6,512 | 2357.94 | 0.362 | 0.082 | 4.966 | 19.3% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Create` | 5,922 | 357.95 | 0.060 | 0.063 | 0.192 | 2.9% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 26,797 | 38.94 | 0.001 | 0.003 | 0.005 | 0.3% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 1 | 1.92 | 1.919 | 1.750 | 2.485 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 2,367 | 36.51 | 0.015 | 0.015 | 0.050 | 0.3% | readdirplus(1594), read(569), lookup(143) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 10,654 | 121.31 | 0.011 | 0.012 | 0.047 | 1.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,594 | 14.89 | 0.009 | 0.008 | 0.045 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 26,797 | 307.69 | 0.011 | 0.010 | 0.046 | 2.5% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 26,797 | 65.00 | 0.002 | 0.003 | 0.009 | 0.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 26,179 | 185.34 | 0.007 | 0.007 | 0.024 | 1.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 6,563 | 1294.86 | 0.197 | 0.008 | 3.544 | 10.6% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 3 | 0.03 | 0.008 | 0.008 | 0.010 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 618 | 131.41 | 0.213 | 0.175 | 5.586 | 1.1% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 618 | 145.47 | 0.235 | 0.176 | 6.567 | 1.2% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 37 | 203.62 | 5.503 | 7.109 | 9.942 | 1.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 655 | 409.45 | 0.625 | 0.174 | 9.636 | 3.3% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 18 | 0.11 | 0.006 | 0.003 | 0.091 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 618 | 119.17 | 0.193 | 0.092 | 5.138 | 1.0% | — |

### 工作负载 `fuse_perf_baseline` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `12.761 s` (`12761.0 ms`) vs FUSE 累计活跃 `156.82 ms`（等效并发度 `0.01x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`156.82 ms` / RPC `171.87 ms`)
- **缓存与流水线命中**：NSCache Attr `99.91%` / Entry `94.84%` / Readlink `N/A` · WriteBehind 聚批 `15.13 MiB` / 直通 `2.03 MiB` (Flush RPC `90`) · ReadAhead 预读 `0.38 MiB` / 命中 `0.20 MiB` · Pebble BlockCache `100.00%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 2,709 | 156.82 | 0.058 | 0.002 | 0.933 | 100.0% | 错误返回 791 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 83 | 71.72 | 0.864 | 0.790 | 8.192 | 45.7% | 占 FUSE 45.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 73 | 22.31 | 0.306 | 0.370 | 0.512 | 14.2% | 占 FUSE 14.2% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 119 | 18.28 | 0.154 | 0.158 | 0.512 | 11.7% | 占 FUSE 11.7% |
| `2. 客户端等待` | `Client InodeLock 等待` | 1 | 0.31 | 0.311 | 0.375 | 0.497 | 0.2% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 428 | 169.28 | 0.396 | 0.317 | 17.575 | 98.5% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 1.10x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 428 | 115.04 | 0.269 | 0.225 | 15.570 | 66.9% | Write:0.483ms, Fsync:0.200ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 428 | 89.21 | 0.208 | 0.146 | 7.525 | 51.9% | HTTP Body+Codec 差值均值 0.081 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 428 | 54.24 | 0.127 | 0.092 | 2.925 | 31.6% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 83 | 22.09 | 0.266 | 0.192 | 2.925 | 12.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 99 | 18.23 | 0.184 | 0.102 | 2.005 | 10.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 356 | 0.49 | 0.001 | 0.003 | 0.007 | 0.3% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 72 | 1.22 | 0.017 | 0.017 | 0.066 | 0.7% | read(68), lookup(4) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 243 | 2.27 | 0.009 | 0.004 | 0.048 | 1.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1 | 0.02 | 0.017 | 0.018 | 0.025 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 356 | 4.08 | 0.011 | 0.008 | 0.048 | 2.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 356 | 0.99 | 0.003 | 0.003 | 0.015 | 0.6% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 266 | 2.45 | 0.009 | 0.008 | 0.028 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 93 | 8.50 | 0.091 | 0.017 | 1.802 | 4.9% | — |
| `7. BlobStore 数据面` | `BlobStore RMW 补齐读 (blob_rmw_read)` | 8 | 0.04 | 0.005 | 0.004 | 0.025 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 82 | 13.79 | 0.168 | 0.173 | 2.950 | 8.0% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 83 | 13.60 | 0.164 | 0.112 | 2.925 | 7.9% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 83 | 16.44 | 0.198 | 0.091 | 3.963 | 9.6% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 1 | 0.00 | 0.004 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 82 | 12.08 | 0.147 | 0.087 | 3.818 | 7.0% | — |

### 工作负载 `jfs_bench` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `6.453 s` (`6453.0 ms`) vs FUSE 累计活跃 `11752.34 ms`（等效并发度 `1.82x`）；占比基准：`FUSE 活跃总耗时 (Hop 1-2) / 异步 RPC 流水线总耗时 (Hop 3-8)` (`11752.34 ms` / RPC `48890.47 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `100.00%` / Readlink `N/A` · WriteBehind 聚批 `4138.00 MiB` / 直通 `8.00 MiB` (Flush RPC `1424`) · ReadAhead 预读 `4096.00 MiB` / 命中 `4094.50 MiB` · Pebble BlockCache `N/A (0 reads)`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 48,325 | 11752.34 | 0.243 | 0.015 | 6.246 | 100.0% | 错误返回 4909 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,499 | 5732.53 | 1.274 | 0.108 | 15.746 | 48.8% | 占 FUSE 48.8% |
| `1. FUSE 入口 (Top Op)` | `fuse.Read` | 33,185 | 5334.83 | 0.161 | 0.015 | 3.253 | 45.4% | 占 FUSE 45.4% |
| `1. FUSE 入口 (Top Op)` | `fuse.Flush` | 810 | 385.25 | 0.476 | 0.064 | 1.733 | 3.3% | 占 FUSE 3.3% |
| `2. 客户端等待` | `WriteBehind 背压等待` | 823 | 5143.46 | 6.250 | 5.668 | 24.326 | 43.8% | — |
| `2. 客户端等待` | `WriteBehind 屏障等待` | 1 | 1.53 | 1.533 | 1.750 | 2.485 | 0.0% | — |
| `2. 客户端等待` | `ReadAhead 预读等待` | 16 | 594.26 | 37.141 | 35.714 | 98.000 | 5.1% | — |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 3,675 | 48809.22 | 13.281 | 15.811 | 49.766 | 99.8% | 非 OK 状态 0 次 · 异步并发流水线 (累计为 FUSE 活跃 4.16x) |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 3,675 | 18991.94 | 5.168 | 4.018 | 39.919 | 38.8% | Read:8.001ms, Write:5.132ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 3,675 | 38284.02 | 10.417 | 13.645 | 49.655 | 78.3% | HTTP Body+Codec 差值均值 2.303 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 3,675 | 29817.28 | 8.114 | 11.792 | 49.591 | 61.0% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 1,432 | 28961.63 | 20.225 | 29.566 | 49.591 | 59.2% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,432 | 800.28 | 0.559 | 0.664 | 2.257 | 1.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 2,242 | 28.86 | 0.013 | 0.003 | 0.023 | 0.1% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 2 | 9.97 | 4.986 | 2.500 | 9.900 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,434 | 759.11 | 0.529 | 0.659 | 2.181 | 1.6% | read(1432), getattr(1), statfs(1) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 2,237 | 17.43 | 0.008 | 0.006 | 0.044 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,024 | 16.35 | 0.016 | 0.017 | 0.049 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 2,237 | 34.51 | 0.015 | 0.013 | 0.072 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 2,237 | 19.11 | 0.009 | 0.005 | 0.024 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 2,237 | 53.18 | 0.024 | 0.010 | 0.048 | 0.1% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,961 | 688.95 | 0.351 | 0.386 | 0.969 | 1.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 1,432 | 2311.81 | 1.614 | 1.643 | 4.907 | 4.7% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 64 | 19.68 | 0.308 | 0.003 | 6.800 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 3 | 11.08 | 3.692 | 3.350 | 7.929 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`323,121` 次（错误返回 `64,458` 次），FUSE 累计活跃 `31088.64 ms`，读 `5206.87 MiB` / 写 `8904.24 MiB`
- **NamespaceCache 命中率**：Attr `99.91%` · Entry `99.01%` · Readlink `N/A` · 负缓存命中 `27,876` · 失效 `68,172`
- **WriteBehind 流水线**：聚批写 `8858.19 MiB` (`29,026` 次) · 直通写 `46.04 MiB` (`125` 次) · Flush RPC `14,427` (错误 `0`) · 背压等待 `949` 次 (`5248.57 ms`) · 屏障等待 `13` 次 (`31.18 ms`)
- **ReadAhead 预读**：触发 `1,216` 次 · 预读 `4864.38 MiB` · 命中 `4861.57 MiB` · 丢弃 `1.05 MiB` · 等待 `35` 次 (`709.08 ms`)
- **Inode 锁与内核队列**：快速获取 `28,834` · 竞争获取 `4`（累计等待 `0.85 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `Read` | 44,105 | 7759.53 | 25.0% | 175.9 | 17.9 | 782.1 | 2851.4 | — |
| `Writev` | 29,154 | 6428.50 | 20.7% | 220.5 | 6.4 | 215.4 | 7701.6 | EINTR:3 |
| `Flush` | 16,477 | 3731.23 | 12.0% | 226.4 | 241.7 | 687.0 | 969.5 | — |
| `Create` | 12,315 | 3606.57 | 11.6% | 292.9 | 319.9 | 496.1 | 511.8 | — |
| `Unlink` | 12,272 | 2952.92 | 9.5% | 240.6 | 210.1 | 460.0 | 507.9 | — |
| `Rmdir` | 6,254 | 1634.02 | 5.3% | 261.3 | 229.8 | 487.9 | 733.0 | — |
| `Mkdir` | 6,254 | 1551.60 | 5.0% | 248.1 | 224.5 | 479.8 | 511.0 | — |
| `Fsync` | 1,319 | 1325.89 | 4.3% | 1005.2 | 787.8 | 1492.1 | 10601.4 | — |
| `ReadDirPlus` | 14,520 | 905.74 | 2.9% | 62.4 | 3.7 | 375.7 | 493.7 | — |
| `Rename` | 3,103 | 733.30 | 2.4% | 236.3 | 216.8 | 469.4 | 506.6 | — |
| `Lookup` | 31,841 | 122.09 | 0.4% | 3.8 | 1.5 | 6.5 | 14.6 | ENOENT:24936 |
| `Release` | 16,477 | 72.99 | 0.2% | 4.4 | 3.5 | 11.2 | 15.8 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `66,102` 次 · 正常 (`OK`) `65,981` 次 · 业务/协议非 OK `121` 次 (`0.18%`，分布 `{"NOT_FOUND": 121}`)
- **客户端 RPC 状态码拆分**：总调用 `66,101` 次 · 正常 (`OK`) `65,980` 次 · 非 OK `121` 次 (`0.18%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Write` | 14,552 | — | 42022.44 | 2.888 | 0.138 | 31.196 | 46.260 |
| `http_outer:Read` | 6,355 | — | 6767.41 | 1.065 | 0.299 | 4.849 | 9.366 |
| `http_outer:Create` | 12,315 | — | 1153.14 | 0.094 | 0.088 | 0.229 | 0.247 |
| `http_outer:Unlink` | 12,272 | — | 884.76 | 0.072 | 0.071 | 0.181 | 0.239 |
| `http_outer:Fsync` | 1,319 | — | 481.23 | 0.365 | 0.317 | 0.491 | 6.336 |
| `http_outer:Rmdir` | 6,254 | — | 467.85 | 0.075 | 0.074 | 0.197 | 0.240 |
| `http_outer:Mkdir` | 6,254 | — | 460.80 | 0.074 | 0.072 | 0.195 | 0.241 |
| `http_outer:ReadDirPlus` | 3,169 | — | 215.23 | 0.068 | 0.069 | 0.146 | 0.241 |
| `http_outer:Rename` | 3,103 | — | 210.96 | 0.068 | 0.066 | 0.184 | 0.239 |
| `http_outer:Lookup` | 302 | — | 28.07 | 0.093 | 0.088 | 0.231 | 0.247 |
| `http_outer:SetAttr` | 72 | — | 6.84 | 0.095 | 0.090 | 0.231 | 0.246 |
| `http_outer:CreateDisk` | 1 | — | 5.45 | 5.446 | 7.500 | 9.750 | 9.950 |
| `http_outer:GetAttr` | 88 | — | 5.38 | 0.061 | 0.067 | 0.100 | 0.217 |
| `http_outer:Link` | 18 | — | 1.83 | 0.101 | 0.095 | 0.233 | 0.247 |
| `http_outer:StatFS` | 11 | — | 1.24 | 0.113 | 0.088 | 0.362 | 0.473 |
| `http_outer:FsyncDir` | 7 | — | 0.79 | 0.112 | 0.145 | 0.240 | 0.248 |
| `http_outer:Symlink` | 6 | — | 0.79 | 0.131 | 0.150 | 0.425 | 0.485 |
| `http_outer:MountDisk` | 1 | — | 0.44 | 0.437 | 0.375 | 0.487 | 0.497 |
| `http_outer:ReclaimOrphan` | 3 | — | 0.36 | 0.121 | 0.175 | 0.242 | 0.249 |
| `rpc:Write` | 14,552 | 14,552 / 0 | 33861.69 | 2.327 | 0.086 | 29.235 | 45.847 |
| `rpc:Read` | 6,355 | 6,355 / 0 | 1762.32 | 0.277 | 0.154 | 0.968 | 2.283 |
| `rpc:Create` | 12,315 | 12,315 / 0 | 731.66 | 0.059 | 0.061 | 0.098 | 0.194 |
| `rpc:Unlink` | 12,272 | 12,272 / 0 | 595.87 | 0.049 | 0.044 | 0.096 | 0.198 |
| `rpc:Fsync` | 1,319 | 1,319 / 0 | 425.23 | 0.322 | 0.202 | 0.471 | 6.121 |
| `rpc:Rmdir` | 6,254 | 6,254 / 0 | 308.57 | 0.049 | 0.046 | 0.096 | 0.177 |
| `rpc:Mkdir` | 6,254 | 6,254 / 0 | 278.55 | 0.045 | 0.041 | 0.094 | 0.188 |
| `rpc:Rename` | 3,103 | 3,103 / 0 | 133.78 | 0.043 | 0.040 | 0.094 | 0.187 |
| `rpc:ReadDirPlus` | 3,169 | 3,169 / 0 | 117.30 | 0.037 | 0.036 | 0.083 | 0.133 |
| `rpc:Lookup` | 302 | 181 / 121 | 14.64 | 0.048 | 0.045 | 0.097 | 0.193 |
| `rpc:CreateDisk` | 1 | 1 / 0 | 5.24 | 5.239 | 7.500 | 9.750 | 9.950 |
| `rpc:SetAttr` | 72 | 72 / 0 | 4.23 | 0.059 | 0.062 | 0.097 | 0.142 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:phase_a_validate` | 56,171 | 636.75 | 0.011 | 0.010 | 0.025 | 0.046 |
| `actor_phase:wal_apply_nosync` | 54,844 | 403.97 | 0.007 | 0.006 | 0.020 | 0.024 |
| `actor_phase:pack_sync_barrier` | 1,319 | 285.38 | 0.216 | 0.172 | 0.246 | 5.878 |
| `actor_phase:dead_pack_remove` | 138 | 261.63 | 1.896 | 0.156 | 9.014 | 9.803 |
| `actor_phase:wal_apply_sync` | 1,319 | 257.82 | 0.195 | 0.175 | 0.246 | 3.836 |
| `actor_phase:phase_b_mutate` | 56,171 | 147.96 | 0.003 | 0.003 | 0.006 | 0.010 |
| `read_op:read` | 6,355 | 1648.90 | 0.259 | 0.152 | 0.959 | 2.251 |
| `read_op:readdirplus` | 3,169 | 48.42 | 0.015 | 0.014 | 0.038 | 0.065 |
| `read_op:lookup` | 302 | 3.84 | 0.013 | 0.011 | 0.034 | 0.048 |
| `read_op:getattr` | 99 | 0.50 | 0.005 | 0.004 | 0.015 | 0.025 |
| `read_op:statfs` | 12 | 0.00 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:point_get` | 30,307 | 275.10 | 0.009 | 0.005 | 0.028 | 0.046 |
| `kv_read:prefix_scan` | 5,115 | 56.18 | 0.011 | 0.009 | 0.024 | 0.046 |
| `control_open_actor:ok` | 1 | 3.38 | 3.378 | 3.750 | 4.875 | 4.975 |
| `actor_queue_wait` | 56,176 | 99.76 | 0.002 | 0.003 | 0.005 | 0.005 |
| `inode_lock_wait` | 3 | 11.89 | 3.963 | 2.125 | 9.250 | 9.850 |
| `blob_pread` | 5,174 | 1458.78 | 0.282 | 0.233 | 0.805 | 2.222 |
| `blob_pwrite` | 14,612 | 4954.70 | 0.339 | 0.009 | 2.338 | 4.366 |
| `blob_rmw_read` | 14 | 0.10 | 0.007 | 0.007 | 0.021 | 0.024 |
| `pebble_wal_fsync` | 1,326 | 242.51 | 0.183 | 0.093 | 0.195 | 4.287 |
| `blob_sync:pack_data` | 1,457 | 695.52 | 0.477 | 0.166 | 3.415 | 9.080 |
| `blob_sync:packs_dir` | 32 | 0.16 | 0.005 | 0.003 | 0.005 | 0.084 |

- **Pebble 状态**：BlockCache 命中率 `99.56%` · ReadAmp `1.0` · WriteStall `0.0` · WAL 写入 `19909444.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 184772470.0, "falloc_zero": 0.0, "full_block": 9151447040.0, "rmw": 1557290.0, "zero_elide": 8.0}` · 读源块计数 `{"disk_pack": 23162.0, "hot_cache": 2168.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`1276` 条（跨 Client+Server 全链路 `827` 条，异常链路 `104` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `9b259425ed77021e` | 75.67 | `bowl-client:read_ahead.Prefetch` | 74.61 | 1.52 | 73.10 | `bowl-client:storage.v1.StorageService/Read` (74.61 ms) |
| `b7cb402ca6eea901` | 73.50 | `bowl-client:read_ahead.Prefetch` | 73.42 | 0.94 | 72.48 | `bowl-client:storage.v1.StorageService/Read` (73.42 ms) |
| `9e621cc28464a33c` | 54.83 | `bowl-client:write_behind.FlushChunk` | 54.79 | 1.85 | 52.94 | `bowl-client:storage.v1.StorageService/Write` (54.79 ms) |
| `48086ab8941327db` | 45.05 | `bowl-client:write_behind.FlushChunk` | 45.03 | 37.56 | 7.48 | `bowl-client:storage.v1.StorageService/Write` (45.03 ms) |
| `29170b2a6f4330b8` | 40.47 | `bowl-client:write_behind.FlushChunk` | 40.44 | 29.88 | 10.56 | `bowl-client:storage.v1.StorageService/Write` (40.44 ms) |
| `43306fbf0637191a` | 40.40 | `bowl-client:write_behind.FlushChunk` | 40.37 | 33.39 | 6.98 | `bowl-client:storage.v1.StorageService/Write` (40.37 ms) |
| `d94996d02f3ff5b5` | 40.19 | `bowl-client:write_behind.FlushChunk` | 40.15 | 31.62 | 8.52 | `bowl-client:storage.v1.StorageService/Write` (40.15 ms) |
| `65f71d11027be5ee` | 39.93 | `bowl-client:write_behind.FlushChunk` | 39.91 | 29.08 | 10.83 | `bowl-client:storage.v1.StorageService/Write` (39.91 ms) |
| `dcbfc6acbd2eb3b8` | 39.73 | `bowl-client:write_behind.FlushChunk` | 39.70 | 30.16 | 9.54 | `bowl-client:storage.v1.StorageService/Write` (39.70 ms) |
| `b7c906bc5adecf04` | 39.54 | `bowl-client:write_behind.FlushChunk` | 39.51 | 34.03 | 5.48 | `bowl-client:storage.v1.StorageService/Write` (39.51 ms) |
| `c856fe42cfeefb2c` | 38.81 | `bowl-client:write_behind.FlushChunk` | 38.78 | 31.34 | 7.44 | `bowl-client:storage.v1.StorageService/Write` (38.78 ms) |
| `c333ca7632fc2b42` | 38.50 | `bowl-client:write_behind.FlushChunk` | 38.46 | 32.73 | 5.72 | `bowl-client:storage.v1.StorageService/Write` (38.46 ms) |
| `fa0c80755aed2216` | 38.28 | `bowl-client:write_behind.FlushChunk` | 38.24 | 32.05 | 6.18 | `bowl-client:storage.v1.StorageService/Write` (38.24 ms) |
| `99741e9a02a64fb5` | 38.14 | `bowl-client:write_behind.FlushChunk` | 38.11 | 34.28 | 3.84 | `bowl-client:storage.v1.StorageService/Write` (38.11 ms) |
| `8acc41aa1ff8767f` | 38.03 | `bowl-client:write_behind.FlushChunk` | 37.66 | 32.60 | 5.05 | `bowl-client:storage.v1.StorageService/Write` (37.66 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`15` · **服务端 Warn/Error 数**：`0` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 7, "flush_begin": 1, "table_created": 2, "flush_end": 1, "compaction_begin": 1, "wal_deleted": 3, "compaction_end": 1}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `macro_go_build_cpu.pb.gz` | `100ms 14.93% 14.93%      100ms 14.93%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      250ms 37.31%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_macro_go_build_cpu.txt` |
| `server` | `macro_go_build_heap.pb.gz` | `33.50MB 26.48% 26.48%    33.50MB 26.48%  bytes.growSlice` | `0     0%     0%    87.80MB 69.40%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_macro_go_build_heap.txt` |
| `server` | `macro_go_build_mutex.pb.gz` | `—` | `—` | `pprof_top/server_macro_go_build_mutex.txt` |
| `server` | `seq_read_cpu.pb.gz` | `1.06s 65.84% 65.84%      1.06s 65.84%  internal/runtime/syscall/linux.Syscall6` | `1060ms 65.84% 65.84%     1060ms 65.84%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_seq_read_cpu.txt` |
| `server` | `seq_read_heap.pb.gz` | `72MB 42.53% 42.53%       72MB 42.53%  bytes.growSlice` | `0     0%     0%   141.16MB 83.38%  connectrpc.com/connect.(*Handler).ServeHTTP` | `pprof_top/server_seq_read_heap.txt` |
| `server` | `seq_read_mutex.pb.gz` | `—` | `—` | `pprof_top/server_seq_read_mutex.txt` |
| `server` | `small_write_fsync_cpu.pb.gz` | `110ms 15.94% 15.94%      110ms 15.94%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      300ms 43.48%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_small_write_fsync_cpu.txt` |
| `server` | `small_write_fsync_heap.pb.gz` | `68.02MB 27.42% 27.42%    68.02MB 27.42%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%   215.87MB 87.01%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_small_write_fsync_heap.txt` |
| `server` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | `pprof_top/server_small_write_fsync_mutex.txt` |
| `server` | `sqlite_wal_cpu.pb.gz` | `10ms 50.00% 50.00%       10ms 50.00%  compress/flate.matchLen (inline)` | `0     0%     0%       10ms 50.00%  bufio.(*Writer).Flush` | `pprof_top/server_sqlite_wal_cpu.txt` |
| `server` | `sqlite_wal_heap.pb.gz` | `32.01MB 23.39% 23.39%    32.01MB 23.39%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    83.74MB 61.18%  github.com/starsrc/Aether/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_sqlite_wal_heap.txt` |
| `server` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | `pprof_top/server_sqlite_wal_mutex.txt` |
| `client` | `macro_go_build_cpu.pb.gz` | `170ms 26.98% 26.98%      170ms 26.98%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      400ms 63.49%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_macro_go_build_cpu.txt` |
| `client` | `macro_go_build_heap.pb.gz` | `64.02MB 49.42% 49.42%    64.02MB 49.42%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   102.35MB 79.00%  sync.(*Pool).Get` | `pprof_top/client_macro_go_build_heap.txt` |
| `client` | `macro_go_build_mutex.pb.gz` | `—` | `—` | `pprof_top/client_macro_go_build_mutex.txt` |
| `client` | `seq_read_cpu.pb.gz` | `0.62s 39.49% 39.49%      0.62s 39.49%  runtime.memmove` | `0     0%     0%      0.63s 40.13%  syscall.Syscall` | `pprof_top/client_seq_read_cpu.txt` |
| `client` | `seq_read_heap.pb.gz` | `80.03MB 30.56% 30.56%    80.03MB 30.56%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%   146.51MB 55.95%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_seq_read_heap.txt` |
| `client` | `seq_read_mutex.pb.gz` | `—` | `—` | `pprof_top/client_seq_read_mutex.txt` |
| `client` | `small_write_fsync_cpu.pb.gz` | `20ms  8.00%  8.00%       20ms  8.00%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      120ms 48.00%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_small_write_fsync_cpu.txt` |
| `client` | `small_write_fsync_heap.pb.gz` | `128MB 44.12% 44.12%      128MB 44.12%  bytes.growSlice` | `0     0%     0%   225.04MB 77.58%  connectrpc.com/connect.(*chain).WrapUnary.unaryThunk.func1` | `pprof_top/client_small_write_fsync_heap.txt` |
| `client` | `small_write_fsync_mutex.pb.gz` | `—` | `—` | `pprof_top/client_small_write_fsync_mutex.txt` |
| `client` | `sqlite_wal_cpu.pb.gz` | `10ms 50.00% 50.00%       10ms 50.00%  compress/flate.matchLen (inline)` | `0     0%     0%       20ms   100%  bufio.(*Writer).Flush` | `pprof_top/client_sqlite_wal_cpu.txt` |
| `client` | `sqlite_wal_heap.pb.gz` | `64.02MB 49.25% 49.25%    64.02MB 49.25%  github.com/starsrc/bowl/client/internal/rpc.init.func1` | `0     0%     0%   101.85MB 78.35%  sync.(*Pool).Get` | `pprof_top/client_sqlite_wal_heap.txt` |
| `client` | `sqlite_wal_mutex.pb.gz` | `—` | `—` | `pprof_top/client_sqlite_wal_mutex.txt` |
