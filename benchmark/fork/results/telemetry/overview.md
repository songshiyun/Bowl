# Bowl `fork` 生产可观测性与慢路径遥测诊断总览

- **部署栈**：`deploy/prod/docker-compose.yaml` + `deploy/prod/storage.env`（PostgreSQL 18 + Storage + Jaeger + Prometheus + Loki + Alloy）
- **原始归档包**：`telemetry-raw.tar.gz`（含 `.pb.gz` 火焰图原始采样、双端全量 `.prom` 快照、Pebble `LOG`/`pebble_events.log` 与 `diskstats`）

## 1. 最慢 Bowl 测试阶段排序（Stage Timings）

| 排名 | 阶段 / 目录 | 耗时 (s) |
| :--- | :--- | ---: |
| 1 | `suite1_fork_matrix` | 53.050 |
| 2 | `suite1_copy_baseline` | 25.922 |
| 3 | `suite2_online_fork` | 12.036 |
| 4 | `suite3_fanout_cow` | 7.529 |

## 2. 分工作负载全链路逐跳耗时拆解表（`phase_deltas`）

### 工作负载 `suite1_copy_baseline` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `25.817 s` (`25817.0 ms`) vs FUSE 累计活跃 `0.00 ms`（等效并发度 `0.00x`）；占比基准：`Server ConnectRPC 总耗时` (`53542.86 ms`)
- **缓存与流水线命中**：NSCache Attr `N/A` / Entry `N/A` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.78%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 83,734 | 53542.86 | 0.639 | 0.216 | 93.500 | 100.0% | HTTP Body+Codec 差值均值 0.263 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 83,734 | 31491.85 | 0.376 | 0.143 | 93.500 | 58.8% | 非 OK 状态 798 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 16,714 | 11530.66 | 0.690 | 0.182 | 15.770 | 21.5% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 34,537 | 9536.47 | 0.276 | 0.046 | 5.699 | 17.8% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 48,352 | 4262.70 | 0.088 | 0.004 | 1.532 | 8.0% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 109 | 1011.87 | 9.283 | 8.833 | 24.969 | 1.9% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 35,356 | 9002.31 | 0.255 | 0.031 | 5.610 | 16.8% | read(34537), lookup(798), statfs(21) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 82,429 | 779.98 | 0.009 | 0.004 | 0.046 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 3,036 | 50.00 | 0.016 | 0.014 | 0.070 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 42,015 | 395.74 | 0.009 | 0.008 | 0.041 | 0.7% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 42,015 | 114.69 | 0.003 | 0.003 | 0.015 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 26,465 | 250.24 | 0.009 | 0.008 | 0.025 | 0.5% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 29,965 | 7853.33 | 0.262 | 0.003 | 4.716 | 14.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 16,716 | 2875.16 | 0.172 | 0.016 | 3.600 | 5.4% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 13,820 | 2885.62 | 0.209 | 0.181 | 1.928 | 5.4% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 15,358 | 2860.74 | 0.186 | 0.145 | 2.005 | 5.3% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 10,355 | 2671.48 | 0.258 | 0.187 | 2.402 | 5.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 46 | 14.94 | 0.325 | 0.004 | 3.850 | 0.0% | — |

### 工作负载 `suite1_fork_matrix` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `53.009 s` (`53009.0 ms`) vs FUSE 累计活跃 `0.00 ms`（等效并发度 `0.00x`）；占比基准：`Server ConnectRPC 总耗时` (`67828.35 ms`)
- **缓存与流水线命中**：NSCache Attr `N/A` / Entry `N/A` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `99.57%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 215,436 | 67828.35 | 0.315 | 0.094 | 15.625 | 100.0% | HTTP Body+Codec 差值均值 0.172 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 215,436 | 30833.98 | 0.143 | 0.046 | 15.625 | 45.5% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 111,366 | 24665.72 | 0.221 | 0.040 | 4.723 | 36.4% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Lookup` | 103,720 | 5162.54 | 0.050 | 0.042 | 0.223 | 7.6% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 225 | 0.72 | 0.003 | 0.003 | 0.014 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 215,211 | 25312.99 | 0.118 | 0.021 | 4.699 | 37.3% | read(111366), lookup(103720), statfs(125) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 525,246 | 2896.26 | 0.006 | 0.003 | 0.030 | 4.3% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 8,120 | 261.14 | 0.032 | 0.018 | 0.239 | 0.4% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 225 | 6.26 | 0.028 | 0.005 | 0.166 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 225 | 0.40 | 0.002 | 0.003 | 0.007 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 115,740 | 19688.65 | 0.170 | 0.003 | 3.910 | 29.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 100 | 14.73 | 0.147 | 0.176 | 0.250 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 5 | 0.01 | 0.002 | 0.003 | 0.005 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 125 | 4.55 | 0.036 | 0.054 | 0.186 | 0.0% | — |

### 工作负载 `suite2_online_fork` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `11.998 s` (`11998.0 ms`) vs FUSE 累计活跃 `16992.63 ms`（等效并发度 `1.42x`）；占比基准：`FUSE 活跃总耗时` (`16992.63 ms`)
- **缓存与流水线命中**：NSCache Attr `100.00%` / Entry `99.86%` / Readlink `N/A` · WriteBehind 聚批 `1246.21 MiB` / 直通 `1286.00 MiB` (Flush RPC `2722`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `97.49%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `1. FUSE 入口` | `fuse.* (总计)` | 34,396 | 16992.63 | 0.494 | 0.004 | 7.216 | 100.0% | 错误返回 7405 次 |
| `1. FUSE 入口 (Top Op)` | `fuse.Fsync` | 2,770 | 10344.15 | 3.734 | 3.397 | 14.031 | 60.9% | 占 FUSE 60.9% |
| `1. FUSE 入口 (Top Op)` | `fuse.Writev` | 4,627 | 3521.00 | 0.761 | 0.052 | 6.326 | 20.7% | 占 FUSE 20.7% |
| `1. FUSE 入口 (Top Op)` | `fuse.Create` | 2,770 | 2304.79 | 0.832 | 0.544 | 4.061 | 13.6% | 占 FUSE 13.6% |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `3. 客户端 ConnectRPC` | `rpc_client_call (总计)` | 12,358 | 16492.45 | 1.335 | 1.096 | 24.400 | 97.1% | 非 OK 状态 0 次 |
| `3. 跨机网络+编解码差值` | `同跳 Client ConnectRPC - Server ConnectRPC` | 12,358 | 7677.69 | 0.621 | 0.572 | 23.910 | 45.2% | Write:1.096ms, Create:0.505ms |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 28,474 | 13735.67 | 0.482 | 0.396 | 49.375 | 80.8% | HTTP Body+Codec 差值均值 0.136 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 28,474 | 9838.37 | 0.346 | 0.259 | 49.375 | 57.9% | 非 OK 状态 0 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 4,008 | 4235.57 | 1.057 | 0.731 | 5.688 | 24.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Fsync` | 2,770 | 3604.53 | 1.301 | 1.142 | 6.106 | 21.2% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 12,364 | 3399.82 | 0.275 | 0.004 | 3.822 | 20.0% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 16,110 | 535.97 | 0.033 | 0.024 | 0.954 | 3.2% | read(15632), readdirplus(110), lookup(344) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 34,279 | 246.53 | 0.007 | 0.005 | 0.044 | 1.5% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 430 | 26.81 | 0.062 | 0.020 | 0.805 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 10,478 | 153.89 | 0.015 | 0.013 | 0.051 | 0.9% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 10,478 | 36.33 | 0.003 | 0.003 | 0.010 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 7,853 | 66.12 | 0.008 | 0.007 | 0.025 | 0.4% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 15,632 | 118.31 | 0.008 | 0.003 | 0.194 | 0.7% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 4,036 | 1210.49 | 0.300 | 0.036 | 2.000 | 7.1% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 2,194 | 591.80 | 0.270 | 0.135 | 2.433 | 3.5% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 2,614 | 1834.48 | 0.702 | 0.208 | 4.653 | 10.8% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 10 | 68.12 | 6.812 | 7.500 | 9.950 | 0.4% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 1,523 | 1454.16 | 0.955 | 0.806 | 4.499 | 8.6% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 42 | 0.80 | 0.019 | 0.003 | 0.218 | 0.0% | — |
| `8. 持久化 Fsync 屏障` | `Pebble 底层 WAL Fsync (pebble_wal_fsync)` | 2,207 | 517.66 | 0.235 | 0.090 | 1.999 | 3.0% | — |

### 工作负载 `suite3_fanout_cow` 全链路逐跳拆解

- **墙钟耗时 vs FUSE 活跃耗时**：墙钟 `7.489 s` (`7489.0 ms`) vs FUSE 累计活跃 `0.00 ms`（等效并发度 `0.00x`）；占比基准：`Server ConnectRPC 总耗时` (`15920.51 ms`)
- **缓存与流水线命中**：NSCache Attr `N/A` / Entry `N/A` / Readlink `N/A` · WriteBehind 聚批 `0.00 MiB` / 直通 `0.00 MiB` (Flush RPC `0`) · ReadAhead 预读 `0.00 MiB` / 命中 `0.00 MiB` · Pebble BlockCache `96.69%`

| 链路跳点 (`Hop`) | 组件 / 指标 | 调用次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P99 (ms) | 耗时占比 | 说明 |
| :--- | :--- | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `2. 客户端等待` | `客户端锁 / WriteBehind / ReadAhead 等待` | 0 | 0.00 | 0.000 | 0.000 | 0.000 | 0.0% | 零阻塞等待 |
| `4. Server HTTP/Codec` | `bowl_storage_http_request (含 Body 读写+序列化)` | 1,831 | 15920.51 | 8.695 | 4.955 | 91.500 | 100.0% | HTTP Body+Codec 差值均值 5.658 ms |
| `4. Server ConnectRPC` | `rpc_server_call (总计)` | 1,831 | 5560.79 | 3.037 | 1.529 | 91.500 | 34.9% | 非 OK 状态 64 次 |
| `4. Server ConnectRPC (Top)` | `rpc_server:Write` | 336 | 3014.70 | 8.972 | 4.104 | 46.182 | 18.9% | — |
| `4. Server ConnectRPC (Top)` | `rpc_server:Read` | 1,253 | 2113.00 | 1.686 | 0.934 | 9.766 | 13.3% | — |
| `5. Actor 队列/Inode 锁` | `StoreActor 入队等待 (actor_queue_wait)` | 450 | 298.13 | 0.663 | 0.003 | 20.781 | 1.9% | — |
| `5. Actor 队列/Inode 锁` | `Server Inode 锁等待 (inode_lock_wait)` | 39 | 391.83 | 10.047 | 9.821 | 24.692 | 2.5% | — |
| `6. 引擎 KV / Phase A&B` | `Store 只读操作 (read_op_duration)` | 1,380 | 2076.24 | 1.505 | 0.833 | 9.761 | 13.0% | read(1253), lookup(110), statfs(17) |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (prefix_scan)` | 1,041 | 32.06 | 0.031 | 0.021 | 0.182 | 0.2% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble KV 读 (point_get)` | 1,600 | 23.15 | 0.014 | 0.012 | 0.092 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase A 校验与延迟编码 (phase_a_validate)` | 437 | 16.06 | 0.037 | 0.015 | 0.493 | 0.1% | — |
| `6. 引擎 KV / Phase A&B` | `Actor Phase B 内存状态提交 (phase_b_mutate)` | 437 | 3.62 | 0.008 | 0.006 | 0.046 | 0.0% | — |
| `6. 引擎 KV / Phase A&B` | `Pebble WAL 批写入 NoSync (wal_apply_nosync)` | 355 | 4.39 | 0.012 | 0.012 | 0.044 | 0.0% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pread (磁盘块读)` | 1,926 | 1934.67 | 1.005 | 0.494 | 4.982 | 12.2% | — |
| `7. BlobStore 数据面` | `BlobStore .pack pwrite (封包写)` | 345 | 589.33 | 1.708 | 1.596 | 5.688 | 3.7% | — |
| `8. 持久化 Fsync 屏障` | `Pebble WAL 同步完整墙钟 (wal_apply_sync)` | 47 | 55.48 | 1.180 | 0.241 | 7.650 | 0.3% | — |
| `8. 持久化 Fsync 屏障` | `Pack 双层同步屏障 (pack_sync_barrier)` | 32 | 203.04 | 6.345 | 3.333 | 24.314 | 1.3% | — |
| `8. 持久化 Fsync 屏障` | `已封口死包回收 (dead_pack_remove)` | 25 | 122.31 | 4.892 | 5.833 | 9.917 | 0.8% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (pack_data)` | 40 | 158.14 | 3.954 | 2.812 | 22.000 | 1.0% | — |
| `8. 持久化 Fsync 屏障` | `BlobStore fdatasync (packs_dir)` | 26 | 6.50 | 0.250 | 0.004 | 4.350 | 0.0% | — |

## 3. 客户端 FUSE 累计耗时与 9 大子系统拆解（`bowl stats`）

- **累计调用**：`34,405` 次（错误返回 `7,405` 次），FUSE 累计活跃 `16994.39 ms`，读 `0.00 MiB` / 写 `2532.21 MiB`
- **NamespaceCache 命中率**：Attr `100.00%` · Entry `99.86%` · Readlink `N/A` · 负缓存命中 `2,778` · 失效 `8,342`
- **WriteBehind 流水线**：聚批写 `1246.21 MiB` (`3,341` 次) · 直通写 `1286.00 MiB` (`1,286` 次) · Flush RPC `2,722` (错误 `0`) · 背压等待 `0` 次 (`0.00 ms`) · 屏障等待 `0` 次 (`0.00 ms`)
- **ReadAhead 预读**：触发 `0` 次 · 预读 `0.00 MiB` · 命中 `0.00 MiB` · 丢弃 `0.00 MiB` · 等待 `0` 次 (`0.00 ms`)
- **Inode 锁与内核队列**：快速获取 `5,540` · 竞争获取 `0`（累计等待 `0.00 ms`） · FUSE Waiting `0` / CongestionThreshold `48`

| 操作 (`FuseOp`) | 调用次数 | 总耗时 (ms) | 占 FUSE 总耗时 | Mean (µs) | P50 (µs) | P95 (µs) | P99 (µs) | 非零 Errno |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | :--- |
| `Fsync` | 2,770 | 10344.15 | 60.9% | 3734.3 | 3397.2 | 7992.2 | 14031.0 | — |
| `Writev` | 4,627 | 3521.00 | 20.7% | 761.0 | 51.9 | 3729.5 | 6326.0 | — |
| `Create` | 2,770 | 2304.79 | 13.6% | 832.1 | 543.9 | 2769.4 | 4061.1 | — |
| `Unlink` | 2,770 | 671.87 | 4.0% | 242.6 | 213.1 | 464.8 | 507.1 | — |
| `Release` | 2,770 | 72.81 | 0.4% | 26.3 | 4.3 | 13.6 | 104.0 | — |
| `FsyncDir` | 4 | 19.40 | 0.1% | 4850.0 | 426.7 | 32768.0 | 32768.0 | — |
| `GetXAttr` | 4,627 | 13.07 | 0.1% | 2.8 | 2.0 | 7.6 | 14.8 | ENODATA:4627 |
| `Flush` | 2,770 | 10.26 | 0.1% | 3.7 | 3.2 | 7.8 | 14.6 | — |
| `Lookup` | 2,807 | 9.88 | 0.1% | 3.5 | 1.8 | 7.5 | 15.3 | ENOENT:2778 |
| `GetAttr` | 5,538 | 7.73 | 0.1% | 1.4 | 1.4 | 3.4 | 4.2 | — |
| `ReadDirPlus` | 129 | 7.41 | 0.0% | 57.5 | 6.1 | 64.0 | 2048.0 | — |
| `Rmdir` | 8 | 4.79 | 0.0% | 599.4 | 512.0 | 2048.0 | 2048.0 | — |

## 4. 双端 ConnectRPC / HTTP 外层与存储引擎内部延迟拆解（Prometheus）

- **服务端 RPC 状态码拆分**：总调用 `345,516` 次 · 正常 (`OK`) `344,468` 次 · 业务/协议非 OK `1,048` 次 (`0.30%`，分布 `{"NOT_FOUND": 1048}`)
- **客户端 RPC 状态码拆分**：总调用 `12,361` 次 · 正常 (`OK`) `12,361` 次 · 非 OK `0` 次 (`0.00%`)

### 4.1 服务端 ConnectRPC 与 HTTP 外层耗时 Top 榜

| 方法 (`procedure`) | 总调用 | OK / 非 OK | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | :--- | ---: | ---: | ---: | ---: | ---: |
| `http_outer:Read` | 162,788 | — | 92165.75 | 0.566 | 0.099 | 3.335 | 9.971 |
| `http_outer:Write` | 26,516 | — | 36061.66 | 1.360 | 0.225 | 7.128 | 24.535 |
| `http_outer:Fsync` | 23,586 | — | 13971.23 | 0.592 | 0.395 | 2.104 | 4.457 |
| `http_outer:Lookup` | 105,158 | — | 10304.49 | 0.098 | 0.084 | 0.233 | 0.395 |
| `http_outer:Create` | 23,586 | — | 5889.39 | 0.250 | 0.187 | 0.745 | 2.259 |
| `http_outer:ForkDisk` | 151 | — | 1099.21 | 7.280 | 7.279 | 19.675 | 40.563 |
| `http_outer:DeleteDisk` | 178 | — | 631.24 | 3.546 | 1.927 | 8.885 | 70.333 |
| `http_outer:Unlink` | 2,770 | — | 218.72 | 0.079 | 0.078 | 0.207 | 0.243 |
| `http_outer:CreateDisk` | 28 | — | 102.00 | 3.643 | 3.750 | 9.333 | 20.800 |
| `http_outer:MountDisk` | 154 | — | 53.35 | 0.346 | 0.369 | 0.493 | 0.910 |
| `http_outer:ReadDirPlus` | 111 | — | 49.87 | 0.449 | 0.323 | 1.311 | 2.262 |
| `http_outer:Mkdir` | 208 | — | 20.62 | 0.099 | 0.081 | 0.240 | 0.820 |
| `http_outer:FsyncDir` | 247 | — | 18.05 | 0.073 | 0.064 | 0.217 | 0.246 |
| `http_outer:ReclaimOrphan` | 16 | — | 7.99 | 0.499 | 0.286 | 3.000 | 4.600 |
| `http_outer:GetDisk` | 10 | — | 7.78 | 0.778 | 0.500 | 3.750 | 4.750 |
| `http_outer:Rmdir` | 8 | — | 1.15 | 0.144 | 0.138 | 0.400 | 0.480 |
| `http_outer:StatFS` | 1 | — | 0.19 | 0.190 | 0.175 | 0.242 | 0.249 |
| `rpc:Read` | 162,788 | 162,788 / 0 | 36999.31 | 0.227 | 0.040 | 1.195 | 4.764 |
| `rpc:Write` | 26,516 | 26,516 / 0 | 22954.11 | 0.866 | 0.197 | 3.704 | 18.135 |
| `rpc:Fsync` | 23,586 | 23,586 / 0 | 12681.61 | 0.538 | 0.361 | 2.028 | 4.337 |
| `rpc:Lookup` | 105,158 | 104,110 / 1,048 | 5265.34 | 0.050 | 0.042 | 0.097 | 0.224 |
| `rpc:Create` | 23,586 | 23,586 / 0 | 4675.72 | 0.198 | 0.095 | 0.583 | 2.171 |
| `rpc:ForkDisk` | 151 | 151 / 0 | 1088.35 | 7.208 | 7.230 | 19.675 | 40.563 |
| `rpc:DeleteDisk` | 178 | 178 / 0 | 619.35 | 3.479 | 1.913 | 8.885 | 70.333 |
| `rpc:Unlink` | 2,770 | 2,770 / 0 | 149.44 | 0.054 | 0.049 | 0.099 | 0.213 |
| `rpc:CreateDisk` | 28 | 28 / 0 | 99.56 | 3.556 | 3.625 | 9.000 | 20.800 |
| `rpc:MountDisk` | 154 | 154 / 0 | 42.13 | 0.274 | 0.307 | 0.485 | 0.730 |
| `rpc:ReadDirPlus` | 111 | 111 / 0 | 33.12 | 0.298 | 0.254 | 0.893 | 0.979 |
| `rpc:Mkdir` | 208 | 208 / 0 | 13.15 | 0.063 | 0.045 | 0.140 | 0.495 |

### 4.2 `StoreActor` 读写路径、`BlobStore` 与持久化子阶段延迟

| 引擎内部子阶段 | 次数 | 累计耗时 (ms) | Mean (ms) | P50 (ms) | P95 (ms) | P99 (ms) |
| :--- | ---: | ---: | ---: | ---: | ---: | ---: |
| `actor_phase:pack_sync_barrier` | 23,074 | 5743.91 | 0.249 | 0.146 | 1.087 | 2.638 |
| `actor_phase:wal_apply_sync` | 20,455 | 4290.56 | 0.210 | 0.177 | 0.485 | 2.135 |
| `actor_phase:phase_a_validate` | 67,153 | 690.84 | 0.010 | 0.008 | 0.024 | 0.046 |
| `actor_phase:wal_apply_nosync` | 43,540 | 398.13 | 0.009 | 0.007 | 0.021 | 0.025 |
| `actor_phase:dead_pack_remove` | 41 | 225.21 | 5.493 | 6.694 | 9.669 | 9.934 |
| `actor_phase:phase_b_mutate` | 67,153 | 188.83 | 0.003 | 0.003 | 0.007 | 0.014 |
| `read_op:read` | 162,788 | 34721.96 | 0.213 | 0.025 | 1.137 | 4.742 |
| `read_op:lookup` | 105,158 | 2181.27 | 0.021 | 0.017 | 0.045 | 0.185 |
| `read_op:readdirplus` | 111 | 25.40 | 0.229 | 0.156 | 0.769 | 0.954 |
| `read_op:getattr` | 11 | 0.47 | 0.043 | 0.038 | 0.072 | 0.095 |
| `read_op:statfs` | 190 | 0.04 | 0.000 | 0.003 | 0.005 | 0.005 |
| `kv_read:point_get` | 648,928 | 4046.65 | 0.006 | 0.004 | 0.020 | 0.041 |
| `kv_read:prefix_scan` | 12,628 | 370.03 | 0.029 | 0.018 | 0.048 | 0.247 |
| `control_open_actor:ok` | 179 | 380.55 | 2.126 | 1.834 | 3.914 | 6.050 |
| `actor_queue_wait` | 77,232 | 9091.03 | 0.118 | 0.004 | 0.423 | 2.250 |
| `inode_lock_wait` | 207 | 1904.98 | 9.203 | 8.736 | 23.247 | 24.799 |
| `blob_pread` | 163,263 | 29594.97 | 0.181 | 0.003 | 1.554 | 4.046 |
| `blob_pwrite` | 26,560 | 5432.09 | 0.205 | 0.017 | 1.354 | 3.401 |
| `pebble_wal_fsync` | 2,209 | 517.86 | 0.234 | 0.090 | 1.132 | 1.999 |
| `blob_sync:pack_data` | 15,461 | 5006.77 | 0.324 | 0.192 | 1.479 | 3.341 |
| `blob_sync:packs_dir` | 133 | 36.14 | 0.272 | 0.004 | 2.102 | 4.169 |

- **Pebble 状态**：BlockCache 命中率 `99.40%` · ReadAmp `1.0` · WriteStall `0.0` · WAL 写入 `3562209.0` B
- **BlobStore 读写分布**：写块字节 `{"extend_tail": 90533888.0, "falloc_zero": 0.0, "full_block": 9781116928.0, "rmw": 0.0, "zero_elide": 0.0}` · 读源块计数 `{"disk_pack": 300097.0, "hot_cache": 6144.0}`

## 5. Jaeger 跨端慢链路 Top 15（端到端瀑布定位）

- 已采样链路：`1330` 条（跨 Client+Server 全链路 `763` 条，异常链路 `132` 条；完整明细见 `jaeger/slow_traces.json`）

| TraceID | 总耗时 (ms) | 入口操作 | Client RPC (ms) | Server RPC (ms) | 同跳网络+编解码差值 (ms) | 最耗时子 Span |
| :--- | ---: | :--- | ---: | ---: | ---: | :--- |
| `c7327a5fd108d453` | 283.73 | `bowl-client:read_ahead.Prefetch` | 282.39 | 20.03 | 262.37 | `bowl-client:storage.v1.StorageService/Read` (282.39 ms) |
| `a87bc190b9bc1138` | 199.88 | `bowl-client:read_ahead.Prefetch` | 199.68 | 3.35 | 196.33 | `bowl-client:storage.v1.StorageService/Read` (199.68 ms) |
| `1f273b275834d808` | 198.07 | `bowl-client:read_ahead.Prefetch` | 198.00 | 3.53 | 194.48 | `bowl-client:storage.v1.StorageService/Read` (198.00 ms) |
| `e83a9a76de70ba2e` | 194.69 | `bowl-client:read_ahead.Prefetch` | 194.26 | 3.81 | 190.45 | `bowl-client:storage.v1.StorageService/Read` (194.26 ms) |
| `da39a3148575c77b` | 183.56 | `bowl-client:read_ahead.Prefetch` | 181.08 | 2.69 | 178.39 | `bowl-client:storage.v1.StorageService/Read` (181.08 ms) |
| `c90a0f73db4e4296` | 169.88 | `bowl-client:read_ahead.Prefetch` | 168.86 | 3.20 | 165.66 | `bowl-client:storage.v1.StorageService/Read` (168.86 ms) |
| `d548839bc0cb8281` | 166.86 | `bowl-client:read_ahead.Prefetch` | 166.77 | 2.56 | 164.21 | `bowl-client:storage.v1.StorageService/Read` (166.77 ms) |
| `aa06c4fe71aae8e0` | 163.15 | `bowl-client:read_ahead.Prefetch` | 160.80 | 4.14 | 156.65 | `bowl-client:storage.v1.StorageService/Read` (160.80 ms) |
| `90d72bc3da2dd9bb` | 153.34 | `bowl-client:read_ahead.Prefetch` | 150.25 | 3.96 | 146.28 | `bowl-client:storage.v1.StorageService/Read` (150.25 ms) |
| `200c57b9702bdd34` | 142.85 | `bowl-client:read_ahead.Prefetch` | 140.43 | 3.40 | 137.03 | `bowl-client:storage.v1.StorageService/Read` (140.43 ms) |
| `5e5d99fb451e0b41` | 140.27 | `bowl-client:read_ahead.Prefetch` | 140.03 | 3.20 | 136.83 | `bowl-client:storage.v1.StorageService/Read` (140.03 ms) |
| `9b63f95fa6cc57cf` | 137.91 | `bowl-client:read_ahead.Prefetch` | 137.30 | 2.31 | 134.99 | `bowl-client:storage.v1.StorageService/Read` (137.30 ms) |
| `34bb8723c7c6c594` | 132.60 | `bowl-client:read_ahead.Prefetch` | 130.87 | 3.85 | 127.02 | `bowl-client:storage.v1.StorageService/Read` (130.87 ms) |
| `45e0a53643a01f0f` | 131.47 | `bowl-client:fuse.Read` | 131.47 | 0.00 | — | `bowl-client:read_ahead.Wait` (131.33 ms) |
| `20635ef01c59b571` | 126.45 | `bowl-client:read_ahead.Prefetch` | 126.33 | 3.55 | 122.78 | `bowl-client:storage.v1.StorageService/Read` (126.33 ms) |

## 6. 结构化日志、Pebble 事件与异常审计（Loki + Client Log + Pebble）

- **Loki 业务容器日志条数**：`904` · **服务端 Warn/Error 数**：`455` · **客户端 Warn/Error 数**：`0`（明细见 `loki/`）
- **Pebble 引擎日志**：`LOG` 文件 `0` 个 · `pebble_events.log` `1` 个 · 告警事件 `0` 条 · 事件统计 `{"open_begin": 1, "wal_created": 14, "flush_begin": 10, "table_created": 19, "flush_end": 10, "compaction_begin": 9, "compaction_end": 9}`

## 7. 在线剖析热点索引（`pprof_top/`，同时提取 `-flat` 与 `-cum`）

| 端 | 剖析文件 | Top-1 自身热点 (`-flat`) | Top-1 累计热点 (`-cum`) | 文本明细 |
| :--- | :--- | :--- | :--- | :--- |
| `server` | `suite1_fork_matrix_cpu.pb.gz` | `3.61s 50.14% 50.14%      3.61s 50.14%  internal/runtime/syscall/linux.Syscall6` | `3.61s 50.14% 50.14%      3.61s 50.14%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_suite1_fork_matrix_cpu.txt` |
| `server` | `suite1_fork_matrix_heap.pb.gz` | `32.01MB 29.04% 29.04%    32.01MB 29.04%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    74.84MB 67.91%  connectrpc.com/connect.(*Handler).ServeHTTP` | `pprof_top/server_suite1_fork_matrix_heap.txt` |
| `server` | `suite1_fork_matrix_mutex.pb.gz` | `—` | `—` | `pprof_top/server_suite1_fork_matrix_mutex.txt` |
| `server` | `suite2_online_fork_cpu.pb.gz` | `3.34s 38.79% 38.79%      3.34s 38.79%  internal/runtime/syscall/linux.Syscall6` | `0.02s  0.23%  0.23%      3.94s 45.76%  net/http.(*http2serverConn).runHandler` | `pprof_top/server_suite2_online_fork_cpu.txt` |
| `server` | `suite2_online_fork_heap.pb.gz` | `32.01MB 31.63% 31.63%    32.01MB 31.63%  github.com/starsrc/bowl/services/storage/internal/server.init.func1` | `0     0%     0%    77.46MB 76.54%  github.com/starsrc/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_suite2_online_fork_heap.txt` |
| `server` | `suite2_online_fork_mutex.pb.gz` | `—` | `—` | `pprof_top/server_suite2_online_fork_mutex.txt` |
| `server` | `suite3_fanout_cow_cpu.pb.gz` | `3.68s 55.34% 55.34%      3.68s 55.34%  internal/runtime/syscall/linux.Syscall6` | `3.68s 55.34% 55.34%      3.68s 55.34%  internal/runtime/syscall/linux.Syscall6` | `pprof_top/server_suite3_fanout_cow_cpu.txt` |
| `server` | `suite3_fanout_cow_heap.pb.gz` | `94.94MB 48.62% 48.62%    94.94MB 48.62%  bytes.growSlice` | `0     0%     0%   169.65MB 86.87%  github.com/starsrc/pkg/serverx.NewHTTPServer.MaxBytesHandler.func1` | `pprof_top/server_suite3_fanout_cow_heap.txt` |
| `server` | `suite3_fanout_cow_mutex.pb.gz` | `—` | `—` | `pprof_top/server_suite3_fanout_cow_mutex.txt` |
| `client` | `suite1_fork_matrix_cpu.pb.gz` | `10ms   100%   100%       10ms   100%  compress/flate.(*compressor).deflate` | `0     0%     0%       10ms   100%  bufio.(*Writer).Flush` | `pprof_top/client_suite1_fork_matrix_cpu.txt` |
| `client` | `suite1_fork_matrix_heap.pb.gz` | `5953.18kB 57.58% 57.58%  5953.18kB 57.58%  github.com/hanwen/go-fuse/v2/fuse.NewServer.func2` | `0     0%     0%     5.81MB 57.58%  github.com/hanwen/go-fuse/v2/fuse.(*fuseFD).readRequest` | `pprof_top/client_suite1_fork_matrix_heap.txt` |
| `client` | `suite1_fork_matrix_mutex.pb.gz` | `—` | `—` | `pprof_top/client_suite1_fork_matrix_mutex.txt` |
| `client` | `suite2_online_fork_cpu.pb.gz` | `1.24s 37.80% 37.80%      1.24s 37.80%  internal/runtime/syscall/linux.Syscall6` | `0     0%     0%      1.51s 46.04%  net/http.(*http2clientStream).doRequest` | `pprof_top/client_suite2_online_fork_cpu.txt` |
| `client` | `suite2_online_fork_heap.pb.gz` | `16.01MB 31.60% 31.60%    16.01MB 31.60%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%    35.95MB 70.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_suite2_online_fork_heap.txt` |
| `client` | `suite2_online_fork_mutex.pb.gz` | `—` | `—` | `pprof_top/client_suite2_online_fork_mutex.txt` |
| `client` | `suite3_fanout_cow_cpu.pb.gz` | `10ms 33.33% 33.33%       10ms 33.33%  aeshashbody` | `0     0%     0%       20ms 66.67%  bufio.(*Writer).Flush` | `pprof_top/client_suite3_fanout_cow_cpu.txt` |
| `client` | `suite3_fanout_cow_heap.pb.gz` | `16.01MB 31.60% 31.60%    16.01MB 31.60%  github.com/starsrc/bowl/client/internal/fusefs.init.func3` | `0     0%     0%    35.95MB 70.98%  github.com/hanwen/go-fuse/v2/fuse.(*Server).loop` | `pprof_top/client_suite3_fanout_cow_heap.txt` |
| `client` | `suite3_fanout_cow_mutex.pb.gz` | `—` | `—` | `pprof_top/client_suite3_fanout_cow_mutex.txt` |
