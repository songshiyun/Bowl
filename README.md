# Bowl

Bowl 是一个基于 ConnectRPC/HTTP2、PostgreSQL 与 Pebble 存储引擎构建的分布式虚拟磁盘系统，提供 Linux FUSE POSIX 文件系统挂载、单写者世代栅栏以及零拷贝 Fork / Diff / 三路 Merge 能力。

## 核心架构

- **控制面（`services/storage`）**：基于 Uber Fx 与 ConnectRPC 提供虚拟磁盘生命周期、挂载软租约、WriterEpoch 写栅栏与血缘提交协调，磁盘元数据与血缘持久化于 PostgreSQL 18。
- **存储引擎（`services/storage/internal/store`）**：按磁盘调度单盘 `StoreActor`，通过 Pebble 键值索引与 64 MiB Append-Only `.pack` 数据容器实现 Group Commit 与在线压缩回收。
- **Linux FUSE 客户端（`client`）**：基于 `hanwen/go-fuse/v2` 实现守护进程化挂载、名字空间缓存、预读/异步写回流水线及 `bowl` CLI 工具。

## 三步快速启动

1. **启动服务端容器栈**（PostgreSQL + Goose 迁移 + Storage Service + Jaeger）：
   ```bash
   task run
   ```
2. **构建并测试 Linux FUSE 客户端**（在 Docker Linux 容器内构建与运行）：
   ```bash
   task test:client
   ```
3. **预览开发手册**（自动生成 RPC 文档并启动 VitePress）：
   ```bash
   task docs:install && task docs:dev
   ```

## 目录地图

| 路径 | 职责 |
| --- | --- |
| `proto/` 与 `gen/` | Protobuf / Buf 契约定义（`storage/v1/storage.proto`）与生成代码 |
| `pkg/` | 跨模块共享编解码（`rpccodec`）与校验器（`validator`） |
| `services/storage/` | 存储服务端（`server`、`control`、`store`、`db`）与 Goose 迁移 |
| `client/` | `bowl` CLI 与 Linux FUSE 客户端实现及容器 E2E 测试 |
| `deploy/` | `dev` / `prod` Docker Compose 与部署配置 |
| `e2e/` 与 `benchmark/` | 跨服务 E2E 测试及 `pjdfstest` / `drive9` / `fork` / `juicefs` 基准套件 |
| `docs/` | Bowl Developer Book（VitePress 源码与生成脚本） |

## 开发手册

完整架构设计、功能规格、接口契约与二次开发指南见 [Bowl Developer Book](docs/index.md)：

- **全局认知**：[能力全景](docs/01-overview/capabilities.md) · [系统架构](docs/01-overview/architecture.md) · [快速开始](docs/01-overview/quickstart.md)
- **功能规格**：[磁盘生命周期](docs/02-features/disk-lifecycle.md) · [派生、差异与三路合并](docs/02-features/fork-diff-merge.md) · [挂载租约与写栅栏](docs/02-features/mount-and-fencing.md) · [POSIX 语义与扩展属性](docs/02-features/posix-semantics.md)
- **接口契约**：[CLI 命令参考](docs/03-interfaces/cli-reference.md) · [RPC 接口契约](docs/03-interfaces/rpc-api.md) · [可观测性与运维指标](docs/03-interfaces/observability.md)
- **内部实现**：[Storage 控制面](docs/04-internals/storage-control-plane.md) · [Storage 存储引擎](docs/04-internals/storage-store-engine.md) · [Client 与 FUSE 流水线](docs/04-internals/client-fuse-pipeline.md) · [二次开发指南](docs/04-internals/extending-bowl.md)
