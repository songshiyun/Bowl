# Bowl 构建、代码生成、迁移与测试工具手册

## 多模块与目录布局

- **根模块（[`go.mod`](../../go.mod)）**：包含 `proto/storage/v1/`（Protobuf 源文件）、`gen/go/`（ConnectRPC 生成代码）、[`pkg/validator`](../../pkg/validator)（请求校验器）、[`tests/contract`](../../tests/contract)（跨模块契约测试）及 [`e2e`](../../e2e)（跨服务黑盒 E2E 测试）。
- **Storage 子模块（[`services/storage/go.mod`](../../services/storage/go.mod)）**：代码位于 `services/storage/internal/{config,control,db,server,store}` 与 `services/storage/db/{migrations,queries}`。
  - `services/storage/internal/store/` 文件职责索引（ADR-0030、ADR-0031 & ADR-0032 单盘独立 Actor 存储引擎与 `64 MiB` Append-Only Pack 数据层）：`actor.go`、`pack_lifecycle.go`、`keys.go`、`values.go`、`errors.go` 实现单盘 `StoreActor` 生命周期、7 类大端序键（含 `sys:stat` `41B` 持久化 `MaxBytes`/`WriterEpoch`、`sys:pack:<packID>` 与 `16B BlobRef`）与 `Comparer.Split`、零拷贝 `SetDeferred`/`DeleteDeferred` 批次编解码、双缓冲 MPSC Group Commit 循环、Phase A `WriterEpoch` 世代号栅栏与 `AdvanceEpoch` 原子递增及孤儿清扫、`UpdateMaxBytes` 配额水位线校验、`stagedBlobs` 预写租约与 `sys:pack` 账本及全死封口 `.pack` 自动回收；`namespace.go`、`xattr_setattr.go`、`unlink_rename.go` 实现命名空间创建/查询、扩展属性/属性变更、解链/重命名与孤儿节点回收；`blob.go`、`block_grid.go`、`data_rw.go`、`data_partial.go`、`fsync.go`、`compact.go` 实现 `BlobStore` 扁平 `64 MiB` `.pack` 封包容器、常驻句柄池与 `64 MiB` 分片热块缓存、`256 KiB` 逻辑块网格双阶段扫描与账本更新原语、严格 Append-Only 写入流水线（含活跃包最末尾同块 `ExtendTail` 顺延扩展）、无锁并发 `Read`、`Fsync`/`FsyncDir` 双层并行落盘屏障以及 `CompactPacks` 低水位已封口 `.pack` 搬迁回收（含 Phase A `opCompactRelocate` 原子 CAS 并发栅栏与双重持久化屏障）。
  - `services/storage/internal/control/` 文件职责索引（ADR-0032 单盘控制面与 `StoreActor` 实例池）：`control.go` 与 `status.go` 实现 `Manager` 接口（`CreateDisk`、`GetDisk`、`ListDisks`、`UpdateDisk`、`DeleteDisk`、`MountDisk` 6 个控制面操作与 `Actor(ctx, diskID)` 实例池路由）、单盘级并发锁、共享 Pebble Cache 管理与领域错误映射；`services/storage/internal/server/` 数据面 Handler（含 `codec.go` `WriteRequest` 池化零分配解码与 `validator.go` 读写热路径零反射参数快检）通过 `control.Actor` 获取 `*store.StoreActor` 后直连引擎方法。PostgreSQL 集成测试通过 `testdb_helper_test.go` 的辅助函数建立隔离数据库并调用 goose 执行迁移。

- **Client 子模块（[`client/go.mod`](../../client/go.mod)）**：Linux FUSE 客户端 CLI，代码位于 `client/{cmd/bowl,internal/{cli,fusefs,rpc,version},e2e}`；macOS 宿主机下须通过 `task test:client` 与 `task test:client:e2e` 在 Linux 容器内完成构建与挂载测试。
  - `client/internal/cli/` 文件职责索引：`mount.go` 与 `daemon.go` 实现单次 `MountDisk` 握手（获取静态 `Epoch` 与根 `Inode`）及前后台 FUSE 挂载守护进程生命周期；`umount.go` 与 `mountinfo.go` 实现本地挂载表解析与卸载；`disk_*.go` 实现磁盘 CRUD CLI 与 `bowl disk delete` 本机 `/proc/self/mountinfo` 防误删快检。
  - `client/internal/fusefs/` 文件职责索引：`fs.go` 与 `fs_*.go` 实现无会话 `RawFS` FUSE 协议分发与各操作入口（只读 RPC 携带 `DiskId`，写/变更 RPC 携带静态 `DiskId` + `Epoch`，遇旧世代号拦截 `ErrStaleEpoch`/`CodeFailedPrecondition` 直接返回 `EBUSY` 且不自动重连）；`namespace_cache.go` 维护路径/目录/属性缓存与代际失效；`handle_table.go` 管理打开文件与目录句柄；`lock_table.go` 与 `inode_lock.go` 管理本地字节范围锁与 inode 并发锁；`read_ahead.go`（`ReadAheadCoordinator`）封装顺序预读缓存、流水线调度与失效；`write_behind.go`（`WriteBehindCoordinator`）封装连续顺序写 `4 MiB` 聚批、`W*=4` 有界并发 Write RPC 流水线调度、观测/持久化屏障排空与句柄级 `errseq` 异步写错误隔离；`tmpfile_manager.go`（`TmpFileManager`）管理 `O_TMPFILE` 匿名临时文件句柄、本地缓冲与 `Link` 具名化。
- **ADR 查阅约定**：查阅 [`docs/adr/`](../adr) 时，先列出目录获取准确文件名，切勿盲猜文件名。

## Worktree 与模块隔离红线

根目录 `go.work` 已 gitignore。在 `.worktrees/<ticket>` 内执行 `go`（含 `go test`、`go tool cover`）命令时，必须通过根目录 [`Taskfile.yml`](../../Taskfile.yml)（已内置 `GOWORK=off`）或显式指定 `GOWORK=off` 并带 `-C services/storage` 或 `-C client` 运行，切勿在根模块直接跑子模块路径或无隔离运行。

本机只有一套 bowl 容器栈：[`deploy/dev/docker-compose.yaml`](../../deploy/dev/docker-compose.yaml) 与 [`deploy/prod/docker-compose.yaml`](../../deploy/prod/docker-compose.yaml) 固定了项目名（`name: bowl`）、容器名（`bowl-postgres` 等）与端口，`Taskfile.yml` 通过 `ENV=dev|prod`（默认 `dev`）切换环境并固定 `COMPOSE_PROJECT_NAME=bowl`。一律通过 `task` 操作容器栈（绕过 `task` 直接调用 `docker compose` 时显式带 `-f deploy/<env>/docker-compose.yaml`）；容器已在运行时直接复用。多个任务共用同一容器栈时（并行修复节点、深度审查汇合验证），以 `KEEP_E2E_ENV=1 task test:e2e` / `KEEP_E2E_ENV=1 task test:client:e2e` 运行 E2E，避免收尾时关停他人正在使用的容器栈。

## 提交前守门

[`.githooks/pre-commit`](../../.githooks/pre-commit) 对暂存的 Go 文件执行 `gofmt -s` 检查，新克隆仓库先运行一次 `git config core.hooksPath .githooks` 启用（所有工作树共享）。

## 常用任务 (`task`)

- **测试与契约校验**：
  - `task test` / `task test:storage`：运行根模块（含 `pkg/validator`、`tests/contract`）与 `services/storage` 测试。
  - `task test:client` / `task test:client:e2e`：macOS 宿主机下在 Linux 容器内完成 `client` FUSE 客户端构建、单测与挂载 E2E 测试。
  - `task test:e2e`：运行跨服务黑盒 E2E 测试。
  - 修改服务接口或状态码后，提交前必须运行 `task test` 与 `task lint` 确保根模块 `tests/contract` 同步通过。
- **静态检查与 Protobuf 代码生成**：
  - `task lint` / `task buf:lint` / `task buf:generate`：静态检查、校验并生成 `proto/` 下的 ConnectRPC 契约代码。
- **数据库访问与迁移（sqlc / goose）**：
  - `task sqlc:generate`（或 `task sqlc:storage:generate`）：生成 sqlc 数据库访问代码。
  - `task goose:storage:up`（及 `down` / `status`）：执行数据库迁移（默认 DSN：`postgres://postgres:postgres@127.0.0.1:9432/bowl?sslmode=disable`，对应本地容器 `bowl-postgres`）。
## 跨仓事务契约

- **事务管理依赖**：依赖 `github.com/starsrc/Aether/pkg/tx`（`Transactor[Querier]`、`ExecTx`、`WithReadOnly()`、`WithIsoLevel(...)`）。
