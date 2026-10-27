# 0018. 全面对齐并复用 Aether 共享底座 (`pkg/serverx`、`pkg/observability`、`pkg/tx`)

## 状态
已采纳 (accepted) — 演进 [ADR-0017](0017-storage-service-fx-bootstrap-and-e2e-boundary.md)

## 背景
在 [ADR-0017](0017-storage-service-fx-bootstrap-and-e2e-boundary.md) 完成 `StorageService` 的可运行装配后，跨仓架构审查发现 `bowl/services/storage` 与 `Aether` 之间存在四处基础设施重复与浅模块（Shallow Module）摩擦：
1. **浅层配置包装重复**：`services/storage/internal/config` 维护了独立的 `config_server.go`、`config_log.go`、`config_telemetry.go` 与 `config_db.go`（超过 250 行单行 Getter 透传代码），未能直接复用 `Aether/pkg/serverx`、`Aether/pkg/observability/{slogx,telemetry}` 与 `Aether/pkg/tx` 的内建环境配置结构体。
2. **HTTP/ConnectRPC 运行时与生命周期重复**：由于 `StorageService` 需要挂载附加 POSIX `ErrorInfo` 的专属 `pkg/validator` 拦截器，此前未能接入 `Aether/pkg/serverx`，而是在 `internal/server/{server.go,interceptor.go}` 中重写了 `h2c` 服务构造、洋葱拦截器链组装与 Fx 启停生命周期，且启动期同步 `net.Listen` 行为与 `Aether` 的异步监听 + `fx.Shutdowner` 基准不一致。
3. **跨层领域哨兵枚举泄漏**：`internal/server/errors.go` 为区分 `meta.ErrInterrupted` 与纯 `context.Canceled`/`DeadlineExceeded`，将 `meta/status.go` 中的 24 个领域哨兵错误在 `server` 包中重复枚举了一份（`domainSentinels`），且 `ExtractErrorInfo` 在 `internal/server` 与 `e2e/testutil` 中重复实现。
4. **连接池装配与事务冲突重试样板**：`internal/db/store.go` 的 `OpenPool`/`ProvidePool` 与 `Aether` 完全同构，且 `internal/meta/manager.go` 手写了外层事务重试循环 `execWriteTxWithRetry`。

## 决策
1. **配置结构体匿名嵌入对齐（裁撤 4 个浅配置模块）**：
   - `services/storage/internal/config.Config` 直接匿名嵌入 `serverx.ServerConfig`、`slogx.LogConfig`、`telemetry.TelemetryConfig` 与 `tx.PoolConfig`，仅保留领域专属的 `Meta MetaConfig`。
   - 在 `config.Load` 中若未显式设置 `SERVER_PORT` 则默认设为 `9000`，默认遥测服务名设为 `"bowl-storage"`；直接删除 `config_server.go`、`config_log.go`、`config_telemetry.go` 与 `config_db.go`。
2. **全面接入 `Aether/pkg/serverx` 并对齐异步监听生命周期**：
   - 通过 `serverx` 开放的校验器接缝注入 `validator.NewStorageValidator()` 拦截器，复用 `serverx.NewHTTPServer` 与 `serverx.RegisterSingleLifecycle`。
   - 将 `StorageService` 的监听生命周期与单测全面对齐至 `Aether` 异步监听 + `fx.Shutdowner` 级联退出基准。
3. **收拢 `meta.ToStatus` 上下文取消判定并复用 `serverx` 错误转换**：
   - `meta.ToStatus(err)` 在未命中 24 个领域哨兵错误时，直接将纯 `context.Canceled` 与 `context.DeadlineExceeded` 映射为不携带 `ErrorInfo` 的 `codes.Canceled` 与 `codes.DeadlineExceeded`。
   - 删除 `internal/server/errors.go` 中的 `domainSentinels` 数组，服务端与 `e2e/testutil` 统一复用 `serverx.StatusToConnectError` 与 `serverx.ExtractErrorInfo`。
4. **复用深化后的 `Aether/pkg/tx` 连接池生命周期与事务重试 `Option`**：
   - `services/storage/internal/db` 直接委托 `tx.OpenPool` 与 `tx.ProvidePool`（保留向 Fx 暴露 `*pgxpool.Pool` 与 `db.Store` 的现有契约）。
   - `meta.manager` 移除手写的 `execWriteTxWithRetry` 循环，直接通过 `store.ExecTxWithOptions(ctx, fn, tx.WithDefaultRetry(errRetryTx))` 委托 `pkg/tx` 执行指数退避重试。

## 结果
- **积极影响**：彻底消除 `bowl` 与 `Aether` 之间的配置、HTTP/Fx 生命周期、错误详情拆箱、数据库连接池及事务退避重试重复代码；新增 POSIX 领域哨兵错误只需修改 `meta/status.go` 单一位置。
- **代价考量**：`services/storage` 升级依赖 `github.com/starsrc/Aether/pkg/{serverx,observability,tx}`，并将服务端生命周期测试对齐至 `Aether` 异步监听契约。
