# 0018. 直接复用 Aether 公共库（`pkg/serverx`、`pkg/observability`、`pkg/tx`）去掉重复代码

## 状态
已采纳 (accepted) — 演进 [ADR-0017](0017-storage-service-fx-bootstrap-and-e2e-boundary.md)

## 背景
在 [ADR-0017](0017-storage-service-fx-bootstrap-and-e2e-boundary.md) 完成 `StorageService` 的服务启动和配置代码后，对比 `bowl/services/storage` 与 `Aether` 仓库，发现有四处代码完全是重复造轮子：
1. **重复写了 250 多行配置读取包装代码**：`services/storage/internal/config` 里单独维护了 `config_server.go`、`config_log.go`、`config_telemetry.go` 和 `config_db.go`，里面全是一行行的 Getter 转发函数，没有直接用 `Aether/pkg/serverx`、`Aether/pkg/observability/{slogx,telemetry}` 和 `Aether/pkg/tx` 现成的配置结构体。
2. **重复写了 HTTP/ConnectRPC 服务启动和关闭逻辑**：因为 `StorageService` 需要挂载自定义的 `pkg/validator` 请求校验拦截器（用来返回带 POSIX `ErrorInfo` 的错误），之前没有直接用 `Aether/pkg/serverx`，而是在 `internal/server/{server.go,interceptor.go}` 里自己重写了一遍 `h2c` 服务创建、拦截器组装和 Fx 启停代码，而且启动时同步调用 `net.Listen` 的方式也跟 `Aether` 异步监听的做法不一致。
3. **在两个地方重复维护了 24 个错误码列表**：`internal/server/errors.go` 为了区分业务上的 `meta.ErrInterrupted` 和普通的请求取消（`context.Canceled` / `DeadlineExceeded`），把 `meta/status.go` 里的 24 个错误变量在 `server` 包里又抄了一遍（`domainSentinels`）；而且解析 `ErrorInfo` 的 `ExtractErrorInfo` 函数也在 `internal/server` 和 `e2e/testutil` 里重复写了两份。
4. **重复写了数据库连接池初始化和事务重试循环**：`internal/db/store.go` 里的 `OpenPool` / `ProvidePool` 跟 `Aether` 一模一样，而且 `internal/meta/manager.go` 还自己手写了一个事务冲突重试循环 `execWriteTxWithRetry`。

## 决策
1. **直接嵌入 `Aether` 的配置结构体，删掉 4 个多余的配置文件**：
   - 在 `services/storage/internal/config.Config` 中直接嵌入 `serverx.ServerConfig`、`slogx.LogConfig`、`telemetry.TelemetryConfig` 和 `tx.PoolConfig`，只保留存储服务特有的 `Meta MetaConfig`。
   - 在 `config.Load` 中，如果没有设置 `SERVER_PORT` 环境变量就默认设为 `9000`，默认服务名设为 `"bowl-storage"`；直接删掉 `config_server.go`、`config_log.go`、`config_telemetry.go` 和 `config_db.go`。
2. **改用 `Aether/pkg/serverx` 启动 HTTP/ConnectRPC 服务**：
   - 通过 `serverx` 提供的扩展参数把 `validator.NewStorageValidator()` 校验拦截器传进去，直接使用 `serverx.NewHTTPServer` 和 `serverx.RegisterSingleLifecycle`。
   - 将 `StorageService` 的启动监听和退出方式统一对齐到 `Aether` 的做法。
3. **在 `meta.ToStatus` 里统一处理上下文取消错误，复用 `serverx` 的错误转换函数**：
   - 当错误不属于 24 个业务错误之一时，由 `meta.ToStatus(err)` 直接把单纯的 `context.Canceled` 和 `context.DeadlineExceeded` 转成不带 `ErrorInfo` 的 `codes.Canceled` 和 `codes.DeadlineExceeded`。
   - 删掉 `internal/server/errors.go` 里重复的 `domainSentinels` 列表，服务端和 `e2e/testutil` 统一调用 `serverx.StatusToConnectError` 和 `serverx.ExtractErrorInfo`。
4. **复用 `Aether/pkg/tx` 的数据库连接池和事务重试功能**：
   - `services/storage/internal/db` 直接调用 `tx.OpenPool` 和 `tx.ProvidePool` 创建连接池。
   - `meta.manager` 删掉手写的 `execWriteTxWithRetry` 循环，直接调用 `store.ExecTxWithOptions(ctx, fn, tx.WithDefaultRetry(errRetryTx))` 让 `pkg/tx` 负责遇到冲突时自动重试。

## 结果
- **正面效果**：删掉了 `bowl` 与 `Aether` 之间重复的配置、HTTP 服务启停、错误解析、数据库连接池和事务重试代码；以后新增 POSIX 业务错误码只需要改 `meta/status.go` 一个地方。
- **代价与取舍**：`services/storage` 升级了对 `github.com/starsrc/Aether/pkg/{serverx,observability,tx}` 的依赖版本，并同步更新了服务启停的单元测试。
