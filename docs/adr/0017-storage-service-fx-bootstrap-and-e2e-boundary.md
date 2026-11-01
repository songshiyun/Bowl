# 0017. Storage 服务启动装配、ConnectRPC 错误码传递与黑盒端到端（E2E）测试规范

## 状态
已采纳 (accepted)

## 背景
`StorageService` 的 36 个元数据与磁盘配额接口已经在 `services/storage/internal/meta` 里实现完毕，请求参数校验也已在 `pkg/validator` 中写好，但还缺少服务启动入口（`cmd/main.go`）、环境变量配置加载（`internal/config`）、数据库连接池管理（`internal/db`）以及 ConnectRPC 服务端处理层（`internal/server`）。在把服务真正跑起来并编写端到端测试时，需要敲定三个问题：
1. **ConnectRPC 版本选择与公共库兼容**：仓库之前生成的 Connect 代码用了 `connectrpc.com/connect/v2` 测试版，而 `Aether` 共享库里的日志和链路追踪组件（`pkg/observability/{slogx,telemetry}`）以及社区的 `otelconnect` 拦截器都是基于 `connectrpc.com/connect` v1 稳定版写的；同时需要明确作为内网服务的 `StorageService` 是否需要对外提供 REST 接口转换。
2. **如何把具体的 POSIX 错误码（`ErrorInfo.Reason`）原封不动传给客户端**：如果直接用通用的 `connectrpc.com/validate` 拦截器，它会把所有参数校验错误统一变成普通的 `CodeInvalidArgument`，不带具体原因，导致 FUSE 客户端分不清到底是文件名太长（`ENAMETOOLONG`）、属性超出范围（`ERANGE`）、列表太大（`E2BIG`）还是普通参数错误（`EINVAL`）；另外业务层 `meta.ToStatus` 返回的 24 种 POSIX 错误原因也需要通过 ConnectRPC 传给客户端，同时还要把未知的内部报错信息隐藏掉以免泄露内部细节。
3. **本地开发容器的端口规划与黑盒 E2E 测试目录位置**：本地开发机上往往同时跑着 `Aether` 的容器（占用了 `8000`、`8080`、`8081` 等 8000 段端口），需要避免端口冲突，并明确针对真实容器的端到端测试放在哪个目录、用什么编译标签隔离。

## 决策
1. **使用 `uber-go/fx` 组装 ConnectRPC v1 服务，不做 REST 转码**：
   - 将 `buf.gen.yaml` 和 Go 依赖统一固定为 `connectrpc.com/connect` v1 稳定版，直接使用 `github.com/starsrc/Aether/pkg/observability/{slogx,telemetry}` 和 `github.com/starsrc/Aether/pkg/tx`。
   - `StorageService` 只作为内部网络调用的 RPC 服务（暂不加公网 JWT 认证，也不转成 REST 接口），直接挂载在标准库的 `http.NewServeMux` 上，并同时开启 HTTP/1.1 和免加密的 HTTP/2（`h2c`）。
   - 使用 `go.uber.org/fx` 管理 `config.Module`、`telemetry.Module`、`slogx.Module`、`db.Module`、`meta.Module` 和 `server.Module` 的初始化与按序退出；在 `config.Module` 中除了基础服务、数据库、日志和追踪配置外，增加 `MetaConfig`（用来配置会话过期时间 `SessionLeaseTTL`、`AtimeMode` 和 FUSE 缓存超时），传给 `meta.New`。
2. **使用自定义 `pkg/validator` 拦截器，在 ConnectRPC 错误中附带 POSIX `ErrorInfo`**：
   - 拦截器从外到内依次为：`slogx.NewMaskInterceptor`（最外层屏蔽 `CodeInternal` / `CodeUnknown` 的内部报错文本，但保留 `Details` 里的结构化错误码） -> `otelconnect.NewInterceptor`（链路追踪） -> `slogx.NewConnectInterceptor`（记录请求日志） -> 基于 `pkg/validator.NewStorageValidator()` 的参数校验拦截器。
   - 在参数校验拦截器和 `StorageServiceHandler` 实现中，统一把 `validator.ToStatus(err)` 和 `meta.ToStatus(err)` 生成的 gRPC `*status.Status`（包含状态码和 `google.rpc.ErrorInfo{Reason, Domain: "bowl.storage"}`）转成 `*connect.Error`，并通过 `connect.NewErrorDetail` 附加上错误详情，确保客户端调 `connect.Error.Details()` 就能拿到准确的 POSIX `errno` 名称。
3. **容器统一使用 9000+ 端口，黑盒测试统一放在根目录 `e2e/` 下**：
   - 所有的端到端黑盒测试统一放在仓库根目录的 `e2e/` 下（归根目录 `go.mod` 管理），测试文件顶部统一加上 `//go:build e2e` 编译标签；测试辅助包 `e2e/testutil` 不加编译标签，方便平时跑 `go vet ./...` 时一并检查语法；`e2e/` 下的代码一律不准引用 `services/storage/internal/...` 内部包。
   - Bowl 的 Docker 容器统一使用 `9000` 以上的端口（`storage-service` 默认监听并映射 `9000:9000`），配有开箱即用的 `docker-compose.yaml`（包含 `bowl-postgres`、`bowl-migration`、`bowl-storage-service`、`bowl-jaeger`）以及 `Taskfile.yml` 快捷命令（`task build`、`task run`、`task down`、`task test:e2e`）。

## 结果
- **正面效果**：完成了 `StorageService` 从读取配置、连数据库、执行业务逻辑到启动 ConnectRPC 监听的完整流程；FUSE 客户端和黑盒测试可以通过标准 ConnectRPC 接口拿到全部 24 种 POSIX 错误码；使用 9000+ 端口和 `//go:build e2e` 标签后，既不会跟本地 `Aether` 容器抢端口，平时跑普通单元测试也不需要先启动 Docker 容器。
- **代价与取舍**：需要把根模块和 `services/storage` 的 ConnectRPC 依赖统一改为 `connectrpc.com/connect` v1 并重新生成 `gen/go` 代码。
