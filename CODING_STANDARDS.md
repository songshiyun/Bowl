# Bowl Coding Standards

This document defines non-mechanical engineering standards enforced during the **Standards** axis of `/code-review`. Mechanical checks (`buf lint`, `gofmt -s`, `go vet`) are enforced by `task lint`.

## 1. Test Authenticity & Independent Oracles

- **No Silent Error Swallowing**: Never guard assertions behind `if err == nil` or broad error whitelists (`ErrIO`, `context.DeadlineExceeded`) that allow broken execution paths to pass silently. Assert exact expected sentinel errors (`require.ErrorIs`) or `require.NoError`.
- **Independent Reference Model (No Self-Oracle)**: State-machine and integration assertions must compute expected post-state from an independent in-memory model or pre-operation snapshot, never by reading back from the system under test to overwrite the model's expectation.
- **删除与回收的直接检查**：验证磁盘删除、孤儿回收或会话过期时，直接按目标编号检查 PostgreSQL 控制面记录和 Pebble 对应的节点、孤儿标记及数据键；不要通过已删除的父记录做连接查询。

## 2. Deterministic Replay & Concurrency Control

- **Deterministic Map Iteration**: In seed-driven state-machine tests (`TestFormal*`), never iterate directly over Go `map` types when selecting candidate actions, targets, or probes. Always extract keys and sort them (`slices.Sort`) before sampling from the PRNG so any `BOWL_FORMAL_SEED` reproduces the exact execution trace.
- **PRNG Stream Isolation**: Read-only probes and invariant auditors must use a dedicated secondary PRNG (`env.ProbeRNG()`) rather than advancing the primary action PRNG (`env.Rng`), ensuring changes to probe frequency never alter the state-transition trajectory.
- **Explicit Timeout Bounds on Blocking Operations**: Tests invoking potentially blocking operations (`SetLkw` expected not to block, background lease sweep) must bind a bounded `context.WithTimeout` and synchronize lease TTLs with sleep intervals so tests fail fast rather than hanging or masking race conditions.

## 3. Cross-Layer Error & Domain Consistency

- **数据库约束错误映射**：在 `internal/meta` 边界将 PostgreSQL 约束错误转换为 `status.go` 定义的业务错误，例如写端租约冲突映射为 `ErrBusy`；不要向 RPC 调用方泄露原始 `pgconn.PgError`。
- **Ubiquitous Language**: Align all identifiers, comments, and error messages with [`GLOSSARY.md`](GLOSSARY.md).
