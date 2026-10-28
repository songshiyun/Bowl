1. ceph, computer, juicefs, drive9 这四个项目为参考项目，他们是我找的权威的开源的一些项目，有可参考的内容（例如 JuiceFS 的 FUSE 适配位于 `pkg/fuse/`、VFS 语义位于 `pkg/vfs/`、元数据与锁/会话位于 `pkg/meta/{base,sql,sql_lock}.go`）。

## Agent skills

### Build, test & tooling

多模块目录布局、`.worktrees` `GOWORK=off` 隔离红线、`task` 构建/测试/代码生成/迁移命令与 Mock 打桩规范（检索代码模块或执行任何构建、测试、代码生成命令前必读）。See [docs/agents/tooling.md](docs/agents/tooling.md).

### Issue tracker

Tracked in Linear (Team: `StarSrc`, Project: `Bowl`) via `ACE_linear` MCP. See [docs/agents/issue-tracker.md](docs/agents/issue-tracker.md).

### Triage labels

Canonical triage roles mapped to Linear labels. See [docs/agents/triage-labels.md](docs/agents/triage-labels.md).

### Domain docs

Single-context layout at repo root. See [docs/agents/domain.md](docs/agents/domain.md).

### Coding standards

Review-stage engineering and test-authenticity standards. See [CODING_STANDARDS.md](CODING_STANDARDS.md).
