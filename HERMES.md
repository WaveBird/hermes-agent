<!-- superpowers-zh:begin (do not edit between these markers) -->
# Superpowers-ZH 中文增强版

本项目已安装 superpowers-zh 技能框架（15 个新增 skills + 5 个已有 Hermes 内置 skill 对应）。

## 核心规则

1. **收到任务时，先检查是否有匹配的 skill** — 哪怕只有 1% 的可能性也要检查
2. **设计先于编码** — 收到功能需求时，先用 brainstorming skill 做需求分析
3. **测试先于实现** — 写代码前先写测试（TDD）
4. **验证先于完成** — 声称完成前必须运行验证命令

## 工具映射

技能中引用的 Claude Code 工具名称对应 Hermes Agent 的等价工具：
- `Read` → `read_file`
- `Write` → `write_file`
- `Edit` → `patch`
- `Bash` → `terminal`
- `Grep` / `Glob` → `search_files`
- `Skill` → `skill_view`
- `Task`（子智能体） → `delegate_task`
- `WebSearch` → `web_search`
- `WebFetch` → `web_extract`
- `TodoWrite` → `todo`

## 新增 Superpowers Skills（15 个）

| Skill | 用途 |
|-------|------|
| **brainstorming** | 需求分析 → 设计规格，不写代码先想清楚 |
| **chinese-code-review** | 中文 review 沟通参考 — 仅 `/chinese-code-review` 手动调用 |
| **chinese-commit-conventions** | 中文 commit 规范 — 仅 `/chinese-commit-conventions` 手动调用 |
| **chinese-documentation** | 中文排版规范、告别机翻味 — 仅 `/chinese-documentation` 手动调用 |
| **chinese-git-workflow** | Gitee/Coding/极狐/CNB Git 工作流 — 仅 `/chinese-git-workflow` 手动调用 |
| **dispatching-parallel-agents** | 2+ 独立任务并发执行 |
| **executing-plans** | 按计划逐步实施，每步验证 |
| **finishing-a-development-branch** | 合并/PR/保留/丢弃四选一 |
| **mcp-builder** | 构建生产级 MCP 服务器 |
| **receiving-code-review** | 技术严谨地处理审查反馈，拒绝敷衍 |
| **using-git-worktrees** | 隔离式特性开发 |
| **using-superpowers** | 元技能：如何调用和优先使用 skills |
| **verification-before-completion** | 证据先行 — 声称完成前必须跑验证 |
| **workflow-runner** | YAML 多角色工作流执行器 |
| **writing-skills** | 创建新 skill 的方法论 |

## 已有 Hermes 内置 Skill 对应（5 个）

| Superpowers Skill | Hermes 内置 Skill | 说明 |
|-------------------|-------------------|------|
| test-driven-development | test-driven-development | Hermes 版已适配本工具 |
| systematic-debugging | systematic-debugging | Hermes 版已适配本工具 |
| writing-plans | writing-plans | Hermes 版已适配本工具 |
| requesting-code-review | requesting-code-review | Hermes 版已适配本工具 |
| subagent-driven-development | subagent-driven-development | Hermes 版已适配本工具 |

## 如何使用

当任务匹配某个 skill 时，使用 `skill_view` 加载对应 skill 并严格遵循其流程。
<!-- superpowers-zh:end -->