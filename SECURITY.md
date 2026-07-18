# 安全策略

## 支持范围

安全修复面向最新发布版本和 `main` 分支。早期私有部署版本应先升级，再确认问题是否仍然存在。

## 私下报告漏洞

请使用 GitHub 仓库的 **Security → Report a vulnerability** 私密报告入口。不要在公开 Issue、Discussion、日志或截图中提交 App Secret、配对码、访问令牌、聊天内容、用户标识符或主机路径。

报告请包含：受影响版本、可复现条件、预期影响和最小化复现步骤。请使用虚构凭据与测试数据。维护者会尽快确认报告、评估影响，并在修复可用后协调披露。

## 重要边界

- 飞行桥允许远程触发本机 AI Agent，因此运行主机必须被视为高价值执行环境。
- 默认权限为 `on-request + workspace-write`；远程 Full Access 默认关闭。
- 只有在隔离主机上充分理解风险后，才应在本机配置中启用 `allow_remote_full_access`。
- 配对码等同于短期所有者登记凭据，只能通过可信本地渠道传递。
- 飞书附件、聊天文本和 Codex 生成文件都应视为不可信输入。
- 项目无法保证第三方平台、Codex CLI、依赖包或用户编写的 Agent 指令不存在漏洞。

更完整的威胁模型见 [docs/SECURITY_MODEL.md](docs/SECURITY_MODEL.md)。
