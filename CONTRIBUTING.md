# 贡献指南

感谢你帮助改进飞行桥。

## 提交前

1. 先搜索现有 Issue，较大的行为变化先提出设计讨论。
2. 从最新 `main` 创建小而清晰的分支。
3. 不得提交真实凭据、聊天内容、个人信息、组织名称、主机路径或来源不明的素材。
4. 若使用 AI Agent 辅助，请在 PR 中说明使用范围，并由提交者亲自审查、测试和承担来源责任。
5. 运行 `python -m pytest -q`、`ruff check .` 和构建检查。

## 贡献授权

项目采用 [Developer Certificate of Origin 1.1](https://developercertificate.org/)；每个提交都必须包含 Signed-off-by：

```bash
git commit -s -m "fix: describe the change"
```

签署表示你有权依据仓库的 Apache License 2.0 提交该贡献，且理解提交及 sign-off 会永久公开。项目不要求转让版权；贡献者保留自己贡献的版权。

## PR 要求

- 说明用户问题、行为变化、安全与隐私影响。
- 新行为必须有测试；涉及兼容性的变化应提供迁移说明。
- 不修改主展示 README，除非维护者明确批准法律或产品定位层面的变更。
- 所有代码和发布变化都要按时间追加到 `AI_README.md`。

安全漏洞请遵循 [SECURITY.md](SECURITY.md)，不要提交公开 PR 复现零日问题。
