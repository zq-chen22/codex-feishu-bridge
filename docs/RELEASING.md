# 发布流程

1. 在 `AI_README.md` 顶部记录版本、迁移和验证结果。
2. 确认 `pyproject.toml`、包内版本及 Git 标签一致。
3. 运行：

```bash
python -m pytest -q
ruff check .
bandit -q -r src
pip-audit -r requirements.lock
python -m build
twine check dist/*
```

4. 重新解析依赖时更新带哈希的 `requirements.lock`，并检查 wheel/sdist 只包含预期代码、配置模板、README、LICENSE 和 NOTICE。
5. 在干净克隆中安装 wheel，验证 `init`、`doctor --help`、安装和卸载脚本。
6. 对当前树和完整待发布历史运行密钥扫描；发现任何真实凭据时先轮换，再清理历史。
7. 创建签名或受保护的版本标签，并由 GitHub Actions 生成构件、SHA-256 清单和 SBOM。
8. Release 说明只陈述已验证事实，不作绝对安全、稳定性或官方背书承诺。

发布失败时不要复用已经上传但内容不同的版本号。
