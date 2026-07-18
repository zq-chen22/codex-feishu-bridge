# 安装与升级

飞行桥面向 Linux 用户级 systemd 环境，要求 Python 3.12+、可运行的 Codex CLI、飞书企业自建应用和当前用户可用的 `systemctl --user`。

## 安装

```bash
git clone https://github.com/zq-chen22/codex-feishu-bridge.git
cd codex-feishu-bridge
./scripts/install-user-service.sh
```

安装器会创建项目专用虚拟环境，按带哈希的 `requirements.lock` 安装依赖，将配置复制到 `~/.config/codex-feishu-bridge/`，并安装尚未启动的用户服务。

随后：

1. 在自己的飞书租户创建企业自建应用，并按飞书开放平台要求开通消息、群组、文件和长连接所需权限。
2. 在 `config.toml` 填写 App ID；只在权限为 `0600` 的 `secrets.env` 中填写 App Secret。
3. 执行 `.venv/bin/codex-feishu-bridge doctor`，修复全部失败项。
4. 执行 `.venv/bin/codex-feishu-bridge pair-code`，在飞书私聊机器人完成所有者配对。
5. 执行 `.venv/bin/codex-feishu-bridge bootstrap`。
6. 启动服务：

```bash
systemctl --user enable --now codex-feishu-bridge.service
```

不要把 App Secret 或配对码交给 AI Agent、粘贴到聊天、Issue 或日志中。

## 安全默认值

- `approval_policy = "on-request"`
- `sandbox = "workspace-write"`
- `allowed_workspace_roots` 默认只有飞行桥管理的 workspace
- `allow_remote_full_access = false`

若需要操作既有项目，应把每个明确的项目根目录加入 `allowed_workspace_roots`，不要用整个用户主目录代替。远程 Full Access 只适合可随时重建、没有其他秘密的隔离主机。

## 升级

```bash
git pull --ff-only
./scripts/install-user-service.sh
systemctl --user restart codex-feishu-bridge.service
```

安装器会保留已有 `config.toml` 和 `secrets.env`。升级后先运行 `doctor` 并查看 `AI_README.md` 的最新迁移记录。

## 卸载

保留配置和数据：

```bash
./scripts/uninstall-user-service.sh
```

删除服务、配置、凭据和本地数据：

```bash
./scripts/uninstall-user-service.sh --purge
```

卸载不会自动删除飞书云端消息或 Codex 自身历史。
