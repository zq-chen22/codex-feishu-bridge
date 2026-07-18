#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd -- "${SCRIPT_DIR}/.." && pwd)"
CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/codex-feishu-bridge"
USER_UNIT_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user"
STATE_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/codex-feishu-bridge"
VENV="${PROJECT_ROOT}/.venv"

if ! command -v python3 >/dev/null 2>&1; then
  echo "错误：未找到 python3。" >&2
  exit 1
fi

if ! python3 -c 'import sys; raise SystemExit(sys.version_info < (3, 12))'; then
  echo "错误：需要 Python 3.12 或更高版本。" >&2
  exit 1
fi

python3 -m venv "${VENV}"
"${VENV}/bin/python" -m pip install --upgrade pip
"${VENV}/bin/python" -m pip install --require-hashes -r "${PROJECT_ROOT}/requirements.lock"
"${VENV}/bin/python" -m pip install --no-deps --upgrade "${PROJECT_ROOT}"

install -d -m 0700 "${CONFIG_DIR}" "${STATE_DIR}" "${USER_UNIT_DIR}"

if [[ ! -e "${CONFIG_DIR}/config.toml" ]]; then
  install -m 0600 "${PROJECT_ROOT}/config.example.toml" "${CONFIG_DIR}/config.toml"
  echo "已创建 ${CONFIG_DIR}/config.toml"
else
  echo "保留已有 ${CONFIG_DIR}/config.toml"
fi

if [[ ! -e "${CONFIG_DIR}/secrets.env" ]]; then
  install -m 0600 /dev/null "${CONFIG_DIR}/secrets.env"
  {
    echo 'FEISHU_CONVERSATION_APP_SECRET='
  } >>"${CONFIG_DIR}/secrets.env"
  echo "已创建仅限当前用户读取的 ${CONFIG_DIR}/secrets.env"
else
  chmod 0600 "${CONFIG_DIR}/secrets.env"
  echo "保留已有 ${CONFIG_DIR}/secrets.env"
fi

# systemd 模板中的项目路径可能不是固定的 ~/Projects/...。
escaped_root=${PROJECT_ROOT//&/\\&}
sed "s&@PROJECT_ROOT@&${escaped_root}&g" \
  "${PROJECT_ROOT}/systemd/codex-feishu-bridge.service" \
  >"${USER_UNIT_DIR}/codex-feishu-bridge.service"
chmod 0600 "${USER_UNIT_DIR}/codex-feishu-bridge.service"
sed "s&@PROJECT_ROOT@&${escaped_root}&g" \
  "${PROJECT_ROOT}/systemd/codex-feishu-daily-stats.service" \
  >"${USER_UNIT_DIR}/codex-feishu-daily-stats.service"
install -m 0600 \
  "${PROJECT_ROOT}/systemd/codex-feishu-daily-stats.timer" \
  "${USER_UNIT_DIR}/codex-feishu-daily-stats.timer"
chmod 0600 "${USER_UNIT_DIR}/codex-feishu-daily-stats.service"
systemctl --user daemon-reload

echo
echo "安装完成，但尚未启动服务。"
echo "1. 在你自己的飞书租户中创建并发布 Codex 飞书自建应用。"
echo "2. 编辑 ${CONFIG_DIR}/config.toml，填写 Codex App ID。"
echo "3. 只在本机编辑 ${CONFIG_DIR}/secrets.env，填写 Codex App Secret；不要发到聊天。"
echo "4. 运行 ${VENV}/bin/codex-feishu-bridge --config ${CONFIG_DIR}/config.toml doctor"
echo "5. 配对并 bootstrap 后执行：systemctl --user enable --now codex-feishu-bridge.service"
echo "6. 配置 daily_stats 后执行：systemctl --user enable --now codex-feishu-daily-stats.timer"
