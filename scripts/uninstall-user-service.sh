#!/usr/bin/env bash
set -euo pipefail

CONFIG_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/codex-feishu-bridge"
USER_UNIT_DIR="${XDG_CONFIG_HOME:-${HOME}/.config}/systemd/user"
STATE_DIR="${XDG_DATA_HOME:-${HOME}/.local/share}/codex-feishu-bridge"
PURGE=false

if [[ "${1:-}" == "--purge" ]]; then
  PURGE=true
elif [[ $# -gt 0 ]]; then
  echo "用法：$0 [--purge]" >&2
  exit 2
fi

systemctl --user disable --now codex-feishu-bridge.service 2>/dev/null || true
systemctl --user disable --now codex-feishu-daily-stats.timer 2>/dev/null || true
rm -f \
  "${USER_UNIT_DIR}/codex-feishu-bridge.service" \
  "${USER_UNIT_DIR}/codex-feishu-daily-stats.service" \
  "${USER_UNIT_DIR}/codex-feishu-daily-stats.timer"
systemctl --user daemon-reload

if [[ "${PURGE}" == true ]]; then
  rm -rf -- "${CONFIG_DIR}" "${STATE_DIR}"
  echo "服务、配置和本地状态已删除。"
else
  echo "服务已删除；配置和本地状态已保留。"
  echo "如需一并删除，请再次执行：$0 --purge"
fi
