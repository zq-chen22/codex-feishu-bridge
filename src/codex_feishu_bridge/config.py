from __future__ import annotations

import os
import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

DEFAULT_CONFIG_DIR = Path.home() / ".config" / "codex-feishu-bridge"
DEFAULT_STATE_DIR = Path.home() / ".local" / "share" / "codex-feishu-bridge"
ADMIN_ENV_VAR = "FEISHU_ADMIN_APP_SECRET"
CONVERSATION_ENV_VAR = "FEISHU_CONVERSATION_APP_SECRET"


@dataclass(slots=True)
class FeishuAppConfig:
    app_id: str = ""
    app_secret_env: str = ""

    def secret(self) -> str:
        return os.environ.get(self.app_secret_env, "") if self.app_secret_env else ""

    @property
    def configured(self) -> bool:
        return bool(self.app_id and self.secret())


@dataclass(slots=True)
class FeishuConfig:
    admin: FeishuAppConfig = field(
        default_factory=lambda: FeishuAppConfig(app_secret_env=ADMIN_ENV_VAR)
    )
    conversation: FeishuAppConfig = field(
        default_factory=lambda: FeishuAppConfig(app_secret_env=CONVERSATION_ENV_VAR)
    )
    owner_open_id: str = ""
    owner_admin_open_id: str = ""
    owner_conversation_open_id: str = ""
    owner_user_id: str = ""
    owner_union_id: str = ""
    pairing_code_ttl_seconds: int = 900


@dataclass(slots=True)
class DailyStatsConfig:
    enabled: bool = False
    spreadsheet_token: str = ""
    sheet_id: str = ""
    timezone: str = "Asia/Shanghai"


@dataclass(slots=True)
class BridgeConfig:
    config_path: Path
    state_dir: Path = DEFAULT_STATE_DIR
    database_path: Path = DEFAULT_STATE_DIR / "bridge.sqlite"
    inbox_dir: Path = DEFAULT_STATE_DIR / "inbox"
    outbox_dir: Path = DEFAULT_STATE_DIR / "outbox"
    admin_scratch_dir: Path = DEFAULT_STATE_DIR / "admin-scratch"
    managed_workspaces_dir: Path = DEFAULT_STATE_DIR / "workspaces"
    codex_bin: str = "codex"
    initial_thread_count: int = 3
    sync_interval_seconds: int = 15
    history_poll_seconds: float = 600.0
    history_poll_warm_seconds: float = 1800.0
    history_poll_idle_seconds: float = 3600.0
    history_poll_cold_seconds: float = 7200.0
    history_warm_after_seconds: float = 6 * 3600.0
    history_idle_after_seconds: float = 24 * 3600.0
    history_cold_after_seconds: float = 7 * 24 * 3600.0
    progress_update_seconds: float = 5.0
    progress_initial_window_seconds: float = 120.0
    progress_steady_update_seconds: float = 30.0
    progress_heartbeat_seconds: float = 30.0
    progress_stale_seconds: float = 120.0
    shutdown_drain_timeout_seconds: float = 6 * 3600.0
    image_proxy_max_edge: int = 1024
    image_proxy_jpeg_quality: int = 75
    group_suffix: str = "-Codex"
    auto_discover_new_threads: bool = True
    source_kinds: list[str] = field(
        default_factory=lambda: ["cli", "vscode", "appServer", "unknown"]
    )
    model: str | None = None
    model_reasoning_effort: str | None = None
    service_tier: str | None = None
    approval_policy: str = "on-request"
    sandbox: str = "workspace-write"
    allowed_workspace_roots: list[Path] = field(default_factory=list)
    allow_remote_full_access: bool = False
    data_retention_days: int = 30
    max_download_bytes: int = 50 * 1024 * 1024
    max_upload_bytes: int = 30 * 1024 * 1024
    feishu: FeishuConfig = field(default_factory=FeishuConfig)
    daily_stats: DailyStatsConfig = field(default_factory=DailyStatsConfig)

    @property
    def visual_proxy_dir(self) -> Path:
        return self.state_dir / "visual-proxies"

    def prepare_dirs(self) -> None:
        for path in (
            self.state_dir,
            self.inbox_dir,
            self.outbox_dir,
            self.admin_scratch_dir,
            self.managed_workspaces_dir,
            self.visual_proxy_dir,
        ):
            if path.is_symlink():
                raise ValueError(f"安全目录不能是符号链接：{path}")
            path.mkdir(parents=True, exist_ok=True, mode=0o700)
            path.chmod(0o700)


def _path(value: str | Path | None, default: Path) -> Path:
    if not value:
        return default
    candidate = Path(value).expanduser()
    if candidate.is_symlink():
        raise ValueError(f"安全路径不能是符号链接：{candidate}")
    return candidate.resolve()


_ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def _validate_config(config: BridgeConfig) -> None:
    if config.approval_policy not in {"on-request", "never"}:
        raise ValueError("approval_policy 只能是 on-request 或 never")
    if config.sandbox not in {"read-only", "workspace-write", "danger-full-access"}:
        raise ValueError("sandbox 只能是 read-only、workspace-write 或 danger-full-access")
    if not config.allowed_workspace_roots:
        raise ValueError("allowed_workspace_roots 至少需要一个明确目录")
    if config.data_retention_days < 0 or config.data_retention_days > 3650:
        raise ValueError("data_retention_days 必须在 0 到 3650 之间")
    if not 1 <= config.initial_thread_count <= 100:
        raise ValueError("initial_thread_count 必须在 1 到 100 之间")
    if config.max_download_bytes <= 0 or config.max_upload_bytes <= 0:
        raise ValueError("附件大小上限必须为正整数")
    if config.feishu.pairing_code_ttl_seconds < 60:
        raise ValueError("pairing_code_ttl_seconds 不能短于 60 秒")
    for app in (config.feishu.admin, config.feishu.conversation):
        if app.app_secret_env and not _ENV_NAME.fullmatch(app.app_secret_env):
            raise ValueError("app_secret_env 必须是合法的环境变量名")


def load_config(path: str | Path | None = None) -> BridgeConfig:
    config_path = Path(path).expanduser() if path else DEFAULT_CONFIG_DIR / "config.toml"
    raw: dict = {}
    if config_path.exists():
        with config_path.open("rb") as handle:
            raw = tomllib.load(handle)

    bridge = raw.get("bridge", {})
    feishu = raw.get("feishu", {})
    daily_stats = raw.get("daily_stats", {})
    admin = feishu.get("admin", {})
    conversation = feishu.get("conversation", {})
    state_dir = _path(bridge.get("state_dir"), DEFAULT_STATE_DIR)
    managed_workspaces_dir = _path(bridge.get("managed_workspaces_dir"), state_dir / "workspaces")

    cfg = BridgeConfig(
        config_path=config_path,
        state_dir=state_dir,
        database_path=_path(bridge.get("database_path"), state_dir / "bridge.sqlite"),
        inbox_dir=_path(bridge.get("inbox_dir"), state_dir / "inbox"),
        outbox_dir=_path(bridge.get("outbox_dir"), state_dir / "outbox"),
        admin_scratch_dir=_path(bridge.get("admin_scratch_dir"), state_dir / "admin-scratch"),
        managed_workspaces_dir=managed_workspaces_dir,
        codex_bin=str(bridge.get("codex_bin", "codex")),
        initial_thread_count=int(bridge.get("initial_thread_count", 3)),
        sync_interval_seconds=int(bridge.get("sync_interval_seconds", 15)),
        history_poll_seconds=float(bridge.get("history_poll_seconds", 600.0)),
        history_poll_warm_seconds=float(bridge.get("history_poll_warm_seconds", 1800.0)),
        history_poll_idle_seconds=float(bridge.get("history_poll_idle_seconds", 3600.0)),
        history_poll_cold_seconds=float(bridge.get("history_poll_cold_seconds", 7200.0)),
        history_warm_after_seconds=float(bridge.get("history_warm_after_seconds", 6 * 3600.0)),
        history_idle_after_seconds=float(bridge.get("history_idle_after_seconds", 24 * 3600.0)),
        history_cold_after_seconds=float(bridge.get("history_cold_after_seconds", 7 * 24 * 3600.0)),
        progress_update_seconds=float(bridge.get("progress_update_seconds", 5.0)),
        progress_initial_window_seconds=float(bridge.get("progress_initial_window_seconds", 120.0)),
        progress_steady_update_seconds=float(bridge.get("progress_steady_update_seconds", 30.0)),
        progress_heartbeat_seconds=float(bridge.get("progress_heartbeat_seconds", 30.0)),
        progress_stale_seconds=float(bridge.get("progress_stale_seconds", 120.0)),
        shutdown_drain_timeout_seconds=float(
            bridge.get("shutdown_drain_timeout_seconds", 6 * 3600.0)
        ),
        image_proxy_max_edge=int(bridge.get("image_proxy_max_edge", 1024)),
        image_proxy_jpeg_quality=int(bridge.get("image_proxy_jpeg_quality", 75)),
        group_suffix=str(bridge.get("group_suffix", "-Codex")),
        auto_discover_new_threads=bool(bridge.get("auto_discover_new_threads", True)),
        source_kinds=list(bridge.get("source_kinds", ["cli", "vscode", "appServer", "unknown"])),
        model=str(bridge["model"]) if bridge.get("model") else None,
        model_reasoning_effort=(
            str(bridge["model_reasoning_effort"]) if bridge.get("model_reasoning_effort") else None
        ),
        service_tier=str(bridge["service_tier"]) if bridge.get("service_tier") else None,
        approval_policy=str(bridge.get("approval_policy", "on-request")),
        sandbox=str(bridge.get("sandbox", "workspace-write")),
        allowed_workspace_roots=[
            _path(item, managed_workspaces_dir)
            for item in bridge.get("allowed_workspace_roots", [str(managed_workspaces_dir)])
        ],
        allow_remote_full_access=bool(bridge.get("allow_remote_full_access", False)),
        data_retention_days=int(bridge.get("data_retention_days", 30)),
        max_download_bytes=int(bridge.get("max_download_bytes", 50 * 1024 * 1024)),
        max_upload_bytes=int(bridge.get("max_upload_bytes", 30 * 1024 * 1024)),
        feishu=FeishuConfig(
            admin=FeishuAppConfig(
                app_id=str(admin.get("app_id", "")),
                app_secret_env=str(admin.get("app_secret_env", "FEISHU_ADMIN_APP_SECRET")),
            ),
            conversation=FeishuAppConfig(
                app_id=str(conversation.get("app_id", "")),
                app_secret_env=str(
                    conversation.get("app_secret_env", "FEISHU_CONVERSATION_APP_SECRET")
                ),
            ),
            owner_open_id=str(feishu.get("owner_open_id", "")),
            owner_admin_open_id=str(
                feishu.get("owner_admin_open_id", feishu.get("owner_open_id", ""))
            ),
            owner_conversation_open_id=str(
                feishu.get("owner_conversation_open_id", feishu.get("owner_open_id", ""))
            ),
            owner_user_id=str(feishu.get("owner_user_id", "")),
            owner_union_id=str(feishu.get("owner_union_id", "")),
            pairing_code_ttl_seconds=int(feishu.get("pairing_code_ttl_seconds", 900)),
        ),
        daily_stats=DailyStatsConfig(
            enabled=bool(daily_stats.get("enabled", False)),
            spreadsheet_token=str(daily_stats.get("spreadsheet_token", "")).strip(),
            sheet_id=str(daily_stats.get("sheet_id", "")).strip(),
            timezone=str(daily_stats.get("timezone", "Asia/Shanghai")).strip() or "Asia/Shanghai",
        ),
    )
    _validate_config(cfg)
    cfg.prepare_dirs()
    return cfg
