from __future__ import annotations

from importlib import resources
from pathlib import Path

import pytest

from codex_feishu_bridge.config import load_config


def test_missing_config_uses_fail_closed_defaults(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("codex_feishu_bridge.config.DEFAULT_STATE_DIR", tmp_path / "state")

    config = load_config(tmp_path / "missing.toml")

    assert config.approval_policy == "on-request"
    assert config.sandbox == "workspace-write"
    assert config.allow_remote_full_access is False
    assert config.data_retention_days == 30
    assert config.allowed_workspace_roots == [config.managed_workspaces_dir]
    assert config.group_suffix == "-Codex"


@pytest.mark.parametrize(
    ("body", "message"),
    [
        ('approval_policy = "always"', "approval_policy"),
        ('sandbox = "host"', "sandbox"),
        ("allowed_workspace_roots = []", "allowed_workspace_roots"),
        ("data_retention_days = -1", "data_retention_days"),
    ],
)
def test_invalid_security_configuration_is_rejected(
    tmp_path: Path, body: str, message: str
) -> None:
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        f'[bridge]\nstate_dir = "{tmp_path / "state"}"\n{body}\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match=message):
        load_config(config_path)


def test_state_directory_cannot_be_a_symlink(tmp_path: Path) -> None:
    target = tmp_path / "real-state"
    target.mkdir()
    link = tmp_path / "state-link"
    link.symlink_to(target, target_is_directory=True)
    config_path = tmp_path / "config.toml"
    config_path.write_text(
        f'[bridge]\nstate_dir = "{link}"\nallowed_workspace_roots = ["{tmp_path}"]\n',
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="符号链接"):
        load_config(config_path)


def test_packaged_configuration_template_matches_repository_copy() -> None:
    repository = Path(__file__).parents[1] / "config.example.toml"
    packaged = resources.files("codex_feishu_bridge").joinpath("config.example.toml")

    assert packaged.read_text(encoding="utf-8") == repository.read_text(encoding="utf-8")
