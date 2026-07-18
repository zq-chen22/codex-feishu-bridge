from __future__ import annotations

import logging

import pytest

from codex_feishu_bridge.cli import _configure_logging, _suggest_title, parser
from codex_feishu_bridge.models import ThreadSummary


def test_suggest_title_uses_thread_name() -> None:
    thread = ThreadSummary(
        thread_id="thread-1",
        name="  Deployment thread  ",
        preview="ignored preview",
        cwd="/tmp",
        created_at=1,
        updated_at=2,
    )

    assert _suggest_title(thread) == "Deployment thread"


def test_cli_reports_package_version(capsys: pytest.CaptureFixture[str]) -> None:
    with pytest.raises(SystemExit) as exit_info:
        parser().parse_args(["--version"])

    assert exit_info.value.code == 0
    assert "0.3.0" in capsys.readouterr().out


def test_verbose_logging_does_not_enable_third_party_debug() -> None:
    _configure_logging(verbose=True)

    assert logging.getLogger("codex_feishu_bridge").level == logging.DEBUG
    for logger_name in ("httpx", "httpcore", "lark_oapi", "websockets"):
        assert logging.getLogger(logger_name).level == logging.WARNING
