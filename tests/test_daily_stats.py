from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from codex_feishu_bridge.daily_stats import (
    DailyStatsError,
    FeishuSheetsClient,
    choose_group_column,
    column_name,
    is_long_task,
    strip_bridge_constraint,
)


def test_strip_bridge_constraint_before_counting() -> None:
    text = "实际问题\n飞书桥交付约束：若本任务需要把新生成的文件回传给用户，请保存到专用目录。"
    assert strip_bridge_constraint(text) == "实际问题"


@pytest.mark.parametrize(
    ("text", "duration_ms", "expected"),
    [
        ("甲" * 101, 180_001, True),
        ("甲" * 100, 180_001, False),
        ("甲" * 101, 180_000, False),
        ("短任务", 600_001, True),
        ("短任务", 600_000, False),
        ("甲" * 101 + "飞书桥交付约束：" + "乙" * 300, 180_001, True),
        ("甲" * 100 + "飞书桥交付约束：" + "乙" * 300, 180_001, False),
    ],
)
def test_long_task_boundaries(text: str, duration_ms: int, expected: bool) -> None:
    assert is_long_task(text, duration_ms) is expected


def test_choose_group_preserves_existing_and_appends() -> None:
    rows = [
        ["日期", "Codex-其他主机", "", "", ""],
        ["", "总任务", "长任务", "", ""],
    ]
    assert choose_group_column(rows, "Codex-本机", column_count=5) == 3
    assert choose_group_column(rows, "Codex-其他主机", column_count=5) == 1


def test_duplicate_bot_group_is_rejected() -> None:
    rows = [
        ["日期", "Codex-本机", "", "Codex-本机", ""],
        ["", "总任务", "长任务", "总任务", "长任务"],
    ]
    with pytest.raises(DailyStatsError, match="duplicate"):
        choose_group_column(rows, "Codex-本机", column_count=5)


def test_column_names() -> None:
    assert [column_name(index) for index in (0, 25, 26, 99)] == ["A", "Z", "AA", "CV"]


@pytest.mark.asyncio
async def test_insert_row_inherits_body_style_from_following_row() -> None:
    client = object.__new__(FeishuSheetsClient)
    client.request = AsyncMock(return_value={})

    await client.insert_row("spreadsheet", "sheet", 3)

    client.request.assert_awaited_once_with(
        "POST",
        "/sheets/v2/spreadsheets/spreadsheet/insert_dimension_range",
        operation="sheet.rows.insert",
        json_body={
            "dimension": {
                "sheetId": "sheet",
                "majorDimension": "ROWS",
                "startIndex": 2,
                "endIndex": 3,
            },
            "inheritStyle": "AFTER",
        },
    )
