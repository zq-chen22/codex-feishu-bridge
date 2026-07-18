from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from codex_feishu_bridge.config import BridgeConfig, FeishuAppConfig, FeishuConfig
from codex_feishu_bridge.feishu import (
    FeishuGateway,
    _normalize_card_action,
    _normalize_history_message,
    _normalize_message,
    conversation_binding_marker,
    conversation_group_description,
    conversation_group_name,
    deterministic_uuid,
    progress_card,
)


def namespace(**values):
    return SimpleNamespace(**values)


def ws_message_event(
    *,
    app_id: str = "cli_conversation",
    message_id: str = "om-1",
    message_type: str = "text",
    content: dict | None = None,
    mentions: list | None = None,
):
    return namespace(
        header=namespace(
            app_id=app_id,
            tenant_key="tenant-test",
            event_id="evt-message",
            create_time="1700000000000",
        ),
        event=namespace(
            message=namespace(
                message_id=message_id,
                chat_id="oc_thread",
                chat_type="group",
                message_type=message_type,
                content=json.dumps(content or {}, ensure_ascii=False),
                mentions=mentions or [],
                create_time="1700000000123",
            ),
            sender=namespace(
                sender_id=namespace(
                    open_id="ou_app_scoped",
                    user_id="u_user",
                    union_id="on_union",
                ),
                tenant_key="tenant-test",
                sender_type="user",
            ),
        ),
    )


def test_conversation_group_name_is_stable_sanitized_and_bounded() -> None:
    suffix = "-测试主机"
    name = conversation_group_name("  很长的对话名\n" + "甲" * 100, suffix)

    assert name.endswith(suffix)
    assert "\n" not in name
    assert len(name) == 60
    assert conversation_group_name(" \t\n ", suffix) == f"Codex 对话{suffix}"


def test_conversation_description_hides_local_path_and_raw_thread_id() -> None:
    description = conversation_group_description(
        "thread-private-id", "/private/workspace/name", 1_700_000_000
    )

    assert "/private/workspace/name" not in description
    assert "thread-private-id" not in description
    assert conversation_binding_marker("thread-private-id") in description
    assert "本地工作区：已隐藏" in description


def test_message_uuid_is_deterministic_but_namespaced() -> None:
    first = deterministic_uuid("message", "om-1")
    assert first == deterministic_uuid("message", "om-1")
    assert first != deterministic_uuid("chat", "om-1")
    assert first.startswith("cfb-")


def test_gateway_ignores_legacy_admin_app_configuration(tmp_path) -> None:
    config = BridgeConfig(
        config_path=tmp_path / "config.toml",
        feishu=FeishuConfig(
            admin=FeishuAppConfig("cli_legacy_admin", "FEISHU_ADMIN_APP_SECRET"),
            conversation=FeishuAppConfig("cli_codex", "FEISHU_CONVERSATION_APP_SECRET"),
        ),
    )
    gateway = FeishuGateway.__new__(FeishuGateway)
    gateway.config = config

    assert gateway._apps() == {"conversation": config.feishu.conversation}


def test_ws_text_event_normalization_removes_bot_mention_and_preserves_identity() -> None:
    data = ws_message_event(
        content={"text": "@_user_1  请继续执行  "},
        mentions=[namespace(key="@_user_1")],
    )

    message = _normalize_message(data, "conversation", "cli_conversation")

    assert message.message_id == "om-1"
    assert message.chat_id == "oc_thread"
    assert message.app_role == "conversation"
    assert message.text == "请继续执行"
    assert message.sender_open_id == "ou_app_scoped"
    assert message.sender_user_id == "u_user"
    assert message.sender_union_id == "on_union"
    assert message.tenant_key == "tenant-test"
    assert message.sender_type == "user"
    assert message.create_time_ms == 1_700_000_000_123
    assert message.attachments == []


@pytest.mark.parametrize(
    ("message_type", "content", "expected_kind", "expected_name"),
    [
        ("image", {"image_key": "img-1"}, "image", "image"),
        (
            "file",
            {"file_key": "file-1", "file_name": "结果.txt"},
            "file",
            "结果.txt",
        ),
        (
            "media",
            {"file_key": "video-1", "file_name": "演示.mp4", "image_key": "cover-1"},
            "media",
            "演示.mp4",
        ),
    ],
)
def test_ws_attachment_event_normalization(
    message_type: str,
    content: dict,
    expected_kind: str,
    expected_name: str,
) -> None:
    message = _normalize_message(
        ws_message_event(message_type=message_type, content=content),
        "conversation",
        "cli_conversation",
    )

    assert message.text == ""
    assert len(message.attachments) == 1
    attachment = message.attachments[0]
    assert attachment.kind == expected_kind
    assert attachment.name == expected_name
    assert attachment.message_id == "om-1"
    assert attachment.image_key == content.get("image_key")
    assert attachment.file_key == content.get("file_key")


def test_ws_event_rejects_another_feishu_application() -> None:
    with pytest.raises(ValueError, match="app_id mismatch"):
        _normalize_message(
            ws_message_event(app_id="cli_wrong"),
            "conversation",
            "cli_conversation",
        )


def test_card_action_normalizes_to_scoped_approval_command() -> None:
    data = namespace(
        header=namespace(
            app_id="cli_conversation",
            event_id="evt-card-1",
            create_time="1700000000999",
            tenant_key="tenant-test",
        ),
        event=namespace(
            action=namespace(
                value={
                    "kind": "codex_approval",
                    "short_id": "approve-42",
                    "decision": "allow_once",
                }
            ),
            context=namespace(open_chat_id="oc_thread"),
            operator=namespace(
                open_id="ou_app_scoped",
                user_id="u_user",
                union_id="on_union",
                tenant_key="tenant-test",
            ),
        ),
    )

    message = _normalize_card_action(data, "conversation", "cli_conversation")

    assert message is not None
    assert message.message_id == "card:evt-card-1"
    assert message.chat_id == "oc_thread"
    assert message.sender_open_id == "ou_app_scoped"
    assert message.text == "!approval approve-42 allow_once"
    assert message.message_type == "card_action"
    assert message.sender_type == "user"


def test_card_action_ignores_non_codex_or_invalid_decisions() -> None:
    base = namespace(
        header=namespace(
            app_id="cli_conversation",
            event_id="evt-card-invalid",
            create_time="1",
            tenant_key="tenant-test",
        ),
        event=namespace(
            action=namespace(value={"kind": "other", "decision": "allow_once"}),
            context=namespace(open_chat_id="oc_thread"),
            operator=namespace(
                open_id="ou_owner",
                user_id=None,
                union_id=None,
                tenant_key="tenant-test",
            ),
        ),
    )
    assert _normalize_card_action(base, "conversation", "cli_conversation") is None

    base.event.action.value = {
        "kind": "codex_approval",
        "short_id": "approval-1",
        "decision": "allow_forever",
    }
    assert _normalize_card_action(base, "conversation", "cli_conversation") is None


def test_card_action_normalizes_cli_setting_picker_choices() -> None:
    data = namespace(
        header=namespace(
            app_id="cli_conversation",
            event_id="evt-card-setting",
            create_time="1700000000999",
            tenant_key="tenant-test",
        ),
        event=namespace(
            action=namespace(
                value={
                    "kind": "codex_setting",
                    "setting": "model",
                    "model": "gpt-5.6-sol",
                    "effort": "xhigh",
                }
            ),
            context=namespace(open_chat_id="oc_thread"),
            operator=namespace(
                open_id="ou_owner",
                user_id="u_user",
                union_id="on_union",
                tenant_key="tenant-test",
            ),
        ),
    )

    message = _normalize_card_action(data, "conversation", "cli_conversation")
    assert message is not None
    assert message.text == "/model gpt-5.6-sol xhigh"

    data.header.event_id = "evt-card-permissions"
    data.event.action.value = {
        "kind": "codex_setting",
        "setting": "permissions",
        "profile": "full-access",
    }
    message = _normalize_card_action(data, "conversation", "cli_conversation")
    assert message is not None
    assert message.text == "/permissions full-access"


def test_card_action_normalizes_cli_compatibility_repair() -> None:
    data = namespace(
        header=namespace(
            app_id="cli_conversation",
            event_id="evt-card-compat",
            create_time="1700000000999",
            tenant_key="tenant-test",
        ),
        event=namespace(
            action=namespace(
                value={
                    "kind": "codex_compatibility",
                    "action": "repair",
                    "version": "0.144.3",
                }
            ),
            context=namespace(open_chat_id="oc_private"),
            operator=namespace(
                open_id="ou_owner",
                user_id="u_user",
                union_id="on_union",
                tenant_key="tenant-test",
            ),
        ),
    )

    message = _normalize_card_action(data, "conversation", "cli_conversation")

    assert message is not None
    assert message.text == "/bridge-settings-compat repair 0.144.3"
    assert message.message_type == "card_action"


def test_history_message_normalization_has_same_message_id_dedupe_key() -> None:
    item = namespace(
        message_id="om-history-stable",
        chat_id="oc_thread",
        msg_type="text",
        body=namespace(content=json.dumps({"text": "恢复消息"}, ensure_ascii=False)),
        mentions=[],
        create_time="1700000000123",
        sender=namespace(
            id="ou_owner",
            id_type="open_id",
            tenant_key="tenant-test",
            sender_type="user",
        ),
    )

    first = _normalize_history_message(item, "conversation", "cli_conversation")
    second = _normalize_history_message(item, "conversation", "cli_conversation")

    assert first == second
    assert first.message_id == "om-history-stable"
    assert first.sender_open_id == "ou_owner"
    assert first.text == "恢复消息"


def test_history_system_message_without_sender_is_safe_to_ignore() -> None:
    item = namespace(
        message_id="om-system",
        chat_id="oc_thread",
        msg_type="system",
        body=None,
        mentions=None,
        create_time="1700000000123",
        sender=None,
    )
    message = _normalize_history_message(item, "conversation", "cli_conversation")
    assert message.sender_type == "system"
    assert message.sender_open_id is None


def test_progress_card_bounds_mobile_content_and_title() -> None:
    card = progress_card("标题" * 100, "前缀" + "进展" * 20_000, color="green")

    assert card["header"]["template"] == "green"
    assert len(card["header"]["title"]["content"]) == 80
    assert len(card["elements"][0]["content"].encode("utf-8")) <= 24_100
    assert card["elements"][0]["content"].startswith("[较早进展已折叠]")
    assert card["config"]["update_multi"] is True
