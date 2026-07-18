from __future__ import annotations

import asyncio
import contextlib
import time
from pathlib import Path
from typing import Any

import pytest

from codex_feishu_bridge.config import BridgeConfig, FeishuConfig
from codex_feishu_bridge.db import BridgeDB
from codex_feishu_bridge.models import (
    ActiveTurn,
    AppRole,
    Attachment,
    InboxItem,
    IncomingMessage,
    PendingApproval,
    ThreadSummary,
    TurnJob,
)
from codex_feishu_bridge.service import (
    BridgeService,
    RuntimeSettings,
    ScheduledMessage,
    _redact,
    generate_pairing_code,
)


def test_redact_covers_environment_and_provider_secret_shapes() -> None:
    text = (
        "FEISHU_CONVERSATION_APP_SECRET=example-secret-value "
        "Authorization: Bearer example-bearer-value "
        "sk-exampleprovidersecret123456"
    )

    redacted = _redact(text)

    assert "example-secret-value" not in redacted
    assert "example-bearer-value" not in redacted
    assert "exampleprovidersecret" not in redacted
    assert redacted.count("[已隐藏]") >= 3


class FakeCodex:
    def __init__(self) -> None:
        self.notification_handlers: list[Any] = []
        self.server_request_handler: Any = None
        self.steered: list[dict[str, Any]] = []
        self.responses: list[tuple[str, dict[str, Any]]] = []
        self.pending_requests: dict[str, dict[str, Any]] = {}
        self.errors: list[tuple[str, int, str]] = []
        self.setting_updates: list[dict[str, Any]] = []
        self.started_threads: list[dict[str, Any]] = []
        self.archived_threads: list[str] = []
        self.unsubscribed_threads: list[str] = []
        self.resumed_threads: list[dict[str, Any]] = []
        self.models = [
            {
                "id": "gpt-test",
                "model": "gpt-test",
                "displayName": "GPT Test",
                "isDefault": True,
                "supportedReasoningEfforts": [
                    {"reasoningEffort": "low"},
                    {"reasoningEffort": "high"},
                ],
                "serviceTiers": [{"id": "priority", "name": "Fast"}],
            }
        ]

    def add_notification_handler(self, handler: Any) -> None:
        self.notification_handlers.append(handler)

    def set_server_request_handler(self, handler: Any) -> None:
        self.server_request_handler = handler

    async def list_threads(self, **_: Any) -> list[ThreadSummary]:
        return []

    async def read_thread(self, thread_id: str, **_: Any) -> dict[str, Any]:
        return {"id": thread_id, "status": {"type": "idle"}, "turns": [], "path": None}

    async def list_turns(
        self,
        thread_id: str,
        *,
        limit: int = 100,
        items_view: str = "summary",
        sort_direction: str = "desc",
        cursor: str | None = None,
    ) -> dict[str, Any]:
        raw = await self.read_thread(thread_id)
        turns = list(raw.get("turns") or [])
        if sort_direction == "desc":
            turns.reverse()
        return {"data": turns[:limit], "nextCursor": None}

    async def resume_thread(self, thread_id: str, **kwargs: Any) -> dict[str, Any]:
        self.resumed_threads.append({"thread_id": thread_id, **kwargs})
        return {"id": thread_id}

    async def list_models(self) -> list[dict[str, Any]]:
        return self.models

    async def update_thread_settings(self, thread_id: str, **kwargs: Any) -> dict[str, Any]:
        value = {"thread_id": thread_id, **kwargs}
        self.setting_updates.append(value)
        return value

    async def start_thread(self, **kwargs: Any) -> dict[str, Any]:
        self.started_threads.append(kwargs)
        return {"id": f"thread-probe-{len(self.started_threads)}"}

    async def archive_thread(self, thread_id: str) -> None:
        self.archived_threads.append(thread_id)

    async def unsubscribe_thread(self, thread_id: str) -> None:
        self.unsubscribed_threads.append(thread_id)

    async def steer_turn(
        self,
        thread_id: str,
        turn_id: str,
        inputs: list[dict[str, Any]],
        *,
        client_message_id: str,
    ) -> None:
        self.steered.append(
            {
                "thread_id": thread_id,
                "turn_id": turn_id,
                "inputs": inputs,
                "client_message_id": client_message_id,
            }
        )

    def pending_server_request(self, rpc_id: str) -> dict[str, Any] | None:
        return self.pending_requests.get(str(rpc_id))

    async def respond_server_request(self, rpc_id: str, result: dict[str, Any]) -> None:
        self.responses.append((str(rpc_id), result))
        self.pending_requests.pop(str(rpc_id), None)

    async def respond_server_error(self, rpc_id: str, code: int, message: str) -> None:
        self.errors.append((str(rpc_id), code, message))


class FastCompletingCodex(FakeCodex):
    def __init__(self) -> None:
        super().__init__()
        self.thread_names: list[tuple[str, str]] = []
        self.archived_threads: list[str] = []

    async def start_thread(self, **_: Any) -> dict[str, Any]:
        return {"id": "thread-fast-scratch"}

    async def set_thread_name(self, thread_id: str, name: str) -> None:
        self.thread_names.append((thread_id, name))

    async def archive_thread(self, thread_id: str) -> None:
        self.archived_threads.append(thread_id)

    async def start_turn(
        self, thread_id: str, inputs: list[dict[str, Any]], **_: Any
    ) -> dict[str, Any]:
        turn = {
            "id": "turn-fast",
            "status": "completed",
            "items": [
                {
                    "type": "agentMessage",
                    "id": "item-final",
                    "phase": "final_answer",
                    "text": "快速完成",
                }
            ],
        }
        for handler in self.notification_handlers:
            await handler(
                {
                    "method": "turn/started",
                    "params": {"threadId": thread_id, "turn": {"id": "turn-fast"}},
                }
            )
            await handler(
                {"method": "turn/completed", "params": {"threadId": thread_id, "turn": turn}}
            )
        return turn


class StaleInProgressCodex(FakeCodex):
    async def read_thread(self, thread_id: str, **_: Any) -> dict[str, Any]:
        return {
            "id": thread_id,
            "status": {"type": "notLoaded"},
            "path": None,
            "turns": [{"id": "turn-stale", "status": "inProgress", "items": []}],
        }


class ThreadStartCrashCodex(FakeCodex):
    def __init__(self, db: BridgeDB, message_id: str) -> None:
        super().__init__()
        self.db = db
        self.message_id = message_id
        self.state_at_rpc: str | None = None

    async def start_thread(self, **_: Any) -> dict[str, Any]:
        self.state_at_rpc = self.db.inbox_state(self.message_id)
        raise ConnectionError("connection lost across thread/start")


class TurnStartCrashCodex(FakeCodex):
    def __init__(self, db: BridgeDB, message_id: str) -> None:
        super().__init__()
        self.db = db
        self.message_id = message_id
        self.state_at_rpc: str | None = None

    async def start_turn(self, *_: Any, **__: Any) -> dict[str, Any]:
        self.state_at_rpc = self.db.inbox_state(self.message_id)
        raise ConnectionError("connection lost across turn/start")


class StaticThreadsCodex(FakeCodex):
    def __init__(self, threads: list[ThreadSummary]) -> None:
        super().__init__()
        self.threads = threads

    async def list_threads(self, **_: Any) -> list[ThreadSummary]:
        return self.threads


class ThreadHistoryCodex(FakeCodex):
    def __init__(self, turns: list[dict[str, Any]]) -> None:
        super().__init__()
        self.turns = turns

    async def read_thread(self, thread_id: str, **_: Any) -> dict[str, Any]:
        return {"id": thread_id, "status": {"type": "idle"}, "turns": self.turns}


class RestartContinuationCodex(ThreadHistoryCodex):
    def __init__(self) -> None:
        super().__init__(
            [
                {
                    "id": "turn-before-restart",
                    "status": "interrupted",
                    "items": [
                        {
                            "type": "agentMessage",
                            "phase": "commentary",
                            "text": "已经完成了一部分现场工作",
                        }
                    ],
                }
            ]
        )
        self.started_inputs: list[list[dict[str, Any]]] = []

    async def start_turn(
        self, thread_id: str, inputs: list[dict[str, Any]], **_: Any
    ) -> dict[str, Any]:
        self.started_inputs.append(inputs)
        turn = {
            "id": "turn-after-restart",
            "status": "completed",
            "items": [
                {
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": "恢复后完成并正常汇报",
                }
            ],
        }
        self.turns.append(turn)
        for handler in self.notification_handlers:
            await handler(
                {
                    "method": "turn/started",
                    "params": {
                        "threadId": thread_id,
                        "turn": {"id": "turn-after-restart"},
                    },
                }
            )
            await handler(
                {
                    "method": "turn/completed",
                    "params": {"threadId": thread_id, "turn": turn},
                }
            )
        return turn


class SummaryOnlyCodex(FakeCodex):
    async def read_thread(self, thread_id: str, *, include_turns: bool = True) -> dict[str, Any]:
        assert include_turns is False
        return {"id": thread_id, "status": {"type": "idle"}}

    async def list_turns(self, thread_id: str, **kwargs: Any) -> dict[str, Any]:
        assert kwargs["items_view"] in {"summary", "notLoaded"}
        return {
            "data": [
                {
                    "id": "turn-summary",
                    "status": "completed",
                    "items": [{"type": "agentMessage", "text": "small"}],
                }
            ],
            "nextCursor": None,
        }


class FakeGateway:
    def __init__(self, *, configured_roles: set[AppRole] | None = None) -> None:
        self.configured_roles = configured_roles or set()
        self.texts: list[dict[str, Any]] = []
        self.cards: list[dict[str, Any]] = []
        self.patches: list[dict[str, Any]] = []
        self.history: list[IncomingMessage] = []
        self.history_calls: list[dict[str, Any]] = []
        self.downloads: list[tuple[AppRole, Attachment]] = []

    def configured(self, role: AppRole) -> bool:
        return role in self.configured_roles

    async def send_text(
        self,
        role: AppRole,
        receive_id: str,
        text: str,
        **kwargs: Any,
    ) -> str:
        self.texts.append({"role": role, "receive_id": receive_id, "text": text, **kwargs})
        return f"text-{len(self.texts)}"

    async def send_card(
        self,
        role: AppRole,
        receive_id: str,
        card: dict[str, Any],
        **kwargs: Any,
    ) -> str:
        self.cards.append({"role": role, "receive_id": receive_id, "card": card, **kwargs})
        return f"card-{len(self.cards)}"

    async def patch_card(self, role: AppRole, message_id: str, card: dict[str, Any]) -> None:
        self.patches.append({"role": role, "message_id": message_id, "card": card})

    async def download_attachment(self, role: AppRole, attachment: Attachment) -> tuple[bytes, str]:
        self.downloads.append((role, attachment))
        return b"staged attachment", attachment.name

    async def list_chat_messages(
        self,
        role: AppRole,
        chat_id: str,
        *,
        start_time_seconds: int,
        end_time_seconds: int,
    ) -> list[IncomingMessage]:
        self.history_calls.append(
            {
                "role": role,
                "chat_id": chat_id,
                "start_time_seconds": start_time_seconds,
                "end_time_seconds": end_time_seconds,
            }
        )
        return list(self.history)


def make_config(tmp_path: Path, **feishu_values: str) -> BridgeConfig:
    state = tmp_path / "state"
    config = BridgeConfig(
        config_path=tmp_path / "config.toml",
        state_dir=state,
        database_path=state / "bridge.sqlite",
        inbox_dir=state / "inbox",
        outbox_dir=state / "outbox",
        admin_scratch_dir=state / "admin-scratch",
        managed_workspaces_dir=state / "workspaces",
        allowed_workspace_roots=[tmp_path],
        feishu=FeishuConfig(**feishu_values),
    )
    config.prepare_dirs()
    return config


def incoming(
    message_id: str,
    *,
    role: AppRole = "conversation",
    chat_id: str = "oc_thread",
    open_id: str = "ou_conversation_owner",
    text: str = "继续",
    tenant_key: str = "tenant-test",
    create_time_ms: int = 1_000,
    chat_type: str | None = None,
) -> IncomingMessage:
    return IncomingMessage(
        message_id=message_id,
        chat_id=chat_id,
        chat_type=chat_type or ("group" if role == "conversation" else "p2p"),
        app_role=role,
        sender_open_id=open_id,
        sender_user_id=None,
        sender_union_id="on_same_human",
        text=text,
        message_type="text",
        create_time_ms=create_time_ms,
        tenant_key=tenant_key,
        app_id=f"cli_{role}",
        sender_type="user",
    )


def stage(db: BridgeDB, message: IncomingMessage) -> InboxItem:
    assert db.enqueue_incoming(message) is True
    claimed = db.claim_incoming("test-worker")
    assert claimed is not None
    assert claimed.message.message_id == message.message_id
    return claimed


def bind_thread(db: BridgeDB, *, thread_id: str = "thread-1", chat_id: str = "oc_thread") -> None:
    db.upsert_thread(
        ThreadSummary(
            thread_id=thread_id,
            name="受控对话",
            preview="",
            cwd="/workspace/test",
            created_at=1,
            updated_at=2,
            source_kind="cli",
        )
    )
    db.bind_chat(thread_id, chat_id)


@pytest.mark.asyncio
async def test_pairing_keeps_app_scoped_open_ids_for_the_same_human(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    code, _ = generate_pairing_code(db, ttl_seconds=300)

    try:
        admin = incoming(
            "om-pair-admin",
            role="admin",
            chat_id="oc_admin_p2p",
            open_id="ou_admin_app_scope",
            text=f"配对 {code}",
        )
        conversation = incoming(
            "om-pair-conversation",
            chat_id="oc_conversation_p2p",
            open_id="ou_conversation_app_scope",
            text=f"pair {code}",
            create_time_ms=2_000,
            chat_type="p2p",
        )

        await service._route_incoming(stage(db, admin))
        await service._route_incoming(stage(db, conversation))

        assert service._owner("admin") == "ou_admin_app_scope"
        assert service._owner("conversation") == "ou_conversation_app_scope"
        assert db.get_setting("owner_chat_id:admin") == "oc_admin_p2p"
        assert db.get_setting("owner_chat_id:conversation") == "oc_conversation_p2p"
        assert [item["role"] for item in gateway.texts] == ["admin", "conversation"]
        assert db.inbox_counts() == {"done": 2}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_pairing_rejects_second_app_from_another_tenant(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    code, _ = generate_pairing_code(db, ttl_seconds=300)
    try:
        await service._route_incoming(
            stage(
                db,
                incoming(
                    "pair-a",
                    role="admin",
                    open_id="ou-a",
                    text=f"配对 {code}",
                    tenant_key="tenant-a",
                ),
            )
        )
        await service._route_incoming(
            stage(
                db,
                incoming(
                    "pair-b",
                    open_id="ou-b",
                    text=f"配对 {code}",
                    tenant_key="tenant-b",
                    chat_type="p2p",
                ),
            )
        )
        assert service._owner("admin") == "ou-a"
        assert service._owner("conversation") == ""
        assert db.get_setting("paired_tenant_key") == "tenant-a"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_completed_before_turn_start_response_leaves_no_ghost_active(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FastCompletingCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)
    message = incoming("om-fast", text="快速回答")
    item = stage(db, message)
    job = ScheduledMessage(
        inbox=item,
        binding=db.get_binding_by_thread("thread-1"),
        progress_message_id="progress-fast",
        app_role="conversation",
        chat_id="oc_thread",
    )
    try:
        await service._execute_job("thread-1", job)
        assert service._active_by_thread == {}
        assert service._active_by_turn == {}
        assert db.inbox_state("om-fast") == "done"
        assert db.outbox_counts() == {"pending": 1}
        assert db.list_recoverable_turn_jobs() == []
    finally:
        db.close()


@pytest.mark.asyncio
async def test_execute_job_discards_stale_completed_active_turn(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FastCompletingCodex()
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    bind_thread(db)
    stale = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-stale-completed",
        chat_id="oc_thread",
    )
    service._register_active(stale)
    service._completed_turns.add(stale.turn_id)
    service._turn_done[stale.turn_id].set()
    item = stage(db, incoming("om-after-stale", text="继续执行"))
    job = ScheduledMessage(
        inbox=item,
        binding=db.get_binding_by_thread("thread-1"),
        progress_message_id="progress-after-stale",
        app_role="conversation",
        chat_id="oc_thread",
    )

    try:
        await service._execute_job("thread-1", job)

        assert service._active_by_thread == {}
        assert service._active_by_turn == {}
        assert db.inbox_state("om-after-stale") == "done"
        assert db.list_recoverable_turn_jobs() == []
    finally:
        db.close()


@pytest.mark.asyncio
async def test_stale_completed_active_turn_does_not_inflate_queue_position(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    gateway = FakeGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    bind_thread(db)
    stale = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-stale-completed",
        chat_id="oc_thread",
    )
    service._register_active(stale)
    service._completed_turns.add(stale.turn_id)
    service._turn_done[stale.turn_id].set()
    blocker = asyncio.create_task(asyncio.Event().wait())
    service._thread_workers["thread-1"] = blocker

    try:
        item = stage(db, incoming("om-after-stale", text="继续执行"))
        binding = db.get_binding_by_thread("thread-1")
        assert binding is not None
        await service._queue_thread_message(item, binding)

        content = gateway.cards[-1]["card"]["elements"][0]["content"]
        assert "正在启动" in content
        assert "前面还有" not in content
        assert service._active_by_thread == {}
        assert service._active_by_turn == {}
    finally:
        blocker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
        db.close()


@pytest.mark.asyncio
async def test_restart_never_resumes_stale_in_progress_turn(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, StaleInProgressCodex(), FakeGateway())  # type: ignore[arg-type]
    db.upsert_turn_job(
        TurnJob(
            message_id="om-stale",
            thread_id="thread-stale",
            turn_id="turn-stale",
            app_role="conversation",
            chat_id="oc-thread",
            progress_message_id="om-progress",
            state="running",
        )
    )
    try:
        await service._recover_turn_jobs()
        assert db.get_setting("blocked_thread:thread-stale") == "turn-stale"
        assert db.list_recoverable_turn_jobs() == []
        assert service._active_by_thread == {}
        assert db.outbox_counts() == {"pending": 1}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_recovery_poll_finalizes_terminal_turn_still_marked_active(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    terminal_turn = {
        "id": "turn-missed-completion",
        "status": "interrupted",
        "items": [
            {
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "已保存的最终回复",
            }
        ],
    }
    service = BridgeService(
        config,
        db,
        ThreadHistoryCodex([terminal_turn]),
        FakeGateway(),
    )  # type: ignore[arg-type]
    db.upsert_turn_job(
        TurnJob(
            message_id="om-missed-completion",
            thread_id="thread-1",
            turn_id="turn-missed-completion",
            app_role="conversation",
            chat_id="oc_thread",
            progress_message_id="card-missed-completion",
            state="accepted",
        )
    )
    service._register_active(
        ActiveTurn(
            thread_id="thread-1",
            turn_id="turn-missed-completion",
            chat_id="oc_thread",
            progress_message_id="card-missed-completion",
        )
    )

    try:
        await service._recover_turn_jobs()

        assert service._active_by_thread == {}
        assert service._active_by_turn == {}
        assert db.list_recoverable_turn_jobs() == []
        outbound = db.claim_outbox("test-worker")
        assert outbound is not None
        assert outbound.content == {"text": "已保存的最终回复"}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_recovery_snapshot_does_not_reregister_turn_completed_during_lookup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    terminal_turn = {
        "id": "turn-completed-during-recovery",
        "status": "completed",
        "items": [
            {
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "并发完成结果",
            }
        ],
    }
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-completed-during-recovery",
        chat_id="oc_thread",
    )
    db.upsert_turn_job(
        TurnJob(
            message_id="om-completed-during-recovery",
            thread_id=active.thread_id,
            turn_id=active.turn_id,
            app_role="conversation",
            chat_id=active.chat_id,
            progress_message_id=None,
            state="accepted",
        )
    )

    async def complete_during_lookup(_: str, __: str) -> dict[str, Any]:
        await service._finalize_turn(active, terminal_turn)
        return terminal_turn

    monkeypatch.setattr(service, "_find_turn_summary", complete_during_lookup)
    try:
        await service._recover_turn_jobs()

        assert service._active_by_thread == {}
        assert service._active_by_turn == {}
        assert service._turn_done[active.turn_id].is_set()
        assert db.list_recoverable_turn_jobs() == []
    finally:
        db.close()


@pytest.mark.asyncio
async def test_startup_continues_interrupted_bridge_turn_without_external_echo(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = RestartContinuationCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)
    db.upsert_turn_job(
        TurnJob(
            message_id="om-before-restart",
            thread_id="thread-1",
            turn_id="turn-before-restart",
            app_role="conversation",
            chat_id="oc_thread",
            progress_message_id="card-before-restart",
            state="accepted",
        )
    )
    summary = ThreadSummary(
        thread_id="thread-1",
        name="受控对话",
        preview="",
        cwd="/workspace/test",
        created_at=1,
        updated_at=2,
        source_kind="cli",
    )
    db.set_setting("external_sync_initialized:thread-1", "1")

    try:
        await service._recover_turn_jobs(startup=True)

        assert db.is_turn_synced("thread-1", "turn-before-restart") is True
        assert db.is_bridge_turn("turn-after-restart") is True
        assert db.list_recoverable_turn_jobs() == []
        assert "系统恢复指令" in codex.started_inputs[0][0]["text"]
        assert "om-before-restart" not in codex.started_inputs[0][0]["text"]
        outbound = db.claim_outbox("restart-test")
        assert outbound is not None
        assert outbound.content == {"text": "恢复后完成并正常汇报"}
        db.complete_outbox(
            outbound.outbox_key,
            thread_id=outbound.thread_id,
            turn_id=outbound.turn_id,
        )

        await service._sync_external_updates([summary])
        assert db.claim_outbox("restart-test-2") is None
        assert len(gateway.patches) >= 2
    finally:
        db.close()


@pytest.mark.asyncio
async def test_graceful_drain_waits_for_accepted_turn_to_finish(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-draining",
        turn_id="turn-draining",
        chat_id="oc-draining",
    )
    service._register_active(active)
    try:
        waiter = asyncio.create_task(service.wait_for_drain(1.0))
        await asyncio.sleep(0.02)
        assert service._draining is True
        assert waiter.done() is False

        service._active_by_turn.pop(active.turn_id)
        service._active_by_thread.pop(active.thread_id)
        assert await waiter is True
    finally:
        db.close()


@pytest.mark.asyncio
async def test_unauthorized_sender_is_completed_without_reply_or_codex_dispatch(
    tmp_path: Path,
) -> None:
    config = make_config(
        tmp_path,
        owner_conversation_open_id="ou_expected_owner",
    )
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        attacker = incoming("om-attacker", open_id="ou_someone_else")
        await service._route_incoming(stage(db, attacker))

        assert db.inbox_state("om-attacker") == "done"
        assert gateway.texts == []
        assert gateway.cards == []
        assert codex.steered == []
        assert service._thread_queues == {}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_same_thread_messages_are_fifo_and_steer_targets_the_active_turn(
    tmp_path: Path,
) -> None:
    config = make_config(
        tmp_path,
        owner_conversation_open_id="ou_conversation_owner",
    )
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    # Occupy the worker slot without consuming the queue so routing order can
    # be asserted deterministically.
    blocker = asyncio.create_task(asyncio.Event().wait())
    service._thread_workers["thread-1"] = blocker
    try:
        first = incoming("om-first", text="第一条", create_time_ms=1_000)
        second = incoming("om-second", text="第二条", create_time_ms=2_000)
        await service._route_incoming(stage(db, first))
        await service._route_incoming(stage(db, second))

        queue = service._thread_queues["thread-1"]
        first_job = queue.get_nowait()
        second_job = queue.get_nowait()
        assert [first_job.inbox.message.message_id, second_job.inbox.message.message_id] == [
            "om-first",
            "om-second",
        ]
        assert db.inbox_state("om-first") == "queued"
        assert db.inbox_state("om-second") == "queued"
        assert "正在启动" in gateway.cards[0]["card"]["elements"][0]["content"]
        assert "前面还有 1 条" in gateway.cards[1]["card"]["elements"][0]["content"]

        active = ActiveTurn(
            thread_id="thread-1",
            turn_id="turn-live",
            chat_id="oc_thread",
            progress_message_id="card-live",
        )
        service._register_active(active)
        steering = incoming("om-steer", text="!steer 优先检查失败日志", create_time_ms=3_000)
        await service._route_incoming(stage(db, steering))

        assert len(codex.steered) == 1
        assert codex.steered[0]["thread_id"] == "thread-1"
        assert codex.steered[0]["turn_id"] == "turn-live"
        assert codex.steered[0]["client_message_id"] == "om-steer"
        assert codex.steered[0]["inputs"][0] == {
            "type": "text",
            "text": "优先检查失败日志",
            "text_elements": [],
        }
        assert "专用目录" in codex.steered[0]["inputs"][1]["text"]
        assert db.inbox_state("om-steer") == "done"
        assert gateway.texts[-1]["idempotency_key"] == "steered:om-steer"
        assert queue.empty()
    finally:
        blocker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
        db.close()


@pytest.mark.asyncio
async def test_steer_after_missed_completion_delivers_result_and_queues_new_turn(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    terminal_turn = {
        "id": "turn-terminal",
        "status": "completed",
        "items": [
            {
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "旧任务已经完成",
            }
        ],
    }
    codex = ThreadHistoryCodex([terminal_turn])
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)
    db.upsert_turn_job(
        TurnJob(
            message_id="om-original",
            thread_id="thread-1",
            turn_id="turn-terminal",
            app_role="conversation",
            chat_id="oc_thread",
            progress_message_id="card-terminal",
            state="running",
        )
    )
    service._register_active(
        ActiveTurn(
            thread_id="thread-1",
            turn_id="turn-terminal",
            chat_id="oc_thread",
            progress_message_id="card-terminal",
        )
    )
    blocker = asyncio.create_task(asyncio.Event().wait())
    service._thread_workers["thread-1"] = blocker

    try:
        message = incoming("om-late-steer", text="!steer 继续处理新要求")
        await service._route_incoming(stage(db, message))

        assert codex.steered == []
        assert service._active_by_thread == {}
        assert service._active_by_turn == {}
        assert db.list_recoverable_turn_jobs() == []
        assert db.inbox_state("om-late-steer") == "queued"
        queued = service._thread_queues["thread-1"].get_nowait()
        assert queued.inbox.message.text == "继续处理新要求"
        outbound = db.claim_outbox("test-worker")
        assert outbound is not None
        assert outbound.content == {"text": "旧任务已经完成"}
    finally:
        blocker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
        db.close()


@pytest.mark.asyncio
async def test_admin_freeform_message_is_durably_queued(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_admin_open_id="ou_admin_owner")
    db = BridgeDB(config.database_path)
    gateway = FakeGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    message = incoming(
        "om-admin-queued",
        role="admin",
        chat_id="oc_admin_p2p",
        open_id="ou_admin_owner",
        text="解释一下当前桥的状态",
    )
    try:
        await service._route_incoming(stage(db, message))
        queued = service._admin_queue.get_nowait()
        assert queued.inbox.message.message_id == message.message_id
        assert db.inbox_state(message.message_id) == "queued"
        assert gateway.cards[-1]["idempotency_key"] == "admin-progress:om-admin-queued"
        service._admin_queue.task_done()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_conversation_private_chat_uses_admin_control_plane(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    gateway = FakeGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    message = incoming(
        "om-conversation-private-help",
        chat_id="oc_conversation_p2p",
        open_id="ou_conversation_owner",
        text="帮助",
        chat_type="p2p",
    )
    try:
        await service._route_incoming(stage(db, message))

        assert db.inbox_state(message.message_id) == "done"
        assert gateway.texts[-1]["role"] == "conversation"
        assert "新对话" in gateway.texts[-1]["text"]
        assert "额度" in gateway.texts[-1]["text"]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_conversation_private_freeform_is_durably_queued(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    gateway = FakeGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    message = incoming(
        "om-conversation-private-task",
        chat_id="oc_conversation_p2p",
        open_id="ou_conversation_owner",
        text="解释一下当前桥的状态",
        chat_type="p2p",
    )
    try:
        await service._route_incoming(stage(db, message))

        queued = service._admin_queue.get_nowait()
        assert queued.app_role == "conversation"
        assert queued.inbox.message.message_id == message.message_id
        assert db.inbox_state(message.message_id) == "queued"
        assert gateway.cards[-1]["role"] == "conversation"
        service._admin_queue.task_done()
    finally:
        db.close()


@pytest.mark.asyncio
async def test_conversation_private_fast_task_keeps_codex_role_through_final(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FastCompletingCodex()
    gateway = FakeGateway(configured_roles={"conversation"})
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    message = incoming(
        "om-conversation-private-fast",
        chat_id="oc_conversation_p2p",
        open_id="ou_conversation_owner",
        text="快速完成一个临时任务",
        chat_type="p2p",
    )
    worker = asyncio.create_task(service._admin_worker())
    try:
        await service._route_incoming(stage(db, message))
        await asyncio.wait_for(service._admin_queue.join(), timeout=1)

        assert db.inbox_state(message.message_id) == "done"
        assert db.list_recoverable_turn_jobs() == []
        outbound = db.claim_outbox("test-outbox")
        assert outbound is not None
        assert outbound.app_role == "conversation"
        assert outbound.receive_id == "oc_conversation_p2p"
        assert outbound.receive_id_type == "chat_id"
        assert outbound.content == {"text": "快速完成"}
        assert codex.archived_threads == ["thread-fast-scratch"]
    finally:
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker
        db.close()


@pytest.mark.asyncio
async def test_attachment_only_message_waits_for_next_codex_text(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    gateway = FakeGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    bind_thread(db)
    blocker = asyncio.create_task(asyncio.Event().wait())
    service._thread_workers["thread-1"] = blocker
    media = incoming("om-held-image", text="", create_time_ms=1_000)
    media.message_type = "image"
    media.attachments = [
        Attachment(
            kind="image",
            name="screen.png",
            message_id=media.message_id,
            image_key="img-key",
        )
    ]
    text = incoming("om-image-instruction", text="分析这张图", create_time_ms=2_000)
    try:
        await service._route_incoming(stage(db, media))

        assert db.inbox_state(media.message_id) == "held"
        assert len(gateway.downloads) == 1
        assert db.outbox_counts() == {"pending": 1}
        assert service._thread_queues == {}

        await service._route_incoming(stage(db, text))
        queued = service._thread_queues["thread-1"].get_nowait()
        assert queued.inbox.message.message_id == text.message_id
        assert [item.message_id for item in queued.inbox.message.attachments] == [media.message_id]
        assert queued.inbox.message.attachments[0].local_path is not None
        assert db.inbox_state(media.message_id) == "done"
        service._thread_queues["thread-1"].task_done()
    finally:
        blocker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await blocker
        db.close()


@pytest.mark.asyncio
async def test_runtime_command_does_not_consume_held_attachment(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    bind_thread(db)
    media = incoming("om-held-before-status", text="", create_time_ms=1_000)
    media.message_type = "image"
    media.attachments = [
        Attachment(
            kind="image",
            name="screen.png",
            message_id=media.message_id,
            image_key="img-key",
        )
    ]
    try:
        await service._route_incoming(stage(db, media))
        await service._route_incoming(
            stage(db, incoming("om-runtime-status", text="/status", create_time_ms=2_000))
        )

        assert db.inbox_state(media.message_id) == "held"
        assert db.inbox_state("om-runtime-status") == "done"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_system_alert_prefers_conversation_private_bot(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        owner_admin_open_id="ou_admin_owner",
        owner_conversation_open_id="ou_conversation_owner",
    )
    db = BridgeDB(config.database_path)
    gateway = FakeGateway(configured_roles={"admin", "conversation"})
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    try:
        await service._notify_admin("测试告警")

        assert gateway.texts[-1]["role"] == "conversation"
        assert gateway.texts[-1]["receive_id"] == "ou_conversation_owner"
        assert gateway.texts[-1]["receive_id_type"] == "open_id"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_runtime_commands_persist_audit_and_apply_to_codex(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    config.allow_remote_full_access = True
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        commands = [
            ("runtime-permission", "/permissions full-access"),
            ("runtime-model", "/model gpt-test high"),
            ("runtime-speed", "/fast"),
            ("runtime-show", "/status"),
            ("runtime-history", "!配置记录"),
        ]
        for index, (message_id, command) in enumerate(commands, 1):
            await service._route_incoming(
                stage(db, incoming(message_id, text=command, create_time_ms=index * 1000))
            )

        settings = service._runtime_settings("thread-1")
        assert settings == RuntimeSettings(
            model="gpt-test",
            effort="high",
            service_tier="priority",
            approval_policy="never",
            sandbox="danger-full-access",
        )
        assert codex.setting_updates[-1] == {
            "thread_id": "thread-1",
            "approval_policy": "never",
            "sandbox": "danger-full-access",
            "model": "gpt-test",
            "effort": "high",
            "service_tier": "priority",
        }
        assert len(db.runtime_config_history("thread-1")) == 5
        assert "GPT" not in gateway.texts[-2]["text"] or "gpt-test" in gateway.texts[-2]["text"]
        assert "最近配置变更" in gateway.texts[-1]["text"]
        assert db.inbox_counts() == {"done": len(commands)}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_cli_setting_commands_render_pickers_and_toggle_fast(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        await service._route_incoming(stage(db, incoming("pick-model", text="/model")))
        model_card = gateway.cards[-1]["card"]
        model_buttons = [
            action for element in model_card["elements"] for action in element.get("actions", [])
        ]
        assert model_buttons[0]["value"] == {
            "kind": "codex_setting",
            "setting": "model",
            "model": "gpt-test",
        }

        await service._route_incoming(stage(db, incoming("pick-permissions", text="/permissions")))
        permission_card = gateway.cards[-1]["card"]
        profiles = [
            action["value"]["profile"]
            for element in permission_card["elements"]
            for action in element.get("actions", [])
        ]
        assert profiles == ["read-only", "default"]

        await service._route_incoming(stage(db, incoming("toggle-fast-1", text="/fast")))
        assert service._runtime_settings("thread-1").service_tier == "priority"
        await service._route_incoming(stage(db, incoming("toggle-fast-2", text="/fast")))
        assert service._runtime_settings("thread-1").service_tier is None
        assert "Fast 模式已关闭" in gateway.texts[-1]["text"]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_remote_full_access_requires_explicit_local_opt_in(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        await service._route_incoming(
            stage(db, incoming("unsafe-permission", text="/permissions full-access"))
        )

        settings = service._runtime_settings("thread-1")
        assert settings.approval_policy == "on-request"
        assert settings.sandbox == "workspace-write"
        assert db.runtime_config_history("thread-1") == []
        assert "远程 Full Access 默认禁用" in gateway.texts[-1]["text"]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_cli_version_change_fails_closed_for_slash_settings(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    codex.cli_version = "9.9.9"
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        await service._probe_runtime_settings_compatibility()
        await service._route_incoming(stage(db, incoming("future-fast", text="/fast")))
        assert service._runtime_settings("thread-1").service_tier is None
        assert "兼容门禁已触发" in gateway.texts[-1]["text"]
        assert "9.9.9" in gateway.texts[-1]["text"]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_cli_version_change_sends_repair_card_to_codex_private_chat(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    codex.cli_version = "9.9.9"
    gateway = FakeGateway(configured_roles={"conversation"})
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]

    try:
        await service._probe_runtime_settings_compatibility()

        assert gateway.cards[-1]["role"] == "conversation"
        assert gateway.cards[-1]["receive_id"] == "ou_conversation_owner"
        actions = gateway.cards[-1]["card"]["elements"][-1]["actions"]
        assert [action["text"]["content"] for action in actions] == [
            "检测并修复",
            "暂不处理",
        ]
        assert actions[0]["value"] == {
            "kind": "codex_compatibility",
            "action": "repair",
            "version": "9.9.9",
        }
    finally:
        db.close()


@pytest.mark.asyncio
async def test_repair_card_exercises_protocol_and_unlocks_cli_settings(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    codex.cli_version = "9.9.9"
    gateway = FakeGateway(configured_roles={"conversation"})
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        await service._probe_runtime_settings_compatibility()
        repair = incoming(
            "compat-repair",
            text="/bridge-settings-compat repair 9.9.9",
            chat_type="p2p",
        )
        repair.message_type = "card_action"
        await service._route_incoming(stage(db, repair))

        assert service._runtime_compatibility_error is None
        assert db.get_setting("codex_settings_verified_version") == "9.9.9"
        assert codex.started_threads == [
            {
                "cwd": str(config.admin_scratch_dir),
                "approval_policy": "on-request",
                "sandbox": "workspace-write",
                "model": None,
                "service_tier": None,
                "ephemeral": True,
            }
        ]
        assert codex.setting_updates[0]["thread_id"] == "thread-probe-1"
        assert codex.unsubscribed_threads == ["thread-probe-1"]
        assert gateway.cards[-1]["card"]["header"]["template"] == "green"

        await service._route_incoming(stage(db, incoming("fast-after-repair", text="/fast")))
        assert service._runtime_settings("thread-1").service_tier == "priority"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_dismissed_cli_version_prompt_stays_gated_without_reprompt(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    codex.cli_version = "9.9.9"
    gateway = FakeGateway(configured_roles={"conversation"})
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]

    try:
        await service._probe_runtime_settings_compatibility()
        initial_cards = len(gateway.cards)
        dismiss = incoming(
            "compat-dismiss",
            text="/bridge-settings-compat dismiss 9.9.9",
            chat_type="p2p",
        )
        dismiss.message_type = "card_action"
        await service._route_incoming(stage(db, dismiss))
        assert db.get_setting("codex_settings_prompt_dismissed_version") == "9.9.9"

        result_cards = len(gateway.cards)
        assert result_cards == initial_cards + 1
        await service._probe_runtime_settings_compatibility()
        assert len(gateway.cards) == result_cards
        assert service._runtime_compatibility_error is not None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_global_runtime_defaults_cover_new_scopes_and_can_be_overridden(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    config.model = "gpt-test"
    config.model_reasoning_effort = "high"
    config.service_tier = "priority"
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)

    try:
        assert service._runtime_settings("future-thread") == RuntimeSettings(
            model="gpt-test",
            effort="high",
            service_tier="priority",
            approval_policy="on-request",
            sandbox="workspace-write",
        )
        assert service._runtime_settings("admin").model == "gpt-test"

        await service._route_incoming(stage(db, incoming("default-fast-off", text="/fast")))
        assert service._runtime_settings("thread-1").service_tier is None
        await service._route_incoming(
            stage(db, incoming("default-effort-off", text="/model gpt-test default"))
        )
        assert service._runtime_settings("thread-1").effort is None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_admin_new_thread_crosses_ambiguity_boundary_before_rpc(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_admin_open_id="ou_admin_owner")
    db = BridgeDB(config.database_path)
    message = incoming(
        "om-new-crash",
        role="admin",
        chat_id="oc_admin_p2p",
        open_id="ou_admin_owner",
        text="新对话 崩溃边界测试",
    )
    codex = ThreadStartCrashCodex(db, message.message_id)
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    item = stage(db, message)
    try:
        with pytest.raises(ConnectionError) as caught:
            await service._route_incoming(item)
        service._record_incoming_failure(item, caught.value)

        assert codex.state_at_rpc == "dispatching"
        assert db.inbox_state(message.message_id) == "ambiguous"
        assert db.outbox_counts() == {"pending": 1}
        assert db.recover_inbox_after_restart() == []
        assert db.claim_incoming("recovery-worker") is None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_admin_helper_thread_crosses_ambiguity_boundary_before_rpc(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_admin_open_id="ou_admin_owner")
    db = BridgeDB(config.database_path)
    message = incoming(
        "om-helper-crash",
        role="admin",
        chat_id="oc_admin_p2p",
        open_id="ou_admin_owner",
        text="做一个零散解释",
    )
    codex = ThreadStartCrashCodex(db, message.message_id)
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    worker = asyncio.create_task(service._admin_worker())
    try:
        await service._route_incoming(stage(db, message))
        await asyncio.wait_for(service._admin_queue.join(), timeout=1)

        assert codex.state_at_rpc == "dispatching"
        assert db.inbox_state(message.message_id) == "ambiguous"
        assert db.outbox_counts() == {"pending": 1}
        assert db.recover_inbox_after_restart() == []
        assert db.claim_incoming("recovery-worker") is None
    finally:
        worker.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker
        db.close()


@pytest.mark.asyncio
async def test_conversation_turn_start_crash_queues_durable_phone_alert(tmp_path: Path) -> None:
    config = make_config(tmp_path, owner_conversation_open_id="ou_conversation_owner")
    db = BridgeDB(config.database_path)
    message = incoming("om-turn-crash", text="执行一个任务")
    codex = TurnStartCrashCodex(db, message.message_id)
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    bind_thread(db)
    item = stage(db, message)
    db.mark_incoming_queued(message.message_id)
    job = ScheduledMessage(
        inbox=item,
        binding=db.get_binding_by_thread("thread-1"),
        progress_message_id="progress-crash",
        app_role="conversation",
        chat_id="oc_thread",
    )
    try:
        await service._execute_job("thread-1", job)
        assert codex.state_at_rpc == "dispatching"
        assert db.inbox_state(message.message_id) == "ambiguous"
        assert db.outbox_counts() == {"pending": 1}
        assert db.claim_incoming("recovery-worker") is None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_reconcile_never_binds_orphan_from_admin_scratch(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    scratch = ThreadSummary(
        thread_id="thread-admin-orphan",
        name=None,
        preview="",
        cwd=str(config.admin_scratch_dir),
        created_at=2,
        updated_at=3,
        source_kind="appServer",
    )
    normal = ThreadSummary(
        thread_id="thread-normal",
        name="正常对话",
        preview="",
        cwd=str(tmp_path),
        created_at=1,
        updated_at=2,
        source_kind="cli",
    )
    service = BridgeService(
        config,
        db,
        StaticThreadsCodex([scratch, normal]),
        FakeGateway(),  # type: ignore[arg-type]
    )
    try:
        bindings = await service.reconcile_once()
        assert [item.thread_id for item in bindings] == ["thread-normal"]
        assert db.get_binding_by_thread("thread-admin-orphan") is None
    finally:
        db.close()


@pytest.mark.asyncio
async def test_approval_command_returns_exact_permissions_payload(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        owner_conversation_open_id="ou_conversation_owner",
    )
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway()
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    requested = {"network": {"enabled": True}, "fileSystem": {"read": ["/tmp"]}}
    codex.pending_requests["rpc-permissions"] = {"id": "rpc-permissions"}
    db.add_approval(
        PendingApproval(
            short_id="permit-1",
            rpc_id="rpc-permissions",
            method="item/permissions/requestApproval",
            thread_id="thread-1",
            turn_id="turn-1",
            chat_id="oc_thread",
            params={"permissions": requested},
        )
    )

    try:
        allow = incoming("om-allow", text="批准 permit-1")
        await service._route_incoming(stage(db, allow))

        assert codex.responses == [("rpc-permissions", {"permissions": requested, "scope": "turn"})]
        assert db.get_approval("permit-1", "oc_thread") is None
        assert db.inbox_state("om-allow") == "done"
        assert gateway.texts[-1]["text"] == "✅ 已允许一次。"
    finally:
        db.close()


def test_all_approval_result_payload_shapes() -> None:
    assert BridgeService._allow_payload("item/commandExecution/requestApproval", {}) == {
        "decision": "accept"
    }
    assert BridgeService._allow_payload("applyPatchApproval", {}) == {"decision": "approved"}
    assert BridgeService._deny_payload("item/fileChange/requestApproval", {}, cancel=False) == {
        "decision": "decline"
    }
    assert BridgeService._deny_payload("item/fileChange/requestApproval", {}, cancel=True) == {
        "decision": "cancel"
    }
    assert BridgeService._deny_payload(
        "item/permissions/requestApproval", {"permissions": {"network": True}}
    ) == {"permissions": {}, "scope": "turn"}
    assert BridgeService._answer_payload(
        {"questions": [{"id": "branch"}, {"id": "mode"}]},
        "branch=main; mode=safe",
    ) == {
        "answers": {
            "branch": {"answers": ["main"]},
            "mode": {"answers": ["safe"]},
        }
    }


@pytest.mark.asyncio
async def test_unknown_server_request_uses_protocol_error_not_fake_approval(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    try:
        await service._on_codex_request(
            {"id": 99, "method": "item/tool/call", "params": {"threadId": "t"}}
        )
        assert codex.responses == []
        assert codex.errors == [("99", -32601, "unsupported bridge server request: item/tool/call")]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_history_backfill_overlaps_and_dedupes_by_message_id(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        owner_conversation_open_id="ou_conversation_owner",
    )
    db = BridgeDB(config.database_path)
    codex = FakeCodex()
    gateway = FakeGateway(configured_roles={"conversation"})
    service = BridgeService(config, db, codex, gateway)  # type: ignore[arg-type]
    bind_thread(db)
    replayed = incoming("om-history", text="断线期间的消息")
    # Simulate both overlap inside one page and the same message being seen on
    # the next recovery scan.
    gateway.history = [replayed, replayed]

    try:
        await service._backfill_once()
        await service._backfill_once()

        assert db.inbox_counts() == {"pending": 1}
        assert len(gateway.history_calls) == 2
        assert all(call["role"] == "conversation" for call in gateway.history_calls)
        assert all(call["chat_id"] == "oc_thread" for call in gateway.history_calls)
        assert (
            gateway.history_calls[1]["start_time_seconds"]
            >= (gateway.history_calls[0]["start_time_seconds"])
        )
        claimed = db.claim_incoming("history-worker")
        assert claimed is not None
        assert claimed.message.message_id == "om-history"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_history_backfill_waits_for_adaptive_due_time(tmp_path: Path) -> None:
    config = make_config(
        tmp_path,
        owner_conversation_open_id="ou_conversation_owner",
    )
    db = BridgeDB(config.database_path)
    gateway = FakeGateway(configured_roles={"conversation"})
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    bind_thread(db)
    try:
        await service._backfill_once(force=True)
        await service._backfill_once(force=False)
        assert len(gateway.history_calls) == 1
        assert service._history_next_poll[("conversation", "oc_thread")] > 0
    finally:
        db.close()


def test_history_poll_slows_for_inactive_conversations(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    now = 2_000_000.0
    recent = incoming("recent", chat_id="recent", create_time_ms=int((now - 60) * 1000))
    warm = incoming("warm", chat_id="warm", create_time_ms=int((now - 7 * 3600) * 1000))
    idle = incoming("idle", chat_id="idle", create_time_ms=int((now - 2 * 86400) * 1000))
    for message in (recent, warm, idle):
        assert db.enqueue_incoming(message)
    try:
        assert service._history_poll_interval("conversation", "recent", now) == 600
        assert service._history_poll_interval("conversation", "warm", now) == 1800
        assert service._history_poll_interval("conversation", "idle", now) == 3600
        assert service._history_poll_interval("conversation", "never-seen", now) == 7200
    finally:
        db.close()


@pytest.mark.asyncio
async def test_external_sync_baselines_history_before_delivering_new_turns(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    old_turn = {
        "id": "turn-old",
        "status": "completed",
        "items": [
            {
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "不应补发的历史回复",
            }
        ],
    }
    codex = ThreadHistoryCodex([old_turn])
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    bind_thread(db)
    summary = ThreadSummary(
        thread_id="thread-1",
        name="受控对话",
        preview="",
        cwd="/workspace/test",
        created_at=1,
        updated_at=2,
        source_kind="cli",
    )

    try:
        await service._sync_external_updates([summary])

        assert db.is_turn_synced("thread-1", "turn-old") is True
        assert db.get_setting("external_sync_initialized:thread-1") == "1"
        assert db.outbox_counts() == {}

        codex.turns.append(
            {
                "id": "turn-new",
                "status": "inProgress",
                "items": [
                    {
                        "type": "userMessage",
                        "content": [{"type": "text", "text": "请执行新的任务"}],
                    },
                    {
                        "type": "agentMessage",
                        "phase": "commentary",
                        "text": "不应发送的思考过程",
                    },
                ],
            }
        )
        await service._sync_external_updates([summary])

        assert db.is_turn_synced("thread-1", "turn-new") is False
        assert db.outbox_counts() == {"pending": 1}
        prompt = db.claim_outbox("test-worker")
        assert prompt is not None
        assert "请执行新的任务" in prompt.content["text"]
        assert "思考过程" not in prompt.content["text"]
        db.complete_outbox(prompt.outbox_key)

        codex.turns[-1] = {
            "id": "turn-new",
            "status": "completed",
            "items": [
                {
                    "type": "userMessage",
                    "content": [{"type": "text", "text": "请执行新的任务"}],
                },
                {
                    "type": "agentMessage",
                    "phase": "commentary",
                    "text": "仍然不应发送的思考过程",
                },
                {
                    "type": "agentMessage",
                    "phase": "final_answer",
                    "text": "应当发送的新回复",
                },
            ],
        }
        await service._sync_external_updates([summary])

        assert db.is_turn_synced("thread-1", "turn-new") is False
        assert db.outbox_counts() == {"done": 1, "pending": 1}
        outbound = db.claim_outbox("test-worker")
        assert outbound is not None
        assert outbound.thread_id == "thread-1"
        assert outbound.turn_id == "turn-new"
        assert "应当发送的新回复" in outbound.content["text"]
        assert "思考过程" not in outbound.content["text"]
        db.complete_outbox(
            outbound.outbox_key,
            thread_id=outbound.thread_id,
            turn_id=outbound.turn_id,
        )
        assert db.is_turn_synced("thread-1", "turn-new") is True
    finally:
        db.close()


@pytest.mark.asyncio
async def test_external_sync_drops_commentary_when_turn_has_no_final(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    turn = {
        "id": "turn-interrupted",
        "status": "interrupted",
        "items": [
            {
                "type": "userMessage",
                "content": [{"type": "text", "text": "请执行任务"}],
            },
            {
                "type": "agentMessage",
                "phase": "commentary",
                "text": "绝不能发送这段思考",
            },
        ],
    }
    codex = ThreadHistoryCodex([turn])
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    bind_thread(db)
    db.set_setting("external_sync_initialized:thread-1", "1")
    summary = ThreadSummary(
        thread_id="thread-1",
        name="受控对话",
        preview="",
        cwd="/workspace/tester",
        created_at=1,
        updated_at=2,
        source_kind="cli",
    )

    try:
        await service._sync_external_updates([summary])

        assert db.is_turn_synced("thread-1", "turn-interrupted") is True
        assert db.outbox_counts() == {"pending": 1}
        prompt = db.claim_outbox("test-worker")
        assert prompt is not None
        assert "请执行任务" in prompt.content["text"]
        assert "思考" not in prompt.content["text"]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_external_sync_retries_completed_turn_until_final_is_persisted(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    turn = {
        "id": "turn-delayed-final",
        "status": "completed",
        "items": [
            {
                "type": "userMessage",
                "content": [{"type": "text", "text": "请执行任务"}],
            },
            {
                "type": "agentMessage",
                "phase": "commentary",
                "text": "不能作为最终回复",
            },
        ],
    }
    codex = ThreadHistoryCodex([turn])
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    bind_thread(db)
    db.set_setting("external_sync_initialized:thread-1", "1")
    summary = ThreadSummary(
        thread_id="thread-1",
        name="受控对话",
        preview="",
        cwd="/workspace/tester",
        created_at=1,
        updated_at=2,
        source_kind="cli",
    )

    try:
        await service._sync_external_updates([summary])

        assert db.is_turn_synced("thread-1", "turn-delayed-final") is False
        assert db.outbox_counts() == {"pending": 1}

        turn["items"].append(
            {
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "最终回复已经持久化",
            }
        )
        await service._sync_external_updates([summary])

        assert db.outbox_counts() == {"pending": 2}
        prompt = db.claim_outbox("test-worker")
        assert prompt is not None
        assert "请执行任务" in prompt.content["text"]
        db.complete_outbox(prompt.outbox_key)
        final = db.claim_outbox("test-worker")
        assert final is not None
        assert "最终回复已经持久化" in final.content["text"]
        assert "不能作为最终回复" not in final.content["text"]
    finally:
        db.close()


def test_automatic_file_delivery_only_accepts_dedicated_outbox(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    private = tmp_path / "private.txt"
    private.write_text("not for automatic upload", encoding="utf-8")
    deliverable = config.outbox_dir / "task" / "report.txt"
    deliverable.parent.mkdir(parents=True)
    deliverable.write_text("safe report", encoding="utf-8")
    try:
        text = f"[private]({private}) [report]({deliverable})"
        assert service.artifacts.outgoing_paths(text) == [deliverable]
    finally:
        db.close()


@pytest.mark.asyncio
async def test_small_outbox_file_is_queued_without_second_approval(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    deliverable = config.outbox_dir / "task" / "report.txt"
    deliverable.parent.mkdir(parents=True)
    deliverable.write_text("safe report", encoding="utf-8")
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-file",
        chat_id="oc_thread",
        final_text=f"已完成：[report]({deliverable})",
    )
    try:
        await service._finalize_turn(active, {"status": "completed", "items": []})

        assert db.outbox_counts() == {"pending": 2}
        text_item = db.claim_outbox("test-worker")
        assert text_item is not None
        assert text_item.msg_type == "text"
        db.complete_outbox(text_item.outbox_key)
        file_item = db.claim_outbox("test-worker")
        assert file_item is not None
        assert file_item.msg_type == "local_file"
        assert file_item.content["path"] == str(deliverable)
    finally:
        db.close()


@pytest.mark.asyncio
async def test_terminal_progress_card_cannot_be_overwritten_by_inflight_progress(
    tmp_path: Path,
) -> None:
    class DelayedPatchGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.running_started = asyncio.Event()
            self.release_running = asyncio.Event()
            self.patch_titles: list[str] = []

        async def patch_card(self, role: AppRole, message_id: str, card: dict[str, Any]) -> None:
            title = str(((card.get("header") or {}).get("title") or {}).get("content") or "")
            if "执行中" in title:
                self.running_started.set()
                await self.release_running.wait()
            self.patch_titles.append(title)

    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    gateway = DelayedPatchGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-progress-race",
        chat_id="oc_thread",
        progress_message_id="card-progress-race",
    )
    service._register_active(active)
    try:
        progress = asyncio.create_task(
            service._patch_active_progress(
                active,
                {"header": {"title": {"content": "Codex 执行中"}}},
                terminal=False,
            )
        )
        await gateway.running_started.wait()
        final = asyncio.create_task(
            service._finalize_turn(
                active,
                {
                    "status": "completed",
                    "items": [
                        {
                            "type": "agentMessage",
                            "phase": "final_answer",
                            "text": "完成",
                        }
                    ],
                },
            )
        )
        await asyncio.sleep(0)
        gateway.release_running.set()
        await asyncio.gather(progress, final)

        assert gateway.patch_titles[-1] == "Codex 已完成"
        assert (
            await service._patch_active_progress(
                active,
                {"header": {"title": {"content": "Codex 执行中"}}},
                terminal=False,
            )
            is False
        )
    finally:
        db.close()


@pytest.mark.asyncio
async def test_progress_patch_failure_backs_off_instead_of_hot_looping(
    tmp_path: Path,
) -> None:
    class FailingPatchGateway(FakeGateway):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def patch_card(self, role: AppRole, message_id: str, card: dict[str, Any]) -> None:
            self.attempts += 1
            raise RuntimeError("Feishu unavailable")

    config = make_config(tmp_path)
    config.progress_update_seconds = 0.01
    config.progress_steady_update_seconds = 0.01
    db = BridgeDB(config.database_path)
    gateway = FailingPatchGateway()
    service = BridgeService(config, db, FakeCodex(), gateway)  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-progress-backoff",
        chat_id="oc_thread",
        progress_message_id="card-progress-backoff",
        started_monotonic=1.0,
    )
    service._register_active(active)
    task = asyncio.create_task(service._progress_loop())
    try:
        await asyncio.sleep(0.3)
        assert gateway.attempts == 1
        assert active.progress_failures == 1
        assert active.progress_retry_monotonic > 0
    finally:
        service._stop.set()
        await asyncio.wait_for(task, timeout=1)
        db.close()


@pytest.mark.asyncio
async def test_stale_progress_audit_finalizes_missed_terminal_notification(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    config.progress_update_seconds = 0.01
    config.progress_steady_update_seconds = 0.01
    config.progress_stale_seconds = 0.01
    db = BridgeDB(config.database_path)
    turn = {
        "id": "turn-missed-while-connected",
        "status": "completed",
        "items": [
            {
                "type": "agentMessage",
                "phase": "final_answer",
                "text": "后台核对找回的最终答复",
            }
        ],
    }
    service = BridgeService(
        config,
        db,
        ThreadHistoryCodex([turn]),
        FakeGateway(),
    )  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-missed-while-connected",
        chat_id="oc_thread",
        progress_message_id="card-missed-while-connected",
        started_monotonic=time.monotonic() - 10,
    )
    service._register_active(active)
    task = asyncio.create_task(service._progress_loop())
    try:
        await asyncio.wait_for(service._turn_done[active.turn_id].wait(), timeout=1)
        assert active.turn_id in service._completed_turns
        assert service._active_by_turn == {}
        outbound = db.claim_outbox("stale-audit-test")
        assert outbound is not None
        assert outbound.content == {"text": "后台核对找回的最终答复"}
    finally:
        service._stop.set()
        await asyncio.wait_for(task, timeout=1)
        db.close()


@pytest.mark.asyncio
async def test_terminal_card_failure_enters_durable_outbox(
    tmp_path: Path,
) -> None:
    class FailingTerminalGateway(FakeGateway):
        async def patch_card(self, role: AppRole, message_id: str, card: dict[str, Any]) -> None:
            raise RuntimeError("temporary Feishu failure")

    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FailingTerminalGateway())  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-terminal-retry",
        chat_id="oc_thread",
        progress_message_id="card-terminal-retry",
        final_text="已完成",
    )
    service._register_active(active)
    try:
        await service._finalize_turn(active, {"status": "completed", "items": []})
        assert db.outbox_counts() == {"pending": 2}
        message_types: set[str] = set()
        for _ in range(2):
            item = db.claim_outbox("test-worker")
            assert item is not None
            message_types.add(item.msg_type)
            db.complete_outbox(item.outbox_key)
        assert message_types == {"card_patch", "text"}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_finalize_failure_keeps_turn_recoverable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-finalize-retry",
        chat_id="oc_thread",
        final_text="最终结果",
    )
    service._register_active(active)
    original = db.enqueue_outbox

    def fail_once(*_: Any, **__: Any) -> None:
        raise RuntimeError("simulated durable enqueue failure")

    try:
        monkeypatch.setattr(db, "enqueue_outbox", fail_once)
        await service._finalize_turn(active, {"status": "completed", "items": []})
        assert active.turn_id not in service._completed_turns
        assert service._active_by_turn[active.turn_id] is active
        assert service._turn_done[active.turn_id].is_set() is False

        monkeypatch.setattr(db, "enqueue_outbox", original)
        await service._finalize_turn(active, {"status": "completed", "items": []})
        assert active.turn_id in service._completed_turns
        assert active.turn_id not in service._active_by_turn
        assert service._turn_done[active.turn_id].is_set() is True
        assert db.outbox_counts() == {"pending": 1}
    finally:
        db.close()


@pytest.mark.asyncio
async def test_token_usage_records_telemetry_without_bridge_compaction(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    try:
        await service._on_codex_notification(
            {
                "method": "thread/tokenUsage/updated",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "tokenUsage": {
                        "last": {
                            "inputTokens": 170_000,
                            "cachedInputTokens": 120_000,
                        },
                        "modelContextWindow": 258_400,
                    },
                },
            }
        )

        assert db.get_setting("token_usage:thread-1:input") == "170000"
        assert db.get_setting("token_usage:thread-1:cached") == "120000"
        assert db.get_setting("token_usage:thread-1:window") == "258400"
        assert not hasattr(service, "_maybe_auto_compact")
        assert not hasattr(service.codex, "compact_thread")
    finally:
        db.close()


@pytest.mark.asyncio
async def test_official_context_compaction_progress_is_still_displayed(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-1",
        chat_id="oc_thread",
    )
    service._register_active(active)
    try:
        await service._on_codex_notification(
            {
                "method": "item/started",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": {"id": "context-1", "type": "contextCompaction"},
                },
            }
        )
        assert active.current_operation == "压缩长上下文"

        await service._on_codex_notification(
            {
                "method": "item/completed",
                "params": {
                    "threadId": "thread-1",
                    "turnId": "turn-1",
                    "item": {"id": "context-1", "type": "contextCompaction"},
                },
            }
        )
        assert active.current_operation == "上下文压缩完成"
    finally:
        db.close()


@pytest.mark.asyncio
async def test_status_and_turn_recovery_never_load_full_image_history(
    tmp_path: Path,
) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    codex = SummaryOnlyCodex()
    service = BridgeService(config, db, codex, FakeGateway())  # type: ignore[arg-type]
    try:
        turns = await service._turn_summaries("thread-1", max_turns=1)
        assert turns[0]["id"] == "turn-summary"
    finally:
        db.close()


def test_progress_text_is_stable_inside_heartbeat_bucket(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    active = ActiveTurn(
        thread_id="thread-1",
        turn_id="turn-1",
        chat_id="oc_thread",
        started_monotonic=1.0,
        last_event_monotonic=1.0,
    )
    try:
        first = service._render_progress(active)
        second = service._render_progress(active)
        assert first == second
    finally:
        db.close()


def test_progress_uses_five_seconds_then_thirty_seconds(tmp_path: Path) -> None:
    config = make_config(tmp_path)
    db = BridgeDB(config.database_path)
    service = BridgeService(config, db, FakeCodex(), FakeGateway())  # type: ignore[arg-type]
    now = 10_000.0
    early = ActiveTurn(
        thread_id="early", turn_id="early", chat_id="early", started_monotonic=now - 60
    )
    steady = ActiveTurn(
        thread_id="steady", turn_id="steady", chat_id="steady", started_monotonic=now - 121
    )
    try:
        assert service._progress_interval(early, now) == 5
        assert service._progress_interval(steady, now) == 30
    finally:
        db.close()
