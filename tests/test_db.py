from __future__ import annotations

from codex_feishu_bridge.db import BridgeDB
from codex_feishu_bridge.models import (
    Attachment,
    IncomingMessage,
    OutboxItem,
    PendingApproval,
    PendingArtifact,
    ThreadSummary,
    TurnJob,
)


def test_held_attachments_merge_atomically_into_next_text(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    staged = tmp_path / "inbox" / "image.png"
    staged.parent.mkdir()
    staged.write_bytes(b"png")
    media = IncomingMessage(
        message_id="om-media",
        chat_id="oc-chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou-owner",
        sender_user_id=None,
        sender_union_id="on-owner",
        text="",
        message_type="image",
        create_time_ms=1_000,
        attachments=[
            Attachment(
                kind="image",
                name="image.png",
                message_id="om-media",
                image_key="img-key",
                local_path=staged,
                size=3,
            )
        ],
    )
    text = IncomingMessage(
        message_id="om-text",
        chat_id="oc-chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou-owner",
        sender_user_id=None,
        sender_union_id="on-owner",
        text="分析这张图",
        message_type="text",
        create_time_ms=2_000,
    )
    try:
        assert db.enqueue_incoming(media) is True
        claimed_media = db.claim_incoming("worker")
        assert claimed_media is not None
        db.hold_incoming_attachments(claimed_media.message)
        assert db.inbox_state("om-media") == "held"

        assert db.enqueue_incoming(text) is True
        claimed_text = db.claim_incoming("worker")
        assert claimed_text is not None
        assert db.merge_held_attachments(claimed_text.message) == 1
        assert [item.message_id for item in claimed_text.message.attachments] == ["om-media"]
        assert db.inbox_state("om-media") == "done"

        # Simulate a crash before the text reaches Codex. The merged attachment
        # must survive on the retrying text row rather than being lost.
        assert db.recover_inbox_after_restart() == []
        recovered = db.claim_incoming("worker-2")
        assert recovered is not None
        assert recovered.message.message_id == "om-text"
        assert [item.message_id for item in recovered.message.attachments] == ["om-media"]
    finally:
        db.close()


def thread(
    thread_id: str = "thread-1",
    *,
    name: str | None = None,
    preview: str = "第一行预览\n第二行",
    updated_at: int = 20,
) -> ThreadSummary:
    return ThreadSummary(
        thread_id=thread_id,
        name=name,
        preview=preview,
        cwd="/workspace/test",
        created_at=10,
        updated_at=updated_at,
        source_kind="cli",
    )


def test_settings_and_message_deduplication(tmp_path):
    db = BridgeDB(tmp_path / "state" / "bridge.sqlite")
    try:
        assert db.get_setting("missing") is None
        assert db.get_setting("missing", "fallback") == "fallback"

        db.set_setting("owner", "ou_123")
        assert db.get_setting("owner") == "ou_123"

        db.set_runtime_config("thread-1", "model", "gpt-test", message_id="om-config")
        assert db.get_setting("runtime:thread-1:model") == "gpt-test"
        assert db.runtime_config_history("thread-1") == [
            {
                "name": "model",
                "old_value": None,
                "new_value": "gpt-test",
                "message_id": "om-config",
                "changed_at": db.runtime_config_history("thread-1")[0]["changed_at"],
            }
        ]

        assert db.claim_message("om_123") is True
        assert db.claim_message("om_123") is False
        assert db.claim_message("om_456") is True
    finally:
        db.close()


def test_binding_lifecycle_and_persistence(tmp_path):
    path = tmp_path / "bridge.sqlite"
    db = BridgeDB(path)
    try:
        binding = db.upsert_thread(thread())
        assert binding.title == "第一行预览"
        assert binding.chat_id is None
        assert binding.sync_state == "pending"
        assert db.list_bindings(pending_only=True) == [binding]

        # 首次登记的标题保持稳定，避免 preview/name 变化造成重复群或串路由。
        db.upsert_thread(thread(name="正式标题", updated_at=30))
        assert db.get_binding_by_thread("thread-1").title == "第一行预览"

        # 建群后保留飞书侧已采用的标题，避免每次轮询重命名。
        db.bind_chat("thread-1", "oc_chat", title="固定群名")
        db.upsert_thread(thread(name="再次改名", updated_at=40))
        bound = db.get_binding_by_chat("oc_chat")
        assert bound is not None
        assert bound.title == "固定群名"
        assert bound.thread_updated_at == 40
        assert bound.sync_state == "ready"

        db.mark_thread_seen(
            "thread-1",
            updated_at=41,
            turn_id="turn-9",
            message_hash="sha256:test",
        )
        seen = db.get_binding_by_thread("thread-1")
        assert seen is not None
        assert seen.thread_updated_at == 41
        assert seen.last_synced_turn_id == "turn-9"
        assert seen.last_synced_message_hash == "sha256:test"
    finally:
        db.close()

    reopened = BridgeDB(path)
    try:
        assert reopened.get_binding_by_chat("oc_chat").title == "固定群名"
    finally:
        reopened.close()


def test_pending_approval_is_scoped_to_chat(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    approval = PendingApproval(
        short_id="a1b2c3",
        rpc_id="777",
        method="item/commandExecution/requestApproval",
        thread_id="thread-1",
        turn_id="turn-1",
        chat_id="oc_expected",
        params={"command": "make test"},
    )
    try:
        db.add_approval(approval)
        assert db.get_approval("a1b2c3", "oc_other") is None
        loaded = db.get_approval("a1b2c3", "oc_expected")
        assert loaded == approval

        db.resolve_approval("a1b2c3", "declined")
        assert db.get_approval("a1b2c3", "oc_expected") is None
    finally:
        db.close()


def test_incoming_queue_is_durable_and_message_idempotent(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    first = IncomingMessage(
        message_id="om_first",
        chat_id="oc_chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou_owner",
        sender_user_id=None,
        sender_union_id="on_union",
        text="继续执行",
        message_type="text",
        create_time_ms=1000,
        tenant_key="tenant",
        app_id="cli_app",
    )
    second = IncomingMessage(
        message_id="om_second",
        chat_id="oc_chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou_owner",
        sender_user_id=None,
        sender_union_id=None,
        text="下一条",
        message_type="text",
        create_time_ms=2000,
    )
    try:
        assert db.enqueue_incoming(first) is True
        assert db.enqueue_incoming(first) is False
        assert db.enqueue_incoming(second) is True
        assert db.inbox_counts() == {"pending": 2}

        claimed = db.claim_incoming("worker-a")
        assert claimed is not None
        assert claimed.message == first
        assert claimed.attempts == 1
        db.mark_incoming_dispatching("om_first")

        claimed = db.claim_incoming("worker-a")
        assert claimed is not None
        assert claimed.message == second
        db.complete_incoming("om_second")

        # 跨 Codex dispatch 重启的消息必须标为模糊状态，不能自动重复执行。
        assert db.recover_inbox_after_restart() == ["om_first"]
        assert db.inbox_counts() == {"ambiguous": 1, "done": 1}
        assert db.claim_message("om_second") is False
    finally:
        db.close()


def test_thread_lease_excludes_other_worker(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    try:
        assert db.acquire_thread_lease("thread-1", "worker-a") is True
        assert db.acquire_thread_lease("thread-1", "worker-b") is False
        assert db.renew_thread_lease("thread-1", "worker-b") is False
        assert db.renew_thread_lease("thread-1", "worker-a") is True

        db.release_thread_lease("thread-1", "worker-a")
        assert db.acquire_thread_lease("thread-1", "worker-b") is True
    finally:
        db.close()


def test_queued_incoming_is_not_reclaimed_until_restart(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    message = IncomingMessage(
        message_id="om_queued",
        chat_id="oc_chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou_owner",
        sender_user_id=None,
        sender_union_id="on_union",
        text="等待外部线程空闲后继续",
        message_type="text",
        create_time_ms=1000,
    )
    try:
        assert db.enqueue_incoming(message) is True
        claimed = db.claim_incoming("worker-a", lease_seconds=0)
        assert claimed is not None
        db.mark_incoming_queued(message.message_id)

        # 即使原 processing 租约已经到期，内存 FIFO 中的工作也不能再次领取。
        assert db.claim_incoming("worker-b") is None
        assert db.inbox_state(message.message_id) == "queued"

        # 只有重启（内存 FIFO 已消失）才把 queued 安全恢复为 retry。
        assert db.recover_inbox_after_restart() == []
        recovered = db.claim_incoming("worker-b")
        assert recovered is not None
        assert recovered.message.message_id == message.message_id
        assert recovered.attempts == 2
    finally:
        db.close()


def test_outbox_is_durable_ordered_and_recovers_interrupted_send(tmp_path):
    path = tmp_path / "bridge.sqlite"
    db = BridgeDB(path)
    try:
        for sequence in (0, 1):
            db.enqueue_outbox(
                OutboxItem(
                    outbox_key=f"final:t1:{sequence}",
                    app_role="conversation",
                    receive_id="oc_chat",
                    receive_id_type="chat_id",
                    msg_type="text",
                    content={"text": str(sequence)},
                    group_key="final:t1",
                    sequence=sequence,
                    thread_id="thread-1" if sequence == 1 else None,
                    turn_id="turn-1" if sequence == 1 else None,
                )
            )
        first = db.claim_outbox("worker")
        assert first is not None and first.sequence == 0
        # The next chunk is blocked until the first one is durably done.
        assert db.claim_outbox("worker") is None
    finally:
        db.close()

    reopened = BridgeDB(path)
    try:
        assert reopened.recover_outbox_after_restart() == 1
        first = reopened.claim_outbox("worker-2")
        assert first is not None and first.sequence == 0
        reopened.complete_outbox(first.outbox_key)
        second = reopened.claim_outbox("worker-2")
        assert second is not None and second.sequence == 1
    finally:
        reopened.close()


def test_turn_job_survives_restart_until_delivery(tmp_path):
    path = tmp_path / "bridge.sqlite"
    db = BridgeDB(path)
    try:
        db.upsert_turn_job(
            TurnJob(
                message_id="om-1",
                thread_id="thread-1",
                turn_id="turn-1",
                app_role="conversation",
                chat_id="oc-chat",
                progress_message_id="om-progress",
                state="running",
            )
        )
    finally:
        db.close()
    reopened = BridgeDB(path)
    try:
        jobs = reopened.list_recoverable_turn_jobs()
        assert len(jobs) == 1 and jobs[0].turn_id == "turn-1"
        reopened.set_turn_job_state("turn-1", "delivered")
        assert reopened.list_recoverable_turn_jobs() == []
    finally:
        reopened.close()


def test_api_usage_is_aggregated_by_month_and_operation(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    try:
        db.record_api_attempt("conversation", "message.history.list")
        db.record_api_result("conversation", "message.history.list", success=True)
        db.record_api_attempt("conversation", "message.history.list")
        db.record_api_result("conversation", "message.history.list", success=False)
        db.record_api_attempt("conversation", "message.card.patch")

        assert db.api_usage() == {
            "message.card.patch": 1,
            "message.history.list": 2,
        }
    finally:
        db.close()


def test_artifact_requires_chat_scoped_second_approval(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    path = tmp_path / "outbox" / "report.pdf"
    path.parent.mkdir()
    path.write_bytes(b"pdf")
    artifact = PendingArtifact(
        approval_id="artifact-token",
        thread_id="thread-1",
        turn_id="turn-1",
        app_role="conversation",
        chat_id="oc-expected",
        path=path,
        sha256="abc",
        size=3,
    )
    try:
        db.add_artifact_approval(artifact)
        assert db.get_artifact_approval("artifact-token", "oc-other") is None
        assert db.get_artifact_approval("artifact-token", "oc-expected") == artifact
        assert db.resolve_artifact_approval("artifact-token", "approved") is True
        assert db.resolve_artifact_approval("artifact-token", "approved") is False
    finally:
        db.close()


def test_retention_prunes_only_terminal_private_payloads(tmp_path):
    db = BridgeDB(tmp_path / "bridge.sqlite")
    done = IncomingMessage(
        message_id="om-expired",
        chat_id="oc-chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou-owner",
        sender_user_id="user-owner",
        sender_union_id="union-owner",
        text="private message body",
        message_type="text",
        create_time_ms=1,
    )
    pending = IncomingMessage(
        message_id="om-pending",
        chat_id="oc-chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou-owner",
        sender_user_id="user-owner",
        sender_union_id="union-owner",
        text="still needed",
        message_type="text",
        create_time_ms=2,
    )
    try:
        assert db.enqueue_incoming(done)
        assert db.claim_incoming("worker") is not None
        db.complete_incoming(done.message_id)
        assert db.enqueue_incoming(pending)
        with db._lock:
            db._conn.execute(
                "UPDATE inbox_messages SET updated_at=1 WHERE message_id=?",
                (done.message_id,),
            )
            db._conn.execute(
                "UPDATE inbox_messages SET updated_at=1 WHERE message_id=?",
                (pending.message_id,),
            )
            db._conn.commit()

        removed = db.prune_retained_data(30, now=4_000_000)

        assert removed["inbox_messages"] == 1
        assert db.inbox_state(done.message_id) is None
        assert db.inbox_state(pending.message_id) == "pending"
    finally:
        db.close()
