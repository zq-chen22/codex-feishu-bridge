from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import logging
import re
import secrets
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .artifacts import ArtifactBroker
from .codex_client import (
    CodexAppServer,
    CodexRPCError,
    extract_agent_messages,
    extract_user_messages,
)
from .config import BridgeConfig
from .db import BridgeDB
from .feishu import FeishuGateway, progress_card
from .models import (
    ActiveTurn,
    AppRole,
    Binding,
    InboxItem,
    IncomingMessage,
    OutboxItem,
    PendingApproval,
    PendingArtifact,
    ThreadSummary,
    TurnJob,
)
from .privacy import log_ref as _log_ref
from .privacy import redact_log
from .visual_proxy import build_codex_hook_config

LOG = logging.getLogger(__name__)
SUPPORTED_SETTINGS_CLI_VERSION = "0.144.1"
RUNTIME_MODEL_DEFAULT = "__model_default__"
RUNTIME_SERVICE_TIER_OFF = "__off__"
PAIR_RE = re.compile(r"^(?:配对|pair)\s+([A-Z2-9-]{8,20})$", re.IGNORECASE)
NEW_THREAD_RE = re.compile(r"^(?:新对话|创建新对话|新建对话|/new)\s+(.+)$", re.IGNORECASE)


@dataclass(slots=True)
class ScheduledMessage:
    inbox: InboxItem
    binding: Binding | None
    progress_message_id: str
    app_role: AppRole
    chat_id: str


@dataclass(slots=True)
class RuntimeSettings:
    model: str | None
    effort: str | None
    service_tier: str | None
    approval_policy: str
    sandbox: str


def generate_pairing_code(db: BridgeDB, ttl_seconds: int) -> tuple[str, int]:
    alphabet = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"  # pragma: allowlist secret
    raw = "".join(secrets.choice(alphabet) for _ in range(16))
    code = f"{raw[:8]}-{raw[8:]}"
    expires_at = int(time.time()) + ttl_seconds
    db.set_setting("pairing_code_hash", hashlib.sha256(code.encode()).hexdigest())
    db.set_setting("pairing_code_expires_at", str(expires_at))
    return code, expires_at


class BridgeService:
    def __init__(
        self,
        config: BridgeConfig,
        db: BridgeDB,
        codex: CodexAppServer,
        gateway: FeishuGateway,
    ) -> None:
        self.config = config
        self.db = db
        self.codex = codex
        self.gateway = gateway
        self.artifacts = ArtifactBroker(config, gateway)
        configure_threads = getattr(self.codex, "configure_thread_defaults", None)
        if callable(configure_threads):
            configure_threads(
                config_overrides=build_codex_hook_config(
                    config.visual_proxy_dir,
                    max_edge=config.image_proxy_max_edge,
                    quality=config.image_proxy_jpeg_quality,
                )
            )
        self.worker_id = f"{uuid.uuid4()}"
        self._stop = asyncio.Event()
        self._draining = False
        self.fatal_error: BaseException | None = None
        self._chat_description_retry_at: dict[str, float] = {}
        self._tasks: list[asyncio.Task[Any]] = []
        self._background_tasks: set[asyncio.Task[Any]] = set()
        self._thread_queues: dict[str, asyncio.Queue[ScheduledMessage]] = {}
        self._thread_workers: dict[str, asyncio.Task[None]] = {}
        self._admin_queue: asyncio.Queue[ScheduledMessage] = asyncio.Queue()
        self._active_by_thread: dict[str, ActiveTurn] = {}
        self._active_by_turn: dict[str, ActiveTurn] = {}
        self._turn_done: dict[str, asyncio.Event] = {}
        self._pending_jobs: dict[str, ScheduledMessage] = {}
        self._pending_recoveries: dict[str, TurnJob] = {}
        self._history_next_poll: dict[tuple[AppRole, str], float] = {}
        self._completed_turns: set[str] = set()
        self._finalizing: set[str] = set()
        self._progress_locks: dict[str, asyncio.Lock] = {}
        self._terminal_progress_messages: set[str] = set()
        self._auditing_turns: set[str] = set()
        self._active_audit_next: dict[str, float] = {}
        self._warned: set[str] = set()
        self._runtime_compatibility_error: str | None = None
        self.codex.add_notification_handler(self._on_codex_notification)
        self.codex.set_server_request_handler(self._on_codex_request)

    async def start(self) -> None:
        ambiguous = self.db.recover_inbox_after_restart()
        recovered_outbox = self.db.recover_outbox_after_restart()
        await self.codex.start()
        await self._probe_runtime_settings_compatibility()
        await self._recover_turn_jobs(startup=True)
        self.gateway.start_receivers()
        await self.reconcile_once()
        try:
            await self._backfill_once()
        except Exception:
            LOG.exception("Initial Feishu history recovery failed")
        self._tasks = [
            self._critical_task(self._inbox_loop(), "feishu-inbox"),
            self._critical_task(self._reconcile_loop(), "thread-reconciler"),
            self._critical_task(self._progress_loop(), "progress-projector"),
            self._critical_task(self._admin_worker(), "admin-worker"),
            self._critical_task(self._history_loop(), "feishu-history-recovery"),
            self._critical_task(self._codex_watch_loop(), "codex-app-server-watch"),
            self._critical_task(self._receiver_watch_loop(), "feishu-ws-watch"),
            self._critical_task(self._outbox_loop(), "feishu-outbox"),
            self._critical_task(self._turn_recovery_loop(), "turn-job-recovery"),
            self._critical_task(self._retention_loop(), "privacy-retention"),
        ]
        if ambiguous:
            await self._notify_admin(
                "⚠️ 服务上次在调用 Codex 的临界区重启。为避免重复执行，"
                f"有 {len(ambiguous)} 条消息已标记为“状态待确认”，不会自动重放。"
            )
        if recovered_outbox:
            LOG.warning("Recovered %d interrupted Feishu outbox send(s)", recovered_outbox)

    async def run_forever(self) -> None:
        await self.start()
        await self._stop.wait()

    async def wait_stopped(self) -> None:
        await self._stop.wait()

    async def stop(self) -> None:
        self._stop.set()
        for task in [*self._tasks, *self._thread_workers.values(), *self._background_tasks]:
            task.cancel()
        for task in [*self._tasks, *self._thread_workers.values(), *self._background_tasks]:
            with contextlib.suppress(asyncio.CancelledError, Exception):
                await task
        self._tasks.clear()
        self._thread_workers.clear()
        self._background_tasks.clear()
        self.gateway.stop_receivers()
        await self.codex.close()

    def begin_drain(self) -> None:
        """Stop claiming new phone messages while already accepted work finishes."""

        self._draining = True

    def _drain_complete(self) -> bool:
        queued = any(not queue.empty() for queue in self._thread_queues.values())
        outbox = self.db.outbox_counts()
        outbound_pending = any(
            count for state, count in outbox.items() if state not in {"done", "dead"}
        )
        return not (
            self._active_by_turn
            or self._pending_jobs
            or self._pending_recoveries
            or self._finalizing
            or queued
            or not self._admin_queue.empty()
            or outbound_pending
        )

    async def wait_for_drain(self, timeout_seconds: float) -> bool:
        self.begin_drain()
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        while not self._drain_complete():
            if time.monotonic() >= deadline:
                return False
            await asyncio.sleep(0.25)
        return True

    def _critical_task(self, awaitable: Any, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(awaitable, name=name)

        def completed(value: asyncio.Task[Any]) -> None:
            if value.cancelled() or self._stop.is_set():
                return
            error = value.exception()
            if error is None:
                error = RuntimeError(f"critical worker {name} exited unexpectedly")
            self.fatal_error = error
            LOG.critical("Critical bridge worker %s stopped: %s", name, redact_log(error))
            self._stop.set()

        task.add_done_callback(completed)
        return task

    def _background_task(self, awaitable: Any, name: str) -> asyncio.Task[Any]:
        task = asyncio.create_task(awaitable, name=name)
        self._background_tasks.add(task)

        def completed(value: asyncio.Task[Any]) -> None:
            self._background_tasks.discard(value)
            if value.cancelled():
                return
            error = value.exception()
            if error:
                LOG.error(
                    "Background bridge task %s failed: %s",
                    name,
                    redact_log(error),
                    exc_info=(type(error), error, error.__traceback__),
                )

        task.add_done_callback(completed)
        return task

    async def reconcile_once(self) -> list[Binding]:
        threads = await self.codex.list_threads(
            limit=max(100, self.config.initial_thread_count),
            source_kinds=self.config.source_kinds,
            sort_key="recency_at",
        )
        threads = [
            thread
            for thread in threads
            if self.db.get_setting(f"exclude_thread:{thread.thread_id}", "0") != "1"
            and not (thread.name or "").startswith("飞行桥临时任务-")
            and not self._is_admin_scratch_thread(thread)
        ]
        recent = threads[: self.config.initial_thread_count]
        bindings = [self.db.upsert_thread(thread) for thread in recent]
        summaries = {thread.thread_id: thread for thread in threads}
        for binding in self.db.list_bindings():
            summary = summaries.get(binding.thread_id)
            if summary:
                self.db.refresh_thread_metadata(summary)
        owner = self._owner("conversation")
        if self.gateway.configured("conversation") and owner:
            for binding in self.db.list_bindings(pending_only=True):
                try:
                    await self._create_binding_chat(binding, owner)
                except Exception as error:
                    LOG.exception(
                        "Failed creating Feishu chat for thread ref=%s",
                        _log_ref(binding.thread_id),
                    )
                    self.db.set_binding_error(binding.thread_id, str(error))
            for binding in self.db.list_bindings():
                if not binding.chat_id:
                    continue
                if self._chat_description_retry_at.get(binding.thread_id, 0) > time.time():
                    continue
                try:
                    await self._sync_chat_description(binding)
                    self._chat_description_retry_at.pop(binding.thread_id, None)
                except Exception as error:
                    self._chat_description_retry_at[binding.thread_id] = time.time() + 300
                    LOG.warning(
                        "Failed updating Feishu chat description for thread ref=%s; "
                        "retrying in 5 minutes: %s",
                        _log_ref(binding.thread_id),
                        redact_log(error),
                    )
        await self._sync_external_updates(threads)
        return [self.db.get_binding_by_thread(item.thread_id) or item for item in bindings]

    async def create_new_conversation(
        self,
        title: str,
        cwd: Path | None = None,
        *,
        inbox_message_id: str | None = None,
    ) -> Binding:
        if cwd is None:
            slug = re.sub(r"[^A-Za-z0-9\u4e00-\u9fff_-]+", "-", title.strip()).strip("-")
            target = self.config.managed_workspaces_dir / (
                f"{slug[:40] or 'codex'}-{secrets.token_hex(4)}"
            )
            target.mkdir(parents=True, exist_ok=False, mode=0o700)
            real_cwd = target.resolve()
        else:
            real_cwd = cwd.expanduser().resolve(strict=True)
        if not real_cwd.is_dir() or not self._allowed_workspace(real_cwd):
            raise ValueError("工作目录不存在，或不在 allowed_workspace_roots 内")
        if inbox_message_id:
            # thread/start has no idempotency key, so persist the ambiguity
            # boundary only after all local validation and immediately before
            # the RPC.
            self.db.mark_incoming_dispatching(inbox_message_id)
        thread = await self.codex.start_thread(
            cwd=str(real_cwd),
            approval_policy=self.config.approval_policy,
            sandbox=self.config.sandbox,
            model=self.config.model,
            service_tier=self.config.service_tier,
            ephemeral=False,
        )
        thread_id = str(thread["id"])
        clean_title = title.strip()[:80] or f"Codex-{thread_id[:8]}"
        await self.codex.set_thread_name(thread_id, clean_title)
        summary = ThreadSummary(
            thread_id=thread_id,
            name=clean_title,
            preview="",
            cwd=str(real_cwd),
            created_at=int(thread.get("createdAt") or time.time()),
            updated_at=int(thread.get("updatedAt") or time.time()),
            source_kind="appServer",
        )
        binding = self.db.upsert_thread(summary, title=clean_title)
        owner = self._owner("conversation")
        if self.gateway.configured("conversation") and owner:
            await self._create_binding_chat(binding, owner)
            binding = self.db.get_binding_by_thread(thread_id) or binding
        return binding

    async def _turn_summaries(
        self,
        thread_id: str,
        *,
        items_view: str = "summary",
        max_turns: int = 500,
    ) -> list[dict[str, Any]]:
        """Read bounded, image-free turn pages newest first.

        ``thread/read(includeTurns=True)`` can serialize every persisted image
        into one JSON line. A handful of generated images is enough to block
        or exceed the app-server stream limit, so polling and recovery must use
        the paginated summary API instead.
        """

        turns: list[dict[str, Any]] = []
        cursor: str | None = None
        while len(turns) < max_turns:
            page = await self.codex.list_turns(
                thread_id,
                limit=min(100, max_turns - len(turns)),
                items_view=items_view,
                sort_direction="desc",
                cursor=cursor,
            )
            batch = list(page.get("data") or [])
            turns.extend(batch)
            cursor = page.get("nextCursor")
            if not cursor or not batch:
                break
        return turns

    async def _find_turn_summary(self, thread_id: str, turn_id: str) -> dict[str, Any] | None:
        turns = await self._turn_summaries(thread_id, max_turns=100)
        return next(
            (turn for turn in turns if str(turn.get("id") or "") == turn_id),
            None,
        )

    async def _create_binding_chat(self, binding: Binding, owner: str) -> None:
        recovered = await self.gateway.find_conversation_chat(binding.thread_id, owner)
        if recovered:
            chat_id, actual_name = recovered
        else:
            chat_id, actual_name = await self.gateway.create_conversation_chat(
                binding.thread_id,
                binding.title,
                owner,
                binding.cwd,
                binding.thread_created_at,
            )
        self.db.bind_chat(
            binding.thread_id, chat_id, actual_name.removesuffix(self.config.group_suffix)
        )
        with contextlib.suppress(Exception):
            turns = await self._turn_summaries(binding.thread_id)
            self._baseline_external_sync(binding.thread_id, turns)
        await self.gateway.send_card(
            "conversation",
            chat_id,
            progress_card(
                "Codex 对话已绑定",
                f"**对话：** {binding.title}\n\n"
                f"**工作目录：** `{binding.cwd}`\n\n"
                f"**线程：** `{binding.thread_id}`\n\n"
                "直接发消息会在这个 Codex 上下文中开启下一轮；执行中用 "
                "`!steer 补充要求` 立即修正，用 `!stop` 停止，用 `!status` 查看状态，"
                "用 `!帮助` 查看模型、速度与权限配置命令。",
                color="green",
            ),
            idempotency_key=f"welcome:{binding.thread_id}",
        )

    async def _sync_chat_description(self, binding: Binding) -> None:
        if not binding.chat_id:
            return
        if binding.thread_created_at <= 0:
            raw = await self.codex.read_thread(binding.thread_id, include_turns=False)
            summary = ThreadSummary(
                thread_id=binding.thread_id,
                name=raw.get("name"),
                preview=str(raw.get("preview") or ""),
                cwd=str(raw.get("cwd") or binding.cwd),
                created_at=int(raw.get("createdAt") or 0),
                updated_at=int(
                    raw.get("recencyAt")
                    or raw.get("updatedAt")
                    or raw.get("createdAt")
                    or binding.thread_updated_at
                ),
            )
            binding = self.db.refresh_thread_metadata(summary) or binding
        signature = hashlib.sha256(
            f"v2\0{binding.chat_id}\0{binding.cwd}\0{binding.thread_created_at}".encode()
        ).hexdigest()
        setting = f"chat_description_hash:{binding.thread_id}"
        if self.db.get_setting(setting, "") == signature:
            return
        await self.gateway.update_conversation_chat_description(
            binding.chat_id,
            binding.thread_id,
            binding.cwd,
            binding.thread_created_at,
        )
        self.db.set_setting(setting, signature)

    async def _sync_external_updates(self, recent: list[ThreadSummary]) -> None:
        for thread in recent:
            binding = self.db.get_binding_by_thread(thread.thread_id)
            if not binding or not binding.chat_id or thread.thread_id in self._active_by_thread:
                continue
            try:
                turns = await self._turn_summaries(thread.thread_id)
                if self.db.get_setting(self._external_sync_key(thread.thread_id), "0") != "1":
                    self._baseline_external_sync(thread.thread_id, turns)
                    continue
                for turn in reversed(turns):
                    turn_id = str(turn.get("id") or "")
                    if (
                        not turn_id
                        or self.db.is_bridge_turn(turn_id)
                        or self.db.is_turn_synced(thread.thread_id, turn_id)
                    ):
                        continue

                    prompt_key = self._external_prompt_sync_key(thread.thread_id, turn_id)
                    if self.db.get_setting(prompt_key, "0") != "1":
                        user_messages = extract_user_messages(turn)
                        if user_messages:
                            self._enqueue_outbound_result(
                                app_role="conversation",
                                chat_id=binding.chat_id,
                                text=(
                                    "🖥️ 本机 Codex 窗口中的用户提问：\n\n"
                                    + _redact(user_messages[-1])
                                ),
                                base_key=f"external-0-user:{thread.thread_id}:{turn_id}",
                                thread_id=None,
                                turn_id=None,
                            )
                            self.db.set_setting(prompt_key, "1")

                    status = str(turn.get("status") or "")
                    if status not in {"completed", "failed", "interrupted"}:
                        continue
                    if status != "completed":
                        self.db.mark_turn_synced(thread.thread_id, turn_id)
                        continue
                    _, final_messages = extract_agent_messages(turn)
                    if not final_messages:
                        continue
                    final = final_messages[-1]
                    self._enqueue_outbound_result(
                        app_role="conversation",
                        chat_id=binding.chat_id,
                        text="✅ 本机 Codex 窗口已完成：\n\n" + _redact(final),
                        base_key=f"external-1-final:{thread.thread_id}:{turn_id}",
                        thread_id=thread.thread_id,
                        turn_id=turn_id,
                    )
            except Exception:
                LOG.exception(
                    "Failed syncing external update for thread ref=%s",
                    _log_ref(thread.thread_id),
                )

    @staticmethod
    def _external_sync_key(thread_id: str) -> str:
        return f"external_sync_initialized:{thread_id}"

    @staticmethod
    def _external_prompt_sync_key(thread_id: str, turn_id: str) -> str:
        return f"external_prompt_synced:{thread_id}:{turn_id}"

    def _baseline_external_sync(self, thread_id: str, turns: list[dict[str, Any]]) -> None:
        """Record the current terminal turns without delivering historical replies.

        The initialization marker is written last.  If the process stops while
        establishing the baseline, the next reconciliation repeats this safe,
        idempotent operation instead of treating partial history as new output.
        """

        for turn in turns:
            turn_id = str(turn.get("id") or "")
            if not turn_id:
                continue
            self.db.set_setting(self._external_prompt_sync_key(thread_id, turn_id), "1")
            if turn.get("status") in {"completed", "failed", "interrupted"}:
                self.db.mark_turn_synced(thread_id, turn_id)
        self.db.set_setting(self._external_sync_key(thread_id), "1")

    async def _reconcile_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(self.config.sync_interval_seconds)
            if self._draining:
                continue
            try:
                await self.reconcile_once()
            except Exception:
                LOG.exception("Thread reconciliation failed")

    async def _codex_watch_loop(self) -> None:
        while not self._stop.is_set():
            await self.codex.wait_closed()
            if self._stop.is_set():
                return
            await self._notify_admin(
                "⚠️ Codex App Server 连接意外关闭，桥接器正在本机自动重启并核对活动任务。"
            )
            delay = 1
            while not self._stop.is_set():
                try:
                    await self.codex.close()
                    await self.codex.start()
                    break
                except Exception:
                    LOG.exception("Could not restart Codex app-server")
                    await asyncio.sleep(delay)
                    delay = min(30, delay * 2)
            if self._stop.is_set():
                return
            for active in list(self._active_by_turn.values()):
                try:
                    turn = await self._find_turn_summary(active.thread_id, active.turn_id)
                    if turn and turn.get("status") in {"completed", "failed", "interrupted"}:
                        await self._finalize_turn(active, turn)
                    else:
                        await self._mark_lost_turn(active)
                except Exception:
                    LOG.exception(
                        "Could not recover active turn ref=%s",
                        _log_ref(active.turn_id),
                    )
                    await self._mark_lost_turn(active)

    async def _recover_turn_jobs(self, *, startup: bool = False) -> None:
        for job in self.db.list_recoverable_turn_jobs():
            known_active = self._active_by_turn.get(job.turn_id)
            age = max(0, int(time.time()) - job.created_at) if job.created_at else 0
            active = known_active or ActiveTurn(
                thread_id=job.thread_id,
                turn_id=job.turn_id,
                chat_id=job.chat_id,
                app_role=job.app_role,
                progress_message_id=job.progress_message_id,
                started_monotonic=time.monotonic() - age,
            )
            try:
                turn = await self._find_turn_summary(job.thread_id, job.turn_id)
                done = self._turn_done.get(job.turn_id)
                if job.turn_id in self._completed_turns or (done and done.is_set()):
                    continue
                known_active = self._active_by_turn.get(job.turn_id)
                if known_active is not None:
                    active = known_active
                if turn and turn.get("status") in {"completed", "failed", "interrupted"}:
                    commentary_messages, final_messages = extract_agent_messages(turn)
                    if startup and turn.get("status") == "interrupted" and not final_messages:
                        await self._continue_interrupted_turn(job)
                        continue
                    if known_active is None:
                        self._register_active(active)
                    await self._finalize_turn(active, turn)
                elif known_active is None:
                    await self._mark_lost_turn(active)
            except Exception:
                LOG.exception(
                    "Could not recover persisted turn job ref=%s",
                    _log_ref(job.turn_id),
                )
                done = self._turn_done.get(job.turn_id)
                if job.turn_id in self._completed_turns or (done and done.is_set()):
                    continue
                if self._active_by_turn.get(job.turn_id) is None:
                    await self._mark_lost_turn(active)

    async def _continue_interrupted_turn(self, job: TurnJob) -> None:
        """Continue bridge-owned work interrupted by an App Server restart.

        The continuation inspects the existing thread/workspace instead of
        replaying the original user text, so completed side effects are not
        blindly repeated and the phone never receives an echo of its prompt.
        """

        old_turn_id = job.turn_id
        self.db.mark_turn_synced(job.thread_id, old_turn_id)
        self._pending_recoveries[job.thread_id] = job
        if job.progress_message_id:
            with contextlib.suppress(Exception):
                await self.gateway.patch_card(
                    job.app_role,
                    job.progress_message_id,
                    progress_card(
                        "Codex 正在恢复",
                        "桥服务更新打断了上一执行进程；已保留原对话和工作目录，"
                        "正在检查现场并从未完成处继续。",
                        color="orange",
                    ),
                )
        runtime = self._runtime_settings(job.thread_id)
        recovery_id = str(uuid.uuid5(uuid.NAMESPACE_URL, f"codex-feishu-recovery:{old_turn_id}"))
        instruction = (
            "系统恢复指令：飞书桥在上一轮执行过程中重启，上一轮因此被基础设施中断。"
            "请根据本对话中紧邻的上一条用户要求和当前工作目录中的已有现场继续完成任务。"
            "先检查已经产生的文件、进程与结果，避免重复已完成的副作用；"
            "不要复述用户原文，不要只报告中断。完成后给出正常的最终汇报。"
        )
        try:
            turn = await self.codex.start_turn(
                job.thread_id,
                [{"type": "text", "text": instruction, "text_elements": []}],
                client_message_id=recovery_id,
                approval_policy=runtime.approval_policy,
                sandbox=runtime.sandbox,
                model=runtime.model,
                effort=runtime.effort,
                service_tier=runtime.service_tier,
            )
            turn_id = str(turn.get("id") or "")
            if not turn_id:
                raise RuntimeError("recovery turn/start returned no id")
            if turn_id in self._completed_turns:
                return
            self.db.upsert_turn_job(
                TurnJob(
                    message_id=job.message_id,
                    thread_id=job.thread_id,
                    turn_id=turn_id,
                    app_role=job.app_role,
                    chat_id=job.chat_id,
                    progress_message_id=job.progress_message_id,
                    state="accepted",
                )
            )
            if turn_id not in self._active_by_turn:
                self._register_active(
                    ActiveTurn(
                        thread_id=job.thread_id,
                        turn_id=turn_id,
                        chat_id=job.chat_id,
                        app_role=job.app_role,
                        progress_message_id=job.progress_message_id,
                        started_monotonic=time.monotonic(),
                    )
                )
        except Exception:
            LOG.exception("Could not continue interrupted turn ref=%s", _log_ref(old_turn_id))
            active = ActiveTurn(
                thread_id=job.thread_id,
                turn_id=old_turn_id,
                chat_id=job.chat_id,
                app_role=job.app_role,
                progress_message_id=job.progress_message_id,
                started_monotonic=time.monotonic(),
            )
            self._register_active(active)
            await self._finalize_turn(
                active,
                {
                    "id": old_turn_id,
                    "status": "interrupted",
                    "items": [],
                    "error": {"message": "桥服务重启后的自动续做未能启动"},
                },
            )
        finally:
            self._pending_recoveries.pop(job.thread_id, None)

    async def _turn_recovery_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(60)
            await self._recover_turn_jobs(startup=False)

    async def _audit_stale_active_turn(self, active: ActiveTurn) -> None:
        """Recover a terminal event lost while the App Server stayed connected."""

        turn_id = active.turn_id
        if turn_id in self._auditing_turns:
            return
        self._auditing_turns.add(turn_id)
        try:
            if self._active_by_turn.get(turn_id) is not active:
                return
            turn = await self._find_turn_summary(active.thread_id, turn_id)
            if turn and turn.get("status") in {"completed", "failed", "interrupted"}:
                await self._finalize_turn(active, turn)
        except Exception:
            LOG.warning(
                "Could not audit stale active turn ref=%s; will retry",
                _log_ref(turn_id),
                exc_info=True,
            )
        finally:
            self._auditing_turns.discard(turn_id)

    async def _mark_lost_turn(self, active: ActiveTurn) -> None:
        self.db.set_turn_job_state(active.turn_id, "ambiguous")
        self.db.set_setting(f"blocked_thread:{active.thread_id}", active.turn_id)
        text = (
            "⚠️ 桥接器与 Codex App Server 的进程边界中断，无法证明原 turn 是否仍在执行。"
            "为避免重复副作用，该任务不会自动恢复或重放，并已锁定此 thread 的后续启动。\n\n"
            f"thread：`{active.thread_id}`\nturn：`{active.turn_id}`\n\n"
            f"请先在本机核对，再到 Codex 机器人私聊发送 `解除线程 {active.thread_id}`；"
            "如仍需原任务，请解除后在原对话重新发送。"
        )
        self._enqueue_outbound_result(
            app_role=active.app_role,
            chat_id=active.chat_id,
            text=text,
            base_key=f"lost-turn:{active.turn_id}",
            thread_id=None,
            turn_id=None,
        )
        if active.progress_message_id:
            with contextlib.suppress(Exception):
                await self.gateway.patch_card(
                    active.app_role,
                    active.progress_message_id,
                    progress_card("Codex 状态待核对", text, color="red"),
                )
        self._turn_done.setdefault(active.turn_id, asyncio.Event()).set()
        self._active_by_turn.pop(active.turn_id, None)
        if self._active_by_thread.get(active.thread_id) is active:
            self._active_by_thread.pop(active.thread_id, None)

    async def _receiver_watch_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(10)
            restarted = self.gateway.ensure_receivers()
            if restarted:
                LOG.warning("Restarted Feishu WS receiver(s): %s", ", ".join(restarted))
                with contextlib.suppress(Exception):
                    await self._backfill_once(force=True)

    async def _retention_loop(self) -> None:
        while not self._stop.is_set():
            try:
                removed = self.db.prune_retained_data(self.config.data_retention_days)
                total = sum(removed.values())
                if total:
                    LOG.info("Pruned %d expired private operational record(s)", total)
            except Exception as error:
                LOG.warning("Privacy retention cleanup failed: %s", redact_log(error))
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=24 * 3600)
            except TimeoutError:
                continue

    async def _inbox_loop(self) -> None:
        while not self._stop.is_set():
            if self._draining:
                await asyncio.sleep(0.25)
                continue
            item = self.db.claim_incoming(self.worker_id, lease_seconds=24 * 3600)
            if not item:
                await asyncio.sleep(0.25)
                continue
            try:
                await self._route_incoming(item)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.exception(
                    "Incoming message failed ref=%s",
                    _log_ref(item.message.message_id),
                )
                self._record_incoming_failure(item, error)

    def _record_incoming_failure(self, item: InboxItem, error: BaseException) -> None:
        message = item.message
        state = self.db.inbox_state(message.message_id)
        if state in {"dispatching", "ambiguous"}:
            # The first non-idempotent Codex RPC may already have succeeded.
            # Automatic replay could create a second thread/turn.
            if state == "dispatching":
                self.db.mark_incoming_ambiguous(message.message_id, str(error))
            self._enqueue_outbound_result(
                app_role=message.app_role,
                chat_id=message.chat_id,
                text=(
                    "⚠️ 调用 Codex 时连接中断，结果状态无法确认。为避免重复执行，"
                    f"消息 `{message.message_id}` 没有自动重放；"
                    "请在 Codex 机器人私聊发送 `待确认`。"
                ),
                base_key=f"inbox-ambiguous:{message.message_id}",
                thread_id=None,
                turn_id=None,
            )
            return
        dead = item.attempts >= 8
        self.db.fail_incoming(
            message.message_id,
            str(error),
            retry_after_seconds=min(300, 2 ** min(item.attempts, 8)),
            dead=dead,
        )

    async def _outbox_loop(self) -> None:
        while not self._stop.is_set():
            item = self.db.claim_outbox(self.worker_id, lease_seconds=120)
            if not item:
                await asyncio.sleep(0.25)
                continue
            try:
                if item.msg_type == "local_file":
                    path = Path(str(item.content["path"]))
                    self.artifacts.validate_outgoing(path)
                    if _file_fingerprint(path) != item.content.get("sha256"):
                        raise ValueError("outbox artifact changed after approval/queueing")
                    await self.gateway.upload_and_send(
                        item.app_role,
                        item.receive_id,
                        path,
                        idempotency_key=item.outbox_key,
                    )
                elif item.msg_type == "card_patch":
                    await self.gateway.patch_card(
                        item.app_role,
                        str(item.content["message_id"]),
                        dict(item.content["card"]),
                    )
                elif item.msg_type == "text":
                    await self.gateway.send_text(
                        item.app_role,
                        item.receive_id,
                        str(item.content.get("text") or ""),
                        receive_id_type=item.receive_id_type,
                        idempotency_key=item.outbox_key,
                    )
                else:
                    await self.gateway.send_message(
                        item.app_role,
                        item.receive_id,
                        item.msg_type,
                        item.content,
                        receive_id_type=item.receive_id_type,
                        idempotency_key=item.outbox_key,
                    )
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.warning(
                    "Feishu outbox failed ref=%s: %s",
                    _log_ref(item.outbox_key),
                    redact_log(error),
                )
                dead = item.msg_type == "local_file" and item.attempts >= 8
                self.db.fail_outbox(
                    item.outbox_key,
                    str(error),
                    retry_after_seconds=min(300, 2 ** min(item.attempts, 8)),
                    dead=dead,
                )
                if dead:
                    self.db.enqueue_outbox(
                        OutboxItem(
                            outbox_key=f"file-delivery-failed:{item.outbox_key}",
                            app_role=item.app_role,
                            receive_id=item.receive_id,
                            receive_id_type=item.receive_id_type,
                            msg_type="text",
                            content={
                                "text": "⛔ 文件在多次重试后仍未能通过校验或发送，已停止自动重试。"
                            },
                            group_key=f"file-delivery-failed:{item.outbox_key}",
                            sequence=0,
                        )
                    )
                continue
            self.db.complete_outbox(item.outbox_key, thread_id=item.thread_id, turn_id=item.turn_id)

    async def _route_incoming(self, item: InboxItem) -> None:
        message = item.message
        if message.sender_type != "user":
            self.db.complete_incoming(message.message_id)
            return
        if await self._try_pair(item):
            return
        if not self._authorized(message):
            LOG.warning("Ignoring unauthorized Feishu message for role=%s", message.app_role)
            self.db.complete_incoming(message.message_id)
            return
        if message.attachments and not message.text.strip():
            await self.artifacts.stage_attachments(message)
            self.db.hold_incoming_attachments(message)
            count = len(message.attachments)
            self._enqueue_outbound_result(
                app_role=message.app_role,
                chat_id=message.chat_id,
                text=f"📎 已暂存 {count} 个附件；请继续发送文字要求，我会一并交给 Codex。",
                base_key=f"attachment-held:{message.message_id}",
                thread_id=None,
                turn_id=None,
            )
            return
        if await self._try_artifact_command(item):
            return
        if await self._try_approval_command(item):
            return
        if await self._try_compatibility_command(item):
            return
        if await self._try_runtime_command(item):
            return
        if message.app_role == "admin" or message.chat_type == "p2p":
            await self._route_admin(item)
        else:
            await self._route_conversation(item)

    async def _try_pair(self, item: InboxItem) -> bool:
        message = item.message
        match = PAIR_RE.fullmatch(message.text.strip())
        if not match:
            return False
        if message.chat_type != "p2p" or not message.tenant_key:
            self.db.complete_incoming(message.message_id)
            return True
        current = self._owner(message.app_role)
        if current and current != message.sender_open_id:
            self.db.complete_incoming(message.message_id)
            return True
        expected = self.db.get_setting("pairing_code_hash", "")
        expires = int(self.db.get_setting("pairing_code_expires_at", "0") or 0)
        candidate = match.group(1).upper()
        if (
            not expected
            or expires < int(time.time())
            or not secrets.compare_digest(expected, hashlib.sha256(candidate.encode()).hexdigest())
        ):
            self.db.complete_incoming(message.message_id)
            return True
        if not message.sender_open_id:
            self.db.complete_incoming(message.message_id)
            return True
        expected_tenant = self.db.get_setting("paired_tenant_key", "") or ""
        if expected_tenant and message.tenant_key != expected_tenant:
            self.db.complete_incoming(message.message_id)
            return True
        anchor_union = (
            self.db.get_setting("paired_owner_union_id", "")
            or self.config.feishu.owner_union_id
            or ""
        )
        anchor_user = (
            self.db.get_setting("paired_owner_user_id", "")
            or self.config.feishu.owner_user_id
            or ""
        )
        if anchor_union or anchor_user:
            same_human = bool(
                (anchor_union and message.sender_union_id == anchor_union)
                or (anchor_user and message.sender_user_id == anchor_user)
            )
            if not same_human:
                self.db.complete_incoming(message.message_id)
                return True
        elif not message.sender_union_id and not message.sender_user_id:
            # Two app-scoped open_ids cannot prove that both bots paired to the
            # same human.  Refuse rather than weakening the identity boundary.
            self.db.complete_incoming(message.message_id)
            return True
        if not expected_tenant:
            self.db.set_setting("paired_tenant_key", message.tenant_key)
        if not anchor_union and message.sender_union_id:
            self.db.set_setting("paired_owner_union_id", message.sender_union_id)
        if not anchor_user and message.sender_user_id:
            self.db.set_setting("paired_owner_user_id", message.sender_user_id)
        self.db.set_setting(f"owner_open_id:{message.app_role}", message.sender_open_id)
        if message.tenant_key:
            self.db.set_setting(f"tenant_key:{message.app_role}", message.tenant_key)
        self.db.set_setting(f"owner_chat_id:{message.app_role}", message.chat_id)
        if message.app_id:
            self.db.set_setting(f"paired_app_id:{message.app_role}", message.app_id)
        await self.gateway.send_text(
            message.app_role,
            message.chat_id,
            "✅ Codex 应用已与本机安全配对。现在可在私聊使用管理功能，"
            "并会创建或接管绑定 Codex thread 的对话群。",
            idempotency_key=f"pair-ack:{message.app_role}:{message.message_id}",
        )
        self.db.complete_incoming(message.message_id)
        if message.app_role == "conversation":
            await self.reconcile_once()
        if self._owner("conversation"):
            self.db.set_setting("pairing_code_hash", "")
            self.db.set_setting("pairing_code_expires_at", "0")
        return True

    async def _history_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(15)
            if self._draining:
                continue
            try:
                await self._backfill_once(force=False)
            except Exception as error:
                key = f"history:{type(error).__name__}:{error}"
                if key not in self._warned:
                    self._warned.add(key)
                    LOG.exception("Feishu history recovery failed")

    def _history_chats(self) -> list[tuple[AppRole, str]]:
        chats: list[tuple[AppRole, str]] = []
        private_chat = self.db.get_setting("owner_chat_id:conversation", "") or ""
        if private_chat and self.gateway.configured("conversation") and self._owner("conversation"):
            chats.append(("conversation", private_chat))
        if self.gateway.configured("conversation") and self._owner("conversation"):
            chats.extend(
                ("conversation", binding.chat_id)
                for binding in self.db.list_bindings()
                if binding.chat_id and ("conversation", binding.chat_id) not in chats
            )
        return chats

    def _history_poll_interval(self, role: AppRole, chat_id: str, now: float) -> float:
        if any(active.chat_id == chat_id for active in self._active_by_turn.values()):
            return self.config.history_poll_seconds
        latest_ms = self.db.last_incoming_create_time_ms(role, chat_id)
        age = float("inf") if latest_ms <= 0 else max(0.0, now - latest_ms / 1000)
        if age >= self.config.history_cold_after_seconds:
            return self.config.history_poll_cold_seconds
        if age >= self.config.history_idle_after_seconds:
            return self.config.history_poll_idle_seconds
        if age >= self.config.history_warm_after_seconds:
            return self.config.history_poll_warm_seconds
        return self.config.history_poll_seconds

    @staticmethod
    def _history_stagger_seconds(chat_id: str, interval: float) -> float:
        window = max(1, min(60, int(interval * 0.1)))
        value = int(hashlib.sha256(chat_id.encode()).hexdigest()[:8], 16)
        return float(value % window)

    async def _backfill_once(self, *, force: bool = True) -> None:
        chats = self._history_chats()
        end_seconds = int(time.time())
        end_ms = end_seconds * 1000
        for role, chat_id in chats:
            schedule_key = (role, chat_id)
            if not force and time.monotonic() < self._history_next_poll.get(schedule_key, 0.0):
                continue
            key = f"history_checkpoint:{role}:{chat_id}"
            checkpoint_ms = int(self.db.get_setting(key, "0") or 0)
            if checkpoint_ms <= 0:
                checkpoint_ms = end_ms - 5 * 60 * 1000
            try:
                messages = await self.gateway.list_chat_messages(
                    role,
                    chat_id,
                    start_time_seconds=max(0, checkpoint_ms // 1000 - 1),
                    end_time_seconds=end_seconds,
                )
                for message in messages:
                    self.db.enqueue_incoming(message)
                # Advance only after the full paginated scan and durable inserts.
                self.db.set_setting(key, str(end_ms))
            finally:
                interval = self._history_poll_interval(role, chat_id, time.time())
                self._history_next_poll[schedule_key] = (
                    time.monotonic() + interval + self._history_stagger_seconds(chat_id, interval)
                )

    def _authorized(self, message: IncomingMessage) -> bool:
        owner = self._owner(message.app_role)
        if not owner or not message.sender_open_id or message.sender_open_id != owner:
            return False
        tenant = self.db.get_setting(f"tenant_key:{message.app_role}", "")
        if tenant and message.tenant_key != tenant:
            return False
        paired_tenant = self.db.get_setting("paired_tenant_key", "") or ""
        if paired_tenant and message.tenant_key != paired_tenant:
            return False
        if not paired_tenant:
            if not message.tenant_key:
                return False
            self.db.set_setting("paired_tenant_key", message.tenant_key)
        anchor_union = (
            self.db.get_setting("paired_owner_union_id", "")
            or self.config.feishu.owner_union_id
            or ""
        )
        anchor_user = (
            self.db.get_setting("paired_owner_user_id", "")
            or self.config.feishu.owner_user_id
            or ""
        )
        if anchor_union or anchor_user:
            if not (
                (anchor_union and message.sender_union_id == anchor_union)
                or (anchor_user and message.sender_user_id == anchor_user)
            ):
                return False
        elif message.sender_union_id or message.sender_user_id:
            if message.sender_union_id:
                self.db.set_setting("paired_owner_union_id", message.sender_union_id)
            if message.sender_user_id:
                self.db.set_setting("paired_owner_user_id", message.sender_user_id)
        else:
            return False
        if not tenant and message.tenant_key:
            self.db.set_setting(f"tenant_key:{message.app_role}", message.tenant_key)
        if message.chat_type == "p2p" and message.chat_id:
            self.db.set_setting(f"owner_chat_id:{message.app_role}", message.chat_id)
        return True

    async def _route_conversation(self, item: InboxItem) -> None:
        message = item.message
        binding = self.db.get_binding_by_chat(message.chat_id)
        if not binding:
            await self.gateway.send_text(
                "conversation",
                message.chat_id,
                "这个群尚未绑定 Codex 对话。请私聊 Codex 机器人发送“同步”。",
                idempotency_key=f"unbound:{message.message_id}",
            )
            self.db.complete_incoming(message.message_id)
            return
        text = message.text.strip()
        if text == "!status":
            active = self._active_by_thread.get(binding.thread_id)
            queue = self._thread_queues.get(binding.thread_id)
            blocked = self.db.get_setting(f"blocked_thread:{binding.thread_id}", "")
            status = (
                f"安全锁定（待核对 turn `{blocked}`）"
                if blocked
                else (f"正在执行 turn `{active.turn_id}`" if active else "当前空闲")
            )
            await self.gateway.send_text(
                "conversation",
                message.chat_id,
                f"{status}；等待队列 {queue.qsize() if queue else 0} 条。",
                idempotency_key=f"status:{message.message_id}",
            )
            self.db.complete_incoming(message.message_id)
            return
        if text == "!stop":
            active = self._active_by_thread.get(binding.thread_id)
            if active:
                await self.codex.interrupt_turn(binding.thread_id, active.turn_id)
                reply = "已请求停止当前执行；队列中的后续消息不会被清空。"
            else:
                reply = "当前没有由飞书桥启动的活动执行。"
            await self.gateway.send_text(
                "conversation",
                message.chat_id,
                reply,
                idempotency_key=f"stop:{message.message_id}",
            )
            self.db.complete_incoming(message.message_id)
            return
        if text.startswith("!steer "):
            message.text = text[7:].strip()
            active = self._active_by_thread.get(binding.thread_id)
            if active:
                self.db.merge_held_attachments(message)
                await self._steer(item, binding, active)
                return
        self.db.merge_held_attachments(message)
        await self._queue_thread_message(item, binding)

    async def _try_runtime_command(self, item: InboxItem) -> bool:
        message = item.message
        text = message.text.strip()
        command, _, argument = text.partition(" ")
        command_aliases = {
            "!帮助": "帮助",
            "!配置": "配置",
            "!设置": "设置",
            "!模型": "模型",
            "/model": "模型",
            "!速度": "速度",
            "/fast": "快速",
            "!推理": "推理",
            "!权限": "权限",
            "/permissions": "权限",
            "/status": "状态",
            "!配置记录": "配置记录",
            "!设置记录": "设置记录",
            "!配置重置": "配置重置",
            "!设置重置": "设置重置",
        }
        kind = command_aliases.get(command.lower())
        if not kind:
            return False
        binding = self.db.get_binding_by_chat(message.chat_id)
        if message.app_role == "conversation" and message.chat_type != "p2p" and not binding:
            return False
        scope = binding.thread_id if binding else "admin"
        argument = argument.strip()
        card: dict[str, Any] | None = None

        if command.startswith("/") and self._runtime_compatibility_error:
            reply = (
                "⛔ Codex CLI 设置兼容门禁已触发，未写入任何设置。\n"
                f"{self._runtime_compatibility_error}\n"
                "请发送 `/compat`，再选择“检测并修复”。"
            )
            await self.gateway.send_text(
                message.app_role,
                message.chat_id,
                reply,
                idempotency_key=f"runtime-incompatible:{message.message_id}",
            )
            self.db.complete_incoming(message.message_id)
            return True

        if kind == "帮助":
            reply = (
                "与 Codex CLI 一致的设置命令：\n"
                "• `/model`：选择模型及推理强度\n"
                "• `/fast`：切换当前模型的 Fast 层级\n"
                "• `/permissions`：选择权限预设\n"
                "• `/status`：查看当前会话配置\n"
                "• `/compat`：检测并修复 CLI 设置兼容门禁\n"
                "飞书会用按钮代替终端里的选择弹窗。"
            )
        elif kind in {"配置", "设置", "状态"}:
            reply = await self._format_runtime_status(scope, binding)
        elif kind in {"配置记录", "设置记录"}:
            events = self.db.runtime_config_history(scope, limit=10)
            if not events:
                reply = "尚无配置变更记录。"
            else:
                lines = [
                    f"• {event['name']}：{event['old_value'] or '默认'} → "
                    f"{event['new_value'] or '默认'}"
                    for event in events
                ]
                reply = "最近配置变更：\n" + "\n".join(lines)
        elif kind in {"配置重置", "设置重置"}:
            for name in ("model", "effort", "service_tier", "approval_policy", "sandbox"):
                self.db.set_runtime_config(scope, name, "", message_id=message.message_id)
            applied = await self._apply_runtime_if_idle(binding, scope)
            reply = "已重置为桥接默认配置。" + ("" if applied else " 当前任务结束后生效。")
        elif kind == "模型":
            models = await self.codex.list_models()
            if not argument:
                card = _model_picker_card(models, self._runtime_settings(scope).model)
                reply = ""
            else:
                parts = argument.split()
                requested_model = parts[0]
                requested_effort = parts[1].lower() if len(parts) == 2 else None
                if len(parts) > 2:
                    requested_effort = "__invalid__"
                selected = next(
                    (
                        model
                        for model in models
                        if requested_model
                        in {str(model.get("model") or ""), str(model.get("id") or "")}
                    ),
                    None,
                )
                if not selected:
                    reply = f"未找到模型 `{requested_model}`；发送 `/model` 查看可用项。"
                else:
                    value = str(selected.get("model") or selected.get("id"))
                    efforts = _model_efforts(selected)
                    if requested_effort == "default":
                        requested_effort = ""
                    if requested_effort is not None and requested_effort not in {"", *efforts}:
                        reply = "该模型不支持这个推理强度；请重新发送 `/model` 选择。"
                    else:
                        self.db.set_runtime_config(
                            scope, "model", value, message_id=message.message_id
                        )
                        previous_effort = self.db.get_setting(f"runtime:{scope}:effort", "") or ""
                        if requested_effort is not None or previous_effort:
                            self.db.set_runtime_config(
                                scope,
                                "effort",
                                (
                                    RUNTIME_MODEL_DEFAULT
                                    if requested_effort == ""
                                    else requested_effort or ""
                                ),
                                message_id=message.message_id,
                            )
                        applied = await self._apply_runtime_if_idle(binding, scope)
                        suffix = "" if applied else " 当前任务结束后生效。"
                        if requested_effort is None and efforts:
                            card = _reasoning_picker_card(
                                value,
                                efforts,
                                str(selected.get("defaultReasoningEffort") or ""),
                                suffix,
                            )
                            reply = ""
                        else:
                            effort_text = requested_effort or str(
                                selected.get("defaultReasoningEffort") or "模型默认"
                            )
                            reply = f"模型已设为 `{value}`，推理强度为 `{effort_text}`。{suffix}"
        elif kind == "快速":
            if argument:
                reply = "`/fast` 是切换命令，不接受参数；再次发送即可开关。"
            else:
                models = await self.codex.list_models()
                settings = self._runtime_settings(scope)
                selected = _selected_model(models, settings.model)
                tiers = selected.get("serviceTiers") or [] if selected else []
                priority = next(
                    (tier for tier in tiers if str(tier.get("id") or "") == "priority"),
                    None,
                )
                if not priority:
                    reply = "当前模型没有在模型目录中声明 Fast 层级；Codex CLI 也不会显示 `/fast`。"
                else:
                    enabled = settings.service_tier != "priority"
                    value = "priority" if enabled else RUNTIME_SERVICE_TIER_OFF
                    self.db.set_runtime_config(
                        scope, "service_tier", value, message_id=message.message_id
                    )
                    applied = await self._apply_runtime_if_idle(binding, scope)
                    reply = f"Fast 模式已{'开启' if enabled else '关闭'}。" + (
                        "" if applied else " 当前任务结束后生效。"
                    )
        elif kind == "速度":
            aliases = {
                "普通": RUNTIME_SERVICE_TIER_OFF,
                "标准": RUNTIME_SERVICE_TIER_OFF,
                "normal": RUNTIME_SERVICE_TIER_OFF,
                "default": RUNTIME_SERVICE_TIER_OFF,
                "off": RUNTIME_SERVICE_TIER_OFF,
                "快速": "priority",
                "fast": "priority",
                "priority": "priority",
                "on": "priority",
            }
            if not argument:
                reply = "速度可选：`普通`、`快速`。设置示例：`!速度 快速`。"
            elif argument.lower() not in aliases:
                reply = "未知速度；可选 `普通` 或 `快速`。"
            else:
                value = aliases[argument.lower()]
                self.db.set_runtime_config(
                    scope, "service_tier", value, message_id=message.message_id
                )
                applied = await self._apply_runtime_if_idle(binding, scope)
                reply = f"服务速度已设为 `{'快速' if value == 'priority' else '普通'}`。" + (
                    "" if applied else " 当前任务结束后生效。"
                )
        elif kind == "推理":
            aliases = {
                "低": "low",
                "快": "low",
                "low": "low",
                "中": "medium",
                "均衡": "medium",
                "medium": "medium",
                "高": "high",
                "深度": "high",
                "high": "high",
                "极高": "xhigh",
                "xhigh": "xhigh",
                "最大": "max",
                "max": "max",
                "超强": "ultra",
                "ultra": "ultra",
            }
            if not argument:
                reply = "推理可选：`低 / 中 / 高 / 极高 / 最大 / 超强`。"
            elif argument.lower() not in aliases:
                reply = "未知推理档位；发送 `!推理` 查看可用项。"
            else:
                value = aliases[argument.lower()]
                self.db.set_runtime_config(scope, "effort", value, message_id=message.message_id)
                applied = await self._apply_runtime_if_idle(binding, scope)
                reply = f"推理档位已设为 `{value}`。" + ("" if applied else " 当前任务结束后生效。")
        else:
            aliases = {
                "full-access": ("never", "danger-full-access"),
                "yolo": ("never", "danger-full-access"),
                "完全": ("never", "danger-full-access"),
                "全权限": ("never", "danger-full-access"),
                "default": ("on-request", "workspace-write"),
                "工作区": ("on-request", "workspace-write"),
                "workspace": ("on-request", "workspace-write"),
                "只读": ("on-request", "read-only"),
                "readonly": ("on-request", "read-only"),
                "read-only": ("on-request", "read-only"),
            }
            if not argument:
                card = _permissions_picker_card(
                    self._runtime_settings(scope),
                    allow_full_access=self.config.allow_remote_full_access,
                )
                reply = ""
            elif argument.lower() not in aliases:
                reply = "未知权限预设；请发送 `/permissions` 后选择。"
            else:
                approval, sandbox = aliases[argument.lower()]
                if (approval, sandbox) == (
                    "never",
                    "danger-full-access",
                ) and not self.config.allow_remote_full_access:
                    reply = (
                        "⛔ 远程 Full Access 默认禁用。只有在本机配置中显式设置 "
                        "`allow_remote_full_access = true` 后才能启用。"
                    )
                else:
                    self.db.set_runtime_config(
                        scope, "approval_policy", approval, message_id=message.message_id
                    )
                    self.db.set_runtime_config(
                        scope, "sandbox", sandbox, message_id=message.message_id
                    )
                    applied = await self._apply_runtime_if_idle(binding, scope)
                    label = {
                        "read-only": "Read Only",
                        "readonly": "Read Only",
                        "只读": "Read Only",
                        "default": "Default",
                        "workspace": "Default",
                        "工作区": "Default",
                        "full-access": "Full Access",
                        "yolo": "Full Access",
                    }.get(argument.lower(), argument)
                    reply = f"权限已设为 `{label}`。" + ("" if applied else " 当前任务结束后生效。")

        if card:
            await self.gateway.send_card(
                message.app_role,
                message.chat_id,
                card,
                idempotency_key=f"runtime-config:{kind}:{message.message_id}",
            )
        else:
            await self.gateway.send_text(
                message.app_role,
                message.chat_id,
                reply,
                idempotency_key=f"runtime-config:{kind}:{message.message_id}",
            )
        self.db.complete_incoming(message.message_id)
        return True

    async def _try_compatibility_command(self, item: InboxItem) -> bool:
        message = item.message
        text = message.text.strip()
        if text in {"/compat", "设置兼容", "修复设置兼容"}:
            if self._runtime_compatibility_error:
                await self.gateway.send_card(
                    message.app_role,
                    message.chat_id,
                    _compatibility_repair_card(
                        getattr(self.codex, "cli_version", None) or "未知",
                        SUPPORTED_SETTINGS_CLI_VERSION,
                        self._runtime_compatibility_error,
                    ),
                    idempotency_key=f"settings-compat-show:{message.message_id}",
                )
            else:
                await self.gateway.send_text(
                    message.app_role,
                    message.chat_id,
                    "✅ 当前 Codex CLI 设置能力已经验证，无需修复。",
                    idempotency_key=f"settings-compat-ok:{message.message_id}",
                )
            self.db.complete_incoming(message.message_id)
            return True

        match = re.fullmatch(
            r"/bridge-settings-compat\s+(repair|dismiss)\s+(\S+)",
            text,
            re.IGNORECASE,
        )
        if not match:
            return False
        action, requested_version = match.groups()
        version = getattr(self.codex, "cli_version", None) or "未知"
        if requested_version != version:
            reply = (
                f"该卡片针对 Codex CLI `{requested_version}`，当前版本为 `{version}`；"
                "操作已取消，请发送 `/compat` 获取新卡片。"
            )
            color = "orange"
        elif action.lower() == "dismiss":
            self.db.set_setting("codex_settings_prompt_dismissed_version", version)
            reply = (
                f"已暂不处理 Codex CLI `{version}`。设置命令继续保持只读门禁；"
                "之后可发送 `/compat` 再次检测。"
            )
            color = "orange"
        else:
            await self._probe_runtime_settings_compatibility(
                allow_version_upgrade=True,
                notify=False,
            )
            if self._runtime_compatibility_error:
                reply = (
                    f"❌ Codex CLI `{version}` 设置能力检测未通过，未更新验证基线。\n"
                    f"{self._runtime_compatibility_error}"
                )
                color = "red"
            else:
                self.db.delete_setting("codex_settings_prompt_dismissed_version")
                reply = (
                    f"✅ Codex CLI `{version}` 设置能力检测通过，已更新本机验证基线。\n"
                    "`/model`、`/fast`、`/permissions` 和 `/status` 已恢复使用。"
                )
                color = "green"
        await self.gateway.send_card(
            message.app_role,
            message.chat_id,
            progress_card("Codex CLI 设置兼容", reply, color=color),
            idempotency_key=f"settings-compat-result:{message.message_id}",
        )
        self.db.complete_incoming(message.message_id)
        return True

    def _runtime_settings(self, scope: str) -> RuntimeSettings:
        def value(name: str) -> str | None:
            return self.db.get_setting(f"runtime:{scope}:{name}", None)

        model = value("model")
        effort = value("effort")
        service_tier = value("service_tier")
        approval_policy = value("approval_policy")
        sandbox = value("sandbox")
        selected_approval = approval_policy or self.config.approval_policy
        selected_sandbox = sandbox or self.config.sandbox
        if (
            approval_policy is not None
            and (selected_approval, selected_sandbox) == ("never", "danger-full-access")
            and not self.config.allow_remote_full_access
        ):
            selected_approval = self.config.approval_policy
            selected_sandbox = self.config.sandbox
        return RuntimeSettings(
            model=model or self.config.model,
            effort=(
                None
                if effort == RUNTIME_MODEL_DEFAULT
                else effort or self.config.model_reasoning_effort
            ),
            service_tier=(
                None
                if service_tier == RUNTIME_SERVICE_TIER_OFF
                else service_tier or self.config.service_tier
            ),
            approval_policy=selected_approval,
            sandbox=selected_sandbox,
        )

    def _format_runtime_settings(self, scope: str, settings: RuntimeSettings) -> str:
        permission = {
            ("never", "danger-full-access"): "YOLO",
            ("on-request", "workspace-write"): "工作区",
            ("on-request", "read-only"): "只读",
        }.get((settings.approval_policy, settings.sandbox), "自定义")
        target = "私聊临时任务" if scope == "admin" else "当前对话"
        return (
            f"{target}配置：\n"
            f"• 模型：`{settings.model or '继承 Codex 默认'}`\n"
            f"• 推理：`{settings.effort or '继承模型默认'}`\n"
            f"• 速度：`{'快速' if settings.service_tier == 'priority' else '普通'}`\n"
            f"• 权限：`{permission}`"
        )

    async def _format_runtime_status(self, scope: str, binding: Binding | None) -> str:
        settings = self._runtime_settings(scope)
        models = await self.codex.list_models()
        selected = _selected_model(models, settings.model)
        model_name = settings.model or (
            str(selected.get("model") or selected.get("id")) if selected else "Codex 默认"
        )
        effort = settings.effort or (
            str(selected.get("defaultReasoningEffort") or "模型默认") if selected else "模型默认"
        )
        permission = _permission_label(settings)
        target = "私聊临时任务" if scope == "admin" else "当前对话"
        version = getattr(self.codex, "cli_version", None) or "未知"
        cwd = binding.cwd if binding else str(self.config.admin_scratch_dir)
        return (
            f"{target}状态：\n"
            f"• Codex CLI：`{version}`\n"
            f"• 模型：`{model_name}`\n"
            f"• 推理：`{effort}`\n"
            f"• Fast：`{'on' if settings.service_tier == 'priority' else 'off'}`\n"
            f"• 权限：`{permission}`\n"
            f"• 工作目录：`{cwd}`"
        )

    async def _probe_runtime_settings_compatibility(
        self,
        *,
        allow_version_upgrade: bool = False,
        notify: bool = True,
    ) -> None:
        version = getattr(self.codex, "cli_version", None)
        previous = self.db.get_setting("codex_cli_version_seen", "") or ""
        if version:
            self.db.set_setting("codex_cli_version_seen", version)
        locally_verified = self.db.get_setting("codex_settings_verified_version", "") or ""
        version_needs_upgrade = bool(
            version and version not in {SUPPORTED_SETTINGS_CLI_VERSION, locally_verified}
        )
        if version_needs_upgrade and not allow_version_upgrade:
            self._runtime_compatibility_error = (
                f"检测到 Codex CLI `{version}`，桥接器已验证的设置命令基线为 "
                f"`{SUPPORTED_SETTINGS_CLI_VERSION}`。"
            )
        else:
            try:
                await self._validate_runtime_settings_catalog()
                if allow_version_upgrade:
                    await self._exercise_runtime_settings_protocol()
            except Exception as error:
                self._runtime_compatibility_error = f"App Server 能力探测失败：{error}"
            else:
                self._runtime_compatibility_error = None
                if version and allow_version_upgrade:
                    self.db.set_setting("codex_settings_verified_version", version)
        state = "ok" if not self._runtime_compatibility_error else self._runtime_compatibility_error
        self.db.set_setting("codex_settings_compatibility", state)
        if not notify:
            return
        if self._runtime_compatibility_error and version_needs_upgrade:
            dismissed = self.db.get_setting("codex_settings_prompt_dismissed_version", "")
            if dismissed != version:
                await self._notify_admin_card(
                    _compatibility_repair_card(
                        version or "未知",
                        SUPPORTED_SETTINGS_CLI_VERSION,
                        self._runtime_compatibility_error,
                    ),
                    idempotency_key=f"settings-compat-upgrade:{version}",
                )
        elif self._runtime_compatibility_error:
            await self._notify_admin("⚠️ " + self._runtime_compatibility_error)
        elif previous and version and previous != version:
            await self._notify_admin(
                f"✅ Codex CLI 已从 `{previous}` 更新到 `{version}`，飞书设置能力探测通过。"
            )

    async def _validate_runtime_settings_catalog(self) -> None:
        models = await self.codex.list_models()
        if not models or any(
            not (model.get("model") or model.get("id"))
            or "supportedReasoningEfforts" not in model
            or "serviceTiers" not in model
            for model in models
        ):
            raise ValueError("model/list 返回的能力目录缺少设置字段")
        selected = _selected_model(models, self.config.model)
        if self.config.model and (
            not selected
            or self.config.model
            not in {
                str(selected.get("model") or ""),
                str(selected.get("id") or ""),
            }
        ):
            raise ValueError(f"默认模型 {self.config.model!r} 不在 model/list 中")
        if (
            selected
            and self.config.model_reasoning_effort
            and self.config.model_reasoning_effort not in _model_efforts(selected)
        ):
            raise ValueError(
                f"默认推理强度 {self.config.model_reasoning_effort!r} "
                f"不受模型 {self.config.model!r} 支持"
            )
        if (
            selected
            and self.config.service_tier
            and self.config.service_tier
            not in {str(tier.get("id") or "") for tier in selected.get("serviceTiers") or []}
        ):
            raise ValueError(
                f"默认服务层级 {self.config.service_tier!r} 不受模型 {self.config.model!r} 支持"
            )

    async def _exercise_runtime_settings_protocol(self) -> None:
        thread = await self.codex.start_thread(
            cwd=str(self.config.admin_scratch_dir),
            approval_policy=self.config.approval_policy,
            sandbox=self.config.sandbox,
            model=self.config.model,
            service_tier=self.config.service_tier,
            ephemeral=True,
        )
        thread_id = str(thread.get("id") or "")
        if not thread_id:
            raise ValueError("thread/start 未返回 thread id")
        try:
            await self.codex.update_thread_settings(
                thread_id,
                approval_policy=self.config.approval_policy,
                sandbox=self.config.sandbox,
                model=self.config.model,
                effort=self.config.model_reasoning_effort,
                service_tier=self.config.service_tier,
            )
        finally:
            with contextlib.suppress(Exception):
                await self.codex.unsubscribe_thread(thread_id)

    async def _apply_runtime_if_idle(self, binding: Binding | None, scope: str) -> bool:
        if not binding:
            return True
        if binding.thread_id in self._active_by_thread:
            return False
        settings = self._runtime_settings(scope)
        try:
            await self.codex.update_thread_settings(
                binding.thread_id,
                approval_policy=settings.approval_policy,
                sandbox=settings.sandbox,
                model=settings.model,
                effort=settings.effort,
                service_tier=settings.service_tier,
            )
        except CodexRPCError:
            await self.codex.resume_thread(
                binding.thread_id,
                approval_policy=settings.approval_policy,
                sandbox=settings.sandbox,
                model=settings.model,
                service_tier=settings.service_tier,
                exclude_turns=True,
            )
            await self.codex.update_thread_settings(
                binding.thread_id,
                approval_policy=settings.approval_policy,
                sandbox=settings.sandbox,
                model=settings.model,
                effort=settings.effort,
                service_tier=settings.service_tier,
            )
        return True

    async def _steer(self, item: InboxItem, binding: Binding, active: ActiveTurn) -> None:
        message = item.message
        try:
            persisted_turn = await self._find_turn_summary(binding.thread_id, active.turn_id)
            if persisted_turn and persisted_turn.get("status") in {
                "completed",
                "failed",
                "interrupted",
            }:
                # A completion notification can be lost while the durable turn
                # is already terminal.  Deliver that result first and preserve
                # the user's correction as a new FIFO turn.
                await self._finalize_turn(active, persisted_turn)
                await self._queue_thread_message(item, binding)
                return
        except Exception:
            LOG.warning(
                "Could not preflight turn ref=%s before steering; falling back to turn/steer",
                _log_ref(active.turn_id),
                exc_info=True,
            )
        inputs = await self.artifacts.prepare_inputs(message)
        self.db.mark_incoming_dispatching(message.message_id)
        try:
            await self.codex.steer_turn(
                binding.thread_id,
                active.turn_id,
                inputs,
                client_message_id=message.message_id,
            )
        except CodexRPCError:
            # Completion may have raced with steering.  Queue it as the next
            # turn rather than dropping the user's correction.
            self.db.fail_incoming(
                message.message_id, "steer raced with completion", retry_after_seconds=0
            )
            return
        except Exception as error:
            self._record_incoming_failure(item, error)
            return
        self.db.complete_incoming(message.message_id)
        await self.gateway.send_text(
            "conversation",
            message.chat_id,
            "↪️ 已把补充要求送入当前执行。",
            idempotency_key=f"steered:{message.message_id}",
        )

    async def _queue_thread_message(self, item: InboxItem, binding: Binding) -> None:
        queue = self._thread_queues.setdefault(binding.thread_id, asyncio.Queue())
        position = queue.qsize() + (1 if self._current_active_turn(binding.thread_id) else 0)
        text = (
            "已收到，正在启动 Codex。" if position == 0 else f"已收到，前面还有 {position} 条消息。"
        )
        self.db.mark_incoming_queued(item.message.message_id)
        progress_id = await self.gateway.send_card(
            "conversation",
            item.message.chat_id,
            progress_card(
                "Codex 已接单",
                text + "\n\n执行中可以发送 `!steer 补充要求`；发送 `!stop` 可停止当前轮。",
            ),
            idempotency_key=f"progress:{item.message.message_id}",
        )
        await queue.put(
            ScheduledMessage(
                inbox=item,
                binding=binding,
                progress_message_id=progress_id,
                app_role="conversation",
                chat_id=item.message.chat_id,
            )
        )
        if (
            binding.thread_id not in self._thread_workers
            or self._thread_workers[binding.thread_id].done()
        ):
            self._thread_workers[binding.thread_id] = asyncio.create_task(
                self._thread_worker(binding.thread_id), name=f"thread-worker:{binding.thread_id}"
            )

    async def _thread_worker(self, thread_id: str) -> None:
        queue = self._thread_queues[thread_id]
        while not self._stop.is_set():
            job = await queue.get()
            try:
                await self._execute_job(thread_id, job)
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.exception("Thread job failed ref=%s", _log_ref(thread_id))
                state = self.db.inbox_state(job.inbox.message.message_id)
                if state in {"processing", "queued"}:
                    self.db.fail_incoming(
                        job.inbox.message.message_id, str(error), retry_after_seconds=15
                    )
                elif state == "dispatching":
                    self._record_incoming_failure(job.inbox, error)
            finally:
                queue.task_done()

    async def _execute_job(self, thread_id: str, job: ScheduledMessage) -> None:
        message = job.inbox.message
        while active := self._current_active_turn(thread_id):
            done = self._turn_done.setdefault(active.turn_id, asyncio.Event())
            await done.wait()
        await self._wait_thread_available(thread_id, job)
        lease_ttl = 300
        if not self.db.acquire_thread_lease(thread_id, self.worker_id, ttl_seconds=lease_ttl):
            self.db.fail_incoming(message.message_id, "thread lease busy", retry_after_seconds=15)
            return
        try:
            runtime = self._runtime_settings(thread_id)
            inputs = await self.artifacts.prepare_inputs(message)
            self.db.mark_incoming_dispatching(message.message_id)
            self._pending_jobs[thread_id] = job
            try:
                turn = await self.codex.start_turn(
                    thread_id,
                    inputs,
                    client_message_id=message.message_id,
                    approval_policy=runtime.approval_policy,
                    sandbox=runtime.sandbox,
                    model=runtime.model,
                    effort=runtime.effort,
                    service_tier=runtime.service_tier,
                )
            except CodexRPCError as error:
                self.db.fail_incoming(message.message_id, str(error), retry_after_seconds=10)
                await self._patch_job_error(job, f"Codex 拒绝启动：{error}")
                return
            except Exception as error:
                self._record_incoming_failure(job.inbox, error)
                await self._patch_job_error(job, "提交时连接中断，状态待人工确认，未自动重放。")
                return
            turn_id = str(turn.get("id") or "")
            if not turn_id:
                self.db.mark_incoming_ambiguous(message.message_id, "turn/start returned no id")
                await self._patch_job_error(job, "Codex 返回了无法确认的启动结果。")
                return
            completed_already = (
                turn_id in self._completed_turns
                or self._turn_done.get(turn_id, asyncio.Event()).is_set()
            )
            self.db.upsert_turn_job(
                TurnJob(
                    message_id=message.message_id,
                    thread_id=thread_id,
                    turn_id=turn_id,
                    app_role=job.app_role,
                    chat_id=job.chat_id,
                    progress_message_id=job.progress_message_id,
                    state="completed" if completed_already else "accepted",
                )
            )
            if completed_already:
                self.db.complete_incoming(message.message_id)
                self._pending_jobs.pop(thread_id, None)
                return
            active = self._active_by_turn.get(turn_id)
            if not active:
                active = ActiveTurn(
                    thread_id=thread_id,
                    turn_id=turn_id,
                    chat_id=job.chat_id,
                    app_role=job.app_role,
                    progress_message_id=job.progress_message_id,
                    started_monotonic=time.monotonic(),
                )
                self._register_active(active)
            elif not active.progress_message_id:
                active.progress_message_id = job.progress_message_id
            self.db.complete_incoming(message.message_id)
            self._pending_jobs.pop(thread_id, None)
            if turn_id in self._completed_turns:
                return
            done = self._turn_done.setdefault(turn_id, asyncio.Event())
            while not done.is_set() and not self._stop.is_set():
                try:
                    await asyncio.wait_for(done.wait(), timeout=60)
                except TimeoutError:
                    self.db.renew_thread_lease(thread_id, self.worker_id, ttl_seconds=300)
        finally:
            self._pending_jobs.pop(thread_id, None)
            self.db.release_thread_lease(thread_id, self.worker_id)

    async def _wait_thread_available(self, thread_id: str, job: ScheduledMessage) -> None:
        last_notice = ""
        while not self._stop.is_set():
            blocked_turn = self.db.get_setting(f"blocked_thread:{thread_id}", "") or ""
            if blocked_turn:
                notice = f"thread 已安全锁定，等待管理员核对并解除：`{blocked_turn}`"
                if notice != last_notice:
                    await self._patch_job_waiting(job, notice)
                    last_notice = notice
                await asyncio.sleep(3)
                continue
            override_until = int(
                self.db.get_setting(f"thread_override_until:{thread_id}", "0") or 0
            )
            if override_until >= int(time.time()):
                self.db.delete_setting(f"thread_override_until:{thread_id}")
                return
            try:
                raw = await self.codex.read_thread(thread_id, include_turns=False)
                turns = await self._turn_summaries(thread_id, items_view="notLoaded", max_turns=1)
            except Exception as error:
                notice = f"暂时无法核对本机 thread 状态：{type(error).__name__}；仍在等待。"
                if notice != last_notice:
                    await self._patch_job_waiting(job, notice)
                    last_notice = notice
                await asyncio.sleep(5)
                continue
            latest = turns[0] if turns else {}
            latest_status = str(latest.get("status") or "")
            status_type = str((raw.get("status") or {}).get("type") or "")
            rollout_path = Path(str(raw.get("path"))) if raw.get("path") else None
            age: float | None = None
            if rollout_path:
                with contextlib.suppress(OSError):
                    age = max(0.0, time.time() - rollout_path.stat().st_mtime)
            busy = status_type == "active" or latest_status == "inProgress"
            if busy:
                external_turn = str(latest.get("id") or "external-active")
                if age is None or age > 300:
                    self.db.set_setting(f"blocked_thread:{thread_id}", external_turn)
                    warning = (
                        "⚠️ 检测到另一个 Codex 进程留下的活动/未终止 turn，且无法安全证明它已结束。"
                        "为避免两个 App Server 同时写同一 thread，手机任务已暂停。\n\n"
                        f"thread：`{thread_id}`\nturn：`{external_turn}`\n\n"
                        f"请在本机核对后，到 Codex 机器人私聊发送 `解除线程 {thread_id}`。"
                    )
                    self._enqueue_outbound_result(
                        app_role=job.app_role,
                        chat_id=job.chat_id,
                        text=warning,
                        base_key=f"external-thread-lock:{thread_id}:{external_turn}",
                        thread_id=None,
                        turn_id=None,
                    )
                    continue
                notice = "检测到本机 Codex 正在使用同一 thread；消息已保留并等待它结束。"
                if notice != last_notice:
                    await self._patch_job_waiting(job, notice)
                    last_notice = notice
                await asyncio.sleep(5)
                continue
            if age is not None and age < 10:
                notice = "thread rollout 刚刚仍在变化；为避免跨进程并发，等待 10 秒静默窗口。"
                if notice != last_notice:
                    await self._patch_job_waiting(job, notice)
                    last_notice = notice
                await asyncio.sleep(min(3, max(0.5, 10 - age)))
                continue
            return
        raise asyncio.CancelledError

    async def _patch_job_waiting(self, job: ScheduledMessage, text: str) -> None:
        with contextlib.suppress(Exception):
            await self.gateway.patch_card(
                job.app_role,
                job.progress_message_id,
                progress_card("Codex 等待安全执行窗口", text, color="orange"),
            )

    async def _route_admin(self, item: InboxItem) -> None:
        message = item.message
        text = message.text.strip()
        if text in {"帮助", "/help", "help"}:
            reply = (
                "Codex 私聊命令：\n"
                "• `最近`：查看已跟进/待创建的 Codex 对话\n"
                "• `新对话 名称 | /工作目录`：创建 Codex 对话和飞书群\n"
                "• `额度`：查看 Codex 额度信息\n"
                "• `同步`：立即扫描最近 3 个对话\n"
                "• `状态`：查看桥接服务状态\n"
                "• `待确认` / `重试 消息ID` / `忽略 消息ID`：处理崩溃临界区消息\n"
                "• `解除线程 threadID`：本机核对进程中断的 turn 后解除安全锁\n"
                "• `/model` / `/fast` / `/permissions` / `/status`："
                "按 Codex CLI 方式管理临时任务配置\n"
                "• `/compat`：检测并修复 CLI 升级后的设置兼容门禁\n"
                "其他文字会在一个临时、上下文无关的 Codex 对话中处理。"
            )
            await self._admin_reply(message, reply, "help")
            self.db.complete_incoming(message.message_id)
            return
        if text in {"最近", "对话", "/recent"}:
            await self.reconcile_once()
            bindings = self.db.list_bindings()
            lines = [
                f"{index}. {item.title} — "
                f"{'已绑定' if item.chat_id else '待创建'}\n   `{item.thread_id}`"
                for index, item in enumerate(bindings, 1)
            ]
            await self._admin_reply(message, "当前跟进：\n" + "\n".join(lines), "recent")
            self.db.complete_incoming(message.message_id)
            return
        if text in {"同步", "/sync"}:
            bindings = await self.reconcile_once()
            await self._admin_reply(
                message, f"同步完成；滚动最近 3 个中有 {len(bindings)} 个已登记。", "sync"
            )
            self.db.complete_incoming(message.message_id)
            return
        if text in {"额度", "/quota"}:
            quota = await self.codex.quota()
            rendered = json.dumps(quota, ensure_ascii=False, indent=2)[:12000]
            await self._admin_reply(
                message, f"Codex 额度/用量：\n```json\n{rendered}\n```", "quota"
            )
            self.db.complete_incoming(message.message_id)
            return
        if text in {"状态", "/status"}:
            counts = self.db.inbox_counts()
            receivers = self.gateway.receiver_status()
            api_usage = self.db.api_usage()
            api_total = sum(api_usage.values())
            api_top = (
                ", ".join(
                    f"{name}={count}"
                    for name, count in sorted(
                        api_usage.items(), key=lambda item: item[1], reverse=True
                    )[:4]
                )
                or "尚无记录"
            )
            reply = (
                f"服务运行中；活动 Codex：{len(self._active_by_thread)}；"
                f"收件箱：{counts or {'empty': 0}}；"
                f"发件箱：{self.db.outbox_counts() or {'empty': 0}}；长连接：{receivers}。\n"
                f"本月桥内已记录飞书 API：{api_total} 次（{api_top}）。"
            )
            await self._admin_reply(message, reply, "bridge-status")
            self.db.complete_incoming(message.message_id)
            return
        if text in {"待确认", "/ambiguous"}:
            pending = self.db.list_ambiguous()
            if pending:
                lines = [
                    f"• `{value.message.message_id}`：{value.message.text[:100]}"
                    for value in pending
                ]
                reply = "以下消息可能已经提交给 Codex，默认不会重放：\n" + "\n".join(lines)
            else:
                reply = "没有状态待确认的消息。"
            await self._admin_reply(message, reply, "ambiguous-list")
            self.db.complete_incoming(message.message_id)
            return
        retry_match = re.fullmatch(r"(?:重试|/retry)\s+(\S+)", text, re.IGNORECASE)
        if retry_match:
            target = retry_match.group(1)
            changed = self.db.retry_ambiguous(target)
            await self._admin_reply(
                message,
                "已重新入队；这可能重复此前已发生的外部副作用。"
                if changed
                else "没有找到该待确认消息。",
                "ambiguous-retry",
            )
            self.db.complete_incoming(message.message_id)
            return
        dismiss_match = re.fullmatch(r"(?:忽略|/dismiss)\s+(\S+)", text, re.IGNORECASE)
        if dismiss_match:
            changed = self.db.dismiss_ambiguous(dismiss_match.group(1))
            await self._admin_reply(
                message,
                "已将该消息标记为不重放。" if changed else "没有找到该待确认消息。",
                "ambiguous-dismiss",
            )
            self.db.complete_incoming(message.message_id)
            return
        unblock_match = re.fullmatch(r"(?:解除线程|/unblock)\s+(\S+)", text, re.IGNORECASE)
        if unblock_match:
            token = unblock_match.group(1)
            candidates = [
                binding.thread_id
                for binding in self.db.list_bindings()
                if binding.thread_id == token or binding.thread_id.startswith(token)
            ]
            if len(candidates) == 1:
                thread_id = candidates[0]
            elif self.db.get_setting(f"blocked_thread:{token}", ""):
                thread_id = token
            else:
                thread_id = ""
            if thread_id and self.db.get_setting(f"blocked_thread:{thread_id}", ""):
                lost_turn = self.db.get_setting(f"blocked_thread:{thread_id}", "") or ""
                self.db.delete_setting(f"blocked_thread:{thread_id}")
                self.db.set_setting(
                    f"thread_override_until:{thread_id}", str(int(time.time()) + 60)
                )
                if lost_turn:
                    self.db.set_turn_job_state(lost_turn, "abandoned")
                reply = "已解除 thread 安全锁；原任务没有自动重放，后续队列可以继续。"
            else:
                reply = "没有找到唯一且处于锁定状态的 thread。"
            await self._admin_reply(message, reply, "thread-unblock")
            self.db.complete_incoming(message.message_id)
            return
        match = NEW_THREAD_RE.match(text)
        if match:
            spec = match.group(1).strip()
            if "|" in spec:
                title, cwd_text = (part.strip() for part in spec.split("|", 1))
                cwd = Path(cwd_text)
            else:
                title, cwd = spec, None
            try:
                binding = await self.create_new_conversation(
                    title, cwd, inbox_message_id=message.message_id
                )
            except (FileNotFoundError, ValueError) as error:
                await self._admin_reply(message, f"无法创建对话：{error}", "new-thread-invalid")
                self.db.complete_incoming(message.message_id)
                return
            reply = f"已创建 Codex 对话 `{binding.thread_id}`。" + (
                "对应飞书群也已创建。" if binding.chat_id else "已登记，待对话机器人配对后创建群。"
            )
            await self._admin_reply(message, reply, "new-thread")
            self.db.complete_incoming(message.message_id)
            return
        self.db.merge_held_attachments(message)
        self.db.mark_incoming_queued(message.message_id)
        progress_id = await self.gateway.send_card(
            message.app_role,
            message.chat_id,
            progress_card("私聊临时对话", "已收到，正在用一个不保留上下文的 Codex 对话处理。"),
            idempotency_key=f"admin-progress:{message.message_id}",
        )
        await self._admin_queue.put(
            ScheduledMessage(
                inbox=item,
                binding=None,
                progress_message_id=progress_id,
                app_role=message.app_role,
                chat_id=message.chat_id,
            )
        )

    async def _admin_worker(self) -> None:
        while not self._stop.is_set():
            job = await self._admin_queue.get()
            thread_id: str | None = None
            message = job.inbox.message
            try:
                runtime = self._runtime_settings("admin")
                # Creating even the context-free helper thread is an
                # irreversible RPC.  Cross the durable ambiguity boundary
                # before calling it so a restart cannot create duplicates.
                self.db.mark_incoming_dispatching(message.message_id)
                thread = await self.codex.start_thread(
                    cwd=str(self.config.admin_scratch_dir),
                    approval_policy=runtime.approval_policy,
                    sandbox=runtime.sandbox,
                    model=runtime.model,
                    service_tier=runtime.service_tier,
                    ephemeral=False,
                )
                thread_id = str(thread["id"])
                self.db.set_setting(f"exclude_thread:{thread_id}", "1")
                await self.codex.set_thread_name(
                    thread_id, f"飞行桥临时任务-{message.message_id[-8:]}"
                )
                inputs = await self.artifacts.prepare_inputs(message)
                self._pending_jobs[thread_id] = job
                turn = await self.codex.start_turn(
                    thread_id,
                    inputs,
                    client_message_id=message.message_id,
                    approval_policy=runtime.approval_policy,
                    sandbox=runtime.sandbox,
                    model=runtime.model,
                    effort=runtime.effort,
                    service_tier=runtime.service_tier,
                )
                turn_id = str(turn["id"])
                completed_already = (
                    turn_id in self._completed_turns
                    or self._turn_done.get(turn_id, asyncio.Event()).is_set()
                )
                self.db.upsert_turn_job(
                    TurnJob(
                        message_id=message.message_id,
                        thread_id=thread_id,
                        turn_id=turn_id,
                        app_role=job.app_role,
                        chat_id=job.chat_id,
                        progress_message_id=job.progress_message_id,
                        state="completed" if completed_already else "accepted",
                    )
                )
                if completed_already:
                    self.db.complete_incoming(message.message_id)
                    self._pending_jobs.pop(thread_id, None)
                    continue
                active = self._active_by_turn.get(turn_id)
                if not active:
                    active = ActiveTurn(
                        thread_id=thread_id,
                        turn_id=turn_id,
                        chat_id=job.chat_id,
                        app_role=job.app_role,
                        progress_message_id=job.progress_message_id,
                        started_monotonic=time.monotonic(),
                    )
                    self._register_active(active)
                self.db.complete_incoming(message.message_id)
                self._pending_jobs.pop(thread_id, None)
                await self._turn_done.setdefault(turn_id, asyncio.Event()).wait()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                LOG.exception("Admin ephemeral task failed")
                state = self.db.inbox_state(message.message_id)
                if state == "dispatching":
                    self._record_incoming_failure(job.inbox, error)
                elif state in {"processing", "queued"}:
                    self.db.fail_incoming(message.message_id, str(error), retry_after_seconds=15)
                await self._patch_job_error(job, f"管理员临时任务失败：{error}")
            finally:
                if thread_id:
                    self._pending_jobs.pop(thread_id, None)
                self._admin_queue.task_done()

    def _is_admin_scratch_thread(self, thread: ThreadSummary) -> bool:
        try:
            return Path(thread.cwd).expanduser().resolve(strict=False) == (
                self.config.admin_scratch_dir.expanduser().resolve(strict=False)
            )
        except (OSError, RuntimeError):
            return False

    async def _admin_reply(self, message: IncomingMessage, text: str, kind: str) -> None:
        await self.gateway.send_text(
            message.app_role,
            message.chat_id,
            text,
            idempotency_key=f"admin:{kind}:{message.message_id}",
        )

    async def _progress_loop(self) -> None:
        while not self._stop.is_set():
            await asyncio.sleep(
                max(
                    0.25,
                    min(
                        self.config.progress_update_seconds,
                        self.config.progress_steady_update_seconds,
                    ),
                )
            )
            if self._stop.is_set():
                return
            for active in list(self._active_by_turn.values()):
                if not active.progress_message_id:
                    continue
                now = time.monotonic()
                last_event = active.last_event_monotonic or active.started_monotonic
                if (
                    now - last_event >= self.config.progress_stale_seconds
                    and now >= self._active_audit_next.get(active.turn_id, 0.0)
                    and active.turn_id not in self._auditing_turns
                ):
                    self._active_audit_next[active.turn_id] = now + 60.0
                    self._background_task(
                        self._audit_stale_active_turn(active),
                        f"stale-turn-audit:{active.turn_id}",
                    )
                if now < active.progress_retry_monotonic:
                    continue
                minimum_interval = self._progress_interval(active, now)
                if (
                    active.last_progress_monotonic
                    and now - active.last_progress_monotonic < minimum_interval
                ):
                    continue
                rendered = self._render_progress(active, now=now)
                if rendered == active.last_progress_text:
                    continue
                try:
                    patched = await self._patch_active_progress(
                        active,
                        progress_card("Codex 执行中", rendered),
                        terminal=False,
                    )
                    if patched:
                        active.last_progress_text = rendered
                        active.last_progress_monotonic = time.monotonic()
                        active.progress_failures = 0
                        active.progress_retry_monotonic = 0.0
                except Exception as error:
                    active.progress_failures += 1
                    delay = min(300, 2 ** min(active.progress_failures, 8))
                    active.progress_retry_monotonic = time.monotonic() + delay
                    if (
                        active.progress_failures == 1
                        or (active.progress_failures & (active.progress_failures - 1)) == 0
                    ):
                        LOG.warning(
                            "Progress card update for turn ref=%s failed %d time(s); "
                            "retrying in %ds: %s",
                            _log_ref(active.turn_id),
                            active.progress_failures,
                            delay,
                            redact_log(error),
                        )

    async def _patch_active_progress(
        self,
        active: ActiveTurn,
        card: dict[str, Any],
        *,
        terminal: bool,
    ) -> bool:
        message_id = active.progress_message_id
        if not message_id:
            return False
        lock = self._progress_locks.setdefault(message_id, asyncio.Lock())
        async with lock:
            if not terminal and (
                message_id in self._terminal_progress_messages
                or active.turn_id in self._finalizing
                or active.turn_id in self._completed_turns
            ):
                return False
            await self.gateway.patch_card(active.app_role, message_id, card)
            if terminal:
                # Keep the terminal revision sticky. A progress PATCH that was
                # already waiting on this lock must never overwrite it with an
                # older "running" card after the turn has completed.
                self._terminal_progress_messages.add(message_id)
            return True

    def _progress_interval(self, active: ActiveTurn, now: float | None = None) -> float:
        current = time.monotonic() if now is None else now
        age = max(0.0, current - active.started_monotonic)
        if age < self.config.progress_initial_window_seconds:
            return max(0.25, self.config.progress_update_seconds)
        return max(0.25, self.config.progress_steady_update_seconds)

    def _render_progress(self, active: ActiveTurn, *, now: float | None = None) -> str:
        current = time.monotonic() if now is None else now
        elapsed = int(max(0, current - active.started_monotonic))
        heartbeat = max(1, int(self._progress_interval(active, current)))
        displayed_elapsed = elapsed - elapsed % heartbeat
        lines = [f"**已运行：** {displayed_elapsed // 60:02d}:{displayed_elapsed % 60:02d}"]
        last_event = active.last_event_monotonic or active.started_monotonic
        quiet = int(max(0, current - last_event))
        if quiet >= self.config.progress_stale_seconds:
            quiet_display = quiet - quiet % heartbeat
            lines.extend(
                [
                    "",
                    f"⚠️ Codex 已 {quiet_display // 60} 分 {quiet_display % 60} 秒"
                    "没有产生新事件；桥仍保持连接，未把等待误报为新进展。",
                ]
            )
        if active.commentary_text.strip():
            lines.extend(["", "**最新进展**", _redact(active.commentary_text.strip())[-3000:]])
        if active.current_operation:
            lines.extend(["", f"**当前操作：** {_redact(active.current_operation)[:300]}"])
        if active.plan:
            lines.extend(["", "**计划**"])
            icons = {"completed": "✅", "in_progress": "🔄", "pending": "▫️"}
            for step in active.plan[-12:]:
                lines.append(f"{icons.get(step.get('status'), '▫️')} {step.get('step', '')}")
        lines.extend(["", "可发送 `!steer 补充要求` 或 `!stop`。"])
        return "\n".join(lines)

    async def _on_codex_notification(self, message: dict[str, Any]) -> None:
        method = str(message.get("method") or "")
        params = message.get("params") or {}
        thread_id = str(params.get("threadId") or "")
        turn_raw = params.get("turn") or {}
        turn_id = str(params.get("turnId") or turn_raw.get("id") or "")
        if method == "thread/tokenUsage/updated" and thread_id:
            usage = params.get("tokenUsage") or {}
            last = usage.get("last") or {}
            input_tokens = int(last.get("inputTokens") or 0)
            self.db.set_setting(
                f"token_usage:{thread_id}:input",
                str(input_tokens),
            )
            self.db.set_setting(
                f"token_usage:{thread_id}:cached",
                str(int(last.get("cachedInputTokens") or 0)),
            )
            window = int(usage.get("modelContextWindow") or 0)
            if window:
                self.db.set_setting(f"token_usage:{thread_id}:window", str(window))
            active_usage = self._active_by_thread.get(thread_id)
            if active_usage:
                active_usage.last_event_monotonic = time.monotonic()
                active_usage.last_event_name = "token usage"
            return
        active = self._active_by_turn.get(turn_id) if turn_id else None
        if method == "turn/started":
            if not active:
                job = self._pending_jobs.get(thread_id)
                if job:
                    active = ActiveTurn(
                        thread_id=thread_id,
                        turn_id=turn_id,
                        chat_id=job.chat_id,
                        app_role=job.app_role,
                        progress_message_id=job.progress_message_id,
                        started_monotonic=time.monotonic(),
                    )
                    self._register_active(active)
                    self.db.upsert_turn_job(
                        TurnJob(
                            message_id=job.inbox.message.message_id,
                            thread_id=thread_id,
                            turn_id=turn_id,
                            app_role=job.app_role,
                            chat_id=job.chat_id,
                            progress_message_id=job.progress_message_id,
                            state="running",
                        )
                    )
                else:
                    recovery = self._pending_recoveries.get(thread_id)
                    if recovery:
                        active = ActiveTurn(
                            thread_id=thread_id,
                            turn_id=turn_id,
                            chat_id=recovery.chat_id,
                            app_role=recovery.app_role,
                            progress_message_id=recovery.progress_message_id,
                            started_monotonic=time.monotonic(),
                        )
                        self._register_active(active)
                        self.db.upsert_turn_job(
                            TurnJob(
                                message_id=recovery.message_id,
                                thread_id=thread_id,
                                turn_id=turn_id,
                                app_role=recovery.app_role,
                                chat_id=recovery.chat_id,
                                progress_message_id=recovery.progress_message_id,
                                state="running",
                            )
                        )
            return
        if method == "configWarning":
            warning = str(params.get("message") or params.get("detail") or params)
            key = _text_hash(warning)
            if key not in self._warned:
                self._warned.add(key)
                await self._notify_admin("⚠️ Codex 配置警告：" + warning[:2000])
            return
        if method == "turn/completed" and not active:
            job = self._pending_jobs.get(thread_id)
            if job:
                active = ActiveTurn(
                    thread_id=thread_id,
                    turn_id=turn_id,
                    chat_id=job.chat_id,
                    app_role=job.app_role,
                    progress_message_id=job.progress_message_id,
                    started_monotonic=time.monotonic(),
                )
                self._register_active(active)
                await self._finalize_turn(active, turn_raw)
            else:
                recovery = self._pending_recoveries.get(thread_id)
                if recovery:
                    active = ActiveTurn(
                        thread_id=thread_id,
                        turn_id=turn_id,
                        chat_id=recovery.chat_id,
                        app_role=recovery.app_role,
                        progress_message_id=recovery.progress_message_id,
                        started_monotonic=time.monotonic(),
                    )
                    self._register_active(active)
                    self.db.upsert_turn_job(
                        TurnJob(
                            message_id=recovery.message_id,
                            thread_id=thread_id,
                            turn_id=turn_id,
                            app_role=recovery.app_role,
                            chat_id=recovery.chat_id,
                            progress_message_id=recovery.progress_message_id,
                            state="running",
                        )
                    )
                    await self._finalize_turn(active, turn_raw)
            return
        if not active:
            return
        active.last_event_monotonic = time.monotonic()
        active.last_event_name = method
        if method == "item/started":
            item = params.get("item") or {}
            item_id = str(item.get("id") or "")
            item_type = item.get("type")
            if item_type == "agentMessage":
                active.item_phases[item_id] = item.get("phase")
            elif item_type == "commandExecution":
                active.current_operation = f"运行命令：{item.get('command', '')}"
            elif item_type == "fileChange":
                active.current_operation = "修改文件"
            elif item_type == "mcpToolCall":
                active.current_operation = (
                    f"调用工具：{item.get('server', '')}/{item.get('tool', '')}"
                )
            elif item_type == "webSearch":
                active.current_operation = "检索网页"
            elif item_type == "imageGeneration":
                active.current_operation = "生成图像"
            elif item_type == "imageView":
                active.current_operation = "查看图像"
            elif item_type == "dynamicToolCall":
                active.current_operation = f"调用动态工具：{item.get('tool', '')}"
            elif item_type == "contextCompaction":
                active.current_operation = "压缩长上下文"
        elif method == "item/agentMessage/delta":
            delta = str(params.get("delta") or "")
            phase = active.item_phases.get(str(params.get("itemId") or ""))
            if phase == "final_answer":
                active.final_text += delta
            else:
                active.commentary_text = (active.commentary_text + delta)[-12000:]
        elif method == "turn/plan/updated":
            active.plan = list(params.get("plan") or [])
        elif method == "turn/diff/updated":
            active.current_operation = "汇总文件更改"
        elif method == "item/completed":
            item = params.get("item") or {}
            item_type = item.get("type")
            if item_type == "agentMessage":
                text = str(item.get("text") or "")
                if item.get("phase") == "final_answer":
                    active.final_text = text
                elif text:
                    active.commentary_text = text[-12000:]
            elif item_type == "commandExecution":
                status = item.get("status")
                code = item.get("exitCode")
                active.current_operation = f"命令已{status}" + (
                    f"（退出码 {code}）" if code is not None else ""
                )
            elif item_type == "fileChange":
                active.current_operation = f"文件修改：{item.get('status', '完成')}"
            elif item_type == "imageGeneration" and item.get("savedPath"):
                active.artifact_paths.append(str(item["savedPath"]))
            elif item_type == "imageView":
                active.current_operation = "图像查看完成"
            elif item_type == "dynamicToolCall":
                active.current_operation = f"动态工具：{item.get('status', '完成')}"
            elif item_type == "contextCompaction":
                active.current_operation = "上下文压缩完成"
        elif method == "turn/completed":
            await self._finalize_turn(active, turn_raw)

    async def _finalize_turn(self, active: ActiveTurn, turn: dict[str, Any]) -> None:
        if active.turn_id in self._finalizing or active.turn_id in self._completed_turns:
            return
        self._finalizing.add(active.turn_id)
        finalized = False
        try:
            commentary, final = extract_agent_messages(turn)
            if final:
                active.final_text = final[-1]
            elif not active.final_text and commentary:
                active.final_text = commentary[-1]
            status = str(turn.get("status") or "completed")
            if not active.final_text:
                error = turn.get("error") or {}
                active.final_text = f"Codex 执行状态：{status}。" + (
                    f"\n错误：{error.get('message', error)}" if error else ""
                )
            color = (
                "green" if status == "completed" else "orange" if status == "interrupted" else "red"
            )
            if active.progress_message_id:
                terminal_card = progress_card(
                    "Codex 已完成" if status == "completed" else f"Codex：{status}",
                    self._render_progress(active),
                    color=color,
                )
                try:
                    await self._patch_active_progress(
                        active,
                        terminal_card,
                        terminal=True,
                    )
                except Exception:
                    LOG.warning(
                        "Terminal card update for turn ref=%s failed; queued durable retry",
                        _log_ref(active.turn_id),
                        exc_info=True,
                    )
                    self.db.enqueue_outbox(
                        OutboxItem(
                            outbox_key=f"terminal-card:{active.turn_id}",
                            app_role=active.app_role,
                            receive_id=active.chat_id,
                            receive_id_type="chat_id",
                            msg_type="card_patch",
                            content={
                                "message_id": active.progress_message_id,
                                "card": terminal_card,
                            },
                            group_key=f"terminal-card:{active.turn_id}",
                            sequence=0,
                        )
                    )
            safe_final = _redact(active.final_text)
            generated_paths = self.artifacts.generated_image_paths(active.artifact_paths)
            automatic_paths = [
                path
                for path in self.artifacts.outgoing_paths(active.final_text)
                if path not in generated_paths
            ]
            self._enqueue_outbound_result(
                app_role=active.app_role,
                chat_id=active.chat_id,
                text=safe_final,
                base_key=f"final:{active.turn_id}",
                thread_id=active.thread_id,
                turn_id=active.turn_id,
                artifact_paths=[*generated_paths, *automatic_paths],
            )
            self.db.set_turn_job_state(active.turn_id, "completed")
            if (
                active.app_role == "admin"
                or self.db.get_setting(f"exclude_thread:{active.thread_id}", "") == "1"
            ):
                with contextlib.suppress(Exception):
                    await self.codex.archive_thread(active.thread_id)
            finalized = True
        except Exception:
            LOG.exception("Failed finalizing Codex turn ref=%s", _log_ref(active.turn_id))
        finally:
            self._finalizing.discard(active.turn_id)
            if finalized:
                self._completed_turns.add(active.turn_id)
                self._turn_done.setdefault(active.turn_id, asyncio.Event()).set()
                self._active_by_turn.pop(active.turn_id, None)
                if self._active_by_thread.get(active.thread_id) is active:
                    self._active_by_thread.pop(active.thread_id, None)
                if active.progress_message_id:
                    self._terminal_progress_messages.discard(active.progress_message_id)
                    self._progress_locks.pop(active.progress_message_id, None)
                self._active_audit_next.pop(active.turn_id, None)

    def _request_artifact_approval(self, active: ActiveTurn, path: Path) -> None:
        approval_id = secrets.token_urlsafe(16)
        fingerprint = _file_fingerprint(path)
        artifact = PendingArtifact(
            approval_id=approval_id,
            thread_id=active.thread_id,
            turn_id=active.turn_id,
            app_role=active.app_role,
            chat_id=active.chat_id,
            path=path,
            sha256=fingerprint,
            size=path.stat().st_size,
        )
        self.db.add_artifact_approval(artifact)
        text = (
            "📎 Codex 准备发送一个普通文件，需你二次确认：\n"
            f"文件：`{path}`\n大小：{artifact.size} bytes\n"
            f"SHA-256：`{fingerprint}`\n\n"
            "模型可能生成或复制文件；请先在本机打开核对内容，确认不含隐私/凭据。\n\n"
            f"回复 `发送文件 {approval_id}` 或 `放弃文件 {approval_id}`。"
        )
        self.db.enqueue_outbox(
            OutboxItem(
                outbox_key=f"zz-artifact-approval:{active.turn_id}:{approval_id}",
                app_role=active.app_role,
                receive_id=active.chat_id,
                receive_id_type="chat_id",
                msg_type="text",
                content={"text": text},
                group_key=f"zz-artifact-approval:{active.turn_id}:{approval_id}",
                sequence=0,
            )
        )

    def _enqueue_outbound_result(
        self,
        *,
        app_role: AppRole,
        chat_id: str,
        text: str,
        base_key: str,
        thread_id: str | None,
        turn_id: str | None,
        artifact_paths: list[Path] | None = None,
    ) -> None:
        parts = _split_utf8(text, 100_000) or [""]
        for index, part in enumerate(parts):
            rendered = part
            if len(parts) > 1:
                rendered = f"[{index + 1}/{len(parts)}]\n{part}"
            is_last = index == len(parts) - 1
            self.db.enqueue_outbox(
                OutboxItem(
                    outbox_key=f"{base_key}:0:{index:04d}",
                    app_role=app_role,
                    receive_id=chat_id,
                    receive_id_type="chat_id",
                    msg_type="text",
                    content={"text": rendered},
                    group_key=base_key,
                    sequence=index,
                    thread_id=thread_id if is_last else None,
                    turn_id=turn_id if is_last else None,
                )
            )
        for index, path in enumerate(artifact_paths or []):
            fingerprint = _file_fingerprint(path)
            self.db.enqueue_outbox(
                OutboxItem(
                    outbox_key=(f"{base_key}:1:{index:04d}:{fingerprint[:20]}"),
                    app_role=app_role,
                    receive_id=chat_id,
                    receive_id_type="chat_id",
                    msg_type="local_file",
                    content={"path": str(path), "sha256": fingerprint},
                    group_key=base_key,
                    sequence=len(parts) + index,
                )
            )

    async def _on_codex_request(self, request: dict[str, Any]) -> None:
        method = str(request.get("method") or "")
        params = request.get("params") or {}
        rpc_id = str(request.get("id"))
        thread_id = str(params.get("threadId") or params.get("conversationId") or "")
        turn_id = str(params.get("turnId") or "") or None
        if method == "currentTime/read":
            await self.codex.respond_server_request(rpc_id, {"currentTimeAt": int(time.time())})
            return
        supported = {
            "item/commandExecution/requestApproval",
            "item/fileChange/requestApproval",
            "item/permissions/requestApproval",
            "item/tool/requestUserInput",
            "execCommandApproval",
            "applyPatchApproval",
        }
        active = self._active_by_thread.get(thread_id)
        binding = self.db.get_binding_by_thread(thread_id)
        if method not in supported:
            await self.codex.respond_server_error(
                rpc_id, -32601, f"unsupported bridge server request: {method}"
            )
            if active:
                await self.gateway.send_text(
                    active.app_role,
                    active.chat_id,
                    f"⚠️ 收到桥接器尚未实现的请求类型 `{method}`，已用协议错误安全终止该请求。",
                    idempotency_key=f"unknown-server-request:{rpc_id}",
                )
            return
        if active:
            role, chat_id = active.app_role, active.chat_id
        elif binding and binding.chat_id:
            role, chat_id = "conversation", binding.chat_id
        else:
            await self.codex.respond_server_request(rpc_id, self._deny_payload(method, params))
            return
        if method == "item/tool/requestUserInput" and any(
            bool(question.get("isSecret")) for question in params.get("questions") or []
        ):
            await self.codex.respond_server_request(rpc_id, {"answers": {}})
            await self.gateway.send_text(
                role,
                chat_id,
                "🔒 Codex 请求了秘密输入。桥接器不会让密码、Token 或 Secret 经过飞书，"
                "已返回空答案；如确有需要，请在本机受信终端完成。",
                idempotency_key=f"secret-question:{rpc_id}",
            )
            return
        short_id = secrets.token_urlsafe(16)
        approval = PendingApproval(
            short_id=short_id,
            rpc_id=rpc_id,
            method=method,
            thread_id=thread_id,
            turn_id=turn_id,
            chat_id=chat_id,
            params=params,
        )
        self.db.add_approval(approval)
        if method == "item/tool/requestUserInput":
            questions = params.get("questions") or []
            question_text = "\n".join(
                f"• **{q.get('header', '')}**：{q.get('question', '')}\n"
                + "  "
                + " / ".join(option.get("label", "") for option in q.get("options") or [])
                for q in questions
            )
            card = progress_card(
                "Codex 等待你的回答",
                question_text + f"\n\n回复：`回答 {short_id} 你的答案`",
                color="orange",
            )
        else:
            summary = self._approval_summary(method, params)
            card = _approval_card(short_id, summary)
        try:
            await self.gateway.send_card(
                role,
                chat_id,
                card,
                idempotency_key=f"approval:{rpc_id}",
            )
        except Exception:
            LOG.exception("Could not deliver approval ref=%s; denying", _log_ref(rpc_id))
            self.db.resolve_approval(short_id, "delivery_failed")
            await self.codex.respond_server_request(rpc_id, self._deny_payload(method, params))

    async def _try_approval_command(self, item: InboxItem) -> bool:
        text = item.message.text.strip()
        short_id = ""
        action = ""
        answer = ""
        internal = re.fullmatch(r"!approval\s+(\S+)\s+(allow_once|deny|cancel)", text)
        if internal:
            short_id, action = internal.group(1), internal.group(2)
        else:
            match = re.fullmatch(r"(?:批准|允许|allow)\s+(\S+)", text, re.IGNORECASE)
            if match:
                short_id, action = match.group(1), "allow_once"
            match = match or re.fullmatch(r"(?:拒绝|deny)\s+(\S+)", text, re.IGNORECASE)
            if match and not action:
                short_id, action = match.group(1), "deny"
            match = match or re.fullmatch(r"(?:取消|cancel)\s+(\S+)", text, re.IGNORECASE)
            if match and not action:
                short_id, action = match.group(1), "cancel"
            answer_match = re.fullmatch(
                r"(?:回答|answer)\s+(\S+)\s+(.+)", text, re.IGNORECASE | re.DOTALL
            )
            if answer_match:
                short_id, action, answer = (
                    answer_match.group(1),
                    "answer",
                    answer_match.group(2).strip(),
                )
        if not short_id:
            return False
        approval = self.db.get_approval(short_id, item.message.chat_id)
        if not approval or not self.codex.pending_server_request(approval.rpc_id):
            if approval:
                self.db.resolve_approval(short_id, "stale")
            await self.gateway.send_text(
                item.message.app_role,
                item.message.chat_id,
                "该审批不存在、已处理或已随服务重启失效。",
                idempotency_key=f"approval-stale:{item.message.message_id}",
            )
            self.db.complete_incoming(item.message.message_id)
            return True
        if approval.method == "item/tool/requestUserInput":
            if action != "answer":
                await self.gateway.send_text(
                    item.message.app_role,
                    item.message.chat_id,
                    f"这是问题而不是权限审批，请用 `回答 {short_id} ...`。",
                    idempotency_key=f"approval-needs-answer:{item.message.message_id}",
                )
                self.db.complete_incoming(item.message.message_id)
                return True
            result = self._answer_payload(approval.params, answer)
        elif action == "allow_once":
            result = self._allow_payload(approval.method, approval.params)
        else:
            result = self._deny_payload(approval.method, approval.params, cancel=action == "cancel")
        await self.codex.respond_server_request(approval.rpc_id, result)
        self.db.resolve_approval(short_id, action)
        self.db.complete_incoming(item.message.message_id)
        await self.gateway.send_text(
            item.message.app_role,
            item.message.chat_id,
            {
                "allow_once": "✅ 已允许一次。",
                "deny": "⛔ 已拒绝。",
                "cancel": "已取消。",
                "answer": "✅ 回答已提交。",
            }[action],
            idempotency_key=f"approval-result:{item.message.message_id}",
        )
        return True

    async def _try_artifact_command(self, item: InboxItem) -> bool:
        text = item.message.text.strip()
        approve = re.fullmatch(r"(?:发送文件|send-file)\s+(\S+)", text, re.IGNORECASE)
        reject = re.fullmatch(r"(?:放弃文件|drop-file)\s+(\S+)", text, re.IGNORECASE)
        match = approve or reject
        if not match:
            return False
        approval_id = match.group(1)
        artifact = self.db.get_artifact_approval(approval_id, item.message.chat_id)
        if not artifact:
            await self.gateway.send_text(
                item.message.app_role,
                item.message.chat_id,
                "该文件发送请求不存在、已处理或不属于这个会话。",
                idempotency_key=f"artifact-stale:{item.message.message_id}",
            )
            self.db.complete_incoming(item.message.message_id)
            return True
        if reject:
            self.db.resolve_artifact_approval(approval_id, "rejected")
            reply = "已放弃发送该文件。"
        else:
            try:
                self.artifacts.validate_outgoing(artifact.path)
                if _file_fingerprint(artifact.path) != artifact.sha256:
                    raise ValueError("文件内容在确认前发生变化")
                self.db.enqueue_outbox(
                    OutboxItem(
                        outbox_key=f"approved-artifact:{approval_id}",
                        app_role=artifact.app_role,
                        receive_id=artifact.chat_id,
                        receive_id_type="chat_id",
                        msg_type="local_file",
                        content={"path": str(artifact.path), "sha256": artifact.sha256},
                        group_key=f"approved-artifact:{approval_id}",
                        sequence=0,
                    )
                )
                self.db.resolve_artifact_approval(approval_id, "approved")
                reply = "✅ 文件已进入持久发送队列。"
            except Exception as error:
                self.db.resolve_artifact_approval(approval_id, "invalid")
                reply = f"⛔ 文件校验失败，未发送：{error}"
        self.db.complete_incoming(item.message.message_id)
        await self.gateway.send_text(
            item.message.app_role,
            item.message.chat_id,
            reply,
            idempotency_key=f"artifact-result:{item.message.message_id}",
        )
        return True

    @staticmethod
    def _allow_payload(method: str, params: dict[str, Any]) -> dict[str, Any]:
        if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
            return {"decision": "accept"}
        if method == "item/permissions/requestApproval":
            return {"permissions": params.get("permissions") or {}, "scope": "turn"}
        if method in {"execCommandApproval", "applyPatchApproval"}:
            return {"decision": "approved"}
        return {}

    @staticmethod
    def _deny_payload(
        method: str, params: dict[str, Any], *, cancel: bool = False
    ) -> dict[str, Any]:
        if method in {"item/commandExecution/requestApproval", "item/fileChange/requestApproval"}:
            return {"decision": "cancel" if cancel else "decline"}
        if method == "item/permissions/requestApproval":
            return {"permissions": {}, "scope": "turn"}
        if method == "item/tool/requestUserInput":
            return {"answers": {}}
        if method in {"execCommandApproval", "applyPatchApproval"}:
            return {"decision": "abort" if cancel else "denied"}
        return {"decision": "decline"}

    @staticmethod
    def _answer_payload(params: dict[str, Any], answer: str) -> dict[str, Any]:
        questions = params.get("questions") or []
        if not questions:
            return {"answers": {}}
        mapping: dict[str, dict[str, list[str]]] = {}
        explicit: dict[str, str] = {}
        for part in answer.split(";"):
            if "=" in part:
                key, value = part.split("=", 1)
                explicit[key.strip()] = value.strip()
        for index, question in enumerate(questions):
            question_id = str(question.get("id") or index)
            value = explicit.get(question_id, answer if len(questions) == 1 else "")
            mapping[question_id] = {"answers": [value] if value else []}
        return {"answers": mapping}

    @staticmethod
    def _approval_summary(method: str, params: dict[str, Any]) -> str:
        if method == "item/commandExecution/requestApproval":
            return (
                f"Codex 请求运行命令：\n`{_redact(str(params.get('command') or ''))[:1000]}`\n\n"
                f"目录：`{params.get('cwd') or ''}`\n\n原因：{params.get('reason') or '未说明'}"
            )
        if method == "item/fileChange/requestApproval":
            return f"Codex 请求写入额外位置。\n\n原因：{params.get('reason') or '未说明'}"
        if method == "item/permissions/requestApproval":
            permissions = json.dumps(params.get("permissions") or {}, ensure_ascii=False)[:3000]
            reason = params.get("reason") or "未说明"
            return f"Codex 请求本轮额外权限：\n```json\n{permissions}\n```\n原因：{reason}"
        return f"Codex 请求审批 `{method}`。\n原因：{params.get('reason') or '未说明'}"

    async def _patch_job_error(self, job: ScheduledMessage, text: str) -> None:
        with contextlib.suppress(Exception):
            await self.gateway.patch_card(
                job.app_role,
                job.progress_message_id,
                progress_card("Codex 未能启动", text, color="red"),
            )

    def _register_active(self, active: ActiveTurn) -> None:
        if not active.started_monotonic:
            active.started_monotonic = time.monotonic()
        if not active.last_event_monotonic:
            active.last_event_monotonic = time.monotonic()
        if not active.last_event_name:
            active.last_event_name = "turn/started"
        self._active_by_thread[active.thread_id] = active
        self._active_by_turn[active.turn_id] = active
        self._turn_done.setdefault(active.turn_id, asyncio.Event())

    def _current_active_turn(self, thread_id: str) -> ActiveTurn | None:
        active = self._active_by_thread.get(thread_id)
        if not active:
            return None
        done = self._turn_done.get(active.turn_id)
        if not done or not done.is_set():
            return active
        LOG.warning(
            "Discarding stale completed turn ref=%s from thread ref=%s",
            _log_ref(active.turn_id),
            _log_ref(thread_id),
        )
        if self._active_by_turn.get(active.turn_id) is active:
            self._active_by_turn.pop(active.turn_id, None)
        if self._active_by_thread.get(thread_id) is active:
            self._active_by_thread.pop(thread_id, None)
        return None

    def _owner(self, role: AppRole) -> str:
        stored = self.db.get_setting(f"owner_open_id:{role}", "") or ""
        if stored:
            return stored
        if role == "admin":
            return self.config.feishu.owner_admin_open_id
        return self.config.feishu.owner_conversation_open_id

    async def _notify_admin(self, text: str) -> None:
        role: AppRole = "conversation"
        owner = self._owner(role)
        if not owner or not self.gateway.configured(role):
            LOG.warning("Codex private notification not delivered ref=%s", _log_ref(text))
            return
        with contextlib.suppress(Exception):
            await self.gateway.send_text(
                role,
                owner,
                text,
                receive_id_type="open_id",
                idempotency_key=f"admin-alert:{_text_hash(text)}:{int(time.time()) // 60}",
            )

    async def _notify_admin_card(self, card: dict[str, Any], *, idempotency_key: str) -> None:
        role: AppRole = "conversation"
        owner = self._owner(role)
        if not owner or not self.gateway.configured(role):
            LOG.warning("Codex private card notification not delivered")
            return
        with contextlib.suppress(Exception):
            await self.gateway.send_card(
                role,
                owner,
                card,
                receive_id_type="open_id",
                idempotency_key=idempotency_key,
            )

    def _allowed_workspace(self, path: Path) -> bool:
        for root in self.config.allowed_workspace_roots:
            try:
                path.relative_to(root.expanduser().resolve())
                return True
            except ValueError:
                continue
        return False


def _selected_model(models: list[dict[str, Any]], selected_id: str | None) -> dict[str, Any] | None:
    if selected_id:
        selected = next(
            (
                model
                for model in models
                if selected_id in {str(model.get("model") or ""), str(model.get("id") or "")}
            ),
            None,
        )
        if selected:
            return selected
    return next(
        (model for model in models if model.get("isDefault")), models[0] if models else None
    )


def _model_efforts(model: dict[str, Any]) -> list[str]:
    return [
        str(option.get("reasoningEffort"))
        for option in model.get("supportedReasoningEfforts") or []
        if option.get("reasoningEffort")
    ]


def _permission_label(settings: RuntimeSettings) -> str:
    return {
        ("never", "danger-full-access"): "Full Access (YOLO)",
        ("on-request", "workspace-write"): "Default",
        ("on-request", "read-only"): "Read Only",
    }.get((settings.approval_policy, settings.sandbox), "Custom")


def _setting_button(text: str, value: dict[str, str], *, selected: bool = False) -> dict[str, Any]:
    button: dict[str, Any] = {
        "tag": "button",
        "text": {"tag": "plain_text", "content": text[:80]},
        "value": {"kind": "codex_setting", **value},
    }
    if selected:
        button["type"] = "primary"
    return {"tag": "action", "actions": [button]}


def _compatibility_repair_card(version: str, baseline: str, error: str) -> dict[str, Any]:
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Codex CLI 设置兼容待确认"},
        },
        "elements": [
            {
                "tag": "markdown",
                "content": (
                    f"{error}\n\n当前版本：`{version}`  ·  内置基线：`{baseline}`\n\n"
                    "检测会创建一个仅驻留内存的临时 Codex thread，实际验证模型目录和设置协议，"
                    "完成后立即解除订阅；"
                    "只有全部通过才更新本机验证基线。"
                ),
            },
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "type": "primary",
                        "text": {"tag": "plain_text", "content": "检测并修复"},
                        "value": {
                            "kind": "codex_compatibility",
                            "action": "repair",
                            "version": version,
                        },
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "暂不处理"},
                        "value": {
                            "kind": "codex_compatibility",
                            "action": "dismiss",
                            "version": version,
                        },
                    },
                ],
            },
        ],
    }


def _model_picker_card(models: list[dict[str, Any]], selected_id: str | None) -> dict[str, Any]:
    selected = _selected_model(models, selected_id)
    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": "与 Codex CLI 的 `/model` 选择器一致；选中模型后继续选择推理强度。",
        }
    ]
    for model in models[:12]:
        model_id = str(model.get("model") or model.get("id") or "")
        if not model_id:
            continue
        name = str(model.get("displayName") or model_id)
        default_effort = str(model.get("defaultReasoningEffort") or "default")
        fast = any(
            str(tier.get("id") or "") == "priority" for tier in model.get("serviceTiers") or []
        )
        suffix = f" · {default_effort}" + (" · Fast" if fast else "")
        elements.append(
            _setting_button(
                f"{name}{suffix}",
                {"setting": "model", "model": model_id},
                selected=model is selected,
            )
        )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "Choose model"},
        },
        "elements": elements,
    }


def _reasoning_picker_card(
    model_id: str, efforts: list[str], default_effort: str, suffix: str
) -> dict[str, Any]:
    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": (f"模型已选择 `{model_id}`。请选择 reasoning effort。" + suffix),
        },
        _setting_button(
            f"Model default ({default_effort or 'default'})",
            {"setting": "model", "model": model_id, "effort": "default"},
        ),
    ]
    elements.extend(
        _setting_button(
            effort,
            {"setting": "model", "model": model_id, "effort": effort},
            selected=effort == default_effort,
        )
        for effort in efforts
    )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "blue",
            "title": {"tag": "plain_text", "content": "Choose reasoning effort"},
        },
        "elements": elements,
    }


def _permissions_picker_card(
    settings: RuntimeSettings, *, allow_full_access: bool = False
) -> dict[str, Any]:
    selected = _permission_label(settings)
    choices = [
        (
            "Read Only",
            "read-only",
            "读取工作区；编辑文件或联网前需要确认。",
        ),
        (
            "Default",
            "default",
            "可在工作区读写并运行命令；联网或修改其他位置前需要确认。",
        ),
    ]
    if allow_full_access:
        choices.append(
            (
                "Full Access",
                "full-access",
                "不询问即可访问工作区外文件和网络；仅限显式启用的隔离主机。",
            )
        )
    elements: list[dict[str, Any]] = [
        {
            "tag": "markdown",
            "content": "与 Codex CLI 的 `/permissions` 预设选择器一致。",
        }
    ]
    for label, profile, description in choices:
        elements.append({"tag": "markdown", "content": f"**{label}** — {description}"})
        elements.append(
            _setting_button(
                label,
                {"setting": "permissions", "profile": profile},
                selected=selected.startswith(label),
            )
        )
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Choose permissions"},
        },
        "elements": elements,
    }


def _approval_card(short_id: str, summary: str) -> dict[str, Any]:
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": "orange",
            "title": {"tag": "plain_text", "content": "Codex 等待审批"},
        },
        "elements": [
            {"tag": "markdown", "content": _redact(summary)[:22000]},
            {
                "tag": "action",
                "actions": [
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "允许一次"},
                        "type": "primary",
                        "value": {
                            "kind": "codex_approval",
                            "short_id": short_id,
                            "decision": "allow_once",
                        },
                    },
                    {
                        "tag": "button",
                        "text": {"tag": "plain_text", "content": "拒绝"},
                        "type": "danger",
                        "value": {
                            "kind": "codex_approval",
                            "short_id": short_id,
                            "decision": "deny",
                        },
                    },
                ],
            },
            {
                "tag": "note",
                "elements": [
                    {
                        "tag": "plain_text",
                        "content": f"按钮不可用时回复：批准 {short_id} 或 拒绝 {short_id}",
                    }
                ],
            },
        ],
    }


def _redact(text: str) -> str:
    patterns = [
        r"(?i)(--?(?:token|secret|password|api[_-]?key)(?:=|\s+))([^\s]+)",
        (
            r"(?i)((?:authorization\s*[:=]\s*(?:bearer\s+)?|bearer\s+))"
            r"([^\s]+)"
        ),
        (
            r"(?i)(\b[A-Z0-9_]*(?:TOKEN|SECRET|PASSWORD|API_KEY)"
            r"\s*[:=]\s*)([^\s,;]+)"
        ),
        r"(?i)\b(sk-[A-Za-z0-9_-]{16,})\b",
    ]
    result = text
    for index, pattern in enumerate(patterns):
        replacement = "[已隐藏]" if index == len(patterns) - 1 else r"\1[已隐藏]"
        result = re.sub(pattern, replacement, result)
    return result


def _text_hash(text: str) -> str:
    return hashlib.sha256(text.encode()).hexdigest()


def _file_fingerprint(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _split_utf8(text: str, max_bytes: int) -> list[str]:
    if not text:
        return []
    parts: list[str] = []
    remaining = text
    while remaining:
        encoded = remaining.encode("utf-8")
        if len(encoded) <= max_bytes:
            parts.append(remaining)
            break
        chunk = encoded[:max_bytes].decode("utf-8", errors="ignore")
        # Prefer a nearby newline so Markdown is less likely to be cut in the
        # middle of a paragraph, while always making forward progress.
        split_at = chunk.rfind("\n", max(0, len(chunk) - 4000))
        if split_at > 0:
            chunk = chunk[:split_at]
        parts.append(chunk)
        remaining = remaining[len(chunk) :].lstrip("\n")
    return parts
