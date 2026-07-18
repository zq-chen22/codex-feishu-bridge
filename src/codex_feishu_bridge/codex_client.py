from __future__ import annotations

import asyncio
import contextlib
import copy
import json
import logging
import os
import re
from collections.abc import Awaitable, Callable, Iterable
from pathlib import Path
from typing import Any

from .models import ThreadSummary
from .privacy import log_ref, redact_log

LOG = logging.getLogger(__name__)
# A single image-generation completion notification can legitimately contain
# several megabytes of inline image data.  The bridge no longer requests full
# image history, but it still has to accept the live JSONL notification.  Keep
# enough headroom for multi-image turns so asyncio's line reader does not tear
# down an otherwise healthy app-server connection.
DEFAULT_STREAM_LIMIT_BYTES = 128 * 1024 * 1024
Message = dict[str, Any]
NotificationHandler = Callable[[Message], Awaitable[None]]
ServerRequestHandler = Callable[[Message], Awaitable[None]]


class CodexRPCError(RuntimeError):
    def __init__(self, method: str, error: Any):
        super().__init__(f"Codex app-server {method} failed: {error}")
        self.method = method
        self.error = error


class CodexAppServer:
    """Async JSONL client for ``codex app-server --listen stdio://``.

    The protocol deliberately omits the JSON-RPC ``jsonrpc`` field. Responses,
    notifications, and server-initiated approval requests share stdout, so a
    single reader task dispatches all three without ever blocking on handlers.
    """

    def __init__(
        self,
        codex_bin: str = "codex",
        *,
        cwd: Path | None = None,
        env: dict[str, str] | None = None,
        request_timeout: float = 60.0,
        stream_limit_bytes: int = DEFAULT_STREAM_LIMIT_BYTES,
    ) -> None:
        self.codex_bin = codex_bin
        self.cwd = cwd
        self.env = env
        self.request_timeout = request_timeout
        self.stream_limit_bytes = stream_limit_bytes
        self.process: asyncio.subprocess.Process | None = None
        self._next_id = 1
        self._pending: dict[str, tuple[str, asyncio.Future[Message]]] = {}
        self._server_requests: dict[str, Message] = {}
        self._notification_handlers: list[NotificationHandler] = []
        self._server_request_handler: ServerRequestHandler | None = None
        self._reader_task: asyncio.Task[None] | None = None
        self._notification_task: asyncio.Task[None] | None = None
        self._notification_queue: asyncio.Queue[Message] | None = None
        self._notification_queue_high_water = 0
        self._stderr_task: asyncio.Task[None] | None = None
        self._handler_tasks: set[asyncio.Task[None]] = set()
        self._write_lock = asyncio.Lock()
        self._closed = asyncio.Event()
        self._resumed_threads: set[str] = set()
        self._thread_config_overrides: dict[str, Any] = {}
        self.initialize_result: Message = {}
        self.cli_version: str | None = None

    async def __aenter__(self) -> CodexAppServer:
        await self.start()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    def add_notification_handler(self, handler: NotificationHandler) -> None:
        self._notification_handlers.append(handler)

    def set_server_request_handler(self, handler: ServerRequestHandler) -> None:
        self._server_request_handler = handler

    def configure_thread_defaults(self, *, config_overrides: dict[str, Any] | None = None) -> None:
        """Apply bridge-owned config to every subsequent start and resume."""

        self._thread_config_overrides = copy.deepcopy(config_overrides or {})
        # Existing app-server subscriptions retain the config they were
        # resumed with. Force the next turn to resume again after a change.
        self._resumed_threads.clear()

    async def start(self) -> None:
        if self.process and self.process.returncode is None:
            return
        proc_env = os.environ.copy()
        if self.env:
            proc_env.update(self.env)
        self.process = await asyncio.create_subprocess_exec(
            self.codex_bin,
            "app-server",
            "--listen",
            "stdio://",
            stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=str(self.cwd) if self.cwd else None,
            env=proc_env,
            limit=self.stream_limit_bytes,
        )
        self._closed.clear()
        self._notification_queue = asyncio.Queue()
        self._notification_queue_high_water = 0
        self._notification_task = asyncio.create_task(
            self._dispatch_notifications(), name="codex-app-server-notifications"
        )
        self._reader_task = asyncio.create_task(self._read_stdout(), name="codex-app-server-reader")
        self._stderr_task = asyncio.create_task(self._read_stderr(), name="codex-app-server-stderr")
        self.initialize_result = await self.request(
            "initialize",
            {
                "clientInfo": {
                    "name": "codex_feishu_bridge",
                    "title": "Codex Feishu Bridge",
                    "version": "0.1.0",
                },
                "capabilities": {
                    "experimentalApi": True,
                    "requestAttestation": False,
                },
            },
        )
        user_agent = str(self.initialize_result.get("userAgent") or "")
        match = re.search(r"/(\d+\.\d+\.\d+)(?:\s|$)", user_agent)
        self.cli_version = match.group(1) if match else None
        await self.notify("initialized", {})

    async def close(self) -> None:
        proc = self.process
        if not proc:
            return
        if proc.stdin:
            proc.stdin.close()
            with contextlib.suppress(Exception):
                await proc.stdin.wait_closed()
        try:
            await asyncio.wait_for(proc.wait(), timeout=3)
        except TimeoutError:
            proc.terminate()
            try:
                await asyncio.wait_for(proc.wait(), timeout=3)
            except TimeoutError:
                proc.kill()
                await proc.wait()
        for task in (
            self._reader_task,
            self._notification_task,
            self._stderr_task,
            *tuple(self._handler_tasks),
        ):
            if task and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self._fail_pending(RuntimeError("Codex app-server closed"))
        self.process = None
        self._resumed_threads.clear()
        self._server_requests.clear()
        self._handler_tasks.clear()
        self._notification_queue = None
        self._closed.set()

    async def request(
        self,
        method: str,
        params: dict[str, Any] | None = None,
        *,
        timeout: float | None = None,
    ) -> Message:
        loop = asyncio.get_running_loop()
        request_id = str(self._next_id)
        self._next_id += 1
        future: asyncio.Future[Message] = loop.create_future()
        self._pending[request_id] = (method, future)
        payload: Message = {"id": int(request_id), "method": method}
        if params is not None:
            payload["params"] = params
        try:
            await self._send(payload)
            result = await asyncio.wait_for(
                future, timeout=self.request_timeout if timeout is None else timeout
            )
        except BaseException:
            self._pending.pop(request_id, None)
            raise
        if "error" in result and result["error"] is not None:
            raise CodexRPCError(method, result["error"])
        return result.get("result", {})

    async def notify(self, method: str, params: dict[str, Any] | None = None) -> None:
        payload: Message = {"method": method}
        if params is not None:
            payload["params"] = params
        await self._send(payload)

    async def respond_server_request(self, rpc_id: str | int, result: dict[str, Any]) -> None:
        key = str(rpc_id)
        if key not in self._server_requests:
            raise KeyError(f"Codex server request {key} is no longer pending")
        await self._send({"id": int(key) if key.isdigit() else key, "result": result})
        self._server_requests.pop(key, None)

    async def respond_server_error(self, rpc_id: str | int, code: int, message: str) -> None:
        key = str(rpc_id)
        if key not in self._server_requests:
            return
        await self._send(
            {
                "id": int(key) if key.isdigit() else key,
                "error": {"code": code, "message": message},
            }
        )
        self._server_requests.pop(key, None)

    def pending_server_request(self, rpc_id: str | int) -> Message | None:
        return self._server_requests.get(str(rpc_id))

    async def wait_closed(self) -> None:
        await self._closed.wait()

    async def list_threads(
        self,
        *,
        limit: int = 100,
        source_kinds: Iterable[str] | None = None,
        sort_key: str = "recency_at",
        archived: bool = False,
    ) -> list[ThreadSummary]:
        data: list[ThreadSummary] = []
        cursor: str | None = None
        while len(data) < limit:
            params: dict[str, Any] = {
                "limit": min(100, limit - len(data)),
                "sortKey": sort_key,
                "sortDirection": "desc",
                "archived": archived,
            }
            if source_kinds is not None:
                params["sourceKinds"] = list(source_kinds)
            if cursor:
                params["cursor"] = cursor
            page = await self.request("thread/list", params)
            for raw in page.get("data", []):
                thread = _thread_summary(raw)
                if thread.ephemeral or thread.parent_thread_id:
                    continue
                data.append(thread)
                if len(data) >= limit:
                    break
            cursor = page.get("nextCursor")
            if not cursor:
                break
        return data

    async def read_thread(self, thread_id: str, *, include_turns: bool = True) -> Message:
        result = await self.request(
            "thread/read", {"threadId": thread_id, "includeTurns": include_turns}
        )
        return result.get("thread", {})

    async def list_turns(
        self,
        thread_id: str,
        *,
        limit: int = 100,
        items_view: str = "summary",
        sort_direction: str = "desc",
        cursor: str | None = None,
    ) -> Message:
        """Read a bounded turn page without materializing large image payloads."""

        params: Message = {
            "threadId": thread_id,
            "limit": limit,
            "itemsView": items_view,
            "sortDirection": sort_direction,
        }
        if cursor:
            params["cursor"] = cursor
        return await self.request("thread/turns/list", params)

    async def resume_thread(
        self,
        thread_id: str,
        *,
        approval_policy: str | None = None,
        sandbox: str | None = None,
        model: str | None = None,
        service_tier: str | None = None,
        cwd: str | None = None,
        exclude_turns: bool = True,
    ) -> Message:
        params: dict[str, Any] = {
            "threadId": thread_id,
            "excludeTurns": exclude_turns,
            "approvalsReviewer": "user",
        }
        if approval_policy:
            params["approvalPolicy"] = approval_policy
        if sandbox:
            params["sandbox"] = sandbox
        if model:
            params["model"] = model
        if service_tier:
            params["serviceTier"] = service_tier
        if cwd:
            params["cwd"] = cwd
        if self._thread_config_overrides:
            params["config"] = copy.deepcopy(self._thread_config_overrides)
        result = await self.request("thread/resume", params)
        self._resumed_threads.add(thread_id)
        return result

    async def ensure_resumed(
        self,
        thread_id: str,
        *,
        approval_policy: str,
        sandbox: str,
        model: str | None = None,
        service_tier: str | None = None,
    ) -> None:
        if thread_id not in self._resumed_threads:
            await self.resume_thread(
                thread_id,
                approval_policy=approval_policy,
                sandbox=sandbox,
                model=model,
                service_tier=service_tier,
                exclude_turns=True,
            )

    async def start_thread(
        self,
        *,
        cwd: str,
        approval_policy: str,
        sandbox: str,
        model: str | None = None,
        service_tier: str | None = None,
        ephemeral: bool = False,
    ) -> Message:
        params: Message = {
            "cwd": cwd,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": "user",
            "sandbox": sandbox,
            "ephemeral": ephemeral,
            "serviceName": "codex-feishu-bridge",
        }
        if model:
            params["model"] = model
        if service_tier:
            params["serviceTier"] = service_tier
        if self._thread_config_overrides:
            params["config"] = copy.deepcopy(self._thread_config_overrides)
        result = await self.request("thread/start", params)
        thread = result.get("thread", {})
        if thread.get("id"):
            self._resumed_threads.add(thread["id"])
        return thread

    async def set_thread_name(self, thread_id: str, name: str) -> None:
        await self.request("thread/name/set", {"threadId": thread_id, "name": name})

    async def archive_thread(self, thread_id: str) -> None:
        await self.request("thread/archive", {"threadId": thread_id})

    async def unsubscribe_thread(self, thread_id: str) -> None:
        await self.request("thread/unsubscribe", {"threadId": thread_id})
        self._resumed_threads.discard(thread_id)

    async def start_turn(
        self,
        thread_id: str,
        inputs: list[dict[str, Any]],
        *,
        client_message_id: str | None = None,
        approval_policy: str,
        sandbox: str,
        model: str | None = None,
        effort: str | None = None,
        service_tier: str | None = None,
    ) -> Message:
        await self.ensure_resumed(
            thread_id,
            approval_policy=approval_policy,
            sandbox=sandbox,
            model=model,
            service_tier=service_tier,
        )
        params: dict[str, Any] = {
            "threadId": thread_id,
            "input": inputs,
            "approvalPolicy": approval_policy,
            "approvalsReviewer": "user",
            "sandboxPolicy": _sandbox_policy(sandbox),
        }
        # These settings are sticky in app-server.  Explicit null clears a
        # previous per-thread override and restores the configured default.
        params["model"] = model
        params["effort"] = effort
        params["serviceTier"] = service_tier
        if client_message_id:
            params["clientUserMessageId"] = client_message_id
        result = await self.request("turn/start", params)
        return result.get("turn", {})

    async def update_thread_settings(
        self,
        thread_id: str,
        *,
        approval_policy: str,
        sandbox: str,
        model: str | None,
        effort: str | None,
        service_tier: str | None,
    ) -> Message:
        return await self.request(
            "thread/settings/update",
            {
                "threadId": thread_id,
                "approvalPolicy": approval_policy,
                "approvalsReviewer": "user",
                "sandboxPolicy": _sandbox_policy(sandbox),
                "model": model,
                "effort": effort,
                "serviceTier": service_tier,
            },
        )

    async def list_models(self) -> list[Message]:
        models: list[Message] = []
        cursor: str | None = None
        while True:
            params: Message = {"limit": 100, "includeHidden": False}
            if cursor:
                params["cursor"] = cursor
            page = await self.request("model/list", params)
            models.extend(page.get("data") or [])
            cursor = page.get("nextCursor")
            if not cursor:
                return models

    async def read_config(self, *, cwd: str | None = None) -> Message:
        params: Message = {"includeLayers": False}
        if cwd:
            params["cwd"] = cwd
        result = await self.request("config/read", params)
        return result.get("config", {})

    async def steer_turn(
        self,
        thread_id: str,
        turn_id: str,
        inputs: list[dict[str, Any]],
        *,
        client_message_id: str | None = None,
    ) -> str:
        params: dict[str, Any] = {
            "threadId": thread_id,
            "expectedTurnId": turn_id,
            "input": inputs,
        }
        if client_message_id:
            params["clientUserMessageId"] = client_message_id
        result = await self.request("turn/steer", params)
        return str(result.get("turnId", turn_id))

    async def interrupt_turn(self, thread_id: str, turn_id: str) -> None:
        await self.request("turn/interrupt", {"threadId": thread_id, "turnId": turn_id})

    async def quota(self) -> dict[str, Any]:
        rate_limits, usage = await asyncio.gather(
            self.request("account/rateLimits/read"),
            self.request("account/usage/read"),
        )
        return {"rate_limits": rate_limits, "usage": usage}

    async def _send(self, payload: Message) -> None:
        proc = self.process
        if not proc or proc.returncode is not None or not proc.stdin:
            raise RuntimeError("Codex app-server is not running")
        encoded = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        async with self._write_lock:
            proc.stdin.write(encoded)
            await proc.stdin.drain()

    async def _read_stdout(self) -> None:
        process = self.process
        if process is None or process.stdout is None:
            raise RuntimeError("Codex app-server stdout is unavailable")
        try:
            while line := await process.stdout.readline():
                try:
                    message: Message = json.loads(line)
                except json.JSONDecodeError:
                    LOG.warning(
                        "Ignoring malformed app-server line bytes=%d ref=%s",
                        len(line),
                        log_ref(line.decode(errors="replace")),
                    )
                    continue
                request_id = message.get("id")
                method = message.get("method")
                if request_id is not None and method:
                    key = str(request_id)
                    self._server_requests[key] = message
                    if self._server_request_handler:
                        self._spawn_handler(
                            self._dispatch_server_request(message), f"server-request:{method}"
                        )
                    else:
                        LOG.warning("No handler for Codex server request %s", method)
                    continue
                if request_id is not None:
                    pending = self._pending.pop(str(request_id), None)
                    if pending:
                        _, future = pending
                        if not future.done():
                            future.set_result(message)
                    else:
                        LOG.debug(
                            "Unmatched app-server response ref=%s",
                            log_ref(str(request_id)),
                        )
                    continue
                if method:
                    # stdout is the protocol's only ordering boundary. Keep
                    # reading responses here so notification handlers may make
                    # nested RPCs without deadlocking, but dispatch every
                    # notification through one FIFO consumer. Spawning one
                    # task per line allowed turn/completed to overtake the
                    # final item or even turn/started.
                    queue = self._notification_queue
                    if queue is not None:
                        queue.put_nowait(self._slim_notification(message))
                        depth = queue.qsize()
                        if depth >= max(32, self._notification_queue_high_water * 2):
                            self._notification_queue_high_water = depth
                            LOG.warning(
                                "Codex notification queue reached %d item(s); "
                                "a downstream handler is slow",
                                depth,
                            )
        except asyncio.CancelledError:
            raise
        except Exception:
            LOG.exception("Codex app-server stdout reader failed")
        finally:
            self._fail_pending(RuntimeError("Codex app-server stdout closed"))
            self._closed.set()

    async def _read_stderr(self) -> None:
        process = self.process
        if process is None or process.stderr is None:
            raise RuntimeError("Codex app-server stderr is unavailable")
        try:
            while line := await process.stderr.readline():
                text = line.decode(errors="replace").rstrip()
                if text:
                    LOG.warning("codex app-server: %s", redact_log(text))
        except asyncio.CancelledError:
            raise

    async def _dispatch_notifications(self) -> None:
        queue = self._notification_queue
        if queue is None:
            raise RuntimeError("Codex notification queue is unavailable")
        try:
            while True:
                message = await queue.get()
                try:
                    for handler in tuple(self._notification_handlers):
                        try:
                            await handler(message)
                        except asyncio.CancelledError:
                            raise
                        except Exception:
                            LOG.exception(
                                "notification:%s failed",
                                message.get("method") or "unknown",
                            )
                finally:
                    queue.task_done()
        except asyncio.CancelledError:
            raise

    @staticmethod
    def _slim_notification(message: Message) -> Message:
        """Drop persisted tool payloads that no bridge handler consumes.

        Image-generation results can contain multi-megabyte base64 strings in
        both item notifications and the completed turn. The image is already
        durably available through ``savedPath``; retaining the inline result
        while a slower Feishu handler runs only multiplies memory usage.
        """

        params = message.get("params")
        if not isinstance(params, dict):
            return message

        def slim_item(item: Any) -> None:
            if not isinstance(item, dict):
                return
            item_type = str(item.get("type") or "")
            if item_type in {"imageGeneration", "image_generation_call"}:
                item.pop("result", None)
            elif item_type == "commandExecution":
                item.pop("aggregatedOutput", None)
            elif item_type in {"mcpToolCall", "dynamicToolCall", "imageView"}:
                for key in ("result", "output", "content", "data"):
                    item.pop(key, None)

        slim_item(params.get("item"))
        turn = params.get("turn")
        if isinstance(turn, dict):
            for item in turn.get("items") or []:
                slim_item(item)
        return message

    async def _dispatch_server_request(self, message: Message) -> None:
        handler = self._server_request_handler
        if handler is None:
            raise RuntimeError("Codex server request handler is unavailable")
        try:
            await handler(message)
        except Exception as error:
            LOG.exception("Codex server request handler failed")
            with contextlib.suppress(Exception):
                await self.respond_server_error(
                    message.get("id", ""), -32603, f"bridge handler failed: {type(error).__name__}"
                )

    def _spawn_handler(self, awaitable: Awaitable[None], name: str) -> None:
        task = asyncio.create_task(awaitable, name=name)
        self._handler_tasks.add(task)

        def done(completed: asyncio.Task[None]) -> None:
            self._handler_tasks.discard(completed)
            with contextlib.suppress(asyncio.CancelledError):
                error = completed.exception()
                if error:
                    LOG.error(
                        "%s failed: %s",
                        name,
                        error,
                        exc_info=(type(error), error, error.__traceback__),
                    )

        task.add_done_callback(done)

    def _fail_pending(self, error: Exception) -> None:
        for _, future in self._pending.values():
            if not future.done():
                future.set_exception(error)
        self._pending.clear()


def _sandbox_policy(sandbox: str) -> Message:
    if sandbox == "danger-full-access":
        return {"type": "dangerFullAccess"}
    if sandbox == "read-only":
        return {"type": "readOnly", "networkAccess": False}
    return {
        "type": "workspaceWrite",
        "writableRoots": [],
        "networkAccess": False,
        "excludeSlashTmp": False,
        "excludeTmpdirEnvVar": False,
    }


def _thread_summary(raw: Message) -> ThreadSummary:
    source = raw.get("source")
    if isinstance(source, dict):
        source_kind = source.get("kind") or source.get("type")
    else:
        source_kind = raw.get("sourceKind") or source
    return ThreadSummary(
        thread_id=str(raw.get("id", "")),
        name=raw.get("name"),
        preview=str(raw.get("preview") or ""),
        cwd=str(raw.get("cwd") or ""),
        created_at=int(raw.get("createdAt") or 0),
        updated_at=int(raw.get("recencyAt") or raw.get("updatedAt") or raw.get("createdAt") or 0),
        source_kind=str(source_kind) if source_kind else None,
        ephemeral=bool(raw.get("ephemeral", False)),
        parent_thread_id=raw.get("parentThreadId"),
        status=raw.get("status") or {},
    )


def extract_agent_messages(turn: Message) -> tuple[list[str], list[str]]:
    commentary: list[str] = []
    final: list[str] = []
    for item in turn.get("items") or []:
        if item.get("type") != "agentMessage":
            continue
        text = str(item.get("text") or "").strip()
        if not text:
            continue
        if item.get("phase") == "final_answer":
            final.append(text)
        else:
            commentary.append(text)
    return commentary, final


def extract_user_messages(turn: Message) -> list[str]:
    messages: list[str] = []
    for item in turn.get("items") or []:
        if item.get("type") != "userMessage":
            continue
        parts: list[str] = []
        content = item.get("content")
        if isinstance(content, str):
            parts.append(content)
        elif isinstance(content, list):
            for part in content:
                if isinstance(part, str):
                    parts.append(part)
                elif isinstance(part, dict):
                    text = part.get("text")
                    if isinstance(text, str):
                        parts.append(text)
        direct_text = item.get("text")
        if not parts and isinstance(direct_text, str):
            parts.append(direct_text)
        message = "\n".join(part.strip() for part in parts if part.strip()).strip()
        if message:
            messages.append(message)
    return messages


def latest_final_from_thread(thread: Message) -> tuple[str | None, str]:
    turns = thread.get("turns") or []
    for turn in reversed(turns):
        _, final = extract_agent_messages(turn)
        if final:
            return str(turn.get("id") or ""), final[-1]
        commentary, _ = extract_agent_messages(turn)
        if commentary and turn.get("status") in {"completed", "interrupted", "failed"}:
            return str(turn.get("id") or ""), commentary[-1]
    return None, ""
