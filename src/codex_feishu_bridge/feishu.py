from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import multiprocessing
import re
import secrets
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import lark_oapi as lark
from lark_oapi.api.im.v1 import (
    CreateChatRequest,
    CreateChatRequestBody,
    CreateFileRequest,
    CreateFileRequestBody,
    CreateImageRequest,
    CreateImageRequestBody,
    CreateMessageRequest,
    CreateMessageRequestBody,
    GetMessageResourceRequest,
    ListChatRequest,
    ListMessageRequest,
    PatchMessageRequest,
    PatchMessageRequestBody,
    UpdateChatRequest,
    UpdateChatRequestBody,
)
from lark_oapi.event.callback.model.p2_card_action_trigger import (
    P2CardActionTrigger,
    P2CardActionTriggerResponse,
)

from .config import BridgeConfig, FeishuAppConfig
from .db import BridgeDB
from .models import AppRole, Attachment, IncomingMessage

LOG = logging.getLogger(__name__)
ReceiveIdType = Literal["chat_id", "open_id"]


class FeishuAPIError(RuntimeError):
    pass


def deterministic_uuid(namespace: str, value: str) -> str:
    return f"cfb-{hashlib.sha256(f'{namespace}:{value}'.encode()).hexdigest()[:40]}"


def conversation_group_name(title: str, suffix: str) -> str:
    clean = re.sub(r"[\r\n\t]+", " ", title).strip() or "Codex 对话"
    max_title = max(1, 60 - len(suffix))
    return f"{clean[:max_title]}{suffix}"


def conversation_binding_marker(thread_id: str) -> str:
    digest = hashlib.sha256(f"thread:{thread_id}".encode()).hexdigest()[:32]
    return f"feixing-binding:{digest}"


def conversation_group_description(thread_id: str, cwd: str, created_at: int) -> str:
    timestamp = created_at / 1000 if created_at > 10_000_000_000 else created_at
    if timestamp > 0:
        started = (
            datetime.fromtimestamp(timestamp, tz=UTC)
            .astimezone()
            .isoformat(sep=" ", timespec="seconds")
        )
    else:
        started = "未知"
    return f"本地工作区：已隐藏\n开始：{started}\n{conversation_binding_marker(thread_id)}"


def progress_card(title: str, text: str, *, color: str = "blue") -> dict[str, Any]:
    # Classic card JSON is supported by both current desktop and mobile clients.
    return {
        "config": {"wide_screen_mode": True, "update_multi": True},
        "header": {
            "template": color,
            "title": {"tag": "plain_text", "content": title[:80]},
        },
        "elements": [{"tag": "markdown", "content": _utf8_tail(text, 24000)}],
    }


class FeishuGateway:
    """REST sender plus a WS child for the unified Codex Feishu app.

    lark-oapi 1.7.1 owns a module-global asyncio loop for its WebSocket
    client.  A child process keeps its callback path independent from Codex
    work.
    """

    def __init__(self, config: BridgeConfig, db: BridgeDB):
        self.config = config
        self.db = db
        self._clients: dict[AppRole, Any] = {}
        self._receivers: dict[AppRole, multiprocessing.Process] = {}
        for role, app in self._apps().items():
            if app.configured:
                self._clients[role] = (
                    lark.Client.builder()
                    .app_id(app.app_id)
                    .app_secret(app.secret())
                    .log_level(lark.LogLevel.WARNING)
                    .build()
                )

    def _apps(self) -> dict[AppRole, FeishuAppConfig]:
        return {"conversation": self.config.feishu.conversation}

    def configured(self, role: AppRole) -> bool:
        return role in self._clients

    def start_receivers(self) -> None:
        self.ensure_receivers()

    def ensure_receivers(self) -> list[AppRole]:
        """Start missing/dead WS children and return the roles restarted."""

        restarted: list[AppRole] = []
        ctx = multiprocessing.get_context("spawn")
        for role, app in self._apps().items():
            if not app.configured:
                continue
            previous = self._receivers.get(role)
            if previous and previous.is_alive():
                continue
            if previous:
                previous.join(timeout=0.2)
            process = ctx.Process(
                target=_ws_process_main,
                name=f"feishu-ws-{role}",
                args=(role, app.app_id, app.secret(), str(self.db.path)),
                daemon=True,
            )
            process.start()
            self._receivers[role] = process
            restarted.append(role)
        return restarted

    def stop_receivers(self) -> None:
        for process in self._receivers.values():
            if process.is_alive():
                process.terminate()
        for process in self._receivers.values():
            process.join(timeout=3)
            if process.is_alive():
                process.kill()
                process.join(timeout=1)
        self._receivers.clear()

    def receiver_status(self) -> dict[str, bool]:
        return {role: process.is_alive() for role, process in self._receivers.items()}

    async def send_text(
        self,
        role: AppRole,
        receive_id: str,
        text: str,
        *,
        receive_id_type: ReceiveIdType = "chat_id",
        idempotency_key: str | None = None,
    ) -> str:
        return await self.send_message(
            role,
            receive_id,
            "text",
            {"text": _utf8_head(text, 120000)},
            receive_id_type=receive_id_type,
            idempotency_key=idempotency_key,
        )

    async def send_card(
        self,
        role: AppRole,
        receive_id: str,
        card: dict[str, Any],
        *,
        receive_id_type: ReceiveIdType = "chat_id",
        idempotency_key: str | None = None,
    ) -> str:
        return await self.send_message(
            role,
            receive_id,
            "interactive",
            card,
            receive_id_type=receive_id_type,
            idempotency_key=idempotency_key,
        )

    async def send_message(
        self,
        role: AppRole,
        receive_id: str,
        msg_type: str,
        content: dict[str, Any],
        *,
        receive_id_type: ReceiveIdType = "chat_id",
        idempotency_key: str | None = None,
    ) -> str:
        client = self._client(role)
        key = idempotency_key or secrets.token_hex(16)
        body = (
            CreateMessageRequestBody.builder()
            .receive_id(receive_id)
            .msg_type(msg_type)
            .content(json.dumps(content, ensure_ascii=False, separators=(",", ":")))
            .uuid(deterministic_uuid("message", key))
            .build()
        )
        request = (
            CreateMessageRequest.builder()
            .receive_id_type(receive_id_type)
            .request_body(body)
            .build()
        )
        response = await self._api_call(
            role, f"message.send.{msg_type}", client.im.v1.message.acreate(request)
        )
        message_id = getattr(response.data, "message_id", None)
        if not message_id:
            raise FeishuAPIError("Feishu send succeeded without a message_id")
        return str(message_id)

    async def patch_card(self, role: AppRole, message_id: str, card: dict[str, Any]) -> None:
        request = (
            PatchMessageRequest.builder()
            .message_id(message_id)
            .request_body(
                PatchMessageRequestBody.builder()
                .content(json.dumps(card, ensure_ascii=False, separators=(",", ":")))
                .build()
            )
            .build()
        )
        await self._api_call(
            role,
            "message.card.patch",
            self._client(role).im.v1.message.apatch(request),
        )

    async def create_conversation_chat(
        self,
        thread_id: str,
        title: str,
        owner_open_id: str,
        cwd: str,
        created_at: int,
    ) -> tuple[str, str]:
        if not owner_open_id:
            raise FeishuAPIError("conversation app owner_open_id is not paired")
        name = conversation_group_name(title, self.config.group_suffix)
        body = (
            CreateChatRequestBody.builder()
            .name(name)
            .description(conversation_group_description(thread_id, cwd, created_at))
            .owner_id(owner_open_id)
            .user_id_list([owner_open_id])
            .group_message_type("chat")
            .chat_mode("group")
            .chat_type("private")
            .join_message_visibility("not_anyone")
            .leave_message_visibility("not_anyone")
            .membership_approval("approval_required")
            .edit_permission("only_owner")
            .build()
        )
        request = (
            CreateChatRequest.builder()
            .user_id_type("open_id")
            .set_bot_manager(True)
            .uuid(deterministic_uuid("chat", thread_id))
            .request_body(body)
            .build()
        )
        response = await self._api_call(
            "conversation",
            "chat.create",
            self._client("conversation").im.v1.chat.acreate(request),
        )
        chat_id = getattr(response.data, "chat_id", None)
        if not chat_id:
            raise FeishuAPIError("Feishu chat creation succeeded without a chat_id")
        return str(chat_id), name

    async def update_conversation_chat_description(
        self, chat_id: str, thread_id: str, cwd: str, created_at: int
    ) -> None:
        request = (
            UpdateChatRequest.builder()
            .user_id_type("open_id")
            .chat_id(chat_id)
            .request_body(
                UpdateChatRequestBody.builder()
                .description(conversation_group_description(thread_id, cwd, created_at))
                .build()
            )
            .build()
        )
        await self._api_call(
            "conversation",
            "chat.update",
            self._client("conversation").im.v1.chat.aupdate(request),
        )

    async def find_conversation_chat(
        self, thread_id: str, owner_open_id: str
    ) -> tuple[str, str] | None:
        markers = {
            conversation_binding_marker(thread_id),
            # Migration support for private deployments created before the
            # public release. New descriptions never expose the raw id.
            f"codex-thread:{thread_id}",
        }
        page_token: str | None = None
        while True:
            builder = ListChatRequest.builder().user_id_type("open_id").page_size(100)
            if page_token:
                builder = builder.page_token(page_token)
            response = await self._api_call(
                "conversation",
                "chat.list",
                self._client("conversation").im.v1.chat.alist(builder.build()),
            )
            for chat in response.data.items or []:
                if (
                    any(marker in str(chat.description or "") for marker in markers)
                    and str(chat.owner_id or "") == owner_open_id
                    and str(chat.chat_status or "normal") != "disbanded"
                ):
                    return str(chat.chat_id), str(chat.name or "")
            if not response.data.has_more or not response.data.page_token:
                return None
            page_token = str(response.data.page_token)

    async def download_attachment(self, role: AppRole, attachment: Attachment) -> tuple[bytes, str]:
        key = attachment.image_key if attachment.kind == "image" else attachment.file_key
        if not key:
            raise FeishuAPIError("attachment does not have a resource key")
        request = (
            GetMessageResourceRequest.builder()
            .message_id(attachment.message_id)
            .file_key(key)
            .type("image" if attachment.kind == "image" else "file")
            .build()
        )
        response = await self._api_call(
            role,
            "message.resource.download",
            self._client(role).im.v1.message_resource.aget(request),
        )
        if not response.file:
            raise FeishuAPIError("Feishu resource response did not contain a file")
        data = response.file.read(self.config.max_download_bytes + 1)
        if len(data) > self.config.max_download_bytes:
            raise FeishuAPIError("attachment exceeds configured download limit")
        return data, str(response.file_name or attachment.name or "attachment")

    async def list_chat_messages(
        self,
        role: AppRole,
        chat_id: str,
        *,
        start_time_seconds: int,
        end_time_seconds: int,
    ) -> list[IncomingMessage]:
        """Backfill a chat with a one-second overlap and message-id dedupe upstream."""

        result: list[IncomingMessage] = []
        page_token: str | None = None
        while True:
            builder = (
                ListMessageRequest.builder()
                .container_id_type("chat")
                .container_id(chat_id)
                .start_time(str(max(0, start_time_seconds)))
                .end_time(str(end_time_seconds))
                .sort_type("ByCreateTimeAsc")
                .page_size(50)
            )
            if page_token:
                builder = builder.page_token(page_token)
            response = await self._api_call(
                role,
                "message.history.list",
                self._client(role).im.v1.message.alist(builder.build()),
            )
            for item in response.data.items or []:
                try:
                    incoming = _normalize_history_message(item, role, self._apps()[role].app_id)
                except Exception:
                    LOG.warning("Ignoring malformed Feishu history item", exc_info=True)
                    continue
                if incoming.sender_type == "user":
                    result.append(incoming)
            if not response.data.has_more or not response.data.page_token:
                break
            page_token = str(response.data.page_token)
        result.sort(key=lambda value: (value.create_time_ms, value.message_id))
        return result

    async def upload_and_send(
        self,
        role: AppRole,
        chat_id: str,
        path: Path,
        *,
        idempotency_key: str,
    ) -> str:
        candidate = path.expanduser()
        if candidate.is_symlink():
            raise FeishuAPIError("symlinks cannot be uploaded")
        real = candidate.resolve(strict=True)
        if not real.is_file():
            raise FeishuAPIError("only regular, non-symlink files can be uploaded")
        if real.stat().st_size > min(self.config.max_upload_bytes, 30 * 1024 * 1024):
            raise FeishuAPIError("file exceeds Feishu/configured upload limit")
        client = self._client(role)
        if real.suffix.lower() in {".png", ".jpg", ".jpeg", ".gif", ".webp", ".bmp"}:
            if real.stat().st_size > 10 * 1024 * 1024:
                raise FeishuAPIError("image exceeds Feishu's 10 MiB upload limit")
            with real.open("rb") as handle:
                request = (
                    CreateImageRequest.builder()
                    .request_body(
                        CreateImageRequestBody.builder().image_type("message").image(handle).build()
                    )
                    .build()
                )
                response = await self._api_call(
                    role, "image.upload", client.im.v1.image.acreate(request)
                )
            return await self.send_message(
                role,
                chat_id,
                "image",
                {"image_key": response.data.image_key},
                idempotency_key=idempotency_key,
            )
        file_type = "mp4" if real.suffix.lower() == ".mp4" else "stream"
        with real.open("rb") as handle:
            request = (
                CreateFileRequest.builder()
                .request_body(
                    CreateFileRequestBody.builder()
                    .file_type(file_type)
                    .file_name(real.name)
                    .file(handle)
                    .build()
                )
                .build()
            )
            response = await self._api_call(role, "file.upload", client.im.v1.file.acreate(request))
        return await self.send_message(
            role,
            chat_id,
            "media" if file_type == "mp4" else "file",
            {"file_key": response.data.file_key},
            idempotency_key=idempotency_key,
        )

    def _client(self, role: AppRole) -> Any:
        try:
            return self._clients[role]
        except KeyError as error:
            raise FeishuAPIError(f"Feishu {role} app is not configured") from error

    async def _api_call(self, role: AppRole, operation: str, awaitable: Any) -> Any:
        with contextlib.suppress(Exception):
            self.db.record_api_attempt(role, operation)
        try:
            response = await awaitable
            self._require_success(response, operation)
        except BaseException:
            with contextlib.suppress(Exception):
                self.db.record_api_result(role, operation, success=False)
            raise
        with contextlib.suppress(Exception):
            self.db.record_api_result(role, operation, success=True)
        return response

    @staticmethod
    def _require_success(response: Any, operation: str) -> None:
        if response.success():
            return
        log_id = response.get_log_id() if hasattr(response, "get_log_id") else ""
        raise FeishuAPIError(
            f"Feishu {operation} failed: code={getattr(response, 'code', '?')} "
            f"msg={getattr(response, 'msg', '')} log_id={log_id}"
        )


def _ws_process_main(role: AppRole, app_id: str, app_secret: str, db_path: str) -> None:
    # Never log credentials.  Each child gets a fresh lark-oapi global loop.
    db = BridgeDB(Path(db_path))

    def on_message(data: Any) -> None:
        incoming = _normalize_message(data, role, app_id)
        if incoming.sender_type == "user":
            db.enqueue_incoming(incoming)

    def on_card(data: P2CardActionTrigger) -> P2CardActionTriggerResponse:
        incoming = _normalize_card_action(data, role, app_id)
        if incoming:
            db.enqueue_incoming(incoming)
            return P2CardActionTriggerResponse(
                {"toast": {"type": "success", "content": "已记录，正在校验并处理"}}
            )
        return P2CardActionTriggerResponse(
            {"toast": {"type": "warning", "content": "无法识别或已失效的操作"}}
        )

    handler = (
        lark.EventDispatcherHandler.builder("", "")
        .register_p2_im_message_receive_v1(on_message)
        .register_p2_card_action_trigger(on_card)
        .build()
    )
    client = lark.ws.Client(
        app_id,
        app_secret,
        event_handler=handler,
        log_level=lark.LogLevel.WARNING,
    )
    try:
        client.start()
    finally:
        db.close()


def _normalize_message(data: Any, role: AppRole, expected_app_id: str) -> IncomingMessage:
    header = data.header
    event = data.event
    message = event.message
    sender = event.sender
    sender_id = sender.sender_id
    if header.app_id and header.app_id != expected_app_id:
        raise ValueError("Feishu event app_id mismatch")
    try:
        content = json.loads(message.content or "{}")
    except json.JSONDecodeError:
        content = {}
    message_type = str(message.message_type or "text")
    text = _utf8_head(str(content.get("text") or ""), 120000) if message_type == "text" else ""
    for mention in message.mentions or []:
        key = getattr(mention, "key", None)
        if key:
            text = text.replace(str(key), "")
    text = text.strip()
    attachments: list[Attachment] = []
    if message_type == "image" and content.get("image_key"):
        attachments.append(
            Attachment(
                kind="image",
                name="image",
                message_id=str(message.message_id),
                image_key=str(content["image_key"]),
            )
        )
    elif message_type in {"file", "audio", "media"} and content.get("file_key"):
        kind = message_type if message_type in {"audio", "media"} else "file"
        attachments.append(
            Attachment(
                kind=kind,
                name=str(content.get("file_name") or message_type),
                message_id=str(message.message_id),
                file_key=str(content["file_key"]),
                image_key=str(content["image_key"]) if content.get("image_key") else None,
            )
        )
    return IncomingMessage(
        message_id=str(message.message_id),
        chat_id=str(message.chat_id),
        chat_type=str(message.chat_type or ""),
        app_role=role,
        sender_open_id=getattr(sender_id, "open_id", None),
        sender_user_id=getattr(sender_id, "user_id", None),
        sender_union_id=getattr(sender_id, "union_id", None),
        text=text,
        message_type=message_type,
        create_time_ms=int(message.create_time or 0),
        tenant_key=str(header.tenant_key or sender.tenant_key or ""),
        app_id=str(header.app_id or expected_app_id),
        sender_type=str(sender.sender_type or ""),
        attachments=attachments,
    )


def _normalize_card_action(
    data: P2CardActionTrigger, role: AppRole, expected_app_id: str
) -> IncomingMessage | None:
    header = data.header
    event = data.event
    action = event.action
    context = event.context
    operator = event.operator
    if header.app_id and header.app_id != expected_app_id:
        raise ValueError("Feishu card app_id mismatch")
    value = action.value or {}
    kind = value.get("kind")
    if kind == "codex_approval":
        short_id = str(value.get("short_id") or "")
        decision = str(value.get("decision") or "")
        if not short_id or decision not in {"allow_once", "deny", "cancel"}:
            return None
        command = f"!approval {short_id} {decision}"
    elif kind == "codex_setting":
        setting = str(value.get("setting") or "")
        if setting == "model":
            model = str(value.get("model") or "")
            effort = str(value.get("effort") or "")
            if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,127}", model):
                return None
            if effort and effort not in {
                "default",
                "low",
                "medium",
                "high",
                "xhigh",
                "max",
                "ultra",
            }:
                return None
            command = f"/model {model}" + (f" {effort}" if effort else "")
        elif setting == "permissions":
            profile = str(value.get("profile") or "")
            if profile not in {"read-only", "default", "full-access"}:
                return None
            command = f"/permissions {profile}"
        else:
            return None
    elif kind == "codex_compatibility":
        action_name = str(value.get("action") or "")
        version = str(value.get("version") or "")
        if action_name not in {"repair", "dismiss"}:
            return None
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9.+_-]{0,63}", version):
            return None
        command = f"/bridge-settings-compat {action_name} {version}"
    else:
        return None
    event_id = str(header.event_id or secrets.token_hex(16))
    return IncomingMessage(
        message_id=f"card:{event_id}",
        chat_id=str(context.open_chat_id or ""),
        chat_type="group",
        app_role=role,
        sender_open_id=operator.open_id,
        sender_user_id=operator.user_id,
        sender_union_id=operator.union_id,
        text=command,
        message_type="card_action",
        create_time_ms=int(getattr(header, "create_time", 0) or 0),
        tenant_key=str(operator.tenant_key or header.tenant_key or ""),
        app_id=str(header.app_id or expected_app_id),
        sender_type="user",
    )


def _normalize_history_message(item: Any, role: AppRole, app_id: str) -> IncomingMessage:
    if item is None or not getattr(item, "message_id", None):
        raise ValueError("history item has no message_id")
    message_type = str(item.msg_type or "text")
    try:
        content = json.loads(getattr(getattr(item, "body", None), "content", "") or "{}")
    except json.JSONDecodeError:
        content = {}
    text = _utf8_head(str(content.get("text") or ""), 120000) if message_type == "text" else ""
    for mention in item.mentions or []:
        if mention.key:
            text = text.replace(str(mention.key), "")
    attachments: list[Attachment] = []
    if message_type == "image" and content.get("image_key"):
        attachments.append(
            Attachment(
                kind="image",
                name="image",
                message_id=str(item.message_id),
                image_key=str(content["image_key"]),
            )
        )
    elif message_type in {"file", "audio", "media"} and content.get("file_key"):
        kind = message_type if message_type in {"audio", "media"} else "file"
        attachments.append(
            Attachment(
                kind=kind,
                name=str(content.get("file_name") or message_type),
                message_id=str(item.message_id),
                file_key=str(content["file_key"]),
                image_key=str(content["image_key"]) if content.get("image_key") else None,
            )
        )
    sender = getattr(item, "sender", None)
    sender_id = str(getattr(sender, "id", "") or "")
    id_type = str(getattr(sender, "id_type", "") or "")
    return IncomingMessage(
        message_id=str(item.message_id),
        chat_id=str(item.chat_id),
        chat_type="group",
        app_role=role,
        sender_open_id=sender_id if sender_id and id_type in {"open_id", ""} else None,
        sender_user_id=sender_id if sender_id and id_type == "user_id" else None,
        sender_union_id=sender_id if sender_id and id_type == "union_id" else None,
        text=text.strip(),
        message_type=message_type,
        create_time_ms=int(item.create_time or 0),
        tenant_key=str(getattr(sender, "tenant_key", "") or ""),
        app_id=app_id,
        sender_type=str(getattr(sender, "sender_type", "system") or "system"),
        attachments=attachments,
    )


def _utf8_head(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return encoded[:max_bytes].decode("utf-8", errors="ignore") + "\n\n[内容过长，已截断]"


def _utf8_tail(text: str, max_bytes: int) -> str:
    encoded = text.encode("utf-8")
    if len(encoded) <= max_bytes:
        return text
    return "[较早进展已折叠]\n\n" + encoded[-max_bytes:].decode("utf-8", errors="ignore")
