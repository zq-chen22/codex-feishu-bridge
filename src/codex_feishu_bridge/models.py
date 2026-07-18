from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

AppRole = Literal["admin", "conversation"]


@dataclass(slots=True)
class ThreadSummary:
    thread_id: str
    name: str | None
    preview: str
    cwd: str
    created_at: int
    updated_at: int
    source_kind: str | None = None
    ephemeral: bool = False
    parent_thread_id: str | None = None
    status: dict[str, Any] = field(default_factory=dict)

    @property
    def display_name(self) -> str:
        if self.name and self.name.strip():
            return self.name.strip()
        first = next((line.strip() for line in self.preview.splitlines() if line.strip()), "")
        return first[:42] if first else f"Codex-{self.thread_id[:8]}"


@dataclass(slots=True)
class Binding:
    thread_id: str
    title: str
    cwd: str
    chat_id: str | None = None
    app_role: AppRole = "conversation"
    thread_created_at: int = 0
    thread_updated_at: int = 0
    last_synced_turn_id: str | None = None
    last_synced_message_hash: str | None = None
    sync_state: str = "pending"
    active: bool = True


@dataclass(slots=True)
class Attachment:
    kind: Literal["image", "file", "media", "audio"]
    name: str
    message_id: str
    file_key: str | None = None
    image_key: str | None = None
    local_path: Path | None = None
    mime_type: str | None = None
    size: int | None = None


@dataclass(slots=True)
class IncomingMessage:
    message_id: str
    chat_id: str
    chat_type: str
    app_role: AppRole
    sender_open_id: str | None
    sender_user_id: str | None
    sender_union_id: str | None
    text: str
    message_type: str
    create_time_ms: int
    tenant_key: str | None = None
    app_id: str | None = None
    sender_type: str = "user"
    attachments: list[Attachment] = field(default_factory=list)


@dataclass(slots=True)
class InboxItem:
    message: IncomingMessage
    state: str = "pending"
    attempts: int = 0
    last_error: str | None = None


@dataclass(slots=True)
class OutboxItem:
    outbox_key: str
    app_role: AppRole
    receive_id: str
    receive_id_type: str
    msg_type: str
    content: dict[str, Any]
    group_key: str = ""
    sequence: int = 0
    thread_id: str | None = None
    turn_id: str | None = None
    attempts: int = 0


@dataclass(slots=True)
class TurnJob:
    message_id: str
    thread_id: str
    turn_id: str
    app_role: AppRole
    chat_id: str
    progress_message_id: str | None
    state: str
    created_at: int = 0


@dataclass(slots=True)
class PendingArtifact:
    approval_id: str
    thread_id: str
    turn_id: str
    app_role: AppRole
    chat_id: str
    path: Path
    sha256: str
    size: int
    state: str = "pending"


@dataclass(slots=True)
class PendingApproval:
    short_id: str
    rpc_id: str
    method: str
    thread_id: str
    turn_id: str | None
    chat_id: str
    params: dict[str, Any]
    state: str = "pending"


@dataclass(slots=True)
class ActiveTurn:
    thread_id: str
    turn_id: str
    chat_id: str
    app_role: AppRole = "conversation"
    progress_message_id: str | None = None
    final_text: str = ""
    commentary_text: str = ""
    started_monotonic: float = 0.0
    last_event_monotonic: float = 0.0
    last_event_name: str = ""
    last_progress_monotonic: float = 0.0
    last_progress_text: str = ""
    progress_failures: int = 0
    progress_retry_monotonic: float = 0.0
    plan: list[dict[str, Any]] = field(default_factory=list)
    current_operation: str = ""
    diff: str = ""
    item_phases: dict[str, str | None] = field(default_factory=dict)
    artifact_paths: list[str] = field(default_factory=list)
