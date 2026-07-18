from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any

from .models import (
    Attachment,
    Binding,
    InboxItem,
    IncomingMessage,
    OutboxItem,
    PendingApproval,
    PendingArtifact,
    ThreadSummary,
    TurnJob,
)
from .privacy import redact_log

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS bindings (
    thread_id TEXT PRIMARY KEY,
    title TEXT NOT NULL,
    cwd TEXT NOT NULL,
    chat_id TEXT UNIQUE,
    app_role TEXT NOT NULL DEFAULT 'conversation',
    thread_created_at INTEGER NOT NULL DEFAULT 0,
    thread_updated_at INTEGER NOT NULL DEFAULT 0,
    last_synced_turn_id TEXT,
    last_synced_message_hash TEXT,
    sync_state TEXT NOT NULL DEFAULT 'pending',
    active INTEGER NOT NULL DEFAULT 1,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS processed_messages (
    message_id TEXT PRIMARY KEY,
    received_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS inbox_messages (
    message_id TEXT PRIMARY KEY,
    app_role TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    create_time_ms INTEGER NOT NULL,
    payload_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    lease_owner TEXT,
    lease_until INTEGER,
    last_error TEXT,
    received_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS thread_leases (
    thread_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL,
    expires_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS outbox_messages (
    outbox_key TEXT PRIMARY KEY,
    app_role TEXT NOT NULL,
    receive_id TEXT NOT NULL,
    receive_id_type TEXT NOT NULL DEFAULT 'chat_id',
    msg_type TEXT NOT NULL,
    content_json TEXT NOT NULL,
    group_key TEXT NOT NULL,
    sequence INTEGER NOT NULL DEFAULT 0,
    thread_id TEXT,
    turn_id TEXT,
    state TEXT NOT NULL DEFAULT 'pending',
    attempts INTEGER NOT NULL DEFAULT 0,
    available_at INTEGER NOT NULL,
    lease_owner TEXT,
    lease_until INTEGER,
    last_error TEXT,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS synced_turns (
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    synced_at INTEGER NOT NULL,
    PRIMARY KEY(thread_id, turn_id)
);

CREATE TABLE IF NOT EXISTS runtime_config_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scope TEXT NOT NULL,
    name TEXT NOT NULL,
    old_value TEXT,
    new_value TEXT NOT NULL,
    message_id TEXT NOT NULL,
    changed_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS turn_jobs (
    message_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL UNIQUE,
    app_role TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    progress_message_id TEXT,
    state TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS api_call_usage (
    period TEXT NOT NULL,
    app_role TEXT NOT NULL,
    operation TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    successes INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    updated_at INTEGER NOT NULL,
    PRIMARY KEY(period, app_role, operation)
);

CREATE TABLE IF NOT EXISTS artifact_approvals (
    approval_id TEXT PRIMARY KEY,
    thread_id TEXT NOT NULL,
    turn_id TEXT NOT NULL,
    app_role TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    path TEXT NOT NULL,
    sha256 TEXT NOT NULL,
    size INTEGER NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS pending_approvals (
    short_id TEXT PRIMARY KEY,
    rpc_id TEXT NOT NULL,
    method TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    turn_id TEXT,
    chat_id TEXT NOT NULL,
    params_json TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_bindings_chat ON bindings(chat_id);
CREATE INDEX IF NOT EXISTS idx_approvals_chat_state ON pending_approvals(chat_id, state);
CREATE INDEX IF NOT EXISTS idx_inbox_ready
    ON inbox_messages(state, available_at, create_time_ms);
CREATE INDEX IF NOT EXISTS idx_outbox_ready
    ON outbox_messages(state, available_at, created_at);
CREATE INDEX IF NOT EXISTS idx_turn_jobs_state ON turn_jobs(state, updated_at);
CREATE INDEX IF NOT EXISTS idx_artifact_approval_chat
    ON artifact_approvals(chat_id, state, created_at);
CREATE INDEX IF NOT EXISTS idx_runtime_config_events_scope
    ON runtime_config_events(scope, changed_at DESC, id DESC);
"""


class BridgeDB:
    def __init__(self, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.path = path
        self._conn = sqlite3.connect(path, check_same_thread=False, timeout=1.5)
        path.chmod(0o600)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.executescript(SCHEMA)
            binding_columns = {
                str(row[1]) for row in self._conn.execute("PRAGMA table_info(bindings)").fetchall()
            }
            if "thread_created_at" not in binding_columns:
                self._conn.execute(
                    "ALTER TABLE bindings ADD COLUMN thread_created_at INTEGER NOT NULL DEFAULT 0"
                )
            outbox_columns = {
                str(row[1])
                for row in self._conn.execute("PRAGMA table_info(outbox_messages)").fetchall()
            }
            if "group_key" not in outbox_columns:
                self._conn.execute(
                    "ALTER TABLE outbox_messages ADD COLUMN group_key TEXT NOT NULL DEFAULT ''"
                )
            if "sequence" not in outbox_columns:
                self._conn.execute(
                    "ALTER TABLE outbox_messages ADD COLUMN sequence INTEGER NOT NULL DEFAULT 0"
                )
            self._conn.execute("UPDATE outbox_messages SET group_key=outbox_key WHERE group_key=''")
            self._conn.execute("PRAGMA busy_timeout=1500")
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def get_setting(self, key: str, default: str | None = None) -> str | None:
        with self._lock:
            row = self._conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """INSERT INTO settings(key, value, updated_at) VALUES(?, ?, ?)
                   ON CONFLICT(key) DO UPDATE SET
                       value=excluded.value,
                       updated_at=excluded.updated_at""",
                (key, value, now),
            )
            self._conn.commit()

    def delete_setting(self, key: str) -> None:
        with self._lock:
            self._conn.execute("DELETE FROM settings WHERE key=?", (key,))
            self._conn.commit()

    def prune_retained_data(self, retention_days: int, *, now: int | None = None) -> dict[str, int]:
        """Delete expired operational records that may contain message content.

        Rows still needed for pending, held, retrying, or ambiguous work are
        deliberately retained. Idempotency markers and thread bindings do not
        contain message bodies and remain intact.
        """

        statements = {
            "inbox_messages": (
                "DELETE FROM inbox_messages WHERE state IN ('done', 'dead') AND updated_at < ?"
            ),
            "outbox_messages": (
                "DELETE FROM outbox_messages WHERE state IN ('done', 'dead') AND updated_at < ?"
            ),
            "runtime_config_events": ("DELETE FROM runtime_config_events WHERE changed_at < ?"),
            "turn_jobs": (
                "DELETE FROM turn_jobs "
                "WHERE state IN ('delivered', 'failed', 'interrupted') AND updated_at < ?"
            ),
            "artifact_approvals": (
                "DELETE FROM artifact_approvals WHERE state != 'pending' AND updated_at < ?"
            ),
            "pending_approvals": (
                "DELETE FROM pending_approvals WHERE state != 'pending' AND updated_at < ?"
            ),
        }
        counts = {table: 0 for table in statements}
        if retention_days == 0:
            return counts
        if retention_days < 0:
            raise ValueError("retention_days cannot be negative")
        current = int(time.time()) if now is None else int(now)
        cutoff = current - retention_days * 24 * 3600
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                for table, statement in statements.items():
                    cursor = self._conn.execute(statement, (cutoff,))
                    counts[table] = cursor.rowcount
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return counts

    def set_runtime_config(self, scope: str, name: str, value: str, *, message_id: str) -> None:
        key = f"runtime:{scope}:{name}"
        now = int(time.time())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT value FROM settings WHERE key=?", (key,)
                ).fetchone()
                old_value = str(row["value"]) if row else None
                self._conn.execute(
                    """INSERT INTO settings(key, value, updated_at) VALUES(?, ?, ?)
                       ON CONFLICT(key) DO UPDATE SET
                       value=excluded.value, updated_at=excluded.updated_at""",
                    (key, value, now),
                )
                self._conn.execute(
                    """INSERT INTO runtime_config_events(
                           scope, name, old_value, new_value, message_id, changed_at
                       ) VALUES(?, ?, ?, ?, ?, ?)""",
                    (scope, name, old_value, value, message_id, now),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise

    def runtime_config_history(self, scope: str, *, limit: int = 10) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT name, old_value, new_value, message_id, changed_at
                   FROM runtime_config_events WHERE scope=?
                   ORDER BY changed_at DESC, id DESC LIMIT ?""",
                (scope, max(1, limit)),
            ).fetchall()
        return [dict(row) for row in rows]

    def claim_message(self, message_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                "INSERT OR IGNORE INTO processed_messages(message_id, received_at) VALUES(?, ?)",
                (message_id, int(time.time())),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def enqueue_incoming(self, message: IncomingMessage) -> bool:
        """Durably accept an event before the Feishu callback returns.

        ``message_id`` is Feishu's documented idempotency key for message
        events.  A worker marks the row done only after it has handed the
        request to the per-thread scheduler, so an ACK followed by a process
        crash does not silently lose the request.
        """

        now = int(time.time())
        payload = self._incoming_payload(message)
        with self._lock:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO inbox_messages(
                       message_id, app_role, chat_id, create_time_ms, payload_json,
                       state, attempts, available_at, received_at, updated_at
                   ) VALUES(?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)""",
                (
                    message.message_id,
                    message.app_role,
                    message.chat_id,
                    message.create_time_ms,
                    json.dumps(payload, ensure_ascii=False),
                    now,
                    now,
                    now,
                ),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def last_incoming_create_time_ms(self, app_role: str, chat_id: str) -> int:
        with self._lock:
            row = self._conn.execute(
                """SELECT MAX(create_time_ms) AS latest FROM inbox_messages
                   WHERE app_role=? AND chat_id=?""",
                (app_role, chat_id),
            ).fetchone()
        return int(row["latest"] or 0) if row else 0

    def record_api_attempt(self, app_role: str, operation: str) -> None:
        now = int(time.time())
        period = time.strftime("%Y-%m", time.localtime(now))
        with self._lock:
            self._conn.execute(
                """INSERT INTO api_call_usage(
                       period, app_role, operation, attempts, successes, failures, updated_at
                   ) VALUES(?, ?, ?, 1, 0, 0, ?)
                   ON CONFLICT(period, app_role, operation) DO UPDATE SET
                       attempts=api_call_usage.attempts + 1,
                       updated_at=excluded.updated_at""",
                (period, app_role, operation, now),
            )
            self._conn.commit()

    def record_api_result(self, app_role: str, operation: str, *, success: bool) -> None:
        now = int(time.time())
        period = time.strftime("%Y-%m", time.localtime(now))
        if success:
            statement = (
                "UPDATE api_call_usage SET successes=successes + 1, updated_at=? "
                "WHERE period=? AND app_role=? AND operation=?"
            )
        else:
            statement = (
                "UPDATE api_call_usage SET failures=failures + 1, updated_at=? "
                "WHERE period=? AND app_role=? AND operation=?"
            )
        with self._lock:
            self._conn.execute(
                statement,
                (now, period, app_role, operation),
            )
            self._conn.commit()

    def api_usage(self, period: str | None = None) -> dict[str, int]:
        selected = period or time.strftime("%Y-%m", time.localtime())
        with self._lock:
            rows = self._conn.execute(
                """SELECT operation, SUM(attempts) AS attempts
                   FROM api_call_usage WHERE period=? GROUP BY operation""",
                (selected,),
            ).fetchall()
        return {str(row["operation"]): int(row["attempts"] or 0) for row in rows}

    @staticmethod
    def _incoming_payload(message: IncomingMessage) -> dict[str, Any]:
        return {
            "message_id": message.message_id,
            "chat_id": message.chat_id,
            "chat_type": message.chat_type,
            "app_role": message.app_role,
            "sender_open_id": message.sender_open_id,
            "sender_user_id": message.sender_user_id,
            "sender_union_id": message.sender_union_id,
            "text": message.text,
            "message_type": message.message_type,
            "create_time_ms": message.create_time_ms,
            "tenant_key": message.tenant_key,
            "app_id": message.app_id,
            "sender_type": message.sender_type,
            "attachments": [
                {
                    "kind": item.kind,
                    "name": item.name,
                    "message_id": item.message_id,
                    "file_key": item.file_key,
                    "image_key": item.image_key,
                    "local_path": str(item.local_path) if item.local_path else None,
                    "mime_type": item.mime_type,
                    "size": item.size,
                }
                for item in message.attachments
            ],
        }

    def hold_incoming_attachments(self, message: IncomingMessage) -> None:
        """Persist a media-only message until a later text task consumes it."""

        now = int(time.time())
        with self._lock:
            cur = self._conn.execute(
                """UPDATE inbox_messages SET payload_json=?, state='held',
                       lease_owner=NULL, lease_until=NULL, last_error=NULL, updated_at=?
                   WHERE message_id=? AND state='processing'""",
                (
                    json.dumps(self._incoming_payload(message), ensure_ascii=False),
                    now,
                    message.message_id,
                ),
            )
            self._conn.commit()
        if cur.rowcount != 1:
            raise RuntimeError(f"inbox message cannot be held: {message.message_id}")

    def merge_held_attachments(self, message: IncomingMessage) -> int:
        """Atomically attach earlier held media to a claimed text message.

        The merged payload is written into the current inbox row before held
        rows become done.  A process restart can therefore retry the text
        message without losing or duplicating its attachments.
        """

        now = int(time.time())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                rows = self._conn.execute(
                    """SELECT message_id, payload_json FROM inbox_messages
                       WHERE state='held' AND app_role=? AND chat_id=?
                         AND create_time_ms <= ?
                       ORDER BY create_time_ms, message_id""",
                    (message.app_role, message.chat_id, message.create_time_ms),
                ).fetchall()
                held_ids: list[str] = []
                attachments = list(message.attachments)
                seen = {
                    (item.message_id, item.kind, item.file_key, item.image_key)
                    for item in attachments
                }
                for row in rows:
                    held = self._incoming(json.loads(row["payload_json"]))
                    if held.sender_open_id != message.sender_open_id:
                        continue
                    held_ids.append(str(row["message_id"]))
                    for attachment in held.attachments:
                        key = (
                            attachment.message_id,
                            attachment.kind,
                            attachment.file_key,
                            attachment.image_key,
                        )
                        if key not in seen:
                            attachments.append(attachment)
                            seen.add(key)
                if not held_ids:
                    self._conn.commit()
                    return 0
                message.attachments = attachments
                current = self._conn.execute(
                    """UPDATE inbox_messages SET payload_json=?, updated_at=?
                       WHERE message_id=? AND state='processing'""",
                    (
                        json.dumps(self._incoming_payload(message), ensure_ascii=False),
                        now,
                        message.message_id,
                    ),
                )
                if current.rowcount != 1:
                    raise RuntimeError("current inbox message is not processing")
                for held_id in held_ids:
                    self._conn.execute(
                        """UPDATE inbox_messages SET state='done', lease_owner=NULL,
                           lease_until=NULL, last_error=NULL, updated_at=?
                           WHERE message_id=? AND state='held'""",
                        (now, held_id),
                    )
                    self._conn.execute(
                        """INSERT OR IGNORE INTO processed_messages(message_id, received_at)
                           VALUES(?, ?)""",
                        (held_id, now),
                    )
                self._conn.commit()
                return len(held_ids)
            except Exception:
                self._conn.rollback()
                raise

    def claim_incoming(self, worker_id: str, *, lease_seconds: int = 120) -> InboxItem | None:
        now = int(time.time())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """SELECT * FROM inbox_messages
                       WHERE state IN ('pending', 'retry') AND available_at <= ?
                       ORDER BY create_time_ms, message_id LIMIT 1""",
                    (now,),
                ).fetchone()
                if not row:
                    self._conn.commit()
                    return None
                self._conn.execute(
                    """UPDATE inbox_messages SET state='processing', attempts=attempts+1,
                       lease_owner=?, lease_until=?, updated_at=? WHERE message_id=?""",
                    (worker_id, now + lease_seconds, now, row["message_id"]),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return InboxItem(
            message=self._incoming(json.loads(row["payload_json"])),
            state="processing",
            attempts=int(row["attempts"]) + 1,
            last_error=row["last_error"],
        )

    def complete_incoming(self, message_id: str) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """UPDATE inbox_messages SET state='done', lease_owner=NULL,
                   lease_until=NULL, last_error=NULL, updated_at=? WHERE message_id=?""",
                (now, message_id),
            )
            self._conn.execute(
                "INSERT OR IGNORE INTO processed_messages(message_id, received_at) VALUES(?, ?)",
                (message_id, now),
            )
            self._conn.commit()

    def mark_incoming_dispatching(self, message_id: str) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE inbox_messages SET state='dispatching', lease_owner=NULL,
                   lease_until=NULL, updated_at=? WHERE message_id=?
                   AND state IN ('processing', 'queued')""",
                (int(time.time()), message_id),
            )
            self._conn.commit()

    def mark_incoming_queued(self, message_id: str) -> None:
        """Persist that a claimed message now lives in an in-memory FIFO.

        Queued rows are deliberately not lease-reclaimed while this process is
        alive: a thread may remain blocked or externally busy for longer than
        any fixed lease.  Startup recovery makes them retryable after the
        in-memory queues have necessarily disappeared.
        """

        with self._lock:
            cur = self._conn.execute(
                """UPDATE inbox_messages SET state='queued', lease_owner=NULL,
                   lease_until=NULL, updated_at=? WHERE message_id=? AND state='processing'""",
                (int(time.time()), message_id),
            )
            self._conn.commit()
        if cur.rowcount != 1:
            raise RuntimeError(f"inbox message is not claimable for queueing: {message_id}")

    def mark_incoming_ambiguous(self, message_id: str, error: str) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE inbox_messages SET state='ambiguous', lease_owner=NULL,
                   lease_until=NULL, last_error=?, updated_at=? WHERE message_id=?""",
                (redact_log(error, max_chars=2000), int(time.time()), message_id),
            )
            self._conn.commit()

    def recover_inbox_after_restart(self) -> list[str]:
        """Recover work that was ACKed but not completed.

        Rows still in ``processing`` had not crossed the Codex RPC boundary.
        ``queued`` rows were in process-local FIFOs that disappeared during the
        restart.  Both are safe to retry.  ``dispatching`` rows may already
        have started a turn; replaying them could repeat side effects, so they
        remain ``ambiguous`` for explicit human review.
        """

        now = int(time.time())
        with self._lock:
            rows = self._conn.execute(
                "SELECT message_id FROM inbox_messages WHERE state='dispatching'"
            ).fetchall()
            self._conn.execute(
                """UPDATE inbox_messages SET state='retry', available_at=?,
                   lease_owner=NULL, lease_until=NULL, updated_at=?
                   WHERE state IN ('processing', 'queued')""",
                (now, now),
            )
            self._conn.execute(
                """UPDATE inbox_messages SET state='ambiguous', lease_owner=NULL,
                   lease_until=NULL, last_error='service restarted across Codex dispatch',
                   updated_at=? WHERE state='dispatching'""",
                (now,),
            )
            self._conn.commit()
        return [str(row["message_id"]) for row in rows]

    def fail_incoming(
        self, message_id: str, error: str, *, retry_after_seconds: int = 10, dead: bool = False
    ) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """UPDATE inbox_messages SET state=?, available_at=?, lease_owner=NULL,
                   lease_until=NULL, last_error=?, updated_at=? WHERE message_id=?""",
                (
                    "dead" if dead else "retry",
                    now + retry_after_seconds,
                    redact_log(error, max_chars=2000),
                    now,
                    message_id,
                ),
            )
            self._conn.commit()

    def inbox_counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT state, COUNT(*) AS count FROM inbox_messages GROUP BY state"
            ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}

    def inbox_state(self, message_id: str) -> str | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT state FROM inbox_messages WHERE message_id=?", (message_id,)
            ).fetchone()
        return str(row["state"]) if row else None

    def list_ambiguous(self, *, limit: int = 20) -> list[InboxItem]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT payload_json, state, attempts, last_error FROM inbox_messages
                   WHERE state='ambiguous' ORDER BY updated_at DESC LIMIT ?""",
                (limit,),
            ).fetchall()
        return [
            InboxItem(
                message=self._incoming(json.loads(row["payload_json"])),
                state=str(row["state"]),
                attempts=int(row["attempts"]),
                last_error=row["last_error"],
            )
            for row in rows
        ]

    def retry_ambiguous(self, message_id: str) -> bool:
        now = int(time.time())
        with self._lock:
            cur = self._conn.execute(
                """UPDATE inbox_messages SET state='retry', available_at=?, last_error=NULL,
                   updated_at=? WHERE message_id=? AND state='ambiguous'""",
                (now, now, message_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def dismiss_ambiguous(self, message_id: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE inbox_messages SET state='done', last_error='dismissed by owner',
                   updated_at=? WHERE message_id=? AND state='ambiguous'""",
                (int(time.time()), message_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def acquire_thread_lease(self, thread_id: str, owner: str, *, ttl_seconds: int = 300) -> bool:
        now = int(time.time())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    "SELECT owner, expires_at FROM thread_leases WHERE thread_id=?", (thread_id,)
                ).fetchone()
                if row and row["owner"] != owner and int(row["expires_at"]) >= now:
                    self._conn.commit()
                    return False
                self._conn.execute(
                    """INSERT INTO thread_leases(thread_id, owner, expires_at, updated_at)
                       VALUES(?, ?, ?, ?) ON CONFLICT(thread_id) DO UPDATE SET
                       owner=excluded.owner, expires_at=excluded.expires_at,
                       updated_at=excluded.updated_at""",
                    (thread_id, owner, now + ttl_seconds, now),
                )
                self._conn.commit()
                return True
            except Exception:
                self._conn.rollback()
                raise

    def renew_thread_lease(self, thread_id: str, owner: str, *, ttl_seconds: int = 300) -> bool:
        now = int(time.time())
        with self._lock:
            cur = self._conn.execute(
                """UPDATE thread_leases SET expires_at=?, updated_at=?
                   WHERE thread_id=? AND owner=?""",
                (now + ttl_seconds, now, thread_id, owner),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def release_thread_lease(self, thread_id: str, owner: str) -> None:
        with self._lock:
            self._conn.execute(
                "DELETE FROM thread_leases WHERE thread_id=? AND owner=?", (thread_id, owner)
            )
            self._conn.commit()

    def enqueue_outbox(self, item: OutboxItem) -> bool:
        now = int(time.time())
        with self._lock:
            cur = self._conn.execute(
                """INSERT OR IGNORE INTO outbox_messages(
                       outbox_key, app_role, receive_id, receive_id_type, msg_type,
                       content_json, group_key, sequence, thread_id, turn_id, state, attempts,
                       available_at, created_at, updated_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)""",
                (
                    item.outbox_key,
                    item.app_role,
                    item.receive_id,
                    item.receive_id_type,
                    item.msg_type,
                    json.dumps(item.content, ensure_ascii=False),
                    item.group_key or item.outbox_key,
                    item.sequence,
                    item.thread_id,
                    item.turn_id,
                    now,
                    now,
                    now,
                ),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def claim_outbox(self, worker_id: str, *, lease_seconds: int = 120) -> OutboxItem | None:
        now = int(time.time())
        with self._lock:
            self._conn.execute("BEGIN IMMEDIATE")
            try:
                row = self._conn.execute(
                    """SELECT candidate.* FROM outbox_messages AS candidate
                       WHERE (
                           (candidate.state IN ('pending', 'retry') AND candidate.available_at <= ?)
                           OR (candidate.state='sending' AND candidate.lease_until < ?)
                       ) AND NOT EXISTS (
                           SELECT 1 FROM outbox_messages AS previous
                           WHERE previous.group_key=candidate.group_key
                             AND previous.sequence < candidate.sequence
                             AND previous.state != 'done'
                       )
                       ORDER BY candidate.created_at, candidate.group_key,
                                candidate.sequence, candidate.outbox_key LIMIT 1""",
                    (now, now),
                ).fetchone()
                if not row:
                    self._conn.commit()
                    return None
                self._conn.execute(
                    """UPDATE outbox_messages SET state='sending', attempts=attempts+1,
                       lease_owner=?, lease_until=?, updated_at=? WHERE outbox_key=?""",
                    (worker_id, now + lease_seconds, now, row["outbox_key"]),
                )
                self._conn.commit()
            except Exception:
                self._conn.rollback()
                raise
        return OutboxItem(
            outbox_key=str(row["outbox_key"]),
            app_role=row["app_role"],
            receive_id=str(row["receive_id"]),
            receive_id_type=str(row["receive_id_type"]),
            msg_type=str(row["msg_type"]),
            content=json.loads(row["content_json"]),
            group_key=str(row["group_key"]),
            sequence=int(row["sequence"]),
            thread_id=row["thread_id"],
            turn_id=row["turn_id"],
            attempts=int(row["attempts"]) + 1,
        )

    def complete_outbox(
        self,
        outbox_key: str,
        *,
        thread_id: str | None = None,
        turn_id: str | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE outbox_messages SET state='done', content_json='{}',
                   lease_owner=NULL, lease_until=NULL, last_error=NULL,
                   updated_at=? WHERE outbox_key=?""",
                (int(time.time()), outbox_key),
            )
            if thread_id and turn_id:
                now = int(time.time())
                self._conn.execute(
                    """INSERT OR IGNORE INTO synced_turns(thread_id, turn_id, synced_at)
                       VALUES(?, ?, ?)""",
                    (thread_id, turn_id, now),
                )
                self._conn.execute(
                    "UPDATE turn_jobs SET state='delivered', updated_at=? WHERE turn_id=?",
                    (now, turn_id),
                )
            self._conn.commit()

    def fail_outbox(
        self,
        outbox_key: str,
        error: str,
        *,
        retry_after_seconds: int,
        dead: bool = False,
    ) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """UPDATE outbox_messages SET state=?, available_at=?,
                   lease_owner=NULL, lease_until=NULL, last_error=?, updated_at=?
                   WHERE outbox_key=?""",
                (
                    "dead" if dead else "retry",
                    now + retry_after_seconds,
                    redact_log(error, max_chars=2000),
                    now,
                    outbox_key,
                ),
            )
            self._conn.commit()

    def recover_outbox_after_restart(self) -> int:
        now = int(time.time())
        with self._lock:
            cur = self._conn.execute(
                """UPDATE outbox_messages SET state='retry', available_at=?,
                   lease_owner=NULL, lease_until=NULL,
                   last_error='service restarted during send', updated_at=?
                   WHERE state='sending'""",
                (now, now),
            )
            self._conn.commit()
            return cur.rowcount

    def outbox_counts(self) -> dict[str, int]:
        with self._lock:
            rows = self._conn.execute(
                "SELECT state, COUNT(*) AS count FROM outbox_messages GROUP BY state"
            ).fetchall()
        return {str(row["state"]): int(row["count"]) for row in rows}

    def mark_turn_synced(self, thread_id: str, turn_id: str) -> None:
        if not thread_id or not turn_id:
            return
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO synced_turns(thread_id, turn_id, synced_at) VALUES(?, ?, ?)",
                (thread_id, turn_id, int(time.time())),
            )
            self._conn.commit()

    def is_turn_synced(self, thread_id: str, turn_id: str) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM synced_turns WHERE thread_id=? AND turn_id=?",
                (thread_id, turn_id),
            ).fetchone()
        return row is not None

    def is_bridge_turn(self, turn_id: str) -> bool:
        if not turn_id:
            return False
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM turn_jobs WHERE turn_id=?", (turn_id,)
            ).fetchone()
        return row is not None

    def upsert_turn_job(self, job: TurnJob) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """INSERT INTO turn_jobs(
                       message_id, thread_id, turn_id, app_role, chat_id,
                       progress_message_id, state, created_at, updated_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
                   ON CONFLICT(message_id) DO UPDATE SET
                       thread_id=excluded.thread_id, turn_id=excluded.turn_id,
                       app_role=excluded.app_role, chat_id=excluded.chat_id,
                       progress_message_id=COALESCE(excluded.progress_message_id,
                                                    turn_jobs.progress_message_id),
                       state=excluded.state, updated_at=excluded.updated_at""",
                (
                    job.message_id,
                    job.thread_id,
                    job.turn_id,
                    job.app_role,
                    job.chat_id,
                    job.progress_message_id,
                    job.state,
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def set_turn_job_state(self, turn_id: str, state: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE turn_jobs SET state=?, updated_at=? WHERE turn_id=?",
                (state, int(time.time()), turn_id),
            )
            self._conn.commit()

    def list_recoverable_turn_jobs(self) -> list[TurnJob]:
        with self._lock:
            rows = self._conn.execute(
                """SELECT * FROM turn_jobs
                   WHERE state IN ('accepted', 'running') ORDER BY created_at"""
            ).fetchall()
        return [
            TurnJob(
                message_id=str(row["message_id"]),
                thread_id=str(row["thread_id"]),
                turn_id=str(row["turn_id"]),
                app_role=row["app_role"],
                chat_id=str(row["chat_id"]),
                progress_message_id=row["progress_message_id"],
                state=str(row["state"]),
                created_at=int(row["created_at"]),
            )
            for row in rows
        ]

    def add_artifact_approval(self, artifact: PendingArtifact) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """INSERT OR IGNORE INTO artifact_approvals(
                       approval_id, thread_id, turn_id, app_role, chat_id, path,
                       sha256, size, state, created_at, updated_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    artifact.approval_id,
                    artifact.thread_id,
                    artifact.turn_id,
                    artifact.app_role,
                    artifact.chat_id,
                    str(artifact.path),
                    artifact.sha256,
                    artifact.size,
                    artifact.state,
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def get_artifact_approval(self, approval_id: str, chat_id: str) -> PendingArtifact | None:
        with self._lock:
            row = self._conn.execute(
                """SELECT * FROM artifact_approvals
                   WHERE approval_id=? AND chat_id=? AND state='pending'""",
                (approval_id, chat_id),
            ).fetchone()
        if not row:
            return None
        return PendingArtifact(
            approval_id=str(row["approval_id"]),
            thread_id=str(row["thread_id"]),
            turn_id=str(row["turn_id"]),
            app_role=row["app_role"],
            chat_id=str(row["chat_id"]),
            path=Path(str(row["path"])),
            sha256=str(row["sha256"]),
            size=int(row["size"]),
            state=str(row["state"]),
        )

    def resolve_artifact_approval(self, approval_id: str, state: str) -> bool:
        with self._lock:
            cur = self._conn.execute(
                """UPDATE artifact_approvals SET state=?, updated_at=?
                   WHERE approval_id=? AND state='pending'""",
                (state, int(time.time()), approval_id),
            )
            self._conn.commit()
            return cur.rowcount == 1

    def upsert_thread(self, thread: ThreadSummary, *, title: str | None = None) -> Binding:
        now = int(time.time())
        effective_title = (title or thread.display_name).strip()
        with self._lock:
            self._conn.execute(
                """INSERT INTO bindings(
                       thread_id, title, cwd, thread_created_at, thread_updated_at,
                       sync_state, created_at, updated_at
                   ) VALUES(?, ?, ?, ?, ?, 'pending', ?, ?)
                   ON CONFLICT(thread_id) DO UPDATE SET
                       title=bindings.title,
                       cwd=excluded.cwd,
                       thread_created_at=CASE
                           WHEN excluded.thread_created_at > 0
                           THEN excluded.thread_created_at
                           ELSE bindings.thread_created_at
                       END,
                       thread_updated_at=MAX(
                           bindings.thread_updated_at,
                           excluded.thread_updated_at
                       ),
                       updated_at=excluded.updated_at""",
                (
                    thread.thread_id,
                    effective_title,
                    thread.cwd,
                    thread.created_at,
                    thread.updated_at,
                    now,
                    now,
                ),
            )
            self._conn.commit()
        binding = self.get_binding_by_thread(thread.thread_id)
        if binding is None:
            raise RuntimeError("thread binding disappeared after registration")
        return binding

    def refresh_thread_metadata(self, thread: ThreadSummary) -> Binding | None:
        """Refresh metadata only for an existing binding without registering a new thread."""

        with self._lock:
            self._conn.execute(
                """UPDATE bindings SET
                       cwd=CASE WHEN ? != '' THEN ? ELSE cwd END,
                       thread_created_at=CASE
                           WHEN ? > 0 THEN ? ELSE thread_created_at
                       END,
                       thread_updated_at=MAX(thread_updated_at, ?),
                       updated_at=?
                   WHERE thread_id=?""",
                (
                    thread.cwd,
                    thread.cwd,
                    thread.created_at,
                    thread.created_at,
                    thread.updated_at,
                    int(time.time()),
                    thread.thread_id,
                ),
            )
            self._conn.commit()
        return self.get_binding_by_thread(thread.thread_id)

    def bind_chat(self, thread_id: str, chat_id: str, title: str | None = None) -> None:
        now = int(time.time())
        with self._lock:
            if title:
                self._conn.execute(
                    """UPDATE bindings SET chat_id=?, title=?, sync_state='ready', updated_at=?
                       WHERE thread_id=?""",
                    (chat_id, title, now, thread_id),
                )
            else:
                self._conn.execute(
                    """UPDATE bindings SET chat_id=?, sync_state='ready', updated_at=?
                       WHERE thread_id=?""",
                    (chat_id, now, thread_id),
                )
            self._conn.commit()

    def set_binding_title(self, thread_id: str, title: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE bindings SET title=?, updated_at=? WHERE thread_id=?",
                (title.strip(), int(time.time()), thread_id),
            )
            self._conn.commit()

    def set_binding_error(self, thread_id: str, reason: str) -> None:
        self.set_setting(f"binding_error:{thread_id}", reason)
        with self._lock:
            self._conn.execute(
                "UPDATE bindings SET sync_state='error', updated_at=? WHERE thread_id=?",
                (int(time.time()), thread_id),
            )
            self._conn.commit()

    def mark_thread_seen(
        self,
        thread_id: str,
        *,
        updated_at: int,
        turn_id: str | None = None,
        message_hash: str | None = None,
    ) -> None:
        with self._lock:
            self._conn.execute(
                """UPDATE bindings SET thread_updated_at=?,
                       last_synced_turn_id=COALESCE(?, last_synced_turn_id),
                       last_synced_message_hash=COALESCE(?, last_synced_message_hash),
                       updated_at=? WHERE thread_id=?""",
                (updated_at, turn_id, message_hash, int(time.time()), thread_id),
            )
            self._conn.commit()

    def get_binding_by_thread(self, thread_id: str) -> Binding | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM bindings WHERE thread_id=?", (thread_id,)
            ).fetchone()
        return self._binding(row) if row else None

    def get_binding_by_chat(self, chat_id: str) -> Binding | None:
        with self._lock:
            row = self._conn.execute(
                "SELECT * FROM bindings WHERE chat_id=? AND active=1", (chat_id,)
            ).fetchone()
        return self._binding(row) if row else None

    def list_bindings(self, *, pending_only: bool = False) -> list[Binding]:
        sql = "SELECT * FROM bindings WHERE active=1"
        if pending_only:
            sql += " AND chat_id IS NULL"
        sql += " ORDER BY thread_updated_at DESC"
        with self._lock:
            rows = self._conn.execute(sql).fetchall()
        return [self._binding(row) for row in rows]

    def add_approval(self, approval: PendingApproval) -> None:
        now = int(time.time())
        with self._lock:
            self._conn.execute(
                """INSERT OR REPLACE INTO pending_approvals(
                       short_id, rpc_id, method, thread_id, turn_id, chat_id,
                       params_json, state, created_at, updated_at
                   ) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    approval.short_id,
                    approval.rpc_id,
                    approval.method,
                    approval.thread_id,
                    approval.turn_id,
                    approval.chat_id,
                    json.dumps(approval.params, ensure_ascii=False),
                    approval.state,
                    now,
                    now,
                ),
            )
            self._conn.commit()

    def get_approval(self, short_id: str, chat_id: str | None = None) -> PendingApproval | None:
        sql = "SELECT * FROM pending_approvals WHERE short_id=? AND state='pending'"
        params: list[Any] = [short_id]
        if chat_id:
            sql += " AND chat_id=?"
            params.append(chat_id)
        with self._lock:
            row = self._conn.execute(sql, params).fetchone()
        if not row:
            return None
        return PendingApproval(
            short_id=row["short_id"],
            rpc_id=row["rpc_id"],
            method=row["method"],
            thread_id=row["thread_id"],
            turn_id=row["turn_id"],
            chat_id=row["chat_id"],
            params=json.loads(row["params_json"]),
            state=row["state"],
        )

    def resolve_approval(self, short_id: str, state: str) -> None:
        with self._lock:
            self._conn.execute(
                "UPDATE pending_approvals SET state=?, updated_at=? WHERE short_id=?",
                (state, int(time.time()), short_id),
            )
            self._conn.commit()

    @staticmethod
    def _binding(row: sqlite3.Row) -> Binding:
        return Binding(
            thread_id=row["thread_id"],
            title=row["title"],
            cwd=row["cwd"],
            chat_id=row["chat_id"],
            app_role=row["app_role"],
            thread_created_at=row["thread_created_at"],
            thread_updated_at=row["thread_updated_at"],
            last_synced_turn_id=row["last_synced_turn_id"],
            last_synced_message_hash=row["last_synced_message_hash"],
            sync_state=row["sync_state"],
            active=bool(row["active"]),
        )

    @staticmethod
    def _incoming(raw: dict[str, Any]) -> IncomingMessage:
        attachments = [
            Attachment(
                kind=item["kind"],
                name=item["name"],
                message_id=item["message_id"],
                file_key=item.get("file_key"),
                image_key=item.get("image_key"),
                local_path=Path(item["local_path"]) if item.get("local_path") else None,
                mime_type=item.get("mime_type"),
                size=item.get("size"),
            )
            for item in raw.get("attachments", [])
        ]
        return IncomingMessage(
            message_id=raw["message_id"],
            chat_id=raw["chat_id"],
            chat_type=raw.get("chat_type", ""),
            app_role=raw["app_role"],
            sender_open_id=raw.get("sender_open_id"),
            sender_user_id=raw.get("sender_user_id"),
            sender_union_id=raw.get("sender_union_id"),
            text=raw.get("text", ""),
            message_type=raw.get("message_type", "text"),
            create_time_ms=int(raw.get("create_time_ms", 0)),
            tenant_key=raw.get("tenant_key"),
            app_id=raw.get("app_id"),
            sender_type=raw.get("sender_type", "user"),
            attachments=attachments,
        )
