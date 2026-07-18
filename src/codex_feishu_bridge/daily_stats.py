from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import socket
import sqlite3
import time
from collections.abc import Iterable
from dataclasses import asdict, dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import httpx

from .config import BridgeConfig
from .db import BridgeDB
from .privacy import log_ref

LOG = logging.getLogger(__name__)
BRIDGE_CONSTRAINT_MARKER = "飞书桥交付约束："
TOTAL_HEADER = "总任务"
LONG_HEADER = "长任务"


class DailyStatsError(RuntimeError):
    pass


@dataclass(frozen=True, slots=True)
class HostIdentity:
    host_id: str
    hostname: str
    bot_name: str
    app_id: str


@dataclass(frozen=True, slots=True)
class DailyCount:
    day: date
    total: int
    long: int


@dataclass(frozen=True, slots=True)
class SyncResult:
    identity: HostIdentity
    column_start_index: int
    counts: tuple[DailyCount, ...]
    sheet_title: str
    read_verified: bool
    write_verified: bool
    other_data_preserved: bool


@dataclass(slots=True)
class _TurnEvent:
    started_at: int | None = None
    duration_ms: int | None = None
    terminal: str | None = None


@dataclass(frozen=True, slots=True)
class _TurnJob:
    thread_id: str
    turn_id: str
    state: str
    created_at: int
    updated_at: int
    text: str


class FeishuSheetsClient:
    def __init__(self, app_id: str, app_secret: str, db: BridgeDB | None = None):
        self.app_id = app_id
        self.app_secret = app_secret
        self._db = db
        self._token: str | None = None
        self._client = httpx.AsyncClient(
            base_url="https://open.feishu.cn/open-apis",
            timeout=20,
            headers={"Content-Type": "application/json; charset=utf-8"},
        )

    async def __aenter__(self) -> FeishuSheetsClient:
        await self._authenticate()
        return self

    async def __aexit__(self, *_: object) -> None:
        await self._client.aclose()

    async def _authenticate(self) -> None:
        payload = await self._request_counted(
            "auth.tenant_access_token",
            "POST",
            "/auth/v3/tenant_access_token/internal",
            json_body={"app_id": self.app_id, "app_secret": self.app_secret},
            authenticated=False,
        )
        token = str(payload.get("tenant_access_token") or "")
        if not token:
            raise DailyStatsError("Feishu authentication succeeded without a token")
        self._token = token

    async def request(
        self,
        method: str,
        path: str,
        *,
        operation: str,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return await self._request_counted(
            operation, method, path, json_body=json_body, authenticated=True
        )

    async def _request_counted(
        self,
        operation: str,
        method: str,
        path: str,
        *,
        json_body: dict[str, Any] | None,
        authenticated: bool,
    ) -> dict[str, Any]:
        if self._db:
            with contextlib.suppress(Exception):
                self._db.record_api_attempt("conversation", operation)
        if authenticated and not self._token:
            raise DailyStatsError("Feishu client is not authenticated")
        headers = {"Authorization": f"Bearer {self._token}"} if authenticated else None
        try:
            response = await self._client.request(method, path, headers=headers, json=json_body)
            payload = _json_response(response, f"{method} {path.split('?')[0]}")
        except BaseException:
            if self._db:
                with contextlib.suppress(Exception):
                    self._db.record_api_result("conversation", operation, success=False)
            raise
        if self._db:
            with contextlib.suppress(Exception):
                self._db.record_api_result("conversation", operation, success=True)
        return payload

    async def bot_name(self) -> str:
        payload = await self.request("GET", "/bot/v3/info", operation="bot.info")
        bot_name = str((payload.get("bot") or {}).get("app_name") or "").strip()
        if not bot_name:
            raise DailyStatsError("Feishu bot info did not contain app_name")
        return bot_name

    async def sheet_info(self, spreadsheet_token: str, sheet_id: str) -> dict[str, Any]:
        payload = await self.request(
            "GET",
            f"/sheets/v3/spreadsheets/{spreadsheet_token}/sheets/{sheet_id}",
            operation="sheet.metadata.read",
        )
        sheet = (payload.get("data") or {}).get("sheet")
        if not isinstance(sheet, dict):
            raise DailyStatsError("Feishu sheet query returned no sheet metadata")
        return sheet

    async def document_permission(self, spreadsheet_token: str, action: str) -> bool:
        payload = await self.request(
            "GET",
            f"/drive/v1/permissions/{spreadsheet_token}/members/auth?action={action}&type=sheet",
            operation=f"sheet.permission.{action}",
        )
        return bool((payload.get("data") or {}).get("auth_result"))

    async def values(self, spreadsheet_token: str, range_name: str) -> list[list[Any]]:
        payload = await self.request(
            "GET",
            f"/sheets/v2/spreadsheets/{spreadsheet_token}/values/{range_name}",
            operation="sheet.values.read",
        )
        values = ((payload.get("data") or {}).get("valueRange") or {}).get("values")
        return list(values or [])

    async def write_values(
        self, spreadsheet_token: str, range_name: str, values: list[list[Any]]
    ) -> None:
        await self.request(
            "PUT",
            f"/sheets/v2/spreadsheets/{spreadsheet_token}/values",
            operation="sheet.values.write",
            json_body={"valueRange": {"range": range_name, "values": values}},
        )

    async def batch_write_values(
        self,
        spreadsheet_token: str,
        value_ranges: list[dict[str, Any]],
    ) -> None:
        await self.request(
            "POST",
            f"/sheets/v2/spreadsheets/{spreadsheet_token}/values_batch_update",
            operation="sheet.values.batch_write",
            json_body={"valueRanges": value_ranges},
        )

    async def merge_cells(self, spreadsheet_token: str, range_name: str) -> None:
        await self.request(
            "POST",
            f"/sheets/v2/spreadsheets/{spreadsheet_token}/merge_cells",
            operation="sheet.cells.merge",
            json_body={"range": range_name, "mergeType": "MERGE_ALL"},
        )

    async def insert_row(self, spreadsheet_token: str, sheet_id: str, row_number: int) -> None:
        start_index = row_number - 1
        await self.request(
            "POST",
            f"/sheets/v2/spreadsheets/{spreadsheet_token}/insert_dimension_range",
            operation="sheet.rows.insert",
            json_body={
                "dimension": {
                    "sheetId": sheet_id,
                    "majorDimension": "ROWS",
                    "startIndex": start_index,
                    "endIndex": start_index + 1,
                },
                "inheritStyle": "AFTER",
            },
        )


def _json_response(response: httpx.Response, operation: str) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError as error:
        raise DailyStatsError(
            f"Feishu {operation} returned HTTP {response.status_code} with invalid JSON"
        ) from error
    if response.is_success and payload.get("code") == 0:
        return payload
    error = payload.get("error") or {}
    log_id = str(error.get("log_id") or response.headers.get("x-tt-logid") or "")
    suffix = f" log_id={log_id}" if log_id else ""
    raise DailyStatsError(
        f"Feishu {operation} failed: HTTP {response.status_code} "
        f"code={payload.get('code')} msg={payload.get('msg', '')}{suffix}"
    )


def strip_bridge_constraint(text: str) -> str:
    return text.split(BRIDGE_CONSTRAINT_MARKER, 1)[0].strip()


def is_long_task(text: str, duration_ms: int) -> bool:
    return (len(strip_bridge_constraint(text)) > 100 and duration_ms > 180_000) or (
        duration_ms > 600_000
    )


def detect_host_id() -> tuple[str, str]:
    hostname = socket.gethostname().strip() or "unknown-host"
    machine_value = ""
    for candidate in (Path("/etc/machine-id"), Path("/var/lib/dbus/machine-id")):
        with contextlib.suppress(OSError):
            machine_value = candidate.read_text(encoding="utf-8").strip()
        if machine_value:
            break
    if not machine_value:
        machine_value = hostname
    digest = hashlib.sha256(f"codex-feishu-bridge:{machine_value}".encode()).hexdigest()
    return f"host-{digest[:20]}", hostname


def column_name(index: int) -> str:
    if index < 0:
        raise ValueError(index)
    result = ""
    value = index + 1
    while value:
        value, remainder = divmod(value - 1, 26)
        result = chr(65 + remainder) + result
    return result


def choose_group_column(
    header_rows: list[list[Any]],
    bot_name: str,
    *,
    column_count: int,
    previous_identity: dict[str, Any] | None = None,
) -> int:
    first = _padded_row(header_rows, 0, column_count)
    second = _padded_row(header_rows, 1, column_count)

    if previous_identity:
        candidate = previous_identity.get("column_start_index")
        previous_name = str(previous_identity.get("bot_name") or "")
        if isinstance(candidate, int) and 1 <= candidate < column_count - 1:
            if (
                str(first[candidate] or "") in {bot_name, previous_name}
                and str(second[candidate] or "") == TOTAL_HEADER
                and str(second[candidate + 1] or "") == LONG_HEADER
            ):
                return candidate

    matches = [index for index, value in enumerate(first) if str(value or "") == bot_name]
    exact = [
        index
        for index in matches
        if index + 1 < column_count
        and str(second[index] or "") == TOTAL_HEADER
        and str(second[index + 1] or "") == LONG_HEADER
    ]
    if len(exact) > 1:
        raise DailyStatsError(f"sheet contains duplicate groups for bot {bot_name!r}")
    if exact:
        return exact[0]
    if matches:
        raise DailyStatsError(
            f"sheet already contains bot header {bot_name!r} with unexpected subcolumns"
        )

    used = [0]
    for index in range(column_count):
        if first[index] not in (None, "") or second[index] not in (None, ""):
            used.append(index)
    start = max(1, max(used) + 1)
    if start + 1 >= column_count:
        raise DailyStatsError("sheet does not have two empty columns for this bot")
    return start


def _padded_row(rows: list[list[Any]], index: int, width: int) -> list[Any]:
    row = list(rows[index]) if index < len(rows) else []
    return row + [None] * max(0, width - len(row))


def _merge_matches(merges: Iterable[dict[str, Any]], start_column: int) -> bool:
    return any(
        int(item.get("start_row_index", -1)) == 0
        and int(item.get("end_row_index", -1)) == 0
        and int(item.get("start_column_index", -1)) == start_column
        and int(item.get("end_column_index", -1)) == start_column + 1
        for item in merges
    )


def _date_from_cell(value: Any) -> date | None:
    if isinstance(value, str):
        with contextlib.suppress(ValueError):
            return date.fromisoformat(value.strip())
    if isinstance(value, (int, float)):
        with contextlib.suppress(OverflowError, ValueError):
            return date(1899, 12, 30) + timedelta(days=int(value))
    return None


def _date_rows(values: list[list[Any]], *, first_row: int = 3) -> dict[date, int]:
    result: dict[date, int] = {}
    for offset, row in enumerate(values):
        value = row[0] if row else None
        parsed = _date_from_cell(value)
        if parsed is None:
            continue
        if parsed in result:
            raise DailyStatsError(f"sheet contains duplicate date rows for {parsed.isoformat()}")
        result[parsed] = first_row + offset
    return result


async def _ensure_date_row(
    client: FeishuSheetsClient,
    spreadsheet_token: str,
    sheet_id: str,
    target: date,
    row_count: int,
) -> int:
    values = await client.values(spreadsheet_token, f"{sheet_id}!A3:A{max(3, row_count)}")
    rows = _date_rows(values)
    if target in rows:
        return rows[target]

    ordered = sorted(rows.items(), key=lambda item: item[1])
    insertion_row = None
    for existing_day, row_number in ordered:
        if existing_day < target:
            insertion_row = row_number
            break
    if insertion_row is None:
        insertion_row = max((row for _, row in ordered), default=2) + 1
        await client.write_values(
            spreadsheet_token,
            f"{sheet_id}!A{insertion_row}:A{insertion_row}",
            [[target.isoformat()]],
        )
        return insertion_row

    await client.insert_row(spreadsheet_token, sheet_id, insertion_row)
    await client.write_values(
        spreadsheet_token,
        f"{sheet_id}!A{insertion_row}:A{insertion_row}",
        [[target.isoformat()]],
    )
    return insertion_row


def _state_database() -> Path:
    candidates = sorted(
        (Path.home() / ".codex").glob("state_*.sqlite"),
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )
    if not candidates:
        raise DailyStatsError("Codex state database was not found")
    return candidates[0]


def _read_turn_jobs(
    database_path: Path, targets: set[date], timezone_info: ZoneInfo
) -> list[_TurnJob]:
    earliest = datetime.combine(
        min(targets) - timedelta(days=1), datetime.min.time(), timezone_info
    )
    latest = datetime.combine(max(targets) + timedelta(days=2), datetime.min.time(), timezone_info)
    connection = sqlite3.connect(f"file:{database_path}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        rows = connection.execute(
            """SELECT j.thread_id, j.turn_id, j.state, j.created_at, j.updated_at,
                      i.payload_json
                 FROM turn_jobs AS j
                 JOIN inbox_messages AS i USING(message_id)
                WHERE j.created_at >= ? AND j.created_at < ?
                ORDER BY j.created_at, j.turn_id""",
            (int(earliest.timestamp()), int(latest.timestamp())),
        ).fetchall()
    finally:
        connection.close()
    result: list[_TurnJob] = []
    for row in rows:
        try:
            payload = json.loads(str(row["payload_json"]))
        except json.JSONDecodeError as error:
            raise DailyStatsError(f"invalid inbox JSON for turn {row['turn_id']}") from error
        result.append(
            _TurnJob(
                thread_id=str(row["thread_id"]),
                turn_id=str(row["turn_id"]),
                state=str(row["state"]),
                created_at=int(row["created_at"]),
                updated_at=int(row["updated_at"]),
                text=str(payload.get("text") or ""),
            )
        )
    return result


def _rollout_paths(thread_ids: set[str]) -> dict[str, Path]:
    if not thread_ids:
        return {}
    state_path = _state_database()
    connection = sqlite3.connect(f"file:{state_path}?mode=ro", uri=True, timeout=5)
    try:
        # Pass the complete id list as one bound JSON value; no SQL identifier
        # or predicate is assembled from thread data.
        rows = connection.execute(
            """SELECT id, rollout_path FROM threads
               WHERE id IN (SELECT value FROM json_each(?))""",
            (json.dumps(sorted(thread_ids)),),
        ).fetchall()
    finally:
        connection.close()
    result = {str(row[0]): Path(str(row[1])).expanduser() for row in rows}
    codex_home = Path.home() / ".codex"
    for thread_id in thread_ids - result.keys():
        candidates = list((codex_home / "archived_sessions").glob(f"*{thread_id}.jsonl"))
        candidates.extend((codex_home / "sessions").glob(f"**/*{thread_id}.jsonl"))
        if candidates:
            result[thread_id] = max(candidates, key=lambda path: path.stat().st_mtime)
    return result


def _turn_events(jobs: list[_TurnJob]) -> dict[str, _TurnEvent]:
    by_thread: dict[str, set[str]] = {}
    for job in jobs:
        by_thread.setdefault(job.thread_id, set()).add(job.turn_id)
    paths = _rollout_paths(set(by_thread))
    result = {job.turn_id: _TurnEvent() for job in jobs}
    for thread_id, turn_ids in by_thread.items():
        path = paths.get(thread_id)
        if not path or not path.exists():
            LOG.warning("No rollout file found for Codex thread ref=%s", log_ref(thread_id))
            continue
        with path.open("r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                try:
                    item = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if item.get("type") != "event_msg":
                    continue
                payload = item.get("payload") or {}
                event_type = payload.get("type")
                turn_id = str(payload.get("turn_id") or "")
                if turn_id not in turn_ids:
                    continue
                event = result[turn_id]
                if event_type == "task_started":
                    with contextlib.suppress(TypeError, ValueError):
                        event.started_at = int(payload.get("started_at"))
                elif event_type in {"task_complete", "turn_aborted"}:
                    with contextlib.suppress(TypeError, ValueError):
                        event.duration_ms = int(payload.get("duration_ms"))
                    event.terminal = str(event_type)
    return result


def calculate_daily_counts(
    config: BridgeConfig,
    targets: Iterable[date],
    *,
    now: datetime | None = None,
) -> tuple[DailyCount, ...]:
    timezone_info = ZoneInfo(config.daily_stats.timezone)
    now_value = now or datetime.now(timezone_info)
    if now_value.tzinfo is None:
        now_value = now_value.replace(tzinfo=timezone_info)
    target_set = set(targets)
    jobs = _read_turn_jobs(config.database_path, target_set, timezone_info)
    events = _turn_events(jobs)
    totals = {day: [0, 0] for day in target_set}
    for job in jobs:
        event = events[job.turn_id]
        started_at = event.started_at if event.started_at is not None else job.created_at
        started = datetime.fromtimestamp(started_at, timezone_info)
        if started.date() not in target_set:
            continue
        if event.duration_ms is not None:
            duration_ms = event.duration_ms
        elif job.state == "accepted":
            duration_ms = max(0, int((now_value.timestamp() - started_at) * 1000))
        else:
            duration_ms = max(0, (job.updated_at - started_at) * 1000)
        totals[started.date()][0] += 1
        if is_long_task(job.text, duration_ms):
            totals[started.date()][1] += 1
    return tuple(
        DailyCount(day=day, total=totals[day][0], long=totals[day][1])
        for day in sorted(target_set, reverse=True)
    )


def _load_identity(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise DailyStatsError(f"cannot read daily stats identity state: {path}") from error
    return payload if isinstance(payload, dict) else None


def _save_identity(
    path: Path, identity: HostIdentity, column_start_index: int, config: BridgeConfig
) -> None:
    payload = {
        "schema": 1,
        **asdict(identity),
        "spreadsheet_token": config.daily_stats.spreadsheet_token,
        "sheet_id": config.daily_stats.sheet_id,
        "column_start_index": column_start_index,
        "updated_at": int(time.time()),
    }
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.chmod(0o600)
    os.replace(temporary, path)
    path.chmod(0o600)


def _preservation_snapshot(
    values: list[list[Any]], own_columns: set[int], width: int
) -> tuple[tuple[Any, ...], dict[date, tuple[Any, ...]]]:
    first = _padded_row(values, 0, width)
    second = _padded_row(values, 1, width)
    header = tuple(
        (first[index], second[index]) for index in range(width) if index not in own_columns
    )
    history: dict[date, tuple[Any, ...]] = {}
    for row in values[2:]:
        padded = list(row) + [None] * max(0, width - len(row))
        parsed = _date_from_cell(padded[0] if padded else None)
        if parsed:
            history[parsed] = tuple(
                padded[index] for index in range(width) if index not in own_columns
            )
    return header, history


async def sync_daily_stats(config: BridgeConfig, db: BridgeDB | None = None) -> SyncResult:
    stats = config.daily_stats
    if not stats.enabled:
        raise DailyStatsError("daily_stats is disabled in config.toml")
    if not stats.spreadsheet_token or not stats.sheet_id:
        raise DailyStatsError("daily_stats spreadsheet_token and sheet_id are required")
    app = config.feishu.conversation
    if not app.configured:
        raise DailyStatsError("Codex Feishu app credentials are not configured")
    timezone_info = ZoneInfo(stats.timezone)
    today = datetime.now(timezone_info).date()
    targets = (today, today - timedelta(days=1))
    counts = calculate_daily_counts(config, targets)
    host_id, hostname = detect_host_id()
    identity_path = config.state_dir / "daily-stats-identity.json"
    previous = _load_identity(identity_path)
    if previous and str(previous.get("host_id") or "") != host_id:
        raise DailyStatsError("daily stats identity belongs to a different host_id")
    if previous and str(previous.get("app_id") or "") != app.app_id:
        raise DailyStatsError("daily stats identity belongs to a different Feishu app")

    async with FeishuSheetsClient(app.app_id, app.secret(), db=db) as client:
        bot_name = await client.bot_name()
        identity = HostIdentity(host_id, hostname, bot_name, app.app_id)
        if not await client.document_permission(stats.spreadsheet_token, "view"):
            raise DailyStatsError("Feishu app does not have view permission on the spreadsheet")
        if not await client.document_permission(stats.spreadsheet_token, "edit"):
            raise DailyStatsError(
                "Feishu app can view the spreadsheet but is not an editable document app"
            )
        sheet = await client.sheet_info(stats.spreadsheet_token, stats.sheet_id)
        properties = sheet.get("grid_properties") or {}
        column_count = int(properties.get("column_count") or 0)
        row_count = int(properties.get("row_count") or 0)
        if column_count < 3 or row_count < 3:
            raise DailyStatsError("target sheet is too small for the daily statistics layout")
        header_range = f"{stats.sheet_id}!A1:{column_name(column_count - 1)}2"
        header_rows = await client.values(stats.spreadsheet_token, header_range)
        start_column = choose_group_column(
            header_rows,
            bot_name,
            column_count=column_count,
            previous_identity=previous,
        )
        own_columns = {start_column, start_column + 1}
        compare_width = max(start_column + 2, 3)
        compare_end = column_name(compare_width - 1)
        before_values = await client.values(
            stats.spreadsheet_token, f"{stats.sheet_id}!A1:{compare_end}{row_count}"
        )
        before_snapshot = _preservation_snapshot(before_values, own_columns, compare_width)

        first = _padded_row(header_rows, 0, column_count)
        second = _padded_row(header_rows, 1, column_count)
        left = column_name(start_column)
        right = column_name(start_column + 1)
        merged = _merge_matches(sheet.get("merges") or [], start_column)
        header_matches = (
            str(first[start_column] or "") == bot_name
            and str(second[start_column] or "") == TOTAL_HEADER
            and str(second[start_column + 1] or "") == LONG_HEADER
        )
        if not header_matches:
            if merged:
                await client.batch_write_values(
                    stats.spreadsheet_token,
                    [
                        {"range": f"{stats.sheet_id}!{left}1:{left}1", "values": [[bot_name]]},
                        {
                            "range": f"{stats.sheet_id}!{left}2:{right}2",
                            "values": [[TOTAL_HEADER, LONG_HEADER]],
                        },
                    ],
                )
            else:
                await client.write_values(
                    stats.spreadsheet_token,
                    f"{stats.sheet_id}!{left}1:{right}2",
                    [[bot_name, ""], [TOTAL_HEADER, LONG_HEADER]],
                )
        if not merged:
            await client.merge_cells(stats.spreadsheet_token, f"{stats.sheet_id}!{left}1:{right}1")

        # Insert older target first.  If both dates are absent this preserves
        # descending date order when the newer date is inserted above it.
        rows: dict[date, int] = {}
        current_row_count = row_count
        for target in sorted(targets):
            rows[target] = await _ensure_date_row(
                client,
                stats.spreadsheet_token,
                stats.sheet_id,
                target,
                current_row_count,
            )
            refreshed = await client.sheet_info(stats.spreadsheet_token, stats.sheet_id)
            current_row_count = int(
                (refreshed.get("grid_properties") or {}).get("row_count") or current_row_count
            )
        # Re-read because inserting one target can shift the other target.
        date_values = await client.values(
            stats.spreadsheet_token,
            f"{stats.sheet_id}!A3:A{max(current_row_count, row_count)}",
        )
        rows = _date_rows(date_values)
        value_ranges = []
        for count in counts:
            row_number = rows.get(count.day)
            if not row_number:
                raise DailyStatsError(f"date row disappeared during sync: {count.day}")
            value_ranges.append(
                {
                    "range": f"{stats.sheet_id}!{left}{row_number}:{right}{row_number}",
                    "values": [[count.total, count.long]],
                }
            )
        # Always perform an idempotent write.  A successful call verifies the
        # app's sheet-write API scope and document edit permission.
        await client.batch_write_values(stats.spreadsheet_token, value_ranges)

        sheet_after = await client.sheet_info(stats.spreadsheet_token, stats.sheet_id)
        after_row_count = int(
            (sheet_after.get("grid_properties") or {}).get("row_count") or row_count
        )
        after_values = await client.values(
            stats.spreadsheet_token,
            f"{stats.sheet_id}!A1:{compare_end}{after_row_count}",
        )
        after_snapshot = _preservation_snapshot(after_values, own_columns, compare_width)
        if before_snapshot[0] != after_snapshot[0]:
            raise DailyStatsError("another robot header changed while this host was syncing")
        for existing_day, existing_values in before_snapshot[1].items():
            if after_snapshot[1].get(existing_day) != existing_values:
                raise DailyStatsError(
                    f"another robot's history changed while syncing {existing_day}"
                )

        verification_header = await client.values(
            stats.spreadsheet_token, f"{stats.sheet_id}!{left}1:{right}2"
        )
        verify_first = _padded_row(verification_header, 0, 2)
        verify_second = _padded_row(verification_header, 1, 2)
        if not (
            str(verify_first[0] or "") == bot_name
            and str(verify_second[0] or "") == TOTAL_HEADER
            and str(verify_second[1] or "") == LONG_HEADER
            and _merge_matches(sheet_after.get("merges") or [], start_column)
        ):
            raise DailyStatsError("bot column group verification failed")
        for count in counts:
            row_number = rows[count.day]
            values = await client.values(
                stats.spreadsheet_token,
                f"{stats.sheet_id}!{left}{row_number}:{right}{row_number}",
            )
            row = _padded_row(values, 0, 2)
            if row[:2] != [count.total, count.long]:
                raise DailyStatsError(f"daily values verification failed for {count.day}")

    _save_identity(identity_path, identity, start_column, config)
    return SyncResult(
        identity=identity,
        column_start_index=start_column,
        counts=counts,
        sheet_title=str(sheet.get("title") or stats.sheet_id),
        read_verified=True,
        write_verified=True,
        other_data_preserved=True,
    )
