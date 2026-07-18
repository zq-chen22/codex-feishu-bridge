from __future__ import annotations

import hashlib
import re

_SECRET_VALUE = re.compile(
    r"(?i)((?:app[_-]?secret|api[_-]?key|access[_-]?token|password|authorization)"
    r"\s*[:=]\s*)([^\s,;]+)"
)
_BEARER = re.compile(r"(?i)(\bbearer\s+)([^\s,;]+)")
_HOME_PATH = re.compile(r"(?<![\w.-])/(?:home|Users)/[^/\s]+")
_PLATFORM_ID = re.compile(r"\b(?:ou|oc|cli)_[A-Za-z0-9_-]{8,}\b")
_UUID = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\b"
)
_EMAIL = re.compile(r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b")


def log_ref(value: str) -> str:
    """Return a stable non-reversible reference suitable for local logs."""

    return hashlib.sha256(value.encode()).hexdigest()[:12]


def redact_log(value: object, *, max_chars: int = 500) -> str:
    """Remove common credentials and identifiers from diagnostic text."""

    text = str(value).replace("\r", " ").replace("\n", " ")
    text = _SECRET_VALUE.sub(r"\1[redacted]", text)
    text = _BEARER.sub(r"\1[redacted]", text)
    text = _HOME_PATH.sub("/[private-home]", text)
    text = _PLATFORM_ID.sub("[platform-id]", text)
    text = _UUID.sub("[uuid]", text)
    text = _EMAIL.sub("[email]", text)
    if len(text) > max_chars:
        text = text[:max_chars] + "…"
    return text
