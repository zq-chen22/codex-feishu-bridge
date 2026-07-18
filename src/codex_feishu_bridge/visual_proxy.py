"""Build physical image proxies and enforce them from Codex ``PreToolUse`` hooks.

The bridge deliberately keeps the original upload outside the model-facing path.
Every image exposed through ``view_image`` is a newly encoded JPEG whose longest
edge is bounded.  The hook also covers the app-server's free-form ``exec`` tool,
because nested ``tools.view_image(...)`` calls are represented as JavaScript in
that tool's input rather than as a standalone ``view_image`` invocation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import secrets
import shlex

# Used only for ``list2cmdline`` quoting on Windows; no process is started here.
import subprocess  # nosec B404
import sys
from collections.abc import Mapping
from dataclasses import KW_ONLY, dataclass
from pathlib import Path
from typing import Any

from PIL import Image, ImageOps

DEFAULT_MAX_EDGE = 1024
DEFAULT_JPEG_QUALITY = 75
PROXY_FORMAT_VERSION = 1
_BACKGROUND = (238, 238, 238, 255)
_VIEW_IMAGE_CALL = re.compile(r"\btools\s*\.\s*view_image\s*\(")
_VIEW_IMAGE_REFERENCE = re.compile(
    r"\btools\s*(?:\.\s*view_image\b|\[\s*['\"`]view_image['\"`]\s*\])"
)
_IDENTIFIER = re.compile(r"[A-Za-z_$][\w$]*")


class ProxyError(RuntimeError):
    """A model-facing image request could not be made safe."""


@dataclass(frozen=True, slots=True)
class ProxyResult:
    """Metadata for a physical JPEG proxy."""

    source: Path
    path: Path
    source_size: tuple[int, int]
    output_size: tuple[int, int]
    cache_hit: bool


@dataclass(frozen=True, slots=True)
class VisualProxyStore:
    """Small convenience wrapper used by bridge input staging."""

    root: Path
    _: KW_ONLY
    max_edge: int = DEFAULT_MAX_EDGE
    quality: int = DEFAULT_JPEG_QUALITY

    def create(self, source: Path) -> Path:
        return make_image_proxy(
            source,
            self.root,
            max_edge=self.max_edge,
            quality=self.quality,
        ).path


def _validate_settings(max_edge: int, quality: int) -> None:
    if not 64 <= max_edge <= 4096:
        raise ValueError("max_edge must be between 64 and 4096")
    if not 30 <= quality <= 95:
        raise ValueError("quality must be between 30 and 95")


def _secure_directory(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    try:
        path.chmod(0o700)
    except OSError:
        # Windows does not implement POSIX mode bits.  The directory is still
        # private according to the user's ACL inherited at creation time.
        if os.name != "nt":
            raise


def proxy_cache_key(
    source: Path,
    *,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
) -> str:
    """Return a stable cache key containing identity, metadata and settings."""

    _validate_settings(max_edge, quality)
    canonical = source.expanduser().resolve(strict=True)
    metadata = canonical.stat()
    digest = hashlib.sha256()
    with canonical.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    material = {
        "format_version": PROXY_FORMAT_VERSION,
        "source": str(canonical),
        "size": metadata.st_size,
        "mtime_ns": metadata.st_mtime_ns,
        "source_sha256": digest.hexdigest(),
        "max_edge": max_edge,
        "quality": quality,
        "background": list(_BACKGROUND),
    }
    encoded = json.dumps(material, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def _inspect_proxy(path: Path) -> tuple[int, int]:
    with Image.open(path) as image:
        if image.format != "JPEG":
            raise ProxyError(f"cached proxy is not JPEG: {path}")
        image.verify()
    with Image.open(path) as image:
        return image.size


def _proxy_for_source(
    source: Path,
    proxy_root: Path,
    *,
    max_edge: int,
    quality: int,
) -> Path:
    """Return an existing managed proxy or create one from an original.

    Reusing an already validated proxy prevents proxy-of-proxy chains when the
    model views a path that the bridge previously supplied as ``localImage``.
    Originals are never returned from this function.
    """

    canonical = source.expanduser().resolve(strict=True)
    root = proxy_root.expanduser().resolve()
    try:
        canonical.relative_to(root)
    except ValueError:
        return make_image_proxy(
            canonical,
            root,
            max_edge=max_edge,
            quality=quality,
        ).path
    dimensions = _inspect_proxy(canonical)
    if max(dimensions) > max_edge:
        raise ProxyError(f"managed proxy exceeds {max_edge}px: {canonical}")
    return canonical


def make_image_proxy(
    source: Path,
    proxy_root: Path,
    *,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
) -> ProxyResult:
    """Physically re-encode ``source`` into a cached, bounded JPEG proxy.

    A source that is already smaller than ``max_edge`` is still decoded and
    encoded into a distinct JPEG.  This is intentional: the model must never be
    handed the original file merely because its dimensions happen to be small.
    """

    _validate_settings(max_edge, quality)
    canonical = source.expanduser().resolve(strict=True)
    root = proxy_root.expanduser().resolve()
    _secure_directory(root)
    digest = proxy_cache_key(canonical, max_edge=max_edge, quality=quality)
    shard = root / digest[:2]
    _secure_directory(shard)
    destination = shard / f"{digest}.jpg"

    if destination.is_file():
        output_size = _inspect_proxy(destination)
        try:
            destination.chmod(0o600)
        except OSError:
            if os.name != "nt":
                raise
        with Image.open(canonical) as source_image:
            source_size = ImageOps.exif_transpose(source_image).size
        return ProxyResult(canonical, destination, source_size, output_size, True)

    temporary = shard / (f".{digest}.tmp-{os.getpid()}-{secrets.token_hex(6)}.jpg")
    try:
        with Image.open(canonical) as opened:
            image = ImageOps.exif_transpose(opened)
            source_size = image.size
            if image.mode in {"RGBA", "LA"} or "transparency" in image.info:
                rgba = image.convert("RGBA")
                background = Image.new("RGBA", rgba.size, _BACKGROUND)
                background.alpha_composite(rgba)
                image = background.convert("RGB")
            else:
                image = image.convert("RGB")
            image.thumbnail((max_edge, max_edge), Image.Resampling.LANCZOS)
            output_size = image.size
            image.save(
                temporary,
                format="JPEG",
                quality=quality,
                optimize=True,
                progressive=True,
            )
        temporary.chmod(0o600)
        os.replace(temporary, destination)
        try:
            destination.chmod(0o600)
        except OSError:
            if os.name != "nt":
                raise
    finally:
        temporary.unlink(missing_ok=True)

    return ProxyResult(canonical, destination, source_size, output_size, False)


def _resolve_source(value: str, cwd: Path) -> Path:
    candidate = Path(value).expanduser()
    if not candidate.is_absolute():
        candidate = cwd / candidate
    try:
        return candidate.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise ProxyError(f"view_image source is unavailable: {value}") from error


def rewrite_view_image_input(
    tool_input: Any,
    *,
    cwd: Path,
    proxy_root: Path,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
) -> dict[str, Any]:
    """Rewrite a direct ``view_image`` input to a physical proxy."""

    if not isinstance(tool_input, Mapping):
        raise ProxyError("view_image input must be an object containing a static path")
    value = tool_input.get("path")
    if not isinstance(value, str) or not value.strip():
        raise ProxyError("view_image requires a non-empty static string path")
    source = _resolve_source(value, cwd)
    rewritten = dict(tool_input)
    rewritten["path"] = str(
        _proxy_for_source(
            source,
            proxy_root,
            max_edge=max_edge,
            quality=quality,
        )
    )
    rewritten["detail"] = "high"
    return rewritten


def _scan_string_end(source: str, start: int) -> int:
    quote = source[start]
    index = start + 1
    while index < len(source):
        character = source[index]
        if character == "\\":
            index += 2
            continue
        if character == quote:
            return index + 1
        if quote == "`" and source.startswith("${", index):
            raise ProxyError("dynamic template paths are forbidden in tools.view_image")
        index += 1
    raise ProxyError("unterminated string in tools.view_image")


def _matching_delimiter(source: str, start: int, opening: str, closing: str) -> int:
    depth = 0
    index = start
    while index < len(source):
        character = source[index]
        if character in "\"'`":
            index = _scan_string_end(source, index)
            continue
        if source.startswith("//", index):
            newline = source.find("\n", index + 2)
            index = len(source) if newline < 0 else newline + 1
            continue
        if source.startswith("/*", index):
            end = source.find("*/", index + 2)
            if end < 0:
                raise ProxyError("unterminated comment in tools.view_image")
            index = end + 2
            continue
        if character == opening:
            depth += 1
        elif character == closing:
            depth -= 1
            if depth == 0:
                return index
        index += 1
    raise ProxyError(f"unterminated {opening}{closing} expression in tools.view_image")


def _decode_js_string(literal: str) -> str:
    quote = literal[0]
    if quote not in "\"'`" or literal[-1] != quote:
        raise ProxyError("view_image path must be a static string literal")
    body = literal[1:-1]
    if quote == "`" and "${" in body:
        raise ProxyError("dynamic template paths are forbidden in tools.view_image")
    output: list[str] = []
    index = 0
    escapes = {
        "b": "\b",
        "f": "\f",
        "n": "\n",
        "r": "\r",
        "t": "\t",
        "v": "\v",
        "0": "\0",
        "\\": "\\",
        "'": "'",
        '"': '"',
        "`": "`",
    }
    while index < len(body):
        character = body[index]
        if character != "\\":
            output.append(character)
            index += 1
            continue
        index += 1
        if index >= len(body):
            raise ProxyError("invalid trailing escape in view_image path")
        escaped = body[index]
        if escaped in "\n\r":
            if escaped == "\r" and index + 1 < len(body) and body[index + 1] == "\n":
                index += 1
            index += 1
            continue
        if escaped == "x":
            digits = body[index + 1 : index + 3]
            if len(digits) != 2 or not all(c in "0123456789abcdefABCDEF" for c in digits):
                raise ProxyError("invalid hexadecimal escape in view_image path")
            output.append(chr(int(digits, 16)))
            index += 3
            continue
        if escaped == "u":
            digits = body[index + 1 : index + 5]
            if len(digits) != 4 or not all(c in "0123456789abcdefABCDEF" for c in digits):
                raise ProxyError("invalid unicode escape in view_image path")
            output.append(chr(int(digits, 16)))
            index += 5
            continue
        if escaped not in escapes:
            raise ProxyError("unsupported escape in view_image path")
        output.append(escapes[escaped])
        index += 1
    return "".join(output)


def _parse_static_view_image_object(object_source: str) -> str:
    """Parse the deliberately tiny safe subset accepted by ``view_image``.

    Only static ``path`` and ``detail`` string properties are accepted.  Spread,
    computed keys and arbitrary expressions are rejected so that JavaScript
    evaluation cannot replace the rewritten path after this hook runs.
    """

    index = 1
    end = len(object_source) - 1
    properties: dict[str, str] = {}
    while True:
        while index < end and object_source[index].isspace():
            index += 1
        if index >= end:
            break
        if object_source[index] in "\"'":
            name_end = _scan_string_end(object_source, index)
            name = _decode_js_string(object_source[index:name_end])
            index = name_end
        else:
            identifier = _IDENTIFIER.match(object_source, index)
            if identifier is None:
                raise ProxyError("tools.view_image contains a dynamic or invalid property")
            name = identifier.group(0)
            index = identifier.end()
        if name not in {"path", "detail"}:
            raise ProxyError(f"tools.view_image contains unsupported property: {name}")
        if name in properties:
            raise ProxyError(f"tools.view_image contains duplicate {name} properties")
        while index < end and object_source[index].isspace():
            index += 1
        if index >= end or object_source[index] != ":":
            raise ProxyError(f"tools.view_image {name} property must use a static value")
        index += 1
        while index < end and object_source[index].isspace():
            index += 1
        if index >= end or object_source[index] not in "\"'`":
            raise ProxyError(f"tools.view_image {name} must be a static string literal")
        value_end = _scan_string_end(object_source, index)
        properties[name] = _decode_js_string(object_source[index:value_end])
        index = value_end
        while index < end and object_source[index].isspace():
            index += 1
        if index >= end:
            break
        if object_source[index] != ",":
            raise ProxyError("tools.view_image properties must be statically delimited")
        index += 1
        while index < end and object_source[index].isspace():
            index += 1
        if index >= end:  # A single trailing comma is valid JavaScript.
            break
    path = properties.get("path")
    if not path:
        raise ProxyError("tools.view_image object must contain a static path property")
    return path


def _rewrite_one_exec_call(
    call_source: str,
    *,
    cwd: Path,
    proxy_root: Path,
    max_edge: int,
    quality: int,
) -> str:
    open_paren = call_source.find("(")
    index = open_paren + 1
    while index < len(call_source) and call_source[index].isspace():
        index += 1
    if index >= len(call_source) or call_source[index] != "{":
        raise ProxyError("tools.view_image argument must be a static object literal")
    close_brace = _matching_delimiter(call_source, index, "{", "}")
    object_source = call_source[index : close_brace + 1]
    trailing = call_source[close_brace + 1 : -1].strip()
    if trailing not in {"", ","}:
        raise ProxyError("tools.view_image contains extra dynamic arguments")
    original_path = _parse_static_view_image_object(object_source)
    safe_path = _proxy_for_source(
        _resolve_source(original_path, cwd),
        proxy_root,
        max_edge=max_edge,
        quality=quality,
    )

    safe_object = '{"path": ' + json.dumps(str(safe_path)) + ', "detail": "high"}'
    return call_source[:index] + safe_object + call_source[close_brace + 1 :]


def rewrite_exec_source(
    source: str,
    *,
    cwd: Path,
    proxy_root: Path,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
) -> tuple[str, int]:
    """Rewrite every static ``tools.view_image`` call in JavaScript source."""

    _validate_settings(max_edge, quality)
    matches = list(_VIEW_IMAGE_CALL.finditer(source))
    references = list(_VIEW_IMAGE_REFERENCE.finditer(source))
    if len(references) != len(matches):
        raise ProxyError("only direct static tools.view_image({...}) calls are permitted")
    if not matches:
        return source, 0
    rewritten = source
    count = 0
    for match in reversed(matches):
        open_paren = match.end() - 1
        close_paren = _matching_delimiter(source, open_paren, "(", ")")
        call = source[match.start() : close_paren + 1]
        safe_call = _rewrite_one_exec_call(
            call,
            cwd=cwd,
            proxy_root=proxy_root,
            max_edge=max_edge,
            quality=quality,
        )
        rewritten = rewritten[: match.start()] + safe_call + rewritten[close_paren + 1 :]
        count += 1
    return rewritten, count


def rewrite_exec_input(
    tool_input: Any,
    *,
    cwd: Path,
    proxy_root: Path,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
) -> tuple[Any, int]:
    """Recursively rewrite code-bearing strings in a free-form ``exec`` input."""

    if isinstance(tool_input, str):
        return rewrite_exec_source(
            tool_input,
            cwd=cwd,
            proxy_root=proxy_root,
            max_edge=max_edge,
            quality=quality,
        )
    if isinstance(tool_input, Mapping):
        rewritten: dict[Any, Any] = {}
        total = 0
        for key, value in tool_input.items():
            safe_value, count = rewrite_exec_input(
                value,
                cwd=cwd,
                proxy_root=proxy_root,
                max_edge=max_edge,
                quality=quality,
            )
            rewritten[key] = safe_value
            total += count
        return rewritten, total
    if isinstance(tool_input, list):
        rewritten_list: list[Any] = []
        total = 0
        for value in tool_input:
            safe_value, count = rewrite_exec_input(
                value,
                cwd=cwd,
                proxy_root=proxy_root,
                max_edge=max_edge,
                quality=quality,
            )
            rewritten_list.append(safe_value)
            total += count
        return rewritten_list, total
    return tool_input, 0


def _allow(updated_input: Any) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "allow",
            "updatedInput": updated_input,
        }
    }


def _deny(reason: str) -> dict[str, Any]:
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": reason,
        }
    }


def process_hook_event(
    event: Any,
    *,
    proxy_root: Path,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
) -> dict[str, Any] | None:
    """Process one Codex hook event; return ``None`` for unrelated tools.

    All failures for a relevant image request become an explicit deny response.
    This prevents malformed or missing paths from falling through to the original.
    """

    if not isinstance(event, Mapping):
        return _deny("invalid PreToolUse event; original-image access was blocked")
    tool_name = event.get("tool_name")
    if not isinstance(tool_name, str):
        return _deny("PreToolUse event has no tool name; original-image access was blocked")
    is_view = (
        tool_name == "view_image"
        or tool_name.endswith("__view_image")
        or tool_name.endswith(".view_image")
    )
    is_exec = tool_name == "exec" or tool_name.endswith("__exec") or tool_name.endswith(".exec")
    if not is_view and not is_exec:
        return None
    cwd_value = event.get("cwd")
    cwd = Path(cwd_value).expanduser() if isinstance(cwd_value, str) else Path.cwd()
    tool_input = event.get("tool_input")
    try:
        if is_view:
            return _allow(
                rewrite_view_image_input(
                    tool_input,
                    cwd=cwd,
                    proxy_root=proxy_root,
                    max_edge=max_edge,
                    quality=quality,
                )
            )
        rewritten, count = rewrite_exec_input(
            tool_input,
            cwd=cwd,
            proxy_root=proxy_root,
            max_edge=max_edge,
            quality=quality,
        )
        if count == 0:
            return None
        return _allow(rewritten)
    except Exception as error:  # A relevant request must fail closed.
        return _deny(f"original-image access blocked: {error}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    hook = subparsers.add_parser("hook", help="process one PreToolUse event from stdin")
    hook.add_argument("--proxy-root", type=Path, required=True)
    hook.add_argument("--max-edge", type=int, default=DEFAULT_MAX_EDGE)
    hook.add_argument("--quality", type=int, default=DEFAULT_JPEG_QUALITY)
    return parser


def build_codex_hook_config(
    proxy_root: Path,
    *,
    max_edge: int = DEFAULT_MAX_EDGE,
    quality: int = DEFAULT_JPEG_QUALITY,
    python_executable: str | Path = sys.executable,
) -> dict[str, Any]:
    """Build app-server request overrides for bridge-owned Codex threads."""

    _validate_settings(max_edge, quality)
    root = proxy_root.expanduser().resolve()
    _secure_directory(root)
    arguments = [
        str(python_executable),
        "-m",
        "codex_feishu_bridge.visual_proxy",
        "hook",
        "--proxy-root",
        str(root),
        "--max-edge",
        str(max_edge),
        "--quality",
        str(quality),
    ]
    command = subprocess.list2cmdline(arguments) if os.name == "nt" else shlex.join(arguments)
    return {
        "bypass_hook_trust": True,
        "hooks.PreToolUse": [
            {
                "matcher": r"(^|.*[._])(?:view_image|exec)$",
                "hooks": [
                    {
                        "type": "command",
                        "command": command,
                        "timeout": 120,
                        "statusMessage": "Preparing a bounded image proxy",
                    }
                ],
            }
        ],
    }


def main(argv: list[str] | None = None) -> int:
    args = _build_parser().parse_args(argv)
    if args.command != "hook":
        return 2
    try:
        event = json.load(sys.stdin)
        response = process_hook_event(
            event,
            proxy_root=args.proxy_root,
            max_edge=args.max_edge,
            quality=args.quality,
        )
    except Exception as error:
        response = _deny(f"image proxy hook failed closed: {error}")
    if response is not None:
        print(json.dumps(response, ensure_ascii=False, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
