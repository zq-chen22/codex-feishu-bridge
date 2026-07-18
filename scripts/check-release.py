#!/usr/bin/env python3
from __future__ import annotations

import sys
import tarfile
import zipfile
from pathlib import Path

FORBIDDEN_PARTS = {
    ".env",
    ".git",
    ".pytest_cache",
    "__pycache__",
    "config.toml",
    "credentials.env",
    "secrets.env",
}
FORBIDDEN_SUFFIXES = {".pyc", ".sqlite", ".sqlite-shm", ".sqlite-wal"}


def archive_names(path: Path) -> list[str]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            return archive.namelist()
    if path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            return archive.getnames()
    return []


def validate(path: Path) -> list[str]:
    names = archive_names(path)
    errors: list[str] = []
    if not names:
        return [f"unsupported or empty archive: {path.name}"]
    lowered = [name.lower() for name in names]
    required = {
        "license": any(name.endswith("license") for name in lowered),
        "notice": any(name.endswith("notice") for name in lowered),
        "packaged config template": any(
            name.endswith("codex_feishu_bridge/config.example.toml") for name in lowered
        ),
    }
    for label, present in required.items():
        if not present:
            errors.append(f"{path.name}: missing {label}")
    for name in names:
        candidate = Path(name)
        parts = {part.lower() for part in candidate.parts}
        if parts & FORBIDDEN_PARTS or any(
            name.lower().endswith(suffix) for suffix in FORBIDDEN_SUFFIXES
        ):
            errors.append(f"{path.name}: forbidden path {name}")
    return errors


def main() -> int:
    directory = Path(sys.argv[1] if len(sys.argv) > 1 else "dist")
    archives = sorted([*directory.glob("*.whl"), *directory.glob("*.tar.gz")])
    if not archives:
        print(f"no release archives found in {directory}", file=sys.stderr)
        return 2
    errors = [error for archive in archives for error in validate(archive)]
    if errors:
        print("\n".join(errors), file=sys.stderr)
        return 1
    print(f"validated {len(archives)} release archive(s)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
