#!/usr/bin/env python3
from __future__ import annotations

import json
import sys


def main() -> int:
    report = json.load(sys.stdin)
    results = report.get("results") or {}
    findings = [
        (path, int(item.get("line_number") or 0), str(item.get("type") or "unknown"))
        for path, items in results.items()
        for item in items
    ]
    if findings:
        for path, line, kind in findings:
            print(f"potential secret: {path}:{line} ({kind})", file=sys.stderr)
        return 1
    print("no potential secrets detected")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
