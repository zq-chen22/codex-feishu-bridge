#!/usr/bin/env python3
"""Validate the static GitHub Pages source without network access."""

from __future__ import annotations

import hashlib
import json
import re
import sys
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

ROOT = Path(__file__).resolve().parents[1]
SITE = ROOT / "site"
CANONICAL = "https://zq-chen22.github.io/codex-feishu-bridge/"
README_SHA256 = "a371a936d3dc3a1b0d6d082afc43735518f764f8ecdb1a1d21db648e77db0272"  # pragma: allowlist secret  # noqa: E501


class PageParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self.title_parts: list[str] = []
        self.h1_parts: list[str] = []
        self.references: set[str] = set()
        self.meta: list[dict[str, str]] = []
        self.links: list[dict[str, str]] = []
        self._in_title = False
        self._h1_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        values = {key: value or "" for key, value in attrs}
        if tag == "title":
            self._in_title = True
        if tag == "h1":
            self._h1_depth += 1
        if tag == "meta":
            self.meta.append(values)
        if tag == "link":
            self.links.append(values)
        for attribute in ("href", "src"):
            if values.get(attribute):
                self.references.add(values[attribute])

    def handle_endtag(self, tag: str) -> None:
        if tag == "title":
            self._in_title = False
        if tag == "h1" and self._h1_depth:
            self._h1_depth -= 1

    def handle_data(self, data: str) -> None:
        if self._in_title:
            self.title_parts.append(data)
        if self._h1_depth:
            self.h1_parts.append(data)


def fail(message: str) -> None:
    raise SystemExit(f"Pages validation failed: {message}")


def compact(parts: list[str]) -> str:
    return " ".join(" ".join(parts).split())


def check_required_files() -> None:
    required = {
        "index.html",
        "404.html",
        "styles.css",
        "favicon.svg",
        "robots.txt",
        "sitemap.xml",
        ".nojekyll",
    }
    missing = sorted(name for name in required if not (SITE / name).is_file())
    if missing:
        fail(f"missing files: {', '.join(missing)}")


def check_readme_frozen() -> None:
    digest = hashlib.sha256((ROOT / "README.md").read_bytes()).hexdigest()
    if digest != README_SHA256:
        fail("README.md changed despite the frozen presentation-page constraint")


def check_index() -> None:
    source = (SITE / "index.html").read_text(encoding="utf-8")
    parser = PageParser()
    parser.feed(source)

    title = compact(parser.title_parts)
    h1 = compact(parser.h1_parts)
    if title != "如何用手机远程操作电脑上的 Codex？｜飞行桥":
        fail(f"unexpected title: {title!r}")
    if "用手机" not in h1 or "Codex" not in h1:
        fail(f"H1 does not state the user problem: {h1!r}")

    descriptions = [
        item.get("content") for item in parser.meta if item.get("name") == "description"
    ]
    if not descriptions or len(descriptions[0] or "") < 50:
        fail("missing or unhelpfully short meta description")

    canonicals = [item.get("href") for item in parser.links if item.get("rel") == "canonical"]
    if canonicals != [CANONICAL]:
        fail(f"unexpected canonical URL: {canonicals!r}")

    json_ld_blocks = re.findall(
        r'<script\s+type="application/ld\+json">\s*(.*?)\s*</script>', source, re.DOTALL
    )
    if len(json_ld_blocks) != 1:
        fail("expected exactly one JSON-LD block")
    structured = json.loads(json_ld_blocks[0])
    if structured.get("@type") != "SoftwareSourceCode" or structured.get("url") != CANONICAL:
        fail("JSON-LD does not describe the canonical software source")

    forbidden = ("/home/", "app_secret", "tenant_access_token", "鼎好", "鼎盛")
    for marker in forbidden:
        if marker.casefold() in source.casefold():
            fail(f"forbidden private or credential marker found: {marker}")

    for reference in sorted(parser.references):
        parsed = urlsplit(reference)
        if parsed.scheme or parsed.netloc or reference.startswith("#"):
            continue
        relative = parsed.path.lstrip("/")
        if relative.startswith("codex-feishu-bridge/"):
            relative = relative.removeprefix("codex-feishu-bridge/")
        source_path = SITE / relative
        if source_path.is_file():
            continue
        if relative.startswith("assets/") and (ROOT / "showcase" / relative).is_file():
            continue
        fail(f"broken local reference: {reference}")


def check_discovery_files() -> None:
    robots = (SITE / "robots.txt").read_text(encoding="utf-8")
    sitemap_url = f"{CANONICAL}sitemap.xml"
    if "User-agent: *" not in robots or f"Sitemap: {sitemap_url}" not in robots:
        fail("robots.txt does not expose the canonical sitemap")

    tree = ET.parse(SITE / "sitemap.xml")
    namespace = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}
    locations = [node.text for node in tree.findall("sm:url/sm:loc", namespace)]
    if locations != [CANONICAL]:
        fail(f"unexpected sitemap URLs: {locations!r}")

    not_found = (SITE / "404.html").read_text(encoding="utf-8")
    if '<meta name="robots" content="noindex">' not in not_found:
        fail("404 page must remain excluded from search results")


def main() -> int:
    check_required_files()
    check_readme_frozen()
    check_index()
    check_discovery_files()
    print("Pages site checks passed; README.md remains frozen.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
