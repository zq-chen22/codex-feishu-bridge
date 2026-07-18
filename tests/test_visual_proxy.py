from __future__ import annotations

import io
import json
import os
import stat
import sys
from pathlib import Path

import pytest
from PIL import Image

from codex_feishu_bridge.visual_proxy import (
    VisualProxyStore,
    build_codex_hook_config,
    main,
    make_image_proxy,
    process_hook_event,
    proxy_cache_key,
    rewrite_exec_source,
)


def _rgba_source(path: Path, size: tuple[int, int] = (32, 16)) -> Path:
    image = Image.new("RGBA", size, (0, 0, 0, 0))
    image.putpixel((size[0] // 2, size[1] // 2), (255, 0, 0, 128))
    image.save(path)
    return path


def _rgb_source(path: Path, size: tuple[int, int] = (32, 16)) -> Path:
    Image.new("RGB", size, (20, 80, 160)).save(path)
    return path


def _decision(response: dict[str, object]) -> dict[str, object]:
    return response["hookSpecificOutput"]  # type: ignore[return-value,index]


def test_small_rgba_is_physically_reencoded_as_private_jpeg(tmp_path: Path) -> None:
    source = _rgba_source(tmp_path / "small.png")
    root = tmp_path / "proxies"

    result = make_image_proxy(source, root)

    assert result.path != source
    assert result.path.suffix == ".jpg"
    assert result.source_size == (32, 16)
    assert result.output_size == (32, 16)
    assert result.cache_hit is False
    assert result.path.read_bytes()[:2] == b"\xff\xd8"
    with Image.open(result.path) as image:
        assert image.format == "JPEG"
        assert image.mode == "RGB"
        assert image.size == (32, 16)
        # A fully transparent source pixel is composited over the fixed gray,
        # allowing for normal JPEG quantization around the exact value 238.
        pixel = image.getpixel((0, 0))
        assert all(220 <= channel <= 250 for channel in pixel)
        assert image.info.get("progressive") or image.info.get("progression")
    if os.name != "nt":
        assert stat.S_IMODE(root.stat().st_mode) == 0o700
        assert stat.S_IMODE(result.path.parent.stat().st_mode) == 0o700
        assert stat.S_IMODE(result.path.stat().st_mode) == 0o600


def test_large_image_is_bounded_and_exif_orientation_is_applied(tmp_path: Path) -> None:
    large = _rgb_source(tmp_path / "large.png", (2000, 1000))
    large_proxy = make_image_proxy(large, tmp_path / "proxies")
    assert large_proxy.output_size == (1024, 512)

    rotated = Image.new("RGB", (40, 20), (1, 2, 3))
    exif = rotated.getexif()
    exif[274] = 6  # Rotate 90 degrees clockwise when normalized.
    rotated_path = tmp_path / "rotated.jpg"
    rotated.save(rotated_path, exif=exif)

    rotated_proxy = make_image_proxy(rotated_path, tmp_path / "proxies")
    assert rotated_proxy.source_size == (20, 40)
    assert rotated_proxy.output_size == (20, 40)
    with Image.open(rotated_proxy.path) as image:
        assert image.getexif().get(274) is None


def test_cache_identity_includes_source_metadata_and_proxy_settings(tmp_path: Path) -> None:
    source = _rgb_source(tmp_path / "source.png")
    root = tmp_path / "proxies"

    first = make_image_proxy(source, root, max_edge=1024, quality=75)
    second = make_image_proxy(source, root, max_edge=1024, quality=75)
    assert first.path == second.path
    assert second.cache_hit is True
    assert proxy_cache_key(source, max_edge=1024, quality=75) != proxy_cache_key(
        source, max_edge=800, quality=75
    )
    assert make_image_proxy(source, root, quality=74).path != first.path

    Image.new("RGB", (33, 16), (9, 8, 7)).save(source)
    assert make_image_proxy(source, root).path != first.path


def test_visual_proxy_store_has_simple_create_interface(tmp_path: Path) -> None:
    source = _rgb_source(tmp_path / "source.png")
    path = VisualProxyStore(tmp_path / "proxies", max_edge=512, quality=70).create(source)
    assert path.is_file()
    with Image.open(path) as image:
        assert image.format == "JPEG"


def test_direct_view_image_hook_rewrites_relative_path_and_forces_high(
    tmp_path: Path,
) -> None:
    source = _rgb_source(tmp_path / "source.png")
    event = {
        "hook_event_name": "PreToolUse",
        "tool_name": "view_image",
        "cwd": str(tmp_path),
        "tool_input": {"path": source.name, "detail": "original", "extra": 1},
    }

    response = process_hook_event(event, proxy_root=tmp_path / "proxies")

    assert response is not None
    output = _decision(response)
    assert output["hookEventName"] == "PreToolUse"
    assert output["permissionDecision"] == "allow"
    updated = output["updatedInput"]
    assert updated["path"] != str(source)  # type: ignore[index]
    assert Path(updated["path"]).is_file()  # type: ignore[index,arg-type]
    assert updated["detail"] == "high"  # type: ignore[index]
    assert updated["extra"] == 1  # type: ignore[index]


def test_exec_hook_rewrites_all_static_view_image_calls(tmp_path: Path) -> None:
    first = _rgb_source(tmp_path / "one.png")
    second = _rgb_source(tmp_path / "two.png")
    source = (
        "const a = await tools.view_image({path: 'one.png', detail: 'original'});\n"
        'const b = await tools.view_image({"path": "two.png"});\n'
        "image(a.image_url); image(b.image_url);"
    )
    event = {
        "tool_name": "exec",
        "cwd": str(tmp_path),
        "tool_input": {"source": source, "other": 7},
    }

    response = process_hook_event(event, proxy_root=tmp_path / "proxies")

    assert response is not None
    output = _decision(response)
    assert output["permissionDecision"] == "allow"
    updated = output["updatedInput"]
    rewritten = updated["source"]  # type: ignore[index]
    assert "one.png" not in rewritten
    assert "two.png" not in rewritten
    assert rewritten.count('"detail": "high"') == 2
    assert updated["other"] == 7  # type: ignore[index]
    proxies = list((tmp_path / "proxies").glob("*/*.jpg"))
    assert len(proxies) == 2
    assert first.is_file() and second.is_file()


def test_namespaced_exec_is_guarded_and_managed_proxy_is_reused(
    tmp_path: Path,
) -> None:
    source = _rgb_source(tmp_path / "source.png")
    root = tmp_path / "proxies"
    existing = VisualProxyStore(root).create(source)
    event = {
        "tool_name": "functions.exec",
        "cwd": str(tmp_path),
        "tool_input": f'tools.view_image({{path: "{existing}"}})',
    }

    response = process_hook_event(event, proxy_root=root)

    assert response is not None
    rewritten = _decision(response)["updatedInput"]
    assert str(existing) in rewritten  # type: ignore[operator]
    assert len(list(root.glob("*/*.jpg"))) == 1


@pytest.mark.parametrize(
    "source",
    [
        'tools["view_image"]({path: "safe.png"})',
        "const inspect = tools.view_image; inspect({path: 'safe.png'})",
    ],
)
def test_exec_hook_denies_indirect_view_image_references(tmp_path: Path, source: str) -> None:
    response = process_hook_event(
        {"tool_name": "exec", "cwd": str(tmp_path), "tool_input": source},
        proxy_root=tmp_path / "proxies",
    )
    assert response is not None
    assert _decision(response)["permissionDecision"] == "deny"


@pytest.mark.parametrize(
    "source",
    [
        "tools.view_image({path: dynamicPath})",
        "tools.view_image({path})",
        "tools.view_image(makeInput())",
        "tools.view_image({path: `${root}/image.png`})",
        "tools.view_image({path: 'safe.png', ...dynamicInput})",
        "tools.view_image({path: 'safe.png'}, dynamicInput)",
    ],
)
def test_exec_hook_denies_dynamic_or_unparseable_image_paths(tmp_path: Path, source: str) -> None:
    response = process_hook_event(
        {"tool_name": "exec", "cwd": str(tmp_path), "tool_input": source},
        proxy_root=tmp_path / "proxies",
    )
    assert response is not None
    output = _decision(response)
    assert output["permissionDecision"] == "deny"
    assert "blocked" in output["permissionDecisionReason"]  # type: ignore[operator]


def test_related_failures_deny_but_unrelated_tools_emit_nothing(tmp_path: Path) -> None:
    missing = process_hook_event(
        {
            "tool_name": "view_image",
            "cwd": str(tmp_path),
            "tool_input": {"path": "missing.png"},
        },
        proxy_root=tmp_path / "proxies",
    )
    malformed = process_hook_event(
        {"tool_name": "view_image", "tool_input": None},
        proxy_root=tmp_path / "proxies",
    )
    unrelated = process_hook_event(
        {"tool_name": "exec", "tool_input": "text('nothing to see')"},
        proxy_root=tmp_path / "proxies",
    )
    other_tool = process_hook_event(
        {"tool_name": "exec_command", "tool_input": {}},
        proxy_root=tmp_path / "proxies",
    )
    assert _decision(missing)["permissionDecision"] == "deny"  # type: ignore[arg-type]
    assert _decision(malformed)["permissionDecision"] == "deny"  # type: ignore[arg-type]
    assert unrelated is None
    assert other_tool is None


def test_hook_config_targets_start_resume_compatible_tool_names(
    tmp_path: Path,
) -> None:
    config = build_codex_hook_config(
        tmp_path / "proxies",
        python_executable="/opt/bridge venv/bin/python",
    )

    assert config["bypass_hook_trust"] is True
    group = config["hooks.PreToolUse"][0]
    assert "view_image|exec" in group["matcher"]
    command = group["hooks"][0]["command"]
    assert "codex_feishu_bridge.visual_proxy" in command
    assert "'/opt/bridge venv/bin/python'" in command


def test_rewrite_exec_source_returns_rewritten_source_and_count(tmp_path: Path) -> None:
    source = _rgb_source(tmp_path / "source.png")
    rewritten, count = rewrite_exec_source(
        f'tools.view_image({{path: "{source}"}})',
        cwd=tmp_path,
        proxy_root=tmp_path / "proxies",
    )
    assert count == 1
    assert str(source) not in rewritten


def test_hook_cli_is_silent_for_unrelated_tool_and_denies_bad_json(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr(
        sys,
        "stdin",
        io.StringIO(json.dumps({"tool_name": "exec", "tool_input": "text('ok')"})),
    )
    assert main(["hook", "--proxy-root", str(tmp_path / "proxies")]) == 0
    assert capsys.readouterr().out == ""

    monkeypatch.setattr(sys, "stdin", io.StringIO("not json"))
    assert main(["hook", "--proxy-root", str(tmp_path / "proxies")]) == 0
    response = json.loads(capsys.readouterr().out)
    assert _decision(response)["permissionDecision"] == "deny"
