from __future__ import annotations

from pathlib import Path

import pytest
from PIL import Image

from codex_feishu_bridge.artifacts import ArtifactBroker, ArtifactError
from codex_feishu_bridge.config import BridgeConfig
from codex_feishu_bridge.models import Attachment, IncomingMessage


class FakeGateway:
    def __init__(self, payload: bytes = b"downloaded-image") -> None:
        self.payload = payload

    async def download_attachment(
        self, _app_role: str, _attachment: Attachment
    ) -> tuple[bytes, str]:
        return self.payload, "original.png"


class RecordingProxyStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.sources: list[Path] = []

    def create(self, source: Path) -> Path:
        source = source.resolve(strict=True)
        self.sources.append(source)
        self.root.mkdir(parents=True, exist_ok=True)
        proxy = self.root / f"{len(self.sources)}.jpg"
        proxy.write_bytes(b"physical-proxy")
        return proxy


class FailingProxyStore:
    def create(self, _source: Path) -> Path:
        raise RuntimeError("proxy encoder unavailable")


class OriginalReturningProxyStore:
    def create(self, source: Path) -> Path:
        return source


def bridge_config(tmp_path: Path) -> BridgeConfig:
    config = BridgeConfig(
        config_path=tmp_path / "config.toml",
        state_dir=tmp_path / "state",
        database_path=tmp_path / "state" / "bridge.sqlite",
        inbox_dir=tmp_path / "state" / "inbox",
        outbox_dir=tmp_path / "state" / "outbox",
        admin_scratch_dir=tmp_path / "state" / "admin-scratch",
        managed_workspaces_dir=tmp_path / "state" / "workspaces",
    )
    config.prepare_dirs()
    return config


def incoming_message(*attachments: Attachment) -> IncomingMessage:
    return IncomingMessage(
        message_id="om-image",
        chat_id="oc-chat",
        chat_type="group",
        app_role="conversation",
        sender_open_id="ou-owner",
        sender_user_id=None,
        sender_union_id=None,
        text="请检查图片和文件",
        message_type="post",
        create_time_ms=1,
        attachments=list(attachments),
    )


@pytest.mark.asyncio
async def test_prepare_inputs_exposes_proxy_but_keeps_non_image_attachment_path(
    tmp_path: Path,
) -> None:
    config = bridge_config(tmp_path)
    image = config.inbox_dir / "source.png"
    document = config.inbox_dir / "notes.txt"
    image.write_bytes(b"original-image")
    document.write_text("notes")
    proxy_store = RecordingProxyStore(config.state_dir / "visual-proxies")
    broker = ArtifactBroker(config, FakeGateway(), proxy_store)

    inputs = await broker.prepare_inputs(
        incoming_message(
            Attachment(
                kind="image",
                name="source.png",
                message_id="om-image",
                local_path=image,
            ),
            Attachment(
                kind="file",
                name="notes.txt",
                message_id="om-image",
                local_path=document,
            ),
        )
    )

    local_images = [item for item in inputs if item["type"] == "localImage"]
    assert local_images == [
        {
            "type": "localImage",
            "path": str((proxy_store.root / "1.jpg").resolve()),
        }
    ]
    assert proxy_store.sources == [image.resolve()]
    assert all(item.get("path") != str(image.resolve()) for item in inputs)
    descriptions = [item["text"] for item in inputs if item["type"] == "text"]
    assert any(str(document.resolve()) in text for text in descriptions)


@pytest.mark.asyncio
async def test_downloaded_image_is_staged_before_proxy_creation(tmp_path: Path) -> None:
    config = bridge_config(tmp_path)
    attachment = Attachment(
        kind="image",
        name="image",
        message_id="om-image",
        image_key="img-key",
    )
    proxy_store = RecordingProxyStore(config.state_dir / "visual-proxies")
    broker = ArtifactBroker(config, FakeGateway(), proxy_store)

    inputs = await broker.prepare_inputs(incoming_message(attachment))

    assert attachment.local_path is not None
    assert attachment.local_path.is_relative_to(config.inbox_dir)
    assert proxy_store.sources == [attachment.local_path.resolve()]
    local_image = next(item for item in inputs if item["type"] == "localImage")
    assert Path(local_image["path"]).is_relative_to(proxy_store.root)


@pytest.mark.asyncio
async def test_default_store_physically_reencodes_and_bounds_local_image(
    tmp_path: Path,
) -> None:
    config = bridge_config(tmp_path)
    source = config.inbox_dir / "large.png"
    Image.new("RGB", (2048, 1024), (12, 34, 56)).save(source)
    broker = ArtifactBroker(config, FakeGateway())

    inputs = await broker.prepare_inputs(
        incoming_message(
            Attachment(
                kind="image",
                name="large.png",
                message_id="om-image",
                local_path=source,
            )
        )
    )

    proxy = Path(next(item for item in inputs if item["type"] == "localImage")["path"])
    assert proxy != source.resolve()
    assert proxy.is_relative_to(config.state_dir / "visual-proxies")
    assert proxy.suffix == ".jpg"
    with Image.open(proxy) as image:
        assert image.format == "JPEG"
        assert image.size == (1024, 512)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "proxy_store",
    [FailingProxyStore(), OriginalReturningProxyStore()],
)
async def test_proxy_failure_never_falls_back_to_original_image(
    tmp_path: Path,
    proxy_store: FailingProxyStore | OriginalReturningProxyStore,
) -> None:
    config = bridge_config(tmp_path)
    source = config.inbox_dir / "source.png"
    source.write_bytes(b"original-image")
    broker = ArtifactBroker(config, FakeGateway(), proxy_store)

    with pytest.raises(ArtifactError):
        await broker.prepare_inputs(
            incoming_message(
                Attachment(
                    kind="image",
                    name="source.png",
                    message_id="om-image",
                    local_path=source,
                )
            )
        )
