"""Tests for Feishu message-resource download paths.

Covers the regressions that broke every inbound attachment:
- ``_read_binary_response`` must stay callable through an instance (it takes a
  single argument and is called as ``self._read_binary_response(response)``); it
  lost its ``@staticmethod`` and raised "takes 1 positional argument but 2 were
  given" for every cached image/file.
- ``_download_feishu_resource_chunked`` must stay callable through an instance
  (it takes ``self``) — it was decorated ``@staticmethod`` and raised a missing
  argument on every call.
- Feishu error 234037 (file exceeds the single-request limit) must switch the
  download to the Range-based chunked path; other error codes must not.
- The chunked path must reassemble the requested byte range faithfully.
"""

import asyncio
import io
import unittest
from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

# lark_oapi is an optional dependency (hermes-agent[feishu]) — skip the module
# when it is not installed so minimal environments don't fail collection.
pytest.importorskip("lark_oapi", reason="lark_oapi not installed — skipping Feishu download tests")

PAYLOAD = b"0123456789abcdef" * 64  # 1024 bytes


def _response(payload: bytes, headers: dict, code: int = 0):
    """Minimal stand-in for a lark SDK response."""
    return SimpleNamespace(
        file=io.BytesIO(payload),
        raw=SimpleNamespace(headers=headers),
        code=code,
        msg="ok" if code == 0 else "error",
        file_name="big.bin",
        success=lambda: code == 0,
    )


def _adapter(payload: bytes = PAYLOAD, code: int = 0):
    """FeishuAdapter instance with just enough wiring to exercise downloads."""
    from plugins.platforms.feishu.adapter import FeishuAdapter

    adapter = object.__new__(FeishuAdapter)
    seen = []

    def get(request):
        seen.append(request.headers.get("Range", ""))
        if code:
            return _response(b"", {}, code=code)
        start_s, _, end_s = request.headers["Range"].replace("bytes=", "").partition("-")
        start = int(start_s)
        end = min(int(end_s), len(payload) - 1)
        headers = {
            "Content-Type": "application/octet-stream",
            "Content-Range": f"bytes {start}-{end}/{len(payload)}",
        }
        return _response(payload[start:end + 1], headers)

    adapter._client = SimpleNamespace(
        im=SimpleNamespace(v1=SimpleNamespace(message_resource=SimpleNamespace(get=get)))
    )
    adapter._build_message_resource_request = lambda **kw: SimpleNamespace(headers={}, **kw)

    async def _run_blocking(func, *args):
        return func(*args)

    adapter._run_blocking = _run_blocking
    adapter.seen_ranges = seen
    return adapter


class TestReadBinaryResponse(unittest.TestCase):
    def test_callable_through_instance(self):
        """Regression: a missing @staticmethod made every attachment read raise TypeError."""
        adapter = _adapter()
        assert adapter._read_binary_response(_response(PAYLOAD[:16], {})) == PAYLOAD[:16]

    def test_missing_file_yields_empty_bytes(self):
        adapter = _adapter()
        assert adapter._read_binary_response(SimpleNamespace(file=None)) == b""


class TestChunkedDownload(unittest.TestCase):
    def test_reassembles_whole_payload(self):
        """Regression: an added @staticmethod made the chunked download uncallable."""
        adapter = _adapter()
        with patch("plugins.platforms.feishu.adapter._FEISHU_CHUNK_DOWNLOAD_SIZE", 256), patch(
            "plugins.platforms.feishu.adapter.cache_document_from_bytes_async",
            new=AsyncMock(return_value="/tmp/cached_big.bin"),
        ):
            path, media_type = asyncio.run(
                adapter._download_feishu_resource_chunked(
                    message_id="om_x", file_key="fk",
                    resource_type="file", fallback_filename="big.bin",
                )
            )

        assert path == "/tmp/cached_big.bin"
        assert media_type == "application/octet-stream"
        assert adapter.seen_ranges == [
            "bytes=0-255", "bytes=256-511", "bytes=512-767", "bytes=768-1023",
        ]


class TestSizeLimitFallback(unittest.TestCase):
    def _adapter_with_fetch(self, code):
        adapter = _adapter()
        adapter._fetch_message_resource = AsyncMock(return_value=_response(b"", {}, code=code))
        return adapter

    def _download(self, adapter):
        return asyncio.run(
            adapter._download_feishu_message_resource(
                message_id="om_x", file_key="fk",
                resource_type="file", fallback_filename="big.bin",
            )
        )

    def test_234037_switches_to_chunked_download(self):
        from plugins.platforms.feishu.adapter import _FEISHU_FILE_SIZE_EXCEEDED_CODE

        adapter = self._adapter_with_fetch(_FEISHU_FILE_SIZE_EXCEEDED_CODE)
        chunked = AsyncMock(return_value=("/tmp/cached.bin", "application/octet-stream"))
        adapter._download_feishu_resource_chunked = chunked

        assert self._download(adapter) == ("/tmp/cached.bin", "application/octet-stream")
        chunked.assert_awaited_once_with(
            message_id="om_x", file_key="fk",
            resource_type="file", fallback_filename="big.bin",
        )

    def test_other_error_does_not_trigger_chunked_download(self):
        adapter = self._adapter_with_fetch(99991672)
        chunked = AsyncMock(return_value=("", ""))
        adapter._download_feishu_resource_chunked = chunked

        assert self._download(adapter) == ("", "")
        chunked.assert_not_awaited()

    def test_failed_chunked_attempt_falls_through_to_file_type(self):
        """An empty ``("", "")`` result is truthy — it must not short-circuit the retry.

        audio/media downloads retry as ``file``; a failed chunked attempt on the
        first resource type has to leave that retry reachable.
        """
        from plugins.platforms.feishu.adapter import _FEISHU_FILE_SIZE_EXCEEDED_CODE

        adapter = self._adapter_with_fetch(_FEISHU_FILE_SIZE_EXCEEDED_CODE)
        chunked = AsyncMock(side_effect=[("", ""), ("/tmp/cached.ogg", "audio/ogg")])
        adapter._download_feishu_resource_chunked = chunked

        result = asyncio.run(
            adapter._download_feishu_message_resource(
                message_id="om_x", file_key="fk",
                resource_type="audio", fallback_filename="voice.ogg",
            )
        )

        assert result == ("/tmp/cached.ogg", "audio/ogg")
        assert chunked.await_count == 2
        assert [call.kwargs["resource_type"] for call in chunked.await_args_list] == ["audio", "file"]
