"""Unit tests for the Twitter yt-dlp handler."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from anypod.db.types import Download, DownloadStatus, SourceType
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore, YtdlpInfo
from anypod.ytdlp_wrapper.handlers.twitter_handler import (
    TwitterEntry,
    TwitterHandler,
    YtdlpTwitterDataError,
)

FEED_ID = "twitter_feed"
STATUS_URL = "https://x.com/interesting_aIl/status/1985052318308409785"
THUMB_URL = (
    "https://pbs.twimg.com/ext_tw_video_thumb/1985052318308409785/pu/img/example.jpg"
)
UPLOADER_ID = "interesting_aIl"


# --- Fixtures -----------------------------------------------------------------


@pytest.fixture
def twitter_handler() -> TwitterHandler:
    """Return a TwitterHandler instance."""
    return TwitterHandler()


@pytest.fixture
def entry_payload() -> dict[str, Any]:
    """Return canonical Twitter metadata from yt-dlp."""
    return {
        "id": "1985052318308409785",
        "title": "interesting_aIl - Check out this amazing video!",
        "description": "This is a test video",
        "uploader": "Interesting AI",
        "uploader_id": UPLOADER_ID,
        "extractor": "twitter",
        "timestamp": 1701388800.0,
        "upload_date": "20241201",
        "epoch": 1701388800,
        "duration": 11.666,
        "duration_string": "0:11",
        "ext": "mp4",
        "filesize": 3_200_000,
        "filesize_approx": 3_173_152,
        "webpage_url": STATUS_URL,
        "original_url": STATUS_URL,
        "thumbnail": THUMB_URL,
        "thumbnails": [
            {
                "id": 0,
                "url": THUMB_URL,
                "height": 720,
                "width": 1280,
                "preference": -1,
            }
        ],
    }


@pytest.fixture
def entry_info(entry_payload: dict[str, Any]) -> YtdlpInfo:
    """Wrap entry payload in YtdlpInfo."""
    return YtdlpInfo(entry_payload)


# --- TwitterEntry tests -------------------------------------------------------


@pytest.mark.unit
def test_twitter_entry_fields(
    entry_info: YtdlpInfo, entry_payload: dict[str, Any]
) -> None:
    """Ensure entry exposes core metadata."""
    entry = TwitterEntry(entry_info, FEED_ID)

    assert entry.feed_id == FEED_ID
    assert entry.download_id == entry_payload["id"]
    assert entry.title == entry_payload["title"]
    assert entry.uploader == entry_payload["uploader"]
    assert entry.extractor == entry_payload["extractor"]
    assert entry.description == entry_payload["description"]


@pytest.mark.unit
def test_twitter_entry_mime_type(entry_info: YtdlpInfo) -> None:
    """MIME type is derived from the extension."""
    entry = TwitterEntry(entry_info, FEED_ID)
    assert entry.mime_type == "video/mp4"


@pytest.mark.unit
def test_twitter_entry_missing_extension_raises() -> None:
    """Missing ext triggers data error."""
    payload = {
        "id": "123",
        "title": "no ext",
        "epoch": 1704067200,
    }
    entry = TwitterEntry(YtdlpInfo(payload), FEED_ID)

    with pytest.raises(YtdlpTwitterDataError):
        _ = entry.ext


# --- TwitterHandler tests -----------------------------------------------------


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info", new_callable=AsyncMock)
async def test_determine_fetch_strategy_single_video(
    mock_extract: AsyncMock, twitter_handler: TwitterHandler, entry_info: YtdlpInfo
) -> None:
    """Single-video URLs resolve as expected."""
    mock_extract.return_value = entry_info
    base_args = YtdlpArgs()

    final_url, source_type = await twitter_handler.determine_fetch_strategy(
        FEED_ID,
        STATUS_URL,
        base_args,
    )

    assert source_type == SourceType.SINGLE_VIDEO
    assert final_url == STATUS_URL


@pytest.mark.unit
def test_extract_feed_metadata(
    twitter_handler: TwitterHandler,
    entry_info: YtdlpInfo,
    entry_payload: dict[str, Any],
) -> None:
    """Feed metadata mirrors entry info."""
    feed = twitter_handler.extract_feed_metadata(
        FEED_ID,
        entry_info,
        SourceType.SINGLE_VIDEO,
        STATUS_URL,
    )

    assert feed.id == FEED_ID
    assert feed.title == entry_payload["title"]
    assert feed.author == entry_payload["uploader"]
    assert feed.remote_image_url == entry_payload["thumbnail"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_success(
    twitter_handler: TwitterHandler,
    entry_info: YtdlpInfo,
    entry_payload: dict[str, Any],
) -> None:
    """Download metadata is parsed correctly."""
    download = await twitter_handler.extract_download_metadata(FEED_ID, entry_info)

    assert download.feed_id == FEED_ID
    assert download.id == entry_payload["id"]
    assert download.source_url == STATUS_URL
    assert download.ext == entry_payload["ext"]
    assert download.mime_type == "video/mp4"
    assert download.duration == 11
    assert download.status == DownloadStatus.QUEUED


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_missing_filesize(
    twitter_handler: TwitterHandler,
    entry_payload: dict[str, Any],
) -> None:
    """Missing filesize fields fall back to placeholder value."""
    payload = {**entry_payload}
    payload.pop("filesize")
    payload.pop("filesize_approx")
    download = await twitter_handler.extract_download_metadata(
        FEED_ID, YtdlpInfo(payload)
    )

    assert download.filesize == 1


@pytest.mark.unit
def test_prepare_methods_return_unchanged(twitter_handler: TwitterHandler) -> None:
    """Prepare helpers are pass-through for Twitter."""
    base_args = YtdlpArgs()
    download = Download(
        feed_id=FEED_ID,
        id="123",
        source_url=STATUS_URL,
        title="Episode",
        published=datetime.now(UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=1,
        duration=10,
        status=DownloadStatus.QUEUED,
    )

    assert twitter_handler.prepare_playlist_info_args(base_args) is base_args
    assert twitter_handler.prepare_thumbnail_args(base_args) is base_args
    assert twitter_handler.prepare_downloads_info_args(base_args) is base_args
    assert twitter_handler.prepare_media_download_args(base_args, download) is base_args
