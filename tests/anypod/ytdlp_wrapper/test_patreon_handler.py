"""Unit tests for PatreonHandler and related Patreon-specific functionality."""

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from anypod.db.types import SourceType
from anypod.ffprobe import FFProbe
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore, YtdlpInfo
from anypod.ytdlp_wrapper.handlers import (
    PatreonHandler,
    YtdlpPatreonDataError,
    YtdlpPatreonPostFilteredOutError,
)

FEED_ID = "patreon_feed"


# --- Fixtures ---


@pytest.fixture
def ffprobe_mock() -> MagicMock:
    """Provide a MagicMock for FFProbe."""
    return MagicMock(spec=FFProbe)


@pytest.fixture
def patreon_handler(ffprobe_mock: MagicMock) -> PatreonHandler:
    """Provide a PatreonHandler with injected FFProbe mock."""
    return PatreonHandler(ffprobe=ffprobe_mock)


@pytest.fixture
def base_args() -> YtdlpArgs:
    """Provide a basic YtdlpArgs instance for tests."""
    return YtdlpArgs().quiet().no_warnings()


@pytest.fixture
def minimal_entry_data() -> dict[str, Any]:
    """Provide minimal entry data for tests."""
    return {
        "id": "post123",
        "title": "Test Post Title",
        "timestamp": 1700000000,
        "epoch": 1700000000,
        "filesize": 2_000_000,
    }


@pytest.fixture
def valid_video_entry_data(minimal_entry_data: dict[str, Any]) -> dict[str, Any]:
    """Provide valid video entry data for tests."""
    data = minimal_entry_data.copy()
    data.update(
        {
            "ext": "mp4",
            "duration": 120,
            "webpage_url": "https://www.patreon.com/posts/post123",
            "thumbnail": "https://example.com/thumb.jpg",
            "description": "Test description",
            "channel": "Creator Channel",
            "filesize": 3_000_000,
        }
    )
    return data


# --- Feed metadata tests ---


@pytest.mark.unit
def test_extract_feed_metadata_full(
    patreon_handler: PatreonHandler, minimal_entry_data: dict[str, Any]
):
    """Test extract_feed_metadata with all available metadata fields."""
    data = minimal_entry_data.copy()
    data.update(
        {
            "description": "Feed description",
            "thumbnail": "https://example.com/feed.jpg",
            "channel": "Creator Channel",  # should take precedence
            "uploader": "Individual Uploader",
        }
    )
    feed = patreon_handler.extract_feed_metadata(
        FEED_ID, YtdlpInfo(data), SourceType.PLAYLIST, "https://patreon.com/creator"
    )

    assert feed.id == FEED_ID
    assert feed.title == data["title"]
    assert feed.description == data["description"]
    assert feed.author == "Creator Channel"  # channel via uploader accessor
    assert feed.remote_image_url == data["thumbnail"]


@pytest.mark.unit
def test_extract_feed_metadata_minimal(
    patreon_handler: PatreonHandler, minimal_entry_data: dict[str, Any]
):
    """Test extract_feed_metadata with minimal metadata."""
    feed = patreon_handler.extract_feed_metadata(
        FEED_ID,
        YtdlpInfo(minimal_entry_data),
        SourceType.UNKNOWN,
        "https://patreon.com/x",
    )
    assert feed.id == FEED_ID
    assert feed.source_type == SourceType.UNKNOWN
    assert feed.title == minimal_entry_data["title"]
    assert feed.author is None
    assert feed.description is None
    assert feed.remote_image_url is None


# --- Download metadata tests ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_success_with_duration(
    patreon_handler: PatreonHandler,
    ffprobe_mock: MagicMock,
    valid_video_entry_data: dict[str, Any],
):
    """Test extract_download_metadata with duration."""
    download = await patreon_handler.extract_download_metadata(
        FEED_ID, YtdlpInfo(valid_video_entry_data)
    )
    assert download.id == valid_video_entry_data["id"]
    assert download.duration == valid_video_entry_data["duration"]
    ffprobe_mock.get_duration_seconds_from_url.assert_not_called()
    assert download.source_url == valid_video_entry_data["webpage_url"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_error_when_filesize_missing(
    patreon_handler: PatreonHandler,
    valid_video_entry_data: dict[str, Any],
) -> None:
    """Test that missing filesize metadata raises."""
    valid_video_entry_data.pop("filesize")
    download_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpPatreonDataError) as exc_info:
        await patreon_handler.extract_download_metadata(FEED_ID, download_info)

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_error_when_filesize_non_positive(
    patreon_handler: PatreonHandler,
    valid_video_entry_data: dict[str, Any],
) -> None:
    """Test that non-positive filesize values raise."""
    valid_video_entry_data["filesize"] = 0
    download_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpPatreonDataError) as exc_info:
        await patreon_handler.extract_download_metadata(FEED_ID, download_info)

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_probes_duration_when_missing(
    patreon_handler: PatreonHandler,
    ffprobe_mock: MagicMock,
    minimal_entry_data: dict[str, Any],
):
    """Test extract_download_metadata with duration missing."""
    data = minimal_entry_data.copy()
    data.update(
        {
            "ext": "mp4",
            "duration": 0,
            # preferred candidate comes from requested_downloads
            "requested_downloads": [
                {"url": "https://mux.com/video.mp4"},
            ],
            "filesize": 3_100_000,
        }
    )
    ffprobe_mock.get_duration_seconds_from_url = AsyncMock(return_value=234)

    download = await patreon_handler.extract_download_metadata(FEED_ID, YtdlpInfo(data))

    assert download.duration == 234
    ffprobe_mock.get_duration_seconds_from_url.assert_awaited_once()
    args, kwargs = ffprobe_mock.get_duration_seconds_from_url.await_args
    assert args[0] == "https://mux.com/video.mp4"
    assert kwargs["headers"]["Referer"] == "https://www.patreon.com"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_filtered_when_ext_missing(
    patreon_handler: PatreonHandler,
    minimal_entry_data: dict[str, Any],
):
    """Test extract_download_metadata with ext missing."""
    with pytest.raises(YtdlpPatreonPostFilteredOutError):
        await patreon_handler.extract_download_metadata(
            FEED_ID, YtdlpInfo(minimal_entry_data)
        )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_error_when_timestamp_missing(
    patreon_handler: PatreonHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Test extract_download_metadata with timestamp missing."""
    data = valid_video_entry_data.copy()
    data.pop("timestamp")
    with pytest.raises(YtdlpPatreonDataError) as e:
        await patreon_handler.extract_download_metadata(FEED_ID, YtdlpInfo(data))
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == data["id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_extract_download_metadata_error_when_mime_unknown(
    patreon_handler: PatreonHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Test extract_download_metadata with mime unknown."""
    data = valid_video_entry_data.copy()
    data["ext"] = "totallyfakeext"
    with pytest.raises(YtdlpPatreonDataError) as e:
        await patreon_handler.extract_download_metadata(FEED_ID, YtdlpInfo(data))
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == data["id"]


# --- Determine fetch strategy tests ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info", new_callable=AsyncMock)
async def test_determine_fetch_strategy_playlist(
    mock_extract_playlist_info: AsyncMock,
    patreon_handler: PatreonHandler,
    base_args: YtdlpArgs,
):
    """Test determine_fetch_strategy with playlist."""
    initial_url = "https://patreon.com/creator"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "patreon:campaign",
            "_type": "playlist",
            "webpage_url": initial_url,
            "id": "123",
            "epoch": 1700000000,
            "title": "Creator",
        }
    )
    fetch_url, st = await patreon_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )
    assert fetch_url == initial_url
    assert st == SourceType.PLAYLIST


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info", new_callable=AsyncMock)
async def test_determine_fetch_strategy_single(
    mock_extract_playlist_info: AsyncMock,
    patreon_handler: PatreonHandler,
    base_args: YtdlpArgs,
):
    """Test determine_fetch_strategy with single video."""
    initial_url = "https://patreon.com/posts/123"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "patreon",
            "_type": "video",
            "webpage_url": initial_url,
            "id": "123",
            "epoch": 1700000000,
            "title": "Post",
        }
    )
    fetch_url, st = await patreon_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )
    assert fetch_url == initial_url
    assert st == SourceType.SINGLE_VIDEO
