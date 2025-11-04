# pyright: reportPrivateUsage=false

"""Tests for the YoutubeHandler and related YouTube-specific functionality."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from anypod.db.types import Download, DownloadStatus, SourceType
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore
from anypod.ytdlp_wrapper.handlers.youtube_handler import (
    YoutubeEntry,
    YoutubeHandler,
    YtdlpInfo,
    YtdlpYoutubeDataError,
    YtdlpYoutubeVideoFilteredOutError,
)

# --- Fixtures ---


@pytest.fixture
def youtube_handler() -> YoutubeHandler:
    """Provides a YoutubeHandler instance for the tests."""
    return YoutubeHandler()


@pytest.fixture
def base_args() -> YtdlpArgs:
    """Provides a basic YtdlpArgs instance for tests."""
    return YtdlpArgs().quiet().no_warnings()


@pytest.fixture
def valid_video_entry_data() -> dict[str, Any]:
    """Provides a minimal valid YouTube video entry dict for tests."""
    return {
        "id": "video123",
        "title": "Test Video Title",
        "timestamp": 1678886400,  # 2023-03-15T12:00:00Z
        "ext": "mp4",
        "duration": 120,
        "webpage_url": "https://www.youtube.com/watch?v=video123",
        "thumbnail": "https://example.com/thumb.jpg",
        "description": "This is a test video description",
        "epoch": 1678886400,  # yt-dlp request timestamp
        "filesize": 1_234_567,
    }


@pytest.fixture
def valid_video_entry(valid_video_entry_data: dict[str, Any]) -> YoutubeEntry:
    """Provides a YoutubeEntry instance with valid data for tests."""
    return YoutubeEntry(YtdlpInfo(valid_video_entry_data.copy()), FEED_ID)


FEED_ID = "test_feed"

# --- Tests for YoutubeHandler.extract_feed_metadata ---


@pytest.mark.unit
def test_extract_feed_metadata_with_full_metadata(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests extract_feed_metadata with all available metadata fields."""
    feed_id = "test_full_metadata_feed"
    valid_video_entry_data.update(
        {
            "title": "Test Feed Title",
            "description": "Test feed description content",
            "channel": "Test Channel Name",
            "uploader": "Test Uploader Name",
            "thumbnail": "https://example.com/feed_thumbnail.jpg",
        }
    )
    ytdlp_info = YtdlpInfo(valid_video_entry_data)
    ref_type = SourceType.CHANNEL

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type, "https://example.com/source"
    )

    assert extracted_feed.id == feed_id
    assert extracted_feed.is_enabled is True
    assert extracted_feed.source_type == SourceType.CHANNEL
    assert extracted_feed.title == "Test Feed Title"
    assert extracted_feed.description == "Test feed description content"
    assert extracted_feed.author == "Test Uploader Name"  # uploader takes precedence
    assert extracted_feed.remote_image_url == "https://example.com/feed_thumbnail.jpg"
    assert extracted_feed.subtitle is None  # Not available from yt-dlp
    assert extracted_feed.language is None  # Not available from yt-dlp


@pytest.mark.unit
def test_extract_feed_metadata_author_fallback(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests that channel is used as fallback when uploader is not available."""
    feed_id = "test_author_fallback_feed"
    valid_video_entry_data["channel"] = "Fallback Channel Name"
    # Ensure uploader is not present
    valid_video_entry_data.pop("uploader", None)
    ytdlp_info = YtdlpInfo(valid_video_entry_data)
    ref_type = SourceType.SINGLE_VIDEO

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type, "https://example.com/source"
    )

    assert extracted_feed.author == "Fallback Channel Name"


@pytest.mark.unit
def test_extract_feed_metadata_minimal_data(
    youtube_handler: YoutubeHandler,
):
    """Tests extract_feed_metadata with minimal required data only."""
    feed_id = "test_minimal_feed"
    minimal_data = {
        "id": "minimal_video_id",
        "title": "Minimal Video Title",
        "epoch": 1678886400,  # yt-dlp request timestamp
    }
    ytdlp_info = YtdlpInfo(minimal_data)
    ref_type = SourceType.UNKNOWN

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type, "https://example.com/source"
    )

    assert extracted_feed.id == feed_id
    assert extracted_feed.is_enabled is True
    assert extracted_feed.source_type == SourceType.UNKNOWN
    assert extracted_feed.title == "Minimal Video Title"
    assert extracted_feed.subtitle is None
    assert extracted_feed.description is None
    assert extracted_feed.language is None
    assert extracted_feed.author is None
    assert extracted_feed.remote_image_url is None


# --- Tests for YoutubeHandler.extract_download_metadata (formerly _parse_single_video_entry) ---


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_success_basic(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests successful parsing of a basic, valid video entry."""
    ytdlp_info = YtdlpInfo(valid_video_entry_data)
    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert isinstance(download, Download)
    assert download.id == valid_video_entry_data["id"]
    assert download.title == valid_video_entry_data["title"]
    assert download.published == datetime.fromtimestamp(
        valid_video_entry_data["timestamp"], UTC
    )
    assert download.ext == valid_video_entry_data["ext"]
    assert download.duration == valid_video_entry_data["duration"]
    assert download.source_url == valid_video_entry_data["webpage_url"]
    assert download.status == DownloadStatus.QUEUED
    assert download.remote_thumbnail_url == valid_video_entry_data["thumbnail"]
    assert download.description == valid_video_entry_data["description"]
    assert download.mime_type == "video/mp4"  # Based on ext="mp4"
    assert download.filesize == valid_video_entry_data["filesize"]
    assert download.feed_id == FEED_ID


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_success_no_description(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests successful parsing when description is missing."""
    del valid_video_entry_data["description"]
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert download.description is None


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_when_filesize_missing(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
) -> None:
    """Tests error when filesize metadata is missing."""
    valid_video_entry_data.pop("filesize")
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as exc_info:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_when_filesize_non_positive(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
) -> None:
    """Tests error when filesize metadata resolves to a non-positive value."""
    valid_video_entry_data["filesize"] = 0
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as exc_info:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "ext, expected_mime",
    [
        ("mp3", "audio/mpeg"),
        ("m4a", "audio/mp4"),
        ("mp4", "video/mp4"),
        ("webm", "video/webm"),
        ("mkv", "video/x-matroska"),
        ("flac", "audio/flac"),
        # Test with dot prefix
        (".mp3", "audio/mpeg"),
        (".mp4", "video/mp4"),
        # Test special case
        ("live", "application/octet-stream"),
        (".live", "application/octet-stream"),
    ],
)
@pytest.mark.asyncio
async def test_parse_single_video_entry_mime_type_mapping(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
    ext: str,
    expected_mime: str,
):
    """Tests that MIME type is correctly determined from file extension."""
    valid_video_entry_data["ext"] = ext
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert download.mime_type == expected_mime


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_mime_type_unknown_type(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests error for unknown extensions."""
    valid_video_entry_data["ext"] = "totallyfakeext"
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as exc_info:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.download_id == valid_video_entry_data["id"]

    assert valid_video_entry_data["ext"] in str(exc_info.value)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_success_with_upload_date(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests successful parsing when 'timestamp' is missing but 'upload_date' is present."""
    del valid_video_entry_data["timestamp"]
    expected_published = datetime(2023, 3, 15, 0, 0, 0, tzinfo=UTC)
    valid_video_entry_data["upload_date"] = expected_published.strftime("%Y%m%d")
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert download.published == expected_published


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_success_with_release_timestamp(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests successful parsing when only 'release_timestamp' is present."""
    del valid_video_entry_data["timestamp"]
    expected_published = datetime(2023, 3, 15, 12, 0, 0, tzinfo=UTC)
    valid_video_entry_data["release_timestamp"] = expected_published.timestamp()
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert download.published == expected_published


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_live_upcoming_video(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests parsing for a live/upcoming video."""
    valid_video_entry_data["is_live"] = True
    valid_video_entry_data["live_status"] = "is_upcoming"
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    assert download.status == DownloadStatus.UPCOMING
    assert download.ext == "live"
    assert download.duration == 0
    assert download.mime_type == "application/octet-stream"  # Special case for live
    assert download.filesize == valid_video_entry_data["filesize"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_source_url_priority(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests the priority for source_url: webpage_url > original_url > default."""
    entry_base_data = valid_video_entry_data.copy()
    del entry_base_data["webpage_url"]
    entry_base_data["id"] = valid_video_entry_data.get("id")

    # Case 1: webpage_url present
    entry1_data = {**entry_base_data, "webpage_url": "https://webpage.url"}
    ytdlp_info1 = YtdlpInfo(entry1_data)
    download1 = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info1)
    assert download1.source_url == "https://webpage.url"

    # Case 2: original_url present, webpage_url missing
    entry2_data = {**entry_base_data, "original_url": "https://original.url"}
    ytdlp_info2 = YtdlpInfo(entry2_data)
    download2 = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info2)
    assert download2.source_url == "https://original.url"

    # Case 3: Both missing, defaults to youtube watch URL
    entry3_data = entry_base_data.copy()
    ytdlp_info3 = YtdlpInfo(entry3_data)
    download3 = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info3)
    assert (
        download3.source_url == f"https://www.youtube.com/watch?v={entry3_data['id']}"
    )


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_duration_as_int(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests parsing when duration is an integer."""
    valid_video_entry_data["duration"] = 120
    ytdlp_info = YtdlpInfo(valid_video_entry_data)
    download = await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)
    assert download.duration == 120


@pytest.mark.unit
def test_parse_single_video_entry_error_missing_id(
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError when video ID is missing during YoutubeEntry creation."""
    data_no_id = valid_video_entry_data.copy()
    del data_no_id["id"]
    with pytest.raises(YtdlpYoutubeDataError) as e:
        YoutubeEntry(YtdlpInfo(data_no_id), FEED_ID)
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == "<missing_id>"


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_video_filtered_out(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeVideoFilteredOutError when video is filtered out (no ext/url/format_id)."""
    del valid_video_entry_data["ext"]
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeVideoFilteredOutError) as e:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_title",
    [None, "[Deleted video]", "[Private video]"],
)
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_invalid_title(
    youtube_handler: YoutubeHandler,
    bad_title: str | None,
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError for missing or invalid titles."""
    entry_id = valid_video_entry_data["id"]
    valid_video_entry_data["title"] = bad_title
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as e:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)

    if bad_title is None:
        assert "Failed to parse YouTube entry." in str(e.value)
    else:
        assert "Video unavailable or deleted" in str(e.value)
    assert e.value.download_id == entry_id


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_missing_all_dts(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError when all date/timestamp fields are missing."""
    del valid_video_entry_data["timestamp"]
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as e:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)
    assert "Missing published datetime" in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "key, bad_value, expected_msg_part",
    [
        ("timestamp", "not-a-number", "Failed to parse YouTube entry."),
        ("upload_date", "20231301", "Invalid upload date"),
        ("upload_date", "not-a-date", "Invalid upload date"),
        ("release_timestamp", "not-a-number", "Failed to parse YouTube entry."),
    ],
)
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_invalid_date_formats_for_dts(
    youtube_handler: YoutubeHandler,
    key: str,
    bad_value: Any,
    expected_msg_part: str,
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError for various invalid date/timestamp formats."""
    del valid_video_entry_data["timestamp"]
    valid_video_entry_data[key] = bad_value
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as e:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)
    assert expected_msg_part in str(e.value)
    assert e.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_missing_extension(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeVideoFilteredOutError if 'ext' is missing (filtered out by yt-dlp)."""
    del valid_video_entry_data["ext"]
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeVideoFilteredOutError) as e:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == valid_video_entry_data["id"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_duration, expected_msg_part",
    [
        ("not-a-float", "Unparsable duration"),
        (None, "Missing duration"),
        (True, "Duration had unexpected type"),
        ({"value": 60}, "Missing duration"),
    ],
)
@pytest.mark.asyncio
async def test_parse_single_video_entry_error_invalid_duration(
    youtube_handler: YoutubeHandler,
    bad_duration: Any,
    expected_msg_part: str,
    valid_video_entry_data: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError for invalid or unparsable duration values."""
    valid_video_entry_data["duration"] = bad_duration
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    with pytest.raises(YtdlpYoutubeDataError) as e:
        await youtube_handler.extract_download_metadata(FEED_ID, ytdlp_info)
    assert expected_msg_part in str(e.value)


# --- Tests for YoutubeHandler.determine_fetch_strategy ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info", new_callable=AsyncMock)
async def test_determine_fetch_strategy_single_video(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests strategy determination for a single YouTube video URL."""
    initial_url = "https://www.youtube.com/watch?v=video123"
    mock_extract_info_return = YtdlpInfo(
        {
            "extractor": "youtube",
            "webpage_url": initial_url,
            "id": "video123",
        }
    )

    mock_extract_playlist_info.return_value = mock_extract_info_return

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    mock_extract_playlist_info.assert_called_once()
    assert fetch_url == initial_url
    assert ref_type == SourceType.SINGLE_VIDEO


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_channel_main_page_finds_videos_tab(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests strategy for a main channel page, successfully finding the 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle"
    videos_tab_url = "https://www.youtube.com/@channelhandle/videos"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,  # or some resolved channel URL
            "id": "channelhandle_id",  # Main ID for YoutubeEntry
            "entries": [
                {
                    "_type": "playlist",
                    "id": "shorts_tab_id",
                    "webpage_url": "https://www.youtube.com/@channelhandle/shorts",
                },
                {
                    "_type": "playlist",
                    "id": "videos_tab_id",
                    "webpage_url": videos_tab_url,
                },
                {
                    "_type": "playlist",
                    "id": "playlists_tab_id",
                    "webpage_url": "https://www.youtube.com/@channelhandle/playlists",
                },
            ],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    mock_extract_playlist_info.assert_called_once()
    assert fetch_url == videos_tab_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_channel_main_page_no_videos_tab(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests strategy for a main channel page where 'Videos' tab is not found, defaulting to the resolved URL."""
    initial_url = "https://www.youtube.com/channel/UCxxxx"
    resolved_channel_url = (
        "https://www.youtube.com/channel/UCxxxx/resolved"  # Mock a resolved URL
    )
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "id": "UCxxxx_id",
            "entries": [
                {
                    "_type": "playlist",
                    "id": "UCxxxx_shorts_id",
                    "webpage_url": "https://www.youtube.com/channel/UCxxxx/shorts",
                },
                # No "/videos" tab
            ],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch("anypod.ytdlp_wrapper.handlers.youtube_handler.YtdlpCore.extract_playlist_info")
async def test_determine_fetch_strategy_channel_videos_tab_direct(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests strategy for a direct URL to a channel's 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle/videos"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,
            "id": "videos_page_id",
            "entries": [{"id": "v1"}, {"id": "v2"}],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )
    assert fetch_url == initial_url
    assert ref_type == SourceType.PLAYLIST


@pytest.mark.unit
@pytest.mark.asyncio
async def test_determine_fetch_strategy_playlist_url(
    youtube_handler: YoutubeHandler, base_args: YtdlpArgs
):
    """Tests strategy for a regular playlist URL."""
    initial_url = "https://www.youtube.com/playlist?list=PLxxxxxxxxxxxxx"
    mock_extract_info_return = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,
            "id": "playlist_id",
            "entries": [{"id": "v1"}, {"id": "v2"}, {"id": "v3"}],
        }
    )

    with patch.object(
        YtdlpCore, "extract_playlist_info", new_callable=AsyncMock
    ) as mock_extract_playlist_info:
        mock_extract_playlist_info.return_value = mock_extract_info_return

        fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
            FEED_ID, initial_url, base_args
        )

        mock_extract_playlist_info.assert_called_once()
        assert fetch_url == initial_url
        assert ref_type == SourceType.PLAYLIST


@pytest.mark.unit
@pytest.mark.asyncio
@patch("anypod.ytdlp_wrapper.handlers.youtube_handler.YtdlpCore.extract_playlist_info")
async def test_determine_fetch_strategy_playlists_tab_error(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests that a 'playlists' tab URL raises YtdlpYoutubeDataError."""
    initial_url = "https://www.youtube.com/@channelhandle/playlists"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "webpage_url": initial_url,
            "id": "playlists_page_id",
        }
    )
    with pytest.raises(YtdlpYoutubeDataError):
        await youtube_handler.determine_fetch_strategy(FEED_ID, initial_url, base_args)


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_discovery_fails(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests strategy when discovery (ydl_caller) returns None."""
    initial_url = "https://www.youtube.com/some_unresolvable_url"
    mock_extract_playlist_info.return_value = None

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    assert fetch_url == initial_url
    assert ref_type == SourceType.UNKNOWN


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_unknown_extractor(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests strategy for an unhandled extractor type."""
    initial_url = "https://some.other.video.site/video1"
    resolved_url_from_yt_dlp = "https://resolved.other.site/video1"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "someother:extractor",
            "webpage_url": resolved_url_from_yt_dlp,
            "id": "other_site_video_id",
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    assert fetch_url == resolved_url_from_yt_dlp
    assert ref_type == SourceType.UNKNOWN


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_channel_with_no_videos_but_has_entries(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests channel identification when entries exist but Videos tab is not found."""
    initial_url = "https://www.youtube.com/@newchannel"
    resolved_channel_url = "https://www.youtube.com/@newchannel/featured"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "id": "newchannel_id",
            "entries": [
                {
                    "_type": "playlist",
                    "id": "shorts_tab_id",
                    "webpage_url": "https://www.youtube.com/@newchannel/shorts",
                },
                {
                    "_type": "playlist",
                    "id": "community_tab_id",
                    "webpage_url": "https://www.youtube.com/@newchannel/community",
                },
                # No videos tab
            ],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    # Should fallback to using the resolved URL as CHANNEL type
    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_channel_with_empty_entries(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests channel identification when channel has no entries (empty/new channel)."""
    initial_url = "https://www.youtube.com/@emptychannel"
    resolved_channel_url = "https://www.youtube.com/@emptychannel/featured"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "id": "emptychannel_id",
            "entries": [],  # Empty channel
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    # Should identify as channel even with empty entries
    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_existing_channel_tab_not_main_page(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Tests that existing channel tabs are not treated as main channel pages."""
    initial_url = "https://www.youtube.com/@channel/shorts"
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,  # Already a specific tab
            "id": "channel_shorts_id",
            "entries": [{"id": "short1"}, {"id": "short2"}],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    # Should be treated as COLLECTION, not attempt channel tab resolution
    assert fetch_url == initial_url
    assert ref_type == SourceType.PLAYLIST


# --- Tests for YoutubeHandler source_type preservation and channel classification ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_preserves_channel_classification(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Test that channel classification is properly preserved through the discovery process.

    This test covers the YouTube channel classification fix where the source_type
    was being lost during the feed metadata synchronization process.
    """
    initial_url = "https://www.youtube.com/@testchannel"
    videos_tab_url = "https://www.youtube.com/@testchannel/videos"

    # Mock yt-dlp discovery response for a channel with videos tab
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,
            "id": "testchannel_main_id",
            "title": "Test Channel",
            "description": "A test channel description",
            "uploader": "Test Channel Creator",
            "thumbnail": "https://yt3.googleusercontent.com/testchannel_image",
            "epoch": 1678886400,  # yt-dlp request timestamp
            "entries": [
                {
                    "_type": "playlist",
                    "id": "testchannel_videos_id",
                    "webpage_url": videos_tab_url,
                    "title": "Videos",
                },
                {
                    "_type": "playlist",
                    "id": "testchannel_shorts_id",
                    "webpage_url": "https://www.youtube.com/@testchannel/shorts",
                    "title": "Shorts",
                },
            ],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    # Verify that the fetch strategy correctly identifies this as a channel
    assert fetch_url == videos_tab_url
    assert ref_type == SourceType.CHANNEL

    # Verify that if we extract feed metadata from this discovery result,
    # the source_type is correctly set to CHANNEL
    discovery_result = mock_extract_playlist_info.return_value
    extracted_feed = youtube_handler.extract_feed_metadata(
        FEED_ID, discovery_result, ref_type, initial_url
    )

    # THE CRITICAL ASSERTION: source_type should be CHANNEL, not UNKNOWN
    assert extracted_feed.source_type == SourceType.CHANNEL
    assert extracted_feed.title == "Test Channel"
    assert extracted_feed.description == "A test channel description"
    assert extracted_feed.author == "Test Channel Creator"
    assert (
        extracted_feed.remote_image_url
        == "https://yt3.googleusercontent.com/testchannel_image"
    )


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_playlist_info")
async def test_determine_fetch_strategy_channel_without_videos_tab_still_classified_correctly(
    mock_extract_playlist_info: AsyncMock,
    youtube_handler: YoutubeHandler,
    base_args: YtdlpArgs,
):
    """Test that channels without explicit videos tab are still classified as CHANNEL."""
    initial_url = "https://www.youtube.com/@newchannel"
    resolved_channel_url = "https://www.youtube.com/@newchannel/featured"

    # Mock yt-dlp discovery response for a channel with no videos tab but still channel-like
    mock_extract_playlist_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "id": "newchannel_id",
            "title": "New Channel",
            "description": "A new channel with no videos yet",
            "uploader": "New Channel Creator",
            "thumbnail": "https://yt3.googleusercontent.com/newchannel_default",
            "epoch": 1678886400,  # yt-dlp request timestamp
            "entries": [
                {
                    "_type": "playlist",
                    "id": "newchannel_community_id",
                    "webpage_url": "https://www.youtube.com/@newchannel/community",
                    "title": "Community",
                },
                # No videos tab available
            ],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, base_args
    )

    # Should still be classified as channel even without videos tab
    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL

    # Extract feed metadata and verify source_type preservation
    discovery_result = mock_extract_playlist_info.return_value
    extracted_feed = youtube_handler.extract_feed_metadata(
        FEED_ID, discovery_result, ref_type, initial_url
    )

    # Should still be CHANNEL, not UNKNOWN
    assert extracted_feed.source_type == SourceType.CHANNEL
    assert extracted_feed.title == "New Channel"


@pytest.mark.unit
def test_extract_feed_metadata_channel_specific_fields():
    """Test that channel-specific metadata fields are properly extracted."""
    feed_id = "test_channel_metadata"

    # Comprehensive channel metadata from yt-dlp
    channel_ytdlp_data = {
        "id": "channel_id_123",
        "title": "Amazing Tech Channel",
        "description": "We review the latest technology and gadgets",
        "uploader": "Tech Reviewer",
        "channel": "Amazing Tech Channel",  # Fallback for author
        "thumbnail": "https://yt3.googleusercontent.com/amazing_tech_channel_image",
        "uploader_id": "UCamazingtech123",
        "channel_id": "UCamazingtech123",
        "webpage_url": "https://www.youtube.com/@amazingtech",
        "epoch": 1678886400,  # yt-dlp request timestamp
    }

    youtube_handler = YoutubeHandler()
    ytdlp_info = YtdlpInfo(channel_ytdlp_data)

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id,
        ytdlp_info,
        SourceType.CHANNEL,
        "https://www.youtube.com/@amazingtech",
    )

    # Verify all metadata is correctly extracted
    assert extracted_feed.source_type == SourceType.CHANNEL
    assert extracted_feed.title == "Amazing Tech Channel"
    assert extracted_feed.description == "We review the latest technology and gadgets"
    assert (
        extracted_feed.author == "Tech Reviewer"
    )  # uploader takes precedence over channel
    assert (
        extracted_feed.remote_image_url
        == "https://yt3.googleusercontent.com/amazing_tech_channel_image"
    )
    assert extracted_feed.subtitle is None  # Not available from yt-dlp
    assert extracted_feed.language is None  # Not available from yt-dlp
    assert extracted_feed.id == feed_id
    assert extracted_feed.is_enabled is True


# --- Tests for thumbnail URL query parameter preservation ---


@pytest.mark.unit
def test_clean_thumbnail_url_preserves_query_parameters():
    """Test that _clean_thumbnail_url preserves query parameters required for YouTube thumbnails."""
    original_url = "https://i.ytimg.com/pl_c/PL8mG-RkN2uTw7PhlnAr4pZZz2QubIbujH/studio_square_thumbnail.jpg?sqp=CNnJ9cQG-oaymwEICOADEOADSFqi85f_AwYIwe77sQY%3D&rs=AOn4CLB5y7iZmQcD8vHcdJ4WtzLCK_wOuQ"

    # Create a YoutubeEntry instance to access the _clean_thumbnail_url method
    video_data = {
        "id": "test_video_123",
        "title": "Test Video",
        "timestamp": 1678886400,
        "ext": "mp4",
        "duration": 120,
        "webpage_url": "https://www.youtube.com/watch?v=test_video_123",
        "thumbnail": original_url,
        "epoch": 1678886400,
    }

    video_entry = YoutubeEntry(YtdlpInfo(video_data), "test_feed")
    url = video_entry.thumbnail

    # Query parameters should be preserved as they are required for YouTube thumbnails
    assert url is not None
    assert "sqp=CNnJ9cQG-oaymwEICOADEOADSFqi85f_AwYIwe77sQY%3D" in url
    assert "rs=AOn4CLB5y7iZmQcD8vHcdJ4WtzLCK_wOuQ" in url
    assert url == original_url


@pytest.mark.unit
def test_thumbnail_property_preserves_query_parameters(youtube_handler: YoutubeHandler):
    """Test that the thumbnail property preserves query parameters in URLs."""
    thumbnail_url_with_params = (
        "https://i.ytimg.com/vi/VIDEO_ID/maxresdefault.jpg?sqp=CAE&rs=AOn4CLA"
    )

    # Mock video entry data with thumbnail URL containing query parameters
    video_data = {
        "id": "test_video_123",
        "title": "Test Video",
        "timestamp": 1678886400,
        "ext": "mp4",
        "duration": 120,
        "webpage_url": "https://www.youtube.com/watch?v=test_video_123",
        "thumbnail": thumbnail_url_with_params,
        "epoch": 1678886400,
    }

    video_entry = YoutubeEntry(YtdlpInfo(video_data), "test_feed")
    result_thumbnail = video_entry.thumbnail

    # Query parameters should be preserved
    assert result_thumbnail == thumbnail_url_with_params
