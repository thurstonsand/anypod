# pyright: reportPrivateUsage=false

"""Tests for the YoutubeHandler and related YouTube-specific functionality."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from anypod.db.types import Download, DownloadStatus, SourceType
from anypod.ytdlp_wrapper.core import YtdlpCore
from anypod.ytdlp_wrapper.youtube_handler import (
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
    assert extracted_feed.image_url == "https://example.com/feed_thumbnail.jpg"
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
    assert extracted_feed.image_url is None


# --- Tests for YoutubeHandler._parse_single_video_entry ---


@pytest.mark.unit
def test_parse_single_video_entry_success_basic(
    youtube_handler: YoutubeHandler, valid_video_entry: YoutubeEntry
):
    """Tests successful parsing of a basic, valid video entry."""
    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    assert isinstance(download, Download)
    assert download.id == valid_video_entry.download_id
    assert download.title == valid_video_entry.title
    assert download.published == valid_video_entry.timestamp
    assert download.ext == valid_video_entry.ext
    assert download.duration == valid_video_entry.duration
    assert download.source_url == valid_video_entry.webpage_url
    assert download.status == DownloadStatus.QUEUED
    assert download.thumbnail == valid_video_entry.thumbnail
    assert download.description == valid_video_entry.description
    assert download.mime_type == "video/mp4"  # Based on ext="mp4"
    assert download.filesize == 0  # Default for QUEUED status
    assert download.feed_id == FEED_ID


@pytest.mark.unit
def test_parse_single_video_entry_success_no_description(
    youtube_handler: YoutubeHandler, valid_video_entry: YoutubeEntry
):
    """Tests successful parsing when description is missing."""
    del valid_video_entry._ytdlp_info._info_dict["description"]

    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    assert download.description is None


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
def test_parse_single_video_entry_mime_type_mapping(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
    ext: str,
    expected_mime: str,
):
    """Tests that MIME type is correctly determined from file extension."""
    valid_video_entry._ytdlp_info._info_dict["ext"] = ext

    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    assert download.mime_type == expected_mime


@pytest.mark.unit
def test_parse_single_video_entry_mime_type_unknown_type(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests YoutubeEntry.mime_type property raises error for unknown extensions."""
    valid_video_entry_data["ext"] = "totallyfakeext"
    entry = YoutubeEntry(YtdlpInfo(valid_video_entry_data), "test_feed")

    with pytest.raises(YtdlpYoutubeDataError) as exc_info:
        _ = youtube_handler._parse_single_video_entry(entry, FEED_ID).mime_type

    assert exc_info.value.feed_id == FEED_ID
    assert exc_info.value.download_id == entry.download_id

    assert valid_video_entry_data["ext"] in str(exc_info.value)


@pytest.mark.unit
def test_parse_single_video_entry_success_with_upload_date(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
):
    """Tests successful parsing when 'timestamp' is missing but 'upload_date' is present."""
    del valid_video_entry._ytdlp_info._info_dict["timestamp"]
    expected_published = datetime(2023, 3, 15, 0, 0, 0, tzinfo=UTC)
    valid_video_entry._ytdlp_info._info_dict["upload_date"] = (
        expected_published.strftime("%Y%m%d")
    )

    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    assert download.published == expected_published


@pytest.mark.unit
def test_parse_single_video_entry_success_with_release_timestamp(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
):
    """Tests successful parsing when only 'release_timestamp' is present."""
    del valid_video_entry._ytdlp_info._info_dict["timestamp"]
    expected_published = datetime(2023, 3, 15, 12, 0, 0, tzinfo=UTC)
    valid_video_entry._ytdlp_info._info_dict["release_timestamp"] = (
        expected_published.timestamp()
    )

    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    assert download.published == expected_published


@pytest.mark.unit
def test_parse_single_video_entry_live_upcoming_video(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
):
    """Tests parsing for a live/upcoming video."""
    valid_video_entry._ytdlp_info._info_dict["is_live"] = True
    valid_video_entry._ytdlp_info._info_dict["live_status"] = "is_upcoming"

    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    assert download.status == DownloadStatus.UPCOMING
    assert download.ext == "live"
    assert download.duration == 0
    assert download.mime_type == "application/octet-stream"  # Special case for live
    assert download.filesize == 0


@pytest.mark.unit
def test_parse_single_video_entry_source_url_priority(
    youtube_handler: YoutubeHandler, valid_video_entry_data: dict[str, Any]
):
    """Tests the priority for source_url: webpage_url > original_url > default."""
    entry_base_data = valid_video_entry_data.copy()
    del entry_base_data["webpage_url"]
    entry_base_data["id"] = valid_video_entry_data.get("id")

    # Case 1: webpage_url present
    entry1_data = {**entry_base_data, "webpage_url": "https://webpage.url"}
    yt_entry1 = YoutubeEntry(YtdlpInfo(entry1_data), FEED_ID)
    download1 = youtube_handler._parse_single_video_entry(yt_entry1, FEED_ID)
    assert download1.source_url == "https://webpage.url"

    # Case 2: original_url present, webpage_url missing
    entry2_data = {**entry_base_data, "original_url": "https://original.url"}
    yt_entry2 = YoutubeEntry(YtdlpInfo(entry2_data), FEED_ID)
    download2 = youtube_handler._parse_single_video_entry(yt_entry2, FEED_ID)
    assert download2.source_url == "https://original.url"

    # Case 3: Both missing, defaults to youtube watch URL
    entry3_data = entry_base_data.copy()
    yt_entry3 = YoutubeEntry(YtdlpInfo(entry3_data), FEED_ID)
    download3 = youtube_handler._parse_single_video_entry(yt_entry3, FEED_ID)
    assert (
        download3.source_url == f"https://www.youtube.com/watch?v={entry3_data['id']}"
    )


@pytest.mark.unit
def test_parse_single_video_entry_duration_as_int(
    youtube_handler: YoutubeHandler, valid_video_entry: YoutubeEntry
):
    """Tests parsing when duration is an integer."""
    valid_video_entry._ytdlp_info._info_dict["duration"] = 120
    download = youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
    assert download.duration == int(
        valid_video_entry._ytdlp_info._info_dict["duration"]
    )


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
def test_parse_single_video_entry_error_video_filtered_out(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
):
    """Tests YtdlpYoutubeVideoFilteredOutError when video is filtered out (no ext/url/format_id)."""
    del valid_video_entry._ytdlp_info._info_dict["ext"]

    with pytest.raises(YtdlpYoutubeVideoFilteredOutError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == valid_video_entry.download_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_title",
    [None, "[Deleted video]", "[Private video]"],
)
def test_parse_single_video_entry_error_invalid_title(
    youtube_handler: YoutubeHandler,
    bad_title: str | None,
    valid_video_entry: YoutubeEntry,
):
    """Tests YtdlpYoutubeDataError for missing or invalid titles."""
    entry_id = valid_video_entry.download_id
    valid_video_entry._ytdlp_info._info_dict["title"] = bad_title

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)

    if bad_title is None:
        assert "Failed to parse YouTube entry." in str(e.value)
    else:
        assert "Video unavailable or deleted" in str(e.value)
    assert e.value.download_id == entry_id


@pytest.mark.unit
def test_parse_single_video_entry_error_missing_all_dts(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
):
    """Tests YtdlpYoutubeDataError when all date/timestamp fields are missing."""
    del valid_video_entry._ytdlp_info._info_dict["timestamp"]

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
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
def test_parse_single_video_entry_error_invalid_date_formats_for_dts(
    youtube_handler: YoutubeHandler,
    key: str,
    bad_value: Any,
    expected_msg_part: str,
    valid_video_entry: YoutubeEntry,
):
    """Tests YtdlpYoutubeDataError for various invalid date/timestamp formats."""
    del valid_video_entry._ytdlp_info._info_dict["timestamp"]
    valid_video_entry._ytdlp_info._info_dict[key] = bad_value

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
    assert expected_msg_part in str(e.value)
    assert e.value.download_id == valid_video_entry.download_id


@pytest.mark.unit
def test_parse_single_video_entry_error_missing_extension(
    youtube_handler: YoutubeHandler,
    valid_video_entry: YoutubeEntry,
):
    """Tests YtdlpYoutubeVideoFilteredOutError if 'ext' is missing (filtered out by yt-dlp)."""
    del valid_video_entry._ytdlp_info._info_dict["ext"]

    with pytest.raises(YtdlpYoutubeVideoFilteredOutError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == valid_video_entry.download_id


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_duration, expected_msg_part",
    [
        ("not-a-float", "Unparsable duration"),
        (None, "Failed to parse YouTube entry."),
        (True, "Duration had unexpected type"),
        ({"value": 60}, "Failed to parse YouTube entry."),
    ],
)
def test_parse_single_video_entry_error_invalid_duration(
    youtube_handler: YoutubeHandler,
    bad_duration: Any,
    expected_msg_part: str,
    valid_video_entry: YoutubeEntry,
):
    """Tests YtdlpYoutubeDataError for invalid or unparsable duration values."""
    valid_video_entry._ytdlp_info._info_dict["duration"] = bad_duration

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
    assert expected_msg_part in str(e.value)


# --- Tests for YoutubeHandler.determine_fetch_strategy ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info", new_callable=AsyncMock)
async def test_determine_fetch_strategy_single_video(
    mock_extract_info: AsyncMock, youtube_handler: YoutubeHandler
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

    mock_extract_info.return_value = mock_extract_info_return

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url
    )

    mock_extract_info.assert_called_once()
    assert fetch_url == initial_url
    assert ref_type == SourceType.SINGLE_VIDEO


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_channel_main_page_finds_videos_tab(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a main channel page, successfully finding the 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle"
    videos_tab_url = "https://www.youtube.com/@channelhandle/videos"
    mock_extract_info.return_value = YtdlpInfo(
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
        FEED_ID, initial_url
    )

    mock_extract_info.assert_called_once()
    assert fetch_url == videos_tab_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_channel_main_page_no_videos_tab(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a main channel page where 'Videos' tab is not found, defaulting to the resolved URL."""
    initial_url = "https://www.youtube.com/channel/UCxxxx"
    resolved_channel_url = (
        "https://www.youtube.com/channel/UCxxxx/resolved"  # Mock a resolved URL
    )
    mock_extract_info.return_value = YtdlpInfo(
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
        FEED_ID, initial_url
    )

    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch("anypod.ytdlp_wrapper.youtube_handler.YtdlpCore.extract_info")
async def test_determine_fetch_strategy_channel_videos_tab_direct(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a direct URL to a channel's 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle/videos"
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,
            "id": "videos_page_id",
            "entries": [{"id": "v1"}, {"id": "v2"}],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url
    )
    assert fetch_url == initial_url
    assert ref_type == SourceType.PLAYLIST


@pytest.mark.unit
@pytest.mark.asyncio
async def test_determine_fetch_strategy_playlist_url(youtube_handler: YoutubeHandler):
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
        YtdlpCore, "extract_info", new_callable=AsyncMock
    ) as mock_extract_info:
        mock_extract_info.return_value = mock_extract_info_return

        fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
            FEED_ID, initial_url
        )

        mock_extract_info.assert_called_once()
        assert fetch_url == initial_url
        assert ref_type == SourceType.PLAYLIST


@pytest.mark.unit
@pytest.mark.asyncio
@patch("anypod.ytdlp_wrapper.youtube_handler.YtdlpCore.extract_info")
async def test_determine_fetch_strategy_playlists_tab_error(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests that a 'playlists' tab URL raises YtdlpYoutubeDataError."""
    initial_url = "https://www.youtube.com/@channelhandle/playlists"
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "webpage_url": initial_url,
            "id": "playlists_page_id",
        }
    )
    with pytest.raises(YtdlpYoutubeDataError):
        await youtube_handler.determine_fetch_strategy(FEED_ID, initial_url)


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_discovery_fails(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests strategy when discovery (ydl_caller) returns None."""
    initial_url = "https://www.youtube.com/some_unresolvable_url"
    mock_extract_info.return_value = None

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url
    )

    assert fetch_url == initial_url
    assert ref_type == SourceType.UNKNOWN


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_unknown_extractor(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for an unhandled extractor type."""
    initial_url = "https://some.other.video.site/video1"
    resolved_url_from_yt_dlp = "https://resolved.other.site/video1"
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "someother:extractor",
            "webpage_url": resolved_url_from_yt_dlp,
            "id": "other_site_video_id",
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url
    )

    assert fetch_url == resolved_url_from_yt_dlp
    assert ref_type == SourceType.UNKNOWN


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_channel_with_no_videos_but_has_entries(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests channel identification when entries exist but Videos tab is not found."""
    initial_url = "https://www.youtube.com/@newchannel"
    resolved_channel_url = "https://www.youtube.com/@newchannel/featured"
    mock_extract_info.return_value = YtdlpInfo(
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
        FEED_ID, initial_url
    )

    # Should fallback to using the resolved URL as CHANNEL type
    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_channel_with_empty_entries(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests channel identification when channel has no entries (empty/new channel)."""
    initial_url = "https://www.youtube.com/@emptychannel"
    resolved_channel_url = "https://www.youtube.com/@emptychannel/featured"
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "id": "emptychannel_id",
            "entries": [],  # Empty channel
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url
    )

    # Should identify as channel even with empty entries
    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_existing_channel_tab_not_main_page(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Tests that existing channel tabs are not treated as main channel pages."""
    initial_url = "https://www.youtube.com/@channel/shorts"
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,  # Already a specific tab
            "id": "channel_shorts_id",
            "entries": [{"id": "short1"}, {"id": "short2"}],
        }
    )

    fetch_url, ref_type = await youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url
    )

    # Should be treated as COLLECTION, not attempt channel tab resolution
    assert fetch_url == initial_url
    assert ref_type == SourceType.PLAYLIST


# --- Tests for YoutubeHandler source_type preservation and channel classification ---


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_preserves_channel_classification(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Test that channel classification is properly preserved through the discovery process.

    This test covers the YouTube channel classification fix where the source_type
    was being lost during the feed metadata synchronization process.
    """
    initial_url = "https://www.youtube.com/@testchannel"
    videos_tab_url = "https://www.youtube.com/@testchannel/videos"

    # Mock yt-dlp discovery response for a channel with videos tab
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,
            "id": "testchannel_main_id",
            "title": "Test Channel",
            "description": "A test channel description",
            "uploader": "Test Channel Creator",
            "thumbnail": "https://yt3.googleusercontent.com/testchannel_image",
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
        FEED_ID, initial_url
    )

    # Verify that the fetch strategy correctly identifies this as a channel
    assert fetch_url == videos_tab_url
    assert ref_type == SourceType.CHANNEL

    # Verify that if we extract feed metadata from this discovery result,
    # the source_type is correctly set to CHANNEL
    discovery_result = mock_extract_info.return_value
    extracted_feed = youtube_handler.extract_feed_metadata(
        FEED_ID, discovery_result, ref_type, initial_url
    )

    # THE CRITICAL ASSERTION: source_type should be CHANNEL, not UNKNOWN
    assert extracted_feed.source_type == SourceType.CHANNEL
    assert extracted_feed.title == "Test Channel"
    assert extracted_feed.description == "A test channel description"
    assert extracted_feed.author == "Test Channel Creator"
    assert (
        extracted_feed.image_url
        == "https://yt3.googleusercontent.com/testchannel_image"
    )


@pytest.mark.unit
@pytest.mark.asyncio
@patch.object(YtdlpCore, "extract_info")
async def test_determine_fetch_strategy_channel_without_videos_tab_still_classified_correctly(
    mock_extract_info: AsyncMock,
    youtube_handler: YoutubeHandler,
):
    """Test that channels without explicit videos tab are still classified as CHANNEL."""
    initial_url = "https://www.youtube.com/@newchannel"
    resolved_channel_url = "https://www.youtube.com/@newchannel/featured"

    # Mock yt-dlp discovery response for a channel with no videos tab but still channel-like
    mock_extract_info.return_value = YtdlpInfo(
        {
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "id": "newchannel_id",
            "title": "New Channel",
            "description": "A new channel with no videos yet",
            "uploader": "New Channel Creator",
            "thumbnail": "https://yt3.googleusercontent.com/newchannel_default",
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
        FEED_ID, initial_url
    )

    # Should still be classified as channel even without videos tab
    assert fetch_url == resolved_channel_url
    assert ref_type == SourceType.CHANNEL

    # Extract feed metadata and verify source_type preservation
    discovery_result = mock_extract_info.return_value
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
        extracted_feed.image_url
        == "https://yt3.googleusercontent.com/amazing_tech_channel_image"
    )
    assert extracted_feed.subtitle is None  # Not available from yt-dlp
    assert extracted_feed.language is None  # Not available from yt-dlp
    assert extracted_feed.id == feed_id
    assert extracted_feed.is_enabled is True


# --- Tests for YoutubeHandler.parse_metadata_to_downloads ---


@pytest.mark.unit
def test_parse_metadata_to_downloads_incomplete_info_dict(
    youtube_handler: YoutubeHandler,
):
    """Tests that an incomplete info_dict results in an empty list of downloads."""
    incomplete_data = {"id": "id_for_init"}
    downloads = youtube_handler.parse_metadata_to_downloads(
        FEED_ID, YtdlpInfo(incomplete_data), FEED_ID, SourceType.SINGLE_VIDEO
    )
    assert downloads == []


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_single_success(
    mock_parse_single: MagicMock, youtube_handler: YoutubeHandler
):
    """Tests parsing for SourceType.SINGLE_VIDEO with successful single entry parsing."""
    mock_download = MagicMock(spec=Download)
    mock_parse_single.return_value = mock_download
    info_dict = {"id": "video1", "title": "Single Video"}
    ytdlp_info = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        FEED_ID, ytdlp_info, FEED_ID, SourceType.SINGLE_VIDEO
    )

    assert downloads == [mock_download]
    expected_yt_entry = YoutubeEntry(ytdlp_info, FEED_ID)
    mock_parse_single.assert_called_once_with(expected_yt_entry, FEED_ID)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_single_parse_error(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
):
    """Tests SourceType.SINGLE_VIDEO when _parse_single_video_entry raises YtdlpYoutubeDataError."""
    feed_id = "feed_single_err"
    download_id = "video_err"
    mock_parse_single.side_effect = YtdlpYoutubeDataError(
        message="Parsing failed", feed_id=feed_id, download_id=download_id
    )
    info_dict = {"id": download_id, "title": "Problem Video"}
    ytdlp_info = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info, feed_id, SourceType.SINGLE_VIDEO
    )

    assert downloads == []
    expected_yt_entry = YoutubeEntry(ytdlp_info, feed_id)
    mock_parse_single.assert_called_once_with(expected_yt_entry, feed_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_single_filtered_out(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
):
    """Tests SourceType.SINGLE_VIDEO when _parse_single_video_entry raises YtdlpYoutubeVideoFilteredOutError."""
    feed_id = "feed_single_filter"
    download_id = "video_filter"
    mock_parse_single.side_effect = YtdlpYoutubeVideoFilteredOutError(
        feed_id=feed_id, download_id=download_id
    )
    info_dict = {"id": download_id, "title": "Filtered Video"}
    ytdlp_info = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info, feed_id, SourceType.SINGLE_VIDEO
    )

    assert downloads == []
    expected_yt_entry = YoutubeEntry(ytdlp_info, feed_id)
    mock_parse_single.assert_called_once_with(expected_yt_entry, feed_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_collection_success(
    mock_parse_single: MagicMock, youtube_handler: YoutubeHandler
):
    """Tests parsing for SourceType.PLAYLIST with multiple successful entries."""
    mock_download1 = MagicMock(spec=Download)
    mock_download2 = MagicMock(spec=Download)
    entry1_data = {"id": "v1", "title": "Video 1"}
    entry2_data = {"id": "v2", "title": "Video 2"}
    mock_parse_single.side_effect = [mock_download1, mock_download2]

    info_dict = {
        "id": "playlist123",
        "entries": [entry1_data, entry2_data],
    }
    ytdlp_info = YtdlpInfo(info_dict)
    source_id = "feed_collection"

    downloads = youtube_handler.parse_metadata_to_downloads(
        source_id, ytdlp_info, source_id, SourceType.PLAYLIST
    )

    assert downloads == [mock_download1, mock_download2]
    assert mock_parse_single.call_count == 2

    expected_yt_entry1 = YoutubeEntry(YtdlpInfo(entry1_data), source_id)
    expected_yt_entry2 = YoutubeEntry(YtdlpInfo(entry2_data), source_id)

    mock_parse_single.assert_any_call(expected_yt_entry1, source_id)
    mock_parse_single.assert_any_call(expected_yt_entry2, source_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_collection_with_errors(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
):
    """Tests SourceType.PLAYLIST where some entries parse successfully and others fail."""
    mock_download1 = MagicMock(spec=Download)
    feed_id = "feed_coll_err"
    entry1_data = {"id": "v1", "title": "Video 1"}
    entry2_data_bad = {"id": "v2_bad", "title": "Bad Video"}

    yt_entry1 = YoutubeEntry(YtdlpInfo(entry1_data), feed_id)
    yt_entry2_bad = YoutubeEntry(YtdlpInfo(entry2_data_bad), feed_id)

    mock_parse_single.side_effect = [
        mock_download1,  # Corresponds to call with yt_entry1
        YtdlpYoutubeDataError(  # Corresponds to call with yt_entry2_bad
            message="Parse failed", feed_id=feed_id, download_id=entry2_data_bad["id"]
        ),
    ]
    info_dict = {
        "id": "playlist_err_coll_id",
        "entries": [entry1_data, entry2_data_bad],
    }
    ytdlp_info_collection = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info_collection, feed_id, SourceType.PLAYLIST
    )

    assert downloads == [mock_download1]
    assert mock_parse_single.call_count == 2
    mock_parse_single.assert_any_call(yt_entry1, feed_id)
    mock_parse_single.assert_any_call(yt_entry2_bad, feed_id)


@pytest.mark.unit
def test_parse_metadata_to_downloads_collection_no_entries_list(
    youtube_handler: YoutubeHandler,
):
    """Tests SourceType.PLAYLIST when 'entries' is missing or not a list."""
    feed_id = "feed_no_entries"
    info_dict = {
        "id": "playlist_no_entries_id",
        "title": "Playlist Title",
    }
    ytdlp_info_no_entries = YtdlpInfo(info_dict)

    downloads_no_entries = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info_no_entries, feed_id, SourceType.PLAYLIST
    )
    assert downloads_no_entries == []


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
@pytest.mark.parametrize(
    "unknown_ref_type",
    [SourceType.UNKNOWN, SourceType.UNKNOWN],
)
def test_parse_metadata_to_downloads_unknown_type_behaves_as_single(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
    unknown_ref_type: SourceType,
):
    """Tests that UNKNOWN reference types are parsed as if they were SINGLE, and logs a warning."""
    mock_download = MagicMock(spec=Download)
    mock_parse_single.return_value = mock_download
    info_dict = {"id": "unknown_video", "title": "Unknown Type Video"}
    ytdlp_info = YtdlpInfo(info_dict)
    source_id = "feed_unknown"

    downloads = youtube_handler.parse_metadata_to_downloads(
        source_id, ytdlp_info, source_id, unknown_ref_type
    )

    assert downloads == [mock_download]
    expected_yt_entry = YoutubeEntry(ytdlp_info, source_id)
    mock_parse_single.assert_called_once_with(expected_yt_entry, source_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
@pytest.mark.parametrize(
    "unknown_ref_type",
    [SourceType.UNKNOWN, SourceType.UNKNOWN],
)
def test_parse_metadata_to_downloads_unknown_type_with_playlist_shape_data(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
    unknown_ref_type: SourceType,
):
    """Tests that UNKNOWN ref type with playlist-like data (has 'entries').

    results in an attempt to parse as a single download, which fails,
    returning an empty list and logging appropriate messages.
    """
    feed_id = "feed_unknown_playlist_shape"
    top_level_id = "top_level_playlist_id_for_unknown"
    info_dict_playlist_shape_data = {
        "id": top_level_id,
        "webpage_url": "http://example.com/some_resolved_url_that_is_a_playlist",
        "entries": [
            {"id": "v1", "title": "Video 1 from playlist"},
            {"id": "v2", "title": "Video 2 from playlist"},
        ],
    }
    ytdlp_info_playlist_shape = YtdlpInfo(info_dict_playlist_shape_data)

    mock_parse_single.side_effect = YtdlpYoutubeDataError(
        message="Cannot parse playlist as single video",
        feed_id=feed_id,
        download_id=top_level_id,
    )

    downloads = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info_playlist_shape, feed_id, unknown_ref_type
    )

    assert downloads == []
    expected_yt_entry = YoutubeEntry(ytdlp_info_playlist_shape, feed_id)
    mock_parse_single.assert_called_once_with(expected_yt_entry, feed_id)
