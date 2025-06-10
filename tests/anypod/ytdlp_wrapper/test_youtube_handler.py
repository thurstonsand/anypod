# pyright: reportPrivateUsage=false

"""Tests for the YoutubeHandler and related YouTube-specific functionality."""

from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from anypod.db.download import Download
from anypod.db.download_status import DownloadStatus
from anypod.db.source_type import SourceType
from anypod.ytdlp_wrapper.base_handler import FetchPurpose
from anypod.ytdlp_wrapper.youtube_handler import (
    ReferenceType,
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


@pytest.mark.unit
@pytest.mark.parametrize(
    "purpose",
    [
        FetchPurpose.DISCOVERY,
        FetchPurpose.METADATA_FETCH,
    ],
)
def test_get_source_specific_ydl_options_returns_empty_dict(
    youtube_handler: YoutubeHandler, purpose: FetchPurpose
):
    """Tests that get_source_specific_ydl_options currently returns an empty dict for all purposes."""
    options = youtube_handler.get_source_specific_ydl_options(purpose)
    assert options == {}, f"Expected empty dict for purpose {purpose}, got {options}"


FEED_ID = "test_feed"


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
    assert download.feed == FEED_ID


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
    """Tests YtdlpYoutubeDataError if 'ext' is missing for a non-live video."""
    del valid_video_entry._ytdlp_info._info_dict["ext"]
    # To avoid YtdlpYoutubeVideoFilteredOutError, ensure one of original_url or format_id exists
    valid_video_entry._ytdlp_info._info_dict["original_url"] = (
        "http://example.com/original"
    )

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)
    assert "Missing extension" in str(e.value)


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


@pytest.mark.unit
def test_determine_fetch_strategy_single_video(youtube_handler: YoutubeHandler):
    """Tests strategy determination for a single YouTube video URL."""
    initial_url = "https://www.youtube.com/watch?v=video123"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "youtube",
                "webpage_url": initial_url,
                "id": "video123",
            }
        )
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    mock_ydl_caller.assert_called_once_with({"playlist_items": "1-5"}, initial_url)
    assert fetch_url == initial_url
    assert ref_type == ReferenceType.SINGLE


@pytest.mark.unit
def test_determine_fetch_strategy_channel_main_page_finds_videos_tab(
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a main channel page, successfully finding the 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle"
    videos_tab_url = "https://www.youtube.com/@channelhandle/videos"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
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
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    mock_ydl_caller.assert_called_once()
    assert fetch_url == videos_tab_url
    assert ref_type == ReferenceType.CHANNEL


@pytest.mark.unit
def test_determine_fetch_strategy_channel_main_page_no_videos_tab(
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a main channel page where 'Videos' tab is not found, defaulting to the resolved URL."""
    initial_url = "https://www.youtube.com/channel/UCxxxx"
    resolved_channel_url = (
        "https://www.youtube.com/channel/UCxxxx/resolved"  # Mock a resolved URL
    )
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
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
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    assert fetch_url == resolved_channel_url
    assert ref_type == ReferenceType.CHANNEL


@pytest.mark.unit
def test_determine_fetch_strategy_channel_videos_tab_direct(
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a direct URL to a channel's 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle/videos"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "youtube:tab",
                "_type": "playlist",
                "webpage_url": initial_url,
                "id": "videos_page_id",
                "entries": [{"id": "v1"}, {"id": "v2"}],
            }
        )
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )
    assert fetch_url == initial_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_determine_fetch_strategy_playlist_url(youtube_handler: YoutubeHandler):
    """Tests strategy for a playlist URL."""
    initial_url = "https://www.youtube.com/playlist?list=PLxxxx"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "youtube:tab",
                "_type": "playlist",
                "webpage_url": initial_url,
                "id": "PLxxxx_id",
                "entries": [{"id": "v1"}, {"id": "v2"}],
            }
        )
    )
    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )
    assert fetch_url == initial_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_determine_fetch_strategy_playlists_tab_error(youtube_handler: YoutubeHandler):
    """Tests that a 'playlists' tab URL raises YtdlpYoutubeDataError."""
    initial_url = "https://www.youtube.com/@channelhandle/playlists"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "youtube:tab",
                "webpage_url": initial_url,
                "id": "playlists_page_id",
            }
        )
    )
    with pytest.raises(YtdlpYoutubeDataError):
        youtube_handler.determine_fetch_strategy(FEED_ID, initial_url, mock_ydl_caller)


@pytest.mark.unit
def test_determine_fetch_strategy_discovery_fails(youtube_handler: YoutubeHandler):
    """Tests strategy when discovery (ydl_caller) returns None."""
    initial_url = "https://www.youtube.com/some_unresolvable_url"
    mock_ydl_caller = MagicMock(return_value=None)

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    assert fetch_url == initial_url
    assert ref_type == ReferenceType.UNKNOWN_DIRECT_FETCH


@pytest.mark.unit
def test_determine_fetch_strategy_unknown_extractor(youtube_handler: YoutubeHandler):
    """Tests strategy for an unhandled extractor type."""
    initial_url = "https://some.other.video.site/video1"
    resolved_url_from_yt_dlp = "https://resolved.other.site/video1"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "someother:extractor",
                "webpage_url": resolved_url_from_yt_dlp,
                "id": "other_site_video_id",
            }
        )
    )
    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    assert fetch_url == resolved_url_from_yt_dlp
    assert ref_type == ReferenceType.UNKNOWN_RESOLVED_URL


@pytest.mark.unit
def test_determine_fetch_strategy_channel_with_no_videos_but_has_entries(
    youtube_handler: YoutubeHandler,
):
    """Tests channel identification when entries exist but Videos tab is not found."""
    initial_url = "https://www.youtube.com/@newchannel"
    resolved_channel_url = "https://www.youtube.com/@newchannel/featured"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
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
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    # Should fallback to using the resolved URL as CHANNEL type
    assert fetch_url == resolved_channel_url
    assert ref_type == ReferenceType.CHANNEL


@pytest.mark.unit
def test_determine_fetch_strategy_channel_with_empty_entries(
    youtube_handler: YoutubeHandler,
):
    """Tests channel identification when channel has no entries (empty/new channel)."""
    initial_url = "https://www.youtube.com/@emptychannel"
    resolved_channel_url = "https://www.youtube.com/@emptychannel/featured"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "youtube:tab",
                "_type": "playlist",
                "webpage_url": resolved_channel_url,
                "id": "emptychannel_id",
                "entries": [],  # Empty channel
            }
        )
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    # Should identify as channel even with empty entries
    assert fetch_url == resolved_channel_url
    assert ref_type == ReferenceType.CHANNEL


@pytest.mark.unit
def test_determine_fetch_strategy_existing_channel_tab_not_main_page(
    youtube_handler: YoutubeHandler,
):
    """Tests that existing channel tabs are not treated as main channel pages."""
    initial_url = "https://www.youtube.com/@channel/shorts"
    mock_ydl_caller = MagicMock(
        return_value=YtdlpInfo(
            {
                "extractor": "youtube:tab",
                "_type": "playlist",
                "webpage_url": initial_url,  # Already a specific tab
                "id": "channel_shorts_id",
                "entries": [{"id": "short1"}, {"id": "short2"}],
            }
        )
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        FEED_ID, initial_url, mock_ydl_caller
    )

    # Should be treated as COLLECTION, not attempt channel tab resolution
    assert fetch_url == initial_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_parse_metadata_to_downloads_incomplete_info_dict(
    youtube_handler: YoutubeHandler,
):
    """Tests that an incomplete info_dict results in an empty list of downloads."""
    incomplete_data = {"id": "id_for_init"}
    downloads = youtube_handler.parse_metadata_to_downloads(
        FEED_ID, YtdlpInfo(incomplete_data), FEED_ID, ReferenceType.SINGLE
    )
    assert downloads == []


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_single_success(
    mock_parse_single: MagicMock, youtube_handler: YoutubeHandler
):
    """Tests parsing for ReferenceType.SINGLE with successful single entry parsing."""
    mock_download = MagicMock(spec=Download)
    mock_parse_single.return_value = mock_download
    info_dict = {"id": "video1", "title": "Single Video"}
    ytdlp_info = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        FEED_ID, ytdlp_info, FEED_ID, ReferenceType.SINGLE
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
    """Tests ReferenceType.SINGLE when _parse_single_video_entry raises YtdlpYoutubeDataError."""
    feed_id = "feed_single_err"
    download_id = "video_err"
    mock_parse_single.side_effect = YtdlpYoutubeDataError(
        message="Parsing failed", feed_id=feed_id, download_id=download_id
    )
    info_dict = {"id": download_id, "title": "Problem Video"}
    ytdlp_info = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info, feed_id, ReferenceType.SINGLE
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
    """Tests ReferenceType.SINGLE when _parse_single_video_entry raises YtdlpYoutubeVideoFilteredOutError."""
    feed_id = "feed_single_filter"
    download_id = "video_filter"
    mock_parse_single.side_effect = YtdlpYoutubeVideoFilteredOutError(
        feed_id=feed_id, download_id=download_id
    )
    info_dict = {"id": download_id, "title": "Filtered Video"}
    ytdlp_info = YtdlpInfo(info_dict)

    downloads = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info, feed_id, ReferenceType.SINGLE
    )

    assert downloads == []
    expected_yt_entry = YoutubeEntry(ytdlp_info, feed_id)
    mock_parse_single.assert_called_once_with(expected_yt_entry, feed_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_collection_success(
    mock_parse_single: MagicMock, youtube_handler: YoutubeHandler
):
    """Tests parsing for ReferenceType.COLLECTION with multiple successful entries."""
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
        source_id, ytdlp_info, source_id, ReferenceType.COLLECTION
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
    """Tests ReferenceType.COLLECTION where some entries parse successfully and others fail."""
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
        feed_id, ytdlp_info_collection, feed_id, ReferenceType.COLLECTION
    )

    assert downloads == [mock_download1]
    assert mock_parse_single.call_count == 2
    mock_parse_single.assert_any_call(yt_entry1, feed_id)
    mock_parse_single.assert_any_call(yt_entry2_bad, feed_id)


@pytest.mark.unit
def test_parse_metadata_to_downloads_collection_no_entries_list(
    youtube_handler: YoutubeHandler,
):
    """Tests ReferenceType.COLLECTION when 'entries' is missing or not a list."""
    feed_id = "feed_no_entries"
    info_dict = {
        "id": "playlist_no_entries_id",
        "title": "Playlist Title",
    }
    ytdlp_info_no_entries = YtdlpInfo(info_dict)

    downloads_no_entries = youtube_handler.parse_metadata_to_downloads(
        feed_id, ytdlp_info_no_entries, feed_id, ReferenceType.COLLECTION
    )
    assert downloads_no_entries == []


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
@pytest.mark.parametrize(
    "unknown_ref_type",
    [ReferenceType.UNKNOWN_DIRECT_FETCH, ReferenceType.UNKNOWN_RESOLVED_URL],
)
def test_parse_metadata_to_downloads_unknown_type_behaves_as_single(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
    unknown_ref_type: ReferenceType,
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
    [ReferenceType.UNKNOWN_DIRECT_FETCH, ReferenceType.UNKNOWN_RESOLVED_URL],
)
def test_parse_metadata_to_downloads_unknown_type_with_playlist_shape_data(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
    unknown_ref_type: ReferenceType,
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


# --- Tests for extract_feed_metadata functionality ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "ref_type, expected_source_type",
    [
        (ReferenceType.SINGLE, SourceType.SINGLE_VIDEO),
        (ReferenceType.CHANNEL, SourceType.CHANNEL),
        (ReferenceType.COLLECTION, SourceType.PLAYLIST),
        (ReferenceType.UNKNOWN_RESOLVED_URL, SourceType.UNKNOWN),
        (ReferenceType.UNKNOWN_DIRECT_FETCH, SourceType.UNKNOWN),
    ],
)
def test_extract_feed_metadata_source_type_mapping(
    youtube_handler: YoutubeHandler,
    valid_video_entry_data: dict[str, Any],
    ref_type: ReferenceType,
    expected_source_type: SourceType,
):
    """Tests that ReferenceType is correctly mapped to SourceType in extract_feed_metadata."""
    feed_id = "test_mapping_feed"
    valid_video_entry_data["channel"] = "Test Channel"
    ytdlp_info = YtdlpInfo(valid_video_entry_data)

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type
    )

    assert extracted_feed.source_type == expected_source_type
    assert extracted_feed.id == feed_id
    assert extracted_feed.is_enabled is True


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
    ref_type = ReferenceType.CHANNEL

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type
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
    ref_type = ReferenceType.SINGLE

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type
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
    ref_type = ReferenceType.UNKNOWN_DIRECT_FETCH

    extracted_feed = youtube_handler.extract_feed_metadata(
        feed_id, ytdlp_info, ref_type
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
