from datetime import UTC, datetime
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from anypod.ytdlp_wrapper.base_handler import FetchPurpose
from anypod.ytdlp_wrapper.youtube_handler import (
    Download,
    DownloadStatus,
    ReferenceType,
    YoutubeHandler,
    YtdlpYoutubeDataError,
    YtdlpYoutubeVideoFilteredOutError,
)


@pytest.fixture
def youtube_handler() -> YoutubeHandler:
    """Provides a YoutubeHandler instance for the tests."""
    return YoutubeHandler()


@pytest.fixture
def valid_video_entry() -> dict[str, Any]:
    """Provides a minimal valid YouTube video entry dict for tests."""
    return {
        "id": "video123",
        "title": "Test Video Title",
        "timestamp": 1678886400,  # 2023-03-15T12:00:00Z
        "ext": "mp4",
        "duration": 120.0,
        "webpage_url": "https://www.youtube.com/watch?v=video123",
        "thumbnail": "https://example.com/thumb.jpg",
    }


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
    """
    Tests that get_source_specific_ydl_options currently returns an empty dict
    for all purposes.
    """
    options = youtube_handler.get_source_specific_ydl_options(purpose)
    assert options == {}, f"Expected empty dict for purpose {purpose}, got {options}"


FEED_ID = "test_feed"


@pytest.mark.unit
def test_parse_single_video_entry_success_basic(
    youtube_handler: YoutubeHandler, valid_video_entry: dict[str, Any]
):
    """Tests successful parsing of a basic, valid video entry."""
    download = youtube_handler._parse_single_video_entry(  # type: ignore
        valid_video_entry, FEED_ID
    )

    assert isinstance(download, Download)
    assert download.id == valid_video_entry["id"]
    assert download.title == valid_video_entry["title"]
    assert download.published == datetime.fromtimestamp(
        float(valid_video_entry["timestamp"]), UTC
    )
    assert download.ext == valid_video_entry["ext"]
    assert download.duration == valid_video_entry["duration"]
    assert download.source_url == valid_video_entry["webpage_url"]
    assert download.status == DownloadStatus.QUEUED
    assert download.thumbnail == valid_video_entry["thumbnail"]
    assert download.feed == FEED_ID


@pytest.mark.unit
def test_parse_single_video_entry_success_with_upload_date(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """Tests successful parsing when 'timestamp' is missing but 'upload_date' is present."""
    del valid_video_entry["timestamp"]
    expected_published = datetime(2023, 3, 15, 0, 0, 0, tzinfo=UTC)
    valid_video_entry["upload_date"] = expected_published.strftime(
        "%Y%m%d"
    )  # YYYYMMDD format
    download = youtube_handler._parse_single_video_entry(  # type: ignore
        valid_video_entry, FEED_ID
    )

    assert download.published == expected_published


@pytest.mark.unit
def test_parse_single_video_entry_success_with_release_timestamp(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """Tests successful parsing when only 'release_timestamp' is present."""
    del valid_video_entry["timestamp"]
    expected_published = datetime(2023, 3, 15, 12, 0, 0, tzinfo=UTC)
    valid_video_entry["release_timestamp"] = expected_published.timestamp()
    download = youtube_handler._parse_single_video_entry(  # type: ignore
        valid_video_entry, FEED_ID
    )

    assert download.published == expected_published


@pytest.mark.unit
def test_parse_single_video_entry_live_upcoming_video(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """Tests parsing for a live/upcoming video."""
    valid_video_entry["is_live"] = True
    valid_video_entry["live_status"] = "is_upcoming"
    download = youtube_handler._parse_single_video_entry(  # type: ignore
        valid_video_entry, FEED_ID
    )

    assert download.status == DownloadStatus.UPCOMING
    assert download.ext == "live"
    assert download.duration == 0


@pytest.mark.unit
def test_parse_single_video_entry_source_url_priority(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """
    Tests the priority for source_url: webpage_url > original_url > default.
    """
    entry_without_urls = valid_video_entry.copy()
    del entry_without_urls["webpage_url"]
    entry_without_urls["id"] = "unique_id"
    # Case 1: webpage_url present
    entry1 = {**entry_without_urls, "webpage_url": "https://webpage.url"}
    download1 = youtube_handler._parse_single_video_entry(entry1, FEED_ID)  # type: ignore
    assert download1.source_url == "https://webpage.url"

    # Case 2: original_url present, webpage_url missing
    entry2 = {**entry_without_urls, "original_url": "https://original.url"}
    download2 = youtube_handler._parse_single_video_entry(entry2, FEED_ID)  # type: ignore
    assert download2.source_url == "https://original.url"

    # Case 3: Both missing, defaults to youtube watch URL
    download3 = youtube_handler._parse_single_video_entry(  # type: ignore
        entry_without_urls, FEED_ID
    )
    assert (
        download3.source_url
        == f"https://www.youtube.com/watch?v={entry_without_urls['id']}"
    )


@pytest.mark.unit
def test_parse_single_video_entry_duration_as_int(
    youtube_handler: YoutubeHandler, valid_video_entry: dict[str, Any]
):
    """Tests parsing when duration is an integer."""
    valid_video_entry["duration"] = 120  # int instead of float
    download = youtube_handler._parse_single_video_entry(  # type: ignore
        valid_video_entry, FEED_ID
    )
    assert download.duration == float(valid_video_entry["duration"])


@pytest.mark.unit
def test_parse_single_video_entry_error_missing_id(
    youtube_handler: YoutubeHandler, valid_video_entry: dict[str, Any]
):
    """Tests YtdlpYoutubeDataError when video ID is missing."""
    del valid_video_entry["id"]
    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert "Missing video ID" in str(e.value)
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == "<missing_id>"


@pytest.mark.unit
def test_parse_single_video_entry_error_video_filtered_out(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """Tests YtdlpYoutubeVideoFilteredOutError when video is filtered out (no ext/url/format_id)."""
    del valid_video_entry["ext"]
    with pytest.raises(YtdlpYoutubeVideoFilteredOutError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert e.value.feed_id == FEED_ID
    assert e.value.download_id == valid_video_entry["id"]


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_title",
    [None, "[Deleted video]", "[Private video]"],
)
def test_parse_single_video_entry_error_invalid_title(
    youtube_handler: YoutubeHandler,
    bad_title: str | None,
    valid_video_entry: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError for missing or invalid titles."""
    valid_video_entry["title"] = bad_title
    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert "Video unavailable or deleted" in str(e.value)
    assert e.value.download_id == valid_video_entry["id"]


@pytest.mark.unit
def test_parse_single_video_entry_error_missing_all_dts(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError when all date/timestamp fields are missing."""
    del valid_video_entry["timestamp"]
    valid_video_entry.pop("upload_date", None)
    valid_video_entry.pop("release_timestamp", None)

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert "Missing published datetime" in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "key, bad_value, expected_msg_part",
    [
        ("timestamp", "not-a-number", "Invalid 'timestamp'"),
        ("upload_date", "20231301", "Invalid 'upload_date'"),  # Invalid month
        ("upload_date", "not-a-date", "Invalid 'upload_date'"),
        ("release_timestamp", "not-a-number", "Invalid 'release_timestamp'"),
    ],
)
def test_parse_single_video_entry_error_invalid_date_formats_for_dts(
    youtube_handler: YoutubeHandler,
    key: str,
    bad_value: Any,
    expected_msg_part: str,
    valid_video_entry: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError for various invalid date/timestamp formats."""
    del valid_video_entry["timestamp"]
    valid_video_entry.pop("upload_date", None)
    valid_video_entry.pop("release_timestamp", None)

    valid_video_entry[key] = bad_value
    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert expected_msg_part in str(e.value)
    assert e.value.download_id == valid_video_entry["id"]


@pytest.mark.unit
def test_parse_single_video_entry_error_missing_extension(
    youtube_handler: YoutubeHandler,
    valid_video_entry: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError if 'ext' is missing for a non-live video."""
    valid_video_entry["url"] = "dummy_download_url"  # add this to prevent filtering out
    del valid_video_entry["ext"]
    valid_video_entry.pop("is_live", None)
    valid_video_entry.pop("live_status", None)

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert "Missing extension" in str(e.value)


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_duration, expected_msg_part",
    [
        ("not-a-float", "Unparsable duration"),
        (None, "Duration had unexpected type"),  # None is not int, float, or str
        (True, "Duration had unexpected type"),  # Boolean is not expected
        ({"value": 60}, "Duration had unexpected type"),  # dict is not expected
    ],
)
def test_parse_single_video_entry_error_invalid_duration(
    youtube_handler: YoutubeHandler,
    bad_duration: Any,
    expected_msg_part: str,
    valid_video_entry: dict[str, Any],
):
    """Tests YtdlpYoutubeDataError for invalid or unparsable duration values."""
    valid_video_entry.pop("is_live", None)
    valid_video_entry.pop("live_status", None)
    valid_video_entry["duration"] = bad_duration

    with pytest.raises(YtdlpYoutubeDataError) as e:
        youtube_handler._parse_single_video_entry(valid_video_entry, FEED_ID)  # type: ignore
    assert expected_msg_part in str(e.value)


@pytest.mark.unit
def test_determine_fetch_strategy_single_video(youtube_handler: YoutubeHandler):
    """Tests strategy determination for a single YouTube video URL."""
    initial_url = "https://www.youtube.com/watch?v=video123"
    mock_ydl_caller = MagicMock(
        return_value={
            "extractor": "youtube",
            "webpage_url": initial_url,
            "id": "video123",
        }
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
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
        return_value={
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,  # or some resolved channel URL
            "entries": [
                {
                    "_type": "playlist",
                    "webpage_url": "https://www.youtube.com/@channelhandle/shorts",
                },
                {"_type": "playlist", "webpage_url": videos_tab_url},
                {
                    "_type": "playlist",
                    "webpage_url": "https://www.youtube.com/@channelhandle/playlists",
                },
            ],
        }
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
    )

    mock_ydl_caller.assert_called_once()
    assert fetch_url == videos_tab_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_determine_fetch_strategy_channel_main_page_no_videos_tab(
    youtube_handler: YoutubeHandler,
):
    """
    Tests strategy for a main channel page where 'Videos' tab is not found,
    defaulting to the resolved URL.
    """
    initial_url = "https://www.youtube.com/channel/UCxxxx"
    resolved_channel_url = (
        "https://www.youtube.com/channel/UCxxxx/resolved"  # Mock a resolved URL
    )
    mock_ydl_caller = MagicMock(
        return_value={
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": resolved_channel_url,
            "entries": [
                {
                    "_type": "playlist",
                    "webpage_url": "https://www.youtube.com/channel/UCxxxx/shorts",
                },
                # No "/videos" tab
            ],
        }
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
    )

    assert fetch_url == resolved_channel_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_determine_fetch_strategy_channel_videos_tab_direct(
    youtube_handler: YoutubeHandler,
):
    """Tests strategy for a direct URL to a channel's 'Videos' tab."""
    initial_url = "https://www.youtube.com/@channelhandle/videos"
    mock_ydl_caller = MagicMock(
        return_value={
            "extractor": "youtube:tab",
            "_type": "playlist",  # yt-dlp often returns this for tab pages
            "webpage_url": initial_url,
            "entries": [{"id": "v1"}, {"id": "v2"}],  # Actual video entries
        }
    )

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
    )
    assert fetch_url == initial_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_determine_fetch_strategy_playlist_url(youtube_handler: YoutubeHandler):
    """Tests strategy for a playlist URL."""
    initial_url = "https://www.youtube.com/playlist?list=PLxxxx"
    mock_ydl_caller = MagicMock(
        return_value={
            "extractor": "youtube:tab",
            "_type": "playlist",
            "webpage_url": initial_url,
            "entries": [{"id": "v1"}, {"id": "v2"}],
        }
    )
    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
    )
    assert fetch_url == initial_url
    assert ref_type == ReferenceType.COLLECTION


@pytest.mark.unit
def test_determine_fetch_strategy_playlists_tab_error(youtube_handler: YoutubeHandler):
    """Tests that a 'playlists' tab URL raises TypeError."""
    initial_url = "https://www.youtube.com/@channelhandle/playlists"
    mock_ydl_caller = MagicMock(
        return_value={
            "extractor": "youtube:tab",
            "webpage_url": initial_url,
            # Other fields might vary
        }
    )
    with pytest.raises(YtdlpYoutubeDataError):
        youtube_handler.determine_fetch_strategy(initial_url, mock_ydl_caller)


@pytest.mark.unit
def test_determine_fetch_strategy_discovery_fails(youtube_handler: YoutubeHandler):
    """Tests strategy when discovery (ydl_caller) returns None."""
    initial_url = "https://www.youtube.com/some_unresolvable_url"
    mock_ydl_caller = MagicMock(return_value=None)

    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
    )

    assert fetch_url == initial_url
    assert ref_type == ReferenceType.UNKNOWN_DIRECT_FETCH


@pytest.mark.unit
def test_determine_fetch_strategy_unknown_extractor(youtube_handler: YoutubeHandler):
    """Tests strategy for an unhandled extractor type."""
    initial_url = "https://some.other.video.site/video1"
    resolved_url_from_yt_dlp = "https://resolved.other.site/video1"
    mock_ydl_caller = MagicMock(
        return_value={
            "extractor": "someother:extractor",
            "webpage_url": resolved_url_from_yt_dlp,
        }
    )
    fetch_url, ref_type = youtube_handler.determine_fetch_strategy(
        initial_url, mock_ydl_caller
    )

    assert fetch_url == resolved_url_from_yt_dlp
    assert ref_type == ReferenceType.UNKNOWN_RESOLVED_URL


@pytest.mark.unit
def test_parse_metadata_to_downloads_empty_info_dict(youtube_handler: YoutubeHandler):
    """Tests that an empty info_dict results in an empty list of downloads."""
    downloads = youtube_handler.parse_metadata_to_downloads(
        {}, "test_feed", ReferenceType.SINGLE
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

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict, "feed_single", ReferenceType.SINGLE
    )

    assert downloads == [mock_download]
    mock_parse_single.assert_called_once_with(info_dict, "feed_single")


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_single_parse_error(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
):
    """
    Tests ReferenceType.SINGLE when _parse_single_video_entry raises YtdlpYoutubeDataError.
    """
    feed_id = "feed_single_err"
    mock_parse_single.side_effect = YtdlpYoutubeDataError(
        feed_id, "video_err", "Parsing failed"
    )
    info_dict = {"id": "video_err", "title": "Problem Video"}

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict, feed_id, ReferenceType.SINGLE
    )

    assert downloads == []
    mock_parse_single.assert_called_once_with(info_dict, feed_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_single_filtered_out(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
):
    """
    Tests ReferenceType.SINGLE when _parse_single_video_entry raises YtdlpYoutubeVideoFilteredOutError.
    """
    feed_id = "feed_single_filter"
    entry_id = "video_filter"
    mock_parse_single.side_effect = YtdlpYoutubeVideoFilteredOutError(feed_id, entry_id)
    info_dict = {"id": entry_id, "title": "Filtered Video"}

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict, feed_id, ReferenceType.SINGLE
    )

    assert downloads == []
    mock_parse_single.assert_called_once_with(info_dict, feed_id)


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

    info_dict = {"entries": [entry1_data, entry2_data]}
    source_id = "feed_collection"

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict, source_id, ReferenceType.COLLECTION
    )

    assert downloads == [mock_download1, mock_download2]
    assert mock_parse_single.call_count == 2
    mock_parse_single.assert_any_call(entry1_data, source_id)
    mock_parse_single.assert_any_call(entry2_data, source_id)


@pytest.mark.unit
@patch.object(YoutubeHandler, "_parse_single_video_entry")
def test_parse_metadata_to_downloads_collection_with_errors(
    mock_parse_single: MagicMock,
    youtube_handler: YoutubeHandler,
):
    """
    Tests ReferenceType.COLLECTION where some entries parse successfully and others fail.
    """
    mock_download1 = MagicMock(spec=Download)
    feed_id = "feed_coll_err"
    entry1_data = {"id": "v1", "title": "Video 1"}
    entry2_data_bad = {"id": "v2_bad", "title": "Bad Video"}
    mock_parse_single.side_effect = [
        mock_download1,
        YtdlpYoutubeDataError(feed_id, entry2_data_bad["id"], "Parse failed"),
    ]
    info_dict = {"entries": [entry1_data, entry2_data_bad]}

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict, feed_id, ReferenceType.COLLECTION
    )

    assert downloads == [mock_download1]
    assert mock_parse_single.call_count == 2


@pytest.mark.unit
def test_parse_metadata_to_downloads_collection_no_entries_list(
    youtube_handler: YoutubeHandler,
):
    """Tests ReferenceType.COLLECTION when 'entries' is missing or not a list."""
    feed_id = "feed_no_entries"
    info_dict_no_entries = {"title": "Playlist Title"}  # No 'entries' key

    downloads_no_entries = youtube_handler.parse_metadata_to_downloads(
        info_dict_no_entries, feed_id, ReferenceType.COLLECTION
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
    """
    Tests that UNKNOWN reference types are parsed as if they were SINGLE,
    and logs a warning.
    """
    mock_download = MagicMock(spec=Download)
    mock_parse_single.return_value = mock_download
    info_dict = {"id": "unknown_video", "title": "Unknown Type Video"}
    source_id = "feed_unknown"

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict, source_id, unknown_ref_type
    )

    assert downloads == [mock_download]
    mock_parse_single.assert_called_once_with(info_dict, source_id)


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
    """
    Tests that UNKNOWN ref type with playlist-like data (has 'entries')
    results in an attempt to parse as a single download, which fails,
    returning an empty list and logging appropriate messages.
    """
    feed_id = "feed_unknown_playlist_shape"
    info_dict_playlist_shape = {
        "webpage_url": "http://example.com/some_resolved_url_that_is_a_playlist",
        "entries": [
            {"id": "v1", "title": "Video 1 from playlist"},
            {"id": "v2", "title": "Video 2 from playlist"},
        ],
    }

    # The attempt to parse as single should fail (e.g., missing 'id' at top level)
    mocked_error = YtdlpYoutubeDataError(feed_id, "<missing_id>", "Missing video ID")
    mock_parse_single.side_effect = mocked_error

    downloads = youtube_handler.parse_metadata_to_downloads(
        info_dict_playlist_shape, feed_id, unknown_ref_type
    )

    assert downloads == []
    # The crucial part: Unknown type tries to parse as single *even if* it looks like a playlist
    mock_parse_single.assert_called_once_with(info_dict_playlist_shape, feed_id)
