# pyright: reportPrivateUsage=false

"""Tests for the YtdlpWrapper class and its yt-dlp integration functionality."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from anypod.db.app_state_db import AppStateDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper import YtdlpWrapper
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore, YtdlpInfo
from anypod.ytdlp_wrapper.youtube_handler import YoutubeHandler


@pytest.fixture
def mock_youtube_handler() -> MagicMock:
    """Fixture to provide a mocked YoutubeHandler."""
    handler = MagicMock(spec=YoutubeHandler)
    return handler


@pytest.fixture
def app_state_db_mock() -> MagicMock:
    """Provide a mocked AppStateDatabase with update gating enabled."""
    app_state_db = MagicMock(spec=AppStateDatabase)
    app_state_db.update_yt_dlp_timestamp_if_stale = AsyncMock(return_value=True)
    return app_state_db


@pytest.fixture
def paths(tmp_path_factory: pytest.TempPathFactory) -> PathManager:
    """Provide a PathManager rooted in a temp directory."""
    app_data_dir = tmp_path_factory.mktemp("app_data")
    return PathManager(app_data_dir, "http://localhost")


@pytest.fixture
def ytdlp_wrapper_with_provider(
    paths: PathManager, app_state_db_mock: MagicMock, mock_youtube_handler: MagicMock
) -> YtdlpWrapper:
    """YtdlpWrapper configured with a provider URL and mocked handler."""
    provider_url = "http://bgutil-provider:4416"
    wrapper = YtdlpWrapper(
        paths,
        provider_url,
        app_state_db=app_state_db_mock,
        yt_channel="stable",
        yt_update_freq=timedelta(hours=12),
    )
    wrapper._source_handler = mock_youtube_handler
    return wrapper


@pytest.fixture
def ytdlp_wrapper(
    paths: PathManager,
    app_state_db_mock: MagicMock,
    mock_youtube_handler: MagicMock,
) -> YtdlpWrapper:
    """YtdlpWrapper with a mocked YoutubeHandler and shared paths/app state."""
    wrapper = YtdlpWrapper(
        paths,
        None,
        app_state_db=app_state_db_mock,
        yt_channel="stable",
        yt_update_freq=timedelta(hours=12),
    )
    wrapper._source_handler = mock_youtube_handler
    return wrapper


# --- Tests for POT provider extractor args injection ---


@pytest.mark.unit
@patch.object(YtdlpCore, "extract_playlist_info")
@pytest.mark.asyncio
async def test_extractor_args_default_none_sets_fetch_pot_never(
    mock_extract_playlist_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Verify default behavior injects fetch_pot=never when URL is not set."""
    ytdlp_wrapper._source_handler.extract_feed_metadata = MagicMock(
        return_value=MagicMock()
    )

    mock_extract_playlist_info.return_value = YtdlpInfo({"id": "x", "title": "t"})

    await ytdlp_wrapper.fetch_playlist_metadata(
        feed_id="f",
        source_type=SourceType.CHANNEL,
        source_url="https://example.com",
        resolved_url="https://example.com",
        user_yt_cli_args=[],
    )

    args: YtdlpArgs = mock_extract_playlist_info.call_args[0][0]
    cmd = args.to_list()
    # Find all extractor-args values
    extractor_args_values = [
        v for i, v in enumerate(cmd) if cmd[i - 1] == "--extractor-args"
    ]
    assert len(extractor_args_values) == 1, "Expected exactly one extractor-args flag"
    assert extractor_args_values[0] == "youtube:fetch_pot=never", (
        "Expected fetch_pot=never when pot_provider_url is None"
    )


@pytest.mark.unit
@patch.object(YtdlpCore, "extract_playlist_info")
@pytest.mark.asyncio
async def test_extractor_args_with_provider_url_sets_http_base_url(
    mock_extract_playlist_info: AsyncMock,
    ytdlp_wrapper_with_provider: YtdlpWrapper,
):
    """Verify provider URL injects youtubepot-bgutilhttp base_url extractor arg."""
    provider_url = "http://bgutil-provider:4416"
    ytdlp_wrapper_with_provider._source_handler.extract_feed_metadata = MagicMock(
        return_value=MagicMock()
    )

    mock_extract_playlist_info.return_value = YtdlpInfo({"id": "x", "title": "t"})

    await ytdlp_wrapper_with_provider.fetch_playlist_metadata(
        feed_id="f",
        source_type=SourceType.CHANNEL,
        source_url="https://example.com",
        resolved_url="https://example.com",
        user_yt_cli_args=[],
    )

    args: YtdlpArgs = mock_extract_playlist_info.call_args[0][0]
    cmd = args.to_list()
    # Find all extractor-args values
    extractor_args_values = [
        v for i, v in enumerate(cmd) if cmd[i - 1] == "--extractor-args"
    ]
    assert len(extractor_args_values) == 1, "Expected exactly one extractor-args flag"
    assert (
        extractor_args_values[0] == f"youtubepot-bgutilhttp:base_url={provider_url}"
    ), "Expected youtubepot-bgutilhttp base_url when pot_provider_url is set"


# --- Tests for YtdlpWrapper._match_filter_since_date ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "since_date,expected_expr",
    [
        (datetime(2023, 1, 1, 12, 30, 45, tzinfo=UTC), "upload_date >= 20230101"),
        (datetime(2024, 12, 31, 23, 59, 59, tzinfo=UTC), "upload_date >= 20241231"),
        (datetime(2022, 6, 15, 0, 0, 0, tzinfo=UTC), "upload_date >= 20220615"),
    ],
)
def test_match_filter_since_date(
    ytdlp_wrapper: YtdlpWrapper,
    since_date: datetime,
    expected_expr: str,
):
    """Test date filter expression generation with various dates."""
    result = ytdlp_wrapper._match_filter_since_date(since_date)
    assert result == expected_expr


# --- Tests for YtdlpWrapper.fetch_playlist_metadata and fetch_new_downloads_metadata ---


@pytest.mark.unit
@patch.object(YtdlpCore, "extract_playlist_info")
@pytest.mark.asyncio
async def test_fetch_playlist_metadata_returns_feed(
    mock_extract_playlist_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
):
    """Tests that fetch_playlist_metadata returns a Feed with proper delegation to handler methods."""
    feed_id = "test_playlist_feed"
    url = "https://www.youtube.com/@test/videos"
    yt_cli_args = ["--format", "best"]

    # Mock the playlist info call to return valid data
    mock_playlist_ytdlp_info = YtdlpInfo(
        {"id": "test_channel", "title": "Test Channel"}
    )
    mock_extract_playlist_info.return_value = mock_playlist_ytdlp_info

    # Create expected Feed object that the handler will return
    expected_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.CHANNEL,
        source_url=url,
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Test Channel Title",
        author="Test Author",
    )

    # Mock handler method to return our expected feed
    mock_youtube_handler.extract_feed_metadata.return_value = expected_feed

    # Call the method under test
    result = await ytdlp_wrapper.fetch_playlist_metadata(
        feed_id=feed_id,
        source_type=SourceType.CHANNEL,
        source_url=url,
        resolved_url=url,
        user_yt_cli_args=yt_cli_args,
    )

    # Verify return type
    assert isinstance(result, Feed), (
        "fetch_playlist_metadata should return a Feed object"
    )
    assert result == expected_feed

    # Verify that the handler method was called with correct parameters
    mock_youtube_handler.extract_feed_metadata.assert_called_once_with(
        feed_id, mock_playlist_ytdlp_info, SourceType.CHANNEL, url
    )


@pytest.mark.unit
@patch.object(YtdlpCore, "extract_downloads_info")
@pytest.mark.asyncio
async def test_fetch_new_downloads_metadata_returns_downloads(
    mock_extract_downloads_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
):
    """Tests that fetch_new_downloads_metadata returns a list of Downloads with proper delegation to handler methods."""
    feed_id = "test_downloads_feed"
    url = "https://www.youtube.com/@test/videos"
    yt_cli_args = ["--format", "best"]

    # Mock the downloads info call to return valid data
    mock_video_info = YtdlpInfo({"id": "test123", "title": "Test Video"})
    mock_extract_downloads_info.return_value = [mock_video_info]

    # Create expected Download object that the handler will return
    expected_download = Download(
        feed_id=feed_id,
        id="test123",
        source_url="https://www.youtube.com/watch?v=test123",
        title="Test Video",
        published=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=120,
        status=DownloadStatus.QUEUED,
    )

    # Mock handler method to return our expected download
    mock_youtube_handler.extract_download_metadata.return_value = expected_download

    # Call the method under test
    result = await ytdlp_wrapper.fetch_new_downloads_metadata(
        feed_id=feed_id,
        source_type=SourceType.CHANNEL,
        source_url=url,
        resolved_url=url,
        user_yt_cli_args=yt_cli_args,
    )

    # Verify return type and values
    assert isinstance(result, list), "fetch_new_downloads_metadata should return a list"
    assert len(result) == 1
    assert result[0] == expected_download

    # Verify that the handler method was called with correct parameters
    mock_youtube_handler.extract_download_metadata.assert_called_once_with(
        feed_id, mock_video_info
    )


# --- Tests for YtdlpWrapper.download_feed_thumbnail ---


@pytest.mark.unit
@patch.object(YtdlpCore, "download")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "source_type, expected_output_method, expected_paths_method",
    [
        (SourceType.SINGLE_VIDEO, "output_thumbnail", "paths_thumbnail"),
        (SourceType.PLAYLIST, "output_pl_thumbnail", "paths_pl_thumbnail"),
        (SourceType.CHANNEL, "output_pl_thumbnail", "paths_pl_thumbnail"),
    ],
)
async def test_download_feed_thumbnail_args(
    mock_ytdlcore_download: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    source_type: SourceType,
    expected_output_method: str,
    expected_paths_method: str,
):
    """Tests that download_feed_thumbnail constructs the correct YtdlpArgs."""
    feed_id = "test_feed_thumb"
    url = "http://example.com/video"
    yt_cli_args: list[str] = ["--format", "best"]

    await ytdlp_wrapper.download_feed_thumbnail(
        feed_id=feed_id,
        source_type=source_type,
        source_url=url,
        resolved_url=url,
        user_yt_cli_args=yt_cli_args,
    )

    mock_ytdlcore_download.assert_called_once()
    call_args = mock_ytdlcore_download.call_args[0]
    ytdlp_args: YtdlpArgs = call_args[0]
    cmd_list = ytdlp_args.to_list()

    # Verify common args
    assert "--skip-download" in cmd_list
    assert "--write-thumbnail" in cmd_list
    assert "--convert-thumbnails" in cmd_list
    assert "jpg" in cmd_list
    assert "--format" in cmd_list
    assert "best" in cmd_list

    # Verify source-type specific args
    try:
        output_index = cmd_list.index("--output")
        output_value = cmd_list[output_index + 1]
    except (ValueError, IndexError):
        pytest.fail("--output flag not found or has no value")

    if source_type == SourceType.SINGLE_VIDEO:
        expected_template = f"thumbnail:{feed_id}.%(ext)s"
        assert output_value == expected_template
    else:
        expected_template = f"pl_thumbnail:{feed_id}.%(ext)s"
        assert output_value == expected_template


@pytest.mark.unit
@patch.object(YtdlpCore, "download")
@patch("aiofiles.os.path.isfile")
@pytest.mark.asyncio
async def test_download_feed_thumbnail_success_return_value(
    mock_is_file: AsyncMock,
    mock_ytdlcore_download: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests that download_feed_thumbnail returns 'jpg' on successful download."""
    mock_is_file.return_value = True

    result = await ytdlp_wrapper.download_feed_thumbnail(
        feed_id="test_feed",
        source_type=SourceType.SINGLE_VIDEO,
        source_url="http://example.com/video",
        resolved_url=None,
        user_yt_cli_args=[],
    )

    assert result == "jpg"
    mock_ytdlcore_download.assert_called_once()
    mock_is_file.assert_called_once()


@pytest.mark.unit
@patch.object(YtdlpCore, "download")
@patch("aiofiles.os.path.isfile")
@pytest.mark.asyncio
async def test_download_feed_thumbnail_file_not_found(
    mock_is_file: AsyncMock,
    mock_ytdlcore_download: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests that download_feed_thumbnail returns None when file doesn't exist."""
    mock_is_file.return_value = False

    result = await ytdlp_wrapper.download_feed_thumbnail(
        feed_id="test_feed",
        source_type=SourceType.SINGLE_VIDEO,
        source_url="http://example.com/video",
        resolved_url=None,
        user_yt_cli_args=[],
    )

    assert result is None
    mock_ytdlcore_download.assert_called_once()
    mock_is_file.assert_called_once()


# --- Tests for YtdlpWrapper.download_media_to_file ---


@pytest.mark.unit
@patch.object(YtdlpCore, "download")
@patch("aiofiles.os.path.isfile", return_value=True)
@patch("aiofiles.os.stat")
@patch.object(YtdlpWrapper, "_prepare_download_dir")
@patch("aiofiles.os.wrap")
@pytest.mark.asyncio
async def test_download_media_to_file_success_simplified(
    mock_aiofiles_wrap: MagicMock,
    mock_prep_dl_dir: AsyncMock,
    mock_stat: AsyncMock,
    mock_is_file: AsyncMock,
    mock_ytdlcore_download: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests the happy path of download_media_to_file."""
    feed_id = "test_feed_happy"
    download_id = "test_id_happy"

    dummy_download = Download(
        feed_id=feed_id,
        id=download_id,
        source_url="http://example.com/video_happy",
        title="Test Happy Video",
        published=datetime(2023, 2, 1, 0, 0, 0, tzinfo=UTC),
        ext="mkv",
        mime_type="video/x-matroska",
        filesize=12345,
        duration=60,
        status=DownloadStatus.QUEUED,
    )
    yt_cli_args: list[str] = ["--format", "bestvideo+bestaudio/best"]

    feed_temp_path = ytdlp_wrapper._paths.base_tmp_dir / feed_id
    feed_home_path = ytdlp_wrapper._paths.base_data_dir / feed_id

    expected_final_file = feed_home_path / f"{download_id}.{dummy_download.ext}"

    mock_ytdlcore_download.return_value = None

    expected_final_file.parent.mkdir(parents=True, exist_ok=True)
    expected_final_file.touch()

    mock_stat_instance = mock_stat.return_value
    mock_stat_instance.st_size = 12345

    mock_prep_dl_dir.return_value = (feed_temp_path, feed_home_path)
    mock_glob = AsyncMock(return_value=[expected_final_file])
    mock_aiofiles_wrap.return_value = mock_glob

    returned_path = await ytdlp_wrapper.download_media_to_file(
        dummy_download, yt_cli_args
    )

    assert returned_path == expected_final_file

    mock_prep_dl_dir.assert_called_once_with(feed_id)

    # Check the download call
    mock_ytdlcore_download.assert_called_once()
    download_args, _ = mock_ytdlcore_download.call_args
    assert isinstance(download_args[0], YtdlpArgs)
    assert download_args[1] == dummy_download.source_url

    mock_aiofiles_wrap.assert_called_once()
    mock_glob.assert_called_once_with(f"{download_id}.*")

    mock_is_file.assert_called_once_with(expected_final_file)
    mock_stat.assert_called_once_with(expected_final_file)

    assert mock_is_file.call_count >= 1
    assert mock_stat.call_count >= 1


# --- Tests for date filtering behavior ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_type,url,should_call_set_date_range",
    [
        (SourceType.SINGLE_VIDEO, "https://www.youtube.com/watch?v=test", False),
        (SourceType.PLAYLIST, "https://www.youtube.com/playlist?list=test", True),
        (SourceType.CHANNEL, "https://www.youtube.com/@test/videos", True),
    ],
)
@patch.object(YtdlpCore, "extract_downloads_info")
@pytest.mark.asyncio
async def test_date_filtering_behavior_by_reference_type(
    mock_extract_downloads_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
    source_type: SourceType,
    url: str,
    should_call_set_date_range: bool,
):
    """Test that date filtering is applied correctly based on source type.

    Single videos should skip date filtering to avoid partial metadata,
    while collections and channels should apply date filtering.
    """
    feed_id = "test_feed"

    # Mock the extract_downloads_info call to avoid actual yt-dlp calls
    mock_extract_downloads_info.return_value = []
    mock_youtube_handler.extract_download_metadata.return_value = MagicMock()

    # Call fetch_new_downloads_metadata with date filtering parameters
    fetch_since_date = datetime(2023, 1, 1, tzinfo=UTC)

    await ytdlp_wrapper.fetch_new_downloads_metadata(
        feed_id=feed_id,
        source_type=source_type,
        source_url=url,
        resolved_url=url,
        user_yt_cli_args=[],
        fetch_since_date=fetch_since_date,
    )

    # Verify date filtering is applied in CLI args based on source type
    if should_call_set_date_range:
        # Check that extract_downloads_info was called with CLI args containing optimization flags
        call_args = mock_extract_downloads_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--lazy-playlist" in cli_args
        assert "--break-match-filters" in cli_args
        assert "upload_date >= 20230101" in cli_args
    else:
        # For single videos, optimization should not be applied
        call_args = mock_extract_downloads_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--lazy-playlist" not in cli_args
        assert "--break-match-filters" not in cli_args


# --- Tests for keep_last filtering behavior ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "source_type,url,should_call_set_playlist_limit",
    [
        (SourceType.SINGLE_VIDEO, "https://www.youtube.com/watch?v=test", False),
        (SourceType.PLAYLIST, "https://www.youtube.com/playlist?list=test", True),
        (SourceType.CHANNEL, "https://www.youtube.com/@test/videos", True),
    ],
)
@patch.object(YtdlpCore, "extract_downloads_info")
@pytest.mark.asyncio
async def test_keep_last_filtering_behavior_by_reference_type(
    mock_extract_downloads_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
    source_type: SourceType,
    url: str,
    should_call_set_playlist_limit: bool,
):
    """Test that keep_last filtering is applied correctly based on source type.

    Single videos should skip playlist limiting since they're not playlists,
    while collections and channels should apply playlist limiting.
    """
    feed_id = "test_feed"
    keep_last = 5

    # Mock the extract_downloads_info call to avoid actual yt-dlp calls
    mock_extract_downloads_info.return_value = []
    mock_youtube_handler.extract_download_metadata.return_value = MagicMock()

    # Call fetch_new_downloads_metadata with keep_last parameter
    await ytdlp_wrapper.fetch_new_downloads_metadata(
        feed_id=feed_id,
        source_type=source_type,
        source_url=url,
        resolved_url=url,
        user_yt_cli_args=[],
        keep_last=keep_last,
    )

    # Verify playlist limit is applied in CLI args based on source type
    if should_call_set_playlist_limit:
        # Check that extract_downloads_info was called with CLI args containing playlist limit
        call_args = mock_extract_downloads_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--playlist-items" in cli_args
        assert f":{keep_last}" in cli_args
    else:
        # For single videos, playlist limiting should not be applied
        call_args = mock_extract_downloads_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--playlist-items" not in cli_args


# NOTE: More complex fetch_metadata and download_media_to_file scenarios are covered by integration tests
