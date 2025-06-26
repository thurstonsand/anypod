# pyright: reportPrivateUsage=false

"""Tests for the YtdlpWrapper class and its yt-dlp integration functionality."""

from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper import YtdlpWrapper
from anypod.ytdlp_wrapper.base_handler import FetchPurpose, ReferenceType
from anypod.ytdlp_wrapper.core import YtdlpArgs, YtdlpCore, YtdlpInfo
from anypod.ytdlp_wrapper.youtube_handler import YoutubeHandler


@pytest.fixture
def mock_youtube_handler() -> MagicMock:
    """Fixture to provide a mocked YoutubeHandler."""
    handler = MagicMock(spec=YoutubeHandler)
    return handler


@pytest.fixture
def ytdlp_wrapper(
    mock_youtube_handler: MagicMock, tmp_path_factory: pytest.TempPathFactory
) -> YtdlpWrapper:
    """Fixture to provide a YtdlpWrapper instance with a mocked YoutubeHandler and temp paths."""
    app_data_dir = tmp_path_factory.mktemp("app_data")
    paths = PathManager(app_data_dir, "http://localhost")
    wrapper = YtdlpWrapper(paths)
    wrapper._source_handler = mock_youtube_handler
    return wrapper


# --- Tests for YtdlpWrapper._prepare_ydl_options ---


@pytest.mark.unit
def test_prepare_ydl_options_discovery_basic(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests basic option preparation for DISCOVERY purpose with no user CLI args and no source-specific options."""
    args = YtdlpArgs()
    purpose = FetchPurpose.DISCOVERY

    prepared_args = ytdlp_wrapper._prepare_ytdlp_options(args, purpose)

    # Convert to list to check options
    prepared_opts = prepared_args.to_list()

    assert "--skip-download" in prepared_opts
    assert "--quiet" in prepared_opts
    assert "--no-warnings" in prepared_opts
    assert "--flat-playlist" in prepared_opts
    assert "--playlist-items" in prepared_opts
    assert ":5" in prepared_opts


@pytest.mark.unit
def test_prepare_ydl_options_metadata_fetch_basic(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests basic option preparation for METADATA_FETCH purpose with no user CLI args and no source-specific options."""
    args = YtdlpArgs()
    purpose = FetchPurpose.METADATA_FETCH

    prepared_args = ytdlp_wrapper._prepare_ytdlp_options(args, purpose)

    # Convert to list to check options
    prepared_opts = prepared_args.to_list()

    assert "--skip-download" in prepared_opts
    assert "--quiet" in prepared_opts
    assert "--no-warnings" in prepared_opts
    # Key difference for METADATA_FETCH - no flat-playlist
    assert "--flat-playlist" not in prepared_opts


@pytest.mark.unit
def test_prepare_ydl_options_media_download(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests option preparation for MEDIA_DOWNLOAD purpose."""
    args = YtdlpArgs()
    purpose = FetchPurpose.MEDIA_DOWNLOAD
    mock_temp_path = Path("/tmp/downloads/feed_id/temp")
    mock_data_path = Path("/tmp/downloads/feed_id/data")
    mock_download_id = "video_id"

    prepared_args = ytdlp_wrapper._prepare_ytdlp_options(
        args,
        purpose,
        download_temp_dir=mock_temp_path,
        download_data_dir=mock_data_path,
        download_id=mock_download_id,
    )

    # Convert to list to check options
    prepared_opts = prepared_args.to_list()

    # Should not contain --skip-download (that's the default, omitted means download)
    assert "--skip-download" not in prepared_opts
    assert "--output" in prepared_opts
    assert f"{mock_download_id}.%(ext)s" in prepared_opts
    assert "--paths" in prepared_opts
    assert f"temp:{mock_temp_path}" in prepared_opts
    assert f"home:{mock_data_path}" in prepared_opts


@pytest.mark.unit
def test_prepare_ydl_options_with_user_cli_args_and_source_opts(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests option preparation with user CLI args and source-specific options, ensuring they are merged correctly."""
    args = YtdlpArgs(["--format", "bestvideo"])
    purpose = FetchPurpose.METADATA_FETCH

    # Add some source-specific options to args using the builder pattern
    args.cookies(Path("cookies.txt")).extend_args(["--ignore-errors"])

    prepared_args = ytdlp_wrapper._prepare_ytdlp_options(args, purpose)

    # Convert to list to check options
    prepared_opts = prepared_args.to_list()

    assert "--skip-download" in prepared_opts
    assert "--quiet" in prepared_opts
    assert "--no-warnings" in prepared_opts

    assert "--format" in prepared_opts
    assert "bestvideo" in prepared_opts
    assert "--cookies" in prepared_opts
    assert "cookies.txt" in prepared_opts
    assert "--ignore-errors" in prepared_opts


# --- Tests for YtdlpWrapper.fetch_metadata ---


@pytest.mark.unit
@patch.object(YtdlpCore, "extract_info")
@pytest.mark.asyncio
async def test_fetch_metadata_returns_feed_and_downloads_tuple(
    mock_extract_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
):
    """Tests that fetch_metadata returns a tuple of (Feed, list[Download]) with proper delegation to handler methods."""
    feed_id = "test_tuple_return"
    url = "https://www.youtube.com/watch?v=test123"
    yt_cli_args = ["--format", "best"]

    # Mock the main fetch call to return valid data (discovery returns None for direct fetch)
    mock_main_ytdlp_info = YtdlpInfo({"id": "test123", "title": "Test Video"})
    mock_extract_info.return_value = mock_main_ytdlp_info

    # Create expected Feed and Download objects that the handler will return
    expected_feed = Feed(
        id=feed_id,
        is_enabled=True,
        source_type=SourceType.SINGLE_VIDEO,
        source_url=url,
        last_successful_sync=datetime.min.replace(tzinfo=UTC),
        title="Test Video Title",
        author="Test Author",
    )
    expected_download = Download(
        feed_id=feed_id,
        id="test123",
        source_url=url,
        title="Test Video",
        published=datetime(2023, 1, 1, 0, 0, 0, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=120,
        status=DownloadStatus.QUEUED,
    )

    # Mock handler methods to return our expected objects
    mock_youtube_handler.set_source_specific_ytdlp_options.side_effect = (
        lambda args, purpose: args  # type: ignore
    )
    mock_youtube_handler.determine_fetch_strategy = AsyncMock(
        return_value=(
            url,
            ReferenceType.SINGLE,
        )
    )
    mock_youtube_handler.extract_feed_metadata.return_value = expected_feed
    mock_youtube_handler.parse_metadata_to_downloads.return_value = [expected_download]

    # Call the method under test
    result = await ytdlp_wrapper.fetch_metadata(feed_id, url, yt_cli_args)

    # Verify return type and structure
    assert isinstance(result, tuple), "fetch_metadata should return a tuple"
    assert len(result) == 2, "fetch_metadata should return a 2-tuple"

    feed, downloads = result
    assert isinstance(feed, Feed), "First element should be a Feed object"
    assert isinstance(downloads, list), "Second element should be a list"

    # Verify the actual values match what the handler returned
    assert feed == expected_feed
    assert downloads == [expected_download]

    # Verify that the handler methods were called with correct parameters
    mock_youtube_handler.extract_feed_metadata.assert_called_once_with(
        feed_id, mock_main_ytdlp_info, ReferenceType.SINGLE, url, None
    )
    mock_youtube_handler.parse_metadata_to_downloads.assert_called_once_with(
        feed_id,
        mock_main_ytdlp_info,
        source_identifier=feed_id,
        ref_type=ReferenceType.SINGLE,
    )


# --- Tests for YtdlpWrapper.download_media_to_file ---


@pytest.mark.unit
@patch.object(YtdlpWrapper, "_prepare_ytdlp_options")
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
    mock_prepare_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
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

    mock_ydl_opts_for_core_download = [
        "--output",
        f"{download_id}.%(ext)s",
        "--paths",
        f"temp:{feed_temp_path}",
        "--paths",
        f"home:{feed_home_path}",
        "--format",
        "bestvideo+bestaudio/best",
    ]
    mock_prepare_options.return_value = YtdlpArgs(mock_ydl_opts_for_core_download)
    mock_ytdlcore_download.return_value = None
    mock_youtube_handler.set_source_specific_ytdlp_options.side_effect = (
        lambda args, purpose: args.extend_args(["--source-opt", "youtube_specific"])  # type: ignore
    )

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

    mock_youtube_handler.set_source_specific_ytdlp_options.assert_called_once()
    # Check that mock_prepare_options was called correctly
    mock_prepare_options.assert_called_once()
    _, kwargs = mock_prepare_options.call_args
    assert kwargs["purpose"] == FetchPurpose.MEDIA_DOWNLOAD
    assert kwargs["download_temp_dir"] == feed_temp_path
    assert kwargs["download_data_dir"] == feed_home_path
    assert kwargs["download_id"] == download_id
    assert kwargs["cookies_path"] is None
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
    "reference_type,url,should_call_set_date_range",
    [
        (ReferenceType.SINGLE, "https://www.youtube.com/watch?v=test", False),
        (ReferenceType.COLLECTION, "https://www.youtube.com/playlist?list=test", True),
        (ReferenceType.CHANNEL, "https://www.youtube.com/@test/videos", True),
    ],
)
@patch.object(YtdlpCore, "extract_info")
@pytest.mark.asyncio
async def test_date_filtering_behavior_by_reference_type(
    mock_extract_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
    reference_type: ReferenceType,
    url: str,
    should_call_set_date_range: bool,
):
    """Test that date filtering is applied correctly based on reference type.

    Single videos should skip date filtering to avoid partial metadata,
    while collections and channels should apply date filtering.
    """
    feed_id = "test_feed"

    # Mock the source handler to return the specified reference type
    mock_youtube_handler.determine_fetch_strategy = AsyncMock(
        return_value=(
            url,
            reference_type,
        )
    )
    # Mock the set_source_specific_ytdlp_options method to return the passed args unchanged
    mock_youtube_handler.set_source_specific_ytdlp_options.side_effect = (
        lambda args, purpose: args  # type: ignore
    )

    # Mock the extract_info call to avoid actual yt-dlp calls
    mock_ytdlp_info = MagicMock()
    mock_extract_info.return_value = mock_ytdlp_info
    mock_youtube_handler.extract_feed_metadata.return_value = MagicMock()
    mock_youtube_handler.parse_metadata_to_downloads.return_value = []

    # Call fetch_metadata with date filtering parameters
    fetch_since_date = datetime(2023, 1, 1, tzinfo=UTC)
    fetch_until_date = datetime.now(UTC)

    await ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=url,
        user_yt_cli_args=[],
        fetch_since_date=fetch_since_date,
        fetch_until_date=fetch_until_date,
    )

    # Verify date filtering is applied in CLI args based on reference type
    if should_call_set_date_range:
        # Check that extract_info was called with CLI args containing date filters
        call_args = mock_extract_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--dateafter" in cli_args
        assert "20230101" in cli_args
        assert "--datebefore" in cli_args
    else:
        # For single videos, date filtering should not be applied
        call_args = mock_extract_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--dateafter" not in cli_args
        assert "--datebefore" not in cli_args


# --- Tests for keep_last filtering behavior ---


@pytest.mark.unit
@pytest.mark.parametrize(
    "reference_type,url,should_call_set_playlist_limit",
    [
        (ReferenceType.SINGLE, "https://www.youtube.com/watch?v=test", False),
        (ReferenceType.COLLECTION, "https://www.youtube.com/playlist?list=test", True),
        (ReferenceType.CHANNEL, "https://www.youtube.com/@test/videos", True),
    ],
)
@patch.object(YtdlpCore, "extract_info")
@pytest.mark.asyncio
async def test_keep_last_filtering_behavior_by_reference_type(
    mock_extract_info: AsyncMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
    reference_type: ReferenceType,
    url: str,
    should_call_set_playlist_limit: bool,
):
    """Test that keep_last filtering is applied correctly based on reference type.

    Single videos should skip playlist limiting since they're not playlists,
    while collections and channels should apply playlist limiting.
    """
    feed_id = "test_feed"
    keep_last = 5

    # Mock the source handler to return the specified reference type
    mock_youtube_handler.determine_fetch_strategy = AsyncMock(
        return_value=(
            url,
            reference_type,
        )
    )
    # Mock the set_source_specific_ytdlp_options method to return the passed args unchanged
    mock_youtube_handler.set_source_specific_ytdlp_options.side_effect = (
        lambda args, purpose: args  # type: ignore
    )

    # Mock the extract_info call to avoid actual yt-dlp calls
    mock_ytdlp_info = MagicMock()
    mock_extract_info.return_value = mock_ytdlp_info
    mock_youtube_handler.extract_feed_metadata.return_value = MagicMock()
    mock_youtube_handler.parse_metadata_to_downloads.return_value = []

    # Call fetch_metadata with keep_last parameter
    await ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=url,
        user_yt_cli_args=[],
        keep_last=keep_last,
    )

    # Verify playlist limit is applied in CLI args based on reference type
    if should_call_set_playlist_limit:
        # Check that extract_info was called with CLI args containing playlist limit
        call_args = mock_extract_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--playlist-items" in cli_args
        assert f":{keep_last}" in cli_args
    else:
        # For single videos, playlist limiting should not be applied
        call_args = mock_extract_info.call_args[0]
        ytdlp_args = call_args[0]
        cli_args = ytdlp_args.to_list()
        assert "--playlist-items" not in cli_args


# NOTE: More complex fetch_metadata and download_media_to_file scenarios are covered by integration tests
