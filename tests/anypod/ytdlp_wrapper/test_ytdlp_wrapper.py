from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from anypod.db import Download, DownloadStatus
from anypod.exceptions import YtdlpApiError
from anypod.ytdlp_wrapper import YtdlpWrapper
from anypod.ytdlp_wrapper.base_handler import FetchPurpose
from anypod.ytdlp_wrapper.youtube_handler import YoutubeHandler
from anypod.ytdlp_wrapper.ytdlp_core import YtdlpCore


@pytest.fixture
def mock_youtube_handler() -> MagicMock:
    """Fixture to provide a mocked YoutubeHandler."""
    handler = MagicMock(spec=YoutubeHandler)
    return handler


@pytest.fixture
def ytdlp_wrapper(mock_youtube_handler: MagicMock) -> YtdlpWrapper:
    """Fixture to provide a YtdlpWrapper instance with a mocked YoutubeHandler."""
    wrapper = YtdlpWrapper()
    wrapper._source_handler = mock_youtube_handler  # type: ignore
    return wrapper


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_core.yt_dlp.parse_options")
def test_prepare_ydl_options_discovery_basic(
    mock_parse_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """
    Tests basic option preparation for DISCOVERY purpose with no user CLI args
    and no source-specific options.
    """
    mock_parse_options.return_value = (
        None,
        None,
        None,
        {},
    )

    user_cli_args: list[str] = []
    purpose = FetchPurpose.DISCOVERY
    source_specific_opts: dict[str, Any] = {}

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(  # type: ignore
        user_cli_args, purpose, source_specific_opts, None
    )

    mock_parse_options.assert_called_once_with(user_cli_args)

    assert prepared_opts["skip_download"] is True
    assert prepared_opts["quiet"] is True
    assert prepared_opts["ignoreerrors"] is True
    assert prepared_opts["no_warnings"] is True
    assert prepared_opts["verbose"] is False
    assert prepared_opts["extract_flat"] == "in_playlist"
    assert prepared_opts["playlist_items"] == "1-5"
    assert "logger" in prepared_opts
    assert "match_filter" not in prepared_opts


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_core.yt_dlp.parse_options")
def test_prepare_ydl_options_metadata_fetch_basic(
    mock_parse_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """
    Tests basic option preparation for METADATA_FETCH purpose with no user CLI args
    and no source-specific options.
    """
    mock_parse_options.return_value = (
        None,
        None,
        None,
        {},
    )

    user_cli_args: list[str] = []
    purpose = FetchPurpose.METADATA_FETCH
    source_specific_opts: dict[str, Any] = {}

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(  # type: ignore
        user_cli_args, purpose, source_specific_opts, None
    )

    mock_parse_options.assert_called_once_with(user_cli_args)

    assert prepared_opts["skip_download"] is True
    assert prepared_opts["quiet"] is True
    assert prepared_opts["ignoreerrors"] is True
    assert prepared_opts["no_warnings"] is True
    assert prepared_opts["verbose"] is False
    assert prepared_opts["extract_flat"] is False  # Key difference for METADATA_FETCH
    assert "playlist_items" not in prepared_opts
    assert "logger" in prepared_opts
    assert "match_filter" not in prepared_opts


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_core.yt_dlp.parse_options")
def test_prepare_ydl_options_media_download(
    mock_parse_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """
    Tests option preparation for MEDIA_DOWNLOAD purpose.
    """
    mock_parse_options.return_value = (None, None, None, {})
    user_cli_args: list[str] = []
    purpose = FetchPurpose.MEDIA_DOWNLOAD
    source_specific_opts: dict[str, Any] = {}
    mock_target_path = Path("/tmp/downloads/feed_id/video_id.mp4")

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(  # type: ignore
        user_cli_args,
        purpose,
        source_specific_opts,
        mock_target_path,
    )

    assert prepared_opts["skip_download"] is False
    assert prepared_opts["outtmpl"] == str(mock_target_path)
    assert prepared_opts["extract_flat"] is False
    assert "logger" in prepared_opts


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_core.yt_dlp.parse_options")
def test_prepare_ydl_options_with_user_cli_args_and_source_opts(
    mock_parse_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """
    Tests option preparation with user CLI args and source-specific options,
    ensuring they are merged correctly.
    """
    parsed_user_cli_opts = {
        "format": "bestvideo",
    }
    mock_parse_options.return_value = (None, None, None, parsed_user_cli_opts)

    user_cli_args = [
        "-f",
        "bestvideo",
    ]
    purpose = FetchPurpose.METADATA_FETCH
    source_specific_opts = {
        "cookies": "cookies.txt",
        "ignoreerrors": False,
    }

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(  # type: ignore
        user_cli_args, purpose, source_specific_opts, None
    )

    mock_parse_options.assert_called_once_with(user_cli_args)

    assert prepared_opts["skip_download"] is True
    assert prepared_opts["quiet"] is True
    assert prepared_opts["no_warnings"] is True

    assert prepared_opts["format"] == "bestvideo"
    assert prepared_opts["cookies"] == "cookies.txt"
    assert prepared_opts["ignoreerrors"] is False

    assert prepared_opts["extract_flat"] is False
    assert "logger" in prepared_opts
    assert "match_filter" not in prepared_opts


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_core.yt_dlp.parse_options")
def test_prepare_ydl_options_parse_options_failure(
    mock_parse_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """
    Tests that a YtdlpApiError is raised if yt_dlp.parse_options fails.
    """
    mock_parse_options.side_effect = Exception("CLI parsing failed")

    user_cli_args = ["--invalid-arg"]
    purpose = FetchPurpose.DISCOVERY
    source_specific_opts: dict[str, Any] = {}

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper._prepare_ydl_options(  # type: ignore
            user_cli_args, purpose, source_specific_opts, None
        )

    mock_parse_options.assert_called_once_with(user_cli_args)


@pytest.mark.unit
@patch.object(YtdlpWrapper, "_prepare_ydl_options")
@patch.object(YtdlpCore, "download")
@patch("pathlib.Path.exists", return_value=True)
def test_download_media_to_file_success(
    mock_exists: MagicMock,
    mock_download: MagicMock,
    mock_prepare_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
):
    """Tests the success path of download_media_to_file."""
    dummy_download = Download(
        feed="test_feed",
        id="test_id",
        source_url="http://example.com/video_to_dl",
        title="Test Download Video",
        published=datetime.fromisoformat("2023-01-01T00:00:00Z".replace("Z", "+00:00")),
        ext="mp4",
        duration=120.0,
        status=DownloadStatus.QUEUED,
    )
    yt_cli_args = ["-f", "best"]
    download_target_dir = Path("/tmp/test_downloads")
    expected_target_path = (
        download_target_dir
        / dummy_download.feed
        / f"{dummy_download.id}.{dummy_download.ext}"
    )

    mock_download_opts = {"outtmpl": str(expected_target_path), "skip_download": False}
    mock_prepare_options.return_value = mock_download_opts

    mock_download.return_value = None
    mock_youtube_handler.get_source_specific_ydl_options.return_value = {
        "handler_opt": "val"
    }

    result_path = ytdlp_wrapper.download_media_to_file(
        dummy_download, yt_cli_args, download_target_dir
    )

    mock_prepare_options.assert_called_once_with(
        user_cli_args=yt_cli_args,
        purpose=FetchPurpose.MEDIA_DOWNLOAD,
        source_specific_opts={"handler_opt": "val"},
        download_target_path=expected_target_path,
    )

    mock_download.assert_called_once_with(mock_download_opts, dummy_download.source_url)

    assert result_path == expected_target_path

    mock_youtube_handler.get_source_specific_ydl_options.assert_called_once_with(
        FetchPurpose.MEDIA_DOWNLOAD
    )
    mock_exists.assert_called()


# NOTE: fetch_metadata is not tested here because it is too complex to mock
# it is covered by integration tests

# NOTE: _compose_match_filters_and may not be needed, so skipping tests for it.
