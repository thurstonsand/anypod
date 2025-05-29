# pyright: reportPrivateUsage=false

"""Tests for the YtdlpWrapper class and its yt-dlp integration functionality."""

from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from anypod.db import Download, DownloadStatus
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
def ytdlp_wrapper(
    mock_youtube_handler: MagicMock, tmp_path_factory: pytest.TempPathFactory
) -> YtdlpWrapper:
    """Fixture to provide a YtdlpWrapper instance with a mocked YoutubeHandler and temp paths."""
    app_tmp_dir = tmp_path_factory.mktemp("app_tmp")
    app_data_dir = tmp_path_factory.mktemp("app_data")
    wrapper = YtdlpWrapper(app_tmp_dir, app_data_dir)
    wrapper._source_handler = mock_youtube_handler
    return wrapper


@pytest.mark.unit
def test_prepare_ydl_options_discovery_basic(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests basic option preparation for DISCOVERY purpose with no user CLI args and no source-specific options."""
    user_cli_args: dict[str, Any] = {}
    purpose = FetchPurpose.DISCOVERY
    source_specific_opts: dict[str, Any] = {}

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(
        user_cli_args, purpose, source_specific_opts, None
    )

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
def test_prepare_ydl_options_metadata_fetch_basic(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests basic option preparation for METADATA_FETCH purpose with no user CLI args and no source-specific options."""
    user_cli_args: dict[str, Any] = {}
    purpose = FetchPurpose.METADATA_FETCH
    source_specific_opts: dict[str, Any] = {}

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(
        user_cli_args, purpose, source_specific_opts, None
    )

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
def test_prepare_ydl_options_media_download(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests option preparation for MEDIA_DOWNLOAD purpose."""
    user_cli_args: dict[str, Any] = {}
    purpose = FetchPurpose.MEDIA_DOWNLOAD
    source_specific_opts: dict[str, Any] = {}
    mock_target_path = Path("/tmp/downloads/feed_id/video_id.mp4")
    mock_download_id = "video_id"

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(
        user_cli_args,
        purpose,
        source_specific_opts,
        mock_target_path,
        mock_target_path,
        download_id=mock_download_id,
    )

    assert prepared_opts["skip_download"] is False
    assert prepared_opts["outtmpl"] == f"{mock_download_id}.%(ext)s"
    assert prepared_opts["paths"] == {
        "temp": str(mock_target_path),
        "home": str(mock_target_path),
    }
    assert prepared_opts["extract_flat"] is False
    assert "logger" in prepared_opts


@pytest.mark.unit
def test_prepare_ydl_options_with_user_cli_args_and_source_opts(
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests option preparation with user CLI args and source-specific options, ensuring they are merged correctly."""
    user_cli_args: dict[str, Any] = {"format": "bestvideo"}
    purpose = FetchPurpose.METADATA_FETCH
    source_specific_opts = {
        "cookies": "cookies.txt",
        "ignoreerrors": False,
    }

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(
        user_cli_args, purpose, source_specific_opts, None
    )

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
@patch.object(YtdlpWrapper, "_prepare_ydl_options")
@patch.object(YtdlpCore, "download")
@patch("pathlib.Path.is_file", return_value=True)
@patch("pathlib.Path.stat")
@patch.object(YtdlpWrapper, "_prepare_download_dir")
@patch.object(Path, "glob", return_value=[])
def test_download_media_to_file_success_simplified(
    mock_path_glob: MagicMock,
    mock_prep_dl_dir: MagicMock,
    mock_stat: MagicMock,
    mock_is_file: MagicMock,
    mock_ytdlcore_download: MagicMock,
    mock_prepare_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
    mock_youtube_handler: MagicMock,
):
    """Tests the happy path of download_media_to_file."""
    feed_id = "test_feed_happy"
    download_id = "test_id_happy"

    dummy_download = Download(
        feed=feed_id,
        id=download_id,
        source_url="http://example.com/video_happy",
        title="Test Happy Video",
        published=datetime.fromisoformat("2023-02-01T00:00:00Z".replace("Z", "+00:00")),
        ext="mkv",
        duration=60.0,
        status=DownloadStatus.QUEUED,
    )
    yt_cli_args: dict[str, Any] = {"format": "bestvideo+bestaudio/best"}

    feed_temp_path = ytdlp_wrapper._app_tmp_dir / feed_id
    feed_home_path = ytdlp_wrapper._app_data_dir / feed_id

    expected_final_file = feed_home_path / f"{download_id}.{dummy_download.ext}"

    mock_ydl_opts_for_core_download = {
        "outtmpl": f"{download_id}.%(ext)s",
        "paths": {"temp": str(feed_temp_path), "home": str(feed_home_path)},
        "skip_download": False,
        "format": "bestvideo+bestaudio/best",
    }
    mock_prepare_options.return_value = mock_ydl_opts_for_core_download
    mock_ytdlcore_download.return_value = None
    mock_youtube_handler.get_source_specific_ydl_options.return_value = {
        "source_opt": "youtube_specific"
    }

    expected_final_file.parent.mkdir(parents=True, exist_ok=True)
    expected_final_file.touch()

    mock_stat_instance = mock_stat.return_value
    mock_stat_instance.st_size = 12345

    mock_prep_dl_dir.return_value = (feed_temp_path, feed_home_path)
    mock_path_glob.return_value = [expected_final_file]

    returned_path = ytdlp_wrapper.download_media_to_file(dummy_download, yt_cli_args)

    # --- Assertions ---
    assert returned_path == expected_final_file

    mock_prep_dl_dir.assert_called_once_with(feed_id)

    mock_youtube_handler.get_source_specific_ydl_options.assert_called_once_with(
        FetchPurpose.MEDIA_DOWNLOAD
    )
    mock_prepare_options.assert_called_once_with(
        user_cli_args=yt_cli_args,
        purpose=FetchPurpose.MEDIA_DOWNLOAD,
        source_specific_opts={"source_opt": "youtube_specific"},
        download_temp_dir=feed_temp_path,
        download_data_dir=feed_home_path,
        download_id=download_id,
    )
    mock_ytdlcore_download.assert_called_once_with(
        mock_ydl_opts_for_core_download, dummy_download.source_url
    )

    mock_path_glob.assert_called_once_with(f"{download_id}.*")

    mock_is_file.assert_called_with()
    mock_stat.assert_called_with()

    assert mock_is_file.call_count >= 1
    assert mock_stat.call_count >= 1


# NOTE: fetch_metadata is not tested here because it is too complex to mock
# it is covered by integration tests
