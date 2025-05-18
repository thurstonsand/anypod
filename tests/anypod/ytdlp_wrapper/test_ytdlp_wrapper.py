from datetime import datetime
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import yt_dlp  # type: ignore

from anypod.db import Download, DownloadStatus
from anypod.exceptions import YtdlpApiError
from anypod.ytdlp_wrapper import YtdlpWrapper
from anypod.ytdlp_wrapper.base_handler import FetchPurpose
from anypod.ytdlp_wrapper.youtube_handler import YoutubeHandler


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
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.parse_options")
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
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.parse_options")
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
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.parse_options")
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
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.parse_options")
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
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.parse_options")
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
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_extract_yt_dlp_info_internal_success(
    mock_youtube_dl_constructor: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests successful metadata extraction by _extract_yt_dlp_info_internal."""
    mock_ydl_instance = MagicMock()
    expected_info_dict = {"id": "test_video", "title": "Test Video"}
    mock_ydl_instance.extract_info.return_value = expected_info_dict
    mock_youtube_dl_constructor.return_value = mock_ydl_instance

    ydl_opts = {"some_other_option": "value"}

    url = "http://example.com/video"

    result = ytdlp_wrapper._extract_yt_dlp_info_internal(ydl_opts, url)  # type: ignore

    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts)
    mock_ydl_instance.extract_info.assert_called_once_with(url, download=False)
    assert result == expected_info_dict, f"Expected {expected_info_dict}, got {result}"


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_extract_yt_dlp_info_internal_ydl_init_fails(
    mock_youtube_dl_constructor: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests that YtdlpApiError is raised if YoutubeDL instantiation fails."""
    mock_youtube_dl_constructor.side_effect = Exception("Init failed")

    ydl_opts = {"some_option": "value"}
    url = "http://example.com/video"

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper._extract_yt_dlp_info_internal(ydl_opts, url)  # type: ignore

    assert "Failed to instantiate yt_dlp.YoutubeDL" in str(excinfo.value)
    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts)


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_extract_yt_dlp_info_internal_extract_info_download_error(
    mock_youtube_dl_constructor: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests YtdlpApiError for a yt-dlp DownloadError during extract_info."""
    mock_ydl_instance = MagicMock()
    mock_youtube_dl_constructor.return_value = mock_ydl_instance
    mock_ydl_instance.extract_info.side_effect = yt_dlp.utils.DownloadError(  # type: ignore
        "Simulated DownloadError"
    )

    ydl_opts = {"key": "value"}
    url = "http://example.com/video_dl_error"

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper._extract_yt_dlp_info_internal(ydl_opts, url)  # type: ignore

    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts)
    mock_ydl_instance.extract_info.assert_called_once_with(url, download=False)


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_extract_yt_dlp_info_internal_extract_info_generic_error(
    mock_youtube_dl_constructor: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests YtdlpApiError for a generic Exception during extract_info."""
    mock_ydl_instance = MagicMock()
    mock_ydl_instance.extract_info.side_effect = Exception("Unexpected error")
    mock_youtube_dl_constructor.return_value = mock_ydl_instance

    ydl_opts = {}
    url = "http://example.com/video_generic_error"

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper._extract_yt_dlp_info_internal(ydl_opts, url)  # type: ignore

    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts)
    mock_ydl_instance.extract_info.assert_called_once_with(url, download=False)


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_download_media_internal_success(
    mock_youtube_dl_constructor: MagicMock, ytdlp_wrapper: YtdlpWrapper
):
    """Tests successful media download by _download_media_internal."""
    mock_ydl_instance = MagicMock()
    mock_ydl_instance.download.return_value = 0
    mock_youtube_dl_constructor.return_value = mock_ydl_instance

    ydl_opts_passed_to_method = {"outtmpl": "/path/to/file.mp4", "format": "best"}
    url = "http://example.com/download_video"

    ytdlp_wrapper._download_media_internal(ydl_opts_passed_to_method, url)  # type: ignore

    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts_passed_to_method)
    mock_ydl_instance.download.assert_called_once_with([url])


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_download_media_internal_download_fails(
    mock_youtube_dl_constructor: MagicMock, ytdlp_wrapper: YtdlpWrapper
):
    """Tests YtdlpApiError if ydl.download() raises DownloadError."""
    mock_ydl_instance = MagicMock()
    mock_ydl_instance.download.side_effect = yt_dlp.utils.DownloadError(  # type: ignore
        "Download failed"
    )
    mock_youtube_dl_constructor.return_value = mock_ydl_instance

    ydl_opts = {"outtmpl": "/path/to/file.mp4"}
    url = "http://example.com/download_video_fail"

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper._download_media_internal(ydl_opts, url)  # type: ignore
    assert "yt-dlp download failed" in str(excinfo.value).lower()


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_download_media_internal_non_zero_retcode(
    mock_youtube_dl_constructor: MagicMock, ytdlp_wrapper: YtdlpWrapper
):
    """Tests YtdlpApiError if ydl.download() returns non-zero."""
    mock_ydl_instance = MagicMock()
    mock_ydl_instance.download.return_value = 1
    mock_youtube_dl_constructor.return_value = mock_ydl_instance

    ydl_opts = {"outtmpl": "/path/to/file.mp4"}
    url = "http://example.com/download_video_retcode"

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper._download_media_internal(ydl_opts, url)  # type: ignore
    assert "failed with non-zero exit code: 1" in str(excinfo.value)


@pytest.mark.unit
@patch.object(YtdlpWrapper, "_prepare_ydl_options")
@patch.object(YtdlpWrapper, "_download_media_internal")
@patch("pathlib.Path.exists", return_value=True)
def test_download_media_to_file_success(
    mock_exists: MagicMock,
    mock_download_internal: MagicMock,
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

    mock_download_internal.return_value = None
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

    mock_download_internal.assert_called_once_with(
        mock_download_opts, dummy_download.source_url
    )

    assert result_path == expected_target_path

    mock_youtube_handler.get_source_specific_ydl_options.assert_called_once_with(
        FetchPurpose.MEDIA_DOWNLOAD
    )
    mock_exists.assert_called()


# NOTE: fetch_metadata is not tested here because it is too complex to mock
# it is covered by integration tests

# NOTE: _compose_match_filters_and may not be needed, so skipping tests for it.
