from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from anypod.exceptions import YtdlpApiError
from anypod.ytdlp_wrapper import YtdlpWrapper
from anypod.ytdlp_wrapper.base_handler import FetchPurpose
from anypod.ytdlp_wrapper.youtube_handler import YoutubeHandler


@pytest.fixture
def mock_youtube_handler() -> MagicMock:
    """Fixture to provide a mocked YoutubeHandler."""
    handler = MagicMock(spec=YoutubeHandler)
    # Configure default return values for methods that might be called
    # if the wrapper's methods are not carefully unit tested.
    # For now, we'll assume specific tests will mock specific interactions.
    return handler


@pytest.fixture
def ytdlp_wrapper(mock_youtube_handler: MagicMock) -> YtdlpWrapper:
    """Fixture to provide a YtdlpWrapper instance with a mocked YoutubeHandler."""
    wrapper = YtdlpWrapper()
    # Directly inject the mock handler
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
    )  # Simulate empty parsed user opts

    user_cli_args: list[str] = []
    purpose = FetchPurpose.DISCOVERY
    source_specific_opts: dict[str, Any] = {}

    prepared_opts = ytdlp_wrapper._prepare_ydl_options(  # type: ignore
        user_cli_args, purpose, source_specific_opts
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
        user_cli_args, purpose, source_specific_opts
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
def test_prepare_ydl_options_with_user_cli_args_and_source_opts(
    mock_parse_options: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """
    Tests option preparation with user CLI args and source-specific options,
    ensuring they are merged correctly.
    """
    # Simulate parsed user options from CLI args
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
        user_cli_args, purpose, source_specific_opts
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
            user_cli_args, purpose, source_specific_opts
        )

    mock_parse_options.assert_called_once_with(user_cli_args)


# Test for _extract_yt_dlp_info_internal and fetch_metadata will be added next.


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

    ydl_opts = {"skip_download": True}
    url = "http://example.com/video"

    result = ytdlp_wrapper._extract_yt_dlp_info_internal(ydl_opts, url)  # type: ignore

    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts)
    mock_ydl_instance.extract_info.assert_called_once_with(url, download=False)
    assert result == expected_info_dict


@pytest.mark.unit
@patch("anypod.ytdlp_wrapper.ytdlp_wrapper.yt_dlp.YoutubeDL")
def test_extract_yt_dlp_info_internal_ydl_init_fails(
    mock_youtube_dl_constructor: MagicMock,
    ytdlp_wrapper: YtdlpWrapper,
):
    """Tests that YtdlpApiError is raised if YoutubeDL instantiation fails."""
    mock_youtube_dl_constructor.side_effect = Exception("Init failed")

    ydl_opts = {"skip_download": True}
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
    mock_ydl_instance.extract_info.side_effect = Exception("Simulated DownloadError")

    ydl_opts = {"skip_download": True}
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

    ydl_opts = {"skip_download": True}
    url = "http://example.com/video_generic_error"

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper._extract_yt_dlp_info_internal(ydl_opts, url)  # type: ignore

    mock_youtube_dl_constructor.assert_called_once_with(ydl_opts)
    mock_ydl_instance.extract_info.assert_called_once_with(url, download=False)


# NOTE: fetch_metadata is not tested here because it is too complex to mock
# it is covered by integration tests

# NOTE: _compose_match_filters_and may not be needed, so skipping tests for it.
