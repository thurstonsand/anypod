import pytest

from anypod.db import (  # Assuming Download model is still relevant for assertions
    DownloadStatus,
)
from anypod.exceptions import YtdlpApiError
from anypod.ytdlp_wrapper import YtdlpWrapper

# some CC-BY licensed urls to test with
TEST_URLS_SINGLE_AND_PLAYLIST = [
    ("video_short_link", "https://youtu.be/aqz-KE-bpKQ?si=gggSJ6WU2A1w7_FL"),
    (
        "video_in_playlist_link",
        "https://www.youtube.com/watch?v=aqz-KE-bpKQ&list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
    ),
]

TEST_URLS_PARAMS = [
    *TEST_URLS_SINGLE_AND_PLAYLIST,
    ("channel", "https://www.youtube.com/@coletdjnz"),
    ("channel_shorts_tab", "https://www.youtube.com/@coletdjnz/shorts"),
    ("channel_videos_tab", "https://www.youtube.com/@coletdjnz/videos"),
    (
        "playlist",
        "https://youtube.com/playlist?list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU&si=ZSBBgcLWYf2bxd5l",
    ),
    ("video_standard_link", "https://www.youtube.com/watch?v=ZY6TS8Q4C8s"),
]

INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality and limited playlist items
YT_DLP_MINIMAL_ARGS = [
    "--playlist-items",
    "1",  # Fetch only one item from playlists/channels
    "-f",
    "worst[ext=mp4]",
]


@pytest.fixture
def ytdlp_wrapper() -> YtdlpWrapper:
    """Provides a YtdlpWrapper instance for the tests."""
    return YtdlpWrapper()


@pytest.mark.integration
@pytest.mark.parametrize("url_type, url", TEST_URLS_PARAMS)
def test_fetch_metadata_success(ytdlp_wrapper: YtdlpWrapper, url_type: str, url: str):
    """
    Tests successful metadata fetching for various URL types.
    Asserts that at least one item is returned (or exactly one due to --playlist-items 1)
    and that basic metadata fields are populated.
    """
    feed_name = f"test_{url_type}"
    downloads = ytdlp_wrapper.fetch_metadata(
        feed_name=feed_name, url=url, yt_cli_args=YT_DLP_MINIMAL_ARGS
    )

    assert len(downloads) == 1, (
        f"Expected 1 download item, got {len(downloads)} for {url_type}"
    )

    item = downloads[0]
    assert item.id, f"Download item ID should not be empty for {url_type}"
    assert item.title, f"Download item title should not be empty for {url_type}"
    assert item.source_url, (
        f"Download item source_url should not be empty for {url_type}"
    )

    assert item.published, f"Download item published should not be empty for {url_type}"

    assert item.duration > 1, (
        f"Download item duration should be > 1 for {url_type}, got {item.duration}"
    )

    assert item.ext == "mp4", f"Download item ext should be mp4 for {url_type}"

    assert item.thumbnail, f"Download item thumbnail should not be empty for {url_type}"
    assert item.status == DownloadStatus.QUEUED, (
        f"Download item status should be QUEUED for {url_type}, got {item.status}"
    )


@pytest.mark.integration
def test_fetch_metadata_non_existent_video(ytdlp_wrapper: YtdlpWrapper):
    """
    Tests that fetching metadata for a non-existent video URL raises YtdlpApiError.
    """
    feed_name = "test_non_existent"

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper.fetch_metadata(
            feed_name=feed_name,
            url=INVALID_VIDEO_URL,
            yt_cli_args=YT_DLP_MINIMAL_ARGS,
        )


@pytest.mark.integration
@pytest.mark.parametrize("url_type, url", TEST_URLS_SINGLE_AND_PLAYLIST)
def test_fetch_metadata_with_impossible_filter(
    ytdlp_wrapper: YtdlpWrapper, url_type: str, url: str
):
    """
    Tests that fetching metadata with a filter that matches no videos returns an empty list.
    """
    feed_name = f"test_impossible_filter_{url_type}"

    impossible_filter_args = [
        "-f",
        "worst[ext=mp4]",
        "--match-filter",
        "duration > 10000000",  # will not match any video from the link
    ]

    downloads = ytdlp_wrapper.fetch_metadata(
        feed_name=feed_name, url=url, yt_cli_args=impossible_filter_args
    )
    assert len(downloads) == 0, (
        f"Expected 0 downloads for impossible filter, got {len(downloads)}"
    )


@pytest.mark.integration
def test_fetch_metadata_invalid_cli_arg(ytdlp_wrapper: YtdlpWrapper):
    """
    Tests that providing an invalid yt-dlp CLI argument raises a ValueError.
    This error originates from _prepare_ydl_options.
    """
    feed_name = "test_invalid_cli_arg"
    test_url = "https://www.youtube.com/@coletdjnz/videos"
    invalid_cli_args = ["--this-is-not-a-real-yt-dlp-option"]

    with pytest.raises(ValueError) as excinfo:
        ytdlp_wrapper.fetch_metadata(
            feed_name=feed_name, url=test_url, yt_cli_args=invalid_cli_args
        )

    assert "Invalid yt-dlp CLI arguments provided" in str(excinfo.value)
