from datetime import UTC, datetime
from pathlib import Path

import pytest

from anypod.db import Download, DownloadStatus
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

# CLI args for minimal quality and limited playlist downloads
YT_DLP_MINIMAL_ARGS = [
    "--playlist-items",
    "1",  # Fetch only one download from playlists/channels
    "-f",
    "worst[ext=mp4]",
]

# Metadata for Big Buck Bunny video - used in several tests
BIG_BUCK_BUNNY_DOWNLOAD = Download(
    feed="video",
    id="aqz-KE-bpKQ",
    source_url="https://www.youtube.com/watch?v=aqz-KE-bpKQ",
    title="Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film",
    published=datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC),
    ext="mp4",
    duration=635.0,
    status=DownloadStatus.QUEUED,
    thumbnail="https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp",
    retries=0,
    last_error=None,
)


@pytest.fixture
def ytdlp_wrapper() -> YtdlpWrapper:
    """Provides a YtdlpWrapper instance for the tests."""
    return YtdlpWrapper()


@pytest.mark.integration
@pytest.mark.parametrize("url_type, url", TEST_URLS_PARAMS)
def test_fetch_metadata_success(ytdlp_wrapper: YtdlpWrapper, url_type: str, url: str):
    """
    Tests successful metadata fetching for various URL types.
    Asserts that at least one download is returned (or exactly one due to --playlist-items 1)
    and that basic metadata fields are populated.
    """
    feed_id = f"test_{url_type}"
    downloads = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id, url=url, yt_cli_args=YT_DLP_MINIMAL_ARGS
    )

    assert len(downloads) == 1, (
        f"Expected 1 download, got {len(downloads)} for {url_type}"
    )

    download = downloads[0]
    assert download.id, f"Download ID should not be empty for {url_type}"
    assert download.title, f"Download title should not be empty for {url_type}"
    assert download.source_url, (
        f"Download source_url should not be empty for {url_type}"
    )

    assert download.published, f"Download published should not be empty for {url_type}"

    assert download.duration > 1, (
        f"Download duration should be > 1 for {url_type}, got {download.duration}"
    )

    assert download.ext == "mp4", f"Download ext should be mp4 for {url_type}"

    assert download.thumbnail, f"Download thumbnail should not be empty for {url_type}"
    assert download.status == DownloadStatus.QUEUED, (
        f"Download status should be QUEUED for {url_type}, got {download.status}"
    )


@pytest.mark.integration
def test_fetch_metadata_non_existent_video(ytdlp_wrapper: YtdlpWrapper):
    """
    Tests that fetching metadata for a non-existent video URL raises YtdlpApiError.
    """
    feed_id = "test_non_existent"

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper.fetch_metadata(
            feed_id=feed_id,
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
    feed_id = f"test_impossible_filter_{url_type}"

    impossible_filter_args = [
        "-f",
        "worst[ext=mp4]",
        "--match-filter",
        "duration > 10000000",  # will not match any video from the link
    ]

    downloads = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id, url=url, yt_cli_args=impossible_filter_args
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
    feed_id = "test_invalid_cli_arg"
    test_url = "https://www.youtube.com/@coletdjnz/videos"
    invalid_cli_args = ["--this-is-not-a-real-yt-dlp-option"]

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper.fetch_metadata(
            feed_id=feed_id, url=test_url, yt_cli_args=invalid_cli_args
        )

    assert "Invalid yt-dlp CLI arguments provided" in str(excinfo.value)


@pytest.mark.integration
def test_download_media_to_file_success(ytdlp_wrapper: YtdlpWrapper, tmp_path: Path):
    """
    Tests successful media download for a specific video.
    Asserts that the file is downloaded to the correct location and exists.
    """
    # Metadata for Big Buck Bunny video
    download = Download(
        feed="video",
        id="aqz-KE-bpKQ",
        source_url="https://www.youtube.com/watch?v=aqz-KE-bpKQ",
        title="Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film",
        published=datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC),
        ext="mp4",  # Expected extension based on common yt-dlp behavior with -f worst[ext=mp4]
        duration=635.0,
        status=DownloadStatus.QUEUED,
        thumbnail="https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp",
        retries=0,
        last_error=None,
    )

    # Use the same minimal args as other tests, could be customized if needed
    cli_args = YT_DLP_MINIMAL_ARGS

    # The download_media_to_file method constructs the full path including feed/id.ext
    # So we pass the base directory where the `video` subfolder will be created.
    base_download_dir = tmp_path / "test_dl_integration"

    expected_file_path = (
        base_download_dir / download.feed / f"{download.id}.{download.ext}"
    )

    downloaded_file_path = ytdlp_wrapper.download_media_to_file(
        download=download,
        yt_cli_args=cli_args,
        download_target_dir=base_download_dir,  # Pass the base directory
    )

    assert downloaded_file_path == expected_file_path, (
        f"Expected path {expected_file_path}, but got {downloaded_file_path}"
    )
    assert downloaded_file_path.exists(), (
        f"Downloaded file does not exist at {downloaded_file_path}"
    )
    assert downloaded_file_path.is_file(), f"Path {downloaded_file_path} is not a file"


@pytest.mark.integration
def test_download_media_to_file_non_existent(
    ytdlp_wrapper: YtdlpWrapper, tmp_path: Path
):
    """
    Tests that download fails with YtdlpApiError for a non-existent video URL.
    """
    non_existent_download = Download(
        feed="non_existent_feed",
        id="non_existent_id",
        source_url=INVALID_VIDEO_URL,
        title="This Video Does Not Exist",
        published=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        ext="mp4",
        duration=10.0,
        status=DownloadStatus.QUEUED,
    )
    cli_args = YT_DLP_MINIMAL_ARGS
    base_download_dir = tmp_path / "test_dl_non_existent"
    base_download_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper.download_media_to_file(
            download=non_existent_download,
            yt_cli_args=cli_args,
            download_target_dir=base_download_dir,
        )

    # Check for messages indicating download failure from yt-dlp
    assert "non-zero exit code" in str(excinfo.value).lower(), (
        f"Expected 'non-zero exit code' in {excinfo.value}"
    )


@pytest.mark.integration
def test_download_media_to_file_impossible_filter(
    ytdlp_wrapper: YtdlpWrapper, tmp_path: Path
):
    """
    Tests that download fails with YtdlpApiError when an impossible filter is applied.
    """
    impossible_filter_args = [
        "-f",
        "worst[ext=mp4]",
        "--match-filter",
        "duration > 99999999",
    ]
    base_download_dir = tmp_path / "test_dl_impossible_filter"
    base_download_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper.download_media_to_file(
            download=BIG_BUCK_BUNNY_DOWNLOAD,
            yt_cli_args=impossible_filter_args,
            download_target_dir=base_download_dir,
        )

    # Expecting failure because no format matches the filter during download attempt
    assert "may have been filtered out" in str(excinfo.value).lower()


@pytest.mark.integration
def test_download_media_to_file_invalid_cli_arg(
    ytdlp_wrapper: YtdlpWrapper, tmp_path: Path
):
    """
    Tests that download fails with YtdlpApiError when invalid CLI args are passed.
    """
    invalid_cli_args = ["--this-argument-is-clearly-invalid"]
    base_download_dir = tmp_path / "test_dl_invalid_arg"
    base_download_dir.mkdir(parents=True, exist_ok=True)

    with pytest.raises(YtdlpApiError) as e:
        ytdlp_wrapper.download_media_to_file(
            download=BIG_BUCK_BUNNY_DOWNLOAD,
            yt_cli_args=invalid_cli_args,
            download_target_dir=base_download_dir,
        )

    # Error should come from parse_options within _prepare_ydl_options
    assert "Invalid yt-dlp CLI arguments provided" in str(e.value)
