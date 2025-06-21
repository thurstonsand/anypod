"""Integration tests for YtdlpWrapper with real YouTube URLs and yt-dlp operations."""

from collections.abc import Generator
from datetime import UTC, datetime
from pathlib import Path
import shutil

import pytest

from anypod.db.types import Download, DownloadStatus, SourceType
from anypod.exceptions import YtdlpApiError
from anypod.path_manager import PathManager
from anypod.ytdlp_wrapper import YtdlpWrapper
from anypod.ytdlp_wrapper.ytdlp_core import YtdlpCore

# some CC-BY licensed urls to test with
TEST_URLS_SINGLE_AND_PLAYLIST = [
    (
        "video_short_link",
        "https://youtu.be/aqz-KE-bpKQ?si=gggSJ6WU2A1w7_FL",
        SourceType.SINGLE_VIDEO,
        "Big Buck Bunny",
    ),
    (
        "video_in_playlist_link",
        "https://www.youtube.com/watch?v=aqz-KE-bpKQ&list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
        SourceType.PLAYLIST,
        "single video playlist",
    ),
]

# (url_type, url, expected_source_type, expected_feed_title_contains)
TEST_URLS_PARAMS = [
    (
        "video_short_link",
        "https://youtu.be/aqz-KE-bpKQ?si=gggSJ6WU2A1w7_FL",
        SourceType.SINGLE_VIDEO,
        "Big Buck Bunny",
    ),
    (
        "video_in_playlist_link",
        "https://www.youtube.com/watch?v=aqz-KE-bpKQ&list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU",
        SourceType.PLAYLIST,
        "single video playlist",
    ),
    (
        "channel",
        "https://www.youtube.com/@coletdjnz",
        SourceType.CHANNEL,
        "cole-dlp-test-acc",
    ),
    (
        "channel_shorts_tab",
        "https://www.youtube.com/@coletdjnz/shorts",
        SourceType.PLAYLIST,
        "cole-dlp-test-acc",
    ),
    (
        "channel_videos_tab",
        "https://www.youtube.com/@coletdjnz/videos",
        SourceType.PLAYLIST,
        "cole-dlp-test-acc",
    ),
    (
        "playlist",
        "https://youtube.com/playlist?list=PLt5yu3-wZAlSLRHmI1qNm0wjyVNWw1pCU&si=ZSBBgcLWYf2bxd5l",
        SourceType.PLAYLIST,
        "single video playlist",
    ),
    (
        "video_standard_link",
        "https://www.youtube.com/watch?v=ZY6TS8Q4C8s",
        SourceType.SINGLE_VIDEO,
        "VFX Artists React to Bad and Great CGi 173",
    ),
]

# --- Tests for YtdlpWrapper integration ---
INVALID_VIDEO_URL = "https://www.youtube.com/watch?v=thisvideodoesnotexistxyz"

# CLI args for minimal quality downloads
YT_DLP_MINIMAL_ARGS = YtdlpCore.parse_options(
    [
        "--format",
        "worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]",
    ]
)

# Metadata for Big Buck Bunny video - used in several tests
BIG_BUCK_BUNNY_DOWNLOAD = Download(
    feed="video",
    id="aqz-KE-bpKQ",
    source_url="https://www.youtube.com/watch?v=aqz-KE-bpKQ",
    title="Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film",
    published=datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC),
    ext="mp4",
    mime_type="video/mp4",
    filesize=12345,
    duration=635,
    status=DownloadStatus.QUEUED,
    discovered_at=datetime(2014, 11, 11, 14, 5, 55, tzinfo=UTC),
    updated_at=datetime(2014, 11, 11, 14, 5, 55, tzinfo=UTC),
    thumbnail="https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp",
    retries=0,
    last_error=None,
)


@pytest.fixture
def ytdlp_wrapper(tmp_path_factory: pytest.TempPathFactory) -> Generator[YtdlpWrapper]:
    """Provides a YtdlpWrapper instance for the tests."""
    app_tmp_dir = tmp_path_factory.mktemp("tmp")
    app_data_dir = tmp_path_factory.mktemp("data")

    paths = PathManager(
        base_data_dir=app_data_dir,
        base_url="http://localhost",
    )

    yield YtdlpWrapper(paths)

    # Teardown: remove temporary directories
    shutil.rmtree(app_tmp_dir)
    shutil.rmtree(app_data_dir)


@pytest.mark.integration
@pytest.mark.parametrize(
    "url_type, url, expected_source_type, expected_title_contains", TEST_URLS_PARAMS
)
def test_fetch_metadata_success(
    ytdlp_wrapper: YtdlpWrapper,
    url_type: str,
    url: str,
    expected_source_type: SourceType,
    expected_title_contains: str,
    cookies_path: Path | None,
):
    """Tests successful metadata fetching for various URL types.

    Asserts that at least one download is returned and that basic metadata
    fields are populated.
    """
    feed_id = f"test_{url_type}"
    feed, downloads = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=url,
        user_yt_cli_args=YT_DLP_MINIMAL_ARGS,
        keep_last=1,
        cookies_path=cookies_path,
    )

    assert len(downloads) == 1, (
        f"Expected 1 download, got {len(downloads)} for {url_type}"
    )

    # Feed metadata assertions
    assert feed.id == feed_id, f"Feed ID should match input for {url_type}"
    assert feed.is_enabled is True, f"Feed should be enabled for {url_type}"
    assert feed.source_type == expected_source_type, (
        f"Feed source_type should be {expected_source_type} for {url_type}, got {feed.source_type}"
    )
    assert feed.title and expected_title_contains.lower() in feed.title.lower(), (
        f"Feed title should contain '{expected_title_contains}' for {url_type}, got '{feed.title}'"
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
@pytest.mark.parametrize(
    "url_type, url, expected_source_type, expected_title_contains", TEST_URLS_PARAMS
)
def test_thumbnail_format_validation(
    ytdlp_wrapper: YtdlpWrapper,
    url_type: str,
    url: str,
    expected_source_type: SourceType,
    expected_title_contains: str,
    cookies_path: Path | None,
):
    """Tests that thumbnail URLs returned are in valid PNG or JPG format.

    Verifies that thumbnail filtering correctly selects only JPG or PNG thumbnails,
    excluding WebP and other unsupported formats for RSS feed compatibility.
    """
    feed_id = f"test_thumbnail_{url_type}"
    _, downloads = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=url,
        user_yt_cli_args=YT_DLP_MINIMAL_ARGS,
        keep_last=1,
        cookies_path=cookies_path,
    )

    assert len(downloads) == 1, (
        f"Expected 1 download for thumbnail test, got {len(downloads)}"
    )

    download = downloads[0]

    # All test videos should have thumbnails
    assert download.thumbnail, f"Download should have a thumbnail for {url_type}"

    # Check that thumbnail URL ends with supported format
    assert download.thumbnail.endswith(".jpg") or download.thumbnail.endswith(".png"), (
        f"Thumbnail URL should end with .jpg or .png, got: {download.thumbnail}"
    )

    # Verify it's a valid URL format
    assert download.thumbnail.startswith("http"), (
        f"Thumbnail should be a valid HTTP URL, got: {download.thumbnail}"
    )


@pytest.mark.integration
def test_fetch_metadata_non_existent_video(
    ytdlp_wrapper: YtdlpWrapper,
    cookies_path: Path | None,
):
    """Tests that fetching metadata for a non-existent video URL raises YtdlpApiError."""
    feed_id = "test_non_existent"

    with pytest.raises(YtdlpApiError):
        ytdlp_wrapper.fetch_metadata(
            feed_id=feed_id,
            url=INVALID_VIDEO_URL,
            user_yt_cli_args=YT_DLP_MINIMAL_ARGS,
            cookies_path=cookies_path,
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "url_type, url, expected_source_type, expected_title_contains",
    TEST_URLS_SINGLE_AND_PLAYLIST,
)
def test_fetch_metadata_with_impossible_filter(
    ytdlp_wrapper: YtdlpWrapper,
    url_type: str,
    url: str,
    expected_source_type: SourceType,
    expected_title_contains: str,
    cookies_path: Path | None,
):
    """Tests that fetching metadata with a filter that matches no videos returns an empty list."""
    feed_id = f"test_impossible_filter_{url_type}"

    impossible_filter_args = YtdlpCore.parse_options(
        [
            "--format",
            "worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]",
            "--match-filter",
            "duration > 10000000",
        ]
    )

    feed, downloads = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=url,
        user_yt_cli_args=impossible_filter_args,
        cookies_path=cookies_path,
    )
    assert len(downloads) == 0, (
        f"Expected 0 downloads for impossible filter, got {len(downloads)}"
    )

    # Even with impossible filter, feed metadata should still be extracted
    assert feed.id == feed_id, f"Feed ID should match input for {url_type}"
    assert feed.is_enabled is True, f"Feed should be enabled for {url_type}"
    assert feed.source_type == expected_source_type, (
        f"Feed source_type should be {expected_source_type} for {url_type}, got {feed.source_type}"
    )
    assert feed.title and expected_title_contains.lower() in feed.title.lower(), (
        f"Feed title should contain '{expected_title_contains}' for {url_type}, got '{feed.title}'"
    )


@pytest.mark.integration
def test_download_media_to_file_success(
    ytdlp_wrapper: YtdlpWrapper,
    cookies_path: Path | None,
):
    """Tests successful media download for a specific video.

    Asserts that the file is downloaded to the correct location and
    exists.
    """
    # Metadata for Big Buck Bunny video
    download = Download(
        feed="video",
        id="aqz-KE-bpKQ",
        source_url="https://www.youtube.com/watch?v=aqz-KE-bpKQ",
        title="Big Buck Bunny 60fps 4K - Official Blender Foundation Short Film",
        published=datetime(2014, 11, 10, 14, 5, 55, tzinfo=UTC),
        ext="mp4",  # Expected extension based on common yt-dlp behavior with -f worst*[ext=mp4]
        mime_type="video/mp4",
        filesize=12345,
        duration=635,
        status=DownloadStatus.QUEUED,
        thumbnail="https://i.ytimg.com/vi_webp/aqz-KE-bpKQ/maxresdefault.webp",
        retries=0,
        last_error=None,
        discovered_at=datetime(2014, 11, 11, 14, 5, 55, tzinfo=UTC),
        updated_at=datetime(2014, 11, 11, 14, 5, 55, tzinfo=UTC),
    )

    # Use the same minimal args as other tests, could be customized if needed
    cli_args = YT_DLP_MINIMAL_ARGS

    downloaded_file_path = ytdlp_wrapper.download_media_to_file(
        download=download,
        yt_cli_args=cli_args,
        cookies_path=cookies_path,
    )

    assert downloaded_file_path.exists(), (
        f"Downloaded file does not exist at {downloaded_file_path}"
    )
    assert downloaded_file_path.is_file(), f"Path {downloaded_file_path} is not a file"


@pytest.mark.integration
def test_download_media_to_file_non_existent(
    ytdlp_wrapper: YtdlpWrapper,
    cookies_path: Path | None,
):
    """Tests that download fails with YtdlpApiError for a non-existent video URL."""
    non_existent_download = Download(
        feed="non_existent_feed",
        id="non_existent_id",
        source_url=INVALID_VIDEO_URL,
        title="This Video Does Not Exist",
        published=datetime(2024, 1, 1, 0, 0, 0, tzinfo=UTC),
        ext="mp4",
        mime_type="video/mp4",
        filesize=12345,
        duration=10,
        status=DownloadStatus.QUEUED,
        discovered_at=datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC),
        updated_at=datetime(2024, 1, 2, 0, 0, 0, tzinfo=UTC),
    )
    cli_args = YT_DLP_MINIMAL_ARGS

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper.download_media_to_file(
            download=non_existent_download,
            yt_cli_args=cli_args,
            cookies_path=cookies_path,
        )

    # Check for messages indicating download failure from yt-dlp
    assert "non-zero exit code" in str(excinfo.value).lower(), (
        f"Expected 'non-zero exit code' in {excinfo.value}"
    )


@pytest.mark.integration
def test_download_media_to_file_impossible_filter(
    ytdlp_wrapper: YtdlpWrapper,
    cookies_path: Path | None,
):
    """Tests that download fails with YtdlpApiError when an impossible filter is applied."""
    impossible_filter_args = YtdlpCore.parse_options(
        [
            "--format",
            "worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]",
            "--match-filter",
            "duration > 99999999",
        ]
    )

    with pytest.raises(YtdlpApiError) as excinfo:
        ytdlp_wrapper.download_media_to_file(
            download=BIG_BUCK_BUNNY_DOWNLOAD,
            yt_cli_args=impossible_filter_args,
            cookies_path=cookies_path,
        )

    # Expecting failure because no format matches the filter during download attempt
    assert "might have filtered" in str(excinfo.value).lower()


@pytest.mark.integration
def test_fetch_metadata_with_keep_last_limit(
    ytdlp_wrapper: YtdlpWrapper,
    cookies_path: Path | None,
):
    """Tests that keep_last parameter correctly limits the number of downloads returned.

    Uses a channel URL with multiple videos and verifies that keep_last=2
    returns exactly 2 downloads (the most recent ones).
    """
    feed_id = "test_keep_last"
    # Use a channel with multiple videos
    channel_url = "https://www.youtube.com/@coletdjnz/videos"
    keep_last = 2

    minimal_args = YtdlpCore.parse_options(
        [
            "--format",
            "worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]",
        ]
    )

    feed, downloads = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=channel_url,
        user_yt_cli_args=minimal_args,
        keep_last=keep_last,
        cookies_path=cookies_path,
    )

    # Should return exactly keep_last number of downloads
    assert len(downloads) == keep_last, (
        f"Expected {keep_last} downloads with keep_last={keep_last}, got {len(downloads)}"
    )

    # Verify feed metadata is still populated correctly
    assert feed.id == feed_id
    assert feed.is_enabled is True
    assert feed.source_type == SourceType.PLAYLIST
    assert feed.title and "cole-dlp-test-acc" in feed.title.lower()

    # Verify all downloads have proper metadata
    for i, download in enumerate(downloads):
        assert download.id, f"Download {i} should have an ID"
        assert download.title, f"Download {i} should have a title"
        assert download.source_url, f"Download {i} should have a source URL"
        assert download.published, f"Download {i} should have a published date"
        assert download.status == DownloadStatus.QUEUED


@pytest.mark.integration
def test_fetch_metadata_with_keep_last_none_vs_limit(
    ytdlp_wrapper: YtdlpWrapper,
    cookies_path: Path | None,
):
    """Tests that keep_last=None returns more downloads than keep_last=1.

    Compares the number of downloads returned with and without keep_last
    to ensure the limiting is working correctly.
    """
    feed_id = "test_keep_last_comparison"
    # Use a channel with multiple videos
    channel_url = "https://www.youtube.com/@coletdjnz/videos"

    minimal_args = YtdlpCore.parse_options(
        [
            "--format",
            "worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]",
        ]
    )

    # First, fetch with keep_last=1
    _, downloads_limited = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=channel_url,
        user_yt_cli_args=minimal_args,
        keep_last=1,
        cookies_path=cookies_path,
    )

    # Then, fetch with keep_last=None (no limit, but we'll use a reasonable playlist limit to avoid too many)
    args_with_reasonable_limit = YtdlpCore.parse_options(
        [
            "--format",
            "worst*[ext=mp4]/worst[ext=mp4]/best[ext=mp4]",
        ]
    )
    _, downloads_unlimited = ytdlp_wrapper.fetch_metadata(
        feed_id=feed_id,
        url=channel_url,
        user_yt_cli_args=args_with_reasonable_limit,
        keep_last=None,
        cookies_path=cookies_path,
    )

    # Limited should return exactly 1
    assert len(downloads_limited) == 1, (
        f"Expected 1 download with keep_last=1, got {len(downloads_limited)}"
    )

    assert len(downloads_unlimited) > 1, (
        f"Expected more than 1 download with keep_last=None, got {len(downloads_unlimited)}"
    )

    # The first download should be the same in both cases (most recent)
    assert downloads_limited[0].id == downloads_unlimited[0].id, (
        "The most recent download should be the same in both limited and unlimited cases"
    )
