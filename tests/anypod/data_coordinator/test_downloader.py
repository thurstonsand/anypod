# pyright: reportPrivateUsage=false

"""Tests for the Downloader service.

This module contains unit tests for the Downloader class, which is responsible
for processing items in the download queue, interacting with YtdlpWrapper for
media fetching, FileManager for storage, and DownloadDatabase for status updates.
"""

from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.data_coordinator.downloader import Downloader
from anypod.data_coordinator.types import ArtifactDownloadResult, DownloadArtifact
from anypod.db import DownloadDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType, TranscriptSource
from anypod.exceptions import (
    DatabaseOperationError,
    DownloadError,
    FFmpegError,
    FFProbeError,
    YtdlpApiError,
)
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.file_manager import FileManager
from anypod.media_processor import MediaProcessor, ProcessedMediaResult
from anypod.ytdlp_wrapper import DownloadedMedia, YtdlpWrapper

# Mock Feed object for testing
MOCK_FEED = Feed(
    id="test_feed",
    title="Test Feed",
    subtitle=None,
    description=None,
    language=None,
    author=None,
    remote_image_url=None,
    is_enabled=True,
    source_type=SourceType.UNKNOWN,
    source_url="https://example.com/test",
    last_successful_sync=datetime.min.replace(tzinfo=UTC),
)

# --- Fixtures ---


@pytest.fixture
def mock_download_db() -> MagicMock:
    """Provides a mock DownloadDatabase."""
    mock = MagicMock(spec=DownloadDatabase)
    # Mock async methods
    mock.get_downloads_by_status = AsyncMock()
    mock.get_download_by_id = AsyncMock()
    mock.upsert_download = AsyncMock()
    mock.update_download = AsyncMock()
    mock.mark_as_downloaded = AsyncMock()
    mock.bump_retries = AsyncMock()
    mock.set_download_logs = AsyncMock()
    mock.set_thumbnail_extension = AsyncMock()
    mock.set_transcript_metadata = AsyncMock()
    return mock


@pytest.fixture
def mock_file_manager() -> MagicMock:
    """Provides a mock FileManager with specific async method mocks."""
    mock = MagicMock(spec=FileManager)

    # Only mock the specific async methods that FileManager actually has
    mock.delete_download_file = AsyncMock()
    mock.download_exists = AsyncMock()
    mock.get_download_file_path = AsyncMock(return_value=Path("/final/video.mp4"))
    mock.get_download_stream = AsyncMock()
    mock.get_transcript_path = AsyncMock(return_value=Path("/transcripts/video.en.vtt"))
    mock.image_exists = AsyncMock(return_value=False)
    mock.get_image_path = AsyncMock(side_effect=FileNotFoundError())

    return mock


@pytest.fixture
def mock_ytdlp_wrapper() -> MagicMock:
    """Provides a mock YtdlpWrapper."""
    mock = MagicMock(spec=YtdlpWrapper)
    # Set async methods to AsyncMock
    mock.fetch_metadata = AsyncMock()
    mock.download_media_to_file = AsyncMock()
    mock.download_media_thumbnail = AsyncMock()
    mock.download_transcript_only = AsyncMock()
    return mock


@pytest.fixture
def mock_ffprobe() -> MagicMock:
    """Provides a mock FFProbe."""
    mock = MagicMock(spec=FFProbe)
    mock.get_duration_seconds_from_file = AsyncMock(return_value=321)
    return mock


def _passthrough_media(*, media_path: Path, **_kwargs: object) -> ProcessedMediaResult:
    """Return the original media path unchanged."""
    return ProcessedMediaResult(file_path=media_path, converted=False)


@pytest.fixture
def mock_media_processor() -> MagicMock:
    """Provides a pass-through mock MediaProcessor."""
    mock = MagicMock(spec=MediaProcessor)
    mock.process = AsyncMock(side_effect=_passthrough_media)
    return mock


@pytest.fixture
def mock_ffmpeg() -> MagicMock:
    """Provides a mock FFmpeg."""
    mock = MagicMock(spec=FFmpeg)
    mock.embed_subtitles = AsyncMock()
    return mock


@pytest.fixture
def downloader(
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    mock_ytdlp_wrapper: MagicMock,
    mock_ffprobe: MagicMock,
    mock_ffmpeg: MagicMock,
    mock_media_processor: MagicMock,
) -> Downloader:
    """Provides a Downloader instance with mocked dependencies."""
    return Downloader(
        mock_download_db,
        mock_file_manager,
        mock_ytdlp_wrapper,
        mock_ffprobe,
        mock_ffmpeg,
        mock_media_processor,
    )


@pytest.fixture
def sample_download() -> Download:
    """Provides a sample Download object."""
    base_time = datetime(2023, 1, 1, 12, 0, 0, tzinfo=UTC)
    return Download(
        feed_id="test_feed",
        id="test_dl_id_1",
        source_url="http://example.com/video1",
        title="Test Video 1",
        published=base_time,
        ext="mp4",
        mime_type="video/mp4",
        duration=120,
        status=DownloadStatus.QUEUED,
        discovered_at=base_time,
        updated_at=base_time,
        description="Original description",
        filesize=0,
    )


@pytest.fixture
def sample_feed_config() -> FeedConfig:
    """Provides a sample FeedConfig object."""
    return FeedConfig(
        url="http://example.com/feed_url",
        yt_args="--format best",  # type: ignore # this gets preprocessed into a dict
        schedule="0 0 * * *",  # type: ignore
        keep_last=10,
        since=None,
        max_errors=3,
        metadata=FeedMetadataOverrides(title="Test Podcast"),  # type: ignore
    )


# --- Tests for _handle_download_success ---


@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_updates_db(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    mock_ffprobe: MagicMock,
    sample_download: Download,
):
    """Tests that _handle_download_success updates download and calls upsert."""
    downloaded_file = Path("/path/to/downloaded_video.mp4")
    logs = "yt-dlp stdout/stderr"

    await downloader._handle_download_success(sample_download, downloaded_file, logs)

    mock_ffprobe.get_duration_seconds_from_file.assert_awaited_once_with(
        downloaded_file
    )
    mock_download_db.update_download.assert_awaited_once()
    updated = mock_download_db.update_download.call_args[0][0]
    assert updated.status == DownloadStatus.DOWNLOADED
    assert updated.ext == "mp4"
    assert updated.mime_type == "video/mp4"
    assert updated.filesize == 1024
    assert updated.duration == 321
    assert updated.retries == 0
    assert updated.last_error is None
    assert updated.download_logs == logs


@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_updates_mime_type_from_final_file(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
):
    """MIME type follows the final file path after post-processing."""
    sample_download.ext = "webm"
    sample_download.mime_type = "video/webm"

    await downloader._handle_download_success(
        sample_download, Path("/path/to/downloaded_video.mp4"), "logs"
    )

    updated = mock_download_db.update_download.call_args[0][0]
    assert updated.ext == "mp4"
    assert updated.mime_type == "video/mp4"


@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_uses_octet_stream_for_unknown_extension(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
):
    """Unknown media extensions use a safe generic MIME type."""
    await downloader._handle_download_success(
        sample_download, Path("/path/to/downloaded_video.unknownext"), "logs"
    )

    updated = mock_download_db.update_download.call_args[0][0]
    assert updated.ext == "unknownext"
    assert updated.mime_type == "application/octet-stream"


@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_db_update_fails_raises_downloader_error(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
):
    """Tests that DB operation failure in _handle_download_success raises DownloadError."""
    downloaded_file = Path("/path/to/downloaded_video.mp4")
    logs = "yt-dlp stdout/stderr"
    db_error = DatabaseOperationError("DB boom")
    mock_download_db.update_download.side_effect = db_error

    with pytest.raises(DownloadError) as exc_info:
        await downloader._handle_download_success(
            sample_download, downloaded_file, logs
        )

    assert exc_info.value.__cause__ is db_error
    assert exc_info.value.feed_id == sample_download.feed_id
    assert exc_info.value.download_id == sample_download.id


@pytest.mark.asyncio
@patch("aiofiles.os.stat", new_callable=AsyncMock, return_value=MagicMock(st_size=1024))
async def test_handle_download_success_handles_ffprobe_errors(
    _mock_stat: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    mock_ffprobe: MagicMock,
    sample_download: Download,
):
    """Even if ffprobe fails, downloading should still succeed with metadata fallback."""
    mock_ffprobe.get_duration_seconds_from_file.side_effect = FFProbeError("probe boom")
    downloaded_file = Path("/path/to/downloaded_video.mp4")
    logs = "yt-dlp stdout/stderr"
    original_duration = sample_download.duration

    await downloader._handle_download_success(sample_download, downloaded_file, logs)

    mock_download_db.update_download.assert_awaited_once()
    updated = mock_download_db.update_download.call_args[0][0]
    assert updated.duration == original_duration


# --- Tests for _handle_download_failure ---


@pytest.mark.asyncio
async def test_handle_download_failure_bumps_retries(
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that _handle_download_failure calls bump_retries on DB manager."""
    error = ValueError("Download exploded")
    await downloader._handle_download_failure(
        sample_download, sample_feed_config, error
    )

    mock_download_db.bump_retries.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        error_message=str(error),
        max_allowed_errors=sample_feed_config.max_errors,
    )


@pytest.mark.asyncio
async def test_handle_download_failure_db_error_logged(
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that DB errors during bump_retries are logged and not re-raised."""
    error = ValueError("Download exploded")
    mock_download_db.bump_retries.side_effect = DatabaseOperationError(
        "DB boom for retries"
    )

    try:
        await downloader._handle_download_failure(
            sample_download, sample_feed_config, error
        )
    except Exception as e:
        pytest.fail(
            f"_handle_download_failure should not raise an exception, but got: {e}"
        )


# --- Tests for download_artifacts ---


@pytest.mark.asyncio
@patch.object(Downloader, "_handle_download_success", new_callable=AsyncMock)
async def test_download_artifacts_all_success_flow(
    mock_handle_success: AsyncMock,
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests the success path of download_artifacts with ALL artifacts and no transcript configured."""
    downloaded_path = Path("/final/video.mp4")
    logs = "yt-dlp stdout/stderr"
    mock_ytdlp_wrapper.download_media_to_file.return_value = DownloadedMedia(
        file_path=downloaded_path,
        logs=logs,
    )

    def set_thumbnail_ext(*_args: object, **_kwargs: object) -> None:
        sample_download.thumbnail_ext = "jpg"

    mock_handle_success.side_effect = set_thumbnail_ext

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.ALL
    )

    assert result.media_downloaded is True
    assert result.thumbnail_downloaded is True
    assert result.transcript_downloaded is None
    mock_ytdlp_wrapper.download_media_to_file.assert_called_once_with(
        sample_download,
        sample_feed_config.yt_args,
        cookies_path=None,
        media_conversion=sample_feed_config.media_conversion,
        prefer_audio=sample_feed_config.prefer_audio,
    )
    mock_ytdlp_wrapper.download_transcript_only.assert_not_awaited()
    mock_handle_success.assert_called_once_with(sample_download, downloaded_path, logs)


@pytest.mark.asyncio
async def test_download_artifacts_all_downloads_transcript_through_shared_path(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    mock_ffmpeg: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
    tmp_path: Path,
):
    """ALL downloads media then falls through to transcript download, MP4 embed, and persistence."""
    sample_download.ext = "webm"  # metadata ext differs from final remuxed mp4
    sample_download.transcript_source = TranscriptSource.CREATOR
    sample_feed_config.transcript_lang = "en"

    downloaded_path = tmp_path / "video.mp4"
    downloaded_path.write_bytes(b"0" * 1024)
    subtitle_path = tmp_path / "video.en.vtt"
    subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")

    mock_ytdlp_wrapper.download_media_to_file.return_value = DownloadedMedia(
        file_path=downloaded_path,
        logs="ok",
    )
    mock_file_manager.image_exists.return_value = True
    mock_file_manager.get_download_file_path.return_value = downloaded_path
    mock_file_manager.get_transcript_path.return_value = subtitle_path
    mock_ytdlp_wrapper.download_transcript_only.return_value = "vtt"

    async def grow_file_on_embed(
        media_path: Path, subtitle_path: Path, lang: str
    ) -> None:
        media_path.write_bytes(b"1" * 2048)

    mock_ffmpeg.embed_subtitles.side_effect = grow_file_on_embed

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.ALL
    )

    assert result.media_downloaded is True
    assert result.thumbnail_downloaded is True
    assert result.transcript_downloaded is True
    assert result.all_succeeded is True
    mock_ytdlp_wrapper.download_media_thumbnail.assert_not_awaited()
    mock_ytdlp_wrapper.download_transcript_only.assert_awaited_once_with(
        download=sample_download,
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
        cookies_path=None,
    )
    mock_ffmpeg.embed_subtitles.assert_awaited_once_with(
        media_path=downloaded_path,
        subtitle_path=subtitle_path,
        lang="en",
    )
    mock_download_db.set_transcript_metadata.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
        filesize=2048,
    )
    assert sample_download.ext == "mp4"
    assert sample_download.filesize == 2048


@pytest.mark.asyncio
async def test_download_artifacts_transcript_only_embeds_into_mp4_and_updates_filesize(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    mock_ffmpeg: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
    tmp_path: Path,
):
    """Standalone TRANSCRIPT download embeds subtitles into existing MP4 and updates filesize."""
    sample_download.status = DownloadStatus.DOWNLOADED
    sample_download.ext = "mp4"
    sample_download.filesize = 1000
    sample_download.transcript_source = TranscriptSource.AUTO
    sample_feed_config.transcript_lang = "en"

    media_path = tmp_path / "video.mp4"
    media_path.write_bytes(b"0" * 1000)
    subtitle_path = tmp_path / "video.en.vtt"
    subtitle_path.write_text("WEBVTT\n\n", encoding="utf-8")

    mock_file_manager.get_download_file_path.return_value = media_path
    mock_file_manager.get_transcript_path.return_value = subtitle_path
    mock_ytdlp_wrapper.download_transcript_only.return_value = "vtt"

    async def grow_file_on_embed(
        media_path: Path, subtitle_path: Path, lang: str
    ) -> None:
        media_path.write_bytes(b"1" * 4096)

    mock_ffmpeg.embed_subtitles.side_effect = grow_file_on_embed

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.TRANSCRIPT
    )

    assert result.transcript_downloaded is True
    mock_ffmpeg.embed_subtitles.assert_awaited_once_with(
        media_path=media_path,
        subtitle_path=subtitle_path,
        lang="en",
    )
    mock_download_db.set_transcript_metadata.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.AUTO,
        filesize=4096,
    )
    assert sample_download.filesize == 4096


@pytest.mark.asyncio
@pytest.mark.parametrize("ext", ["m4a", "mp3", "opus"])
async def test_download_artifacts_transcript_only_skips_embed_for_audio(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_ffmpeg: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
    ext: str,
):
    """Audio-only containers skip FFmpeg subtitle embedding and leave filesize untouched."""
    sample_download.status = DownloadStatus.DOWNLOADED
    sample_download.ext = ext
    sample_download.filesize = 1000
    sample_download.transcript_source = TranscriptSource.CREATOR
    sample_feed_config.transcript_lang = "en"
    mock_ytdlp_wrapper.download_transcript_only.return_value = "vtt"

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.TRANSCRIPT
    )

    assert result.transcript_downloaded is True
    mock_ffmpeg.embed_subtitles.assert_not_awaited()
    mock_download_db.set_transcript_metadata.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
        filesize=None,
    )
    assert sample_download.filesize == 1000


@pytest.mark.asyncio
async def test_download_artifacts_transcript_only_ffmpeg_failure_is_best_effort(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_ffmpeg: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """FFmpeg failure during subtitle embed still persists transcript metadata."""
    sample_download.status = DownloadStatus.DOWNLOADED
    sample_download.ext = "mp4"
    sample_download.filesize = 1000
    sample_download.transcript_source = TranscriptSource.CREATOR
    sample_feed_config.transcript_lang = "en"
    mock_ytdlp_wrapper.download_transcript_only.return_value = "vtt"
    mock_ffmpeg.embed_subtitles.side_effect = FFmpegError("remux boom")

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.TRANSCRIPT
    )

    assert result.transcript_downloaded is True
    assert result.errors == []
    mock_download_db.set_transcript_metadata.assert_awaited_once_with(
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        transcript_ext="vtt",
        transcript_lang="en",
        transcript_source=TranscriptSource.CREATOR,
        filesize=None,
    )
    assert sample_download.filesize == 1000


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "transcript_source",
    [TranscriptSource.NOT_AVAILABLE, None],
)
@patch.object(Downloader, "_handle_download_success", new_callable=AsyncMock)
async def test_download_artifacts_all_transcript_not_applicable_is_none(
    mock_handle_success: AsyncMock,
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
    transcript_source: TranscriptSource | None,
):
    """Returns None for transcript_downloaded when transcript is not applicable."""
    sample_download.transcript_source = transcript_source
    sample_feed_config.transcript_lang = "en"

    downloaded_path = Path("/final/video.mp4")
    mock_ytdlp_wrapper.download_media_to_file.return_value = DownloadedMedia(
        file_path=downloaded_path,
        logs="ok",
    )

    def set_thumbnail_ext(*_args: object, **_kwargs: object) -> None:
        sample_download.thumbnail_ext = "jpg"

    mock_handle_success.side_effect = set_thumbnail_ext

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.ALL
    )

    assert result.media_downloaded is True
    assert result.thumbnail_downloaded is True
    assert result.transcript_downloaded is None
    assert result.all_succeeded is True


@pytest.mark.asyncio
async def test_download_artifacts_all_ytdlp_failure_returns_failure(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests that YtdlpApiError during download returns failure result."""
    original_ytdlp_error = YtdlpApiError(
        message="yt-dlp failed",
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        logs="stderr output",
    )
    mock_ytdlp_wrapper.download_media_to_file.side_effect = original_ytdlp_error

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.ALL
    )

    assert result.all_succeeded is False
    assert result.media_downloaded is False
    assert len(result.errors) == 1
    downloader.download_db.set_download_logs.assert_awaited_once_with(  # type: ignore[attr-defined] this is an AsyncMock
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
        logs="stderr output",
    )


# --- Tests for download_queued ---


@pytest.mark.asyncio
async def test_download_queued_no_items_returns_zero_counts(
    downloader: Downloader, mock_download_db: MagicMock, sample_feed_config: FeedConfig
):
    """Tests download_queued returns (0,0) if no items are fetched from DB."""
    mock_download_db.get_downloads_by_status.return_value = []

    success, failure = await downloader.download_queued(
        "test_feed", sample_feed_config, limit=5
    )

    assert success == 0
    assert failure == 0
    mock_download_db.get_downloads_by_status.assert_awaited_once_with(
        DownloadStatus.QUEUED, "test_feed", 5
    )


@pytest.mark.asyncio
async def test_download_queued_db_fetch_error_raises_downloader_error(
    downloader: Downloader, mock_download_db: MagicMock, sample_feed_config: FeedConfig
):
    """Tests that DB error when fetching queued items raises DownloadError."""
    db_error = DatabaseOperationError("DB fetch failed")
    mock_download_db.get_downloads_by_status.side_effect = db_error

    with pytest.raises(DownloadError) as exc_info:
        await downloader.download_queued("test_feed", sample_feed_config)

    assert exc_info.value.feed_id == "test_feed"
    assert exc_info.value.__cause__ is db_error


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
async def test_download_queued_processes_items_and_counts_success(
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Tests download_queued iterates and counts successful processing."""
    download2 = sample_download.model_copy(update={"id": "test_dl_id_2"})
    queued_items = [sample_download, download2]
    mock_download_db.get_downloads_by_status.return_value = queued_items

    mock_download_artifacts.return_value = ArtifactDownloadResult(
        media_downloaded=True, thumbnail_downloaded=True, transcript_downloaded=True
    )

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 2
    assert failure == 0
    mock_download_artifacts.assert_has_calls(
        [
            call(sample_download, sample_feed_config, DownloadArtifact.ALL, None),
            call(download2, sample_feed_config, DownloadArtifact.ALL, None),
        ],
        any_order=False,
    )


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
@patch.object(Downloader, "_handle_download_failure", new_callable=AsyncMock)
async def test_download_queued_processes_items_and_counts_failures(
    mock_handle_failure: AsyncMock,
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Tests download_queued iterates and counts failures from download_artifacts."""
    download2 = sample_download.model_copy(update={"id": "test_dl_id_2"})
    queued_items = [sample_download, download2]
    mock_download_db.get_downloads_by_status.return_value = queued_items

    failure_error = DownloadError("Processing failed")
    mock_download_artifacts.return_value = ArtifactDownloadResult(
        media_downloaded=False, errors=[failure_error]
    )

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 0
    assert failure == 2
    mock_download_artifacts.assert_has_calls(
        [
            call(sample_download, sample_feed_config, DownloadArtifact.ALL, None),
            call(download2, sample_feed_config, DownloadArtifact.ALL, None),
        ],
        any_order=False,
    )


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
@patch.object(Downloader, "_handle_download_failure", new_callable=AsyncMock)
async def test_download_queued_mixed_success_and_failure(
    mock_handle_failure: AsyncMock,
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Tests download_queued handles a mix of success and failure."""
    dl1 = sample_download.model_copy(update={"id": "dl1_success"})
    dl2 = sample_download.model_copy(update={"id": "dl2_fail"})
    dl3 = sample_download.model_copy(update={"id": "dl3_success"})
    queued_items = [dl1, dl2, dl3]
    mock_download_db.get_downloads_by_status.return_value = queued_items

    success_result = ArtifactDownloadResult(
        media_downloaded=True, thumbnail_downloaded=True, transcript_downloaded=True
    )
    failure_error = DownloadError("dl2 failed")
    failure_result = ArtifactDownloadResult(
        media_downloaded=False, errors=[failure_error]
    )

    mock_download_artifacts.side_effect = [
        success_result,
        failure_result,
        success_result,
    ]

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 2
    assert failure == 1
    mock_download_artifacts.assert_has_calls(
        [
            call(dl1, sample_feed_config, DownloadArtifact.ALL, None),
            call(dl2, sample_feed_config, DownloadArtifact.ALL, None),
            call(dl3, sample_feed_config, DownloadArtifact.ALL, None),
        ],
        any_order=False,
    )


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
@patch.object(Downloader, "_handle_download_failure", new_callable=AsyncMock)
async def test_download_queued_counts_success_when_media_succeeds_and_transcript_fails(
    mock_handle_failure: AsyncMock,
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Best-effort transcript failure after media succeeds does not fail or bump retries."""
    mock_download_db.get_downloads_by_status.return_value = [sample_download]
    mock_download_artifacts.return_value = ArtifactDownloadResult(
        media_downloaded=True,
        thumbnail_downloaded=True,
        transcript_downloaded=False,
    )

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 1
    assert failure == 0
    mock_handle_failure.assert_not_awaited()


# --- Tests for download_delay filtering ---


@pytest.fixture
def feed_config_with_delay() -> FeedConfig:
    """Provides a FeedConfig with download_delay configured."""
    return FeedConfig(
        url="http://example.com/feed_url",
        yt_args="--format best",  # type: ignore # this gets preprocessed
        schedule="0 0 * * *",  # type: ignore
        keep_last=10,
        since=None,
        max_errors=3,
        download_delay=timedelta(hours=24),
        metadata=FeedMetadataOverrides(title="Test Podcast"),  # type: ignore
    )


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
async def test_download_queued_with_delay_filters_recent_downloads(
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    feed_config_with_delay: FeedConfig,
    sample_download: Download,
):
    """Downloads published within the delay window are skipped."""
    now = datetime.now(UTC)

    old_download = sample_download.model_copy(
        update={
            "id": "old_dl",
            "published": now - timedelta(hours=48),
        }
    )
    recent_download = sample_download.model_copy(
        update={
            "id": "recent_dl",
            "published": now - timedelta(hours=12),
        }
    )

    mock_download_db.get_downloads_by_status.return_value = [
        old_download,
        recent_download,
    ]
    mock_download_artifacts.return_value = ArtifactDownloadResult(
        media_downloaded=True, thumbnail_downloaded=True, transcript_downloaded=True
    )

    success, failure = await downloader.download_queued(
        "test_feed", feed_config_with_delay
    )

    assert success == 1
    assert failure == 0
    mock_download_artifacts.assert_called_once_with(
        old_download, feed_config_with_delay, DownloadArtifact.ALL, None
    )


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
async def test_download_queued_with_delay_at_boundary(
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    feed_config_with_delay: FeedConfig,
    sample_download: Download,
):
    """Downloads exactly at the delay boundary are processed."""
    now = datetime.now(UTC)

    boundary_download = sample_download.model_copy(
        update={
            "id": "boundary_dl",
            "published": now - timedelta(hours=24),
        }
    )

    mock_download_db.get_downloads_by_status.return_value = [boundary_download]
    mock_download_artifacts.return_value = ArtifactDownloadResult(
        media_downloaded=True, thumbnail_downloaded=True, transcript_downloaded=True
    )

    success, failure = await downloader.download_queued(
        "test_feed", feed_config_with_delay
    )

    assert success == 1
    assert failure == 0


@pytest.mark.asyncio
async def test_download_queued_with_delay_all_deferred(
    downloader: Downloader,
    mock_download_db: MagicMock,
    feed_config_with_delay: FeedConfig,
    sample_download: Download,
):
    """When all downloads are within delay window, returns (0, 0)."""
    now = datetime.now(UTC)

    recent_download = sample_download.model_copy(
        update={
            "id": "recent_dl",
            "published": now - timedelta(hours=1),  # 1 hour ago - should skip
        }
    )

    mock_download_db.get_downloads_by_status.return_value = [recent_download]

    success, failure = await downloader.download_queued(
        "test_feed", feed_config_with_delay
    )

    assert success == 0
    assert failure == 0


@pytest.mark.asyncio
@patch.object(Downloader, "download_artifacts", new_callable=AsyncMock)
async def test_download_queued_without_delay_processes_all(
    mock_download_artifacts: AsyncMock,
    downloader: Downloader,
    mock_download_db: MagicMock,
    sample_feed_config: FeedConfig,
    sample_download: Download,
):
    """Without download_delay configured, all downloads are processed."""
    now = datetime.now(UTC)

    recent_download = sample_download.model_copy(
        update={
            "id": "recent_dl",
            "published": now - timedelta(minutes=5),
        }
    )

    mock_download_db.get_downloads_by_status.return_value = [recent_download]
    mock_download_artifacts.return_value = ArtifactDownloadResult(
        media_downloaded=True, thumbnail_downloaded=True, transcript_downloaded=True
    )

    success, failure = await downloader.download_queued("test_feed", sample_feed_config)

    assert success == 1
    assert failure == 0
    mock_download_artifacts.assert_called_once()


# --- Tests for download_thumbnail_for_existing_download ---


@pytest.mark.asyncio
async def test_download_artifacts_thumbnail_only_success(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests successful thumbnail-only download updates database and returns True."""
    mock_file_manager.image_exists.return_value = True

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.THUMBNAIL
    )

    assert result.thumbnail_downloaded is True
    mock_ytdlp_wrapper.download_media_thumbnail.assert_awaited_once_with(
        sample_download, sample_feed_config.yt_args, None
    )
    mock_file_manager.image_exists.assert_awaited_once_with(
        sample_download.feed_id, sample_download.id, "jpg"
    )
    mock_download_db.set_thumbnail_extension.assert_awaited_once_with(
        sample_download.feed_id, sample_download.id, "jpg"
    )


@pytest.mark.asyncio
async def test_download_artifacts_thumbnail_only_ytdlp_failure(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests yt-dlp failure returns False without updating database."""
    mock_ytdlp_wrapper.download_media_thumbnail.side_effect = YtdlpApiError(
        message="Thumbnail fetch failed",
        feed_id=sample_download.feed_id,
        download_id=sample_download.id,
    )

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.THUMBNAIL
    )

    assert result.thumbnail_downloaded is False
    mock_ytdlp_wrapper.download_media_thumbnail.assert_awaited_once()
    mock_download_db.set_thumbnail_extension.assert_not_awaited()


@pytest.mark.asyncio
async def test_download_artifacts_thumbnail_only_file_not_found(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests returns False when thumbnail file not found after download."""
    mock_file_manager.image_exists.return_value = False

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.THUMBNAIL
    )

    assert result.thumbnail_downloaded is False
    mock_ytdlp_wrapper.download_media_thumbnail.assert_awaited_once()
    mock_file_manager.image_exists.assert_awaited_once()
    mock_download_db.set_thumbnail_extension.assert_not_awaited()


@pytest.mark.asyncio
async def test_download_artifacts_thumbnail_only_db_failure(
    downloader: Downloader,
    mock_ytdlp_wrapper: MagicMock,
    mock_download_db: MagicMock,
    mock_file_manager: MagicMock,
    sample_download: Download,
    sample_feed_config: FeedConfig,
):
    """Tests database failure returns error in result."""
    mock_file_manager.image_exists.return_value = True
    db_error = DatabaseOperationError("DB update failed")
    mock_download_db.set_thumbnail_extension.side_effect = db_error

    result = await downloader.download_artifacts(
        sample_download, sample_feed_config, DownloadArtifact.THUMBNAIL
    )

    assert result.thumbnail_downloaded is False
    assert len(result.errors) == 1
    assert isinstance(result.errors[0], DownloadError)
    assert result.errors[0].__cause__ is db_error
    assert result.errors[0].feed_id == sample_download.feed_id
    assert result.errors[0].download_id == sample_download.id
    mock_ytdlp_wrapper.download_media_thumbnail.assert_awaited_once()
    mock_download_db.set_thumbnail_extension.assert_awaited_once()
