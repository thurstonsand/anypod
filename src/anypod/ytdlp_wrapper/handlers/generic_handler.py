"""Generic best-effort handler for arbitrary yt-dlp-supported sources.

Provides a fallback handler implementing ``SourceHandlerBase`` for URLs that
do not match a first-class source handler (YouTube, Patreon, X/Twitter), such
as Vimeo, Twitch, SoundCloud, Nebula, or direct media sites supported by
yt-dlp.
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
import logging
from pathlib import Path

from ...db.types import Download, DownloadStatus, Feed, SourceType, TranscriptSource
from ...exceptions import (
    YtdlpApiError,
    YtdlpDataError,
    YtdlpDownloadFilteredOutError,
    YtdlpFieldInvalidError,
    YtdlpFieldMissingError,
)
from ...mimetypes import mimetypes
from ..core import YtdlpArgs, YtdlpCore, YtdlpInfo

logger = logging.getLogger(__name__)


class YtdlpGenericDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for a generic source.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self, message: str, feed_id: str | None = None, download_id: str | None = None
    ) -> None:
        super().__init__(f"Generic Parser: {message}")
        self.feed_id = feed_id
        self.download_id = download_id


class YtdlpGenericFilteredOutError(YtdlpDownloadFilteredOutError):
    """Raised when an item from a generic source is filtered out by yt-dlp.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self, feed_id: str | None = None, download_id: str | None = None
    ) -> None:
        super().__init__(
            "Generic: Item filtered out by yt-dlp.",
            feed_id=feed_id,
            download_id=download_id,
        )


class GenericEntry:
    """Represent a single yt-dlp entry from an arbitrary extractor.

    Provides best-effort field extraction with sensible fallbacks for optional
    fields like duration, filesize, and published timestamp so arbitrary
    yt-dlp extractors work without source-specific assumptions.

    Attributes:
        feed_id: The feed identifier this entry belongs to.
        download_id: The item identifier reported by yt-dlp.
        _ytdlp_info: The underlying yt-dlp metadata.
    """

    def __init__(self, ytdlp_info: YtdlpInfo, feed_id: str) -> None:
        self._ytdlp_info = ytdlp_info
        self.feed_id = feed_id

        try:
            self.download_id = self._ytdlp_info.required("id", str)
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpGenericDataError(
                message="Failed to parse entry ID.",
                feed_id=self.feed_id,
                download_id="<missing_id>",
            ) from e

    @contextmanager
    def _annotate_exceptions(self) -> Generator[None]:
        try:
            yield
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpGenericDataError(
                message="Failed to parse generic entry.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def webpage_url(self) -> str | None:
        """Get the webpage URL for the entry."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("webpage_url", str)

    @property
    def original_url(self) -> str | None:
        """Get the original URL for the entry."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("original_url", str)

    @property
    def extractor(self) -> str | None:
        """Get the extractor name (normalized to lowercase)."""
        with self._annotate_exceptions():
            extractor = self._ytdlp_info.get("extractor", str)
            return extractor.lower() if extractor else None

    @property
    def type(self) -> str | None:
        """Get the entry type from yt-dlp metadata."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("_type", str)

    @property
    def live_status(self) -> str | None:
        """Get the live status if present."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("live_status", str)

    @property
    def playlist_index(self) -> int | None:
        """Get the 1-based playlist index if present."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("playlist_index", int)

    @property
    def epoch(self) -> datetime:
        """Get the yt-dlp request epoch, defaulting to current UTC time if absent."""
        with self._annotate_exceptions():
            epoch_timestamp = self._ytdlp_info.get("epoch", (int, float))
        if epoch_timestamp is None:
            return datetime.now(UTC)
        return datetime.fromtimestamp(float(epoch_timestamp), UTC)

    @property
    def title(self) -> str:
        """Get the entry title."""
        with self._annotate_exceptions():
            return self._ytdlp_info.required("title", str)

    @property
    def description(self) -> str | None:
        """Get the entry description."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("description", str)

    @property
    def uploader(self) -> str | None:
        """Get the channel or uploader name."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("channel", str) or self._ytdlp_info.get(
                "uploader", str
            )

    @property
    def thumbnail(self) -> str | None:
        """Get a best-effort JPG/PNG thumbnail URL."""
        with self._annotate_exceptions():
            thumbnails = self._ytdlp_info.thumbnails()
            if not thumbnails:
                return self._ytdlp_info.get("thumbnail", str)
            best = thumbnails.best_supported()
            return best.url if best else self._ytdlp_info.get("thumbnail", str)

    @property
    def ext(self) -> str | None:
        """Get the media extension if present."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("ext", str)

    @property
    def mime_type(self) -> str:
        """Infer MIME type from extension, defaulting to application/octet-stream."""
        ext = self.ext
        if not ext:
            return "application/octet-stream"
        guessed = mimetypes.guess_type(f"file.{ext.lstrip('.')}")[0]
        return guessed or "application/octet-stream"

    @property
    def filesize(self) -> int:
        """Get filesize or filesize_approx, defaulting to placeholder 1 if unavailable."""
        with self._annotate_exceptions():
            raw_filesize = self._ytdlp_info.get(
                "filesize", (int, float)
            ) or self._ytdlp_info.get("filesize_approx", (int, float))
        if raw_filesize is None or int(raw_filesize) <= 0:
            return 1
        return int(raw_filesize)

    @property
    def timestamp(self) -> datetime | None:
        """Get the timestamp as a UTC datetime object."""
        with self._annotate_exceptions():
            ts = self._ytdlp_info.get("timestamp", (int, float))
        if ts is None:
            return None
        try:
            return datetime.fromtimestamp(float(ts), UTC)
        except (TypeError, ValueError, OSError) as e:
            raise YtdlpGenericDataError(
                message=f"Invalid timestamp: '{ts}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def upload_date(self) -> datetime | None:
        """Get the upload date (YYYYMMDD) as a UTC datetime object."""
        with self._annotate_exceptions():
            upload_date_str = self._ytdlp_info.get("upload_date", str)
        if upload_date_str is None:
            return None
        try:
            return datetime.strptime(upload_date_str, "%Y%m%d").replace(tzinfo=UTC)
        except (TypeError, ValueError) as e:
            raise YtdlpGenericDataError(
                message=f"Invalid upload date: '{upload_date_str}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def duration(self) -> int:
        """Get duration in seconds, falling back to placeholder 1 if unavailable."""
        raw_duration = self._ytdlp_info.get_raw("duration")
        normalized = 0
        match raw_duration:
            case bool() | None:
                normalized = 0
            case float() as num:
                normalized = int(num)
            case int() as num:
                normalized = num
            case str() as s:
                try:
                    normalized = int(float(s))
                except ValueError:
                    normalized = 0
            case _:
                normalized = 0
        return normalized if normalized > 0 else 1

    @property
    def quality_info(self) -> str | None:
        """Get a concise quality string if resolution or codec info is available."""
        with self._annotate_exceptions():
            parts: list[str] = []
            if resolution := self._ytdlp_info.get("resolution", str):
                parts.append(resolution)
            elif height := self._ytdlp_info.get("height", int):
                parts.append(f"{height}p")
            acodec = self._ytdlp_info.get("acodec", str)
            if acodec and acodec != "none":
                parts.append(acodec)
            return " | ".join(parts) if parts else None


class GenericHandler:
    """Best-effort generic handler for arbitrary yt-dlp-supported sources.

    Implements ``SourceHandlerBase`` without assuming YouTube-specific URL
    structures, tabs, or transcript APIs.
    """

    async def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        base_args: YtdlpArgs,
    ) -> tuple[str | None, SourceType]:
        """Classify a generic URL as a playlist or single video via flat discovery.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to analyze.
            base_args: Pre-configured YtdlpArgs with shared settings.

        Returns:
            Tuple of (final_url_to_fetch, source_type).
        """
        log_params = {"feed_id": feed_id, "initial_url": initial_url}
        logger.debug("Determining generic fetch strategy.", extra=log_params)

        discovery_args = base_args.skip_download().flat_playlist()
        discovery_result = await YtdlpCore.extract_playlist_info(
            discovery_args, initial_url
        )
        entry = GenericEntry(discovery_result.payload, feed_id)
        fetch_url = entry.webpage_url or initial_url

        if entry.type == "playlist":
            return fetch_url, SourceType.PLAYLIST
        if entry.type in ("video", "url") or entry.ext is not None:
            return fetch_url, SourceType.SINGLE_VIDEO
        return fetch_url, SourceType.SINGLE_VIDEO

    def prepare_playlist_info_args(self, args: YtdlpArgs) -> YtdlpArgs:
        """Return args unchanged for generic playlist metadata calls."""
        return args

    def extract_feed_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_type: SourceType,
        source_url: str,
    ) -> Feed:
        """Extract feed-level metadata from a generic yt-dlp response.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            source_type: The type of source being parsed.
            source_url: The original source URL for this feed.

        Returns:
            Feed object with extracted metadata populated.
        """
        entry = GenericEntry(ytdlp_info, feed_id)
        return Feed(
            id=feed_id,
            is_enabled=True,
            source_type=source_type,
            source_url=source_url,
            last_successful_sync=entry.epoch,
            title=entry.title,
            subtitle=None,
            description=entry.description,
            language=None,
            author=entry.uploader,
            remote_image_url=entry.thumbnail,
        )

    def prepare_thumbnail_args(self, args: YtdlpArgs) -> YtdlpArgs:
        """Return args unchanged for generic thumbnail downloads."""
        return args

    def prepare_downloads_info_args(self, args: YtdlpArgs) -> YtdlpArgs:
        """Return args unchanged for generic downloads metadata calls."""
        return args

    async def extract_download_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        transcript_lang: str | None = None,
        transcript_source_priority: list[TranscriptSource] | None = None,
    ) -> Download:
        """Extract metadata from a single generic yt-dlp entry.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata for a single item.
            transcript_lang: Optional language code for transcripts.
            transcript_source_priority: Optional ordered list of transcript sources.

        Returns:
            Parsed Download object.

        Raises:
            YtdlpGenericDataError: If required fields like source URL are missing.
            YtdlpGenericFilteredOutError: If the entry has no downloadable media.
        """
        entry = GenericEntry(ytdlp_info, feed_id)
        source_url = entry.webpage_url or entry.original_url
        if not source_url:
            raise YtdlpGenericDataError(
                "Missing source URL.",
                feed_id=feed_id,
                download_id=entry.download_id,
            )

        ext = entry.ext
        if not ext:
            raise YtdlpGenericFilteredOutError(feed_id, entry.download_id)

        published_dt = entry.timestamp or entry.upload_date
        if published_dt is None:
            logger.warning(
                "Generic source metadata missing publish date; falling back to fetch time.",
                extra={"feed_id": feed_id, "download_id": entry.download_id},
            )
            published_dt = entry.epoch

        status = (
            DownloadStatus.UPCOMING
            if entry.live_status in ("is_live", "is_upcoming")
            else DownloadStatus.QUEUED
        )

        transcript = (
            ytdlp_info.transcript(transcript_lang, transcript_source_priority)
            if transcript_lang and transcript_source_priority
            else None
        )
        transcript_source = (
            transcript.source if transcript else TranscriptSource.NOT_AVAILABLE
        )

        return Download(
            feed_id=feed_id,
            id=entry.download_id,
            source_url=source_url,
            title=entry.title,
            published=published_dt,
            ext=ext,
            mime_type=entry.mime_type,
            filesize=entry.filesize,
            duration=entry.duration,
            status=status,
            remote_thumbnail_url=entry.thumbnail,
            description=entry.description,
            quality_info=entry.quality_info,
            playlist_index=entry.playlist_index,
            transcript_source=transcript_source,
        )

    def prepare_media_download_args(
        self,
        args: YtdlpArgs,
        download: Download,
    ) -> YtdlpArgs:
        """Apply playlist item selection when present on the download.

        Args:
            args: Base YtdlpArgs to modify.
            download: The Download object being processed.

        Returns:
            Modified YtdlpArgs.
        """
        if download.playlist_index is not None:
            args = args.playlist_items(download.playlist_index)
        return args

    async def download_transcript(
        self,
        download: Download,
        transcript_lang: str,
        transcript_source: TranscriptSource,
        output_path: Path,
        cookies_path: Path | None = None,
    ) -> bool:
        """Download subtitles via yt-dlp as VTT for generic sources.

        Args:
            download: The Download to fetch subtitles for.
            transcript_lang: Language code (e.g., "en").
            transcript_source: Source type (CREATOR or AUTO).
            output_path: Destination VTT path.
            cookies_path: Optional cookies file path.

        Returns:
            True if the subtitle file was downloaded, False otherwise.
        """
        transcripts_dir = output_path.parent
        transcripts_dir.mkdir(parents=True, exist_ok=True)

        args = (
            YtdlpArgs()
            .quiet()
            .no_warnings()
            .skip_download()
            .sub_format("vtt")
            .sub_langs(transcript_lang)
            .convert_subs("vtt")
            .paths_temp(transcripts_dir)
            .paths_subtitle(transcripts_dir)
            .output_subtitle(f"{download.id}.%(ext)s")
        )
        if cookies_path is not None:
            args = args.cookies(cookies_path)
        if download.playlist_index is not None:
            args = args.playlist_items(download.playlist_index)

        if transcript_source == TranscriptSource.CREATOR:
            args = args.write_subs()
        elif transcript_source == TranscriptSource.AUTO:
            args = args.write_auto_subs()
        else:
            return False

        try:
            await YtdlpCore.download(args, download.source_url)
        except YtdlpApiError as e:
            e.download_id = download.id
            e.url = download.source_url
            raise

        return output_path.exists()
