"""Patreon-specific handler for yt-dlp operations.

This module provides Patreon-specific implementations for fetch strategy
determination and metadata parsing. Patreon behaves similarly to YouTube in
yt-dlp's extraction model: creator pages act like playlists and individual
posts act like single videos.
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
import logging
from typing import Any, cast

from ...db.types import Download, DownloadStatus, Feed, SourceType
from ...exceptions import (
    FFProbeError,
    YtdlpDataError,
    YtdlpDownloadFilteredOutError,
    YtdlpFieldInvalidError,
    YtdlpFieldMissingError,
)
from ...ffprobe import FFProbe
from ...mimetypes import mimetypes
from ..core import YtdlpArgs, YtdlpCore, YtdlpInfo

logger = logging.getLogger(__name__)

_PATREON_REFERER = "https://www.patreon.com"


class YtdlpPatreonDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for Patreon.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self, message: str, feed_id: str | None = None, download_id: str | None = None
    ):
        super().__init__(f"Patreon Parser: {message}")
        self.feed_id = feed_id
        self.download_id = download_id


class YtdlpPatreonPostFilteredOutError(YtdlpDownloadFilteredOutError):
    """Raised when a post is filtered out (e.g., audio-only or text-only).

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(self, feed_id: str | None = None, download_id: str | None = None):
        super().__init__(
            "Patreon: Post filtered out by yt-dlp.",
            feed_id=feed_id,
            download_id=download_id,
        )


class PatreonEntry:
    """Represent a single Patreon entry with field extraction.

    Provides typed access to Patreon-specific fields from yt-dlp metadata,
    with error handling and data validation for common post properties.

    Attributes:
        feed_id: The feed identifier this entry belongs to.
        download_id: The Patreon post ID.
        _ytdlp_info: The underlying yt-dlp metadata.
    """

    def __init__(self, ytdlp_info: YtdlpInfo, feed_id: str):
        self._ytdlp_info = ytdlp_info
        self.feed_id = feed_id

        try:
            self.download_id = self._ytdlp_info.required("id", str)
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpPatreonDataError(
                message="Failed to parse Patreon entry.",
                feed_id=self.feed_id,
                download_id="<missing_id>",
            ) from e

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, PatreonEntry):
            return NotImplemented
        return self.feed_id == other.feed_id and self._ytdlp_info == other._ytdlp_info

    @contextmanager
    def _annotate_exceptions(self) -> Generator[None]:
        try:
            yield
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpPatreonDataError(
                message="Failed to parse Patreon entry.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    # --- common fields ---

    @property
    def webpage_url(self) -> str | None:
        """Get the webpage URL for the post."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("webpage_url", str)

    @property
    def original_url(self) -> str | None:
        """Get the original URL for the post."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("original_url", str)

    @property
    def extractor(self) -> str | None:
        """Get the extractor name (normalized to lowercase)."""
        with self._annotate_exceptions():
            extractor = self._ytdlp_info.get("extractor", str)
            return extractor.lower() if extractor else None

    @property
    def playlist_index(self) -> int | None:
        """Get the 1-based playlist index for this entry.

        For multi-attachment posts, this indicates which item this entry represents
        within the playlist (e.g., audio vs video). Used to select specific items
        during download with yt-dlp's --playlist-items flag.
        """
        with self._annotate_exceptions():
            return self._ytdlp_info.get("playlist_index", int)

    @property
    def type(self) -> str | None:
        """Get the entry type from yt-dlp metadata."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("_type", str)

    @property
    def epoch(self) -> datetime:
        """Get the timestamp of the request."""
        with self._annotate_exceptions():
            epoch_timestamp = self._ytdlp_info.required("epoch", int)
            return datetime.fromtimestamp(epoch_timestamp, tz=UTC)

    # --- feed-level metadata fields ---

    @property
    def title(self) -> str:
        """Get the post title."""
        with self._annotate_exceptions():
            return self._ytdlp_info.required("title", str)

    @property
    def description(self) -> str | None:
        """Get the description for the post or playlist."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("description", str)

    @property
    def uploader(self) -> str | None:
        """Get the campaign/channel name (primary) with individual uploader as fallback."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("channel", str) or self._ytdlp_info.get(
                "uploader", str
            )

    @property
    def thumbnail(self) -> str | None:
        """Get a best-effort JPG/PNG thumbnail URL for the post."""
        with self._annotate_exceptions():
            thumbnails = self._ytdlp_info.thumbnails()
            if not thumbnails:
                return self._ytdlp_info.get("thumbnail", str)
            best = thumbnails.best_supported()
            if not best:
                logger.warning(
                    "No JPG/PNG thumbnails available for Patreon entry.",
                    extra={
                        "source_url": self.webpage_url,
                        "download_id": self.download_id,
                    },
                )
                return None
            return best.url

    # --- individual post fields ---

    @property
    def media_url(self) -> str | None:
        """Return top-level media URL if present (e.g., direct MP3 URL)."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("url", str)

    @property
    def requested_download_urls(self) -> list[str]:
        """Return ordered candidate URLs extracted from requested_downloads.

        For each entry, prefer `url` then `manifest_url`.
        """
        urls: list[str] = []
        with self._annotate_exceptions():
            rd_list = self._ytdlp_info.get("requested_downloads", list[Any])
        if not rd_list:
            return urls
        for rd in rd_list:
            if not isinstance(rd, dict):
                continue
            rd_dict: dict[str, Any] = cast(dict[str, Any], rd)
            url = rd_dict.get("url")
            if isinstance(url, str):
                urls.append(url)
                continue
            manifest_url = rd_dict.get("manifest_url")
            if isinstance(manifest_url, str):
                urls.append(manifest_url)
        return urls

    @property
    def first_format_url(self) -> str | None:
        """Return first format's URL (or manifest URL) if available.

        For Patreon/Mux, all formats should have same content length.
        """
        with self._annotate_exceptions():
            fmts = self._ytdlp_info.get("formats", list[Any])
        if not fmts:
            return None
        first = fmts[0]
        if not isinstance(first, dict):
            return None
        first_dict: dict[str, Any] = cast(dict[str, Any], first)
        url = first_dict.get("url")
        if isinstance(url, str):
            return url
        manifest_url = first_dict.get("manifest_url")
        if isinstance(manifest_url, str):
            return manifest_url
        return None

    @property
    def ext(self) -> str | None:
        """Get the file extension for the post's media (if any)."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("ext", str)

    @property
    def mime_type_from_ext(self) -> str:
        """Map extension to MIME type, defaulting to octet-stream when unknown."""
        ext = self.ext
        if ext is None:
            return "application/octet-stream"
        if not ext.startswith("."):
            ext = f".{ext}"

        mime_type = mimetypes.guess_type(f"file{ext}")[0]
        if mime_type is None:
            raise YtdlpPatreonDataError(
                f"Could not determine MIME type for extension '{ext}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            )
        return mime_type

    @property
    def filesize(self) -> int:
        """Get the file size using filesize_approx as fallback.

        Returns a placeholder value of 1 if filesize metadata is unavailable, since
        actual filesize will be determined during download.

        Raises:
            YtdlpPatreonDataError: If filesize is invalid (≤0).
        """
        with self._annotate_exceptions():
            raw_filesize = self._ytdlp_info.get(
                "filesize", (int, float)
            ) or self._ytdlp_info.get("filesize_approx", (int, float))
            if raw_filesize is None:
                logger.warning(
                    "Patreon metadata missing filesize; using placeholder.",
                    extra={
                        "feed_id": self.feed_id,
                        "download_id": self.download_id,
                    },
                )
                return 1

            normalized_size = int(raw_filesize)
            if normalized_size <= 0:
                raise YtdlpPatreonDataError(
                    f"Invalid filesize: {raw_filesize}.",
                    feed_id=self.feed_id,
                    download_id=self.download_id,
                )
            return normalized_size

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
            raise YtdlpPatreonDataError(
                message=f"Invalid timestamp: '{ts}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def published_source_field(self) -> str:
        """Source field used for determining the published datetime."""
        if self.timestamp:
            return "timestamp"
        else:
            return "unknown"

    @property
    def duration_or_default(self) -> int:
        """Get duration in seconds; default to 0 when missing or invalid.

        Patreon posts frequently omit duration for audio-only or even video posts.
        This accessor deliberately avoids raising for missing duration.
        """
        match self._ytdlp_info.get_raw("duration"):
            case None:
                return 0
            case bool():
                # Avoid bool subclass of int confusion
                return 0
            case float() as num:
                return int(num)
            case int() as num:
                return num
            case str() as s:
                try:
                    return int(float(s))
                except ValueError:
                    return 0
            case _:
                return 0

    @property
    def quality_info(self) -> str | None:
        """Get a concise quality string when available.

        Keep this minimal to avoid large parsing overhead. Attempts resolution
        or height, then simple codec hints.
        """
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


class PatreonHandler:
    """Patreon-specific implementation for fetching strategy and parsing.

    Implements the SourceHandlerBase protocol to provide Patreon-specific
    behavior for URL classification and metadata parsing into Download
    objects. No live/upcoming logic is applied for Patreon posts.
    """

    def __init__(self, ffprobe: FFProbe):
        self._ffprobe = ffprobe

    async def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        base_args: YtdlpArgs,
    ) -> tuple[str | None, SourceType]:
        """Determine the fetch strategy for a Patreon URL.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to analyze.
            base_args: Pre-configured YtdlpArgs with shared settings.

        Returns:
            Tuple of (final_url_to_fetch, source_type).
        """
        logger.debug(
            "Determining Patreon fetch strategy.", extra={"initial_url": initial_url}
        )

        discovery_args = (
            base_args.skip_download().flat_playlist().referer("https://www.patreon.com")
        )

        discovery_info = await YtdlpCore.extract_playlist_info(
            discovery_args, initial_url
        )
        if not discovery_info:
            logger.warning(
                "Patreon discovery returned no info; defaulting to UNKNOWN.",
                extra={"initial_url": initial_url},
            )
            return initial_url, SourceType.UNKNOWN

        entry = PatreonEntry(discovery_info, feed_id)
        fetch_url = entry.webpage_url or initial_url

        logger.debug(
            "Patreon discovery call successful.",
            extra={
                "initial_url": initial_url,
                "fetch_url": fetch_url,
                "extractor": entry.extractor,
                "resolved_type": entry.type or "<unknown>",
            },
        )

        # Classify by extractor/type
        if entry.type == "playlist" or entry.extractor == "patreon:campaign":
            return fetch_url, SourceType.PLAYLIST
        elif entry.type in ("video", "url") or entry.extractor == "patreon":
            return fetch_url, SourceType.SINGLE_VIDEO

        logger.warning(
            "Unhandled Patreon URL classification. Defaulting to UNKNOWN.",
            extra={
                "initial_url": initial_url,
                "fetch_url": fetch_url,
                "extractor": entry.extractor,
                "type": entry.type,
            },
        )
        return fetch_url, SourceType.UNKNOWN

    def prepare_playlist_info_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Apply Patreon referer for playlist metadata calls."""
        return args.referer(_PATREON_REFERER)

    def extract_feed_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_type: SourceType,
        source_url: str,
    ) -> Feed:
        """Extract feed-level metadata from Patreon yt-dlp response.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            source_type: The type of source being parsed.
            source_url: The original source URL for this feed.

        Returns:
            Feed object with extracted metadata populated.
        """
        logger.debug(
            "Extracting Patreon feed metadata.",
            extra={"feed_id": feed_id, "source_type": source_type},
        )

        p = PatreonEntry(ytdlp_info, feed_id)

        author = p.uploader
        title = p.title
        description = p.description
        image_url = p.thumbnail
        last_successful_sync = p.epoch

        feed = Feed(
            id=feed_id,
            is_enabled=True,
            source_type=source_type,
            source_url=source_url,
            last_successful_sync=last_successful_sync,
            title=title,
            subtitle=None,
            description=description,
            language=None,
            author=author,
            remote_image_url=image_url,
        )

        logger.debug(
            "Successfully extracted Patreon feed metadata.",
            extra={
                "feed_id": feed_id,
                "source_type": source_type.value,
                "title": title,
                "author": author,
                "has_description": description is not None,
                "has_image_url": image_url is not None,
            },
        )
        return feed

    def prepare_thumbnail_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Apply Patreon referer for thumbnail downloads."""
        return args.referer(_PATREON_REFERER)

    def prepare_downloads_info_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Apply Patreon referer and match filter for downloads metadata calls."""
        return args.referer(_PATREON_REFERER).match_filter("vcodec")

    async def _probe_duration(self, feed_id: str, entry: PatreonEntry) -> int:
        """Probe duration using entry URLs; raise wrapped error on failure.

        Order of candidates:
        1) requested_download_urls[0]
        2) media_url
        3) first_format_url
        """
        candidate_url: str | None = None
        candidate_source: str | None = None
        if entry.requested_download_urls:
            candidate_url = entry.requested_download_urls[0]
            candidate_source = "requested_downloads"
        elif entry.media_url:
            candidate_url = entry.media_url
            candidate_source = "media_url"
        elif entry.first_format_url:
            candidate_url = entry.first_format_url
            candidate_source = "first_format_url"

        if not candidate_url:
            raise YtdlpPatreonDataError(
                "No media URL candidates found for duration probing.",
                feed_id=feed_id,
                download_id=entry.download_id,
            )

        try:
            logger.debug(
                "Probing duration with ffprobe.",
                extra={
                    "feed_id": feed_id,
                    "download_id": entry.download_id,
                    "candidate_source": candidate_source or "<unknown>",
                    "candidate_url": candidate_url,
                },
            )
            duration = await self._ffprobe.get_duration_seconds_from_url(
                candidate_url, headers={"Referer": "https://www.patreon.com"}
            )
        except FFProbeError as e:
            raise YtdlpPatreonDataError(
                "Failed to probe duration from media URL.",
                feed_id=feed_id,
                download_id=entry.download_id,
            ) from e

        if duration <= 0:
            raise YtdlpPatreonDataError(
                "Invalid duration after probing.",
                feed_id=feed_id,
                download_id=entry.download_id,
            )

        return duration

    async def extract_download_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
    ) -> Download:
        """Extract metadata from a single Patreon post into a Download object.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata for a single post.

        Returns:
            A Download object parsed from the metadata.

        Raises:
            YtdlpPatreonDataError: If required metadata is missing.
            YtdlpPatreonPostFilteredOutError: If the post was filtered out.
        """
        log_config = {"feed_id": feed_id}
        logger.debug("Extracting Patreon post metadata.", extra=log_config)

        entry = PatreonEntry(ytdlp_info, feed_id)

        source_url = (
            entry.webpage_url
            or entry.original_url
            or f"https://www.patreon.com/posts/{entry.download_id}"
        )

        published_dt = entry.timestamp
        if published_dt is None:
            raise YtdlpPatreonDataError(
                "Missing published datetime.",
                feed_id=feed_id,
                download_id=entry.download_id,
            )

        # Determine media properties. Posts without ext are filtered out by yt-dlp.
        ext = entry.ext
        if not ext:
            raise YtdlpPatreonPostFilteredOutError(feed_id, entry.download_id)

        mime_type = entry.mime_type_from_ext
        duration = entry.duration_or_default
        if duration <= 0:
            duration = await self._probe_duration(feed_id, entry)

        parsed_download = Download(
            feed_id=feed_id,
            id=entry.download_id,
            source_url=source_url,
            title=entry.title,
            published=published_dt,
            ext=ext,
            mime_type=mime_type,
            filesize=entry.filesize,
            duration=duration,
            status=DownloadStatus.QUEUED,
            remote_thumbnail_url=entry.thumbnail,
            description=entry.description,
            quality_info=entry.quality_info,
            playlist_index=entry.playlist_index,
        )

        logger.debug(
            "Successfully parsed Patreon post.",
            extra={
                "feed_id": feed_id,
                "download_id": entry.download_id,
                "title": entry.title,
            },
        )
        return parsed_download

    def prepare_media_download_args(
        self,
        args: YtdlpArgs,
        download: Download,
    ) -> YtdlpArgs:
        """Apply Patreon referer and playlist item selection for media downloads.

        Args:
            args: Base YtdlpArgs to modify.
            download: The Download object being processed.

        Returns:
            Modified YtdlpArgs for the download operation.
        """
        args = args.referer(_PATREON_REFERER)

        # Use playlist_index to download specific item from multi-attachment posts, if they are one
        if download.playlist_index is not None:
            args = args.playlist_items(download.playlist_index)

        return args
