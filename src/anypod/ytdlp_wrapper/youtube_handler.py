"""YouTube-specific handler for yt-dlp operations.

This module provides YouTube-specific implementations for fetch strategy
determination and metadata parsing, including handling of different YouTube
URL types (videos, channels, playlists) and status detection.
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
import logging
import mimetypes
from pathlib import Path

from ..db.types import Download, DownloadStatus, Feed, SourceType
from ..exceptions import (
    YtdlpDataError,
    YtdlpFieldInvalidError,
    YtdlpFieldMissingError,
)
from .base_handler import FetchPurpose, ReferenceType
from .core import YtdlpArgs, YtdlpCore, YtdlpInfo

logger = logging.getLogger(__name__)

# Normalize common audio extensions across platforms
mimetypes.add_type("audio/mp4", ".m4a")
mimetypes.add_type("audio/flac", ".flac")


class YtdlpYoutubeDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for YouTube.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self, message: str, feed_id: str | None = None, download_id: str | None = None
    ):
        super().__init__(f"YouTube Parser: {message}")
        self.feed_id = feed_id
        self.download_id = download_id


class YtdlpYoutubeVideoFilteredOutError(YtdlpDataError):
    """Raised when a video is filtered out by yt-dlp.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(self, feed_id: str | None = None, download_id: str | None = None):
        self.feed_id = feed_id
        self.download_id = download_id
        super().__init__("YouTube: Video filtered out by yt-dlp.")


class YoutubeEntry:
    """Represent a single YouTube video entry with field extraction.

    Provides typed access to YouTube-specific fields from yt-dlp metadata,
    with error handling and data validation for common YouTube video properties.

    Attributes:
        feed_id: The feed identifier this entry belongs to.
        download_id: The YouTube video ID.
        _ytdlp_info: The underlying yt-dlp metadata.
    """

    def __init__(self, ytdlp_info: YtdlpInfo, feed_id: str):
        self._ytdlp_info = ytdlp_info
        self.feed_id = feed_id

        # force the id to exist before moving on
        try:
            self.download_id = self._ytdlp_info.required("id", str)
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpYoutubeDataError(
                message="Failed to parse YouTube entry.",
                feed_id=self.feed_id,
                download_id="<missing_id>",
            ) from e

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, YoutubeEntry):
            return NotImplemented
        return self.feed_id == other.feed_id and self._ytdlp_info == other._ytdlp_info

    @contextmanager
    def _annotate_exceptions(self) -> Generator[None]:
        try:
            yield
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpYoutubeDataError(
                message="Failed to parse YouTube entry.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    # --- common fields ---

    @property
    def webpage_url(self) -> str | None:
        """Get the webpage URL for the video."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("webpage_url", str)

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

    # --- playlist fields ---

    @property
    def entries(self) -> list["YoutubeEntry | None"] | None:
        """Get playlist entries as YoutubeEntry objects."""
        with self._annotate_exceptions():
            entries = self._ytdlp_info.entries()
            if entries is None:
                return None
            yt_entries: list[YoutubeEntry | None] = []
            for entry in entries:
                yt_entries.append(YoutubeEntry(entry, self.feed_id) if entry else None)
            return yt_entries

    # --- individual video fields ---

    @property
    def format_id(self) -> str | None:
        """Get the format ID for the video."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("format_id", str)

    @property
    def original_url(self) -> str | None:
        """Get the original URL for the video."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("original_url", str)

    @property
    def title(self) -> str:
        """Get the video title, raising an error for deleted/private videos."""
        with self._annotate_exceptions():
            title = self._ytdlp_info.required("title", str)

        if title in ("[Deleted video]", "[Private video]"):
            raise YtdlpYoutubeDataError(
                message=f"Video unavailable or deleted (title: '{title}').",
                feed_id=self.feed_id,
                download_id=self.download_id,
            )
        return title

    @property
    def ext(self) -> str | None:
        """Get the file extension for the video."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("ext", str)

    @property
    def mime_type(self) -> str:
        """Get the MIME type for the video.

        Defaults to application/octet-stream if video is live or not present.
        """
        ext = self.ext
        if ext is None:
            return "application/octet-stream"
        if not ext.startswith("."):
            ext = f".{ext}"

        # Special case for live streams
        if ext == ".live":
            return "application/octet-stream"

        mime_type = mimetypes.guess_type(f"file{ext}")[0]
        if mime_type is None:
            raise YtdlpYoutubeDataError(
                f"Could not determine MIME type for extension '{ext}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            )

        return mime_type

    @property
    def filesize(self) -> int:
        """Get the file size for the video.

        Uses filesize_approx as fallback when filesize is None.
        Defaults to 0 if neither are present.
        """
        with self._annotate_exceptions():
            return (
                self._ytdlp_info.get("filesize", int)
                or self._ytdlp_info.get("filesize_approx", int)
                or 0
            )

    @property
    def timestamp(self) -> datetime | None:
        """Get the timestamp as a UTC datetime object."""
        with self._annotate_exceptions():
            timestamp = self._ytdlp_info.get("timestamp", (int, float))
        if timestamp is None:
            return None
        try:
            return datetime.fromtimestamp(float(timestamp), UTC)
        except (TypeError, ValueError, OSError) as e:
            raise YtdlpYoutubeDataError(
                message=f"Invalid timestamp: '{timestamp}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def upload_date(self) -> datetime | None:
        """Get the upload date as a UTC datetime object."""
        with self._annotate_exceptions():
            upload_date_str = self._ytdlp_info.get("upload_date", str)
        if upload_date_str is None:
            return None
        try:
            return datetime.strptime(upload_date_str, "%Y%m%d").replace(tzinfo=UTC)
        except (TypeError, ValueError) as e:
            raise YtdlpYoutubeDataError(
                message=f"Invalid upload date: '{upload_date_str}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def release_timestamp(self) -> datetime | None:
        """Get the release timestamp as a UTC datetime object."""
        with self._annotate_exceptions():
            release_ts = self._ytdlp_info.get("release_timestamp", (int, float))
        if release_ts is None:
            return None
        try:
            return datetime.fromtimestamp(float(release_ts), UTC)
        except (TypeError, ValueError, OSError) as e:
            raise YtdlpYoutubeDataError(
                message=f"Invalid release timestamp: '{release_ts}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

    @property
    def published_source_field(self) -> str:
        """Get the source field name used for determining the published datetime."""
        if self.timestamp:
            return "timestamp"
        elif self.upload_date:
            return "upload_date"
        elif self.release_timestamp:
            return "release_timestamp"
        else:
            # should not happen
            return "unknown"

    @property
    def is_live(self) -> bool | None:
        """Check if the video is currently live."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("is_live", bool)

    @property
    def live_status(self) -> str | None:
        """Get the live status of the video."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("live_status", str)

    @property
    def duration(self) -> int:
        """Get the video duration in seconds as an int."""
        # Explicitly check for bool first, as bool is a subclass of int
        raw_duration = self._ytdlp_info.get_raw("duration")
        if isinstance(raw_duration, bool):
            raise YtdlpYoutubeDataError(
                f"Duration had unexpected type: '({type(raw_duration)}){raw_duration}'.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            )

        # Now the normal extraction
        with self._annotate_exceptions():
            match self._ytdlp_info.required("duration", (float, int, str)):
                case float() | int() as duration:
                    return int(duration)
                case str() as duration_str:
                    try:
                        return int(float(duration_str))
                    except ValueError as e:
                        raise YtdlpYoutubeDataError(
                            f"Unparsable duration '{duration_str}'.",
                            feed_id=self.feed_id,
                            download_id=self.download_id,
                        ) from e

    @property
    def thumbnail(self) -> str | None:
        """Get the best quality JPG or PNG thumbnail URL for the video."""
        with self._annotate_exceptions():
            # Get thumbnails using the type-safe wrapper
            thumbnails = self._ytdlp_info.thumbnails()
            if not thumbnails:
                # Fallback to default thumbnail field
                return self._ytdlp_info.get("thumbnail", str)

            # Get the best supported format thumbnail
            best_thumbnail = thumbnails.best_supported()
            if not best_thumbnail:
                # No JPG/PNG thumbnails found, log warning and return None
                logger.warning(
                    "No JPG or PNG thumbnails available, skipping thumbnail",
                    extra={
                        "source_url": self.webpage_url,
                        "download_id": self.download_id,
                    },
                )
                return None

            return best_thumbnail.url

    @property
    def description(self) -> str | None:
        """Get the description for the video."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("description", str)

    @property
    def quality_info(self) -> str | None:
        """Get quality information for the video as a formatted string.

        Extracts and formats key quality-related metadata from yt-dlp including
        resolution, fps, HDR, and codec information for end users.
        """
        with self._annotate_exceptions():
            quality_parts: list[str] = []

            # Resolution - prioritize actual resolution over format_note
            if resolution := self._ytdlp_info.get("resolution", str):
                quality_parts.append(resolution)
            elif height := self._ytdlp_info.get("height", int):
                if width := self._ytdlp_info.get("width", int):
                    quality_parts.append(f"{width}x{height}")
                else:
                    quality_parts.append(f"{height}p")
            elif format_note := self._ytdlp_info.get("format_note", str):
                # Fallback to format_note for edge cases
                quality_parts.append(format_note)

            # Frame rate - only show if notable (not 30fps)
            match self._ytdlp_info.get("fps", (int, float)):
                case int() as fps if fps != 30:
                    quality_parts.append(f"{fps}fps")
                case float() as fps:
                    quality_parts.append(f"{fps:.1f}fps")
                case _:
                    pass

            # Dynamic range - only show HDR variants
            match self._ytdlp_info.get("dynamic_range", str):
                case str() as dynamic_range if dynamic_range != "SDR":
                    quality_parts.append(dynamic_range)
                case _:
                    pass

            # Video codec - simplified for readability
            match self._ytdlp_info.get("vcodec", str):
                case "none" | None:
                    pass
                case vcodec if vcodec.startswith("av01"):
                    quality_parts.append("AV1")
                case vcodec if vcodec.startswith(("vp09", "vp9")):
                    quality_parts.append("VP9")
                case vcodec if vcodec.startswith(("avc1", "h264")):
                    quality_parts.append("H.264")
                case vcodec if vcodec.startswith(("hev1", "h265")):
                    quality_parts.append("H.265")
                case vcodec:
                    quality_parts.append(vcodec)

            # Audio codec - simplified for readability
            match self._ytdlp_info.get("acodec", str):
                case "none" | None:
                    pass
                case "opus":
                    quality_parts.append("Opus")
                case acodec if acodec.startswith("mp4a.40"):
                    quality_parts.append("AAC")
                case "mp3":
                    quality_parts.append("MP3")
                case acodec:
                    quality_parts.append(acodec)

            return " | ".join(quality_parts) if quality_parts else None

    # --- feed-level metadata fields ---

    @property
    def channel(self) -> str | None:
        """Get the channel name for feed author."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("channel", str)

    @property
    def uploader(self) -> str | None:
        """Get the uploader name (fallback for feed author)."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("uploader", str)


class YoutubeHandler:
    """YouTube-specific implementation for fetching strategy and parsing.

    Implements the SourceHandlerBase protocol to provide YouTube-specific
    behavior for URL classification, option customization, and metadata
    parsing into Download objects.
    """

    def set_source_specific_ytdlp_options(
        self, args: YtdlpArgs, purpose: FetchPurpose
    ) -> YtdlpArgs:
        """Apply YouTube-specific CLI options to yt-dlp arguments.

        Args:
            args: YtdlpArgs object to modify with source-specific options.
            purpose: The purpose of the fetch operation.

        Returns:
            Modified YtdlpArgs object with source-specific options applied.
        """
        # Convert thumbnails to JPG format for podcast compatibility
        args.convert_thumbnails("jpg")
        return args

    def extract_feed_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        ref_type: ReferenceType,
        source_url: str,
        fetch_until_date: datetime | None = None,
    ) -> Feed:
        """Extract feed-level metadata from yt-dlp response.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            ref_type: The type of reference being parsed.
            source_url: The original source URL for this feed.
            fetch_until_date: The upper bound date for the fetch operation, used for setting last_successful_sync. Optional.

        Returns:
            Feed object with extracted metadata populated.
        """
        logger.debug(
            "Extracting feed metadata.",
            extra={
                "feed_id": feed_id,
                "ref_type": ref_type,
            },
        )

        youtube_info = YoutubeEntry(ytdlp_info, feed_id)

        # Map ReferenceType to SourceType
        match ref_type:
            case ReferenceType.SINGLE:
                source_type = SourceType.SINGLE_VIDEO
            case ReferenceType.CHANNEL:
                source_type = SourceType.CHANNEL
            case ReferenceType.COLLECTION:
                source_type = SourceType.PLAYLIST
            case (
                ReferenceType.UNKNOWN_RESOLVED_URL | ReferenceType.UNKNOWN_DIRECT_FETCH
            ):
                source_type = SourceType.UNKNOWN

        # Extract metadata with fallbacks
        author = youtube_info.uploader or youtube_info.channel
        title = youtube_info.title
        description = youtube_info.description
        image_url = youtube_info.thumbnail

        # Use fetch_until_date if provided, otherwise use current time
        last_successful_sync = (
            fetch_until_date if fetch_until_date else datetime.now(UTC)
        )

        feed = Feed(
            id=feed_id,
            is_enabled=True,
            source_type=source_type,
            source_url=source_url,
            last_successful_sync=last_successful_sync,
            title=title,
            subtitle=None,  # Not available from yt-dlp
            description=description,
            language=None,  # Not available from yt-dlp
            author=author,
            image_url=image_url,
        )

        logger.debug(
            "Successfully extracted feed metadata.",
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

    def _parse_single_video_entry(self, entry: YoutubeEntry, feed_id: str) -> Download:
        # if a single video is requested, but the match filter excludes it,
        # yt-dlp will return a partial set of data that excludes the fields
        # on how to download the video. Check for that here
        if not entry.ext and not entry.original_url and not entry.format_id:
            raise YtdlpYoutubeVideoFilteredOutError(feed_id, entry.download_id)

        source_url = (
            entry.webpage_url
            or entry.original_url
            or f"https://www.youtube.com/watch?v={entry.download_id}"
        )

        published_dt = entry.timestamp or entry.upload_date or entry.release_timestamp
        if published_dt is None:
            raise YtdlpYoutubeDataError(
                "Missing published datetime.",
                feed_id=feed_id,
                download_id=entry.download_id,
            )

        logger.debug(
            "Determined published datetime.",
            extra={
                "video_id": entry.download_id,
                "published_dt": published_dt.isoformat(),
                "source_field": entry.published_source_field,
            },
        )
        # Determine status: upcoming if live or scheduled, else queued
        status = (
            DownloadStatus.UPCOMING
            if entry.is_live or entry.live_status == "is_upcoming"
            else DownloadStatus.QUEUED
        )

        if status == DownloadStatus.UPCOMING:
            # For live/upcoming entries, these values are not yet available
            ext = "live"
            duration = 0
            mime_type = "application/octet-stream"
            logger.debug(
                "Entry is upcoming/live, setting default extension and duration.",
                extra={
                    "video_id": entry.download_id,
                    "extension": ext,
                    "duration": duration,
                    "mime_type": mime_type,
                },
            )
        else:
            if not entry.ext:
                raise YtdlpYoutubeDataError(
                    "Missing extension.",
                    feed_id=feed_id,
                    download_id=entry.download_id,
                )
            ext = entry.ext
            duration = entry.duration
            mime_type = entry.mime_type

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
            status=status,
            thumbnail=entry.thumbnail,
            description=entry.description,
            quality_info=entry.quality_info,
        )
        logger.debug(
            "Successfully parsed single video entry.",
            extra={
                "download_id": entry.download_id,
                "title": entry.title,
                "feed_id": feed_id,
            },
        )
        return parsed_download

    async def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        cookies_path: Path | None = None,
    ) -> tuple[str | None, ReferenceType]:
        """Determine the fetch strategy for a YouTube URL.

        Analyzes the URL to determine if it represents a single video,
        a collection (playlist/channel), or requires special handling.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to analyze.
            cookies_path: Path to cookies.txt file for authentication, or None if not needed.

        Returns:
            Tuple of (final_url_to_fetch, reference_type).

        Raises:
            YtdlpYoutubeDataError: If the URL is a playlists tab or other unsupported format.
        """
        logger.debug(
            "Determining fetch strategy for URL.", extra={"initial_url": initial_url}
        )

        # Build discovery args directly
        discovery_args = YtdlpArgs([])
        # Apply source-specific discovery options
        discovery_args = self.set_source_specific_ytdlp_options(
            discovery_args, FetchPurpose.DISCOVERY
        )
        # Apply standard discovery options
        discovery_args.quiet().no_warnings().skip_download().flat_playlist()

        # Add cookies if provided
        if cookies_path is not None:
            discovery_args.cookies(cookies_path)

        logger.debug(
            "Performing discovery call.",
            extra={"initial_url": initial_url},
        )
        discovery_info = await YtdlpCore.extract_info(discovery_args, initial_url)

        if not discovery_info:
            logger.warning(
                "Discovery call failed for URL.", extra={"initial_url": initial_url}
            )
            return initial_url, ReferenceType.UNKNOWN_DIRECT_FETCH

        youtube_info = YoutubeEntry(discovery_info, feed_id)

        fetch_url = youtube_info.webpage_url or initial_url
        discovery_type = youtube_info.type or "<unknown>"
        logger.debug(
            "Discovery call successful.",
            extra={
                "initial_url": initial_url,
                "fetch_url": fetch_url,
                "extractor": youtube_info.extractor,
                "resolved_type": discovery_type,
            },
        )

        # Handle single video
        if youtube_info.extractor == "youtube":
            logger.debug(
                "Resolved as single video.",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "reference_type": ReferenceType.SINGLE,
                    "extractor": youtube_info.extractor,
                },
            )
            return fetch_url, ReferenceType.SINGLE
        # Handle channels
        # Heuristic to identify a "main channel page" (e.g., @handle, /channel/UC...)
        # In this case, we delegate to the channel's Videos tab
        elif (
            # represents basically any list of videos in youtube
            youtube_info.extractor == "youtube:tab"
            and youtube_info.type == "playlist"
            and youtube_info.entries is not None
            # Check if this is actually a main channel page by ensuring it's not already a specific tab
            and not (
                youtube_info.webpage_url
                and any(
                    youtube_info.webpage_url.rstrip("/").endswith(tab)
                    for tab in [
                        "/videos",
                        "/shorts",
                        "/streams",
                        "/playlists",
                        "/community",
                        "/channels",
                        "/about",
                    ]
                )
            )
            and (
                # maybe only the case if a channel is brand new and has no videos yet
                not youtube_info.entries
                # it's a channel if all the entries underneath it are also playlists (representing tabs)
                or all(e and e.type == "playlist" for e in youtube_info.entries)
            )
        ):
            logger.debug(
                "URL identified as a main channel page. Searching for 'Videos' tab.",
                extra={
                    "initial_url": initial_url,
                    "num_potential_tabs": len(youtube_info.entries),
                },
            )
            for entry in youtube_info.entries:
                if entry and entry.type == "playlist":
                    tab_url = entry.webpage_url
                    if tab_url and tab_url.rstrip("/").endswith("/videos"):
                        logger.debug(
                            "Found 'Videos' tab for channel.",
                            extra={
                                "initial_url": initial_url,
                                "videos_tab_url": tab_url,
                                "reference_type": ReferenceType.CHANNEL,
                                "extractor": youtube_info.extractor,
                                "type": youtube_info.type,
                            },
                        )
                        return tab_url, ReferenceType.CHANNEL

            logger.warning(
                "'Videos' tab not found for main channel page. Using resolved URL as channel.",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "reference_type": ReferenceType.CHANNEL,
                    "extractor": youtube_info.extractor,
                    "type": youtube_info.type,
                },
            )
            return fetch_url, ReferenceType.CHANNEL
        # Handle the Playlist tabs
        # Playlist tabs will end up creating a "playlist" of playlists
        # I don't know what to do with that, so just throw
        elif (
            youtube_info.extractor == "youtube:tab"
            and youtube_info.webpage_url
            and youtube_info.webpage_url.rstrip("/").endswith("/playlists")
        ):
            raise YtdlpYoutubeDataError(
                f"Link is a playlists tab, not a specific playlist. Pick a specific list. URL: {initial_url}",
                download_id=initial_url,
            )
        # Handle playlists and any other channel tabs
        elif youtube_info.extractor == "youtube:tab":
            logger.debug(
                "URL is a content collection (e.g., playlist or specific channel tab).",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "extractor": youtube_info.extractor,
                    "type": youtube_info.type,
                    "reference_type": ReferenceType.COLLECTION,
                },
            )
            return fetch_url, ReferenceType.COLLECTION

        logger.warning(
            "Unhandled URL classification by extractor. Defaulting to unknown resolved URL.",
            extra={
                "initial_url": initial_url,
                "fetch_url": fetch_url,
                "extractor": youtube_info.extractor,
                "type": youtube_info.type,
                "reference_type": ReferenceType.UNKNOWN_RESOLVED_URL,
            },
        )
        return fetch_url, ReferenceType.UNKNOWN_RESOLVED_URL

    def parse_metadata_to_downloads(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_identifier: str,
        ref_type: ReferenceType,
    ) -> list[Download]:
        """Parse yt-dlp metadata into Download objects.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            source_identifier: Identifier for the source being parsed.
            ref_type: The type of reference being parsed.

        Returns:
            List of successfully parsed Download objects.
        """
        logger.debug(
            "Parsing metadata to downloads.",
            extra={
                "source_identifier": source_identifier,
                "ref_type": ref_type,
            },
        )
        youtube_info = YoutubeEntry(ytdlp_info, feed_id)
        downloads: list[Download] = []
        if ref_type == ReferenceType.SINGLE:
            logger.debug(
                "Parsing as single download.",
                extra={"source_identifier": source_identifier},
            )
            try:
                downloads.append(
                    self._parse_single_video_entry(youtube_info, source_identifier)
                )
            except YtdlpYoutubeDataError as e:
                logger.error(
                    "Failed to parse single download. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )
            except YtdlpYoutubeVideoFilteredOutError as e:
                logger.debug(
                    "Download filtered out by yt-dlp. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )
        elif ref_type in (ReferenceType.COLLECTION, ReferenceType.CHANNEL):
            logger.debug(
                "Parsing as collection.",
                extra={
                    "source_identifier": source_identifier,
                    "num_entries_found": len(youtube_info.entries)
                    if youtube_info.entries
                    else "<not present>",
                },
            )

            if youtube_info.entries:
                for i, entry in enumerate(youtube_info.entries):
                    if entry is None:
                        logger.warning(
                            "Entry in collection returned nothing. Skipping.",
                            extra={
                                "source_identifier": source_identifier,
                                "entry_index": i,
                            },
                        )
                        continue
                    try:
                        parsed_download = self._parse_single_video_entry(
                            entry,
                            source_identifier,
                        )
                        downloads.append(parsed_download)
                    except YtdlpYoutubeDataError as e:
                        logger.error(
                            "Failed to parse video entry in collection. Skipping download.",
                            exc_info=e,
                            extra={
                                "source_identifier": source_identifier,
                                "download_id": entry.download_id,
                            },
                        )
                    except YtdlpYoutubeVideoFilteredOutError as e:
                        logger.debug(
                            "Video in collection filtered out by yt-dlp. Skipping download.",
                            exc_info=e,
                            extra={
                                "source_identifier": source_identifier,
                                "download_id": entry.download_id,
                            },
                        )
            else:
                logger.warning(
                    "Expected collection but no 'entries' list found in info_dict.",
                    extra={
                        "source_identifier": source_identifier,
                        "ref_type": ref_type,
                    },
                )
        else:  # UNKNOWN_RESOLVED_URL or UNKNOWN_DIRECT_FETCH
            logger.warning(
                "Parsing with unknown reference type, attempting as single download.",
                extra={
                    "source_identifier": source_identifier,
                    "ref_type": ref_type,
                },
            )
            try:
                downloads.append(
                    self._parse_single_video_entry(youtube_info, source_identifier)
                )
            except YtdlpYoutubeDataError as e:
                logger.error(
                    "Failed to parse entry of unknown type. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )
            except YtdlpYoutubeVideoFilteredOutError as e:
                logger.debug(
                    "Video of unknown type filtered out by yt-dlp. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )

        logger.debug(
            "Finished parsing metadata.",
            extra={
                "source_identifier": source_identifier,
                "ref_type": ref_type,
                "downloads_identified": len(downloads),
            },
        )

        return downloads
