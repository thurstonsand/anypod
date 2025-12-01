"""X/Twitter-specific handler for yt-dlp operations.

This module provides X/Twitter-specific implementations for fetch strategy
determination and metadata parsing. Twitter URLs are always single videos
(posts) with no playlist support.
"""

from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
import logging

from ...db.types import Download, DownloadStatus, Feed, SourceType, TranscriptSource
from ...exceptions import (
    YtdlpDataError,
    YtdlpDownloadFilteredOutError,
    YtdlpFieldInvalidError,
    YtdlpFieldMissingError,
)
from ...mimetypes import mimetypes
from ..core import YtdlpArgs, YtdlpCore, YtdlpInfo

logger = logging.getLogger(__name__)


class YtdlpTwitterDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for X/Twitter.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(
        self, message: str, feed_id: str | None = None, download_id: str | None = None
    ) -> None:
        super().__init__(f"Twitter Parser: {message}")
        self.feed_id = feed_id
        self.download_id = download_id


class YtdlpTwitterPostFilteredOutError(YtdlpDownloadFilteredOutError):
    """Raised when a Twitter post is filtered out by yt-dlp.

    Attributes:
        feed_id: The feed identifier associated with the error.
        download_id: The download identifier associated with the error.
    """

    def __init__(self, feed_id: str | None = None, download_id: str | None = None):
        super().__init__(
            "Twitter: Post filtered out by yt-dlp.",
            feed_id=feed_id,
            download_id=download_id,
        )


class TwitterEntry:
    """Represent a single Twitter video entry with field extraction.

    Provides typed access to Twitter-specific fields from yt-dlp metadata,
    with error handling and data validation for common Twitter video properties.

    Attributes:
        feed_id: The feed identifier this entry belongs to.
        download_id: The Twitter post ID.
        _ytdlp_info: The underlying yt-dlp metadata.
    """

    def __init__(self, ytdlp_info: YtdlpInfo, feed_id: str):
        self._ytdlp_info = ytdlp_info
        self.feed_id = feed_id

        try:
            self.download_id = self._ytdlp_info.required("id", str)
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpTwitterDataError(
                message="Failed to parse Twitter entry.",
                feed_id=self.feed_id,
                download_id="<missing_id>",
            ) from e

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, TwitterEntry):
            return NotImplemented
        return self.feed_id == other.feed_id and self._ytdlp_info == other._ytdlp_info

    @contextmanager
    def _annotate_exceptions(self) -> Generator[None]:
        try:
            yield
        except (YtdlpFieldMissingError, YtdlpFieldInvalidError) as e:
            raise YtdlpTwitterDataError(
                message="Failed to parse Twitter entry.",
                feed_id=self.feed_id,
                download_id=self.download_id,
            ) from e

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

    @property
    def title(self) -> str:
        """Get the post title."""
        with self._annotate_exceptions():
            return self._ytdlp_info.required("title", str)

    @property
    def description(self) -> str | None:
        """Get the description for the post."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("description", str)

    @property
    def uploader(self) -> str | None:
        """Get the uploader name (Twitter handle display name)."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("uploader", str)

    @property
    def uploader_id(self) -> str | None:
        """Get the uploader handle used in public status URLs."""
        with self._annotate_exceptions():
            return self._ytdlp_info.get("uploader_id", str)

    @property
    def thumbnail(self) -> str | None:
        """Get the best quality JPG or PNG thumbnail URL for the post."""
        with self._annotate_exceptions():
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
    def ext(self) -> str:
        """Get the file extension for the post's media."""
        with self._annotate_exceptions():
            return self._ytdlp_info.required("ext", str)

    @property
    def mime_type(self) -> str:
        """Get the MIME type for the video."""
        guessed = mimetypes.guess_type(f"file.{self.ext}")[0]
        if guessed:
            return guessed
        raise YtdlpTwitterDataError(
            message="Missing MIME type.",
            feed_id=self.feed_id,
            download_id=self.download_id,
        )

    @property
    def filesize(self) -> int:
        """Get the file size using filesize_approx as fallback.

        Raises:
            YtdlpTwitterDataError: If filesize is missing or invalid (≤0).
        """
        with self._annotate_exceptions():
            raw_filesize = self._ytdlp_info.get(
                "filesize", (int, float)
            ) or self._ytdlp_info.get("filesize_approx", (int, float))
            if raw_filesize is None:
                logger.warning(
                    "Twitter metadata missing filesize; using placeholder.",
                    extra={
                        "feed_id": self.feed_id,
                        "download_id": self.download_id,
                    },
                )
                return 1

            normalized_size = int(raw_filesize)
            if normalized_size <= 0:
                raise YtdlpTwitterDataError(
                    f"Invalid filesize: {raw_filesize}.",
                    feed_id=self.feed_id,
                    download_id=self.download_id,
                )
            return normalized_size

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
            raise YtdlpTwitterDataError(
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
            raise YtdlpTwitterDataError(
                message=f"Invalid upload date: '{upload_date_str}'.",
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
        else:
            return "unknown"

    @property
    def duration(self) -> int:
        """Get the video duration in seconds as an int."""
        with self._annotate_exceptions():
            raw_duration = self._ytdlp_info.get_raw("duration")

            match raw_duration:
                case bool():  # special case for int vs bool type confusion
                    raise YtdlpTwitterDataError(
                        f"Duration had unexpected type: '({type(raw_duration)}){raw_duration}'.",
                        feed_id=self.feed_id,
                        download_id=self.download_id,
                    )
                case float() as duration:
                    normalized_duration = int(duration)
                case int() as duration:
                    normalized_duration = duration
                case str() as duration_str:
                    try:
                        normalized_duration = int(float(duration_str))
                    except ValueError as e:
                        raise YtdlpTwitterDataError(
                            f"Unparsable duration '{duration_str}'.",
                            feed_id=self.feed_id,
                            download_id=self.download_id,
                        ) from e
                case None:
                    logger.warning(
                        "Twitter metadata missing duration; using placeholder.",
                        extra={
                            "feed_id": self.feed_id,
                            "download_id": self.download_id,
                        },
                    )
                    return 1
                case _:
                    logger.warning(
                        "Twitter metadata returned unsupported duration type; using placeholder.",
                        extra={
                            "feed_id": self.feed_id,
                            "download_id": self.download_id,
                            "raw_type": type(raw_duration).__name__,
                        },
                    )
                    return 1

        if normalized_duration <= 0:
            logger.warning(
                "Twitter metadata provided non-positive duration; using placeholder.",
                extra={
                    "feed_id": self.feed_id,
                    "download_id": self.download_id,
                    "raw_duration": raw_duration,
                },
            )
            return 1

        return normalized_duration

    @property
    def quality_info(self) -> str | None:
        """Get quality information for the video as a formatted string.

        Extracts resolution information from requested_downloads for Twitter posts.
        """
        with self._annotate_exceptions():
            # Try to get resolution from requested_downloads first
            rd_list = self._ytdlp_info.get(
                "requested_downloads", list[dict[str, object]]
            )
            if rd_list and len(rd_list) > 0:
                first_rd = rd_list[0]
                height = first_rd.get("height")
                if isinstance(height, int) and height > 0:
                    return f"{height}p"

            # Fallback to top-level resolution
            if resolution := self._ytdlp_info.get("resolution", str):
                return resolution
            elif height := self._ytdlp_info.get("height", int):
                return f"{height}p"

            return None


class TwitterHandler:
    """X/Twitter-specific implementation for fetching strategy and parsing.

    Implements the SourceHandlerBase protocol to provide Twitter-specific
    behavior for URL classification and metadata parsing into Download
    objects. Twitter URLs are always single video posts.
    """

    async def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        base_args: YtdlpArgs,
    ) -> tuple[str | None, SourceType]:
        """Determine the fetch strategy for a Twitter URL.

        Args:
            feed_id: The feed identifier.
            initial_url: The initial URL to analyze.
            base_args: Pre-configured YtdlpArgs with shared settings.

        Returns:
            Tuple of (final_url_to_fetch, source_type).
        """
        log_params = {"feed_id": feed_id, "initial_url": initial_url}
        logger.debug("Determining Twitter fetch strategy.", extra=log_params)

        # Add Twitter-specific discovery options
        discovery_args = base_args.skip_download().flat_playlist()

        logger.debug("Performing discovery call.", extra=log_params)
        discovery_result = await YtdlpCore.extract_playlist_info(
            discovery_args, initial_url
        )
        discovery_logs = discovery_result.logs
        if discovery_logs:
            log_params["ytdlp_logs"] = discovery_logs
            logger.debug("yt-dlp Twitter discovery logs.", extra=log_params)

        discovery_info = discovery_result.payload
        twitter_info = TwitterEntry(discovery_info, feed_id)

        fetch_url = twitter_info.webpage_url or initial_url
        discovery_type = twitter_info.type or "<unknown>"
        log_params = {
            **log_params,
            "fetch_url": fetch_url,
            "extractor": twitter_info.extractor,
            "resolved_type": discovery_type,
        }
        logger.debug("Twitter discovery call successful.", extra=log_params)

        # Twitter URLs are always single videos (status posts)
        if twitter_info.extractor == "twitter":
            logger.debug(
                "Resolved as single Twitter video.",
                extra={**log_params, "source_type": SourceType.SINGLE_VIDEO},
            )
            return fetch_url, SourceType.SINGLE_VIDEO

        logger.warning(
            "Unhandled Twitter URL classification. Defaulting to UNKNOWN.",
            extra={
                **log_params,
                "type": twitter_info.type,
                "source_type": SourceType.UNKNOWN,
            },
        )
        return fetch_url, SourceType.UNKNOWN

    def prepare_playlist_info_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Return args unchanged for Twitter playlist metadata calls."""
        return args

    def extract_feed_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        source_type: SourceType,
        source_url: str,
    ) -> Feed:
        """Extract feed-level metadata from Twitter yt-dlp response.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata information.
            source_type: The type of source being parsed.
            source_url: The original source URL for this feed.

        Returns:
            Feed object with extracted metadata populated.
        """
        logger.debug(
            "Extracting Twitter feed metadata.",
            extra={"feed_id": feed_id, "source_type": source_type},
        )

        twitter_info = TwitterEntry(ytdlp_info, feed_id)

        author = twitter_info.uploader
        title = twitter_info.title
        description = twitter_info.description
        image_url = twitter_info.thumbnail
        last_successful_sync = twitter_info.epoch

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
            "Successfully extracted Twitter feed metadata.",
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
        """Return args unchanged for Twitter thumbnail downloads."""
        return args

    def prepare_downloads_info_args(
        self,
        args: YtdlpArgs,
    ) -> YtdlpArgs:
        """Return args unchanged for Twitter downloads metadata calls."""
        return args

    async def extract_download_metadata(
        self,
        feed_id: str,
        ytdlp_info: YtdlpInfo,
        transcript_lang: str | None = None,
        transcript_source_priority: list[TranscriptSource] | None = None,
    ) -> Download:
        """Extract metadata from a single Twitter post into a Download object.

        Args:
            feed_id: The feed identifier.
            ytdlp_info: The yt-dlp metadata for a single post.
            transcript_lang: Language code for transcripts (e.g., "en"). If provided,
                determines transcript_source from yt-dlp subtitle metadata.
            transcript_source_priority: Ordered list of transcript sources to try.
                Defaults to [CREATOR, AUTO] if not provided.

        Returns:
            A Download object parsed from the metadata.

        Raises:
            YtdlpTwitterDataError: If required metadata is missing.
            YtdlpTwitterPostFilteredOutError: If the post was filtered out.
        """
        log_config = {"feed_id": feed_id}
        logger.debug("Extracting Twitter post metadata.", extra=log_config)

        entry = TwitterEntry(ytdlp_info, feed_id)

        source_url = entry.webpage_url or entry.original_url
        match source_url, entry.uploader_id:
            case None, None:
                raise YtdlpTwitterDataError(
                    "Missing source URL.",
                    feed_id=feed_id,
                    download_id=entry.download_id,
                )
            case None, uploader_id:
                source_url = f"https://x.com/{uploader_id}/status/{entry.download_id}"
            case _, _:
                pass

        published_dt = entry.timestamp or entry.upload_date
        if published_dt is None:
            raise YtdlpTwitterDataError(
                "Missing published datetime.",
                feed_id=feed_id,
                download_id=entry.download_id,
            )

        logger.debug(
            "Determined published datetime.",
            extra={
                **log_config,
                "post_id": entry.download_id,
                "published_dt": published_dt.isoformat(),
                "source_field": entry.published_source_field,
            },
        )

        # Twitter posts are always queued (no live/upcoming concept)
        status = DownloadStatus.QUEUED

        ext = entry.ext
        duration = entry.duration
        mime_type = entry.mime_type

        transcript = (
            ytdlp_info.transcript(transcript_lang, transcript_source_priority)
            if transcript_lang and transcript_source_priority
            else None
        )
        transcript_source = (
            transcript.source if transcript else TranscriptSource.NOT_AVAILABLE
        )

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
            remote_thumbnail_url=entry.thumbnail,
            description=entry.description,
            quality_info=entry.quality_info,
            transcript_source=transcript_source,
        )

        logger.debug(
            "Successfully parsed Twitter post.",
            extra={
                **log_config,
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
        """Return args unchanged for Twitter media downloads.

        Args:
            args: Base YtdlpArgs to modify.
            download: The Download object being processed.

        Returns:
            Unmodified YtdlpArgs.
        """
        return args
