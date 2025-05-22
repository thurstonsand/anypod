from collections.abc import Generator
from contextlib import contextmanager
from datetime import UTC, datetime
import logging
from typing import Any

from ..db import Download, DownloadStatus
from ..exceptions import (
    YtdlpDataError,
    YtdlpFieldInvalidError,
    YtdlpFieldMissingError,
)
from .base_handler import FetchPurpose, ReferenceType, YdlApiCaller
from .ytdlp_core import YtdlpInfo

logger = logging.getLogger(__name__)


class YtdlpYoutubeDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for YouTube."""

    def __init__(
        self, message: str, feed_id: str | None = None, download_id: str | None = None
    ):
        super().__init__(f"YouTube Parser: {message}")
        self.feed_id = feed_id
        self.download_id = download_id


class YtdlpYoutubeVideoFilteredOutError(YtdlpDataError):
    """Raised when a video is filtered out by yt-dlp."""

    def __init__(self, feed_id: str | None = None, download_id: str | None = None):
        self.feed_id = feed_id
        self.download_id = download_id
        super().__init__("YouTube: Video filtered out by yt-dlp.")


class YoutubeEntry:
    """Represents a single YouTube video entry."""

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
        with self._annotate_exceptions():
            return self._ytdlp_info.get("webpage_url", str)

    @property
    def extractor(self) -> str | None:
        with self._annotate_exceptions():
            extractor = self._ytdlp_info.get("extractor", str)
            return extractor.lower() if extractor else None

    @property
    def type(self) -> str | None:
        with self._annotate_exceptions():
            return self._ytdlp_info.get("_type", str)

    # --- playlist fields ---

    @property
    def entries(self) -> list["YoutubeEntry | None"] | None:
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
        with self._annotate_exceptions():
            return self._ytdlp_info.get("format_id", str)

    @property
    def original_url(self) -> str | None:
        with self._annotate_exceptions():
            return self._ytdlp_info.get("original_url", str)

    @property
    def title(self) -> str:
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
        with self._annotate_exceptions():
            return self._ytdlp_info.get("ext", str)

    @property
    def timestamp(self) -> datetime | None:
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
        with self._annotate_exceptions():
            return self._ytdlp_info.get("is_live", bool)

    @property
    def live_status(self) -> str | None:
        with self._annotate_exceptions():
            return self._ytdlp_info.get("live_status", str)

    @property
    def duration(self) -> float:
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
                    return float(duration)
                case str() as duration_str:
                    try:
                        return float(duration_str)
                    except ValueError as e:
                        raise YtdlpYoutubeDataError(
                            f"Unparsable duration '{duration_str}'.",
                            feed_id=self.feed_id,
                            download_id=self.download_id,
                        ) from e

    @property
    def thumbnail(self) -> str | None:
        with self._annotate_exceptions():
            return self._ytdlp_info.get("thumbnail", str)


class YoutubeHandler:
    """YouTube-specific implementation for fetching strategy and parsing.

    Implements the SourceHandlerBase protocol.
    """

    def get_source_specific_ydl_options(self, purpose: FetchPurpose) -> dict[str, Any]:
        logger.debug(
            "No source specific ydl options for Youtube.",
            extra={"purpose": purpose},
        )
        # No filtering at discovery, metadata fetch, or media download needed from here
        return {}

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
            logger.debug(
                "Entry is upcoming/live, setting default extension and duration.",
                extra={
                    "video_id": entry.download_id,
                    "extension": ext,
                    "duration": duration,
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

        parsed_download = Download(
            feed=feed_id,
            id=entry.download_id,
            source_url=source_url,
            title=entry.title,
            published=published_dt,
            ext=ext,
            duration=duration,
            status=status,
            thumbnail=entry.thumbnail,
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

    def determine_fetch_strategy(
        self,
        feed_id: str,
        initial_url: str,
        ydl_caller_for_discovery: YdlApiCaller,
    ) -> tuple[str | None, ReferenceType]:
        logger.info(
            "Determining fetch strategy for URL.", extra={"initial_url": initial_url}
        )
        # `extract_flat` will be set to "in_playlist" by _prepare_ydl_options for DISCOVERY purpose
        discovery_opts = {"playlist_items": "1-5"}  # Small number for speed
        logger.debug(
            "Performing discovery call.",
            extra={"initial_url": initial_url, "discovery_opts": discovery_opts},
        )
        discovery_info = ydl_caller_for_discovery(discovery_opts, initial_url)

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
            logger.info(
                "Resolved as single video.",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "reference_type": ReferenceType.SINGLE,
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
            and (
                # maybe only the case if a channel is brand new and has no videos yet
                not youtube_info.entries
                # it's a channel if all the entries underneath it are also playlists (representing tabs)
                or all(e and e.type == "playlist" for e in youtube_info.entries)
            )
        ):
            logger.info(
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
                        logger.info(
                            "Found 'Videos' tab for channel.",
                            extra={
                                "initial_url": initial_url,
                                "videos_tab_url": tab_url,
                                "reference_type": ReferenceType.COLLECTION,
                            },
                        )
                        return tab_url, ReferenceType.COLLECTION

            logger.warning(
                "'Videos' tab not found for main channel page. Using resolved URL as collection.",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "reference_type": ReferenceType.COLLECTION,
                },
            )
            return fetch_url, ReferenceType.COLLECTION
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
            logger.info(
                "URL is a content collection (e.g., playlist or specific channel tab).",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "extractor": youtube_info.extractor,
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
                logger.info(
                    "Download filtered out by yt-dlp. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )
        elif ref_type == ReferenceType.COLLECTION:
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
                        logger.info(
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
                logger.info(
                    "Video of unknown type filtered out by yt-dlp. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )

        logger.info(
            "Finished parsing metadata.",
            extra={
                "source_identifier": source_identifier,
                "ref_type": ref_type,
                "downloads_identified": len(downloads),
            },
        )

        return downloads
