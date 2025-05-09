from datetime import UTC, datetime
import logging
from typing import Any

from ..db import Download, DownloadStatus
from ..exceptions import YtdlpDataError
from .base_handler import FetchPurpose, ReferenceType, YdlApiCaller

logger = logging.getLogger(__name__)


class YtdlpYoutubeDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for YouTube."""

    def __init__(self, feed_context: str, entry_id: str, message: str):
        self.feed_context = feed_context
        self.entry_id = entry_id
        super().__init__(
            f"YouTube Parser: Skipping entry {entry_id} in feed '{feed_context}': {message}"
        )


class YtdlpYoutubeVideoFilteredOutError(YtdlpDataError):
    """Raised when a video is filtered out by yt-dlp."""

    def __init__(self, feed_context: str, entry_id: str):
        self.feed_context = feed_context
        self.entry_id = entry_id
        super().__init__(
            f"YouTube: Video filtered out. Skipping entry {entry_id} in feed '{feed_context}'"
        )


class YoutubeHandler:
    """
    YouTube-specific implementation for fetching strategy and parsing.
    Implements the SourceHandlerBase protocol.
    """

    def get_source_specific_ydl_options(self, purpose: FetchPurpose) -> dict[str, Any]:
        # No filtering at discovery or metadata needed
        return {}

    def _parse_single_video_entry(
        self, entry: dict[str, Any], feed_context: str
    ) -> Download:
        video_id = entry.get("id")
        if not video_id:
            raise YtdlpYoutubeDataError(
                feed_context,
                "<missing_id>",
                f"Missing video ID. Data: {str(entry)[:200]}",
            )

        # if a single video is requested, but the match filter excludes it,
        # yt-dlp will return a partial set of data that excludes the fields
        # on how to download the video. Check for that here
        if not entry.get("ext") and not entry.get("url") and not entry.get("format_id"):
            raise YtdlpYoutubeVideoFilteredOutError(feed_context, video_id)

        source_url = (
            entry.get("webpage_url")
            or entry.get("original_url")
            or f"https://www.youtube.com/watch?v={video_id}"
        )

        title = entry.get("title")
        if not title or title in ("[Deleted video]", "[Private video]"):
            raise YtdlpYoutubeDataError(
                feed_context,
                str(video_id),
                f"Video unavailable or deleted (title: '{title}')",
            )

        if (ts_val := entry.get("timestamp")) is not None:
            try:
                published_dt = datetime.fromtimestamp(float(ts_val), UTC)
            except (TypeError, ValueError, OSError) as e:
                raise YtdlpYoutubeDataError(
                    feed_context, str(video_id), f"Invalid 'timestamp' {ts_val}"
                ) from e
        elif (upload_date_str := entry.get("upload_date")) is not None:
            try:
                published_dt = datetime.strptime(
                    str(upload_date_str), "%Y%m%d"
                ).replace(tzinfo=UTC)
            except (TypeError, ValueError) as e:
                raise YtdlpYoutubeDataError(
                    feed_context,
                    str(video_id),
                    f"Invalid 'upload_date' {upload_date_str}",
                ) from e
        elif (release_ts_val := entry.get("release_timestamp")) is not None:
            try:
                published_dt = datetime.fromtimestamp(float(release_ts_val), UTC)
            except (TypeError, ValueError, OSError) as e:
                raise YtdlpYoutubeDataError(
                    feed_context,
                    str(video_id),
                    f"Invalid 'release_timestamp' {release_ts_val}",
                ) from e
        else:
            raise YtdlpYoutubeDataError(
                feed_context, str(video_id), "Missing published datetime"
            )

        # Determine status: upcoming if live or scheduled, else queued
        status = (
            DownloadStatus.UPCOMING
            if entry.get("is_live") or entry.get("live_status") == "is_upcoming"
            else DownloadStatus.QUEUED
        )

        if status == DownloadStatus.UPCOMING:
            # For live/upcoming entries, these values are not yet available
            extension = "live"
            duration_float = 0
        else:
            extension = entry.get("ext")
            if not extension:
                raise YtdlpYoutubeDataError(
                    feed_context, str(video_id), "Missing extension"
                )
            duration_val = entry.get("duration")
            # Explicitly check for bool first, as bool is a subclass of int
            if isinstance(duration_val, bool):
                raise YtdlpYoutubeDataError(
                    feed_context,
                    str(video_id),
                    f"Duration had unexpected type: '({type(duration_val)}){duration_val}'",
                )
            elif isinstance(duration_val, int | float):
                duration_float = float(duration_val)
            elif isinstance(duration_val, str):
                try:
                    duration_float = float(duration_val)
                except ValueError as e:
                    raise YtdlpYoutubeDataError(
                        feed_context,
                        str(video_id),
                        f"Unparsable duration '{duration_val}'",
                    ) from e
            else:
                raise YtdlpYoutubeDataError(
                    feed_context,
                    str(video_id),
                    f"Duration had unexpected type: '({type(duration_val)}){duration_val}'",
                )

        thumbnail = entry.get("thumbnail")

        return Download(
            feed=str(feed_context),
            id=str(video_id),
            source_url=str(source_url),
            title=str(title),
            published=published_dt,
            ext=str(extension),
            duration=duration_float,
            status=status,
            thumbnail=str(thumbnail) if thumbnail else None,
        )

    def determine_fetch_strategy(
        self, initial_url: str, ydl_caller_for_discovery: YdlApiCaller
    ) -> tuple[str | None, ReferenceType]:
        logger.info(f"YouTube Strategy: Resolving URL type for: {initial_url}")
        # `extract_flat` will be set to "in_playlist" by _prepare_ydl_options for DISCOVERY purpose
        discovery_opts = {"playlist_items": "1-5"}
        discovery_info = ydl_caller_for_discovery(discovery_opts, initial_url)

        if not discovery_info:
            logger.warning(f"YouTube Strategy: Discovery failed for {initial_url}.")
            return initial_url, ReferenceType.UNKNOWN_DIRECT_FETCH

        fetch_url = discovery_info.get("webpage_url", initial_url)
        extractor = discovery_info.get("extractor", "").lower()

        # Handle single video
        if extractor == "youtube":
            logger.info(
                f"YouTube Strategy: Resolved {initial_url} as {ReferenceType.SINGLE}. URL: {fetch_url}"
            )
            return fetch_url, ReferenceType.SINGLE
        # Handle channels
        # Heuristic to identify a "main channel page" (e.g., @handle, /channel/UC...)
        # In this case, we delegate to the channel's Videos tab
        elif (
            # represents basically any list of videos in youtube
            extractor == "youtube:tab"
            # also represents basically any list of videos in youtube
            and discovery_info.get("_type") == "playlist"
            and isinstance(discovery_info.get("entries"), list)
            and (
                # maybe only the case if a channel is brand new and has no videos yet
                not discovery_info.get("entries")
                # it's a channel if all the entries underneath it are also playlists (representing tabs)
                or all(
                    isinstance(e, dict) and e.get("_type") == "playlist"
                    for e in discovery_info.get("entries")
                )
            )
        ):
            entries = discovery_info.get("entries")
            logger.info(
                f"YouTube Strategy: {initial_url} identified as a main channel page. Searching for 'Videos' tab."
            )
            for entry_data in entries:
                if isinstance(entry_data, dict):
                    tab_url = entry_data.get("webpage_url")
                    if tab_url and tab_url.rstrip("/").endswith("/videos"):
                        logger.info(
                            f"YouTube Strategy: Found 'Videos' tab for {initial_url}: {tab_url}"
                        )
                        return tab_url, ReferenceType.COLLECTION

            logger.warning(
                f"YouTube Strategy: 'Videos' tab not found for main channel page {initial_url}. Using resolved URL: {fetch_url} as {ReferenceType.COLLECTION}."
            )
            return fetch_url, ReferenceType.COLLECTION
        # Handle the Playlist tabs
        # Playlist tabs will end up creating a "playlist" of playlists
        # I don't know what to do with that, so just throw
        elif extractor == "youtube:tab" and discovery_info.get("webpage_url").rstrip(
            "/"
        ).endswith("/playlists"):
            raise YtdlpDataError(
                f"Youtube Parser: link is a playlists tab, not a specific playlist. Pick a specific list. URL: {initial_url}"
            )
        # Handle playlists and any other channel tabs
        elif extractor == "youtube:tab":
            logger.info(
                f"YouTube Strategy: {initial_url} (extractor: youtubetab) is a direct content collection "
                f"(e.g., playlist or specific channel tab). Fetching as {ReferenceType.COLLECTION}. URL: {fetch_url}"
            )
            return fetch_url, ReferenceType.COLLECTION

        logger.warning(
            f"YouTube Strategy: Unhandled classification for {initial_url} (extractor: {extractor}). Defaulting to {ReferenceType.UNKNOWN_RESOLVED_URL} with URL: {fetch_url}"
        )
        return fetch_url, ReferenceType.UNKNOWN_RESOLVED_URL

    def parse_metadata_to_downloads(
        self,
        info_dict: dict[str, Any],
        source_identifier: str,
        ref_type: ReferenceType,
    ) -> list[Download]:
        if not info_dict:
            return []

        if ref_type == ReferenceType.SINGLE:
            downloads: list[Download] = []
            try:
                downloads.append(
                    self._parse_single_video_entry(info_dict, source_identifier)
                )
            except YtdlpYoutubeDataError as e:
                logger.error(f"{e} Skipping Download...")
            except YtdlpYoutubeVideoFilteredOutError as e:
                logger.info(f"{e}")
            return downloads
        elif ref_type == ReferenceType.COLLECTION:
            entries = info_dict.get("entries")
            downloads: list[Download] = []
            if isinstance(entries, list):
                for entry_data in entries:
                    try:
                        parsed_download = self._parse_single_video_entry(
                            entry_data, source_identifier
                        )
                    except YtdlpYoutubeDataError as e:
                        logger.error(f"{e} Skipping Download...")
                    else:
                        downloads.append(parsed_download)
            else:
                logger.warning(
                    f"YouTube Parser: Expected {ReferenceType.COLLECTION} for '{source_identifier}' but no 'entries' list found."
                )
            return downloads
        else:  # UNKNOWN_RESOLVED_URL or UNKNOWN_DIRECT_FETCH
            logger.warning(
                f"YouTube Parser: Parsing UNKNOWN type for '{source_identifier}' as potential single item."
            )
            downloads: list[Download] = []
            try:
                downloads.append(
                    self._parse_single_video_entry(info_dict, source_identifier)
                )
            except YtdlpYoutubeDataError as e:
                logger.error(f"{e}. Skipping Download...")
            except YtdlpYoutubeVideoFilteredOutError as e:
                # video was actually filtered out, so skip it
                logger.info(f"{e}")
            return downloads
