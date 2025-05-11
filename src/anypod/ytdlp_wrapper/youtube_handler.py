from datetime import UTC, datetime
import logging
from typing import Any

from ..db import Download, DownloadStatus
from ..exceptions import YtdlpDataError
from .base_handler import FetchPurpose, ReferenceType, YdlApiCaller

logger = logging.getLogger(__name__)


class YtdlpYoutubeDataError(YtdlpDataError):
    """Raised when yt-dlp data extraction fails for YouTube."""

    def __init__(self, feed_name: str, entry_id: str, message: str):
        self.feed_name = feed_name
        self.entry_id = entry_id
        super().__init__(f"YouTube Parser: {message}")


class YtdlpYoutubeVideoFilteredOutError(YtdlpDataError):
    """Raised when a video is filtered out by yt-dlp."""

    def __init__(self, feed_name: str, entry_id: str):
        self.feed_name = feed_name
        self.entry_id = entry_id
        super().__init__("YouTube: Video filtered out by yt-dlp.")


class YoutubeHandler:
    """
    YouTube-specific implementation for fetching strategy and parsing.
    Implements the SourceHandlerBase protocol.
    """

    def get_source_specific_ydl_options(self, purpose: FetchPurpose) -> dict[str, Any]:
        logger.debug(
            "No source specific ydl options for Youtube.",
            extra={"purpose": str(purpose)},
        )
        # No filtering at discovery or metadata needed
        return {}

    def _parse_single_video_entry(
        self, entry: dict[str, Any], feed_name: str
    ) -> Download:
        video_id = entry.get("id")
        logger.debug(
            "Parsing single video entry.",
            extra={"video_id": video_id, "feed_name": feed_name},
        )

        if not video_id:
            raise YtdlpYoutubeDataError(
                feed_name,
                "<missing_id>",
                f"Missing video ID. Data: {str(entry)[:200]}",
            )
        logger.debug("Video ID found.", extra={"video_id": video_id})

        # if a single video is requested, but the match filter excludes it,
        # yt-dlp will return a partial set of data that excludes the fields
        # on how to download the video. Check for that here
        if not entry.get("ext") and not entry.get("url") and not entry.get("format_id"):
            raise YtdlpYoutubeVideoFilteredOutError(feed_name, video_id)

        source_url = (
            entry.get("webpage_url")
            or entry.get("original_url")
            or f"https://www.youtube.com/watch?v={video_id}"
        )
        logger.debug(
            "Determined source URL.",
            extra={"video_id": video_id, "source_url": source_url},
        )

        title = entry.get("title")
        if not title or title in ("[Deleted video]", "[Private video]"):
            raise YtdlpYoutubeDataError(
                feed_name,
                str(video_id),
                f"Video unavailable or deleted (title: '{title}')",
            )
        logger.debug("Title found.", extra={"video_id": video_id, "title": title})

        if (ts_val := entry.get("timestamp")) is not None:
            try:
                published_dt = datetime.fromtimestamp(float(ts_val), UTC)
                publish_source_field = "timestamp"
            except (TypeError, ValueError, OSError) as e:
                raise YtdlpYoutubeDataError(
                    feed_name, str(video_id), f"Invalid 'timestamp' {ts_val}"
                ) from e
        elif (upload_date_str := entry.get("upload_date")) is not None:
            try:
                published_dt = datetime.strptime(
                    str(upload_date_str), "%Y%m%d"
                ).replace(tzinfo=UTC)
                publish_source_field = "upload_date"
            except (TypeError, ValueError) as e:
                raise YtdlpYoutubeDataError(
                    feed_name,
                    str(video_id),
                    f"Invalid 'upload_date' {upload_date_str}",
                ) from e
        elif (release_ts_val := entry.get("release_timestamp")) is not None:
            try:
                published_dt = datetime.fromtimestamp(float(release_ts_val), UTC)
                publish_source_field = "release_timestamp"
            except (TypeError, ValueError, OSError) as e:
                raise YtdlpYoutubeDataError(
                    feed_name,
                    str(video_id),
                    f"Invalid 'release_timestamp' {release_ts_val}",
                ) from e
        else:
            raise YtdlpYoutubeDataError(
                feed_name, str(video_id), "Missing published datetime"
            )

        if published_dt and publish_source_field:
            logger.debug(
                "Determined published datetime.",
                extra={
                    "video_id": video_id,
                    "published_dt": published_dt.isoformat(),
                    "source_field": publish_source_field,
                },
            )
        # Determine status: upcoming if live or scheduled, else queued
        status = (
            DownloadStatus.UPCOMING
            if entry.get("is_live") or entry.get("live_status") == "is_upcoming"
            else DownloadStatus.QUEUED
        )
        logger.debug(
            "Determined download status.",
            extra={"video_id": video_id, "status": status},
        )

        if status == DownloadStatus.UPCOMING:
            # For live/upcoming entries, these values are not yet available
            extension = "live"
            duration_float = 0
            logger.debug(
                "Entry is upcoming/live, setting default extension and duration.",
                extra={
                    "video_id": video_id,
                    "extension": extension,
                    "duration": duration_float,
                },
            )
        else:
            extension = entry.get("ext")
            if not extension:
                raise YtdlpYoutubeDataError(
                    feed_name, str(video_id), "Missing extension"
                )
            duration_val = entry.get("duration")
            # Explicitly check for bool first, as bool is a subclass of int
            if isinstance(duration_val, bool):
                raise YtdlpYoutubeDataError(
                    feed_name,
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
                        feed_name,
                        str(video_id),
                        f"Unparsable duration '{duration_val}'",
                    ) from e
            else:
                raise YtdlpYoutubeDataError(
                    feed_name,
                    str(video_id),
                    f"Duration had unexpected type: '({type(duration_val)}){duration_val}'",
                )
            logger.debug(
                "Determined extension and duration for non-live entry.",
                extra={
                    "video_id": video_id,
                    "extension": extension,
                    "duration_val": duration_val,
                    "duration_float": duration_float,
                },
            )

        thumbnail = entry.get("thumbnail")
        if thumbnail:
            logger.debug(
                "Thumbnail found.",
                extra={"video_id": video_id, "thumbnail_url": thumbnail},
            )
        else:
            logger.debug("No thumbnail found.", extra={"video_id": video_id})

        parsed_download = Download(
            feed=str(feed_name),
            id=str(video_id),
            source_url=str(source_url),
            title=str(title),
            published=published_dt,
            ext=str(extension),
            duration=duration_float,
            status=status,
            thumbnail=str(thumbnail) if thumbnail else None,
        )
        logger.debug(
            "Successfully parsed single video entry.",
            extra={"video_id": video_id, "title": title, "feed_name": feed_name},
        )
        return parsed_download

    def determine_fetch_strategy(
        self,
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

        fetch_url = discovery_info.get("webpage_url", initial_url)
        extractor = discovery_info.get("extractor", "").lower()
        resolved_type_from_discovery = discovery_info.get("_type")
        logger.debug(
            "Discovery call successful.",
            extra={
                "initial_url": initial_url,
                "fetch_url": fetch_url,
                "extractor": extractor,
                "resolved_type": resolved_type_from_discovery,
            },
        )

        # Handle single video
        if extractor == "youtube":
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
            extractor == "youtube:tab"
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
            entries = discovery_info.get("entries", [])
            logger.info(
                "URL identified as a main channel page. Searching for 'Videos' tab.",
                extra={"initial_url": initial_url, "num_potential_tabs": len(entries)},
            )
            for entry_data in entries:
                if isinstance(entry_data, dict):
                    tab_url = entry_data.get("webpage_url")
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
        elif extractor == "youtube:tab" and discovery_info.get(
            "webpage_url", ""
        ).rstrip("/").endswith("/playlists"):
            raise YtdlpYoutubeDataError(
                f"Link is a playlists tab, not a specific playlist. Pick a specific list. URL: {initial_url}"
            )
        # Handle playlists and any other channel tabs
        elif extractor == "youtube:tab":
            logger.info(
                "URL is a content collection (e.g., playlist or specific channel tab).",
                extra={
                    "initial_url": initial_url,
                    "fetch_url": fetch_url,
                    "extractor": extractor,
                    "reference_type": ReferenceType.COLLECTION,
                },
            )
            return fetch_url, ReferenceType.COLLECTION

        logger.warning(
            "Unhandled URL classification by extractor. Defaulting to unknown resolved URL.",
            extra={
                "initial_url": initial_url,
                "fetch_url": fetch_url,
                "extractor": extractor,
                "reference_type": ReferenceType.UNKNOWN_RESOLVED_URL,
            },
        )
        return fetch_url, ReferenceType.UNKNOWN_RESOLVED_URL

    def parse_metadata_to_downloads(
        self,
        info_dict: dict[str, Any],
        source_identifier: str,
        ref_type: ReferenceType,
    ) -> list[Download]:
        logger.debug(
            "Parsing metadata to downloads.",
            extra={
                "source_identifier": source_identifier,
                "ref_type": ref_type,
                "info_dict_empty": not info_dict,
            },
        )
        if not info_dict:
            return []

        downloads: list[Download] = []
        if ref_type == ReferenceType.SINGLE:
            logger.debug(
                "Parsing as single item.",
                extra={"source_identifier": source_identifier},
            )
            try:
                downloads.append(
                    self._parse_single_video_entry(info_dict, source_identifier)
                )
            except YtdlpYoutubeDataError as e:
                logger.error(
                    "Failed to parse single video entry. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )
            except YtdlpYoutubeVideoFilteredOutError as e:
                logger.info(
                    "Video filtered out by yt-dlp. Skipping.",
                    exc_info=e,
                    extra={"source_identifier": source_identifier},
                )
        elif ref_type == ReferenceType.COLLECTION:
            entries = info_dict.get("entries")
            logger.debug(
                "Parsing as collection.",
                extra={
                    "source_identifier": source_identifier,
                    "num_entries_found": len(entries)
                    if isinstance(entries, list)
                    else "N/A (not a list)",
                },
            )
            if isinstance(entries, list):
                for i, entry_data in enumerate(entries):
                    item_id_for_log = entry_data.get("id", f"entry_{i}")
                    try:
                        parsed_download = self._parse_single_video_entry(
                            entry_data, source_identifier
                        )
                        downloads.append(parsed_download)
                    except YtdlpYoutubeDataError as e:
                        logger.error(
                            "Failed to parse video entry in collection. Skipping item.",
                            exc_info=e,
                            extra={
                                "source_identifier": source_identifier,
                                "item_id_approx": item_id_for_log,  # entry_id in exception has actual
                            },
                        )
                    except YtdlpYoutubeVideoFilteredOutError as e:
                        logger.info(
                            "Video in collection filtered out by yt-dlp. Skipping item.",
                            exc_info=e,
                            extra={
                                "source_identifier": source_identifier,
                                "item_id_approx": item_id_for_log,  # entry_id in exception has actual
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
                "Parsing with unknown reference type, attempting as single item.",
                extra={
                    "source_identifier": source_identifier,
                    "ref_type": ref_type,
                },
            )
            try:
                downloads.append(
                    self._parse_single_video_entry(info_dict, source_identifier)
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
