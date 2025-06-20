"""Handles the enqueueing of new downloads for configured feeds.

This module defines the Enqueuer class, which is responsible for fetching
feed metadata, identifying new downloads, and managing their status in the
database for subsequent processing by the Downloader.
"""

from copy import deepcopy
from datetime import datetime
import logging
from typing import Any

from ..config import FeedConfig
from ..db import DownloadDatabase
from ..db.feed_db import FeedDatabase
from ..db.types import Download, DownloadStatus, Feed
from ..exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
    FeedNotFoundError,
    YtdlpApiError,
)
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class Enqueuer:
    """Manage the enqueueing of new downloads from feed sources.

    The Enqueuer fetches metadata from feed URLs, identifies new downloadable
    items, and manages their initial database states. It handles both new
    items and existing items that need status updates.

    Attributes:
        _feed_db: Database manager for feed record operations.
        _download_db: Database manager for download record operations.
        _ytdlp_wrapper: Wrapper for yt-dlp metadata extraction operations.
    """

    def __init__(
        self,
        feed_db: FeedDatabase,
        download_db: DownloadDatabase,
        ytdlp_wrapper: YtdlpWrapper,
    ):
        self._feed_db = feed_db
        self._download_db = download_db
        self._ytdlp_wrapper = ytdlp_wrapper
        logger.debug("Enqueuer initialized.")

    def _try_bump_retries_and_log(
        self,
        feed_id: str,
        download_id: str,
        error_message: str,
        max_errors: int,
        log_params_base: dict[str, Any],
    ) -> bool:
        """Attempt to bump retries and log the outcome.

        Args:
            feed_id: The feed identifier.
            download_id: The download identifier.
            error_message: The error message to record.
            max_errors: Maximum allowed errors before transitioning to ERROR.
            log_params_base: Relevant logging parameters.

        Returns:
            True if transitioned to ERROR state.
        """
        transitioned_to_error_state = False
        try:
            _, _, transitioned_to_error = self._download_db.bump_retries(
                feed_id=feed_id,
                download_id=download_id,
                error_message=error_message,
                max_allowed_errors=max_errors,
            )
        except (DownloadNotFoundError, DatabaseOperationError) as db_err:
            logger.warning(
                "Could not bump error count for download.",
                extra=log_params_base,
                exc_info=db_err,
            )
        else:
            if transitioned_to_error:
                logger.warning(
                    "Download transitioned to ERROR due to repeated failures.",
                    extra=log_params_base,
                )
                transitioned_to_error_state = True
            else:
                logger.debug(
                    "Incremented error count for download.",
                    extra=log_params_base,
                )
        return transitioned_to_error_state

    # --- Helpers for _handle_existing_upcoming_downloads ---

    def _get_upcoming_downloads_for_feed(
        self,
        feed_id: str,
        feed_url: str,
    ) -> list[Download]:
        """Fetch UPCOMING downloads for a feed from the database.

        Args:
            feed_id: The feed identifier.
            feed_url: The feed URL (for error reporting).

        Returns:
            List of Download objects with UPCOMING status.

        Raises:
            EnqueueError: If the database fetch fails.
        """
        log_params = {"feed_id": feed_id}
        logger.debug("Fetching upcoming downloads from DB.", extra=log_params)
        try:
            return self._download_db.get_downloads_by_status(
                DownloadStatus.UPCOMING, feed_id=feed_id
            )
        except (DatabaseOperationError, ValueError) as e:
            raise EnqueueError(
                "Could not fetch upcoming downloads from DB.",
                feed_id=feed_id,
                feed_url=feed_url,
            ) from e

    def _extract_fetched_download(
        self,
        fetched_downloads: list[Download],
        original_download_id: str,
        feed_id: str,
        download_log_params: dict[str, Any],
    ) -> Download | None:
        """Search for a matching download in the fetched list.

        Logs warnings for mismatches or multiple results.

        Args:
            fetched_downloads: List of fetched downloads to search.
            original_download_id: The original download ID to find.
            feed_id: The feed identifier for matching.
            download_log_params: Relevant logging parameters.

        Returns:
            Matching Download object if found, None otherwise.
        """
        match fetched_downloads:
            case []:
                return None
            # Expected case
            case [download] if (
                download.id == original_download_id and download.feed == feed_id
            ):
                return download
            case [download]:
                logger.warning(
                    "Re-fetched single download does not match expected DB download. Skipping.",
                    extra={
                        **download_log_params,
                        "fetched_download_id": download.id,
                    },
                )
                return None
            # Multiple results
            case downloads:
                logger.warning(
                    "Metadata re-fetch for download unexpectedly returned multiple results. Searching for ID.",
                    extra={
                        **download_log_params,
                        "num_fetched_downloads": len(downloads),
                    },
                )
                for download in downloads:
                    if download.id == original_download_id and download.feed == feed_id:
                        return download
                logger.warning(
                    "Could not find matching download in re-fetched multiple metadata. Original download might have been removed or changed ID.",
                    extra=download_log_params,
                )
                return None

    def _update_status_to_queued_if_vod(
        self,
        feed_id: str,
        db_download_id: str,
        refetched_download: Download,
        download_log_params: dict[str, Any],
    ) -> bool:
        """Check if refetched download is VOD and update DB status to QUEUED.

        Args:
            feed_id: The feed identifier.
            db_download_id: The download ID in the database.
            refetched_download: The refetched Download object.
            download_log_params: Relevant logging parameters.

        Returns:
            True if status was updated to QUEUED.
        """
        match refetched_download.status:
            case DownloadStatus.QUEUED:
                logger.info(
                    "Upcoming download has transitioned to VOD (QUEUED). Updating status.",
                    extra=download_log_params,
                )
                try:
                    self._download_db.mark_as_queued_from_upcoming(
                        feed_id, db_download_id
                    )
                except (DownloadNotFoundError, DatabaseOperationError) as e:
                    raise EnqueueError(
                        "Video became ready to download, but could not update status to QUEUED.",
                        feed_id=feed_id,
                        download_id=db_download_id,
                    ) from e
                else:
                    logger.info(
                        "Successfully updated upcoming to QUEUED.",
                        extra=download_log_params,
                    )
                    return True
            case DownloadStatus.UPCOMING:
                logger.info(
                    f"Re-fetched download status is still {DownloadStatus.UPCOMING}. No change needed.",
                    extra=download_log_params,
                )
            case _:  # Should ideally not happen if ytdlp_wrapper returns consistent Download objects
                logger.warning(
                    f"Re-fetched upcoming download has unexpected status: {refetched_download.status}. Skipping.",
                    extra=download_log_params,
                )
        return False

    def _process_single_upcoming_download(
        self,
        db_download: Download,
        feed_id: str,
        feed_config: FeedConfig,
        feed_log_params: dict[str, Any],
    ) -> bool:
        """Process a single upcoming download by re-fetching metadata.

        Re-fetches metadata and updates status if the download has transitioned
        to a downloadable state.

        Args:
            db_download: The upcoming Download object from the database.
            feed_id: The feed identifier.
            feed_config: The feed configuration object.
            feed_log_params: Relevant logging parameters.

        Returns:
            True if the download was successfully transitioned to QUEUED.
        """
        download_log_params = {
            **feed_log_params,
            "download_id": db_download.id,
            "source_url": db_download.source_url,
        }
        logger.debug(
            "Re-checking status for upcoming download.", extra=download_log_params
        )

        fetched_downloads: list[Download] | None = None
        try:
            _, fetched_downloads = self._ytdlp_wrapper.fetch_metadata(
                feed_id,
                db_download.source_url,
                feed_config.yt_args,
            )
        except YtdlpApiError as e:
            error_message = "Failed to re-fetch metadata for upcoming download."
            logger.warning(
                error_message,
                extra={
                    **download_log_params,
                    "cli_args": feed_config.yt_args,
                },
                exc_info=e,
            )
            self._try_bump_retries_and_log(
                feed_id,
                db_download.id,
                error_message,
                feed_config.max_errors,
                download_log_params,
            )
            return False

        refetched_download = self._extract_fetched_download(
            fetched_downloads, db_download.id, feed_id, download_log_params
        )

        if not refetched_download:
            error_message = "Original ID not found in re-fetched metadata, or mismatched/multiple downloads found."
            logger.warning(
                error_message,
                extra=download_log_params,
            )

            self._try_bump_retries_and_log(
                feed_id,
                db_download.id,
                error_message,
                feed_config.max_errors,
                download_log_params,
            )
            return False

        return self._update_status_to_queued_if_vod(
            feed_id, db_download.id, refetched_download, download_log_params
        )

    # --- Helpers for _fetch_and_process_feed_downloads ---

    def _fetch_all_metadata_for_feed_url(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
        log_params: dict[str, Any],
    ) -> tuple[Feed, list[Download]]:
        """Fetch all media metadata for the feed URL with date filtering.

        Args:
            feed_id: The feed identifier.
            feed_config: The feed configuration object.
            fetch_since_date: Only fetch downloads published after this date.
            fetch_until_date: Only fetch downloads published before this date.
            log_params: Relevant logging parameters.

        Returns:
            Tuple of (Feed, list[Download]) containing extracted feed metadata
            and list of Download objects fetched from the feed.

        Raises:
            EnqueueError: If the main ytdlp fetch fails.
        """
        # Use user-configured yt_args directly (date filtering handled by wrapper)
        user_yt_cli_args = dict(feed_config.yt_args)  # Make a copy
        logger.info(
            "Fetching feed downloads.",
            extra={
                **log_params,
                "fetch_since_date": fetch_since_date.isoformat(),
                "fetch_until_date": fetch_until_date.isoformat(),
            },
        )

        try:
            fetched_feed, all_fetched_downloads = self._ytdlp_wrapper.fetch_metadata(
                feed_id,
                feed_config.url,
                user_yt_cli_args,
                fetch_since_date,
                fetch_until_date,
            )
        except YtdlpApiError as e:
            raise EnqueueError(
                "Could not fetch main feed metadata.",
                feed_id=feed_id,
                feed_url=feed_config.url,
            ) from e

        if not all_fetched_downloads:
            logger.debug(
                "No downloads returned from feed metadata fetch (may be filtered or empty).",
                extra=log_params,
            )
        else:
            logger.debug(
                f"Fetched {len(all_fetched_downloads)} downloads from feed URL.",
                extra=log_params,
            )
        return fetched_feed, all_fetched_downloads

    def _synchronize_feed_metadata(
        self,
        feed_id: str,
        fetched_feed: Feed,
        feed_config: FeedConfig,
        log_params: dict[str, Any],
    ) -> None:
        """Synchronize feed metadata from ytdlp extraction with database record.

        Compares config overrides with extracted metadata and updates database
        only if fields have changed. Config overrides take precedence over
        extracted values.

        Args:
            feed_id: The feed identifier.
            fetched_feed: Feed metadata extracted from ytdlp.
            feed_config: Feed configuration with potential overrides.
            log_params: Logging parameters for context.

        Raises:
            EnqueueError: If feed is not found or update fails.
        """
        try:
            # Get current feed from database
            current_feed = self._feed_db.get_feed_by_id(feed_id)
        except FeedNotFoundError as e:
            raise EnqueueError(
                "Feed not found in database during metadata sync.",
                feed_id=feed_id,
            ) from e
        except DatabaseOperationError as e:
            logger.warning(
                "Could not retrieve feed for metadata sync, skipping.",
                extra=log_params,
                exc_info=e,
            )
            return

        # Current metadata in database
        current_metadata = {
            "title": current_feed.title,
            "subtitle": current_feed.subtitle,
            "description": current_feed.description,
            "language": current_feed.language,
            "author": current_feed.author,
            "image_url": current_feed.image_url,
            "category": str(current_feed.category) if current_feed.category else None,
            "explicit": str(current_feed.explicit) if current_feed.explicit else None,
        }

        # Start with override metadata if present
        metadata_overrides = feed_config.metadata
        if metadata_overrides:
            candidate_metadata: dict[str, Any] = {
                "title": metadata_overrides.title,
                "subtitle": metadata_overrides.subtitle,
                "description": metadata_overrides.description,
                "language": metadata_overrides.language,
                "author": metadata_overrides.author,
                "image_url": metadata_overrides.image_url,
                "category": str(metadata_overrides.categories)
                if metadata_overrides.categories
                else None,
                "explicit": str(metadata_overrides.explicit)
                if metadata_overrides.explicit
                else None,
            }
        else:
            candidate_metadata: dict[str, Any] = {}

        # Fill in missing values from fetched feed
        candidate_metadata["title"] = (
            candidate_metadata.get("title") or fetched_feed.title
        )
        candidate_metadata["subtitle"] = (
            candidate_metadata.get("subtitle") or fetched_feed.subtitle
        )
        candidate_metadata["description"] = (
            candidate_metadata.get("description") or fetched_feed.description
        )
        candidate_metadata["language"] = (
            candidate_metadata.get("language") or fetched_feed.language
        )
        candidate_metadata["author"] = (
            candidate_metadata.get("author") or fetched_feed.author
        )
        candidate_metadata["image_url"] = (
            candidate_metadata.get("image_url") or fetched_feed.image_url
        )
        candidate_metadata["category"] = candidate_metadata.get("category") or (
            str(fetched_feed.category) if fetched_feed.category else None
        )
        candidate_metadata["explicit"] = candidate_metadata.get("explicit") or (
            str(fetched_feed.explicit) if fetched_feed.explicit else None
        )

        # Find fields that need updating
        updates_needed = {
            key: val
            for key, val in candidate_metadata.items()
            if current_metadata[key] != val
        }

        if not updates_needed:
            logger.debug(
                "No feed metadata changes detected, skipping update.",
                extra=log_params,
            )
            return

        logger.info(
            "Feed metadata changes detected, updating database.",
            extra={**log_params, "changed_fields": list(updates_needed.keys())},
        )

        # Use transaction for atomic update
        try:
            self._feed_db.update_feed_metadata(feed_id, **updates_needed)
        except (FeedNotFoundError, DatabaseOperationError) as e:
            raise EnqueueError(
                "Could not update feed metadata in database.",
                feed_id=feed_id,
            ) from e

    # TODO: if this fails, download is potentially lost forever
    def _handle_newly_fetched_download(
        self, download: Download, log_params: dict[str, Any]
    ) -> bool:
        """Handle a new download by upserting it to the database.

        Args:
            download: The new Download object.
            log_params: Relevant logging parameters.

        Returns:
            True if download status is QUEUED.
        """
        logger.info(
            "New download found. Inserting.",
            extra=log_params,
        )
        try:
            self._download_db.upsert_download(download)
        except DatabaseOperationError as e:
            raise EnqueueError(
                "Failed to insert new download.",
                download_id=download.id,
            ) from e
        else:
            logger.debug("Successfully upserted download.", extra=log_params)
            return download.status == DownloadStatus.QUEUED

    def _handle_existing_fetched_download(
        self,
        existing_db_download: Download,
        fetched_download: Download,
        feed_id: str,
        log_params: dict[str, Any],
    ) -> bool:
        """Handle an existing download based on status comparison and metadata changes.

        Args:
            existing_db_download: The existing Download from the database.
            fetched_download: The newly fetched Download object.
            feed_id: The feed identifier.
            log_params: Logging parameters.

        Returns:
            True if download is newly QUEUED.
        """
        current_log_params = {
            **log_params,
            "existing_db_status": existing_db_download.status,
        }

        # Create a copy of existing download to track changes
        updated_download = deepcopy(existing_db_download)

        # Apply metadata changes directly to the copy
        if existing_db_download.source_url != fetched_download.source_url:
            updated_download.source_url = fetched_download.source_url
        if existing_db_download.title != fetched_download.title:
            updated_download.title = fetched_download.title
        if existing_db_download.published != fetched_download.published:
            updated_download.published = fetched_download.published
        if existing_db_download.ext != fetched_download.ext:
            updated_download.ext = fetched_download.ext
        if existing_db_download.mime_type != fetched_download.mime_type:
            updated_download.mime_type = fetched_download.mime_type
        if existing_db_download.filesize != fetched_download.filesize:
            updated_download.filesize = fetched_download.filesize
        if existing_db_download.duration != fetched_download.duration:
            updated_download.duration = fetched_download.duration
        if existing_db_download.thumbnail != fetched_download.thumbnail:
            updated_download.thumbnail = fetched_download.thumbnail
        if existing_db_download.description != fetched_download.description:
            updated_download.description = fetched_download.description
        if existing_db_download.quality_info != fetched_download.quality_info:
            updated_download.quality_info = fetched_download.quality_info

        # Handle status changes and their side effects
        match (existing_db_download.status, fetched_download.status):
            case (DownloadStatus.UPCOMING, DownloadStatus.QUEUED as fetched_status):
                logger.info(
                    f"Existing UPCOMING download has transitioned to VOD ({fetched_status}). Updating status.",
                    extra=current_log_params,
                )
                updated_download.status = DownloadStatus.QUEUED
            case (DownloadStatus.ERROR, DownloadStatus.QUEUED):
                logger.info(
                    "Existing ERROR download set to be requeued.",
                    extra=current_log_params,
                )
                updated_download.status = DownloadStatus.QUEUED
                # For ERROR->QUEUED transitions, reset retry state
                updated_download.retries = 0
                updated_download.last_error = None
            case (DownloadStatus.DOWNLOADED, DownloadStatus.QUEUED):
                logger.debug(
                    "Existing DOWNLOADED item found in feed, skipping.",
                    extra=current_log_params,
                )
                return False
            case (
                DownloadStatus.UPCOMING as existing_status,
                DownloadStatus.UPCOMING as fetched_status,
            ) | (
                DownloadStatus.QUEUED as existing_status,
                DownloadStatus.QUEUED as fetched_status,
            ):
                logger.debug(
                    f"Existing download status '{existing_status}' matches fetched '{fetched_status}'. No status change needed.",
                    extra=current_log_params,
                )
            case (existing_status, fetched_status):
                logger.info(
                    f"Existing download status '{existing_status}' differs from fetched '{fetched_status}'. Updating status for consistency.",
                    extra=current_log_params,
                )
                updated_download.status = fetched_status

        # Only upsert if there are actual changes
        if not updated_download.content_equals(existing_db_download):
            logger.debug(
                "Changes detected, performing database update.",
                extra=current_log_params,
            )
            try:
                self._download_db.upsert_download(updated_download)
            except DatabaseOperationError as e:
                raise EnqueueError(
                    "Failed to update download.",
                    feed_id=feed_id,
                    download_id=fetched_download.id,
                ) from e
            return updated_download.status == DownloadStatus.QUEUED
        else:
            logger.debug(
                "No changes detected, skipping database update.",
                extra=current_log_params,
            )
            return False

    def _process_single_download(
        self,
        fetched_dl: Download,
        feed_id: str,
        feed_log_params: dict[str, Any],
    ) -> bool:
        """Process a single fetched download.

        Args:
            fetched_dl: The fetched Download object.
            feed_id: The feed identifier.
            feed_log_params: Relevant logging parameters.

        Returns:
            True if it results in a newly QUEUED download.
        """
        log_params = {
            **feed_log_params,
            "download_id": fetched_dl.id,
            "fetched_status": fetched_dl.status,
        }
        logger.debug("Processing fetched download.", extra=log_params)

        try:
            existing_db_download = self._download_db.get_download_by_id(
                feed_id, fetched_dl.id
            )
        except DatabaseOperationError as e:
            logger.error(
                "Database error checking for existing download.",
                extra=log_params,
                exc_info=e,
            )
            return False  # Did not result in a new QUEUED download
        except DownloadNotFoundError:
            return self._handle_newly_fetched_download(fetched_dl, log_params)
        else:
            return self._handle_existing_fetched_download(
                existing_db_download, fetched_dl, feed_id, log_params
            )

    def _handle_existing_upcoming_downloads(
        self, feed_id: str, feed_config: FeedConfig
    ) -> int:
        """Re-fetch metadata for existing DB entries with UPCOMING status.

        If a download is now a VOD, its status is updated to QUEUED. If
        metadata re-fetch fails repeatedly (controlled by feed_config.max_errors),
        the download's status is transitioned to ERROR.

        Args:
            feed_id: The unique identifier for the feed.
            feed_config: The configuration object for the feed.

        Returns:
            The count of downloads successfully transitioned from 'upcoming' to 'queued'.
        """
        feed_log_params = {"feed_id": feed_id}
        logger.debug("Handling existing upcoming downloads.", extra=feed_log_params)

        upcoming_db_downloads = self._get_upcoming_downloads_for_feed(
            feed_id, feed_config.url
        )

        if not upcoming_db_downloads:
            logger.debug(
                "No existing upcoming downloads to process.", extra=feed_log_params
            )
            return 0

        logger.info(
            f"Found {len(upcoming_db_downloads)} existing upcoming downloads to re-check.",
            extra=feed_log_params,
        )

        queued_count = 0
        for db_download in upcoming_db_downloads:
            if self._process_single_upcoming_download(
                db_download, feed_id, feed_config, feed_log_params
            ):
                queued_count += 1

        return queued_count

    def _fetch_and_process_new_feed_downloads(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
    ) -> int:
        """Fetch all media metadata for the feed URL within the given date range.

        For each download:
        - If new: inserts with status QUEUED (if VOD) or UPCOMING (if live/scheduled).

        Args:
            feed_id: The unique identifier for the feed.
            feed_config: The configuration object for the feed.
            fetch_since_date: Fetches downloads published after this date.
            fetch_until_date: Fetches downloads published before this date.

        Returns:
            The count of downloads newly set to QUEUED status (either new VODs or
            UPCOMING downloads that transitioned to QUEUED).
        """
        feed_log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.debug(
            "Fetching and processing all feed downloads.",
            extra=feed_log_params,
        )

        fetched_feed, all_fetched_downloads = self._fetch_all_metadata_for_feed_url(
            feed_id, feed_config, fetch_since_date, fetch_until_date, feed_log_params
        )

        # Synchronize feed metadata with database
        self._synchronize_feed_metadata(
            feed_id, fetched_feed, feed_config, feed_log_params
        )

        queued_count = 0
        for fetched_dl in all_fetched_downloads:
            if self._process_single_download(fetched_dl, feed_id, feed_log_params):
                queued_count += 1

        logger.debug(
            "Identified downloads as newly QUEUED from main feed processing.",
            extra={**feed_log_params, "queued_count": queued_count},
        )
        return queued_count

    def enqueue_new_downloads(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
    ) -> int:
        """Fetch media metadata for a feed and enqueue new downloads.

        This method performs two main phases:
        1. Re-polls existing database entries with status UPCOMING for the given feed.
           If an UPCOMING download is now a VOD (Video on Demand), its status is updated to QUEUED.
        2. Fetches the latest media metadata from the feed source using YtdlpWrapper,
           filtered by fetch_since_date and fetch_until_date.
           - For each new download not already in the database:
             - If its parsed status is QUEUED (VOD), it's inserted as QUEUED.
             - If its parsed status is UPCOMING (live/scheduled), it's inserted as UPCOMING.
           - For existing UPCOMING entries that are now found to be VOD (QUEUED status from fetch),
             their status is updated to QUEUED.

        Args:
            feed_id: The unique identifier for the feed.
            feed_config: The configuration object for the feed, containing URL and yt-dlp arguments.
            fetch_since_date: Fetching will only look for downloads published after this date.
            fetch_until_date: Fetching will only look for downloads published before this date.

        Returns:
            The total count of downloads that were newly set to QUEUED status
            (either new downloads or UPCOMING downloads that transitioned to QUEUED).

        Raises:
            EnqueueError: If a critical, non-recoverable error occurs during the enqueue process.
                          This wraps underlying YtdlpApiError or DatabaseOperationError.
        """
        feed_log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.info("Starting enqueue_new_downloads process.", extra=feed_log_params)

        # Handle existing UPCOMING downloads
        queued_from_upcoming = self._handle_existing_upcoming_downloads(
            feed_id, feed_config
        )
        logger.info(
            "Upcoming downloads transitioned to QUEUED.",
            extra={**feed_log_params, "queued_count": queued_from_upcoming},
        )

        # Fetch and process all feed downloads
        queued_from_feed_fetch = self._fetch_and_process_new_feed_downloads(
            feed_id, feed_config, fetch_since_date, fetch_until_date
        )
        logger.info(
            "New/updated downloads set to QUEUED.",
            extra={**feed_log_params, "queued_count": queued_from_feed_fetch},
        )

        total_queued_count = queued_from_upcoming + queued_from_feed_fetch
        logger.info(
            "Enqueue process completed for feed.",
            extra={
                **feed_log_params,
                "queued_count": total_queued_count,
            },
        )
        return total_queued_count
