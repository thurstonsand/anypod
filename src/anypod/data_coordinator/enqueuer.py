"""Handles the enqueueing of new downloads for configured feeds.

This module defines the Enqueuer class, which is responsible for fetching
feed metadata, identifying new downloads, and managing their status in the
database for subsequent processing by the Downloader.
"""

from datetime import UTC, datetime
import logging
from pathlib import Path
from typing import Any

from ..config import FeedConfig
from ..db import DownloadDatabase
from ..db.feed_db import FeedDatabase
from ..db.types import Download, DownloadStatus, Feed, SourceType
from ..exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
    FeedNotFoundError,
    YtdlpApiError,
)
from ..metadata import merge_feed_metadata
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

    async def _try_bump_retries_and_log(
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
            _, _, transitioned_to_error = await self._download_db.bump_retries(
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

    async def _get_upcoming_downloads_for_feed(
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
            return await self._download_db.get_downloads_by_status(
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
                download.id == original_download_id and download.feed_id == feed_id
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
                    if (
                        download.id == original_download_id
                        and download.feed_id == feed_id
                    ):
                        return download
                logger.warning(
                    "Could not find matching download in re-fetched multiple metadata. Original download might have been removed or changed ID.",
                    extra=download_log_params,
                )
                return None

    async def _update_status_to_queued_if_vod(
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
                    await self._download_db.mark_as_queued_from_upcoming(
                        feed_id, db_download_id
                    )
                except (DownloadNotFoundError, DatabaseOperationError) as e:
                    raise EnqueueError(
                        "Video became ready to download, but could not update status to QUEUED.",
                        feed_id=feed_id,
                        download_id=db_download_id,
                    ) from e
                else:
                    logger.debug(
                        "Successfully updated upcoming to QUEUED.",
                        extra=download_log_params,
                    )
                    return True
            case DownloadStatus.UPCOMING:
                logger.debug(
                    f"Re-fetched download status is still {DownloadStatus.UPCOMING}. No change needed.",
                    extra=download_log_params,
                )
            case _:  # Should ideally not happen if ytdlp_wrapper returns consistent Download objects
                logger.warning(
                    f"Re-fetched upcoming download has unexpected status: {refetched_download.status}. Skipping.",
                    extra=download_log_params,
                )
        return False

    async def _process_single_upcoming_download(
        self,
        db_download: Download,
        feed: Feed,
        feed_config: FeedConfig,
        feed_log_params: dict[str, Any],
        cookies_path: Path | None = None,
    ) -> bool:
        """Process a single upcoming download by re-fetching metadata.

        Re-fetches metadata and updates status if the download has transitioned
        to a downloadable state.

        Args:
            db_download: The upcoming Download object from the database.
            feed: The Feed object from the database.
            feed_config: The feed configuration object.
            feed_log_params: Relevant logging parameters.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

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
            _, fetched_downloads = await self._ytdlp_wrapper.fetch_metadata(
                feed.id,
                SourceType.SINGLE_VIDEO,
                db_download.source_url,
                None,
                feed_config.yt_args,
                cookies_path=cookies_path,
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
            await self._try_bump_retries_and_log(
                feed.id,
                db_download.id,
                error_message,
                feed_config.max_errors,
                download_log_params,
            )
            return False

        refetched_download = self._extract_fetched_download(
            fetched_downloads, db_download.id, feed.id, download_log_params
        )

        if not refetched_download:
            error_message = "Original ID not found in re-fetched metadata, or mismatched/multiple downloads found."
            logger.warning(
                error_message,
                extra=download_log_params,
            )

            await self._try_bump_retries_and_log(
                feed.id,
                db_download.id,
                error_message,
                feed_config.max_errors,
                download_log_params,
            )
            return False

        return await self._update_status_to_queued_if_vod(
            feed.id, db_download.id, refetched_download, download_log_params
        )

    # --- Helpers for _fetch_and_process_feed_downloads ---

    async def _fetch_all_metadata_for_feed_url(
        self,
        feed: Feed,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
        log_params: dict[str, Any],
        cookies_path: Path | None = None,
    ) -> tuple[Feed, list[Download]]:
        """Fetch all media metadata for the feed URL with date filtering.

        Args:
            feed: The Feed object from the database.
            feed_config: The feed configuration object.
            fetch_since_date: Only fetch downloads published after this date.
            fetch_until_date: Only fetch downloads published before this date.
            log_params: Relevant logging parameters.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            Tuple of (Feed, list[Download]) containing extracted feed metadata
            and list of Download objects fetched from the feed.

        Raises:
            EnqueueError: If the main ytdlp fetch fails.
        """
        # Use user-configured yt_args directly (date filtering handled by wrapper)
        user_yt_cli_args = list(feed_config.yt_args)  # Make a copy
        logger.debug(
            "Fetching feed downloads.",
            extra={
                **log_params,
                "fetch_since_date": fetch_since_date.isoformat(),
                "fetch_until_date": fetch_until_date.isoformat(),
            },
        )

        try:
            (
                fetched_feed,
                all_fetched_downloads,
            ) = await self._ytdlp_wrapper.fetch_metadata(
                feed.id,
                feed.source_type,
                feed.source_url,
                feed.resolved_url,
                user_yt_cli_args,
                fetch_since_date,
                fetch_until_date,
                feed_config.keep_last,
                cookies_path,
            )
        except YtdlpApiError as e:
            raise EnqueueError(
                "Could not fetch main feed metadata.",
                feed_id=feed.id,
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

    async def _synchronize_feed_metadata(
        self,
        current_feed: Feed,
        fetched_feed: Feed,
        feed_config: FeedConfig,
        log_params: dict[str, Any],
    ) -> Feed:
        """Synchronize feed metadata from ytdlp extraction with database record.

        Compares config overrides with extracted metadata and updates database
        only if fields have changed. Config overrides take precedence over
        extracted values.

        Args:
            current_feed: The Feed object from the database.
            fetched_feed: Feed metadata extracted from ytdlp.
            feed_config: Feed configuration with potential overrides.
            log_params: Logging parameters for context.

        Raises:
            EnqueueError: If feed is not found or update fails.
        """
        async with self._feed_db.session() as session:
            # Current metadata in database
            current_metadata = {
                "title": current_feed.title,
                "subtitle": current_feed.subtitle,
                "description": current_feed.description,
                "language": current_feed.language,
                "author": current_feed.author,
                "author_email": current_feed.author_email,
                "image_url": current_feed.image_url,
                "category": current_feed.category,
                "podcast_type": current_feed.podcast_type,
                "explicit": current_feed.explicit,
            }

            candidate_metadata = merge_feed_metadata(fetched_feed, feed_config)

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
                return current_feed

            logger.debug(
                "Feed metadata changes detected, updating database.",
                extra={**log_params, "changed_fields": list(updates_needed.keys())},
            )

            # Update metadata
            try:
                await self._feed_db.update_feed_metadata(
                    current_feed.id, **updates_needed
                )
                await session.commit()
            except (FeedNotFoundError, DatabaseOperationError) as e:
                raise EnqueueError(
                    "Could not update feed metadata in database.",
                    feed_id=current_feed.id,
                ) from e

            return current_feed.model_copy(update=updates_needed)

    # TODO: if this fails, download is potentially lost forever
    async def _handle_newly_fetched_download(
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
            await self._download_db.upsert_download(download)
        except DatabaseOperationError as e:
            raise EnqueueError(
                "Failed to insert new download.",
                download_id=download.id,
            ) from e
        else:
            logger.debug("Successfully upserted download.", extra=log_params)
            return download.status == DownloadStatus.QUEUED

    async def _handle_existing_fetched_download(
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
        updated_download = existing_db_download.model_copy()

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
                logger.debug(
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
                logger.debug(
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
                await self._download_db.upsert_download(updated_download)
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

    async def _process_single_download(
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
            existing_db_download = await self._download_db.get_download_by_id(
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
            return await self._handle_newly_fetched_download(fetched_dl, log_params)
        else:
            return await self._handle_existing_fetched_download(
                existing_db_download, fetched_dl, feed_id, log_params
            )

    async def _handle_remaining_upcoming_downloads(
        self, feed: Feed, feed_config: FeedConfig, cookies_path: Path | None = None
    ) -> int:
        """Re-fetch metadata for existing UPCOMING downloads not processed by main feed.

        If a download is now a VOD, its status is updated to QUEUED. If metadata
        re-fetch fails repeatedly (controlled by feed_config.max_errors), the
        download's status is transitioned to ERROR.

        Only processes UPCOMING downloads where the published date is in the past,
        as these are likely to have transitioned to VOD. Skips downloads with
        future published dates to avoid unnecessary network calls.

        Args:
            feed: The Feed object from the database.
            feed_config: The configuration object for the feed.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            The count of downloads successfully transitioned from 'upcoming' to 'queued'.
        """
        feed_log_params = {"feed_id": feed.id}
        logger.debug("Handling remaining upcoming downloads.", extra=feed_log_params)

        upcoming_db_downloads = await self._get_upcoming_downloads_for_feed(
            feed.id, feed_config.url
        )

        if not upcoming_db_downloads:
            logger.debug(
                "No remaining upcoming downloads to process.", extra=feed_log_params
            )
            return 0

        # Filter to only check downloads with past published dates
        current_time = datetime.now(UTC)
        ready_downloads = [
            download
            for download in upcoming_db_downloads
            if download.published <= current_time
        ]

        future_downloads_count = len(upcoming_db_downloads) - len(ready_downloads)
        if future_downloads_count > 0:
            logger.debug(
                f"Skipping {future_downloads_count} upcoming downloads with future published dates.",
                extra=feed_log_params,
            )

        if not ready_downloads:
            logger.debug(
                "No upcoming downloads with past published dates to check.",
                extra=feed_log_params,
            )
            return 0

        logger.debug(
            f"Found {len(ready_downloads)} upcoming downloads with past published dates to re-check.",
            extra=feed_log_params,
        )

        queued_count = 0
        for db_download in ready_downloads:
            if await self._process_single_upcoming_download(
                db_download, feed, feed_config, feed_log_params, cookies_path
            ):
                queued_count += 1

        return queued_count

    async def _fetch_and_process_feed_and_new_downloads(
        self,
        feed: Feed,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
        cookies_path: Path | None = None,
    ) -> tuple[Feed, int]:
        """Fetch all media metadata for the feed URL within the given date range.

        For each download:
        - If new: inserts with status QUEUED (if VOD) or UPCOMING (if live/scheduled).

        Args:
            feed: The Feed object from the database.
            feed_config: The configuration object for the feed.
            fetch_since_date: Fetches downloads published after this date.
            fetch_until_date: Fetches downloads published before this date.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            A tuple of (updated feed, count of downloads newly set to QUEUED status)
        """
        feed_log_params = {"feed_id": feed.id, "feed_url": feed_config.url}
        logger.debug(
            "Fetching and processing all feed downloads.",
            extra=feed_log_params,
        )

        (
            fetched_feed,
            all_fetched_downloads,
        ) = await self._fetch_all_metadata_for_feed_url(
            feed,
            feed_config,
            fetch_since_date,
            fetch_until_date,
            feed_log_params,
            cookies_path,
        )

        # Synchronize feed metadata with database
        feed = await self._synchronize_feed_metadata(
            feed, fetched_feed, feed_config, feed_log_params
        )

        queued_count = 0
        for fetched_dl in all_fetched_downloads:
            if await self._process_single_download(
                fetched_dl, feed.id, feed_log_params
            ):
                queued_count += 1

        logger.debug(
            "Identified downloads as newly QUEUED from main feed processing.",
            extra={**feed_log_params, "queued_count": queued_count},
        )
        return feed, queued_count

    async def enqueue_new_downloads(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        fetch_until_date: datetime,
        cookies_path: Path | None = None,
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
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            The total count of downloads that were newly set to QUEUED status
            (either new downloads or UPCOMING downloads that transitioned to QUEUED).

        Raises:
            EnqueueError: If a critical, non-recoverable error occurs during the enqueue process.
                          This wraps underlying YtdlpApiError or DatabaseOperationError.
        """
        feed_log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.debug("Starting enqueue_new_downloads process.", extra=feed_log_params)

        # Fetch feed from database to get source_type and resolved_url
        try:
            feed = await self._feed_db.get_feed_by_id(feed_id)
        except FeedNotFoundError as e:
            raise EnqueueError(
                "Feed not found in database.",
                feed_id=feed_id,
                feed_url=feed_config.url,
            ) from e
        except DatabaseOperationError as e:
            raise EnqueueError(
                "Failed to fetch feed from database.",
                feed_id=feed_id,
                feed_url=feed_config.url,
            ) from e

        # Fetch and process all feed downloads first (includes UPCOMING)
        (
            feed,
            queued_from_feed_fetch,
        ) = await self._fetch_and_process_feed_and_new_downloads(
            feed, feed_config, fetch_since_date, fetch_until_date, cookies_path
        )
        logger.debug(
            "Downloads processed from feed.",
            extra={**feed_log_params, "queued_count": queued_from_feed_fetch},
        )

        # Handle remaining UPCOMING downloads (only those with past published dates)
        queued_from_remaining_upcoming = (
            await self._handle_remaining_upcoming_downloads(
                feed, feed_config, cookies_path
            )
        )
        logger.debug(
            "Remaining upcoming downloads transitioned to QUEUED.",
            extra={**feed_log_params, "queued_count": queued_from_remaining_upcoming},
        )

        total_queued_count = queued_from_feed_fetch + queued_from_remaining_upcoming
        logger.debug(
            "Enqueue process completed for feed.",
            extra={
                **feed_log_params,
                "queued_count": total_queued_count,
            },
        )
        return total_queued_count
