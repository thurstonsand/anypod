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
from ..db.types import Download, DownloadStatus, Feed, SourceType, TranscriptSource
from ..exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
    FeedNotFoundError,
    YtdlpApiError,
)
from ..metadata import merge_download_metadata
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

        try:
            # Re-fetch single video metadata to check if it's now available
            fetched_downloads = await self._ytdlp_wrapper.fetch_new_downloads_metadata(
                feed.id,
                SourceType.SINGLE_VIDEO,
                db_download.source_url,
                None,  # No resolved URL needed for single video
                feed_config.yt_args,
                None,  # No date filtering for single video
                None,  # No keep_last for single video
                feed_config.transcript_lang,
                feed_config.transcript_source_priority,
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

        if not fetched_downloads:
            error_message = "No metadata returned for upcoming download re-fetch."
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
            "New download found.",
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

        updated_download = merge_download_metadata(
            existing_db_download, fetched_download
        )

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

        feed_url = feed_config.url
        # This is guaranteed by earlier filtering
        assert feed_url is not None, "Scheduled feeds must define url"

        upcoming_db_downloads = await self._get_upcoming_downloads_for_feed(
            feed.id, feed_url
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

    async def _fetch_and_process_new_downloads(
        self,
        feed: Feed,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        cookies_path: Path | None = None,
    ) -> tuple[int, datetime]:
        """Fetch download metadata for the feed URL within the given date range.

        For each download:
        - If new: inserts with status QUEUED (if VOD) or UPCOMING (if live/scheduled).

        Args:
            feed: The Feed object from the database.
            feed_config: The configuration object for the feed.
            fetch_since_date: Fetches downloads published after this date.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            A tuple of (count of downloads newly set to QUEUED status, sync timestamp)
        """
        feed_log_params = {"feed_id": feed.id, "feed_url": feed_config.url}
        logger.debug(
            "Fetching and processing all feed downloads.",
            extra=feed_log_params,
        )

        # Use user-configured yt_args directly (date filtering handled by wrapper)
        user_yt_cli_args = list(feed_config.yt_args)  # Make a copy
        logger.debug(
            "Fetching feed downloads.",
            extra={
                **feed_log_params,
                "fetch_since_date": fetch_since_date.isoformat(),
            },
        )

        # Use this time for last_successful_sync
        sync_timestamp = datetime.now(UTC)
        try:
            # this is guaranteed by earlier filtering
            assert feed.source_url is not None
            # Fetch filtered video metadata for enqueuing
            all_fetched_downloads = (
                await self._ytdlp_wrapper.fetch_new_downloads_metadata(
                    feed.id,
                    feed.source_type,
                    feed.source_url,
                    feed.resolved_url,
                    user_yt_cli_args,
                    fetch_since_date,
                    feed_config.keep_last,
                    feed_config.transcript_lang,
                    feed_config.transcript_source_priority,
                    cookies_path,
                )
            )
        except YtdlpApiError as e:
            raise EnqueueError(
                "Could not fetch downloads metadata.",
                feed_id=feed.id,
                feed_url=feed_config.url,
            ) from e

        if not all_fetched_downloads:
            logger.debug(
                "No downloads returned from feed metadata fetch (may be filtered or empty).",
                extra=feed_log_params,
            )
        else:
            logger.debug(
                f"Fetched {len(all_fetched_downloads)} downloads from feed URL.",
                extra=feed_log_params,
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
        return queued_count, sync_timestamp

    async def enqueue_new_downloads(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
        cookies_path: Path | None = None,
    ) -> tuple[int, datetime]:
        """Fetch media metadata for a feed and enqueue new downloads.

        This method performs two main phases:
        1. Re-polls existing database entries with status UPCOMING for the given feed.
           If an UPCOMING download is now a VOD (Video on Demand), its status is updated to QUEUED.
        2. Fetches the latest media metadata from the feed source using YtdlpWrapper,
           filtered by fetch_since_date.
           - For each new download not already in the database:
             - If its parsed status is QUEUED (VOD), it's inserted as QUEUED.
             - If its parsed status is UPCOMING (live/scheduled), it's inserted as UPCOMING.
           - For existing UPCOMING entries that are now found to be VOD (QUEUED status from fetch),
             their status is updated to QUEUED.

        Args:
            feed_id: The unique identifier for the feed.
            feed_config: The configuration object for the feed, containing URL and yt-dlp arguments.
            fetch_since_date: Fetching will only look for downloads published after this date.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            Tuple of (total downloads newly set to QUEUED status, last_successful_sync timestamp from yt-dlp).


        Raises:
            EnqueueError: If a critical, non-recoverable error occurs during the enqueue process.
                          This wraps underlying YtdlpApiError or DatabaseOperationError.
        """
        feed_log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.debug("Starting enqueue_new_downloads process.", extra=feed_log_params)

        if feed_config.is_manual:
            logger.debug(
                "Manual feed configured for manual submissions only; skipping metadata fetch.",
                extra=feed_log_params,
            )
            return 0, datetime.now(UTC)

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
            queued_from_feed_fetch,
            sync_timestamp,
        ) = await self._fetch_and_process_new_downloads(
            feed, feed_config, fetch_since_date, cookies_path
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

        return total_queued_count, sync_timestamp

    async def fetch_refreshed_metadata(
        self,
        existing_download: Download,
        yt_args: list[str],
        transcript_lang: str | None = None,
        transcript_source_priority: list[TranscriptSource] | None = None,
        cookies_path: Path | None = None,
    ) -> Download:
        """Fetch fresh metadata and merge with existing download (read-only).

        Fetches fresh metadata from yt-dlp for the specified download and merges
        it with the existing download. Does NOT persist changes to the database.
        The caller is responsible for persisting the returned Download.

        See persist_refreshed_metadata for the second half of this flow.

        Fields updated from yt-dlp (if non-empty):
            - title, description, published, source_url
            - quality_info, remote_thumbnail_url
            - transcript_ext, transcript_lang, transcript_source

        Args:
            existing_download: The current Download object from the database.
            yt_args: User-configured yt-dlp command-line arguments.
            transcript_lang: Language code for transcripts (e.g., "en").
            transcript_source_priority: Ordered list of transcript sources to try.
            cookies_path: Path to cookies.txt file for yt-dlp authentication.

        Returns:
            The merged Download object with updated metadata fields.
            Returns the original download if no metadata changes detected.

        Raises:
            EnqueueError: If metadata fetch fails or download ID not found.
        """
        feed_id = existing_download.feed_id
        download_id = existing_download.id
        log_params = {"feed_id": feed_id, "download_id": download_id}
        logger.debug("Fetching refreshed metadata for download.", extra=log_params)

        # Fetch fresh metadata from yt-dlp using the download's source URL
        try:
            fetched_downloads = await self._ytdlp_wrapper.fetch_new_downloads_metadata(
                feed_id,
                SourceType.SINGLE_VIDEO,
                existing_download.source_url,
                None,  # No resolved URL needed for single video
                yt_args,
                None,  # No date filtering
                None,  # No keep_last limit
                transcript_lang=transcript_lang,
                transcript_source_priority=transcript_source_priority,
                cookies_path=cookies_path,
            )
        except YtdlpApiError as e:
            raise EnqueueError(
                "Failed to fetch fresh metadata from yt-dlp.",
                feed_id=feed_id,
                download_id=download_id,
            ) from e

        fetched_download = self._extract_fetched_download(
            fetched_downloads, download_id, feed_id, log_params
        )

        if fetched_download is None:
            raise EnqueueError(
                "Download ID not found in fetched metadata. "
                "The video may have been removed or changed ID.",
                feed_id=feed_id,
                download_id=download_id,
            )

        updated_download = merge_download_metadata(existing_download, fetched_download)

        if updated_download.content_equals(existing_download):
            logger.debug(
                "No metadata changes detected during refresh.",
                extra=log_params,
            )
            return existing_download

        logger.debug(
            "Metadata changes detected, returning merged download.",
            extra=log_params,
        )
        return updated_download

    async def persist_refreshed_metadata(
        self,
        existing_download: Download,
        merged_download: Download,
        old_thumbnail_url: str | None,
        thumbnail_refreshed: bool,
    ) -> Download:
        """Persist a refreshed download based on thumbnail outcome.

        Determines the final state of a refreshed download by reconciling metadata
        changes with thumbnail download results. If the thumbnail URL changed but
        the download failed, reverts to the old URL to maintain database consistency.
        Only persists if there are actual changes from the existing download.

        See fetch_refreshed_download for the first half of this flow.

        Args:
            existing_download: The original download from the database.
            merged_download: The download with merged metadata from yt-dlp.
            old_thumbnail_url: The original thumbnail URL before refresh.
            thumbnail_refreshed: Whether the thumbnail was successfully downloaded.

        Returns:
            The persisted Download (or existing if no changes).

        Raises:
            EnqueueError: If database update fails.
        """
        feed_id = merged_download.feed_id
        download_id = merged_download.id
        log_params = {"feed_id": feed_id, "download_id": download_id}

        thumbnail_url_changed = (
            merged_download.remote_thumbnail_url != old_thumbnail_url
            and merged_download.remote_thumbnail_url is not None
        )

        if thumbnail_url_changed and not thumbnail_refreshed:
            # Thumbnail download failed - keep old URL to maintain consistency
            final_download = merged_download.model_copy(
                update={"remote_thumbnail_url": old_thumbnail_url}
            )
            logger.info(
                "Reverting to old thumbnail URL due to download failure.",
                extra=log_params,
            )
        elif thumbnail_refreshed:
            # Thumbnail download succeeded - set the extension
            final_download = merged_download.model_copy(update={"thumbnail_ext": "jpg"})
        else:
            final_download = merged_download

        # Only persist if there are actual changes
        if final_download.content_equals(existing_download):
            return existing_download

        try:
            await self._download_db.update_download(final_download)
        except (DownloadNotFoundError, DatabaseOperationError) as e:
            raise EnqueueError(
                "Failed to persist refreshed metadata.",
                feed_id=feed_id,
                download_id=download_id,
            ) from e

        logger.info("Refreshed metadata persisted.", extra=log_params)
        return final_download
