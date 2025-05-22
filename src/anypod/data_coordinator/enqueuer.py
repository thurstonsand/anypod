from datetime import datetime
import logging
from typing import Any

from ..config import FeedConfig
from ..db import DatabaseManager, Download, DownloadStatus
from ..exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
    YtdlpApiError,
)
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class Enqueuer:
    def __init__(self, db_manager: DatabaseManager, ytdlp_wrapper: YtdlpWrapper):
        self.db_manager = db_manager
        self.ytdlp_wrapper = ytdlp_wrapper
        logger.debug("Enqueuer initialized.")

    def _try_bump_retries_and_log(
        self,
        feed_id: str,
        download_id: str,
        error_message: str,
        max_errors: int,
        log_params_base: dict[str, Any],
    ) -> bool:
        """Attempts to bump retries and logs the outcome.

        Returns True if transitioned to ERROR.
        """
        transitioned_to_error_state = False
        try:
            _, _, transitioned_to_error = self.db_manager.bump_retries(
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

    # TODO: if this fails, download is lost forever
    def _upsert_download_in_db(
        self, download: Download, log_params_base: dict[str, Any]
    ) -> None:
        """Upserts a download in DB and logs outcome."""
        try:
            self.db_manager.upsert_download(download)
            logger.debug(
                "Successfully upserted download.", extra=log_params_base
            )  # Changed to debug for less noise on normal ops
        except DatabaseOperationError as e:
            logger.error(
                "Database error upserting download.",
                extra=log_params_base,
                exc_info=e,
            )

    # --- Helpers for _handle_existing_upcoming_downloads ---

    def _get_upcoming_downloads_for_feed(
        self,
        feed_id: str,
        feed_url: str,
    ) -> list[Download]:
        """Fetches UPCOMING downloads for a feed from the database."""
        log_params = {"feed_id": feed_id}
        logger.debug("Fetching upcoming downloads from DB.", extra=log_params)
        try:
            return self.db_manager.get_downloads_by_status(
                DownloadStatus.UPCOMING, feed=feed_id
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
        """Searches for a matching download in the fetched list.

        Logs warnings for mismatches or multiple results.
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
        """Checks if refetched download is VOD and updates DB status to QUEUED.

        Returns True if status updated.
        """
        match refetched_download.status:
            case DownloadStatus.QUEUED:
                logger.info(
                    "Upcoming download has transitioned to VOD (QUEUED). Updating status.",
                    extra=download_log_params,
                )
                try:
                    self.db_manager.mark_as_queued_from_upcoming(
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
        """Processes a single upcoming download, re-fetching metadata and
        updating status if necessary.

        Returns True if the download was successfully transitioned to
        QUEUED.
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
            fetched_downloads = self.ytdlp_wrapper.fetch_metadata(
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
        log_params: dict[str, Any],
    ) -> list[Download]:
        """Fetches all media metadata for the feed URL, applying date filter.

        Raises EnqueueError if the main ytdlp fetch fails.
        """
        current_yt_cli_args = dict(feed_config.yt_args)  # Make a copy
        if fetch_since_date:
            date_str = fetch_since_date.strftime("%Y%m%d")
            current_yt_cli_args["dateafter"] = date_str
            logger.info(
                f"Fetching feed downloads --dateafter {date_str}.", extra=log_params
            )

        try:
            all_fetched_downloads = self.ytdlp_wrapper.fetch_metadata(
                feed_id,
                feed_config.url,
                current_yt_cli_args,
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
        return all_fetched_downloads

    def _handle_newly_fetched_download(
        self, download: Download, log_params: dict[str, Any]
    ) -> bool:
        """Handles a new download by upserting it.

        Returns True if download is QUEUED.
        """
        logger.info(
            "New download found. Inserting.",
            extra=log_params,
        )
        self._upsert_download_in_db(download, log_params)
        return download.status == DownloadStatus.QUEUED

    def _handle_existing_fetched_download(
        self,
        existing_db_download: Download,
        fetched_download: Download,
        feed_id: str,
        log_params: dict[str, Any],
    ) -> bool:
        """Handles an existing download based on status comparison.

        Returns True if download newly QUEUED.
        """
        current_log_params = {
            **log_params,
            "existing_db_status": existing_db_download.status,
        }

        match (existing_db_download.status, fetched_download.status):
            case (DownloadStatus.UPCOMING, DownloadStatus.QUEUED as fetched_status):
                logger.info(
                    f"Existing UPCOMING download has transitioned to VOD ({fetched_status}). Updating status.",
                    extra=current_log_params,
                )
                try:
                    self.db_manager.mark_as_queued_from_upcoming(
                        feed_id, fetched_download.id
                    )
                    return True
                except (DownloadNotFoundError, DatabaseOperationError) as e:
                    raise EnqueueError(
                        "Download became ready to download, but could not update status to QUEUED.",
                        feed_id=feed_id,
                        download_id=fetched_download.id,
                    ) from e
            case (
                DownloadStatus.DOWNLOADED | DownloadStatus.ERROR as existing_status,
                DownloadStatus.QUEUED,
            ):
                logger.debug(
                    f"Existing {existing_status} download set to be requeued.",
                    extra=current_log_params,
                )
                try:
                    self.db_manager.requeue_download(feed_id, fetched_download.id)
                    logger.info(
                        "Successfully re-queued existing DOWNLOADED item.",
                        extra=current_log_params,
                    )
                    return True
                except (DownloadNotFoundError, DatabaseOperationError) as e:
                    raise EnqueueError(
                        "Download was set to be redownloaded, but could not update status to QUEUED.",
                        feed_id=feed_id,
                        download_id=fetched_download.id,
                    ) from e
            case (
                DownloadStatus.UPCOMING as existing_status,
                DownloadStatus.UPCOMING as fetched_status,
            ) | (
                DownloadStatus.QUEUED as existing_status,
                DownloadStatus.QUEUED as fetched_status,
            ):
                logger.debug(
                    f"Existing download status '{existing_status}' matches fetched '{fetched_status}'. No action needed.",
                    extra=current_log_params,
                )
                return False
            case (existing_status, fetched_status):
                logger.info(
                    f"Existing download status '{existing_status}' differs from fetched '{fetched_status}'. Upserting for consistency.",
                    extra=current_log_params,
                )
                self._upsert_download_in_db(fetched_download, current_log_params)
                return fetched_download.status == DownloadStatus.QUEUED

    def _process_single_download(
        self,
        fetched_dl: Download,
        feed_id: str,
        feed_log_params: dict[str, Any],
    ) -> bool:
        """Processes a single fetched download.

        Returns True if it's newly QUEUED.
        """
        log_params = {
            **feed_log_params,
            "download_id": fetched_dl.id,
            "fetched_status": fetched_dl.status,
        }
        logger.debug("Processing fetched download.", extra=log_params)

        try:
            existing_db_download = self.db_manager.get_download_by_id(
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
        """Re-fetches metadata for existing DB entries with status `UPCOMING`.
        If a download is now a VOD, its status is updated to `QUEUED`. If
        metadata re-fetch fails repeatedly (controlled by
        `feed_config.max_errors`), the download's status is transitioned to
        `ERROR`.

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
    ) -> int:
        """Fetches all media metadata for the feed URL after the given date.

        For each download:
        - If new: inserts with status `QUEUED` (if VOD) or `UPCOMING` (if live/scheduled).

        Args:
            feed_id: The unique identifier for the feed.
            feed_config: The configuration object for the feed.
            fetch_since_date: If provided, fetches downloads published after this date.

        Returns:
            The count of downloads newly set to `QUEUED` status (either new VODs or
            `UPCOMING` downloads that transitioned to `QUEUED`).
        """
        feed_log_params = {"feed_id": feed_id, "feed_url": feed_config.url}
        logger.debug(
            "Fetching and processing all feed downloads.",
            extra=feed_log_params,
        )

        all_fetched_downloads = self._fetch_all_metadata_for_feed_url(
            feed_id, feed_config, fetch_since_date, feed_log_params
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
    ) -> int:
        """Fetches media metadata for a given feed and enqueues new downloads
        into the database.

        This method performs two main phases:
        1. Re-polls existing database entries with status `UPCOMING` for the given feed.
           If an `UPCOMING`' download is now a VOD (Video on Demand), its status is updated to `QUEUED`.
        2. Fetches the latest media metadata from the feed source using `YtdlpWrapper`,
           optionally filtered by `fetch_since_date`.
           - For each new download not already in the database:
             - If its parsed status is `QUEUED` (VOD), it's inserted as `QUEUED`.
             - If its parsed status is `UPCOMING` (live/scheduled), it's inserted as `UPCOMING`.
           - For existing `UPCOMING` entries that are now found to be VOD (`QUEUED` status from fetch),
             their status is updated to `QUEUED`.

        Args:
            feed_id: The unique identifier for the feed.
            feed_config: The configuration object for the feed, containing URL and yt-dlp arguments.
            fetch_since_date: fetching will only look for downloads published after this date.

        Returns:
            The total count of downloads that were newly set to `QUEUED` status
            (either new downloads or `UPCOMING` downloads that transitioned to `QUEUED`).

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
            feed_id, feed_config, fetch_since_date
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
