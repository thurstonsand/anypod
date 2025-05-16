from datetime import datetime
import logging
import shlex

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

    def _handle_existing_upcoming_downloads(
        self, feed_name: str, feed_config: FeedConfig, yt_cli_args: list[str]
    ) -> int:
        """
        Re-fetches metadata for existing DB entries with status `UPCOMING`.
        If a download is now a VOD, its status is updated to `QUEUED`.
        If metadata re-fetch fails repeatedly (controlled by `feed_config.max_errors`),
        the download's status is transitioned to `ERROR`.

        Args:
            feed_name: The unique identifier for the feed.
            feed_config: The configuration object for the feed.
            yt_cli_args: Parsed yt-dlp CLI arguments for the feed.

        Returns:
            The count of downloads successfully transitioned from 'upcoming' to 'queued'.
        """
        queued_count = 0
        log_params = {"feed_name": feed_name}
        logger.debug("Handling existing upcoming downloads.", extra=log_params)

        try:
            upcoming_db_downloads = self.db_manager.get_downloads_by_status(
                status_to_filter=DownloadStatus.UPCOMING, feed=feed_name
            )
        except (DatabaseOperationError, ValueError) as e:
            raise EnqueueError(
                "Could not fetch upcoming downloads.",
                feed_name=feed_name,
                feed_url=feed_config.url,
            ) from e

        if not upcoming_db_downloads:
            logger.debug("No existing upcoming downloads to process.", extra=log_params)
            return 0

        logger.info(
            f"Found {len(upcoming_db_downloads)} existing upcoming downloads to re-check.",
            extra=log_params,
        )

        for db_download in upcoming_db_downloads:
            download_log_params = {
                **log_params,
                "download_id": db_download.id,
                "source_url": db_download.source_url,
            }
            logger.debug(
                "Re-checking status for upcoming download.", extra=download_log_params
            )

            # if this fails enough times, it should be marked as error
            try:
                fetched_downloads: list[Download] = self.ytdlp_wrapper.fetch_metadata(
                    feed_name=feed_name,
                    url=db_download.source_url,
                    yt_cli_args=yt_cli_args,
                )
            except YtdlpApiError as e:
                logger.warning(
                    "Could not re-fetch metadata for an upcoming download. It might still be upcoming or have issues.",
                    extra=download_log_params,
                    exc_info=e,
                )
                # This is a fetch failure, let's bump retries
                error_message = "Failed to re-fetch metadata for upcoming download during periodic check."
                log_params_bump_err = {
                    **download_log_params,
                    "error_message": error_message,
                }
                try:
                    _, _, transitioned_to_error = self.db_manager.bump_retries(
                        feed_id=feed_name,
                        download_id=db_download.id,
                        error_message=error_message,
                        max_allowed_errors=feed_config.max_errors,
                    )
                    if transitioned_to_error:
                        logger.warning(
                            "Upcoming download transitioned to ERROR due to repeated metadata re-fetch failures.",
                            extra=log_params_bump_err,
                        )
                    else:
                        logger.debug(
                            "Incremented error count for upcoming download due to metadata re-fetch failure.",
                            extra=log_params_bump_err,
                        )
                except (DownloadNotFoundError, DatabaseOperationError) as db_err:
                    logger.warning(
                        "Could not bump error count for upcoming download.",
                        extra=log_params_bump_err,
                        exc_info=db_err,
                    )
                continue  # Whether bumped or not, if fetch failed, move to next

            if not fetched_downloads:
                logger.warning(
                    "Metadata fetch for existing upcoming download returned no results. Download might be removed or inaccessible.",
                    extra=download_log_params,
                )
                error_message = (
                    "Metadata re-fetch for upcoming item returned no results."
                )
                log_params_bump_err = {
                    **download_log_params,
                    "error_message": error_message,
                }
                try:
                    _, _, transitioned_to_error = self.db_manager.bump_retries(
                        feed_id=feed_name,
                        download_id=db_download.id,
                        error_message=error_message,
                        max_allowed_errors=feed_config.max_errors,
                    )
                    if transitioned_to_error:
                        logger.warning(
                            "Upcoming download transitioned to ERROR: metadata re-fetch yielded no results.",
                            extra=log_params_bump_err,
                        )
                    else:
                        logger.debug(
                            "Incremented error count for upcoming download: metadata re-fetch yielded no results.",
                            extra=log_params_bump_err,
                        )
                except (DownloadNotFoundError, DatabaseOperationError) as db_err:
                    logger.warning(
                        "Could not bump error count for upcoming download.",
                        extra=log_params_bump_err,
                        exc_info=db_err,
                    )
                continue

            refetched_download: Download | None = None
            # Expected case
            if (
                len(fetched_downloads) == 1
                and fetched_downloads[0].id == db_download.id
                and fetched_downloads[0].feed == feed_name
            ):
                refetched_download = fetched_downloads[0]
            # if there's a mismatch between ytdlp data and db, it's an error
            elif len(fetched_downloads) == 1:
                logger.warning(
                    f"Re-fetched single download ID '{fetched_downloads[0].id}' does not match expected DB ID '{db_download.id}' for URL {db_download.source_url}. Skipping.",
                    extra=download_log_params,
                )
            elif len(fetched_downloads) > 1:
                logger.warning(
                    f"Metadata re-fetch for download URL {db_download.source_url} returned multiple results ({len(fetched_downloads)}). Searching for original ID '{db_download.id}'.",
                    extra=download_log_params,
                )
                for download_item in fetched_downloads:
                    if (
                        download_item.id == db_download.id
                        and download_item.feed == feed_name
                    ):
                        refetched_download = download_item
                        break

            if not refetched_download:
                logger.warning(
                    f"Could not find matching download (ID: {db_download.id}) in re-fetched metadata for URL {db_download.source_url}. Original item might have been removed or changed ID.",
                    extra=download_log_params,
                )
                error_message = (
                    "Original ID not found in re-fetched metadata for upcoming item."
                )
                log_params_bump_err = {
                    **download_log_params,
                    "error_message": error_message,
                }
                try:
                    _, _, transitioned_to_error = self.db_manager.bump_retries(
                        feed_id=feed_name,
                        download_id=db_download.id,
                        error_message=error_message,
                        max_allowed_errors=feed_config.max_errors,
                    )
                    if transitioned_to_error:
                        logger.warning(
                            "Upcoming download transitioned to ERROR: original ID not found in re-fetched metadata.",
                            extra=log_params_bump_err,
                        )
                    else:
                        logger.debug(
                            "Incremented error count for upcoming download: original ID not found in re-fetched metadata.",
                            extra=log_params_bump_err,
                        )

                except (DownloadNotFoundError, DatabaseOperationError) as db_err:
                    logger.warning(
                        "Could not bump error count for upcoming download.",
                        extra=log_params_bump_err,
                        exc_info=db_err,
                    )
                continue

            if refetched_download.status == DownloadStatus.QUEUED:
                logger.info(
                    "Phase 1: Upcoming download has transitioned to VOD (QUEUED). Updating status.",
                    extra=download_log_params,
                )
                try:
                    updated = self.db_manager.update_status(
                        feed=feed_name,
                        id=db_download.id,
                        status=DownloadStatus.QUEUED,
                    )
                except DatabaseOperationError as e:
                    logger.error(
                        "Database error updating status to QUEUED for an upcoming download.",
                        extra=download_log_params,
                        exc_info=e,
                    )
                    continue

                if updated:
                    queued_count += 1
                    logger.info(
                        "Successfully updated status to QUEUED.",
                        extra=download_log_params,
                    )
                else:
                    logger.warning(
                        "Failed to update status to QUEUED (DB row not changed).",
                        extra=download_log_params,
                    )
            elif refetched_download.status == DownloadStatus.UPCOMING:
                logger.info(
                    f"Re-fetched download status is still {DownloadStatus.UPCOMING}. No change needed.",
                    extra=download_log_params,
                )
            else:  # the only valid statuses should be QUEUED or UPCOMING
                logger.info(
                    f"Re-fetched upcoming download has unexpected status: {refetched_download.status}. Skipping.",
                    extra=download_log_params,
                )
                continue

        return queued_count

    def _fetch_and_process_feed_downloads(
        self,
        feed_name: str,
        feed_config: FeedConfig,
        yt_cli_args: list[str],
        fetch_since_date: datetime,
    ) -> int:
        """
        Fetches all media metadata for the feed URL filtered by date.
        For each download:
        - If new: inserts with status `QUEUED` (if VOD) or `UPCOMING`.
        - If existing `UPCOMING` and now VOD: updates status to `QUEUED`.

        Args:
            feed_name: The unique identifier for the feed.
            feed_config: The configuration object for the feed.
            yt_cli_args: Parsed yt-dlp CLI arguments for the feed.
            fetch_since_date: If provided, fetches items published after this date.

        Returns:
            The count of downloads newly set to `QUEUED` status (either new VODs or
            `UPCOMING` downloads that transitioned to `QUEUED`).
        """
        log_params = {"feed_name": feed_name, "feed_url": feed_config.url}
        logger.debug(
            "Fetching and processing all feed downloads.",
            extra=log_params,
        )

        current_yt_cli_args = list(yt_cli_args)
        if fetch_since_date:
            date_str = fetch_since_date.strftime("%Y%m%d")
            current_yt_cli_args.extend(["--dateafter", date_str])
            logger.info(
                f"Fetching feed items --dateafter {date_str}.", extra=log_params
            )

        try:
            all_fetched_downloads: list[Download] = self.ytdlp_wrapper.fetch_metadata(
                feed_name,
                feed_config.url,
                current_yt_cli_args,
            )
        except YtdlpApiError as e:
            raise EnqueueError(
                "Could not fetch main feed metadata.",
                feed_name=feed_name,
                feed_url=feed_config.url,
            ) from e

        if not all_fetched_downloads:
            logger.debug(
                "No downloads returned from feed metadata fetch.",
                extra=log_params,
            )
            return 0

        logger.debug(
            f"Fetched {len(all_fetched_downloads)} downloads from feed URL.",
            extra=log_params,
        )

        queued_count = 0
        for fetched_dl in all_fetched_downloads:
            download_log_params = {
                **log_params,
                "download_id": fetched_dl.id,
                "fetched_status": fetched_dl.status,
            }
            logger.debug("Processing fetched download.", extra=download_log_params)
            try:
                existing_db_download = self.db_manager.get_download_by_id(
                    feed_name, fetched_dl.id
                )
            except DatabaseOperationError as e:
                logger.error(
                    "Database error checking for existing download.",
                    extra=download_log_params,
                    exc_info=e,
                )
                continue

            if existing_db_download is None:
                logger.info(
                    "New download found. Inserting.",
                    extra=download_log_params,
                )
                try:
                    self.db_manager.upsert_download(fetched_dl)
                except DatabaseOperationError as e:
                    logger.error(
                        "Database error inserting new download.",
                        extra=download_log_params,
                        exc_info=e,
                    )
                else:
                    if fetched_dl.status == DownloadStatus.QUEUED:
                        queued_count += 1
            else:
                download_log_params["existing_db_status"] = existing_db_download.status
                match (existing_db_download.status, fetched_dl.status):
                    case (DownloadStatus.UPCOMING, DownloadStatus.QUEUED):
                        logger.info(
                            "Existing UPCOMING download has transitioned to VOD (QUEUED). Updating status.",
                            extra=download_log_params,
                        )
                        try:
                            updated = self.db_manager.update_status(
                                feed=feed_name,
                                id=fetched_dl.id,
                                status=DownloadStatus.QUEUED,
                            )
                        except DatabaseOperationError as e:
                            logger.error(
                                "Failed to update status to QUEUED.",
                                extra=download_log_params,
                                exc_info=e,
                            )
                        else:
                            if updated:
                                queued_count += 1
                            else:
                                logger.warning(
                                    "Failed to update status to QUEUED (DB row not changed).",
                                    extra=download_log_params,
                                )
                    case (DownloadStatus.UPCOMING, DownloadStatus.UPCOMING) | (
                        DownloadStatus.QUEUED,
                        DownloadStatus.QUEUED,
                    ):
                        logger.debug(
                            f"Existing download status '{existing_db_download.status}' matches fetched '{fetched_dl.status}'. No action needed.",
                            extra=download_log_params,
                        )
                    case (DownloadStatus.DOWNLOADED, _):
                        logger.debug(
                            f"Existing download already {DownloadStatus.DOWNLOADED}. Skipping.",
                            extra=download_log_params,
                        )
                    case _:
                        logger.info(
                            f"Existing download status '{existing_db_download.status}' differs from fetched '{fetched_dl.status}'. Upserting for consistency.",
                            extra=download_log_params,
                        )
                        try:
                            self.db_manager.upsert_download(fetched_dl)
                            if (
                                existing_db_download.status != DownloadStatus.QUEUED
                                and fetched_dl.status == DownloadStatus.QUEUED
                            ):
                                queued_count += 1
                                logger.debug(
                                    "Upsert resulted in QUEUED status, incremented count.",
                                    extra=download_log_params,
                                )
                        except DatabaseOperationError as e:
                            logger.error(
                                "Failed to upsert download for status consistency.",
                                extra=download_log_params,
                                exc_info=e,
                            )

        logger.debug(
            f"Identified {queued_count} downloads as newly QUEUED.",
            extra=log_params,
        )
        return queued_count

    def enqueue_new_downloads(
        self,
        feed_name: str,
        feed_config: FeedConfig,
        fetch_since_date: datetime,
    ) -> int:
        """
        Fetches media metadata for a given feed and enqueues new downloads into the database.

        This method performs two main phases:
        1. Re-polls existing database entries with status 'upcoming' for the given feed.
           If an 'upcoming' download is now a VOD (Video on Demand), its status is updated to 'queued'.
        2. Fetches the latest media metadata from the feed source using `YtdlpWrapper`,
           optionally filtered by `fetch_since_date`.
           - For each new download not already in the database:
             - If its parsed status is `QUEUED` (VOD), it's inserted as `QUEUED`.
             - If its parsed status is `UPCOMING` (live/scheduled), it's inserted as `UPCOMING`.
           - For existing 'upcoming' entries that are now found to be VOD (`QUEUED` status from fetch),
             their status is updated to `QUEUED`.

        Args:
            feed_name: The unique identifier for the feed.
            feed_config: The configuration object for the feed, containing URL and yt-dlp arguments.
            fetch_since_date: fetching will only look for items published after this date.

        Returns:
            The total count of downloads that were newly set to `QUEUED` status
            (either new downloads or `UPCOMING` downloads that transitioned to `QUEUED`).

        Raises:
            EnqueueError: If a critical, non-recoverable error occurs during the enqueue process.
                          This wraps underlying YtdlpApiError or DatabaseOperationError.
        """
        total_newly_queued_count = 0
        log_params = {"feed_name": feed_name, "feed_url": feed_config.url}
        logger.info("Starting enqueue_new_downloads process.", extra=log_params)

        # TODO: is this needed or can ytdlp just accept the string?
        yt_cli_args_list = shlex.split(feed_config.yt_args or "")

        total_newly_queued_count += self._handle_existing_upcoming_downloads(
            feed_name, feed_config, yt_cli_args_list
        )

        total_newly_queued_count += self._fetch_and_process_feed_downloads(
            feed_name, feed_config, yt_cli_args_list, fetch_since_date
        )

        logger.info(
            f"Enqueue process completed. Total newly or transitioned to QUEUED: {total_newly_queued_count}",
            extra=log_params,
        )
        return total_newly_queued_count
