import logging

from ..config import FeedConfig
from ..db import DatabaseManager
from ..file_manager import FileManager
from ..ytdlp_wrapper import YtdlpWrapper

logger = logging.getLogger(__name__)


class Downloader:
    def __init__(
        self,
        db_manager: DatabaseManager,
        file_manager: FileManager,
        ytdlp_wrapper: YtdlpWrapper,
    ):
        self.db_manager = db_manager
        self.file_manager = file_manager
        self.ytdlp_wrapper = ytdlp_wrapper
        logger.debug("Downloader initialized.")

    def download_queued(
        self,
        feed_id: str,
        feed_config: FeedConfig,
        limit: int = 0,
    ) -> tuple[int, int]:
        """Processes and downloads media items that are in the 'queued' status for a specific feed.

        This method retrieves 'queued' Download objects from the database. For each item:
        1. It attempts to download the media content using `YtdlpWrapper.download_media_to_file()`,
           saving it to a temporary location.
        2. If the download is successful, the media file is moved from the temporary location to its
           final permanent storage using `FileManager.save_download_file()`.
        3. The database record for the Download is updated to 'downloaded' status, along with any
           new metadata (e.g., file extension, filesize) obtained during the download.
        4. If any step fails (download, file move, DB update), the Download's status is set to 'error',
           the error message is logged, and its retry count is incremented in the database.
        5. Temporary files are cleaned up regardless of the outcome for the individual download.

        Args:
            feed_id: The unique identifier for the feed whose queued items are to be processed.
            feed_config: The configuration object for the feed, containing yt-dlp arguments.
            limit: The maximum number of queued items to process in this run.
                   If 0 (default), processes all queued items for the feed.

        Returns:
            A tuple containing two integers: (success_count, failure_count), representing the number
            of downloads successfully processed and the number that failed, respectively.

        Raises:
            DatabaseOperationError: If a critical database operation fails (e.g., fetching queued items).
            YtdlpApiError: If a yt-dlp related error occurs that is not handled per-download.
            FileOperationError: If a critical file system operation fails that is not handled per-download.
        """
        # Business logic to be implemented
        logger.info(
            "download_queued called (stub).",
            extra={
                "feed_id": feed_id,
                "feed_url": feed_config.url,
                "limit": limit,
            },
        )
        return 0, 0
