"""Debug mode for testing yt-dlp functionality directly.

This module provides functionality to test yt-dlp operations in isolation.
"""

import logging

from ..config import AppSettings
from ..db import AppStateDatabase
from ..db.sqlalchemy_core import SqlalchemyCore
from ..db.types import DownloadStatus
from ..exceptions import YtdlpApiError
from ..ffprobe import FFProbe
from ..path_manager import PathManager
from ..ytdlp_wrapper import YtdlpWrapper
from ..ytdlp_wrapper.base_handler import HandlerSelector

logger = logging.getLogger(__name__)


async def run_debug_ytdlp_mode(settings: AppSettings, paths: PathManager) -> None:
    """Process feeds in yt-dlp debug mode to test metadata fetching and downloading.

    Tests yt-dlp metadata fetching and downloading for each configured feed.

    Args:
        settings: Application settings containing feed configurations.
        paths: PathManager instance containing data and temporary directories.
    """
    logger.info(
        "Processing feeds in yt-dlp debug mode.",
        extra={
            "feed_count": len(settings.feeds),
            "config_path": str(settings.config_file),
        },
    )

    db_core = SqlalchemyCore(await paths.db_dir())
    app_state_db = AppStateDatabase(db_core)
    handler_selector = HandlerSelector(FFProbe())
    ytdlp_wrapper = YtdlpWrapper(
        paths,
        pot_provider_url=settings.pot_provider_url,
        app_state_db=app_state_db,
        yt_channel=settings.yt_channel,
        yt_update_freq=settings.yt_dlp_update_freq,
        handler_selector=handler_selector,
    )

    for feed_id, feed_config in settings.feeds.items():
        logger.info(
            "Fetching metadata for feed.",
            extra={"feed_url": feed_config.url, "feed_id": feed_id},
        )
        try:
            logger.debug(
                "Calling YtdlpWrapper.fetch_metadata.",
                extra={
                    "feed_id": feed_id,
                    "feed_url": feed_config.url,
                    "yt_cli_args": feed_config.yt_args,
                },
            )
            source_type, resolved_url = await ytdlp_wrapper.discover_feed_properties(
                feed_id, feed_config.url, cookies_path=settings.cookies_path
            )

            # Get feed metadata and downloads separately for debug mode
            feed = await ytdlp_wrapper.fetch_playlist_metadata(
                feed_id=feed_id,
                source_type=source_type,
                source_url=feed_config.url,
                resolved_url=resolved_url,
                user_yt_cli_args=feed_config.yt_args,
                cookies_path=settings.cookies_path,
            )

            downloads = await ytdlp_wrapper.fetch_new_downloads_metadata(
                feed_id=feed_id,
                source_type=source_type,
                source_url=feed_config.url,
                resolved_url=resolved_url,
                user_yt_cli_args=feed_config.yt_args,
                fetch_since_date=feed_config.since,
                keep_last=feed_config.keep_last,
                cookies_path=settings.cookies_path,
            )

            if not downloads:
                logger.info(
                    "No downloads found due to filtering or content unavailability.",
                    extra={"feed_id": feed_id, "feed_url": feed_config.url},
                )
                continue

            if downloads:
                logger.info(
                    "Metadata fetch successful.",
                    extra={
                        "feed_id": feed_id,
                        "feed_title": feed.title,
                        "download_count": len(downloads),
                    },
                )

                downloadable_count = 0
                for i, download in enumerate(downloads[:5]):  # Limit to first 5
                    status_str = (
                        "VOD (ready to download)"
                        if download.status == DownloadStatus.QUEUED
                        else "Live/Upcoming"
                    )
                    logger.info(
                        f"  {i + 1}. ID: {download.id}, Title: {download.title}, "
                        f"Status: {status_str}, Ext: {download.ext}, "
                        f"Duration: {download.duration}s, Published: {download.published.isoformat()}"
                    )

                    # Try to download if it's ready (VOD)
                    if download.status == DownloadStatus.QUEUED:
                        try:
                            logger.info(f"Attempting to download: {download.title}")
                            download_path = await ytdlp_wrapper.download_media_to_file(
                                download,
                                feed_config.yt_args,
                                cookies_path=settings.cookies_path,
                            )
                            logger.info(f"Download successful: {download_path}")
                            downloadable_count += 1
                        except YtdlpApiError as download_error:
                            logger.warning(
                                f"Download failed for {download.title}: {download_error}",
                                extra={"download_id": download.id},
                            )
                        except Exception as download_error:
                            logger.warning(
                                f"Unexpected download error for {download.title}: {download_error}",
                                extra={"download_id": download.id},
                            )

                if len(downloads) > 5:
                    logger.info(f"  ... and {len(downloads) - 5} more downloads.")

                logger.info(
                    f"Successfully downloaded {downloadable_count} out of {min(5, len(downloads))} attempted downloads."
                )

            else:
                logger.warning(
                    "No downloads found in feed metadata.",
                    extra={"feed_id": feed_id},
                )

        except YtdlpApiError as e:
            logger.error(
                "yt-dlp failed to extract metadata for feed.",
                extra={
                    "feed_id": feed_id,
                    "feed_url": feed_config.url,
                    "yt_cli_args": feed_config.yt_args,
                },
                exc_info=e,
            )
        except Exception as e:
            logger.error(
                "Unexpected error processing feed. See application logs for details.",
                exc_info=e,
                extra={
                    "feed_url": feed_config.url,
                    "feed_id": feed_id,
                },
            )

    logger.info("Debug mode processing complete.")
