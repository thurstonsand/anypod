"""Debug mode for testing yt-dlp functionality directly.

This module provides functionality to test yt-dlp operations in isolation,
loading configuration from a YAML file and fetching metadata or downloading
media based on the configuration.
"""

import logging
from pathlib import Path
import sys

import yaml

from ..db.types import DownloadStatus
from ..exceptions import YtdlpApiError
from ..path_manager import PathManager
from ..ytdlp_wrapper import YtdlpWrapper

# Import Download for potential type hinting if we pretty print, though not strictly needed if just printing raw output.
# from ..db import Download

logger = logging.getLogger(__name__)


async def run_debug_ytdlp_mode(debug_yaml_path: Path, paths: PathManager) -> None:
    """Load feed URLs from YAML and fetch metadata using yt-dlp.

    Loads feed URLs and yt-dlp CLI args from a YAML file and fetches metadata.
    Optionally downloads media if enabled in the configuration.

    Args:
        debug_yaml_path: Path to the YAML configuration file.
        paths: PathManager instance containing data and temporary directories.
    """
    logger.debug(
        "Entered yt-dlp debug mode execution.",
        extra={"debug_yaml_path": str(debug_yaml_path)},
    )

    if not debug_yaml_path.exists():
        logger.critical(
            "Debug YAML file not found at specified path, cannot proceed with debug mode. Exiting.",
            extra={"config_path": str(debug_yaml_path)},
        )
        sys.exit(1)

    try:
        with debug_yaml_path.open("r") as f:
            config = yaml.safe_load(f)
    except Exception:
        logger.critical(
            "Failed to load or parse debug YAML file.",
            exc_info=True,
            extra={"config_path": str(debug_yaml_path)},
        )
        sys.exit(1)

    if config is None:  # Handle case where YAML is valid but empty or only comments
        logger.critical(
            "Debug YAML file is empty or invalid (parsed as None). Cannot proceed.",
            extra={"config_path": str(debug_yaml_path)},
        )
        sys.exit(1)

    cli_args: list[str] = config.get("cli_args", [])
    feed_urls_dict: dict[str, str] = config.get("feeds", [])
    should_download: bool = config.get("download", False)

    if not feed_urls_dict:
        logger.info(
            "No feed URLs found in debug configuration. Nothing to process.",
            extra={"config_path": str(debug_yaml_path)},
        )
        return

    ytdlp_wrapper = YtdlpWrapper(paths)
    logger.debug("YtdlpWrapper initialized for debug mode.")

    logger.info(
        "Processing feeds in yt-dlp debug mode.",
        extra={
            "feed_count": len(feed_urls_dict),
            "cli_args_used": cli_args if cli_args else "default_wrapper_options",
            "config_path": str(debug_yaml_path),
        },
    )

    # Define download directory relative to project root (assuming script is run from root or similar)
    project_root = (
        Path(__file__).resolve().parents[3]
    )  # Adjust index based on actual structure
    download_dir = project_root / "debug_downloads"

    if should_download:
        try:
            download_dir.mkdir(parents=True, exist_ok=True)
            logger.info(
                "Downloading is enabled.",
                extra={"download_directory": str(download_dir)},
            )
        except OSError:
            logger.critical(
                "Failed to create download directory.",
                exc_info=True,
                extra={"directory_path": str(download_dir)},
            )
            sys.exit(1)
    else:
        logger.info("Downloading is disabled. Fetching metadata only.")

    for feed_id, url in feed_urls_dict.items():
        # Create a simple feed name for context. This is not a stored feed_id from a DB.
        logger.info(
            "Fetching metadata for feed.",
            extra={"feed_url": url, "feed_id": feed_id},
        )
        try:
            logger.debug(
                "Calling YtdlpWrapper.fetch_metadata.",
                extra={
                    "feed_id": feed_id,
                    "feed_url": url,
                    "yt_cli_args": cli_args,
                },
            )
            feed, downloads = await ytdlp_wrapper.fetch_metadata(
                feed_id=feed_id,
                url=url,
                user_yt_cli_args=cli_args,
            )

            if downloads:
                logger.info(
                    "Successfully fetched downloads for feed.",
                    extra={
                        "download_count": len(downloads),
                        "feed_url": url,
                        "feed_title": feed.title,
                        "feed_author": feed.author,
                    },
                )
                for download in downloads:
                    if should_download:
                        # Only attempt to download if the download is actually downloadable (not upcoming/live)
                        if download.status == DownloadStatus.QUEUED:
                            logger.info(
                                "Attempting to download.",
                                extra={
                                    "feed_url": url,
                                    "feed_id": feed_id,
                                    "download_title": download.title,
                                    "download_id": download.id,
                                },
                            )
                            try:
                                file_path = await ytdlp_wrapper.download_media_to_file(
                                    download=download,
                                    user_yt_cli_args=cli_args,
                                )
                                logger.info(
                                    "Successfully downloaded.",
                                    extra={
                                        "feed_url": url,
                                        "feed_id": feed_id,
                                        "download_title": download.title,
                                        "download_id": download.id,
                                        "file_path": file_path,
                                    },
                                )
                            except YtdlpApiError as e:
                                logger.error(
                                    "Failed to download.",
                                    exc_info=e,
                                    extra={
                                        "feed_url": url,
                                        "feed_id": feed_id,
                                        "download_title": download.title,
                                        "download_id": download.id,
                                    },
                                )
                        else:
                            logger.info(
                                "Skipping download due to status.",
                                extra={
                                    "feed_url": url,
                                    "feed_id": feed_id,
                                    "download_title": download.title,
                                    "download_id": download.id,
                                    "download_status": download.status,
                                },
                            )
                    else:
                        logger.info(
                            "Fetched download details.",
                            extra={
                                "feed_url": url,
                                "feed_id": feed_id,
                                "download_title": download.title,
                                "download_id": download.id,
                                "download_source_url": download.source_url,
                                "download_published": download.published.isoformat()
                                if download.published
                                else "N/A",
                                "download_duration_s": download.duration,
                                "download_ext": download.ext,
                                "download_thumbnail": download.thumbnail or "N/A",
                                "download_status": download.status,
                                "download_quality_info": download.quality_info or "N/A",
                            },
                        )
            else:
                logger.info(
                    "No downloads found or parsed for feed.",
                    extra={"feed_url": url, "feed_id": feed_id},
                )
        except RuntimeError:
            logger.error(
                "Error processing feed. See application logs for details.",
                exc_info=True,
                extra={"feed_url": url, "feed_id": feed_id},
            )
        except Exception:
            logger.error(
                "Unexpected error processing feed. See application logs for details.",
                exc_info=True,
                extra={"feed_url": url, "feed_id": feed_id},
            )

    logger.info("Debug mode processing complete.")
