import logging
from pathlib import Path
import sys

import yaml

from ..ytdlp_wrapper import YtdlpWrapper

# Import Download for potential type hinting if we pretty print, though not strictly needed if just printing raw output.
# from ..db import Download

logger = logging.getLogger(__name__)


def run_debug_ytdlp_mode(debug_yaml_path: Path) -> None:
    """Loads feed URLs and yt-dlp CLI args from a YAML file and fetches metadata.
    Assumes the YAML file is correctly formatted for debugging purposes.
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
    feed_urls: list[str] = config.get("feeds", [])

    if not feed_urls:
        logger.info(
            "No feed URLs found in debug configuration. Nothing to process.",
            extra={"config_path": str(debug_yaml_path)},
        )
        return

    ytdlp_wrapper = YtdlpWrapper()
    logger.debug("YtdlpWrapper initialized for debug mode.")

    logger.info(
        "Processing feeds in yt-dlp debug mode.",
        extra={
            "feed_count": len(feed_urls),
            "cli_args_used": cli_args if cli_args else "default_wrapper_options",
            "config_path": str(debug_yaml_path),
        },
    )

    for i, url in enumerate(feed_urls):
        # Create a simple feed name for context. This is not a stored feed_id from a DB.
        feed_name = f"debug_feed_{i + 1}_{Path(url).name}"
        logger.info(
            "Fetching metadata for feed.",
            extra={"feed_url": url, "feed_name": feed_name},
        )
        try:
            logger.debug(
                "Calling YtdlpWrapper.fetch_metadata.",
                extra={
                    "feed_name": feed_name,
                    "feed_url": url,
                    "yt_cli_args": cli_args,
                },
            )
            downloads = ytdlp_wrapper.fetch_metadata(
                feed_name=feed_name, url=url, yt_cli_args=cli_args
            )

            if downloads:
                logger.info(
                    "Successfully fetched items for feed.",
                    extra={"item_count": len(downloads), "feed_url": url},
                )
                for download_item in downloads:
                    logger.info(
                        "Fetched item details.",
                        extra={
                            "feed_url": url,
                            "feed_name": feed_name,
                            "item_title": download_item.title,
                            "item_id": download_item.id,
                            "item_source_url": download_item.source_url,
                            "item_published": download_item.published.isoformat()
                            if download_item.published
                            else "N/A",
                            "item_duration_s": download_item.duration,
                            "item_ext": download_item.ext,
                            "item_thumbnail": download_item.thumbnail or "N/A",
                            "item_status": download_item.status,
                        },
                    )
            else:
                logger.info(
                    "No downloadable items found or parsed for feed.",
                    extra={"feed_url": url, "feed_name": feed_name},
                )
        except RuntimeError:
            logger.error(
                "Error processing feed. See application logs for details.",
                exc_info=True,
                extra={"feed_url": url, "feed_name": feed_name},
            )
        except Exception:
            logger.error(
                "Unexpected error processing feed. See application logs for details.",
                exc_info=True,
                extra={"feed_url": url, "feed_name": feed_name},
            )

    logger.info("Debug mode processing complete.")
