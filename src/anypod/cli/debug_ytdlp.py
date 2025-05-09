import logging
from pathlib import Path
import reprlib
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
    reprlib.aRepr.maxstring = 10_000_000
    reprlib.aRepr.maxother = 10_000_000
    if not debug_yaml_path.exists():
        # Still good to have a clean exit if the file is missing
        logger.error(f"Debug YAML file not found: {debug_yaml_path}")
        print(f"Error: Debug YAML file not found at {debug_yaml_path}", file=sys.stderr)
        sys.exit(1)

    # Simplified: directly open and load, will crash if issues occur
    with debug_yaml_path.open("r") as f:
        config = yaml.safe_load(f)

    # Assume config is not None and has the correct structure
    cli_args: list[str] = config.get("cli_args", [])
    feed_urls: list[str] = config.get("feeds", [])

    if not feed_urls:
        logger.warning(
            f"No feed URLs found in '{debug_yaml_path}'. Nothing to process."
        )
        print("No feed URLs provided in the debug YAML. Exiting.")
        return

    ytdlp_wrapper = YtdlpWrapper()

    print(
        f"Processing {len(feed_urls)} feed(s) with yt-dlp args: {cli_args if cli_args else '[default wrapper options]'}"
    )

    for i, url in enumerate(feed_urls):
        feed_name = f"debug_feed_{i + 1}_{Path(url).name}"  # Create a simple feed name for context
        print(f"\n--- Fetching metadata for: {url} (as feed: {feed_name}) ---")
        try:
            downloads = ytdlp_wrapper.fetch_metadata(
                feed_name=feed_name, url=url, yt_cli_args=cli_args
            )

            if downloads:
                print(f"Successfully fetched {len(downloads)} item(s) for {url}:")
                for download_item in downloads:
                    print(f"  - Title: {download_item.title}")
                    print(f"    ID: {download_item.id}")
                    print(f"    Source URL: {download_item.source_url}")
                    print(
                        f"    Published: {download_item.published.isoformat() if download_item.published else 'N/A'}"
                    )
                    print(f"    Duration: {download_item.duration}s")
                    print(f"    Ext: {download_item.ext}")
                    print(f"    Thumbnail: {download_item.thumbnail or 'N/A'}")
                    print(f"    Status: {download_item.status}")
            else:
                print(
                    f"No downloadable items found or parsed for {url}. Check logs for details."
                )
        except RuntimeError as e:
            logger.error(
                f"A critical runtime error occurred while processing {url}: {e}",
                exc_info=True,
            )
            print(f"Critical Error processing {url}: {e}. See logs.", file=sys.stderr)
        except Exception as e:
            logger.error(
                f"An unexpected error occurred while processing {url}: {e}",
                exc_info=True,
            )
            print(f"Unexpected Error processing {url}: {e}. See logs.", file=sys.stderr)

    print("\nDebug processing complete.")
