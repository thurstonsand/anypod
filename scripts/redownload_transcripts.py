#!/usr/bin/env python3
"""Re-download transcripts for all existing DOWNLOADED videos.

This script queries the database for all downloads with DOWNLOADED status
that have transcript metadata (transcript_source is not null and not NOT_AVAILABLE),
then calls the admin API to refresh their transcripts using the new
youtube-transcript-api implementation.

Usage:
    uv run scripts/redownload_transcripts.py [--admin-url URL] [--feed-id FEED_ID] [--dry-run]

Environment:
    Expects the anypod service to be running with admin API accessible.
"""

import argparse
import asyncio
import logging
from pathlib import Path
import sys

import httpx

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT / "src"))

from anypod.db.download_db import DownloadDatabase  # noqa: E402
from anypod.db.sqlalchemy_core import SqlalchemyCore  # noqa: E402
from anypod.db.types import DownloadStatus, TranscriptSource  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)


async def get_downloads_with_transcripts(
    download_db: DownloadDatabase,
    feed_id: str | None = None,
) -> list[tuple[str, str, str | None, TranscriptSource | None]]:
    """Query downloads that have transcript metadata.

    Args:
        download_db: Database access object.
        feed_id: Optional filter to specific feed.

    Returns:
        List of (feed_id, download_id, transcript_lang, transcript_source) tuples.
    """
    downloads = await download_db.get_downloads_by_status(
        status_to_filter=DownloadStatus.DOWNLOADED,
        feed_id=feed_id,
        limit=-1,
    )

    results: list[tuple[str, str, str | None, TranscriptSource | None]] = []
    for d in downloads:
        if (
            d.transcript_source
            and d.transcript_source != TranscriptSource.NOT_AVAILABLE
        ):
            results.append((d.feed_id, d.id, d.transcript_lang, d.transcript_source))

    return results


async def refresh_transcript_via_api(
    client: httpx.AsyncClient,
    admin_url: str,
    feed_id: str,
    download_id: str,
) -> tuple[bool, str]:
    """Call admin API to refresh transcript for a download.

    Args:
        client: HTTP client.
        admin_url: Base URL for admin API.
        feed_id: Feed identifier.
        download_id: Download identifier.

    Returns:
        Tuple of (success, message).
    """
    url = f"{admin_url}/admin/feeds/{feed_id}/downloads/{download_id}/refresh-metadata"
    try:
        response = await client.post(
            url,
            json={"refresh_transcript": True},
            timeout=60.0,
        )
        if response.status_code == 200:
            data = response.json()
            return True, f"transcript_refreshed={data.get('transcript_refreshed')}"
        return False, f"HTTP {response.status_code}: {response.text}"
    except httpx.RequestError as e:
        return False, f"Request failed: {e}"


async def main() -> int:
    """Entry point for transcript re-download script."""
    parser = argparse.ArgumentParser(
        description="Re-download transcripts for existing downloads."
    )
    parser.add_argument(
        "--admin-url",
        default="http://localhost:8025",
        help="Admin API base URL (default: http://localhost:8025)",
    )
    parser.add_argument(
        "--data-dir",
        default=str(PROJECT_ROOT / "tmpdata" / "db"),
        help="Path to database directory (default: tmpdata/db)",
    )
    parser.add_argument(
        "--feed-id",
        help="Only process downloads from this feed",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="List downloads without making changes",
    )
    parser.add_argument(
        "--concurrency",
        type=int,
        default=3,
        help="Number of concurrent API requests (default: 3)",
    )
    args = parser.parse_args()

    db_path = Path(args.data_dir)
    if not (db_path / "anypod.db").exists():
        logger.error("Database not found at %s", db_path / "anypod.db")
        return 1

    db_core = SqlalchemyCore(db_path)
    download_db = DownloadDatabase(db_core)

    try:
        downloads = await get_downloads_with_transcripts(download_db, args.feed_id)
    finally:
        await db_core.close()

    if not downloads:
        logger.info("No downloads with transcripts found.")
        return 0

    logger.info("Found %d downloads with transcripts to refresh.", len(downloads))

    if args.dry_run:
        for feed_id, download_id, lang, source in downloads:
            logger.info(
                "  [DRY RUN] %s/%s (lang=%s, source=%s)",
                feed_id,
                download_id,
                lang,
                source,
            )
        return 0

    semaphore = asyncio.Semaphore(args.concurrency)
    success_count = 0
    failure_count = 0

    async def process_download(
        client: httpx.AsyncClient,
        feed_id: str,
        download_id: str,
        lang: str | None,
        source: TranscriptSource | None,
    ) -> None:
        nonlocal success_count, failure_count
        async with semaphore:
            success, message = await refresh_transcript_via_api(
                client, args.admin_url, feed_id, download_id
            )
            if success:
                success_count += 1
                logger.info(
                    "✓ %s/%s (lang=%s, source=%s): %s",
                    feed_id,
                    download_id,
                    lang,
                    source,
                    message,
                )
            else:
                failure_count += 1
                logger.warning(
                    "✗ %s/%s (lang=%s, source=%s): %s",
                    feed_id,
                    download_id,
                    lang,
                    source,
                    message,
                )

    async with httpx.AsyncClient() as client:
        tasks = [
            process_download(client, feed_id, download_id, lang, source)
            for feed_id, download_id, lang, source in downloads
        ]
        await asyncio.gather(*tasks)

    logger.info(
        "Completed: %d successful, %d failed out of %d total.",
        success_count,
        failure_count,
        len(downloads),
    )

    return 0 if failure_count == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
