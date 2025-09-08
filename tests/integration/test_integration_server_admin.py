# pyright: reportPrivateUsage=false

"""Integration tests for the admin HTTP server with real dependencies."""

from datetime import UTC, datetime

from fastapi.testclient import TestClient
import pytest

from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType

# admin_test_app fixture provided by tests/integration/conftest.py


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admin_reset_errors_happy_path(
    admin_test_app: TestClient, feed_db: FeedDatabase, download_db: DownloadDatabase
) -> None:
    """Resets all ERROR items to QUEUED for a feed and returns count."""
    feed_id = "int_admin_reset"
    # Create feed
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.CHANNEL,
            source_url="https://example.com/channel",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        )
    )
    # Add downloads: two ERROR, one SKIPPED
    await download_db.upsert_download(
        Download(
            feed_id=feed_id,
            id="err1",
            source_url="u",
            title="t",
            published=datetime(2024, 1, 1, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1,
            duration=1,
            status=DownloadStatus.ERROR,
            retries=2,
            last_error="boom",
        )
    )
    await download_db.upsert_download(
        Download(
            feed_id=feed_id,
            id="err2",
            source_url="u",
            title="t",
            published=datetime(2024, 1, 2, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1,
            duration=1,
            status=DownloadStatus.ERROR,
            retries=1,
            last_error="boom",
        )
    )
    await download_db.upsert_download(
        Download(
            feed_id=feed_id,
            id="skip1",
            source_url="u",
            title="t",
            published=datetime(2024, 1, 3, tzinfo=UTC),
            ext="mp4",
            mime_type="video/mp4",
            filesize=1,
            duration=1,
            status=DownloadStatus.SKIPPED,
        )
    )

    # Invoke admin endpoint
    resp = admin_test_app.post(f"/admin/feeds/{feed_id}/reset-errors")
    assert resp.status_code == 200
    data = resp.json()
    assert data["feed_id"] == feed_id
    assert data["reset_count"] == 2

    # Verify DB state for affected rows
    for dlid in ("err1", "err2"):
        row = await download_db.get_download_by_id(feed_id, dlid)
        assert row.status == DownloadStatus.QUEUED
        assert row.retries == 0
        assert row.last_error is None

    # Idempotent: re-run yields 0
    resp2 = admin_test_app.post(f"/admin/feeds/{feed_id}/reset-errors")
    assert resp2.status_code == 200
    assert resp2.json()["reset_count"] == 0


@pytest.mark.integration
def test_admin_reset_errors_feed_not_found(admin_test_app: TestClient) -> None:
    """404 when feed is missing."""
    resp = admin_test_app.post("/admin/feeds/missing/reset-errors")
    assert resp.status_code == 404
    assert resp.json()["detail"] == "Feed not found"
