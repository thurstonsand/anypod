# pyright: reportPrivateUsage=false

"""Integration tests for the admin HTTP server with real dependencies."""

from datetime import UTC, datetime

from fastapi.testclient import TestClient
import pytest

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.manual_feed_runner import ManualFeedRunner

# admin_test_app fixture provided by tests/integration/conftest.py


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admin_refresh_feed_happy_path(
    admin_test_app: TestClient,
    feed_db: FeedDatabase,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Triggers feed processing and downloads queued items."""
    feed_id = "int_admin_refresh"
    feed_configs[feed_id] = FeedConfig(
        url="https://www.youtube.com/watch?v=jNQXAC9IVRw",
        schedule="0 3 * * *",  # type: ignore[arg-type]
    )
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.SINGLE_VIDEO,
            source_url="https://www.youtube.com/watch?v=jNQXAC9IVRw",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        )
    )

    resp = admin_test_app.post(f"/admin/feeds/{feed_id}/refresh")
    assert resp.status_code == 202
    data = resp.json()
    assert data["feed_id"] == feed_id
    assert "triggered" in data["message"].lower()


@pytest.mark.integration
def test_admin_refresh_feed_not_found(admin_test_app: TestClient) -> None:
    """404 when feed is missing from database."""
    resp = admin_test_app.post("/admin/feeds/missing/refresh")
    assert resp.status_code == 404


@pytest.mark.integration
def test_admin_refresh_feed_not_configured(
    admin_test_app: TestClient,
) -> None:
    """404 when feed not in configuration."""
    resp = admin_test_app.post("/admin/feeds/unconfigured_feed/refresh")
    assert resp.status_code == 404
    assert "not configured" in resp.json()["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_admin_refresh_feed_disabled(
    admin_test_app: TestClient,
    feed_db: FeedDatabase,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """400 when feed is disabled."""
    feed_id = "int_admin_refresh_disabled"
    feed_configs[feed_id] = FeedConfig(
        enabled=False,
        url="https://www.youtube.com/watch?v=jNQXAC9IVRw",
        schedule="0 3 * * *",  # type: ignore[arg-type]
    )
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=False,
            source_type=SourceType.SINGLE_VIDEO,
            source_url="https://www.youtube.com/watch?v=jNQXAC9IVRw",
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        )
    )

    resp = admin_test_app.post(f"/admin/feeds/{feed_id}/refresh")
    assert resp.status_code == 400
    assert "disabled" in resp.json()["detail"].lower()


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
    assert "feed" in resp.json()["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_endpoint_enqueues_download(
    admin_test_app: TestClient,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
):
    """Manual submissions insert downloads and can be processed end-to-end."""
    feed_id = "manual"
    feed_configs[feed_id] = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.MANUAL,
            source_url=None,
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
            title="Manual Feed",
        )
    )

    resp = admin_test_app.post(
        f"/admin/feeds/{feed_id}/downloads",
        json={"url": "https://www.youtube.com/watch?v=jNQXAC9IVRw"},
    )
    assert resp.status_code == 200
    download_id = resp.json()["download_id"]

    feed_config = feed_configs[feed_id]
    await manual_feed_runner._run_feed(feed_id, feed_config)

    stored = await download_db.get_download_by_id(feed_id, download_id)
    assert stored.status == DownloadStatus.DOWNLOADED


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_requeue_existing_downloads(
    admin_test_app: TestClient,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    manual_feed_runner: ManualFeedRunner,
    feed_configs: dict[str, FeedConfig],
):
    """Resubmitting a failed download requeues it for processing."""
    feed_id = "manual"
    feed_configs[feed_id] = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.MANUAL,
            source_url=None,
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
            title="Manual Feed",
        )
    )

    resp = admin_test_app.post(
        f"/admin/feeds/{feed_id}/downloads",
        json={"url": "https://www.youtube.com/watch?v=jNQXAC9IVRw"},
    )
    assert resp.status_code == 200
    download_id = resp.json()["download_id"]

    feed_config = feed_configs[feed_id]
    await manual_feed_runner._run_feed(feed_id, feed_config)

    downloaded = await download_db.get_download_by_id(feed_id, download_id)
    errored = downloaded.model_copy(
        update={
            "status": DownloadStatus.ERROR,
            "last_error": "Simulated failure",
        }
    )
    await download_db.upsert_download(errored)

    resp2 = admin_test_app.post(
        f"/admin/feeds/{feed_id}/downloads",
        json={"url": "https://www.youtube.com/watch?v=jNQXAC9IVRw"},
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["download_id"] == download_id
    assert body["new"] is False
    assert body["status"] == DownloadStatus.QUEUED.value

    stored = await download_db.get_download_by_id(feed_id, download_id)
    assert stored.status == DownloadStatus.QUEUED


@pytest.mark.integration
def test_manual_submission_feed_not_in_database(
    admin_test_app: TestClient,
    feed_configs: dict[str, FeedConfig],
):
    """Submitting to feed not persisted in DB returns 404."""
    feed_id = "manual"
    feed_configs[feed_id] = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )

    resp = admin_test_app.post(
        f"/admin/feeds/{feed_id}/downloads",
        json={"url": "https://www.youtube.com/watch?v=jNQXAC9IVRw"},
    )
    assert resp.status_code == 404
    assert "not found" in resp.json()["detail"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_empty_url_validation(
    admin_test_app: TestClient,
    feed_db: FeedDatabase,
    feed_configs: dict[str, FeedConfig],
):
    """Empty URL in request body fails validation."""
    feed_id = "manual"
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.MANUAL,
            source_url=None,
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
            title="Manual Feed",
        )
    )

    feed_configs[feed_id] = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )

    resp = admin_test_app.post(
        f"/admin/feeds/{feed_id}/downloads",
        json={"url": ""},
    )
    assert resp.status_code == 422  # Pydantic validation error


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_missing_url_field(
    admin_test_app: TestClient,
    feed_db: FeedDatabase,
    feed_configs: dict[str, FeedConfig],
):
    """Missing URL field in request body fails validation."""
    feed_id = "manual"
    await feed_db.upsert_feed(
        Feed(
            id=feed_id,
            is_enabled=True,
            source_type=SourceType.MANUAL,
            source_url=None,
            last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
            title="Manual Feed",
        )
    )

    feed_configs[feed_id] = FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )

    resp = admin_test_app.post(
        f"/admin/feeds/{feed_id}/downloads",
        json={},
    )
    assert resp.status_code == 422  # Pydantic validation error
