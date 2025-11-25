# pyright: reportPrivateUsage=false

"""Integration tests for manual submission feature end-to-end."""

import asyncio
from datetime import UTC, datetime
import time

from fastapi.testclient import TestClient
import pytest
import pytest_asyncio

from anypod.config import FeedConfig
from anypod.config.types import FeedMetadataOverrides
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus, Feed, SourceType
from anypod.exceptions import DownloadNotFoundError
from anypod.manual_feed_runner import ManualFeedRunner
from anypod.path_manager import PathManager

MANUAL_FEED_ID = "manual_test_feed"
DEFAULT_STATUS_TIMEOUT_SECONDS = 60


async def _wait_for_download_status(
    download_db: DownloadDatabase,
    feed_id: str,
    download_id: str,
    expected_status: DownloadStatus,
    timeout_seconds: int = DEFAULT_STATUS_TIMEOUT_SECONDS,
) -> Download:
    """Poll the download status until it matches ``expected_status`` or timeout."""
    deadline = time.monotonic() + timeout_seconds
    last_download: Download | None = None

    while time.monotonic() < deadline:
        last_download = await download_db.get_download_by_id(feed_id, download_id)
        if last_download.status == expected_status:
            return last_download
        await asyncio.sleep(1)

    pytest.fail(
        f"Download {download_id} did not reach status {expected_status} within "
        f"{timeout_seconds}s (last status: {last_download.status if last_download else 'unknown'})"
    )


@pytest.fixture
def manual_feed_config() -> FeedConfig:
    """Create a manual feed configuration for testing."""
    return FeedConfig(
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(
            title="Manual Test Feed",
            description="Feed for testing manual submissions",
        ),
    )


@pytest_asyncio.fixture
async def manual_feed_setup(
    feed_db: FeedDatabase,
    manual_feed_config: FeedConfig,
) -> Feed:
    """Set up a manual feed in the database."""
    feed = Feed(
        id=MANUAL_FEED_ID,
        is_enabled=True,
        source_type=SourceType.MANUAL,
        source_url=None,
        resolved_url=None,
        last_successful_sync=datetime(2024, 1, 1, tzinfo=UTC),
        title=manual_feed_config.metadata.title
        if manual_feed_config.metadata
        else None,
        description=(
            manual_feed_config.metadata.description
            if manual_feed_config.metadata
            else None
        ),
    )
    await feed_db.upsert_feed(feed)
    return feed


@pytest.fixture
def manual_admin_app(
    admin_test_app: TestClient,
    feed_configs: dict[str, FeedConfig],
    manual_feed_config: FeedConfig,
) -> TestClient:
    """Admin TestClient configured with the manual feed entry."""
    feed_configs[MANUAL_FEED_ID] = manual_feed_config
    return admin_test_app


# --- Integration Tests ---


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_full_flow(
    manual_admin_app: TestClient,
    manual_feed_runner: ManualFeedRunner,
    manual_feed_setup: Feed,
    download_db: DownloadDatabase,
    path_manager: PathManager,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Submit URL, download media, verify files and database state."""
    # Use a real YouTube video (short test video)
    test_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"

    # Submit the URL
    resp = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": test_url},
    )
    assert resp.status_code == 200
    body = resp.json()
    assert body["feed_id"] == MANUAL_FEED_ID
    assert body["new"] is True
    assert body["status"] == DownloadStatus.QUEUED.value

    download_id = body["download_id"]

    feed_config = feed_configs[MANUAL_FEED_ID]
    await manual_feed_runner._run_feed(MANUAL_FEED_ID, feed_config)

    # Wait for download to complete
    download = await _wait_for_download_status(
        download_db, MANUAL_FEED_ID, download_id, DownloadStatus.DOWNLOADED
    )

    # Verify media file exists
    media_path = await path_manager.media_file_path(
        MANUAL_FEED_ID, download_id, download.ext
    )
    assert media_path.exists()

    # Verify RSS feed was generated
    rss_path = await path_manager.feed_xml_path(MANUAL_FEED_ID)
    assert rss_path.exists()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_resubmit_downloaded(
    manual_admin_app: TestClient,
    manual_feed_runner: ManualFeedRunner,
    manual_feed_setup: Feed,
    download_db: DownloadDatabase,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Resubmitting an already downloaded URL returns appropriate status."""
    test_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"

    # First submission
    resp1 = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": test_url},
    )
    assert resp1.status_code == 200
    download_id = resp1.json()["download_id"]

    feed_config = feed_configs[MANUAL_FEED_ID]
    await manual_feed_runner._run_feed(MANUAL_FEED_ID, feed_config)

    # Wait for download to complete
    await _wait_for_download_status(
        download_db, MANUAL_FEED_ID, download_id, DownloadStatus.DOWNLOADED
    )

    # Resubmit same URL
    resp2 = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": test_url},
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["feed_id"] == MANUAL_FEED_ID
    assert body["download_id"] == download_id
    assert body["new"] is False
    assert body["status"] == DownloadStatus.DOWNLOADED.value
    assert "already completed" in body["message"].lower()


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_requeue_error(
    manual_admin_app: TestClient,
    manual_feed_runner: ManualFeedRunner,
    manual_feed_setup: Feed,
    download_db: DownloadDatabase,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Resubmitting an errored download requeues it."""
    test_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"

    # First submission
    resp1 = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": test_url},
    )
    assert resp1.status_code == 200
    download_id = resp1.json()["download_id"]

    feed_config = feed_configs[MANUAL_FEED_ID]
    await manual_feed_runner._run_feed(MANUAL_FEED_ID, feed_config)

    # Wait for processing
    download = await _wait_for_download_status(
        download_db, MANUAL_FEED_ID, download_id, DownloadStatus.DOWNLOADED
    )

    # Manually mark as ERROR for testing
    errored_download = download.model_copy(
        update={
            "status": DownloadStatus.ERROR,
            "last_error": "Simulated error",
        }
    )
    await download_db.upsert_download(errored_download)

    # Verify it's in ERROR state
    download = await download_db.get_download_by_id(MANUAL_FEED_ID, download_id)
    assert download.status == DownloadStatus.ERROR

    # Resubmit to requeue
    resp2 = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": test_url},
    )
    assert resp2.status_code == 200
    body = resp2.json()
    assert body["download_id"] == download_id
    assert body["new"] is False
    assert body["status"] == DownloadStatus.QUEUED.value
    assert "requeued" in body["message"].lower()

    # Verify it's back in QUEUED state
    download = await download_db.get_download_by_id(MANUAL_FEED_ID, download_id)
    assert download.status == DownloadStatus.QUEUED


@pytest.mark.integration
def test_manual_submission_feed_not_configured(
    manual_admin_app: TestClient,
    manual_feed_setup: Feed,
) -> None:
    """Submitting to unconfigured feed returns 404."""
    resp = manual_admin_app.post(
        "/admin/feeds/nonexistent_feed/downloads",
        json={"url": "https://www.youtube.com/watch?v=test"},
    )
    assert resp.status_code == 404
    assert "not configured" in resp.json()["detail"].lower()


@pytest.mark.integration
def test_manual_submission_feed_disabled(
    manual_admin_app: TestClient,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Submitting to disabled feed returns 400."""
    disabled_feed_id = "disabled_manual"
    disabled_config = FeedConfig(
        enabled=False,
        url=None,
        schedule="manual",  # type: ignore[arg-type]
        metadata=FeedMetadataOverrides(title="Disabled Manual Feed"),
    )

    feed_configs[disabled_feed_id] = disabled_config

    resp = manual_admin_app.post(
        f"/admin/feeds/{disabled_feed_id}/downloads",
        json={"url": "https://www.youtube.com/watch?v=test"},
    )
    assert resp.status_code == 400
    assert "disabled" in resp.json()["detail"].lower()


@pytest.mark.integration
def test_manual_submission_scheduled_feed_rejects(
    manual_admin_app: TestClient,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Submitting to scheduled feed returns 400."""
    scheduled_feed_id = "scheduled_feed"
    scheduled_config = FeedConfig(
        url="https://www.youtube.com/@example",
        schedule="0 3 * * *",  # type: ignore[arg-type]
    )

    feed_configs[scheduled_feed_id] = scheduled_config

    resp = manual_admin_app.post(
        f"/admin/feeds/{scheduled_feed_id}/downloads",
        json={"url": "https://www.youtube.com/watch?v=test"},
    )
    assert resp.status_code == 400
    assert "does not accept manual" in resp.json()["detail"].lower()


@pytest.mark.integration
def test_manual_submission_invalid_url(
    manual_admin_app: TestClient,
    manual_feed_setup: Feed,
) -> None:
    """Submitting unsupported URL returns 400."""
    # Use a URL that yt-dlp cannot handle
    resp = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": "https://not-a-video-site.com/invalid"},
    )
    # Should return 400 (unsupported) or 422 (unavailable)
    assert resp.status_code in (400, 422)


@pytest.mark.integration
def test_manual_submission_empty_url(
    manual_admin_app: TestClient,
    manual_feed_setup: Feed,
) -> None:
    """Submitting empty URL returns validation error."""
    resp = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": ""},
    )
    assert resp.status_code == 422  # Pydantic validation error


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_multiple_submissions(
    manual_admin_app: TestClient,
    manual_feed_runner: ManualFeedRunner,
    manual_feed_setup: Feed,
    download_db: DownloadDatabase,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Multiple submissions to same feed are processed correctly."""
    url1 = "https://www.youtube.com/watch?v=jNQXAC9IVRw"
    url2 = "https://www.youtube.com/watch?v=aqz-KE-bpKQ"

    # Submit first URL
    resp1 = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": url1},
    )
    assert resp1.status_code == 200
    id1 = resp1.json()["download_id"]

    # Submit second URL
    resp2 = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": url2},
    )
    assert resp2.status_code == 200
    id2 = resp2.json()["download_id"]

    assert id1 != id2

    feed_config = feed_configs[MANUAL_FEED_ID]
    await manual_feed_runner._run_feed(MANUAL_FEED_ID, feed_config)

    # Wait for both downloads to complete
    download1, download2 = await asyncio.gather(
        _wait_for_download_status(
            download_db, MANUAL_FEED_ID, id1, DownloadStatus.DOWNLOADED
        ),
        _wait_for_download_status(
            download_db, MANUAL_FEED_ID, id2, DownloadStatus.DOWNLOADED
        ),
    )

    assert download1.status == DownloadStatus.DOWNLOADED
    assert download2.status == DownloadStatus.DOWNLOADED


@pytest.mark.integration
@pytest.mark.asyncio
async def test_manual_submission_delete_flow(
    manual_admin_app: TestClient,
    manual_feed_runner: ManualFeedRunner,
    manual_feed_setup: Feed,
    download_db: DownloadDatabase,
    path_manager: PathManager,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """Submit, download, verify, delete, and confirm cleanup for manual feed."""
    test_url = "https://www.youtube.com/watch?v=jNQXAC9IVRw"

    # Submit download request
    submit_resp = manual_admin_app.post(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads",
        json={"url": test_url},
    )
    assert submit_resp.status_code == 200
    download_id = submit_resp.json()["download_id"]

    # Run manual feed to process the queued download
    feed_config = feed_configs[MANUAL_FEED_ID]
    await manual_feed_runner._run_feed(MANUAL_FEED_ID, feed_config)

    download = await _wait_for_download_status(
        download_db, MANUAL_FEED_ID, download_id, DownloadStatus.DOWNLOADED
    )

    media_path = await path_manager.media_file_path(
        MANUAL_FEED_ID, download_id, download.ext
    )
    assert media_path.exists()

    # Delete the download via admin endpoint
    delete_resp = manual_admin_app.delete(
        f"/admin/feeds/{MANUAL_FEED_ID}/downloads/{download_id}"
    )
    assert delete_resp.status_code == 204

    # Record should be gone
    with pytest.raises(DownloadNotFoundError):
        await download_db.get_download_by_id(MANUAL_FEED_ID, download_id)

    # Media file should be deleted
    assert not media_path.exists()

    # RSS should be regenerated and present
    rss_path = await path_manager.feed_xml_path(MANUAL_FEED_ID)
    assert rss_path.exists()
