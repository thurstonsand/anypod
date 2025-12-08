# pyright: reportPrivateUsage=false

"""Tests for the admin router endpoints."""

from datetime import UTC, datetime
from unittest.mock import Mock

from fastapi import FastAPI
from fastapi.testclient import TestClient
import pytest

from anypod.config import FeedConfig
from anypod.config.types import CronExpression, FeedMetadataOverrides
from anypod.data_coordinator import DataCoordinator
from anypod.data_coordinator.types import ProcessingResults
from anypod.db import DownloadDatabase, FeedDatabase
from anypod.db.types import Download, DownloadStatus
from anypod.exceptions import (
    DatabaseOperationError,
    DownloadNotFoundError,
    EnqueueError,
    FeedNotFoundError,
)
from anypod.file_manager import FileManager
from anypod.server.routers.admin import router

# Shared test constants
ADMIN_PREFIX = "/admin"
FEED_ID = "test_feed"
RESET_COUNT = 3


@pytest.fixture
def mock_feed_database() -> Mock:
    """Create a mock FeedDatabase for testing."""
    return Mock(spec=FeedDatabase)


@pytest.fixture
def mock_download_database() -> Mock:
    """Create a mock DownloadDatabase for testing."""
    return Mock(spec=DownloadDatabase)


@pytest.fixture
def mock_file_manager() -> Mock:
    """Create a mock FileManager for testing."""
    return Mock(spec=FileManager)


@pytest.fixture
def mock_data_coordinator() -> Mock:
    """Create a mock DataCoordinator for testing."""
    return Mock(spec=DataCoordinator)


@pytest.fixture
def mock_manual_feed_runner() -> Mock:
    """Create a mock ManualFeedRunner for testing."""
    from anypod.manual_feed_runner import ManualFeedRunner

    return Mock(spec=ManualFeedRunner)


@pytest.fixture
def feed_configs() -> dict[str, FeedConfig]:
    """In-memory feed config mapping used by admin dependencies."""
    return {}


@pytest.fixture
def app(
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_file_manager: Mock,
    mock_data_coordinator: Mock,
    mock_manual_feed_runner: Mock,
    feed_configs: dict[str, FeedConfig],
) -> FastAPI:
    """Create a FastAPI app with the admin router and mocked dependencies."""
    app = FastAPI()
    app.include_router(router)

    # Attach mocked dependencies to app state
    app.state.feed_database = mock_feed_database
    app.state.download_database = mock_download_database
    app.state.file_manager = mock_file_manager
    app.state.data_coordinator = mock_data_coordinator
    app.state.manual_feed_runner = mock_manual_feed_runner
    app.state.feed_configs = feed_configs

    return app


@pytest.fixture
def client(app: FastAPI) -> TestClient:
    """Create a test client for the admin router."""
    return TestClient(app)


@pytest.fixture
def manual_feed_config() -> FeedConfig:
    """Create a manual feed configuration."""
    return FeedConfig(  # type: ignore[call-arg]
        schedule="manual",
        metadata=FeedMetadataOverrides(title="Manual Feed"),
    )


@pytest.fixture
def scheduled_feed_config() -> FeedConfig:
    """Create a scheduled feed configuration."""
    return FeedConfig(  # type: ignore[call-arg]
        url="https://example.com",
        schedule=CronExpression("0 3 * * *"),
    )


@pytest.fixture
def disabled_feed_config() -> FeedConfig:
    """Create a disabled feed configuration."""
    return FeedConfig(  # type: ignore[call-arg]
        url="https://example.com",
        schedule=CronExpression("0 3 * * *"),
        enabled=False,
    )


@pytest.fixture
def sample_download() -> Download:
    """Create a sample downloaded item."""
    return Download(
        feed_id=FEED_ID,
        id="dl-1",
        source_url="https://example.com/video",
        title="Test Video",
        published=datetime(2024, 1, 1, tzinfo=UTC),
        ext="mp3",
        mime_type="audio/mpeg",
        filesize=1000000,
        duration=120,
        status=DownloadStatus.DOWNLOADED,
        thumbnail_ext="jpg",
    )


# --- Tests for reset-sync endpoint ---


@pytest.mark.unit
def test_reset_sync_success(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """Returns feed_id + sync_time; calls mark_sync_success with provided timestamp."""
    sync_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
    mock_feed_database.mark_sync_success.return_value = None

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-sync",
        json={"sync_time": sync_time.isoformat()},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["feed_id"] == FEED_ID
    assert datetime.fromisoformat(data["sync_time"]) == sync_time
    mock_feed_database.mark_sync_success.assert_called_once_with(
        FEED_ID, sync_time=sync_time
    )


@pytest.mark.unit
def test_reset_sync_feed_not_found(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """404 when feed does not exist."""
    missing_feed_id = "missing"
    sync_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
    mock_feed_database.mark_sync_success.side_effect = FeedNotFoundError(
        "Feed not found.", feed_id=missing_feed_id
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{missing_feed_id}/reset-sync",
        json={"sync_time": sync_time.isoformat()},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


@pytest.mark.unit
def test_reset_sync_db_error(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """500 when DB error occurs during mark_sync_success."""
    sync_time = datetime(2024, 6, 15, 12, 0, 0, tzinfo=UTC)
    mock_feed_database.mark_sync_success.side_effect = DatabaseOperationError(
        "DB error"
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-sync",
        json={"sync_time": sync_time.isoformat()},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"


@pytest.mark.unit
def test_reset_sync_invalid_timestamp_format(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """422 when timestamp is not valid ISO 8601 format."""
    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-sync",
        json={"sync_time": "not-a-timestamp"},
    )

    assert response.status_code == 422
    mock_feed_database.mark_sync_success.assert_not_called()


@pytest.mark.unit
def test_reset_sync_missing_timestamp(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """422 when sync_time field is missing from request body."""
    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-sync",
        json={},
    )

    assert response.status_code == 422
    mock_feed_database.mark_sync_success.assert_not_called()


@pytest.mark.unit
def test_reset_sync_naive_datetime_rejected(
    client: TestClient,
    mock_feed_database: Mock,
) -> None:
    """422 when timestamp lacks timezone information."""
    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/reset-sync",
        json={"sync_time": "2024-06-15T12:00:00"},
    )

    assert response.status_code == 422
    mock_feed_database.mark_sync_success.assert_not_called()


# --- Tests for reset-errors endpoint ---


@pytest.mark.unit
def test_reset_errors_success(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """Returns feed_id + requeue_count; calls bulk requeue for ERROR items only."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.requeue_downloads.return_value = RESET_COUNT

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/requeue")

    assert response.status_code == 200
    data = response.json()
    assert data == {
        "feed_id": FEED_ID,
        "download_id": None,
        "requeue_count": RESET_COUNT,
    }
    mock_feed_database.get_feed_by_id.assert_called_once_with(FEED_ID)
    mock_download_database.requeue_downloads.assert_called_once_with(
        feed_id=FEED_ID, download_ids=None, from_status=DownloadStatus.ERROR
    )


@pytest.mark.unit
def test_reset_errors_feed_not_found(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """404 when feed does not exist in database."""
    missing_feed_id = "missing"
    feed_configs[missing_feed_id] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.side_effect = FeedNotFoundError(
        "Feed not found.", feed_id=missing_feed_id
    )

    response = client.post(f"{ADMIN_PREFIX}/feeds/{missing_feed_id}/requeue")

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


@pytest.mark.unit
def test_reset_errors_db_error_on_requeue(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """500 when DB error occurs during bulk requeue."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.requeue_downloads.side_effect = DatabaseOperationError(
        "DB error"
    )

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/requeue")

    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"


# --- Tests for refresh endpoint ---


@pytest.mark.unit
def test_refresh_feed_success_scheduled_feed(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_manual_feed_runner: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """202 when refresh is triggered for a scheduled feed."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/refresh")

    assert response.status_code == 202
    data = response.json()
    assert data["feed_id"] == FEED_ID
    assert data["message"] == "Feed processing triggered"
    mock_feed_database.get_feed_by_id.assert_awaited_once_with(FEED_ID)
    mock_manual_feed_runner.trigger.assert_awaited_once_with(
        FEED_ID, scheduled_feed_config
    )


@pytest.mark.unit
def test_refresh_feed_success_manual_feed(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_manual_feed_runner: Mock,
    manual_feed_config: FeedConfig,
) -> None:
    """202 when refresh is triggered for a manual feed."""
    feed_configs[FEED_ID] = manual_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/refresh")

    assert response.status_code == 202
    data = response.json()
    assert data["feed_id"] == FEED_ID
    assert data["message"] == "Feed processing triggered"
    mock_feed_database.get_feed_by_id.assert_awaited_once_with(FEED_ID)
    mock_manual_feed_runner.trigger.assert_awaited_once_with(
        FEED_ID, manual_feed_config
    )


@pytest.mark.unit
def test_refresh_feed_not_configured(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """404 when feed is not in configuration."""
    # feed_configs is empty, so no feed is configured

    response = client.post(f"{ADMIN_PREFIX}/feeds/not-configured/refresh")

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not configured"


@pytest.mark.unit
def test_refresh_feed_disabled(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    disabled_feed_config: FeedConfig,
) -> None:
    """400 when feed is disabled."""
    feed_configs[FEED_ID] = disabled_feed_config

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/refresh")

    assert response.status_code == 400
    assert response.json()["detail"] == "Feed is disabled"


@pytest.mark.unit
def test_refresh_feed_not_in_database(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """404 when feed exists in config but not in database."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.side_effect = FeedNotFoundError(
        "Feed not found.", feed_id=FEED_ID
    )

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/refresh")

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


@pytest.mark.unit
def test_refresh_feed_database_error(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """500 when database error occurs during feed lookup."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.side_effect = DatabaseOperationError(
        "Database error"
    )

    response = client.post(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/refresh")

    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"


# --- Tests for get-download-fields endpoint ---


@pytest.mark.unit
def test_get_download_fields_success_default_fields(
    client: TestClient,
    mock_download_database: Mock,
) -> None:
    """Returns download data for all fields when no field filter is provided."""
    download_id = "dl-123"
    download = Mock()

    dump = {
        "status": DownloadStatus.QUEUED.value,
        "download_logs": "log contents",
    }

    def model_dump_mock(
        *, mode: str, include: set[str], exclude: set[str]
    ) -> dict[str, str]:
        assert mode == "json"
        assert "feed" not in include
        assert exclude == {"feed", "id"}
        return dump

    download.model_dump.side_effect = model_dump_mock
    mock_download_database.get_download_by_id.return_value = download

    response = client.get(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}")

    assert response.status_code == 200
    assert response.json() == {
        "feed_id": FEED_ID,
        "download_id": download_id,
        "download": dump,
    }
    mock_download_database.get_download_by_id.assert_awaited_once_with(
        FEED_ID, download_id
    )
    download.model_dump.assert_called_once()


@pytest.mark.unit
def test_get_download_fields_success_with_filter(
    client: TestClient,
    mock_download_database: Mock,
) -> None:
    """Returns only requested fields when query parameter is used."""
    download_id = "dl-456"
    download = Mock()

    dump = {
        "download_logs": "log contents",
        "last_error": "boom",
    }

    def model_dump_mock(
        *, mode: str, include: set[str], exclude: set[str]
    ) -> dict[str, str]:
        assert include == {"download_logs", "last_error"}
        return dump

    download.model_dump.side_effect = model_dump_mock
    mock_download_database.get_download_by_id.return_value = download

    response = client.get(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}",
        params={"fields": "download_logs,last_error"},
    )

    assert response.status_code == 200
    assert response.json() == {
        "feed_id": FEED_ID,
        "download_id": download_id,
        "download": dump,
    }
    mock_download_database.get_download_by_id.assert_awaited_once_with(
        FEED_ID, download_id
    )
    download.model_dump.assert_called_once()


@pytest.mark.unit
def test_get_download_fields_invalid_field_returns_400(
    client: TestClient,
    mock_download_database: Mock,
) -> None:
    """400 when unsupported fields are requested."""
    download_id = "dl-789"

    response = client.get(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}",
        params={"fields": "download_logs,not_a_column"},
    )

    assert response.status_code == 400
    assert "not_a_column" in response.json()["detail"]
    mock_download_database.get_download_by_id.assert_not_called()


@pytest.mark.unit
def test_get_download_fields_empty_fields_returns_400(
    client: TestClient,
    mock_download_database: Mock,
) -> None:
    """400 when fields query resolves to an empty list."""
    download_id = "dl-999"

    response = client.get(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}",
        params={"fields": " , , "},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "No fields specified"
    mock_download_database.get_download_by_id.assert_not_called()


@pytest.mark.unit
def test_get_download_fields_not_found_returns_404(
    client: TestClient,
    mock_download_database: Mock,
) -> None:
    """404 when the download cannot be located."""
    download_id = "missing"
    mock_download_database.get_download_by_id.side_effect = DownloadNotFoundError(
        "missing", feed_id=FEED_ID, download_id=download_id
    )

    response = client.get(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}",
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Download not found"


@pytest.mark.unit
def test_get_download_fields_database_error_returns_500(
    client: TestClient,
    mock_download_database: Mock,
) -> None:
    """500 when the database raises an unexpected error."""
    download_id = "db-error"
    mock_download_database.get_download_by_id.side_effect = DatabaseOperationError(
        "db error"
    )

    response = client.get(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}",
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"


# --- Tests for delete-download endpoint ---


@pytest.mark.unit
def test_delete_download_success(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_file_manager: Mock,
    mock_data_coordinator: Mock,
    manual_feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """Deletes download, files, and regenerates RSS for manual feeds."""
    feed_configs[FEED_ID] = manual_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.delete_download.return_value = sample_download
    mock_data_coordinator.regenerate_rss.return_value = ProcessingResults(
        feed_id=FEED_ID,
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        overall_success=True,
    )

    response = client.delete(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{sample_download.id}"
    )

    assert response.status_code == 204
    assert response.text == ""
    mock_feed_database.get_feed_by_id.assert_awaited_once_with(FEED_ID)
    mock_download_database.delete_download.assert_awaited_once_with(
        FEED_ID, sample_download.id
    )
    mock_file_manager.delete_download_file.assert_awaited_once_with(
        FEED_ID, sample_download.id, sample_download.ext
    )
    mock_file_manager.delete_image.assert_awaited_once_with(
        FEED_ID, sample_download.id, sample_download.thumbnail_ext
    )
    mock_data_coordinator.regenerate_rss.assert_awaited_once_with(FEED_ID)


@pytest.mark.unit
def test_delete_download_not_found(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    manual_feed_config: FeedConfig,
) -> None:
    """404 when the download does not exist."""
    feed_configs[FEED_ID] = manual_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    missing_id = "missing"
    mock_download_database.delete_download.side_effect = DownloadNotFoundError(
        "missing", feed_id=FEED_ID, download_id=missing_id
    )

    response = client.delete(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{missing_id}")

    assert response.status_code == 404
    assert response.json()["detail"] == "Download not found"
    mock_feed_database.get_feed_by_id.assert_awaited_once_with(FEED_ID)
    mock_download_database.delete_download.assert_awaited_once_with(FEED_ID, missing_id)


@pytest.mark.unit
def test_delete_download_rejects_non_manual_feed(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    scheduled_feed_config: FeedConfig,
) -> None:
    """400 when attempting to delete from a scheduled feed."""
    feed_configs[FEED_ID] = scheduled_feed_config

    response = client.delete(f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/dl-1")

    assert response.status_code == 400
    assert (
        response.json()["detail"]
        == "Download deletion is only supported for manual feeds"
    )


@pytest.mark.unit
def test_delete_download_handles_missing_files(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_file_manager: Mock,
    mock_data_coordinator: Mock,
    manual_feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """Missing media files are tolerated while deleting the download."""
    # Use a download without thumbnail to simplify test
    download_no_thumb = sample_download.model_copy(update={"thumbnail_ext": None})

    feed_configs[FEED_ID] = manual_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.delete_download.return_value = download_no_thumb
    mock_file_manager.delete_download_file.side_effect = FileNotFoundError()
    mock_data_coordinator.regenerate_rss.return_value = ProcessingResults(
        feed_id=FEED_ID,
        start_time=datetime(2024, 1, 1, tzinfo=UTC),
        overall_success=True,
    )

    response = client.delete(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_no_thumb.id}"
    )

    assert response.status_code == 204
    mock_feed_database.get_feed_by_id.assert_awaited_once_with(FEED_ID)
    mock_file_manager.delete_download_file.assert_awaited_once()
    mock_file_manager.delete_image.assert_not_awaited()
    mock_data_coordinator.regenerate_rss.assert_awaited_once_with(FEED_ID)


# --- Tests for refresh-metadata endpoint ---


@pytest.mark.unit
def test_refresh_metadata_success(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_data_coordinator: Mock,
    scheduled_feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """200 when metadata is successfully refreshed."""
    download_id = sample_download.id
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.get_download_by_id.return_value = sample_download

    updated_download = sample_download.model_copy(
        update={"title": "Updated Title", "description": "Updated description"}
    )
    mock_data_coordinator.refresh_download_metadata.return_value = (
        updated_download,
        ["description", "title"],
        None,
        None,
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}/refresh-metadata",
        json={},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["feed_id"] == FEED_ID
    assert data["download_id"] == download_id
    assert data["metadata_changed"] is True
    assert data["thumbnail_refreshed"] is None
    assert data["transcript_refreshed"] is None
    assert "title" in data["updated_fields"]
    assert "description" in data["updated_fields"]
    mock_data_coordinator.refresh_download_metadata.assert_awaited_once_with(
        feed_id=FEED_ID,
        download_id=download_id,
        feed_config=scheduled_feed_config,
        refresh_transcript=False,
    )


@pytest.mark.unit
def test_refresh_metadata_no_changes(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_data_coordinator: Mock,
    scheduled_feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """200 with metadata_changed=False when no metadata changed."""
    download_id = sample_download.id
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.get_download_by_id.return_value = sample_download

    mock_data_coordinator.refresh_download_metadata.return_value = (
        sample_download,
        [],
        None,
        None,
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}/refresh-metadata",
        json={},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["metadata_changed"] is False
    assert data["thumbnail_refreshed"] is None
    assert data["transcript_refreshed"] is None
    assert data["updated_fields"] == []


@pytest.mark.unit
def test_refresh_metadata_with_thumbnail_refresh(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_data_coordinator: Mock,
    scheduled_feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """200 with thumbnail_refreshed=True when thumbnail URL changed and was refreshed."""
    download_id = sample_download.id
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.get_download_by_id.return_value = sample_download

    updated_download = sample_download.model_copy(
        update={"remote_thumbnail_url": "https://example.com/new-thumb.jpg"}
    )
    mock_data_coordinator.refresh_download_metadata.return_value = (
        updated_download,
        ["remote_thumbnail_url"],
        True,
        None,
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{download_id}/refresh-metadata",
        json={},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["thumbnail_refreshed"] is True
    assert data["transcript_refreshed"] is None
    assert "remote_thumbnail_url" in data["updated_fields"]


@pytest.mark.unit
def test_refresh_metadata_feed_not_configured(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
) -> None:
    """404 when feed is not in configuration."""
    # feed_configs is empty
    response = client.post(
        f"{ADMIN_PREFIX}/feeds/not-configured/downloads/dl-1/refresh-metadata",
        json={},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not configured"


@pytest.mark.unit
def test_refresh_metadata_feed_disabled(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    disabled_feed_config: FeedConfig,
) -> None:
    """400 when feed is disabled."""
    feed_configs[FEED_ID] = disabled_feed_config

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/dl-1/refresh-metadata",
        json={},
    )

    assert response.status_code == 400
    assert response.json()["detail"] == "Feed is disabled"


@pytest.mark.unit
def test_refresh_metadata_feed_not_in_database(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """404 when feed exists in config but not in database."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.side_effect = FeedNotFoundError(
        "Feed not found.", feed_id=FEED_ID
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/dl-1/refresh-metadata",
        json={},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Feed not found"


@pytest.mark.unit
def test_refresh_metadata_download_not_found(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_data_coordinator: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """404 when download does not exist."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    cause = DownloadNotFoundError(
        "Download not found", feed_id=FEED_ID, download_id="missing"
    )
    error = EnqueueError("Download not found", feed_id=FEED_ID, download_id="missing")
    error.__cause__ = cause
    mock_data_coordinator.refresh_download_metadata.side_effect = error

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/missing/refresh-metadata",
        json={},
    )

    assert response.status_code == 404
    assert response.json()["detail"] == "Download not found"


@pytest.mark.unit
def test_refresh_metadata_database_error(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    scheduled_feed_config: FeedConfig,
) -> None:
    """500 when database error occurs during feed lookup."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.side_effect = DatabaseOperationError(
        "Database error"
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/dl-1/refresh-metadata",
        json={},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Database error"


@pytest.mark.unit
def test_refresh_metadata_enqueue_error(
    client: TestClient,
    feed_configs: dict[str, FeedConfig],
    mock_feed_database: Mock,
    mock_download_database: Mock,
    mock_data_coordinator: Mock,
    scheduled_feed_config: FeedConfig,
    sample_download: Download,
) -> None:
    """500 when refresh_download_metadata raises EnqueueError."""
    feed_configs[FEED_ID] = scheduled_feed_config
    mock_feed_database.get_feed_by_id.return_value = object()
    mock_download_database.get_download_by_id.return_value = sample_download
    mock_data_coordinator.refresh_download_metadata.side_effect = EnqueueError(
        "Failed to fetch metadata from yt-dlp",
        feed_id=FEED_ID,
        download_id=sample_download.id,
    )

    response = client.post(
        f"{ADMIN_PREFIX}/feeds/{FEED_ID}/downloads/{sample_download.id}/refresh-metadata",
        json={},
    )

    assert response.status_code == 500
    assert response.json()["detail"] == "Failed to refresh metadata"
