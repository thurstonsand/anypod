"""Shared fixtures for integration tests."""

from asyncio import Semaphore
from collections.abc import AsyncGenerator, Iterator
from datetime import timedelta
from pathlib import Path

from fastapi.testclient import TestClient
from helpers.alembic import run_migrations
import pytest
import pytest_asyncio

from anypod.config import FeedConfig
from anypod.data_coordinator import DataCoordinator
from anypod.data_coordinator.downloader import Downloader
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.data_coordinator.pruner import Pruner
from anypod.db import AppStateDatabase
from anypod.db.download_db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.db.sqlalchemy_core import SqlalchemyCore
from anypod.ffmpeg import FFmpeg
from anypod.ffprobe import FFProbe
from anypod.file_manager import FileManager
from anypod.image_downloader import ImageDownloader
from anypod.manual_feed_runner import ManualFeedRunner
from anypod.manual_submission_service import ManualSubmissionService
from anypod.path_manager import PathManager
from anypod.rss.rss_feed import RSSFeedGenerator
from anypod.server.app import create_admin_app, create_app
from anypod.ytdlp_wrapper.handlers import HandlerSelector
from anypod.ytdlp_wrapper.ytdlp_wrapper import YtdlpWrapper


@pytest.fixture
def cookies_path() -> Path | None:
    """Provide cookies.txt path if it exists, otherwise None.

    Integration tests can use this fixture to conditionally authenticate
    with YouTube to avoid rate limiting during testing.

    Returns:
        Path to cookies.txt file if it exists, None otherwise.
    """
    cookies_file = Path(__file__).parent / "cookies.txt"
    return cookies_file if cookies_file.exists() else None


@pytest.fixture
def temp_db_path(tmp_path: Path) -> Path:
    """Provide a temporary database file path.

    Returns:
        Temporary database file path that auto-cleans up after test.
    """
    return tmp_path / "test.db"


@pytest.fixture
def path_manager(tmp_path: Path) -> PathManager:
    """Provide a PathManager instance with temporary data directory.

    Returns:
        PathManager configured with temporary directories for testing.
    """
    return PathManager(
        base_data_dir=tmp_path,
        base_url="http://localhost",
    )


@pytest.fixture
def file_manager(path_manager: PathManager) -> FileManager:
    """Provide a FileManager instance with shared data directory.

    Returns:
        FileManager instance configured with test path manager.
    """
    return FileManager(path_manager)


@pytest_asyncio.fixture
async def db_core(tmp_path: Path) -> AsyncGenerator[SqlalchemyCore]:
    """Provide a temporary database directory.

    Returns:
        Temporary database directory that auto-cleans up after test.
    """
    db_path = tmp_path / "anypod.db"

    # Run Alembic migrations to set up the database schema
    run_migrations(db_path)

    # Create SqlalchemyCore instance
    db_core = SqlalchemyCore(db_dir=tmp_path)
    yield db_core
    await db_core.close()


@pytest.fixture
def feed_db(db_core: SqlalchemyCore) -> FeedDatabase:
    """Provide a FeedDatabase instance with temporary database.

    Returns:
        FeedDatabase instance that gets properly closed after test.
    """
    return FeedDatabase(db_core)


@pytest.fixture
def download_db(db_core: SqlalchemyCore) -> DownloadDatabase:
    """Provide a DownloadDatabase instance with temporary database.

    Returns:
        DownloadDatabase instance that gets properly closed after test.
    """
    return DownloadDatabase(db_core)


@pytest.fixture
def feed_configs() -> dict[str, FeedConfig]:
    """Provide mutable feed configuration mapping for app state."""
    return {}


@pytest.fixture
def ffmpeg() -> FFmpeg:
    """Provide an FFmpeg instance for integration tests."""
    return FFmpeg()


@pytest.fixture
def ffprobe() -> FFProbe:
    """Provide an FFProbe instance for integration tests."""
    return FFProbe()


@pytest.fixture
def handler_selector(ffprobe: FFProbe) -> HandlerSelector:
    """Provide a HandlerSelector instance with shared FFProbe.

    Returns:
        HandlerSelector instance configured with test FFProbe.
    """
    return HandlerSelector(ffprobe)


@pytest.fixture
def ytdlp_wrapper(
    path_manager: PathManager,
    db_core: SqlalchemyCore,
    handler_selector: HandlerSelector,
    ffmpeg: FFmpeg,
    ffprobe: FFProbe,
) -> YtdlpWrapper:
    """Provide a YtdlpWrapper instance with shared directories.

    Returns:
        YtdlpWrapper instance configured with test path manager.
    """
    app_state_db = AppStateDatabase(db_core)
    return YtdlpWrapper(
        path_manager,
        None,
        app_state_db=app_state_db,
        yt_channel="stable",
        yt_update_freq=timedelta(hours=12),
        ffmpeg=ffmpeg,
        ffprobe=ffprobe,
        handler_selector=handler_selector,
    )


@pytest.fixture
def image_downloader(
    path_manager: PathManager,
    ytdlp_wrapper: YtdlpWrapper,
    ffprobe: FFProbe,
    ffmpeg: FFmpeg,
) -> ImageDownloader:
    """Provide an ImageDownloader instance with shared components.

    Returns:
        ImageDownloader instance configured with test path manager and ytdlp wrapper.
    """
    return ImageDownloader(path_manager, ytdlp_wrapper, ffprobe=ffprobe, ffmpeg=ffmpeg)


@pytest.fixture
def test_images() -> dict[str, Path]:
    """Provide paths to test image files.

    Returns:
        Dictionary mapping image format names to their file paths.
    """
    test_dir = Path(__file__).parent.parent / "helpers"
    return {
        "jpg": test_dir / "image.jpg",
        "png": test_dir / "image.png",
    }


@pytest.fixture
def enqueuer(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    ytdlp_wrapper: YtdlpWrapper,
) -> Enqueuer:
    """Provide an Enqueuer instance for the tests.

    Returns:
        Enqueuer instance configured with test databases and wrapper.
    """
    return Enqueuer(feed_db, download_db, ytdlp_wrapper)


@pytest.fixture
def downloader(
    download_db: DownloadDatabase,
    file_manager: FileManager,
    ytdlp_wrapper: YtdlpWrapper,
    ffprobe: FFProbe,
) -> Downloader:
    """Provide a Downloader instance for the tests.

    Returns:
        Downloader instance configured with test components.
    """
    return Downloader(
        download_db=download_db,
        file_manager=file_manager,
        ytdlp_wrapper=ytdlp_wrapper,
        ffprobe=ffprobe,
    )


@pytest.fixture
def pruner(
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    file_manager: FileManager,
) -> Pruner:
    """Provide a Pruner instance for the tests.

    Returns:
        Pruner instance configured with test databases and file manager.
    """
    return Pruner(feed_db, download_db, file_manager)


@pytest.fixture
def rss_generator(
    download_db: DownloadDatabase,
    path_manager: PathManager,
) -> RSSFeedGenerator:
    """Provide an RSSFeedGenerator instance for the tests.

    Returns:
        RSSFeedGenerator instance configured with test components.
    """
    return RSSFeedGenerator(download_db, path_manager)


@pytest.fixture
def data_coordinator(
    enqueuer: Enqueuer,
    downloader: Downloader,
    pruner: Pruner,
    rss_generator: RSSFeedGenerator,
    download_db: DownloadDatabase,
    feed_db: FeedDatabase,
    cookies_path: Path | None,
) -> DataCoordinator:
    """Provide a DataCoordinator instance for server fixtures."""
    return DataCoordinator(
        enqueuer=enqueuer,
        downloader=downloader,
        pruner=pruner,
        rss_generator=rss_generator,
        download_db=download_db,
        feed_db=feed_db,
        cookies_path=cookies_path,
    )


@pytest.fixture
def manual_feed_runner(
    data_coordinator: DataCoordinator,
    feed_configs: dict[str, FeedConfig],
) -> ManualFeedRunner:
    """Provide ManualFeedRunner with shared feed config mapping."""
    return ManualFeedRunner(data_coordinator, feed_configs, Semaphore(1))


@pytest.fixture
def manual_submission_service(
    ytdlp_wrapper: YtdlpWrapper,
) -> ManualSubmissionService:
    """Provide ManualSubmissionService backed by the shared wrapper."""
    return ManualSubmissionService(ytdlp_wrapper)


@pytest.fixture
def test_app(
    file_manager: FileManager,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    data_coordinator: DataCoordinator,
    ytdlp_wrapper: YtdlpWrapper,
    manual_feed_runner: ManualFeedRunner,
    manual_submission_service: ManualSubmissionService,
    feed_configs: dict[str, FeedConfig],
    cookies_path: Path | None,
) -> Iterator[TestClient]:
    """Create a FastAPI test client with real dependencies.

    Returns:
        TestClient configured with real FileManager and RSSFeedGenerator.
    """
    app = create_app(
        file_manager=file_manager,
        feed_database=feed_db,
        download_database=download_db,
        feed_configs=feed_configs,
        data_coordinator=data_coordinator,
        ytdlp_wrapper=ytdlp_wrapper,
        manual_feed_runner=manual_feed_runner,
        manual_submission_service=manual_submission_service,
        cookies_path=cookies_path,
    )
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def admin_test_app(
    file_manager: FileManager,
    feed_db: FeedDatabase,
    download_db: DownloadDatabase,
    data_coordinator: DataCoordinator,
    ytdlp_wrapper: YtdlpWrapper,
    manual_feed_runner: ManualFeedRunner,
    manual_submission_service: ManualSubmissionService,
    feed_configs: dict[str, FeedConfig],
    cookies_path: Path | None,
) -> Iterator[TestClient]:
    """Create a FastAPI admin test client with real dependencies.

    Returns:
        TestClient configured with real FileManager and RSSFeedGenerator for admin APIs.
    """
    app = create_admin_app(
        file_manager=file_manager,
        feed_database=feed_db,
        download_database=download_db,
        feed_configs=feed_configs,
        data_coordinator=data_coordinator,
        ytdlp_wrapper=ytdlp_wrapper,
        manual_feed_runner=manual_feed_runner,
        manual_submission_service=manual_submission_service,
        cookies_path=cookies_path,
    )
    client = TestClient(app)
    try:
        yield client
    finally:
        client.close()
