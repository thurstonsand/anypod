"""Shared fixtures for integration tests."""

from collections.abc import Generator
from pathlib import Path

import pytest

from anypod.data_coordinator.downloader import Downloader
from anypod.data_coordinator.enqueuer import Enqueuer
from anypod.data_coordinator.pruner import Pruner
from anypod.db.download_db import DownloadDatabase
from anypod.db.feed_db import FeedDatabase
from anypod.file_manager import FileManager
from anypod.path_manager import PathManager
from anypod.rss.rss_feed import RSSFeedGenerator
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
        base_data_dir=tmp_path / "data",
        base_url="http://localhost",
    )


@pytest.fixture
def feed_db(temp_db_path: Path) -> Generator[FeedDatabase]:
    """Provide a FeedDatabase instance with temporary database.

    Returns:
        FeedDatabase instance that gets properly closed after test.
    """
    feed_db = FeedDatabase(db_path=temp_db_path)
    yield feed_db
    feed_db.close()


@pytest.fixture
def download_db(temp_db_path: Path) -> Generator[DownloadDatabase]:
    """Provide a DownloadDatabase instance with temporary database.

    Returns:
        DownloadDatabase instance that gets properly closed after test.
    """
    download_db = DownloadDatabase(db_path=temp_db_path)
    yield download_db
    download_db.close()


@pytest.fixture
def file_manager(path_manager: PathManager) -> FileManager:
    """Provide a FileManager instance with shared data directory.

    Returns:
        FileManager instance configured with test path manager.
    """
    return FileManager(path_manager)


@pytest.fixture
def ytdlp_wrapper(path_manager: PathManager) -> YtdlpWrapper:
    """Provide a YtdlpWrapper instance with shared directories.

    Returns:
        YtdlpWrapper instance configured with test path manager.
    """
    return YtdlpWrapper(path_manager)


@pytest.fixture
def enqueuer(
    feed_db: FeedDatabase, download_db: DownloadDatabase, ytdlp_wrapper: YtdlpWrapper
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
) -> Downloader:
    """Provide a Downloader instance for the tests.

    Returns:
        Downloader instance configured with test components.
    """
    return Downloader(download_db, file_manager, ytdlp_wrapper)


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
