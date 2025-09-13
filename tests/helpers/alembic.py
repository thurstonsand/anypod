"""Test helper for running Alembic migrations programmatically."""

from pathlib import Path

from alembic.config import Config

from alembic import command


def run_migrations(db_path: Path) -> None:
    """Configures and runs Alembic migrations up to 'head' for a test database.

    This function is synchronous and intended for test setup.
    """
    project_root = Path(__file__).resolve().parent.parent.parent
    alembic_ini = project_root / "alembic.ini"
    if not alembic_ini.exists():
        raise FileNotFoundError(f"alembic.ini not found at {alembic_ini}")

    config = Config(str(alembic_ini))

    # Override the sqlalchemy.url from alembic.ini to point to our test DB
    # using a synchronous driver.
    sync_db_url = f"sqlite:///{db_path}"
    config.set_main_option("sqlalchemy.url", sync_db_url)

    # Run the upgrade command directly.
    command.upgrade(config, "head")
