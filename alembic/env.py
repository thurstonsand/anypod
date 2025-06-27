"""Alembic environment script for asynchronous database migrations.

This script is the entrypoint for all Alembic commands. It configures the
database connection, sets up the migration context, and defines the logic
for running migrations in both 'online' (connected to a database) and
'offline' (generating SQL scripts) modes.

The script is configured to:
- Use an asynchronous database driver (aiosqlite).
- Read the database URL from the DATABASE_URL environment variable,
  falling back to the `sqlalchemy.url` key in alembic.ini.
- Use SQLModel metadata for autogenerate support, allowing Alembic to
  detect model changes and generate migration scripts automatically.
"""

import asyncio
from logging.config import fileConfig
import os

from sqlalchemy import engine_from_config, pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import async_engine_from_config
from sqlmodel import SQLModel

from alembic import context

# Import all models to register them with SQLModel's metadata
# This must happen before we reference SQLModel.metadata
from anypod.db import types as db_types

_ = db_types  # signal that we want to keep the import

# this is the Alembic Config object, which provides
# access to the values within the .ini file in use.
config = context.config

# Interpret the config file for Python logging.
# This line sets up loggers basically.
if config.config_file_name is not None:
    fileConfig(config.config_file_name)

# add your model's MetaData object here
# for 'autogenerate' support
target_metadata = SQLModel.metadata


def run_migrations_offline() -> None:
    """Run migrations in 'offline' mode.

    This configures the context with just a URL
    and not an Engine, though an Engine is acceptable
    here as well.  By skipping the Engine creation
    we don't even need a DBAPI to be available.

    Calls to context.execute() here emit the given string to the
    script output.
    """
    url = config.get_main_option("sqlalchemy.url")
    context.configure(
        url=url,
        target_metadata=target_metadata,
        literal_binds=True,
        dialect_opts={"paramstyle": "named"},
    )

    with context.begin_transaction():
        context.run_migrations()


def do_run_migrations(connection: Connection) -> None:
    """Run the actual migrations against a database connection.

    This function is called by `run_async_migrations` after a connection
    has been established.

    Args:
        connection: An active SQLAlchemy connection.
    """
    context.configure(connection=connection, target_metadata=target_metadata)

    with context.begin_transaction():
        context.run_migrations()


def run_migrations_for_context(connection: Connection):
    """A common migration runner function used by both sync and async paths.

    It configures the migration context and runs the migrations.
    """
    context.configure(connection=connection, target_metadata=target_metadata)
    with context.begin_transaction():
        context.run_migrations()


async def run_async_migrations(connectable_config: dict[str, str]) -> None:
    """Run migrations asynchronously."""
    async_engine = async_engine_from_config(
        connectable_config,
        prefix="sqlalchemy.",
        poolclass=pool.NullPool,
    )
    async with async_engine.connect() as connection:
        await connection.run_sync(run_migrations_for_context)
    await async_engine.dispose()


def run_migrations_online() -> None:
    """Run migrations in 'online' mode.

    This function is the main router for 'online' migrations.
    It checks the database URL to decide whether to use a synchronous
    or asynchronous engine.
    """
    connectable_config = config.get_section(config.config_ini_section)
    if connectable_config is None:
        raise ValueError(
            f"Alembic .ini file is missing section: {config.config_ini_section}"
        )

    # Allow overriding the URL with an environment variable, which is useful
    # for production environments.
    ini_url = config.get_main_option("sqlalchemy.url")
    url = os.getenv("DATABASE_URL", ini_url)
    if not url:
        raise ValueError(
            "Database URL is not configured. Set sqlalchemy.url in alembic.ini or DATABASE_URL env var."
        )

    connectable_config["sqlalchemy.url"] = url

    if "aiosqlite" in url:
        asyncio.run(run_async_migrations(connectable_config))
    else:
        engine = engine_from_config(
            connectable_config, prefix="sqlalchemy.", poolclass=pool.NullPool
        )
        with engine.connect() as connection:
            run_migrations_for_context(connection)


if context.is_offline_mode():
    run_migrations_offline()
else:
    run_migrations_online()
