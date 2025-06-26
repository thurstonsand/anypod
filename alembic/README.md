# Alembic Database Migrations

This directory contains database migration scripts for Anypod, managed by [Alembic](https://alembic.sqlalchemy.org/).

## Overview

Anypod uses Alembic to manage database schema changes over time. The migration system ensures that database schemas can be versioned, upgraded, and downgraded in a controlled manner.

## Configuration

- **Database URL**: Configured in `alembic.ini` to use async SQLite (`sqlite+aiosqlite`)
- **Target Metadata**: Uses `SQLModel.metadata` from the application models
- **Async Support**: Configured for async database operations

## Migration Files

- `env.py` - Alembic environment configuration with async support
- `script.py.mako` - Template for generating new migration scripts
- `versions/` - Contains all migration scripts in chronological order

## Common Commands

### Create a new migration
```bash
alembic revision --autogenerate -m "Description of changes"
```

### Apply migrations (upgrade to latest)
```bash
alembic upgrade head
```

### View current migration status
```bash
alembic current
```

### View migration history
```bash
alembic history --verbose
```

### Downgrade to previous migration
```bash
alembic downgrade -1
```

## Migration Workflow

1. **Make model changes** in `src/anypod/db/types/`
2. **Generate migration**: `alembic revision --autogenerate -m "Description"`
3. **Review migration** in `versions/` directory
4. **Apply migration**: `alembic upgrade head`

## Important Notes

- Always review auto-generated migrations before applying
- Test migrations on a copy of production data
- Migrations are applied automatically during application startup
- Database triggers are managed through migrations (see `add_database_triggers.py`)

## Existing Migrations

1. **Initial Schema** (`2025_06_25_423d964333d1_initial_schema_from_sqlmodels.py`)
   - Creates feeds and downloads tables from SQLModel definitions
   - Establishes foreign key relationships and indexes

2. **Database Triggers** (`2025_06_25_78f7e4e33398_add_database_triggers.py`)
   - Adds automatic timestamp triggers for `updated_at` and `downloaded_at`
   - Maintains referential integrity with cascade operations