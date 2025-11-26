#!/usr/bin/env bash
# Check for schema drift between SQLModel models and Alembic migrations.
#
# This script performs two checks:
# 1. Schema drift: Creates a temporary database, runs all migrations, then
#    checks if autogenerate would create any changes. If so, the models
#    don't match the migrations.
# 2. FK constraint safety: Tests that migrations work on databases with
#    existing foreign key relationships, using the async driver (aiosqlite)
#    that's used in production.
#
# Exit codes:
#   0: All checks pass
#   1: Schema drift detected, FK migration failed, or other error

set -euo pipefail

# Create temporary directory and databases
TMP_DIR="$(mktemp -d)"
TMP_DB="$TMP_DIR/check_migrations.db"
TMP_DB_FK="$TMP_DIR/check_fk_migrations.db"
cleanup() {
    rm -rf "$TMP_DIR"
    # Clean up any temporary migration files that might have been created
    # (e.g., if script was interrupted during a previous run)
    rm -f alembic/versions/*tmp_drift_check*.py
}
trap cleanup EXIT

# ------------------------------------------------------------------
# Check 1: Schema drift detection
# ------------------------------------------------------------------
echo "🔍 Checking for schema drift between models and migrations..."

# Run migrations on the temporary database
echo "  Creating temporary database and running migrations..."
# Use synchronous sqlite:// (not aiosqlite) for compatibility with alembic check
export DATABASE_URL="sqlite:///$TMP_DB"
if ! uv run alembic upgrade head >/dev/null 2>&1; then
    echo "❌ Failed to run migrations on temporary database"
    exit 1
fi

# Use 'alembic check' to detect drift without creating files
# Capture both output and exit code in a single invocation
echo "  Checking for schema changes..."
CHECK_OUTPUT=$(uv run alembic check 2>&1) && CHECK_EXIT=0 || CHECK_EXIT=$?

if [ "$CHECK_EXIT" -ne 0 ]; then
    # Non-zero exit - check if it's drift or an error
    if echo "$CHECK_OUTPUT" | grep -q "New upgrade operations detected"; then
        echo ""
        echo "❌ Schema drift detected!"
        echo ""
        echo "Your SQLModel models don't match the current Alembic migrations."
        echo "Create a migration to fix this:"
        echo ""
        echo "  uv run alembic revision --autogenerate -m 'describe your changes'"
        echo ""
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "Detected changes:"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo "$CHECK_OUTPUT"
        echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
        echo ""
        exit 1
    else
        echo ""
        echo "❌ Failed to run schema drift check"
        echo ""
        echo "$CHECK_OUTPUT"
        exit 1
    fi
fi

echo "✅ Migrations match models (no drift detected)"

# ------------------------------------------------------------------
# Check 2: FK constraint safety with aiosqlite
# ------------------------------------------------------------------
echo ""
echo "🔗 Checking migrations work with foreign key relationships..."

# Use aiosqlite (the production driver) for this test
export DATABASE_URL="sqlite+aiosqlite:///$TMP_DB_FK"

# Run all migrations to create schema
echo "  Running migrations to head..."
if ! uv run alembic upgrade head >/dev/null 2>&1; then
    echo "❌ Failed to run migrations with aiosqlite driver"
    exit 1
fi

# Insert realistic test data with FK relationships
echo "  Inserting test data with foreign key relationships..."
sqlite3 "$TMP_DB_FK" "
-- Realistic feed entry (similar to a YouTube channel)
INSERT INTO feed (
    id, is_enabled, source_type, source_url, resolved_url,
    last_successful_sync, consecutive_failures, total_downloads,
    title, subtitle, description, language, author, author_email,
    remote_image_url, category, podcast_type, explicit
) VALUES (
    'test-channel', 1, 'CHANNEL',
    'https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw',
    'https://www.youtube.com/channel/UC_x5XG1OV2P6uZZ5FSM9Ttw',
    datetime('now'), 0, 1,
    'Test Channel', 'A test podcast feed', 'This is a test feed for migration checks',
    'en', 'Test Author', 'test@example.com',
    'https://example.com/thumbnail.jpg', 'Technology', 'EPISODIC', 0
);

-- Realistic download entry (similar to a YouTube video)
INSERT INTO download (
    feed_id, id, source_url, title, published,
    ext, mime_type, filesize, duration, status,
    remote_thumbnail_url, description, quality_info
) VALUES (
    'test-channel', 'dQw4w9WgXcQ',
    'https://www.youtube.com/watch?v=dQw4w9WgXcQ',
    'Test Video Title', datetime('now', '-7 days'),
    'mp4', 'video/mp4', 52428800, 212, 'COMPLETED',
    'https://i.ytimg.com/vi/dQw4w9WgXcQ/maxresdefault.jpg',
    'A test video description for migration testing',
    '1080p'
);
"

# Verify data was inserted correctly
FEED_COUNT=$(sqlite3 "$TMP_DB_FK" "SELECT COUNT(*) FROM feed;")
DOWNLOAD_COUNT=$(sqlite3 "$TMP_DB_FK" "SELECT COUNT(*) FROM download;")

if [ "$FEED_COUNT" -ne 1 ] || [ "$DOWNLOAD_COUNT" -ne 1 ]; then
    echo "❌ Failed to insert test data (feed=$FEED_COUNT, download=$DOWNLOAD_COUNT)"
    exit 1
fi

# Downgrade to a migration before batch_alter_table operations on feed table,
# then upgrade back. This ensures we test FK constraint handling regardless of
# what future migrations do.
#
# e8752d424e88 is the migration before ad59f082d627 (first batch_alter_table on feed)
# This exercises both ad59f082d627 and cd72a61e713d which use batch_alter_table on feed.
echo "  Testing downgrade/upgrade cycle with FK data..."
FK_TEST_TARGET="e8752d424e88"

if ! uv run alembic downgrade "$FK_TEST_TARGET" >/dev/null 2>&1; then
    echo "❌ Failed to downgrade to $FK_TEST_TARGET with FK data present"
    echo "   This may indicate batch_alter_table fails when other tables"
    echo "   have foreign key constraints on the table being altered."
    exit 1
fi

# Upgrade back to head
if ! uv run alembic upgrade head >/dev/null 2>&1; then
    echo "❌ Failed to upgrade from $FK_TEST_TARGET with FK data present"
    echo "   This may indicate batch_alter_table fails when other tables"
    echo "   have foreign key constraints on the table being altered."
    exit 1
fi

# Verify data integrity after migrations
FEED_COUNT=$(sqlite3 "$TMP_DB_FK" "SELECT COUNT(*) FROM feed;")
DOWNLOAD_COUNT=$(sqlite3 "$TMP_DB_FK" "SELECT COUNT(*) FROM download;")

if [ "$FEED_COUNT" -ne 1 ] || [ "$DOWNLOAD_COUNT" -ne 1 ]; then
    echo "❌ Data integrity lost after migration cycle (feed=$FEED_COUNT, download=$DOWNLOAD_COUNT)"
    exit 1
fi

echo "✅ Migrations work correctly with foreign key relationships"
echo ""
echo "All checks passed!"
