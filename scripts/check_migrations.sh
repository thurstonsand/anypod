#!/usr/bin/env bash
# Check for schema drift between SQLModel models and Alembic migrations.
#
# This script creates a temporary database, runs all migrations to get it
# up to date, then runs 'alembic check' to detect if autogenerate would
# create any changes. If so, it means the database schema (defined by
# SQLModel models) doesn't match the current migrations, indicating drift
# that needs to be resolved.
#
# Exit codes:
#   0: Migrations match models (no drift detected)
#   1: Schema drift detected or other error

set -euo pipefail

# Create temporary directory and database
TMP_DIR="$(mktemp -d)"
TMP_DB="$TMP_DIR/check_migrations.db"
cleanup() {
    rm -rf "$TMP_DIR"
    # Clean up any temporary migration files that might have been created
    # (e.g., if script was interrupted during a previous run)
    rm -f alembic/versions/*tmp_drift_check*.py
}
trap cleanup EXIT

echo "🔍 Checking for schema drift between models and migrations..."

# Run migrations on the temporary database
echo "  Creating temporary database and running migrations..."
# Use synchronous sqlite:// (not aiosqlite) for compatibility with alembic
export DATABASE_URL="sqlite:///$TMP_DB"
if ! uv run alembic upgrade head >/dev/null 2>&1; then
    echo "❌ Failed to run migrations on temporary database"
    exit 1
fi

# Use 'alembic check' to detect drift without creating files
# Capture both output and exit code in a single invocation
echo "  Checking for schema changes..."
CHECK_OUTPUT=$(uv run alembic check 2>&1) && CHECK_EXIT=0 || CHECK_EXIT=$?

if [ "$CHECK_EXIT" -eq 0 ]; then
    echo "✅ Migrations match models (no drift detected)"
    exit 0
fi

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
