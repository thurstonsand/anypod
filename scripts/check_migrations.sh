#!/usr/bin/env bash
# Check for schema drift between SQLModel models and Alembic migrations.
#
# This script creates a temporary database, runs all migrations to get it
# up to date, then runs 'alembic revision --autogenerate' to check if any
# changes would be generated. If so, it means the database schema (defined
# by SQLModel models) doesn't match the current migrations, indicating drift
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
}
trap cleanup EXIT

echo "🔍 Checking for schema drift between models and migrations..."

# Ensure models can be imported cleanly
uv run python3 - <<'PY'
import sys
try:
    # Import the types module to ensure all models are registered
    from anypod.db import types as db_types
    _ = db_types  # Keep import
except Exception as e:
    print(f"❌ Failed to import models: {e}", file=sys.stderr)
    sys.exit(1)
PY

if [ $? -ne 0 ]; then
    echo "❌ Model import failed. Fix import errors before checking migrations."
    exit 1
fi

# Run migrations on the temporary database
echo "  Creating temporary database and running migrations..."
# Use synchronous sqlite:// (not aiosqlite) for compatibility with alembic
export DATABASE_URL="sqlite:///$TMP_DB"
if ! uv run alembic upgrade head >/dev/null 2>&1; then
    echo "❌ Failed to run migrations on temporary database"
    exit 1
fi

# Run autogenerate to check for drift
echo "  Checking for schema changes..."
AUTOGEN_OUTPUT=$(uv run alembic revision --autogenerate -m "tmp_drift_check" 2>&1 || true)

# Check if autogenerate detected any changes
# Alembic will log messages like "Detected added column" or "Detected removed table"
if echo "$AUTOGEN_OUTPUT" | grep -qE 'Detected (added|removed|modified|type change)'; then
    # Drift detected! Show preview and cleanup
    echo ""
    echo "❌ Schema drift detected!"
    echo ""
    echo "Your SQLModel models don't match the current Alembic migrations."
    echo "Review the changes below and create a proper migration:"
    echo ""
    echo "  alembic revision --autogenerate -m 'describe your changes'"
    echo ""
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "Detected changes:"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo "$AUTOGEN_OUTPUT" | grep "Detected"
    echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
    echo ""

    # Clean up the temporary revision
    rm -f alembic/versions/*tmp_drift_check*.py

    exit 1
else
    # No drift detected - clean up any empty migration that may have been generated
    rm -f alembic/versions/*tmp_drift_check*.py
    echo "✅ Migrations match models (no drift detected)"
    exit 0
fi
