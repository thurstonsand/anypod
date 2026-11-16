#!/usr/bin/env bash
# Generate an Alembic migration for the current SQLModel changes, then verify it.
#
# Autogenerate runs against a temporary database migrated to head, so it never
# depends on a local anypod.db. The new migration is then checked with
# check_migrations.sh (no remaining drift, safe with foreign-key data).
#
# Usage: ./scripts/new_migration.sh "describe your changes"
#
# Exit codes:
#   0: Migration generated and verified
#   1: Bad usage, no model changes to migrate, or verification failed

set -euo pipefail

if [ $# -ne 1 ] || [ -z "$1" ]; then
    echo "Usage: $0 \"describe your changes\""
    exit 1
fi
MESSAGE="$1"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT
export DATABASE_URL="sqlite:///$TMP_DIR/new_migration.db"

echo "🛠  Migrating a temporary database to head..."
uv run alembic upgrade head >/dev/null 2>&1

if uv run alembic check >/dev/null 2>&1; then
    echo "❌ Models match the migrations; there is nothing to generate."
    exit 1
fi

echo "📝 Generating migration..."
uv run alembic revision --autogenerate -m "$MESSAGE"

echo ""
echo "⬆️  Applying it to the temporary database..."
if ! UPGRADE_OUTPUT=$(uv run alembic upgrade head 2>&1); then
    echo "$UPGRADE_OUTPUT" | tail -n 15
    echo ""
    echo "❌ The generated migration does not apply. Fix it in alembic/versions/ and rerun ./scripts/check_migrations.sh."
    exit 1
fi

unset DATABASE_URL
echo ""
./scripts/check_migrations.sh

echo ""
echo "Review the generated file in alembic/versions/ before committing."
