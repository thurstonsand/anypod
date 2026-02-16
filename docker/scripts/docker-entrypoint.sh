#!/bin/bash -e

# Handle runtime user switching and permissions
if [ "$(id -u)" = '0' ]; then
    # Running as root, need to switch to target user
    TARGET_USER_ID=${PUID:-1000}
    TARGET_GROUP_ID=${PGID:-1000}

    # Ensure data directories exist and have correct permissions
    mkdir -p /config /data /cookies
    chown -R "${TARGET_USER_ID}:${TARGET_GROUP_ID}" /config /data /app/bin

    # Re-exec this script as the target user
    exec setpriv --reuid="${TARGET_USER_ID}" --regid="${TARGET_GROUP_ID}" --init-groups "$0" "$@"
fi

# Now running as non-root user
# Set database file path
DB_FILE="${DATA_DIR:-/data}/db/anypod.db"

# Create database directory if it doesn't exist
mkdir -p "$(dirname "$DB_FILE")"

# Set database URL for alembic
export DATABASE_URL="sqlite+aiosqlite:///$DB_FILE"

# Run database migrations
echo "Running database migrations..."
alembic upgrade head

# Start the application
echo "Starting anypod application..."
if [ $# -eq 0 ]; then
    # No arguments provided, use the default CMD
    exec anypod
else
    # Arguments provided, pass them to anypod
    exec anypod "$@"
fi
