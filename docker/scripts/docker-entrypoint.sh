#!/bin/bash -e

DATA_ROOT="${DATA_DIR:-/data}"
DB_FILE="${DATA_ROOT}/db/anypod.db"

# Handle runtime user switching and permissions
if [ "$(id -u)" = '0' ]; then
    TARGET_USER_ID=${PUID:-1000}
    TARGET_GROUP_ID=${PGID:-1000}
    TARGET_OWNER="${TARGET_USER_ID}:${TARGET_GROUP_ID}"

    # Ensure writable directories exist (never chown /config or /cookies so :ro mounts work)
    mkdir -p "${DATA_ROOT}" "${DATA_ROOT}/db" /app/bin

    # Only adjust ownership when the directory owner does not match, avoiding slow
    # recursive chown walks over large media libraries on every restart.
    for dir in "${DATA_ROOT}" "${DATA_ROOT}/db" /app/bin; do
        CURRENT_OWNER="$(stat -c '%u:%g' "$dir" 2>/dev/null || echo '')"
        if [ "$CURRENT_OWNER" != "$TARGET_OWNER" ]; then
            chown "$TARGET_OWNER" "$dir"
        fi
    done
    if [ -e /app/bin/yt-dlp ]; then
        YTDLP_OWNER="$(stat -c '%u:%g' /app/bin/yt-dlp 2>/dev/null || echo '')"
        if [ "$YTDLP_OWNER" != "$TARGET_OWNER" ]; then
            chown "$TARGET_OWNER" /app/bin/yt-dlp
        fi
    fi

    # Re-exec this script as the target user
    exec setpriv --reuid="${TARGET_USER_ID}" --regid="${TARGET_GROUP_ID}" --clear-groups "$0" "$@"
fi

# Now running as non-root user
mkdir -p "$(dirname "$DB_FILE")"
export DATABASE_URL="sqlite+aiosqlite:///$DB_FILE"

# Run database migrations when starting anypod
if [ $# -eq 0 ] || [ "$1" = "anypod" ] || [ "${1#-}" != "$1" ]; then
    echo "Running database migrations..."
    alembic upgrade head
fi

# Start the requested command
if [ $# -eq 0 ]; then
    exec anypod
elif [ "${1#-}" != "$1" ]; then
    exec anypod "$@"
else
    exec "$@"
fi
