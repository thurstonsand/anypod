#!/bin/bash
set -euo pipefail

# Debug runner script for anypod with database setup

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DATA_DIR="$PROJECT_ROOT/tmpdata"

usage() {
    echo "Usage: $0 <debug_mode> [--keep]"
    echo "  debug_mode    One of: enqueuer, downloader, ytdlp"
    echo "  --keep        Keep existing database and downloaded files (default: clean start)"
    exit 1
}

clean_data_directory() {
    echo "Cleaning data directory: $DATA_DIR"
    if [ -d "$DATA_DIR" ]; then
        rm -rf "$DATA_DIR"
    fi
    
    # Create fresh directory structure
    mkdir -p "$DATA_DIR/media"
    echo "Created fresh data directory: $DATA_DIR"
}

init_database() {
    echo "Initializing database with Alembic..."
    cd "$PROJECT_ROOT"
    # Ensure db directory exists
    mkdir -p "$DATA_DIR/db"
    # Set DATABASE_URL for Alembic to match application's database path
    export DATABASE_URL="sqlite+aiosqlite:///$DATA_DIR/db/anypod.db"
    uv run alembic upgrade head
}

# Parse arguments
if [ $# -lt 1 ]; then
    echo "Error: debug_mode is required"
    usage
fi

DEBUG_MODE="$1"
shift

# Validate debug mode
case "$DEBUG_MODE" in
    enqueuer|downloader|ytdlp)
        ;;
    *)
        echo "Error: Invalid debug_mode '$DEBUG_MODE'"
        usage
        ;;
esac

KEEP_DATA=false
while [[ $# -gt 0 ]]; do
    case $1 in
        --keep)
            KEEP_DATA=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Change to project root
cd "$PROJECT_ROOT"

# Clean and initialize if not keeping existing state
if [ "$KEEP_DATA" = false ]; then
    clean_data_directory
    init_database
else
    echo "Keeping existing data and database state"
fi

# Set up environment and run the application in debug mode
echo "Starting anypod in $DEBUG_MODE debug mode..."
exec env DEBUG_MODE="$DEBUG_MODE" DATA_DIR="$DATA_DIR" TZ="US/Eastern" CONFIG_FILE="example_feeds.yaml" COOKIES_FILE="cookies.txt" uv run anypod