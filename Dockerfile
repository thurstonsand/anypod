# Dockerfile for anypod using uv with Debian and managed Python
FROM ghcr.io/astral-sh/uv:trixie-slim AS builder

# Set environment variables for uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_PYTHON_INSTALL_DIR=/python

# Install Python 3.14
RUN uv python install 3.14

# Set working directory
WORKDIR /app
# Install dependencies first (using bind mounts for better caching)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY alembic_helpers/ ./alembic_helpers/
COPY alembic/ ./alembic/
COPY src/ ./src/
COPY pyproject.toml uv.lock alembic.ini README.md ./

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Runtime stage - use base Debian slim and copy uv binary
FROM debian:trixie-slim

# Cache-busting arg for security updates (changes weekly to force package refresh)
# Format: YYYY-WW (ISO week number)
# Updated automatically by update-dockerfile-deps.yml workflow
ARG CACHE_BUST_WEEK=2025-W51

ARG BGUTIL_POT_PROVIDER_VERSION=1.2.2

# Copy deno binary from official image
COPY --from=denoland/deno:bin /deno /usr/local/bin/deno

# Install curl for health check, ca-certificates for SSL verification, gosu for user switching, and yt-dlp
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    gosu \
    ffmpeg && \
    # Install yt-dlp binary into dedicated writable directory
    mkdir -p /app/bin && \
    curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /app/bin/yt-dlp && \
    chmod 0775 /app/bin/yt-dlp && \
    # Install bgutil POT provider plugin (zip) into yt-dlp system plugins dir
    mkdir -p /etc/yt-dlp/plugins && \
    curl -L https://github.com/brainicism/bgutil-ytdlp-pot-provider/releases/download/${BGUTIL_POT_PROVIDER_VERSION}/bgutil-ytdlp-pot-provider.zip -o /etc/yt-dlp/plugins/bgutil-ytdlp-pot-provider.zip && \
    rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy the Python installation and virtual environment from the builder stage
COPY --from=builder /python /python
COPY --from=builder /app /app

# Copy entrypoint scripts and make them executable
COPY --chmod=755 docker/scripts/ /usr/local/bin/

# Expose public port
EXPOSE 8024

# Expose admin port
EXPOSE 8025

ENV PATH="/app/bin:/app/.venv/bin:$PATH" \
    CONFIG_FILE=/config/feeds.yaml \
    DATA_DIR=/data \
    SERVER_HOST=0.0.0.0 \
    SERVER_PORT=8024 \
    ADMIN_SERVER_PORT=8025 \
    LOG_FORMAT=json \
    LOG_LEVEL=INFO \
    LOG_INCLUDE_STACKTRACE=true

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8024/api/health || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["anypod"]
