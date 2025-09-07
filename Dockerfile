# Dockerfile for anypod using uv with Debian and managed Python
FROM ghcr.io/astral-sh/uv:bookworm-slim AS builder

# Set environment variables for uv
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_PREFERENCE=only-managed \
    UV_PYTHON_INSTALL_DIR=/python

# Install Python 3.13
RUN uv python install 3.13

# Set working directory
WORKDIR /app
# Install dependencies first (using bind mounts for better caching)
RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-install-project --no-dev

COPY src/ ./src/
COPY pyproject.toml uv.lock alembic.ini README.md ./
COPY alembic/ ./alembic/

# Install the project itself
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev

# Runtime stage - use base Debian slim and copy uv binary
FROM debian:bookworm-slim

ARG BGUTIL_POT_PROVIDER_VERSION=1.2.2

# Install curl for health check, ca-certificates for SSL verification, gosu for user switching, and yt-dlp
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        gosu \
        ffmpeg && \
    # Install yt-dlp binary with write permissions for updates
    curl -L https://github.com/yt-dlp/yt-dlp/releases/latest/download/yt-dlp -o /usr/local/bin/yt-dlp && \
    chmod a+rwx /usr/local/bin/yt-dlp && \
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

# Expose port
EXPOSE 8024

ENV PATH="/app/.venv/bin:$PATH" \
    CONFIG_FILE=/config/feeds.yaml \
    DATA_DIR=/data \
    COOKIE_PATH=/cookies/cookies.txt \
    SERVER_HOST=0.0.0.0 \
    SERVER_PORT=8024 \
    LOG_FORMAT=json \
    LOG_LEVEL=INFO \
    LOG_INCLUDE_STACKTRACE=true

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:8024/api/health || exit 1

ENTRYPOINT ["docker-entrypoint.sh"]
CMD ["anypod"]
