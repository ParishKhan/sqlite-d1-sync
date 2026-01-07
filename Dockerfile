# D1 Sync - Docker Configuration
# Multi-stage build for minimal production image

# Build stage
FROM python:3.12-slim as builder

# Install build dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv for fast package management
RUN pip install --no-cache-dir uv

# Create app directory
WORKDIR /app

# Copy project files
COPY pyproject.toml ./
COPY src/ ./src/

# Install dependencies
RUN uv pip install --system --no-cache .

# =============================================================================
# Production stage
# =============================================================================
FROM python:3.12-slim as production

# Security: Create non-root user
RUN groupadd --gid 1000 d1sync \
    && useradd --uid 1000 --gid d1sync --shell /bin/bash --create-home d1sync

# Install runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*

# Create directories
WORKDIR /app
RUN mkdir -p /data /config && chown -R d1sync:d1sync /app /data /config

# Copy installed packages from builder
COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages
COPY --from=builder /usr/local/bin/d1-sync /usr/local/bin/d1-sync

# Copy source (for reference)
COPY --chown=d1sync:d1sync src/ ./src/

# Switch to non-root user
USER d1sync

# Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    D1_SYNC_LOGGING__FORMAT=rich

# Default data volume
VOLUME ["/data", "/config"]

# Default command
ENTRYPOINT ["d1-sync"]
CMD ["--help"]

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD d1-sync --version || exit 1

# Labels
LABEL org.opencontainers.image.title="D1 Sync" \
      org.opencontainers.image.description="SQLite to Cloudflare D1 synchronization tool" \
      org.opencontainers.image.version="1.0.0" \
      org.opencontainers.image.vendor="D1 Sync Contributors"
