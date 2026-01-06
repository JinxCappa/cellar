# Multi-stage Dockerfile for Cellar server and CLI
# Stage 1: Build environment
FROM rust:1.92-slim-bookworm AS builder

WORKDIR /build

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy workspace files
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build release binaries
RUN cargo build --release --bin cellard --bin cellarctl

# Stage 2: Runtime environment
FROM debian:bookworm-slim AS runtime

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 1000 -m cellar

# Create data directories
RUN mkdir -p /var/lib/cellar/storage /var/lib/cellar/data /etc/cellar \
    && chown -R cellar:cellar /var/lib/cellar /etc/cellar

# Copy binaries from builder
COPY --from=builder /build/target/release/cellard /usr/local/bin/
COPY --from=builder /build/target/release/cellarctl /usr/local/bin/

# Set permissions
RUN chmod +x /usr/local/bin/cellard /usr/local/bin/cellarctl

USER cellar
WORKDIR /var/lib/cellar

# Default configuration path
ENV CELLAR_CONFIG=/etc/cellar/server.toml

# Expose server port
EXPOSE 8080

# Health check
HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
    CMD curl -f http://localhost:8080/v1/health || exit 1

# Default command (uses CELLAR_CONFIG env var)
CMD ["cellard"]
