# CLI Test Dockerfile
# Tests cellarctl push/pull with real Nix packages
#
# This image uses a multi-stage build:
# - Stage 1: Build static musl binary using cellar-musl-builder
# - Stage 2: Runtime with Nix for testing

# Stage 1: Build static binary
FROM cellar-musl-builder:latest AS builder

WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Build for the target architecture
ARG TARGETARCH
RUN if [ "$TARGETARCH" = "amd64" ] || [ -z "$TARGETARCH" ]; then \
      cargo build --release --target x86_64-unknown-linux-musl --bin cellarctl; \
      cp target/x86_64-unknown-linux-musl/release/cellarctl /cellarctl; \
    else \
      cargo build --release --target aarch64-unknown-linux-musl --bin cellarctl; \
      cp target/aarch64-unknown-linux-musl/release/cellarctl /cellarctl; \
    fi

# Stage 2: Runtime with Nix
FROM nixos/nix:2.24.10

# Enable flakes and nix-command
RUN mkdir -p /etc/nix && \
    echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf && \
    echo "sandbox = false" >> /etc/nix/nix.conf

# Install test utilities via Nix (no Rust toolchain needed)
RUN nix-env -iA \
    nixpkgs.curl \
    nixpkgs.jq \
    nixpkgs.bash \
    nixpkgs.coreutils \
    nixpkgs.gnugrep \
    nixpkgs.gnused

# Copy static binary from builder
COPY --from=builder /cellarctl /usr/local/bin/cellarctl
RUN chmod +x /usr/local/bin/cellarctl

# Create directories
RUN mkdir -p /tests /results /workspace

# Copy test scripts
COPY docker/tests/cli/entrypoint.sh /usr/local/bin/
COPY docker/tests/cli/scenarios/ /tests/scenarios/

RUN chmod +x /usr/local/bin/entrypoint.sh && \
    chmod +x /tests/scenarios/*.sh 2>/dev/null || true

WORKDIR /workspace

# Environment variables
ENV PATH="/usr/local/bin:${PATH}"
ENV CELLAR_SERVER_URL=http://server:8080
ENV CELLAR_TEST_TOKEN=""
ENV TEST_SCENARIOS=all
ENV RESULTS_DIR=/results

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
