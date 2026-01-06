# Dev environment for Cellar
# Multi-target: server + client
#
# Build context: repository root (../../)

# ---------------------------------------------------------------------------
# Stage 1: Build both binaries
# ---------------------------------------------------------------------------
FROM rust:1.92-slim-bookworm AS builder

WORKDIR /build

RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

RUN cargo build --bin cellard --bin cellarctl

# ---------------------------------------------------------------------------
# Stage 2: Server
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS server

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

RUN useradd -r -u 1000 -m cellar

RUN mkdir -p /var/lib/cellar/storage /var/lib/cellar/data /etc/cellar \
    && chown -R cellar:cellar /var/lib/cellar /etc/cellar

COPY --from=builder /build/target/debug/cellard /usr/local/bin/
COPY --from=builder /build/target/debug/cellarctl /usr/local/bin/

USER cellar
WORKDIR /var/lib/cellar

ENV CELLAR_CONFIG=/etc/cellar/server.toml
EXPOSE 8080

HEALTHCHECK --interval=5s --timeout=3s --start-period=15s --retries=5 \
    CMD curl -f http://localhost:8080/v1/health || exit 1

CMD ["cellard"]

# ---------------------------------------------------------------------------
# Stage 3: Client (Debian + Nix + cellarctl)
# ---------------------------------------------------------------------------
FROM debian:bookworm-slim AS client

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    curl \
    git \
    jq \
    xz-utils \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# Install Nix (multi-user)
RUN curl -L https://nixos.org/nix/install | bash -s -- --daemon --yes

# Configure Nix
RUN mkdir -p /etc/nix && \
    echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf && \
    echo "sandbox = false" >> /etc/nix/nix.conf && \
    echo "trusted-users = root" >> /etc/nix/nix.conf && \
    echo "extra-substituters = http://server:8080" >> /etc/nix/nix.conf && \
    echo "trusted-public-keys = cache.nixos.org-1:6NCHdD59X431o0gWypbMrAURkbJ16ZPMQFGspcDShjY= dev-cache-1:WakyPak7SOlSzC/z8MGuuEvPewBWKreSirrud+qC5F0=" >> /etc/nix/nix.conf && \
    echo "narinfo-cache-negative-ttl = 0" >> /etc/nix/nix.conf

COPY --from=builder /build/target/debug/cellarctl /usr/local/bin/

# Source nix profile in interactive shells
RUN echo '. /nix/var/nix/profiles/default/etc/profile.d/nix.sh' >> /root/.bashrc

# Entrypoint: start nix-daemon then keep alive
COPY --chmod=755 <<'ENTRY' /usr/local/bin/entrypoint.sh
#!/bin/bash
/nix/var/nix/profiles/default/bin/nix-daemon &
. /nix/var/nix/profiles/default/etc/profile.d/nix.sh
exec sleep infinity
ENTRY

WORKDIR /workspace

ENV CELLAR_SERVER=http://server:8080
ENV CELLAR_TOKEN=cellar-test-token-12345

CMD ["/usr/local/bin/entrypoint.sh"]
