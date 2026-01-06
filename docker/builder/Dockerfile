# Multi-arch builder for static musl binaries
# Supports building x86_64 and aarch64 Linux binaries from any host
FROM rust:1.92-bookworm

# Build arguments - BUILDARCH is automatically set by Docker BuildKit
ARG BUILDARCH

# Install base dependencies
RUN apt-get update && apt-get install -y \
    curl \
    xz-utils \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /opt

# Download and install cross-compilers based on host architecture
# - amd64 hosts: use musl.cc pre-built toolchains (native performance)
# - arm64 hosts: use zig cc for C compilation only (native performance, no emulation)
RUN set -eux; \
    case "${BUILDARCH}" in \
        amd64) \
            curl -fsSL https://musl.cc/x86_64-linux-musl-cross.tgz | tar xzf -; \
            curl -fsSL https://musl.cc/aarch64-linux-musl-cross.tgz | tar xzf -; \
            ln -s /opt/x86_64-linux-musl-cross/bin/x86_64-linux-musl-gcc /usr/local/bin/; \
            ln -s /opt/aarch64-linux-musl-cross/bin/aarch64-linux-musl-gcc /usr/local/bin/; \
            ;; \
        arm64) \
            ZIG_VERSION="0.15.2"; \
            curl -fsSL "https://ziglang.org/download/${ZIG_VERSION}/zig-aarch64-linux-${ZIG_VERSION}.tar.xz" | tar xJf -; \
            mv "zig-aarch64-linux-${ZIG_VERSION}" zig; \
            ln -s /opt/zig/zig /usr/local/bin/zig; \
            # Create wrapper scripts named *-gcc (what build tools expect) \
            # - Filter out --target flags (zig uses different format) \
            # - Add -fno-sanitize=undefined to disable UBSan (zig enables by default) \
            printf '%s\n' '#!/bin/sh' 'args=""' 'for arg in "$@"; do' '  case "$arg" in' '    --target=*) ;;' '    *) args="$args $arg" ;;' '  esac' 'done' 'exec /opt/zig/zig cc -target x86_64-linux-musl -fno-sanitize=undefined $args' > /usr/local/bin/x86_64-linux-musl-gcc; \
            printf '%s\n' '#!/bin/sh' 'args=""' 'for arg in "$@"; do' '  case "$arg" in' '    --target=*) ;;' '    *) args="$args $arg" ;;' '  esac' 'done' 'exec /opt/zig/zig cc -target aarch64-linux-musl -fno-sanitize=undefined $args' > /usr/local/bin/aarch64-linux-musl-gcc; \
            chmod +x /usr/local/bin/x86_64-linux-musl-gcc /usr/local/bin/aarch64-linux-musl-gcc; \
            ;; \
        *) \
            echo "Unsupported build architecture: ${BUILDARCH}"; \
            exit 1; \
            ;; \
    esac

# Add Rust targets
RUN rustup target add x86_64-unknown-linux-musl \
    && rustup target add aarch64-unknown-linux-musl

# Configure cargo for cross-compilation (architecture-specific)
# amd64: use musl.cc gcc for both compiling and linking
# arm64: use zig for C compilation, rust-lld for linking (avoids zig CRT conflicts)
RUN mkdir -p /root/.cargo && \
    if [ "${BUILDARCH}" = "amd64" ]; then \
        printf '%s\n' \
            '[target.x86_64-unknown-linux-musl]' \
            'linker = "x86_64-linux-musl-gcc"' \
            'rustflags = ["-C", "target-feature=+crt-static"]' \
            '' \
            '[target.aarch64-unknown-linux-musl]' \
            'linker = "aarch64-linux-musl-gcc"' \
            'rustflags = ["-C", "target-feature=+crt-static"]' \
            > /root/.cargo/config.toml; \
    else \
        printf '%s\n' \
            '[target.x86_64-unknown-linux-musl]' \
            'linker = "rust-lld"' \
            'rustflags = ["-C", "target-feature=+crt-static", "-C", "link-self-contained=yes"]' \
            '' \
            '[target.aarch64-unknown-linux-musl]' \
            'linker = "rust-lld"' \
            'rustflags = ["-C", "target-feature=+crt-static", "-C", "link-self-contained=yes"]' \
            > /root/.cargo/config.toml; \
    fi

# Update PATH (for amd64 with musl.cc toolchains)
ENV PATH="/opt/x86_64-linux-musl-cross/bin:/opt/aarch64-linux-musl-cross/bin:${PATH}"

# Environment variables for static linking of C dependencies
# These MUST be set as ENV for build scripts to pick them up
ENV LIBZSTD_NO_PKG_CONFIG="1"
ENV LZMA_API_STATIC="1"
ENV CC_x86_64_unknown_linux_musl="x86_64-linux-musl-gcc"
ENV CC_aarch64_unknown_linux_musl="aarch64-linux-musl-gcc"

WORKDIR /build
