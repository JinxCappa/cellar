# Cellar - Nix Binary Cache Server
# Run `just` to see all available commands

set shell := ["bash", "-cu"]

# Default: show available commands
default:
    @just --list

# ============================================================================
# Build Commands
# ============================================================================

# Build all Docker images (server, nix, test-runner)
build: build-server build-nix build-test-runner
    @echo "All images built successfully"

# Build server image only
build-server:
    @echo "Building server image..."
    docker build -t cellar-server:latest -f docker/Dockerfile .

# Build Nix builder and client images
build-nix:
    @echo "Building Nix builder image..."
    docker build -t cellar-builder:latest --target builder -f docker/Dockerfile.nix .
    @echo "Building Nix client image..."
    docker build -t cellar-client:latest --target client -f docker/Dockerfile.nix .

# Build test runner image
build-test-runner:
    @echo "Building test runner image..."
    docker build -t cellar-test-runner:latest -f docker/tests/runner/Dockerfile .

# Build CLI test image
build-cli-test: build-musl-builder
    @echo "Building CLI test image..."
    docker build -t cellar-cli-test:latest -f docker/tests/cli/Dockerfile .

# Build static binary builder image (musl cross-compiler)
build-musl-builder:
    @echo "Building static binary builder image (musl cross-compiler)..."
    docker build -t cellar-musl-builder:latest -f docker/builder/Dockerfile .

# ============================================================================
# Static Binary Builds (cross-compilation)
# ============================================================================

# Build static x86_64 Linux binary
build-static-x86_64: build-musl-builder
    @echo "Building static x86_64 binary..."
    docker run --rm -v {{justfile_directory()}}:/build -w /build cellar-musl-builder:latest \
        cargo build --release --target x86_64-unknown-linux-musl --bin cellarctl --bin cellard
    @echo "Static binaries at: target/x86_64-unknown-linux-musl/release/"

# Build static aarch64 Linux binary
build-static-aarch64: build-musl-builder
    @echo "Building static aarch64 binary..."
    docker run --rm -v {{justfile_directory()}}:/build -w /build cellar-musl-builder:latest \
        cargo build --release --target aarch64-unknown-linux-musl --bin cellarctl --bin cellard
    @echo "Static binaries at: target/aarch64-unknown-linux-musl/release/"

# Build static binaries for all architectures
build-static-all: build-musl-builder
    @echo "Building static binaries for all architectures..."
    docker run --rm -v {{justfile_directory()}}:/build -w /build cellar-musl-builder:latest \
        sh -c "cargo build --release --target x86_64-unknown-linux-musl --bin cellarctl --bin cellard && \
               cargo build --release --target aarch64-unknown-linux-musl --bin cellarctl --bin cellard"
    @echo "Static binaries at:"
    @echo "  x86_64: target/x86_64-unknown-linux-musl/release/"
    @echo "  aarch64: target/aarch64-unknown-linux-musl/release/"

# Build static binaries via Docker (both architectures, handles ARM64 hosts)
build-static-docker: build-musl-builder
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Building static binaries via Docker (both architectures)..."
    # On ARM64 hosts, override linker to rust-lld to avoid zig CRT conflicts
    if [ "$(uname -m)" = "arm64" ] || [ "$(uname -m)" = "aarch64" ]; then
        docker run --rm -v {{justfile_directory()}}:/build -w /build \
            -e CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
            -e CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_LINKER=rust-lld \
            -e 'CARGO_TARGET_X86_64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-C target-feature=+crt-static -C link-self-contained=yes' \
            -e 'CARGO_TARGET_AARCH64_UNKNOWN_LINUX_MUSL_RUSTFLAGS=-C target-feature=+crt-static -C link-self-contained=yes' \
            cellar-musl-builder:latest \
            cargo build --release --target x86_64-unknown-linux-musl --target aarch64-unknown-linux-musl --bin cellarctl --bin cellard
    else
        docker run --rm -v {{justfile_directory()}}:/build -w /build cellar-musl-builder:latest \
            cargo build --release --target x86_64-unknown-linux-musl --target aarch64-unknown-linux-musl --bin cellarctl --bin cellard
    fi

# ============================================================================
# Test Commands
# ============================================================================

# Run all tests (API + CLI)
test: test-all

# Run all test configurations
test-all: test-postgres-fs test-sqlite-s3 test-cli
    @echo ""
    @echo "============================================"
    @echo "All test configurations completed!"
    @echo "============================================"

# Run API tests with PostgreSQL + Filesystem
test-postgres-fs: build
    @echo ""
    @echo "============================================"
    @echo "Running tests: PostgreSQL + Filesystem"
    @echo "============================================"
    docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
    @echo ""
    @echo "PostgreSQL + Filesystem tests completed successfully."
    docker compose -f docker/compose/postgres-fs.yml down -v

# Run API tests with SQLite + MinIO (S3)
test-sqlite-s3: build
    @echo ""
    @echo "============================================"
    @echo "Running tests: SQLite + MinIO (S3)"
    @echo "============================================"
    docker compose -f docker/compose/sqlite-s3.yml up --build --abort-on-container-exit --exit-code-from test-runner
    @echo ""
    @echo "SQLite + S3 tests completed successfully."
    docker compose -f docker/compose/sqlite-s3.yml down -v

# Run CLI tests (cellarctl with SQLite + Filesystem)
test-cli: build-server build-cli-test
    @echo ""
    @echo "============================================"
    @echo "Running CLI tests: cellarctl (SQLite + Filesystem)"
    @echo "============================================"
    docker compose -f docker/compose/sqlite-fs.yml up --build --abort-on-container-exit --exit-code-from cli-test
    @echo ""
    @echo "CLI tests completed successfully."
    docker compose -f docker/compose/sqlite-fs.yml down -v

# Run quick CLI test (basic push only)
test-cli-quick: build-server build-cli-test
    @echo "Running quick CLI test (basic push only)..."
    TEST_SCENARIOS=01-cellarctl-push docker compose -f docker/compose/sqlite-fs.yml up --build --abort-on-container-exit --exit-code-from cli-test
    @echo ""
    @echo "Quick CLI test completed successfully."
    docker compose -f docker/compose/sqlite-fs.yml down -v

# Run only health check tests
test-quick:
    @echo "Running quick health check tests..."
    TEST_SCENARIOS=01-health-check docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
    @echo ""
    @echo "Quick health check tests completed successfully."
    docker compose -f docker/compose/postgres-fs.yml down -v

# Run specific test scenario (e.g., just test-scenario 03-upload-flow postgres-fs)
test-scenario scenario config="postgres-fs":
    @echo "Running test scenario: {{scenario}} with config: {{config}}"
    TEST_SCENARIOS={{scenario}} docker compose -f docker/compose/{{config}}.yml up --build --abort-on-container-exit --exit-code-from test-runner
    @echo ""
    @echo "Test scenario completed successfully."
    docker compose -f docker/compose/{{config}}.yml down -v

# Run large upload tests (tests chunked uploads with simulated large packages)
test-large:
    @echo "Running large upload tests..."
    TEST_SCENARIOS=09-large-upload docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
    @echo ""
    @echo "Large upload tests completed successfully."
    docker compose -f docker/compose/postgres-fs.yml down -v

# Run resumable upload tests
test-resume:
    @echo "Running resumable upload tests..."
    TEST_SCENARIOS=10-resume-upload docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
    @echo ""
    @echo "Resumable upload tests completed successfully."
    docker compose -f docker/compose/postgres-fs.yml down -v

# Build and push HashiCorp Vault (real large package test, ~5 min build)
test-vault: up-postgres-fs
    @echo ""
    @echo "============================================"
    @echo "Building and pushing HashiCorp Vault"
    @echo "This will take approximately 4-5 minutes"
    @echo "============================================"
    docker exec -it cellar-builder build-and-push vault
    @echo ""
    @echo "Vault push completed. Run 'just down' when done."

# ============================================================================
# Manual Package Testing (requires running environment)
# ============================================================================

# Build and push vault to the running cache server
push-vault:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker ps | grep -q cellar-builder; then
        echo "Error: Builder container not running. Start with 'just up-postgres-fs' or 'just up-sqlite-s3' first."
        exit 1
    fi
    echo "Building and pushing HashiCorp Vault..."
    docker exec -it cellar-builder build-and-push vault

# Build and push a specific package (e.g., just push-package hello)
push-package pkg:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker ps | grep -q cellar-builder; then
        echo "Error: Builder container not running. Start with 'just up-postgres-fs' or 'just up-sqlite-s3' first."
        exit 1
    fi
    echo "Building and pushing {{pkg}}..."
    docker exec -it cellar-builder build-and-push package {{pkg}}

# Test resumable upload with a real package (e.g., just test-resume-manual hello 2)
test-resume-manual pkg="hello" interrupt_at="2":
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker ps | grep -q cellar-builder; then
        echo "Error: Builder container not running. Start with 'just up-postgres-fs' or 'just up-sqlite-s3' first."
        exit 1
    fi
    echo "Testing resumable upload with {{pkg}} (interrupt after chunk {{interrupt_at}})..."
    docker exec -it cellar-builder test-resume-upload {{pkg}} {{interrupt_at}}

# ============================================================================
# Development Commands
# ============================================================================

# Start dev environment (server + nix client for interactive testing)
dev:
    @echo "Building dev environment..."
    docker compose -f docker/dev/docker-compose.yml up -d --build
    @echo ""
    @echo "Dev environment ready."
    @echo "  Server: http://localhost:8080"
    @echo ""
    @echo "Shell into the client:"
    @echo "  docker compose -f docker/dev/docker-compose.yml exec client bash"
    @echo ""
    @echo "Stop with: just dev-down"

# Stop dev environment
dev-down:
    docker compose -f docker/dev/docker-compose.yml down -v

# Shell into dev client container
dev-shell:
    docker compose -f docker/dev/docker-compose.yml exec client bash

# Follow dev environment logs
dev-logs:
    docker compose -f docker/dev/docker-compose.yml logs -f

# Start PostgreSQL + Filesystem environment
up-postgres-fs: build
    @echo "Starting PostgreSQL + Filesystem environment..."
    docker compose -f docker/compose/postgres-fs.yml up -d postgres server builder client
    @echo ""
    @echo "Services started. Access:"
    @echo "  Server: http://localhost:8080"
    @echo "  PostgreSQL: localhost:5432"
    @echo ""
    @echo "Run 'just logs' to see container logs"
    @echo "Run 'just down' to stop all containers"

# Start SQLite + MinIO environment
up-sqlite-s3: build
    @echo "Starting SQLite + MinIO environment..."
    docker compose -f docker/compose/sqlite-s3.yml up -d minio minio-init server builder client
    @echo ""
    @echo "Services started. Access:"
    @echo "  Server: http://localhost:8080"
    @echo "  MinIO Console: http://localhost:9001"
    @echo "  MinIO API: http://localhost:9000"
    @echo ""
    @echo "Run 'just logs' to see container logs"
    @echo "Run 'just down' to stop all containers"

# Stop all containers
down:
    @echo "Stopping all containers..."
    -docker compose -f docker/dev/docker-compose.yml down -v 2>/dev/null || true
    -docker compose -f docker/compose/postgres-fs.yml down -v 2>/dev/null || true
    -docker compose -f docker/compose/sqlite-s3.yml down -v 2>/dev/null || true
    -docker compose -f docker/compose/sqlite-fs.yml down -v 2>/dev/null || true
    @echo "All containers stopped"

# Follow container logs
logs:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Following logs (Ctrl+C to stop)..."
    if docker compose -f docker/compose/postgres-fs.yml ps -q 2>/dev/null | grep -q .; then
        docker compose -f docker/compose/postgres-fs.yml logs -f
    elif docker compose -f docker/compose/sqlite-s3.yml ps -q 2>/dev/null | grep -q .; then
        docker compose -f docker/compose/sqlite-s3.yml logs -f
    else
        echo "No running containers found"
    fi

# Follow server container logs
logs-server:
    docker logs -f cellar-server 2>/dev/null || echo "Server container not running"

# Remove containers, volumes, and local images
clean:
    @echo "Cleaning up Docker resources..."
    -docker compose -f docker/dev/docker-compose.yml down -v --rmi local 2>/dev/null || true
    -docker compose -f docker/compose/postgres-fs.yml down -v --rmi local 2>/dev/null || true
    -docker compose -f docker/compose/sqlite-s3.yml down -v --rmi local 2>/dev/null || true
    -docker compose -f docker/compose/sqlite-fs.yml down -v --rmi local 2>/dev/null || true
    -docker rmi cellar-server:latest cellar-builder:latest cellar-client:latest cellar-test-runner:latest cellar-cli-test:latest cellar-musl-builder:latest 2>/dev/null || true
    -docker volume prune -f
    @echo "Cleanup complete"

# Deep clean: remove everything including base images and build cache
clean-all: clean
    @echo "Deep cleaning Docker resources..."
    -docker rmi postgres:16-alpine minio/minio:latest minio/mc:latest nixos/nix:2.24.10 2>/dev/null || true
    -docker rmi alpine:3.20 debian:bookworm-slim rust:1.92-bookworm 2>/dev/null || true
    -docker builder prune -af
    -docker system prune -f
    @echo "Deep cleanup complete"

# ============================================================================
# Shell Access
# ============================================================================

# Open shell in server container
shell-server:
    docker exec -it cellar-server /bin/bash || echo "Server container not running. Start with 'just up-postgres-fs' or 'just up-sqlite-s3'"

# Open shell in builder container
shell-builder:
    docker exec -it cellar-builder /bin/bash || echo "Builder container not running. Start with 'just up-postgres-fs' or 'just up-sqlite-s3'"

# Open shell in client container
shell-client:
    docker exec -it cellar-client /bin/bash || echo "Client container not running. Start with 'just up-postgres-fs' or 'just up-sqlite-s3'"

# Open shell in CLI test container
shell-cli-test:
    docker exec -it cellar-cli-test /bin/bash || echo "CLI test container not running"

# ============================================================================
# Utilities
# ============================================================================

# Generate a new signing keypair
generate-key:
    @echo "Generating signing keypair..."
    @mkdir -p ./keys
    bash docker/scripts/generate-test-key.sh "cellar-key-$(date +%Y%m%d)" ./keys
    @echo "Keys generated in ./keys/"

# Check prerequisites (docker, docker compose)
check-deps:
    #!/usr/bin/env bash
    set -euo pipefail
    echo "Checking dependencies..."
    command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed."; exit 1; }
    docker compose version >/dev/null 2>&1 || { echo "Docker Compose plugin is required but not installed."; exit 1; }
    echo "All dependencies satisfied"

# Show test results from last run
results:
    #!/usr/bin/env bash
    if [ -f docker/tests/results/results.json ]; then
        cat docker/tests/results/results.json | jq .
    else
        echo "No test results found. Run 'just test' first."
    fi

# ============================================================================
# CI/CD Targets
# ============================================================================

# Run code checks in Docker (fmt + clippy + unit tests, no Docker socket)
code-check:
    @echo "Running code checks in Docker (fmt + clippy + unit tests)..."
    docker run --rm -v {{justfile_directory()}}:/build -w /build \
        -e SQLX_OFFLINE=true \
        -e CARGO_TARGET_DIR=/build/target/docker \
        -e CARGO_BUILD_JOBS=2 \
        rust:1.92-bookworm \
        sh -c "rustup component add rustfmt clippy && \
               cargo clippy --workspace --all-targets --all-features -- -D warnings && \
               cargo fmt --check && \
               cargo test --workspace --all-features --exclude cellar-server --exclude cellar-storage && \
               cargo test --package cellar-server --all-features --lib && \
               cargo test --package cellar-storage --all-features --lib"

# Run full code checks with Docker socket (includes testcontainer integration tests)
# SECURITY NOTE: Mounting Docker socket grants container access to host Docker daemon.
code-check-full:
    @echo "Running full code checks in Docker (includes testcontainer integration tests)..."
    docker run --rm -v {{justfile_directory()}}:/build -w /build \
        -v /var/run/docker.sock:/var/run/docker.sock \
        -e SQLX_OFFLINE=true \
        -e CARGO_TARGET_DIR=/build/target/docker \
        -e CARGO_BUILD_JOBS=2 \
        rust:1.92-bookworm \
        sh -c "rustup component add rustfmt clippy && \
               cargo clippy --workspace --all-targets --all-features -- -D warnings && \
               cargo fmt --check && \
               cargo test --workspace --all-features"

# Run CI tests (check deps, build, test all)
ci-test: check-deps build test-all
    @echo "CI tests completed"

# Run CI build (check deps, build)
ci-build: check-deps build
    @echo "CI build completed"

# Create a release: bump version, update changelog, commit and push
release version:
    #!/usr/bin/env bash
    set -euo pipefail

    VERSION="{{version}}"
    # Strip 'v' prefix if provided (e.g., v0.2.0 -> 0.2.0)
    VERSION="${VERSION#v}"

    echo "Preparing release v${VERSION}..."

    # 1. Update version in Cargo.toml
    sed -i.bak 's/^version = ".*"/version = "'"${VERSION}"'"/' Cargo.toml
    rm -f Cargo.toml.bak

    # 2. Update Cargo.lock
    cargo check --quiet

    # 3. Update CHANGELOG.md with git-cliff (skip alpha/beta/rc tags)
    git-cliff --config keepachangelog --tag "v${VERSION}" --output CHANGELOG.md --ignore-tags ".*-.*"

    # 4. Commit changes
    git add Cargo.toml Cargo.lock CHANGELOG.md
    git commit -m "chore: prepare for v${VERSION}"

    # 5. Create tag
    git tag -s "v${VERSION}" -m "v${VERSION}"

    # 6. Push commit and tag
    git push origin main
    git push origin "v${VERSION}"

    echo "Released v${VERSION} successfully!"
