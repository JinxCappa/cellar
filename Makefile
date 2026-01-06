# Cellar Makefile
# Commands for building, testing, and managing Docker environments

.PHONY: help build build-server build-nix build-test-runner \
        test test-postgres-fs test-sqlite-s3 test-all \
        test-quick test-large test-resume test-vault \
        up-postgres-fs up-sqlite-s3 down clean clean-all logs \
        shell-server shell-builder shell-client \
        push-vault push-package \
        generate-key

# Default target
help:
	@echo "Cellar - Nix Binary Cache Server"
	@echo ""
	@echo "Build Commands:"
	@echo "  make build              - Build all Docker images"
	@echo "  make build-server       - Build server image only"
	@echo "  make build-nix          - Build Nix builder/client images"
	@echo "  make build-test-runner  - Build test runner image"
	@echo ""
	@echo "Test Commands:"
	@echo "  make test               - Run all tests (both configurations)"
	@echo "  make test-postgres-fs   - Run tests with PostgreSQL + Filesystem"
	@echo "  make test-sqlite-s3     - Run tests with SQLite + MinIO (S3)"
	@echo "  make test-quick         - Run only health check tests"
	@echo "  make test-large         - Run large upload tests (chunked uploads)"
	@echo "  make test-resume        - Run resumable upload tests"
	@echo "  make test-vault         - Build and push HashiCorp Vault (~5 min build)"
	@echo ""
	@echo "Manual Testing (requires running environment):"
	@echo "  make push-vault         - Build vault and push to cache"
	@echo "  make push-package PKG=x - Build and push package 'x' to cache"
	@echo ""
	@echo "Development Commands:"
	@echo "  make up-postgres-fs     - Start PostgreSQL + Filesystem environment"
	@echo "  make up-sqlite-s3       - Start SQLite + MinIO environment"
	@echo "  make down               - Stop all containers"
	@echo "  make logs               - Follow container logs"
	@echo "  make clean              - Remove containers, volumes, and local images"
	@echo "  make clean-all          - Deep clean: also removes base images and build cache"
	@echo ""
	@echo "Shell Access:"
	@echo "  make shell-server       - Open shell in server container"
	@echo "  make shell-builder      - Open shell in builder container"
	@echo "  make shell-client       - Open shell in client container"
	@echo ""
	@echo "Utilities:"
	@echo "  make generate-key       - Generate a new signing keypair"

# ============================================================================
# Build Commands
# ============================================================================

build: build-server build-nix build-test-runner
	@echo "All images built successfully"

build-server:
	@echo "Building server image..."
	docker build -t cellar-server:latest -f docker/Dockerfile .

build-nix:
	@echo "Building Nix builder image..."
	docker build -t cellar-builder:latest --target builder -f docker/Dockerfile.nix .
	@echo "Building Nix client image..."
	docker build -t cellar-client:latest --target client -f docker/Dockerfile.nix .

build-test-runner:
	@echo "Building test runner image..."
	docker build -t cellar-test-runner:latest -f docker/tests/runner/Dockerfile .

# ============================================================================
# Test Commands
# ============================================================================

test: test-all

test-all: test-postgres-fs test-sqlite-s3
	@echo ""
	@echo "============================================"
	@echo "All test configurations completed!"
	@echo "============================================"

test-postgres-fs: build
	@echo ""
	@echo "============================================"
	@echo "Running tests: PostgreSQL + Filesystem"
	@echo "============================================"
	@docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
	@docker compose -f docker/compose/postgres-fs.yml down -v

test-sqlite-s3: build
	@echo ""
	@echo "============================================"
	@echo "Running tests: SQLite + MinIO (S3)"
	@echo "============================================"
	@docker compose -f docker/compose/sqlite-s3.yml up --build --abort-on-container-exit --exit-code-from test-runner
	@docker compose -f docker/compose/sqlite-s3.yml down -v

test-quick:
	@echo "Running quick health check tests..."
	@TEST_SCENARIOS=01-health-check docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
	@docker compose -f docker/compose/postgres-fs.yml down -v

# Run specific test scenario
# Usage: make test-scenario SCENARIO=03-upload-flow CONFIG=postgres-fs
test-scenario:
	@if [ -z "$(SCENARIO)" ]; then echo "Usage: make test-scenario SCENARIO=<name> [CONFIG=postgres-fs|sqlite-s3]"; exit 1; fi
	@CONFIG=$${CONFIG:-postgres-fs}; \
	TEST_SCENARIOS=$(SCENARIO) docker compose -f docker/compose/$$CONFIG.yml up --build --abort-on-container-exit --exit-code-from test-runner; \
	docker compose -f docker/compose/$$CONFIG.yml down -v

# Run large upload tests (tests chunked uploads with simulated large packages)
test-large:
	@echo "Running large upload tests..."
	@TEST_SCENARIOS=09-large-upload docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
	@docker compose -f docker/compose/postgres-fs.yml down -v

# Run resumable upload tests
test-resume:
	@echo "Running resumable upload tests..."
	@TEST_SCENARIOS=10-resume-upload docker compose -f docker/compose/postgres-fs.yml up --build --abort-on-container-exit --exit-code-from test-runner
	@docker compose -f docker/compose/postgres-fs.yml down -v

# Build and push HashiCorp Vault (real large package test, ~5 min build)
# This tests actual Nix package building and multi-chunk uploads
test-vault: up-postgres-fs
	@echo ""
	@echo "============================================"
	@echo "Building and pushing HashiCorp Vault"
	@echo "This will take approximately 4-5 minutes"
	@echo "============================================"
	@docker exec -it cellar-builder build-and-push vault
	@echo ""
	@echo "Vault push completed. Run 'make down' when done."

# ============================================================================
# Manual Package Testing (requires running environment)
# ============================================================================

# Build and push vault to the running cache server
push-vault:
	@if ! docker ps | grep -q cellar-builder; then \
		echo "Error: Builder container not running. Start with 'make up-postgres-fs' or 'make up-sqlite-s3' first."; \
		exit 1; \
	fi
	@echo "Building and pushing HashiCorp Vault..."
	@docker exec -it cellar-builder build-and-push vault

# Build and push a specific package
# Usage: make push-package PKG=hello
push-package:
	@if [ -z "$(PKG)" ]; then echo "Usage: make push-package PKG=<package-name>"; exit 1; fi
	@if ! docker ps | grep -q cellar-builder; then \
		echo "Error: Builder container not running. Start with 'make up-postgres-fs' or 'make up-sqlite-s3' first."; \
		exit 1; \
	fi
	@echo "Building and pushing $(PKG)..."
	@docker exec -it cellar-builder build-and-push package $(PKG)

# Test resumable upload with a real package (manual test)
# Usage: make test-resume-manual PKG=hello INTERRUPT_AT=2
test-resume-manual:
	@if ! docker ps | grep -q cellar-builder; then \
		echo "Error: Builder container not running. Start with 'make up-postgres-fs' or 'make up-sqlite-s3' first."; \
		exit 1; \
	fi
	@PKG=$${PKG:-hello}; \
	INTERRUPT=$${INTERRUPT_AT:-2}; \
	echo "Testing resumable upload with $$PKG (interrupt after chunk $$INTERRUPT)..."; \
	docker exec -it cellar-builder test-resume-upload $$PKG $$INTERRUPT

# ============================================================================
# Development Commands
# ============================================================================

up-postgres-fs: build
	@echo "Starting PostgreSQL + Filesystem environment..."
	docker compose -f docker/compose/postgres-fs.yml up -d postgres server builder client
	@echo ""
	@echo "Services started. Access:"
	@echo "  Server: http://localhost:8080"
	@echo "  PostgreSQL: localhost:5432"
	@echo ""
	@echo "Run 'make logs' to see container logs"
	@echo "Run 'make down' to stop all containers"

up-sqlite-s3: build
	@echo "Starting SQLite + MinIO environment..."
	docker compose -f docker/compose/sqlite-s3.yml up -d minio minio-init server builder client
	@echo ""
	@echo "Services started. Access:"
	@echo "  Server: http://localhost:8080"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  MinIO API: http://localhost:9000"
	@echo ""
	@echo "Run 'make logs' to see container logs"
	@echo "Run 'make down' to stop all containers"

down:
	@echo "Stopping all containers..."
	-docker compose -f docker/compose/postgres-fs.yml down -v 2>/dev/null
	-docker compose -f docker/compose/sqlite-s3.yml down -v 2>/dev/null
	@echo "All containers stopped"

logs:
	@echo "Following logs (Ctrl+C to stop)..."
	@if docker compose -f docker/compose/postgres-fs.yml ps -q 2>/dev/null | grep -q .; then \
		docker compose -f docker/compose/postgres-fs.yml logs -f; \
	elif docker compose -f docker/compose/sqlite-s3.yml ps -q 2>/dev/null | grep -q .; then \
		docker compose -f docker/compose/sqlite-s3.yml logs -f; \
	else \
		echo "No running containers found"; \
	fi

logs-server:
	@docker logs -f cellar-server 2>/dev/null || echo "Server container not running"

clean:
	@echo "Cleaning up Docker resources..."
	-docker compose -f docker/compose/postgres-fs.yml down -v --rmi local 2>/dev/null
	-docker compose -f docker/compose/sqlite-s3.yml down -v --rmi local 2>/dev/null
	-docker rmi cellar-server:latest cellar-builder:latest cellar-client:latest cellar-test-runner:latest 2>/dev/null
	-docker volume prune -f
	@echo "Cleanup complete"

# Deep clean - removes everything including base images and build cache
clean-all: clean
	@echo "Deep cleaning Docker resources..."
	-docker rmi postgres:16-alpine minio/minio:latest minio/mc:latest nixos/nix:2.24.10 2>/dev/null
	-docker rmi alpine:3.20 debian:bookworm-slim rust:1.85-bookworm 2>/dev/null
	-docker builder prune -af
	-docker system prune -f
	@echo "Deep cleanup complete"

# ============================================================================
# Shell Access
# ============================================================================

shell-server:
	@docker exec -it cellar-server /bin/bash || echo "Server container not running. Start with 'make up-postgres-fs' or 'make up-sqlite-s3'"

shell-builder:
	@docker exec -it cellar-builder /bin/bash || echo "Builder container not running. Start with 'make up-postgres-fs' or 'make up-sqlite-s3'"

shell-client:
	@docker exec -it cellar-client /bin/bash || echo "Client container not running. Start with 'make up-postgres-fs' or 'make up-sqlite-s3'"

# ============================================================================
# Utilities
# ============================================================================

generate-key:
	@echo "Generating signing keypair..."
	@mkdir -p ./keys
	@bash docker/scripts/generate-test-key.sh "cellar-key-$$(date +%Y%m%d)" ./keys
	@echo "Keys generated in ./keys/"

# Check prerequisites
check-deps:
	@echo "Checking dependencies..."
	@command -v docker >/dev/null 2>&1 || { echo "Docker is required but not installed."; exit 1; }
	@command -v docker compose >/dev/null 2>&1 || { echo "Docker Compose is required but not installed."; exit 1; }
	@echo "All dependencies satisfied"

# Show test results from last run
results:
	@if [ -f docker/tests/results/results.json ]; then \
		cat docker/tests/results/results.json | jq .; \
	else \
		echo "No test results found. Run 'make test' first."; \
	fi

# ============================================================================
# CI/CD Targets
# ============================================================================

ci-test: check-deps build test-all
	@echo "CI tests completed"

ci-build: check-deps build
	@echo "CI build completed"
