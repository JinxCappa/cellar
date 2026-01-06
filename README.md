# Cellar

Your own private Nix binary cache — fast, reliable, and easy to set up.

## What is Cellar?

If you use [Nix](https://nixos.org/), you know that building packages can take a long time. Cellar solves this by storing your built packages so you (and your team) never have to rebuild them twice.

Think of it as your personal package warehouse. Build once, download everywhere.

## Why Cellar?

- **Saves time** — Skip lengthy rebuilds by caching everything you've already built
- **Works with Nix** — Drop-in compatible with Nix's binary cache system
- **Handles large packages** — Upload big packages in chunks, resume if interrupted
- **Saves disk space** — Automatically deduplicates identical content across packages
- **Flexible storage** — Store packages locally or in the cloud (S3-compatible)
- **Secure** — Sign packages with Ed25519 keys, control access with tokens

## Getting Started

### The Quick Way (Docker)

```bash
# Get everything running
make build
make up-postgres-fs

# That's it! Your cache is ready at http://localhost:8080
```

### For Development

```bash
# Build the project
cargo build

# Start the server
cargo run --bin cellar-server -- --config config/server.dev.toml

# Generate a signing key for your cache
make generate-key
```

## Using Your Cache

Once Cellar is running, point Nix at it:

```bash
# Push a package to your cache
make push-package PKG=hello

# Or configure Nix to use your cache as a substituter
# Add to your nix.conf:
#   substituters = http://localhost:8080
#   trusted-public-keys = cache-1:YOUR_PUBLIC_KEY
```

## Configuration

Cellar uses simple TOML config files. Here's a basic example:

```toml
[server]
bind = "127.0.0.1:8080"

[storage]
type = "filesystem"  # or "s3" for cloud storage
path = "./data/storage"

[metadata]
type = "sqlite"  # or "postgres" for production
path = "./data/metadata.db"

[signing]
key_name = "cache-1"
```

See `config/server.dev.toml` for a complete example.

## Project Structure

Cellar is built as a Rust workspace:

| Crate | Purpose |
|-------|---------|
| `core` | Shared types and logic |
| `storage` | Where packages live (filesystem or S3) |
| `metadata` | Package database (SQLite or PostgreSQL) |
| `signer` | Package signing |
| `server` | The HTTP API |
| `cli` | Command-line admin tools |

## Requirements

- **Rust 1.85+** for building from source
- **Docker** for the containerized setup (recommended)

## API Reference

### For Nix Clients

| Endpoint | What it does |
|----------|--------------|
| `GET /nix-cache-info` | Cache info for Nix |
| `GET /{hash}.narinfo` | Package metadata |
| `GET /nar/{hash}.nar` | Download a package |

### For Uploading

| Endpoint | What it does |
|----------|--------------|
| `POST /v1/uploads` | Start an upload |
| `PUT /v1/uploads/{id}/chunks/{hash}` | Upload a piece |
| `GET /v1/uploads/{id}` | Check upload progress |
| `POST /v1/uploads/{id}/commit` | Finish the upload |

### For Admins

| Endpoint | What it does |
|----------|--------------|
| `GET /v1/admin/health` | Health check |
| `POST /v1/admin/tokens` | Create access tokens |
| `POST /v1/admin/gc` | Clean up unused data |
| `GET /v1/admin/metrics` | Prometheus metrics |

## Access Control

Cellar uses token-based access with three permission levels:

| Token Scope | What you can do |
|-------------|-----------------|
| `cache:read` | Download packages |
| `cache:write` | Upload packages |
| `cache:admin` | Manage tokens, run garbage collection |

## Running Tests

```bash
# Run all tests
make test

# Quick health check
make test-quick

# Test large uploads
make test-large
```

## Useful Commands

```bash
make logs           # View server logs
make shell-server   # Open a shell in the server container
make down           # Stop everything
make clean          # Clean up Docker resources
```

## License

[GNU Affero General Public License v3.0](LICENSE)

## Contributing

Contributions welcome! Open an issue or submit a pull request.
