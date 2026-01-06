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
- **Multi-tenant** — Isolate caches for different teams or projects

## Quickstart: Day 0 → Day 1

### Day 0: Server Setup (15 min)

**Option A: Docker (recommended)**

```bash
# Clone and build
git clone https://github.com/anthropics/cellar && cd cellar
make build

# Start with PostgreSQL + filesystem storage
make up-postgres-fs

# Server is now running at http://localhost:8080
curl http://localhost:8080/v1/health
```

The Docker setup includes a pre-configured admin token: `cellar-test-token-12345`

**Option B: Native (requires Rust 1.85+)**

```bash
# Build
cargo build --release

# Generate an admin token
cargo run --bin cellarctl -- token generate
# Output:
#   Secret: <SAVE_THIS_TOKEN>
#   Hash:   sha256:abc123...

# Create config (copy and edit)
cp config/server.dev.toml config/server.toml
# Add the hash to [admin] section:
#   token_hash = "sha256:abc123..."

# Start server
./target/release/cellar-server --config config/server.toml
```

### Day 0: Create Your Cache (5 min)

```bash
# Set admin credentials
export CELLAR_SERVER="http://localhost:8080"
export CELLAR_TOKEN="<your-admin-token>"

# Create your first cache
cellarctl cache create --name "main" --public --default

# Create a push token for CI/developers
cellarctl token create \
  --scopes "cache:read,cache:write" \
  --description "CI push token"
# Save the token_secret!
```

### Day 0: Client Setup (5 min)

```bash
# On your dev machine or CI runner
cellarctl login main http://your-server:8080 \
  --token-stdin --set-default <<< "your-push-token"

# Update nix.conf with substituter + public key
cellarctl use main --set-nix-conf

# Verify
cellarctl whoami
```

### Day 1: Push Your First Package

```bash
# Build and push a flake output
cellarctl push --flake .#mypackage

# Or push existing store paths
nix build .#mypackage
cellarctl push /nix/store/abc123-mypackage

# Or push with closure (all dependencies)
nix-store -qR /nix/store/abc123-mypackage | xargs cellarctl push
```

### Day 1: Use Your Cache

On any machine with Nix:

```bash
# Add to ~/.config/nix/nix.conf (or /etc/nix/nix.conf)
substituters = http://your-server:8080
trusted-public-keys = main:YOUR_PUBLIC_KEY_HERE

# Now builds fetch from your cache automatically
nix build .#mypackage  # Cache hit!
```

### Day 1: CI Integration (GitHub Actions example)

```yaml
- name: Configure Cellar cache
  run: |
    mkdir -p ~/.config/nix
    echo "substituters = https://cache.example.com" >> ~/.config/nix/nix.conf
    echo "trusted-public-keys = main:${{ secrets.CACHE_PUBLIC_KEY }}" >> ~/.config/nix/nix.conf

- name: Build
  run: nix build .#mypackage

- name: Push to cache
  env:
    CELLAR_SERVER: https://cache.example.com
    CELLAR_TOKEN: ${{ secrets.CELLAR_PUSH_TOKEN }}
  run: |
    cellarctl push /nix/store/*-mypackage
```

## CLI Tool (cellarctl)

Cellar includes `cellarctl` for administrative tasks.

Admin commands (cache/token/gc/stats) require a server URL and admin token:

```bash
export CELLAR_SERVER="http://localhost:8080"
export CELLAR_TOKEN="<admin-secret>"
```

You can also pass `--server` and `--token` together on each command.

### Key Management

```bash
# Generate a new signing key
cellarctl key generate --name "cache-1" --output ./keys/cache-1.sec

# View public key (for Nix trusted-public-keys)
cellarctl key public --key-file ./keys/cache-1.sec
```

### Cache Management

Caches provide logical separation for different teams or projects.

```bash
# Create a new cache
cellarctl cache create --name "team-frontend" --base-url https://cache.example.com

# Create a cache in a specific storage domain
cellarctl cache create --name "team-backend" --domain "production"

# List all caches
cellarctl cache list

# View cache details
cellarctl cache show <CACHE_ID>

# Update a cache
cellarctl cache update <CACHE_ID> --name "new-name"

# Set a cache as the default public cache
cellarctl cache update <CACHE_ID> --public true --default true

# Unset default cache
cellarctl cache update <CACHE_ID> --default false

# Move a cache to a different domain
cellarctl cache update <CACHE_ID> --domain "staging"

# Delete a cache
cellarctl cache delete <CACHE_ID> --force
```

### Storage Domains

Storage domains provide physical isolation of data between caches. Each domain stores its chunks in a separate directory/prefix, ensuring complete data separation for compliance or multi-tenant scenarios.

```bash
# Create a new domain
cellarctl domain create --name "production"

# List all domains
cellarctl domain list

# View domain details (by name or UUID)
cellarctl domain show production
cellarctl domain show <DOMAIN_ID>

# Rename a domain
cellarctl domain rename production --new-name "prod"

# Delete a domain (must have no caches assigned)
cellarctl domain delete staging --force
```

Caches without an explicit domain use the default storage domain.

### Token Management

```bash
# Generate an admin token hash (offline, for config file)
cellarctl token generate

# Create a read-only token
cellarctl token create --scopes "cache:read" --description "CI read token"

# Create a write token scoped to a specific cache
cellarctl token create --scopes "cache:read,cache:write" --cache-id <CACHE_ID>

# Create an admin token with expiration
cellarctl token create --scopes "cache:admin" --expires-in 86400

# List all tokens
cellarctl token list

# Revoke a token
cellarctl token revoke <TOKEN_ID>
```

### Client Usage

```bash
# Save a cache token locally
cellarctl login ci https://cache.example.com --token-stdin --set-default

# Update nix.conf with substituter + trusted public key
cellarctl use ci --set-default --set-nix-conf

# Push a store path
cellarctl push /nix/store/<hash>-<name>

# Push a flake output
cellarctl push --flake .#netbird -- --fallback -L
```

### Client Configuration

The CLI stores credentials in `~/.config/cellar/client.toml` (or `$XDG_CONFIG_HOME/cellar/client.toml`).

You can override the location with `--client-config` or `CELLAR_CLIENT_CONFIG`.

**Example client.toml:**

```toml
# Default cache for push commands
default_cache = "work"

[caches.work]
url = "https://cache.work.example.com"
token = "cellar_abc123..."
cache_id = "550e8400-e29b-41d4-a716-446655440000"
cache_name = "main"
public_base_url = "https://cache.work.example.com"
public_key = "main:BASE64PUBKEY..."

[caches.personal]
url = "https://cache.home.example.com"
token = "cellar_xyz789..."
cache_name = "personal"
public_key = "personal:BASE64PUBKEY..."
```

**Using profiles:**

```bash
# Push to a specific cache (overrides default_cache)
cellarctl push --cache work /nix/store/...
cellarctl push --cache personal /nix/store/...

# Admin commands can also use profiles
cellarctl --profile work cache list
cellarctl --profile personal stats
```

### Garbage Collection

GC runs on a single cache at a time, not globally across all caches.

```bash
# Run GC on the default public cache (cache_id = None)
cellarctl gc run

# Run GC for a specific cache
cellarctl gc run --cache-id <CACHE_ID>

# Run GC on multiple caches (must run separately)
for cache_id in $(cellarctl cache list | awk 'NR>2 {print $1}'); do
  cellarctl gc run --cache-id "$cache_id" --wait
done

# Run a specific GC job type
# Types: upload_gc, chunk_gc, manifest_gc, committing_gc, storage_sweep
cellarctl gc run --job-type chunk_gc
cellarctl gc run --job-type storage_sweep

# Wait for GC completion
cellarctl gc run --wait

# Check GC status
cellarctl gc status
cellarctl gc status <JOB_ID>
cellarctl gc status --limit 10
```

### Stats

```bash
# View server statistics
cellarctl stats
```

### Server Info

```bash
# Check your token identity and permissions
cellarctl whoami

# Check server health and version
cellarctl health
```

## Configuration

Cellar uses TOML config files. Environment variables with the `CELLAR_` prefix can override settings (use `__` for nested keys, e.g., `CELLAR_SERVER__BIND`). See `config/.env.example` for a complete list of environment variables.

You can also specify a config file with `CELLAR_CONFIG=/path/to/config.toml` or `--config`.

```toml
[server]
bind = "127.0.0.1:8080"
max_parallel_chunks = 8
default_chunk_size = 16777216    # 16 MiB
max_chunk_size = 33554432        # 32 MiB
upload_timeout_secs = 86400      # 24 hours
compression = "zstd"             # "zstd" (default), "xz", or "none"
metrics_enabled = true           # Enable /metrics endpoint for Prometheus

[storage]
type = "filesystem"  # or "s3" for cloud storage
path = "./data/storage"

# For S3 storage:
# type = "s3"
# bucket = "my-bucket"
# endpoint = "https://s3.amazonaws.com"
# region = "us-east-1"  # Optional; falls back to AWS env/IMDS, then defaults to us-east-1
# prefix = "cellar/"
# force_path_style = false  # Set true for MinIO or S3-compatible services
# Optional: Explicit AWS credentials (overrides env vars)
# WARNING: Prefer env vars or IAM roles for production deployments.
# access_key_id = "AKIAIOSFODNN7EXAMPLE"
# secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

[metadata]
type = "sqlite"  # or "postgres" for production
path = "./data/metadata.db"

# Admin token (REQUIRED)
[admin]
token_hash = "sha256:abc123..."
# token_scopes = ["cache:admin"]      # Optional, defaults to cache:admin
# token_description = "Admin token"   # Optional description

# For PostgreSQL:
# type = "postgres"
# url = "postgres://user:pass@localhost/cellar"
# max_connections = 20
# statement_timeout_ms = 300000  # 5 min timeout for long GC queries

[signing]
key_name = "cache-1"
[signing.private_key]
type = "file"
path = "./keys/cache-1.sec"

[gc]
grace_period_secs = 3600         # Wait before collecting orphans
batch_size = 1000                # Items per GC batch
dry_run = false                  # Test GC without deleting
# Automatic scheduling (disabled by default)
# auto_schedule_enabled = true
# auto_schedule_interval_secs = 3600  # Run every hour
# auto_schedule_jobs = ["upload_gc", "chunk_gc", "manifest_gc", "committing_gc"]

[rate_limit]
enabled = true
ip_requests_per_minute = 60      # Per-IP limit
token_requests_per_minute = 600  # Per-token limit
burst_size = 20                  # Burst allowance above limit
# trusted_proxies = ["10.0.0.0/8", "172.16.0.0/12"]  # Trust X-Forwarded-For from these IPs
```

See `config/server.dev.toml` for a complete example.

## Project Structure

Cellar is built as a Rust workspace:

| Crate | Purpose |
|-------|---------|
| `core` | Shared types, hashing, and domain logic |
| `storage` | Object storage backends (filesystem, S3) |
| `metadata` | Metadata database (SQLite, PostgreSQL) |
| `signer` | Ed25519 key management and signing |
| `server` | HTTP API server |
| `cli` | Administrative CLI (`cellarctl`) |

## Requirements

- **Rust 1.85+** for building from source
- **Docker** for the containerized setup (recommended)

## API Reference

### Public Endpoints (No Auth)

| Endpoint | What it does |
|----------|--------------|
| `GET /v1/health` | Health check (for k8s probes, load balancers) |
| `GET /v1/capabilities` | Server capabilities (chunk size, limits) |

### For Nix Clients

| Endpoint | What it does |
|----------|--------------|
| `GET /nix-cache-info` | Cache info for Nix |
| `GET /{hash}.narinfo` | Package metadata |
| `GET /nar/{hash}.nar` | Download a package |

### For Uploading

| Endpoint | What it does |
|----------|--------------|
| `POST /v1/uploads` | Start an upload session |
| `GET /v1/uploads/{id}` | Check upload progress |
| `PUT /v1/uploads/{id}/chunks/{hash}` | Upload a chunk |
| `POST /v1/uploads/{id}/commit` | Finish the upload |

### For Authenticated Clients

| Endpoint | What it does |
|----------|--------------|
| `GET /v1/auth/whoami` | Show token identity and cache context |

### For Admins

| Endpoint | What it does |
|----------|--------------|
| `GET /v1/admin/metrics` | Server metrics |
| `GET /v1/admin/tokens` | List all tokens |
| `POST /v1/admin/tokens` | Create access token |
| `DELETE /v1/admin/tokens/{id}` | Revoke a token |
| `GET /v1/admin/gc` | List GC jobs |
| `POST /v1/admin/gc` | Trigger garbage collection |
| `GET /v1/admin/gc/{id}` | Get GC job status |
| `GET /v1/admin/caches` | List all caches |
| `POST /v1/admin/caches` | Create a cache |
| `GET /v1/admin/caches/{id}` | Get cache details |
| `PUT /v1/admin/caches/{id}` | Update a cache |
| `DELETE /v1/admin/caches/{id}` | Delete a cache |
| `GET /v1/admin/domains` | List all domains |
| `POST /v1/admin/domains` | Create a domain |
| `GET /v1/admin/domains/{id}` | Get domain details |
| `GET /v1/admin/domains/by-name/{name}` | Get domain by name |
| `PUT /v1/admin/domains/{id}` | Update a domain |
| `DELETE /v1/admin/domains/{id}` | Delete a domain |

## Access Control

Cellar uses token-based access with three permission levels:

| Token Scope | What you can do |
|-------------|-----------------|
| `cache:read` | Download packages |
| `cache:write` | Upload packages |
| `cache:admin` | Manage tokens, caches, run GC |

Tokens can be scoped to specific caches for multi-tenant isolation.

## Garbage Collection

Cellar automatically manages storage through garbage collection:

- **Upload GC** — Cleans up abandoned upload sessions
- **Chunk GC** — Removes orphaned chunks no longer referenced
- **Manifest GC** — Cleans up unreferenced manifests
- **Committing GC** — Recovers stuck commit operations
- **Storage Sweep** — Scans storage for orphaned files not in the database

GC jobs are resumable and can recover from crashes. A watchdog monitors for panics and automatically recovers within seconds.

## Running Tests

```bash
# Run all tests
make test

# Quick health check
make test-quick

# PostgreSQL + filesystem tests
make test-postgres-fs

# SQLite + S3 (MinIO) tests
make test-sqlite-s3

# Test large uploads
make test-large

# Test resumable uploads
make test-resume
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
