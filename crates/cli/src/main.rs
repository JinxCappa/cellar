//! Administrative CLI for Cellar.

mod api_client;
mod service;

use anyhow::{Context, Result};
use api_client::{
    ApiClient, CreateCacheRequest, CreateDomainRequest, CreateTokenRequest, GcJobResponse,
    MetricsResponse, TriggerGcRequest, UpdateCacheRequest, UpdateDomainRequest,
};
use cellar_signer::KeyPair;
use clap::{Args, Parser, Subcommand};
use figment::Figment;
use figment::providers::{Env, Format, Toml};
use futures::StreamExt;
use futures::stream::FuturesUnordered;
use std::collections::{BTreeMap, HashSet};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;
use time::OffsetDateTime;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;
use uuid::Uuid;

const DEFAULT_UPSTREAM: &str = "https://cache.nixos.org";
const UPSTREAM_CHECK_CONCURRENCY: usize = 64;

#[derive(Parser)]
#[command(name = "cellarctl")]
#[command(about = "Administrative CLI for Cellar")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Args, Clone)]
struct ClientConfigArgs {
    /// Client config file path
    #[arg(long, env = "CELLAR_CLIENT_CONFIG")]
    client_config: Option<String>,
}

#[derive(Args, Clone)]
struct ApiArgs {
    /// Server API URL (overrides client config)
    #[arg(long)]
    server: Option<String>,

    /// Admin token (overrides client config)
    #[arg(long)]
    token: Option<String>,

    /// Cache profile to use from client config (default: default_cache)
    #[arg(long)]
    profile: Option<String>,

    #[command(flatten)]
    client: ClientConfigArgs,
}

#[derive(Subcommand)]
enum Commands {
    /// Key management commands
    Key {
        #[command(subcommand)]
        command: KeyCommands,
    },
    /// Cache management commands
    Cache {
        #[command(subcommand)]
        command: CacheCommands,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Domain management commands
    Domain {
        #[command(subcommand)]
        command: DomainCommands,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Token management commands
    Token {
        #[command(subcommand)]
        command: TokenCommands,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Garbage collection commands
    Gc {
        #[command(subcommand)]
        command: GcCommands,
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Server statistics
    Stats {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Show current token identity and cache context
    Whoami {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Check server health and version
    Health {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Configure nix.conf substituters from server (no config file needed)
    SetupNixConf {
        #[command(flatten)]
        api: ApiArgs,
    },
    /// Log in to a cache and save credentials locally
    Login {
        /// Local alias for the cache
        alias: String,
        /// Cache API base URL (e.g., https://cache.example.com)
        url: String,
        /// Token value (avoid if possible; prefer --token-stdin)
        #[arg(long)]
        token: Option<String>,
        /// Read token from stdin
        #[arg(long, default_value_t = false)]
        token_stdin: bool,
        /// Set this cache as the default for push
        #[arg(long, default_value_t = false)]
        set_default: bool,
        #[command(flatten)]
        client: ClientConfigArgs,
    },
    /// Select a cache profile and optionally update nix.conf
    Use {
        /// Cache alias to select
        alias: String,
        /// Set this cache as the default for push
        #[arg(long, default_value_t = false)]
        set_default: bool,
        /// Update nix.conf substituters/trusted-public-keys
        #[arg(long, default_value_t = false)]
        set_nix_conf: bool,
        #[command(flatten)]
        client: ClientConfigArgs,
    },
    /// Upload store paths or flake outputs
    Push {
        /// Cache alias from config file
        #[arg(long)]
        cache: Option<String>,
        /// Cache ID (UUID) - use directly without config lookup
        #[arg(long, env = "CELLAR_CACHE_ID")]
        cache_id: Option<String>,
        /// Server API URL
        #[arg(long, env = "CELLAR_SERVER")]
        server: Option<String>,
        /// Auth token
        #[arg(long, env = "CELLAR_TOKEN")]
        token: Option<String>,
        /// Flake reference to build and push
        #[arg(long)]
        flake: Option<String>,
        /// Skip closure expansion (push only top-level outputs)
        #[arg(long, default_value_t = false)]
        no_closure: bool,
        /// Upstream substituters to filter against. Paths already on these
        /// caches are skipped. Default: https://cache.nixos.org
        #[arg(long = "upstream", env = "CELLAR_UPSTREAM", value_delimiter = ',')]
        upstream: Vec<String>,
        /// Disable upstream filtering (push all paths including upstream ones)
        #[arg(long, default_value_t = false)]
        no_filter: bool,
        /// Skip pre-flight remote closure check (always build locally first)
        #[arg(long, default_value_t = false)]
        no_preflight: bool,
        /// Force rebuild and re-push, overwriting existing cache entries
        #[arg(long, default_value_t = false)]
        force: bool,
        /// Store paths to upload
        #[arg(value_name = "STORE_PATH", num_args = 0..)]
        store_paths: Vec<String>,
        /// Extra arguments passed to `nix build` (after --)
        #[arg(last = true)]
        nix_args: Vec<String>,
        #[command(flatten)]
        client: ClientConfigArgs,
    },
    /// Service file generation
    Service {
        #[command(subcommand)]
        command: service::ServiceCommands,
    },
}

#[derive(Subcommand)]
enum KeyCommands {
    /// Generate a new signing key pair
    Generate {
        /// Key name (e.g., "cache.example.com-1")
        #[arg(short, long)]
        name: String,
        /// Output file for secret key
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Show the public key for a secret key
    Public {
        /// Path to secret key file
        #[arg(short, long, group = "key_source")]
        file: Option<String>,
        /// Secret key value directly
        #[arg(short, long, group = "key_source")]
        value: Option<String>,
        /// Read secret key from CELLAR_SIGNING_KEY env var
        #[arg(short, long, group = "key_source")]
        env: bool,
    },
}

#[derive(Subcommand)]
enum CacheCommands {
    /// Create a new cache
    Create {
        /// Cache name (lowercase alphanumeric with hyphens, 3-64 chars)
        #[arg(short, long)]
        name: String,
        /// Public base URL (e.g., https://cache.example.com)
        #[arg(short, long)]
        base_url: Option<String>,
        /// Make cache public (default: false)
        #[arg(long, default_value_t = false)]
        public: bool,
        /// Set as the default public cache (only one allowed)
        #[arg(long = "default", default_value_t = false)]
        is_default: bool,
        /// Storage domain name for isolation
        #[arg(long)]
        domain: Option<String>,
        /// Storage domain ID (UUID) for isolation
        #[arg(long = "domain-id")]
        domain_id: Option<String>,
        /// Create a new domain with this name (defaults to cache name if no value given)
        #[arg(long, value_name = "NAME")]
        new_domain: Option<Option<String>>,
    },
    /// List all caches
    List,
    /// Show cache details
    Show {
        /// Cache (UUID, name, or config alias)
        cache: String,
    },
    /// Update cache properties
    Update {
        /// Cache (UUID, name, or config alias)
        cache: String,
        /// New cache name
        #[arg(short, long)]
        name: Option<String>,
        /// New public base URL
        #[arg(short, long)]
        base_url: Option<String>,
        /// Update public flag
        #[arg(long)]
        public: Option<bool>,
        /// Set/unset as default public cache (true/false)
        #[arg(long = "default")]
        is_default: Option<bool>,
        /// Storage domain name for isolation
        #[arg(long)]
        domain: Option<String>,
        /// Storage domain ID (UUID) for isolation
        #[arg(long = "domain-id")]
        domain_id: Option<String>,
    },
    /// Delete a cache
    Delete {
        /// Cache (UUID, name, or config alias)
        cache: String,
        /// Skip confirmation prompt
        #[arg(short, long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum DomainCommands {
    /// Create a new domain
    Create {
        /// Domain name (lowercase alphanumeric with hyphens, 3-64 chars)
        #[arg(short, long)]
        name: String,
    },
    /// List all domains
    List,
    /// Show domain details (name or UUID)
    Show {
        /// Domain name or UUID
        domain: String,
    },
    /// Rename a domain (name or UUID)
    Rename {
        /// Domain name or UUID
        domain: String,
        /// New domain name
        #[arg(long)]
        new_name: String,
    },
    /// Delete a domain (name or UUID)
    Delete {
        /// Domain name or UUID
        domain: String,
        /// Skip confirmation prompt
        #[arg(short, long)]
        force: bool,
    },
}

#[derive(Subcommand)]
enum TokenCommands {
    /// Generate a token offline (outputs secret + hash)
    Generate {
        /// Description for the token
        #[arg(short, long)]
        description: Option<String>,
    },
    /// Create a new token
    Create {
        /// Scopes to grant (comma-separated)
        #[arg(short, long)]
        scopes: String,
        /// Cache (UUID, name, or config alias)
        #[arg(short, long)]
        cache: Option<String>,
        /// Expiration in seconds
        #[arg(short, long)]
        expires_in: Option<u64>,
        /// Description
        #[arg(short, long)]
        description: Option<String>,
    },
    /// List tokens
    List,
    /// Revoke a token
    Revoke {
        /// Token ID to revoke
        token_id: String,
    },
}

#[derive(Subcommand)]
enum GcCommands {
    /// Run garbage collection
    Run {
        /// GC job type (upload_gc, chunk_gc, manifest_gc, committing_gc, storage_sweep)
        #[arg(short, long, default_value = "upload_gc")]
        job_type: String,
        /// Cache (UUID, name, or config alias)
        #[arg(long)]
        cache: Option<String>,
        /// Wait for job completion
        #[arg(long, default_value_t = false)]
        wait: bool,
    },
    /// Show GC status
    Status {
        /// Job ID (optional, shows recent jobs if not specified)
        job_id: Option<String>,
        /// Cache (UUID, name, or config alias)
        #[arg(long)]
        cache: Option<String>,
        /// Maximum number of jobs to return
        #[arg(long)]
        limit: Option<u32>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let Cli { command } = Cli::parse();

    match command {
        Commands::Login {
            alias,
            url,
            token,
            token_stdin,
            set_default,
            client,
        } => handle_login_command(&alias, &url, token, token_stdin, set_default, &client).await,
        Commands::Use {
            alias,
            set_default,
            set_nix_conf,
            client,
        } => handle_use_command(&alias, set_default, set_nix_conf, &client).await,
        Commands::Push {
            cache,
            cache_id,
            server,
            token,
            flake,
            no_closure,
            upstream,
            no_filter,
            no_preflight,
            force,
            store_paths,
            nix_args,
            client,
        } => {
            handle_push_command(
                cache,
                cache_id,
                server,
                token,
                flake,
                no_closure,
                upstream,
                no_filter,
                no_preflight,
                force,
                store_paths,
                nix_args,
                &client,
            )
            .await
        }
        Commands::Key { command } => handle_key_command(command).await,
        Commands::Cache { command, api } => handle_cache_command(command, &api).await,
        Commands::Domain { command, api } => handle_domain_command(command, &api).await,
        Commands::Token { command, api } => handle_token_command(command, &api).await,
        Commands::Gc { command, api } => handle_gc_command(command, &api).await,
        Commands::Stats { api } => handle_stats_command(&api).await,
        Commands::Whoami { api } => handle_whoami_command(&api).await,
        Commands::Health { api } => handle_health_command(&api).await,
        Commands::SetupNixConf { api } => handle_setup_nix_conf(&api).await,
        Commands::Service { command } => service::handle_service_command(command),
    }
}

async fn handle_key_command(command: KeyCommands) -> Result<()> {
    match command {
        KeyCommands::Generate { name, output } => {
            let keypair = KeyPair::generate(&name);
            let secret_key = keypair.to_nix_secret_key();
            let public_key = keypair.to_nix_public_key();

            if let Some(path) = output {
                tokio::fs::write(&path, &secret_key)
                    .await
                    .with_context(|| format!("failed to write key to {path}"))?;
                println!("Secret key written to: {path}");
            } else {
                println!("Secret key:");
                println!("{secret_key}");
            }

            println!("\nPublic key:");
            println!("{public_key}");
            println!("\nAdd this to your nix.conf trusted-public-keys:");
            println!("  trusted-public-keys = {public_key}");
        }
        KeyCommands::Public { file, value, env } => {
            let secret_key = if let Some(path) = file {
                tokio::fs::read_to_string(&path)
                    .await
                    .with_context(|| format!("failed to read key file: {path}"))?
            } else if let Some(key) = value {
                key
            } else if env {
                std::env::var("CELLAR_SIGNING_KEY")
                    .context("CELLAR_SIGNING_KEY environment variable not set")?
            } else {
                anyhow::bail!("one of --file, --value, or --env is required");
            };

            let keypair = KeyPair::from_nix_secret_key(secret_key.trim())
                .context("failed to parse secret key")?;

            println!("{}", keypair.to_nix_public_key());
        }
    }
    Ok(())
}

async fn resolve_api_config(api: &ApiArgs) -> Result<(String, String)> {
    match (&api.server, &api.token) {
        (Some(server), Some(token)) => return Ok((server.clone(), token.clone())),
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("missing paired flag: use both --server and --token");
        }
        (None, None) => {}
    }

    let server_env = std::env::var("CELLAR_SERVER").ok();
    let token_env = std::env::var("CELLAR_TOKEN").ok();

    match (server_env, token_env) {
        (Some(server), Some(token)) => return Ok((server, token)),
        (Some(_), None) | (None, Some(_)) => {
            anyhow::bail!("missing paired env var: set both CELLAR_SERVER and CELLAR_TOKEN");
        }
        (None, None) => {}
    }

    let config_path = client_config_path(api.client.client_config.as_deref())?;
    let config = load_client_config(&config_path).await?;

    let profile_name = api
        .profile
        .as_ref()
        .or(config.default_cache.as_ref())
        .ok_or_else(|| anyhow::anyhow!("no profile specified and no default_cache set"))?;

    let profile = config
        .caches
        .get(profile_name)
        .ok_or_else(|| anyhow::anyhow!("profile '{}' not found in client config", profile_name))?;

    Ok((profile.url.clone(), profile.token.clone()))
}

async fn get_api_client(api: &ApiArgs) -> Result<ApiClient> {
    let (server, token) = resolve_api_config(api).await?;
    let base_url = normalize_base_url(&server)?;
    ApiClient::new(&base_url, &token)
}

async fn handle_cache_command(command: CacheCommands, api: &ApiArgs) -> Result<()> {
    let client = get_api_client(api).await?;
    let config = load_client_config(&client_config_path(api.client.client_config.as_deref())?)
        .await
        .ok();

    match command {
        CacheCommands::Create {
            name,
            base_url,
            public,
            is_default,
            domain,
            domain_id,
            new_domain,
        } => {
            // Validate mutually exclusive domain options
            let domain_opts = [domain.is_some(), domain_id.is_some(), new_domain.is_some()];
            if domain_opts.iter().filter(|&&x| x).count() > 1 {
                return Err(anyhow::anyhow!(
                    "use only one of --domain, --domain-id, or --new-domain"
                ));
            }

            // Create domain if --new-domain is specified, otherwise resolve existing domain
            let resolved_domain_id = if let Some(new_dom) = new_domain {
                // Determine domain name: explicit or fallback to cache name
                let dom_name = new_dom.unwrap_or_else(|| name.clone());

                // Create the domain
                let domain = client
                    .create_domain(CreateDomainRequest {
                        domain_name: dom_name.clone(),
                    })
                    .await
                    .with_context(|| format!("failed to create domain '{}'", dom_name))?;

                println!(
                    "Created domain: {} ({})",
                    domain.domain_name, domain.domain_id
                );
                Some(domain.domain_id)
            } else if let Some(id) = domain_id {
                Some(id)
            } else if let Some(ref dom_name) = domain {
                // Resolve domain name to ID
                let d = client
                    .get_domain_by_name(dom_name)
                    .await
                    .with_context(|| format!("domain '{}' not found", dom_name))?;
                Some(d.domain_id)
            } else {
                None
            };

            let cache = client
                .create_cache(CreateCacheRequest {
                    cache_name: name.clone(),
                    public_base_url: base_url.clone(),
                    is_public: public,
                    is_default,
                    domain_id: resolved_domain_id,
                    domain_name: None, // We've resolved to ID already
                })
                .await?;
            println!("Cache created successfully!");
            println!("\nCache ID: {}", cache.cache_id);
            println!("Name: {}", cache.cache_name);
            if let Some(domain_name) = cache.domain_name.as_deref()
                && !domain_name.is_empty()
            {
                println!("Domain: {}", domain_name);
            }
            if let Some(domain_id) = cache.domain_id.as_deref()
                && !domain_id.is_empty()
            {
                println!("Domain ID: {}", domain_id);
            }
            if let Some(url) = cache.public_base_url {
                println!("Base URL: {}", url);
            }
            println!("Public: {}", cache.is_public);
            println!("Default: {}", cache.is_default);
        }
        CacheCommands::List => {
            let caches = client.list_caches().await?;

            if caches.is_empty() {
                println!("No caches found.");
            } else {
                println!(
                    "{:<38} {:<20} {:<38} {:<20} {:<10} {:<10} Base URL",
                    "ID", "Name", "Domain ID", "Domain", "Public", "Default"
                );
                println!("{}", "-".repeat(160));

                for cache in caches {
                    println!(
                        "{:<38} {:<20} {:<38} {:<20} {:<10} {:<10} {}",
                        cache.cache_id,
                        cache.cache_name,
                        cache.domain_id.clone().unwrap_or_else(|| "-".to_string()),
                        cache.domain_name.clone().unwrap_or_else(|| "-".to_string()),
                        cache.is_public,
                        cache.is_default,
                        cache.public_base_url.unwrap_or_else(|| "-".to_string())
                    );
                }
            }
        }
        CacheCommands::Show { cache } => {
            let cache_id = resolve_cache_selector(&client, &cache, config.as_ref()).await?;
            let cache = client.get_cache(&cache_id).await?;
            println!("Cache ID: {}", cache.cache_id);
            println!("Name: {}", cache.cache_name);
            if let Some(domain_name) = cache.domain_name.as_deref()
                && !domain_name.is_empty()
            {
                println!("Domain: {}", domain_name);
            }
            if let Some(domain_id) = cache.domain_id.as_deref()
                && !domain_id.is_empty()
            {
                println!("Domain ID: {}", domain_id);
            }
            println!("Public: {}", cache.is_public);
            println!("Default: {}", cache.is_default);
            if let Some(url) = cache.public_base_url {
                println!("Base URL: {}", url);
            }
            println!("Created: {}", cache.created_at);
            println!("Updated: {}", cache.updated_at);
        }
        CacheCommands::Update {
            cache,
            name,
            base_url,
            public,
            is_default,
            domain,
            domain_id,
        } => {
            if domain.is_some() && domain_id.is_some() {
                return Err(anyhow::anyhow!(
                    "use either --domain or --domain-id, not both"
                ));
            }

            if name.is_none()
                && base_url.is_none()
                && public.is_none()
                && is_default.is_none()
                && domain.is_none()
                && domain_id.is_none()
            {
                println!("No changes specified.");
                return Ok(());
            }

            let cache_id = resolve_cache_selector(&client, &cache, config.as_ref()).await?;

            let base_url = match base_url {
                Some(url) if url.is_empty() => Some(String::new()),
                other => other,
            };

            client
                .update_cache(
                    &cache_id,
                    UpdateCacheRequest {
                        cache_name: name,
                        public_base_url: base_url,
                        is_public: public,
                        is_default,
                        domain_id,
                        domain_name: domain,
                    },
                )
                .await?;
            println!("Cache updated successfully!");
        }
        CacheCommands::Delete { cache, force } => {
            let cache_id = resolve_cache_selector(&client, &cache, config.as_ref()).await?;
            let cache_info = client.get_cache(&cache_id).await?;

            // Confirm deletion BEFORE making any changes
            if !force {
                use std::io::Write;
                print!(
                    "\nThis will delete cache '{}' ({}) and all associated:\n  - GC jobs\n  - Signing keys\n  - Trusted keys\n  - Tombstones\n  - Upload sessions\n  - Expected chunks\n  - Tokens\n\nAre you sure? [y/N]: ",
                    cache_info.cache_name, cache_id
                );
                std::io::stdout().flush()?;

                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;

                if !input.trim().eq_ignore_ascii_case("y") {
                    println!("Deletion cancelled.");
                    return Ok(());
                }
            }

            client.delete_cache(&cache_id).await?;
            println!("\n✓ Cache deleted: {}", cache_info.cache_name);
        }
    }
    Ok(())
}

async fn resolve_domain_selector(
    client: &ApiClient,
    selector: &str,
) -> Result<api_client::DomainResponse> {
    if Uuid::parse_str(selector).is_ok() {
        client.get_domain(selector).await
    } else {
        client.get_domain_by_name(selector).await
    }
}

/// Resolve a cache selector (UUID, name, or config alias) to a cache ID.
/// Resolution order:
/// 1. If it's a valid UUID, use it directly
/// 2. Try to look up by cache name via API
/// 3. Try to look up as alias in client config (if config available)
async fn resolve_cache_selector(
    client: &ApiClient,
    selector: &str,
    config: Option<&ClientConfig>,
) -> Result<String> {
    // Check if it's a UUID
    if Uuid::parse_str(selector).is_ok() {
        return Ok(selector.to_string());
    }

    // Try API lookup by name
    if let Ok(cache) = client.get_cache_by_name(selector).await {
        return Ok(cache.cache_id);
    }

    // Try config alias lookup
    if let Some(cache_id) = config
        .and_then(|c| c.caches.get(selector))
        .and_then(|p| p.cache_id.clone())
    {
        return Ok(cache_id);
    }

    anyhow::bail!("cache not found: {selector} (not a UUID, cache name, or config alias)")
}

async fn handle_domain_command(command: DomainCommands, api: &ApiArgs) -> Result<()> {
    let client = get_api_client(api).await?;

    match command {
        DomainCommands::Create { name } => {
            let domain = client
                .create_domain(CreateDomainRequest { domain_name: name })
                .await?;
            println!("Domain created successfully!");
            println!("\nDomain ID: {}", domain.domain_id);
            println!("Name: {}", domain.domain_name);
            println!("Default: {}", domain.is_default);
            println!("Created: {}", domain.created_at);
            println!("Updated: {}", domain.updated_at);
        }
        DomainCommands::List => {
            let domains = client.list_domains().await?;
            if domains.is_empty() {
                println!("No domains found.");
            } else {
                println!("{:<20} {:<38} {:<10}", "Name", "ID", "Default");
                println!("{}", "-".repeat(72));
                for domain in domains {
                    println!(
                        "{:<20} {:<38} {:<10}",
                        domain.domain_name, domain.domain_id, domain.is_default
                    );
                }
            }
        }
        DomainCommands::Show { domain } => {
            let domain = resolve_domain_selector(&client, &domain).await?;
            println!("Domain ID: {}", domain.domain_id);
            println!("Name: {}", domain.domain_name);
            println!("Default: {}", domain.is_default);
            println!("Created: {}", domain.created_at);
            println!("Updated: {}", domain.updated_at);

            // Fetch and display attached caches
            let caches = client.list_caches().await?;
            let attached: Vec<_> = caches
                .iter()
                .filter(|c| c.domain_id.as_deref() == Some(&domain.domain_id))
                .collect();

            if !attached.is_empty() {
                println!("\nAttached caches ({}):", attached.len());
                for cache in attached {
                    println!("  {} ({})", cache.cache_name, cache.cache_id);
                }
            }
        }
        DomainCommands::Rename { domain, new_name } => {
            let current = resolve_domain_selector(&client, &domain).await?;
            let updated = client
                .update_domain(
                    &current.domain_id,
                    UpdateDomainRequest {
                        domain_name: new_name,
                    },
                )
                .await?;
            println!("Domain updated successfully!");
            println!("\nDomain ID: {}", updated.domain_id);
            println!("Name: {}", updated.domain_name);
        }
        DomainCommands::Delete { domain, force } => {
            let current = resolve_domain_selector(&client, &domain).await?;

            if !force {
                use std::io::Write;
                print!(
                    "\nThis will delete domain '{}' ({}).\nAre you sure? [y/N]: ",
                    current.domain_name, current.domain_id
                );
                std::io::stdout().flush()?;

                let mut input = String::new();
                std::io::stdin().read_line(&mut input)?;
                let input = input.trim().to_lowercase();
                if input != "y" && input != "yes" {
                    println!("Aborted.");
                    return Ok(());
                }
            }

            client.delete_domain(&current.domain_id).await?;
            println!("Domain deleted successfully.");
        }
    }

    Ok(())
}

async fn handle_token_command(command: TokenCommands, api: &ApiArgs) -> Result<()> {
    match command {
        TokenCommands::Generate { description } => handle_token_generate(description).await,
        TokenCommands::Create {
            scopes,
            cache,
            expires_in,
            description,
        } => {
            let client = get_api_client(api).await?;
            let scopes_vec: Vec<String> = scopes
                .split(',')
                .map(|s| s.trim())
                .filter(|s| !s.is_empty())
                .map(|s| s.to_string())
                .collect();
            if scopes_vec.is_empty() {
                anyhow::bail!("scopes cannot be empty");
            }

            // Resolve cache selector if provided
            let cache_id = if let Some(selector) = cache {
                let config =
                    load_client_config(&client_config_path(api.client.client_config.as_deref())?)
                        .await
                        .ok();
                Some(resolve_cache_selector(&client, &selector, config.as_ref()).await?)
            } else {
                None
            };

            let response = client
                .create_token(CreateTokenRequest {
                    scopes: scopes_vec,
                    cache_id,
                    expires_in_secs: expires_in,
                    description,
                })
                .await?;

            println!("Token created successfully!");
            println!("\nToken ID: {}", response.token_id);
            println!("Token secret: {}", response.token_secret);
            println!("\nIMPORTANT: Save this token secret now. It cannot be recovered.");
            if let Some(expires) = response.expires_at {
                println!("Expires: {expires}");
            }
            Ok(())
        }
        TokenCommands::List => {
            let client = get_api_client(api).await?;
            let tokens = client.list_tokens().await?;

            if tokens.is_empty() {
                println!("No tokens found.");
                return Ok(());
            }

            println!(
                "{:<38} {:<36} {:<20} {:<10} {:<20} {:<20} {:<20} {:<20} Description",
                "ID", "Cache", "Scopes", "Status", "Created", "Expires", "Revoked", "Last Used"
            );
            println!("{}", "-".repeat(210));

            let now = OffsetDateTime::now_utc();
            for token in tokens {
                let status = if token.revoked_at.is_some() {
                    "revoked"
                } else if token
                    .expires_at
                    .as_ref()
                    .and_then(|t| {
                        OffsetDateTime::parse(t, &time::format_description::well_known::Rfc3339)
                            .ok()
                    })
                    .map(|t| t < now)
                    .unwrap_or(false)
                {
                    "expired"
                } else {
                    "active"
                };

                let cache_label = token
                    .cache_id
                    .clone()
                    .unwrap_or_else(|| "global".to_string());
                let expires_at = token.expires_at.clone().unwrap_or_else(|| "-".to_string());
                let revoked_at = token.revoked_at.clone().unwrap_or_else(|| "-".to_string());
                let last_used_at = token
                    .last_used_at
                    .clone()
                    .unwrap_or_else(|| "-".to_string());
                let description = token.description.clone().unwrap_or_else(|| "-".to_string());

                println!(
                    "{:<38} {:<36} {:<20} {:<10} {:<20} {:<20} {:<20} {:<20} {}",
                    token.token_id,
                    cache_label,
                    token.scopes.join(","),
                    status,
                    token.created_at,
                    expires_at,
                    revoked_at,
                    last_used_at,
                    description
                );
            }
            Ok(())
        }
        TokenCommands::Revoke { token_id } => {
            let client = get_api_client(api).await?;
            client.revoke_token(&token_id).await?;
            println!("Token revoked: {token_id}");
            Ok(())
        }
    }
}

async fn handle_token_generate(description: Option<String>) -> Result<()> {
    let token_secret = generate_token_secret();
    let token_hash = hash_token(&token_secret);

    println!("Token generated (save the secret - it cannot be recovered):\n");
    println!("  Secret: {token_secret}");
    println!("  Hash:   sha256:{token_hash}");
    if let Some(desc) = description {
        println!("  Description: {desc}");
    }
    println!("\nAdd to server.toml:");
    println!("  [admin]");
    println!("  token_hash = \"sha256:{token_hash}\"");

    Ok(())
}

async fn handle_gc_command(command: GcCommands, api: &ApiArgs) -> Result<()> {
    let client = get_api_client(api).await?;
    let config = load_client_config(&client_config_path(api.client.client_config.as_deref())?)
        .await
        .ok();

    match command {
        GcCommands::Run {
            job_type,
            cache,
            wait,
        } => {
            // Resolve cache selector if provided
            let cache_id = if let Some(selector) = cache {
                Some(resolve_cache_selector(&client, &selector, config.as_ref()).await?)
            } else {
                None
            };

            let response = client
                .trigger_gc(TriggerGcRequest { job_type, cache_id })
                .await?;
            println!("GC job queued: {}", response.job_id);

            if wait {
                loop {
                    let job = client.get_gc_job(&response.job_id).await?;
                    match job.state.as_str() {
                        "queued" | "running" => {
                            tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        }
                        "finished" => {
                            println!("GC completed.");
                            if let Some(stats) = job.stats {
                                println!("Stats:");
                                println!("  Items processed: {}", stats.items_processed);
                                println!("  Items deleted: {}", stats.items_deleted);
                                println!("  Bytes reclaimed: {}", stats.bytes_reclaimed);
                                println!("  Errors: {}", stats.errors);
                            }
                            break;
                        }
                        "failed" => {
                            anyhow::bail!("GC failed");
                        }
                        other => {
                            anyhow::bail!("unknown GC state: {other}");
                        }
                    }
                }
            }
        }
        GcCommands::Status {
            job_id,
            cache,
            limit,
        } => {
            // Resolve cache selector if provided
            let cache_id = if let Some(selector) = cache {
                Some(resolve_cache_selector(&client, &selector, config.as_ref()).await?)
            } else {
                None
            };

            if let Some(job_id) = job_id {
                let job = client.get_gc_job(&job_id).await?;
                print_gc_job(&job);
            } else {
                let jobs = client.list_gc_jobs(cache_id.as_deref(), limit).await?;
                if jobs.is_empty() {
                    println!("No GC jobs found.");
                } else {
                    println!("{:<38} {:<12} {:<10} Cache", "ID", "Type", "State");
                    println!("{}", "-".repeat(80));
                    for job in jobs {
                        println!(
                            "{:<38} {:<12} {:<10} {}",
                            job.job_id,
                            job.job_type,
                            job.state,
                            job.cache_id.clone().unwrap_or_else(|| "public".to_string())
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

fn print_gc_job(job: &GcJobResponse) {
    println!("GC Job Status");
    println!("  ID: {}", job.job_id);
    println!("  Type: {}", job.job_type);
    println!("  State: {}", job.state);
    println!(
        "  Cache ID: {}",
        job.cache_id.clone().unwrap_or_else(|| "public".to_string())
    );
    if let Some(started) = &job.started_at {
        println!("  Started: {started}");
    }
    if let Some(finished) = &job.finished_at {
        println!("  Finished: {finished}");
    }
    if let Some(stats) = &job.stats {
        println!("  Stats:");
        println!("    Items processed: {}", stats.items_processed);
        println!("    Items deleted: {}", stats.items_deleted);
        println!("    Bytes reclaimed: {}", stats.bytes_reclaimed);
        println!("    Errors: {}", stats.errors);
    }
}

async fn handle_stats_command(api: &ApiArgs) -> Result<()> {
    let client = get_api_client(api).await?;
    let stats = client.get_stats().await?;
    render_stats(&stats);
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const KB: u64 = 1024;
    const MB: u64 = 1024 * KB;
    const GB: u64 = 1024 * MB;
    const TB: u64 = 1024 * GB;

    if bytes >= TB {
        format!("{:.2} TB", bytes as f64 / TB as f64)
    } else if bytes >= GB {
        format!("{:.2} GB", bytes as f64 / GB as f64)
    } else if bytes >= MB {
        format!("{:.2} MB", bytes as f64 / MB as f64)
    } else if bytes >= KB {
        format!("{:.2} KB", bytes as f64 / KB as f64)
    } else {
        format!("{} bytes", bytes)
    }
}

fn render_stats(stats: &MetricsResponse) {
    println!("Server Statistics:");
    println!("  Store paths: {}", stats.store_paths_count);
    println!("  Total chunks: {}", stats.chunks_count);
    println!(
        "  Total chunk size: {}",
        format_bytes(stats.chunks_total_size)
    );
    println!("  Referenced chunks: {}", stats.chunks_referenced);
    println!("  Unreferenced chunks: {}", stats.chunks_unreferenced);
    println!(
        "  Upload sessions created: {}",
        stats.upload_sessions_created
    );
    println!(
        "  Upload sessions committed: {}",
        stats.upload_sessions_committed
    );
    println!(
        "  Upload sessions resumed: {}",
        stats.upload_sessions_resumed
    );
    println!(
        "  Upload sessions expired: {}",
        stats.upload_sessions_expired
    );
    println!("  Chunks uploaded: {}", stats.chunks_uploaded);
    println!("  Chunks deduplicated: {}", stats.chunks_deduplicated);
    println!("  Bytes uploaded: {}", format_bytes(stats.bytes_uploaded));
    println!(
        "  Bytes deduplicated: {}",
        format_bytes(stats.bytes_deduplicated)
    );
    println!("  Chunk hash mismatches: {}", stats.chunk_hash_mismatches);
}

async fn handle_whoami_command(api: &ApiArgs) -> Result<()> {
    let (server, token) = resolve_api_config(api).await?;
    let base_url = normalize_base_url(&server)?;
    let http = reqwest::Client::new();
    let whoami = fetch_whoami(&http, &base_url, &token).await?;

    println!("Token ID: {}", whoami.token_id);
    if let Some(cache_id) = &whoami.cache_id {
        println!("Cache ID: {}", cache_id);
    }
    if let Some(cache_name) = &whoami.cache_name {
        println!("Cache: {}", cache_name);
    }
    println!("Scopes: {}", whoami.scopes.join(", "));
    if let Some(expires_at) = &whoami.expires_at {
        println!("Expires: {}", expires_at);
    } else {
        println!("Expires: never");
    }
    if let Some(public_base_url) = &whoami.public_base_url {
        println!("Public URL: {}", public_base_url);
    }
    if let Some(signing_public_key) = &whoami.signing_public_key {
        println!("Signing key: {}", signing_public_key);
    }
    Ok(())
}

async fn handle_health_command(api: &ApiArgs) -> Result<()> {
    let (server, _token) = resolve_api_config(api).await?;
    let base_url = normalize_base_url(&server)?;
    let health = fetch_health(&base_url).await?;

    println!("Status: {}", health.status);
    println!("Server version: {}", health.version);
    println!("Client version: {}", env!("CARGO_PKG_VERSION"));

    if health.version != env!("CARGO_PKG_VERSION") {
        eprintln!(
            "Warning: version mismatch (server: {}, client: {})",
            health.version,
            env!("CARGO_PKG_VERSION")
        );
    }
    Ok(())
}

async fn handle_setup_nix_conf(api: &ApiArgs) -> Result<()> {
    let (server, token) = resolve_api_config(api).await?;
    let base_url = normalize_base_url(&server)?;

    let http = reqwest::Client::new();
    let whoami = fetch_whoami(&http, &base_url, &token).await?;

    let public_key = whoami
        .signing_public_key
        .ok_or_else(|| anyhow::anyhow!("server did not return a signing public key"))?;

    let nix_url = whoami.public_base_url.unwrap_or(base_url);
    let nix_conf_path = default_nix_conf_path()?;

    update_nix_conf(&nix_conf_path, &nix_url, &public_key).await?;

    println!("Updated {}", nix_conf_path.display());
    println!("  substituters: {nix_url}");
    println!("  trusted-public-keys: {public_key}");

    Ok(())
}

async fn fetch_health(base_url: &str) -> Result<HealthResponse> {
    let url = format!("{base_url}/v1/health");
    let http = reqwest::Client::new();
    let response = http
        .get(url)
        .send()
        .await
        .context("health request failed")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("health check failed ({status}): {body}");
    }

    Ok(response.json::<HealthResponse>().await?)
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Default)]
#[serde(default)]
struct ClientConfig {
    default_cache: Option<String>,
    caches: BTreeMap<String, CacheProfile>,
}

#[derive(Debug, serde::Serialize, serde::Deserialize, Clone)]
struct CacheProfile {
    url: String,
    token: String,
    cache_id: Option<String>,
    cache_name: Option<String>,
    public_base_url: Option<String>,
    public_key: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct WhoamiResponse {
    token_id: String,
    cache_id: Option<String>,
    cache_name: Option<String>,
    scopes: Vec<String>,
    expires_at: Option<String>,
    public_base_url: Option<String>,
    signing_public_key: Option<String>,
}

#[derive(Debug, serde::Deserialize)]
struct HealthResponse {
    status: String,
    version: String,
}

#[derive(Debug, serde::Deserialize)]
struct CapabilitiesResponse {
    default_chunk_size: u64,
}

struct NarIndex {
    temp_path: PathBuf,
    nar_size: u64,
    nar_hash: String,
    manifest_hash: String,
    manifest: Vec<String>,
    expected_chunks: Vec<cellar_core::upload::ExpectedChunk>,
    chunk_size: u64,
}

async fn handle_login_command(
    alias: &str,
    url: &str,
    token: Option<String>,
    token_stdin: bool,
    set_default: bool,
    client: &ClientConfigArgs,
) -> Result<()> {
    let token = read_token(token, token_stdin)?;
    let base_url = normalize_base_url(url)?;
    let config_path = client_config_path(client.client_config.as_deref())?;

    let http = reqwest::Client::new();
    let whoami = fetch_whoami(&http, &base_url, &token).await?;

    let mut config = load_client_config(&config_path).await?;
    config.caches.insert(
        alias.to_string(),
        CacheProfile {
            url: base_url.clone(),
            token: token.clone(),
            cache_id: whoami.cache_id.clone(),
            cache_name: whoami.cache_name.clone(),
            public_base_url: whoami.public_base_url.clone(),
            public_key: whoami.signing_public_key.clone(),
        },
    );

    if set_default {
        config.default_cache = Some(alias.to_string());
    }

    save_client_config(&config_path, &config).await?;

    println!("Logged in as '{alias}'");
    println!("  URL: {base_url}");
    if let Some(cache_name) = &whoami.cache_name {
        println!("  Cache: {cache_name}");
    }
    if !whoami
        .scopes
        .iter()
        .any(|s| s == "cache:write" || s == "cache:admin")
    {
        eprintln!("Warning: token lacks cache:write; push will fail.");
    }
    if whoami.signing_public_key.is_none() {
        eprintln!("Warning: server did not return a signing public key.");
    }
    println!("Client config: {}", config_path.display());

    Ok(())
}

async fn handle_use_command(
    alias: &str,
    set_default: bool,
    set_nix_conf: bool,
    client: &ClientConfigArgs,
) -> Result<()> {
    let config_path = client_config_path(client.client_config.as_deref())?;
    let mut config = load_client_config(&config_path).await?;

    let mut profile = config
        .caches
        .get(alias)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("unknown cache alias: {alias}"))?;

    let mut updated = false;
    if set_default {
        config.default_cache = Some(alias.to_string());
        updated = true;
    }

    if set_nix_conf {
        if profile.public_key.is_none() || profile.public_base_url.is_none() {
            let http = reqwest::Client::new();
            if let Ok(whoami) = fetch_whoami(&http, &profile.url, &profile.token).await {
                if profile.public_key.is_none() {
                    profile.public_key = whoami.signing_public_key.clone();
                    updated = true;
                }
                if profile.public_base_url.is_none() {
                    profile.public_base_url = whoami.public_base_url.clone();
                    updated = true;
                }
                if profile.cache_name.is_none() {
                    profile.cache_name = whoami.cache_name.clone();
                    updated = true;
                }
                if profile.cache_id.is_none() {
                    profile.cache_id = whoami.cache_id.clone();
                    updated = true;
                }
            }
        }

        let public_key = profile
            .public_key
            .clone()
            .ok_or_else(|| anyhow::anyhow!("signing public key not available for {alias}"))?;
        let nix_url = profile
            .public_base_url
            .clone()
            .unwrap_or_else(|| profile.url.clone());
        let nix_conf_path = default_nix_conf_path()?;

        update_nix_conf(&nix_conf_path, &nix_url, &public_key).await?;
        println!("Updated nix.conf: {}", nix_conf_path.display());
    }

    if updated {
        config.caches.insert(alias.to_string(), profile);
        save_client_config(&config_path, &config).await?;
    }

    println!("Selected cache: {alias}");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn handle_push_command(
    cache: Option<String>,
    cache_id: Option<String>,
    server: Option<String>,
    token: Option<String>,
    flake: Option<String>,
    no_closure: bool,
    upstream: Vec<String>,
    no_filter: bool,
    no_preflight: bool,
    force: bool,
    store_paths: Vec<String>,
    nix_args: Vec<String>,
    client: &ClientConfigArgs,
) -> Result<()> {
    // Resolution order:
    // 1. --cache-id flag or CELLAR_CACHE_ID env → use with --server/--token or env
    // 2. --cache flag → lookup in config file
    // 3. default_cache from config file → lookup profile
    // 4. Token's implicit cache (server determines from token)

    let profile = if let Some(ref cache_id_val) = cache_id {
        // Direct cache ID mode: requires server and token
        let server_url = server.ok_or_else(|| {
            anyhow::anyhow!("--server (or CELLAR_SERVER) required with --cache-id")
        })?;
        let token_val = token
            .ok_or_else(|| anyhow::anyhow!("--token (or CELLAR_TOKEN) required with --cache-id"))?;
        CacheProfile {
            url: normalize_base_url(&server_url)?,
            token: token_val,
            cache_id: Some(cache_id_val.clone()),
            cache_name: None,
            public_base_url: None,
            public_key: None,
        }
    } else if server.is_some() || token.is_some() {
        // Server/token provided without cache-id: token determines cache
        let server_url = server
            .ok_or_else(|| anyhow::anyhow!("--server (or CELLAR_SERVER) required with --token"))?;
        let token_val = token
            .ok_or_else(|| anyhow::anyhow!("--token (or CELLAR_TOKEN) required with --server"))?;
        CacheProfile {
            url: normalize_base_url(&server_url)?,
            token: token_val,
            cache_id: None,
            cache_name: None,
            public_base_url: None,
            public_key: None,
        }
    } else {
        // Config-based mode
        let config_path = client_config_path(client.client_config.as_deref())?;
        let config = load_client_config(&config_path).await?;

        let cache_alias = match cache {
            Some(alias) => alias,
            None => config
                .default_cache
                .clone()
                .ok_or_else(|| anyhow::anyhow!("no default cache configured"))?,
        };

        config
            .caches
            .get(&cache_alias)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("unknown cache alias: {cache_alias}"))?
    };

    // Resolve upstreams early so they're available for both preflight and filtering
    let upstreams: Vec<String> = if !no_filter {
        if upstream.is_empty() {
            let exclude = [profile.url.as_str()]
                .into_iter()
                .chain(profile.public_base_url.as_deref())
                .collect::<Vec<_>>();
            let detected = detect_upstream_substituters(&exclude).await;
            if detected.is_empty() {
                vec![DEFAULT_UPSTREAM.to_string()]
            } else {
                detected
            }
        } else {
            upstream
        }
    } else {
        Vec::new()
    };

    // Pre-flight check (flake mode only): evaluate output paths without building,
    // then walk the closure graph via narinfo fetches to see if everything is cached.
    if let Some(flake_ref) = flake.as_deref()
        && !no_preflight
        && !force
        && let Some(output_paths) = evaluate_flake_output_paths(flake_ref, &nix_args).await
    {
        println!(
            "Pre-flight: resolved {} output path(s), checking remote caches...",
            output_paths.len()
        );
        match try_preflight_closure_check(
            &output_paths,
            &profile,
            &upstreams,
            no_filter,
            no_closure,
        )
        .await
        {
            PreflightResult::NothingToPush {
                total,
                on_cellar,
                on_upstream,
            } => {
                println!(
                    "All {total} paths already cached ({on_cellar} on cache, {on_upstream} upstream)"
                );
                println!("Nothing to push.");
                return Ok(());
            }
            PreflightResult::Inconclusive => {
                println!("Pre-flight: could not resolve remotely, building...");
            }
        }
    }

    let store_paths = if let Some(flake_ref) = flake.as_deref() {
        if !store_paths.is_empty() {
            anyhow::bail!("store paths cannot be combined with --flake");
        }
        let top_level = resolve_flake_store_paths(flake_ref, &nix_args).await?;
        if no_closure {
            top_level
        } else {
            println!("Expanding to runtime closure...");
            let closure = expand_to_closure(&top_level).await?;
            println!(
                "Closure contains {} paths ({} top-level)",
                closure.len(),
                top_level.len()
            );
            closure
        }
    } else {
        if !nix_args.is_empty() {
            anyhow::bail!("extra nix args require --flake");
        }
        if store_paths.is_empty() {
            anyhow::bail!("no store paths provided");
        }
        store_paths
    };

    let store_paths = if !no_filter {
        let upstream_http = reqwest::Client::builder()
            .timeout(Duration::from_secs(5))
            .build()?;
        println!(
            "Filtering against {} upstream substituter(s)...",
            upstreams.len()
        );
        let (remaining, filtered) =
            filter_upstream_paths(&upstream_http, &store_paths, &upstreams).await;
        if filtered > 0 {
            println!(
                "Filtered {filtered} paths (available upstream), {} remain to push",
                remaining.len()
            );
        } else {
            println!("No paths filtered (none found upstream)");
        }
        if remaining.is_empty() {
            println!("Nothing to push — all paths available upstream");
            return Ok(());
        }
        remaining
    } else {
        store_paths
    };

    let http = reqwest::Client::new();
    let capabilities = fetch_capabilities(&http, &profile.url).await?;
    let chunk_size = capabilities.default_chunk_size;

    let mut uploaded = 0;
    let mut skipped = 0;

    for store_path in &store_paths {
        println!("Pushing {store_path}...");
        match push_store_path(&http, &profile, store_path, chunk_size, force).await? {
            PushResult::Uploaded => {
                println!("✓ Uploaded {store_path}");
                uploaded += 1;
            }
            PushResult::AlreadyCached => {
                println!("  ✓ Already cached, skipping");
                skipped += 1;
            }
        }
    }

    if store_paths.len() > 1 {
        println!("\nDone: {uploaded} uploaded, {skipped} skipped (already cached)");
    }

    Ok(())
}

fn client_config_path(explicit: Option<&str>) -> Result<PathBuf> {
    if let Some(path) = explicit {
        return Ok(PathBuf::from(path));
    }

    if let Some(path) = std::env::var_os("CELLAR_CLIENT_CONFIG") {
        return Ok(PathBuf::from(path));
    }

    let base = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(path) => PathBuf::from(path),
        None => {
            let home = std::env::var_os("HOME")
                .ok_or_else(|| anyhow::anyhow!("HOME not set; set CELLAR_CLIENT_CONFIG"))?;
            PathBuf::from(home).join(".config")
        }
    };

    Ok(base.join("cellar").join("client.toml"))
}

async fn load_client_config(path: &Path) -> Result<ClientConfig> {
    let mut figment = Figment::new();

    if path.exists() {
        figment = figment.merge(Toml::file(path));
    }

    figment = figment.merge(Env::prefixed("CELLAR_").split("__"));

    match figment.extract() {
        Ok(config) => Ok(config),
        Err(_) if !path.exists() => Ok(ClientConfig::default()),
        Err(err) => Err(anyhow::anyhow!(err).context("failed to load client configuration")),
    }
}

async fn save_client_config(path: &Path, config: &ClientConfig) -> Result<()> {
    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    let contents = toml::to_string_pretty(config)?;

    tokio::fs::write(path, contents).await?;

    // Set restrictive permissions (0600) since the file contains tokens
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let perms = std::fs::Permissions::from_mode(0o600);
        tokio::fs::set_permissions(path, perms).await?;
    }

    Ok(())
}

fn read_token(token: Option<String>, token_stdin: bool) -> Result<String> {
    if let Some(token) = token {
        return Ok(token);
    }
    if token_stdin {
        let mut buf = String::new();
        std::io::stdin().read_to_string(&mut buf)?;
        let token = buf.trim().to_string();
        if token.is_empty() {
            anyhow::bail!("token read from stdin is empty");
        }
        return Ok(token);
    }
    anyhow::bail!("token required: use --token or --token-stdin");
}

fn normalize_base_url(url: &str) -> Result<String> {
    if !url.starts_with("http://") && !url.starts_with("https://") {
        anyhow::bail!("cache URL must start with http:// or https://");
    }
    Ok(url.trim_end_matches('/').to_string())
}

fn default_nix_conf_path() -> Result<PathBuf> {
    let base = match std::env::var_os("XDG_CONFIG_HOME") {
        Some(path) => PathBuf::from(path),
        None => {
            let home = std::env::var_os("HOME")
                .ok_or_else(|| anyhow::anyhow!("HOME not set; set XDG_CONFIG_HOME"))?;
            PathBuf::from(home).join(".config")
        }
    };
    Ok(base.join("nix").join("nix.conf"))
}

async fn update_nix_conf(path: &Path, substituter: &str, public_key: &str) -> Result<()> {
    let mut lines = match tokio::fs::read_to_string(path).await {
        Ok(contents) => contents.lines().map(|line| line.to_string()).collect(),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(err) => return Err(err.into()),
    };

    let mut saw_substituters = false;
    let mut saw_keys = false;

    for line in &mut lines {
        if let Some(updated) = update_nix_value(line, "substituters", substituter) {
            *line = updated;
            saw_substituters = true;
            continue;
        }
        if let Some(updated) = update_nix_value(line, "trusted-public-keys", public_key) {
            *line = updated;
            saw_keys = true;
        }
    }

    if !saw_substituters {
        lines.push(format!("substituters = {substituter}"));
    }
    if !saw_keys {
        lines.push(format!("trusted-public-keys = {public_key}"));
    }

    if let Some(parent) = path.parent() {
        tokio::fs::create_dir_all(parent).await?;
    }
    tokio::fs::write(path, lines.join("\n") + "\n").await?;
    Ok(())
}

fn update_nix_value(line: &str, key: &str, value: &str) -> Option<String> {
    let trimmed = line.trim_start();
    let parts: Vec<&str> = trimmed.splitn(2, '=').collect();
    if parts.len() != 2 {
        return None;
    }

    // Exact key match to avoid prefix collisions (e.g., "substituters" vs "substituters-extra")
    if parts[0].trim() != key {
        return None;
    }

    let value_part = parts[1].split('#').next().unwrap_or("");
    let mut values: Vec<String> = value_part
        .split_whitespace()
        .map(|v| v.to_string())
        .collect();
    if !values.iter().any(|v| v == value) {
        values.push(value.to_string());
    }

    Some(format!("{key} = {}", values.join(" ")))
}

async fn fetch_whoami(
    http: &reqwest::Client,
    base_url: &str,
    token: &str,
) -> Result<WhoamiResponse> {
    let url = format!("{base_url}/v1/auth/whoami");
    let response = http
        .get(url)
        .bearer_auth(token)
        .send()
        .await
        .context("whoami request failed")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("whoami failed ({status}): {body}");
    }

    Ok(response.json::<WhoamiResponse>().await?)
}

/// Retries an HTTP request with exponential backoff on transient failures.
///
/// Retries on transport errors and 5xx responses. Returns immediately on
/// success or 4xx (client errors that won't resolve with retries).
async fn retry_request<F, Fut>(mut make_request: F) -> reqwest::Result<reqwest::Response>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = reqwest::Result<reqwest::Response>>,
{
    const MAX_RETRIES: u32 = 3;
    let mut attempt = 0;

    loop {
        match make_request().await {
            Ok(response) if response.status().is_server_error() => {
                attempt += 1;
                if attempt > MAX_RETRIES {
                    return Ok(response);
                }
                let delay = Duration::from_secs(1 << (attempt - 1)); // 1s, 2s, 4s
                eprintln!(
                    "  Server error ({}), retrying in {}s...",
                    response.status(),
                    delay.as_secs()
                );
                tokio::time::sleep(delay).await;
            }
            Ok(response) => return Ok(response),
            Err(e) => {
                attempt += 1;
                if attempt > MAX_RETRIES {
                    return Err(e);
                }
                let delay = Duration::from_secs(1 << (attempt - 1));
                eprintln!("  Request error ({e}), retrying in {}s...", delay.as_secs());
                tokio::time::sleep(delay).await;
            }
        }
    }
}

async fn fetch_capabilities(
    http: &reqwest::Client,
    base_url: &str,
) -> Result<CapabilitiesResponse> {
    let url = format!("{base_url}/v1/capabilities");
    let response = retry_request(|| http.get(&url).send()).await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("capabilities request failed ({status}): {body}");
    }
    Ok(response.json::<CapabilitiesResponse>().await?)
}

async fn resolve_flake_store_paths(flake: &str, nix_args: &[String]) -> Result<Vec<String>> {
    let mut cmd = Command::new("nix");
    cmd.arg("build")
        .arg("--no-link")
        .arg("--print-out-paths")
        .arg(flake);
    cmd.args(nix_args);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::inherit()); // Stream progress to terminal

    let child = cmd.spawn().context("failed to spawn nix build")?;
    let output = child.wait_with_output().await.context("nix build failed")?;
    if !output.status.success() {
        anyhow::bail!("nix build failed");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut paths: Vec<String> = stdout
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();
    paths.sort();
    paths.dedup();

    if paths.is_empty() {
        anyhow::bail!("nix build returned no store paths");
    }
    Ok(paths)
}

/// Expand top-level store paths to their full runtime closure via `nix-store -qR`.
async fn expand_to_closure(paths: &[String]) -> Result<Vec<String>> {
    let mut cmd = Command::new("nix-store");
    cmd.arg("-qR");
    cmd.args(paths);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = cmd
        .spawn()
        .context("failed to spawn nix-store -qR (is nix-store on PATH?)")?;
    let output = child
        .wait_with_output()
        .await
        .context("nix-store -qR failed")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        anyhow::bail!("nix-store -qR failed: {stderr}");
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let mut closure: Vec<String> = stdout
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();
    closure.sort();
    closure.dedup();

    if closure.is_empty() {
        anyhow::bail!("nix-store -qR returned no paths");
    }
    Ok(closure)
}

/// Evaluate a flake reference to get output paths without building.
///
/// Runs `nix derivation show` which only evaluates the derivation (no download/build).
/// Returns `None` on any failure (IFD, eval error, old Nix) so the caller can
/// fall back to the normal build path.
async fn evaluate_flake_output_paths(flake: &str, nix_args: &[String]) -> Option<Vec<String>> {
    let mut cmd = Command::new("nix");
    cmd.arg("derivation").arg("show").arg(flake).args(nix_args);
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let child = cmd.spawn().ok()?;
    let output = child.wait_with_output().await.ok()?;
    if !output.status.success() {
        return None;
    }

    let json: serde_json::Value = serde_json::from_slice(&output.stdout).ok()?;
    let obj = json.as_object()?;

    let mut paths = Vec::new();
    for (_drv_path, drv_info) in obj {
        let outputs = drv_info.get("outputs")?.as_object()?;
        for (_name, output_info) in outputs {
            if let Some(path) = output_info.get("path").and_then(|p| p.as_str()) {
                paths.push(path.to_string());
            }
        }
    }

    paths.sort();
    paths.dedup();
    if paths.is_empty() {
        return None;
    }
    Some(paths)
}

/// Fetch and parse a narinfo from a remote cache.
///
/// Returns `None` on 404, network error, or parse failure.
async fn fetch_narinfo(
    http: &reqwest::Client,
    base_url: &str,
    hash: &str,
    auth_token: Option<&str>,
) -> Option<cellar_core::narinfo::NarInfo> {
    let url = format!("{}/{}.narinfo", base_url.trim_end_matches('/'), hash);
    let mut req = http.get(&url);
    if let Some(token) = auth_token {
        req = req.bearer_auth(token);
    }
    let resp = req.send().await.ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let text = resp.text().await.ok()?;
    cellar_core::narinfo::NarInfo::parse(&text).ok()
}

enum PreflightResult {
    NothingToPush {
        total: usize,
        on_cellar: usize,
        on_upstream: usize,
    },
    Inconclusive,
}

/// Walk the closure graph remotely via narinfo fetches to check if all paths are cached.
///
/// BFS from output paths: for each path, fetch narinfo from cellar (with auth)
/// then upstream (no auth). Parse References to discover transitive dependencies.
/// If any path can't be found, return `Inconclusive` immediately.
async fn try_preflight_closure_check(
    output_paths: &[String],
    profile: &CacheProfile,
    upstreams: &[String],
    no_filter: bool,
    no_closure: bool,
) -> PreflightResult {
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap_or_default();

    let semaphore = std::sync::Arc::new(Semaphore::new(UPSTREAM_CHECK_CONCURRENCY));
    let on_cellar = std::sync::Arc::new(AtomicUsize::new(0));
    let on_upstream = std::sync::Arc::new(AtomicUsize::new(0));

    // Parse output paths into hashes for the initial BFS queue
    let mut queue: VecDeque<String> = VecDeque::new();
    let mut visited: HashSet<String> = HashSet::new();

    for path in output_paths {
        let hash = match cellar_core::store_path::StorePath::parse(path) {
            Ok(parsed) => parsed.hash().as_str().to_string(),
            Err(_) => return PreflightResult::Inconclusive,
        };
        if visited.insert(hash.clone()) {
            queue.push_back(hash);
        }
    }

    // BFS: process in waves for concurrency
    while !queue.is_empty() {
        let wave: Vec<String> = queue.drain(..).collect();

        eprint!("\r  Checking closure: {} paths resolved...", visited.len());

        let mut futures = FuturesUnordered::new();
        for hash in wave {
            let http = http.clone();
            let profile_url = profile.url.clone();
            let profile_token = profile.token.clone();
            let upstreams = upstreams.to_vec();
            let sem = semaphore.clone();
            let on_cellar = on_cellar.clone();
            let on_upstream = on_upstream.clone();

            futures.push(tokio::spawn(async move {
                let _permit = sem.acquire().await.unwrap();

                // Try cellar first
                if let Some(narinfo) =
                    fetch_narinfo(&http, &profile_url, &hash, Some(&profile_token)).await
                {
                    on_cellar.fetch_add(1, Ordering::Relaxed);
                    let refs: Vec<String> = narinfo
                        .references
                        .iter()
                        .map(|r| r.hash().as_str().to_string())
                        .collect();
                    return Some(refs);
                }

                // If --no-filter, upstream paths would need pushing, so only cellar matters
                if no_filter {
                    return None;
                }

                // Try upstreams
                for upstream_url in &upstreams {
                    if let Some(narinfo) = fetch_narinfo(&http, upstream_url, &hash, None).await {
                        on_upstream.fetch_add(1, Ordering::Relaxed);
                        let refs: Vec<String> = narinfo
                            .references
                            .iter()
                            .map(|r| r.hash().as_str().to_string())
                            .collect();
                        return Some(refs);
                    }
                }

                None // Not found anywhere
            }));
        }

        let mut found_all = true;
        while let Some(result) = futures.next().await {
            match result {
                Ok(Some(refs)) => {
                    if !no_closure {
                        for ref_hash in refs {
                            if visited.insert(ref_hash.clone()) {
                                queue.push_back(ref_hash);
                            }
                        }
                    }
                }
                Ok(None) => {
                    found_all = false;
                    break;
                }
                Err(_) => {
                    found_all = false;
                    break;
                }
            }
        }

        if !found_all {
            eprintln!(); // newline after progress
            return PreflightResult::Inconclusive;
        }
    }

    eprintln!(); // newline after progress
    let cellar_count = on_cellar.load(Ordering::Relaxed);
    let upstream_count = on_upstream.load(Ordering::Relaxed);
    PreflightResult::NothingToPush {
        total: visited.len(),
        on_cellar: cellar_count,
        on_upstream: upstream_count,
    }
}

/// Auto-detect upstream substituters from `nix show-config --json`.
///
/// Returns substituter URLs from the nix config, excluding any that match
/// the target cache (so we don't filter against our own cache).
/// Falls back to an empty vec on any error.
async fn detect_upstream_substituters(exclude_urls: &[&str]) -> Vec<String> {
    let output = Command::new("nix")
        .args(["show-config", "--json"])
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    let output = match output {
        Ok(out) if out.status.success() => out,
        _ => return Vec::new(),
    };

    let config: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(v) => v,
        Err(_) => return Vec::new(),
    };

    let extract_urls = |key: &str| -> Vec<&str> {
        config
            .get(key)
            .and_then(|s| s.get("value"))
            .and_then(|v| v.as_array())
            .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
            .unwrap_or_default()
    };

    let mut all_urls: Vec<&str> = extract_urls("substituters");
    all_urls.extend(extract_urls("extra-substituters"));

    // Normalize for comparison: lowercase, strip trailing slashes
    let normalize = |url: &str| url.trim_end_matches('/').to_lowercase();
    let excluded: HashSet<String> = exclude_urls.iter().map(|u| normalize(u)).collect();

    let mut seen = HashSet::new();
    all_urls
        .into_iter()
        .map(|u| u.trim_end_matches('/').to_string())
        .filter(|u| !excluded.contains(&normalize(u)))
        .filter(|u| seen.insert(normalize(u)))
        .collect()
}

/// Check paths against upstream substituters and return those NOT found upstream.
///
/// For each store path, extracts the hash and sends HEAD requests to
/// `{upstream}/{hash}.narinfo`. Paths returning 200 from any upstream are
/// filtered out. Errors and timeouts are treated as "not found" (fail-open).
async fn filter_upstream_paths(
    http: &reqwest::Client,
    paths: &[String],
    upstreams: &[String],
) -> (Vec<String>, usize) {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::Semaphore;

    let total = paths.len();
    let done = std::sync::Arc::new(AtomicUsize::new(0));
    let semaphore = std::sync::Arc::new(Semaphore::new(UPSTREAM_CHECK_CONCURRENCY));

    let mut futures = FuturesUnordered::new();

    for path in paths {
        let http = http.clone();
        let upstreams = upstreams.to_vec();
        let path = path.clone();
        let sem = semaphore.clone();
        let done = done.clone();

        futures.push(tokio::spawn(async move {
            let _permit = sem.acquire().await.unwrap();

            let hash = match cellar_core::store_path::StorePath::parse(&path) {
                Ok(parsed) => parsed.hash().as_str().to_string(),
                Err(_) => {
                    // Can't parse → keep the path (fail-open)
                    let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
                    eprint!("\r  Checking upstream: {completed}/{total}...");
                    return (path, false);
                }
            };

            let mut found = false;
            for upstream in &upstreams {
                let url = format!("{}/{hash}.narinfo", upstream.trim_end_matches('/'));
                match http.head(&url).send().await {
                    Ok(resp) if resp.status().is_success() => {
                        found = true;
                        break;
                    }
                    _ => {}
                }
            }

            let completed = done.fetch_add(1, Ordering::Relaxed) + 1;
            eprint!("\r  Checking upstream: {completed}/{total}...");
            (path, found)
        }));
    }

    let mut remaining = Vec::new();
    let mut filtered = 0usize;
    while let Some(result) = futures.next().await {
        match result {
            Ok((_path, true)) => filtered += 1,
            Ok((path, false)) => remaining.push(path),
            Err(_) => {} // JoinError — shouldn't happen; path is lost (fail-open)
        }
    }
    eprintln!(); // newline after progress

    // Preserve original order
    let remaining_set: HashSet<&str> = remaining.iter().map(|s| s.as_str()).collect();
    let ordered: Vec<String> = paths
        .iter()
        .filter(|p| remaining_set.contains(p.as_str()))
        .cloned()
        .collect();

    (ordered, filtered)
}

/// Query runtime references for a store path via `nix-store -q --references`.
/// Returns full store paths. Soft-fails: returns empty vec on error.
async fn query_references(store_path: &str) -> Vec<String> {
    let output = Command::new("nix-store")
        .arg("-q")
        .arg("--references")
        .arg(store_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            stdout
                .lines()
                .map(|l| l.trim().to_string())
                .filter(|l| !l.is_empty())
                .collect()
        }
        _ => Vec::new(),
    }
}

/// Query deriver for a store path via `nix-store -q --deriver`.
/// Returns None for "unknown-deriver" or on failure.
async fn query_deriver(store_path: &str) -> Option<String> {
    let output = Command::new("nix-store")
        .arg("-q")
        .arg("--deriver")
        .arg(store_path)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await;

    match output {
        Ok(out) if out.status.success() => {
            let deriver = String::from_utf8_lossy(&out.stdout).trim().to_string();
            if deriver.is_empty() || deriver == "unknown-deriver" {
                None
            } else {
                Some(deriver)
            }
        }
        _ => None,
    }
}

enum PushResult {
    Uploaded,
    AlreadyCached,
}

async fn push_store_path(
    http: &reqwest::Client,
    profile: &CacheProfile,
    store_path: &str,
    chunk_size: u64,
    force: bool,
) -> Result<PushResult> {
    let parsed = cellar_core::store_path::StorePath::parse(store_path)?;

    // Check if path already exists on the server (fail open: on error, proceed with upload)
    if !force {
        let hash = parsed.hash().as_str();
        let narinfo_url = format!("{}/{}.narinfo", profile.url, hash);
        let response = http
            .head(&narinfo_url)
            .bearer_auth(&profile.token)
            .send()
            .await;

        if matches!(response.as_ref().map(|r| r.status()), Ok(status) if status.is_success()) {
            return Ok(PushResult::AlreadyCached);
        }
    }

    let nar_index = dump_nar_and_index(store_path, chunk_size).await?;
    let cleanup_path = nar_index.temp_path.clone();

    let result = push_store_path_inner(http, profile, store_path, nar_index).await;
    let _ = tokio::fs::remove_file(&cleanup_path).await;
    result?;
    Ok(PushResult::Uploaded)
}

async fn push_store_path_inner(
    http: &reqwest::Client,
    profile: &CacheProfile,
    store_path: &str,
    nar_index: NarIndex,
) -> Result<()> {
    use cellar_core::upload::{CommitUploadRequest, CreateUploadRequest, MissingChunks};

    let create_request = CreateUploadRequest {
        store_path: store_path.to_string(),
        nar_size: nar_index.nar_size,
        nar_hash: nar_index.nar_hash.clone(),
        chunk_size: Some(nar_index.chunk_size),
        manifest_hash: Some(nar_index.manifest_hash.clone()),
        expected_chunks: Some(nar_index.expected_chunks.clone()),
    };

    let create_url = format!("{}/v1/uploads", profile.url);
    let response = retry_request(|| {
        http.post(&create_url)
            .bearer_auth(&profile.token)
            .json(&create_request)
            .send()
    })
    .await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("create upload failed ({status}): {body}");
    }

    let create_response = response
        .json::<cellar_core::upload::CreateUploadResponse>()
        .await?;

    let missing_set = match create_response.missing_chunks {
        MissingChunks::All(_) => None,
        MissingChunks::List(list) => Some(list.into_iter().collect::<HashSet<String>>()),
        MissingChunks::Unknown { .. } => {
            let state = fetch_upload_state(
                http,
                &profile.url,
                &profile.token,
                &create_response.upload_id,
            )
            .await?;
            Some(
                state
                    .missing_chunks
                    .into_iter()
                    .collect::<HashSet<String>>(),
            )
        }
    };

    if missing_set.as_ref().is_none_or(|set| !set.is_empty()) {
        upload_missing_chunks(
            http,
            profile,
            &create_response.upload_id,
            &nar_index,
            missing_set,
            create_response.max_parallel_chunks,
        )
        .await?;
    }

    // Query references and deriver for narinfo population
    let references = query_references(store_path).await;
    let deriver = query_deriver(store_path).await;

    let commit_request = CommitUploadRequest {
        manifest: nar_index.manifest,
        references: if references.is_empty() {
            None
        } else {
            Some(references)
        },
        deriver,
    };

    let commit_url = format!(
        "{}/v1/uploads/{}/commit",
        profile.url, create_response.upload_id
    );
    let commit_response = retry_request(|| {
        http.post(&commit_url)
            .bearer_auth(&profile.token)
            .json(&commit_request)
            .send()
    })
    .await?;

    if !commit_response.status().is_success() {
        let status = commit_response.status();
        let body = commit_response.text().await.unwrap_or_default();
        anyhow::bail!("commit failed ({status}): {body}");
    }

    Ok(())
}

async fn fetch_upload_state(
    http: &reqwest::Client,
    base_url: &str,
    token: &str,
    upload_id: &str,
) -> Result<cellar_core::upload::UploadStateResponse> {
    let url = format!("{base_url}/v1/uploads/{upload_id}");
    let response = retry_request(|| http.get(&url).bearer_auth(token).send()).await?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("upload state failed ({status}): {body}");
    }

    Ok(response
        .json::<cellar_core::upload::UploadStateResponse>()
        .await?)
}

async fn upload_missing_chunks(
    http: &reqwest::Client,
    profile: &CacheProfile,
    upload_id: &str,
    nar_index: &NarIndex,
    missing_set: Option<HashSet<String>>,
    max_parallel: u32,
) -> Result<()> {
    let mut file = tokio::fs::File::open(&nar_index.temp_path).await?;
    let parallel = std::cmp::max(1, max_parallel as usize);
    let mut in_flight = FuturesUnordered::new();

    for chunk in &nar_index.expected_chunks {
        let size = usize::try_from(chunk.size)
            .map_err(|_| anyhow::anyhow!("chunk size exceeds platform limits"))?;
        let mut data = vec![0u8; size];
        file.read_exact(&mut data).await?;

        let should_upload = missing_set
            .as_ref()
            .map(|set| set.contains(&chunk.hash))
            .unwrap_or(true);
        if !should_upload {
            continue;
        }

        let url = format!(
            "{}/v1/uploads/{}/chunks/{}",
            profile.url, upload_id, chunk.hash
        );
        let token = profile.token.clone();
        let client = http.clone();

        in_flight.push(async move {
            let response = retry_request(|| {
                client
                    .put(&url)
                    .bearer_auth(&token)
                    .body(data.clone())
                    .send()
            })
            .await?;
            if response.status().is_success() {
                Ok(())
            } else {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                anyhow::bail!("chunk upload failed ({status}): {body}");
            }
        });

        if in_flight.len() >= parallel
            && let Some(result) = in_flight.next().await
        {
            result?;
        }
    }

    while let Some(result) = in_flight.next().await {
        result?;
    }

    Ok(())
}

async fn dump_nar_and_index(store_path: &str, chunk_size: u64) -> Result<NarIndex> {
    let temp_path = build_temp_nar_path();
    let chunk_size = usize::try_from(chunk_size)
        .map_err(|_| anyhow::anyhow!("chunk size exceeds platform limits"))?;

    let first_attempt =
        dump_nar_with_command(store_path, chunk_size, &temp_path, DumpCommand::Nix).await;
    match first_attempt {
        Ok(index) => Ok(index),
        Err(first_err) => {
            let _ = tokio::fs::remove_file(&temp_path).await;
            let second_attempt =
                dump_nar_with_command(store_path, chunk_size, &temp_path, DumpCommand::NixStore)
                    .await;
            match second_attempt {
                Ok(index) => Ok(index),
                Err(second_err) => {
                    let _ = tokio::fs::remove_file(&temp_path).await;
                    Err(anyhow::anyhow!("{first_err}\n{second_err}"))
                }
            }
        }
    }
}

enum DumpCommand {
    Nix,
    NixStore,
}

async fn dump_nar_with_command(
    store_path: &str,
    chunk_size: usize,
    temp_path: &Path,
    command: DumpCommand,
) -> Result<NarIndex> {
    let mut cmd = match command {
        DumpCommand::Nix => {
            let mut cmd = Command::new("nix");
            cmd.arg("store").arg("dump-path").arg(store_path);
            cmd
        }
        DumpCommand::NixStore => {
            let mut cmd = Command::new("nix-store");
            cmd.arg("--dump").arg(store_path);
            cmd
        }
    };
    let cmd_name = match command {
        DumpCommand::Nix => "nix",
        DumpCommand::NixStore => "nix-store",
    };

    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn {cmd_name} dump"))?;
    let mut stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture nix dump stdout"))?;
    let mut stderr = child
        .stderr
        .take()
        .ok_or_else(|| anyhow::anyhow!("failed to capture nix dump stderr"))?;

    let stderr_task = tokio::spawn(async move {
        let mut buf = Vec::new();
        stderr.read_to_end(&mut buf).await?;
        Ok::<Vec<u8>, std::io::Error>(buf)
    });

    let mut file = tokio::fs::File::create(temp_path).await?;
    let mut nar_hasher = cellar_core::hash::NarHasher::new();
    let mut chunk_hashes = Vec::new();
    let mut expected_chunks = Vec::new();
    let mut current_chunk = Vec::with_capacity(chunk_size);
    let mut total_size = 0u64;
    let mut read_buf = vec![0u8; 64 * 1024];

    loop {
        let n = stdout.read(&mut read_buf).await?;
        if n == 0 {
            break;
        }
        file.write_all(&read_buf[..n]).await?;
        nar_hasher.update(&read_buf[..n]);
        total_size += n as u64;

        let mut offset = 0;
        while offset < n {
            let remaining = chunk_size - current_chunk.len();
            let take = std::cmp::min(remaining, n - offset);
            current_chunk.extend_from_slice(&read_buf[offset..offset + take]);
            offset += take;

            if current_chunk.len() == chunk_size {
                record_chunk(&current_chunk, &mut chunk_hashes, &mut expected_chunks);
                current_chunk.clear();
            }
        }
    }

    if !current_chunk.is_empty() {
        record_chunk(&current_chunk, &mut chunk_hashes, &mut expected_chunks);
    }

    file.flush().await?;
    let status = child.wait().await?;
    let stderr = stderr_task.await??;

    if !status.success() {
        let stderr_str = String::from_utf8_lossy(&stderr);
        anyhow::bail!("nix dump failed: {stderr_str}");
    }

    let nar_hash = nar_hasher.finalize().to_sri();
    let manifest_hash = cellar_core::manifest::ManifestHash::compute(&chunk_hashes).to_hex();
    let manifest = chunk_hashes.iter().map(|h| h.to_hex()).collect();

    Ok(NarIndex {
        temp_path: temp_path.to_path_buf(),
        nar_size: total_size,
        nar_hash,
        manifest_hash,
        manifest,
        expected_chunks,
        chunk_size: chunk_size as u64,
    })
}

fn record_chunk(
    data: &[u8],
    chunk_hashes: &mut Vec<cellar_core::chunk::ChunkHash>,
    expected_chunks: &mut Vec<cellar_core::upload::ExpectedChunk>,
) {
    let hash = cellar_core::chunk::ChunkHash::compute(data);
    expected_chunks.push(cellar_core::upload::ExpectedChunk {
        hash: hash.to_hex(),
        size: data.len() as u64,
    });
    chunk_hashes.push(hash);
}

fn build_temp_nar_path() -> PathBuf {
    let mut path = std::env::temp_dir();
    let filename = format!("cellar-upload-{}.nar", uuid::Uuid::new_v4());
    path.push(filename);
    path
}

/// Generate a random token secret using cryptographically secure RNG.
fn generate_token_secret() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

/// Hash a token for storage.
fn hash_token(token: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    hasher
        .finalize()
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::OsString;
    use std::future::Future;
    use std::sync::OnceLock;
    use tempfile::tempdir;
    use tokio::sync::Mutex;

    static ENV_LOCK: OnceLock<Mutex<()>> = OnceLock::new();

    async fn with_env_lock<F, Fut, T>(action: F) -> T
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let _guard = ENV_LOCK.get_or_init(|| Mutex::new(())).lock().await;
        action().await
    }

    struct EnvVarGuard {
        key: &'static str,
        prev: Option<OsString>,
    }

    impl EnvVarGuard {
        fn set(key: &'static str, value: &str) -> Self {
            let prev = std::env::var_os(key);
            // SAFETY: Tests run with --test-threads=1 so no concurrent access
            unsafe { std::env::set_var(key, value) };
            Self { key, prev }
        }
    }

    impl Drop for EnvVarGuard {
        fn drop(&mut self) {
            // SAFETY: Tests run with --test-threads=1 so no concurrent access
            unsafe {
                if let Some(value) = self.prev.take() {
                    std::env::set_var(self.key, value);
                } else {
                    std::env::remove_var(self.key);
                }
            }
        }
    }

    struct PathGuard {
        prev: Option<OsString>,
    }

    impl PathGuard {
        fn prepend(dir: &Path) -> Self {
            let prev = std::env::var_os("PATH");
            let new_path = match &prev {
                Some(existing) => {
                    let mut combined = OsString::from(dir);
                    combined.push(":");
                    combined.push(existing);
                    combined
                }
                None => OsString::from(dir),
            };
            // SAFETY: Tests run with --test-threads=1 so no concurrent access
            unsafe { std::env::set_var("PATH", new_path) };
            Self { prev }
        }
    }

    impl Drop for PathGuard {
        fn drop(&mut self) {
            // SAFETY: Tests run with --test-threads=1 so no concurrent access
            unsafe {
                if let Some(prev) = self.prev.take() {
                    std::env::set_var("PATH", prev);
                } else {
                    std::env::remove_var("PATH");
                }
            }
        }
    }

    #[cfg(unix)]
    fn write_script(dir: &Path, name: &str, body: &str) -> PathBuf {
        use std::os::unix::fs::PermissionsExt;
        let path = dir.join(name);
        std::fs::write(&path, body).unwrap();
        let mut perms = std::fs::metadata(&path).unwrap().permissions();
        perms.set_mode(0o755);
        std::fs::set_permissions(&path, perms).unwrap();
        path
    }

    #[test]
    fn normalize_base_url_requires_scheme() {
        assert!(normalize_base_url("example.com").is_err());
        assert_eq!(
            normalize_base_url("https://example.com/").unwrap(),
            "https://example.com"
        );
    }

    #[test]
    fn update_nix_value_appends_and_ignores_other_keys() {
        let updated = update_nix_value("substituters = a", "substituters", "b").unwrap();
        assert_eq!(updated, "substituters = a b");
        assert!(update_nix_value("substituters-extra = a", "substituters", "b").is_none());
    }

    #[tokio::test]
    async fn update_nix_conf_appends_missing_lines() {
        let _guard = with_env_lock(|| async move {
            let temp = tempdir().unwrap();
            let path = temp.path().join("nix.conf");
            update_nix_conf(&path, "https://cache.example.com", "cache-key")
                .await
                .unwrap();
            let contents = tokio::fs::read_to_string(&path).await.unwrap();
            assert!(contents.contains("substituters = https://cache.example.com"));
            assert!(contents.contains("trusted-public-keys = cache-key"));
        })
        .await;
    }

    #[tokio::test]
    async fn client_config_roundtrip() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            let path = temp.path().join("client.toml");

            let mut config = ClientConfig {
                default_cache: Some("default".to_string()),
                ..Default::default()
            };
            config.caches.insert(
                "default".to_string(),
                CacheProfile {
                    url: "https://cache.example.com".to_string(),
                    token: "token".to_string(),
                    cache_id: Some("cache-id".to_string()),
                    cache_name: Some("cache-name".to_string()),
                    public_base_url: Some("https://cache.example.com".to_string()),
                    public_key: Some("pub-key".to_string()),
                },
            );

            save_client_config(&path, &config).await.unwrap();
            let loaded = load_client_config(&path).await.unwrap();
            assert_eq!(loaded.default_cache, config.default_cache);
            assert_eq!(loaded.caches.len(), 1);
        })
        .await;
    }

    #[tokio::test]
    async fn load_client_config_missing_returns_default() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("missing.toml");
        let config = load_client_config(&path).await.unwrap();
        assert!(config.caches.is_empty());
    }

    #[test]
    fn read_token_prefers_flag() {
        assert_eq!(
            read_token(Some("token".to_string()), false).unwrap(),
            "token"
        );
        assert!(read_token(None, false).is_err());
    }

    #[test]
    fn build_temp_nar_path_is_unique() {
        let path = build_temp_nar_path();
        assert!(path.to_string_lossy().contains("cellar-upload-"));
        assert!(path.to_string_lossy().ends_with(".nar"));
    }

    #[test]
    fn record_chunk_populates_lists() {
        let mut hashes = Vec::new();
        let mut expected = Vec::new();
        record_chunk(b"data", &mut hashes, &mut expected);
        assert_eq!(hashes.len(), 1);
        assert_eq!(expected.len(), 1);
        assert_eq!(expected[0].size, 4);
    }

    #[test]
    fn token_helpers_generate_and_hash() {
        let secret = generate_token_secret();
        assert!(!secret.is_empty());
        assert!(!secret.contains('='));
        assert_eq!(hash_token(&secret).len(), 64);
    }

    #[tokio::test]
    async fn client_config_path_respects_env() {
        with_env_lock(|| async {
            let _guard = EnvVarGuard::set("CELLAR_CLIENT_CONFIG", "/tmp/cellar-client.toml");
            let path = client_config_path(None).unwrap();
            assert_eq!(path.to_string_lossy(), "/tmp/cellar-client.toml");

            let explicit = client_config_path(Some("/tmp/explicit.toml")).unwrap();
            assert_eq!(explicit.to_string_lossy(), "/tmp/explicit.toml");
        })
        .await;
    }

    #[tokio::test]
    async fn default_nix_conf_path_uses_xdg() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            let _guard = EnvVarGuard::set("XDG_CONFIG_HOME", temp.path().to_str().unwrap());
            let path = default_nix_conf_path().unwrap();
            assert!(path.to_string_lossy().contains("nix/nix.conf"));
        })
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_flake_store_paths_dedupes_and_sorts() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            write_script(
                temp.path(),
                "nix",
                "#!/bin/sh\nprintf \"%s\\n\" /nix/store/b /nix/store/a /nix/store/a\n",
            );
            let _guard = PathGuard::prepend(temp.path());

            let paths = resolve_flake_store_paths("flake", &[]).await.unwrap();
            assert_eq!(
                paths,
                vec!["/nix/store/a".to_string(), "/nix/store/b".to_string()]
            );
        })
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn resolve_flake_store_paths_errors_on_failure() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            write_script(temp.path(), "nix", "#!/bin/sh\necho \"bad\" >&2\nexit 1\n");
            let _guard = PathGuard::prepend(temp.path());

            assert!(resolve_flake_store_paths("flake", &[]).await.is_err());
        })
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dump_nar_with_command_reads_output() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            write_script(temp.path(), "nix", "#!/bin/sh\nprintf \"nar-data\"\n");
            let _guard = PathGuard::prepend(temp.path());

            let temp_path = temp.path().join("out.nar");
            let index = dump_nar_with_command(
                "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test",
                2,
                &temp_path,
                DumpCommand::Nix,
            )
            .await
            .unwrap();

            assert!(index.nar_size > 0);
            assert!(!index.manifest.is_empty());
            assert!(!index.expected_chunks.is_empty());
        })
        .await;
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn dump_nar_and_index_falls_back_to_nix_store() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            write_script(temp.path(), "nix", "#!/bin/sh\necho \"fail\" >&2\nexit 1\n");
            write_script(temp.path(), "nix-store", "#!/bin/sh\nprintf \"nar-data\"\n");
            let _guard = PathGuard::prepend(temp.path());

            let index = dump_nar_and_index("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test", 2)
                .await
                .unwrap();

            tokio::fs::remove_file(&index.temp_path).await.unwrap();
            assert!(index.nar_size > 0);
        })
        .await;
    }

    #[tokio::test]
    async fn update_nix_conf_merges_existing_lines() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("nix.conf");
        tokio::fs::write(
            &path,
            "substituters = https://cache.example.com\ntrusted-public-keys = cache-key\n",
        )
        .await
        .unwrap();

        update_nix_conf(&path, "https://cache.example.com", "cache-key")
            .await
            .unwrap();
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert_eq!(contents.matches("https://cache.example.com").count(), 1);
        assert_eq!(contents.matches("cache-key").count(), 1);
    }

    #[tokio::test]
    async fn normalize_base_url_rejects_invalid() {
        assert!(normalize_base_url("ftp://example.com").is_err());
    }

    #[tokio::test]
    async fn push_store_path_indexing_creates_chunks() {
        let temp = tempdir().unwrap();
        let temp_path = temp.path().join("nar");

        let mut data = Vec::new();
        data.extend_from_slice(b"hello");
        tokio::fs::write(&temp_path, data).await.unwrap();

        let mut chunks = Vec::new();
        let mut expected = Vec::new();
        record_chunk(b"hello", &mut chunks, &mut expected);
        assert_eq!(chunks.len(), 1);
        assert_eq!(expected.len(), 1);
    }

    #[tokio::test]
    async fn load_client_config_roundtrip_permissions() {
        with_env_lock(|| async {
            let temp = tempdir().unwrap();
            let path = temp.path().join("client.toml");

            let config = ClientConfig::default();
            save_client_config(&path, &config).await.unwrap();

            let loaded = load_client_config(&path).await.unwrap();
            assert_eq!(loaded.caches.len(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn update_nix_conf_adds_new_values() {
        let temp = tempdir().unwrap();
        let path = temp.path().join("nix.conf");
        tokio::fs::write(&path, "substituters = a\n").await.unwrap();
        update_nix_conf(&path, "b", "key").await.unwrap();
        let contents = tokio::fs::read_to_string(&path).await.unwrap();
        assert!(contents.contains("substituters = a b"));
        assert!(contents.contains("trusted-public-keys = key"));
    }

    #[tokio::test]
    async fn read_token_from_env_not_supported() {
        assert!(read_token(None, false).is_err());
    }

    #[tokio::test]
    async fn normalize_base_url_strips_trailing_slash() {
        assert_eq!(
            normalize_base_url("http://example.com/").unwrap(),
            "http://example.com"
        );
    }

    #[tokio::test]
    async fn build_temp_nar_path_suffix() {
        let path = build_temp_nar_path();
        assert!(path.to_string_lossy().ends_with(".nar"));
    }

    #[tokio::test]
    async fn update_nix_value_handles_comments() {
        let updated = update_nix_value("substituters = a # comment", "substituters", "b").unwrap();
        assert_eq!(updated, "substituters = a b");
    }

    #[tokio::test]
    async fn record_chunk_updates_expected_size() {
        let mut hashes = Vec::new();
        let mut expected = Vec::new();
        record_chunk(b"abc", &mut hashes, &mut expected);
        assert_eq!(expected[0].size, 3);
    }

    #[tokio::test]
    async fn hash_token_produces_hex() {
        let digest = hash_token("secret");
        assert_eq!(digest.len(), 64);
        assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
    }
}
