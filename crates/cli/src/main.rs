//! Administrative CLI for Cellar.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use figment::providers::{Env, Format, Toml};
use figment::Figment;
use cellar_core::config::AppConfig;
use cellar_signer::KeyPair;

#[derive(Parser)]
#[command(name = "cellarctl")]
#[command(about = "Administrative CLI for Cellar")]
#[command(version)]
struct Cli {
    /// Configuration file path
    #[arg(short, long, default_value = "config/server.toml")]
    config: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Key management commands
    Key {
        #[command(subcommand)]
        command: KeyCommands,
    },
    /// Token management commands
    Token {
        #[command(subcommand)]
        command: TokenCommands,
    },
    /// Garbage collection commands
    Gc {
        #[command(subcommand)]
        command: GcCommands,
    },
    /// Database management commands
    Db {
        #[command(subcommand)]
        command: DbCommands,
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
        #[arg(short, long)]
        key_file: String,
    },
}

#[derive(Subcommand)]
enum TokenCommands {
    /// Create a new token
    Create {
        /// Scopes to grant (comma-separated)
        #[arg(short, long)]
        scopes: String,
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
        /// GC job type (upload_gc, chunk_gc, manifest_gc)
        #[arg(short, long, default_value = "upload_gc")]
        job_type: String,
    },
    /// Show GC status
    Status {
        /// Job ID (optional, shows recent jobs if not specified)
        job_id: Option<String>,
    },
}

#[derive(Subcommand)]
enum DbCommands {
    /// Run database migrations
    Migrate,
    /// Show database statistics
    Stats,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();

    // Load configuration
    let config: AppConfig = Figment::new()
        .merge(Toml::file(&cli.config))
        .merge(Env::prefixed("CELLAR_").split("__"))
        .extract()
        .context("failed to load configuration")?;

    match cli.command {
        Commands::Key { command } => handle_key_command(command).await,
        Commands::Token { command } => handle_token_command(command, &config).await,
        Commands::Gc { command } => handle_gc_command(command, &config).await,
        Commands::Db { command } => handle_db_command(command, &config).await,
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
        KeyCommands::Public { key_file } => {
            let secret_key = tokio::fs::read_to_string(&key_file)
                .await
                .with_context(|| format!("failed to read key file: {key_file}"))?;

            let keypair = KeyPair::from_nix_secret_key(secret_key.trim())
                .context("failed to parse secret key")?;

            println!("Public key:");
            println!("{}", keypair.to_nix_public_key());
        }
    }
    Ok(())
}

async fn handle_token_command(command: TokenCommands, config: &AppConfig) -> Result<()> {
    let metadata = cellar_metadata::from_config(&config.metadata)
        .await
        .context("failed to initialize metadata store")?;

    match command {
        TokenCommands::Create {
            scopes,
            expires_in,
            description,
        } => {
            use cellar_metadata::models::TokenRow;
            use sha2::{Digest, Sha256};
            use time::OffsetDateTime;
            use uuid::Uuid;

            // Generate token
            let token_secret = generate_token_secret();
            let mut hasher = Sha256::new();
            hasher.update(token_secret.as_bytes());
            let token_hash: String = hasher
                .finalize()
                .iter()
                .map(|b| format!("{b:02x}"))
                .collect();

            let now = OffsetDateTime::now_utc();
            let expires_at =
                expires_in.map(|secs| now + time::Duration::seconds(secs as i64));

            let scopes_vec: Vec<String> = scopes.split(',').map(|s| s.trim().to_string()).collect();
            let token_id = Uuid::new_v4();

            let token_row = TokenRow {
                token_id,
                cache_id: None,
                token_hash,
                scopes: serde_json::to_string(&scopes_vec)?,
                expires_at,
                revoked_at: None,
                created_at: now,
                last_used_at: None,
                description,
            };

            metadata.create_token(&token_row).await?;

            println!("Token created successfully!");
            println!("\nToken ID: {token_id}");
            println!("Token secret: {token_secret}");
            println!("\nIMPORTANT: Save this token secret now. It cannot be recovered.");
            if let Some(expires) = expires_at {
                println!(
                    "Expires: {}",
                    expires
                        .format(&time::format_description::well_known::Rfc3339)
                        .unwrap()
                );
            }
        }
        TokenCommands::List => {
            let tokens = metadata.list_tokens(None).await?;

            if tokens.is_empty() {
                println!("No tokens found.");
            } else {
                println!("{:<38} {:<20} {:<15} {}", "ID", "Scopes", "Status", "Created");
                println!("{}", "-".repeat(100));

                for token in tokens {
                    let status = if token.revoked_at.is_some() {
                        "revoked"
                    } else if token
                        .expires_at
                        .map(|t| t < OffsetDateTime::now_utc())
                        .unwrap_or(false)
                    {
                        "expired"
                    } else {
                        "active"
                    };

                    let scopes: Vec<String> =
                        serde_json::from_str(&token.scopes).unwrap_or_default();

                    println!(
                        "{:<38} {:<20} {:<15} {}",
                        token.token_id,
                        scopes.join(","),
                        status,
                        token
                            .created_at
                            .format(&time::format_description::well_known::Rfc3339)
                            .unwrap()
                    );
                }
            }
        }
        TokenCommands::Revoke { token_id } => {
            use time::OffsetDateTime;
            use uuid::Uuid;

            let id = Uuid::parse_str(&token_id).context("invalid token ID")?;
            metadata.revoke_token(id, OffsetDateTime::now_utc()).await?;
            println!("Token revoked: {token_id}");
        }
    }
    Ok(())
}

async fn handle_gc_command(command: GcCommands, config: &AppConfig) -> Result<()> {
    let metadata = cellar_metadata::from_config(&config.metadata)
        .await
        .context("failed to initialize metadata store")?;

    match command {
        GcCommands::Run { job_type } => {
            use cellar_metadata::models::GcJobRow;
            use cellar_metadata::repos::gc::{GcJobState, GcJobType};
            use time::OffsetDateTime;
            use uuid::Uuid;

            let job_type = match job_type.as_str() {
                "upload_gc" => GcJobType::UploadGc,
                "chunk_gc" => GcJobType::ChunkGc,
                "manifest_gc" => GcJobType::ManifestGc,
                _ => anyhow::bail!("unknown job type: {job_type}"),
            };

            let job_id = Uuid::new_v4();
            let now = OffsetDateTime::now_utc();

            let job = GcJobRow {
                gc_job_id: job_id,
                cache_id: None,
                job_type: job_type.as_str().to_string(),
                state: GcJobState::Running.as_str().to_string(),
                started_at: Some(now),
                finished_at: None,
                stats_json: None,
            };

            metadata.create_gc_job(&job).await?;
            println!("Started GC job: {job_id}");
            println!("Job type: {}", job_type.as_str());

            // Note: Actual GC logic would run here
            // For CLI, we'd need to implement the actual GC operations
            println!("\nGC job submitted. Use 'cellarctl gc status {job_id}' to check progress.");
        }
        GcCommands::Status { job_id } => {
            use uuid::Uuid;

            if let Some(id) = job_id {
                let job_id = Uuid::parse_str(&id).context("invalid job ID")?;
                if let Some(job) = metadata.get_gc_job(job_id).await? {
                    println!("Job ID: {}", job.gc_job_id);
                    println!("Type: {}", job.job_type);
                    println!("State: {}", job.state);
                    if let Some(started) = job.started_at {
                        println!(
                            "Started: {}",
                            started
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap()
                        );
                    }
                    if let Some(finished) = job.finished_at {
                        println!(
                            "Finished: {}",
                            finished
                                .format(&time::format_description::well_known::Rfc3339)
                                .unwrap()
                        );
                    }
                    if let Some(stats) = job.stats_json {
                        println!("Stats: {stats}");
                    }
                } else {
                    println!("Job not found: {id}");
                }
            } else {
                let jobs = metadata.get_recent_gc_jobs(10).await?;
                if jobs.is_empty() {
                    println!("No GC jobs found.");
                } else {
                    println!("{:<38} {:<15} {:<10} {}", "ID", "Type", "State", "Started");
                    println!("{}", "-".repeat(90));
                    for job in jobs {
                        let started = job
                            .started_at
                            .map(|t| {
                                t.format(&time::format_description::well_known::Rfc3339)
                                    .unwrap()
                            })
                            .unwrap_or_else(|| "-".to_string());
                        println!(
                            "{:<38} {:<15} {:<10} {}",
                            job.gc_job_id, job.job_type, job.state, started
                        );
                    }
                }
            }
        }
    }
    Ok(())
}

async fn handle_db_command(command: DbCommands, config: &AppConfig) -> Result<()> {
    match command {
        DbCommands::Migrate => {
            let metadata = cellar_metadata::from_config(&config.metadata)
                .await
                .context("failed to initialize metadata store")?;

            metadata.migrate().await?;
            println!("Migrations completed successfully.");
        }
        DbCommands::Stats => {
            let metadata = cellar_metadata::from_config(&config.metadata)
                .await
                .context("failed to initialize metadata store")?;

            let store_paths = metadata.count_store_paths().await?;
            let chunk_stats = metadata.get_stats().await?;

            println!("Database Statistics:");
            println!("  Store paths: {store_paths}");
            println!("  Total chunks: {}", chunk_stats.count);
            println!("  Total chunk size: {} bytes", chunk_stats.total_size);
            println!("  Referenced chunks: {}", chunk_stats.referenced_count);
            println!("  Unreferenced chunks: {}", chunk_stats.unreferenced_count);
        }
    }
    Ok(())
}

/// Generate a random token secret using cryptographically secure RNG.
fn generate_token_secret() -> String {
    use base64::Engine;
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

use time::OffsetDateTime;
