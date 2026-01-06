//! Admin token initialization.

use anyhow::{Result, bail};
use cellar_core::config::AdminConfig;
use cellar_core::token::TokenScope;
use cellar_metadata::MetadataStore;
use cellar_metadata::models::TokenRow;
use time::OffsetDateTime;
use uuid::Uuid;

/// Ensure the configured admin token exists, rotating the previous one if needed.
///
/// If the token hash changes between restarts, the previous admin token is
/// automatically revoked and a new one is created with the new hash.
pub async fn ensure_admin_token(metadata: &dyn MetadataStore, config: &AdminConfig) -> Result<()> {
    // Normalize to lowercase to match auth.rs hash_token() which uses lowercase hex encoding.
    // Without this, uppercase hashes in config would never match during authentication.
    let hash = config
        .token_hash
        .strip_prefix("sha256:")
        .unwrap_or(&config.token_hash)
        .to_lowercase();
    let hash = hash.as_str();
    if hash.len() != 64 || !hash.chars().all(|c| c.is_ascii_hexdigit()) {
        bail!("invalid admin token_hash: expected 64 hex chars");
    }

    if let Some(existing) = metadata.get_token_by_hash(hash).await? {
        // Reject if the token was previously revoked
        if existing.revoked_at.is_some() {
            bail!(
                "admin token hash matches a revoked token (id={}); \
                 use a new token hash or clear the revoked token",
                existing.token_id
            );
        }
        // Reject if the token is expired
        let now = OffsetDateTime::now_utc();
        if let Some(expires_at) = existing.expires_at
            && expires_at <= now
        {
            bail!(
                "admin token hash matches an expired token (id={}, expired={}); \
                 use a new token hash",
                existing.token_id,
                expires_at
            );
        }
        metadata.set_bootstrap_token_id(existing.token_id).await?;
        tracing::debug!("Admin token already exists");
        return Ok(());
    }

    let now = OffsetDateTime::now_utc();
    if let Some(prev_id) = metadata.get_bootstrap_token_id().await? {
        metadata.revoke_token(prev_id, now).await?;
        tracing::info!(token_id = %prev_id, "Previous admin token revoked");
    }

    let scopes = config
        .token_scopes
        .clone()
        .unwrap_or_else(|| vec!["cache:admin".to_string()]);
    for scope in &scopes {
        TokenScope::parse(scope).map_err(|_| anyhow::anyhow!("invalid admin scope: {scope}"))?;
    }

    let token = TokenRow {
        token_id: Uuid::new_v4(),
        cache_id: None,
        token_hash: hash.to_string(),
        scopes: serde_json::to_string(&scopes)?,
        expires_at: None,
        revoked_at: None,
        created_at: now,
        last_used_at: None,
        description: config.token_description.clone(),
    };

    metadata.create_token(&token).await?;
    metadata.set_bootstrap_token_id(token.token_id).await?;
    tracing::info!(token_id = %token.token_id, "Admin token created");

    Ok(())
}
