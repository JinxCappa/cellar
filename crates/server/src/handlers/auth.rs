//! Authentication-related endpoints.

use crate::auth::require_auth;
use crate::error::{ApiError, ApiResult};
use crate::state::AppState;
use axum::Json;
use axum::extract::{Request, State};
use serde::Serialize;
use time::format_description::well_known::Rfc3339;

/// Response for the authenticated caller.
#[derive(Debug, Serialize)]
pub struct WhoamiResponse {
    pub token_id: String,
    pub cache_id: Option<String>,
    pub cache_name: Option<String>,
    pub scopes: Vec<String>,
    pub expires_at: Option<String>,
    pub public_base_url: Option<String>,
    pub signing_public_key: Option<String>,
}

/// GET /v1/auth/whoami - Return token identity and cache context.
pub async fn whoami(
    State(state): State<AppState>,
    req: Request,
) -> ApiResult<Json<WhoamiResponse>> {
    let auth = require_auth(&req)?;
    let token = &auth.token;

    let cache = match token.cache_id {
        Some(cache_id) => state.metadata.get_cache(cache_id).await?,
        None => None,
    };

    let mut scopes: Vec<String> = token
        .scopes
        .iter()
        .map(|s| s.as_str().to_string())
        .collect();
    scopes.sort();

    let expires_at = match token.expires_at {
        Some(ts) => Some(
            ts.format(&Rfc3339)
                .map_err(|e| ApiError::Internal(format!("failed to format expires_at: {e}")))?,
        ),
        None => None,
    };

    let signing_public_key = state.signer.as_ref().map(|signer| signer.nix_public_key());

    Ok(Json(WhoamiResponse {
        token_id: token.id.to_string(),
        cache_id: token.cache_id.map(|id| id.to_string()),
        cache_name: cache.as_ref().map(|row| row.cache_name.clone()),
        public_base_url: cache.and_then(|row| row.public_base_url),
        scopes,
        expires_at,
        signing_public_key,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthenticatedUser;
    use crate::state::{AppState, GcTaskRegistry};
    use axum::body::Body;
    use cellar_core::config::AppConfig;
    use cellar_core::storage_domain::default_storage_domain_id;
    use cellar_core::token::{Token, TokenScope};
    use cellar_metadata::SqliteStore;
    use cellar_metadata::models::CacheRow;
    use cellar_metadata::repos::CacheRepo;
    use cellar_signer::NarInfoSigner;
    use cellar_storage::backends::filesystem::FilesystemBackend;
    use std::collections::HashSet;
    use std::sync::Arc;
    use time::OffsetDateTime;
    use time::format_description::well_known::Rfc3339;
    use uuid::Uuid;

    #[tokio::test]
    async fn test_whoami_includes_cache_and_signer() {
        let storage_dir = tempfile::tempdir().unwrap();
        let storage = FilesystemBackend::new(storage_dir.path()).await.unwrap();

        let metadata_dir = tempfile::tempdir().unwrap();
        let db_path = metadata_dir.path().join("metadata.db");
        let metadata = Arc::new(SqliteStore::new(&db_path, None).await.unwrap());

        let gc_registry = Arc::new(GcTaskRegistry::new(metadata.clone()));
        let signer = NarInfoSigner::generate("test-cache");
        let signing_public_key = signer.nix_public_key();

        let state = AppState::new(
            AppConfig::for_testing(),
            Arc::new(storage),
            metadata.clone(),
            Some(signer),
            gc_registry,
        );

        let now = OffsetDateTime::now_utc();
        let cache_id = Uuid::new_v4();
        let cache = CacheRow {
            cache_id,
            domain_id: default_storage_domain_id(),
            cache_name: "test-cache".to_string(),
            public_base_url: Some("https://example.test".to_string()),
            is_public: true,
            is_default: false,
            created_at: now,
            updated_at: now,
        };
        metadata.create_cache(&cache).await.unwrap();

        let mut scopes = HashSet::new();
        scopes.insert(TokenScope::CacheWrite);
        scopes.insert(TokenScope::CacheRead);
        let expires_at = now + time::Duration::seconds(3600);
        let token = Token {
            id: cellar_core::token::TokenId::new(),
            cache_id: Some(cache_id),
            scopes,
            expires_at: Some(expires_at),
            revoked_at: None,
            created_at: now,
            description: None,
        };

        let mut req = Request::new(Body::empty());
        req.extensions_mut().insert(AuthenticatedUser {
            token: token.clone(),
        });

        let Json(response) = whoami(State(state), req).await.unwrap();

        assert_eq!(response.token_id, token.id.to_string());
        assert_eq!(response.cache_id, Some(cache_id.to_string()));
        assert_eq!(response.cache_name, Some("test-cache".to_string()));
        assert_eq!(
            response.public_base_url,
            Some("https://example.test".to_string())
        );
        assert_eq!(
            response.scopes,
            vec!["cache:read".to_string(), "cache:write".to_string()]
        );
        assert_eq!(
            response.expires_at,
            Some(expires_at.format(&Rfc3339).unwrap())
        );
        assert_eq!(response.signing_public_key, Some(signing_public_key));
    }
}
