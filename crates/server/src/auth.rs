//! Authentication and authorization middleware.

use crate::error::{ApiError, ApiResult};
use crate::ratelimit::TokenIdExtension;
use crate::state::AppState;
use axum::extract::{Request, State};
use axum::http::header::AUTHORIZATION;
use axum::middleware::Next;
use axum::response::Response;
use cellar_core::token::{Token, TokenScope};
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use time::OffsetDateTime;
use tracing::Instrument;
use uuid::Uuid;

/// Maximum length for trace IDs.
/// Longer trace IDs are truncated to prevent log bloat and potential log injection.
const MAX_TRACE_ID_LEN: usize = 128;

/// Trace ID for request correlation.
#[derive(Clone, Debug)]
pub struct TraceId(pub String);

impl TraceId {
    /// Generate a new random trace ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Create a trace ID from a client-provided value.
    /// The value is sanitized: truncated to MAX_TRACE_ID_LEN characters and non-printable characters removed.
    pub fn from_client(value: &str) -> Self {
        // Truncate to max length using char iterator to avoid UTF-8 boundary panics.
        // We limit by character count, not byte count, to safely handle multi-byte UTF-8.
        // Then filter to ASCII-only for log safety.
        let sanitized: String = value
            .chars()
            .take(MAX_TRACE_ID_LEN)
            .filter(|c| c.is_ascii_graphic() || *c == ' ')
            .collect();

        if sanitized.is_empty() {
            Self::new()
        } else {
            Self(sanitized)
        }
    }

    /// Get the trace ID as a string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl Default for TraceId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for TraceId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Authenticated request extension.
#[derive(Clone, Debug)]
pub struct AuthenticatedUser {
    /// The validated token.
    pub token: Token,
}

impl AuthenticatedUser {
    /// Check if the user has a specific scope.
    pub fn has_scope(&self, scope: TokenScope) -> bool {
        self.token.has_scope(scope)
    }

    /// Require a specific scope, returning an error if not present.
    pub fn require_scope(&self, scope: TokenScope) -> ApiResult<()> {
        if self.has_scope(scope) {
            Ok(())
        } else {
            Err(ApiError::Forbidden(format!(
                "missing required scope: {}",
                scope
            )))
        }
    }
}

/// Extract bearer token from Authorization header.
/// Per RFC 6750, the "Bearer" scheme is case-insensitive.
fn extract_bearer_token(req: &Request) -> Option<&str> {
    req.headers()
        .get(AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            // RFC 6750: Authorization scheme is case-insensitive
            if v.len() >= 7 && v[..7].eq_ignore_ascii_case("bearer ") {
                Some(&v[7..])
            } else {
                None
            }
        })
}

/// Extract trace ID from X-Trace-Id header or generate a new one.
/// Client-provided trace IDs are sanitized to prevent log injection and truncated to 128 chars.
fn extract_or_generate_trace_id(req: &Request) -> TraceId {
    req.headers()
        .get("x-trace-id")
        .and_then(|v| v.to_str().ok())
        .map(TraceId::from_client)
        .unwrap_or_else(TraceId::new)
}

/// Hash a token for storage lookup.
fn hash_token(token: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(token.as_bytes());
    let result = hasher.finalize();
    hex::encode(result)
}

/// Authentication middleware that validates tokens and sets up trace context.
pub async fn auth_middleware(
    State(state): State<AppState>,
    mut req: Request,
    next: Next,
) -> Result<Response, ApiError> {
    // Extract or generate trace ID
    let trace_id = extract_or_generate_trace_id(&req);
    let trace_id_str = trace_id.0.clone();

    // Store trace ID in request extensions
    req.extensions_mut().insert(trace_id);

    // Extract token from header
    let token_str = extract_bearer_token(&req);

    if let Some(token_str) = token_str {
        let token_hash = hash_token(token_str);

        // Look up token in metadata store
        if let Some(token_row) = state.metadata.get_token_by_hash(&token_hash).await? {
            // Parse scopes from JSON
            let scopes: Vec<String> = serde_json::from_str(&token_row.scopes)
                .map_err(|e| ApiError::Internal(format!("invalid token scopes: {e}")))?;

            let scopes: HashSet<TokenScope> = scopes
                .iter()
                .filter_map(|s| match TokenScope::parse(s) {
                    Ok(scope) => Some(scope),
                    Err(_) => {
                        tracing::warn!(
                            token_id = %token_row.token_id,
                            invalid_scope = %s,
                            "Token contains invalid scope, ignoring"
                        );
                        None
                    }
                })
                .collect();

            let token = Token {
                id: cellar_core::token::TokenId::parse(&token_row.token_id.to_string())?,
                cache_id: token_row.cache_id,
                scopes,
                expires_at: token_row.expires_at,
                revoked_at: token_row.revoked_at,
                created_at: token_row.created_at,
                description: token_row.description,
            };

            // Check if token is valid
            if !token.is_valid() {
                return Err(ApiError::Unauthorized(
                    "token expired or revoked".to_string(),
                ));
            }

            // Update last used time (fire and forget)
            let metadata = state.metadata.clone();
            let token_id = token_row.token_id;
            tokio::spawn(async move {
                let _ = metadata
                    .touch_token(token_id, OffsetDateTime::now_utc())
                    .await;
            });

            // Add authenticated user to request extensions
            req.extensions_mut().insert(AuthenticatedUser { token });

            // Add token ID extension for rate limiting
            req.extensions_mut()
                .insert(TokenIdExtension(token_row.token_id.to_string()));
        }
    }

    // Run the request within a tracing span that includes the trace ID
    let response = next
        .run(req)
        .instrument(tracing::info_span!("request", trace_id = %trace_id_str))
        .await;

    Ok(response)
}

/// Require authentication (token must be present).
pub fn require_auth(req: &Request) -> ApiResult<&AuthenticatedUser> {
    req.extensions()
        .get::<AuthenticatedUser>()
        .ok_or_else(|| ApiError::Unauthorized("authentication required".to_string()))
}

/// Get optional authentication.
pub fn get_auth(req: &Request) -> Option<&AuthenticatedUser> {
    req.extensions().get::<AuthenticatedUser>()
}

/// Get the trace ID from request extensions.
pub fn get_trace_id(req: &Request) -> Option<&TraceId> {
    req.extensions().get::<TraceId>()
}

// Note: hex is a simple utility, we'll inline it
mod hex {
    pub fn encode(bytes: impl AsRef<[u8]>) -> String {
        bytes.as_ref().iter().map(|b| format!("{b:02x}")).collect()
    }
}
