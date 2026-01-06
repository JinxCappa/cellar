//! Token types and authorization.

use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::fmt;
use time::OffsetDateTime;
use uuid::Uuid;

/// Unique identifier for a token.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TokenId(Uuid);

impl TokenId {
    /// Generate a new random token ID.
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }

    /// Parse from a string.
    pub fn parse(s: &str) -> crate::Result<Self> {
        Uuid::parse_str(s)
            .map(Self)
            .map_err(|e| crate::Error::InvalidToken(format!("invalid token ID: {e}")))
    }

    /// Get the underlying UUID.
    pub fn as_uuid(&self) -> &Uuid {
        &self.0
    }
}

impl Default for TokenId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Debug for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenId({})", self.0)
    }
}

impl fmt::Display for TokenId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Token scopes for authorization.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TokenScope {
    /// Read access (download NARs and narinfo).
    #[serde(rename = "cache:read")]
    CacheRead,
    /// Write access (upload NARs).
    #[serde(rename = "cache:write")]
    CacheWrite,
    /// Admin access (GC, signing, config).
    #[serde(rename = "cache:admin")]
    CacheAdmin,
}

impl TokenScope {
    /// Parse from string.
    pub fn parse(s: &str) -> crate::Result<Self> {
        match s {
            "cache:read" => Ok(Self::CacheRead),
            "cache:write" => Ok(Self::CacheWrite),
            "cache:admin" => Ok(Self::CacheAdmin),
            _ => Err(crate::Error::InvalidToken(format!("unknown scope: {s}"))),
        }
    }

    /// Get the string representation.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CacheRead => "cache:read",
            Self::CacheWrite => "cache:write",
            Self::CacheAdmin => "cache:admin",
        }
    }

    /// Check if this scope implies another scope.
    pub fn implies(&self, other: &Self) -> bool {
        match self {
            Self::CacheAdmin => true, // Admin implies all
            Self::CacheWrite => matches!(other, Self::CacheWrite | Self::CacheRead),
            Self::CacheRead => matches!(other, Self::CacheRead),
        }
    }
}

impl fmt::Display for TokenScope {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// A validated token with its metadata.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Token {
    /// Token identifier.
    pub id: TokenId,
    /// Cache this token is scoped to (if any).
    pub cache_id: Option<Uuid>,
    /// Granted scopes.
    pub scopes: HashSet<TokenScope>,
    /// When the token expires.
    #[serde(with = "time::serde::rfc3339::option")]
    pub expires_at: Option<OffsetDateTime>,
    /// When the token was revoked (if revoked).
    #[serde(with = "time::serde::rfc3339::option")]
    pub revoked_at: Option<OffsetDateTime>,
    /// When the token was created.
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    /// Description for the token.
    pub description: Option<String>,
}

impl Token {
    /// Check if the token is valid (not expired or revoked).
    pub fn is_valid(&self) -> bool {
        let now = OffsetDateTime::now_utc();

        if self.revoked_at.is_some() {
            return false;
        }

        if let Some(expires_at) = self.expires_at
            && now > expires_at
        {
            return false;
        }

        true
    }

    /// Check if the token has a specific scope.
    pub fn has_scope(&self, scope: TokenScope) -> bool {
        self.scopes.iter().any(|s| s.implies(&scope))
    }

    /// Check if the token can read from the cache.
    pub fn can_read(&self) -> bool {
        self.is_valid() && self.has_scope(TokenScope::CacheRead)
    }

    /// Check if the token can write to the cache.
    pub fn can_write(&self) -> bool {
        self.is_valid() && self.has_scope(TokenScope::CacheWrite)
    }

    /// Check if the token has admin access.
    pub fn is_admin(&self) -> bool {
        self.is_valid() && self.has_scope(TokenScope::CacheAdmin)
    }
}

/// Request to create a token.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateTokenRequest {
    /// Scopes to grant.
    pub scopes: Vec<String>,
    /// Expiration duration in seconds (optional).
    pub expires_in: Option<u64>,
    /// Description for the token.
    pub description: Option<String>,
}

/// Response from creating a token.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CreateTokenResponse {
    /// The token ID.
    pub token_id: String,
    /// The token secret (only returned once).
    pub token_secret: String,
    /// When the token expires.
    pub expires_at: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_scope_implies() {
        assert!(TokenScope::CacheAdmin.implies(&TokenScope::CacheRead));
        assert!(TokenScope::CacheAdmin.implies(&TokenScope::CacheWrite));
        assert!(TokenScope::CacheAdmin.implies(&TokenScope::CacheAdmin));

        assert!(TokenScope::CacheWrite.implies(&TokenScope::CacheRead));
        assert!(TokenScope::CacheWrite.implies(&TokenScope::CacheWrite));
        assert!(!TokenScope::CacheWrite.implies(&TokenScope::CacheAdmin));

        assert!(TokenScope::CacheRead.implies(&TokenScope::CacheRead));
        assert!(!TokenScope::CacheRead.implies(&TokenScope::CacheWrite));
    }

    #[test]
    fn test_scope_parse() {
        assert_eq!(
            TokenScope::parse("cache:read").unwrap(),
            TokenScope::CacheRead
        );
        assert_eq!(
            TokenScope::parse("cache:write").unwrap(),
            TokenScope::CacheWrite
        );
        assert_eq!(
            TokenScope::parse("cache:admin").unwrap(),
            TokenScope::CacheAdmin
        );
        assert!(TokenScope::parse("invalid").is_err());
    }
}
