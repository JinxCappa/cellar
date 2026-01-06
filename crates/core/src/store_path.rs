//! Nix store path types and parsing.

use serde::{Deserialize, Serialize};
use std::fmt;

/// A Nix store path hash (the 32-character base32 portion).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorePathHash(String);

impl StorePathHash {
    /// Create from a string, validating format.
    pub fn new(hash: impl Into<String>) -> crate::Result<Self> {
        let hash = hash.into();
        if hash.len() != 32 {
            return Err(crate::Error::InvalidStorePath(format!(
                "store path hash must be 32 chars, got {}",
                hash.len()
            )));
        }
        // Nix base32 alphabet: 0-9, a-d, f-n, p-s, v-z
        for c in hash.chars() {
            if !matches!(c, '0'..='9' | 'a'..='d' | 'f'..='n' | 'p'..='s' | 'v'..='z') {
                return Err(crate::Error::InvalidStorePath(format!(
                    "invalid character in store path hash: {c}"
                )));
            }
        }
        Ok(Self(hash))
    }

    /// Get the hash string.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorePathHash({self})")
    }
}

impl fmt::Display for StorePathHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A full Nix store path (/nix/store/<hash>-<name>).
#[derive(Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct StorePath {
    hash: StorePathHash,
    name: String,
}

impl StorePath {
    /// The standard Nix store directory.
    pub const STORE_DIR: &'static str = "/nix/store";

    /// Parse a full store path string.
    pub fn parse(path: &str) -> crate::Result<Self> {
        let prefix = format!("{}/", Self::STORE_DIR);
        let rest = path
            .strip_prefix(&prefix)
            .ok_or_else(|| crate::Error::InvalidStorePath(format!("must start with {prefix}")))?;

        if !rest.is_ascii() {
            return Err(crate::Error::InvalidStorePath(
                "store path contains non-ASCII characters".to_string(),
            ));
        }

        if rest.len() < 34 {
            return Err(crate::Error::InvalidStorePath("path too short".to_string()));
        }

        let hash_part = &rest[..32];
        let sep = rest.chars().nth(32);
        if sep != Some('-') {
            return Err(crate::Error::InvalidStorePath(
                "expected '-' after hash".to_string(),
            ));
        }

        let name = &rest[33..];
        if name.is_empty() {
            return Err(crate::Error::InvalidStorePath(
                "name cannot be empty".to_string(),
            ));
        }

        // Validate name characters (alphanumeric, -, _, ., +)
        for c in name.chars() {
            if !matches!(c, 'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' | '+') {
                return Err(crate::Error::InvalidStorePath(format!(
                    "invalid character in name: {c}"
                )));
            }
        }

        Ok(Self {
            hash: StorePathHash::new(hash_part)?,
            name: name.to_string(),
        })
    }

    /// Create from components.
    pub fn new(hash: StorePathHash, name: impl Into<String>) -> crate::Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(crate::Error::InvalidStorePath(
                "name cannot be empty".to_string(),
            ));
        }
        Ok(Self { hash, name })
    }

    /// Get the store path hash.
    pub fn hash(&self) -> &StorePathHash {
        &self.hash
    }

    /// Get the name portion.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the full path string.
    pub fn to_path_string(&self) -> String {
        format!("{}/{}-{}", Self::STORE_DIR, self.hash, self.name)
    }

    /// Get the basename (`hash-name`) without the `/nix/store/` prefix.
    pub fn basename(&self) -> String {
        format!("{}-{}", self.hash, self.name)
    }

    /// Construct a `StorePath` from a basename (`hash-name`) string.
    pub fn from_basename(basename: &str) -> crate::Result<Self> {
        let path = format!("{}/{}", Self::STORE_DIR, basename);
        Self::parse(&path)
    }
}

impl fmt::Debug for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StorePath({})", self.to_path_string())
    }
}

impl fmt::Display for StorePath {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_path_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_valid_store_path() {
        let path = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo";
        let parsed = StorePath::parse(path).unwrap();
        assert_eq!(parsed.hash().as_str(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(parsed.name(), "foo");
        assert_eq!(parsed.to_path_string(), path);
    }

    #[test]
    fn test_parse_invalid_prefix() {
        let path = "/usr/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-foo";
        assert!(StorePath::parse(path).is_err());
    }

    #[test]
    fn test_parse_non_ascii_does_not_panic() {
        // Multi-byte UTF-8 that passes byte-length check but would panic on byte slicing
        let path = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa\u{00e9}-foo";
        let result = StorePath::parse(path);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-ASCII"),);
    }

    #[test]
    fn test_parse_invalid_hash_char() {
        // 'e' is not in Nix base32 alphabet
        let path = "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaea-foo";
        assert!(StorePath::parse(path).is_err());
    }

    #[test]
    fn test_basename() {
        let path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test-pkg").unwrap();
        assert_eq!(path.basename(), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test-pkg");
    }

    #[test]
    fn test_from_basename() {
        let path = StorePath::from_basename("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test-pkg").unwrap();
        assert_eq!(
            path.to_path_string(),
            "/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test-pkg"
        );
    }
}
