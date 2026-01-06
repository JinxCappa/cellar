//! Ed25519 key types and operations.

use crate::error::{SignerError, SignerResult};
use base64::Engine;
use ed25519_dalek::{SigningKey, VerifyingKey};
use std::fmt;

/// A secret (private) key for signing.
pub struct SecretKey {
    inner: SigningKey,
}

impl SecretKey {
    /// Generate a new random secret key.
    pub fn generate() -> Self {
        use ed25519_dalek::SigningKey;
        let mut rng = rand_core::OsRng;
        Self {
            inner: SigningKey::generate(&mut rng),
        }
    }

    /// Parse from Nix secret key format: keyname:base64(secret || public).
    pub fn from_nix_format(s: &str) -> SignerResult<(String, Self)> {
        let (name, b64) = s.split_once(':').ok_or_else(|| {
            SignerError::KeyParsing("expected 'keyname:base64' format".to_string())
        })?;

        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| SignerError::KeyParsing(format!("invalid base64: {e}")))?;

        if bytes.len() != 64 {
            return Err(SignerError::KeyParsing(format!(
                "expected 64 bytes, got {}",
                bytes.len()
            )));
        }

        let secret_bytes: [u8; 32] = bytes[..32]
            .try_into()
            .map_err(|_| SignerError::KeyParsing("invalid secret key bytes".to_string()))?;

        let inner = SigningKey::from_bytes(&secret_bytes);

        Ok((name.to_string(), Self { inner }))
    }

    /// Encode as Nix secret key format.
    pub fn to_nix_format(&self, key_name: &str) -> String {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(self.inner.as_bytes());
        bytes.extend_from_slice(self.inner.verifying_key().as_bytes());
        let b64 = base64::engine::general_purpose::STANDARD.encode(&bytes);
        format!("{key_name}:{b64}")
    }

    /// Get the corresponding public key.
    pub fn public_key(&self) -> PublicKey {
        PublicKey {
            inner: self.inner.verifying_key(),
        }
    }

    /// Get the inner signing key.
    pub(crate) fn signing_key(&self) -> &SigningKey {
        &self.inner
    }
}

impl fmt::Debug for SecretKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "SecretKey([REDACTED])")
    }
}

/// A public key for verification.
#[derive(Clone)]
pub struct PublicKey {
    inner: VerifyingKey,
}

impl PublicKey {
    /// Parse from Nix public key format: keyname:base64(public).
    pub fn from_nix_format(s: &str) -> SignerResult<(String, Self)> {
        let (name, b64) = s.split_once(':').ok_or_else(|| {
            SignerError::KeyParsing("expected 'keyname:base64' format".to_string())
        })?;

        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64)
            .map_err(|e| SignerError::KeyParsing(format!("invalid base64: {e}")))?;

        if bytes.len() != 32 {
            return Err(SignerError::KeyParsing(format!(
                "expected 32 bytes, got {}",
                bytes.len()
            )));
        }

        let key_bytes: [u8; 32] = bytes
            .try_into()
            .map_err(|_| SignerError::KeyParsing("invalid public key bytes".to_string()))?;

        let inner = VerifyingKey::from_bytes(&key_bytes)
            .map_err(|e| SignerError::KeyParsing(format!("invalid public key: {e}")))?;

        Ok((name.to_string(), Self { inner }))
    }

    /// Encode as Nix public key format.
    pub fn to_nix_format(&self, key_name: &str) -> String {
        let b64 = base64::engine::general_purpose::STANDARD.encode(self.inner.as_bytes());
        format!("{key_name}:{b64}")
    }

    /// Get the inner verifying key.
    pub(crate) fn verifying_key(&self) -> &VerifyingKey {
        &self.inner
    }
}

impl fmt::Debug for PublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let b64 = base64::engine::general_purpose::STANDARD.encode(self.inner.as_bytes());
        write!(f, "PublicKey({}...)", &b64[..8])
    }
}

/// A key pair containing both secret and public keys.
pub struct KeyPair {
    /// The key name (e.g., "cache.example.com-1").
    pub name: String,
    /// The secret key.
    pub secret: SecretKey,
    /// The public key.
    pub public: PublicKey,
}

impl KeyPair {
    /// Generate a new key pair with the given name.
    pub fn generate(name: impl Into<String>) -> Self {
        let secret = SecretKey::generate();
        let public = secret.public_key();
        Self {
            name: name.into(),
            secret,
            public,
        }
    }

    /// Parse from Nix secret key format.
    pub fn from_nix_secret_key(s: &str) -> SignerResult<Self> {
        let (name, secret) = SecretKey::from_nix_format(s)?;
        let public = secret.public_key();
        Ok(Self {
            name,
            secret,
            public,
        })
    }

    /// Get the Nix-format secret key string.
    pub fn to_nix_secret_key(&self) -> String {
        self.secret.to_nix_format(&self.name)
    }

    /// Get the Nix-format public key string.
    pub fn to_nix_public_key(&self) -> String {
        self.public.to_nix_format(&self.name)
    }
}

impl fmt::Debug for KeyPair {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("KeyPair")
            .field("name", &self.name)
            .field("public", &self.public)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_generation() {
        let keypair = KeyPair::generate("test-key-1");
        assert_eq!(keypair.name, "test-key-1");
    }

    #[test]
    fn test_secret_key_roundtrip() {
        let keypair = KeyPair::generate("test-key-1");
        let nix_format = keypair.to_nix_secret_key();

        let parsed = KeyPair::from_nix_secret_key(&nix_format).unwrap();
        assert_eq!(parsed.name, keypair.name);

        // Public keys should match
        assert_eq!(
            parsed.public.inner.as_bytes(),
            keypair.public.inner.as_bytes()
        );
    }

    #[test]
    fn test_public_key_roundtrip() {
        let keypair = KeyPair::generate("test-key-1");
        let nix_format = keypair.to_nix_public_key();

        let (name, public) = PublicKey::from_nix_format(&nix_format).unwrap();
        assert_eq!(name, keypair.name);
        assert_eq!(public.inner.as_bytes(), keypair.public.inner.as_bytes());
    }
}
