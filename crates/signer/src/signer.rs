//! Narinfo signing and verification.

use crate::error::{SignerError, SignerResult};
use crate::key::{KeyPair, PublicKey};
use base64::Engine;
use cellar_core::narinfo::{NarInfo, Signature};
use ed25519_dalek::Signer as _;
use ed25519_dalek::Verifier;

/// A signer that can sign narinfo fingerprints.
pub struct NarInfoSigner {
    keypair: KeyPair,
}

impl NarInfoSigner {
    /// Create a new signer from a key pair.
    pub fn new(keypair: KeyPair) -> Self {
        Self { keypair }
    }

    /// Create from a Nix-format secret key string.
    pub fn from_nix_secret_key(s: &str) -> SignerResult<Self> {
        let keypair = KeyPair::from_nix_secret_key(s)?;
        Ok(Self::new(keypair))
    }

    /// Generate a new signer with a random key.
    pub fn generate(key_name: impl Into<String>) -> Self {
        Self::new(KeyPair::generate(key_name))
    }

    /// Get the key name.
    pub fn key_name(&self) -> &str {
        &self.keypair.name
    }

    /// Get the public key.
    pub fn public_key(&self) -> &PublicKey {
        &self.keypair.public
    }

    /// Get the Nix-format public key string.
    pub fn nix_public_key(&self) -> String {
        self.keypair.to_nix_public_key()
    }

    /// Get the Nix-format secret key string.
    pub fn nix_secret_key(&self) -> String {
        self.keypair.to_nix_secret_key()
    }

    /// Sign a narinfo and add the signature.
    pub fn sign(&self, narinfo: &mut NarInfo) {
        let fingerprint = narinfo.fingerprint();
        let signature = self.sign_fingerprint(&fingerprint);
        narinfo.add_signature(signature);
    }

    /// Sign a fingerprint string and return the signature.
    pub fn sign_fingerprint(&self, fingerprint: &str) -> Signature {
        let sig = self
            .keypair
            .secret
            .signing_key()
            .sign(fingerprint.as_bytes());
        let sig_b64 = base64::engine::general_purpose::STANDARD.encode(sig.to_bytes());
        Signature::new(&self.keypair.name, sig_b64)
    }
}

/// Trait for signature verification.
pub trait Signer {
    /// Sign a fingerprint and return the signature.
    fn sign_fingerprint(&self, fingerprint: &str) -> Signature;

    /// Get the key name.
    fn key_name(&self) -> &str;
}

impl Signer for NarInfoSigner {
    fn sign_fingerprint(&self, fingerprint: &str) -> Signature {
        self.sign_fingerprint(fingerprint)
    }

    fn key_name(&self) -> &str {
        self.key_name()
    }
}

/// Verify a narinfo signature.
pub fn verify_signature(
    narinfo: &NarInfo,
    signature: &Signature,
    public_key: &PublicKey,
) -> SignerResult<()> {
    let fingerprint = narinfo.fingerprint();

    let sig_bytes = base64::engine::general_purpose::STANDARD
        .decode(&signature.signature)
        .map_err(|e| SignerError::InvalidSignature(format!("invalid base64: {e}")))?;

    if sig_bytes.len() != 64 {
        return Err(SignerError::InvalidSignature(format!(
            "expected 64 bytes, got {}",
            sig_bytes.len()
        )));
    }

    let sig_array: [u8; 64] = sig_bytes
        .try_into()
        .map_err(|_| SignerError::InvalidSignature("invalid signature length".to_string()))?;

    let signature = ed25519_dalek::Signature::from_bytes(&sig_array);

    public_key
        .verifying_key()
        .verify(fingerprint.as_bytes(), &signature)
        .map_err(|_| SignerError::VerificationFailed)?;

    Ok(())
}

/// Verify any signature on a narinfo against a list of trusted public keys.
pub fn verify_narinfo(
    narinfo: &NarInfo,
    trusted_keys: &[(String, PublicKey)],
) -> SignerResult<bool> {
    for sig in &narinfo.signatures {
        // Find matching public key by name
        if let Some((_, pubkey)) = trusted_keys.iter().find(|(name, _)| name == &sig.key_name)
            && verify_signature(narinfo, sig, pubkey).is_ok()
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use cellar_core::hash::NarHash;
    use cellar_core::store_path::StorePath;

    #[test]
    fn test_sign_and_verify() {
        let signer = NarInfoSigner::generate("test-cache-1");

        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let mut narinfo = NarInfo::new_uncompressed(store_path, nar_hash, 12345);

        signer.sign(&mut narinfo);

        assert_eq!(narinfo.signatures.len(), 1);
        assert_eq!(narinfo.signatures[0].key_name, "test-cache-1");

        // Verify the signature
        let result = verify_signature(&narinfo, &narinfo.signatures[0], signer.public_key());
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_with_wrong_key() {
        let signer1 = NarInfoSigner::generate("key-1");
        let signer2 = NarInfoSigner::generate("key-2");

        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let mut narinfo = NarInfo::new_uncompressed(store_path, nar_hash, 12345);
        signer1.sign(&mut narinfo);

        // Verification with wrong key should fail
        let result = verify_signature(&narinfo, &narinfo.signatures[0], signer2.public_key());
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_narinfo_with_trusted_keys() {
        let signer = NarInfoSigner::generate("trusted-cache-1");

        let store_path =
            StorePath::parse("/nix/store/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-test").unwrap();
        let nar_hash =
            NarHash::from_sri("sha256-LCa0a2j/xo/5m0U8HTBBNBNCLXBkg7+g+YpeiGJm564=").unwrap();

        let mut narinfo = NarInfo::new_uncompressed(store_path, nar_hash, 12345);
        signer.sign(&mut narinfo);

        let trusted_keys = vec![("trusted-cache-1".to_string(), signer.public_key().clone())];

        assert!(verify_narinfo(&narinfo, &trusted_keys).unwrap());
    }
}
